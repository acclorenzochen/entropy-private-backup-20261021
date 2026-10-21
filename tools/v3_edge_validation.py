#!/usr/bin/env python3
"""Offline migration/compatible-rollback rehearsal. NEVER writes the source DB.

The source is opened mode=ro and copied with SQLite backup. Only temporary
copies are migrated. Rollback means current safety/group code with fast path
disabled, not an old binary or restoration of an old trading database.
No runtime, venue, sender, account connection or production process is started.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from entropy_arb.v3.core import Settings
from entropy_arb.v3.journal import Journal


CORE_TABLES = ('orders', 'batches', 'cycles', 'live_cycles', 'live_steps', 'request_events')


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _snapshot(db, columns=None):
    """Compare every preexisting canonical field, not just aggregate balances."""
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if columns is None:
        columns = {table: [r[1] for r in db.execute(f'PRAGMA table_info({_identifier(table)})')]
                   for table in CORE_TABLES if table in tables}
    result = {}
    for table, names in columns.items():
        rows = list(db.execute('SELECT ' + ','.join(map(_identifier, names)) +
                               ' FROM ' + _identifier(table)))
        rows = sorted((list(row) for row in rows), key=lambda row: json.dumps(row, default=str))
        totals = {}
        for index, name in enumerate(names):
            if name.endswith(('_units', '_usd')) or name in ('limit_px', 'avg_px', 'loss_budget'):
                values = [row[index] for row in rows if row[index] is not None]
                totals[name] = {'sum': str(sum((Decimal(str(v)) for v in values), Decimal(0))),
                                'null_count': len(rows) - len(values)}
        result[table] = {'rows': len(rows), 'sha256': _digest(rows), 'totals': totals}
    return columns, result


def _state(db):
    pending = [tuple(row) for row in db.execute("SELECT order_ref FROM orders WHERE state!='TERMINAL' ORDER BY order_ref")]
    active = [tuple(row) for row in db.execute("SELECT cycle_id FROM live_cycles WHERE phase!='DONE' ORDER BY cycle_id")]
    return {'pending_count': len(pending), 'pending_ids_sha256': _digest(pending),
            'active_count': len(active), 'active_ids_sha256': _digest(active)}


def _integrity(db):
    return [row[0] for row in db.execute('PRAGMA integrity_check')]


def _schema(db):
    return [tuple(row) for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")]


def _open_check(path, identity, columns):
    journal = Journal(path, identity=identity)
    try:
        _, core = _snapshot(journal.db, columns)
        state = _state(journal.db)
        pending = journal.pending()
        active = journal.active_live_cycle()
        api_ok = (_digest(sorted([(row['order_ref'],) for row in pending])) ==
                  state['pending_ids_sha256'] and
                  ((active is None and state['active_count'] == 0) or
                   (active is not None and state['active_count'] == 1 and
                    _digest([(active['cycle_id'],)]) == state['active_ids_sha256'])))
        return {'integrity': _integrity(journal.db), 'core': core, 'state': state,
                'all_current_core_fields': _snapshot(journal.db)[1],
                'recovery_read_api_ok': api_ok,
                'schema_sha256': _digest(_schema(journal.db)),
                'metadata': {k: v for k, v in journal.db.execute('SELECT key,value FROM metadata')
                             if k != 'identity'}}
    finally:
        journal.close()


def _backup(source, destination):
    with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)) as src:
        with closing(sqlite3.connect(destination)) as dst:
            src.backup(dst)


def _newer_rejection(path, identity, current_version):
    with closing(sqlite3.connect(path)) as db:
        db.execute("UPDATE metadata SET value=? WHERE key='economics_schema_version'",
                   (str(current_version + 1),))
        db.commit()
        before_schema = _schema(db)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    statements = []
    connect = sqlite3.connect
    def traced_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection
    error = None
    with patch.object(sqlite3, 'connect', traced_connect):
        try:
            journal = Journal(path, identity=identity)
        except ValueError as exc:
            error = str(exc)
        else:
            journal.close()
    with closing(connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        unchanged_schema = before_schema == _schema(db)
    mutations = [s for s in statements if s.lstrip().upper().startswith(
        ('CREATE', 'ALTER', 'DROP', 'INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'PRAGMA'))]
    unchanged = before == hashlib.sha256(path.read_bytes()).hexdigest()
    return {'passed': bool(error and 'newer' in error and unchanged and unchanged_schema and not mutations),
            'rejection': error, 'database_bytes_unchanged': unchanged,
            'schema_unchanged': unchanged_schema, 'mutation_statements': mutations,
            'sql_statement_count': len(statements), 'tested_version': current_version + 1}


def _write_report(path, report):
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False,
                                     prefix='.' + path.name, suffix='.tmp') as stream:
        temporary = Path(stream.name)
        try:
            json.dump(report, stream, indent=2, sort_keys=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _interrupted_migration(source, destination, identity, columns, baseline, state):
    """Interrupt the second additive ALTER on a disposable consistent copy."""
    _backup(source, destination)
    connect = sqlite3.connect
    attempts = 0
    def authorize(action, *unused):
        nonlocal attempts
        if action == sqlite3.SQLITE_ALTER_TABLE:
            attempts += 1
            if attempts == 2:
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK
    def interrupted_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        if kwargs.get('isolation_level', 'default') is None:
            connection.set_authorizer(authorize)
        return connection
    interrupted = False
    with patch.object(sqlite3, 'connect', interrupted_connect):
        try:
            journal = Journal(destination, identity=identity)
        except sqlite3.DatabaseError:
            interrupted = attempts == 2
            if not interrupted:
                raise
        else:
            journal.close()
    recovered = _open_check(destination, identity, columns)
    reopened = _open_check(destination, identity, columns)
    preserved = (recovered['core'] == baseline and recovered['state'] == state
                 and recovered['integrity'] == ['ok'] and reopened == recovered)
    return {'passed': preserved and (interrupted or attempts < 2),
            'status': 'INTERRUPTED_AND_RECOVERED' if interrupted else 'NO_SECOND_ADDITIVE_DDL_REQUIRED',
            'fault': 'SQLite authorizer denies second ALTER; no production permissions changed',
            'alter_attempts_before_interruption': attempts,
            'core_preserved': preserved, 'reopen_idempotent': recovered == reopened,
            'recovered_state': recovered['state']}


def rehearse(source: Path, output_dir: Path):
    source = source.resolve(strict=True)
    output_dir = output_dir.resolve()
    targets = [output_dir / name for name in ('migration_rehearsal.json', 'rollback_rehearsal.json')]
    if any(target.resolve() == source or (target.exists() and os.path.samefile(target, source))
           for target in targets):
        raise ValueError('report output must not overwrite source DB')
    source_before = hashlib.sha256(source.read_bytes()).hexdigest()
    root = Path(__file__).resolve().parents[1]
    files = ['entropy_arb/v3/journal.py', 'entropy_arb/v3/core.py',
             'entropy_arb/v3/runtime.py', 'entropy_arb/v3/live_host.py',
             'entropy_arb/venue_hl.py', 'tools/v3_edge_validation.py']
    provenance = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in files}
    common = {'generated_at': datetime.now(timezone.utc).isoformat(), 'source': str(source),
              'source_sha256': source_before, 'source_files': provenance,
              'python_executable': sys.executable, 'python_version': sys.version,
              'live_actions': 'NONE', 'source_access': 'SQLite mode=ro backup',
              'limitations': ['Offline ledger readability/preservation only; no runtime recovery or orders executed.',
                              'No historical rollback binary tested; current safety/group implementation retained.',
                              'Fault injection covers additive DDL interruption, not OS power loss.']}
    with tempfile.TemporaryDirectory(prefix='entropy-edge-rehearsal-') as directory:
        work = Path(directory)
        copied = work / 'migration.sqlite3'
        _backup(source, copied)
        common['consistent_backup_sha256'] = hashlib.sha256(copied.read_bytes()).hexdigest()
        with closing(sqlite3.connect(copied)) as db:
            metadata = dict(db.execute('SELECT key,value FROM metadata'))
            identity = metadata['identity']
            columns, baseline = _snapshot(db)
            baseline_state = _state(db)
            baseline_integrity = _integrity(db)
        interruption = _interrupted_migration(
            copied, work / 'interrupted.sqlite3', identity, columns, baseline, baseline_state)
        first = _open_check(copied, identity, columns)
        second = _open_check(copied, identity, columns)
        newer = work / 'newer.sqlite3'
        _backup(copied, newer)
        rejection = _newer_rejection(newer, identity, int(first['metadata']['economics_schema_version']))
        migration_ok = (baseline_integrity == ['ok'] and
                        all(row['integrity'] == ['ok'] and row['core'] == baseline and
                            row['state'] == baseline_state and row['recovery_read_api_ok']
                            for row in (first, second)) and first == second and rejection['passed']
                        and interruption['passed'])
        migration = {**common, 'passed': migration_ok, 'baseline_core': baseline,
                     'baseline_state': baseline_state, 'baseline_integrity': baseline_integrity,
                     'baseline_schema_version': metadata.get('economics_schema_version'),
                     'first_open': first, 'second_open': second,
                     'idempotent': first == second, 'newer_version_rejection': rejection,
                     'interruption_recovery': interruption}
        rollback_db = work / 'rollback.sqlite3'
        _backup(copied, rollback_db)
        rollback_settings = replace(Settings(), exit_fast_path_enabled=False)
        rollback_first = _open_check(rollback_db, identity, columns)
        rollback_second = _open_check(rollback_db, identity, columns)
        rollback = {**common, 'passed': bool(migration_ok and rollback_first == second and
                                             rollback_second == second and
                                             not rollback_settings.exit_fast_path_enabled),
                    'mode': 'current_safety_and_group_code_fast_disabled',
                    'exit_fast_path_enabled': rollback_settings.exit_fast_path_enabled,
                    'configuration_scope': 'in-memory rehearsal only; no config or production state changed',
                    'first_open': rollback_first, 'second_open': rollback_second}
    source_unchanged = source_before == hashlib.sha256(source.read_bytes()).hexdigest()
    source_code_unchanged = all(hashlib.sha256((root / name).read_bytes()).hexdigest() == digest
                                for name, digest in provenance.items())
    output_dir.mkdir(parents=True, exist_ok=True)
    for target, report in zip(targets, (migration, rollback)):
        report['source_bytes_unchanged'] = source_unchanged
        report['source_code_unchanged_during_run'] = source_code_unchanged
        report['passed'] = report['passed'] and source_unchanged and source_code_unchanged
        _write_report(target, report)
    return migration, rollback


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path,
                        default=Path('reports/entropy-edge-v2/baseline_journal.sqlite3'),
                        help='read-only source DB; NEVER migrated/written in place')
    parser.add_argument('--output-dir', type=Path, default=Path('reports/entropy-edge-v2'))
    args = parser.parse_args(argv)
    try:
        reports = rehearse(args.source, args.output_dir)
    except (OSError, sqlite3.Error, ValueError, KeyError, RuntimeError) as exc:
        print(f'Rehearsal failed: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1
    print(json.dumps({name: report['passed'] for name, report in
                      zip(('migration', 'compatible_rollback'), reports)}))
    return 0 if all(report['passed'] for report in reports) else 1


if __name__ == '__main__':
    raise SystemExit(main())
