#!/usr/bin/env python3
"""Manual, offline installation and explicit existing-batch budget migration.

Default is read-only. Only a human's --apply invocation writes anything.
No engine/SDK imports, network, process control, risk unlocking or new batch.
The files and SQLite commit are separate resources, with a durable recovery
record; this is not an atomic multi-resource transaction.
"""
import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
from uuid import uuid4


class Refused(RuntimeError):
    pass


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_hash(path):
    return digest(path.read_bytes()) if path.is_file() else None


def inside(root, path):
    path = Path(path).resolve()
    if not path.is_relative_to(root):
        raise Refused('path is outside the selected project')
    return path


def source_path(root, name):
    relative = Path(name)
    if (relative.is_absolute() or '..' in relative.parts or not relative.parts
            or relative.parts[0] in ('.env', '.git', '.aws', 'logs', 'backups')
            or any(part.startswith('.env') for part in relative.parts)):
        raise Refused('invalid source manifest path')
    return inside(root, root / relative)


def atomic_write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = path.stat().st_mode & 0o777 if path.exists() else 0o600
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def record(path, value):
    atomic_write(path, (json.dumps(value, indent=2, sort_keys=True) + '\n').encode())


def database_digest(db, *, original_batch=None):
    """All schemas/rows, allowing only the selected batch's two budget cells."""
    result = []
    for name, schema in db.execute("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name"):
        quoted = '"' + name.replace('"', '""') + '"'
        cursor = db.execute('SELECT * FROM ' + quoted)
        columns = [column[0] for column in cursor.description]
        rows = []
        for row in cursor:
            value = dict(zip(columns, row))
            if name == 'batches' and original_batch and value['batch_id'] == original_batch['batch_id']:
                for key in ('loss_budget', 'max_position_cycles'):
                    value[key] = original_batch[key]
            rows.append(json.dumps(value, sort_keys=True,
                default=lambda item: {'blob_hex': item.hex()} if isinstance(item, bytes) else str(item)))
        result.append((name, schema, sorted(rows)))
    return digest(json.dumps(result, separators=(',', ':')).encode())


def checked_database(db, manifest, authorization):
    db.row_factory = sqlite3.Row
    rows = db.execute('SELECT * FROM batches WHERE batch_id=?', (manifest['batch_id'],)).fetchall()
    if len(rows) != 1:
        raise Refused('authorized existing batch not found')
    batch = dict(rows[0])
    if (batch['max_position_cycles'] != authorization['max_position_cycles']
            or str(batch['loss_budget']) != str(authorization['loss_budget_usd'])):
        raise Refused('batch budgets differ from the frozen authorization')
    if db.execute("SELECT 1 FROM live_cycles WHERE phase!='DONE' LIMIT 1").fetchone():
        raise Refused('active cycle exists')
    if db.execute("SELECT 1 FROM orders WHERE state!='TERMINAL' LIMIT 1").fetchone():
        raise Refused('pending order exists')
    if db.execute('SELECT 1 FROM live_cycles WHERE entropy_units IS NULL OR hedge_units IS NULL '
                  'OR entropy_units!=0 OR hedge_units!=0 LIMIT 1').fetchone():
        raise Refused('unknown or nonzero residual inventory exists')
    if db.execute("SELECT 1 FROM operator_controls WHERE intent!='STOPPED' LIMIT 1").fetchone():
        raise Refused('operator intent is not stopped')
    return batch


def checked_files(project, manifest, candidate):
    entries = manifest['files']
    if type(entries) is not list or not entries:
        raise Refused('empty source manifest')
    names = set()
    for entry in entries:
        if type(entry) is not dict or set(entry) != {'path', 'before_sha256', 'after_sha256'}:
            raise Refused('invalid source manifest entry')
        name = entry['path']
        if name in names:
            raise Refused('duplicate source manifest path')
        names.add(name)
        target = source_path(project, name)
        source = source_path(candidate, name)
        if file_hash(target) != entry['before_sha256']:
            raise Refused('installed source drift: ' + name)
        if file_hash(source) != entry['after_sha256'] or entry['after_sha256'] is None:
            raise Refused('candidate source drift: ' + name)


def acquire_locks(stack, paths):
    for path in paths:
        if not path.is_file():
            raise Refused('required existing ownership lock file missing')
        handle = stack.enter_context(path.open('rb'))
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Refused('owner/shared journal lock held; exit the owner before applying') from None


def run(*, project_root, journal, authorization, manifest_path, apply=False,
        shared_lock=None, backup_dir=None):
    project = Path(project_root).resolve()
    journal = inside(project, journal)
    authorization = inside(project, authorization)
    shared = inside(project, shared_lock or project / 'logs/entropy-arb.lock')
    if shared != inside(project, project / 'logs/entropy-arb.lock'):
        raise Refused('retain the configured shared logs/entropy-arb.lock')
    owner_lock = inside(project, str(journal) + '.owner.lock')
    manifest_path = Path(manifest_path).resolve()
    candidate = (manifest_path.parent / 'source').resolve()
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    required = {'files', 'auth_before_sha256', 'batch_id', 'pair_identity'}
    if type(manifest) is not dict or set(manifest) != required:
        raise Refused('invalid install manifest')
    auth_bytes = authorization.read_bytes()
    if digest(auth_bytes) != manifest['auth_before_sha256']:
        raise Refused('authorization drift')
    auth = json.loads(auth_bytes)
    if (set(auth) != {'batch_id', 'pair_identity', 'max_position_cycles', 'loss_budget_usd',
                     'max_notional_usd', 'approved_live'} or auth['approved_live'] is not True
            or auth['batch_id'] != manifest['batch_id'] or auth['pair_identity'] != manifest['pair_identity']
            or type(auth['max_position_cycles']) is not int or auth['max_position_cycles'] < 1
            or auth['loss_budget_usd'] is None):
        raise Refused('expected finite ordinary-basis authorization required')
    checked_files(project, manifest, candidate)
    with sqlite3.connect(journal.as_uri() + '?mode=ro', uri=True) as readonly:
        readonly.execute('PRAGMA query_only=ON')
        readonly.execute('BEGIN')
        batch = checked_database(readonly, manifest, auth)
        before_database = database_digest(readonly)
        readonly.rollback()
    plan = dict(mode='apply' if apply else 'dry_run', batch_id=manifest['batch_id'],
        changed_batch_fields=['loss_budget', 'max_position_cycles'],
        budgets_before={key: batch[key] for key in ('loss_budget', 'max_position_cycles')},
        budgets_after=dict(loss_budget='disabled', max_position_cycles='disabled'),
        source_files=[entry['path'] for entry in manifest['files']],
        preserved_database_sha256=before_database)
    if not apply:
        try:
            with ExitStack() as locks:
                acquire_locks(locks, (shared, owner_lock))
            plan['apply_blocked_reason'] = None
        except Refused as exc:
            plan['apply_blocked_reason'] = str(exc)
        return plan
    backup = Path(backup_dir).resolve() if backup_dir else project / 'backups' / (
        'budget-zero-fill-' + datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S') + '-' + uuid4().hex[:8])
    backup = inside(project, backup)
    with ExitStack() as locks:
        acquire_locks(locks, (shared, owner_lock))
        checked_files(project, manifest, candidate)
        if authorization.read_bytes() != auth_bytes:
            raise Refused('authorization changed while acquiring ownership')
        if backup.exists():
            raise Refused('backup directory already exists')
        backup.mkdir(parents=True, mode=0o700)
        with sqlite3.connect(journal.as_uri() + '?mode=ro', uri=True) as readonly:
            readonly.execute('PRAGMA query_only=ON')
            with sqlite3.connect(backup / 'journal.sqlite3') as destination:
                readonly.backup(destination)
        with sqlite3.connect(backup / 'journal.sqlite3') as saved:
            if database_digest(saved) != before_database:
                raise Refused('database changed before consistent backup')
        for entry in manifest['files']:
            original = source_path(project, entry['path'])
            if original.exists():
                target = backup / 'source' / entry['path']
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(original, target)
                if file_hash(target) != entry['before_sha256']:
                    raise Refused('source drift during backup: ' + entry['path'])
        atomic_write(backup / 'authorization.json', auth_bytes)
        for saved_file in backup.rglob('*'):
            if saved_file.is_file():
                with saved_file.open('rb') as durable:
                    os.fsync(durable.fileno())
        for directory in sorted([backup, *(path for path in backup.rglob('*') if path.is_dir())],
                                key=lambda path: len(path.parts), reverse=True):
            descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        progress = dict(plan, phase='backed_up', manifest_sha256=digest(manifest_bytes),
            project_root=str(project), journal=str(journal), authorization=str(authorization),
            installed_files=[], auth_updated=False,
            files=manifest['files'], original_authorization_sha256=digest(auth_bytes))
        recovery_record = backup / 'install-record.json'
        record(recovery_record, progress)
        db = sqlite3.connect(journal, timeout=2, isolation_level=None)
        changed = []
        auth_updated = False
        committed = False
        try:
            db.execute('BEGIN IMMEDIATE')
            checked_database(db, manifest, auth)
            if database_digest(db) != before_database:
                raise Refused('database drift after backup')
            checked_files(project, manifest, candidate)
            if authorization.read_bytes() != auth_bytes:
                raise Refused('authorization drift after backup')
            if manifest_path.read_bytes() != manifest_bytes:
                raise Refused('install manifest drift after backup')
            progress['phase'] = 'applying'
            record(recovery_record, progress)
            for entry in manifest['files']:
                target = source_path(project, entry['path'])
                new_bytes = source_path(candidate, entry['path']).read_bytes()
                if file_hash(target) != entry['before_sha256'] or digest(new_bytes) != entry['after_sha256']:
                    raise Refused('source drift during install: ' + entry['path'])
                progress['pending_file'] = entry['path']
                record(recovery_record, progress)
                changed.append(entry)
                atomic_write(target, new_bytes)
                progress['installed_files'].append(entry['path'])
                progress['pending_file'] = None
                record(recovery_record, progress)
            next_auth = {**auth, 'loss_budget_usd': None, 'max_position_cycles': None}
            auth_updated = True
            progress['auth_update_pending'] = True
            record(recovery_record, progress)
            atomic_write(authorization, (json.dumps(next_auth, indent=2) + '\n').encode())
            progress['auth_update_pending'] = False
            progress['auth_updated'] = True
            record(recovery_record, progress)
            db.execute("UPDATE batches SET loss_budget='disabled',max_position_cycles='disabled' WHERE batch_id=?",
                       (manifest['batch_id'],))
            if database_digest(db, original_batch=batch) != before_database:
                raise Refused('unapproved database state changed')
            progress['phase'] = 'database_commit_pending'
            record(recovery_record, progress)
            db.execute('COMMIT')
            committed = True
            progress['phase'] = 'completed'
            record(recovery_record, progress)
        except BaseException as exc:
            if db.in_transaction:
                db.execute('ROLLBACK')
            if committed or database_digest(db) != before_database:
                raise Refused('commit may have completed; preserve installed state and review ' + str(recovery_record)) from exc
            # Never overwrite a later edit while recovering a pre-commit failure.
            for entry in changed:
                if file_hash(source_path(project, entry['path'])) not in (
                        entry['before_sha256'], entry['after_sha256']):
                    raise Refused('source changed during recovery; review ' + str(recovery_record)) from exc
                if (entry['before_sha256'] is not None and file_hash(backup / 'source' / entry['path'])
                        != entry['before_sha256']):
                    raise Refused('source backup changed during recovery; review ' + str(recovery_record)) from exc
            if auth_updated:
                if authorization.read_bytes() == auth_bytes:
                    pass
                elif json.loads(authorization.read_text()) == next_auth:
                    atomic_write(authorization, auth_bytes)
                else:
                    raise Refused('authorization changed during recovery; review ' + str(recovery_record)) from exc
            for entry in reversed(changed):
                target = source_path(project, entry['path'])
                original = backup / 'source' / entry['path']
                if file_hash(target) == entry['before_sha256']:
                    continue
                if entry['before_sha256'] is None:
                    target.unlink()
                else:
                    atomic_write(target, original.read_bytes())
            progress['phase'] = 'rolled_back'
            progress['error_type'] = type(exc).__name__
            record(recovery_record, progress)
            raise
        finally:
            db.close()
    return {**plan, 'phase': 'completed', 'backup_dir': str(backup), 'recovery_record': str(recovery_record)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('project-root', 'journal', 'authorization', 'manifest-path'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--shared-lock')
    parser.add_argument('--backup-dir')
    parser.add_argument('--apply', action='store_true', help='human-approved write; default is read-only dry run')
    try:
        result = run(**vars(parser.parse_args()))
    except (Refused, OSError, ValueError, sqlite3.Error) as exc:
        print(json.dumps(dict(applied=False, error=str(exc)), ensure_ascii=False))
        return 1
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
