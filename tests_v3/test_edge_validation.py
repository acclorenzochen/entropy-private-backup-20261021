import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from entropy_arb.v3.journal import Journal
from tools import v3_edge_validation as validation


@pytest.fixture
def source(tmp_path):
    path = tmp_path / 'baseline.sqlite3'
    journal = Journal(path, identity='offline-test-only')
    try:
        journal.db.execute("INSERT INTO orders(order_ref,cycle_id,venue,side,requested_units,"
                           "limit_px,reduce_only,state,filled_units,avg_px,fee_usd) "
                           "VALUES('pending','cycle','entropy','BUY',85,'101',0,'UNKNOWN',30,'100','.015')")
        journal.db.execute("INSERT INTO live_cycles(cycle_id,batch_id,direction,planned_units,"
                           "reference_bps,entry_basis_bps,entropy_units,hedge_units,cash_net_usd,"
                           "phase,started_ts,updated_ts) VALUES "
                           "('cycle','batch','LONG_ENTROPY',85,'1','2',30,-12,'-18.015','RECOVERY',1,2)")
        journal.db.execute("INSERT INTO live_steps(step_key,cycle_id,order_ref,venue,side,"
                           "requested_units,reduce_only,execution_group_id,parent_step_key) "
                           "VALUES('step','cycle','pending','entropy','BUY',85,0,'group',NULL)")
    finally:
        journal.close()
    return path


def test_migration_and_rollback_preserve_active_pending_and_source(source, tmp_path):
    before = source.read_bytes()
    reports = validation.rehearse(source, tmp_path / 'reports')
    assert all(report['passed'] for report in reports)
    migration, rollback = reports
    assert migration['baseline_state']['pending_count'] == 1
    assert migration['baseline_state']['active_count'] == 1
    assert migration['first_open']['core']['orders']['totals']['filled_units']['sum'] == '30'
    assert migration['first_open']['core']['orders']['totals']['fee_usd']['sum'] == '0.015'
    assert migration['newer_version_rejection']['mutation_statements'] == []
    assert migration['idempotent']
    assert rollback['exit_fast_path_enabled'] is False
    assert source.read_bytes() == before
    again = validation.rehearse(source, tmp_path / 'reports')
    assert all(report['passed'] for report in again)
    for name in ('migration_rehearsal.json', 'rollback_rehearsal.json'):
        assert json.loads((tmp_path / 'reports' / name).read_text())['passed']


def test_additive_migration_from_older_schema(source, tmp_path):
    with sqlite3.connect(source) as db:
        db.execute('ALTER TABLE live_steps DROP COLUMN parent_step_key')
        db.execute('ALTER TABLE live_steps DROP COLUMN execution_group_id')
        db.execute("UPDATE metadata SET value='7' WHERE key='economics_schema_version'")
    migration, rollback = validation.rehearse(source, tmp_path / 'reports')
    assert migration['passed'] and rollback['passed']
    assert migration['baseline_schema_version'] == '7'
    assert migration['interruption_recovery']['status'] == 'INTERRUPTED_AND_RECOVERED'
    assert migration['interruption_recovery']['alter_attempts_before_interruption'] == 2
    assert int(migration['first_open']['metadata']['economics_schema_version']) >= 8
    with sqlite3.connect(source) as db:
        assert 'parent_step_key' not in {row[1] for row in db.execute('PRAGMA table_info(live_steps)')}


def test_unverifiable_identity_is_rejected_before_ddl(tmp_path):
    path = tmp_path / 'unidentified.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE unrelated(value TEXT)')
        db.execute("INSERT INTO unrelated VALUES ('keep')")
    before = path.read_bytes()
    with pytest.raises(ValueError, match='metadata identity'):
        Journal(path, identity='not-proven')
    assert path.read_bytes() == before


def test_detects_core_mutation_even_when_row_counts_match(source, tmp_path, monkeypatch):
    original = validation._open_check
    def corrupt(path, identity, columns):
        result = original(path, identity, columns)
        result['core']['orders']['sha256'] = 'changed'
        return result
    monkeypatch.setattr(validation, '_open_check', corrupt)
    reports = validation.rehearse(source, tmp_path / 'reports')
    assert not any(report['passed'] for report in reports)


def test_source_cannot_be_report_target(source, tmp_path):
    directory = tmp_path / 'reports'
    directory.mkdir()
    alias = directory / 'migration_rehearsal.json'
    alias.symlink_to(source)
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match='overwrite source'):
        validation.rehearse(source, directory)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before


def test_help_and_missing_source(tmp_path, capsys):
    with pytest.raises(SystemExit) as exc:
        validation.main(['--help'])
    assert exc.value.code == 0
    assert 'NEVER writes the source DB' in capsys.readouterr().out
    assert validation.main(['--source', str(tmp_path / 'missing')]) == 1
    assert not (tmp_path / 'missing').exists()
