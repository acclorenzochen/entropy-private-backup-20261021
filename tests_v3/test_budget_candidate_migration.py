"""Installer checks use only fresh fake projects/databases below pytest tmp."""
import fcntl
import json
from pathlib import Path
import shutil
import sqlite3
from decimal import Decimal

import pytest

from tools import apply_budget_candidate as tool


def fixture(tmp_path):
    project, package = tmp_path / 'project', tmp_path / 'package'
    project.mkdir()
    (package / 'source').mkdir(parents=True)
    files = []
    for name, old, new in [('main_v3.py', b'old source\n', b'tested source\n'),
            ('entropy_arb/v3/new_helper.py', None, b'tested new helper\n')]:
        if old is not None:
            (project / name).parent.mkdir(parents=True, exist_ok=True)
            (project / name).write_bytes(old)
        (package / 'source' / name).parent.mkdir(parents=True, exist_ok=True)
        (package / 'source' / name).write_bytes(new)
        files.append(dict(path=name, before_sha256=tool.digest(old) if old else None,
                          after_sha256=tool.digest(new)))
    logs = project / 'logs'
    logs.mkdir()
    shared = logs / 'entropy-arb.lock'
    shared.write_text('original owner PID evidence\n')
    journal = logs / 'journal.sqlite3'
    Path(str(journal) + '.owner.lock').write_text('original journal owner evidence\n')
    auth = logs / 'authorization.json'
    auth.write_text(json.dumps(dict(batch_id='existing', pair_identity='default:hl:SNDK:lighter:SNDK',
        max_position_cycles=100, loss_budget_usd='3', max_notional_usd='200', approved_live=True)))
    with sqlite3.connect(journal) as db:
        db.executescript('''
            CREATE TABLE batches(batch_id TEXT PRIMARY KEY,max_position_cycles INTEGER,loss_budget TEXT,
                closed_for_entries INTEGER,close_reason TEXT,close_value TEXT);
            INSERT INTO batches VALUES('existing',100,'3',1,'SINGLE_CYCLE_REVIEW_LOSS','-0.2519515325');
            CREATE TABLE live_cycles(cycle_id TEXT,phase TEXT,entropy_units INTEGER,hedge_units INTEGER,
                position_evidence_authoritative INTEGER);
            INSERT INTO live_cycles VALUES('historical','DONE',0,0,1);
            CREATE TABLE orders(order_ref TEXT,state TEXT,filled_units INTEGER);
            INSERT INTO orders VALUES('zero','TERMINAL',0);
            CREATE TABLE operator_controls(scope_key TEXT,intent TEXT,intent_revision INTEGER);
            INSERT INTO operator_controls VALUES('private-scope','STOPPED',14);
            CREATE TABLE metadata(key TEXT,value TEXT);
            INSERT INTO metadata VALUES('operator_parameters:private-scope',
                '{"current":{"revision":2},"saved":{"revision":4}}');
            CREATE TABLE cycles(cycle_id TEXT,result_kind TEXT,pnl_usd TEXT,accounting_final INTEGER);
            INSERT INTO cycles VALUES('loss','normal_exit','-3.478339',1);
            CREATE TABLE fee_fills(venue TEXT,fee_usd TEXT);
            INSERT INTO fee_fills VALUES('entropy','0.031221');
        ''')
        db.executemany('INSERT INTO cycles VALUES(?,?,?,?)',
            [(f'old-{index}', 'normal_exit', '0', 1) for index in range(99)])
    manifest = package / 'install-manifest.json'
    manifest.write_text(json.dumps(dict(files=files, auth_before_sha256=tool.file_hash(auth),
        batch_id='existing', pair_identity='default:hl:SNDK:lighter:SNDK')))
    return dict(project_root=project, journal=journal, authorization=auth,
        manifest_path=manifest, shared_lock=shared, backup_dir=project / 'backups' / 'before')


def fingerprint(options):
    project = options['project_root']
    files = {str(path.relative_to(project)): path.read_bytes()
        for path in project.rglob('*') if path.is_file()}
    with sqlite3.connect(options['journal']) as db:
        data = tool.database_digest(db)
    return files, data


def test_default_dry_run_is_read_only_including_pid_lock_files(tmp_path):
    options = fixture(tmp_path)
    before = fingerprint(options)
    result = tool.run(**options)
    assert result['mode'] == 'dry_run' and result['apply_blocked_reason'] is None
    assert not options['backup_dir'].exists()
    assert fingerprint(options) == before


@pytest.mark.parametrize('lock', ['shared_lock', 'owner'])
def test_held_owner_or_shared_lock_blocks_apply_without_mutation(tmp_path, lock):
    options = fixture(tmp_path)
    before = fingerprint(options)
    path = options['shared_lock'] if lock == 'shared_lock' else Path(str(options['journal']) + '.owner.lock')
    with path.open('rb') as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert 'lock held' in tool.run(**options)['apply_blocked_reason']
        with pytest.raises(tool.Refused, match='lock held'):
            tool.run(**options, apply=True)
    assert not options['backup_dir'].exists()
    assert fingerprint(options) == before


def test_apply_changes_only_two_budget_cells_and_authorization_keys(tmp_path):
    options = fixture(tmp_path)
    before_files, before_digest = fingerprint(options)
    auth_before = json.loads(options['authorization'].read_text())
    with sqlite3.connect(options['journal']) as db:
        db.row_factory = sqlite3.Row
        batch_before = dict(db.execute('SELECT * FROM batches').fetchone())
    result = tool.run(**options, apply=True)
    assert result['phase'] == 'completed'
    updated = json.loads(options['authorization'].read_text())
    assert updated == {**auth_before, 'loss_budget_usd': None, 'max_position_cycles': None}
    with sqlite3.connect(options['journal']) as db:
        db.row_factory = sqlite3.Row
        batch_after = dict(db.execute('SELECT * FROM batches').fetchone())
        assert batch_after == {**batch_before, 'loss_budget': 'disabled', 'max_position_cycles': 'disabled'}
        assert tool.database_digest(db, original_batch=batch_before) == before_digest
        assert db.execute('SELECT COUNT(*) FROM cycles').fetchone()[0] == 100
        assert db.execute("SELECT pnl_usd FROM cycles WHERE cycle_id='loss'").fetchone()[0] == '-3.478339'
        assert json.loads(db.execute('SELECT value FROM metadata').fetchone()[0]) == dict(
            current=dict(revision=2), saved=dict(revision=4))
    assert options['shared_lock'].read_bytes() == before_files['logs/entropy-arb.lock']
    assert Path(str(options['journal']) + '.owner.lock').read_bytes() == before_files['logs/journal.sqlite3.owner.lock']
    backup = options['backup_dir']
    with sqlite3.connect(backup / 'journal.sqlite3') as db:
        assert tool.database_digest(db) == before_digest
    assert (backup / 'authorization.json').read_bytes() == before_files['logs/authorization.json']
    assert (backup / 'source/main_v3.py').read_bytes() == before_files['main_v3.py']
    progress = json.loads((backup / 'install-record.json').read_text())
    assert progress['phase'] == 'completed' and progress['auth_updated'] is True
    assert len(progress['installed_files']) == 2


@pytest.mark.parametrize('failure', ['active', 'pending', 'residual', 'unknown', 'running'])
def test_unsafe_database_state_blocks_before_backup_or_source_change(tmp_path, failure):
    options = fixture(tmp_path)
    with sqlite3.connect(options['journal']) as db:
        sql = dict(active="UPDATE live_cycles SET phase='OPEN'",
            pending="UPDATE orders SET state='UNKNOWN'", residual='UPDATE live_cycles SET entropy_units=1',
            unknown='UPDATE live_cycles SET hedge_units=NULL',
            running="UPDATE operator_controls SET intent='RUNNING'")[failure]
        db.execute(sql)
    before = fingerprint(options)
    with pytest.raises(tool.Refused):
        tool.run(**options, apply=True)
    assert not options['backup_dir'].exists() and fingerprint(options) == before


@pytest.mark.parametrize('failure', ['source', 'candidate', 'authorization', 'budget', 'batch', 'new_file'])
def test_drift_or_wrong_batch_refuses_without_overwriting_fresh_state(tmp_path, failure):
    options = fixture(tmp_path)
    project = options['project_root']
    if failure == 'source':
        (project / 'main_v3.py').write_text('fresh user source change')
    elif failure == 'candidate':
        (options['manifest_path'].parent / 'source/main_v3.py').write_text('untested candidate')
    elif failure == 'authorization':
        value = json.loads(options['authorization'].read_text())
        value['max_notional_usd'] = '150'
        options['authorization'].write_text(json.dumps(value))
    elif failure == 'new_file':
        (project / 'entropy_arb/v3').mkdir(parents=True)
        (project / 'entropy_arb/v3/new_helper.py').write_text('user file must remain')
    else:
        with sqlite3.connect(options['journal']) as db:
            db.execute("UPDATE batches SET loss_budget='4'" if failure == 'budget'
                       else "UPDATE batches SET batch_id='different'")
    before = fingerprint(options)
    with pytest.raises(tool.Refused):
        tool.run(**options, apply=True)
    assert not options['backup_dir'].exists() and fingerprint(options) == before


@pytest.mark.parametrize('name', ['../escape.py', '.env', 'logs/status.json', '/tmp/escape.py'])
def test_manifest_cannot_overwrite_credentials_runtime_or_outside_project(tmp_path, name):
    options = fixture(tmp_path)
    value = json.loads(options['manifest_path'].read_text())
    value['files'][0]['path'] = name
    options['manifest_path'].write_text(json.dumps(value))
    before = fingerprint(options)
    with pytest.raises(tool.Refused):
        tool.run(**options, apply=True)
    assert fingerprint(options) == before


def test_precommit_failure_restores_code_auth_database_and_retains_recovery_record(tmp_path, monkeypatch):
    options = fixture(tmp_path)
    before = fingerprint(options)
    write = tool.atomic_write
    failed = False
    def interrupt_after_auth(path, data):
        nonlocal failed
        write(path, data)
        if path == options['authorization'] and not failed:
            failed = True
            raise OSError('simulated failure after atomic authorization replace')
    monkeypatch.setattr(tool, 'atomic_write', interrupt_after_auth)
    with pytest.raises(OSError, match='simulated failure'):
        tool.run(**options, apply=True)
    original_files, original_digest = before
    for name, data in original_files.items():
        assert (options['project_root'] / name).read_bytes() == data
    with sqlite3.connect(options['journal']) as db:
        assert tool.database_digest(db) == original_digest
    assert not (options['project_root'] / 'entropy_arb/v3/new_helper.py').exists()
    record = json.loads((options['backup_dir'] / 'install-record.json').read_text())
    assert record['phase'] == 'rolled_back'


def test_recovery_does_not_overwrite_a_later_user_source_edit(tmp_path, monkeypatch):
    options = fixture(tmp_path)
    write = tool.atomic_write
    target = options['project_root'] / 'main_v3.py'
    def user_edit_after_install(path, data):
        write(path, data)
        if path == options['authorization']:
            target.write_text('later user edit must stay')
            raise OSError('simulated interruption plus user edit')
    monkeypatch.setattr(tool, 'atomic_write', user_edit_after_install)
    with pytest.raises(tool.Refused, match='source changed during recovery'):
        tool.run(**options, apply=True)
    assert target.read_text() == 'later user edit must stay'
    with sqlite3.connect(options['journal']) as db:
        assert db.execute('SELECT max_position_cycles,loss_budget FROM batches').fetchone() == (100, '3')


def test_corrupt_source_backup_is_refused_before_install(tmp_path, monkeypatch):
    options = fixture(tmp_path)
    before = fingerprint(options)
    copy = tool.shutil.copy2
    def corrupt_copy(original, target):
        copy(original, target)
        target.write_text('corrupted during backup copy')
    monkeypatch.setattr(tool.shutil, 'copy2', corrupt_copy)
    with pytest.raises(tool.Refused, match='source drift during backup'):
        tool.run(**options, apply=True)
    for name, data in before[0].items():
        assert (options['project_root'] / name).read_bytes() == data
    with sqlite3.connect(options['journal']) as db:
        assert tool.database_digest(db) == before[1]


def test_corrupt_backup_during_failure_is_not_used_to_restore_source(tmp_path, monkeypatch):
    options = fixture(tmp_path)
    write = tool.atomic_write
    def corrupt_backup_and_fail(path, data):
        write(path, data)
        if path == options['authorization']:
            (options['backup_dir'] / 'source/main_v3.py').write_text('tampered backup')
            raise OSError('simulated interruption after backup tampering')
    monkeypatch.setattr(tool, 'atomic_write', corrupt_backup_and_fail)
    with pytest.raises(tool.Refused, match='source backup changed during recovery'):
        tool.run(**options, apply=True)
    assert (options['project_root'] / 'main_v3.py').read_bytes() == b'tested source\n'
    with sqlite3.connect(options['journal']) as db:
        assert db.execute('SELECT max_position_cycles,loss_budget FROM batches').fetchone() == (100, '3')


def test_frozen_readonly_snapshot_copy_preserves_real_history_parameters_and_loss(tmp_path):
    options = fixture(tmp_path)
    candidate = Path(__file__).resolve().parents[2]
    shutil.copy2(candidate / 'journal-readonly-snapshot.sqlite3', options['journal'])
    options['authorization'].write_bytes((candidate / 'authorization-before.json').read_bytes())
    authorization = json.loads(options['authorization'].read_text())
    manifest = json.loads(options['manifest_path'].read_text())
    manifest.update(auth_before_sha256=tool.file_hash(options['authorization']),
        batch_id=authorization['batch_id'], pair_identity=authorization['pair_identity'])
    options['manifest_path'].write_text(json.dumps(manifest))
    with sqlite3.connect(options['journal']) as db:
        db.row_factory = sqlite3.Row
        original = dict(db.execute('SELECT * FROM batches WHERE batch_id=?',
            (authorization['batch_id'],)).fetchone())
        before = tool.database_digest(db)
        original_parameters = list(db.execute("SELECT key,value FROM metadata WHERE key LIKE 'operator_parameters:%'"))
        original_loss = sum((max(-Decimal(row[0] or '0'), Decimal('0')) for row in db.execute(
            'SELECT pnl_usd FROM cycles WHERE batch_id=? AND complete_accounting=1 AND accounting_final=1',
            (authorization['batch_id'],))), Decimal('0'))
        assert original['max_position_cycles'] == 100 and original['loss_budget'] == '3'
        assert original['close_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
        assert original_loss.quantize(Decimal('0.000001')) == Decimal('3.478339')
    tool.run(**options, apply=True)
    with sqlite3.connect(options['journal']) as db:
        assert tool.database_digest(db, original_batch=original) == before
        assert list(db.execute("SELECT key,value FROM metadata WHERE key LIKE 'operator_parameters:%'")) == [tuple(row) for row in original_parameters]
        assert db.execute('SELECT closed_for_entries,close_reason FROM batches WHERE batch_id=?',
            (authorization['batch_id'],)).fetchone() == (1, 'SINGLE_CYCLE_REVIEW_LOSS')
