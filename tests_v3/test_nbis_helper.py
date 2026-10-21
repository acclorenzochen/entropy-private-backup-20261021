import argparse
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import nbis_funding as helper
from entropy_arb.v3.live_host import BatchAuthorization


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    template = Path(__file__).resolve().parents[1] / 'nbis-funding-authorization.example.json'
    (tmp_path / template.name).write_bytes(template.read_bytes())
    monkeypatch.setattr(helper, 'ROOT', tmp_path)
    monkeypatch.chdir(tmp_path)
    executed, checked = [], []
    monkeypatch.setattr(helper.os, 'execv', lambda executable, args: executed.append((executable, args)))

    def check(args, *, cwd):
        checked.append((args, cwd))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(helper.subprocess, 'run', check)
    return tmp_path, executed, checked


def test_default_is_public_only_without_authorization_or_signing(isolated):
    root, executed, checked = isolated
    assert helper.main([]) == 0
    assert checked == [] and len(executed) == 1
    args = executed[0][1]
    assert '--public-preflight-only' in args and '--live' not in args
    assert '--batch-authorization' not in args
    assert args[args.index('--env-file') + 1] == '/dev/null'
    assert not (root / helper.AUTHORIZATION).exists()


@pytest.mark.parametrize('cycles', [1, 6, 100])
def test_prepare_creates_unapproved_auth_and_does_not_launch(isolated, cycles):
    root, executed, checked = isolated
    assert helper.main(['prepare', '--cycles', str(cycles)]) == 0
    auth = json.loads((root / helper.AUTHORIZATION).read_text())
    assert auth['max_position_cycles'] == cycles and auth['approved_live'] is False
    assert auth['batch_id'].startswith('nbis-funding-')
    assert auth['batch_id'] != 'REQUIRES_EXPLICIT_USER_AUTHORIZATION_NBIS'
    assert auth['pair_identity'] == 'nbis-funding:hl:NBIS:lighter:NBIS'
    assert auth['max_notional_usd'] == '200' and auth['loss_budget_usd'] is None
    assert executed == checked == []


def test_prepare_asks_for_round_limit_without_default_five(isolated, monkeypatch):
    root, executed, checked = isolated
    monkeypatch.setattr('builtins.input', lambda prompt: '17')
    assert helper.main(['prepare']) == 0
    assert json.loads((root / helper.AUTHORIZATION).read_text())['max_position_cycles'] == 17
    assert executed == checked == []


def test_prepare_refuses_to_overwrite_existing_authorization(isolated):
    root, executed, checked = isolated
    path = root / helper.AUTHORIZATION
    path.write_text('existing batch must stay intact')
    assert helper.main(['prepare', '--cycles', '100']) == 2
    assert path.read_text() == 'existing batch must stay intact'
    assert executed == checked == []


@pytest.mark.parametrize('value', ['0', '-1', 'nan', '1.5', str(2**63)])
def test_invalid_cycle_count_is_rejected(value):
    with pytest.raises(argparse.ArgumentTypeError):
        helper.positive_cycles(value)


@pytest.mark.parametrize('mode', ['check', 'start', 'status'])
def test_cycle_override_cannot_change_an_existing_batch(isolated, mode):
    with pytest.raises(SystemExit) as exc:
        helper.main([mode, '--cycles', '100'])
    assert exc.value.code == 2
    assert isolated[1] == isolated[2] == []


@pytest.mark.parametrize('contents', [None, '{}', '{"approved_live": false}',
                                    '{"approved_live": "true"}', 'bad json'])
def test_start_requires_manual_boolean_approval(isolated, contents):
    root, executed, checked = isolated
    if contents is not None:
        (root / helper.AUTHORIZATION).write_text(contents)
    assert helper.main(['start']) == 2
    assert executed == checked == []


def test_manual_start_preserves_authorization_and_has_no_session_cap(isolated):
    root, executed, checked = isolated
    helper.main(['prepare', '--cycles', '6'])
    path = root / helper.AUTHORIZATION
    auth = json.loads(path.read_text())
    auth['approved_live'] = True
    path.write_text(json.dumps(auth))
    before = path.read_bytes()
    parsed = BatchAuthorization.load(
        path, expected_pair='nbis-funding:hl:NBIS:lighter:NBIS',
        configured_max_notional=Decimal('200'), allow_disabled_loss_budget=True)
    assert parsed.max_position_cycles == 6 and parsed.loss_budget_usd is None
    assert helper.main(['start']) == 0
    assert len(checked) == len(executed) == 1
    assert '--public-preflight-only' in checked[0][0] and '--live' not in checked[0][0]
    args = executed[0][1]
    assert '--live' in args and '--max-new-cycles' not in args
    assert args[args.index('--batch-authorization') + 1] == helper.AUTHORIZATION
    assert args[args.index('--env-file') + 1] == '.env'
    assert path.read_bytes() == before


@pytest.mark.parametrize('returncode,expected', [(2, 2), (-15, 130)])
def test_failed_public_preflight_never_enters_live(isolated, monkeypatch, returncode, expected):
    root, executed, checked = isolated
    (root / helper.AUTHORIZATION).write_text('{"approved_live": true}')
    monkeypatch.setattr(helper.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(returncode=returncode))
    assert helper.main(['start']) == expected
    assert executed == []


def test_status_never_falls_back_to_old_sndk_status(isolated):
    root, executed, checked = isolated
    (root / 'logs/v3').mkdir(parents=True)
    (root / 'logs/v3/status.json').write_text('{"current_state": "READY", "mode": "live"}')
    assert helper.main(['status']) == 2
    assert executed == checked == []


def test_status_reads_only_nbis_file_and_warns_it_is_last_published(isolated, capsys):
    root, executed, checked = isolated
    (root / 'logs/v3/nbis').mkdir(parents=True)
    (root / 'logs/v3/nbis/status.json').write_text(
        '{"current_state": "HOLDING_FUNDING", "mode": "live", "funding_state": {"settlement_counts": {"entropy": 1, "hedge": 0}}}')
    assert helper.main(['status']) == 0
    assert 'HOLDING_FUNDING' in capsys.readouterr().out
    assert executed == checked == []
