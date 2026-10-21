"""Launch control is explicit and never aliases the recovery/unlock modes."""
from types import SimpleNamespace

import pytest
from main_v3 import parser, validate_launch

ARGS = ['--symbol', 'SNDK', '--hedge', 'lighter']

def test_control_requires_explicit_live_owner_opt_in():
    assert not parser().parse_args(ARGS).operator_control
    assert parser().parse_args(ARGS + ['--live', '--operator-control']).operator_control

@pytest.mark.parametrize('mode', [[], ['--record-only'], ['--public-preflight-only'],
    ['--live', '--validate-only'], ['--live', '--recovery-only'],
    ['--live', '--preflight-only'], ['--live', '--reopen-entries']])
def test_other_modes_cannot_enable_operator_commands(mode):
    with pytest.raises(SystemExit):
        parser().parse_args(ARGS + mode + ['--operator-control'])


def test_funding_control_refuses_before_account_or_authorization_access():
    args = parser().parse_args(ARGS + ['--live', '--operator-control'])
    with pytest.raises(ValueError, match='supports ordinary basis only'):
        validate_launch(args, None, SimpleNamespace(strategy_mode='funding_hedge'))
