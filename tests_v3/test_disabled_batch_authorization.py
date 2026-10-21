"""Explicit nullable basis budgets; no credentials, accounts or live dispatch."""
from dataclasses import replace
from decimal import Decimal as D
import json
from types import SimpleNamespace

import pytest

from main_v3 import parser, validate_launch
from entropy_arb.v3.core import Settings
from entropy_arb.v3.live_host import BatchAuthorization, LiveAuthorizationError


PAIR = 'default:hl:SNDK:lighter:SNDK'


def authorization(tmp_path, **changes):
    path = tmp_path / 'authorization.json'
    path.write_text(json.dumps({**dict(batch_id='original-batch', pair_identity=PAIR,
        max_position_cycles=100, loss_budget_usd='3', max_notional_usd='200',
        approved_live=True), **changes}))
    return path


def load(path, **policy):
    return BatchAuthorization.load(path, expected_pair=PAIR,
        configured_max_notional=D('200'), **policy)


def basis_configuration():
    return SimpleNamespace(entropy=SimpleNamespace(kind='hl', symbol='SNDK', cap_usd=200),
        hedge=SimpleNamespace(kind='lighter', symbol='SNDK', cap_usd=200))


@pytest.mark.parametrize('maximum,loss', [(100, '3'), (None, '3'), (100, None), (None, None)])
def test_basis_launch_accepts_explicit_null_independently_and_preserves_other_authorization(tmp_path, maximum, loss):
    path = authorization(tmp_path, max_position_cycles=maximum, loss_budget_usd=loss)
    before = path.read_bytes()
    args = parser().parse_args(['--symbol', 'SNDK', '--hedge', 'lighter', '--live',
        '--operator-control', '--batch-authorization', str(path)])
    pair, result = validate_launch(args, basis_configuration(), Settings(max_notional_usd=D('200')))
    assert pair == PAIR and result.batch_id == 'original-batch'
    assert result.max_position_cycles == maximum
    assert result.loss_budget_usd == (None if loss is None else D(loss))
    assert result.max_notional_usd == D('200') and result.approved_live is True
    assert result.opening_experiment_id is None
    assert path.read_bytes() == before


@pytest.mark.parametrize('bad', [True, False, 0, -1, 2.0, '100', 'disabled', ''])
def test_disabled_cycle_limit_remains_strict_null_or_positive_integer(tmp_path, bad):
    with pytest.raises(LiveAuthorizationError, match='max_position_cycles'):
        load(authorization(tmp_path, max_position_cycles=bad), allow_disabled_cycle_limit=True)


def test_parser_default_requires_explicit_policy_for_nullable_limit(tmp_path):
    with pytest.raises(LiveAuthorizationError, match='max_position_cycles'):
        load(authorization(tmp_path, max_position_cycles=None))
    with pytest.raises(LiveAuthorizationError, match='explicit launch policy'):
        load(authorization(tmp_path, loss_budget_usd=None))


@pytest.mark.parametrize('field', ['max_position_cycles', 'loss_budget_usd'])
def test_missing_authorization_key_is_not_an_implicit_disabled_budget(tmp_path, field):
    path = authorization(tmp_path)
    raw = json.loads(path.read_text())
    raw.pop(field)
    path.write_text(json.dumps(raw))
    with pytest.raises(LiveAuthorizationError, match='exactly'):
        load(path, allow_disabled_cycle_limit=True, allow_disabled_loss_budget=True)


@pytest.mark.parametrize('maximum', [None, 19, 21])
def test_opening_ab_remains_finite_twenty_even_with_nullable_permissions(tmp_path, maximum):
    path = authorization(tmp_path, max_position_cycles=maximum,
        loss_budget_usd=None, opening_experiment_id='experiment')
    with pytest.raises(LiveAuthorizationError, match='twenty attempts'):
        load(path, expected_opening_experiment_id='experiment',
            allow_disabled_cycle_limit=True, allow_disabled_loss_budget=True)


def test_opening_ab_twenty_and_disabled_loss_still_parse(tmp_path):
    result = load(authorization(tmp_path, max_position_cycles=20,
        loss_budget_usd=None, opening_experiment_id='experiment'),
        expected_opening_experiment_id='experiment')
    assert result.max_position_cycles == 20 and result.loss_budget_usd is None


def test_funding_policy_keeps_finite_cycle_limit(tmp_path, monkeypatch):
    import main_v3
    monkeypatch.setattr(main_v3, 'validate_nbis_policy', lambda *args, **kwargs: None)
    args = parser().parse_args(['--symbol', 'SNDK', '--hedge', 'lighter', '--live',
        '--batch-authorization', str(authorization(tmp_path, loss_budget_usd=None))])
    settings = replace(Settings(max_notional_usd=D('200')), strategy_mode='funding_hedge')
    result = validate_launch(args, basis_configuration(), settings)[1]
    assert result.max_position_cycles == 100 and result.loss_budget_usd is None
    args.batch_authorization = str(authorization(tmp_path, max_position_cycles=None, loss_budget_usd=None))
    with pytest.raises(LiveAuthorizationError, match='max_position_cycles'):
        validate_launch(args, basis_configuration(), settings)


def test_null_budgets_cannot_change_pair_approval_or_notional_cap(tmp_path):
    for changes, message in [(dict(pair_identity='wrong'), 'pair'),
            (dict(approved_live=False), 'approved'), (dict(max_notional_usd='201'), 'enlarge')]:
        with pytest.raises(LiveAuthorizationError, match=message):
            load(authorization(tmp_path, max_position_cycles=None, loss_budget_usd=None, **changes),
                allow_disabled_cycle_limit=True, allow_disabled_loss_budget=True)
