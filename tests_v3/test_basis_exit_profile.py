"""Offline checks for the next basis profile's gross exit and loss gates."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import json
from pathlib import Path
import socket
import time
from types import SimpleNamespace

import pytest

from entropy_arb.v3.core import Direction, Settings
from entropy_arb.v3.journal import Journal
from entropy_arb.v3.live_host import LiveAuthorizationError
from entropy_arb.v3.runtime import RuntimeHalt
from test_funding_hedge_runtime import fresh_runtime
from test_normal_exit import record_fill, seed_pair
from test_parallel_exit import assert_closed_once, finish_exit
from test_runtime import complete_background_audit, seed_close_cycle, setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('basis profile tests cannot access network')

    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


@pytest.fixture
def basis_runtime(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, single_cycle_review_loss_usd=None,
                               exit_min_gross_profit_bps=D('2.5'))
    yield runtime, e, h, journal
    journal.close()


def test_next_profile_uses_explicit_disabled_loss_and_five_cent_gross_target():
    from main_v3 import load_settings

    path = Path(__file__).parents[1] / 'config.v3.edge-next.json'
    raw = json.loads(path.read_text())
    current = json.loads((Path(__file__).parents[1] / 'config.v3.edge.json').read_text())
    changed = {'single_cycle_review_loss_usd', 'exit_min_gross_profit_usd',
               'lighter_zero_fee_account_index', 'lighter_zero_fee_batch_id',
               'lighter_zero_fee_policy_source'}
    assert {key: value for key, value in raw.items() if key not in changed} == {
        key: value for key, value in current.items() if key not in changed}
    settings = load_settings(path)
    assert settings == Settings.from_mapping(raw)
    assert raw['single_cycle_review_loss_usd'] is None
    assert settings.single_cycle_review_loss_usd is None
    assert settings.exit_min_gross_profit_usd == D('.05')
    assert settings.strategy_mode == 'basis'


def test_disabled_single_cycle_loss_preserves_default_and_legacy_values():
    assert Settings().single_cycle_review_loss_usd == D('.10')
    assert Settings.from_mapping({}).single_cycle_review_loss_usd == D('.10')
    assert Settings.from_mapping({'single_cycle_review_loss_usd': '.25'}).single_cycle_review_loss_usd == D('.25')
    assert Settings.from_mapping({'single_cycle_review_loss_usd': None}).single_cycle_review_loss_usd is None


@pytest.mark.parametrize('override,accepted', [
    ({}, True),
    ({'loss_budget_usd': None}, False),
    ({'max_notional_usd': '201'}, False),
    ({'approved_live': False}, False),
    ({'pair_identity': 'default:hl:NBIS:lighter:NBIS'}, False),
])
def test_next_profile_accepts_old_authorization_shape_and_preserves_launch_gates(
        tmp_path, override, accepted):
    from main_v3 import load_settings, parser, validate_launch

    root = Path(__file__).parents[1]
    path = tmp_path / 'offline-authorization.json'
    raw = dict(batch_id='offline-test-only', pair_identity='default:hl:SNDK:lighter:SNDK',
               max_position_cycles=100, loss_budget_usd='3',
               max_notional_usd='200', approved_live=True)
    path.write_text(json.dumps({**raw, **override}))
    args = parser().parse_args([
        '--symbol', 'SNDK', '--hedge', 'lighter', '--live',
        '--batch-authorization', str(path),
        '--v3-config', str(root / 'config.v3.edge.json'),
        '--v3-config', str(root / 'config.v3.edge-next.json')])
    settings = load_settings(args.v3_config)
    assert settings.single_cycle_review_loss_usd is None
    assert settings.exit_min_gross_profit_usd == D('.05')
    cfg = SimpleNamespace(entropy=SimpleNamespace(kind='hl', symbol='SNDK'),
                          hedge=SimpleNamespace(kind='lighter', symbol='SNDK'))
    if accepted:
        pair, authorization = validate_launch(args, cfg, settings)
        assert pair == raw['pair_identity']
        assert authorization.max_position_cycles == 100
        assert authorization.loss_budget_usd == D('3')
        assert authorization.max_notional_usd == D('200')
    else:
        with pytest.raises(LiveAuthorizationError):
            validate_launch(args, cfg, settings)


@pytest.mark.parametrize('value', ['0', '-.1', 'NaN', False, 'Infinity', '-Infinity', 'disabled'])
def test_single_cycle_loss_requires_positive_finite_value_or_null(value):
    with pytest.raises((ValueError, ArithmeticError)):
        Settings.from_mapping({'single_cycle_review_loss_usd': value})


@pytest.mark.parametrize('value', [D('0'), D('-.1'), D('NaN'), D('Infinity'), D('-Infinity'), False])
def test_direct_settings_rejects_invalid_disabled_loss_values(value):
    with pytest.raises(ValueError):
        Settings(single_cycle_review_loss_usd=value)


@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('profit,allowed', [('.002499', False), ('.0025', True)])
def test_bp_boundary_uses_actual_cycle_fills_and_remaining_depth(
        basis_runtime, direction, profit, allowed):
    runtime, e, h, journal = basis_runtime
    exits = []

    async def close(cycle, **kwargs):
        exits.append(cycle['cycle_id'])

    runtime._basis_exit = close

    async def scenario():
        await runtime.initialize()
        runtime.instrument = replace(runtime.instrument, entropy_fee_bps=D('100'),
                                     hedge_fee_bps=D('200'))
        seed_pair(runtime, e, h, journal, direction, planned_units=2000)
        close_side = direction.entropy_side.opposite
        record_fill(journal, 'current', 'partial-e', 'entropy', close_side, 500,
                    '100.5' if direction is Direction.LONG_ENTROPY else '99.5', reduce_only=True)
        record_fill(journal, 'current', 'partial-h', 'hedge', close_side.opposite,
                    500, '100', reduce_only=True)
        journal.update_live_cycle('current', entropy_units=direction.entropy_side.sign * 500,
                                  hedge_units=-direction.entropy_side.sign * 500,
                                  cash_delta_usd=D('-2') if direction is Direction.LONG_ENTROPY else D('2'),
                                  updated_ts=time.time())
        e.position, h.position = direction.entropy_side.sign * .05, -direction.entropy_side.sign * .05
        remaining_move = (D(profit) - D('.025') - D('.01')) / D('.04')
        if direction is Direction.LONG_ENTROPY:
            depth = [[{'px': '101', 'sz': '.01'},
                      {'px': str(D('100') + remaining_move), 'sz': '1'}],
                     [{'px': '102', 'sz': '1'}]]
        else:
            depth = [[{'px': '98', 'sz': '1'}],
                     [{'px': '99', 'sz': '.01'},
                      {'px': str(D('100') - remaining_move), 'sz': '1'}]]
        e.book.apply_hl(depth)
        h.set_book('100', '100')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert decision.executable_gross_usd == D(profit)
        assert decision.allowed is allowed
        assert (decision.executable_net_usd < 0) is (direction is Direction.LONG_ENTROPY)
        await runtime._resume_cycle(journal.active_live_cycle())
        status = json.loads(runtime.status.path.read_text())
        assert D(status['executable_gross_usd']) == D(profit)
        assert status['normal_exit_min_gross_profit_bps'] == '2.5'
        assert D(status['profit_entry_notional_usd']) == D('10')
        assert D(status['profit_bps']) == D(profit) / D('10') * D('10000')
        assert status['normal_exit_rule'] is allowed
        assert bool(exits) is allowed
        assert e.sent == h.sent == []

    asyncio.run(scenario())


def test_disabled_single_cycle_loss_keeps_three_dollar_batch_gate(basis_runtime):
    runtime, e, h, journal = basis_runtime

    async def scenario():
        await runtime.initialize()
        cycle = seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
        h.set_book('100', '100')
        e.set_book('98', '99')
        assert runtime._mark_to_flat(cycle, runtime._market()) == D('-.2')
        await runtime._resume_cycle(cycle)
        assert not journal.batch_state('batch')['closed_for_entries']
        assert journal.active_live_cycle()['phase'] == 'OPEN'
        e.set_book('70', '71')
        await runtime._resume_cycle(journal.active_live_cycle())
        state = journal.batch_state('batch')
        assert state['closed_for_entries'] and not state['can_open']
        assert D(state['block_value']) == D('-3')
        assert D(state['block_threshold']) == D('-3')
        await runtime._maybe_open(runtime._market(), D('0'))
        assert e.sent == h.sent == []

    asyncio.run(scenario())


def test_old_single_cycle_lock_survives_null_setting_and_same_batch_restart(basis_runtime, tmp_path):
    runtime, e, h, journal = basis_runtime

    async def first_run():
        runtime.settings = replace(runtime.settings, single_cycle_review_loss_usd=D('.1'))
        await runtime.initialize()
        cycle = seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
        e.set_book('98', '99')
        h.set_book('100', '100')
        await runtime._observe_active_risk(cycle)
        state = journal.batch_state('batch')
        assert state['close_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
        assert D(state['block_threshold']) == D('-.1')
        runtime.settings = replace(runtime.settings, single_cycle_review_loss_usd=None)
        await runtime._observe_active_risk(cycle)
        assert journal.batch_state('batch')['close_ts'] == state['close_ts']
        return state['close_ts']

    close_ts = asyncio.run(first_run())
    journal.close()
    reopened = Journal(tmp_path / 'runtime.sqlite', identity='pair-account')
    restarted = fresh_runtime(runtime, reopened)

    async def second_run():
        await restarted.initialize()
        await restarted._resume_cycle(reopened.active_live_cycle())
        await restarted._maybe_open(restarted._market(), D('0'))
        state = reopened.batch_state('batch')
        assert state['closed_for_entries'] and not state['can_open']
        assert state['close_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
        assert state['close_ts'] == close_ts
        assert D(state['block_threshold']) == D('-.1')
        assert e.sent == h.sent == []

    try:
        asyncio.run(second_run())
    finally:
        reopened.close()


def test_disabled_loss_does_not_allow_stale_book_exit(basis_runtime):
    runtime, e, h, journal = basis_runtime

    async def scenario():
        await runtime.initialize()
        cycle = seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
        e.set_book('100.5', '101')
        h.set_book('100', '100')
        assert runtime._basis_exit_decision(cycle, runtime._market(), None).allowed
        e.book.alive_ts = time.time() - 10
        with pytest.raises(RuntimeHalt, match='not fresh'):
            await runtime._resume_cycle(cycle)
        assert e.sent == h.sent == []
        assert journal.active_live_cycle()['phase'] == 'OPEN'

    asyncio.run(scenario())


def test_disabled_loss_keeps_residual_recovery_before_profit_gate(basis_runtime, monkeypatch):
    runtime, e, h, journal = basis_runtime

    def no_normal_check(*args):
        raise AssertionError('residual recovery must precede the 2.5 bp profit gate')

    monkeypatch.setattr(runtime, '_basis_exit_decision', no_normal_check)

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(journal, e, h, e_units=0, h_units=-85)
        h.set_book('100.99', '101')
        await runtime._resume_cycle(cycle)
        assert e.sent == [] and h.sent and all(order['reduce_only'] for order in h.sent)
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None

    asyncio.run(scenario())


@pytest.mark.parametrize('direction', list(Direction))
def test_bp_exit_keeps_parallel_iocs_unknown_gate_and_residual_recovery(basis_runtime, direction):
    runtime, e, h, journal = basis_runtime
    e.fill_fractions, e.unknown_once = [.5], True

    async def scenario():
        await runtime.initialize()
        seed_pair(runtime, e, h, journal, direction)
        e.set_book('100.5' if direction is Direction.LONG_ENTROPY else '99',
                   '101' if direction is Direction.LONG_ENTROPY else '99.5')
        h.set_book('100', '100')
        entered = {key: asyncio.Event() for key in ('entropy', 'hedge')}
        allow_resolution, resolves = [False], []
        original_resolve = e.resolve_order

        async def resolve(order_ref, timeout=None):
            resolves.append(order_ref)
            if not allow_resolution[0]:
                return {'status': 'timeout', 'filled_base': 0, 'avg_px': None,
                        'err': None, 'unresolved': True, 'order_ref': order_ref}
            return await original_resolve(order_ref, timeout=timeout)

        e.resolve_order = resolve
        for key, venue in (('entropy', e), ('hedge', h)):
            original = venue.send_taker

            async def send(key=key, original=original, **kwargs):
                assert kwargs['reduce_only']
                steps = [step for step in journal.live_steps('current') if step['reduce_only']]
                assert {step['venue'] for step in steps} == {'entropy', 'hedge'}
                assert all(step['reduce_only'] for step in steps)
                entered[key].set()
                await asyncio.wait_for(entered['hedge' if key == 'entropy' else 'entropy'].wait(), .5)
                return await original(**kwargs)

            venue.send_taker = send
        await asyncio.wait_for(runtime._resume_cycle(journal.active_live_cycle()), 1)
        assert len(e.sent) == len(h.sent) == 1
        ref = e.sent[0]['order_ref']
        assert [row['order_ref'] for row in journal.pending()] == [ref]
        assert journal.active_live_cycle()['basis_exit_requested_ts'] is not None
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(e.sent) == len(h.sent) == 1
        await runtime._resolve_pending()
        assert resolves and set(resolves) == {ref}
        allow_resolution[0] = True
        e.set_book('99', '99.01')
        await finish_exit(runtime, journal)
        assert e.sent[-1]['qty'] == .05
        assert len(e.sent) == 2 and len(h.sent) == 1
        assert_closed_once(e, h, journal)

    asyncio.run(scenario())
