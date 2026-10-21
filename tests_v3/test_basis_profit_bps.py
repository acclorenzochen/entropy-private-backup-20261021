"""Offline checks for immutable matched entry notional and cycle price profit."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import json
import time

import pytest

from entropy_arb.v3.core import Direction, Settings, Side, exit_decision
from entropy_arb.v3.journal import Intent, Journal, Result
from tests_v3.test_runtime import setup_runtime


def record(runtime, venue, side, units, price, *, reduce_only=False, terminal=True):
    journal = runtime.journal
    cycle = journal.active_live_cycle()
    ref = f"{cycle['cycle_id']}:{venue}:{len(journal.cycle_orders(cycle['cycle_id']))}"
    intent = Intent(ref, cycle['cycle_id'], venue, side, units, D(price), reduce_only)
    journal.prepare_dispatch(intent, step=dict(step_key=ref,
        quantity_step=runtime.instrument.quantity_step))
    result = Result(terminal, units, D(price), D('0'))
    journal.record(ref, result)
    positions = {key: cycle[f'{key}_units'] for key in ('entropy', 'hedge')}
    positions[venue] += side.sign * units
    journal.apply_live_step(ref, result_units=units,
        cash_delta_usd=-side.sign * units * runtime.instrument.quantity_step * D(price),
        accounting_final=True, entropy_units=positions['entropy'], hedge_units=positions['hedge'],
        next_phase='OPEN_HEDGE' if not reduce_only else 'RECOVERY', updated_ts=time.time())
    runtime.venues.entropy.position = positions['entropy'] * runtime.instrument.quantity_step
    runtime.venues.hedge.position = positions['hedge'] * runtime.instrument.quantity_step
    return ref


def seed(runtime, *, direction=Direction.LONG_ENTROPY, e_units=1000, h_units=1000):
    runtime.journal.create_live_cycle(batch_id=runtime.authorization.batch_id, cycle_id='profit',
        direction=direction.value, planned_units=max(e_units, h_units, 2000),
        reference_bps=D('0'), entry_basis_bps=D('0'), started_ts=time.time())
    record(runtime, 'entropy', direction.entropy_side, e_units, '100')
    record(runtime, 'hedge', direction.entropy_side.opposite, h_units, '100')


def set_remaining_profit(runtime, direction, gross):
    qty = runtime.instrument.quantity(abs(runtime.journal.active_live_cycle()['entropy_units']))
    movement = D(gross) / qty
    price = D('100') + movement * direction.entropy_side.sign
    runtime.venues.entropy.set_book(price if direction is Direction.LONG_ENTROPY else price - 1,
        price + 1 if direction is Direction.LONG_ENTROPY else price)
    runtime.venues.hedge.set_book('100', '100')


def basis(runtime):
    return json.loads(runtime.journal.active_live_cycle()['basis_profit_basis_json'])


@pytest.mark.parametrize('group', ['ordinary', 'A', 'B'])
def test_real_opening_completion_freezes_actual_mean_not_planned_cap(tmp_path, group):
    if group != 'ordinary':
        from tests_v3.test_ab_opening import setup_ab, finish_zero
    runtime, e, h, journal = setup_runtime(tmp_path) if group == 'ordinary' else setup_ab(tmp_path)
    async def scenario():
        await runtime.initialize()
        if group == 'B':
            e.fill_fractions = h.fill_fractions = [0]
            await runtime._maybe_open(runtime._market(), D('0'))
            await finish_zero(runtime)
            journal.rearm_opening_opportunity(runtime.settings.opening_experiment_id)
        e.fill_fractions, h.fill_fractions = [.5], [1]
        await runtime._maybe_open(runtime._market(), D('0'))
        cycle = journal.active_live_cycle()
        assert cycle['phase'] == 'OPEN'
        locked = basis(runtime)
        assert locked['matched_units'] == abs(cycle['entropy_units'])
        assert locked['matched_units'] < cycle['planned_units']
        orders = journal.cycle_orders(cycle['cycle_id'])
        opening = {key: [row for row in orders if row['venue'] == key
                         and not row['reduce_only'] and row['filled_units']]
                   for key in ('entropy', 'hedge')}
        prices = {key: sum((D(row['avg_px']) * row['filled_units'] for row in rows), D('0'))
                       / sum(row['filled_units'] for row in rows) for key, rows in opening.items()}
        expected = (prices['entropy'] + prices['hedge']) * runtime.instrument.quantity(abs(cycle['entropy_units'])) / 2
        assert D(locked['entry_notional_usd']) == expected > 0
        assert expected < runtime.settings.max_notional_usd
        assert all(row['state'] == 'TERMINAL' for row in journal.cycle_orders(cycle['cycle_id']))
        if group == 'A':
            assert any(row['reduce_only'] and row['venue'] == 'hedge'
                       for row in journal.cycle_orders(cycle['cycle_id']))
        assert runtime.settings.exit_min_gross_profit_bps == D('2.5')
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('gross,allowed', [('0.002499', False), ('0.0025', True)])
def test_two_point_five_boundary_both_directions_ignores_cash_fees_funding(tmp_path, direction, gross, allowed):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime, direction=direction)
        runtime._complete_basis_open(journal.active_live_cycle())
        journal.update_live_cycle('profit', cash_delta_usd=D('-99'), updated_ts=time.time())
        runtime.instrument = replace(runtime.instrument, entropy_fee_bps=D('100'), hedge_fee_bps=D('100'))
        set_remaining_profit(runtime, direction, gross)
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert decision.executable_gross_usd == D(gross)
        assert decision.profit_entry_notional_usd == D('10')
        assert decision.profit_bps == D(gross) * 1000
        assert decision.allowed is allowed and decision.executable_net_usd < 0
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('direction', list(Direction))
def test_partial_reduction_keeps_original_denominator_and_realized_price_profit(tmp_path, direction):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime, direction=direction)
        runtime._complete_basis_open(journal.active_live_cycle())
        frozen = journal.active_live_cycle()['basis_profit_basis_json']
        record(runtime, 'entropy', direction.entropy_side.opposite, 500, '100', reduce_only=True)
        record(runtime, 'hedge', direction.entropy_side, 500, '100', reduce_only=True)
        runtime._complete_basis_open(journal.active_live_cycle())
        set_remaining_profit(runtime, direction, '.0015')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert not decision.allowed and decision.profit_bps == D('1.5')
        assert decision.profit_entry_notional_usd == 10
        assert journal.active_live_cycle()['basis_profit_basis_json'] == frozen
        # A completed partial close contributes its realized price cash.
        price = '100.04' if direction is Direction.LONG_ENTROPY else '99.96'
        record(runtime, 'entropy', direction.entropy_side.opposite, 250, price, reduce_only=True)
        record(runtime, 'hedge', direction.entropy_side, 250, '100', reduce_only=True)
        set_remaining_profit(runtime, direction, '.0015')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert decision.executable_gross_usd == D('.0025')
        assert decision.allowed and decision.profit_bps == D('2.5')
        assert journal.active_live_cycle()['basis_profit_basis_json'] == frozen
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('excess', ['entropy', 'hedge'])
@pytest.mark.parametrize('repair_profit', [D('.003'), D('-.003')])
def test_prelock_excess_repair_cash_counts_but_excess_notional_does_not(tmp_path, direction, excess, repair_profit):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime, direction=direction, e_units=2000 if excess == 'entropy' else 1000,
             h_units=2000 if excess == 'hedge' else 1000)
        opening_side = direction.entropy_side if excess == 'entropy' else direction.entropy_side.opposite
        close_side = opening_side.opposite
        close_price = D('100') - close_side.sign * repair_profit / D('.1')
        record(runtime, excess, close_side, 1000, close_price, reduce_only=True)
        runtime._complete_basis_open(journal.active_live_cycle())
        locked = basis(runtime)
        assert locked['matched_units'] == 1000 and D(locked['entry_notional_usd']) == 10
        e.set_book('100', '100')
        h.set_book('100', '100')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert decision.executable_gross_usd == repair_profit
        assert decision.profit_bps == repair_profit * 1000
        assert decision.allowed is (repair_profit > 0)
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_legacy_open_missing_denominator_is_never_rebuilt_from_remaining_position(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime)
        journal.db.execute("UPDATE live_cycles SET basis_profit_basis_json=NULL WHERE cycle_id='profit'")
        journal.update_live_cycle('profit', phase='OPEN', updated_ts=time.time())
        runtime._complete_basis_open(journal.active_live_cycle())
        set_remaining_profit(runtime, Direction.LONG_ENTROPY, '1')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert not decision.allowed and decision.reason == 'hold_missing_entry_notional'
        assert journal.active_live_cycle()['basis_profit_basis_json'] is None
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_legacy_recovery_with_partial_close_cannot_freeze_residual_notional(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime)
        record(runtime, 'entropy', Side.SELL, 500, '100', reduce_only=True)
        record(runtime, 'hedge', Side.BUY, 500, '100', reduce_only=True)
        journal.db.execute("UPDATE live_cycles SET basis_profit_basis_json=NULL WHERE cycle_id='profit'")
        assert journal.active_live_cycle()['phase'] == 'RECOVERY'
        runtime._complete_basis_open(journal.active_live_cycle())
        set_remaining_profit(runtime, Direction.LONG_ENTROPY, '1')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert not decision.allowed and decision.reason == 'hold_missing_entry_notional'
        assert journal.active_live_cycle()['basis_profit_basis_json'] is None
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_new_pending_initial_repair_restarts_and_freezes_once(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime, e_units=2000)
        record(runtime, 'entropy', Side.SELL, 1000, '100.03', reduce_only=True)
        assert journal.active_live_cycle()['basis_profit_basis_json'] == journal.BASIS_PROFIT_PENDING
        assert runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None).reason == 'hold_entry_notional_not_locked'
        journal.close()
        resumed = Journal(tmp_path / 'runtime.sqlite', identity='pair-account')
        runtime.journal = resumed
        try:
            runtime._complete_basis_open(resumed.active_live_cycle())
            frozen = resumed.active_live_cycle()['basis_profit_basis_json']
            assert basis(runtime)['matched_units'] == 1000
            assert D(basis(runtime)['entry_notional_usd']) == 10
            runtime._complete_basis_open(resumed.active_live_cycle())
            assert resumed.active_live_cycle()['basis_profit_basis_json'] == frozen
            e.set_book('100', '100')
            h.set_book('100', '100')
            decision = runtime._basis_exit_decision(resumed.active_live_cycle(), runtime._market(), None)
            assert decision.allowed and decision.profit_bps == 3
        finally:
            resumed.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('bad', ['null', '{}', '[]', 'true', '{"entry_notional_usd":"0"}', 'bad-json'])
def test_invalid_persisted_basis_blocks_instead_of_guessing(tmp_path, bad):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime)
        runtime._complete_basis_open(journal.active_live_cycle())
        journal.db.execute('UPDATE live_cycles SET basis_profit_basis_json=? WHERE cycle_id=?', (bad, 'profit'))
        set_remaining_profit(runtime, Direction.LONG_ENTROPY, '1')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert not decision.allowed and decision.profit_bps is None
        assert decision.reason == 'hold_invalid_entry_notional_evidence'
        assert journal.active_live_cycle()['basis_profit_basis_json'] == bad
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_frozen_price_correction_or_grid_change_requires_review_but_decimal_format_does_not(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime)
        runtime._complete_basis_open(journal.active_live_cycle())
        frozen = journal.active_live_cycle()['basis_profit_basis_json']
        ref = journal.cycle_orders('profit')[0]['order_ref']
        set_remaining_profit(runtime, Direction.LONG_ENTROPY, '1')
        journal.db.execute('UPDATE orders SET avg_px=? WHERE order_ref=?', ('100.000', ref))
        assert runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None).allowed
        journal.db.execute('UPDATE orders SET avg_px=? WHERE order_ref=?', ('100.01', ref))
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert not decision.allowed and decision.reason == 'hold_entry_notional_evidence_changed'
        journal.db.execute('UPDATE orders SET avg_px=? WHERE order_ref=?', ('100', ref))
        journal.db.execute('UPDATE live_steps SET quantity_step=? WHERE order_ref=?', ('.001', ref))
        assert runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None).reason == 'hold_entry_notional_evidence_changed'
        assert journal.active_live_cycle()['basis_profit_basis_json'] == frozen
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_exit_latch_survives_missing_basis_market_reversal_and_open_completion(tmp_path, monkeypatch):
    runtime, e, h, journal = setup_runtime(tmp_path)
    called = []
    async def closing(cycle, **kwargs):
        called.append(cycle['cycle_id'])
    async def scenario():
        await runtime.initialize()
        seed(runtime)
        runtime._complete_basis_open(journal.active_live_cycle())
        journal.latch_basis_exit('profit', requested_ts=time.time())
        requested = journal.active_live_cycle()['basis_exit_requested_ts']
        journal.db.execute("UPDATE live_cycles SET basis_profit_basis_json=NULL WHERE cycle_id='profit'")
        e.set_book('99', '99.1')
        runtime._complete_basis_open(journal.active_live_cycle())
        monkeypatch.setattr(runtime, '_basis_exit', closing)
        await runtime._resume_cycle_inner(journal.active_live_cycle())
        assert called == ['profit'] and journal.active_live_cycle()['phase'] == 'BASIS_EXIT'
        assert journal.active_live_cycle()['basis_exit_requested_ts'] == requested
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_basis_lock_and_OPEN_are_atomic_and_resume_preserves_same_json(tmp_path, monkeypatch):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime)
        original = json.dumps
        def crash(*args, **kwargs):
            raise OSError('fault before basis/OPEN commit')
        monkeypatch.setattr('entropy_arb.v3.journal.json.dumps', crash)
        with pytest.raises(OSError):
            journal.lock_basis_profit_basis('profit', quantity_step=runtime.instrument.quantity_step, locked_ts=time.time())
        assert journal.active_live_cycle()['basis_profit_basis_json'] == journal.BASIS_PROFIT_PENDING
        assert journal.active_live_cycle()['phase'] == 'OPEN_HEDGE'
        monkeypatch.setattr('entropy_arb.v3.journal.json.dumps', original)
        runtime._complete_basis_open(journal.active_live_cycle())
        frozen = journal.active_live_cycle()['basis_profit_basis_json']
        journal.close()
        resumed = Journal(tmp_path / 'runtime.sqlite', identity='pair-account')
        runtime.journal = resumed
        try:
            runtime._complete_basis_open(resumed.active_live_cycle())
            assert resumed.active_live_cycle()['basis_profit_basis_json'] == frozen
            set_remaining_profit(runtime, Direction.LONG_ENTROPY, '.0025')
            assert runtime._basis_exit_decision(resumed.active_live_cycle(), runtime._market(), None).allowed
        finally:
            resumed.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('value', [None, D('0'), D('-1'), D('NaN'), True, 'bad'])
def test_core_never_guesses_denominator(value, tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        decision = exit_decision(runtime._market(), runtime.instrument, runtime.settings,
            Direction.LONG_ENTROPY, 1000, None, D('100'),
            accrued_price_cash_usd=D('1'), profit_entry_notional_usd=value)
        assert not decision.allowed and decision.reason == 'hold_missing_entry_notional'
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('value', [D('0'), D('-1'), D('NaN'), True, 2.5])
def test_bp_threshold_requires_positive_finite_decimal(value):
    with pytest.raises(ValueError):
        Settings(exit_min_gross_profit_bps=value)


@pytest.mark.parametrize('units,notional', [(1500, D('15')), (20000, D('200'))])
@pytest.mark.parametrize('bps,allowed', [(D('2.499'), False), (D('2.5'), True)])
def test_fifteen_and_two_hundred_notional_use_same_bp_boundary(tmp_path, units, notional, bps, allowed):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime, e_units=units, h_units=units)
        runtime._complete_basis_open(journal.active_live_cycle())
        gross = notional * bps / 10000
        set_remaining_profit(runtime, Direction.LONG_ENTROPY, gross)
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert decision.profit_entry_notional_usd == notional
        assert decision.profit_bps == bps and decision.allowed is allowed
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('grid', [None, '.001'])
def test_postlock_close_requires_its_own_canonical_quantity_grid(tmp_path, grid):
    runtime, e, h, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        seed(runtime)
        runtime._complete_basis_open(journal.active_live_cycle())
        frozen = journal.active_live_cycle()['basis_profit_basis_json']
        ref = record(runtime, 'entropy', Side.SELL, 500, '101', reduce_only=True)
        record(runtime, 'hedge', Side.BUY, 500, '100', reduce_only=True)
        journal.db.execute('UPDATE live_steps SET quantity_step=? WHERE order_ref=?', (grid, ref))
        e.set_book('100', '100')
        h.set_book('100', '100')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert not decision.allowed and decision.reason == 'hold_entry_notional_evidence_changed'
        assert journal.active_live_cycle()['basis_profit_basis_json'] == frozen
    try:
        asyncio.run(scenario())
    finally:
        journal.close()
