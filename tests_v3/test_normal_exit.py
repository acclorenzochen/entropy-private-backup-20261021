"""Offline basis exit checks against canonical fills and executable depth."""
import asyncio
from decimal import Decimal as D
import json
import socket
import time

import pytest

from entropy_arb.v3.core import Direction, Side
from entropy_arb.v3.journal import Intent, Result
from entropy_arb.v3.runtime import RuntimeHalt
from test_runtime import complete_background_audit, seed_close_cycle, setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('normal exit tests cannot access network')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


def record_fill(journal, cid, ref, venue, side, units, price, *, reduce_only=False):
    journal.ensure_live_step(step_key=f'{cid}:{ref}:0', cycle_id=cid, order_ref=ref,
                             venue=venue, side=side.value, requested_units=units,
                             reduce_only=reduce_only, quantity_step=D('.0001'))
    journal.prepare(Intent(ref, cid, venue, side, units, D(price), reduce_only))
    journal.mark_submitting(ref)
    journal.record(ref, Result(True, units, D(price), D('0')))
    journal.db.execute("UPDATE live_steps SET applied=1,result_units=?,cash_delta_usd='0' WHERE order_ref=?",
                       (units, ref))


def seed_pair(runtime, e, h, journal, direction, *, planned_units=1000):
    cid = 'current'
    journal.create_live_cycle(
        batch_id='batch', cycle_id=cid, direction=direction.value,
        planned_units=planned_units, reference_bps=D('0'), entry_basis_bps=D('0'),
        started_ts=time.time())
    side = direction.entropy_side
    record_fill(journal, cid, 'open-e', 'entropy', side, 1000, '100')
    record_fill(journal, cid, 'open-h', 'hedge', side.opposite, 1000, '100')
    journal.update_live_cycle(cid, phase='OPEN_HEDGE', entropy_units=side.sign * 1000,
                              hedge_units=-side.sign * 1000, updated_ts=time.time())
    assert journal.lock_basis_profit_basis(cid, quantity_step=runtime.instrument.quantity_step,
                                           locked_ts=time.time())
    e.position, h.position = side.sign * .1, -side.sign * .1
    return journal.active_live_cycle()


@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('profit,allowed', [('0.002499', False), ('0.0025', True)])
def test_runtime_actual_fills_boundary_and_partial_open(tmp_path, direction, profit, allowed):
    runtime, e, h, journal = setup_runtime(tmp_path)
    exits = []

    async def close(cycle, **kwargs):
        exits.append(cycle['cycle_id'])

    runtime._basis_exit = close

    async def scenario():
        await runtime.initialize()
        seed_pair(runtime, e, h, journal, direction, planned_units=2000)
        # Deliberately unrelated net accounting must not substitute for price PnL.
        journal.update_live_cycle('current', cash_delta_usd=D('2'), updated_ts=time.time())
        movement = D(profit) / D('.1')
        e.set_book(D('100') + movement if direction is Direction.LONG_ENTROPY else '99.5',
                   '101' if direction is Direction.LONG_ENTROPY else D('100') - movement)
        h.set_book('100', '100')
        await runtime._resume_cycle(journal.active_live_cycle())
        status = json.loads(runtime.status.path.read_text())
        assert D(status['executable_gross_usd']) == D(profit)
        assert status['normal_exit_rule'] is allowed
        assert bool(exits) is allowed
        assert e.sent == h.sent == []

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('net_cash', ['-20', '20'])
def test_partial_close_realized_plus_remaining_excludes_old_cycle(tmp_path, direction, net_cash):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        journal.begin_cycle('batch', 'old')
        record_fill(journal, 'old', 'old-e', 'entropy', Side.SELL, 1000, '999')
        journal.finish_cycle('old', kind='clean', net_pnl_usd=D('1'),
                             positions_verified_flat=True, all_orders_terminal=True)
        seed_pair(runtime, e, h, journal, direction)
        close_side = direction.entropy_side.opposite
        closed_price = '100.03' if close_side is Side.SELL else '99.97'
        record_fill(journal, 'current', 'partial-e', 'entropy', close_side, 500,
                    closed_price, reduce_only=True)
        record_fill(journal, 'current', 'partial-h', 'hedge', close_side.opposite,
                    500, '100', reduce_only=True)
        journal.update_live_cycle('current', entropy_units=direction.entropy_side.sign * 500,
                                  hedge_units=-direction.entropy_side.sign * 500,
                                  cash_delta_usd=D(net_cash), updated_ts=time.time())
        e.set_book('100.03' if close_side is Side.SELL else '99.5',
                   '101' if close_side is Side.SELL else '99.97')
        h.set_book('100', '100')
        cycle = journal.active_live_cycle()
        decision = runtime._basis_exit_decision(cycle, runtime._market(), None)
        assert decision.executable_gross_usd == D('.003')
        assert decision.allowed
        # Each .0015 portion is below 2.5 bp; their sum clears the fixed $10 basis.
        assert runtime.instrument.quantity(abs(cycle['entropy_units'])) == D('.05')

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_full_depth_not_top_or_mark_and_missing_price_or_position_mismatch_hold(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle = seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
        e.book.apply_hl([
            [{'px': '101', 'sz': '.001'}, {'px': '100', 'sz': '1'}],
            [{'px': '102', 'sz': '1'}]])
        h.set_book('100', '100')
        market = runtime._market()
        decision = runtime._basis_exit_decision(cycle, market, None)
        assert decision.executable_gross_usd == D('.001') and not decision.allowed
        journal.db.execute("UPDATE orders SET avg_px=NULL WHERE order_ref='open-e'")
        assert runtime._basis_exit_decision(cycle, market, None).reason == 'hold_missing_price_evidence'
        journal.db.execute("UPDATE orders SET avg_px='100' WHERE order_ref='open-e'")
        mismatched = {**cycle, 'entropy_units': 999}
        assert not runtime._basis_exit_decision(mismatched, market, None).allowed

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_stale_book_cannot_trigger_normal_exit(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
        e.set_book('101', '102')
        e.book.alive_ts = time.time() - 10
        with pytest.raises(RuntimeHalt, match='not fresh'):
            await runtime._resume_cycle(journal.active_live_cycle())
        assert e.sent == h.sent == []
        assert journal.active_live_cycle()['phase'] == 'OPEN'

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_legacy_zero_fill_retry_rechecks_bp_floor_even_without_basis_anchor(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    e.fill_fractions = [0]
    original_send = e.send_taker

    async def lose_profit(**kwargs):
        result = await original_send(**kwargs)
        e.set_book('100.02499', '101')
        return result

    e.send_taker = lose_profit

    async def scenario():
        await runtime.initialize()
        seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
        e.set_book('100.025', '101')
        h.set_book('100', '100')
        journal.update_live_cycle('current', phase='CLOSE_FIRST', updated_ts=time.time())
        await runtime._close(journal.active_live_cycle(), new_exit=True)
        assert len(e.sent) == 1 and h.sent == []
        assert journal.active_live_cycle()['phase'] == 'OPEN'
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert not decision.allowed and decision.executable_gross_usd == D('.002499')

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_basis_loss_lock_stops_entries_without_forcing_balanced_close(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
        e.set_book('98', '99')
        h.set_book('100', '100')
        await runtime._resume_cycle(journal.active_live_cycle())
        assert journal.batch_state('batch')['close_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
        assert journal.active_live_cycle()['phase'] == 'OPEN'
        assert e.sent == h.sent == []

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_residual_recovery_closes_before_normal_profit_check(tmp_path, monkeypatch):
    runtime, e, h, journal = setup_runtime(tmp_path)

    def no_normal_check(*args):
        raise AssertionError('residual recovery must precede normal profit checks')

    monkeypatch.setattr(runtime, '_basis_exit_decision', no_normal_check)

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(journal, e, h, e_units=0, h_units=-85)
        h.set_book('100.99', '101')
        await runtime._resume_cycle(cycle)
        assert e.sent == [] and h.sent and h.sent[0]['reduce_only']
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('known_units', [500, 1000])
def test_missing_terminal_vwap_cannot_reuse_smaller_partial_fill_price(tmp_path, known_units):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cid = 'partial-evidence'
        journal.create_live_cycle(batch_id='batch', cycle_id=cid, direction='long_entropy',
                                  planned_units=1000, reference_bps=D('0'), entry_basis_bps=D('0'),
                                  started_ts=time.time())
        journal.prepare(Intent('partial-open-e', cid, 'entropy', Side.BUY, 1000, D('100')))
        journal.ensure_live_step(step_key=f'{cid}:partial-open-e:0', cycle_id=cid, order_ref='partial-open-e',
                                 venue='entropy', side='buy', requested_units=1000,
                                 reduce_only=False, quantity_step=D('.0001'))
        journal.mark_submitting('partial-open-e')
        journal.record('partial-open-e', Result(False, known_units, D('100')))
        journal.record('partial-open-e', Result(True, 1000))
        record_fill(journal, cid, 'open-h', 'hedge', Side.SELL, 1000, '100')
        journal.update_live_cycle(cid, phase='OPEN_HEDGE', entropy_units=1000, hedge_units=-1000,
                                  updated_ts=time.time())
        journal.db.execute("UPDATE live_steps SET applied=1,result_units=1000,cash_delta_usd='0' WHERE order_ref='partial-open-e'")
        locked = journal.lock_basis_profit_basis(cid, quantity_step=runtime.instrument.quantity_step,
                                                locked_ts=time.time())
        assert locked is (known_units == 1000)
        if not locked:
            journal.update_live_cycle(cid, phase='OPEN', updated_ts=time.time())
        e.set_book('100.3', '101')
        h.set_book('100', '100')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert decision.allowed is (known_units == 1000)
        assert journal.order('partial-open-e')['avg_px'] == ('100' if known_units == 1000 else None)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_unbalanced_or_wrong_direction_cannot_quote_a_normal_same_quantity_exit(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY, planned_units=1500)
        record_fill(journal, 'current', 'extra-h', 'hedge', Side.SELL, 500, '100')
        journal.update_live_cycle('current', hedge_units=-1500, updated_ts=time.time())
        e.set_book('100.299', '101')
        h.set_book('100', '100')
        decision = runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(), None)
        assert not decision.allowed and decision.executable_gross_usd is None
        record_fill(journal, 'current', 'reduce-h', 'hedge', Side.BUY, 500, '100', reduce_only=True)
        journal.update_live_cycle('current', hedge_units=-1000, updated_ts=time.time())
        e.set_book('100.3', '101')
        wrong_direction = {**journal.active_live_cycle(), 'direction': 'short_entropy'}
        assert runtime._basis_exit_decision(wrong_direction, runtime._market(), None).executable_gross_usd is None

    try:
        asyncio.run(scenario())
    finally:
        journal.close()
