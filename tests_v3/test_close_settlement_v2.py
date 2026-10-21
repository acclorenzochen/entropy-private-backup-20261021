"""R2/R3 counterexamples run the actual Runtime with offline adapters."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import time

import pytest

from test_runtime import complete_background_audit, setup_runtime, seed_close_cycle
from entropy_arb.v3.core import Side
from entropy_arb.v3.journal import Intent, Result
from entropy_arb.v3.live_host import OrderRefFactory


@pytest.mark.parametrize('fractions', [[1], [.4, 1]])
@pytest.mark.parametrize('legacy_baseline', [False, True])
def test_late_hedge_snapshot_never_repeats_known_reduction(tmp_path, fractions, legacy_baseline):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [30 / 85]
    hedge.fill_fractions = list(fractions)
    original = hedge.fetch_position
    stale = True

    async def position():
        if stale and hedge.sent:
            return -.0085
        return await original()

    hedge.fetch_position = position

    async def scenario():
        nonlocal stale
        await runtime.initialize()
        cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                                 phase='CLOSE_FIRST')
        await runtime._close(cycle)
        count = len(hedge.sent)
        assert count == len(fractions)
        assert all(o['reduce_only'] and o['is_buy'] for o in hedge.sent)
        assert round(hedge.position * 10000) == -55
        if legacy_baseline:
            journal.db.execute('UPDATE live_steps SET baseline_entropy_units=NULL, baseline_hedge_units=NULL')
        for _ in range(3):
            await runtime._resume_cycle(journal.active_live_cycle())
        assert len(hedge.sent) == count
        assert journal.active_live_cycle()['phase'] == 'OPEN'
        assert (journal.active_live_cycle()['entropy_units'],
                journal.active_live_cycle()['hedge_units']) == (55, -55)
        await runtime._audit_positions()
        assert not journal.active_live_cycle()['position_evidence_authoritative']
        assert round(hedge.position * 10000) == -55
        assert len(hedge.sent) == count
        stale = False
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        assert journal.active_live_cycle()['phase'] == 'OPEN'
        assert journal.active_live_cycle()['entropy_units'] == 55
        assert len(hedge.sent) == count

    asyncio.run(scenario())
    journal.close()


def test_full_close_requires_matching_authoritative_zero(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    original = hedge.fetch_position

    async def stale():
        return -.0085 if hedge.sent else await original()

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                                 phase='CLOSE_FIRST')
        hedge.fetch_position = stale
        await runtime._close(cycle)
        assert journal.active_live_cycle() is not None
        assert len(hedge.sent) == 1
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is not None
        hedge.fetch_position = original
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None

    asyncio.run(scenario())
    journal.close()


def test_fresh_close_context_skips_duplicate_position_rpc(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    reads = []
    original_position, original_send = entropy.fetch_position, entropy.send_taker

    async def position():
        reads.append('position')
        return await original_position()

    async def send(**kwargs):
        assert not reads
        return await original_send(**kwargs)

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                                 phase='CLOSE_FIRST')
        await runtime._audit_positions()
        cycle = journal.active_live_cycle()
        context = runtime._context(cycle, runtime._last_position_evidence)
        assert runtime._context_valid(cycle, context)
        assert not runtime._context_valid(cycle, replace(context, hedge_received_ts=0))
        entropy.fetch_position, entropy.send_taker = position, send
        await runtime._close(cycle, new_exit=True, context=context)
        assert reads == []
        assert journal.active_live_cycle() is not None
        await complete_background_audit(runtime)
        assert reads == ['position']
        assert journal.active_live_cycle() is None
        assert runtime._close_context is runtime._fast_close_step_key is None

    asyncio.run(scenario())
    journal.close()


def test_close_completion_ignores_legacy_logical_order_budget(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                                 phase='CLOSE_FIRST')
        def no_legacy_reservation(*args, **kwargs):
            raise AssertionError('reduce-only completion cannot use legacy 6/min quota')
        journal.reserve_request = no_legacy_reservation
        await asyncio.wait_for(runtime._close(cycle), .25)
        assert len(entropy.sent) == len(hedge.sent) == 1
        assert all(order['reduce_only'] for order in entropy.sent + hedge.sent)
        assert not journal.pending()
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None

    asyncio.run(scenario())
    journal.close()


def test_slow_analytics_are_after_fallback_close_handoff(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [30 / 85]
    original = journal.record_execution_benchmark
    events = []

    def slow(**kwargs):
        events.append('analytics')
        assert hedge.sent, 'research work occurred before hedge submission'
        time.sleep(.01)
        return original(**kwargs)

    journal.record_execution_benchmark = slow

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                                 phase='CLOSE_FIRST')
        await runtime._close(cycle)
        assert events and len(hedge.sent) == 1
        assert not runtime._in_handoff
        assert journal.db.execute('PRAGMA synchronous').fetchone()[0] == 2

    asyncio.run(scenario())
    journal.close()


def test_quote_prefix_reprices_every_partial_quantity(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        market = runtime._market()
        for side in (Side.BUY, Side.SELL):
            raw = runtime._quote_snapshot(market, entropy_side=side, units=85, serialize=False)
            assert isinstance(raw['entropy']['asks'], tuple)
            for q in (1, 12, 30, 85):
                quote = runtime._materialize_quote({**raw, 'units': q})
                e, _ = market.entropy.quote(side, q, runtime.instrument.quantity_step)
                h, _ = market.hedge.quote(side.opposite, q, runtime.instrument.quantity_step)
                assert D(quote['basis_bps']) == (e / h - 1) * 10000

    asyncio.run(scenario())
    journal.close()


def test_analytics_absence_does_not_erase_canonical_entry_anchor(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        cycle = journal.active_live_cycle()
        journal.db.execute('DELETE FROM execution_economics_steps')
        journal.db.execute('DELETE FROM cycle_economics')
        journal.db.execute('UPDATE live_cycles SET live_entry_basis_bps=NULL')
        anchor = runtime._live_entry_anchor(journal.active_live_cycle())
        assert anchor and anchor[1] == 'canonical_order_vwap'
        assert cycle['entropy_units'] == -cycle['hedge_units']

    asyncio.run(scenario())
    journal.close()


@pytest.mark.parametrize('error', [RuntimeError, asyncio.CancelledError])
def test_fast_context_cleanup_after_error_or_cancel(tmp_path, error):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, exit_fast_path_enabled=True)
    original = entropy.send_taker

    async def receipt(**kwargs):
        response = await original(**kwargs)
        response.update(status='filled', confirmation_source='exchange_http')
        return response

    async def failure(cycle):
        raise error('injected after durable first fill')

    entropy.send_taker = receipt
    runtime._hedge_known_close_group = failure

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                                 phase='CLOSE_FIRST')
        with pytest.raises(error):
            await runtime._close(cycle, new_exit=True)
        assert runtime._close_context is runtime._fast_close_step_key is None
        assert runtime._fast_close_purpose is runtime._fast_close_order_ref is None
        assert runtime._deferred_fast_close_telemetry is None
        assert not runtime._in_handoff
        assert len(entropy.sent) == 1 and not hedge.sent
        assert journal.active_live_cycle()['phase'] == 'CLOSE_HEDGE'

    asyncio.run(scenario())
    journal.close()


def test_missing_analytics_allows_second_cycle_after_canonical_finalization(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        first = journal.active_live_cycle()['cycle_id']
        journal.db.execute('DELETE FROM execution_economics_steps')
        journal.db.execute('DELETE FROM cycle_economics')
        entropy.set_book(99.60, 99.61)
        await runtime._resume_cycle(journal.active_live_cycle())
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert await runtime._reconcile_cycle_accounting(first) is not None
        assert journal.batch_state('batch')['can_open']
        entropy.set_book(100.10, 100.11)
        await runtime._refresh_flat_lease()
        await runtime._maybe_open(runtime._market(), D('0'))
        second = journal.active_live_cycle()
        assert second['cycle_id'] != first and second['phase'] == 'OPEN'
    asyncio.run(scenario())
    journal.close()


def test_fast_close_next_open_hedge_has_no_old_context(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, exit_fast_path_enabled=True)
    original_send = entropy.send_taker

    async def receipt(**kwargs):
        response = await original_send(**kwargs)
        response.update(status='filled', confirmation_source='exchange_http')
        return response

    entropy.send_taker = receipt

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                                 phase='CLOSE_FIRST')
        await runtime._close(cycle, new_exit=True)
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert runtime._close_context is runtime._fast_close_step_key is None
        # Seed the original unit grid and stable exchange identity as well as
        # the opening fill; net cash cannot stand in for missing gross evidence.
        for venue in (entropy, hedge):
            journal.ensure_live_step(step_key=f'close-risk:seed-{venue.key}',
                cycle_id='close-risk', order_ref=f'seed-{venue.key}', venue=venue.key,
                side='buy' if venue is entropy else 'sell', requested_units=85,
                reduce_only=False, quantity_step=D('.0001'))
            venue.fee_fills.append({'order_ref': f'seed-{venue.key}',
                'exchange_fill_key': f'fake:{venue.key}:seed',
                'ts': time.time(), 'side': 'buy' if venue is entropy else 'sell',
                'qty': .0085, 'price': 100., 'fee_usd': 0})
        assert await runtime._reconcile_cycle_accounting('close-risk') is not None
        original_snapshot = runtime.brokers['hedge'].position_snapshot
        async def no_stale_force(*, force_rest=False):
            assert not force_rest
            return await original_snapshot(force_rest=force_rest)
        await runtime._refresh_flat_lease()
        runtime.brokers['hedge'].position_snapshot = no_stale_force
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        assert active['phase'] == 'OPEN'
        assert active['entropy_units'] == -active['hedge_units'] != 0
        for step in journal.live_steps(active['cycle_id']):
            assert not (step['parent_step_key'] or '').startswith('close-risk:')

    asyncio.run(scenario())
    journal.close()


@pytest.mark.parametrize('boundary', ['registered', 'prepared', 'terminal_unapplied', 'hedge_registered'])
def test_close_additional_persist_boundaries_restart_without_duplicate(tmp_path, boundary):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    async def seed():
        await runtime.initialize()
        cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                                 phase='CLOSE_FIRST')
        key = 'close-risk:close_first_0:0'
        ref = OrderRefFactory.create('hl', deployment='test', cycle_id='close-risk',
                                     leg='close_first_0:0')
        journal.ensure_live_step(step_key=key, cycle_id='close-risk', order_ref=ref,
            venue='entropy', side='sell', requested_units=85, reduce_only=True,
            execution_group_id=key)
        if boundary != 'registered':
            journal.prepare(Intent(ref, 'close-risk', 'entropy', Side.SELL, 85, D('100'), True))
        if boundary in {'terminal_unapplied', 'hedge_registered'}:
            journal.mark_submitting(ref)
            journal.record(ref, Result(True, 30, D('100'), D('0')))
        if boundary == 'hedge_registered':
            journal.apply_live_step(key, result_units=30, cash_delta_usd=D('.3'),
                accounting_final=True, entropy_units=55, hedge_units=-85,
                next_phase='CLOSE_HEDGE', updated_ts=time.time())
            hkey = 'close-risk:close_hedge_1:0'
            href = OrderRefFactory.create('lighter', deployment='test', cycle_id='close-risk',
                                         leg='close_hedge_1:0')
            journal.ensure_live_step(step_key=hkey, cycle_id='close-risk', order_ref=href,
                venue='hedge', side='buy', requested_units=30, reduce_only=True,
                execution_group_id=key, parent_step_key=key)
    asyncio.run(seed())
    journal.close()
    restarted, e2, h2, j2 = setup_runtime(tmp_path)
    positive = boundary in {'terminal_unapplied', 'hedge_registered'}
    e2.position, h2.position = (.0055 if positive else .0085), -.0085
    async def resume():
        await restarted.initialize()
        await restarted._resolve_pending()
        await restarted._audit_positions()
        await restarted._resume_cycle(j2.active_live_cycle())
        assert not e2.sent
        assert len(h2.sent) == int(positive)
        if positive:
            assert h2.sent[0]['qty'] == .003 and h2.sent[0]['reduce_only']
        current = j2.active_live_cycle()
        assert (current['entropy_units'], current['hedge_units']) == (
            (55, -55) if positive else (85, -85))
        assert current['phase'] == 'OPEN'
        assert not j2.pending()
    try:
        asyncio.run(resume())
    finally:
        j2.close()
