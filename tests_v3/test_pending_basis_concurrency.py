import asyncio
from decimal import Decimal as D
import socket

import pytest

from test_pending_basis_accounting import complete_round, cycle_result, RATES
from test_runtime import complete_background_audit, setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('pending-basis concurrency checks must remain offline')

    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


@pytest.mark.parametrize('late_result', ['final_loss', 'estimate_exhausted'])
def test_zero_fill_retry_rechecks_budget_after_background_accounting(tmp_path, late_result):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    original_send = entropy.send_taker
    entropy.fill_fractions = [0, 1]

    async def scenario():
        await runtime.initialize()
        complete_round(journal, 'old')
        assert runtime._estimate_basis_accounting('old')
        first_zero = asyncio.Event()

        async def send(**kwargs):
            result = await original_send(**kwargs)
            if not result['filled_base']:
                first_zero.set()
                # Model the independent accounting result arriving while the
                # exchange response is still in flight, before retry admission.
                await asyncio.sleep(0)
            return result

        async def late_accounting():
            await first_zero.wait()
            if late_result == 'final_loss':
                journal.finalize_cycle_accounting('old', D('-3.1'))
            else:
                assert journal.estimate_basis_accounting(
                    'old', fee_bps={'entropy': D('100'), 'hedge': D('100')})

        entropy.send_taker = send
        background = asyncio.create_task(late_accounting())
        await runtime._maybe_open(runtime._market(), D('0'))
        await background
        await complete_background_audit(runtime)
        assert len(entropy.sent) == 1, 'new exposure submitted after late budget exhaustion'
        assert hedge.sent == []
        assert entropy.position == hedge.position == 0
        assert not journal.pending()
        assert journal.active_live_cycle() is None
        state = journal.batch_state('batch')
        assert not state['can_open']
        assert state['closed_for_entries'] == (late_result == 'final_loss')

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_late_budget_latch_allows_hedge_of_already_filled_first_leg(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    original_send = entropy.send_taker

    async def scenario():
        await runtime.initialize()
        complete_round(journal, 'old')
        assert runtime._estimate_basis_accounting('old')

        async def send(**kwargs):
            result = await original_send(**kwargs)
            assert result['filled_base'] > 0
            journal.finalize_cycle_accounting('old', D('-3.1'))
            return result

        entropy.send_taker = send
        await runtime._maybe_open(runtime._market(), D('0'))
        assert len(entropy.sent) == len(hedge.sent) == 1
        assert entropy.position == -hedge.position != 0
        assert journal.active_live_cycle()['phase'] == 'OPEN'
        assert not journal.pending()
        assert journal.batch_state('batch')['close_reason'] == 'REALIZED_LOSS_BUDGET'

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('analytics_updated', [False, True])
def test_closed_circle_funding_window_excludes_prior_and_next_circle(tmp_path, analytics_updated):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    reads = []

    async def scenario():
        await runtime.initialize()
        complete_round(journal, 'old', entropy_fee=D('0'), hedge_fee=D('0'))
        assert journal.estimate_basis_accounting('old', fee_bps=RATES)
        for order in journal.cycle_orders('old'):
            venue = entropy if order['venue'] == 'entropy' else hedge
            venue.fee_fills.append({
                'order_ref': order['order_ref'], 'exchange_fill_key': order['order_ref'],
                'qty': D(order['filled_units']), 'price': D(order['avg_px']),
                'side': order['side'], 'ts': 10001, 'fee_usd': D('0')})
        if analytics_updated:
            journal.set_live_entry_basis(
                'old', basis_bps=D('0'), source='canonical_order_vwap', updated_ts=10020)
        journal.create_live_cycle(
            batch_id='batch', cycle_id='next', direction='long_entropy', planned_units=1,
            reference_bps=D('0'), entry_basis_bps=D('0'), started_ts=10005.5)

        for venue in (entropy, hedge):
            async def funding(since, until, venue=venue):
                reads.append((venue.key, since, until))
                events = [(9999, D('1')), (10003, D('.02')), (10006, D('-.5'))]
                return sum((amount for ts, amount in events if since <= ts <= until), D('0')) \
                    if venue is entropy else D('0')
            venue.fetch_funding_payments = funding
        final = await runtime._reconcile_cycle_accounting('old')
        assert reads == [('entropy', 10000, 10005), ('hedge', 10000, 10005)]
        assert final == D('.02')
        assert D(cycle_result(journal, 'old')['pnl_usd']) == D('.02')
        assert journal.active_live_cycle()['cycle_id'] == 'next'

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_known_fee_risk_is_refreshed_before_slow_peer_finishes(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        complete_round(journal, 'old')
        assert runtime._estimate_basis_accounting('old')
        for order in journal.cycle_orders('old'):
            if order['venue'] == 'entropy':
                entropy.fee_fills.append({
                    'order_ref': order['order_ref'], 'exchange_fill_key': order['order_ref'],
                    'qty': D(order['filled_units']), 'price': D(order['avg_px']),
                    'side': order['side'], 'ts': 10001, 'fee_usd': D('4')})
        slow_peer_started = asyncio.Event()

        async def slow_history(*args):
            slow_peer_started.set()
            await asyncio.Future()

        hedge.fetch_fee_fills = slow_history
        background = asyncio.create_task(runtime._reconcile_cycle_accounting('old'))
        try:
            await asyncio.wait_for(slow_peer_started.wait(), .5)
            assert not background.done()
            state = journal.batch_state('batch')
            assert state['known_loss_used_usd'] == D('8')
            assert state['pending_cost_estimate_usd'] == D('0')
            assert not state['can_open']
            assert not cycle_result(journal, 'old')['accounting_final']
            assert cycle_result(journal, 'old')['pnl_usd'] is None
        finally:
            background.cancel()
            with pytest.raises(asyncio.CancelledError):
                await background

    try:
        asyncio.run(scenario())
    finally:
        journal.close()
