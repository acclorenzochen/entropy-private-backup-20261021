"""Real local Lighter admission through runtime retries; no SDK/network I/O."""
import asyncio
from decimal import Decimal
import time
from types import SimpleNamespace

import pytest

from entropy_arb.v3 import runtime as runtime_module
from entropy_arb.v3.runtime import RuntimeVenues
from tests_v3.test_lighter_request_integration import (
    Response, account_payload, install_signer, make_venue, no_network,
)
from tests_v3.test_runtime import setup_runtime


def connected_runtime(tmp_path, monkeypatch, *, fractions=(1,), defer_once=False):
    runtime, entropy, original_hedge, journal = setup_runtime(tmp_path)
    hedge, session, budget, clock = make_venue(
        monkeypatch, lambda *args: Response(account_payload()), feed=True)
    clock.wall = time.time()
    # Only the resource clock moves in the tests; book/evidence wall times
    # remain current, so quota expiry cannot manufacture a stale market.
    monkeypatch.setattr(runtime_module, 'time', SimpleNamespace(
        time=time.time, monotonic=clock.monotonic,
        monotonic_ns=lambda: int(clock.monotonic() * 1_000_000_000)))
    hedge.book = original_hedge.book
    hedge.min_base = original_hedge.min_base
    hedge.min_quote = original_hedge.min_quote
    remaining = list(fractions)
    once = [defer_once]

    def before_order():
        if once[0]:
            once[0] = False
            # Simulate a concurrent response opening the real shared limiter
            # after preparation but before this order's POST starts.
            budget.defer(429, {})

    async def settle(method, path):
        if method != 'POST':
            return
        order = signer.orders[-1]
        fraction = remaining.pop(0) if remaining else 1
        filled_amount = int(Decimal(order['base_amount']) * Decimal(str(fraction)))
        filled = filled_amount / 10 ** hedge.size_decimals
        hedge.position += -filled if order['is_ask'] else filled
        hedge.orders_feed._resolve(order['client_order_index'], {
            'status': 'filled' if filled_amount == order['base_amount']
                      else 'partial' if filled_amount else 'canceled',
            'filled_base': filled,
            'avg_px': order['price'] / 10 ** hedge.price_decimals if filled else None,
        })

    signer, rest = install_signer(hedge, callback=settle, before_order=before_order)

    async def local_lifecycle():
        assert hedge.signer is signer

    # Trading/signing still uses the real venue adapter and wrapped budget.
    # Startup is already represented by this injected fake signer/feed.
    hedge.load_market = hedge.prepare_signer = local_lifecycle
    hedge.start_tasks = lambda *args, **kwargs: []
    feed = hedge.orders_feed
    feed.connection_generation = 1
    feed.ready.set()
    feed.last_liveness_ts = clock.time()
    feed._handle_account({
        'type': 'subscribed/account_all',
        'positions': {'139': {'market_id': 139, 'position': '0', 'sign': 1}},
        'assets': {'3': {'margin_balance': '100', 'multiplier': '1'}},
    })
    runtime.venues = RuntimeVenues(entropy, hedge)
    tokens, preflights, admissions = [], [], []
    real_prepare = hedge.prepare_dispatch
    real_reserve = hedge.reserve_completion

    async def tracked_prepare(**kwargs):
        admissions.append(dict(kwargs))
        token = await real_prepare(**kwargs)
        tokens.append(token)
        return token

    def tracked_reserve():
        ticket = real_reserve()
        preflights.append(ticket)
        return ticket

    hedge.prepare_dispatch = tracked_prepare
    hedge.reserve_completion = tracked_reserve
    return SimpleNamespace(runtime=runtime, entropy=entropy, hedge=hedge, journal=journal,
                           signer=signer, rest=rest, session=session, budget=budget,
                           clock=clock, tokens=tokens, preflights=preflights,
                           admissions=admissions)


@pytest.mark.parametrize('first_fill', [.25, 0])
def test_second_hedge_attempt_uses_new_physical_ticket_after_partial_or_zero(
        tmp_path, monkeypatch, first_fill):
    context = connected_runtime(tmp_path, monkeypatch, fractions=(first_fill, 1))

    async def scenario():
        await context.runtime.initialize()
        started = context.budget.snapshot()['counters']['started']
        await context.runtime._maybe_open(context.runtime._market(), Decimal('0'))
        await context.hedge.wait_for_submission()
        cycle = context.journal.active_live_cycle()
        assert cycle['phase'] == 'OPEN'
        assert cycle['entropy_units'] == -cycle['hedge_units'] != 0
        assert len(context.entropy.sent) == 1 and len(context.signer.orders) == 2
        assert len(context.tokens) == 2 and len(context.preflights) == 1
        assert context.tokens[0].ticket is context.preflights[0]
        assert context.tokens[1].ticket is not context.tokens[0].ticket
        assert all(token.released and token.ticket.remaining == 0 for token in context.tokens)
        hedge_orders = [row for row in context.journal.cycle_orders(cycle['cycle_id'])
                        if row['venue'] == 'hedge']
        assert len(hedge_orders) == 2 and all(row['state'] == 'TERMINAL' for row in hedge_orders)
        assert hedge_orders[0]['filled_units'] == int(cycle['planned_units'] * first_fill)
        assert sum(row['filled_units'] for row in hedge_orders) == abs(cycle['entropy_units'])
        assert context.signer.orders[1]['base_amount'] == (
            context.signer.orders[0]['base_amount'] - hedge_orders[0]['filled_units'])
        assert context.budget.snapshot()['counters']['started'] - started == 2
        assert context.budget.snapshot()['reserved_units'] == 0
        assert not context.hedge._submission_lock.locked()
        assert context.journal.pending() == [] and context.runtime._completion_ticket is None
        assert context.journal.db.execute(
            "SELECT COUNT(*) FROM request_events WHERE venue='hedge'").fetchone()[0] == 0

    try:
        asyncio.run(scenario())
    finally:
        context.journal.close()


def test_released_unsent_ticket_is_not_reused_when_resource_retry_wakes(tmp_path, monkeypatch):
    context = connected_runtime(tmp_path, monkeypatch, defer_once=True)

    async def scenario():
        await context.runtime.initialize()
        started = context.budget.snapshot()['counters']['started']
        await context.runtime._maybe_open(context.runtime._market(), Decimal('0'))
        await context.hedge.wait_for_submission()
        cycle = context.journal.active_live_cycle()
        assert cycle['entropy_units'] != 0 and cycle['hedge_units'] == 0
        assert len(context.tokens) == 1 and context.preflights[0].remaining == 0
        assert context.tokens[0].released and context.rest.calls == []
        assert context.journal.pending() == []
        assert context.budget.snapshot()['counters']['started'] == started
        ref = context.signer.orders[0]['client_order_index']
        assert not [row for row in context.journal.live_steps(cycle['cycle_id'])
                    if ':open_hedge:' in row['step_key']]
        # A premature wake must not touch either SDK or a consumed ticket.
        await context.runtime._open_hedge(cycle)
        assert len(context.signer.orders) == 1 and len(context.tokens) == 1
        context.clock.mono += 61
        await context.runtime._open_hedge(context.journal.active_live_cycle())
        await context.hedge.wait_for_submission()
        cycle = context.journal.active_live_cycle()
        assert cycle['phase'] == 'OPEN' and cycle['entropy_units'] == -cycle['hedge_units']
        assert len(context.tokens) == 2
        assert context.tokens[1].ticket is not context.tokens[0].ticket
        assert context.signer.orders[1]['client_order_index'] == ref
        assert context.rest.calls == [('POST', '/api/v1/sendTx')]
        assert context.budget.snapshot()['counters']['started'] - started == 1
        assert context.budget.snapshot()['reserved_units'] == 0
        assert context.runtime._completion_ticket is None and context.journal.pending() == []
        assert not context.hedge._submission_lock.locked()
        assert len([row for row in context.journal.live_steps(cycle['cycle_id'])
                    if ':open_hedge:' in row['step_key']]) == 1
        assert context.journal.db.execute(
            "SELECT COUNT(*) FROM request_events WHERE venue='hedge'").fetchone()[0] == 0

    try:
        asyncio.run(scenario())
    finally:
        context.journal.close()


@pytest.mark.parametrize('cold,slots', [(False, 0), (True, 1)])
def test_completion_shortage_cannot_send_entropy_first_leg(tmp_path, monkeypatch, cold, slots):
    context = connected_runtime(tmp_path, monkeypatch)

    async def scenario():
        await context.runtime.initialize()
        if cold:
            context.signer.nonce_manager.nonce.clear()
        used = context.budget.snapshot()['used']
        context.budget.reserve(context.budget.limit - used - slots, priority=0)
        await context.runtime._maybe_open(context.runtime._market(), Decimal('0'))
        assert context.entropy.sent == context.signer.orders == context.rest.calls == []
        assert context.journal.active_live_cycle() is None and context.journal.pending() == []
        assert context.preflights == context.tokens == []
        assert context.runtime.status.current_snapshot['reason'] == 'completion_resource_unavailable'
        assert context.journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 0
        assert context.runtime._completion_ticket is None

    try:
        asyncio.run(scenario())
    finally:
        context.journal.close()
