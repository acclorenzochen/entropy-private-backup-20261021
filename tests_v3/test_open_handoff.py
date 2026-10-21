"""Offline regressions for event-backed opening and background account audits."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import time
from types import SimpleNamespace

import pytest

from entropy_arb.venue_hl import HLOrdersFeed
from entropy_arb.venue_lighter import AccountOrdersFeed, LighterVenue
from entropy_arb.lighter_requests import RequestBudget, RequestDeferred
from entropy_arb.v3 import runtime as runtime_module
from tests_v3.test_runtime import setup_runtime


def streams(entropy, hedge):
    entropy.orders_feed = HLOrdersFeed('test', 'unused', '0xabc', 'SNDK')
    hedge.orders_feed = AccountOrdersFeed('test', 'unused', 139, 42, None)
    hedge.market_id = 139
    hedge._position_from_rows = LighterVenue._position_from_rows
    for feed in (entropy.orders_feed, hedge.orders_feed):
        feed.connection_generation = 1
        feed.ready.set()
    hedge.orders_feed.last_liveness_ts = time.time()
    hedge.orders_feed._handle_account({
        'type': 'subscribed/account_all',
        'positions': {'139': {'market_id': 139, 'position': '0', 'sign': 1}},
        'assets': {'3': {'margin_balance': '100', 'multiplier': '1'}},
    })


def external_fill(entropy):
    entropy.position += .001
    entropy.orders_feed._handle_fills({'fills': [{
        'coin': 'SNDK', 'oid': 999, 'tid': 1, 'sz': '.001', 'px': '100'}]})


async def open_counted(runtime, entropy, hedge, journal, *, failure=None):
    counts = {'entropy': 0, 'hedge': 0}
    originals = (entropy.fetch_position, hedge.fetch_position)
    campaign = runtime._campaign

    async def e_position():
        counts['entropy'] += 1
        value = await originals[0]()
        if entropy.sent and failure == 'delayed_settlement':
            return 0
        if entropy.sent and failure == 'snapshot_conflict':
            return value + .001
        if entropy.sent and failure == 'event_during_snapshot':
            external_fill(entropy)
        return value

    async def h_position():
        counts['hedge'] += 1
        return await originals[1]()

    async def mutate_after_first(*args, **kwargs):
        result = await campaign(*args, **kwargs)
        if kwargs['purpose'] != 'open_first':
            return result
        if failure in ('stale', 'applied_watermark', 'cycle_watermark', 'order_watermark'):
            # Legacy handoff contexts are no longer an opening gate. A local
            # canonical terminal remains usable after this snapshot ages.
            evidence = runtime._last_position_evidence
            changes = {
                'stale': {'observed_ts': time.time() - 2, 'entropy_received_ts': time.time() - 2},
                'applied_watermark': {'entropy_units': 0, 'hedge_units': 0},
                'cycle_watermark': {'cycle_id': 'different-cycle'},
                'order_watermark': {'stream_watermark': ()},
            }
            runtime._last_position_evidence = replace(evidence, **changes[failure])
            assert runtime._open_handoff is None
        elif failure == 'reconnect':
            hedge.orders_feed.connection_generation += 1
        elif failure == 'disconnect':
            entropy.orders_feed.ready.clear()
        elif failure == 'external_fill':
            external_fill(entropy)
        elif failure == 'hedge_account_change':
            hedge.position += .001
            hedge.orders_feed._handle_account({
                'type': 'update/account_all',
                'positions': {'139': {'market_id': 139, 'position': '.001', 'sign': 1}},
            })
        elif failure == 'out_of_order':
            entropy.orders_feed._handle_orders([{'order': {
                'coin': 'SNDK', 'oid': 999, 'cloid': '0x999', 'origSz': '1', 'sz': '1'},
                'status': 'open'}])
        return result

    entropy.fetch_position, hedge.fetch_position = e_position, h_position
    runtime._campaign = mutate_after_first
    await runtime.initialize()
    baseline_reads = dict(counts)
    await runtime._maybe_open(runtime._market(), D('0'))
    assert counts == baseline_reads, 'order path performed a synchronous account query'
    assert runtime._open_handoff is None
    return {key: counts[key] - baseline_reads[key] for key in counts}


@pytest.mark.parametrize('fraction', [1, .5])
def test_terminal_open_uses_no_position_probes_and_hedges_exact_fill(tmp_path, fraction):
    for enabled in (False, True):
        runtime, entropy, hedge, journal = setup_runtime(tmp_path / str(enabled))
        entropy.fill_fractions = [fraction]
        if enabled:
            streams(entropy, hedge)
        async def scenario():
            assert await open_counted(runtime, entropy, hedge, journal) == {
                'entropy': 0, 'hedge': 0}
            active = journal.active_live_cycle()
            assert active['phase'] == 'OPEN'
            assert active['entropy_units'] == -active['hedge_units'] != 0
            assert len(entropy.sent) == len(hedge.sent) == 1
            assert hedge.sent[0]['qty'] == entropy.sent[0]['qty'] * fraction
            assert not hedge.sent[0]['reduce_only']
        try:
            asyncio.run(scenario())
        finally:
            journal.close()


@pytest.mark.parametrize('failure', [
    'stale', 'reconnect', 'disconnect', 'external_fill', 'hedge_account_change', 'out_of_order',
    'applied_watermark', 'cycle_watermark', 'order_watermark',
    'delayed_settlement', 'snapshot_conflict', 'event_during_snapshot',
])
def test_open_local_evidence_blocks_known_ambiguity_without_position_fallback(tmp_path, failure):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)
    async def scenario():
        assert await open_counted(runtime, entropy, hedge, journal, failure=failure) == {
            'entropy': 0, 'hedge': 0}
        if failure in {'reconnect', 'disconnect', 'external_fill', 'hedge_account_change', 'out_of_order'}:
            assert hedge.sent == [], 'known account uncertainty created new hedge exposure'
            assert len(entropy.sent) >= 2
            assert all(order['reduce_only'] for order in entropy.sent[1:])
            assert entropy.sent[1]['qty'] == entropy.sent[0]['qty']
            if failure == 'external_fill':
                assert entropy.position == 0
                assert journal.active_live_cycle() is not None
            assert not runtime._basis_state_ready()
        else:
            assert len(entropy.sent) == len(hedge.sent) == 1
            assert journal.active_live_cycle()['phase'] == 'OPEN'
            if failure in {'delayed_settlement', 'snapshot_conflict', 'event_during_snapshot'}:
                assert not await runtime._audit_positions()
                assert not runtime._basis_state_ready()
                assert len(entropy.sent) == len(hedge.sent) == 1
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_unknown_first_order_has_no_new_exposure_and_resolves_same_id(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)
    entropy.unknown_once = True
    original_resolve = entropy.resolve_order
    resolving = []

    async def unresolved(order_ref, timeout=None):
        resolving.append(order_ref)
        return {'status': 'timeout', 'filled_base': 0, 'avg_px': None,
                'unresolved': True, 'order_ref': order_ref}

    entropy.resolve_order = unresolved
    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        assert runtime._open_handoff is None
        assert len(entropy.sent) == 1 and not hedge.sent
        ref = journal.pending()[0]['order_ref']
        await runtime._resolve_pending()
        assert resolving and set(resolving) == {ref}
        entropy.resolve_order = original_resolve
        await runtime._resolve_pending()
        assert not journal.pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(entropy.sent) == len(hedge.sent) == 1
        active = journal.active_live_cycle()
        assert active['entropy_units'] == -active['hedge_units'] != 0
        assert hedge.sent[0]['qty'] == entropy.sent[0]['qty']
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('failure', [
    'budget_external_fill', 'budget_reconnect', 'budget_expired', 'budget_changed_after_reserve'])
def test_completion_reservation_denial_drops_candidate_before_first_leg(tmp_path, failure):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)
    releases = []
    ticket = RequestBudget().reserve(2, priority=0)
    def reserve():
        if failure in {'budget_external_fill', 'budget_changed_after_reserve'}:
            external_fill(entropy)
        elif failure == 'budget_reconnect':
            hedge.orders_feed.connection_generation += 1
        if failure == 'budget_changed_after_reserve':
            return ticket
        raise RequestDeferred('offline completion capacity', .5)
    hedge.reserve_completion = reserve
    hedge.release_completion = releases.append
    async def scenario():
        await runtime.initialize()
        async def no_account_query():
            raise AssertionError('completion reservation cannot query account state')
        entropy.fetch_position = hedge.fetch_position = no_account_query
        await asyncio.wait_for(runtime._maybe_open(runtime._market(), D('0')), .25)
        assert entropy.sent == hedge.sent == []
        assert not journal.pending()
        active = journal.active_live_cycle()
        if active:
            assert journal.live_steps(active['cycle_id']) == []
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 0
        if failure == 'budget_changed_after_reserve':
            assert releases == [ticket]
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_completion_resource_wait_does_not_queue_signal_or_expire_baseline(tmp_path, monkeypatch):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)
    offset = [0.0]
    monkeypatch.setattr(runtime_module, 'time', SimpleNamespace(
        time=lambda: time.time() + offset[0],
        monotonic=lambda: time.monotonic() + offset[0], monotonic_ns=time.monotonic_ns))
    ticket = RequestBudget().reserve(2, priority=0)
    def reserve():
        if offset[0] < 2:
            raise RequestDeferred('offline completion capacity', .5)
        return ticket
    hedge.reserve_completion = reserve
    hedge.release_completion = lambda value: None
    async def scenario():
        await runtime.initialize()
        async def no_account_query():
            raise AssertionError('opening cannot perform an account query after resource wait')
        entropy.fetch_position = hedge.fetch_position = no_account_query
        await runtime._maybe_open(runtime._market(), D('0'))
        assert entropy.sent == hedge.sent == []
        offset[0] = 2
        entropy.set_book('100', '100.01')
        hedge.set_book('99.99', '100')
        for venue in (entropy, hedge):
            venue.book.alive_ts = venue.book.last_update_ts = time.time() + offset[0]
        await runtime._maybe_open(runtime._market(), D('0'))
        assert entropy.sent == hedge.sent == []
        offset[0] = 3
        entropy.set_book('100.1', '100.11')
        hedge.set_book('99.99', '100')
        for venue in (entropy, hedge):
            venue.book.alive_ts = venue.book.last_update_ts = time.time() + offset[0]
        await runtime._maybe_open(runtime._market(), D('0'))
        assert len(entropy.sent) == len(hedge.sent) == 1
        active = journal.active_live_cycle()
        assert active['entropy_units'] == -active['hedge_units'] != 0
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 1
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('change', ['revision', 'generation', 'elapsed'])
def test_submission_preflight_await_revalidates_local_state_before_hedge(tmp_path, monkeypatch, change):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)
    offset = [0.0]
    monkeypatch.setattr(runtime_module, 'time', SimpleNamespace(
        time=lambda: time.time() + offset[0], monotonic=time.monotonic, monotonic_ns=time.monotonic_ns))
    async def scenario():
        await runtime.initialize()
        entered, release = asyncio.Event(), asyncio.Event()
        original = runtime.brokers['hedge'].prepare_dispatch
        async def preflight(intent, **kwargs):
            entered.set()
            await release.wait()
            await original(intent, **kwargs)
            return SimpleNamespace(release=lambda: None)
        runtime.brokers['hedge'].prepare_dispatch = preflight
        async def change_while_waiting():
            await entered.wait()
            assert len(entropy.sent) == 1 and not hedge.sent
            assert len(journal.live_steps(journal.active_live_cycle()['cycle_id'])) == 1
            if change == 'revision':
                external_fill(entropy)
            elif change == 'generation':
                hedge.orders_feed.connection_generation += 1
            else:
                offset[0] += 2
                for venue in (entropy, hedge):
                    venue.book.alive_ts = venue.book.last_update_ts = time.time() + offset[0]
            release.set()
        async def no_account_query():
            raise AssertionError('nonce wait cannot perform synchronous position fallback')
        entropy.fetch_position = hedge.fetch_position = no_account_query
        changer = asyncio.create_task(change_while_waiting())
        await asyncio.wait_for(runtime._maybe_open(runtime._market(), D('0')), 1)
        await changer
        if change == 'elapsed':
            assert len(entropy.sent) == len(hedge.sent) == 1
            active = journal.active_live_cycle()
            assert active['entropy_units'] == -active['hedge_units'] != 0
        else:
            assert hedge.sent == []
            assert len(entropy.sent) == 1
            assert not runtime._basis_state_ready()
            await runtime._resume_cycle(journal.active_live_cycle())
            assert len(entropy.sent) >= 2
            assert all(order['reduce_only'] for order in entropy.sent[1:])
            assert hedge.sent == []
    try:
        asyncio.run(scenario())
    finally:
        journal.close()
