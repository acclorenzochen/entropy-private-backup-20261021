import asyncio
from decimal import Decimal as D
import json
import socket

import pytest

from entropy_arb.funding import FundingRiskSnapshot
from entropy_arb.v3.core import Direction, Side
from entropy_arb.v3.journal import Intent, Result
from entropy_arb.v3 import runtime as runtime_module
from test_funding_hedge_runtime import carry, fresh_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('offline risk-cache tests cannot access network')

    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


def risk_snapshot(venue, clock, *, ts=None, quantity=None, liquidation=None):
    qty = D(str(venue.position)) if quantity is None else quantity
    liq = (D('95') if qty > 0 else D('105')) if liquidation is None else liquidation
    return FundingRiskSnapshot(qty, D('100'), liq,
                               clock[0] if ts is None else ts, 'fake:readonly')


def exit_reason(journal, cycle):
    return journal.funding_state(cycle['cycle_id'])['exit_reason']


async def drain_risk(runtime):
    await asyncio.gather(*runtime._funding_risk_tasks.values(), return_exceptions=True)
    runtime._flush_telemetry()


def test_timeout_is_ten_seconds_with_original_freshness_and_distance(carry):
    runtime, *_ = carry
    assert runtime_module.FUNDING_RISK_REQUEST_TIMEOUT_SECONDS == 10.0
    assert runtime.settings.funding_risk_stale_seconds == 10
    assert runtime.settings.funding_liquidation_distance == D('.02')


def test_eight_second_success_preserves_source_time(carry, monkeypatch):
    runtime, e, h, journal, clock, tick, open_pair = carry
    original_wait = asyncio.wait
    observed_timeouts = []

    async def controlled_wait(awaitables, *, timeout=None, return_when=asyncio.ALL_COMPLETED):
        if any(getattr(task.get_coro(), 'cr_code', None) is not None
               and task.get_coro().cr_code.co_name == 'slow_risk' for task in awaitables):
            observed_timeouts.append(timeout)
        return await original_wait(awaitables, timeout=timeout, return_when=return_when)

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        source_ts = clock[0]

        async def slow_risk():
            await asyncio.sleep(0)
            clock[0] = source_ts + 8.18
            return risk_snapshot(e, clock, ts=source_ts)

        e.fetch_funding_risk = slow_risk
        monkeypatch.setattr(asyncio, 'wait', controlled_wait)
        await runtime._check_funding_liquidation(cycle)
        assert observed_timeouts == [10.0]
        assert not exit_reason(journal, cycle)
        leg = runtime.status.base['funding_risk']['legs']['entropy']
        assert leg['source_ts'] == source_ts
        assert clock[0] - leg['source_ts'] == pytest.approx(8.18)
        assert e.position == -h.position == 1.99

    asyncio.run(scenario())


def test_timeout_without_cache_exits_and_cancels_read(carry, monkeypatch):
    runtime, e, h, journal, clock, tick, open_pair = carry
    monkeypatch.setattr(runtime_module, 'FUNDING_RISK_REQUEST_TIMEOUT_SECONDS', .015)

    async def scenario():
        cycle = await open_pair()
        runtime._funding_risk_cache.clear()
        tick(10003)
        cancelled = asyncio.Event()

        async def hung():
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        e.fetch_funding_risk = hung
        await asyncio.wait_for(runtime._check_funding_liquidation(cycle), .3)
        assert cancelled.is_set()
        assert exit_reason(journal, cycle) == 'risk_data_unavailable'

    asyncio.run(scenario())


@pytest.mark.parametrize('failed_leg', ['entropy', 'hedge'])
@pytest.mark.parametrize('failure', ['timeout', 'error'])
def test_first_order_requires_reliable_flat_risk_data(carry, monkeypatch, failed_leg, failure):
    runtime, e, h, journal, clock, tick, open_pair = carry
    monkeypatch.setattr(runtime_module, 'FUNDING_RISK_REQUEST_TIMEOUT_SECONDS', .015)
    failed_venue = e if failed_leg == 'entropy' else h

    async def unavailable():
        if failure == 'timeout':
            await asyncio.Event().wait()
        raise OSError('risk data unavailable before the first order')

    async def scenario():
        failed_venue.fetch_funding_risk = unavailable
        await asyncio.wait_for(open_pair(), .3)
        assert e.sent == h.sent == []
        assert e.position == h.position == 0
        assert not journal.pending()
        row = journal.db.execute('SELECT cycle_id FROM funding_cycles').fetchone()
        assert row is not None
        assert journal.funding_state(row['cycle_id'])['exit_reason'] == 'risk_data_unavailable'
        assert journal.batch_state('batch')['closed_for_entries']

    asyncio.run(scenario())


def test_failed_reads_use_valid_cache_without_renewing_source_time(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        original_ts = runtime.status.base['funding_risk']['legs']['entropy']['source_ts']
        e.risk_error = h.risk_error = TimeoutError('private payload must not be logged')
        for stamp in (10006, 10009, 10013):
            tick(stamp)
            await runtime._check_funding_liquidation(cycle)
            assert not exit_reason(journal, cycle)
            assert runtime.status.base['funding_risk']['legs']['entropy']['source_ts'] == original_ts
        tick(10016)
        await runtime._check_funding_liquidation(cycle)
        assert exit_reason(journal, cycle) == 'risk_data_unavailable'
        assert e.position == -h.position == 1.99

    asyncio.run(scenario())


def test_timeout_with_valid_cache_holds_but_does_not_extend_cache(carry, monkeypatch):
    runtime, e, h, journal, clock, tick, open_pair = carry
    monkeypatch.setattr(runtime_module, 'FUNDING_RISK_REQUEST_TIMEOUT_SECONDS', .015)

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        original_ts = runtime.status.base['funding_risk']['legs']['entropy']['source_ts']

        async def hung():
            await asyncio.Event().wait()

        e.fetch_funding_risk = hung
        tick(10006)
        await asyncio.wait_for(runtime._check_funding_liquidation(cycle), .3)
        assert not exit_reason(journal, cycle)
        assert runtime.status.base['funding_risk']['legs']['entropy']['source_ts'] == original_ts
        tick(10016)
        await asyncio.wait_for(runtime._check_funding_liquidation(cycle), .3)
        assert exit_reason(journal, cycle) == 'risk_data_unavailable'

    asyncio.run(scenario())


def test_timeout_does_not_wait_for_adapter_that_suppresses_cancellation(carry, monkeypatch):
    runtime, e, h, journal, clock, tick, open_pair = carry
    monkeypatch.setattr(runtime_module, 'FUNDING_RISK_REQUEST_TIMEOUT_SECONDS', .015)

    async def scenario():
        cycle = await open_pair()
        runtime._funding_risk_cache.clear()
        tick(10003)
        cancelled, release = asyncio.Event(), asyncio.Event()

        async def late_read():
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return risk_snapshot(e, clock)

        e.fetch_funding_risk = late_read
        try:
            await asyncio.wait_for(runtime._check_funding_liquidation(cycle), .3)
            await asyncio.wait_for(cancelled.wait(), .3)
            assert not release.is_set()
            assert exit_reason(journal, cycle) == 'risk_data_unavailable'
        finally:
            release.set()
            await asyncio.gather(*runtime._funding_reads, return_exceptions=True)
        assert 'entropy' not in runtime._funding_risk_cache

    asyncio.run(scenario())


def test_fast_leg_continues_polling_without_overlapping_slow_leg(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        entered, release = asyncio.Event(), asyncio.Event()
        reads = {'entropy': 0, 'hedge': 0}

        async def slow():
            reads['entropy'] += 1
            entered.set()
            await release.wait()
            return risk_snapshot(e, clock)

        async def fast():
            reads['hedge'] += 1
            return risk_snapshot(h, clock)

        e.fetch_funding_risk, h.fetch_funding_risk = slow, fast
        try:
            for stamp in (10003, 10005, 10007):
                tick(stamp)
                await runtime._check_funding_liquidation(cycle, wait=False)
                await asyncio.wait_for(entered.wait(), .3)
                await asyncio.wait_for(runtime._funding_risk_tasks['hedge'], .3)
            assert reads == {'entropy': 1, 'hedge': 3}
            assert not exit_reason(journal, cycle)
        finally:
            release.set()
            await drain_risk(runtime)

    asyncio.run(scenario())


def test_failed_cache_expiry_exits_even_while_refresh_is_still_pending(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        e.risk_error = TimeoutError('failed read')
        tick(10006)
        await runtime._check_funding_liquidation(cycle)
        assert not exit_reason(journal, cycle)
        entered, release = asyncio.Event(), asyncio.Event()

        async def slow_refresh():
            entered.set()
            await release.wait()
            return risk_snapshot(e, clock)

        e.fetch_funding_risk = slow_refresh
        try:
            tick(10009)
            await runtime._check_funding_liquidation(cycle, wait=False)
            await asyncio.wait_for(entered.wait(), .3)
            assert not exit_reason(journal, cycle)
            tick(10014)
            await runtime._check_funding_liquidation(cycle, wait=False)
            assert not release.is_set()
            assert exit_reason(journal, cycle) == 'risk_data_unavailable'
            await runtime._check_funding_liquidation(cycle, wait=False)
        finally:
            release.set()
            await drain_risk(runtime)
        records = [json.loads(line) for line in runtime.observer.path.read_text().splitlines()]
        expiries = [row for row in records if row.get('kind') == 'v3_funding_risk_expiry']
        assert len(expiries) == 1
        assert expiries[0]['venue'] == 'entropy'
        assert expiries[0]['failure_code'] == 'cached_evidence_unavailable'
        assert expiries[0]['failure_since_ts'] == 10006
        assert expiries[0]['consecutive_failures'] == 1
        assert expiries[0]['failure_duration_seconds'] >= 0
        assert expiries[0]['source_ts'] == 10003
        assert expiries[0]['data_age_seconds'] == 11
        assert expiries[0]['exit_reason'] == 'risk_data_unavailable'

    asyncio.run(scenario())


@pytest.mark.parametrize('changed_leg', ['entropy', 'hedge'])
def test_position_change_invalidates_both_leg_cache(carry, changed_leg):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        changed = cycle[f'{changed_leg}_units'] + (100 if changed_leg == 'entropy' else -100)
        tick(10006)
        journal.update_live_cycle(cycle['cycle_id'], updated_ts=clock[0],
                                  **{f'{changed_leg}_units': changed})
        e.risk_error = h.risk_error = OSError('transport failed')
        await runtime._check_funding_liquidation(journal.active_live_cycle())
        assert exit_reason(journal, cycle) == 'risk_data_unavailable'

    asyncio.run(scenario())


@pytest.mark.parametrize('invalid', ['no_snapshot', 'wrong_schema', 'expired', 'future', 'missing_liquidation',
                                    'zero_quantity', 'wrong_quantity', 'wrong_sign'])
def test_invalid_snapshot_cannot_seed_cache(carry, invalid):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        runtime._funding_risk_cache.clear()
        tick(10003)

        async def malformed():
            if invalid == 'no_snapshot':
                return None
            if invalid == 'wrong_schema':
                return {'position_qty': str(e.position), 'mark_price': '100'}
            qty = D(str(e.position))
            ts, liq = clock[0], D('95')
            if invalid == 'expired':
                ts -= 10.01
            elif invalid == 'future':
                ts += .01
            elif invalid == 'missing_liquidation':
                liq = None
            elif invalid == 'zero_quantity':
                qty = D('0')
            elif invalid == 'wrong_quantity':
                qty += D('.0001')
            else:
                qty = -qty
            return FundingRiskSnapshot(qty, D('100'), liq, ts, 'fake:readonly')

        e.fetch_funding_risk = malformed
        await runtime._check_funding_liquidation(cycle)
        assert exit_reason(journal, cycle) == 'risk_data_unavailable'
        assert 'entropy' not in runtime._funding_risk_cache

    asyncio.run(scenario())


def test_flat_leg_without_liquidation_price_is_not_a_risk_failure(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        h.position = 0
        journal.update_live_cycle(cycle['cycle_id'], hedge_units=0, updated_ts=clock[0])
        await runtime._check_funding_liquidation(journal.active_live_cycle())
        runtime._flush_telemetry()
        assert not exit_reason(journal, cycle)
        records = [json.loads(line) for line in runtime.observer.path.read_text().splitlines()]
        hedge_risk = [row for row in records if row.get('kind') == 'v3_funding_risk'
                      and row.get('stage') == 'finished' and row.get('venue') == 'hedge']
        assert hedge_risk[-1]['failure_code'] is None
        assert hedge_risk[-1]['observation']['position_qty'] == '0'
        assert hedge_risk[-1]['observation']['liquidation_price'] is None

    asyncio.run(scenario())


@pytest.mark.parametrize('dangerous_leg', ['entropy', 'hedge'])
def test_dangerous_leg_latches_without_waiting_for_slow_peer(carry, dangerous_leg):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        dangerous, peer = (e, h) if dangerous_leg == 'entropy' else (h, e)
        peer_entered, release = asyncio.Event(), asyncio.Event()

        async def slow():
            peer_entered.set()
            await release.wait()
            return risk_snapshot(peer, clock)

        async def danger():
            await peer_entered.wait()
            return risk_snapshot(dangerous, clock,
                                 liquidation=D('99') if dangerous.position > 0 else D('101'))

        peer.fetch_funding_risk, dangerous.fetch_funding_risk = slow, danger
        watcher = asyncio.create_task(runtime._check_funding_liquidation(cycle))
        try:
            await asyncio.wait_for(watcher, .3)
            assert not release.is_set()
            assert exit_reason(journal, cycle) == f'liquidation_{dangerous_leg}'
            await asyncio.wait_for(runtime._resume_cycle(cycle), .3)
            assert not release.is_set()
            assert e.position == h.position == 0
            assert all(order['reduce_only'] for order in e.sent[1:] + h.sent[1:])
        finally:
            release.set()
            await asyncio.gather(watcher, return_exceptions=True)
            await drain_risk(runtime)

    asyncio.run(scenario())


def test_nonblocking_check_returns_while_read_is_inflight(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        entered, release = asyncio.Event(), asyncio.Event()

        async def slow():
            entered.set()
            await release.wait()
            return risk_snapshot(e, clock)

        e.fetch_funding_risk = slow
        try:
            await asyncio.wait_for(runtime._check_funding_liquidation(cycle, wait=False), .3)
            await asyncio.wait_for(entered.wait(), .3)
            assert runtime._funding_risk_tasks
            assert not runtime._funding_risk_tasks['entropy'].done()
            assert not exit_reason(journal, cycle)
        finally:
            release.set()
            await drain_risk(runtime)

    asyncio.run(scenario())


def test_already_requested_normal_exit_does_not_wait_for_inflight_risk(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        entered, release = asyncio.Event(), asyncio.Event()

        async def slow():
            entered.set()
            await release.wait()
            return risk_snapshot(e, clock)

        e.fetch_funding_risk = slow
        await runtime._check_funding_liquidation(cycle, wait=False)
        await asyncio.wait_for(entered.wait(), .3)
        runtime._latch_funding_exit(cycle, 'two_settlements_each')
        try:
            await asyncio.wait_for(runtime._resume_cycle(cycle), .3)
            assert not release.is_set()
            assert e.position == h.position == 0
            assert journal.active_live_cycle() is None
            assert exit_reason(journal, cycle) == 'two_settlements_each'
            assert all(order['reduce_only'] for order in e.sent[1:] + h.sent[1:])
        finally:
            release.set()
            await drain_risk(runtime)

    asyncio.run(scenario())


@pytest.mark.parametrize('stage', ['settlements', 'rates'])
def test_exit_interrupts_funding_hold_readonly_io(carry, stage):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        entered, release, flattened = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = (e.fetch_funding_settlements if stage == 'settlements' else e.fetch_funding)

        for venue in (e, h):
            original_send = venue.send_taker

            async def send(*args, original_send=original_send, **kwargs):
                result = await original_send(*args, **kwargs)
                if e.position == h.position == 0:
                    flattened.set()
                return result

            venue.send_taker = send

        async def slow(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        if stage == 'settlements':
            e.fetch_funding_settlements = slow
        else:
            e.fetch_funding = slow
        holding = asyncio.create_task(runtime._resume_cycle(cycle))
        try:
            await asyncio.wait_for(entered.wait(), .3)
            runtime._latch_funding_exit(cycle, 'two_settlements_each')
            await asyncio.wait_for(flattened.wait(), .3)
            assert not release.is_set()
            assert e.position == h.position == 0
            release.set()  # Accounting after flat may read the history again.
            await asyncio.wait_for(holding, .3)
            assert journal.active_live_cycle() is None
        finally:
            release.set()
            await asyncio.gather(holding, return_exceptions=True)
            await drain_risk(runtime)

    asyncio.run(scenario())


def test_late_response_for_changed_position_is_discarded(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        entered, release = asyncio.Event(), asyncio.Event()
        old_qty = D(str(e.position))

        async def old_danger():
            entered.set()
            await release.wait()
            return risk_snapshot(e, clock, quantity=old_qty, liquidation=D('99'))

        e.fetch_funding_risk = old_danger
        await runtime._check_funding_liquidation(cycle, wait=False)
        await asyncio.wait_for(entered.wait(), .3)
        journal.update_live_cycle(cycle['cycle_id'], entropy_units=19800,
                                  updated_ts=clock[0])
        e.position = 1.98
        release.set()
        await drain_risk(runtime)
        assert not exit_reason(journal, cycle)
        assert 'entropy' not in runtime._funding_risk_cache

    asyncio.run(scenario())


def test_late_response_after_flat_cannot_latch_exit(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        entered, release = asyncio.Event(), asyncio.Event()
        old_qty = D(str(e.position))

        async def old_danger():
            entered.set()
            await release.wait()
            return risk_snapshot(e, clock, quantity=old_qty, liquidation=D('99'))

        e.fetch_funding_risk = old_danger
        await runtime._check_funding_liquidation(cycle, wait=False)
        await asyncio.wait_for(entered.wait(), .3)
        journal.update_live_cycle(cycle['cycle_id'], phase='DONE', entropy_units=0,
                                  hedge_units=0, updated_ts=clock[0])
        release.set()
        await drain_risk(runtime)
        assert not exit_reason(journal, cycle)
        assert 'entropy' not in runtime._funding_risk_cache

    asyncio.run(scenario())


def test_stale_cycle_caller_cannot_reuse_current_cycle_cache(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        before = dict(runtime._funding_risk_cache)
        requests = []

        async def unexpected_read():
            requests.append('unexpected')
            raise OSError('cannot read for an obsolete cycle')

        e.fetch_funding_risk = h.fetch_funding_risk = unexpected_read
        tick(10006)
        obsolete = {**cycle, 'cycle_id': 'obsolete-cycle'}
        await runtime._check_funding_liquidation(obsolete)
        assert requests == []
        assert runtime._funding_risk_cache == before
        assert not exit_reason(journal, cycle)
        assert journal.funding_state('obsolete-cycle') is None

    asyncio.run(scenario())


def test_observer_failure_cannot_prevent_danger_exit(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        e.liquidation = D('99')

        def failed_observer(event):
            raise OSError('disk unavailable')

        runtime.observer.publish = failed_observer
        await runtime._check_funding_liquidation(cycle)
        runtime._flush_telemetry()
        assert exit_reason(journal, cycle) == 'liquidation_entropy'

    asyncio.run(scenario())


def test_runtime_restart_has_no_trusted_risk_cache(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        assert runtime._funding_risk_cache
        restarted = fresh_runtime(runtime, journal)
        assert restarted._funding_risk_cache == {}
        restarted.instrument = runtime.instrument
        e.risk_error = h.risk_error = OSError('interface unavailable')
        tick(10006)
        await restarted._check_funding_liquidation(cycle)
        assert exit_reason(journal, cycle) == 'risk_data_unavailable'

    asyncio.run(scenario())


def test_persistent_risk_events_record_failure_streak_and_recovery_without_secrets(
        carry, monkeypatch):
    runtime, e, h, journal, clock, tick, open_pair = carry
    original_monotonic = runtime_module.time.monotonic
    monkeypatch.setattr(runtime_module.time, 'monotonic',
                        lambda: original_monotonic() + clock[0] - 10000)
    secret_marker = 'synthetic-secret-account-address'

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        e.risk_error = TimeoutError(secret_marker)
        for stamp in (10006, 10009):
            tick(stamp)
            await runtime._check_funding_liquidation(cycle)
        e.risk_error = None
        tick(10011)
        await runtime._check_funding_liquidation(cycle)
        runtime._flush_telemetry()
        text = runtime.observer.path.read_text()
        assert secret_marker not in text
        records = [json.loads(line) for line in text.splitlines()]
        finished = [row for row in records if row.get('kind') == 'v3_funding_risk'
                    and row.get('stage') == 'finished' and row.get('venue') == 'entropy']
        failures = [row for row in finished if row.get('failure_code') == 'timeout']
        assert [row['consecutive_failures'] for row in failures] == [1, 2]
        assert [row['outcome'] for row in failures] == ['cache', 'cache']
        assert failures[1]['failure_since_ts'] == 10006
        assert failures[1]['failure_duration_seconds'] == pytest.approx(3, abs=.15)
        assert failures[1]['observation']['source_ts'] == 10003
        assert failures[1]['observation']['data_age_seconds'] == 6
        recovered = finished[-1]
        assert recovered['failure_code'] is None
        assert recovered['consecutive_failures'] == 0
        assert recovered['recovered_failure_duration_seconds'] == pytest.approx(5, abs=.15)
        assert recovered['request_latency_seconds'] >= 0
        assert recovered['data_age_seconds'] == 0
        assert recovered['request_timeout_seconds'] == 10
        assert recovered['endpoints'] == ['POST /info clearinghouseState',
                                          'POST /info metaAndAssetCtxs']
        assert not exit_reason(journal, cycle)

    asyncio.run(scenario())


def test_http_failure_records_status_and_class_without_response_or_url(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    class PrivateHttpError(Exception):
        status = 403
        url = 'https://example.invalid/private?token=synthetic-secret'

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        e.risk_error = PrivateHttpError('synthetic-secret private account response')
        tick(10006)
        await runtime._check_funding_liquidation(cycle)
        runtime._flush_telemetry()
        text = runtime.observer.path.read_text()
        assert 'synthetic-secret' not in text
        assert 'example.invalid' not in text
        records = [json.loads(line) for line in text.splitlines()]
        finished = [row for row in records if row.get('kind') == 'v3_funding_risk'
                    and row.get('stage') == 'finished' and row.get('venue') == 'entropy']
        assert finished[-1]['failure_code'] == 'http_auth'
        assert finished[-1]['http_status'] == 403
        assert finished[-1]['cache_used']
        assert not exit_reason(journal, cycle)

    asyncio.run(scenario())


def test_nonreducing_step_rejects_cache_expired_inside_poll_interval(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        reads = []

        async def nearly_expired():
            reads.append(clock[0])
            return risk_snapshot(e, clock, ts=clock[0] - 8.5)

        e.fetch_funding_risk = nearly_expired
        await runtime._check_funding_liquidation(cycle)
        assert runtime.status.base['funding_risk']['legs']['entropy']['data_age_seconds'] == 8.5
        assert runtime._funding_risk_failures == {}
        before_sends = len(e.sent) + len(h.sent)
        before_orders = len(journal.cycle_orders(cycle['cycle_id']))
        tick(10004.6)
        await runtime._check_funding_liquidation(cycle)
        assert reads == [10003]
        assert runtime._funding_risk_failures == {}
        assert not exit_reason(journal, cycle)
        result = await runtime._step(
            cycle, purpose='risk_cache_expired', attempt=0, venue_key='hedge',
            side=Direction(cycle['direction']).entropy_side.opposite,
            units=2000, reduce_only=False, room_bps=runtime.settings.hedge_rooms_bps[0],
            hard_cap_bps=runtime.settings.hedge_hard_cap_bps,
            next_phase='RECOVERY', completion=True)
        assert result is None
        step = journal.live_step(f'{cycle["cycle_id"]}:risk_cache_expired:0')
        assert step['abort_reason'] == 'FUNDING_RISK_UNAVAILABLE'
        assert len(e.sent) + len(h.sent) == before_sends
        assert len(journal.cycle_orders(cycle['cycle_id'])) == before_orders
        assert not journal.pending()
        assert not exit_reason(journal, cycle)

    asyncio.run(scenario())


@pytest.mark.parametrize('change', ['signed_units', 'order_watermark'])
def test_nonreducing_step_rejects_risk_superseded_while_waiting(carry, change):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        entered, release = asyncio.Event(), asyncio.Event()
        old_qty = D(str(e.position))

        async def slow_risk():
            entered.set()
            await release.wait()
            return risk_snapshot(e, clock, quantity=old_qty)

        e.fetch_funding_risk = slow_risk
        checking = asyncio.create_task(runtime._check_funding_liquidation(cycle))
        try:
            await asyncio.wait_for(entered.wait(), .3)
            if change == 'signed_units':
                journal.update_live_cycle(cycle['cycle_id'], entropy_units=19800,
                                          updated_ts=clock[0])
                e.position = 1.98
            else:
                ref = 'offline-risk-watermark-change'
                journal.prepare(Intent(ref, cycle['cycle_id'], 'entropy', Side.SELL,
                                       100, D('100'), reduce_only=True))
                journal.record(ref, Result(terminal=True, cumulative_units=0))
            release.set()
            await asyncio.wait_for(checking, .3)
            assert not exit_reason(journal, cycle)
            before_sends = len(e.sent) + len(h.sent)
            before_orders = len(journal.cycle_orders(cycle['cycle_id']))
            current = journal.active_live_cycle()
            result = await runtime._step(
                current, purpose='risk_superseded', attempt=0, venue_key='hedge',
                side=Direction(cycle['direction']).entropy_side.opposite,
                units=2000, reduce_only=False, room_bps=runtime.settings.hedge_rooms_bps[0],
                hard_cap_bps=runtime.settings.hedge_hard_cap_bps,
                next_phase='RECOVERY', completion=True)
            assert result is None
            step = journal.live_step(f'{cycle["cycle_id"]}:risk_superseded:0')
            assert step['abort_reason'] == 'FUNDING_RISK_UNAVAILABLE'
            assert len(e.sent) + len(h.sent) == before_sends
            assert len(journal.cycle_orders(cycle['cycle_id'])) == before_orders
            assert not journal.pending()
            assert not exit_reason(journal, cycle)
        finally:
            release.set()
            await asyncio.gather(checking, return_exceptions=True)
            await drain_risk(runtime)

    asyncio.run(scenario())


def test_run_shutdown_flushes_cancelled_risk_record_before_venue_close(carry, monkeypatch):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        entered = asyncio.Event()

        async def no_initialize(**kwargs):
            return []

        async def parked():
            await runtime.stop.wait()

        async def pending_risk():
            entered.set()
            await asyncio.Event().wait()

        async def stop_after_risk_starts(active):
            await runtime._check_funding_liquidation(active, wait=False)
            await entered.wait()
            runtime.stop.set()
            runtime.changed.set()

        def cancelled_record_is_persistent():
            records = [json.loads(line) for line in runtime.observer.path.read_text().splitlines()]
            risk = [row for row in records if row.get('kind') == 'v3_funding_risk'
                    and row.get('venue') == 'entropy' and row.get('request_started_ts') == 10003]
            started = [row for row in risk if row.get('stage') == 'started']
            cancelled = [row for row in risk if row.get('stage') == 'cancelled']
            assert len(started) == len(cancelled) == 1
            assert started[0]['attempt'] == cancelled[0]['attempt']
            assert cancelled[0]['failure_code'] == 'request_superseded_or_exit'
            assert cancelled[0]['request_latency_seconds'] >= 0

        async def close():
            cancelled_record_is_persistent()

        monkeypatch.setattr(runtime, 'initialize', no_initialize)
        monkeypatch.setattr(runtime, '_resume_cycle', stop_after_risk_starts)
        for name in ('_flat_lease_loop', '_http_keepalive_loop', '_equity_loop',
                     '_analytics_reconcile_loop', '_reference_loop', '_funding_risk_loop'):
            monkeypatch.setattr(runtime, name, parked)
        for venue in (e, h):
            monkeypatch.setattr(venue, 'init_signer', lambda: pytest.fail('shutdown test cannot initialize signer'))
            monkeypatch.setattr(venue, 'close', close)
        e.fetch_funding_risk = pending_risk
        before_sends = len(e.sent) + len(h.sent)
        await asyncio.wait_for(runtime.run(), .5)
        cancelled_record_is_persistent()
        assert len(e.sent) + len(h.sent) == before_sends
        assert not exit_reason(journal, cycle)

    asyncio.run(scenario())


@pytest.mark.parametrize('change', ['exit_latched', 'cache_expired'])
def test_nonreducing_step_rechecks_risk_after_prior_submission_ack(carry, monkeypatch, change):
    runtime, e, h, journal, clock, tick, open_pair = carry

    async def scenario():
        cycle = await open_pair()
        tick(10003)
        await runtime._check_funding_liquidation(cycle)
        entered, release = asyncio.Event(), asyncio.Event()
        previous_ack = asyncio.create_task(release.wait())
        drain_calls = []
        original_send = h.send_taker

        async def wait_for_submission():
            drain_calls.append(clock[0])
            entered.set()
            if not previous_ack.done():
                await asyncio.gather(previous_ack)

        async def send_after_ack(**kwargs):
            await wait_for_submission()  # Match Lighter's second drain before signing.
            return await original_send(**kwargs)

        monkeypatch.setattr(h, 'wait_for_submission', wait_for_submission, raising=False)
        monkeypatch.setattr(h, 'send_taker', send_after_ack)
        before_sends = len(e.sent) + len(h.sent)
        before_orders = len(journal.cycle_orders(cycle['cycle_id']))
        attempt = asyncio.create_task(runtime._step(
            cycle, purpose='risk_changed_during_ack', attempt=0, venue_key='hedge',
            side=Direction(cycle['direction']).entropy_side.opposite,
            units=2000, reduce_only=False, room_bps=runtime.settings.hedge_rooms_bps[0],
            hard_cap_bps=runtime.settings.hedge_hard_cap_bps,
            next_phase='RECOVERY', completion=True))
        try:
            await asyncio.wait_for(entered.wait(), .3)
            assert not attempt.done()
            assert not previous_ack.done()
            if change == 'exit_latched':
                runtime._latch_funding_exit(cycle, 'manual_test')
            else:
                tick(10014)
            assert not previous_ack.cancelled()
            release.set()
            assert await asyncio.wait_for(attempt, .3) is None
            assert previous_ack.done() and not previous_ack.cancelled()
            assert len(drain_calls) == 1
            step = journal.live_step(f'{cycle["cycle_id"]}:risk_changed_during_ack:0')
            expected = ('FUNDING_EXIT_REQUESTED' if change == 'exit_latched'
                        else 'FUNDING_RISK_UNAVAILABLE')
            assert step['abort_reason'] == expected
            assert len(e.sent) + len(h.sent) == before_sends
            assert len(journal.cycle_orders(cycle['cycle_id'])) == before_orders
            assert not journal.pending()
        finally:
            release.set()
            await asyncio.gather(previous_ack, attempt, return_exceptions=True)

    asyncio.run(scenario())


def test_lighter_post_guard_allows_only_own_unsent_watermark(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    async def scenario():
        cycle = await open_pair()
        await runtime._check_funding_liquidation(cycle)
        ref = '199000001'
        journal.prepare_submitting(Intent(ref, cycle['cycle_id'], 'hedge',
                                          Side.BUY, 1, D('100'), False))
        assert runtime._lighter_increase_guard(ref) is None
        # A risk refresh after prepare has the full current watermark.
        await runtime._check_funding_liquidation(cycle)
        assert runtime._lighter_increase_guard(ref) is None
        entropy_order = next(row for row in journal.cycle_orders(cycle['cycle_id'])
                             if row['venue'] == 'entropy')
        journal.db.execute("UPDATE orders SET state='UNKNOWN' WHERE order_ref=?",
                           (entropy_order['order_ref'],))
        journal.db.commit()
        assert runtime._lighter_increase_guard(ref) == 'funding_risk_unavailable'
        # Even a newly refreshed risk cache cannot authorize another unknown.
        await runtime._check_funding_liquidation(cycle)
        assert runtime._lighter_increase_guard(ref) == 'funding_risk_unavailable'
    asyncio.run(scenario())


def test_lighter_post_guard_does_not_extend_risk_cache_expiry(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    async def scenario():
        cycle = await open_pair()
        await runtime._check_funding_liquidation(cycle)
        ref = '199000002'
        journal.prepare_submitting(Intent(ref, cycle['cycle_id'], 'hedge',
                                          Side.BUY, 1, D('100'), False))
        assert runtime._lighter_increase_guard(ref) is None
        runtime._funding_risk_cache = {
            key: (signature, snapshot, runtime_module.time.monotonic() - 1)
            for key, (signature, snapshot, _) in runtime._funding_risk_cache.items()}
        assert runtime._lighter_increase_guard(ref) == 'funding_risk_unavailable'
    asyncio.run(scenario())
