"""Offline checks for request admission, evidence freshness, and recovery."""
import asyncio
import socket
import sys
import time as real_time
from dataclasses import replace
from types import SimpleNamespace

import aiohttp
import pytest

import entropy_arb.venue_lighter as lighter_module
import entropy_arb.v3.runtime as runtime_module
from entropy_arb.lighter_requests import RequestBudget, RequestDeferred
from entropy_arb.venue_lighter import AccountOrdersFeed, LighterVenue
from tests_v3.test_runtime import setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("integration tests must never use real network/SDK")

    async def blocked_http(*args, **kwargs):
        blocked()

    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked)
    monkeypatch.setattr(aiohttp.ClientSession, "_request", blocked_http)
    monkeypatch.setattr(lighter_module, "ws_connect", blocked)
    constants = SimpleNamespace(
        ORDER_TYPE_MARKET=1,
        ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL=0,
        DEFAULT_IOC_EXPIRY=0,
    )
    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(SignerClient=constants))


class Clock:
    def __init__(self):
        self.wall = 1_700_000_000.0
        self.mono = 100.0

    def time(self):
        return self.wall

    def monotonic(self):
        return self.mono

    perf_counter = staticmethod(real_time.perf_counter)

    def advance(self, seconds):
        self.wall += seconds
        self.mono += seconds


def account_payload(position="0", equity="100"):
    return {"accounts": [{
        "positions": [{"market_id": 139, "position": position, "sign": 1}],
        "total_asset_value": equity,
        "available_balance": "90",
    }]}


class Response:
    status = 200
    headers = {}

    def __init__(self, payload, *, gate=None, started=None):
        self.payload = payload
        self.gate, self.started = gate, started
        self.cancelled = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def raise_for_status(self):
        return None

    def getheaders(self):
        return {}

    async def json(self):
        if self.started is not None:
            self.started.set()
        try:
            if self.gate is not None:
                await self.gate.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return self.payload


class Session:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []
        self.responses = []

    def get(self, url, **kwargs):
        path = url.split(".invalid", 1)[-1]
        self.calls.append((path, kwargs))
        response = self.handler(path, len(self.calls))
        self.responses.append(response)
        return response


def make_venue(monkeypatch, handler, *, feed=False):
    clock = Clock()
    monkeypatch.setattr(lighter_module, "time", clock)
    session = Session(handler)
    conf = SimpleNamespace(
        key="hedge", label="lighter", symbol="SNDK", fee_bps=0,
        cap_usd=15, orders_per_min=6,
        lighter_profile=SimpleNamespace(api_url="https://lighter.invalid"),
        lighter_creds=SimpleNamespace(account_index=42),
    )
    venue = LighterVenue(conf, session, settle_timeout_sec=.01)
    venue.market_id = 139
    budget = RequestBudget(clock=clock.monotonic, wall_clock=clock.time)
    venue._request_budget = lambda: budget
    if feed:
        venue.orders_feed = AccountOrdersFeed("test", "unused", 139, 42, None)
    venue._fee_auth_header = lambda: {}
    return venue, session, budget, clock


class FakeRest:
    def __init__(self, callback=None):
        self.pool_manager = SimpleNamespace(
            _retry_connection=True, request=self.pool_request)
        self.retry_client = None
        self.callback = callback
        self.calls = []

    async def pool_request(self, *args, **kwargs):
        return Response({})

    async def request(self, method, url, *args, **kwargs):
        path = url.split(".invalid", 1)[-1]
        self.calls.append((method, path))
        if self.callback is not None:
            await self.callback(method, path)
        return await self.pool_manager.request(method, url)


class FakeSigner:
    """A local nonce/HTTP sequence, without importing or invoking the SDK."""
    def __init__(self, rest, *, before_order=None):
        self.api_client = SimpleNamespace(rest_client=rest)
        self.nonce_manager = SimpleNamespace(nonce={7: 10})
        self.before_order = before_order
        self.orders = []
        self.cold_refreshes = 0

    async def create_order(self, **kwargs):
        self.orders.append(kwargs)
        if self.before_order is not None:
            self.before_order()
        rest = self.api_client.rest_client
        if not self.nonce_manager.nonce:
            await rest.request("GET", "https://lighter.invalid/api/v1/nextNonce")
            self.cold_refreshes += 1
            self.nonce_manager.nonce[7] = 10
        self.nonce_manager.nonce[7] += 1
        await rest.request("POST", "https://lighter.invalid/api/v1/sendTx")
        return None, SimpleNamespace(code=200), None


def install_signer(venue, *, callback=None, before_order=None):
    rest = FakeRest(callback)
    signer = FakeSigner(rest, before_order=before_order)
    venue.signer = signer
    venue._wrap_sdk_requests()
    return signer, rest


async def send(venue, ref="101"):
    return await venue.send_taker(
        is_buy=True, qty=.01, limit_px=100, reduce_only=False, order_ref=ref)


def test_account_raw_json_single_flight_cache_and_force_refresh(monkeypatch):
    async def scenario():
        gate, started = asyncio.Event(), asyncio.Event()
        venue, session, _, clock = make_venue(
            monkeypatch, lambda path, count: Response(
                account_payload(), gate=gate if count == 1 else None,
                started=started if count == 1 else None))
        sdk_reads = []

        def forbidden_account_api(*args):
            sdk_reads.append(args)
            raise AssertionError("account reads must use the raw JSON path")

        monkeypatch.setattr(sys.modules["lighter"], "AccountApi", forbidden_account_api,
                            raising=False)
        venue.signer = SimpleNamespace(api_client=object())
        one = asyncio.create_task(venue.fetch_account_snapshot())
        two = asyncio.create_task(venue.fetch_account_snapshot())
        await asyncio.wait_for(started.wait(), .2)
        assert len(session.calls) == 1
        gate.set()
        first, second = await asyncio.gather(one, two)
        assert first is second
        assert first.equity == 100 and first.available == 90
        assert first.source == "raw_rest_fallback"
        received = first.received_ts
        clock.advance(1.9)
        cached = await venue.fetch_account_snapshot()
        assert cached is first and cached.received_ts == received
        assert len(session.calls) == 1
        fresh = await venue.fetch_account_snapshot(force_refresh=True)
        assert fresh.received_ts == clock.time() and fresh.received_ts > received
        assert len(session.calls) == 2
        clock.advance(2.01)
        await venue.fetch_account_snapshot()
        assert len(session.calls) == 3 and sdk_reads == []
        assert all(path == "/api/v1/account" for path, _ in session.calls)

    asyncio.run(scenario())


def test_cancelled_account_waiter_does_not_cancel_shared_read(monkeypatch):
    async def scenario():
        gate, started = asyncio.Event(), asyncio.Event()
        venue, session, _, _ = make_venue(
            monkeypatch, lambda path, count: Response(
                account_payload(), gate=gate, started=started))
        one = asyncio.create_task(venue.fetch_account_snapshot())
        two = asyncio.create_task(venue.fetch_account_snapshot())
        await asyncio.wait_for(started.wait(), .2)
        one.cancel()
        with pytest.raises(asyncio.CancelledError):
            await one
        assert not session.responses[0].cancelled
        gate.set()
        snapshot = await asyncio.wait_for(two, .2)
        assert snapshot.equity == 100 and len(session.calls) == 1

    asyncio.run(scenario())


def test_account_timestamp_and_cache_age_start_before_http_response(monkeypatch):
    async def scenario():
        def response(path, count):
            result = Response(account_payload())

            async def body():
                if count == 1:
                    clock.advance(1.25)
                return result.payload

            result.json = body
            return result

        venue, session, _, clock = make_venue(monkeypatch, response)
        started = clock.time()
        first = await venue.fetch_account_snapshot()
        assert first.received_ts == started
        clock.advance(.74)
        assert (await venue.fetch_account_snapshot()) is first
        assert len(session.calls) == 1
        clock.advance(.02)
        second = await venue.fetch_account_snapshot()
        assert len(session.calls) == 2 and second.received_ts == clock.time()

    asyncio.run(scenario())


def test_account_response_older_than_two_seconds_is_unknown_not_cached(monkeypatch):
    async def scenario():
        def response(path, count):
            result = Response(account_payload())

            async def body():
                if count == 1:
                    clock.advance(2.01)
                return result.payload

            result.json = body
            return result

        venue, session, _, clock = make_venue(monkeypatch, response)
        with pytest.raises(RuntimeError, match="freshness"):
            await venue.fetch_account_snapshot()
        assert venue._account_cache is None
        fresh = await venue.fetch_account_snapshot()
        assert fresh.received_ts == clock.time() and len(session.calls) == 2

    asyncio.run(scenario())


def test_account_epoch_rejects_old_inflight_and_force_read_starts_fresh(monkeypatch):
    async def scenario():
        gate, started = asyncio.Event(), asyncio.Event()
        venue, session, _, _ = make_venue(
            monkeypatch, lambda path, count: Response(
                account_payload(equity=str(count * 100)),
                gate=gate if count == 1 else None,
                started=started if count == 1 else None))
        old = asyncio.create_task(venue.fetch_account_snapshot())
        await asyncio.wait_for(started.wait(), .2)
        venue._invalidate_account_cache()
        fresh = await asyncio.wait_for(venue.fetch_account_snapshot(force_refresh=True), .2)
        assert fresh.equity == 200 and len(session.calls) == 2
        gate.set()
        with pytest.raises(RuntimeError):
            await old
        assert (await venue.fetch_account_snapshot()) is fresh

    asyncio.run(scenario())


def test_ws_terminal_during_account_read_invalidates_without_another_probe(monkeypatch):
    async def scenario():
        gate, started = asyncio.Event(), asyncio.Event()
        venue, session, _, _ = make_venue(
            monkeypatch, lambda path, count: Response(
                account_payload(), gate=gate if count == 1 else None,
                started=started if count == 1 else None), feed=True)
        old = asyncio.create_task(venue.fetch_account_snapshot())
        await asyncio.wait_for(started.wait(), .2)
        venue.orders_feed._handle_orders({"orders": {"139": [{
            "client_order_index": 101, "status": "filled",
            "filled_base_amount": ".01", "filled_quote_amount": "1",
        }]}})
        gate.set()
        with pytest.raises(RuntimeError, match="superseded"):
            await old
        assert venue._account_cache is None
        fresh = await venue.fetch_account_snapshot(force_refresh=True)
        assert fresh.equity == 100 and len(session.calls) == 2

    asyncio.run(scenario())


def test_real_post_and_terminal_invalidate_account_cache_but_nonce_allocation_does_not(monkeypatch):
    async def scenario():
        venue, _, _, _ = make_venue(
            monkeypatch, lambda path, count: Response(account_payload()), feed=True)
        await venue.fetch_account_snapshot()
        initial_epoch = venue._account_epoch
        entered, release = asyncio.Event(), asyncio.Event()
        order_epoch = []

        def before_order():
            assert venue._account_cache is not None
            assert venue._account_epoch == initial_epoch

        async def http(method, path):
            assert method == 'POST'
            assert venue._account_cache is None
            assert venue._account_epoch > initial_epoch
            order_epoch.append(venue._account_epoch)
            entered.set()
            await release.wait()

        install_signer(venue, callback=http, before_order=before_order)
        sending = asyncio.create_task(send(venue))
        try:
            await asyncio.wait_for(entered.wait(), .2)
            await venue.fetch_account_snapshot()
            assert venue._account_cache is not None
            venue.orders_feed._resolve(101, {
                "status": "filled", "filled_base": .01, "avg_px": 100})
            result = await asyncio.wait_for(sending, .2)
            assert result["unresolved"] is False
            assert venue._account_cache is None
            assert venue._account_epoch > order_epoch[0]
        finally:
            release.set()
            await asyncio.gather(sending, return_exceptions=True)
            await venue.wait_for_submission()

    asyncio.run(scenario())


def pending_order():
    return {"orders": [{"client_order_index": 101, "status": "pending",
                        "filled_base_amount": "0", "filled_quote_amount": "0"}]}


def test_same_id_concurrent_lookup_is_single_flight_without_arbitrary_cooldown(monkeypatch):
    async def scenario():
        gate, started = asyncio.Event(), asyncio.Event()
        venue, session, _, clock = make_venue(
            monkeypatch, lambda path, count: Response(
                pending_order(), gate=gate if count == 1 else None,
                started=started if count == 1 else None), feed=True)
        calls = [asyncio.create_task(venue.resolve_order("101", timeout=.02))
                 for _ in range(2)]
        try:
            await asyncio.wait_for(started.wait(), .2)
            results = await asyncio.wait_for(asyncio.gather(*calls), .2)
            assert all(result["unresolved"] for result in results)
            assert len(session.calls) == 1
            assert not session.responses[0].cancelled
            gate.set()
            await asyncio.sleep(.01)
            assert (await venue.resolve_order("101", timeout=.01))["unresolved"]
            assert len(session.calls) == 2
            assert (await venue.resolve_order("101", timeout=.01))["unresolved"]
            assert len(session.calls) == 3
        finally:
            gate.set()
            for task in calls:
                task.cancel()
            await asyncio.gather(*calls, return_exceptions=True)

    asyncio.run(scenario())


def test_slow_lookup_deadline_does_not_block_account_or_ws_terminal(monkeypatch):
    async def scenario():
        gate, started = asyncio.Event(), asyncio.Event()

        def response(path, count):
            if path == "/api/v1/accountOrders":
                return Response(pending_order(), gate=gate, started=started)
            return Response(account_payload())

        venue, session, _, _ = make_venue(monkeypatch, response, feed=True)
        resolving = asyncio.create_task(venue.resolve_order("101", timeout=.02))
        try:
            await asyncio.wait_for(started.wait(), .2)
            result = await asyncio.wait_for(resolving, .2)
            assert result["unresolved"] and not session.responses[0].cancelled
            risk_account = await asyncio.wait_for(venue.fetch_account_snapshot(), .2)
            assert risk_account.equity == 100
            venue.orders_feed._resolve(101, {
                "status": "filled", "filled_base": .01, "avg_px": 100})
            terminal = await asyncio.wait_for(venue.resolve_order("101", timeout=.02), .2)
            assert terminal["unresolved"] is False and terminal["filled_base"] == .01
            assert len([path for path, _ in session.calls
                        if path == "/api/v1/accountOrders"]) == 1
            assert not session.responses[0].cancelled
        finally:
            gate.set()
            await asyncio.gather(resolving, return_exceptions=True)
            await asyncio.sleep(.01)

    asyncio.run(scenario())


@pytest.mark.parametrize("waiters", [1, 2])
def test_ws_terminal_can_win_while_lookup_http_is_still_pending(monkeypatch, waiters):
    async def scenario():
        gate, started = asyncio.Event(), asyncio.Event()
        venue, session, _, _ = make_venue(
            monkeypatch, lambda path, count: Response(
                pending_order(), gate=gate, started=started), feed=True)
        resolving = [asyncio.create_task(venue.resolve_order("101", timeout=1.0))
                     for _ in range(waiters)]
        try:
            await asyncio.wait_for(started.wait(), .2)
            venue.orders_feed._resolve(101, {
                "status": "filled", "filled_base": .01, "avg_px": 100})
            terminals = await asyncio.wait_for(asyncio.gather(*resolving), .2)
            assert all(terminal["unresolved"] is False
                       and terminal["filled_base"] == .01 for terminal in terminals)
            assert len(session.calls) == 1
            assert not session.responses[0].cancelled
        finally:
            gate.set()
            await asyncio.gather(*resolving, return_exceptions=True)
            await asyncio.sleep(.01)

    asyncio.run(scenario())


def test_send_budget_denial_proves_local_unsent_without_sdk_or_nonce(monkeypatch):
    async def scenario():
        venue, session, budget, _ = make_venue(
            monkeypatch, lambda path, count: Response(account_payload()))
        signer, rest = install_signer(venue)
        budget.reserve(36)
        nonce = dict(signer.nonce_manager.nonce)
        result = await send(venue)
        assert result["status"] == "deferred" and result["unresolved"] is False
        assert result['deferred'] and result['retry_after'] == 60
        assert "proved_local_unsent" in result["err"]
        assert result["filled_base"] == 0
        assert signer.orders == rest.calls == session.calls == []
        assert signer.nonce_manager.nonce == nonce

    asyncio.run(scenario())


def test_deferred_before_post_clears_nonce_for_next_cold_refresh(monkeypatch):
    async def scenario():
        venue, _, budget, clock = make_venue(
            monkeypatch, lambda path, count: Response(account_payload()))
        once = True

        async def http(method, path):
            nonlocal once
            if method == "GET" and once:
                once = False
                budget.defer(429, {})

        signer, rest = install_signer(venue, callback=http)
        signer.nonce_manager.nonce.clear()
        result = await send(venue)
        assert result["unresolved"] is False
        assert "proved_local_unsent" in result["err"]
        assert rest.calls == [("GET", "/api/v1/nextNonce")]
        assert signer.nonce_manager.nonce == {}
        clock.advance(61)
        await send(venue, ref="102")
        assert signer.cold_refreshes == 2
        assert rest.calls == [("GET", "/api/v1/nextNonce"),
                              ("GET", "/api/v1/nextNonce"),
                              ("POST", "/api/v1/sendTx")]

    asyncio.run(scenario())


def test_deferred_after_post_started_stays_unknown_without_resend(monkeypatch):
    async def scenario():
        venue, _, _, _ = make_venue(
            monkeypatch, lambda path, count: Response(account_payload()))

        async def http(method, path):
            assert method == "POST"
            raise RequestDeferred("http_429", 60)

        signer, rest = install_signer(venue, callback=http)
        result = await send(venue)
        assert result["unresolved"] is True
        assert "proved_local_unsent" not in str(result.get("err"))
        assert len(signer.orders) == 1 and rest.calls == [("POST", "/api/v1/sendTx")]
        assert signer.nonce_manager.nonce == {7: 11}

    asyncio.run(scenario())


@pytest.mark.parametrize("source,age", [
    ("lighter_account_rest:raw_rest_fallback", 1.6),
    ("lighter_account_rest:raw_rest_fallback", .2),
    ("lighter_account_all_ws", 100),
])
def test_background_flat_proof_preserves_timestamp_without_mechanical_expiry(
        tmp_path, monkeypatch, source, age):
    runtime, _, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        clock = Clock()
        clock.wall = real_time.time()
        monkeypatch.setattr(runtime_module, "time", clock)
        runtime.settings = replace(runtime.settings, flat_position_lease_sec=1.5)
        reads = []

        async def position_snapshot(*, force_rest=False):
            reads.append(force_rest)
            return SimpleNamespace(position=0, received_ts=clock.time() - age,
                                   authoritative=True, source=source, generation=None)

        hedge.fetch_position_snapshot = position_snapshot
        await runtime._refresh_flat_lease()
        lease = runtime._flat_lease
        assert lease is not None
        assert lease.valid_until == float('inf')
        assert runtime._last_position_evidence.hedge_received_ts == clock.time() - age
        assert runtime._valid_flat_lease(clock.time()) is lease
        clock.advance(120)
        assert runtime._valid_flat_lease(clock.time()) is lease
        assert reads == [True], 'checking local proof repeated an account read'
        hedge.ready_to_trade = lambda: False
        assert runtime._valid_flat_lease(clock.time()) is None

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('status,headers,reason,delay', [
    (405, {}, 'http_405', 60),
    (405, {'X-Amzn-Waf-Action': 'captcha'}, 'captcha', 300),
    (429, {'Retry-After': '180'}, 'http_429', 180),
])
def test_http_limits_open_shared_backoff_without_account_fallback(
        monkeypatch, status, headers, reason, delay):
    async def scenario():
        def response(path, count):
            result = Response(account_payload())
            result.status, result.headers = status, headers
            def reject():
                raise aiohttp.ClientResponseError(
                    SimpleNamespace(real_url='https://lighter.invalid'), (),
                    status=status, headers=headers)
            result.raise_for_status = reject
            return result
        venue, session, budget, _ = make_venue(monkeypatch, response)
        expected = lighter_module.LighterWafBlocked if reason == 'captcha' else aiohttp.ClientResponseError
        with pytest.raises(expected):
            await venue.fetch_account_snapshot()
        snapshot = budget.snapshot()
        assert snapshot['blocked_reason'] == reason
        assert snapshot['blocked_for_sec'] == delay
        assert snapshot['counters']['started'] == 1
        assert venue._account_cache is None and len(session.calls) == 1
        with pytest.raises(RequestDeferred):
            await venue._get('/api/v1/orderBookDetails')
        assert len(session.calls) == 1  # No fallback or probe during backoff.
    asyncio.run(scenario())


def test_ordinary_account_risk_reads_preserve_completion_and_required_order_lookup(monkeypatch):
    async def scenario():
        venue, session, budget, _ = make_venue(monkeypatch, lambda path, count: Response({}))
        for _ in range(36):
            async with budget.request(priority=1, endpoint='/ordinary'):
                pass
        with pytest.raises(RequestDeferred):
            await venue._get('/api/v1/funding-rates')
        with pytest.raises(RequestDeferred):
            await venue._get('/api/v1/account')
        with pytest.raises(RequestDeferred):
            await venue._get('/api/v1/orderBookDetails')
        await venue._get('/api/v1/accountOrders')
        assert len(session.calls) == 1 and budget.snapshot()['used'] == 37
    asyncio.run(scenario())


def test_funding_exit_cancels_queued_history_without_blocking_order_capacity(
        tmp_path, monkeypatch):
    runtime, _, _, journal = setup_runtime(tmp_path)
    async def scenario():
        queued = asyncio.Event()
        async def sleeping(seconds):
            queued.set()
            await asyncio.Event().wait()
        budget = RequestBudget(sleep=sleeping)
        for _ in range(6):
            async with budget.request(priority=2, endpoint='/history'):
                pass
        async def history():
            async with budget.request(priority=2, endpoint='/history'):
                raise AssertionError('history must stay queued')
        cycle = {'cycle_id': 'offline-history-exit'}
        task = asyncio.create_task(runtime._funding_hold_read(cycle, history))
        await asyncio.wait_for(queued.wait(), .2)
        assert budget.snapshot()['history_queued'] == 1
        runtime._funding_exit_signal(cycle['cycle_id']).set()
        assert await asyncio.wait_for(task, .2) is None
        await asyncio.sleep(0)
        assert budget.snapshot()['history_queued'] == 0
        async with budget.request(priority=0, endpoint='/risk_exit'):
            pass
        assert budget.snapshot()['endpoint_starts']['/risk_exit'] == 1
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('reduce_only', [False, True])
def test_cold_nonce_exit_guard_prevents_late_increase_but_allows_reduction(
        monkeypatch, reduce_only):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda path, count: Response({}))
        requested = [False]
        async def nonce_read(method, path):
            if path.endswith('/nextNonce'):
                requested[0] = True
        signer, rest = install_signer(venue, callback=nonce_read)
        signer.nonce_manager.nonce.clear()
        venue.submission_guard = lambda ref: 'funding_exit_requested' if requested[0] else None
        result = await venue.send_taker(
            is_buy=True, qty=.01, limit_px=100, reduce_only=reduce_only, order_ref='101')
        if reduce_only:
            assert result['unresolved']
            assert rest.calls == [('GET', '/api/v1/nextNonce'), ('POST', '/api/v1/sendTx')]
            assert budget.snapshot()['counters']['started'] == 2
        else:
            assert not result['unresolved'] and result['status'] == 'deferred'
            assert result['deferred'] and result['post_started'] is False
            assert 'proved_local_unsent: submission_guard:funding_exit_requested' == result['err']
            assert rest.calls == [('GET', '/api/v1/nextNonce')]
            assert signer.nonce_manager.nonce == {}
            assert budget.snapshot()['counters']['started'] == 1
        assert budget.snapshot()['reserved_units'] == 0
    asyncio.run(scenario())


def test_runtime_submission_guard_uses_stop_exit_and_existing_risk_freshness(
        tmp_path, monkeypatch):
    runtime, _, _, journal = setup_runtime(tmp_path)
    cycle = {'cycle_id': 'funding-guard'}
    journal.active_live_cycle = lambda: cycle
    runtime.settings = replace(runtime.settings, strategy_mode='funding_hedge')
    assert runtime._lighter_increase_guard('101') == 'funding_risk_unavailable'
    runtime._funding_exit_signal(cycle['cycle_id']).set()
    assert runtime._lighter_increase_guard('101') == 'funding_exit_requested'
    runtime.stop.set()
    assert runtime._lighter_increase_guard('101') == 'runtime_stop'
    journal.close()


@pytest.mark.parametrize('blocked,remaining,reserve_denied,opens', [
    (300, 48, False, False), (0, 1, False, True), (0, 1, True, False),
])
def test_entry_uses_shared_cooldown_and_real_reservation_not_fixed_two_request_guess(
        tmp_path, blocked, remaining, reserve_denied, opens):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    async def scenario():
        await runtime.initialize()
        entropy.set_book(98, 98.01)
        hedge.set_book(99.99, 100)
        await runtime._refresh_flat_lease()
        hedge.request_diagnostics = lambda: {
            'blocked_for_sec': blocked, 'high_priority_remaining': remaining}
        if reserve_denied:
            def unavailable():
                raise RequestDeferred('request_budget', 60)
            hedge.reserve_completion = unavailable
        await runtime._maybe_open(runtime._market(), runtime_module.dec('0'))
        if opens:
            assert len(entropy.sent) == len(hedge.sent) == 1
            cycle = journal.active_live_cycle()
            assert cycle['phase'] == 'OPEN' and cycle['entropy_units'] == -cycle['hedge_units'] != 0
        else:
            assert entropy.sent == hedge.sent == []
            assert journal.active_live_cycle() is None and journal.pending() == []
            assert runtime.status.current_snapshot['reason'] == (
                'completion_resource_unavailable' if reserve_denied else 'lighter_http_request_deferred')
    try:
        asyncio.run(scenario())
    finally:
        journal.close()
