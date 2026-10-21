"""Public metadata startup retries; no account, signer or transaction replay."""
import asyncio
import json

import aiohttp
import pytest

import entropy_arb.venue_lighter as lighter_module
from entropy_arb.lighter_requests import RequestDeferred
from entropy_arb.venue_lighter import LighterWafBlocked
from tests_v3.test_lighter_request_integration import Response, make_venue, no_network
from tests_v3.test_lighter_startup_verification import fast_backoff
from tests_v3.test_runtime import setup_runtime


def market_payload(*, symbol='SNDK', status='active'):
    return {'order_books': [{
        'symbol': symbol, 'status': status, 'market_id': 139,
        'supported_price_decimals': 2, 'supported_size_decimals': 4,
        'supported_quote_decimals': 6, 'min_base_amount': '0.0035',
        'min_quote_amount': '10', 'taker_fee': '0',
    }]}


class BodyResponse(Response):
    def __init__(self, *, error=None, **kwargs):
        super().__init__(market_payload(), **kwargs)
        self.error, self.closed = error, False

    async def json(self):
        if self.error is not None:
            raise self.error
        return await super().json()

    async def __aexit__(self, *args):
        self.closed = True
        return False


def test_healthy_metadata_has_one_request_and_no_sleep(monkeypatch):
    async def scenario():
        venue, session, budget, clock = make_venue(
            monkeypatch, lambda path, count: BodyResponse())
        delays = fast_backoff(monkeypatch, clock)
        await venue.load_market()
        assert len(session.calls) == 1 and delays == []
        assert venue.market_id == 139 and venue.min_quote == 10
        assert session.responses[0].closed and venue.signer is None
        assert budget.snapshot()['endpoint_starts']['/api/v1/orderBooks'] == 1
        assert session.calls[0][1]['timeout'].total == lighter_module.REST_TIMEOUT == 10
    asyncio.run(scenario())


def test_body_timeouts_retry_budgeted_reads_and_release_responses(monkeypatch):
    async def scenario():
        venue, session, budget, clock = make_venue(monkeypatch,
            lambda path, count: BodyResponse(error=TimeoutError() if count < 3 else None))
        delays = fast_backoff(monkeypatch, clock)
        await venue.load_market()
        assert len(session.calls) == 3 and delays == [1, 2]
        assert all(r.closed for r in session.responses)
        assert venue.market_id == 139 and venue.signer is None
        assert budget.snapshot()['endpoint_starts']['/api/v1/orderBooks'] == 3
        assert budget.snapshot()['queued'] == 0
    asyncio.run(scenario())


def test_repeated_timeouts_are_bounded_and_explain_endpoint(monkeypatch):
    async def scenario():
        venue, session, budget, clock = make_venue(monkeypatch,
            lambda path, count: BodyResponse(error=TimeoutError()))
        delays = fast_backoff(monkeypatch, clock)
        with pytest.raises(TimeoutError, match=r'orderBooks.*3.*10'):
            await venue.load_market()
        assert len(session.calls) == 3 and delays == [1, 2]
        assert all(r.closed for r in session.responses)
        assert budget.snapshot()['queued'] == 0 and venue.signer is None
    asyncio.run(scenario())


def test_retry_honors_request_circuit_without_another_http_start(monkeypatch):
    async def scenario():
        class DeferredResponse(BodyResponse):
            async def json(self):
                budget._blocked_until = clock.monotonic() + 20
                budget._blocked_reason = 'http_429'
                raise TimeoutError()
        venue, session, budget, clock = make_venue(monkeypatch,
            lambda path, count: DeferredResponse())
        delays = fast_backoff(monkeypatch, clock)
        with pytest.raises(RequestDeferred):
            await venue.load_market()
        assert len(session.calls) == 1 and delays == [1]
        assert session.responses[0].closed and budget.snapshot()['queued'] == 0
    asyncio.run(scenario())


@pytest.mark.parametrize('error', [
    aiohttp.ClientResponseError(None, (), status=401),
    aiohttp.ClientResponseError(None, (), status=403),
    aiohttp.ClientResponseError(None, (), status=429),
    aiohttp.ClientResponseError(None, (), status=500),
    LighterWafBlocked('captcha'), RequestDeferred('http_429', 3),
    json.JSONDecodeError('invalid', '', 0),
])
def test_non_timeout_failures_are_not_retried(monkeypatch, error):
    async def scenario():
        def fail(path, count):
            raise error
        venue, session, _, clock = make_venue(monkeypatch, fail)
        delays = fast_backoff(monkeypatch, clock)
        with pytest.raises(type(error)) as caught:
            await venue.load_market()
        assert caught.value is error
        assert len(session.calls) == 1 and delays == [] and venue.signer is None
    asyncio.run(scenario())


@pytest.mark.parametrize('payload', [market_payload(symbol='OTHER'), market_payload(status='inactive')])
def test_invalid_market_metadata_fails_without_retry(monkeypatch, payload):
    async def scenario():
        venue, session, _, clock = make_venue(monkeypatch,
            lambda path, count: Response(payload))
        venue.profile.name = 'fake-mainnet'
        delays = fast_backoff(monkeypatch, clock)
        with pytest.raises(RuntimeError):
            await venue.load_market()
        assert len(session.calls) == 1 and delays == []
    asyncio.run(scenario())


def test_cancellation_during_response_closes_read_without_retry(monkeypatch):
    async def scenario():
        started = asyncio.Event()
        venue, session, budget, _ = make_venue(monkeypatch,
            lambda path, count: BodyResponse(gate=asyncio.Event(), started=started))
        task = asyncio.create_task(venue.load_market())
        await asyncio.wait_for(started.wait(), .2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(session.calls) == 1
        assert session.responses[0].closed and session.responses[0].cancelled
        assert budget.snapshot()['queued'] == 0
    asyncio.run(scenario())


def test_cancellation_during_backoff_starts_no_more_reads(monkeypatch):
    async def scenario():
        venue, session, budget, _ = make_venue(monkeypatch,
            lambda path, count: BodyResponse(error=TimeoutError()))
        sleeping = asyncio.Event()
        async def sleep(delay):
            sleeping.set()
            await asyncio.Event().wait()
        monkeypatch.setattr(lighter_module.asyncio, 'sleep', sleep)
        task = asyncio.create_task(venue.load_market())
        try:
            await asyncio.wait_for(sleeping.wait(), .2)
        finally:
            task.cancel()
            with pytest.raises((asyncio.CancelledError, TimeoutError)):
                await task
        assert len(session.calls) == 1 and session.responses[0].closed
        assert budget.snapshot()['queued'] == 0
    asyncio.run(scenario())


def test_failed_runtime_startup_closes_both_venues_before_any_signer(monkeypatch, tmp_path):
    async def scenario():
        venue, session, _, clock = make_venue(monkeypatch,
            lambda path, count: BodyResponse(error=TimeoutError()))
        fast_backoff(monkeypatch, clock)
        runtime, entropy, hedge, journal = setup_runtime(tmp_path)
        hedge.load_market = venue.load_market
        closed = []
        for item in (entropy, hedge):
            def forbidden(*args, **kwargs):
                raise AssertionError('failed metadata must not initialize signer or feeds')
            item.init_signer = item.start_tasks = forbidden
            async def close(key=item.key):
                closed.append(key)
            item.close = close
        try:
            with pytest.raises(TimeoutError, match='orderBooks'):
                await runtime.run()
            assert sorted(closed) == ['entropy', 'hedge'] and runtime.stop.is_set()
            assert runtime.operator_control is None and runtime._reference_writer is None
            assert entropy.sent == hedge.sent == []
            assert journal.active_live_cycle() is None and not journal.pending()
            assert all(r.closed for r in session.responses)
        finally:
            journal.close()
    asyncio.run(scenario())
