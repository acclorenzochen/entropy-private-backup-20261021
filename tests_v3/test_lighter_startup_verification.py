"""Offline startup verification: retry reads, never replay leverage transactions."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace

import aiohttp
import pytest

import entropy_arb.venue_lighter as lighter_module
from entropy_arb.venue_lighter import WsAccountSnapshot
from tests_v3.test_lighter_request_integration import (
    FakeRest, Response, account_payload, make_venue, no_network,
)
from tests_v3.test_runtime import setup_runtime


def leverage_payload(imf=10):
    payload = account_payload()
    payload['accounts'][0]['positions'][0].update(
        initial_margin_fraction=imf, margin_mode=0)
    return payload


def startup_venue(monkeypatch, handler, *, feed=False, after_submit=None,
                  update_error=None, acknowledgement=200):
    venue, session, budget, clock = make_venue(monkeypatch, handler, feed=feed)
    rest = FakeRest()
    signer = SimpleNamespace(api_client=SimpleNamespace(rest_client=rest), calls=[])

    async def update(market, mode, leverage):
        signer.calls.append((market, mode, leverage))
        await rest.request('GET', 'https://lighter.invalid/api/v1/nextNonce')
        await rest.request('POST', 'https://lighter.invalid/api/v1/sendTx')
        if after_submit is not None:
            after_submit(venue, budget, clock)
        if update_error is not None:
            raise update_error
        resp = None if acknowledgement is None else SimpleNamespace(code=acknowledgement)
        return None, resp, None

    signer.update_leverage = update
    venue.signer = signer
    venue._wrap_sdk_requests()
    return venue, session, budget, clock, signer, rest


def fast_backoff(monkeypatch, clock):
    real_sleep = asyncio.sleep
    delays = []

    async def sleep(delay):
        delays.append(delay)
        clock.advance(delay)
        await real_sleep(0)

    monkeypatch.setattr(lighter_module.asyncio, 'sleep', sleep)
    return delays


def assert_one_submit(signer, rest):
    assert signer.calls == [(139, 0, 10)]
    assert rest.calls == [('GET', '/api/v1/nextNonce'), ('POST', '/api/v1/sendTx')]


def test_slow_response_is_rejected_then_fresh_read_verifies_and_initializes(monkeypatch, tmp_path):
    async def scenario():
        def response(path, count):
            result = Response(leverage_payload())

            async def body():
                if count == 2:
                    assert venue._account_cache is None
                clock.advance(2.01 if count == 1 else .1)
                return result.payload

            result.json = body
            return result

        venue, session, budget, clock, signer, rest = startup_venue(monkeypatch, response)
        delays = fast_backoff(monkeypatch, clock)
        started = clock.time()
        runtime, entropy, hedge, journal = setup_runtime(tmp_path)
        hedge.conf.leverage = 10
        hedge.set_leverage = venue.set_leverage
        streams = []

        def start_tasks(*args, **kwargs):
            assert len(session.calls) == 2
            streams.append(True)
            return []

        entropy.start_tasks = hedge.start_tasks = start_tasks
        try:
            await runtime.initialize()
            assert runtime.status.base['applied_leverage'] == {'hedge': 10}
            assert len(streams) == 2 and entropy.sent == hedge.sent == []
            assert journal.active_live_cycle() is None and not journal.pending()
        finally:
            journal.close()
        assert delays == [.5]
        assert venue._account_cache[0].received_ts == pytest.approx(started + 2.51)
        assert clock.time() - venue._account_cache[0].received_ts == pytest.approx(.1)
        assert budget.snapshot()['endpoint_starts']['/api/v1/account'] == 2
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


@pytest.mark.parametrize('error', [asyncio.TimeoutError(), aiohttp.ClientConnectionError()])
def test_transient_read_failures_recover_without_resubmission(monkeypatch, error):
    async def scenario():
        def response(path, count):
            if count == 1:
                raise error
            return Response(leverage_payload())

        venue, session, _, clock, signer, rest = startup_venue(monkeypatch, response)
        assert fast_backoff(monkeypatch, clock) == []
        await venue.set_leverage(10)
        assert len(session.calls) == 2
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


def test_repeated_read_timeouts_stop_at_deadline_and_cancel_owned_reads(monkeypatch):
    async def scenario():
        venue, session, budget, clock, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(leverage_payload(), gate=asyncio.Event()))
        delays = fast_backoff(monkeypatch, clock)
        real_wait_for = asyncio.wait_for
        limits = []

        async def timed_read(coro, timeout):
            limits.append(timeout)
            try:
                return await real_wait_for(coro, timeout=.005)
            except asyncio.TimeoutError:
                clock.advance(timeout)
                raise

        monkeypatch.setattr(lighter_module.asyncio, 'wait_for', timed_read)
        started = clock.monotonic()
        with pytest.raises(RuntimeError, match='verification limit 30s'):
            await venue.set_leverage(10)
        assert limits == [10, 10, 8.5] and delays == [.5, 1]
        assert clock.monotonic() - started == 30
        assert len(session.calls) == 3 and all(r.cancelled for r in session.responses)
        assert venue._account_cache is None and venue._account_task is None
        assert budget.snapshot()['queued'] == 0
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


def test_cancelling_verification_cleans_owned_read_and_preserves_shared_read(monkeypatch):
    async def scenario():
        gates = [asyncio.Event(), asyncio.Event()]
        starts = [asyncio.Event(), asyncio.Event()]
        venue, session, budget, _, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(
                leverage_payload(), gate=gates[count - 1], started=starts[count - 1]))
        verification = asyncio.create_task(venue.set_leverage(10))
        await asyncio.wait_for(starts[0].wait(), .2)
        shared = asyncio.create_task(venue.fetch_account_snapshot())
        await asyncio.wait_for(starts[1].wait(), .2)
        verification.cancel()
        with pytest.raises(asyncio.CancelledError):
            await verification
        assert session.responses[0].cancelled
        assert not session.responses[1].cancelled and not shared.done()
        gates[1].set()
        assert (await asyncio.wait_for(shared, .2)).equity == 100
        assert budget.snapshot()['queued'] == 0
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


def test_cancelling_backoff_does_not_leave_a_read_or_replay_transaction(monkeypatch):
    async def scenario():
        venue, session, _, _, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(leverage_payload(20)))
        sleeping = asyncio.Event()

        async def sleep(delay):
            sleeping.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(lighter_module.asyncio, 'sleep', sleep)
        verification = asyncio.create_task(venue.set_leverage(10))
        await asyncio.wait_for(sleeping.wait(), .2)
        verification.cancel()
        with pytest.raises(asyncio.CancelledError):
            await verification
        assert len(session.calls) == 1 and venue._account_task is None
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


@pytest.mark.parametrize('delay', [.75, 60])
def test_budget_deferral_is_honored_or_fails_before_another_http_start(monkeypatch, delay):
    async def scenario():
        def block(venue, budget, clock):
            budget._blocked_until = clock.monotonic() + delay
            budget._blocked_reason = 'http_429'

        venue, session, budget, clock, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(leverage_payload()), after_submit=block)
        delays = fast_backoff(monkeypatch, clock)
        if delay < 30:
            await venue.set_leverage(10)
            assert delays == [delay] and len(session.calls) == 1
        else:
            with pytest.raises(RuntimeError, match='request deferred'):
                await venue.set_leverage(10)
            assert delays == [] and session.calls == []
        assert budget.snapshot()['counters']['deferred'] == 1
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


@pytest.mark.parametrize('status', [405, 429, 503, 401, 403])
def test_http_error_retry_obeys_shared_cooldown_and_rejects_auth_failures(monkeypatch, status):
    async def scenario():
        def response(path, count):
            result = Response(leverage_payload())
            if count == 1:
                result.status = status

                def fail():
                    raise aiohttp.ClientResponseError(None, (), status=status)

                result.raise_for_status = fail
            return result

        venue, session, budget, clock, signer, rest = startup_venue(monkeypatch, response)
        delays = fast_backoff(monkeypatch, clock)
        if status == 503:
            await venue.set_leverage(10)
            assert delays == [.5] and len(session.calls) == 2
        else:
            error = RuntimeError if status in (405, 429) else aiohttp.ClientResponseError
            with pytest.raises(error):
                await venue.set_leverage(10)
            assert delays == [] and len(session.calls) == 1
        if status in (405, 429):
            assert budget.snapshot()['blocked_for_sec'] == 60
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


@pytest.mark.parametrize('already_blocked', [False, True])
def test_captcha_circuit_is_never_retried_inside_startup_window(monkeypatch, already_blocked):
    async def scenario():
        def response(path, count):
            result = Response({})
            result.status = 405
            result.headers = {'x-amzn-waf-action': 'captcha'}
            return result

        def blocked_after_ack(venue, budget, clock):
            budget.defer(405, {'x-amzn-waf-action': 'captcha'})
        venue, session, _, clock, signer, rest = startup_venue(
            monkeypatch, response,
            after_submit=blocked_after_ack if already_blocked else None)
        delays = fast_backoff(monkeypatch, clock)
        with pytest.raises(lighter_module.LighterWafBlocked):
            await venue.set_leverage(10)
        assert delays == [] and len(session.calls) == (0 if already_blocked else 1)
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


@pytest.mark.parametrize('post_ack', [False, True])
def test_only_reliable_post_ack_ws_can_verify_without_rest(monkeypatch, post_ack):
    async def scenario():
        venue, session, _, clock, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(leverage_payload()), feed=True)
        feed = venue.orders_feed
        feed.connection_generation = 1
        feed.account_ready.set()

        def snapshot(max_age):
            clock.advance(.001)
            feed.last_liveness_ts = clock.time()
            feed.account_snapshot = WsAccountSnapshot(
                100, 90, tuple(leverage_payload()['accounts'][0]['positions']),
                clock.time() if post_ack else clock.time() - 1, 1)
            return feed.account_snapshot

        monkeypatch.setattr(feed, 'fresh_account_snapshot', snapshot)
        await venue.set_leverage(10)
        assert len(session.calls) == (0 if post_ack else 1)
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


@pytest.mark.parametrize('acknowledgement,error', [
    (None, None), (500, None), (200, asyncio.TimeoutError()),
])
def test_unknown_or_failed_transaction_ack_is_not_replayed_or_verified(monkeypatch, acknowledgement, error):
    async def scenario():
        venue, session, _, _, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(leverage_payload()),
            update_error=error, acknowledgement=acknowledgement)
        with pytest.raises(asyncio.TimeoutError if error is not None else RuntimeError):
            await venue.set_leverage(10)
        assert session.calls == []
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


def test_invalid_account_data_is_not_retried(monkeypatch):
    async def scenario():
        venue, session, _, _, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(account_payload(equity='NaN')))
        with pytest.raises(RuntimeError, match='equity unavailable'):
            await venue.set_leverage(10)
        assert len(session.calls) == 1
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


def test_margin_mismatch_stays_bounded_and_never_resubmits(monkeypatch):
    async def scenario():
        venue, session, _, clock, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(leverage_payload(20)))
        fast_backoff(monkeypatch, clock)
        started = clock.monotonic()
        with pytest.raises(RuntimeError, match='margin settings not observed'):
            await venue.set_leverage(10)
        assert clock.monotonic() - started < 30 and len(session.calls) <= 20
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


@pytest.mark.parametrize('failure', ['not_ready', 'generation', 'not_authoritative', 'stale_liveness'])
def test_unreliable_ws_uses_a_validated_rest_read(monkeypatch, failure):
    async def scenario():
        def publish(venue, budget, clock):
            feed = venue.orders_feed
            feed.connection_generation = 1
            feed.account_ready.set()
            feed.last_liveness_ts = clock.time()
            feed.account_snapshot = WsAccountSnapshot(
                100, 90, tuple(leverage_payload()['accounts'][0]['positions']),
                clock.time() + .001, 1)
            if failure == 'not_ready':
                feed.account_ready.clear()
            elif failure == 'generation':
                feed.connection_generation = 2
            elif failure == 'not_authoritative':
                feed.account_snapshot = replace(feed.account_snapshot, authoritative=False)
            else:
                feed.last_liveness_ts -= 6

        venue, session, _, _, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(leverage_payload()),
            feed=True, after_submit=publish)
        await venue.set_leverage(10)
        assert len(session.calls) == 1
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


def test_superseded_evidence_is_not_retried(monkeypatch):
    async def scenario():
        def response(path, count):
            result = Response(leverage_payload())

            async def body():
                venue._invalidate_account_cache()
                return result.payload

            result.json = body
            return result

        venue, session, _, _, signer, rest = startup_venue(monkeypatch, response)
        with pytest.raises(RuntimeError, match='superseded'):
            await venue.set_leverage(10)
        assert len(session.calls) == 1 and venue._account_cache is None
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


def test_fresh_result_at_deadline_cannot_verify(monkeypatch):
    async def scenario():
        def response(path, count):
            result = Response(leverage_payload())

            async def body():
                clock.mono += 30
                return result.payload

            result.json = body
            return result

        venue, session, _, clock, signer, rest = startup_venue(monkeypatch, response)
        with pytest.raises(RuntimeError, match='verification limit 30s'):
            await venue.set_leverage(10)
        assert len(session.calls) == 1
        assert_one_submit(signer, rest)

    asyncio.run(scenario())


def test_read_attempts_are_capped_even_with_a_stalled_clock(monkeypatch):
    async def scenario():
        venue, session, budget, _, signer, rest = startup_venue(
            monkeypatch, lambda path, count: Response(leverage_payload(20)))
        real_sleep = asyncio.sleep

        async def sleep(delay):
            await real_sleep(0)

        monkeypatch.setattr(lighter_module.asyncio, 'sleep', sleep)
        with pytest.raises(RuntimeError, match='margin settings not observed'):
            await venue.set_leverage(10)
        assert len(session.calls) == 20 and budget.snapshot()['used'] == 22
        assert_one_submit(signer, rest)

    asyncio.run(scenario())
