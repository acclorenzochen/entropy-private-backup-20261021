import asyncio
from email.utils import formatdate

import pytest

from entropy_arb.lighter_requests import RequestBudget, RequestDeferred, shared_budget


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


def make_budget(clock=None, **settings):
    clock = clock or Clock()
    return RequestBudget(clock=clock, wall_clock=lambda: 1_700_000_000.0, **settings)


def start(budget, count=1, priority=0, endpoint="sendTx"):
    ticket = budget.reserve(count, priority)
    for _ in range(count):
        budget.consume_reserved(ticket, endpoint)
    return ticket


def test_ordinary_ceiling_preserves_twelve_transaction_units():
    budget = make_budget()
    start(budget, 36, priority=1, endpoint="account")
    with pytest.raises(RequestDeferred, match="request_budget"):
        budget.reserve(1, priority=1)
    start(budget, 12)
    with pytest.raises(RequestDeferred):
        budget.reserve()
    state = budget.snapshot()
    assert state["used"] == 48
    assert state["counters"]["started"] == 48
    assert state["endpoint_starts"] == {"account": 36, "sendTx": 12}


def test_rolling_minute_exact_boundary_and_staggered_starts():
    clock = Clock()
    budget = make_budget(clock, limit=60, reserve=0)
    start(budget, 20)
    clock.now += 10
    start(budget, 40)
    clock.now = 159.999
    with pytest.raises(RequestDeferred) as exc:
        budget.reserve()
    assert exc.value.retry_after == pytest.approx(0.001)
    clock.now = 160.0
    start(budget, 20)
    assert budget.snapshot()["used"] == 60
    with pytest.raises(RequestDeferred) as exc:
        budget.reserve()
    assert exc.value.retry_after == pytest.approx(10.0)


def test_history_has_six_request_limit_independent_of_total():
    budget = make_budget()
    start(budget, 6, priority=2, endpoint="trades")
    with pytest.raises(RequestDeferred) as exc:
        budget.reserve(1, priority=2)
    assert exc.value.reason == "history_budget"
    start(budget, 30, priority=1, endpoint="account")
    start(budget, 12)
    assert budget.snapshot()["used"] == 48


def test_prepaid_units_refresh_to_actual_start_and_release_only_unused():
    clock = Clock()
    budget = make_budget(clock)
    ticket = budget.reserve(2)
    assert ticket.remaining == 2
    assert budget.snapshot()["reserved_units"] == 2
    assert budget.snapshot()["counters"].get("started", 0) == 0
    clock.now = 150.0
    budget.consume_reserved(ticket, "sendTx")
    assert ticket.remaining == 1
    budget.release_reserved(ticket)
    assert ticket.remaining == 0
    state = budget.snapshot()
    assert state["used"] == 1 and state["reserved_units"] == 0
    assert state["endpoint_starts"] == {"sendTx": 1}
    clock.now = 160.0
    assert budget.snapshot()["used"] == 1
    clock.now = 210.0
    assert budget.snapshot()["used"] == 0


def test_expired_prepaid_unit_must_reacquire_current_window():
    clock = Clock()
    budget = make_budget(clock)
    ticket = budget.reserve(2)
    clock.now = 160.0
    start(budget, 48)
    with pytest.raises(RequestDeferred) as exc:
        budget.consume_reserved(ticket, "sendTx")
    assert exc.value.reason == "request_budget"
    assert budget.snapshot()["counters"]["started"] == 48
    clock.now = 220.0
    budget.consume_reserved(ticket, "sendTx")
    assert budget.snapshot()["used"] == 1
    clock.now = 279.999
    assert budget.snapshot()["used"] == 1
    clock.now = 280.0
    assert budget.snapshot()["used"] == 0


def test_prepaid_unit_cannot_bypass_shared_exchange_cooldown():
    budget = make_budget()
    ticket = budget.reserve(2)
    assert budget.defer(429, {}) == "http_429"
    with pytest.raises(RequestDeferred, match="http_429"):
        budget.consume_reserved(ticket, "sendTx")
    budget.release_reserved(ticket)
    assert budget.snapshot()["used"] == 0


def test_ticket_identity_and_ownership_prevent_accidental_refunds():
    budget = make_budget()
    first, second = budget.reserve(), budget.reserve()
    budget.consume_reserved(first, "sendTx")
    budget.release_reserved(second)
    assert budget.snapshot()["used"] == 1
    budget.release_reserved(first)
    assert budget.snapshot()["used"] == 1
    with pytest.raises(ValueError, match="another budget"):
        make_budget().consume_reserved(first)
    with pytest.raises(ValueError, match="no unused units"):
        budget.consume_reserved(first)


def test_history_waiting_for_budget_does_not_hold_up_trading():
    async def scenario():
        clock = Clock()
        sleeping, resume = asyncio.Event(), asyncio.Event()
        delays = []

        async def sleep(delay):
            delays.append(delay)
            sleeping.set()
            await resume.wait()
            clock.now += delay

        budget = make_budget(clock, sleep=sleep)
        start(budget, 6, priority=2)
        start(budget, 30, priority=1)

        async def history():
            async with budget.request(priority=2, endpoint="trades"):
                return "done"

        queued = asyncio.create_task(history())
        await sleeping.wait()
        assert not budget._history_lock.locked()
        async with budget.request(priority=0, cost=12, endpoint="sendTx"):
            assert budget.snapshot()["used"] == 48
        assert budget.snapshot()["history_queued"] == 1
        resume.set()
        assert await queued == "done"
        assert delays == [60.0]
        assert budget.snapshot()["history_queued"] == 0

    asyncio.run(scenario())


def test_slow_history_is_serial_but_account_and_transaction_do_not_wait():
    async def scenario():
        budget = make_budget()
        entered, finish = asyncio.Event(), asyncio.Event()
        order = []

        async def history(name):
            async with budget.request(priority=2, endpoint="trades"):
                order.append(name)
                if name == "first":
                    entered.set()
                    await finish.wait()

        first = asyncio.create_task(history("first"))
        await entered.wait()
        second = asyncio.create_task(history("second"))
        await asyncio.sleep(0)
        assert budget.snapshot()["history_queued"] == 1
        assert budget.snapshot()["history_in_flight"] == 1
        async with budget.request(priority=1, endpoint="account"):
            order.append("account")
        async with budget.request(priority=0, endpoint="sendTx"):
            order.append("transaction")
        assert order == ["first", "account", "transaction"]
        finish.set()
        await asyncio.gather(first, second)
        assert order[-1] == "second"
        assert budget.snapshot()["history_in_flight"] == 0

    asyncio.run(scenario())


def test_cancelled_history_queue_cleans_counts_without_cancelling_io():
    async def scenario():
        budget = make_budget()
        entered, finish = asyncio.Event(), asyncio.Event()

        async def history():
            async with budget.request(priority=2, endpoint="positionFunding"):
                entered.set()
                await finish.wait()

        first = asyncio.create_task(history())
        await entered.wait()
        queued = asyncio.create_task(history())
        await asyncio.sleep(0)
        queued.cancel()
        with pytest.raises(asyncio.CancelledError):
            await queued
        assert not first.done()
        assert budget.snapshot()["history_queued"] == 0
        assert budget.snapshot()["history_in_flight"] == 1
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not budget._history_lock.locked()
        assert budget.snapshot()["used"] == 1
        assert budget.snapshot()["queued"] == 0

    asyncio.run(scenario())


def test_cancelled_budget_wait_cleans_history_queue_and_never_starts_request():
    async def scenario():
        sleeping = asyncio.Event()

        async def sleep(_):
            sleeping.set()
            await asyncio.Event().wait()

        budget = make_budget(sleep=sleep)
        start(budget, 6, priority=2)

        async def history():
            async with budget.request(priority=2, endpoint="trades"):
                pytest.fail("budget was exhausted")

        task = asyncio.create_task(history())
        await sleeping.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert budget.snapshot()["queued"] == 0
        assert budget.snapshot()["history_queued"] == 0
        assert budget.snapshot()["counters"]["started"] == 6
        assert not budget._history_lock.locked()

    asyncio.run(scenario())


def test_wait_false_history_never_waits_for_inflight_history():
    async def scenario():
        budget = make_budget()
        async with budget.request(priority=2, endpoint="trades"):
            with pytest.raises(RequestDeferred, match="history_in_flight"):
                async with budget.request(priority=2, wait=False):
                    pytest.fail("must not overlap history")
        assert budget.snapshot()["queued"] == 0

    asyncio.run(scenario())


def test_waf_classification_requires_405_and_captcha_header():
    clock = Clock()
    budget = make_budget(clock)
    assert budget.defer(200, {"x-amzn-waf-action": "captcha"}) is None
    assert budget.defer(405, {}) == "http_405"
    assert budget.snapshot()["blocked_for_sec"] == 60
    assert budget.defer(405, {"X-Amzn-Waf-Action": "CAPTCHA"}) == "captcha"
    assert budget.snapshot()["blocked_for_sec"] == 300
    budget.record_success()
    assert budget.snapshot()["blocked_for_sec"] == 300
    assert budget.defer(405, {"x-amzn-waf-action": "captcha"}) == "captcha"
    assert budget.snapshot()["blocked_for_sec"] == 900
    budget.defer(405, {"x-amzn-waf-action": "captcha"})
    assert budget.snapshot()["blocked_for_sec"] == 1800
    clock.now += 1800
    budget.record_success()
    budget.defer(405, {"x-amzn-waf-action": "captcha"})
    assert budget.snapshot()["blocked_for_sec"] == 300


def test_retry_after_seconds_http_date_and_invalid_values():
    clock = Clock()
    budget = make_budget(clock)
    budget.defer(429, {"Retry-After": "180"})
    assert budget.snapshot()["blocked_for_sec"] == 180
    budget.defer(429, {"Retry-After": formatdate(1_700_000_500, usegmt=True)})
    assert budget.snapshot()["blocked_for_sec"] == 500
    budget.defer(429, {"Retry-After": "garbage"})
    assert budget.snapshot()["blocked_for_sec"] == 500
    budget.defer(429, {"Retry-After": "inf"})
    assert budget.snapshot()["blocked_for_sec"] == 500
    budget.defer(429, {"Retry-After": "-20"})
    assert budget.snapshot()["blocked_for_sec"] == 900
    budget.defer(429, {})
    assert budget.snapshot()["blocked_for_sec"] == 900


def test_request_cooldown_applies_to_every_priority_and_clears_naturally():
    async def scenario():
        clock = Clock()
        budget = make_budget(clock)
        budget.defer(429, {})
        for priority in (0, 1, 2):
            with pytest.raises(RequestDeferred, match="http_429"):
                async with budget.request(priority=priority, wait=False):
                    pytest.fail("must not bypass server cooldown")
        assert budget.snapshot()["counters"].get("started", 0) == 0
        clock.now += 60
        async with budget.request(priority=0, endpoint="sendTx"):
            pass
        assert budget.snapshot()["blocked_reason"] == ""

    asyncio.run(scenario())


def test_shared_registry_is_host_and_loop_scoped():
    async def scenario():
        first = shared_budget("https://MAINNET.example/api/v1/")
        assert shared_budget("https://mainnet.example") is first
        assert shared_budget("https://different.example") is not first
        with pytest.raises(ValueError, match="conflicting"):
            shared_budget("https://mainnet.example", limit=60)
        return first

    first = asyncio.run(scenario())
    assert asyncio.run(scenario()) is not first


def test_endpoint_observability_never_keeps_query_strings():
    budget = make_budget()
    start(budget, endpoint="https://exchange.example/api/v1/trades?auth=secret#fragment")
    assert budget.snapshot()["endpoint_starts"] == {"/api/v1/trades": 1}
    assert "secret" not in str(budget.snapshot())


@pytest.mark.parametrize("settings", [
    {"limit": 61}, {"limit": 0}, {"reserve": 48},
    {"reserve": -1}, {"history_limit": 37}, {"history_limit": -1},
])
def test_invalid_budget_configuration_is_rejected(settings):
    with pytest.raises(ValueError):
        make_budget(**settings)
