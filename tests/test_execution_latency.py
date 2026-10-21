"""Latency regressions at real adapter/book seams; no exchange requests."""
import asyncio
import json
import time
from types import SimpleNamespace

import pytest

from entropy_arb.book import OrderBook
from entropy_arb.venue_hl import HLOrdersFeed, HLVenue
from entropy_arb.venue_lighter import AccountOrdersFeed, LighterVenue


def test_book_sorts_at_most_once_per_side_per_update(monkeypatch):
    book = OrderBook()
    book.apply_lighter({"bids": [{"price": str(99-i/100), "size": "1"}
                                  for i in range(1000)],
                        "asks": [{"price": str(100+i/100), "size": "1"}
                                  for i in range(1000)]}, snapshot=True)
    import builtins
    original = builtins.sorted
    calls = []

    def counted(*a, **kw):
        calls.append(1)
        return original(*a, **kw)

    monkeypatch.setattr(builtins, "sorted", counted)
    for _ in range(10):
        assert book.sorted_bids()[0][0] == 99
        assert book.sorted_asks()[0][0] == 100
    assert len(calls) <= 2


def test_hl_first_confirmation_retry_does_not_wait_half_second(monkeypatch):
    venue = HLVenue.__new__(HLVenue)
    venue.account = SimpleNamespace(query_address="test-account")
    venue.settle_timeout = 2
    responses = iter([
        {"status": "order", "order": {"status": "open", "order": {}}},
        {"status": "order", "order": {"status": "filled", "order": {
            "origSz": "1", "sz": "0", "avgPx": "100"}}},
    ])
    delays = []

    async def info(_):
        return next(responses)

    async def sleep(delay):
        delays.append(delay)

    venue._info = info
    monkeypatch.setattr(asyncio, "sleep", sleep)
    result = asyncio.run(venue.resolve_order("test-order"))
    assert result["filled_base"] == 1
    assert delays[0] <= .1


def test_hl_order_and_fill_stream_join_cloid_to_actual_fill():
    async def run():
        feed = HLOrdersFeed("test", "unused", "0xabc", "io:SNDK")
        future = feed.watch("0x01")
        feed._handle_orders([{
            "order": {"coin": "io:SNDK", "cloid": "0x01", "oid": 7,
                      "origSz": "1", "sz": "0.6"},
            "status": "canceled",
        }])
        assert not future.done(), "partial fill price must come from userFills"
        feed._handle_fills({"fills": [{
            "coin": "io:SNDK", "oid": 7, "tid": 701, "sz": "0.4", "px": "101",
        }]})
        result = await future
        assert result["filled_base"] == pytest.approx(.4)
        assert result["avg_px"] == pytest.approx(101)

    asyncio.run(run())


def test_hl_order_stream_waits_for_all_partial_fills():
    async def run():
        feed = HLOrdersFeed("test", "unused", "0xabc", "io:SNDK")
        future = feed.watch("0x02")
        feed._handle_orders([{
            "order": {"coin": "io:SNDK", "cloid": "0x02", "oid": 8,
                      "origSz": "1", "sz": "0"},
            "status": "FILLED",
        }])
        feed._handle_fills({"fills": [{
            "coin": "io:SNDK", "oid": 8, "tid": 801, "sz": "0.4", "px": "100",
        }]})
        assert not future.done()
        feed._handle_fills({"fills": [{
            "coin": "io:SNDK", "oid": 8, "tid": 802, "sz": "0.6", "px": "101",
        }]})
        result = await future
        assert result["filled_base"] == pytest.approx(1)
        assert result["avg_px"] == pytest.approx(100.6)

    asyncio.run(run())


def test_hl_terminal_ws_does_not_wait_for_slow_exchange_response():
    async def run():
        release = asyncio.Event()
        confirmed = asyncio.Event()
        feed = HLOrdersFeed("test", "unused", "0xabc", "io:SNDK")
        feed.ready.set()

        class Cloid:
            @staticmethod
            def to_raw():
                return "0x01"

        class Signing:
            order_request_to_order_wire = staticmethod(lambda request, asset: {})
            order_wires_to_order_action = staticmethod(lambda wires: {})
            sign_l1_action = staticmethod(lambda *args: {})

        venue = HLVenue.__new__(HLVenue)
        venue.account = SimpleNamespace(
            wallet=object(), is_mainnet=True,
            nonces=SimpleNamespace(next=lambda: 1))
        venue.asset_id = 1
        venue.coin = "io:SNDK"
        venue._signing = Signing()
        venue.orders_feed = feed
        venue._submit_tasks = set()
        venue.settle_timeout = 1
        venue._next_cloid = lambda order_ref=None: Cloid()

        async def post(_payload):
            feed._resolve("0x01", {
                "status": "filled", "filled_base": 1, "avg_px": 100})
            confirmed.set()
            await release.wait()
            return {"status": "ok"}, None, False

        venue._post_exchange = post
        task = asyncio.create_task(venue.send_taker(
            is_buy=True, qty=1, limit_px=100, order_ref="1"))
        try:
            await confirmed.wait()
            done, _ = await asyncio.wait({task}, timeout=.05)
            assert task in done, "authoritative fill is blocked by HTTP ACK"
            assert task.result()["confirmation_source"] == "order_fills_ws"
        finally:
            release.set()
            await task
            for _ in range(3):
                await asyncio.sleep(0)
        assert not venue._submit_tasks

    asyncio.run(run())


def test_lighter_terminal_ws_does_not_wait_for_slow_submit_ack():
    async def run():
        release = asyncio.Event()
        confirmed = asyncio.Event()
        feed = AccountOrdersFeed("test", "unused", 1, 1, None)

        async def create_order(**kw):
            feed._resolve(kw["client_order_index"], {
                "status": "filled", "filled_base": 1, "avg_px": 100})
            confirmed.set()
            await release.wait()
            return None, SimpleNamespace(code=200), None

        venue = LighterVenue.__new__(LighterVenue)
        venue.signer = SimpleNamespace(create_order=create_order)
        venue.orders_feed = feed
        venue._coi = 0
        venue.size_decimals = venue.price_decimals = 2
        venue.market_id = 1
        venue.name = "test"
        venue.settle_timeout = 1
        venue.profile = SimpleNamespace(api_url='https://offline.invalid')
        venue.conf = SimpleNamespace()
        task = asyncio.create_task(venue.send_taker(
            is_buy=True, qty=1, limit_px=100, order_ref="1"))
        try:
            await confirmed.wait()
            done, _ = await asyncio.wait({task}, timeout=.05)
            assert task in done, "authoritative fill is blocked by HTTP submit ACK"
            assert task.result()["filled_base"] == 1
        finally:
            release.set()
            await task
            for _ in range(3):
                await asyncio.sleep(0)

    asyncio.run(run())


def test_book_cache_invalidates_on_updates_but_not_heartbeat():
    book = OrderBook()
    book.apply_hl([[{"px": "99", "sz": "2"}], [{"px": "101", "sz": "3"}]])
    assert book.mid() == 100
    bids = book.sorted_bids()
    bids.clear()  # A caller must not corrupt a shared cached list.
    assert book.sorted_bids() == [(99, 2)]
    assert book.sorted_asks() == [(101, 3)]
    book.touch()
    assert book.mid() == 100
    book.apply_lighter({"bids": [{"price": "99", "size": "0"},
                                   {"price": "98", "size": "4"}]}, False)
    assert book.sorted_bids() == [(98, 4)]
    assert book.best_bid() == 98 and book.mid() == 99.5
    book.apply_lighter({"asks": [{"price": "102", "size": "1"}]}, True)
    assert book.sorted_bids() == [] and book.mid() is None
    assert book.best_ask() == 102
    book.clear()
    assert book.sorted_asks() == [] and book.best_ask() is None


def lighter_fixture(create_order, timeout=.02):
    feed = AccountOrdersFeed("test", "unused", 1, 1, None)
    feed.ready.set()
    feed.account_ready.set()
    venue = LighterVenue.__new__(LighterVenue)
    venue.signer = SimpleNamespace(create_order=create_order)
    venue.orders_feed = feed
    venue._coi = 0
    venue.size_decimals = venue.price_decimals = 2
    venue.market_id = 1
    venue.name = "test"
    venue.settle_timeout = timeout
    venue.profile = SimpleNamespace(api_url='https://offline.invalid')
    venue.conf = SimpleNamespace()
    return venue


@pytest.mark.parametrize("outcome", ["filled", "partial", "canceled", "reject",
                                    "timeout", "transport_error"])
def test_lighter_ack_and_terminal_outcomes(outcome):
    async def run():
        async def submit(**kw):
            if outcome == "transport_error":
                raise ConnectionError("test disconnect")
            if outcome == "reject":
                return None, SimpleNamespace(code=400), "explicit rejection"
            if outcome not in {"timeout"}:
                info = {"status": "filled" if outcome == "filled" else "canceled",
                        "filled_base": {"filled": 1, "partial": .4, "canceled": 0}[outcome],
                        "avg_px": 100}
                # ACK first, terminal afterward, unlike the initial race repro.
                asyncio.get_running_loop().call_later(
                    .001, venue.orders_feed._resolve, kw["client_order_index"], info)
            return None, SimpleNamespace(code=200), None

        venue = lighter_fixture(submit)
        result = await venue.send_taker(is_buy=True, qty=1, limit_px=100)
        assert result["unresolved"] == (outcome in {"timeout", "transport_error"})
        assert result["filled_base"] == {"filled": 1, "partial": .4}.get(outcome, 0)
        assert not venue.orders_feed._pending
        assert not venue._submit_tasks
        assert venue.ready_to_trade()
        if outcome == "timeout":
            venue.orders_feed._resolve(int(result["order_ref"]), {
                "status": "filled", "filled_base": 1, "avg_px": 100})
            late = await venue.resolve_order(result["order_ref"], timeout=0)
            assert late["filled_base"] == 1  # Recover the same order, no resend.

    asyncio.run(run())


def test_lighter_late_ack_serializes_next_send_and_drains_error():
    async def run():
        release = asyncio.Event()
        sent = []

        async def submit(**kw):
            coi = kw["client_order_index"]
            sent.append(coi)
            venue.orders_feed._resolve(coi, {
                "status": "filled", "filled_base": 1, "avg_px": 100})
            if len(sent) == 1:
                await release.wait()
                raise ConnectionError("late error after authoritative fill")
            return None, SimpleNamespace(code=200), None

        venue = lighter_fixture(submit)
        first = await venue.send_taker(is_buy=True, qty=1, limit_px=100)
        assert first["filled_base"] == 1
        assert not venue.ready_to_trade()
        second = asyncio.create_task(venue.send_taker(is_buy=True, qty=1, limit_px=100))
        try:
            await asyncio.sleep(0)
            assert len(sent) == 1
        finally:
            release.set()
            result = await second
        assert result["filled_base"] == 1 and len(sent) == 2
        await asyncio.sleep(0)
        assert not venue._submit_tasks and venue.ready_to_trade()

    asyncio.run(run())


def test_lighter_cancellation_keeps_submission_alive_and_cleans_watcher():
    async def run():
        started, release = asyncio.Event(), asyncio.Event()

        async def submit(**kw):
            started.set()
            await release.wait()
            venue.orders_feed._resolve(kw["client_order_index"], {
                "status": "filled", "filled_base": 1, "avg_px": 100})
            return None, SimpleNamespace(code=200), None

        venue = lighter_fixture(submit)
        task = asyncio.create_task(venue.send_taker(is_buy=True, qty=1, limit_px=100))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not venue.orders_feed._pending
        assert len(venue._submit_tasks) == 1
        assert not next(iter(venue._submit_tasks)).cancelled()
        release.set()
        await asyncio.gather(*tuple(venue._submit_tasks))
        result = await venue.resolve_order("1", timeout=0)
        assert result["filled_base"] == 1
        assert not venue._submit_tasks

    asyncio.run(run())


def test_hl_confirmation_retry_backs_off(monkeypatch):
    delays = []
    venue = HLVenue.__new__(HLVenue)
    venue.account = SimpleNamespace(query_address="test-account")
    venue.settle_timeout = 10

    async def info(_):
        status = "filled" if len(delays) == 4 else "open"
        return {"status": "order", "order": {"status": status, "order": {
            "origSz": "1", "sz": "0", "avgPx": "100"}}}

    async def sleep(delay):
        delays.append(delay)

    venue._info = info
    monkeypatch.setattr(asyncio, "sleep", sleep)
    result = asyncio.run(venue.resolve_order("test-order"))
    assert result["filled_base"] == 1
    assert delays == [.1, .2, .4, .5]


def test_execution_stage_latencies_persist_in_ledger():
    from tests.test_engine import make_engine

    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    try:
        candidate = eng._build_candidates(time.time())[1]
        plan, _ = eng._plan(candidate.buy, candidate.sell,
                            candidate.cap_notional, candidate.threshold_bps)
        trade = eng._execution_bounds(candidate, plan)
        asyncio.run(eng._execute(trade))
        row = eng.ledger._conn.execute("SELECT context_json FROM executions").fetchone()
        timing = json.loads(row[0])["latency_ms"]
        assert set(timing) == {"preflight_to_first_submit", "first_submit_to_result",
                              "first_result_to_hedge_submit", "hedge_submit_to_result",
                              "attempt_to_results"}
        assert all(value >= 0 for value in timing.values())
        assert timing["attempt_to_results"] >= max(
            value for name, value in timing.items() if name != "attempt_to_results")
    finally:
        eng.ledger.close()
