import asyncio
from types import SimpleNamespace

import pytest

from entropy_arb.lighter_requests import RequestBudget, RequestDeferred

from entropy_arb.venue_lighter import (
    AccountOrdersFeed,
    AccountSnapshot,
    LighterVenue,
    LighterWafBlocked,
)


def account_frame(position="0.085", sign=-1):
    return {
        "type": "subscribed/account_all",
        "positions": {
            "139": {"market_id": 139, "position": position, "sign": sign},
        },
        "assets": {
            "3": {"margin_balance": "32.305", "multiplier": "1"},
        },
    }


def bare_venue(feed=None):
    venue = object.__new__(LighterVenue)
    venue.name = "lighter"
    venue.market_id = 139
    venue.orders_feed = feed
    venue.signer = None
    venue.conf = SimpleNamespace(
        lighter_creds=SimpleNamespace(account_index=42))
    return venue


def test_account_all_snapshot_is_authoritative_and_updates_to_flat():
    feed = AccountOrdersFeed("test", "unused", 139, 42, None)
    feed.connection_generation = 7
    feed.last_liveness_ts = __import__("time").time()
    feed._handle_account(account_frame())
    first = feed.fresh_account_snapshot()
    assert first is not None and first.generation == 7
    assert first.source == "lighter_account_all_ws"
    assert first.equity == pytest.approx(32.305)

    feed._handle_account({
        "type": "update/account_all",
        "positions": {
            "139": {"market_id": 139, "position": "0", "sign": 1},
        },
    })
    venue = bare_venue(feed)
    snapshot = asyncio.run(venue.fetch_position_snapshot())
    assert snapshot.position == 0
    assert snapshot.authoritative
    assert snapshot.source == "lighter_account_all_ws"
    assert asyncio.run(venue.fetch_equity()) == pytest.approx((32.305, 32.305))


def test_fresh_account_all_position_and_equity_do_not_touch_rest():
    feed = AccountOrdersFeed("test", "unused", 139, 42, None)
    feed.connection_generation = 1
    feed.last_liveness_ts = __import__("time").time()
    feed._handle_account(account_frame())
    venue = bare_venue(feed)

    async def no_rest():
        raise AssertionError("fresh account_all must not call REST")

    venue.fetch_account_snapshot = no_rest

    async def scenario():
        position = await venue.fetch_position_snapshot()
        equity = await venue.fetch_equity()
        assert position.position == pytest.approx(-0.085)
        assert position.authoritative
        assert equity == pytest.approx((32.305, 32.305))

    asyncio.run(scenario())


def test_disconnect_reconnect_and_malformed_frames_are_unknown():
    feed = AccountOrdersFeed("test", "unused", 139, 42, None)
    feed.connection_generation = 1
    feed.last_liveness_ts = __import__("time").time()
    feed._handle_account(account_frame())
    old = feed.account_snapshot
    assert old is not None

    feed.connection_generation += 1
    feed._invalidate_account()
    assert feed.fresh_account_snapshot() is None

    feed.account_snapshot = old
    feed.account_ready.set()
    assert feed.fresh_account_snapshot() is None

    feed._handle_account({"type": "update/account_all", "positions": []})
    assert feed.fresh_account_snapshot() is None


def test_account_all_non_position_delta_preserves_authoritative_state():
    feed = AccountOrdersFeed("test", "unused", 139, 42, None)
    feed.connection_generation = 1
    feed.last_liveness_ts = __import__("time").time()
    feed._handle_account(account_frame())

    feed._handle_account({
        "type": "update/account_all",
        "trades": {"123": {"market_id": 139}},
    })

    venue = bare_venue(feed)
    snapshot = asyncio.run(venue.fetch_position_snapshot())
    assert snapshot.position == pytest.approx(-0.085)
    assert snapshot.authoritative


def test_partial_asset_delta_cannot_zero_account_equity():
    feed = AccountOrdersFeed("test", "unused", 139, 42, None)
    feed.connection_generation = 1
    feed.last_liveness_ts = __import__("time").time()
    feed._handle_account(account_frame())
    feed._handle_account({
        "type": "update/account_all",
        "assets": {"3": {"asset_index": 3}},
    })
    snapshot = feed.fresh_account_snapshot()
    assert snapshot is not None
    assert snapshot.equity == pytest.approx(32.305)


@pytest.mark.parametrize("assets", [{}, {"3": {"asset_index": 3}},
                                      {"3": {"margin_balance": "nan"}}])
def test_incomplete_account_assets_are_not_authoritative(assets):
    feed = AccountOrdersFeed("test", "unused", 139, 42, None)
    feed.connection_generation = 1
    feed.last_liveness_ts = __import__("time").time()
    frame = account_frame()
    frame["assets"] = assets
    feed._handle_account(frame)
    assert feed.fresh_account_snapshot() is None


def test_rest_missing_equity_does_not_become_zero():
    venue = bare_venue()

    async def missing_equity():
        return {"positions": [], "available_balance": "32"}

    venue._raw_account = missing_equity
    with pytest.raises(RuntimeError, match="equity unavailable"):
        asyncio.run(venue.fetch_account_snapshot())


def test_stale_ws_falls_back_to_rest_position():
    feed = AccountOrdersFeed("test", "unused", 139, 42, None)
    feed.connection_generation = 1
    feed.last_liveness_ts = 0.0
    feed._handle_account(account_frame())
    venue = bare_venue(feed)

    async def rest():
        return AccountSnapshot(40, 30, (
            {"market_id": 139, "position": "0.125", "sign": 1},
        ), 123.0, "raw_rest_fallback")

    venue.fetch_account_snapshot = rest
    snapshot = asyncio.run(venue.fetch_position_snapshot())
    assert snapshot.position == pytest.approx(0.125)
    assert snapshot.source == "lighter_account_rest:raw_rest_fallback"


def test_waf_captcha_uses_shared_budget_without_duplicate_account_circuit():
    venue = bare_venue()
    budget = RequestBudget()
    venue._request_budget = lambda: budget
    calls = 0

    async def blocked():
        nonlocal calls
        calls += 1
        budget.defer(405, {'x-amzn-waf-action': 'captcha'})
        raise LighterWafBlocked("captcha")

    venue._raw_account = blocked

    async def scenario():
        with pytest.raises(LighterWafBlocked):
            await venue.fetch_account_snapshot()
        with pytest.raises(RequestDeferred, match='captcha'):
            async with budget.request(priority=1):
                await venue.fetch_account_snapshot()

    asyncio.run(scenario())
    assert calls == 1


def test_lighter_ready_requires_orders_and_account_streams():
    feed = AccountOrdersFeed("test", "unused", 139, 42, None)
    venue = bare_venue(feed)
    venue._submit_tasks = set()
    feed.ready.set()
    assert not venue.ready_to_trade()
    feed.connection_generation = 1
    feed.last_liveness_ts = __import__("time").time()
    feed._handle_account(account_frame())
    assert venue.ready_to_trade()
