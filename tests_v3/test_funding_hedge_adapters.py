"""Offline fixtures follow the official userFunding/PositionFunding schemas."""
import asyncio
import socket
from dataclasses import FrozenInstanceError
from decimal import Decimal
from types import SimpleNamespace

import aiohttp
import pytest

from entropy_arb.funding import FundingRiskSnapshot, FundingSettlement
from entropy_arb.venue_hl import HLVenue
from entropy_arb.venue_lighter import AccountSnapshot, LighterVenue


TS = 1_780_000_000.0
D = Decimal


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("real network/signing/trading is forbidden")

    async def forbidden_async(*args, **kwargs):
        forbidden()

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(aiohttp.ClientSession, "_request", forbidden_async)
    for venue in (HLVenue, LighterVenue):
        monkeypatch.setattr(venue, "init_signer", forbidden)
        monkeypatch.setattr(venue, "set_leverage", forbidden_async)
        monkeypatch.setattr(venue, "send_taker", forbidden_async)
    monkeypatch.setattr(LighterVenue, "_fee_auth_header", forbidden)


def hl_venue():
    venue = HLVenue.__new__(HLVenue)
    venue.name = "Entropy"
    venue.coin = "io:NBIS"
    venue.conf = SimpleNamespace(hl_dex="io", symbol="NBIS")
    venue._query_address = lambda: "0xABC"
    return venue


def lighter_venue():
    venue = LighterVenue.__new__(LighterVenue)
    venue.name = "Lighter"
    venue.market_id = 139
    venue.profile = SimpleNamespace(name="robinhood")
    venue.conf = SimpleNamespace(symbol="NBIS", lighter_creds=SimpleNamespace(account_index=42))
    venue._fee_auth_header = lambda: {"authorization": "offline-token"}
    return venue


@pytest.mark.parametrize('index', ['201.25', None, '0', 'NaN'])
def test_hl_optional_oracle_funding_price_not_mark(index):
    venue = hl_venue()

    async def info(payload):
        return [{'universe': [{'name': 'io:NBIS'}]},
                [{'funding': '0.0001', 'oraclePx': index, 'markPx': '999'}]]

    venue._info = info
    if index == '201.25':
        snapshot = asyncio.run(venue.fetch_funding(include_price=True))
        assert snapshot.index_price == D('201.25')
        assert snapshot.rate_bps_per_hour == 1
    else:
        with pytest.raises(ValueError):
            asyncio.run(venue.fetch_funding(include_price=True))
    # The legacy rate-only contract never requires an index price.
    assert asyncio.run(venue.fetch_funding()).index_price is None


@pytest.mark.parametrize('index', ['202.50', None, '0', 'Infinity'])
def test_lighter_optional_index_funding_price_not_mark(index):
    venue = lighter_venue()
    requests = []

    async def get(path, params=None, **kwargs):
        requests.append(path)
        if path.endswith('funding-rates'):
            return {'code': 200, 'funding_rates': [
                {'market_id': 139, 'symbol': 'NBIS', 'exchange': 'lighter', 'rate': '0.0008'}]}
        assert params == {'market_id': 139}
        return {'code': 200, 'order_book_details': [
            {'market_id': 139, 'symbol': 'NBIS', 'market_type': 'perp',
             'index_price': index, 'mark_price': '999'}]}

    venue._get = get
    if index == '202.50':
        snapshot = asyncio.run(venue.fetch_funding(include_price=True))
        assert snapshot.index_price == D('202.50')
        assert snapshot.rate_bps_per_hour == 1
    else:
        with pytest.raises(ValueError):
            asyncio.run(venue.fetch_funding(include_price=True))
    requests.clear()
    assert asyncio.run(venue.fetch_funding()).index_price is None
    assert requests == ['/api/v1/funding-rates']


def hl_event(ts=TS, amount="0", *, coin="io:NBIS", identity="hash", qty="-2.5"):
    return {"time": round(ts * 1000), "hash": identity,
            "delta": {"type": "funding", "coin": coin, "usdc": amount,
                      "szi": qty, "fundingRate": "0", "nSamples": None}}


def lighter_event(ts=TS, amount="0", *, market=139, identity=1, side="short"):
    return {"timestamp": int(ts), "market_id": market, "funding_id": identity,
            "change": amount, "discount": "0", "rate": "0",
            "position_size": "2.5", "position_side": side}


def hl_history(venue, rows):
    requests = []

    async def info(payload):
        assert payload["type"] == "userFunding"
        assert payload["user"] == "0xABC"
        assert "dex" not in payload  # account-wide official request schema
        requests.append(payload)
        return [row for row in rows
                if payload["startTime"] <= row["time"] <= payload["endTime"]][:500]

    venue._info = info
    return requests


def lighter_history(venue, pages):
    requests = []

    async def get(path, params=None, headers=None):
        assert path == "/api/v1/positionFunding"
        assert headers == {"authorization": "offline-token"}
        assert params["account_index"] == 42
        assert params["market_ids"] == "139"
        assert params["side"] == "all"
        assert params["limit"] == 100
        requests.append(params)
        return pages[params.get("cursor")]

    venue._get = get
    return requests


def test_models_keep_explicit_zero_and_unknown_liquidation_and_are_frozen():
    event = FundingSettlement("event", TS, D("0"), D("-2.5"))
    risk = FundingRiskSnapshot(D("2.5"), D("99.12"), None, TS)
    assert event.amount_usd == 0
    assert risk.liquidation_price is None  # open position with unknown risk
    with pytest.raises(FrozenInstanceError):
        event.ts = 0
    with pytest.raises(FrozenInstanceError):
        risk.mark_price = D(1)


@pytest.mark.parametrize("kwargs", [
    {"event_id": ""}, {"ts": float("nan")}, {"ts": -1}, {"ts": TS * 1000},
    {"amount_usd": D("NaN")}, {"amount_usd": D("Infinity")},
    {"amount_usd": 0.0}, {"position_qty": D("-Infinity")},
])
def test_settlement_rejects_invalid_values(kwargs):
    with pytest.raises(ValueError):
        FundingSettlement(**({"event_id": "event", "ts": TS, "amount_usd": D(0)} | kwargs))


@pytest.mark.parametrize("kwargs", [
    {"ts": float("inf")}, {"ts": TS * 1000},
    {"position_qty": D("NaN")}, {"mark_price": D(0)},
    {"mark_price": D("Infinity")}, {"liquidation_price": D(0)},
    {"liquidation_price": D("NaN")}, {"liquidation_price": D(-1)},
])
def test_risk_rejects_invalid_values(kwargs):
    with pytest.raises(ValueError):
        FundingRiskSnapshot(**({"position_qty": D(1), "mark_price": D(99),
                                "liquidation_price": None, "ts": TS} | kwargs))


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
@pytest.mark.parametrize("since,until", [(TS, TS - 1), (-1, TS), (TS, float("nan"))])
def test_invalid_history_windows_fail_before_io(factory, since, until):
    with pytest.raises(ValueError):
        asyncio.run(factory().fetch_funding_settlements(since, until))


def test_hl_pages_account_wide_history_for_full_two_hour_window_and_replays_boundary():
    venue = hl_venue()
    rows = [hl_event(TS + i * 10, "8", coin="xyz:NBIS", identity=str(i))
            for i in range(600)]
    rows += [hl_event(TS + 100, "0"), hl_event(TS + 6000, "-0.123456789012345678"),
             hl_event(TS + 7200, "0.25"), hl_event(TS + 1, "90", coin="NBIS")]
    rows.sort(key=lambda row: row["time"])
    requests = hl_history(venue, rows)
    first = asyncio.run(venue.fetch_funding_settlements(TS, TS + 7200))
    assert len(first) == 3
    assert [event.amount_usd for event in first] == [D(0), D("-0.123456789012345678"), D("0.25")]
    assert all(event.position_qty == D("-2.5") for event in first)
    assert len(requests) == 2
    assert requests[1]["startTime"] == rows[499]["time"]
    assert requests[0]["endTime"] == int((TS + 7200) * 1000)
    assert asyncio.run(venue.fetch_funding_settlements(TS, TS + 7200)) == first
    total = asyncio.run(venue.fetch_funding_payments(TS, TS + 7200))
    assert total == float(D("0.126543210987654322"))


def test_hl_filters_exact_coin_and_submillisecond_interval_and_deduplicates():
    venue = hl_venue()
    rows = [hl_event(TS), hl_event(TS + 0.001, "0.012"),
            hl_event(TS + 0.001, "0.012"), hl_event(TS + 0.002, "0.5"),
            hl_event(TS + 0.001, "9", coin="xyz:NBIS"),
            hl_event(TS + 0.001, "9", coin="NBIS")]
    hl_history(venue, rows)
    result = asyncio.run(venue.fetch_funding_settlements(TS + 0.0005, TS + 0.0015))
    assert len(result) == 1 and result[0].amount_usd == D("0.012")


@pytest.mark.parametrize("amount", [None, "", "NaN", "Infinity"])
def test_hl_missing_or_nonfinite_amount_is_never_zero(amount):
    venue = hl_venue()
    hl_history(venue, [hl_event(amount=amount)])
    with pytest.raises(ValueError):
        asyncio.run(venue.fetch_funding_settlements(TS, TS + 1))


def test_hl_missing_amount_identity_or_conflicting_duplicate_fails():
    venue = hl_venue()
    row = hl_event()
    del row["delta"]["usdc"]
    hl_history(venue, [row])
    with pytest.raises(KeyError):
        asyncio.run(venue.fetch_funding_payments(TS, TS + 1))
    hl_history(venue, [hl_event(identity="")])
    with pytest.raises(RuntimeError, match="hash missing"):
        asyncio.run(venue.fetch_funding_settlements(TS, TS + 1))
    hl_history(venue, [hl_event(amount="1"), hl_event(amount="2")])
    with pytest.raises(RuntimeError, match="conflicting"):
        asyncio.run(venue.fetch_funding_settlements(TS, TS + 1))


def test_hl_saturated_timestamp_and_page_budget_fail_without_partial_results(monkeypatch):
    venue = hl_venue()
    hl_history(venue, [hl_event(identity=str(i)) for i in range(500)])
    with pytest.raises(RuntimeError, match="stalled"):
        asyncio.run(venue.fetch_funding_settlements(TS, TS + 7200))
    monkeypatch.setattr("entropy_arb.venue_hl.FUNDING_HISTORY_MAX_PAGES", 1)
    hl_history(venue, [hl_event(TS + i, identity=str(i)) for i in range(501)])
    with pytest.raises(RuntimeError, match="limit reached"):
        asyncio.run(venue.fetch_funding_payments(TS, TS + 7200))


def test_lighter_cursor_history_uses_seconds_and_decimal_cash_flow_for_two_hours():
    venue = lighter_venue()
    zero = lighter_event(TS + 3600, "0")
    paid = lighter_event(TS + 7200, "-0.123456789012345678", identity=2, side="long")
    requests = lighter_history(venue, {
        None: {"code": 200, "position_fundings": [zero], "next_cursor": "second"},
        "second": {"code": 200, "position_fundings": [zero, paid], "next_cursor": ""},
    })
    result = asyncio.run(venue.fetch_funding_settlements(TS, TS + 7200))
    assert len(result) == 2 and result[0].amount_usd == 0
    assert result[0].position_qty == D("-2.5")
    assert result[1].position_qty == D("2.5")
    assert result[1].amount_usd == D("-0.123456789012345678")
    assert requests[0]["start_timestamp"] == int(TS)
    assert requests[0]["end_timestamp"] == int(TS + 7200)
    assert requests[1]["cursor"] == "second"
    assert asyncio.run(venue.fetch_funding_settlements(TS, TS + 7200)) == result
    assert asyncio.run(venue.fetch_funding_payments(TS, TS + 7200)) == float(result[1].amount_usd)


def test_lighter_filters_market_and_interval_without_substituting_missing_market():
    venue = lighter_venue()
    requests = lighter_history(venue, {None: {"code": 200, "position_fundings": [
        lighter_event(TS), lighter_event(TS + 1, "0.012"),
        lighter_event(TS + 2, "5"), lighter_event(TS + 1, "9", market=1),
    ]}})
    result = asyncio.run(venue.fetch_funding_settlements(TS + 0.5, TS + 1.5))
    assert len(result) == 1 and result[0].amount_usd == D("0.012")
    assert requests[0]["start_timestamp"] == TS
    assert requests[0]["end_timestamp"] == TS + 2
    row = lighter_event()
    del row["market_id"]
    lighter_history(venue, {None: {"code": 200, "position_fundings": [row]}})
    with pytest.raises(RuntimeError, match="market missing"):
        asyncio.run(venue.fetch_funding_settlements(TS, TS + 1))


@pytest.mark.parametrize("updates", [
    {"change": None}, {"change": ""}, {"change": "NaN"}, {"change": "Infinity"},
    {"timestamp": int(TS * 1000)}, {"timestamp": TS + 0.5},
    {"funding_id": None}, {"funding_id": 1.5},
    {"position_side": "buy"}, {"position_size": "-1"},
])
def test_lighter_invalid_history_is_never_a_zero_event(updates):
    venue = lighter_venue()
    lighter_history(venue, {None: {"code": 200,
                                 "position_fundings": [lighter_event() | updates]}})
    with pytest.raises((RuntimeError, ValueError)):
        asyncio.run(venue.fetch_funding_settlements(TS, TS + 1))


def test_lighter_missing_change_or_conflicting_identity_fails():
    venue = lighter_venue()
    row = lighter_event()
    del row["change"]
    lighter_history(venue, {None: {"code": 200, "position_fundings": [row]}})
    with pytest.raises(ValueError):
        asyncio.run(venue.fetch_funding_payments(TS, TS + 1))
    lighter_history(venue, {None: {"code": 200, "position_fundings": [
        lighter_event(amount="1"), lighter_event(amount="2")]}})
    with pytest.raises(RuntimeError, match="conflicting"):
        asyncio.run(venue.fetch_funding_settlements(TS, TS + 1))


@pytest.mark.parametrize("pages,match", [
    ({None: {"code": 200, "position_fundings": [lighter_event()] * 100}}, "no cursor"),
    ({None: {"code": 200, "position_fundings": [lighter_event()], "next_cursor": "a"},
      "a": {"code": 200, "position_fundings": [lighter_event()], "next_cursor": "a"}}, "stalled"),
    ({None: {"code": 200, "position_fundings": [], "next_cursor": "a"}}, "stalled"),
    ({None: {"code": 400, "message": "bad request"}}, "request failed"),
    ({None: {"code": 200}}, "rows missing"),
])
def test_lighter_incomplete_history_fails_closed(pages, match):
    venue = lighter_venue()
    lighter_history(venue, pages)
    with pytest.raises(RuntimeError, match=match):
        asyncio.run(venue.fetch_funding_payments(TS, TS + 7200))


def test_lighter_page_budget_fails_without_returning_partial_cash_flow(monkeypatch):
    monkeypatch.setattr("entropy_arb.venue_lighter.FUNDING_HISTORY_MAX_PAGES", 1)
    venue = lighter_venue()
    lighter_history(venue, {None: {"code": 200, "position_fundings": [lighter_event()],
                                 "next_cursor": "more"}})
    with pytest.raises(RuntimeError, match="limit reached"):
        asyncio.run(venue.fetch_funding_payments(TS, TS + 7200))


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
def test_empty_delayed_ledger_never_creates_settlement(factory):
    venue = factory()
    if isinstance(venue, HLVenue):
        hl_history(venue, [])
    else:
        lighter_history(venue, {None: {"code": 200, "position_fundings": []}})
    assert asyncio.run(venue.fetch_funding_settlements(TS, TS + 7200)) == []
    assert asyncio.run(venue.fetch_funding_payments(TS, TS + 7200)) == 0.0


def hl_risk(venue, *, positions=None, state_ts=TS, mark="99.12"):
    if positions is None:
        positions = [{"position": {"coin": "io:NBIS", "szi": "-2.5",
                                   "liquidationPx": "151.25"}}]
    state = {"assetPositions": positions, "time": state_ts * 1000}
    data = [{"universe": [{"name": "xyz:NBIS"}, {"name": "io:NBIS"}]},
            [{"markPx": "1"}, {"markPx": mark}]]

    async def info(payload):
        assert payload["dex"] == "io"
        if payload["type"] == "clearinghouseState":
            assert payload["user"] == "0xABC"
            return state
        assert payload["type"] == "metaAndAssetCtxs"
        return data

    venue._info = info
    return state, data


def lighter_risk(venue, *, positions=None, account_ts=TS, mark="99.12"):
    if positions is None:
        positions = ({"market_id": 139, "position": "2.5", "sign": -1,
                      "liquidation_price": "151.25"},)
    account = AccountSnapshot(100, 90, positions, account_ts, "offline_account")
    data = {"code": 200, "order_book_details": [
        {"market_id": 139, "symbol": "NBIS", "market_type": "perp", "mark_price": mark}]}

    async def snapshot():
        return account

    async def get(path, params=None):
        assert path == "/api/v1/orderBookDetails" and params == {"market_id": 139}
        return data

    venue.fetch_account_snapshot = snapshot
    venue._get = get
    return data


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
def test_risk_reads_exact_market_signed_qty_and_actual_prices(factory, monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = factory()
    if isinstance(venue, HLVenue):
        hl_risk(venue)
    else:
        lighter_risk(venue)
    result = asyncio.run(venue.fetch_funding_risk())
    assert result.position_qty == D("-2.5")
    assert result.mark_price == D("99.12")
    assert result.liquidation_price == D("151.25")
    assert result.ts == TS


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
@pytest.mark.parametrize("qty,liquidation", [("0", None), ("1", None)])
def test_flat_and_open_positions_with_missing_liquidation_are_not_guessed(
        factory, qty, liquidation, monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = factory()
    if isinstance(venue, HLVenue):
        hl_risk(venue, positions=[{"position": {"coin": "io:NBIS", "szi": qty,
                                                "liquidationPx": liquidation}}])
    else:
        lighter_risk(venue, positions=({"market_id": 139, "position": qty,
                                        "sign": 1, "liquidation_price": liquidation},))
    result = asyncio.run(venue.fetch_funding_risk())
    assert result.position_qty == D(qty) and result.liquidation_price is None


def test_lighter_zero_liquidation_for_open_position_is_unknown(monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = lighter_venue()
    lighter_risk(venue, positions=({"market_id": 139, "position": "1", "sign": 1,
                                   "liquidation_price": "0"},))
    assert asyncio.run(venue.fetch_funding_risk()).liquidation_price is None


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
def test_no_target_position_is_flat_only_with_complete_valid_snapshot(factory, monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = factory()
    if isinstance(venue, HLVenue):
        state, _ = hl_risk(venue, positions=[])
    else:
        lighter_risk(venue, positions=())
    result = asyncio.run(venue.fetch_funding_risk())
    assert result.position_qty == 0 and result.liquidation_price is None
    if isinstance(venue, HLVenue):
        del state["assetPositions"]
        with pytest.raises(RuntimeError, match="positions missing"):
            asyncio.run(venue.fetch_funding_risk())


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
@pytest.mark.parametrize("stamp", [TS - 20, TS + 2, TS * 1000, float("nan")])
def test_stale_future_or_wrong_unit_risk_is_unknown(factory, stamp, monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = factory()
    if isinstance(venue, HLVenue):
        hl_risk(venue, state_ts=stamp)
    else:
        lighter_risk(venue, account_ts=stamp)
    with pytest.raises((RuntimeError, ValueError)):
        asyncio.run(venue.fetch_funding_risk())


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
@pytest.mark.parametrize("mark", [None, "", "0", "-1", "NaN", "Infinity"])
def test_invalid_mark_never_falls_back_to_book_or_entry_price(factory, mark, monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = factory()
    venue.book = SimpleNamespace(mid=99)
    if isinstance(venue, HLVenue):
        hl_risk(venue, mark=mark)
    else:
        lighter_risk(venue, mark=mark)
    with pytest.raises(ValueError):
        asyncio.run(venue.fetch_funding_risk())


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
def test_missing_position_quantity_is_unknown(factory, monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = factory()
    if isinstance(venue, HLVenue):
        hl_risk(venue, positions=[{"position": {"coin": "io:NBIS"}}])
    else:
        lighter_risk(venue, positions=({"market_id": 139, "sign": 1},))
    with pytest.raises((KeyError, ValueError)):
        asyncio.run(venue.fetch_funding_risk())


def test_lighter_account_snapshot_missing_positions_is_unknown():
    venue = lighter_venue()
    venue.signer = None
    venue._account_rest_allowed = lambda: None

    async def account():
        return {"total_asset_value": "100", "available_balance": "90"}

    venue._raw_account = account
    with pytest.raises(RuntimeError, match="positions unavailable"):
        asyncio.run(venue.fetch_account_snapshot())


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
@pytest.mark.parametrize("value", [None, "NaN", "Infinity"])
def test_missing_market_identity_or_invalid_risk_values_fail(factory, value, monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = factory()
    if isinstance(venue, HLVenue):
        row = {"position": {"szi": "1", "liquidationPx": "80"}}
        hl_risk(venue, positions=[row])
    else:
        row = {"position": "1", "sign": 1, "liquidation_price": "80"}
        lighter_risk(venue, positions=(row,))
    with pytest.raises(RuntimeError, match="market|position"):
        asyncio.run(venue.fetch_funding_risk())
    if isinstance(venue, HLVenue):
        row["position"]["coin"] = "io:NBIS"
        row["position"]["szi"] = value
    else:
        row["market_id"] = 139
        row["position"] = value
    with pytest.raises(ValueError):
        asyncio.run(venue.fetch_funding_risk())


@pytest.mark.parametrize("qty,sign", [("-1", 1), ("1", 0), ("1", None),
                                     ("1", 0.5), ("1", True)])
def test_lighter_risk_requires_unsigned_size_and_explicit_valid_sign(qty, sign, monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = lighter_venue()
    lighter_risk(venue, positions=({"market_id": 139, "position": qty, "sign": sign},))
    with pytest.raises(ValueError):
        asyncio.run(venue.fetch_funding_risk())


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1"])
def test_invalid_reported_liquidation_fails(factory, value, monkeypatch):
    monkeypatch.setattr("time.time", lambda: TS + 1)
    venue = factory()
    if isinstance(venue, HLVenue):
        hl_risk(venue, positions=[{"position": {"coin": "io:NBIS", "szi": "1",
                                                "liquidationPx": value}}])
    else:
        lighter_risk(venue, positions=({"market_id": 139, "position": "1", "sign": 1,
                                        "liquidation_price": value},))
    with pytest.raises(ValueError):
        asyncio.run(venue.fetch_funding_risk())


@pytest.mark.parametrize("factory", [hl_venue, lighter_venue])
def test_request_that_takes_too_long_cannot_be_marked_fresh(factory, monkeypatch):
    now = [TS]
    monkeypatch.setattr("time.time", lambda: now[0])
    venue = factory()
    if isinstance(venue, HLVenue):
        hl_risk(venue)
        original = venue._info

        async def delayed(payload):
            now[0] = TS + 21
            return await original(payload)

        venue._info = delayed
    else:
        lighter_risk(venue)
        original = venue.fetch_account_snapshot

        async def delayed():
            now[0] = TS + 21
            account = await original()
            return AccountSnapshot(account.equity, account.available, account.positions,
                                   now[0], account.source)

        venue.fetch_account_snapshot = delayed
    with pytest.raises(RuntimeError, match="stale"):
        asyncio.run(venue.fetch_funding_risk())


@pytest.mark.parametrize("raw,expected", [(0.0001, 0.125), (0, 0), (-0.0008, -1)])
def test_lighter_funding_comparison_is_eight_hour_equivalent(raw, expected):
    venue = lighter_venue()

    async def get(path):
        assert path == "/api/v1/funding-rates"
        return {"code": 200, "funding_rates": [
            {"market_id": 139, "symbol": "NBIS", "exchange": "binance", "rate": 9},
            {"market_id": 1, "symbol": "NBIS", "exchange": "lighter", "rate": 9},
            {"market_id": 139, "symbol": "NBIS", "exchange": "lighter", "rate": raw}]}

    venue._get = get
    assert asyncio.run(venue.fetch_funding()).rate_bps_per_hour == pytest.approx(expected)


@pytest.mark.parametrize("rows", [
    [{"market_id": 139, "symbol": "NBIS", "exchange": "binance", "rate": 0.0001}],
    [{"market_id": 139, "symbol": "NBIS", "exchange": "lighter"}],
    [{"market_id": 139, "symbol": "NBIS", "exchange": "lighter", "rate": "NaN"}],
])
def test_lighter_missing_own_funding_is_never_other_exchange_or_zero(rows):
    venue = lighter_venue()

    async def get(path):
        return {"code": 200, "funding_rates": rows}

    venue._get = get
    with pytest.raises((RuntimeError, ValueError)):
        asyncio.run(venue.fetch_funding())


def test_hl_funding_remains_hourly_and_rejects_nonfinite_rate():
    venue = hl_venue()
    ctx = {"funding": "0.0000125"}

    async def info(payload):
        return [{"universe": [{"name": "io:NBIS"}]}, [ctx]]

    venue._info = info
    assert asyncio.run(venue.fetch_funding()).rate_bps_per_hour == 0.125
    ctx["funding"] = "NaN"
    with pytest.raises(RuntimeError, match="non-finite"):
        asyncio.run(venue.fetch_funding())
