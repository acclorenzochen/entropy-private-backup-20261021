import asyncio
from types import SimpleNamespace

from entropy_arb.venue_hl import HLVenue
from entropy_arb.venue_lighter import LighterVenue


def test_hyperliquid_fee_fill_parser_uses_user_fill_fee():
    venue = HLVenue.__new__(HLVenue)
    venue.coin = "io:SNDK"
    venue.conf = SimpleNamespace(hl_dex="io", symbol="SNDK")
    venue._query_address = lambda: "0xabc"

    async def info(payload):
        assert payload["type"] == "userFillsByTime"
        assert payload["dex"] == "io"
        return [{
            "coin": "io:SNDK", "time": 100_000, "side": "A",
            "sz": "0.04", "px": "1485.5", "fee": "0.0054",
            "feeToken": "USDC", "tid": "tid-1", "cloid": "order-1",
        }]

    venue._info = info
    rows = asyncio.run(venue.fetch_fee_fills(99.0, 101.0))
    assert rows[0]["side"] == "sell"
    assert rows[0]["order_ref"] == "order-1"
    assert rows[0]["fee_usd"] == 0.0054


def test_lighter_fee_parser_selects_account_side_and_quote_decimals():
    venue = LighterVenue.__new__(LighterVenue)
    venue.name = "Lighter"
    venue.market_id = 4
    venue.quote_decimals = 6
    venue.conf = SimpleNamespace(
        lighter_creds=SimpleNamespace(account_index=7))
    venue._fee_auth_header = lambda: {"authorization": "token"}

    async def get(path, params=None, headers=None):
        assert path == "/api/v1/trades"
        assert params["account_index"] == 7
        assert headers == {"authorization": "token"}
        return {"trades": [{
            "market_id": 4, "timestamp": 100_000,
            "size": "0.04", "price": "1485.5",
            "ask_account_id": 7, "bid_account_id": 8,
            "is_maker_ask": False, "taker_fee": 5400,
            "maker_fee": 0, "ask_client_id_str": "order-2",
            "trade_id_str": "trade-2",
        }]}

    venue._get = get
    rows = asyncio.run(venue.fetch_fee_fills(99_999.0, 100_001.0))
    assert rows[0]["side"] == "sell"
    assert rows[0]["order_ref"] == "order-2"
    assert rows[0]["fee_usd"] == 0.0054


def test_hyperliquid_unified_account_uses_spot_available_margin():
    venue = HLVenue.__new__(HLVenue)
    venue.account = SimpleNamespace(query_address="0xabc")
    venue.include_core_equity = True
    venue.conf = SimpleNamespace(hl_dex="io", symbol="SNDK")
    calls = []

    async def info(payload):
        calls.append(payload)
        kind = payload["type"]
        if kind == "portfolio":
            return [["day", {"accountValueHistory": [[100, "54.683967"]]}]]
        if kind == "userAbstraction":
            return "unifiedAccount"
        if kind == "spotClearinghouseState":
            return {"tokenToAvailableAfterMaintenance": [[0, "54.683967"]]}
        if kind == "clearinghouseState":
            return {"marginSummary": {"accountValue": "0"},
                    "withdrawable": "0"}
        raise AssertionError(f"unexpected info request: {payload}")

    venue._info = info
    equity, available = asyncio.run(venue.fetch_equity())

    assert equity == 54.683967
    assert available == 54.683967
    assert [p["type"] for p in calls] == [
        "portfolio", "clearinghouseState", "clearinghouseState",
        "userAbstraction", "spotClearinghouseState",
    ]
