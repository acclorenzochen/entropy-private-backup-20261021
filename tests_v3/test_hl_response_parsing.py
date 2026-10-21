"""Offline adapter contract regressions; no signing keys or network calls."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from entropy_arb.venue_hl import HLVenue


def response(status):
    return {"status": "ok", "response": {"data": {"statuses": [status]}}}


@pytest.mark.parametrize('only_isolated,reported', [
    (True, {'type': 'isolated', 'value': 10}),
    (False, {'type': 'cross', 'value': 10}),
    (True, {'type': 'isolated', 'value': 3}),
    (True, {'type': 'cross', 'value': 10}),
    (True, None),
])
def test_leverage_success_requires_exchange_readback(monkeypatch, only_isolated, reported):
    venue = HLVenue.__new__(HLVenue)
    venue.name, venue.coin, venue.asset_id = 'entropy', 'io:SNDK', 110001
    venue.max_leverage, venue.only_isolated = 10, only_isolated
    venue.account = SimpleNamespace(
        wallet=None, query_address='0xabc', is_mainnet=True,
        nonces=SimpleNamespace(next=lambda: 1))
    venue._signing = SimpleNamespace(sign_l1_action=lambda *args: {})
    venue._exchange_timings = {}
    actions, polls = [], []

    async def post(payload):
        actions.append(payload['action'])
        return {'status': 'ok'}, None, False

    async def info(payload):
        assert payload == {'type': 'activeAssetData', 'user': '0xabc', 'coin': 'io:SNDK'}
        polls.append(payload)
        return {'leverage': reported}

    async def no_sleep(delay):
        return None

    monkeypatch.setattr(asyncio, 'sleep', no_sleep)
    venue._post_exchange, venue._info = post, info
    expected_mode = 'isolated' if only_isolated else 'cross'
    if reported == {'type': expected_mode, 'value': 10}:
        asyncio.run(venue.set_leverage(10))
        assert len(polls) == 1
    else:
        with pytest.raises(RuntimeError, match='could not be verified'):
            asyncio.run(venue.set_leverage(10))
        assert len(polls) == 20
    assert actions == [{'type': 'updateLeverage', 'asset': 110001,
                        'isCross': not only_isolated, 'leverage': 10}]


def test_leverage_above_market_limit_is_never_submitted():
    venue = HLVenue.__new__(HLVenue)
    venue.name, venue.max_leverage = 'entropy', 10
    venue.account, venue._signing = object(), object()
    with pytest.raises(RuntimeError, match='outside market range'):
        asyncio.run(venue.set_leverage(11))


@pytest.mark.parametrize("body", [
    None, [], "ok", {}, {"status": "ok"}, {"status": "err"},
    {"status": "err", "response": {}},
    {"status": "ok", "response": {"data": {"statuses": []}}},
    {"status": "ok", "response": {"data": {"statuses": [{}, {}]}}},
    response(None), response("filled"), response({"futureStatus": {}}),
    response({"error": None}), response({"filled": {}, "error": "rejected"}),
    response({"filled": {"oid": 7, "avgPx": "100"}}),
    response({"filled": {"totalSz": "1", "avgPx": "100"}}),
    response({"filled": {"oid": 7, "totalSz": "1"}}),
])
def test_untrusted_submitted_response_is_unknown(body):
    result = HLVenue._parse(body)
    assert result["unresolved"] is True
    assert result["status"] == "unknown"


@pytest.mark.parametrize("field,value", [
    ("totalSz", None), ("totalSz", ""), ("totalSz", "-1"),
    ("totalSz", "0"), ("totalSz", "nan"), ("totalSz", "inf"),
    ("avgPx", "nan"), ("avgPx", "0"), ("avgPx", "-1"),
    ("oid", None), ("oid", "bad"),
])
def test_invalid_filled_fields_cannot_prove_receipt(field, value):
    filled = {"oid": 7, "totalSz": "1", "avgPx": "100"}
    filled[field] = value
    assert HLVenue._parse(response({"filled": filled}))["unresolved"]


def test_valid_fill_and_explicit_rejections_keep_existing_semantics():
    result = HLVenue._parse(response({"filled": {
        "oid": 7, "totalSz": "0.4", "avgPx": "101"}}))
    assert not result["unresolved"]
    assert result["filled_base"] == .4 and result["avg_px"] == 101
    for body in (response({"error": "Order could not immediately match"}),
                 response({"error": "Insufficient margin"}),
                 {"status": "err", "response": "Invalid signature"}):
        result = HLVenue._parse(body)
        assert not result["unresolved"] and result["filled_base"] == 0


@pytest.mark.parametrize("status,inner", [
    ("filled", {}), ("canceled", {"origSz": "1"}),
    ("canceled", {"sz": "0"}),
    ("filled", {"origSz": "nan", "sz": "0"}),
    ("filled", {"origSz": "1", "sz": "-1"}),
    ("canceled", {"origSz": "1", "sz": "2"}),
    ("unexpected", {"origSz": "1", "sz": "1"}),
    ("triggered", {"origSz": "1", "sz": "1"}),
    ("filled", {"origSz": "1", "sz": "0", "cloid": "other"}),
])
def test_untrusted_order_status_keeps_same_id_unknown(status, inner):
    async def run():
        venue = HLVenue.__new__(HLVenue)
        venue.account = SimpleNamespace(query_address="unused")
        calls = []

        async def info(payload):
            calls.append(payload["oid"])
            return {"status": "order", "order": {"status": status, "order": inner}}

        venue._info = info
        result = await venue.resolve_order("0x12", timeout=.001)
        assert result["unresolved"] and result["order_ref"] == "0x12"
        assert calls and set(calls) == {"0x12"}
    asyncio.run(run())


def test_corrupt_action_response_resolves_original_id_without_resubmitting():
    async def run():
        venue = HLVenue.__new__(HLVenue)
        venue.account = SimpleNamespace(
            wallet=None, is_mainnet=False, nonces=SimpleNamespace(next=lambda: 1))
        venue.asset_id, venue.coin = 1, "io:SNDK"
        venue._signing = SimpleNamespace(
            order_request_to_order_wire=lambda *args: {},
            order_wires_to_order_action=lambda *args: {},
            sign_l1_action=lambda *args: {})
        venue._next_cloid = lambda ref: SimpleNamespace(to_raw=lambda: ref)
        venue.orders_feed, venue._submit_tasks = None, set()
        submitted, resolved = [], []

        async def post(payload):
            submitted.append(payload)
            return response({"filled": {"oid": 7}}), None, False

        async def resolve(ref, timeout=None):
            resolved.append(ref)
            return {"status": "filled", "filled_base": .4, "avg_px": 101,
                    "unresolved": False, "order_ref": ref,
                    "confirmation_source": "order_status_poll"}

        venue._post_exchange, venue.resolve_order = post, resolve
        result = await venue.send_taker(is_buy=True, qty=1, limit_px=101,
                                        order_ref="0x12")
        assert len(submitted) == 1 and resolved == ["0x12"]
        assert result["filled_base"] == .4
        assert result["confirmation_source"] == "order_status_poll"
    asyncio.run(run())


@pytest.mark.parametrize('status,body,unknown', [
    (408, 'request timed out', True), (400, '{}', True),
    (400, json.dumps({'status': 'err', 'response': 'Invalid signature'}), False),
    (200, '<html>proxy response</html>', True), (503, '{}', True),
])
def test_http_errors_require_explicit_exchange_rejection(status, body, unknown):
    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def text(self):
            return body

    reply = Response()
    reply.status = status
    venue = HLVenue.__new__(HLVenue)
    venue.api_url, venue._exchange_timings = 'unused', {}
    venue.session = SimpleNamespace(post=lambda *args, **kwargs: reply)
    result, error, unresolved = asyncio.run(venue._post_exchange({'nonce': 1}))
    assert unresolved is unknown
    assert error is None
    if not unknown:
        assert HLVenue._parse(result)['status'] == 'send-failed'
