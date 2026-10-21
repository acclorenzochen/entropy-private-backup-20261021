import asyncio
from decimal import Decimal as D
import json

import pytest

from entropy_arb.v3.core import Instrument, Side
from entropy_arb.v3.journal import Intent, Journal, Result
from entropy_arb.v3.live_host import (
    AdapterContractError,
    AuditedLiveExecutor,
    BatchAuthorization,
    LIGHTER_MAX_CLIENT_ORDER_INDEX,
    LiveAuthorizationError,
    OrderRefFactory,
    ReadOnlyObserver,
    StatusWriter,
    VenueAdapterBroker,
    reconcile_pair,
)


def instrument():
    return Instrument("pair", D("0.001"), D("0.01"), D("0.01"), D("2"), D("2"))


class FakeVenue:
    def __init__(self, response=None, *, position=0):
        self.response = response or {}
        self.position = position
        self.sent = []
        self.resolved = []

    async def send_taker(self, **kwargs):
        self.sent.append(kwargs)
        return dict(self.response)

    async def resolve_order(self, order_ref, timeout=None):
        self.resolved.append((order_ref, timeout))
        return dict(self.response)

    async def fetch_position(self):
        return self.position


def make_intent(kind="lighter", *, venue="hedge", units=85, ref=None):
    order_ref = ref or OrderRefFactory.create(
        kind, deployment="test", cycle_id="cycle-1", leg=venue)
    return Intent(order_ref, "cycle-1", venue, Side.BUY, units, D("100.00"))


def test_real_lighter_fixture_preserves_85_integer_units():
    intent = make_intent()
    raw = {"status": "filled", "filled_base": "0.085", "avg_px": "100.01",
           "err": None, "unresolved": False, "order_ref": intent.order_ref}
    venue = FakeVenue(raw)
    broker = VenueAdapterBroker(venue, venue_key="hedge", venue_kind="lighter",
                                instrument=instrument())
    result = asyncio.run(broker.submit(intent))
    assert result == Result(True, 85, D("100.01"), note="filled")
    assert venue.sent[0]["qty"] == pytest.approx(.085)


def test_real_hl_partial_ioc_fixture_is_terminal_cumulative_fill():
    intent = make_intent("hl", venue="entropy")
    raw = {"status": "partial", "filled_base": "0.040", "avg_px": "99.98",
           "err": None, "unresolved": False, "order_ref": intent.order_ref}
    broker = VenueAdapterBroker(FakeVenue(raw), venue_key="entropy", venue_kind="hl",
                                instrument=instrument())
    result = asyncio.run(broker.submit(intent))
    assert result.terminal and result.cumulative_units == 40


def test_direct_hl_http_fill_has_verified_receipt_only_for_matching_intent():
    intent = make_intent('hl', venue='entropy')
    raw = {'status': 'filled', 'filled_base': '0.040', 'avg_px': '99.98',
           'err': None, 'unresolved': False, 'order_ref': intent.order_ref,
           'confirmation_source': 'exchange_http'}
    broker = VenueAdapterBroker(FakeVenue(raw), venue_key='entropy',
                                venue_kind='hl', instrument=instrument())
    result = asyncio.run(broker.submit(intent))
    receipt = result.receipt
    assert receipt is not None
    assert receipt.order_ref == intent.order_ref
    assert receipt.requested_units == 85
    assert receipt.cumulative_filled_units == 40
    assert receipt.fill_quantity_verified and receipt.fill_identity_complete
    broker.venue.response['confirmation_source'] = 'order_status_poll'
    assert asyncio.run(broker.submit(intent)).receipt is None


def test_real_hl_ioc_cancel_rejected_fixture_is_terminal_zero_fill():
    intent = make_intent("hl", venue="entropy")
    raw = {"status": "iocCancelRejected", "filled_base": "0", "avg_px": None,
           "err": None, "unresolved": False, "order_ref": intent.order_ref}
    broker = VenueAdapterBroker(FakeVenue(raw), venue_key="entropy", venue_kind="hl",
                                instrument=instrument())
    result = asyncio.run(broker.submit(intent))
    assert result.terminal and result.cumulative_units == 0


def test_real_lighter_slippage_cancel_fixture_is_terminal_zero_fill():
    intent = make_intent()
    raw = {"status": "canceled-too-much-slippage", "filled_base": "0",
           "avg_px": None, "err": None, "unresolved": False,
           "order_ref": intent.order_ref}
    broker = VenueAdapterBroker(FakeVenue(raw), venue_key="hedge", venue_kind="lighter",
                                instrument=instrument())
    result = asyncio.run(broker.submit(intent))
    assert result.terminal and result.cumulative_units == 0


@pytest.mark.parametrize("raw", [
    {"status": "open", "filled_base": "0", "unresolved": False},
    {"status": "mystery", "filled_base": "0", "unresolved": False},
    {"status": "filled", "filled_base": "0", "unresolved": True},
])
def test_only_explicit_adapter_terminal_status_is_terminal(raw):
    intent = make_intent()
    raw = {**raw, "order_ref": intent.order_ref}
    broker = VenueAdapterBroker(FakeVenue(raw), venue_key="hedge", venue_kind="lighter",
                                instrument=instrument())
    assert not asyncio.run(broker.submit(intent)).terminal


def test_adapter_cannot_replace_reference_or_round_quantity():
    intent = make_intent()
    changed = {"status": "filled", "filled_base": ".085", "unresolved": False,
               "order_ref": str(int(intent.order_ref) + 1)}
    broker = VenueAdapterBroker(FakeVenue(changed), venue_key="hedge", venue_kind="lighter",
                                instrument=instrument())
    with pytest.raises(AdapterContractError, match="replaced"):
        asyncio.run(broker.submit(intent))
    bad_fill = {**changed, "order_ref": intent.order_ref, "filled_base": ".0845"}
    broker.venue.response = bad_fill
    with pytest.raises(AdapterContractError, match="common unit"):
        asyncio.run(broker.submit(intent))


def test_lighter_reference_is_prevalidated_before_adapter_call():
    intent = make_intent(ref=str(LIGHTER_MAX_CLIENT_ORDER_INDEX + 1))
    venue = FakeVenue()
    broker = VenueAdapterBroker(venue, venue_key="hedge", venue_kind="lighter",
                                instrument=instrument())
    with pytest.raises(AdapterContractError, match="outside"):
        asyncio.run(broker.submit(intent))
    assert venue.sent == []


def test_durable_submitting_precedes_send_and_unknown_is_resolved_same_id(tmp_path):
    path = tmp_path / "journal.sqlite"
    journal = Journal(path, identity="pair-account")
    intent = make_intent()

    class InspectVenue(FakeVenue):
        async def send_taker(self, **kwargs):
            assert journal.order(intent.order_ref)["state"] == "SUBMITTING"
            return await super().send_taker(**kwargs)

    timeout = {"status": "timeout", "filled_base": "0", "avg_px": None,
               "unresolved": True, "order_ref": intent.order_ref}
    venue = InspectVenue(timeout)
    broker = VenueAdapterBroker(venue, venue_key="hedge", venue_kind="lighter",
                                instrument=instrument())
    other = VenueAdapterBroker(FakeVenue(), venue_key="entropy", venue_kind="hl",
                               instrument=instrument())
    executor = AuditedLiveExecutor(journal, {"entropy": other, "hedge": broker})
    result = asyncio.run(executor.send(intent))
    assert not result.terminal
    assert journal.order(intent.order_ref)["state"] == "UNKNOWN"

    venue.response = {"status": "filled", "filled_base": ".085", "avg_px": "100",
                      "unresolved": False, "order_ref": intent.order_ref}
    recovered = asyncio.run(executor.recover_pending(timeout=0))
    assert recovered[0].terminal and recovered[0].cumulative_units == 85
    assert venue.resolved == [(intent.order_ref, 0)]
    assert len(venue.sent) == 1
    journal.close()


def test_prepared_restart_is_proved_unsent_without_adapter_call(tmp_path):
    journal = Journal(tmp_path / "j.sqlite", identity="pair-account")
    intent = make_intent()
    journal.prepare(intent)
    venue = FakeVenue()
    broker = VenueAdapterBroker(venue, venue_key="hedge", venue_kind="lighter",
                                instrument=instrument())
    other = VenueAdapterBroker(FakeVenue(), venue_key="entropy", venue_kind="hl",
                               instrument=instrument())
    executor = AuditedLiveExecutor(journal, {"entropy": other, "hedge": broker})
    result = asyncio.run(executor.recover_pending())
    assert result == [Result(True, 0, note="proved_unsent")]
    assert venue.sent == venue.resolved == []
    journal.close()


def test_position_reconciliation_requires_local_exchange_match(tmp_path):
    journal = Journal(tmp_path / "j.sqlite", identity="pair-account")
    entropy = VenueAdapterBroker(FakeVenue(position="0.085"), venue_key="entropy",
                                 venue_kind="hl", instrument=instrument())
    hedge = VenueAdapterBroker(FakeVenue(position="-0.085"), venue_key="hedge",
                               venue_kind="lighter", instrument=instrument())
    evidence = asyncio.run(reconcile_pair(
        entropy, hedge, journal, expected_entropy_units=85, expected_hedge_units=-85))
    assert evidence.authoritative and evidence.balanced
    assert evidence.local_and_exchange_match and evidence.all_orders_terminal
    journal.close()


def test_live_authorization_is_exact_and_cannot_enlarge_cap(tmp_path):
    path = tmp_path / "auth.json"
    payload = {"batch_id": "b1", "pair_identity": "pair",
               "max_position_cycles": 2, "loss_budget_usd": "1",
               "max_notional_usd": "15", "approved_live": True}
    path.write_text(json.dumps(payload))
    auth = BatchAuthorization.load(path, expected_pair="pair",
                                   configured_max_notional=D("15"))
    assert auth.batch_id == "b1"
    payload["max_notional_usd"] = "16"
    path.write_text(json.dumps(payload))
    with pytest.raises(LiveAuthorizationError, match="enlarge"):
        BatchAuthorization.load(path, expected_pair="pair",
                                configured_max_notional=D("15"))


def test_status_is_atomic_and_observer_has_no_gate_api(tmp_path):
    status_path = tmp_path / "status.json"
    writer = StatusWriter(status_path, strategy_version="v3", config_hash="abc",
                          mode="record-only")
    writer.write("READY", "test", source="unit",
                 batch_state={"loss_used_usd": D("0")})
    payload = json.loads(status_path.read_text())
    assert {"process_id", "strategy_version", "config_hash", "start_ts",
            "current_state", "reason", "source_ts"} <= payload.keys()
    assert payload["batch_state"]["loss_used_usd"] == "0"
    observer = ReadOnlyObserver(tmp_path / "observer.jsonl")
    assert observer.publish({"legacy_prediction": 1}) is None
    assert not hasattr(observer, "allowed") and not hasattr(observer, "blocker")


def test_observer_write_failure_cannot_gate_runtime(tmp_path):
    observer = ReadOnlyObserver(tmp_path / "directory")
    observer.path.mkdir()
    assert observer.publish({"legacy_prediction": 1}) is None
    assert observer.last_error is not None
