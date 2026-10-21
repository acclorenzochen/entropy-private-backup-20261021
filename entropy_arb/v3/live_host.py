"""Audited production-host primitives for the standalone V3 core.

This module deliberately does not import the legacy Engine.  It adapts the
existing venue clients at a narrow boundary, keeps quantities in integer
common units internally, and preserves unknown order outcomes across restart.
It does not decide whether an entry is attractive; that remains in ``core``.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from decimal import Decimal, DecimalException
import asyncio
import hashlib
import json
import os
from pathlib import Path
import time
import threading
from typing import Any, Callable, Mapping, Protocol

from ..lighter_requests import RequestDeferred
from ..venue_lighter import TERMINAL_STATUSES as LIGHTER_TERMINAL_STATUSES
from .core import Instrument, Side, dec
from ..order_timing import source_name
from .execution import PairEvidence
from .journal import Intent, Journal, Result, TerminalReceipt


LIGHTER_MAX_CLIENT_ORDER_INDEX = (1 << 48) - 1

# These are terminal outcomes actually emitted by the existing IOC adapters.
# Anything else remains UNKNOWN even if an adapter happened to set
# ``unresolved`` false.
_TERMINAL_STATUSES = frozenset({
    "filled", "partial", "canceled", "cancelled", "rejected", "expired",
    "margincanceled", "reduceonlycanceled", "selftradecanceled",
    "scheduledcancel", "ioccancelrejected", "canceledtoomuchslippage",
    "vaultwithdrawalcanceled", "openinterestcapcanceled",
    "siblingfilledcanceled", "delistedcanceled", "liquidatedcanceled",
    "tickrejected", "mintradentlrejected", "perpmarginrejected",
    "reduceonlyrejected", "badalopxrejected", "badtriggerpxrejected",
    "marketordernoliquidityrejected", "positionincreaseatopeninterestcaprejected",
    "positionflipatopeninterestcaprejected", "tooaggressiveatopeninterestcaprejected",
    "openinterestincreaserejected", "insufficientspotbalancerejected",
    "oraclerejected", "perpmaxpositionrejected", "sendfailed",
})


class AdapterContractError(RuntimeError):
    """The real venue response cannot safely be represented by V3."""


class LiveAuthorizationError(RuntimeError):
    """Explicit live-mode authorization is absent or inconsistent."""


class VenueClient(Protocol):
    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool, order_ref: str,
                         resolve_timeout: float | None = None,
                         timing: dict[str, object] | None = None) -> dict: ...

    async def resolve_order(self, order_ref: str, timeout: float | None = None) -> dict: ...

    async def fetch_position(self) -> float: ...


@dataclass(frozen=True)
class BrokerPositionSnapshot:
    units: int
    received_ts: float
    authoritative: bool
    source: str
    generation: int | None = None


@dataclass(frozen=True)
class BatchAuthorization:
    batch_id: str
    pair_identity: str
    max_position_cycles: int | None
    loss_budget_usd: Decimal | None
    max_notional_usd: Decimal
    approved_live: bool
    opening_experiment_id: str | None = None

    @classmethod
    def load(cls, path: str | Path, *, expected_pair: str,
             configured_max_notional: Decimal,
             allow_disabled_loss_budget: bool = False,
             allow_disabled_cycle_limit: bool = False,
             expected_opening_experiment_id: str | None = None,
             provenance: dict | None = None) -> "BatchAuthorization":
        try:
            resolved = Path(path).resolve()
            content = resolved.read_bytes()
            raw = json.loads(content)
        except (OSError, json.JSONDecodeError) as exc:
            raise LiveAuthorizationError(f"cannot read batch authorization: {exc}") from exc
        required = {
            "batch_id", "pair_identity", "max_position_cycles",
            "loss_budget_usd", "max_notional_usd", "approved_live",
        }
        if expected_opening_experiment_id is not None:
            required.add('opening_experiment_id')
        if not isinstance(raw, dict) or set(raw) != required:
            raise LiveAuthorizationError(
                f"batch authorization keys must be exactly {sorted(required)}")
        if raw["approved_live"] is not True:
            raise LiveAuthorizationError("batch authorization is not approved for live use")
        if raw["pair_identity"] != expected_pair:
            raise LiveAuthorizationError("batch authorization pair does not match this launch")
        maximum = raw['max_position_cycles']
        if (maximum is None and not allow_disabled_cycle_limit) or (maximum is not None
                and (type(maximum) is not int or maximum < 1)):
            raise LiveAuthorizationError('max_position_cycles must be a positive integer or explicitly permitted null')
        if expected_opening_experiment_id is not None and (
                not expected_opening_experiment_id
                or raw['opening_experiment_id'] != expected_opening_experiment_id
                or raw['max_position_cycles'] != 20
                or raw['loss_budget_usd'] is not None
                or configured_max_notional > Decimal('200')):
            raise LiveAuthorizationError('opening experiment requires matching identity, twenty attempts, disabled cumulative loss budget and 200 USD cap')
        if raw['loss_budget_usd'] is None and not (allow_disabled_loss_budget
                                                  or expected_opening_experiment_id):
            raise LiveAuthorizationError('disabled loss budget requires explicit launch policy')
        loss = None if raw['loss_budget_usd'] is None else dec(raw["loss_budget_usd"])
        notional = dec(raw["max_notional_usd"])
        if not raw["batch_id"] or (loss is not None and loss <= 0) or notional <= 0:
            raise LiveAuthorizationError("batch id and budgets must be positive")
        if notional > configured_max_notional:
            raise LiveAuthorizationError(
                "authorization cannot enlarge the configured V3 notional cap")
        if provenance is not None:
            provenance.update(path=str(resolved), sha256=hashlib.sha256(content).hexdigest(),
                              evidence="parsed_bytes")
        return cls(raw["batch_id"], raw["pair_identity"],
                   raw["max_position_cycles"], loss, notional, True,
                   expected_opening_experiment_id)


class OrderRefFactory:
    """Deterministic per-intent IDs valid for both current adapters."""

    @staticmethod
    def create(venue_kind: str, *, deployment: str, cycle_id: str,
               leg: str) -> str:
        seed = f"v3|{deployment}|{cycle_id}|{leg}".encode()
        if venue_kind == "lighter":
            value = int.from_bytes(hashlib.blake2b(seed, digest_size=6).digest(), "big")
            return str(value or 1)
        if venue_kind == "hl":
            return "0x" + hashlib.blake2b(seed, digest_size=16).hexdigest()
        raise ValueError(f"unsupported venue kind: {venue_kind}")


def _normalized_status(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def order_timing_trace(broker, intent, timing=None):
    try:
        method = getattr(broker, 'order_timing', None)
        return method(intent, timing) if method is not None else None
    except Exception:
        return None


def consume_order_timing(broker, intent, timing=None, when=None):
    trace = order_timing_trace(broker, intent, timing)
    if trace is not None:
        try:
            trace.consumed(when)
        except Exception:
            pass


class VenueAdapterBroker:
    """Translate one existing venue client to exact V3 Result semantics."""

    is_live = True

    def __init__(self, venue: VenueClient, *, venue_key: str, venue_kind: str,
                 instrument: Instrument) -> None:
        if venue_key not in {"entropy", "hedge"} or venue_kind not in {"hl", "lighter"}:
            raise ValueError("unsupported venue identity")
        self.venue = venue
        self.venue_key = venue_key
        self.venue_kind = venue_kind
        self.instrument = instrument

    @property
    def supports_order_progress(self) -> bool:
        return bool(getattr(self.venue, 'supports_order_progress', False))

    def _validate_intent(self, intent: Intent) -> tuple[float, float]:
        if intent.venue != self.venue_key:
            raise AdapterContractError("intent sent to the wrong venue")
        if self.venue_kind == "lighter":
            try:
                value = int(intent.order_ref)
            except ValueError as exc:
                raise AdapterContractError("Lighter order_ref must be numeric") from exc
            if str(value) != intent.order_ref or not 0 < value <= LIGHTER_MAX_CLIENT_ORDER_INDEX:
                raise AdapterContractError("Lighter order_ref is outside the accepted range")
        elif not (intent.order_ref.startswith("0x") and len(intent.order_ref) == 34):
            raise AdapterContractError("Hyperliquid order_ref must be a 16-byte Cloid")

        quantity = self.instrument.quantity(intent.units)
        qty_float = float(quantity)
        if self.instrument.exact_units(str(qty_float)) != intent.units:
            raise AdapterContractError("float adapter boundary changed common quantity units")
        tick = (self.instrument.entropy_tick if intent.venue == "entropy"
                else self.instrument.hedge_tick)
        if intent.limit_px / tick != (intent.limit_px / tick).to_integral_value():
            raise AdapterContractError("limit price is not on the venue tick")
        return qty_float, float(intent.limit_px)

    def _result(self, intent: Intent, raw: Mapping[str, Any],
                timing: dict[str, object] | None = None) -> Result:
        if not isinstance(raw, Mapping):
            raise AdapterContractError("adapter response is not a mapping")
        returned_ref = raw.get("order_ref")
        if returned_ref is None or str(returned_ref) != intent.order_ref:
            raise AdapterContractError("adapter replaced or omitted the persisted order_ref")
        try:
            quantity = dec(raw["filled_base"])
            if not quantity.is_finite() or quantity < 0:
                raise ValueError("fill must be finite and nonnegative")
            units = self.instrument.exact_units(quantity)
        except (KeyError, ValueError, TypeError, DecimalException, OverflowError) as exc:
            raise AdapterContractError("filled_base is not on the common unit grid") from exc
        if units > intent.units:
            raise AdapterContractError("adapter reported a fill above requested quantity")
        if raw.get('deferred') is True:
            if units or raw.get('post_started') is not False \
                    or raw.get('proved_local_unsent') is not True:
                raise AdapterContractError('dispatch deferral lacks no-POST proof')
            try:
                delay = float(dec(raw.get('retry_after', 0)))
                return Result(False, 0, note=str(raw.get('err') or 'dispatch deferred'),
                              deferred=True, retry_after=delay)
            except (ValueError, TypeError, DecimalException, OverflowError) as exc:
                raise AdapterContractError('invalid dispatch retry delay') from exc
        status = _normalized_status(raw.get("status"))
        unresolved = raw.get("unresolved") is not False
        terminal = not unresolved and (status in _TERMINAL_STATUSES or
            self.venue_kind == 'lighter' and
            str(raw.get('status') or '').strip().lower() in LIGHTER_TERMINAL_STATUSES)
        if status in {'filled', 'partial'} and units == 0:
            terminal = False
        if timing is not None:
            for name in (
                    'http_post_start', 'http_headers_received',
                    'http_body_received', 'http_result_ts', 'resolve_start_ts',
                    'resolve_end_ts', 'exchange_fill_ts'):
                if raw.get(name) is not None:
                    timing[name] = raw[name]
            for name in ('http_post_start_mono_ns', 'exchange_last_fill_ts'):
                if raw.get(name) is not None:
                    timing[name] = raw[name]
            timing['result_status'] = str(raw.get('status') or '')
            if raw.get('confirmation_source') is not None:
                timing['confirmation_source'] = raw['confirmation_source']
        average = raw.get("avg_px")
        fee = raw.get("fee_usd")
        try:
            average = dec(average) if average not in (None, "") else None
            fee = dec(fee) if fee not in (None, "") else None
            if average is not None and (not average.is_finite() or average <= 0):
                raise ValueError("invalid average")
            if fee is not None and not fee.is_finite():
                raise ValueError("invalid fee")
        except (ValueError, TypeError, DecimalException) as exc:
            raise AdapterContractError("invalid fill price or fee") from exc
        note = str(raw.get("err") or raw.get("status") or "adapter response")
        # The direct exchange action's filled stanza reports one IOC's final
        # cumulative quantity. WS and poll results need separate completeness
        # proof before they can authorize a position-derived fast path.
        receipt = None
        if (self.venue_key == 'entropy' and terminal and units > 0
                and status == 'filled'
                and raw.get('confirmation_source') == 'exchange_http'
                and average not in (None, '')):
            receipt = TerminalReceipt(
                intent.order_ref, intent.venue, intent.side, intent.units,
                units, 'exchange_http', time.time(), True, True)
        result = Result(terminal, units, average, fee, note=note, receipt=receipt)
        if timing is not None and terminal:
            observed = raw.get('observed_ts')
            observed_mono = raw.get('observed_mono_ns')
            timing['terminal_result_ts'] = (observed if type(observed) in (int, float)
                and 0 < observed < float('inf') else time.time())
            timing['terminal_result_mono_ns'] = (observed_mono
                if type(observed_mono) is int and observed_mono > 0 else time.monotonic_ns())
        trace = self.order_timing(intent, timing)
        if trace is not None:
            trace.mark('adapter_normalized')
            trace.result(status=raw.get('status', ''), filled_base=float(quantity),
                         average=float(average) if average is not None else None,
                         source=raw.get('confirmation_source'))
            trace.data['normalized_result_confirmation_source'] = source_name(raw.get('confirmation_source'))
        return result

    def order_timing(self, intent, timing=None):
        get_trace = getattr(self.venue, 'order_timing', None)
        if get_trace is None:
            return None
        try:
            metadata = (timing or {}).get('_order_timing_context', {})
            return get_trace(intent.order_ref, create=True, cycle_id=intent.cycle_id,
                side=intent.side.value, requested_units=intent.units,
                quantity_step=str(self.instrument.quantity_step),
                requested_base=float(self.instrument.quantity(intent.units)),
                limit_px=float(intent.limit_px), reduce_only=intent.reduce_only, **metadata)
        except Exception:
            return None

    async def prepare_dispatch(self, intent: Intent, *, completion_ticket=None,
                               completion: bool = False):
        self._validate_intent(intent)
        if not getattr(self.venue, 'supports_dispatch_token', False):
            return None
        return await self.venue.prepare_dispatch(
            order_ref=intent.order_ref, reduce_only=intent.reduce_only,
            completion=completion, completion_ticket=completion_ticket)

    async def submit(self, intent: Intent, *, resolve_timeout: float | None = None,
                     timing: dict[str, object] | None = None,
                     dispatch_token=None, completion_ticket=None,
                     completion: bool = False,
                     on_progress: Callable[[Result], None] | None = None) -> Result:
        quantity, price = self._validate_intent(intent)
        kwargs = dict(
            is_buy=intent.side is Side.BUY, qty=quantity, limit_px=price,
            reduce_only=intent.reduce_only, order_ref=intent.order_ref)
        if self.venue_kind == 'hl' and getattr(
                self.venue, 'supports_open_first_fast_resolve', False):
            kwargs.update(resolve_timeout=resolve_timeout, timing=timing)
            if timing is not None and resolve_timeout is not None:
                timing['_fast_resolve_performed'] = True
        if getattr(self.venue, 'supports_dispatch_token', False):
            kwargs.update(dispatch_token=dispatch_token,
                          completion_ticket=completion_ticket, completion=completion)
        if self.supports_order_progress and on_progress is not None:
            def progress(raw):
                if raw.get('confirmation_source') not in {'account_orders_ws', 'accountOrders_rest'}:
                    raise AdapterContractError('order progress lacks trusted execution evidence')
                result = self._result(intent, raw, timing)
                if result.deferred:
                    raise AdapterContractError('execution progress cannot prove local unsent')
                on_progress(result)
            kwargs['on_progress'] = progress
        raw = await self.venue.send_taker(**kwargs)
        return self._result(intent, raw, timing)

    async def resolve(self, intent: Intent, *, timeout: float | None = None,
                      timing: dict[str, object] | None = None) -> Result:
        quantity, _ = self._validate_intent(intent)
        register = getattr(self.venue, 'register_order_identity', None)
        if register is not None:
            register(intent.order_ref, qty=quantity, is_buy=intent.side is Side.BUY)
        trace = self.order_timing(intent, timing)
        if trace is not None:
            trace.mark('resolve_wrapper_start')
        if timing is not None:
            timing.setdefault('resolve_start_ts', time.time())
        raw = await self.venue.resolve_order(intent.order_ref, timeout=timeout)
        if trace is not None:
            trace.mark('resolve_wrapper_end')
        if timing is not None:
            timing['resolve_end_ts'] = time.time()
        result = self._result(intent, raw, timing)
        if result.deferred:
            raise AdapterContractError('possibly submitted order cannot be deferred')
        return result

    async def position_units(self) -> int:
        return (await self.position_snapshot()).units

    async def position_snapshot(self, *, force_rest: bool = False) -> BrokerPositionSnapshot:
        try:
            fetch = getattr(self.venue, "fetch_position_snapshot", None)
            if fetch is None:
                position = await self.venue.fetch_position()
                return BrokerPositionSnapshot(
                    self.instrument.exact_units(position), time.time(), True,
                    "venue_position")
            snapshot = await fetch(force_rest=True) if force_rest else await fetch()
            return BrokerPositionSnapshot(
                self.instrument.exact_units(snapshot.position),
                float(snapshot.received_ts), bool(snapshot.authoritative),
                str(snapshot.source), getattr(snapshot, "generation", None))
        except (ValueError, TypeError) as exc:
            raise AdapterContractError(
                f"{self.venue_key} position is not on the common unit grid") from exc


class AuditedLiveExecutor:
    """Production sender that preserves the reference core's fail-closed guard.

    ``IntentExecutor`` continues to reject live brokers.  This separate host
    implementation is the audited path and deliberately repeats the short
    persistence protocol so deleting ``LiveIntegrationRequired`` is unnecessary.
    """

    def __init__(self, journal: Journal, brokers: Mapping[str, VenueAdapterBroker]) -> None:
        if set(brokers) != {"entropy", "hedge"}:
            raise ValueError("both venue brokers are required")
        self.journal = journal
        self.brokers = dict(brokers)
        self.entries_halted = False
        self.operator_entry_gate: Callable[[Intent], str | None] | None = None
        self.clock_session_id: str | None = None

    async def send(self, intent: Intent, *, timing: dict[str, object] | None = None,
                   resolve_timeout: float | None = None,
                   before_submit: Callable[[], None] | None = None,
                   persist_dispatch: Callable[[], dict | None] | None = None,
                   dispatch_token=None, completion_ticket=None,
                   completion: bool = False,
                   on_progress: Callable[[Intent, Result, dict | None], None] | None = None) -> Result:
        if self.entries_halted and not intent.reduce_only:
            raise LiveAuthorizationError("new exposure halted after persistence failure")
        if timing is None and getattr(self.brokers[intent.venue], 'supports_order_progress', False):
            timing = {}
        trace = order_timing_trace(self.brokers[intent.venue], intent, timing)
        if trace is not None:
            trace.mark('prepare_start', first=True)
        try:
            if dispatch_token is None:
                try:
                    dispatch_token = await self.brokers[intent.venue].prepare_dispatch(
                        intent, completion_ticket=completion_ticket, completion=completion)
                except RequestDeferred as exc:
                    return Result(False, 0, note=exc.reason,
                                  deferred=True, retry_after=exc.retry_after)
            if timing is not None:
                timing['intent_prepare_start_ts'] = time.time()
            if trace is not None:
                trace.mark('intent_persist_start', first=True)
            if self.operator_entry_gate is not None and not intent.reduce_only:
                reason = self.operator_entry_gate(intent)
                if reason:
                    return Result(False, 0, note=reason, deferred=True, retry_after=.5)
            prepared = None
            if persist_dispatch is None:
                self.journal.prepare_submitting(intent)
            else:
                prepared = persist_dispatch()
                if prepared is None:
                    return Result(False, 0, note='logical_request_budget',
                                  deferred=True, retry_after=.5)
            if timing is not None:
                timing['intent_prepare_end_ts'] = time.time()
            if trace is not None:
                trace.mark('intent_persist_end', first=True)
            if before_submit is not None:
                before_submit()
            submitted_ts = time.time()
            if timing is not None:
                timing['submit_start_ts'] = submitted_ts
                timing['submit_start_mono_ns'] = time.monotonic_ns()
            if trace is not None:
                trace.mark('broker_submit_start', first=True)
            progress_error = None
            def progress(result):
                nonlocal progress_error
                try:
                    self.journal.record(intent.order_ref, result, submitted_ts=submitted_ts,
                        timing=timing, clock_session_id=self.clock_session_id,
                        known_partial=not result.terminal)
                    if on_progress is not None:
                        on_progress(intent, self._preserve_recorded_progress(intent, result), timing)
                except Exception as exc:
                    self.entries_halted = True
                    progress_error = exc
                    raise
            try:
                kwargs = dict(resolve_timeout=resolve_timeout, timing=timing,
                    dispatch_token=dispatch_token, completion_ticket=completion_ticket,
                    completion=completion)
                if getattr(self.brokers[intent.venue], 'supports_order_progress', False):
                    kwargs['on_progress'] = progress
                result = await self.brokers[intent.venue].submit(intent, **kwargs)
            except AdapterContractError as exc:
                if progress_error is not None:
                    raise progress_error
                result = Result(False, 0, note=f"adapter contract unknown: {exc}")
            except Exception as exc:
                if progress_error is not None:
                    raise progress_error
                result = Result(False, 0, note=f"transport outcome unknown: {type(exc).__name__}")
            if progress_error is not None:
                raise progress_error
            result = self._preserve_recorded_progress(intent, result)
            if timing is not None:
                timing['submit_result_ts'] = time.time()
            try:
                if result.deferred:
                    self.journal.discard_unsent_dispatch(
                        intent, request_event_ts=(prepared or {}).get('request_event_ts'))
                else:
                    self.journal.record(intent.order_ref, result, submitted_ts=submitted_ts,
                                        timing=timing, clock_session_id=self.clock_session_id)
                    result = self._preserve_recorded_progress(intent, result)
            except Exception:
                self.entries_halted = True
                raise
            return result
        finally:
            if dispatch_token is not None:
                dispatch_token.release()

    def _preserve_recorded_progress(self, intent: Intent, result: Result) -> Result:
        # HTTP submission evidence cannot erase a trusted execution observation.
        row = self.journal.order(intent.order_ref)
        same_quantity = result.cumulative_units == row['filled_units']
        average = (dec(row['avg_px']) if row['avg_px'] is not None else
                   result.avg_px if same_quantity else None)
        fee = (dec(row['fee_usd']) if row['fee_usd'] is not None else
               result.fee_usd if same_quantity else None)
        if (row['state'] == 'TERMINAL'
                or result.cumulative_units < row['filled_units']
                or result.deferred and row['filled_units']):
            return replace(result, terminal=row['state'] == 'TERMINAL',
                cumulative_units=row['filled_units'], avg_px=average, fee_usd=fee,
                note=row['note'] if row['state'] == 'TERMINAL' else result.note,
                receipt=result.receipt if same_quantity else None,
                deferred=False, retry_after=0.0)
        if same_quantity and not result.deferred:
            return replace(result, avg_px=average, fee_usd=fee)
        return result

    async def resolve(self, intent: Intent, *, timeout: float | None = None,
                      timing: dict[str, object] | None = None) -> Result:
        if timing is None:
            timing = {}
        order_timing_trace(self.brokers[intent.venue], intent, timing)
        row = self.journal.order(intent.order_ref)
        if row["state"] == "PREPARED":
            self.journal.cancel_unsent_prepared(intent.order_ref)
            return Result(True, 0, note="proved_unsent")
        if row["state"] == "TERMINAL":
            return Result(True, row["filled_units"],
                          dec(row["avg_px"]) if row["avg_px"] else None,
                          dec(row["fee_usd"]) if row["fee_usd"] else None)
        try:
            result = await self.brokers[intent.venue].resolve(
                intent, timeout=timeout, timing=timing)
        except AdapterContractError as exc:
            result = Result(False, row["filled_units"],
                            note=f"adapter contract unknown: {exc}")
        result = self._preserve_recorded_progress(intent, result)
        self.journal.record(intent.order_ref, result, timing=timing,
                            clock_session_id=self.clock_session_id)
        return self._preserve_recorded_progress(intent, result)

    async def recover_pending(self, *, timeout: float | None = None) -> list[Result]:
        results = []
        for row in self.journal.pending():
            intent = Intent(
                row["order_ref"], row["cycle_id"], row["venue"], Side(row["side"]),
                row["requested_units"], dec(row["limit_px"]), bool(row["reduce_only"]))
            results.append(await self.resolve(intent, timeout=timeout))
        return results


async def reconcile_pair(entropy: VenueAdapterBroker,
                         hedge: VenueAdapterBroker, journal: Journal,
                         *, expected_entropy_units: int | None = None,
                         expected_hedge_units: int | None = None) -> PairEvidence:
    e_snapshot = await entropy.position_snapshot()
    h_snapshot = await hedge.position_snapshot()
    e_units = e_snapshot.units
    h_units = h_snapshot.units
    expected_known = expected_entropy_units is not None and expected_hedge_units is not None
    matches = expected_known and e_units == expected_entropy_units and h_units == expected_hedge_units
    return PairEvidence(e_units, h_units,
                        authoritative=(e_snapshot.authoritative
                                       and h_snapshot.authoritative),
                        all_orders_terminal=not journal.pending(),
                        local_and_exchange_match=matches)


class StatusWriter:
    """One file-only worker, one pending snapshot; never share a Journal.

    Production opts into background publication. Legacy callers retain
    read-after-write behavior with a bounded wait. Close at host exit.
    """

    def __init__(self, path: str | Path, *, strategy_version: str,
                 config_hash: str, mode: str, background: bool = False) -> None:
        self.path = Path(path)
        self.background = background
        self.base = {
            "process_id": os.getpid(), "strategy_version": strategy_version,
            "config_hash": config_hash, "start_ts": time.time(), "mode": mode,
        }
        self.last_error: str | None = None
        self.dropped_events = 0
        self.write_errors = 0
        self._condition = threading.Condition()
        self._pending: str | None = None
        self._latest: str | None = None
        self._busy = False
        self._closed = False
        self._thread: threading.Thread | None = None

    @property
    def errors(self) -> int:
        return self.write_errors

    @property
    def dropped(self) -> int:
        return self.dropped_events

    @property
    def current_snapshot(self) -> dict[str, Any] | None:
        """Independent copy of latest accepted state, even while disk is slow."""
        with self._condition:
            if self._latest is None:
                return None
            snapshot = json.loads(self._latest)
            snapshot['status_writer'] = {
                'dropped_events': self.dropped, 'write_errors': self.errors,
                'last_error': self.last_error}
            return snapshot

    def write(self, current_state: str, reason: str, **extra: Any) -> None:
        with self._condition:
            if self._closed:
                self.dropped_events += 1
                return
            if self._pending is not None:
                self.dropped_events += 1
            payload = {**self.base, "current_state": current_state, "reason": reason,
                       "source_ts": time.time(), **extra,
                       "status_writer": {"dropped_events": self.dropped_events,
                                         "write_errors": self.write_errors,
                                         "last_error": self.last_error}}
            try:
                # Freeze values on the owner thread; worker receives only text.
                self._pending = json.dumps(payload, sort_keys=True,
                                           ensure_ascii=False, default=str) + "\n"
                self._latest = self._pending
            except Exception as exc:
                self.dropped_events += 1
                self.write_errors += 1
                self.last_error = type(exc).__name__
                return
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, daemon=True,
                                                name="v3-status-writer")
                self._thread.start()
            self._condition.notify_all()
        if not self.background and not self.flush():
            self.last_error = 'writer_flush_timeout'

    def _write_file(self, payload: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_name(f".{self.path.name}.{os.getpid()}.{id(self)}.tmp")
        with temp.open("w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp, self.path)

    def _run(self) -> None:
        next_write = 0.0
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._pending is not None or self._closed)
                if self._pending is None:
                    return
                # Coalesce ordinary updates at up to 10Hz. Shutdown drains now.
                self._condition.wait_for(lambda: self._closed,
                                         max(0.0, next_write - time.monotonic()))
                payload, self._pending = self._pending, None
                self._busy = True
            try:
                self._write_file(payload)
            except Exception as exc:
                with self._condition:
                    self.write_errors += 1
                    self.dropped_events += 1
                    self.last_error = type(exc).__name__
            else:
                with self._condition:
                    self.last_error = None
            finally:
                with self._condition:
                    self._busy = False
                    next_write = time.monotonic() + (0.1 if self.background else 0)
                    self._condition.notify_all()

    def flush(self, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(
                lambda: self._pending is None and not self._busy, timeout)

    async def close(self, timeout: float = 2.0) -> bool:
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join, timeout)
            return not self._thread.is_alive()
        return True


class ReadOnlyObserver:
    """Best-effort research sink with no return value and no gating API."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.last_error: str | None = None

    def publish(self, event: Mapping[str, Any]) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"source_ts": time.time(), **dict(event)}
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, sort_keys=True, ensure_ascii=False,
                                    default=str) + "\n")
            self.last_error = None
        except OSError as exc:
            # Research telemetry is deliberately unable to veto or delay risk
            # closure.  The status path remains the production alert surface.
            self.last_error = f"{type(exc).__name__}: {exc}"


def canonical_config_hash(settings: Any, *, pair_identity: str) -> str:
    values = asdict(settings)
    # Absent opening overrides retain the exact pre-extension config identity.
    for key in ('open_hedge_rooms_bps', 'open_hedge_hard_cap_bps'):
        if values.get(key) is None:
            values.pop(key, None)
    payload = {"pair_identity": pair_identity, "settings": values}
    encoded = json.dumps(payload, sort_keys=True, default=str,
                         separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()
