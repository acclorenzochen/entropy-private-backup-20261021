"""Standalone V3 production loop.

The loop owns only the frozen V3 policy and the narrow audited adapter bridge.
Legacy signals, regimes, EV models, dashboards, and Engine state are neither
imported nor consulted.  Recovery/hedging decisions depend only on durable
intent evidence and authoritative per-venue positions.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, replace
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import math
import hashlib
import json
import hashlib
from pathlib import Path
import sys
import time
import uuid
from typing import Any, Callable

from ..funding import FundingRiskSnapshot
from ..lighter_requests import RequestDeferred
from .core import (
    BPS, Book, Direction, ExitDecision, Instrument, Market, OpenPlan, Settings, Side, dec,
    exit_decision, plan_open, price_limit,
)
from .economics import (
    actual_reversion_bps, break_even_reversion_bps, directional_dislocation,
    execution_cost_bps, paired_basis_bps, weighted_vwap,
)
from .journal import Intent, Journal, PendingOrder, Result
from .live_host import (
    AuditedLiveExecutor, BatchAuthorization, OrderRefFactory, ReadOnlyObserver,
    StatusWriter, VenueAdapterBroker, canonical_config_hash,
    consume_order_timing, order_timing_trace,
)
from .reference import FastReference, load_checkpoint, save_checkpoint
from .parameter_config import (
    apply_parameters, parameter_limits, parameter_record, parameter_scope,
    parameter_values, validate_document, validate_parameters,
    LEGACY_PARAMETER_FIELDS, OPENING_PARAMETER_FIELDS, project_record, opening_policies,
)
from .funding_hedge import (
    entry_direction, liquidation_distance, pair_income_usd_per_hour,
    remaining_cost_usd, reversal_confirmed, settlements_complete,
)


class RuntimeHalt(RuntimeError):
    pass


FUNDING_RISK_REQUEST_TIMEOUT_SECONDS = 10.0
FUNDING_RISK_RETRY_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class RuntimeVenues:
    entropy: Any
    hedge: Any


@dataclass(frozen=True)
class PositionEvidence:
    entropy_units: int | None
    hedge_units: int | None
    entropy_source: str
    hedge_source: str
    entropy_authoritative: bool
    hedge_authoritative: bool
    observed_ts: float
    last_authoritative_ts: float | None
    degraded: bool
    error: str | None
    entropy_generation: int | None = None
    hedge_generation: int | None = None
    cycle_id: str | None = None
    entropy_received_ts: float | None = None
    hedge_received_ts: float | None = None
    stream_watermark: tuple | None = None

    @property
    def fully_authoritative(self) -> bool:
        return self.entropy_authoritative and self.hedge_authoritative


@dataclass(frozen=True)
class FlatLease:
    entropy_units: int
    hedge_units: int
    checked_ts: float
    valid_until: float
    generation: int


@dataclass(frozen=True)
class CloseContext:
    cycle_id: str
    entropy_units: int
    hedge_units: int
    cycle_updated_ts: float
    observed_ts: float
    hedge_generation: int | None
    hedge_source: str
    entropy_generation: int | None = None
    entropy_received_ts: float | None = None
    hedge_received_ts: float | None = None
    order_watermark: tuple = ()


@dataclass(frozen=True)
class OpenDecision(OpenPlan):
    entropy_top_px: Decimal
    entropy_marginal_px: Decimal
    entropy_limit_px: Decimal
    entropy_book_revision: str
    hedge_book_revision: str
    decision_ts: float
    decision_mono_ns: int
    decision_market: Market


@dataclass(frozen=True)
class OpenHandoff:
    cycle_id: str
    cycle_updated_ts: float
    evidence: PositionEvidence
    orders: tuple
    steps: tuple


def _tick_for(venue: Any, price: float) -> Decimal:
    if venue.kind == "lighter":
        return Decimal(1).scaleb(-int(venue.price_decimals))
    max_dec = max(0, 6 - int(venue.size_decimals))
    sig_dec = 4 - math.floor(math.log10(price))
    places = max(0, min(max_dec, sig_dec))
    return Decimal(1).scaleb(-places)


def instrument_from_venues(pair_id: str, entropy: Any, hedge: Any) -> Instrument:
    if entropy.conf.symbol != hedge.conf.symbol:
        raise RuntimeHalt("venues do not have an exact common symbol")
    e_step = Decimal(1).scaleb(-int(entropy.size_decimals))
    h_step = Decimal(1).scaleb(-int(hedge.size_decimals))
    # Both current adapters expose linear base quantities.  The coarser power-
    # of-ten step is the exact common grid; no contract multiplier is inferred.
    step = max(e_step, h_step)
    em = entropy.book.mid()
    hm = hedge.book.mid()
    if not em or not hm:
        raise RuntimeHalt("cannot establish price ticks before both books are ready")
    return Instrument(
        pair_id, step, _tick_for(entropy, em), _tick_for(hedge, hm),
        dec(entropy.fee_bps), dec(hedge.fee_bps))


def _levels(raw: list[tuple[float, float]], step: Decimal) -> tuple[tuple[Decimal, int], ...]:
    converted = []
    for price, quantity in raw:
        units = int((dec(quantity) / step).to_integral_value(rounding=ROUND_FLOOR))
        if units > 0:
            converted.append((dec(price), units))
    if not converted:
        raise RuntimeHalt("book has no executable common-unit depth")
    return tuple(converted)


def market_from_venues(venues: RuntimeVenues, instrument: Instrument,
                       *, now: float, settings: Settings) -> Market:
    books = []
    for venue in (venues.entropy, venues.hedge):
        book = venue.book
        if not book.is_fresh(settings.max_book_age_seconds):
            raise RuntimeHalt(f"{venue.key} book is not fresh")
        # Both event-driven caches are captured synchronously below (there is
        # no await in this function).  A venue's last level-change timestamp
        # can legitimately be older when its book did not change; treating
        # that as the snapshot time creates false cross-venue skew.  Stream
        # liveness, disconnect invalidation, and Lighter nonce continuity are
        # validated before this atomic host-side capture.
        books.append(Book(
            _levels(book.sorted_bids(), instrument.quantity_step),
            _levels(book.sorted_asks(), instrument.quantity_step),
            now, f"{venue.key}:{book.last_update_ts:.9f}"))
    market = Market(*books)
    reason = market.valid_reason(now, settings)
    if reason:
        raise RuntimeHalt(reason)
    return market


def _minimum_units(venue: Any, instrument: Instrument, price: Decimal) -> int:
    base = (dec(venue.min_base) / instrument.quantity_step).to_integral_value(
        rounding=ROUND_CEILING)
    quote = (dec(venue.min_quote) / price / instrument.quantity_step).to_integral_value(
        rounding=ROUND_CEILING)
    return max(1, int(base), int(quote))


def _opening_units_cap(book: Book, side: Side, step: Decimal, cap: Decimal) -> int:
    cash, units = Decimal(0), 0
    for price, available in book.asks if side is Side.BUY else book.bids:
        unit_cash = price * step
        take = min(available, int(((cap - cash) / unit_cash)
                                 .to_integral_value(rounding=ROUND_FLOOR)))
        cash += unit_cash * take
        units += take
        if take < available:
            break
    return units


def choose_open_plan(market: Market, instrument: Instrument, settings: Settings,
                     reference_bps: Decimal, *,
                     entropy_venue: Any, hedge_venue: Any) -> OpenDecision | None:
    # Bid/ask execution can only reduce midpoint deviation.
    if abs(market.mid_basis_bps - reference_bps) < settings.entry_band_bps - Decimal('0.000000001'):
        return None
    minimum = max(1, *(int((dec(venue.min_base) / instrument.quantity_step)
                          .to_integral_value(rounding=ROUND_CEILING))
                      for venue in (entropy_venue, hedge_venue)))
    candidates: list[OpenDecision] = []
    decision_ts = time.time()
    decision_mono_ns = time.monotonic_ns()
    for direction in Direction:
        side = direction.entropy_side
        best_basis = (market.entropy.top(side) / market.hedge.top(side.opposite) - 1) * BPS
        best_distance = (reference_bps - best_basis if side is Side.BUY
                         else best_basis - reference_bps)
        if best_distance < settings.entry_band_bps:
            continue
        upper = min(_opening_units_cap(book, leg_side, instrument.quantity_step,
                                       settings.max_notional_usd)
                    for book, leg_side in ((market.entropy, side),
                                           (market.hedge, side.opposite)))
        for units in range(upper, minimum - 1, -1):
            try:
                plan = plan_open(market, instrument, settings, reference_bps,
                                 direction, units)
                if plan is not None:
                    side = plan.direction.entropy_side
                    ec, marginal = market.entropy.quote(
                        side, units, instrument.quantity_step)
                    hc, _ = market.hedge.quote(side.opposite, units, instrument.quantity_step)
                    if ec < dec(entropy_venue.min_quote) or hc < dec(hedge_venue.min_quote):
                        break
                    top = market.entropy.top(side)
                    rooms, cap_bps = settings.first_leg_execution_policy('open')
                    limit_px = price_limit(
                        top, marginal, side, rooms[0], cap_bps,
                        instrument.entropy_tick)
            except ValueError:
                plan = None
            if plan is not None:
                candidates.append(OpenDecision(
                    plan.direction, plan.units, plan.reference_bps,
                    plan.executable_basis_bps, plan.distance_bps,
                    plan.notional_usd, plan.reference_scenario_usd,
                    plan.scenario_floor_usd, plan.entry_quoted_cash_usd,
                    plan.entry_quoted_fees_usd, top, marginal, limit_px,
                    market.entropy.revision, market.hedge.revision, decision_ts,
                    decision_mono_ns, market))
                break
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item.distance_bps, item.units))


class LiveRuntime:
    def __init__(self, *, settings: Settings, legacy_config: Any,
                 authorization: BatchAuthorization, pair_id: str,
                 deployment: str, venues: RuntimeVenues, journal: Journal,
                 status: StatusWriter, observer: ReadOnlyObserver,
                 stop: asyncio.Event,
                 session_max_cycles: int | None = None,
                 reference_sampler_enabled: bool = True,
                 reference_checkpoint_path: str | Path = 'logs/v3/reference_checkpoint.json',
                 operator_socket_path: str | Path | None = None) -> None:
        if operator_socket_path is not None and (
                settings.opening_experiment_id or authorization.opening_experiment_id is not None):
            raise RuntimeHalt('operator_control_unsupported_opening_ab')
        self.settings = replace(
            settings,
            max_notional_usd=min(settings.max_notional_usd,
                                 authorization.max_notional_usd))
        self.cfg = legacy_config
        self.authorization = authorization
        self.pair_id = pair_id
        self.deployment = deployment
        self.venues = venues
        self.journal = journal
        self.status = status
        self._parameters_base_settings = self.settings
        self._parameters_base_config_hash = self.status.base['config_hash']
        self._parameters_scope = None
        self.status.base.update(strategy_mode=self.settings.strategy_mode,
                                pair_identity=pair_id)
        self.observer = observer
        self.operator_socket_path = operator_socket_path
        self.operator_control = None
        self._operator_scope = None
        self._operator_wake = asyncio.Event()
        self.stop = stop
        if session_max_cycles is not None and session_max_cycles < 1:
            raise ValueError('session_max_cycles must be positive')
        self.session_max_cycles = session_max_cycles
        self.reference_sampler_enabled = reference_sampler_enabled
        self._starting_position_cycles = 0
        self.instrument: Instrument | None = None
        self.brokers: dict[str, VenueAdapterBroker] = {}
        self.executor: AuditedLiveExecutor | None = None
        self.reference_checkpoint_path = Path(reference_checkpoint_path)
        started_at = time.time()
        checkpoint = load_checkpoint(self.reference_checkpoint_path)
        self.reference = FastReference(
            pair_id, self.settings, checkpoint, started_at=started_at)
        self._last_reference_checkpoint_ts = (
            checkpoint.get('ts') if checkpoint is not None else None)
        self._last_reference_checkpoint_save = 0.0
        self.changed = asyncio.Event()
        self._funding_rates = (None, None)
        self._funding_poll_ts = 0.0
        self._funding_entry_poll_ts = 0.0
        self._funding_risk_ts = 0.0
        self._basis_accounting_attempts: dict[str, float] = {}
        self._accounting_queue_cursor = 0
        self._funding_risk_lock = asyncio.Lock()
        self._funding_risk_signature: tuple | None = None
        self._funding_risk_generation = 0
        self._funding_risk_attempt = 0
        self._funding_risk_tasks: dict[str, asyncio.Task] = {}
        self._funding_risk_started: dict[str, float] = {}
        self._funding_risk_cache: dict[str, tuple] = {}
        self._funding_risk_failures: dict[str, tuple] = {}
        self._funding_risk_changed = asyncio.Event()
        self._funding_reads: set[asyncio.Task] = set()
        self._funding_exit_event = asyncio.Event()
        self._funding_exit_event_cycle_id: str | None = None
        self._funding_exit_event_loop = None
        self._basis_risk_exit_task: asyncio.Task | None = None
        self._risk_trigger: dict[str, object] | None = None
        self._funding_hold_observed_ts = 0.0
        self._funding_common_step: Decimal | None = None
        self._reconcile_backoff_seconds = 2.0
        self._last_equity_poll = 0.0
        self._mfe_cache: dict[str, tuple[Decimal, float]] = {}
        self._mfe_last_flush: dict[str, float] = {}
        self._flat_lease: FlatLease | None = None
        self._flat_generation = 0
        self._last_position_evidence: PositionEvidence | None = None
        self._open_handoff: OpenHandoff | None = None
        self._close_context: CloseContext | None = None
        self._fast_close_step_key: str | None = None
        self._fast_close_purpose: str | None = None
        self._fast_close_order_ref: str | None = None
        self._deferred_fast_close_telemetry: Callable[[], None] | None = None
        self._deferred_open_telemetry: Callable[[], None] | None = None
        self._open_timing: dict[str, dict[str, object]] = {}
        self.clock_session_id = uuid.uuid4().hex
        self._exit_trigger: dict[str, object] | None = None
        self.telemetry_errors = 0
        self.telemetry_dropped = 0
        self._last_equity: Decimal | None = None
        self._telemetry_queue: list[Callable[[], None]] = []
        self._in_handoff = False
        self._reference_writer = None
        self._cycle_lock = asyncio.Lock()
        self._basis_exit_lock = asyncio.Lock()
        self._basis_exit_retry_after = 0.0
        self._parallel_tasks: dict[str, asyncio.Task] = {}
        self._parallel_completions: list[tuple[str, Callable | None]] = []
        self._parallel_resolve_after: dict[str, float] = {}
        self._position_audit_requested = asyncio.Event()
        self._position_audit_after = 0.0
        self._basis_recovered = False
        self._basis_event_watermark = None
        self._basis_account_received_ts = 0.0
        self._completion_ticket = None
        self._send_retry_after: dict[str, float] = {}
        experiment = self.settings.opening_experiment_id
        if experiment or self.authorization.opening_experiment_id is not None:
            if (not experiment or self.authorization.opening_experiment_id != experiment
                    or self.settings.strategy_mode != 'basis'
                    or not self.authorization.approved_live
                    or self.authorization.max_position_cycles != 20
                    or self.authorization.loss_budget_usd is not None
                    or self.authorization.max_notional_usd > Decimal('200')
                    or self.settings.max_notional_usd > Decimal('200')):
                raise RuntimeHalt('invalid explicit opening experiment authorization')
            self.status.base['opening_experiment_contract'] = {
                'experiment_id': experiment, 'batch_id': self.authorization.batch_id,
                'pair_identity': self.pair_id, 'attempts_per_group': 10,
                'max_attempts': 20, 'group_order': 'AB',
                'group_A': 'parallel_equal_units',
                'group_B': 'terminal_entropy_unmatched_units',
                'cumulative_loss_budget_usd': None,
                'config_hash': canonical_config_hash(self.settings, pair_identity=self.pair_id),
                'authorization_hash': canonical_config_hash(self.authorization, pair_identity=self.pair_id),
                'source_build_hash': self._opening_source_build_hash(),
                'max_notional_usd': str(min(self.settings.max_notional_usd,
                                            self.authorization.max_notional_usd))}

    def _basis_events(self) -> tuple | None:
        """Local connection and external-order evidence; pong is not a sequence proof."""
        known = {'entropy': set(), 'hedge': set()}
        for row in self.journal.db.execute('SELECT venue,order_ref FROM orders'):
            known[row['venue']].add(str(row['order_ref']).lower())
        tokens = []
        for key in ('entropy', 'hedge'):
            venue = getattr(self.venues, key)
            feed = getattr(venue, 'orders_feed', None)
            if feed is None:
                # Offline adapters have no stream. Production ready_to_trade
                # requires its authenticated order stream.
                if not venue.ready_to_trade():
                    return None
                tokens.append((None,))
                continue
            if not feed.ready.is_set():
                return None
            fresh = getattr(feed, 'fresh_account_snapshot', None)
            if fresh is not None and fresh() is None:
                return None
            external = []
            if key == 'entropy':
                own_oids = {oid for oid, order in feed._orders.items()
                            if order['cloid'].lower() in known[key]}
                external.extend((oid, o['status'], o['filled_base'])
                                for oid, o in feed._orders.items() if oid not in own_oids)
                external.extend(('fill', oid, f['filled_base'])
                                for oid, f in feed._fills.items() if oid not in own_oids)
            else:
                external.extend((coi, o['status'], o['filled_base'])
                                for coi, o in getattr(feed, 'order_updates', {}).items()
                                if str(coi) not in known[key])
                external.append(('unidentified', getattr(feed, 'unidentified_order_revision', 0)))
            if any(len(fact) == 3 and fact[1] in {'open', 'pending', 'in-progress'}
                   for fact in external):
                return None
            tokens.append((id(feed), feed.connection_generation, tuple(external)))
        return tuple(tokens)

    def _operator_running(self) -> bool:
        scope = getattr(self, '_operator_scope', None)
        return scope is None or self.journal.operator_control_state(scope)['intent'] == 'RUNNING'

    def _operator_entry_gate(self, intent: Intent) -> str | None:
        # Existing unmatched E may still be completed on H while STOPPING.
        if intent.venue == 'entropy' and not intent.reduce_only and not self._operator_running():
            return 'operator_entries_paused'
        return None

    def _wake_operator(self) -> None:
        self.changed.set()
        self._operator_wake.set()

    def _publish_batch_state(self, batch: dict) -> None:
        self.status.base.update(position_cycles=batch['position_cycles'],
            max_position_cycles=batch['max_position_cycles'],
            max_position_cycles_disabled=batch['max_position_cycles'] is None,
            loss_used_usd=str(batch['loss_used_usd']),
            loss_budget_usd=str(batch['loss_budget_usd']) if batch['loss_budget_usd'] is not None else None,
            loss_budget_disabled=batch['loss_budget_usd'] is None)

    def enable_operator_control(self, binding: dict) -> None:
        from .operator_control import OperatorControl
        if (self.settings.opening_experiment_id
                or self.authorization.opening_experiment_id is not None
                or self.journal.db.execute('SELECT 1 FROM opening_experiments WHERE batch_id=?',
                    (self.authorization.batch_id,)).fetchone()):
            raise ValueError('operator_control_unsupported_opening_ab')
        if self.settings.strategy_mode != 'basis':
            raise ValueError('operator control supports ordinary basis only')
        intent_scope = {key: binding[key] for key in
                        ('account_binding_hash', 'pair_identity', 'deployment')}
        self._operator_scope = hashlib.sha256(json.dumps(
            intent_scope, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.restore_operator_parameters(binding)
        binding = {**binding, 'config_hash': self.status.base['config_hash']}
        self.journal.ensure_operator_control(self._operator_scope, initial_intent='STOPPED')
        self.operator_control = OperatorControl(binding, snapshot=self._operator_snapshot,
            commit=self._operator_commit, operation=self._operator_operation,
            wake=self._wake_operator, save_parameters=self._operator_save_parameters,
            parameter_save=self._operator_parameter_save)

    def restore_operator_parameters(self, binding: dict | None = None) -> None:
        """Restore before live recovery/dispatch, after account identity is known."""
        binding = self._operator_binding() if binding is None else binding
        scope = parameter_scope(binding)
        if (self.journal.operator_parameters(scope) is None
                and (self.journal.active_live_cycle() is not None or self.journal.pending())):
            raise ValueError('parameters_initialization_requires_flat')
        limits = parameter_limits(self._parameters_base_settings, self.authorization, self.cfg)
        values = validate_parameters({key: value for key, value in
            parameter_values(self._parameters_base_settings).items() if key in LEGACY_PARAMETER_FIELDS}, limits)
        initial = dict(schema_version=1, base_config_hash=self._parameters_base_config_hash,
            limits={key: limits[key] for key in LEGACY_PARAMETER_FIELDS},
            current=parameter_record(self._parameters_base_settings,
                pair_id=self.pair_id, revision=0, values=values), saved=None)
        document = self.journal.ensure_operator_parameters(scope, initial)
        validate_document(document, base_hash=self._parameters_base_config_hash,
            base_settings=self._parameters_base_settings, pair_id=self.pair_id, limits=limits)
        first_restore = self._parameters_scope is None
        intent_scope = {key: binding[key] for key in
                        ('account_binding_hash', 'pair_identity', 'deployment')}
        self._operator_scope = hashlib.sha256(json.dumps(
            intent_scope, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.journal.ensure_operator_control(self._operator_scope, initial_intent='STOPPED')
        if first_restore:
            self.journal.pause_operator_boot(self._operator_scope)
        self._parameters_scope = scope
        current = document['current']
        settings = (self._parameters_base_settings if current['revision'] == 0 else
                    apply_parameters(self._parameters_base_settings, current['values']))
        self._apply_operator_parameters(settings, current)

    @staticmethod
    def _parameter_status(settings, record):
        policies = {}
        for name, policy in (('entropy', settings.first_leg_execution_policy),
                             ('hedge', settings.hedge_execution_policy)):
            policies[name] = {purpose: dict(rooms_bps=[str(v) for v in policy(purpose)[0]],
                hard_cap_bps=str(policy(purpose)[1])) for purpose in ('open', 'close')}
        return dict(config_hash=record['config_hash'],
            effective_settings=json.loads(json.dumps(asdict(settings), default=str, allow_nan=False)),
            execution_policies=policies, active_parameter_manifest=dict(record))

    def _apply_operator_parameters(self, settings, record, prepared_status=None) -> None:
        prepared_status = (self._parameter_status(settings, record)
                           if prepared_status is None else prepared_status)
        self.settings = settings
        self.reference.cfg = settings
        self.status.base.update(prepared_status)
        if self.operator_control is not None:
            self.operator_control.binding['config_hash'] = record['config_hash']

    def _operator_parameters(self, state) -> dict:
        document = self.journal.operator_parameters(self._parameters_scope)
        current, saved = project_record(document['current']), project_record(document['saved'])
        def policies(record):
            settings = (self._parameters_base_settings if record['revision'] == 0 else
                        apply_parameters(self._parameters_base_settings, record['values']))
            return opening_policies(settings)
        reason = ('parameters_require_stopped' if state['intent'] != 'STOPPED' else
            'parameters_active_cycle' if state['active_cycle_id'] is not None else
            'parameters_pending_orders' if state['pending_orders'] else
            'parameters_position_refresh_required' if not state['position_authoritative'] else
            'parameters_require_flat' if state['entropy_units'] != 0 or state['hedge_units'] != 0 else None)
        return dict(base_config_hash=document['base_config_hash'], current=current,
            saved=saved, limits=parameter_limits(self._parameters_base_settings, self.authorization, self.cfg),
            can_save=reason is None, reason_code=reason,
            capabilities=dict(opening_concessions=True),
            opening_policies=dict(base=opening_policies(self._parameters_base_settings),
                                  current=policies(current), saved=policies(saved) if saved else None))

    def _operator_parameter_save(self, idempotency_key):
        prior = self.journal.operator_parameter_save(self._parameters_scope, idempotency_key)
        if prior is None:
            return None
        state = self._operator_snapshot()
        return dict(parameters=state['parameters'], state=state,
                    **{key: prior['receipt'][key] for key in ('revision', 'config_hash')})

    def _operator_save_parameters(self, expected_revision, values, idempotency_key) -> dict:
        from .operator_control import ControlRefusal
        try:
            limits = parameter_limits(self._parameters_base_settings, self.authorization, self.cfg)
            values = validate_parameters(values, limits)
            opening_fields_present = set(values) != LEGACY_PARAMETER_FIELDS
            if type(expected_revision) is not int or expected_revision < 0:
                raise ValueError('invalid_parameters_revision')
            fingerprint = hashlib.sha256(json.dumps(dict(expected_revision=expected_revision,
                values={key: value for key, value in values.items()
                        if key not in OPENING_PARAMETER_FIELDS or value is not None}),
                sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            prior = self.journal.operator_parameter_save(self._parameters_scope, idempotency_key)
            if prior is not None:
                if prior['request_fingerprint'] != fingerprint:
                    raise ValueError('parameters_idempotency_payload_changed')
                if ('opening_fields_present' in prior['receipt']
                        and prior['receipt']['opening_fields_present'] != opening_fields_present):
                    raise ValueError('parameters_idempotency_payload_changed')
                return self._operator_parameter_save(idempotency_key)
            state = self._operator_snapshot()
            if not state['parameters']['can_save']:
                raise ValueError(state['parameters']['reason_code'])
            if not opening_fields_present:
                target = state['parameters']['saved'] or state['parameters']['current']
                values.update({key: target['values'][key] for key in OPENING_PARAMETER_FIELDS})
            settings = apply_parameters(self._parameters_base_settings, values)
            record = parameter_record(settings, pair_id=self.pair_id,
                                      revision=expected_revision + 1, values=values)
            receipt = self.journal.save_operator_parameters(self._parameters_scope,
                operator_scope=self._operator_scope, expected_revision=expected_revision,
                record=record, idempotency_key=idempotency_key, request_fingerprint=fingerprint,
                confirmation_evidence=state, document_limits=limits,
                opening_fields_present=opening_fields_present)
        except ValueError as exc:
            raise ControlRefusal(str(exc)) from None
        self._wake_operator()
        state = self._operator_snapshot()
        return dict(parameters=state['parameters'], state=state,
                    **{key: receipt[key] for key in ('revision', 'config_hash')})

    def _operator_binding(self) -> dict:
        address = getattr(getattr(self.venues.entropy, 'account', None), 'query_address', None)
        creds = getattr(getattr(self.venues.hedge, 'conf', None), 'lighter_creds', None)
        profile = getattr(getattr(self.venues.hedge, 'conf', None), 'lighter_profile', None)
        if (not isinstance(address, str) or len(address) != 42
                or creds is None or type(creds.account_index) is not int or profile is None):
            raise RuntimeHalt('operator account binding unavailable')
        account = dict(hl_query_account=address.lower(),
            lighter_account_index=creds.account_index, lighter_profile=profile.name)
        return dict(account_binding_hash=hashlib.sha256(json.dumps(
            account, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            pair_identity=self.pair_id, deployment=self.deployment,
            batch_id=self.authorization.batch_id, config_hash=self.status.base['config_hash'])

    def _operator_snapshot(self) -> dict:
        state = self.journal.operator_control_state(self._operator_scope)
        active = self.journal.active_live_cycle()
        pending = self.journal.pending()
        batch = self.journal.batch_state(self.authorization.batch_id)
        self._publish_batch_state(batch)
        self._basis_state_ready()
        evidence = self._last_position_evidence
        authoritative = bool(evidence is not None and evidence.fully_authoritative
            and 0 <= time.time() - evidence.observed_ts <= 15
            and not any(order['state'] == 'SUBMITTING' for order in pending))
        if active is not None and authoritative:
            pending_venues = {order['venue'] for order in pending}
            authoritative = (self._position_covers_orders(active, evidence)
                and all(order['submitted_ts'] is None or evidence.observed_ts >= order['submitted_ts']
                        for order in pending)
                and all(getattr(evidence, key + '_units') == active[key + '_units']
                        for key in ('entropy', 'hedge') if key not in pending_venues))
        elif active is None and authoritative:
            authoritative = (evidence.entropy_units == 0 and evidence.hedge_units == 0
                             and self._basis_state_ready())
        risk = [batch['block_reason']] if batch['block_reason'] else []
        if self.session_max_cycles is not None and (
                batch['position_cycles'] - self._starting_position_cycles >= self.session_max_cycles):
            risk.append('SESSION_CYCLE_LIMIT')
        if pending:
            risk.append('ORDER_RECOVERY')
        if getattr(self.executor, 'entries_halted', False):
            risk.append('PERSISTENCE_FAILURE')
        if not self._basis_recovered:
            risk.append('POSITION_RECONCILIATION')
        if not authoritative:
            self._position_audit_requested.set()
        allowed = ['stop'] if state['intent'] != 'STOPPED' else []
        if not risk and authoritative:
            allowed.append('start')
        if active is not None and authoritative:
            allowed.append('close')
        phase = ('EXITING' if active and active['basis_exit_requested_ts'] is not None
                 else 'MANAGING' if active else state['intent'])
        if phase == 'RUNNING' and risk:
            phase = 'BLOCKED'
        snapshot = dict(**state, phase=phase, allowed_actions=allowed,
            risk_locks=list(dict.fromkeys(risk)), blocked_reason=next(iter(risk), None),
            active_cycle_id=active['cycle_id'] if active else None,
            entropy_units=evidence.entropy_units if authoritative else None,
            hedge_units=evidence.hedge_units if authoritative else None,
            position_observed_ts=evidence.observed_ts if evidence is not None else None,
            position_authoritative=authoritative,
            quantity_step=str(self.instrument.quantity_step),
            order_watermark=[list(row) for row in self._order_watermark(active['cycle_id'])]
                if active else [],
            pending_orders=[{key: row[key] for key in
                ('order_ref', 'venue', 'state', 'reduce_only', 'filled_units')} for row in pending])
        snapshot['parameters'] = self._operator_parameters(snapshot)
        return snapshot

    def _operator_operation(self, operation_id=None, *, idempotency_key=None):
        operation = self.journal.operator_operation(self._operator_scope, operation_id,
                                                   idempotency_key=idempotency_key)
        if operation is not None and operation['action'] == 'close':
            row = self.journal.db.execute('SELECT l.phase,c.accounting_final,c.pnl_usd '
                'FROM live_cycles l JOIN cycles c USING(cycle_id) WHERE cycle_id=?',
                (operation['expected_cycle_id'],)).fetchone()
            fees = self.journal.fee_state(operation['expected_cycle_id'])
            operation['evidence'].update(accounting_final=bool(row['accounting_final']),
                net_pnl_usd=row['pnl_usd'] if row['accounting_final'] else None,
                fees_complete=fees['fees_complete'], known_fees_usd=str(fees['known_fees_usd']))
            operation['evidence'].update(self.journal.operator_control_state(self._operator_scope))
            if row['phase'] == 'DONE':
                # Late actual fees may revoke finality after the durable command completed.
                operation['state'] = 'completed' if row['accounting_final'] else 'flat_pending_accounting'
        return operation

    def _operator_commit(self, *, action, expected_revision, idempotency_key,
                         request_fingerprint, expected_cycle_id=None,
                         parameters_revision=None, parameters_hash=None) -> dict:
        existing = self._operator_operation(idempotency_key=idempotency_key)
        new_settings = new_record = prepared_status = None
        if existing is None:
            state = self._operator_snapshot()
            if action == 'start' and action not in state['allowed_actions']:
                raise ValueError(state['blocked_reason'] or 'authoritative_position_refresh_required')
            if action == 'close' and (expected_cycle_id != state['active_cycle_id']
                                     or not state['position_authoritative']):
                raise ValueError('close_target_or_authority_changed')
            if action == 'start':
                from .operator_control import ControlRefusal
                parameters = state['parameters']
                if parameters_revision is None and parameters_hash is None:
                    if parameters['saved'] is not None:
                        raise ControlRefusal('saved_parameters_require_explicit_start')
                else:
                    if not parameters['can_save']:
                        raise ControlRefusal(parameters['reason_code'])
                    new_record = parameters['saved'] or parameters['current']
                    if (type(parameters_revision) is not int
                            or parameters_revision != new_record['revision']
                            or parameters_hash != new_record['config_hash']):
                        raise ControlRefusal('parameters_revision_changed')
                    new_settings = (self._parameters_base_settings if parameters_revision == 0
                                    else apply_parameters(self._parameters_base_settings, new_record['values']))
                    prepared_status = self._parameter_status(new_settings, new_record)
        operation = self.journal.commit_operator_command(self._operator_scope,
            action=action, expected_revision=expected_revision,
            idempotency_key=idempotency_key, request_fingerprint=request_fingerprint,
            expected_cycle_id=expected_cycle_id,
            parameters_scope=self._parameters_scope if action == 'start' else None,
            parameters_revision=parameters_revision, parameters_hash=parameters_hash,
            parameters_limits=(parameter_limits(self._parameters_base_settings, self.authorization, self.cfg)
                               if action == 'start' and parameters_revision is not None else None),
            confirmation_evidence=state if existing is None else None)
        if new_settings is not None:
            # No await or fallible serialization after the durable atomic commit.
            self._apply_operator_parameters(new_settings, new_record, prepared_status)
        if existing is None and action == 'close':
            self.journal.record_exit_trigger(expected_cycle_id, trigger_ts=time.time(),
                reason='operator_close', trigger_basis_bps=None,
                reversion_bps=None, executable_pnl_usd=None,
                trigger_mono_ns=time.monotonic_ns(), trigger_clock_session_id=self.clock_session_id)
        if action == 'start' and operation['state'] == 'accepted':
            self.journal.update_operator_operation(self._operator_scope,
                operation['operation_id'], state='completed', evidence=self._operator_snapshot())
            operation = self._operator_operation(operation['operation_id'])
        self._wake_operator()
        return operation

    def _update_operator_progress(self) -> None:
        if self.operator_control is None:
            return
        state = self._operator_snapshot()
        flat = (state['position_authoritative'] and state['entropy_units'] == 0
                and state['hedge_units'] == 0 and state['active_cycle_id'] is None
                and not state['pending_orders'])
        self.journal.advance_operator_stop(self._operator_scope, authoritative_flat=flat)
        for operation in self.journal.operator_pending_operations(self._operator_scope):
            progress = self._operator_snapshot()
            outcome = 'executing'
            if operation['action'] == 'stop':
                outcome = 'completed' if progress['intent'] == 'STOPPED' else 'executing'
            elif operation['action'] == 'close':
                cid = operation['expected_cycle_id']
                row = self.journal.db.execute('SELECT l.*,c.accounting_final AS final '
                    'FROM live_cycles l JOIN cycles c USING(cycle_id) WHERE cycle_id=?', (cid,)).fetchone()
                orders = self.journal.cycle_orders(cid)
                progress['cycle_id'] = cid
                progress['legs'] = [{
                    'venue': key, 'remaining_units': row[f'{key}_units'],
                    'initial_units': operation['receipt']['confirmation_evidence'][f'{key}_units'],
                    'orders': [{field: order[field] for field in
                        ('order_ref', 'state', 'filled_units', 'reduce_only')}
                        for order in orders if order['venue'] == key]} for key in ('entropy', 'hedge')]
                progress['accounting_final'] = bool(row['final'])
                canonical = self.journal.db.execute('SELECT pnl_usd FROM cycles WHERE cycle_id=?',
                                                    (cid,)).fetchone()
                progress['net_pnl_usd'] = canonical['pnl_usd'] if row['final'] else None
                fees = self.journal.fee_state(cid)
                progress['fees_complete'] = fees['fees_complete']
                progress['known_fees_usd'] = str(fees['known_fees_usd'])
                if row['phase'] == 'DONE':
                    outcome = 'completed' if row['final'] else 'flat_pending_accounting'
            elif operation['action'] == 'start':
                outcome = 'completed'
            if operation['state'] != outcome or operation['evidence'] != progress:
                self.journal.update_operator_operation(self._operator_scope,
                    operation['operation_id'], state=outcome, evidence=progress)

    def _publish_operator_state(self) -> None:
        if self.operator_control is not None:
            control = self.operator_control.state()
            self.status.base['operator_control'] = control
            self.status.write(control['phase'], 'operator_' + control['intent'].lower())

    def _basis_state_ready(self) -> bool:
        events = self._basis_events()
        if events is None or events != self._basis_event_watermark:
            self._basis_recovered = False
            self._position_audit_requested.set()
        feed = getattr(self.venues.hedge, 'orders_feed', None)
        fresh = getattr(feed, 'fresh_account_snapshot', None)
        snapshot = fresh() if fresh is not None else None
        if snapshot is not None and snapshot.received_ts > self._basis_account_received_ts:
            self._basis_account_received_ts = snapshot.received_ts
            read = getattr(self.venues.hedge, '_position_from_rows', None)
            if read is not None:
                cycle = self.journal.active_live_cycle()
                expected = int(cycle['hedge_units']) if cycle else 0
                try:
                    actual = self.instrument.exact_units(read(snapshot.positions, self.venues.hedge.market_id))
                except (TypeError, ValueError):
                    actual = None
                if actual != expected:
                    self._basis_recovered = False
                    self._position_audit_requested.set()
        return self._basis_recovered and not self.journal.pending()

    def _external_fill_scope(self, venue_key: str) -> dict | None:
        """Derive public history identity from the actual configured adapters."""
        try:
            binding = self._operator_binding()
            entropy, hedge = self.venues.entropy, self.venues.hedge
            address = entropy._query_address()
            configured_profile = hedge.conf.lighter_profile.name
            if (not isinstance(address, str) or address.lower() != entropy.account.query_address.lower()
                    or hedge.profile.name != configured_profile):
                return None
            if venue_key == 'entropy' and entropy.kind == 'hl':
                dex = entropy.conf.hl_dex
                market = f'{dex}:{entropy.conf.symbol}' if dex else entropy.conf.symbol
                if entropy.coin != market:
                    return None
                account, profile, source = address.lower(), None, 'hyperliquid:userFillsByTime'
            elif venue_key == 'hedge' and hedge.kind == 'lighter':
                if type(hedge.market_id) is not int or hedge.market_id < 0:
                    return None
                account, market = str(hedge.conf.lighter_creds.account_index), str(hedge.market_id)
                profile, dex, source = configured_profile, None, 'lighter:trades'
            else:
                return None
            return dict(binding_hash=binding['account_binding_hash'], source_account=account,
                        source_market=market, source_profile=profile, source_dex=dex, source=source)
        except (AttributeError, KeyError, TypeError, RuntimeHalt):
            return None

    async def _recover_external_fills(self, cycle: dict, evidence: PositionEvidence) -> int:
        """Import only uniquely chained account reductions; never submit an order."""
        if (self.instrument is None or evidence.cycle_id != cycle['cycle_id']
                or not math.isfinite(evidence.observed_ts)
                or evidence.observed_ts < cycle['started_ts'] or self.journal.pending()):
            return 0
        recovered = 0
        cid, until = cycle['cycle_id'], evidence.observed_ts
        grid = self.instrument.quantity_step
        for venue_key in ('entropy', 'hedge'):
            scope = self._external_fill_scope(venue_key)
            if scope is None:
                continue
            watermark = self._order_watermark(cid)
            try:
                rows = await getattr(self.venues, venue_key).fetch_fee_fills(cycle['started_ts'] - 2, until)
            except Exception:
                continue  # History failure is unknown, never evidence of zero execution.
            current = self.journal.active_live_cycle()
            if (current is None or current['cycle_id'] != cid or current['phase'] == 'DONE'
                    or self.journal.pending() or self._order_watermark(cid) != watermark
                    or self._external_fill_scope(venue_key) != scope):
                return recovered
            facts, conflicts = {}, set()
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, dict) or any(row.get(k) != scope[k] for k in
                        ('source_account', 'source_market', 'source_profile', 'source_dex', 'source')):
                    continue
                try:
                    key = row['exchange_fill_key']
                    if not isinstance(key, str) or not key:
                        continue
                    ts, received = float(row['ts']), float(row['evidence_received_ts'])
                    quantity, price = dec(row['quantity_exact']), dec(row['price_exact'])
                    fee = dec(row['fee_usd']) if row.get('fee_usd') is not None else None
                    if (not all(v.is_finite() for v in (quantity, price)) or min(quantity, price) <= 0
                            or (fee is not None and not fee.is_finite()) or not math.isfinite(ts)
                            or not math.isfinite(received) or received < ts
                            or not cycle['started_ts'] <= ts <= until or not isinstance(row.get('raw'), dict)):
                        continue
                    units = self.instrument.exact_units(quantity)
                    fact = dict(row=row, ts=ts, quantity=quantity, price=price, units=units,
                                side=Side(row['side']), fee=fee)
                except (KeyError, ValueError, TypeError, ArithmeticError):
                    continue
                old = facts.get(key)
                if old is not None:
                    fields = ('ts', 'quantity', 'price', 'side')
                    if (any(old[k] != fact[k] for k in fields)
                            or any(old['row'].get(k) != row.get(k) for k in
                                   ('order_ref', 'start_position', 'exchange_order_id', 'exchange_trade_id'))
                            or (old['fee'] is not None and fee is not None and old['fee'] != fee)):
                        conflicts.add(key)
                    elif old['fee'] is None and fee is not None:
                        facts[key] = fact
                else:
                    facts[key] = fact
            facts = {key: fact for key, fact in facts.items() if key not in conflicts}
            orders = self.journal.cycle_orders(cid)
            steps = {step['order_ref']: step for step in self.journal.live_steps(cid)}
            witness = None
            for order in orders:
                ref = order['order_ref']
                if (order['venue'] != venue_key or order['state'] != 'TERMINAL'
                        or order['filled_units'] <= 0 or order['avg_px'] is None or ref.startswith('external:')
                        or ref not in steps or steps[ref]['quantity_step'] is None
                        or dec(steps[ref]['quantity_step']) != grid):
                    continue
                matching = [fact for fact in facts.values()
                            if str(fact['row'].get('order_ref') or '').lower() == ref.lower()
                            and fact['side'].value == order['side']]
                amount = order['filled_units'] * grid
                if (matching and sum((f['quantity'] for f in matching), Decimal(0)) == amount
                        and sum((f['quantity'] * f['price'] for f in matching), Decimal(0))
                        == amount * dec(order['avg_px'])):
                    witness = ref
                    break
            if witness is None:
                continue  # A legacy cycle is not bound by a matching symbol/time alone.
            account, market = scope['source_account'], scope['source_market']
            try:
                self.journal.bind_external_cycle(cid, binding_hash=scope['binding_hash'], venue=venue_key,
                    account=account, market=market, witness_order_ref=witness)
            except (ValueError, KeyError, PendingOrder):
                continue
            existing = {row['exchange_fill_key'] for row in self.journal.external_fills(cid)
                        if row['venue'] == venue_key}
            candidates = []
            for key, fact in facts.items():
                row = fact['row']
                if row.get('order_ref') not in (None, ''):
                    if venue_key == 'entropy' and self.journal.db.execute(
                            'SELECT 1 FROM orders WHERE venue=? AND LOWER(order_ref)=?',
                            (venue_key, str(row['order_ref']).lower())).fetchone() is not None:
                        continue  # Hex cloid casing cannot change a registered strategy identity.
                    try:
                        self.journal.order(str(row['order_ref']))
                        continue  # A registered strategy order retains its original identity.
                    except KeyError:
                        pass
                try:
                    start = dec(row['start_position'])
                    oid, tid = row['exchange_order_id'], row['exchange_trade_id']
                    if (not start.is_finite() or not start or row.get('is_reduction') is not True
                            or oid in (None, '') or tid in (None, '') or isinstance(tid, bool)
                            or fact['side'].sign * start >= 0 or fact['quantity'] > abs(start)):
                        continue
                    args = dict(binding_hash=scope['binding_hash'], venue=venue_key, account=account,
                        market=market, exchange_order_id=str(oid), exchange_fill_key=key, trade_id=str(tid),
                        side=fact['side'], units=fact['units'], price=fact['price'], fee_usd=fact['fee'],
                        ts=fact['ts'], start_position=start, quantity_step=grid, raw_evidence=row)
                    if key in existing:
                        self.journal.record_external_fill(cid, **args)  # Idempotent fee enrichment.
                    else:
                        candidates.append(args)
                except (ValueError, KeyError, TypeError, ArithmeticError, PendingOrder):
                    continue
            candidates.sort(key=lambda fact: fact['ts'])
            while candidates:
                first_ts = candidates[0]['ts']
                try:
                    inventory = self.journal.external_inventory_at(cid, venue_key, first_ts, grid) * grid
                except ValueError:
                    break
                matches = [fact for fact in candidates if fact['ts'] == first_ts
                           and fact['start_position'] == inventory]
                if len(matches) != 1:
                    break  # Same-time ambiguity and gaps remain unknown.
                fact = matches[0]
                try:
                    recovered += int(self.journal.record_external_fill(cid, **fact))
                except (ValueError, KeyError, PendingOrder):
                    break
                candidates.remove(fact)
        return recovered

    def _known_positions(self, cycle: dict) -> tuple[int, int]:
        units = {'entropy': 0, 'hedge': 0}
        for order in self.journal.cycle_orders(cycle['cycle_id']):
            units[order['venue']] += Side(order['side']).sign * order['filled_units']
        return units['entropy'], units['hedge']

    def _position_anchor(self, cycle: dict) -> dict | None:
        row = self.journal.db.execute('SELECT value FROM metadata WHERE key=?',
            ('position_anchor:' + cycle['cycle_id'],)).fetchone()
        return json.loads(row['value']) if row is not None else None

    def _save_position_anchor(self, cycle: dict, *, observed_ts: float,
                              entropy_units: int, hedge_units: int) -> None:
        orders = self.journal.cycle_orders(cycle['cycle_id'])
        anchor = dict(entropy_units=entropy_units, hedge_units=hedge_units,
            observed_ts=observed_ts,
            watermark={row['order_ref']: int(row['filled_units']) for row in orders},
            unresolved={row['order_ref']: row['venue'] for row in orders
                        if row['state'] != 'TERMINAL'})
        with self.journal.transaction():
            self.journal.db.execute('''INSERT INTO metadata(key,value) VALUES (?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value''',
                ('position_anchor:' + cycle['cycle_id'],
                 json.dumps(anchor, sort_keys=True, allow_nan=False)))

    def _ensure_position_anchor(self, cycle: dict) -> None:
        # Legacy holdings may already contain an authoritative external change.
        # Preserve that inventory before any new IOC or ledger replay occurs.
        if (self._position_anchor(cycle) is None
                and cycle.get('position_evidence_authoritative')
                and isinstance(cycle.get('last_authoritative_ts'), (int, float))
                and math.isfinite(cycle['last_authoritative_ts'])):
            self._save_position_anchor(cycle, observed_ts=cycle['last_authoritative_ts'],
                entropy_units=int(cycle['entropy_units']), hedge_units=int(cycle['hedge_units']))

    def _execution_positions(self, cycle: dict) -> tuple[dict[str, int], set[str]]:
        """Actual inventory plus cumulative changes after its account snapshot."""
        anchor = self._position_anchor(cycle)
        if anchor is None:
            return dict(zip(('entropy', 'hedge'), self._known_positions(cycle))), set()
        units = {key: int(anchor[key + '_units']) for key in ('entropy', 'hedge')}
        unproven = set()
        external = {row['order_ref']: row for row in self.journal.external_fills(cycle['cycle_id'])}
        for order in self.journal.cycle_orders(cycle['cycle_id']):
            ref, key = order['order_ref'], order['venue']
            delta = int(order['filled_units']) - int(anchor['watermark'].get(ref, 0))
            if ref in anchor['unresolved']:
                # The snapshot can already contain the still-unknown fill.
                # A late receipt needs a new account witness before reusing it.
                if delta or order['state'] == 'TERMINAL':
                    unproven.add(key)
                continue
            fact = external.get(ref)
            if fact is not None and float(fact['ts']) <= anchor['observed_ts']:
                continue  # Historical evidence explains inventory; it does not move it again.
            units[key] += Side(order['side']).sign * delta
        if unproven:
            self._position_audit_requested.set()
        return units, unproven

    async def _audit_positions(self) -> bool:
        """Background/startup only. Never publish a snapshot overtaken by an order."""
        if any(order['state'] == 'SUBMITTING' for order in self.journal.pending()):
            self._position_audit_requested.set()
            return False  # The dispatch owner still owns this in-flight intent.
        cycle = self.journal.active_live_cycle()
        identity = (cycle['cycle_id'], self._order_watermark(cycle['cycle_id'])) if cycle else None
        events = self._basis_events()
        feed = getattr(self.venues.hedge, 'orders_feed', None)
        receipt = getattr(getattr(feed, 'account_snapshot', None), 'received_ts', None)
        evidence = await self._position_evidence(cycle, force_hedge_rest=True, publish=False)
        current = self.journal.active_live_cycle()
        after = (current['cycle_id'], self._order_watermark(current['cycle_id'])) if current else None
        if (identity != after or events != self._basis_events()
                or receipt != getattr(getattr(feed, 'account_snapshot', None), 'received_ts', None)
                or (current is not None and not self._position_covers_orders(current, evidence))):
            self._position_audit_requested.set()
            return False
        if current is not None and identity is not None:
            self._basis_exit_retry_after = 0.0
        self._last_position_evidence = evidence
        if current is not None:
            current = await self._sync_positions(current, evidence=evidence)
            self._route_external_residual(current, evidence)
            if (evidence.fully_authoritative and not self.journal.pending()
                    and self._known_positions(current) != (
                        evidence.entropy_units, evidence.hedge_units)):
                task = getattr(self, '_external_recovery_task', None)
                if task is None or task.done():
                    async def recover(cycle=current, snapshot=evidence):
                        try:
                            await self._recover_external_fills(cycle, snapshot)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.status.base['external_fill_recovery'] = 'evidence_unavailable'
                        finally:
                            self.changed.set()
                    self._external_recovery_task = asyncio.create_task(recover())
            if evidence.fully_authoritative and self._known_positions(current) == (
                    evidence.entropy_units, evidence.hedge_units) and not self.journal.pending():
                group = self._latest_close_group_id(current['cycle_id'])
                if group is not None:
                    try:
                        self.journal.record_authoritative_reconcile(current['cycle_id'],
                            observed_ts=evidence.observed_ts, mono_ns=time.monotonic_ns(),
                            clock_session_id=self.clock_session_id, execution_group_id=group)
                    except KeyError:
                        pass  # Optional analytics cannot block authoritative recovery.
        expected = self._known_positions(current) if current else (0, 0)
        self._basis_recovered = (evidence.fully_authoritative and events is not None
                                 and not self.journal.pending()
                                 and (evidence.entropy_units, evidence.hedge_units) == expected)
        self._basis_event_watermark = events if self._basis_recovered else None
        if not self._basis_recovered:
            self._invalidate_flat_lease()
        elif current is None:
            self._flat_lease = FlatLease(0, 0, evidence.observed_ts, math.inf, self._flat_generation)
        self.status.base['position_audit_seconds'] = self.settings.position_audit_seconds
        self.status.base['position_audit_ts'] = evidence.observed_ts
        self.status.base['position_state'] = 'recovered' if self._basis_recovered else 'reconciliation_required'
        self.changed.set()
        return self._basis_recovered

    def _release_completion(self) -> None:
        release = getattr(self.venues.hedge, 'release_completion', None)
        if self._completion_ticket is not None and release is not None:
            release(self._completion_ticket)
        self._completion_ticket = None

    def _enqueue_telemetry(self, callback: Callable[[], None]) -> None:
        if len(self._telemetry_queue) >= 256:
            self.telemetry_dropped += 1
        else:
            self._telemetry_queue.append(callback)

    def _queue_order_timing(self, event):
        event['runtime_telemetry_dropped_total'] = self.telemetry_dropped
        self._enqueue_telemetry(lambda: self.observer.publish(event))

    async def _close_order_observer(self, observer, *, timeout=2.0):
        incomplete = False
        try:
            await asyncio.wait_for(observer.close(), timeout=timeout)
            incomplete = bool(getattr(observer, 'last_error', None))
        except Exception:
            incomplete = True
            observer.abort()
        self.status.base.update(observer_flush_incomplete=incomplete,
                                observer_dropped_events=observer.dropped_events)
        latest = self.status.current_snapshot or {}
        extra = {key: value for key, value in latest.items()
                 if key not in {'current_state', 'reason', 'source_ts', 'status_writer'}}
        extra.update(observer_flush_incomplete=incomplete, observer_dropped_events=observer.dropped_events)
        self.status.write(latest.get('current_state', 'SHUTDOWN'), latest.get('reason'), **extra)

    def _flush_telemetry(self) -> None:
        callbacks, self._telemetry_queue = self._telemetry_queue, []
        for callback in callbacks:
            try:
                callback()
            except Exception:
                self.telemetry_errors += 1

    @staticmethod
    def _phase_kind(purpose: str) -> str:
        if purpose.startswith('close_pair_entropy'):
            return 'close_first'
        if purpose.startswith('close_pair_hedge'):
            return 'close_hedge'
        if purpose.startswith('open_first'):
            return 'open_first'
        if purpose.startswith('open_hedge'):
            return 'open_hedge'
        if purpose.startswith('close_first'):
            return 'close_first'
        if purpose.startswith('close_hedge'):
            return 'close_hedge'
        return 'recovery'

    def _latest_close_group_id(self, cycle_id: str) -> str | None:
        for step in reversed(self.journal.live_steps(cycle_id)):
            if (':close_first_' in step['step_key'] or ':close_pair_' in step['step_key']) and step['applied'] \
                    and (step['result_units'] or 0) > 0:
                return step['execution_group_id'] or step['step_key']
        return None

    async def initialize(self, *, apply_leverage: bool = True) -> list[asyncio.Task]:
        e, h = self.venues.entropy, self.venues.hedge
        for venue in (e, h):
            configure_timing = getattr(venue, 'configure_order_timing', None)
            if configure_timing is not None:
                try:
                    configure_timing(self._queue_order_timing, self.clock_session_id)
                except Exception:
                    self.telemetry_errors += 1
        await asyncio.gather(e.load_market(), h.load_market())
        if self.settings.strategy_mode == 'funding_hedge' and self.journal.active_live_cycle() is None:
            positions = await asyncio.gather(e.fetch_position(), h.fetch_position())
            if any(dec(q) != 0 for q in positions):
                raise RuntimeHalt('pre-existing positions have no V3 cycle evidence')
        e.init_signer()
        prepare = getattr(h, 'prepare_signer', None)
        if prepare is not None:
            await prepare()
        else:
            h.init_signer()
        if self.operator_socket_path is not None:
            self.restore_operator_parameters()
        for venue in (e, h):
            if getattr(venue, 'supports_submission_guard', False):
                venue.submission_guard = self._lighter_increase_guard
        if apply_leverage:
            applied = {}
            for key, venue in (('entropy', e), ('hedge', h)):
                leverage = getattr(venue.conf, 'leverage', None)
                if leverage is not None:
                    await venue.set_leverage(leverage)
                    applied[key] = leverage
            self.status.base['applied_leverage'] = applied
        tasks = e.start_tasks(self.stop, self.changed.set, live=True)
        tasks += h.start_tasks(self.stop, self.changed.set, live=True)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not self.stop.is_set():
            if e.book.ready and h.book.ready and e.ready_to_trade() and h.ready_to_trade():
                break
            await asyncio.sleep(.1)
        else:
            raise RuntimeHalt("books/account order streams did not become ready")
        if self.journal.active_live_cycle() is None and not self.journal.pending():
            await asyncio.gather(*(venue.warm_http() for venue in (e, h)
                if callable(getattr(venue, 'warm_http', None))), return_exceptions=True)
        self.instrument = instrument_from_venues(self.pair_id, e, h)
        if self.settings.strategy_mode == 'funding_hedge':
            # Submission matching uses the coarser COMMON lot. Ledger and
            # recovery must also represent Lighter's finer partial fills.
            self._funding_common_step = self.instrument.quantity_step
            native_step = min(Decimal(1).scaleb(-int(v.size_decimals)) for v in (e, h))
            self.instrument = replace(self.instrument, quantity_step=native_step)
            self.status.base['common_quantity_step'] = str(self._funding_common_step)
            self.status.base['accounting_quantity_step'] = str(native_step)
        self.brokers = {
            "entropy": VenueAdapterBroker(e, venue_key="entropy",
                                           venue_kind=e.kind,
                                           instrument=self.instrument),
            "hedge": VenueAdapterBroker(h, venue_key="hedge", venue_kind=h.kind,
                                         instrument=self.instrument),
        }
        self.executor = AuditedLiveExecutor(self.journal, self.brokers)
        self.executor.operator_entry_gate = self._operator_entry_gate
        self.executor.clock_session_id = self.clock_session_id
        self.journal.ensure_batch(
            self.authorization.batch_id,
            max_position_cycles=self.authorization.max_position_cycles,
            loss_budget_usd=self.authorization.loss_budget_usd)
        if self.settings.opening_experiment_id:
            self._ensure_opening_experiment()
        elif self.journal.db.execute('SELECT 1 FROM opening_experiments WHERE batch_id=?',
                                    (self.authorization.batch_id,)).fetchone():
            raise RuntimeHalt('experiment batch cannot be launched as ordinary basis')
        if self.settings.strategy_mode == 'basis':
            for row in self.journal.provisional_cycles(self.authorization.batch_id):
                self._estimate_basis_accounting(row['cycle_id'])
        batch = self.journal.batch_state(self.authorization.batch_id)
        self._starting_position_cycles = batch['position_cycles']
        self._publish_batch_state(batch)
        self.observer.publish({
            'kind': 'v3_batch_startup', 'batch_id': self.authorization.batch_id,
            'position_cycles': batch['position_cycles'],
            'loss_used_usd': str(batch['loss_used_usd']),
            'max_position_cycles': batch['max_position_cycles'],
            'loss_budget_usd': str(batch['loss_budget_usd']) if batch['loss_budget_usd'] is not None else None,
            'closed_for_entries': batch['closed_for_entries'],
            'close_reason': batch['close_reason'],
            'block_reason': batch['block_reason'],
        })
        if self.journal.active_live_cycle() is None and not self.journal.pending():
            await self._refresh_flat_lease()
        return tasks

    async def _http_keepalive_loop(self) -> None:
        warmers = [venue.warm_http for venue in (self.venues.entropy, self.venues.hedge)
                   if callable(getattr(venue, 'warm_http', None))]
        interval = min(float(getattr(self.cfg, 'http_keepalive_sec', 10)), 10.0)
        if not warmers or interval <= 0:
            return
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=interval)
                continue
            except asyncio.TimeoutError:
                pass
            if self.journal.active_live_cycle() is None and not self.journal.pending():
                await asyncio.gather(*(warm() for warm in warmers), return_exceptions=True)

    def _invalidate_flat_lease(self) -> None:
        self._flat_generation += 1
        self._flat_lease = None

    def _valid_flat_lease(self, now: float | None = None) -> FlatLease | None:
        if self.settings.strategy_mode == 'basis' and not self._basis_state_ready():
            return None
        lease = self._flat_lease
        current = time.time() if now is None else now
        if lease is None or lease.generation != self._flat_generation \
                or current > lease.valid_until or self.journal.pending() \
                or self.journal.active_live_cycle() is not None:
            return None
        return lease

    async def _position_evidence(self, cycle: dict | None = None, *,
                                 force_hedge_rest: bool = False,
                                 publish: bool = True) -> PositionEvidence:
        observed = time.time()
        streams = self._open_stream_watermark()
        results = await asyncio.gather(
            self.brokers['entropy'].position_snapshot(),
            self.brokers['hedge'].position_snapshot(force_rest=force_hedge_rest),
            return_exceptions=True)
        values: list[int | None] = []
        authoritative: list[bool] = []
        sources: list[str] = []
        generations: list[int | None] = []
        received: list[float | None] = []
        errors = []
        for venue_key, result in zip(('entropy', 'hedge'), results):
            if isinstance(result, BaseException):
                values.append(None)
                authoritative.append(False)
                sources.append('durable_cycle')
                generations.append(None)
                received.append(None)
                errors.append(f"{venue_key}={type(result).__name__}: {result}")
            else:
                values.append(int(result.units))
                authoritative.append(bool(result.authoritative))
                sources.append(str(result.source))
                generations.append(result.generation)
                received.append(result.received_ts)
        if cycle is not None:
            if values[0] is None:
                values[0] = int(cycle['entropy_units'])
            if values[1] is None:
                values[1] = int(cycle['hedge_units'])
        fully = all(authoritative)
        last_authoritative = observed if fully else (
            float(cycle['last_authoritative_ts'])
            if cycle is not None and cycle.get('last_authoritative_ts') is not None
            else (self._last_position_evidence.last_authoritative_ts
                  if self._last_position_evidence is not None else None))
        evidence = PositionEvidence(
            values[0], values[1],
            sources[0] if authoritative[0] else 'durable_cycle',
            sources[1] if authoritative[1] else 'durable_cycle',
            authoritative[0], authoritative[1], observed, last_authoritative,
            not fully, '; '.join(errors) or None,
            generations[0], generations[1],
            cycle['cycle_id'] if cycle is not None else None,
            received[0], received[1],
            streams if streams == self._open_stream_watermark() else None)
        if publish:
            self._last_position_evidence = evidence
        if publish and evidence.degraded:
            self._invalidate_flat_lease()
        return evidence

    async def _refresh_flat_lease(self) -> None:
        if self.settings.strategy_mode == 'basis':
            await self._audit_positions()
            return
        if self.journal.active_live_cycle() is not None or self.journal.pending():
            self._invalidate_flat_lease()
            return
        evidence = await self._position_evidence()
        if evidence.fully_authoritative and evidence.entropy_units == 0 \
                and evidence.hedge_units == 0:
            valid_until = evidence.observed_ts + self.settings.flat_position_lease_sec
            if evidence.hedge_source.startswith('lighter_account_rest:'):
                valid_until = min(valid_until, evidence.hedge_received_ts + self.settings.flat_position_lease_sec)
            self._flat_lease = FlatLease(
                0, 0, evidence.observed_ts, valid_until, self._flat_generation)
        else:
            self._invalidate_flat_lease()

    async def _flat_lease_loop(self) -> None:
        if self.settings.strategy_mode == 'basis':
            # Only this background owner performs account IO. Market ticks and
            # own fills never turn the low-frequency audit into a polling loop.
            next_audit = time.monotonic() + self.settings.position_audit_seconds
            while not self.stop.is_set():
                self._basis_state_ready()
                now = time.monotonic()
                if now >= self._position_audit_after and (
                        self._position_audit_requested.is_set() or now >= next_audit):
                    self._position_audit_requested.clear()
                    try:
                        for row in self.journal.pending():
                            if row['state'] == 'UNKNOWN':
                                intent = Intent(row['order_ref'], row['cycle_id'], row['venue'],
                                    Side(row['side']), row['requested_units'], dec(row['limit_px']),
                                    bool(row['reduce_only']))
                                await self.executor.resolve(intent, timeout=1.0)
                        good = await self._audit_positions()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        good = False
                        self._basis_recovered = False
                    self._position_audit_after = time.monotonic() + (2.0 if not good else 0.0)
                    next_audit = time.monotonic() + self.settings.position_audit_seconds
                    if not good:
                        self._position_audit_requested.set()
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=.25)
                except asyncio.TimeoutError:
                    pass
            return
        while not self.stop.is_set():
            try:
                await self._refresh_flat_lease()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._invalidate_flat_lease()
            try:
                await asyncio.wait_for(
                    self.stop.wait(), timeout=self.settings.flat_position_probe_sec)
            except asyncio.TimeoutError:
                pass

    async def _account_equity(self) -> Decimal | None:
        values = []
        for venue in (self.venues.entropy, self.venues.hedge):
            fetch = getattr(venue, 'fetch_equity', None)
            if fetch is None:
                return None
            try:
                result = await fetch()
            except Exception:
                return None
            if result is None:
                return None
            values.append(dec(result[0] if isinstance(result, tuple) else result))
        return sum(values, Decimal('0'))

    def _lighter_increase_guard(self, order_ref: str) -> str | None:
        # Recheck after a cold nonce read, immediately before the SDK POST.
        if self.stop.is_set():
            return 'runtime_stop'
        if self.settings.strategy_mode == 'basis':
            try:
                order = self.journal.order(order_ref)
            except KeyError:
                return 'basis_order_identity_unavailable'
            cycle = self.journal.active_live_cycle()
            self._basis_state_ready()
            opening = self.journal.opening_attempt(cycle['cycle_id']) if cycle is not None and self.settings.opening_experiment_id else None
            if opening is None and cycle is not None:
                opening = self._standard_opening_pair(cycle)
            if (opening is not None and opening['group_name'] == 'A'
                    and order['order_ref'] in {opening['first_ref'], opening['hedge_ref']}):
                return self._opening_pair_guard(cycle, order, opening)
            if (cycle is None or order['cycle_id'] != cycle['cycle_id']
                    or order['state'] != 'SUBMITTING' or order['filled_units']
                    or order['had_unknown'] or order['reduce_only']
                    or not self._basis_recovered or self._basis_events() != self._basis_event_watermark
                    or any(o['order_ref'] != order_ref for o in self.journal.pending())
                    or cycle['basis_exit_requested_ts'] is not None):
                return 'basis_position_recovery_required'
            if order['venue'] == 'entropy':
                if not self._operator_running():
                    return 'operator_entries_paused'
                if cycle['entropy_units'] or cycle['hedge_units'] or cycle['phase'] != 'OPEN_FIRST':
                    return 'basis_entry_exposure_changed'
                if order['requested_units'] != cycle['planned_units'] \
                        or Side(order['side']) is not Direction(cycle['direction']).entropy_side:
                    return 'basis_entry_identity_changed'
                batch = self.journal.batch_state(self.authorization.batch_id)
                if batch['closed_for_entries']:
                    return 'basis_batch_closed'
                try:
                    if plan_open(self._market(), self.instrument, self.settings,
                                 dec(cycle['reference_bps']), Direction(cycle['direction']),
                                 cycle['planned_units']) is None:
                        return 'basis_signal_no_longer_valid'
                except (RuntimeHalt, ValueError):
                    return 'basis_market_unavailable'
            else:
                if cycle['phase'] not in {'OPEN_HEDGE', 'RECOVERY'}:
                    return 'basis_hedge_phase_changed'
                if not self.venues.hedge.book.is_fresh(self.settings.max_book_age_seconds):
                    return 'basis_hedge_market_unavailable'
                delta = -cycle['entropy_units'] - cycle['hedge_units']
                if not delta or Side(order['side']).sign * delta <= 0 \
                        or order['requested_units'] > abs(delta):
                    return 'basis_hedge_exposure_changed'
            return None
        if self.settings.strategy_mode == 'funding_hedge':
            cycle = self.journal.active_live_cycle()
            if cycle is None:
                return 'no_active_funding_cycle'
            if self._funding_exit_signal(cycle['cycle_id']).is_set():
                return 'funding_exit_requested'
            pending = self.journal.pending()
            if len(pending) != 1 or pending[0]['order_ref'] != order_ref:
                return 'funding_risk_unavailable'
            order = self.journal.order(order_ref)
            if (order is None or order['cycle_id'] != cycle['cycle_id']
                    or order['venue'] != 'hedge' or order['state'] != 'SUBMITTING'
                    or order['filled_units'] != 0 or order['reduce_only']):
                return 'funding_risk_unavailable'
            identity = self._funding_risk_identity(cycle)
            # This unsent IOC was durably prepared after risk admission. Only
            # its own zero-fill watermark may be omitted from the old signature.
            prior = (*identity[:3], tuple(row for row in identity[3]
                                         if row[0] != order_ref))
            for key in ('entropy', 'hedge'):
                cached = self._funding_risk_cache.get(key)
                if (cached is None or cached[0] not in (identity, prior)
                        or time.monotonic() > cached[2]
                        or self._funding_risk_invalid(cached[1], cycle, key)):
                    return 'funding_risk_unavailable'
        return None

    async def _equity_loop(self) -> None:
        while not self.stop.is_set():
            diagnostics = getattr(self.venues.hedge, 'request_diagnostics', None)
            if diagnostics is not None:
                try:
                    self.status.base['lighter_requests'] = diagnostics()
                except Exception:
                    self.telemetry_errors += 1
            value = await self._account_equity()
            if value is not None:
                self._last_equity = value
                self._last_equity_poll = time.time()
            try:
                await asyncio.wait_for(self.stop.wait(), self.settings.equity_poll_seconds)
            except asyncio.TimeoutError:
                pass

    def _persist_reference(self, *, force: bool = False,
                           now: float | None = None) -> None:
        checkpoint = self.reference.checkpoint()
        if not checkpoint or checkpoint.get('ts') == self._last_reference_checkpoint_ts:
            return
        current = time.monotonic() if now is None else now
        if not force and current - self._last_reference_checkpoint_save < 1.0:
            return
        if self._reference_writer is not None:
            self._reference_writer.publish(
                'checkpoint', save_checkpoint, self.reference_checkpoint_path, dict(checkpoint))
        else:
            save_checkpoint(self.reference_checkpoint_path, checkpoint)
        self._last_reference_checkpoint_ts = checkpoint.get('ts')
        self._last_reference_checkpoint_save = current

    def _market(self) -> Market:
        assert self.instrument is not None
        return market_from_venues(self.venues, self.instrument,
                                  now=time.time(), settings=self.settings)

    def _quote_snapshot(self, market: Market, *, entropy_side: Side,
                        units: int, serialize: bool = True) -> dict[str, Any]:
        assert self.instrument is not None
        snapshot: dict[str, Any] = {
            'quantity_step': self.instrument.quantity_step,
            'units': units, 'entropy_side': entropy_side.value,
            'capture_wall_ts': max(market.entropy.received_at,
                                   market.hedge.received_at),
        }
        for key in ('entropy', 'hedge'):
            book = getattr(market, key)
            raw = getattr(self.venues, key).book
            revision = f'{key}:{raw.last_update_ts:.9f}'
            current = revision == book.revision
            # Only retain the immutable depth prefix necessary for any partial
            # q <= requested. Decimal/string/JSON work is after the handoff.
            side = entropy_side if key == 'entropy' else entropy_side.opposite
            levels = book.asks if side is Side.BUY else book.bids
            prefix, available = [], 0
            for px, qty in levels:
                prefix.append((px, qty))
                available += qty
                if available >= units:
                    break
            snapshot[key] = {
                'bids': tuple(prefix) if side is Side.SELL else (),
                'asks': tuple(prefix) if side is Side.BUY else (),
                'book_revision': book.revision,
                'received_wall_ts': raw.last_update_ts if current else None,
                'received_mono_ns': getattr(raw, 'last_update_mono_ns', None)
                if current else None,
                'connection_generation': getattr(raw, 'connection_generation', None)
                if current else None,
                'stream_alive_ts': raw.alive_ts
                if current else None,
                'last_book_update_ts': raw.last_update_ts if current else None,
            }
        return self._materialize_quote(snapshot) if serialize else snapshot

    @staticmethod
    def _materialize_quote(snapshot: dict | None) -> dict | None:
        if snapshot is None:
            return None
        def encode(value):
            if isinstance(value, Decimal):
                return str(value)
            if isinstance(value, dict):
                return {key: encode(item) for key, item in value.items()}
            if isinstance(value, (tuple, list)):
                return [encode(item) for item in value]
            return value
        result = encode(snapshot)
        try:
            side = Side(snapshot['entropy_side'])
            def cash(key, side):
                remaining, total = snapshot['units'], Decimal('0')
                for px, qty in snapshot[key]['asks' if side is Side.BUY else 'bids']:
                    take = min(remaining, qty)
                    total += px * take
                    remaining -= take
                    if not remaining:
                        return total
                raise ValueError('insufficient captured depth')
            result['basis_bps'] = str((cash('entropy', side) / cash('hedge', side.opposite) - 1) * BPS)
        except (ValueError, ZeroDivisionError):
            result['basis_bps'] = None
        return result

    async def _reference_loop(self) -> None:
        """Keep the existing one-second reference sampling alive during orders."""
        while not self.stop.is_set():
            now = time.time()
            try:
                market = self._market()
                self.reference.observe(now, market.mid_basis_bps, valid=True)
            except (RuntimeHalt, ValueError):
                self.reference.observe(now, dec(0), valid=False)
            await asyncio.sleep(.5)

    async def _resolve_pending(self) -> bool:
        assert self.executor is not None
        await asyncio.sleep(0)  # Let already-ready send owners publish their results.
        await self._drain_parallel()
        if not self.journal.pending():
            return True
        self.status.write("RESOLVING_ORDERS", "same_id_recovery",
                          pending_orders=len(self.journal.pending()))
        pending = [row for row in self.journal.pending()
                   if row['order_ref'] not in self._parallel_tasks]
        contexts = {}
        for row in pending:
            metadata = {}
            try:
                intent = Intent(row['order_ref'], row['cycle_id'], row['venue'], Side(row['side']),
                    row['requested_units'], dec(row['limit_px']), bool(row['reduce_only']))
                step = next((step for step in self.journal.live_steps(row['cycle_id'])
                             if step['order_ref'] == row['order_ref']), None)
                if step is not None:
                    metadata['step_key'] = step['step_key']
                    economics = self.journal.economics_step(step['step_key']) or {}
                    metadata.update({key: economics[key] for key in ('attempt', 'purpose') if key in economics})
                contexts[row['order_ref']] = {'_order_timing_context': metadata}
                order_timing_trace(self.brokers[row['venue']], intent, contexts[row['order_ref']])
            except Exception:
                self.telemetry_errors += 1
        for row in pending:
            intent = Intent(row['order_ref'], row['cycle_id'], row['venue'], Side(row['side']),
                row['requested_units'], dec(row['limit_px']), bool(row['reduce_only']))
            await self.executor.resolve(intent, timeout=1.0, timing=contexts.get(row['order_ref']))
        for row in pending:
            try:
                intent = Intent(row['order_ref'], row['cycle_id'], row['venue'], Side(row['side']),
                    row['requested_units'], dec(row['limit_px']), bool(row['reduce_only']))
                consume_order_timing(self.brokers[row['venue']], intent, contexts.get(row['order_ref']))
            except Exception:
                self.telemetry_errors += 1
        return not self.journal.pending()

    async def _apply_resolved_steps(self, cycle: dict) -> dict:
        for step in self.journal.live_steps(cycle['cycle_id']):
            if step['applied']:
                continue
            try:
                row = self.journal.order(step['order_ref'])
            except KeyError:
                # Registration precedes budget reservation and durable intent.
                # There is no submitted action to resolve or account here.
                continue
            if row['state'] != 'TERMINAL':
                continue
            result = Result(
                True, row['filled_units'],
                dec(row['avg_px']) if row['avg_px'] else None,
                dec(row['fee_usd']) if row['fee_usd'] else None,
                row['note'] or '')
            current = self.journal.active_live_cycle() or cycle
            e_units, h_units, evidence = await self._positions_after_terminal(
                current, venue_key=step['venue'], side=Side(step['side']),
                filled_units=result.cumulative_units)
            cash, final = self._cash_result(
                step['venue'], Side(step['side']), result, dec(row['limit_px']))
            current_phase = current['phase']
            if result.cumulative_units > 0 and ':close_first_' in step['step_key']:
                current_phase = 'CLOSE_HEDGE'
            elif result.cumulative_units > 0 and ':open_first:' in step['step_key']:
                current_phase = 'OPEN_HEDGE'
            if current['basis_exit_requested_ts'] is not None:
                current_phase = 'BASIS_EXIT'
            self.journal.apply_live_step(
                step['step_key'], result_units=result.cumulative_units,
                cash_delta_usd=cash, accounting_final=final,
                entropy_units=e_units, hedge_units=h_units,
                next_phase=current_phase, updated_ts=time.time(),
                position_evidence_authoritative=evidence.fully_authoritative,
                last_authoritative_ts=(evidence.observed_ts
                                       if evidence.fully_authoritative else None))
            consumed_ts, consumed_mono = time.time(), time.monotonic_ns()
            def recovered_telemetry(step_key=step['step_key'], outcome=result,
                                    record=dict(row), ts=consumed_ts, mono=consumed_mono):
                self._record_execution_result(step_key, outcome, result_ts=ts)
                self.journal.record_execution_measurement(
                    step_key, terminal_observed_ts=record.get('terminal_received_ts'),
                    terminal_observed_mono_ns=record.get('terminal_received_mono_ns'),
                    terminal_clock_session_id=record.get('terminal_clock_session_id'),
                    runtime_consumed_ts=ts, runtime_consumed_mono_ns=mono,
                    runtime_consumed_clock_session_id=self.clock_session_id)
                self.journal.record_execution_attempt_timing(
                    step_key, terminal_result_ts=record.get('terminal_received_ts'),
                    confirmation_source=record.get('terminal_source'),
                    result_status=record.get('terminal_status'))
            self._enqueue_telemetry(recovered_telemetry)
        return self.journal.active_live_cycle() or cycle

    def _limit(self, venue_key: str, side: Side, units: int,
               room_bps: Decimal, hard_cap_bps: Decimal) -> Decimal:
        assert self.instrument is not None
        raw = getattr(self.venues, venue_key).book
        if not raw.is_fresh(self.settings.max_book_age_seconds):
            raise RuntimeHalt(f'{venue_key} book is not fresh for reduce-only exit')
        book = Book(_levels(raw.sorted_bids(), self.instrument.quantity_step),
                    _levels(raw.sorted_asks(), self.instrument.quantity_step),
                    time.time(), f'{venue_key}:{raw.last_update_ts:.9f}')
        tick = (self.instrument.entropy_tick if venue_key == "entropy"
                else self.instrument.hedge_tick)
        _, marginal = book.quote(side, units, self.instrument.quantity_step)
        return price_limit(book.top(side), marginal, side, room_bps,
                           hard_cap_bps, tick)

    def _execution_benchmark(self, cycle: dict, *, venue_key: str, side: Side,
                             units: int, room_bps: Decimal,
                             hard_cap_bps: Decimal,
                             market: Market | None = None) -> dict[str, Any]:
        assert self.instrument is not None
        market = market or self._market()
        book = market.entropy if venue_key == 'entropy' else market.hedge
        tick = (self.instrument.entropy_tick if venue_key == 'entropy'
                else self.instrument.hedge_tick)
        _, marginal = book.quote(side, units, self.instrument.quantity_step)
        top = book.top(side)
        limit_px = price_limit(top, marginal, side, room_bps, hard_cap_bps, tick)
        entropy_side = side if venue_key == 'entropy' else side.opposite
        entropy_cash, _ = market.entropy.quote(
            entropy_side, units, self.instrument.quantity_step)
        hedge_cash, _ = market.hedge.quote(
            entropy_side.opposite, units, self.instrument.quantity_step)
        basis = (entropy_cash / hedge_cash - 1) * BPS
        reference = dec(cycle['reference_bps'])
        return {
            'market': market, 'top_px': top, 'marginal_px': marginal,
            'limit_px': limit_px, 'paired_basis_bps': basis,
            'reference_bps': reference,
            'dislocation_bps': (reference - basis if entropy_side is Side.BUY
                                else basis - reference),
            'book_revision_entropy': market.entropy.revision,
            'book_revision_hedge': market.hedge.revision,
        }

    @staticmethod
    def _decision_benchmark(decision: OpenDecision) -> dict[str, Any]:
        return {
            'market': decision.decision_market,
            'top_px': decision.entropy_top_px,
            'marginal_px': decision.entropy_marginal_px,
            'limit_px': decision.entropy_limit_px,
            'paired_basis_bps': decision.executable_basis_bps,
            'reference_bps': decision.reference_bps,
            'dislocation_bps': decision.distance_bps,
            'book_revision_entropy': decision.entropy_book_revision,
            'book_revision_hedge': decision.hedge_book_revision,
        }

    def _record_execution_result(self, step_key: str, result: Result,
                                 *, result_ts: float,
                                 result_market: Market | None = None) -> None:
        try:
            benchmark = self.journal.economics_step(step_key)
            cost = None
            result_basis = None
            result_dislocation = None
            if benchmark is not None and result.avg_px is not None \
                    and result.cumulative_units:
                cost = execution_cost_bps(
                    Side(benchmark['side']), result.avg_px, dec(benchmark['top_px']))
                if result_market is not None:
                    cycle = self.journal.db.execute(
                        'SELECT * FROM live_cycles WHERE cycle_id=?',
                        (benchmark['cycle_id'],)).fetchone()
                    entropy_side = (Side(benchmark['side']) if benchmark['venue'] == 'entropy'
                                    else Side(benchmark['side']).opposite)
                    entropy_cash, _ = result_market.entropy.quote(
                        entropy_side, result.cumulative_units,
                        self.instrument.quantity_step)
                    hedge_cash, _ = result_market.hedge.quote(
                        entropy_side.opposite, result.cumulative_units,
                        self.instrument.quantity_step)
                    result_basis = (entropy_cash / hedge_cash - 1) * BPS
                    reference = dec(cycle['reference_bps'])
                    result_dislocation = (reference - result_basis
                                          if entropy_side is Side.BUY
                                          else result_basis - reference)
            self.journal.record_execution_result(
                step_key, result_ts=result_ts,
                result_units=result.cumulative_units,
                provisional_avg_px=result.avg_px,
                actual_fee_usd=result.fee_usd, execution_cost_bps=cost,
                result_paired_executable_basis_bps=result_basis,
                result_directional_dislocation_bps=result_dislocation)
            if benchmark is not None and result.cumulative_units > 0 \
                    and result.avg_px is not None \
                    and benchmark['purpose'].startswith('close_'):
                fill_ts = float(
                    benchmark['exchange_fill_ts']
                    or benchmark['terminal_result_ts'] or result_ts)
                paired_at_fill = None
                if benchmark['venue'] == 'entropy' and result_market is not None:
                    hedge_cash, _ = result_market.hedge.quote(
                        Side(benchmark['side']).opposite,
                        result.cumulative_units, self.instrument.quantity_step)
                    hedge_vwap = hedge_cash / self.instrument.quantity(
                        result.cumulative_units)
                    paired_at_fill = paired_basis_bps(result.avg_px, hedge_vwap)
                self.journal.record_close_fill(
                    benchmark['cycle_id'], venue=benchmark['venue'],
                    fill_ts=fill_ts, fill_px=result.avg_px,
                    fill_units=result.cumulative_units,
                    paired_basis_at_entropy_fill=paired_at_fill)
        except Exception:
            # Once an order may exist, telemetry can never delay position closure.
            self.telemetry_errors += 1
            return

    def _capture_entry_economics(self, cycle_id: str) -> None:
        cycle = self.journal.active_live_cycle()
        if cycle is not None and cycle['cycle_id'] == cycle_id:
            self._confirm_funding_hedged(cycle)
        try:
            steps = self.journal.economics_steps(cycle_id)
            opening = [row for row in steps if row['purpose'] in {
                'open_first', 'open_hedge'} and row['result_units']]
            entropy = [row for row in opening if row['venue'] == 'entropy'
                       and row['provisional_avg_px']]
            hedge = [row for row in opening if row['venue'] == 'hedge'
                     and row['provisional_avg_px']]
            if not entropy or not hedge:
                return

            def aggregate(rows: list[dict]) -> Decimal:
                units = sum(row['result_units'] for row in rows)
                return sum((dec(row['actual_fill_vwap'] or row['provisional_avg_px'])
                            * row['result_units']
                            for row in rows), Decimal('0')) / units

            def weighted_cost(rows: list[dict]) -> Decimal | None:
                measured = [row for row in rows if row['execution_cost_bps'] is not None]
                units = sum(row['result_units'] for row in measured)
                if not units:
                    return None
                return sum((dec(row['execution_cost_bps']) * row['result_units']
                            for row in measured), Decimal('0')) / units

            first = min(entropy, key=lambda row: row['submit_ts'])
            attempt = self.journal.opening_attempt(cycle_id) if self.settings.opening_experiment_id else None
            parallel = (any(row['execution_path'] == 'parallel' for row in opening)
                        or attempt is not None and attempt['group_name'] == 'A')
            observed = [row for row in opening if row['result_ts'] is not None]
            if parallel and observed:
                first = min(observed, key=lambda row: row['result_ts'])
            exchange_fill_ts = first['actual_fill_ts']
            if parallel:
                exchange_fill_ts = (min(float(row['actual_fill_ts']) for row in opening)
                                    if all(row['actual_fill_ts'] is not None for row in opening)
                                    else None)
            first_fill_basis = (first['result_paired_executable_basis_bps']
                                or first['paired_executable_basis_bps'])
            entry_basis = paired_basis_bps(aggregate(entropy), aggregate(hedge))
            source = ('exchange_fills' if all(
                row['actual_fill_vwap'] is not None for row in entropy + hedge)
                else 'terminal_vwap')
            self.journal.set_live_entry_basis(
                cycle_id, basis_bps=entry_basis, source=source,
                updated_ts=time.time())
            self.journal.capture_entry_economics(
                cycle_id,
                actual_entry_basis_bps=entry_basis,
                first_fill_mark_basis_bps=None if parallel else dec(first_fill_basis),
                first_fill_observed_ts=(float(first['result_ts'])
                                        if first['result_ts'] is not None else None),
                first_fill_exchange_ts=(float(exchange_fill_ts)
                                        if exchange_fill_ts is not None else None),
                first_leg_cost_bps=weighted_cost(entropy),
                hedge_cost_bps=weighted_cost(hedge), updated_ts=time.time())
        except Exception:
            return

    def _live_entry_anchor(self, cycle: dict) -> tuple[Decimal, str] | None:
        if cycle.get('live_entry_basis_bps') is not None:
            return (dec(cycle['live_entry_basis_bps']),
                    cycle.get('live_entry_basis_source') or 'persisted_step_vwap')
        # Execution anchors are safety facts, not optional benchmark rows.
        totals = {'entropy': [0, Decimal('0')], 'hedge': [0, Decimal('0')]}
        opening_refs = {s['order_ref'] for s in self.journal.live_steps(cycle['cycle_id'])
                        if not s['reduce_only']}
        direction = Direction(cycle['direction'])
        for order in self.journal.cycle_orders(cycle['cycle_id']):
            if order['order_ref'] not in opening_refs or not order['filled_units']:
                continue
            expected_side = (direction.entropy_side if order['venue'] == 'entropy'
                             else direction.entropy_side.opposite)
            if order['state'] != 'TERMINAL' or order['avg_px'] is None \
                    or Side(order['side']) is not expected_side:
                return None
            total = totals[order['venue']]
            total[0] += order['filled_units']
            total[1] += dec(order['avg_px']) * order['filled_units']
        if all(v[0] and v[1] > 0 for v in totals.values()):
            e, h = totals['entropy'], totals['hedge']
            basis = ((e[1] / e[0]) / (h[1] / h[0]) - 1) * BPS
            self.journal.set_live_entry_basis(
                cycle['cycle_id'], basis_bps=basis,
                source='canonical_order_vwap', updated_ts=time.time())
            return basis, 'canonical_order_vwap'
        self._capture_entry_economics(cycle['cycle_id'])
        current = self.journal.active_live_cycle() or cycle
        if current.get('live_entry_basis_bps') is not None:
            return (dec(current['live_entry_basis_bps']),
                    current.get('live_entry_basis_source') or 'persisted_step_vwap')
        row = self.journal.db.execute('''SELECT actual_entry_basis_bps
            FROM cycle_economics WHERE cycle_id=?''', (cycle['cycle_id'],)).fetchone()
        if row and row['actual_entry_basis_bps'] is not None:
            basis = dec(row['actual_entry_basis_bps'])
            self.journal.set_live_entry_basis(
                cycle['cycle_id'], basis_bps=basis,
                source='persisted_step_vwap', updated_ts=time.time())
            return basis, 'persisted_step_vwap'
        return None

    def _update_mfe(self, cycle_id: str, reversion_bps: Decimal, *,
                    observed_ts: float, force: bool = False) -> None:
        cached = self._mfe_cache.get(cycle_id)
        if cached is None:
            row = self.journal.db.execute('''SELECT max_favorable_reversion_bps,
                max_favorable_reversion_ts,max_reversion_flush_ts
                FROM cycle_economics WHERE cycle_id=?''', (cycle_id,)).fetchone()
            persisted = (dec(row['max_favorable_reversion_bps'])
                         if row and row['max_favorable_reversion_bps'] is not None
                         else None)
            cached = (persisted, float(row['max_favorable_reversion_ts'] or observed_ts)) \
                if persisted is not None else (reversion_bps, observed_ts)
            self._mfe_last_flush[cycle_id] = float(
                row['max_reversion_flush_ts'] or 0) if row else 0
        if reversion_bps > cached[0]:
            cached = (reversion_bps, observed_ts)
        self._mfe_cache[cycle_id] = cached
        row = self.journal.db.execute('''SELECT max_favorable_reversion_bps
            FROM cycle_economics WHERE cycle_id=?''', (cycle_id,)).fetchone()
        persisted = (dec(row['max_favorable_reversion_bps'])
                     if row and row['max_favorable_reversion_bps'] is not None
                     else None)
        last = self._mfe_last_flush.get(cycle_id, 0)
        if force or persisted is None or cached[0] - persisted >= Decimal('.05') \
                or observed_ts - last >= 1.0:
            self.journal.update_max_reversion(
                cycle_id, cached[0], observed_ts=cached[1], force=True)
            self._mfe_last_flush[cycle_id] = observed_ts

    def _cash_result(self, venue_key: str, side: Side, result: Result,
                     limit_px: Decimal) -> tuple[Decimal, bool]:
        assert self.instrument is not None
        if result.cumulative_units == 0:
            return Decimal('0'), True
        px = result.avg_px if result.avg_px is not None else limit_px
        quantity = self.instrument.quantity(result.cumulative_units)
        notional = quantity * px
        fee_bps = (self.instrument.entropy_fee_bps if venue_key == "entropy"
                   else self.instrument.hedge_fee_bps)
        fee = result.fee_usd if result.fee_usd is not None else notional * fee_bps / BPS
        cash = (notional if side is Side.SELL else -notional) - fee
        final = result.avg_px is not None and result.fee_usd is not None
        return cash, final

    def _can_fast_close(self, cycle: dict, *, purpose: str, attempt: int,
                        venue_key: str, side: Side, units: int,
                        result: Result, existing_order: bool) -> bool:
        context = self._close_context
        receipt = result.receipt
        if (not self.settings.exit_fast_path_enabled or context is None
                or existing_order or purpose != self._fast_close_purpose
                or attempt != 0 or venue_key != 'entropy'
                or not result.terminal or result.cumulative_units <= 0
                or receipt is None or not receipt.fill_identity_complete
                or not receipt.fill_quantity_verified
                or receipt.confirmation_source != 'exchange_http'
                or receipt.order_ref != self._fast_close_order_ref
                or receipt.venue != 'entropy' or receipt.side is not side
                or receipt.requested_units != units
                or receipt.cumulative_filled_units != result.cumulative_units
                or self.journal.pending()):
            return False
        if (cycle['cycle_id'] != context.cycle_id
                or cycle['phase'] != 'CLOSE_FIRST'
                or cycle['entropy_units'] != context.entropy_units
                or cycle['hedge_units'] != context.hedge_units
                or cycle['updated_ts'] != context.cycle_updated_ts
                or not 0 <= time.time() - context.observed_ts <= 1.5):
            return False
        if context.hedge_generation is not None:
            feed = getattr(self.venues.hedge, 'orders_feed', None)
            if feed is None or getattr(feed, 'connection_generation', None) != context.hedge_generation:
                return False
        return True

    async def _step(self, cycle: dict, **kwargs) -> Result | None:
        token = None
        if self.settings.strategy_mode == 'basis':
            key = kwargs['venue_key']
            if time.monotonic() < self._send_retry_after.get(key, 0.0):
                return Result(False, 0, note='resource_wait', deferred=True,
                              retry_after=self._send_retry_after[key] - time.monotonic())
            venue = getattr(self.venues, key)
            ref = OrderRefFactory.create(venue.kind, deployment=self.deployment,
                cycle_id=cycle['cycle_id'], leg=f"{kwargs['purpose']}:{kwargs['attempt']}")
            try:
                self.journal.order(ref)
                existing = True
            except KeyError:
                existing = False
            if not existing:
                admission = Intent(ref, cycle['cycle_id'], key, kwargs['side'], kwargs['units'],
                    getattr(self.instrument, f'{key}_tick'), kwargs['reduce_only'])
                ticket = self._completion_ticket if key == 'hedge' else None
                if ticket is not None and (ticket.remaining == 0 or ticket.owner is not None):
                    self._completion_ticket = ticket = None
                try:
                    token = await self.brokers[key].prepare_dispatch(admission,
                        completion=kwargs['completion'],
                        completion_ticket=ticket)
                except RequestDeferred as exc:
                    self._send_retry_after[key] = time.monotonic() + exc.retry_after
                    return Result(False, 0, note=exc.reason, deferred=True, retry_after=exc.retry_after)
                # Ownership may have waited for a previous SDK body. Price only
                # after that wait; never reuse a candidate through it.
                if token is not None and not kwargs['reduce_only']:
                    try:
                        market = self._market()
                        if kwargs['purpose'] == 'open_first' and plan_open(
                                market, self.instrument, self.settings, dec(cycle['reference_bps']),
                                Direction(cycle['direction']), cycle['planned_units']) is None:
                            token.release()
                            return None
                        kwargs['open_decision'] = None
                        kwargs['decision_market'] = market
                    except (RuntimeHalt, ValueError):
                        token.release()
                        return None
        try:
            return await self._step_inner(cycle, _dispatch_token=token, **kwargs)
        finally:
            if token is not None:
                token.release()

    async def _step_inner(self, cycle: dict, *, purpose: str, attempt: int,
                    venue_key: str, side: Side, units: int, reduce_only: bool,
                    room_bps: Decimal, hard_cap_bps: Decimal,
                    next_phase: str, completion: bool,
                    open_decision: OpenDecision | None = None,
                    decision_market: Market | None = None,
                    request_reserved: bool = False,
                    parent_step_key: str | None = None,
                    _dispatch_token=None,
                    _prepared_dispatch: dict | None = None,
                    _prepared_benchmark: dict | None = None,
                    _prepared_decision_ts: float | None = None,
                    _prepared_decision_mono_ns: int | None = None,
                    _parallel_dispatch: bool = False) -> Result | None:
        assert self.executor is not None and self.instrument is not None
        step_key = f"{cycle['cycle_id']}:{purpose}:{attempt}"
        venue = self.venues.entropy if venue_key == "entropy" else self.venues.hedge
        ref = OrderRefFactory.create(
            venue.kind, deployment=self.deployment,
            cycle_id=cycle['cycle_id'], leg=f"{purpose}:{attempt}")
        existing_economics = self.journal.economics_step(step_key)
        try:
            row = self.journal.order(ref)
        except KeyError:
            row = None
        prepared_send = _prepared_dispatch is not None
        if prepared_send and (row is None or row['state'] != 'SUBMITTING'
                or row['had_unknown'] or _prepared_dispatch['step_key'] != step_key):
            raise RuntimeHalt('prepared opening dispatch identity unavailable')
        if (row is None and not reduce_only
                and self.settings.strategy_mode == 'funding_hedge'):
            drain = getattr(venue, 'wait_for_submission', None)
            if drain is not None:
                # Finish prior nonce bookkeeping before the final risk gate.
                # Lighter's send_taker then has no preceding submission to await.
                await drain()
        benchmark = None
        benchmark_start = time.time()
        decision_mono_ns = time.monotonic_ns()
        if prepared_send:
            benchmark = _prepared_benchmark
            if benchmark is None or benchmark['limit_px'] != dec(row['limit_px']):
                raise RuntimeHalt('prepared opening decision differs from durable limit')
            limit_px = dec(row['limit_px'])
            benchmark_start = _prepared_decision_ts
            decision_mono_ns = _prepared_decision_mono_ns
        elif row is not None:
            limit_px = dec(row['limit_px'])
        elif open_decision is not None:
            if purpose != 'open_first' or attempt != 0 or venue_key != 'entropy':
                raise ValueError('frozen open decision is only valid for first IOC')
            benchmark = self._decision_benchmark(open_decision)
            limit_px = open_decision.entropy_limit_px
        else:
            try:
                benchmark = self._execution_benchmark(
                    cycle, venue_key=venue_key, side=side, units=units,
                    room_bps=room_bps, hard_cap_bps=hard_cap_bps,
                    market=decision_market)
                limit_px = benchmark['limit_px']
            except Exception:
                if purpose == 'open_first' and not cycle['entropy_units'] \
                        and not cycle['hedge_units']:
                    raise
                limit_px = self._limit(
                    venue_key, side, units, room_bps, hard_cap_bps)
        benchmark_end = time.time()
        if open_decision is not None:
            benchmark_start = open_decision.decision_ts
            decision_mono_ns = open_decision.decision_mono_ns
        step_arguments = dict(
            step_key=step_key, cycle_id=cycle['cycle_id'], order_ref=ref,
            venue=venue_key, side=side.value, requested_units=units,
            reduce_only=reduce_only,
            execution_group_id=(parent_step_key or step_key
                                if parent_step_key or purpose == 'open_first'
                                or purpose.startswith('close_first_')
                                else None),
            parent_step_key=parent_step_key,
            quantity_step=self.instrument.quantity_step)
        if prepared_send:
            step_arguments['execution_group_id'] = _prepared_dispatch['execution_group_id']
        basis_dispatch = self.settings.strategy_mode == 'basis' and row is None
        step = {'applied': False} if basis_dispatch else self.journal.ensure_live_step(**step_arguments)
        intent = Intent(ref, cycle['cycle_id'], venue_key, side, units,
                        limit_px, reduce_only)
        if not basis_dispatch and purpose.startswith('close_first_') and self._exit_trigger is not None:
            self.journal.bind_step_trigger(step_key, self._exit_trigger)
            step = self.journal.live_step(step_key)
        send_timing: dict[str, object] | None = None
        collect_attempt_timing = True
        decision_quote = None
        if benchmark is not None:
            decision_quote = self._quote_snapshot(
                benchmark['market'],
                entropy_side=(side if venue_key == 'entropy' else side.opposite),
                units=units, serialize=False)
        submit_quote = None
        submit_quote_ts = None
        def capture_submit_quote() -> None:
            nonlocal submit_quote, submit_quote_ts
            try:
                submit_market = self._market()
                submit_quote_ts = max(submit_market.entropy.received_at,
                                      submit_market.hedge.received_at)
                submit_quote = self._quote_snapshot(
                    submit_market,
                    entropy_side=(side if venue_key == 'entropy' else side.opposite),
                    units=units, serialize=False)
            except Exception:
                pass
        if row is None or prepared_send:
            if purpose == 'open_first' and not reduce_only:
                if not self._operator_running():
                    return None
                # A background reconciliation can exhaust budget between zero-fill IOCs.
                # This cycle is already admitted; can_open also counts its pending slot.
                batch = self.journal.batch_state(self.authorization.batch_id)
                budget = batch['loss_budget_usd']
                if batch['closed_for_entries'] or (
                        budget is not None and batch['loss_used_usd'] >= budget):
                    if not basis_dispatch:
                        self.journal.mark_step_abort(step_key, 'BATCH_RISK_GATE')
                    return None
            if self.settings.strategy_mode == 'funding_hedge' and not reduce_only:
                state = self.journal.funding_state(cycle['cycle_id'])
                if state and state['exit_reason']:
                    self.journal.mark_step_abort(step_key, 'FUNDING_EXIT_REQUESTED')
                    return None
                current = self.journal.active_live_cycle()
                if (current is None or current['cycle_id'] != cycle['cycle_id']
                        or any(current[f'{key}_units'] != cycle[f'{key}_units']
                               for key in ('entropy', 'hedge'))
                        or any(self._funding_cached_risk(key, current) is None
                               for key in ('entropy', 'hedge'))):
                    self.journal.mark_step_abort(step_key, 'FUNDING_RISK_UNAVAILABLE')
                    return None
            limit = (self.settings.entropy_orders_per_min if venue_key == 'entropy'
                     else self.settings.hedge_orders_per_min)
            reserve = min(2, max(0, limit - 1))
            if not basis_dispatch and not prepared_send and not request_reserved and not self.journal.reserve_request(
                    venue_key, now=time.time(), limit=limit,
                    reserve_for_completion=reserve, completion=completion,
                    request_kind=self._phase_kind(purpose),
                    cycle_id=cycle['cycle_id']):
                self.journal.mark_step_abort(step_key, 'BUDGET_DENIED')
                return None
            self._invalidate_flat_lease()
            send_timing = {'_order_timing_context': dict(step_key=step_key,
                attempt=attempt, purpose=purpose)} if collect_attempt_timing else None
            def persist_dispatch():
                nonlocal step
                if purpose == 'open_first' and not reduce_only and not self._operator_running():
                    return None
                current = self.journal.active_live_cycle()
                if not reduce_only and (not self._basis_state_ready() or current is None
                        or current['cycle_id'] != cycle['cycle_id']
                        or any(current[f'{key}_units'] != cycle[f'{key}_units']
                               for key in ('entropy', 'hedge'))
                        or current['basis_exit_requested_ts'] is not None):
                    return None
                # Only the Entropy entry throttle remains. Completion uses the
                # physical HTTP budget, without another six-orders limit.
                request = (dict(now=time.time(), limit=limit,
                    reserve_for_completion=0, completion=False,
                    request_kind='open_first') if purpose == 'open_first' else None)
                step = self.journal.prepare_dispatch(intent, step=step_arguments, request=request)
                if step is not None and purpose.startswith('close_first_') and self._exit_trigger is not None:
                    self.journal.bind_step_trigger(step_key, self._exit_trigger)
                return step
            result = await self.executor.send(
                intent, timing=send_timing,
                resolve_timeout=1.0 if purpose == 'open_first' else None,
                before_submit=capture_submit_quote,
                persist_dispatch=((lambda: _prepared_dispatch) if prepared_send
                                  else persist_dispatch if basis_dispatch else None),
                dispatch_token=_dispatch_token,
                completion_ticket=self._completion_ticket if venue_key == 'hedge' else None,
                completion=completion, on_progress=self._order_progress)
        else:
            intent = Intent(
                row['order_ref'], row['cycle_id'], row['venue'], Side(row['side']),
                row['requested_units'], dec(row['limit_px']), bool(row['reduce_only']))
            limit_px = intent.limit_px
            send_timing = {'_order_timing_context': dict(step_key=step_key,
                attempt=attempt, purpose=purpose)} if collect_attempt_timing else None
            result = await self.executor.resolve(
                intent, timeout=1.0, timing=send_timing)
        async def consume_result():
            nonlocal result
            if result.deferred:
                consume_order_timing(self.brokers[venue_key], intent, send_timing)
                self._send_retry_after[venue_key] = time.monotonic() + result.retry_after
                self._enqueue_telemetry(lambda: self.observer.publish({
                    'kind': 'v3_dispatch_deferred', 'cycle_id': cycle['cycle_id'],
                    'venue': venue_key, 'reason': result.note, 'retry_after': result.retry_after}))
                return result
            if not _parallel_dispatch and not result.terminal and not self.stop.is_set():
                # One bounded same-ID retry keeps the common fast path cheap.  Any
                # continuing uncertainty returns to the outer loop, where account
                # positions are reconciled and exposure is covered first.
                if purpose != 'open_first' or not (send_timing or {}).get(
                        '_fast_resolve_performed'):
                    result = await self.executor.resolve(
                        intent, timeout=1.0,
                        timing=send_timing if collect_attempt_timing else None)
            result_ts = time.time()
            result_mono_ns = time.monotonic_ns()
            consume_order_timing(self.brokers[venue_key], intent, send_timing,
                {'wall_ts': result_ts, 'mono_ns': result_mono_ns})
            step = self.journal.live_step(step_key)
            current = self.journal.active_live_cycle() if result.terminal and not step['applied'] else None
            if result.terminal and not step['applied'] and current is None:
                raise RuntimeHalt("terminal result has no active cycle")
            fast = (current is not None and self._can_fast_close(
                current, purpose=purpose, attempt=attempt,
                venue_key=venue_key, side=side, units=units,
                result=result, existing_order=row is not None))
            defer_open = ((purpose == 'open_first' or purpose.startswith('close_first_')) and result.terminal
                          and result.cumulative_units > 0 and not step['applied'])
            result_market = None
            if result.terminal and result.cumulative_units > 0:
                try:
                    result_market = self._market()
                except Exception:
                    pass
            fill_mark_quote = None
            if result_market is not None:
                fill_mark_quote = self._quote_snapshot(
                    result_market,
                    entropy_side=(side if venue_key == 'entropy' else side.opposite),
                    units=result.cumulative_units, serialize=False)
            def record_telemetry() -> None:
                nonlocal decision_quote, submit_quote, fill_mark_quote
                decision_quote = self._materialize_quote(decision_quote)
                submit_quote = self._materialize_quote(submit_quote)
                fill_mark_quote = self._materialize_quote(fill_mark_quote)
                if benchmark is not None:
                    try:
                        self.journal.record_execution_benchmark(
                            step_key=step_key, cycle_id=cycle['cycle_id'], purpose=purpose,
                            venue=venue_key, attempt=attempt, order_ref=ref, side=side.value,
                            requested_units=units, room_bps=room_bps,
                            hard_cap_bps=hard_cap_bps,
                            submit_ts=float((send_timing or {}).get(
                                'submit_start_ts', time.time())),
                            top_px=benchmark['top_px'], marginal_px=benchmark['marginal_px'],
                            limit_px=limit_px,
                            paired_executable_basis_bps=benchmark['paired_basis_bps'],
                            reference_bps=benchmark['reference_bps'],
                            directional_dislocation_bps=benchmark['dislocation_bps'],
                            book_revision_entropy=benchmark['book_revision_entropy'],
                            book_revision_hedge=benchmark['book_revision_hedge'],
                            open_timing=(
                                {**self._open_timing.get(cycle['cycle_id'], {}),
                                 'benchmark_start_ts': benchmark_start,
                                 'benchmark_end_ts': benchmark_end}
                                if purpose == 'open_first' else None))
                    except Exception:
                        # Once submit was attempted, analytics cannot block recovery,
                        # hedging, or closure.
                        self.telemetry_errors += 1
                    finally:
                        if purpose == 'open_first' and attempt == 0:
                            self._open_timing.pop(cycle['cycle_id'], None)
                try:
                    original = self.journal.db.execute(
                        'SELECT signal_ts,signal_executable_basis_bps,exit_trigger_ts,'
                        'exit_trigger_basis_bps FROM cycle_economics WHERE cycle_id=?',
                        (cycle['cycle_id'],)).fetchone()
                    phase_kind = self._phase_kind(purpose)
                    is_close = phase_kind.startswith('close_')
                    group_trigger = (self.journal.live_step(parent_step_key)
                                     if parent_step_key else step)
                    missing = []
                    for name, value in (
                            ('decision_quote', decision_quote),
                            ('submit_quote', submit_quote),
                            ('fill_mark_quote', fill_mark_quote if result.cumulative_units else True),
                            ('exchange_fill_ts', (send_timing or {}).get('exchange_fill_ts')
                             if result.cumulative_units else True)):
                        if value is None:
                            missing.append(name)
                    if row is not None and not prepared_send:
                        missing.append('recovered_original_decision')
                    path = ('parallel' if prepared_send and (self._standard_opening_pair(cycle)
                            or self.journal.opening_attempt(cycle['cycle_id'])['group_name'] == 'A')
                            else 'standard' if prepared_send
                            else 'recovered' if row is not None else
                            'fast' if fast or (purpose.startswith('close_hedge') and
                                               parent_step_key == self._fast_close_step_key and
                                               parent_step_key is not None) else
                            'fallback' if is_close else 'standard')
                    self.journal.record_execution_measurement(
                        step_key, telemetry_version=2, phase_kind=phase_kind,
                        clock_session_id=self.clock_session_id,
                        quantity_step=self.instrument.quantity_step,
                        original_signal_ts=(group_trigger.get('trigger_ts') if is_close
                                            else original['signal_ts'] if original else None),
                        original_signal_basis_bps=(
                            group_trigger.get('trigger_basis_bps') if is_close else
                            original['signal_executable_basis_bps'] if original else None),
                        attempt_decision_ts=benchmark_start if benchmark else None,
                        attempt_decision_mono_ns=decision_mono_ns if benchmark else None,
                        attempt_decision_basis_bps=(
                            benchmark['paired_basis_bps'] if benchmark else None),
                        decision_units=units if benchmark else None,
                        decision_quote_json=decision_quote,
                        submit_quote_ts=submit_quote_ts,
                        submit_quote_json=submit_quote,
                        submit_basis_bps=(submit_quote or {}).get('basis_bps'),
                        submit_start_mono_ns=(send_timing or {}).get('submit_start_mono_ns'),
                        http_post_start_mono_ns=(send_timing or {}).get('http_post_start_mono_ns'),
                        terminal_observed_ts=((send_timing or {}).get('terminal_result_ts', result_ts)
                                              if result.terminal else None),
                        terminal_observed_mono_ns=((send_timing or {}).get('terminal_result_mono_ns')
                                                  if result.terminal else None),
                        terminal_clock_session_id=self.clock_session_id if result.terminal else None,
                        runtime_consumed_ts=result_ts,
                        runtime_consumed_mono_ns=result_mono_ns,
                        runtime_consumed_clock_session_id=self.clock_session_id,
                        exchange_first_fill_ts=(send_timing or {}).get('exchange_fill_ts'),
                        exchange_last_fill_ts=(send_timing or {}).get('exchange_last_fill_ts'),
                        fill_mark_quote_json=fill_mark_quote,
                        fill_mark_time_source=('runtime_consumed_local'
                                               if fill_mark_quote else None),
                        execution_path=path,
                        measurement_quality='complete' if not missing else 'partial',
                        missing_fields_json=missing)
                except Exception:
                    self.telemetry_errors += 1
                if purpose == 'open_first':
                    try:
                        self.journal.record_submit_result_timing(
                            cycle['cycle_id'],
                            submit_start_ts=(send_timing or {}).get('submit_start_ts'),
                            submit_result_ts=(send_timing or {}).get(
                                'submit_result_ts', time.time()),
                            intent_prepare_start_ts=(send_timing or {}).get(
                                'intent_prepare_start_ts'),
                            intent_prepare_end_ts=(send_timing or {}).get(
                                'intent_prepare_end_ts'))
                    except Exception:
                        self.telemetry_errors += 1
                if send_timing:
                    try:
                        allowed = {
                            'submit_start_ts', 'http_post_start',
                            'http_headers_received', 'http_body_received',
                            'http_result_ts', 'terminal_result_ts',
                            'confirmation_source', 'result_status',
                            'resolve_start_ts', 'resolve_end_ts', 'exchange_fill_ts',
                        }
                        attempt_timing = {
                            key: value for key, value in send_timing.items()
                            if key in allowed
                        }
                        if attempt_timing:
                            self.journal.record_execution_attempt_timing(
                                step_key, **attempt_timing)
                    except Exception:
                        self.telemetry_errors += 1
                if purpose.startswith('close_'):
                    try:
                        economics_step = self.journal.economics_step(step_key)
                        submit_ts = ((send_timing or {}).get('submit_start_ts')
                                     or (economics_step or {}).get('submit_start_ts')
                                     or (economics_step or {}).get('submit_ts'))
                        if submit_ts is not None:
                            self.journal.record_close_submit(
                                cycle['cycle_id'], venue=venue_key,
                                submit_ts=float(submit_ts))
                    except Exception:
                        self.telemetry_errors += 1
                self._record_execution_result(
                    step_key, result, result_ts=result_ts,
                    result_market=result_market)
            if not fast and not defer_open:
                if self._in_handoff:
                    self._enqueue_telemetry(record_telemetry)
                else:
                    record_telemetry()
            if not result.terminal:
                return result
            if not step['applied']:
                # Position evidence may already have covered an UNKNOWN order.
                # Applying final accounting must never add the fill a second time.
                if fast:
                    e_units = int(current['entropy_units']) + side.sign * result.cumulative_units
                    h_units = int(current['hedge_units'])
                    evidence = None
                else:
                    e_units, h_units, evidence = await self._positions_after_terminal(
                        current, venue_key=venue_key, side=side,
                        filled_units=result.cumulative_units)
                cash, final = self._cash_result(venue_key, side, result, limit_px)
                self.journal.apply_live_step(
                    step_key, result_units=result.cumulative_units,
                    cash_delta_usd=cash, accounting_final=final,
                    entropy_units=e_units, hedge_units=h_units,
                    next_phase=('BASIS_EXIT' if current['basis_exit_requested_ts'] is not None else
                                'OPEN_FIRST' if self.settings.strategy_mode == 'basis'
                                and purpose == 'open_first' and result.cumulative_units == 0 else next_phase),
                    updated_ts=time.time(),
                    position_evidence_authoritative=(evidence.fully_authoritative
                                                     if evidence is not None else False),
                    last_authoritative_ts=(evidence.observed_ts
                                           if evidence is not None and evidence.fully_authoritative
                                           else None))
                if purpose == 'open_first' and row is None and evidence is not None:
                    updated = self.journal.active_live_cycle()
                    expected = (int(current['entropy_units']) + side.sign * result.cumulative_units,
                                int(current['hedge_units']))
                    if updated is not None and result.cumulative_units > 0 and (
                            evidence.entropy_units, evidence.hedge_units) == expected:
                        handoff = OpenHandoff(
                            updated['cycle_id'], updated['updated_ts'], evidence,
                            self._order_watermark(updated['cycle_id']),
                            self._open_step_watermark(updated['cycle_id']))
                        if self._open_handoff_valid(updated, handoff):
                            self._open_handoff = handoff
                if fast:
                    self._fast_close_step_key = step_key
                    self._deferred_fast_close_telemetry = record_telemetry
                elif defer_open:
                    if purpose == 'open_first':
                        self._deferred_open_telemetry = record_telemetry
                    else:
                        self._deferred_fast_close_telemetry = record_telemetry
            return result

        if _parallel_dispatch:
            self._parallel_completions.append((intent.order_ref, consume_result))
            self.changed.set()
            return result
        return await consume_result()

    async def _campaign(self, cycle: dict, *, purpose: str, venue_key: str,
                        side: Side, units: int, reduce_only: bool,
                        rooms: tuple[Decimal, ...], hard_cap: Decimal,
                        next_phase: str, completion: bool,
                        open_decision: OpenDecision | None = None,
                        first_request_reserved: bool = False,
                        break_on_positive_fill: bool = False,
                        parent_step_key: str | None = None) -> int:
        filled = 0
        existing = [s for s in self.journal.live_steps(cycle['cycle_id'])
                    if s['step_key'].split(':')[-2] == purpose]
        unapplied = [s for s in existing if not s['applied']]
        attempt = (int(unapplied[0]['step_key'].rsplit(':', 1)[1])
                   if unapplied else len(existing))
        while filled < units and attempt < len(rooms) and not self.stop.is_set():
            if purpose == 'open_first' and not reduce_only and not self._operator_running():
                break
            if self.settings.strategy_mode == 'funding_hedge' and not reduce_only:
                await self._check_funding_liquidation(self.journal.active_live_cycle() or cycle)
                state = self.journal.funding_state(cycle['cycle_id'])
                if state and state['exit_reason']:
                    break
                if purpose == 'open_first' and attempt == 0 and open_decision is not None:
                    # Waiting on the risk watcher invalidates borrowed books.
                    # Reprice the same common quantity, never revive stale admission.
                    try:
                        open_decision = self._funding_open_plan(
                            self._market(), units=units,
                            direction=Direction(cycle['direction']))
                    except RuntimeHalt:
                        break
                    if open_decision is None:
                        break
            decision_market = None
            if attempt > 0 and (purpose == 'open_first' or purpose.startswith('close_first_')):
                step_key = f"{cycle['cycle_id']}:{purpose}:{attempt}"
                # Existing intent retains its original identity and limit on recovery.
                submitted = {o['order_ref'] for o in self.journal.cycle_orders(cycle['cycle_id'])}
                if not any(s['step_key'] == step_key and s['order_ref'] in submitted
                           for s in existing):
                    if self.journal.pending():
                        break
                    current = self.journal.active_live_cycle() or cycle
                    try:
                        decision_market = self._market()
                        if purpose == 'open_first':
                            if current['entropy_units'] or current['hedge_units']:
                                for registered in existing:
                                    if registered['step_key'] == step_key:
                                        self.journal.mark_step_abort(step_key, 'EXPOSURE_CONTRADICTION')
                                self.journal.update_live_cycle(
                                    current['cycle_id'], phase='RECOVERY', updated_ts=time.time())
                                break
                            if self.settings.strategy_mode == 'funding_hedge':
                                allowed = self._funding_open_plan(
                                    decision_market, units=units,
                                    direction=Direction(current['direction'])) is not None
                            else:
                                allowed = plan_open(
                                    decision_market, self.instrument, self.settings,
                                    dec(current['reference_bps']),
                                    Direction(current['direction']), units) is not None
                        else:
                            paired = (current['entropy_units'] == -current['hedge_units']
                                      and abs(current['entropy_units']) == units)
                            if not paired:
                                break
                            anchor = self._live_entry_anchor(current)
                            allowed = self._basis_exit_decision(
                                current, decision_market, anchor).allowed
                            if not allowed:
                                self.journal.update_live_cycle(
                                    current['cycle_id'], phase='OPEN', updated_ts=time.time())
                    except (RuntimeHalt, ValueError):
                        break
                    if not allowed:
                        for registered in existing:
                            if registered['step_key'] == step_key:
                                self.journal.mark_step_abort(step_key, 'SIGNAL_NO_LONGER_VALID')
                        break
            result = await self._step(
                self.journal.active_live_cycle() or cycle,
                purpose=purpose, attempt=attempt, venue_key=venue_key,
                side=side, units=units - filled, reduce_only=reduce_only,
                room_bps=rooms[attempt], hard_cap_bps=hard_cap,
                next_phase=next_phase, completion=completion,
                open_decision=(open_decision if attempt == 0 else None),
                decision_market=decision_market,
                request_reserved=first_request_reserved and attempt == 0,
                parent_step_key=parent_step_key)
            if result is None:
                if purpose == 'open_first' or purpose.startswith('close_first_'):
                    if purpose.startswith('close_first_'):
                        self.journal.update_live_cycle(
                            cycle['cycle_id'], phase='OPEN', updated_ts=time.time())
                    break
                await asyncio.sleep(.5)
                continue
            if result.deferred:
                break  # No POST, no IOC attempt; outer wakeup reprices after resource release.
            if not result.terminal:
                break
            filled += result.cumulative_units
            attempt += 1
            if result.cumulative_units > 0 and break_on_positive_fill:
                break
            if result.cumulative_units == 0 and attempt < len(rooms) and purpose != 'open_first':
                await asyncio.sleep(.1)
        return filled

    async def _authoritative_positions(self) -> tuple[int, int]:
        evidence = await self._position_evidence()
        if not evidence.fully_authoritative:
            raise RuntimeHalt(
                f"authoritative position evidence unavailable: {evidence.error}")
        assert evidence.entropy_units is not None and evidence.hedge_units is not None
        return evidence.entropy_units, evidence.hedge_units

    async def _sync_positions(self, cycle: dict,
                              evidence: PositionEvidence | None = None) -> dict:
        if evidence is None and self.settings.strategy_mode == 'basis':
            self._ensure_position_anchor(cycle)
            return self.journal.active_live_cycle() or cycle
        evidence = evidence or await self._position_evidence(cycle)
        evidence = self._settlement_evidence(cycle, evidence)
        assert evidence.entropy_units is not None and evidence.hedge_units is not None
        e_units, h_units = evidence.entropy_units, evidence.hedge_units
        if (self.settings.strategy_mode == 'basis' and evidence.fully_authoritative
                and self._position_covers_orders(cycle, evidence)):
            self._save_position_anchor(cycle, observed_ts=evidence.observed_ts,
                entropy_units=e_units, hedge_units=h_units)
        if (e_units, h_units) != (cycle['entropy_units'], cycle['hedge_units']):
            self.journal.update_live_cycle(
                cycle['cycle_id'], entropy_units=e_units, hedge_units=h_units,
                accounting_final=False,
                position_evidence_authoritative=evidence.fully_authoritative,
                last_authoritative_ts=(evidence.observed_ts
                                       if evidence.fully_authoritative else None),
                updated_ts=time.time())
        elif (bool(cycle.get('position_evidence_authoritative')) != evidence.fully_authoritative
                or (evidence.fully_authoritative
                    and cycle.get('last_authoritative_ts') != evidence.observed_ts)):
            self.journal.update_live_cycle(
                cycle['cycle_id'],
                position_evidence_authoritative=evidence.fully_authoritative,
                last_authoritative_ts=(evidence.observed_ts
                                       if evidence.fully_authoritative else None),
                updated_ts=time.time())
        return self.journal.active_live_cycle() or cycle

    def _route_external_residual(self, cycle: dict, evidence: PositionEvidence | None) -> bool:
        if (self.settings.strategy_mode != 'basis' or cycle['phase'] not in {'OPEN', 'BASIS_EXIT'}
                or not cycle.get('position_evidence_authoritative')):
            return False
        e_units, h_units = int(cycle['entropy_units']), int(cycle['hedge_units'])
        if (e_units and e_units == -h_units
                and e_units * Direction(cycle['direction']).entropy_side.sign > 0):
            return False
        if not any((e_units, h_units)) and self._known_positions(cycle) == (0, 0):
            return False
        self._ensure_position_anchor(cycle)
        self.journal.mark_recovery_used(cycle['cycle_id'], updated_ts=time.time())
        self.journal.latch_basis_exit(cycle['cycle_id'], requested_ts=time.time(),
                                     reason='external_position_mismatch')
        self.journal.close_entries(cycle['batch_id'], 'POSITION_RISK', cycle_id=cycle['cycle_id'])
        self._basis_recovered = False
        self._position_audit_requested.set()
        self.status.write('EXITING' if e_units or h_units else 'RECONCILE_REQUIRED',
            'external_position_residual_recovery' if e_units or h_units
            else 'external_flat_accounting_reconciliation_required',
            cycle_id=cycle['cycle_id'], entropy_units=e_units, hedge_units=h_units,
            canonical_entropy_units=self._known_positions(cycle)[0],
            canonical_hedge_units=self._known_positions(cycle)[1])
        self.changed.set()
        return True

    async def _reduce_residual(self, cycle: dict, e_units: int, h_units: int,
                               *, purpose: str) -> bool:
        """Reduce only the authoritative unmatched leg, never build its pair."""
        net = e_units + h_units
        if net == 0:
            return False
        if net > 0:
            if e_units > 0:
                venue_key, side, units = 'entropy', Side.SELL, min(net, e_units)
            elif h_units > 0:
                venue_key, side, units = 'hedge', Side.SELL, min(net, h_units)
            else:
                raise RuntimeHalt("positive residual has no reducible long leg")
        else:
            need = -net
            if e_units < 0:
                venue_key, side, units = 'entropy', Side.BUY, min(need, -e_units)
            elif h_units < 0:
                venue_key, side, units = 'hedge', Side.BUY, min(need, -h_units)
            else:
                raise RuntimeHalt("negative residual has no reducible short leg")

        pending_orders = self.journal.pending()
        if self.settings.strategy_mode == 'basis' and any(
                row['venue'] == venue_key for row in pending_orders):
            self._position_audit_requested.set()
            return False
        # The journal deliberately forbids a new same-venue action while an
        # opening IOC is unresolved. Do not turn that expected wait into a
        # process-ending PendingOrder or silently bypass the safety contract.
        if any(row['venue'] == venue_key and not row['reduce_only']
               for row in pending_orders):
            self.status.write('RECONCILE_REQUIRED', 'same_venue_open_order_unresolved',
                              cycle_id=cycle['cycle_id'], venue=venue_key)
            return False
        pending_refs = {row['order_ref'] for row in pending_orders}
        if any(step['order_ref'] in pending_refs
               and f":{purpose}_" in step['step_key']
               for step in self.journal.live_steps(cycle['cycle_id'])):
            return False
        if purpose in {'repair', 'unknown_cover'} or cycle['phase'] == 'RECOVERY':
            self.journal.mark_recovery_used(
                cycle['cycle_id'], updated_ts=time.time())
        parent_step_key = None
        if purpose == 'close_hedge' and venue_key == 'hedge':
            first_steps = [step for step in self.journal.live_steps(cycle['cycle_id'])
                           if ':close_first_' in step['step_key']
                           and step['applied'] and (step['result_units'] or 0) > 0]
            if first_steps:
                parent_step_key = first_steps[-1]['step_key']
        rooms, cap = self.settings.hedge_rooms_bps, self.settings.hedge_hard_cap_bps
        if self.settings.strategy_mode == 'basis' and purpose == 'close_hedge':
            rooms, cap = (self.settings.first_leg_execution_policy('close')
                          if venue_key == 'entropy' else self.settings.hedge_execution_policy('close'))
        await self._campaign(
            cycle,
            purpose=f"{purpose}_{venue_key}_{len(self.journal.live_steps(cycle['cycle_id']))}",
            venue_key=venue_key, side=side, units=units, reduce_only=True,
            rooms=rooms, hard_cap=cap,
            next_phase=cycle['phase'], completion=True,
            parent_step_key=parent_step_key)
        return True

    def _prove_zero_fill_pending_reductions(
            self, cycle: dict, evidence: PositionEvidence) -> int:
        """Close superseded reduce-only UNKNOWNs using flat-position conservation.

        A later same-side reduce-only fill is the witness.  Once the venue is
        authoritatively flat and all known cumulative fills also reconstruct
        flat, an older same-side reduce-only IOC has no unaccounted fill and
        cannot fill in the future without reversing, which reduce-only forbids.
        """
        if not evidence.fully_authoritative:
            return 0
        actual = {
            'entropy': evidence.entropy_units,
            'hedge': evidence.hedge_units,
        }
        orders = self.journal.cycle_orders(cycle['cycle_id'])
        proved = []
        for venue_key in ('entropy', 'hedge'):
            pending = [row for row in orders
                       if row['venue'] == venue_key and row['state'] != 'TERMINAL']
            if not pending:
                continue
            sides = {row['side'] for row in pending}
            if actual[venue_key] != 0 or len(sides) != 1 \
                    or any(not row['reduce_only'] for row in pending):
                continue
            known_units = sum(
                Side(row['side']).sign * int(row['filled_units'])
                for row in orders if row['venue'] == venue_key)
            if known_units != actual[venue_key]:
                continue
            side = next(iter(sides))
            if any(not any(
                    witness['sequence'] > row['sequence']
                    and witness['venue'] == venue_key
                    and witness['state'] == 'TERMINAL'
                    and bool(witness['reduce_only'])
                    and witness['side'] == side
                    and int(witness['filled_units']) > 0
                    for witness in orders)
                    for row in pending):
                continue
            proved.extend(pending)
        for row in proved:
            self.journal.record(
                row['order_ref'],
                Result(True, int(row['filled_units']),
                       dec(row['avg_px']) if row['avg_px'] else None,
                       dec(row['fee_usd']) if row['fee_usd'] else None,
                       note='proved_zero_additional_fill_by_reduce_only_conservation'))
        return len(proved)

    async def _positions_after_terminal(self, cycle: dict, *, venue_key: str,
                                        side: Side, filled_units: int) \
            -> tuple[int, int, PositionEvidence]:
        if self.settings.strategy_mode == 'basis':
            units, unproven = self._execution_positions(cycle)
            for key in unproven:
                units[key] = int(cycle[key + '_units'])
            e_units, h_units = units['entropy'], units['hedge']
            old = self._last_position_evidence
            evidence = PositionEvidence(e_units, h_units, 'canonical_order_fills',
                'canonical_order_fills', False, False, time.time(),
                old.last_authoritative_ts if old else None, True, None,
                cycle_id=cycle['cycle_id'], stream_watermark=self._open_stream_watermark())
            self._last_position_evidence = evidence
            return e_units, h_units, evidence
        evidence = await self._position_evidence(
            cycle, force_hedge_rest=(venue_key == 'hedge'
                                     and self._fast_close_step_key is not None))
        evidence = self._settlement_evidence(cycle, evidence)
        assert evidence.entropy_units is not None and evidence.hedge_units is not None
        e_units, h_units = evidence.entropy_units, evidence.hedge_units
        delta = side.sign * filled_units
        if evidence.error in {'settlement_pending_known_fills', 'settlement_unavailable_known_fills',
                              'legacy_close_group_baseline_unavailable',
                              'unknown_close_group_snapshot_inconsistent'}:
            return e_units, h_units, evidence
        if venue_key == 'entropy' and not evidence.entropy_authoritative:
            e_units = int(cycle['entropy_units']) + delta
        elif venue_key == 'hedge' and not evidence.hedge_authoritative:
            h_units = int(cycle['hedge_units']) + delta
        return e_units, h_units, evidence

    def _close_group(self, cycle: dict) -> tuple[dict, list[dict]] | None:
        steps = self.journal.live_steps(cycle['cycle_id'])
        for first in reversed(steps):
            if ':close_first_' not in first['step_key']:
                continue
            try:
                order = dict(self.journal.order(first['order_ref']))
            except KeyError:
                continue
            if not order['filled_units'] and order['state'] == 'TERMINAL':
                continue
            members = []
            for step in steps:
                if step['step_key'] == first['step_key'] or step['parent_step_key'] == first['step_key']:
                    try:
                        members.append(dict(self.journal.order(step['order_ref'])))
                    except KeyError:
                        pass
            # UNKNOWN recovery can have reduced exposure before the original
            # same-ID result arrives. Those durable reductions consume this
            # group's remaining amount even without a normal hedge parent.
            cycle_orders = self.journal.cycle_orders(cycle['cycle_id'])
            sequence = next(o['sequence'] for o in cycle_orders
                            if o['order_ref'] == first['order_ref'])
            if first.get('baseline_entropy_units') is None:
                prior = [o for o in cycle_orders if o['sequence'] < sequence]
                if prior and all(o['state'] == 'TERMINAL' for o in prior):
                    # Pre-schema8 cycles started from the existing FLAT lease.
                    # Rebuild only from their canonical signed quantities; this
                    # is derived group state, never authoritative position data.
                    for venue in ('entropy', 'hedge'):
                        first[f'baseline_{venue}_units'] = sum(
                            Side(o['side']).sign * o['filled_units']
                            for o in prior if o['venue'] == venue)
            known_refs = {o['order_ref'] for o in members}
            members.extend(o for o in cycle_orders
                           if o['sequence'] >= sequence and o['reduce_only']
                           and o['order_ref'] not in known_refs)
            return first, members
        return None

    def _settlement_evidence(self, cycle: dict, evidence: PositionEvidence) -> PositionEvidence:
        """Known terminal close-group fills cannot be undone by a late snapshot.

        A mismatch is not proof of an external trade. Keep the group active and
        its derived balances non-authoritative until snapshots agree. UNKNOWN
        orders retain the existing position-driven recovery path.
        """
        group = self._close_group(cycle)
        if group is None:
            return evidence
        first, orders = group
        if first.get('baseline_entropy_units') is None:
            # No trustworthy historical starting state: do not let a snapshot
            # manufacture another normal hedge for a terminal legacy group.
            return replace(evidence, entropy_units=cycle['entropy_units'],
                           hedge_units=cycle['hedge_units'], entropy_authoritative=False,
                           hedge_authoritative=False, degraded=True,
                           error='legacy_close_group_baseline_unavailable')
        expected = {'entropy': first['baseline_entropy_units'],
                    'hedge': first['baseline_hedge_units']}
        for order in orders:
            expected[order['venue']] += Side(order['side']).sign * order['filled_units']
        pending = [o for o in orders if o['state'] != 'TERMINAL']
        if pending:
            # UNKNOWN is an interval, not zero. A fixed (already terminal) leg
            # must match; an unresolved reduce-only leg may move only toward
            # zero, never back past an already persisted covered position.
            valid = evidence.fully_authoritative
            derived = dict(expected)
            for venue in ('entropy', 'hedge'):
                base = first[f'baseline_{venue}_units']
                low = high = expected[venue]
                unresolved = [o for o in pending if o['venue'] == venue]
                for order in unresolved:
                    sign = Side(order['side']).sign
                    if not order['reduce_only'] or base * sign >= 0:
                        valid = False
                        continue
                    remaining = order['requested_units'] - order['filled_units']
                    if sign < 0:
                        low = max(0, low - remaining)
                    else:
                        high = min(0, high + remaining)
                if unresolved:
                    durable = int(cycle[f'{venue}_units'])
                    if base > 0 and 0 <= durable <= base:
                        high = min(high, durable)
                        derived[venue] = min(expected[venue], durable)
                    elif base < 0 and base <= durable <= 0:
                        low = max(low, durable)
                        derived[venue] = max(expected[venue], durable)
                actual = getattr(evidence, f'{venue}_units')
                valid = valid and actual is not None and low <= actual <= high
            if valid:
                return evidence
            guarded = replace(evidence,
                entropy_units=derived['entropy'], hedge_units=derived['hedge'],
                entropy_authoritative=False, hedge_authoritative=False,
                degraded=True, error='unknown_close_group_snapshot_inconsistent')
            self._last_position_evidence = guarded
            return guarded
        if evidence.fully_authoritative and (evidence.entropy_units, evidence.hedge_units) == (expected['entropy'], expected['hedge']):
            return evidence
        guarded = replace(evidence, entropy_units=expected['entropy'],
                          hedge_units=expected['hedge'], entropy_authoritative=False,
                          hedge_authoritative=False, entropy_source='known_group_fills',
                          hedge_source='known_group_fills', degraded=True,
                          error=('settlement_pending_known_fills' if evidence.fully_authoritative
                                 else 'settlement_unavailable_known_fills'))
        self._last_position_evidence = guarded
        return guarded

    async def _hedge_known_close_group(self, cycle: dict) -> bool:
        group = self._close_group(cycle)
        if group is None or self.journal.pending():
            return False
        evidence = self._last_position_evidence
        if self.settings.strategy_mode != 'basis' and evidence is not None and not evidence.fully_authoritative \
                and evidence.error != 'settlement_pending_known_fills':
            return False
        first, orders = group
        first_order = self.journal.order(first['order_ref'])
        if first_order['state'] != 'TERMINAL':
            return False
        done = sum(o['filled_units'] for o in orders if o['venue'] == 'hedge')
        first_units = sum(o['filled_units'] for o in orders if o['venue'] == 'entropy')
        remaining = first_units - done
        if remaining < 0:
            raise RuntimeHalt('close group hedge exceeds confirmed first-leg fill')
        if remaining:
            rooms, cap = (self.settings.hedge_execution_policy('close')
                          if self.settings.strategy_mode == 'basis'
                          else (self.settings.hedge_rooms_bps, self.settings.hedge_hard_cap_bps))
            await self._campaign(
                cycle, purpose=f"close_hedge_{len(self.journal.live_steps(cycle['cycle_id']))}",
                venue_key='hedge', side=Side(first_order['side']).opposite,
                units=remaining, reduce_only=True,
                rooms=rooms, hard_cap=cap,
                next_phase='CLOSE_HEDGE', completion=True,
                parent_step_key=first['step_key'])
        return True

    def _mark_to_flat(self, cycle: dict, market: Market) -> Decimal:
        assert self.instrument is not None
        value = dec(cycle['cash_net_usd'])
        if self.settings.strategy_mode == 'funding_hedge':
            state = self.journal.funding_state(cycle['cycle_id'])
            if state:
                value += state['actual_funding_usd']
        for venue_key, units, book, fee_bps in (
                ('entropy', cycle['entropy_units'], market.entropy,
                 self.instrument.entropy_fee_bps),
                ('hedge', cycle['hedge_units'], market.hedge,
                 self.instrument.hedge_fee_bps)):
            if not units:
                continue
            side = Side.SELL if units > 0 else Side.BUY
            cash, _ = book.quote(side, abs(units), self.instrument.quantity_step)
            fee = cash * fee_bps / BPS
            value += (cash if side is Side.SELL else -cash) - fee
        return value

    def _complete_basis_open(self, cycle: dict) -> None:
        assert self.instrument is not None
        current = self.journal.active_live_cycle()
        if (current is None or current['cycle_id'] != cycle['cycle_id']
                or current['basis_exit_requested_ts'] is not None
                or not current['entropy_units']
                or current['entropy_units'] != -current['hedge_units']):
            return
        if not self.journal.lock_basis_profit_basis(cycle['cycle_id'],
                quantity_step=self.instrument.quantity_step, locked_ts=time.time()):
            # A valid balanced position can be held even when its historical
            # notional cannot be proved. Normal exit remains explicitly blocked.
            self.journal.update_live_cycle(cycle['cycle_id'], phase='OPEN', updated_ts=time.time())

    def _basis_exit_decision(self, cycle: dict, market: Market,
                             anchor: tuple[Decimal, str] | None) -> ExitDecision:
        """Rebuild price cash from this cycle's actual cumulative fills only."""
        assert self.instrument is not None
        cash = Decimal('0')
        positions = {'entropy': 0, 'hedge': 0}
        complete = (cycle['entropy_units'] == -cycle['hedge_units']
                    and cycle['entropy_units'] * Direction(cycle['direction']).entropy_side.sign > 0)
        orders = self.journal.cycle_orders(cycle['cycle_id'])
        for order in orders:
            if order['state'] != 'TERMINAL':
                complete = False
                break
            units = order['filled_units']
            if not units:
                continue
            try:
                price = dec(order['avg_px']) if order['avg_px'] is not None else None
            except (ValueError, TypeError, ArithmeticError):
                price = None
            if price is None or price <= 0:
                complete = False
                break
            sign = Side(order['side']).sign
            positions[order['venue']] += sign * units
            cash -= sign * units * self.instrument.quantity_step * price
        complete = complete and all(
            positions[key] == cycle[f'{key}_units'] for key in positions)
        notional = None
        basis_error = None
        try:
            stored = self.journal.db.execute('SELECT basis_profit_basis_json FROM live_cycles WHERE cycle_id=?',
                                              (cycle['cycle_id'],)).fetchone()[0]
            if stored == self.journal.BASIS_PROFIT_PENDING:
                basis_error = 'hold_entry_notional_not_locked'
            elif stored is not None:
                basis = json.loads(stored)
                notional = dec(basis['entry_notional_usd'])
                locked_units = basis['matched_units']
                costs = basis['entry_inventory_cost_units']
                if (type(locked_units) is not int or locked_units <= 0
                        or notional <= 0 or dec(basis['quantity_step']) != self.instrument.quantity_step
                        or abs(cycle['entropy_units']) > locked_units
                        or any(dec(costs[key]) <= 0 for key in ('entropy', 'hedge'))
                        or notional != (dec(costs['entropy']) + dec(costs['hedge'])) * self.instrument.quantity_step / 2):
                    raise ValueError('invalid frozen matched notional')
                old = {row[0]: row for row in basis['canonical_orders']}
                if len(old) != len(basis['canonical_orders']) or not old:
                    raise ValueError('invalid frozen order evidence')
                current = {order['order_ref']: order for order in orders}
                if any(ref not in current for ref in old):
                    basis_error = 'hold_entry_notional_evidence_changed'
                for ref, order in current.items():
                    actual = [ref, order['filled_units'], order['avg_px'], order['side'],
                              order['venue'], bool(order['reduce_only'])]
                    if ref in old:
                        frozen = old[ref]
                        same_price = (not order['filled_units'] or
                            (order['avg_px'] is not None and frozen[2] is not None
                             and dec(order['avg_px']) == dec(frozen[2])))
                        step = self.journal.db.execute('SELECT quantity_step FROM live_steps WHERE order_ref=?',
                                                       (ref,)).fetchone()
                        same_grid = (not order['filled_units'] or
                            (step is not None and step['quantity_step'] is not None
                             and dec(step['quantity_step']) == self.instrument.quantity_step))
                        changed = (actual[:2] != frozen[:2] or actual[3:] != frozen[3:]
                                   or not same_price or not same_grid)
                    else:
                        expected_side = (Direction(cycle['direction']).entropy_side.opposite
                                         if order['venue'] == 'entropy'
                                         else Direction(cycle['direction']).entropy_side)
                        changed = not order['reduce_only'] or Side(order['side']) is not expected_side
                        if order['filled_units']:
                            step = self.journal.db.execute('SELECT quantity_step FROM live_steps WHERE order_ref=?',
                                                           (ref,)).fetchone()
                            changed = changed or step is None or step['quantity_step'] is None or dec(step['quantity_step']) != self.instrument.quantity_step
                    if changed:
                        basis_error = 'hold_entry_notional_evidence_changed'
                if basis_error:
                    notional = None
        except (ValueError, TypeError, KeyError, IndexError, ArithmeticError):
            notional = None
            basis_error = 'hold_invalid_entry_notional_evidence'
        decision = exit_decision(
            market, self.instrument, self.settings, Direction(cycle['direction']),
            abs(cycle['entropy_units']), anchor[0] if anchor else None,
            dec(cycle['cash_net_usd']),
            accrued_price_cash_usd=cash if complete else None,
            profit_entry_notional_usd=notional)
        return replace(decision, reason=basis_error) if basis_error and complete else decision

    async def _observe_active_risk(self, cycle: dict) -> None:
        active_net = (dec(cycle['cash_net_usd'])
                      if not cycle['entropy_units'] and not cycle['hedge_units']
                      else self._mark_to_flat(cycle, self._market()))
        limit = (self.settings.funding_round_loss_limit_usd
                 if self.settings.strategy_mode == 'funding_hedge'
                 else self.settings.single_cycle_review_loss_usd)
        if limit is not None and active_net <= -limit:
            self.journal.close_entries(
                self.authorization.batch_id, 'SINGLE_CYCLE_REVIEW_LOSS',
                value=active_net,
                threshold=-limit,
                cycle_id=cycle['cycle_id'])
            self.status.write('REVIEW_REQUIRED', 'single_cycle_loss_limit',
                              cycle_id=cycle['cycle_id'],
                              active_cycle_net_pnl_usd=str(active_net))
        self.journal.observe_open_risk(
            self.authorization.batch_id,
            active_cycle_net_pnl_usd=active_net,
            cycle_id=cycle['cycle_id'])

    async def _cover_unknown_exposure(self, cycle: dict) -> None:
        if self.settings.strategy_mode == 'basis':
            if (self._standard_opening_pair(cycle)
                    and cycle['basis_profit_basis_json'] == self.journal.BASIS_PROFIT_PENDING
                    and cycle['basis_exit_requested_ts'] is None):
                # A known partial position is not this IOC's final quantity.
                # Keep its identity in recovery until both original legs settle.
                self._position_audit_requested.set()
                return
            self._basis_recovered = False
            self._position_audit_requested.set()
            evidence = self._last_position_evidence
            if evidence is not None and evidence.cycle_id == cycle['cycle_id'] \
                    and evidence.fully_authoritative:
                await self._reduce_residual(cycle, cycle['entropy_units'],
                                            cycle['hedge_units'], purpose='unknown_cover')
            return
        await self._resolve_pending()
        evidence = await self._position_evidence(cycle)
        evidence = self._settlement_evidence(cycle, evidence)
        if not evidence.fully_authoritative:
            self.status.write(
                'RECONCILE_REQUIRED',
                evidence.error or 'authoritative position evidence unavailable',
                cycle_id=cycle['cycle_id'],
                pending_orders=len(self.journal.pending()))
            return
        cycle = await self._sync_positions(cycle, evidence=evidence)
        e_units = int(evidence.entropy_units)
        h_units = int(evidence.hedge_units)
        net = e_units + h_units
        if net == 0:
            if self.journal.pending() and self._prove_zero_fill_pending_reductions(
                    cycle, evidence):
                cycle = await self._apply_resolved_steps(
                    self.journal.active_live_cycle() or cycle)
            if e_units == 0 and h_units == 0 and not self.journal.pending():
                await self._finish_flat(cycle)
                return
            if e_units != 0:
                self.journal.update_live_cycle(
                    cycle['cycle_id'], phase='OPEN', updated_ts=time.time())
            self.status.write('POSITION_COVERED_UNANCHORED',
                              'order_accounting_pending',
                              cycle_id=cycle['cycle_id'],
                              pending_orders=len(self.journal.pending()))
            return
        await self._reduce_residual(
            cycle, e_units, h_units, purpose='unknown_cover')
        updated = await self._sync_positions(self.journal.active_live_cycle() or cycle)
        balanced = updated['entropy_units'] + updated['hedge_units'] == 0
        if balanced and not updated['entropy_units'] and self.journal.pending():
            final_evidence = await self._position_evidence(updated)
            if self._prove_zero_fill_pending_reductions(updated, final_evidence):
                updated = await self._apply_resolved_steps(
                    self.journal.active_live_cycle() or updated)
        if balanced and updated['entropy_units']:
            self.journal.update_live_cycle(
                updated['cycle_id'], phase='OPEN', updated_ts=time.time())
        elif balanced and not self.journal.pending():
            await self._finish_flat(updated)
            return
        state = 'POSITION_COVERED_UNANCHORED' if balanced else 'RECONCILE_REQUIRED'
        self.status.write(state, 'unknown_order_position_reconciled',
                          cycle_id=cycle['cycle_id'],
                          entropy_units=updated['entropy_units'],
                          hedge_units=updated['hedge_units'])

    async def _finish_flat(self, cycle: dict, *, zero_fill: bool = False,
                           evidence: PositionEvidence | None = None) -> None:
        if self.settings.strategy_mode == 'basis':
            cycle = self.journal.active_live_cycle() or cycle
            evidence = evidence or self._last_position_evidence
            orders = self.journal.cycle_orders(cycle['cycle_id'])
            submitted = {order['order_ref'] for order in orders}
            if any(not step['applied'] and step['order_ref'] in submitted
                   for step in self.journal.live_steps(cycle['cycle_id'])):
                self.changed.set()
                return  # Terminal quantities must also be accounted before DONE.
            if not orders and evidence is not None:
                # A proved-unsent candidate has no order to settle.
                evidence = replace(evidence, cycle_id=cycle['cycle_id'])
            if (evidence is None or not evidence.fully_authoritative
                    or evidence.cycle_id != cycle['cycle_id']
                    or not self._position_covers_orders(cycle, evidence)
                    or (evidence.entropy_units, evidence.hedge_units) != (0, 0)
                    or self.journal.pending() or self._known_positions(cycle) != (0, 0)):
                self._position_audit_requested.set()
                self.status.write('FLAT_PENDING_RECONCILIATION', 'background_flat_audit_required',
                                  cycle_id=cycle['cycle_id'])
                return
            self._release_completion()
            if (cycle['entropy_units'], cycle['hedge_units']) != (0, 0):
                self._position_audit_requested.set()
                return
        position = evidence
        if (position is None or position.cycle_id != cycle['cycle_id']
                or not position.fully_authoritative
                or not self._position_covers_orders(cycle, position)
                or (position.entropy_units, position.hedge_units) !=
                    (cycle['entropy_units'], cycle['hedge_units'])
                or self.journal.pending()):
            position = await self._position_evidence(cycle)
        position = self._settlement_evidence(cycle, position)
        if not position.fully_authoritative:
            self.status.write(
                'FLAT_PENDING_RECONCILIATION',
                position.error or 'authoritative position evidence unavailable',
                cycle_id=cycle['cycle_id'], entropy_units=cycle['entropy_units'],
                hedge_units=cycle['hedge_units'])
            return
        if not self._position_covers_orders(cycle, position):
            raise RuntimeHalt('position evidence predates the latest durable order result')
        if position.entropy_units != 0 or position.hedge_units != 0 \
                or self.journal.pending():
            raise RuntimeHalt("cannot finish cycle without authoritative FLAT")
        try:
            self.journal.record_authoritative_reconcile(
                cycle['cycle_id'], observed_ts=time.time(),
                mono_ns=time.monotonic_ns(),
                clock_session_id=self.clock_session_id,
                execution_group_id=self._latest_close_group_id(cycle['cycle_id']))
        except Exception:
            pass
        self.journal.update_live_cycle(
            cycle['cycle_id'], entropy_units=0, hedge_units=0,
            position_evidence_authoritative=True,
            last_authoritative_ts=position.observed_ts, updated_ts=time.time())
        cycle = self.journal.active_live_cycle() or cycle
        economics = self.journal.db.execute(
            'SELECT recovery_used FROM cycle_economics WHERE cycle_id=?',
            (cycle['cycle_id'],)).fetchone()
        repaired = bool(cycle.get('recovery_used') or (economics and economics['recovery_used'])
                        or any(':repair_' in s['step_key'] or ':unknown_cover_' in s['step_key']
                               for s in self.journal.live_steps(cycle['cycle_id'])))
        kind = 'zero_fill' if zero_fill else ('repair' if repaired else 'clean')
        pnl = dec(cycle['cash_net_usd'])
        funding_mode = self.settings.strategy_mode == 'funding_hedge'
        if funding_mode:
            state = self.journal.funding_state(cycle['cycle_id'])
            if state:
                pnl += state['actual_funding_usd']
            if not self.journal.fee_state(cycle['cycle_id'])['fees_complete']:
                pnl = None
        self.journal.finish_cycle(
            cycle['cycle_id'], kind=kind, net_pnl_usd=pnl,
            positions_verified_flat=True, all_orders_terminal=True,
            accounting_final=bool(cycle['accounting_final']) and not funding_mode,
            conservative_pnl_bound=not bool(cycle['accounting_final']) or funding_mode)
        self.journal.update_live_cycle(
            cycle['cycle_id'], phase='DONE', updated_ts=time.time())
        if self.settings.opening_experiment_id:
            self._finish_opening_slot(cycle['cycle_id'])
        if self.settings.strategy_mode == 'basis' and self._basis_state_ready():
            self._flat_lease = FlatLease(0, 0, position.observed_ts, math.inf, self._flat_generation)
        for order in self.journal.cycle_orders(cycle['cycle_id']):
            venue = getattr(self.venues, order['venue'])
            feed = getattr(venue, 'orders_feed', None)
            release = getattr(feed, 'release_evidence', None)
            if release is not None:
                release(order['order_ref'])
        if funding_mode:
            final = await self._reconcile_cycle_accounting(cycle['cycle_id'])
        else:
            self._estimate_basis_accounting(cycle['cycle_id'])
            final = None  # The background worker owns remote final accounting.
        if funding_mode:
            current_pnl = self.journal.db.execute(
                'SELECT pnl_usd FROM cycles WHERE cycle_id=?', (cycle['cycle_id'],)).fetchone()
            if current_pnl:
                pnl = dec(current_pnl['pnl_usd']) if current_pnl['pnl_usd'] is not None else None
        self.status.write("FLAT", "cycle_complete", cycle_id=cycle['cycle_id'],
                          result_kind=kind,
                          pnl_usd=str(final if final is not None else pnl) if pnl is not None else None,
                          accounting_final=final is not None,
                          **(self._funding_completion_fields(cycle['cycle_id'])
                             if funding_mode else {}))

    def _finalize_economics_accounting(
            self, cycle_id: str, orders: list[dict],
            matched_by_order: dict[str, list[dict]],
            funding_by_venue: dict[str, Decimal], pnl_usd: Decimal) -> None:
        if self._in_handoff:
            self._enqueue_telemetry(lambda: self._finalize_economics_accounting(
                cycle_id, orders, matched_by_order, funding_by_venue, pnl_usd))
            return
        assert self.instrument is not None
        try:
            cycle = dict(self.journal.db.execute(
                'SELECT * FROM live_cycles WHERE cycle_id=?', (cycle_id,)).fetchone())
            result = self.journal.db.execute(
                'SELECT result_kind FROM cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
            result_kind = result['result_kind']
            steps = {row['order_ref']: row for row in self.journal.live_steps(cycle_id)}
            records: list[dict] = []
            venue_fees = {'entropy': Decimal('0'), 'hedge': Decimal('0')}
            fee_state = self.journal.fee_state(cycle_id)
            for order in orders:
                fills = matched_by_order.get(order['order_ref'], [])
                evidence = fee_state['order_fees'][order['order_ref']]
                if not evidence['fee_complete']:
                    return
                fee = evidence['known_fee_usd']
                venue_fees[order['venue']] += fee
                if evidence['fee_policy_source'] is not None:
                    step = steps[order['order_ref']]
                    qty = order['filled_units'] * dec(step['quantity_step'])
                    if sum((dec(fill['qty']) for fill in fills), Decimal('0')) != qty:
                        # Price-only fallback; partial history cannot represent the full order.
                        # No synthetic exchange fill or timestamp is persisted.
                        fills = [{'qty': qty, 'price': dec(order['avg_px']), 'ts': None}]
                if fills:
                    vwap, _ = weighted_vwap(fills)
                    step = steps.get(order['order_ref'])
                    if step is not None:
                        timestamps = [float(fill['ts']) for fill in fills
                                      if fill.get('ts') is not None]
                        self.journal.finalize_execution_fill(
                            step['step_key'], fill_ts=min(timestamps) if timestamps else None,
                            fill_vwap=vwap, fee_usd=fee)
                for fill in fills:
                    records.append({**fill, 'venue': order['venue'],
                                    'reduce_only': bool(order['reduce_only'])})

            def leg(venue: str, reduce_only: bool) -> tuple[Decimal, Decimal] | None:
                selected = [row for row in records if row['venue'] == venue
                            and row['reduce_only'] is reduce_only]
                return weighted_vwap(selected) if selected else None

            entry_entropy, entry_hedge = leg('entropy', False), leg('hedge', False)
            exit_entropy, exit_hedge = leg('entropy', True), leg('hedge', True)
            complete = all((entry_entropy, entry_hedge, exit_entropy, exit_hedge))
            self._capture_entry_economics(cycle_id)
            economics = self.journal.economics_rows(
                batch_id=cycle['batch_id'])
            current = next(row for row in economics if row['cycle_id'] == cycle_id)
            values: dict[str, object] = {
                'actual_fees_usd': sum(venue_fees.values(), Decimal('0')),
                'entropy_fees_usd': venue_fees['entropy'],
                'hedge_fees_usd': venue_fees['hedge'],
                'actual_funding_usd': sum(funding_by_venue.values(), Decimal('0')),
                'entropy_funding_usd': funding_by_venue['entropy'],
                'hedge_funding_usd': funding_by_venue['hedge'],
                'finalized_pnl_usd': pnl_usd, 'result_kind': result_kind,
                'accounting_final': self.settings.strategy_mode != 'funding_hedge',
                'updated_ts': time.time(),
            }
            eligible = False
            if complete:
                assert entry_entropy and entry_hedge and exit_entropy and exit_hedge
                entry_basis = paired_basis_bps(entry_entropy[0], entry_hedge[0])
                exit_basis = paired_basis_bps(exit_entropy[0], exit_hedge[0])
                closing_steps = [row for row in self.journal.economics_steps(cycle_id)
                                 if row['purpose'].startswith('close_')
                                 and row['result_units']]
                first_entropy_step = next(
                    (row for row in closing_steps if row['venue'] == 'entropy'), None)
                first_hedge_step = next(
                    (row for row in closing_steps if row['venue'] == 'hedge'), None)
                if first_entropy_step and first_hedge_step:
                    entropy_wave_fills = matched_by_order.get(
                        first_entropy_step['order_ref'], [])
                    hedge_wave_fills = [
                        fill for row in closing_steps
                        if row['venue'] == 'hedge'
                        and row['purpose'] == first_hedge_step['purpose']
                        for fill in matched_by_order.get(row['order_ref'], [])
                    ]
                    if entropy_wave_fills and hedge_wave_fills:
                        entropy_wave = weighted_vwap(entropy_wave_fills)
                        hedge_wave = weighted_vwap(hedge_wave_fills)
                        entropy_fill_times = [
                            float(row['ts']) for row in entropy_wave_fills
                            if row.get('ts') is not None]
                        hedge_fill_times = [
                            float(row['ts']) for row in hedge_wave_fills
                            if row.get('ts') is not None]
                        self.journal.finalize_close_execution(
                            cycle_id,
                            entropy_fill_ts=(min(entropy_fill_times)
                                             if entropy_fill_times else None),
                            entropy_fill_px=entropy_wave[0],
                            entropy_fill_units=self.instrument.exact_units(
                                entropy_wave[1]),
                            lighter_fill_ts=(max(hedge_fill_times)
                                             if hedge_fill_times else None),
                            lighter_fill_px=hedge_wave[0],
                            lighter_fill_units=self.instrument.exact_units(
                                hedge_wave[1]),
                            actual_paired_exit_basis=paired_basis_bps(
                                entropy_wave[0], hedge_wave[0]),
                            updated_ts=time.time())
                reversion = actual_reversion_bps(
                    Direction(cycle['direction']), entry_basis, exit_basis)
                entry_notional = max(entry_entropy[0] * entry_entropy[1],
                                     entry_hedge[0] * entry_hedge[1])
                pnl_bps = pnl_usd / entry_notional * BPS
                fee_bps = values['actual_fees_usd'] / entry_notional * BPS
                funding_bps = values['actual_funding_usd'] / entry_notional * BPS
                values.update({
                    'actual_entry_basis_bps': entry_basis,
                    'actual_exit_basis_bps': exit_basis,
                    'actual_reversion_bps': reversion,
                    'actual_entry_notional_usd': entry_notional,
                    'actual_fees_bps': fee_bps,
                    'actual_funding_bps': funding_bps,
                    'finalized_pnl_bps': pnl_bps,
                    'break_even_reversion_bps': break_even_reversion_bps(
                        reversion, pnl_usd, entry_notional),
                })
                if current.get('live_entry_basis_bps') is not None:
                    values['anchor_error_bps'] = (
                        dec(current['live_entry_basis_bps']) - entry_basis)
                required = (
                    'first_leg_cost_bps', 'hedge_cost_bps',
                    'signal_to_first_fill_decay_bps',
                    'first_fill_to_hedged_decay_bps', 'total_entry_decay_bps',
                    'max_favorable_reversion_bps', 'exit_trigger_reason',
                    'exit_trigger_reversion_bps',
                    'exit_trigger_basis_bps', 'entropy_close_submit_ts',
                    'entropy_close_fill_ts', 'entropy_close_fill_px',
                    'entropy_close_fill_units', 'paired_basis_at_entropy_fill',
                    'lighter_close_submit_ts', 'lighter_close_fill_ts',
                    'lighter_close_fill_px', 'lighter_close_fill_units',
                    'actual_paired_exit_basis',
                    'trigger_to_entropy_fill_ms',
                    'entropy_fill_to_lighter_submit_ms',
                    'lighter_submit_to_fill_ms',
                    'trigger_to_entropy_fill_decay_bps',
                    'interleg_decay_bps',
                    'total_exit_execution_decay_bps',
                )
                opening_steps = [row for row in self.journal.economics_steps(cycle_id)
                                 if row['purpose'] in {'open_first', 'open_hedge'}
                                 and row['result_units']]
                authoritative_steps = bool(opening_steps) and all(
                    row['actual_fill_vwap'] is not None for row in opening_steps)
                first_observed = any(
                    row['purpose'] == 'open_first'
                    and row['result_paired_executable_basis_bps'] is not None
                    for row in opening_steps)
                balanced = (entry_entropy[1] == entry_hedge[1]
                            and exit_entropy[1] == exit_hedge[1])
                first_fill_lag = current.get('first_fill_observation_lag_ms')
                timing_eligible = (first_fill_lag is not None
                                   and dec(first_fill_lag) <= Decimal('1000'))
                eligible = (result_kind == 'clean' and balanced
                            and authoritative_steps and first_observed
                            and timing_eligible
                            and all(current.get(name) not in (None, '')
                                    for name in required))
            values['clean_summary_eligible'] = eligible
            values['telemetry_incomplete'] = not eligible
            self.journal.finalize_cycle_economics(cycle_id, **values)
            event = dict(self.journal.db.execute(
                'SELECT * FROM cycle_economics WHERE cycle_id=?',
                (cycle_id,)).fetchone())
            self.observer.publish({'kind': 'v3_cycle_economics', **event})
        except Exception:
            # Canonical accounting and risk closure always outrank analytics.
            return

    def _estimate_basis_accounting(self, cycle_id: str) -> bool:
        assert self.instrument is not None
        return self.journal.estimate_basis_accounting(cycle_id, fee_bps={
            'entropy': self.instrument.entropy_fee_bps,
            'hedge': self.instrument.hedge_fee_bps})

    def _apply_lighter_zero_fee_policy(self, cycle_id: str) -> bool:
        h = self.venues.hedge
        conf = h.conf
        creds = getattr(conf, 'lighter_creds', None)
        profile = getattr(h, 'profile', None)
        account = self.settings.lighter_zero_fee_account_index
        if (account is None or self.settings.strategy_mode != 'basis'
                or self.authorization.batch_id != self.settings.lighter_zero_fee_batch_id
                or h.kind != 'lighter' or conf.symbol != 'SNDK'
                or getattr(profile, 'name', None) != 'mainnet'
                or getattr(h, 'market_id', None) != 139
                or getattr(creds, 'account_index', None) != account
                or dec(h.fee_bps) != 0
                or getattr(h, 'fee_policy_order_type', None) != 'market_ioc_no_integrator'):
            return False
        return self.journal.authorize_lighter_zero_fee(cycle_id, policy={
            'batch_id': self.settings.lighter_zero_fee_batch_id, 'account_index': account,
            'profile': 'mainnet', 'symbol': 'SNDK', 'market_id': 139,
            'order_type': 'market_ioc_no_integrator', 'fee_usd': '0',
            'source': self.settings.lighter_zero_fee_policy_source})

    async def _reconcile_cycle_accounting(self, cycle_id: str) -> Decimal | None:
        cycle = self.journal.db.execute(
            'SELECT * FROM live_cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
        if cycle is None:
            return None
        funding_mode = self.settings.strategy_mode == 'funding_hedge'
        policy_zero = self._apply_lighter_zero_fee_policy(cycle_id)
        orders = [row for row in self.journal.cycle_orders(cycle_id) if row['filled_units'] > 0]
        by_ref = {row['order_ref']: row for row in orders}
        steps = {row['order_ref']: row for row in self.journal.live_steps(cycle_id)}
        quantities = {}
        for row in self.journal.economics_steps(cycle_id):
            if row.get('quantity_step') is not None:
                quantities[row['order_ref']] = dec(row['quantity_step'])
        for ref, step in steps.items():
            if step['quantity_step'] is not None:
                quantities[ref] = dec(step['quantity_step'])

        def refresh_cash() -> Decimal | None:
            # Only actual cumulative prices and the recorded unit grid define gross.
            # Legacy cash_net_usd already includes fees and is never a gross fallback.
            gross = Decimal('0')
            for order in self.journal.cycle_orders(cycle_id):
                if not order['filled_units']:
                    continue
                quantity_step = quantities.get(order['order_ref'])
                if quantity_step is None or quantity_step <= 0 or order['avg_px'] is None:
                    gross = None
                    break
                notional = order['filled_units'] * quantity_step * dec(order['avg_px'])
                gross += notional if order['side'] == 'sell' else -notional
            fee_state = self.journal.fee_state(cycle_id, use_zero_fee_policy=policy_zero)
            pnl = (self.journal.refresh_funding_pnl(cycle_id, trade_gross_cash_usd=gross)
                   if funding_mode else None)
            # Analytics mirrors complete venue totals; missing fees remain NULL.
            # Canonical evidence was already committed independently above.
            try:
                values = {f'{key}_fees_usd': fee_state['known_fees_by_venue'][key]
                          if fee_state['fees_complete_by_venue'][key] else None
                          for key in ('entropy', 'hedge')}
                values['actual_fees_usd'] = (fee_state['known_fees_usd']
                                             if fee_state['fees_complete'] else None)
                if funding_mode:
                    values.update(finalized_pnl_usd=pnl, accounting_final=False,
                                  finalized_pnl_bps=None, actual_fees_bps=None,
                                  actual_funding_bps=None, break_even_reversion_bps=None,
                                  clean_summary_eligible=False, telemetry_incomplete=True)
                    state = self.journal.funding_state(cycle_id)
                    recorded = {row['venue'] for row in self.journal.db.execute(
                        'SELECT DISTINCT venue FROM funding_settlements WHERE cycle_id=?', (cycle_id,))}
                    observed = {key: key in recorded or state['last_complete_poll_ts'] is not None
                                for key in ('entropy', 'hedge')}
                    values.update({f'{key}_funding_usd': state['funding_by_venue'][key]
                                   if observed[key] else None for key in observed})
                    values['actual_funding_usd'] = (state['actual_funding_usd']
                                                     if all(observed.values()) else None)
                self.journal.finalize_cycle_economics(cycle_id, **values, updated_ts=time.time())
            except Exception:
                pass  # Research rows never gate canonical cash or risk accounting.
            return gross

        if funding_mode:
            self.journal.ensure_funding_cycle(cycle_id)
        for key in ('entropy', 'hedge'):
            # Invalidate unproven legacy/partial totals even when a venue query fails.
            self.journal.record_fee_fills(cycle_id, key, [])
        refresh_cash()
        start = float(cycle['started_ts'])
        end = (cycle['last_authoritative_ts'] if not funding_mode and cycle['phase'] == 'DONE'
               else cycle['updated_ts'])
        if (not isinstance(end, (int, float)) or not math.isfinite(end)
                or not math.isfinite(start) or end < start):
            return None
        since, until = start - 2.0, float(end) + 2.0
        if not funding_mode:
            self._estimate_basis_accounting(cycle_id)

        async def reconcile_venue(key: str, venue) -> None:
            if key == 'hedge' and policy_zero:
                return  # No trades query is needed solely to confirm an authorized zero fee.
            try:
                fills = await venue.fetch_fee_fills(since, until)
                selected = []
                for fill in fills:
                    ref = str(fill.get('order_ref', ''))
                    if ref not in by_ref and fill.get('exchange_fill_key'):
                        ref = (self.journal.external_fill_order_ref(
                            key, str(fill['exchange_fill_key'])) or ref)
                    order = by_ref.get(ref)
                    quantity_step = quantities.get(ref)
                    if (order is None or order['venue'] != key or not fill.get('exchange_fill_key')
                            or quantity_step is None or quantity_step <= 0):
                        continue
                    units = dec(fill['qty']) / quantity_step
                    if units <= 0 or units != units.to_integral_value():
                        raise ValueError('fee fill quantity is outside its recorded grid')
                    selected.append({**fill, 'order_ref': ref, 'units': int(units)})
                self.journal.record_fee_fills(cycle_id, key, selected)
                refresh_cash()  # Commit this venue before a slow/failing peer returns.
                if not funding_mode:
                    self._estimate_basis_accounting(cycle_id)
            except Exception:
                return  # Never persist exchange errors containing private account data.

        async def reconcile_funding() -> None:
            await self._poll_funding_settlements(dict(cycle))
            refresh_cash()  # Book late funding independently of fee API tails.

        funding_by_venue: dict[str, Decimal] = {}

        async def reconcile_basis_funding(key: str, venue) -> None:
            try:
                amount = dec(await venue.fetch_funding_payments(start, end))
            except Exception:
                return  # Missing funding is never implied zero by a fee policy.
            funding_by_venue[key] = amount
            try:
                self.journal.finalize_cycle_economics(
                    cycle_id, **{f'{key}_funding_usd': amount}, updated_ts=time.time())
            except Exception:
                pass

        reads = [reconcile_venue('entropy', self.venues.entropy),
                 reconcile_venue('hedge', self.venues.hedge)]
        if funding_mode:
            reads.append(reconcile_funding())
        else:
            reads.extend(reconcile_basis_funding(key, venue) for key, venue in (
                ('entropy', self.venues.entropy), ('hedge', self.venues.hedge)))
        await asyncio.gather(*reads)
        if not funding_mode:
            try:
                self.journal.finalize_cycle_economics(
                    cycle_id, actual_funding_usd=(sum(funding_by_venue.values(), Decimal('0'))
                                                 if len(funding_by_venue) == 2 else None),
                    updated_ts=time.time())
            except Exception:
                pass
        gross = refresh_cash()
        fee_state = self.journal.fee_state(cycle_id, use_zero_fee_policy=policy_zero)
        if not funding_mode:
            self._estimate_basis_accounting(cycle_id)
        if gross is None or not fee_state['accounting_evidence_complete'] or not fee_state['fees_complete']:
            return None
        orders = [row for row in self.journal.cycle_orders(cycle_id) if row['filled_units'] > 0]
        matched_by_order: dict[str, list[dict]] = {}
        for row in self.journal.db.execute('SELECT * FROM fee_fills WHERE cycle_id=?', (cycle_id,)):
            order = by_ref[row['order_ref']]
            matched_by_order.setdefault(row['order_ref'], []).append(
                {**dict(row), 'qty': row['units'] * quantities[row['order_ref']], 'side': order['side']})
        if funding_mode:
            state = self.journal.funding_state(cycle_id)
            cash = self.journal.refresh_funding_pnl(cycle_id, trade_gross_cash_usd=gross)
            # Successful polls prove observed cash, never a complete posting watermark.
            if cash is not None and (state['last_complete_poll_ts'] is not None or all(
                    self.journal.db.execute('''SELECT 1 FROM funding_settlements
                        WHERE cycle_id=? AND venue=? LIMIT 1''', (cycle_id, key)).fetchone()
                    for key in ('entropy', 'hedge'))):
                self._finalize_economics_accounting(
                    cycle_id, orders, matched_by_order, state['funding_by_venue'], cash)
            return None
        if set(funding_by_venue) != {'entropy', 'hedge'}:
            return None
        cash = gross - fee_state['known_fees_usd'] + sum(funding_by_venue.values(), Decimal('0'))
        self.journal.finalize_cycle_accounting(cycle_id, cash)
        self._finalize_economics_accounting(cycle_id, orders, matched_by_order, funding_by_venue, cash)
        if not orders:
            self.journal.finalize_cycle_economics(
                cycle_id, finalized_pnl_usd=cash, result_kind='zero_fill', accounting_final=True,
                clean_summary_eligible=False, telemetry_incomplete=True, updated_ts=time.time())
        return cash

    async def _reconcile_pending_economics(self) -> None:
        # Both canonical and analytics-only tails use the same fair worker.
        rows = {row['cycle_id']: row for row in
                self.journal.provisional_cycles(self.authorization.batch_id)}
        rows.update({row['cycle_id']: row for row in
                     self.journal.pending_economics_cycles(self.authorization.batch_id)})
        candidates = sorted(rows)
        if not candidates or self._in_handoff:
            return
        start = self._accounting_queue_cursor % len(candidates)
        for offset in range(len(candidates)):
            index = (start + offset) % len(candidates)
            cycle_id = candidates[index]
            now = time.monotonic()
            last = self._basis_accounting_attempts.get(cycle_id)
            delay = (self.settings.funding_accounting_grace_seconds
                     if self.settings.strategy_mode == 'funding_hedge' else 120.0)
            if last is not None and now - last < delay:
                continue
            self._basis_accounting_attempts[cycle_id] = now
            self._accounting_queue_cursor = index + 1
            await self._reconcile_cycle_accounting(cycle_id)
            break  # One circle per pass; its pages also share the HTTP budget.

    async def _analytics_reconcile_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await self._reconcile_pending_economics()
            except Exception:
                self.telemetry_errors += 1
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    async def _repair_to_balance(self, cycle: dict, *, purpose: str = "repair",
                                 evidence: PositionEvidence | None = None) -> None:
        if self.settings.strategy_mode == 'basis':
            if (self._standard_opening_pair(cycle)
                    and cycle['basis_profit_basis_json'] == self.journal.BASIS_PROFIT_PENDING):
                await self._continue_standard_opening(cycle)
                return
            if self.journal.pending():
                await self._cover_unknown_exposure(cycle)
            elif not cycle['entropy_units'] and not cycle['hedge_units']:
                await self._finish_flat(cycle)
            elif cycle['phase'] == 'CLOSE_HEDGE' or self._close_group(cycle) is not None:
                await self._hedge_known_close_group(cycle)
                cycle = self.journal.active_live_cycle() or cycle
                if cycle['entropy_units'] + cycle['hedge_units']:
                    await self._reduce_residual(cycle, cycle['entropy_units'],
                                               cycle['hedge_units'], purpose='close_hedge')
                    cycle = self.journal.active_live_cycle() or cycle
                if cycle['entropy_units'] + cycle['hedge_units']:
                    return
                if cycle['entropy_units']:
                    self.journal.update_live_cycle(cycle['cycle_id'], phase='OPEN', updated_ts=time.time())
                else:
                    await self._finish_flat(cycle)
            elif self._basis_state_ready() and cycle['entropy_units']:
                await self._open_hedge(cycle)
            else:
                await self._reduce_residual(cycle, cycle['entropy_units'], cycle['hedge_units'],
                                            purpose=purpose)
            return
        if cycle['phase'] == 'CLOSE_HEDGE' and not self.journal.pending():
            await self._hedge_known_close_group(cycle)
            cycle = self.journal.active_live_cycle() or cycle
        if (evidence is None or evidence.cycle_id != cycle['cycle_id']
                or not evidence.fully_authoritative):
            evidence = await self._position_evidence(cycle)
        evidence = self._settlement_evidence(cycle, evidence)
        if not evidence.fully_authoritative:
            self.status.write(
                'RECONCILE_REQUIRED',
                evidence.error or 'authoritative position evidence unavailable',
                cycle_id=cycle['cycle_id'],
                pending_orders=len(self.journal.pending()))
            return
        cycle = await self._sync_positions(cycle, evidence=evidence)
        net = cycle['entropy_units'] + cycle['hedge_units']
        if net == 0:
            phase = 'OPEN' if cycle['entropy_units'] else 'DONE'
            if phase == 'DONE':
                if not self.journal.pending():
                    await self._finish_flat(cycle, evidence=evidence)
            else:
                group_id = self._latest_close_group_id(cycle['cycle_id'])
                if group_id is not None:
                    try:
                        self.journal.record_authoritative_reconcile(
                            cycle['cycle_id'], observed_ts=time.time(),
                            mono_ns=time.monotonic_ns(),
                            clock_session_id=self.clock_session_id,
                            execution_group_id=group_id)
                    except Exception:
                        pass
                self.journal.update_live_cycle(
                    cycle['cycle_id'], phase=phase, updated_ts=time.time())
                self._capture_entry_economics(cycle['cycle_id'])
            return
        if cycle['entropy_units'] and not self.journal.pending() \
                and cycle['phase'] in {'OPEN_HEDGE', 'RECOVERY'}:
            target = -cycle['entropy_units']
            delta = target - cycle['hedge_units']
            if delta:
                self.journal.mark_recovery_used(
                    cycle['cycle_id'], updated_ts=time.time())
                await self._campaign(
                    cycle, purpose='repair_complete_pair', venue_key='hedge',
                    side=Side.BUY if delta > 0 else Side.SELL,
                    units=abs(delta), reduce_only=False,
                    rooms=self.settings.hedge_execution_policy('open')[0],
                    hard_cap=self.settings.hedge_execution_policy('open')[1],
                    next_phase='RECOVERY', completion=True)
                cycle = await self._sync_positions(
                    self.journal.active_live_cycle() or cycle)
                net = cycle['entropy_units'] + cycle['hedge_units']
                if net == 0:
                    self.journal.update_live_cycle(
                        cycle['cycle_id'], phase='OPEN', updated_ts=time.time())
                    self._capture_entry_economics(cycle['cycle_id'])
                    return
        if self.settings.strategy_mode == 'funding_hedge':
            self._latch_funding_exit(cycle, 'partial_hedge_recovery')
            await self._funding_exit(cycle)
            return
        await self._reduce_residual(
            cycle, cycle['entropy_units'], cycle['hedge_units'], purpose=purpose)
        updated = await self._sync_positions(self.journal.active_live_cycle() or cycle)
        if not updated['position_evidence_authoritative']:
            self.status.write(
                'RECONCILE_REQUIRED', 'authoritative position evidence unavailable',
                cycle_id=cycle['cycle_id'],
                pending_orders=len(self.journal.pending()))
            return
        if updated['entropy_units'] + updated['hedge_units'] != 0:
            raise RuntimeHalt("residual remains after bounded reduce-only repair")
        if updated['entropy_units'] == 0:
            if not self.journal.pending():
                await self._finish_flat(updated)
        else:
            group_id = self._latest_close_group_id(updated['cycle_id'])
            if group_id is not None:
                try:
                    self.journal.record_authoritative_reconcile(
                        updated['cycle_id'], observed_ts=time.time(),
                        mono_ns=time.monotonic_ns(),
                        clock_session_id=self.clock_session_id,
                        execution_group_id=group_id)
                except Exception:
                    pass
            self.journal.update_live_cycle(
                updated['cycle_id'], phase='OPEN', updated_ts=time.time())

    async def _open_first(self, cycle: dict,
                          decision: OpenDecision | None = None,
                          *, request_reserved: bool = False) -> None:
        try:
            await self._open_first_inner(cycle, decision, request_reserved=request_reserved)
        finally:
            self._open_handoff = None
            self._in_handoff = False
            self._flush_telemetry()

    @staticmethod
    def _opening_source_build_hash() -> str:
        source_root = Path(__file__).resolve().parents[2]
        paths = [source_root / 'main_v3.py', *sorted((source_root / 'entropy_arb').rglob('*.py'))]
        source = hashlib.sha256()
        for path in paths:
            source.update(str(path.relative_to(source_root)).encode() + b'\0')
            source.update(hashlib.sha256(path.read_bytes()).digest())
        return source.hexdigest()

    def _ensure_opening_experiment(self) -> None:
        contract = self.status.base['opening_experiment_contract']
        contract['authorization_hash'] = canonical_config_hash(
            self.authorization, pair_identity=self.pair_id)
        contract['source_build_hash'] = self._opening_source_build_hash()
        self.journal.ensure_opening_experiment(self.settings.opening_experiment_id,
            batch_id=self.authorization.batch_id, contract=contract)
        # A crash after DONE but before recording the experimental outcome
        # must not strand a completed slot or allocate it a second time.
        state = self.journal.opening_experiment_state(self.settings.opening_experiment_id)
        current = state['current_attempt']
        if current is not None and not current['terminal_flat']:
            row = self.journal.db.execute('SELECT phase FROM live_cycles WHERE cycle_id=?',
                                          (current['cycle_id'],)).fetchone()
            if row is not None and row['phase'] == 'DONE':
                self._finish_opening_slot(current['cycle_id'])
        self.status.base['opening_experiment_state'] = self.journal.opening_experiment_state(
            self.settings.opening_experiment_id)

    def _finish_opening_slot(self, cycle_id: str) -> None:
        orders = self.journal.cycle_orders(cycle_id)
        opening = [o for o in orders if not o['reduce_only']]
        if any(o['had_unknown'] for o in orders):
            outcome = 'unknown_resolved'
        elif not any(o['filled_units'] for o in opening):
            outcome = 'failed' if not opening else 'zero'
        elif self.journal.db.execute(
                'SELECT basis_profit_basis_json FROM live_cycles WHERE cycle_id=?',
                (cycle_id,)).fetchone()[0] in (None, self.journal.BASIS_PROFIT_PENDING):
            # Filling one leg and unwinding it never formed a paired holding.
            outcome = 'unpaired_repaired'
        elif any(0 < o['filled_units'] < o['requested_units'] for o in opening):
            outcome = 'partial_repaired'
        else:
            outcome = 'filled'
        self.journal.finish_opening_attempt(cycle_id, outcome=outcome)
        self.status.base['opening_experiment_state'] = self.journal.opening_experiment_state(
            self.settings.opening_experiment_id)

    def _opening_pair_guard(self, cycle: dict, order: dict, opening: dict) -> str | None:
        refs = {opening['first_ref'], opening['hedge_ref']}
        if (opening['terminal_flat'] or cycle['phase'] not in {'OPEN_FIRST', 'OPEN_HEDGE'}
                or cycle['basis_exit_requested_ts'] is not None
                or not self._operator_running()
                or not self._basis_recovered or self._basis_events() != self._basis_event_watermark
                or self.journal.batch_state(self.authorization.batch_id)['closed_for_entries']
                or any(o['order_ref'] not in refs for o in self.journal.pending())):
            return 'opening_pair_recovery_required'
        rows = self.journal.cycle_orders(cycle['cycle_id'])
        pair = [o for o in rows if o['order_ref'] in refs]
        direction = Direction(cycle['direction'])
        known = dict(zip(('entropy', 'hedge'), self._known_positions(cycle)))
        steps = {s['order_ref']: s for s in self.journal.live_steps(cycle['cycle_id'])}
        # A peer may have reached the ledger before the loop consumes its
        # result. Only that pair's unapplied cumulative fill can explain lag.
        positions_valid = all(
            known[o['venue']] == cycle[f"{o['venue']}_units"]
            or (steps.get(o['order_ref'], {}).get('applied') == 0
                and known[o['venue']] == Side(o['side']).sign * o['filled_units']
                and 0 <= Side(o['side']).sign * cycle[f"{o['venue']}_units"] <= o['filled_units'])
            for o in pair)
        if (len(pair) != 2 or any(o['had_unknown'] for o in (pair if opening.get('standard') else rows))
                or order['state'] != 'SUBMITTING' or order['filled_units']
                or any(o['state'] not in {'SUBMITTING', 'TERMINAL'} or o['reduce_only']
                       or o['requested_units'] != cycle['planned_units']
                       or Side(o['side']) is not (direction.entropy_side if o['venue'] == 'entropy'
                                                 else direction.entropy_side.opposite)
                       for o in pair)
                or any(o['filled_units'] and o['order_ref'] not in refs for o in rows)
                or not positions_valid):
            return 'opening_pair_identity_changed'
        try:
            if plan_open(self._market(), self.instrument, self.settings,
                         dec(cycle['reference_bps']), direction, cycle['planned_units']) is None:
                return 'basis_signal_no_longer_valid'
        except (RuntimeHalt, ValueError):
            return 'basis_market_unavailable'
        return None

    def _opening_plans(self, cycle: dict, market: Market, *, group: str,
                       attempt: int, standard: bool = False) -> tuple[list, list] | None:
        plans, jobs = [], []
        decision_ts, decision_mono = time.time(), time.monotonic_ns()
        first_key = f"{cycle['cycle_id']}:open_first:{attempt}"
        direction = Direction(cycle['direction'])
        for key in ('entropy', 'hedge') if group == 'A' else ('entropy',):
            purpose = 'open_first' if key == 'entropy' else 'open_hedge'
            rooms, cap = (self.settings.first_leg_execution_policy('open') if key == 'entropy'
                          else self.settings.hedge_execution_policy('open'))
            side = direction.entropy_side if key == 'entropy' else direction.entropy_side.opposite
            try:
                benchmark = self._execution_benchmark(cycle, venue_key=key, side=side,
                    units=cycle['planned_units'], room_bps=rooms[attempt], hard_cap_bps=cap,
                    market=market)
            except ValueError as exc:
                if str(exc) != 'depth outside physical hard cap':
                    raise
                self.status.write('READY', 'opening_depth_outside_hard_cap',
                                  venue=key, hard_cap_bps=str(cap))
                return None
            ref = OrderRefFactory.create(getattr(self.venues, key).kind, deployment=self.deployment,
                cycle_id=cycle['cycle_id'], leg=f'{purpose}:{attempt}')
            intent = Intent(ref, cycle['cycle_id'], key, side, cycle['planned_units'],
                            benchmark['limit_px'], False)
            step = {'step_key': f"{cycle['cycle_id']}:{purpose}:{attempt}",
                    'execution_group_id': (f"{cycle['cycle_id']}:open_pair:{attempt}" if standard else first_key),
                    'parent_step_key': first_key if key == 'hedge' else None,
                    'quantity_step': self.instrument.quantity_step}
            plans.append((intent, step))
            jobs.append(dict(purpose=purpose, attempt=attempt, venue_key=key, side=side,
                units=intent.units, reduce_only=False, room_bps=rooms[attempt], hard_cap_bps=cap,
                next_phase='OPEN_HEDGE', completion=False, decision_market=market,
                parent_step_key=first_key if key == 'hedge' else None,
                _prepared_benchmark=benchmark, _prepared_decision_ts=decision_ts,
                _prepared_decision_mono_ns=decision_mono))
        return plans, jobs

    def _standard_opening_pair(self, cycle: dict) -> dict | None:
        if self.settings.opening_experiment_id:
            return None
        steps = [s for s in self.journal.live_steps(cycle['cycle_id']) if not s['reduce_only']
                 and (s['execution_group_id'] or '').startswith(f"{cycle['cycle_id']}:open_pair:")]
        if not steps:
            return None
        group = steps[-1]['execution_group_id']
        pair = {s['venue']: s['order_ref'] for s in steps if s['execution_group_id'] == group}
        return dict(group_name='A', standard=True, terminal_flat=False,
                    first_ref=pair.get('entropy'), hedge_ref=pair.get('hedge'))

    async def _admit_standard_opening(self, decision: OpenDecision) -> None:
        cycle = dict(cycle_id=f"v3-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
            batch_id=self.authorization.batch_id, direction=decision.direction.value,
            planned_units=decision.units, reference_bps=decision.reference_bps,
            entry_basis_bps=decision.executable_basis_bps, started_ts=time.time(),
            planned_notional_usd=decision.notional_usd, signal_ts=decision.decision_ts,
            signal_mono_ns=decision.decision_mono_ns, signal_clock_session_id=self.clock_session_id,
            position_check_start_ts=decision.decision_ts, position_check_end_ts=decision.decision_ts,
            cycle_persist_start_ts=time.time())
        await self._dispatch_standard_opening(cycle, attempt=0, initial=True)
        current = self.journal.active_live_cycle()
        if current is not None and current['cycle_id'] == cycle['cycle_id']:
            await self._continue_standard_opening(current)

    async def _dispatch_standard_opening(self, cycle: dict, *, attempt: int,
                                         initial: bool = False) -> bool:
        tokens = {}
        settings = self.settings
        config_hash = self.status.base.get('config_hash')
        try:
            opening = self._opening_plans(cycle, self._market(), group='A',
                                          attempt=attempt, standard=True)
            if opening is None:
                return False
            plans, jobs = opening
            for intent, _ in plans:
                tokens[intent.venue] = await self.brokers[intent.venue].prepare_dispatch(
                    intent, completion=False)
            market = self._market()
            current = self.journal.active_live_cycle()
            batch = self.journal.batch_state(cycle['batch_id'])
            if (self.stop.is_set() or not self._operator_running()
                    or batch['closed_for_entries']
                    or (batch['loss_budget_usd'] is not None
                        and batch['loss_used_usd'] >= batch['loss_budget_usd'])
                    or self.settings is not settings
                    or self.status.base.get('config_hash') != config_hash
                    or (current is not None if initial else
                        current is None or current['cycle_id'] != cycle['cycle_id']
                        or current['direction'] != cycle['direction']
                        or current['phase'] not in {'OPEN_FIRST', 'OPEN_HEDGE'}
                        or current['basis_exit_requested_ts'] is not None
                        or current['entropy_units'] or current['hedge_units'])
                    or (self._valid_flat_lease() is None if initial else not self._basis_state_ready())
                    or plan_open(market, self.instrument, self.settings, dec(cycle['reference_bps']),
                                 Direction(cycle['direction']), cycle['planned_units']) is None):
                return False
            opening = self._opening_plans(cycle, market, group='A', attempt=attempt, standard=True)
            if opening is None:
                return False
            plans, jobs = opening
            prepared = self.journal.prepare_standard_opening_pair(cycle=cycle, plans=plans,
                initial=initial, request=dict(now=time.time(), limit=self.settings.entropy_orders_per_min,
                    reserve_for_completion=0, completion=False, request_kind='open_first'))
            if prepared is None:
                return False
            self._invalidate_flat_lease()
            await self._dispatch_opening_plans(self.journal.active_live_cycle(), plans, jobs, prepared, tokens)
            return True
        except RequestDeferred as exc:
            self._send_retry_after['hedge'] = time.monotonic() + exc.retry_after
            self.status.write('READY', 'opening_pair_resource_unavailable', retry_after=exc.retry_after)
            return False
        finally:
            for token in tokens.values():
                if token is not None:
                    token.release()

    async def _continue_standard_opening(self, cycle: dict) -> None:
        while not self.stop.is_set():
            current = self.journal.active_live_cycle()
            if current is None or current['cycle_id'] != cycle['cycle_id']:
                return
            cycle = current
            if cycle['basis_exit_requested_ts'] is not None or self.journal.pending():
                return  # Unknown/in-flight identities are recovered; never increase their remainder.
            e_units, h_units = self._known_positions(cycle)
            if e_units + h_units:
                self.journal.update_live_cycle(cycle['cycle_id'], phase='RECOVERY', updated_ts=time.time())
                await self._reduce_residual(cycle, e_units, h_units, purpose='repair')
                cycle = self.journal.active_live_cycle() or cycle
                if self.journal.pending() or cycle['entropy_units'] + cycle['hedge_units']:
                    self.status.write('RECONCILE_REQUIRED', 'opening_pair_repair_pending',
                                      cycle_id=cycle['cycle_id'])
                    return
            if cycle['entropy_units']:
                self._complete_basis_open(cycle)
                self._enqueue_telemetry(lambda: self._capture_entry_economics(cycle['cycle_id']))
                return
            orders = self.journal.cycle_orders(cycle['cycle_id'])
            attempt = sum(o['venue'] == 'entropy' and not o['reduce_only'] for o in orders)
            batch = self.journal.batch_state(cycle['batch_id'])
            rounds = min(len(self.settings.first_leg_execution_policy('open')[0]),
                         len(self.settings.hedge_execution_policy('open')[0]))
            if (any(o['filled_units'] for o in orders) or len(orders) != attempt * 2
                    or attempt >= rounds or not self._operator_running()
                    or batch['closed_for_entries']
                    or (batch['loss_budget_usd'] is not None
                        and batch['loss_used_usd'] >= batch['loss_budget_usd'])
                    or any(time.monotonic() < value for value in self._send_retry_after.values())
                    or not self._basis_state_ready()):
                await self._finish_flat(cycle, zero_fill=not any(o['filled_units'] for o in orders))
                return
            plan = choose_open_plan(self._market(), self.instrument, self.settings,
                dec(cycle['reference_bps']), entropy_venue=self.venues.entropy,
                hedge_venue=self.venues.hedge)
            if plan is None or plan.direction.value != cycle['direction']:
                await self._finish_flat(cycle, zero_fill=True)
                return
            cycle = {**cycle, 'planned_units': plan.units}
            if not await self._dispatch_standard_opening(cycle, attempt=attempt):
                return

    def _order_progress(self, intent: Intent, result: Result, timing: dict | None) -> None:
        # The executor has persisted the trusted cumulative observation first.
        current = self.journal.active_live_cycle()
        if current is None or current['cycle_id'] != intent.cycle_id:
            row = self.journal.order(intent.order_ref)
            if row['state'] == 'TERMINAL' and any(
                    step['order_ref'] == intent.order_ref and step['applied']
                    for step in self.journal.live_steps(intent.cycle_id)):
                return  # Late evidence belongs to the completed order, never the new cycle.
            raise RuntimeHalt('order progress has no matching active cycle')
        if self.settings.strategy_mode == 'basis':
            units, unproven = self._execution_positions(current)
            for key in unproven:
                units[key] = int(current[key + '_units'])
            e_units, h_units = units['entropy'], units['hedge']
            self.journal.update_live_cycle(intent.cycle_id, entropy_units=e_units,
                hedge_units=h_units, position_evidence_authoritative=False,
                updated_ts=time.time())
            self._basis_exit_retry_after = 0.0
        trace = order_timing_trace(self.brokers[intent.venue], intent, timing)
        if trace is not None:
            trace.mark('runtime_progress_consumed', first=True)
            if result.terminal:
                trace.consumed()
        self.changed.set()

    def _track_parallel(self, ref: str, operation, consume=None) -> asyncio.Task:
        if ref in self._parallel_tasks:
            raise RuntimeHalt('duplicate in-flight order identity')
        async def execute():
            try:
                result = await operation
            except BaseException as exc:
                async def failed(error=exc):
                    if isinstance(error, asyncio.CancelledError):
                        raise error
                    self.status.write('RECONCILE_REQUIRED',
                        'parallel_dispatch_or_persistence_unknown',
                        order_ref=ref, error=type(error).__name__)
                    if getattr(self.executor, 'entries_halted', False):
                        raise RuntimeHalt('parallel persistence failure') from error
                self._parallel_completions.append((ref, failed))
            else:
                async def completed():
                    await consume(result)
                self._parallel_completions.append((ref, completed if consume else None))
            finally:
                self.changed.set()
        task = asyncio.create_task(execute(), name=f'v3-order-{ref}')
        self._parallel_tasks[ref] = task
        return task

    async def _drain_parallel(self) -> None:
        while self._parallel_completions:
            ref, consume = self._parallel_completions.pop(0)
            try:
                if consume is not None:
                    await consume()
            finally:
                task = self._parallel_tasks.get(ref)
                if task is not None and task.done():
                    self._parallel_tasks.pop(ref, None)

    def _recover_parallel_unknown(self) -> None:
        if self.settings.strategy_mode != 'basis' or self.stop.is_set():
            return
        for row in self.journal.pending():
            if (row['state'] != 'UNKNOWN' or row['order_ref'] in self._parallel_tasks
                    or time.monotonic() < self._parallel_resolve_after.get(row['order_ref'], 0)):
                continue
            intent = Intent(row['order_ref'], row['cycle_id'], row['venue'], Side(row['side']),
                row['requested_units'], dec(row['limit_px']), bool(row['reduce_only']))
            async def recovered(result, ref=intent.order_ref):
                if result.terminal:
                    self._parallel_resolve_after.pop(ref, None)
                else:
                    self._parallel_resolve_after[ref] = time.monotonic() + 1.0
            self._track_parallel(intent.order_ref,
                self.executor.resolve(intent, timeout=1.0), recovered)

    async def _wait_parallel(self, tasks: list[asyncio.Task]) -> None:
        wake = asyncio.create_task(self.changed.wait())
        try:
            await asyncio.wait([*tasks, wake], return_when=asyncio.FIRST_COMPLETED)
            await self._drain_parallel()
        finally:
            wake.cancel()
            await asyncio.gather(wake, return_exceptions=True)

    async def _dispatch_opening_plans(self, cycle: dict, plans: list, jobs: list,
                                      prepared: dict, tokens: dict) -> None:
        self._in_handoff = True
        try:
            parallel = len(plans) > 1
            operations = [(intent, self._step_inner(cycle, **job,
                _dispatch_token=tokens.get(intent.venue),
                _prepared_dispatch=prepared[intent.order_ref],
                _parallel_dispatch=parallel))
                for (intent, _), job in zip(plans, jobs)]
            if parallel:
                self.changed.clear()
                tasks = [self._track_parallel(intent.order_ref, operation)
                         for intent, operation in operations]
                await self._wait_parallel(tasks)
            else:
                for _, operation in operations:
                    await operation
        finally:
            self._in_handoff = False
            telemetry = self._deferred_open_telemetry
            self._deferred_open_telemetry = None
            if telemetry is not None:
                telemetry()
            self._flush_telemetry()
        self.changed.set()

    async def _admit_opening_experiment(self, decision: OpenDecision) -> None:
        cycle_id = f"v3-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
        state = self.journal.opening_experiment_state(self.settings.opening_experiment_id)
        group = 'A' if (state['attempts'] + 1) % 2 else 'B'
        cycle = dict(cycle_id=cycle_id, batch_id=self.authorization.batch_id,
            direction=decision.direction.value, planned_units=decision.units,
            reference_bps=decision.reference_bps, entry_basis_bps=decision.executable_basis_bps,
            started_ts=time.time(), planned_notional_usd=decision.notional_usd,
            signal_ts=decision.decision_ts, signal_mono_ns=decision.decision_mono_ns,
            signal_clock_session_id=self.clock_session_id,
            position_check_start_ts=decision.decision_ts,
            position_check_end_ts=decision.decision_ts, cycle_persist_start_ts=time.time())
        tokens = {}
        admitted = False
        try:
            if group == 'B':
                reserve = getattr(self.venues.hedge, 'reserve_completion', None)
                if reserve is not None:
                    self._completion_ticket = reserve()
            market = self._market()
            opening = self._opening_plans(cycle, market, group=group, attempt=0)
            if opening is None:
                return
            plans, jobs = opening
            # A's initial H is new exposure, not completion: it must use
            # ordinary physical admission and preserve the exit reserve.
            for intent, _ in plans:
                tokens[intent.venue] = await self.brokers[intent.venue].prepare_dispatch(
                    intent, completion=False)
            market = self._market()
            evidence = self._last_position_evidence
            if (evidence is None or not evidence.fully_authoritative
                    or (evidence.entropy_units, evidence.hedge_units) != (0, 0)
                    or time.time() - evidence.observed_ts > self.settings.position_audit_seconds):
                self._position_audit_requested.set()
                return
            if (self.stop.is_set() or self._valid_flat_lease() is None
                    or plan_open(market, self.instrument, self.settings, decision.reference_bps,
                                 decision.direction, decision.units) is None):
                return
            opening = self._opening_plans(cycle, market, group=group, attempt=0)
            if opening is None:
                return
            plans, jobs = opening
            prepared = self.journal.admit_opening_attempt(self.settings.opening_experiment_id,
                cycle=cycle, plans=plans, pair_plan={
                    'entropy_units': decision.units,
                    'hedge_units': decision.units if group == 'A' else 'terminal_entropy_unmatched',
                    'entropy_side': decision.direction.entropy_side.value,
                    'hedge_side': decision.direction.entropy_side.opposite.value},
                request=dict(now=time.time(), limit=self.settings.entropy_orders_per_min,
                    reserve_for_completion=0, completion=False, request_kind='open_first'))
            if prepared is None:
                return
            admitted = True
            self._invalidate_flat_lease()
            self.status.base['opening_experiment_state'] = self.journal.opening_experiment_state(
                self.settings.opening_experiment_id)
            await self._dispatch_opening_plans(self.journal.active_live_cycle(), plans, jobs, prepared, tokens)
        except RequestDeferred as exc:
            self._send_retry_after['hedge'] = time.monotonic() + exc.retry_after
            return
        finally:
            for token in tokens.values():
                if token is not None:
                    token.release()
            if not admitted:
                self._release_completion()
        cycle = self.journal.active_live_cycle()
        if cycle is None:
            return
        if self.journal.pending():
            await self._cover_unknown_exposure(cycle)
        elif group == 'A':
            await self._continue_opening_pair(cycle)
        elif cycle['entropy_units']:
            await self._open_hedge(cycle)
        else:
            await self._open_first(cycle)

    async def _continue_opening_pair(self, cycle: dict) -> None:
        if self.journal.pending():
            await self._cover_unknown_exposure(cycle)
            return
        orders = self.journal.cycle_orders(cycle['cycle_id'])
        if any(o['filled_units'] or o['had_unknown'] for o in orders):
            if cycle['entropy_units'] or cycle['hedge_units']:
                await self._open_hedge(cycle)
            else:
                await self._finish_flat(cycle, zero_fill=True)
            return
        attempt = len([o for o in orders if o['venue'] == 'entropy'])
        rounds = min(len(self.settings.first_leg_execution_policy('open')[0]),
                     len(self.settings.hedge_execution_policy('open')[0]))
        # A proved-unsent peer may have been removed by the shared executor.
        # It is a failed admission, never permission to send a new A wave.
        if (len(orders) != attempt * 2 or attempt >= rounds
                or not self._basis_state_ready()):
            await self._finish_flat(cycle, zero_fill=True)
            return
        tokens = {}
        try:
            market = self._market()
            direction = Direction(cycle['direction'])
            if plan_open(market, self.instrument, self.settings, dec(cycle['reference_bps']),
                         direction, cycle['planned_units']) is None:
                await self._finish_flat(cycle, zero_fill=True)
                return
            opening = self._opening_plans(cycle, market, group='A', attempt=attempt)
            if opening is None:
                await self._finish_flat(cycle, zero_fill=True)
                return
            plans, jobs = opening
            for intent, _ in plans:
                tokens[intent.venue] = await self.brokers[intent.venue].prepare_dispatch(
                    intent, completion=False)
            market = self._market()
            if (self.stop.is_set() or not self._basis_state_ready()
                    or plan_open(market, self.instrument, self.settings, dec(cycle['reference_bps']),
                                 direction, cycle['planned_units']) is None):
                await self._finish_flat(cycle, zero_fill=True)
                return
            opening = self._opening_plans(cycle, market, group='A', attempt=attempt)
            if opening is None:
                await self._finish_flat(cycle, zero_fill=True)
                return
            plans, jobs = opening
            prepared = self.journal.prepare_opening_pair(cycle['cycle_id'], plans=plans,
                request=dict(now=time.time(), limit=self.settings.entropy_orders_per_min,
                    reserve_for_completion=0, completion=False, request_kind='open_first'))
            if prepared is None:
                return
            await self._dispatch_opening_plans(cycle, plans, jobs, prepared, tokens)
        except RequestDeferred as exc:
            self._send_retry_after['hedge'] = time.monotonic() + exc.retry_after
            return
        finally:
            for token in tokens.values():
                if token is not None:
                    token.release()
        self.journal.update_live_cycle(cycle['cycle_id'], phase='OPEN_HEDGE', updated_ts=time.time())
        self.changed.set()

    async def _open_first_inner(self, cycle: dict,
                                decision: OpenDecision | None = None,
                                *, request_reserved: bool = False) -> None:
        self._deferred_open_telemetry = None
        self._open_handoff = None
        self._in_handoff = True
        direction = Direction(cycle['direction'])
        filled = await self._campaign(
            cycle, purpose='open_first', venue_key='entropy',
            side=direction.entropy_side, units=cycle['planned_units'],
            reduce_only=False, rooms=self.settings.first_leg_execution_policy('open')[0],
            hard_cap=self.settings.first_leg_execution_policy('open')[1],
            next_phase='OPEN_HEDGE', completion=False,
            open_decision=decision,
            first_request_reserved=request_reserved,
            break_on_positive_fill=True)
        try:
            cycle = self.journal.active_live_cycle() or cycle
            handoff = self._open_handoff
            if self.settings.strategy_mode != 'basis' and not self._open_handoff_valid(cycle, handoff):
                handoff = None
                cycle = await self._sync_positions(cycle)
            if filled == 0 and not cycle['entropy_units'] and not self.journal.pending():
                self._release_completion()
                await self._finish_flat(cycle, zero_fill=True)
                return
            if cycle['entropy_units']:
                await self._open_hedge(cycle, handoff=handoff)
        finally:
            self._in_handoff = False
            telemetry = self._deferred_open_telemetry
            self._deferred_open_telemetry = None
            if telemetry is not None:
                telemetry()
            self._flush_telemetry()

    async def _open_hedge(self, cycle: dict, *, handoff: OpenHandoff | None = None,
                          request_reserved: bool = False) -> None:
        if self.settings.strategy_mode == 'basis':
            cycle = self.journal.active_live_cycle() or cycle
            if (self._standard_opening_pair(cycle)
                    and cycle['basis_profit_basis_json'] == self.journal.BASIS_PROFIT_PENDING):
                await self._continue_standard_opening(cycle)
                return
            repairing = cycle['phase'] == 'RECOVERY'
            if self.journal.pending():
                await self._cover_unknown_exposure(cycle)
                return
            delta = -cycle['entropy_units'] - cycle['hedge_units']
            if cycle['hedge_units'] and (not cycle['entropy_units']
                    or cycle['hedge_units'] * delta < 0):
                # A can fill H before E, or fill more H than E. Reduce that
                # exact excess before the common paired holding path.
                if not cycle['entropy_units'] or abs(cycle['hedge_units']) > abs(cycle['entropy_units']):
                    await self._reduce_residual(cycle, cycle['entropy_units'],
                                               cycle['hedge_units'], purpose='repair')
                    cycle = self.journal.active_live_cycle() or cycle
                    if cycle['entropy_units'] + cycle['hedge_units']:
                        return
                    delta = 0
            if delta:
                if not self._basis_state_ready():
                    await self._reduce_residual(cycle, cycle['entropy_units'],
                                               cycle['hedge_units'], purpose='repair')
                    return
                first = [s for s in self.journal.live_steps(cycle['cycle_id'])
                         if ':open_first:' in s['step_key'] and s['applied']
                         and (s['result_units'] or 0) > 0]
                await self._campaign(cycle, purpose='repair_complete_pair' if repairing else 'open_hedge',
                    venue_key='hedge',
                    side=Side.BUY if delta > 0 else Side.SELL, units=abs(delta),
                    reduce_only=False, rooms=self.settings.hedge_execution_policy('open')[0],
                    hard_cap=self.settings.hedge_execution_policy('open')[1],
                    next_phase='RECOVERY' if repairing else 'OPEN_HEDGE',
                    completion=True, parent_step_key=first[-1]['step_key'] if first else None)
                if time.monotonic() < self._send_retry_after.get('hedge', 0.0):
                    return
            self._release_completion()
            cycle = self.journal.active_live_cycle() or cycle
            if self.journal.pending():
                self._position_audit_requested.set()
                return
            if cycle['entropy_units'] + cycle['hedge_units']:
                self.journal.update_live_cycle(cycle['cycle_id'], phase='RECOVERY', updated_ts=time.time())
                if not repairing:
                    await self._repair_to_balance(self.journal.active_live_cycle() or cycle)
                else:
                    await self._reduce_residual(cycle, cycle['entropy_units'], cycle['hedge_units'], purpose='repair')
            elif cycle['entropy_units']:
                self._complete_basis_open(cycle)
                self._enqueue_telemetry(lambda: self._capture_entry_economics(cycle['cycle_id']))
            else:
                await self._finish_flat(cycle)
            return
        if self.settings.strategy_mode == 'funding_hedge' and self.journal.pending():
            self._latch_funding_exit(cycle, 'unknown_order_recovery')
            await self._funding_exit(cycle)
            return
        wait_for_submission = getattr(self.venues.hedge, 'wait_for_submission', None)
        if wait_for_submission is not None:
            await wait_for_submission()
            cycle = self.journal.active_live_cycle() or cycle
        if not self._open_handoff_valid(cycle, handoff):
            handoff = None
            cycle = await self._sync_positions(cycle)
        target = -cycle['entropy_units']
        delta = target - cycle['hedge_units']
        if delta:
            ref = OrderRefFactory.create(
                self.venues.hedge.kind, deployment=self.deployment,
                cycle_id=cycle['cycle_id'], leg='open_hedge:0')
            try:
                self.journal.order(ref)
                existing_order = True
            except KeyError:
                existing_order = False
            while not existing_order and not request_reserved and not self.stop.is_set():
                if self.settings.strategy_mode == 'funding_hedge':
                    await self._check_funding_liquidation(cycle)
                    state = self.journal.funding_state(cycle['cycle_id'])
                    if state and state['exit_reason']:
                        return
                limit = self.settings.hedge_orders_per_min
                request_reserved = self.journal.reserve_request(
                    'hedge', now=time.time(), limit=limit,
                    reserve_for_completion=min(2, max(0, limit - 1)), completion=True,
                    request_kind='open_hedge', cycle_id=cycle['cycle_id'])
                if not request_reserved:
                    # Recalculate before registering an immutable order intent;
                    # every budget wait invalidates the borrowed snapshot.
                    self._open_handoff = None
                    handoff = None
                    await asyncio.sleep(.5)
                    if wait_for_submission is not None:
                        await wait_for_submission()
                    cycle = await self._sync_positions(self.journal.active_live_cycle() or cycle)
                elif handoff is not None and not self._open_handoff_valid(cycle, handoff):
                    self._open_handoff = None
                    handoff = None
                    cycle = await self._sync_positions(self.journal.active_live_cycle() or cycle)
                target = -cycle['entropy_units']
                delta = target - cycle['hedge_units']
                if not delta:
                    await self._open_hedge(cycle, request_reserved=request_reserved)
                    return
            side = Side.BUY if delta > 0 else Side.SELL
            first_steps = [step for step in self.journal.live_steps(cycle['cycle_id'])
                           if ':open_first:' in step['step_key']
                           and step['applied'] and (step['result_units'] or 0) > 0]
            parent_step_key = first_steps[-1]['step_key'] if first_steps else None
            await self._campaign(
                cycle, purpose='open_hedge', venue_key='hedge', side=side,
                units=abs(delta), reduce_only=False,
                rooms=self.settings.hedge_execution_policy('open')[0],
                hard_cap=self.settings.hedge_execution_policy('open')[1],
                next_phase='OPEN_HEDGE', completion=True,
                parent_step_key=parent_step_key,
                first_request_reserved=request_reserved)
        telemetry = self._deferred_open_telemetry
        self._deferred_open_telemetry = None
        if telemetry is not None:
            telemetry()
        self._flush_telemetry()
        cycle = await self._sync_positions(self.journal.active_live_cycle() or cycle)
        if cycle['entropy_units'] + cycle['hedge_units']:
            self.journal.update_live_cycle(
                cycle['cycle_id'], phase='RECOVERY', updated_ts=time.time())
            await self._repair_to_balance(self.journal.active_live_cycle() or cycle)
        elif cycle['entropy_units']:
            self.journal.update_live_cycle(
                cycle['cycle_id'], phase='OPEN', updated_ts=time.time())
            self._capture_entry_economics(cycle['cycle_id'])
            self._confirm_funding_hedged(self.journal.active_live_cycle() or cycle)
        else:
            await self._finish_flat(cycle)

    def _open_stream_watermark(self) -> tuple | None:
        """Only healthy, observable account streams permit snapshot reuse."""
        tokens = []
        for venue in (self.venues.entropy, self.venues.hedge):
            feed = getattr(venue, 'orders_feed', None)
            ready = getattr(feed, 'ready', None)
            if ready is None or not ready.is_set():
                return None
            generation = getattr(feed, 'connection_generation', None)
            revision = getattr(feed, 'evidence_revision', None)
            if generation is None or revision is None:
                return None
            fresh = getattr(feed, 'fresh_account_snapshot', None)
            if fresh is not None and fresh() is None:
                return None
            tokens.append((id(feed), generation, revision))
        return tuple(tokens)

    def _open_step_watermark(self, cycle_id: str) -> tuple:
        return tuple((s['step_key'], s['order_ref'], s['applied'], s['result_units'])
                     for s in self.journal.live_steps(cycle_id))

    def _open_handoff_valid(self, cycle: dict, handoff: OpenHandoff | None) -> bool:
        if handoff is None:
            return False
        evidence = handoff.evidence
        if (cycle['phase'] != 'OPEN_HEDGE' or handoff.cycle_id != cycle['cycle_id']
                or evidence.cycle_id != cycle['cycle_id']
                or handoff.cycle_updated_ts != cycle['updated_ts']
                or not evidence.fully_authoritative or evidence.degraded or evidence.error
                or evidence.stream_watermark is None
                or evidence.stream_watermark != self._open_stream_watermark()
                or self.journal.pending()
                or handoff.orders != self._order_watermark(cycle['cycle_id'])
                or handoff.steps != self._open_step_watermark(cycle['cycle_id'])
                or (evidence.entropy_units, evidence.hedge_units) != (
                    cycle['entropy_units'], cycle['hedge_units'])):
            return False
        now = time.time()
        stamps = (evidence.observed_ts, evidence.entropy_received_ts, evidence.hedge_received_ts)
        if any(ts is None or not 0 <= now - ts <= 1.5 for ts in stamps):
            return False
        steps = {s['order_ref']: s for s in self.journal.live_steps(cycle['cycle_id'])}
        for order in self.journal.cycle_orders(cycle['cycle_id']):
            step = steps.get(order['order_ref'])
            received = getattr(evidence, f"{order['venue']}_received_ts")
            if (order['state'] != 'TERMINAL' or step is None or not step['applied']
                    or step['result_units'] != order['filled_units']
                    or order.get('terminal_recorded_ts') is None
                    or received < order['terminal_recorded_ts']):
                return False
        return True

    def _order_watermark(self, cycle_id: str) -> tuple:
        return tuple((o['order_ref'], o['state'], o['filled_units'])
                     for o in self.journal.cycle_orders(cycle_id))

    def _position_covers_orders(self, cycle: dict, evidence: PositionEvidence) -> bool:
        timestamps = [o['terminal_recorded_ts'] for o in
                      self.journal.cycle_orders(cycle['cycle_id'])
                      if o.get('terminal_recorded_ts') is not None]
        if timestamps and evidence.observed_ts < max(timestamps):
            return False
        if self.settings.strategy_mode == 'basis':
            for key in ('entropy', 'hedge'):
                stamps = [o['terminal_recorded_ts'] for o in self.journal.cycle_orders(cycle['cycle_id'])
                          if o['venue'] == key and o.get('terminal_recorded_ts') is not None]
                received = getattr(evidence, f'{key}_received_ts')
                if stamps and (received is None or received < max(stamps)):
                    return False
        if evidence.hedge_source.startswith('lighter_account_rest:'):
            hedge_times = [o['terminal_recorded_ts'] for o in
                           self.journal.cycle_orders(cycle['cycle_id'])
                           if o['venue'] == 'hedge' and o.get('terminal_recorded_ts') is not None]
            if hedge_times and (evidence.hedge_received_ts is None
                                or evidence.hedge_received_ts < max(hedge_times)):
                return False
        return True

    def _context(self, cycle: dict, evidence: PositionEvidence | None) -> CloseContext | None:
        if evidence is None or not evidence.fully_authoritative \
                or evidence.cycle_id != cycle['cycle_id'] \
                or not self._position_covers_orders(cycle, evidence) \
                or self.journal.pending() \
                or (evidence.entropy_units, evidence.hedge_units) != (
                    cycle['entropy_units'], cycle['hedge_units']):
            return None
        return CloseContext(
            cycle['cycle_id'], cycle['entropy_units'], cycle['hedge_units'],
            cycle['updated_ts'], evidence.observed_ts, evidence.hedge_generation,
            evidence.hedge_source, evidence.entropy_generation,
            evidence.entropy_received_ts, evidence.hedge_received_ts,
            self._order_watermark(cycle['cycle_id']))

    def _context_valid(self, cycle: dict, context: CloseContext | None) -> bool:
        if context is None or context.cycle_id != cycle['cycle_id'] \
                or context.cycle_updated_ts != cycle['updated_ts'] \
                or context.order_watermark != self._order_watermark(cycle['cycle_id']) \
                or self.journal.pending():
            return False
        stamps = (context.entropy_received_ts, context.hedge_received_ts)
        if any(ts is None or not 0 <= time.time() - ts <= 1.5 for ts in stamps):
            return False
        for venue, generation in ((self.venues.entropy, context.entropy_generation),
                                  (self.venues.hedge, context.hedge_generation)):
            ws = getattr(venue, 'orders_feed', None) or getattr(venue, '_acct_ws', None)
            current = getattr(ws, 'connection_generation', generation)
            if generation is not None and current != generation:
                return False
        return (context.entropy_units, context.hedge_units) == (
            cycle['entropy_units'], cycle['hedge_units'])

    async def _close(self, cycle: dict, *, new_exit: bool = False,
                     context: CloseContext | None = None) -> None:
        self._in_handoff = True
        try:
            await self._close_inner(cycle, new_exit=new_exit, context=context)
        finally:
            self._in_handoff = False
            telemetry = self._deferred_fast_close_telemetry
            self._deferred_fast_close_telemetry = None
            self._close_context = None
            self._fast_close_step_key = None
            self._fast_close_purpose = None
            self._fast_close_order_ref = None
            self._exit_trigger = None
            if telemetry is not None:
                try:
                    telemetry()
                except Exception:
                    self.telemetry_errors += 1
            self._flush_telemetry()

    async def _close_inner(self, cycle: dict, *, new_exit: bool = False,
                           context: CloseContext | None = None) -> None:
        self._close_context = None
        self._fast_close_step_key = None
        self._deferred_fast_close_telemetry = None
        if not (new_exit and self._context_valid(cycle, context)):
            cycle = await self._sync_positions(cycle)
        if self._last_position_evidence is not None and \
                self._last_position_evidence.error in {
                    'settlement_pending_known_fills', 'settlement_unavailable_known_fills',
                    'legacy_close_group_baseline_unavailable',
                    'unknown_close_group_snapshot_inconsistent'}:
            return
        if not cycle['entropy_units'] and not cycle['hedge_units']:
            await self._finish_flat(cycle)
            return
        if cycle['entropy_units']:
            side = Side.SELL if cycle['entropy_units'] > 0 else Side.BUY
            purpose = f"close_first_{len(self.journal.live_steps(cycle['cycle_id']))}"
            if (new_exit and self.settings.exit_fast_path_enabled
                    and cycle['phase'] == 'CLOSE_FIRST'
                    and cycle['entropy_units'] == -cycle['hedge_units']
                    and not self.journal.pending()
                    and (evidence := self._last_position_evidence) is not None
                    and evidence.fully_authoritative
                    and (evidence.entropy_units, evidence.hedge_units) == (
                        cycle['entropy_units'], cycle['hedge_units'])):
                self._close_context = self._context(cycle, evidence)
                self._fast_close_purpose = purpose
                self._fast_close_order_ref = OrderRefFactory.create(
                    self.venues.entropy.kind, deployment=self.deployment,
                    cycle_id=cycle['cycle_id'], leg=f'{purpose}:0')
            filled = await self._campaign(
                cycle, purpose=purpose,
                venue_key='entropy', side=side, units=abs(cycle['entropy_units']),
                reduce_only=True, rooms=self.settings.first_leg_execution_policy('close')[0],
                hard_cap=self.settings.first_leg_execution_policy('close')[1],
                next_phase='CLOSE_HEDGE', completion=True,
                break_on_positive_fill=True)
            if filled > 0:
                cycle = self.journal.active_live_cycle() or cycle
                residual = cycle['entropy_units'] + cycle['hedge_units']
                try:
                    await self._hedge_known_close_group(cycle)
                finally:
                    self._close_context = None
                    telemetry = self._deferred_fast_close_telemetry
                    self._deferred_fast_close_telemetry = None
                    if telemetry is not None:
                        telemetry()
                    self._flush_telemetry()
                if self.journal.pending():
                    self.journal.update_live_cycle(
                        cycle['cycle_id'], phase='RECOVERY', updated_ts=time.time())
                    return
                await self._repair_to_balance(
                    self.journal.active_live_cycle() or cycle,
                    purpose='close_hedge', evidence=self._last_position_evidence)
                return
        self._close_context = None
        cycle = await self._sync_positions(self.journal.active_live_cycle() or cycle)
        await self._repair_to_balance(cycle, purpose="close_hedge")

    async def _resume_cycle(self, cycle: dict) -> None:
        async with self._cycle_lock:
            await self._drain_parallel()
            self._recover_parallel_unknown()
            active = self.journal.active_live_cycle()
            if active is None or active['cycle_id'] != cycle['cycle_id']:
                return
            self._in_handoff = True
            try:
                await self._resume_cycle_inner(active)
            finally:
                self._in_handoff = False
                self._flush_telemetry()
                self._exit_trigger = None

    async def _confirm_basis_exit(self, cycle: dict,
                                  context: CloseContext | None = None) -> dict | None:
        if self.settings.strategy_mode == 'basis':
            # Terminal cumulative fills drive each known leg independently.
            # A peer UNKNOWN cannot freeze a known reduce-only residual.
            self._ensure_position_anchor(cycle)
            units, unproven = self._execution_positions(cycle)
            for key in unproven:
                units[key] = int(cycle[key + '_units'])
            for step in self.journal.live_steps(cycle['cycle_id']):
                if step['applied']:
                    continue
                try:
                    order = self.journal.order(step['order_ref'])
                except KeyError:
                    continue
                if order['state'] != 'TERMINAL':
                    continue
                result = Result(True, order['filled_units'],
                    dec(order['avg_px']) if order['avg_px'] is not None else None,
                    dec(order['fee_usd']) if order['fee_usd'] is not None else None)
                cash, final = self._cash_result(step['venue'], Side(step['side']), result,
                                                dec(order['limit_px']))
                self.journal.apply_live_step(step['step_key'], result_units=result.cumulative_units,
                    cash_delta_usd=cash, accounting_final=final,
                    entropy_units=units['entropy'], hedge_units=units['hedge'],
                    next_phase='BASIS_EXIT', updated_ts=time.time(),
                    position_evidence_authoritative=False)
            cycle = self.journal.active_live_cycle() or cycle
            if (cycle['entropy_units'], cycle['hedge_units']) != (
                    units['entropy'], units['hedge']):
                self.journal.update_live_cycle(cycle['cycle_id'],
                    entropy_units=units['entropy'], hedge_units=units['hedge'],
                    position_evidence_authoritative=False, updated_ts=time.time())
                cycle = self.journal.active_live_cycle() or cycle
            try:
                await self._observe_active_risk(cycle)
            except (RuntimeHalt, ValueError):
                pass  # A missing book cannot delay a latched reduce-only exit.
            if not any(units.values()) and not unproven:
                await self._finish_flat(cycle)
                return None
            return cycle
        if self.journal.pending():
            return None
        expected = {'entropy': 0, 'hedge': 0}
        for order in self.journal.cycle_orders(cycle['cycle_id']):
            expected[order['venue']] += Side(order['side']).sign * order['filled_units']
        evidence = (self._last_position_evidence if self._context_valid(cycle, context)
                    else await self._position_evidence(cycle))
        if (evidence is None or not evidence.fully_authoritative
                or not self._position_covers_orders(cycle, evidence)
                or (evidence.entropy_units, evidence.hedge_units) != (
                    expected['entropy'], expected['hedge'])):
            self.status.write('EXITING', 'basis_exit_position_confirmation_required',
                              cycle_id=cycle['cycle_id'])
            return None
        # Each terminal fill is accounted once, against one reconciled pair snapshot.
        self.journal.update_live_cycle(
            cycle['cycle_id'], entropy_units=expected['entropy'], hedge_units=expected['hedge'],
            position_evidence_authoritative=True, last_authoritative_ts=evidence.observed_ts,
            updated_ts=time.time())
        for step in self.journal.live_steps(cycle['cycle_id']):
            if step['applied']:
                continue
            try:
                order = dict(self.journal.order(step['order_ref']))
            except KeyError:
                continue  # Registered before a crash, but never submitted.
            result = Result(True, order['filled_units'],
                            dec(order['avg_px']) if order['avg_px'] is not None else None,
                            dec(order['fee_usd']) if order['fee_usd'] is not None else None)
            cash, final = self._cash_result(step['venue'], Side(step['side']), result,
                                            dec(order['limit_px']))
            self.journal.apply_live_step(
                step['step_key'], result_units=result.cumulative_units,
                cash_delta_usd=cash, accounting_final=final,
                entropy_units=expected['entropy'], hedge_units=expected['hedge'],
                next_phase='BASIS_EXIT', updated_ts=time.time(),
                position_evidence_authoritative=True, last_authoritative_ts=evidence.observed_ts)
            economics = self.journal.economics_step(step['step_key'])
            if economics is not None:
                if economics['result_units'] is None:
                    self._record_execution_result(step['step_key'], result, result_ts=time.time())
                try:
                    self.journal.record_execution_measurement(step['step_key'],
                        terminal_observed_ts=order.get('terminal_received_ts'),
                        terminal_observed_mono_ns=order.get('terminal_received_mono_ns'),
                        terminal_clock_session_id=order.get('terminal_clock_session_id'),
                        runtime_consumed_ts=time.time(), runtime_consumed_mono_ns=time.monotonic_ns(),
                        runtime_consumed_clock_session_id=self.clock_session_id,
                        authoritative_reconcile_complete_ts=evidence.observed_ts,
                        authoritative_reconcile_complete_mono_ns=time.monotonic_ns())
                    self.journal.record_execution_attempt_timing(step['step_key'],
                        terminal_result_ts=order.get('terminal_received_ts'),
                        confirmation_source=order.get('terminal_source'),
                        result_status=order.get('terminal_status'))
                except Exception:
                    self.telemetry_errors += 1
        cycle = self.journal.active_live_cycle() or cycle
        try:
            await self._observe_active_risk(cycle)
        except (RuntimeHalt, ValueError):
            # A missing book cannot block an already latched reduce-only exit.
            pass
        if not any(expected.values()):
            self._flush_telemetry()
            await self._finish_flat(cycle, evidence=evidence)
            return None
        return cycle

    async def _basis_exit(self, cycle: dict, *, context: CloseContext | None = None) -> None:
        """One concurrent IOC wave; durable intent survives partial/UNKNOWN results."""
        assert self.instrument is not None and self.executor is not None
        expected_cycle_id = cycle['cycle_id']
        async with self._basis_exit_lock:
            self._recover_parallel_unknown()
            cycle = self.journal.active_live_cycle()
            if (cycle is None or cycle['cycle_id'] != expected_cycle_id
                    or cycle['phase'] != 'BASIS_EXIT'):
                return
            if self.journal.pending():
                self._position_audit_requested.set()
            cycle = await self._confirm_basis_exit(cycle, context)
            if cycle is None:
                return
            if not self.journal.pending() and time.monotonic() < self._basis_exit_retry_after:
                return
            expected = {'entropy': cycle['entropy_units'], 'hedge': cycle['hedge_units']}
            _, unproven = self._execution_positions(cycle)
            if self.stop.is_set():
                return
            planned = []
            steps = self.journal.live_steps(cycle['cycle_id'])
            submitted_refs = {o['order_ref'] for o in self.journal.cycle_orders(cycle['cycle_id'])}
            wave = len(steps)
            for key in ('entropy', 'hedge'):
                units = expected[key]
                if not units or key in unproven or any(o['venue'] == key for o in self.journal.pending()) \
                        or time.monotonic() < self._send_retry_after.get(key, 0.0):
                    continue
                side = Side.SELL if units > 0 else Side.BUY
                venue = getattr(self.venues, key)
                if not venue.book.is_fresh(self.settings.max_book_age_seconds):
                    continue
                rooms, cap = (self.settings.first_leg_execution_policy('close')
                              if key == 'entropy' else
                              self.settings.hedge_execution_policy('close'))
                attempt = sum(s['venue'] == key and ':close_pair_' in s['step_key']
                              and s['order_ref'] in submitted_refs for s in steps)
                room = rooms[min(attempt, len(rooms) - 1)]
                # The IOC may fill only depth inside this bounded marketable limit.
                try:
                    book = Book(_levels(venue.book.sorted_bids(), self.instrument.quantity_step),
                                _levels(venue.book.sorted_asks(), self.instrument.quantity_step),
                                time.time(), f'{key}:{venue.book.last_update_ts}')
                    limit_px = price_limit(book.top(side), book.top(side), side, room, cap,
                                           getattr(self.instrument, f'{key}_tick'))
                except (RuntimeHalt, ValueError):
                    continue
                purpose = f'close_pair_{key}_{wave}'
                ref = OrderRefFactory.create(venue.kind, deployment=self.deployment,
                                             cycle_id=cycle['cycle_id'], leg=f'{purpose}:0')
                step_key = f'{cycle["cycle_id"]}:{purpose}:0'
                trigger = self._exit_trigger
                if trigger is None:
                    economics = self.journal.db.execute(
                        'SELECT * FROM cycle_economics WHERE cycle_id=?', (cycle['cycle_id'],)).fetchone()
                    trigger = {'ts': economics['exit_trigger_ts'],
                               'basis': economics['exit_trigger_basis_bps'],
                               'mono_ns': economics['exit_trigger_mono_ns'],
                               'session_id': economics['exit_trigger_clock_session_id']}
                decision_ts, decision_mono = time.time(), time.monotonic_ns()
                benchmark = None
                try:
                    benchmark = self._execution_benchmark(cycle, venue_key=key, side=side,
                        units=abs(units), room_bps=room, hard_cap_bps=cap)
                except (RuntimeHalt, ValueError):
                    pass
                planned.append((Intent(ref, cycle['cycle_id'], key, side, abs(units), limit_px, True),
                                step_key, purpose, room, cap, benchmark, decision_ts, decision_mono))
            self.status.write('EXITING', 'basis_exit_reduce_only_ioc', cycle_id=cycle['cycle_id'],
                              basis_exit_requested_ts=cycle['basis_exit_requested_ts'],
                              entropy_units=expected['entropy'], hedge_units=expected['hedge'])
            if not planned:
                self._basis_exit_retry_after = time.monotonic() + .5
                return
            # Both durable steps exist before either task reaches its first send.
            timings = [{'_order_timing_context': dict(step_key=step_key, purpose=purpose,
                attempt=sum(s['venue'] == intent.venue and ':close_pair_' in s['step_key']
                            and s['order_ref'] in submitted_refs for s in steps))}
                for intent, step_key, purpose, *_ in planned]
            submit_quotes = [{} for _ in planned]
            def capture_submit(intent, quote):
                try:
                    market = self._market()
                    quote['ts'] = max(market.entropy.received_at, market.hedge.received_at)
                    quote['value'] = self._quote_snapshot(market,
                        entropy_side=intent.side if intent.venue == 'entropy' else intent.side.opposite,
                        units=intent.units, serialize=False)
                except (RuntimeHalt, ValueError):
                    pass
            self._invalidate_flat_lease()
            def persist_exit(intent, step_key):
                step = self.journal.prepare_dispatch(intent, step={
                    'step_key': step_key, 'execution_group_id': f'{cycle["cycle_id"]}:close_pair_{wave}',
                    'quantity_step': self.instrument.quantity_step})
                self.journal.bind_step_trigger(step_key, trigger)
                return step
            prepared = {intent.order_ref: persist_exit(intent, step_key)
                        for intent, step_key, *_ in planned}
            async def consume_exit(plan, timing, submit_quote, result):
                intent, step_key, purpose, room, cap, benchmark, decision_ts, decision_mono = plan
                consume_order_timing(self.brokers[intent.venue], intent, timing)
                if isinstance(result, BaseException):
                    self.status.write('EXITING', 'basis_exit_send_or_persistence_unknown',
                                      cycle_id=cycle['cycle_id'], venue=intent.venue, error=str(result))
                    return
                if result.deferred:
                    try:
                        self.journal.order(intent.order_ref)
                    except KeyError:
                        pass  # Executor already removed its proved-unsent dispatch.
                    else:
                        self.journal.discard_unsent_dispatch(intent)
                    self._send_retry_after[intent.venue] = time.monotonic() + result.retry_after
                    return
                active = self.journal.active_live_cycle()
                if (active is not None and active['cycle_id'] == cycle['cycle_id']
                        and active['phase'] == 'BASIS_EXIT'):
                    await self._confirm_basis_exit(active)
                try:
                    if benchmark is None:
                        return  # Partial depth or a stale peer: do not invent paired analytics.
                    self.journal.record_execution_benchmark(
                        step_key=step_key, cycle_id=cycle['cycle_id'], purpose=purpose,
                        venue=intent.venue, attempt=0, order_ref=intent.order_ref,
                        side=intent.side.value, requested_units=intent.units, room_bps=room,
                        hard_cap_bps=cap, submit_ts=float(timing.get('submit_start_ts', time.time())),
                        top_px=benchmark['top_px'], marginal_px=benchmark['marginal_px'],
                        limit_px=intent.limit_px, paired_executable_basis_bps=benchmark['paired_basis_bps'],
                        reference_bps=benchmark['reference_bps'], directional_dislocation_bps=benchmark['dislocation_bps'],
                        book_revision_entropy=benchmark['book_revision_entropy'],
                        book_revision_hedge=benchmark['book_revision_hedge'])
                    self.journal.record_execution_attempt_timing(step_key, **{
                        k: v for k, v in timing.items() if k in {
                            'submit_start_ts', 'terminal_result_ts', 'exchange_fill_ts',
                            'confirmation_source', 'result_status', 'resolve_start_ts', 'resolve_end_ts'}})
                    self.journal.record_close_submit(cycle['cycle_id'], venue=intent.venue,
                        submit_ts=float(timing.get('submit_start_ts', time.time())))
                    result_market = None
                    try:
                        result_market = self._market()
                    except (RuntimeHalt, ValueError):
                        pass
                    side = intent.side if intent.venue == 'entropy' else intent.side.opposite
                    decision_quote = self._quote_snapshot(benchmark['market'],
                        entropy_side=side, units=intent.units, serialize=False)
                    fill_quote = (self._quote_snapshot(result_market, entropy_side=side,
                        units=result.cumulative_units, serialize=False)
                        if result_market is not None and result.cumulative_units else None)
                    missing = [key for key, value in (
                        ('submit_quote', submit_quote.get('value')),
                        ('fill_mark_quote', fill_quote if result.cumulative_units else True),
                        ('exchange_fill_ts', timing.get('exchange_fill_ts')
                         if result.cumulative_units else True)) if value is None]
                    step = self.journal.live_step(step_key)
                    self.journal.record_execution_measurement(step_key, telemetry_version=2,
                        phase_kind=self._phase_kind(purpose), clock_session_id=self.clock_session_id,
                        quantity_step=self.instrument.quantity_step,
                        original_signal_ts=step['trigger_ts'], original_signal_basis_bps=step['trigger_basis_bps'],
                        attempt_decision_ts=decision_ts, attempt_decision_mono_ns=decision_mono,
                        attempt_decision_basis_bps=benchmark['paired_basis_bps'], decision_units=intent.units,
                        decision_quote_json=self._materialize_quote(decision_quote),
                        submit_quote_ts=submit_quote.get('ts'),
                        submit_quote_json=self._materialize_quote(submit_quote.get('value')),
                        submit_basis_bps=(submit_quote.get('value') or {}).get('basis_bps'),
                        submit_start_mono_ns=timing.get('submit_start_mono_ns'),
                        http_post_start_mono_ns=timing.get('http_post_start_mono_ns'),
                        terminal_observed_ts=timing.get('terminal_result_ts'),
                        terminal_observed_mono_ns=timing.get('terminal_result_mono_ns'),
                        terminal_clock_session_id=self.clock_session_id if result.terminal else None,
                        runtime_consumed_ts=time.time(), runtime_consumed_mono_ns=time.monotonic_ns(),
                        runtime_consumed_clock_session_id=self.clock_session_id,
                        exchange_first_fill_ts=timing.get('exchange_fill_ts'),
                        exchange_last_fill_ts=timing.get('exchange_last_fill_ts'),
                        fill_mark_quote_json=self._materialize_quote(fill_quote),
                        fill_mark_time_source='runtime_consumed_local' if fill_quote else None,
                        execution_path='parallel', measurement_quality='partial' if missing else 'complete',
                        missing_fields_json=missing)
                    if result.terminal:
                        self._record_execution_result(step_key, result, result_ts=time.time(),
                                                      result_market=result_market)
                except Exception:
                    self.telemetry_errors += 1
            self.changed.clear()
            tasks = []
            for plan, timing, quote in zip(planned, timings, submit_quotes):
                intent = plan[0]
                async def consume(result, plan=plan, timing=timing, quote=quote):
                    await consume_exit(plan, timing, quote, result)
                task = self._track_parallel(intent.order_ref,
                    self.executor.send(intent, timing=timing, completion=True,
                        persist_dispatch=lambda i=intent: prepared[i.order_ref],
                        before_submit=lambda i=intent, q=quote: capture_submit(i, q),
                        on_progress=self._order_progress), consume)
                tasks.append(task)
            await self._wait_parallel(tasks)
            if planned and not self.journal.pending():
                await self._confirm_basis_exit(self.journal.active_live_cycle() or cycle)
            if planned and self.journal.active_live_cycle() is not None:
                self.changed.set()

    async def _fetch_funding_rates(self) -> tuple:
        async def fetch(venue):
            try:
                return await asyncio.wait_for(venue.fetch_funding(include_price=True), timeout=5)
            except Exception:
                return None
        rates = tuple(await asyncio.gather(fetch(self.venues.entropy),
                                          fetch(self.venues.hedge)))
        self.status.base['funding_rates'] = {
            key: ({'rate_bps_per_hour': str(rate.rate_bps_per_hour),
                   'index_price': str(rate.index_price) if rate.index_price is not None else None,
                   'source_ts': rate.ts, 'source': rate.source} if rate is not None else None)
            for key, rate in zip(('entropy', 'hedge'), rates)}
        return rates

    def _funding_open_plan(self, market: Market, *, units: int | None = None,
                           direction: Direction | None = None) -> OpenDecision | None:
        assert self.instrument is not None
        if any(r is None or r.index_price is None for r in self._funding_rates):
            return None
        selected = entry_direction(
            *(r.index_price for r in self._funding_rates), *self._funding_rates,
            now=time.time(), stale_seconds=self.settings.funding_stale_seconds)
        if selected is None or (direction is not None and selected is not direction):
            return None
        side = selected.entropy_side
        step = self.instrument.quantity_step
        lot_units = int((self._funding_common_step or step) / step)
        worst_price = max(market.entropy.top(side), market.hedge.top(side.opposite))
        cap_units = int((self.settings.max_notional_usd / worst_price / step)
                        .to_integral_value(rounding=ROUND_FLOOR))
        depth = min(sum(q for _, q in (market.entropy.asks if side is Side.BUY
                                      else market.entropy.bids)),
                    sum(q for _, q in (market.hedge.bids if side is Side.BUY
                                      else market.hedge.asks)))
        minimum = max(_minimum_units(self.venues.entropy, self.instrument, market.entropy.mid),
                      _minimum_units(self.venues.hedge, self.instrument, market.hedge.mid))
        upper = min(depth, cap_units) if units is None else units
        if units is not None and units % lot_units:
            return None
        upper -= upper % lot_units
        rooms, cap = self.settings.first_leg_execution_policy('open')
        for count in range(upper, minimum - 1, -lot_units):
            try:
                ec, marginal = market.entropy.quote(side, count, step)
                hc, hedge_marginal = market.hedge.quote(side.opposite, count, step)
                top = market.entropy.top(side)
                limit = price_limit(top, marginal, side, rooms[0], cap,
                                    self.instrument.entropy_tick)
                hedge_limit = price_limit(
                    market.hedge.top(side.opposite), hedge_marginal, side.opposite,
                    self.settings.hedge_execution_policy('open')[0][-1],
                    self.settings.hedge_execution_policy('open')[1],
                    self.instrument.hedge_tick)
                notional = max(ec, hc, limit * count * step, hedge_limit * count * step)
                if notional > self.settings.max_notional_usd:
                    if units is not None:
                        return None
                    continue
            except ValueError:
                if units is not None:
                    return None
                continue
            basis = (ec / hc - 1) * BPS
            fees = (ec * self.instrument.entropy_fee_bps
                    + hc * self.instrument.hedge_fee_bps) / BPS
            return OpenDecision(
                selected, count, market.mid_basis_bps, basis, Decimal('0'), notional,
                Decimal('0'), Decimal('0'), (hc - ec if side is Side.BUY else ec - hc),
                fees, top, marginal, limit, market.entropy.revision, market.hedge.revision,
                time.time(), time.monotonic_ns(), market)
        return None

    def _confirm_funding_hedged(self, cycle: dict) -> None:
        if self.settings.strategy_mode != 'funding_hedge':
            return
        self.journal.ensure_funding_cycle(cycle['cycle_id'], hedged_ts=None)
        state = self.journal.funding_state(cycle['cycle_id'])
        if (not state['exit_reason'] and cycle['entropy_units'] != 0
                and cycle['entropy_units'] == -cycle['hedge_units']
                and cycle['position_evidence_authoritative'] and not self.journal.pending()):
            self.journal.ensure_funding_cycle(cycle['cycle_id'], hedged_ts=time.time())

    def _latch_funding_exit(self, cycle: dict, reason: str) -> None:
        state = self.journal.ensure_funding_cycle(cycle['cycle_id'], hedged_ts=None)
        # Risk can override an already requested normal close, never the reverse.
        if not state['exit_reason'] or reason.startswith('liquidation_'):
            self.journal.set_funding_exit(
                cycle['cycle_id'], reason=reason,
                completed=settlements_complete(state, self.settings.funding_settlements_required))
        if reason != 'two_settlements_each':
            self.journal.close_entries(
                cycle['batch_id'], 'FUNDING_EARLY_EXIT', cycle_id=cycle['cycle_id'])
        self._funding_exit_signal(cycle['cycle_id']).set()
        self.changed.set()

    def _funding_completion_fields(self, cycle_id: str) -> dict:
        state = self.journal.funding_state(cycle_id)
        completed = bool(state and state['completed'])
        return {'funding_state': state, 'two_cycles_completed': completed,
                'funding_completion_label': '两周期完成' if completed else '两周期未完成',
                'funding_exit_reason': state['exit_reason'] if state else None}

    def _funding_exit_signal(self, cycle_id: str) -> asyncio.Event:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if (self._funding_exit_event_cycle_id != cycle_id
                or (loop is not None and self._funding_exit_event_loop is not loop)):
            self._funding_exit_event_cycle_id = cycle_id
            self._funding_exit_event_loop = loop
            self._funding_exit_event = asyncio.Event()
        if self._risk_exit_reason(cycle_id):
            self._funding_exit_event.set()
        return self._funding_exit_event

    def _risk_exit_reason(self, cycle_id: str) -> str | None:
        if self.settings.strategy_mode == 'funding_hedge':
            state = self.journal.funding_state(cycle_id)
            return state['exit_reason'] if state else None
        row = self.journal.db.execute(
            'SELECT risk_exit_reason FROM live_cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
        return row['risk_exit_reason'] if row else None

    def _latch_liquidation_exit(self, cycle: dict, reason: str) -> None:
        first = not self._risk_exit_reason(cycle['cycle_id'])
        if self.settings.strategy_mode == 'funding_hedge':
            self._latch_funding_exit(cycle, reason)
        else:
            self.journal.latch_basis_exit(cycle['cycle_id'], requested_ts=time.time(), reason=reason)
            self.journal.close_entries(cycle['batch_id'], 'POSITION_RISK', cycle_id=cycle['cycle_id'])
            self._funding_exit_signal(cycle['cycle_id']).set()
            self.changed.set()
        if first:
            self._risk_trigger = dict(cycle_id=cycle['cycle_id'], reason=reason,
                ts=time.time(), mono_ns=time.monotonic_ns(), clock_session_id=self.clock_session_id)
            event = dict(kind='v3_liquidation_trigger', mode=self.settings.strategy_mode,
                         **self._risk_trigger)
            self._publish_protection(event)

    def _publish_protection(self, event: dict) -> None:
        try:
            self.observer.publish(event)
        except Exception:
            self.telemetry_errors += 1

    def _protection_heartbeat(self, cycle: dict | None) -> None:
        self.status.base['liquidation_protection'] = {
            'enabled': True, 'mode': self.settings.strategy_mode,
            'threshold': str(self.settings.funding_liquidation_distance),
            'request_timeout_seconds': FUNDING_RISK_REQUEST_TIMEOUT_SECONDS,
            'failure_retry_limit': 1,
            'failure_retry_timeout_seconds': FUNDING_RISK_RETRY_TIMEOUT_SECONDS,
            'data_max_age_seconds': self.settings.funding_risk_stale_seconds,
            'poll_seconds': self.settings.funding_risk_poll_seconds,
            'heartbeat_ts': time.time(), 'heartbeat_mono_ns': time.monotonic_ns(),
            'clock_session_id': self.clock_session_id,
            'cycle_id': cycle['cycle_id'] if cycle else None}
        risk = self.status.base.get('funding_risk')
        if risk is not None:
            for observation in risk.get('legs', {}).values():
                if observation is not None:
                    observation['data_age_seconds'] = time.time() - observation['source_ts']
        snapshot = self.status.current_snapshot
        if snapshot is not None:
            reserved = {*self.status.base, 'current_state', 'reason', 'source_ts', 'status_writer'}
            self.status.write(snapshot['current_state'], snapshot['reason'],
                **{key: value for key, value in snapshot.items() if key not in reserved})

    async def _owner_heartbeat_loop(self) -> None:
        while not self.stop.is_set():
            cycle = self.journal.active_live_cycle()
            self._publish_protection(dict(kind='v3_owner_heartbeat', ts=time.time(),
                mono_ns=time.monotonic_ns(), clock_session_id=self.clock_session_id,
                cycle_id=cycle['cycle_id'] if cycle else None,
                phase=cycle['phase'] if cycle else None,
                liquidation_protection=self.status.base.get('liquidation_protection'),
                funding_risk=self.status.base.get('funding_risk')))
            try:
                await asyncio.wait_for(self.stop.wait(), self.settings.funding_risk_poll_seconds)
            except asyncio.TimeoutError:
                pass

    async def _basis_risk_exit(self, cycle_id: str) -> None:
        cycle = self.journal.active_live_cycle()
        if (cycle is None or cycle['cycle_id'] != cycle_id or self.stop.is_set()
                or not cycle.get('risk_exit_reason') or cycle['phase'] != 'BASIS_EXIT'):
            return
        outcome = None
        try:
            await self._basis_exit(cycle)
        except RuntimeHalt:
            outcome = 'reconciliation_required'
            self._position_audit_requested.set()
            self.status.write('RECONCILE_REQUIRED', 'liquidation_exit_reconciliation_required',
                              active_cycle=self.journal.active_live_cycle())
        except BaseException:
            outcome = 'interrupted_or_error'
            raise
        finally:
            trigger = self._risk_trigger
            finished_mono_ns = time.monotonic_ns()
            elapsed = ((finished_mono_ns - trigger['mono_ns']) / 1e9
                       if trigger and trigger['cycle_id'] == cycle_id
                       and trigger['clock_session_id'] == self.clock_session_id else None)
            active = self.journal.active_live_cycle()
            row = self.journal.db.execute(
                'SELECT * FROM live_cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
            pending = [dict(order_ref=o['order_ref'], venue=o['venue'], state=o['state'],
                            filled_units=o['filled_units'])
                       for o in self.journal.pending() if o['cycle_id'] == cycle_id]
            flat_confirmed = bool(row and row['phase'] == 'DONE'
                and row['entropy_units'] == row['hedge_units'] == 0
                and row['position_evidence_authoritative'] and not pending)
            if outcome is None:
                outcome = ('flat_confirmed' if flat_confirmed else
                           'flat_pending_reconcile' if row
                           and row['entropy_units'] == row['hedge_units'] == 0 else
                           'wave_progress')
            evidence = self._last_position_evidence
            event = dict(kind='v3_liquidation_exit_result', cycle_id=cycle_id,
                ts=time.time(), mono_ns=finished_mono_ns, clock_session_id=self.clock_session_id,
                trigger_to_wave_result_seconds=elapsed, result_scope='exit_wave_return', outcome=outcome,
                active_cycle_id=active['cycle_id'] if active else None,
                phase=row['phase'] if row else None,
                entropy_units=row['entropy_units'] if row else None,
                hedge_units=row['hedge_units'] if row else None,
                flat_confirmed=flat_confirmed, pending_orders=pending,
                orders=[dict(order_ref=o['order_ref'], venue=o['venue'], state=o['state'],
                             filled_units=o['filled_units']) for o in self.journal.cycle_orders(cycle_id)],
                confirmation_source=(dict(cycle_id=evidence.cycle_id,
                    entropy=evidence.entropy_source, hedge=evidence.hedge_source,
                    fully_authoritative=evidence.fully_authoritative,
                    observed_ts=evidence.observed_ts) if evidence else None))
            self._publish_protection(event)

    def _start_basis_risk_exit(self, cycle: dict) -> None:
        if self.settings.strategy_mode != 'basis' or not cycle.get('risk_exit_reason'):
            return
        task = self._basis_risk_exit_task
        if task is not None:
            if not task.done():
                return
            if not task.cancelled() and task.exception() is not None:
                raise task.exception()
        self._basis_risk_exit_task = asyncio.create_task(
            self._basis_risk_exit(cycle['cycle_id']), name='basis-liquidation-exit')

    def _funding_risk_identity(self, cycle: dict) -> tuple:
        # The order watermark also rejects A -> B -> A position changes.
        orders = tuple((row['order_ref'], row['state'], row['filled_units'])
                       for row in self.journal.cycle_orders(cycle['cycle_id']))
        return cycle['cycle_id'], cycle['entropy_units'], cycle['hedge_units'], orders

    def _funding_risk_current(self, signature: tuple, generation: int) -> dict | None:
        cycle = self.journal.active_live_cycle()
        if (cycle is None or generation != self._funding_risk_generation
                or self._funding_risk_identity(cycle) != signature):
            return None
        return cycle

    @staticmethod
    def _funding_risk_error(exc: Exception) -> str:
        # Never persist exception text, URLs, headers or account identifiers.
        if isinstance(exc, TimeoutError):
            return 'timeout'
        name = type(exc).__name__
        if name == 'LighterWafBlocked':
            return 'waf_captcha_or_backoff'
        status = getattr(exc, 'status', None)
        if type(status) is int and status:
            return 'http_auth' if status in (401, 403) else 'http_error'
        if name in {'ClientConnectorError', 'ClientOSError', 'ServerDisconnectedError',
                    'ClientConnectionError', 'ClientPayloadError'}:
            return 'transport'
        if isinstance(exc, (KeyError, ValueError, TypeError)):
            return 'schema_or_value_invalid'
        for text, code in (('stale or future', 'stale_or_future_timestamp'),
                           ('positions missing', 'positions_missing'),
                           ('mark unavailable', 'mark_missing'),
                           ('malformed', 'snapshot_schema_invalid')):
            if text in str(exc):
                return code
        return 'adapter_error'

    def _funding_risk_invalid(self, snapshot, cycle: dict, key: str) -> str | None:
        if not isinstance(snapshot, FundingRiskSnapshot):
            return 'snapshot_schema_invalid'
        age = time.time() - snapshot.ts
        if not math.isfinite(age) or age < 0:
            return 'future_timestamp'
        if age > self.settings.funding_risk_stale_seconds:
            return 'stale_timestamp'
        assert self.instrument is not None
        if snapshot.position_qty != self.instrument.quantity_step * cycle[f'{key}_units']:
            return 'position_mismatch'
        if snapshot.position_qty != 0 and snapshot.liquidation_price is None:
            return 'liquidation_missing'
        return None

    def _funding_cached_risk(self, key: str, cycle: dict):
        cached = self._funding_risk_cache.get(key)
        if (cached is None or cached[0] != self._funding_risk_identity(cycle)
                or time.monotonic() > cached[2]
                or self._funding_risk_invalid(cached[1], cycle, key)):
            return None
        return cached[1]

    def _track_funding_read(self, task: asyncio.Task) -> asyncio.Task:
        self._funding_reads.add(task)
        def completed(done):
            self._funding_reads.discard(done)
            if not done.cancelled():
                done.exception()  # Drain late errors; never adopt a late response.
        task.add_done_callback(completed)
        return task

    async def _funding_risk_leg(self, key: str, venue, signature: tuple,
                                generation: int, attempt: int) -> None:
        started_ts, started_mono = time.time(), time.monotonic()
        cycle_id = signature[0]
        event = {'kind': 'v3_funding_risk', 'cycle_id': cycle_id, 'venue': key,
                 'attempt': attempt, 'request_started_ts': started_ts,
                 'request_started_mono_ns': time.monotonic_ns(),
                 'clock_session_id': self.clock_session_id, 'mode': self.settings.strategy_mode,
                 'request_timeout_seconds': FUNDING_RISK_REQUEST_TIMEOUT_SECONDS,
                 'endpoints': (['POST /info clearinghouseState', 'POST /info metaAndAssetCtxs']
                               if key == 'entropy' else
                               ['GET /api/v1/account', 'GET /api/v1/orderBookDetails'])}
        snapshot, error, http_status = None, None, None
        first_failure = None
        timeouts = (FUNDING_RISK_REQUEST_TIMEOUT_SECONDS,
                    min(FUNDING_RISK_RETRY_TIMEOUT_SECONDS, FUNDING_RISK_REQUEST_TIMEOUT_SECONDS))
        for read_attempt, timeout in enumerate(timeouts, 1):
            event['read_attempts'] = read_attempt
            self._publish_protection(dict(event, stage='started', read_attempt=read_attempt,
                request_timeout_seconds=timeout, request_read_started_ts=time.time(),
                request_read_started_mono_ns=time.monotonic_ns()))
            read = self._track_funding_read(asyncio.create_task(venue.fetch_funding_risk()))
            snapshot, error, http_status = None, None, None
            try:
                done, _ = await asyncio.wait({read}, timeout=timeout)
                if not done:
                    read.cancel()
                    error = 'timeout'
                else:
                    try:
                        snapshot = read.result()
                    except Exception as exc:
                        error = self._funding_risk_error(exc)
                        status = getattr(exc, 'status', None)
                        http_status = status if type(status) is int else None
            except asyncio.CancelledError:
                read.cancel()
                self._publish_protection(dict(event, stage='cancelled', checked_at=time.time(),
                    result_mono_ns=time.monotonic_ns(),
                    request_latency_seconds=time.monotonic() - started_mono,
                    failure_code='request_superseded_or_exit'))
                raise
            cycle = self._funding_risk_current(signature, generation)
            if cycle is None:
                break
            if not error:
                error = self._funding_risk_invalid(snapshot, cycle, key)
                previous = self._funding_risk_cache.get(key)
                if not error and previous is not None and snapshot.ts < previous[1].ts:
                    error = 'older_source_timestamp'
            if error and first_failure is None:
                first_failure = time.time(), time.monotonic()
            if (not error or error == 'position_mismatch' or read_attempt == 2
                    or self._risk_exit_reason(cycle_id)):
                break
            # One supplementary read; neither this wait nor a fresh heartbeat
            # extends a previously accepted snapshot's original deadline.
            self._publish_protection(dict(event, stage='retry', checked_at=time.time(),
                failure_code=error, retry_timeout_seconds=timeouts[1]))
        cycle = self._funding_risk_current(signature, generation)
        source_ts = snapshot.ts if isinstance(snapshot, FundingRiskSnapshot) else None
        event.update(stage='finished', checked_at=time.time(),
                     result_mono_ns=time.monotonic_ns(),
                     request_latency_seconds=time.monotonic() - started_mono,
                     http_status=http_status, source_ts=source_ts,
                     data_age_seconds=(time.time() - source_ts if source_ts is not None else None))
        if cycle is None:
            event.update(outcome='discarded', failure_code='cycle_or_position_changed')
            self._publish_protection(event)
            return
        if error == 'position_mismatch':
            self._funding_risk_cache.pop(key, None)
        used_cache = False
        if error:
            previous = self._funding_risk_failures.get(key)
            failed_since, failed_mono, count = (previous if previous else
                                               (*first_failure, 0))
            self._funding_risk_failures[key] = failed_since, failed_mono, count + 1
            event.update(failure_code=error, failure_since_ts=failed_since,
                         failure_duration_seconds=time.monotonic() - failed_mono,
                         consecutive_failures=count + 1)
            snapshot = self._funding_cached_risk(key, cycle)
            used_cache = snapshot is not None
        else:
            previous = self._funding_risk_failures.pop(key, None)
            event.update(failure_code=None, consecutive_failures=0,
                         recovered_failure_duration_seconds=(time.monotonic() - previous[1]
                                                             if previous else None))
            if not self._risk_exit_reason(cycle_id):
                age = time.time() - snapshot.ts
                expiry = time.monotonic() + self.settings.funding_risk_stale_seconds - age
                previous_cache = self._funding_risk_cache.get(key)
                if previous_cache and snapshot.ts == previous_cache[1].ts:
                    expiry = min(expiry, previous_cache[2])
                self._funding_risk_cache[key] = signature, snapshot, expiry
                self._funding_risk_changed.set()
        distance = (liquidation_distance(
            snapshot, now=time.time(), stale_seconds=self.settings.funding_risk_stale_seconds)
            if snapshot is not None else None)
        observation = ({'position_qty': str(snapshot.position_qty),
                        'mark_price': str(snapshot.mark_price),
                        'liquidation_price': (str(snapshot.liquidation_price)
                                              if snapshot.liquidation_price is not None else None),
                        'source_ts': snapshot.ts,
                        'source': ('hyperliquid:clearinghouseState+metaAndAssetCtxs'
                                   if key == 'entropy' else 'lighter:account+orderBookDetails'),
                        'data_age_seconds': time.time() - snapshot.ts,
                        'distance': str(distance) if distance is not None else None,
                        'cache_used': used_cache} if snapshot is not None else None)
        event.update(outcome='cache' if used_cache else 'error' if error else 'fresh',
                     phase=cycle['phase'], expected_units=cycle[f'{key}_units'],
                     cache_used=used_cache, observation=observation)
        if distance is not None and distance <= self.settings.funding_liquidation_distance:
            self._latch_liquidation_exit(cycle, f'liquidation_{key}')
        elif ((snapshot is None or (cycle[f'{key}_units'] != 0 and distance is None))
              and not self._risk_exit_reason(cycle_id)):
            self._latch_liquidation_exit(cycle, 'risk_data_unavailable')
        risk = self.status.base.get('funding_risk', {})
        legs = dict(risk.get('legs', {})) if risk.get('cycle_id') == cycle_id else {}
        legs[key] = observation
        self.status.base['funding_risk'] = {
            'cycle_id': cycle_id, 'checked_at': time.time(), 'legs': legs}
        event['exit_reason'] = self._risk_exit_reason(cycle_id)
        self._publish_protection(event)

    def _expire_failed_funding_cache(self, cycle: dict) -> None:
        if self._risk_exit_reason(cycle['cycle_id']):
            return
        for key in self._funding_risk_cache.keys() | self._funding_risk_failures.keys():
            if self._funding_cached_risk(key, cycle) is None:
                self._latch_liquidation_exit(cycle, 'risk_data_unavailable')
                cached = self._funding_risk_cache.get(key)
                failed_since, failed_mono, count = self._funding_risk_failures.get(
                    key, (time.time(), time.monotonic(), 0))
                event = {'kind': 'v3_funding_risk_expiry', 'cycle_id': cycle['cycle_id'],
                         'venue': key, 'checked_at': time.time(),
                         'mono_ns': time.monotonic_ns(), 'clock_session_id': self.clock_session_id,
                         'failure_code': 'cached_evidence_unavailable',
                         'failure_since_ts': failed_since, 'consecutive_failures': count,
                         'failure_duration_seconds': time.monotonic() - failed_mono,
                         'source_ts': cached[1].ts if cached else None,
                         'data_age_seconds': time.time() - cached[1].ts if cached else None,
                         'exit_reason': 'risk_data_unavailable'}
                self._publish_protection(event)
                return

    async def _check_funding_liquidation(self, cycle: dict, *, wait: bool = True) -> None:
        async with self._funding_risk_lock:
            current = self.journal.active_live_cycle()
            if current is None or current['cycle_id'] != cycle['cycle_id']:
                return
            cycle = current
            signal = self._funding_exit_signal(cycle['cycle_id'])
            if signal.is_set():
                return
            signature = self._funding_risk_identity(cycle)
            if signature != self._funding_risk_signature:
                for task in self._funding_risk_tasks.values():
                    task.cancel()
                self._funding_risk_generation += 1
                self._funding_risk_signature = signature
                self._funding_risk_tasks.clear()
                self._funding_risk_started.clear()
                self._funding_risk_cache.clear()
                self._funding_risk_failures.clear()
                self.status.base.pop('funding_risk', None)
            self._expire_failed_funding_cache(cycle)
            if signal.is_set():
                return
            now = time.time()
            for key, venue in (('entropy', self.venues.entropy), ('hedge', self.venues.hedge)):
                # Funding entry still validates the flat account before its first order.
                if (self.settings.strategy_mode == 'basis' and not cycle[f'{key}_units']
                        and not any(row['venue'] == key for row in self.journal.pending())):
                    continue
                task = self._funding_risk_tasks.get(key)
                if task is not None:
                    if not task.done():
                        continue
                    if not task.cancelled() and task.exception() is not None:
                        raise task.exception()
                if 0 <= now - self._funding_risk_started.get(key, 0) < self.settings.funding_risk_poll_seconds:
                    continue
                self._funding_risk_attempt += 1
                self._funding_risk_started[key] = now
                self._funding_risk_ts = now
                self._funding_risk_tasks[key] = asyncio.create_task(self._funding_risk_leg(
                    key, venue, signature, self._funding_risk_generation, self._funding_risk_attempt))
            pending = {task for task in self._funding_risk_tasks.values() if not task.done()}
            generation = self._funding_risk_generation
        if not wait or not pending:
            return
        exited = asyncio.create_task(signal.wait())
        try:
            while pending and not signal.is_set():
                # A failed read may use cache only until its original expiry.
                expiries = [v[2] for v in self._funding_risk_cache.values()]
                timeout = max(.001, min(expiries) - time.monotonic()) if expiries else None
                done, _ = await asyncio.wait(pending | {exited}, timeout=timeout,
                                             return_when=asyncio.FIRST_COMPLETED)
                pending -= done
                current = self._funding_risk_current(signature, generation)
                if current is None:
                    return
                self._expire_failed_funding_cache(current)
                for task in done - {exited}:
                    if not task.cancelled() and task.exception() is not None:
                        raise task.exception()
        finally:
            exited.cancel()

    async def _funding_risk_loop(self) -> None:
        while not self.stop.is_set():
            self._funding_risk_changed.clear()
            cycle = self.journal.active_live_cycle()
            self._protection_heartbeat(cycle)
            if cycle is not None:
                await self._check_funding_liquidation(cycle, wait=False)
                cycle = self.journal.active_live_cycle()
                if cycle is not None:
                    self._start_basis_risk_exit(cycle)
            delay = self.settings.funding_risk_poll_seconds
            expiries = ([v[2] for v in self._funding_risk_cache.values()]
                        if cycle is not None
                        and not self._funding_exit_signal(cycle['cycle_id']).is_set() else [])
            if expiries:
                delay = min(delay, max(.001, min(expiries) - time.monotonic()))
            stopped = asyncio.create_task(self.stop.wait())
            refreshed = asyncio.create_task(self._funding_risk_changed.wait())
            signal = self._funding_exit_signal(cycle['cycle_id']) if cycle else None
            exited = asyncio.create_task(signal.wait()) if signal and not signal.is_set() else None
            try:
                await asyncio.wait({stopped, refreshed, exited} if exited else {stopped, refreshed}, timeout=delay,
                                   return_when=asyncio.FIRST_COMPLETED)
            finally:
                stopped.cancel()
                refreshed.cancel()
                if exited is not None:
                    exited.cancel()

    async def _funding_hold_read(self, cycle: dict, read: Callable) -> Any:
        signal = self._funding_exit_signal(cycle['cycle_id'])
        if signal.is_set():
            return None
        task = self._track_funding_read(asyncio.create_task(read()))
        exited = asyncio.create_task(signal.wait())
        try:
            await asyncio.wait({task, exited}, return_when=asyncio.FIRST_COMPLETED)
            if signal.is_set():
                return None
            return task.result()
        finally:
            exited.cancel()
            if not task.done():
                task.cancel()  # Only readonly funding/rates IO, never an order task.

    async def _poll_funding_settlements(self, cycle: dict) -> dict:
        self.journal.ensure_funding_cycle(cycle['cycle_id'], hedged_ts=None)
        until = (float(cycle['updated_ts']) if cycle['phase'] == 'DONE' else time.time())
        async def fetch(venue):
            try:
                return await asyncio.wait_for(venue.fetch_funding_settlements(
                    float(cycle['started_ts']), until), timeout=5)
            except Exception:
                return None
        records = await asyncio.gather(fetch(self.venues.entropy), fetch(self.venues.hedge))
        for key, rows in zip(('entropy', 'hedge'), records):
            if rows is None:
                continue
            # Same account/market is exclusive. Timestamp and position sign are
            # nevertheless checked so a prior position's delayed row is not adopted.
            expected = Direction(cycle['direction']).entropy_side.sign * (1 if key == 'entropy' else -1)
            selected = []
            for row in rows:
                if not float(cycle['started_ts']) <= row.ts <= until:
                    continue
                if row.position_qty is not None and row.position_qty * expected <= 0:
                    continue
                selected.append(row)
            self.journal.record_funding_settlements(cycle['cycle_id'], key, selected)
        self.journal.record_funding_poll(
            cycle['cycle_id'], polled_ts=time.time(),
            entropy_ok=records[0] is not None, hedge_ok=records[1] is not None,
            closed_window=cycle['phase'] == 'DONE')
        return self.journal.funding_state(cycle['cycle_id'])

    async def _funding_hold(self, cycle: dict) -> None:
        state = self.journal.funding_state(cycle['cycle_id'])
        if time.time() - self._funding_poll_ts >= self.settings.funding_poll_seconds:
            self._funding_poll_ts = time.time()
            await self._funding_hold_read(cycle, lambda: self._poll_funding_settlements(cycle))
            state = self.journal.funding_state(cycle['cycle_id'])
            # Completion does not wait for funding rates to recover or load.
            if settlements_complete(state, self.settings.funding_settlements_required):
                self._latch_funding_exit(cycle, 'two_settlements_each')
            if self.journal.funding_state(cycle['cycle_id'])['exit_reason']:
                await self._funding_exit(cycle)
                return
            else:
                rates = await self._funding_hold_read(cycle, self._fetch_funding_rates)
                if self.journal.funding_state(cycle['cycle_id'])['exit_reason']:
                    await self._funding_exit(cycle)
                    return
                self._funding_rates = rates
                net = None
                if all(r is not None and r.index_price is not None for r in self._funding_rates):
                    net = pair_income_usd_per_hour(
                        self.instrument.quantity_step * cycle['entropy_units'],
                        self.instrument.quantity_step * cycle['hedge_units'],
                        *(r.index_price for r in self._funding_rates), *self._funding_rates,
                        now=time.time(), stale_seconds=self.settings.funding_stale_seconds)
                sample_ts = (min(r.ts for r in self._funding_rates) if net is not None else time.time())
                state = self.journal.record_funding_sample(
                    cycle['cycle_id'], sample_ts=sample_ts, net_usd_per_hour=net,
                    max_gap_seconds=self.settings.funding_poll_seconds * 1.5)
        state = self.journal.funding_state(cycle['cycle_id'])
        if state['exit_reason']:
            await self._funding_exit(cycle)
            return
        active_net = self._mark_to_flat(cycle, self._market())
        self.journal.observe_open_risk(
            self.authorization.batch_id, active_cycle_net_pnl_usd=active_net,
            cycle_id=cycle['cycle_id'])
        batch = self.journal.batch_state(self.authorization.batch_id)
        cost = None
        if not state['exit_reason']:
            loss_limit = self.settings.funding_round_loss_limit_usd
            if (loss_limit is not None and active_net <= -loss_limit) or batch['close_reason'] in {
                    'SINGLE_CYCLE_REVIEW_LOSS', 'REALIZED_LOSS_BUDGET'}:
                self._latch_funding_exit(cycle, 'round_loss_limit')
            elif settlements_complete(state, self.settings.funding_settlements_required):
                self._latch_funding_exit(cycle, 'two_settlements_each')
            elif (all(r is not None and r.is_fresh(
                    time.time(), self.settings.funding_stale_seconds) for r in self._funding_rates)
                  and reversal_confirmed(state, samples=self.settings.funding_reversal_samples,
                                         seconds=self.settings.funding_reversal_seconds)):
                rate = state['last_net_usd_per_hour']
                cost = remaining_cost_usd(rate, state['settlement_counts'],
                                          self.settings.funding_settlements_required)
                if cost > self.settings.funding_remaining_cost_limit_usd:
                    self._latch_funding_exit(cycle, 'funding_reversal_cost')
                elif loss_limit is not None and active_net - cost <= -loss_limit:
                    self._latch_funding_exit(cycle, 'projected_round_loss_limit')
        state = self.journal.funding_state(cycle['cycle_id'])
        if time.time() - self._funding_hold_observed_ts >= self.settings.funding_risk_poll_seconds:
            self._funding_hold_observed_ts = time.time()
            event = {'kind': 'v3_funding_hold', 'cycle_id': cycle['cycle_id'],
                     'phase': cycle['phase'], 'observed_at': time.time(),
                     'entropy_units': cycle['entropy_units'], 'hedge_units': cycle['hedge_units'],
                     'executable_net_usd': str(active_net),
                     'funding_state': state, 'funding_risk': self.status.base.get('funding_risk')}
            self._enqueue_telemetry(lambda: self.observer.publish(event))
        self.status.write('EXITING' if state['exit_reason'] else 'HOLDING_FUNDING',
                          state['exit_reason'] or 'await_actual_settlements',
                          cycle_id=cycle['cycle_id'], funding_state=state,
                          expected_remaining_funding_cost_usd=cost,
                          executable_net_usd=active_net,
                          two_cycles_completed=settlements_complete(
                              state, self.settings.funding_settlements_required))
        if state['exit_reason']:
            await self._funding_exit(cycle)

    async def _funding_exit(self, cycle: dict) -> None:
        """Persisted, signal-independent, reduce-only campaign on BOTH venues."""
        await self._resolve_pending()
        cycle = await self._sync_positions(self.journal.active_live_cycle() or cycle)
        cycle = await self._apply_resolved_steps(cycle)
        state = self.journal.funding_state(cycle['cycle_id'])
        order = ('hedge', 'entropy') if state['exit_reason'] == 'liquidation_hedge' else ('entropy', 'hedge')
        self.journal.update_live_cycle(cycle['cycle_id'], phase='RECOVERY', updated_ts=time.time())
        for key in order:
            cycle = self.journal.active_live_cycle() or cycle
            units = cycle[f'{key}_units']
            if not units:
                continue
            if any(o['venue'] == key and not o['reduce_only'] for o in self.journal.pending()):
                continue  # Do not invent UNKNOWN opening fills or bypass same-venue exclusion.
            try:
                await self._campaign(
                    cycle, purpose=f'funding_exit_{key}_{len(self.journal.live_steps(cycle["cycle_id"]))}',
                    venue_key=key, side=Side.SELL if units > 0 else Side.BUY,
                    units=abs(units), reduce_only=True, rooms=self.settings.hedge_rooms_bps,
                    hard_cap=self.settings.hedge_hard_cap_bps, next_phase='RECOVERY', completion=True)
            except (RuntimeHalt, ValueError) as exc:
                self.status.write('EXITING', 'reduce_only_leg_retry_required',
                                  venue=key, error=str(exc), cycle_id=cycle['cycle_id'])
            cycle = await self._sync_positions(self.journal.active_live_cycle() or cycle)
        if not cycle['entropy_units'] and not cycle['hedge_units']:
            if self.journal.pending():
                evidence = await self._position_evidence(cycle)
                if self._prove_zero_fill_pending_reductions(cycle, evidence):
                    cycle = await self._apply_resolved_steps(cycle)
            if not self.journal.pending():
                await self._finish_flat(cycle)
                return
        self.status.write('EXITING', state['exit_reason'], cycle_id=cycle['cycle_id'],
                          pending_orders=len(self.journal.pending()),
                          **self._funding_completion_fields(cycle['cycle_id']))

    async def _resume_cycle_inner(self, cycle: dict) -> None:
        if self.settings.strategy_mode == 'basis':
            self._ensure_position_anchor(cycle)
            if self._route_external_residual(cycle, self._last_position_evidence):
                cycle = self.journal.active_live_cycle() or cycle
        if self.settings.strategy_mode == 'basis' and cycle['phase'] == 'BASIS_EXIT':
            await self._basis_exit(cycle)
            return
        if self.settings.strategy_mode == 'funding_hedge':
            state = self.journal.funding_state(cycle['cycle_id'])
            if state and state['exit_reason']:
                await self._funding_exit(cycle)
                return
            # Readonly risk IO cannot block an already requested exit or holding decisions.
            # Campaigns which increase exposure still wait for the independent risk checks.
            await self._check_funding_liquidation(cycle, wait=False)
            state = self.journal.funding_state(cycle['cycle_id'])
            if state and state['exit_reason']:
                await self._funding_exit(cycle)
                return
        if self.settings.strategy_mode == 'basis':
            if self.journal.pending():
                self._position_audit_requested.set()
        else:
            await self._resolve_pending()
        cycle = await self._sync_positions(self.journal.active_live_cycle() or cycle)
        if self.settings.strategy_mode == 'basis' and cycle['phase'] == 'OPEN':
            e_units, h_units = cycle['entropy_units'], cycle['hedge_units']
            if type(e_units) is not int or type(h_units) is not int:
                raise ValueError('integer position units required')
            if (e_units * Direction(cycle['direction']).entropy_side.sign <= 0
                    or e_units != -h_units
                    or (self.journal.cycle_orders(cycle['cycle_id'])
                        and (e_units, h_units) != self._known_positions(cycle))):
                # A restored OPEN label cannot prove a matched holding. Keep
                # external position gaps out of profit quotes and ledger replay.
                self._basis_recovered = False
                self._position_audit_requested.set()
                raise RuntimeHalt('basis_holding_position_reconciliation_required')
        cycle = await self._apply_resolved_steps(cycle)
        if self.settings.strategy_mode != 'funding_hedge':
            await self._observe_active_risk(cycle)
        if self.journal.pending():
            if self.settings.strategy_mode == 'funding_hedge':
                # Fine-grid UNKNOWN fills must not be rounded or used to
                # increase a hedge. Resolve the same order IDs while exiting
                # with native-precision reduce-only quantities.
                self._latch_funding_exit(cycle, 'unknown_order_recovery')
                await self._funding_exit(cycle)
                return
            await self._cover_unknown_exposure(cycle)
            return
        cycle = self.journal.active_live_cycle() or cycle
        if self.settings.strategy_mode == 'funding_hedge':
            self._confirm_funding_hedged(cycle)
            state = self.journal.funding_state(cycle['cycle_id'])
            if state and state['exit_reason']:
                await self._funding_exit(cycle)
                return
            if cycle['phase'] == 'OPEN':
                await self._funding_hold(cycle)
                return
        phase = cycle['phase']
        if (self.settings.strategy_mode == 'basis' and self._standard_opening_pair(cycle)
                and cycle['basis_profit_basis_json'] == self.journal.BASIS_PROFIT_PENDING
                and phase in {'OPEN_FIRST', 'OPEN_HEDGE', 'RECOVERY'}):
            await self._continue_standard_opening(cycle)
            return
        opening = self.journal.opening_attempt(cycle['cycle_id']) if self.settings.opening_experiment_id else None
        if (opening is not None and opening['group_name'] == 'A'
                and phase in {'OPEN_FIRST', 'OPEN_HEDGE'}):
            await self._continue_opening_pair(cycle)
            return
        if phase == 'OPEN_FIRST':
            await self._open_first(cycle)
        elif phase == 'OPEN_HEDGE':
            await self._open_hedge(cycle)
        elif phase == 'CLOSE_FIRST':
            # A restart is not a fresh Exit decision. Positive durable fills
            # resume their group; an unsent/zero first leg returns to normal
            # Exit evaluation instead of reviving the pre-crash signal.
            if self._close_group(cycle) is not None:
                self.journal.update_live_cycle(
                    cycle['cycle_id'], phase='CLOSE_HEDGE', updated_ts=time.time())
                await self._repair_to_balance(
                    self.journal.active_live_cycle() or cycle, purpose='close_hedge')
            else:
                self.journal.update_live_cycle(
                    cycle['cycle_id'], phase='OPEN', updated_ts=time.time())
        elif phase in {'CLOSE_HEDGE', 'RECOVERY'}:
            await self._repair_to_balance(cycle, purpose=(
                'close_hedge' if phase == 'CLOSE_HEDGE' else 'repair'))
        elif phase == 'OPEN':
            if cycle['basis_profit_basis_json'] == self.journal.BASIS_PROFIT_PENDING:
                self._complete_basis_open(cycle)
                cycle = self.journal.active_live_cycle() or cycle
            market = self._market()
            units = abs(cycle['entropy_units'])
            anchor = self._live_entry_anchor(cycle)
            decision = self._basis_exit_decision(cycle, market, anchor)
            if anchor is not None:
                try:
                    self._enqueue_telemetry(lambda: self._update_mfe(
                        cycle['cycle_id'], decision.reversion_bps,
                        observed_ts=time.time(), force=decision.allowed))
                except Exception:
                    pass
            age = time.time() - cycle['started_ts']
            evidence = self._last_position_evidence
            degraded = evidence is not None and evidence.degraded
            holding_state = ('HOLDING_DEGRADED_POSITION_EVIDENCE'
                             if degraded else 'HOLDING')
            self.status.write(
                holding_state if not decision.allowed else "EXITING", decision.reason,
                cycle_id=cycle['cycle_id'], executable_net_usd=str(decision.executable_net_usd),
                executable_gross_usd=(str(decision.executable_gross_usd)
                                      if decision.executable_gross_usd is not None else None),
                normal_exit_min_gross_profit_bps=str(self.settings.exit_min_gross_profit_bps),
                profit_bps=(str(decision.profit_bps) if decision.profit_bps is not None else None),
                profit_entry_notional_usd=(str(decision.profit_entry_notional_usd)
                                           if decision.profit_entry_notional_usd is not None else None),
                normal_exit_rule=decision.allowed,
                reversion_bps=str(decision.reversion_bps), hold_seconds=age,
                long_hold_alarm=age > 600,
                position_evidence_authoritative=not degraded,
                position_evidence_error=(evidence.error if degraded else None),
                entropy_position_source=(evidence.entropy_source if evidence else None),
                hedge_position_source=(evidence.hedge_source if evidence else None),
                exit_anchor_source=(anchor[1] if anchor else None),
                telemetry_errors=self.telemetry_errors,
                telemetry_dropped=self.telemetry_dropped,
                equity_last_success_ts=self._last_equity_poll or None,
                equity_value_usd=(str(self._last_equity) if self._last_equity is not None else None))
            if decision.allowed:
                try:
                    close_side = Direction(cycle['direction']).entropy_side.opposite
                    entropy_cash, _ = market.entropy.quote(
                        close_side, units, self.instrument.quantity_step)
                    hedge_cash, _ = market.hedge.quote(
                        close_side.opposite, units,
                        self.instrument.quantity_step)
                    trigger_basis = (entropy_cash / hedge_cash - 1) * BPS
                    self._exit_trigger = {
                        'ts': time.time(), 'basis': trigger_basis,
                        'mono_ns': time.monotonic_ns(), 'session_id': self.clock_session_id}
                    trigger = dict(self._exit_trigger)
                    self._enqueue_telemetry(lambda: self.journal.record_exit_trigger(
                        cycle['cycle_id'], trigger_ts=trigger['ts'],
                        reason=decision.reason,
                        trigger_basis_bps=trigger_basis,
                        reversion_bps=decision.reversion_bps,
                        executable_pnl_usd=decision.executable_net_usd,
                        anchor_basis_bps=(anchor[0] if anchor else None),
                        anchor_source=(anchor[1] if anchor else None),
                        trigger_mono_ns=trigger['mono_ns'],
                        trigger_clock_session_id=self.clock_session_id))
                except Exception:
                    pass
                self.journal.latch_basis_exit(cycle['cycle_id'], requested_ts=time.time())
                closing = self.journal.active_live_cycle() or cycle
                await self._basis_exit(closing, context=self._context(closing, evidence))

    async def _maybe_open(self, market: Market, reference_bps: Decimal) -> None:
        assert self.instrument is not None
        if not self._operator_running():
            self._publish_operator_state()
            return
        ready_ts = time.time()
        batch = self.journal.batch_state(self.authorization.batch_id)
        self._publish_batch_state(batch)
        funding_mode = self.settings.strategy_mode == 'funding_hedge'
        if self.settings.opening_experiment_id:
            state = self.journal.opening_experiment_state(self.settings.opening_experiment_id)
            self.status.base['opening_experiment_state'] = state
            if state['attempts'] >= 20:
                self.status.write('EXPERIMENT_COMPLETE' if state['finished'] else 'EXPERIMENT_DRAINING',
                                  'opening_experiment_twenty_attempt_limit')
                return
        if funding_mode:
            latest = self._latest_funding_completion_fields()
            reason = latest.get('funding_exit_reason')
            if reason and reason != 'two_settlements_each':
                # Also protect journals produced before early exits latched the
                # batch. Restart/late settlement arrival never rearms entries.
                self.journal.close_entries(
                    self.authorization.batch_id, 'FUNDING_EARLY_EXIT')
                batch = self.journal.batch_state(self.authorization.batch_id)
            if (not batch['closed_for_entries']
                    and batch['max_position_cycles'] is not None
                    and batch['position_cycles'] >= batch['max_position_cycles']):
                self.status.write('BATCH_COMPLETE', 'authorized_cycle_limit_reached',
                                  position_cycles=batch['position_cycles'],
                                  max_position_cycles=batch['max_position_cycles'],
                                  accounting_final=False, **latest)
                return
        if not self.settings.opening_experiment_id and self.session_max_cycles is not None and (
                batch['position_cycles'] - self._starting_position_cycles
                >= self.session_max_cycles):
            self.status.write("SESSION_COMPLETE", "session_cycle_limit_reached",
                              session_position_cycles=(
                                  batch['position_cycles'] - self._starting_position_cycles))
            if not funding_mode:
                if self.operator_control is None:
                    self.stop.set()
                else:
                    self._operator_commit(action='stop',
                        expected_revision=self._operator_snapshot()['intent_revision'],
                        idempotency_key=f'session-limit:{self.clock_session_id}',
                        request_fingerprint='session_cycle_limit_reached', expected_cycle_id=None)
            return
        if not batch['can_open']:
            self.status.write(
                ('ACCOUNTING_PENDING' if batch['block_reason'] in {
                    'PENDING_ACCOUNTING', 'PENDING_ACCOUNTING_ESTIMATE_BUDGET'}
                 else 'BATCH_CLOSED'), batch['block_reason'],
                batch_id=self.authorization.batch_id,
                value=batch['block_value'],
                threshold=batch['block_threshold'],
                cycle_id=batch['block_cycle_id'],
                position_cycles=batch['position_cycles'],
                loss_used_usd=str(batch['loss_used_usd']),
                pending_cost_estimate_usd=str(batch['pending_cost_estimate_usd']),
                closed_for_entries=batch['closed_for_entries'],
                close_reason=batch['close_reason'])
            return
        lease = self._valid_flat_lease(ready_ts)
        if lease is None:
            self.status.write('READY', 'local_flat_state_unavailable' if not funding_mode
                              else 'fresh_authoritative_flat_lease_unavailable')
            return
        if not funding_mode and any(time.monotonic() < t for t in self._send_retry_after.values()):
            self.status.write('READY', 'resource_wait_candidate_dropped')
            return
        diagnostics = getattr(self.venues.hedge, 'request_diagnostics', None)
        if diagnostics is not None:
            requests = diagnostics()
            self.status.base['lighter_requests'] = requests
            if requests['blocked_for_sec'] > 0:
                self.status.write('READY', 'lighter_http_request_deferred')
                return  # Drop this signal before sending the Entropy first leg.
        if self.settings.strategy_mode == 'funding_hedge':
            if ready_ts - self._funding_entry_poll_ts >= self.settings.funding_poll_seconds:
                self._funding_entry_poll_ts = ready_ts
                self._funding_rates = await self._fetch_funding_rates()
            market = self._market()
            plan = self._funding_open_plan(market)
            if self._valid_flat_lease(time.time()) is None:
                self.status.write('READY', 'fresh_authoritative_flat_lease_unavailable')
                return
        else:
            plan = choose_open_plan(
                market, self.instrument, self.settings, reference_bps,
                entropy_venue=self.venues.entropy, hedge_venue=self.venues.hedge)
        if plan is None:
            if self.settings.opening_experiment_id:
                self.journal.rearm_opening_opportunity(self.settings.opening_experiment_id)
            self.status.write("READY", "no_send_entry_check")
            return
        if self.settings.opening_experiment_id:
            state = self.journal.opening_experiment_state(self.settings.opening_experiment_id)
            self.status.base['opening_experiment_state'] = state
            if state['attempts'] >= 20:
                self.status.write('EXPERIMENT_COMPLETE' if state['finished'] else 'EXPERIMENT_DRAINING',
                                  'opening_experiment_twenty_attempt_limit')
                return
            if not state['armed']:
                self.status.write('READY', 'opening_experiment_waiting_for_independent_signal')
                return
            evidence = self._last_position_evidence
            if (evidence is None or not evidence.fully_authoritative
                    or (evidence.entropy_units, evidence.hedge_units) != (0, 0)
                    or time.time() - evidence.observed_ts > self.settings.position_audit_seconds):
                self._position_audit_requested.set()
                self.status.write('READY', 'opening_experiment_fresh_flat_audit_required')
                return
            await self._admit_opening_experiment(plan)
            return
        if not funding_mode:
            await self._admit_standard_opening(plan)
            return
        limit = self.settings.entropy_orders_per_min
        reserve = min(2, max(0, limit - 1))
        cycle_id = f"v3-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
        if funding_mode and not self.journal.reserve_request(
                'entropy', now=time.time(), limit=limit,
                reserve_for_completion=reserve, completion=False,
                request_kind='open_first', cycle_id=cycle_id):
            self.status.write('READY', 'OPEN_SIGNAL_DROPPED_RATE_LIMIT')
            return
        persist_start = time.time()
        if not self._operator_running():
            self._release_completion()
            return
        self.journal.create_live_cycle(
            batch_id=self.authorization.batch_id, cycle_id=cycle_id,
            direction=plan.direction.value, planned_units=plan.units,
            reference_bps=plan.reference_bps,
            entry_basis_bps=plan.executable_basis_bps, started_ts=time.time(),
            planned_notional_usd=plan.notional_usd, signal_ts=plan.decision_ts,
            signal_mono_ns=plan.decision_mono_ns,
            signal_clock_session_id=self.clock_session_id,
            position_check_start_ts=plan.decision_ts,
            position_check_end_ts=plan.decision_ts,
            cycle_persist_start_ts=persist_start)
        if self.settings.strategy_mode == 'funding_hedge':
            self.journal.ensure_funding_cycle(cycle_id, hedged_ts=None)
            self._funding_poll_ts = self._funding_risk_ts = 0.0
        persist_end = time.time()
        self._open_timing[cycle_id] = {
            'cycle_persist_end_ts': persist_end,
        }
        self._invalidate_flat_lease()
        await self._open_first(
            self.journal.active_live_cycle(), plan, request_reserved=funding_mode)

    def _latest_funding_completion_fields(self) -> dict:
        row = self.journal.db.execute('''SELECT cycle_id FROM live_cycles
            WHERE batch_id=? ORDER BY started_ts DESC LIMIT 1''',
            (self.authorization.batch_id,)).fetchone()
        return self._funding_completion_fields(row['cycle_id']) if row else {}

    async def run(self) -> None:
        tasks: list[asyncio.Task] = []
        buffered_observer = None
        owner_started_mono_ns = time.monotonic_ns()
        try:
            from .market_observer import BufferedJSONLObserver, ObserverFileWriter
            if type(self.observer) is ReadOnlyObserver and 'publish' not in vars(self.observer):
                buffered_observer = BufferedJSONLObserver(self.observer.path,
                    dict(kind='v3_observer_session', pair_id=self.pair_id,
                         session_id=self.clock_session_id, config_hash=self.status.base['config_hash']),
                    append_only=True)
                buffered_observer.start()
                self.observer = buffered_observer
            self._reference_writer = ObserverFileWriter({'checkpoint': 1.0})
            self._reference_writer.start()
            self.status.write("STARTING", "loading_markets_and_account_streams")
            tasks = await self.initialize()
            tasks.append(asyncio.create_task(self._owner_heartbeat_loop(), name='v3-owner-heartbeat'))
            tasks.append(asyncio.create_task(
                self._http_keepalive_loop(), name='entropy-http-keepalive'))
            tasks.append(asyncio.create_task(self._equity_loop(), name='v3-equity-refresh'))
            tasks.append(asyncio.create_task(
                self._analytics_reconcile_loop(), name='v3-analytics-reconcile'))
            if self.reference_sampler_enabled:
                tasks.append(asyncio.create_task(
                    self._reference_loop(), name='v3-reference-sampler'))
            self.status.write("RECONCILING", "startup_position_and_order_recovery")
            await self._resolve_pending()
            active = self.journal.active_live_cycle()
            if self.settings.strategy_mode == 'basis':
                if active is not None or self._last_position_evidence is None:
                    await self._audit_positions()
                startup_evidence = self._last_position_evidence
            else:
                startup_evidence = await self._position_evidence(active)
            if active is None and startup_evidence.fully_authoritative and (
                    startup_evidence.entropy_units or startup_evidence.hedge_units):
                raise RuntimeHalt("pre-existing positions have no V3 cycle evidence")
            if active is not None:
                await self._sync_positions(active, evidence=startup_evidence)
                self._live_entry_anchor(self.journal.active_live_cycle() or active)
            if self.operator_socket_path is not None:
                self.enable_operator_control(self._operator_binding())
                await self.operator_control.start(self.operator_socket_path)
            tasks.append(asyncio.create_task(
                self._flat_lease_loop(), name='v3-position-audit'))
            tasks.append(asyncio.create_task(self._funding_risk_loop(), name='funding-risk-watch'))
            while not self.stop.is_set():
                await self._drain_parallel()
                if self.operator_control is not None:
                    self._update_operator_progress()
                    self.status.base['operator_control'] = self.operator_control.state()
                for task in tasks:
                    if task.done() and task.exception() is not None:
                        raise RuntimeHalt(f"feed task failed: {task.get_name()}") from task.exception()
                active = self.journal.active_live_cycle()
                if active is None and not self.journal.pending() and not self._operator_running():
                    self._publish_operator_state()
                    try:
                        await asyncio.wait_for(self.changed.wait(), timeout=.5)
                    except asyncio.TimeoutError:
                        pass
                    self.changed.clear()
                    continue
                if active is not None and (self.settings.strategy_mode == 'funding_hedge'
                                           or active['phase'] == 'BASIS_EXIT'):
                    try:
                        await self._resume_cycle(active)
                    except RuntimeHalt as exc:
                        self.status.write('RECONCILE_REQUIRED', str(exc),
                                          active_cycle=self.journal.active_live_cycle())
                    self._flush_telemetry()
                    try:
                        await asyncio.wait_for(self.changed.wait(), timeout=.5)
                    except asyncio.TimeoutError:
                        pass
                    self.changed.clear()
                    continue
                try:
                    market = self._market()
                    view = (self.reference.restore_market_valid(time.time())
                            if self.reference_sampler_enabled else
                            self.reference.observe(time.time(), market.mid_basis_bps,
                                                   valid=True))
                except RuntimeHalt as exc:
                    if not self.reference_sampler_enabled:
                        self.reference.observe(time.time(), dec(0), valid=False)
                    self.status.write("WAITING_FOR_MARKET", str(exc))
                    await asyncio.sleep(.25)
                    continue
                self.observer.publish({
                    "kind": "v3_market", "pair_identity": self.pair_id,
                    "basis_bps": str(market.mid_basis_bps),
                    "reference_bps": str(view.center_bps) if view.center_bps is not None else None,
                    "reference_usable": view.usable,
                })
                try:
                    active = self.journal.active_live_cycle()
                    if active is not None:
                        await self._resume_cycle(active)
                    elif self.journal.pending():
                        await self._resolve_pending()
                        self.status.write('RECONCILE_REQUIRED',
                                          'orphan_order_accounting_pending',
                                          pending_orders=len(self.journal.pending()))
                    elif self.settings.strategy_mode == 'funding_hedge':
                        await self._maybe_open(market, market.mid_basis_bps)
                    elif view.usable:
                        await self._maybe_open(market, view.center_bps)
                    else:
                        self.status.write("COLLECTING_REFERENCE", view.source,
                                          live_samples=view.live_samples,
                                          live_span_seconds=view.live_span_seconds)
                    self._reconcile_backoff_seconds = 2.0
                except RuntimeHalt as exc:
                    # Missing account evidence blocks OPEN and all assumptions
                    # about FLAT, but a transient REST failure must not discard
                    # durable cycle/order state or force a process restart.
                    self.status.write(
                        "RECONCILE_REQUIRED", str(exc),
                        pending_orders=len(self.journal.pending()),
                        active_cycle=self.journal.active_live_cycle())
                    if self.operator_control is None:
                        await asyncio.sleep(self._reconcile_backoff_seconds)
                    else:
                        try:
                            await asyncio.wait_for(self._operator_wake.wait(),
                                                   timeout=self._reconcile_backoff_seconds)
                        except asyncio.TimeoutError:
                            pass
                        self._operator_wake.clear()
                    self._reconcile_backoff_seconds = min(
                        self._reconcile_backoff_seconds * 2, 30.0)
                self._flush_telemetry()
                self._persist_reference()
                try:
                    await asyncio.wait_for(self.changed.wait(), timeout=.5)
                except asyncio.TimeoutError:
                    pass
                self.changed.clear()
        except BaseException as exc:
            self.status.write("HALTED", f"{type(exc).__name__}: {exc}",
                              pending_orders=len(self.journal.pending()),
                              active_cycle=self.journal.active_live_cycle())
            raise
        finally:
            unwinding_error = sys.exc_info()[0] is not None
            if self.operator_control is not None:
                await self.operator_control.close()
            active = self.journal.active_live_cycle()
            if active is not None and active['cycle_id'] in self._mfe_cache:
                try:
                    value, value_ts = self._mfe_cache[active['cycle_id']]
                    self.journal.update_max_reversion(
                        active['cycle_id'], value, observed_ts=value_ts, force=True)
                except Exception:
                    pass
            self._persist_reference(force=True)
            if self._reference_writer is not None:
                await self._reference_writer.close()
                self._reference_writer = None
            self.stop.set()
            parallel_cleanup_error = None
            try:
                if self._basis_risk_exit_task is not None:
                    await asyncio.shield(asyncio.gather(self._basis_risk_exit_task,
                                                       return_exceptions=True))
                if self._parallel_tasks:
                    await asyncio.shield(asyncio.gather(*tuple(self._parallel_tasks.values()),
                                                       return_exceptions=True))
                await self._drain_parallel()
            except BaseException as exc:
                parallel_cleanup_error = exc
            self._release_completion()
            tasks += list(self._funding_risk_tasks.values()) + list(self._funding_reads)
            external_recovery = getattr(self, '_external_recovery_task', None)
            if external_recovery is not None:
                tasks.append(external_recovery)
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._flush_telemetry()
            await asyncio.gather(self.venues.entropy.close(),
                                 self.venues.hedge.close(), return_exceptions=True)
            self._flush_telemetry()
            self._publish_protection(dict(kind='v3_owner_exit', ts=time.time(),
                mono_ns=time.monotonic_ns(), clock_session_id=self.clock_session_id,
                owner_elapsed_seconds=(time.monotonic_ns() - owner_started_mono_ns) / 1e9,
                outcome='error' if unwinding_error or parallel_cleanup_error else 'stopped',
                pending_orders=len(self.journal.pending()),
                active_cycle_id=(self.journal.active_live_cycle() or {}).get('cycle_id')))
            if buffered_observer is not None:
                await self._close_order_observer(buffered_observer)
            if parallel_cleanup_error is not None and not unwinding_error:
                raise parallel_cleanup_error

    async def preflight(self) -> dict[str, Any]:
        """Exercise real market/account streams and recovery without opening."""
        tasks: list[asyncio.Task] = []
        try:
            self.status.write("PREFLIGHT", "loading_real_adapters")
            tasks = await self.initialize(apply_leverage=False)
            resolved = await self._resolve_pending()
            active = self.journal.active_live_cycle()
            evidence = await self._position_evidence(active)
            if active is None and evidence.fully_authoritative and (
                    evidence.entropy_units or evidence.hedge_units):
                raise RuntimeHalt("pre-existing positions have no V3 cycle evidence")
            entry_anchor = None
            if active is not None:
                active = await self._sync_positions(active)
                entry_anchor = self._live_entry_anchor(active)
                active = self.journal.active_live_cycle() or active
            market = self._market()
            result = {
                "orders_resolved": resolved,
                "entropy_units": evidence.entropy_units,
                "hedge_units": evidence.hedge_units,
                "position_evidence_authoritative": evidence.fully_authoritative,
                "position_evidence_error": evidence.error,
                "active_cycle": active,
                "live_entry_basis_bps": (str(entry_anchor[0])
                                         if entry_anchor is not None else None),
                "live_entry_basis_source": (entry_anchor[1]
                                             if entry_anchor is not None else None),
                "basis_bps": str(market.mid_basis_bps),
                "quantity_step": str(self.instrument.quantity_step),
            }
            self.status.write("PREFLIGHT_OK", "real_adapters_ready", **result)
            return result
        finally:
            self.stop.set()
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(self.venues.entropy.close(),
                                 self.venues.hedge.close(), return_exceptions=True)

    async def reopen_entries(self) -> dict[str, Any]:
        """Operator-only unlock after a fresh authoritative flat check."""
        tasks: list[asyncio.Task] = []
        try:
            self.status.write('UNLOCK_CHECK', 'checking_authoritative_flat')
            tasks = await self.initialize(apply_leverage=False)
            evidence = await self._position_evidence()
            try:
                self.journal.reopen_entries(self.authorization.batch_id,
                                            evidence=evidence)
            except PendingOrder as exc:
                self.status.write('UNLOCK_REFUSED', str(exc),
                                  batch_id=self.authorization.batch_id)
                raise
            batch = self.journal.batch_state(self.authorization.batch_id)
            self.status.write('UNLOCKED', 'manual_reopen',
                              batch_id=self.authorization.batch_id,
                              position_cycles=batch['position_cycles'],
                              loss_used_usd=str(batch['loss_used_usd']))
            return batch
        finally:
            self.stop.set()
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(self.venues.entropy.close(),
                                 self.venues.hedge.close(), return_exceptions=True)
