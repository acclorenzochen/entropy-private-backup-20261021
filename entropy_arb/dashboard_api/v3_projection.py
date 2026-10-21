"""V3 dashboard projections; absence of evidence is never coerced to zero."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import math
import re
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from .v3_readmodel import number, requote_market
from .v3_profit import profit_overview


def _safe_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    value = re.sub(r"https?://\S+", "[url]", value)
    value = re.sub(r"/(?:Users|home|private|tmp|etc)/[^\s,;]+", "[path]", value)
    return value[:300]


def public_status(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    fields = ("batch_id", "build_hash", "close_reason", "closed_for_entries",
              "config_hash", "current_state", "cycle_id", "loss_used_usd",
              "mode", "position_cycles", "max_position_cycles", "loss_budget_usd",
              "max_position_cycles_disabled", "loss_budget_disabled",
              "process_id", "reason", "result_kind", "source_ts",
              "start_ts", "strategy_version", "threshold", "value",
              "checkpoint_writer_coalesced", "checkpoint_writer_errors",
              "live_samples", "observer_dropped_events", "reference_usable")
    out = {key: row.get(key) for key in fields if key in row}
    if "reason" in out:
        out["reason"] = _safe_text(out["reason"])
    writer = row.get("status_writer")
    if isinstance(writer, dict):
        out["status_writer"] = {key: writer.get(key) for key in (
            "dropped_events", "write_errors")}
    return out


def manual_exit_view(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    """Historical exit evidence, bound to the original ledger and cycle.

    Even a successful result describes positions at position_observed_ts; it is
    never a lease proving the account's current positions.
    """
    sources = snapshot['sources']
    raw = sources.get('manual_exit', {}).get('data')
    if not isinstance(raw, dict):
        return None
    live = sources.get('live', {}).get('data') or {}
    manifest = sources.get('live_manifest', {}).get('data') or {}
    journal = snapshot.get('journal') or {}
    metadata = journal.get('metadata') or {}
    cid = raw.get('cycle_id')
    cycle = next((r for r in journal.get('live_cycles', []) if r.get('cycle_id') == cid), {})
    canonical = next((r for r in journal.get('cycles', []) if r.get('cycle_id') == cid), {})
    economics = next((r for r in journal.get('cycle_economics', []) if r.get('cycle_id') == cid), {})
    stamp, observed = number(raw.get('source_ts')), number(raw.get('position_observed_ts'))
    status_observed = observed
    result = {'cycle_id': cid, 'batch_id': cycle.get('batch_id'), 'state': 'unknown',
              'source_ts': float(stamp) if stamp is not None else None,
              'position_observed_ts': float(observed) if observed is not None else None,
              'remaining': {'entropy_units': None, 'hedge_units': None, 'authoritative': False},
              'pending': [], 'legs': [{'venue': venue, 'initial_units': None,
                  'closed_units': None, 'remaining_units': None, 'state': 'unknown'}
                  for venue in ('entropy', 'hedge')],
              'accounting_final': False, 'net_pnl_usd': None,
              'supersedes_live': False, 'reason': 'exit_evidence_unmatched'}
    pair = manifest.get('pair_identity') or live.get('pair_identity')
    config = manifest.get('config_hash') or live.get('config_hash')
    batch = (manifest.get('authorization') or {}).get('batch_id') or live.get('batch_id')
    if (raw.get('mode') != 'manual-exit' or not pair or not config or not batch or not cycle
            or raw.get('pair_identity') != pair or raw.get('config_hash') != config
            or metadata.get('identity') != f'{pair}:account'
            or cycle.get('batch_id') != batch
            or raw.get('batch_id') not in (None, batch)
            or (canonical and canonical.get('batch_id') != batch)):
        return result
    try:
        account_text = metadata[f'manual_exit_account:{cid}']
        account = json.loads(account_text)
        request = json.loads(metadata[f'manual_exit:{cid}'])
        requested = number(request.get('requested_ts'))
        account_valid = (isinstance(account, dict)
            and isinstance(account.get('hl_query_account'), str)
            and re.fullmatch(r'0x[0-9a-fA-F]{40}', account['hl_query_account'])
            and type(account.get('lighter_account_index')) is int
            and account['lighter_account_index'] >= 0
            and isinstance(account.get('lighter_profile'), str) and bool(account['lighter_profile']))
        scope_hash = raw.get('account_scope_hash')
        if not account_valid or (scope_hash is not None and scope_hash != hashlib.sha256(
                account_text.encode()).hexdigest()):
            raise ValueError('account_binding')
    except (KeyError, ValueError, TypeError, AttributeError):
        result['reason'] = 'exit_account_binding_unverified'
        return result
    if (stamp is None or requested is None or stamp < requested
            or stamp > Decimal(str(snapshot['generated_at'])) + 5
            or (observed is not None and (observed < requested or observed > stamp))):
        result['reason'] = 'exit_timestamp_unverified'
        return result
    orders = list({r['order_ref']: r for r in journal.get('orders', [])
                   if r.get('cycle_id') == cid and r.get('order_ref')}.values())
    pending = [r for r in orders if r.get('state') != 'TERMINAL']
    ledger_updated = number(cycle.get('updated_ts'))
    if isinstance(raw.get('pending'), list) and (ledger_updated is None or stamp >= ledger_updated):
        pending = list({r.get('order_ref'): r for r in pending + raw['pending']
                        if isinstance(r, dict) and r.get('order_ref')}.values())
    result['pending'] = [{key: _safe_text(row.get(key)) for key in
                          ('order_ref', 'venue', 'state', 'reduce_only', 'filled_units')}
                         for row in pending]
    # Status is preferred when it contains a newer observation; otherwise retain
    # the ledger's own inventory evidence and its original observation time.
    remaining = raw.get('remaining') if isinstance(raw.get('remaining'), dict) else raw
    status_positions = all(number(remaining.get(f'{venue}_units')) is not None
                           for venue in ('entropy', 'hedge'))
    use_status = status_positions and (ledger_updated is None or stamp >= ledger_updated)
    position = remaining if use_status else cycle
    authoritative = (remaining.get('authoritative') is True if use_status else
                     cycle.get('position_evidence_authoritative') == 1)
    if not use_status:
        observed = number(cycle.get('last_authoritative_ts'))
        result['position_observed_ts'] = float(observed) if observed is not None else None
    inventories, all_orders_valid = {}, bool(orders) and all(
        row.get('venue') in ('entropy', 'hedge') for row in orders)
    for leg in result['legs']:
        venue = leg['venue']
        venue_orders = [r for r in orders if r.get('venue') == venue]
        initial, closed, inventory, valid = 0, 0, 0, bool(venue_orders)
        for row in venue_orders:
            filled, requested_units = number(row.get('filled_units')), number(row.get('requested_units'))
            if (filled is None or requested_units is None or filled < 0 or filled > requested_units
                    or filled != filled.to_integral_value() or requested_units < 0
                    or requested_units != requested_units.to_integral_value()
                    or row.get('side') not in ('buy', 'sell')):
                valid = False
                continue
            inventory += int(filled) * (1 if row['side'] == 'buy' else -1)
            if row.get('reduce_only'):
                closed += int(filled)
            else:
                initial += int(filled) * (1 if row['side'] == 'buy' else -1)
        valid = valid and closed <= abs(initial) and abs(inventory) == abs(initial) - closed
        units = number(position.get(f'{venue}_units'))
        if units is not None and units == units.to_integral_value():
            result['remaining'][f'{venue}_units'] = int(units)
            leg['remaining_units'] = abs(int(units))
        if valid:
            leg.update(initial_units=abs(initial), closed_units=closed)
            inventories[venue] = inventory
        all_orders_valid &= valid
        venue_pending = [r for r in pending if r.get('venue') == venue]
        leg['state'] = ('unknown' if not valid or any(r.get('state') in ('UNKNOWN', 'SUBMITTING')
                        for r in venue_pending) else 'executing' if venue_pending
                        else 'complete' if leg['remaining_units'] == 0 and inventory == 0
                        else 'partial' if closed else 'executing')
    result['remaining']['authoritative'] = bool(authoritative and observed is not None)
    raw_pending = raw.get('pending')
    terminal_times = [number(row.get('terminal_recorded_ts') or row.get('terminal_received_ts'))
                      for row in orders]
    ledger_observed = number(cycle.get('last_authoritative_ts'))
    flat = (raw.get('success') is True and raw.get('active_cycle', False) is None
            and isinstance(raw_pending, list) and not raw_pending and not pending
            and status_observed is not None and remaining.get('authoritative') is True
            and all_orders_valid and all(inventories.get(v) == 0 for v in ('entropy', 'hedge'))
            and all(number(remaining.get(f'{v}_units')) == number(cycle.get(f'{v}_units')) == 0
                    for v in ('entropy', 'hedge'))
            and cycle.get('phase') == 'DONE' and cycle.get('position_evidence_authoritative') == 1
            and ledger_observed is not None and requested <= ledger_observed <= status_observed
            and all(ts is not None and ts <= status_observed for ts in terminal_times)
            and number(canonical.get('completed_ts')) is not None
            and requested <= number(canonical['completed_ts']) <= stamp)
    final = (_pnl_breakdown(canonical, economics) is not None
             and all(number(economics.get(f'{venue}_{field}_usd')) is not None
                     for venue in ('entropy', 'hedge') for field in ('fees', 'funding'))
             and all(sum(number(economics[f'{venue}_{field}_usd']) for venue in ('entropy', 'hedge'))
                     == number(economics.get(f'actual_{field}_usd')) for field in ('fees', 'funding')))
    if raw.get('net_pnl_usd') is not None and number(raw.get('net_pnl_usd')) != number(canonical.get('pnl_usd')):
        final = False
    unknown = any(leg['state'] == 'unknown' for leg in result['legs']) or any(
        row.get('state') in ('UNKNOWN', 'SUBMITTING') for row in pending)
    result.update(state='complete' if flat and final else 'accounting_pending' if flat else
                  'unknown' if unknown or raw.get('success') is True else
                  'partial' if any(leg['closed_units'] for leg in result['legs']) else 'executing',
                  accounting_final=bool(flat and final),
                  net_pnl_usd=str(number(canonical['pnl_usd'])) if flat and final else None,
                  reason='authoritative_flat_at_observation' if flat else 'exit_flat_claim_unverified'
                  if raw.get('success') is True else 'exit_orders_unresolved' if unknown else 'exit_in_progress')
    if flat:
        observed = status_observed
        result['position_observed_ts'] = float(observed)
        result['remaining']['authoritative'] = True
    live_ts = number(live.get('source_ts'))
    newer_cycle = any(r.get('cycle_id') != cid and (
        r.get('phase') != 'DONE' or (number(r.get('started_ts')) or Decimal(0)) > stamp)
        for r in journal.get('live_cycles', []))
    result['supersedes_live'] = bool(flat and not newer_cycle and live.get('cycle_id') == cid
        and live.get('pair_identity') in (None, pair) and live.get('config_hash') in (None, config)
        and live_ts is not None and stamp > live_ts and observed > live_ts)
    if newer_cycle:
        result['reason'] = 'newer_cycle_present'
    elif flat and not result['supersedes_live']:
        result['reason'] = 'historical_exit_evidence'
    return result


def public_diagnostic(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    allowed = ("run_id", "pair_id", "config_hash", "decision_seq", "evaluation_ts",
               "phase", "entry", "exit", "position", "budget", "reason", "cycle_id",
               "source_ts", "schema_version", "source")
    def clean(value: Any, depth: int = 0) -> Any:
        if depth > 3:
            return None
        if isinstance(value, dict):
            return {str(key)[:80]: clean(item, depth + 1) for key, item in value.items()
                    if not re.search(r"secret|token|password|credential|(^|_)path$", str(key), re.I)}
        if isinstance(value, list):
            return [clean(item, depth + 1) for item in value[:50]]
        return _safe_text(value)
    return {key: clean(row.get(key)) for key in allowed if key in row}


def _cycle_diagnostic(snapshot: dict[str, Any], cycle_id: str | None) -> dict[str, Any] | None:
    row = snapshot['sources'].get('live_diagnostics', {}).get('data')
    return public_diagnostic(row) if cycle_id and row and row.get('cycle_id') == cycle_id else None


def funding_cycle(snapshot: dict[str, Any], cycle_id: str | None) -> dict[str, Any] | None:
    """Project durable settlements without importing the trading Journal."""
    journal = snapshot.get('journal') or {}
    row = next((r for r in journal.get('funding_cycles', [])
                if r.get('cycle_id') == cycle_id), None)
    if row is None:
        return None
    fields = ('cycle_id', 'hedged_ts', 'negative_sample_count', 'negative_since_ts',
              'last_sample_ts', 'last_net_usd_per_hour', 'last_complete_poll_ts',
              'trade_accounting_complete', 'closed_poll_complete', 'completed', 'exit_reason')
    state = {key: row.get(key) for key in fields}
    amounts = {'entropy': Decimal(0), 'hedge': Decimal(0)}
    periods = {'entropy': set(), 'hedge': set()}
    hedged = number(row.get('hedged_ts'))
    records = []
    for item in journal.get('funding_settlements', []):
        if item.get('cycle_id') != cycle_id or item.get('venue') not in amounts:
            continue
        amount, ts = number(item.get('amount_usd')), number(item.get('ts'))
        if amount is None or ts is None:
            return None
        venue = item['venue']
        amounts[venue] += amount
        if hedged is not None and ts > hedged:
            periods[venue].add(math.floor(ts / 3600))
        records.append({key: _safe_text(item.get(key)) for key in
                        ('venue', 'event_id', 'ts', 'amount_usd', 'source')})
    state.update(settlement_counts={key: len(value) for key, value in periods.items()},
                 funding_by_venue={key: str(value) for key, value in amounts.items()},
                 actual_funding_usd=str(sum(amounts.values())),
                 settlements=sorted(records, key=lambda r: r['ts'], reverse=True)[:20])
    return state


def funding_dashboard(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    sources = snapshot['sources']
    live = sources.get('live', {}).get('data') or {}
    manifest = sources.get('live_manifest', {}).get('data') or {}
    preflight = sources.get('public_preflight', {}).get('data') or {}
    settings = manifest.get('settings') or {}
    if settings.get('strategy_mode') not in (None, 'funding_hedge'):
        return None
    if (settings.get('strategy_mode') != 'funding_hedge' and not live.get('funding_state')
            and preflight.get('kind') != 'nbis_public_preflight'):
        return None
    now = snapshot['generated_at']
    source_ts = number(live.get('source_ts'))
    live_fresh = source_ts is not None and 0 <= now - float(source_ts) <= 10
    journal = snapshot.get('journal') or {}
    authorization = manifest.get('authorization') or {}
    batch_id = live.get('batch_id') or authorization.get('batch_id')
    cycles = [row for row in journal.get('live_cycles', [])
              if not batch_id or row.get('batch_id') == batch_id]
    cycles.sort(key=lambda r: r.get('started_ts') or 0, reverse=True)
    active = next((row for row in cycles if row.get('phase') != 'DONE'), None)
    selected = active or (cycles[0] if cycles else None)
    cycle_id = selected.get('cycle_id') if selected else live.get('cycle_id')
    state = funding_cycle(snapshot, cycle_id)
    state_source = 'journal' if state is not None else None
    if state is None and isinstance(live.get('funding_state'), dict):
        raw = live['funding_state']
        if raw.get('cycle_id') == cycle_id:
            state = {key: raw.get(key) for key in (
                'cycle_id', 'hedged_ts', 'settlement_counts', 'funding_by_venue',
                'actual_funding_usd', 'last_sample_ts', 'last_net_usd_per_hour',
                'negative_sample_count', 'negative_since_ts', 'last_complete_poll_ts',
                'completed', 'exit_reason', 'trade_accounting_complete', 'closed_poll_complete')}
            for key in ('settlement_counts', 'funding_by_venue'):
                values = raw.get(key) or {}
                state[key] = {venue: values.get(venue) for venue in ('entropy', 'hedge')}
            state_source = 'live_status'
    policy = {key: settings.get(key) for key in (
        'max_notional_usd', 'funding_settlements_required', 'funding_stale_seconds',
        'funding_reversal_samples', 'funding_reversal_seconds',
        'funding_remaining_cost_limit_usd', 'funding_round_loss_limit_usd',
        'funding_liquidation_distance', 'funding_risk_stale_seconds')}
    policy['configured'] = settings.get('strategy_mode') == 'funding_hedge'
    step = number(live.get('accounting_quantity_step'))
    position = {key: str(number(selected.get(f'{key}_units')) * step)
                if selected and step is not None and number(selected.get(f'{key}_units')) is not None
                else None for key in ('entropy', 'hedge')}
    sample_ts = number((state or {}).get('last_sample_ts'))
    stale_seconds = number(policy['funding_stale_seconds'])
    rate_fresh = bool(live_fresh and sample_ts is not None and stale_seconds is not None
                      and 0 <= now - float(sample_ts) <= float(stale_seconds))
    def public_legs(field, keys, timestamp_key, max_age):
        raw = live.get(field) or {}
        if field == 'funding_risk' and raw.get('cycle_id') != cycle_id:
            raw = {}
        raw = raw.get('legs', {}) if field == 'funding_risk' else raw
        result = {}
        for venue in ('entropy', 'hedge'):
            item = raw.get(venue) or {}
            ts = number(item.get(timestamp_key))
            valid = bool(live_fresh and ts is not None and max_age is not None
                         and 0 <= now - float(ts) <= float(max_age))
            result[venue] = {**{key: _safe_text(item.get(key)) if valid else None for key in keys},
                             'source_ts': float(ts) if ts is not None else None,
                             'quality': 'available' if valid else 'stale' if item else 'unavailable'}
        return result
    batch = next((row for row in journal.get('batches', [])
                  if row.get('batch_id') == batch_id), {})
    used = live.get('position_cycles')
    if used is None:
        used = (sum(row.get('result_kind') != 'zero_fill' for row in journal.get('cycles', [])
                    if row.get('batch_id') == batch_id) if batch else None)
    def batch_limit(raw_key, authorization_key):
        if raw_key in batch:
            return batch[raw_key]
        if authorization_key in authorization:
            value = authorization[authorization_key]
            return 'disabled' if value is None else value
        return None
    pf = {key: preflight.get(key) for key in (
        'ok', 'source_ts', 'current_state', 'live_ready', 'account_verified',
        'settlements_verified', 'accounting_delay_verified')}
    pf['error'] = _safe_text(preflight.get('error'))
    return {'strategy_mode': 'funding_hedge', 'pair_identity': manifest.get('pair_identity')
            or preflight.get('pair_identity') or live.get('pair_identity'),
            'live_quality': 'available' if live_fresh else 'stale' if live else 'unavailable',
            'policy': policy, 'configured_leverage': live.get('configured_leverage')
            or manifest.get('configured_leverage') or preflight.get('configured_leverage'),
            'applied_leverage': live.get('applied_leverage'),
            'common_quantity_step': live.get('common_quantity_step')
            or preflight.get('common_quantity_step'),
            'accounting_quantity_step': live.get('accounting_quantity_step'),
            'cycle': selected, 'ledger_quantities': position,
            'state': state, 'state_source': state_source,
            'net_usd_per_hour': str(number((state or {}).get('last_net_usd_per_hour')))
            if rate_fresh and number((state or {}).get('last_net_usd_per_hour')) is not None else None,
            'rate_quality': 'available' if rate_fresh else 'unavailable',
            'expected_remaining_cost_usd': live.get('expected_remaining_funding_cost_usd')
            if live_fresh and live.get('cycle_id') == cycle_id else None,
            'rates': public_legs('funding_rates', ('rate_bps_per_hour', 'index_price', 'source'),
                                 'source_ts', stale_seconds),
            'risk': public_legs('funding_risk', ('position_qty', 'mark_price',
                                'liquidation_price', 'distance', 'source'), 'source_ts',
                                number(policy['funding_risk_stale_seconds'])),
            'batch': {'batch_id': batch_id, 'position_cycles': used,
                      'max_position_cycles': batch_limit('max_position_cycles', 'max_position_cycles'),
                      'closed_for_entries': batch.get('closed_for_entries'),
                      'close_reason': batch.get('close_reason'),
                      'loss_budget_usd': batch_limit('loss_budget', 'loss_budget_usd'),
                      'loss_used_usd': live.get('loss_used_usd')
                      if live.get('loss_used_usd') is not None else batch.get('loss_used_usd')},
            'public_preflight': pf if preflight else None}


@dataclass(frozen=True)
class Window:
    scope: str
    from_ts: float | None
    to_ts: float | None
    timezone: str
    batch_id: str | None = None
    session: str | None = None

    def public(self) -> dict[str, Any]:
        return {"scope": self.scope, "from_ts": self.from_ts, "to_ts": self.to_ts,
                "timezone": self.timezone, "batch_id": self.batch_id,
                "session": self.session, "cohort_basis": "explicit-per-metric"}

    def contains(self, stamp: Any) -> bool:
        value = number(stamp)
        return (value is not None
                and (self.from_ts is None or Decimal(str(self.from_ts)) <= value)
                and (self.to_ts is None or value < Decimal(str(self.to_ts))))


def resolve_window(query: dict[str, Any], live: dict[str, Any] | None,
                   *, now: float) -> Window:
    scope = query.get("scope") or "session"
    if scope in {'1h', '6h'}:
        scope = '7d'
    zone = query.get("timezone") or "Asia/Taipei"
    if zone not in {"Asia/Taipei", "UTC"}:
        raise ValueError("unsupported_timezone")
    if scope == 'all':
        return Window(scope, None, None, zone)
    batch_id = query.get("batch_id") or (live or {}).get("batch_id")
    session = query.get("session") or (
        f"{live.get('process_id')}:{live.get('start_ts')}:{live.get('build_hash')}"
        if live and live.get("start_ts") else None)
    actual_session = (f"{live.get('process_id')}:{live.get('start_ts')}:{live.get('build_hash')}"
                      if live and live.get("start_ts") else None)
    if scope == "session" and query.get("session") and query["session"] != actual_session:
        raise ValueError("historical_session_not_available")
    if scope == "custom":
        start, end = number(query.get("from_ts")), number(query.get("to_ts"))
        if start is None or end is None:
            raise ValueError("custom_window_requires_from_ts_and_to_ts")
        left, right = float(start), float(end)
    elif scope == "today":
        local = datetime.fromtimestamp(now, ZoneInfo(zone))
        left = local.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        right = now
    elif scope in {"24h", "7d"}:
        left, right = now - {"24h": 86400, "7d": 7 * 86400}[scope], now
    elif scope == "batch":
        if not batch_id:
            raise ValueError("batch_id_unavailable")
        # The batch cohort is filtered by ID. Its wall-clock range is explicit.
        left, right = 0.0, now
    elif scope == "session":
        started = number((live or {}).get("start_ts"))
        if started is None and query.get("from_ts") is None:
            raise ValueError("session_start_unavailable")
        left, right = float(started or number(query.get("from_ts"))), now
    else:
        raise ValueError("unsupported_scope")
    if (left < 0 or right <= left or right > now + 60
            or (scope != 'batch' and right - left > 366 * 86400)):
        raise ValueError("invalid_window")
    return Window(scope, left, right, zone, batch_id if scope == "batch" else None, session)


def _selected_cycle(row: dict[str, Any], window: Window) -> bool:
    return (window.batch_id is None or row.get("batch_id") == window.batch_id)


def _opening_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [o for o in orders if not bool(o.get("reduce_only"))]


def _submitted(order: dict[str, Any]) -> bool:
    return order.get("submitted_ts") is not None


def _paired_open(orders: list[dict[str, Any]]) -> bool:
    signed = {"entropy": 0, "hedge": 0}
    for order in _opening_orders(list({o['order_ref']: o for o in orders}.values())):
        if order.get("state") != "TERMINAL":
            continue
        venue = order.get("venue")
        if venue in signed:
            signed[venue] += int(order.get("filled_units") or 0) * (
                1 if str(order.get("side")).lower() == "buy" else -1)
    return signed["entropy"] != 0 and signed["entropy"] == -signed["hedge"]


def _cycle_duration(live: dict[str, Any], economics: dict[str, Any],
                    orders: list[dict[str, Any]], telemetry: list[dict[str, Any]],
                    fills: list[dict[str, Any]], *, now: float) -> dict[str, Any]:
    """Keep exchange-first duration separate from local positive-fill confirmation."""
    orders = list({order['order_ref']: order for order in orders}.values())
    opening = _opening_orders(orders)
    current = number(now)
    first_times = []
    confirmed_times = []
    missing_first = [o['order_ref'] for o in opening if o.get('state') != 'TERMINAL']
    first_complete = bool(opening) and all(o.get('state') == 'TERMINAL' for o in opening)
    # Closing fills need no first-fill timestamp: their end proof is BOTH-flat.
    for order in opening:
        units = number(order.get('filled_units'))
        requested = number(order.get('requested_units'))
        if (order.get('venue') not in ('entropy', 'hedge')
                or order.get('side') not in ('buy', 'sell')
                or units is None or units < 0 or units != units.to_integral_value()
                or requested is None or requested < 0
                or requested != requested.to_integral_value() or units > requested):
            first_complete = False
            missing_first.append(order['order_ref'])
            continue
        if not units:
            continue
        matches = [trace for trace in telemetry
                   if trace.get('order_ref') == order['order_ref']
                   and trace.get('cycle_id') == live['cycle_id']
                   and trace.get('venue') == order.get('venue')]
        positive = [trace for trace in matches
                    if (number(trace.get('result_units')) or 0) > 0
                    and number(trace['result_units']) <= units
                    and number(trace['result_units']) == number(trace['result_units']).to_integral_value()
                    and (number(trace.get('actual_fill_vwap'))
                         or number(trace.get('provisional_avg_px')) or 0) > 0]
        local = [number(trace.get(field)) for trace in positive
                 for field in ('terminal_observed_ts', 'result_ts')]
        if order.get('state') == 'TERMINAL' and (number(order.get('avg_px')) or 0) > 0:
            received = order.get('terminal_received_ts')
            local.append(number(received if received is not None else order.get('terminal_recorded_ts')))
        local = [stamp for stamp in local if stamp is not None and stamp > 0
                 and current is not None and stamp <= current]
        if local:
            confirmed_times.append(min(local))
        records = {}
        valid = True
        for fill in fills:
            if (fill.get('order_ref') != order['order_ref']
                    or fill.get('cycle_id') != live['cycle_id'] or fill.get('venue') != order.get('venue')):
                continue
            key = fill.get('exchange_fill_key')
            qty, price, stamp = (number(fill.get(field)) for field in ('units', 'price', 'ts'))
            if (not key or qty is None or qty <= 0 or qty != qty.to_integral_value()
                    or price is None or price <= 0 or stamp is None or stamp <= 0
                    or current is None or stamp > current):
                valid = False
                break
            record = (qty, price, stamp)
            if key in records and records[key] != record:
                valid = False
                break
            records[key] = record
        covered = sum((record[0] for record in records.values()), Decimal(0))
        stamp = (min(record[2] for record in records.values())
                 if valid and covered == units else None)
        if valid and covered > units:
            valid = False
        if valid and stamp is None:
            exact = [trace for trace in matches if number(trace.get('result_units')) == units]
            if len(exact) == 1 and (number(exact[0].get('actual_fill_vwap')) or 0) > 0:
                trace = exact[0]
                stamp = number(trace.get('exchange_first_fill_ts'))
                if trace.get('exchange_first_fill_ts') is None:
                    # Legacy finalization stored min(history timestamps) here.
                    # An explicitly last-only measurement cannot establish first.
                    stamp = number(trace.get('actual_fill_ts'))
                    if stamp == number(trace.get('exchange_last_fill_ts')):
                        stamp = None
        if not valid or stamp is None or stamp <= 0 or current is None or stamp > current:
            first_complete = False
            missing_first.append(order['order_ref'])
        else:
            first_times.append(stamp)
    first = min(first_times) if first_complete and first_times else None
    first_confirmed = min(confirmed_times) if confirmed_times else None
    if first is not None and (current is None or first > current):
        first = None
    inventory = {'entropy': Decimal(0), 'hedge': Decimal(0)}
    flat = bool(orders) and live.get('phase') == 'DONE' and bool(live.get('position_evidence_authoritative'))
    for order in orders:
        units, requested = number(order.get('filled_units')), number(order.get('requested_units'))
        if (order.get('state') != 'TERMINAL' or order.get('venue') not in inventory
                or order.get('side') not in ('buy', 'sell') or units is None or units < 0
                or units != units.to_integral_value() or requested is None or requested < 0
                or requested != requested.to_integral_value() or units > requested):
            flat = False
            continue
        inventory[order['venue']] += units if order['side'] == 'buy' else -units
    flat = flat and all(number(live.get(f'{venue}_units')) == units == 0
                        for venue, units in inventory.items())
    confirmed = [number(value) for value in (live.get('last_authoritative_ts'),
                                             economics.get('authoritative_reconcile_complete_ts'))]
    confirmed = [stamp for stamp in confirmed if stamp is not None and stamp > 0]
    end = max(confirmed) if flat and confirmed else None
    terminal_times = [number(o.get(field)) for o in orders
                      for field in ('terminal_received_ts', 'terminal_recorded_ts')
                      if o.get(field) is not None]
    if end is not None and (current is None or end > current
                            or any(stamp is None or stamp <= 0 or stamp > end for stamp in terminal_times)
                            or any(stamp > end for stamp in first_times + confirmed_times)):
        end = None
    elapsed = (end if live.get('phase') == 'DONE' else current)
    known = first is not None and elapsed is not None and elapsed >= first
    estimated = first_confirmed is not None and elapsed is not None and elapsed >= first_confirmed
    return {'first_fill_at': float(first) if first is not None else None,
            'first_fill_missing_order_refs': list(dict.fromkeys(missing_first)),
            'first_fill_confirmed_at': float(first_confirmed) if first_confirmed is not None else None,
            'flat_confirmed_at': float(end) if end is not None else None,
            'hold_seconds': float(elapsed - first) if known else None,
            'duration_state': ('closed' if live.get('phase') == 'DONE' else 'active') if known else 'unknown',
            'estimated_hold_seconds': float(elapsed - first_confirmed) if estimated else None,
            'confirmation_duration_state': ('closed' if live.get('phase') == 'DONE' else 'active') if estimated else 'unknown'}


def _cycle_aliases(live: dict[str, Any], canonical: dict[str, Any],
                   economics: dict[str, Any], orders: list[dict[str, Any]],
                   *, now: float, window: Window | None = None,
                   telemetry: list[dict[str, Any]] | None = None,
                   fills: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    orders = list({o['order_ref']: o for o in orders}.values())
    opening = _opening_orders(orders)
    paired = _paired_open(orders)
    paired_times = [number(o.get("terminal_recorded_ts") or o.get("terminal_received_ts"))
                    for o in opening if int(o.get("filled_units") or 0) > 0]
    paired_at = (float(max(paired_times)) if paired and paired_times
                 and all(ts is not None for ts in paired_times) else None)
    completed_at = canonical.get("completed_ts")
    final = bool(canonical.get("accounting_final")) and number(
        canonical.get("pnl_usd")) is not None
    known_unknown = [o.get("had_unknown") for o in orders]
    had_unknown = (any(bool(value) for value in known_unknown) if all(
        value is not None for value in known_unknown) else None)
    positive_entropy = sum(int(o.get("filled_units") or 0) for o in opening
                           if o.get("venue") == "entropy")
    positive_hedge = sum(int(o.get("filled_units") or 0) for o in opening
                         if o.get("venue") == "hedge")
    return {
        "started_at": live.get("started_ts"), "paired_at": paired_at,
        "completed_at": completed_at, "side": live.get("direction"),
        "actual_units": min(positive_entropy, positive_hedge) if paired else None,
        **_cycle_duration(live, economics, orders, telemetry or [], fills or [], now=now),
        "open_zero_fill_count": sum(_submitted(o) and o.get("state") == "TERMINAL"
                                    and int(o.get("filled_units") or 0) == 0
                                    for o in opening),
        "close_zero_fill_count": sum(_submitted(o) and o.get("state") == "TERMINAL"
                                     and int(o.get("filled_units") or 0) == 0
                                     for o in orders if bool(o.get("reduce_only"))),
        "finalized_net_usd": str(number(canonical.get("pnl_usd"))) if final else None,
        "accounting_state": "final" if final else "pending" if completed_at else "active",
        "quality": "partial" if any(o.get("state") != "TERMINAL" for o in orders)
        else "good" if orders else "unknown",
        "had_unknown": had_unknown,
        "inherited": (window.from_ts is not None and live.get("started_ts") is not None
                      and float(live["started_ts"]) < window.from_ts)
        if window is not None else None,
        "entry_anchor_bps": live.get("live_entry_basis_bps") or economics.get("live_entry_basis_bps"),
        "anchor_source": live.get("live_entry_basis_source") or economics.get("live_entry_basis_source"),
    }


def _pnl_breakdown(canonical: dict[str, Any], econ: dict[str, Any]) -> dict[str, str] | None:
    net = number(canonical.get("pnl_usd")) if canonical.get("accounting_final") else None
    fees = number(econ.get("actual_fees_usd"))
    funding = number(econ.get("actual_funding_usd"))
    econ_net = number(econ.get("finalized_pnl_usd"))
    if (net is None or not bool(econ.get("accounting_final")) or
            fees is None or funding is None or econ_net != net):
        return None
    # Canonical cash includes signed fill cash, minus fees, plus signed funding.
    return {"trade_cash_usd": str(net + fees - funding), "fees_usd": str(fees),
            "funding_usd": str(funding), "net_usd": str(net)}


def _largest_negative_component(breakdown: dict[str, str] | None) -> str | None:
    if breakdown is None:
        return None
    components = {"成交价差现金结果": Decimal(breakdown["trade_cash_usd"]),
                  "手续费": -Decimal(breakdown["fees_usd"]),
                  "资金费": Decimal(breakdown["funding_usd"])}
    label, amount = min(components.items(), key=lambda item: item[1])
    return label if amount < 0 else None


def cycle_facts(snapshot: dict[str, Any], window: Window) -> dict[str, Any]:
    journal = snapshot.get("journal")
    if journal is None:
        return {"available": False, "reason": "journal_unavailable"}
    canonical = {r["cycle_id"]: r for r in journal["cycles"]}
    economics = {r["cycle_id"]: r for r in journal["cycle_economics"]}
    by_cycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    telemetry_by_cycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    fills_by_cycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    order_rows = list({(row['cycle_id'], row['order_ref']): row for row in journal['orders']}.values())
    for row in order_rows:
        by_cycle[row["cycle_id"]].append(row)
    for row in journal.get('execution_economics_steps', []):
        telemetry_by_cycle[row['cycle_id']].append(row)
    for row in journal.get('fee_fills', []):
        fills_by_cycle[row['cycle_id']].append(row)
    items = []
    for live in journal["live_cycles"]:
        if not _selected_cycle(live, window):
            continue
        cid = live["cycle_id"]
        canon = canonical.get(cid, {})
        econ = economics.get(cid, {})
        orders = by_cycle.get(cid, [])
        opening = _opening_orders(orders)
        submit = any(_submitted(order) for order in opening)
        # UNKNOWN, registered-only and missing terminal proof remain unknown.
        zero_fill = bool(opening) and submit and all(
            order.get("state") == "TERMINAL" and int(order.get("filled_units") or 0) == 0
            and _submitted(order) for order in opening)
        paired = _paired_open(orders)
        has_confirmed_fill = any(o.get("state") == "TERMINAL" and
                                 int(o.get("filled_units") or 0) > 0 for o in orders)
        complete = (live.get("phase") == "DONE" and paired
                    and live.get("entropy_units") == live.get("hedge_units") == 0
                    and bool(live.get("position_evidence_authoritative"))
                    and canon.get("completed_ts") is not None
                    and bool(orders) and all(o.get("state") == "TERMINAL" for o in orders))
        pnl = number(canon.get("pnl_usd"))
        accounting_final = bool(canon.get("accounting_final")) and pnl is not None
        breakdown = _pnl_breakdown(canon, econ)
        items.append({**live, **_cycle_aliases(live, canon, econ, orders,
                                               now=snapshot["generated_at"], window=window,
                                               telemetry=telemetry_by_cycle[cid], fills=fills_by_cycle[cid]),
                      "completed_ts": canon.get("completed_ts"),
                      "result_kind": canon.get("result_kind"),
                      "pnl_usd": str(pnl) if accounting_final else None,
                      "accounting_final": accounting_final,
                      "submitted_open_campaign": submit,
                      "has_confirmed_fill": has_confirmed_fill,
                      "largest_negative_component": _largest_negative_component(breakdown),
                      "terminal_zero_fill_campaign": zero_fill,
                      "paired_opened": paired, "paired_round_trip": complete,
                      "opening_attempts_submitted": sum(_submitted(o) for o in opening),
                      "opening_terminal_zero_attempts": sum(
                          o.get("state") == "TERMINAL" and _submitted(o)
                          and int(o.get("filled_units") or 0) == 0 for o in opening),
                      "opening_unknown_attempts": sum(
                          o.get("state") != "TERMINAL" for o in opening),
                      "exit_trigger_ts": econ.get("exit_trigger_ts"),
                      "exit_trigger_reason": econ.get("exit_trigger_reason"),
                      "exit_trigger_executable_pnl_usd": econ.get("exit_trigger_executable_pnl_usd"),
                      "economics_available": bool(econ),
                      "funding_state": funding_cycle(snapshot, cid)})
    created = [r for r in items if window.contains(r.get("started_ts"))]
    completed = [r for r in items if window.contains(r.get("completed_ts"))]
    active = [r for r in items if r.get("phase") != "DONE"]
    finalized = [r for r in completed if r["accounting_final"]]
    pending = [r for r in completed if not r["accounting_final"]]
    pnl = sum((Decimal(r["pnl_usd"]) for r in finalized), Decimal(0))
    submitted_count = sum(r["submitted_open_campaign"] for r in created)
    paired_count = sum(r["paired_opened"] for r in created)
    selected_ids = {r["cycle_id"] for r in items}
    window_hours = (Decimal(str((window.to_ts - window.from_ts) / 3600))
                    if window.from_ts is not None and window.to_ts is not None else None)
    return {
        "available": True, "items": items, "created": created, "completed": completed,
        "active": active, "pending_accounting": pending,
        "kpis": {
            "finalized_net_pnl_usd": str(pnl),
            "paired_round_trips": sum(r["paired_round_trip"] for r in completed),
            "losing_cycles": sum(Decimal(r["pnl_usd"]) < 0 for r in finalized
                                 if r["paired_opened"]),
            "paired_opened_cycles": paired_count,
            "open_campaigns_submitted": submitted_count,
            "terminal_zero_fill_campaigns": sum(r["terminal_zero_fill_campaign"] for r in created),
            "campaign_success_rate": str(Decimal(paired_count) / submitted_count)
            if submitted_count else None,
            "pending_accounting_cycles": len(pending),
            "current_executable_net_usd": None,
            "completed_cycles_per_hour": str(
                Decimal(sum(r["paired_round_trip"] for r in completed)) / window_hours)
            if window_hours is not None and window_hours > 0 else None,
            "created_cycles": len(created),
            "submitted_orders": sum(o.get("cycle_id") in selected_ids
                                    and _submitted(o) and window.contains(o.get("submitted_ts"))
                                    for o in order_rows),
        }}


def overview(snapshot: dict[str, Any], window: Window) -> dict[str, Any]:
    facts = cycle_facts(snapshot, window)
    raw_live = snapshot["sources"].get("live", {}).get("data")
    live = public_status(raw_live)
    diagnostic = _cycle_diagnostic(snapshot, (raw_live or {}).get('cycle_id'))
    if diagnostic is None and raw_live is not None:
        diagnostic = {"source": "live_status_summary", "reason": _safe_text(raw_live.get("reason")),
                      "source_ts": raw_live.get("source_ts")}
        stamp = number(raw_live.get("source_ts"))
        if ("executable_gross_usd" in raw_live and stamp is not None
                and 0 <= snapshot["generated_at"] - float(stamp) <= 10):
            diagnostic["exit"] = {key: raw_live.get(key) for key in (
                "executable_gross_usd", "executable_net_usd", "normal_exit_rule",
                "normal_exit_min_gross_profit_usd", "reversion_bps")}
            diagnostic["exit"].update(anchor_source=raw_live.get("exit_anchor_source"))
    if not facts["available"]:
        return {"kpis": {key: None for key in (
            "finalized_net_pnl_usd", "paired_round_trips", "paired_opened_cycles",
            "open_campaigns_submitted", "terminal_zero_fill_campaigns",
            "campaign_success_rate", "pending_accounting_cycles",
            "current_executable_net_usd")}, "live": live, "active_cycle": None,
                "recent_cycles": [], "diagnostic": diagnostic,
                "missing_reason": facts["reason"], "funding": funding_dashboard(snapshot),
                "profit": profit_overview(snapshot, window, facts)}
    active = sorted(facts["active"], key=lambda r: r.get("started_ts") or 0, reverse=True)
    recent = sorted((r for r in facts["created"] if r["has_confirmed_fill"]),
                    key=lambda r: r.get("started_ts") or 0,
                    reverse=True)[:8]
    kpis = dict(facts["kpis"])
    if active and raw_live and raw_live.get("cycle_id") == active[0]["cycle_id"]:
        valuation = number(raw_live.get("executable_net_usd"))
        source_time = number(raw_live.get("source_ts"))
        if (valuation is not None and source_time is not None and
                snapshot["generated_at"] - float(source_time) <= 10):
            kpis["current_executable_net_usd"] = str(valuation)
            active[0] = {**active[0], "executable_net_usd": str(valuation),
                         "valuation_as_of": float(source_time)}
    return {"kpis": kpis, "live": live, "active_cycle": active[0] if active else None,
            "active_cycle_count": len(active), "recent_cycles": recent,
            "diagnostic": diagnostic, "funding": funding_dashboard(snapshot),
            "profit": profit_overview(snapshot, window, facts)}


def trader_cycle_view(items: list[dict[str, Any]], view: str,
                      sort: str) -> list[dict[str, Any]]:
    """Filter before pagination, using only confirmed fills and final accounting."""
    if view == "filled":
        items = [r for r in items if r["has_confirmed_fill"]]
    elif view == "complete":
        items = [r for r in items if r["paired_round_trip"]]
    elif view == "open":
        items = [r for r in items if r["has_confirmed_fill"] and r.get("phase") != "DONE"]
    elif view == "loss":
        items = [r for r in items if r["paired_opened"] and r["accounting_final"]
                 and Decimal(r["pnl_usd"]) < 0]
    elif view == "profit":
        items = [r for r in items if r["paired_opened"] and r["accounting_final"]
                 and Decimal(r["pnl_usd"]) > 0]
    elif view == "unfilled":
        items = [r for r in items if r["terminal_zero_fill_campaign"]]
    if sort in {"loss", "profit"}:
        items = sorted(items, key=lambda r: (
            not (r["accounting_final"] and r["pnl_usd"] is not None),
            (Decimal(r["pnl_usd"]) if sort == "loss" else -Decimal(r["pnl_usd"]))
            if r["pnl_usd"] is not None else Decimal(0),
            -float(r.get("started_ts") or 0), r["cycle_id"]))
    elif sort == "hold":
        items = sorted(items, key=lambda r: (
            r.get("hold_seconds") is not None,
            float(r.get("hold_seconds") or 0),
            float(r.get("started_ts") or 0), r["cycle_id"]), reverse=True)
    else:
        items = sorted(items, key=lambda r: (float(r.get("started_ts") or 0),
                                             r["cycle_id"]), reverse=True)
    return items


def cycle_detail(snapshot: dict[str, Any], cycle_id: str) -> dict[str, Any] | None:
    journal = snapshot.get("journal")
    if journal is None:
        return None
    live = next((r for r in journal["live_cycles"] if r["cycle_id"] == cycle_id), None)
    if live is None:
        return None
    canonical = next((r for r in journal["cycles"] if r["cycle_id"] == cycle_id), {})
    econ = next((r for r in journal["cycle_economics"] if r["cycle_id"] == cycle_id), {})
    orders = {r["order_ref"]: r for r in journal["orders"] if r["cycle_id"] == cycle_id}
    steps = {r["step_key"]: r for r in journal["live_steps"] if r["cycle_id"] == cycle_id}
    telemetry = {r["step_key"]: r for r in journal["execution_economics_steps"]
                 if r["cycle_id"] == cycle_id}
    groups: dict[str, dict[str, Any]] = {}
    for key, step in steps.items():
        order = orders.get(step["order_ref"], {})
        trace = telemetry.get(key, {})
        group_id = step.get("execution_group_id") or trace.get("execution_group_id") or key
        group = groups.setdefault(group_id, {"group_id": group_id, "kind": None,
                                             "attempts": [], "attribution": None})
        purpose = str(trace.get("purpose") or "")
        if purpose.startswith("open"):
            group["kind"] = "open"
        elif purpose.startswith("close"):
            group["kind"] = "close"
        else:
            group["kind"] = group["kind"] or "recovery"
        submitted = order.get("submitted_ts") is not None
        outcome = ("terminal_positive" if order.get("state") == "TERMINAL" and
                   int(order.get("filled_units") or 0) > 0 else
                   "terminal_zero" if order.get("state") == "TERMINAL" and submitted else
                   "UNKNOWN" if order.get("state") in ("UNKNOWN", "SUBMITTING") else
                   "registered_only")
        group["attempts"].append({**step, **trace, **order,
                                  "order_ref": step["order_ref"],
                                  "submitted": submitted, "order_outcome": outcome,
                                  "analytics_missing": not bool(trace)})
    # Unlinked order facts are visible rather than silently dropped.
    linked = {r["order_ref"] for g in groups.values() for r in g["attempts"]}
    for ref, order in orders.items():
        if ref not in linked:
            groups[ref] = {"group_id": ref, "kind": "unlinked", "attribution": None,
                           "attempts": [{**order, "submitted": order.get("submitted_ts") is not None,
                                         "order_outcome": "unknown_history",
                                         "analytics_missing": True}]}
    for group in groups.values():
        group["attempts"].sort(key=lambda r: (r.get("submit_ts") or r.get("submitted_ts") or 0,
                                               str(r.get("order_ref"))))
        group["attribution"] = conservative_attribution(group["attempts"])
        attr = group["attribution"]
        attr.update({"A": attr.get("a_basis_bps"), "B": attr.get("b_basis_bps"),
                     "C": attr.get("c_basis_bps"), "D": attr.get("d_basis_bps"),
                     "a_to_b_bps": attr.get("a_to_b_adverse_bps"),
                     "b_to_c_bps": attr.get("b_to_c_adverse_bps"),
                     "c_to_d_bps": attr.get("c_to_d_adverse_bps")})
        group["matched_units"] = attr.get("matched_units")
        group["quality"] = attr.get("measurement_quality")
        group["purpose"] = group["kind"]
        visible = ("step_key", "order_ref", "venue", "side", "purpose", "attempt",
                   "requested_units", "result_units", "filled_units", "reduce_only",
                   "state", "order_outcome", "submitted", "submitted_ts", "submit_ts",
                   "terminal_recorded_ts", "terminal_received_ts", "result_ts",
                   "actual_fill_ts", "exchange_first_fill_ts", "exchange_last_fill_ts",
                   "room_bps", "hard_cap_bps", "limit_px", "actual_fill_vwap",
                   "avg_px", "actual_fee_usd", "fee_usd", "had_unknown",
                   "confirmation_source", "terminal_source", "measurement_quality",
                   "execution_path", "execution_group_id", "parent_step_key",
                   "analytics_missing", "http_post_start", "terminal_observed_ts",
                   "runtime_consumed_ts", "submit_start_mono_ns",
                   "terminal_observed_mono_ns", "clock_session_id",
                   "terminal_clock_session_id", "quantity_step",
                   "decision_quote_json", "submit_quote_json",
                   "fill_mark_quote_json", "fill_mark_time_source")
        group["attempts"] = [{**{key: attempt.get(key) for key in visible},
                              "remaining_units": max(0, int(attempt.get("requested_units") or 0)
                                                     - int(attempt.get("filled_units") or 0))
                              if attempt.get("state") == "TERMINAL" else None,
                              "avg_fill_px": attempt.get("actual_fill_vwap") or attempt.get("avg_px"),
                              "cap_bps": attempt.get("hard_cap_bps"),
                              "submitted_at": attempt.get("submitted_ts") or attempt.get("submit_ts"),
                              "terminal_at": attempt.get("terminal_recorded_ts")
                              or attempt.get("terminal_observed_ts")}
                             for attempt in group["attempts"]]
    cycle = {**live, **canonical, **_cycle_aliases(
        live, canonical, econ, list(orders.values()), now=snapshot["generated_at"],
        telemetry=list(telemetry.values()),
        fills=[row for row in journal.get('fee_fills', []) if row.get('cycle_id') == cycle_id])}
    cycle['funding_state'] = funding_cycle(snapshot, cycle_id)
    cycle["has_confirmed_fill"] = any(
        o.get("state") == "TERMINAL" and int(o.get("filled_units") or 0) > 0
        for o in orders.values())
    cycle["paired_opened"] = _paired_open(list(orders.values()))
    cycle["paired_round_trip"] = (
        live.get("phase") == "DONE" and cycle["paired_opened"]
        and live.get("entropy_units") == live.get("hedge_units") == 0
        and bool(live.get("position_evidence_authoritative"))
        and canonical.get("completed_ts") is not None
        and bool(orders) and all(o.get("state") == "TERMINAL" for o in orders.values()))
    if not canonical.get("accounting_final"):
        cycle["pnl_usd"] = None
        cycle["pnl_reason"] = "pending_accounting"
    detail_econ = dict(econ) if econ else None
    if detail_econ is not None and not canonical.get("accounting_final"):
        detail_econ["finalized_pnl_usd"] = None
    breakdown = _pnl_breakdown(canonical, econ)
    return {"cycle": cycle, "groups": list(groups.values()),
            "pnl_breakdown": breakdown,
            "economics": detail_econ,
            "diagnostic": _cycle_diagnostic(snapshot, cycle_id)}


def _quote_from_book(snapshot: Any, venue: str, side: str, units: int) -> Decimal | None:
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except ValueError:
            return None
    if not isinstance(snapshot, dict):
        return None
    book = snapshot.get(venue)
    if not isinstance(book, dict):
        return None
    from .v3_readmodel import _quote
    return _quote(book.get("asks" if side == "buy" else "bids"), units)


def conservative_attribution(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    """Matched-q A/B/C/D only; no value if provenance or units are incomplete."""
    empty = {"matched_units": None, "a_basis_bps": None, "b_basis_bps": None,
             "c_basis_bps": None, "d_basis_bps": None,
             "a_to_b_adverse_bps": None, "b_to_c_adverse_bps": None,
             "c_to_d_adverse_bps": None, "entropy_execution_loss_usd": None,
             "hedge_execution_loss_usd": None, "total_execution_loss_usd": None,
             "measurement_quality": "incomplete", "reason": "paired_fills_unavailable"}
    first = [r for r in attempts if r.get("venue") == "entropy"
             and number(r.get("actual_fill_vwap")) is not None
             and int(r.get("result_units") or 0) > 0]
    hedges = [r for r in attempts if r.get("venue") == "hedge"
              and number(r.get("actual_fill_vwap")) is not None
              and int(r.get("result_units") or 0) > 0]
    if len(first) != 1 or not hedges:
        return empty
    initial = first[0]
    e_units = int(initial["result_units"])
    h_units = sum(int(r["result_units"]) for r in hedges)
    if e_units != h_units or any(r.get("parent_step_key") not in (None, initial.get("step_key"))
                                 for r in hedges):
        return {**empty, "matched_units": min(e_units, h_units),
                "reason": "unmatched_units_or_parent"}
    side = str(initial.get("side", "")).lower()
    if side not in ("buy", "sell") or any(str(r.get("side", "")).lower() == side for r in hedges):
        return {**empty, "reason": "side_mismatch"}
    opposite = "sell" if side == "buy" else "buy"
    e_px = number(initial["actual_fill_vwap"])
    h_px = sum(Decimal(str(r["actual_fill_vwap"])) * int(r["result_units"])
               for r in hedges) / h_units
    def basis(e: Decimal | None, h: Decimal | None) -> str | None:
        return str((e / h - 1) * 10000) if e is not None and h is not None and h > 0 else None
    a = basis(_quote_from_book(initial.get("decision_quote_json"), "entropy", side, e_units),
              _quote_from_book(initial.get("decision_quote_json"), "hedge", opposite, e_units))
    b = basis(_quote_from_book(initial.get("submit_quote_json"), "entropy", side, e_units),
              _quote_from_book(initial.get("submit_quote_json"), "hedge", opposite, e_units))
    c = basis(e_px, _quote_from_book(initial.get("fill_mark_quote_json"), "hedge",
                                      opposite, e_units))
    d = basis(e_px, h_px)
    sign = 1 if side == "buy" else -1
    qstep = number(initial.get("quantity_step"))
    e_benchmark = _quote_from_book(initial.get("submit_quote_json"), "entropy", side, e_units)
    h_benchmark = _quote_from_book(initial.get("fill_mark_quote_json"), "hedge", opposite, e_units)
    e_loss = h_loss = None
    qstep_consistent = (qstep is not None and qstep > 0 and all(
        number(row.get("quantity_step")) == qstep for row in hedges))
    if qstep_consistent:
        q = qstep * e_units
        if e_benchmark is not None:
            e_loss = Decimal(sign) * q * (e_px - e_benchmark)
        if h_benchmark is not None:
            h_loss = -Decimal(sign) * q * (h_px - h_benchmark)
    adverse = (lambda start, end: str(Decimal(sign) *
               (Decimal(end) - Decimal(start))) if start is not None and end is not None else None)
    complete = (all((a, b, c, d)) and qstep_consistent and
                initial.get("fill_mark_time_source") in {
                    "terminal_observed_local", "runtime_consumed_local", "exchange_asof"})
    return {"matched_units": e_units, "a_basis_bps": a, "b_basis_bps": b,
            "c_basis_bps": c, "d_basis_bps": d,
            "a_to_b_adverse_bps": adverse(a, b),
            "b_to_c_adverse_bps": adverse(b, c),
            "c_to_d_adverse_bps": adverse(c, d),
            "entropy_execution_loss_usd": str(e_loss) if e_loss is not None else None,
            "hedge_execution_loss_usd": str(h_loss) if h_loss is not None else None,
            "total_execution_loss_usd": str(e_loss + h_loss)
            if e_loss is not None and h_loss is not None else None,
            "fill_mark_time_source": initial.get("fill_mark_time_source"),
            "measurement_quality": "complete" if complete else "partial",
            "reason": None if complete else "quote_or_fill_mark_or_quantity_provenance_missing"}


def market_coverage(snapshot: dict[str, Any], window: Window, *, source_name: str,
                    loaded_rows: int, first_ts, last_ts, discontinuities: int) -> dict[str, Any]:
    source = snapshot['sources'].get(source_name, {})
    live = source_name == 'live_market'
    prefix = 'live_summary' if live else 'recorder'
    observed = number(source.get('observed_at'))
    latest = max(float(observed), last_ts) if observed is not None and last_ts is not None else float(observed) if observed is not None else last_ts
    reasons = []
    if source.get('reason') == 'missing' or not source:
        reasons.append(prefix + '_source_missing')
    if source.get('quality') == 'unsupported_schema':
        reasons.append(prefix + '_source_unsupported_schema')
    if source.get('backfill_complete') is False and source.get('reason') != 'missing':
        reasons.append('historical_backfill_in_progress')
    if first_ts is None and not reasons:
        reasons.append('no_' + prefix + '_samples_in_selected_window')
    if first_ts is not None and window.from_ts is not None and first_ts-window.from_ts > 10:
        reasons.append('window_starts_before_' + prefix + '_coverage')
    if last_ts is not None and window.to_ts is not None and window.to_ts-last_ts > 10:
        reasons.append('window_ends_after_' + prefix + '_coverage')
    if discontinuities:
        reasons.append(prefix + '_samples_have_gaps_or_invalid_frames')
    stale = latest is not None and snapshot['generated_at']-float(latest) > 5
    if stale:
        reasons.append(prefix + '_source_stale')
    quality = source.get('quality', 'unavailable')
    if stale:
        quality = 'stale'
    elif reasons and quality == 'available':
        quality = 'partial'
    return {'backfill_complete': source.get('backfill_complete'), 'loaded_rows': loaded_rows,
        'quality': quality, 'requested_from_ts': window.from_ts, 'requested_to_ts': window.to_ts,
        'from_ts': first_ts, 'to_ts': last_ts, 'latest_ts': latest,
        'discontinuity_count': discontinuities, 'reason': reasons[0] if reasons else None,
        'reasons': reasons}


def _market_boundary(point, previous):
    boundary = bool(point.get('continuity_break')) or not point.get('valid')
    if previous is not None:
        a_ts, b_ts = point.get('ts'), previous.get('ts')
        boundary |= (point.get('session_id') != previous.get('session_id') or
            point.get('_source_span_id') != previous.get('_source_span_id') or
            a_ts is None or b_ts is None or a_ts-b_ts > 3 or a_ts < b_ts)
        a, b = previous.get('sample_seq'), point.get('sample_seq')
        if type(a) is int and type(b) is int and point.get('schema_version') == 2:
            boundary |= b != a+1
    return boundary


def _sample_market_points(points, max_points, fields, row_count=None):
    """Select first/last/extrema and propagate only boundaries in raw intervals."""
    if row_count is None:
        points = list(points)
        row_count = len(points)
    if row_count <= max_points:
        points = list(points)
        return points if len(points) <= max_points else _sample_market_points(points, max_points, fields)
    per_bucket = 2+2*len(fields)
    buckets = max(1, max_points//per_bucket)
    stride = (row_count+buckets-1)//buckets
    sampled = []
    previous_breaks = None
    breaks = 0
    low, high = [None]*len(fields), [None]*len(fields)
    candidates = {}
    first = last = None

    def flush():
        nonlocal previous_breaks, low, high, candidates, first, last
        if first is None:
            return
        candidates.update((first, last))
        for item in (*low, *high):
            if item is not None:
                _, index, point, count = item
                candidates[index] = (point, count)
        for index in sorted(candidates):
            point, count = candidates[index]
            point = dict(point)
            if previous_breaks is not None and count > previous_breaks:
                point['continuity_break'] = True
            sampled.append(point)
            previous_breaks = count
        low, high = [None]*len(fields), [None]*len(fields)
        candidates, first, last = {}, None, None

    for index, point in enumerate(points):
        if index and index % stride == 0:
            flush()
        breaks += bool(point.get('continuity_break'))
        entry = (index, (point, breaks))
        first = first or entry
        last = entry
        for slot, field in enumerate(fields):
            value = number(point.get(field))
            if value is not None:
                if low[slot] is None or value < low[slot][0]:
                    low[slot] = (value, index, point, breaks)
                if high[slot] is None or value > high[slot][0]:
                    high[slot] = (value, index, point, breaks)
    flush()
    return sampled if len(sampled) <= max_points else _sample_market_points(sampled, max_points, fields)


def live_market_points(snapshot: dict[str, Any], window: Window, *, max_points=1200,
                       pair=None, cached_rows=None, cached_row_count=None) -> dict[str, Any]:
    rows = cached_rows if cached_rows is not None else sorted((row for row in snapshot.get('live_market', [])
        if window.contains(row.get('capture_wall_ts')) and (pair is None or row.get('pair_id') == pair)),
        key=lambda row: row['capture_wall_ts'])
    loaded_rows = discontinuities = 0
    first_ts = last_ts = None
    def frames():
        nonlocal loaded_rows, discontinuities, first_ts, last_ts
        previous = None
        for row in rows:
            point = {'ts': row['capture_wall_ts'], 'pair_id': row.get('pair_id'),
                'session_id': row.get('session_id'), 'mid_basis_bps': row.get('mid_basis_bps'),
                'reference_bps': row.get('reference_bps') if row.get('reference_usable') else None,
                'reference_usable': row.get('reference_usable') is True,
                'valid': row.get('valid') is True, 'continuity_break': bool(row.get('continuity_break')),
                '_source_span_id': row.get('_source_span_id')}
            point['continuity_break'] = _market_boundary(point, previous)
            discontinuities += point['continuity_break']
            if loaded_rows == 0:
                first_ts = point['ts']
            last_ts = point['ts']
            loaded_rows += 1
            previous = point
            yield point
    points = _sample_market_points(frames(), max_points, ('mid_basis_bps', 'reference_bps'), cached_row_count)
    coverage = market_coverage(snapshot, window, source_name='live_market', loaded_rows=loaded_rows,
        first_ts=first_ts, last_ts=last_ts, discontinuities=discontinuities)
    for point in points:
        point.pop('_source_span_id', None)
    coverage['display_points'] = len(points)
    return {'points': points, 'coverage': coverage}


def market_points(snapshot: dict[str, Any], window: Window, *, units: int,
                  max_points: int = 1200, pair: str | None = None,
                  cached_rows: Iterable[dict[str, Any]] | None = None,
                  cached_row_count: int | None = None) -> dict[str, Any]:
    if cached_rows is None:
        rows = [r for r in snapshot.get("market", []) if window.contains(r.get("capture_wall_ts"))
                and (pair is None or r.get("pair_id") == pair)]
        rows.sort(key=lambda r: float(r.get("capture_wall_ts") or 0))
    else:
        rows = cached_rows
    live_manifest = snapshot["sources"].get("live_manifest", {}).get("data") or {}
    band = (live_manifest.get("settings") or {}).get("entry_band_bps")
    recorder_manifest = snapshot['sources'].get('recorder_manifest', {}).get('data') or {}
    loaded_rows = 0
    first_ts = last_ts = None
    discontinuities = 0
    steps = set()
    step_missing = False
    threshold_sources = set()
    threshold_values = set()
    episodes: list[dict[str, Any]] = []
    active_episode: dict[str, dict[str, Any]] = {}
    def frames():
        nonlocal loaded_rows, first_ts, last_ts, discontinuities, step_missing
        previous = None
        for row in rows:
            loaded_rows += 1
            parsed_stamp = number(row.get('capture_wall_ts'))
            if parsed_stamp is not None:
                if first_ts is None:
                    first_ts = float(parsed_stamp)
                last_ts = float(parsed_stamp)
            recorded = row.get('config_hash') and row.get('config_hash') == recorder_manifest.get('config_hash')
            row_band = (recorder_manifest.get('settings') or {}).get('entry_band_bps') if recorded else band
            threshold_source = 'recorded_config' if recorded and row_band is not None else 'current_live_display_threshold' if row_band is not None else 'unknown'
            frame = requote_market(row, units, row_band)
            frame['ts'] = float(parsed_stamp) if parsed_stamp is not None else None
            frame['entry_band_source'] = threshold_source
            threshold_sources.add(threshold_source)
            if row_band is not None:
                threshold_values.add(str(row_band))
            instrument = row.get('instrument')
            step = number(row.get('quantity_step')) if row.get('quantity_step_source') == 'recorded_instrument' else number(instrument.get('quantity_step')) if isinstance(instrument, dict) else None
            if step is not None and step > 0:
                steps.add(step)
            elif row.get('valid'):
                step_missing = True
            frame['continuity_break'] = _market_boundary(frame, previous)
            discontinuities += frame['continuity_break']
            if frame['continuity_break']:
                episodes.extend(active_episode.values())
                active_episode.clear()
            threshold = number(frame.get('entry_band_bps'))
            for direction, field in (('long_entropy', 'long_edge_bps'), ('short_entropy', 'short_edge_bps')):
                edge = number(frame.get(field))
                if frame.get('valid') and threshold is not None and edge is not None and edge >= threshold:
                    episode = active_episode.get(direction)
                    if episode is None:
                        episode = {'start_ts': frame.get('ts'), 'end_ts': frame.get('ts'),
                            'direction': direction, 'units': units, 'max_edge_bps': str(edge),
                            'sample_count': 0, 'evidence': 'recorder_observation_only',
                            'live_decision_link': 'unproven', 'created_cycle_id': None,
                            'quality': 'partial' if frame.get('schema_version') == 1 else 'observed'}
                        active_episode[direction] = episode
                    episode['end_ts'] = frame.get('ts')
                    episode['sample_count'] += 1
                    if edge > Decimal(episode['max_edge_bps']):
                        episode['max_edge_bps'] = str(edge)
                elif direction in active_episode:
                    episodes.append(active_episode.pop(direction))
            previous = frame
            yield frame
    points = _sample_market_points(frames(), max_points, ('long_edge_bps', 'short_edge_bps',
        'buy_basis_bps', 'sell_basis_bps', 'mid_basis_bps', 'reference_bps'), cached_row_count)
    episodes.extend(active_episode.values())
    episodes.sort(key=lambda row: row.get("start_ts") or 0, reverse=True)
    coverage = market_coverage(snapshot, window, source_name='market', loaded_rows=loaded_rows,
        first_ts=first_ts, last_ts=last_ts, discontinuities=discontinuities)
    coverage['display_points'] = len(points)
    step = next(iter(steps)) if len(steps) == 1 and not step_missing else None
    return {"units": units, "points": points, "episodes": episodes[:50],
            "episodes_truncated": len(episodes) > 50,
            "coverage": coverage, 'quantity_step': str(step) if step is not None else None,
            'quantity_step_source': 'recorded_instrument' if step is not None else 'unavailable',
            'base_quantity': str(step*units) if step is not None else None,
            'entry_band_bps': next(iter(threshold_values)) if len(threshold_values) == 1 else None,
            'display_entry_band_bps': str(band) if band is not None else None,
            'entry_band_source': next(iter(threshold_sources)) if len(threshold_sources) == 1 else 'mixed' if threshold_sources else 'unknown',
            "diagnostic": _cycle_diagnostic(snapshot, (
                snapshot['sources'].get('live', {}).get('data') or {}).get('cycle_id'))}


def market_diagnostic(snapshot):
    return _cycle_diagnostic(snapshot, (snapshot['sources'].get('live', {}).get('data') or {}).get('cycle_id'))


def economics(snapshot: dict[str, Any], window: Window) -> dict[str, Any]:
    facts = cycle_facts(snapshot, window)
    if not facts["available"]:
        return {"finalized_net_pnl_usd": None, "completed_cycles": None,
                "pending_accounting_cycles": None, "cumulative": [],
                "current_executable_net_usd": None,
                "attribution": None, "reason": facts["reason"]}
    total = Decimal(0)
    series = []
    for row in sorted(facts["completed"], key=lambda r: r.get("completed_ts") or 0):
        if not row["accounting_final"]:
            continue
        total += Decimal(row["pnl_usd"])
        series.append({"ts": row["completed_ts"], "cycle_id": row["cycle_id"],
                       "pnl_usd": row["pnl_usd"], "cumulative_pnl_usd": str(total)})
    known_losses = []
    groups_total = groups_complete = 0
    for row in facts["completed"]:
        detail = cycle_detail(snapshot, row["cycle_id"])
        for group in detail["groups"] if detail else []:
            attr = group["attribution"]
            groups_total += 1
            groups_complete += attr["measurement_quality"] == "complete"
            loss = number(attr.get("total_execution_loss_usd"))
            if loss is not None:
                known_losses.append(loss)
    return {"finalized_net_pnl_usd": str(total),
            "completed_cycles": facts["kpis"]["paired_round_trips"],
            "pending_accounting_cycles": len(facts["pending_accounting"]),
            "cumulative": series, "current_executable_net_usd": None,
            "attribution": {"group_count": groups_total,
                            "complete_group_count": groups_complete,
                            "known_execution_loss_usd": str(sum(known_losses, Decimal(0))),
                            "loss_coverage_count": len(known_losses)},
            "accounting_as_of": snapshot["generated_at"],
            "account_return_pct": None, "account_return_reason": "cashflow_history_unavailable"}


def compatibility(live_manifest: dict[str, Any] | None,
                  recorder_manifest: dict[str, Any] | None) -> dict[str, Any]:
    if not live_manifest or not recorder_manifest:
        return {"status": "unknown", "reason": "manifest_missing"}
    left = live_manifest.get("loaded_modules") or {}
    right = recorder_manifest.get("loaded_modules") or {}
    shared = {name for name in set(left).intersection(right)
              if name.startswith("entropy_arb.")}
    different = sorted(key for key in shared if left[key] != right[key])
    pair_match = live_manifest.get("pair_identity") == recorder_manifest.get("pair_identity")
    config_match = live_manifest.get("config_hash") == recorder_manifest.get("config_hash")
    schema_match = (live_manifest.get("journal_schema") or {}).get(
        "economics_schema_version") in (8, "8")
    return {"status": "compatible" if not different and pair_match and config_match
            and schema_match else "mismatch",
            "shared_module_count": len(shared), "different_shared_modules": different,
            "shared_core_hash_match": not different if shared else None,
            "pair_match": pair_match, "config_match": config_match,
            "schema_match": schema_match,
            "reason": ("shared_module_identity_or_schema_mismatch" if different or not pair_match
                       or not config_match or not schema_match else None),
            "role_build_hashes_may_differ": True}
