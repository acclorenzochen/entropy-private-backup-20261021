"""Pure execution-economics calculations for V3.

This module has no network, order, configuration, or database side effects.
Decimal values are kept as strings at the reporting boundary so summaries are
stable and JSON serializable without silently converting through binary floats.
"""
from __future__ import annotations

from decimal import Decimal
import json
from statistics import mean, median
from typing import Any, Iterable, Mapping

from .core import BPS, Direction, Side, dec


def directional_dislocation(direction: Direction | str, basis_bps: Decimal,
                             reference_bps: Decimal) -> Decimal:
    direction = Direction(direction)
    return (reference_bps - basis_bps if direction is Direction.LONG_ENTROPY
            else basis_bps - reference_bps)


def execution_cost_bps(side: Side | str, fill_px: Decimal,
                       benchmark_px: Decimal) -> Decimal:
    side = Side(side)
    if fill_px <= 0 or benchmark_px <= 0:
        raise ValueError('execution prices must be positive')
    if side is Side.BUY:
        return (fill_px / benchmark_px - 1) * BPS
    return (benchmark_px / fill_px - 1) * BPS


def paired_basis_bps(entropy_vwap: Decimal, hedge_vwap: Decimal) -> Decimal:
    if entropy_vwap <= 0 or hedge_vwap <= 0:
        raise ValueError('paired prices must be positive')
    return (entropy_vwap / hedge_vwap - 1) * BPS


def actual_reversion_bps(direction: Direction | str, entry_basis_bps: Decimal,
                         exit_basis_bps: Decimal) -> Decimal:
    direction = Direction(direction)
    return (exit_basis_bps - entry_basis_bps
            if direction is Direction.LONG_ENTROPY
            else entry_basis_bps - exit_basis_bps)


def break_even_reversion_bps(actual_reversion: Decimal, pnl_usd: Decimal,
                             entry_notional_usd: Decimal) -> Decimal:
    if entry_notional_usd <= 0:
        raise ValueError('entry notional must be positive')
    pnl_bps = pnl_usd / entry_notional_usd * BPS
    return actual_reversion - pnl_bps


def weighted_vwap(fills: Iterable[Mapping[str, Any]]) -> tuple[Decimal, Decimal]:
    quantity = Decimal('0')
    notional = Decimal('0')
    for fill in fills:
        qty = dec(fill['qty'])
        price = dec(fill['price'])
        if qty <= 0 or price <= 0:
            raise ValueError('fill quantity and price must be positive')
        quantity += qty
        notional += qty * price
    if quantity <= 0:
        raise ValueError('at least one fill is required')
    return notional / quantity, quantity


def _percentile(values: list[Decimal], percentile: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = percentile * Decimal(len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _distribution(rows: list[Mapping[str, Any]], field: str,
                  percentiles: tuple[int, ...]) -> dict[str, str | None]:
    values = [dec(row[field]) for row in rows if row.get(field) not in (None, '')]
    return {
        f'p{p}': (str(value) if (value := _percentile(values, Decimal(p) / 100))
                  is not None else None)
        for p in percentiles
    }


def _bucket_name(value: Decimal) -> str:
    if value < 4:
        return '3-4bps'
    if value < 6:
        return '4-6bps'
    if value < 8:
        return '6-8bps'
    return '8bps+'


def _number(value: Any) -> Decimal | None:
    if value in (None, ''):
        return None
    try:
        number = dec(value)
        return number if number.is_finite() else None
    except (ValueError, ArithmeticError):
        return None


def _json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _phase(row: Mapping[str, Any]) -> str:
    purpose = str(row.get('phase_kind') or row.get('purpose') or '')
    if purpose.startswith('open'):
        return 'open'
    if purpose.startswith('close'):
        return 'close'
    return 'recovery'


def _quote_vwap(snapshot: Mapping[str, Any] | None, venue: str,
                side: Side, units: int) -> Decimal | None:
    """Requote the saved depth for the matched quantity; never extrapolate it."""
    if not snapshot or units <= 0:
        return None
    venue_book = snapshot.get(venue)
    if not isinstance(venue_book, Mapping):
        return None
    levels = venue_book.get('asks' if side is Side.BUY else 'bids')
    if not isinstance(levels, list):
        return None
    left = units
    cash = Decimal('0')
    for level in levels:
        if isinstance(level, Mapping):
            price = _number(level.get('price'))
            size = _number(level.get('units'))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price, size = _number(level[0]), _number(level[1])
        else:
            return None
        if price is None or size is None or price <= 0 or size <= 0 \
                or size != size.to_integral_value():
            return None
        take = min(left, int(size))
        cash += price * take
        left -= take
        if left == 0:
            return cash / units
    return None


def _basis_from_snapshot(snapshot: Mapping[str, Any] | None,
                         entropy_side: Side, units: int) -> Decimal | None:
    entropy_px = _quote_vwap(snapshot, 'entropy', entropy_side, units)
    hedge_px = _quote_vwap(snapshot, 'hedge', entropy_side.opposite, units)
    if entropy_px is None or hedge_px is None:
        return None
    return paired_basis_bps(entropy_px, hedge_px)


def _weighted_step_price(rows: list[Mapping[str, Any]]) -> tuple[Decimal | None, int]:
    total_units = 0
    total_cash = Decimal('0')
    for row in rows:
        units = row.get('result_units')
        price = _number(row.get('actual_fill_vwap') or row.get('provisional_avg_px'))
        if units is None or int(units) <= 0:
            continue
        if price is None or price <= 0:
            return None, total_units + int(units)
        total_units += int(units)
        total_cash += price * int(units)
    return ((total_cash / total_units) if total_units else None), total_units


def _distribution_values(values: list[Decimal]) -> dict[str, Any]:
    return {
        'count': len(values),
        **{f'p{p}': (str(value) if (value := _percentile(
            values, Decimal(p) / 100)) is not None else None)
           for p in (50, 90, 95)},
    }


def _interval(rows: list[Mapping[str, Any]], name: str,
              pairs: Iterable[tuple[Any, Any, str]]) -> tuple[str, dict[str, Any]]:
    values: list[Decimal] = []
    missing = negative = 0
    clocks: dict[str, int] = {}
    for start, end, clock in pairs:
        begin, finish = _number(start), _number(end)
        if begin is None or finish is None:
            missing += 1
        elif finish < begin:
            negative += 1
        else:
            divisor = Decimal('1000000') if clock == 'monotonic_ns' else Decimal('0.001')
            values.append((finish - begin) / divisor)
            clocks[clock] = clocks.get(clock, 0) + 1
    count = len(rows)
    return name, {
        **_distribution_values(values),
        'eligible_count': count,
        'missing_count': missing,
        'negative_count': negative,
        'missing_rate': str(Decimal(missing) / count) if count else None,
        'negative_rate': str(Decimal(negative) / count) if count else None,
        'clock_counts': clocks,
    }


def summarize_system(rows: Iterable[Mapping[str, Any]],
                     live_cycles: Iterable[Mapping[str, Any]],
                     canonical_cycles: Iterable[Mapping[str, Any]] | None = None,
                     request_events: Iterable[Mapping[str, Any]] = (),
                     *, window_start_ts: float | None = None,
                     window_end_ts: float | None = None,
                     orders: Iterable[Mapping[str, Any]] = (),
                     valuation: Mapping[str, Any] | None = None,
                     fee_events: Iterable[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """All-cycle cash view; account equity remains unknown without flow data."""
    rows = [dict(row) for row in rows]
    cycles = [dict(row) for row in live_cycles]
    canonical = ([dict(row) for row in canonical_cycles]
                 if canonical_cycles is not None else rows)
    all_cycles = cycles
    by_id = {row['cycle_id']: row for row in cycles}
    canonical_by_id = {row.get('cycle_id'): row for row in canonical}
    all_canonical = canonical
    order_rows = [dict(row) for row in orders]
    explicit = window_start_ts is not None or window_end_ts is not None
    def completion(row):
        live = by_id.get(row.get('cycle_id'), row)
        facts = canonical_by_id.get(row.get('cycle_id'), {})
        return next((value for value in (
            facts.get('completed_ts'), row.get('completed_ts'), live.get('completed_ts'),
            row.get('flat_confirmed_ts'), live.get('flat_confirmed_ts'))
            if value is not None), None)
    starts = [float(row['started_ts']) for row in cycles if row.get('started_ts') is not None]
    ends = [float(row['updated_ts']) for row in cycles if row.get('updated_ts') is not None]
    window_start = window_start_ts if window_start_ts is not None else (min(starts) if starts else None)
    window_end = window_end_ts if window_end_ts is not None else (max(ends) if ends else None)
    if window_start is not None and window_end is not None and window_end < window_start:
        raise ValueError('window_end_ts precedes window_start_ts')
    if any(value is not None and _number(value) is None
           for value in (window_start, window_end)):
        raise ValueError('window timestamps must be finite')
    def inside(ts):
        return ts is not None and (window_start is None or float(ts) >= window_start) \
            and (window_end is None or float(ts) < window_end)
    missing_completion = [row.get('cycle_id') for row in canonical
                          if completion(row) is None]
    def overlaps(row):
        start, end = row.get('started_ts'), completion(row)
        return (window_end is None or start is None or float(start) < window_end) \
            and (window_start is None or end is None or float(end) >= window_start)
    cycles = [row for row in cycles if overlaps(row)]
    if explicit:
        canonical = [row for row in canonical if inside(completion(row))]
        selected_ids = {row.get('cycle_id') for row in canonical}
        rows = [row for row in rows if row.get('cycle_id') in selected_ids]
    window_hours = ((window_end - window_start) / 3600
                    if window_start is not None and window_end is not None
                    and window_end > window_start else None)
    def pnl(row):
        return row.get('pnl_usd') if row.get('pnl_usd') is not None else row.get('finalized_pnl_usd')
    finalized = [row for row in canonical if row.get('accounting_final')
                 and _number(pnl(row)) is not None]
    provisional = [row for row in all_canonical
                   if (not row.get('accounting_final')
                       or (row.get('pnl_usd') if row.get('pnl_usd') is not None
                           else row.get('finalized_pnl_usd')) in (None, ''))
                   and (not explicit or overlaps(by_id.get(row.get('cycle_id'), row)))]
    finalized_economics = [row for row in rows if row.get('accounting_final')]
    total = sum((dec(pnl(row))
                 for row in finalized), Decimal('0'))
    kinds: dict[str, int] = {}
    for row in canonical:
        kind = row.get('result_kind') or 'unclassified'
        kinds[kind] = kinds.get(kind, 0) + 1
    active = [row for row in cycles if row.get('phase') != 'DONE'
              or (window_end is not None and completion(row) is not None
                  and float(completion(row)) >= window_end)]
    active_ids = {row['cycle_id'] for row in active}
    pending_ids = sorted({row['cycle_id'] for row in provisional if row.get('cycle_id')
                          and row['cycle_id'] not in active_ids})
    completed = [row for row in cycles if row.get('phase') == 'DONE'
                 and (not explicit or inside(completion(row)))]
    roundtrips = []
    for row in completed:
        facts = canonical_by_id.get(row['cycle_id'], {})
        cycle_orders = [order for order in order_rows
                        if order.get('cycle_id') == row['cycle_id']]
        opening = [order for order in cycle_orders if order.get('reduce_only') == 0
                   and order.get('state') == 'TERMINAL']
        opening_units = {venue: sum(int(order.get('filled_units') or 0)
                                   * (1 if order.get('side') == 'buy' else -1)
                                   for order in opening if order.get('venue') == venue)
                         for venue in ('entropy', 'hedge')}
        paired = (opening_units['entropy'] != 0
                  and opening_units['entropy'] == -opening_units['hedge'])
        if (row.get('paired_opened') or facts.get('paired_opened') or paired) \
                and row.get('entropy_units') == row.get('hedge_units') == 0 \
                and row.get('position_evidence_authoritative') \
                and completion(row) is not None and cycle_orders \
                and all(order.get('state') == 'TERMINAL' for order in cycle_orders):
            roundtrips.append(row)
    request_groups: dict[str, dict[str, int]] = {}
    missing_request_times = 0
    for row in request_events:
        used_ts = row.get('used_ts')
        if used_ts is None:
            missing_request_times += 1
        if explicit and not inside(used_ts):
            continue
        key = f"{row.get('venue') or 'unknown'}:{row.get('request_kind') or 'unknown'}"
        group = request_groups.setdefault(key, {'events': 0, 'weight': 0})
        group['events'] += 1
        group['weight'] += int(row.get('weight') or 0)
    holding_intervals: list[tuple[float, float]] = []
    economics_by_cycle = {row['cycle_id']: row for row in rows if row.get('cycle_id')}
    for cycle in cycles:
        econ = economics_by_cycle.get(cycle['cycle_id'], {})
        opened = econ.get('first_fill_observed_ts')
        closed = completion(cycle) if cycle.get('phase') == 'DONE' else window_end
        if opened is not None and closed is not None and float(closed) >= float(opened):
            start = max(float(opened), window_start) if window_start is not None else float(opened)
            end = min(float(closed), window_end) if window_end is not None else float(closed)
            if end >= start:
                holding_intervals.append((start, end))
    holding_intervals.sort()
    merged: list[tuple[float, float]] = []
    for start, end in holding_intervals:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    known_holding_seconds = sum(end - start for start, end in merged)
    complete_holding_coverage_count = len(holding_intervals)
    fees_known = [row for row in finalized_economics
                  if row.get('actual_fees_usd') not in (None, '')]
    funding_known = [row for row in finalized_economics
                     if row.get('actual_funding_usd') not in (None, '')]
    fees_in_window = [row for row in fee_events if inside(row.get('event_ts'))
                      and _number(row.get('fee_usd')) is not None]
    return {
        'window_start_ts': window_start,
        'window_end_ts': window_end,
        'window_hours': window_hours,
        'window_basis': ('explicit' if window_start_ts is not None and window_end_ts is not None
                         else 'first_cycle_start_to_last_cycle_update'),
        'window_limit': ('idle after the last cycle update is outside the inferred window'
                         if window_start_ts is None or window_end_ts is None else None),
        'cycle_count': len(cycles), 'result_kind_counts': kinds,
        'canonical_cycle_count': len(canonical),
        'economics_coverage_count': len(rows),
        'finalized_cycle_count': len(finalized),
        'finalized_cycle_pnl_usd': str(total),
        'finalized_cycle_pnl_per_hour_usd': (
            str(total / dec(window_hours)) if window_hours else None),
        'closed_attempts': len(completed),
        'paired_round_trips': len(roundtrips),
        'zero_fill_cycles': kinds.get('zero_fill', 0),
        'repair_unwound_cycles': kinds.get('repair', 0) + kinds.get('unwound', 0),
        'completed_cycle_count': len(roundtrips),
        'completed_cycles_per_hour': (str(dec(len(roundtrips)) / dec(window_hours))
                                      if window_hours else None),
        'known_actual_fees_usd': str(sum(
            (dec(row['actual_fees_usd']) for row in fees_known), Decimal('0'))),
        'actual_fees_coverage_count': len(fees_known),
        'cycle_fee_basis': 'lifetime fees of selected completed cycles, included in cash PnL',
        'window_fee_events_usd': str(sum((_number(row['fee_usd']) for row in fees_in_window),
                                        Decimal('0'))) if fees_in_window else None,
        'window_fee_event_count': len(fees_in_window),
        'known_actual_funding_usd': str(sum(
            (dec(row['actual_funding_usd']) for row in funding_known), Decimal('0'))),
        'actual_funding_coverage_count': len(funding_known),
        'provisional_cycle_count': len(provisional),
        'provisional_cycle_ids': sorted(row['cycle_id'] for row in provisional
                                        if row.get('cycle_id')),
        'censored_completed_cycle_ids': pending_ids,
        'active_cycle_count': len(active),
        'active_cycle_ids': [row['cycle_id'] for row in active],
        'known_holding_seconds': known_holding_seconds,
        'holding_time_basis': 'first_fill_observed_to_immutable_completion_clipped_to_window',
        'holding_coverage_count': complete_holding_coverage_count,
        'idle_seconds': (max(0.0, window_hours * 3600 - known_holding_seconds)
                         if window_hours is not None
                         and complete_holding_coverage_count == len(completed)
                         and not active else None),
        'request_budget_by_venue_and_kind': request_groups,
        'measurement_quality': {
            'missing_completion_cycle_ids': missing_completion,
            'legacy_completion_proxies': [
                {'cycle_id': row['cycle_id'], 'proxy_ts': row.get('updated_ts'),
                 'source': 'mutable_live_cycle_updated_ts'} for row in all_cycles
                if row.get('phase') == 'DONE' and completion(row) is None],
            'request_events_missing_time': missing_request_times,
            'data_coverage_fraction': None,
            'coverage_reason': 'continuous observation coverage unavailable',
        },
        'boundary_start_cycle_ids': [row['cycle_id'] for row in cycles
            if window_start is not None and row.get('started_ts') is not None
            and float(row['started_ts']) < window_start
            and (completion(row) is None or float(completion(row)) >= window_start)],
        'boundary_end_cycle_ids': [row['cycle_id'] for row in active],
        'cash_pnl_basis': 'completed cycle lifetime cash including fees and funding once; opening may precede window',
        'account_equity_change_usd': None,
        'account_equity_change_reason': 'deposit, withdrawal and unrelated-position flows unavailable',
        'open_position_valuation_usd': _open_valuation(active, valuation, window_end),
        'open_position_valuation_basis': 'executable liquidation value plus supplied cash; excludes unknown closing fees/funding',
        'open_position_valuation_source': valuation.get('source') if valuation else None,
        'open_position_valuation_ts': valuation.get('observed_ts') if valuation else None,
    }


def _open_valuation(cycles, valuation, boundary):
    """Value only a timestamped authoritative boundary snapshot at executable depth.

    Input: {observed_ts, source, positions: [{cycle_id, entropy_units,
    hedge_units, cash_net_usd, authoritative}], market: {quantity_step, ...}}.
    Cash must already include known fees/funding; closing fees remain unknown.
    """
    if not valuation or boundary is None or valuation.get('observed_ts') != boundary \
            or not valuation.get('source'):
        return None
    market = valuation.get('market', {})
    step = _number(market.get('quantity_step'))
    if step is None or step <= 0:
        return None
    positions = {row.get('cycle_id'): row for row in valuation.get('positions', [])}
    total = Decimal('0')
    for cycle in cycles:
        row = positions.get(cycle['cycle_id'], {})
        cash = _number(row.get('cash_net_usd'))
        if not row.get('authoritative') or cash is None:
            return None
        total += cash
        for venue in ('entropy', 'hedge'):
            units = _number(row.get(f'{venue}_units'))
            if units is None or units != units.to_integral_value():
                return None
            if units:
                price = _quote_vwap(market, venue, Side.SELL if units > 0 else Side.BUY,
                                    abs(int(units)))
                if price is None:
                    return None
                total += units * step * price
    return str(total) if cycles else None


def _market_received(snapshot: Mapping[str, Any] | None,
                     clock: str) -> Decimal | None:
    if not snapshot:
        return None
    field = 'received_mono_ns' if clock == 'monotonic_ns' else 'received_wall_ts'
    values = []
    for venue in ('entropy', 'hedge'):
        book = snapshot.get(venue)
        if isinstance(book, Mapping):
            value = _number(book.get(field))
            if value is not None:
                values.append(value)
    return max(values) if len(values) == 2 else None


def _mono_field(field: str) -> str:
    return f"{field.removesuffix('_ts')}_mono_ns"


def _event_session(row: Mapping[str, Any], field: str) -> Any:
    # A row-level session cannot prove the provenance of a post-restart event.
    if field == 'terminal_received_ts':
        return row.get('terminal_received_clock_session_id')
    if field == 'terminal_observed_ts':
        return row.get('terminal_clock_session_id') or row.get(
            f"{field.removesuffix('_ts')}_clock_session_id")
    if field in {'attempt_decision_ts', 'submit_start_ts', 'http_post_start'}:
        return row.get(f"{field.removesuffix('_ts')}_clock_session_id") or row.get('clock_session_id')
    if field == 'runtime_consumed_ts':
        return row.get('runtime_consumed_clock_session_id')
    return row.get(f"{field.removesuffix('_ts')}_clock_session_id")


def _step_pair(row: Mapping[str, Any], start: str, end: str) -> tuple[Any, Any, str]:
    start_mono, end_mono = row.get(_mono_field(start)), row.get(_mono_field(end))
    session = _event_session(row, start)
    if start_mono is not None and end_mono is not None and session \
            and session == _event_session(row, end):
        return start_mono, end_mono, 'monotonic_ns'
    return row.get(start), row.get(end), 'wall_utc'


def _cross_pair(first: Mapping[str, Any], second: Mapping[str, Any],
                start: str, end: str) -> tuple[Any, Any, str]:
    if _event_session(first, start) and _event_session(first, start) == _event_session(second, end):
        begin, finish = first.get(_mono_field(start)), second.get(_mono_field(end))
        if begin is not None and finish is not None:
            return begin, finish, 'monotonic_ns'
    return first.get(start), second.get(end), 'wall_utc'


def _signal_pair(row: Mapping[str, Any], econ: Mapping[str, Any]) -> tuple[Any, Any, str]:
    if _phase(row) == 'close':
        return _step_pair(row, 'group_trigger_ts', 'attempt_decision_ts')
    close = _phase(row) == 'close'
    session = econ.get('exit_trigger_clock_session_id' if close
                       else 'signal_clock_session_id')
    begin = econ.get('exit_trigger_mono_ns' if close else 'signal_mono_ns')
    finish = row.get('attempt_decision_mono_ns')
    if session and session == _event_session(row, 'attempt_decision_ts') \
            and begin is not None and finish is not None:
        return begin, finish, 'monotonic_ns'
    return row.get('original_signal_ts'), row.get('attempt_decision_ts'), 'wall_utc'


def _execution_group(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    firsts = [row for row in rows if row.get('venue') == 'entropy'
              and str(row.get('purpose', '')).startswith(('open_first', 'close_first', 'close_pair_entropy_'))
              and row.get('order_outcome') == 'terminal_positive']
    hedges = [row for row in rows if row.get('venue') == 'hedge'
              and (str(row.get('purpose', '')).startswith(('open_hedge', 'close_hedge', 'close_pair_hedge_'))
                   or row.get('parent_step_key'))
              and row.get('order_outcome') == 'terminal_positive']
    firsts.sort(key=lambda row: (int(row.get('attempt') or 0), str(row.get('step_key') or '')))
    hedges.sort(key=lambda row: (int(row.get('attempt') or 0), str(row.get('step_key') or '')))
    group_id = next((row.get('execution_group_id') for row in rows
                     if row.get('execution_group_id')), None)
    result: dict[str, Any] = {
        'cycle_id': rows[0].get('cycle_id'), 'execution_group_id': group_id,
        'phase': _phase(rows[0]), 'attempt_count': len(rows),
        'execution_path': 'unknown',
        'first_step_keys': [row.get('step_key') for row in firsts],
        'hedge_step_keys': [row.get('step_key') for row in hedges],
        'matched_units': 0, 'comparable_units': None,
        'a_basis_bps': None, 'b_basis_bps': None,
        'c_basis_bps': None, 'd_basis_bps': None,
        'a_to_b_adverse_bps': None, 'b_to_c_adverse_bps': None,
        'c_to_d_adverse_bps': None,
        'entropy_execution_loss_usd': None,
        'hedge_execution_loss_usd': None,
        'total_execution_loss_usd': None,
        'measurement_quality': 'incomplete', 'missing_fields': [],
        'fill_mark_time_source': None,
    }
    missing = result['missing_fields']
    if not firsts or not hedges:
        missing.append('paired_positive_fills')
        return result
    first = firsts[0]
    result['execution_path'] = first.get('execution_path') or 'unknown'
    if any(row.get('parent_step_key') not in (None, first.get('step_key')) for row in hedges):
        missing.append('parent_step_mismatch')
        return result
    e_units = sum(int(row['result_units']) for row in firsts)
    h_units = sum(int(row['result_units']) for row in hedges)
    result['matched_units'] = min(e_units, h_units)
    if e_units != h_units:
        missing.append('unmatched_fill_units')
        return result
    units = e_units
    result['comparable_units'] = units
    if len(firsts) != 1:
        missing.append('multiple_first_fill_marks')
        return result
    side = Side(first['side'])
    if any(Side(row['side']) is not side.opposite for row in hedges):
        missing.append('hedge_side_mismatch')
        return result
    e_px, _ = _weighted_step_price(firsts)
    h_px, _ = _weighted_step_price(hedges)
    if e_px is None or h_px is None or any(row.get('actual_fill_vwap') in (None, '')
                                        for row in firsts + hedges):
        missing.append('authoritative_fill_vwap')
        return result
    q_step = _number(first.get('quantity_step'))
    if q_step is None or q_step <= 0:
        missing.append('quantity_step')
    elif any(_number(row.get('quantity_step')) != q_step for row in hedges):
        missing.append('quantity_step_mismatch')
    a_quote = _json_object(first.get('decision_quote_json'))
    b_quote = _json_object(first.get('submit_quote_json'))
    c_quote = _json_object(first.get('fill_mark_quote_json'))
    a = _basis_from_snapshot(a_quote, side, units)
    b = _basis_from_snapshot(b_quote, side, units)
    if result['execution_path'] == 'parallel' and result['phase'] == 'open':
        # Parallel legs can finish in either order. The Entropy fill mark
        # cannot be used as a prior market benchmark for an already-filled H.
        e_benchmark = _quote_vwap(b_quote, 'entropy', side, units)
        h_marks = [(_quote_vwap(_json_object(row.get('submit_quote_json')),
                               'hedge', side.opposite, int(row['result_units'])),
                    int(row['result_units'])) for row in hedges]
        h_benchmark = (sum((price * qty for price, qty in h_marks), Decimal('0')) / h_units
                       if all(price is not None for price, _ in h_marks) else None)
        if a is None:
            missing.append('decision_quote_depth')
        if e_benchmark is None or h_benchmark is None:
            missing.append('per_leg_submit_quote_depth')
        submit_basis = (paired_basis_bps(e_benchmark, h_benchmark)
                        if e_benchmark is not None and h_benchmark is not None else None)
        result.update({
            'a_basis_bps': str(a) if a is not None else None,
            'b_basis_bps': str(submit_basis) if submit_basis is not None else None,
            'd_basis_bps': str(paired_basis_bps(e_px, h_px)),
            'execution_loss_benchmark': 'per_leg_submit_quotes_at_actual_filled_units',
            'not_applicable_fields': ['c_basis_bps', 'b_to_c_adverse_bps', 'c_to_d_adverse_bps'],
        })
        if a is not None and submit_basis is not None:
            result['a_to_b_adverse_bps'] = str(side.sign * (submit_basis - a))
        if q_step is not None and q_step > 0 and 'quantity_step_mismatch' not in missing:
            quantity = q_step * units
            if e_benchmark is not None:
                result['entropy_execution_loss_usd'] = str(side.sign * quantity * (e_px - e_benchmark))
            if h_benchmark is not None:
                result['hedge_execution_loss_usd'] = str(-side.sign * quantity * (h_px - h_benchmark))
            if e_benchmark is not None and h_benchmark is not None:
                result['total_execution_loss_usd'] = str(
                    dec(result['entropy_execution_loss_usd']) + dec(result['hedge_execution_loss_usd']))
        result['measurement_quality'] = 'complete' if not missing else 'incomplete'
        return result
    c_hedge = _quote_vwap(c_quote, 'hedge', side.opposite, units)
    c = paired_basis_bps(e_px, c_hedge) if c_hedge is not None else None
    d = paired_basis_bps(e_px, h_px)
    for label, value in (('decision_quote_depth', a), ('submit_quote_depth', b),
                         ('fill_mark_hedge_depth', c)):
        if value is None:
            missing.append(label)
    mark_source = first.get('fill_mark_time_source')
    if mark_source not in {'terminal_observed_local', 'runtime_consumed_local', 'exchange_asof'}:
        missing.append('fill_mark_time_source')
    result.update({
        'a_basis_bps': str(a) if a is not None else None,
        'b_basis_bps': str(b) if b is not None else None,
        'c_basis_bps': str(c) if c is not None else None,
        'd_basis_bps': str(d),
        'fill_mark_time_source': mark_source,
    })
    if a is not None and b is not None:
        result['a_to_b_adverse_bps'] = str(side.sign * (b - a))
    if b is not None and c is not None:
        result['b_to_c_adverse_bps'] = str(side.sign * (c - b))
    if c is not None:
        result['c_to_d_adverse_bps'] = str(side.sign * (d - c))
    if q_step is not None and q_step > 0 and 'quantity_step_mismatch' not in missing:
        q = q_step * units
        e_benchmark = _quote_vwap(b_quote, 'entropy', side, units)
        if e_benchmark is not None:
            e_loss = side.sign * q * (e_px - e_benchmark)
            result['entropy_execution_loss_usd'] = str(e_loss)
        if c_hedge is not None:
            h_loss = -side.sign * q * (h_px - c_hedge)
            result['hedge_execution_loss_usd'] = str(h_loss)
        if e_benchmark is not None and c_hedge is not None:
            result['total_execution_loss_usd'] = str(e_loss + h_loss)
    result['measurement_quality'] = 'complete' if not missing else 'incomplete'
    return result


def canonical_attempts(rows: Iterable[Mapping[str, Any]],
                       live_steps: Iterable[Mapping[str, Any]] = (),
                       orders: Iterable[Mapping[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Join safety facts without treating an analytics quantity as a terminal receipt."""
    rows = list(rows)
    result = {row['step_key']: dict(row) for row in rows if row.get('step_key')}
    anonymous = [dict(row) for row in rows if not row.get('step_key')]
    for step in live_steps:
        row = result.setdefault(step['step_key'], {'analytics_missing': True})
        row.update({key: value for key, value in dict(step).items()
                    if key in {'step_key', 'order_ref', 'cycle_id', 'venue', 'side',
                               'requested_units', 'execution_group_id', 'parent_step_key',
                               'accounting_final', 'abort_reason', 'trigger_ts',
                               'trigger_basis_bps', 'trigger_mono_ns',
                               'trigger_clock_session_id'}})
        for suffix in ('ts', 'basis_bps', 'mono_ns', 'clock_session_id'):
            if step.get(f'trigger_{suffix}') is not None:
                row[f'group_trigger_{suffix}'] = step[f'trigger_{suffix}']
        row.setdefault('purpose', str(step['step_key']).rsplit(':', 2)[-2]
                       if ':' in str(step['step_key']) else 'unknown')
    for row in result.values():
        parent = result.get(row.get('parent_step_key'), {})
        if parent and parent.get('cycle_id') == row.get('cycle_id') \
                and parent.get('execution_group_id') == row.get('execution_group_id'):
            for suffix in ('ts', 'basis_bps', 'mono_ns', 'clock_session_id'):
                key = f'group_trigger_{suffix}'
                if row.get(key) is None and parent.get(key) is not None:
                    row[key] = parent[key]
    order_map = {row['order_ref']: dict(row) for row in orders or ()}
    linked_refs = {row.get('order_ref') for row in [*result.values(), *anonymous]}
    for ref, order in order_map.items():
        if ref not in linked_refs:
            result[f'order:{ref}'] = {
                **{key: order.get(key) for key in ('order_ref', 'cycle_id', 'venue',
                                                   'side', 'requested_units')},
                'step_key': f'order:{ref}', 'analytics_missing': True,
                'safe_step_missing': True, 'purpose': 'unknown'}
    for row in [*result.values(), *anonymous]:
        order = order_map.get(row.get('order_ref'), {})
        state = order.get('state') if orders is not None else row.get('order_state')
        units = order.get('filled_units') if order else row.get('result_units')
        row['order_state'] = state
        row['canonical_order'] = ({key: order[key] for key in (
            'order_ref', 'cycle_id', 'state', 'reduce_only', 'venue', 'side',
            'filled_units', 'fee_usd') if key in order} if order else row.get('canonical_order'))
        row['known_cumulative_units'] = units
        row['result_units'] = units
        row['order_outcome'] = ('terminal_positive' if units is not None and int(units) > 0
                                else 'terminal_zero' if units == 0 else 'UNKNOWN') \
            if state == 'TERMINAL' else ('not_submitted' if state == 'PREPARED' else 'UNKNOWN')
        row['accounting_status'] = ('final' if row.get('accounting_final') else 'pending')
        for key in ('had_unknown', 'first_unknown_ts', 'terminal_at', 'terminal_recorded_ts', 'submitted_ts',
                    'terminal_received_ts', 'terminal_received_mono_ns',
                    'terminal_received_clock_session_id', 'terminal_source', 'terminal_status'):
            row[key] = order.get(key, row.get(key))
        if state == 'UNKNOWN':
            row['had_unknown'] = True
        row['terminal_at'] = (row.get('terminal_received_ts') if row.get('terminal_received_ts')
                              is not None else row.get('terminal_at') or row.get('terminal_recorded_ts'))
        row['terminal_at_source'] = ('canonical_terminal_received' if row.get('terminal_received_ts')
                                     is not None else 'canonical_recorded_wall'
                                     if row.get('terminal_recorded_ts') is not None else None)
        row['submitted'] = row.get('submitted_ts') is not None
        if row.get('terminal_source'):
            row['confirmation_source'] = row['terminal_source']
        if row['order_outcome'] not in {'terminal_positive', 'terminal_zero'}:
            for key in ('terminal_observed_ts', 'terminal_observed_mono_ns',
                        'terminal_received_ts', 'terminal_received_mono_ns'):
                row[key] = None
    return [*result.values(), *anonymous]


def summarize_attempts(rows: Iterable[Mapping[str, Any]],
                       live_cycles: Iterable[Mapping[str, Any]] = (),
                       cycle_economics: Iterable[Mapping[str, Any]] = (),
                       live_steps: Iterable[Mapping[str, Any]] = (),
                       orders: Iterable[Mapping[str, Any]] | None = None,
                       *, window_start_ts: float | None = None,
                       window_end_ts: float | None = None) -> dict[str, Any]:
    rows = [dict(row) for row in rows]
    journal_steps = [dict(row) for row in live_steps]
    telemetry_step_keys = {row.get('step_key') for row in rows if not row.get('analytics_missing')}
    rows = canonical_attempts(rows, journal_steps, orders)
    excluded_missing_submit_time = 0
    if window_start_ts is not None or window_end_ts is not None:
        excluded_missing_submit_time = sum(row.get('submitted_ts') is None for row in rows)
        rows = [row for row in rows if row.get('submitted_ts') is not None
                and (window_start_ts is None or row['submitted_ts'] >= window_start_ts)
                and (window_end_ts is None or row['submitted_ts'] < window_end_ts)]
        for row in rows:
            if row['order_outcome'].startswith('terminal') and (row.get('terminal_at') is None
                    or (window_end_ts is not None and row['terminal_at'] >= window_end_ts)):
                row['order_outcome'] = 'UNKNOWN'
                row['censored_at_window_end'] = True
                row['known_cumulative_units_at_window_end'] = None
                row['terminal_observed_ts'] = row['terminal_observed_mono_ns'] = None
                row['terminal_received_ts'] = row['terminal_received_mono_ns'] = None
    directions = {row['cycle_id']: row.get('direction') for row in live_cycles
                  if row.get('cycle_id')}
    economics = {row['cycle_id']: row for row in cycle_economics
                 if row.get('cycle_id')}
    groups: dict[str, dict[str, Any]] = {}
    rows_by_diagnostic_group: dict[str, list[Mapping[str, Any]]] = {}
    diagnostic_key_by_step: dict[str, str] = {}
    by_group: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        phase = _phase(row)
        key = ':'.join(str(x) for x in (
            directions.get(row.get('cycle_id')) or 'unknown', row.get('venue') or 'unknown',
            phase, row.get('side') or 'unknown', row.get('attempt')
            if row.get('attempt') is not None else 'unknown',
            row.get('decision_units') or row.get('requested_units') or 'unknown',
            row.get('confirmation_source') or 'unknown',
            row.get('execution_path') or 'unknown'))
        group = groups.setdefault(key, {'attempts': 0, 'positive_fill': 0,
                                        'zero_fill': 0, 'unknown': 0,
                                        'filled_units': 0, 'requested_units': 0,
                                        'retry_attempts': 0, 'fill_rate': None,
                                        'paired_execution_loss_attributed_usd': '0',
                                        'execution_loss_coverage_count': 0,
                                        'dimensions': {
                                            'direction': directions.get(row.get('cycle_id')) or 'unknown',
                                            'venue': row.get('venue') or 'unknown',
                                            'phase': phase,
                                            'side': row.get('side') or 'unknown',
                                            'attempt': row.get('attempt'),
                                            'decision_units': row.get('decision_units')
                                            or row.get('requested_units'),
                                            'confirmation_source': row.get('confirmation_source')
                                            or 'unknown',
                                            'execution_path': row.get('execution_path') or 'unknown',
                                        }})
        rows_by_diagnostic_group.setdefault(key, []).append(row)
        if row.get('step_key'):
            diagnostic_key_by_step[str(row['step_key'])] = key
        group['attempts'] += 1
        group['requested_units'] += int(row.get('requested_units') or 0)
        if int(row.get('attempt') or 0) > 0:
            group['retry_attempts'] += 1
        if row['order_outcome'] not in {'terminal_zero', 'terminal_positive'}:
            group['unknown'] += 1
        elif int(row['result_units']) > 0:
            group['positive_fill'] += 1
            group['filled_units'] += int(row['result_units'])
        else:
            group['zero_fill'] += 1
        group_id = row.get('execution_group_id')
        if group_id:
            by_group.setdefault((str(row.get('cycle_id')), str(group_id)), []).append(row)
    for group in groups.values():
        terminal = group['positive_fill'] + group['zero_fill']
        group['fill_rate'] = str(Decimal(group['positive_fill']) / terminal) if terminal else None

    paired = [_execution_group(group_rows) for group_rows in by_group.values()]
    paired.sort(key=lambda row: (str(row['cycle_id']), str(row['execution_group_id'])))
    for pair in paired:
        loss = pair['total_execution_loss_usd']
        first_step_key = next(iter(pair['first_step_keys']), None)
        key = diagnostic_key_by_step.get(first_step_key)
        if loss is not None and key is not None:
            group = groups[key]
            group['paired_execution_loss_attributed_usd'] = str(
                dec(group['paired_execution_loss_attributed_usd']) + dec(loss))
            group['execution_loss_coverage_count'] += 1
    for key, group_rows in rows_by_diagnostic_group.items():
        groups[key]['submit_to_terminal_latency_ms'] = _interval(
            group_rows, 'latency', (
                _step_pair(row, 'submit_start_ts', 'terminal_observed_ts')
                for row in group_rows))[1]
    timing_rows = [row for row in rows if row['submitted']]
    latency: dict[str, Any] = {}
    latency.update([_interval(rows, 'market_received_to_decision_ms', (
        (_market_received(_json_object(row.get('decision_quote_json')), 'wall_utc'),
              row.get('attempt_decision_ts'), 'wall_utc') for row in rows))])
    latency.update([_interval(rows, 'original_signal_to_attempt_decision_ms', (
        _signal_pair(row, economics.get(row.get('cycle_id'), {})) for row in rows))])
    for label, start, end in (
            ('attempt_decision_to_submit_start_ms', 'attempt_decision_ts', 'submit_start_ts'),
            ('submit_start_to_http_post_start_ms', 'submit_start_ts', 'http_post_start'),
            ('http_post_start_to_terminal_observed_ms', 'http_post_start', 'terminal_observed_ts'),
            ('submit_to_terminal_received_ms', 'submit_start_ts', 'terminal_received_ts'),
            ('terminal_received_to_runtime_consumed_ms', 'terminal_received_ts', 'runtime_consumed_ts'),
            ('hedge_submit_to_terminal_observed_ms', 'submit_start_ts', 'terminal_observed_ts')):
        eligible = ([row for row in rows if row.get('venue') == 'hedge']
                    if label.startswith('hedge_') else rows)
        latency.update([_interval(eligible, label, (
            _step_pair(row, start, end) for row in eligible))])
    first_hedge_pairs = []
    exit_terminal_pairs = []
    reconcile_pairs = []
    for group_rows in by_group.values():
        first = next((row for row in group_rows if row.get('venue') == 'entropy'
                      and (row.get('result_units') or 0) > 0), None)
        hedge = next((row for row in group_rows if row.get('venue') == 'hedge'
                      and row.get('submit_start_ts') is not None), None)
        parallel = first is not None and first.get('execution_path') == 'parallel'
        terminals = [row for row in group_rows if row.get('terminal_observed_ts') is not None
                     and (parallel or row.get('venue') == 'hedge')]
        sessions = {_event_session(row, 'terminal_observed_ts') for row in terminals}
        mono = (parallel and len(sessions) == 1 and None not in sessions
                and all(row.get('terminal_observed_mono_ns') is not None for row in terminals))
        last_terminal = max(terminals, key=lambda row: (
            int(row['terminal_observed_mono_ns']) if mono else float(row['terminal_observed_ts'])),
            default=None)
        if first is not None and first.get('execution_path') != 'parallel':
            first_hedge_pairs.append(
                _cross_pair(first, hedge, 'terminal_observed_ts', 'submit_start_ts')
                if hedge is not None else
                (first.get('terminal_observed_ts'), None, 'wall_utc'))
        if first is not None and _phase(first) == 'close':
            econ = economics.get(first.get('cycle_id'), {})
            exit_terminal_pairs.append(_cross_pair(
                first, last_terminal or {}, 'group_trigger_ts', 'terminal_observed_ts'))
            if last_terminal is None:
                reconcile_pairs.append((None, None, 'wall_utc'))
                continue
            reconciled_mono = (last_terminal.get('authoritative_reconcile_complete_mono_ns')
                               or econ.get('authoritative_reconcile_complete_mono_ns'))
            reconciled_wall = (last_terminal.get('authoritative_reconcile_complete_ts')
                               or econ.get('authoritative_reconcile_complete_ts'))
            reconciled_session = (_event_session(last_terminal, 'authoritative_reconcile_complete_ts')
                                  if last_terminal.get('authoritative_reconcile_complete_mono_ns')
                                  is not None else econ.get('authoritative_reconcile_clock_session_id'))
            if reconciled_session is not None \
                    and reconciled_session == _event_session(last_terminal, 'terminal_observed_ts') \
                    and reconciled_mono is not None \
                    and last_terminal.get('terminal_observed_mono_ns') is not None:
                reconcile_pairs.append((last_terminal['terminal_observed_mono_ns'],
                                        reconciled_mono,
                                        'monotonic_ns'))
            else:
                reconcile_pairs.append((last_terminal.get('terminal_observed_ts'),
                                        reconciled_wall,
                                        'wall_utc'))
    latency.update([_interval([{}] * len(first_hedge_pairs),
                              'entropy_terminal_to_hedge_submit_ms', first_hedge_pairs)])
    latency.update([_interval([{}] * len(exit_terminal_pairs),
                              'exit_trigger_to_paired_terminal_ms', exit_terminal_pairs)])
    latency.update([_interval([{}] * len(reconcile_pairs),
                              'paired_terminal_to_authoritative_reconcile_ms', reconcile_pairs)])
    complete = [row for row in paired if row['measurement_quality'] == 'complete']
    total_losses = [dec(row['total_execution_loss_usd']) for row in paired
                    if row['total_execution_loss_usd'] is not None]
    paired_by_path: dict[str, dict[str, int]] = {}
    for pair in paired:
        path = pair['execution_path']
        bucket = paired_by_path.setdefault(path, {'groups': 0, 'complete': 0})
        bucket['groups'] += 1
        bucket['complete'] += int(pair['measurement_quality'] == 'complete')
    return {
        'attempt_count': len(rows), 'submitted_attempt_count': len(timing_rows),
        'submitted_attempt_count_basis': 'canonical adapter-call attempt timestamp; not proof of exchange receipt; missing evidence is not zero submissions',
        'attempts': rows,
        'terminal_zero_attempts': sum(row['order_outcome'] == 'terminal_zero' for row in rows),
        'unknown_attempts': sum(row['order_outcome'] == 'UNKNOWN' for row in rows),
        'measurement_quality': {
            'unknown_history_missing_count': sum(row.get('had_unknown') is None for row in rows),
            'submission_time_missing_count': sum(row.get('submitted_ts') is None for row in rows),
            'safe_step_missing_count': sum(bool(row.get('safe_step_missing')) for row in rows),
            'window_excluded_missing_submission_time': excluded_missing_submit_time,
            'terminal_received_missing_count': sum(row.get('terminal_received_ts') is None for row in rows),
            'event_clock_provenance_missing_count': sum(
                not _event_session(row, 'terminal_observed_ts') for row in rows),
        },
        'journal_step_count': len(journal_steps),
        'missing_economics_step_keys': sorted(
            row['step_key'] for row in journal_steps
            if row.get('step_key') and row['step_key'] not in telemetry_step_keys),
        'groups': groups, 'paired_groups': paired,
        'paired_group_count': len(paired), 'complete_paired_group_count': len(complete),
        'paired_groups_by_first_leg_path': paired_by_path,
        'known_execution_loss_usd': str(sum(total_losses, Decimal('0'))),
        'execution_loss_coverage_count': len(total_losses),
        'latency': latency,
        'exchange_clock': {
            'first_fill_timestamp_count': sum(row.get('exchange_first_fill_ts')
                                              is not None for row in rows),
            'last_fill_timestamp_count': sum(row.get('exchange_last_fill_ts')
                                             is not None for row in rows),
            'local_to_exchange_latency_ms': None,
            'reason': 'clock alignment evidence unavailable',
        },
        'interpretation': ('A→B, B→C, C→D are directional adverse basis changes; '
                           'execution losses are diagnostic and are not subtracted again '
                           'from cash-flow PnL.'),
    }


def summarize(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    all_rows = [dict(row) for row in rows]
    clean = [row for row in all_rows if row.get('result_kind') == 'clean'
             and bool(row.get('accounting_final'))
             and bool(row.get('clean_summary_eligible'))]
    count = len(clean)
    status = ('EMPIRICAL_READY' if count >= 50 else
              'EARLY_EMPIRICAL' if count >= 20 else 'INSUFFICIENT')
    result: dict[str, Any] = {
        'sample_status': status,
        'clean_count': count,
        'recovery_count': sum(row.get('result_kind') in {'repair', 'unwound'}
                              for row in all_rows),
    }
    timing_specs = {
        'decision_to_persist_ms': ('decision_ts', 'cycle_persist_start_ts'),
        'decision_to_submit_ms': ('decision_ts', 'submit_start_ts'),
        'signal_to_position_check_start_ms': ('signal_ts', 'position_check_start_ts'),
        'position_check_ms': ('position_check_start_ts', 'position_check_end_ts'),
        'position_check_to_persist_ms': ('position_check_end_ts', 'cycle_persist_start_ts'),
        'persist_ms': ('cycle_persist_start_ts', 'cycle_persist_end_ts'),
        'intent_prepare_ms': ('intent_prepare_start_ts', 'intent_prepare_end_ts'),
        'signal_to_submit_ms': ('signal_ts', 'submit_start_ts'),
        'signal_to_first_fill_observed_ms': ('signal_ts', 'first_fill_observed_ts'),
    }
    timing_rows = [row for row in all_rows
                   if row.get('submit_start_ts') not in (None, '')]
    latency_distributions = {}
    latency_medians: dict[str, Decimal] = {}
    for name, (start, end) in timing_specs.items():
        values = []
        missing = negative = 0
        for row in timing_rows:
            begin, finish = _number(row.get(start)), _number(row.get(end))
            if begin is None or finish is None:
                missing += 1
            elif finish < begin:
                negative += 1
            else:
                values.append((finish - begin) * Decimal('1000'))
        latency_distributions[name] = {
            'count': len(values), 'missing_count': missing,
            'negative_count': negative,
            'missing_rate': (str(Decimal(missing) / len(timing_rows))
                             if timing_rows else None),
        }
        latency_distributions[name].update({
            f'p{p}': (str(value) if (value := _percentile(
                values, Decimal(p) / 100)) is not None else None)
            for p in (50, 90, 95)
        })
        median_value = _percentile(values, Decimal('.5'))
        if median_value is not None:
            latency_medians[name] = median_value
    segment_medians = {name: value for name, value in latency_medians.items()
                       if name not in {'signal_to_submit_ms',
                                       'signal_to_first_fill_observed_ms'}}
    result['latency'] = {
        'open_attempt_count': len(timing_rows),
        'distributions_ms': latency_distributions,
        'dominant_segment': (max(segment_medians, key=segment_medians.get)
                             if len(timing_rows) >= 10 and segment_medians else None),
        'exchange_clock': {
            'first_fill_timestamp_count': sum(row.get('first_fill_exchange_ts')
                                              not in (None, '') for row in timing_rows),
            'local_to_exchange_latency_ms': None,
            'reason': 'exchange clock offset and alignment quality unavailable',
        },
    }
    if not clean:
        return result
    result['distributions'] = {
        'entry_dislocation_bps': _distribution(clean, 'entry_dislocation_bps', (25, 50, 75)),
        'first_leg_cost_bps': _distribution(clean, 'first_leg_cost_bps', (50, 75, 90)),
        'signal_to_first_fill_decay_bps': _distribution(
            clean, 'signal_to_first_fill_decay_bps', (50, 75, 90)),
        'hedge_cost_bps': _distribution(clean, 'hedge_cost_bps', (50, 75, 90)),
        'first_fill_to_hedged_decay_bps': _distribution(
            clean, 'first_fill_to_hedged_decay_bps', (50, 75, 90)),
        'actual_fees_bps': _distribution(clean, 'actual_fees_bps', (50, 75)),
        'actual_funding_bps': _distribution(clean, 'actual_funding_bps', (50, 75)),
        'actual_reversion_bps': _distribution(clean, 'actual_reversion_bps', (25, 50, 75)),
        'break_even_reversion_bps': _distribution(
            clean, 'break_even_reversion_bps', (50, 75, 90)),
    }
    pnl = [dec(row['finalized_pnl_usd']) for row in clean]
    result['pnl'] = {
        'mean_usd': str(mean(pnl)), 'median_usd': str(median(pnl)),
        'total_usd': str(sum(pnl, Decimal('0'))),
        'win_rate': str(Decimal(sum(value > 0 for value in pnl)) / count),
    }
    if count >= 50:
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for row in clean:
            grouped.setdefault(_bucket_name(dec(row['entry_dislocation_bps'])), []).append(row)
        buckets = {}
        for name, group in grouped.items():
            if len(group) < 5:
                continue
            group_pnl = [dec(row['finalized_pnl_usd']) for row in group]
            total_decay = [dec(row['total_entry_decay_bps']) for row in group
                           if row.get('total_entry_decay_bps') not in (None, '')]
            break_even = [dec(row['break_even_reversion_bps']) for row in group]
            buckets[name] = {
                'count': len(group), 'mean_pnl_usd': str(mean(group_pnl)),
                'median_pnl_usd': str(median(group_pnl)),
                'win_rate': str(Decimal(sum(value > 0 for value in group_pnl)) / len(group)),
                'median_break_even_reversion_bps': str(median(break_even)),
                'median_total_entry_decay_bps': (
                    str(median(total_decay)) if total_decay else None),
            }
        result['entry_dislocation_buckets'] = buckets
    return result
