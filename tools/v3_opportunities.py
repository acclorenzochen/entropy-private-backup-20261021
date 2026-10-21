#!/usr/bin/env python3
"""Read-only V3 opportunity paths and calibrated, uncertain shadow cycle replay."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from dataclasses import fields, replace
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from statistics import median
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from entropy_arb.v3.core import (Book, Direction, Instrument, Market, Settings,
                                 Side, dec, exit_decision, plan_open, price_limit)
from entropy_arb.v3.reference import FastReference
from entropy_arb.v3.market_observer import quote_snapshot
from entropy_arb.v3.economics import _quote_vwap


HORIZONS = ((.1, '100ms'), (.25, '250ms'), (.5, '500ms'), (1., '1s'))
WINDOWS = ((5., '5s'), (30., '30s'), (120., '120s'), (600., '600s'))
DIRECTIONS = (('buy_entropy', 'buy_entropy_basis_bps', -1),
              ('sell_entropy', 'sell_entropy_basis_bps', 1))


def load_snapshots(paths: list[Path]) -> list[dict]:
    rows = []
    for path in paths:
        with path.open(encoding='utf-8') as handle:
            for line in handle:
                row = json.loads(line)
                if row.get('kind') in {'v3_market_snapshot', 'v3_observer_gap'}:
                    rows.append(row)
    return rows


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _decimal(value: object) -> Decimal | None:
    try:
        result = dec(value)
    except (TypeError, ValueError, ArithmeticError):
        return None
    return result if result.is_finite() else None


def _quote(row: dict, units: int, field: str) -> Decimal | None:
    if row.get('valid', True) is not True:
        return None
    if (row.get('entropy') is not None and row.get('lighter') is not None
            and (row.get('instrument') or {}).get('quantity_step') is not None):
        direction = (Direction.LONG_ENTROPY if field == 'buy_entropy_basis_bps'
                     else Direction.SHORT_ENTROPY)
        quoted = quote_snapshot({**row, 'valid': row.get('valid', True)}, units, direction)
        return _decimal(quoted.get('basis_bps'))
    for item in row.get('quotes') or []:
        if item.get('units') == units:
            return _decimal(item.get(field))
    return None


def _clocked_sessions(rows: list[dict], max_gap: float):
    """Keep each capture stream in sequence order and split missing/invalid time."""
    groups = defaultdict(list)
    for row in rows:
        if row.get('session_id'):
            groups[(row.get('pair_id'), row['session_id'])].append(row)
    for (pair_id, session_id), group in groups.items():
        use_mono = all(_number(row.get('capture_mono_ns')) is not None
                       for row in group)
        clock = 'capture_mono_ns' if use_mono else 'capture_wall_ts'
        scale = 1e9 if use_mono else 1
        # Input order is the capture order, including old invalid records with
        # no sequence/clock. Sorting those records to the end fabricates coverage.
        ordered = group
        segment: list[tuple[float, dict]] = []
        previous_t: float | None = None
        previous_seq: float | None = None
        previous_row = None
        for row in ordered:
            raw_t = _number(row.get(clock))
            sequence = _number(row.get('sample_seq'))
            t = raw_t / scale if raw_t is not None else None
            if (sequence is not None and previous_seq is not None
                    and sequence <= previous_seq):
                if row != previous_row and segment:
                    yield pair_id, session_id, clock, segment, 'out_of_order_sequence'
                    segment = []
                # An exact replay cannot create another opportunity. Conflicting
                # or out-of-order evidence ends coverage instead of being hidden.
                continue
            reason = ('missing_clock' if t is None else
                      'observer_gap' if row.get('kind') == 'v3_observer_gap' else
                      'continuity_break' if row.get('continuity_break') else
                      'invalid_snapshot' if row.get('valid', True) is not True else
                      'nonmonotonic_clock' if previous_t is not None and t <= previous_t else
                      'data_gap' if previous_t is not None and t - previous_t > max_gap else
                      'missing_sequence' if sequence is not None and previous_seq is not None
                      and sequence > previous_seq + 1 else None)
            if reason:
                if segment:
                    yield pair_id, session_id, clock, segment, reason
                segment = []
            if (t is not None and row.get('valid', True) is True
                    and row.get('kind') != 'v3_observer_gap'):
                segment.append((t, row))
                previous_t, previous_seq = t, sequence
                previous_row = row
            else:
                previous_t, previous_seq = None, None
        if segment:
            yield pair_id, session_id, clock, segment, 'session_end'


def _vwap(row: dict, venue: str, side: str, units: int) -> Decimal | None:
    book = row.get(venue) or {}
    remaining, cash = units, Decimal(0)
    for level in book.get(side) or []:
        if len(level) != 2:
            return None
        price, available = _decimal(level[0]), _number(level[1])
        if price is None or price <= 0 or available is None or available <= 0:
            return None
        taken = min(remaining, int(available))
        cash += price * taken
        remaining -= taken
        if not remaining:
            return cash / units
    return None


def _movement(previous: dict | None, current: dict, units: int,
              direction: str) -> dict:
    unknown = {'movement_source': 'unknown', 'entropy_move_bps': None,
               'lighter_move_bps': None}
    if previous is None:
        return unknown
    entropy_side, lighter_side = (('asks', 'bids') if direction == 'buy_entropy'
                                  else ('bids', 'asks'))
    before_e = _vwap(previous, 'entropy', entropy_side, units)
    after_e = _vwap(current, 'entropy', entropy_side, units)
    before_l = _vwap(previous, 'lighter', lighter_side, units)
    after_l = _vwap(current, 'lighter', lighter_side, units)
    if None in (before_e, after_e, before_l, after_l):
        return unknown
    sign = -1 if direction == 'buy_entropy' else 1
    e = sign * math.log(float(after_e / before_e)) * 10000
    l = -sign * math.log(float(after_l / before_l)) * 10000
    source = ('both' if e > 0 and l > 0 else
              'entropy' if e > 0 else 'lighter' if l > 0 else 'unknown')
    return {'movement_source': source, 'entropy_move_bps': e,
            'lighter_move_bps': l}


def _path(segment: list[tuple[float, dict]], start: int, units: int,
          field: str, segment_end_reason: str) -> tuple[list[tuple[float, Decimal]], str]:
    origin = segment[start][0]
    result = []
    for t, row in segment[start:]:
        value = _quote(row, units, field)
        if value is None:
            return result, 'quantity_unavailable'
        result.append((t - origin, value))
        if t - origin > WINDOWS[-1][0]:
            return result, 'max_window_reached'
    return result, segment_end_reason


def _horizon(path: list[tuple[float, Decimal]], target: float,
             end_reason: str) -> dict:
    before = None
    for elapsed, basis in path:
        if abs(elapsed - target) <= 1e-6:
            return {'status': 'observed', 'basis_bps': str(basis),
                    'observed_seconds': elapsed}
        if elapsed > target:
            return {'status': 'interval_censored', 'basis_bps': None,
                    'interval_seconds': [before[0], elapsed],
                    'bracket_basis_bps': [str(before[1]), str(basis)]}
        before = (elapsed, basis)
    return {'status': 'right_censored', 'basis_bps': None,
            'observed_through_seconds': before[0] if before else 0.,
            'censor_reason': end_reason}


def _window(path: list[tuple[float, Decimal]], target: float,
            reference: Decimal, trigger_edge: Decimal, sign: int,
            band: Decimal, end_reason: str) -> dict:
    observed = [(elapsed, sign * (basis - reference)) for elapsed, basis in path
                if elapsed <= target + 1e-6]
    exact_end = any(abs(elapsed - target) <= 1e-6 for elapsed, _ in path)
    bracketed_end = any(elapsed > target for elapsed, _ in path)
    first_reversion = None
    for index, (elapsed, edge) in enumerate(observed[1:], 1):
        if edge < band:
            first_reversion = [observed[index - 1][0], elapsed]
            break
    min_edge = min(edge for _, edge in observed)
    max_edge = max(edge for _, edge in observed)
    return {
        'status': ('covered' if exact_end else 'interval_censored'
                   if bracketed_end else 'right_censored'),
        'censor_reason': end_reason if not exact_end and not bracketed_end else None,
        'observed_through_seconds': min(target, observed[-1][0]),
        'observed_samples': len(observed),
        'max_reversion_bps': str(max(Decimal(0), trigger_edge - min_edge)),
        'max_adverse_move_bps': str(max(Decimal(0), max_edge - trigger_edge)),
        'reversion_wait_interval_seconds': first_reversion,
        'not_reverted_at_end': (False if first_reversion is not None else
                                True if exact_end else None),
    }


def episodes(rows: list[dict], settings: Settings) -> list[dict]:
    result = []
    # The recorder has a 1s heartbeat. Longer gaps cannot support path claims.
    max_gap = min(settings.reset_gap_seconds, 2.0)
    for pair_id, session_id, clock, segment, segment_end_reason in _clocked_sessions(
            rows, max_gap):
        active: dict[tuple[int, str], bool] = {}
        for index, (t, row) in enumerate(segment):
            reference = _decimal(row.get('reference_bps'))
            present = set()
            if reference is None or row.get('reference_usable') is False:
                active.clear()
                continue
            quotes = list(row.get('quotes') or [])
            candidate_units = {q.get('units') for q in quotes}
            for units in {key[0] for key in active} - candidate_units:
                quotes.append({'units': units})
            for quote in quotes:
                units = quote.get('units')
                if not isinstance(units, int) or isinstance(units, bool) or units <= 0:
                    continue
                for direction, field, sign in DIRECTIONS:
                    basis = _quote(row, units, field)
                    if basis is None:
                        continue
                    key = (units, direction)
                    present.add(key)
                    edge = sign * (basis - reference)
                    triggered = edge >= settings.entry_band_bps
                    if triggered and not active.get(key, False):
                        onset_status = ('observed_crossing' if key in active
                                        else 'left_censored')
                        path, end_reason = _path(
                            segment, index, units, field, segment_end_reason)
                        previous = segment[index - 1][1] if index else None
                        opposite = ('sell_entropy_basis_bps' if direction == 'buy_entropy'
                                    else 'buy_entropy_basis_bps')
                        entry = {
                            'pair_id': pair_id, 'session_id': session_id,
                            'sample_seq': row.get('sample_seq'),
                            'clock_source': clock,
                            'trigger_ts': (row.get('capture_wall_ts')
                                           if onset_status == 'observed_crossing' else None),
                            'first_observed_ts': row.get('capture_wall_ts'),
                            'onset_status': onset_status,
                            'direction': direction, 'units': units,
                            'reference_bps': str(reference),
                            'reference_source': row.get('reference_source'),
                            'executable_basis_bps': str(basis),
                            'opposite_executable_basis_bps': quote.get(opposite),
                            'mid_basis_bps': row.get('mid_basis_bps'),
                            'depth_coverage_units': {
                                venue: (row.get(venue) or {}).get('depth_coverage_units')
                                for venue in ('entropy', 'lighter')},
                        }
                        entry.update(_movement(previous, row, units, direction))
                        entry['horizons'] = {
                            label: _horizon(path, seconds, end_reason)
                            for seconds, label in HORIZONS}
                        entry['windows'] = {
                            label: _window(path, seconds, reference, edge, sign,
                                           settings.entry_band_bps, end_reason)
                            for seconds, label in WINDOWS}
                        for _, label in HORIZONS:
                            entry[f'future_{label}_bps'] = entry['horizons'][label]['basis_bps']
                        entry['censored'] = entry['windows']['600s']['status'] != 'covered'
                        result.append(entry)
                    active[key] = triggered
            for key in set(active) - present:
                active.pop(key)
    return result


def _coverage(found: list[dict]) -> dict:
    windows = {}
    for _, label in WINDOWS:
        counts = Counter(row['windows'][label]['status'] for row in found)
        not_reverted = sum(row['windows'][label]['not_reverted_at_end'] is True
                           for row in found)
        windows[label] = {
            'coverage': dict(counts),
            'not_reverted_at_end': not_reverted,
            'not_reverted_rate_among_covered': (
                not_reverted / counts['covered'] if counts['covered'] else None),
        }
    return {
        'horizons': {label: dict(Counter(
            row['horizons'][label]['status'] for row in found))
                     for _, label in HORIZONS},
        'windows': windows,
    }


def read_calibration(path: Path) -> tuple[list[dict], list[dict]]:
    """Read a copied journal without opening it for writes or migrating it."""
    db = sqlite3.connect(f'file:{path.resolve().as_posix()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        attempts = [dict(row) for row in db.execute(
            '''SELECT e.*, o.state AS order_state,
                      o.filled_units AS canonical_filled_units
               FROM execution_economics_steps e LEFT JOIN orders o
               ON o.order_ref=e.order_ref ORDER BY e.submit_ts''')]
        for row in attempts:
            # Analytics may still hold the first timeout's zero after resolve.
            # Canonical cumulative evidence remains authoritative.
            if row['canonical_filled_units'] is not None:
                row['result_units'] = row['canonical_filled_units']
        return attempts, [dict(row) for row in db.execute(
            'SELECT * FROM cycle_economics ORDER BY signal_ts')]
    finally:
        db.close()


def _instrument(raw: dict | None, pair_id: str) -> tuple[Instrument, int] | None:
    """Use explicit exchange metadata; never infer minimum size or fees."""
    if not raw:
        return None
    names = ('quantity_step', 'entropy_tick', 'hedge_tick',
             'entropy_fee_bps', 'hedge_fee_bps', 'entropy_min_base',
             'entropy_min_quote', 'lighter_min_base', 'lighter_min_quote')
    if any(raw.get(name) is None for name in names):
        return None
    try:
        values = {name: dec(raw[name]) for name in names}
        instrument = Instrument(pair_id or 'unknown_pair',
                                values['quantity_step'], values['entropy_tick'],
                                values['hedge_tick'], values['entropy_fee_bps'],
                                values['hedge_fee_bps'])
        if min(values['entropy_min_base'], values['lighter_min_base'],
               values['entropy_min_quote'], values['lighter_min_quote']) < 0:
            return None
        # Quote minima vary with price; the fixed base minimum is only a floor.
        base = max(values['entropy_min_base'], values['lighter_min_base'])
        minimum = max(1, math.ceil(base / instrument.quantity_step))
        return instrument, minimum
    except (ValueError, TypeError, ArithmeticError):
        return None


def _book(raw: dict, ts: float) -> Book | None:
    try:
        def levels(side: str) -> tuple[tuple[Decimal, int], ...]:
            result = []
            for px, qty in raw[side]:
                if type(qty) is not int:
                    return ()
                result.append((dec(px), qty))
            return tuple(result)
        return Book(levels('bids'), levels('asks'), ts,
                    str(raw.get('book_revision') or 'recorded'))
    except (KeyError, ValueError, TypeError, ArithmeticError):
        return None


def _market(row: dict, ts: float) -> Market | None:
    e, h = _book(row.get('entropy') or {}, ts), _book(row.get('lighter') or {}, ts)
    return Market(e, h) if e is not None and h is not None else None


def _minimum_units(raw: dict, instrument: Instrument, floor: int,
                   market: Market) -> int:
    try:
        entropy_quote = dec(raw['entropy_min_quote'])
        lighter_quote = dec(raw['lighter_min_quote'])
        return max(floor, math.ceil(entropy_quote / market.entropy.mid /
                                    instrument.quantity_step),
                   math.ceil(lighter_quote / market.hedge.mid /
                             instrument.quantity_step))
    except (KeyError, ValueError, ArithmeticError, TypeError):
        return 10**18  # Missing exchange minimum is not permission to trade.


def _attempt_phase(row: dict) -> str | None:
    purpose = str(row.get('purpose') or '')
    if purpose.startswith('open_'):
        return 'open'
    if purpose.startswith('close_'):
        return 'close'
    if purpose.startswith(('repair', 'recovery', 'unknown_')):
        return 'repair'
    return None


def _terminal(row: dict) -> bool:
    # A quantity (including zero) or a resolve-start timestamp is not proof.
    return row.get('order_state') == 'TERMINAL' or row.get('terminal') is True


def calibrate(attempts: list[dict], cycles: list[dict], cutoff_ts: float,
              quantity_step: Decimal | None = None,
              require_new_execution: bool = False) -> dict:
    """Calibrate only from attempts finalized before the replay interval."""
    groups: dict[str, list[dict]] = defaultdict(list)
    missing = Counter()
    fee_rates: dict[str, list[float]] = defaultdict(list)
    censored = []
    handoffs = {}
    parents = {row.get('step_key'): row for row in attempts if row.get('step_key')}
    for child in attempts:
        parent = parents.get(child.get('parent_step_key'))
        if parent is None or not _terminal(parent) or child.get('venue') != 'hedge':
            continue
        received = _number(parent.get('terminal_received_ts') or parent.get('terminal_result_ts'))
        sent = _number(child.get('submit_start_ts') or child.get('submit_ts'))
        if received is not None and sent is not None and received <= sent < cutoff_ts:
            key = parent['step_key']
            handoffs[key] = min(handoffs.get(key, math.inf), sent - received)
    for row in attempts:
        phase = _attempt_phase(row)
        venue = row.get('venue')
        if require_new_execution and (_number(row.get('telemetry_version')) or 0) < 2:
            missing['pre_new_execution_telemetry'] += 1
            continue
        if (require_new_execution and venue == 'entropy'
                and phase in {'open', 'close'}
                and _decimal(row.get('room_bps')) != Decimal(3)):
            missing['non_3bp_first_leg_attempt'] += 1
            continue
        start = _number(row.get('submit_start_ts') or row.get('submit_ts'))
        end = _number(row.get('terminal_received_ts') or row.get('terminal_result_ts'))
        if phase is None or venue not in {'entropy', 'hedge'} or start is None:
            continue
        if start >= cutoff_ts or (end is not None and end >= cutoff_ts):
            continue
        key = f'{phase}:{venue}'
        if not _terminal(row):
            missing[f'{key}:censored_nonterminal'] += 1
            censored.append({'order_ref': row.get('order_ref'), 'stratum': key,
                'outcome': row.get('order_state') or 'UNKNOWN',
                'known_cumulative_units': row.get('result_units'),
                'requested_units': row.get('requested_units'),
                'submit_ts': start, 'terminal_received_ts': None})
            continue
        units = row.get('requested_units')
        filled = row.get('result_units')
        if (type(units) is not int or units <= 0 or
                filled is not None and (type(filled) is not int or filled < 0 or filled > units)):
            missing[f'{key}:invalid_units'] += 1
            continue
        if end is None or end < start:
            missing[f'{key}:missing_terminal_time'] += 1
            continue
        if filled is None:
            missing[f'{key}:missing_terminal_quantity'] += 1
            continue
        decision = _number(row.get('attempt_decision_ts') or row.get('decision_ts'))
        handoff = handoffs.get(row.get('step_key'), _number(row.get('handoff_seconds')))
        benchmark = _number(row.get('b_vwap') or row.get('submit_benchmark_vwap'))
        if benchmark is None and filled and row.get('side') in {'buy', 'sell'}:
            try:
                snapshot = row.get('submit_quote_json')
                if isinstance(snapshot, str):
                    snapshot = json.loads(snapshot)
                benchmark = _number(_quote_vwap(snapshot, venue, Side(row['side']), filled))
            except (ValueError, TypeError):
                pass
        actual = _number(row.get('actual_fill_vwap'))
        cost = ((actual / benchmark - 1) * 10000 *
                (1 if row.get('side') == 'buy' else -1)
                if actual is not None and benchmark and row.get('side') in {'buy', 'sell'}
                else None)
        if decision is None:
            missing[f'{key}:decision_to_submit'] += 1
        if venue == 'entropy' and handoff is None:
            missing[f'{key}:handoff'] += 1
        if filled and cost is None:
            missing[f'{key}:execution_cost'] += 1
        groups[key].append({'fraction': filled / units,
                            'latency_seconds': end - start,
                            'decision_to_submit_seconds': (start - decision
                                if decision is not None and decision <= start else None),
                            'handoff_seconds': handoff,
                            'execution_cost_bps': cost,
                            'execution_ts': row.get('exchange_fill_ts'),
                            'terminal_time_source': ('terminal_received_ts' if
                                row.get('terminal_received_ts') is not None else 'terminal_result_ts'),
                            'side': row.get('side'), 'requested_units': units,
                            'attempt': row.get('attempt'),
                            'source': row.get('confirmation_source'),
                            'order_ref': row.get('order_ref'),
                            'resolved_after_unknown': row.get('had_unknown') is True,
                            'status': 'terminal'})
        fee = _number(row.get('actual_fee_usd'))
        price = _number(row.get('actual_fill_vwap'))
        if (filled is not None and filled > 0 and fee is not None
                and fee >= 0 and price is not None and price > 0
                and quantity_step is not None):
            fee_rates[venue].append(fee /
                (price * filled * float(quantity_step)) * 10000)
            groups[key][-1]['fee_bps'] = fee_rates[venue][-1]
    funding_rates: list[float] = []
    for row in cycles:
        start = _number(row.get('signal_ts'))
        end = _number(row.get('lighter_close_fill_ts'))
        funding = _number(row.get('actual_funding_usd'))
        notional = _number(row.get('actual_entry_notional_usd'))
        if (row.get('accounting_final') and start is not None and end is not None
                and end < cutoff_ts and end > start and funding is not None
                and notional is not None and notional > 0):
            funding_rates.append(funding / notional / ((end - start) / 3600))
    required = ('open:entropy', 'open:hedge', 'close:entropy', 'close:hedge')
    counts = {key: len(groups[key]) for key in required}
    ready = (all(value >= 30 for value in counts.values())
             and len(funding_rates) >= 20
             and all(len(fee_rates[venue]) >= 20 for venue in ('entropy', 'hedge')))
    return {'groups': dict(groups), 'censored_attempts': censored,
        'funding_per_notional_hour': None,
        'funding_model': 'unknown; mixed directions and holding periods are not a transferable rate',
        'execution_time_model': 'submit-book scenario; terminal receipt is not matching time',
        'scenario_semantics': 'neutral and conservative are sensitivity/stress scenarios, not bounds',
        'observed_fee_bps_by_venue': {venue: median(values)
                                      for venue, values in fee_rates.items()},
        'observed_fee_counts_by_venue': {venue: len(values)
                                         for venue, values in fee_rates.items()},
        'counts': counts, 'funding_cycle_count': len(funding_rates),
        'missing': dict(missing), 'ready': ready}


def _draw(calibration: dict, phase: str, venue: str, identity: str,
          scenario: str) -> dict | None:
    samples = calibration['groups'].get(f'{phase}:{venue}') or []
    if not samples:
        return None
    digest = hashlib.sha256(identity.encode()).digest()
    sample = samples[int.from_bytes(digest[:8], 'big') % len(samples)]
    latency = sample['latency_seconds']
    fraction = sample['fraction']
    if fraction is not None and scenario == 'conservative':
        fraction = max(0., fraction - .25)
    return {**sample, 'fraction': fraction, 'latency_seconds': max(.001, latency),
            'resolved_after_unknown': bool(sample.get('resolved_after_unknown'))}


class _Budget:
    def __init__(self, cfg: Settings) -> None:
        self.limits = {'entropy': cfg.entropy_orders_per_min,
                       'hedge': cfg.hedge_orders_per_min}
        self.uses: dict[str, list[float]] = {'entropy': [], 'hedge': []}
        self.total = 0

    def use(self, venue: str, ts: float, *, completion: bool = False) -> bool:
        recent = [value for value in self.uses[venue] if ts - value < 60]
        self.uses[venue] = recent
        limit = self.limits[venue]
        ceiling = limit if completion else limit - min(2, max(0, limit - 1))
        if len(recent) >= ceiling:
            return False
        stamp = ts
        while stamp in recent:
            stamp = math.nextafter(stamp, math.inf)
        recent.append(stamp)
        self.total += 1
        return True


def _quote_cash(market: Market, instrument: Instrument, venue: str,
                side: Side, units: int) -> tuple[Decimal, Decimal] | None:
    try:
        book = market.entropy if venue == 'entropy' else market.hedge
        cash, marginal = book.quote(side, units, instrument.quantity_step)
        return cash, marginal
    except ValueError:
        return None


def _mark(cycle: dict, market: Market, instrument: Instrument,
          fee_bps_by_venue: dict[str, Decimal] | None = None) -> Decimal | None:
    value = cycle['cash']
    for venue, position in (('entropy', cycle['e_units']),
                            ('hedge', cycle['h_units'])):
        if not position:
            continue
        side = Side.SELL if position > 0 else Side.BUY
        quote = _quote_cash(market, instrument, venue, side, abs(position))
        if quote is None:
            return None
        cash, _ = quote
        fee = ((fee_bps_by_venue or {}).get(venue)
               if fee_bps_by_venue is not None else None)
        if fee is None:
            fee = (instrument.entropy_fee_bps if venue == 'entropy'
                   else instrument.hedge_fee_bps)
        value += cash * (Decimal(1) if side is Side.SELL else Decimal(-1))
        value -= cash * fee / Decimal(10000)
    return value


def _choose_plan(market: Market, instrument: Instrument, cfg: Settings,
                 reference: Decimal, minimum: int,
                 unit_override: int | None,
                 direction_filter: Direction | None = None) -> tuple[Direction, int, Decimal] | None:
    depth = min(sum(q for _, q in side) for side in (
        market.entropy.bids, market.entropy.asks,
        market.hedge.bids, market.hedge.asks))
    upper = min(depth, math.floor(cfg.max_notional_usd /
                                  max(market.entropy.mid, market.hedge.mid) /
                                  instrument.quantity_step))
    if upper < minimum or unit_override is not None and not minimum <= unit_override <= upper:
        return None
    choices = []
    for direction in Direction:
        if direction_filter is not None and direction is not direction_filter:
            continue
        sizes = ([unit_override] if unit_override is not None else
                 range(upper, minimum - 1, -1))
        for units in sizes:
            try:
                plan = plan_open(market, instrument, cfg, reference,
                                 direction, units)
                if plan is None:
                    continue
                side = direction.entropy_side
                _, marginal = market.entropy.quote(side, units,
                                                   instrument.quantity_step)
                rooms, hard_cap = cfg.first_leg_execution_policy('open')
                price_limit(market.entropy.top(side), marginal, side,
                            rooms[0], hard_cap, instrument.entropy_tick)
            except ValueError:
                continue
            choices.append((direction, units, plan.distance_bps))
            break
    return max(choices, key=lambda item: (item[2], item[1])) if choices else None


def _rate_percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(percentile * (len(values) - 1)))]


def replay(rows: list[dict], cfg: Settings, calibration: dict, *,
           scenario: str, unit_override: int | None = None,
           instrument_override: dict | None = None,
           direction_filter: Direction | None = None,
           trade_start_ts: float | None = None,
           score_start_ts: float | None = None) -> dict:
    """Causal, one-cycle shadow path. Results are scenarios, never fill claims."""
    if scenario not in {'neutral', 'conservative'}:
        raise ValueError('unknown replay scenario')
    segments = sorted(_clocked_sessions(rows, min(cfg.reset_gap_seconds, 2.0)),
                      key=lambda item: item[3][0][1].get('capture_wall_ts') or 0)
    complete: list[dict] = []
    incomplete: list[dict] = []
    zero_fill_sequences = 0
    exit_reasons = Counter()
    blocked = Counter()
    matched_units = 0
    repair_loss = Decimal(0)
    total_orders = 0
    resolved_unknown_orders = 0
    terminal_asof_ages: list[float] = []
    occupied_notional_hours = Decimal(0)
    account_cash = Decimal(0)
    final_state = None
    trace: list[dict] = []
    censored_at = None
    model_censoring = None
    window_start = min((_number(row.get('capture_wall_ts')) for row in rows
                        if _number(row.get('capture_wall_ts')) is not None), default=None)
    window_end = max((_number(row.get('capture_wall_ts')) for row in rows
                      if _number(row.get('capture_wall_ts')) is not None), default=None)
    # All contiguous files in one recorder session are merged by the reader.
    # A break cannot prove what happened to an outstanding order/account. Stop
    # this account path, retaining its final state; never append a flat restart.
    for segment_index, (pair_id, session_id, _, frames, boundary) in enumerate(segments):
        if segment_index:
            break
        first_clock = frames[0][0]
        reference = FastReference(str(pair_id or 'unknown_pair'), cfg,
                                  started_at=first_clock)
        budget = _Budget(cfg)
        cycle: dict | None = None
        pending: dict | None = None
        unknown_order: dict | None = None
        previous_wall: float | None = None
        for ts, row in frames:
            mid = _decimal(row.get('mid_basis_bps'))
            if mid is None:
                blocked['missing_mid'] += 1
                continue
            view = reference.view(ts)
            # The current quote sees only reference samples from earlier rows.
            market = _market(row, ts)
            details = _instrument(instrument_override or row.get('instrument'),
                                  str(pair_id or 'unknown_pair'))
            if market is None or details is None:
                blocked['missing_book_or_instrument'] += 1
                reference.observe(ts, mid)
                previous_wall = ts
                continue
            instrument, floor = details
            effective_cap = _decimal((instrument_override or row.get('instrument') or {}).get(
                'effective_max_notional_usd'))
            effective_cfg = (replace(cfg, max_notional_usd=min(
                cfg.max_notional_usd, effective_cap))
                if effective_cap is not None and effective_cap > 0 else cfg)
            scenario_fees = {
                venue: (max((instrument.entropy_fee_bps if venue == 'entropy'
                             else instrument.hedge_fee_bps), dec(value))
                        if scenario == 'conservative' else dec(value))
                for venue, value in calibration.get(
                    'observed_fee_bps_by_venue', {}).items()}
            minimum = _minimum_units(instrument_override or row.get('instrument') or {},
                                     instrument, floor, market)
            if cycle is not None and previous_wall is not None:
                dt_hours = Decimal(str(max(0., ts - previous_wall))) / 3600
                notional = cycle['notional']
                occupied_notional_hours += notional * dt_hours
                funding_rate = calibration['funding_per_notional_hour']
                if funding_rate is not None:
                    funding = notional * dec(funding_rate) * dt_hours
                    cycle['cash'] += funding
                    account_cash += funding
                    cycle['funding'] += funding
            previous_wall = ts
            resolved_this_frame = False
            if unknown_order is not None and model_censoring is None and ts >= unknown_order['resolve_due']:
                original = unknown_order
                target = math.floor(original['units'] * original['sample']['resolved_fraction'])
                # Late matching beyond the observed cumulative fill requires a
                # fresh position/fill event. Never invent that evidence.
                if target != original['known_units']:
                    model_censoring = 'unknown_resolution_changed_without_position_event'
                    cycle['phase'] = 'UNRESOLVED'
                    original['resolve_due'] = math.inf
                else:
                    cycle['cash'] += original['deferred_cash']
                    cycle['fees'] += original['deferred_fee']
                    account_cash += original['deferred_cash']
                    trace.append({'event': 'resolve_terminal', 'order_ref': original['order_ref'],
                                  'cumulative_units': target, 'applied_position_increment': 0})
                    resolved_unknown_orders += 1
                    resolved_this_frame = True
                    unknown_order = None
                    if pending is None:
                        cycle['phase'] = (
                            'OPEN_H' if original['phase'] == 'OPEN_E'
                            and cycle['e_units'] + cycle['h_units'] else
                            'REPAIR' if cycle['e_units'] + cycle['h_units'] else
                            'OPEN' if cycle['e_units'] else 'DONE')
            if cycle is not None:
                mark = _mark(cycle, market, instrument, scenario_fees)
                if mark is not None:
                    cycle['last_mark'] = mark
                    cycle['worst_mark'] = min(cycle['worst_mark'], mark)
            if pending is not None and pending.get('submitted') is False and ts >= pending['submit_due']:
                phase = pending['phase']
                if budget.use(pending['venue'], ts, completion=phase != 'OPEN_E'):
                    pending.update(submitted=True, asof_market=market, asof_ts=ts,
                                   due=ts + pending['sample']['latency_seconds'])
                    cycle['attempts'] += 1
                    cycle['orders'] += 1
                    total_orders += 1
                    trace.append({'event': 'submit', 'order_ref': pending['order_ref'],
                        'phase': phase, 'venue': pending['venue'], 'side': pending['side'].value,
                        'units': pending['units'], 'reduce_only': phase in {'CLOSE_E', 'CLOSE_H', 'REPAIR'},
                        'limit': str(pending['limit']), 'ts': ts})
                else:
                    blocked['request_budget'] += 1
                    if phase in {'OPEN_E', 'CLOSE_E'}:
                        cycle['phase'] = 'DONE' if phase == 'OPEN_E' else 'OPEN'
                        pending = None
            if pending is not None and ts >= pending['due']:
                assert cycle is not None
                if pending['sample'].get('resolved_after_unknown'):
                    resolved_unknown_orders += 1
                age = pending['due'] - pending['asof_ts']
                terminal_asof_ages.append(age)
                fraction = pending['sample']['fraction']
                known = pending['sample'].get('known_cumulative_units')
                known_partial = (fraction is None and type(known) is int
                                 and 0 < known <= pending['units'])
                if known_partial:
                    fraction = known / pending['units']
                if fraction is None:
                    cycle['phase'] = 'UNKNOWN'
                    cycle['unknown_orders'] += 1
                    resolution_delay = pending['sample'].get('resolve_after_seconds')
                    resolved_fraction = pending['sample'].get('resolved_fraction')
                    if resolution_delay is not None and resolved_fraction is not None:
                        pending['due'] = ts + resolution_delay
                        pending['sample'] = {**pending['sample'],
                            'fraction': resolved_fraction, 'resolved_after_unknown': True}
                        trace.append({'event': 'resolve', 'order_ref': pending['order_ref']})
                    else:
                        pending['due'] = math.inf
                    blocked['unknown_terminal'] += 1
                else:
                    attempt_sample = pending['sample']
                    filled = min(pending['units'], math.floor(pending['units'] * fraction))
                    side, venue = pending['side'], pending['venue']
                    # Matching time is unavailable. Freeze the submit book and
                    # label this a scenario, rather than pricing at confirmation.
                    quote = _quote_cash(pending['asof_market'], instrument,
                                        venue, side, filled) if filled else None
                    if quote is None and filled:
                        filled = 0
                    if filled:
                        cash, marginal = quote
                        cost = pending['sample'].get('execution_cost_bps')
                        if cost is not None:
                            multiplier = 1 + dec(cost) / 10000 * (
                                1 if side is Side.BUY else -1)
                            cash *= multiplier
                            marginal *= multiplier
                        if (side is Side.BUY and marginal > pending['limit'] or
                                side is Side.SELL and marginal < pending['limit']):
                            filled = 0
                    if filled:
                        fee_bps = dec(pending['sample'].get('fee_bps', scenario_fees.get(
                            venue, instrument.entropy_fee_bps if venue == 'entropy'
                            else instrument.hedge_fee_bps)))
                        fee = cash * fee_bps / 10000
                        cycle['cash'] += (cash if side is Side.SELL else -cash) - fee
                        account_cash += (cash if side is Side.SELL else -cash) - fee
                        cycle['fees'] += fee
                        sign = 1 if side is Side.BUY else -1
                        cycle['e_units' if venue == 'entropy' else 'h_units'] += sign * filled
                        if pending['phase'] == 'OPEN_E':
                            cycle['entry_units'] = filled
                            cycle['notional'] = cash
                            cycle['entry_e_cash'] += cash
                        elif pending['phase'] in {'OPEN_H', 'REPAIR_PAIR'}:
                            cycle['entry_h_cash'] += cash
                            cycle['entry_h_units'] += filled
                        if pending['phase'] in {'OPEN_H', 'CLOSE_H'}:
                            matched_units += filled
                    phase = pending['phase']
                    trace.append({'event': 'known_partial' if known_partial else 'terminal', 'order_ref': pending['order_ref'],
                                  'filled_units': filled, 'e_units': cycle['e_units'],
                                  'h_units': cycle['h_units'], 'cash_usd': str(cycle['cash'])})
                    if known_partial:
                        # Positions are visible before the unresolved order's
                        # cash accounting can be finalized. Retain its identity.
                        deferred_cash = (cash if side is Side.SELL else -cash) - fee if filled else Decimal(0)
                        cycle['cash'] -= deferred_cash
                        cycle['fees'] -= fee if filled else Decimal(0)
                        account_cash -= deferred_cash
                        unknown_order = {**pending, 'known_units': filled,
                            'deferred_cash': deferred_cash,
                            'deferred_fee': fee if filled else Decimal(0),
                            'resolve_due': (ts + attempt_sample['resolve_after_seconds']
                                if attempt_sample.get('resolve_after_seconds') is not None
                                and attempt_sample.get('resolved_fraction') is not None else math.inf)}
                        cycle['unknown_orders'] += 1
                    handoff = pending['sample'].get('handoff_seconds')
                    cycle['not_before'] = ts + (handoff or 0.)
                    pending = None
                    if phase == 'OPEN_E':
                        cycle['phase'] = 'OPEN_H' if filled else 'OPEN_E'
                        if filled:
                            cycle['attempts'] = 0
                    elif phase in {'OPEN_H', 'REPAIR_PAIR'}:
                        cycle['phase'] = ('OPEN' if cycle['e_units'] + cycle['h_units'] == 0
                                          else phase)
                        if cycle['phase'] == 'OPEN' and cycle['entry_units']:
                            cycle['attempts'] = 0
                    elif phase == 'CLOSE_E':
                        cycle['phase'] = 'CLOSE_H' if filled else 'CLOSE_E'
                        if filled:
                            cycle['attempts'] = 0
                    elif phase == 'CLOSE_H':
                        cycle['phase'] = ('OPEN' if cycle['e_units'] + cycle['h_units'] == 0
                                          and cycle['e_units'] else
                                          'DONE' if cycle['e_units'] == cycle['h_units'] == 0
                                          else 'CLOSE_H')
                    else:
                        cycle['phase'] = ('OPEN' if cycle['e_units'] + cycle['h_units'] == 0
                                          and cycle['e_units'] else
                                          'DONE' if cycle['e_units'] == cycle['h_units'] == 0
                                          else 'REPAIR')
                    if cycle['phase'] in {'OPEN_H', 'CLOSE_H', 'REPAIR_PAIR'} and cycle['attempts'] >= len(cfg.hedge_rooms_bps):
                        cycle['phase'] = 'REPAIR_PAIR' if phase == 'OPEN_H' else 'REPAIR'
                        cycle['attempts'] = 0
                    attempt_limit = (len(cfg.first_leg_execution_policy(
                        'open' if phase == 'OPEN_E' else 'close')[0])
                        if phase in {'OPEN_E', 'CLOSE_E'} else len(cfg.hedge_rooms_bps))
                    if filled == 0 and cycle['attempts'] >= attempt_limit:
                        if phase == 'OPEN_E':
                            cycle['phase'] = 'DONE'
                        elif phase == 'CLOSE_E':
                            cycle['phase'] = 'OPEN'
                        elif phase in {'OPEN_H', 'CLOSE_H'}:
                            cycle['phase'] = 'REPAIR'
                            cycle['attempts'] = 0
                        else:
                            cycle['phase'] = 'UNRESOLVED'
                    if known_partial:
                        evidence = attempt_sample.get('position_evidence') or {}
                        if (evidence.get('authoritative') is True and
                                (evidence.get('entropy_units'), evidence.get('hedge_units')) ==
                                (cycle['e_units'], cycle['h_units'])):
                            cycle['phase'] = 'UNKNOWN' if phase == 'OPEN_E' else 'REPAIR'
                            cycle['attempts'] = 0
                            if phase == 'OPEN_E' and not math.isfinite(unknown_order['resolve_due']):
                                # A visible partial still cannot authorize new
                                # exposure before this same order is terminal.
                                model_censoring = 'pending_open_blocks_new_exposure'
                        else:
                            cycle['phase'] = 'UNRESOLVED'
                            model_censoring = 'unknown_partial_without_authoritative_positions'
                    if unknown_order is not None and cycle['phase'] == 'DONE':
                        cycle['phase'] = 'UNKNOWN'
                    if attempt_sample.get('settlement_status') in {'pending', 'unavailable', 'conflict'}:
                        cycle['phase'] = 'UNRESOLVED'
                        model_censoring = 'unmodeled_settlement_' + attempt_sample['settlement_status']
            if (cycle is not None and cycle['phase'] == 'OPEN'
                    and not cycle['paired_opened'] and unknown_order is None
                    and cycle['entry_units'] and cycle['entry_h_units']):
                e_px = cycle['entry_e_cash'] / instrument.quantity(cycle['entry_units'])
                h_px = cycle['entry_h_cash'] / instrument.quantity(cycle['entry_h_units'])
                cycle['entry_basis'] = (e_px / h_px - 1) * 10000
                cycle['profit_entry_notional_usd'] = (
                    instrument.quantity(abs(cycle['e_units'])) * (e_px + h_px) / 2)
                cycle['paired_opened'] = True
            finished_on_frame = cycle is not None and cycle['phase'] == 'DONE'
            if finished_on_frame:
                record = {'pnl_usd': str(cycle['cash']),
                          'duration_seconds': ts - cycle['started'],
                          'orders': cycle['orders'], 'funding_usd': str(cycle['funding']),
                          'fees_usd': str(cycle['fees']),
                          'direction': cycle['direction'].value,
                          'repair_used': cycle['repair_used'],
                          'paired_opened': cycle['paired_opened'],
                          'profit_entry_notional_usd': (
                              str(cycle['profit_entry_notional_usd'])
                              if cycle['profit_entry_notional_usd'] is not None else None),
                          'result_kind': ('zero_fill' if not cycle['entry_units'] else
                                          'repair' if cycle['repair_used'] else 'clean'),
                          'worst_observed_mark_usd': str(cycle['worst_mark']),
                          'exit_reasons': list(cycle['exit_reasons'])}
                if cycle['entry_units'] and (score_start_ts is None or
                        row['capture_wall_ts'] >= score_start_ts):
                    complete.append(record)
                    if cycle['repair_used'] and cycle['cash'] < 0:
                        repair_loss += cycle['cash']
                elif not cycle['entry_units']:
                    zero_fill_sequences += 1
                cycle = None
            if (cycle is None and not finished_on_frame and view.usable
                    and view.center_bps is not None and
                    (trade_start_ts is None or row['capture_wall_ts'] >= trade_start_ts)):
                choice = _choose_plan(market, instrument, effective_cfg, view.center_bps,
                                      minimum, unit_override, direction_filter)
                if choice is not None:
                    direction, units, _ = choice
                    cycle = {'phase': 'OPEN_E', 'direction': direction,
                             'planned_units': units, 'entry_units': 0,
                             'signal_reference_bps': view.center_bps,
                             'entry_basis': None, 'entry_e_cash': Decimal(0),
                             'entry_h_cash': Decimal(0), 'entry_h_units': 0,
                             'profit_entry_notional_usd': None,
                             'paired_opened': False, 'e_units': 0, 'h_units': 0,
                             'cash': Decimal(0), 'fees': Decimal(0),
                             'funding': Decimal(0), 'notional': Decimal(0),
                             'started': ts, 'attempts': 0, 'orders': 0,
                             'repair_used': False, 'exit_reasons': [],
                             'unknown_orders': 0, 'last_mark': None,
                             'worst_mark': Decimal(0), 'not_before': ts}
            if cycle is not None and pending is None and cycle['phase'] not in {
                    'UNKNOWN', 'UNRESOLVED'} and ts >= cycle['not_before']:
                phase = cycle['phase']
                if phase == 'OPEN' and unknown_order is None and not resolved_this_frame:
                    if cycle['entry_basis'] is None:
                        cycle['phase'] = 'UNRESOLVED'
                    else:
                        try:
                            decision = exit_decision(
                                market, instrument, effective_cfg, cycle['direction'],
                                abs(cycle['e_units']), cycle['entry_basis'],
                                cycle['cash'], accrued_price_cash_usd=(
                                    cycle['cash'] + cycle['fees'] - cycle['funding']),
                                profit_entry_notional_usd=cycle['profit_entry_notional_usd'])
                        except ValueError:
                            decision = None
                        if decision is not None and decision.allowed:
                            cycle['phase'] = phase = 'CLOSE_E'
                            cycle['attempts'] = 0
                            cycle['exit_reasons'].append(decision.reason)
                            exit_reasons[decision.reason] += 1
                if cycle['phase'] in {'OPEN_E', 'OPEN_H', 'CLOSE_E', 'CLOSE_H', 'REPAIR', 'REPAIR_PAIR'}:
                    phase = cycle['phase']
                    direction = cycle['direction']
                    venue = ('hedge' if phase in {'OPEN_H', 'CLOSE_H', 'REPAIR_PAIR'} else 'entropy')
                    if phase == 'REPAIR_PAIR':
                        cycle['repair_used'] = True
                    if phase == 'REPAIR':
                        cycle['repair_used'] = True
                        venue = ('entropy' if abs(cycle['e_units']) > abs(cycle['h_units'])
                                 else 'hedge')
                    if phase == 'OPEN_E':
                        side, units = direction.entropy_side, cycle['planned_units']
                        if cycle['attempts']:
                            try:
                                fresh_plan = plan_open(
                                    market, instrument, effective_cfg,
                                    cycle['signal_reference_bps'], direction, units)
                            except ValueError:
                                fresh_plan = None
                            if fresh_plan is None:
                                cycle['phase'] = 'DONE'
                                blocked['open_recheck_failed'] += 1
                    elif phase in {'OPEN_H', 'REPAIR_PAIR'}:
                        side = direction.entropy_side.opposite
                        units = abs(cycle['e_units'] + cycle['h_units'])
                    elif phase == 'CLOSE_E':
                        side, units = direction.entropy_side.opposite, abs(cycle['e_units'])
                        if cycle['attempts'] and cycle['entry_basis'] is not None:
                            try:
                                decision = exit_decision(market, instrument, effective_cfg,
                                    direction, units, cycle['entry_basis'], cycle['cash'],
                                    accrued_price_cash_usd=(cycle['cash'] + cycle['fees']
                                                           - cycle['funding']),
                                    profit_entry_notional_usd=cycle['profit_entry_notional_usd'])
                            except ValueError:
                                decision = None
                            if decision is None or not decision.allowed:
                                cycle['phase'] = 'OPEN'
                                blocked['close_recheck_failed'] += 1
                    elif phase == 'CLOSE_H':
                        side = direction.entropy_side
                        units = abs(cycle['e_units'] + cycle['h_units'])
                    else:
                        position = cycle['e_units' if venue == 'entropy' else 'h_units']
                        side = Side.SELL if position > 0 else Side.BUY
                        units = abs(cycle['e_units'] + cycle['h_units'])
                    raw_instrument = instrument_override or row.get('instrument') or {}
                    prefix = 'entropy' if venue == 'entropy' else 'lighter'
                    book_mid = market.entropy.mid if venue == 'entropy' else market.hedge.mid
                    action_minimum = max(1,
                        math.ceil(dec(raw_instrument[prefix + '_min_base']) / instrument.quantity_step),
                        math.ceil(dec(raw_instrument[prefix + '_min_quote']) / book_mid / instrument.quantity_step))
                    if cycle['phase'] == phase and 0 < units < action_minimum:
                        cycle['phase'] = 'UNRESOLVED'
                        blocked['below_exchange_minimum'] += 1
                    if cycle['phase'] == phase and units >= action_minimum:
                        quote = _quote_cash(market, instrument, venue, side, units)
                        if quote is None:
                            blocked['insufficient_depth'] += 1
                        else:
                            book = market.entropy if venue == 'entropy' else market.hedge
                            rooms, cap = (cfg.first_leg_execution_policy(
                                'close' if phase == 'CLOSE_E' else 'open')
                                if phase in {'OPEN_E', 'CLOSE_E'} else
                                (cfg.hedge_rooms_bps, cfg.hedge_hard_cap_bps))
                            tick = (instrument.entropy_tick if venue == 'entropy'
                                    else instrument.hedge_tick)
                            try:
                                limit = price_limit(book.top(side), quote[1], side,
                                    rooms[min(cycle['attempts'], len(rooms)-1)], cap, tick)
                            except ValueError:
                                blocked['limit_not_marketable'] += 1
                            else:
                                identity = f'{session_id}:{row.get("sample_seq")}:{cycle["orders"]}:{phase}'
                                sample = _draw(calibration,
                                    'repair' if phase in {'REPAIR', 'REPAIR_PAIR'} else
                                    'close' if phase.startswith('CLOSE') else 'open',
                                    venue, identity, scenario)
                                if sample is None:
                                    cycle['phase'] = 'UNRESOLVED'
                                    blocked['missing_attempt_calibration'] += 1
                                elif (sample.get('decision_to_submit_seconds') or 0) > 0:
                                    pending = {'phase': phase, 'venue': venue,
                                        'order_ref': identity, 'side': side, 'units': units,
                                        'limit': limit, 'submitted': False, 'due': math.inf,
                                        'submit_due': ts + sample['decision_to_submit_seconds'],
                                        'asof_market': market, 'asof_ts': ts, 'sample': sample}
                                elif budget.use(venue, ts, completion=phase != 'OPEN_E'):
                                    pending = {'phase': phase, 'venue': venue,
                                               'order_ref': identity,
                                               'side': side, 'units': units,
                                               'limit': limit,
                                               'due': ts + sample['latency_seconds'],
                                               'asof_market': market, 'asof_ts': ts,
                                               'sample': sample}
                                    cycle['attempts'] += 1
                                    cycle['orders'] += 1
                                    total_orders += 1
                                    trace.append({'event': 'submit', 'order_ref': identity,
                                        'phase': phase, 'venue': venue, 'side': side.value,
                                        'units': units, 'reduce_only': phase in {'CLOSE_E', 'CLOSE_H', 'REPAIR'},
                                        'limit': str(limit), 'ts': ts})
                                else:
                                    blocked['request_budget'] += 1
                                    if phase == 'OPEN_E':
                                        cycle['phase'] = 'DONE'
                                    elif phase == 'CLOSE_E':
                                        cycle['phase'] = 'OPEN'
                if cycle['phase'] == 'REPAIR' and cycle['attempts'] >= len(cfg.hedge_rooms_bps):
                    cycle['phase'] = 'UNRESOLVED'
            reference.observe(ts, mid)
        if cycle is not None:
            outstanding = pending or unknown_order
            last_market = _market(frames[-1][1], frames[-1][0])
            details = _instrument(instrument_override or frames[-1][1].get('instrument'),
                                  str(pair_id or 'unknown_pair'))
            mark = (_mark(cycle, last_market, details[0], {
                venue: (max((details[0].entropy_fee_bps if venue == 'entropy'
                             else details[0].hedge_fee_bps), dec(value))
                        if scenario == 'conservative' else dec(value))
                for venue, value in calibration.get(
                    'observed_fee_bps_by_venue', {}).items()})
                    if last_market is not None and details is not None else None)
            incomplete.append({'session_id': session_id, 'phase': cycle['phase'],
                               'entropy_units': cycle['e_units'], 'hedge_units': cycle['h_units'],
                               'occupied_notional_usd': str(cycle['notional']),
                               'profit_entry_notional_usd': (
                                   str(cycle['profit_entry_notional_usd'])
                                   if cycle['profit_entry_notional_usd'] is not None else None),
                               'request_window': budget.uses,
                               'pending_order_ref': outstanding['order_ref'] if outstanding else None,
                               'pending_requested_units': outstanding['units'] if outstanding else None,
                               'pending_known_cumulative_units': (
                                   outstanding['sample'].get('known_cumulative_units') if outstanding else None),
                               'pending_terminal_unobserved': outstanding is not None,
                               'pending_submitted': (pending.get('submitted', True)
                                   if pending is not None else None),
                               'known_cash_usd': str(cycle['cash']),
                               'window_end_executable_mark_usd': (
                                   str(mark) if mark is not None and unknown_order is None else None),
                               'unfinalized_order_cash_usd': (
                                   str(unknown_order['deferred_cash']) if unknown_order else None),
                               'mark_is_realized': False,
                               'unknown_order_count': cycle['unknown_orders'],
                               'orders': cycle['orders'],
                               'worst_observed_mark_usd': str(cycle['worst_mark'])})
        final_state = {'phase': cycle['phase'] if cycle else 'FLAT',
            'entropy_units': cycle['e_units'] if cycle else 0,
            'hedge_units': cycle['h_units'] if cycle else 0,
            'known_cash_usd': str(account_cash), 'request_window': budget.uses,
            'pending_order_ref': ((pending or unknown_order)['order_ref']
                                  if pending or unknown_order else None),
            'reference': reference.checkpoint(),
            'last_capture_wall_ts': frames[-1][1].get('capture_wall_ts')}
        if len(segments) > 1 or boundary != 'session_end':
            censored_at = {'reason': boundary, 'wall_ts': frames[-1][1].get('capture_wall_ts'),
                           'unprocessed_segments': len(segments) - 1}
    if score_start_ts is not None:
        window_start = score_start_ts
    if model_censoring is not None:
        censored_at = {'reason': model_censoring}
    hours = ((window_end - window_start) / 3600
             if window_start is not None and window_end is not None
             and window_end > window_start else None)
    realized = sum((dec(row['pnl_usd']) for row in complete), Decimal(0))
    durations = [row['duration_seconds'] for row in complete]
    adverse = [float(max(Decimal(0), -dec(row['worst_observed_mark_usd'])))
               for row in complete + incomplete]
    paired_complete = [row for row in complete if row['paired_opened']]
    cycles_total = len(complete) + len(incomplete)
    return {'scenario': scenario, 'status': 'CENSORED' if censored_at else 'SCENARIO_ONLY',
            'execution_model': 'legacy_sequential_exit', 'live_execution_parity': False,
            'path_censoring': censored_at, 'action_trace': trace,
            'final_account_state': final_state,
            'price_model': 'submit_book_with_observed_cost_if_available',
            'timing_model': 'terminal receipt scenario, not exchange execution time',
            'funding_known': calibration.get('funding_per_notional_hour') is not None,
            'profit_lower_bound': None,
            'window_hours': hours, 'complete_cycle_count': len(paired_complete),
            'realized_cycle_count': len(complete),
            'zero_fill_sequence_count': zero_fill_sequences,
            'incomplete_cycle_count': len(incomplete),
            'incomplete_fraction': (len(incomplete) / cycles_total if cycles_total else None),
            'known_cash_scenario_usd': str(realized) if rows else None,
            'realized_pnl_usd': (str(realized) if rows and not censored_at and
                calibration.get('funding_per_notional_hour') is not None else None),
            'realized_pnl_per_hour_usd': (str(realized / dec(hours)) if hours and
                calibration.get('funding_per_notional_hour') is not None and not censored_at else None),
            'complete_cycles_per_hour': (len(paired_complete) / hours if hours else None),
            'matched_units': matched_units, 'order_count': total_orders,
            'resolved_unknown_order_count': resolved_unknown_orders,
            'orders_per_complete_cycle': (total_orders / len(paired_complete)
                                          if paired_complete else None),
            'hold_seconds_p50': _rate_percentile(durations, .5),
            'hold_seconds_p90': _rate_percentile(durations, .9),
            'repair_loss_usd': str(repair_loss),
            'repair_loss_definition': 'negative net PnL of realized repair cycles; proxy, not isolated incremental slippage',
            'max_adverse_usd': max(adverse, default=None),
            'terminal_asof_age_seconds_p50': _rate_percentile(
                terminal_asof_ages, .5),
            'terminal_asof_age_seconds_p90': _rate_percentile(
                terminal_asof_ages, .9),
            'capital_utilization': (str(occupied_notional_hours /
                                        cfg.max_notional_usd / dec(hours))
                                    if hours else None),
            'exit_reason_counts': dict(exit_reasons),
            'blocked_counts': dict(blocked), 'complete_cycles': complete,
            'incomplete_cycles': incomplete}


def _attempt_attribution(attempts: list[dict]) -> dict:
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in attempts:
        phase = _attempt_phase(row)
        if phase is not None and row.get('venue') in {'entropy', 'hedge'}:
            groups[f'{phase}:{row["venue"]}'].append(row)
    result = {}
    for key, group in sorted(groups.items()):
        delays = []
        for row in group:
            start = _number(row.get('submit_start_ts') or row.get('submit_ts'))
            end = _number(row.get('terminal_received_ts') or row.get('terminal_result_ts'))
            if _terminal(row) and start is not None and end is not None and end >= start:
                delays.append((end - start) * 1000)
        result[key] = {'attempts': len(group),
                       'positive': sum(_terminal(row) and (row.get('result_units') or 0) > 0
                                       for row in group),
                       'zero': sum(_terminal(row) and row.get('result_units') == 0 for row in group),
                       'unknown': sum(not _terminal(row) or row.get('result_units') is None for row in group),
                       'resolved_after_unknown': sum(_terminal(row) and row.get('had_unknown') is True
                                                     for row in group),
                       'partial': sum(type(row.get('result_units')) is int
                                      and 0 < row['result_units'] < row['requested_units']
                                      for row in group),
                       'terminal_latency_ms_p50': _rate_percentile(delays, .5),
                       'terminal_latency_ms_p90': _rate_percentile(delays, .9)}
    return {'status': 'OBSERVED_ATTEMPTS', 'groups': result,
            'caution': 'Observed outcomes calibrate shadow scenarios; they do not prove counterfactual IOC fills.'}


def _compact_replay(result: dict) -> dict:
    return {key: value for key, value in result.items()
            if key not in {'complete_cycles', 'incomplete_cycles'}}


def _time_range(rows: list[dict]) -> tuple[float | None, float | None]:
    values = [_number(row.get('capture_wall_ts')) for row in rows]
    valid = [value for value in values if value is not None]
    return (min(valid), max(valid)) if valid else (None, None)


def candidate_calibration(attempts: list[dict], cycles: list[dict],
                          settings: Settings) -> dict:
    """Describe admitted historical cycles; never predict excluded IOC fills."""
    ordered = sorted((dict(row) for row in cycles
                      if _number(row.get('signal_ts')) is not None),
                     key=lambda row: float(row['signal_ts']))
    first = {}
    for row in sorted(attempts, key=lambda r: _number(r.get('submit_ts')) or 0):
        if str(row.get('purpose') or '').startswith('open_first'):
            first.setdefault(row.get('cycle_id'), row)
    for row in ordered:
        attempt = first.get(row.get('cycle_id'), {})
        quote = attempt.get('decision_quote_json') or {}
        if isinstance(quote, str):
            try:
                quote = json.loads(quote)
            except ValueError:
                quote = {}
        depth = []
        side = attempt.get('side')
        units = _number(attempt.get('requested_units'))
        for venue, book_side in (('entropy', 'asks' if side == 'buy' else 'bids'),
                                 ('hedge', 'bids' if side == 'buy' else 'asks')):
            levels = (quote.get(venue) or {}).get(book_side) if isinstance(quote, dict) else None
            if isinstance(levels, list) and levels and units and units > 0 and side in {'buy', 'sell'}:
                quantities = [_number(level[1]) for level in levels
                              if isinstance(level, (list, tuple)) and len(level) == 2]
                if len(quantities) == len(levels) and all(q is not None and q > 0 for q in quantities):
                    depth.append(sum(quantities) / units)
        start = _number(attempt.get('submit_start_ts') or attempt.get('submit_ts'))
        end = _number(attempt.get('terminal_received_ts') or attempt.get('terminal_result_ts'))
        latency = end - start if _terminal(attempt) and start is not None and end is not None and end >= start else None
        amount = _number(row.get('planned_notional_usd'))
        volatility = _number(row.get('predecision_volatility_bps'))
        amount = amount if amount is not None and amount > 0 else None
        volatility = volatility if volatility is not None and volatility >= 0 else None
        row['_strata'] = {
            'direction': row.get('direction') or 'unknown',
            'result_kind': row.get('result_kind') or 'unknown',
            'planned_amount_usd': ('unknown' if amount is None else '<=5' if amount <= 5 else '<=15' if amount <= 15 else '>15'),
            'decision_depth_multiple': ('unknown' if len(depth) != 2 else '<1' if min(depth) < 1 else '1..10' if min(depth) <= 10 else '>10'),
            'predecision_volatility_bps': ('unknown' if volatility is None else '<=1' if volatility <= 1 else '<=3' if volatility <= 3 else '>3'),
            'terminal_receipt_seconds': ('unknown' if latency is None else '<=0.25' if latency <= .25 else '<=1' if latency <= 1 else '>1'),
            'first_attempt_room_bps': str(attempt.get('room_bps')) if attempt.get('room_bps') is not None else 'unknown',
        }

    def summary(rows: list[dict], boundary: float | None = None) -> dict:
        complete, missing = [], Counter()
        holds, occupied = [], []
        for row in rows:
            finish = _number(row.get('authoritative_reconcile_complete_ts'))
            if boundary is not None and (finish is None or finish >= boundary):
                missing['not_finalized_before_next_split'] += 1
                continue
            names = ('finalized_pnl_usd', 'actual_fees_usd', 'actual_funding_usd', 'actual_entry_notional_usd')
            missing.update(name for name in names if _number(row.get(name)) is None)
            if not row.get('accounting_final'):
                missing['accounting_not_final'] += 1
            if row.get('accounting_final') and all(_number(row.get(name)) is not None for name in names):
                complete.append(row)
            begin = _number(row.get('first_fill_observed_ts'))
            notional = _number(row.get('actual_entry_notional_usd'))
            if finish is not None and begin is not None and finish >= begin:
                holds.append(finish - begin)
                if notional is not None and notional > 0:
                    occupied.append(notional * (finish - begin) / 3600)
        return {'admitted_cycles': len(rows), 'complete_cost_cycles': len(complete),
                'status': 'OBSERVED_ONLY' if complete else 'UNESTIMABLE',
                'missing': dict(missing),
                'mean_net_usd': (str(sum(dec(r['finalized_pnl_usd']) for r in complete) / len(complete)) if complete else None),
                'fees_usd': str(sum(dec(r['actual_fees_usd']) for r in complete)) if complete else None,
                'signed_funding_usd': str(sum(dec(r['actual_funding_usd']) for r in complete)) if complete else None,
                'observed_hold_seconds_p50': _rate_percentile(holds, .5),
                'hold_coverage': len(holds), 'capital_coverage': len(occupied),
                'entry_notional_hours': sum(occupied) if occupied else None}

    warmup_end, train_end = len(ordered) // 5, len(ordered) * 7 // 10
    holdout_start = float(ordered[train_end]['signal_ts']) if train_end < len(ordered) else None
    # Equal signal timestamps stay together; training outcomes cannot cross C.
    train_start = float(ordered[warmup_end]['signal_ts']) if ordered else None
    train = [r for r in ordered if train_start <= float(r['signal_ts']) < holdout_start] if ordered else []
    holdout = [r for r in ordered if float(r['signal_ts']) >= holdout_start] if ordered else []
    strata = {}
    for dimension in ('direction', 'result_kind', 'planned_amount_usd', 'decision_depth_multiple',
                      'predecision_volatility_bps', 'terminal_receipt_seconds', 'first_attempt_room_bps'):
        strata[dimension] = {}
        for label in sorted({r['_strata'][dimension] for r in ordered}):
            strata[dimension][label] = {
                'train': summary([r for r in train if r['_strata'][dimension] == label], holdout_start),
                'holdout': summary([r for r in holdout if r['_strata'][dimension] == label])}
    thresholds = []
    for threshold in sorted({Decimal(3), Decimal(4), Decimal(5), settings.entry_band_bps}):
        thresholds.append({'entry_band_bps': str(threshold), **{
            stage: summary([r for r in rows if (_decimal(r.get('entry_dislocation_bps')) is not None
                and _decimal(r['entry_dislocation_bps']) >= threshold)], boundary)
            for stage, rows, boundary in (('train', train, holdout_start), ('holdout', holdout, None))}})
    return {'status': 'OBSERVED_HISTORY_ONLY' if ordered else 'UNESTIMABLE',
            'interpretation': 'Historical admitted cycles only. Threshold subsets and observed rooms are descriptive; excluded/unobserved IOC fills and causal price-room effects are unestimable. No EV or parameter recommendation.',
            'time_range': [float(ordered[0]['signal_ts']), float(ordered[-1]['signal_ts'])] if ordered else [None, None],
            'split': {'method': 'chronological signal time: first 20% warmup, next 50% train, final 30% holdout; training completion must precede holdout',
                      'train_start_ts': train_start, 'holdout_start_ts': holdout_start},
            'strata': strata, 'thresholds': thresholds,
            'price_room': {'candidate_effect': None, 'reason': 'only observed first-attempt rooms; counterfactual fills unavailable',
                           'configured_open_first_rooms_bps': [str(v) for v in settings.first_leg_execution_policy('open')[0]]},
            'convergence': {'kind': 'SCENARIO_ONLY', 'reversion_bps': ['1', '2', '3'], 'expected_net_usd': None,
                            'reason': 'reversion magnitude alone does not estimate fill, fee, funding or holding outcomes'},
            'holding_definition': 'first locally observed fill to authoritative reconcile, wall-clock proxy; entry notional hours is not margin capital or peak exposure',
            'latency_definition': 'submit to terminal receipt; diagnostic post-decision stratum, not exchange execution latency',
            'volatility_definition': 'requires explicit predecision_volatility_bps from a prior continuous window; order snapshots alone cannot supply it',
            'recommendation': None}


def compare_parameters(snapshots: list[dict], settings: Settings,
                       attempts: list[dict], cycles: list[dict],
                       instrument_override: dict | None = None) -> dict:
    ordered = sorted((row for row in snapshots
                      if _number(row.get('capture_wall_ts')) is not None),
                     key=lambda row: _number(row['capture_wall_ts']))
    split_cal = len(ordered) // 5
    split_validation = len(ordered) * 7 // 10
    split_internal = len(ordered) * 45 // 100
    train = ordered[split_cal:split_internal]
    validation = ordered[split_internal:split_validation]
    holdout = ordered[split_validation:]
    cutoff = (_number(train[0]['capture_wall_ts']) if train else 0.)
    metadata = (instrument_override or (ordered[0].get('instrument')
                                    if ordered else None))
    quantity_step = (_decimal(metadata.get('quantity_step'))
                     if isinstance(metadata, dict) else None)
    calibration = calibrate(attempts, cycles, cutoff, quantity_step,
                            require_new_execution=True)
    all_start, all_end = _time_range(ordered)
    train_start, train_end = _time_range(train)
    val_start, val_end = _time_range(validation)
    contiguous_seconds = sum(max(0., (_number(segment[-1][1].get('capture_wall_ts')) or 0)
                                 - (_number(segment[0][1].get('capture_wall_ts')) or 0))
                             for _, _, _, segment, _ in _clocked_sessions(
                                 ordered, min(settings.reset_gap_seconds, 2.0)))
    observed_span = (all_end - all_start if all_start is not None
                     and all_end is not None and all_end > all_start else None)
    input_quality = {
        'snapshot_count': len(snapshots), 'ordered_count': len(ordered),
        'source_attempt_count': len(attempts),
        'source_cycle_economics_count': len(cycles),
        'valid_fraction': (sum(row.get('valid') is True for row in snapshots) /
                           len(snapshots) if snapshots else None),
        'instrument_fraction': (sum(_instrument(
            instrument_override or row.get('instrument'),
            str(row.get('pair_id') or 'unknown_pair')) is not None
            for row in snapshots) / len(snapshots) if snapshots else None),
        'effective_authorization_cap_fraction': (
            sum((_decimal((instrument_override or row.get('instrument') or {}).get(
                'effective_max_notional_usd')) or Decimal(0)) > 0
            for row in snapshots) / len(snapshots) if snapshots else None),
        'pair_id_fraction': (sum(row.get('pair_id') is not None for row in snapshots) /
                             len(snapshots) if snapshots else None),
        'config_hash_fraction': (sum(row.get('config_hash') is not None for row in snapshots) /
                                 len(snapshots) if snapshots else None),
        'continuous_time_coverage': (min(1., contiguous_seconds / observed_span)
                                     if observed_span else None),
        'pair_ids': sorted({str(row.get('pair_id')) for row in snapshots
                            if row.get('pair_id') is not None}),
        'config_hashes': sorted({str(row.get('config_hash')) for row in snapshots
                                 if row.get('config_hash') is not None}),
        'effective_cap_values': sorted({str((instrument_override or
            row.get('instrument') or {}).get('effective_max_notional_usd'))
            for row in snapshots if (instrument_override or
            row.get('instrument') or {}).get('effective_max_notional_usd') is not None}),
        'time_range': [all_start, all_end],
        'training_time_range': [train_start, train_end],
        'validation_time_range': [val_start, val_end],
        'holdout_time_range': list(_time_range(holdout)),
        'split': 'frame-count chronological: A 20% calibration/reference warmup; B 50% training/internal validation; C 30% untouched final holdout',
    }
    enough_market = (len(train) >= 1000 and len(validation) >= 600
                     and train_start is not None and train_end is not None
                     and val_start is not None and val_end is not None
                     and val_end - train_start >= 3600
                     and len(holdout) >= 600
                     and _time_range(holdout)[1] - _time_range(holdout)[0] >= 3600
                     and input_quality['valid_fraction'] >= .95
                     and input_quality['instrument_fraction'] >= .99
                     and input_quality['effective_authorization_cap_fraction'] >= .99
                     and input_quality['pair_id_fraction'] >= .99
                     and input_quality['config_hash_fraction'] >= .99
                     and input_quality['continuous_time_coverage'] >= .8
                     and len(input_quality['pair_ids']) == 1
                     and len(input_quality['config_hashes']) == 1
                     and len(input_quality['effective_cap_values']) == 1)
    fixed_first_room = all(
        rooms == (Decimal(3), Decimal(3), Decimal(3))
        for rooms, _ in (settings.first_leg_execution_policy('open'),
                         settings.first_leg_execution_policy('close')))
    input_quality['fixed_3bp_first_leg_ladders'] = fixed_first_room
    enough_market = enough_market and fixed_first_room
    result = {'status': 'INSUFFICIENT_DATA',
              'execution_model': 'legacy_sequential_exit', 'live_execution_parity': False,
              'reason': 'Need chronological single-pair/single-config executable snapshots, verified effective authorization caps and instrument metadata, fixed 3bp first-leg ladders, at least 30 telemetry-v2 terminal attempts in each first/hedge open/close stratum, 20 actual fee observations per venue, and 20 finalized funding cycles before train start.',
              'effective_settings': {
                  field.name: ([str(x) for x in value] if isinstance(value, tuple)
                               else str(value) if isinstance(value, Decimal) else value)
                  for field in fields(settings)
                  if (value := getattr(settings, field.name)) is not None},
              'input_quality': input_quality,
              'calibration': {key: value for key, value in calibration.items()
                              if key != 'groups'},
              'experiments': [], 'shadow_stage_selections': {},
              'recommendation': None,
              'final_holdout': None,
              'no_fill_prediction': True}
    if not calibration['ready'] or not enough_market:
        return result
    result['status'] = 'SHADOW_SCENARIOS'
    result['reason'] = ('Legacy sequential-exit shadow scenarios only; they do not validate '
                        'the live concurrent, latched basis exit. IOC fills remain uncertain.')
    stable_changes: list[str] = []

    def experiment(stage: str, label: str, cfg: Settings, *,
                   direction: Direction | None = None,
                   units: int | None = None) -> dict:
        train_result = replay(ordered[:split_internal], cfg, calibration, scenario='conservative',
                              trade_start_ts=cutoff, score_start_ts=train_start,
                              direction_filter=direction, unit_override=units,
                              instrument_override=instrument_override)
        validation_result = replay(ordered[:split_validation], cfg, calibration,
                                   trade_start_ts=cutoff, score_start_ts=val_start,
                                   scenario='conservative',
                                   direction_filter=direction, unit_override=units,
                                   instrument_override=instrument_override)
        neutral_result = replay(ordered[:split_validation], cfg, calibration, scenario='neutral',
                                trade_start_ts=cutoff, score_start_ts=val_start,
                                direction_filter=direction, unit_override=units,
                                instrument_override=instrument_override)
        row = {'stage': stage, 'label': label,
               'direction': direction.value if direction else 'both',
               'units': units,
               'settings': {'reference_half_life_seconds': cfg.reference_half_life_seconds,
                            'entry_band_bps': str(cfg.entry_band_bps),
                            'exit_min_gross_profit_bps': str(cfg.exit_min_gross_profit_bps)},
               'train_conservative': _compact_replay(train_result),
               'validation_conservative': _compact_replay(validation_result),
               'validation_neutral': _compact_replay(neutral_result)}
        result['experiments'].append(row)
        return row

    def choose(stage: str, candidates: list[tuple[str, Settings]],
               current_label: str, *, direction: Direction | None = None,
               units_by_label: dict[str, int] | None = None) -> str:
        reports = [experiment(stage, label, cfg, direction=direction,
                              units=(units_by_label or {}).get(label))
                   for label, cfg in candidates]
        eligible = [row for row in reports if
                    row['train_conservative']['complete_cycle_count'] >= 20
                    and row['validation_conservative']['complete_cycle_count'] >= 10
                    and row['train_conservative']['incomplete_fraction'] <= .1
                    and row['validation_conservative']['incomplete_fraction'] <= .1]
        if not eligible:
            result['shadow_stage_selections'][stage + ':' +
                (direction.value if direction else 'both')] = {
                    'label': None, 'reason': 'too few complete cycles or excessive incomplete cycles'}
            return current_label
        winner = max(eligible, key=lambda row: _number(
            row['train_conservative']['realized_pnl_per_hour_usd'])
            if row['train_conservative']['realized_pnl_per_hour_usd'] is not None
            else -math.inf)
        baseline = next((row for row in eligible if row['label'] == current_label), None)
        winner_score = _number(winner['validation_conservative'][
            'realized_pnl_per_hour_usd'])
        baseline_score = (_number(baseline['validation_conservative'][
            'realized_pnl_per_hour_usd']) if baseline is not None else None)
        selected, reason = current_label, 'validation did not improve over current'
        if (winner['label'] != current_label and baseline_score is not None
                and winner_score is not None and winner_score > baseline_score
                and winner_score > 0):
            candidate_cfg = next(cfg for label, cfg in candidates
                                 if label == winner['label'])
            baseline_cfg = next(cfg for label, cfg in candidates
                                if label == current_label)
            stable = True
            for index in range(3):
                fold_start = split_internal + len(validation) * index // 3
                fold_end = split_internal + len(validation) * (index + 1) // 3
                fold = ordered[:fold_end]
                common = {'scenario': 'conservative',
                          'trade_start_ts': cutoff,
                          'score_start_ts': ordered[fold_start]['capture_wall_ts'],
                          'direction_filter': direction,
                          'instrument_override': instrument_override}
                candidate_fold = replay(fold, candidate_cfg, calibration,
                    unit_override=(units_by_label or {}).get(winner['label']),
                    **common)
                baseline_fold = replay(fold, baseline_cfg, calibration,
                    unit_override=(units_by_label or {}).get(current_label),
                    **common)
                candidate_score = _number(candidate_fold['realized_pnl_per_hour_usd'])
                baseline_fold_score = _number(baseline_fold['realized_pnl_per_hour_usd'])
                if (candidate_fold['complete_cycle_count'] < 5
                        or baseline_fold['complete_cycle_count'] < 5
                        or candidate_fold['incomplete_fraction'] is None
                        or candidate_fold['incomplete_fraction'] > .1
                        or candidate_score is None or baseline_fold_score is None
                        or candidate_score <= max(0., baseline_fold_score)):
                    stable = False
                    break
            if stable:
                selected, reason = winner['label'], 'train selected; three chronological validation folds improved'
                stable_changes.append(stage)
            else:
                reason = 'chronological validation folds did not consistently improve'
        result['shadow_stage_selections'][stage + ':' +
            (direction.value if direction else 'both')] = {
                'label': selected, 'reason': reason}
        return selected

    half = choose('reference_half_life_seconds',
                  [(str(value), replace(settings,
                    reference_half_life_seconds=float(value)))
                   for value in (30, 60, 120)],
                  str(int(settings.reference_half_life_seconds)))
    chosen = replace(settings, reference_half_life_seconds=float(half))
    bands = {}
    for direction in Direction:
        bands[direction.value] = choose('entry_band_bps',
            [(str(value), replace(chosen, entry_band_bps=Decimal(value)))
             for value in (3, 4, 5)], str(settings.entry_band_bps),
            direction=direction)
    if len(set(bands.values())) == 1:
        chosen = replace(chosen, entry_band_bps=Decimal(next(iter(bands.values()))))
    sizes = sorted({item['units'] for row in train for item in row.get('quotes') or []
                    if type(item.get('units')) is int and item['units'] > 0})
    if sizes:
        size_labels = {str(value): value for value in
                       (sizes[0], sizes[len(sizes)//2], sizes[-1])}
        choose('quantity_units', [('live_dynamic', chosen)] +
               [(label, chosen) for label in size_labels],
               'live_dynamic',
               units_by_label=size_labels)
    patch = {}
    if float(half) != settings.reference_half_life_seconds:
        patch['reference_half_life_seconds'] = float(half)
    if len(set(bands.values())) == 1 and Decimal(next(iter(bands.values()))) != settings.entry_band_bps:
        patch['entry_band_bps'] = next(iter(bands.values()))
    # Only the final deployable settings see C; the dynamic quantity policy is
    # retained. Prefix B runs causally to carry positions and budgets into C.
    holdout_result = replay(ordered, chosen, calibration, scenario='conservative',
                            trade_start_ts=cutoff,
                            score_start_ts=_time_range(holdout)[0],
                            instrument_override=instrument_override)
    result['final_holdout'] = _compact_replay(holdout_result)
    result['holdout_evaluation_count'] = 1
    result['quantity_research_only'] = True
    if (patch and holdout_result.get('status') == 'SCENARIO_ONLY'
            and holdout_result.get('funding_known') is True
            and holdout_result['complete_cycle_count'] >= 10
            and (_number(holdout_result['realized_pnl_per_hour_usd']) or 0) > 0):
        result['recommendation'] = {
            'kind': 'OFFLINE_SHADOW_CANDIDATE', 'config_patch': patch,
            'validated_stages': sorted(set(stable_changes)),
            'production_change_authorized': False,
            'warning': 'Counterfactual IOC fill uncertainty remains; controlled live validation required.'}
    return result


def write_report(output: Path, snapshots: list[dict], found: list[dict],
                 settings: Settings, *, attempts: list[dict] | None = None,
                 cycles: list[dict] | None = None,
                 instrument_override: dict | None = None) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / 'opportunities.json').write_text(json.dumps(found, indent=2) + '\n')
    fields = list(found[0]) if found else [
        'pair_id', 'session_id', 'sample_seq', 'trigger_ts', 'direction',
        'units', 'reference_bps', 'executable_basis_bps', 'censored']
    with (output / 'opportunities.csv').open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({key: json.dumps(value) if isinstance(value, (dict, list))
                          else value for key, value in row.items()} for row in found)
    comparison = compare_parameters(snapshots, settings, attempts or [],
                                    cycles or [], instrument_override)
    (output / 'candidate_calibration.json').write_text(json.dumps(
        candidate_calibration(attempts or [], cycles or [], settings), indent=2) + '\n')
    state = comparison['status']
    comparison.update({'snapshot_count': len(snapshots), 'episode_count': len(found),
        'observed_crossing_count': sum(
            row['onset_status'] == 'observed_crossing' for row in found),
        'left_censored_count': sum(
            row['onset_status'] == 'left_censored' for row in found),
        'coverage': _coverage(found)})
    (output / 'parameter_comparison.json').write_text(
        json.dumps(comparison, indent=2) + '\n')
    with (output / 'parameter_comparison.csv').open('w', newline='') as handle:
        fields = ['stage', 'label', 'direction', 'units', 'train_pnl_per_hour_usd',
                  'validation_conservative_pnl_per_hour_usd',
                  'validation_neutral_pnl_per_hour_usd',
                  'validation_complete_cycles', 'validation_incomplete_cycles']
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in comparison['experiments']:
            writer.writerow({
                'stage': row['stage'], 'label': row['label'],
                'direction': row['direction'], 'units': row['units'],
                'train_pnl_per_hour_usd': row['train_conservative'][
                    'realized_pnl_per_hour_usd'],
                'validation_conservative_pnl_per_hour_usd': row[
                    'validation_conservative']['realized_pnl_per_hour_usd'],
                'validation_neutral_pnl_per_hour_usd': row[
                    'validation_neutral']['realized_pnl_per_hour_usd'],
                'validation_complete_cycles': row['validation_conservative'][
                    'complete_cycle_count'],
                'validation_incomplete_cycles': row['validation_conservative'][
                    'incomplete_cycle_count']})
    attribution = (_attempt_attribution(attempts) if attempts else
                   {'status': 'INSUFFICIENT_DATA',
                    'reason': 'No live attempt export supplied'})
    (output / 'replay_calibration.json').write_text(
        json.dumps(attribution, indent=2) + '\n')
    attribution_path = output / 'execution_attribution.json'
    try:
        existing_attribution = json.loads(attribution_path.read_text())
    except (OSError, ValueError):
        existing_attribution = {}
    if 'attempt_count' not in existing_attribution:
        attribution_path.write_text(json.dumps(attribution, indent=2) + '\n')
    (output / 'candidate_config.json').write_text(
        json.dumps({'status': state, 'current_entry_band_bps': str(settings.entry_band_bps),
                    'current_reference_half_life_seconds':
                        settings.reference_half_life_seconds,
                    'current_exit_min_gross_profit_bps': str(settings.exit_min_gross_profit_bps),
                    'shadow_stage_selections': comparison['shadow_stage_selections'],
                    'recommendation': comparison['recommendation']}, indent=2) + '\n')
    (output / 'validation_summary.md').write_text(
        f'# V3 opportunity validation\n\nStatus: {state}\n\n'
        f'{comparison["reason"]}\n\n'
        f'Snapshots: {len(snapshots)}; episodes: {len(found)}; '
        f'journal attempts: {len(attempts or [])}; '
        f'cycle economics rows: {len(cycles or [])}. '
        f'Effective authorization cap coverage: '
        f'{comparison["input_quality"]["effective_authorization_cap_fraction"]}. '
        'Episodes already active at the first observable quote are left-censored; '
        'their paths start at first observation, and trigger_ts is null. '
        'Each episode uses one session, direction and quantity. The trigger '
        'reference is held fixed for descriptive path measurements. Horizon '
        'quotes are exact only at an observed timestamp; otherwise their '
        'bracketing interval or censoring is recorded. The 1s recorder heartbeat '
        'limits path continuity to gaps of at most 2s. Window extrema and '
        'reversion waits are observed bounds, not unobserved fills. '
        'Shadow replay allows at most one active paired cycle, respects the '
        'configured rolling request budget, and tracks zero/partial/UNKNOWN, '
        'repair, one-time fees and signed funding. Unfinished cycles retain '
        'known cash and a separate executable window-end mark; that mark is '
        'never counted as realized PnL. Calibration uses attempts and finalized '
        'funding that predate each replay interval. Training and validation are '
        'chronological. IOC outcomes remain scenario estimates because queue '
        'and counterfactual fill information is unavailable. '
        'No production parameter change is recommended.\n')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshots', type=Path, nargs='+', default=[],
                        help='continuous market capture files; omit for historical journal diagnostics only')
    parser.add_argument('--config', type=Path, default=Path('config.v3.json'))
    parser.add_argument('--journal', type=Path,
                        help='read-only copied V3 journal for attempt/funding calibration')
    parser.add_argument('--attempts', type=Path,
                        help='JSON export with attempts and cycles arrays')
    parser.add_argument('--instrument', type=Path,
                        help='JSON instrument metadata if snapshots lack it')
    parser.add_argument('--effective-max-notional-usd', type=str,
                        help='explicit read-only authorization cap; never inferred from Settings')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    settings = Settings.from_mapping(json.loads(args.config.read_text()))
    snapshots = load_snapshots(args.snapshots)
    found = episodes(snapshots, settings)
    attempts, cycles = (read_calibration(args.journal) if args.journal else ([], []))
    if args.attempts:
        exported = json.loads(args.attempts.read_text())
        attempts = exported['attempts']
        cycles = exported['cycles']
    instrument = json.loads(args.instrument.read_text()) if args.instrument else None
    if args.effective_max_notional_usd is not None:
        try:
            cap = dec(args.effective_max_notional_usd)
        except (ValueError, ArithmeticError):
            parser.error('--effective-max-notional-usd must be a finite number')
        if cap <= 0:
            parser.error('--effective-max-notional-usd must be positive')
        if instrument is not None:
            instrument['effective_max_notional_usd'] = str(cap)
        else:
            for row in snapshots:
                row['instrument'] = {**(row.get('instrument') or {}),
                                     'effective_max_notional_usd': str(cap)}
    write_report(args.output, snapshots, found, settings, attempts=attempts,
                 cycles=cycles, instrument_override=instrument)
    comparison = json.loads((args.output / 'parameter_comparison.json').read_text())
    print(json.dumps({'status': comparison['status'], 'episodes': len(found),
                      'experiments': len(comparison['experiments'])}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
