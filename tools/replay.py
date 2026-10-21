#!/usr/bin/env python3
"""One-second top-of-book event study and chronological strategy replay.

This is deliberately not an exact fill simulator: seconds.csv contains only
top-of-book price/size. Results are shown under explicit per-leg slippage
stress scenarios and must not be treated as a fill guarantee.
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.config import Config, load_config  # noqa: E402
from entropy_arb.cycle_policy import evaluate_cycle_exit, exit_net_pnl
from entropy_arb.funding import expected_pair_funding_cost_bps  # noqa: E402
from entropy_arb.signal import (InventoryCycle, PairState, RollingPremiumModel,
                                MidlineLagState, PremiumDataHealth,
                                PremiumSampleQuality,
                                compute_adaptive_entry_band,
                                compute_cycle_midline_drift,
                                cycle_direction_for_pair_state,
                                entry_threshold_long, entry_threshold_short,
                                exit_threshold_long, exit_threshold_short,
                                is_cycle_add_blocked,
                                is_volatility_guarded, pair_state)  # noqa: E402

HORIZONS = (300, 900, 3600, 14400)


@dataclass
class Row:
    ts: float
    e_bid: float
    e_ask: float
    e_bid_size: float
    e_ask_size: float
    h_bid: float
    h_ask: float
    h_bid_size: float
    h_ask_size: float
    premium: float
    sell_edge: float
    buy_edge: float
    e_funding: Optional[float]
    h_funding: Optional[float]
    premium_sample_valid: bool = True
    premium_sample_reject_reason: Optional[str] = None
    entropy_book_age_ms: Optional[float] = None
    hedge_book_age_ms: Optional[float] = None
    cross_venue_skew_ms: Optional[float] = None


def _float(row: dict, key: str, optional: bool = False) -> Optional[float]:
    value = row.get(key, "")
    if optional and value in (None, ""):
        return None
    return float(value)


def _bool(row: dict, key: str, default: bool = True) -> bool:
    value = row.get(key, "")
    if value in (None, ""):
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "invalid"}


def load_rows(path: str, hours: float = 0.0, now: Optional[float] = None) -> list[Row]:
    cutoff = ((time.time() if now is None else now) - hours * 3600
              if hours > 0 else -math.inf)
    rows = []
    with open(path, newline="") as fh:
        for raw in csv.DictReader(fh):
            try:
                ts = float(raw["ts"])
                if ts < cutoff:
                    continue
                rows.append(Row(
                    ts, _float(raw, "entropy_bid"), _float(raw, "entropy_ask"),
                    _float(raw, "entropy_bid_size"),
                    _float(raw, "entropy_ask_size"),
                    _float(raw, "hedge_bid"), _float(raw, "hedge_ask"),
                    _float(raw, "hedge_bid_size"),
                    _float(raw, "hedge_ask_size"),
                    _float(raw, "premium_mid_bps"),
                    _float(raw, "sell_edge_bps"),
                    _float(raw, "buy_edge_bps"),
                    _float(raw, "entropy_funding_bps_h", True),
                    _float(raw, "hedge_funding_bps_h", True),
                    _bool(raw, "premium_sample_valid", True),
                    (str(raw.get("premium_sample_reject_reason") or "")
                     or None),
                    _float(raw, "entropy_book_age_ms", True),
                    _float(raw, "hedge_book_age_ms", True),
                    _float(raw, "cross_venue_skew_ms", True)))
            except (KeyError, TypeError, ValueError):
                continue
    rows.sort(key=lambda r: r.ts)
    return rows


def make_model(cfg: Config, *, long_window_sec: Optional[float] = None,
               base_move_bps_per_10m: Optional[float] = None,
               sigma_window_sec: Optional[float] = None) -> RollingPremiumModel:
    return RollingPremiumModel(
        short_window_sec=cfg.dynamic_short_window_sec,
        long_window_sec=(cfg.dynamic_long_window_sec
                         if long_window_sec is None else long_window_sec),
        warmup_sec=cfg.dynamic_warmup_sec,
        min_samples=cfg.dynamic_min_samples,
        shift_min_bps=cfg.dynamic_shift_min_bps,
        shift_sigma_mult=cfg.dynamic_shift_sigma_mult,
        recovery_ratio=cfg.dynamic_recovery_ratio,
        recovery_sec=cfg.dynamic_recovery_sec,
        seed_midline_bps=cfg.midline_bps,
        max_move_bps_per_10m=cfg.dynamic_max_move_bps_per_10m,
        base_move_bps_per_10m=(
            cfg.dynamic_base_move_bps_per_10m
            if base_move_bps_per_10m is None else base_move_bps_per_10m),
        accelerated_move_bps_per_10m=(
            cfg.dynamic_accelerated_move_bps_per_10m),
        absolute_max_move_bps_per_10m=(
            cfg.dynamic_absolute_max_move_bps_per_10m),
        sigma_window_sec=(cfg.adaptive_bands.sigma_window_sec
                          if sigma_window_sec is None else sigma_window_sec),
        midline_lag_guard_enabled=cfg.midline_lag_guard.enabled,
        midline_lag_warn_bps=cfg.midline_lag_guard.warn_bps,
        midline_lag_block_bps=cfg.midline_lag_guard.block_bps,
        midline_lag_persist_sec=cfg.midline_lag_guard.persist_sec,
        midline_lag_accelerate_bps=cfg.midline_lag_guard.accelerate_bps,
        midline_lag_accelerate_persist_sec=(
            cfg.midline_lag_guard.accelerate_persist_sec),
        midline_lag_recovery_sec=cfg.midline_lag_guard.recovery_sec,
        midline_lag_recovery_bps=cfg.midline_lag_guard.recovery_bps)


def signal_history(rows: list[Row], cfg: Config):
    return [(context["midline_bps"], context["regime_shift"])
            for context in signal_contexts(rows, cfg)]


def _row_quality(row: Row, cfg: Config,
                  max_cross_venue_skew_ms: Optional[float] = None
                  ) -> PremiumSampleQuality:
    """Reconstruct recorded sample quality, allowing skew scenario overrides."""
    max_skew = (cfg.premium_model.max_cross_venue_skew_ms
                if max_cross_venue_skew_ms is None
                else max_cross_venue_skew_ms)
    ages_present = any(value is not None for value in (
        row.entropy_book_age_ms, row.hedge_book_age_ms,
        row.cross_venue_skew_ms))
    if not ages_present:
        return PremiumSampleQuality(
            row.premium_sample_valid, None, None, None,
            None if row.premium_sample_valid else row.premium_sample_reject_reason)

    entropy_age = row.entropy_book_age_ms
    hedge_age = row.hedge_book_age_ms
    skew = row.cross_venue_skew_ms
    if entropy_age is None:
        return PremiumSampleQuality(False, None, hedge_age, skew,
                                    "entropy_book_missing")
    if hedge_age is None:
        return PremiumSampleQuality(False, entropy_age, None, skew,
                                    "hedge_book_missing")
    if entropy_age > cfg.premium_model.max_book_age_ms:
        return PremiumSampleQuality(False, entropy_age, hedge_age, skew,
                                    "entropy_book_stale")
    if hedge_age > cfg.premium_model.max_book_age_ms:
        return PremiumSampleQuality(False, entropy_age, hedge_age, skew,
                                    "hedge_book_stale")
    if skew is not None and skew > max_skew:
        return PremiumSampleQuality(False, entropy_age, hedge_age, skew,
                                    "cross_venue_skew")
    if not row.premium_sample_valid:
        return PremiumSampleQuality(False, entropy_age, hedge_age, skew,
                                    row.premium_sample_reject_reason
                                    or "cross_venue_skew")
    return PremiumSampleQuality(True, entropy_age, hedge_age, skew, None)


def signal_contexts(rows: list[Row], cfg: Config, *,
                    adaptive_entry_sigma_mult: Optional[float] = None,
                    long_window_sec: Optional[float] = None,
                    base_move_bps_per_10m: Optional[float] = None,
                    sigma_window_sec: Optional[float] = None,
                    max_cross_venue_skew_ms: Optional[float] = None):
    """Yield the live signal context in chronological, no-lookahead order."""
    model = make_model(
        cfg, long_window_sec=long_window_sec,
        base_move_bps_per_10m=base_move_bps_per_10m,
        sigma_window_sec=sigma_window_sec)
    health = PremiumDataHealth(cfg.premium_model.degraded_after_sec)
    adaptive_enabled = (cfg.adaptive_bands.enabled
                         or adaptive_entry_sigma_mult is not None)
    sigma_mult = (cfg.adaptive_bands.entry_sigma_mult
                  if adaptive_entry_sigma_mult is None
                  else adaptive_entry_sigma_mult)
    for row in rows:
        quality = _row_quality(row, cfg, max_cross_venue_skew_ms)
        if cfg.dynamic_midline_enabled:
            health.observe(row.ts, quality)
        if cfg.dynamic_midline_enabled and quality.valid:
            model.observe(row.ts, row.premium)
        snap = model.snapshot(row.ts)
        mid = snap.active_midline_bps if cfg.dynamic_midline_enabled else cfg.midline_bps
        if adaptive_enabled:
            upper = compute_adaptive_entry_band(
                base_bps=cfg.upper_bps,
                configured_min_bps=cfg.adaptive_bands.min_upper_bps,
                robust_sigma_bps=snap.robust_sigma_bps,
                sigma_mult=sigma_mult,
                max_bps=cfg.adaptive_bands.max_upper_bps)
            lower = compute_adaptive_entry_band(
                base_bps=cfg.lower_bps,
                configured_min_bps=cfg.adaptive_bands.min_lower_bps,
                robust_sigma_bps=snap.robust_sigma_bps,
                sigma_mult=sigma_mult,
                max_bps=cfg.adaptive_bands.max_lower_bps)
            volatility_guard = is_volatility_guarded(
                snap.robust_sigma_bps, cfg.adaptive_bands.max_sigma_bps)
        else:
            upper, lower, volatility_guard = cfg.upper_bps, cfg.lower_bps, False
        lag_state = snap.midline_lag_state
        if not isinstance(lag_state, MidlineLagState):
            try:
                lag_state = MidlineLagState(str(lag_state))
            except ValueError:
                lag_state = MidlineLagState.NORMAL
        yield {
            "snap": snap,
            "midline_bps": mid,
            "upper_bps": upper,
            "lower_bps": lower,
            "volatility_guard": volatility_guard,
            "regime_shift": snap.regime_shift,
            "premium_sample_valid": quality.valid,
            "premium_sample_reject_reason": quality.reason,
            "entropy_book_age_ms": quality.entropy_age_ms,
            "hedge_book_age_ms": quality.hedge_age_ms,
            "cross_venue_skew_ms": quality.cross_venue_skew_ms,
            "premium_data_degraded": (
                cfg.dynamic_midline_enabled and health.is_degraded(row.ts)),
            "midline_lag_bps": snap.midline_lag_bps,
            "midline_lag_state": lag_state.value,
            "midline_lag_blocked": lag_state == MidlineLagState.BLOCKED,
        }


def _percentile(values: list[float], q: float) -> Optional[float]:
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return values[lo] if lo == hi else values[lo] * (hi - k) + values[hi] * (k - lo)


def event_study(rows: list[Row], cfg: Config, upper: Optional[float] = None,
                lower: Optional[float] = None, exit_bps: Optional[float] = None,
                max_horizon: int = 14400) -> dict:
    """Retrospective convergence study; future inspection is confined here."""
    contexts = list(signal_contexts(rows, cfg))
    fees = cfg.entropy.fee_bps + cfg.hedge.fee_bps
    exit_bps = cfg.exit_bps if exit_bps is None else exit_bps
    events = []
    prev_short = prev_long = False
    for i, row in enumerate(rows):
        context = contexts[i]
        mid = context["midline_bps"]
        entry_upper = context["upper_bps"] if upper is None else upper
        entry_lower = context["lower_bps"] if lower is None else lower
        exposure_ok = (not context["regime_shift"]
                       and not context["volatility_guard"]
                       and not context["midline_lag_blocked"]
                       and not context["premium_data_degraded"])
        short_hit = exposure_ok and row.sell_edge - fees >= mid + entry_upper
        long_hit = exposure_ok and row.buy_edge - fees >= entry_lower - mid
        directions = []
        if short_hit and not prev_short:
            directions.append("short_entropy")
        if long_hit and not prev_long:
            directions.append("long_entropy")
        prev_short, prev_long = short_hit, long_hit
        for direction in directions:
            entry_premium = row.premium
            hit_ts = None
            adverse = favorable = 0.0
            for j in range(i + 1, len(rows)):
                future = rows[j]
                if future.ts - row.ts > max_horizon:
                    break
                if direction == "short_entropy":
                    adverse = max(adverse, future.premium - entry_premium)
                    favorable = max(favorable, entry_premium - future.premium)
                    exited = (future.buy_edge - fees >=
                              exit_threshold_short(mid, exit_bps))
                else:
                    adverse = max(adverse, entry_premium - future.premium)
                    favorable = max(favorable, future.premium - entry_premium)
                    exited = (future.sell_edge - fees >=
                              exit_threshold_long(mid, exit_bps))
                if exited:
                    hit_ts = future.ts
                    break
            events.append({"direction": direction, "entry_ts": row.ts,
                           "entry_midline": mid, "exit_ts": hit_ts,
                           "time_to_exit_sec": (None if hit_ts is None else
                                                hit_ts - row.ts),
                           "mae_bps": adverse, "mfe_bps": favorable})
    times = [e["time_to_exit_sec"] for e in events
             if e["time_to_exit_sec"] is not None]
    return {
        "events": events,
        "count": len(events),
        "convergence": {h: (sum(e["time_to_exit_sec"] is not None and
                                e["time_to_exit_sec"] <= h for e in events)
                              / len(events) if events else 0.0)
                        for h in HORIZONS},
        "holding_p50": _percentile(times, .5),
        "holding_p90": _percentile(times, .9),
        "mae_p50": _percentile([e["mae_bps"] for e in events], .5),
        "mae_p90": _percentile([e["mae_bps"] for e in events], .9),
        "mae_p99": _percentile([e["mae_bps"] for e in events], .99),
    }


def _funding_rates(row: Row, state: dict, cfg: Config):
    for key, value in (("e", row.e_funding), ("h", row.h_funding)):
        if value is not None:
            state[key] = (value, row.ts)
    e = state.get("e")
    h = state.get("h")
    er = e[0] if e and row.ts - e[1] <= cfg.funding_stale_sec else None
    hr = h[0] if h and row.ts - h[1] <= cfg.funding_stale_sec else None
    return er, hr


def simulate(rows: list[Row], cfg: Config, *, notional: float = 100.0,
             slippage_bps: float = 0.0,
             adaptive_entry_sigma_mult: Optional[float] = None,
             cycle_drift_limit: Optional[float] = None,
             long_window_sec: Optional[float] = None,
             base_move_bps_per_10m: Optional[float] = None,
             sigma_window_sec: Optional[float] = None,
             max_cross_venue_skew_ms: Optional[float] = None,
             execution_scenario: Optional["ExecutionScenario"] = None) -> dict:
    """Historical signal research; pass a scenario for chronological leg stress.

    None preserves the legacy API's ideal fills. The CLI defaults to stress.
    Neither mode claims full live-engine entry-policy equivalence.
    """
    if execution_scenario is not None:
        return simulate_execution(rows, cfg, notional=notional,
                                  slippage_bps=slippage_bps,
                                  scenario=execution_scenario,
                                  adaptive_entry_sigma_mult=adaptive_entry_sigma_mult,
                                  cycle_drift_limit=cycle_drift_limit,
                                  long_window_sec=long_window_sec,
                                  base_move_bps_per_10m=base_move_bps_per_10m,
                                  sigma_window_sec=sigma_window_sec,
                                  max_cross_venue_skew_ms=max_cross_venue_skew_ms)
    contexts = list(signal_contexts(
        rows, cfg, adaptive_entry_sigma_mult=adaptive_entry_sigma_mult,
        long_window_sec=long_window_sec,
        base_move_bps_per_10m=base_move_bps_per_10m,
        sigma_window_sec=sigma_window_sec,
        max_cross_venue_skew_ms=max_cross_venue_skew_ms))
    cycle_enabled = cfg.cycle_exit.enabled or cycle_drift_limit is not None
    cycle_limit = (cfg.cycle_exit.max_midline_drift_bps
                   if cycle_drift_limit is None else cycle_drift_limit)
    cycle = InventoryCycle()
    ep = hp = ec = hc = 0.0
    fees_usd = turnover = slip_cost = funding_pnl = 0.0
    opens = adds = reduces = rounds = 0
    blocked_regime = blocked_funding = blocked_slippage = 0
    blocked_volatility = blocked_cycle_drift = blocked_cycle_unanchored = 0
    blocked_opens_by_midline_lag = blocked_adds_by_midline_lag = 0
    blocked_data_degraded = 0
    max_inventory = max_net = guard_seconds = 0.0
    volatility_guard_seconds = unanchored_cycle_seconds = 0.0
    effective_upper_total = effective_lower_total = 0.0
    effective_band_samples = 0
    funding_known = funding_total = 0.0
    funding_state = {}
    cycle_start = None
    cycle_start_equity = None
    holding = []
    direction_stats = {"short_entropy": {"opens": 0, "adds": 0, "reduces": 0,
                                          "round_trips": 0, "net_pnl_usd": 0.0},
                       "long_entropy": {"opens": 0, "adds": 0, "reduces": 0,
                                         "round_trips": 0, "net_pnl_usd": 0.0}}
    equity_curve = []
    prev = None
    invalid_reasons = Counter()
    lag_values = []
    skew_values = []
    lag_state_seconds = Counter()
    for row, context in zip(rows, contexts):
        snap = context["snap"]
        mid = context["midline_bps"]
        upper = context["upper_bps"]
        lower = context["lower_bps"]
        volatility_guard = context["volatility_guard"]
        if not context["premium_sample_valid"]:
            invalid_reasons[context["premium_sample_reject_reason"]
                            or "unknown"] += 1
        if context["midline_lag_bps"] is not None:
            lag_values.append(abs(context["midline_lag_bps"]))
        if context["cross_venue_skew_ms"] is not None:
            skew_values.append(context["cross_venue_skew_ms"])
        lag_state_seconds[context["midline_lag_state"]] += (
            max(row.ts - prev.ts, 0.0) if prev is not None else 0.0)
        effective_upper_total += upper
        effective_lower_total += lower
        effective_band_samples += 1
        er, hr = _funding_rates(row, funding_state, cfg)
        if prev is not None:
            dt = max(row.ts - prev.ts, 0.0)
            funding_total += dt
            if er is not None and hr is not None:
                funding_known += dt
                cost = ((math.copysign(1, ep) * er * abs(ep * row.e_bid)
                         if ep else 0.0)
                        + (math.copysign(1, hp) * hr * abs(hp * row.h_bid)
                           if hp else 0.0)) * dt / 3600 / 1e4
                ec -= (math.copysign(1, ep) * er * abs(ep * row.e_bid)
                       if ep else 0.0) * dt / 3600 / 1e4
                hc -= (math.copysign(1, hp) * hr * abs(hp * row.h_bid)
                       if hp else 0.0) * dt / 3600 / 1e4
                funding_pnl -= cost
            if snap.regime_shift:
                guard_seconds += dt
            if volatility_guard:
                volatility_guard_seconds += dt
            if cycle.active and not cycle.anchored:
                unanchored_cycle_seconds += dt
        prev = row
        state = pair_state(ep, hp, cfg.net_tolerance_base)
        if cycle_enabled:
            if state == PairState.FLAT:
                if cfg.cycle_exit.reset_on_flat:
                    cycle.reset()
            elif state in (PairState.SHORT_ENTROPY, PairState.LONG_ENTROPY):
                expected = cycle_direction_for_pair_state(state)
                if not cycle.active or cycle.direction != expected:
                    cycle = InventoryCycle(active=True, anchored=False,
                                           direction=expected,
                                           peak_paired_qty=min(abs(ep), abs(hp)))
        cycle_entry_midline = (cycle.entry_midline_bps
                               if cycle_enabled and cycle.active
                               and cycle.anchored else None)
        cycle_drift = (compute_cycle_midline_drift(mid, cycle_entry_midline)
                       if cycle_entry_midline is not None else None)
        cycle_drift_guard = (cycle_enabled and cycle_drift is not None
                             and is_cycle_add_blocked(cycle_drift, cycle_limit))
        cycle_add_blocked = False
        if cycle_enabled and state in (PairState.SHORT_ENTROPY,
                                       PairState.LONG_ENTROPY):
            expected = cycle_direction_for_pair_state(state)
            cycle_add_blocked = (not cycle.active or cycle.direction != expected
                                 or not cycle.anchored
                                 or cycle_drift_guard)
        candidates = []
        if state == PairState.SHORT_ENTROPY:
            reduce_mid = (cycle_entry_midline
                          if cycle.direction == "short_entropy"
                          and cycle_entry_midline is not None else mid)
            candidates.append(("short_entropy", "reduce", "e", "h",
                               exit_threshold_short(reduce_mid, cfg.exit_bps),
                               min(abs(ep), abs(hp))))
        elif state == PairState.LONG_ENTROPY:
            reduce_mid = (cycle_entry_midline
                          if cycle.direction == "long_entropy"
                          and cycle_entry_midline is not None else mid)
            candidates.append(("long_entropy", "reduce", "h", "e",
                               exit_threshold_long(reduce_mid, cfg.exit_bps),
                               min(abs(ep), abs(hp))))

        total_fees = cfg.entropy.fee_bps + cfg.hedge.fee_bps
        short_signal = (state in (PairState.FLAT, PairState.SHORT_ENTROPY)
                        and row.sell_edge - total_fees >= mid + upper)
        long_signal = (state in (PairState.FLAT, PairState.LONG_ENTROPY)
                       and row.buy_edge - total_fees >= lower - mid)
        if volatility_guard:
            blocked_volatility += int(short_signal) + int(long_signal)
        if context["midline_lag_blocked"]:
            if state == PairState.FLAT:
                blocked_opens_by_midline_lag += int(short_signal) + int(long_signal)
            else:
                blocked_adds_by_midline_lag += int(short_signal) + int(long_signal)
        if context["premium_data_degraded"]:
            blocked_data_degraded += int(short_signal) + int(long_signal)
        if cycle_drift_guard and state != PairState.FLAT:
            blocked_cycle_drift += int(short_signal) + int(long_signal)
        if cycle_enabled and cycle_add_blocked and state != PairState.FLAT:
            blocked_cycle_unanchored += int(
                (short_signal or long_signal) and not cycle_drift_guard)
        can_expose = (not snap.regime_shift and not volatility_guard
                      and not context["midline_lag_blocked"]
                      and not context["premium_data_degraded"])
        short_cost = long_cost = 0.0
        if cfg.funding_enabled:
            if er is None or hr is None:
                if cfg.funding_on_unavailable == "block_open":
                    can_expose = False
                    blocked_funding += int(short_signal) + int(long_signal)
            else:
                short_cost = expected_pair_funding_cost_bps(
                    er, hr, cfg.funding_expected_hold_hours, short_entropy=True)
                long_cost = -short_cost
        if snap.regime_shift:
            blocked_regime += int(short_signal) + int(long_signal)
        elif cfg.funding_enabled and er is not None and hr is not None:
            blocked_funding += int(short_signal and
                                   short_cost > cfg.funding_max_adverse_cost_bps)
            blocked_funding += int(long_signal and
                                   long_cost > cfg.funding_max_adverse_cost_bps)
        if (can_expose
                and state in (PairState.FLAT, PairState.SHORT_ENTROPY)
                and short_cost <= cfg.funding_max_adverse_cost_bps
                and (state == PairState.FLAT or not cycle_add_blocked)):
            candidates.append(("short_entropy",
                               "open" if state == PairState.FLAT else "add",
                               "h", "e", entry_threshold_short(
                                   mid, upper, 0, short_cost), math.inf))
        if (can_expose
                and state in (PairState.FLAT, PairState.LONG_ENTROPY)
                and long_cost <= cfg.funding_max_adverse_cost_bps
                and (state == PairState.FLAT or not cycle_add_blocked)):
            candidates.append(("long_entropy",
                               "open" if state == PairState.FLAT else "add",
                               "e", "h", entry_threshold_long(
                                   mid, lower, 0, long_cost), math.inf))

        for direction, action, buy_key, sell_key, threshold, reduce_qty in candidates:
            buy_px = row.e_ask if buy_key == "e" else row.h_ask
            sell_px = row.e_bid if sell_key == "e" else row.h_bid
            buy_size = row.e_ask_size if buy_key == "e" else row.h_ask_size
            sell_size = row.e_bid_size if sell_key == "e" else row.h_bid_size
            net_edge = (sell_px * (1 - (cfg.entropy.fee_bps if sell_key == "e"
                                        else cfg.hedge.fee_bps) / 1e4)
                        / (buy_px * (1 + (cfg.entropy.fee_bps if buy_key == "e"
                                          else cfg.hedge.fee_bps) / 1e4)) - 1) * 1e4
            if net_edge < threshold:
                continue
            surplus = net_edge - threshold
            if surplus < cfg.min_edge_surplus_bps:
                blocked_slippage += 1
                continue
            qty = min(notional / buy_px, buy_size, sell_size,
                      cfg.max_order_notional / buy_px, reduce_qty)
            if action != "reduce":
                qty = min(qty, max((cfg.entropy.cap_usd - abs(ep) * row.e_ask)
                                   / row.e_ask, 0),
                          max((cfg.hedge.cap_usd - abs(hp) * row.h_ask)
                              / row.h_ask, 0))
            if qty <= 0 or qty * buy_px < cfg.min_order_notional:
                continue
            slip = slippage_bps / 1e4
            bfill, sfill = buy_px * (1 + slip), sell_px * (1 - slip)
            buy_fee = (cfg.entropy.fee_bps if buy_key == "e"
                       else cfg.hedge.fee_bps) / 1e4
            sell_fee = (cfg.entropy.fee_bps if sell_key == "e"
                        else cfg.hedge.fee_bps) / 1e4
            pre_trade_equity = (ec + hc
                                + ep * ((row.e_bid + row.e_ask) / 2)
                                + hp * ((row.h_bid + row.h_ask) / 2))
            if buy_key == "e":
                ep += qty; ec -= qty * bfill * (1 + buy_fee)
            else:
                hp += qty; hc -= qty * bfill * (1 + buy_fee)
            if sell_key == "e":
                ep -= qty; ec += qty * sfill * (1 - sell_fee)
            else:
                hp -= qty; hc += qty * sfill * (1 - sell_fee)
            trade_turnover = qty * (bfill + sfill)
            turnover += trade_turnover
            fees_usd += qty * (bfill * buy_fee + sfill * sell_fee)
            slip_cost += qty * (buy_px + sell_px) * slip
            direction_stats[direction][action + "s"] += 1
            if action == "open":
                opens += 1
                cycle_start = row.ts
                cycle_start_equity = pre_trade_equity
            elif action == "add":
                adds += 1
            else:
                reduces += 1
            new_state = pair_state(ep, hp, cfg.net_tolerance_base)
            if cycle_enabled:
                if action == "open":
                    direction_name = cycle_direction_for_pair_state(new_state)
                    if direction_name is not None:
                        cycle.start(
                            direction=direction_name,
                            opened_ts=row.ts,
                            entry_midline_bps=mid,
                            entry_sigma_bps=snap.robust_sigma_bps,
                            entry_band_bps=(upper if direction_name == "short_entropy"
                                            else lower),
                            entry_threshold_bps=threshold,
                            paired_qty=min(abs(ep), abs(hp)))
                elif action == "add":
                    if cycle.active and cycle.anchored:
                        cycle.add_count += 1
                        cycle.peak_paired_qty = max(
                            cycle.peak_paired_qty, min(abs(ep), abs(hp)))
                elif action == "reduce" and cycle.active:
                    cycle.reduce_count += 1
            if action == "reduce" and new_state == PairState.FLAT:
                ep = hp = 0.0
                rounds += 1
                direction_stats[direction]["round_trips"] += 1
                if cycle_start is not None:
                    holding.append(row.ts - cycle_start)
                if cycle_start_equity is not None:
                    direction_stats[direction]["net_pnl_usd"] += (
                        ec + hc - cycle_start_equity)
                cycle_start = cycle_start_equity = None
                if cycle_enabled and cfg.cycle_exit.reset_on_flat:
                    cycle.reset()
            break  # at most one action per second; REDUCE is listed first

        equity = ec + hc + ep * ((row.e_bid + row.e_ask) / 2) \
            + hp * ((row.h_bid + row.h_ask) / 2)
        equity_curve.append(equity)
        max_inventory = max(max_inventory, abs(ep * row.e_bid), abs(hp * row.h_bid))
        max_net = max(max_net, abs(ep + hp))
    final_equity = equity_curve[-1] if equity_curve else 0.0
    peak = -math.inf
    drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        drawdown = max(drawdown, peak - value)
    gross = final_equity + fees_usd - funding_pnl + slip_cost
    return {
        "mode": "legacy_ideal_fills",
        "completed_net_pnl_usd": sum(x["net_pnl_usd"] for x in direction_stats.values()),
        "open_mtm_pnl_usd": final_equity - sum(x["net_pnl_usd"] for x in direction_stats.values()),
        "total_mtm_pnl_usd": final_equity,
        "samples": len(rows),
        "period_sec": (rows[-1].ts - rows[0].ts if len(rows) > 1 else 0.0),
        "data_coverage_pct": (100 * len(rows) /
                              (rows[-1].ts - rows[0].ts + 1)
                              if rows else 0.0),
        "opens": opens, "adds": adds,
        "reduces": reduces, "round_trips": rounds, "turnover_usd": turnover,
        "gross_spread_pnl_usd": gross, "fees_usd": fees_usd,
        "funding_pnl_usd": funding_pnl, "slippage_cost_usd": slip_cost,
        "net_pnl_usd": final_equity,
        "net_bps_per_turnover": (final_equity / turnover * 1e4
                                  if turnover else 0.0),
        "holding_avg": statistics.mean(holding) if holding else None,
        "holding_p50": _percentile(holding, .5),
        "holding_p90": _percentile(holding, .9),
        "holding_p95": _percentile(holding, .95),
        "max_inventory_usd": max_inventory, "max_net_delta": max_net,
        "regime_guard_seconds": guard_seconds,
        "time_in_volatility_guard": volatility_guard_seconds,
        "unanchored_cycle_seconds": unanchored_cycle_seconds,
        "avg_effective_upper_bps": (effective_upper_total / effective_band_samples
                                     if effective_band_samples else 0.0),
        "avg_effective_lower_bps": (effective_lower_total / effective_band_samples
                                     if effective_band_samples else 0.0),
        "entries_blocked_by_regime": blocked_regime,
        "entries_blocked_by_funding": blocked_funding,
        "entries_blocked_by_slippage_budget": blocked_slippage,
        "entries_blocked_by_volatility": blocked_volatility,
        "opens_blocked_by_midline_lag": blocked_opens_by_midline_lag,
        "adds_blocked_by_midline_lag": blocked_adds_by_midline_lag,
        "entries_blocked_by_premium_data_degraded": blocked_data_degraded,
        "adds_blocked_by_cycle_drift": blocked_cycle_drift,
        "adds_blocked_by_cycle_unanchored": blocked_cycle_unanchored,
        "funding_coverage_pct": (100 * funding_known / funding_total
                                  if funding_total else 0.0),
        "max_drawdown_usd": drawdown,
        "invalid_premium_samples": sum(invalid_reasons.values()),
        "invalid_sample_ratio": (sum(invalid_reasons.values()) / len(rows)
                                  if rows else 0.0),
        "samples_rejected_by_stale_entropy": invalid_reasons["entropy_book_stale"],
        "samples_rejected_by_stale_hedge": invalid_reasons["hedge_book_stale"],
        "samples_rejected_by_cross_venue_skew": invalid_reasons["cross_venue_skew"],
        "time_midline_lag_warning": lag_state_seconds[MidlineLagState.WARNING.value],
        "time_midline_lag_accelerating": lag_state_seconds[MidlineLagState.ACCELERATING.value],
        "time_midline_lag_blocked": lag_state_seconds[MidlineLagState.BLOCKED.value],
        "avg_abs_midline_lag_bps": (statistics.mean(lag_values)
                                     if lag_values else None),
        "p90_abs_midline_lag_bps": _percentile(lag_values, .9),
        "p99_abs_midline_lag_bps": _percentile(lag_values, .99),
        "avg_cross_venue_skew_ms": (statistics.mean(skew_values)
                                     if skew_values else None),
        "p90_cross_venue_skew_ms": _percentile(skew_values, .9),
        "p99_cross_venue_skew_ms": _percentile(skew_values, .99),
        "direction_stats": direction_stats}


@dataclass(frozen=True)
class ExecutionScenario:
    """Deterministic stresses, rounded UP to the next available data row.

    Zero delay still requires a later row. One-second snapshots cannot resolve
    subsecond fills, queue priority, depth or actual venue recovery outcomes.
    """
    first_delay_sec: float = 1.0
    hedge_delay_sec: float = 1.0
    recovery_delay_sec: float = 1.0
    first_fill_fraction: float = 1.0
    hedge_fill_fraction: float = 1.0
    zero_fill_every: int = 0
    orders_per_minute: int = 12
    max_adverse_bps: float = 10.0

    def __post_init__(self):
        for name in ("first_delay_sec", "hedge_delay_sec", "recovery_delay_sec",
                     "max_adverse_bps"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(name + " must be finite and nonnegative")
        for name in ("first_fill_fraction", "hedge_fill_fraction"):
            if not 0 <= getattr(self, name) <= 1:
                raise ValueError(name + " must be between zero and one")
        if self.zero_fill_every < 0 or self.orders_per_minute < 1:
            raise ValueError("invalid order count or budget")


def simulate_execution(rows, cfg, *, notional=100.0, slippage_bps=0.0,
                       scenario=None, cycle_drift_limit=None, **signal_options):
    """Execution stress of legacy signal candidates; not live-engine parity.

    Only realized-cost exit decisions share the live pure policy. This small
    single-cycle harness deliberately excludes ADD and does not estimate EV.
    IOC retries, budget waits and unfilled tail exposure remain in the result.
    """
    scenario = scenario or ExecutionScenario()
    if not math.isfinite(notional) or notional <= 0 or not math.isfinite(slippage_bps) or slippage_bps < 0:
        raise ValueError("notional must be positive and slippage nonnegative")
    positions = {"e": 0.0, "h": 0.0}
    cash = fees = turnover = funding = 0.0
    pending = None
    cycle_start = None
    start_cash = 0.0
    direction = None
    entry_mid = 0.0
    adverse_started = None
    sends = []
    events = []
    completed = []
    opens = reduces = attempts = zeros = partials = budget_waits = 0
    max_net = max_inventory = unhedged_sec = recovery_sec = 0.0
    previous = None
    funding_state = {}
    equity_curve = [0.0]

    def mark(row, liquidate=False):
        if liquidate:
            return exit_net_pnl(cashflow_usd=cash, entropy_qty=positions["e"],
                hedge_qty=positions["h"], entropy_bid=row.e_bid,
                entropy_ask=row.e_ask, hedge_bid=row.h_bid, hedge_ask=row.h_ask,
                entropy_fee_bps=cfg.entropy.fee_bps,
                hedge_fee_bps=cfg.hedge.fee_bps, slippage_bps=slippage_bps)
        return cash + positions["e"] * (row.e_bid + row.e_ask) / 2 + positions["h"] * (row.h_bid + row.h_ask) / 2

    def order(row, index, key, sign, qty, action, phase, delay):
        quote = getattr(row, key + ("_ask" if sign > 0 else "_bid"))
        return dict(key=key, sign=sign, qty=qty, action=action, phase=phase,
                    due=row.ts + delay, index=index,
                    limit=quote * (1 + sign * scenario.max_adverse_bps / 1e4))

    for index, (row, context) in enumerate(zip(rows, signal_contexts(rows, cfg, **signal_options))):
        er, hr = _funding_rates(row, funding_state, cfg)
        if previous is not None:
            dt = max(row.ts - previous.ts, 0.0)
            if abs(positions["e"] + positions["h"]) > 1e-12:
                unhedged_sec += dt
            if pending and pending.get("recovery"):
                recovery_sec += dt
            if cfg.funding_enabled and er is not None and hr is not None:
                cost = (positions["e"] * row.e_bid * er + positions["h"] * row.h_bid * hr) * dt / 3600 / 1e4
                cash -= cost
                funding -= cost
        previous = row
        sends = [ts for ts in sends if row.ts - ts < 60]
        if pending is not None and index > pending["index"] and row.ts >= pending["due"]:
            if len(sends) >= scenario.orders_per_minute:
                budget_waits += 1
            else:
                o = pending
                sends.append(row.ts)
                attempts += 1
                key, sign = o["key"], o["sign"]
                quote = getattr(row, key + ("_ask" if sign > 0 else "_bid"))
                size = getattr(row, key + ("_ask_size" if sign > 0 else "_bid_size"))
                px = quote * (1 + sign * slippage_bps / 1e4)
                fraction = scenario.first_fill_fraction if o["phase"] == "first" else scenario.hedge_fill_fraction
                qty = min(o["qty"] * fraction, max(size, 0.0))
                if not context["premium_sample_valid"] or sign * (px - o["limit"]) > 1e-10 or (scenario.zero_fill_every and attempts % scenario.zero_fill_every == 0):
                    qty = 0.0
                if qty <= 1e-12:
                    qty = 0.0
                    zeros += 1
                elif qty < o["qty"] - 1e-12:
                    partials += 1
                fee = qty * px * (cfg.entropy.fee_bps if key == "e" else cfg.hedge.fee_bps) / 1e4
                if qty and cycle_start is None:
                    cycle_start, start_cash = row.ts, cash
                    opens += 1
                positions[key] += sign * qty
                cash -= sign * qty * px + fee
                fees += fee
                turnover += qty * px
                events.append(dict(ts=row.ts, venue=key, phase=o["phase"], action=o["action"], qty=qty, price=px))
                if o["phase"] == "first":
                    pending = (order(row, index, "h", -sign, qty, o["action"], "hedge", scenario.hedge_delay_sec) if qty else None)
                else:
                    remaining = o["qty"] - qty
                    pending = (order(row, index, key, sign, remaining, o["action"], "hedge", scenario.recovery_delay_sec) if remaining > 1e-12 else None)
                    if pending is not None:
                        pending["recovery"] = True
                    if pending is None and o["action"] == "reduce":
                        reduces += 1
                if cycle_start is not None and pending is None and max(abs(v) for v in positions.values()) <= 1e-12:
                    completed.append(dict(opened_ts=cycle_start, closed_ts=row.ts, net_pnl_usd=cash - start_cash, direction=direction))
                    cycle_start = None
                    direction = None
        # Never submit another first leg until the hedge is fully resolved.
        if pending is None:
            action = None
            if cycle_start is None:
                if context["premium_sample_valid"] and not context["premium_data_degraded"]:
                    fees_bps = cfg.entropy.fee_bps + cfg.hedge.fee_bps
                    if (cfg.allow_sell_entropy_open
                            and row.sell_edge - fees_bps >= context["midline_bps"] + context["upper_bps"] + cfg.min_edge_surplus_bps):
                        direction, sign, action = "short_entropy", -1, "open"
                    elif (cfg.allow_buy_entropy_open
                          and row.buy_edge - fees_bps >= context["lower_bps"] - context["midline_bps"] + cfg.min_edge_surplus_bps):
                        direction, sign, action = "long_entropy", 1, "open"
                if action:
                    entry_mid = context["midline_bps"]
                    adverse_started = None
                    qty = min(notional, cfg.max_order_notional, cfg.entropy.cap_usd, cfg.hedge.cap_usd) / max(row.e_ask, row.h_ask)
                    if qty * min(row.e_bid, row.h_bid) < cfg.min_order_notional:
                        action = None
            else:
                sign = 1 if positions["e"] < 0 else -1
                qty = abs(positions["e"])
                if cfg.cycle_exit.mode == "fill_pnl":
                    drift = context["midline_bps"] - entry_mid
                    adverse = max(drift if direction == "short_entropy" else -drift, 0.0)
                    if adverse >= cfg.cycle_exit.adverse_drift_bps:
                        if adverse_started is None:
                            adverse_started = row.ts
                    else:
                        adverse_started = None
                    policy = evaluate_cycle_exit(cfg.cycle_exit, net_pnl_usd=mark(row, True) - start_cash,
                        notional_usd=max(qty * row.e_ask, 1e-12), held_sec=row.ts - cycle_start,
                        adverse_drift_bps=adverse,
                        adverse_persist_sec=row.ts - adverse_started if adverse_started is not None else 0.0,
                        regime_shift=bool(context["snap"].regime_shift),
                        regime_warning=context["midline_lag_state"] in {
                            MidlineLagState.WARNING.value, MidlineLagState.ACCELERATING.value,
                            MidlineLagState.BLOCKED.value})
                    close = policy["allowed"]
                else:
                    edge = row.buy_edge if sign > 0 else row.sell_edge
                    threshold = exit_threshold_short(entry_mid, cfg.exit_bps) if sign > 0 else exit_threshold_long(entry_mid, cfg.exit_bps)
                    close = edge - cfg.entropy.fee_bps - cfg.hedge.fee_bps >= threshold + cfg.min_edge_surplus_bps
                if close and qty > 1e-12:
                    action = "reduce"
            if action:
                pending = order(row, index, "e", sign, qty, action, "first", scenario.first_delay_sec)
        max_net = max(max_net, abs(positions["e"] + positions["h"]))
        max_inventory = max(max_inventory, abs(positions["e"] * row.e_bid), abs(positions["h"] * row.h_bid))
        equity_curve.append(mark(row))
    completed_net = sum(c["net_pnl_usd"] for c in completed)
    final_mtm = mark(rows[-1]) if rows else 0.0
    final_liquidation = mark(rows[-1], True) if rows else 0.0
    peak = equity_curve[0]
    drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        drawdown = max(drawdown, peak - value)
    holding = [c["closed_ts"] - c["opened_ts"] for c in completed]
    period = rows[-1].ts - rows[0].ts if len(rows) > 1 else 0.0
    return dict(mode="legacy_signal_execution_stress", samples=len(rows), period_sec=period,
        opens=opens, adds=0, reduces=reduces, round_trips=len(completed),
        completed_net_pnl_usd=completed_net, net_pnl_usd=completed_net,
        open_mtm_pnl_usd=final_mtm-completed_net,
        open_liquidation_pnl_usd=final_liquidation-completed_net,
        total_mtm_pnl_usd=final_mtm, total_liquidation_pnl_usd=final_liquidation,
        completed_cycles_per_hour=len(completed) * 3600 / period if period else 0.0,
        fees_usd=fees, funding_pnl_usd=funding, turnover_usd=turnover,
        holding_p50=_percentile(holding, .5), holding_p90=_percentile(holding, .9),
        order_attempts=attempts, zero_fills=zeros, partial_fills=partials,
        budget_wait_rows=budget_waits, recovery_seconds=recovery_sec,
        unhedged_seconds=unhedged_sec, max_net_delta=max_net,
        max_inventory_usd=max_inventory, max_drawdown_usd=drawdown,
        entropy_position=positions["e"], hedge_position=positions["h"],
        pending_order=pending, completed_cycles=completed, execution_events=events)


def _duration(value: Optional[float]) -> str:
    return "—" if value is None else f"{value / 60:.1f}m"


def main() -> None:
    p = argparse.ArgumentParser(description="1-second top-of-book convergence "
                                            "study and strategy replay")
    p.add_argument("--legacy-ideal-fills", action="store_true",
                   help="historical same-row ideal fills; signal research only")
    p.add_argument("--first-delay-sec", type=float, default=1.0)
    p.add_argument("--hedge-delay-sec", type=float, default=1.0)
    p.add_argument("--recovery-delay-sec", type=float, default=1.0)
    p.add_argument("--first-fill-fraction", type=float, default=1.0)
    p.add_argument("--hedge-fill-fraction", type=float, default=1.0)
    p.add_argument("--zero-fill-every", type=int, default=0)
    p.add_argument("--orders-per-minute", type=int, default=12)
    p.add_argument("--csv", default="logs/seconds.csv")
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--hours", type=float, default=72)
    p.add_argument("--symbol", default="REPLAY")
    p.add_argument("--hedge", default="lighter",
                   choices=("lighter", "lighter-rh", "tradexyz"))
    p.add_argument("--notional", type=float, default=100)
    p.add_argument("--entry-bands", default="")
    p.add_argument("--exit-bps", default="")
    p.add_argument("--slippage-scenarios", default="0,0.5,1")
    p.add_argument("--adaptive-entry-sigma-mults", default="",
                   help="comma-separated adaptive entry sigma multipliers")
    p.add_argument("--cycle-drift-limits", default="",
                   help="comma-separated cycle midline drift limits in bps")
    p.add_argument("--long-windows", default="",
                   help="comma-separated dynamic midline long windows in seconds")
    p.add_argument("--move-speeds", default="",
                   help="comma-separated base midline move speeds in bps/10m")
    p.add_argument("--sigma-windows", default="",
                   help="comma-separated sigma windows in seconds")
    p.add_argument("--cross-venue-skews", default="",
                   help="comma-separated max cross-venue skews in milliseconds")
    args = p.parse_args()
    try:
        execution_scenario = (None if args.legacy_ideal_fills else ExecutionScenario(
            first_delay_sec=args.first_delay_sec, hedge_delay_sec=args.hedge_delay_sec,
            recovery_delay_sec=args.recovery_delay_sec,
            first_fill_fraction=args.first_fill_fraction,
            hedge_fill_fraction=args.hedge_fill_fraction,
            zero_fill_every=args.zero_fill_every, orders_per_minute=args.orders_per_minute))
    except ValueError as exc:
        p.error(str(exc))
    cfg = load_config(args.config, os.devnull, symbol=args.symbol,
                      hedge_venue=args.hedge)
    try:
        rows = load_rows(args.csv, args.hours)
    except FileNotFoundError:
        p.error(f"{args.csv} not found")
    if not rows:
        p.error("no usable rows")
    print(f"Top-of-book replay: {len(rows)} samples, "
          f"{(rows[-1].ts - rows[0].ts) / 3600:.2f}h (not an exact fill simulation)")
    bands = ([float(x) for x in args.entry_bands.split(",") if x]
             or [cfg.upper_bps])
    exits = ([float(x) for x in args.exit_bps.split(",") if x]
             or [cfg.exit_bps])
    for band in bands:
        for exit_band in exits:
            study = event_study(rows, cfg, upper=band, lower=band,
                                exit_bps=exit_band)
            conv = "  ".join(f"{h // 60}m={study['convergence'][h]:.1%}"
                              for h in HORIZONS)
            print(f"\nEvent study band={band:g} exit={exit_band:g}: "
                  f"events={study['count']}  {conv}")
            print(f"  time p50={_duration(study['holding_p50'])} "
                  f"p90={_duration(study['holding_p90'])}; "
                  f"MAE p50={study['mae_p50']} p90={study['mae_p90']} "
                  f"p99={study['mae_p99']}")
    for slip in [float(x) for x in args.slippage_scenarios.split(",") if x]:
        sigma_mults = ([float(x) for x in args.adaptive_entry_sigma_mults.split(",")
                        if x] if args.adaptive_entry_sigma_mults else [None])
        drift_limits = ([float(x) for x in args.cycle_drift_limits.split(",")
                         if x] if args.cycle_drift_limits else [None])
        long_windows = ([float(x) for x in args.long_windows.split(",") if x]
                        if args.long_windows else [None])
        move_speeds = ([float(x) for x in args.move_speeds.split(",") if x]
                       if args.move_speeds else [None])
        sigma_windows = ([float(x) for x in args.sigma_windows.split(",") if x]
                         if args.sigma_windows else [None])
        cross_skews = ([float(x) for x in args.cross_venue_skews.split(",") if x]
                       if args.cross_venue_skews else [None])
        for sigma_mult in sigma_mults:
            for drift_limit in drift_limits:
                for long_window in long_windows:
                    for move_speed in move_speeds:
                        for sigma_window in sigma_windows:
                            for cross_skew in cross_skews:
                                if execution_scenario is not None:
                                    from tools.replay_engine import simulate_engine
                                    runner = simulate_engine
                                else:
                                    runner = simulate
                                r = runner(
                                    rows, cfg, notional=args.notional,
                                    slippage_bps=slip,
                                    execution_scenario=execution_scenario,
                                    adaptive_entry_sigma_mult=sigma_mult,
                                    cycle_drift_limit=drift_limit,
                                    long_window_sec=long_window,
                                    base_move_bps_per_10m=move_speed,
                                    sigma_window_sec=sigma_window,
                                    max_cross_venue_skew_ms=cross_skew)
                                if execution_scenario is not None:
                                    print(f"\nExecution stress ({slip:g} bps/leg): chronological signal and recovery model; top-of-book fill assumptions")
                                    print(f"  completed cycles={r['round_trips']}; cycles/h={r['completed_cycles_per_hour']:.3f}; completed net=${r['completed_net_pnl_usd']:+.5f}")
                                    print(f"  open MTM=${r['open_mtm_pnl_usd']:+.5f}; open liquidation stress=${r['open_liquidation_pnl_usd']:+.5f}; total liquidation=${r['total_liquidation_pnl_usd']:+.5f}")
                                    print(f"  attempts={r['order_attempts']}; zero/partial={r['zero_fills']}/{r['partial_fills']}; unhedged={r['unhedged_seconds']:.1f}s; recovery={r['recovery_seconds']:.1f}s")
                                    print(f"  remaining e/h={r['entropy_position']:.8g}/{r['hedge_position']:.8g}; unfinished cycles={r['unfinished_cycles']}; dust={r['inventory_flatness']['has_dust']}; pending order={r['pending_order'] is not None}")
                                    continue
                                label_window = ("default" if long_window is None
                                                else f"{long_window:g}s")
                                label_move = ("default" if move_speed is None
                                              else f"{move_speed:g}bp/10m")
                                label_sigma_window = ("default" if sigma_window is None
                                                      else f"{sigma_window:g}s")
                                label_skew = ("default" if cross_skew is None
                                              else f"{cross_skew:g}ms")
                                label_sigma = ("default" if sigma_mult is None
                                               else f"{sigma_mult:g}")
                                label_drift = ("default" if drift_limit is None
                                               else f"{drift_limit:g}")
                                print(f"\nStrategy replay — {slip:g} bps/leg stress; "
                                      f"adaptive sigma={label_sigma}; cycle drift={label_drift} bps; "
                                      f"long={label_window}; move={label_move}; "
                                      f"sigma-window={label_sigma_window}; skew={label_skew}")
                                print(f"  period {r['period_sec'] / 3600:.2f}h; samples "
                                      f"{r['samples']}; data coverage {r['data_coverage_pct']:.1f}%")
                                print(f"  opens/adds/reduces/round trips: {r['opens']}/{r['adds']}/"
                                      f"{r['reduces']}/{r['round_trips']}  turnover ${r['turnover_usd']:.2f}")
                                print(f"  gross ${r['gross_spread_pnl_usd']:+.4f}  fees "
                                      f"-${r['fees_usd']:.4f}  funding ${r['funding_pnl_usd']:+.4f} "
                                      f"slippage -${r['slippage_cost_usd']:.4f}  "
                                      f"total MTM (includes open inventory) ${r['net_pnl_usd']:+.4f} "
                                      f"({r['net_bps_per_turnover']:+.3f} bps/turnover)")
                                print(f"  holding avg={_duration(r['holding_avg'])} "
                                      f"p50={_duration(r['holding_p50'])} p90={_duration(r['holding_p90'])} "
                                      f"p95={_duration(r['holding_p95'])}")
                                print(f"  max inventory ${r['max_inventory_usd']:.2f}; max net delta "
                                      f"{r['max_net_delta']:.6g}; max drawdown ${r['max_drawdown_usd']:.4f}")
                                print(f"  effective entry band avg short/long="
                                      f"{r['avg_effective_upper_bps']:.2f}/{r['avg_effective_lower_bps']:.2f} bps; "
                                      f"volatility guard {r['time_in_volatility_guard']:.0f}s; "
                                      f"unanchored cycle {r['unanchored_cycle_seconds']:.0f}s")
                                print(f"  regime guard {r['regime_guard_seconds']:.0f}s / blocks "
                                      f"regime={r['entries_blocked_by_regime']} "
                                      f"volatility={r['entries_blocked_by_volatility']} "
                                      f"lag(open/add)={r['opens_blocked_by_midline_lag']}/"
                                      f"{r['adds_blocked_by_midline_lag']} "
                                      f"cycle_drift={r['adds_blocked_by_cycle_drift']} "
                                      f"cycle_unanchored={r['adds_blocked_by_cycle_unanchored']} "
                                      f"funding={r['entries_blocked_by_funding']} "
                                      f"slippage={r['entries_blocked_by_slippage_budget']}; "
                                      f"funding coverage={r['funding_coverage_pct']:.1f}%")
                                print(f"  invalid premium {r['invalid_premium_samples']} "
                                      f"({r['invalid_sample_ratio']:.1%}); lag time "
                                      f"warning/accelerating/blocked="
                                      f"{r['time_midline_lag_warning']:.0f}/"
                                      f"{r['time_midline_lag_accelerating']:.0f}/"
                                      f"{r['time_midline_lag_blocked']:.0f}s; "
                                      f"lag p90/p99={r['p90_abs_midline_lag_bps']} / "
                                      f"{r['p99_abs_midline_lag_bps']} bps; "
                                      f"skew p90/p99={r['p90_cross_venue_skew_ms']} / "
                                      f"{r['p99_cross_venue_skew_ms']}ms")
                                for direction, stats in r["direction_stats"].items():
                                    print(f"  {direction}: {stats}")


if __name__ == "__main__":
    main()
