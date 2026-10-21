"""Read-only dashboard snapshots written by the trading process.

The writer is deliberately isolated from strategy execution: callers may log
and ignore any failure.  The API process only reads the atomically replaced
JSON file and never reaches into Engine memory.
"""
from __future__ import annotations

import csv
import json
import logging
import math
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from .funding import fresh_rate
from .signal import PairState, StrategyAction

log = logging.getLogger("dashboard-state")


class EquityPnlTracker:
    """Track source-backed PnL from the combined exchange account equity.

    The daily baseline is persisted so restarting the engine does not reset
    today's PnL.  Session PnL intentionally starts from the first complete
    equity poll of this process.
    """

    def __init__(self, path: str | os.PathLike[str], day_timezone: str) -> None:
        self.path = Path(path)
        self.tz = ZoneInfo(day_timezone)
        self.day: Optional[str] = None
        self.day_start_equity: Optional[float] = None
        self.day_started_ts: Optional[float] = None
        self.session_start_equity: Optional[float] = None
        self.session_started_ts: Optional[float] = None
        self.current_equity: Optional[float] = None
        self.updated_ts: Optional[float] = None
        self._load()

    def _load(self) -> None:
        try:
            with self.path.open(encoding="utf-8") as fh:
                data = json.load(fh)
            if data.get("schema_version") != 1:
                return
            day = data.get("day")
            equity = _finite(data.get("day_start_equity_usd"))
            started = _finite(data.get("day_started_ts"))
            if isinstance(day, str) and equity is not None and started is not None:
                self.day = day
                self.day_start_equity = equity
                self.day_started_ts = started
        except (FileNotFoundError, OSError, json.JSONDecodeError, AttributeError):
            return

    def _persist(self) -> None:
        write_dashboard_snapshot(self.path, {
            "schema_version": 1,
            "day": self.day,
            "day_start_equity_usd": self.day_start_equity,
            "day_started_ts": self.day_started_ts,
            "updated_ts": self.updated_ts,
        })

    def observe(self, equities: list[float], now: Optional[float] = None) -> None:
        """Accept one complete, same-cycle equity observation."""
        now = time.time() if now is None else now
        if not equities or any(_finite(value) is None for value in equities):
            return
        total = sum(float(value) for value in equities)
        local_day = datetime.fromtimestamp(now, tz=self.tz).date().isoformat()
        changed = False
        if self.day != local_day:
            self.day = local_day
            self.day_start_equity = total
            self.day_started_ts = now
            changed = True
        if self.session_start_equity is None:
            self.session_start_equity = total
            self.session_started_ts = now
        self.current_equity = total
        self.updated_ts = now
        if changed:
            self._persist()

    def snapshot(self) -> dict:
        day_pnl = (None if self.current_equity is None
                   or self.day_start_equity is None
                   else self.current_equity - self.day_start_equity)
        session_pnl = (None if self.current_equity is None
                       or self.session_start_equity is None
                       else self.current_equity - self.session_start_equity)
        complete = False
        if self.day_started_ts is not None:
            started = datetime.fromtimestamp(self.day_started_ts, tz=self.tz)
            complete = (started.hour == 0 and started.minute == 0)
        return {
            "net_pnl_usd": _finite(day_pnl),
            "session_pnl_usd": _finite(session_pnl),
            "day_start_equity_usd": _finite(self.day_start_equity),
            "session_start_equity_usd": _finite(self.session_start_equity),
            "current_equity_usd": _finite(self.current_equity),
            "day_started_ts": self.day_started_ts,
            "session_started_ts": self.session_started_ts,
            "coverage_complete": complete,
            "equity_updated_ts": self.updated_ts,
        }


def _finite(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _today_trade_summary(path: str, day_timezone: str,
                         now: float) -> tuple[float, int, Optional[dict]]:
    """Return reliable turnover/count plus the latest trade for local day."""
    tz = ZoneInfo(day_timezone)
    day = datetime.fromtimestamp(now, tz=tz).date()
    turnover = 0.0
    count = 0
    last: Optional[dict] = None
    try:
        with open(path, newline="") as fh:
            for row in csv.DictReader(fh):
                ts = _finite(row.get("ts"))
                if ts is None or datetime.fromtimestamp(ts, tz=tz).date() != day:
                    continue
                ok = str(row.get("ok", "")).strip().lower() in {
                    "1", "true", "yes", "success"}
                if ok:
                    buy = _finite(row.get("buy_notional")) or 0.0
                    sell = _finite(row.get("sell_notional")) or 0.0
                    turnover += abs(buy) + abs(sell)
                    count += 1
                last = {
                    "ts": ts,
                    "action": str(row.get("action") or "").lower() or None,
                    "direction": str(row.get("direction") or "").lower() or None,
                }
    except (FileNotFoundError, OSError, csv.Error):
        pass
    return turnover, count, last


def _equity_snapshot(engine, pnl: dict,
                     now: Optional[float] = None) -> dict:
    """Expose the per-venue equity baseline used by the running engine.

    The engine records each venue's first successful equity observation as
    ``start_equity``. Keeping the venue values in the shared snapshot lets the
    dashboard show both the combined change and which side produced it,
    without re-deriving account PnL in the browser.
    """
    venues = {}
    complete = bool(getattr(engine, "venues", {}))
    total_start = 0.0
    total_current = 0.0
    for key, venue in getattr(engine, "venues", {}).items():
        start = _finite(getattr(venue, "start_equity", None))
        current = _finite(getattr(venue, "equity", None))
        change = (current - start
                  if start is not None and current is not None else None)
        venues[key] = {
            "start_equity_usd": start,
            "current_equity_usd": current,
            "change_usd": _finite(change),
        }
        if start is None or current is None:
            complete = False
        else:
            total_start += start
            total_current += current

    now = time.time() if now is None else float(now)
    ledger = getattr(engine, "ledger", None)
    lifetime = (ledger.equity_lifetime(
        now=now,
        current_equity=(total_current if complete else None))
        if ledger is not None and hasattr(ledger, "equity_lifetime")
        else {
            "start_ts": None,
            "end_ts": now,
            "start_equity_usd": None,
            "current_equity_usd": None,
            "change_usd": None,
            "coverage_complete": False,
            "source": "unavailable",
            "sample_count": 0,
        })

    return {
        "start_ts": (_finite(pnl.get("session_started_ts"))
                     or _finite(getattr(engine, "start_ts", None))),
        "complete": complete,
        "start_equity_usd": (_finite(total_start) if complete else None),
        "current_equity_usd": (_finite(total_current) if complete else None),
        "change_usd": (_finite(total_current - total_start)
                       if complete else None),
        "venues": venues,
        "lifetime": lifetime,
    }


def build_dashboard_snapshot(engine, now: Optional[float] = None) -> dict:
    """Build the versioned JSON contract without mutating trading state."""
    now = time.time() if now is None else now
    cfg = engine.cfg
    entropy, hedge = engine.entropy, engine.hedge
    strategy = engine._dashboard_strategy_values(now)

    eb, ea = entropy.book.best_bid(), entropy.book.best_ask()
    hb, ha = hedge.book.best_bid(), hedge.book.best_ask()
    em, hm = entropy.book.mid(), hedge.book.mid()
    premium = ((em / hm - 1.0) * 1e4) if em and hm else None
    sell_edge = ((eb / ha - 1.0) * 1e4) if eb and ha else None
    buy_edge = ((hb / ea - 1.0) * 1e4) if hb and ea else None

    state = engine._pair_state()
    net_delta = float(entropy.position + hedge.position)
    mark = em or hm
    net_delta_usd = net_delta * mark if mark is not None else None
    regime_shift = bool(strategy["regime_shift"])

    er = fresh_rate(getattr(entropy, "funding", None), now,
                    cfg.funding_stale_sec)
    hr = fresh_rate(getattr(hedge, "funding", None), now,
                    cfg.funding_stale_sec)
    funding_available = er is not None and hr is not None
    funding_stale = cfg.funding_enabled and not funding_available

    open_reason = None
    if state == PairState.UNBALANCED:
        open_reason = "unbalanced"
    elif getattr(engine, "active_execution", None) is not None:
        open_reason = "execution_recovery"
    elif getattr(engine, "reconciliation", {}).get("status") in {
            "syncing", "recovering", "recovery_pending", "blocked", "error"}:
        open_reason = "reconciliation_pending"
    elif strategy["premium_data_degraded"]:
        open_reason = "premium_data_degraded"
    elif (cfg.dynamic_midline_enabled
          and not strategy.get("dynamic_midline_tradable",
                               strategy["dynamic_midline_ready"])):
        open_reason = "dynamic_midline_warmup"
    elif strategy.get("reference_valid") is False:
        open_reason = "reference_invalid"
    elif strategy.get("margin_open_blocked"):
        open_reason = "margin_risk"
    elif strategy.get("open_lag_blocked", strategy["midline_lag_blocked"]):
        open_reason = "reference_deviation"
    elif state == PairState.FLAT and strategy["reentry_guard"]["blocked"]:
        open_reason = "reentry_guard"
    elif strategy["volatility_guard"]:
        open_reason = "volatility_guard"
    elif strategy["cycle_drift_guard"]:
        open_reason = "cycle_midline_drift"
    elif strategy["cycle_add_blocked"] and state != PairState.FLAT:
        open_reason = "cycle_unanchored"
    elif funding_stale and cfg.funding_on_unavailable == "block_open":
        open_reason = "funding_stale"
    elif cfg.funding_enabled and funding_available and (
            strategy["short_entropy_cost_bps"]
            > cfg.funding_max_adverse_cost_bps
            and strategy["long_entropy_cost_bps"]
            > cfg.funding_max_adverse_cost_bps):
        open_reason = "funding_too_expensive"
    elif engine.halted:
        open_reason = "halted"

    # Profit-budget blockers are produced after signal selection, so they do
    # not appear in the structural checks above. Surface the most recent
    # opportunity-level reason while it is still fresh.
    if open_reason is None and state == PairState.FLAT:
        recent_checks = [value for value in (getattr(
            engine, "profit_checks", {}) or {}).values()
                         if isinstance(value, dict)]
        recent_checks.sort(key=lambda value: float(
            value.get("checked_ts") or 0.0), reverse=True)
        if recent_checks and now - float(
                recent_checks[0].get("checked_ts") or 0.0) <= 10.0:
            candidate_reason = str(recent_checks[0].get("reason") or "")
            if candidate_reason in {
                    "expected_pnl_negative", "expected_cycle_pnl_below_floor",
                    "economic_bound_not_marketable",
                    "open_execution_room_below_min",
                    "protected_open_edge_below_threshold"}:
                open_reason = candidate_reason

    hard_block_reasons = {
        "unbalanced", "execution_recovery", "reconciliation_pending",
        "premium_data_degraded", "dynamic_midline_warmup",
        "reference_invalid", "margin_risk", "funding_stale",
        "venue_degraded", "book_stale",
        "cross_skew", "order_budget", "halted",
    }
    soft_block_reasons = {
        "reference_deviation", "reentry_guard", "cycle_midline_drift",
        "cycle_unanchored", "funding_too_expensive", "no_executable_edge",
        "protected_edge", "opportunity_locked", "expected_pnl_negative",
        "expected_cycle_pnl_below_floor", "economic_bound_not_marketable",
        "open_execution_room_below_min",
        "protected_open_edge_below_threshold", "volatility_guard",
    }
    hard_blocked = open_reason in hard_block_reasons
    soft_restricted = (not hard_blocked and (
        strategy.get("reference_state", "normal") != "normal"
        or open_reason in soft_block_reasons))
    open_state = ("hard_blocked" if hard_blocked else
                  "soft_restricted" if soft_restricted else "allowed")
    open_block_class = ("hard" if hard_blocked else
                        "soft" if soft_restricted else None)

    pnl = engine.pnl_tracker.snapshot()
    equity = _equity_snapshot(engine, pnl, now)
    lifetime = equity.get("lifetime") or {}
    attribution = engine.ledger.attribution(
        now=now,
        day_start_ts=pnl.get("day_started_ts"),
        session_start_ts=pnl.get("session_started_ts"),
        lifetime_start_ts=lifetime.get("start_ts"),
        marks={"entropy": em, "hedge": hm,
               getattr(entropy, "key", "entropy"): em,
               getattr(hedge, "key", "hedge"): hm},
        equity_day=pnl.get("net_pnl_usd"),
        equity_session=pnl.get("session_pnl_usd"),
        equity_lifetime=lifetime.get("change_usd"))
    ledger_summary = engine.ledger.trade_summary(
        since=pnl.get("day_started_ts"), until=now,
        day_timezone=cfg.dashboard_day_timezone)
    if ledger_summary["attempt_count"] or engine.ledger.has_data():
        turnover = ledger_summary["turnover_usd"]
        trade_count = ledger_summary["trade_count"]
        last_trade = ledger_summary["last_trade"]
    else:
        turnover, trade_count, last_trade = _today_trade_summary(
            cfg.trades_csv, cfg.dashboard_day_timezone, now)
    day_attr = attribution["day"]
    execution = _execution_snapshot(engine)
    cycle = (dict(getattr(engine, "_cycle_telemetry", {}) or {})
             or dict(getattr(engine, "_last_cycle_telemetry", {}) or {}))
    cycle_kpis = engine.ledger.cycle_summary(
        since=pnl.get("day_started_ts"), until=now,
        order_budget_block_sec=getattr(
            engine, "_order_budget_block_sec", lambda _now: 0.0)(now))
    bootstrap = getattr(engine, "_bootstrap_snapshot", lambda: {})()
    one_hour_funnel = ((cycle_kpis.get("funnel_windows") or {}).get("1h")
                       or {})
    rolling_cycles = cycle_kpis.get("rolling_20") or {}
    # Keep the bootstrap card backed by the durable one-hour funnel and
    # cycle ledger.  This survives dashboard/browser restarts and avoids
    # retaining another per-order history in the API process.
    bootstrap.update({
        "complete_cycles_per_hour": int(
            one_hour_funnel.get("complete_cycle_count") or 0),
        "open_campaigns_per_hour": int(
            one_hour_funnel.get("open_campaign_count") or 0),
        "open_fill_rate_per_hour": float(
            one_hour_funnel.get("open_campaign_fill_rate") or 0.0),
        "hedge_completion_rate_per_hour": float(
            one_hour_funnel.get("hedge_completion_rate") or 0.0),
        "close_opportunities_per_hour": int(
            one_hour_funnel.get("exit_opportunity_count") or 0),
        "close_fill_rate_per_hour": float(
            one_hour_funnel.get("exit_campaign_fill_rate") or 0.0),
        "completion_rate": float(cycle_kpis.get("completion_rate") or 0.0),
        "median_hold_sec": cycle_kpis.get("median_cycle_duration_sec"),
        "p90_hold_sec": cycle_kpis.get("p90_cycle_duration_sec"),
        "mean_cycle_pnl_usd": rolling_cycles.get("mean_pnl_usd"),
        "median_cycle_pnl_usd": rolling_cycles.get("median_pnl_usd"),
        "total_bootstrap_pnl_usd": bootstrap.get("realized_pnl_usd", 0.0),
    })
    execution_cost_model = getattr(engine, "execution_cost_model", None)
    execution_costs = (execution_cost_model.snapshot(
        full_weight_samples=(bootstrap.get("empirical_full_weight_samples")
                             if bootstrap.get("active") else None))
                       if execution_cost_model is not None else {})
    open_telemetry = dict(getattr(
        engine, "_open_campaign_telemetry", {}) or {})
    # These lists are intentionally copied and bounded by the engine.  The
    # dashboard gets observability without retaining another copy of every
    # order attempt in the browser process.
    open_telemetry["attempts_to_fill"] = list(
        open_telemetry.get("attempts_to_fill") or [])
    open_telemetry["opportunity_to_fill_ms"] = list(
        open_telemetry.get("opportunity_to_fill_ms") or [])
    open_telemetry["fill_probability"] = getattr(
        engine, "_open_fill_probability_snapshot", lambda: {})()
    latency_horizon = getattr(engine, "_open_latency_horizon_sec", None)
    latency_estimate = (latency_horizon()
                        if callable(latency_horizon) else
                        getattr(engine, "_open_latency_sec_ewma", None))
    open_execution = {
        "latency_estimate_sec": _finite(latency_estimate),
        "adverse_move_economic_p75_bps": _finite(getattr(
            engine, "_open_adverse_move_economic_p75",
            lambda _buy, _now: 0.0)(True, now)),
        "adverse_move_stress_p95_bps": _finite(getattr(
            engine, "_open_adverse_move_stress_p95",
            lambda _buy, _now: 0.0)(True, now)),
        "adverse_move_stress_p99_bps": _finite(getattr(
            engine, "_open_adverse_move_stress_p99",
            lambda _buy, _now: 0.0)(True, now)),
        "adverse_move_p90_bps": _finite(getattr(
            engine, "_open_adverse_move_p90", lambda _buy, _now: 0.0)(
                True, now)),
        "telemetry": open_telemetry,
        "active_campaign": (dict(getattr(engine, "_open_campaign", None))
                             if getattr(engine, "_open_campaign", None)
                             else None),
        "last_opportunity": (dict(getattr(
            engine, "_last_open_opportunity", None))
                             if getattr(engine, "_last_open_opportunity", None)
                             else None),
        "degraded_until": dict(getattr(
            engine, "_open_degraded_until", {}) or {}),
        "bootstrap": bootstrap,
    }
    close_telemetry = dict(getattr(
        engine, "_exit_campaign_telemetry", {}) or {})
    close_telemetry["fill_probability"] = getattr(
        engine, "_close_fill_probability_snapshot", lambda: {})()
    close_execution = {
        "telemetry": close_telemetry,
        "fill_probability": close_telemetry["fill_probability"],
        "active_campaign": (dict(getattr(engine, "_exit_campaign", None))
                             if getattr(engine, "_exit_campaign", None)
                             else None),
    }
    cycle_cost_summary = {}
    cost_summary = getattr(engine.ledger, "execution_cost_summary", None)
    if callable(cost_summary):
        cycle_cost_summary = cost_summary(
            cycle.get("execution_ids") if cycle else None)
    cycle_kpis["execution_cost_model"] = execution_costs
    cycle_kpis["current_cycle_execution_costs"] = cycle_cost_summary
    cycle_kpis["bootstrap"] = bootstrap
    time_utc = datetime.fromtimestamp(now, tz=timezone.utc).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")

    return {
        "schema_version": 1,
        "ts": now,
        "time_utc": time_utc,
        "bot_status": "error" if engine.halted else "running",
        "bot_started_ts": engine.start_ts,
        "strategy_name": getattr(engine, "strategy_name", "legacy"),
        "lifecycle": getattr(engine, "_lifecycle_state", lambda: "flat")(),
        "venues": {"entropy": entropy.name, "hedge": hedge.name},
        "symbol": cfg.symbol,
        "pair_state": state.value,
        # A model regime shift changes the reference policy; it is not the
        # same thing as the explicit CLI flatten-only mode.
        "regime": ("reduce_only" if getattr(engine, "flatten_only", False)
                    else "normal"),
        "open_state": open_state,
        "inventory_flatness": engine._inventory_flatness(),
        "positions": {
            "entropy_base": float(entropy.position),
            "hedge_base": float(hedge.position),
            "net_delta_base": net_delta,
            "net_delta_usd": _finite(net_delta_usd),
        },
        "equity": equity,
        "market": {
            "entropy_bid": _finite(eb), "entropy_ask": _finite(ea),
            "hedge_bid": _finite(hb), "hedge_ask": _finite(ha),
            "entropy_mid": _finite(em), "hedge_mid": _finite(hm),
            "premium_mid_bps": _finite(premium),
            "sell_edge_bps": _finite(sell_edge),
            "buy_edge_bps": _finite(buy_edge),
        },
        "strategy": {
            "seed_midline_bps": cfg.midline_bps,
            "active_midline_bps": strategy["active_midline_bps"],
            "short_center_bps": strategy["short_center_bps"],
            "long_center_bps": strategy["long_center_bps"],
            "sigma_center_bps": strategy["sigma_center_bps"],
            "robust_sigma_bps": strategy["robust_sigma_bps"],
            "sigma_sample_count": strategy["sigma_sample_count"],
            "sigma_window_sec": strategy["sigma_window_sec"],
            "midline_lag_bps": strategy["midline_lag_bps"],
            "midline_lag_state": strategy["midline_lag_state"],
            "midline_lag_blocked": strategy["midline_lag_blocked"],
            "open_lag_blocked": strategy["open_lag_blocked"],
            "entry_reference_midline_bps": strategy[
                "entry_reference_midline_bps"],
            "entry_reference_mode": strategy["entry_reference_mode"],
            "dynamic_center_bps": strategy["dynamic_center_bps"],
            "blocked_entry_deviation_bps": strategy[
                "blocked_entry_deviation_bps"],
            "blocked_entry_required_bps": strategy[
                "blocked_entry_required_bps"],
            "blocked_entry_allowed": strategy["blocked_entry_allowed"],
            "warning_entry_penalty_bps": strategy[
                "warning_entry_penalty_bps"],
            "reference_error_bps": strategy["reference_error_bps"],
            "reference_confidence": strategy["reference_confidence"],
            "reference_state": strategy.get("reference_state", "normal"),
            "reference_valid": strategy.get("reference_valid", True),
            "required_sigma_mult": strategy.get("required_sigma_mult", 0.0),
            "required_deviation_bps": strategy.get(
                "required_deviation_bps"),
            "signal_deviation_bps": strategy.get("signal_deviation_bps"),
            "extreme_dislocation": strategy.get(
                "extreme_dislocation", False),
            "extreme_threshold_bps": strategy.get(
                "extreme_threshold_bps"),
            "entry_penalty_bps": strategy.get("entry_penalty_bps", 0.0),
            "open_upper_bps": strategy.get("open_upper_bps"),
            "open_lower_bps": strategy.get("open_lower_bps"),
            "open_state": open_state,
            "open_block_reason": open_reason,
            "effective_open_surplus_bps": strategy[
                "effective_open_surplus_bps"],
            "distance_to_threshold_bps": strategy.get(
                "distance_to_threshold_bps", strategy[
                    "effective_open_surplus_bps"]),
            "reentry_guard": strategy["reentry_guard"],
            "midline_move_speed_bps_per_10m": (
                strategy["midline_move_speed_bps_per_10m"]),
            "entropy_book_age_ms": strategy["entropy_book_age_ms"],
            "hedge_book_age_ms": strategy["hedge_book_age_ms"],
            "cross_venue_skew_ms": strategy["cross_venue_skew_ms"],
            "premium_sample_valid": strategy["premium_sample_valid"],
            "premium_sample_reject_reason": (
                strategy["premium_sample_reject_reason"]),
            "premium_data_degraded": strategy["premium_data_degraded"],
            "premium_data_invalid": strategy.get(
                "premium_data_invalid", strategy["premium_data_degraded"]),
            "premium_data_tier": strategy.get("premium_data_tier", "GOOD"),
            "premium_data_uncertainty_bps": strategy.get(
                "premium_data_uncertainty_bps", 0.0),
            "premium_data_invalid_duration_sec": strategy.get(
                "premium_data_invalid_duration_sec", 0.0),
            "dynamic_midline_validation_remaining_sec": strategy[
                "dynamic_midline_validation_remaining_sec"],
            "dynamic_midline_warmup_sec": strategy[
                "dynamic_midline_warmup_sec"],
            "dynamic_midline_min_samples": strategy[
                "dynamic_midline_min_samples"],
            "dynamic_midline_tradable": strategy.get(
                "dynamic_midline_tradable", strategy["dynamic_midline_ready"]),
            "dynamic_midline_readiness": strategy.get(
                "dynamic_midline_readiness", "NORMAL"),
            "dynamic_midline_history_sec": strategy.get(
                "dynamic_midline_history_sec"),
            "dynamic_midline_latest_sample_age_sec": strategy.get(
                "dynamic_midline_latest_sample_age_sec"),
            "dynamic_midline_history_source": strategy[
                "dynamic_midline_history_source"],
            "dynamic_midline_history_samples": strategy[
                "dynamic_midline_history_samples"],
            "short_entry_bps": strategy["short_entry_bps"],
            "short_exit_bps": strategy["short_exit_bps"],
            "long_entry_bps": strategy["long_entry_bps"],
            "long_exit_bps": strategy["long_exit_bps"],
            "effective_upper_bps": strategy["effective_upper_bps"],
            "effective_lower_bps": strategy["effective_lower_bps"],
            "volatility_guard": strategy["volatility_guard"],
            "cycle_active": strategy["cycle_active"],
            "cycle_anchored": strategy["cycle_anchored"],
            "cycle_direction": strategy["cycle_direction"],
            "cycle_entry_midline_bps": strategy["cycle_entry_midline_bps"],
            "cycle_entry_reference_bps": strategy[
                "cycle_entry_reference_bps"],
            "cycle_entry_reference_mode": strategy[
                "cycle_entry_reference_mode"],
            "cycle_entry_active_midline_bps": strategy[
                "cycle_entry_active_midline_bps"],
            "cycle_entry_dynamic_center_bps": strategy[
                "cycle_entry_dynamic_center_bps"],
            "cycle_entry_sigma_bps": strategy["cycle_entry_sigma_bps"],
            "cycle_entry_band_bps": strategy["cycle_entry_band_bps"],
            "cycle_midline_drift_bps": strategy["cycle_midline_drift_bps"],
            "cycle_add_blocked": strategy["cycle_add_blocked"],
            "cycle_drift_guard": strategy["cycle_drift_guard"],
            "cycle_allow_add": strategy["cycle_allow_add"],
            "cycle_max_active_pairs": strategy["cycle_max_active_pairs"],
            "cycle_exit_mode": cfg.cycle_exit.mode,
            "cycle_exit_normal_min_profit_usd": (
                cfg.cycle_exit.normal_min_profit_usd),
            "cycle_exit_fast_after_sec": cfg.cycle_exit.fast_exit_after_sec,
            "cycle_exit_fast_min_profit_usd": (
                cfg.cycle_exit.fast_exit_min_profit_usd),
            "cycle_exit_max_hold_sec": cfg.cycle_exit.max_hold_sec,
            "cycle_exit_long_hold_after_sec": (
                cfg.cycle_exit.long_hold_after_sec),
            "cycle_exit_hard_timeout_enabled": (
                cfg.cycle_exit.hard_timeout_enabled),
            "cycle_exit_hard_timeout_sec": cfg.cycle_exit.hard_timeout_sec,
            "cycle_exit_hard_timeout_min_profit_usd": (
                cfg.cycle_exit.hard_timeout_min_profit_usd),
            "cycle_exit_pnl_loss_stop_enabled": (
                cfg.cycle_exit.pnl_loss_stop_enabled),
            "cycle_exit_adverse_drift_exit_enabled": (
                cfg.cycle_exit.adverse_drift_exit_enabled),
            "cycle_exit_max_cycle_loss_bps": (
                cfg.cycle_exit.max_cycle_loss_bps),
            "cycle_hold_state": strategy["cycle_hold_state"],
            "cycle_hold_state_ts": strategy["cycle_hold_state_ts"],
            "margin_state": strategy["margin_state"],
            "margin_available_ratio": strategy["margin_available_ratio"],
            "margin_known": strategy["margin_known"],
            "reference_uncertainty_bps": strategy.get(
                "reference_uncertainty_bps", 0.0),
            "model_uncertainty_bps": strategy.get(
                "model_uncertainty_bps", 0.0),
            "reentry_uncertainty_bps": strategy.get(
                "reentry_uncertainty_bps", 0.0),
            "margin_uncertainty_bps": strategy.get(
                "margin_uncertainty_bps", 0.0),
            "profit_guard": {
                "enabled": cfg.profit_guard.enabled,
                "min_profit_usd": cfg.profit_guard.min_profit_usd,
                "entry_buffer_bps": cfg.profit_guard.entry_buffer_bps,
                "exit_buffer_bps": cfg.profit_guard.exit_buffer_bps,
                "mode": cfg.profit_guard.mode,
                "normal_exit_mode": (
                    "fill_pnl" if cfg.cycle_exit.enabled
                    and cfg.cycle_exit.mode == "fill_pnl"
                    else ("fifo_profit" if cfg.profit_guard.enabled
                          else "midline")),
                "checks": dict(engine.profit_checks),
            },
            "dynamic_midline_ready": strategy["dynamic_midline_ready"],
            "regime_shift": regime_shift,
        },
        "funding": {
            "entropy_bps_h": _finite(er), "hedge_bps_h": _finite(hr),
            "short_entropy_cost_bps": strategy["short_entropy_cost_bps"],
            "long_entropy_cost_bps": strategy["long_entropy_cost_bps"],
            "available": funding_available,
            "stale": funding_stale,
        },
        "risk": {
            "open_blocked": open_reason is not None or strategy[
                "margin_open_blocked"],
            "open_block_reason": (open_reason or ("margin_" + strategy[
                "margin_state"] if strategy["margin_open_blocked"] else None)),
            "open_block_class": open_block_class,
            "open_state": open_state,
            "reduce_allowed": True,
            "net_tolerance_base": cfg.net_tolerance_base,
            "margin_state": strategy["margin_state"],
            "margin_open_blocked": strategy["margin_open_blocked"],
            "margin_hard_risk": strategy["margin_hard_risk"],
            "margin_available_ratio": strategy["margin_available_ratio"],
            "margin_known": strategy["margin_known"],
            "margin_venues": strategy["margin_venues"],
        },
        "execution_policy": {
            "policy_key": engine._cycle_policy_key(),
            "effective_policy": {
                "entry_pnl": "scenario_projection_with_conservative_realized_error_calibration",
                "execution_cost": "time_weighted_mean_with_live_prior",
                "adaptive_band_floor": "configured_minimum",
                "risk_exit": "reduce_only_with_physical_limits_without_profit_floor",
                "exit_policy_active": bool(cfg.cycle_exit.enabled and cfg.cycle_exit.mode == "fill_pnl"),
                "compatibility_only": ["cycle_exit.hard_timeout_min_profit_usd",
                                       "reduce_execution.max_campaigns_per_signal_episode",
                                       "reduce_execution.reopen_improvement_bps"],
                "observation_only": ["cycle_exit.max_hold_sec", "cycle_exit.long_hold_after_sec"],
            },
            "open_persist_sec": engine._execution_persist_sec(StrategyAction.OPEN),
            "reduce_persist_sec": engine._execution_persist_sec(StrategyAction.REDUCE),
            "open_cooldown_sec": engine._execution_cooldown_sec(StrategyAction.OPEN),
            "reduce_cooldown_sec": engine._execution_cooldown_sec(StrategyAction.REDUCE),
            "open_first_leg_slippage_bps": (
                engine._first_leg_slippage_bps(StrategyAction.OPEN)),
            "reduce_first_leg_slippage_bps": (
                engine._first_leg_slippage_bps(StrategyAction.REDUCE)),
            "open_second_leg_slippage_bps": (
                engine._second_leg_slippage_bps(StrategyAction.OPEN)),
            "reduce_second_leg_slippage_bps": (
                engine._second_leg_slippage_bps(StrategyAction.REDUCE)),
            "open_no_fill_retry_sec": engine._no_fill_retry_sec(StrategyAction.OPEN),
            "reduce_no_fill_retry_sec": engine._no_fill_retry_sec(StrategyAction.REDUCE),
            "emergency_slippage_bps": cfg.emergency_slippage_bps,
            "max_reduce_retry_attempts": cfg.max_reduce_retry_attempts,
            "reduce_retry_escalation_bps": list(
                cfg.reduce_retry_escalation_bps),
            "exit_campaign_sec": cfg.reduce_execution.exit_campaign_sec,
            "exit_campaign_retry_interval_sec": (
                cfg.reduce_execution.exit_campaign_retry_interval_sec),
            "dynamic_hedge_reserve": {
                "enabled": cfg.reduce_execution.dynamic_hedge_reserve.enabled,
                "min_bps": cfg.reduce_execution.dynamic_hedge_reserve.min_bps,
                "max_bps": cfg.reduce_execution.dynamic_hedge_reserve.max_bps,
                "adverse_move_percentile": (
                    cfg.reduce_execution.dynamic_hedge_reserve
                    .adverse_move_percentile),
                "safety_margin_bps": (
                    cfg.reduce_execution.dynamic_hedge_reserve
                    .safety_margin_bps),
            },
            "dynamic_open_first_leg": {
                "enabled": cfg.reduce_execution.dynamic_open_first_leg.enabled,
                "min_bps": cfg.reduce_execution.dynamic_open_first_leg.min_bps,
                "max_bps": cfg.reduce_execution.dynamic_open_first_leg.max_bps,
                "adverse_move_percentile": (
                    cfg.reduce_execution.dynamic_open_first_leg
                    .adverse_move_percentile),
                "safety_margin_bps": (
                    cfg.reduce_execution.dynamic_open_first_leg
                    .safety_margin_bps),
            },
            "open_retry_campaign_sec": (
                cfg.reduce_execution.open_retry_campaign_sec),
            "open_retry_interval_sec": (
                cfg.reduce_execution.open_retry_interval_sec),
            "open_retry_max_attempts": (
                cfg.reduce_execution.open_retry_max_attempts),
            "open_execution_room_min_bps": (
                cfg.reduce_execution.open_execution_room_min_bps),
            "reserve_reduce_slots": cfg.reduce_execution.reserve_reduce_slots,
            "open_first_leg_hard_cap_bps": (
                cfg.reduce_execution.open_first_leg_hard_cap_bps),
            "open_second_leg_hard_cap_bps": (
                cfg.reduce_execution.open_second_leg_hard_cap_bps),
            "exit_campaign_max_attempts": (
                cfg.reduce_execution.exit_campaign_max_attempts),
            "reopen_improvement_bps": (
                cfg.reduce_execution.reopen_improvement_bps),
            "max_campaigns_per_signal_episode": (
                cfg.reduce_execution.max_campaigns_per_signal_episode),
            "economic_execution": {
                "min_expected_cycle_profit_usd": (
                    cfg.economic_execution.min_expected_cycle_profit_usd),
                "open_first_cost_percentile": (
                    cfg.economic_execution.open_first_cost_percentile),
                "open_hedge_cost_percentile": (
                    cfg.economic_execution.open_hedge_cost_percentile),
                "close_first_cost_percentile": (
                    cfg.economic_execution.close_first_cost_percentile),
                "close_hedge_cost_percentile": (
                    cfg.economic_execution.close_hedge_cost_percentile),
                "safety_margin_bps": cfg.economic_execution.safety_margin_bps,
            },
            "bootstrap": bootstrap,
        },
        "today": {
            **pnl,
            "realized_pnl_usd": day_attr["gross_realized_usd"],
            "unrealized_pnl_usd": day_attr["unrealized_usd"],
            "gross_pnl_usd": (day_attr["gross_realized_usd"]
                              + day_attr["unrealized_usd"]),
            "fees_usd": day_attr["fees_usd"],
            "fees_status": day_attr.get("fees_status", "none"),
            "actual_fee_usd": day_attr.get("actual_fee_usd", 0.0),
            "estimated_fee_usd": day_attr.get("estimated_fee_usd", 0.0),
            "actual_fee_fill_count": day_attr.get("actual_fee_fill_count", 0),
            "estimated_fee_fill_count": day_attr.get(
                "estimated_fee_fill_count", 0),
            "funding_pnl_usd": day_attr["funding_pnl_usd"],
            "turnover_usd": turnover, "trade_count": trade_count,
        },
        "last_trade": last_trade,
        "execution": execution,
        "open_execution": open_execution,
        "close_execution": close_execution,
        "open_block_telemetry": getattr(
            engine, "_open_block_telemetry_snapshot", lambda _now: {})(now),
        "cycle": cycle or None,
        "cycle_kpis": cycle_kpis,
        "execution_costs": execution_costs,
        "order_budget": getattr(
            engine, "_order_budget_snapshot", lambda: {})(now),
        "config_restart_required": bool(
            getattr(engine, "config_restart_required", False)),
        "config_restart_required_paths": list(getattr(
            engine, "config_restart_required_paths", [])),
        "reconciliation": _reconciliation_snapshot(engine),
        "fee_reconciliation": dict(getattr(
            engine, "fee_reconciliation", {"status": "unknown"})),
        "attribution": attribution,
        "retro_farming": dict(getattr(engine, "retro_farming_snapshot", {
            "status": "unavailable",
            "shadow_canary_plan": {
                "mode": "shadow", "eligible": False,
                "should_execute": False,
            },
        })),
    }


def _execution_snapshot(engine) -> Optional[dict]:
    state = getattr(engine, "active_execution", None)
    if state is None:
        return None
    entropy, hedge = engine.entropy, engine.hedge
    campaign = getattr(engine, "_exit_campaign", None)
    open_campaign = getattr(engine, "_open_campaign", None)
    return {
        "execution_id": state.cycle_id,
        "phase": state.phase.value,
        "action": state.action,
        "direction": state.direction,
        "planned_qty": state.planned_qty,
        "first": {
            "venue": entropy.key, "side": "buy" if state.entropy_is_buy
            else "sell", "planned_qty": state.planned_qty,
            "filled_qty": state.first_filled,
            "status": state.first_order_status,
            "order_ref": state.first_order_ref,
            "limit_px": state.first_limit_px,
            "avg_px": state.first_avg_px,
            "position_before": state.first_position_before,
            "position_now": entropy.position,
        },
        "hedge": {
            "venue": hedge.key, "side": "buy" if state.hedge_is_buy
            else "sell", "planned_qty": state.first_filled,
            "filled_qty": state.hedge_filled,
            "status": state.hedge_order_status,
            "order_ref": state.hedge_order_ref,
            "limit_px": state.hedge_limit_px,
            "avg_px": state.hedge_avg_px,
            "position_before": state.hedge_position_before,
            "position_now": hedge.position,
        },
        "remaining_hedge_qty": state.remaining_hedge,
        "pending_leg": state.pending_leg,
        "pending_order_ref": state.pending_order_ref,
        "pending_filled_qty": state.pending_filled,
        "retry_count": state.retry_count,
        "first_topup_count": state.first_topup_count,
        "recovery_status": state.recovery_status.value,
        "recovery_started_ts": state.recovery_started_ts,
        "recovery_reason": state.recovery_reason,
        "recovery_next_retry_ts": state.recovery_next_retry_ts,
        "recovery_attempt": state.recovery_attempt,
        "recovery_backoff_sec": state.recovery_backoff_sec,
        "hedge_reserve_breach": state.hedge_reserve_breach,
        "hedge_reserve_breach_ts": state.hedge_reserve_breach_ts,
        "hedge_completion_state": getattr(
            state, "hedge_completion_state", "not_started"),
        "last_error": state.last_error,
        "created_ts": state.created_ts,
        "updated_ts": state.updated_ts,
        "exit_campaign": (dict(campaign) if campaign is not None else None),
        "open_campaign": (dict(open_campaign)
                           if open_campaign is not None else None),
    }


def _reconciliation_snapshot(engine) -> dict:
    raw = getattr(engine, "reconciliation", {}) or {}
    return {
        "status": raw.get("status", "not_started"),
        "last_ts": raw.get("last_ts"),
        "startup_reconciled": bool(raw.get("startup_reconciled", False)),
        "net_delta_base": raw.get("net_delta_base"),
        "last_error": raw.get("last_error"),
        "venues": raw.get("venues", {}),
    }


def write_dashboard_snapshot(path: str | os.PathLike[str], payload: dict) -> None:
    """Durably replace a snapshot using a temporary file in the same folder."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        dir=target.parent, prefix=f".{target.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary, target)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def safe_write_dashboard_snapshot(path: str | os.PathLike[str],
                                  payload: dict) -> bool:
    try:
        write_dashboard_snapshot(path, payload)
        return True
    except Exception as exc:
        log.warning("dashboard snapshot write failed: %r", exc)
        return False


def mark_dashboard_status(path: str | os.PathLike[str], status: str,
                          now: Optional[float] = None) -> bool:
    """Mark the last published snapshot as stopped or errored.

    Keep ``ts`` unchanged: it is the timestamp of the last live snapshot and
    must continue to drive the API's staleness calculation.  Updating it here
    would make a dead engine look fresh to the dashboard.
    """
    if status not in {"stopped", "error"}:
        raise ValueError(f"unsupported dashboard status: {status}")
    try:
        target = Path(path)
        with target.open(encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            return False
        payload["bot_status"] = status
        # Keep the timestamp separate from ``ts`` so consumers can distinguish
        # a stale last-live snapshot from the time the process stopped.
        payload["status_changed_ts"] = time.time() if now is None else now
        return safe_write_dashboard_snapshot(target, payload)
    except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError):
        return False
