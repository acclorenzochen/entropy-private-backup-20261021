"""Retrospective-farming contribution projection.

Entropy has not published a points formula.  This module therefore keeps raw
project contributions separate from a small set of explicitly hypothetical
score models.  It never turns a hypothesis into realised PnL or an official
points estimate.
"""
from __future__ import annotations

import json
import math
import sqlite3
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from .control_plane import is_clean_circle


@dataclass(frozen=True)
class RetroScoreTargets:
    """Campaign coverage targets used to compare hypotheses.

    These are portfolio-diversification scales, not predicted Entropy weights.
    Every report includes them so a score cannot be read without its assumed
    denominator.
    """

    fee_contribution_usd: float = 0.50
    growth_adjusted_volume_usd: float = 5_000.0
    active_days: int = 14
    active_weeks: int = 4
    oi_usd_hours: float = 210.0
    maker_volume_usd: float = 1_000.0


@dataclass(frozen=True)
class RetroAnalysisConfig:
    growth_volume_multiplier: float = 0.10
    ultra_short_hold_sec: float = 60.0
    notional_bucket_usd: float = 0.25
    targets: RetroScoreTargets = RetroScoreTargets()


@dataclass(frozen=True)
class RetroCanaryPolicy:
    """Loss-budget and spacing policy for a contribution canary.

    ``mode`` defaults to ``shadow``: the planner can explain whether an
    episode would qualify, but cannot authorize an order.
    """

    mode: str = "shadow"
    total_loss_budget_usd: float = 3.0
    retention_budget_share: float = 0.40
    oi_budget_share: float = 0.30
    liquidity_budget_share: float = 0.20
    taker_quality_budget_share: float = 0.10
    max_episode_expected_loss_usd: float = 0.01
    max_acquisition_cost_per_1k_growth_volume_usd: float = 2.0
    min_episode_spacing_sec: float = 3600.0


def _number(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _utc_keys(ts: float) -> tuple[str, str, str]:
    stamp = datetime.fromtimestamp(ts, tz=timezone.utc)
    iso = stamp.isocalendar()
    return (
        stamp.strftime("%Y-%m-%d"),
        f"{iso.year}-W{iso.week:02d}",
        stamp.strftime("%Y-%m-%dT%H"),
    )


def _is_maker_fill(row: Mapping[str, Any]) -> bool:
    metadata = _payload(row.get("metadata_json"))
    for key in ("is_maker", "maker"):
        value = metadata.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
    role = str(metadata.get("liquidity_role") or metadata.get("role") or "")
    return role.strip().lower() in {"maker", "m", "add", "added"}


def _coverage(value: float, target: float) -> float:
    """Smooth, diminishing campaign coverage in [0, 1)."""
    if value <= 0.0 or target <= 0.0:
        return 0.0
    return 1.0 - math.exp(-value / target)


def _safe_div(numerator: float, denominator: float) -> Optional[float]:
    return numerator / denominator if denominator > 0.0 else None


def _same_utc_bucket(left: Optional[float], right: float, fmt: str) -> bool:
    if left is None:
        return False
    return (datetime.fromtimestamp(left, tz=timezone.utc).strftime(fmt)
            == datetime.fromtimestamp(right, tz=timezone.utc).strftime(fmt))


def _quantile(values: Sequence[float], fraction: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = min(max(math.ceil(len(ordered) * fraction) - 1, 0),
                len(ordered) - 1)
    return ordered[index]


def analyze_project_contribution(
        cycles: Iterable[Mapping[str, Any]],
        fills: Iterable[Mapping[str, Any]], *,
        config: RetroAnalysisConfig = RetroAnalysisConfig()) -> dict[str, Any]:
    """Build raw contributions and transparent hypothetical score coverage."""
    strict_cycles: list[dict[str, Any]] = []
    execution_ids: set[str] = set()
    for raw in cycles:
        row = dict(raw)
        payload = _payload(row.get("payload_json"))
        merged = {**payload, **row}
        if not is_clean_circle(merged):
            continue
        strict_cycles.append(merged)
        ids = payload.get("execution_ids") or merged.get("execution_ids") or []
        if isinstance(ids, (list, tuple)):
            execution_ids.update(str(value) for value in ids if value)
        cycle_id = merged.get("cycle_id")
        if cycle_id:
            execution_ids.add(str(cycle_id))

    strict_cycles.sort(key=lambda row: _number(row.get("flat_ts")) or 0.0)
    selected_fills: list[dict[str, Any]] = []
    for raw in fills:
        row = dict(raw)
        if str(row.get("execution_id") or "") not in execution_ids:
            continue
        if str(row.get("venue") or "").lower() != "entropy":
            continue
        if str(row.get("source") or "") == "position_baseline":
            continue
        selected_fills.append(row)

    raw_volume = 0.0
    fee_contribution = 0.0
    maker_volume = 0.0
    volume_by_hour: dict[str, float] = {}
    active_days: set[str] = set()
    active_weeks: set[str] = set()
    active_hours: set[str] = set()
    for fill in selected_fills:
        qty = abs(_number(fill.get("qty")) or 0.0)
        price = _number(fill.get("price")) or 0.0
        volume = qty * price
        raw_volume += volume
        fee_contribution += max(_number(fill.get("fee_usd")) or 0.0, 0.0)
        if _is_maker_fill(fill):
            maker_volume += volume
        ts = _number(fill.get("ts"))
        if ts is not None:
            day, week, hour = _utc_keys(ts)
            active_days.add(day)
            active_weeks.add(week)
            active_hours.add(hour)
            volume_by_hour[hour] = volume_by_hour.get(hour, 0.0) + volume

    holds: list[float] = []
    notionals: list[float] = []
    total_pnl = 0.0
    pnl_count = 0
    oi_usd_hours = 0.0
    first_ts: Optional[float] = None
    last_ts: Optional[float] = None
    for cycle in strict_cycles:
        hold = max(_number(cycle.get("hold_sec")) or 0.0, 0.0)
        notional = max(_number(cycle.get("notional_usd")) or 0.0, 0.0)
        pnl = _number(cycle.get("total_cycle_pnl_usd"))
        holds.append(hold)
        notionals.append(notional)
        oi_usd_hours += notional * hold / 3600.0
        if pnl is not None:
            total_pnl += pnl
            pnl_count += 1
        opened = _number(cycle.get("open_first_fill_ts"))
        flat = _number(cycle.get("flat_ts"))
        if opened is not None:
            first_ts = opened if first_ts is None else min(first_ts, opened)
        if flat is not None:
            last_ts = flat if last_ts is None else max(last_ts, flat)

    ultra_short = sum(hold < config.ultra_short_hold_sec for hold in holds)
    ultra_short_rate = _safe_div(float(ultra_short), float(len(holds))) or 0.0
    top_hour_share = (_safe_div(max(volume_by_hour.values(), default=0.0),
                                raw_volume) or 0.0)
    bucket = max(float(config.notional_bucket_usd), 1e-9)
    notional_counts: dict[int, int] = {}
    for notional in notionals:
        key = round(notional / bucket)
        notional_counts[key] = notional_counts.get(key, 0) + 1
    dominant_notional_share = (
        _safe_div(float(max(notional_counts.values(), default=0)),
                  float(len(notionals))) or 0.0)
    # This is an explicit hypothesis for anti-pattern stress, not an accusation
    # or an official fraud classification.
    organic_quality = max(
        0.0,
        1.0 - (0.40 * ultra_short_rate
               + 0.30 * top_hour_share
               + 0.30 * dominant_notional_share),
    )

    growth_volume = raw_volume * max(config.growth_volume_multiplier, 0.0)
    targets = config.targets
    retention_coverage = statistics.fmean((
        _coverage(float(len(active_days)), float(targets.active_days)),
        _coverage(float(len(active_weeks)), float(targets.active_weeks)),
    ))
    contribution_coverage = {
        "FEE": _coverage(fee_contribution, targets.fee_contribution_usd),
        "VOLUME": _coverage(growth_volume,
                            targets.growth_adjusted_volume_usd),
        "RETENTION": retention_coverage,
        "OI": _coverage(oi_usd_hours, targets.oi_usd_hours),
        "LIQUIDITY": _coverage(maker_volume, targets.maker_volume_usd),
    }
    scenario_scores = {
        "FEE_WEIGHTED": (
            contribution_coverage["FEE"]
            * (0.5 + 0.5 * organic_quality)),
        "VOLUME_WEIGHTED": (
            contribution_coverage["VOLUME"] * organic_quality),
        "RETENTION_WEIGHTED": retention_coverage,
        "OI_WEIGHTED": contribution_coverage["OI"],
        "LIQUIDITY_QUALITY": (
            contribution_coverage["LIQUIDITY"] * organic_quality),
    }
    ordered_scenarios = sorted(scenario_scores.items(), key=lambda item: item[1])
    acquisition_cost = max(-total_pnl, 0.0)
    result = {
        "schema_version": 1,
        "assumption": "hypothetical_retro_score_coverage_not_official_points",
        "strict_cycles": len(strict_cycles),
        "entropy_fill_count": len(selected_fills),
        "first_activity_ts": first_ts,
        "last_activity_ts": last_ts,
        "activity_span_sec": (
            max(last_ts - first_ts, 0.0)
            if first_ts is not None and last_ts is not None else 0.0),
        "raw_entropy_volume_usd": raw_volume,
        "growth_volume_multiplier": config.growth_volume_multiplier,
        "growth_adjusted_volume_usd": growth_volume,
        "entropy_fee_contribution_usd": fee_contribution,
        "maker_volume_usd": maker_volume,
        "oi_usd_hours": oi_usd_hours,
        "active_utc_days": len(active_days),
        "active_utc_weeks": len(active_weeks),
        "active_utc_hours": len(active_hours),
        "hold_sec": {
            "mean": statistics.fmean(holds) if holds else None,
            "median": statistics.median(holds) if holds else None,
            "p90": _quantile(holds, 0.90),
        },
        "cash": {
            "total_cycle_pnl_usd": total_pnl if pnl_count else None,
            "acquisition_cost_usd": acquisition_cost if pnl_count else None,
            "acquisition_cost_per_1k_raw_volume_usd": (
                _safe_div(acquisition_cost * 1000.0, raw_volume)
                if pnl_count else None),
            "acquisition_cost_per_1k_growth_volume_usd": (
                _safe_div(acquisition_cost * 1000.0, growth_volume)
                if pnl_count else None),
        },
        "anti_pattern_indicators": {
            "ultra_short_hold_sec": config.ultra_short_hold_sec,
            "ultra_short_cycle_rate": ultra_short_rate,
            "top_utc_hour_volume_share": top_hour_share,
            "dominant_notional_bucket_usd": bucket,
            "dominant_notional_share": dominant_notional_share,
            "hypothetical_organic_quality": organic_quality,
        },
        "coverage_targets": asdict(targets),
        "contribution_coverage": contribution_coverage,
        "hypothesis_coverage": scenario_scores,
        "robust_coverage_floor": ordered_scenarios[0][1]
        if ordered_scenarios else 0.0,
        "hypothesis_coverage_priority": [name for name, _ in ordered_scenarios],
    }
    return result


def plan_retro_canary(
        report: Mapping[str, Any], *, now: Optional[float] = None,
        expected_loss_usd: float = 0.005,
        expected_raw_volume_usd: float = 30.0,
        spent_loss_usd: float = 0.0,
        spent_by_bucket_usd: Optional[Mapping[str, float]] = None,
        requested_bucket: Optional[str] = None,
        policy: RetroCanaryPolicy = RetroCanaryPolicy()) -> dict[str, Any]:
    """Plan one bounded canary episode without mutating trading state.

    The result is an admission decision, not an order instruction.  A shadow
    policy always returns ``should_execute=False`` even when all gates pass.
    """
    now = datetime.now(tz=timezone.utc).timestamp() if now is None else float(now)
    mode = str(policy.mode).strip().lower()
    if mode not in {"disabled", "shadow", "active"}:
        raise ValueError("retro canary mode must be disabled, shadow, or active")

    shares = {
        "RETENTION": float(policy.retention_budget_share),
        "OI": float(policy.oi_budget_share),
        "LIQUIDITY": float(policy.liquidity_budget_share),
        "TAKER_QUALITY": float(policy.taker_quality_budget_share),
    }
    if any(value < 0.0 for value in shares.values()) or not math.isclose(
            sum(shares.values()), 1.0, abs_tol=1e-9):
        raise ValueError("retro canary budget shares must be non-negative and sum to 1")

    coverage = report.get("contribution_coverage")
    coverage = dict(coverage) if isinstance(coverage, Mapping) else {}
    urgency = {
        "RETENTION": shares["RETENTION"] * (1.0 - min(max(
            _number(coverage.get("RETENTION")) or 0.0, 0.0), 1.0)),
        "OI": shares["OI"] * (1.0 - min(max(
            _number(coverage.get("OI")) or 0.0, 0.0), 1.0)),
        "LIQUIDITY": shares["LIQUIDITY"] * (1.0 - min(max(
            _number(coverage.get("LIQUIDITY")) or 0.0, 0.0), 1.0)),
        "TAKER_QUALITY": shares["TAKER_QUALITY"] * (1.0 - min(max(
            _number(coverage.get("VOLUME")) or 0.0, 0.0), 1.0)),
    }
    priorities = sorted(urgency, key=lambda key: (-urgency[key], key))
    last_ts = _number(report.get("last_activity_ts"))
    same_day = _same_utc_bucket(last_ts, now, "%Y-%m-%d")
    same_hour = _same_utc_bucket(last_ts, now, "%Y-%m-%dT%H")

    def is_new_bucket(candidate: str) -> bool:
        return (
            (candidate == "RETENTION" and not same_day)
            or (candidate == "LIQUIDITY"
                and (_number(report.get("maker_volume_usd")) or 0.0) <= 0.0)
            or (candidate == "OI"
                and (_number(report.get("oi_usd_hours")) or 0.0) <= 0.0)
            or (candidate == "TAKER_QUALITY"
                and int(_number(report.get("entropy_fill_count")) or 0) == 0)
        )

    if requested_bucket is None:
        # Within an already-active UTC day, skip a higher-weight bucket that
        # would merely repeat the same contribution and choose a genuinely new
        # bucket when one exists.
        bucket = next((candidate for candidate in priorities
                       if not same_day or is_new_bucket(candidate)), priorities[0])
    else:
        bucket = str(requested_bucket).strip().upper()
    if bucket not in shares:
        raise ValueError(f"unknown retro canary bucket: {bucket}")
    new_bucket = is_new_bucket(bucket)

    expected_loss = max(float(expected_loss_usd), 0.0)
    expected_raw_volume = max(float(expected_raw_volume_usd), 0.0)
    multiplier = max(_number(report.get("growth_volume_multiplier")) or 0.0,
                     0.0)
    expected_growth_volume = expected_raw_volume * multiplier
    expected_acquisition_cost = _safe_div(
        expected_loss * 1000.0, expected_growth_volume)
    total_budget = max(float(policy.total_loss_budget_usd), 0.0)
    total_spent = max(float(spent_loss_usd), 0.0)
    bucket_spend_map = spent_by_bucket_usd or {}
    bucket_spent = max(_number(bucket_spend_map.get(bucket)) or 0.0, 0.0)
    bucket_budget = total_budget * shares[bucket]
    total_remaining = max(total_budget - total_spent, 0.0)
    bucket_remaining = max(bucket_budget - bucket_spent, 0.0)

    reasons: list[str] = []
    if mode == "disabled":
        reasons.append("policy_disabled")
    if same_hour:
        reasons.append("same_utc_hour_episode")
    if same_day and not new_bucket:
        reasons.append("daily_or_new_bucket_gate")
    if last_ts is not None and now - last_ts < policy.min_episode_spacing_sec:
        reasons.append("minimum_spacing")
    if expected_loss > policy.max_episode_expected_loss_usd + 1e-12:
        reasons.append("episode_expected_loss_cap")
    if expected_loss > total_remaining + 1e-12:
        reasons.append("total_loss_budget")
    if expected_loss > bucket_remaining + 1e-12:
        reasons.append("bucket_loss_budget")
    if expected_growth_volume <= 0.0:
        reasons.append("missing_expected_growth_volume")
    elif (expected_acquisition_cost is not None
          and expected_acquisition_cost
          > policy.max_acquisition_cost_per_1k_growth_volume_usd + 1e-12):
        reasons.append("acquisition_cost_cap")

    eligible = not reasons
    return {
        "mode": mode,
        "status": ("active_eligible" if eligible and mode == "active"
                   else "shadow_eligible" if eligible and mode == "shadow"
                   else "blocked"),
        "eligible": eligible,
        "should_execute": bool(eligible and mode == "active"),
        "selected_bucket": bucket,
        "required_strategy": ("maker-hedge" if bucket == "LIQUIDITY"
                              else "legacy"),
        "priority": priorities,
        "urgency": urgency,
        "is_new_contribution_bucket": new_bucket,
        "same_utc_day_as_last_activity": same_day,
        "same_utc_hour_as_last_activity": same_hour,
        "expected_loss_usd": expected_loss,
        "expected_raw_volume_usd": expected_raw_volume,
        "expected_growth_adjusted_volume_usd": expected_growth_volume,
        "expected_acquisition_cost_per_1k_growth_volume_usd": (
            expected_acquisition_cost),
        "loss_budget": {
            "total_usd": total_budget,
            "total_spent_usd": total_spent,
            "total_remaining_usd": total_remaining,
            "bucket_usd": bucket_budget,
            "bucket_spent_usd": bucket_spent,
            "bucket_remaining_usd": bucket_remaining,
        },
        "reasons": reasons,
    }


def load_project_contribution(
        sqlite_path: str | Path, *,
        since: Optional[float] = None,
        until: Optional[float] = None,
        limit: Optional[int] = None,
        config: RetroAnalysisConfig = RetroAnalysisConfig()) -> dict[str, Any]:
    """Read a ledger snapshot without mutating the live SQLite database."""
    path = Path(sqlite_path).expanduser().resolve()
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        clauses = ["status='completed'"]
        params: list[Any] = []
        if since is not None:
            clauses.append("flat_ts>=?")
            params.append(float(since))
        if until is not None:
            clauses.append("flat_ts<=?")
            params.append(float(until))
        query = ("SELECT * FROM cycle_records WHERE "
                 + " AND ".join(clauses)
                 + " ORDER BY flat_ts DESC")
        if limit is not None:
            query += " LIMIT ?"
            params.append(max(int(limit), 0))
        cycle_rows = [dict(row) for row in connection.execute(query, params)]

        execution_ids: set[str] = set()
        for row in cycle_rows:
            payload = _payload(row.get("payload_json"))
            ids = payload.get("execution_ids") or []
            if isinstance(ids, (list, tuple)):
                execution_ids.update(str(value) for value in ids if value)
            if row.get("cycle_id"):
                execution_ids.add(str(row["cycle_id"]))

        fill_rows: list[dict[str, Any]] = []
        identifiers = sorted(execution_ids)
        for start in range(0, len(identifiers), 900):
            chunk = identifiers[start:start + 900]
            if not chunk:
                continue
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"SELECT * FROM fills WHERE execution_id IN ({placeholders})",
                chunk,
            ).fetchall()
            fill_rows.extend(dict(row) for row in rows)
    finally:
        connection.close()
    return analyze_project_contribution(cycle_rows, fill_rows, config=config)


def build_shadow_retro_snapshot(
        sqlite_path: str | Path, *, now: Optional[float] = None,
        limit: Optional[int] = None,
        config: RetroAnalysisConfig = RetroAnalysisConfig(),
        policy: RetroCanaryPolicy = RetroCanaryPolicy()) -> dict[str, Any]:
    """Build the read-only dashboard payload for the shadow planner."""
    report = load_project_contribution(sqlite_path, limit=limit, config=config)
    report["shadow_canary_plan"] = plan_retro_canary(
        report, now=now, policy=policy)
    return report
