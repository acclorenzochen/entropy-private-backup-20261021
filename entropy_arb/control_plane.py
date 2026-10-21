"""Small, deterministic helpers for the arbitrage control plane.

The live engine owns venue I/O and state transitions.  Keeping the admission
and optimizer math here makes the policy easy to test without a live venue and
prevents execution telemetry from becoming a second, hidden risk gate.
"""
from __future__ import annotations

import math
from typing import Mapping, Optional


ROOM_CANDIDATES_BPS: tuple[float, ...] = (
    0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0,
)
FILL_BUCKETS: tuple[str, ...] = ("0-2", "2-4", "4-6", "6-8", "8-10", "10+")


def fill_probability_bucket(room_bps: float) -> str:
    """Map execution room to the stable telemetry buckets used by the plan."""
    value = max(float(room_bps), 0.0)
    if value < 2.0:
        return "0-2"
    if value < 4.0:
        return "2-4"
    if value < 6.0:
        return "4-6"
    if value < 8.0:
        return "6-8"
    if value < 10.0:
        return "8-10"
    return "10+"


def beta_fill_probability(
        attempts: int, fills: int, *, prior_attempts: float = 2.0,
        prior_fills: float = 1.0) -> float:
    """Return a conservative Beta-smoothed fill probability.

    The prior is intentionally tiny.  It prevents a single zero-fill sample
    from making a room look permanently unusable while still allowing live
    observations to dominate quickly.
    """
    a = max(float(attempts), 0.0)
    f = min(max(float(fills), 0.0), a)
    pa = max(float(prior_attempts), 0.0)
    pf = min(max(float(prior_fills), 0.0), pa)
    total = a + pa
    return (f + pf) / total if total > 0.0 else 0.0


def _bucket_stats(stats: Optional[Mapping[str, Mapping[str, int]]],
                  bucket: str) -> tuple[int, int]:
    row = (stats or {}).get(bucket) or {}
    attempts = max(int(row.get("attempts") or 0), 0)
    fills = min(max(int(row.get("fills") or 0), 0), attempts)
    return attempts, fills


def choose_execution_room(
        *, phase: str, affordable_room_bps: float, hard_cap_bps: float,
        fill_stats: Optional[Mapping[str, Mapping[str, int]]] = None,
        base_cycle_pnl_bps: float = 0.0, capital_lock_sec: float = 1.0,
        fallback_open_bps: float = 3.0,
        fallback_close_bps: float = 2.0,
        expected_incremental_cost_bps: float = 0.0) -> dict:
    """Choose the room with the best observed fill-probability-adjusted EV.

    ``affordable_room_bps`` is an economic budget, while ``hard_cap_bps`` is
    the venue/physical ceiling.  Neither can be widened by the optimizer.
    With no observations we use the plan's small deterministic fallback.
    """
    affordable = max(float(affordable_room_bps), 0.0)
    hard_cap = max(float(hard_cap_bps), 0.0)
    cap = min(affordable, hard_cap)
    phase_name = str(phase).lower()
    fallback = (fallback_close_bps if "close" in phase_name
                or "reduce" in phase_name else fallback_open_bps)
    candidates = [room for room in ROOM_CANDIDATES_BPS if room <= cap + 1e-9]
    if not candidates and cap > 0.0:
        candidates = [cap]
    if not candidates:
        return {
            "room_bps": 0.0,
            "fill_probability": 0.0,
            "score": 0.0,
            "bucket": fill_probability_bucket(0.0),
            "cap_bps": cap,
            "reason": "no_affordable_room",
        }

    incremental_cost = max(float(expected_incremental_cost_bps), 0.0)
    lock = max(float(capital_lock_sec), 1e-6)

    def score(room: float, probability: float) -> tuple[float, float]:
        # The caller supplies the cycle EV before this phase's incremental
        # execution cost.  IOC room is a price/fill choice, not a realized
        # cost, so never subtract the room itself from EV.
        conditional_cost = min(incremental_cost, max(float(room), 0.0))
        return (probability * (float(base_cycle_pnl_bps) - conditional_cost)
                / lock, conditional_cost)

    has_observations = any(_bucket_stats(fill_stats, bucket)[0] > 0
                           for bucket in FILL_BUCKETS)
    if not has_observations:
        room = min(max(float(fallback), 0.0), cap)
        # Prefer a configured candidate when one is available; this keeps the
        # initial behavior stable and avoids inventing a precision level.
        eligible = [item for item in candidates if item <= room + 1e-9]
        room = max(eligible) if eligible else min(candidates)
        bucket = fill_probability_bucket(room)
        probability = beta_fill_probability(0, 0)
        room_score, conditional_cost = score(room, probability)
        return {
            "room_bps": room,
            "fill_probability": probability,
            "score": room_score,
            "expected_incremental_cost_bps": conditional_cost,
            "bucket": bucket,
            "cap_bps": cap,
            "reason": "fallback_no_samples",
        }

    scored: list[tuple[float, float, float, str, float]] = []
    for room in candidates:
        bucket = fill_probability_bucket(room)
        attempts, fills = _bucket_stats(fill_stats, bucket)
        probability = beta_fill_probability(attempts, fills)
        room_score, conditional_cost = score(room, probability)
        # Candidates in one telemetry bucket share a fill estimate. Prefer
        # the wider room in that tie so an IOC spends its available budget.
        scored.append((room_score, room, probability, bucket,
                       conditional_cost))
    best_score, room, probability, bucket, conditional_cost = max(scored)
    return {
        "room_bps": room,
        "fill_probability": probability,
        "score": best_score,
        "expected_incremental_cost_bps": conditional_cost,
        "bucket": bucket,
        "cap_bps": cap,
        "reason": "empirical_ev",
    }


def dynamic_exit_floor_bps(held_sec: float) -> float:
    """Return the time-decaying economic exit floor from the plan."""
    held = max(float(held_sec), 0.0)
    if held < 30.0:
        return 0.0
    if held < 120.0:
        return -0.5
    if held < 300.0:
        return -1.0
    return -2.0


def positive_close_budget_bps(net_profit_usd: float, floor_usd: float,
                              notional_usd: float) -> float:
    """Convert only positive remaining cycle budget into close room."""
    notional = float(notional_usd)
    if not all(math.isfinite(float(value)) for value in (
            net_profit_usd, floor_usd, notional)) or notional <= 0.0:
        return 0.0
    return max((float(net_profit_usd) - float(floor_usd)) / notional * 1e4,
               0.0)


def is_clean_circle(record: Mapping[str, object]) -> bool:
    """Classify a completed cycle without mixing recovery into clean KPIs."""
    if record.get("status") != "completed":
        return False
    if any(record.get(field) is None for field in (
            "open_first_fill_ts", "hedge_completed_ts",
            "reduce_first_fill_ts", "flat_ts")):
        return False
    if int(record.get("recovery_count") or 0) > 0:
        return False
    if bool(record.get("emergency_flag")):
        return False
    mode = str(record.get("exit_mode") or "").strip().lower()
    reason = str(record.get("exit_reason") or "").strip().lower()
    return mode not in {"manual", "manual_flatten", "emergency", "forced",
                        "execution_emergency"} and reason not in {
                            "manual_flatten", "emergency", "forced"}
