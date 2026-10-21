"""Small, restart-safe execution cost model.

The order path has two different responsibilities:

* a physical bound prevents an order from crossing an unsafe price; and
* an economic reserve estimates what a normal order is likely to cost.

Keeping those values separate prevents a rare worst-case bound from being
charged as if it were the expected cost of every opportunity.  This module is
deliberately dependency-free and bounded so it is safe to keep running for
weeks.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional


PHASES = ("open_first", "open_interleg", "open_hedge",
          "close_first", "close_interleg", "close_hedge")


def _finite(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _quantile(values: list[float], percentile: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    p = min(max(float(percentile), 0.0), 1.0)
    if len(ordered) == 1:
        return ordered[0]
    position = p * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


@dataclass(frozen=True)
class ExecutionCostObservation:
    phase: str
    execution_id: str
    ts: float
    requested_qty: float
    filled_qty: float
    is_buy: Optional[bool] = None
    quoted_top_px: Optional[float] = None
    quoted_depth_avg_px: Optional[float] = None
    quoted_marginal_px: Optional[float] = None
    limit_px: Optional[float] = None
    avg_fill_px: Optional[float] = None
    latency_sec: Optional[float] = None
    # The economic model uses only the incremental cost from the quoted
    # depth-weighted average to the realised average fill. ``all_in`` remains
    # available for attribution and compatibility with the previous schema.
    incremental_execution_cost_bps: Optional[float] = None
    all_in_execution_cost_bps: Optional[float] = None
    realized_vs_marginal_bps: Optional[float] = None
    realized_slippage_bps: Optional[float] = None
    latency_adverse_move_bps: Optional[float] = None
    depth_cost_bps: Optional[float] = None
    outcome: str = "unknown"
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def total_cost_bps(self) -> Optional[float]:
        value = (_finite(self.all_in_execution_cost_bps)
                 if self.all_in_execution_cost_bps is not None
                 else _finite(self.realized_slippage_bps))
        return max(value, 0.0) if value is not None else None

    @property
    def all_in_cost_bps(self) -> Optional[float]:
        """Compatibility alias for the pre-final-plan field name."""
        return self.all_in_execution_cost_bps

    @property
    def adverse_move_bps(self) -> Optional[float]:
        """Compatibility alias for the pre-final-plan field name."""
        return self.latency_adverse_move_bps

    @property
    def incremental_cost_bps(self) -> Optional[float]:
        """Cost charged by economics, excluding the already-quoted depth."""
        value = _finite(self.incremental_execution_cost_bps)
        # New rows always set the canonical field. This fallback keeps rows
        # produced by a compatible writer readable when it persisted the
        # depth-average quote but not the derived value.
        if value is None and self.quoted_depth_avg_px is not None:
            value = _finite(self.realized_vs_marginal_bps)
        return max(value, 0.0) if value is not None else None


class ExecutionCostModel:
    """Bounded per-phase observations with conservative quantile estimates."""

    def __init__(self, ledger=None, *, max_observations: int = 200) -> None:
        self.ledger = ledger
        self.max_observations = max(int(max_observations), 20)
        self._observations: dict[str, deque[ExecutionCostObservation]] = {
            phase: deque(maxlen=self.max_observations) for phase in PHASES
        }

    @staticmethod
    def _phase(phase: str) -> str:
        value = str(phase or "").strip().lower()
        if value not in PHASES:
            raise ValueError(f"unsupported execution-cost phase: {phase!r}")
        return value

    def restore(self) -> int:
        """Restore the latest bounded sample window from SQLite."""
        if self.ledger is None:
            return 0
        for phase in PHASES:
            self._observations[phase].clear()
        restored = 0
        for phase in PHASES:
            try:
                rows = self.ledger.load_execution_cost_observations(
                    phases=[phase], limit=self.max_observations)
            except Exception:
                continue
            for row in reversed(rows):
                try:
                    row_phase = self._phase(row.get("phase"))
                    if row_phase != phase:
                        continue
                    metadata = row.get("metadata")
                    if not isinstance(metadata, dict):
                        metadata = {}
                    raw_is_buy = row.get("is_buy")
                    if raw_is_buy is None:
                        raw_is_buy = metadata.get("is_buy")
                    is_buy = (None if raw_is_buy is None else
                              bool(int(raw_is_buy)))
                except (TypeError, ValueError):
                    continue
                try:
                    self._observations[phase].append(
                        ExecutionCostObservation(
                            phase=row_phase,
                            execution_id=str(row.get("execution_id") or ""),
                            ts=float(row.get("ts") or 0.0),
                            requested_qty=max(float(
                                row.get("requested_qty") or 0.0), 0.0),
                            filled_qty=max(float(
                                row.get("filled_qty") or 0.0), 0.0),
                            is_buy=is_buy,
                            quoted_top_px=_finite(row.get("quoted_top_px")),
                            quoted_depth_avg_px=_finite(
                                row.get("quoted_depth_avg_px")),
                            quoted_marginal_px=_finite(
                                row.get("quoted_marginal_px")),
                            limit_px=_finite(row.get("limit_px")),
                            avg_fill_px=_finite(row.get("avg_fill_px")),
                            latency_sec=_finite(row.get("latency_sec")),
                            incremental_execution_cost_bps=_finite(
                                row.get("incremental_execution_cost_bps")),
                            all_in_execution_cost_bps=_finite(
                                row.get("all_in_execution_cost_bps")
                                if row.get("all_in_execution_cost_bps") is not None
                                else row.get("all_in_cost_bps")),
                            realized_vs_marginal_bps=_finite(
                                row.get("realized_vs_marginal_bps")),
                            realized_slippage_bps=_finite(
                                row.get("realized_slippage_bps")),
                            latency_adverse_move_bps=_finite(
                                row.get("latency_adverse_move_bps")
                                if row.get("latency_adverse_move_bps") is not None
                                else row.get("adverse_move_bps")),
                            depth_cost_bps=_finite(
                                row.get("depth_cost_bps")),
                            outcome=str(row.get("outcome") or "unknown"),
                            metadata=metadata,
                        ))
                    restored += 1
                except (TypeError, ValueError):
                    continue
        return restored

    def observe(self, *, phase: str, execution_id: str, requested_qty: float,
                filled_qty: float, quoted_top_px: Any = None,
                quoted_depth_avg_px: Any = None,
                quoted_marginal_px: Any = None, limit_px: Any = None,
                avg_fill_px: Any = None, latency_sec: Any = None,
                adverse_move_bps: Any = None, outcome: str = "unknown",
                ts: Optional[float] = None,
                is_buy: Optional[bool] = None,
                metadata: Optional[dict[str, Any]] = None
                ) -> ExecutionCostObservation:
        phase = self._phase(phase)
        requested = max(_finite(requested_qty) or 0.0, 0.0)
        filled = max(_finite(filled_qty) or 0.0, 0.0)
        top = _finite(quoted_top_px)
        depth_avg = _finite(quoted_depth_avg_px)
        marginal = _finite(quoted_marginal_px)
        limit = _finite(limit_px)
        average = _finite(avg_fill_px)
        latency = _finite(latency_sec)
        adverse = _finite(adverse_move_bps)
        # Older direct integrations supplied only a marginal quote. Treat it
        # as the depth average for that observation; live execution supplies
        # the actual walk-depth average explicitly.
        if depth_avg is None or depth_avg <= 0:
            depth_avg = marginal if marginal is not None and marginal > 0 else top
        all_in = None
        incremental = None
        realized_vs_marginal = None
        if (top is not None and top > 0 and average is not None
                and average > 0):
            if is_buy:
                all_in = (average / top - 1.0) * 1e4
            else:
                all_in = (1.0 - average / top) * 1e4
            if not math.isfinite(all_in):
                all_in = None
            elif all_in < 0:
                # Favorable execution is useful context, but it should not
                # create a negative reserve that makes future admission
                # look safer than the observed worst-side cost.
                all_in = 0.0
        fill_reference = (marginal if marginal is not None and marginal > 0
                          else top)
        if (fill_reference is not None and fill_reference > 0
                and average is not None and average > 0):
            if is_buy:
                realized_vs_marginal = (average / fill_reference - 1.0) * 1e4
            else:
                realized_vs_marginal = (1.0 - average / fill_reference) * 1e4
            if not math.isfinite(realized_vs_marginal):
                realized_vs_marginal = None
            else:
                realized_vs_marginal = max(realized_vs_marginal, 0.0)
        if (depth_avg is not None and depth_avg > 0
                and average is not None and average > 0):
            if is_buy:
                incremental = (average / depth_avg - 1.0) * 1e4
            else:
                incremental = (1.0 - average / depth_avg) * 1e4
            if not math.isfinite(incremental):
                incremental = None
            else:
                incremental = max(incremental, 0.0)
        depth = None
        if top is not None and top > 0 and depth_avg is not None and depth_avg > 0:
            if is_buy:
                depth = (depth_avg / top - 1.0) * 1e4
            else:
                depth = (1.0 - depth_avg / top) * 1e4
            if not math.isfinite(depth):
                depth = None
        observation = ExecutionCostObservation(
            phase=phase,
            execution_id=str(execution_id or ""),
            ts=_finite(ts) or 0.0,
            requested_qty=requested,
            filled_qty=filled,
            is_buy=is_buy,
            quoted_top_px=top,
            quoted_depth_avg_px=depth_avg,
            quoted_marginal_px=marginal,
            limit_px=limit,
            avg_fill_px=average,
            latency_sec=(max(latency, 0.0) if latency is not None else None),
            incremental_execution_cost_bps=incremental,
            all_in_execution_cost_bps=all_in,
            realized_vs_marginal_bps=realized_vs_marginal,
            # Compatibility readers still consume this name.  New rows use
            # the canonical top-of-book all-in definition.
            realized_slippage_bps=all_in,
            latency_adverse_move_bps=(max(adverse, 0.0)
                                      if adverse is not None else None),
            depth_cost_bps=(max(depth, 0.0) if depth is not None else None),
            outcome=str(outcome or "unknown"),
            metadata=dict(metadata or {}),
        )
        self._observations[phase].append(observation)
        if self.ledger is not None:
            try:
                self.ledger.record_execution_cost_observation(observation)
            except Exception:
                # The trading path already has its own fail-closed ledger
                # checkpoint. A best-effort model sample must not turn a
                # successful order response into a duplicate retry.
                pass
        return observation

    def _phase_observations(self, phase: str) -> list[ExecutionCostObservation]:
        return list(self._observations[self._phase(phase)])

    def expected_cost_bps(self, phase: str, percentile: float = 0.75,
                          fallback: float = 0.0, *, min_samples: int = 3) -> float:
        """Legacy point estimate retained for older integrations.

        The engine uses :meth:`estimate`, which blends a live cold-start prior
        with empirical incremental fills. Keeping this method's old minimum-sample
        semantics avoids surprising downstream integrations that use it for a
        display-only value.
        """
        samples = [item.incremental_cost_bps for item in self._phase_observations(phase)
                   if item.filled_qty > 0]
        usable = [value for value in samples if value is not None and math.isfinite(value)]
        if len(usable) < max(int(min_samples), 1):
            return max(_finite(fallback) or 0.0, 0.0)
        return max(_quantile(usable, percentile) or 0.0, 0.0)

    def estimate(self, phase: str, *, percentile: float = 0.75,
                 prior_bps: Optional[float] = None,
                 live_depth_bps: float = 0.0,
                 latency_adverse_bps: float = 0.0,
                 small_prior_bps: float = 0.0,
                 full_weight_samples: Optional[int] = None,
                 now: Optional[float] = None, is_buy: Optional[bool] = None,
                 qty: float = 0.0) -> dict[str, Any]:
        """Estimate one phase's incremental cost with a bounded cold-start blend.

        ``prior_bps`` is an economic prior: latency adverse movement plus a
        small residual. Depth is already present in the projected cash flow
        through the walk-depth quote and is never charged here. Live estimates age observations with a 30-minute half-life and retain
        at least 20% live-prior weight; fewer fills are blended linearly so a restart cannot jump from a fixed fallback
        straight to a noisy single sample.
        """
        phase = self._phase(phase)
        observations = self._phase_observations(phase)
        samples = []
        for item in observations:
            value = item.incremental_cost_bps
            if item.filled_qty <= 0 or value is None or not math.isfinite(value):
                continue
            if is_buy is not None and item.is_buy is not None and item.is_buy != is_buy:
                continue
            if qty > 0 and item.requested_qty > 0 and not (
                    0.25 <= qty / item.requested_qty <= 4.0):
                continue
            age = max(float(now) - item.ts, 0.0) if now is not None else 0.0
            if age > 21600.0:
                continue
            samples.append((value, 2.0 ** (-age / 1800.0)))
        values = [value for value, _ in samples]
        effective_samples = sum(weight for _, weight in samples)
        depth = max(_finite(live_depth_bps) or 0.0, 0.0)
        adverse = max(_finite(latency_adverse_bps) or 0.0, 0.0)
        residual = max(_finite(small_prior_bps) or 0.0, 0.0)
        prior = (_finite(prior_bps) if prior_bps is not None
                 else adverse + residual)
        prior = max(prior or 0.0, 0.0)
        empirical = _quantile(values, percentile)
        if now is not None and samples:
            # Time-weighted quantile: old expensive fills cannot dominate a
            # quiet current market forever merely because no new orders fill.
            target = min(max(float(percentile), 0.0), 1.0) * effective_samples
            cumulative = 0.0
            for value, sample_weight in sorted(samples):
                cumulative += sample_weight
                if cumulative >= target:
                    empirical = value
                    break
        tail = empirical
        empirical = (sum(value * weight for value, weight in samples) / effective_samples
                     if effective_samples > 0 else None)
        sample_cap = max(int(full_weight_samples or 10), 1)
        weight = min(effective_samples / sample_cap, 0.8)
        expected = (prior if empirical is None else
                    weight * max(empirical, 0.0)
                    + (1.0 - weight) * prior)
        if not values:
            source = "cold_start_prior"
        elif weight < 1.0:
            source = "blended"
        else:
            source = "empirical"
        return {
            "phase": phase,
            "expected_cost_bps": max(expected, 0.0),
            "expected_incremental_execution_cost_bps": max(expected, 0.0),
            "prior_cost_bps": prior,
            "prior_incremental_execution_cost_bps": prior,
            "empirical_cost_bps": (max(empirical, 0.0)
                                    if empirical is not None else None),
            "empirical_incremental_execution_cost_bps": (
                max(empirical, 0.0) if empirical is not None else None),
            "empirical_tail_cost_bps": tail,
            "empirical_tail_percentile": percentile,
            "estimator": "time_weighted_mean_with_live_prior",
            "empirical_weight": weight,
            "effective_samples": effective_samples,
            "live_weight": 1.0 - weight,
            "usable_samples": len(values),
            "empirical_full_weight_samples": sample_cap,
            "source": source,
            "live_depth_bps": depth,
            "latency_adverse_bps": adverse,
            "small_prior_bps": residual,
        }

    def stats(self, phase: str, *,
              full_weight_samples: Optional[int] = None) -> dict[str, Any]:
        observations = self._phase_observations(phase)
        # A rejected/no-fill IOC tells us about fill rate, but not the cost of
        # a completed leg. Keep it in observation_count while excluding it
        # from the economic percentile model.
        incremental_values = [item.incremental_cost_bps for item in observations
                              if item.filled_qty > 0
                              and item.incremental_cost_bps is not None]
        latency_values = [item.latency_sec for item in observations
                          if item.latency_sec is not None]
        realized_values = [item.realized_slippage_bps for item in observations
                           if item.realized_slippage_bps is not None]
        all_in_values = [item.total_cost_bps for item in observations
                         if item.filled_qty > 0 and item.total_cost_bps is not None]
        marginal_values = [item.realized_vs_marginal_bps for item in observations
                           if item.realized_vs_marginal_bps is not None]
        adverse_values = [item.adverse_move_bps for item in observations
                          if item.adverse_move_bps is not None]
        depth_values = [item.depth_cost_bps for item in observations
                        if item.depth_cost_bps is not None]

        def q(values: list[float], p: float) -> Optional[float]:
            return _quantile(values, p)

        filled_count = sum(item.filled_qty > 0 for item in observations)
        usable_samples = len(incremental_values)
        sample_cap = max(int(full_weight_samples or 10), 1)
        source = "cold_start_prior" if usable_samples == 0 else "historical_summary"
        return {
            "phase": phase,
            "observation_count": len(observations),
            "filled_count": filled_count,
            "fill_rate": filled_count / len(observations) if observations else None,
            # ``ready`` remains the historical >=3 display/recovery flag.
            # ``empirical_ready`` is the economic model's >=10 threshold.
            "ready": usable_samples >= 3,
            "empirical_ready": usable_samples >= sample_cap,
            "usable_samples": usable_samples,
            "source": source,
            "empirical_weight": min(usable_samples / sample_cap, 0.8),
            "empirical_full_weight_samples": sample_cap,
            # Historical summary, not the direction/size/live-prior estimate
            # used by admission (that estimate is in the decision budget).
            "expected_cost_bps": (sum(incremental_values) / usable_samples
                                  if usable_samples else None),
            "empirical_cost_bps": (sum(incremental_values) / usable_samples
                                   if usable_samples else None),
            "p50_incremental_execution_cost_bps": q(incremental_values, .50),
            "p75_incremental_execution_cost_bps": q(incremental_values, .75),
            "p90_incremental_execution_cost_bps": q(incremental_values, .90),
            "p95_incremental_execution_cost_bps": q(incremental_values, .95),
            "prior_cost_bps": None,
            "p50_all_in_cost_bps": q(all_in_values, .50),
            "p75_all_in_cost_bps": q(all_in_values, .75),
            "p90_all_in_cost_bps": q(all_in_values, .90),
            "p95_all_in_cost_bps": q(all_in_values, .95),
            "p50_all_in_execution_cost_bps": q(all_in_values, .50),
            "p75_all_in_execution_cost_bps": q(all_in_values, .75),
            "p90_all_in_execution_cost_bps": q(all_in_values, .90),
            "p95_all_in_execution_cost_bps": q(all_in_values, .95),
            "p50_cost_bps": q(incremental_values, .50),
            "p75_cost_bps": q(incremental_values, .75),
            "p90_cost_bps": q(incremental_values, .90),
            "p95_cost_bps": q(incremental_values, .95),
            "p50_latency_sec": q(latency_values, .50),
            "p75_latency_sec": q(latency_values, .75),
            "p90_latency_sec": q(latency_values, .90),
            "p95_latency_sec": q(latency_values, .95),
            "p50_realized_slippage_bps": q(realized_values, .50),
            "p75_realized_slippage_bps": q(realized_values, .75),
            "p90_realized_slippage_bps": q(realized_values, .90),
            "p95_realized_slippage_bps": q(realized_values, .95),
            "p50_realized_vs_marginal_bps": q(marginal_values, .50),
            "p75_realized_vs_marginal_bps": q(marginal_values, .75),
            "p90_realized_vs_marginal_bps": q(marginal_values, .90),
            "p95_realized_vs_marginal_bps": q(marginal_values, .95),
            "p50_adverse_move_bps": q(adverse_values, .50),
            "p75_adverse_move_bps": q(adverse_values, .75),
            "p90_adverse_move_bps": q(adverse_values, .90),
            "p95_adverse_move_bps": q(adverse_values, .95),
            "p99_adverse_move_bps": q(adverse_values, .99),
            "p50_latency_adverse_move_bps": q(adverse_values, .50),
            "p75_latency_adverse_move_bps": q(adverse_values, .75),
            "p90_latency_adverse_move_bps": q(adverse_values, .90),
            "p95_latency_adverse_move_bps": q(adverse_values, .95),
            "p99_latency_adverse_move_bps": q(adverse_values, .99),
            "p95_depth_cost_bps": q(depth_values, .95),
        }

    def snapshot(self, *, full_weight_samples: Optional[int] = None) -> dict[str, Any]:
        phases = {phase: self.stats(
            phase, full_weight_samples=full_weight_samples)
                  for phase in PHASES}
        total = sum(item["observation_count"] for item in phases.values())
        return {
            "ready": any(item["ready"] for item in phases.values()),
            "observation_count": total,
            "max_observations_per_phase": self.max_observations,
            "phases": phases,
        }
