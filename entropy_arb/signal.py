"""Pure strategy state and robust rolling-premium model.

This module deliberately has no venue or wall-clock dependencies so the live
engine and chronological replay use exactly the same signal semantics.
"""
from __future__ import annotations

import math
import statistics
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Optional


class PairState(str, Enum):
    FLAT = "flat"
    SHORT_ENTROPY = "short_entropy"
    LONG_ENTROPY = "long_entropy"
    UNBALANCED = "unbalanced"


class StrategyAction(str, Enum):
    OPEN = "open"
    ADD = "add"
    REDUCE = "reduce"


class PremiumDataTier(str, Enum):
    """Quality of the current cross-venue market-data observation."""

    GOOD = "GOOD"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"


class PremiumReadiness(str, Enum):
    """How much rolling history is available for the reference model."""

    UNAVAILABLE = "UNAVAILABLE"
    COLD_DEGRADED = "COLD_DEGRADED"
    NORMAL = "NORMAL"


@dataclass(frozen=True)
class PremiumSampleQuality:
    """Quality metadata for one cross-venue premium observation."""

    valid: bool
    entropy_age_ms: Optional[float]
    hedge_age_ms: Optional[float]
    cross_venue_skew_ms: Optional[float]
    reason: Optional[str] = None
    tier: Optional[PremiumDataTier] = None
    uncertainty_bps: float = 0.0

    def __post_init__(self) -> None:
        # Keep the historical five-argument constructor source-compatible.
        # A caller that only supplied ``valid`` still gets an explicit tier.
        tier = self.tier
        if tier is None:
            tier = (PremiumDataTier.GOOD if self.valid
                    else PremiumDataTier.INVALID)
            object.__setattr__(self, "tier", tier)
        elif not isinstance(tier, PremiumDataTier):
            try:
                object.__setattr__(self, "tier", PremiumDataTier(str(tier)))
            except ValueError:
                object.__setattr__(self, "tier", PremiumDataTier.INVALID)

    @property
    def data_tier(self) -> PremiumDataTier:
        """Compatibility/readability alias for the tier field."""
        return self.tier or PremiumDataTier.INVALID

    @property
    def degraded(self) -> bool:
        return self.data_tier == PremiumDataTier.DEGRADED


def check_premium_sample_quality(
        now: float, entropy_book_ts: Optional[float],
        hedge_book_ts: Optional[float], max_book_age_ms: float,
        max_cross_venue_skew_ms: float) -> PremiumSampleQuality:
    """Classify book age/skew without treating near-threshold data as absent.

    The configured limits describe the GOOD tier.  A bounded degraded band is
    still usable and is priced by the engine as data uncertainty; only a
    clearly stale/malformed observation is INVALID.  This keeps a brief feed
    timing wobble from becoming a global trading veto while preserving a hard
    data boundary.
    """
    def age_ms(value: Optional[float]) -> Optional[float]:
        try:
            ts = float(value)
            current = float(now)
        except (TypeError, ValueError):
            return None
        if not (math.isfinite(ts) and math.isfinite(current)):
            return None
        return max(current - ts, 0.0) * 1000.0

    entropy_age = age_ms(entropy_book_ts)
    hedge_age = age_ms(hedge_book_ts)
    if entropy_age is None:
        return PremiumSampleQuality(
            False, None, hedge_age, None, "entropy_book_missing",
            PremiumDataTier.INVALID)
    if hedge_age is None:
        return PremiumSampleQuality(
            False, entropy_age, None, None, "hedge_book_missing",
            PremiumDataTier.INVALID)
    skew = abs(float(entropy_book_ts) - float(hedge_book_ts)) * 1000.0
    max_age = max(float(max_book_age_ms), 1e-9)
    max_skew = max(float(max_cross_venue_skew_ms), 0.0)
    # The degraded band is deliberately finite.  The wider hard boundary is
    # still conservative for the live order path, which has its own book
    # freshness check as an additional physical safety control.
    hard_age = max_age * 2.0
    hard_skew = max(max_skew * 2.0, max_skew + 500.0)
    if entropy_age > hard_age:
        return PremiumSampleQuality(False, entropy_age, hedge_age, skew,
                                    "entropy_book_stale",
                                    PremiumDataTier.INVALID)
    if hedge_age > hard_age:
        return PremiumSampleQuality(False, entropy_age, hedge_age, skew,
                                    "hedge_book_stale",
                                    PremiumDataTier.INVALID)
    if skew > hard_skew:
        return PremiumSampleQuality(False, entropy_age, hedge_age, skew,
                                    "cross_venue_skew",
                                    PremiumDataTier.INVALID)

    degraded_age = max_age * 0.75
    degraded_skew = max_skew * 0.75 if max_skew > 0 else math.inf
    tier = (PremiumDataTier.DEGRADED
            if max(entropy_age, hedge_age) >= degraded_age
            or skew >= degraded_skew else PremiumDataTier.GOOD)
    reason = None
    if tier == PremiumDataTier.DEGRADED:
        if max(entropy_age, hedge_age) >= degraded_age:
            reason = ("entropy_book_stale" if entropy_age >= hedge_age
                      else "hedge_book_stale")
        elif skew >= degraded_skew:
            reason = "cross_venue_skew"
    age_ratio = max(entropy_age, hedge_age) / max_age
    skew_ratio = skew / max_skew if max_skew > 0 else 0.0
    uncertainty = 0.0
    if tier == PremiumDataTier.DEGRADED:
        uncertainty = min(3.0, max(0.5, max(age_ratio, skew_ratio)))
    return PremiumSampleQuality(True, entropy_age, hedge_age, skew, reason,
                                tier, uncertainty)


@dataclass
class PremiumDataHealth:
    """Track whether valid premium samples have stopped arriving."""

    degraded_after_sec: float
    last_valid_ts: Optional[float] = None
    invalid_since: Optional[float] = None
    last_quality: Optional[PremiumSampleQuality] = None
    valid_count: int = 0
    invalid_count: int = 0
    current_tier: PremiumDataTier = PremiumDataTier.INVALID
    current_uncertainty_bps: float = 0.0

    def observe(self, now: float, quality: PremiumSampleQuality) -> None:
        now = float(now)
        self.last_quality = quality
        self.current_tier = quality.data_tier
        self.current_uncertainty_bps = max(
            float(quality.uncertainty_bps or 0.0), 0.0)
        if quality.valid:
            self.last_valid_ts = now
            self.invalid_since = None
            self.valid_count += 1
            return
        if self.invalid_since is None:
            self.invalid_since = now
        self.invalid_count += 1

    def uncertainty_bps(self, now: Optional[float] = None) -> float:
        """Return current data uncertainty; invalid data is a hard gate."""
        if self.current_tier == PremiumDataTier.INVALID:
            return 0.0
        return self.current_uncertainty_bps

    def is_degraded(self, now: float) -> bool:
        return (self.invalid_since is not None
                and float(now) - self.invalid_since >= self.degraded_after_sec)

    def invalid_duration_sec(self, now: float) -> float:
        if self.invalid_since is None:
            return 0.0
        return max(float(now) - self.invalid_since, 0.0)


class MidlineLagState(str, Enum):
    NORMAL = "normal"
    WARNING = "warning"
    ACCELERATING = "accelerating"
    BLOCKED = "blocked"


def compute_midline_lag(short_center_bps: Optional[float],
                        active_midline_bps: float) -> Optional[float]:
    """Return the signed short-center minus active-midline lag."""
    if short_center_bps is None:
        return None
    try:
        value = float(short_center_bps) - float(active_midline_bps)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


@dataclass
class MidlineLagGuard:
    """Persistent lag state machine used by the rolling midline model."""

    enabled: bool = False
    warn_bps: float = 1.5
    block_bps: float = 3.0
    persist_sec: float = 900.0
    accelerate_bps: float = 2.0
    accelerate_persist_sec: float = 300.0
    recovery_sec: float = 90.0
    recovery_bps: float = 2.0
    state: MidlineLagState = MidlineLagState.NORMAL
    _accelerate_since: Optional[float] = None
    _block_since: Optional[float] = None
    _recovery_since: Optional[float] = None

    @property
    def lag_state(self) -> MidlineLagState:
        return self.state

    def reset(self) -> None:
        self.state = MidlineLagState.NORMAL
        self._accelerate_since = None
        self._block_since = None
        self._recovery_since = None

    def update(self, lag_bps: Optional[float], now: float) -> MidlineLagState:
        """Advance the guard while preserving persistence and hysteresis."""
        if not self.enabled:
            self.reset()
            return self.state
        try:
            lag = abs(float(lag_bps))
            now = float(now)
        except (TypeError, ValueError):
            lag = math.nan
        if not math.isfinite(lag) or not math.isfinite(now):
            # Missing data cannot make a blocked state safe; data health has a
            # separate gate for the case where the lag cannot be measured.
            if self.state != MidlineLagState.BLOCKED:
                self.reset()
            return self.state

        if self.state == MidlineLagState.BLOCKED:
            self._accelerate_since = None
            self._block_since = None
            if lag <= self.recovery_bps:
                if self._recovery_since is None:
                    self._recovery_since = now
                if now - self._recovery_since >= self.recovery_sec:
                    self.reset()
            else:
                self._recovery_since = None
            return self.state

        self._recovery_since = None
        if lag >= self.block_bps:
            if self._block_since is None:
                self._block_since = now
            if now - self._block_since >= self.persist_sec:
                self.state = MidlineLagState.BLOCKED
                self._accelerate_since = None
                return self.state
        else:
            self._block_since = None

        if lag >= self.accelerate_bps:
            if self._accelerate_since is None:
                self._accelerate_since = now
            if now - self._accelerate_since >= self.accelerate_persist_sec:
                self.state = MidlineLagState.ACCELERATING
            else:
                self.state = MidlineLagState.WARNING
        elif lag >= self.warn_bps:
            self._accelerate_since = None
            self.state = MidlineLagState.WARNING
        else:
            self._accelerate_since = None
            self.state = MidlineLagState.NORMAL
        return self.state


def compute_adaptive_entry_band(*, base_bps: float,
                                configured_min_bps: float,
                                robust_sigma_bps: Optional[float],
                                sigma_mult: float,
                                max_bps: float) -> float:
    """Use the bootstrap band only until a valid sigma is available.

    The configured minimum is the steady-state floor. Economic admission
    separately prices fees and execution, so a seed band need not permanently
    exclude quiet-market candidates.
    """
    floor_bps = max(float(configured_min_bps), 0.0)
    cap_bps = max(float(max_bps), floor_bps)
    if robust_sigma_bps is None or not math.isfinite(robust_sigma_bps):
        return min(max(float(base_bps), floor_bps), cap_bps)
    adaptive = max(float(robust_sigma_bps), 0.0) * float(sigma_mult)
    return min(max(floor_bps, adaptive), cap_bps)


def is_volatility_guarded(robust_sigma_bps: Optional[float],
                          max_sigma_bps: float) -> bool:
    """Whether OPEN/ADD must be blocked for extreme spread volatility."""
    return (robust_sigma_bps is not None
            and math.isfinite(robust_sigma_bps)
            and robust_sigma_bps > max_sigma_bps)


def compute_cycle_midline_drift(current_midline_bps: float,
                                entry_midline_bps: float) -> float:
    return float(current_midline_bps) - float(entry_midline_bps)


def is_cycle_add_blocked(drift_bps: float, max_drift_bps: float) -> bool:
    return abs(float(drift_bps)) > float(max_drift_bps)


@dataclass
class InventoryCycle:
    """Entry context for one real paired inventory cycle.

    Venue positions remain the source of truth.  This object only preserves
    the market context needed to evaluate ADD and anchored REDUCE decisions.
    """

    active: bool = False
    anchored: bool = False
    cycle_id: Optional[str] = None
    direction: Optional[str] = None
    opened_ts: Optional[float] = None
    entry_midline_bps: Optional[float] = None
    # ``entry_midline_bps`` is retained as a compatibility alias.  The
    # reference used by a cycle must also record which model coordinate it
    # came from; otherwise a dynamic-center entry is later compared with the
    # active/seed midline and its drift is fabricated after a restart.
    entry_reference_bps: Optional[float] = None
    entry_reference_mode: Optional[str] = None
    entry_active_midline_bps: Optional[float] = None
    entry_dynamic_center_bps: Optional[float] = None
    entry_sigma_bps: Optional[float] = None
    entry_band_bps: Optional[float] = None
    entry_threshold_bps: Optional[float] = None
    peak_paired_qty: float = 0.0
    add_count: int = 0
    reduce_count: int = 0
    # These fields intentionally survive ``reset()``.  A flat position still
    # needs the last completed risk exit to gate an immediate same-direction
    # re-entry after a restart.
    last_risk_exit_direction: Optional[str] = None
    last_risk_exit_ts: Optional[float] = None
    last_risk_exit_basis_bps: Optional[float] = None
    reentry_reset_seen: bool = False
    reentry_reset_ts: Optional[float] = None

    VALID_DIRECTIONS = frozenset({"short_entropy", "long_entropy"})

    def reset(self) -> None:
        self.active = False
        self.anchored = False
        self.cycle_id = None
        self.direction = None
        self.opened_ts = None
        self.entry_midline_bps = None
        self.entry_reference_bps = None
        self.entry_reference_mode = None
        self.entry_active_midline_bps = None
        self.entry_dynamic_center_bps = None
        self.entry_sigma_bps = None
        self.entry_band_bps = None
        self.entry_threshold_bps = None
        self.peak_paired_qty = 0.0
        self.add_count = 0
        self.reduce_count = 0

    def start(self, *, direction: str, opened_ts: float,
              entry_midline_bps: float,
              entry_sigma_bps: Optional[float],
              entry_band_bps: float, entry_threshold_bps: float,
              paired_qty: float, cycle_id: Optional[str] = None,
              entry_reference_bps: Optional[float] = None,
              entry_reference_mode: Optional[str] = None,
              entry_active_midline_bps: Optional[float] = None,
              entry_dynamic_center_bps: Optional[float] = None) -> None:
        if direction not in self.VALID_DIRECTIONS:
            raise ValueError(f"invalid cycle direction: {direction}")
        self.active = True
        self.anchored = True
        self.cycle_id = cycle_id
        self.direction = direction
        self.opened_ts = float(opened_ts)
        reference = (entry_midline_bps if entry_reference_bps is None
                     else entry_reference_bps)
        self.entry_midline_bps = float(reference)
        self.entry_reference_bps = float(reference)
        mode = str(entry_reference_mode or "active_midline")
        if mode not in {"active_midline", "dynamic_center",
                        "blended_midline", "configured_midline"}:
            mode = "active_midline"
        self.entry_reference_mode = mode
        self.entry_active_midline_bps = (
            None if entry_active_midline_bps is None
            else float(entry_active_midline_bps))
        self.entry_dynamic_center_bps = (
            None if entry_dynamic_center_bps is None
            else float(entry_dynamic_center_bps))
        self.entry_sigma_bps = (None if entry_sigma_bps is None
                                else float(entry_sigma_bps))
        self.entry_band_bps = float(entry_band_bps)
        self.entry_threshold_bps = float(entry_threshold_bps)
        self.peak_paired_qty = max(float(paired_qty), 0.0)
        self.add_count = 0
        self.reduce_count = 0

    def to_dict(self) -> dict:
        return {
            "active": self.active,
            "anchored": self.anchored,
            "cycle_id": self.cycle_id,
            "direction": self.direction,
            "opened_ts": self.opened_ts,
            "entry_midline_bps": self.entry_midline_bps,
            "entry_reference_bps": self.entry_reference_bps,
            "entry_reference_mode": self.entry_reference_mode,
            "entry_active_midline_bps": self.entry_active_midline_bps,
            "entry_dynamic_center_bps": self.entry_dynamic_center_bps,
            "entry_sigma_bps": self.entry_sigma_bps,
            "entry_band_bps": self.entry_band_bps,
            "entry_threshold_bps": self.entry_threshold_bps,
            "peak_paired_qty": self.peak_paired_qty,
            "add_count": self.add_count,
            "reduce_count": self.reduce_count,
            "last_risk_exit_direction": self.last_risk_exit_direction,
            "last_risk_exit_ts": self.last_risk_exit_ts,
            "last_risk_exit_basis_bps": self.last_risk_exit_basis_bps,
            "reentry_reset_seen": self.reentry_reset_seen,
            "reentry_reset_ts": self.reentry_reset_ts,
        }

    @classmethod
    def from_dict(cls, data: object) -> "InventoryCycle":
        if not isinstance(data, dict):
            return cls()

        def finite_or_none(value):
            try:
                number = float(value)
            except (TypeError, ValueError):
                return None
            return number if math.isfinite(number) else None

        risk_direction = data.get("last_risk_exit_direction")
        if risk_direction not in cls.VALID_DIRECTIONS:
            risk_direction = None
        risk_ts = finite_or_none(data.get("last_risk_exit_ts"))
        risk_basis = finite_or_none(data.get("last_risk_exit_basis_bps"))
        reset_ts = finite_or_none(data.get("reentry_reset_ts"))
        reentry_reset_seen = bool(data.get("reentry_reset_seen"))

        direction = data.get("direction")
        if direction not in cls.VALID_DIRECTIONS or not data.get("active"):
            return cls(
                last_risk_exit_direction=risk_direction,
                last_risk_exit_ts=risk_ts,
                last_risk_exit_basis_bps=risk_basis,
                reentry_reset_seen=reentry_reset_seen,
                reentry_reset_ts=reset_ts,
            )

        opened_ts = finite_or_none(data.get("opened_ts"))
        entry_reference = finite_or_none(
            data.get("entry_reference_bps", data.get("entry_midline_bps")))
        entry_midline = (entry_reference if entry_reference is not None
                         else finite_or_none(data.get("entry_midline_bps")))
        entry_active = finite_or_none(data.get("entry_active_midline_bps"))
        entry_dynamic = finite_or_none(data.get("entry_dynamic_center_bps"))
        entry_mode = str(data.get("entry_reference_mode") or "active_midline")
        if entry_mode not in {"active_midline", "dynamic_center",
                              "blended_midline", "configured_midline"}:
            entry_mode = "active_midline"
        entry_sigma = finite_or_none(data.get("entry_sigma_bps"))
        entry_band = finite_or_none(data.get("entry_band_bps"))
        entry_threshold = finite_or_none(data.get("entry_threshold_bps"))
        anchored = bool(data.get("anchored")) and all(
            value is not None
            for value in (opened_ts, entry_midline, entry_band,
                          entry_threshold))

        def nonnegative_float(value) -> float:
            number = finite_or_none(value)
            return max(number or 0.0, 0.0)

        def nonnegative_int(value) -> int:
            try:
                return max(int(value), 0)
            except (TypeError, ValueError):
                return 0

        return cls(
            active=True,
            anchored=anchored,
            cycle_id=(str(data["cycle_id"]) if data.get("cycle_id")
                      else None),
            direction=direction,
            opened_ts=opened_ts if anchored else None,
            entry_midline_bps=entry_midline if anchored else None,
            entry_reference_bps=entry_reference if anchored else None,
            entry_reference_mode=entry_mode if anchored else None,
            entry_active_midline_bps=entry_active if anchored else None,
            entry_dynamic_center_bps=entry_dynamic if anchored else None,
            entry_sigma_bps=entry_sigma if anchored else None,
            entry_band_bps=entry_band if anchored else None,
            entry_threshold_bps=entry_threshold if anchored else None,
            peak_paired_qty=nonnegative_float(data.get("peak_paired_qty")),
            add_count=nonnegative_int(data.get("add_count")),
            reduce_count=nonnegative_int(data.get("reduce_count")),
            last_risk_exit_direction=risk_direction,
            last_risk_exit_ts=risk_ts,
            last_risk_exit_basis_bps=risk_basis,
            reentry_reset_seen=reentry_reset_seen,
            reentry_reset_ts=reset_ts,
        )


def cycle_direction_for_pair_state(state: PairState) -> Optional[str]:
    if state == PairState.SHORT_ENTROPY:
        return "short_entropy"
    if state == PairState.LONG_ENTROPY:
        return "long_entropy"
    return None


def pair_state(entropy_pos: float, hedge_pos: float,
               tolerance: float) -> PairState:
    e_flat = abs(entropy_pos) <= tolerance
    h_flat = abs(hedge_pos) <= tolerance
    if e_flat and h_flat:
        return PairState.FLAT
    if entropy_pos < -tolerance and hedge_pos > tolerance:
        return PairState.SHORT_ENTROPY
    if entropy_pos > tolerance and hedge_pos < -tolerance:
        return PairState.LONG_ENTROPY
    return PairState.UNBALANCED


def paired_qty(entropy_pos: float, hedge_pos: float) -> float:
    return min(abs(entropy_pos), abs(hedge_pos))


def entry_threshold_short(midline_bps: float, upper_bps: float,
                          inventory_bps: float = 0.0,
                          funding_cost_bps: float = 0.0) -> float:
    return (midline_bps + upper_bps + inventory_bps
            )


def entry_threshold_long(midline_bps: float, lower_bps: float,
                         inventory_bps: float = 0.0,
                         funding_cost_bps: float = 0.0) -> float:
    return (lower_bps - midline_bps + inventory_bps
            )


def exit_threshold_short(midline_bps: float, exit_bps: float) -> float:
    return -(midline_bps + exit_bps)


def exit_threshold_long(midline_bps: float, exit_bps: float) -> float:
    return midline_bps - exit_bps


@dataclass(frozen=True)
class PremiumModelSnapshot:
    ts: float
    sample_count: int
    ready: bool
    short_center_bps: Optional[float]
    long_center_bps: Optional[float]
    mad_bps: Optional[float]
    robust_sigma_bps: Optional[float]
    raw_target_midline_bps: Optional[float]
    active_midline_bps: float
    shift_threshold_bps: Optional[float]
    regime_shift: bool
    regime_since: Optional[float]
    sigma_center_bps: Optional[float] = None
    sigma_sample_count: int = 0
    sigma_window_sec: Optional[float] = None
    midline_lag_bps: Optional[float] = None
    midline_lag_state: MidlineLagState = MidlineLagState.NORMAL
    midline_move_speed_bps_per_10m: float = 0.0
    readiness: PremiumReadiness = PremiumReadiness.UNAVAILABLE
    history_sec: float = 0.0
    latest_sample_age_sec: Optional[float] = None
    tradable: bool = False
    model_uncertainty_bps: float = 0.0

    @property
    def lag_state(self) -> MidlineLagState:
        return self.midline_lag_state

    @property
    def move_speed_bps_per_10m(self) -> float:
        return self.midline_move_speed_bps_per_10m

    @property
    def readiness_state(self) -> PremiumReadiness:
        return self.readiness

    @property
    def is_tradable(self) -> bool:
        # ``ready=True`` is retained as a compatibility escape hatch for
        # older snapshot fixtures that predate the explicit state machine.
        return bool(self.tradable or self.ready
                    or self.readiness != PremiumReadiness.UNAVAILABLE)


class RollingPremiumModel:
    def __init__(self, *, short_window_sec: float, long_window_sec: float,
                 warmup_sec: float, min_samples: int,
                 shift_min_bps: float, shift_sigma_mult: float,
                 recovery_ratio: float, recovery_sec: float,
                 seed_midline_bps: float,
                 max_move_bps_per_10m: Optional[float] = None,
                 sigma_window_sec: Optional[float] = None,
                 base_move_bps_per_10m: Optional[float] = None,
                 accelerated_move_bps_per_10m: Optional[float] = None,
                 absolute_max_move_bps_per_10m: Optional[float] = None,
                 midline_lag_guard_enabled: bool = False,
                 midline_lag_warn_bps: float = 1.5,
                 midline_lag_block_bps: float = 3.0,
                 midline_lag_persist_sec: float = 900.0,
                 midline_lag_accelerate_bps: float = 2.0,
                 midline_lag_accelerate_persist_sec: float = 300.0,
                 midline_lag_recovery_sec: float = 90.0,
                 midline_lag_recovery_bps: float = 2.0,
                 cold_start_warmup_sec: float = 180.0,
                 cold_start_min_samples: int = 120) -> None:
        self.short_window_sec = short_window_sec
        self.long_window_sec = long_window_sec
        self.warmup_sec = warmup_sec
        self.min_samples = min_samples
        self.cold_start_warmup_sec = max(float(cold_start_warmup_sec), 0.0)
        self.cold_start_min_samples = max(int(cold_start_min_samples), 1)
        self.shift_min_bps = shift_min_bps
        self.shift_sigma_mult = shift_sigma_mult
        legacy_move = (0.5 if max_move_bps_per_10m is None
                       else float(max_move_bps_per_10m))
        self.base_move_bps_per_10m = (
            legacy_move if base_move_bps_per_10m is None
            else float(base_move_bps_per_10m))
        self.accelerated_move_bps_per_10m = (
            self.base_move_bps_per_10m if accelerated_move_bps_per_10m is None
            else float(accelerated_move_bps_per_10m))
        self.absolute_max_move_bps_per_10m = (
            max(self.accelerated_move_bps_per_10m,
                self.base_move_bps_per_10m)
            if absolute_max_move_bps_per_10m is None
            else float(absolute_max_move_bps_per_10m))
        # Compatibility for integrations that still inspect the former single
        # speed attribute.  The model itself uses the three explicit speeds.
        self.max_move_bps_per_10m = self.base_move_bps_per_10m
        self.sigma_window_sec = (long_window_sec if sigma_window_sec is None
                                 else float(sigma_window_sec))
        self.recovery_ratio = recovery_ratio
        self.recovery_sec = recovery_sec
        self.seed_midline_bps = seed_midline_bps
        self._samples: deque[tuple[float, float]] = deque()
        self._sigma_samples: deque[tuple[float, float]] = deque()
        self._last_observed_ts: Optional[float] = None
        self._active = seed_midline_bps
        self._last_snapshot_ts: Optional[float] = None
        self._regime_since: Optional[float] = None
        self._recovery_since: Optional[float] = None
        self._prior_robust_sigma_bps: Optional[float] = None
        self._prior_midline_ts: Optional[float] = None
        self.lag_guard = MidlineLagGuard(
            enabled=midline_lag_guard_enabled,
            warn_bps=midline_lag_warn_bps,
            block_bps=midline_lag_block_bps,
            persist_sec=midline_lag_persist_sec,
            accelerate_bps=midline_lag_accelerate_bps,
            accelerate_persist_sec=midline_lag_accelerate_persist_sec,
            recovery_sec=midline_lag_recovery_sec,
            recovery_bps=midline_lag_recovery_bps)

    @property
    def has_prior(self) -> bool:
        """Whether a restored active reference can seed cold-start pricing."""
        return (self._prior_midline_ts is not None
                or abs(float(self._active) - float(self.seed_midline_bps))
                > 1e-12
                or self._prior_robust_sigma_bps is not None)

    def observe(self, ts: float, premium_bps: float) -> None:
        if not (math.isfinite(ts) and math.isfinite(premium_bps)):
            return
        if self._last_observed_ts is not None and ts <= self._last_observed_ts:
            return
        self._last_observed_ts = ts
        self._samples.append((ts, premium_bps))
        self._sigma_samples.append((ts, premium_bps))
        self._prune(ts)

    def bootstrap(self, samples: Iterable[tuple[float, float]]) -> None:
        # Recorder rows are chronological; consume the iterator directly so a
        # large seconds.csv never needs to be materialized in memory.
        for ts, premium in samples:
            self.observe(float(ts), float(premium))

    def merge_history(self, samples: Iterable[tuple[float, float]],
                      now: Optional[float] = None) -> int:
        """Merge historical observations, including rows older than the tail.

        A restored checkpoint may contain only the most recent few minutes.
        ``bootstrap()`` intentionally ignores out-of-order rows because it is
        the live append path, so using it for a cold-start backfill would
        leave the model with a short history and keep it in warmup.  This
        startup-only path rebuilds the two bounded deques in timestamp order,
        deduplicates second-level rows, and keeps the current active midline.
        """
        current = float(time.time() if now is None else now)
        if not math.isfinite(current):
            return 0
        merged = {ts: premium for ts, premium in self._samples
                  if math.isfinite(ts) and math.isfinite(premium)}
        added = 0
        for row in samples:
            if not isinstance(row, (list, tuple)) or len(row) != 2:
                continue
            try:
                ts, premium = float(row[0]), float(row[1])
            except (TypeError, ValueError):
                continue
            if not (math.isfinite(ts) and math.isfinite(premium)):
                continue
            if ts > current:
                continue
            if ts not in merged:
                added += 1
            merged[ts] = premium
        if not added:
            self._prune(current)
            return 0
        ordered = sorted(merged.items())
        self._samples = deque(ordered)
        self._sigma_samples = deque(ordered)
        self._last_observed_ts = ordered[-1][0]
        self._prune(current)
        return added

    def export_state(self, now: Optional[float] = None) -> dict:
        """Return the bounded rolling history needed for a warm restart.

        The samples are already bounded by ``long_window_sec`` and
        ``sigma_window_sec``.  Keeping the model state in the SQLite ledger
        avoids replaying a potentially huge CSV and, more importantly, keeps
        a restart from trading on the seed midline while the model warms up.
        """
        if now is not None:
            self._prune(float(now))
        return {
            "schema_version": 2,
            "updated_ts": self._last_observed_ts,
            "active_midline_bps": self._active,
            "prior_midline_bps": self._active,
            "prior_midline_ts": self._prior_midline_ts,
            "prior_robust_sigma_bps": self._prior_robust_sigma_bps,
            "regime_since": self._regime_since,
            "recovery_since": self._recovery_since,
            "samples": [[ts, value] for ts, value in self._samples],
            "sigma_samples": [[ts, value]
                              for ts, value in self._sigma_samples],
            "lag_guard": {
                "state": self.lag_guard.state.value,
                "accelerate_since": self.lag_guard._accelerate_since,
                "block_since": self.lag_guard._block_since,
                "recovery_since": self.lag_guard._recovery_since,
            },
        }

    def restore_state(self, state: object, now: Optional[float] = None) -> int:
        """Restore finite, recent observations and return their count.

        Invalid or future rows are ignored.  Restoring never changes model
        configuration and never fabricates a sample when the state is empty.
        ``_last_snapshot_ts`` is reset to the restore time so a long process
        gap cannot cause one oversized midline jump on the next snapshot.
        """
        if not isinstance(state, dict):
            return 0
        current = float(time.time() if now is None else now)

        def rows(value: object, cutoff: float) -> list[tuple[float, float]]:
            if not isinstance(value, list):
                return []
            out: list[tuple[float, float]] = []
            for row in value:
                if not isinstance(row, (list, tuple)) or len(row) != 2:
                    continue
                try:
                    ts, premium = float(row[0]), float(row[1])
                except (TypeError, ValueError):
                    continue
                if (math.isfinite(ts) and math.isfinite(premium)
                        and ts <= current and ts >= cutoff):
                    out.append((ts, premium))
            out.sort(key=lambda item: item[0])
            return out

        try:
            prior_midline = float(state.get(
                "prior_midline_bps", state.get("active_midline_bps")))
        except (TypeError, ValueError):
            prior_midline = self.seed_midline_bps
        if math.isfinite(prior_midline):
            self._active = prior_midline
        try:
            prior_sigma = float(state.get("prior_robust_sigma_bps"))
        except (TypeError, ValueError):
            prior_sigma = math.nan
        self._prior_robust_sigma_bps = (
            prior_sigma if math.isfinite(prior_sigma) and prior_sigma >= 0
            else None)
        self._prior_midline_ts = None
        try:
            prior_ts = float(state.get("prior_midline_ts"))
        except (TypeError, ValueError):
            prior_ts = math.nan
        if math.isfinite(prior_ts):
            self._prior_midline_ts = prior_ts

        self._samples = deque(rows(
            state.get("samples"), current - self.long_window_sec))
        self._sigma_samples = deque(rows(
            state.get("sigma_samples"), current - self.sigma_window_sec))
        if not self._sigma_samples and self._samples:
            self._sigma_samples = deque(
                item for item in self._samples
                if item[0] >= current - self.sigma_window_sec)
        if not self._samples:
            self._last_observed_ts = None
            self._last_snapshot_ts = current
            self._regime_since = None
            self._recovery_since = None
            guard = state.get("lag_guard")
            if isinstance(guard, dict):
                try:
                    self.lag_guard.state = MidlineLagState(str(
                        guard.get("state", MidlineLagState.NORMAL.value)))
                except ValueError:
                    self.lag_guard.reset()
            return 0

        self._last_observed_ts = self._samples[-1][0]
        def finite_or_none(value: object) -> Optional[float]:
            try:
                value = float(value)
            except (TypeError, ValueError):
                return None
            return value if math.isfinite(value) else None

        self._regime_since = finite_or_none(state.get("regime_since"))
        self._recovery_since = finite_or_none(state.get("recovery_since"))
        # A restored state is immediately usable for measuring history, but
        # its next active-midline update starts from the current wall clock.
        self._last_snapshot_ts = current

        guard = state.get("lag_guard")
        if isinstance(guard, dict):
            try:
                self.lag_guard.state = MidlineLagState(str(
                    guard.get("state", MidlineLagState.NORMAL.value)))
            except ValueError:
                self.lag_guard.reset()
            self.lag_guard._accelerate_since = finite_or_none(
                guard.get("accelerate_since"))
            self.lag_guard._block_since = finite_or_none(
                guard.get("block_since"))
            self.lag_guard._recovery_since = finite_or_none(
                guard.get("recovery_since"))
        self._prune(current)
        return len(self._samples)

    def reconfigure(self, *, short_window_sec: float,
                    long_window_sec: float, warmup_sec: float,
                    min_samples: int, shift_min_bps: float,
                    shift_sigma_mult: float, recovery_ratio: float,
                    recovery_sec: float, seed_midline_bps: float,
                    base_move_bps_per_10m: float,
                    accelerated_move_bps_per_10m: float,
                    absolute_max_move_bps_per_10m: float,
                    sigma_window_sec: float,
                    midline_lag_guard_enabled: bool,
                    midline_lag_warn_bps: float,
                    midline_lag_block_bps: float,
                    midline_lag_persist_sec: float,
                    midline_lag_accelerate_bps: float,
                    midline_lag_accelerate_persist_sec: float,
                    midline_lag_recovery_sec: float = 90.0,
                    midline_lag_recovery_bps: float = 2.0) -> None:
        """Update model knobs while preserving its observed history/state.

        The configuration loader has already validated all limits.  Existing
        samples are retained, so a newly enlarged window fills progressively
        from the live feed; the model never fabricates missing history.
        """
        self.short_window_sec = float(short_window_sec)
        self.long_window_sec = float(long_window_sec)
        self.warmup_sec = float(warmup_sec)
        self.min_samples = int(min_samples)
        self.shift_min_bps = float(shift_min_bps)
        self.shift_sigma_mult = float(shift_sigma_mult)
        self.base_move_bps_per_10m = float(base_move_bps_per_10m)
        self.accelerated_move_bps_per_10m = float(
            accelerated_move_bps_per_10m)
        self.absolute_max_move_bps_per_10m = float(
            absolute_max_move_bps_per_10m)
        self.max_move_bps_per_10m = self.base_move_bps_per_10m
        self.sigma_window_sec = float(sigma_window_sec)
        self.recovery_ratio = float(recovery_ratio)
        self.recovery_sec = float(recovery_sec)
        self.seed_midline_bps = float(seed_midline_bps)

        # Preserve the guard's persistence state.  A changed threshold takes
        # effect on the next snapshot; disabling it naturally resets the
        # state through MidlineLagGuard.update().
        self.lag_guard.enabled = bool(midline_lag_guard_enabled)
        self.lag_guard.warn_bps = float(midline_lag_warn_bps)
        self.lag_guard.block_bps = float(midline_lag_block_bps)
        self.lag_guard.persist_sec = float(midline_lag_persist_sec)
        self.lag_guard.accelerate_bps = float(midline_lag_accelerate_bps)
        self.lag_guard.accelerate_persist_sec = float(
            midline_lag_accelerate_persist_sec)
        self.lag_guard.recovery_sec = float(midline_lag_recovery_sec)
        self.lag_guard.recovery_bps = float(midline_lag_recovery_bps)

        if self._last_observed_ts is not None:
            self._prune(self._last_observed_ts)

    def _prune(self, now: float) -> None:
        cutoff = now - self.long_window_sec
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()
        sigma_cutoff = now - self.sigma_window_sec
        while self._sigma_samples and self._sigma_samples[0][0] < sigma_cutoff:
            self._sigma_samples.popleft()

    @staticmethod
    def _median(values: list[float]) -> Optional[float]:
        return statistics.median(values) if values else None

    def snapshot(self, now: float) -> PremiumModelSnapshot:
        self._prune(now)
        long_values = [v for ts, v in self._samples if ts <= now]
        short_values = [v for ts, v in self._samples
                        if now - self.short_window_sec <= ts <= now]
        sigma_values = [v for ts, v in self._sigma_samples if ts <= now]
        long_center = self._median(long_values)
        short_center = self._median(short_values)
        sigma_center = self._median(sigma_values)
        mad = None
        sigma = None
        if sigma_center is not None:
            mad = statistics.median(abs(v - sigma_center) for v in sigma_values)
            sigma = 1.4826 * mad
        history_sec = (self._samples[-1][0] - self._samples[0][0]
                       if len(self._samples) > 1 else 0.0)
        latest_age = (max(float(now) - self._samples[-1][0], 0.0)
                      if self._samples else None)
        normal = (len(long_values) >= self.min_samples
                  and history_sec >= self.warmup_sec)
        cold_min = min(self.min_samples, self.cold_start_min_samples)
        cold_warmup = min(self.warmup_sec, self.cold_start_warmup_sec)
        cold = (len(long_values) >= cold_min
                and history_sec >= cold_warmup)
        readiness = (PremiumReadiness.NORMAL if normal else
                     PremiumReadiness.COLD_DEGRADED if cold else
                     PremiumReadiness.UNAVAILABLE)
        ready = readiness == PremiumReadiness.NORMAL
        tradable = readiness != PremiumReadiness.UNAVAILABLE
        if sigma is None:
            sigma = self._prior_robust_sigma_bps
        if sigma is not None and math.isfinite(sigma) and sigma >= 0:
            self._prior_robust_sigma_bps = float(sigma)
            # A checkpoint may contain only an active prior after all rows
            # have aged out. Preserve its timestamp in that case; an empty
            # live tail must not erase the restored prior metadata.
            if self._last_observed_ts is not None:
                self._prior_midline_ts = self._last_observed_ts
        target = long_center if tradable else None
        if target is None and tradable:
            target = self._active

        if readiness == PremiumReadiness.NORMAL:
            model_uncertainty = 0.0
        elif readiness == PremiumReadiness.COLD_DEGRADED:
            sigma_value = max(float(sigma or 0.0), 0.0)
            model_uncertainty = (max(1.5, 1.5 * sigma_value)
                                 if history_sec < 600.0
                                 else max(0.75, sigma_value))
        else:
            model_uncertainty = 0.0

        lag_for_guard = (compute_midline_lag(short_center, self._active)
                         if tradable else None)
        lag_state = self.lag_guard.update(lag_for_guard, now)
        if lag_state in (MidlineLagState.ACCELERATING, MidlineLagState.BLOCKED):
            move_speed = min(self.accelerated_move_bps_per_10m,
                             self.absolute_max_move_bps_per_10m)
        else:
            move_speed = min(self.base_move_bps_per_10m,
                             self.absolute_max_move_bps_per_10m)

        if target is not None:
            elapsed = (0.0 if self._last_snapshot_ts is None else
                       max(now - self._last_snapshot_ts, 0.0))
            allowed = move_speed * elapsed / 600.0
            delta = max(min(target - self._active, allowed), -allowed)
            self._active += delta
        # Keep the last valid active midline while the rolling model is not
        # ready.  The constructor/empty restore path already provides the
        # seed for a genuinely cold start; resetting it on every invalid or
        # incomplete snapshot would reintroduce the restart lag bug.

        shift_threshold = None
        divergence = None
        if tradable and short_center is not None and long_center is not None \
                and sigma is not None:
            shift_threshold = max(self.shift_min_bps,
                                  self.shift_sigma_mult * sigma)
            divergence = abs(short_center - long_center)
            if self._regime_since is None and divergence > shift_threshold:
                self._regime_since = now
                self._recovery_since = None
            elif self._regime_since is not None:
                recovery_threshold = shift_threshold * self.recovery_ratio
                if divergence <= recovery_threshold:
                    if self._recovery_since is None:
                        self._recovery_since = now
                    if now - self._recovery_since >= self.recovery_sec:
                        self._regime_since = None
                        self._recovery_since = None
                else:
                    self._recovery_since = None

        self._last_snapshot_ts = now
        return PremiumModelSnapshot(
            ts=now, sample_count=len(long_values), ready=ready,
            short_center_bps=short_center, long_center_bps=long_center,
            mad_bps=mad, robust_sigma_bps=sigma,
            raw_target_midline_bps=target,
            active_midline_bps=self._active,
            shift_threshold_bps=shift_threshold,
            regime_shift=self._regime_since is not None,
            regime_since=self._regime_since,
            sigma_center_bps=sigma_center,
            sigma_sample_count=len(sigma_values),
            sigma_window_sec=self.sigma_window_sec,
            midline_lag_bps=compute_midline_lag(short_center, self._active),
            midline_lag_state=lag_state,
            midline_move_speed_bps_per_10m=move_speed,
            readiness=readiness,
            history_sec=history_sec,
            latest_sample_age_sec=latest_age,
            tradable=tradable,
            model_uncertainty_bps=model_uncertainty)
