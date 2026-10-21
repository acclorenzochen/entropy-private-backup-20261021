"""Durable state for one two-leg execution.

The exchange does not expose an atomic two-venue transaction. This object is
therefore deliberately boring: it contains every fact needed to continue the
same execution after a timeout, partial fill, websocket reconnect, or process
restart. ``trade_ledger`` checkpoints it after every observable transition.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class ExecutionPhase(str, Enum):
    FIRST_LEG = "first_leg"
    HEDGE_LEG = "hedge_leg"
    RECONCILING = "reconciling"
    FLATTENING = "flattening"
    VERIFIED = "verified"


class RecoveryStatus(str, Enum):
    """Durable recovery mode for an execution.

    A phase describes which leg the normal state machine is working on.  The
    recovery status is orthogonal: it records whether the cost basis is known
    and whether the execution has fallen into a risk-only exit path.
    """

    NORMAL = "normal"
    RECOVERING_FILL = "recovering_fill"
    ORPHANED_INVENTORY = "orphaned_inventory"
    EMERGENCY_EXIT = "emergency_exit"


# These are deliberately strings instead of another enum so checkpoints from
# older versions remain easy to inspect and forward-compatible.  The order
# evidence and position evidence are intentionally represented separately by
# the state names used by the recovery logic.
HEDGE_COMPLETION_NOT_STARTED = "not_started"
HEDGE_COMPLETION_PENDING = "hedge_pending"
HEDGE_ORDER_UNKNOWN = "hedge_order_unknown"
POSITION_COVERED_UNANCHORED = "position_covered_unanchored"
POSITION_COVERED_CONFIRMED = "position_covered_confirmed"
EXECUTION_SAFETY_FLAT = "flat"


@dataclass
class ExecutionState:
    """Runtime and recovery state for a single Entropy-first execution."""

    cycle_id: str
    action: str
    direction: str
    planned_qty: float
    entropy_is_buy: bool
    hedge_is_buy: bool
    reduce_only: bool
    phase: ExecutionPhase = ExecutionPhase.FIRST_LEG
    recovery_status: RecoveryStatus = RecoveryStatus.NORMAL
    recovery_started_ts: Optional[float] = None
    recovery_reason: Optional[str] = None
    # Durable single-flight recovery backoff. These fields prevent a
    # reconnect/reconcile storm from submitting the same retry repeatedly.
    recovery_next_retry_ts: Optional[float] = None
    recovery_attempt: int = 0
    recovery_backoff_sec: float = 0.0
    hedge_reserve_breach: bool = False
    hedge_reserve_breach_ts: Optional[float] = None
    first_filled: float = 0.0
    hedge_filled: float = 0.0
    pending_leg: Optional[str] = None
    pending_order_ref: Optional[str] = None
    pending_filled: float = 0.0
    retry_count: int = 0
    first_topup_count: int = 0
    first_topup_last_ts: Optional[float] = None
    last_error: Optional[str] = None
    created_ts: float = field(default_factory=lambda: time.time())
    updated_ts: float = field(default_factory=lambda: time.time())
    first_order_ref: Optional[str] = None
    hedge_order_ref: Optional[str] = None
    first_order_status: Optional[str] = None
    hedge_order_status: Optional[str] = None
    first_limit_px: Optional[float] = None
    hedge_limit_px: Optional[float] = None
    first_avg_px: Optional[float] = None
    hedge_avg_px: Optional[float] = None
    first_position_before: Optional[float] = None
    hedge_position_before: Optional[float] = None
    last_transition: Optional[str] = None
    # Position evidence can prove that the hedge is already present even when
    # the order resolver is still unknown.  Keep this separate from the fill
    # quantities, which remain accounting facts recovered from the ledger.
    hedge_completion_state: str = HEDGE_COMPLETION_NOT_STARTED

    @property
    def remaining_hedge(self) -> float:
        return max(self.first_filled - self.hedge_filled, 0.0)

    @property
    def has_pending_order(self) -> bool:
        return bool(self.pending_leg and self.pending_order_ref)

    @property
    def terminal(self) -> bool:
        return self.phase == ExecutionPhase.VERIFIED

    def touch(self) -> None:
        self.updated_ts = time.time()

    def transition(self, phase: ExecutionPhase, reason: Optional[str] = None) -> None:
        self.phase = (phase if isinstance(phase, ExecutionPhase)
                      else ExecutionPhase(str(phase)))
        self.last_transition = reason
        self.touch()

    def set_recovery(self, status: RecoveryStatus | str,
                     reason: Optional[str] = None) -> None:
        """Change the recovery mode while retaining its first-seen time."""
        try:
            status = (status if isinstance(status, RecoveryStatus)
                      else RecoveryStatus(str(status)))
        except ValueError:
            # A future/unknown checkpoint must remain fail-closed instead of
            # silently becoming normal trading state.
            status = RecoveryStatus.RECOVERING_FILL
        if status != RecoveryStatus.NORMAL and self.recovery_started_ts is None:
            self.recovery_started_ts = time.time()
        if status == RecoveryStatus.NORMAL:
            self.recovery_started_ts = None
            self.recovery_reason = None
            self.recovery_next_retry_ts = None
            self.recovery_backoff_sec = 0.0
        else:
            self.recovery_reason = reason or self.recovery_reason
        self.recovery_status = status
        self.touch()

    def set_pending(self, leg: str, order_ref: Optional[object]) -> None:
        self.pending_leg = leg
        self.pending_order_ref = (None if order_ref is None
                                  else str(order_ref))
        self.pending_filled = 0.0
        self.transition(ExecutionPhase.RECONCILING, f"pending_{leg}")

    def clear_pending(self) -> None:
        self.pending_leg = None
        self.pending_order_ref = None
        self.pending_filled = 0.0
        self.touch()

    def set_order_result(self, leg: str, info: dict,
                         *, limit_px: Optional[float] = None) -> None:
        ref = info.get("order_ref") or info.get("client_order_id")
        status = str(info.get("status") or "unknown")
        avg_px = info.get("avg_px")
        if leg == "first":
            self.first_order_ref = None if ref is None else str(ref)
            self.first_order_status = status
            self.first_limit_px = limit_px
            self.first_avg_px = _optional_float(avg_px)
        else:
            self.hedge_order_ref = None if ref is None else str(ref)
            self.hedge_order_status = status
            self.hedge_limit_px = limit_px
            self.hedge_avg_px = _optional_float(avg_px)
        self.touch()

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "action": self.action,
            "direction": self.direction,
            "planned_qty": self.planned_qty,
            "entropy_is_buy": self.entropy_is_buy,
            "hedge_is_buy": self.hedge_is_buy,
            "reduce_only": self.reduce_only,
            "phase": self.phase.value,
            "recovery_status": self.recovery_status.value,
            "recovery_started_ts": self.recovery_started_ts,
            "recovery_reason": self.recovery_reason,
            "recovery_next_retry_ts": self.recovery_next_retry_ts,
            "recovery_attempt": self.recovery_attempt,
            "recovery_backoff_sec": self.recovery_backoff_sec,
            "hedge_reserve_breach": self.hedge_reserve_breach,
            "hedge_reserve_breach_ts": self.hedge_reserve_breach_ts,
            "first_filled": self.first_filled,
            "hedge_filled": self.hedge_filled,
            "pending_leg": self.pending_leg,
            "pending_order_ref": self.pending_order_ref,
            "pending_filled": self.pending_filled,
            "retry_count": self.retry_count,
            "first_topup_count": self.first_topup_count,
            "first_topup_last_ts": self.first_topup_last_ts,
            "last_error": self.last_error,
            "created_ts": self.created_ts,
            "updated_ts": self.updated_ts,
            "first_order_ref": self.first_order_ref,
            "hedge_order_ref": self.hedge_order_ref,
            "first_order_status": self.first_order_status,
            "hedge_order_status": self.hedge_order_status,
            "first_limit_px": self.first_limit_px,
            "hedge_limit_px": self.hedge_limit_px,
            "first_avg_px": self.first_avg_px,
            "hedge_avg_px": self.hedge_avg_px,
            "first_position_before": self.first_position_before,
            "hedge_position_before": self.hedge_position_before,
            "last_transition": self.last_transition,
            "hedge_completion_state": self.hedge_completion_state,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionState":
        """Load a checkpoint, accepting older checkpoints with fewer fields."""
        if not isinstance(data, dict):
            raise TypeError("execution checkpoint must be an object")
        raw_phase = data.get("phase", ExecutionPhase.FIRST_LEG.value)
        try:
            phase = raw_phase if isinstance(raw_phase, ExecutionPhase) \
                else ExecutionPhase(str(raw_phase))
        except ValueError:
            phase = ExecutionPhase.RECONCILING
        raw_recovery = data.get("recovery_status", RecoveryStatus.NORMAL.value)
        try:
            recovery_status = (raw_recovery
                               if isinstance(raw_recovery, RecoveryStatus)
                               else RecoveryStatus(str(raw_recovery)))
        except ValueError:
            recovery_status = RecoveryStatus.RECOVERING_FILL
        kwargs: dict[str, Any] = {
            "cycle_id": str(data["cycle_id"]),
            "action": str(data.get("action") or "open"),
            "direction": str(data.get("direction") or "unknown"),
            "planned_qty": float(data.get("planned_qty") or 0.0),
            "entropy_is_buy": bool(data.get("entropy_is_buy")),
            "hedge_is_buy": bool(data.get("hedge_is_buy")),
            "reduce_only": bool(data.get("reduce_only")),
            "phase": phase,
            "recovery_status": recovery_status,
            "recovery_started_ts": _optional_float(
                data.get("recovery_started_ts")),
            "recovery_reason": data.get("recovery_reason"),
            "recovery_next_retry_ts": _optional_float(
                data.get("recovery_next_retry_ts")),
            "recovery_attempt": int(data.get("recovery_attempt") or 0),
            "recovery_backoff_sec": float(
                data.get("recovery_backoff_sec") or 0.0),
            "hedge_reserve_breach": bool(data.get("hedge_reserve_breach")),
            "hedge_reserve_breach_ts": _optional_float(
                data.get("hedge_reserve_breach_ts")),
        }
        for name in ("first_filled", "hedge_filled", "pending_filled"):
            kwargs[name] = float(data.get(name) or 0.0)
        kwargs["retry_count"] = int(data.get("retry_count") or 0)
        kwargs["first_topup_count"] = int(data.get("first_topup_count") or 0)
        kwargs["first_topup_last_ts"] = _optional_float(
            data.get("first_topup_last_ts"))
        for name in (
                "pending_leg", "pending_order_ref", "last_error",
                "first_order_ref", "hedge_order_ref", "first_order_status",
                "hedge_order_status", "last_transition"):
            kwargs[name] = data.get(name)
        for name in (
                "first_limit_px", "hedge_limit_px", "first_avg_px",
                "hedge_avg_px", "first_position_before",
                "hedge_position_before"):
            kwargs[name] = _optional_float(data.get(name))
        kwargs["created_ts"] = float(data.get("created_ts") or time.time())
        kwargs["updated_ts"] = float(
            data.get("updated_ts") or kwargs["created_ts"])
        completion_state = str(data.get(
            "hedge_completion_state", HEDGE_COMPLETION_NOT_STARTED))
        if completion_state not in {
                HEDGE_COMPLETION_NOT_STARTED, HEDGE_COMPLETION_PENDING,
                HEDGE_ORDER_UNKNOWN, POSITION_COVERED_UNANCHORED,
                POSITION_COVERED_CONFIRMED, EXECUTION_SAFETY_FLAT}:
            # Unknown future values must remain fail-closed and must not be
            # treated as a normal, sendable execution.
            completion_state = HEDGE_ORDER_UNKNOWN
        kwargs["hedge_completion_state"] = completion_state
        return cls(**kwargs)


def _optional_float(value: Any) -> Optional[float]:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None
