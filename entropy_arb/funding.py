"""Venue-neutral funding data and cost helpers.

All adapters normalize exchange-specific values to basis points per hour.
Positive funding follows the common convention: longs pay shorts.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional


@dataclass(frozen=True)
class FundingSnapshot:
    rate_bps_per_hour: float
    ts: float
    source: str
    index_price: Decimal | None = None

    def __post_init__(self) -> None:
        if self.index_price is not None and (
                not isinstance(self.index_price, Decimal)
                or not self.index_price.is_finite() or self.index_price <= 0):
            raise ValueError('funding index_price must be a positive finite Decimal')

    def is_fresh(self, now: float, stale_sec: float) -> bool:
        return (math.isfinite(self.rate_bps_per_hour)
                and 0.0 <= now - self.ts <= stale_sec)


def funding_decimal(value, field: str) -> Decimal:
    """Parse a human-unit exchange amount; missing values are never zero."""
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"invalid funding {field}") from exc
    if not result.is_finite():
        raise ValueError(f"non-finite funding {field}")
    return result


def funding_timestamp(value, *, milliseconds: bool = False) -> float:
    result = float(funding_decimal(value, "timestamp"))
    if milliseconds:
        result /= 1000
    # Epoch seconds, bounded to reject contemporary millisecond timestamps.
    if not math.isfinite(result) or not 0 <= result < 1e11:
        raise ValueError("invalid funding timestamp (expected epoch seconds)")
    return result


def funding_window(since_ts: float, until_ts: float) -> tuple[float, float]:
    since, until = funding_timestamp(since_ts), funding_timestamp(until_ts)
    if until < since:
        raise ValueError("funding window ends before it starts")
    return since, until


def _validate_decimal(value: Decimal, field: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"funding {field} must be a finite Decimal")


@dataclass(frozen=True)
class FundingSettlement:
    """An actual account ledger event; positive USD amounts are credits."""

    event_id: str
    ts: float
    amount_usd: Decimal
    position_qty: Decimal | None = None
    source: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id.strip():
            raise ValueError("funding settlement identity missing")
        funding_timestamp(self.ts)
        _validate_decimal(self.amount_usd, "amount_usd")
        if self.position_qty is not None:
            _validate_decimal(self.position_qty, "position_qty")


@dataclass(frozen=True)
class FundingRiskSnapshot:
    """Signed base quantity and quote/base prices. None means unknown risk."""

    position_qty: Decimal
    mark_price: Decimal
    liquidation_price: Decimal | None
    ts: float
    source: str = ""

    def __post_init__(self) -> None:
        funding_timestamp(self.ts)
        _validate_decimal(self.position_qty, "position_qty")
        _validate_decimal(self.mark_price, "mark_price")
        if self.mark_price <= 0:
            raise ValueError("funding mark_price must be positive")
        if self.liquidation_price is not None:
            _validate_decimal(self.liquidation_price, "liquidation_price")
            if self.liquidation_price <= 0:
                raise ValueError("funding liquidation_price must be positive")


def expected_pair_funding_cost_bps(
        entropy_rate_bps_h: float,
        hedge_rate_bps_h: float,
        expected_hold_hours: float,
        *, short_entropy: bool) -> float:
    """Expected funding cost for a delta-neutral pair; negative is income."""
    spread = hedge_rate_bps_h - entropy_rate_bps_h
    if not short_entropy:
        spread = -spread
    return spread * expected_hold_hours


def fresh_rate(snapshot: Optional[FundingSnapshot], now: float,
               stale_sec: float) -> Optional[float]:
    if snapshot is None or not snapshot.is_fresh(now, stale_sec):
        return None
    return snapshot.rate_bps_per_hour
