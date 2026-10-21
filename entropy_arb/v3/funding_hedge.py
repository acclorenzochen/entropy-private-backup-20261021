"""Deterministic carry policy: USD cash per hour, never points or trade signals."""
from __future__ import annotations

from decimal import Decimal

from entropy_arb.funding import FundingRiskSnapshot, FundingSnapshot
from .core import BPS, Direction, dec


def leg_income_usd_per_hour(quantity: Decimal, price: Decimal,
                            snapshot: FundingSnapshot | None, *, now: float,
                            stale_seconds: float) -> Decimal | None:
    if snapshot is None or not snapshot.is_fresh(now, stale_seconds):
        return None
    quantity, price = dec(quantity), dec(price)
    if price <= 0:
        raise ValueError('positive funding price required')
    # Adapters normalize hourly bps; positive rate means longs pay shorts.
    return -quantity * price * dec(snapshot.rate_bps_per_hour) / BPS


def pair_income_usd_per_hour(entropy_qty: Decimal, hedge_qty: Decimal,
                             entropy_price: Decimal, hedge_price: Decimal,
                             entropy_rate: FundingSnapshot | None,
                             hedge_rate: FundingSnapshot | None, *, now: float,
                             stale_seconds: float) -> Decimal | None:
    incomes = [leg_income_usd_per_hour(q, p, r, now=now,
                                      stale_seconds=stale_seconds)
               for q, p, r in ((entropy_qty, entropy_price, entropy_rate),
                               (hedge_qty, hedge_price, hedge_rate))]
    return None if any(v is None for v in incomes) else sum(incomes, Decimal('0'))


def entry_direction(entropy_price: Decimal, hedge_price: Decimal,
                    entropy_rate: FundingSnapshot | None,
                    hedge_rate: FundingSnapshot | None, *, now: float,
                    stale_seconds: float) -> Direction | None:
    long_income = pair_income_usd_per_hour(
        Decimal('1'), Decimal('-1'), entropy_price, hedge_price,
        entropy_rate, hedge_rate, now=now, stale_seconds=stale_seconds)
    if long_income is None or long_income == 0:
        return None
    return Direction.LONG_ENTROPY if long_income > 0 else Direction.SHORT_ENTROPY


def settlements_complete(state: dict, required: int) -> bool:
    return state.get('hedged_ts') is not None and all(
        state['settlement_counts'][v] >= required for v in ('entropy', 'hedge'))


def reversal_confirmed(state: dict, *, samples: int, seconds: float) -> bool:
    return (state['negative_sample_count'] >= samples
            and state['negative_since_ts'] is not None
            and state['last_sample_ts'] is not None
            and state['last_sample_ts'] - state['negative_since_ts'] >= seconds)


def remaining_cost_usd(net_usd_per_hour: Decimal, counts: dict,
                       required: int) -> Decimal:
    """Conservative full hourly bills, not pro-rata waiting time.

    Both legs stay open until the slower ledger has enough confirmations.
    Delayed/missing entries cannot reduce this estimate or prove completion.
    This estimate is not booked as actual PnL.
    """
    periods = max(max(0, required - counts[v]) for v in ('entropy', 'hedge'))
    return max(Decimal('0'), -dec(net_usd_per_hour) * periods)


def liquidation_distance(snapshot: FundingRiskSnapshot, *, now: float,
                          stale_seconds: float) -> Decimal | None:
    if not 0 <= now - snapshot.ts <= stale_seconds:
        return None
    if snapshot.position_qty == 0:
        return None
    if snapshot.liquidation_price is None:
        return None
    # A crossed liquidation boundary stays dangerous (negative distance).
    sign = Decimal('1') if snapshot.position_qty > 0 else Decimal('-1')
    return sign * (snapshot.mark_price - snapshot.liquidation_price) / snapshot.mark_price
