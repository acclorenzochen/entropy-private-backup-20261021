"""Small, deterministic strategy core. No network, credentials, or legacy gates."""
from __future__ import annotations

from dataclasses import dataclass, fields
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from enum import Enum
import math
from typing import Any, Mapping

BPS = Decimal('10000')
ZERO = Decimal('0')


def dec(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ValueError('boolean is not a number')
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError('non-finite number')
    return number


class Side(str, Enum):
    BUY = 'buy'
    SELL = 'sell'

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1

    @property
    def opposite(self) -> 'Side':
        return Side.SELL if self is Side.BUY else Side.BUY


class Direction(str, Enum):
    LONG_ENTROPY = 'long_entropy'
    SHORT_ENTROPY = 'short_entropy'

    @property
    def entropy_side(self) -> Side:
        return Side.BUY if self is Direction.LONG_ENTROPY else Side.SELL


@dataclass(frozen=True)
class Settings:
    strategy_mode: str = 'basis'
    opening_experiment_id: str = ''
    funding_settlements_required: int = 2
    funding_poll_seconds: float = 30.0
    funding_stale_seconds: float = 90.0
    funding_reversal_samples: int = 3
    funding_reversal_seconds: float = 60.0
    funding_remaining_cost_limit_usd: Decimal = Decimal('0.5')
    funding_round_loss_limit_usd: Decimal | None = None
    funding_liquidation_distance: Decimal = Decimal('0.02')
    funding_risk_poll_seconds: float = 2.0
    funding_risk_stale_seconds: float = 10.0
    funding_accounting_grace_seconds: float = 120.0
    cold_seconds: float = 30.0
    cold_samples: int = 20
    warm_seconds: float = 5.0
    warm_samples: int = 5
    checkpoint_max_age_seconds: float = 120.0
    reset_gap_seconds: float = 10.0
    reference_half_life_seconds: float = 60.0
    max_book_age_seconds: float = 1.0
    max_book_skew_seconds: float = 0.5
    entry_band_bps: Decimal = Decimal('5')
    execution_allowance_bps: Decimal = Decimal('1')
    sampling_floor_bps: Decimal = Decimal('-2')
    max_notional_usd: Decimal = Decimal('15')
    exit_min_gross_profit_bps: Decimal = Decimal('2.5')
    # Accepted for old configurations; basis exits use the locked notional in bp.
    exit_min_gross_profit_usd: Decimal = Decimal('0.03')
    # Accepted for old configs; these no longer permit basis exits.
    exit_reversion_bps: Decimal = Decimal('2')
    exit_loss_allowance_usd: Decimal = Decimal('0.01')
    first_rooms_bps: tuple[Decimal, ...] = (Decimal('3'), Decimal('5'), Decimal('8'))
    hedge_rooms_bps: tuple[Decimal, ...] = (Decimal('4'), Decimal('8'), Decimal('12'))
    first_hard_cap_bps: Decimal = Decimal('10')
    hedge_hard_cap_bps: Decimal = Decimal('12')
    open_first_rooms_bps: tuple[Decimal, ...] | None = None
    open_first_hard_cap_bps: Decimal | None = None
    open_hedge_rooms_bps: tuple[Decimal, ...] | None = None
    open_hedge_hard_cap_bps: Decimal | None = None
    close_first_rooms_bps: tuple[Decimal, ...] | None = None
    close_first_hard_cap_bps: Decimal | None = None
    close_hedge_rooms_bps: tuple[Decimal, ...] | None = None
    close_hedge_hard_cap_bps: Decimal | None = None
    exit_fast_path_enabled: bool = False
    entropy_orders_per_min: int = 6
    hedge_orders_per_min: int = 6
    single_cycle_review_loss_usd: Decimal | None = Decimal('0.10')
    equity_poll_seconds: float = 5.0
    position_audit_seconds: float = 60.0
    flat_position_probe_sec: float = 1.0
    flat_position_lease_sec: float = 1.5
    # Explicit user policy for one account/batch; zero configured bps alone is not evidence.
    lighter_zero_fee_account_index: int | None = None
    lighter_zero_fee_batch_id: str | None = None
    lighter_zero_fee_policy_source: str | None = None

    def __post_init__(self) -> None:
        if (not isinstance(self.opening_experiment_id, str)
                or self.opening_experiment_id != self.opening_experiment_id.strip()
                or len(self.opening_experiment_id) > 128):
            raise ValueError('invalid opening experiment identity')
        if self.opening_experiment_id and (self.strategy_mode != 'basis'
                or self.max_notional_usd > Decimal('200')):
            raise ValueError('opening experiment requires basis mode and at most 200 USD per leg')
        if (isinstance(self.position_audit_seconds, bool)
                or not isinstance(self.position_audit_seconds, (int, float))
                or not math.isfinite(self.position_audit_seconds)
                or self.position_audit_seconds <= 0):
            raise ValueError('position_audit_seconds must be finite and positive')
        policy = (self.lighter_zero_fee_account_index, self.lighter_zero_fee_batch_id,
                  self.lighter_zero_fee_policy_source)
        if any(value is not None for value in policy):
            if (type(policy[0]) is not int or policy[0] < 0
                    or any(not isinstance(value, str) or not value.strip() for value in policy[1:])):
                raise ValueError('Lighter zero-fee policy requires account, batch and user source')
        if self.strategy_mode not in {'basis', 'funding_hedge'}:
            raise ValueError('strategy_mode must be basis or funding_hedge')
        for name, minimum in (('funding_settlements_required', 2),
                              ('funding_reversal_samples', 3)):
            if type(getattr(self, name)) is not int or getattr(self, name) < minimum:
                raise ValueError(f'{name} must be an integer >={minimum}')
        if self.funding_reversal_seconds < 60:
            raise ValueError('funding_reversal_seconds must be >=60')
        if not Decimal('0.02') <= self.funding_liquidation_distance < 1:
            raise ValueError('funding_liquidation_distance must be in [0.02,1)')
        for name in ('funding_poll_seconds', 'funding_stale_seconds',
                     'funding_remaining_cost_limit_usd',
                     'funding_risk_poll_seconds', 'funding_risk_stale_seconds',
                     'funding_accounting_grace_seconds'):
            if getattr(self, name) <= 0:
                raise ValueError(f'{name} must be positive')
        if self.funding_round_loss_limit_usd is not None and (
                not isinstance(self.funding_round_loss_limit_usd, Decimal)
                or not self.funding_round_loss_limit_usd.is_finite()
                or self.funding_round_loss_limit_usd <= 0):
            raise ValueError('funding_round_loss_limit_usd must be positive or null (disabled)')
        if self.single_cycle_review_loss_usd is not None and (
                not isinstance(self.single_cycle_review_loss_usd, Decimal)
                or not self.single_cycle_review_loss_usd.is_finite()
                or self.single_cycle_review_loss_usd <= 0):
            raise ValueError('single_cycle_review_loss_usd must be positive or null (disabled)')
        if (not isinstance(self.exit_min_gross_profit_bps, Decimal)
                or not self.exit_min_gross_profit_bps.is_finite()
                or self.exit_min_gross_profit_bps <= 0):
            raise ValueError('exit_min_gross_profit_bps must be positive and finite')
        for f in fields(self):
            v = getattr(self, f.name)
            if isinstance(v, Decimal) and not v.is_finite():
                raise ValueError(f'{f.name}: non-finite')
            if isinstance(v, float) and not math.isfinite(v):
                raise ValueError(f'{f.name}: non-finite')
        for name in ('cold_seconds', 'warm_seconds', 'checkpoint_max_age_seconds',
                     'reset_gap_seconds', 'reference_half_life_seconds',
                     'max_book_age_seconds', 'max_book_skew_seconds',
                     'entry_band_bps', 'max_notional_usd', 'exit_min_gross_profit_usd',
                     'exit_min_gross_profit_bps',
                     'first_hard_cap_bps',
                     'hedge_hard_cap_bps', 'equity_poll_seconds'):
            if getattr(self, name) <= 0:
                raise ValueError(f'{name} must be positive')
        for name in ('close_first_hard_cap_bps', 'close_hedge_hard_cap_bps'):
            if (value := getattr(self, name)) is not None and (
                    not isinstance(value, Decimal) or not value.is_finite() or value <= 0):
                raise ValueError(f'{name} must be positive')
        for name in ('open_first_hard_cap_bps', 'open_hedge_hard_cap_bps'):
            if (value := getattr(self, name)) is not None and (
                    not isinstance(value, Decimal) or not value.is_finite() or value < 0):
                raise ValueError(f'{name} must be nonnegative')
        if self.flat_position_probe_sec <= 0 or self.flat_position_lease_sec <= 0:
            raise ValueError('flat position probe/lease must be positive')
        for name in ('cold_samples', 'warm_samples'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 2:
                raise ValueError(f'{name} must be an integer >=2')
        if self.execution_allowance_bps < 0 or self.exit_loss_allowance_usd < 0:
            raise ValueError('negative cost allowance')
        for name in ('entropy_orders_per_min', 'hedge_orders_per_min'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 3:
                raise ValueError(f'{name} must be an integer >=3')
        if type(self.exit_fast_path_enabled) is not bool:
            raise ValueError('exit_fast_path_enabled must be boolean')
        if self.exit_reversion_bps < 0 or self.sampling_floor_bps > 0:
            raise ValueError('invalid sampling/exit parameter')
        for rooms, cap in ((self.first_rooms_bps, self.first_hard_cap_bps),
                           (self.hedge_rooms_bps, self.hedge_hard_cap_bps),
                           self.first_leg_execution_policy('open'),
                           self.first_leg_execution_policy('close'),
                           self.hedge_execution_policy('open'),
                           self.hedge_execution_policy('close')):
            if not rooms or any(not isinstance(r, Decimal) or not r.is_finite()
                                or r < 0 or r > cap for r in rooms):
                raise ValueError('invalid execution ladder')
            if tuple(sorted(rooms)) != rooms:
                raise ValueError('execution ladder must be nondecreasing')

    def first_leg_execution_policy(self, purpose: str) -> tuple[tuple[Decimal, ...], Decimal]:
        if purpose not in {'open', 'close'}:
            raise ValueError('first-leg purpose must be open or close')
        rooms = getattr(self, f'{purpose}_first_rooms_bps')
        cap = getattr(self, f'{purpose}_first_hard_cap_bps')
        return (rooms if rooms is not None else self.first_rooms_bps,
                cap if cap is not None else self.first_hard_cap_bps)

    def hedge_execution_policy(self, purpose: str) -> tuple[tuple[Decimal, ...], Decimal]:
        if purpose not in {'open', 'close'}:
            raise ValueError('hedge purpose must be open or close')
        rooms = getattr(self, f'{purpose}_hedge_rooms_bps')
        cap = getattr(self, f'{purpose}_hedge_hard_cap_bps')
        return (rooms if rooms is not None else self.hedge_rooms_bps,
                cap if cap is not None else self.hedge_hard_cap_bps)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> 'Settings':
        known = {f.name for f in fields(cls)}
        if set(raw) - known:
            raise ValueError(f'unknown V3 keys: {sorted(set(raw) - known)}')
        defaults = cls()
        values: dict[str, Any] = {}
        for key, value in raw.items():
            if key in {'lighter_zero_fee_account_index', 'lighter_zero_fee_batch_id',
                       'lighter_zero_fee_policy_source'}:
                values[key] = value
                continue
            if key in {'funding_round_loss_limit_usd', 'single_cycle_review_loss_usd'}:
                values[key] = None if value is None else dec(value)
                continue
            if key in {'open_hedge_rooms_bps', 'close_hedge_rooms_bps'} and value is None:
                values[key] = None
                continue
            if key in {'open_first_rooms_bps', 'open_hedge_rooms_bps',
                       'close_first_rooms_bps', 'close_hedge_rooms_bps'}:
                if not isinstance(value, (tuple, list)):
                    raise ValueError(f'{key}: expected list')
                values[key] = tuple(dec(v) for v in value)
                continue
            if key in {'open_first_hard_cap_bps', 'open_hedge_hard_cap_bps', 'close_first_hard_cap_bps',
                       'close_hedge_hard_cap_bps'}:
                values[key] = None if key in {'open_hedge_hard_cap_bps',
                    'close_hedge_hard_cap_bps'} and value is None else dec(value)
                continue
            default = getattr(defaults, key)
            if isinstance(default, Decimal):
                values[key] = dec(value)
            elif isinstance(default, tuple):
                if not isinstance(value, (tuple, list)):
                    raise ValueError(f'{key}: expected list')
                values[key] = tuple(dec(v) for v in value)
            elif isinstance(default, bool):
                if type(value) is not bool:
                    raise ValueError(f'{key}: expected boolean')
                values[key] = value
            elif isinstance(default, int):
                if type(value) is not int:
                    raise ValueError(f'{key}: expected integer')
                values[key] = value
            elif isinstance(default, str):
                if not isinstance(value, str):
                    raise ValueError(f'{key}: expected string')
                values[key] = value
            else:
                if isinstance(value, bool):
                    raise ValueError(f'{key}: expected number')
                values[key] = float(value)
        return cls(**values)


@dataclass(frozen=True)
class Instrument:
    """Common base-unit quantum is established by the host before construction.

    Only same-currency, same-underlying, linear contracts are supported here.
    Neither approximate symbol matching nor automatic FX conversion is performed.
    """
    pair_id: str
    quantity_step: Decimal
    entropy_tick: Decimal
    hedge_tick: Decimal
    entropy_fee_bps: Decimal
    hedge_fee_bps: Decimal

    def __post_init__(self) -> None:
        if not self.pair_id:
            raise ValueError('missing pair identity')
        for name in ('quantity_step', 'entropy_tick', 'hedge_tick'):
            if not getattr(self, name).is_finite() or getattr(self, name) <= 0:
                raise ValueError(f'{name} must be positive')
        for name in ('entropy_fee_bps', 'hedge_fee_bps'):
            if not getattr(self, name).is_finite() or getattr(self, name) < 0:
                raise ValueError('explicit nonnegative taker fee required')

    def quantity(self, units: int) -> Decimal:
        if type(units) is not int or units <= 0:
            raise ValueError('positive integer units required')
        return self.quantity_step * units

    def exact_units(self, quantity: Any) -> int:
        scaled = dec(quantity) / self.quantity_step
        if scaled != scaled.to_integral_value():
            raise ValueError('quantity does not lie on common unit grid')
        return int(scaled)


@dataclass(frozen=True)
class Book:
    """Sorted executable depth: each level is (price, integer quantity units)."""
    bids: tuple[tuple[Decimal, int], ...]
    asks: tuple[tuple[Decimal, int], ...]
    received_at: float
    revision: str

    def __post_init__(self) -> None:
        if not self.bids or not self.asks or not self.revision:
            raise ValueError('missing executable book/revision')
        if not math.isfinite(self.received_at):
            raise ValueError('invalid book timestamp')
        for levels in (self.bids, self.asks):
            if any(not p.is_finite() or p <= 0 or type(q) is not int or q <= 0
                   for p, q in levels):
                raise ValueError('invalid book level')
        if self.bids != tuple(sorted(self.bids, reverse=True)):
            raise ValueError('bids must be descending')
        if self.asks != tuple(sorted(self.asks)):
            raise ValueError('asks must be ascending')
        if self.bids[0][0] > self.asks[0][0]:
            raise ValueError('crossed book')

    @property
    def mid(self) -> Decimal:
        return (self.bids[0][0] + self.asks[0][0]) / 2

    def top(self, side: Side) -> Decimal:
        return (self.asks if side is Side.BUY else self.bids)[0][0]

    def quote(self, side: Side, units: int, step: Decimal) -> tuple[Decimal, Decimal]:
        if type(units) is not int or units <= 0:
            raise ValueError('positive integer units required')
        remaining, cash, marginal = units, ZERO, ZERO
        for price, available in (self.asks if side is Side.BUY else self.bids):
            take = min(remaining, available)
            cash += price * take * step
            marginal = price
            remaining -= take
            if not remaining:
                return cash, marginal
        raise ValueError('insufficient executable depth')


@dataclass(frozen=True)
class Market:
    entropy: Book
    hedge: Book

    def valid_reason(self, now: float, cfg: Settings) -> str | None:
        if not math.isfinite(now):
            return 'invalid_clock'
        for name, book in (('entropy', self.entropy), ('hedge', self.hedge)):
            age = now - book.received_at
            if age < 0 or age > cfg.max_book_age_seconds:
                return f'{name}_book_stale_or_future'
        if abs(self.entropy.received_at - self.hedge.received_at) > cfg.max_book_skew_seconds:
            return 'book_skew'
        return None

    @property
    def mid_basis_bps(self) -> Decimal:
        return (self.entropy.mid / self.hedge.mid - 1) * BPS


@dataclass(frozen=True)
class OpenPlan:
    direction: Direction
    units: int
    reference_bps: Decimal
    executable_basis_bps: Decimal
    distance_bps: Decimal
    notional_usd: Decimal
    reference_scenario_usd: Decimal | None
    scenario_floor_usd: Decimal | None
    entry_quoted_cash_usd: Decimal
    entry_quoted_fees_usd: Decimal


@dataclass(frozen=True)
class ExitDecision:
    allowed: bool
    reason: str
    executable_net_usd: Decimal
    reversion_bps: Decimal
    executable_gross_usd: Decimal | None = None
    profit_bps: Decimal | None = None
    profit_entry_notional_usd: Decimal | None = None


def plan_open(market: Market, instrument: Instrument, cfg: Settings,
              reference_bps: Decimal, direction: Direction,
              units: int) -> OpenPlan | None:
    """Require executable opening deviation and each leg's actual notional cap."""
    if not reference_bps.is_finite():
        raise ValueError('finite explicit reference required')
    instrument.quantity(units)
    es = direction.entropy_side
    ec, _ = market.entropy.quote(es, units, instrument.quantity_step)
    hc, hedge_marginal = market.hedge.quote(es.opposite, units, instrument.quantity_step)
    notional = max(ec, hc)
    if notional > cfg.max_notional_usd:
        return None
    basis = (ec / hc - 1) * BPS
    distance = reference_bps - basis if es is Side.BUY else basis - reference_bps
    if distance < cfg.entry_band_bps:
        return None
    if cfg.open_hedge_hard_cap_bps is not None:
        rooms, cap = cfg.hedge_execution_policy('open')
        try:
            price_limit(market.hedge.top(es.opposite), hedge_marginal,
                        es.opposite, rooms[0], cap, instrument.hedge_tick)
        except ValueError:
            return None
    entry_cash = (-ec + hc) if es is Side.BUY else (ec - hc)
    entry_fees = (ec * instrument.entropy_fee_bps + hc * instrument.hedge_fee_bps) / BPS
    return OpenPlan(direction, units, reference_bps, basis, distance, notional,
                    None, None, entry_cash, entry_fees)


def exit_decision(market: Market, instrument: Instrument, cfg: Settings,
                  direction: Direction, units: int, entry_basis_bps: Decimal | None,
                  accrued_cash_net_usd: Decimal, *,
                  accrued_price_cash_usd: Decimal | None,
                  profit_entry_notional_usd: Decimal | None = None) -> ExitDecision:
    """Basis exit uses this cycle's signed fill cash plus full-depth close cash.

    Price cash includes actual opening and partial closing fills, never fees,
    funding or an execution reserve. Net value and reversion are diagnostics.
    Missing fill evidence cannot authorize a normal exit.
    """
    es = direction.entropy_side.opposite
    ec, _ = market.entropy.quote(es, units, instrument.quantity_step)
    hc, _ = market.hedge.quote(es.opposite, units, instrument.quantity_step)
    exit_cash = (ec - hc) if es is Side.SELL else (-ec + hc)
    fees = (ec * instrument.entropy_fee_bps + hc * instrument.hedge_fee_bps) / BPS
    net = accrued_cash_net_usd + exit_cash - fees
    basis = (ec / hc - 1) * BPS
    reversion = (ZERO if entry_basis_bps is None else
                 basis - entry_basis_bps if es is Side.SELL else entry_basis_bps - basis)
    if accrued_price_cash_usd is None:
        return ExitDecision(False, 'hold_missing_price_evidence', net, reversion)
    gross = dec(accrued_price_cash_usd) + exit_cash
    if profit_entry_notional_usd is None:
        return ExitDecision(False, 'hold_missing_entry_notional', net, reversion, gross)
    try:
        notional = dec(profit_entry_notional_usd)
    except (ValueError, TypeError, ArithmeticError):
        return ExitDecision(False, 'hold_missing_entry_notional', net, reversion, gross)
    if notional <= 0:
        return ExitDecision(False, 'hold_missing_entry_notional', net, reversion, gross)
    profit_bps = gross / notional * BPS
    allowed = profit_bps >= cfg.exit_min_gross_profit_bps
    return ExitDecision(allowed, 'gross_price_profit_exit' if allowed else 'hold',
                        net, reversion, gross, profit_bps, notional)


def price_limit(top: Decimal, marginal: Decimal, side: Side, room_bps: Decimal,
                hard_cap_bps: Decimal, tick: Decimal) -> Decimal:
    """Round to an executable tick without ever rounding through the hard cap.

    All distances start at CURRENT same-side top. Requote each retry. Room is a
    limit, not an expected cost. A zero-extra-room marketable IOC remains legal.
    """
    if any(not v.is_finite() for v in (top, marginal, room_bps, hard_cap_bps, tick)):
        raise ValueError('non-finite price input')
    if min(top, marginal, tick) <= 0 or room_bps < 0 or hard_cap_bps < 0:
        raise ValueError('invalid price input')
    r = min(room_bps, hard_cap_bps) / BPS
    hard = hard_cap_bps / BPS
    if side is Side.BUY:
        target = max(top * (1 + r), marginal)
        proposed = (target / tick).to_integral_value(rounding=ROUND_CEILING) * tick
        cap = (top * (1 + hard) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
        result = min(proposed, cap)
        if result < marginal:
            raise ValueError('depth outside physical hard cap')
    else:
        target = min(top * (1 - r), marginal)
        proposed = (target / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
        cap = (top * (1 - hard) / tick).to_integral_value(rounding=ROUND_CEILING) * tick
        result = max(proposed, cap)
        if result > marginal:
            raise ValueError('depth outside physical hard cap')
    if result <= 0:
        raise ValueError('invalid rounded limit')
    return result
