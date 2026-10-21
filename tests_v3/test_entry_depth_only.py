"""Offline checks for sizing from the two actual opening sides only."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D, ROUND_CEILING
import time
from types import SimpleNamespace

import pytest

from entropy_arb.v3 import runtime as runtime_module
from entropy_arb.v3.core import (
    BPS, Book, Direction, Instrument, Market, OpenPlan, Settings, Side,
    plan_open, price_limit,
)
from entropy_arb.v3.runtime import choose_open_plan
from tests_v3.test_ab_opening import finish_zero, open_once, setup_ab


def instrument(*, fees=('0', '0')):
    return Instrument('pair', D('.01'), D('.01'), D('.01'), *map(D, fees))


def book(bids, asks, *, name, now=None):
    return Book(tuple((D(p), q) for p, q in bids),
                tuple((D(p), q) for p, q in asks),
                time.time() if now is None else now, name)


def opening_market(direction, *, depth=400, reverse=1):
    now = time.time()
    if direction is Direction.LONG_ENTROPY:
        e = book([('99.89', reverse)], [('99.90', depth)], name='e', now=now)
        h = book([('100', depth)], [('100.01', reverse)], name='h', now=now)
    else:
        e = book([('100.10', depth)], [('100.11', reverse)], name='e', now=now)
        h = book([('99.99', reverse)], [('100', depth)], name='h', now=now)
    return Market(e, h)


def venue(*, min_base='.01', min_quote='0'):
    return SimpleNamespace(min_base=D(min_base), min_quote=D(min_quote))


def choose(market, *, inst=None, cfg=None, reference='0', e=None, h=None):
    return choose_open_plan(market, inst or instrument(),
        cfg or Settings(max_notional_usd=D('200')), D(reference),
        entropy_venue=e or venue(), hedge_venue=h or venue())


@pytest.mark.parametrize('direction,expected', [
    (Direction.LONG_ENTROPY, 200), (Direction.SHORT_ENTROPY, 199),
])
def test_reverse_one_unit_does_not_limit_fixed_plan_or_largest_opening(direction, expected):
    market, inst, cfg = opening_market(direction), instrument(), Settings(max_notional_usd=D('200'))
    assert market.valid_reason(time.time(), cfg) is None
    for leg, side in ((market.entropy, direction.entropy_side.opposite),
                      (market.hedge, direction.entropy_side)):
        with pytest.raises(ValueError, match='insufficient executable depth'):
            leg.quote(side, expected, inst.quantity_step)
    fixed = plan_open(market, inst, cfg, D('0'), direction, expected)
    picked = choose(market, inst=inst, cfg=cfg)
    assert fixed is not None and picked is not None
    assert picked.direction is direction and picked.units == expected
    assert fixed.reference_scenario_usd is picked.reference_scenario_usd is None
    assert fixed.scenario_floor_usd is picked.scenario_floor_usd is None
    for leg, side in ((market.entropy, direction.entropy_side),
                      (market.hedge, direction.entropy_side.opposite)):
        cash, _ = leg.quote(side, expected, inst.quantity_step)
        assert cash <= D('200')
    assert plan_open(market, inst, cfg, D('0'), direction, expected + 1) is None
    assert inst.exact_units(inst.quantity(expected)) == expected


@pytest.mark.parametrize('direction', list(Direction))
def test_opening_does_not_quote_reverse_depth(direction, monkeypatch):
    market = opening_market(direction)
    original, calls = Book.quote, []
    def quote(self, side, units, step):
        expected = direction.entropy_side if self is market.entropy else direction.entropy_side.opposite
        assert side is expected, 'opening selection must not request a simulated closing quote'
        calls.append((self.revision, side, units))
        return original(self, side, units, step)
    monkeypatch.setattr(Book, 'quote', quote)
    decision = choose(market)
    assert decision is not None and decision.units > 1
    assert {row[0] for row in calls} == {'e', 'h'}


@pytest.mark.parametrize('direction', list(Direction))
def test_high_fees_allowance_and_sampling_floor_are_diagnostics_only(direction):
    market, inst = opening_market(direction), instrument(fees=('100', '250'))
    cfg = Settings(max_notional_usd=D('200'), execution_allowance_bps=D('1000'),
                   sampling_floor_bps=D('0'))
    decision = choose(market, inst=inst, cfg=cfg)
    assert decision is not None and decision.units == (200 if direction is Direction.LONG_ENTROPY else 199)
    ec, _ = market.entropy.quote(direction.entropy_side, decision.units, inst.quantity_step)
    hc, _ = market.hedge.quote(direction.entropy_side.opposite, decision.units, inst.quantity_step)
    assert decision.entry_quoted_fees_usd == (ec * D('100') + hc * D('250')) / BPS
    assert decision.entry_quoted_cash_usd == (-ec + hc if direction is Direction.LONG_ENTROPY else ec - hc)
    assert decision.reference_scenario_usd is decision.scenario_floor_usd is None


@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('distance,allowed', [('4.9999', False), ('5', True), ('5.0001', True)])
def test_actual_executable_five_bp_boundary(direction, distance, allowed):
    price = D('100') * (1 + (D(distance) / BPS) * direction.entropy_side.sign * -1)
    now = time.time()
    market = Market(book([(str(price), 300)], [(str(price), 300)], name='e', now=now),
                    book([('100', 300)], [('100', 300)], name='h', now=now))
    decision = choose(market)
    assert (decision is not None) is allowed
    if allowed:
        assert decision.direction is direction and decision.distance_bps == D(distance)


@pytest.mark.parametrize('direction', list(Direction))
def test_entropy_physical_price_protection_still_downsizes(direction):
    now = time.time()
    if direction is Direction.LONG_ENTROPY:
        e = book([('99.89', 1)], [('99.9', 150), ('100.1', 100)], name='e', now=now)
        h = book([('100', 500)], [('100.01', 1)], name='h', now=now)
    else:
        e = book([('100.1', 150), ('99.9', 100)], [('100.11', 1)], name='e', now=now)
        h = book([('99.99', 1)], [('100', 500)], name='h', now=now)
    market, inst, cfg = Market(e, h), instrument(), Settings(max_notional_usd=D('200'))
    assert plan_open(market, inst, cfg, D('0'), direction, 151) is not None
    decision = choose(market, inst=inst, cfg=cfg)
    assert decision is not None and decision.direction is direction and decision.units == 150
    rooms, cap = cfg.first_leg_execution_policy('open')
    _, marginal = e.quote(direction.entropy_side, 151, inst.quantity_step)
    with pytest.raises(ValueError, match='depth outside physical hard cap'):
        price_limit(e.top(direction.entropy_side), marginal, direction.entropy_side,
                    rooms[0], cap, inst.entropy_tick)


@pytest.mark.parametrize('leg', ['entropy', 'hedge'])
@pytest.mark.parametrize('depth,allowed', [(2, False), (3, True)])
def test_minimum_base_rounds_up_on_common_grid(leg, depth, allowed):
    market = opening_market(Direction.LONG_ENTROPY, depth=depth)
    kwargs = {'e' if leg == 'entropy' else 'h': venue(min_base='.0201')}
    decision = choose(market, **kwargs)
    assert (decision is not None) is allowed
    if allowed:
        assert decision.units == 3 and instrument().quantity(decision.units) == D('.03')


@pytest.mark.parametrize('direction,minimum_leg', [
    (Direction.LONG_ENTROPY, 'entropy'), (Direction.SHORT_ENTROPY, 'hedge'),
])
@pytest.mark.parametrize('minimum,allowed', [('199.8995', True), ('199.89951', False)])
def test_minimum_quote_uses_actual_multilevel_opening_cash(direction, minimum_leg, minimum, allowed):
    now = time.time()
    low = book([('99.89', 1)], [('99.9', 1), ('99.95', 400)], name='low', now=now)
    high = book([('100', 400)], [('100.01', 1)], name='high', now=now)
    market = Market(low, high) if direction is Direction.LONG_ENTROPY else Market(high, low)
    kwargs = {'e' if minimum_leg == 'entropy' else 'h': venue(min_quote=minimum)}
    decision = choose(market, **kwargs)
    assert (decision is not None) is allowed
    if allowed:
        assert decision.direction is direction and decision.units == 200
        leg = market.entropy if minimum_leg == 'entropy' else market.hedge
        side = direction.entropy_side if minimum_leg == 'entropy' else direction.entropy_side.opposite
        assert leg.quote(side, decision.units, instrument().quantity_step)[0] == D('199.8995')


@pytest.mark.parametrize('leg', ['entropy', 'hedge'])
def test_each_venue_minimum_quote_above_its_cap_rejects(leg):
    kwargs = {'e' if leg == 'entropy' else 'h': venue(min_quote='200.00001')}
    assert choose(opening_market(Direction.LONG_ENTROPY), **kwargs) is None


def test_directions_have_independent_opening_depth_and_cash_caps():
    now = time.time()
    market = Market(book([('100', 100)], [('100.01', 200)], name='e', now=now),
                    book([('100.10', 300)], [('100.11', 400)], name='h', now=now))
    long = choose(market, reference='30')
    short = choose(market, reference='-30')
    assert long is not None and long.direction is Direction.LONG_ENTROPY and long.units == 199
    assert short is not None and short.direction is Direction.SHORT_ENTROPY and short.units == 100


@pytest.mark.parametrize('distances,quantities,winner', [
    (('4', '5'), (180, 150), 1),
    (('4', '4'), (180, 150), 0),
    (('4', '4'), (180, 180), 0),
])
def test_candidate_ranking_keeps_distance_then_units_then_iteration_order(monkeypatch, distances, quantities, winner):
    # Healthy uncrossed books cannot signal both directions simultaneously.
    # Isolate the ranking of synthetic eligible candidates, not market physics.
    directions = [SimpleNamespace(entropy_side=Side.BUY, label=i) for i in range(2)]
    monkeypatch.setattr(runtime_module, 'Direction', directions)
    def eligible(market, inst, cfg, reference, direction, units):
        i = direction.label
        if units > quantities[i]:
            return None
        return OpenPlan(direction, units, reference, D('-10'), D(distances[i]),
                        inst.quantity(units) * 100, None, None, D('0'), D('0'))
    monkeypatch.setattr(runtime_module, 'plan_open', eligible)
    decision = choose(opening_market(Direction.LONG_ENTROPY, reverse=400))
    assert decision is not None and decision.direction is directions[winner]
    assert decision.units == quantities[winner]


def exhaustive_opening(market, inst, cfg, reference, e, h):
    """Independent oracle: greatest eligible quantity per direction, then rank."""
    minimum = max(1, *(int((v.min_base / inst.quantity_step).to_integral_value(rounding=ROUND_CEILING))
                       for v in (e, h)))
    candidates = []
    for direction in Direction:
        side = direction.entropy_side
        depth = min(sum(q for _, q in (market.entropy.asks if side is Side.BUY else market.entropy.bids)),
                    sum(q for _, q in (market.hedge.bids if side is Side.BUY else market.hedge.asks)))
        feasible = []
        for units in range(minimum, depth + 1):
            ec, marginal = market.entropy.quote(side, units, inst.quantity_step)
            hc, _ = market.hedge.quote(side.opposite, units, inst.quantity_step)
            basis = (ec / hc - 1) * BPS
            distance = reference - basis if side is Side.BUY else basis - reference
            if (max(ec, hc) > cfg.max_notional_usd or ec < e.min_quote or hc < h.min_quote
                    or distance < cfg.entry_band_bps):
                continue
            rooms, cap = cfg.first_leg_execution_policy('open')
            try:
                limit = price_limit(market.entropy.top(side), marginal, side,
                                    rooms[0], cap, inst.entropy_tick)
            except ValueError:
                continue
            feasible.append((direction, units, distance, max(ec, hc), limit))
        if feasible:
            candidates.append(feasible[-1])
    return max(candidates, key=lambda item: (item[2], item[1])) if candidates else None


@pytest.mark.parametrize('reference', ['-30', '-3', '0', '3', '30'])
@pytest.mark.parametrize('profile', ['shallow_reverse', 'multi_level', 'protection', 'exact_three'])
def test_choose_matches_exhaustive_two_side_policy(profile, reference):
    now = time.time()
    if profile == 'shallow_reverse':
        market = opening_market(Direction.LONG_ENTROPY, depth=230)
    elif profile == 'multi_level':
        market = Market(book([('100.1', 81), ('100.07', 39)], [('100.11', 43), ('100.15', 187)], name='e', now=now),
                        book([('99.99', 109), ('99.95', 121)], [('100', 67), ('100.02', 163)], name='h', now=now))
    elif profile == 'protection':
        market = Market(book([('99.89', 1)], [('99.9', 37), ('100.1', 193)], name='e', now=now),
                        book([('100', 230)], [('100.01', 1)], name='h', now=now))
    else:
        market = Market(book([('99.97', 17), ('99.96', 213)], [('99.97', 29), ('99.98', 201)], name='e', now=now),
                        book([('100', 230)], [('100', 230)], name='h', now=now))
    inst = instrument(fees=('100', '250'))
    cfg = Settings(max_notional_usd=D('200'), execution_allowance_bps=D('500'), sampling_floor_bps=D('0'))
    e, h = venue(min_base='.0201', min_quote='10'), venue(min_base='.01', min_quote='12')
    expected = exhaustive_opening(market, inst, cfg, D(reference), e, h)
    actual = choose(market, inst=inst, cfg=cfg, reference=reference, e=e, h=h)
    if expected is None:
        assert actual is None
    else:
        assert actual is not None
        assert (actual.direction, actual.units, actual.distance_bps,
                actual.notional_usd, actual.entropy_limit_px) == expected


def test_basic_book_validity_and_market_clock_guards_remain():
    with pytest.raises(ValueError, match='missing executable book'):
        book([], [('100', 1)], name='e')
    with pytest.raises(ValueError, match='crossed book'):
        book([('101', 1)], [('100', 1)], name='e')
    cfg = Settings()
    market = opening_market(Direction.LONG_ENTROPY)
    now = max(market.entropy.received_at, market.hedge.received_at)
    assert market.valid_reason(now, cfg) is None
    assert market.valid_reason(now + cfg.max_book_age_seconds + .1, cfg) == 'entropy_book_stale_or_future'
    assert market.valid_reason(market.entropy.received_at - .1, cfg) == 'entropy_book_stale_or_future'
    skewed = replace(market, entropy=replace(market.entropy, received_at=now - .6),
                     hedge=replace(market.hedge, received_at=now))
    assert skewed.valid_reason(now, cfg) == 'book_skew'


@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('group', ['A', 'B'])
def test_ab_shared_opening_guards_allow_shallow_reverse_and_keep_execution_policy(tmp_path, direction, group):
    runtime, e, h, journal = setup_ab(tmp_path)
    async def scenario():
        await runtime.initialize()
        if group == 'B':
            e.fill_fractions = h.fill_fractions = [0]
            await open_once(runtime)
            await finish_zero(runtime)
            journal.rearm_opening_opportunity(runtime.settings.opening_experiment_id)
        prices = (('99.89', '99.90', '100', '100.01') if direction is Direction.LONG_ENTROPY
                  else ('100.10', '100.11', '99.99', '100'))
        eb, ea, hb, ha = prices
        for v, bid, ask in ((e, eb, ea), (h, hb, ha)):
            opening_buy = (direction.entropy_side is Side.BUY) if v is e else (direction.entropy_side is Side.SELL)
            v.book.apply_hl([[{'px': bid, 'sz': '.0001' if opening_buy else '2'}],
                             [{'px': ask, 'sz': '2' if opening_buy else '.0001'}]])
        before = (len(e.sent), len(h.sent))
        policies = (runtime.settings.first_leg_execution_policy('open'),
                    runtime.settings.hedge_rooms_bps, runtime.settings.hedge_hard_cap_bps,
                    runtime.settings.exit_min_gross_profit_bps)
        ready, entered = asyncio.Event(), []
        for v in (e, h):
            original = v.send_taker
            async def send(*, v=v, original=original, **kwargs):
                if group == 'A':
                    assert runtime._lighter_increase_guard(kwargs['order_ref']) is None
                    assert len(journal.pending()) == 2
                else:
                    if v is h:
                        first = [o for o in journal.cycle_orders(journal.active_live_cycle()['cycle_id']) if o['venue'] == 'entropy'][0]
                        assert first['state'] == 'TERMINAL' and first['filled_units'] > 0
                entered.append((v.key, kwargs))
                if len(entered) == 2:
                    ready.set()
                if group == 'A':
                    await ready.wait()
                return await original(**kwargs)
            v.send_taker = send
        decision = choose_open_plan(runtime._market(), runtime.instrument, runtime.settings,
            D('0'), entropy_venue=e, hedge_venue=h)
        assert decision is not None and decision.units == (1500 if direction is Direction.LONG_ENTROPY else 1498)
        await asyncio.wait_for(runtime._maybe_open(runtime._market(), D('0')), 2)
        cycle = journal.active_live_cycle()
        assert cycle is not None and cycle['phase'] == 'OPEN'
        assert cycle['direction'] == direction.value
        assert cycle['entropy_units'] == direction.entropy_side.sign * decision.units
        assert cycle['hedge_units'] == -cycle['entropy_units']
        assert cycle['basis_profit_basis_json'] is not None and not journal.pending()
        assert (len(e.sent), len(h.sent)) == (before[0] + 1, before[1] + 1)
        assert all(not row['reduce_only'] for _, row in entered)
        assert entered[0][1]['qty'] == entered[1][1]['qty']
        for key, row in entered:
            assert row['is_buy'] is ((direction.entropy_side is Side.BUY) if key == 'entropy' else (direction.entropy_side is Side.SELL))
        state = journal.opening_experiment_state(runtime.settings.opening_experiment_id)
        assert state['attempts'] == (1 if group == 'A' else 2)
        assert state['current_attempt']['group_name'] == group
        assert policies == (runtime.settings.first_leg_execution_policy('open'),
                            runtime.settings.hedge_rooms_bps, runtime.settings.hedge_hard_cap_bps,
                            runtime.settings.exit_min_gross_profit_bps)
        assert policies[1] == (D('4'),) and policies[2] == D('12') and policies[3] == D('2.5')
    try:
        asyncio.run(scenario())
    finally:
        journal.close()
