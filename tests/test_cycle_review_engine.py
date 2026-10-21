"""Offline behavioral checks for the complete-cycle review fixes."""
import asyncio
from dataclasses import replace
import time

import pytest

from entropy_arb.execution_cost import ExecutionCostModel
from entropy_arb.signal import StrategyAction
from test_profit_guard import engine, open_lot, candidate_plan


def test_small_tail_exits_below_open_policy_minimum():
    eng = engine()
    eng.cfg.profit_guard = replace(eng.cfg.profit_guard, min_profit_usd=0)
    eng.cfg.take_fraction = .25
    for venue in eng.venues.values():
        venue.min_quote = 1
    open_lot(eng, "entropy", "sell", .05, 102)
    open_lot(eng, "hedge", "buy", .05, 100)

    async def run():
        now = time.time()
        eng._scan(now)
        trade = eng._scan(now + .2)
        assert trade is not None and trade.plan.qty == pytest.approx(.05)
        assert trade.plan.buy_notional < eng.cfg.min_order_notional
        await eng._execute(trade)

    asyncio.run(run())
    assert eng._positions_flat()
    assert all(v.sent[0]["reduce_only"] for v in eng.venues.values())


def test_emergency_small_tail_uses_venue_minimum():
    eng = engine()
    for venue in eng.venues.values():
        venue.min_quote = 1
    open_lot(eng, "entropy", "sell", .05, 100)
    open_lot(eng, "hedge", "buy", .05, 100)
    assert asyncio.run(eng._emergency_exit_positions("margin_risk"))
    assert eng._positions_flat()
    assert all(v.sent[0]["reduce_only"] for v in eng.venues.values())


def test_small_balanced_residual_is_not_strict_flat():
    eng = engine()
    eng.entropy.position, eng.hedge.position = -.0001, .0001
    assert eng.entropy.position + eng.hedge.position == 0
    assert not eng._positions_flat()
    assert eng._inventory_flatness()["has_dust"]
    assert eng._pair_state().value == "short_entropy"


def test_deep_hedge_close_uses_marketable_bound_within_pnl_budget():
    eng = engine()
    eng.cfg.reduce_execution = replace(
        eng.cfg.reduce_execution,
        dynamic_hedge_reserve=replace(
            eng.cfg.reduce_execution.dynamic_hedge_reserve, enabled=True))
    eng.hedge.book.apply_hl([
        [{"px": "100", "sz": ".01"}, {"px": "99.9", "sz": "5"}],
        [{"px": "100.01", "sz": "5"}]])
    open_lot(eng, "entropy", "sell", 1, 102)
    open_lot(eng, "hedge", "buy", 1, 100)
    candidate, plan = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    trade = eng._execution_bounds(candidate, plan)
    assert trade is not None
    assert trade.sell_bound <= plan.sell_limit
    assert trade.cycle_budget["limit_net_profit_usd"] >= eng.cfg.profit_guard.min_profit_usd


def test_live_cost_prior_survives_ten_old_samples():
    model = ExecutionCostModel()
    for i in range(10):
        model.observe(phase="close_hedge", execution_id=str(i),
                      requested_qty=1, filled_qty=1, is_buy=True,
                      quoted_top_px=100, quoted_depth_avg_px=100,
                      avg_fill_px=100.001, ts=100)
    calm = model.estimate("close_hedge", now=100, is_buy=True,
                          latency_adverse_bps=.1)
    stress = model.estimate("close_hedge", now=100, is_buy=True,
                            latency_adverse_bps=8)
    expired = model.estimate("close_hedge", now=22000, is_buy=True,
                             latency_adverse_bps=.1)
    assert stress["expected_cost_bps"] > calm["expected_cost_bps"] + 1
    assert stress["live_weight"] >= .2 - 1e-9
    assert expired["usable_samples"] == 0


def test_short_hold_forecast_is_not_clamped_to_ten_minutes():
    eng = engine()
    eng.cfg.cycle_exit = replace(eng.cfg.cycle_exit, enabled=True,
                                 mode="fill_pnl", fast_exit_after_sec=20)
    assert eng._expected_cycle_hold_sec("sell_entropy") == 20
    eng._cycle_learning = lambda direction=None: {
        "duration": {"p90_sec": 15, "count": 10, "max_active_age_sec": 80}}
    assert eng._expected_cycle_hold_sec("sell_entropy") == 80


def test_action_budgets_do_not_count_other_actions_as_attempts():
    eng = engine()
    eng.entropy.orders_per_min = 30
    eng.cfg.reduce_execution = replace(eng.cfg.reduce_execution,
                                       open_attempts_per_min=2)
    for _ in range(3):
        eng._record_send(eng.entropy, StrategyAction.REDUCE)
    assert eng._venue_rate_ok(eng.entropy, StrategyAction.OPEN)
    for _ in range(2):
        eng._record_send(eng.entropy, StrategyAction.OPEN)
    assert not eng._venue_rate_ok(eng.entropy, StrategyAction.OPEN)
    assert eng._venue_rate_ok(eng.entropy, StrategyAction.REDUCE)


def test_calibration_retains_losing_recovery_outcomes():
    eng = engine()
    eng._cycle_learning = lambda direction=None: {"outcomes": {"outcomes": [
        {"net_pnl_usd": -.1, "entry_projected_cycle_pnl_usd": .01,
         "notional_usd": 100, "recovery_count": 1} for _ in range(10)]}}
    result = eng._calibrate_cycle_projection(.02, 100, "sell_entropy")
    assert result["calibrated_cycle_pnl_usd"] < 0
    assert result["cycle_model_risk_exit_rate"] == 1
    assert result["projected_cycle_pnl_usd"] == .02


def test_calibration_does_not_extrapolate_a_model_without_ranking_value():
    eng = engine()
    outcomes = []
    for index in range(10):
        outcomes.append({
            "net_pnl_usd": -0.03 - index * 0.001,
            "entry_projected_cycle_pnl_usd": -0.01 + index * 0.001,
            "notional_usd": 100,
        })
    eng._cycle_learning = lambda direction=None: {
        "outcomes": {"outcomes": outcomes}}

    result = eng._calibrate_cycle_projection(.02, 100, "sell_entropy")

    assert result["cycle_model_slope"] == 0
    assert result["cycle_model_mean_actual_bps"] < 0
    assert result["cycle_model_confidence_margin_bps"] > 0
    assert result["calibrated_cycle_pnl_usd"] < 0


def test_calibration_never_adds_optimism_to_the_costed_projection():
    eng = engine()
    eng._cycle_learning = lambda direction=None: {"outcomes": {"outcomes": [
        {"net_pnl_usd": .05 + index * .01,
         "entry_projected_cycle_pnl_usd": .01 + index * .01,
         "notional_usd": 100} for index in range(10)]}}

    result = eng._calibrate_cycle_projection(.02, 100, "sell_entropy")

    assert result["cycle_model_slope"] == pytest.approx(1)
    assert result["calibrated_cycle_pnl_usd"] <= .02


def test_policy_key_ignores_storage_but_changes_for_market_and_exit_policy():
    eng = engine()
    initial = eng._cycle_policy_key()
    eng.cfg.cycle_exit = replace(eng.cfg.cycle_exit, state_json='/temporary/other.json')
    assert eng._cycle_policy_key() == initial
    eng.cfg.entropy.symbol += '-OTHER'
    assert eng._cycle_policy_key() != initial
    market_key = eng._cycle_policy_key()
    eng.cfg.cycle_exit = replace(eng.cfg.cycle_exit, hard_timeout_sec=123)
    assert eng._cycle_policy_key() != market_key


def test_economic_cost_is_mean_and_tail_is_reported_separately():
    model = ExecutionCostModel()
    for index, price in enumerate([100,100,100,100.1]):
        model.observe(phase='open_first', execution_id=str(index), ts=100,
            requested_qty=1, filled_qty=1, is_buy=True,
            quoted_top_px=100, quoted_depth_avg_px=100, avg_fill_px=price)
    estimate = model.estimate('open_first', now=100, percentile=.95, prior_bps=0)
    assert estimate['empirical_cost_bps'] == pytest.approx(2.5)
    assert estimate['empirical_tail_cost_bps'] == pytest.approx(10)
    assert estimate['expected_cost_bps'] == pytest.approx(1)


def test_projection_calibration_does_not_mix_reference_regimes():
    eng = engine()
    eng._cycle_learning = lambda direction=None: {'outcomes': {'outcomes': [
        {'net_pnl_usd': -.1, 'entry_projected_cycle_pnl_usd': .01,
         'notional_usd': 100, 'entry_reference_state': 'lagging',
         'exit_mode': 'hard_timeout'} for _ in range(10)]}}
    result = eng._calibrate_cycle_projection(.02,100,'sell_entropy','normal')
    assert result['cycle_model_samples'] == 0
    assert not result['cycle_model_empirical_ready']
    loss = eng._calibrate_cycle_projection(.02,100,'sell_entropy','lagging')
    assert loss['calibrated_cycle_pnl_usd'] < 0
    assert loss['cycle_model_risk_exit_rate'] == 1


def test_finer_venue_fill_cannot_hide_under_shared_pair_step():
    eng = engine()
    eng.entropy.size_decimals, eng.hedge.size_decimals = 3, 5
    eng._step = .001
    eng.hedge.position = .00001
    assert not eng._positions_flat()
    assert not eng._positions_flat(eng.cfg.net_tolerance_base)
    assert eng._pair_state().value == 'unbalanced'
    assert eng._inventory_flatness()['has_dust']


def test_exchange_specific_taker_minima_and_exact_reduce_only_close():
    eng = engine()
    e, h = eng.entropy, eng.hedge
    e.kind, h.kind = 'hl', 'lighter'
    e.position, h.position = -.0001, .0001
    h.min_base = .005
    assert eng._taker_minimums(h) == (.0001, 0)
    assert eng._taker_minimums(e, reduce_only=True, qty=.0001)[1] == 0
    assert eng._taker_minimums(e, reduce_only=False, qty=.0001)[1] == 10
    e.position = -.0002
    assert eng._taker_minimums(e, reduce_only=True, qty=.0001)[1] == 10
    e.position = -.0001
    assert asyncio.run(eng._emergency_exit_positions('manual_flatten'))
    assert eng._positions_flat()
    assert all(v.sent[0]['qty'] == .0001 and v.sent[0]['reduce_only'] for v in (e,h))


def test_normal_reduce_can_finish_below_shared_open_minimum():
    eng = engine()
    eng.entropy.kind, eng.hedge.kind = 'hl', 'lighter'
    eng.hedge.min_base = eng._min_base = .005
    eng.cfg.cycle_exit = replace(eng.cfg.cycle_exit, enabled=True, mode='fill_pnl')
    open_lot(eng, 'entropy', 'sell', .0001, 102)
    open_lot(eng, 'hedge', 'buy', .0001, 100)
    async def run():
        eng._scan(time.time())
        trade = eng._scan(time.time()+.2)
        assert trade is not None
        assert trade.plan.qty == .0001
        await eng._execute(trade)
    asyncio.run(run())
    assert eng._positions_flat()
