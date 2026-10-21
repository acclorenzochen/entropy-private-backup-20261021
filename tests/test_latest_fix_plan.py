"""Focused regressions for the latest execution-fix plan."""
import asyncio
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from entropy_arb.book import ArbPlan
from entropy_arb.config import (BootstrapConfig, DynamicHedgeReserveConfig,
                                DynamicOpenFirstLegConfig,
                                ReduceExecutionConfig)
from entropy_arb.engine import (Engine, PlannedTrade, TradeCandidate,
                                _choose_open_execution_limit)
from entropy_arb.execution_cost import ExecutionCostModel
from entropy_arb.execution_state import ExecutionState
from entropy_arb.signal import StrategyAction, MidlineLagState
from entropy_arb.trade_ledger import TradeLedger
from test_profit_guard import candidate_plan, engine, open_lot


def _short_cycle_engine(tmp_path, *, campaign_sec=0.0):
    eng = engine(midline=20)
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        normal_min_profit_usd=0.0, fast_exit_after_sec=30.0,
        fast_exit_min_profit_usd=-0.01,
        state_json=str(tmp_path / "cycle.json"))
    eng.cfg.reduce_execution = ReduceExecutionConfig(
        exit_campaign_sec=campaign_sec,
        retry_interval_sec=0.001,
        dynamic_hedge_reserve=DynamicHedgeReserveConfig(
            enabled=True, min_bps=2.0, max_bps=8.0,
            adverse_move_percentile=0.95, safety_margin_bps=1.0))
    open_lot(eng, "entropy", "sell", 1.0, 110.0, suffix="open-e")
    open_lot(eng, "hedge", "buy", 1.0, 100.0, suffix="open-h")
    eng.cycle.start(
        direction="short_entropy", opened_ts=time.time() - 10.0,
        entry_midline_bps=20.0, entry_sigma_bps=1.0, entry_band_bps=4.0,
        entry_threshold_bps=4.0, paired_qty=1.0)
    eng.entropy.set_book(99.9, 100.0)
    eng.hedge.set_book(99.0, 100.0)
    return eng


def test_dynamic_cycle_anchor_roundtrip_uses_one_reference_coordinate(tmp_path):
    eng = _short_cycle_engine(tmp_path)
    eng.cycle.start(
        direction="short_entropy", opened_ts=time.time() - 10.0,
        entry_midline_bps=7.0, entry_sigma_bps=1.0, entry_band_bps=4.0,
        entry_threshold_bps=4.0, paired_qty=1.0,
        entry_reference_bps=7.0, entry_reference_mode="dynamic_center",
        entry_active_midline_bps=9.0, entry_dynamic_center_bps=7.0)
    restored = type(eng.cycle).from_dict(eng.cycle.to_dict())
    eng.cycle = restored

    dynamic = replace(
        eng.model_snapshot, active_midline_bps=9.0,
        short_center_bps=10.0, long_center_bps=12.0,
        midline_lag_state=MidlineLagState.BLOCKED)
    assert eng._cycle_drift(11.0, dynamic) == pytest.approx(4.0)

    # A temporary missing robust center falls back to the last active model
    # value, never to the configured seed.
    missing_dynamic = replace(
        dynamic, short_center_bps=None, long_center_bps=None,
        active_midline_bps=9.5)
    assert eng._cycle_drift(9.5, missing_dynamic) == pytest.approx(2.5)


def test_dynamic_hedge_reserve_is_clamped_to_configured_bounds(tmp_path):
    eng = _short_cycle_engine(tmp_path)
    now = time.time()
    eng._hedge_latency_sec_ewma = 0.05
    eng._record_hedge_quote(now - 1.0)
    eng.hedge.set_book(99.0, 100.5)
    eng._record_hedge_quote(now)
    metrics = eng._hedge_reserve_metrics(True, 1.0, now)
    assert metrics["adverse_move_p95_bps"] > 8.0
    assert metrics["reserve_bps"] == pytest.approx(8.0)


def test_economic_close_hedge_uses_completion_hard_cap(tmp_path):
    eng = _short_cycle_engine(tmp_path)
    eng.cycle.cycle_id = "inventory-cycle"
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    reserved = trade.cycle_budget["hedge_reserved_limit_px"]

    asyncio.run(eng._execute(trade))

    hard_cap = eng._execution_hard_cap_bps(StrategyAction.REDUCE, "hedge")
    completion_bound = eng._live_taker_bound(
        eng.hedge, False, hard_cap)
    assert eng.hedge.sent[-1]["limit_px"] == pytest.approx(completion_bound)
    assert eng.hedge.sent[-1]["limit_px"] != pytest.approx(reserved)
    assert eng._last_cycle_telemetry["hedge_completion_mode"] is True
    assert eng._pair_state().value == "flat"


def test_close_positive_budget_is_allowed_below_diagnostic_room(tmp_path):
    eng = _short_cycle_engine(tmp_path)
    eng.cfg.bootstrap = BootstrapConfig(
        enabled=True, close_min_execution_room_bps=1.5)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    budget = eng._cycle_profit_budget(c, p)
    budget.update(
        net_profit_usd=0.0008, allowed_floor_usd=0.0,
        exit_reason="normal_profit", exit_class="economic",
        bootstrap_cycle=True, entry_mode="bootstrap")

    trade = eng._build_pnl_protected_close_bound(
        c, p, budget, 0.0, apply_physical_bound=True)

    assert trade is not None
    assert budget["exit_budget_bps"] > 0
    assert budget["close_execution_room_bps"] < 1.5
    assert budget["allowed"] is True


def test_close_fast_floor_can_spend_small_negative_budget(tmp_path):
    eng = _short_cycle_engine(tmp_path)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    budget = eng._cycle_profit_budget(c, p)
    budget.update(
        net_profit_usd=-0.0018, allowed_floor_usd=-0.002,
        exit_reason="fast_exit", exit_class="economic")

    trade = eng._build_pnl_protected_close_bound(
        c, p, budget, -0.002, apply_physical_bound=True)

    assert trade is not None
    assert budget["exit_economic_budget_usd"] == pytest.approx(0.0002)
    assert budget["allowed"] is True


def test_close_without_remaining_economic_budget_is_rejected(tmp_path):
    eng = _short_cycle_engine(tmp_path)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    budget = eng._cycle_profit_budget(c, p)
    budget.update(net_profit_usd=-0.003, allowed_floor_usd=-0.002)

    trade = eng._build_pnl_protected_close_bound(
        c, p, budget, -0.002, apply_physical_bound=True)

    assert trade is None
    assert budget["reason"] == "close_pnl_budget_insufficient"


def test_dynamic_exit_floor_is_used_after_a_long_hold(tmp_path):
    eng = _short_cycle_engine(tmp_path)
    eng.cycle.opened_ts = time.time() - 180.0
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)

    budget = eng._cycle_profit_budget(c, p)

    assert budget["dynamic_exit_floor_bps"] == -1.0
    assert budget["allowed_floor_usd"] < 0.0


def test_economic_exit_campaign_keeps_room_within_budget_after_zero_fills(tmp_path):
    eng = _short_cycle_engine(tmp_path, campaign_sec=0.02)
    eng.cfg.max_reduce_retry_attempts = 4
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    eng.entropy.next_results = [
        {"status": "canceled", "filled_base": 0.0, "avg_px": None,
         "err": None, "unresolved": False}
        for _ in range(4)]

    asyncio.run(eng._execute(trade))

    assert len(eng.entropy.sent) == 4
    assert eng.hedge.sent == []
    limits = [round(item["limit_px"], 8) for item in eng.entropy.sent]
    assert limits == sorted(limits)
    assert len(set(limits)) > 1
    assert eng._last_exit_campaign["final_result"] == "zero_fill"
    assert eng._last_exit_campaign["retry_count"] == 3
    assert eng._exit_campaign_telemetry["missed_profitable_exit_count"] == 1


def _open_policy(**kwargs):
    values = {
        "open_retry_campaign_sec": 0.0,
        "open_retry_interval_sec": 0.001,
        "open_retry_max_attempts": 1,
        "open_execution_room_min_bps": 0.0,
        "reserve_reduce_slots": 2,
        "dynamic_open_first_leg": DynamicOpenFirstLegConfig(),
    }
    values.update(kwargs)
    return ReduceExecutionConfig(**values)


def test_dynamic_open_first_leg_reserve_is_clamped_and_added_to_live_quote():
    eng = engine()
    eng.cfg.reduce_execution = _open_policy(
        dynamic_open_first_leg=DynamicOpenFirstLegConfig(
            enabled=True, min_bps=3.5, max_bps=8.0,
            adverse_move_percentile=0.90, safety_margin_bps=0.5))
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    now = time.time()
    eng._open_latency_sec_ewma = 0.05
    eng._record_open_quote(now - 0.1)
    eng.entropy.set_book(99.80, 99.90)
    eng._record_open_quote(now)

    metrics = eng._open_first_leg_metrics(c, p, now)

    assert 3.5 <= metrics["adverse_move_reserve_bps"] <= 8.0
    assert metrics["bound_px"] < p.sell_limit
    assert metrics["first_leg_room_bps"] >= 3.5


def test_dynamic_open_reserve_uses_latency_horizon_not_adjacent_quote_move():
    eng = engine()
    eng.cfg.reduce_execution = _open_policy(
        dynamic_open_first_leg=DynamicOpenFirstLegConfig(
            enabled=True, min_bps=0.0, max_bps=20.0,
            adverse_move_percentile=0.90, safety_margin_bps=0.0))
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    now = time.time()
    eng._open_latency_sec_ewma = 0.8
    eng.entropy.set_book(100.0, 101.0)
    eng._record_open_quote(now - 0.8)
    eng.entropy.set_book(99.96, 100.96)
    eng._record_open_quote(now - 0.4)
    eng.entropy.set_book(99.92, 100.92)
    eng._record_open_quote(now)

    metrics = eng._open_first_leg_metrics(c, p, now)

    assert metrics["latency_horizon_sec"] == pytest.approx(0.8)
    assert metrics["adverse_move_p90_bps"] == pytest.approx(
        (100.0 / 99.92 - 1.0) * 1e4)


def test_dynamic_open_reserve_prefers_recent_latency_p90():
    eng = engine()
    eng._open_latency_sec_ewma = 0.2
    eng._open_latency_samples_sec.extend((0.1, 0.2, 0.8, 1.0))

    assert eng._open_latency_horizon_sec() == pytest.approx(1.0)


def test_bootstrap_does_not_override_model_warmup():
    eng = object.__new__(Engine)
    eng.record_only = False
    eng.flatten_only = False
    eng._bootstrap_state = {"disabled": False}
    eng.cfg = SimpleNamespace(
        dynamic_warmup_sec=1800.0,
        dynamic_min_samples=600,
        bootstrap=BootstrapConfig(
            enabled=True, dynamic_midline_warmup_sec=180.0,
            dynamic_midline_min_samples=120))

    assert eng._dynamic_model_warmup() == (1800.0, 600)

    eng._bootstrap_state = {"disabled": True}
    assert eng._dynamic_model_warmup() == (1800.0, 600)


@pytest.mark.parametrize(
    ("economic_bound", "physical_bound", "expected_limit", "allowed"),
    ((100.08, 100.10, 100.08, True),   # 8 bps economics, 10 bps hard cap
     (100.15, 100.10, 100.10, True),   # 15 bps economics, capped at 10
     (100.02, 100.10, 100.02, False))) # 2 bps room, below 3.5 bps required
def test_open_limit_uses_economic_room_before_absolute_hard_cap(
        economic_bound, physical_bound, expected_limit, allowed):
    """Latency admission does not clip or widen the economic IOC room."""
    result = _choose_open_execution_limit(
        economic_bound=economic_bound, physical_bound=physical_bound,
        current_executable=100.0, latency_required_room_bps=3.5,
        is_buy=True)
    assert result["allowed"] is allowed
    assert result["limit_px"] == pytest.approx(expected_limit)


def test_pnl_budgeted_open_limit_does_not_use_stress_as_a_second_cap():
    """The engine caller passes only the absolute hard cap as physical bound."""
    entropy = SimpleNamespace(
        fee_bps=0.0,
        px_round=lambda px, round_up: px,
    )
    hedge = object()
    eng = object.__new__(Engine)
    eng.entropy = entropy
    eng.cfg = SimpleNamespace(
        economic_execution=SimpleNamespace(
            min_expected_cycle_profit_usd=0.003),
        reduce_execution=SimpleNamespace(open_execution_room_min_bps=3.5),
        min_edge_surplus_bps=0.0,
    )
    candidate = TradeCandidate(
        direction="sell_entropy", action=StrategyAction.OPEN,
        buy=hedge, sell=entropy, threshold_bps=0.0, cap_notional=100.0,
        reduce_only=False, funding_cost_bps=0.0,
        pair_state_before=None)
    plan = ArbPlan(
        qty=1.0, buy_limit=100.0, sell_limit=99.94,
        buy_notional=100.0, sell_notional=99.94, q_max=1.0,
        q_max_notional=100.0, top_premium_bps=-6.0,
        marginal_premium_bps=-6.0, buy_fee=0.0, sell_fee=0.0)

    eng._execution_hard_cap_bps = lambda *_args: 10.0
    eng._live_taker_bound = lambda *_args: 99.90
    eng._quote_snapshot = lambda *_args: {"marginal_px": 100.0}

    def hedge_bound(_is_buy, _qty, _now, *, expected, **_kwargs):
        return (100.0, {"reserve_bps": 0.0,
                        "expected_reserve_bps": 0.0})

    eng._hedge_bound_for_side = hedge_bound
    result = eng._build_pnl_budgeted_open_bound(
        candidate, plan,
        {"net_profit_usd": 0.023},
        {"bound_px": 99.965, "latency_required_room_bps": 3.5,
         "adverse_move_reserve_bps": 3.5},
        100.0)

    assert result is not None
    assert result["physical_stress_bound_px"] == pytest.approx(99.965)
    assert result["physical_hard_cap_px"] == pytest.approx(99.90)
    assert result["physical_entropy_limit_px"] == pytest.approx(99.90)
    assert result["entropy_limit_px"] == pytest.approx(99.965)
    assert (result["first_leg_execution_room_bps"]
            >= result["latency_required_room_bps"])


def test_open_opportunity_id_is_unique_across_restart_and_event_persists(tmp_path):
    path = tmp_path / "opportunities.sqlite3"
    first = engine()
    first.ledger.close()
    first.ledger = TradeLedger(path)
    second = None
    try:
        candidate, _ = candidate_plan(first, "sell_entropy", StrategyAction.OPEN)
        first_opportunity = first._ensure_signal_opportunity(candidate, now=100.0)
        first_events = first.ledger.load_opportunity_events(
            opportunity_id=first_opportunity["opportunity_id"])
        assert any(event["stage"] == "statistical_signal"
                   for event in first_events)
        first.ledger.close()

        second = engine()
        second.ledger.close()
        second.ledger = TradeLedger(path)
        candidate, _ = candidate_plan(second, "sell_entropy", StrategyAction.OPEN)
        second_opportunity = second._ensure_signal_opportunity(candidate, now=101.0)
        assert (second_opportunity["opportunity_id"]
                != first_opportunity["opportunity_id"])
        second_events = second.ledger.load_opportunity_events(
            opportunity_id=second_opportunity["opportunity_id"])
        assert len(second_events) == 1
        assert second_events[0]["stage"] == "statistical_signal"
    finally:
        if second is not None:
            second.ledger.close()


def test_negative_protected_open_edge_is_rejected_before_any_send(monkeypatch):
    eng = engine()
    eng.cfg.reduce_execution = _open_policy(
        dynamic_open_first_leg=DynamicOpenFirstLegConfig(enabled=True),
        open_execution_room_min_bps=1.0)
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)

    monkeypatch.setattr(
        eng, "_protected_open_metrics",
        lambda *_args, **_kwargs: {
            "protected_open_edge_bps": 0.2,
            "required_entry_edge_bps": 1.0,
            "execution_room_bps": 4.0,
            "first_leg_room_bps": 4.0,
            "buy_bound_px": p.buy_limit,
            "sell_bound_px": p.sell_limit,
        })

    assert eng._execution_bounds(c, p) is None
    assert eng.entropy.sent == []
    assert eng.hedge.sent == []
    # A pre-send economic rejection creates a durable signal identity for
    # attribution, but it has not consumed a campaign and must remain
    # eligible for a later fresh economic check.
    assert not eng._open_opportunity_blocked(c, time.time())


def test_open_campaign_is_finite_and_uses_at_most_configured_attempts():
    eng = engine()
    eng.cfg.reduce_execution = _open_policy(
        open_retry_campaign_sec=0.02, open_retry_interval_sec=0.001,
        open_retry_max_attempts=3)
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    eng.entropy.next_results = [
        {"status": "canceled", "filled_base": 0.0, "avg_px": None,
         "err": None, "unresolved": False}
        for _ in range(3)]

    asyncio.run(eng._execute(trade))

    # The wall-clock campaign deadline may end before the final interval;
    # the invariant is bounded retry, not a guarantee to fill every slot.
    assert 1 <= len(eng.entropy.sent) <= 3
    assert eng.hedge.sent == []
    assert eng._last_open_opportunity["final_result"] == "zero_fill"
    assert eng._last_open_opportunity["send_count"] == 3
    assert eng._open_opportunity_blocked(c, time.time())


def test_completed_open_campaign_can_reopen_after_pair_returns_flat():
    eng = engine()
    eng.cfg.reduce_execution = _open_policy(
        open_retry_campaign_sec=0.02, open_retry_interval_sec=0.001,
        open_retry_max_attempts=2)
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None

    asyncio.run(eng._execute(trade))

    assert eng._last_open_opportunity["final_result"] == "filled"
    old_opportunity_id = eng._last_open_opportunity["opportunity_id"]
    assert not eng._positions_flat()
    assert eng._open_opportunity_blocked(c, time.time())

    # A completed cycle releases the directional opportunity even if the
    # market never crossed the old signal reset zone.
    eng.entropy.position = 0.0
    eng.hedge.position = 0.0
    assert eng._positions_flat()
    assert not eng._open_opportunity_blocked(c, time.time())
    reopened = eng._ensure_signal_opportunity(c, time.time())
    assert reopened["opportunity_id"] != old_opportunity_id


def test_flat_pair_does_not_bypass_unresolved_open_order():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    opportunity = eng._ensure_open_opportunity(c, trade, time.time())
    opportunity.update(last_result="unknown", reset_required=False)
    eng._emergency_pending["pending"] = {"venue": "entropy"}

    assert eng._positions_flat()
    assert eng._open_opportunity_blocked(c, time.time())


def test_open_campaign_retries_when_edge_worsens_but_stays_above_floor(
        monkeypatch):
    eng = engine()
    eng.cfg.reduce_execution = _open_policy(
        open_retry_campaign_sec=0.02, open_retry_interval_sec=0.001,
        open_retry_max_attempts=2,
        dynamic_open_first_leg=DynamicOpenFirstLegConfig(enabled=True))
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    first_budget = {
        **trade.cycle_budget,
        "protected_open_edge_bps": 4.0,
        "required_entry_edge_bps": 2.0,
        "execution_room_bps": 2.0,
    }
    initial = replace(trade, cycle_budget=first_budget)
    worse = replace(
        initial, cycle_budget={**first_budget, "protected_open_edge_bps": 3.0})
    refreshes = iter((initial, worse))
    monkeypatch.setattr(eng, "_refresh_execution_plan",
                        lambda _trade: next(refreshes))
    eng.entropy.next_results = [
        {"status": "canceled", "filled_base": 0.0, "avg_px": None,
         "err": None, "unresolved": False}
        for _ in range(2)]

    asyncio.run(eng._execute(initial))

    assert len(eng.entropy.sent) == 2
    assert eng.hedge.sent == []
    assert eng._last_open_opportunity["final_result"] == "zero_fill"


def test_open_campaign_stops_when_current_protected_floor_fails(monkeypatch):
    eng = engine()
    eng.cfg.reduce_execution = _open_policy(
        open_retry_campaign_sec=0.02, open_retry_interval_sec=0.001,
        open_retry_max_attempts=3,
        dynamic_open_first_leg=DynamicOpenFirstLegConfig(enabled=True))
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    first_budget = {
        **trade.cycle_budget,
        "protected_open_edge_bps": 4.0,
        "required_entry_edge_bps": 2.0,
        "execution_room_bps": 2.0,
    }
    initial = replace(trade, cycle_budget=first_budget)
    below_floor = replace(
        initial, cycle_budget={
            **first_budget,
            "protected_open_edge_bps": 1.5,
            "expected_cycle_pnl_usd": 0.0,
            "net_profit_usd": 0.0,
        })
    refreshes = iter((initial, below_floor))
    monkeypatch.setattr(eng, "_refresh_execution_plan",
                        lambda _trade: next(refreshes))
    eng.entropy.next_results = [{
        "status": "canceled", "filled_base": 0.0, "avg_px": None,
        "err": None, "unresolved": False,
    }]

    asyncio.run(eng._execute(initial))

    assert len(eng.entropy.sent) == 1
    assert eng.hedge.sent == []
    assert eng._last_open_opportunity["final_result"] == "expected_cycle_pnl_below_floor"


def test_execution_cost_model_restores_filled_samples_and_ignores_zero_fills(
        tmp_path):
    ledger = TradeLedger(tmp_path / "execution-cost.sqlite3")
    try:
        model = ExecutionCostModel(ledger)
        model.observe(
            phase="open_first", execution_id="zero", requested_qty=1,
            filled_qty=0, quoted_top_px=100, quoted_marginal_px=100,
            avg_fill_px=None, latency_sec=.2, outcome="canceled")
        for index, adverse in enumerate((0.5, 1.0, 2.0)):
            model.observe(
                phase="open_first", execution_id=f"fill-{index}",
                requested_qty=1, filled_qty=1, quoted_top_px=100,
                quoted_marginal_px=100, avg_fill_px=100.01,
                latency_sec=.1 + index * .1, adverse_move_bps=adverse,
                outcome="filled", is_buy=True)

        stats = model.stats("open_first")
        assert stats["observation_count"] == 4
        assert stats["filled_count"] == 3
        assert stats["ready"]
        assert model.expected_cost_bps("open_first", .75, fallback=99) < 5

        restored = ExecutionCostModel(ledger)
        assert restored.restore() == 4
        restored_stats = restored.stats("open_first")
        assert restored_stats["filled_count"] == 3
        assert restored_stats["p75_latency_sec"] is not None
        assert restored.expected_cost_bps("open_first", .75, fallback=99) == pytest.approx(
            model.expected_cost_bps("open_first", .75, fallback=99))
    finally:
        ledger.close()


def test_execution_cost_model_uses_incremental_cost_and_blends_cold_start(
        tmp_path):
    ledger = TradeLedger(tmp_path / "all-in-cost.sqlite3")
    try:
        model = ExecutionCostModel(ledger)
        prior = model.estimate("open_first", prior_bps=4.0)
        assert prior["expected_cost_bps"] == pytest.approx(4.0)
        assert prior["source"] == "cold_start_prior"
        assert prior["empirical_weight"] == 0.0

        for index in range(5):
            observation = model.observe(
                phase="open_first", execution_id=f"all-in-{index}",
                requested_qty=1.0, filled_qty=1.0,
                quoted_top_px=100.0, quoted_depth_avg_px=100.05,
                quoted_marginal_px=100.05,
                avg_fill_px=100.10, latency_sec=.2,
                adverse_move_bps=20.0, outcome="filled", is_buy=True)

        assert observation.is_buy is True
        assert observation.all_in_execution_cost_bps == pytest.approx(10.0)
        assert observation.latency_adverse_move_bps == pytest.approx(20.0)
        persisted = ledger.load_execution_cost_observations(
            phases=["open_first"], limit=1)[0]
        assert persisted["is_buy"] is True
        assert persisted["all_in_execution_cost_bps"] == pytest.approx(10.0)
        assert persisted["latency_adverse_move_bps"] == pytest.approx(20.0)

        estimate = model.estimate("open_first", prior_bps=4.0)
        assert estimate["empirical_cost_bps"] == pytest.approx(
            (100.10 / 100.05 - 1.0) * 1e4)
        assert estimate["expected_cost_bps"] == pytest.approx(
            0.5 * ((100.10 / 100.05 - 1.0) * 1e4) + 0.5 * 4.0)
        assert estimate["source"] == "blended"
        assert estimate["empirical_weight"] == pytest.approx(.5)
        stats = model.stats("open_first")
        assert stats["p75_all_in_cost_bps"] == pytest.approx(10.0)
        assert stats["p75_realized_vs_marginal_bps"] == pytest.approx(5.0,
                                                                       abs=1e-2)
        assert stats["p75_incremental_execution_cost_bps"] == pytest.approx(
            (100.10 / 100.05 - 1.0) * 1e4)

        summary = ledger.execution_cost_summary([f"all-in-{index}" for index in range(5)])
        # Walk-depth cash flow already contains the 5 bps top-to-depth cost;
        # only the additional depth-to-fill cost belongs to economics.
        assert summary["all_in_cost_usd"] == pytest.approx(.5)
        assert summary["total_cost_usd"] == pytest.approx(.25, abs=1e-6)
    finally:
        ledger.close()


def test_cumulative_status_snapshot_cannot_double_inventory_or_hedge_coverage(
        tmp_path):
    ledger = TradeLedger(tmp_path / "cumulative-overfill.sqlite3")
    try:
        ledger.record_order(
            execution_id="exec-1", leg="hedge", venue="lighter",
            order_ref="lighter-1", side="sell", planned_qty=0.0092,
            limit_px=100.0, reduce_only=False, status="partial",
            filled_qty=0.0092, avg_px=100.0, unresolved=False, ts=1.0)
        ledger.record_fill(
            fill_key="confirmation", execution_id="exec-1", leg="hedge",
            venue="lighter", order_ref="lighter-1", ts=1.0, side="sell",
            qty=0.0092, price=100.0, source="order_confirmation",
            metadata={"price_source": "exchange_average"})
        ledger.record_fill(
            fill_key="duplicate-status", execution_id="exec-1", leg="hedge",
            venue="lighter", order_ref="lighter-1", ts=2.0, side="sell",
            qty=0.0092, price=100.0, source="order_status",
            metadata={"cumulative_filled": 0.0184})

        result = ledger.record_cumulative_fill(
            execution_id="exec-1", leg="hedge", venue="lighter",
            order_ref="lighter-1", side="sell", observed_qty=0.0184,
            avg_px=100.0, ts=3.0, source="order_status")

        rows = ledger._conn.execute(
            "SELECT qty,source FROM fills ORDER BY id").fetchall()
        assert result["capped_to_order_plan"]
        assert sum(float(row["qty"]) for row in rows) == pytest.approx(0.0092)
        assert rows[-1]["source"] == "duplicate_cumulative_ignored"
        assert ledger.order_filled_qty(
            venue="lighter", order_ref="lighter-1") == pytest.approx(0.0092)
        assert ledger.execution_authoritative_fill_quantities("exec-1") == {
            "first": 0.0, "hedge": pytest.approx(0.0092)}
    finally:
        ledger.close()


def test_bootstrap_floor_uses_the_less_permissive_loss_cap():
    eng = engine()
    eng.cfg.bootstrap = BootstrapConfig(
        enabled=True, max_expected_loss_bps=1.0,
        max_expected_loss_usd=0.0025)

    assert eng._bootstrap_floor_usd(15.0) == pytest.approx(-0.0015)
    assert eng._bootstrap_floor_usd(100.0) == pytest.approx(-0.0025)


def _patch_open_economics(monkeypatch, eng, *, expected_pnl,
                          economic_floor, execution_room_bps):
    eng.cfg.profit_guard = replace(eng.cfg.profit_guard, enabled=False)
    candidate, plan = candidate_plan(
        eng, "sell_entropy", StrategyAction.OPEN, qty=0.1)
    bootstrap_active = eng._bootstrap_active()
    economics = {
        "net_profit_usd": expected_pnl,
        "books_fresh": True,
        "cross_venue_skew_ms": 0.0,
        "max_cross_venue_skew_ms": 500.0,
        "allowed": expected_pnl >= economic_floor,
        "economic_floor_usd": economic_floor,
        "entry_mode": "bootstrap" if bootstrap_active else "normal",
        "bootstrap_active": bootstrap_active,
        "bootstrap_gross_ok": True,
    }
    protected = {
        "expected_cycle_pnl_usd": expected_pnl,
        "economic_floor_usd": economic_floor,
        "bootstrap_active": bootstrap_active,
        "bootstrap_gross_ok": True,
        "economic_bound_not_marketable": False,
        "first_leg_execution_room_bps": execution_room_bps,
        "execution_room_bps": execution_room_bps,
        "latency_required_room_bps": 0.0,
        "buy_bound_px": plan.buy_limit,
        "sell_bound_px": plan.sell_limit,
    }
    monkeypatch.setattr(
        eng, "_cycle_profit_budget",
        lambda *_args, **_kwargs: dict(economics))
    monkeypatch.setattr(
        eng, "_protected_open_metrics",
        lambda *_args, **_kwargs: dict(protected))
    return candidate, plan


def test_cycle_profit_budget_exposes_effective_bootstrap_floor():
    eng = engine()
    eng.cfg.profit_guard = replace(eng.cfg.profit_guard, enabled=False)
    eng.cfg.bootstrap = BootstrapConfig(enabled=True)
    candidate, plan = candidate_plan(
        eng, "sell_entropy", StrategyAction.OPEN, qty=0.1)

    budget = eng._cycle_profit_budget(candidate, plan)

    expected_floor = eng._bootstrap_floor_usd(
        max(plan.buy_notional, plan.sell_notional))
    assert budget["economic_floor_usd"] == pytest.approx(expected_floor)


def test_bootstrap_open_allows_yellow_cycle_above_bootstrap_floor(monkeypatch):
    eng = engine()
    eng.cfg.bootstrap = BootstrapConfig(
        enabled=True, max_expected_loss_bps=1.0,
        max_expected_loss_usd=0.0025, open_execution_room_min_bps=0.5)
    candidate, plan = _patch_open_economics(
        monkeypatch, eng, expected_pnl=-0.0005,
        economic_floor=-0.0015, execution_room_bps=0.8)

    trade = eng._execution_bounds(candidate, plan)

    assert trade is not None
    assert trade.cycle_budget["economic_floor_usd"] == pytest.approx(-0.0015)
    assert trade.cycle_budget["entry_mode"] == "bootstrap"


def test_bootstrap_open_rejects_cycle_below_bootstrap_floor(monkeypatch):
    eng = engine()
    eng.cfg.bootstrap = BootstrapConfig(
        enabled=True, max_expected_loss_bps=1.0,
        max_expected_loss_usd=0.0025, open_execution_room_min_bps=0.5)
    candidate, plan = _patch_open_economics(
        monkeypatch, eng, expected_pnl=-0.002,
        economic_floor=-0.0015, execution_room_bps=0.8)

    assert eng._execution_bounds(candidate, plan) is None
    assert eng.entropy.sent == []
    assert eng.hedge.sent == []


def test_open_execution_room_is_diagnostic_not_hard_gate(monkeypatch):
    eng = engine()
    eng.cfg.reduce_execution = _open_policy(
        open_execution_room_min_bps=1.0)
    eng.cfg.bootstrap = BootstrapConfig(
        enabled=True, open_execution_room_min_bps=0.5)
    candidate, plan = _patch_open_economics(
        monkeypatch, eng, expected_pnl=0.01,
        economic_floor=0.003, execution_room_bps=0.8)

    assert eng._execution_bounds(candidate, plan) is not None

    eng.cfg.bootstrap = BootstrapConfig(enabled=False)
    candidate, plan = _patch_open_economics(
        monkeypatch, eng, expected_pnl=0.01,
        economic_floor=0.003, execution_room_bps=0.8)

    assert eng._execution_bounds(candidate, plan) is not None


def test_bootstrap_classifies_a_near_zero_negative_open_as_yellow():
    eng = engine()
    eng.cfg.bootstrap = BootstrapConfig(enabled=True)
    candidate, plan = candidate_plan(
        eng, "sell_entropy", StrategyAction.OPEN, qty=0.1)
    trade = PlannedTrade(
        candidate=candidate, plan=plan, net_marginal_edge_bps=0.0,
        edge_surplus_bps=0.0, per_leg_slippage_bps=0.0,
        buy_bound=plan.buy_limit, sell_bound=plan.sell_limit,
        cycle_budget={"expected_cycle_pnl_usd": -0.0005})

    eng._bootstrap_note_decision(trade)

    assert trade.cycle_budget["entry_mode"] == "bootstrap"
    assert trade.cycle_budget["bootstrap_color"] == "YELLOW"
    snapshot = eng._bootstrap_snapshot()
    assert snapshot["accepted_count"] == 1
    assert snapshot["yellow_count"] == 1
    assert snapshot["active"]


def test_bootstrap_keeps_running_after_phase_samples_until_bounded_stop():
    eng = engine()
    eng.cfg.bootstrap = BootstrapConfig(enabled=True)
    eng._bootstrap_state["phase_samples"] = {
        "open_first": 3, "open_hedge": 3,
        "close_first": 3, "close_hedge": 3,
    }

    assert not eng._maybe_auto_disable_bootstrap()
    assert not eng._bootstrap_state.get("disabled")

    eng._bootstrap_state["accepted_cycle_count"] = 10
    assert eng._maybe_auto_disable_bootstrap()
    assert eng._bootstrap_state["disabled"]
    assert eng._bootstrap_state["disabled_reason"] == "max_complete_cycles"
    assert not eng._bootstrap_active()


@pytest.mark.parametrize(
    ("age_sec", "spread", "expected_reason"),
    ((11.0, 0.015, "normal_hold"),
     (31.0, 0.040, "fast_exit"),
     (121.0, 0.050, "fast_exit")),
)
def test_bootstrap_close_uses_the_normal_exit_ladder(
        age_sec, spread, expected_reason):
    eng = engine(midline=20)
    eng.cfg.bootstrap = BootstrapConfig(enabled=True)
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        normal_min_profit_usd=0.0, fast_exit_after_sec=30.0,
        fast_exit_min_profit_usd=-0.01)
    open_lot(eng, "entropy", "sell", 0.1, 100.0, suffix="ladder-e")
    open_lot(eng, "hedge", "buy", 0.1, 100.0, suffix="ladder-h")
    eng.cycle.start(
        direction="short_entropy", opened_ts=time.time() - age_sec,
        entry_midline_bps=20.0, entry_sigma_bps=1.0, entry_band_bps=4.0,
        entry_threshold_bps=4.0, paired_qty=0.1)
    eng._cycle_telemetry = {
        "cycle_id": eng.cycle.cycle_id, "entry_mode": "bootstrap"}
    eng.entropy.set_book(99.9, 100.0 + spread)
    eng.hedge.set_book(100.0, 100.1)
    candidate, plan = candidate_plan(
        eng, "buy_entropy", StrategyAction.REDUCE, qty=0.1)

    budget = eng._cycle_profit_budget(candidate, plan)

    assert budget["bootstrap_cycle"]
    assert budget["exit_reason"] == expected_reason
    assert budget["allowed"] is (expected_reason != "normal_hold")


def test_bootstrap_close_campaign_uses_the_normal_retry_budget(tmp_path):
    eng = _short_cycle_engine(tmp_path, campaign_sec=0.02)
    eng.cfg.bootstrap = BootstrapConfig(
        enabled=True, close_campaign_sec=0.02,
        close_retry_interval_sec=0.001, close_campaign_max_attempts=2)
    eng._cycle_telemetry["entry_mode"] = "bootstrap"
    candidate, plan = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    trade = PlannedTrade(
        candidate=candidate, plan=plan, net_marginal_edge_bps=0.0,
        edge_surplus_bps=0.0, per_leg_slippage_bps=0.0,
        buy_bound=plan.buy_limit, sell_bound=plan.sell_limit,
        cycle_budget={"bootstrap_cycle": True, "entry_mode": "bootstrap",
                      "exit_reason": "sampling_exit",
                      "exit_class": "economic"})
    state = ExecutionState(
        "bootstrap-close", "reduce", "buy_entropy", plan.qty,
        True, False, True)

    campaign = eng._start_exit_campaign(state, trade)

    assert campaign is not None
    assert campaign["duration_sec"] == pytest.approx(0.02)
    assert campaign["retry_interval_sec"] == pytest.approx(0.001)
    assert campaign["max_attempts"] == 4


def test_critical_reconciliation_mismatch_suspends_bootstrap():
    eng = engine()
    eng.cfg.bootstrap = BootstrapConfig(enabled=True)

    eng._mark_orphaned("reconciliation_mismatch")

    assert not eng._bootstrap_state["disabled"]
    assert eng._bootstrap_state["suspended"]
    assert eng._bootstrap_state["suspended_reason"] == \
        "critical_recovery_mismatch"
    assert eng._bootstrap_snapshot()["mode"] == "suspended"


def test_suspended_bootstrap_resumes_after_clean_flat_reconciliation():
    eng = engine()
    eng.cfg.bootstrap = BootstrapConfig(enabled=True)
    eng._bootstrap_state.update({
        "suspended": True,
        "suspended_reason": "critical_recovery_mismatch",
        "suspended_ts": time.time() - 31.0,
        "resume_after_ts": time.time() - 1.0,
    })
    eng.startup_reconciled = True
    eng.reconciliation["status"] = "synced"

    assert eng._maybe_resume_bootstrap()
    assert eng._bootstrap_active()
    assert not eng._bootstrap_state["suspended"]
    assert eng._bootstrap_state["suspended_reason"] is None
    assert eng._bootstrap_snapshot()["mode"] == "bootstrap"


def test_permanent_bootstrap_disable_does_not_resume():
    eng = engine()
    eng.cfg.bootstrap = BootstrapConfig(enabled=True)
    eng._bootstrap_state.update({
        "disabled": True,
        "disabled_reason": "max_complete_cycles",
        "suspended": False,
    })
    eng.startup_reconciled = True
    eng.reconciliation["status"] = "synced"

    assert not eng._maybe_resume_bootstrap()
    assert eng._bootstrap_state["disabled"]
    assert eng._bootstrap_state["disabled_reason"] == "max_complete_cycles"


def test_bootstrap_execution_cost_weight_reaches_half_at_three_of_six_samples():
    model = ExecutionCostModel()
    for index in range(3):
        model.observe(
            phase="open_first", execution_id=f"bootstrap-{index}",
            requested_qty=1.0, filled_qty=1.0,
            quoted_top_px=100.0, quoted_depth_avg_px=100.0,
            quoted_marginal_px=100.0, avg_fill_px=100.01,
            latency_sec=0.2, is_buy=True, outcome="filled")

    estimate = model.estimate("open_first", full_weight_samples=6)
    stats = model.stats("open_first", full_weight_samples=6)

    assert estimate["empirical_weight"] == pytest.approx(0.5)
    assert estimate["empirical_full_weight_samples"] == 6
    assert stats["empirical_ready"] is False


def test_open_budget_uses_reverse_directions_for_projected_close_costs():
    eng = engine()
    candidate, plan = candidate_plan(eng, "buy_entropy", StrategyAction.OPEN)
    seen = {}

    def estimate(phase, **kwargs):
        seen[phase] = kwargs["is_buy"]
        return {"expected_cost_bps": 0.0}

    original = eng._execution_cost_estimate
    eng._execution_cost_estimate = estimate
    try:
        eng._cycle_profit_budget(candidate, plan)
    finally:
        eng._execution_cost_estimate = original

    assert seen == {
        "open_first": True,
        "open_interleg": False,
        "open_hedge": False,
        "close_first": False,
        "close_interleg": True,
        "close_hedge": True,
    }


def test_open_budget_charges_both_cross_leg_waits():
    eng = engine()
    candidate, plan = candidate_plan(eng, "buy_entropy", StrategyAction.OPEN)
    original = eng._execution_cost_estimate
    eng._execution_cost_estimate = lambda phase, **kwargs: {
        "expected_cost_bps": 1.0,
    }
    try:
        budget = eng._cycle_profit_budget(candidate, plan)
    finally:
        eng._execution_cost_estimate = original

    assert set(budget["expected_execution_costs_bps"]) == {
        "open_first", "open_interleg", "open_hedge",
        "close_first", "close_interleg", "close_hedge",
    }
    notional = max(plan.buy_notional, plan.sell_notional)
    assert budget["expected_execution_cost_usd"] == pytest.approx(
        notional * 6.0 / 1e4)


def test_open_budget_reserves_capacity_for_reduce_and_recovery():
    eng = engine()
    eng.entropy.orders_per_min = 6
    eng.cfg.reduce_execution = _open_policy(reserve_reduce_slots=2)
    for _ in range(4):
        eng._record_send(eng.entropy)

    assert eng._venue_order_budget_limit(eng.entropy, StrategyAction.OPEN) == 4
    assert not eng._venue_rate_ok(eng.entropy, StrategyAction.OPEN)
    assert eng._venue_rate_ok(eng.entropy, StrategyAction.REDUCE)


def test_same_sustained_open_signal_is_deduplicated_until_reset():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    eng._ensure_open_opportunity(c, trade, time.time())

    assert eng._open_opportunity_blocked(c, time.time())
    eng._mark_open_signal_reset(eng._open_opportunity_key(c))
    assert not eng._open_opportunity_blocked(c, time.time())


def test_open_dedupe_waits_for_reset_zone_instead_of_basis_bucket():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    first_key = eng._open_opportunity_key(c)
    eng._ensure_open_opportunity(c, trade, time.time())
    assert eng._open_opportunity_blocked(c, time.time())

    # A materially different quote is still the same sustained directional
    # signal; crossing a former 2-bps bucket must not unlock it.
    eng.entropy.set_book(100.0, 100.01)
    assert eng._open_opportunity_key(c) == first_key
    assert eng._open_opportunity_blocked(c, time.time())

    # Only a move back inside the entry threshold plus the reset margin
    # unlocks the direction for its next threshold crossing.
    eng.hedge.set_book(99.99, 100.01)
    reset_basis = c.threshold_bps - eng.OPEN_SIGNAL_RESET_BPS - 0.1
    reset_mid = eng.hedge.book.mid() * (1.0 + reset_basis / 1e4)
    eng.entropy.set_book(reset_mid - 0.01, reset_mid + 0.01)
    assert not eng._open_opportunity_blocked(c, time.time())
