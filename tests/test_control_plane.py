from entropy_arb.control_plane import (
    beta_fill_probability,
    choose_execution_room,
    dynamic_exit_floor_bps,
    is_clean_circle,
    positive_close_budget_bps,
)
import pytest


def test_beta_fill_probability_uses_small_prior_and_observations_dominate():
    assert beta_fill_probability(0, 0) == 0.5
    assert beta_fill_probability(100, 90) > 0.8
    assert beta_fill_probability(100, 0) < 0.05


def test_room_optimizer_respects_economic_and_physical_caps():
    result = choose_execution_room(
        phase="open", affordable_room_bps=5.0, hard_cap_bps=3.0,
        fill_stats={"0-2": {"attempts": 50, "fills": 5},
                    "2-4": {"attempts": 50, "fills": 40}},
        base_cycle_pnl_bps=5.0)
    assert result["room_bps"] <= 3.0
    assert result["bucket"] == "2-4"


def test_room_optimizer_has_deterministic_fallbacks():
    assert choose_execution_room(
        phase="open", affordable_room_bps=5.0, hard_cap_bps=5.0)[
            "room_bps"] == 3.0
    assert choose_execution_room(
        phase="close", affordable_room_bps=5.0, hard_cap_bps=5.0)[
            "room_bps"] == 2.0


def test_room_optimizer_charges_incremental_cost_not_ioc_room():
    result = choose_execution_room(
        phase="open", affordable_room_bps=3.0, hard_cap_bps=3.0,
        fill_stats={"0-2": {"attempts": 100, "fills": 0},
                    "2-4": {"attempts": 100, "fills": 100}},
        base_cycle_pnl_bps=6.0, expected_incremental_cost_bps=1.0)

    assert result["room_bps"] == 3.0
    assert result["expected_incremental_cost_bps"] == 1.0
    assert result["score"] == pytest.approx(
        result["fill_probability"] * (6.0 - 1.0))


def test_positive_close_budget_and_dynamic_floor():
    assert positive_close_budget_bps(0.02, 0.01, 100.0) == 1.0
    assert positive_close_budget_bps(0.01, 0.02, 100.0) == 0.0
    assert dynamic_exit_floor_bps(10) == 0.0
    assert dynamic_exit_floor_bps(60) == -0.5
    assert dynamic_exit_floor_bps(180) == -1.0
    assert dynamic_exit_floor_bps(360) == -2.0


def test_clean_circle_excludes_recovery_and_manual_flatten():
    base = {"status": "completed", "open_first_fill_ts": 1.0,
            "hedge_completed_ts": 1.2, "reduce_first_fill_ts": 1.8,
            "flat_ts": 2.0, "recovery_count": 0, "emergency_flag": 0,
            "exit_mode": "normal", "exit_reason": "profit"}
    assert is_clean_circle(base)
    assert not is_clean_circle({**base, "recovery_count": 1})
    assert not is_clean_circle({**base, "exit_mode": "manual_flatten"})
    assert not is_clean_circle({**base, "reduce_first_fill_ts": None})
    assert not is_clean_circle({**base, "status": "active"})


def test_room_optimizer_spends_wider_room_within_same_bucket():
    result = choose_execution_room(
        phase="close", affordable_room_bps=1.9, hard_cap_bps=7.0,
        fill_stats={"0-2": {"attempts": 20, "fills": 2}},
        base_cycle_pnl_bps=2.0, expected_incremental_cost_bps=0.2)
    assert result["room_bps"] == 1.0
