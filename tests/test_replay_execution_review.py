"""Offline execution stress checks; no adapters or credentials are loaded."""
from dataclasses import replace

import pytest

from entropy_arb.config import load_config
from entropy_arb.cycle_policy import evaluate_cycle_exit, exit_net_pnl
from tools.replay import ExecutionScenario, Row, simulate


@pytest.fixture
def cfg(tmp_path):
    path = tmp_path / "replay.yaml"
    path.write_text("""
thresholds:
  midline_bps: 0
  upper_bps: 5
  lower_bps: 5
  exit_bps: 1
dynamic_midline:
  enabled: false
funding:
  enabled: false
execution:
  min_edge_surplus_bps: 0.5
""")
    return load_config(str(path), "/dev/null", symbol="TEST", hedge_venue="lighter")


def row(ts, premium=0, h=100):
    e = h * (1 + premium / 1e4)
    return Row(ts, e, e, 10, 10, h, h, 10, 10, premium,
               (e / h - 1) * 1e4, (h / e - 1) * 1e4, None, None)


def run(cfg, rows, **stress):
    return simulate(rows, cfg, notional=100,
                    execution_scenario=ExecutionScenario(**stress))


def test_first_and_hedge_require_separate_observations(cfg):
    result = run(cfg, [row(0, 6), row(1, 6), row(2), row(3), row(4)],
                 first_delay_sec=0, hedge_delay_sec=0)
    assert result["round_trips"] == 1
    assert [e["ts"] for e in result["execution_events"]] == [1, 2, 3, 4]
    assert result["unhedged_seconds"] == 2
    assert result["completed_net_pnl_usd"] > 0
    assert result["open_mtm_pnl_usd"] == pytest.approx(0)
    assert result["total_liquidation_pnl_usd"] == pytest.approx(result["completed_net_pnl_usd"])


def test_replay_respects_open_direction_policy(cfg):
    short_only = replace(
        cfg, allow_sell_entropy_open=True, allow_buy_entropy_open=False)
    long_only = replace(
        cfg, allow_sell_entropy_open=False, allow_buy_entropy_open=True)

    rich = run(long_only, [row(i, 6) for i in range(5)])
    cheap = run(short_only, [row(i, -6) for i in range(5)])

    assert rich["opens"] == 0
    assert cheap["opens"] == 0


def test_tail_is_not_completed_profit_and_prices_hedge_later(cfg):
    result = run(cfg, [row(0, 6), row(1, 6), row(2, 0, h=101)], max_adverse_bps=200)
    assert result["opens"] == 1 and result["round_trips"] == 0
    assert result["completed_net_pnl_usd"] == 0
    assert result["net_pnl_usd"] == 0
    events = result["execution_events"]
    assert events[1]["price"] == 101
    assert result["open_liquidation_pnl_usd"] < 0


def test_partial_hedge_blocks_next_first_leg_and_survives_tail(cfg):
    result = run(cfg, [row(i, 6 if i < 2 else 0) for i in range(6)], hedge_fill_fraction=0.5)
    assert result["partial_fills"] > 0
    assert result["round_trips"] == 0
    assert result["pending_order"]["phase"] == "hedge"
    assert len([e for e in result["execution_events"] if e["phase"] == "first"]) == 1
    assert result["entropy_position"] + result["hedge_position"] < 0


def test_zero_ioc_recovery_and_budget_wait_are_visible(cfg):
    result = run(cfg, [row(i, 6 if i < 2 else 0) for i in range(70)],
                 zero_fill_every=2, orders_per_minute=2)
    assert result["zero_fills"] >= 1
    assert result["budget_wait_rows"] > 0
    # Recovery starts at the zero hedge at t=2 and ends at its fill at t=61.
    assert result["recovery_seconds"] == 59
    assert result["unhedged_seconds"] >= 60


def test_zero_first_leg_never_counts_as_open(cfg):
    result = run(cfg, [row(i, 6) for i in range(10)], first_fill_fraction=0)
    assert result["opens"] == result["round_trips"] == 0
    assert result["turnover_usd"] == 0
    assert result["zero_fills"] > 0


def test_gap_beyond_submitted_limit_is_zero_ioc(cfg):
    result = run(cfg, [row(0, 6), row(1, 6, h=90)], max_adverse_bps=1)
    assert result["zero_fills"] == 1
    assert result["opens"] == 0


def test_hard_timeout_releases_loss_independent_of_legacy_floor(cfg):
    policy = replace(cfg.cycle_exit, hard_timeout_enabled=True, hard_timeout_sec=5,
                     hard_timeout_min_profit_usd=-0.02)
    result = evaluate_cycle_exit(policy, net_pnl_usd=-5, notional_usd=100, held_sec=5)
    assert result["allowed"] and result["risk_exit"]
    assert result["risk_exit_type"] == "hard_timeout"


def test_shared_policy_rejects_invalid_estimates(cfg):
    result = evaluate_cycle_exit(cfg.cycle_exit, net_pnl_usd=float("nan"),
                                 notional_usd=100, held_sec=10000)
    assert not result["allowed"]


def test_liquidation_mark_charges_exit_fees_and_slippage_once():
    # Historical fees/slippage are already in the -1 cashflow.
    result = exit_net_pnl(cashflow_usd=-1, entropy_qty=1, hedge_qty=-1,
        entropy_bid=100, entropy_ask=102, hedge_bid=98, hedge_ask=100,
        entropy_fee_bps=10, hedge_fee_bps=10, slippage_bps=10)
    assert result == pytest.approx(-1.4)


def test_subsecond_delays_round_up_to_available_row(cfg):
    result = run(cfg, [row(0, 6), row(2, 6), row(10), row(20), row(21)],
                 first_delay_sec=0.2, hedge_delay_sec=0.2)
    assert [e["ts"] for e in result["execution_events"]] == [2, 10, 20, 21]


def test_fill_pnl_timeout_closes_real_loss(cfg):
    policy = replace(cfg.cycle_exit, enabled=True, mode="fill_pnl",
                     hard_timeout_enabled=True, hard_timeout_sec=3,
                     fast_exit_after_sec=100, fast_exit_min_profit_usd=0)
    cfg = replace(cfg, cycle_exit=policy)
    rows = [row(0, 6), row(1, 6)] + [row(i, 100) for i in range(2, 7)]
    result = run(cfg, rows, max_adverse_bps=200)
    assert result["round_trips"] == 1
    assert result["completed_net_pnl_usd"] < -0.02
    assert result["completed_cycles"][0]["closed_ts"] == 6


def test_regime_stop_requires_regime_even_without_warning_requirement(cfg):
    policy = replace(cfg.cycle_exit, regime_stop_enabled=True,
                     regime_stop_require_midline_warning=False,
                     hard_timeout_enabled=False)
    args = dict(net_pnl_usd=-1, notional_usd=100, held_sec=100)
    assert not evaluate_cycle_exit(policy, **args)["risk_exit"]
    assert evaluate_cycle_exit(policy, **args, regime_shift=True)["risk_exit_type"] == "regime_stop"


def test_ioc_rejects_invalid_observation(cfg):
    result = run(cfg, [row(0, 6), replace(row(1, 6), premium_sample_valid=False)])
    assert result["zero_fills"] == 1
    assert result["opens"] == 0
