"""Focused offline checks for the third-stage lifecycle and guardrails."""
import time
import asyncio
from dataclasses import replace

import pytest

from entropy_arb.config import ReentryGuardConfig
from entropy_arb.signal import MidlineLagState, StrategyAction
from entropy_arb.trade_ledger import TradeLedger
from test_engine import make_engine


def _fixed_snapshot(eng, state: MidlineLagState, *, regime_shift=False,
                    short_center=0.0, long_center=0.0):
    eng.cfg.dynamic_midline_enabled = True
    base = eng.premium_model.snapshot(time.time())
    snapshot = replace(
        base,
        short_center_bps=short_center,
        long_center_bps=long_center,
        active_midline_bps=4.0,
        robust_sigma_bps=1.0,
        midline_lag_bps=2.5 if state != MidlineLagState.NORMAL else 0.0,
        midline_lag_state=state,
        regime_shift=regime_shift,
        ready=True,
    )

    def get_snapshot(now):
        current = replace(snapshot, ts=now)
        eng.model_snapshot = current
        return current

    eng._premium_snapshot = get_snapshot


def test_lag_state_degrades_reference_without_global_open_block():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)

    _fixed_snapshot(eng, MidlineLagState.NORMAL)
    normal = next(c for c in eng._build_candidates(time.time())
                  if c.direction == "sell_entropy")

    _fixed_snapshot(eng, MidlineLagState.WARNING)
    warning = next(c for c in eng._build_candidates(time.time())
                   if c.direction == "sell_entropy")
    assert warning.threshold_bps == pytest.approx(normal.threshold_bps)
    values = eng._dashboard_strategy_values(time.time())
    assert values["reference_confidence"] == pytest.approx(.5)
    assert values["warning_entry_penalty_bps"] == pytest.approx(0.0)

    _fixed_snapshot(eng, MidlineLagState.ACCELERATING)
    accelerating = eng._build_candidates(time.time())
    assert any(c.direction == "sell_entropy" for c in accelerating)
    values = eng._dashboard_strategy_values(time.time())
    assert values["reference_confidence"] == pytest.approx(.25)
    assert values["entry_reference_mode"] == "blended_midline"
    assert values["entry_reference_midline_bps"] == pytest.approx(2.0)

    _fixed_snapshot(eng, MidlineLagState.BLOCKED)
    blocked = eng._build_candidates(time.time())
    assert any(c.direction == "sell_entropy" for c in blocked)
    values = eng._dashboard_strategy_values(time.time())
    assert values["reference_confidence"] == pytest.approx(.25)
    assert values["entry_reference_mode"] == "dynamic_center"
    assert values["open_lag_blocked"] is False
    assert values["blocked_entry_allowed"] is True

    # A blocked state is a reference-quality diagnostic, not a global
    # reduce-only switch. Economic admission remains responsible for deciding
    # whether this small deviation is executable.
    eng.entropy.set_book(100.0, 100.02)
    eng.hedge.set_book(100.0, 100.02)
    candidates = eng._build_candidates(time.time())
    assert candidates
    assert all(candidate.action == StrategyAction.OPEN
               for candidate in candidates)


def test_regime_shift_uses_dynamic_reference_and_allows_extreme_open():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)

    _fixed_snapshot(eng, MidlineLagState.NORMAL, regime_shift=True)
    values = eng._dashboard_strategy_values(time.time())

    assert values["reference_state"] == "shifting"
    assert values["entry_reference_mode"] == "dynamic_center"
    assert values["entry_reference_midline_bps"] == pytest.approx(0.0)
    assert values["required_deviation_bps"] == pytest.approx(2.0)
    assert values["extreme_dislocation"]
    assert not values["open_lag_blocked"]
    assert any(candidate.action == StrategyAction.OPEN
               for candidate in eng._build_candidates(time.time()))


def test_regime_shift_is_soft_for_small_deviation_without_reduce_only():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng.entropy.set_book(100.00, 100.02)
    eng.hedge.set_book(100.00, 100.02)
    _fixed_snapshot(eng, MidlineLagState.NORMAL, regime_shift=True)

    values = eng._dashboard_strategy_values(time.time())
    assert values["reference_state"] == "shifting"
    assert not values["open_lag_blocked"]
    candidates = eng._build_candidates(time.time())
    assert candidates
    assert all(candidate.action == StrategyAction.OPEN
               for candidate in candidates)

    # The same regime flag must not suppress a valid reduce candidate.
    eng.entropy.position, eng.hedge.position = -1.0, 1.0
    eng.entropy.set_book(100.00, 100.02)
    eng.hedge.set_book(100.00, 100.02)
    candidates = eng._build_candidates(time.time())
    assert StrategyAction.REDUCE in [candidate.action
                                     for candidate in candidates]


def test_blocked_reference_requires_both_dynamic_centers():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    _fixed_snapshot(eng, MidlineLagState.BLOCKED, short_center=None,
                    long_center=0.0)

    values = eng._dashboard_strategy_values(time.time())
    assert values["reference_valid"] is False
    assert values["open_lag_blocked"]
    assert eng._build_candidates(time.time()) == []


def test_open_block_telemetry_tracks_reason_counts_and_duration():
    eng = make_engine()
    eng._record_open_blocker("reference_deviation", 100.0)
    eng._record_open_blocker("reference_deviation", 101.0)
    eng._record_open_blocker("no_executable_edge", 103.0)
    snapshot = eng._open_block_telemetry_snapshot(104.0)

    assert snapshot["count_by_reason"] == {
        "reference_deviation": 2, "no_executable_edge": 1}
    assert snapshot["seconds_by_reason"]["reference_deviation"] == \
        pytest.approx(3.0)
    assert snapshot["active_reason"] == "no_executable_edge"
    assert snapshot["active_duration_sec"] == pytest.approx(1.0)


def test_reentry_guard_requires_cooldown_and_basis_reset_but_allows_opposite():
    eng = make_engine(midline=0.0, upper=2.0, lower=2.0)
    eng.cfg.reentry_guard = ReentryGuardConfig(
        enabled=True, post_risk_exit_cooldown_sec=30.0,
        basis_reset_bps=1.0, same_direction_only=True)
    eng.cycle.last_risk_exit_direction = "long_entropy"
    eng.cycle.last_risk_exit_ts = time.time() - 60.0
    eng.cycle.last_risk_exit_basis_bps = -5.0
    eng.cycle.reentry_reset_seen = False

    statuses = {
        direction: eng._reentry_status(direction, time.time())
        for direction in ("buy_entropy", "sell_entropy")
    }
    assert statuses["buy_entropy"]["blocked"]
    assert statuses["buy_entropy"]["reason"] == "basis_reset_pending"
    assert not statuses["sell_entropy"]["blocked"]
    assert statuses["sell_entropy"]["reason"] == "opposite_direction"

    # The guard remains visible as uncertainty telemetry, but it is not a
    # global OPEN veto; whole-cycle economics decides whether to proceed.
    eng.entropy.set_book(99.94, 99.95)
    eng.hedge.set_book(99.99, 100.0)
    blocked_candidates = eng._build_candidates(time.time())
    assert any(c.direction == "buy_entropy"
               and c.action == StrategyAction.OPEN
               for c in blocked_candidates)

    # Observe a neutral basis, then require a fresh same-direction signal.
    eng.entropy.set_book(99.0, 101.0)
    eng.hedge.set_book(99.0, 101.0)
    eng._build_candidates(time.time())
    assert eng.cycle.reentry_reset_seen

    eng.entropy.set_book(99.94, 99.95)
    eng.hedge.set_book(99.99, 100.0)
    candidates = eng._build_candidates(time.time())
    assert any(c.direction == "buy_entropy"
               and c.action == StrategyAction.OPEN for c in candidates)


def test_loss_recovery_and_long_hold_do_not_auto_flatten():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        fast_exit_after_sec=30.0, fast_exit_min_profit_usd=-.01,
        max_hold_sec=600.0, long_hold_after_sec=600.0,
        hard_timeout_enabled=True, hard_timeout_sec=3600.0,
        hard_timeout_min_profit_usd=-.02,
        pnl_loss_stop_enabled=False, max_cycle_loss_bps=None,
        adverse_drift_exit_enabled=False, regime_stop_enabled=False)
    for venue, side in (("entropy", "buy"), ("hedge", "sell")):
        eng.ledger.record_fill(
            fill_key=f"{venue}:phase3-open", execution_id="phase3-open",
            leg=venue, venue=venue, order_ref=f"{venue}:phase3-open",
            ts=time.time() - 700.0, side=side, qty=1.0, price=100.0,
            fee_usd=0.0, fee_source="exchange_api")
        eng.venues[venue].position = 1.0 if side == "buy" else -1.0
    eng.entropy.set_book(99.94, 99.95)
    eng.hedge.set_book(99.99, 100.0)
    eng.cycle.start(
        direction="long_entropy", opened_ts=time.time() - 121.0,
        entry_midline_bps=0.0, entry_sigma_bps=1.0, entry_band_bps=4.0,
        entry_threshold_bps=4.0, paired_qty=1.0)
    candidate = next(c for c in eng._build_candidates(time.time())
                     if c.action == StrategyAction.REDUCE)
    plan, _ = eng._plan(candidate.buy, candidate.sell, 100.0,
                        -9999.0)
    assert plan is not None
    budget = eng._cycle_profit_budget(candidate, plan)
    assert budget["reason"] == "loss_recovery_hold"
    assert budget["hold_state"] == "loss_recovery_hold"
    assert not budget["allowed"]
    assert not budget["risk_exit"]
    assert not budget["forced"]

    # A deeper loss remains a recovery hold; the old PnL-stop path must not
    # turn it into an automatic risk exit.
    eng.cycle.opened_ts = time.time() - 300.0
    budget = eng._cycle_profit_budget(candidate, plan)
    assert budget["reason"] == "loss_recovery_hold"
    assert not budget["allowed"]
    assert not budget["risk_exit"]

    # 600 seconds is an observation state, not a forced flatten.
    eng.cycle.opened_ts = time.time() - 601.0
    budget = eng._cycle_profit_budget(candidate, plan)
    assert budget["reason"] == "long_hold"
    assert budget["hold_state"] == "long_hold"
    assert not budget["allowed"]
    assert not budget["risk_exit"]
    assert not budget["forced"]


def test_hard_timeout_releases_both_legs_even_below_economic_floor():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        fast_exit_after_sec=30.0, fast_exit_min_profit_usd=-.01,
        long_hold_after_sec=30.0, max_hold_sec=30.0,
        hard_timeout_enabled=True, hard_timeout_sec=60.0,
        hard_timeout_min_profit_usd=-.02)
    for venue, side in (("entropy", "buy"), ("hedge", "sell")):
        eng.ledger.record_fill(
            fill_key=f"{venue}:hard-timeout-open",
            execution_id="hard-timeout-open", leg=venue, venue=venue,
            order_ref=f"{venue}:hard-timeout-open", ts=time.time() - 90.0,
            side=side, qty=1.0, price=100.0, fee_usd=0.0,
            fee_source="exchange_api")
        eng.venues[venue].position = 1.0 if side == "buy" else -1.0
    eng.cycle.start(
        direction="long_entropy", opened_ts=time.time() - 61.0,
        entry_midline_bps=0.0, entry_sigma_bps=1.0, entry_band_bps=4.0,
        entry_threshold_bps=4.0, paired_qty=1.0)

    # The protected executable PnL is below the hard-timeout release floor.
    eng.entropy.set_book(99.94, 99.95)
    eng.hedge.set_book(99.99, 100.0)
    candidate = next(c for c in eng._build_candidates(time.time())
                     if c.direction == "sell_entropy"
                     and c.action == StrategyAction.REDUCE)
    plan, _ = eng._plan(candidate.buy, candidate.sell, 100.0, -9999.0)
    assert plan is not None
    budget = eng._cycle_profit_budget(candidate, plan)
    assert budget["net_profit_usd"] < -.02
    assert budget["reason"] == "hard_timeout_release"
    assert budget["risk_exit"] and budget["forced"]
    assert budget["risk_exit_type"] == "hard_timeout"
    assert budget["allowed"]
    eng._scan(time.time())
    trade = eng._scan(time.time())
    assert trade is not None
    asyncio.run(eng._execute(trade))
    assert eng._positions_flat()
    assert all(o["reduce_only"] for v in eng.venues.values() for o in v.sent)


def test_reconciliation_restores_persisted_hold_state():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng.cfg.cycle_exit = replace(eng.cfg.cycle_exit, enabled=True)
    eng.cycle.start(
        direction="long_entropy", opened_ts=time.time() - 300.0,
        entry_midline_bps=0.0, entry_sigma_bps=1.0, entry_band_bps=4.0,
        entry_threshold_bps=4.0, paired_qty=1.0)
    eng._cycle_telemetry = {
        "cycle_id": eng.cycle.cycle_id,
        "hold_state": "loss_recovery_hold",
        "hold_state_ts": time.time() - 100.0,
    }
    eng.entropy.position = 1.0
    eng.hedge.position = -1.0
    eng._cycle_hold_state = None
    eng._sync_cycle_with_positions()
    assert eng._cycle_hold_state == "loss_recovery_hold"
    assert eng._cycle_hold_state_ts == pytest.approx(
        eng._cycle_telemetry["hold_state_ts"])


def test_margin_warning_is_soft_but_critical_keeps_open_blocked():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng._update_margin_snapshot(eng.entropy, 100.0, 20.0)
    eng._update_margin_snapshot(eng.hedge, 100.0, 80.0)
    margin = eng._margin_snapshot()
    assert margin["state"] == "warning"
    assert margin["known"]
    assert not margin["open_blocked"]
    assert not margin["hard_risk"]

    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    candidates = eng._build_candidates(time.time())
    assert any(candidate.action == StrategyAction.OPEN
               for candidate in candidates)

    eng.entropy.position = 1.0
    eng.hedge.position = -1.0
    eng.entropy.set_book(99.94, 99.95)
    eng.hedge.set_book(99.99, 100.0)
    candidates = eng._build_candidates(time.time())
    assert any(candidate.action == StrategyAction.REDUCE
               for candidate in candidates)

    eng._update_margin_snapshot(eng.entropy, 100.0, 5.0)
    margin = eng._margin_snapshot()
    assert margin["state"] == "critical"
    assert margin["hard_risk"]
    eng.entropy.position = eng.hedge.position = 0.0
    assert not any(candidate.action == StrategyAction.OPEN
                   for candidate in eng._build_candidates(time.time()))


def test_hold_state_blocks_add_without_blocking_reduce_candidate():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng.cfg.cycle_exit = replace(eng.cfg.cycle_exit, enabled=True)
    eng.cfg.cycle_allow_add = True
    eng.cycle.start(
        direction="long_entropy", opened_ts=time.time() - 700.0,
        entry_midline_bps=0.0, entry_sigma_bps=1.0, entry_band_bps=4.0,
        entry_threshold_bps=4.0, paired_qty=1.0)
    eng.entropy.position = 1.0
    eng.hedge.position = -1.0
    eng.entropy.set_book(99.94, 99.95)
    eng.hedge.set_book(99.99, 100.0)
    eng._cycle_hold_state = "long_hold"
    candidates = eng._build_candidates(time.time())
    assert [candidate.action for candidate in candidates] == [StrategyAction.REDUCE]


def test_emergency_retry_attempts_are_campaign_scoped():
    eng = make_engine()
    first = eng._ensure_emergency_campaign("max_hold_exit", "cycle-a")
    first["attempts"]["entropy"] = 2
    same = eng._ensure_emergency_campaign("max_hold_exit", "cycle-a")
    assert same["id"] == first["id"]
    assert same["attempts"]["entropy"] == 2

    new_cycle = eng._ensure_emergency_campaign("max_hold_exit", "cycle-b")
    assert new_cycle["id"] != first["id"]
    assert new_cycle["attempts"] == {}


def test_cycle_summary_reports_mutually_exclusive_exit_buckets(tmp_path):
    ledger = TradeLedger(tmp_path / "phase3.sqlite3")
    try:
        rows = [
            ("normal", "normal_profit", "", False),
            ("fast", "fast_exit", "", False),
            ("regime", "regime_stop", "regime_stop", False),
            ("max", "max_hold", "max_hold_exit", False),
            ("hard", "hard_timeout", "hard_timeout_release", False),
            ("execution", "execution_emergency", "recovery_failure", True),
            ("manual", "manual_flatten", "manual_flatten", False),
        ]
        for index, (name, mode, reason, emergency) in enumerate(rows):
            ts = 10.0 + index
            ledger.upsert_cycle({
                "cycle_id": name, "updated_ts": ts + 1,
                "status": "completed", "direction": "long_entropy",
                "open_first_fill_ts": ts, "flat_ts": ts + 1,
                "hold_sec": 1.0, "total_cycle_pnl_usd": 0.01,
                "exit_mode": mode, "exit_reason": reason,
                "emergency_flag": emergency,
            })
        summary = ledger.cycle_summary(since=0.0, until=100.0)
        for key in ("normal_exit_rate", "fast_exit_rate",
                    "regime_stop_rate", "max_hold_exit_rate",
                    "hard_timeout_exit_rate",
                    "execution_emergency_rate", "manual_flatten_rate"):
            assert summary[key] == pytest.approx(1 / len(rows))
    finally:
        ledger.close()
