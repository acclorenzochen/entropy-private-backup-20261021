"""Focused offline checks for the second-stage execution contracts."""
import asyncio
import time

from entropy_arb.config import merge_hot_reloadable_config
from entropy_arb.engine import Engine
from entropy_arb.execution_state import ExecutionState
from entropy_arb.signal import InventoryCycle
from entropy_arb.trade_ledger import TradeLedger
from test_config_reload import NO_ENV, config_text, read_config
from test_engine import make_cfg


def test_execution_checkpoint_roundtrip_keeps_recovery_metadata():
    state = ExecutionState(
        "phase2", "open", "buy_entropy", 1.0, True, False, False,
        recovery_next_retry_ts=123.5, recovery_attempt=3,
        recovery_backoff_sec=2.0, hedge_reserve_breach=True,
        hedge_reserve_breach_ts=122.0)
    restored = ExecutionState.from_dict(state.to_dict())
    assert restored.recovery_next_retry_ts == 123.5
    assert restored.recovery_attempt == 3
    assert restored.recovery_backoff_sec == 2.0
    assert restored.hedge_reserve_breach is True
    assert restored.hedge_reserve_breach_ts == 122.0


def test_inventory_cycle_roundtrip_keeps_cycle_id():
    cycle = InventoryCycle()
    cycle.start(direction="long_entropy", opened_ts=100.0,
                entry_midline_bps=-4.0, entry_sigma_bps=1.2,
                entry_band_bps=1.8, entry_threshold_bps=-2.2,
                paired_qty=.1, cycle_id="cycle-1")
    assert InventoryCycle.from_dict(cycle.to_dict()).cycle_id == "cycle-1"


def test_cycle_ledger_reports_completion_and_execution_quality(tmp_path):
    ledger = TradeLedger(tmp_path / "phase2.sqlite3")
    try:
        ledger.upsert_cycle({
            "cycle_id": "cycle-1", "updated_ts": 130.0,
            "status": "completed", "direction": "long_entropy",
            "open_first_fill_ts": 100.0, "hedge_completed_ts": 101.0,
            "flat_ts": 130.0, "hold_sec": 30.0,
            "total_cycle_pnl_usd": .04, "predicted_close_pnl_usd": .06,
            "exit_mode": "normal", "partial_first_fill": False,
            "recovery_count": 0, "emergency_flag": False,
        })
        ledger.upsert_cycle({
            "cycle_id": "cycle-2", "updated_ts": 150.0,
            "status": "active", "direction": "short_entropy",
            "open_first_fill_ts": 140.0, "hedge_completed_ts": 143.0,
            "partial_first_fill": True, "recovery_count": 1,
        })
        summary = ledger.cycle_summary(since=0, until=200)
        assert summary["record_count"] == 2
        assert summary["completed_count"] == 1
        assert summary["completion_rate"] == .5
        assert summary["median_cycle_duration_sec"] == 30.0
        assert summary["p95_unhedged_sec"] == 3.0
        assert summary["partial_first_fill_rate"] == .5
        assert summary["recovery_rate"] == .5
    finally:
        ledger.close()


def test_cycle_summary_persists_monotonic_opportunity_funnel_windows(tmp_path):
    ledger = TradeLedger(tmp_path / "funnel.sqlite3")
    try:
        ledger.record_trade_attempt({
            "trade_key": "funnel-exec-1", "execution_id": "funnel-exec-1",
            "ts": 1000.0, "action": "open", "direction": "buy_entropy",
            "qty": 1.0, "buy_fill": 1.0, "sell_fill": 1.0,
            "buy_venue": "entropy", "sell_venue": "hedge",
            "outcome": "paired", "open_opportunity_id": "op-1",
            "open_campaign_id": "op-1:campaign:1",
            "economic_opportunity": True,
        })
        ledger.upsert_cycle({
            "cycle_id": "funnel-exec-1", "updated_ts": 1030.0,
            "status": "completed", "direction": "long_entropy",
            "open_first_fill_ts": 1000.0, "hedge_completed_ts": 1001.0,
            "flat_ts": 1030.0, "hold_sec": 30.0,
            "total_cycle_pnl_usd": .01,
        })

        summary = ledger.cycle_summary(since=0.0, until=2000.0)
        funnel = summary["funnel"]
        assert funnel == {
            **funnel,
            "signal_episode_count": 1,
            "economic_opportunity_count": 1,
            "open_campaign_count": 1,
            "open_fill_count": 1,
            "hedge_complete_count": 1,
            "complete_cycle_count": 1,
        }
        assert set(summary["funnel_windows"]) == {
            "session", "15m", "1h", "6h", "24h"
        }
        assert summary["funnel_windows"]["session"] == funnel
        assert summary["funnel_windows"]["15m"]["complete_cycle_count"] == 0
    finally:
        ledger.close()


def test_recovery_backoff_is_durable_and_blocks_duplicate_call(tmp_path):
    cfg = make_cfg(midline=0, upper=2, lower=2)
    engine = Engine(cfg, ledger_path=str(tmp_path / "recovery.sqlite3"))
    state = ExecutionState(
        "recovery-1", "open", "buy_entropy", .1, True, False, False,
        first_filled=.1, pending_leg="hedge", pending_order_ref="hedge-1",
        recovery_next_retry_ts=time.time() + 10.0, recovery_attempt=2,
        recovery_backoff_sec=1.0)
    engine.active_execution = state
    try:
        assert asyncio.run(engine._recover_active_execution(immediate=False)) is False
        assert state.recovery_attempt == 2
        assert engine.hedge.resolved == [] if engine.hedge else True
    finally:
        engine.ledger.close()


def test_hot_reload_marks_restart_only_settings_without_applying_them(tmp_path):
    current_path = tmp_path / "current.yaml"
    candidate_path = tmp_path / "candidate.yaml"
    current_path.write_text(config_text())
    candidate_path.write_text(config_text(upper=3.0))
    current = read_config(current_path)
    candidate = read_config(candidate_path)
    updated, changed, ignored = merge_hot_reloadable_config(current, candidate)
    engine = Engine(current, config_file=str(current_path), env_file=NO_ENV,
                    ledger_path=str(tmp_path / "hot.sqlite3"))
    try:
        engine._apply_hot_reload(updated, changed, ignored + ("cycle_exit",))
        assert engine.config_restart_required is True
        assert "cycle_exit" in engine.config_restart_required_paths
    finally:
        engine.ledger.close()


def test_bounded_run_stops_only_after_new_completed_cycles(tmp_path):
    engine = Engine(make_cfg(), ledger_path=str(tmp_path / "bounded.sqlite3"),
                    max_complete_cycles=2)
    completed = {
        "status": "completed",
        "open_first_fill_ts": 10.0,
        "hedge_completed_ts": 11.0,
        "reduce_first_fill_ts": 19.0,
        "flat_ts": 20.0,
        "recovery_count": 0,
        "emergency_flag": 0,
        "exit_mode": "normal_profit",
    }
    try:
        engine._maybe_stop_after_complete_cycle({
            **completed, "cycle_id": "cycle-1"})
        engine._maybe_stop_after_complete_cycle({
            **completed, "cycle_id": "cycle-1"})
        assert engine.completed_cycle_count == 1
        assert not engine.stop.is_set()

        engine._maybe_stop_after_complete_cycle({
            **completed, "cycle_id": "cycle-2"})
        assert engine.completed_cycle_count == 2
        assert engine.stop.is_set()
    finally:
        engine.ledger.close()


def test_run_loss_limit_counts_every_completed_cycle_but_target_is_strict(tmp_path):
    engine = Engine(make_cfg(), ledger_path=str(tmp_path / "loss.sqlite3"),
                    max_complete_cycles=2, max_run_loss_usd=0.05)
    recovery = {
        "cycle_id": "recovery", "status": "completed",
        "open_first_fill_ts": 10.0, "flat_ts": 20.0,
        "total_cycle_pnl_usd": -0.051, "recovery_count": 1,
    }
    try:
        engine._maybe_stop_after_complete_cycle(recovery)
        engine._maybe_stop_after_complete_cycle(recovery)
        assert engine.run_realized_pnl_usd == -0.051
        assert engine.completed_cycle_count == 0
        assert engine.stop.is_set()
    finally:
        engine.ledger.close()
