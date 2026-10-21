import json
import sqlite3
from datetime import datetime, timezone

import pytest

from entropy_arb.retro_farming import (
    RetroAnalysisConfig,
    RetroCanaryPolicy,
    analyze_project_contribution,
    build_shadow_retro_snapshot,
    load_project_contribution,
    plan_retro_canary,
)


def _ts(day: int, hour: int = 0) -> float:
    return datetime(2026, 1, day, hour, tzinfo=timezone.utc).timestamp()


def _cycle(cycle_id: str, opened: float, *, hold: float, notional: float,
           pnl: float, execution_ids: list[str]) -> dict:
    return {
        "cycle_id": cycle_id,
        "status": "completed",
        "open_first_fill_ts": opened,
        "hedge_completed_ts": opened + 1,
        "reduce_first_fill_ts": opened + hold,
        "flat_ts": opened + hold + 1,
        "hold_sec": hold,
        "notional_usd": notional,
        "total_cycle_pnl_usd": pnl,
        "recovery_count": 0,
        "emergency_flag": 0,
        "exit_mode": "normal",
        "exit_reason": "profit",
        "payload_json": json.dumps({"execution_ids": execution_ids}),
    }


def test_contribution_projection_keeps_raw_metrics_and_hypotheses_separate():
    first = _cycle("c1", _ts(1), hold=120, notional=100, pnl=-0.02,
                   execution_ids=["open1", "close1"])
    second = _cycle("c2", _ts(8), hold=3600, notional=200, pnl=0.01,
                    execution_ids=["open2", "close2"])
    incomplete = {**_cycle("bad", _ts(9), hold=60, notional=50, pnl=1,
                           execution_ids=["bad-open"]),
                  "reduce_first_fill_ts": None}
    fills = [
        {"execution_id": "open1", "venue": "entropy", "source": "order",
         "ts": _ts(1), "qty": 1, "price": 100, "fee_usd": 0.01,
         "metadata_json": "{}"},
        {"execution_id": "close1", "venue": "entropy", "source": "order",
         "ts": _ts(1, 1), "qty": 1, "price": 101, "fee_usd": 0.01,
         "metadata_json": json.dumps({"liquidity_role": "maker"})},
        {"execution_id": "open2", "venue": "entropy", "source": "order",
         "ts": _ts(8), "qty": 2, "price": 100, "fee_usd": 0.02,
         "metadata_json": "{}"},
        {"execution_id": "close2", "venue": "hedge", "source": "order",
         "ts": _ts(8, 1), "qty": 2, "price": 99, "fee_usd": 0.0,
         "metadata_json": "{}"},
        {"execution_id": "bad-open", "venue": "entropy", "source": "order",
         "ts": _ts(9), "qty": 50, "price": 100, "fee_usd": 10,
         "metadata_json": "{}"},
    ]

    result = analyze_project_contribution(
        [first, second, incomplete], fills,
        config=RetroAnalysisConfig(growth_volume_multiplier=0.1))

    assert result["strict_cycles"] == 2
    assert result["entropy_fill_count"] == 3
    assert result["raw_entropy_volume_usd"] == pytest.approx(401)
    assert result["growth_adjusted_volume_usd"] == pytest.approx(40.1)
    assert result["entropy_fee_contribution_usd"] == pytest.approx(0.04)
    assert result["maker_volume_usd"] == pytest.approx(101)
    assert result["oi_usd_hours"] == pytest.approx(
        100 * 120 / 3600 + 200)
    assert result["active_utc_days"] == 2
    assert result["active_utc_weeks"] == 2
    assert result["cash"]["total_cycle_pnl_usd"] == pytest.approx(-0.01)
    assert result["cash"]["acquisition_cost_per_1k_raw_volume_usd"] == \
        pytest.approx(0.01 / 401 * 1000)
    assert set(result["hypothesis_coverage"]) == {
        "FEE_WEIGHTED", "VOLUME_WEIGHTED", "RETENTION_WEIGHTED",
        "OI_WEIGHTED", "LIQUIDITY_QUALITY",
    }
    assert result["assumption"].startswith("hypothetical_")
    assert result["contribution_coverage"]["LIQUIDITY"] > 0


def test_loader_uses_strict_recent_cycle_cohort_and_exact_execution_ids(tmp_path):
    path = tmp_path / "ledger.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE cycle_records (
            cycle_id TEXT PRIMARY KEY,
            status TEXT,
            open_first_fill_ts REAL,
            hedge_completed_ts REAL,
            reduce_first_fill_ts REAL,
            flat_ts REAL,
            hold_sec REAL,
            notional_usd REAL,
            total_cycle_pnl_usd REAL,
            recovery_count INTEGER,
            emergency_flag INTEGER,
            exit_mode TEXT,
            exit_reason TEXT,
            payload_json TEXT
        );
        CREATE TABLE fills (
            execution_id TEXT,
            venue TEXT,
            source TEXT,
            ts REAL,
            qty REAL,
            price REAL,
            fee_usd REAL,
            metadata_json TEXT
        );
        """)
    old = _cycle("old", _ts(1), hold=120, notional=10, pnl=-1,
                 execution_ids=["old-open"])
    new = _cycle("new", _ts(8), hold=120, notional=20, pnl=-0.1,
                 execution_ids=["new-open"])
    columns = list(old)
    placeholders = ",".join("?" for _ in columns)
    connection.execute(
        f"INSERT INTO cycle_records({','.join(columns)}) VALUES({placeholders})",
        [old[column] for column in columns])
    connection.execute(
        f"INSERT INTO cycle_records({','.join(columns)}) VALUES({placeholders})",
        [new[column] for column in columns])
    connection.executemany(
        "INSERT INTO fills VALUES(?,?,?,?,?,?,?,?)",
        [("old-open", "entropy", "order", _ts(1), 1, 10, 0.01, "{}"),
         ("new-open", "entropy", "order", _ts(8), 2, 10, 0.02, "{}")])
    connection.commit()
    connection.close()

    result = load_project_contribution(path, limit=1)

    assert result["strict_cycles"] == 1
    assert result["raw_entropy_volume_usd"] == pytest.approx(20)
    assert result["cash"]["total_cycle_pnl_usd"] == pytest.approx(-0.1)


def test_shadow_canary_prefers_retention_and_never_executes():
    report = analyze_project_contribution(
        [_cycle("c1", _ts(1), hold=10, notional=15, pnl=-0.002,
                execution_ids=["open", "close"])],
        [{"execution_id": execution_id, "venue": "entropy",
          "source": "order", "ts": _ts(1), "qty": 0.15, "price": 100,
          "fee_usd": 0.001, "metadata_json": "{}"}
         for execution_id in ("open", "close")])

    decision = plan_retro_canary(
        report, now=_ts(2), expected_loss_usd=0.005,
        expected_raw_volume_usd=30)

    assert decision["selected_bucket"] == "RETENTION"
    assert decision["status"] == "shadow_eligible"
    assert decision["eligible"] is True
    assert decision["should_execute"] is False
    assert decision["loss_budget"]["bucket_usd"] == pytest.approx(1.2)


def test_shadow_canary_uses_new_liquidity_bucket_within_active_day():
    report = analyze_project_contribution(
        [_cycle("c1", _ts(1), hold=10, notional=15, pnl=-0.002,
                execution_ids=["open", "close"])],
        [{"execution_id": execution_id, "venue": "entropy",
          "source": "order", "ts": _ts(1), "qty": 0.15, "price": 100,
          "fee_usd": 0.001, "metadata_json": "{}"}
         for execution_id in ("open", "close")])

    decision = plan_retro_canary(
        report, now=_ts(1, 2), expected_loss_usd=0.005,
        expected_raw_volume_usd=30)

    assert decision["selected_bucket"] == "LIQUIDITY"
    assert decision["required_strategy"] == "maker-hedge"
    assert decision["status"] == "shadow_eligible"
    assert decision["should_execute"] is False


def test_canary_blocks_repeated_episode_and_cost_or_budget_breach():
    report = analyze_project_contribution(
        [_cycle("c1", _ts(1, 3), hold=10, notional=15, pnl=-0.002,
                execution_ids=["open", "close"])],
        [{"execution_id": execution_id, "venue": "entropy",
          "source": "order", "ts": _ts(1, 3), "qty": 0.15,
          "price": 100, "fee_usd": 0.001, "metadata_json": "{}"}
         for execution_id in ("open", "close")])

    repeated = plan_retro_canary(
        report, now=_ts(1, 3) + 1800, requested_bucket="OI",
        expected_loss_usd=0.02, expected_raw_volume_usd=1,
        spent_loss_usd=3.0,
        policy=RetroCanaryPolicy(mode="active"))

    assert repeated["eligible"] is False
    assert repeated["should_execute"] is False
    assert {"same_utc_hour_episode", "daily_or_new_bucket_gate",
            "minimum_spacing", "episode_expected_loss_cap",
            "total_loss_budget", "acquisition_cost_cap"}.issubset(
                repeated["reasons"])


def test_shadow_snapshot_loader_keeps_execution_disabled(tmp_path):
    path = tmp_path / "ledger.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE cycle_records (
            cycle_id TEXT PRIMARY KEY, status TEXT,
            open_first_fill_ts REAL, hedge_completed_ts REAL,
            reduce_first_fill_ts REAL, flat_ts REAL, hold_sec REAL,
            notional_usd REAL, total_cycle_pnl_usd REAL,
            recovery_count INTEGER, emergency_flag INTEGER,
            exit_mode TEXT, exit_reason TEXT, payload_json TEXT
        );
        CREATE TABLE fills (
            execution_id TEXT, venue TEXT, source TEXT, ts REAL, qty REAL,
            price REAL, fee_usd REAL, metadata_json TEXT
        );
        """)
    connection.commit()
    connection.close()

    snapshot = build_shadow_retro_snapshot(path, now=_ts(1))

    assert snapshot["strict_cycles"] == 0
    assert snapshot["shadow_canary_plan"]["mode"] == "shadow"
    assert snapshot["shadow_canary_plan"]["should_execute"] is False
