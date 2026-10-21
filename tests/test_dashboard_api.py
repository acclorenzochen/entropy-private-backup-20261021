import csv
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.dashboard_api.app import create_app  # noqa: E402
from entropy_arb.dashboard_api.readers import (downsample_points,
                                                read_history)  # noqa: E402
from entropy_arb.dashboard_state import (mark_dashboard_status,
                                         write_dashboard_snapshot)  # noqa: E402
from entropy_arb.trade_ledger import TradeLedger  # noqa: E402


def status_payload(ts: float = 1_700_000_000.0) -> dict:
    return {
        "schema_version": 1, "ts": ts,
        "time_utc": "2023-11-14T22:13:20.000Z",
        "bot_status": "running", "bot_started_ts": ts - 100,
        "venues": {"entropy": "ENTROPY", "hedge": "RH"},
        "symbol": "SNDK", "pair_state": "flat", "regime": "normal",
        "positions": {"entropy_base": 0, "hedge_base": 0,
                      "net_delta_base": 0, "net_delta_usd": 0},
        "equity": {
            "start_ts": ts - 90, "complete": True,
            "start_equity_usd": 1500, "current_equity_usd": 1503.5,
            "change_usd": 3.5,
            "venues": {
                "entropy": {"start_equity_usd": 1000,
                             "current_equity_usd": 1002,
                             "change_usd": 2},
                "hedge": {"start_equity_usd": 500,
                           "current_equity_usd": 501.5,
                           "change_usd": 1.5},
            },
        },
        "market": {"entropy_bid": 100, "entropy_ask": 100.02,
                   "hedge_bid": 99.99, "hedge_ask": 100.01,
                   "entropy_mid": 100.01, "hedge_mid": 100,
                   "premium_mid_bps": 1, "sell_edge_bps": -1,
                   "buy_edge_bps": -3},
        "strategy": {"seed_midline_bps": 0, "active_midline_bps": 0,
                     "short_center_bps": None, "long_center_bps": None,
                     "robust_sigma_bps": None, "short_entry_bps": 5,
                     "short_exit_bps": 1, "long_entry_bps": -5,
                     "long_exit_bps": -1, "effective_upper_bps": 5,
                     "effective_lower_bps": 5, "volatility_guard": False,
                     "cycle_active": False, "cycle_anchored": False,
                     "cycle_direction": None,
                     "cycle_entry_midline_bps": None,
                     "cycle_entry_sigma_bps": None,
                     "cycle_entry_band_bps": None,
                     "cycle_midline_drift_bps": None,
                     "cycle_add_blocked": False, "cycle_drift_guard": False,
                     "dynamic_midline_ready": False,
                     "regime_shift": False},
        "funding": {"entropy_bps_h": None, "hedge_bps_h": None,
                    "short_entropy_cost_bps": 0,
                    "long_entropy_cost_bps": 0,
                    "available": False, "stale": False},
        "risk": {"open_blocked": False, "open_block_reason": None,
                 "reduce_allowed": True, "net_tolerance_base": .001},
        "today": {"net_pnl_usd": None, "session_pnl_usd": None,
                  "day_started_ts": None, "session_started_ts": None,
                  "coverage_complete": False, "equity_updated_ts": None,
                  "realized_pnl_usd": None,
                  "unrealized_pnl_usd": None, "gross_pnl_usd": None,
                  "fees_usd": None, "funding_pnl_usd": None,
                  "turnover_usd": 0, "trade_count": 0},
        "last_trade": None,
    }


def test_status_unavailable_is_clean_503():
    root = Path(tempfile.mkdtemp())
    client = TestClient(create_app(root))
    response = client.get("/api/status")
    assert response.status_code == 503
    assert response.json() == {"detail": "status snapshot unavailable"}
    (root / "status.json").write_text("{")
    assert client.get("/api/status").status_code == 503
    (root / "status.json").write_text('{"ts": 1}')
    assert client.get("/api/status").status_code == 503


def test_existing_sqlite_dashboard_is_read_only_without_explicit_migration(
        tmp_path):
    root = tmp_path
    sqlite_path = root / "entropy_arb.sqlite3"
    ledger = TradeLedger(sqlite_path)
    ledger.close()
    (root / "trades.csv").write_text(
        "ts,action,direction,qty,buy_fill,sell_fill\n"
        "100,open,buy_entropy,1,1,1\n")
    before = sqlite_path.stat().st_mtime_ns

    with TestClient(create_app(root)):
        pass

    with sqlite3.connect(sqlite_path) as connection:
        trade_count = connection.execute(
            "SELECT COUNT(*) FROM trade_attempts").fetchone()[0]
    assert trade_count == 0
    assert sqlite_path.stat().st_mtime_ns == before


def test_status_staleness_and_atomic_writer(monkeypatch):
    root = Path(tempfile.mkdtemp())
    path = root / "status.json"
    write_dashboard_snapshot(path, status_payload())
    monkeypatch.setattr("entropy_arb.dashboard_api.readers.time.time",
                        lambda: 1_700_000_006.0)
    response = TestClient(create_app(root)).get("/api/status")
    assert response.status_code == 200
    assert response.json()["is_stale"] is True
    assert response.json()["snapshot_age_sec"] == 6
    assert not list(root.glob("*.tmp"))


def test_status_accepts_engine_strategy_name_field():
    root = Path(tempfile.mkdtemp())
    payload = status_payload()
    payload["strategy_name"] = "legacy"
    write_dashboard_snapshot(root / "status.json", payload)

    response = TestClient(create_app(root)).get("/api/status")

    assert response.status_code == 200
    assert response.json()["strategy_name"] == "legacy"


def test_stopped_status_preserves_last_snapshot_time():
    root = Path(tempfile.mkdtemp())
    path = root / "status.json"
    payload = status_payload()
    write_dashboard_snapshot(path, payload)

    assert mark_dashboard_status(path, "stopped", now=payload["ts"] + 10)
    response = TestClient(create_app(root)).get("/api/status")

    assert response.status_code == 200
    assert response.json()["bot_status"] == "stopped"
    assert response.json()["ts"] == payload["ts"]


def test_history_filters_and_downsamples_to_dashboard_budget():
    root = Path(tempfile.mkdtemp())
    path = root / "seconds.csv"
    now = 1_700_000_000.0
    fields = ["ts", "premium_mid_bps", "active_midline_bps",
              "short_entry_bps", "short_exit_bps", "long_entry_bps",
              "long_exit_bps"]
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for offset in range(3600):
            writer.writerow({key: offset for key in fields} | {
                "ts": now - 3599 + offset})
    result = read_history(path, "1h", now)
    assert len(result["points"]) <= 2000
    assert result["points"][-1]["ts"] == now


def test_chart_downsampling_preserves_endpoints_and_extrema():
    points = [{"ts": float(i), "value": float(i)} for i in range(100)]
    points[37]["value"] = -1000.0
    points[83]["value"] = 1000.0

    result = downsample_points(points, 12, "value")
    values = {point["value"] for point in result}

    assert len(result) <= 12
    assert result[0] == points[0]
    assert result[-1] == points[-1]
    assert -1000.0 in values and 1000.0 in values


def test_trades_normalization_and_empty_pnl():
    root = Path(tempfile.mkdtemp())
    payload = status_payload()
    payload["today"]["day_started_ts"] = 0
    payload["today"]["session_started_ts"] = 0
    (root / "status.json").write_text(json.dumps(payload))
    fields = ["ts", "action", "direction", "buy_notional",
              "sell_notional", "qty", "threshold_bps",
              "net_marginal_edge_bps", "fill_edge_usd", "ok",
              "buy_fill", "sell_fill", "buy_status", "sell_status",
              "buy_venue", "sell_venue", "reduce_only"]
    with (root / "trades.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerow({"ts": 1, "action": "HEDGE", "direction": "buy",
                         "buy_notional": 100, "sell_notional": 100,
                         "qty": 1, "threshold_bps": "",
                         "net_marginal_edge_bps": "", "fill_edge_usd": .1,
                         "ok": 1, "buy_fill": 1, "sell_fill": 0,
                         "buy_status": "filled", "sell_status": "failed",
                         "buy_venue": "ENTROPY", "sell_venue": "LIGHTER",
                         "reduce_only": 1})
    client = TestClient(create_app(root))
    response = client.get("/api/trades").json()
    trade = response["trades"][0]
    assert trade["action"] == "hedge" and trade["outcome"] == "one_leg"
    assert trade["entropy_leg"] == {
        "side": "buy", "planned_qty": 1, "fill_qty": 1,
        "status": "filled", "fee_usd": None,
        "fee_status": "none", "gross_realized_pnl_usd": None,
        "net_realized_pnl_usd": None, "pnl_status": "unavailable"}
    assert trade["lighter_leg"]["side"] == "sell"
    assert trade["lighter_leg"]["fee_usd"] is None
    assert trade["lighter_leg"]["fee_status"] == "none"
    assert trade["lighter_leg"]["pnl_status"] == "unavailable"
    assert trade["net_realized_pnl_usd"] is None
    assert trade["pnl_status"] == "unavailable"
    assert response["summary"]["one_leg_count"] == 1
    assert response["summary"]["paired_fill_edge_usd"] == 0
    assert response["total_count"] == 1
    assert response["offset"] == 0
    assert response["limit"] == 20
    assert response["has_more"] is False
    pnl = client.get("/api/pnl?range=today").json()
    assert pnl["summary"]["net_pnl_usd"] is None and pnl["points"] == []


def test_api_parameter_validation():
    client = TestClient(create_app(Path(tempfile.mkdtemp())))
    assert client.get("/api/history?range=7d").status_code == 400
    assert client.get("/api/trades?limit=101").status_code == 422
    assert client.get("/api/trades?offset=-1").status_code == 422
    assert client.get("/api/pnl?range=7d").status_code == 400


def test_unified_dashboard_contract_uses_one_snapshot():
    root = Path(tempfile.mkdtemp())
    write_dashboard_snapshot(root / "status.json", status_payload())
    response = TestClient(create_app(root)).get("/api/dashboard?range=1h")
    assert response.status_code == 200
    payload = response.json()
    assert payload["as_of_ts"] == 1_700_000_000.0
    assert set(payload) == {"as_of_ts", "status", "history", "pnl", "trades"}
    assert payload["status"]["ts"] == payload["as_of_ts"]
    assert payload["status"]["equity"]["change_usd"] == 3.5
    assert payload["status"]["equity"]["venues"]["hedge"]["change_usd"] == 1.5
