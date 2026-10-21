import json
from datetime import datetime
from zoneinfo import ZoneInfo

from entropy_arb.dashboard_state import EquityPnlTracker


TZ = ZoneInfo("Asia/Taipei")


def ts(value: str) -> float:
    return datetime.fromisoformat(value).replace(tzinfo=TZ).timestamp()


def test_tracks_daily_and_session_equity_delta_and_persists(tmp_path):
    path = tmp_path / "pnl_state.json"
    tracker = EquityPnlTracker(path, "Asia/Taipei")
    tracker.observe([100.0, 50.0], ts("2026-08-29T12:00:00"))
    tracker.observe([100.75, 49.50], ts("2026-08-29T12:01:00"))
    snap = tracker.snapshot()
    assert snap["net_pnl_usd"] == .25
    assert snap["session_pnl_usd"] == .25
    assert snap["coverage_complete"] is False
    assert json.loads(path.read_text())["day_start_equity_usd"] == 150.0

    restarted = EquityPnlTracker(path, "Asia/Taipei")
    restarted.observe([100.5, 49.5], ts("2026-08-29T12:02:00"))
    snap = restarted.snapshot()
    assert snap["net_pnl_usd"] == 0.0
    assert snap["session_pnl_usd"] == 0.0
    restarted.observe([101.0, 49.5], ts("2026-08-29T12:03:00"))
    assert restarted.snapshot()["net_pnl_usd"] == .5
    assert restarted.snapshot()["session_pnl_usd"] == .5


def test_rolls_daily_baseline_at_local_midnight(tmp_path):
    tracker = EquityPnlTracker(tmp_path / "pnl_state.json", "Asia/Taipei")
    tracker.observe([150.0], ts("2026-08-29T23:59:59"))
    tracker.observe([151.0], ts("2026-08-30T00:00:05"))
    snap = tracker.snapshot()
    assert snap["net_pnl_usd"] == 0.0
    assert snap["session_pnl_usd"] == 1.0
    assert snap["coverage_complete"] is True
