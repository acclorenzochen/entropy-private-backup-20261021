from types import SimpleNamespace

from entropy_arb.dashboard_state import _equity_snapshot


def test_equity_snapshot_sums_and_preserves_venue_changes():
    engine = SimpleNamespace(
        start_ts=100.0,
        venues={
            "entropy": SimpleNamespace(start_equity=1000.0, equity=1002.25),
            "hedge": SimpleNamespace(start_equity=500.0, equity=499.5),
        },
    )

    result = _equity_snapshot(engine, {"session_started_ts": 101.0})

    assert result["complete"] is True
    assert result["start_ts"] == 101.0
    assert result["start_equity_usd"] == 1500.0
    assert result["current_equity_usd"] == 1501.75
    assert result["change_usd"] == 1.75
    assert result["venues"]["entropy"]["change_usd"] == 2.25
    assert result["venues"]["hedge"]["change_usd"] == -0.5


def test_equity_snapshot_marks_incomplete_without_guessing():
    engine = SimpleNamespace(
        start_ts=100.0,
        venues={
            "entropy": SimpleNamespace(start_equity=1000.0, equity=1002.25),
            "hedge": SimpleNamespace(start_equity=None, equity=499.5),
        },
    )

    result = _equity_snapshot(engine, {"session_started_ts": 101.0})

    assert result["complete"] is False
    assert result["start_equity_usd"] is None
    assert result["current_equity_usd"] is None
    assert result["change_usd"] is None
    assert result["venues"]["entropy"]["change_usd"] == 2.25
    assert result["venues"]["hedge"]["change_usd"] is None
