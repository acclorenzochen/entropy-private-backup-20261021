import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from entropy_arb.config import load_config  # noqa: E402
from replay import Row, event_study, simulate  # noqa: E402


def cfg():
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write("""
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
    f.close()
    return load_config(f.name, os.devnull, symbol="TEST", hedge_venue="lighter")


def row(ts, premium):
    hp = 100.0
    ep = hp * (1 + premium / 1e4)
    return Row(ts, ep, ep, 10, 10, hp, hp, 10, 10, premium,
               (ep / hp - 1) * 1e4, (hp / ep - 1) * 1e4, None, None)


def test_event_study_convergence_and_mae():
    rows = [row(0, 0), row(1, 6), row(2, 8), row(3, 4), row(4, 0)]
    result = event_study(rows, cfg())
    assert result["count"] == 1
    event = result["events"][0]
    assert event["direction"] == "short_entropy"
    assert event["time_to_exit_sec"] == 3
    assert event["mae_bps"] == 2


def test_event_study_non_convergence():
    rows = [row(0, 0), row(1, 6), row(2, 10), row(3, 14), row(4, 12)]
    result = event_study(rows, cfg(), max_horizon=100)
    assert result["events"][0]["time_to_exit_sec"] is None


def test_strategy_replay_one_flat_to_flat_cycle():
    rows = [row(0, 0), row(1, 6), row(2, 4), row(3, 2), row(4, 0)]
    result = simulate(rows, cfg(), notional=100, slippage_bps=0)
    assert result["opens"] == 1
    assert result["round_trips"] == 1
    assert result["holding_p50"] == 3


def test_strategy_replay_reports_adaptive_and_cycle_metrics():
    result = simulate(
        [row(0, 0), row(1, 6), row(2, 4), row(3, 2), row(4, 0)],
        cfg(), notional=100, slippage_bps=0,
        adaptive_entry_sigma_mult=2.0, cycle_drift_limit=4.0)
    assert result["avg_effective_upper_bps"] == 5
    assert result["avg_effective_lower_bps"] == 5
    assert result["entries_blocked_by_volatility"] == 0
    assert result["adds_blocked_by_cycle_drift"] == 0
    assert result["time_in_volatility_guard"] == 0
    assert result["unanchored_cycle_seconds"] == 0
