"""Config loading: example file, validation, CLI-selected markets.

Run:  python3 -m pytest tests/  (or  python3 tests/test_config.py)
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.config import ConfigError, load_config  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
EXAMPLE = os.path.join(ROOT, "config.example.yaml")
NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def write_tmp(text: str) -> str:
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(text)
    f.close()
    return f.name


MINIMAL = """
thresholds:
  midline_bps: 5.0
  upper_bps: 4.0
  lower_bps: 3.0
"""


def load(yaml_text: str, symbol="SNDK", hedge="lighter-rh"):
    return load_config(write_tmp(yaml_text), NO_ENV,
                       symbol=symbol, hedge_venue=hedge)


def test_example_config_loads():
    cfg = load_config(EXAMPLE, NO_ENV,
                      symbol="SNDK", hedge_venue="lighter-rh")
    assert cfg.symbol == "SNDK"
    assert cfg.entropy.kind == "hl" and cfg.entropy.hl_dex == "io"
    assert cfg.hedge_venue == "lighter-rh"
    assert cfg.hedge.kind == "lighter"
    assert cfg.hedge.lighter_profile.chain_id == 466324
    assert cfg.entropy.symbol == "SNDK" and cfg.hedge.symbol == "SNDK"
    assert cfg.recorder_enabled and cfg.recorder_csv
    assert cfg.dashboard and cfg.log_file
    assert cfg.cycle_exit.fast_exit_after_sec == 20
    assert cfg.cycle_exit.max_hold_sec == 600
    assert cfg.cycle_exit.long_hold_after_sec == 600
    assert cfg.cycle_exit.hard_timeout_sec == 3600
    assert cfg.cycle_exit.hard_timeout_min_profit_usd == -0.02
    assert not cfg.cycle_exit.pnl_loss_stop_enabled
    assert cfg.cycle_exit.max_cycle_loss_bps is None
    assert not cfg.cycle_exit.adverse_drift_exit_enabled
    assert cfg.cycle_exit.regime_stop_min_age_sec == 90
    assert cfg.reentry_guard.enabled
    assert cfg.margin_risk.enabled
    assert cfg.margin_risk.warning_available_ratio == 0.20
    assert cfg.margin_risk.critical_available_ratio == 0.05


def test_minimal_defaults():
    cfg = load(MINIMAL, hedge="lighter")
    assert cfg.midline_bps == 5.0 and cfg.upper_bps == 4.0 and cfg.lower_bps == 3.0
    assert cfg.hedge.label == "LIGHTER"
    assert cfg.hedge.lighter_profile.chain_id == 304
    assert cfg.take_fraction == 0.5          # defaults kick in
    assert cfg.recorder_enabled is True
    assert cfg.web_dashboard_enabled is False
    assert cfg.dashboard_api_host == "127.0.0.1"
    assert cfg.dashboard_api_port == 8765
    assert cfg.entropy.leverage is None and cfg.hedge.leverage is None
    assert cfg.allow_sell_entropy_open and cfg.allow_buy_entropy_open
    assert not cfg.adaptive_bands.enabled and not cfg.cycle_exit.enabled
    assert not cfg.wechat_clawbot.enabled
    assert cfg.wechat_clawbot.interval_sec == 3600.0
    assert cfg.wechat_clawbot.credentials_file == \
        "logs/wechat_clawbot_credentials.json"


def test_open_direction_policy_is_explicit():
    cfg = load(MINIMAL + """
thresholds:
  midline_bps: 5.0
  upper_bps: 4.0
  lower_bps: 3.0
  allow_sell_entropy_open: false
  allow_buy_entropy_open: true
""")
    assert not cfg.allow_sell_entropy_open
    assert cfg.allow_buy_entropy_open


def test_wechat_clawbot_config_is_non_secret_and_validated():
    cfg = load(MINIMAL + """
wechat_clawbot:
  enabled: true
  base_url: https://example.invalid
  interval_sec: 3600
  poll_timeout_sec: 35
  request_timeout_sec: 45
  state_file: logs/wechat-state.json
  credentials_file: logs/wechat-credentials.json
  send_startup: true
""")
    assert cfg.wechat_clawbot.enabled
    assert cfg.wechat_clawbot.base_url == "https://example.invalid"
    assert cfg.wechat_clawbot.state_file == "logs/wechat-state.json"
    assert cfg.wechat_clawbot.credentials_file == "logs/wechat-credentials.json"
    expect_error(MINIMAL + """
wechat_clawbot:
  interval_sec: 0
""", "wechat_clawbot")


def test_adaptive_bands_and_cycle_exit_config():
    cfg = load(MINIMAL + """
adaptive_bands:
  enabled: true
  entry_sigma_mult: 2.5
  min_upper_bps: 5
  min_lower_bps: 6
  max_upper_bps: 15
  max_lower_bps: 16
  max_sigma_bps: 8
cycle_exit:
  enabled: true
  anchor_mode: entry_midline
  max_midline_drift_bps: 4
  reset_on_flat: true
  state_json: /tmp/test-cycle.json
""")
    assert cfg.adaptive_bands.enabled
    assert cfg.adaptive_bands.entry_sigma_mult == 2.5
    assert cfg.adaptive_bands.min_upper_bps == 5
    assert cfg.adaptive_bands.max_lower_bps == 16
    assert cfg.cycle_exit.enabled
    assert cfg.cycle_exit.max_midline_drift_bps == 4
    assert cfg.cycle_exit.state_json == "/tmp/test-cycle.json"
    expect_error(MINIMAL + "\nadaptive_bands:\n  entry_sigma_mult: 0\n",
                 "adaptive_bands")
    expect_error(MINIMAL + "\ncycle_exit:\n  anchor_mode: current\n",
                 "anchor_mode")


def test_reference_policy_config_is_validated():
    cfg = load(MINIMAL + """
reference_policy:
  warning_penalty_bps: 1.0
  shifting:
    mode: dynamic_center
    sigma_mult: 2.0
    min_deviation_bps: 2.0
""")
    assert cfg.reference_policy.warning_penalty_bps == 1.0
    assert cfg.reference_policy.shifting_mode == "dynamic_center"
    assert cfg.reference_policy.shifting_min_deviation_bps == 2.0
    expect_error(MINIMAL + """
reference_policy:
  shifting:
    mode: unknown_mode
""", "reference_policy limits")


def test_explicit_leverage():
    cfg = load(MINIMAL + "\nentropy:\n  leverage: 3\nhedge:\n  leverage: 3\n",
               hedge="lighter")
    assert cfg.entropy.leverage == 3 and cfg.hedge.leverage == 3
    expect_error(MINIMAL + "\nentropy:\n  leverage: 0\n", "leverage")


def test_tradexyz_hedge():
    cfg = load(MINIMAL, hedge="tradexyz")
    assert cfg.hedge.kind == "hl" and cfg.hedge.hl_dex == "xyz"
    assert cfg.hedge.label == "XYZ"


def expect_error(yaml_text: str, needle: str, **kw):
    try:
        load(yaml_text, **kw)
    except ConfigError as e:
        assert needle in str(e), f"{needle!r} not in {e}"
        return
    raise AssertionError(f"expected ConfigError containing {needle!r}")


def test_unknown_key_rejected():
    expect_error(MINIMAL + "\nthresholdz:\n  x: 1\n",
                 "unknown config key 'thresholdz'")
    expect_error(MINIMAL + "\nsizing:\n  take_fractionn: 0.5\n",
                 "sizing.take_fractionn")


def test_markets_no_longer_config_keys():
    # symbol / hedge_venue moved to --symbol / --hedge: leftovers in the
    # YAML must fail loudly, not silently override the flags
    expect_error("symbol: SNDK\n" + MINIMAL, "unknown config key 'symbol'")
    expect_error("hedge_venue: tradexyz\n" + MINIMAL,
                 "unknown config key 'hedge_venue'")


def test_bad_cli_markets():
    expect_error(MINIMAL, "--hedge", hedge="binance")
    expect_error(MINIMAL, "--symbol", symbol="")


def test_missing_thresholds():
    expect_error("recorder:\n  enabled: true\n", "thresholds.")


def test_nonpositive_band():
    expect_error("thresholds:\n"
                 "  midline_bps: 5\n  upper_bps: 0\n  lower_bps: 3\n",
                 "must be > 0")


def test_new_config_ranges_and_unknown_keys():
    bad_exit = """
thresholds:
  midline_bps: 0
  upper_bps: 4
  lower_bps: 4
  exit_bps: 4
"""
    expect_error(bad_exit, "exit_bps")
    expect_error(MINIMAL + "\ndynamic_midline:\n  recovery_ratio: 1\n",
                 "dynamic_midline")
    expect_error(MINIMAL + "\nfunding:\n  on_unavailable: maybe\n",
                 "on_unavailable")
    expect_error(MINIMAL + "\ndashboard:\n  api_port: 70000\n", "api_port")
    expect_error(MINIMAL + "\ndashboard:\n  snapshot_sec: 0\n", "snapshot_sec")
    expect_error(MINIMAL + "\ndashboard:\n  day_timezone: Mars/Base\n",
                 "day_timezone")


def test_midline_lag_and_sample_quality_config():
    cfg = load(MINIMAL + """
premium_model:
  max_book_age_ms: 1200
  max_cross_venue_skew_ms: 500
  degraded_after_sec: 5
dynamic_midline:
  long_window_sec: 10800
  base_move_bps_per_10m: 0.5
  accelerated_move_bps_per_10m: 1.0
  absolute_max_move_bps_per_10m: 1.25
adaptive_bands:
  sigma_window_sec: 1800
midline_lag_guard:
  enabled: true
  warn_bps: 1.5
  block_bps: 3
  persist_sec: 900
  accelerate_bps: 2
  accelerate_persist_sec: 300
  recovery_sec: 300
  recovery_bps: 2
""")
    assert cfg.premium_model.max_book_age_ms == 1200
    assert cfg.dynamic_long_window_sec == 10800
    assert cfg.dynamic_accelerated_move_bps_per_10m == 1
    assert cfg.adaptive_bands.sigma_window_sec == 1800
    assert cfg.midline_lag_guard.enabled
    assert cfg.midline_lag_guard.recovery_bps == 2
    assert cfg.midline_lag_guard.warning_entry_penalty_bps == 1.5
    expect_error(MINIMAL + "\npremium_model:\n  max_book_age_ms: 0\n",
                 "premium_model")
    expect_error(MINIMAL + """
dynamic_midline:
  base_move_bps_per_10m: 1
  accelerated_move_bps_per_10m: 0.5
""", "dynamic_midline")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")


def test_lighter_request_budget_defaults_and_override():
    cfg = load(MINIMAL, hedge='lighter')
    assert (cfg.hedge.lighter_request_limit, cfg.hedge.lighter_realtime_reserve,
            cfg.hedge.lighter_history_limit) == (48, 12, 6)
    cfg = load(MINIMAL + "\n" + "lighter_requests:\n  limit_per_min: 30\n  realtime_reserve: 10\n  history_per_min: 3\n", hedge='lighter')
    assert (cfg.hedge.lighter_request_limit, cfg.hedge.lighter_realtime_reserve,
            cfg.hedge.lighter_history_limit) == (30, 10, 3)


def test_lighter_request_budget_rejects_invalid_bounds():
    import pytest
    for values in ((61, 12, 6), (1, 0, 1), (48, 48, 6), (48, -1, 6),
                   (48, 12, 37), (48, 12, 0)):
        limit, reserve, history = values
        settings = (f"\nlighter_requests:\n  limit_per_min: {limit}\n"
                    f"  realtime_reserve: {reserve}\n  history_per_min: {history}\n")
        with pytest.raises(ConfigError):
            load(MINIMAL + settings, hedge='lighter')
