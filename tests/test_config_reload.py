"""Runtime YAML reload: safe strategy overlay and file watcher."""
import asyncio
import os
import tempfile

import pytest

from entropy_arb.config import (ConfigError, load_config,
                                merge_hot_reloadable_config)
from entropy_arb.engine import Engine


NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def config_text(*, upper=2.0, min_upper=5.0, max_order=500.0,
                dynamic_sample=1.0, recovery_bps=2.0,
                allow_sell=True):
    return f"""
thresholds:
  midline_bps: -4.2
  upper_bps: {upper}
  lower_bps: 2.0
  exit_bps: 0.8
  allow_sell_entropy_open: {str(allow_sell).lower()}
dynamic_midline:
  enabled: true
  sample_sec: {dynamic_sample}
midline_lag_guard:
  enabled: true
  recovery_bps: {recovery_bps}
adaptive_bands:
  enabled: true
  min_upper_bps: {min_upper}
  min_lower_bps: 5.0
  max_upper_bps: 15.0
  max_lower_bps: 15.0
hot_reload:
  enabled: true
  poll_sec: 0.01
sizing:
  max_order_notional_usd: {max_order}
"""


def read_config(path):
    return load_config(str(path), NO_ENV, symbol="SNDK",
                       hedge_venue="lighter-rh")


def test_merge_applies_strategy_fields_only(tmp_path):
    current_path = tmp_path / "current.yaml"
    candidate_path = tmp_path / "candidate.yaml"
    current_path.write_text(config_text())
    candidate_path.write_text(config_text(upper=3.0, min_upper=3.0,
                                          max_order=999.0,
                                          dynamic_sample=0.5,
                                          recovery_bps=1.8,
                                          allow_sell=False))

    current = read_config(current_path)
    candidate = read_config(candidate_path)
    updated, changed, ignored = merge_hot_reloadable_config(current, candidate)

    assert updated.upper_bps == 3.0
    assert updated.adaptive_bands.min_upper_bps == 3.0
    assert updated.dynamic_sample_sec == 0.5
    assert updated.midline_lag_guard.recovery_bps == 1.8
    assert not updated.allow_sell_entropy_open
    # Per-slice sizing is deliberately restart-only.
    assert updated.max_order_notional == current.max_order_notional == 500.0
    assert "thresholds.upper_bps" in changed
    assert "adaptive_bands.min_upper_bps" in changed
    assert "dynamic_midline.sample_sec" in changed
    assert "thresholds.allow_sell_entropy_open" in changed
    assert "sizing" in ignored


def test_bootstrap_model_warmup_is_validated_and_hot_reloadable(tmp_path):
    current_path = tmp_path / "current.yaml"
    candidate_path = tmp_path / "candidate.yaml"
    current_path.write_text(config_text() + """
bootstrap:
  enabled: true
  dynamic_midline_warmup_sec: 120
  dynamic_midline_min_samples: 90
""")
    candidate_path.write_text(config_text() + """
bootstrap:
  enabled: true
  dynamic_midline_warmup_sec: 180
  dynamic_midline_min_samples: 120
""")

    current = read_config(current_path)
    candidate = read_config(candidate_path)
    updated, changed, ignored = merge_hot_reloadable_config(current, candidate)

    assert updated.bootstrap.enabled is True
    assert updated.bootstrap.dynamic_midline_warmup_sec == 180
    assert updated.bootstrap.dynamic_midline_min_samples == 120
    assert "bootstrap.dynamic_midline_warmup_sec" in changed
    assert "bootstrap.dynamic_midline_min_samples" in changed
    assert not ignored


def test_execution_repair_policy_is_nested_and_hot_reloadable(tmp_path):
    current_path = tmp_path / "current.yaml"
    candidate_path = tmp_path / "candidate.yaml"
    current_path.write_text(config_text() + """
execution:
  exit_campaign_sec: 3
  exit_campaign_retry_interval_sec: 0.15
  dynamic_open_first_leg:
    enabled: true
    min_bps: 3.5
    max_bps: 8
    adverse_move_percentile: 0.9
    safety_margin_bps: 0.5
  open_retry_campaign_sec: 2
  open_retry_interval_sec: 0.25
  open_retry_max_attempts: 3
  open_execution_room_min_bps: 1
  reserve_reduce_slots: 2
""")
    candidate_path.write_text(current_path.read_text().replace(
        "open_retry_campaign_sec: 2", "open_retry_campaign_sec: 4").replace(
        "open_execution_room_min_bps: 1", "open_execution_room_min_bps: 1.5"))

    current = read_config(current_path)
    candidate = read_config(candidate_path)
    updated, changed, ignored = merge_hot_reloadable_config(current, candidate)

    assert updated.reduce_execution.open_retry_campaign_sec == 4
    assert updated.reduce_execution.open_execution_room_min_bps == 1.5
    assert "execution.open_retry_campaign_sec" in changed
    assert "execution.open_execution_room_min_bps" in changed
    assert not any(path.startswith("execution.open_") for path in ignored)


def test_reference_policy_is_nested_and_hot_reloadable(tmp_path):
    current_path = tmp_path / "current.yaml"
    candidate_path = tmp_path / "candidate.yaml"
    reference = """
reference_policy:
  enabled: true
  warning_penalty_bps: 1.0
  accelerating:
    mode: blended_midline
    sigma_mult: 1.5
  shifting:
    mode: dynamic_center
    sigma_mult: 2.0
    min_deviation_bps: 2.0
  blocked:
    mode: dynamic_center
    sigma_mult: 2.0
    min_deviation_bps: 2.5
  extreme_dislocation:
    enabled: true
    sigma_mult: 3.0
"""
    current_path.write_text(config_text() + reference)
    candidate_path.write_text((config_text() + reference).replace(
        "warning_penalty_bps: 1.0", "warning_penalty_bps: 1.25").replace(
        "min_deviation_bps: 2.5", "min_deviation_bps: 3.0"))

    current = read_config(current_path)
    candidate = read_config(candidate_path)
    updated, changed, ignored = merge_hot_reloadable_config(current, candidate)

    assert updated.reference_policy.warning_penalty_bps == 1.25
    assert updated.reference_policy.blocked_min_deviation_bps == 3.0
    assert "reference_policy.warning_penalty_bps" in changed
    assert "reference_policy.blocked.min_deviation_bps" in changed
    assert not ignored


def test_legacy_top_level_reduce_execution_is_rejected(tmp_path):
    path = tmp_path / "legacy.yaml"
    path.write_text(config_text() + """
reduce_execution:
  open_retry_campaign_sec: 2
""")
    with pytest.raises(ConfigError, match="unknown config key 'reduce_execution'"):
        read_config(path)


def test_config_watcher_applies_next_runtime_scan(tmp_path):
    async def run():
        path = tmp_path / "config.yaml"
        path.write_text(config_text())
        engine = Engine(read_config(path), config_file=str(path),
                        env_file=NO_ENV)
        task = asyncio.create_task(engine._config_reload_loop())
        try:
            await asyncio.sleep(0.03)
            path.write_text(config_text(upper=3.0, min_upper=3.0,
                                        max_order=999.0, dynamic_sample=0.5,
                                        recovery_bps=1.8))
            for _ in range(100):
                if engine.cfg.upper_bps == 3.0:
                    break
                await asyncio.sleep(0.01)
            assert engine.cfg.upper_bps == 3.0
            assert engine.cfg.adaptive_bands.min_upper_bps == 3.0
            assert engine.cfg.dynamic_sample_sec == 0.5
            assert engine.cfg.midline_lag_guard.recovery_bps == 1.8
            assert engine.cfg.max_order_notional == 500.0
            assert engine.last_config_reload_ts is not None
            assert engine.last_config_reload_error is None
        finally:
            engine.request_stop()
            await task

    asyncio.run(run())
