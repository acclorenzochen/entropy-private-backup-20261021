import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.signal import (MidlineLagGuard, MidlineLagState,
                                RollingPremiumModel)  # noqa: E402


def test_lag_guard_requires_persistence_for_acceleration_and_block():
    guard = MidlineLagGuard(
        enabled=True, warn_bps=1.5, block_bps=3, persist_sec=900,
        accelerate_bps=2, accelerate_persist_sec=300, recovery_sec=300)
    assert guard.update(1, 0) == MidlineLagState.NORMAL
    assert guard.update(2, 0) == MidlineLagState.WARNING
    assert guard.update(2, 299) == MidlineLagState.WARNING
    assert guard.update(2, 300) == MidlineLagState.ACCELERATING
    assert guard.update(3.1, 301) == MidlineLagState.ACCELERATING
    assert guard.update(3.1, 1201) == MidlineLagState.BLOCKED


def test_blocked_lag_recovers_below_independent_threshold():
    guard = MidlineLagGuard(
        enabled=True, warn_bps=1.5, block_bps=3, persist_sec=1,
        accelerate_bps=2, accelerate_persist_sec=1, recovery_sec=300,
        recovery_bps=2.0)
    guard.update(3.1, 0)
    assert guard.update(3.1, 1) == MidlineLagState.BLOCKED
    assert guard.update(1.8, 301) == MidlineLagState.BLOCKED
    assert guard.update(1.8, 601) == MidlineLagState.NORMAL


def test_not_ready_snapshot_freezes_last_valid_active_midline():
    model = RollingPremiumModel(
        short_window_sec=4, long_window_sec=20, warmup_sec=0,
        min_samples=1, shift_min_bps=3, shift_sigma_mult=2,
        recovery_ratio=.6, recovery_sec=3, seed_midline_bps=0,
        base_move_bps_per_10m=60, accelerated_move_bps_per_10m=60,
        absolute_max_move_bps_per_10m=60, sigma_window_sec=10)
    model.observe(0, 10)
    model.snapshot(0)
    valid = model.snapshot(10)
    assert valid.ready and valid.active_midline_bps > 0

    model.min_samples = 99
    frozen = model.snapshot(11)
    assert not frozen.ready
    assert frozen.active_midline_bps == valid.active_midline_bps


def test_blocked_state_still_moves_midline_toward_long_center():
    model = RollingPremiumModel(
        short_window_sec=4, long_window_sec=20, warmup_sec=0,
        min_samples=1, shift_min_bps=3, shift_sigma_mult=2,
        recovery_ratio=.6, recovery_sec=3, seed_midline_bps=0,
        base_move_bps_per_10m=.5, accelerated_move_bps_per_10m=1,
        absolute_max_move_bps_per_10m=1.25,
        midline_lag_guard_enabled=True, midline_lag_warn_bps=1.5,
        midline_lag_block_bps=3, midline_lag_persist_sec=900,
        midline_lag_accelerate_bps=2,
        midline_lag_accelerate_persist_sec=300,
        midline_lag_recovery_sec=300, sigma_window_sec=10)
    for ts in range(5):
        model.observe(ts, 10)
    first = model.snapshot(4)
    for ts in range(5, 905):
        model.observe(ts, 10)
    assert first.midline_lag_state == MidlineLagState.WARNING
    blocked = model.snapshot(904)
    assert blocked.midline_lag_state == MidlineLagState.BLOCKED
    assert blocked.active_midline_bps > first.active_midline_bps
    assert blocked.midline_move_speed_bps_per_10m <= 1.25
