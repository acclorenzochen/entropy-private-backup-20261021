import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.signal import RollingPremiumModel  # noqa: E402


def test_sigma_uses_local_window_not_long_center_window():
    model = RollingPremiumModel(
        short_window_sec=1800, long_window_sec=10800, warmup_sec=0,
        min_samples=1, shift_min_bps=3, shift_sigma_mult=2,
        recovery_ratio=.6, recovery_sec=3, seed_midline_bps=-7,
        base_move_bps_per_10m=.5, accelerated_move_bps_per_10m=1,
        absolute_max_move_bps_per_10m=1.25, sigma_window_sec=1800)
    for ts in range(0, 9000, 100):
        model.observe(ts, -7)
    for ts, value in ((9000, -4), (9300, -3), (9600, -2),
                      (9900, -3), (10200, -4), (10500, -3)):
        model.observe(ts, value)
    snapshot = model.snapshot(10800)
    assert snapshot.long_center_bps == -7
    assert snapshot.sigma_center_bps == -3
    assert snapshot.sigma_sample_count == 6
    assert snapshot.robust_sigma_bps < 1.6
