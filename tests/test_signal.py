import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.signal import (PairState, RollingPremiumModel,
                                entry_threshold_long, pair_state)


def model(**kw):
    defaults = dict(short_window_sec=4, long_window_sec=20, warmup_sec=4,
                    min_samples=5, shift_min_bps=3, shift_sigma_mult=2,
                    max_move_bps_per_10m=0.5, recovery_ratio=0.6,
                    recovery_sec=3, seed_midline_bps=0)
    defaults.update(kw)
    return RollingPremiumModel(**defaults)


def test_pair_state_uses_both_real_positions():
    assert pair_state(0, 0, .01) == PairState.FLAT
    assert pair_state(-2, 2, .01) == PairState.SHORT_ENTROPY
    assert pair_state(2, -2, .01) == PairState.LONG_ENTROPY
    assert pair_state(2, 2, .01) == PairState.UNBALANCED


def test_robust_median_mad_and_warmup():
    m = model(warmup_sec=4, min_samples=5)
    for ts, value in enumerate([1, 1, 1, 2, 100]):
        m.observe(ts, value)
    s = m.snapshot(4)
    assert s.ready and s.long_center_bps == 1
    assert s.mad_bps == 0 and s.robust_sigma_bps == 0


def test_warmup_falls_back_to_seed():
    m = model(seed_midline_bps=7, min_samples=10)
    m.observe(0, 50)
    s = m.snapshot(1)
    assert not s.ready and s.active_midline_bps == 7


def test_merge_history_rehydrates_rows_older_than_checkpoint_tail():
    m = model(long_window_sec=1000, warmup_sec=180, min_samples=3)
    m.observe(200, 2)
    m.observe(300, 2)
    assert not m.snapshot(300).ready

    added = m.merge_history(((100, 1), (150, 1), (250, 2)), now=300)

    assert added == 3
    snapshot = m.snapshot(300)
    assert snapshot.ready
    assert snapshot.sample_count == 5
    assert snapshot.long_center_bps == 2


def test_active_midline_rate_limit():
    m = model(max_move_bps_per_10m=0.5, long_window_sec=1000)
    for ts in range(5):
        m.observe(ts, 20)
    m.snapshot(4)
    s = m.snapshot(604)
    assert abs(s.active_midline_bps - 0.5) < 1e-9


def test_regime_entry_and_hysteretic_recovery():
    m = model(long_window_sec=40, short_window_sec=4, recovery_sec=3)
    for ts in range(20):
        m.observe(ts, 0)
    for ts in range(20, 25):
        m.observe(ts, 10)
    assert m.snapshot(24).regime_shift
    for ts in range(25, 30):
        m.observe(ts, 0)
    assert m.snapshot(29).regime_shift
    assert m.snapshot(31).regime_shift
    assert not m.snapshot(32).regime_shift


def test_favorable_funding_never_lowers_entry():
    assert entry_threshold_long(0, 5, funding_cost_bps=-20) == 5
