import csv
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.recorder import SecondRecorder  # noqa: E402
from entropy_arb.signal import (PremiumDataHealth, PremiumSampleQuality,
                                RollingPremiumModel,
                                check_premium_sample_quality)  # noqa: E402


def test_fresh_books_with_small_skew_are_valid():
    quality = check_premium_sample_quality(
        100.0, 99.8, 99.9, 1200, 500)
    assert quality.valid
    assert abs(quality.entropy_age_ms - 200) < 1e-9
    assert abs(quality.hedge_age_ms - 100) < 1e-9
    assert abs(quality.cross_venue_skew_ms - 100) < 1e-9
    assert quality.reason is None


def test_stale_and_skew_reasons_are_specific():
    assert check_premium_sample_quality(
        100.0, 97.5, 99.9, 1200, 500).reason == "entropy_book_stale"
    assert check_premium_sample_quality(
        100.0, 99.9, 97.5, 1200, 500).reason == "hedge_book_stale"
    assert check_premium_sample_quality(
        100.0, 98.0, 99.9, 1200, 500).reason == "cross_venue_skew"


def test_data_health_degrades_only_after_continuous_invalid_period():
    health = PremiumDataHealth(5)
    invalid = PremiumSampleQuality(False, 1500, 100, 1400,
                                   "entropy_book_stale")
    valid = PremiumSampleQuality(True, 100, 100, 0, None)
    health.observe(10, invalid)
    assert not health.is_degraded(14.99)
    assert health.is_degraded(15)
    health.observe(16, valid)
    assert not health.is_degraded(100)


def test_invalid_sample_is_not_added_to_midline_or_sigma_windows():
    model = RollingPremiumModel(
        short_window_sec=100, long_window_sec=300, warmup_sec=0,
        min_samples=1, shift_min_bps=3, shift_sigma_mult=2,
        recovery_ratio=.6, recovery_sec=3, seed_midline_bps=0,
        sigma_window_sec=100)
    valid = PremiumSampleQuality(True, 100, 100, 0, None)
    invalid = PremiumSampleQuality(False, 1500, 100, 1400,
                                   "entropy_book_stale")
    for ts, premium, quality in (
            (0, 1, valid), (1, 1, valid), (2, 100, invalid), (3, 1, valid)):
        if quality.valid:
            model.observe(ts, premium)
    snapshot = model.snapshot(3)
    assert snapshot.sample_count == 3
    assert snapshot.sigma_sample_count == 3
    assert snapshot.long_center_bps == 1
    assert snapshot.sigma_center_bps == 1


class _Venue:
    def __init__(self, book):
        self.book = book
        self.funding = None


def _book(bid: float, ask: float) -> OrderBook:
    book = OrderBook()
    book.apply_hl([[{"px": str(bid), "sz": "10"}],
                   [{"px": str(ask), "sz": "10"}]])
    return book


def test_second_recorder_keeps_raw_stale_books_but_rejects_sample():
    entropy, hedge = _book(100, 100.02), _book(99.99, 100.01)
    entropy.last_update_ts = 97.5
    hedge.last_update_ts = 99.9
    path = os.path.join(tempfile.mkdtemp(), "seconds.csv")
    recorder = SecondRecorder(
        path, _Venue(entropy), _Venue(hedge), 10,
        lambda now: type("Snapshot", (), {
            "active_midline_bps": 0.0,
            "short_center_bps": None,
            "long_center_bps": None,
            "robust_sigma_bps": None,
            "regime_shift": False,
            "midline_lag_bps": None,
            "midline_lag_state": "normal",
            "midline_move_speed_bps_per_10m": 0.5,
            "sigma_center_bps": None,
            "sigma_window_sec": 1800,
        })(),
        max_book_age_ms=1200, max_cross_venue_skew_ms=500)
    assert recorder.sample(100.0)
    recorder.close()
    with open(path, newline="") as fh:
        row = next(csv.DictReader(fh))
    assert row["premium_sample_valid"] == "false"
    assert row["premium_sample_reject_reason"] == "entropy_book_stale"
    assert row["entropy_bid"] == "100.0"
