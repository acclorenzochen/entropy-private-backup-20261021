import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.signal import (compute_adaptive_entry_band,
                                is_volatility_guarded)


def band(sigma, *, base=5.0, configured_min=5.0, mult=2.0, maximum=15.0):
    return compute_adaptive_entry_band(
        base_bps=base, configured_min_bps=configured_min,
        robust_sigma_bps=sigma, sigma_mult=mult, max_bps=maximum)


def test_adaptive_band_keeps_the_configured_floor():
    assert band(None) == 5.0
    assert band(1.0) == 5.0
    assert band(2.0) == 5.0


def test_adaptive_band_tracks_sigma_and_caps_at_maximum():
    assert band(4.0) == 8.0
    assert band(20.0) == 15.0
    assert band(4.0, base=9.0, configured_min=5.0) == 8.0


def test_adaptive_band_handles_nonfinite_sigma_without_lowering_base():
    assert band(math.nan) == 5.0
    assert band(math.inf) == 5.0


def test_volatility_guard_is_strictly_above_limit():
    assert not is_volatility_guarded(None, 8.0)
    assert not is_volatility_guarded(8.0, 8.0)
    assert is_volatility_guarded(8.01, 8.0)
