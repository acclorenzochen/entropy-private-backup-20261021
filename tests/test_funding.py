import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.funding import (FundingSnapshot,
                                 expected_pair_funding_cost_bps, fresh_rate)


def test_funding_direction_signs():
    short = expected_pair_funding_cost_bps(2, 1, 2, short_entropy=True)
    long = expected_pair_funding_cost_bps(2, 1, 2, short_entropy=False)
    assert short == -2
    assert long == 2


def test_stale_snapshot_is_unavailable():
    snap = FundingSnapshot(1.5, 100, "test")
    assert fresh_rate(snap, 110, 20) == 1.5
    assert fresh_rate(snap, 121, 20) is None
