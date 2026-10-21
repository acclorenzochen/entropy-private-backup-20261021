import asyncio
import os
import sys
import tempfile
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.cycle_state import load_cycle_state, write_cycle_state  # noqa: E402
from entropy_arb.config import AdaptiveBandsConfig, CycleExitConfig  # noqa: E402
from entropy_arb.engine import Engine  # noqa: E402
from entropy_arb.signal import (InventoryCycle, PairState, StrategyAction,
                                compute_cycle_midline_drift,
                                is_cycle_add_blocked)  # noqa: E402

from test_engine import make_engine  # noqa: E402


def test_cycle_state_round_trips_and_partial_reduces_do_not_reset():
    cycle = InventoryCycle()
    cycle.start(direction="short_entropy", opened_ts=10,
                entry_midline_bps=2, entry_sigma_bps=1.5,
                entry_band_bps=5, entry_threshold_bps=7,
                paired_qty=.04)
    cycle.reduce_count = 1
    path = os.path.join(tempfile.mkdtemp(), "cycle.json")
    write_cycle_state(path, cycle)
    loaded = load_cycle_state(path)
    assert loaded.active and loaded.anchored
    assert loaded.direction == "short_entropy"
    assert loaded.reduce_count == 1
    loaded.reset()
    assert not loaded.active and loaded.entry_midline_bps is None


def test_cycle_drift_is_signed_and_only_exceeds_limit_blocks_add():
    assert compute_cycle_midline_drift(7, 2) == 5
    assert not is_cycle_add_blocked(4, 4)
    assert is_cycle_add_blocked(-4.01, 4)


def test_engine_uses_entry_anchor_for_reduce_and_blocks_add_on_drift():
    eng = make_engine(midline=0, upper=4, lower=4)
    eng.cfg.dynamic_midline_enabled = True
    eng.cfg.cycle_exit = CycleExitConfig(
        enabled=True, anchor_mode="entry_midline", max_midline_drift_bps=4,
        reset_on_flat=True,
        state_json=os.path.join(tempfile.mkdtemp(), "cycle.json"))
    eng.cycle.start(direction="short_entropy", opened_ts=1,
                    entry_midline_bps=0, entry_sigma_bps=1,
                    entry_band_bps=4, entry_threshold_bps=4,
                    paired_qty=1)
    eng.entropy.set_book(100.19, 100.20)
    eng.hedge.set_book(99.99, 100.00)
    shifted = replace(eng.model_snapshot, active_midline_bps=6,
                      robust_sigma_bps=1, regime_shift=False)
    eng._premium_snapshot = lambda now: shifted
    eng.entropy.position, eng.hedge.position = -1, 1
    candidates = eng._build_candidates(100)
    assert [candidate.action.value for candidate in candidates] == ["reduce"]
    assert candidates[0].threshold_bps == -eng.cfg.exit_bps
    assert candidates[0].cycle_drift_guard
    assert candidates[0].cycle_midline_drift_bps == 6


def test_paired_open_persists_cycle_entry_context():
    eng = make_engine(midline=0, upper=4, lower=4)
    state_path = os.path.join(tempfile.mkdtemp(), "cycle.json")
    eng.cfg.cycle_exit = CycleExitConfig(
        enabled=True, anchor_mode="entry_midline", max_midline_drift_bps=4,
        reset_on_flat=True, state_json=state_path)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    candidate = eng._build_candidates(100)[0]
    assert candidate.action == StrategyAction.OPEN
    plan, _ = eng._plan(candidate.buy, candidate.sell,
                         candidate.cap_notional, candidate.threshold_bps)
    trade = eng._execution_bounds(candidate, plan)
    assert trade is not None
    asyncio.run(eng._execute(trade))
    assert eng.cycle.active and eng.cycle.anchored
    assert eng.cycle.direction == "short_entropy"
    assert eng.cycle.entry_midline_bps == 0
    assert eng.cycle.entry_band_bps == 4
    assert os.path.exists(state_path)


def test_restart_with_unanchored_inventory_allows_reduce_only():
    eng = make_engine(midline=0, upper=4, lower=4)
    eng.cfg.cycle_exit = CycleExitConfig(
        enabled=True, anchor_mode="entry_midline", max_midline_drift_bps=4,
        reset_on_flat=True,
        state_json=os.path.join(tempfile.mkdtemp(), "cycle.json"))
    eng.cycle = InventoryCycle(active=True, anchored=False,
                               direction="short_entropy")
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    eng.entropy.position, eng.hedge.position = -1, 1
    candidates = eng._build_candidates(100)
    assert [candidate.action for candidate in candidates] == [StrategyAction.REDUCE]
    assert candidates[0].cycle_entry_midline_bps is None


def test_engine_volatility_guard_keeps_reduce_available():
    eng = make_engine(midline=0, upper=4, lower=4)
    eng.cfg.adaptive_bands = replace(
        eng.cfg.adaptive_bands, enabled=True, max_sigma_bps=1)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    eng.entropy.position, eng.hedge.position = -1, 1
    shifted = replace(eng.model_snapshot, robust_sigma_bps=2,
                      regime_shift=False)
    eng._premium_snapshot = lambda now: shifted
    candidates = eng._build_candidates(100)
    reduce = next(c for c in candidates
                  if c.action.value == "reduce")
    assert reduce.volatility_guard
