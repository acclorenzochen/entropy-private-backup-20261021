"""Covered terminal orders may shed risk while their cost facts recover."""
import asyncio
from dataclasses import replace
import time

from entropy_arb.execution_state import ExecutionPhase, ExecutionState
from tests.test_engine import make_cfg
from tests.test_execution_recovery import _engine


def _covered(tmp_path):
    cfg = make_cfg()
    cfg.trades_csv = str(tmp_path / "trades.csv")
    cfg.persistence = replace(cfg.persistence, migrate_legacy=False)
    cfg.recovery_timeout_sec = 0
    engine = _engine(cfg, tmp_path / "ledger.sqlite3")
    state = ExecutionState(
        cycle_id="covered-missing-cost", action="open", direction="buy_entropy",
        planned_qty=0.2, entropy_is_buy=True, hedge_is_buy=False,
        reduce_only=False, phase=ExecutionPhase.RECONCILING,
        first_position_before=0.0, hedge_position_before=0.0)
    engine.active_execution = state
    for leg, venue, is_buy in (("first", engine.entropy, True),
                               ("hedge", engine.hedge, False)):
        engine._observe_execution_result(
            state, leg, venue, is_buy=is_buy,
            info={"order_ref": leg, "status": "filled", "filled_base": 0.2,
                  "avg_px": None, "unresolved": False}, fallback_px=100.0)
    return engine, state


def test_terminal_covered_pair_exits_and_retains_accounting(tmp_path):
    engine, state = _covered(tmp_path)
    try:
        assert not asyncio.run(engine._recover_active_execution(immediate=True))
        assert engine._positions_flat()
        assert engine.active_execution is state
        assert all(order["reduce_only"] for venue in engine.venues.values()
                   for order in venue.sent)
        assert sum(len(v.sent) for v in engine.venues.values()) == 2
        # A repeated tick must neither duplicate exits nor replace the hedge.
        state.recovery_next_retry_ts = None
        assert not asyncio.run(engine._recover_active_execution(immediate=False))
        assert sum(len(v.sent) for v in engine.venues.values()) == 2
        assert engine._recovery_context["covered_recovery_exit"]
        engine._restored_execution = engine.ledger.load_active_execution()
        engine.active_execution = None
        engine._recovery_context = {}
        engine._restore_execution_after_reconcile()
        state = engine.active_execution
        assert engine._recovery_context["covered_recovery_exit"]
        # Late original fill facts repair accounting without resurrecting positions.
        for venue in engine.venues.values():
            venue.resolve_results = [{"status": "filled", "filled_base": 0.2,
                                      "avg_px": 100.0, "unresolved": False}]
        state.recovery_next_retry_ts = None
        assert asyncio.run(engine._recover_active_execution(immediate=False))
        assert engine.active_execution is None
        assert engine.last_execution_outcome == "flattened"
        assert engine._positions_flat()
    finally:
        engine.ledger.close()


def test_unknown_original_order_still_blocks_competing_exit(tmp_path):
    engine, state = _covered(tmp_path)
    try:
        state.set_pending("hedge", "hedge")
        assert not asyncio.run(engine._recover_active_execution(immediate=True))
        assert not engine._positions_flat()
        assert not engine.entropy.sent and not engine.hedge.sent
    finally:
        engine.ledger.close()


def test_margin_risk_bypasses_accounting_backoff(tmp_path):
    engine, state = _covered(tmp_path)
    try:
        engine.cfg.recovery_timeout_sec = 3600
        state.recovery_next_retry_ts = time.time() + 300
        engine._margin_snapshot = lambda: {"hard_risk": True}
        assert not asyncio.run(engine._recover_active_execution(immediate=False))
        assert engine._positions_flat()
        assert engine._recovery_context["covered_recovery_exit"] == "margin_risk"
    finally:
        engine.ledger.close()


def test_blocked_reconciliation_still_services_active_recovery(tmp_path):
    engine, state = _covered(tmp_path)
    try:
        engine.startup_reconciled = True
        engine.reconciliation["status"] = "blocked"
        asyncio.run(engine._evaluate())
        assert engine._positions_flat()
        assert engine.active_execution is state
    finally:
        engine.ledger.close()


def test_partial_risk_exit_never_reopens_the_closed_leg(tmp_path):
    engine, state = _covered(tmp_path)
    try:
        engine.hedge.next_results = [
            {"status": "canceled", "filled_base": 0.0, "avg_px": None,
             "unresolved": False}]
        assert not asyncio.run(engine._recover_active_execution(immediate=True))
        assert engine.entropy.position == 0.0
        assert engine.hedge.position == -0.2
        state.recovery_next_retry_ts = None
        assert not asyncio.run(engine._recover_active_execution(immediate=False))
        assert engine._positions_flat()
        assert len(engine.entropy.sent) == 1
        assert len(engine.hedge.sent) == 2
        assert all(order["reduce_only"] for venue in engine.venues.values()
                   for order in venue.sent)
    finally:
        engine.ledger.close()


def test_risk_release_precedes_network_accounting_lookups(tmp_path):
    engine, state = _covered(tmp_path)
    async def unavailable(*args, **kwargs):
        raise AssertionError('risk release must not wait on cost history')
    engine._recover_execution_cost_history = unavailable
    try:
        assert not asyncio.run(engine._recover_active_execution(immediate=True))
        assert engine._positions_flat()
        assert engine.active_execution is state
    finally:
        engine.ledger.close()
