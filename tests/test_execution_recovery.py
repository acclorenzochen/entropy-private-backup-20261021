import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from entropy_arb.engine import Engine  # noqa: E402
from entropy_arb.execution_state import (  # noqa: E402
    ExecutionPhase, ExecutionState, POSITION_COVERED_UNANCHORED)
from tests.test_engine import StubVenue, make_cfg  # noqa: E402


def _engine(cfg, db):
    engine = Engine(cfg, ledger_path=str(db))
    engine.entropy = StubVenue("entropy", "ENTROPY")
    engine.hedge = StubVenue("hedge", "RH")
    engine.venues = {"entropy": engine.entropy, "hedge": engine.hedge}
    engine._step, engine._min_base, engine._min_notional = 1e-4, 1e-4, 10.0
    engine.entropy.set_book(98.0, 98.01)
    engine.hedge.set_book(99.99, 100.0)
    return engine


class _CrashBeforeConfirmation(StubVenue):
    """Model an accepted request whose process dies before the response."""

    async def send_taker(self, **kw):
        self.sent.append(kw)
        raise asyncio.CancelledError


def test_restart_resumes_hedge_without_resending_first_leg(tmp_path):
    cfg = make_cfg(midline=0, upper=2, lower=2)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    db = tmp_path / "ledger.sqlite3"
    first = _engine(cfg, db)
    try:
        candidate = first._build_candidates(1_700_000_000.0)[1]
        plan, _ = first._plan(candidate.buy, candidate.sell,
                               candidate.cap_notional, candidate.threshold_bps)
        trade = first._execution_bounds(candidate, plan)
        assert trade is not None
        first.hedge.next_results = [{
            "status": "timeout", "filled_base": 0.0, "avg_px": None,
            "err": None, "unresolved": True,
        }]
        first.hedge.resolve_results = [{
            "status": "timeout", "filled_base": 0.0, "avg_px": None,
            "err": None, "unresolved": True,
        }]
        asyncio.run(first._execute(trade))
        assert first.active_execution is not None
        assert len(first.entropy.sent) == 1
        persisted_entropy_position = first.entropy.position
        first.ledger.close()
    finally:
        # close() above is intentional: it models an unclean process exit
        # while leaving the durable execution row active.
        if getattr(first.ledger, "_conn", None) is not None:
            try:
                first.ledger.close()
            except Exception:
                pass

    restarted = _engine(cfg, db)
    try:
        restarted.entropy.position = persisted_entropy_position
        restarted.hedge.position = 0.0
        restarted.hedge.resolve_results = [{
            "status": "canceled", "filled_base": 0.0, "avg_px": None,
            "err": None, "unresolved": False,
        }]
        restarted._restore_execution_after_reconcile()
        assert restarted.active_execution is not None
        asyncio.run(restarted._recover_active_execution(immediate=True))
        assert restarted.active_execution is None
        assert restarted.entropy.sent == []
        assert len(restarted.hedge.sent) == 1
        assert abs(restarted.entropy.position + restarted.hedge.position) < 1e-9
        # The important invariant is the first leg's cumulative fill key: it
        # is not duplicated by startup reconciliation.
        count = restarted.ledger._conn.execute(
            "SELECT COUNT(*) FROM fills WHERE leg='first'"
        ).fetchone()[0]
        assert count == 1
        assert restarted.ledger.has_data("trade_attempts")
    finally:
        restarted.ledger.close()


def test_restart_resolves_first_leg_send_intent_without_duplicate(tmp_path):
    cfg = make_cfg(midline=0, upper=2, lower=2)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    db = tmp_path / "ledger.sqlite3"
    first = _engine(cfg, db)
    first.entropy = _CrashBeforeConfirmation("entropy", "ENTROPY")
    first.venues["entropy"] = first.entropy
    first.entropy.set_book(98.0, 98.01)
    try:
        candidate = first._build_candidates(1_700_000_000.0)[1]
        plan, _ = first._plan(candidate.buy, candidate.sell,
                               candidate.cap_notional, candidate.threshold_bps)
        trade = first._execution_bounds(candidate, plan)
        assert trade is not None
        try:
            asyncio.run(first._execute(trade))
        except asyncio.CancelledError:
            pass
        assert first.active_execution is not None
        state = first.active_execution
        assert state.pending_leg == "first"
        assert state.pending_order_ref == first.entropy.sent[0]["order_ref"]
        first.ledger.close()
    finally:
        if getattr(first.ledger, "_conn", None) is not None:
            try:
                first.ledger.close()
            except Exception:
                pass

    restarted = _engine(cfg, db)
    try:
        restarted.entropy.resolve_results = [{
            "status": "filled", "filled_base": state.planned_qty,
            "avg_px": 98.01, "err": None, "unresolved": False,
        }]
        restarted._restore_execution_after_reconcile()
        asyncio.run(restarted._recover_active_execution(immediate=True))
        assert restarted.active_execution is None
        assert len(restarted.entropy.sent) == 0
        assert len(restarted.hedge.sent) == 1
        assert abs(restarted.entropy.position + restarted.hedge.position) < 1e-9
    finally:
        restarted.ledger.close()


def test_recovery_does_not_touch_execution_while_send_is_in_flight(tmp_path):
    cfg = make_cfg(midline=0, upper=2, lower=2)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    engine = _engine(cfg, tmp_path / "ledger.sqlite3")
    engine.active_execution = ExecutionState(
        cycle_id="in-flight",
        action="open",
        direction="short_entropy",
        planned_qty=0.04,
        entropy_is_buy=False,
        hedge_is_buy=True,
        reduce_only=False,
        first_filled=0.04,
        pending_leg="first",
        pending_order_ref="123",
    )
    engine._execution_inflight = True
    try:
        assert asyncio.run(engine._recover_active_execution(immediate=True)) is False
        assert engine.entropy.resolved == []
        assert engine.hedge.resolved == []
    finally:
        engine.ledger.close()


def test_external_flatten_releases_stale_unpaired_execution_without_hedging(
        tmp_path):
    """A manual close must not cause recovery to submit a fresh hedge leg."""
    cfg = make_cfg(midline=0, upper=2, lower=2)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    engine = _engine(cfg, tmp_path / "ledger.sqlite3")
    state = ExecutionState(
        cycle_id="external-flat",
        action="open",
        direction="buy_entropy",
        planned_qty=0.0326,
        entropy_is_buy=True,
        hedge_is_buy=False,
        reduce_only=False,
        phase="reconciling",
        first_filled=0.0066,
        hedge_filled=0.0,
        first_position_before=-0.0007,
        hedge_position_before=0.0001,
    )
    engine.entropy.position = 0.0
    engine.hedge.position = 0.0
    engine.active_execution = state
    engine._external_flatten_seen_at[state.cycle_id] = (
        time.time() - engine.EXTERNAL_FLAT_CONFIRMATION_SEC - 0.1)

    try:
        assert asyncio.run(
            engine._recover_active_execution(immediate=True)) is True
        assert engine.active_execution is None
        assert engine.hedge.sent == []
        row = engine.ledger._conn.execute(
            "SELECT status,outcome FROM executions WHERE execution_id=?",
            (state.cycle_id,)).fetchone()
        assert tuple(row) == ("complete", "one_leg")
    finally:
        engine.ledger.close()


def test_position_reconciliation_resolves_unknown_hedge_without_resend(
        tmp_path):
    """Authoritative fill history settles the gap without a duplicate hedge."""
    cfg = make_cfg(midline=0, upper=2, lower=2)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    engine = _engine(cfg, tmp_path / "ledger.sqlite3")
    state = ExecutionState(
        cycle_id="position-reconciled",
        action="open",
        direction="buy_entropy",
        planned_qty=0.0388,
        entropy_is_buy=True,
        hedge_is_buy=False,
        reduce_only=False,
        phase=ExecutionPhase.RECONCILING,
        first_filled=0.0388,
        hedge_filled=0.0099,
        pending_leg="hedge",
        pending_order_ref="hedge-retry",
        first_order_ref="first-order",
        hedge_order_ref="hedge-retry",
        first_position_before=0.0,
        hedge_position_before=0.0,
    )
    engine.active_execution = state
    engine.entropy.position = 0.0388
    engine.hedge.position = -0.0099
    engine.ledger.record_fill(
        fill_key="first-position-reconciled", execution_id=state.cycle_id,
        leg="first", venue=engine.entropy.key, order_ref="first-order",
        ts=time.time(), side="buy", qty=0.0388, price=100.0,
        metadata={"price_source": "exchange_average"})
    engine.ledger.record_fill(
        fill_key="hedge-position-reconciled", execution_id=state.cycle_id,
        leg="hedge", venue=engine.hedge.key, order_ref="hedge-initial",
        ts=time.time(), side="sell", qty=0.0099, price=100.0,
        metadata={"price_source": "exchange_average"})

    async def entropy_position():
        return 0.0388

    async def hedge_position():
        # The exchange account is already fully hedged even though the
        # client-order resolver still reports the retry as unresolved.
        return -0.0388

    engine.entropy.fetch_position = entropy_position
    engine.hedge.fetch_position = hedge_position

    async def hedge_fills(_since, _until):
        return [{"trade_id": "hedge-retry-fill", "order_ref": "hedge-retry",
                 "ts": time.time(), "side": "sell", "qty": 0.0289,
                 "price": 100.0, "fee_usd": 0.0}]

    engine.hedge.fetch_fee_fills = hedge_fills
    engine._checkpoint_execution(state, event_type="created")
    engine.ledger.record_order(
        execution_id=state.cycle_id, leg="hedge", venue=engine.hedge.key,
        order_ref=state.pending_order_ref, side="sell",
        planned_qty=0.0289, limit_px=100.0, reduce_only=False,
        status="timeout", filled_qty=0.0, avg_px=None, unresolved=True)

    try:
        async def run():
            await engine._reconcile_positions(hedge=False)
            await engine._recover_active_execution(immediate=False)

        asyncio.run(run())
        assert engine.active_execution is None
        assert engine.hedge.sent == []
        row = engine.ledger._conn.execute(
            "SELECT status,outcome FROM executions "
            "WHERE execution_id=?", (state.cycle_id,)).fetchone()
        assert tuple(row) == ("complete", "paired")
        order = engine.ledger._conn.execute(
            "SELECT unresolved,filled_qty FROM orders "
            "WHERE execution_id=? AND leg='hedge'",
            (state.cycle_id,)).fetchone()
        assert order is not None
        assert order[0] == 0
        assert abs(order[1] - 0.0289) < 1e-9
    finally:
        engine.ledger.close()


def test_position_coverage_suppresses_unknown_hedge_duplicate(tmp_path):
    cfg = make_cfg(midline=0, upper=2, lower=2)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    engine = _engine(cfg, tmp_path / "ledger.sqlite3")
    state = ExecutionState(
        cycle_id="covered-unknown", action="open",
        direction="buy_entropy", planned_qty=0.04,
        entropy_is_buy=True, hedge_is_buy=False, reduce_only=False,
        phase=ExecutionPhase.RECONCILING, first_filled=0.04,
        hedge_filled=0.0, pending_order_ref="unknown-hedge",
        first_position_before=0.0, hedge_position_before=0.0)
    engine.entropy.position = 0.04
    engine.hedge.position = -0.04

    try:
        engine._update_hedge_completion_evidence(
            state, order_info={"unresolved": True},
            reason="hedge_order_unresolved")
        assert state.hedge_completion_state == POSITION_COVERED_UNANCHORED
        assert state.hedge_filled == 0.04
        assert asyncio.run(engine._send_hedge_remainder(state)) is False
        assert engine.hedge.sent == []
    finally:
        engine.ledger.close()


def test_flat_execution_does_not_start_emergency_order_campaign(tmp_path):
    cfg = make_cfg(midline=0, upper=2, lower=2)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    engine = _engine(cfg, tmp_path / "ledger.sqlite3")
    state = ExecutionState(
        cycle_id="already-flat", action="open", direction="buy_entropy",
        planned_qty=0.04, entropy_is_buy=True, hedge_is_buy=False,
        reduce_only=False, first_filled=0.04, hedge_filled=0.04)
    engine.active_execution = state

    try:
        assert asyncio.run(
            engine._emergency_exit_positions("stale_recovery")) is True
        assert engine.entropy.sent == []
        assert engine.hedge.sent == []
        assert engine._emergency_campaign is None
        assert state.hedge_completion_state == "flat"
    finally:
        engine.ledger.close()


def test_unknown_cost_timeout_uses_reduce_only_emergency_exit(tmp_path):
    """Unknown cost may block profit judgment, but not risk flattening."""
    cfg = make_cfg(midline=0, upper=2, lower=2)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    cfg.recovery_timeout_sec = 0.0
    engine = _engine(cfg, tmp_path / "ledger.sqlite3")
    state = ExecutionState(
        cycle_id="orphan-emergency", action="open",
        direction="buy_entropy", planned_qty=0.2,
        entropy_is_buy=True, hedge_is_buy=False, reduce_only=False,
        phase=ExecutionPhase.RECONCILING,
        first_filled=0.2, hedge_filled=0.2)
    engine.active_execution = state
    engine.entropy.position = 0.2
    engine.hedge.position = -0.2

    try:
        assert asyncio.run(
            engine._recover_active_execution(immediate=True)) is True
        assert engine.active_execution is None
        assert engine.entropy.position == 0.0
        assert engine.hedge.position == 0.0
        assert len(engine.entropy.sent) == 1
        assert len(engine.hedge.sent) == 1
        assert all(order["reduce_only"] for order in (
            engine.entropy.sent + engine.hedge.sent))
        row = engine.ledger._conn.execute(
            "SELECT status,outcome FROM executions "
            "WHERE execution_id=?", (state.cycle_id,)).fetchone()
        assert tuple(row) == ("complete", "flattened")
    finally:
        engine.ledger.close()


def test_partial_first_fill_aggregates_before_hedge(tmp_path):
    """A sub-minimum first fill must not strand the execution forever."""
    cfg = make_cfg(midline=0, upper=2, lower=2)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    engine = _engine(cfg, tmp_path / "ledger.sqlite3")
    engine.entropy.set_book(1499.0, 1500.0)
    engine.hedge.set_book(1499.0, 1500.0)
    state = ExecutionState(
        cycle_id="partial-first-fill", action="open",
        direction="buy_entropy", planned_qty=0.04,
        entropy_is_buy=True, hedge_is_buy=False, reduce_only=False,
        phase=ExecutionPhase.HEDGE_LEG,
        first_filled=0.003, hedge_filled=0.0,
        first_order_ref="initial-first", first_order_status="filled",
        first_avg_px=1500.0, first_position_before=0.0,
        hedge_position_before=0.0)
    engine.active_execution = state
    engine.entropy.position = 0.003
    engine.hedge.position = 0.0
    engine.ledger.record_fill(
        fill_key="partial-first", execution_id=state.cycle_id,
        leg="first", venue=engine.entropy.key, order_ref="initial-first",
        ts=time.time(), side="buy", qty=0.003, price=1500.0,
        metadata={"price_source": "exchange_average"})

    try:
        assert asyncio.run(
            engine._recover_active_execution(immediate=True)) is True
        assert engine.active_execution is None
        assert len(engine.entropy.sent) == 1
        assert engine.entropy.sent[0]["qty"] * engine.entropy.sent[0]["limit_px"] >= 10
        assert len(engine.hedge.sent) == 1
        assert abs(engine.entropy.position + engine.hedge.position) < 1e-9
        assert engine.entropy.sent[0]["reduce_only"] is False
        assert engine.hedge.sent[0]["qty"] > 0.003
        row = engine.ledger._conn.execute(
            "SELECT state_json FROM executions WHERE execution_id=?",
            (state.cycle_id,)).fetchone()
        assert json.loads(row[0])["first_topup_count"] == 1
    finally:
        engine.ledger.close()
