"""Offline regressions for Aug 31 zero-fill incidents; no live adapters."""
import asyncio
import json
import math
import time
from dataclasses import replace

import pytest

from entropy_arb.signal import StrategyAction
from entropy_arb.execution_state import ExecutionState
from entropy_arb.trade_ledger import TradeLedger
from entropy_arb.venue_hl import HLVenue
from test_profit_guard import engine, open_lot, candidate_plan


@pytest.mark.parametrize("direction", ["buy_entropy", "sell_entropy"])
def test_close_limit_spends_positive_exit_budget_with_hard_cap(direction):
    eng = engine()
    eng.entropy.fee_bps = .9
    eng.hedge.fee_bps = .2
    eng.entropy.px_round = lambda px, round_up: (
        math.ceil(px * 100) if round_up else math.floor(px * 100)) / 100
    short = direction == "buy_entropy"
    open_lot(eng, "entropy", "sell" if short else "buy", 1, 101 if short else 99, .01)
    open_lot(eng, "hedge", "buy" if short else "sell", 1, 100, .002)
    c, p = candidate_plan(eng, direction, StrategyAction.REDUCE)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    bound = trade.buy_bound if short else trade.sell_bound
    ref = p.buy_limit if short else p.sell_limit
    b = trade.cycle_budget
    actual_room = abs(bound / ref - 1) * 1e4
    assert b["exit_economic_budget_usd"] > 0
    assert b["exit_budget_bps"] > 0
    assert actual_room <= b["physical_hard_cap_bps"] + 0.1
    assert b["limit_net_profit_usd"] >= eng.cfg.profit_guard.min_profit_usd - 1e-9


def test_quote_profit_spends_only_remaining_budget_after_hedge_reserve():
    eng = engine()
    open_lot(eng, "entropy", "sell", 1, 100)
    open_lot(eng, "hedge", "buy", 1, 100)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    assert eng._cycle_profit_budget(c, p)["allowed"]
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    assert trade.cycle_budget["hedge_budget_consumed_usd"] > 0
    assert (trade.cycle_budget["limit_net_profit_usd"]
            >= eng.cfg.profit_guard.min_profit_usd - 1e-9)


def test_reduce_budget_reserves_one_recovery_slot():
    eng = engine()
    eng.entropy.orders_per_min = 6
    for _ in range(4):
        eng._record_send(eng.entropy)
    assert eng._venue_rate_ok(eng.entropy, StrategyAction.REDUCE)
    eng._record_send(eng.entropy)
    assert not eng._venue_rate_ok(eng.entropy, StrategyAction.REDUCE)


def test_reduce_retry_campaign_respects_reserved_budget():
    eng = engine()
    eng.entropy.orders_per_min = 6
    eng.cfg.action_specific_execution = True
    eng.cfg.max_reduce_retry_attempts = 4
    eng.cfg.reduce_no_fill_retry_sec = 0
    eng.cfg.reduce_retry_escalation_bps = (4.0, 6.0, 8.0, 12.0)
    open_lot(eng, "entropy", "buy", 1, 99)
    open_lot(eng, "hedge", "sell", 1, 100)
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.REDUCE)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    eng.entropy.next_results = [
        {"status": "canceled", "filled_base": 0,
         "avg_px": None, "err": None, "unresolved": False}
        for _ in range(4)]
    for _ in range(3):
        eng._record_send(eng.entropy)

    asyncio.run(eng._execute(trade))

    assert len(eng.entropy.sent) == 2
    assert len(eng._sends["entropy"]) == 5


def test_heartbeat_cannot_make_old_quote_executable():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    for v in eng.venues.values():
        v.book.last_update_ts = time.time() - 60
        v.book.touch()
    assert eng._cycle_profit_budget(c, p)["reason"] == "stale_or_skewed_books"


def test_tiny_real_fill_is_not_no_fill_or_paired():
    eng = engine()
    eng.cfg.net_tolerance_base = .001
    state = ExecutionState("tiny", "open", "buy_entropy", .05, True, False, False)
    eng.active_execution = state
    eng._observe_execution_result(state, "first", eng.entropy, is_buy=True,
        info={"order_ref": "first", "status": "filled", "filled_base": .0006,
              "avg_px": 100, "unresolved": False}, fallback_px=100)
    asyncio.run(eng._recover_active_execution(immediate=True))
    assert eng.last_execution_outcome == "one_leg"
    assert eng.entropy.position == pytest.approx(.0006)
    assert eng.hedge.sent == []  # below venue minimum; never over-hedge dust
    assert eng.ledger.list_trades()["summary"]["one_leg_count"] == 1


def test_terminal_zero_fill_is_not_sent_and_rearms_only_after_signal_reset():
    eng = engine()
    eng.cfg.profit_guard = replace(eng.cfg.profit_guard, enabled=False)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(100, 100.01)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    eng.entropy.next_result = {"status": "canceled", "filled_base": 0,
                              "err": None, "unresolved": False}
    eng._armed["open:buy_entropy"] = time.time() - 10
    async def run():
        await eng._execute(trade)
        assert eng._scan(time.time()) is None
        # Refreshing the same sustained signal is not a new opportunity: the
        # final stabilization policy allows at most the bounded campaign.
        eng.entropy.set_book(98, 98.01)
        eng.hedge.set_book(100, 100.01)
        assert eng._scan(time.time()) is None  # fresh persistence gate
        assert eng._scan(time.time()) is None  # opportunity remains locked
        eng.entropy.set_book(100, 100.01)
        eng.hedge.set_book(100, 100.01)
        eng._mark_open_signal_reset(eng._open_opportunity_key(c), c,
                                    time.time())
        eng.entropy.set_book(98, 98.01)
        eng.hedge.set_book(100, 100.01)
        assert eng._scan(time.time()) is None  # fresh persistence gate
        assert eng._scan(time.time()) is not None
    asyncio.run(run())
    assert not eng.hedge.sent
    row = eng.ledger._conn.execute("SELECT sell_status FROM trade_attempts").fetchone()
    assert row[0] == "not-sent"


def test_ioc_reason_survives_without_counting_as_transport_error():
    reason = "Order could not immediately match against any resting orders."
    info = HLVenue._parse({"status": "ok", "response": {
        "data": {"statuses": [{"error": reason}]}}})
    assert info["err"] is None and not info["unresolved"]
    assert info["exchange_reason"] == reason
    eng = engine()
    state = ExecutionState("ioc", "open", "buy_entropy", 1, True, False, False)
    eng._observe_execution_result(state, "first", eng.entropy,
        is_buy=True, info=info, fallback_px=100)
    payload = eng.ledger._conn.execute(
        "SELECT payload_json FROM execution_events ORDER BY id DESC LIMIT 1").fetchone()[0]
    assert json.loads(payload)["exchange_reason"] == reason


def test_legacy_completion_time_and_historical_tiny_fill(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    state = ExecutionState("live", "open", "buy_entropy", .05, True, False, False)
    state.created_ts = 100
    ledger.checkpoint_execution(state)
    ledger._conn.execute("UPDATE executions SET final_ts=104.086 WHERE execution_id='live'")
    ledger._conn.commit()
    base = dict(trade_key="live", execution_id="live", ts=100, action="open",
                direction="buy_entropy", qty=.05, buy_fill=.0006, sell_fill=0,
                buy_notional=5, sell_notional=5, buy_venue="entropy", sell_venue="hedge",
                outcome="no_fill")
    ledger.record_trade_attempt(base)
    legacy = dict(base, trade_key="legacy:copy", execution_id="legacy:copy",
                  ts=104.086, outcome="one_leg", source="legacy_csv")
    ledger.record_trade_attempt(legacy, source="legacy_csv")
    assert ledger._has_live_trade_copy(legacy)
    result = ledger.list_trades()
    assert result["total_count"] == 1
    assert result["summary"]["one_leg_count"] == 1
    assert result["trades"][0]["outcome"] == "one_leg"
    assert ledger.activity_summary(since=0, until=200)["one_leg_count"] == 1
    # A different live execution must not disappear, even with the same shape.
    ledger.record_trade_attempt(dict(base, trade_key="live2", execution_id="live2", ts=104.1))
    assert ledger.list_trades()["total_count"] == 2
    ledger.close()


def test_send_exception_remains_pending_and_resolves_original_order():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)

    async def lost_response(**kw):
        eng.entropy.sent.append(kw)
        raise OSError("response lost after request may have arrived")

    eng.entropy.send_taker = lost_response
    asyncio.run(eng._execute(trade))
    assert eng.active_execution is not None
    assert eng.active_execution.pending_leg == "first"
    ref = eng.entropy.sent[0]["order_ref"]
    assert eng.active_execution.pending_order_ref == ref
    eng.entropy.resolve_results = [{"status": "canceled", "filled_base": 0,
                                   "err": None, "unresolved": False}]
    asyncio.run(eng._recover_active_execution(immediate=True))
    assert eng.active_execution is None
    assert len(eng.entropy.sent) == 1 and eng.hedge.sent == []
    assert eng.entropy.resolved[-1] == ref


def test_tiny_but_hedgeable_fill_still_sends_hedge():
    eng = engine()
    eng.cfg.net_tolerance_base = .001
    eng.cfg.min_order_notional = 0
    eng.hedge.min_quote = 0
    state = ExecutionState("tiny-hedge", "open", "buy_entropy", .05, True, False, False)
    eng.active_execution = state
    eng._observe_execution_result(state, "first", eng.entropy, is_buy=True,
        info={"order_ref": "first", "status": "filled", "filled_base": .0006,
              "avg_px": 100, "unresolved": False}, fallback_px=100)
    asyncio.run(eng._recover_active_execution(immediate=True))
    assert len(eng.hedge.sent) == 1
    assert eng.hedge.sent[0]["qty"] == pytest.approx(.0006)
    assert abs(eng.entropy.position + eng.hedge.position) < 1e-8


@pytest.mark.parametrize("qty", [.2, .5, 1])
def test_partial_close_budget_uses_only_its_allocated_costs(qty):
    eng = engine()
    eng.entropy.fee_bps = .9
    eng.cfg.profit_guard = replace(eng.cfg.profit_guard, exit_buffer_bps=3)
    open_lot(eng, "entropy", "sell", 1, 101, .01)
    open_lot(eng, "hedge", "buy", 1, 100)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE, qty)
    t = eng._execution_bounds(c, p)
    assert t is not None
    assert t.cycle_budget["entry_cash_usd"] == pytest.approx(qty)
    assert t.cycle_budget["entry_fees_usd"] == pytest.approx(.01 * qty)
    assert t.cycle_budget["limit_net_profit_usd"] >= .01 - 1e-9
    # Reprice immediately before send; do not use a now-unprofitable limit.
    eng.entropy.set_book(102, 102.01)
    asyncio.run(eng._execute(t))
    assert not eng.entropy.sent and not eng.hedge.sent


def test_legacy_partial_pair_label_does_not_duplicate_same_fills(tmp_path):
    ledger = TradeLedger(tmp_path / "partial.sqlite3")
    base = dict(trade_key="partial", execution_id="partial", ts=100,
                action="reduce", direction="sell_entropy", qty=1,
                buy_fill=.4, sell_fill=.4, buy_notional=100, sell_notional=100,
                buy_venue="hedge", sell_venue="entropy", outcome="paired")
    ledger.record_trade_attempt(base)
    ledger.record_trade_attempt(dict(base, trade_key="legacy:partial",
        execution_id="legacy:partial", ts=100.5, outcome="partial_paired"),
        source="legacy_csv")
    assert ledger.list_trades()["total_count"] == 1
    assert ledger.list_trades()["summary"]["partial_paired_count"] == 1
    ledger.close()
