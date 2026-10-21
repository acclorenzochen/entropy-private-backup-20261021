"""Offline whole-cycle replay. Never constructs a live exchange adapter."""
import asyncio
import time
from dataclasses import replace

import pytest

from entropy_arb.book import ArbPlan
from entropy_arb.config import ProfitGuardConfig, ConfigError, load_config, merge_hot_reloadable_config
from entropy_arb.execution_state import ExecutionState
from entropy_arb.signal import MidlineLagState, StrategyAction
from entropy_arb.trade_ledger import TradeLedger
from test_engine import make_engine, NO_ENV


def engine(midline=-20):
    eng = make_engine(midline=midline, upper=4, lower=4)
    eng.cfg.profit_guard = ProfitGuardConfig(True, .01, 0, 0)
    eng.cfg.funding_enabled = False
    eng.entropy.set_book(99.94, 99.95)
    eng.hedge.set_book(99.99, 100)
    return eng


def open_lot(eng, venue, side, qty, price, fee=0, ts=None, suffix="open"):
    ts = ts if ts is not None else time.time() - 10
    eng.ledger.record_fill(
        fill_key=f"{venue}:{suffix}", execution_id=suffix, leg=venue,
        venue=venue, order_ref=f"{venue}:{suffix}", ts=ts,
        side=side, qty=qty, price=price, fee_usd=fee,
        fee_source="exchange_api" if venue == "entropy" else "venue_zero_fee",
        metadata={"price_source": "exchange_average"})
    eng.venues[venue].position += qty if side == "buy" else -qty


def candidate_plan(eng, direction, action, qty=1):
    c = next(c for c in eng._build_candidates(time.time())
             if c.direction == direction and c.action == action)
    b = eng._depth_quote(c.buy, True, qty)
    s = eng._depth_quote(c.sell, False, qty)
    p = ArbPlan(qty, b[0], s[0], b[1], s[1], qty, b[1],
                (s[0] / b[0] - 1) * 1e4, (s[0] / b[0] - 1) * 1e4,
                c.buy.fee_bps / 1e4, c.sell.fee_bps / 1e4)
    return c, p


def test_negative_open_edge_can_have_positive_round_trip():
    eng = engine()
    eng.entropy.fee_bps = .9
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    assert p.exp_edge_usd < 0
    t = eng._execution_bounds(c, p)
    assert t is not None
    # Entry-only edge is negative, but the reverse-book projection remains
    # positive after the new expected execution-cost reserve and safety
    # margin are charged.
    assert t.cycle_budget["net_profit_usd"] > 0
    assert t.cycle_budget["net_profit_usd"] >= eng.cfg.economic_execution.min_expected_cycle_profit_usd
    # Costs include entry AND projected exit fees, plus reverse book spread.
    assert t.cycle_budget["entry_fees_usd"] > 0
    assert t.cycle_budget["exit_fees_usd"] > 0


def test_fees_and_both_execution_buffers_block_apparent_edge():
    eng = engine()
    eng.cfg.profit_guard = replace(eng.cfg.profit_guard,
                                  entry_buffer_bps=8.5, exit_buffer_bps=8.5)
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    budget = eng._cycle_profit_budget(c, p)
    assert budget["execution_buffer_usd"] == pytest.approx(.17)
    assert not budget["allowed"]
    assert eng._execution_bounds(c, p) is None


@pytest.mark.parametrize("direction", ["buy_entropy", "sell_entropy"])
def test_profitable_normal_exit_does_not_wait_for_midline(direction):
    eng = engine(midline=-20 if direction == "buy_entropy" else 20)
    if direction == "buy_entropy":
        # Leave enough actual profit to reserve the hedge's protected price.
        open_lot(eng, "entropy", "sell", 1, 100.2)
        open_lot(eng, "hedge", "buy", 1, 100)
    else:
        open_lot(eng, "entropy", "buy", 1, 99)
        open_lot(eng, "hedge", "sell", 1, 100)
    c, p = candidate_plan(eng, direction, StrategyAction.REDUCE)
    t = eng._execution_bounds(c, p)
    assert t is not None and t.cycle_budget["allowed"]
    assert t.net_marginal_edge_bps < c.threshold_bps  # legacy would block

    async def scan():
        now = time.time()
        eng._scan(now)
        return eng._scan(now + .2)

    selected = asyncio.run(scan())
    assert selected is not None
    assert selected.candidate.action == StrategyAction.REDUCE


def test_real_aug31_losing_close_is_rejected_even_with_zero_buffer():
    eng = engine(midline=-5.45082987799)
    eng.entropy.fee_bps = .9
    open_lot(eng, "entropy", "sell", .1111, 1530, .015298)
    open_lot(eng, "hedge", "buy", .1111, 1530.75)
    eng.entropy.set_book(1524.2, 1524.3)
    eng.hedge.set_book(1524.92, 1525)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE, .111)
    b = eng._cycle_profit_budget(c, p)
    assert b["entry_cash_usd"] + b["exit_cash_usd"] == pytest.approx(-.01443)
    assert b["net_profit_usd"] == pytest.approx(-.044941987423, abs=1e-9)
    assert not b["allowed"]
    assert eng._execution_bounds(c, p) is None


def test_aug31_exit_quote_is_not_profit_after_actual_entry_cost():
    eng = engine(midline=-5.45082987799)
    eng.entropy.fee_bps = .9
    open_lot(eng, "entropy", "sell", .1111, 1530, .015298)
    open_lot(eng, "hedge", "buy", .1111, 1530.75)
    eng.entropy.set_book(1524.8, 169.25905 / .111)
    eng.hedge.set_book(1525.9, 1526)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE, .111)
    assert p.exp_edge_usd > .10
    b = eng._cycle_profit_budget(c, p)
    assert 0 < b["net_profit_usd"] < .01
    assert eng._execution_bounds(c, p) is None


def test_fifo_fee_allocation_survives_partial_close_and_restart(tmp_path):
    eng = engine()
    eng.ledger.close()
    path = tmp_path / "cost.sqlite3"
    eng.ledger = TradeLedger(path)
    open_lot(eng, "entropy", "sell", 1, 100, .1, suffix="a")
    open_lot(eng, "entropy", "sell", 1, 110, .2, suffix="b", ts=time.time())
    first = eng.ledger.preview_close_cost(venue="entropy", position=-2,
                                         qty=1.5, now=time.time())
    assert first["entry_cash_usd"] == 155
    assert first["entry_fees_usd"] == pytest.approx(.2)
    open_lot(eng, "entropy", "buy", 1.5, 99, .01, suffix="close")
    eng.ledger.close()
    eng.ledger = TradeLedger(path)
    rest = eng.ledger.preview_close_cost(venue="entropy", position=-.5,
                                        qty=.5, now=time.time())
    assert rest["ready"]
    assert rest["entry_cash_usd"] == 55
    assert rest["entry_fees_usd"] == pytest.approx(.1)
    eng.ledger.close()


def test_baseline_or_mismatched_positions_cannot_manufacture_profit():
    eng = engine()
    eng.ledger.ensure_position_baseline(venue="entropy", exchange_qty=-1,
                                       mark_px=100, ts=time.time())
    eng.entropy.position = -1
    open_lot(eng, "hedge", "buy", 1, 100)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    assert eng._cycle_profit_budget(c, p)["reason"] == "unknown_entry_cost"
    eng.entropy.position = -2
    assert eng._cycle_profit_budget(c, p)["reason"] == "inventory_mismatch"


def test_unconfirmed_limit_price_cannot_be_used_as_entry_cost():
    eng = engine()
    state = ExecutionState("fallback", "open", "buy_entropy", 1, True, False, False)
    eng._observe_execution_result(state, "first", eng.entropy, is_buy=True,
                                  info={"status": "filled", "filled_base": 1},
                                  fallback_px=100)
    got = eng.ledger.preview_close_cost(venue="entropy", position=1, qty=1,
                                       now=time.time())
    assert not got["ready"] and got["reason"] == "unknown_entry_cost"


def test_legacy_confirmation_without_average_is_not_trusted():
    eng = engine()
    eng.ledger.record_fill(
        fill_key="legacy-fallback", execution_id="legacy", leg="first",
        venue="entropy", order_ref="no-average", ts=100,
        side="buy", qty=1, price=100,
        metadata={"cumulative_filled": 1, "previous_cumulative_filled": 0})
    got = eng.ledger.preview_close_cost(venue="entropy", position=1, qty=1,
                                       now=time.time())
    assert got["reason"] == "unknown_entry_cost"


def test_cumulative_vwap_is_converted_to_incremental_cost():
    eng = engine()
    state = ExecutionState("partials", "open", "buy_entropy", 1, True, False, False)
    eng._observe_execution_result(state, "first", eng.entropy, is_buy=True,
                                  info={"order_ref": "o1", "status": "partial",
                                        "filled_base": .4, "avg_px": 100,
                                        "unresolved": True}, fallback_px=120)
    state = ExecutionState.from_dict(state.to_dict())  # recovery checkpoint
    eng._observe_execution_result(state, "first", eng.entropy, is_buy=True,
                                  info={"order_ref": "o1", "status": "filled",
                                        "filled_base": 1, "avg_px": 106,
                                        "unresolved": False}, fallback_px=120)
    rows = eng.ledger._conn.execute("SELECT qty,price FROM fills ORDER BY id").fetchall()
    assert rows[1]["qty"] == pytest.approx(.6)
    assert rows[1]["price"] == pytest.approx(110)
    got = eng.ledger.preview_close_cost(venue="entropy", position=1, qty=1,
                                       now=time.time())
    assert got["entry_cash_usd"] == pytest.approx(-106)
    assert eng.entropy.cash == pytest.approx(-106)


def test_funding_debits_allocated_without_using_credits():
    eng = engine()
    open_lot(eng, "entropy", "sell", 2, 100, ts=10)
    for ts, pnl in [(9, -10), (11, -.2), (12, .5)]:
        eng.ledger.record_funding_event(ts=ts, venue="entropy", position_qty=-2,
                                        mark_px=100, rate_bps_h=1, pnl_usd=pnl)
    cost = eng.ledger.preview_close_cost(venue="entropy", position=-2,
                                        qty=.5, now=20)
    assert cost["funding_debit_usd"] == pytest.approx(.05)


def test_fee_api_backfill_changes_remaining_cost_without_restart():
    eng = engine()
    open_lot(eng, "entropy", "sell", 1, 100, .01, ts=100)
    eng.ledger.reconcile_exchange_fills("entropy", [{
        "trade_id": "x1", "order_ref": "entropy:open", "ts": 100,
        "side": "sell", "qty": 1, "price": 100, "fee_usd": .03,
        "fee_token": "USDC",
    }], now=101)
    cost = eng.ledger.preview_close_cost(venue="entropy", position=-1,
                                        qty=.5, now=102)
    assert cost["entry_fees_usd"] == pytest.approx(.015)


def test_stale_or_skewed_quotes_fail_closed():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    eng.hedge.book.last_update_ts -= 2
    assert eng._cycle_profit_budget(c, p)["reason"] == "stale_or_skewed_books"


def test_final_reprice_allows_alive_books_inside_hard_stale_boundary():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    for venue in (eng.entropy, eng.hedge):
        venue.book.last_update_ts = time.time() - 2.0
        venue.book.touch()  # transport is alive, but the book is old
    assert eng._refresh_execution_plan(trade) is not None


def test_final_reprice_allows_cross_venue_skew_inside_hard_boundary():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    now = time.time()
    eng.entropy.book.last_update_ts = now
    eng.entropy.book.alive_ts = now
    eng.hedge.book.last_update_ts = now - 0.8
    eng.hedge.book.alive_ts = now
    assert eng._refresh_execution_plan(trade) is not None


def test_cycle_loss_enters_recovery_hold_without_risk_trigger():
    eng = engine()
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        max_hold_sec=600, max_cycle_loss_bps=3.0)
    open_lot(eng, "entropy", "buy", 1, 100)
    open_lot(eng, "hedge", "sell", 1, 99)
    eng.cycle.opened_ts = time.time() - 10
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.REDUCE)
    budget = eng._cycle_profit_budget(c, p)
    assert budget["net_profit_usd"] < -budget["loss_limit_usd"]
    assert not budget["allowed"]
    assert budget["reason"] == "loss_recovery_hold"
    assert budget["hold_state"] == "loss_recovery_hold"
    assert not budget["forced"]
    assert not budget["risk_exit"]


def test_fill_pnl_normal_exit_uses_pnl_protected_bound():
    eng = engine()
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        fast_exit_after_sec=600, max_hold_sec=600)
    open_lot(eng, "entropy", "sell", 1, 101)
    open_lot(eng, "hedge", "buy", 1, 100)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)

    trade = eng._execution_bounds(c, p)

    assert trade is not None
    assert trade.cycle_budget["reason"] == "ready"
    assert trade.cycle_budget["exit_class"] == "economic"
    assert not trade.cycle_budget["forced"]
    assert not trade.cycle_budget["risk_exit"]
    assert trade.cycle_budget["limit_basis"] == "pnl_protected"
    assert trade.cycle_budget["limit_net_profit_usd"] >= 0


def test_fill_pnl_fast_quote_is_blocked_when_worst_case_fails():
    eng = engine()
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        fast_exit_after_sec=20, fast_exit_min_profit_usd=-.01,
        max_hold_sec=600)
    open_lot(eng, "entropy", "sell", 1, 100)
    open_lot(eng, "hedge", "buy", 1, 100.045)
    eng.cycle.start(
        direction="short_entropy", opened_ts=time.time() - 25,
        entry_midline_bps=0, entry_sigma_bps=1, entry_band_bps=4,
        entry_threshold_bps=4, paired_qty=1)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    c = replace(c, cycle_midline_drift_bps=0)

    budget = eng._cycle_profit_budget(c, p)

    assert budget["reason"] == "fast_exit"
    assert budget["allowed"]
    assert not budget["forced"]
    assert not budget["risk_exit"]
    # The quote-level fast allowance is not enough: the protected IOC bound
    # must still survive the hedge's worst executable price.
    assert eng._execution_bounds(c, p) is None


def test_fill_pnl_fast_exit_can_pass_with_a_protected_bound():
    eng = engine()
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        fast_exit_after_sec=20, fast_exit_min_profit_usd=-.01,
        max_hold_sec=600)
    # Keep the fast-exit floor meaningful while leaving enough room for the
    # configured second-leg protection to remain executable.
    eng.cfg.second_leg_slippage_bps = .1
    open_lot(eng, "entropy", "sell", 1, 100)
    # Give the close a little more whole-cycle room than the worst-case case
    # above; the first leg should then have a real positive budget after the
    # hedge reserve is consumed.
    open_lot(eng, "hedge", "buy", 1, 100.043)
    eng.cycle.start(
        direction="short_entropy", opened_ts=time.time() - 25,
        entry_midline_bps=0, entry_sigma_bps=1, entry_band_bps=4,
        entry_threshold_bps=4, paired_qty=1)
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)
    c = replace(c, cycle_midline_drift_bps=0)

    trade = eng._execution_bounds(c, p)

    assert trade is not None
    assert trade.cycle_budget["reason"] == "ready"
    assert trade.cycle_budget["exit_class"] == "economic"
    assert trade.cycle_budget["allowed_floor_usd"] == pytest.approx(-.01)
    assert trade.cycle_budget["limit_net_profit_usd"] >= -.01 - 1e-9
    assert not trade.cycle_budget["forced"]
    assert not trade.cycle_budget["risk_exit"]


def test_fill_pnl_economic_exit_prices_cross_venue_skew_as_soft_uncertainty():
    eng = engine()
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        fast_exit_after_sec=600, max_hold_sec=600)
    open_lot(eng, "entropy", "sell", 1, 101)
    open_lot(eng, "hedge", "buy", 1, 100)
    now = time.time()
    eng.entropy.book.last_update_ts = now
    eng.entropy.book.alive_ts = now
    eng.hedge.book.last_update_ts = now - .8
    eng.hedge.book.alive_ts = now - .8
    c, p = candidate_plan(eng, "buy_entropy", StrategyAction.REDUCE)

    budget = eng._cycle_profit_budget(c, p)

    assert budget["cross_venue_skew_ms"] > eng.cfg.premium_model.max_cross_venue_skew_ms
    assert budget["allowed"]
    assert budget["reason"] != "stale_or_skewed_books"
    assert eng._execution_bounds(c, p) is not None


@pytest.mark.parametrize(
    "direction, action, entropy_side, hedge_side, entropy_book, hedge_book, drift",
    [
        ("long_entropy", "sell_entropy", "buy", "sell",
         (99.94, 99.95), (99.99, 100.0), -6.0),
        ("short_entropy", "buy_entropy", "sell", "buy",
         (100.05, 100.06), (99.99, 100.0), 6.0),
    ],
)
def test_fill_pnl_enabled_adverse_drift_releases_risk(
        direction, action, entropy_side, hedge_side, entropy_book,
        hedge_book, drift):
    eng = engine()
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        fast_exit_after_sec=600, max_hold_sec=600,
        adverse_drift_enabled=True,
        adverse_drift_min_age_sec=30, adverse_drift_min_loss_usd=-.03,
        adverse_drift_bps=5, adverse_drift_persist_sec=5,
        regime_stop_min_age_sec=30, regime_stop_min_loss_usd=-.03,
        regime_stop_require_midline_warning=True)
    open_lot(eng, "entropy", entropy_side, 1, 100)
    open_lot(eng, "hedge", hedge_side, 1, 100)
    eng.entropy.set_book(*entropy_book)
    eng.hedge.set_book(*hedge_book)
    eng.cycle.start(
        direction=direction, opened_ts=time.time() - 40,
        entry_midline_bps=0, entry_sigma_bps=1, entry_band_bps=4,
        entry_threshold_bps=4, paired_qty=1)
    eng._adverse_drift_started_ts = time.time() - 6
    c, p = candidate_plan(eng, action, StrategyAction.REDUCE)
    c = replace(c, cycle_midline_drift_bps=drift)
    eng.model_snapshot = replace(
        eng.model_snapshot, midline_lag_state=MidlineLagState.WARNING)

    budget = eng._cycle_profit_budget(c, p)

    assert budget["adverse_drift_bps"] >= 5
    assert budget["adverse_drift_persist_sec"] >= 5
    assert budget["allowed"]
    assert budget["reason"] == "adverse_drift_release"
    assert budget["forced"] and budget["risk_exit"]
    assert budget["exit_class"] == "risk"
    trade = eng._execution_bounds(c, p)
    assert trade is not None and trade.candidate.reduce_only


def test_fill_pnl_adverse_drift_needs_persistence():
    eng = engine()
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        fast_exit_after_sec=600, max_hold_sec=600,
        adverse_drift_min_age_sec=30, adverse_drift_min_loss_usd=-.03,
        adverse_drift_bps=5, adverse_drift_persist_sec=5)
    open_lot(eng, "entropy", "buy", 1, 100)
    open_lot(eng, "hedge", "sell", 1, 100)
    eng.cycle.start(
        direction="long_entropy", opened_ts=time.time() - 40,
        entry_midline_bps=0, entry_sigma_bps=1, entry_band_bps=4,
        entry_threshold_bps=4, paired_qty=1)
    eng._adverse_drift_started_ts = time.time() - 1
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.REDUCE)
    c = replace(c, cycle_midline_drift_bps=-6)

    budget = eng._cycle_profit_budget(c, p)

    assert budget["adverse_drift_bps"] >= 5
    assert budget["adverse_drift_persist_sec"] < 5
    assert not budget["allowed"]
    assert budget["reason"] == "loss_recovery_hold"
    assert budget["hold_state"] == "loss_recovery_hold"
    assert not budget["risk_exit"]


def test_fill_pnl_ordinary_drift_does_not_exit():
    eng = engine()
    eng.cfg.cycle_exit = replace(
        eng.cfg.cycle_exit, enabled=True, mode="fill_pnl",
        fast_exit_after_sec=600, max_hold_sec=600,
        adverse_drift_min_age_sec=30, adverse_drift_min_loss_usd=-.03,
        adverse_drift_bps=5, adverse_drift_persist_sec=5)
    open_lot(eng, "entropy", "buy", 1, 100)
    open_lot(eng, "hedge", "sell", 1, 100)
    eng.cycle.start(
        direction="long_entropy", opened_ts=time.time() - 40,
        entry_midline_bps=0, entry_sigma_bps=1, entry_band_bps=4,
        entry_threshold_bps=4, paired_qty=1)
    eng._adverse_drift_started_ts = time.time() - 10
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.REDUCE)
    c = replace(c, cycle_midline_drift_bps=-4)

    budget = eng._cycle_profit_budget(c, p)

    assert budget["adverse_drift_bps"] == pytest.approx(4)
    assert budget["adverse_drift_persist_sec"] == 0
    assert not budget["allowed"]
    assert budget["reason"] == "loss_recovery_hold"
    assert budget["hold_state"] == "loss_recovery_hold"
    assert not budget["risk_exit"]


def test_fill_coverage_requires_authoritative_quantity_for_both_legs():
    eng = engine()
    state = ExecutionState(
        cycle_id="coverage", action="open", direction="buy_entropy",
        planned_qty=.0388, entropy_is_buy=True, hedge_is_buy=False,
        reduce_only=False, first_filled=.0388, hedge_filled=.0099,
        first_position_before=0.0, hedge_position_before=0.0)
    eng.entropy.position = .0388
    eng.hedge.position = -.0388
    eng.ledger.record_fill(
        fill_key="coverage-first", execution_id=state.cycle_id,
        leg="first", venue="entropy", order_ref="coverage-first",
        ts=time.time(), side="buy", qty=.0388, price=100,
        metadata={"price_source": "exchange_average"})
    eng.ledger.record_fill(
        fill_key="coverage-hedge-a", execution_id=state.cycle_id,
        leg="hedge", venue="hedge", order_ref="coverage-hedge-a",
        ts=time.time(), side="sell", qty=.0099, price=100,
        metadata={"price_source": "exchange_average"})

    assert eng._exchange_observed_hedge_qty(state) == pytest.approx(.0388)
    assert not eng._execution_fill_coverage_ready(state)

    eng.ledger.record_fill(
        fill_key="coverage-hedge-b", execution_id=state.cycle_id,
        leg="hedge", venue="hedge", order_ref="coverage-hedge-b",
        ts=time.time(), side="sell", qty=.0289, price=100,
        metadata={"price_source": "exchange_average"})
    assert eng._execution_fill_coverage_ready(state)


def test_missing_reverse_depth_and_funding_block_entries():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    p = replace(p, qty=100)
    assert eng._cycle_profit_budget(c, p)["reason"] == "insufficient_exit_depth"
    eng.cfg.funding_enabled = True
    assert eng._cycle_profit_budget(c, replace(p, qty=1))["reason"] == "funding_unavailable"


def test_price_change_before_first_send_revalidates_without_order():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    assert trade is not None
    eng.hedge.set_book(100.9, 101)
    asyncio.run(eng._execute(trade))
    assert not eng.entropy.sent and not eng.hedge.sent
    assert eng.active_execution is None


def test_second_leg_still_hedges_after_price_moves_against_profit():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    original = eng.entropy.send_taker

    async def first(**kwargs):
        result = await original(**kwargs)
        eng.hedge.set_book(100.9, 101)
        return result

    eng.entropy.send_taker = first
    asyncio.run(eng._execute(trade))
    assert len(eng.entropy.sent) == len(eng.hedge.sent) == 1
    assert eng.entropy.position + eng.hedge.position == pytest.approx(0)


def test_profit_guard_config_defaults_and_hot_reload(tmp_path):
    assert not ProfitGuardConfig().enabled
    path = tmp_path / "config.yaml"
    path.write_text("thresholds:\n  midline_bps: 0\n  upper_bps: 4\n  lower_bps: 4\n"
                    "execution:\n  leg_slippage_bps: 2.5\n  second_leg_slippage_bps: 6\n")
    old = load_config(str(path), NO_ENV, symbol="SNDK", hedge_venue="lighter")
    assert old.profit_guard == ProfitGuardConfig(False, .01, 8.5, 8.5)
    candidate = replace(old, profit_guard=ProfitGuardConfig(True, .03, 5, 3))
    merged, changed, ignored = merge_hot_reloadable_config(old, candidate)
    assert merged.profit_guard == candidate.profit_guard
    assert "profit_guard.enabled" in changed
    assert "profit_guard.min_profit_usd" in changed
    assert "profit_guard" not in ignored


def test_emergency_flatten_ignores_profit_floor_and_missing_cost():
    eng = engine()
    eng.cfg.profit_guard = replace(eng.cfg.profit_guard, min_profit_usd=1e6)
    eng.entropy.position = 1
    asyncio.run(eng._maybe_hedge())
    assert len(eng.entropy.sent) == 1
    assert eng.entropy.sent[0]["reduce_only"]
    assert eng.entropy.position == pytest.approx(0)


def test_unknown_inventory_cannot_be_added_to():
    eng = engine()
    eng.entropy.position, eng.hedge.position = -1, 1
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.ADD)
    assert eng._cycle_profit_budget(c, p)["reason"] == "inventory_mismatch"


def test_normal_exit_slice_is_clipped_to_paired_position():
    eng = engine(midline=20)
    eng.cfg.take_fraction = 1
    open_lot(eng, "entropy", "buy", 1, 90)
    open_lot(eng, "hedge", "sell", 1, 100)
    eng.entropy.set_book(110, 110.01)

    async def scan():
        now = time.time()
        eng._scan(now)
        return eng._scan(now + .2)

    trade = asyncio.run(scan())
    assert trade is not None
    assert trade.plan.qty == 1  # Entropy ref / hedge buy otherwise oversizes


def test_profit_condition_must_persist_not_just_statistical_band():
    eng = engine()
    eng.cfg.premium_persist_sec = 1
    key = "open:sell_entropy"

    async def scan():
        now = time.time()
        assert eng._scan(now) is None
        assert eng._armed[key] == now
        eng.cfg.profit_guard = replace(eng.cfg.profit_guard, min_profit_usd=100)
        assert eng._scan(now + .5) is None
        assert eng._armed[key] is None
        eng.cfg.profit_guard = replace(eng.cfg.profit_guard, min_profit_usd=.01)
        assert eng._scan(now + 1) is None
        assert eng._scan(now + 2) is not None

    asyncio.run(scan())


def test_old_dust_does_not_age_entire_new_position():
    eng = engine()
    open_lot(eng, "entropy", "sell", .001, 100, ts=0, suffix="old")
    open_lot(eng, "entropy", "sell", .999, 100, ts=3600, suffix="new")
    cost = eng.ledger.preview_close_cost(venue="entropy", position=-1,
                                        qty=1, now=3600)
    assert cost["age_qty_hours"] == pytest.approx(.001)


def test_accepted_budget_is_durable_in_execution_context():
    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    trade = eng._execution_bounds(c, p)
    eng._begin_execution(trade)
    _, context = eng.ledger.load_active_execution()
    assert context["cycle_budget"]["net_profit_usd"] > 0
    assert context["cycle_budget"]["cost_basis"] == "projected_round_trip"


def test_status_api_exposes_guard_budget_and_exit_mode(tmp_path):
    from fastapi.testclient import TestClient
    from entropy_arb.dashboard_state import build_dashboard_snapshot, write_dashboard_snapshot
    from entropy_arb.dashboard_api.app import create_app

    eng = engine()
    c, p = candidate_plan(eng, "sell_entropy", StrategyAction.OPEN)
    eng._execution_bounds(c, p)
    payload = build_dashboard_snapshot(eng)
    write_dashboard_snapshot(tmp_path / "status.json", payload)
    response = TestClient(create_app(tmp_path)).get("/api/status")
    assert response.status_code == 200
    guard = response.json()["strategy"]["profit_guard"]
    assert guard["normal_exit_mode"] == "fifo_profit"
    assert guard["checks"]["open:sell_entropy"]["net_profit_usd"] > 0


@pytest.mark.parametrize("value", ["-.1", ".nan", ".inf"])
def test_profit_guard_rejects_invalid_values(tmp_path, value):
    path = tmp_path / "config.yaml"
    path.write_text("thresholds:\n  midline_bps: 0\n  upper_bps: 4\n  lower_bps: 4\n"
                    f"profit_guard:\n  min_profit_usd: {value}\n")
    with pytest.raises(ConfigError, match="profit_guard"):
        load_config(str(path), NO_ENV, symbol="SNDK", hedge_venue="lighter")
