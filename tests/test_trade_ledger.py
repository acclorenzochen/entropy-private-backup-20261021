import csv
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))

from entropy_arb.execution_state import ExecutionState  # noqa: E402
from entropy_arb.trade_ledger import TradeLedger  # noqa: E402


def test_fill_ledger_is_idempotent_and_fifo(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        assert ledger.record_fill(
            fill_key="e:1", execution_id="e", leg="first", venue="entropy",
            order_ref="o1", ts=100.0, side="buy", qty=1.0, price=100.0,
            fee_usd=.1)
        assert not ledger.record_fill(
            fill_key="e:1", execution_id="e", leg="first", venue="entropy",
            order_ref="o1", ts=100.0, side="buy", qty=1.0, price=100.0,
            fee_usd=.1)
        assert ledger.record_fill(
            fill_key="e:2", execution_id="e", leg="hedge", venue="entropy",
            order_ref="o2", ts=101.0, side="sell", qty=.4, price=110.0,
            fee_usd=.04)
        assert ledger.record_fill(
            fill_key="e:3", execution_id="e", leg="hedge", venue="entropy",
            order_ref="o3", ts=102.0, side="sell", qty=.6, price=90.0,
            fee_usd=.06)
        attr = ledger.attribution(
            now=200.0, day_start_ts=0.0, session_start_ts=0.0,
            marks={"entropy": 100.0}, equity_day=None, equity_session=None)
        assert attr["day"]["gross_realized_usd"] == -2.0
        assert attr["day"]["fees_usd"] == .2
        assert attr["day"]["unrealized_usd"] == 0.0
        assert attr["day"]["ledger_net_pnl_usd"] == -2.2
        assert ledger.execution_fill_edge("e") == -2.2
    finally:
        ledger.close()


def test_checkpoint_survives_restart(tmp_path):
    path = tmp_path / "ledger.sqlite3"
    state = ExecutionState(
        cycle_id="cycle-1", action="open", direction="sell_entropy",
        planned_qty=.04, entropy_is_buy=False, hedge_is_buy=True,
        reduce_only=False, first_filled=.02, first_order_ref="entropy-1",
        first_position_before=0.0)
    first = TradeLedger(path)
    first.checkpoint_execution(state, {"buy_venue": "hedge"}, "created")
    first.close()

    second = TradeLedger(path)
    try:
        restored = second.load_active_execution()
        assert restored is not None
        loaded, context = restored
        assert loaded.cycle_id == "cycle-1"
        assert loaded.first_filled == .02
        assert context["buy_venue"] == "hedge"
    finally:
        second.close()


def test_noncritical_write_skips_bounded_sqlite_contention(tmp_path):
    path = tmp_path / "locked.sqlite3"
    holder = TradeLedger(path)
    contender = TradeLedger(path)
    try:
        holder._conn.execute("BEGIN IMMEDIATE")
        assert contender.record_market_sample(
            ts=100.0, payload={"premium_mid_bps": 1.0}) is None
        stats = contender.write_stats()
        assert stats["sqlite_busy_noncritical_count"] >= 2
        assert not contender.has_data("market_samples")
    finally:
        holder._conn.rollback()
        contender.close()
        holder.close()


def test_critical_write_retries_then_fails_closed_on_sqlite_contention(tmp_path):
    path = tmp_path / "locked-critical.sqlite3"
    holder = TradeLedger(path)
    contender = TradeLedger(path)
    try:
        holder._conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            contender.record_fill(
                fill_key="blocked", execution_id="blocked", leg="first",
                venue="entropy", order_ref="blocked", ts=100.0,
                side="buy", qty=1.0, price=100.0)
        assert contender.write_stats()["sqlite_busy_critical_count"] >= 4
        assert not contender.has_data("fills")
    finally:
        holder._conn.rollback()
        contender.close()
        holder.close()


def test_read_only_ledger_does_not_run_schema_or_data_writes(tmp_path):
    path = tmp_path / "readonly.sqlite3"
    writer = TradeLedger(path)
    writer.record_market_sample(ts=100.0, payload={"premium_mid_bps": 1.0})
    writer.close()
    before = path.stat().st_mtime_ns

    ledger = TradeLedger(path, read_only=True)
    try:
        assert ledger.has_data("market_samples")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            ledger.record_market_sample(
                ts=101.0, payload={"premium_mid_bps": 2.0})
    finally:
        ledger.close()
    assert path.stat().st_mtime_ns == before


def test_valid_premium_history_requires_quality_flag(tmp_path):
    ledger = TradeLedger(tmp_path / "premium-history.sqlite3")
    try:
        ledger.record_market_sample(
            ts=100.0,
            payload={"premium_mid_bps": "1.25",
                     "premium_sample_valid": "true"})
        ledger.record_market_sample(
            ts=101.0,
            payload={"premium_mid_bps": "9.00",
                     "premium_sample_valid": "false"})
        ledger.record_market_sample(
            ts=102.0, payload={"premium_mid_bps": "8.00"})

        assert ledger.load_valid_premium_samples(
            since=99.0, until=103.0) == [(100.0, 1.25)]
    finally:
        ledger.close()


def test_dashboard_history_reads_one_sample_per_display_bucket(tmp_path):
    ledger = TradeLedger(tmp_path / "history.sqlite3")
    try:
        for ts, premium in ((100.0, 1.0), (101.0, 2.0),
                            (102.0, 3.0), (103.0, 4.0)):
            ledger.record_market_sample(
                ts=ts, payload={"premium_mid_bps": premium})

        result = ledger.history("1h", now=104.0)

        assert [point["premium_bps"] for point in result["points"]] == [
            2.0, 4.0]
    finally:
        ledger.close()


def test_position_baseline_does_not_create_pnl(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        assert ledger.ensure_position_baseline(
            venue="entropy", exchange_qty=1.0, mark_px=100.0,
            ts=100.0)
        # A later reconciliation consumes the unknown baseline at a different
        # mark. That price move is not attributable to this bot's fills.
        assert ledger.record_fill(
            fill_key="baseline-close", execution_id="reconcile:entropy",
            leg="baseline", venue="entropy", order_ref=None, ts=101.0,
            side="sell", qty=1.0, price=110.0,
            source="position_baseline")
        attr = ledger.attribution(
            now=200.0, day_start_ts=0.0, session_start_ts=0.0,
            marks={"entropy": 110.0}, equity_day=None,
            equity_session=None)
        assert attr["day"]["gross_realized_usd"] == 0.0
        assert attr["day"]["fees_usd"] == 0.0
        assert attr["day"]["unrealized_usd"] == 0.0
        assert attr["day"]["fill_count"] == 0
    finally:
        ledger.close()


def test_legacy_migration_is_repeatable(tmp_path):
    trades = tmp_path / "trades.csv"
    with trades.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=[
            "ts", "action", "direction", "qty", "buy_notional",
            "sell_notional", "buy_fill", "sell_fill", "buy_status",
            "sell_status", "buy_venue", "sell_venue", "threshold_bps",
            "net_marginal_edge_bps", "fill_edge_usd", "reduce_only"])
        writer.writeheader()
        writer.writerow({
            "ts": 100, "action": "open", "direction": "sell_entropy",
            "qty": 1, "buy_notional": 100, "sell_notional": 100,
            "buy_fill": 1, "sell_fill": 1, "buy_status": "filled",
            "sell_status": "filled", "buy_venue": "hedge",
            "sell_venue": "entropy", "threshold_bps": 5,
            "net_marginal_edge_bps": 6, "fill_edge_usd": .1,
            "reduce_only": 0})
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        assert ledger.migrate_legacy(trades_paths=(trades,))["trades"] == 1
        assert ledger.migrate_legacy(trades_paths=(trades,))["trades"] == 0
        result = ledger.list_trades(20)
        assert result["summary"]["paired_count"] == 1
        assert result["trades"][0]["source"] == "legacy_csv"
    finally:
        ledger.close()


def test_trade_history_deduplicates_engine_and_legacy_copy(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        engine_payload = {
            "trade_key": "1788177645173-1",
            "execution_id": "1788177645173-1",
            "ts": 100.000,
            "action": "open",
            "direction": "buy_entropy",
            "qty": .1142,
            "buy_notional": 170.01496,
            "sell_notional": 170.14772,
            "buy_fill": .1142,
            "sell_fill": .1142,
            "buy_status": "filled",
            "sell_status": "filled",
            "buy_venue": "entropy",
            "sell_venue": "hedge",
            "threshold_bps": 5.413346,
            "net_edge_bps": 6.555079,
            "fill_edge_usd": .073771,
            "outcome": "paired",
            "reduce_only": False,
        }
        legacy_payload = {
            **engine_payload,
            "trade_key": "legacy:copy",
            "execution_id": "legacy:copy",
            "ts": 101.259,
            "buy_notional": 170.01,
            "sell_notional": 170.15,
            "threshold_bps": 5.413346,
            "fill_edge_usd": .0738,
        }
        ledger.record_trade_attempt(engine_payload)
        ledger.record_trade_attempt(legacy_payload, source="legacy_csv")

        result = ledger.list_trades(20)
        assert len(result["trades"]) == 1
        assert result["trades"][0]["execution_id"] == "1788177645173-1"
        assert result["trades"][0]["source"] == "engine"
        assert result["summary"]["attempt_count"] == 1
        assert result["total_count"] == 1
        assert ledger.trade_summary(
            since=0.0, until=200.0, day_timezone="Asia/Taipei"
        )["trade_count"] == 1
        assert ledger.activity_summary(
            since=0.0, until=200.0
        )["trade_count"] == 1
    finally:
        ledger.close()


def test_legacy_migration_skips_existing_live_copy(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    trades = tmp_path / "trades.csv"
    fields = ["ts", "action", "direction", "qty", "buy_notional",
              "sell_notional", "buy_fill", "sell_fill", "buy_status",
              "sell_status", "buy_venue", "sell_venue", "threshold_bps",
              "net_marginal_edge_bps", "fill_edge_usd", "reduce_only"]
    try:
        ledger.record_trade_attempt({
            "trade_key": "live-1", "execution_id": "live-1",
            "ts": 100.0, "action": "open", "direction": "buy_entropy",
            "qty": .1142, "buy_notional": 170.01496,
            "sell_notional": 170.14772, "buy_fill": .1142,
            "sell_fill": .1142, "buy_status": "filled",
            "sell_status": "filled", "buy_venue": "entropy",
            "sell_venue": "hedge", "threshold_bps": 5.413346,
            "net_edge_bps": 6.555079, "fill_edge_usd": .073771,
            "outcome": "paired", "reduce_only": False,
        })
        with trades.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerow({
                "ts": 101.259, "action": "open",
                "direction": "buy_entropy", "qty": .1142,
                "buy_notional": 170.01, "sell_notional": 170.15,
                "buy_fill": .1142, "sell_fill": .1142,
                "buy_status": "filled", "sell_status": "filled",
                "buy_venue": "ENTROPY", "sell_venue": "LIGHTER",
                "threshold_bps": 5.413346,
                "net_marginal_edge_bps": 6.555079,
                "fill_edge_usd": .0738, "reduce_only": 0,
            })

        assert ledger.migrate_legacy(trades_paths=(trades,))["trades"] == 0
        assert ledger.list_trades(20)["total_count"] == 1
        assert ledger._conn.execute(
            "SELECT COUNT(*) FROM legacy_imports WHERE table_name='trade_attempts'"
        ).fetchone()[0] == 1
    finally:
        ledger.close()


def test_activity_summary_reports_entropy_volume_and_unpaired_volume(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        assert ledger.record_fill(
            fill_key="paired-entropy", execution_id="paired", leg="first",
            venue="entropy", order_ref="e1", ts=100.0, side="sell",
            qty=.4, price=100.0)
        assert ledger.record_fill(
            fill_key="paired-hedge", execution_id="paired", leg="hedge",
            venue="hedge", order_ref="h1", ts=100.1, side="buy",
            qty=.4, price=100.0)
        ledger.record_trade_attempt({
            "trade_key": "paired", "execution_id": "paired", "ts": 100.0,
            "action": "open", "direction": "sell_entropy", "qty": .4,
            "buy_fill": .4, "sell_fill": .4, "buy_venue": "hedge",
            "sell_venue": "entropy", "outcome": "paired"})

        assert ledger.record_fill(
            fill_key="one-leg-entropy", execution_id="one-leg", leg="first",
            venue="entropy", order_ref="e2", ts=110.0, side="buy",
            qty=.2, price=99.0)
        ledger.record_trade_attempt({
            "trade_key": "one-leg", "execution_id": "one-leg", "ts": 110.0,
            "action": "open", "direction": "buy_entropy", "qty": .2,
            "buy_fill": .2, "sell_fill": 0.0, "buy_venue": "entropy",
            "sell_venue": "hedge", "outcome": "one_leg"})

        result = ledger.activity_summary(since=90.0, until=120.0)
        assert result["trade_count"] == 2
        assert result["paired_count"] == 1
        assert result["one_leg_count"] == 1
        assert result["entropy_fill_count"] == 2
        assert result["entropy_qty"] == pytest.approx(.6)
        assert result["entropy_notional_usd"] == pytest.approx(59.8)
        assert result["entropy_single_leg_qty"] == pytest.approx(.2)
        assert result["entropy_single_leg_notional_usd"] == pytest.approx(19.8)
        assert result["entropy_single_leg_fill_count"] == 1

        first_page = ledger.list_trades(1)
        assert first_page["total_count"] == 2
        assert first_page["offset"] == 0
        assert first_page["limit"] == 1
        assert first_page["has_more"] is True
        second_page = ledger.list_trades(1, offset=1)
        assert second_page["trades"][0]["execution_id"] == "paired"
        assert second_page["has_more"] is False
    finally:
        ledger.close()


def test_equity_pnl_change_uses_durable_snapshots(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        ledger.record_pnl_snapshot(
            ts=100.0, equity_net_pnl_usd=.10, attribution={})
        ledger.record_pnl_snapshot(
            ts=130.0, equity_net_pnl_usd=.25, attribution={})
        ledger.record_pnl_snapshot(
            ts=160.0, equity_net_pnl_usd=.20, attribution={})
        assert ledger.equity_pnl_change(since=120.0, until=160.0) == .10
        assert ledger.equity_pnl_change(since=170.0, until=180.0) is None
    finally:
        ledger.close()


def test_equity_pnl_change_uses_absolute_equity_across_day_reset(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        ledger.record_pnl_snapshot(
            ts=86390.0, equity_net_pnl_usd=1.0,
            equity_total_usd=1001.0, attribution={})
        ledger.record_pnl_snapshot(
            ts=86410.0, equity_net_pnl_usd=0.0,
            equity_total_usd=1000.5, attribution={})
        assert ledger.equity_pnl_change(since=86390.0, until=86410.0) == -.5
    finally:
        ledger.close()


def test_lifetime_equity_baseline_and_points_survive_restart(tmp_path):
    path = tmp_path / "ledger.sqlite3"
    first = TradeLedger(path)
    first.record_pnl_snapshot(
        ts=100.0, equity_net_pnl_usd=0.0, equity_total_usd=1000.0,
        attribution={})
    first.record_pnl_snapshot(
        ts=110.0, equity_net_pnl_usd=1.25, equity_total_usd=1001.25,
        attribution={})
    first.record_pnl_snapshot(
        ts=120.0, equity_net_pnl_usd=-1.25, equity_total_usd=998.75,
        attribution={})
    first.close()

    ledger = TradeLedger(path)
    try:
        lifetime = ledger.equity_lifetime(now=130.0)
        assert lifetime == {
            "start_ts": 100.0,
            "end_ts": 130.0,
            "start_equity_usd": 1000.0,
            "current_equity_usd": 998.75,
            "change_usd": -1.25,
            "coverage_complete": True,
            "source": "sqlite:pnl_snapshots",
            "sample_count": 3,
        }
        assert ledger.lifetime_pnl_points(until=130.0) == [
            {"ts": 100.0, "net_pnl_usd": 0.0},
            {"ts": 110.0, "net_pnl_usd": 1.25},
            {"ts": 120.0, "net_pnl_usd": -1.25},
        ]

        attr = ledger.attribution(
            now=130.0, day_start_ts=100.0, session_start_ts=120.0,
            lifetime_start_ts=lifetime["start_ts"],
            marks={}, equity_day=None, equity_session=None,
            equity_lifetime=lifetime["change_usd"])
        assert attr["lifetime"]["equity_net_pnl_usd"] == -1.25
        assert attr["lifetime"]["coverage_start_ts"] == 100.0
    finally:
        ledger.close()


def test_lifetime_pnl_points_limit_keeps_curve_bounded_and_extrema(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        values = [1000.0, 1004.0, 998.0, 1002.0, 1001.0,
                  1007.0, 999.0, 1003.0, 1005.0, 1006.0,
                  1001.0, 1008.0]
        for index, equity in enumerate(values):
            ledger.record_pnl_snapshot(
                ts=100.0 + index, equity_net_pnl_usd=equity - 1000.0,
                equity_total_usd=equity, attribution={})

        points = ledger.lifetime_pnl_points(until=200.0, limit=8)
        assert len(points) <= 8
        assert points[0] == {"ts": 100.0, "net_pnl_usd": 0.0}
        assert points[-1] == {"ts": 111.0, "net_pnl_usd": 8.0}
        assert min(point["net_pnl_usd"] for point in points) == -2.0
        assert max(point["net_pnl_usd"] for point in points) == 8.0
    finally:
        ledger.close()


def test_zero_fee_venue_clears_estimates_and_is_excluded_from_fee_status(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        assert ledger.record_fill(
            fill_key="lighter-fee", execution_id="zero-fee-exec",
            leg="hedge", venue="hedge", order_ref="h1", ts=100.0,
            side="buy", qty=1.0, price=100.0, fee_usd=0.25)
        changed = ledger.mark_zero_fee_venue("hedge")
        assert changed == 1
        lot = ledger._conn.execute(
            "SELECT fee_remaining FROM inventory_lots").fetchone()
        assert float(lot["fee_remaining"]) == 0.0

        attr = ledger.attribution(
            now=200.0, day_start_ts=0.0, session_start_ts=0.0,
            marks={"hedge": 100.0}, equity_day=None, equity_session=None)
        assert attr["day"]["fees_usd"] == 0.0
        assert attr["day"]["fees_status"] == "none"
        assert attr["day"]["estimated_fee_fill_count"] == 0
    finally:
        ledger.close()


def test_trade_history_exposes_entropy_fee_only(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        assert ledger.record_fill(
            fill_key="entropy-fee", execution_id="fee-display",
            leg="first", venue="entropy", order_ref="e1", ts=100.0,
            side="sell", qty=1.0, price=100.0, fee_usd=0.10)
        assert ledger.record_fill(
            fill_key="lighter-no-fee", execution_id="fee-display",
            leg="hedge", venue="hedge", order_ref="h1", ts=100.1,
            side="buy", qty=1.0, price=100.0, fee_usd=0.20)
        ledger.mark_zero_fee_venue("hedge")
        ledger.record_trade_attempt({
            "trade_key": "fee-display", "execution_id": "fee-display",
            "ts": 100.0, "action": "open", "direction": "sell_entropy",
            "qty": 1.0, "buy_fill": 1.0, "sell_fill": 1.0,
            "buy_venue": "hedge", "sell_venue": "entropy",
            "buy_notional": 100.0, "sell_notional": 100.0,
            "buy_status": "filled", "sell_status": "filled",
            "outcome": "paired"})

        trade = ledger.list_trades(1)["trades"][0]
        assert trade["fees_usd"] == pytest.approx(0.10)
        assert trade["entropy_leg"]["fee_usd"] == pytest.approx(0.10)
        assert trade["lighter_leg"]["fee_usd"] is None
        assert trade["lighter_leg"]["fee_status"] == "none"
    finally:
        ledger.close()


def test_trade_history_exposes_per_leg_and_pair_realized_pnl(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        assert ledger.record_fill(
            fill_key="open-entropy", execution_id="open-pair", leg="first",
            venue="entropy", order_ref="open-e", ts=100.0, side="buy",
            qty=1.0, price=100.0, fee_usd=0.10)
        assert ledger.record_fill(
            fill_key="open-lighter", execution_id="open-pair", leg="hedge",
            venue="hedge", order_ref="open-h", ts=100.1, side="sell",
            qty=1.0, price=101.0, fee_usd=0.0)
        ledger.mark_zero_fee_venue("hedge")
        ledger.record_trade_attempt({
            "trade_key": "open-pair", "execution_id": "open-pair",
            "ts": 100.0, "action": "open", "direction": "buy_entropy",
            "qty": 1.0, "buy_fill": 1.0, "sell_fill": 1.0,
            "buy_venue": "entropy", "sell_venue": "hedge",
            "buy_status": "filled", "sell_status": "filled",
            "outcome": "paired"})

        assert ledger.record_fill(
            fill_key="close-entropy", execution_id="close-pair", leg="first",
            venue="entropy", order_ref="close-e", ts=110.0, side="sell",
            qty=1.0, price=105.0, fee_usd=0.20)
        assert ledger.record_fill(
            fill_key="close-lighter", execution_id="close-pair", leg="hedge",
            venue="hedge", order_ref="close-h", ts=110.1, side="buy",
            qty=1.0, price=99.0, fee_usd=0.0)
        ledger.mark_zero_fee_venue("hedge")
        ledger.record_trade_attempt({
            "trade_key": "close-pair", "execution_id": "close-pair",
            "ts": 110.0, "action": "reduce", "direction": "sell_entropy",
            "qty": 1.0, "buy_fill": 1.0, "sell_fill": 1.0,
            "buy_venue": "hedge", "sell_venue": "entropy",
            "buy_status": "filled", "sell_status": "filled",
            "outcome": "paired", "reduce_only": True})

        trades = ledger.list_trades(10)["trades"]
        close = next(item for item in trades
                     if item["execution_id"] == "close-pair")
        assert close["entropy_leg"]["gross_realized_pnl_usd"] == pytest.approx(5.0)
        assert close["entropy_leg"]["net_realized_pnl_usd"] == pytest.approx(4.8)
        assert close["lighter_leg"]["gross_realized_pnl_usd"] == pytest.approx(2.0)
        assert close["lighter_leg"]["net_realized_pnl_usd"] == pytest.approx(2.0)
        assert close["gross_realized_pnl_usd"] == pytest.approx(7.0)
        assert close["net_realized_pnl_usd"] == pytest.approx(6.8)
        assert close["pnl_status"] == "ledger"

        opened = next(item for item in trades
                      if item["execution_id"] == "open-pair")
        assert opened["entropy_leg"]["net_realized_pnl_usd"] is None
        assert opened["pnl_status"] == "not_closed"
    finally:
        ledger.close()


def test_exchange_fee_reconciliation_replaces_estimate_once(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        assert ledger.record_fill(
            fill_key="fee-local", execution_id="fee-exec", leg="first",
            venue="entropy", order_ref="order-1", ts=100.0,
            side="buy", qty=1.0, price=100.0, fee_usd=0.01)
        first = ledger.reconcile_exchange_fills(
            "entropy", [{
                "exchange_fill_key": "entropy:trade-1",
                "exchange_trade_id": "trade-1",
                "order_ref": "order-1",
                "ts": 100.1,
                "side": "buy",
                "qty": 1.0,
                "price": 100.0,
                "fee_amount": 0.09,
                "fee_usd": 0.09,
                "fee_token": "USDC",
                "raw": {"tid": "trade-1"},
            }], now=200.0, cash_since_ts=90.0)
        assert first["stored_count"] == 1
        assert first["actual_fill_count"] == 1
        assert first["fee_delta_usd"] == pytest.approx(0.08)
        assert first["cash_adjustment_usd"] == pytest.approx(0.08)

        row = ledger._conn.execute(
            "SELECT fee_usd,fee_source,fee_token FROM fills").fetchone()
        assert dict(row) == {
            "fee_usd": 0.09, "fee_source": "exchange_api",
            "fee_token": "USDC",
        }
        attr = ledger.attribution(
            now=200.0, day_start_ts=0.0, session_start_ts=0.0,
            marks={"entropy": 100.0}, equity_day=None, equity_session=None)
        assert attr["day"]["fees_status"] == "actual"
        assert attr["day"]["actual_fee_usd"] == pytest.approx(0.09)

        replay = ledger.reconcile_exchange_fills(
            "entropy", [{
                "exchange_fill_key": "entropy:trade-1",
                "exchange_trade_id": "trade-1",
                "order_ref": "order-1",
                "ts": 100.1,
                "side": "buy",
                "qty": 1.0,
                "price": 100.0,
                "fee_amount": 0.09,
                "fee_usd": 0.09,
                "fee_token": "USDC",
                "raw": {"tid": "trade-1"},
            }], now=201.0, cash_since_ts=90.0)
        assert replay["stored_count"] == 0
        assert replay["fee_delta_usd"] == 0.0
        assert replay["cash_adjustment_usd"] == 0.0
    finally:
        ledger.close()


def test_exchange_fee_reconciliation_does_not_match_partial_page(tmp_path):
    ledger = TradeLedger(tmp_path / "ledger.sqlite3")
    try:
        assert ledger.record_fill(
            fill_key="partial-local", execution_id="partial-exec",
            leg="first", venue="entropy", order_ref="order-2", ts=100.0,
            side="sell", qty=1.0, price=100.0, fee_usd=0.01)
        result = ledger.reconcile_exchange_fills(
            "entropy", [{
                "exchange_fill_key": "entropy:trade-2",
                "order_ref": "order-2", "ts": 100.1, "side": "sell",
                "qty": 0.5, "price": 100.0, "fee_usd": 0.045,
                "fee_token": "USDC",
            }], now=200.0)
        assert result["unmatched_count"] == 1
        row = ledger._conn.execute(
            "SELECT fee_usd,fee_source FROM fills").fetchone()
        assert dict(row) == {"fee_usd": 0.01, "fee_source": "estimated_config"}
    finally:
        ledger.close()
