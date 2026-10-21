"""Cycle throughput and accounting regressions, using isolated ledgers."""
import json

import pytest

from entropy_arb.trade_ledger import TradeLedger


def cycle(ledger, name, opened, flat=None, **extra):
    ledger.upsert_cycle(dict(cycle_id=name, open_first_fill_ts=opened,
                            flat_ts=flat, hold_sec=None if flat is None else flat-opened,
                            status='active' if flat is None else 'completed',
                            direction='buy_entropy', policy_key='v2', **extra))


def test_completion_window_is_flat_time_and_never_looks_ahead(tmp_path):
    ledger = TradeLedger(tmp_path / 'ledger.db')
    try:
        cycle(ledger, 'carry', 50, 120)
        cycle(ledger, 'future', 130, 250)
        cycle(ledger, 'current', 150, 180)
        summary = ledger.cycle_summary(since=100, until=200)
        assert summary['completed_count'] == 2
        assert summary['cohort_completed_count'] == 1
        assert summary['active_count'] == 1
        assert summary['completion_rate'] == .5
        assert summary['cycles_per_hour'] == pytest.approx(72)
        assert summary['observation_hours'] == pytest.approx(100/3600)
    finally:
        ledger.close()


def test_duration_filters_retain_losses_and_censored_age(tmp_path):
    ledger = TradeLedger(tmp_path / 'ledger.db')
    try:
        cycle(ledger, 'recent-loss', 110, 130, total_cycle_pnl_usd=-.2,
              exit_mode='hard_timeout', recovery_count=1, notional_usd=10)
        cycle(ledger, 'future', 100, 500)
        cycle(ledger, 'old', 1, 20)
        ledger.upsert_cycle(dict(cycle_id='other', open_first_fill_ts=100,
                                 flat_ts=190, direction='sell_entropy', policy_key='v2'))
        result = ledger.cycle_duration_stats(now=200, since=100,
                                            direction='buy_entropy', policy_key='v2')
        assert result['count'] == 1
        assert result['p90_sec'] == 20
        assert result['max_active_age_sec'] == 100
        outcomes = ledger.recent_cycle_outcomes(now=200, since=100,
                                                direction='buy_entropy', policy_key='v2')
        assert outcomes['outcomes'][0]['net_pnl_usd'] == -.2
        assert ledger.cycle_duration_stats(now=200, policy_key='v3')['count'] == 0
    finally:
        ledger.close()


def test_exchange_fee_backfill_updates_saved_cycle_and_json_idempotently(tmp_path):
    ledger = TradeLedger(tmp_path / 'ledger.db')
    try:
        for key, ts, side, price in [('open', 100, 'buy', 100), ('close', 120, 'sell', 101)]:
            ledger.record_fill(fill_key=key, execution_id=key, leg='first', venue='entropy',
                               order_ref=key, ts=ts, side=side, qty=1, price=price, fee_usd=.1)
        cycle(ledger, 'c', 100, 120, execution_ids=['open','close'], total_cycle_pnl_usd=.8)
        fees = [dict(exchange_fill_key=key, order_ref=key, ts=ts, side=side,
                     qty=1, price=price, fee_usd=.2) for key,ts,side,price in
                [('open',100,'buy',100),('close',120,'sell',101)]]
        for _ in range(2):
            ledger.reconcile_exchange_fills('entropy', fees, now=150)
            saved = ledger.load_cycle('c')
            assert saved['total_cycle_pnl_usd'] == pytest.approx(.6)
            assert saved['realized_cycle_pnl_usd'] == pytest.approx(.6)
            assert saved['fee_accounting_status'] == 'final'
            assert saved['accounting_status'] == 'provisional'
            raw = ledger._conn.execute('SELECT payload_json FROM cycle_records').fetchone()[0]
            assert json.loads(raw)['total_cycle_pnl_usd'] == pytest.approx(.6)
        ledger.mark_zero_fee_venue('entropy')
        assert ledger.load_cycle('c')['total_cycle_pnl_usd'] == pytest.approx(1)
    finally:
        ledger.close()


@pytest.mark.parametrize("side,close_side,late_price,expected", [
    ("buy", "sell", 99, 2), ("sell", "buy", 103, 2)])
def test_late_open_price_repairs_consumed_and_remaining_fifo(tmp_path, side, close_side,
                                                           late_price, expected):
    ledger = TradeLedger(tmp_path / 'price.db')
    try:
        ledger.record_fill(fill_key='open', execution_id='open', leg='first',
            venue='entropy', order_ref='open', ts=100, side=side, qty=2, price=100,
            fee_usd=0, metadata={'price_source': 'limit_fallback'})
        ledger.record_fill(fill_key='close1', execution_id='close1', leg='first',
            venue='entropy', order_ref='close1', ts=120, side=close_side, qty=1,
            price=101, fee_usd=0)
        for _ in range(2):
            ledger.record_cumulative_fill(execution_id='open', leg='first',
                venue='entropy', order_ref='open', side=side, observed_qty=2,
                avg_px=late_price, ts=130)
            row = ledger._conn.execute("SELECT * FROM fills WHERE fill_key='close1'").fetchone()
            assert row['gross_realized_pnl_usd'] == pytest.approx(expected)
            assert row['ts'] == 120
            assert ledger._conn.execute('SELECT price FROM inventory_lots').fetchone()[0] == late_price
        ledger.record_fill(fill_key='close2', execution_id='close2', leg='first',
            venue='entropy', order_ref='close2', ts=140, side=close_side, qty=1,
            price=101, fee_usd=0)
        cycle(ledger, 'c', 100, 140, execution_ids=['open', 'close1', 'close2'])
        assert ledger.cycle_financials(['open', 'close1', 'close2'])['net_pnl_usd'] == pytest.approx(2 * expected)
    finally:
        ledger.close()


def test_completed_cycle_late_open_and_close_prices_update_without_retiming(tmp_path):
    ledger = TradeLedger(tmp_path/'completed-prices.db')
    try:
        for name, ts, side, px in [('open',100,'buy',100),('close',120,'sell',101)]:
            ledger.record_fill(fill_key=name, execution_id=name, leg='first',
                venue='entropy', order_ref=name, ts=ts, side=side, qty=1, price=px,
                fee_usd=0, metadata={'price_source':'limit_fallback'})
        cycle(ledger,'c',100,120,execution_ids=['open','close'],total_cycle_pnl_usd=1)
        for name, side, px, expected in [('open','buy',99,2),('close','sell',102,3)]:
            for _ in range(2):
                ledger.record_cumulative_fill(execution_id=name,leg='first',venue='entropy',
                    order_ref=name,side=side,observed_qty=1,avg_px=px,ts=150)
                assert ledger.load_cycle('c')['total_cycle_pnl_usd'] == pytest.approx(expected)
                fill = ledger._conn.execute("SELECT * FROM fills WHERE fill_key='close'").fetchone()
                assert fill['gross_realized_pnl_usd'] == pytest.approx(expected)
                assert fill['ts'] == 120
    finally:
        ledger.close()


def test_cumulative_growth_distributes_unknown_cost_once(tmp_path):
    ledger = TradeLedger(tmp_path/'growth.db')
    try:
        ledger.record_fill(fill_key='old',execution_id='e',leg='first',venue='entropy',
            order_ref='o',ts=100,side='buy',qty=1,price=105,
            metadata={'price_source':'limit_fallback'})
        for _ in range(2):
            ledger.record_cumulative_fill(execution_id='e',leg='first',venue='entropy',
                order_ref='o',side='buy',observed_qty=2,avg_px=100,ts=120)
            totals = ledger._conn.execute('SELECT SUM(qty),SUM(qty*price) FROM fills').fetchone()
            assert tuple(totals) == pytest.approx((2,200))
    finally:
        ledger.close()
