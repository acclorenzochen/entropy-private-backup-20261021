"""Exercise production decisions through simulated exchange I/O only."""
from dataclasses import replace

import pytest

from test_engine import make_cfg
from test_replay_execution_review import row
from tools.replay import ExecutionScenario
from tools.replay_engine import simulate_engine
from entropy_arb.engine import Engine


def config():
    cfg = make_cfg(0, 5, 5)
    cfg.cycle_exit = replace(cfg.cycle_exit, enabled=True, mode='fill_pnl')
    cfg.cooldown_sec = 0
    cfg.funding_enabled = False
    return cfg


def run(rows, **kwargs):
    return simulate_engine(rows, config(), execution_scenario=ExecutionScenario(
        orders_per_minute=30, **kwargs))


def test_real_scan_and_execution_complete_four_chronological_legs(monkeypatch, caplog):
    calls = []
    scan = Engine._scan
    def observe(self, now):
        result = scan(self, now)
        calls.append(None if result is None else result.candidate.action.value+':'+result.candidate.direction)
        return result
    monkeypatch.setattr(Engine, '_scan', observe)
    result = run([row(1000+i, 200 if i<4 else 0) for i in range(15)])
    assert [d['selected'] for d in result['decisions']] == calls
    assert result['round_trips'] == 1
    events = result['execution_events']
    assert len(events) == 4
    assert all(e['ts'] > e['submitted_ts'] for e in events)
    assert [e['action'] for e in events] == ['open','open','reduce','reduce']
    assert all(e['reduce_only'] for e in events[2:])
    assert result['entropy_position'] == result['hedge_position'] == 0
    assert result['completed_net_pnl_usd'] == pytest.approx(2)
    assert result['total_liquidation_pnl_usd'] == pytest.approx(2)
    assert result['completed_cycles_per_hour'] == pytest.approx(3600/14)
    assert result['unhedged_seconds'] == 2
    assert not [r for r in caplog.records if r.levelname == 'ERROR']


def test_missing_future_ack_stays_unresolved_and_blocks_new_open():
    result = run([row(1000+i, 200) for i in range(3)])
    assert result['round_trips'] == 0
    assert result['pending_order'] is not None
    assert result['execution_events'][-1]['unresolved']
    assert result['entropy_position'] < 0
    assert result['hedge_position'] == 0
    assert result['max_unhedged_usd'] > 0
    assert result['completed_net_pnl_usd'] == 0


def test_partial_hedge_uses_engine_recovery_without_reopening_first_leg(caplog):
    result = run([row(1000+i, 200) for i in range(5)], hedge_fill_fraction=.5)
    assert result['partial_fills'] > 0
    assert result['round_trips'] == 0
    assert result['pending_order'] is not None
    assert len([e for e in result['execution_events'] if e['phase']=='first']) == 1
    assert not [r for r in caplog.records if 'execute failed' in r.message]


def test_zero_first_fill_does_not_create_hedge_or_realized_cycle():
    result = run([row(1000+i, 200) for i in range(15)], first_fill_fraction=0)
    assert result['zero_fills'] > 0
    assert not [e for e in result['execution_events'] if e['phase']=='hedge']
    assert result['round_trips'] == result['turnover_usd'] == 0


def test_cost_pressure_and_late_hedge_loss_remain_in_all_capital_result():
    rows = [row(1000+i, 200 if i<4 else 0) for i in range(15)]
    base = simulate_engine(rows, config())
    stressed = simulate_engine(rows, config(), slippage_bps=1)
    assert stressed['total_liquidation_pnl_usd'] < base['total_liquidation_pnl_usd']
    tail = run([row(1000,200), row(1001,200),row(1002,200),row(1003,200,h=110)],
               hedge_delay_sec=1)
    assert tail['round_trips'] == 0
    assert tail['net_pnl_usd'] == 0
    assert tail['open_liquidation_pnl_usd'] < 0


def test_high_fees_rejected_by_actual_engine_economics():
    cfg = config()
    cfg.entropy.fee_bps = cfg.hedge.fee_bps = 100
    result = simulate_engine([row(1000+i, 30) for i in range(8)], cfg)
    assert result['order_attempts'] == 0
    assert all(d['selected'] is None for d in result['decisions'])


def test_invalid_timestamp_order_is_rejected():
    with pytest.raises(ValueError, match='chronological'):
        run([row(2),row(1)])
