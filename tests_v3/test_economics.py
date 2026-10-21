import asyncio
from decimal import Decimal as D
import json
import sqlite3
import time

import pytest

from entropy_arb.v3.core import Direction, Side
from entropy_arb.v3.economics import (
    actual_reversion_bps, break_even_reversion_bps, directional_dislocation,
    execution_cost_bps, summarize, summarize_attempts, summarize_system,
    weighted_vwap,
)
from entropy_arb.v3.journal import Intent, Journal, Result
from tests_v3.test_runtime import setup_runtime


@pytest.fixture(autouse=True)
def isolated_runtime_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def test_directional_dislocation_and_reversion_are_symmetric():
    assert directional_dislocation(Direction.LONG_ENTROPY, D('-4'), D('1')) == D('5')
    assert directional_dislocation(Direction.SHORT_ENTROPY, D('6'), D('1')) == D('5')
    assert actual_reversion_bps(Direction.LONG_ENTROPY, D('-4'), D('2')) == D('6')
    assert actual_reversion_bps(Direction.SHORT_ENTROPY, D('6'), D('0')) == D('6')


def test_execution_cost_has_one_worse_is_positive_convention():
    assert execution_cost_bps(Side.BUY, D('101'), D('100')) == D('100')
    assert execution_cost_bps(Side.SELL, D('99'), D('100')) > 0
    assert execution_cost_bps(Side.BUY, D('99'), D('100')) < 0
    assert execution_cost_bps(Side.SELL, D('101'), D('100')) < 0


def test_system_summary_keeps_repair_provisional_and_active():
    rows = [
        {'result_kind': 'repair', 'accounting_final': 1,
         'finalized_pnl_usd': '-0.04', 'actual_fees_usd': '0.01',
         'actual_funding_usd': '0'},
        {'result_kind': 'clean', 'accounting_final': 0,
         'finalized_pnl_usd': None},
    ]
    cycles = [
        {'cycle_id': 'a', 'phase': 'DONE', 'started_ts': 0, 'updated_ts': 1800},
        {'cycle_id': 'b', 'phase': 'OPEN', 'started_ts': 1800, 'updated_ts': 3600},
    ]
    result = summarize_system(rows, cycles)
    assert result['finalized_cycle_pnl_usd'] == '-0.04'
    assert result['finalized_cycle_pnl_per_hour_usd'] == '-0.04'
    assert result['provisional_cycle_count'] == 1
    assert result['active_cycle_ids'] == ['b']
    assert result['account_equity_change_usd'] is None
    canonical = [
        {'result_kind': 'repair', 'accounting_final': 1, 'pnl_usd': '-0.04'},
        {'result_kind': 'clean', 'accounting_final': 1, 'pnl_usd': '0.01'},
    ]
    all_cycles = summarize_system(rows[:1], cycles, canonical)
    assert all_cycles['finalized_cycle_count'] == 2
    assert all_cycles['finalized_cycle_pnl_usd'] == '-0.03'
    assert all_cycles['economics_coverage_count'] == 1
    assert all_cycles['known_actual_fees_usd'] == '0.01'
    attempts = summarize_attempts([
        {'purpose': 'open_first', 'venue': 'entropy', 'side': 'buy',
         'result_units': 0},
        {'purpose': 'close_first', 'venue': 'entropy', 'side': 'sell',
         'result_units': None},
    ])
    assert attempts['attempt_count'] == 2


def test_newer_journal_is_rejected_before_any_schema_change(tmp_path):
    path = tmp_path / 'future.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        db.executemany('INSERT INTO metadata VALUES (?, ?)', [
            ('version', '1'), ('identity', 'pair'),
            ('economics_schema_version', '999')])
    before = path.read_bytes()
    with pytest.raises(ValueError, match='newer'):
        Journal(path, identity='pair')
    assert path.read_bytes() == before


def test_weighted_vwap_and_break_even_use_real_notional():
    vwap, quantity = weighted_vwap([
        {'qty': '1', 'price': '100'}, {'qty': '3', 'price': '102'}])
    assert vwap == D('101.5') and quantity == D('4')
    assert break_even_reversion_bps(D('4'), D('-0.02'), D('100')) == D('6')


def _summary_row(index: int, *, kind='clean'):
    value = D(index % 10 + 3)
    return {
        'result_kind': kind, 'accounting_final': 1,
        'clean_summary_eligible': int(kind == 'clean'),
        'entry_dislocation_bps': str(value), 'first_leg_cost_bps': '1',
        'signal_to_first_fill_decay_bps': '0.5', 'hedge_cost_bps': '1.5',
        'first_fill_to_hedged_decay_bps': '0.25',
        'total_entry_decay_bps': '0.75', 'actual_fees_bps': '1.8',
        'actual_funding_bps': '0', 'actual_reversion_bps': '5',
        'break_even_reversion_bps': '6',
        'finalized_pnl_usd': '0.01' if index % 2 else '-0.01',
    }


def test_summary_sample_gates_and_recovery_is_excluded():
    assert summarize([_summary_row(i) for i in range(19)])['sample_status'] == 'INSUFFICIENT'
    early = summarize([_summary_row(i) for i in range(20)] + [_summary_row(0, kind='repair')])
    assert early['sample_status'] == 'EARLY_EMPIRICAL'
    assert early['clean_count'] == 20 and early['recovery_count'] == 1
    ready = summarize([_summary_row(i) for i in range(50)])
    assert ready['sample_status'] == 'EMPIRICAL_READY'
    assert ready['entry_dislocation_buckets']
    assert all(bucket['count'] >= 5 for bucket in ready['entry_dislocation_buckets'].values())


def test_latency_summary_waits_for_ten_attempts_before_dominant_segment():
    rows = []
    for index in range(10):
        row = _summary_row(index)
        row.update({
            'signal_ts': '1', 'position_check_start_ts': '1.01',
            'position_check_end_ts': '1.11', 'cycle_persist_start_ts': '1.12',
            'cycle_persist_end_ts': '1.13', 'benchmark_start_ts': '1.13',
            'benchmark_end_ts': '1.14', 'intent_prepare_start_ts': '1.14',
            'intent_prepare_end_ts': '1.15', 'submit_start_ts': '1.16',
            'first_fill_exchange_ts': '1.46', 'first_fill_observed_ts': '1.50',
        })
        rows.append(row)
    assert summarize(rows[:9])['latency']['dominant_segment'] is None
    report = summarize(rows)
    assert report['latency']['open_attempt_count'] == 10
    assert report['latency']['dominant_segment'] == 'position_check_ms'
    assert report['latency']['exchange_clock']['local_to_exchange_latency_ms'] is None


def test_additive_schema_upgrades_existing_v1_journal(tmp_path):
    path = tmp_path / 'old.sqlite'
    db = sqlite3.connect(path)
    db.executescript('''
        CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO metadata VALUES ('version','1'),('identity','pair-account');
    ''')
    db.close()
    journal = Journal(path, identity='pair-account')
    names = {row[0] for row in journal.db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {'cycle_economics', 'execution_economics_steps'} <= names
    columns = {row[1] for row in journal.db.execute(
        'PRAGMA table_info(cycle_economics)')}
    assert {
        'exit_trigger_basis_bps', 'paired_basis_at_entropy_fill',
        'actual_paired_exit_basis', 'interleg_decay_bps',
        'total_exit_execution_decay_bps',
    } <= columns
    assert journal.db.execute("SELECT value FROM metadata WHERE key="
                              "'economics_schema_version'").fetchone()[0] == '8'
    journal.close()


def test_schema7_migrates_pending_schema5_and_reopens_without_loss(tmp_path):
    path = tmp_path / 'active.sqlite'
    journal = Journal(path, identity='pair-account')
    journal.ensure_batch('b', max_position_cycles=2, loss_budget_usd=D('3'))
    journal.create_live_cycle(
        batch_id='b', cycle_id='c', direction='long_entropy',
        planned_units=85, reference_bps=D('0'), entry_basis_bps=D('-4'),
        started_ts=time.time())
    journal.ensure_live_step(
        step_key='c:close_first_0:0', cycle_id='c', order_ref='ref',
        venue='entropy', side='sell', requested_units=85, reduce_only=True)
    intent = Intent('ref', 'c', 'entropy', Side.SELL, 85, D('100'), True)
    journal.prepare(intent)
    journal.mark_submitting(intent.order_ref)
    journal.record(intent.order_ref, Result(False, 0, note='timeout'))
    journal.close()

    with sqlite3.connect(path) as db:
        for table in ('live_steps', 'execution_economics_steps'):
            db.execute(f'ALTER TABLE {table} DROP COLUMN execution_group_id')
            db.execute(f'ALTER TABLE {table} DROP COLUMN parent_step_key')
        db.execute("UPDATE metadata SET value='5' WHERE key='economics_schema_version'")

    for reopened in range(2):
        journal = Journal(path, identity='pair-account')
        assert journal.db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        assert journal.active_live_cycle()['cycle_id'] == 'c'
        assert journal.pending()[0]['order_ref'] == 'ref'
        step = journal.live_step('c:close_first_0:0')
        assert step['parent_step_key'] is None
        assert step['execution_group_id'] == (
            None if reopened == 0 else 'c:close_first_0:0')
        journal.ensure_live_step(
            step_key='c:close_first_0:0', cycle_id='c', order_ref='ref',
            venue='entropy', side='sell', requested_units=85,
            reduce_only=True, execution_group_id='c:close_first_0:0')
        assert journal.db.execute(
            "SELECT value FROM metadata WHERE key='economics_schema_version'"
            ).fetchone()[0] == '8'
        journal.close()


@pytest.mark.parametrize(
    ('direction', 'trigger', 'at_entropy_fill', 'actual'),
    [('long_entropy', D('10'), D('8'), D('6')),
     ('short_entropy', D('-10'), D('-8'), D('-6'))],
)
def test_exit_execution_metrics_are_direction_normalized(
        tmp_path, direction, trigger, at_entropy_fill, actual):
    journal = Journal(tmp_path / f'{direction}.sqlite', identity=direction)
    journal.ensure_batch('b', max_position_cycles=2, loss_budget_usd=D('3'))
    journal.create_live_cycle(
        batch_id='b', cycle_id='c', direction=direction, planned_units=10,
        reference_bps=D('0'), entry_basis_bps=D('-4'), started_ts=1)
    journal.record_exit_trigger(
        'c', trigger_ts=10, reason='test', trigger_basis_bps=trigger,
        reversion_bps=D('1'), executable_pnl_usd=D('0'))
    journal.record_close_submit('c', venue='entropy', submit_ts=10.1)
    journal.record_close_fill(
        'c', venue='entropy', fill_ts=10.2, fill_px=D('100'), fill_units=10,
        paired_basis_at_entropy_fill=at_entropy_fill)
    journal.record_close_submit('c', venue='hedge', submit_ts=10.25)
    journal.record_close_fill(
        'c', venue='hedge', fill_ts=10.35, fill_px=D('100'), fill_units=10)
    journal.finalize_close_execution(
        'c', entropy_fill_ts=10.2, entropy_fill_px=D('100'),
        entropy_fill_units=10, lighter_fill_ts=10.35,
        lighter_fill_px=D('100'), lighter_fill_units=10,
        actual_paired_exit_basis=actual, updated_ts=11)
    row = journal.economics_rows()[0]
    assert D(row['trigger_to_entropy_fill_ms']) == D('200')
    assert D(row['entropy_fill_to_lighter_submit_ms']) == D('50')
    assert D(row['lighter_submit_to_fill_ms']) == D('100')
    assert D(row['trigger_to_entropy_fill_decay_bps']) == D('2')
    assert D(row['interleg_decay_bps']) == D('2')
    assert D(row['total_exit_execution_decay_bps']) == D('4')
    journal.close()


def test_cycle_economics_is_atomic_and_restart_idempotent(tmp_path):
    path = tmp_path / 'journal.sqlite'
    journal = Journal(path, identity='pair-account')
    journal.ensure_batch('b', max_position_cycles=2, loss_budget_usd=D('3'))
    journal.create_live_cycle(
        batch_id='b', cycle_id='c', direction='long_entropy', planned_units=10,
        reference_bps=D('1'), entry_basis_bps=D('-4'), started_ts=1,
        planned_notional_usd=D('15'))
    assert journal.economics_rows()[0]['entry_dislocation_bps'] == '5'
    journal.update_max_reversion('c', D('2'), observed_ts=2)
    journal.update_max_reversion('c', D('1'), observed_ts=3)
    journal.close()
    journal = Journal(path, identity='pair-account')
    assert len(journal.economics_rows()) == 1
    assert journal.economics_rows()[0]['max_favorable_reversion_bps'] == '2'
    journal.update_max_reversion('c', D('3'), observed_ts=4, force=True)
    assert journal.economics_rows()[0]['max_favorable_reversion_bps'] == '3'
    journal.close()


def test_full_cycle_finalizes_same_canonical_pnl_and_is_summary_eligible(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        assert active and active['phase'] == 'OPEN'
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(active)

    asyncio.run(scenario())
    economics = journal.economics_rows()[0]
    cycle = journal.db.execute('SELECT * FROM cycles').fetchone()
    assert economics['accounting_final'] == 1
    assert economics['clean_summary_eligible'] == 1
    assert economics['telemetry_incomplete'] == 0
    assert economics['finalized_pnl_usd'] == cycle['pnl_usd']
    assert economics['actual_entry_basis_bps'] is not None
    assert economics['actual_exit_basis_bps'] is not None
    for name in (
            'exit_trigger_ts', 'exit_trigger_basis_bps',
            'entropy_close_submit_ts', 'entropy_close_fill_ts',
            'entropy_close_fill_px', 'entropy_close_fill_units',
            'paired_basis_at_entropy_fill', 'lighter_close_submit_ts',
            'lighter_close_fill_ts', 'lighter_close_fill_px',
            'lighter_close_fill_units', 'actual_paired_exit_basis',
            'trigger_to_entropy_fill_ms',
            'entropy_fill_to_lighter_submit_ms',
            'lighter_submit_to_fill_ms',
            'trigger_to_entropy_fill_decay_bps', 'interleg_decay_bps',
            'total_exit_execution_decay_bps'):
        assert economics[name] is not None
    assert D(economics['actual_paired_exit_basis']) == D(
        economics['actual_exit_basis_bps'])
    assert D(economics['total_exit_execution_decay_bps']) == (
        D(economics['trigger_to_entropy_fill_decay_bps'])
        + D(economics['interleg_decay_bps']))
    assert economics['break_even_reversion_bps'] is not None
    assert len(journal.economics_steps(cycle['cycle_id'])) == 4
    for name in ('position_check_start_ts', 'position_check_end_ts',
                 'decision_ts',
                 'cycle_persist_start_ts', 'cycle_persist_end_ts',
                 'benchmark_start_ts', 'benchmark_end_ts',
                 'intent_prepare_start_ts', 'intent_prepare_end_ts',
                 'submit_start_ts', 'submit_result_ts'):
        assert economics[name] is not None
    assert economics['pre_submit_ts'] is None
    assert economics['live_entry_basis_bps'] is not None
    assert economics['live_entry_basis_source'] in {'canonical_order_vwap', 'exchange_fills'}
    assert economics['exit_anchor_basis_bps'] is not None
    assert economics['anchor_error_bps'] is not None
    journal.close()


def test_cycle_costs_are_replaced_by_authoritative_exchange_fills(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        entropy.fee_fills[0]['price'] = float(entropy.fee_fills[0]['price']) + .02
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(active)

    asyncio.run(scenario())
    first = next(row for row in journal.economics_steps(
        journal.economics_rows()[0]['cycle_id']) if row['purpose'] == 'open_first')
    economics = journal.economics_rows()[0]
    expected = execution_cost_bps(
        Side(first['side']), D(first['actual_fill_vwap']), D(first['top_px']))
    assert D(first['execution_cost_bps']) == expected
    assert D(economics['first_leg_cost_bps']) == expected
    assert first['actual_fill_vwap'] != first['provisional_avg_px']
    journal.close()


def test_exit_interleg_metrics_only_measure_first_close_wave(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        opened_units = abs(active['entropy_units'])
        entropy.fill_fractions = [30 / opened_units]
        hedge.fill_fractions = [30 / opened_units]
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(active)
        partial = journal.active_live_cycle()
        assert abs(partial['entropy_units']) == opened_units - 30
        entropy.set_book(99.70, 99.71)
        hedge.set_book(100.09, 100.10)
        await runtime._resume_cycle(partial)

    asyncio.run(scenario())
    row = journal.economics_rows()[0]
    assert row['accounting_final'] == 1
    assert row['entropy_close_fill_units'] == 30
    assert row['lighter_close_fill_units'] == 30
    assert D(row['actual_paired_exit_basis']) != D(row['actual_exit_basis_bps'])
    journal.close()


def test_first_fill_mark_uses_result_time_market_not_submit_market(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    original = entropy.send_taker

    async def move_market_after_fill(**kwargs):
        result = await original(**kwargs)
        entropy.set_book(100.20, 100.21)
        return result

    async def scenario():
        await runtime.initialize()
        entropy.send_taker = move_market_after_fill
        await runtime._maybe_open(runtime._market(), D('0'))

    asyncio.run(scenario())
    row = journal.economics_rows()[0]
    step = next(item for item in journal.economics_steps(row['cycle_id'])
                if item['purpose'] == 'open_first')
    assert step['result_paired_executable_basis_bps'] is not None
    assert step['result_paired_executable_basis_bps'] != step['paired_executable_basis_bps']
    assert row['first_fill_mark_basis_bps'] == step['result_paired_executable_basis_bps']
    assert row['first_fill_observed_ts'] is not None
    assert row['first_fill_exchange_ts'] is None
    journal.close()


def test_large_fill_confirmation_lag_excludes_clean_summary(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        entropy.fee_fills[0]['ts'] = time.time() + 1.5
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(active)

    asyncio.run(scenario())
    row = journal.economics_rows()[0]
    assert D(row['first_fill_observation_lag_ms']) > 1000
    assert row['clean_summary_eligible'] == 0
    assert row['telemetry_incomplete'] == 1
    journal.close()


def test_unknown_cover_marker_cannot_be_classified_clean(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        journal.ensure_live_step(
            step_key=f"{active['cycle_id']}:unknown_cover_hedge:0",
            cycle_id=active['cycle_id'], order_ref='classification-marker',
            venue='hedge', side='buy', requested_units=1, reduce_only=False)
        journal.db.execute('''UPDATE live_steps SET applied=1,result_units=0,
            cash_delta_usd='0',accounting_final=1
            WHERE order_ref='classification-marker' ''')
        journal.mark_recovery_used(active['cycle_id'], updated_ts=time.time())
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(active)

    asyncio.run(scenario())
    cycle = journal.db.execute('SELECT * FROM cycles').fetchone()
    economics = journal.economics_rows()[0]
    assert cycle['result_kind'] == economics['result_kind'] == 'repair'
    assert economics['clean_summary_eligible'] == 0
    journal.close()


def test_pending_economics_is_retried_after_canonical_accounting(tmp_path, monkeypatch):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    original = journal.finalize_cycle_economics
    failed = False

    def fail_once(cycle_id, **values):
        nonlocal failed
        if values.get('result_kind') == 'clean' and not failed:
            failed = True
            raise OSError('transient telemetry write')
        return original(cycle_id, **values)

    async def scenario():
        await runtime.initialize()
        monkeypatch.setattr(journal, 'finalize_cycle_economics', fail_once)
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(active)
        assert journal.db.execute('SELECT accounting_final FROM cycles').fetchone()[0] == 1
        assert journal.economics_rows()[0]['accounting_final'] == 0
        monkeypatch.setattr(journal, 'finalize_cycle_economics', original)
        assert await runtime._reconcile_provisional_cycles()
        # Analytics retry is independent of the canonical new-entry gate.
        await runtime._reconcile_pending_economics()

    asyncio.run(scenario())
    assert journal.economics_rows()[0]['accounting_final'] == 1
    journal.close()


def test_mfe_keeps_exact_in_memory_high_until_forced_flush(tmp_path):
    runtime, _entropy, _hedge, journal = setup_runtime(tmp_path)
    journal.ensure_batch('batch', max_position_cycles=2, loss_budget_usd=D('3'))
    journal.create_live_cycle(
        batch_id='batch', cycle_id='mfe', direction='long_entropy',
        planned_units=10, reference_bps=D('0'), entry_basis_bps=D('-4'),
        started_ts=1)
    runtime._update_mfe('mfe', D('1.00'), observed_ts=10)
    runtime._update_mfe('mfe', D('1.02'), observed_ts=10.1)
    assert D(journal.economics_rows()[0]['max_favorable_reversion_bps']) == D('1.00')
    runtime._update_mfe('mfe', D('.90'), observed_ts=10.2, force=True)
    assert D(journal.economics_rows()[0]['max_favorable_reversion_bps']) == D('1.02')
    journal.close()


def test_telemetry_failure_does_not_delay_or_prevent_first_send(tmp_path, monkeypatch):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        monkeypatch.setattr(journal, 'record_execution_benchmark',
                            lambda **kwargs: (_ for _ in ()).throw(OSError('disk')))
        await runtime._maybe_open(runtime._market(), D('0'))
        assert len(entropy.sent) == len(hedge.sent) == 1
        cycle = journal.active_live_cycle()
        assert cycle and cycle['entropy_units'] == -cycle['hedge_units'] != 0

    asyncio.run(scenario())
    journal.close()


def test_telemetry_failure_after_first_fill_does_not_block_hedge(tmp_path, monkeypatch):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    original = journal.record_execution_benchmark

    def fail_after_first(**kwargs):
        if kwargs['purpose'] != 'open_first':
            raise OSError('telemetry unavailable')
        return original(**kwargs)

    async def scenario():
        await runtime.initialize()
        monkeypatch.setattr(journal, 'record_execution_benchmark', fail_after_first)
        await runtime._maybe_open(runtime._market(), D('0'))
        cycle = journal.active_live_cycle()
        assert cycle and cycle['entropy_units'] == -cycle['hedge_units'] != 0

    asyncio.run(scenario())
    journal.close()


@pytest.mark.parametrize(('side', 'a_entropy', 'b_entropy', 'e_fill',
                          'h_mark', 'h_fill'), [
    ('buy', '100', '101', '102', '100', '99'),
    ('sell', '100', '99', '98', '100', '101'),
])
def test_attempt_group_requotes_same_matched_quantity_with_adverse_sign(
        side, a_entropy, b_entropy, e_fill, h_mark, h_fill):
    opposite = 'sell' if side == 'buy' else 'buy'

    def quote(e_price, h_price):
        return json.dumps({
            'quantity_step': '0.1',
            'entropy': {'asks': [[e_price, 10]], 'bids': [[e_price, 10]],
                        'received_wall_ts': 1},
            'hedge': {'asks': [[h_price, 10]], 'bids': [[h_price, 10]],
                      'received_wall_ts': 1},
        })

    first = {
        'cycle_id': 'c', 'execution_group_id': 'g', 'step_key': 'e',
        'purpose': 'close_first', 'venue': 'entropy', 'side': side,
        'attempt': 0, 'requested_units': 10, 'result_units': 10,
        'actual_fill_vwap': e_fill, 'quantity_step': '0.1',
        'decision_quote_json': quote(a_entropy, '100'),
        'submit_quote_json': quote(b_entropy, '100'),
        'fill_mark_quote_json': quote(e_fill, h_mark),
        'fill_mark_time_source': 'terminal_observed_local',
        'original_signal_ts': 1, 'attempt_decision_ts': 1.1,
        'submit_start_ts': 1.2, 'http_post_start': 1.21,
        'terminal_observed_ts': 1.3, 'confirmation_source': 'exchange_http',
        'execution_path': 'fast',
    }
    hedge = {
        'cycle_id': 'c', 'execution_group_id': 'g', 'parent_step_key': 'e',
        'step_key': 'h', 'purpose': 'close_hedge', 'venue': 'hedge',
        'side': opposite, 'attempt': 0, 'requested_units': 10,
        'result_units': 10, 'actual_fill_vwap': h_fill,
        'quantity_step': '0.1', 'submit_start_ts': 1.31,
        'terminal_observed_ts': 1.4,
        'execution_path': 'fallback',
    }
    first['order_state'] = hedge['order_state'] = 'TERMINAL'
    report = summarize_attempts([first, hedge],
                                [{'cycle_id': 'c', 'direction': 'long_entropy'}])
    pair = report['paired_groups'][0]
    assert pair['measurement_quality'] == 'complete'
    assert pair['comparable_units'] == 10
    assert D(pair['a_to_b_adverse_bps']) > 0
    assert D(pair['b_to_c_adverse_bps']) > 0
    assert D(pair['c_to_d_adverse_bps']) > 0
    assert D(pair['entropy_execution_loss_usd']) == D('1')
    assert D(pair['hedge_execution_loss_usd']) == D('1')
    assert D(pair['total_execution_loss_usd']) == D('2')
    assert report['known_execution_loss_usd'] == '2.0'
    assert pair['execution_path'] == 'fast'
    assert report['paired_groups_by_first_leg_path']['fast']['complete'] == 1
    entropy_bucket = next(value for key, value in report['groups'].items()
                          if ':entropy:' in key)
    assert D(entropy_bucket['paired_execution_loss_attributed_usd']) == D('2')
    assert entropy_bucket['submit_to_terminal_latency_ms']['count'] == 1


def test_attempt_group_does_not_pair_unmatched_or_missing_depth():
    rows = [
        {'cycle_id': 'c', 'execution_group_id': 'g', 'step_key': 'e',
         'purpose': 'open_first', 'venue': 'entropy', 'side': 'buy',
         'result_units': 10, 'actual_fill_vwap': '100'},
        {'cycle_id': 'c', 'execution_group_id': 'g', 'step_key': 'h',
         'parent_step_key': 'e', 'purpose': 'open_hedge', 'venue': 'hedge',
         'side': 'sell', 'result_units': 5, 'actual_fill_vwap': '100'},
    ]
    for row in rows:
        row['order_state'] = 'TERMINAL'
    pair = summarize_attempts(rows)['paired_groups'][0]
    assert pair['matched_units'] == 5
    assert pair['comparable_units'] is None
    assert pair['d_basis_bps'] is None
    assert pair['missing_fields'] == ['unmatched_fill_units']

    sparse = json.dumps({
        'entropy': {'bids': [['100', 4]], 'asks': [['100', 4]]},
        'hedge': {'bids': [['100', 4]], 'asks': [['100', 4]]},
    })
    rows[0].update({'result_units': 10, 'quantity_step': '0.1',
                    'decision_quote_json': sparse,
                    'submit_quote_json': sparse,
                    'fill_mark_quote_json': sparse,
                    'fill_mark_time_source': 'terminal_observed_local'})
    rows[1].update({'result_units': 10, 'quantity_step': '0.1'})
    sparse_pair = summarize_attempts(rows)['paired_groups'][0]
    assert sparse_pair['matched_units'] == 10
    assert sparse_pair['a_basis_bps'] is None
    assert sparse_pair['b_basis_bps'] is None
    assert sparse_pair['c_basis_bps'] is None
    assert 'decision_quote_depth' in sparse_pair['missing_fields']


def test_latency_reports_missing_and_negative_samples():
    rows = [
        {'cycle_id': 'c', 'purpose': 'open_first', 'venue': 'entropy',
         'attempt_decision_ts': 2, 'submit_start_ts': 1},
        {'cycle_id': 'c', 'purpose': 'open_first', 'venue': 'entropy',
         'attempt_decision_ts': 2, 'submit_start_ts': None},
    ]
    measure = summarize_attempts(rows)['latency']['attempt_decision_to_submit_start_ms']
    assert measure['count'] == 0
    assert measure['missing_count'] == 1
    assert measure['negative_count'] == 1
    assert measure['missing_rate'] == '0.5'
    assert summarize_attempts(
        rows, live_steps=[{'step_key': 'unrecorded'}]
    )['missing_economics_step_keys'] == ['unrecorded']


def test_attempt_latency_prefers_mono_only_with_matching_session():
    first = {
        'cycle_id': 'c', 'step_key': 'e', 'execution_group_id': 'g',
        'purpose': 'close_first_0', 'venue': 'entropy', 'side': 'sell',
        'result_units': 10, 'clock_session_id': 'run-1',
        'original_signal_ts': 2, 'attempt_decision_ts': 1,
        'submit_start_ts': .9, 'http_post_start': .8,
        'terminal_observed_ts': .7,
        'attempt_decision_mono_ns': 1_100_000_000,
        'submit_start_mono_ns': 1_200_000_000,
        'http_post_start_mono_ns': 1_300_000_000,
        'terminal_observed_mono_ns': 1_400_000_000,
    }
    hedge = {
        'cycle_id': 'c', 'step_key': 'h', 'execution_group_id': 'g',
        'parent_step_key': 'e', 'purpose': 'close_hedge_0',
        'venue': 'hedge', 'side': 'buy', 'result_units': 10,
        'clock_session_id': 'run-1', 'original_signal_ts': 2,
        'attempt_decision_ts': .65, 'submit_start_ts': .6,
        'terminal_observed_ts': .5,
        'attempt_decision_mono_ns': 1_450_000_000,
        'submit_start_mono_ns': 1_500_000_000,
        'terminal_observed_mono_ns': 1_600_000_000,
        'authoritative_reconcile_complete_mono_ns': 1_700_000_000,
    }
    economics = [{
        'cycle_id': 'c', 'exit_trigger_mono_ns': 1_000_000_000,
        'exit_trigger_clock_session_id': 'run-1',
    }]
    for row in (first, hedge):
        row['order_state'] = 'TERMINAL'
        row['group_trigger_ts'] = 2
        row['group_trigger_mono_ns'] = 1_000_000_000
        for name in ('group_trigger', 'attempt_decision', 'submit_start',
                     'http_post_start', 'terminal_observed', 'authoritative_reconcile_complete'):
            row[f'{name}_clock_session_id'] = 'run-1'
    timing = summarize_attempts([first, hedge], cycle_economics=economics)['latency']
    assert D(timing['original_signal_to_attempt_decision_ms']['p50']) == D('275')
    assert D(timing['attempt_decision_to_submit_start_ms']['p50']) == D('75')
    assert D(timing['submit_start_to_http_post_start_ms']['p50']) == D('100')
    assert D(timing['http_post_start_to_terminal_observed_ms']['p50']) == D('100')
    assert D(timing['entropy_terminal_to_hedge_submit_ms']['p50']) == D('100')
    assert D(timing['exit_trigger_to_paired_terminal_ms']['p50']) == D('600')
    assert D(timing['paired_terminal_to_authoritative_reconcile_ms']['p50']) == D('100')
    assert timing['exit_trigger_to_paired_terminal_ms']['clock_counts'] == {
        'monotonic_ns': 1}
    assert timing['attempt_decision_to_submit_start_ms']['negative_count'] == 0

    hedge['clock_session_id'] = 'run-2'
    hedge['submit_start_clock_session_id'] = 'run-2'
    mismatched = summarize_attempts([first, hedge], cycle_economics=economics)['latency']
    assert mismatched['entropy_terminal_to_hedge_submit_ms']['clock_counts'] == {}
    assert mismatched['entropy_terminal_to_hedge_submit_ms']['negative_count'] == 1


def test_full_system_explicit_window_and_request_categories():
    result = summarize_system(
        [], [{'cycle_id': 'c', 'phase': 'DONE', 'started_ts': 0,
              'updated_ts': 60}],
        [{'cycle_id': 'c', 'result_kind': 'zero_fill',
          'accounting_final': 1, 'pnl_usd': '0', 'completed_ts': 60}],
        [{'venue': 'entropy', 'weight': 1, 'request_kind': 'close_first', 'used_ts': 1},
         {'venue': 'entropy', 'weight': 2, 'request_kind': None, 'used_ts': 2}],
        window_start_ts=0, window_end_ts=3600)
    assert result['window_basis'] == 'explicit'
    assert result['finalized_cycle_pnl_usd'] == '0'
    assert D(result['completed_cycles_per_hour']) == 0
    assert result['closed_attempts'] == 1
    assert result['request_budget_by_venue_and_kind']['entropy:close_first']['weight'] == 1
    assert result['request_budget_by_venue_and_kind']['entropy:unknown']['weight'] == 2
    assert result['account_equity_change_usd'] is None


def test_runtime_attempt_groups_have_requotable_matched_depth(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(active)

    asyncio.run(scenario())
    report = summarize_attempts(
        [dict(row) for row in journal.db.execute(
            'SELECT * FROM execution_economics_steps')],
        [dict(row) for row in journal.db.execute('SELECT * FROM live_cycles')],
        journal.economics_rows(), orders=[dict(row) for row in journal.db.execute('SELECT * FROM orders')])
    assert {row['phase'] for row in report['paired_groups']} == {'open', 'close'}
    assert all(row['measurement_quality'] == 'complete'
               for row in report['paired_groups'])
    assert all(row['comparable_units'] == row['matched_units']
               for row in report['paired_groups'])
    journal.close()
