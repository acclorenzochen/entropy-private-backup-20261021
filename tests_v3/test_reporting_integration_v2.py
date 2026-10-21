"""R5/F11 integration against fake venues and temporary journals only."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path

import pytest

from entropy_arb.v3.core import Side
from entropy_arb.v3.economics import summarize_attempts, summarize_system
from tests_v3.test_runtime import complete_background_audit, setup_runtime, seed_close_cycle
from tools.v3_economics import read_rows


@pytest.fixture(autouse=True)
def isolated_runtime_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _report(journal):
    path = Path(journal.db.execute('PRAGMA database_list').fetchone()[2])
    rows, cycles, attempts, canonical, requests, steps = read_rows(path)
    return summarize_attempts(attempts, cycles, rows, steps)


def test_parallel_partial_exit_groups_keep_the_original_safe_trigger(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    try:
        async def scenario():
            await runtime.initialize()
            await runtime._maybe_open(runtime._market(), D('0'))
            active = journal.active_live_cycle()
            entropy.fill_fractions = [30 / abs(active['entropy_units'])]
            hedge.fill_fractions = [30 / abs(active['entropy_units'])]
            entropy.set_book(99.60, 99.61)
            hedge.set_book(99.99, 100.00)
            await runtime._resume_cycle(active)
            first_trigger = journal.economics_rows()[0]['exit_trigger_ts']
            active = journal.active_live_cycle()
            assert active and active['phase'] == 'BASIS_EXIT'
            entropy.set_book(99.50, 99.51)
            await runtime._resume_cycle(active)
            assert journal.economics_rows()[0]['exit_trigger_ts'] == first_trigger
            firsts = [row for row in journal.live_steps(active['cycle_id'])
                      if ':close_pair_entropy_' in row['step_key']]
            assert len(firsts) == 2
            assert firsts[0]['execution_group_id'] != firsts[1]['execution_group_id']
            assert firsts[0]['trigger_ts'] == firsts[1]['trigger_ts'] == first_trigger
            assert firsts[0]['trigger_basis_bps'] == firsts[1]['trigger_basis_bps']
            report = _report(journal)
            for step in firsts:
                row = next(row for row in report['attempts'] if row['step_key'] == step['step_key'])
                assert row['group_trigger_ts'] == step['trigger_ts']
                assert row['original_signal_ts'] == step['trigger_ts']
                children = [child for child in report['attempts']
                            if child.get('execution_group_id') == step['execution_group_id']
                            and child.get('venue') == 'hedge']
                assert children
                assert all(child.get('parent_step_key') is None for child in children)
                assert all(child['group_trigger_ts'] == step['trigger_ts'] for child in children)
            assert report['latency']['exit_trigger_to_paired_terminal_ms']['count'] == 2
        asyncio.run(scenario())
    finally:
        journal.close()


def test_parallel_paired_terminal_uses_the_later_entropy_result(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    try:
        async def scenario():
            await runtime.initialize()
            await runtime._maybe_open(runtime._market(), D('0'))
            active = journal.active_live_cycle()
            entropy.set_book(99.60, 99.61)
            hedge.set_book(99.99, 100.00)
            hedge_done = asyncio.Event()
            send_entropy, send_hedge = entropy.send_taker, hedge.send_taker

            async def fast_hedge(**kwargs):
                result = await send_hedge(**kwargs)
                hedge_done.set()
                return result

            async def slow_entropy(**kwargs):
                await asyncio.wait_for(hedge_done.wait(), timeout=1)
                await asyncio.sleep(.002)
                return await send_entropy(**kwargs)

            entropy.send_taker, hedge.send_taker = slow_entropy, fast_hedge
            await runtime._resume_cycle(active)
            report = _report(journal)
            closed = [row for row in report['attempts'] if row.get('execution_path') == 'parallel']
            entropy_result = next(row for row in closed if row['venue'] == 'entropy')
            hedge_result = next(row for row in closed if row['venue'] == 'hedge')
            assert entropy_result['terminal_observed_ts'] > hedge_result['terminal_observed_ts']
            expected = (entropy_result['terminal_observed_mono_ns']
                        - entropy_result['group_trigger_mono_ns']) / 1_000_000
            metric = report['latency']['exit_trigger_to_paired_terminal_ms']
            assert metric['count'] == 1 and float(metric['p50']) == pytest.approx(expected)
            # Parallel submissions have no first-leg-to-hedge handoff delay.
            assert report['latency']['entropy_terminal_to_hedge_submit_ms']['count'] == 1
            assert runtime.telemetry_errors == 0
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('missing_terminal_session', [False, True])
def test_unknown_resolve_across_sessions_never_subtracts_process_mono(tmp_path, missing_terminal_session):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    original_resolve = entropy.resolve_order
    entropy.unknown_once = True

    async def unresolved(order_ref, timeout=None):
        return {'status': 'timeout', 'filled_base': 0, 'unresolved': True,
                'order_ref': order_ref, 'err': None}

    try:
        async def scenario():
            await runtime.initialize()
            journal.create_live_cycle(batch_id='batch', cycle_id='unknown',
                direction='short_entropy', planned_units=85, reference_bps=D('0'),
                entry_basis_bps=D('4'), started_ts=1)
            entropy.resolve_order = unresolved
            runtime.clock_session_id = 'before-restart'
            kwargs = dict(purpose='open_first', attempt=0, venue_key='entropy',
                          side=Side.SELL, units=85, reduce_only=False,
                          room_bps=D('3'), hard_cap_bps=D('10'),
                          next_phase='OPEN_HEDGE', completion=False)
            result = await runtime._step(journal.active_live_cycle(), **kwargs)
            assert not result.terminal
            runtime._flush_telemetry()
            entropy.resolve_order = original_resolve
            runtime.clock_session_id = 'after-restart'
            result = await runtime._step(journal.active_live_cycle(), **kwargs)
            assert result.terminal
            # _step deliberately defers positive first-leg analytics until handoff.
            if runtime._deferred_open_telemetry is not None:
                runtime._deferred_open_telemetry()
                runtime._deferred_open_telemetry = None
            runtime._flush_telemetry()
            assert len(entropy.sent) == 1
            order = journal.cycle_orders('unknown')[0]
            assert order['had_unknown'] == 1
            step = journal.economics_steps('unknown')[0]
            assert step['clock_session_id'] == 'before-restart'
            assert step['terminal_clock_session_id'] == 'after-restart'
            if missing_terminal_session:
                journal.db.execute('UPDATE execution_economics_steps SET terminal_clock_session_id=NULL')
            report = _report(journal)
            metric = next(iter(report['groups'].values()))['submit_to_terminal_latency_ms']
            assert metric['clock_counts'].get('monotonic_ns', 0) == 0
            assert report['attempts'][0]['had_unknown'] == 1
            assert report['attempts'][0]['order_outcome'] == 'terminal_positive'
            assert step['runtime_consumed_ts'] >= step['terminal_observed_ts'], (
                'final terminal consumption must replace the first UNKNOWN consumption; '
                'otherwise its time is incorrectly associated with the new terminal session')
        asyncio.run(scenario())
    finally:
        journal.close()


def test_canonical_child_with_numbered_purpose_retains_parent_and_execution_label(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, exit_fast_path_enabled=True)
    entropy.fill_fractions = [30 / 85]
    original_send = entropy.send_taker

    async def receipt(**kwargs):
        response = await original_send(**kwargs)
        response.update(status='filled', confirmation_source='exchange_http')
        return response

    entropy.send_taker = receipt
    try:
        async def scenario():
            await runtime.initialize()
            position_reads = []
            for venue in (entropy, hedge):
                original_position = venue.fetch_position

                async def position(key=venue.key, original=original_position):
                    position_reads.append(key)
                    return await original()

                venue.fetch_position = position
            cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                                     phase='CLOSE_FIRST')
            await runtime._close(cycle, new_exit=True)
            report = _report(journal)
            first = next(row for row in report['attempts'] if row.get('purpose') == 'close_first_0')
            child = next(row for row in report['attempts']
                         if str(row.get('purpose', '')).startswith('close_hedge_'))
            # Canonical fills now drive every basis handoff. The retained
            # 'fallback' reporting label does not imply a REST position wait.
            assert first['execution_path'] == child['execution_path'] == 'fallback'
            assert position_reads == []
            assert first['result_units'] == child['result_units'] == 30
            assert first['confirmation_source'] == 'exchange_http'
            assert child['parent_step_key'] == first['step_key']
            assert child['execution_group_id'] == first['execution_group_id']
            assert journal.order(first['order_ref'])['filled_units'] == 30
            assert journal.order(child['order_ref'])['filled_units'] == 30
        asyncio.run(scenario())
    finally:
        journal.close()


def test_canonical_terminal_receipt_uses_its_own_session_and_final_source():
    from entropy_arb.v3.economics import canonical_attempts

    telemetry = [{'step_key': 's', 'order_ref': 'r', 'clock_session_id': 'old',
                  'terminal_clock_session_id': 'runtime-session',
                  'confirmation_source': 'timeout', 'result_status': 'timeout',
                  'submit_start_ts': 1, 'submit_start_mono_ns': 10,
                  'runtime_consumed_ts': 4, 'runtime_consumed_mono_ns': 900}]
    canonical = [{'order_ref': 'r', 'state': 'TERMINAL', 'filled_units': 3,
                  'had_unknown': 1, 'first_unknown_ts': 2, 'submitted_ts': 1,
                  'terminal_received_ts': 3, 'terminal_received_mono_ns': 100,
                  'terminal_received_clock_session_id': 'new',
                  'terminal_recorded_ts': 3.1, 'terminal_source': 'exchange_http',
                  'terminal_status': 'filled'}]
    rows = canonical_attempts(telemetry, orders=canonical)
    assert rows[0]['terminal_at'] == 3
    assert rows[0]['terminal_at_source'] == 'canonical_terminal_received'
    assert rows[0]['terminal_received_clock_session_id'] == 'new'
    assert rows[0]['terminal_status'] == 'filled'
    assert rows[0]['confirmation_source'] == 'exchange_http'
    report = summarize_attempts(rows)
    assert report['submitted_attempt_count'] == 1
    metric = report['latency']['submit_to_terminal_received_ms']
    assert metric['clock_counts'] == {'wall_utc': 1}
    assert D(metric['p50']) == 2000
    consumed = report['latency']['terminal_received_to_runtime_consumed_ms']
    assert consumed['clock_counts'] == {'wall_utc': 1}
    assert D(consumed['p50']) == 1000


def test_completion_time_stays_immutable_through_accounting_reconcile(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    try:
        async def scenario():
            await runtime.initialize()
            await runtime._maybe_open(runtime._market(), D('0'))
            cycle = journal.active_live_cycle()
            entropy.set_book(99.60, 99.61)
            await runtime._resume_cycle(cycle)
            pending_audit = journal.active_live_cycle()
            assert pending_audit is not None
            assert pending_audit['entropy_units'] == pending_audit['hedge_units'] == 0
            assert not pending_audit['position_evidence_authoritative']
            assert not journal.pending()
            assert journal.db.execute('SELECT completed_ts FROM cycles').fetchone()[0] is None
            await complete_background_audit(runtime)
            assert journal.active_live_cycle() is None
            completed = dict(journal.db.execute('SELECT * FROM cycles').fetchone())
            stamp = completed['completed_ts']
            assert stamp is not None
            assert not completed['accounting_final']
            assert not journal.fee_state(cycle['cycle_id'])['accounting_evidence_complete']
            journal.update_live_cycle(cycle['cycle_id'], updated_ts=stamp + 1000)
            finalized_pnl = await runtime._reconcile_cycle_accounting(cycle['cycle_id'])
            assert finalized_pnl is not None
            fees = journal.fee_state(cycle['cycle_id'])
            assert fees['fees_complete'] and fees['accounting_evidence_complete']
            orders = journal.cycle_orders(cycle['cycle_id'])
            assert all(order['state'] == 'TERMINAL' for order in orders)
            assert all(fees['order_fees'][order['order_ref']]['fee_covered_units']
                       == order['filled_units'] for order in orders if order['filled_units'])
            accounted = dict(journal.db.execute('SELECT * FROM cycles').fetchone())
            assert accounted['completed_ts'] == stamp
            assert accounted['complete_accounting'] == accounted['accounting_final'] == 1
            assert D(accounted['pnl_usd']) == finalized_pnl
            journal.finish_cycle(cycle['cycle_id'], kind=completed['result_kind'],
                net_pnl_usd=finalized_pnl, positions_verified_flat=True,
                all_orders_terminal=True)
            canonical = dict(journal.db.execute('SELECT * FROM cycles').fetchone())
            assert canonical['completed_ts'] == stamp
            assert canonical['complete_accounting'] == canonical['accounting_final'] == 1
            assert D(canonical['pnl_usd']) == finalized_pnl
            live = dict(journal.db.execute('SELECT * FROM live_cycles').fetchone())
            assert live['phase'] == 'DONE' and live['position_evidence_authoritative']
            report = summarize_system([], [live], [canonical],
                                      window_start_ts=stamp, window_end_ts=stamp + 1)
            assert report['finalized_cycle_count'] == 1
        asyncio.run(scenario())
    finally:
        journal.close()
