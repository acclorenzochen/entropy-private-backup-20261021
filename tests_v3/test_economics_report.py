"""Offline reporting regressions: no adapters, accounts or production journals."""
from decimal import Decimal
import json
import sys

from entropy_arb.v3.economics import canonical_attempts, summarize_attempts, summarize_system
from entropy_arb.v3.journal import Journal
from tools.v3_economics import main, read_rows


def test_unknown_zero_and_positive_are_not_terminal_or_resolved():
    steps = [{'step_key': str(i), 'order_ref': str(i), 'cycle_id': 'c'} for i in range(4)]
    orders = [{'order_ref': str(i), 'state': state, 'filled_units': units}
              for i, (state, units) in enumerate([
                  ('UNKNOWN', 0), ('UNKNOWN', 3), ('TERMINAL', 0), ('TERMINAL', 4)])]
    telemetry = [{**step, 'result_units': 0, 'resolve_start_ts': 1,
                  'terminal_observed_ts': 3, 'submit_start_ts': 2} for step in steps]
    report = summarize_attempts(telemetry, live_steps=steps, orders=orders)
    assert report['unknown_attempts'] == 2
    assert report['terminal_zero_attempts'] == 1
    assert report['submitted_attempt_count'] == 0
    assert report['attempts'][1]['known_cumulative_units'] == 3
    assert report['attempts'][0]['terminal_observed_ts'] is None
    assert report['attempts'][2]['had_unknown'] is None


def test_safe_group_and_trigger_override_telemetry_and_unknown_history_survives():
    rows = canonical_attempts([
        {'step_key': 'c:close_first:0', 'order_ref': 'r', 'execution_group_id': 'wrong'}], [
        {'step_key': 'c:close_first:0', 'order_ref': 'r', 'execution_group_id': 'safe',
         'trigger_ts': 5, 'trigger_mono_ns': 10, 'trigger_clock_session_id': 's',
         'abort_reason': 'budget_denied'}], [
        {'order_ref': 'r', 'state': 'TERMINAL', 'filled_units': 0,
         'had_unknown': 1, 'terminal_recorded_ts': 8}])
    assert rows[0]['execution_group_id'] == 'safe'
    assert rows[0]['group_trigger_ts'] == 5
    assert rows[0]['had_unknown'] == 1
    assert rows[0]['terminal_at'] == 8
    assert rows[0]['terminal_at_source'] == 'canonical_recorded_wall'


def test_cross_restart_mono_requires_event_session_not_row_session():
    row = {'step_key': 's', 'order_state': 'TERMINAL', 'result_units': 1,
           'clock_session_id': 'old', 'submit_start_ts': 1, 'terminal_observed_ts': 3,
           'submit_start_mono_ns': 100, 'terminal_observed_mono_ns': 200,
           'terminal_clock_session_id': 'new'}
    latency = summarize_attempts([row])['groups']
    metric = next(iter(latency.values()))['submit_to_terminal_latency_ms']
    assert metric['clock_counts'] == {'wall_utc': 1}
    assert Decimal(metric['p50']) == 2000
    row['submit_start_clock_session_id'] = 'new'
    metric = next(iter(summarize_attempts([row])['groups'].values()))['submit_to_terminal_latency_ms']
    assert metric['clock_counts'] == {'monotonic_ns': 1}


def test_eight_zero_fill_cycles_are_closed_attempts_only():
    cycles = [{'cycle_id': str(i), 'phase': 'DONE', 'started_ts': 0, 'updated_ts': 999}
              for i in range(8)]
    canonical = [{'cycle_id': str(i), 'completed_ts': i + 1,
                  'accounting_final': 1, 'pnl_usd': '-0.01', 'result_kind': 'zero_fill'}
                 for i in range(8)]
    report = summarize_system([], cycles, canonical, window_start_ts=0, window_end_ts=10)
    assert report['closed_attempts'] == report['zero_fill_cycles'] == 8
    assert report['paired_round_trips'] == report['completed_cycle_count'] == 0
    assert Decimal(report['finalized_cycle_pnl_usd']) == Decimal('-.08')


def test_completion_window_is_half_open_and_cash_costs_not_deducted_twice():
    cycles = [{'cycle_id': str(i), 'phase': 'DONE', 'started_ts': 0,
               'updated_ts': 1000} for i in range(3)]
    canonical = [{'cycle_id': str(i), 'completed_ts': ts, 'accounting_final': 1,
                  'pnl_usd': '2', 'result_kind': 'repair'} for i, ts in enumerate([9, 10, 20])]
    economics = [{'cycle_id': '1', 'accounting_final': 1,
                  'actual_fees_usd': '.1', 'actual_funding_usd': '.2'}]
    report = summarize_system(economics, cycles, canonical,
        [{'used_ts': 10, 'venue': 'entropy', 'weight': 1},
         {'used_ts': 20, 'venue': 'entropy', 'weight': 2}],
        window_start_ts=10, window_end_ts=20)
    assert report['finalized_cycle_count'] == 1
    assert report['finalized_cycle_pnl_usd'] == '2'
    assert report['request_budget_by_venue_and_kind']['entropy:unknown']['weight'] == 1
    assert report['boundary_end_cycle_ids'] == ['2']
    assert report['account_equity_change_usd'] is None


def test_legacy_completion_proxy_excluded_and_active_mtm_requires_exact_evidence():
    cycles = [{'cycle_id': 'old', 'phase': 'DONE', 'started_ts': 0, 'updated_ts': 2},
              {'cycle_id': 'active', 'phase': 'OPEN', 'started_ts': 1, 'updated_ts': 3}]
    canonical = [{'cycle_id': 'old', 'accounting_final': 1, 'pnl_usd': '9'}]
    valuation = {'observed_ts': 10, 'source': 'offline_fixture',
                 'positions': [{'cycle_id': 'active', 'authoritative': True,
                                'entropy_units': 1, 'hedge_units': -1, 'cash_net_usd': '.5'}],
                 'market': {'quantity_step': '1', 'entropy': {'bids': [['101', 1]]},
                            'hedge': {'asks': [['100', 1]]}}}
    report = summarize_system([], cycles, canonical, window_start_ts=0,
                              window_end_ts=10, valuation=valuation)
    assert report['finalized_cycle_count'] == 0
    assert report['measurement_quality']['missing_completion_cycle_ids'] == ['old']
    assert report['open_position_valuation_usd'] == '1.5'
    valuation['observed_ts'] = 9
    assert summarize_system([], cycles, canonical, window_start_ts=0,
        window_end_ts=10, valuation=valuation)['open_position_valuation_usd'] is None


def test_paired_roundtrip_requires_authoritative_flat_and_terminal_orders():
    cycles = [{'cycle_id': 'c', 'phase': 'DONE', 'started_ts': 0, 'updated_ts': 2,
               'entropy_units': 0, 'hedge_units': 0, 'position_evidence_authoritative': 1}]
    canonical = [{'cycle_id': 'c', 'completed_ts': 2, 'pnl_usd': '1', 'accounting_final': 1}]
    orders = [{'cycle_id': 'c', 'state': 'TERMINAL', 'reduce_only': 0,
               'venue': venue, 'side': side, 'filled_units': 3}
              for venue, side in [('entropy', 'buy'), ('hedge', 'sell')]]
    assert summarize_system([], cycles, canonical, orders=orders)['paired_round_trips'] == 1
    orders[1]['state'] = 'UNKNOWN'
    assert summarize_system([], cycles, canonical, orders=orders)['paired_round_trips'] == 0


def test_report_cli_reads_schema8_without_writes_and_emits_quality(tmp_path, monkeypatch, capsys):
    path = tmp_path / 'fixture.sqlite'
    journal = Journal(path, identity='test')
    journal.close()
    before = path.read_bytes()
    assert len(read_rows(path)) == 6
    output = tmp_path / 'report'
    monkeypatch.setattr(sys, 'argv', ['v3_economics', '--journal', str(path),
                                     '--output-dir', str(output)])
    assert main() == 0
    assert path.read_bytes() == before
    report = json.loads(capsys.readouterr().out)
    assert report['effective_manifest'] is None
    assert (output / 'measurement_quality.json').exists()
    assert (output / 'execution_latency.csv').exists()


def test_two_close_groups_use_safe_trigger_not_first_cycle_trigger():
    rows = []
    steps = []
    for group, trigger in [('g1', 1), ('g2', 10)]:
        for venue, purpose, end in [('entropy', 'close_first', trigger + 1),
                                    ('hedge', 'close_hedge', trigger + 2)]:
            key = f'{group}:{purpose}:0'
            rows.append({'step_key': key, 'cycle_id': 'c', 'order_state': 'TERMINAL',
                         'purpose': purpose, 'venue': venue, 'result_units': 1,
                         'side': 'sell' if venue == 'entropy' else 'buy',
                         'submit_start_ts': trigger + .5, 'terminal_observed_ts': end})
            steps.append({'step_key': key, 'execution_group_id': group,
                          'trigger_ts': trigger, 'cycle_id': 'c', 'venue': venue})
    latency = summarize_attempts(rows, live_steps=steps,
        cycle_economics=[{'cycle_id': 'c', 'exit_trigger_ts': 1}])['latency']
    assert Decimal(latency['exit_trigger_to_paired_terminal_ms']['p50']) == 2000


def test_window_censors_late_terminal_and_preserves_missing_analytics():
    report = summarize_attempts([], live_steps=[{'step_key': 's', 'order_ref': 'r'}],
        orders=[{'order_ref': 'r', 'state': 'TERMINAL', 'filled_units': 0,
                 'submitted_ts': 10, 'terminal_recorded_ts': 20}],
        window_start_ts=10, window_end_ts=20)
    assert report['submitted_attempt_count'] == 1
    assert report['terminal_zero_attempts'] == 0
    assert report['unknown_attempts'] == 1
    assert report['missing_economics_step_keys'] == ['s']


def test_fee_events_and_provisional_positions_remain_separate_from_cash():
    report = summarize_system([], [
        {'cycle_id': 'a', 'phase': 'OPEN', 'started_ts': 0, 'updated_ts': 15},
        {'cycle_id': 'b', 'phase': 'DONE', 'started_ts': 0, 'updated_ts': 12}], [
        {'cycle_id': 'a', 'accounting_final': 0, 'pnl_usd': None},
        {'cycle_id': 'b', 'accounting_final': 1, 'pnl_usd': '1', 'completed_ts': 12}],
        window_start_ts=10, window_end_ts=20, fee_events=[
            {'event_ts': 10, 'fee_usd': '.1'}, {'event_ts': 20, 'fee_usd': '9'}])
    assert report['provisional_cycle_ids'] == ['a']
    assert report['active_cycle_ids'] == ['a']
    assert report['window_fee_events_usd'] == '0.1'
    assert report['finalized_cycle_pnl_usd'] == '1'
