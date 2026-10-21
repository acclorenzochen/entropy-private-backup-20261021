from entropy_arb.v3.core import Settings
from decimal import Decimal
import pytest
import json

from tools.v3_opportunities import (calibrate, compare_parameters, episodes,
                                    replay, write_report)


def test_continuous_trigger_is_one_episode_and_reset_allows_next():
    def frame(seq, basis):
        return {'session_id': 's', 'sample_seq': seq,
                'capture_wall_ts': float(seq), 'reference_bps': '0',
                'quotes': [{'units': 10, 'buy_entropy_basis_bps': str(basis),
                            'sell_entropy_basis_bps': '0'}]}

    found = episodes([frame(1, -4), frame(2, -5), frame(3, 0),
                      frame(4, -4)], Settings())
    assert [row['sample_seq'] for row in found] == [1, 4]
    assert [row['onset_status'] for row in found] == [
        'left_censored', 'observed_crossing']
    assert found[0]['trigger_ts'] is None
    assert found[1]['trigger_ts'] == 4
    assert all(row['censored'] for row in found)


def test_future_quote_requires_same_units_and_sampling_window():
    def frame(seq, ts, units, basis):
        return {'session_id': 's', 'sample_seq': seq, 'capture_wall_ts': ts,
                'reference_bps': '0', 'quotes': [{
                    'units': units, 'buy_entropy_basis_bps': str(basis),
                    'sell_entropy_basis_bps': '0'}]}

    found = episodes([frame(1, 0, 10, -4), frame(2, .1, 10, -5),
                      frame(3, .25, 20, -6)], Settings())
    assert found[0]['future_100ms_bps'] == '-5'
    assert found[0]['future_250ms_bps'] is None
    assert found[0]['horizons']['250ms']['status'] == 'right_censored'


def frame(seq, ts, basis, *, session='s', units=10, mono=None, pair='p',
          reference='0'):
    row = {'pair_id': pair, 'session_id': session, 'sample_seq': seq,
           'capture_wall_ts': ts, 'reference_bps': reference,
           'quotes': [{'units': units, 'buy_entropy_basis_bps': str(basis),
                       'sell_entropy_basis_bps': '0'}]}
    if mono is not None:
        row['capture_mono_ns'] = int(mono * 1e9)
    return row


def test_horizon_is_interval_censored_without_exact_sample():
    found = episodes([frame(1, 0, -4), frame(2, .08, -5),
                      frame(3, .2, -6)], Settings())
    first = found[0]
    assert first['future_100ms_bps'] is None
    assert first['horizons']['100ms'] == {
        'status': 'interval_censored', 'basis_bps': None,
        'interval_seconds': [.08, .2],
        'bracket_basis_bps': ['-5', '-6']}
    assert first['horizons']['250ms']['status'] == 'right_censored'
    assert first['horizons']['250ms']['censor_reason'] == 'session_end'


def test_window_reports_observed_extrema_wait_interval_and_censoring():
    found = episodes([frame(1, 0, -4), frame(2, 1, -6),
                      frame(3, 2, -4), frame(4, 3, -2),
                      frame(5, 4, -1), frame(6, 5, -1)], Settings())
    window = found[0]['windows']['5s']
    assert window['status'] == 'covered'
    assert window['max_reversion_bps'] == '3'
    assert window['max_adverse_move_bps'] == '2'
    assert window['reversion_wait_interval_seconds'] == [2, 3]
    assert window['not_reverted_at_end'] is False
    assert found[0]['windows']['30s']['status'] == 'right_censored'
    assert found[0]['windows']['30s']['not_reverted_at_end'] is False


def test_session_quantity_and_missing_sequence_censor_paths():
    rows = [frame(1, 0, -4, units=10), frame(2, 1, -5, units=20),
            frame(3, 2, -6, units=10), frame(5, 3, -7, units=10),
            frame(1, .1, -8, session='other', units=10)]
    found = episodes(rows, Settings())
    assert [(row['session_id'], row['units'], row['sample_seq']) for row in found] == [
        ('s', 10, 1), ('s', 20, 2), ('s', 10, 3), ('s', 10, 5),
        ('other', 10, 1)]
    assert all(row['onset_status'] == 'left_censored' for row in found)
    assert found[0]['horizons']['1s']['status'] == 'right_censored'
    assert found[0]['horizons']['1s']['censor_reason'] == 'quantity_unavailable'
    assert found[2]['windows']['5s']['status'] == 'right_censored'
    assert found[2]['windows']['5s']['censor_reason'] == 'missing_sequence'


def test_mono_clock_is_used_within_session_when_wall_clock_moves_back():
    rows = [frame(1, 100, -4, mono=1), frame(2, 99, -5, mono=1.1)]
    found = episodes(rows, Settings())
    assert len(found) == 1
    assert found[0]['clock_source'] == 'capture_mono_ns'
    assert found[0]['future_100ms_bps'] == '-5'


def test_unusable_reference_does_not_trigger_episode():
    stale = frame(1, 0, -4)
    stale['reference_usable'] = False
    fresh = frame(2, 1, -4)
    fresh['reference_usable'] = True
    fresh['reference_source'] = 'live_reanchored'
    found = episodes([stale, fresh], Settings())
    assert [row['sample_seq'] for row in found] == [2]
    assert found[0]['reference_source'] == 'live_reanchored'


def test_movement_classification_uses_same_quantity_depth():
    before = frame(1, 0, 0)
    after = frame(2, 1, -4)
    for row, entropy_price in ((before, '100'), (after, '99')):
        row['entropy'] = {'asks': [[entropy_price, 10]]}
        row['lighter'] = {'bids': [['100', 10]]}
    found = episodes([before, after], Settings())
    assert found[0]['movement_source'] == 'entropy'
    assert found[0]['entropy_move_bps'] > 0
    assert found[0]['lighter_move_bps'] == 0


def test_report_keeps_parameter_choice_unavailable_without_fills(tmp_path):
    snapshots = [frame(1, 0, -4), frame(2, 5, -4)]
    found = episodes(snapshots, Settings())
    write_report(tmp_path, snapshots, found, Settings())
    comparison = json.loads((tmp_path / 'parameter_comparison.json').read_text())
    candidate = json.loads((tmp_path / 'candidate_config.json').read_text())
    assert comparison['status'] == 'INSUFFICIENT_DATA'
    assert comparison['left_censored_count'] == 2
    assert comparison['observed_crossing_count'] == 0
    assert comparison['coverage']['windows']['5s']['not_reverted_rate_among_covered'] is None
    assert candidate['recommendation'] is None
    assert candidate['current_exit_min_gross_profit_bps'] == '2.5'


def test_replay_report_preserves_detailed_execution_attribution(tmp_path):
    attribution = {'attempt_count': 7, 'paired_groups': []}
    (tmp_path / 'execution_attribution.json').write_text(json.dumps(attribution))
    write_report(tmp_path, [], [], Settings())
    assert json.loads((tmp_path / 'execution_attribution.json').read_text()) == attribution
    assert (tmp_path / 'replay_calibration.json').exists()


def replay_frame(seq, ts, entropy_px):
    return {
        'kind': 'v3_market_snapshot', 'pair_id': 'p', 'session_id': 's',
        'sample_seq': seq, 'capture_wall_ts': ts,
        'capture_mono_ns': int(ts * 1e9), 'valid': True,
        'mid_basis_bps': str((entropy_px / 100 - 1) * 10000),
        'entropy': {'bids': [[str(entropy_px), 100]],
                    'asks': [[str(entropy_px), 100]],
                    'book_revision': str(seq)},
        'lighter': {'bids': [['100', 100]], 'asks': [['100', 100]],
                    'book_revision': str(seq)},
        'instrument': {'quantity_step': '0.01', 'entropy_tick': '0.01',
                       'hedge_tick': '0.01', 'entropy_fee_bps': '0.5',
                       'hedge_fee_bps': '0.5', 'entropy_min_base': '0.1',
                       'lighter_min_base': '0.1', 'entropy_min_quote': '1',
                       'lighter_min_quote': '1'},
        'quotes': [{'units': 10, 'buy_entropy_basis_bps': '-10',
                    'sell_entropy_basis_bps': '-10'}],
    }


def replay_calibration(fraction=1., latency=.1, *, unknown=False):
    return {'groups': {f'{phase}:{venue}': [
        {'fraction': None if unknown else fraction,
         'latency_seconds': latency, 'status': 'terminal'}]
        for phase in ('open', 'close', 'repair')
        for venue in ('entropy', 'hedge')},
        'funding_per_notional_hour': 0., 'ready': False}


def test_shadow_replay_completes_one_cycle_without_double_counting_costs():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                         99.9 if i < 10 else 100.2) for i in range(13)]
    cfg = Settings(cold_seconds=1., cold_samples=2, warm_seconds=1.,
                   warm_samples=2)
    result = replay(rows, cfg, replay_calibration(), scenario='neutral',
                    unit_override=10)
    assert result['complete_cycle_count'] == 1
    assert result['incomplete_cycle_count'] == 0
    assert result['order_count'] == 4
    assert result['matched_units'] == 20
    cycle = result['complete_cycles'][0]
    assert cycle['result_kind'] == 'clean'
    assert Decimal(cycle['profit_entry_notional_usd']) == Decimal('9.995')
    assert Decimal(cycle['fees_usd']) == Decimal('0.0020005')
    assert float(cycle['funding_usd']) == 0
    assert Decimal(cycle['pnl_usd']) == Decimal('0.0279995')


def test_unknown_order_is_unresolved_and_not_counted_as_zero_fill():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9)
            for i in range(12)]
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2),
                    replay_calibration(unknown=True), scenario='neutral',
                    unit_override=10)
    assert result['complete_cycle_count'] == 0
    assert result['incomplete_cycle_count'] == 1
    assert result['incomplete_cycles'][0]['phase'] == 'UNKNOWN'
    assert result['incomplete_cycles'][0]['unknown_order_count'] == 1
    assert result['incomplete_cycles'][0]['mark_is_realized'] is False


def test_partial_first_leg_hedges_only_observed_fill_and_finishes_cycle():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                         99.9 if i < 10 else 100.2) for i in range(13)]
    calibration = replay_calibration()
    calibration['groups']['open:entropy'][0]['fraction'] = .5
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2,
                                   max_notional_usd=Decimal('30')),
                    calibration, scenario='neutral', unit_override=20)
    assert result['complete_cycle_count'] == 1
    assert result['incomplete_cycle_count'] == 0
    assert result['matched_units'] == 20  # 10 entry + 10 exit, not planned 20.
    assert result['order_count'] == 4
    assert Decimal(result['complete_cycles'][0]['profit_entry_notional_usd']) == Decimal('9.995')


def test_signed_funding_enters_cash_once_without_extra_execution_cost():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                         99.9 if i < 10 else 100.2) for i in range(13)]
    cfg = Settings(cold_seconds=1., cold_samples=2)
    plain = replay(rows, cfg, replay_calibration(), scenario='neutral',
                   unit_override=10)
    funded_calibration = replay_calibration()
    funded_calibration['funding_per_notional_hour'] = .01
    funded = replay(rows, cfg, funded_calibration, scenario='neutral',
                    unit_override=10)
    before = Decimal(plain['complete_cycles'][0]['pnl_usd'])
    after = Decimal(funded['complete_cycles'][0]['pnl_usd'])
    funding = Decimal(funded['complete_cycles'][0]['funding_usd'])
    assert funding > 0
    assert after - before == funding


@pytest.mark.parametrize('exit_price,complete', [('99.9249874', 0), ('99.9249875', 1)])
def test_replay_gross_profit_boundary_ignores_large_funding(exit_price, complete):
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                         99.9 if i < 10 else Decimal(exit_price)) for i in range(13)]
    profile = replay_calibration()
    profile['funding_per_notional_hour'] = 100
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2), profile,
                    scenario='neutral', unit_override=10)
    assert result['complete_cycle_count'] == complete
    assert result['order_count'] == (4 if complete else 2)
    assert result['execution_model'] == 'legacy_sequential_exit'
    assert result['live_execution_parity'] is False


def test_replay_partial_close_keeps_first_matched_opening_notional(monkeypatch):
    import tools.v3_opportunities as opportunity_tool
    decide = opportunity_tool.exit_decision
    remaining_decisions = []

    def record_decision(*args, **kwargs):
        decision = decide(*args, **kwargs)
        if args[4] == 10:
            remaining_decisions.append(decision)
        return decision

    monkeypatch.setattr(opportunity_tool, 'exit_decision', record_decision)
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                         99.9 if i < 10 else Decimal('99.925') if i < 12
                         else Decimal('99.92')) for i in range(16)]
    profile = replay_calibration()
    profile['groups']['close:entropy'][0]['fraction'] = .5
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2,
                                   max_notional_usd=Decimal('30')),
                    profile, scenario='neutral', unit_override=20)
    assert result['complete_cycle_count'] == 0
    assert result['order_count'] == 4
    remaining = result['incomplete_cycles'][0]
    assert remaining['phase'] == 'OPEN'
    assert (remaining['entropy_units'], remaining['hedge_units']) == (10, -10)
    assert Decimal(remaining['profit_entry_notional_usd']) == Decimal('19.99')
    # .0045 gross exceeds 2.5bp of the remaining half, but not the locked entry.
    assert remaining_decisions[-1].executable_gross_usd == Decimal('.0045')
    assert remaining_decisions[-1].profit_entry_notional_usd == Decimal('19.99')
    assert not remaining_decisions[-1].allowed


@pytest.mark.parametrize('repair_price,exit_price,complete', [
    ('99.85', '99.93', 0), ('99.95', '99.88', 1)])
def test_replay_pre_open_repair_price_cash_enters_basis_exit(
        monkeypatch, repair_price, exit_price, complete):
    import tools.v3_opportunities as opportunity_tool
    draw = opportunity_tool._draw
    hedge_attempts = 0

    def repair_draw(calibration, phase, venue, identity, scenario):
        nonlocal hedge_attempts
        sample = draw(calibration, phase, venue, identity, scenario)
        if phase == 'open' and venue == 'hedge':
            hedge_attempts += 1
            sample['fraction'] = .5 if hedge_attempts == 1 else 0.
        elif phase == 'repair' and venue == 'hedge':
            sample['fraction'] = 0.
        return sample

    monkeypatch.setattr(opportunity_tool, '_draw', repair_draw)
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                         99.9 if i < 11 else Decimal(repair_price) if i == 11
                         else Decimal(exit_price)) for i in range(16)]
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2,
                                   max_notional_usd=Decimal('30'),
                                   entropy_orders_per_min=20, hedge_orders_per_min=20),
                    replay_calibration(), scenario='neutral', unit_override=20)
    assert result['complete_cycle_count'] == complete
    cycles = result['complete_cycles'] if complete else result['incomplete_cycles']
    assert Decimal(cycles[0]['profit_entry_notional_usd']) == Decimal('9.995')
    if complete:
        assert cycles[0]['repair_used'] is True
        assert cycles[0]['exit_reasons'] == ['gross_price_profit_exit']
    else:
        assert cycles[0]['phase'] == 'OPEN'
        assert result['exit_reason_counts'] == {}


def test_zero_fill_respects_request_budget_and_keeps_unfinished_separate():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9)
            for i in range(40)]
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2,
                                   entropy_orders_per_min=3),
                    replay_calibration(fraction=0.), scenario='neutral',
                    unit_override=10)
    assert result['order_count'] <= 3
    assert result['blocked_counts']['request_budget'] > 0
    assert Decimal(result['realized_pnl_usd']) == 0


def test_failed_hedge_attempts_pair_completion_before_reduce_only():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9)
            for i in range(10)]
    calibration = replay_calibration()
    calibration['groups']['open:hedge'][0]['fraction'] = 0.
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2),
                    calibration, scenario='neutral', unit_override=10)
    assert result['complete_cycle_count'] == 0
    assert result['realized_cycle_count'] == 0
    assert result['incomplete_cycles'][0]['phase'] == 'OPEN'
    repair = [row for row in result['action_trace']
              if row.get('phase') == 'REPAIR_PAIR']
    assert len(repair) == 1
    assert repair[0]['venue'] == 'hedge'
    assert repair[0]['reduce_only'] is False


def test_calibration_excludes_future_attempts_and_report_remains_insufficient():
    attempts = [{'purpose': 'open_first', 'venue': 'entropy',
                 'requested_units': 10, 'result_units': 10,
                 'submit_ts': 1., 'result_ts': 1.1},
                {'purpose': 'open_first', 'venue': 'entropy',
                 'requested_units': 10, 'result_units': 10,
                 'submit_ts': 100., 'result_ts': 100.1},
                {'purpose': 'open_first', 'venue': 'entropy',
                 'requested_units': 10, 'result_units': 10,
                 'submit_ts': 9., 'result_ts': 10.1}]
    for row in attempts:
        row['order_state'] = 'TERMINAL'
        row['terminal_result_ts'] = row['result_ts']
    found = calibrate(attempts, [], cutoff_ts=10.)
    assert found['counts']['open:entropy'] == 1
    assert found['ready'] is False
    comparison = compare_parameters([], Settings(), attempts, [])
    assert comparison['status'] == 'INSUFFICIENT_DATA'
    assert comparison['recommendation'] is None


def test_terminal_quote_uses_book_at_or_before_modeled_fill_time():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                         99.9 if i == 4 else 100.2) for i in range(6)]
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2),
                    replay_calibration(), scenario='neutral', unit_override=10)
    assert result['incomplete_cycle_count'] == 1
    assert Decimal(result['incomplete_cycles'][0]['known_cash_usd']) < 0
    assert result['terminal_asof_age_seconds_p50'] <= .1 + 1e-9


def test_parameter_pipeline_runs_chronological_stages_with_ready_inputs(monkeypatch):
    import tools.v3_opportunities as opportunity_tool
    metadata = replay_frame(1, 1., 100)['instrument']
    metadata['effective_max_notional_usd'] = '15'
    rows = [{'capture_wall_ts': float(i), 'valid': True,
             'pair_id': 'p', 'session_id': 's', 'sample_seq': i + 1,
             'config_hash': 'h',
             'instrument': metadata, 'quotes': [{'units': 10}]}
            for i in range(12001)]
    def fake_calibration(*args, **kwargs):
        return {'groups': {}, 'ready': True, 'funding_per_notional_hour': 0.,
                'counts': {}, 'funding_cycle_count': 20, 'missing': {},
                'observed_fee_bps_by_venue': {},
                'observed_fee_counts_by_venue': {}}
    calls = []
    def fake_replay(rows, *args, **kwargs):
        calls.append((rows, kwargs))
        return {'realized_pnl_per_hour_usd': '1', 'complete_cycle_count': 20,
                'incomplete_cycle_count': 0, 'incomplete_fraction': 0,
                'complete_cycles': [], 'incomplete_cycles': []}
    monkeypatch.setattr(opportunity_tool, 'calibrate', fake_calibration)
    monkeypatch.setattr(opportunity_tool, 'replay', fake_replay)
    cfg = Settings(open_first_rooms_bps=(Decimal('3'),) * 3,
                   close_first_rooms_bps=(Decimal('3'),) * 3)
    result = compare_parameters(rows, cfg, [], [])
    assert result['status'] == 'SHADOW_SCENARIOS'
    assert [item['stage'] for item in result['experiments'][:3]] == [
        'reference_half_life_seconds'] * 3
    assert any(item['direction'] == 'short_entropy'
               for item in result['experiments'])
    assert all(item['stage'] != 'exit_reversion_bps' for item in result['experiments'])
    assert all(item['settings']['exit_min_gross_profit_bps'] == '2.5'
               for item in result['experiments'])
    assert result['recommendation'] is None
    assert result['holdout_evaluation_count'] == 1
    assert all(max(row['capture_wall_ts'] for row in data) < 8400
               for data, _ in calls[:-1])
    assert calls[-1][0] == rows
    assert calls[-1][1]['score_start_ts'] == 8400
    assert all(data[0]['capture_wall_ts'] == 0 and kw['trade_start_ts'] == 2400
               for data, kw in calls)
    assert any(item['label'] == 'live_dynamic' for item in result['experiments'])


def test_fixed_quantity_path_survives_candidate_grid_change():
    rows = [replay_frame(i + 1, i * .5, 99.9) for i in range(4)]
    for i, row in enumerate(rows):
        row['reference_bps'] = '0'
        row['quotes'] = [{'units': 85 if i in (0, 3) else 84}]
    found = episodes(rows, Settings())
    original = [row for row in found if row['units'] == 85
                and row['direction'] == 'buy_entropy']
    assert len(original) == 1
    assert original[0]['future_1s_bps'] == '-10.000'
    assert original[0]['windows']['5s']['censor_reason'] == 'session_end'


def test_reader_keeps_gap_marker_and_legacy_invalid_in_source_order(tmp_path):
    from tools.v3_opportunities import load_snapshots, _clocked_sessions
    rows = [replay_frame(1, 1, 100),
            {'kind': 'v3_market_snapshot', 'pair_id': 'p', 'session_id': 's', 'valid': False},
            replay_frame(2, 1.5, 100),
            {'kind': 'v3_observer_gap', 'pair_id': 'p', 'session_id': 's'},
            replay_frame(3, 2, 100),
            dict(replay_frame(4, 2.5, 100), continuity_break=True)]
    path = tmp_path / 'snapshots.jsonl'
    path.write_text('\n'.join(json.dumps(row) for row in rows))
    loaded = load_snapshots([path])
    assert loaded == rows
    segments = list(_clocked_sessions(loaded, 2.))
    assert [len(item[3]) for item in segments] == [1, 1, 1, 1]
