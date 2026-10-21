from copy import deepcopy

import pytest

from entropy_arb.dashboard_api.v3_projection import Window, _cycle_duration, cycle_detail, cycle_facts, overview
from entropy_arb.dashboard_api.v3_profit import profit_overview


def snapshot():
    cycle = {'cycle_id': 'cycle', 'batch_id': 'batch', 'direction': 'long_entropy',
             'phase': 'DONE', 'started_ts': 90, 'entropy_units': 0, 'hedge_units': 0,
             'position_evidence_authoritative': 1, 'last_authoritative_ts': 155}
    orders = []
    telemetry = []
    for ref, venue, side, closing, actual, terminal in (
            ('e-open', 'entropy', 'buy', False, 101, 110),
            ('h-open', 'hedge', 'sell', False, 105, 120),
            ('e-close', 'entropy', 'sell', True, 150, 152),
            ('h-close', 'hedge', 'buy', True, 151, 154)):
        orders.append({'order_ref': ref, 'cycle_id': 'cycle', 'venue': venue,
                       'side': side, 'reduce_only': closing, 'state': 'TERMINAL',
                       'requested_units': 10, 'filled_units': 10, 'avg_px': '100',
                       'submitted_ts': terminal - 2, 'terminal_recorded_ts': terminal,
                       'terminal_received_ts': terminal - 1, 'fee_usd': None})
        telemetry.append({'step_key': ref, 'order_ref': ref, 'cycle_id': 'cycle',
                          'venue': venue, 'result_units': 10, 'actual_fill_vwap': '100',
                          'actual_fill_ts': actual, 'purpose': 'close' if closing else 'open'})
    journal = {'batches': [], 'cycles': [{'cycle_id': 'cycle', 'batch_id': 'batch',
               'completed_ts': 158, 'result_kind': 'clean', 'accounting_final': 0}],
               'live_cycles': [cycle], 'orders': orders, 'live_steps': [],
               'execution_economics_steps': telemetry, 'fee_fills': [],
               'cycle_economics': [{'cycle_id': 'cycle',
                                    'authoritative_reconcile_complete_ts': 156}]}
    return {'generated_at': 200, 'journal': journal, 'sources': {
        'live': {'data': {'cycle_id': 'cycle', 'start_ts': 90, 'source_ts': 199}},
        'live_diagnostics': {'data': None}}}


def projected(snap):
    window = Window('batch', 0, snap['generated_at'], 'Asia/Shanghai', 'batch')
    facts = cycle_facts(snap, window)
    listed = facts['items'][0]
    detailed = cycle_detail(snap, 'cycle')['cycle']
    fields = ('first_fill_at', 'flat_confirmed_at', 'hold_seconds', 'duration_state',
              'first_fill_confirmed_at', 'estimated_hold_seconds', 'confirmation_duration_state',
              'first_fill_missing_order_refs', 'has_confirmed_fill')
    for field in fields:
        assert listed[field] == detailed[field]
    profits = profit_overview(snap, window, facts)['cycles']
    if profits:
        for field in fields:
            assert listed[field] == profits[0][field]
    return listed


def test_holding_starts_at_first_actual_fill_and_ends_at_both_flat_confirmation():
    row = projected(snapshot())
    assert row['paired_round_trip'] is True
    assert row['paired_at'] == 120
    assert row['completed_at'] == 158
    assert row['first_fill_at'] == 101
    assert row['flat_confirmed_at'] == 156
    assert row['hold_seconds'] == 55
    assert row['duration_state'] == 'closed'
    assert row['accounting_final'] is False  # Trading closure does not require fee finality.


def test_active_holding_grows_from_actual_first_fill():
    snap = snapshot()
    live = snap['journal']['live_cycles'][0]
    live.update(phase='OPEN', entropy_units=10, hedge_units=-10, last_authoritative_ts=None)
    snap['journal']['orders'] = snap['journal']['orders'][:2]
    row = projected(snap)
    assert row['flat_confirmed_at'] is None
    assert row['duration_state'] == 'active'
    assert row['hold_seconds'] == 99
    snap['generated_at'] = 210
    assert projected(snap)['hold_seconds'] == 109


def test_missing_close_first_fill_timestamps_do_not_hide_verified_duration():
    snap = snapshot()
    for trace in snap['journal']['execution_economics_steps'][2:]:
        trace['actual_fill_ts'] = None
    row = projected(snap)
    assert row['first_fill_at'] == 101
    assert row['flat_confirmed_at'] == 156
    assert row['hold_seconds'] == 55
    assert row['duration_state'] == 'closed'


def test_cumulative_partial_order_snapshots_do_not_change_round_trip_or_order_counts():
    snap = snapshot()
    for order in list(snap['journal']['orders']):
        partial = {**order, 'filled_units': 4, 'state': 'UNKNOWN'}
        snap['journal']['orders'].insert(0, partial)
    snap['journal']['orders'].append(deepcopy(snap['journal']['orders'][-1]))
    row = projected(snap)
    assert row['paired_opened'] is True
    assert row['paired_round_trip'] is True
    assert row['actual_units'] == 10
    assert row['hold_seconds'] == 55
    facts = cycle_facts(snap, Window('batch', 0, 200, 'Asia/Shanghai', 'batch'))
    assert facts['kpis']['submitted_orders'] == 4
    assert row['opening_attempts_submitted'] == 2


@pytest.mark.parametrize('change', ['missing', 'quantity_mismatch', 'wrong_venue', 'missing_vwap', 'future'])
def test_incomplete_first_actual_fill_evidence_is_never_filled_from_terminal_time(change):
    snap = snapshot()
    trace = snap['journal']['execution_economics_steps'][0]
    if change == 'missing':
        trace['actual_fill_ts'] = None
    elif change == 'quantity_mismatch':
        trace['result_units'] = 5
    elif change == 'wrong_venue':
        trace['venue'] = 'hedge'
    elif change == 'missing_vwap':
        trace['actual_fill_vwap'] = None
    else:
        trace['actual_fill_ts'] = 201
    row = projected(snap)
    assert row['first_fill_at'] is None
    assert row['hold_seconds'] is None
    assert row['duration_state'] == 'unknown'


def test_complete_identified_fills_supply_first_time_and_duplicates_count_once():
    snap = snapshot()
    snap['journal']['execution_economics_steps'] = []
    fills = [{'cycle_id': 'cycle', 'order_ref': ref, 'venue': venue,
              'exchange_fill_key': key, 'units': units, 'price': '100', 'ts': ts}
             for ref, venue, key, units, ts in (
                 ('e-open', 'entropy', 'e-1', 4, 100), ('e-open', 'entropy', 'e-2', 6, 103),
                 ('h-open', 'hedge', 'h-1', 10, 105))]
    fills.append(deepcopy(fills[0]))
    snap['journal']['fee_fills'] = fills
    snap['journal']['orders'].append(deepcopy(snap['journal']['orders'][0]))
    row = projected(snap)
    assert row['first_fill_at'] == 100
    assert row['hold_seconds'] == 56


@pytest.mark.parametrize('change', ['partial', 'conflict', 'missing_date'])
def test_incomplete_or_conflicting_raw_fill_evidence_is_unknown(change):
    snap = snapshot()
    snap['journal']['execution_economics_steps'] = []
    fills = [{'cycle_id': 'cycle', 'order_ref': ref, 'venue': venue,
              'exchange_fill_key': ref, 'units': 10, 'price': '100', 'ts': ts}
             for ref, venue, ts in (('e-open', 'entropy', 100), ('h-open', 'hedge', 105))]
    if change == 'partial':
        fills[0]['units'] = 4
    elif change == 'conflict':
        fills.append({**fills[0], 'ts': 99})
    else:
        fills[0]['ts'] = None
    snap['journal']['fee_fills'] = fills
    row = projected(snap)
    assert row['first_fill_at'] is None
    assert row['hold_seconds'] is None


@pytest.mark.parametrize('change', ['unverified', 'missing_time', 'before_terminal', 'negative', 'nonflat', 'unknown_order'])
def test_missing_or_inconsistent_both_flat_confirmation_is_unknown(change):
    snap = snapshot()
    live = snap['journal']['live_cycles'][0]
    econ = snap['journal']['cycle_economics'][0]
    if change == 'unverified':
        live['position_evidence_authoritative'] = 0
    elif change == 'missing_time':
        live['last_authoritative_ts'] = None
        econ['authoritative_reconcile_complete_ts'] = None
    elif change in ('before_terminal', 'negative'):
        live['last_authoritative_ts'] = 153 if change == 'before_terminal' else 99
        econ['authoritative_reconcile_complete_ts'] = None
        if change == 'negative':
            for order in snap['journal']['orders']:
                order['terminal_received_ts'] = order['terminal_recorded_ts'] = None
    elif change == 'nonflat':
        live['entropy_units'] = 1
    else:
        snap['journal']['orders'][-1]['state'] = 'UNKNOWN'
    row = projected(snap)
    assert row['first_fill_at'] == 101
    assert row['flat_confirmed_at'] is None
    assert row['hold_seconds'] is None
    assert row['duration_state'] == 'unknown'


def test_duration_is_independent_of_legacy_completed_ts_and_round_trip_definition():
    snap = snapshot()
    snap['journal']['cycles'][0]['completed_ts'] = None
    row = projected(snap)
    assert row['paired_round_trip'] is False
    assert row['hold_seconds'] == 55
    assert row['duration_state'] == 'closed'


@pytest.mark.parametrize('cycle_id', ['cycle', 'another-cycle', None])
def test_cycle_details_only_bind_matching_historical_diagnostic(cycle_id):
    snap = snapshot()
    snap['sources']['live_diagnostics']['data'] = {'cycle_id': cycle_id, 'reason': 'test diagnostic'}
    diagnostic = cycle_detail(snap, 'cycle')['diagnostic']
    assert (diagnostic is not None) == (cycle_id == 'cycle')
    if diagnostic is not None:
        assert diagnostic['cycle_id'] == 'cycle'


def test_overview_does_not_attach_another_cycles_diagnostic():
    snap = snapshot()
    snap['sources']['live_diagnostics']['data'] = {'cycle_id': 'other', 'reason': 'unrelated history'}
    data = overview(snap, Window('batch', 0, 200, 'Asia/Shanghai', 'batch'))
    assert data['diagnostic']['source'] == 'live_status_summary'
    assert 'unrelated history' not in str(data['diagnostic'])


def test_missing_one_exchange_leg_preserves_local_confirmation_and_estimate():
    snap = snapshot()
    snap['journal']['execution_economics_steps'][1]['actual_fill_ts'] = None
    row = projected(snap)
    assert row['first_fill_at'] is None and row['hold_seconds'] is None
    assert row['first_fill_missing_order_refs'] == ['h-open']
    assert row['first_fill_confirmed_at'] == 109
    assert row['estimated_hold_seconds'] == 47
    assert row['confirmation_duration_state'] == 'closed'


def test_legacy_order_confirmation_does_not_need_exchange_telemetry():
    snap = snapshot()
    snap['journal']['execution_economics_steps'] = []
    row = projected(snap)
    assert row['first_fill_at'] is None and row['duration_state'] == 'unknown'
    assert row['first_fill_confirmed_at'] == 109
    assert row['estimated_hold_seconds'] == 47


def test_legacy_recorded_only_is_local_confirmation_never_exchange_first():
    snap = snapshot()
    snap['journal']['execution_economics_steps'] = []
    for order in snap['journal']['orders'][:2]:
        order['terminal_received_ts'] = None
    row = projected(snap)
    assert row['first_fill_at'] is None and row['hold_seconds'] is None
    assert row['first_fill_confirmed_at'] == 110
    assert row['estimated_hold_seconds'] == 46


@pytest.mark.parametrize('field', ['terminal_observed_ts', 'result_ts'])
def test_local_confirmation_falls_back_only_to_matching_positive_fill_trace(field):
    snap = snapshot()
    for order in snap['journal']['orders'][:2]:
        order['terminal_received_ts'] = None
    for trace in snap['journal']['execution_economics_steps'][:2]:
        trace['actual_fill_ts'] = None
        trace[field] = 108 if trace['venue'] == 'entropy' else 118
        trace['provisional_avg_px'], trace['actual_fill_vwap'] = '100', None
    row = projected(snap)
    assert row['first_fill_at'] is None
    assert row['first_fill_confirmed_at'] == 108
    assert row['estimated_hold_seconds'] == 48


def test_unknown_other_leg_cannot_erase_known_local_positive_confirmation():
    snap = snapshot()
    live = snap['journal']['live_cycles'][0]
    live.update(phase='OPEN_HEDGE', entropy_units=10, hedge_units=0)
    snap['journal']['orders'] = snap['journal']['orders'][:2]
    snap['journal']['orders'][1].update(state='UNKNOWN', filled_units=0, avg_px=None,
                                      terminal_received_ts=None, terminal_recorded_ts=None)
    snap['journal']['execution_economics_steps'] = snap['journal']['execution_economics_steps'][:1]
    row = projected(snap)
    assert row['first_fill_at'] is None
    assert row['first_fill_confirmed_at'] == 109
    assert row['estimated_hold_seconds'] == 91
    assert row['confirmation_duration_state'] == 'active'
    snap['generated_at'] = 210
    assert projected(snap)['estimated_hold_seconds'] == 101


def test_positive_partial_confirmation_does_not_require_terminal_order():
    snap = snapshot()
    order, trace = snap['journal']['orders'][0], snap['journal']['execution_economics_steps'][0]
    order.update(state='UNKNOWN', filled_units=4, avg_px='100', terminal_received_ts=None)
    trace.update(result_units=4, result_ts=108, actual_fill_ts=None)
    live = {**snap['journal']['live_cycles'][0], 'phase': 'OPEN_HEDGE'}
    row = _cycle_duration(live, {}, [order], [trace], [], now=200)
    assert row['first_fill_at'] is None and row['hold_seconds'] is None
    assert row['first_fill_confirmed_at'] == 108
    assert row['estimated_hold_seconds'] == 92
    assert row['confirmation_duration_state'] == 'active'


def test_exchange_first_is_not_the_last_fill_timestamp():
    snap = snapshot()
    trace = snap['journal']['execution_economics_steps'][0]
    trace.update(exchange_first_fill_ts=100, actual_fill_ts=104, exchange_last_fill_ts=104)
    row = projected(snap)
    assert row['first_fill_at'] == 100 and row['hold_seconds'] == 56
    trace['exchange_first_fill_ts'] = None
    row = projected(snap)
    assert row['first_fill_at'] is None
    assert row['first_fill_confirmed_at'] == 109


def test_complete_fill_history_first_precedes_telemetry_last_and_first():
    snap = snapshot()
    snap['journal']['execution_economics_steps'][0].update(
        exchange_first_fill_ts=102, actual_fill_ts=104, exchange_last_fill_ts=104)
    snap['journal']['fee_fills'] = [dict(cycle_id='cycle', order_ref='e-open',
        venue='entropy', exchange_fill_key=key, units=units, price='100', ts=ts)
        for key, units, ts in (('first', 4, 100), ('last', 6, 104))]
    row = projected(snap)
    assert row['first_fill_at'] == 100 and row['hold_seconds'] == 56


@pytest.mark.parametrize('invalid', [None, 0, -1, 201, 'NaN', 'Infinity'])
def test_missing_or_invalid_local_time_is_not_replaced_by_started_or_submitted(invalid):
    snap = snapshot()
    order = snap['journal']['orders'][0]
    order['terminal_received_ts'] = invalid
    order['terminal_recorded_ts'] = None
    snap['journal']['cycle_economics'][0]['first_fill_observed_ts'] = 95
    result = _cycle_duration(snap['journal']['live_cycles'][0], snap['journal']['cycle_economics'][0],
        [order], [], [], now=200)
    assert result['first_fill_confirmed_at'] is None
    assert result['estimated_hold_seconds'] is None


@pytest.mark.parametrize('change', ['wrong_order', 'wrong_cycle', 'wrong_venue',
                                   'quantity_above_order', 'missing_quantity', 'missing_vwap'])
def test_local_trace_requires_order_quantity_and_vwap_evidence(change):
    snap = snapshot()
    order, trace = snap['journal']['orders'][0], snap['journal']['execution_economics_steps'][0]
    order.update(avg_px=None, terminal_received_ts=None)
    trace.update(result_ts=108)
    if change == 'wrong_order':
        trace['order_ref'] = 'another-order'
    elif change == 'wrong_cycle':
        trace['cycle_id'] = 'another-cycle'
    elif change == 'wrong_venue':
        trace['venue'] = 'hedge'
    elif change == 'quantity_above_order':
        trace['result_units'] = 11
    elif change == 'missing_quantity':
        trace['result_units'] = None
    else:
        trace['actual_fill_vwap'] = None
    result = _cycle_duration(snap['journal']['live_cycles'][0], {}, [order], [trace], [], now=200)
    assert result['first_fill_confirmed_at'] is None and result['estimated_hold_seconds'] is None


@pytest.mark.parametrize('missing_orders', [True, False])
def test_no_positive_opening_fill_never_creates_holding_duration(missing_orders):
    snap = snapshot()
    if missing_orders:
        snap['journal']['orders'] = []
    else:
        for order in snap['journal']['orders']:
            order['filled_units'] = 0
    row = projected(snap)
    assert row['has_confirmed_fill'] is False
    assert row['first_fill_at'] is row['first_fill_confirmed_at'] is None
    assert row['hold_seconds'] is row['estimated_hold_seconds'] is None
    assert row['duration_state'] == row['confirmation_duration_state'] == 'unknown'


@pytest.mark.parametrize('change', ['unverified', 'nonflat', 'unknown_order', 'future', 'invalid_terminal'])
def test_local_estimate_requires_authoritative_terminal_both_flat_end(change):
    snap = snapshot()
    live = snap['journal']['live_cycles'][0]
    if change == 'unverified':
        live['position_evidence_authoritative'] = 0
    elif change == 'nonflat':
        live['hedge_units'] = -1
    elif change == 'unknown_order':
        snap['journal']['orders'][-1]['state'] = 'UNKNOWN'
    elif change == 'future':
        live['last_authoritative_ts'] = 201
    else:
        snap['journal']['orders'][-1]['terminal_received_ts'] = 'NaN'
    row = projected(snap)
    assert row['first_fill_confirmed_at'] == 109
    assert row['estimated_hold_seconds'] is None
    assert row['confirmation_duration_state'] == 'unknown'
