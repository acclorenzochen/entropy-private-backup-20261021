from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import sqlite3

import pytest

from entropy_arb.dashboard_api.v3_projection import overview, resolve_window
from entropy_arb.dashboard_api.v3_readmodel import journal_source


def snapshot(final=True):
    live = {'cycle_id': 'cycle', 'batch_id': 'batch', 'source_ts': 195,
            'start_ts': 100, 'process_id': 1, 'build_hash': 'test',
            'accounting_quantity_step': '1'}
    orders = []
    for index, (venue, side, px, close) in enumerate([
        ('entropy', 'buy', '100', False), ('hedge', 'sell', '100', False),
        ('entropy', 'sell', '102', True), ('hedge', 'buy', '101', True),
    ]):
        orders.append({'order_ref': f'order-{index}', 'cycle_id': 'cycle',
                       'venue': venue, 'side': side, 'avg_px': px, 'fee_usd': '.1',
                       'requested_units': 1, 'filled_units': 1, 'reduce_only': close,
                       'state': 'TERMINAL', 'submitted_ts': 110 + index,
                       'terminal_recorded_ts': 120 + index})
    journal = {key: [] for key in ('batches', 'cycles', 'live_cycles', 'orders',
                                   'live_steps', 'cycle_economics', 'execution_economics_steps',
                                   'request_events', 'funding_cycles', 'funding_settlements')}
    journal.update(orders=orders, live_cycles=[{
        'cycle_id': 'cycle', 'batch_id': 'batch', 'direction': 'long_entropy',
        'started_ts': 110, 'phase': 'DONE', 'entropy_units': 0, 'hedge_units': 0,
        'position_evidence_authoritative': 1}], cycles=[{
        'cycle_id': 'cycle', 'completed_ts': 160, 'accounting_final': int(final),
        'pnl_usd': '.8', 'result_kind': 'clean'}], cycle_economics=[{
        'cycle_id': 'cycle', 'accounting_final': int(final), 'actual_fees_usd': '.4',
        'actual_funding_usd': '.2', 'finalized_pnl_usd': '.8'}])
    return {'id': 'TEST_FIXTURE', 'generated_at': 200, 'journal': journal, 'market': [],
            'sources': {'live': {'data': live}, 'live_diagnostics': {'data': None},
                        'live_manifest': {'data': {'pair_identity': 'test:hl:COIN:lighter:COIN',
                                                   'settings': {'strategy_mode': 'basis'}}}}}


def project(snap, **query):
    window = resolve_window({'scope': 'batch', **query}, snap['sources']['live']['data'],
                            now=snap['generated_at'])
    return overview(snap, window)['profit']


def test_final_breakdown_matches_net_and_all_views():
    data = project(snapshot())
    assert Decimal(data['totals']['realized_usd']) == 1
    assert Decimal(data['totals']['net_usd']) == Decimal('.8')
    assert data['totals']['accounting'] == 'final'
    for field in ('realized_usd', 'fees_usd', 'funding_usd', 'net_usd'):
        assert Decimal(data['totals'][field]) == sum(Decimal(r[field]) for r in data['cycles'])
        assert Decimal(data['totals'][field]) == sum(Decimal(r[field]) for r in data['daily'])
        assert data['symbols'][0][field] == data['totals'][field]


def test_unknown_nbis_fees_do_not_reuse_unpaid_canonical_net():
    snap = snapshot(False)
    snap['sources']['live_manifest']['data']['settings']['strategy_mode'] = 'funding_hedge'
    snap['journal']['cycle_economics'] = []
    for order in snap['journal']['orders']:
        order['fee_usd'] = None
    snap['journal']['cycles'][0]['pnl_usd'] = '1.2'  # Unpaid provisional baseline.
    snap['journal']['funding_settlements'] = [{'cycle_id': 'cycle', 'venue': 'entropy',
        'event_id': 'settlement', 'ts': 150, 'amount_usd': '.2'}]
    data = project(snap)
    assert data['totals']['fees_usd'] is None
    assert data['totals']['net_usd'] is None
    assert data['cycles'][0]['net_usd'] is None
    assert Decimal(data['totals']['realized_usd']) == 1
    assert Decimal(data['totals']['funding_usd']) == Decimal('.2')
    assert data['totals']['pending_fees'] == 4


def test_final_history_can_use_accounted_cycle_fees_when_order_fees_missing():
    snap = snapshot()
    for order in snap['journal']['orders']:
        order['fee_usd'] = None
    data = project(snap)
    assert Decimal(data['totals']['fees_usd']) == Decimal('.4')
    assert data['cycles'][0]['fee_basis'] == 'cycle_complete'
    assert data['totals']['accounting'] == 'final'


def test_daily_beijing_event_attribution_and_scope_use_same_sums():
    snap = snapshot()
    midnight = datetime.fromisoformat('2026-10-09T16:00:00+00:00').timestamp()
    snap['generated_at'] = midnight + 200
    snap['journal']['live_cycles'][0]['started_ts'] = midnight - 100
    snap['journal']['cycles'][0]['completed_ts'] = midnight + 100
    for index, order in enumerate(snap['journal']['orders']):
        order['terminal_recorded_ts'] = midnight - 10 + index
    snap['journal']['funding_settlements'] = [{'cycle_id': 'cycle', 'venue': 'entropy',
        'event_id': 'settlement', 'ts': midnight - 20, 'amount_usd': '.2'}]
    data = project(snap)
    days = {row['day']: row for row in data['daily']}
    assert Decimal(days['2026-10-09']['net_usd']) == Decimal('-.2')
    assert Decimal(days['2026-10-10']['net_usd']) == 1
    day = project(snap, scope='custom', from_ts=midnight, to_ts=midnight + 200)
    assert Decimal(day['totals']['net_usd']) == 1
    assert len(day['cycles']) == 1  # Inherited cycle closed within range.


def test_funding_and_cumulative_order_snapshots_are_not_double_counted():
    snap = snapshot()
    row = {'cycle_id': 'cycle', 'venue': 'entropy', 'event_id': 'settlement',
           'ts': 150, 'amount_usd': '.2'}
    snap['journal']['funding_settlements'] = [row, deepcopy(row)]
    snap['journal']['orders'].insert(0, {**snap['journal']['orders'][0], 'filled_units': 0})
    data = project(snap)
    assert Decimal(data['totals']['funding_usd']) == Decimal('.2')
    assert Decimal(data['totals']['fees_usd']) == Decimal('.4')
    assert data['cycles'][0]['order_count'] == 4


def test_conflicting_duplicate_funding_is_unknown():
    snap = snapshot()
    row = {'cycle_id': 'cycle', 'venue': 'entropy', 'event_id': 'settlement',
           'ts': 150, 'amount_usd': '.2'}
    snap['journal']['funding_settlements'] = [row, {**row, 'amount_usd': '.3'}]
    assert project(snap)['totals']['net_usd'] is None


def test_conflicting_settlement_cannot_disappear_into_another_cycle():
    snap = snapshot(False)
    row = {'cycle_id': 'cycle', 'venue': 'entropy', 'event_id': 'settlement',
           'ts': 150, 'amount_usd': '.2'}
    snap['journal']['funding_settlements'] = [row, {**row, 'cycle_id': 'other'}]
    snap['journal']['funding_cycles'] = [{'cycle_id': 'cycle', 'last_complete_poll_ts': 195}]
    assert project(snap)['totals']['funding_usd'] is None
    assert project(snap)['totals']['net_usd'] is None


@pytest.mark.parametrize('mutation', ['missing_position_proof', 'nonzero_leg', 'unknown_order', 'missing_step'])
def test_incomplete_closed_cycle_cannot_become_zero_price_profit(mutation):
    snap = snapshot(False)
    snap['journal']['cycle_economics'] = []
    cycle = snap['journal']['live_cycles'][0]
    if mutation == 'missing_position_proof':
        cycle['position_evidence_authoritative'] = 0
    elif mutation == 'nonzero_leg':
        cycle['entropy_units'] = 1
    elif mutation == 'unknown_order':
        snap['journal']['orders'][0]['state'] = 'UNKNOWN'
    else:
        snap['sources']['live']['data']['accounting_quantity_step'] = None
    data = project(snap)
    assert data['totals']['realized_usd'] is None
    assert data['totals']['net_usd'] is None


def test_fee_telemetry_must_match_cumulative_quantity():
    snap = snapshot(False)
    snap['journal']['orders'][0]['fee_usd'] = None
    step = {'cycle_id': 'cycle', 'order_ref': 'order-0', 'result_units': 2, 'actual_fee_usd': '.1'}
    snap['journal']['execution_economics_steps'] = [step]
    assert project(snap)['totals']['fees_usd'] is None
    step['result_units'] = 1
    assert Decimal(project(snap)['totals']['fees_usd']) == Decimal('.4')


def test_partial_close_average_cost_and_unrealized_are_separate():
    snap = snapshot(False)
    cycle = snap['journal']['live_cycles'][0]
    cycle.update(phase='EXITING', entropy_units=6, hedge_units=-10)
    snap['journal']['cycles'][0]['completed_ts'] = None
    orders = snap['journal']['orders'][:3]
    orders[0].update(filled_units=10, requested_units=10)
    orders[1].update(filled_units=10, requested_units=10)
    orders[2].update(filled_units=4, requested_units=4)
    snap['journal']['orders'] = orders
    snap['sources']['live']['data'].update(accounting_quantity_step='.1', executable_gross_usd='1.8')
    data = project(snap)
    current = data['holding']['cycles'][0]
    assert Decimal(current['active_realized_usd']) == Decimal('.8')
    assert Decimal(current['unrealized_usd']) == 1
    assert Decimal(data['totals']['realized_usd']) == 0  # Deferred until cycle completion.
    for stamp in (189, 201):
        snap['sources']['live']['data']['source_ts'] = stamp
        assert project(snap)['holding']['unrealized_usd'] is None


def test_empty_scope_and_missing_journal_are_distinct():
    snap = snapshot()
    data = project(snap, scope='custom', from_ts=180, to_ts=200)
    assert data['cycles'] == []
    assert Decimal(data['totals']['net_usd']) == 0
    snap['journal'] = None
    assert project(snap)['available'] is False


def test_final_funding_with_late_new_settlement_is_provisional():
    snap = snapshot()
    snap['journal']['funding_settlements'] = [{'cycle_id': 'cycle', 'venue': 'entropy',
        'event_id': 'settlement', 'ts': 150, 'amount_usd': '.3'}]
    data = project(snap)
    assert data['totals']['accounting'] == 'provisional'
    assert Decimal(data['totals']['net_usd']) == Decimal('.9')


def test_explicit_zero_fee_is_not_missing_and_result_remains_provisional():
    snap = snapshot(False)
    for order in snap['journal']['orders']:
        order['fee_usd'] = '0'
    data = project(snap)
    assert Decimal(data['totals']['fees_usd']) == 0
    assert Decimal(data['totals']['net_usd']) == Decimal('1.2')
    assert data['totals']['accounting'] == 'provisional'
    assert data['daily'][0]['accounting'] == 'provisional'
    snap['journal']['orders'][0]['fee_usd'] = None
    assert project(snap)['totals']['net_usd'] is None


def test_quantity_step_conflict_does_not_fall_back_to_live_value():
    snap = snapshot(False)
    snap['journal']['execution_economics_steps'] = [
        {'cycle_id': 'cycle', 'quantity_step': '1'},
        {'cycle_id': 'cycle', 'quantity_step': '.1'}]
    assert project(snap)['totals']['realized_usd'] is None


def test_empty_funding_records_require_successful_poll_and_are_never_final():
    snap = snapshot(False)
    snap['journal']['cycle_economics'] = []
    snap['sources']['live_manifest']['data']['settings']['strategy_mode'] = 'funding_hedge'
    assert project(snap)['totals']['funding_usd'] is None
    snap['journal']['funding_cycles'] = [{'cycle_id': 'cycle', 'last_complete_poll_ts': 195,
                                        'closed_poll_complete': 1}]
    data = project(snap)
    assert Decimal(data['totals']['funding_usd']) == 0
    assert Decimal(data['totals']['net_usd']) == Decimal('.6')
    assert data['totals']['accounting'] == 'provisional'


@pytest.mark.parametrize('evidence', ['fill_partial', 'receipt_partial', 'history_larger', 'receipt_larger', 'rebate'])
def test_known_partial_order_fees_do_not_make_full_fees_or_net(evidence):
    snap = snapshot(False)
    snap['sources']['live_manifest']['data']['settings']['strategy_mode'] = 'funding_hedge'
    snap['journal']['funding_cycles'] = [{'cycle_id': 'cycle', 'known_cash_result_usd': '.98',
                                         'trade_cash_net_usd': None, 'closed_poll_complete': 0}]
    for order in snap['journal']['orders']:
        order.update(filled_units=4, requested_units=4)
    order = snap['journal']['orders'][0]
    order.update(fee_usd=None, known_fee_usd='.02', fee_covered_units=1)
    fill = {'cycle_id': 'cycle', 'order_ref': order['order_ref'], 'venue': order['venue'],
            'exchange_fill_key': 'first-fill', 'units': 1, 'fee_usd': '.02'}
    expected = Decimal('.32')
    if evidence == 'fill_partial':
        order.update(known_fee_usd=None, fee_covered_units=None)
        snap['journal']['fee_fills'] = [fill, {**fill, 'exchange_fill_key': 'missing', 'units': 3, 'fee_usd': None}]
    elif evidence == 'history_larger':
        snap['journal']['fee_fills'] = [fill, {**fill, 'exchange_fill_key': 'second-fill', 'fee_usd': '.03'}]
        expected = Decimal('.35')
    elif evidence == 'receipt_larger':
        order.update(known_fee_usd='.04', fee_covered_units=2)
        snap['journal']['fee_fills'] = [fill]
        expected = Decimal('.34')
    elif evidence == 'rebate':
        order['known_fee_usd'] = '-.02'
        expected = Decimal('.28')
    before = deepcopy(snap)
    data = project(snap)
    assert project(snap) == data  # Repeated reads cannot accumulate or mutate evidence.
    assert snap == before
    assert data['totals']['pending_fees'] == 1
    for row in [data['totals'], data['cycles'][0], data['symbols'][0], data['daily'][0]]:
        assert row['fees_usd'] is None
        assert row['net_usd'] is None
        assert Decimal(row['known_fees_usd']) == expected
    detail = data['cycles'][0]['order_fees'][0]
    assert detail['fee_usd'] is None
    assert Decimal(detail['known_fee_usd']) == expected - Decimal('.3')


@pytest.mark.parametrize('mode', ['basis', 'funding_hedge'])
def test_complete_order_fee_replaces_partial_history_and_receipt_once(mode):
    snap = snapshot(False)
    snap['sources']['live_manifest']['data']['settings']['strategy_mode'] = mode
    snap['journal']['funding_cycles'] = [{'cycle_id': 'cycle', 'closed_poll_complete': 0}]
    order = snap['journal']['orders'][0]
    order.update(filled_units=2, requested_units=2, fee_usd=None,
                 known_fee_usd='.02', fee_covered_units=1)
    # Keep the opposing close cumulative quantity consistent.
    snap['journal']['orders'][2].update(filled_units=2, requested_units=2)
    fill = {'cycle_id': 'cycle', 'order_ref': order['order_ref'], 'venue': order['venue'],
            'exchange_fill_key': 'fee-fill', 'units': 1, 'fee_usd': '.02'}
    snap['journal']['fee_fills'] = [fill, deepcopy(fill)]
    pending = project(snap)
    assert Decimal(pending['totals']['known_fees_usd']) == Decimal('.32')
    assert pending['totals']['net_usd'] is None
    order.update(fee_usd='.05', known_fee_usd='.05', fee_covered_units=2)
    complete = project(snap)
    assert project(snap) == complete
    assert Decimal(complete['totals']['fees_usd']) == Decimal('.35')
    assert Decimal(complete['totals']['known_fees_usd']) == Decimal('.35')
    assert Decimal(complete['totals']['net_usd']) == Decimal('2.85')
    assert complete['totals']['pending_fees'] == 0
    assert complete['totals']['accounting'] == 'provisional'


def test_partial_fee_scope_uses_same_order_confirmation_attribution():
    snap = snapshot(False)
    snap['journal']['orders'][0].update(fee_usd=None, known_fee_usd='.02', fee_covered_units=1)
    included = project(snap, scope='custom', from_ts=120, to_ts=121)
    assert Decimal(included['totals']['known_fees_usd']) == Decimal('.02')
    assert included['totals']['fees_usd'] is None
    assert included['totals']['net_usd'] is None
    excluded = project(snap, scope='custom', from_ts=130, to_ts=200)
    assert Decimal(excluded['totals']['known_fees_usd']) == 0
    assert Decimal(excluded['totals']['fees_usd']) == 0


@pytest.mark.parametrize('new_fields', [False, True])
def test_read_only_journal_exposes_fee_history_and_accepts_old_schema(tmp_path, new_fields):
    path = tmp_path / 'journal.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE metadata (key TEXT, value TEXT)')
        db.execute("INSERT INTO metadata VALUES ('economics_schema_version', '8')")
        db.execute('CREATE TABLE orders (order_ref TEXT, fee_usd TEXT)')
        db.execute("INSERT INTO orders VALUES ('order', NULL)")
        if new_fields:
            db.execute('ALTER TABLE orders ADD COLUMN known_fee_usd TEXT')
            db.execute('ALTER TABLE orders ADD COLUMN fee_covered_units INTEGER')
            db.execute("UPDATE orders SET known_fee_usd='.02', fee_covered_units=1")
            db.execute('CREATE TABLE fee_fills (exchange_fill_key TEXT, fee_usd TEXT)')
            db.execute("INSERT INTO fee_fills VALUES ('fill', '.02')")
    before = path.read_bytes()
    source = journal_source(path)
    assert source['quality'] == 'available'
    if new_fields:
        assert source['data']['orders'][0]['known_fee_usd'] == '.02'
        assert source['data']['orders'][0]['fee_covered_units'] == 1
        assert source['data']['fee_fills'] == [{'exchange_fill_key': 'fill', 'fee_usd': '.02'}]
    else:
        assert 'known_fee_usd' not in source['data']['orders'][0]
        assert source['data']['fee_fills'] == []
    assert journal_source(path)['data'] == source['data']
    assert path.read_bytes() == before
