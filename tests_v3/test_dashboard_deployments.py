from copy import deepcopy
from decimal import Decimal

from fastapi.testclient import TestClient
import pytest

from entropy_arb.dashboard_api.v3_app import create_app
from entropy_arb.dashboard_api.v3_readmodel import SourcePaths
from tests_v3.test_dashboard_profit import snapshot


@pytest.fixture
def client(tmp_path):
    def paths(name):
        root = tmp_path / name
        return SourcePaths(*(root / part for part in ('journal', 'live', 'manifest', 'diagnostics', 'recorder', 'rec_manifest', 'data', 'reports', 'cache')))
    with TestClient(create_app(start_collector=False, deployment_paths={name: paths(name) for name in ('basis', 'funding')})) as client:
        basis = snapshot()
        funding = snapshot(False)
        # IDs deliberately collide in independent journals, including funding event and orders.
        for snap, symbol, mode in ((basis, 'SNDK', 'basis'), (funding, 'NBIS', 'funding_hedge')):
            snap['sources']['live_manifest']['data'].update(pair_identity=f'test:hl:{symbol}:lighter:{symbol}', settings={'strategy_mode': mode})
            for source in snap['sources'].values():
                source.update(quality='available', observed_at=195)
            snap['journal']['funding_settlements'] = [{'cycle_id': 'cycle', 'venue': 'entropy', 'event_id': 'same-event', 'ts': 150, 'amount_usd': '.2'}]
        funding['journal']['cycle_economics'] = []
        for order in funding['journal']['orders']:
            order['fee_usd'] = None
        for key, value in (('basis', basis), ('funding', funding)):
            client.app.state.v3_read_models[key].snapshot = value
        yield client


def get(client, path, **params):
    response = client.get(f'/api/v3/{path}', params={'scope': 'batch', 'mode': 'all', **params})
    assert response.status_code == 200, response.text
    return response.json()['data']


def test_mixed_complete_cycles_keep_missing_fee_and_dedup_per_source(client):
    data = get(client, 'overview')
    profit = data['profit']
    assert profit['view'] == 'complete'
    assert profit['counts']['complete'] == 2
    assert profit['totals']['complete_cycles'] == 2
    assert profit['totals']['net_usd'] is None
    assert Decimal(profit['totals']['known_fees_usd']) == Decimal('.4')
    assert Decimal(profit['totals']['funding_usd']) == Decimal('.4')
    assert {(row['source_id'], row['cycle_id']) for row in profit['cycles']} == {('basis', 'cycle'), ('funding', 'cycle')}
    assert len(data['deployments']) == 2
    assert len(get(client, 'health')['deployments']) == 2
    # Selecting a source-qualified circle does not narrow global All cohort.
    assert get(client, 'overview', source='funding')['profit']['totals'] == profit['totals']
    assert get(client, 'overview')['profit']['totals'] == profit['totals']


def test_filters_and_source_context_are_independent(client):
    basis = get(client, 'overview', mode='basis')['profit']
    assert basis['totals']['net_usd'] == '0.8'
    assert basis['cycles'][0]['symbol'] == 'SNDK'
    funding = get(client, 'overview', symbol='NBIS')['profit']
    assert len(funding['cycles']) == 1 and funding['totals']['net_usd'] is None
    assert get(client, 'cycles', source='funding')['total'] == 2
    assert get(client, 'overview', batch_id='basis:batch')['profit']['totals']['cycle_count'] == 1
    assert get(client, 'overview', symbol='missing')['profit']['cycles'] == []
    assert get(client, 'overview', scope='custom', from_ts=180, to_ts=200)['profit']['cycles'] == []


def test_colliding_cycle_detail_is_bound_to_requested_source(client):
    basis = get(client, 'cycles/cycle', source='basis')
    funding = get(client, 'cycles/cycle', source='funding')
    assert basis['cycle']['source_id'] == 'basis' and basis['profit']['net_usd'] == '0.8'
    assert funding['cycle']['source_id'] == 'funding' and funding['profit']['net_usd'] is None
    assert client.get('/api/v3/cycles/cycle', params={'mode': 'all'}).status_code == 400
    assert client.get('/api/v3/system', params={'source': '../../account'}).status_code == 400
    assert get(client, 'system', source='basis')['live']['process_id'] == 1


def test_source_qualified_pagination_does_not_skip_colliding_ids(client):
    first = get(client, 'cycles', view='complete', limit=1)
    assert first['total'] == 2
    second = get(client, 'cycles', view='complete', limit=1, cursor=first['next_cursor'])
    assert first['items'][0]['source_id'] != second['items'][0]['source_id']
    assert second['next_cursor'] is None
    selected = get(client, 'cycles', source='funding', view='complete', limit=1, cursor=first['next_cursor'])
    assert selected['items'] == second['items']


def test_zero_fill_incomplete_and_active_are_separate_from_default_complete(client):
    snap = client.app.state.v3_read_models['basis'].snapshot
    zero = {**snap['journal']['live_cycles'][0], 'cycle_id': 'zero'}
    snap['journal']['live_cycles'].append(zero)
    snap['journal']['cycles'].append({'cycle_id': 'zero', 'completed_ts': 160, 'result_kind': 'zero_fill'})
    snap['journal']['orders'].append({**snap['journal']['orders'][0], 'cycle_id': 'zero', 'order_ref': 'zero-order', 'filled_units': 0})
    incomplete = deepcopy(snap['journal']['live_cycles'][0]); incomplete.update(cycle_id='incomplete', entropy_units=1)
    snap['journal']['live_cycles'].append(incomplete)
    snap['journal']['cycles'].append({'cycle_id': 'incomplete', 'completed_ts': 160, 'accounting_final': 0})
    snap['journal']['orders'].append({**snap['journal']['orders'][0], 'cycle_id': 'incomplete', 'order_ref': 'single-leg'})
    active = {**incomplete, 'cycle_id': 'active', 'phase': 'OPEN'}
    snap['journal']['live_cycles'].append(active)
    snap['journal']['orders'].append({**snap['journal']['orders'][0], 'cycle_id': 'active', 'order_ref': 'active-order'})
    data = get(client, 'overview')['profit']
    assert data['counts'] == {'complete': 2, 'open': 1, 'incomplete': 1, 'unfilled': 1}
    assert data['totals']['cycle_count'] == 2 and data['holding']['cycle_count'] == 2
    assert get(client, 'cycles', view='unfilled')['items'][0]['cycle_id'] == 'zero'
    assert get(client, 'overview', view='incomplete')['profit']['cycles'][0]['cycle_id'] == 'incomplete'
    assert get(client, 'overview', view='unfilled')['profit']['totals']['cycle_count'] == 0


def test_missing_source_preserves_known_coverage_without_fake_zero(client):
    client.app.state.v3_read_models['funding'].snapshot['journal'] = None
    profit = get(client, 'overview')['profit']
    assert profit['coverage_missing_sources'] == 1
    assert profit['totals']['net_usd'] is None
    assert Decimal(profit['totals']['known_realized_usd']) == 1
    assert client.post('/api/v3/overview').status_code == 405


def test_late_funding_keeps_circle_visible_in_same_time_cohort(client):
    for model in client.app.state.v3_read_models.values():
        model.snapshot['journal']['funding_settlements'][0]['ts'] = 195
    profit = get(client, 'overview', scope='custom', from_ts=180, to_ts=200)['profit']
    circles = get(client, 'cycles', scope='custom', from_ts=180, to_ts=200, view='complete')
    assert profit['totals']['cycle_count'] == profit['counts']['complete'] == circles['total'] == 2
    assert Decimal(profit['totals']['funding_usd']) == Decimal('.4')
    assert {(r['source_id'],r['cycle_id']) for r in profit['cycles']} == {(r['source_id'],r['cycle_id']) for r in circles['items']}


@pytest.mark.parametrize("configured,settings", [
    ({}, {"single_cycle_review_loss_usd": None}),
    ({"single_cycle_review_loss_usd": None}, {}),
    ({"single_cycle_review_loss_usd": "0.10"}, {"single_cycle_review_loss_usd": "0.10"}),
    ({}, {}),
])
def test_nullable_review_parameter_preserves_source_presence(client, configured, settings):
    manifest = client.app.state.v3_read_models['basis'].snapshot['sources']['live_manifest']['data']
    manifest['configured_settings'] = configured
    manifest['settings'].update(settings)
    rows = get(client, 'system', source='basis')['config']
    review = next((row for row in rows if row['key'] == 'single_cycle_review_loss_usd'), None)
    if not configured and not settings:
        assert review is None
    else:
        assert review['configured_present'] is ('single_cycle_review_loss_usd' in configured)
        assert review['effective_present'] is ('single_cycle_review_loss_usd' in settings)
        assert review['configured'] == configured.get('single_cycle_review_loss_usd')
        assert review['effective'] == settings.get('single_cycle_review_loss_usd')
        # An unprovided authorization is still unknown, never evidence of disablement.
        assert 'authorized_present' not in review


def test_unified_today_uses_one_beijing_window_across_midnight(client):
    from datetime import datetime, timezone
    now = datetime(2026, 10, 10, 16, 0, 1, tzinfo=timezone.utc).timestamp()
    client.app.state.v3_read_models['basis'].snapshot['generated_at'] = now
    client.app.state.v3_read_models['funding'].snapshot['generated_at'] = now - 2
    response = client.get('/api/v3/overview', params={'scope': 'today', 'mode': 'all'})
    assert response.status_code == 200
    payload = response.json()
    windows = payload['meta']['deployment_windows']
    assert windows['basis']['from_ts'] == windows['funding']['from_ts'] == now - 1
    assert windows['basis']['to_ts'] == windows['funding']['to_ts'] == now
    # Historical unknown fees do not become a trade in today's shared window.
    assert payload['data']['profit']['totals']['cycle_count'] == 0
    assert payload['data']['profit']['totals']['net_usd'] == '0'


def test_residual_position_remains_visible_after_terminal_phase(client):
    snap = client.app.state.v3_read_models['basis'].snapshot
    snap['journal']['live_cycles'][0].update(phase='DONE', entropy_units=1, hedge_units=0)
    holding = get(client, 'overview', scope='custom', from_ts=180, to_ts=200)['profit']['holding']
    residual = next(row for row in holding['cycles'] if row['source_id'] == 'basis')
    assert residual['entropy_units'] == 1 and residual['hedge_units'] == 0
    assert residual['unrealized_usd'] is None


def test_all_history_and_seven_days_match_totals_full_pagination_and_detail(client):
    now = 1_800_000_000
    for model in client.app.state.v3_read_models.values():
        snap = model.snapshot
        original = deepcopy(snap['journal'])
        original['execution_economics_steps'] = [{'cycle_id': 'cycle', 'step_key': 'quantity', 'quantity_step': '1'}]
        snap['journal'] = {key: [] for key in original}
        snap['generated_at'] = now
        for index in range(80):
            offset = now - (2 if index >= 70 else 900) * 86400 + index * 10 - 200
            for table, rows in original.items():
                for original_row in rows:
                    row = deepcopy(original_row)
                    if 'cycle_id' in row:
                        row['cycle_id'] = f'cycle-{index}'
                    if 'order_ref' in row:
                        row['order_ref'] = f"{index}:{row['order_ref']}"
                    if 'event_id' in row:
                        row['event_id'] = f"{index}:{row['event_id']}"
                    for key, value in row.items():
                        if key.endswith('_ts') and value is not None:
                            row[key] = value + offset
                    snap['journal'][table].append(row)
    client.app.state.v3_read_models['funding'].snapshot['generated_at'] -= 2
    for scope, expected in (('all', 160), ('7d', 20)):
        response = client.get('/api/v3/overview', params={'scope': scope, 'mode': 'all'})
        assert response.status_code == 200
        payload = response.json()
        windows = payload['meta']['deployment_windows']
        assert windows['basis'] == windows['funding']
        assert windows['basis']['from_ts'] == (None if scope == 'all' else now - 7 * 86400)
        assert windows['basis']['to_ts'] == (None if scope == 'all' else now)
        profit = payload['data']['profit']
        assert profit['totals']['cycle_count'] == profit['counts']['complete'] == expected
        assert len(profit['cycles']) == expected
        assert Decimal(profit['totals']['known_realized_usd']) == expected
        rows, cursor = [], None
        while True:
            params = {'scope': scope, 'view': 'complete', 'limit': 37}
            if cursor:
                params['cursor'] = cursor
            page = get(client, 'cycles', **params)
            assert page['total'] == expected
            rows.extend(page['items'])
            cursor = page['next_cursor']
            if not cursor:
                break
        assert len(rows) == expected
        assert {(r['source_id'], r['cycle_id']) for r in rows} == {
            (r['source_id'], r['cycle_id']) for r in profit['cycles']}
        last = rows[-1]
        detail = client.get(f"/api/v3/cycles/{last['cycle_id']}", params={
            'source': last['source_id'], 'scope': scope})
        assert detail.status_code == 200
        assert detail.json()['meta']['window']['scope'] == scope
        assert detail.json()['data']['cycle']['source_id'] == last['source_id']
        assert detail.json()['data']['profit']['order_count'] == 4


def test_all_history_preserves_undated_cycles_unknown_fees_and_zero_fill(client):
    for model in client.app.state.v3_read_models.values():
        snap = model.snapshot
        snap['journal']['live_cycles'][0]['started_ts'] = None
        snap['journal']['cycles'][0].update(completed_ts=None, accounting_final=0)
        snap['journal']['cycle_economics'] = []
        for order in snap['journal']['orders']:
            order['terminal_recorded_ts'] = None
            order['fee_usd'] = None
        snap['journal']['live_cycles'].append({
            **snap['journal']['live_cycles'][0], 'cycle_id': 'undated-zero'})
        snap['journal']['cycles'].append({'cycle_id': 'undated-zero', 'completed_ts': None})
        snap['journal']['orders'].append({**snap['journal']['orders'][0],
            'cycle_id': 'undated-zero', 'order_ref': 'undated-zero', 'filled_units': 0})
    profit = get(client, 'overview', scope='all', view='filled')['profit']
    assert profit['totals']['cycle_count'] == profit['counts']['incomplete'] == 2
    assert profit['counts']['complete'] == 0  # Missing completion time remains incomplete evidence.
    assert profit['totals']['net_usd'] is None
    assert profit['totals']['pending_fees'] == 8
    assert profit['counts']['unfilled'] == 2
    assert any(day['day'] == '归属时间待确认' for day in profit['daily'])
    assert get(client, 'cycles', scope='all', view='unfilled')['total'] == 2
    assert get(client, 'cycles', scope='all', view='filled')['total'] == 2


def test_all_range_does_not_keep_a_hidden_batch_source_filter(client):
    profit = get(client, 'overview', scope='all', batch_id='basis:batch')['profit']
    assert {(row['source_id'], row['cycle_id']) for row in profit['cycles']} == {
        ('basis', 'cycle'), ('funding', 'cycle')}
    assert get(client, 'cycles', scope='all', batch_id='basis:batch', view='complete')['total'] == 2
