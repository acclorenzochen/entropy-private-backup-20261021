import json
import sqlite3
import time

from fastapi.testclient import TestClient
import pytest

from entropy_arb.dashboard_api.v3_app import create_app
from entropy_arb.dashboard_api.v3_projection import funding_dashboard, overview, resolve_window
from entropy_arb.dashboard_api.v3_readmodel import SourcePaths, journal_source
from test_dashboard_v3 import fixture_snapshot


def snapshot():
    snap = fixture_snapshot()
    snap['sources']['live']['data'].update(
        source_ts=195, cycle_id='cycle-7', current_state='HOLDING_FUNDING',
        accounting_quantity_step='0.0001', common_quantity_step='0.01',
        configured_leverage={'entropy': 7, 'hedge': 7},
        applied_leverage={'entropy': 7, 'hedge': 7})
    snap['sources']['live_manifest'] = {'data': {
        'pair_identity': 'nbis-funding:hl:NBIS:lighter:NBIS',
        'authorization': {'batch_id': 'batch', 'max_position_cycles': 10,
                          'loss_budget_usd': None},
        'settings': {'strategy_mode': 'funding_hedge', 'max_notional_usd': '200',
                     'funding_settlements_required': 2, 'funding_stale_seconds': 90,
                     'funding_reversal_samples': 3, 'funding_reversal_seconds': 60,
                     'funding_remaining_cost_limit_usd': '0.5',
                     'funding_round_loss_limit_usd': None,
                     'funding_liquidation_distance': '0.02', 'funding_risk_stale_seconds': 10},
        'private_key': 'NEVER_EXPOSE_THIS', 'authorization_source': {'path': '/Users/secret'}}}
    snap['journal']['batches'] = [{'batch_id': 'batch', 'max_position_cycles': 10,
                                  'loss_budget': 'disabled', 'closed_for_entries': 0}]
    snap['journal']['funding_cycles'] = [{
        'cycle_id': 'cycle-7', 'hedged_ts': 120, 'negative_sample_count': 3,
        'negative_since_ts': 130, 'last_sample_ts': 195, 'last_net_usd_per_hour': '-0.1',
        'last_complete_poll_ts': 195, 'completed': 0, 'exit_reason': None}]
    snap['journal']['funding_settlements'] = [
        {'cycle_id': 'cycle-7', 'venue': venue, 'event_id': f'{venue}-{stamp}',
         'ts': stamp, 'amount_usd': amount, 'source': 'test'}
        for venue, stamp, amount in [('entropy', 120, '0.2'), ('entropy', 121, '0'),
                                     ('entropy', 130, '-0.3'), ('hedge', 121, '0.1')]]
    return snap


def test_funding_mode_projects_real_zero_settlements_native_qty_and_actual_amounts():
    snap = snapshot()
    data = overview(snap, resolve_window({'scope': 'session'}, snap['sources']['live']['data'], now=200))
    funding = data['funding']
    assert funding['policy']['configured'] is True
    assert funding['state']['settlement_counts'] == {'entropy': 1, 'hedge': 1}
    # Amounts include the hedged-at row, but that row cannot prove a new period.
    assert funding['state']['funding_by_venue'] == {'entropy': '-0.1', 'hedge': '0.1'}
    assert funding['state']['actual_funding_usd'] == '0.0'
    assert funding['net_usd_per_hour'] == '-0.1'
    assert funding['ledger_quantities'] == {'entropy': '0.0010', 'hedge': '-0.0010'}
    assert funding['state']['negative_sample_count'] == 3
    assert funding['expected_remaining_cost_usd'] is None
    assert funding['policy']['funding_round_loss_limit_usd'] is None
    assert funding['batch']['position_cycles'] == 8
    assert data['active_cycle']['funding_state']['cycle_id'] == 'cycle-7'
    assert 'NEVER_EXPOSE_THIS' not in json.dumps(data)
    assert '/Users/secret' not in json.dumps(data)


@pytest.mark.parametrize('change', ['old_live', 'old_sample', 'missing_rate', 'missing_step'])
def test_missing_and_stale_funding_values_are_never_zero(change):
    snap = snapshot()
    if change == 'old_live':
        snap['sources']['live']['data']['source_ts'] = 180
    elif change == 'old_sample':
        snap['journal']['funding_cycles'][0]['last_sample_ts'] = 100
    elif change == 'missing_rate':
        snap['journal']['funding_cycles'][0]['last_net_usd_per_hour'] = None
    else:
        del snap['sources']['live']['data']['accounting_quantity_step']
    data = funding_dashboard(snap)
    if change == 'missing_step':
        assert data['ledger_quantities'] == {'entropy': None, 'hedge': None}
    else:
        assert data['net_usd_per_hour'] is None
    assert data['state']['settlement_counts'] == {'entropy': 1, 'hedge': 1}


@pytest.mark.parametrize('ts,cycle_id,expected', [(195, 'cycle-7', True),
                                               (189, 'cycle-7', False),
                                               (201, 'cycle-7', False),
                                               (195, 'old-cycle', False)])
def test_risk_requires_fresh_matching_cycle_and_preserves_zero_rate(ts, cycle_id, expected):
    snap = snapshot()
    live = snap['sources']['live']['data']
    live['funding_risk'] = {'cycle_id': cycle_id, 'legs': {'entropy': {
        'position_qty': '0.0010', 'mark_price': '100', 'liquidation_price': '99',
        'distance': '0.01', 'source_ts': ts, 'private_key': 'NEVER_EXPOSE_THIS'}}}
    live['funding_rates'] = {'entropy': {'rate_bps_per_hour': '0', 'source_ts': 195}}
    data = funding_dashboard(snap)
    assert data['risk']['entropy']['distance'] == ('0.01' if expected else None)
    assert data['rates']['entropy']['rate_bps_per_hour'] == '0'
    assert data['rates']['hedge']['rate_bps_per_hour'] is None
    assert 'NEVER_EXPOSE_THIS' not in json.dumps(data)


def test_late_posting_stays_on_original_cycle_and_does_not_change_final_pnl():
    snap = snapshot()
    snap['journal']['funding_cycles'].append({'cycle_id': 'cycle-0', 'hedged_ts': 110,
                                              'exit_reason': 'funding_reversal_cost', 'completed': 0})
    snap['journal']['funding_settlements'].append({
        'cycle_id': 'cycle-0', 'venue': 'entropy', 'event_id': 'late-old',
        'ts': 7201, 'amount_usd': '-0.5', 'source': 'test'})
    data = overview(snap, resolve_window({'scope': 'session'}, snap['sources']['live']['data'], now=200))
    assert data['funding']['state']['actual_funding_usd'] == '0.0'
    assert next(row for row in data['recent_cycles'] if row['cycle_id'] == 'cycle-0')[
        'funding_state']['actual_funding_usd'] == '-0.5'
    assert data['kpis']['finalized_net_pnl_usd'] == '-0.001694'


def test_preflight_only_is_displayed_without_fake_live_or_account_verification(tmp_path):
    paths = SourcePaths(*(tmp_path / name for name in (
        'journal.sqlite3', 'status.json', 'manifest.json', 'diagnostics.json',
        'recorder.json', 'recorder-manifest.json', 'recorder-data', 'reports', 'cache')))
    (tmp_path / 'public-preflight-status.json').write_text(json.dumps({
        'kind': 'nbis_public_preflight', 'pair_identity': 'nbis-funding:hl:NBIS:lighter:NBIS',
        'ok': True, 'mode': 'public-preflight-only', 'start_ts': time.time() - 10,
        'live_ready': False, 'account_verified': False}))
    with TestClient(create_app(paths, start_collector=False)) as client:
        response = client.get('/api/v3/overview')
        assert response.status_code == 200
        data = response.json()['data']
        assert data['live'] is None
        assert data['funding']['live_quality'] == 'unavailable'
        assert data['funding']['state'] is None
        assert data['funding']['public_preflight']['account_verified'] is False
        assert data['funding']['net_usd_per_hour'] is None
        assert client.post('/api/v3/overview').status_code == 405
    assert not paths.journal.exists()


def test_legacy_mode_does_not_adopt_nbis_preflight():
    snap = snapshot()
    snap['sources']['live_manifest']['data']['settings']['strategy_mode'] = 'basis_reversion'
    snap['sources']['public_preflight'] = {'data': {'kind': 'nbis_public_preflight'}}
    assert funding_dashboard(snap) is None


def test_collector_reads_funding_tables_without_migration(tmp_path):
    path = tmp_path / 'journal.sqlite3'
    with sqlite3.connect(path) as db:
        db.executescript('''CREATE TABLE metadata(key TEXT, value TEXT);
            INSERT INTO metadata VALUES ('economics_schema_version', '8');
            CREATE TABLE funding_cycles(cycle_id TEXT, hedged_ts REAL);
            INSERT INTO funding_cycles VALUES ('cycle', 100);
            CREATE TABLE funding_settlements(cycle_id TEXT, amount_usd TEXT);
            INSERT INTO funding_settlements VALUES ('cycle', '0');''')
    before = path.read_bytes()
    source = journal_source(path)
    assert source['quality'] == 'available'
    assert source['data']['funding_cycles'][0]['hedged_ts'] == 100
    assert source['data']['funding_settlements'][0]['amount_usd'] == '0'
    assert path.read_bytes() == before


def test_status_fallback_is_whitelisted_and_cycle_matched():
    snap = snapshot()
    snap['journal']['funding_cycles'] = []
    snap['sources']['live']['data']['funding_state'] = {
        'cycle_id': 'cycle-7', 'actual_funding_usd': '0',
        'settlement_counts': {'entropy': 0, 'hedge': 0, 'secret': 'NEVER_EXPOSE_THIS'},
        'private_key': 'NEVER_EXPOSE_THIS'}
    data = funding_dashboard(snap)
    assert data['state_source'] == 'live_status'
    assert data['state']['settlement_counts'] == {'entropy': 0, 'hedge': 0}
    assert 'NEVER_EXPOSE_THIS' not in json.dumps(data)
    snap['sources']['live']['data']['funding_state']['cycle_id'] = 'old-cycle'
    assert funding_dashboard(snap)['state'] is None


def test_current_batch_filter_uses_manifest_when_live_status_omits_batch_id(tmp_path):
    paths = SourcePaths(*(tmp_path / name for name in (
        'journal.sqlite3', 'status.json', 'manifest.json', 'diagnostics.json',
        'recorder.json', 'recorder-manifest.json', 'recorder-data', 'reports', 'cache')))
    snap = snapshot()
    snap['generated_at'] = time.time()
    del snap['sources']['live']['data']['batch_id']
    with TestClient(create_app(paths, start_collector=False)) as client:
        client.app.state.v3_read_model.snapshot = snap
        response = client.get('/api/v3/overview?scope=batch')
        assert response.status_code == 200
        assert response.json()['meta']['window']['batch_id'] == 'batch'
        assert response.json()['data']['funding']['batch']['batch_id'] == 'batch'
        assert client.get('/api/v3/overview', params={
            'scope': 'custom', 'from_ts': 1, 'to_ts': snap['generated_at']}).status_code == 400
