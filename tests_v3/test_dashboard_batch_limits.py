"""Batch caps are independent from accumulated statistics; in-memory projections only."""
from copy import deepcopy

import pytest
from fastapi.testclient import TestClient

from entropy_arb.dashboard_api.v3_app import create_app
from entropy_arb.dashboard_api.v3_readmodel import SourcePaths

from entropy_arb.dashboard_api.v3_projection import funding_dashboard, public_status
from entropy_arb.dashboard_api.v3_deployments import deployment_info
from test_dashboard_funding import snapshot


def disabled_snapshot():
    snap = snapshot()
    snap['sources']['live']['data'].update(position_cycles=94, loss_used_usd='3.47865123')
    snap['sources']['live_manifest']['data']['authorization'].update(
        max_position_cycles=None, loss_budget_usd=None)
    snap['journal']['batches'][0].update(max_position_cycles='disabled', loss_budget='disabled')
    return snap


def test_disabled_funding_limits_preserve_actual_stats_and_snapshot():
    snap = disabled_snapshot()
    original = deepcopy(snap)
    data = funding_dashboard(snap)['batch']
    assert data['max_position_cycles'] == data['loss_budget_usd'] == 'disabled'
    assert data['position_cycles'] == 94
    assert data['loss_used_usd'] == '3.47865123'
    assert snap == original


@pytest.mark.parametrize('mode', ['basis', 'funding'])
def test_both_deployment_raw_caps_preserve_stats(mode):
    snap = disabled_snapshot()
    snap['sources']['live_manifest']['data']['settings']['strategy_mode'] = (
        'funding_hedge' if mode == 'funding' else 'basis_reversion')
    data = deployment_info(mode, snap)
    assert data['mode'] == mode
    assert data['batch']['max_position_cycles'] == data['batch']['loss_budget_usd'] == 'disabled'
    assert data['batch']['position_cycles'] == 94
    assert data['batch']['loss_used_usd'] == '3.47865123'


def test_explicit_authorization_null_disables_only_if_no_raw_cap():
    snap = disabled_snapshot()
    for key in ('max_position_cycles', 'loss_budget'):
        del snap['journal']['batches'][0][key]
    assert funding_dashboard(snap)['batch']['max_position_cycles'] == 'disabled'
    assert funding_dashboard(snap)['batch']['loss_budget_usd'] == 'disabled'
    for key in ('max_position_cycles', 'loss_budget_usd'):
        del snap['sources']['live_manifest']['data']['authorization'][key]
    assert funding_dashboard(snap)['batch']['max_position_cycles'] is None
    assert funding_dashboard(snap)['batch']['loss_budget_usd'] is None


@pytest.mark.parametrize('raw', [None, 0, 'disabled'])
def test_raw_cap_is_not_truthy_replaced_by_old_authorization(raw):
    snap = disabled_snapshot()
    snap['sources']['live_manifest']['data']['authorization'].update(max_position_cycles=100, loss_budget_usd='3')
    snap['journal']['batches'][0].update(max_position_cycles=raw, loss_budget=raw)
    data = funding_dashboard(snap)['batch']
    assert data['max_position_cycles'] == data['loss_budget_usd'] == raw


def test_stat_fallback_keeps_existing_cycle_count_and_zero_values():
    snap = disabled_snapshot()
    snap['sources']['live']['data'].update(position_cycles=None, loss_used_usd=None)
    snap['journal']['batches'][0]['loss_used_usd'] = '3.47865123'
    data = funding_dashboard(snap)['batch']
    assert data['position_cycles'] == 8
    assert data['loss_used_usd'] == '3.47865123'
    snap['sources']['live']['data'].update(position_cycles=0, loss_used_usd='0')
    data = funding_dashboard(snap)['batch']
    assert data['position_cycles'] == 0
    assert data['loss_used_usd'] == '0'


@pytest.mark.parametrize('mode', ['basis', 'funding'])
def test_system_get_exposes_disabled_raw_caps_and_actual_stats(tmp_path, mode):
    snap = disabled_snapshot()
    snap['sources']['live_manifest']['data']['settings']['strategy_mode'] = (
        'funding_hedge' if mode == 'funding' else 'basis_reversion')
    paths = SourcePaths(*(tmp_path / name for name in (
        'journal.sqlite3', 'status.json', 'manifest.json', 'diagnostics.json',
        'recorder.json', 'recorder-manifest.json', 'recorder-data', 'reports', 'cache')))
    with TestClient(create_app(paths, start_collector=False)) as client:
        client.app.state.v3_read_model.snapshot = snap
        response = client.get('/api/v3/system?scope=all')
        assert response.status_code == 200
        data = response.json()['data']['batch']
        assert data['max_position_cycles'] == data['loss_budget_usd'] == 'disabled'
        assert data['position_cycles'] == 94
        assert data['loss_used_usd'] == '3.47865123'
    assert not paths.journal.exists()


def test_runtime_nullable_caps_and_explicit_flags_are_public_evidence():
    raw = {'position_cycles': 94, 'max_position_cycles': None,
           'loss_used_usd': '3.47865123', 'loss_budget_usd': None,
           'max_position_cycles_disabled': True, 'loss_budget_disabled': True,
           'account_id': 'not_public'}
    data = public_status(raw)
    assert data == {key: value for key, value in raw.items() if key != 'account_id'}
    assert public_status({'max_position_cycles_disabled': False,
                          'loss_budget_disabled': False}) == {
        'max_position_cycles_disabled': False, 'loss_budget_disabled': False}
    assert 'max_position_cycles_disabled' not in public_status({})
