from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from entropy_arb.dashboard_api.v3_deployments import deployment_info
from entropy_arb.dashboard_api.v3_projection import manual_exit_view
from entropy_arb.dashboard_api.v3_readmodel import SourcePaths, V3ReadModel, manual_exit_source


def exit_snapshot():
    pair = 'default:hl:SNDK:lighter:SNDK'
    account = json.dumps({'hl_query_account': '0x' + 'a' * 40,
                          'lighter_account_index': 1, 'lighter_profile': 'mainnet'}, sort_keys=True)
    orders = []
    for venue, opening, closing in (('entropy', 'buy', 'sell'), ('hedge', 'sell', 'buy')):
        for purpose, side, stamp in (('open', opening, 80), ('close', closing, 188)):
            orders.append({'order_ref': f'{venue}-{purpose}', 'cycle_id': 'cycle', 'venue': venue,
                           'side': side, 'requested_units': 10, 'filled_units': 10,
                           'reduce_only': int(purpose == 'close'), 'state': 'TERMINAL',
                           'submitted_ts': stamp - 1, 'terminal_recorded_ts': stamp})
    journal = {
        'metadata': {'identity': f'{pair}:account', 'manual_exit_account:cycle': account,
                     'manual_exit:cycle': json.dumps({'requested_ts': 150})},
        'batches': [{'batch_id': 'batch', 'closed_for_entries': 1, 'close_reason': 'MANUAL_STOP',
                     'close_cycle_id': 'cycle', 'loss_budget': '3'}],
        'live_cycles': [{'cycle_id': 'cycle', 'batch_id': 'batch', 'phase': 'DONE',
                         'entropy_units': 0, 'hedge_units': 0, 'started_ts': 50,
                         'updated_ts': 199, 'last_authoritative_ts': 189,
                         'position_evidence_authoritative': 1, 'accounting_final': 0}],
        'cycles': [{'cycle_id': 'cycle', 'batch_id': 'batch', 'completed_ts': 191,
                    'accounting_final': 1, 'complete_accounting': 1, 'pnl_usd': '-0.004772'}],
        'cycle_economics': [{'cycle_id': 'cycle', 'batch_id': 'batch', 'accounting_final': 1,
                             'finalized_pnl_usd': '-0.004772', 'actual_fees_usd': '0.007414',
                             'actual_funding_usd': '0.005469', 'entropy_fees_usd': '0.007414',
                             'hedge_fees_usd': '0', 'entropy_funding_usd': '-0.000584',
                             'hedge_funding_usd': '0.006053'}],
        'orders': orders, 'live_steps': [], 'execution_economics_steps': [], 'request_events': [],
    }
    live = {'current_state': 'HOLDING', 'cycle_id': 'cycle', 'source_ts': 100,
            'start_ts': 40, 'pair_identity': pair, 'config_hash': 'config',
            'closed_for_entries': False, 'close_reason': None}
    manual = {'mode': 'manual-exit', 'cycle_id': 'cycle', 'source_ts': 200,
              'position_observed_ts': 190, 'pair_identity': pair, 'config_hash': 'config',
              'success': True, 'remaining': {'entropy_units': 0, 'hedge_units': 0,
                                          'authoritative': True},
              'active_cycle': None, 'pending': [], 'accounting_final': True,
              'net_pnl_usd': '-0.004772'}
    return {'id': 'fixture', 'generated_at': 300, 'journal': journal, 'market': [], 'sources': {
        'live': {'data': live}, 'manual_exit': {'data': manual}, 'live_manifest': {'data': {
            'pair_identity': pair, 'config_hash': 'config', 'authorization': {'batch_id': 'batch'}}}}}


def test_completed_exit_supersedes_same_cycle_old_live_and_preserves_observation_time():
    snap = exit_snapshot()
    before = deepcopy(snap)
    info = deployment_info('basis', snap)
    result = info['manual_exit']
    assert result['state'] == 'complete'
    assert result['supersedes_live'] is True
    assert result['accounting_final'] is True  # live_cycles.accounting_final remains zero.
    assert result['net_pnl_usd'] == '-0.004772'
    assert result['position_observed_ts'] == 190
    assert result['source_ts'] == 200
    assert result['remaining'] == {'entropy_units': 0, 'hedge_units': 0, 'authoritative': True}
    assert [leg['closed_units'] for leg in result['legs']] == [10, 10]
    assert info['status']['current_state'] == 'HOLDING'
    assert info['status']['source_ts'] == 100
    assert info['batch']['closed_for_entries'] == 1
    assert info['batch']['close_reason'] == 'MANUAL_STOP'
    assert snap == before  # Projection cannot alter the ledger or original source.


@pytest.mark.parametrize('field,value', [('cycle_id', 'other'), ('batch_id', 'other'),
                                       ('pair_identity', 'other'), ('config_hash', 'other')])
def test_mismatched_exit_cannot_claim_flat(field, value):
    snap = exit_snapshot()
    snap['sources']['manual_exit']['data'][field] = value
    result = manual_exit_view(snap)
    assert result['state'] == 'unknown'
    assert result['supersedes_live'] is False
    assert result['remaining']['entropy_units'] is None
    assert result['accounting_final'] is False


@pytest.mark.parametrize('key', ['manual_exit_account:cycle', 'manual_exit:cycle'])
def test_missing_persisted_account_or_request_binding_is_unknown(key):
    snap = exit_snapshot()
    del snap['journal']['metadata'][key]
    result = manual_exit_view(snap)
    assert result['reason'] == 'exit_account_binding_unverified'
    assert result['remaining']['authoritative'] is False


def test_account_fingerprint_must_match_if_published():
    snap = exit_snapshot()
    raw = snap['sources']['manual_exit']['data']
    raw['account_scope_hash'] = '0' * 64
    assert manual_exit_view(snap)['state'] == 'unknown'
    raw['account_scope_hash'] = hashlib.sha256(
        snap['journal']['metadata']['manual_exit_account:cycle'].encode()).hexdigest()
    assert manual_exit_view(snap)['state'] == 'complete'


def test_old_exit_is_historical_when_live_or_another_cycle_is_newer():
    snap = exit_snapshot()
    snap['sources']['live']['data']['source_ts'] = 210
    result = manual_exit_view(snap)
    assert result['state'] == 'complete'
    assert result['supersedes_live'] is False
    assert result['reason'] == 'historical_exit_evidence'
    snap = exit_snapshot()
    snap['journal']['live_cycles'].append({'cycle_id': 'new-cycle', 'batch_id': 'batch',
                                          'phase': 'HOLDING', 'started_ts': 220})
    result = manual_exit_view(snap)
    assert result['supersedes_live'] is False
    assert result['reason'] == 'newer_cycle_present'


@pytest.mark.parametrize('change', ['nonzero', 'pending', 'active', 'no_authority',
                                   'ledger_conflict', 'late_terminal', 'future_timestamp'])
def test_flat_claim_needs_all_runtime_and_ledger_proofs(change):
    snap = exit_snapshot()
    raw = snap['sources']['manual_exit']['data']
    if change == 'nonzero':
        raw['remaining']['entropy_units'] = 1
    elif change == 'pending':
        raw['pending'] = [{'order_ref': 'unresolved', 'venue': 'hedge', 'state': 'UNKNOWN'}]
    elif change == 'active':
        raw['active_cycle'] = {'cycle_id': 'cycle'}
    elif change == 'no_authority':
        raw['remaining']['authoritative'] = False
    elif change == 'ledger_conflict':
        snap['journal']['live_cycles'][0]['entropy_units'] = 1
    elif change == 'late_terminal':
        snap['journal']['orders'][-1]['terminal_recorded_ts'] = 201
    elif change == 'future_timestamp':
        raw['source_ts'] = 400
    result = manual_exit_view(snap)
    assert result['state'] == 'unknown'
    assert result['supersedes_live'] is False
    assert result['accounting_final'] is False


@pytest.mark.parametrize('field', ['accounting_final', 'actual_fees_usd', 'entropy_funding_usd',
                                 'finalized_pnl_usd'])
def test_completed_exit_waits_for_consistent_accounting(field):
    snap = exit_snapshot()
    snap['journal']['cycle_economics'][0][field] = 0 if field == 'accounting_final' else None
    result = manual_exit_view(snap)
    assert result['state'] == 'accounting_pending'
    assert result['supersedes_live'] is True
    assert result['accounting_final'] is False
    assert result['net_pnl_usd'] is None


def test_later_accounting_update_keeps_original_exit_position_timestamp():
    snap = exit_snapshot()
    snap['journal']['live_cycles'][0]['updated_ts'] = 250
    result = manual_exit_view(snap)
    assert result['state'] == 'complete'
    assert result['position_observed_ts'] == 190
    assert result['supersedes_live'] is True
    snap['journal']['cycle_economics'][0]['hedge_fees_usd'] = '1'
    assert manual_exit_view(snap)['state'] == 'accounting_pending'


def progress_snapshot(*, unknown=False):
    snap = exit_snapshot()
    raw = snap['sources']['manual_exit']['data']
    raw.update(success=False, active_cycle={'cycle_id': 'cycle'}, accounting_final=False,
               net_pnl_usd=None, remaining={'entropy_units': 6, 'hedge_units': -8,
                                          'authoritative': False})
    snap['journal']['live_cycles'][0].update(phase='BASIS_EXIT', entropy_units=6,
                                            hedge_units=-8, position_evidence_authoritative=0)
    closes = [row for row in snap['journal']['orders'] if row['reduce_only']]
    closes[0]['filled_units'] = 4
    closes[1].update(filled_units=2, state='UNKNOWN' if unknown else 'TERMINAL')
    return snap


def test_unknown_and_known_partial_leg_progress_use_original_cumulative_fills():
    snap = progress_snapshot(unknown=True)
    # Duplicate observation of one order must not double the cumulative fill.
    snap['journal']['orders'].append(deepcopy(snap['journal']['orders'][1]))
    result = manual_exit_view(snap)
    assert result['state'] == 'unknown'
    assert result['legs'][0] == {'venue': 'entropy', 'initial_units': 10,
                                 'closed_units': 4, 'remaining_units': 6, 'state': 'partial'}
    assert result['legs'][1]['state'] == 'unknown'
    assert result['legs'][1]['closed_units'] == 2
    assert result['pending'][0]['order_ref'] == 'hedge-close'
    assert result['pending'][0]['state'] == 'UNKNOWN'
    assert result['supersedes_live'] is False


def test_partial_and_executing_are_distinct_and_stale_status_uses_ledger_observation():
    snap = progress_snapshot()
    result = manual_exit_view(snap)
    assert result['state'] == 'partial'
    raw = snap['sources']['manual_exit']['data']
    raw['source_ts'] = 198  # Ledger updated at 199; raw positions cannot override it.
    raw['remaining']['entropy_units'] = 10
    result = manual_exit_view(snap)
    assert result['legs'][0]['remaining_units'] == 6
    assert result['position_observed_ts'] == 189
    for row in snap['journal']['orders']:
        if row['reduce_only']:
            row.update(filled_units=0, state='SUBMITTED')
    snap['journal']['live_cycles'][0].update(entropy_units=10, hedge_units=-10)
    assert manual_exit_view(snap)['state'] == 'executing'


def test_collector_reads_adjacent_exit_file_with_strict_nested_whitelist(tmp_path: Path):
    paths = SourcePaths(*(tmp_path / name for name in ('journal.sqlite3', 'custom-live.json',
        'manifest.json', 'diagnostics.json', 'recorder.json', 'recorder-manifest.json',
        'recorder-data', 'reports', 'cache')))
    raw = exit_snapshot()['sources']['manual_exit']['data']
    raw.update(secret='SHOULD_NOT_PUBLISH', hl_query_account='PRIVATE_ACCOUNT',
               reason='token=SHOULD_NOT_PUBLISH', environment={'token': 'SHOULD_NOT_PUBLISH'},
               schema_version={'token': 'SHOULD_NOT_PUBLISH'}, start_ts='PRIVATE_ACCOUNT')
    raw['remaining']['account'] = 'PRIVATE_ACCOUNT'
    raw['remaining']['hedge_units'] = 'PRIVATE_ACCOUNT'
    raw['active_cycle'] = {'cycle_id': 'cycle', 'password': 'SHOULD_NOT_PUBLISH'}
    raw['pending'] = [{'order_ref': 'safe', 'venue': 'hedge', 'state': 'UNKNOWN',
                       'filled_units': 2, 'credential': 'SHOULD_NOT_PUBLISH'}]
    path = tmp_path / 'manual-exit-status.json'
    path.write_text(json.dumps(raw))
    source = manual_exit_source(path)
    encoded = json.dumps(source)
    assert 'SHOULD_NOT_PUBLISH' not in encoded
    assert 'PRIVATE_ACCOUNT' not in encoded
    assert source['data']['pending'][0]['filled_units'] == 2
    model = V3ReadModel(paths)
    model.refresh()
    assert model.snapshot['sources']['manual_exit']['data'] == source['data']
    assert not paths.journal.exists()


def test_missing_exit_source_stays_absent():
    snap = exit_snapshot()
    del snap['sources']['manual_exit']
    assert manual_exit_view(snap) is None
