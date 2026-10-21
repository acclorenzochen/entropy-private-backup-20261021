from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import time

from fastapi.testclient import TestClient
import pytest

from entropy_arb.dashboard_api import v3_app
from entropy_arb.dashboard_api import v3_projection
from entropy_arb.dashboard_api.v3_projection import (
    live_market_points, market_points, resolve_window)
from entropy_arb.dashboard_api.v3_readmodel import (
    LiveMarketTail, MarketTail, SourcePaths, compact_live_market, compact_market, requote_market)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def prohibited(*args, **kwargs):
        raise AssertionError('Network access is prohibited in market series fixtures')
    monkeypatch.setattr(socket.socket, 'connect', prohibited)


def paths(root):
    root.mkdir(parents=True, exist_ok=True)
    return SourcePaths(*(root / name for name in ('journal.sqlite3', 'status.json',
        'status.effective_manifest.json', 'diagnostics.json', 'record-status.edge.json',
        'record-status.edge.effective_manifest.json', '.', 'reports', 'cache')))


def write_rows(path, rows):
    path.write_text(''.join(json.dumps(row)+'\n' for row in rows))


def fixed(seq=1, ts=100, **extra):
    return {'kind': 'v3_market_snapshot', 'schema_version': 2, 'pair_id': 'p',
        'session_id': 's', 'sample_seq': seq, 'capture_wall_ts': ts, 'valid': True,
        'reference_bps': '105', 'instrument': {'quantity_step': '0.0001'},
        'entropy': {'asks': [['100', 2000]], 'bids': [['99', 2000]]},
        'lighter': {'asks': [['100', 2000]], 'bids': [['99', 2000]]}, **extra}


def summary(ts=100, **extra):
    return {'kind': 'v3_market', 'pair_identity': 'p', 'source_ts': ts,
        'basis_bps': '2', 'reference_bps': '1', 'reference_usable': True, **extra}


def snapshot(rows=(), live=(), now=200):
    return {'id': 'fixture', 'generated_at': now, 'journal': None,
        'market': list(rows), 'live_market': list(live), 'sources': {
            'live': {'data': None}, 'live_manifest': {'data': {'settings': {'entry_band_bps': '3',
                'quantity_step': '100'}}},
            'market': {'quality': 'available', 'backfill_complete': True},
            'live_market': {'quality': 'available', 'backfill_complete': True}}}


def all_window():
    return resolve_window({'scope': 'all'}, None, now=200)


def test_live_summary_is_whitelisted_and_never_executable_depth(tmp_path):
    write_rows(tmp_path/'observer.jsonl', [summary(secret='PRIVATE', account='ACCOUNT',
        entropy={'asks': [['999', 2000]]}, hedge={'bids': [['998', 2000]]}, units=1245)])
    live = LiveMarketTail(tmp_path, tmp_path/'cache')
    live.poll()
    rows = list(live.iter_window(None, None))
    assert len(rows) == 1 and rows[0]['session_id'] is None
    assert not {'secret', 'account', 'entropy', 'hedge', 'units'} & rows[0].keys()
    result = live_market_points(snapshot(), all_window(), cached_rows=iter(rows))
    point = result['points'][0]
    assert point['mid_basis_bps'] == '2' and point['reference_bps'] == '1'
    assert not {'buy_basis_bps', 'sell_basis_bps', 'long_edge_bps', 'short_edge_bps'} & point.keys()
    assert '_source_span_id' not in point
    with sqlite3.connect(live.cache_dir/'readmodel.sqlite3') as db:
        payload = db.execute('SELECT payload FROM live_market_samples').fetchone()[0]
    assert 'PRIVATE' not in payload and 'ACCOUNT' not in payload


@pytest.mark.parametrize('change', [
    {'source_ts': 'NaN'}, {'source_ts': True}, {'source_ts': -1}, {'source_ts': 1e300},
    {'pair_identity': 'https://private.invalid'}, {'kind': 'v3_cycle_economics'}])
def test_malformed_summary_identity_or_clock_is_rejected(change):
    assert compact_live_market(summary(**change)) is None


@pytest.mark.parametrize('usable', [False, 'true', 1, None])
def test_unusable_reference_is_not_plotted(usable):
    row = compact_live_market(summary(reference_usable=usable))
    assert row['reference_bps'] is None and row['reference_usable'] is False


def test_invalid_mid_has_explicit_gap_without_coercing_to_zero():
    row = compact_live_market(summary(basis_bps='NaN'))
    output = live_market_points(snapshot(live=[row]), all_window())
    assert output['points'][0]['mid_basis_bps'] is None
    assert output['points'][0]['continuity_break']


def test_live_half_line_rotation_and_restart_checkpoint(tmp_path):
    active = tmp_path/'observer.jsonl'
    active.write_text(json.dumps(summary()))
    tail = LiveMarketTail(tmp_path, tmp_path/'cache')
    tail.poll()
    assert tail.points == []
    with active.open('a') as output:
        output.write('\n')
    tail.poll()
    active.rename(tmp_path/'observer.20261009.jsonl')
    write_rows(active, [summary(101)])
    tail.poll()
    restarted = LiveMarketTail(tmp_path, tmp_path/'cache')
    restarted.poll()
    rows = list(restarted.iter_window(None, None))
    assert len(rows) == 2
    assert rows[1]['continuity_break']  # another file; continuity is unproven


def test_startup_marker_boundary_survives_collector_restart(tmp_path):
    active = tmp_path/'observer.jsonl'
    write_rows(active, [summary(), {'kind': 'v3_batch_startup'}])
    tail = LiveMarketTail(tmp_path, tmp_path/'cache')
    tail.poll()
    with active.open('a') as output:
        output.write(json.dumps(summary(101))+'\n')
    restarted = LiveMarketTail(tmp_path, tmp_path/'cache')
    restarted.poll()
    assert list(restarted.iter_window(None, None))[1]['continuity_break']


def test_live_cold_tail_backfills_without_fake_clock_rollback(tmp_path):
    write_rows(tmp_path/'observer.jsonl', [summary(n) for n in range(1, 20)])
    tail = LiveMarketTail(tmp_path, tmp_path/'cache')
    for _ in range(100):
        tail.poll(max_bytes=300)
        if tail.backfill_complete:
            break
    rows = list(tail.iter_window(None, None))
    assert len(rows) == 19 and tail.backfill_complete
    assert not any(row['continuity_break'] for row in rows)


def test_live_truncation_keeps_new_summary_and_marks_a_boundary(tmp_path):
    active = tmp_path/'observer.jsonl'
    write_rows(active, [summary(100, ignored_padding='x'*1000)])
    tail = LiveMarketTail(tmp_path, tmp_path/'cache')
    tail.poll()
    write_rows(active, [summary(101)])
    tail.poll()
    rows = list(tail.iter_window(None, None))
    assert [r['capture_wall_ts'] for r in rows] == [100, 101]
    assert rows[1]['continuity_break']


@pytest.mark.parametrize('change', [
    {'capture_wall_ts': 110}, {'session_id': 'another'},
    {'_source_span_id': 'another-file'}, {'continuity_break': True}])
def test_live_breaks_only_on_saved_boundaries(change):
    rows = [compact_live_market(summary(100, session_id='s')),
            {**compact_live_market(summary(101, session_id='s')), **change}]
    result = live_market_points(snapshot(live=rows), all_window())
    assert result['points'][1]['continuity_break']


def test_downsampling_does_not_create_time_gaps_and_preserves_all_curve_extrema():
    rows = [fixed(n+1, 100+n*.2, reference_bps=None,
        entropy={'asks': [['110' if n == 151 else '100', 2000]], 'bids': [['99', 2000]]})
        for n in range(1000)]
    output = market_points(snapshot(rows), all_window(), units=1245, max_points=50)
    assert any(p['sample_seq'] == 152 for p in output['points'])
    assert any(b['ts']-a['ts'] > 3 for a, b in zip(output['points'], output['points'][1:]))
    assert not any(p['continuity_break'] for p in output['points'])
    assert output['coverage']['discontinuity_count'] == 0


def test_raw_invalid_and_sequence_gaps_propagate_across_downsampling():
    rows = [fixed(n+1, 100+n*.2, valid=n != 151) for n in range(1000)]
    rows[800]['sample_seq'] = 3000
    output = market_points(snapshot(rows), all_window(), units=1245, max_points=50)
    assert output['coverage']['discontinuity_count'] == 3
    assert any(p['continuity_break'] for p in output['points'])
    full = market_points(snapshot(rows), all_window(), units=1245, max_points=1200)
    assert output['episodes'] == full['episodes']
    assert all(e['live_decision_link'] == 'unproven' and e['created_cycle_id'] is None for e in output['episodes'])


def test_live_downsampling_preserves_extrema_without_fake_gaps():
    rows = [compact_live_market(summary(100+n*.2, basis_bps='100' if n == 152 else '2'))
            for n in range(1000)]
    output = live_market_points(snapshot(live=rows), all_window(), max_points=50)
    assert any(p['mid_basis_bps'] == '100' for p in output['points'])
    assert not any(p['continuity_break'] for p in output['points'])


def test_missing_recorder_is_missing_not_backfill_with_live_evidence(tmp_path):
    tail = MarketTail(tmp_path, tmp_path/'cache')
    tail.poll()
    snap = snapshot(live=[compact_live_market(summary())])
    snap['sources']['market'] = {'quality': tail.quality, 'reason': tail.reason,
                                'backfill_complete': tail.backfill_complete}
    result = market_points(snap, all_window(), units=1245)
    assert result['coverage']['reason'] == 'recorder_source_missing'
    assert 'historical_backfill_in_progress' not in result['coverage']['reasons']
    assert result['points'] == [] and result['quantity_step'] is None
    assert live_market_points(snap, all_window())['coverage']['loaded_rows'] == 1


def test_present_summary_file_without_market_rows_is_not_missing_or_backfill(tmp_path):
    write_rows(tmp_path/'observer.jsonl', [{'kind': 'v3_batch_startup'}])
    tail = LiveMarketTail(tmp_path, tmp_path/'cache')
    tail.poll()
    snap = snapshot()
    snap['sources']['live_market'] = {'quality': tail.quality, 'reason': tail.reason,
        'backfill_complete': tail.backfill_complete}
    result = live_market_points(snap, all_window())
    assert result['coverage']['reason'] == 'no_live_summary_samples_in_selected_window'
    assert 'historical_backfill_in_progress' not in result['coverage']['reasons']


def test_coverage_reports_both_edges_stale_and_raw_gaps():
    snap = snapshot([fixed(1, 100), fixed(2, 200)], now=400)
    window = resolve_window({'scope': 'custom', 'from_ts': 1, 'to_ts': 399}, None, now=400)
    output = market_points(snap, window, units=1245)
    coverage = output['coverage']
    assert coverage['latest_ts'] == 200 and coverage['quality'] == 'stale'
    assert set(coverage['reasons']) == {'window_starts_before_recorder_coverage',
        'window_ends_after_recorder_coverage', 'recorder_samples_have_gaps_or_invalid_frames',
        'recorder_source_stale'}


@pytest.mark.parametrize('steps,expected', [(['0.0001', '0.0001'], '0.0001'),
    (['0.0001', '0.001'], None), (['0.0001', None], None)])
def test_quantity_step_requires_consistent_recorded_evidence(steps, expected):
    rows = [compact_market(fixed(i+1, 100+i, instrument={'quantity_step': step} if step else {}))
            for i, step in enumerate(steps)]
    output = market_points(snapshot(rows), all_window(), units=1245)
    assert output['quantity_step'] == expected
    assert output['base_quantity'] == ('0.1245' if expected else None)


def test_threshold_provenance_requires_matching_config_hash():
    snap = snapshot([fixed(config_hash='recorded')])
    output = market_points(snap, all_window(), units=1245)
    assert output['entry_band_source'] == 'current_live_display_threshold'
    snap['sources']['recorder_manifest'] = {'data': {'config_hash': 'recorded',
        'settings': {'entry_band_bps': '5'}}}
    output = market_points(snap, all_window(), units=1245)
    assert output['entry_band_source'] == 'recorded_config' and output['entry_band_bps'] == '5'
    assert output['display_entry_band_bps'] == '3'
    assert all(e['live_decision_link'] == 'unproven' for e in output['episodes'])


def test_unusable_recorded_reference_does_not_produce_edge():
    point = requote_market(fixed(reference_usable=False), 1245, '3')
    assert point['reference_bps'] is None and point['long_edge_bps'] is None


def test_api_separates_deployments_and_does_not_write_on_get(tmp_path):
    basis, funding = paths(tmp_path/'basis'), paths(tmp_path/'funding')
    write_rows(basis.recorder_data_dir/'record-observer.edge.jsonl', [fixed()])
    write_rows(basis.live_status.parent/'observer.jsonl', [summary()])
    write_rows(funding.live_status.parent/'observer.jsonl', [summary(pair_identity='nbis-funding:hl:NBIS:lighter:NBIS')])
    app = v3_app.create_app(basis, start_collector=False, deployment_paths={'basis': basis, 'funding': funding})
    with TestClient(app) as client:
        files = [p for d in (basis.cache_dir, funding.cache_dir) for p in d.glob('readmodel.sqlite3*') if p.suffix != '-shm' and not p.name.endswith('-shm')]
        hashes = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
        assert client.get('/api/v3/market?scope=all').status_code == 400
        output = client.get('/api/v3/market?source=basis&scope=all&units=1245').json()['data']
        assert len(output['points']) == len(output['live_points']) == 1
        assert output['quantity_step'] == '0.0001' and output['base_quantity'] == '0.1245'
        assert output['series_sources']['live_summary']['reconnect_coverage'] == 'unknown_when_not_recorded'
        assert output['series_sources']['fixed_q']['decision_link'] == 'unproven'
        missing = client.get('/api/v3/market?source=funding&scope=all').json()['data']
        assert missing['points'] == [] and len(missing['live_points']) == 1
        assert missing['coverage']['reason'] == 'recorder_source_missing'
        assert missing['live_coverage']['latest_ts'] == 100
        assert client.post('/api/v3/market').status_code == 405
        assert all(hashlib.sha256(p.read_bytes()).hexdigest() == digest for p, digest in hashes.items())
    assert not basis.journal.exists() and not funding.journal.exists()


def test_projection_cache_reuses_historical_bounds_is_bounded_and_invalidates(tmp_path, monkeypatch):
    source = paths(tmp_path)
    active = source.recorder_data_dir/'record-observer.edge.jsonl'
    write_rows(active, [fixed(ts=100000)])
    calls = []
    original = v3_app.market_points
    def counted(*args, **kwargs):
        calls.append(kwargs['units'])
        return original(*args, **kwargs)
    monkeypatch.setattr(v3_app, 'market_points', counted)
    app = v3_app.create_app(source, start_collector=False)
    with TestClient(app) as client:
        model = app.state.v3_read_model
        model.snapshot['generated_at'] = 700000
        url = '/api/v3/market?scope=7d&units='
        first = client.get(url+'100').json()['data']
        model.snapshot['generated_at'] = 700010
        second = client.get(url+'100').json()['data']
        assert calls == [100] and first['points'] == second['points']
        assert second['coverage']['requested_to_ts'] == 700010
        for units in (200, 300, 100):
            client.get(url+str(units))
        assert calls == [100, 200, 300, 100]  # at most two keys
        with active.open('a') as output:
            output.write(json.dumps(fixed(2, 100001))+'\n')
        model.refresh()
        third = client.get('/api/v3/market?scope=all&units=100').json()['data']
        assert len(third['points']) == 2 and calls[-1] == 100
        model.snapshot['sources']['live_manifest']['data'] = {'settings': {'entry_band_bps': '9'}}
        updated = client.get('/api/v3/market?scope=all&units=100').json()['data']
        assert updated['entry_band_bps'] == '9' and len(calls) == 6


@pytest.mark.parametrize('series,unused', [('live', 'market_points'), ('fixed', 'live_market_points')])
def test_selected_series_never_calculates_unused_projection(tmp_path, monkeypatch, series, unused):
    source = paths(tmp_path)
    write_rows(tmp_path/'record-observer.edge.jsonl', [fixed()])
    write_rows(tmp_path/'observer.jsonl', [summary()])
    def prohibited(*args, **kwargs):
        raise AssertionError('Unused series must not be projected')
    monkeypatch.setattr(v3_app, unused, prohibited)
    with TestClient(v3_app.create_app(source, start_collector=False)) as client:
        response = client.get('/api/v3/market?scope=all&series='+series)
        assert response.status_code == 200
        result = response.json()['data']
        coverage = result['coverage'] if series == 'live' else result['live_coverage']
        assert coverage['quality'] == 'not_requested' and coverage['loaded_rows'] is None
        assert coverage['reason'] == 'series_not_requested'
        assert result['series_sources']['fixed_q']['requested'] is (series == 'fixed')
        assert result['series_sources']['live_summary']['requested'] is (series == 'live')
        assert client.get('/api/v3/market?scope=all&series=invalid').status_code == 422


def test_concurrent_cold_projection_is_built_once(tmp_path, monkeypatch):
    source = paths(tmp_path)
    write_rows(tmp_path/'record-observer.edge.jsonl', [fixed()])
    calls = []
    original = v3_app.market_points
    def delayed(*args, **kwargs):
        calls.append(1)
        time.sleep(.03)
        return original(*args, **kwargs)
    monkeypatch.setattr(v3_app, 'market_points', delayed)
    with TestClient(v3_app.create_app(source, start_collector=False)) as client:
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: client.get('/api/v3/market?scope=all&series=fixed&units=100'), range(4)))
        assert all(response.status_code == 200 for response in results)
        assert len(calls) == 1


def test_live_collector_progress_does_not_invalidate_fixed_projection(tmp_path, monkeypatch):
    source = paths(tmp_path)
    write_rows(tmp_path/'record-observer.edge.jsonl', [fixed()])
    active = tmp_path/'observer.jsonl'
    write_rows(active, [summary()])
    calls = []
    original = v3_app.market_points
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(v3_app, 'market_points', counted)
    app = v3_app.create_app(source, start_collector=False)
    with TestClient(app) as client:
        client.get('/api/v3/market?scope=all&units=100')
        with active.open('a') as output:
            output.write(json.dumps(summary(101))+'\n')
        app.state.v3_read_model.refresh()
        result = client.get('/api/v3/market?scope=all&units=100').json()['data']
        assert len(calls) == 1 and result['live_coverage']['loaded_rows'] == 2


@pytest.mark.parametrize('live', [False, True])
def test_streamed_sampling_keeps_exact_points_extrema_breaks_and_complete_episodes(live):
    rows = [compact_live_market(summary(100+n*.2, basis_bps=str(n % 97),
        reference_bps=str(n % 41), continuity_break=n == 147)) if live else
        fixed(n+1, 100+n*.2, reference_bps=str(100+n % 71), valid=n != 151,
            entropy={'asks': [[str(100+n % 31), 2000]], 'bids': [['99', 2000]]})
        for n in range(2000)]
    project = live_market_points if live else market_points
    kwargs = {} if live else {'units': 1245}
    expected = project(snapshot(), all_window(), cached_rows=iter(rows), max_points=50, **kwargs)
    streamed = project(snapshot(), all_window(), cached_rows=iter(rows),
        cached_row_count=len(rows), max_points=50, **kwargs)
    assert streamed == expected
    # Appends between the read-only count and row cursor cannot exceed the cap.
    changed = project(snapshot(), all_window(), cached_rows=iter(rows),
        cached_row_count=51, max_points=50, **kwargs)
    assert len(changed['points']) <= 50
    assert changed['coverage'] == {**expected['coverage'], 'display_points': len(changed['points'])}
    if not live:
        assert changed['episodes'] == expected['episodes']


def test_http_streamed_projection_retains_bounded_frames(monkeypatch):
    active = peak = 0
    class TrackedFrame(dict):
        def __init__(self, value):
            nonlocal active, peak
            super().__init__(value)
            active += 1
            peak = max(peak, active)
        def __del__(self):
            nonlocal active
            active -= 1
    original = v3_projection.requote_market
    monkeypatch.setattr(v3_projection, 'requote_market', lambda *a: TrackedFrame(original(*a)))
    output = market_points(snapshot(), all_window(), units=1245, max_points=50,
        cached_rows=(fixed(n+1, 100+n*.2) for n in range(20_000)), cached_row_count=20_000)
    assert output['coverage']['loaded_rows'] == 20_000
    assert len(output['points']) <= 50 and peak < 200
