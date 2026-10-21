"""No sockets, credentials, signers or real orders: local control boundary checks."""
import asyncio
from copy import deepcopy
import time
from uuid import uuid4

from fastapi import HTTPException
from fastapi.testclient import TestClient
import pytest

from entropy_arb.dashboard_api.v3_exit_app import create_app

ORIGIN = 'http://127.0.0.1:5186'
TARGET = {'source': 'basis', 'cycle_id': 'mock-cycle'}


class FakeWorker:
    enabled = True

    def __init__(self, outcome='complete'):
        self.previews = self.executions = 0
        self.outcome = outcome
        self.receipt = None
        self.receipts = []

    async def run(self, cycle_id, *, receipt=None):
        if not self.enabled:
            raise HTTPException(409, 'disabled')
        if receipt is None:
            self.previews += 1
            self.receipt = {'cycle_id': cycle_id, 'batch_id': 'mock-batch', 'account_scope_hash': 'a' * 64,
                            'entropy_units': 257, 'hedge_units': -257, 'observed_at': time.time(), 'expires_at': time.time() + 15}
            self.receipts.append(deepcopy(self.receipt))
            return {'receipt': self.receipt, 'preview': {**self.receipt, 'symbol': 'SNDK',
                    'quantity_step': '.0001', 'authoritative': True, 'position_observed_ts': time.time()}}
        self.executions += 1
        assert receipt in self.receipts
        await asyncio.sleep(.03)
        return {'success': self.outcome in ('complete', 'accounting_pending'),
                'remaining': {'authoritative': True, 'entropy_units': 0, 'hedge_units': 0},
                'active_cycle': None, 'pending': [{'state': 'UNKNOWN'}] if self.outcome == 'unknown' else [],
                'accounting_final': self.outcome == 'complete',
                'net_pnl_usd': '-.004772' if self.outcome == 'complete' else None,
                'entries_remain_closed': True}

    def progress(self, receipt, started_at):
        return {'state': self.outcome if self.outcome in ('unknown', 'partial') else 'executing',
                'legs': [{'venue': venue, 'state': 'closed' if venue == 'entropy' else self.outcome,
                          'initial_units': 257, 'closed_units': 257 if venue == 'entropy' else 120,
                          'remaining_units': 0 if venue == 'entropy' else -137}
                         for venue in ('entropy', 'hedge')]}


def client(worker=None, **kwargs):
    return TestClient(create_app(worker or FakeWorker(), origin=ORIGIN),
                      base_url='http://127.0.0.1:8786', client=('127.0.0.1', 1234), **kwargs)


def session(c):
    result = c.get('/api/v3-control/session')
    assert result.status_code == 200
    assert 'httponly' in result.headers['set-cookie'].lower()
    assert 'samesite=strict' in result.headers['set-cookie'].lower()
    assert result.headers['cache-control'] == 'no-store'
    return {'Origin': ORIGIN, 'X-Entropy-CSRF': result.json()['csrf_token']}


def preview(c, headers):
    response = c.post('/api/v3-control/preview', headers=headers, json=TARGET)
    assert response.status_code == 200
    assert 'receipt' not in response.json() and 'account_scope_hash' not in response.json()
    return response.json()['preview_id']


def confirmation(pid, **changes):
    return {**TARGET, 'preview_id': pid, 'idempotency_key': str(uuid4()),
            'action': 'close_both_legs', **changes}


@pytest.mark.parametrize('headers', [{}, {'Origin': 'https://attacker.invalid'},
    {'Origin': 'null'}, {'Origin': ORIGIN, 'X-Entropy-CSRF': 'fake'},
    {'Origin': ORIGIN, 'Sec-Fetch-Site': 'cross-site'}])
def test_origin_csrf_rejects_before_any_preview(headers):
    worker = FakeWorker()
    with client(worker) as c:
        session(c)
        assert c.post('/api/v3-control/preview', headers=headers, json=TARGET).status_code == 403
    assert worker.previews == worker.executions == 0


def test_loopback_and_host_required():
    with client() as c:
        assert c.get('/api/v3-control/session', headers={'Host': 'attacker.invalid'}).status_code == 403
    with TestClient(create_app(FakeWorker(), origin=ORIGIN), base_url='http://127.0.0.1:8786',
                    client=('192.0.2.1', 1234)) as remote:
        assert remote.get('/api/v3-control/session').status_code == 403


@pytest.mark.parametrize('body', [{**TARGET, 'command': 'kill 1'}, {**TARGET, 'source': 'funding'},
    {**TARGET, 'cycle_id': '--reopen-entries'}, {**TARGET, 'cycle_id': 'cycle;echo secret'}])
def test_no_browser_selected_commands_paths_or_other_owner(body):
    worker = FakeWorker()
    with client(worker) as c:
        headers = session(c)
        assert c.post('/api/v3-control/preview', headers=headers, json=body).status_code == 422
    assert worker.previews == worker.executions == 0


def test_post_json_required_and_preview_never_executes():
    worker = FakeWorker()
    with client(worker) as c:
        headers = session(c)
        assert c.get('/api/v3-control/preview').status_code == 405
        assert c.post('/api/v3-control/preview', headers=headers, content='text').status_code == 415
        assert c.post('/api/v3-control/preview', headers=headers, json={'huge': 'x' * 3000}).status_code == 413
        preview(c, headers)
    assert worker.previews == 1 and worker.executions == 0


def test_expired_changed_cycle_and_other_session_refuse():
    worker = FakeWorker()
    with client(worker) as c:
        headers = session(c)
        pid = preview(c, headers)
        assert c.post('/api/v3-control/confirm', headers=headers,
                      json=confirmation(pid, cycle_id='another')).status_code == 409
        original_cookie = c.cookies.get('entropy_exit_session')
        c.cookies.clear()
        other_headers = session(c)
        assert c.post('/api/v3-control/confirm', headers=other_headers, json=confirmation(pid)).status_code == 409
        c.cookies.clear()
        c.cookies.set('entropy_exit_session', original_cookie)
        worker.receipt['expires_at'] = time.time() - 1
        assert c.post('/api/v3-control/confirm', headers=headers, json=confirmation(pid)).status_code == 409
    assert worker.executions == 0


@pytest.mark.parametrize('outcome', ['complete', 'accounting_pending', 'unknown', 'partial'])
def test_final_confirmation_is_idempotent_and_progress_distinguishes_states(outcome):
    worker = FakeWorker(outcome)
    with client(worker) as c:
        headers = session(c)
        pid = preview(c, headers)
        assert worker.executions == 0
        body = confirmation(pid)
        first = c.post('/api/v3-control/confirm', headers=headers, json=body)
        assert first.status_code == 200
        assert c.post('/api/v3-control/confirm', headers=headers, json=body).json() == first.json()
        assert c.post('/api/v3-control/confirm', headers=headers, json=confirmation(pid)).json() == first.json()
        oid = first.json()['operation_id']
        time.sleep(.05)
        status = c.get(f'/api/v3-control/operations/{oid}').json()
        assert status['state'] == outcome
        assert status['net_pnl_usd'] == ('-.004772' if outcome == 'complete' else None)
        assert worker.executions == 1
        c.cookies.clear()
        session(c)
        assert c.get(f'/api/v3-control/operations/{oid}').status_code == 404


def test_single_manager_and_idempotency_key_cannot_switch_confirmation():
    worker = FakeWorker()
    with client(worker) as c:
        headers = session(c)
        one, two = preview(c, headers), preview(c, headers)
        body = confirmation(one)
        assert c.post('/api/v3-control/confirm', headers=headers, json=body).status_code == 200
        assert c.post('/api/v3-control/confirm', headers=headers,
                      json=confirmation(two, idempotency_key=body['idempotency_key'])).status_code == 409
        duplicate = c.post('/api/v3-control/confirm', headers=headers, json=confirmation(two))
        assert duplicate.status_code == 200
        assert duplicate.json() == c.post('/api/v3-control/confirm', headers=headers, json=body).json()
    assert worker.executions == 1


def test_service_defaults_disabled(monkeypatch):
    monkeypatch.delenv('V3_EXIT_CONTROL_ENABLED', raising=False)
    with TestClient(create_app(origin=ORIGIN), base_url='http://127.0.0.1:8786',
                    client=('127.0.0.1', 1234)) as c:
        headers = session(c)
        assert not c.get('/api/v3-control/session').json()['enabled']
        assert c.post('/api/v3-control/preview', headers=headers, json=TARGET).status_code == 409


def test_legacy_enable_flag_cannot_bypass_missing_engine_owner_protocol(monkeypatch):
    monkeypatch.setenv('V3_EXIT_CONTROL_ENABLED', '1')
    with TestClient(create_app(origin=ORIGIN), base_url='http://127.0.0.1:8786',
                    client=('127.0.0.1', 1234)) as c:
        headers = session(c)
        assert not c.get('/api/v3-control/session').json()['enabled']
        assert c.post('/api/v3-control/preview', headers=headers, json=TARGET).status_code == 409


def test_progress_requires_order_fills_never_infers_from_zero_ledger(monkeypatch):
    from entropy_arb.dashboard_api import v3_exit_app as module
    monkeypatch.delenv('V3_EXIT_CONTROL_ENABLED', raising=False)
    worker = module.ExitWorker()
    journal = {'live_cycles': [{'cycle_id': 'mock-cycle', 'entropy_units': 0, 'hedge_units': 0}], 'orders': []}
    monkeypatch.setattr(module, 'journal_source', lambda _: {'data': journal})
    monkeypatch.setattr(module, 'json_source', lambda _: {'data': {}})
    receipt = {'cycle_id': 'mock-cycle', 'entropy_units': 257, 'hedge_units': -257,
               'close_fills_baseline': {'entropy': {}, 'hedge': {}}}
    data = worker.progress(receipt, time.time())
    assert all(leg['closed_units'] == 0 and leg['state'] != 'closed' for leg in data['legs'])
    assert data['progress_basis'] == 'ledger'
    receipt.pop('close_fills_baseline')
    assert all(leg['closed_units'] is None for leg in worker.progress(receipt, 0)['legs'])


def test_cumulative_close_progress_deduplicates_and_subtracts_preview_baseline(monkeypatch):
    from entropy_arb.dashboard_api import v3_exit_app as module
    worker = module.ExitWorker()
    order = {'order_ref': 'close-h', 'cycle_id': 'mock-cycle', 'venue': 'hedge',
             'reduce_only': 1, 'state': 'UNKNOWN', 'requested_units': 257, 'filled_units': 120}
    journal = {'live_cycles': [{'cycle_id': 'mock-cycle', 'entropy_units': 257, 'hedge_units': -137}],
               'orders': [deepcopy(order), deepcopy(order)]}
    monkeypatch.setattr(module, 'journal_source', lambda _: {'data': journal})
    monkeypatch.setattr(module, 'json_source', lambda _: {'data': {}})
    receipt = {'cycle_id': 'mock-cycle', 'entropy_units': 257, 'hedge_units': -257,
               'close_fills_baseline': {'entropy': {}, 'hedge': {'close-h': 100}}}
    leg = worker.progress(receipt, 0)['legs'][1]
    assert leg['closed_units'] == 20 and leg['state'] == 'unknown'
    journal['orders'][0]['filled_units'] = journal['orders'][1]['filled_units'] = 150
    assert worker.progress(receipt, 0)['legs'][1]['closed_units'] == 50
    journal['orders'][0]['filled_units'] = journal['orders'][1]['filled_units'] = 99
    assert worker.progress(receipt, 0)['legs'][1]['closed_units'] is None


def test_zero_fill_incomplete_exit_stays_unknown():
    class ZeroWorker(FakeWorker):
        def progress(self, receipt, started_at):
            return {'state': 'executing', 'legs': [
                {'venue': v, 'state': 'waiting', 'initial_units': 257,
                 'closed_units': 0, 'remaining_units': None} for v in ('entropy', 'hedge')]}
    worker = ZeroWorker('incomplete')
    with client(worker) as c:
        headers = session(c)
        pid = preview(c, headers)
        oid = c.post('/api/v3-control/confirm', headers=headers, json=confirmation(pid)).json()['operation_id']
        time.sleep(.05)
        data = c.get(f'/api/v3-control/operations/{oid}').json()
        assert data['state'] == 'unknown' and data['net_pnl_usd'] is None
        assert not data['accounting_final']


def test_missing_active_cycle_evidence_cannot_claim_complete():
    class MissingActiveWorker(FakeWorker):
        async def run(self, cycle_id, *, receipt=None):
            result = await super().run(cycle_id, receipt=receipt)
            if receipt is not None:
                result.pop('active_cycle')
            return result
    worker = MissingActiveWorker()
    with client(worker) as c:
        headers = session(c)
        pid = preview(c, headers)
        oid = c.post('/api/v3-control/confirm', headers=headers, json=confirmation(pid)).json()['operation_id']
        time.sleep(.05)
        data = c.get(f'/api/v3-control/operations/{oid}').json()
        assert data['state'] == 'unknown' and data['net_pnl_usd'] is None
        assert not data['accounting_final']
