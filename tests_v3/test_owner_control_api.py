"""Offline fake owner/orders only; no credentials, engine or trading connection.

Browser fixture: uvicorn tests_v3.test_owner_control_api:fake_app --factory ...
The fixture is explicitly fake and never imports an engine or production client.
"""
import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from entropy_arb.dashboard_api import v3_control_app as module
from entropy_arb.dashboard_api.v3_control_app import Refusal, SocketOwner, create_app

ORIGIN = 'http://127.0.0.1:5186'
PREFIX = '/api/v3-control'
BINDING = {'account_binding_hash': 'a' * 64, 'pair_identity': 'SNDK:entropy:lighter',
           'deployment': 'fake-basis', 'batch_id': 'fake-batch', 'config_hash': 'b' * 64}


class FakeOwner:
    """Small protocol double with durable operations and cumulative fake fills."""
    enabled = True

    def __init__(self):
        self.binding = dict(BINDING)  # HTTP's trusted configuration
        self.actual_binding = dict(BINDING)  # independent owner scope
        self.instance = str(uuid4())
        self.current = {'intent': 'RUNNING', 'intent_revision': 0, 'phase': 'MANAGING',
                        'allowed_actions': ['start', 'stop', 'close'], 'risk_locks': [],
                        'blocked_reason': None, 'active_cycle_id': 'fake-cycle',
                        'entropy_units': 257, 'hedge_units': -257, 'quantity_step': '.0001',
                        'position_observed_ts': time.time(), 'position_authoritative': True,
                        'order_watermark': 'fake-watermark', 'pending_orders': []}
        self.receipts, self.operations = {}, {}
        self.frames, self.commits, self.order_submissions = [], 0, 0
        self.drop_confirm = False
        self.drop_before_confirm = False
        self.unavailable = False

    def state(self):
        return {**deepcopy(self.current), **self.actual_binding, 'protocol_version': 1,
                'owner_instance_id': self.instance, 'source_ts': time.time()}

    def restart(self):
        self.instance = str(uuid4())
        self.receipts.clear()

    def fingerprint(self, payload):
        raw = {**self.actual_binding, **{key: payload.get(key) for key in (
            'action', 'intent_revision', 'expected_cycle_id', 'receipt_id')}}
        return hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()

    def admit(self, payload):
        if payload['intent_revision'] != self.current['intent_revision']:
            raise Refusal('intent_revision_changed')
        if payload['action'] == 'start' and self.current['risk_locks']:
            raise Refusal('risk_locked')
        if payload['action'] not in self.current['allowed_actions']:
            raise Refusal('action_unavailable')
        if payload['action'] == 'close':
            if payload['expected_cycle_id'] != self.current['active_cycle_id']:
                raise Refusal('cycle_changed')
            if (not self.current['position_authoritative']
                    or not 0 <= time.time() - self.current['position_observed_ts'] <= 15):
                raise Refusal('position_evidence_stale')
            if any(type(self.current[k]) is not int for k in ('entropy_units', 'hedge_units')):
                raise Refusal('position_unavailable')

    async def request(self, payload):
        self.frames.append(deepcopy(payload))
        if self.unavailable:
            raise Refusal('owner_unavailable', 503)
        try:
            if any(payload.get(k) != v for k, v in self.actual_binding.items()):
                raise Refusal('binding_mismatch')
            op = payload['op']
            if op != 'state' and payload.get('owner_instance_id') != self.instance:
                raise Refusal('owner_instance_mismatch')
            if op == 'state':
                result = self.state()
            elif op == 'preview':
                self.admit(payload)
                receipt_id = str(uuid4())
                self.receipts[receipt_id] = {'payload': deepcopy(payload), 'state': deepcopy(self.current),
                                             'expires': time.time() + 15}
                result = {'receipt_id': receipt_id, 'expires_ts': time.time() + 15,
                          **{k: payload.get(k) for k in ('action', 'intent_revision', 'expected_cycle_id')},
                          'state': self.state()}
            elif op == 'operation':
                record = (next((v for v in self.operations.values() if v['idempotency_key'] == payload['idempotency_key']), None)
                          if 'idempotency_key' in payload else self.operations.get(payload['operation_id']))
                if record is None:
                    raise Refusal('operation_not_found')
                result = {'operation': deepcopy(record), 'state': self.state()}
            elif op == 'confirm':
                if self.drop_before_confirm:
                    self.drop_before_confirm = False
                    raise Refusal('owner_unavailable', 503)
                record = next((v for v in self.operations.values() if v['idempotency_key'] == payload['idempotency_key']), None)
                if record:
                    if record['request_fingerprint'] != self.fingerprint(payload):
                        raise Refusal('idempotency_conflict')
                else:
                    receipt = self.receipts.get(payload['receipt_id'])
                    if not receipt:
                        raise Refusal('receipt_unavailable')
                    if receipt['expires'] <= time.time():
                        raise Refusal('receipt_expired')
                    if any(payload.get(k) != receipt['payload'].get(k) for k in (
                            'action', 'intent_revision', 'expected_cycle_id')):
                        raise Refusal('receipt_mismatch')
                    self.admit(payload)
                    if payload['action'] == 'close' and any(
                            self.current.get(k) != receipt['state'].get(k) for k in (
                                'active_cycle_id', 'entropy_units', 'hedge_units', 'order_watermark')):
                        raise Refusal('close_evidence_changed')
                    self.commits += 1
                    if payload['action'] in ('start', 'stop'):
                        self.current['intent'] = 'RUNNING' if payload['action'] == 'start' else 'STOPPING'
                        self.current['intent_revision'] += 1
                    if payload['action'] == 'close':
                        self.order_submissions += 2  # fake reduce-only legs
                    record = {'operation_id': str(uuid4()), 'idempotency_key': payload['idempotency_key'],
                              'action': payload['action'], 'expected_cycle_id': payload.get('expected_cycle_id'),
                              'state': 'executing', 'intent': self.current['intent'],
                              'intent_revision': self.current['intent_revision'], 'accepted_ts': time.time(),
                              'updated_ts': time.time(), 'request_fingerprint': self.fingerprint(payload),
                              'private_receipt': receipt, 'evidence': {
                                  'intent': self.current['intent'], 'intent_revision': self.current['intent_revision'],
                                  'accounting_final': False, 'fees_complete': False, 'net_pnl_usd': None,
                                  'legs': [{'venue': venue, 'initial_units': self.current[f'{venue}_units'],
                                            'remaining_units': self.current[f'{venue}_units'], 'orders': [
                                                {'order_ref': f'fake-{venue}', 'state': 'UNKNOWN', 'filled_units': 0,
                                                 'reduce_only': True, 'private_nonce': 'never-public'}]}
                                           for venue in ('entropy', 'hedge')], 'pending_orders': []}}
                    self.operations[record['operation_id']] = record
                result = {'operation': deepcopy(record), 'state': self.state()}
                if self.drop_confirm:
                    self.drop_confirm = False
                    raise Refusal('owner_unavailable', 503)
            else:
                raise AssertionError('unexpected op')
            return {'ok': True, 'request_id': payload['request_id'], **result}
        except Refusal as exc:
            if exc.status == 503:
                raise
            return {'ok': False, 'request_id': payload['request_id'], 'protocol_version': 1,
                    'owner_instance_id': self.instance, 'reason': exc.code}


class BrowserFakeOwner(FakeOwner):
    """Explicit browser simulation of continuing fake account observations."""
    def state(self):
        if self.current['position_authoritative']:
            self.current['position_observed_ts'] = time.time()
        return super().state()


def fake_app():
    """Explicit offline browser fixture; no real socket or engine can be selected."""
    owner = BrowserFakeOwner()
    app = create_app(owner, origin=os.getenv('V3_OWNER_UI_ORIGIN', ORIGIN),
                     control_port=os.getenv('V3_OWNER_CONTROL_PORT', '18786'))
    app.state.fake_owner = owner
    @app.middleware('http')
    async def fixture_label(request, call_next):
        response = await call_next(request)
        response.headers['X-Entropy-Fixture'] = 'fake-owner'
        return response
    return app


def client(owner=None, **kwargs):
    return TestClient(create_app(owner or FakeOwner(), origin=ORIGIN),
                      base_url='http://127.0.0.1:8786', client=('127.0.0.1', 1234), **kwargs)


def session(c):
    response = c.get(f'{PREFIX}/session')
    assert response.status_code == 200
    assert 'httponly' in response.headers['set-cookie'].lower()
    assert 'samesite=strict' in response.headers['set-cookie'].lower()
    assert response.headers['cache-control'] == 'no-store'
    return {'Origin': ORIGIN, 'X-Entropy-CSRF': response.json()['csrf_token']}


def preview(c, headers, action='close'):
    body = {'source': 'basis', 'action': action}
    if action == 'close':
        body['expected_cycle_id'] = 'fake-cycle'
    response = c.post(f'{PREFIX}/preview', headers=headers, json=body)
    assert response.status_code == 200, response.text
    return response.json()


def confirmation(preview, key=None):
    return {'preview_id': preview['preview_id'], 'idempotency_key': key or str(uuid4())}


def confirm(c, headers, p, key=None):
    body = confirmation(p, key)
    return c.post(f'{PREFIX}/confirm', headers=headers, json=body), body


@pytest.mark.parametrize('headers', [{}, {'Origin': 'null'}, {'Origin': 'https://attacker.invalid'},
    {'Origin': 'http://localhost:5186'}, {'Origin': ORIGIN, 'X-Entropy-CSRF': 'invalid'},
    {'Origin': ORIGIN, 'Sec-Fetch-Site': 'cross-site'}, {'Origin': ORIGIN, 'Sec-Fetch-Site': 'same-site'}])
def test_origin_session_csrf_before_owner_request(headers):
    owner = FakeOwner()
    with client(owner) as c:
        session(c)
        owner.frames.clear()
        assert c.post(f'{PREFIX}/preview', headers=headers, json={'source': 'basis', 'action': 'stop'}).status_code == 403
    assert owner.frames == [] and owner.commits == 0


def test_loopback_host_and_cookie_session_required():
    with client() as c:
        assert c.get(f'{PREFIX}/session', headers={'Host': 'attacker.invalid'}).status_code == 403
        assert c.get(f'{PREFIX}/state').status_code == 403
    with TestClient(create_app(FakeOwner(), origin=ORIGIN), base_url='http://127.0.0.1:8786',
                    client=('192.0.2.1', 1234)) as remote:
        assert remote.get(f'{PREFIX}/session').status_code == 403
    # Installed TestClient's URL parser mishandles IPv6 netloc; exercise the
    # actual ASGI peer and bracketed Host separately without opening TCP.
    with TestClient(create_app(FakeOwner(), origin=ORIGIN), base_url='http://127.0.0.1:8786',
                    client=('::1', 1234)) as ipv6:
        assert ipv6.get(f'{PREFIX}/session', headers={'Host': '[::1]:8786'}).status_code == 200


def test_expired_session_renewal_rotates_cookie_and_csrf_without_command(monkeypatch):
    now = [time.time()]
    monkeypatch.setattr(module.time, 'time', lambda: now[0])
    owner = FakeOwner()
    with client(owner) as c:
        old_headers = session(c)
        old_cookie = c.cookies.get('entropy_owner_session')
        old_preview = preview(c, old_headers, 'stop')
        owner.frames.clear()
        now[0] += 3601
        expired = c.get(f'{PREFIX}/state?source=basis', headers={'Origin': ORIGIN})
        assert expired.status_code == 403
        assert expired.json()['detail']['code'] == 'session_expired'
        assert owner.frames == []

        headers = session(c)
        assert c.cookies.get('entropy_owner_session') != old_cookie
        assert headers['X-Entropy-CSRF'] != old_headers['X-Entropy-CSRF']
        state = c.get(f'{PREFIX}/state?source=basis', headers={'Origin': ORIGIN})
        assert state.status_code == 200 and state.json()['enabled'] is True
        assert state.json()['state']['intent'] == 'RUNNING'
        assert [frame['op'] for frame in owner.frames] == ['state', 'state']

        owner.frames.clear()
        stale_preview = c.post(f'{PREFIX}/preview', headers=old_headers,
                               json={'source': 'basis', 'action': 'stop'})
        stale_confirm = c.post(f'{PREFIX}/confirm', headers=old_headers,
                               json=confirmation(old_preview))
        assert stale_preview.status_code == stale_confirm.status_code == 403
        assert owner.frames == []
        renewed_preview = preview(c, headers, 'stop')
        assert renewed_preview['action'] == 'stop'
        assert renewed_preview['state']['intent_revision'] == 0
        assert [frame['op'] for frame in owner.frames] == ['state', 'preview']
    assert owner.current['intent'] == 'RUNNING'
    assert owner.commits == owner.order_submissions == 0


@pytest.mark.parametrize('endpoint', ['session', 'state'])
def test_localhost_origin_cannot_renew_or_read_numeric_loopback_session(endpoint):
    owner = FakeOwner()
    with client(owner) as c:
        session(c)
        owner.frames.clear()
        refused = c.get(f'{PREFIX}/{endpoint}?source=basis',
                        headers={'Origin': 'http://localhost:5186'})
        assert refused.status_code == 403
        assert owner.frames == []
        allowed = c.get(f'{PREFIX}/{endpoint}?source=basis', headers={'Origin': ORIGIN})
        assert allowed.status_code == 200 and allowed.json()['enabled'] is True
        assert [frame['op'] for frame in owner.frames] == ['state']
    assert owner.commits == owner.order_submissions == 0


@pytest.mark.parametrize('extra', [{'socket': '/tmp/evil'}, {'account_binding_hash': 'bad'},
    {'command': 'kill 28072'}, {'intent_revision': 100}, {'receipt_id': str(uuid4())}])
def test_browser_cannot_choose_paths_commands_binding_or_revision(extra):
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        owner.frames.clear()
        assert c.post(f'{PREFIX}/preview', headers=headers, json={
            'source': 'basis', 'action': 'stop', **extra}).status_code == 422
    assert owner.frames == []


def test_methods_json_size_and_duplicate_fields():
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        owner.frames.clear()
        assert c.get(f'{PREFIX}/preview').status_code == 405
        assert c.put(f'{PREFIX}/confirm').status_code == 405
        assert c.post(f'{PREFIX}/preview', headers=headers, content='x').status_code == 415
        assert c.post(f'{PREFIX}/preview', headers=headers, json={'large': 'x' * 3000}).status_code == 413
        for raw in ('{"source":"basis","action":"stop","action":"start"}', '{"x":NaN}', '[]'):
            assert c.post(f'{PREFIX}/preview', headers={**headers, 'Content-Type': 'application/json'}, content=raw).status_code == 422
    assert owner.frames == []


def test_absent_owner_and_funding_never_claim_capability(monkeypatch):
    monkeypatch.delenv('V3_OWNER_CONTROL_ENABLED', raising=False)
    with TestClient(create_app(origin=ORIGIN), base_url='http://127.0.0.1:8786', client=('127.0.0.1', 1)) as c:
        headers = session(c)
        assert c.get(f'{PREFIX}/state').json()['reason_code'] == 'control_not_configured'
        response = c.post(f'{PREFIX}/preview', headers=headers, json={'source': 'basis', 'action': 'start'})
        assert response.status_code == 503
    owner = FakeOwner()
    owner.unavailable = True
    with client(owner) as c:
        session(c)
        assert not c.get(f'{PREFIX}/state').json()['enabled']
        assert c.get(f'{PREFIX}/state?source=funding').json()['reason_code'] == 'unsupported_source'
    assert owner.commits == owner.order_submissions == 0


def test_preview_private_receipt_valuation_unknown_and_no_intent_change():
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        p = preview(c, headers)
        assert p['state']['entropy_units'] == 257 and p['state']['hedge_units'] == -257
        assert p['valuation']['executable_gross_usd'] is None
        assert 'receipt_id' not in json.dumps(p) and 'account_binding_hash' not in json.dumps(p)
        assert owner.current['intent'] == 'RUNNING' and owner.commits == 0
        assert all(frame[k] == value for frame in owner.frames for k, value in BINDING.items())


@pytest.mark.parametrize('field,value,code', [('intent_revision', 1, 'intent_revision_changed'),
    ('active_cycle_id', 'new-cycle', 'cycle_changed'), ('entropy_units', 258, 'close_evidence_changed'),
    ('hedge_units', -258, 'close_evidence_changed'), ('order_watermark', 'changed', 'close_evidence_changed'),
    ('position_observed_ts', time.time() - 60, 'position_evidence_stale'),
    ('position_authoritative', False, 'position_evidence_stale'), ('hedge_units', None, 'position_unavailable')])
def test_confirm_rechecks_revision_cycle_quantity_authority_and_orders(field, value, code):
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        p = preview(c, headers)
        owner.current[field] = value
        response, _ = confirm(c, headers, p)
        assert response.status_code == 409 and response.json()['detail']['code'] == code
    assert owner.commits == owner.order_submissions == 0


@pytest.mark.parametrize('field', list(BINDING))
def test_every_scope_change_refuses_without_action(field):
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        p = preview(c, headers)
        owner.actual_binding[field] += '-changed'
        response, _ = confirm(c, headers, p)
        assert response.status_code == 409 and response.json()['detail']['code'] == 'binding_mismatch'
        assert not c.get(f'{PREFIX}/state').json()['enabled']
    assert owner.commits == 0


def test_owner_restart_before_first_confirm_requires_new_preview():
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        p = preview(c, headers)
        owner.restart()
        response, _ = confirm(c, headers, p)
        assert response.json()['detail']['code'] == 'owner_instance_mismatch'
    assert owner.commits == 0


def test_expired_or_cross_session_preview_refused():
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        p = preview(c, headers)
        c.app.state.previews[p['preview_id']]['expires_at'] = time.time() - 1
        assert confirm(c, headers, p)[0].json()['detail']['code'] == 'receipt_expired'
        p = preview(c, headers)
        c.cookies.clear()
        other = session(c)
        assert confirm(c, other, p)[0].json()['detail']['code'] == 'preview_unavailable'
    assert owner.commits == 0


def test_duplicate_clicks_key_conflict_and_private_result_projection():
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        p = preview(c, headers)
        first, body = confirm(c, headers, p)
        oid = first.json()['operation_id']
        assert first.status_code == 200
        assert c.post(f'{PREFIX}/confirm', headers=headers, json=body).json()['operation_id'] == oid
        assert confirm(c, headers, p)[0].json()['operation_id'] == oid
        other = preview(c, headers)
        conflict, _ = confirm(c, headers, other, body['idempotency_key'])
        assert conflict.json()['detail']['code'] == 'idempotency_conflict'
        data = c.get(f'{PREFIX}/operations/{oid}').json()
        assert data['operation']['evidence']['legs'][0]['orders'][0]['state'] == 'UNKNOWN'
        raw = json.dumps(data)
        assert all(name not in raw for name in ('private_receipt', 'request_fingerprint', 'private_nonce', 'account_binding_hash'))
    assert owner.commits == 1 and owner.order_submissions == 2


def test_stop_then_close_and_old_confirmation_never_restore_running():
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        old = preview(c, headers)
        stopped, _ = confirm(c, headers, preview(c, headers, 'stop'))
        assert stopped.json()['state']['intent'] == 'STOPPING'
        assert confirm(c, headers, old)[0].json()['detail']['code'] == 'intent_revision_changed'
        closed, close_body = confirm(c, headers, preview(c, headers))
        assert closed.json()['state']['intent'] == 'STOPPING'
        owner.current['intent'] = 'STOPPED'
        owner.current['intent_revision'] += 1
        replay = c.post(f'{PREFIX}/confirm', headers=headers, json=close_body).json()
        assert replay['operation']['intent'] == 'STOPPING'  # accepted historical intent
        assert replay['state']['intent'] == 'STOPPED'  # current canonical intent
    assert owner.commits == 2 and owner.order_submissions == 2


def test_close_then_stop_query_keeps_new_intent_and_start_preserves_risk_lock():
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        result, body = confirm(c, headers, preview(c, headers))
        confirm(c, headers, preview(c, headers, 'stop'))
        replay = c.post(f'{PREFIX}/confirm', headers=headers, json=body).json()
        assert replay['operation']['intent'] == 'RUNNING' and replay['state']['intent'] == 'STOPPING'
        owner.current['risk_locks'] = ['MANUAL_STOP', 'loss_budget']
        response = c.post(f'{PREFIX}/preview', headers=headers, json={'source': 'basis', 'action': 'start'})
        assert response.json()['detail']['code'] == 'risk_locked'
        assert owner.current['risk_locks'] == ['MANUAL_STOP', 'loss_budget']
    assert owner.commits == 2


@pytest.mark.parametrize('restart', [False, True])
def test_lost_ack_recovery_key_lookup_and_replay_are_durable(restart):
    owner = FakeOwner()
    owner.drop_confirm = True
    with client(owner) as c:
        headers = session(c)
        p = preview(c, headers)
        response, body = confirm(c, headers, p)
        assert response.status_code == 503 and response.json()['detail']['code'] == 'outcome_unknown'
        if restart:
            owner.restart()
        by_key = c.get(f'{PREFIX}/operations/by-key/{body["idempotency_key"]}?source=basis')
        assert by_key.status_code == 200
        assert c.post(f'{PREFIX}/confirm', headers=headers, json=body).json()['operation_id'] == by_key.json()['operation_id']
    assert owner.commits == 1 and owner.order_submissions == 2
    # HTTP restart loses private receipts, but a saved key recovers from owner.
    with client(owner) as c:
        session(c)
        assert c.get(f'{PREFIX}/operations/by-key/{body["idempotency_key"]}').json()['operation_id'] == by_key.json()['operation_id']
    assert owner.commits == 1


def test_transport_loss_before_acceptance_retries_exact_payload_once():
    owner = FakeOwner()
    owner.drop_before_confirm = True
    with client(owner) as c:
        headers = session(c)
        p = preview(c, headers)
        response, body = confirm(c, headers, p)
        assert response.status_code == 503 and owner.commits == 0
        assert c.post(f'{PREFIX}/confirm', headers=headers, json=body).status_code == 200
        frames = [v for v in owner.frames if v['op'] == 'confirm']
        assert len(frames) == 2
        assert {k: v for k, v in frames[0].items() if k != 'request_id'} == {
            k: v for k, v in frames[1].items() if k != 'request_id'}
    assert owner.commits == 1


@pytest.mark.parametrize('reason', ['owner_operation_failed', 'invalid_or_busy_frame', 'unexpected_internal_error'])
def test_refusal_after_persisted_action_does_not_claim_no_action(reason):
    owner = FakeOwner()
    original = owner.request
    async def interrupted(payload):
        result = await original(payload)
        if payload['op'] == 'confirm':
            return {'ok': False, 'reason': reason, 'request_id': payload['request_id']}
        return result
    owner.request = interrupted
    with client(owner) as c:
        headers = session(c)
        response, body = confirm(c, headers, preview(c, headers))
        assert response.status_code == 503 and response.json()['detail']['code'] == 'outcome_unknown'
        recovered = c.get(f'{PREFIX}/operations/by-key/{body["idempotency_key"]}')
        assert recovered.status_code == 200
        assert c.post(f'{PREFIX}/confirm', headers=headers, json=body).json()['operation_id'] == recovered.json()['operation_id']
    assert owner.commits == 1 and owner.order_submissions == 2


def test_partial_unknown_cumulative_fills_and_late_fee_reopen_accounting():
    owner = FakeOwner()
    with client(owner) as c:
        headers = session(c)
        response, _ = confirm(c, headers, preview(c, headers))
        oid = response.json()['operation_id']
        op = owner.operations[oid]
        leg = op['evidence']['legs'][1]
        leg['remaining_units'] = -137
        leg['orders'][0]['filled_units'] = 120
        for _ in range(2):
            data = c.get(f'{PREFIX}/operations/{oid}').json()
            assert data['operation']['evidence']['legs'][1]['orders'][0]['filled_units'] == 120
            assert data['operation']['evidence']['net_pnl_usd'] is None
        op['state'] = 'completed'
        op['evidence'].update(accounting_final=True, fees_complete=True, net_pnl_usd='-.004772')
        assert c.get(f'{PREFIX}/operations/{oid}').json()['operation']['evidence']['net_pnl_usd'] == '-.004772'
        op['state'] = 'flat_pending_accounting'
        op['evidence'].update(accounting_final=False, fees_complete=False, net_pnl_usd='-.004772')
        data = c.get(f'{PREFIX}/operations/{oid}').json()
        assert data['operation']['state'] == 'flat_pending_accounting'
        assert data['operation']['evidence']['net_pnl_usd'] is None
    assert owner.commits == 1 and owner.order_submissions == 2


def test_response_time_is_not_authoritative_position_time():
    owner = FakeOwner()
    owner.current.update(position_observed_ts=time.time() - 60, position_authoritative=False,
                         entropy_units=None, hedge_units=None)
    with client(owner) as c:
        headers = session(c)
        data = c.get(f'{PREFIX}/state').json()['state']
        assert data['source_ts'] > data['position_observed_ts'] + 50
        assert data['entropy_units'] is None and data['position_authoritative'] is False
        response = c.post(f'{PREFIX}/preview', headers=headers, json={
            'source': 'basis', 'action': 'close', 'expected_cycle_id': 'fake-cycle'})
        assert response.json()['detail']['code'] == 'position_evidence_stale'
    assert owner.commits == 0


def test_browser_fixture_refreshes_only_fake_observations_and_labels_responses():
    app = fake_app()
    owner = app.state.fake_owner
    owner.current['position_observed_ts'] = time.time() - 60
    with TestClient(app, base_url='http://127.0.0.1:18786', client=('127.0.0.1', 1)) as c:
        headers = session(c)
        assert c.get(f'{PREFIX}/state').headers['X-Entropy-Fixture'] == 'fake-owner'
        owner.current['position_observed_ts'] = time.time() - 60
        p = preview(c, headers)
        assert p['state']['position_observed_ts'] > time.time() - 1
        response, _ = confirm(c, headers, p)
        assert response.status_code == 200
        assert response.headers['X-Entropy-Fixture'] == 'fake-owner'
    regular = FakeOwner()
    old = time.time() - 60
    regular.current['position_observed_ts'] = old
    assert regular.state()['position_observed_ts'] == old


@pytest.mark.parametrize('change', [lambda state: state.update(account_binding_hash='foreign'),
    lambda state: state.update(source_ts=time.time() - 100),
    lambda state: state.update(owner_instance_id='not-uuid'),
    lambda state: state.update(allowed_actions=['shell']),
    lambda state: state.update(protocol_version=2)])
def test_bad_handshake_never_enables_control(change):
    owner = FakeOwner()
    original = owner.state
    def changed():
        state = original()
        change(state)
        return state
    owner.state = changed
    with client(owner) as c:
        assert c.get(f'{PREFIX}/session').json()['enabled'] is False
    assert owner.commits == 0


def test_socket_transport_private_permissions_bounded_frame_and_no_shell(monkeypatch):
    async def scenario():
        with tempfile.TemporaryDirectory(prefix='v3-owner-test-',
                dir=os.getenv('ENTROPY_PROTOCOL_TEST_SOCKET_ROOT', '/tmp')) as directory:
            parent = Path(directory)
            parent.chmod(0o700)
            path = parent / 'owner.sock'
            monkeypatch.setenv('V3_OWNER_CONTROL_ENABLED', '1')
            monkeypatch.setenv('V3_OWNER_SOCKET', str(path))
            for key, value in BINDING.items():
                monkeypatch.setenv(f'V3_OWNER_{key.upper()}', value)
            transport = SocketOwner()
            with pytest.raises(Refusal, match='owner_unavailable'):
                await transport.request({'op': 'state'})
            received = []
            async def echo(reader, writer):
                received.append(module.decode_frame(await reader.readline()))
                writer.write(module.encode_frame({'ok': True, 'request_id': 'fake'}))
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            server = await asyncio.start_unix_server(echo, path=path)
            path.chmod(0o600)
            try:
                assert await transport.request({'op': 'state'}) == {'ok': True, 'request_id': 'fake'}
                assert received == [{'op': 'state'}]
                path.chmod(0o666)
                with pytest.raises(Refusal, match='unsafe_socket_path'):
                    await transport.request({'op': 'state'})
                path.chmod(0o600)
                parent.chmod(0o755)
                with pytest.raises(Refusal, match='unsafe_socket_path'):
                    await transport.request({'op': 'state'})
                parent.chmod(0o700)
            finally:
                server.close()
                await server.wait_closed()
    asyncio.run(scenario())
    assert not hasattr(module, 'subprocess')


@pytest.mark.parametrize('raw', [b'{}', b'{"ok":true,"ok":false}\n', b'{"x":NaN}\n',
    b'[]\n', b'"x"\n', b'x' * 17000 + b'\n'])
def test_socket_frame_rejects_unbounded_nonfinite_or_duplicate_response(raw):
    with pytest.raises(Refusal):
        module.decode_frame(raw)


@pytest.mark.parametrize('origin', ['http://0.0.0.0:5186', 'https://example.invalid',
    '*', 'http://127.0.0.1:5186/path', 'http://user@localhost:5186'])
def test_origin_configuration_cannot_expand_to_public_access(origin):
    with pytest.raises(ValueError):
        create_app(FakeOwner(), origin=origin)
