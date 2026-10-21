"""In-memory HTTP adapter + real owner protocol, without sockets or trading.

Parameter validation is the candidate's real validator. The callback double
models durable receipts; runtime/SQLite atomicity has its own focused tests.
"""
from copy import deepcopy
import hashlib
import json
import signal
import time
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from entropy_arb.dashboard_api import v3_control_app as module
from entropy_arb.dashboard_api.v3_control_app import Refusal, create_app
from entropy_arb.v3.operator_control import ControlRefusal, OperatorControl
from entropy_arb.v3.parameter_config import validate_parameters


ORIGIN = 'http://127.0.0.1:5186'
PREFIX = '/api/v3-control'
BINDING = dict(account_binding_hash='a' * 64, pair_identity='SNDK:entropy:lighter',
    deployment='fake-basis', batch_id='fake-batch', config_hash='b' * 64)
VALUES = dict(entry_band_bps='5', exit_min_gross_profit_bps='2.5',
    entropy_exit_room_bps='12', lighter_exit_room_bps='12', max_notional_usd='200')
LIMITS = dict(entry_band_bps='100', exit_min_gross_profit_bps='100',
    entropy_exit_room_bps='12', lighter_exit_room_bps='12', max_notional_usd='200')


def uid():
    return str(uuid4())


class ParameterOwner:
    """HTTP bridge to OperatorControl.handle; no transport implementation."""
    enabled = True

    def __init__(self, *, parameters=True, opening=False):
        self.binding = dict(BINDING)  # separate HTTP trusted scope
        self.opening = opening
        self.limits = {**LIMITS, **(dict(entropy_open_room_bps='10',
            lighter_open_room_bps='12') if opening else {})}
        self.base_policies = dict(entropy=dict(rooms_bps=['3', '3', '3'], hard_cap_bps='10'),
            lighter=dict(rooms_bps=['4', '8', '12'], hard_cap_bps='12'))
        self.values = dict(intent='STOPPED', intent_revision=0, phase='STOPPED',
            risk_locks=[], blocked_reason=None, active_cycle_id=None, entropy_units=0,
            hedge_units=0, position_authoritative=True, position_observed_ts=time.time(),
            order_watermark=[], pending_orders=[], allowed_actions=['start'])
        self.base_hash = BINDING['config_hash']
        self.current = dict(revision=0, config_hash=self.base_hash,
            values={**VALUES, **(dict(entropy_open_room_bps=None,
                lighter_open_room_bps=None) if opening else {})})
        self.saved = None
        self.operations, self.saves, self.frames = {}, {}, []
        self.commits = self.save_commits = self.wakes = 0
        self.drop_before = self.drop_after = self.corrupt_after = None
        self.fail_commit = False
        self.has_parameters = parameters
        self.control = OperatorControl(dict(BINDING), snapshot=self.snapshot,
            commit=self.commit, operation=self.operation, wake=self.wake,
            save_parameters=self.save if parameters else None,
            parameter_save=self.save_lookup if parameters else None)

    def reason(self):
        state = self.values
        if state['intent'] != 'STOPPED':
            return 'parameters_require_stopped'
        if state['active_cycle_id'] is not None:
            return 'parameters_active_cycle'
        if state['pending_orders']:
            return 'parameters_pending_orders'
        stamp = state['position_observed_ts']
        if (state['position_authoritative'] is not True or type(stamp) not in (int, float)
                or not 0 <= time.time() - stamp <= 15):
            return 'parameters_position_refresh_required'
        if state['entropy_units'] != 0 or state['hedge_units'] != 0:
            return 'parameters_require_flat'
        return None

    def parameters(self):
        result = dict(base_config_hash=self.base_hash, current=deepcopy(self.current),
            saved=deepcopy(self.saved), can_save=self.reason() is None,
            reason_code=self.reason(), limits=dict(self.limits))
        if self.opening:
            def policies(record):
                if record is None:
                    return None
                return {venue: deepcopy(self.base_policies[venue])
                    if record['values'][venue + '_open_room_bps'] is None else
                    dict(rooms_bps=[record['values'][venue + '_open_room_bps']] * 3,
                         hard_cap_bps=record['values'][venue + '_open_room_bps'])
                    for venue in ('entropy', 'lighter')}
            result.update(capabilities=dict(opening_concessions=True),
                opening_policies=dict(base=deepcopy(self.base_policies),
                    current=policies(self.current), saved=policies(self.saved)))
        return result

    def snapshot(self):
        state = deepcopy(self.values)
        if self.has_parameters:
            state['parameters'] = self.parameters()
        return state

    def wake(self):
        self.wakes += 1

    def operation(self, operation_id=None, *, idempotency_key=None):
        return next((deepcopy(row) for row in self.operations.values()
            if row['operation_id'] == operation_id or row['idempotency_key'] == idempotency_key), None)

    def save_lookup(self, key):
        if key not in self.saves:
            return None
        return dict(parameters=self.parameters(), **deepcopy(self.saves[key]))

    def save(self, *, expected_revision, values, idempotency_key):
        try:
            normalized = validate_parameters(values, self.limits)
        except ValueError as exc:
            raise ControlRefusal(str(exc)) from None
        fingerprint = dict(expected_revision=expected_revision,
            opening_fields_present=len(values) == 7,
            values={name: value for name, value in normalized.items()
                if name not in ('entropy_open_room_bps', 'lighter_open_room_bps') or value is not None})
        prior = self.saves.get(idempotency_key)
        if prior is not None:
            if prior['request'] != fingerprint:
                raise ControlRefusal('parameters_idempotency_payload_changed')
            return self.save_lookup(idempotency_key)
        if self.reason():
            raise ControlRefusal(self.reason())
        target = self.saved or self.current
        if expected_revision != target['revision']:
            raise ControlRefusal('parameters_revision_changed')
        if self.opening and len(normalized) == 5:
            normalized = {**target['values'], **normalized}
        self.saved = dict(revision=expected_revision + 1, values=normalized,
            config_hash=hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest())
        self.save_commits += 1
        self.saves[idempotency_key] = dict(request=fingerprint,
            revision=self.saved['revision'], config_hash=self.saved['config_hash'])
        return self.save_lookup(idempotency_key)

    def commit(self, **arguments):
        if self.fail_commit:
            raise RuntimeError('simulated durable commit failure')
        action = arguments['action']
        if action == 'start' and self.has_parameters:
            target = self.saved or self.current
            if 'parameters_revision' in arguments:
                if self.reason():
                    raise ControlRefusal(self.reason())
                if (arguments['parameters_revision'] != target['revision']
                        or arguments['parameters_hash'] != target['config_hash']):
                    raise ControlRefusal('parameters_revision_changed')
                self.current, self.saved = deepcopy(target), None
                self.control.binding['config_hash'] = self.current['config_hash']
            elif self.saved is not None:
                raise ControlRefusal('saved_parameters_require_explicit_start')
        self.commits += 1
        self.values['intent_revision'] += 1
        self.values['intent'] = 'RUNNING' if action == 'start' else 'STOPPING'
        self.values['phase'] = self.values['intent']
        self.values['allowed_actions'] = ['stop']
        operation = dict(operation_id=uid(), state='accepted',
            intent=self.values['intent'], intent_revision=self.values['intent_revision'],
            accepted_ts=time.time(), updated_ts=time.time(), **arguments)
        self.operations[operation['operation_id']] = operation
        return deepcopy(operation)

    async def request(self, payload):
        self.frames.append(deepcopy(payload))
        if self.drop_before == payload['op']:
            self.drop_before = None
            raise Refusal('owner_unavailable', 503)
        result = self.control.handle(payload)
        if self.drop_after == payload['op']:
            self.drop_after = None
            raise Refusal('owner_unavailable', 503)
        if self.corrupt_after == payload['op']:
            self.corrupt_after = None
            result['parameters'] = {'corrupted': True}
        return result


def client(owner, **kwargs):
    return TestClient(create_app(owner, origin=ORIGIN), base_url='http://127.0.0.1:8786',
        client=('127.0.0.1', 1234), **kwargs)


def session(c):
    response = c.get(f'{PREFIX}/session')
    assert response.status_code == 200, response.text
    return {'Origin': ORIGIN, 'X-Entropy-CSRF': response.json()['csrf_token']}


def save_body(*, revision=0, values=None, key=None):
    return dict(source='basis', expected_revision=revision,
        values=dict(VALUES if values is None else values), idempotency_key=key or uid())


def save(c, headers, **kwargs):
    response = c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body(**kwargs))
    assert response.status_code == 200, response.text
    return response.json()


def start_preview(c, headers, record=None):
    body = dict(source='basis', action='start')
    if record is not None:
        body.update(parameters_revision=record['revision'], parameters_hash=record['config_hash'])
    response = c.post(f'{PREFIX}/preview', headers=headers, json=body)
    assert response.status_code == 200, response.text
    return response.json()


def reason(response, code, status=409):
    assert response.status_code == status, response.text
    assert response.json()['detail']['code'] == code, response.text


def test_save_is_only_saved_revision_then_explicit_start_advances_binding():
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        saved = save(c, headers)['parameters']
        assert saved['current']['revision'] == 0 and saved['saved']['revision'] == 1
        assert owner.values['intent'] == 'STOPPED' and owner.commits == 0
        assert owner.control.binding == BINDING and owner.binding == BINDING
        p = start_preview(c, headers, saved['saved'])
        assert owner.commits == 0
        accepted = c.post(f'{PREFIX}/confirm', headers=headers,
            json=dict(preview_id=p['preview_id'], idempotency_key=uid()))
        assert accepted.status_code == 200, accepted.text
        active = accepted.json()['state']['parameters']
        assert active['saved'] is None and active['current'] == saved['saved']
        assert owner.commits == 1 and owner.values['intent'] == 'RUNNING'
        assert owner.binding['config_hash'] == owner.control.binding['config_hash'] == active['current']['config_hash']
        state = c.get(f'{PREFIX}/state').json()
        assert state['enabled'] is True and state['state']['allowed_actions'] == ['stop']


@pytest.mark.parametrize('field', list(VALUES))
@pytest.mark.parametrize('invalid', ['NaN', 'sNaN', 'Infinity', '-Infinity', '-1', '0',
    '1e9999', '1e-9999', '', ' 5', '5 ', 'nope', True, False, 5, None])
def test_invalid_numeric_parameters_never_save(field, invalid):
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        owner.frames.clear()
        values = {**VALUES, field: invalid}
        response = c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body(values=values))
        assert response.status_code in (409, 422), response.text
        assert owner.save_commits == owner.commits == 0
        assert owner.saved is None and owner.current['revision'] == 0
        assert owner.control.binding == BINDING


@pytest.mark.parametrize('field', list(VALUES))
def test_above_each_trusted_limit_rejected(field):
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        response = c.post(f'{PREFIX}/parameters/save', headers=headers,
            json=save_body(values={**VALUES, field: str(float(LIMITS[field]) + .01)}))
        reason(response, 'parameter_out_of_bounds_' + field)
    assert owner.saved is None and owner.save_commits == owner.commits == 0


@pytest.mark.parametrize('change', [dict(source='funding'), dict(source='other'),
    dict(expected_revision=True), dict(expected_revision=-1), dict(expected_revision=1.0),
    dict(config_hash='evil'), dict(socket='/tmp/evil'), dict(account_binding_hash='evil'),
    dict(command='start'), dict(idempotency_key='not-a-uuid')])
def test_save_schema_cannot_override_scope_or_revision(change):
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        owner.frames.clear()
        response = c.post(f'{PREFIX}/parameters/save', headers=headers,
            json={**save_body(), **change})
        assert response.status_code == 422, response.text
        assert owner.frames == []
    assert owner.save_commits == owner.commits == 0


@pytest.mark.parametrize('values', [{**VALUES, 'risk_locks': []},
    {key: value for key, value in VALUES.items() if key != 'entry_band_bps'}])
def test_values_require_complete_known_fields(values):
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        owner.frames.clear()
        response = c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body(values=values))
        assert response.status_code == 422 and owner.frames == []


@pytest.mark.parametrize('headers', [{}, {'Origin': 'null'}, {'Origin': 'https://evil.invalid'},
    {'Origin': 'http://localhost:5186'}, {'Origin': ORIGIN},
    {'Origin': ORIGIN, 'X-Entropy-CSRF': 'incorrect'},
    {'Origin': ORIGIN, 'Sec-Fetch-Site': 'cross-site'}])
def test_save_origin_and_csrf_before_owner(headers):
    owner = ParameterOwner()
    with client(owner) as c:
        session(c)
        owner.frames.clear()
        assert c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body()).status_code == 403
        assert owner.frames == []


def test_parameter_routes_require_loopback_host_and_session():
    owner = ParameterOwner()
    with client(owner) as c:
        assert c.get(f'{PREFIX}/parameters/saves/by-key/{uid()}').status_code == 403
        headers = session(c)
        owner.frames.clear()
        assert c.post(f'{PREFIX}/parameters/save', headers={**headers, 'Host': 'evil.invalid'},
            json=save_body()).status_code == 403
        assert owner.frames == []
    with TestClient(create_app(owner, origin=ORIGIN), base_url='http://127.0.0.1:8786',
            client=('192.0.2.4', 1234)) as remote:
        assert remote.get(f'{PREFIX}/session').status_code == 403
        assert remote.post(f'{PREFIX}/parameters/save', headers={'Origin': ORIGIN},
            json=save_body()).status_code == 403


@pytest.mark.parametrize('field', ['account_binding_hash', 'pair_identity', 'deployment', 'batch_id'])
def test_fixed_owner_scope_cannot_follow_identity_change(field):
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        owner.control.binding[field] = 'different'
        state = c.get(f'{PREFIX}/state').json()
        assert state['enabled'] is False and state['reason_code'] == 'binding_mismatch'
        reason(c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body()), 'binding_mismatch')
    assert owner.save_commits == owner.commits == 0 and owner.binding == BINDING


def test_wrong_base_hash_and_unproven_config_change_do_not_advance_proxy():
    owner = ParameterOwner()
    with client(owner) as c:
        session(c)
        owner.base_hash = 'c' * 64
        state = c.get(f'{PREFIX}/state').json()
        assert state['enabled'] is False and state['reason_code'] == 'binding_mismatch'
        assert owner.binding == BINDING
    legacy = ParameterOwner(parameters=False)
    with client(legacy) as c:
        session(c)
        legacy.control.binding['config_hash'] = 'c' * 64
        state = c.get(f'{PREFIX}/state').json()
        assert state['enabled'] is False and state['reason_code'] == 'binding_mismatch'
        assert legacy.binding == BINDING


def test_stale_save_revision_and_changed_idempotency_payload_refused():
    owner = ParameterOwner()
    key = uid()
    with client(owner) as c:
        headers = session(c)
        first = save(c, headers, key=key)
        duplicate = save(c, headers, key=key)
        assert duplicate['parameters'] == first['parameters'] and owner.save_commits == 1
        reason(c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body()),
            'parameters_revision_changed')
        reason(c.post(f'{PREFIX}/parameters/save', headers=headers,
            json=save_body(key=key, values={**VALUES, 'entry_band_bps': '6'})),
            'parameters_idempotency_payload_changed')
    assert owner.save_commits == 1 and owner.commits == 0


@pytest.mark.parametrize('change', [dict(parameters_revision=0, parameters_hash='b' * 64),
    dict(parameters_revision=1, parameters_hash='c' * 64), {}])
def test_saved_parameters_cannot_start_with_omitted_or_stale_version(change):
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        save(c, headers)
        reason(c.post(f'{PREFIX}/preview', headers=headers,
            json=dict(source='basis', action='start', **change)), 'parameters_revision_changed')
    assert owner.commits == 0 and owner.current['revision'] == 0


def test_revision_changed_between_preview_and_confirm_never_applies_old_saved():
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        target = save(c, headers)['parameters']['saved']
        p = start_preview(c, headers, target)
        next_target = save(c, headers, revision=1,
            values={**VALUES, 'entry_band_bps': '6'})['parameters']['saved']
        reason(c.post(f'{PREFIX}/confirm', headers=headers,
            json=dict(preview_id=p['preview_id'], idempotency_key=uid())), 'parameters_revision_changed')
        assert owner.saved == next_target and owner.current['revision'] == 0
    assert owner.commits == 0 and owner.control.binding == BINDING


@pytest.mark.parametrize('stage', ['save', 'preview', 'confirm'])
@pytest.mark.parametrize('change,code', [
    (dict(intent='RUNNING'), 'parameters_require_stopped'),
    (dict(intent='STOPPING'), 'parameters_require_stopped'),
    (dict(active_cycle_id='open-cycle'), 'parameters_active_cycle'),
    (dict(entropy_units=1), 'parameters_require_flat'),
    (dict(hedge_units=-1), 'parameters_require_flat'),
    (dict(pending_orders=[dict(order_ref='pending', state='UNKNOWN')]), 'parameters_pending_orders'),
    (dict(position_authoritative=False), 'parameters_position_refresh_required'),
    (dict(position_observed_ts=0), 'parameters_position_refresh_required'),
])
def test_unsafe_boundary_rechecked_at_every_parameter_stage(stage, change, code):
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        if stage != 'save':
            target = save(c, headers)['parameters']['saved']
            if stage == 'confirm':
                p = start_preview(c, headers, target)
        owner.values.update(change)
        if stage == 'save':
            response = c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body())
        elif stage == 'preview':
            response = c.post(f'{PREFIX}/preview', headers=headers, json=dict(source='basis',
                action='start', parameters_revision=target['revision'], parameters_hash=target['config_hash']))
        else:
            response = c.post(f'{PREFIX}/confirm', headers=headers,
                json=dict(preview_id=p['preview_id'], idempotency_key=uid()))
        reason(response, code)
    assert owner.commits == 0 and owner.current['revision'] == 0
    assert owner.control.binding == BINDING


def test_parameter_save_preserves_risk_lock_and_start_cannot_clear_it():
    owner = ParameterOwner()
    owner.values['risk_locks'] = ['BATCH_LOSS_LIMIT']
    owner.values['blocked_reason'] = 'BATCH_LOSS_LIMIT'
    owner.values['allowed_actions'] = []
    with client(owner) as c:
        headers = session(c)
        target = save(c, headers)['parameters']['saved']
        reason(c.post(f'{PREFIX}/preview', headers=headers, json=dict(source='basis', action='start',
            parameters_revision=target['revision'], parameters_hash=target['config_hash'])), 'risk_locked')
    assert owner.values['risk_locks'] == ['BATCH_LOSS_LIMIT'] and owner.commits == 0
    assert owner.control.binding['batch_id'] == BINDING['batch_id']


@pytest.mark.parametrize('lost', ['before', 'after', 'corrupt'])
def test_unknown_parameter_save_recovers_only_by_read_lookup(lost):
    owner = ParameterOwner()
    key = uid()
    with client(owner) as c:
        headers = session(c)
        setattr(owner, {'before': 'drop_before', 'after': 'drop_after', 'corrupt': 'corrupt_after'}[lost],
            'parameters_save')
        reason(c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body(key=key)),
            'outcome_unknown', 503)
        before = (owner.save_commits, owner.commits)
        owner.frames.clear()
        lookup = c.get(f'{PREFIX}/parameters/saves/by-key/{key}?source=basis')
        if lost == 'before':
            reason(lookup, 'parameter_save_not_found')
            assert owner.saved is None
        else:
            assert lookup.status_code == 200, lookup.text
            assert lookup.json()['parameters']['saved']['revision'] == 1
        assert [frame['op'] for frame in owner.frames] == ['state', 'parameters_lookup']
        assert (owner.save_commits, owner.commits) == before


def test_start_lost_response_recovers_across_new_hash_with_original_operation_key():
    owner = ParameterOwner()
    key = uid()
    with client(owner) as c:
        headers = session(c)
        target = save(c, headers)['parameters']['saved']
        p = start_preview(c, headers, target)
        owner.drop_after = 'confirm'
        body = dict(preview_id=p['preview_id'], idempotency_key=key)
        reason(c.post(f'{PREFIX}/confirm', headers=headers, json=body), 'outcome_unknown', 503)
        assert owner.commits == 1 and owner.current == target
        assert owner.binding['config_hash'] == BINDING['config_hash']  # response was lost
        recovered = c.get(f'{PREFIX}/operations/by-key/{key}')
        assert recovered.status_code == 200, recovered.text
        operation_id = recovered.json()['operation_id']
        assert owner.binding['config_hash'] == target['config_hash']
        owner.frames.clear()
        duplicate = c.post(f'{PREFIX}/confirm', headers=headers, json=body)
        assert duplicate.status_code == 200 and duplicate.json()['operation_id'] == operation_id
        assert [frame['op'] for frame in owner.frames] == ['state', 'operation']
        another_ui_key = c.post(f'{PREFIX}/confirm', headers=headers,
            json=dict(preview_id=p['preview_id'], idempotency_key=uid()))
        assert another_ui_key.status_code == 200 and another_ui_key.json()['operation_id'] == operation_id
    assert owner.commits == owner.save_commits == 1


def test_commit_failure_does_not_partially_activate_configuration():
    owner = ParameterOwner()
    key = uid()
    with client(owner) as c:
        headers = session(c)
        target = save(c, headers)['parameters']['saved']
        p = start_preview(c, headers, target)
        owner.fail_commit = True
        reason(c.post(f'{PREFIX}/confirm', headers=headers,
            json=dict(preview_id=p['preview_id'], idempotency_key=key)), 'outcome_unknown', 503)
        assert owner.saved == target and owner.current['revision'] == 0
        assert owner.values['intent'] == 'STOPPED' and owner.control.binding == BINDING
        reason(c.get(f'{PREFIX}/operations/by-key/{key}'), 'operation_not_found')
    assert owner.commits == 0


def test_expired_session_renewal_does_not_replay_save_or_start(monkeypatch):
    now = [time.time()]
    monkeypatch.setattr(module.time, 'time', lambda: now[0])
    owner = ParameterOwner()
    with client(owner) as c:
        old_headers = session(c)
        key = uid()
        saved = save(c, old_headers, key=key)
        p = start_preview(c, old_headers, saved['parameters']['saved'])
        owner.frames.clear()
        now[0] += 3601
        assert c.post(f'{PREFIX}/parameters/save', headers=old_headers, json=save_body()).status_code == 403
        headers = session(c)
        assert headers['X-Entropy-CSRF'] != old_headers['X-Entropy-CSRF']
        assert c.get(f'{PREFIX}/state').status_code == 200
        assert c.get(f'{PREFIX}/parameters/saves/by-key/{key}').status_code == 200
        reason(c.post(f'{PREFIX}/confirm', headers=headers,
            json=dict(preview_id=p['preview_id'], idempotency_key=uid())), 'preview_unavailable')
        assert all(frame['op'] in ('state', 'parameters_lookup') for frame in owner.frames)
    assert owner.save_commits == 1 and owner.commits == 0


def test_legacy_owner_refuses_parameters_but_preserves_legacy_start():
    owner = ParameterOwner(parameters=False)
    with client(owner) as c:
        headers = session(c)
        reason(c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body()), 'parameters_unavailable')
        p = start_preview(c, headers)
        response = c.post(f'{PREFIX}/confirm', headers=headers,
            json=dict(preview_id=p['preview_id'], idempotency_key=uid()))
        assert response.status_code == 200, response.text
    assert owner.save_commits == 0 and owner.commits == 1


def test_owner_durable_confirmation_with_original_hash_replays_once_after_activation():
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        target = save(c, headers)['parameters']['saved']
        start_preview(c, headers, target)
        frame = dict(owner.frames[-1], op='confirm', request_id=uid(),
            receipt_id=next(reversed(owner.control._receipts)), idempotency_key=uid())
        first = owner.control.handle(frame)
        assert first['ok'] is True
        assert owner.control.binding['config_hash'] != frame['config_hash']
        owner.control._receipts.clear()  # accepted durable key survives preview loss
        replay = owner.control.handle(dict(frame, request_id=uid()))
        assert replay['ok'] is True and replay['operation'] == first['operation']
        assert owner.commits == 1
        for field in ('account_binding_hash', 'pair_identity', 'deployment', 'batch_id'):
            denied = owner.control.handle(dict(frame, request_id=uid(), **{field: 'different'}))
            assert denied['ok'] is False and denied['reason'] == 'binding_mismatch'
        changed = owner.control.handle(dict(frame, request_id=uid(), parameters_revision=2))
        assert changed['ok'] is False and changed['reason'] == 'idempotency_conflict'
    assert owner.commits == 1


@pytest.mark.parametrize('change', [dict(parameters_revision=True, parameters_hash='b' * 64),
    dict(parameters_revision=-1, parameters_hash='b' * 64), dict(parameters_revision=1),
    dict(parameters_hash='b' * 64), dict(parameters_revision=0, parameters_hash='g' * 64),
    dict(parameters_revision=0, parameters_hash='b' * 63), dict(config_hash='b' * 64),
    dict(account_binding_hash='a' * 64), dict(batch_id='new-batch')])
def test_start_version_schema_rejects_partial_or_browser_scope_override(change):
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        owner.frames.clear()
        response = c.post(f'{PREFIX}/preview', headers=headers,
            json=dict(source='basis', action='start', **change))
        assert response.status_code == 422, response.text
        assert owner.frames == []
    assert owner.commits == 0


@pytest.mark.parametrize('change', [dict(revision=True), dict(revision=-1),
    dict(config_hash='short'), dict(config_hash='c' * 64), dict(values={**VALUES, 'entry_band_bps': 'NaN'}),
    dict(values={**VALUES, 'entry_band_bps': True}), dict(values={**VALUES, 'entropy_exit_room_bps': '13'}),
    dict(values={**VALUES, 'entry_band_bps': '1.000000001'}),
    dict(values={**VALUES, 'entry_band_bps': '1e-999999999'})])
def test_proxy_rejects_invalid_owner_current_version_without_binding_update(change):
    owner = ParameterOwner()
    with client(owner) as c:
        session(c)
        owner.current.update(change)
        response = c.get(f'{PREFIX}/state').json()
        assert response['enabled'] is False and response['reason_code'] == 'invalid_owner_response'
        assert owner.binding == BINDING


def test_proxy_rejects_saved_revision_not_newer_than_current():
    owner = ParameterOwner()
    with client(owner) as c:
        session(c)
        owner.saved = deepcopy(owner.current)
        response = c.get(f'{PREFIX}/state').json()
        assert response['enabled'] is False and response['reason_code'] == 'invalid_owner_response'
        assert owner.binding == BINDING


def test_unavailable_owner_or_wrong_lookup_source_has_no_mutating_frame():
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        owner.frames.clear()
        owner.drop_before = 'state'
        reason(c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body()),
            'owner_unavailable', 503)
        assert [frame['op'] for frame in owner.frames] == ['state']
        owner.frames.clear()
        reason(c.get(f'{PREFIX}/parameters/saves/by-key/{uid()}?source=funding'),
            'unsupported_source', 503)
        assert owner.frames == []
    assert owner.save_commits == owner.commits == 0


def test_lookup_original_save_receipt_is_not_replaced_by_later_saved_revision():
    owner = ParameterOwner()
    key = uid()
    with client(owner) as c:
        headers = session(c)
        first = save(c, headers, key=key)
        target = first['parameters']['saved']
        second = save(c, headers, revision=1, values={**VALUES, 'entry_band_bps': '6'})
        assert second['parameters']['saved']['revision'] == 2
        owner.frames.clear()
        response = c.get(f'{PREFIX}/parameters/saves/by-key/{key}')
        assert response.status_code == 200, response.text
        found = response.json()
        assert found['save_receipt'] == dict(revision=target['revision'], config_hash=target['config_hash'])
        assert found['parameters']['saved']['revision'] == 2
        assert found['parameters']['saved']['config_hash'] != found['save_receipt']['config_hash']
        assert [frame['op'] for frame in owner.frames] == ['state', 'parameters_lookup']
    assert owner.save_commits == 2 and owner.commits == 0


@pytest.mark.parametrize('change', [dict(revision=True), dict(revision=-1),
    dict(revision='1'), dict(config_hash='short'), dict(config_hash=None), dict(config_hash='x' * 64)])
def test_invalid_owner_save_receipt_is_not_reported_as_accepted(change):
    owner = ParameterOwner()
    key = uid()
    with client(owner) as c:
        headers = session(c)
        save(c, headers, key=key)
        owner.saves[key].update(change)
        owner.frames.clear()
        reason(c.get(f'{PREFIX}/parameters/saves/by-key/{key}'), 'invalid_owner_response', 503)
        assert [frame['op'] for frame in owner.frames] == ['state', 'parameters_lookup']
    assert owner.save_commits == 1 and owner.commits == 0


def test_matching_current_and_state_nonhex_hash_cannot_advance_proxy_binding():
    owner = ParameterOwner()
    with client(owner) as c:
        session(c)
        owner.current['config_hash'] = 'x' * 64
        owner.control.binding['config_hash'] = 'x' * 64
        response = c.get(f'{PREFIX}/state').json()
        assert response['enabled'] is False and response['reason_code'] == 'invalid_owner_response'
        assert owner.binding == BINDING
    assert owner.commits == owner.save_commits == 0


def test_huge_negative_exponent_refused_before_formatting_with_timeout_guard(monkeypatch):
    from entropy_arb.v3 import parameter_config

    def timeout(_signum, _frame):
        raise AssertionError('short invalid exponent was not promptly rejected')

    def no_formatting(*_args, **_kwargs):
        raise AssertionError('invalid exponent reached decimal string expansion')

    # A regression must fail before allocating a huge normalized decimal string.
    monkeypatch.setattr(parameter_config, 'format', no_formatting, raising=False)
    previous_handler = signal.signal(signal.SIGALRM, timeout)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, 2)
    try:
        with pytest.raises(ValueError, match='invalid_parameter_entropy_exit_room_bps'):
            validate_parameters({**VALUES, 'entropy_exit_room_bps': '1e-999999999'}, LIMITS)
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        signal.signal(signal.SIGALRM, previous_handler)


def opening_values(entropy=None, lighter=None):
    return {**VALUES, 'entropy_open_room_bps': entropy, 'lighter_open_room_bps': lighter}


def test_opening_values_save_pending_then_start_applies_exact_policy_and_hash():
    owner = ParameterOwner(opening=True)
    with client(owner) as c:
        headers = session(c)
        initial = c.get(f'{PREFIX}/state').json()
        assert initial['enabled'] is True
        assert initial['state']['parameters']['opening_policies']['current'] == owner.base_policies
        result = save(c, headers, values=opening_values('0', '12'))['parameters']
        assert owner.frames[-1]['values'] == opening_values('0', '12')
        assert result['current']['values'] == opening_values()
        assert result['opening_policies']['current'] == owner.base_policies
        assert result['opening_policies']['saved'] == dict(
            entropy=dict(rooms_bps=['0'] * 3, hard_cap_bps='0'),
            lighter=dict(rooms_bps=['12'] * 3, hard_cap_bps='12'))
        assert owner.commits == 0 and owner.binding == BINDING
        target = result['saved']
        p = start_preview(c, headers, target)
        accepted = c.post(f'{PREFIX}/confirm', headers=headers,
            json=dict(preview_id=p['preview_id'], idempotency_key=uid()))
        assert accepted.status_code == 200, accepted.text
        active = accepted.json()['state']['parameters']
        assert active['current']['values'] == opening_values('0', '12')
        assert active['opening_policies']['current'] == result['opening_policies']['saved']
        assert active['saved'] is active['opening_policies']['saved'] is None
        assert owner.binding['config_hash'] == target['config_hash']


def test_legacy_missing_fields_are_forwarded_unchanged_and_do_not_reset_new_saved_policy():
    owner = ParameterOwner(opening=True)
    with client(owner) as c:
        headers = session(c)
        save(c, headers, values=opening_values('4', '7'))
        response = save(c, headers, revision=1, values={**VALUES, 'entry_band_bps': '6'})
        assert owner.frames[-1]['values'] == {**VALUES, 'entry_band_bps': '6'}
        assert response['parameters']['saved']['values'] == {
            **opening_values('4', '7'), 'entry_band_bps': '6'}
        reset = save(c, headers, revision=2, values=opening_values())['parameters']
        assert owner.frames[-1]['values'] == opening_values()
        assert reset['saved']['values'] == opening_values()
        assert reset['opening_policies']['saved'] == owner.base_policies
    assert owner.save_commits == 3 and owner.commits == 0


def test_missing_and_explicit_null_opening_modes_cannot_reuse_new_save_key():
    owner = ParameterOwner(opening=True)
    key = uid()
    with client(owner) as c:
        headers = session(c)
        first = save(c, headers, key=key)
        reason(c.post(f'{PREFIX}/parameters/save', headers=headers,
            json=save_body(key=key, values=opening_values())),
            'parameters_idempotency_payload_changed')
        assert c.get(f'{PREFIX}/parameters/saves/by-key/{key}').json()['save_receipt'] == dict(
            revision=first['parameters']['saved']['revision'],
            config_hash=first['parameters']['saved']['config_hash'])
    assert owner.save_commits == 1 and owner.commits == 0


@pytest.mark.parametrize('values', [opening_values('0', None), opening_values(None, '4')])
def test_explicit_opening_value_is_refused_before_old_owner_save(values):
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        owner.frames.clear()
        reason(c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body(values=values)),
            'opening_parameters_unavailable')
        assert [frame['op'] for frame in owner.frames] == ['state']
    assert owner.save_commits == owner.commits == 0 and owner.binding == BINDING


def test_null_opening_values_allow_old_owner_save_and_legacy_controls():
    owner = ParameterOwner()
    with client(owner) as c:
        headers = session(c)
        response = save(c, headers, values=opening_values())['parameters']
        assert owner.frames[-1]['values'] == VALUES
        assert response['saved']['values'] == VALUES
        p = start_preview(c, headers, response['saved'])
        accepted = c.post(f'{PREFIX}/confirm', headers=headers,
            json=dict(preview_id=p['preview_id'], idempotency_key=uid()))
        assert accepted.status_code == 200, accepted.text
    assert owner.save_commits == owner.commits == 1


@pytest.mark.parametrize('field,limit', [('entropy_open_room_bps', '10.00000001'),
    ('lighter_open_room_bps', '12.00000001')])
@pytest.mark.parametrize('invalid', ['NaN', 'Infinity', '-1', '0.000000001',
    '1e-999999999', '', ' ', ' 0', '0 ', 'nope', True, False, 1])
def test_invalid_opening_values_never_reach_a_save_commit(field, limit, invalid):
    owner = ParameterOwner(opening=True)
    with client(owner) as c:
        headers = session(c)
        for value in (invalid, limit):
            response = c.post(f'{PREFIX}/parameters/save', headers=headers,
                json=save_body(values={**opening_values(), field: value}))
            assert response.status_code in (409, 422), response.text
            assert owner.saved is None and owner.binding == BINDING
    assert owner.save_commits == owner.commits == 0


@pytest.mark.parametrize('values', [{**VALUES, 'entropy_open_room_bps': None},
    {**opening_values(), 'unknown': None}])
def test_partial_or_extra_opening_fields_are_rejected_before_owner(values):
    owner = ParameterOwner(opening=True)
    with client(owner) as c:
        headers = session(c)
        owner.frames.clear()
        response = c.post(f'{PREFIX}/parameters/save', headers=headers, json=save_body(values=values))
        assert response.status_code == 422, response.text
        assert owner.frames == []


@pytest.mark.parametrize('corruption', ['missing_capability', 'false_capability',
    'integer_capability', 'extra_capability', 'missing_policy', 'extra_policy',
    'unexpected_saved_policy', 'base_limit_mismatch', 'wrong_inherited_policy',
    'wrong_fixed_policy', 'missing_limit', 'extra_limit', 'numeric_limit', 'six_fields'])
def test_new_owner_policy_corruption_disables_control_without_advancing_binding(corruption):
    owner = ParameterOwner(opening=True)
    with client(owner) as c:
        session(c)
        parameters = owner.parameters()
        if corruption == 'missing_capability':
            parameters.pop('capabilities')
        elif corruption == 'false_capability':
            parameters['capabilities']['opening_concessions'] = False
        elif corruption == 'integer_capability':
            parameters['capabilities']['opening_concessions'] = 1
        elif corruption == 'extra_capability':
            parameters['capabilities']['unknown'] = True
        elif corruption == 'missing_policy':
            parameters.pop('opening_policies')
        elif corruption == 'extra_policy':
            parameters['opening_policies']['unknown'] = {}
        elif corruption == 'unexpected_saved_policy':
            parameters['opening_policies']['saved'] = deepcopy(owner.base_policies)
        elif corruption == 'base_limit_mismatch':
            parameters['opening_policies']['base']['entropy']['hard_cap_bps'] = '12'
        elif corruption == 'wrong_inherited_policy':
            parameters['opening_policies']['current']['lighter']['rooms_bps'] = ['4'] * 3
        elif corruption == 'wrong_fixed_policy':
            parameters['current']['values']['entropy_open_room_bps'] = '0'
        elif corruption == 'missing_limit':
            parameters['limits'].pop('lighter_open_room_bps')
        elif corruption == 'extra_limit':
            parameters['limits']['unknown'] = '1'
        elif corruption == 'numeric_limit':
            parameters['limits']['entropy_open_room_bps'] = 10
        elif corruption == 'six_fields':
            parameters['current']['values'].pop('lighter_open_room_bps')
        owner.parameters = lambda: deepcopy(parameters)
        response = c.get(f'{PREFIX}/state').json()
        assert response['enabled'] is False and response['reason_code'] == 'invalid_owner_response'
        assert owner.binding == BINDING
    assert owner.commits == owner.save_commits == 0


def test_opening_parameter_start_cannot_clear_single_cycle_review_loss():
    owner = ParameterOwner(opening=True)
    owner.values.update(risk_locks=['SINGLE_CYCLE_REVIEW_LOSS'], allowed_actions=[])
    with client(owner) as c:
        headers = session(c)
        target = save(c, headers, values=opening_values('3', '4'))['parameters']['saved']
        reason(c.post(f'{PREFIX}/preview', headers=headers,
            json=dict(source='basis', action='start', parameters_revision=target['revision'],
                parameters_hash=target['config_hash'])), 'risk_locked')
    assert owner.values['risk_locks'] == ['SINGLE_CYCLE_REVIEW_LOSS'] and owner.commits == 0
