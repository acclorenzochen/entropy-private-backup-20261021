"""Real candidate runtime/Journal -> owner protocol -> in-memory ASGI API.

All venue objects are existing offline fakes. No trading loop or transport runs.
"""
from copy import deepcopy
from decimal import Decimal as D
from uuid import uuid4
import sqlite3

from fastapi.testclient import TestClient
import pytest

from entropy_arb.dashboard_api.v3_control_app import create_app
from entropy_arb.v3.live_host import canonical_config_hash
from entropy_arb.v3.parameter_config import apply_parameters
from tests_v3.test_parameter_config import VALUES, make_owner


PREFIX = '/api/v3-control'
ORIGIN = 'http://127.0.0.1:5186'


def uid():
    return str(uuid4())


def opening_values(entropy=None, lighter=None):
    return {**VALUES, 'entropy_open_room_bps': entropy, 'lighter_open_room_bps': lighter}


class RuntimeOwner:
    enabled = True

    def __init__(self, runtime):
        self.runtime = runtime
        self.binding = dict(runtime.operator_control.binding)
        self.binding['config_hash'] = runtime._parameters_base_config_hash
        self.frames = []

    async def request(self, payload):
        self.frames.append(deepcopy(payload))
        return self.runtime.operator_control.handle(payload)


@pytest.fixture
def integration_owner(tmp_path, monkeypatch):
    # Existing TestClient runs ASGI on its portal thread. This test-only
    # connection remains under offline_guard and every request is serial.
    connect = sqlite3.connect
    def temporary_connection(*args, **kwargs):
        return connect(*args, **{**kwargs, 'check_same_thread': False})
    monkeypatch.setattr(sqlite3, 'connect', temporary_connection)
    runtime, entropy, lighter, journal = make_owner(tmp_path, base_hash=None)
    owner = RuntimeOwner(runtime)
    assert owner.binding['config_hash'] == canonical_config_hash(
        runtime._parameters_base_settings, pair_identity=runtime.pair_id)
    try:
        yield runtime, entropy, lighter, journal, owner
        assert entropy.sent == lighter.sent == []
        assert journal.active_live_cycle() is None and not journal.pending()
    finally:
        journal.close()


def api_client(owner):
    return TestClient(create_app(owner, origin=ORIGIN),
        client=('127.0.0.1', 1234), base_url='http://127.0.0.1:8786')


def session(client):
    response = client.get(f'{PREFIX}/session')
    assert response.status_code == 200, response.text
    assert response.json()['enabled'] is True
    return {'Origin': ORIGIN, 'X-Entropy-CSRF': response.json()['csrf_token']}


def state(client):
    response = client.get(f'{PREFIX}/state')
    assert response.status_code == 200, response.text
    assert response.json()['enabled'] is True, response.text
    return response.json()['state']


def save(client, headers, values, *, revision=0):
    response = client.post(f'{PREFIX}/parameters/save', headers=headers,
        json=dict(source='basis', expected_revision=revision, values=values,
            idempotency_key=uid()))
    assert response.status_code == 200, response.text
    assert set(response.json()['save_receipt']) == {'revision', 'config_hash'}
    return response.json()['parameters']


def command(client, headers, action, record=None):
    body = dict(source='basis', action=action)
    if record is not None:
        body.update(parameters_revision=record['revision'], parameters_hash=record['config_hash'])
    preview = client.post(f'{PREFIX}/preview', headers=headers, json=body)
    assert preview.status_code == 200, preview.text
    response = client.post(f'{PREFIX}/confirm', headers=headers,
        json=dict(preview_id=preview.json()['preview_id'], idempotency_key=uid()))
    assert response.status_code == 200, response.text
    return response.json()


def test_real_runtime_get_projects_legacy_document_without_writing_or_trading(integration_owner):
    runtime, _, _, journal, owner = integration_owner
    before = journal.operator_parameters(runtime._parameters_scope)

    with api_client(owner) as client:
        session(client)
        current = state(client)
        parameters = current['parameters']
        assert current['intent'] == 'STOPPED' and parameters['can_save'] is True
        assert parameters['capabilities'] == dict(opening_concessions=True)
        assert parameters['current']['values'] == opening_values()
        assert parameters['limits']['entropy_open_room_bps'] == '10'
        assert parameters['limits']['lighter_open_room_bps'] == '12'
        assert parameters['opening_policies'] == dict(base=dict(
            entropy=dict(rooms_bps=['3'] * 3, hard_cap_bps='10'),
            lighter=dict(rooms_bps=['4', '8', '12'], hard_cap_bps='12')),
            current=dict(entropy=dict(rooms_bps=['3'] * 3, hard_cap_bps='10'),
                lighter=dict(rooms_bps=['4', '8', '12'], hard_cap_bps='12')), saved=None)
        assert current['config_hash'] == parameters['current']['config_hash'] == owner.binding['config_hash']
    assert before['schema_version'] == 1
    assert journal.operator_parameters(runtime._parameters_scope) == before
    assert all(frame['op'] == 'state' for frame in owner.frames)


def test_real_runtime_null_save_resets_pending_policy_and_start_inherits_base(integration_owner):
    runtime, _, _, journal, owner = integration_owner
    base = runtime._parameters_base_settings

    with api_client(owner) as client:
        headers = session(client)
        initial = (state(client))['parameters']['opening_policies']['base']
        fixed = save(client, headers, opening_values('4', '7'))
        reset = save(client, headers, opening_values(), revision=1)
        assert fixed['saved']['config_hash'] != reset['saved']['config_hash']
        assert reset['current']['revision'] == 0 and reset['saved']['revision'] == 2
        assert reset['opening_policies']['saved'] == initial
        assert runtime.settings is base
        command(client, headers, 'start', reset['saved'])
        current = state(client)
        assert current['parameters']['saved'] is None
        assert current['parameters']['current'] == reset['saved']
        assert current['parameters']['opening_policies']['current'] == initial
        assert runtime.settings.first_leg_execution_policy('open') == base.first_leg_execution_policy('open')
        assert runtime.settings.hedge_execution_policy('open') == base.hedge_execution_policy('open')
        assert current['config_hash'] == canonical_config_hash(runtime.settings, pair_identity=runtime.pair_id)
    assert journal.operator_parameters(runtime._parameters_scope)['schema_version'] == 2


def test_real_runtime_zero_and_fixed_opening_values_activate_after_confirm_only(integration_owner):
    runtime, _, _, _, owner = integration_owner
    before = runtime.settings

    with api_client(owner) as client:
        headers = session(client)
        parameters = save(client, headers, opening_values('0', '2'))
        assert runtime.settings is before
        assert runtime.operator_control.binding['config_hash'] == owner.binding['config_hash']
        command(client, headers, 'start', parameters['saved'])
        current = state(client)
        assert runtime.settings.first_leg_execution_policy('open') == ((D('0'),) * 3, D('0'))
        assert runtime.settings.hedge_execution_policy('open') == ((D('2'),) * 3, D('2'))
        assert runtime.settings.entry_band_bps == before.entry_band_bps
        assert runtime.settings.first_leg_execution_policy('close') == before.first_leg_execution_policy('close')
        assert runtime.settings.hedge_execution_policy('close') == before.hedge_execution_policy('close')
        assert current['parameters']['opening_policies']['current'] == dict(
            entropy=dict(rooms_bps=['0'] * 3, hard_cap_bps='0'),
            lighter=dict(rooms_bps=['2'] * 3, hard_cap_bps='2'))
        assert current['config_hash'] == parameters['saved']['config_hash'] == owner.binding['config_hash']
        assert current['config_hash'] == canonical_config_hash(runtime.settings, pair_identity=runtime.pair_id)


def test_real_runtime_five_field_save_preserves_active_opening_policy_and_verifies_new_hash(integration_owner):
    runtime, _, _, journal, owner = integration_owner
    base = runtime._parameters_base_settings

    with api_client(owner) as client:
        headers = session(client)
        fixed = save(client, headers, opening_values('2', '7'))
        command(client, headers, 'start', fixed['saved'])
        first = state(client)
        command(client, headers, 'stop')
        runtime._update_operator_progress()
        assert (state(client))['intent'] == 'STOPPED'
        legacy = {**VALUES, 'entry_band_bps': '6'}
        pending = save(client, headers, legacy, revision=1)
        assert owner.frames[-1]['values'] == legacy
        expected = {**opening_values('2', '7'), 'entry_band_bps': '6'}
        assert pending['saved']['values'] == expected
        assert pending['current']['config_hash'] == first['config_hash'] == owner.binding['config_hash']
        assert pending['saved']['config_hash'] == canonical_config_hash(
            apply_parameters(base, expected), pair_identity=runtime.pair_id)
        assert pending['saved']['config_hash'] != first['config_hash']
        command(client, headers, 'start', pending['saved'])
        active = state(client)
        assert active['parameters']['current'] == pending['saved']
        assert active['parameters']['opening_policies']['current'] == first['parameters']['opening_policies']['current']
        assert runtime.settings.entry_band_bps == D('6')
        assert active['config_hash'] == owner.binding['config_hash'] == canonical_config_hash(
            runtime.settings, pair_identity=runtime.pair_id)
    assert journal.operator_parameters(runtime._parameters_scope)['current']['revision'] == 2


def test_real_runtime_parameter_save_preserves_loss_lock_and_blocks_start(integration_owner):
    runtime, _, _, journal, owner = integration_owner
    journal.close_entries('batch', 'SINGLE_CYCLE_REVIEW_LOSS')
    before = journal.batch_state('batch')
    initial_hash = runtime.operator_control.binding['config_hash']

    with api_client(owner) as client:
        headers = session(client)
        saved = save(client, headers, opening_values('2', '7'))
        response = client.post(f'{PREFIX}/preview', headers=headers,
            json=dict(source='basis', action='start',
                parameters_revision=saved['saved']['revision'],
                parameters_hash=saved['saved']['config_hash']))
        assert response.status_code == 409, response.text
        assert response.json()['detail']['code'] == 'risk_locked'
        current = state(client)
        assert current['intent'] == 'STOPPED'
        assert current['risk_locks'] == ['SINGLE_CYCLE_REVIEW_LOSS']
        assert current['parameters']['current']['revision'] == 0
        assert current['parameters']['saved'] == saved['saved']
    assert journal.batch_state('batch') == before
    assert runtime.operator_control.binding['config_hash'] == owner.binding['config_hash'] == initial_hash
    assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == 0
