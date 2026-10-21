"""Offline durable configuration save/activation and strict safety boundaries."""
import asyncio
import copy
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal as D
import sqlite3
import time
from types import SimpleNamespace
import uuid

import pytest

from entropy_arb.v3.core import Direction, Side, plan_open
from entropy_arb.v3.journal import Intent
from entropy_arb.v3.live_host import canonical_config_hash
from entropy_arb.v3.operator_control import ControlRefusal
from entropy_arb.v3.parameter_config import parameter_limits, validate_parameters
from entropy_arb.v3.runtime import choose_open_plan
from tests_v3.test_normal_exit import seed_pair
from tests_v3.test_runtime import setup_runtime
from tests_v3.test_operator_runtime import request, state


VALUES = dict(entry_band_bps='5', exit_min_gross_profit_bps='2.5',
    entropy_exit_room_bps='12', lighter_exit_room_bps='12', max_notional_usd='200')


def make_owner(tmp_path, *, base_hash='hash'):
    runtime, e, h, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, entry_band_bps=D('5'),
        max_notional_usd=D('200'), single_cycle_review_loss_usd=None,
        open_first_rooms_bps=(D('3'),) * 3, open_first_hard_cap_bps=D('10'),
        close_first_rooms_bps=(D('12'),) * 3, close_first_hard_cap_bps=D('12'),
        close_hedge_rooms_bps=(D('12'),) * 3, close_hedge_hard_cap_bps=D('12'))
    runtime.authorization = replace(runtime.authorization, max_notional_usd=D('200'))
    runtime._parameters_base_settings = runtime.settings
    if base_hash is None:
        base_hash = canonical_config_hash(runtime.settings, pair_identity=runtime.pair_id)
    runtime._parameters_base_config_hash = base_hash
    runtime.status.base['config_hash'] = base_hash
    runtime.cfg = SimpleNamespace(entropy=SimpleNamespace(cap_usd=250),
                                  hedge=SimpleNamespace(cap_usd=210))
    asyncio.run(runtime.initialize(apply_leverage=False))
    runtime.enable_operator_control(dict(account_binding_hash='a' * 64,
        pair_identity='pair', deployment='test', batch_id='batch', config_hash=base_hash))
    return runtime, e, h, journal


@pytest.fixture
def owner(tmp_path):
    value = make_owner(tmp_path)
    yield value
    if value[3].db:
        value[3].close()


def save(runtime, values=None, *, revision=0, key=None):
    return runtime._operator_save_parameters(revision, VALUES if values is None else values,
                                           key or str(uuid.uuid4()))


def start(runtime, *, record=None, key=None, fingerprint='parameters-start'):
    record = record or state(runtime)['parameters']['saved'] or state(runtime)['parameters']['current']
    return runtime._operator_commit(action='start',
        expected_revision=state(runtime)['intent_revision'],
        idempotency_key=key or str(uuid.uuid4()), request_fingerprint=fingerprint,
        parameters_revision=record['revision'], parameters_hash=record['config_hash'])


def test_new_owner_is_stopped_and_defaults_have_no_trade_side_effects(owner):
    runtime, e, h, journal = owner
    current = state(runtime)
    assert current['intent'] == 'STOPPED' and not runtime._operator_running()
    assert current['parameters']['current']['values'] == {**VALUES,
        'entropy_open_room_bps': None, 'lighter_open_room_bps': None}
    assert current['parameters']['current']['revision'] == 0
    assert current['parameters']['saved'] is None
    assert current['parameters']['base_config_hash'] == 'hash'
    assert current['parameters']['can_save']
    assert e.sent == h.sent == []
    assert journal.active_live_cycle() is None and not journal.pending()


@pytest.mark.parametrize('pending', [False, True])
def test_first_configuration_cannot_replace_policy_while_existing_position_or_order_exists(tmp_path, pending):
    runtime, e, h, journal = setup_runtime(tmp_path)
    try:
        asyncio.run(runtime.initialize(apply_leverage=False))
        if pending:
            journal.begin_cycle('batch', 'untracked')
            journal.prepare(Intent('pending', 'untracked', 'entropy', Side.SELL, 1, D('100'), True))
        else:
            seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
        with pytest.raises(ValueError, match='initialization_requires_flat'):
            runtime.enable_operator_control(dict(account_binding_hash='a' * 64,
                pair_identity='pair', deployment='test', batch_id='batch', config_hash='hash'))
        assert journal.db.execute("SELECT COUNT(*) FROM metadata WHERE key LIKE 'operator_parameters:%'").fetchone()[0] == 0
        assert e.sent == h.sent == []
    finally:
        journal.close()


def test_save_only_changes_durable_pending_values_and_is_idempotent(owner):
    runtime, e, h, journal = owner
    before = runtime.settings
    binding = dict(runtime.operator_control.binding)
    status_hash = runtime.status.base['config_hash']
    key = str(uuid.uuid4())
    values = {**VALUES, 'entry_band_bps': '6', 'max_notional_usd': '150'}
    first = save(runtime, values, key=key)
    second = save(runtime, {**values, 'entry_band_bps': '6.0'}, key=key)
    assert first['revision'] == second['revision'] == 1
    assert runtime.settings is before and runtime.reference.cfg is before
    assert runtime.status.base['config_hash'] == status_hash
    assert runtime.operator_control.binding == binding
    assert state(runtime)['intent'] == 'STOPPED'
    assert journal.operator_parameters(runtime._parameters_scope)['saved']['values'] == {**values,
        'entropy_open_room_bps': None, 'lighter_open_room_bps': None}
    assert runtime._operator_parameter_save(key)['revision'] == 1
    assert runtime._operator_parameter_save(str(uuid.uuid4())) is None
    assert e.sent == h.sent == [] and not journal.pending()
    with pytest.raises(ControlRefusal, match='idempotency'):
        save(runtime, {**values, 'entry_band_bps': '7'}, key=key)


@pytest.mark.parametrize('field', sorted(VALUES))
@pytest.mark.parametrize('bad', ['NaN', 'Infinity', '-Infinity', '0', '-1',
    '1e-999999999', '0.000000001', '', ' 5', '5 ', 'garbage', None, 5, True])
def test_invalid_numeric_inputs_are_rejected_without_persistence(owner, field, bad):
    runtime, e, h, journal = owner
    before = journal.operator_parameters(runtime._parameters_scope)
    with pytest.raises(ControlRefusal):
        save(runtime, {**VALUES, field: bad})
    assert journal.operator_parameters(runtime._parameters_scope) == before
    assert state(runtime)['intent'] == 'STOPPED' and e.sent == h.sent == []


@pytest.mark.parametrize('field,too_large', [('entry_band_bps', '100.00000001'),
    ('exit_min_gross_profit_bps', '100.00000001'), ('entropy_exit_room_bps', '12.00000001'),
    ('lighter_exit_room_bps', '12.00000001'), ('max_notional_usd', '200.00000001')])
def test_upper_bounds_are_enforced(owner, field, too_large):
    with pytest.raises(ControlRefusal, match='out_of_bounds'):
        save(owner[0], {**VALUES, field: too_large})


def test_notional_ceiling_is_minimum_of_original_settings_authorization_and_both_venues(owner):
    runtime, _, _, _ = owner
    limits = parameter_limits(runtime._parameters_base_settings,
        replace(runtime.authorization, max_notional_usd=D('190')),
        SimpleNamespace(entropy=SimpleNamespace(cap_usd=195), hedge=SimpleNamespace(cap_usd=180)))
    assert limits['max_notional_usd'] == '180'
    assert validate_parameters({**VALUES, 'max_notional_usd': '180'}, limits)['max_notional_usd'] == '180'
    with pytest.raises(ValueError):
        validate_parameters(VALUES, limits)


@pytest.mark.parametrize('invalid', [{**VALUES, 'surprise': '1'}, {'entry_band_bps': '5'}])
def test_parameter_fields_are_exact(owner, invalid):
    with pytest.raises(ControlRefusal, match='fields'):
        save(owner[0], invalid)


def test_stale_revision_preserves_current_and_saved(owner):
    runtime, _, _, journal = owner
    save(runtime)
    before = journal.operator_parameters(runtime._parameters_scope)
    with pytest.raises(ControlRefusal, match='revision_changed'):
        save(runtime, {**VALUES, 'entry_band_bps': '6'})
    assert journal.operator_parameters(runtime._parameters_scope) == before
    assert save(runtime, {**VALUES, 'entry_band_bps': '6'}, revision=1)['revision'] == 2


def test_save_rejects_running_and_start_rejects_stale_or_missing_parameter_binding(owner):
    runtime, _, _, journal = owner
    save(runtime)
    with pytest.raises(ControlRefusal, match='explicit_start'):
        runtime._operator_commit(action='start', expected_revision=0,
            idempotency_key='old-start', request_fingerprint='old-start')
    record = state(runtime)['parameters']['saved']
    with pytest.raises(ControlRefusal, match='revision_changed'):
        start(runtime, record={**record, 'revision': 0})
    assert journal.operator_control_state(runtime._operator_scope)['intent'] == 'STOPPED'
    start(runtime)
    with pytest.raises(ControlRefusal, match='require_stopped'):
        save(runtime, revision=1)
    with pytest.raises(ControlRefusal, match='require_stopped'):
        start(runtime)


def test_save_rejects_active_position_and_stale_evidence(owner):
    runtime, e, h, journal = owner
    runtime._last_position_evidence = replace(runtime._last_position_evidence,
                                            observed_ts=time.time() - 16)
    with pytest.raises(ControlRefusal, match='position_refresh'):
        save(runtime)
    asyncio.run(runtime._audit_positions())
    seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
    asyncio.run(runtime._audit_positions())
    with pytest.raises(ControlRefusal, match='active_cycle'):
        save(runtime)


def test_save_rejects_pending_order_even_without_active_cycle(owner):
    runtime, _, _, journal = owner
    journal.begin_cycle('batch', 'untracked')
    journal.prepare(Intent('pending', 'untracked', 'entropy', Side.SELL, 1, D('100'), True))
    assert journal.active_live_cycle() is None
    with pytest.raises(ControlRefusal, match='pending_orders'):
        save(runtime)


def test_risk_locked_owner_can_save_but_cannot_start_or_clear_lock(owner):
    runtime, e, h, journal = owner
    journal.close_entries('batch', 'REALIZED_LOSS_BUDGET')
    before = journal.batch_state('batch')
    save(runtime)
    assert journal.batch_state('batch') == before
    with pytest.raises(ValueError, match='REALIZED_LOSS_BUDGET'):
        start(runtime)
    assert state(runtime)['intent'] == 'STOPPED'
    assert journal.batch_state('batch') == before and e.sent == h.sent == []


def test_start_atomic_activation_updates_settings_reference_status_binding_and_receipt(owner):
    runtime, e, h, journal = owner
    opening = (runtime.settings.first_leg_execution_policy('open'),
               runtime.settings.hedge_execution_policy('open'))
    save(runtime, {**VALUES, 'entry_band_bps': '8', 'exit_min_gross_profit_bps': '3',
        'entropy_exit_room_bps': '10', 'lighter_exit_room_bps': '11', 'max_notional_usd': '120'})
    record = state(runtime)['parameters']['saved']
    key = str(uuid.uuid4())
    result = start(runtime, key=key)
    assert result['state'] == 'completed' and result['receipt']['parameters'] == record
    assert state(runtime)['intent'] == 'RUNNING'
    assert runtime.settings.entry_band_bps == D('8')
    assert runtime.settings.exit_min_gross_profit_bps == D('3')
    assert runtime.settings.max_notional_usd == D('120')
    assert runtime.reference.cfg is runtime.settings
    assert runtime.settings.first_leg_execution_policy('close') == ((D('10'),) * 3, D('10'))
    assert runtime.settings.hedge_execution_policy('close') == ((D('11'),) * 3, D('11'))
    assert opening == (runtime.settings.first_leg_execution_policy('open'),
                       runtime.settings.hedge_execution_policy('open'))
    assert runtime.status.base['config_hash'] == record['config_hash']
    assert runtime.operator_control.binding['config_hash'] == record['config_hash']
    assert runtime.status.base['effective_settings']['entry_band_bps'] == '8'
    assert runtime.status.base['active_parameter_manifest'] == record
    assert state(runtime)['parameters']['saved'] is None
    assert journal.operator_parameters(runtime._parameters_scope)['current'] == record
    # Repeated accepted request remains the same operation after RUNNING.
    retry = start(runtime, record=record, key=key)
    assert retry['operation_id'] == result['operation_id']
    assert e.sent == h.sent == []


def test_wire_save_preview_confirm_and_lost_response_retry_keep_old_binding_idempotency(owner):
    runtime, e, h, journal = owner
    old_binding = dict(runtime.operator_control.binding)
    key = str(uuid.uuid4())
    saved = runtime.operator_control.handle(request(runtime, 'parameters_save',
        expected_revision=0, values={**VALUES, 'entry_band_bps': '7'}, idempotency_key=key))
    assert saved['ok'], saved
    record = saved['parameters']['saved']
    version = dict(parameters_revision=record['revision'], parameters_hash=record['config_hash'])
    preview = runtime.operator_control.handle(request(runtime, 'preview', action='start',
        intent_revision=0, expected_cycle_id=None, **version))
    assert preview['ok'], preview
    confirm = request(runtime, 'confirm', action='start', intent_revision=0,
        expected_cycle_id=None, receipt_id=preview['receipt_id'],
        idempotency_key=str(uuid.uuid4()), **version)
    first = runtime.operator_control.handle(confirm)
    assert first['ok'] and first['state']['intent'] == 'RUNNING', first
    assert runtime.operator_control.binding['config_hash'] != old_binding['config_hash']
    second = runtime.operator_control.handle(confirm)
    assert second['ok'] and second['operation']['operation_id'] == first['operation']['operation_id'], second
    lookup = request(runtime, 'parameters_lookup', idempotency_key=key)
    lookup.update(old_binding)
    recovered = runtime.operator_control.handle(lookup)
    assert recovered['ok'] and recovered['revision'] == 1, recovered
    assert recovered['parameters']['saved'] is None
    assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == 1
    assert e.sent == h.sent == []


@pytest.mark.parametrize('fail_stage', ['before_metadata', 'after_metadata', 'operation_insert'])
def test_persistence_failure_rolls_back_activation_and_running_without_memory_or_binding_change(owner, monkeypatch, fail_stage):
    runtime, e, h, journal = owner
    save(runtime, {**VALUES, 'entry_band_bps': '7'})
    doc_before = journal.operator_parameters(runtime._parameters_scope)
    settings_before = runtime.settings
    status_before = copy.deepcopy(runtime.status.base)
    binding_before = dict(runtime.operator_control.binding)
    original = journal._write_operator_parameters
    def fail(scope, document):
        if fail_stage == 'after_metadata':
            original(scope, document)
        if fail_stage != 'operation_insert':
            raise sqlite3.OperationalError('injected persistence failure')
        original(scope, document)
        # The next INSERT fails inside the same SQLite transaction.
        journal.db.execute('''CREATE TEMP TRIGGER reject_start BEFORE INSERT ON operator_operations
            BEGIN SELECT RAISE(ABORT,'injected operation failure'); END''')
    monkeypatch.setattr(journal, '_write_operator_parameters', fail)
    with pytest.raises(sqlite3.DatabaseError):
        start(runtime)
    assert journal.operator_parameters(runtime._parameters_scope) == doc_before
    assert journal.operator_control_state(runtime._operator_scope)['intent'] == 'STOPPED'
    assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == 0
    assert runtime.settings is settings_before and runtime.reference.cfg is settings_before
    assert runtime.status.base == status_before and runtime.operator_control.binding == binding_before
    assert not journal.db.in_transaction and e.sent == h.sent == []


@pytest.mark.parametrize('activate', [False, True])
def test_saved_and_current_parameters_survive_restart_but_require_new_manual_start(tmp_path, activate):
    runtime, e, h, journal = make_owner(tmp_path)
    save(runtime, {**VALUES, 'entry_band_bps': '7', 'max_notional_usd': '120'})
    if activate:
        start(runtime)
    expected = journal.operator_parameters(runtime._parameters_scope)
    journal.close()
    recovered, re, rh, reopened = make_owner(tmp_path)
    try:
        assert reopened.operator_parameters(recovered._parameters_scope) == expected
        assert state(recovered)['intent'] == ('STOPPING' if activate else 'STOPPED')
        assert recovered.settings.entry_band_bps == D('7' if activate else '5')
        assert recovered.settings.max_notional_usd == D('120' if activate else '200')
        assert recovered.reference.cfg is recovered.settings
        asyncio.run(recovered._maybe_open(recovered._market(), D('0')))
        assert re.sent == rh.sent == []
        recovered._update_operator_progress()
        assert state(recovered)['intent'] == 'STOPPED'
        if activate:
            start(recovered)
            recovered.restore_operator_parameters(dict(recovered.operator_control.binding))
            assert state(recovered)['intent'] == 'RUNNING'
        assert re.sent == rh.sent == []
    finally:
        reopened.close()


def test_restart_base_configuration_mismatch_fails_closed(owner):
    runtime, _, _, journal = owner
    before = journal.operator_parameters(runtime._parameters_scope)
    runtime._parameters_base_config_hash = 'changed-config-file-hash'
    with pytest.raises(ValueError, match='base_configuration_changed'):
        runtime.restore_operator_parameters(dict(runtime.operator_control.binding))
    assert journal.operator_parameters(runtime._parameters_scope) == before


@pytest.mark.parametrize('action', ['save', 'start'])
def test_sqlite_lock_wait_cannot_expire_flat_authority_then_commit(owner, monkeypatch, action):
    runtime, e, h, journal = owner
    if action == 'start':
        save(runtime)
    before = journal.operator_parameters(runtime._parameters_scope)
    original = journal.transaction
    delayed_now = time.time() + 16
    @contextmanager
    def delayed_transaction():
        with original():
            monkeypatch.setattr(time, 'time', lambda: delayed_now)
            yield
    monkeypatch.setattr(journal, 'transaction', delayed_transaction)
    with pytest.raises(ValueError, match='require_stopped_flat'):
        save(runtime) if action == 'save' else start(runtime)
    assert journal.operator_parameters(runtime._parameters_scope) == before
    assert journal.operator_control_state(runtime._operator_scope)['intent'] == 'STOPPED'
    assert not journal.db.in_transaction and e.sent == h.sent == []


def test_commit_failure_rolls_back_before_memory_or_binding_activation(owner, monkeypatch):
    runtime, e, h, journal = owner
    save(runtime, {**VALUES, 'entry_band_bps': '8'})
    before = journal.operator_parameters(runtime._parameters_scope)
    settings = runtime.settings
    binding = dict(runtime.operator_control.binding)
    class CommitFailure:
        def __init__(self, connection):
            self.connection = connection
        def __getattr__(self, name):
            return getattr(self.connection, name)
        def execute(self, sql, *args):
            if sql == 'COMMIT':
                raise sqlite3.OperationalError('injected COMMIT failure')
            return self.connection.execute(sql, *args)
    monkeypatch.setattr(journal, 'db', CommitFailure(journal.db))
    with pytest.raises(sqlite3.OperationalError, match='COMMIT'):
        start(runtime)
    assert journal.operator_parameters(runtime._parameters_scope) == before
    assert journal.operator_control_state(runtime._operator_scope)['intent'] == 'STOPPED'
    assert runtime.settings is settings and runtime.operator_control.binding == binding
    assert not journal.db.in_transaction and e.sent == h.sent == []


def test_save_failure_rolls_back_without_saved_revision_or_idempotency_receipt(owner, monkeypatch):
    runtime, e, h, journal = owner
    before = journal.operator_parameters(runtime._parameters_scope)
    original = journal._write_operator_parameters
    key = str(uuid.uuid4())
    def partial_failure(scope, document):
        original(scope, document)
        raise sqlite3.OperationalError('injected save failure')
    monkeypatch.setattr(journal, '_write_operator_parameters', partial_failure)
    with pytest.raises(sqlite3.OperationalError, match='save failure'):
        save(runtime, key=key)
    assert journal.operator_parameters(runtime._parameters_scope) == before
    assert journal.operator_parameter_save(runtime._parameters_scope, key) is None
    assert journal.operator_control_state(runtime._operator_scope)['intent'] == 'STOPPED'
    assert e.sent == h.sent == []


def test_five_bp_is_shared_by_coarse_filter_and_formal_admission(owner):
    runtime, e, h, _ = owner
    e.set_book('100.04', '100.04')
    h.set_book('100', '100')
    market = runtime._market()
    assert runtime.settings.entry_band_bps == runtime.reference.cfg.entry_band_bps == D('5')
    assert choose_open_plan(market, runtime.instrument, runtime.settings, D('0'),
        entropy_venue=e, hedge_venue=h) is None
    assert plan_open(market, runtime.instrument, runtime.settings, D('0'),
        Direction.SHORT_ENTROPY, 1000) is None
    e.set_book('100.06', '100.06')
    market = runtime._market()
    assert choose_open_plan(market, runtime.instrument, runtime.settings, D('0'),
        entropy_venue=e, hedge_venue=h) is not None
    assert plan_open(market, runtime.instrument, runtime.settings, D('0'),
        Direction.SHORT_ENTROPY, 1000) is not None
