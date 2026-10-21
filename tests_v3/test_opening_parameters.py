"""Opening overrides and legacy configuration compatibility, fake-only."""
import asyncio
import copy
from dataclasses import replace
from decimal import Decimal as D
import json
import sqlite3

import pytest

from main_v3 import load_settings
from entropy_arb.v3.core import Settings, Side, price_limit
from entropy_arb.v3.live_host import canonical_config_hash
from entropy_arb.v3.operator_control import ControlRefusal
from entropy_arb.v3.parameter_config import (LEGACY_PARAMETER_FIELDS, OPENING_PARAMETER_FIELDS,
    apply_parameters, parameter_limits, validate_document, validate_parameters)
from entropy_arb.v3.runtime import choose_open_plan
from tests_v3.test_parameter_config import VALUES, make_owner, save, start
from tests_v3.test_operator_runtime import state
from tests_v3.test_runtime import setup_runtime, complete_background_audit
from tests_v3.test_ab_opening import setup_ab, open_once
from tests_v3.test_funding_hedge_runtime import carry


NULL_VALUES = {**VALUES, **dict.fromkeys(OPENING_PARAMETER_FIELDS)}


@pytest.fixture
def owner(tmp_path):
    value = make_owner(tmp_path)
    yield value
    value[3].close()


def test_null_preserves_default_ladders_and_known_base_and_legacy_overlay_hashes():
    from pathlib import Path
    settings = load_settings(Path(__file__).resolve().parents[1] / 'config.v3.edge-next.json')
    pair = 'default:hl:SNDK:lighter:SNDK'
    assert canonical_config_hash(settings, pair_identity=pair) == 'df81bc5ce633a688901b15e63cf862cc2048a565fa179db0af40779ad9f666db'
    null_overlay = apply_parameters(settings, NULL_VALUES)
    assert canonical_config_hash(null_overlay, pair_identity=pair) == canonical_config_hash(settings, pair_identity=pair)
    old_overlay = apply_parameters(settings, {**VALUES, 'entry_band_bps': '12'})
    assert canonical_config_hash(old_overlay, pair_identity=pair) == 'dc44f7d6c8c8ad0cf9cc7319fda27f6f66b8b8c06334ccedc07f40cb0dca9854'
    assert null_overlay.first_leg_execution_policy('open') == ((D('3'),)*3, D('10'))
    assert null_overlay.hedge_execution_policy('open') == ((D('4'),D('8'),D('12')),D('12'))
    # Only the newly introduced absent H fields are excluded from hashing.
    changed = replace(settings, close_first_rooms_bps=None, close_first_hard_cap_bps=None)
    assert canonical_config_hash(changed, pair_identity=pair) != canonical_config_hash(settings, pair_identity=pair)


def test_schema1_is_strictly_verified_then_projected_without_writing(owner):
    runtime, e, h, journal = owner
    original = copy.deepcopy(journal.operator_parameters(runtime._parameters_scope))
    assert original['schema_version'] == 1 and set(original['current']['values']) == LEGACY_PARAMETER_FIELDS
    runtime.restore_operator_parameters(dict(runtime.operator_control.binding))
    snap = state(runtime)['parameters']
    assert journal.operator_parameters(runtime._parameters_scope) == original
    assert snap['current']['values'] == NULL_VALUES and snap['current']['revision'] == 0
    assert snap['capabilities']['opening_concessions'] is True
    assert snap['opening_policies']['base'] == snap['opening_policies']['current']
    assert snap['opening_policies']['saved'] is None
    assert snap['limits']['entropy_open_room_bps'] == '10' and snap['limits']['lighter_open_room_bps'] == '12'
    limits = parameter_limits(runtime._parameters_base_settings, runtime.authorization, runtime.cfg)
    for bad in ({**original, 'limits': limits}, {**original, 'schema_version': 9}):
        with pytest.raises(ValueError):
            validate_document(bad, base_hash='hash', base_settings=runtime._parameters_base_settings,
                              pair_id='pair', limits=limits)
    damaged = copy.deepcopy(original)
    damaged['current']['config_hash'] = 'f'*64
    with pytest.raises(ValueError, match='persistence_invalid'):
        validate_document(damaged, base_hash='hash', base_settings=runtime._parameters_base_settings,
                          pair_id='pair', limits=limits)
    assert e.sent == h.sent == []


@pytest.mark.parametrize('value', ['0','3','10'])
def test_explicit_opening_changes_cap_and_all_original_attempts_only(owner, value):
    runtime, e, h, journal = owner
    before = runtime.settings
    save(runtime, {**NULL_VALUES,'entropy_open_room_bps':value,'lighter_open_room_bps':value})
    assert runtime.settings is before
    pending = state(runtime)['parameters']
    assert pending['opening_policies']['saved']['entropy'] == {'rooms_bps':[value]*3,'hard_cap_bps':value}
    assert journal.operator_parameters(runtime._parameters_scope)['schema_version'] == 2
    start(runtime)
    for policy in (runtime.settings.first_leg_execution_policy, runtime.settings.hedge_execution_policy):
        assert policy('open') == ((D(value),)*3,D(value))
        assert policy('close') == ((D('12'),)*3,D('12'))
    assert runtime.reference.cfg is runtime.settings and e.sent == h.sent == []
    assert canonical_config_hash(runtime.settings, pair_identity='pair') == pending['saved']['config_hash']


def test_missing_opening_preserves_saved_target_and_null_resets_to_base(owner):
    runtime, _, _, journal = owner
    save(runtime, {**NULL_VALUES,'entropy_open_room_bps':'2','lighter_open_room_bps':'1'})
    save(runtime, {**VALUES, 'entry_band_bps':'6'}, revision=1)
    record = state(runtime)['parameters']['saved']
    assert record['values']['entropy_open_room_bps'] == '2' and record['values']['lighter_open_room_bps'] == '1'
    start(runtime)
    # Missing values also preserve the activated current target.
    runtime._operator_commit(action='stop', expected_revision=state(runtime)['intent_revision'],
                             idempotency_key='stop-after-opening', request_fingerprint='stop-after-opening')
    runtime._update_operator_progress()
    save(runtime, VALUES, revision=2)
    assert state(runtime)['parameters']['saved']['values']['lighter_open_room_bps'] == '1'
    save(runtime, NULL_VALUES, revision=3)
    start(runtime)
    assert runtime.settings.first_leg_execution_policy('open') == ((D('3'),)*3,D('10'))
    assert runtime.settings.hedge_execution_policy('open') == ((D('4'),D('8'),D('12')),D('12'))
    assert journal.operator_parameters(runtime._parameters_scope)['current']['revision'] == 4


@pytest.mark.parametrize('field', sorted(OPENING_PARAMETER_FIELDS))
@pytest.mark.parametrize('bad', ['-1','NaN','sNaN','Infinity','-Infinity','1e-999999999',
    '0.000000001','', ' ', ' 1','1 ', 1, True, {}, '1'*65])
def test_bad_opening_values_cannot_persist_or_start(owner, field, bad):
    runtime, e, h, journal = owner
    before = journal.operator_parameters(runtime._parameters_scope)
    with pytest.raises(ControlRefusal):
        save(runtime, {**NULL_VALUES, field:bad})
    assert journal.operator_parameters(runtime._parameters_scope) == before
    assert state(runtime)['intent'] == 'STOPPED' and e.sent == h.sent == []


@pytest.mark.parametrize('field,too_large', [('entropy_open_room_bps','10.00000001'),('lighter_open_room_bps','12.00000001')])
def test_opening_cannot_expand_original_physical_cap(owner, field, too_large):
    with pytest.raises(ControlRefusal, match='out_of_bounds'):
        save(owner[0], {**NULL_VALUES,field:too_large})


def test_partial_six_fields_rejected(owner):
    with pytest.raises(ControlRefusal, match='fields'):
        save(owner[0], {**VALUES,'entropy_open_room_bps':None})


def test_signed_and_decimal_zero_share_canonical_request_identity(owner):
    runtime, _, _, _ = owner
    first = save(runtime,{**NULL_VALUES,'entropy_open_room_bps':'-0','lighter_open_room_bps':'0.000'},key='zero')
    second = save(runtime,{**NULL_VALUES,'entropy_open_room_bps':'0','lighter_open_room_bps':'0'},key='zero')
    assert first['revision'] == second['revision'] == 1
    assert first['parameters']['saved']['values']['entropy_open_room_bps'] == '0'


def test_original_attempt_count_and_scalar_cap_are_distinct_from_inherit(owner):
    runtime, _, _, _ = owner
    base = replace(runtime._parameters_base_settings, open_first_rooms_bps=(D('1'),D('3')),
                   open_hedge_rooms_bps=(D('1'),D('2'),D('4'),D('7')))
    explicit = apply_parameters(base, {**NULL_VALUES,'entropy_open_room_bps':'3','lighter_open_room_bps':'4'})
    assert explicit.first_leg_execution_policy('open') == ((D('3'),)*2,D('3'))
    assert explicit.hedge_execution_policy('open') == ((D('4'),)*4,D('4'))
    inherited = apply_parameters(base, NULL_VALUES)
    assert inherited.first_leg_execution_policy('open') == base.first_leg_execution_policy('open')
    assert inherited.hedge_execution_policy('open') == base.hedge_execution_policy('open')
    assert canonical_config_hash(explicit,pair_identity='pair') != canonical_config_hash(inherited,pair_identity='pair')


@pytest.mark.parametrize('side',list(Side))
def test_zero_cap_requires_an_executable_tick_and_depth_within_top(side):
    assert price_limit(D('100'),D('100'),side,D('0'),D('0'),D('.1')) == D('100')
    with pytest.raises(ValueError,match='physical hard cap'):
        price_limit(D('100'),D('100.1') if side is Side.BUY else D('99.9'),side,D('0'),D('0'),D('.1'))
    with pytest.raises(ValueError,match='physical hard cap'):
        price_limit(D('100.05'),D('100.05'),side,D('0'),D('0'),D('.1'))


@pytest.mark.parametrize('field', ['first_hard_cap_bps','hedge_hard_cap_bps','close_first_hard_cap_bps','close_hedge_hard_cap_bps'])
def test_zero_opening_exception_never_relaxes_general_or_close_cap(field):
    with pytest.raises(ValueError,match='positive'):
        replace(Settings(), **{field:D('0')})
    assert Settings.from_mapping({'open_hedge_rooms_bps':['0']*3,'open_hedge_hard_cap_bps':'0'}).hedge_execution_policy('open') == ((D('0'),)*3,D('0'))


def test_new_receipt_preserve_and_reset_modes_cannot_reuse_same_key(owner):
    runtime, _, _, journal = owner
    save(runtime, {**NULL_VALUES,'entropy_open_room_bps':'2','lighter_open_room_bps':'1'})
    saved = save(runtime, VALUES, revision=1, key='preserve-key')
    before = journal.operator_parameters(runtime._parameters_scope)
    with pytest.raises(ControlRefusal,match='payload_changed'):
        save(runtime, NULL_VALUES, revision=1, key='preserve-key')
    prior = journal.operator_parameter_save(runtime._parameters_scope,'preserve-key')
    with pytest.raises(ValueError,match='payload_changed'):
        journal.save_operator_parameters(runtime._parameters_scope,operator_scope=runtime._operator_scope,
            expected_revision=1,record=before['saved'], idempotency_key='preserve-key',
            request_fingerprint=prior['request_fingerprint'],confirmation_evidence=state(runtime),
            opening_fields_present=True)
    assert journal.operator_parameters(runtime._parameters_scope) == before
    assert runtime._operator_parameter_save('preserve-key')['revision'] == saved['revision']


def test_legacy_lost_response_retry_ignores_later_explicit_target_and_never_resets(owner):
    runtime, e, h, journal = owner
    original = save(runtime, VALUES, key='legacy-unknown')
    prior = journal.operator_parameter_save(runtime._parameters_scope,'legacy-unknown')
    receipt = {key:prior['receipt'][key] for key in ('revision','config_hash')}
    journal.db.execute('UPDATE operator_parameter_saves SET receipt_json=? WHERE idempotency_key=?',
                       (json.dumps(receipt),'legacy-unknown'))
    save(runtime,{**NULL_VALUES,'entropy_open_room_bps':'2','lighter_open_room_bps':'1'},revision=1)
    before = journal.operator_parameters(runtime._parameters_scope)
    for request in (VALUES,NULL_VALUES):
        retry = save(runtime,request,key='legacy-unknown')
        assert retry['revision'] == original['revision']
        assert journal.operator_parameters(runtime._parameters_scope) == before
    assert e.sent == h.sent == []


def test_schema2_migration_and_activation_rollback_leave_legacy_document_unchanged(owner,monkeypatch):
    runtime, e, h, journal = owner
    before = copy.deepcopy(journal.operator_parameters(runtime._parameters_scope))
    settings, binding = runtime.settings, dict(runtime.operator_control.binding)
    original = journal._write_operator_parameters
    def fail(scope, document):
        original(scope, document)
        raise sqlite3.OperationalError('migration commit rejected')
    monkeypatch.setattr(journal,'_write_operator_parameters',fail)
    with pytest.raises(sqlite3.OperationalError):
        start(runtime)
    assert journal.operator_parameters(runtime._parameters_scope) == before
    assert runtime.settings is settings and runtime.operator_control.binding == binding
    assert state(runtime)['intent'] == 'STOPPED' and not journal.db.in_transaction
    assert e.sent == h.sent == []


@pytest.mark.parametrize('activate',[False,True])
def test_explicit_parameters_survive_restart_without_dispatch(tmp_path,activate):
    runtime, e, h, journal = make_owner(tmp_path)
    save(runtime,{**NULL_VALUES,'entropy_open_room_bps':'0','lighter_open_room_bps':'2'})
    if activate:
        start(runtime)
    before=journal.operator_parameters(runtime._parameters_scope)
    journal.close()
    recovered,re,rh,reopened=make_owner(tmp_path)
    try:
        assert reopened.operator_parameters(recovered._parameters_scope) == before
        assert recovered.settings.first_leg_execution_policy('open')[1] == D('0' if activate else '10')
        assert state(recovered)['intent'] == ('STOPPING' if activate else 'STOPPED')
        assert re.sent == rh.sent == []
    finally:
        reopened.close()


def test_explicit_hedge_preflight_shrinks_known_depth_but_inherit_is_unchanged(tmp_path):
    runtime,e,h,journal=setup_runtime(tmp_path)
    try:
        asyncio.run(runtime.initialize())
        h.book.apply_hl([[{'px':'99.99','sz':'2'}],[{'px':'100','sz':'.11'},{'px':'100.10','sz':'2'}]])
        market=runtime._market()
        inherited=choose_open_plan(market,runtime.instrument,runtime.settings,D('0'),entropy_venue=e,hedge_venue=h)
        assert inherited is not None and inherited.units > 1100
        strict=replace(runtime.settings,open_hedge_rooms_bps=(D('0'),)*3,open_hedge_hard_cap_bps=D('0'))
        admitted=choose_open_plan(market,runtime.instrument,strict,D('0'),entropy_venue=e,hedge_venue=h)
        assert admitted is not None and admitted.units == 1100
        h.book.apply_hl([[{'px':'99.99','sz':'2'}],[{'px':'100','sz':'.01'},{'px':'100.10','sz':'2'}]])
        assert choose_open_plan(runtime._market(),runtime.instrument,strict,D('0'),entropy_venue=e,hedge_venue=h) is None
        assert e.sent == h.sent == []
    finally:
        journal.close()


@pytest.mark.parametrize('room',['0','2'])
def test_serial_open_retry_limits_use_fixed_overrides_and_close_stays_original(tmp_path,room):
    runtime,e,h,journal=setup_runtime(tmp_path)
    runtime.settings=apply_parameters(runtime.settings,{**NULL_VALUES,'max_notional_usd':'15',
        'entropy_open_room_bps':room,'lighter_open_room_bps':room})
    h.fill_fractions=[0,1]
    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(),D('0'))
        active=journal.active_live_cycle()
        assert active['phase'] == 'OPEN' and len(e.sent)==1 and len(h.sent)==2
        for s in journal.economics_steps(active['cycle_id']):
            assert D(s['room_bps'])==D(room) and D(s['hard_cap_bps'])==D(room)
        opening_refs=[o['order_ref'] for o in e.sent+h.sent]
        assert len(set(opening_refs))==3
        e.set_book('99.60','99.61')
        await runtime._resume_cycle(active)
        await complete_background_audit(runtime,accounting=True)
        close_steps=[s for s in journal.economics_steps(active['cycle_id']) if s['purpose'].startswith('close_pair')]
        assert close_steps and all(s['room_bps']=='12' and s['hard_cap_bps']=='12' for s in close_steps)
        assert all(o['reduce_only'] for o in e.sent[1:]+h.sent[2:])
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_parallel_open_helper_uses_overrides_without_changing_ab_authorization(tmp_path):
    runtime,e,h,journal=setup_ab(tmp_path)
    runtime.settings=replace(runtime.settings,open_first_rooms_bps=(D('0'),),open_first_hard_cap_bps=D('0'),
                             open_hedge_rooms_bps=(D('0'),),open_hedge_hard_cap_bps=D('0'))
    async def scenario():
        await runtime.initialize()
        cycle=await open_once(runtime)
        assert cycle['phase']=='OPEN' and len(e.sent)==len(h.sent)==1
        for s in journal.economics_steps(cycle['cycle_id']):
            assert s['room_bps']=='0' and s['hard_cap_bps']=='0' and s['execution_path']=='parallel'
        with pytest.raises(ValueError,match='unsupported_opening_ab'):
            runtime.enable_operator_control(dict(account_binding_hash='a'*64,pair_identity='pair',deployment='test',batch_id='batch',config_hash='hash'))
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_repair_completion_uses_explicit_opening_policy_after_zero_attempts(tmp_path):
    runtime,e,h,journal=setup_runtime(tmp_path)
    runtime.settings=replace(runtime.settings,open_first_rooms_bps=(D('0'),)*3,open_first_hard_cap_bps=D('0'),
                             open_hedge_rooms_bps=(D('2'),)*3,open_hedge_hard_cap_bps=D('2'))
    h.fill_fractions=[0,0,0,1]
    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(),D('0'))
        active=journal.active_live_cycle()
        assert active['phase']=='OPEN' and len(e.sent)==1 and len(h.sent)==4
        repairs=[s for s in journal.economics_steps(active['cycle_id']) if s['purpose']=='repair_complete_pair']
        assert len(repairs)==1 and repairs[0]['room_bps']=='2' and repairs[0]['hard_cap_bps']=='2'
        assert not h.sent[-1]['reduce_only']
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_funding_open_uses_override_and_risk_exit_retains_general_cap(carry):
    runtime,e,h,journal,clock,tick,open_pair=carry
    runtime.settings=replace(runtime.settings,open_first_rooms_bps=(D('0'),)*3,open_first_hard_cap_bps=D('0'),
                             open_hedge_rooms_bps=(D('0'),)*3,open_hedge_hard_cap_bps=D('0'))
    cycle=asyncio.run(open_pair())
    assert cycle['phase']=='OPEN' and len(e.sent)==len(h.sent)==1
    opened=journal.economics_steps(cycle['cycle_id'])
    assert all(s['room_bps']=='0' and s['hard_cap_bps']=='0' for s in opened)
    h.liquidation=D('101')
    tick(10003)
    asyncio.run(runtime._resume_cycle(cycle))
    assert h.position==e.position==0
    reduced=[s for s in journal.economics_steps(cycle['cycle_id']) if s['purpose'].startswith('funding_exit')]
    assert len(reduced)==2 and all(s['room_bps']=='4' and s['hard_cap_bps']=='12' for s in reduced)
    assert all(o['reduce_only'] for o in e.sent[1:]+h.sent[1:])


def test_pending_original_id_and_limit_survive_quote_change_with_override(tmp_path):
    runtime,e,h,journal=setup_runtime(tmp_path)
    runtime.settings=replace(runtime.settings,open_first_rooms_bps=(D('0'),)*3,open_first_hard_cap_bps=D('0'),
                             open_hedge_rooms_bps=(D('2'),)*3,open_hedge_hard_cap_bps=D('2'))
    e.unknown_once=True
    original=e.resolve_order
    async def unresolved(order_ref,timeout=None):
        return {'status':'timeout','filled_base':0,'avg_px':None,'unresolved':True,'order_ref':order_ref}
    e.resolve_order=unresolved
    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(),D('0'))
        pending=dict(journal.pending()[0])
        original_px=pending['limit_px']
        original_ref=pending['order_ref']
        e.set_book('99.8','99.81')
        async def resolved(order_ref,timeout=None):
            assert order_ref==original_ref
            assert journal.order(order_ref)['limit_px']==original_px
            return await original(order_ref,timeout)
        e.resolve_order=resolved
        await runtime._resolve_pending()
        assert len(e.sent)==1 and e.sent[0]['order_ref']==original_ref
        assert journal.order(original_ref)['limit_px']==original_px
        assert journal.order(original_ref)['state']=='TERMINAL'
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_prepare_wait_refresh_rejects_known_unhedgeable_strict_cap(tmp_path,monkeypatch):
    runtime,e,h,journal=setup_runtime(tmp_path)
    runtime.settings=replace(runtime.settings,open_hedge_rooms_bps=(D('0'),)*3,open_hedge_hard_cap_bps=D('0'))
    released=[]
    class Token:
        def release(self):
            released.append(True)
    async def prepare(*args,**kwargs):
        await asyncio.sleep(0)
        h.book.apply_hl([[{'px':'99.99','sz':'2'}],[{'px':'100','sz':'.01'},{'px':'100.02','sz':'2'}]])
        return Token()
    async def scenario():
        await runtime.initialize()
        monkeypatch.setattr(runtime.brokers['entropy'],'prepare_dispatch',prepare)
        await runtime._maybe_open(runtime._market(),D('0'))
        assert e.sent==h.sent==[] and released
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_first_retry_uses_shared_cap_preflight_without_new_submit(tmp_path):
    runtime,e,h,journal=setup_runtime(tmp_path)
    runtime.settings=replace(runtime.settings,open_hedge_rooms_bps=(D('0'),)*3,open_hedge_hard_cap_bps=D('0'))
    e.fill_fractions=[0,1]
    original=e.send_taker
    async def send(**kwargs):
        result=await original(**kwargs)
        h.book.apply_hl([[{'px':'99.99','sz':'2'}],[{'px':'100','sz':'.01'},{'px':'100.02','sz':'2'}]])
        return result
    e.send_taker=send
    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(),D('0'))
        assert len(e.sent)==1 and h.sent==[]
        assert e.sent[0]['qty']>0.01 and e.fill_fractions==[1]
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_ab_prepare_refresh_rejects_before_pair_admission_or_send(tmp_path,monkeypatch):
    runtime,e,h,journal=setup_ab(tmp_path)
    runtime.settings=replace(runtime.settings,open_hedge_rooms_bps=(D('0'),),open_hedge_hard_cap_bps=D('0'))
    async def scenario():
        await runtime.initialize()
        original=runtime.brokers['hedge'].prepare_dispatch
        async def prepare(*args,**kwargs):
            token=await original(*args,**kwargs)
            h.book.apply_hl([[{'px':'99.99','sz':'2'}],[{'px':'100','sz':'.01'},{'px':'100.02','sz':'2'}]])
            return token
        monkeypatch.setattr(runtime.brokers['hedge'],'prepare_dispatch',prepare)
        await open_once(runtime)
        assert e.sent==h.sent==[] and journal.pending()==[]
        assert journal.active_live_cycle() is None
        assert journal.opening_experiment_state('opening-ab-test')['attempts']==0
    try:
        asyncio.run(scenario())
    finally:
        journal.close()
