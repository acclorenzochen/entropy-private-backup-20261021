"""Strict zero execution and disabled batch limits, candidate-local/fake-only."""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal as D
import hashlib
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace

import pytest

from entropy_arb.v3.core import Direction, Side
from entropy_arb.v3.journal import Intent, Journal, PendingOrder, Result
from tests_v3.test_normal_exit import seed_pair
from tests_v3.test_runtime import setup_runtime


def evidence(**changes):
    return SimpleNamespace(**{**dict(fully_authoritative=True, entropy_units=0,
        hedge_units=0, observed_ts=time.time()), **changes})


@pytest.fixture
def journal(tmp_path):
    value=Journal(tmp_path/'zero.sqlite',identity='fake')
    value.ensure_batch('batch',max_position_cycles=None,loss_budget_usd=None)
    yield value
    value.close()


def seed_zero(j, *, order=False, step=False):
    j.create_live_cycle(batch_id='batch',cycle_id='zero',direction='long_entropy',planned_units=1,
        reference_bps=D('0'),entry_basis_bps=D('0'),started_ts=time.time())
    intent=Intent('zero-order','zero','entropy',Side.BUY,1,D('100'))
    if order:
        j.prepare(intent)
        j.record(intent.order_ref,Result(True,0))
    if step:
        j.ensure_live_step(step_key='zero-step',cycle_id='zero',order_ref=intent.order_ref,
            venue='entropy',side=Side.BUY,requested_units=1,reduce_only=False)
    j.finish_cycle('zero',kind='zero_fill',net_pnl_usd=D('0'),positions_verified_flat=True,
                   all_orders_terminal=True,accounting_final=False,conservative_pnl_bound=True)
    j.update_live_cycle('zero',phase='DONE',entropy_units=0,hedge_units=0,
                       position_evidence_authoritative=True,last_authoritative_ts=time.time(),updated_ts=time.time())
    return intent


def finances(j):
    return {table:list(map(tuple,j.db.execute('SELECT * FROM '+table))) for table in
        ('cycles','live_cycles','orders','live_steps','cycle_economics','fee_fills','funding_settlements')}


@pytest.mark.parametrize('order,step',[(False,False),(True,False),(True,True)])
def test_proved_zero_can_reopen_without_faking_final_or_erasing_accounting(journal,order,step):
    seed_zero(journal,order=order,step=step)
    journal.close_entries('batch','MANUAL_STOP')
    before=finances(journal)
    assert journal.zero_execution_reentry_ready('zero')
    assert journal.batch_state('batch')['pending_accounting']==1
    journal.reopen_entries('batch',evidence=evidence())
    state=journal.batch_state('batch')
    assert state['can_open'] and state['blocking_pending_accounting']==0
    assert state['pending_accounting']==1 and state['position_cycles']==0
    assert state['max_position_cycles'] is None and state['loss_budget_usd'] is None
    assert finances(journal)==before
    journal.begin_cycle('batch','next')


@pytest.mark.parametrize('sql,parameters',[
    ("UPDATE live_cycles SET phase='OPEN' WHERE cycle_id='zero'",()),
    ("UPDATE live_cycles SET position_evidence_authoritative=0 WHERE cycle_id='zero'",()),
    ("UPDATE live_cycles SET entropy_units=1 WHERE cycle_id='zero'",()),
    ("UPDATE live_cycles SET cash_net_usd='0.01' WHERE cycle_id='zero'",()),
    ("UPDATE cycles SET result_kind='clean' WHERE cycle_id='zero'",()),
    ("UPDATE cycles SET complete_accounting=0 WHERE cycle_id='zero'",()),
    ("UPDATE cycles SET pnl_usd=NULL WHERE cycle_id='zero'",()),
    ("UPDATE cycles SET pnl_usd='-0.01' WHERE cycle_id='zero'",()),
    ("UPDATE cycles SET pending_known_cash_usd='0.01' WHERE cycle_id='zero'",()),
    ("UPDATE cycles SET pending_cost_estimate_usd='0.01' WHERE cycle_id='zero'",()),
    ("UPDATE cycles SET pnl_usd='invalid' WHERE cycle_id='zero'",()),
    ("UPDATE orders SET state='UNKNOWN' WHERE order_ref='zero-order'",()),
    ("UPDATE orders SET filled_units=1,avg_px='100' WHERE order_ref='zero-order'",()),
    ("UPDATE orders SET known_fee_usd='0.001' WHERE order_ref='zero-order'",()),
    ("UPDATE live_steps SET result_units=1 WHERE step_key='zero-step'",()),
    ("UPDATE live_steps SET cash_delta_usd='-0.01' WHERE step_key='zero-step'",()),
    ("UPDATE cycle_economics SET actual_fees_usd='0.001' WHERE cycle_id='zero'",()),
    ("UPDATE cycle_economics SET actual_funding_usd='0.001' WHERE cycle_id='zero'",()),
    ("UPDATE cycle_economics SET actual_entry_notional_usd='10' WHERE cycle_id='zero'",()),
    ("UPDATE cycle_economics SET first_fill_exchange_ts=1 WHERE cycle_id='zero'",()),
    ("UPDATE cycle_economics SET finalized_pnl_usd='0.01' WHERE cycle_id='zero'",()),
])
def test_any_contrary_execution_or_cash_evidence_stays_blocked(journal,sql,parameters):
    seed_zero(journal,order=True,step=True)
    journal.db.execute(sql,parameters)
    journal.close_entries('batch','MANUAL_STOP')
    assert not journal.zero_execution_reentry_ready('zero')
    with pytest.raises(PendingOrder):
        journal.reopen_entries('batch',evidence=evidence())
    assert journal.db.execute("SELECT closed_for_entries FROM batches WHERE batch_id='batch'").fetchone()[0]==1


def test_orphan_step_is_not_proof_of_no_submission(journal):
    seed_zero(journal,step=True)
    assert not journal.zero_execution_reentry_ready('zero')
    with pytest.raises(PendingOrder):
        journal.reopen_entries('batch',evidence=evidence())


@pytest.mark.parametrize('basis',[
    '{"state":"locked","matched_units":1,"entry_notional_usd":"100","canonical_orders":["filled"]}',
    '{"state":"unknown"}',
])
def test_locked_or_unknown_basis_without_orders_is_contrary_evidence(journal,basis):
    seed_zero(journal)
    journal.db.execute('UPDATE live_cycles SET basis_profit_basis_json=? WHERE cycle_id=?',(basis,'zero'))
    assert not journal.zero_execution_reentry_ready('zero')
    with pytest.raises(PendingOrder):journal.reopen_entries('batch',evidence=evidence())


def test_legacy_missing_basis_can_still_prove_zero_execution(journal):
    seed_zero(journal)
    journal.db.execute("UPDATE live_cycles SET basis_profit_basis_json=NULL WHERE cycle_id='zero'")
    assert journal.zero_execution_reentry_ready('zero')
    journal.reopen_entries('batch',evidence=evidence())


@pytest.mark.parametrize('wrong_cycle',[False,True])
def test_actual_fill_fee_row_by_cycle_or_order_reference_blocks(journal,wrong_cycle):
    seed_zero(journal,order=True)
    journal.db.execute('INSERT INTO fee_fills(venue,exchange_fill_key,cycle_id,order_ref,units,price,ts,fee_usd) '
        'VALUES(?,?,?,?,?,?,?,?)',('entropy','fill','other' if wrong_cycle else 'zero','zero-order',1,'100',time.time(),'0'))
    assert not journal.zero_execution_reentry_ready('zero')
    with pytest.raises(PendingOrder):journal.reopen_entries('batch',evidence=evidence())


@pytest.mark.parametrize('wrong_cycle',[False,True])
def test_execution_economics_cannot_hide_a_fill_by_wrong_cycle(journal,wrong_cycle):
    seed_zero(journal,order=True,step=True)
    journal.record_execution_benchmark(step_key='zero-step',cycle_id='zero',purpose='open_first',venue='entropy',
        attempt=0,side=Side.BUY,requested_units=1,room_bps=D('3'),hard_cap_bps=D('10'),submit_ts=time.time(),
        top_px=D('100'),marginal_px=D('100'),limit_px=D('100'),paired_executable_basis_bps=D('0'),
        reference_bps=D('0'),directional_dislocation_bps=D('0'),order_ref='zero-order',
        book_revision_entropy='e',book_revision_hedge='h')
    journal.db.execute('UPDATE execution_economics_steps SET cycle_id=?,order_ref=?,actual_fill_vwap=? WHERE step_key=?',
        ('other' if wrong_cycle else 'zero','zero-order','100','zero-step'))
    assert not journal.zero_execution_reentry_ready('zero')
    with pytest.raises(PendingOrder):journal.reopen_entries('batch',evidence=evidence())


def test_funding_records_even_zero_amount_do_not_become_zero_execution(journal):
    seed_zero(journal)
    journal.db.execute("INSERT INTO funding_cycles(cycle_id) VALUES('zero')")
    journal.db.execute("INSERT INTO funding_settlements(venue,event_id,cycle_id,ts,amount_usd,source) VALUES('entropy','fund','zero',1,'0','fake')")
    assert not journal.zero_execution_reentry_ready('zero')
    with pytest.raises(PendingOrder):journal.reopen_entries('batch',evidence=evidence())


def test_resolved_unknown_terminal_zero_qualifies_but_current_unknown_does_not(journal):
    intent=seed_zero(journal,order=True)
    journal.db.execute("UPDATE orders SET state='UNKNOWN',had_unknown=1 WHERE order_ref='zero-order'")
    assert not journal.zero_execution_reentry_ready('zero')
    journal.record(intent.order_ref,Result(True,0))
    assert journal.zero_execution_reentry_ready('zero')


def test_reopen_checks_authority_after_sqlite_wait_without_erasing_lock(journal,monkeypatch):
    seed_zero(journal)
    journal.close_entries('batch','MANUAL_STOP')
    captured=evidence()
    original=journal.transaction
    delayed=time.time()+11
    @contextmanager
    def delayed_transaction():
        with original():
            monkeypatch.setattr(time,'time',lambda:delayed)
            yield
    monkeypatch.setattr(journal,'transaction',delayed_transaction)
    with pytest.raises(PendingOrder,match='fresh authoritative'):
        journal.reopen_entries('batch',evidence=captured)
    assert journal.batch_state('batch')['closed_for_entries']


def test_disabled_limits_persist_and_keep_realized_statistics(tmp_path):
    path=tmp_path/'disabled.sqlite'
    j=Journal(path,identity='fake')
    try:
        j.ensure_batch('batch',max_position_cycles=None,loss_budget_usd=None)
        for cid,pnl in (('loss','-.25'),('profit','1')):
            j.begin_cycle('batch',cid)
            j.finish_cycle(cid,kind='clean',net_pnl_usd=D(pnl),positions_verified_flat=True,all_orders_terminal=True)
        state=j.batch_state('batch')
        assert state['position_cycles']==2 and state['loss_used_usd']==D('.25') and state['pnl_usd']==D('.75')
        assert state['can_open'] and state['max_position_cycles'] is state['loss_budget_usd'] is None
        assert tuple(j.db.execute('SELECT max_position_cycles,loss_budget FROM batches').fetchone())==('disabled','disabled')
    finally:j.close()
    j=Journal(path,identity='fake')
    try:
        j.ensure_batch('batch',max_position_cycles=None,loss_budget_usd=None)
        assert j.batch_state('batch')==state
        with pytest.raises(ValueError):j.ensure_batch('batch',max_position_cycles=999,loss_budget_usd=None)
    finally:j.close()


@pytest.mark.parametrize('cap,budget',[(1,None),(None,D('.1'))])
def test_disabling_one_limit_never_disables_the_other(tmp_path,cap,budget):
    j=Journal(tmp_path/'mixed.sqlite',identity='fake')
    try:
        j.ensure_batch('batch',max_position_cycles=cap,loss_budget_usd=budget)
        j.begin_cycle('batch','loss')
        j.finish_cycle('loss',kind='clean',net_pnl_usd=D('-.2'),positions_verified_flat=True,all_orders_terminal=True)
        assert not j.batch_state('batch')['can_open']
        with pytest.raises(PendingOrder):j.reopen_entries('batch',evidence=evidence())
    finally:j.close()


@pytest.mark.parametrize('bad',[0,-1,True,'disabled',1.5])
def test_only_explicit_none_or_positive_integer_cap_is_accepted(tmp_path,bad):
    j=Journal(tmp_path/'invalid.sqlite',identity='fake')
    try:
        with pytest.raises(ValueError):j.ensure_batch('batch',max_position_cycles=bad,loss_budget_usd=None)
    finally:j.close()


def test_runtime_publishes_actual_statistics_and_explicit_disabled_states(tmp_path):
    runtime,e,h,j=setup_runtime(tmp_path)
    runtime.authorization=replace(runtime.authorization,max_position_cycles=None,loss_budget_usd=None)
    j.ensure_batch('batch',max_position_cycles=None,loss_budget_usd=None)
    j.begin_cycle('batch','loss')
    j.finish_cycle('loss',kind='clean',net_pnl_usd=D('-.25'),positions_verified_flat=True,all_orders_terminal=True)
    try:
        asyncio.run(runtime.initialize(apply_leverage=False))
        assert runtime.status.base['position_cycles']==1 and runtime.status.base['loss_used_usd']=='0.25'
        assert runtime.status.base['max_position_cycles'] is runtime.status.base['loss_budget_usd'] is None
        assert runtime.status.base['max_position_cycles_disabled'] and runtime.status.base['loss_budget_disabled']
        runtime.enable_operator_control(dict(account_binding_hash='a'*64,pair_identity='pair',deployment='test',batch_id='batch',config_hash='hash'))
        runtime._operator_snapshot()
        assert runtime.status.base['loss_used_usd']=='0.25' and e.sent==h.sent==[]
        j.begin_cycle('batch','later-loss')
        j.finish_cycle('later-loss',kind='clean',net_pnl_usd=D('-.10'),positions_verified_flat=True,all_orders_terminal=True)
        runtime._operator_snapshot()
        assert runtime.status.base['position_cycles']==2 and runtime.status.base['loss_used_usd']=='0.35'
    finally:j.close()


@pytest.mark.parametrize('single_limit',[D('.10'),None])
def test_single_cycle_review_rule_is_independent_of_disabled_cumulative_caps(tmp_path,single_limit):
    runtime,e,h,j=setup_runtime(tmp_path)
    runtime.authorization=replace(runtime.authorization,max_position_cycles=None,loss_budget_usd=None)
    runtime.settings=replace(runtime.settings,single_cycle_review_loss_usd=single_limit)
    try:
        asyncio.run(runtime.initialize())
        cycle=seed_pair(runtime,e,h,j,Direction.LONG_ENTROPY)
        e.set_book('90','90.01')
        asyncio.run(runtime._observe_active_risk(cycle))
        state=j.batch_state('batch')
        assert state['closed_for_entries']==(single_limit is not None)
        if single_limit is not None:assert state['block_reason']=='SINGLE_CYCLE_REVIEW_LOSS'
        assert runtime.settings.single_cycle_review_loss_usd==single_limit
    finally:j.close()


def test_frozen_snapshot_classifies_115_without_mutation_or_hot_path_queries():
    path=Path(__file__).resolve().parents[2]/'journal-readonly-snapshot.sqlite3'
    before=hashlib.sha256(path.read_bytes()).hexdigest()
    db=sqlite3.connect('file:'+str(path)+'?mode=ro&immutable=1',uri=True)
    db.row_factory=sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    reader=Journal.__new__(Journal);reader.db=db
    try:
        rows=list(db.execute('SELECT cycle_id FROM cycles WHERE accounting_final=0'))
        assert len(rows)==115 and all(reader.zero_execution_reentry_ready(row['cycle_id']) for row in rows)
        calls=[];original=reader.zero_execution_reentry_ready
        def counted(cid):calls.append(cid);return original(cid)
        reader.zero_execution_reentry_ready=counted
        for batch, in db.execute('SELECT batch_id FROM batches'):
            reader.batch_state(batch)
        assert calls==[]
    finally:db.close()
    assert hashlib.sha256(path.read_bytes()).hexdigest()==before
