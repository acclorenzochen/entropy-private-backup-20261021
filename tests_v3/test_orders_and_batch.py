import asyncio
from decimal import Decimal as D
import sqlite3
import time
from types import SimpleNamespace
import pytest
from entropy_arb.v3.core import Side
from entropy_arb.v3.journal import Intent,Journal,PendingOrder,Result
from entropy_arb.v3.execution import (IntentExecutor,PairEvidence,LiveIntegrationRequired,
                                     EvidenceRequired,hedge_units,residual_action,safe_to_reopen)
from entropy_arb.v3.demo import run_demo


def intent(ref='1',venue='entropy'):
    return Intent(ref,'c',venue,Side.BUY,85,D(100))


@pytest.fixture
def journal(tmp_path):
    j=Journal(tmp_path/'j.db',identity='pair-account')
    yield j
    j.close()


def test_terminal_zero_fill_unlocks_next_attempt(journal):
    journal.prepare(intent())
    journal.mark_submitting('1')
    journal.record('1',Result(True,0))
    journal.prepare(intent('2'))


def test_pending_blocks_duplicate(journal):
    journal.prepare(intent());journal.mark_submitting('1')
    journal.record('1',Result(False,0))
    with pytest.raises(PendingOrder): journal.prepare(intent('2'))


def test_cumulative_fill_updates_idempotent(journal):
    journal.prepare(intent()); journal.mark_submitting('1')
    journal.record('1',Result(False,40,D(99)))
    journal.record('1',Result(False,40,D(99)))
    journal.record('1',Result(True,85,D(100),D('.001')))
    journal.record('1',Result(True,85,D(100),D('.001')))
    assert journal.order('1')['filled_units']==85
    assert journal.order('1')['fee_usd']=='.001' or D(journal.order('1')['fee_usd'])==D('.001')
    with pytest.raises(ValueError): journal.record('1',Result(True,84,D(100)))


def test_missing_fee_remains_unknown(journal):
    journal.prepare(intent());journal.record('1',Result(True,85,D(100)))
    assert journal.order('1')['fee_usd'] is None


def test_crash_before_send_provably_unsent(journal):
    journal.prepare(intent());journal.cancel_unsent_prepared('1')
    assert journal.order('1')['filled_units']==0 and not journal.pending()


def test_crash_after_submit_may_not_be_cleared_without_exchange_evidence(journal):
    journal.prepare(intent());journal.mark_submitting('1')
    with pytest.raises(PendingOrder): journal.cancel_unsent_prepared('1')


def test_restart_preserves_unknown_and_batch(tmp_path):
    path=tmp_path/'r.db'
    j=Journal(path,identity='pair-account')
    j.ensure_batch('b',max_position_cycles=20,loss_budget_usd=D(1))
    j.prepare(intent());j.mark_submitting('1');j.close()
    j=Journal(path,identity='pair-account')
    try:
        assert j.pending()[0]['state']=='SUBMITTING'
        with pytest.raises(PendingOrder): j.prepare(intent('2'))
        with pytest.raises(ValueError): j.ensure_batch('b',max_position_cycles=20,loss_budget_usd=D(2))
    finally: j.close()


def test_wrong_account_cannot_reuse_journal(tmp_path):
    path=tmp_path/'r.db';j=Journal(path,identity='old');j.close()
    with pytest.raises(ValueError): Journal(path,identity='new')


def test_second_writer_is_rejected(tmp_path):
    p=tmp_path/'r.db';j=Journal(p,identity='x')
    try:
        with pytest.raises(RuntimeError): Journal(p,identity='x')
    finally: j.close()


def test_loss_budget_counts_recovery_and_never_refills(journal):
    j=journal;j.ensure_batch('b',max_position_cycles=20,loss_budget_usd=D('.02'))
    for cid,pnl,kind in [('a','-.01','repair'),('b','1','clean'),('c','-.011','unwound')]:
        j.begin_cycle('b',cid)
        j.finish_cycle(cid,kind=kind,net_pnl_usd=D(pnl),positions_verified_flat=True,all_orders_terminal=True)
    state=j.batch_state('b')
    assert state['pnl_usd']>0 and state['loss_used_usd']==D('.021') and not state['can_open']
    assert state['block_reason'] == 'REALIZED_LOSS_BUDGET'
    assert D(state['block_value']) == D('.021')
    assert D(state['block_threshold']) == D('.02')


def test_strategy_open_loss_latches_reason_and_survives_restart(tmp_path):
    path = tmp_path / 'batch.sqlite'
    j = Journal(path, identity='x')
    j.ensure_batch('b', max_position_cycles=20, loss_budget_usd=D('3'))
    assert not j.observe_open_risk(
        'b', active_cycle_net_pnl_usd=D('-3.1'), cycle_id='active')
    first = j.batch_state('b')
    assert first['block_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
    assert first['block_value'] == '-3.1'
    assert first['block_threshold'] == '-3'
    assert first['block_cycle_id'] == 'active'
    j.close()
    j = Journal(path, identity='x')
    assert j.batch_state('b')['block_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
    j.close()


def test_historical_account_drawdown_latch_is_preserved(tmp_path):
    path = tmp_path / 'batch.sqlite'
    j = Journal(path, identity='x')
    j.ensure_batch('b', max_position_cycles=20, loss_budget_usd=D('3'))
    j.close_entries('b', 'ACCOUNT_DRAWDOWN', value=D('47.8'), threshold=D('3'))
    j.close()
    j = Journal(path, identity='x')
    assert j.batch_state('b')['block_reason'] == 'ACCOUNT_DRAWDOWN'
    j.close()


def test_max_cycles_is_dynamic_block_not_safety_latch(journal):
    journal.ensure_batch('b', max_position_cycles=1, loss_budget_usd=D('3'))
    journal.begin_cycle('b', 'one')
    journal.finish_cycle(
        'one', kind='clean', net_pnl_usd=D('0'),
        positions_verified_flat=True, all_orders_terminal=True)
    state = journal.batch_state('b')
    assert state['block_reason'] == 'MAX_POSITION_CYCLES'
    assert state['block_value'] == state['block_threshold'] == 1
    assert not state['closed_for_entries']


def test_reopen_requires_fresh_authoritative_flat_and_no_pending(journal):
    journal.ensure_batch('b', max_position_cycles=20, loss_budget_usd=D('3'))
    journal.begin_cycle('b', 'active')
    journal.close_entries('b', 'MANUAL_STOP')
    def evidence(**overrides):
        values = dict(fully_authoritative=True, entropy_units=0,
                      hedge_units=0, observed_ts=time.time())
        values.update(overrides)
        return SimpleNamespace(**values)
    with pytest.raises(PendingOrder):
        journal.reopen_entries('b', evidence=evidence(entropy_units=1))
    with pytest.raises(PendingOrder):
        journal.reopen_entries('b', evidence=evidence(fully_authoritative=False))
    with pytest.raises(PendingOrder):
        journal.reopen_entries('b', evidence=evidence(observed_ts=time.time()-30))
    with pytest.raises(PendingOrder):
        journal.reopen_entries('b', evidence=evidence())
    journal.finish_cycle('active', kind='clean', net_pnl_usd=D('0'),
                         positions_verified_flat=True, all_orders_terminal=True)
    journal.prepare(intent())
    with pytest.raises(PendingOrder):
        journal.reopen_entries('b', evidence=evidence())
    journal.cancel_unsent_prepared('1')
    journal.reopen_entries('b', evidence=evidence())
    state = journal.batch_state('b')
    assert state['can_open'] and state['close_reason'] is None
    assert state['close_ts'] is None


def test_reopen_cannot_bypass_cycle_cap(journal):
    journal.ensure_batch('b', max_position_cycles=1, loss_budget_usd=D('3'))
    journal.begin_cycle('b', 'one')
    journal.finish_cycle('one', kind='clean', net_pnl_usd=D('0'),
                         positions_verified_flat=True, all_orders_terminal=True)
    evidence = SimpleNamespace(
        fully_authoritative=True, entropy_units=0, hedge_units=0,
        observed_ts=time.time())
    with pytest.raises(PendingOrder, match='MAX_POSITION_CYCLES'):
        journal.reopen_entries('b', evidence=evidence)


def test_legacy_latch_is_preserved_without_inventing_reason(tmp_path):
    path = tmp_path / 'old.sqlite'
    db = sqlite3.connect(path)
    db.executescript('''
        CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        INSERT INTO metadata VALUES ('version','1'),('identity','pair-account');
        CREATE TABLE batches(batch_id TEXT PRIMARY KEY,
            max_position_cycles INTEGER NOT NULL, loss_budget TEXT NOT NULL,
            closed_for_entries INTEGER NOT NULL DEFAULT 0);
        INSERT INTO batches VALUES ('b',20,'3',1);
    ''')
    db.close()
    journal = Journal(path, identity='pair-account')
    state = journal.batch_state('b')
    assert state['closed_for_entries']
    assert state['block_reason'] == 'LEGACY_UNATTRIBUTED'
    assert state['close_reason'] is None
    journal.close()


def test_zero_fill_does_not_consume_position_sample_target(journal):
    j=journal;j.ensure_batch('b',max_position_cycles=1,loss_budget_usd=D(1))
    for i in range(4):
        j.begin_cycle('b',str(i))
        j.finish_cycle(str(i),kind='zero_fill',net_pnl_usd=D(0),positions_verified_flat=True,all_orders_terminal=True)
    assert j.batch_state('b')['can_open'] and j.batch_state('b')['position_cycles']==0


def test_unknown_accounting_does_not_create_fake_positive_ev(journal):
    j=journal;j.ensure_batch('b',max_position_cycles=20,loss_budget_usd=D(1));j.begin_cycle('b','x')
    j.finish_cycle('x',kind='repair',net_pnl_usd=None,positions_verified_flat=True,all_orders_terminal=True)
    assert not j.batch_state('b')['can_open']
    # A conservative bound is reportable, but production entry remains blocked
    # until exchange fees/funding have been reconciled as final.
    j.finish_cycle('x',kind='repair',net_pnl_usd=D('-.1'),positions_verified_flat=True,
                   all_orders_terminal=True,accounting_final=False,conservative_pnl_bound=True)
    assert not j.batch_state('b')['can_open']
    assert j.db.execute('SELECT accounting_final FROM cycles').fetchone()[0]==0


def test_batch_flat_confirmation_never_uses_net_zero(journal):
    j=journal;j.ensure_batch('b',max_position_cycles=20,loss_budget_usd=D(1));j.begin_cycle('b','x')
    with pytest.raises(PendingOrder):
        j.finish_cycle('x',kind='clean',net_pnl_usd=D(1),positions_verified_flat=False,all_orders_terminal=True)


@pytest.mark.parametrize('e,h,terminal,expected',[(85,-85,True,'HEDGED'),(0,0,False,'RESOLVE_ORDERS'),
                                            (0,0,True,'FLAT'),(85,-40,True,'COMPLETE_OR_UNWIND_EXCESS')])
def test_position_evidence_classification(e,h,terminal,expected):
    evidence=PairEvidence(e,h,True,terminal,True)
    assert residual_action(evidence,closing=False)[0]==expected


def test_flat_after_fill_needs_no_signal_reset():
    evidence=PairEvidence(0,0,True,True,True)
    assert safe_to_reopen(evidence,active_execution=False,unresolved_journal_orders=False)
    assert not safe_to_reopen(evidence,active_execution=False,unresolved_journal_orders=True)


def test_partial_first_fill_hedges_only_confirmed_quantity():
    assert hedge_units(Result(True,40,D(100)))==40
    assert hedge_units(Result(True,85,D(100)),40)==45
    with pytest.raises(EvidenceRequired): hedge_units(Result(False,40,D(100)))


def test_send_is_after_durable_submitting(journal):
    class Fake:
        is_live=False
        async def submit(self,i):
            assert journal.order(i.order_ref)['state']=='SUBMITTING'
            return Result(True,85,D(100),D(0))
    asyncio.run(IntentExecutor(journal,Fake()).send(intent()))


def test_checkpoint_failure_sends_nothing(journal,monkeypatch):
    class Fake:
        is_live=False
        calls=0
        async def submit(self,i): self.calls+=1;return Result(True,0)
    fake=Fake()
    def fail(i): raise sqlite3.OperationalError('busy')
    monkeypatch.setattr(journal,'prepare',fail)
    with pytest.raises(sqlite3.OperationalError): asyncio.run(IntentExecutor(journal,fake).send(intent()))
    assert fake.calls==0


def test_transport_timeout_stays_unknown(journal):
    class Fake:
        is_live=False
        async def submit(self,i): raise TimeoutError()
    r=asyncio.run(IntentExecutor(journal,Fake()).send(intent()))
    assert not r.terminal and journal.pending()


def test_real_broker_cannot_be_accidentally_armed(journal):
    class Live: is_live=True
    with pytest.raises(LiveIntegrationRequired): IntentExecutor(journal,Live())


def test_two_full_cycles_actual_mock_order_path(tmp_path):
    output=asyncio.run(run_demo(tmp_path/'demo.db'))
    assert len(output['cycles'])==2 and output['orders']==8
    assert output['positions']=={'entropy':0,'hedge':0} and output['live_orders_sent']==0
