"""Offline operator receipts, intent revisions and monotonic whole-pair exit."""
from contextlib import closing
from decimal import Decimal as D
import socket
import sqlite3

import pytest

from entropy_arb.v3.core import Side
from entropy_arb.v3.journal import Intent, Journal, PendingOrder, Result


IDENTITY = 'offline-operator-control'
SCOPE = 'account:pair:deployment'


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('operator journal tests must remain offline')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


@pytest.fixture
def journal(tmp_path):
    with closing(Journal(tmp_path / 'operator.sqlite', identity=IDENTITY)) as journal:
        journal.ensure_batch('batch', max_position_cycles=100, loss_budget_usd=D('3'))
        journal.ensure_operator_control(SCOPE)
        yield journal


def cycle(journal, cycle_id='cycle'):
    journal.create_live_cycle(batch_id='batch', cycle_id=cycle_id,
        direction='long_entropy', planned_units=10, reference_bps=D('0'),
        entry_basis_bps=D('-10'), started_ts=1.0)


def command(journal, action, *, revision=None, key=None, target=None, now=10.0):
    revision = journal.operator_control_state(SCOPE)['intent_revision'] if revision is None else revision
    key = key or f'{action}:{revision}'
    return journal.commit_operator_command(SCOPE, action=action, expected_revision=revision,
        idempotency_key=key, request_fingerprint=f'{action}:{revision}:{target}',
        expected_cycle_id=target, now=now)


def order(journal, ref, venue, side, units=10, *, reduce_only=False,
          terminal=True, grid=D('.01'), received_ts=5.0):
    intent = Intent(ref, 'cycle', venue, side, units, D('100'), reduce_only)
    journal.prepare_dispatch(intent, step={'step_key': ref, 'quantity_step': grid})
    journal.record(ref, Result(terminal, units, D('100'), D('0')),
                   timing={'terminal_result_ts': received_ts})


def done_zero(journal, cycle_id='cycle', *, now=20.0):
    journal.update_live_cycle(cycle_id, entropy_units=0, hedge_units=0,
        position_evidence_authoritative=True, last_authoritative_ts=now, updated_ts=now)
    journal.finish_cycle(cycle_id, kind='zero_fill', net_pnl_usd=D('0'),
        positions_verified_flat=True, all_orders_terminal=True)
    journal.update_live_cycle(cycle_id, phase='DONE', updated_ts=now)


def test_initialization_is_idempotent_and_scopes_are_separate(journal):
    original = journal.operator_control_state(SCOPE)
    assert original['intent'] == 'RUNNING' and original['intent_revision'] == 0
    assert journal.ensure_operator_control(SCOPE, initial_intent='STOPPED') == original
    other = journal.ensure_operator_control('other', initial_intent='STOPPED')
    assert other['intent'] == 'STOPPED' and other['intent_revision'] == 0
    with pytest.raises(KeyError):
        journal.operator_control_state('missing')


def test_lost_response_retry_returns_same_receipt_after_cycle_and_revision_change(journal):
    cycle(journal)
    accepted = command(journal, 'close', target='cycle', key='close-once')
    assert accepted['state'] == 'accepted'
    assert accepted['intent'] == 'RUNNING' and accepted['intent_revision'] == 1
    assert accepted['receipt']['control_before']['intent_revision'] == 0
    assert accepted['receipt']['cycle_before']['phase'] == 'OPEN_FIRST'
    assert accepted['receipt']['cycle']['phase'] == 'BASIS_EXIT'
    done_zero(journal)
    cycle(journal, 'next')
    command(journal, 'stop')
    retried = command(journal, 'close', revision=0, target='cycle', key='close-once', now=99)
    assert retried == accepted
    assert journal.active_live_cycle()['cycle_id'] == 'next'
    assert journal.active_live_cycle()['basis_exit_requested_ts'] is None
    assert journal.operator_control_state(SCOPE)['intent'] == 'STOPPING'
    assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == 2


def test_receipt_and_progress_survive_journal_reopen(tmp_path):
    path = tmp_path / 'reopen.sqlite'
    with closing(Journal(path, identity=IDENTITY)) as journal:
        journal.ensure_batch('batch', max_position_cycles=100, loss_budget_usd=D('3'))
        journal.ensure_operator_control(SCOPE)
        cycle(journal)
        accepted = command(journal, 'close', key='stable-key', target='cycle')
        progress = journal.update_operator_operation(SCOPE, accepted['operation_id'],
            state='executing', evidence={'target': 'cycle', 'unknown_refs': ['same-id']}, now=12)
        assert progress['receipt'] == accepted['receipt']
        persisted_control = journal.operator_control_state(SCOPE)
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.ensure_operator_control(SCOPE, initial_intent='STOPPED') == persisted_control
        assert command(journal, 'close', revision=0, target='cycle', key='stable-key') == progress
        assert journal.operator_operation(SCOPE, progress['operation_id']) == progress
        assert journal.operator_operation(SCOPE, idempotency_key='stable-key') == progress
        assert journal.operator_operation('other', progress['operation_id']) is None
        assert progress['idempotency_key'] == 'stable-key'
        assert progress['request_fingerprint'] == 'close:0:cycle'
        assert journal.active_live_cycle()['basis_exit_requested_ts'] == 10


def test_same_key_changed_fingerprint_rejects_before_any_fresh_commit(journal):
    accepted = command(journal, 'stop', key='stable')
    state = journal.operator_control_state(SCOPE)
    with pytest.raises(ValueError, match='payload changed'):
        journal.commit_operator_command(SCOPE, action='start', expected_revision=1,
            idempotency_key='stable', request_fingerprint='different', now=11)
    assert journal.operator_control_state(SCOPE) == state
    assert journal.operator_operation(SCOPE, accepted['operation_id']) == accepted


@pytest.mark.parametrize('action', ['start', 'stop', 'close'])
def test_fresh_revision_conflict_has_no_receipt_or_exit_latch(journal, action):
    cycle(journal)
    command(journal, 'stop')
    state = journal.operator_control_state(SCOPE)
    with pytest.raises(ValueError, match='revision changed'):
        command(journal, action, revision=0, key='stale', target='cycle' if action == 'close' else None)
    assert journal.operator_control_state(SCOPE) == state
    assert journal.operator_operation(SCOPE, idempotency_key='stale') is None
    assert journal.active_live_cycle()['basis_exit_requested_ts'] is None


@pytest.mark.parametrize('first', ['stop', 'close'])
def test_stop_close_order_never_restores_running(journal, first):
    cycle(journal)
    if first == 'stop':
        command(journal, 'stop')
        closing_op = command(journal, 'close', target='cycle')
        assert closing_op['intent'] == 'STOPPING'
    else:
        closing_op = command(journal, 'close', target='cycle')
        command(journal, 'stop')
    assert journal.operator_control_state(SCOPE)['intent_revision'] == 2
    assert journal.advance_operator_stop(SCOPE, authoritative_flat=True, now=15)['intent'] == 'STOPPING'
    done_zero(journal)
    stopped = journal.advance_operator_stop(SCOPE, authoritative_flat=True, now=21)
    assert stopped == {'intent': 'STOPPED', 'intent_revision': 3, 'updated_ts': 21}
    journal.update_operator_operation(SCOPE, closing_op['operation_id'], state='completed',
                                      evidence={'flat': True}, now=22)
    assert journal.operator_control_state(SCOPE) == stopped
    assert journal.advance_operator_stop(SCOPE, authoritative_flat=True, now=23) == stopped


def test_close_exact_target_rejects_missing_done_or_other_cycle(journal):
    cycle(journal)
    with pytest.raises(PendingOrder, match='no longer active'):
        command(journal, 'close', target='missing')
    assert journal.operator_control_state(SCOPE)['intent_revision'] == 0
    command(journal, 'close', target='cycle')
    done_zero(journal)
    cycle(journal, 'next')
    with pytest.raises(PendingOrder, match='no longer active'):
        command(journal, 'close', target='cycle')
    assert journal.active_live_cycle()['basis_exit_requested_ts'] is None
    assert journal.operator_control_state(SCOPE)['intent_revision'] == 1


def test_close_latch_revision_and_receipt_roll_back_together(journal):
    cycle(journal)
    journal.db.execute('''CREATE TRIGGER reject_operator_receipt BEFORE INSERT ON operator_operations
        BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END''')
    before = journal.operator_control_state(SCOPE)
    with pytest.raises(sqlite3.IntegrityError, match='injected receipt failure'):
        command(journal, 'close', target='cycle')
    assert journal.operator_control_state(SCOPE) == before
    assert journal.active_live_cycle()['phase'] == 'OPEN_FIRST'
    assert journal.active_live_cycle()['basis_exit_requested_ts'] is None
    assert journal.operator_pending_operations(SCOPE) == []


def test_confirmation_snapshot_is_atomic_immutable_and_retry_keeps_original_proof(journal):
    cycle(journal)
    proof = {'authority_timestamp': 9, 'actual_signed_units': {'entropy': 4, 'hedge': -4},
             'known_watermark': [['order-one', 'TERMINAL', 4]]}
    args = dict(action='close', expected_revision=0, idempotency_key='proof-once',
                request_fingerprint='close-proof', expected_cycle_id='cycle', now=10)
    accepted = journal.commit_operator_command(SCOPE, **args, confirmation_evidence=proof)
    assert accepted['receipt']['confirmation_evidence'] == proof
    assert accepted['receipt']['cycle']['basis_exit_requested_ts'] == 10
    assert accepted['intent_revision'] == 1
    proof['actual_signed_units']['entropy'] = 999
    retried = journal.commit_operator_command(SCOPE, **args,
        confirmation_evidence={'authority_timestamp': 99, 'actual_signed_units': {'entropy': 0, 'hedge': 0}})
    assert retried == accepted
    assert retried['receipt']['confirmation_evidence']['actual_signed_units']['entropy'] == 4
    persisted = journal.operator_operation(SCOPE, accepted['operation_id'])
    assert persisted == accepted


@pytest.mark.parametrize('proof', [[], 'invalid', {'authority_timestamp': float('nan')},
                                 {'actual_signed_units': {'entropy': float('inf')}}])
def test_invalid_confirmation_snapshot_cannot_leave_exit_or_revision_without_receipt(journal, proof):
    cycle(journal)
    before = journal.operator_control_state(SCOPE)
    with pytest.raises(ValueError):
        journal.commit_operator_command(SCOPE, action='close', expected_revision=0,
            idempotency_key='invalid-proof', request_fingerprint='invalid-proof',
            expected_cycle_id='cycle', confirmation_evidence=proof, now=10)
    assert journal.operator_control_state(SCOPE) == before
    assert journal.active_live_cycle()['basis_exit_requested_ts'] is None
    assert journal.active_live_cycle()['phase'] == 'OPEN_FIRST'
    assert journal.operator_operation(SCOPE, idempotency_key='invalid-proof') is None


def test_acceptance_proof_and_close_receipt_roll_back_on_insert_failure(journal):
    cycle(journal)
    journal.db.execute('''CREATE TRIGGER reject_operator_proof BEFORE INSERT ON operator_operations
        BEGIN SELECT RAISE(ABORT,'injected proof failure'); END''')
    before = journal.operator_control_state(SCOPE)
    with pytest.raises(sqlite3.IntegrityError, match='injected proof failure'):
        journal.commit_operator_command(SCOPE, action='close', expected_revision=0,
            idempotency_key='proof-failure', request_fingerprint='proof-failure', expected_cycle_id='cycle',
            confirmation_evidence={'authority_timestamp': 9, 'actual_signed_units': [4, -4]}, now=10)
    assert journal.operator_control_state(SCOPE) == before
    assert journal.active_live_cycle()['basis_exit_requested_ts'] is None
    assert journal.operator_pending_operations(SCOPE) == []


def test_stop_requires_explicit_flat_and_global_no_pending_or_active(journal):
    command(journal, 'stop')
    stopping = journal.operator_control_state(SCOPE)
    assert journal.advance_operator_stop(SCOPE, authoritative_flat=False) == stopping
    journal.prepare(Intent('outside', 'outside-cycle', 'entropy', Side.BUY, 1, D('100')))
    assert journal.advance_operator_stop(SCOPE, authoritative_flat=True) == stopping
    journal.record('outside', Result(True, 0))
    cycle(journal)
    assert journal.advance_operator_stop(SCOPE, authoritative_flat=True) == stopping
    done_zero(journal)
    assert journal.advance_operator_stop(SCOPE, authoritative_flat=True, now=30)['intent'] == 'STOPPED'


@pytest.mark.parametrize('reason', sorted(Journal.CLOSE_REASONS))
def test_start_changes_only_operator_intent_and_never_reopens_risk_batch(journal, reason):
    journal.close_entries('batch', reason, value=D('-1'), threshold=D('3'), cycle_id='old')
    before_batch = dict(journal.db.execute("SELECT * FROM batches WHERE batch_id='batch'").fetchone())
    command(journal, 'stop')
    journal.advance_operator_stop(SCOPE, authoritative_flat=True, now=11)
    accepted = command(journal, 'start', now=12)
    assert accepted['intent'] == 'RUNNING' and accepted['intent_revision'] == 3
    assert dict(journal.db.execute("SELECT * FROM batches WHERE batch_id='batch'").fetchone()) == before_batch
    assert not journal.batch_state('batch')['can_open']
    assert journal.batch_state('batch')['close_reason'] == reason
    with pytest.raises(PendingOrder):
        cycle(journal)


def test_progress_is_scoped_persistent_and_does_not_change_acceptance_or_intent(journal):
    accepted = command(journal, 'stop')
    control = journal.operator_control_state(SCOPE)
    for state in ['executing', 'blocked', 'executing', 'flat_pending_accounting', 'completed']:
        progress = journal.update_operator_operation(SCOPE, accepted['operation_id'],
            state=state, evidence={'state': state}, now=20)
        assert progress['receipt'] == accepted['receipt']
        assert journal.operator_control_state(SCOPE) == control
        assert len(journal.operator_pending_operations(SCOPE)) == (0 if state == 'completed' else 1)
    with pytest.raises(KeyError):
        journal.update_operator_operation('other', accepted['operation_id'],
            state='completed', evidence={})
    with pytest.raises(ValueError, match='cannot regress'):
        journal.update_operator_operation(SCOPE, accepted['operation_id'], state='executing', evidence={})


@pytest.mark.parametrize('phase', ['OPEN_FIRST', 'OPEN_HEDGE', 'RECOVERY', 'OPEN', 'CLOSE_FIRST', 'CLOSE_HEDGE'])
def test_update_live_cycle_cannot_undo_latched_exit(journal, phase):
    cycle(journal)
    command(journal, 'close', target='cycle')
    journal.update_live_cycle('cycle', phase=phase, entropy_units=4, hedge_units=-4,
                              position_evidence_authoritative=False, updated_ts=12)
    current = journal.active_live_cycle()
    assert current['phase'] == 'BASIS_EXIT'
    assert (current['entropy_units'], current['hedge_units']) == (4, -4)
    assert current['basis_exit_requested_ts'] == 10
    assert not journal.lock_basis_profit_basis('cycle', quantity_step=D('.01'), locked_ts=13)
    assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'


@pytest.mark.parametrize('phase', ['OPEN_FIRST', 'OPEN_HEDGE', 'RECOVERY', 'OPEN'])
def test_apply_live_step_cannot_undo_latched_exit_or_double_count(journal, phase):
    cycle(journal)
    order(journal, 'opening', 'entropy', Side.BUY)
    command(journal, 'close', target='cycle')
    args = dict(result_units=10, cash_delta_usd=D('-10'), accounting_final=True,
        entropy_units=10, hedge_units=0, next_phase=phase, updated_ts=12,
        position_evidence_authoritative=False)
    journal.apply_live_step('opening', **args)
    journal.apply_live_step('opening', **args)
    current = journal.active_live_cycle()
    assert current['phase'] == 'BASIS_EXIT' and current['entropy_units'] == 10
    assert D(current['cash_net_usd']) == -10
    assert journal.live_step('opening')['applied'] == 1


@pytest.mark.parametrize('fault', ['nonflat', 'non_authoritative', 'unknown', 'unbalanced', 'missing_grid', 'grid_drift', 'stale_watermark'])
def test_latched_done_requires_terminal_authoritative_quantity_conservation(journal, fault):
    cycle(journal)
    order(journal, 'buy', 'entropy', Side.BUY)
    order(journal, 'sell', 'entropy', Side.SELL, reduce_only=True,
          terminal=fault != 'unknown', grid=D('.02') if fault == 'grid_drift' else D('.01'))
    if fault == 'unbalanced':
        journal.db.execute("UPDATE orders SET filled_units=9 WHERE order_ref='sell'")
    if fault == 'missing_grid':
        journal.db.execute("UPDATE live_steps SET quantity_step=NULL WHERE order_ref='sell'")
    command(journal, 'close', target='cycle')
    with pytest.raises(PendingOrder):
        journal.update_live_cycle('cycle', phase='DONE', entropy_units=1 if fault == 'nonflat' else 0,
            hedge_units=0, position_evidence_authoritative=fault != 'non_authoritative',
            last_authoritative_ts=4 if fault == 'stale_watermark' else 15, updated_ts=15)
    assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'


def test_done_with_conserved_fills_keeps_fee_finality_separate_and_never_resurrects(journal):
    cycle(journal)
    order(journal, 'buy', 'entropy', Side.BUY)
    order(journal, 'sell', 'entropy', Side.SELL, reduce_only=True)
    command(journal, 'close', target='cycle')
    journal.update_live_cycle('cycle', phase='DONE', entropy_units=0, hedge_units=0,
        position_evidence_authoritative=True, last_authoritative_ts=15, updated_ts=15)
    assert journal.active_live_cycle() is None
    journal.update_live_cycle('cycle', phase='OPEN', updated_ts=16)
    assert journal.active_live_cycle() is None
    canonical = journal.db.execute("SELECT * FROM cycles WHERE cycle_id='cycle'").fetchone()
    assert canonical['accounting_final'] == 0 and canonical['pnl_usd'] is None
    with pytest.raises(PendingOrder):
        journal.update_live_cycle('cycle', entropy_units=1, updated_ts=17)


def test_apply_done_failure_rolls_back_step_and_valid_flat_can_complete(journal):
    cycle(journal)
    order(journal, 'buy', 'entropy', Side.BUY)
    order(journal, 'sell', 'entropy', Side.SELL, reduce_only=True)
    command(journal, 'close', target='cycle')
    args = dict(result_units=10, cash_delta_usd=D('10'), accounting_final=False,
        entropy_units=0, hedge_units=0, next_phase='DONE', updated_ts=15)
    with pytest.raises(PendingOrder):
        journal.apply_live_step('sell', **args, position_evidence_authoritative=False)
    assert journal.live_step('sell')['applied'] == 0
    assert D(journal.active_live_cycle()['cash_net_usd']) == 0
    journal.apply_live_step('sell', **args, position_evidence_authoritative=True, last_authoritative_ts=15)
    assert journal.live_step('sell')['applied'] == 1
    assert journal.active_live_cycle() is None


@pytest.mark.parametrize('value', [None, '', 1])
def test_invalid_scope_is_rejected(journal, value):
    with pytest.raises(ValueError):
        journal.ensure_operator_control(value)


def test_invalid_command_and_progress_inputs_do_not_mutate_state(journal):
    before = journal.operator_control_state(SCOPE)
    for payload in [dict(action='restart'), dict(expected_revision=True),
                    dict(expected_cycle_id='unrelated'), dict(now=float('nan'))]:
        args = dict(action='start', expected_revision=0, idempotency_key='bad', request_fingerprint='bad')
        args.update(payload)
        with pytest.raises(ValueError):
            journal.commit_operator_command(SCOPE, **args)
        assert journal.operator_control_state(SCOPE) == before
        assert journal.operator_operation(SCOPE, idempotency_key='bad') is None
    accepted = command(journal, 'stop')
    for state, evidence in [('invalid', {}), ('executing', {'nan': float('nan')}), ('executing', [])]:
        with pytest.raises(ValueError):
            journal.update_operator_operation(SCOPE, accepted['operation_id'], state=state, evidence=evidence)
    assert journal.operator_operation(SCOPE, accepted['operation_id']) == accepted


@pytest.mark.parametrize('metrics', [None, (D('-1.25'), D('2.5'), D('.05'))])
def test_operator_close_missing_market_metrics_are_sql_null_and_decimal_path_is_preserved(journal, metrics):
    cycle(journal)
    values = metrics or (None, None, None)
    journal.record_exit_trigger('cycle', trigger_ts=10, reason='operator_close',
        trigger_basis_bps=values[0], reversion_bps=values[1], executable_pnl_usd=values[2])
    row = journal.db.execute('''SELECT exit_trigger_ts,exit_trigger_reason,
        exit_trigger_basis_bps,exit_trigger_reversion_bps,exit_trigger_executable_pnl_usd
        FROM cycle_economics WHERE cycle_id='cycle' ''').fetchone()
    assert row['exit_trigger_ts'] == 10 and row['exit_trigger_reason'] == 'operator_close'
    assert tuple(row)[2:] == tuple(str(value) if value is not None else None for value in values)
    assert 'None' not in tuple(row)
