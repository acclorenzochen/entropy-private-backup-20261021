"""Operator protocol wired to the production runtime with offline fake venues."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import socket
import time
from types import SimpleNamespace
import uuid

import pytest

from entropy_arb.v3.core import Direction
from entropy_arb.v3.journal import Journal, Result
from entropy_arb.v3.runtime import LiveRuntime
from entropy_arb.v3.live_host import canonical_config_hash
from tests_v3.test_normal_exit import seed_pair
from tests_v3.test_parallel_exit import assert_closed_once, finish_exit
from tests_v3.test_runtime import complete_background_audit, setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('operator runtime checks must remain offline')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


@pytest.fixture
def owner_runtime(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, single_cycle_review_loss_usd=None)
    runtime._parameters_base_settings = runtime.settings
    try:
        yield runtime, e, h, journal
    finally:
        journal.close()


def request(runtime, op='state', **values):
    owner = runtime.operator_control
    result = dict(protocol_version=1, request_id=str(uuid.uuid4()), op=op,
                  **owner.binding, **values)
    if op != 'state':
        result['owner_instance_id'] = owner.owner_instance_id
    return result


def state(runtime):
    result = runtime.operator_control.handle(request(runtime))
    assert result['ok'], result
    return result


def preview(runtime, action, *, target=None):
    return runtime.operator_control.handle(request(runtime, 'preview', action=action,
        intent_revision=state(runtime)['intent_revision'], expected_cycle_id=target))


def command(runtime, action, *, target=None):
    receipt = preview(runtime, action, target=target)
    assert receipt['ok'], receipt
    confirmation = request(runtime, 'confirm', action=action,
        intent_revision=receipt['intent_revision'], expected_cycle_id=target,
        receipt_id=receipt['receipt_id'], idempotency_key=str(uuid.uuid4()))
    result = runtime.operator_control.handle(confirmation)
    assert result['ok'], result
    return result['operation'], confirmation


def operation(runtime, operation_id):
    response = runtime.operator_control.handle(request(runtime, 'operation', operation_id=operation_id))
    assert response['ok'], response
    return response['operation']


async def initialize_owner(runtime):
    runtime._parameters_base_settings = runtime.settings
    await runtime.initialize(apply_leverage=False)
    runtime.enable_operator_control(dict(account_binding_hash='a' * 64,
        pair_identity=runtime.pair_id, deployment=runtime.deployment,
        batch_id=runtime.authorization.batch_id, config_hash=runtime.status.base['config_hash']))
    assert state(runtime)['intent'] == 'STOPPED'
    command(runtime, 'start')
    assert state(runtime)['intent'] == 'RUNNING'


async def paired(runtime, e, h, journal, direction=Direction.LONG_ENTROPY):
    seed_pair(runtime, e, h, journal, direction)
    assert await runtime._audit_positions()
    assert state(runtime)['position_authoritative']


def no_manual_batch_stop(journal):
    batch = journal.batch_state('batch')
    assert not batch['closed_for_entries'] and batch['close_reason'] is None, batch


@pytest.mark.parametrize('direction', list(Direction))
def test_running_close_finishes_target_without_pausing_entries_or_manual_batch_stop(owner_runtime, direction):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        await paired(runtime, e, h, journal, direction)
        # Flat-price books fail the normal profit target; operator close latches exit.
        e.set_book('100', '100')
        h.set_book('100', '100')
        assert not runtime._basis_exit_decision(journal.active_live_cycle(), runtime._market(),
                                              runtime._live_entry_anchor(journal.active_live_cycle())).allowed
        close, confirmation = command(runtime, 'close', target='current')
        accepted_receipt = journal.operator_operation(runtime._operator_scope,
                                                       close['operation_id'])['receipt']
        assert close['intent'] == 'RUNNING'
        assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'
        trigger = journal.db.execute('SELECT exit_trigger_reason,exit_trigger_basis_bps '
                                    'FROM cycle_economics WHERE cycle_id=?', ('current',)).fetchone()
        assert trigger['exit_trigger_reason'] == 'operator_close'
        assert trigger['exit_trigger_basis_bps'] is None
        no_manual_batch_stop(journal)
        await finish_exit(runtime, journal)
        runtime._update_operator_progress()
        assert_closed_once(e, h, journal)
        assert state(runtime)['intent'] == 'RUNNING' and runtime._operator_running()
        assert not runtime.stop.is_set()
        done = journal.db.execute('SELECT phase FROM live_cycles WHERE cycle_id=?', ('current',)).fetchone()
        assert done['phase'] == 'DONE'
        saved = operation(runtime, close['operation_id'])
        assert saved['state'] == 'flat_pending_accounting'
        assert saved['evidence']['accounting_final'] is False
        assert saved['evidence']['intent'] == 'RUNNING'
        assert all(leg['remaining_units'] == 0 for leg in saved['evidence']['legs'])
        assert {leg['venue']: leg['initial_units'] for leg in saved['evidence']['legs']} == {
            'entropy': direction.entropy_side.sign * 1000,
            'hedge': -direction.entropy_side.sign * 1000}
        durable = journal.operator_operation(runtime._operator_scope, close['operation_id'])
        assert durable['receipt']['confirmation_evidence'] == accepted_receipt['confirmation_evidence']
        before = (len(e.sent), len(h.sent))
        retry = runtime.operator_control.handle(confirmation)
        assert retry['ok'] and retry['operation']['operation_id'] == close['operation_id']
        assert (len(e.sent), len(h.sent)) == before
        no_manual_batch_stop(journal)
    asyncio.run(scenario())


def test_fresh_flat_stop_reaches_stopped_without_process_stop_or_new_entry(owner_runtime):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        stop, _ = command(runtime, 'stop')
        assert state(runtime)['intent'] == 'STOPPING'
        await runtime._maybe_open(runtime._market(), D('0'))
        runtime._update_operator_progress()
        assert state(runtime)['intent'] == 'STOPPED'
        assert operation(runtime, stop['operation_id'])['state'] == 'completed'
        assert not runtime.stop.is_set() and e.sent == h.sent == []
        assert journal.active_live_cycle() is None and not journal.pending()
        no_manual_batch_stop(journal)
        start, _ = command(runtime, 'start')
        assert start['state'] == 'completed' and state(runtime)['intent'] == 'RUNNING'
        assert not runtime.stop.is_set()
        await runtime._maybe_open(runtime._market(), D('0'))
        assert len(e.sent) == len(h.sent) == 1
        assert journal.active_live_cycle()['phase'] == 'OPEN'
    asyncio.run(scenario())


def test_stop_during_existing_entropy_submission_allows_exact_hedge_and_normal_exit(owner_runtime):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        entered, release = asyncio.Event(), asyncio.Event()
        original = e.send_taker
        async def send(**kwargs):
            result = await original(**kwargs)
            entered.set()
            await release.wait()
            return result
        e.send_taker = send
        opening = asyncio.create_task(runtime._maybe_open(runtime._market(), D('0')))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            assert len(e.sent) == 1 and h.sent == []
            assert journal.pending()[0]['state'] == 'SUBMITTING'
            stop, _ = command(runtime, 'stop')
            assert state(runtime)['intent'] == 'STOPPING'
        finally:
            release.set()
            await opening
        active = journal.active_live_cycle()
        assert active['phase'] == 'OPEN' and active['basis_exit_requested_ts'] is None
        assert active['entropy_units'] == -active['hedge_units'] != 0
        assert len(e.sent) == len(h.sent) == 1
        assert h.sent[0]['qty'] == e.sent[0]['qty'] and not h.sent[0]['reduce_only']
        no_manual_batch_stop(journal)
        runtime._update_operator_progress()
        assert state(runtime)['intent'] == 'STOPPING' and not runtime.stop.is_set()
        # This short-Entropy pair exits through the ordinary profit decision.
        e.set_book('99.60', '99.61')
        h.set_book('99.99', '100')
        await finish_exit(runtime, journal)
        runtime._update_operator_progress()
        assert state(runtime)['intent'] == 'STOPPED'
        assert operation(runtime, stop['operation_id'])['state'] == 'completed'
        assert e.position == h.position == 0 and not runtime.stop.is_set()
        sent = (len(e.sent), len(h.sent))
        await runtime._maybe_open(runtime._market(), D('0'))
        assert (len(e.sent), len(h.sent)) == sent
        no_manual_batch_stop(journal)
    asyncio.run(scenario())


@pytest.mark.parametrize('first', ['stop', 'close'])
def test_stop_close_and_close_then_stop_never_restore_running(owner_runtime, first):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        await paired(runtime, e, h, journal)
        if first == 'stop':
            stop, _ = command(runtime, 'stop')
            close, confirmation = command(runtime, 'close', target='current')
        else:
            close, confirmation = command(runtime, 'close', target='current')
            stop, _ = command(runtime, 'stop')
        assert state(runtime)['intent'] == 'STOPPING'
        retry = runtime.operator_control.handle(confirmation)
        assert retry['ok'] and retry['operation']['operation_id'] == close['operation_id']
        assert retry['state']['intent'] == 'STOPPING'
        assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == 3
        await finish_exit(runtime, journal)
        runtime._update_operator_progress()
        assert_closed_once(e, h, journal)
        assert state(runtime)['intent'] == 'STOPPED' and not runtime.stop.is_set()
        assert operation(runtime, stop['operation_id'])['state'] == 'completed'
        assert operation(runtime, close['operation_id'])['evidence']['intent'] == 'STOPPED'
        no_manual_batch_stop(journal)
    asyncio.run(scenario())


@pytest.mark.parametrize('reason', ['SINGLE_CYCLE_REVIEW_LOSS', 'REALIZED_LOSS_BUDGET'])
def test_start_refuses_existing_risk_lock_and_preserves_batch(owner_runtime, reason):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        command(runtime, 'stop')
        runtime._update_operator_progress()
        journal.close_entries('batch', reason)
        before = journal.batch_state('batch')
        revision = state(runtime)['intent_revision']
        refusal = preview(runtime, 'start')
        assert not refusal['ok'] and refusal['reason'] == 'risk_locked'
        assert state(runtime)['intent'] == 'STOPPED' and state(runtime)['intent_revision'] == revision
        assert journal.batch_state('batch') == before
        assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == 2
        assert not runtime.stop.is_set() and e.sent == h.sent == []
    asyncio.run(scenario())


def test_stop_while_last_dispatch_token_awaits_has_zero_submissions(owner_runtime):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        waiting, release = asyncio.Event(), asyncio.Event()
        released = []
        e.supports_dispatch_token = True
        async def prepare_dispatch(**kwargs):
            assert not kwargs['reduce_only']
            waiting.set()
            await release.wait()
            return SimpleNamespace(release=lambda: released.append(True))
        e.prepare_dispatch = prepare_dispatch
        opening = asyncio.create_task(runtime._maybe_open(runtime._market(), D('0')))
        try:
            await asyncio.wait_for(waiting.wait(), 1)
            assert journal.active_live_cycle() is not None and not journal.pending()
            stop, _ = command(runtime, 'stop')
        finally:
            release.set()
            await opening
        await complete_background_audit(runtime)
        runtime._update_operator_progress()
        assert released == [True] and e.sent == h.sent == []
        assert journal.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0] == 0
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 0
        assert journal.active_live_cycle() is None and not journal.pending()
        assert state(runtime)['intent'] == 'STOPPED' and not runtime.stop.is_set()
        assert operation(runtime, stop['operation_id'])['state'] == 'completed'
        no_manual_batch_stop(journal)
    asyncio.run(scenario())


@pytest.mark.parametrize('pending_key', ['entropy', 'hedge'])
def test_close_partial_unknown_keeps_same_id_and_duplicate_cumulative_is_not_reapplied(owner_runtime, pending_key):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        await paired(runtime, e, h, journal)
        e.fill_fractions, h.fill_fractions = [.5], [.25]
        pending = e if pending_key == 'entropy' else h
        pending.unknown_once = True
        original = pending.resolve_order
        resolves, allow = [], False
        async def resolve(ref, timeout=None):
            resolves.append(ref)
            if not allow:
                return dict(status='timeout', filled_base=0, avg_px=None,
                            unresolved=True, order_ref=ref)
            return await original(ref, timeout=timeout)
        pending.resolve_order = resolve
        close, _ = command(runtime, 'close', target='current')
        await runtime._resume_cycle(journal.active_live_cycle())
        ref = pending.sent[0]['order_ref']
        assert [row['order_ref'] for row in journal.pending()] == [ref]
        runtime._update_operator_progress()
        saved = operation(runtime, close['operation_id'])
        assert saved['state'] == 'executing'
        assert saved['evidence']['pending_orders'][0]['order_ref'] == ref
        await runtime._resume_cycle(journal.active_live_cycle())
        await runtime._resolve_pending()
        assert len(pending.sent) == 1 and set(resolves) == {ref}
        allow = True
        await runtime._resolve_pending()
        row = journal.order(ref)
        assert row['state'] == 'TERMINAL'
        expected = 500 if pending_key == 'entropy' else 250
        assert row['filled_units'] == expected
        await runtime._audit_positions()
        before = journal.active_live_cycle()
        duplicate = Result(True, expected, D(row['avg_px']), D('0'))
        journal.record(ref, duplicate)
        journal.record(ref, duplicate)
        await runtime._audit_positions()
        after = journal.active_live_cycle()
        assert (after['entropy_units'], after['hedge_units'], after['cash_net_usd']) == (
            before['entropy_units'], before['hedge_units'], before['cash_net_usd'])
        assert journal.order(ref)['filled_units'] == expected
        await finish_exit(runtime, journal)
        runtime._update_operator_progress()
        assert_closed_once(e, h, journal)
        assert len(pending.sent) == 2 and pending.sent[1]['order_ref'] != ref
        assert D(str(pending.sent[1]['qty'])) == D('0.1') - runtime.instrument.quantity(expected)
        assert set(resolves) == {ref}
        assert state(runtime)['intent'] == 'RUNNING' and not runtime.stop.is_set()
    asyncio.run(scenario())


def test_flat_pending_accounting_retains_unknown_actual_fees_funding_and_net(owner_runtime):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        await paired(runtime, e, h, journal)
        for venue in (e, h):
            original = venue.send_taker
            async def send(_original=original, **kwargs):
                raw = await _original(**kwargs)
                raw['fee_usd'] = None
                return raw
            async def unavailable(*args, **kwargs):
                return None
            async def no_history(*args, **kwargs):
                return []
            venue.send_taker = send
            venue.fetch_funding_payments = unavailable
            venue.fetch_fee_fills = no_history
        close, _ = command(runtime, 'close', target='current')
        await finish_exit(runtime, journal)
        assert await runtime._reconcile_cycle_accounting('current') is None
        runtime._update_operator_progress()
        saved = operation(runtime, close['operation_id'])
        assert saved['state'] == 'flat_pending_accounting'
        assert saved['evidence']['accounting_final'] is False
        assert all(leg['remaining_units'] == 0 for leg in saved['evidence']['legs'])
        assert not journal.fee_state('current')['fees_complete']
        cycle = journal.db.execute('SELECT accounting_final FROM cycles WHERE cycle_id=?', ('current',)).fetchone()
        economics = journal.db.execute('SELECT actual_fees_usd,actual_funding_usd,finalized_pnl_usd '
                                      'FROM cycle_economics WHERE cycle_id=?', ('current',)).fetchone()
        assert not cycle['accounting_final']
        assert economics['actual_fees_usd'] is None
        assert economics['actual_funding_usd'] is None and economics['finalized_pnl_usd'] is None
        assert journal.active_live_cycle() is None and not journal.pending()
        assert state(runtime)['intent'] == 'RUNNING' and not runtime.stop.is_set()
    asyncio.run(scenario())


def test_inflight_post_invalidates_old_flat_authority_and_close_preview(owner_runtime):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        old = state(runtime)
        assert old['position_authoritative'] and old['entropy_units'] == old['hedge_units'] == 0
        entered, release = asyncio.Event(), asyncio.Event()
        original = e.send_taker
        async def send(**kwargs):
            result = await original(**kwargs)
            entered.set()
            await release.wait()
            return result
        e.send_taker = send
        task = asyncio.create_task(runtime._maybe_open(runtime._market(), D('0')))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            current = state(runtime)
            assert current['pending_orders'][0]['state'] == 'SUBMITTING'
            assert current['position_authoritative'] is False
            assert current['entropy_units'] is None and current['hedge_units'] is None
            refusal = preview(runtime, 'close', target=current['active_cycle_id'])
            assert not refusal['ok']
            assert journal.active_live_cycle()['basis_exit_requested_ts'] is None
            assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == 1
        finally:
            release.set()
            await task
    asyncio.run(scenario())


def test_start_confirmation_rechecks_new_risk_lock_without_changing_intent_or_batch(owner_runtime):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        command(runtime, 'stop')
        runtime._update_operator_progress()
        receipt = preview(runtime, 'start')
        assert receipt['ok']
        journal.close_entries('batch', 'ACCOUNT_DRAWDOWN')
        before = journal.batch_state('batch')
        confirmation = request(runtime, 'confirm', action='start',
            intent_revision=receipt['intent_revision'], expected_cycle_id=None,
            receipt_id=receipt['receipt_id'], idempotency_key=str(uuid.uuid4()))
        refused = runtime.operator_control.handle(confirmation)
        assert not refused['ok'] and refused['reason'] == 'risk_locked'
        assert state(runtime)['intent'] == 'STOPPED'
        assert state(runtime)['intent_revision'] == receipt['intent_revision']
        assert journal.batch_state('batch') == before
        assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == 2
        assert e.sent == h.sent == [] and not runtime.stop.is_set()
    asyncio.run(scenario())


def test_stale_flat_evidence_cannot_complete_stop_until_background_refresh(owner_runtime):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        runtime._last_position_evidence = replace(runtime._last_position_evidence,
                                                  observed_ts=time.time() - 16)
        stop, _ = command(runtime, 'stop')
        runtime._update_operator_progress()
        assert state(runtime)['intent'] == 'STOPPING'
        assert operation(runtime, stop['operation_id'])['state'] == 'executing'
        assert not state(runtime)['position_authoritative']
        assert await runtime._audit_positions()
        runtime._update_operator_progress()
        assert state(runtime)['intent'] == 'STOPPED'
        assert operation(runtime, stop['operation_id'])['state'] == 'completed'
        assert e.sent == h.sent == [] and not runtime.stop.is_set()
    asyncio.run(scenario())


def test_stop_preserves_unknown_open_id_then_allows_exact_terminal_hedge_completion(owner_runtime):
    runtime, e, h, journal = owner_runtime
    async def scenario():
        await initialize_owner(runtime)
        e.unknown_once = True
        e.fill_fractions = [.5]
        original, resolves, allow = e.resolve_order, [], False
        async def resolve(ref, timeout=None):
            resolves.append(ref)
            if not allow:
                return dict(status='timeout', filled_base=0, avg_px=None,
                            unresolved=True, order_ref=ref)
            return await original(ref, timeout=timeout)
        e.resolve_order = resolve
        await runtime._maybe_open(runtime._market(), D('0'))
        ref = e.sent[0]['order_ref']
        assert [row['order_ref'] for row in journal.pending()] == [ref]
        assert h.sent == []
        # The old initialization flat snapshot cannot describe this UNKNOWN IOC.
        assert not state(runtime)['position_authoritative']
        stop, _ = command(runtime, 'stop')
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(e.sent) == 1 and h.sent == []
        # UNKNOWN price accounting can conservatively latch the existing 3U risk
        # budget. Operator stop must preserve that ordinary risk decision.
        locked = journal.batch_state('batch')
        assert locked['closed_for_entries'] and locked['close_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
        lock_fields = ('close_reason', 'block_value', 'block_threshold', 'block_cycle_id')
        risk_lock = {key: locked[key] for key in lock_fields}
        allow = True
        await runtime._resolve_pending()
        assert await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        active = journal.active_live_cycle()
        assert active['phase'] == 'OPEN' and active['entropy_units'] == -active['hedge_units'] != 0
        assert len(e.sent) == len(h.sent) == 1
        assert D(str(h.sent[0]['qty'])) == D(str(e.sent[0]['qty'])) / 2
        assert set(resolves) == {ref} and not journal.pending()
        assert state(runtime)['intent'] == 'STOPPING'
        e.set_book('99.60', '99.61')
        h.set_book('99.99', '100')
        await finish_exit(runtime, journal)
        runtime._update_operator_progress()
        assert state(runtime)['intent'] == 'STOPPED'
        assert operation(runtime, stop['operation_id'])['state'] == 'completed'
        assert e.position == h.position == 0 and not runtime.stop.is_set()
        final_batch = journal.batch_state('batch')
        assert final_batch['closed_for_entries']
        assert {key: final_batch[key] for key in lock_fields} == risk_lock
    asyncio.run(scenario())


def test_stopped_intent_survives_journal_reopen_and_new_batch_config(owner_runtime, tmp_path):
    runtime, e, h, journal = owner_runtime
    async def first():
        await initialize_owner(runtime)
        command(runtime, 'stop')
        runtime._update_operator_progress()
        saved = state(runtime)
        assert saved['intent'] == 'STOPPED'
        return saved
    saved = asyncio.run(first())
    journal.close()
    recovered_journal = Journal(tmp_path / 'runtime.sqlite', identity='pair-account')
    runtime.status.base['config_hash'] = 'different-config-hash'
    recovered = LiveRuntime(settings=replace(runtime.settings, entry_band_bps=D('4')),
        legacy_config=runtime.cfg, authorization=replace(runtime.authorization, batch_id='new-batch'),
        pair_id=runtime.pair_id, deployment=runtime.deployment, venues=runtime.venues,
        journal=recovered_journal, status=runtime.status, observer=runtime.observer,
        stop=asyncio.Event(), reference_checkpoint_path=runtime.reference_checkpoint_path)
    async def restart():
        await recovered.initialize(apply_leverage=False)
        recovered.enable_operator_control({**runtime.operator_control.binding,
            'config_hash': 'different-config-hash', 'batch_id': 'new-batch'})
        current = state(recovered)
        assert current['intent'] == 'STOPPED' and current['intent_revision'] == saved['intent_revision']
        assert current['owner_instance_id'] != saved['owner_instance_id']
        assert current['batch_id'] == 'new-batch'
        assert current['parameters']['base_config_hash'] == 'different-config-hash'
        assert current['config_hash'] == canonical_config_hash(recovered.settings, pair_identity='pair')
        assert recovered._operator_scope == runtime._operator_scope
        await recovered._maybe_open(recovered._market(), D('0'))
        assert e.sent == h.sent == [] and not recovered.stop.is_set()
        assert recovered_journal.active_live_cycle() is None and not recovered_journal.pending()
        assert recovered_journal.batch_state('batch')['close_reason'] is None
        assert recovered_journal.batch_state('new-batch')['close_reason'] is None
    try:
        asyncio.run(restart())
    finally:
        recovered_journal.close()


def test_completed_close_query_rechecks_late_fee_revocation_without_resending_or_restarting(owner_runtime):
    runtime, e, h, journal = owner_runtime
    runtime.settings = replace(runtime.settings, lighter_zero_fee_account_index=42,
        lighter_zero_fee_batch_id='batch', lighter_zero_fee_policy_source='explicit_offline_authorization')
    h.conf.lighter_creds = SimpleNamespace(account_index=42)
    h.profile = SimpleNamespace(name='mainnet')
    h.market_id, h.fee_policy_order_type = 139, 'market_ioc_no_integrator'
    original = h.send_taker
    async def missing_fee(**kwargs):
        result = await original(**kwargs)
        result['fee_usd'] = None
        for fill in h.fee_fills:
            fill['fee_usd'] = None
        return result
    h.send_taker = missing_fee
    async def scenario():
        await initialize_owner(runtime)
        await runtime._maybe_open(runtime._market(), D('0'))
        assert await runtime._audit_positions()
        target = journal.active_live_cycle()['cycle_id']
        close, confirmation = command(runtime, 'close', target=target)
        e.set_book('99.60', '99.61')
        h.set_book('99.99', '100')
        await finish_exit(runtime, journal)
        final_net = await runtime._reconcile_cycle_accounting(target)
        assert final_net is not None
        runtime._update_operator_progress()
        completed = operation(runtime, close['operation_id'])
        assert completed['state'] == 'completed'
        assert completed['evidence']['accounting_final'] is True
        assert completed['evidence']['fees_complete'] is True
        assert D(completed['evidence']['net_pnl_usd']) == final_net
        assert D(completed['evidence']['known_fees_usd']) == 0
        assert state(runtime)['intent'] == 'RUNNING'
        command(runtime, 'stop')
        runtime._update_operator_progress()
        assert state(runtime)['intent'] == 'STOPPED'
        revision = state(runtime)['intent_revision']
        sends = (len(e.sent), len(h.sent))
        order_refs = [row['order_ref'] for row in journal.cycle_orders(target)]
        # A real identified partial fee contradicts the previous zero-fee policy.
        # The canonical Journal API revokes finality; no direct SQL mutation.
        hedge_close = next(row for row in journal.cycle_orders(target)
                           if row['venue'] == 'hedge' and row['reduce_only'])
        units = hedge_close['filled_units'] // 2
        assert 0 < units < hedge_close['filled_units']
        journal.record_fee_fills(target, 'hedge', [dict(
            exchange_fill_key='late-hedge-close-fee', order_ref=hedge_close['order_ref'],
            units=units, price=hedge_close['avg_px'], ts=time.time(),
            side=hedge_close['side'], fee_usd=D('.01'))])
        canonical = journal.db.execute('SELECT accounting_final FROM cycles WHERE cycle_id=?',
                                        (target,)).fetchone()
        assert not canonical['accounting_final']
        assert not journal.fee_state(target)['fees_complete']
        assert journal.fee_state(target)['known_fees_usd'] == D('.01')
        # No progress tick runs here: each read must refresh completed operations.
        by_id = operation(runtime, close['operation_id'])
        by_key = runtime.operator_control.handle(request(runtime, 'operation',
                                    idempotency_key=close['idempotency_key']))
        repeated = runtime.operator_control.handle(confirmation)
        assert by_key['ok'] and repeated['ok']
        for current in (by_id, by_key['operation'], repeated['operation']):
            assert current['operation_id'] == close['operation_id']
            assert current['state'] == 'flat_pending_accounting'
            assert current['evidence']['accounting_final'] is False
            assert current['evidence']['net_pnl_usd'] is None
            assert current['evidence']['fees_complete'] is False
            assert D(current['evidence']['known_fees_usd']) == D('.01')
            assert current['evidence']['intent'] == 'STOPPED'
            assert all(leg['remaining_units'] == 0 for leg in current['evidence']['legs'])
        assert repeated['state']['intent'] == 'STOPPED'
        assert state(runtime)['intent_revision'] == revision
        assert (len(e.sent), len(h.sent)) == sends
        assert [row['order_ref'] for row in journal.cycle_orders(target)] == order_refs
        assert journal.active_live_cycle() is None and not journal.pending()
        assert e.position == h.position == 0 and not runtime.stop.is_set()
    asyncio.run(scenario())
