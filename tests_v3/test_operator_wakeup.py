"""Control wakeups interrupt recovery backoff without changing market throttling."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import socket
import time
import uuid

import pytest

from entropy_arb.v3.runtime import RuntimeHalt
from entropy_arb.v3.core import Direction
from tests_v3.test_normal_exit import seed_pair
from tests_v3.test_operator_runtime import command, initialize_owner, paired, preview, request, state
from tests_v3.test_parallel_exit import finish_exit
from tests_v3.test_runtime import setup_runtime


@pytest.fixture
def runtime_fixture(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('control wakeup tests forbid external network')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect_ex', forbidden)
    monkeypatch.setattr(socket, 'getaddrinfo', forbidden)
    runtime, e, h, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, single_cycle_review_loss_usd=None)
    try:
        yield runtime, e, h, journal
    finally:
        journal.close()


def test_session_cap_start_refuses_after_automatic_stop_without_changing_batch(runtime_fixture):
    runtime, e, h, journal = runtime_fixture
    async def scenario():
        runtime.session_max_cycles = 1
        await initialize_owner(runtime)
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        assert active['phase'] == 'OPEN'
        await runtime._audit_positions()
        command(runtime, 'close', target=active['cycle_id'])
        await finish_exit(runtime, journal)
        runtime._update_operator_progress()
        await runtime._maybe_open(runtime._market(), D('0'))
        runtime._update_operator_progress()
        before = state(runtime)
        assert before['intent'] == 'STOPPED'
        assert 'SESSION_CYCLE_LIMIT' in before['risk_locks']
        assert 'start' not in before['allowed_actions']
        batch = journal.batch_state(runtime.authorization.batch_id)
        count = journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0]
        assert not preview(runtime, 'start')['ok']
        assert state(runtime)['intent_revision'] == before['intent_revision']
        assert journal.batch_state(runtime.authorization.batch_id) == batch
        assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == count
        sent = (len(e.sent), len(h.sent))
        await runtime._maybe_open(runtime._market(), D('0'))
        assert (len(e.sent), len(h.sent)) == sent and not runtime.stop.is_set()
    asyncio.run(scenario())


def idle_backgrounds(runtime, monkeypatch):
    async def initialized():
        return []
    async def idle():
        await runtime.stop.wait()
    monkeypatch.setattr(runtime, 'initialize', initialized)
    for name in ('_http_keepalive_loop', '_equity_loop', '_analytics_reconcile_loop', '_flat_lease_loop'):
        monkeypatch.setattr(runtime, name, idle)
    runtime.reference_sampler_enabled = False


def test_actual_run_close_interrupts_recovery_backoff_but_market_updates_do_not(runtime_fixture, monkeypatch):
    runtime, e, h, journal = runtime_fixture
    async def scenario():
        await initialize_owner(runtime)
        await paired(runtime, e, h, journal)
        idle_backgrounds(runtime, monkeypatch)
        runtime._reconcile_backoff_seconds = 2.0
        waiting, exiting = asyncio.Event(), asyncio.Event()
        original_wait = runtime._operator_wake.wait
        async def observe_wait():
            waiting.set()
            return await original_wait()
        monkeypatch.setattr(runtime._operator_wake, 'wait', observe_wait)
        calls = []
        async def manage(active):
            calls.append(active['phase'])
            if active['phase'] == 'BASIS_EXIT':
                exiting.set()
                runtime.stop.set()
                runtime.changed.set()
                return
            raise RuntimeHalt('fake transient account or market error')
        monkeypatch.setattr(runtime, '_resume_cycle', manage)
        task = asyncio.create_task(runtime.run())
        try:
            await asyncio.wait_for(waiting.wait(), .5)
            for _ in range(3):
                runtime.changed.set()
                await asyncio.sleep(.01)
            assert calls == ['OPEN'] and not exiting.is_set()
            command(runtime, 'close', target='current')
            assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'
            await asyncio.wait_for(exiting.wait(), .3)
            await asyncio.wait_for(task, .5)
            assert calls == ['OPEN', 'BASIS_EXIT']
            assert e.sent == h.sent == []
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_actual_run_without_control_keeps_original_sleep_backoff(runtime_fixture, monkeypatch):
    runtime, e, h, journal = runtime_fixture
    async def scenario():
        await runtime.initialize(apply_leverage=False)
        seed_pair(runtime, e, h, journal, Direction.LONG_ENTROPY)
        await runtime._audit_positions()
        idle_backgrounds(runtime, monkeypatch)
        runtime._reconcile_backoff_seconds = .4
        waiting, release, resumed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original_sleep = asyncio.sleep
        sleeps, calls = [], []
        async def observe_sleep(delay):
            if delay == .4:
                sleeps.append(delay)
                waiting.set()
                await release.wait()
            else:
                await original_sleep(delay)
        monkeypatch.setattr('entropy_arb.v3.runtime.asyncio.sleep', observe_sleep)
        async def manage(active):
            calls.append(active['phase'])
            if len(calls) == 1:
                raise RuntimeHalt('fake transient account or market error')
            resumed.set()
            runtime.stop.set()
            runtime.changed.set()
        monkeypatch.setattr(runtime, '_resume_cycle', manage)
        task = asyncio.create_task(runtime.run())
        try:
            await asyncio.wait_for(waiting.wait(), .5)
            runtime.changed.set()
            runtime._operator_wake.set()
            await original_sleep(.03)
            assert not resumed.is_set() and calls == ['OPEN']
            assert sleeps == [.4] and runtime.operator_control is None
            release.set()
            await asyncio.wait_for(task, .5)
            assert calls == ['OPEN', 'OPEN'] and e.sent == h.sent == []
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_active_stopping_local_stream_change_refuses_start_with_cached_authority(runtime_fixture, monkeypatch):
    runtime, e, h, journal = runtime_fixture
    async def scenario():
        await initialize_owner(runtime)
        command(runtime, 'stop')
        receipt = preview(runtime, 'start')
        assert receipt['ok'] and receipt['state']['intent'] == 'STOPPING', receipt
        # Populate the durable terminal pair after the preview, as a delayed
        # existing submission can complete while the operator is STOPPING.
        await paired(runtime, e, h, journal)
        active = journal.active_live_cycle()
        assert active['phase'] == 'OPEN' and not journal.pending()
        runtime._update_operator_progress()
        before = state(runtime)
        assert before['intent'] == 'STOPPING' and before['active_cycle_id'] == active['cycle_id']
        assert before['position_authoritative'] and before['risk_locks'] == ['PENDING_ACCOUNTING']
        cached = runtime._last_position_evidence
        assert cached.fully_authoritative and 0 <= time.time() - cached.observed_ts <= 15
        batch = journal.batch_state(runtime.authorization.batch_id)
        count = journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0]
        sent = (len(e.sent), len(h.sent))
        old_token = runtime._basis_event_watermark
        changed_token = (old_token, 'known_local_stream_generation_change')
        monkeypatch.setattr(runtime, '_basis_events', lambda: changed_token)
        async def no_rest():
            raise AssertionError('operator snapshot must refresh local gates without REST')
        monkeypatch.setattr(e, 'fetch_position', no_rest)
        monkeypatch.setattr(h, 'fetch_position', no_rest)
        confirmation = request(runtime, 'confirm', action='start',
            intent_revision=receipt['intent_revision'], expected_cycle_id=None,
            receipt_id=receipt['receipt_id'], idempotency_key=str(uuid.uuid4()))
        refused = runtime.operator_control.handle(confirmation)
        assert not refused['ok'] and refused['reason'] == 'risk_locked'
        after = state(runtime)
        assert after['risk_locks'] == ['PENDING_ACCOUNTING', 'POSITION_RECONCILIATION']
        assert after['position_authoritative'] and runtime._last_position_evidence is cached
        assert after['intent'] == 'STOPPING' and after['intent_revision'] == before['intent_revision']
        assert 'start' not in after['allowed_actions'] and not runtime._basis_recovered
        assert runtime._position_audit_requested.is_set()
        refused_preview = preview(runtime, 'start')
        assert not refused_preview['ok'] and refused_preview['reason'] == 'risk_locked'
        assert journal.batch_state(runtime.authorization.batch_id) == batch
        assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == count
        assert (len(e.sent), len(h.sent)) == sent
        assert journal.active_live_cycle()['phase'] == 'OPEN' and not runtime.stop.is_set()
    asyncio.run(scenario())


def test_actual_run_stopped_keeps_owner_and_feed_alive_then_start_wakes(runtime_fixture, monkeypatch):
    runtime, e, h, journal = runtime_fixture
    async def scenario():
        await initialize_owner(runtime)
        command(runtime, 'stop')
        runtime._update_operator_progress()
        assert state(runtime)['intent'] == 'STOPPED'
        idle_backgrounds(runtime, monkeypatch)
        owner = runtime.operator_control
        feed_started, feed_finished = asyncio.Event(), asyncio.Event()
        paused_wait, resumed = asyncio.Event(), asyncio.Event()
        feeds = []
        async def feed():
            feed_started.set()
            try:
                await runtime.stop.wait()
            finally:
                feed_finished.set()
        async def initialized():
            feeds.append(asyncio.create_task(feed(), name='fake-market-feed'))
            return list(feeds)
        monkeypatch.setattr(runtime, 'initialize', initialized)
        original_wait = runtime.changed.wait
        async def observe_wait():
            if not runtime._operator_running():
                paused_wait.set()
            return await original_wait()
        monkeypatch.setattr(runtime.changed, 'wait', observe_wait)
        original_market = runtime._market
        def market():
            resumed.set()
            runtime.stop.set()
            runtime.changed.set()
            return original_market()
        async def no_entry(*args):
            pass
        monkeypatch.setattr(runtime, '_market', market)
        monkeypatch.setattr(runtime, '_maybe_open', no_entry)
        runtime.changed.clear()
        runtime._operator_wake.clear()
        task = asyncio.create_task(runtime.run())
        try:
            await asyncio.wait_for(paused_wait.wait(), .5)
            await asyncio.wait_for(feed_started.wait(), .5)
            assert not task.done() and not runtime.stop.is_set()
            assert not feeds[0].done() and not feed_finished.is_set()
            assert runtime.operator_control is owner and state(runtime)['intent'] == 'STOPPED'
            assert runtime.status.base['operator_control']['intent'] == 'STOPPED'
            assert not resumed.is_set() and e.sent == h.sent == []
            start, _ = command(runtime, 'start')
            assert start['intent'] == 'RUNNING' and start['state'] == 'completed'
            await asyncio.wait_for(resumed.wait(), .3)
            await asyncio.wait_for(task, .5)
            assert feed_finished.is_set() and e.sent == h.sent == []
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())
