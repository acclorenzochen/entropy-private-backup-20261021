"""Offline checks for admission while a completed basis round awaits accounting."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import socket

import pytest

from test_runtime import complete_background_audit, setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('pending-accounting checks cannot access network')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


def test_missing_fee_close_does_not_wait_for_remote_query_and_can_open_again(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, single_cycle_review_loss_usd=None)
    original_send = h.send_taker
    reads = []

    async def missing_fee(**kwargs):
        result = await original_send(**kwargs)
        result.pop('fee_usd', None)
        for fill in h.fee_fills:
            fill['fee_usd'] = None
        return result

    async def slow_history(*args):
        reads.append(True)
        await asyncio.Future()

    h.send_taker = missing_fee
    h.fetch_fee_fills = slow_history

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        first = journal.active_live_cycle()['cycle_id']
        e.set_book(99.60, 99.61)
        h.set_book(99.99, 100)
        await asyncio.wait_for(runtime._resume_cycle(journal.active_live_cycle()), .5)
        assert reads == []
        assert journal.active_live_cycle() is not None
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert e.position == h.position == 0
        state = journal.batch_state('batch')
        assert state['can_open'] and state['pending_accounting'] == 1
        assert state['pending_cost_estimate_usd'] == 0  # Existing hedge fee config is zero.
        assert all(row['fee_usd'] is None for row in journal.cycle_orders(first)
                   if row['venue'] == 'hedge')
        assert not journal.db.execute('SELECT accounting_final FROM cycles').fetchone()[0]
        # The background request can remain pending while admission proceeds.
        background = asyncio.create_task(runtime._reconcile_cycle_accounting(first))
        await asyncio.sleep(0)
        e.set_book(100.10, 100.11)
        h.set_book(99.99, 100)
        for position_error, position in [(RuntimeError('unknown position'), 0), (None, .01)]:
            e.position_error, e.position = position_error, position
            await runtime._refresh_flat_lease()
            await runtime._maybe_open(runtime._market(), D('0'))
            assert journal.active_live_cycle() is None
            assert len(e.sent) == len(h.sent) == 2
        e.position_error, e.position = None, 0
        await runtime._refresh_flat_lease()
        await runtime._maybe_open(runtime._market(), D('0'))
        assert journal.active_live_cycle()['cycle_id'] != first
        assert len(e.sent) == len(h.sent) == 3
        background.cancel()
        with pytest.raises(asyncio.CancelledError):
            await background

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_basis_background_failed_attempt_is_not_retried_every_five_seconds(tmp_path, monkeypatch):
    runtime, _e, _h, journal = setup_runtime(tmp_path)
    now = [0.0]
    reads = []
    row = {'cycle_id': 'waiting'}
    journal.provisional_cycles = lambda _batch: [row]
    monkeypatch.setattr('entropy_arb.v3.runtime.time.monotonic', lambda: now[0])

    async def failed(cycle_id):
        reads.append((cycle_id, now[0]))
        raise RuntimeError('sanitized history unavailable')

    async def wait_for(_event, timeout):
        _event.close()
        now[0] += timeout
        if now[0] >= 125:
            runtime.stop.set()
        raise asyncio.TimeoutError

    runtime._reconcile_cycle_accounting = failed
    monkeypatch.setattr('entropy_arb.v3.runtime.asyncio.wait_for', wait_for)
    try:
        asyncio.run(runtime._analytics_reconcile_loop())
        assert reads == [('waiting', 0.0), ('waiting', 120.0)]
    finally:
        journal.close()
