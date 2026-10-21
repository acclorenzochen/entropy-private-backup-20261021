"""Offline event/audit and canonical cumulative-fill state regressions."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import socket
import time

import pytest

from entropy_arb.venue_lighter import LighterVenue
from entropy_arb.v3.core import Side
from entropy_arb.v3.journal import Intent, Result
from tests_v3.test_open_handoff import streams
from tests_v3.test_runtime import setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('state regression cannot access the network')

    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


def cycle(journal):
    journal.create_live_cycle(
        batch_id='batch', cycle_id='state', direction='long_entropy',
        planned_units=100, reference_bps=D('0'), entry_basis_bps=D('4'),
        started_ts=time.time())
    return journal.active_live_cycle()


def intent(journal, ref, venue, side, *, filled=0, terminal=False):
    journal.prepare_submitting(Intent(ref, 'state', venue, side, 100, D('100'), False))
    journal.record(ref, Result(terminal, filled, D('100') if filled else None, D('0')))


@pytest.mark.parametrize('key', ['entropy', 'hedge'])
def test_external_open_order_blocks_even_after_authoritative_flat_audit(tmp_path, key):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)

    def external(status):
        if key == 'entropy':
            entropy.orders_feed._handle_orders([{'order': {
                'coin': 'SNDK', 'oid': 999, 'cloid': '0x999',
                'origSz': '1', 'sz': '1'}, 'status': status}])
        else:
            hedge.orders_feed._handle_orders({'orders': {'139': [{
                'client_order_index': 999, 'status': status,
                'filled_base_amount': '0', 'filled_quote_amount': '0'}]}})

    async def scenario():
        await runtime.initialize()
        assert await runtime._audit_positions()
        external('open')
        assert not runtime._basis_state_ready()
        assert not await runtime._audit_positions()
        assert not runtime._basis_state_ready()
        external('canceled')
        assert not runtime._basis_state_ready()
        assert await runtime._audit_positions()
        assert runtime._basis_state_ready()
        assert entropy.sent == hedge.sent == []

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_disconnect_requires_background_recovery_after_reconnect(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)

    async def scenario():
        await runtime.initialize()
        assert await runtime._audit_positions()
        entropy.orders_feed.ready.clear()
        assert not runtime._basis_state_ready()
        assert runtime._position_audit_requested.is_set()
        entropy.orders_feed.ready.set()
        entropy.orders_feed.connection_generation += 1
        assert not runtime._basis_state_ready()
        assert await runtime._audit_positions()
        assert runtime._basis_state_ready()

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_position_only_account_update_invalidates_local_flat_state(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)
    hedge.market_id = 139
    hedge._position_from_rows = LighterVenue._position_from_rows

    async def scenario():
        await runtime.initialize()
        assert await runtime._audit_positions()
        assert runtime._basis_state_ready()
        previous = hedge.orders_feed.account_snapshot.received_ts
        hedge.orders_feed._handle_account({
            'type': 'update/account_all',
            'positions': {'139': {'market_id': 139, 'position': '.001', 'sign': 1}},
        })
        hedge.orders_feed.account_snapshot = replace(
            hedge.orders_feed.account_snapshot, received_ts=previous + 1)
        hedge.position = .001
        assert not runtime._basis_state_ready()
        assert runtime._position_audit_requested.is_set()
        assert not await runtime._audit_positions()
        assert entropy.sent == hedge.sent == []

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_audit_started_before_fill_cannot_overwrite_newer_ledger_state(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        assert await runtime._audit_positions()
        active = cycle(journal)
        previous = runtime._last_position_evidence
        entered, release = asyncio.Event(), asyncio.Event()

        async def old_position():
            captured = entropy.position
            entered.set()
            await release.wait()
            return captured

        entropy.fetch_position = old_position
        task = asyncio.create_task(runtime._audit_positions())
        await entered.wait()
        journal.ensure_live_step(
            step_key='state:open_first:0', cycle_id='state', order_ref='fill-during-audit',
            venue='entropy', side='buy', requested_units=100, reduce_only=False,
            quantity_step=runtime.instrument.quantity_step)
        intent(journal, 'fill-during-audit', 'entropy', Side.BUY, filled=25, terminal=True)
        e_units, h_units, newer = await runtime._positions_after_terminal(
            active, venue_key='entropy', side=Side.BUY, filled_units=25)
        journal.apply_live_step(
            'state:open_first:0', result_units=25, cash_delta_usd=D('-.25'),
            accounting_final=True, entropy_units=e_units, hedge_units=h_units,
            next_phase='OPEN_HEDGE', updated_ts=time.time(),
            position_evidence_authoritative=False)
        assert newer is runtime._last_position_evidence and newer is not previous
        entropy.position = .0025
        release.set()
        assert not await task
        assert journal.active_live_cycle()['entropy_units'] == 25
        assert runtime._last_position_evidence is newer
        assert runtime._position_audit_requested.is_set()

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_slow_audit_cannot_erase_position_only_contradiction_already_seen_locally(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)
    hedge.market_id = 139
    hedge._position_from_rows = LighterVenue._position_from_rows

    async def scenario():
        await runtime.initialize()
        assert runtime._basis_state_ready()
        e_read, h_read, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        def old_position(venue, entered):
            async def read():
                captured = venue.position
                entered.set()
                await release.wait()
                return captured
            return read

        entropy.fetch_position = old_position(entropy, e_read)
        hedge.fetch_position = old_position(hedge, h_read)
        task = asyncio.create_task(runtime._audit_positions())
        await asyncio.gather(e_read.wait(), h_read.wait())
        previous = hedge.orders_feed.account_snapshot.received_ts
        hedge.orders_feed._handle_account({
            'type': 'update/account_all',
            'positions': {'139': {'market_id': 139, 'position': '.001', 'sign': 1}},
        })
        hedge.orders_feed.account_snapshot = replace(
            hedge.orders_feed.account_snapshot, received_ts=previous + 1)
        hedge.position = .001
        assert not runtime._basis_state_ready()
        release.set()
        assert not await task
        assert not runtime._basis_state_ready()
        assert runtime._position_audit_requested.is_set()
        assert entropy.sent == hedge.sent == []

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_unknown_audit_retains_authoritative_residual_without_enabling_exposure(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle(journal)
        intent(journal, 'known-e', 'entropy', Side.BUY, filled=100, terminal=True)
        intent(journal, 'unknown-h', 'hedge', Side.SELL, filled=20)
        entropy.position, hedge.position = .01, -.006
        assert not await runtime._audit_positions()
        active = journal.active_live_cycle()
        assert (active['entropy_units'], active['hedge_units']) == (100, -60)
        assert runtime._last_position_evidence.fully_authoritative
        assert not runtime._basis_state_ready()
        assert [row['order_ref'] for row in journal.pending()] == ['unknown-h']
        assert runtime._known_positions(active) == (100, -20)
        assert entropy.sent == hedge.sent == []

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_known_terminal_does_not_erase_other_leg_unknown_audited_position(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle(journal)
        intent(journal, 'known-e', 'entropy', Side.BUY, filled=100, terminal=True)
        intent(journal, 'unknown-h', 'hedge', Side.SELL, filled=20)
        entropy.position, hedge.position = .01, -.006
        await runtime._audit_positions()
        active = journal.active_live_cycle()
        journal.prepare_submitting(Intent('closed-e', 'state', 'entropy', Side.SELL,
                                          100, D('100'), True))
        journal.record('closed-e', Result(True, 100, D('100'), D('0')))
        e_units, h_units, evidence = await runtime._positions_after_terminal(
            active, venue_key='entropy', side=Side.SELL, filled_units=100)
        assert (e_units, h_units) == (0, -60)
        assert not evidence.fully_authoritative
        assert [row['order_ref'] for row in journal.pending()] == ['unknown-h']

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_canonical_cumulative_fill_counts_duplicates_and_late_unknown_once(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        active = cycle(journal)
        intent(journal, 'cumulative-e', 'entropy', Side.BUY, filled=20)
        duplicate = Result(False, 20, D('100'), D('0'))
        journal.record('cumulative-e', duplicate)
        journal.record('cumulative-e', duplicate)
        assert runtime._known_positions(active) == (20, 0)
        journal.record('cumulative-e', Result(True, 35, D('100'), D('0')))
        journal.record('cumulative-e', Result(True, 35, D('100'), D('0')))
        assert runtime._known_positions(active) == (35, 0)
        assert len(journal.cycle_orders(active['cycle_id'])) == 1
        assert not journal.pending()

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_basis_flat_proof_does_not_expire_at_old_lease_timeout(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        assert await runtime._audit_positions()
        lease = runtime._valid_flat_lease(time.time() + 30)
        assert lease is not None
        assert runtime._basis_state_ready()
        assert runtime.settings.position_audit_seconds == 60

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_idle_local_state_monitor_does_not_turn_into_account_hot_poll(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    reads = []

    async def counted():
        reads.append(True)
        return 0

    async def scenario():
        await runtime.initialize()
        entropy.fetch_position = hedge.fetch_position = counted
        monitor = asyncio.create_task(runtime._flat_lease_loop())
        asyncio.get_running_loop().call_later(.55, runtime.stop.set)
        await asyncio.wait_for(monitor, timeout=1)
        assert not reads
        assert runtime._basis_state_ready()
        assert runtime.settings.position_audit_seconds == 60

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('blocked', ['pending', 'nonzero_canonical', 'stale', 'old_cycle_argument', None])
def test_finish_flat_requires_terminal_conservation_and_post_result_authority(tmp_path, blocked):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    reads = []

    async def forbidden():
        reads.append(True)
        raise AssertionError('finish_flat cannot synchronously read an account')

    async def scenario():
        await runtime.initialize()
        cycle(journal)
        intent(journal, 'last-e', 'entropy', Side.BUY,
               filled=25 if blocked == 'nonzero_canonical' else 0,
               terminal=blocked != 'pending')
        await runtime._audit_positions()
        evidence = runtime._last_position_evidence
        if blocked == 'stale':
            evidence = replace(evidence, observed_ts=0, entropy_received_ts=0,
                               hedge_received_ts=0)
        entropy.fetch_position = hedge.fetch_position = forbidden
        active = journal.active_live_cycle()
        if blocked == 'old_cycle_argument':
            active = {**active, 'entropy_units': 25}
        await runtime._finish_flat(active, zero_fill=True, evidence=evidence)
        assert not reads
        if blocked and blocked != 'old_cycle_argument':
            assert journal.active_live_cycle() is not None
            assert runtime._position_audit_requested.is_set()
        else:
            assert journal.active_live_cycle() is None
            assert not journal.pending()
            done = journal.db.execute('SELECT phase FROM live_cycles WHERE cycle_id=?',
                                      ('state',)).fetchone()
            assert done['phase'] == 'DONE'
        assert entropy.sent == hedge.sent == []

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('resolve_error', [False, True])
def test_unknown_hedge_order_does_not_freeze_known_entropy_reduce_only(tmp_path, resolve_error):
    from tests_v3.test_normal_exit import seed_pair
    from entropy_arb.v3.core import Direction

    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        seed_pair(runtime, entropy, hedge, journal, Direction.LONG_ENTROPY)
        journal.latch_basis_exit('current', requested_ts=time.time())
        ref = 'unknown-hedge-close'
        journal.prepare_submitting(Intent(ref, 'current', 'hedge', Side.BUY,
                                          1000, D('101'), True))
        journal.record(ref, Result(False, 0, None, None))

        async def unresolved(order_ref, timeout=None):
            assert order_ref == ref
            if resolve_error:
                raise ConnectionError('offline hedge order lookup unavailable')
            return {'status': 'timeout', 'filled_base': 0, 'avg_px': None,
                    'unresolved': True, 'order_ref': ref}

        hedge.resolve_order = unresolved
        await runtime._audit_positions()
        await runtime._basis_exit(journal.active_live_cycle())
        assert len(entropy.sent) == 1
        assert entropy.sent[0]['reduce_only'] and not entropy.sent[0]['is_buy']
        assert entropy.position == 0
        assert not hedge.sent
        assert journal.active_live_cycle() is not None
        assert [row['order_ref'] for row in journal.pending()] == [ref]

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('changed', [None, 'disconnect', 'external_open', 'account_position', 'exit_latch'])
def test_basis_sdk_guard_rechecks_local_state_after_nonce_wait(tmp_path, changed):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    streams(entropy, hedge)
    hedge.market_id = 139
    hedge._position_from_rows = LighterVenue._position_from_rows

    async def scenario():
        await runtime.initialize()
        assert runtime._basis_state_ready()
        cycle(journal)
        intent(journal, 'first-fill', 'entropy', Side.BUY, filled=25, terminal=True)
        journal.update_live_cycle('state', phase='OPEN_HEDGE', entropy_units=25,
                                  hedge_units=0, updated_ts=time.time())
        ref = '42'
        journal.prepare_submitting(Intent(ref, 'state', 'hedge', Side.SELL,
                                          25, D('100'), False))
        if changed == 'disconnect':
            entropy.orders_feed.ready.clear()
        elif changed == 'external_open':
            entropy.orders_feed._handle_orders([{'order': {
                'coin': 'SNDK', 'oid': 999, 'cloid': '0x999',
                'origSz': '1', 'sz': '1'}, 'status': 'open'}])
        elif changed == 'account_position':
            hedge.orders_feed._handle_account({
                'type': 'update/account_all',
                'positions': {'139': {'market_id': 139, 'position': '.001', 'sign': 1}},
            })
        elif changed == 'exit_latch':
            journal.latch_basis_exit('state', requested_ts=time.time())

        async def forbidden():
            raise AssertionError('SDK increase guard cannot read an account')

        entropy.fetch_position = hedge.fetch_position = forbidden
        reason = runtime._lighter_increase_guard(ref)
        assert bool(reason) is (changed is not None)
        assert entropy.sent == hedge.sent == []

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('identity', [
    'prepared', 'terminal', 'unknown', 'partial', 'reduce_only', 'wrong_phase',
    'wrong_side', 'excess_units', 'missing',
])
def test_basis_sdk_guard_accepts_only_fresh_matching_unsubmitted_intent(tmp_path, identity):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle(journal)
        intent(journal, 'first-fill', 'entropy', Side.BUY, filled=25, terminal=True)
        journal.update_live_cycle('state', phase='OPEN_HEDGE', entropy_units=25,
                                  hedge_units=0, updated_ts=time.time())
        ref = '42'
        proposed = Intent(ref, 'state', 'hedge',
                          Side.BUY if identity == 'wrong_side' else Side.SELL,
                          26 if identity == 'excess_units' else 25,
                          D('100'), identity == 'reduce_only')
        if identity == 'prepared':
            journal.prepare(proposed)
        elif identity != 'missing':
            journal.prepare_submitting(proposed)
        if identity == 'terminal':
            journal.record(ref, Result(True, 0))
        elif identity in ('unknown', 'partial'):
            journal.record(ref, Result(False, 5 if identity == 'partial' else 0))
        elif identity == 'wrong_phase':
            journal.update_live_cycle('state', phase='OPEN', updated_ts=time.time())
        assert runtime._lighter_increase_guard(ref)
        assert entropy.sent == hedge.sent == []

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_inflight_submitting_audit_preserves_baseline_without_account_reads(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    reads = []

    async def unexpected():
        reads.append(True)
        return 0

    async def scenario():
        await runtime.initialize()
        assert runtime._basis_recovered
        previous = runtime._last_position_evidence
        cycle(journal)
        journal.prepare_submitting(Intent('inflight-e', 'state', 'entropy', Side.BUY,
                                          100, D('100'), False))
        entropy.fetch_position = hedge.fetch_position = unexpected
        assert not await runtime._audit_positions()
        assert runtime._basis_recovered
        assert runtime._last_position_evidence is previous
        assert not reads
        assert not runtime._basis_state_ready()  # No additional intent while SDK owns this one.

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_basis_resume_leaves_unknown_resolution_to_background(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    resolves = []

    async def unexpected():
        resolves.append(True)
        raise AssertionError('main basis resume cannot synchronously recover an UNKNOWN')

    async def scenario():
        await runtime.initialize()
        cycle(journal)
        intent(journal, 'known-e', 'entropy', Side.BUY, filled=100, terminal=True)
        intent(journal, 'unknown-h', 'hedge', Side.SELL, filled=20)
        entropy.position, hedge.position = .01, -.002
        await runtime._audit_positions()
        journal.update_live_cycle('state', phase='OPEN_HEDGE', updated_ts=time.time())
        runtime._resolve_pending = unexpected
        await runtime._resume_cycle_inner(journal.active_live_cycle())
        assert not resolves
        assert runtime._position_audit_requested.is_set()
        assert journal.pending()[0]['order_ref'] == 'unknown-h'
        assert len(entropy.sent) == 1 and entropy.sent[0]['reduce_only']
        assert not hedge.sent

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_known_hedge_reduce_only_does_not_require_entropy_book_fresh(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle(journal)
        intent(journal, 'known-h', 'hedge', Side.SELL, filled=100, terminal=True)
        journal.update_live_cycle('state', phase='RECOVERY', entropy_units=0,
                                  hedge_units=-100, updated_ts=time.time())
        hedge.position = -.01
        entropy.book.alive_ts = time.time() - 10
        assert not entropy.book.is_fresh(runtime.settings.max_book_age_seconds)
        assert await runtime._reduce_residual(journal.active_live_cycle(), 0, -100,
                                             purpose='repair')
        assert not entropy.sent
        assert len(hedge.sent) == 1 and hedge.sent[0]['reduce_only']
        assert hedge.sent[0]['is_buy'] and hedge.position == 0
        assert not journal.pending()

    try:
        asyncio.run(scenario())
    finally:
        journal.close()
