"""Offline checks for persisted, concurrent reduce-only basis exits."""
import asyncio
from decimal import Decimal as D
import socket
import time

import pytest

from entropy_arb.v3.core import Direction
from entropy_arb.v3.journal import Journal, Result
from entropy_arb.v3.runtime import LiveRuntime, RuntimeHalt
from test_normal_exit import seed_pair
from test_runtime import complete_background_audit, setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('parallel exit tests cannot access network')

    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


def profitable_pair(runtime, entropy, hedge, journal, direction=Direction.LONG_ENTROPY):
    cycle = seed_pair(runtime, entropy, hedge, journal, direction)
    entropy.set_book('100.3' if direction is Direction.LONG_ENTROPY else '99.5',
                     '101' if direction is Direction.LONG_ENTROPY else '99.7')
    hedge.set_book('100', '100')
    return cycle


async def finish_exit(runtime, journal):
    for _ in range(6):
        cycle = journal.active_live_cycle()
        if cycle is None:
            return
        if journal.pending():
            await runtime._resolve_pending()
        await complete_background_audit(runtime)
        cycle = journal.active_live_cycle()
        if cycle is None:
            return
        await runtime._resume_cycle(cycle)
    await complete_background_audit(runtime)
    assert journal.active_live_cycle() is None, 'persisted exit did not finish its residual'


def assert_closed_once(entropy, hedge, journal):
    assert entropy.position == hedge.position == 0
    assert journal.active_live_cycle() is None
    assert not journal.pending()
    orders = [order for order in journal.cycle_orders('current') if order['reduce_only']]
    for key, venue in (('entropy', entropy), ('hedge', hedge)):
        assert sum(order['filled_units'] for order in orders if order['venue'] == key) == 1000
        assert venue.sent and all(order['reduce_only'] for order in venue.sent)
    steps = [step for step in journal.live_steps('current') if step['reduce_only']]
    assert steps and all(step['applied'] for step in steps)
    assert sum(step['result_units'] for step in steps) == 2000
    cycle = journal.db.execute(
        'SELECT cash_net_usd FROM live_cycles WHERE cycle_id=?', ('current',)).fetchone()
    assert D(cycle['cash_net_usd']) == sum(
        (D(step['cash_delta_usd']) for step in steps), D('0'))


@pytest.mark.parametrize('direction', list(Direction))
def test_both_iocs_are_prepared_and_submitted_before_either_result(tmp_path, direction):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal, direction)
        entered = {'entropy': asyncio.Event(), 'hedge': asyncio.Event()}
        originals = {'entropy': e.send_taker, 'hedge': h.send_taker}

        def sender(key):
            async def send(**kwargs):
                active = journal.active_live_cycle()
                assert active['basis_exit_requested_ts'] is not None
                prepared = [step for step in journal.live_steps('current') if step['reduce_only']]
                assert {step['venue'] for step in prepared} == {'entropy', 'hedge'}
                own = [step for step in prepared if step['venue'] == key]
                assert len(own) == 1
                assert journal.order(own[0]['order_ref'])['state'] == 'SUBMITTING'
                assert all(step['reduce_only'] for step in prepared)
                assert kwargs['reduce_only']
                entered[key].set()
                other = 'hedge' if key == 'entropy' else 'entropy'
                # A sequential sender deadlocks here and fails the bounded wait.
                await asyncio.wait_for(entered[other].wait(), timeout=1)
                assert {step['venue'] for step in [step for step in journal.live_steps('current') if step['reduce_only']]} == {
                    'entropy', 'hedge'}
                return await originals[key](**kwargs)
            return send

        e.send_taker, h.send_taker = sender('entropy'), sender('hedge')
        await asyncio.wait_for(runtime._resume_cycle(journal.active_live_cycle()), timeout=2)
        assert len(e.sent) == len(h.sent) == 1
        assert e.sent[0]['is_buy'] is (direction is Direction.SHORT_ENTROPY)
        assert h.sent[0]['is_buy'] is (direction is Direction.LONG_ENTROPY)
        await finish_exit(runtime, journal)
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('fractions', [(.5, .25), (1, 0), (0, 1)])
def test_partial_or_zero_fill_continues_residual_without_profit_recheck(
        tmp_path, monkeypatch, fractions):
    runtime, e, h, journal = setup_runtime(tmp_path)
    e.fill_fractions, h.fill_fractions = [fractions[0]], [fractions[1]]

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(e.sent) == len(h.sent) == 1
        cycle = journal.active_live_cycle()
        assert cycle['basis_exit_requested_ts'] is not None
        assert e.position == round(.1 * (1 - fractions[0]), 4)
        assert h.position == round(-.1 * (1 - fractions[1]), 4)

        def no_profit_check(*args, **kwargs):
            raise AssertionError('a latched exit must not wait for another profit threshold')

        monkeypatch.setattr(runtime, '_basis_exit_decision', no_profit_check)
        e.set_book('100.05', '100.06')
        h.set_book('100', '100')
        await finish_exit(runtime, journal)
        if fractions[0] < 1:
            assert e.sent[1]['qty'] == round(.1 * (1 - fractions[0]), 4)
        else:
            assert len(e.sent) == 1
        if fractions[1] < 1:
            assert h.sent[1]['qty'] == round(.1 * (1 - fractions[1]), 4)
        else:
            assert len(h.sent) == 1
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('transport_error', [False, True])
def test_unknown_leg_is_resolved_by_same_id_before_any_residual_resubmit(
        tmp_path, monkeypatch, transport_error):
    runtime, e, h, journal = setup_runtime(tmp_path)
    e.fill_fractions, e.unknown_once = [.5], True
    original_send, original_resolve = e.send_taker, e.resolve_order
    allow_resolution, resolves = [False], []

    async def send(**kwargs):
        result = await original_send(**kwargs)
        if transport_error and len(e.sent) == 1:
            raise ConnectionError('offline lost response after exchange accepted IOC')
        return result

    async def resolve(order_ref, timeout=None):
        resolves.append(order_ref)
        if not allow_resolution[0]:
            return {'status': 'timeout', 'filled_base': 0, 'avg_px': None,
                    'err': None, 'unresolved': True, 'order_ref': order_ref}
        return await original_resolve(order_ref, timeout=timeout)

    e.send_taker, e.resolve_order = send, resolve

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        await runtime._resume_cycle(journal.active_live_cycle())
        assert e.position == .05 and h.position == 0
        assert len(e.sent) == len(h.sent) == 1
        ref = e.sent[0]['order_ref']
        assert [row['order_ref'] for row in journal.pending()] == [ref]
        assert journal.active_live_cycle()['basis_exit_requested_ts'] is not None
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(e.sent) == len(h.sent) == 1
        await runtime._resolve_pending()
        assert resolves and set(resolves) == {ref}

        def no_profit_check(*args, **kwargs):
            raise AssertionError('unknown recovery must retain the original exit intent')

        monkeypatch.setattr(runtime, '_basis_exit_decision', no_profit_check)
        allow_resolution[0] = True
        e.set_book('100.05', '100.06')
        h.set_book('100', '100')
        await finish_exit(runtime, journal)
        assert len(e.sent) == 2 and len(h.sent) == 1
        assert e.sent[1]['qty'] == .05 and e.sent[1]['order_ref'] != ref
        assert set(resolves) == {ref}
        assert_closed_once(e, h, journal)
        assert runtime.telemetry_errors == 0

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_exit_intent_and_both_partial_leg_results_survive_runtime_restart(tmp_path, monkeypatch):
    runtime, e, h, journal = setup_runtime(tmp_path)
    e.fill_fractions, h.fill_fractions = [.5], [.5]

    async def first_wave():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        await runtime._resume_cycle(journal.active_live_cycle())
        cycle = journal.active_live_cycle()
        assert cycle['basis_exit_requested_ts'] is not None
        closes = [order for order in journal.cycle_orders('current') if order['reduce_only']]
        assert {order['venue'] for order in closes} == {'entropy', 'hedge'}
        assert all(order['state'] == 'TERMINAL' and order['filled_units'] == 500
                   for order in closes)
        assert e.position == .05 and h.position == -.05
        return cycle['basis_exit_requested_ts']

    requested_ts = asyncio.run(first_wave())
    journal.close()
    recovered_journal = Journal(tmp_path / 'runtime.sqlite', identity='pair-account')
    recovered = LiveRuntime(
        settings=runtime.settings, legacy_config=runtime.cfg,
        authorization=runtime.authorization, pair_id=runtime.pair_id,
        deployment=runtime.deployment, venues=runtime.venues, journal=recovered_journal,
        status=runtime.status, observer=runtime.observer, stop=asyncio.Event(),
        reference_checkpoint_path=runtime.reference_checkpoint_path)

    def no_profit_check(*args, **kwargs):
        raise AssertionError('restart must restore the persisted exit before signal evaluation')

    monkeypatch.setattr(recovered, '_basis_exit_decision', no_profit_check)

    async def recover():
        e.set_book('100.05', '100.06')
        h.set_book('100', '100')
        await recovered.initialize()
        assert recovered_journal.active_live_cycle()['basis_exit_requested_ts'] == requested_ts
        await finish_exit(recovered, recovered_journal)
        assert len(e.sent) == len(h.sent) == 2
        assert e.sent[1]['qty'] == h.sent[1]['qty'] == .05
        assert_closed_once(e, h, recovered_journal)

    try:
        asyncio.run(recover())
    finally:
        recovered_journal.close()


@pytest.mark.parametrize('pending_key', ['entropy', 'hedge'])
def test_unknown_peer_does_not_freeze_known_reduce_only_residual(tmp_path, pending_key):
    runtime, e, h, journal = setup_runtime(tmp_path)
    pending = getattr(runtime.venues, pending_key)
    healthy = h if pending is e else e
    pending.unknown_once = True
    pending.fill_fractions, healthy.fill_fractions = [.5], [.5]
    resolve = pending.resolve_order
    allow_resolution = False

    async def same_id(order_ref, timeout=None):
        if allow_resolution:
            return await resolve(order_ref, timeout=timeout)
        return {'status': 'unknown', 'filled_base': 0, 'avg_px': None,
                'unresolved': True, 'order_ref': order_ref}

    pending.resolve_order = same_id

    async def scenario():
        nonlocal allow_resolution
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        await runtime._resume_cycle(journal.active_live_cycle())
        ref = pending.sent[0]['order_ref']
        assert [row['order_ref'] for row in journal.pending()] == [ref]
        assert abs(pending.position) == abs(healthy.position) == .05
        async def no_account_query():
            raise AssertionError('a known residual cannot synchronously query account state')
        originals = e.fetch_position, h.fetch_position
        e.fetch_position = h.fetch_position = no_account_query
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(pending.sent) == 1 and len(healthy.sent) == 2
        assert healthy.sent[1]['reduce_only'] and healthy.sent[1]['qty'] == .05
        assert healthy.position == 0 and abs(pending.position) == .05
        assert [row['order_ref'] for row in journal.pending()] == [ref]
        assert journal.active_live_cycle() is not None
        e.fetch_position, h.fetch_position = originals
        allow_resolution = True
        await finish_exit(runtime, journal)
        assert len(e.sent) == len(h.sent) == 2
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_partial_exit_uses_known_fills_and_waits_for_authoritative_flat(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    e.fill_fractions, h.fill_fractions = [.5], [.5]

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        await runtime._resume_cycle(journal.active_live_cycle())
        e.position_error = RuntimeError('offline account snapshot unavailable')
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(e.sent) == len(h.sent) == 2
        assert e.position == h.position == 0
        assert journal.active_live_cycle()['basis_exit_requested_ts'] is not None
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is not None
        assert not journal.active_live_cycle()['position_evidence_authoritative']
        e.position_error = None
        e.set_book('100.05', '100.06')
        h.set_book('100', '100')
        await finish_exit(runtime, journal)
        assert e.sent[1]['qty'] == h.sent[1]['qty'] == .05
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_completion_budget_deferral_backs_off_without_empty_steps_or_position_polling(
        tmp_path, monkeypatch):
    runtime, e, h, journal = setup_runtime(tmp_path)
    monotonic = [1000.0]
    monkeypatch.setattr(time, 'monotonic', lambda: monotonic[0])
    limits = (runtime.settings.entropy_orders_per_min, runtime.settings.hedge_orders_per_min)
    assert limits == (6, 6)
    e.fill_fractions, h.fill_fractions = [0] * 6, [0] * 6
    position_calls = []
    for venue in (e, h):
        original = venue.fetch_position

        async def position(v=venue, fetch=original):
            position_calls.append(v.key)
            return await fetch()

        venue.fetch_position = position

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        for _ in range(6):
            await runtime._resume_cycle(journal.active_live_cycle())
        assert len(e.sent) == len(h.sent) == 6
        assert len([step for step in journal.live_steps('current') if step['reduce_only']]) == 12
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 0
        calls_before_completion = len(position_calls)
        send, allowed, attempts = runtime.executor.send, [False], []

        async def physical_budget(intent, **kwargs):
            attempts.append((intent.venue, monotonic[0]))
            if not allowed[0]:
                return Result(False, 0, deferred=True, retry_after=.5,
                              note='offline physical request budget')
            return await send(intent, **kwargs)

        runtime.executor.send = physical_budget
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(attempts) == 2
        assert not journal.pending()
        assert len([step for step in journal.live_steps('current') if step['reduce_only']]) == 12
        for _ in range(50):
            await runtime._resume_cycle(journal.active_live_cycle())
        monotonic[0] += .49
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(attempts) == 2
        assert len(position_calls) == calls_before_completion
        assert len(e.sent) == len(h.sent) == 6
        monotonic[0] += .01
        allowed[0] = True
        e.set_book('100.05', '100.06')
        h.set_book('100', '100')
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(e.sent) == len(h.sent) == 7
        assert len(position_calls) == calls_before_completion
        await complete_background_audit(runtime)
        assert (runtime.settings.entropy_orders_per_min,
                runtime.settings.hedge_orders_per_min) == limits
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_loss_during_partial_exit_locks_entries_and_still_closes_residual(tmp_path, monkeypatch):
    runtime, e, h, journal = setup_runtime(tmp_path)
    e.fill_fractions, h.fill_fractions = [.5], [.5]

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        await runtime._resume_cycle(journal.active_live_cycle())
        assert not journal.batch_state('batch')['closed_for_entries']
        e.set_book('97', '97.01')
        h.set_book('100', '100')

        def no_profit_check(*args, **kwargs):
            raise AssertionError('the loss lock must not rearm the normal profit gate')

        monkeypatch.setattr(runtime, '_basis_exit_decision', no_profit_check)
        for venue in (e, h):
            original = venue.send_taker

            async def close(v=venue, send=original, **kwargs):
                batch = journal.batch_state('batch')
                assert batch['closed_for_entries']
                assert batch['close_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
                assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'
                assert kwargs['reduce_only'] and kwargs['qty'] == .05
                return await send(**kwargs)

            venue.send_taker = close
        await finish_exit(runtime, journal)
        batch = journal.batch_state('batch')
        assert batch['closed_for_entries'] and not batch['can_open']
        assert batch['close_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_flat_loss_lock_uses_cash_even_when_books_expire_after_fill(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        for venue in (e, h):
            original = venue.send_taker

            async def close(v=venue, send=original, **kwargs):
                result = await send(**kwargs)
                result['fee_usd'] = .1
                v.fee_fills[-1]['fee_usd'] = .1
                e.book.alive_ts = h.book.alive_ts = time.time() - 10
                return result

            venue.send_taker = close
        await runtime._resume_cycle(journal.active_live_cycle())
        await complete_background_audit(runtime)
        batch = journal.batch_state('batch')
        assert batch['closed_for_entries'] and not batch['can_open']
        assert batch['close_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
        cash = journal.db.execute(
            'SELECT cash_net_usd FROM live_cycles WHERE cycle_id=?', ('current',)).fetchone()[0]
        assert D(cash) < -runtime.settings.single_cycle_review_loss_usd
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('stale_key', ['entropy', 'hedge'])
def test_stale_leg_does_not_block_healthy_leg_reduce_only_exit(tmp_path, monkeypatch, stale_key):
    runtime, e, h, journal = setup_runtime(tmp_path)
    e.fill_fractions, h.fill_fractions = [.5], [.5]

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        await runtime._resume_cycle(journal.active_live_cycle())
        stale = getattr(runtime.venues, stale_key)
        healthy = h if stale is e else e
        stale.book.alive_ts = time.time() - 10

        def no_profit_check(*args, **kwargs):
            raise AssertionError('a stale peer must not return a latched exit to signal evaluation')

        monkeypatch.setattr(runtime, '_basis_exit_decision', no_profit_check)
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(stale.sent) == 1 and len(healthy.sent) == 2
        assert abs(stale.position) == .05 and healthy.position == 0
        assert healthy.sent[1]['reduce_only'] and healthy.sent[1]['qty'] == .05
        assert len([step for step in journal.live_steps('current') if step['reduce_only']]) == 3
        assert not journal.pending()
        e.set_book('100.05', '100.06')
        h.set_book('100', '100')
        await finish_exit(runtime, journal)
        assert len(e.sent) == len(h.sent) == 2
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_concurrent_resume_calls_cannot_duplicate_the_exit_wave(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle = profitable_pair(runtime, e, h, journal)
        entered, release = asyncio.Event(), asyncio.Event()
        original_send = e.send_taker

        async def send(**kwargs):
            entered.set()
            await release.wait()
            return await original_send(**kwargs)

        e.send_taker = send
        first = asyncio.create_task(runtime._resume_cycle(cycle))
        await asyncio.wait_for(entered.wait(), timeout=1)
        second = asyncio.create_task(runtime._resume_cycle(dict(cycle)))
        await asyncio.sleep(0)
        release.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=2)
        assert len(e.sent) == len(h.sent) == 1
        await finish_exit(runtime, journal)
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('direction', list(Direction))
def test_parallel_orders_are_marketable_reduce_only_and_keep_price_caps(tmp_path, direction):
    runtime, e, h, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal, direction)
        market = runtime._market()
        await runtime._resume_cycle(journal.active_live_cycle())
        for key, venue, book, cap in (
                ('entropy', e, market.entropy, runtime.settings.first_leg_execution_policy('close')[1]),
                ('hedge', h, market.hedge, runtime.settings.hedge_hard_cap_bps)):
            assert len(venue.sent) == 1
            order = venue.sent[0]
            side = direction.entropy_side.opposite
            if key == 'hedge':
                side = side.opposite
            top, px = book.top(side), D(str(order['limit_px']))
            assert order['reduce_only'] and order['qty'] == .1
            assert abs(px / top - 1) * D('10000') <= cap
            assert px >= top if order['is_buy'] else px <= top
        await finish_exit(runtime, journal)
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('shallow_key', ['entropy', 'hedge'])
def test_fresh_leg_without_common_unit_depth_does_not_block_healthy_exit(
        tmp_path, monkeypatch, shallow_key):
    runtime, e, h, journal = setup_runtime(tmp_path)
    e.fill_fractions, h.fill_fractions = [.5], [.5]

    async def scenario():
        await runtime.initialize()
        profitable_pair(runtime, e, h, journal)
        await runtime._resume_cycle(journal.active_live_cycle())
        shallow = getattr(runtime.venues, shallow_key)
        healthy = h if shallow is e else e
        below_step = str(runtime.instrument.quantity_step / 2)
        shallow.book.apply_hl([
            [{'px': '100', 'sz': below_step}, {'px': '99.99', 'sz': below_step}],
            [{'px': '100.01', 'sz': below_step}, {'px': '100.02', 'sz': below_step}],
        ])
        assert shallow.book.is_fresh(runtime.settings.max_book_age_seconds)
        with pytest.raises(RuntimeHalt, match='no executable common-unit depth'):
            runtime._market()

        def no_profit_check(*args, **kwargs):
            raise AssertionError('insufficient peer depth must not rearm the profit gate')

        monkeypatch.setattr(runtime, '_basis_exit_decision', no_profit_check)
        await runtime._resume_cycle(journal.active_live_cycle())
        assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'
        assert len(shallow.sent) == 1 and len(healthy.sent) == 2
        assert abs(shallow.position) == .05 and healthy.position == 0
        assert healthy.sent[1]['reduce_only'] and healthy.sent[1]['qty'] == .05
        assert len([step for step in journal.live_steps('current') if step['reduce_only']]) == 3
        assert not journal.pending()
        e.set_book('100.05', '100.06')
        h.set_book('100', '100')
        await finish_exit(runtime, journal)
        assert len(e.sent) == len(h.sent) == 2
        assert_closed_once(e, h, journal)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()
