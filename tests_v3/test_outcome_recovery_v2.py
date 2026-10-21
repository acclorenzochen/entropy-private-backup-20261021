"""T06–T11: real Runtime/broker/Journal with offline venue evidence.

Only venue I/O is replaced. Restart cases close and reopen the SQLite journal
and enter through initialize + _resume_cycle, not _open_first directly.
"""
import asyncio
import time
from decimal import Decimal as D

import pytest

from entropy_arb.venue_hl import HLVenue
from entropy_arb.venue_hl import HLOrdersFeed
from entropy_arb.v3.core import Side
from entropy_arb.v3.journal import Intent, Result
from entropy_arb.v3.live_host import OrderRefFactory
from tests_v3.test_runtime import setup_runtime


@pytest.fixture
def runtime_factory(tmp_path):
    journals = []

    def create():
        case = setup_runtime(tmp_path)
        journals.append(case[3])
        return case

    yield create
    for journal in journals:
        journal.close()


def register_first(journal, *, register_step=True):
    cycle_id = 'outcome-restart'
    journal.create_live_cycle(
        batch_id='batch', cycle_id=cycle_id, direction='short_entropy',
        planned_units=85, reference_bps=D('0'), entry_basis_bps=D('5'),
        started_ts=time.time())
    ref = OrderRefFactory.create(
        'hl', deployment='test', cycle_id=cycle_id, leg='open_first:0')
    key = f'{cycle_id}:open_first:0'
    if register_step:
        journal.ensure_live_step(
            step_key=key, cycle_id=cycle_id, order_ref=ref, venue='entropy',
            side='sell', requested_units=85, reduce_only=False,
            execution_group_id=key)
    return Intent(ref, cycle_id, 'entropy', Side.SELL, 85, D('100')), key


def unknown(ref):
    return {'status': 'unknown', 'filled_base': 0, 'avg_px': None,
            'unresolved': True, 'order_ref': ref}


@pytest.mark.parametrize('body', [
    {'status': 'ok', 'response': {'data': {'statuses': []}}},
    {'status': 'ok', 'response': {'data': {'statuses': [
        {'filled': {'oid': 7, 'avgPx': '100'}}]}}},
    {'status': 'unexpected'},
])
def test_t06_malformed_response_after_actual_fill_never_expands_first_leg(
        runtime_factory, body):
    runtime, entropy, hedge, journal = runtime_factory()
    original_send = entropy.send_taker
    terminal, resolves = {}, []
    resolve_terminal = False

    async def send(**kwargs):
        result = await original_send(**kwargs)  # Actual position changes first.
        terminal[kwargs['order_ref']] = result
        return {**HLVenue._parse(body), 'order_ref': kwargs['order_ref']}

    async def resolve(ref, timeout=None):
        resolves.append(ref)
        return terminal[ref] if resolve_terminal else unknown(ref)

    entropy.send_taker, entropy.resolve_order = send, resolve

    async def scenario():
        nonlocal resolve_terminal
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        ref = entropy.sent[0]['order_ref']
        assert journal.order(ref)['state'] == 'UNKNOWN'
        assert len(entropy.sent) == 1 and hedge.sent == []
        assert entropy.position != 0 and hedge.position == 0
        await runtime._resolve_pending()
        await runtime._resume_cycle(journal.active_live_cycle())
        assert journal.order(ref)['state'] == 'UNKNOWN'
        assert len(entropy.sent) == 1 and hedge.sent == []
        resolve_terminal = True
        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        assert journal.order(ref)['state'] == 'TERMINAL'
        assert not journal.pending()
        assert len(entropy.sent) == len(hedge.sent) == 1
        assert resolves and set(resolves) == {ref}
        cycle = journal.active_live_cycle()
        assert cycle['entropy_units'] == -cycle['hedge_units'] != 0

    asyncio.run(scenario())


@pytest.mark.parametrize('quantity', ['0.00015', '-0.0001', 'nan', '1000'])
def test_t07_invalid_or_oversized_quantity_stays_unknown_same_id(
        runtime_factory, quantity):
    runtime, entropy, hedge, journal = runtime_factory()
    original_send = entropy.send_taker
    resolves = []

    def corrupt(ref):
        return {'status': 'filled', 'filled_base': quantity, 'avg_px': '100',
                'unresolved': False, 'order_ref': ref,
                'confirmation_source': 'exchange_http'}

    async def send(**kwargs):
        await original_send(**kwargs)
        return corrupt(kwargs['order_ref'])

    async def resolve(ref, timeout=None):
        resolves.append(ref)
        return corrupt(ref)

    entropy.send_taker, entropy.resolve_order = send, resolve

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        ref = entropy.sent[0]['order_ref']
        row = journal.order(ref)
        assert row['state'] == 'UNKNOWN' and row['filled_units'] == 0
        assert 'adapter contract unknown' in row['note']
        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        assert journal.order(ref)['state'] == 'UNKNOWN'
        assert len(entropy.sent) == 1 and hedge.sent == []
        assert entropy.position != 0 and hedge.position == 0
        assert resolves and set(resolves) == {ref}
        assert [row['order_ref'] for row in journal.pending()] == [ref]

    asyncio.run(scenario())


def test_t08_terminal_zero_contradicts_position_no_new_first_leg(runtime_factory):
    runtime, entropy, hedge, journal = runtime_factory()
    original_send = entropy.send_taker
    entropy.orders_feed = HLOrdersFeed('test', 'unused', '0xabc', 'SNDK')
    entropy.orders_feed.connection_generation = 1
    entropy.orders_feed.ready.set()

    async def send(**kwargs):
        result = await original_send(**kwargs)
        entropy.orders_feed._handle_fills({'fills': [{
            'coin': 'SNDK', 'oid': 999, 'tid': 1,
            'sz': str(result['filled_base']), 'px': '100'}]})
        return {**result, 'status': 'canceled', 'filled_base': 0, 'avg_px': None}

    entropy.send_taker = send

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        assert len(entropy.sent) == 1, 'terminal zero must not override actual exposure'
        first = journal.order(entropy.sent[0]['order_ref'])
        assert first['state'] == 'TERMINAL' and first['filled_units'] == 0
        cycle = journal.active_live_cycle()
        assert cycle is not None
        assert not runtime._basis_state_ready()
        assert hedge.sent == []
        assert not await runtime._audit_positions()
        await runtime._resume_cycle(cycle)
        assert len([order for order in entropy.sent if not order['reduce_only']]) == 1
        assert hedge.sent == []

    asyncio.run(scenario())


def test_t09_registered_only_restart_enters_real_resume(runtime_factory):
    runtime, _, _, journal = runtime_factory()

    async def prepare():
        await runtime.initialize()
        return register_first(journal)

    intent, key = asyncio.run(prepare())
    with pytest.raises(KeyError):
        journal.order(intent.order_ref)
    journal.close()
    restarted, entropy, hedge, reopened = runtime_factory()

    async def resume():
        await restarted.initialize()
        assert not reopened.live_step(key)['applied']
        await restarted._audit_positions()
        await restarted._resume_cycle(reopened.active_live_cycle())
        assert len(entropy.sent) == len(hedge.sent) == 1
        assert entropy.sent[0]['order_ref'] == intent.order_ref
        assert entropy.sent[0]['qty'] == .0085
        assert reopened.live_step(key)['applied']
        assert reopened.active_live_cycle()['entropy_units'] == -85

    asyncio.run(resume())


@pytest.mark.parametrize('state', ['PREPARED', 'SUBMITTING', 'UNKNOWN', 'TERMINAL'])
def test_t10_restart_preserves_intent_and_applies_terminal_once(runtime_factory, state):
    runtime, _, _, journal = runtime_factory()

    async def prepare():
        await runtime.initialize()
        intent, key = register_first(journal)
        journal.prepare(intent)
        if state != 'PREPARED':
            journal.mark_submitting(intent.order_ref)
        if state == 'UNKNOWN':
            journal.record(intent.order_ref, Result(False, 0, note='timeout'))
        elif state == 'TERMINAL':
            journal.record(intent.order_ref, Result(True, 85, D('100'), D('0')))
        return intent, key

    intent, key = asyncio.run(prepare())
    journal.close()
    restarted, entropy, hedge, reopened = runtime_factory()
    entropy.position = 0 if state == 'PREPARED' else -.0085
    resolves = []

    async def resolve(ref, timeout=None):
        resolves.append(ref)
        return {'status': 'filled', 'filled_base': .0085, 'avg_px': 100,
                'fee_usd': 0, 'unresolved': False, 'order_ref': ref}

    entropy.resolve_order = resolve

    async def resume():
        await restarted.initialize()
        await restarted._resolve_pending()
        await restarted._audit_positions()
        await restarted._resume_cycle(reopened.active_live_cycle())
        row = reopened.order(intent.order_ref)
        assert row['state'] == 'TERMINAL'
        assert (row['side'], row['requested_units'], D(row['limit_px'])) == (
            'sell', 85, D('100'))
        assert all(order['order_ref'] != intent.order_ref for order in entropy.sent)
        step = reopened.live_step(key)
        assert step['applied']
        assert step['result_units'] == (0 if state == 'PREPARED' else 85)
        if state == 'PREPARED':
            assert row['note'] == 'proved_unsent_prepared' and resolves == []
        else:
            assert entropy.sent == [] and len(hedge.sent) == 1
            assert hedge.sent[0]['qty'] == .0085
            assert resolves == ([intent.order_ref] if state in {'SUBMITTING', 'UNKNOWN'} else [])
        cycle = reopened.active_live_cycle()
        before_cash = cycle['cash_net_usd']
        before_sends = (len(entropy.sent), len(hedge.sent))
        await restarted._resume_cycle(cycle)
        assert reopened.active_live_cycle()['cash_net_usd'] == before_cash
        assert (len(entropy.sent), len(hedge.sent)) == before_sends
        assert reopened.live_step(key) == step

    asyncio.run(resume())


def test_t11_budget_denied_atomic_dispatch_leaves_no_intent_on_restart(runtime_factory):
    runtime, entropy, hedge, journal = runtime_factory()

    async def prepare():
        await runtime.initialize()
        intent, key = register_first(journal, register_step=False)
        assert journal.live_steps(intent.cycle_id) == []
        limit = runtime.settings.entropy_orders_per_min
        reserve = 0
        for _ in range(limit):
            assert journal.reserve_request(
                'entropy', now=time.time(), limit=limit,
                reserve_for_completion=reserve, completion=False)
        result = await runtime._step(
            journal.active_live_cycle(), purpose='open_first', attempt=0,
            venue_key='entropy', side=Side.SELL, units=85, reduce_only=False,
            room_bps=D('3'), hard_cap_bps=D('10'), next_phase='OPEN_HEDGE',
            completion=False)
        assert result.deferred and entropy.sent == hedge.sent == []
        with pytest.raises(KeyError):
            journal.live_step(key)
        with pytest.raises(KeyError):
            journal.order(intent.order_ref)
        return intent, key, limit, reserve

    intent, key, limit, reserve = asyncio.run(prepare())
    budget_rows = list(journal.db.execute('SELECT * FROM request_events'))
    journal.close()
    restarted, entropy2, hedge2, reopened = runtime_factory()

    async def resume():
        await restarted.initialize()
        await restarted._audit_positions()
        await restarted._resume_cycle(reopened.active_live_cycle())
        assert entropy2.sent == hedge2.sent == []
        with pytest.raises(KeyError):
            reopened.live_step(key)
        with pytest.raises(KeyError):
            reopened.order(intent.order_ref)
        assert list(reopened.db.execute('SELECT * FROM request_events')) == budget_rows
        assert not reopened.reserve_request(
            'entropy', now=time.time(), limit=limit,
            reserve_for_completion=reserve, completion=False)

    asyncio.run(resume())
