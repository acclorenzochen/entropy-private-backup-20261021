"""Offline AB integration through production adapters; transports/signers are fake."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import itertools
import json
import math
import time
from types import SimpleNamespace

import pytest

import entropy_arb.venue_hl as hl_module
import entropy_arb.v3.journal as journal_module
import entropy_arb.v3.live_host as host_module
import entropy_arb.v3.runtime as runtime_module
from entropy_arb.venue_hl import HLOrdersFeed, HLVenue
from entropy_arb.venue_lighter import LighterVenue
from entropy_arb.v3.journal import Journal
from entropy_arb.v3.runtime import LiveRuntime, RuntimeVenues, choose_open_plan
from tests_v3.test_lighter_request_integration import (
    Response, account_payload, install_signer, make_venue, no_network,
)
from tests_v3.test_runtime import complete_background_audit, setup_runtime


@pytest.fixture(autouse=True)
def isolated_cwd_and_hl_network_guard(tmp_path, monkeypatch, no_network):
    monkeypatch.chdir(tmp_path)
    def forbidden(*args, **kwargs):
        raise AssertionError('AB adapter tests must never open a real websocket')
    monkeypatch.setattr(hl_module, 'ws_connect', forbidden)


async def noop(*args, **kwargs):
    return None


class AdapterTransport:
    """Fake exchange outcomes behind the unmodified real send_taker methods."""

    def __init__(self):
        self.venues = {}
        self.journal = None
        self.records = []
        self.positions = {'entropy': D('0'), 'hedge': D('0')}
        self.outcomes = {'entropy': [], 'hedge': []}
        self.entered = {key: asyncio.Event() for key in self.outcomes}
        self.release = asyncio.Event()
        self.release.set()
        self.sequence = itertools.count(1)

    def gate_opening(self):
        self.release.clear()

    def account(self):
        position = self.positions['hedge']
        payload = account_payload(str(abs(position)), equity='1000')
        payload['accounts'][0]['positions'][0]['sign'] = -1 if position < 0 else 1
        return payload

    def lookup(self, path, count):
        if path == '/api/v1/account':
            return Response(self.account())
        if path == '/api/v1/accountOrders':
            return Response({'orders': []})
        raise AssertionError(f'unexpected fake Lighter GET: {path}')

    async def enter(self, key, *, ref, qty, is_buy, reduce_only, limit):
        order = self.journal.order(ref)
        assert order['state'] == 'SUBMITTING'
        assert order['requested_units'] > 0
        e_orders = [row for row in self.journal.cycle_orders(order['cycle_id'])
                    if row['venue'] == 'entropy' and not row['reduce_only']]
        record = dict(venue=key, ref=ref, qty=D(str(qty)), is_buy=is_buy,
                      reduce_only=reduce_only, limit=D(str(limit)),
                      e_before=[(row['state'], row['filled_units']) for row in e_orders],
                      ordinal=next(self.sequence))
        self.records.append(record)
        self.entered[key].set()
        if not reduce_only:
            await self.release.wait()
        outcome = self.outcomes[key].pop(0) if self.outcomes[key] and not reduce_only else {}
        filled = (record['qty'] * D(str(outcome.get('fraction', 1)))).quantize(D('.0001'))
        venue = self.venues[key]
        if reduce_only:
            closable = max(D('0'), -D(str(venue.position)) if is_buy else D(str(venue.position)))
            filled = min(filled, closable)
        price = D(str(venue.book.best_ask() if is_buy else venue.book.best_bid()))
        self.positions[key] += filled if is_buy else -filled
        venue.position = float(self.positions[key])
        record.update(filled=filled, price=price, fault=outcome.get('fault'),
                      oid=next(self.sequence), terminal_released=False)
        return record

    def terminal(self, record, *, duplicate=False):
        key, ref = record['venue'], record['ref']
        venue = self.venues[key]
        status = 'filled' if record['filled'] == record['qty'] else 'canceled'
        if key == 'entropy':
            order = {'order': {'coin': venue.coin, 'cloid': ref, 'oid': record['oid'],
                              'origSz': str(record['qty']),
                              'sz': str(record['qty'] - record['filled'])}, 'status': status}
            fill = {'coin': venue.coin, 'oid': record['oid'], 'tid': record['oid'],
                    'sz': str(record['filled']), 'px': str(record['price'])}
            venue.orders_feed._handle_orders([order])
            if record['filled']:
                venue.orders_feed._handle_fills({'fills': [fill]})
            if duplicate:
                venue.orders_feed._handle_orders([order])
                if record['filled']:
                    venue.orders_feed._handle_fills({'fills': [fill]})
        else:
            row = {'client_order_index': int(ref), 'status': status,
                   'filled_base_amount': str(record['filled']),
                   'filled_quote_amount': str(record['filled'] * record['price'])}
            venue.orders_feed._handle_orders({'orders': {'139': [row]}})
            if duplicate:
                venue.orders_feed._handle_orders({'orders': {'139': [row]}})
        record['terminal_released'] = True

    async def lighter_post(self, method, path):
        assert (method, path) == ('POST', '/api/v1/sendTx')
        venue = self.venues['hedge']
        order = venue.signer.orders[-1]
        record = await self.enter('hedge', ref=str(order['client_order_index']),
            qty=D(order['base_amount']) / (10 ** venue.size_decimals),
            limit=D(order['price']) / (10 ** venue.price_decimals),
            is_buy=not order['is_ask'], reduce_only=order['reduce_only'])
        assert order['time_in_force'] == 0
        assert order['order_type'] == 1
        if not record['fault']:
            self.terminal(record)
        if record['fault'] == 'ack_loss':
            raise ConnectionResetError('fake ACK lost after POST')

    def hl_post(self, url, **kwargs):
        payload = kwargs['json']
        transport = self
        wire = payload['action']['orders'][0]
        assert wire['order_type'] == {'limit': {'tif': 'Ioc'}}

        class Reply:
            status = 200
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
            async def text(self):
                record = await transport.enter('entropy', ref=wire['cloid'],
                    qty=wire['sz'], limit=wire['limit_px'], is_buy=wire['is_buy'],
                    reduce_only=wire['reduce_only'])
                if record['fault'] == 'ack_loss':
                    raise ConnectionResetError('fake ACK lost after POST')
                if record['fault'] == 'unknown':
                    return json.dumps({'status': 'ok', 'response': {'data': {'statuses': [{'resting': {}}]}}})
                stanza = ({'filled': {'totalSz': str(record['filled']),
                                     'avgPx': str(record['price']), 'oid': record['oid']}}
                          if record['filled'] else {'error': 'Order could not immediately match'})
                return json.dumps({'status': 'ok', 'response': {'data': {'statuses': [stanza]}}})
        return Reply()


def real_adapters(monkeypatch, transport):
    h, _, budget, clock = make_venue(monkeypatch, transport.lookup, feed=True)
    clock.wall, clock.mono = time.time(), time.monotonic()
    origin = time.monotonic()
    clock.time = lambda: clock.wall + time.monotonic() - origin
    clock.monotonic = lambda: clock.mono + time.monotonic() - origin
    clock.monotonic_ns = lambda: int(clock.monotonic() * 1e9)
    for module in (hl_module, runtime_module, journal_module, host_module):
        monkeypatch.setattr(module, 'time', clock)
    h.conf.cap_usd, h.cap_usd = 200, 200
    e_conf = SimpleNamespace(key='entropy', label='hl', symbol='SNDK', fee_bps=.5,
                            cap_usd=200, orders_per_min=6, hl_dex='io', leverage=None)
    e = HLVenue(e_conf, 'https://hl.invalid', 'unused',
                SimpleNamespace(post=transport.hl_post), settle_timeout_sec=.001)
    e.coin, e.asset_id, e.size_decimals = 'io:SNDK', 1, 4
    nonce = itertools.count(1)
    e.account = SimpleNamespace(wallet=None, is_mainnet=False,
                                query_address='0xabc', nonces=SimpleNamespace(next=lambda: next(nonce)))
    e._next_cloid = lambda ref: SimpleNamespace(to_raw=lambda: ref)
    e._signing = SimpleNamespace(
        order_request_to_order_wire=lambda request, asset: {**request, 'cloid': request['cloid'].to_raw()},
        order_wires_to_order_action=lambda wires: {'orders': wires},
        sign_l1_action=lambda *args: {})
    e.orders_feed = HLOrdersFeed('test', 'unused', '0xabc', e.coin)
    for venue, bid, ask in ((e, '99.90', '99.91'), (h, '100', '100.01')):
        venue.min_base, venue.min_quote, venue.price_decimals = .0001, 1, 2
        venue.load_market = noop
        venue.init_signer = lambda: None
        venue.start_tasks = lambda *args, **kwargs: []
        venue.ready_to_trade = lambda: True
        venue.book.apply_hl([[{'px': bid, 'sz': '5'}], [{'px': ask, 'sz': '5'}]])
        venue.orders_feed.connection_generation = 1
        venue.orders_feed.ready.set()
        venue.position = float(transport.positions[venue.key])
        venue.book.last_update = clock.monotonic()
    h.prepare_signer = noop
    h.orders_feed.last_liveness_ts = clock.wall
    position = transport.positions['hedge']
    h.orders_feed._handle_account({'type': 'subscribed/account_all',
        'positions': {'139': {'market_id': 139, 'position': str(abs(position)),
                              'sign': -1 if position < 0 else 1}},
        'assets': {'3': {'margin_balance': '1000', 'multiplier': '1'}}})
    transport.venues = {'entropy': e, 'hedge': h}
    signer, rest = install_signer(h, callback=transport.lighter_post)

    async def info(payload):
        assert payload['type'] in {'exchangeStatus', 'orderStatus'}
        return {'status': 'unknownOid'} if payload['type'] == 'orderStatus' else {}
    e._info = info
    async def position():
        return float(transport.positions['entropy'])
    # Metadata and account data are fake; execution and same-ID recovery use
    # the production adapter methods, including HTTP/WS parsing.
    e.fetch_position = position
    async def equity():
        return (1000, 1000)
    e.fetch_equity = equity
    for venue in (e, h):
        async def fees(since_ts, until_ts=None, key=venue.key, fee_bps=D(str(venue.fee_bps))):
            return [dict(order_ref=row['ref'], ts=clock.time(),
                exchange_fill_key=f"offline:{key}:{row['ref']}",
                side='buy' if row['is_buy'] else 'sell', qty=float(row['filled']),
                price=float(row['price']),
                fee_usd=float(row['filled'] * row['price'] * fee_bps / 10000))
                for row in transport.records if row['venue'] == key and row.get('filled')]
        venue.fetch_fee_fills = fees
        async def funding(*args, **kwargs):
            return 0.
        venue.fetch_funding_payments = funding
    return e, h, budget, clock, rest


def adapter_runtime(tmp_path, monkeypatch, *, transport=None, journal=None):
    transport = transport or AdapterTransport()
    old, _, _, initial_journal = setup_runtime(tmp_path)
    if journal is not None:
        initial_journal.close()
    else:
        journal = initial_journal
        journal.ensure_batch('ordinary-baseline', max_position_cycles=100, loss_budget_usd=D('3'))
    e, h, budget, clock, rest = real_adapters(monkeypatch, transport)
    settings = replace(old.settings, opening_experiment_id='ab-test',
        max_notional_usd=D('200'), open_first_rooms_bps=(D('3'), D('3'), D('3')),
        open_first_hard_cap_bps=D('10'), hedge_rooms_bps=(D('4'), D('8'), D('12')),
        hedge_hard_cap_bps=D('12'), exit_min_gross_profit_bps=D('2.5'),
        single_cycle_review_loss_usd=None)
    authorization = replace(old.authorization, max_position_cycles=20,
        loss_budget_usd=None, max_notional_usd=D('200'), opening_experiment_id='ab-test')
    runtime = LiveRuntime(settings=settings, legacy_config=old.cfg,
        authorization=authorization, pair_id=old.pair_id, deployment=old.deployment,
        venues=RuntimeVenues(e, h), journal=journal, status=old.status,
        observer=old.observer, stop=old.stop,
        reference_checkpoint_path=old.reference_checkpoint_path)
    transport.journal = journal
    assert e.send_taker.__func__ is HLVenue.send_taker
    assert h.send_taker.__func__ is LighterVenue.send_taker
    return runtime, transport, journal, clock


def fresh_books(runtime, clock, *, short=False, exit=False):
    e_price = ('97' if short else '103') if exit else ('100.10' if short else '99.90')
    for venue, bid in ((runtime.venues.entropy, D(e_price)),
                       (runtime.venues.hedge, D('100'))):
        venue.book.apply_hl([[{'px': str(bid), 'sz': '5'}],
                             [{'px': str(bid + D('.01')), 'sz': '5'}]])
        venue.book.last_update = clock.monotonic()
        venue.book.alive_since = clock.monotonic() - 1
    feed = runtime.venues.hedge.orders_feed
    feed.last_liveness_ts = clock.time()


async def finish_cycle(runtime, transport, clock):
    active = runtime.journal.active_live_cycle()
    assert active is not None
    cycle_id = active['cycle_id']
    frozen = active.get('basis_profit_basis_json')
    fresh_books(runtime, clock, short=active['direction'] == 'short_entropy', exit=True)
    for _ in range(12):
        await runtime._resolve_pending()
        await complete_background_audit(runtime)
        active = runtime.journal.active_live_cycle()
        if active is None:
            break
        if active['phase'] == 'OPEN' and not runtime.journal.pending():
            assert_transport_profit_basis(runtime, transport, active)
            frozen = frozen or active['basis_profit_basis_json']
        await runtime._resume_cycle(active)
    await complete_background_audit(runtime, accounting=True)
    assert runtime.journal.active_live_cycle() is None
    orders = runtime.journal.cycle_orders(cycle_id)
    assert orders and all(row['state'] == 'TERMINAL' for row in orders)
    assert transport.positions == {'entropy': D('0'), 'hedge': D('0')}
    assert all(sum((1 if row['side'] == 'buy' else -1) * row['filled_units']
                   for row in orders if row['venue'] == key) == 0
               for key in transport.positions)
    attempt = runtime.journal.opening_attempt(cycle_id)
    assert attempt['terminal_flat']
    if frozen:
        assert runtime.journal.db.execute(
            'SELECT basis_profit_basis_json FROM live_cycles WHERE cycle_id=?',
            (cycle_id,)).fetchone()[0] == frozen
    return cycle_id


def assert_transport_profit_basis(runtime, transport, active):
    """Check the exact denominator from independently normalized adapter fills."""
    assert active['phase'] == 'OPEN' and not runtime.journal.pending()
    basis = json.loads(active['basis_profit_basis_json'])
    matched = runtime.instrument.quantity(abs(active['entropy_units']))
    refs = {row['order_ref'] for row in runtime.journal.cycle_orders(active['cycle_id'])}
    inventory = {key: [D('0'), D('0')] for key in ('entropy', 'hedge')}
    for row in transport.records:
        if row['ref'] not in refs or not row['filled']:
            continue
        qty, cost = inventory[row['venue']]
        if row['reduce_only']:
            assert row['filled'] <= qty
            inventory[row['venue']] = [qty - row['filled'], cost * (qty - row['filled']) / qty]
        else:
            price = row['price']
            if row['venue'] == 'hedge':
                # The production feed divides wire quote/base as IEEE floats;
                # the host stores Decimal(str(avg_px)), not the ideal wire price.
                average = float(row['filled'] * price) / float(row['filled'])
                report = runtime.venues.hedge.orders_feed.terminal(int(row['ref']))
                assert report is not None and report['avg_px'] == average
                assert D(str(report['filled_base'])) == row['filled']
                price = D(str(average))
                assert abs(price - row['price']) <= 2 * D.from_float(math.ulp(float(row['price'])))
            order = runtime.journal.order(row['ref'])
            assert D(order['avg_px']) == price
            assert order['filled_units'] == runtime.instrument.exact_units(row['filled'])
            inventory[row['venue']] = [qty + row['filled'], cost + row['filled'] * price]
    assert all(qty == matched for qty, _ in inventory.values())
    expected = sum((cost for _, cost in inventory.values()), D('0')) / 2
    assert basis['matched_units'] == abs(active['entropy_units'])
    assert D(basis['entry_notional_usd']) == expected
    assert D(basis['quantity_step']) == runtime.instrument.quantity_step


async def prepare_arm(runtime, transport, clock, arm, *, short=False):
    await runtime.initialize()
    if arm == 'B':
        await runtime._maybe_open(runtime._market(), D('0'))
        assert runtime.journal.opening_experiment_state('ab-test')['current_attempt']['group_name'] == 'A'
        await finish_cycle(runtime, transport, clock)
        clock.advance(61)
        runtime.journal.rearm_opening_opportunity('ab-test')
        transport.records.clear()
    transport.entered = {key: asyncio.Event() for key in transport.entered}
    fresh_books(runtime, clock, short=short)
    await runtime._audit_positions()


@pytest.mark.parametrize('arm', ['A', 'B'])
@pytest.mark.parametrize('short', [False, True])
def test_real_adapters_share_plan_and_policy_but_change_opening_topology(tmp_path, monkeypatch, arm, short):
    runtime, transport, journal, clock = adapter_runtime(tmp_path, monkeypatch)
    async def scenario():
        await prepare_arm(runtime, transport, clock, arm, short=short)
        plan = choose_open_plan(runtime._market(), runtime.instrument, runtime.settings, D('0'),
            entropy_venue=runtime.venues.entropy, hedge_venue=runtime.venues.hedge)
        assert plan is not None
        transport.gate_opening()
        task = asyncio.create_task(runtime._maybe_open(runtime._market(), D('0')))
        try:
            await asyncio.wait_for(transport.entered['entropy'].wait(), 1)
            if arm == 'A':
                await asyncio.wait_for(transport.entered['hedge'].wait(), 1)
                pending = journal.pending()
                assert len(pending) == 2 and all(row['state'] == 'SUBMITTING' for row in pending)
                assert {row['venue'] for row in transport.records} == {'entropy', 'hedge'}
                assert all('filled' not in row for row in transport.records)
            else:
                assert [row['venue'] for row in transport.records] == ['entropy']
                assert len(journal.pending()) == 1
        finally:
            transport.release.set()
        await asyncio.wait_for(task, 2)
        opening = [row for row in transport.records if not row['reduce_only']]
        assert len(opening) == 2
        assert all(row['qty'] == runtime.instrument.quantity(plan.units) for row in opening)
        assert {row['is_buy'] for row in opening} == {False, True}
        h = next(row for row in opening if row['venue'] == 'hedge')
        if arm == 'B':
            assert h['e_before'] == [('TERMINAL', plan.units)]
        else:
            assert h['e_before'] == [('SUBMITTING', 0)]
        attempt = journal.opening_experiment_state('ab-test')['current_attempt']
        assert attempt['group_name'] == arm
        active = journal.active_live_cycle()
        assert active['planned_units'] == plan.units
        assert active['reference_bps'] == str(plan.reference_bps)
        assert abs(active['entropy_units']) == abs(active['hedge_units']) == plan.units
        assert_transport_profit_basis(runtime, transport, active)
        frozen_basis = active['basis_profit_basis_json']
        economics = journal.economics_steps(active['cycle_id'])
        assert len(economics) == 2
        for step in economics:
            assert step['execution_path'] in {'parallel', 'standard'}
            assert step['execution_path'] != 'recovered'
            assert step['decision_units'] == plan.units
            quote = json.loads(step['decision_quote_json'])
            assert quote['units'] == plan.units and quote['entropy'] and quote['hedge']
            assert step['attempt_decision_ts'] is not None
            assert step['attempt_decision_mono_ns'] is not None
            assert step['attempt_decision_basis_bps'] is not None
            assert step['top_px'] and step['marginal_px'] and step['limit_px']
            assert D(step['room_bps']) == D('3' if step['venue'] == 'entropy' else '4')
            assert D(step['hard_cap_bps']) == D('10' if step['venue'] == 'entropy' else '12')
            record = next(row for row in opening if row['ref'] == step['order_ref'])
            assert record['limit'] == D(step['limit_px'])
        assert runtime.settings.open_first_rooms_bps == (D('3'),) * 3
        assert runtime.settings.hedge_rooms_bps == (D('4'), D('8'), D('12'))
        assert runtime.settings.exit_min_gross_profit_bps == D('2.5')
        assert runtime.instrument.entropy_fee_bps == D('.5')
        assert runtime.instrument.hedge_fee_bps == D('0')
        assert journal.batch_state('ordinary-baseline')['loss_budget_usd'] == D('3')
        count = len(transport.records)
        await runtime._resume_cycle(active)
        assert len(transport.records) == count
        assert journal.active_live_cycle()['basis_exit_requested_ts'] is None
        assert journal.active_live_cycle()['basis_profit_basis_json'] == frozen_basis
        await finish_cycle(runtime, transport, clock)
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('arm', ['A', 'B'])
@pytest.mark.parametrize('fractions', [(.5, 1), (1, .5)])
def test_real_adapter_partial_fills_reconcile_exact_residual_without_flipping(tmp_path, monkeypatch, arm, fractions):
    runtime, transport, journal, clock = adapter_runtime(tmp_path, monkeypatch)
    async def scenario():
        await prepare_arm(runtime, transport, clock, arm)
        for key, fraction in zip(('entropy', 'hedge'), fractions):
            transport.outcomes[key] = [{'fraction': fraction}]
        await runtime._maybe_open(runtime._market(), D('0'))
        for _ in range(6):
            await runtime._audit_positions()
            active = journal.active_live_cycle()
            if active['phase'] == 'OPEN' and active['entropy_units'] + active['hedge_units'] == 0:
                break
            await runtime._resume_cycle(active)
        primary = [row for row in transport.records if not row['reduce_only']][:2]
        e = next(row for row in primary if row['venue'] == 'entropy')
        h = next(row for row in primary if row['venue'] == 'hedge')
        assert h['qty'] == (e['qty'] if arm == 'A' else e['filled'])
        if arm == 'A' and fractions[0] < fractions[1]:
            repair = [row for row in transport.records if row['venue'] == 'hedge' and row['reduce_only']]
            assert repair and repair[0]['qty'] == h['filled'] - e['filled']
            assert repair[0]['is_buy']
        active = journal.active_live_cycle()
        assert active['entropy_units'] > 0 and active['hedge_units'] < 0
        assert active['entropy_units'] + active['hedge_units'] == 0
        assert not journal.pending()
        assert_transport_profit_basis(runtime, transport, active)
        assert all(row['filled_units'] <= row['requested_units'] for row in journal.cycle_orders(active['cycle_id']))
        await finish_cycle(runtime, transport, clock)
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('arm', ['A', 'B'])
@pytest.mark.parametrize('leg', ['entropy', 'hedge'])
@pytest.mark.parametrize('fault', ['unknown', 'ack_loss'])
def test_real_adapter_unknown_and_lost_ack_resolve_original_id_once(tmp_path, monkeypatch, arm, leg, fault):
    runtime, transport, journal, clock = adapter_runtime(tmp_path, monkeypatch)
    async def scenario():
        await prepare_arm(runtime, transport, clock, arm)
        transport.outcomes[leg] = [{'fault': fault, 'fraction': .5}]
        if arm == 'A':
            transport.gate_opening()
            sending = asyncio.create_task(runtime._maybe_open(runtime._market(), D('0')))
            try:
                await asyncio.wait_for(asyncio.gather(*(event.wait() for event in transport.entered.values())), 1)
            finally:
                transport.release.set()
            await sending
        else:
            await runtime._maybe_open(runtime._market(), D('0'))
        original = next(row for row in transport.records if row['venue'] == leg and row['fault'] == fault)
        pending = journal.pending()
        assert any(row['order_ref'] == original['ref'] for row in pending)
        allocated = journal.opening_experiment_state('ab-test')['attempts']
        sent = [(row['ref'], row['reduce_only']) for row in transport.records]
        for _ in range(2):
            await runtime._resolve_pending()
            fresh_books(runtime, clock)
            await runtime._audit_positions()
            await runtime._resume_cycle(journal.active_live_cycle())
        assert all(row['reduce_only'] for row in transport.records[len(sent):])
        assert sum(row['ref'] == original['ref'] for row in transport.records) == 1
        if arm == 'B' and leg == 'entropy':
            assert all(row['venue'] == 'entropy' for row in transport.records)
        transport.terminal(original, duplicate=True)
        await runtime._resolve_pending()
        await runtime._resolve_pending()
        assert journal.order(original['ref'])['state'] == 'TERMINAL'
        assert journal.order(original['ref'])['filled_units'] == runtime.instrument.exact_units(original['filled'])
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        assert journal.opening_experiment_state('ab-test')['attempts'] == allocated
        assert sum(row['ref'] == original['ref'] for row in transport.records) == 1
        await finish_cycle(runtime, transport, clock)
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_twentieth_attempt_stops_entry_and_real_adapters_finish_existing_exposure(tmp_path, monkeypatch):
    runtime, transport, journal, clock = adapter_runtime(tmp_path, monkeypatch)
    async def scenario():
        await runtime.initialize()
        for slot in range(1, 21):
            if slot > 1:
                clock.advance(61)
                journal.rearm_opening_opportunity('ab-test')
            fresh_books(runtime, clock)
            await runtime._audit_positions()
            await runtime._maybe_open(runtime._market(), D('0'))
            state = journal.opening_experiment_state('ab-test')
            assert state['attempts'] == slot
            assert state['current_attempt']['group_name'] == ('A' if slot % 2 else 'B')
            assert journal.active_live_cycle() is not None
            if slot == 20:
                assert not state['finished']
                assert not journal.batch_state(runtime.authorization.batch_id)['can_open']
            await finish_cycle(runtime, transport, clock)
        state = journal.opening_experiment_state('ab-test')
        assert state['finished'] and state['groups'] == {'A': 10, 'B': 10}
        count = len(transport.records)
        journal.rearm_opening_opportunity('ab-test')
        fresh_books(runtime, clock)
        await runtime._maybe_open(runtime._market(), D('0'))
        assert len(transport.records) == count
        assert journal.opening_experiment_state('ab-test')['attempts'] == 20
        assert journal.batch_state('ordinary-baseline')['loss_budget_usd'] == D('3')
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_restarted_real_adapters_resolve_both_paired_intents_without_new_send_or_slot(tmp_path, monkeypatch):
    runtime, transport, journal, clock = adapter_runtime(tmp_path, monkeypatch)
    reopened = None
    async def scenario():
        nonlocal reopened
        await runtime.initialize()
        for key in transport.outcomes:
            transport.outcomes[key] = [{'fault': 'ack_loss', 'fraction': .5}]
        transport.gate_opening()
        sending = asyncio.create_task(runtime._maybe_open(runtime._market(), D('0')))
        try:
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in transport.entered.values())), 1)
            assert len(journal.pending()) == 2
        finally:
            transport.release.set()
        await sending
        refs = {row['order_ref'] for row in journal.pending()}
        assert len(refs) == 2
        current = journal.opening_experiment_state('ab-test')['current_attempt']
        cycle_id, slot = current['cycle_id'], current['slot']
        sent = len(transport.records)
        await runtime.venues.hedge.wait_for_submission()
        journal.close()
        reopened = Journal(tmp_path / 'runtime.sqlite', identity='pair-account')
        e, h, _, new_clock, _ = real_adapters(monkeypatch, transport)
        restarted = LiveRuntime(settings=runtime.settings, legacy_config=runtime.cfg,
            authorization=runtime.authorization, pair_id=runtime.pair_id,
            deployment=runtime.deployment, venues=RuntimeVenues(e, h), journal=reopened,
            status=runtime.status, observer=runtime.observer, stop=asyncio.Event(),
            reference_checkpoint_path=runtime.reference_checkpoint_path)
        transport.journal = reopened
        await restarted.initialize()
        assert {row['order_ref'] for row in reopened.pending()} == refs
        for row in transport.records:
            transport.terminal(row, duplicate=True)
        await restarted._resolve_pending()
        await restarted._resolve_pending()
        fresh_books(restarted, new_clock)
        await restarted._audit_positions()
        await restarted._resume_cycle(reopened.active_live_cycle())
        assert not reopened.pending()
        assert len(transport.records) == sent
        assert all(sum(row['ref'] == ref for row in transport.records) == 1 for ref in refs)
        recovered = reopened.opening_attempt(cycle_id)
        assert recovered['slot'] == slot and recovered['group_name'] == 'A'
        assert reopened.opening_experiment_state('ab-test')['attempts'] == 1
        cycle = reopened.active_live_cycle()
        expected = restarted.instrument.exact_units(transport.positions['entropy'])
        assert (cycle['entropy_units'], cycle['hedge_units']) == (expected, -expected)
        await finish_cycle(restarted, transport, new_clock)
    try:
        asyncio.run(scenario())
    finally:
        if reopened is not None:
            reopened.close()
        else:
            journal.close()


@pytest.mark.parametrize('change', ['unknown_peer', 'external_fill'])
def test_real_sdk_request_guard_blocks_peer_post_after_last_nonce_wait(tmp_path, monkeypatch, change):
    runtime, transport, journal, clock = adapter_runtime(tmp_path, monkeypatch)
    async def scenario():
        await runtime.initialize()
        h = runtime.venues.hedge
        h.signer.nonce_manager.nonce.clear()
        entered, release = asyncio.Event(), asyncio.Event()
        rest = h.signer.api_client.rest_client
        async def request(method, path):
            if (method, path) == ('GET', '/api/v1/nextNonce'):
                entered.set()
                await release.wait()
            else:
                await transport.lighter_post(method, path)
        rest.callback = request
        if change == 'unknown_peer':
            transport.outcomes['entropy'] = [{'fault': 'ack_loss', 'fraction': .5}]
        sending = asyncio.create_task(runtime._maybe_open(runtime._market(), D('0')))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            await asyncio.wait_for(transport.entered['entropy'].wait(), 1)
            async def first_result():
                while True:
                    row = journal.cycle_orders(journal.active_live_cycle()['cycle_id'])[0]
                    if row['state'] == ('UNKNOWN' if change == 'unknown_peer' else 'TERMINAL'):
                        return row
                    await asyncio.sleep(0)
            original = await asyncio.wait_for(first_result(), 2)
            if change == 'external_fill':
                e = runtime.venues.entropy
                transport.positions['entropy'] += D('.0001')
                e.position = float(transport.positions['entropy'])
                e.orders_feed._handle_fills({'fills': [{
                    'coin': e.coin, 'oid': 999999, 'tid': 999999,
                    'sz': '.0001', 'px': '99.91'}]})
            fresh_books(runtime, clock)
        finally:
            release.set()
        await asyncio.wait_for(sending, 2)
        await h.wait_for_submission()
        assert rest.calls == [('GET', '/api/v1/nextNonce')]
        assert not any(row['venue'] == 'hedge' for row in transport.records)
        assert len(h.signer.orders) == 1
        assert sum(row['ref'] == original['order_ref'] for row in transport.records) == 1
        assert journal.opening_experiment_state('ab-test')['attempts'] == 1
        assert not any(row['venue'] == 'hedge' and row['state'] == 'UNKNOWN' for row in journal.pending())
    try:
        asyncio.run(scenario())
    finally:
        journal.close()
