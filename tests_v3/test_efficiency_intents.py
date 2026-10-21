import asyncio
from decimal import Decimal as D

import pytest

from entropy_arb.lighter_requests import RequestDeferred
from entropy_arb.v3.core import Instrument, Side
from entropy_arb.v3.journal import Intent, Journal, PendingOrder, Result
from entropy_arb.v3.live_host import AuditedLiveExecutor, VenueAdapterBroker


@pytest.fixture
def journal(tmp_path):
    ledger = Journal(tmp_path / 'journal.sqlite', identity='offline')
    ledger.ensure_batch('b', max_position_cycles=10, loss_budget_usd=D('3'))
    ledger.create_live_cycle(batch_id='b', cycle_id='c', direction='long_entropy',
                             planned_units=85, reference_bps=D('0'),
                             entry_basis_bps=D('-4'), started_ts=100)
    yield ledger
    ledger.close()


def intent(price='100'):
    return Intent('12345', 'c', 'hedge', Side.BUY, 85, D(price))


def prepare(journal, order, *, now=100, limit=6):
    return journal.prepare_dispatch(
        order, step={'step_key': 'c:open_hedge:0', 'quantity_step': D('.001')},
        request=dict(now=now, limit=limit, reserve_for_completion=0,
                     completion=True, request_kind='open_hedge'))


class Token:
    def __init__(self):
        self.releases = 0

    def release(self):
        self.releases += 1


class Venue:
    supports_dispatch_token = True

    def __init__(self, raw=None):
        self.raw = raw or {'status': 'canceled', 'filled_base': '0',
                           'unresolved': False, 'order_ref': '12345'}
        self.token = Token()
        self.prepared = []
        self.sent = []
        self.resolved = []
        self.on_submit = lambda: None
        self.prepare_error = None
        self.send_error = None

    async def prepare_dispatch(self, **kwargs):
        self.prepared.append(kwargs)
        if self.prepare_error is not None:
            raise self.prepare_error
        return self.token

    async def send_taker(self, **kwargs):
        self.sent.append(kwargs)
        self.on_submit()
        if self.send_error is not None:
            raise self.send_error
        return self.raw.copy()

    async def resolve_order(self, order_ref, timeout=None):
        self.resolved.append(order_ref)
        return self.raw.copy()


def executor(journal, venue):
    broker = VenueAdapterBroker(
        venue, venue_key='hedge', venue_kind='lighter',
        instrument=Instrument('pair', D('.001'), D('.01'), D('.01'), D('0'), D('0')))
    return AuditedLiveExecutor(journal, {'entropy': broker, 'hedge': broker})


def test_complete_dispatch_is_durable_in_one_transaction_before_adapter(journal):
    statements = []
    journal.db.set_trace_callback(statements.append)
    venue = Venue()

    def before_transport():
        assert journal.order('12345')['state'] == 'SUBMITTING'
        step = journal.live_step('c:open_hedge:0')
        assert step['baseline_entropy_units'] == step['baseline_hedge_units'] == 0
        assert journal.active_live_cycle()['phase'] == 'OPEN_FIRST'
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 1
        assert statements.count('BEGIN IMMEDIATE') == statements.count('COMMIT') == 1
        assert not journal.db.in_transaction

    venue.on_submit = before_transport
    result = asyncio.run(executor(journal, venue).send(
        intent(), persist_dispatch=lambda: prepare(journal, intent()), completion=True))
    assert result.terminal and result.cumulative_units == 0 and not result.deferred
    assert venue.sent[0]['dispatch_token'] is venue.token
    assert venue.sent[0]['completion'] is True
    assert venue.token.releases == 1


def test_atomic_dispatch_rolls_back_step_ticket_and_order_on_conflicting_order(journal):
    journal.prepare_submitting(Intent('67890', 'c', 'hedge', Side.BUY, 1, D('100')))
    with pytest.raises(PendingOrder):
        prepare(journal, intent())
    assert journal.live_steps('c') == []
    assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 0
    assert [row['order_ref'] for row in journal.pending()] == ['67890']


def test_logical_budget_denial_leaves_no_dispatch_and_releases_physical_token(journal):
    journal.reserve_request('hedge', now=100, limit=1, reserve_for_completion=0,
                            completion=True, request_kind='open_hedge', cycle_id='prior')
    venue = Venue()
    result = asyncio.run(executor(journal, venue).send(
        intent(), persist_dispatch=lambda: prepare(journal, intent(), limit=1)))
    assert result.deferred and not result.terminal
    assert venue.sent == [] and venue.token.releases == 1
    assert journal.pending() == [] and journal.live_steps('c') == []
    assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 1


def test_pre_admission_deferral_never_persists_an_order(journal):
    venue = Venue()
    venue.prepare_error = RequestDeferred('request_budget', 12)
    result = asyncio.run(executor(journal, venue).send(
        intent(), persist_dispatch=lambda: pytest.fail('must not persist before admission')))
    assert result == Result(False, 0, note='request_budget', deferred=True, retry_after=12)
    assert venue.sent == [] and journal.pending() == []


def test_proved_unsent_deferral_refunds_only_its_ticket_and_reuses_same_attempt(journal):
    journal.reserve_request('hedge', now=100, limit=6, reserve_for_completion=0,
                            completion=True, request_kind='recovery', cycle_id='other')
    venue = Venue({'status': 'deferred', 'filled_base': '0', 'order_ref': '12345',
                   'unresolved': False, 'deferred': True, 'post_started': False,
                   'proved_local_unsent': True, 'retry_after': 3})
    sender = executor(journal, venue)
    result = asyncio.run(sender.send(intent(), persist_dispatch=lambda: prepare(journal, intent())))
    assert result.deferred and not result.terminal
    assert journal.pending() == [] and journal.live_steps('c') == []
    assert journal.db.execute('SELECT cycle_id FROM request_events').fetchall()[0][0] == 'other'
    assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 1
    with pytest.raises(KeyError):
        journal.order('12345')
    venue.raw = {'status': 'canceled', 'filled_base': '0', 'order_ref': '12345', 'unresolved': False}
    repriced = intent('100.01')
    result = asyncio.run(sender.send(repriced, persist_dispatch=lambda: prepare(journal, repriced)))
    assert result.terminal and not result.deferred
    assert journal.live_step('c:open_hedge:0')['order_ref'] == '12345'
    assert journal.order('12345')['limit_px'] == '100.01'
    assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 2


@pytest.mark.parametrize('proof', [{'post_started': True, 'proved_local_unsent': True},
                                  {'post_started': False},
                                  {'post_started': False, 'proved_local_unsent': True,
                                   'filled_base': '.001'}])
def test_unproven_deferral_stays_unknown_and_keeps_ticket(journal, proof):
    venue = Venue({'status': 'deferred', 'filled_base': '0', 'order_ref': '12345',
                   'unresolved': False, 'deferred': True, 'retry_after': 1, **proof})
    result = asyncio.run(executor(journal, venue).send(
        intent(), persist_dispatch=lambda: prepare(journal, intent())))
    assert not result.terminal and not result.deferred
    assert journal.order('12345')['state'] == 'UNKNOWN'
    assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 1


def test_post_exception_keeps_stable_id_and_resolves_without_resending(journal):
    venue = Venue()
    venue.send_error = TimeoutError('POST outcome unknown')
    sender = executor(journal, venue)
    result = asyncio.run(sender.send(intent(), persist_dispatch=lambda: prepare(journal, intent())))
    assert not result.terminal and not result.deferred
    assert journal.order('12345')['state'] == 'UNKNOWN'
    with pytest.raises(PendingOrder):
        journal.discard_unsent_dispatch(intent(), request_event_ts=100)
    result = asyncio.run(sender.resolve(intent()))
    assert result.terminal and venue.resolved == ['12345'] and len(venue.sent) == 1


def test_cancellation_after_durable_intent_recovers_same_id_after_restart(tmp_path):
    path = tmp_path / 'journal.sqlite'
    ledger = Journal(path, identity='restart')
    venue = Venue()
    venue.send_error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(executor(ledger, venue).send(intent()))
    assert ledger.order('12345')['state'] == 'SUBMITTING'
    ledger.close()
    ledger = Journal(path, identity='restart')
    try:
        result = asyncio.run(executor(ledger, venue).resolve(intent()))
        assert result.terminal and venue.resolved == ['12345'] and len(venue.sent) == 1
    finally:
        ledger.close()


def test_deferred_cannot_be_recorded_as_terminal_accounting(journal):
    prepare(journal, intent())
    with pytest.raises(ValueError, match='not an order outcome'):
        journal.record('12345', Result(False, 0, deferred=True))
    assert journal.order('12345')['state'] == 'SUBMITTING'
    with pytest.raises(ValueError, match='proved unsent'):
        Result(True, 0, deferred=True)
