"""Last-dispatch guard checks use fake signing, HTTP and temporary journals."""
import asyncio
import json
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from entropy_arb.v3.core import Instrument, Side
from entropy_arb.v3.journal import Intent, Journal
from entropy_arb.v3.live_host import AuditedLiveExecutor, VenueAdapterBroker
from entropy_arb.venue_hl import HLOrdersFeed, HLVenue


REF = '0x' + '12' * 16
REJECTION = {'status': 'err', 'response': 'Order could not immediately match'}


def venue_with_fake_http(*, status=200, body=REJECTION):
    venue = HLVenue.__new__(HLVenue)
    venue.account = SimpleNamespace(
        wallet=None, is_mainnet=False, nonces=SimpleNamespace(next=lambda: 1))
    venue.asset_id, venue.coin = 1, 'io:SNDK'
    venue._signing = SimpleNamespace(
        order_request_to_order_wire=lambda *args: {},
        order_wires_to_order_action=lambda *args: {},
        sign_l1_action=lambda *args: {})
    venue._next_cloid = lambda ref: SimpleNamespace(to_raw=lambda: ref)
    venue.orders_feed, venue._submit_tasks, venue._exchange_timings = None, set(), {}
    venue.api_url = 'unused'
    posts = []

    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def text(self):
            return json.dumps(body)

    response = Response()
    response.status = status

    def post(*args, **kwargs):
        posts.append(kwargs['json'])
        return response

    venue.session = SimpleNamespace(post=post)
    return venue, posts


def sender(journal, venue):
    broker = VenueAdapterBroker(
        venue, venue_key='entropy', venue_kind='hl',
        instrument=Instrument('pair', D('.001'), D('.01'), D('.01'), D('0'), D('0')))
    return AuditedLiveExecutor(journal, {'entropy': broker, 'hedge': broker})


def test_task_boundary_guard_change_blocks_http_and_refunds_logical_ticket(tmp_path):
    async def run(journal):
        venue, posts = venue_with_fake_http()
        feed = HLOrdersFeed('test', 'unused', '0xabc', venue.coin)
        venue.orders_feed = feed
        old_watch = feed.watch
        state = {'allowed': True}
        checks = []
        waiting = []

        def watch(ref):
            future = old_watch(ref)
            waiting.append(future)
            # Runs after durable intent/signing but before the POST task starts.
            asyncio.get_running_loop().call_soon(lambda: state.update(allowed=False))
            return future

        feed.watch = watch

        def guard(ref):
            checks.append(ref)
            assert journal.order(ref)['state'] == 'SUBMITTING'
            assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 1
            assert not state['allowed']
            assert not waiting[0].cancelled()
            return 'quote_changed'

        venue.submission_guard = guard
        order = Intent(REF, 'c', 'entropy', Side.BUY, 85, D('100'))

        def persist():
            assert state['allowed']
            return journal.prepare_dispatch(
                order, step={'step_key': 'c:open_first:0', 'quantity_step': D('.001')},
                request=dict(now=100, limit=6, reserve_for_completion=0,
                             completion=False, request_kind='open_first'))

        result = await sender(journal, venue).send(order, persist_dispatch=persist)
        assert result.deferred and not result.terminal and result.cumulative_units == 0
        assert result.note == 'submission_guard:quote_changed'
        assert result.retry_after == 0 and posts == [] and checks == [REF]
        assert journal.pending() == [] and journal.live_steps('c') == []
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 0
        with pytest.raises(KeyError):
            journal.order(REF)
        assert not venue._submit_tasks and not feed._pending

    journal = Journal(tmp_path / 'journal.sqlite', identity='offline')
    try:
        journal.ensure_batch('b', max_position_cycles=10, loss_budget_usd=D('3'))
        journal.create_live_cycle(batch_id='b', cycle_id='c', direction='long_entropy',
                                  planned_units=85, reference_bps=D('0'),
                                  entry_basis_bps=D('-4'), started_ts=100)
        asyncio.run(run(journal))
    finally:
        journal.close()


@pytest.mark.parametrize('guard_mode', ['absent', 'allow', 'reduce_only'])
def test_allowed_and_reduce_only_orders_keep_http_terminal_semantics(guard_mode):
    async def run():
        venue, posts = venue_with_fake_http()
        checks = []
        if guard_mode != 'absent':
            def guard(ref):
                checks.append(ref)
                return 'blocked' if guard_mode == 'reduce_only' else None
            venue.submission_guard = guard
        result = await venue.send_taker(
            is_buy=True, qty=.085, limit_px=100, order_ref=REF,
            reduce_only=guard_mode == 'reduce_only')
        assert len(posts) == 1 and result['order_ref'] == REF
        assert result['status'] == 'send-failed' and not result['unresolved']
        assert not result.get('deferred')
        assert checks == ([REF] if guard_mode == 'allow' else [])
    asyncio.run(run())


def test_signing_failure_remains_terminal_zero_without_guard_or_http():
    async def run():
        venue, posts = venue_with_fake_http()

        def fail(*args):
            raise ValueError('fake signing failure')

        venue._signing.sign_l1_action = fail
        venue.submission_guard = lambda ref: pytest.fail('guard runs only after signing')
        result = await venue.send_taker(is_buy=True, qty=.085, limit_px=100, order_ref=REF)
        assert result['status'] == 'send-failed' and result['filled_base'] == 0
        assert 'signing failed' in result['err'] and not result['unresolved']
        assert not result.get('deferred') and posts == []
    asyncio.run(run())


def test_post_unknown_preserves_original_id_and_never_becomes_deferred():
    async def run():
        venue, posts = venue_with_fake_http(status=503, body={})
        checks, resolved = [], []
        venue.submission_guard = lambda ref: checks.append(ref)

        async def resolve(ref, timeout=None):
            resolved.append(ref)
            venue.submission_guard = lambda ref: 'blocked_after_post'
            return {'status': 'timeout', 'filled_base': 0, 'avg_px': None,
                    'unresolved': True, 'order_ref': ref}

        venue.resolve_order = resolve
        result = await venue.send_taker(is_buy=True, qty=.085, limit_px=100, order_ref=REF)
        assert len(posts) == 1 and checks == resolved == [REF]
        assert result['unresolved'] and result['order_ref'] == REF
        assert not result.get('deferred') and not result.get('proved_local_unsent')
    asyncio.run(run())


def test_ws_confirmation_does_not_cancel_inflight_http_submission():
    async def run():
        venue, _ = venue_with_fake_http()
        feed = HLOrdersFeed('test', 'unused', '0xabc', venue.coin)
        venue.orders_feed = feed
        entered, finish = asyncio.Event(), asyncio.Event()
        venue.submission_guard = lambda ref: None

        async def post(payload):
            entered.set()
            await finish.wait()
            return REJECTION, None, False

        venue._post_exchange = post
        send = asyncio.create_task(venue.send_taker(
            is_buy=True, qty=.085, limit_px=100, order_ref=REF))
        await entered.wait()
        submission = next(iter(venue._submit_tasks))
        feed._resolve(REF, {'status': 'filled', 'filled_base': .085, 'avg_px': 100})
        result = await send
        assert result['confirmation_source'] == 'order_fills_ws'
        assert not submission.done() and not submission.cancelled()
        finish.set()
        await submission
        assert not venue._submit_tasks
    asyncio.run(run())
