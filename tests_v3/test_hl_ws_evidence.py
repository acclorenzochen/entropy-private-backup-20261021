import asyncio
from types import SimpleNamespace

import pytest

from entropy_arb.venue_hl import HLOrdersFeed, HLVenue


def test_fill_snapshot_replay_by_tid_does_not_double_count():
    feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
    fill = {'coin': 'io:SNDK', 'oid': 12, 'tid': 91,
            'sz': '0.4', 'px': '101', 'time': 1000}
    feed._handle_fills({'isSnapshot': True, 'fills': [fill]})
    feed._handle_fills({'fills': [fill]})
    assert feed._fills[12]['filled_base'] == pytest.approx(.4)
    feed._handle_orders([{
        'order': {'coin': 'io:SNDK', 'cloid': '0x12', 'oid': 12,
                  'origSz': '1', 'sz': '0.6'}, 'status': 'canceled'}])
    assert feed.terminal('0x12')['filled_base'] == pytest.approx(.4)


def test_ws_fill_without_trade_identity_cannot_prove_terminal_quantity():
    feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
    fill = {'coin': 'io:SNDK', 'oid': 12, 'sz': '0.4', 'px': '101'}
    feed._handle_fills({'fills': [fill]})
    feed._handle_fills({'isSnapshot': True, 'fills': [fill]})
    feed._handle_orders([{
        'order': {'coin': 'io:SNDK', 'cloid': '0x12', 'oid': 12,
                  'origSz': '1', 'sz': '0.6'}, 'status': 'canceled'}])
    assert feed.terminal('0x12') is None


def test_ws_aggregate_must_match_order_cumulative_before_terminal():
    feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
    for tid in (1, 2):
        feed._handle_fills({'fills': [{
            'coin': 'io:SNDK', 'oid': 12, 'tid': tid,
            'sz': '0.4', 'px': '101'}]})
    feed._handle_orders([{
        'order': {'coin': 'io:SNDK', 'cloid': '0x12', 'oid': 12,
                  'origSz': '1', 'sz': '0.4'}, 'status': 'canceled'}])
    assert feed.terminal('0x12') is None


def test_multiple_watchers_survive_independent_unwatch():
    async def run():
        feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
        first, second = feed.watch('0x13'), feed.watch('0x13')
        feed.unwatch('0x13', first)
        assert first.cancelled() and not second.done()
        feed._resolve('0x13', {'status': 'filled'})
        assert (await second)['status'] == 'filled'
    asyncio.run(run())


def test_resolve_accepts_ws_terminal_during_http_poll():
    async def run():
        feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
        entered = asyncio.Event()
        venue = HLVenue.__new__(HLVenue)
        venue.account = SimpleNamespace(query_address='0xabc')
        venue.orders_feed = feed
        venue.settle_timeout = 2

        async def slow_info(_):
            entered.set()
            await asyncio.sleep(2)
            return None

        venue._info = slow_info
        task = asyncio.create_task(venue.resolve_order('0x14', timeout=1))
        await entered.wait()
        feed._resolve('0x14', {'status': 'filled', 'filled_base': 1, 'avg_px': 100})
        result = await asyncio.wait_for(task, timeout=.1)
        assert result['confirmation_source'] == 'order_fills_ws'
        assert not feed._pending
    asyncio.run(run())


def order_event(oid, status='open', remaining='0.6'):
    return {'order': {'coin': 'io:SNDK', 'cloid': f'0x{oid:x}', 'oid': oid,
                      'origSz': '1', 'sz': remaining}, 'status': status}


def fill_event(oid, tid=1, size='0.4'):
    return {'coin': 'io:SNDK', 'oid': oid, 'tid': tid, 'sz': size, 'px': '101'}


@pytest.mark.parametrize('fills_first', [True, False])
def test_over_capacity_active_evidence_survives_replay_and_late_terminal(fills_first):
    feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
    for oid in range(600):
        if not fills_first:
            feed._handle_orders([order_event(oid)])
        feed._handle_fills({'fills': [fill_event(oid)]})
    # Fills without order mapping are also unresolved evidence, not history.
    assert len(feed._fills) == 600 and len(feed._fill_ids) == 600
    if not fills_first:
        assert len(feed._orders) == 600
    for oid in range(600):
        feed._handle_fills({'isSnapshot': True, 'fills': [fill_event(oid)]})
        assert feed._fills[oid]['filled_base'] == pytest.approx(.4)
        feed._handle_orders([order_event(oid, 'canceled')])
        assert feed.terminal(f'0x{oid:x}')['filled_base'] == pytest.approx(.4)


def test_pending_and_unwatched_recovery_evidence_stays_pinned_over_capacity():
    async def run():
        feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
        waiters = [feed.watch(f'0x{oid:x}') for oid in range(600)]
        feed.unwatch('0x0', waiters[0])
        for oid in range(600):
            feed._handle_orders([order_event(oid, 'canceled')])
            feed._handle_fills({'fills': [fill_event(oid)]})
        assert waiters[0].cancelled()
        assert all(waiter.done() for waiter in waiters)
        assert len(feed._terminal) == 600
        assert len(feed._orders) == len(feed._fills) == 600
        assert (await feed.watch('0x0'))['filled_base'] == pytest.approx(.4)
        for oid in range(600):
            feed.release_evidence(f'0x{oid:x}')
        assert len(feed._terminal) == 512
        # Retired history must never rebuild a false aggregate from replay.
        feed._handle_fills({'fills': [fill_event(0), fill_event(0, tid=2)]})
        feed._handle_orders([order_event(0, 'canceled', remaining='0.2')])
        assert feed.terminal('0x0') is None and 0 not in feed._fills
    asyncio.run(run())


@pytest.mark.parametrize('missing', ['origSz', 'sz'])
def test_missing_order_quantity_never_resolves_zero(missing):
    feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
    event = order_event(1, 'canceled', remaining='1')
    del event['order'][missing]
    feed._handle_orders([event])
    assert feed.terminal('0x1') is None


def test_same_price_different_tid_and_out_of_order_updates():
    feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
    feed._handle_orders([order_event(1, 'canceled', remaining='0.2')])
    feed._handle_orders([order_event(1, 'open', remaining='1')])
    feed._handle_fills({'isSnapshot': True, 'fills': [fill_event(1, tid=2)]})
    assert feed.terminal('0x1') is None
    feed._handle_fills({'fills': [fill_event(1, tid=1), fill_event(1, tid=2)]})
    assert feed.terminal('0x1')['filled_base'] == pytest.approx(.8)
    assert feed._fills[1]['filled_base'] == pytest.approx(.8)


def test_missing_tid_never_accumulates_unverifiable_replays():
    feed = HLOrdersFeed('test', 'unused', '0xabc', 'io:SNDK')
    for _ in range(3):
        feed._handle_fills({'fills': [fill_event(1, tid=None)]})
    assert feed._fills[1]['missing_identity']
    assert feed._fills[1]['filled_base'] == 0
    feed._handle_orders([order_event(1, 'canceled', remaining='1')])
    assert feed.terminal('0x1') is None
