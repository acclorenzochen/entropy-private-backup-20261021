"""Offline pagination and shared-worker checks: partial history stays unknown."""
import asyncio
from decimal import Decimal as D
from types import SimpleNamespace
import socket
import time

import pytest

from entropy_arb.venue_lighter import LighterHistoryPending
import entropy_arb.venue_lighter as adapter
from test_funding_hedge_adapters import lighter_venue, lighter_event, TS
from test_runtime import setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('history checks must remain offline')
    monkeypatch.setattr(socket, 'getaddrinfo', denied)
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)


def trade(identity, stamp):
    return dict(timestamp=stamp, size='.01', price='100', market_id=139,
                ask_account_id=42, bid_account_id=99, is_maker_ask=False,
                ask_client_id_str=str(identity), trade_id_str=str(identity),
                maker_fee='0', taker_fee='0')


def test_trade_scan_resumes_pages_and_refreshes_late_fees(monkeypatch):
    venue = lighter_venue()
    venue.quote_decimals = 6
    now = [0.0]
    monkeypatch.setattr(adapter, 'time', SimpleNamespace(monotonic=lambda: now[0], time=time.time))
    calls = []
    pages = {
        None: {'trades': [trade(1, TS + 3)], 'next_cursor': 'p1'},
        'p1': {'trades': [trade(2, TS + 2)], 'next_cursor': 'p2'},
        'p2': {'trades': [trade(3, TS + 1)]},
    }
    async def get(path, params=None, headers=None):
        assert path.endswith('/trades')
        calls.append(params.get('cursor'))
        return pages[params.get('cursor')]
    venue._get = get
    async def scenario():
        with pytest.raises(LighterHistoryPending):
            await venue.fetch_fee_fills(TS, TS + 5)
        result = await venue.fetch_fee_fills(TS, TS + 5)
        assert {row['exchange_trade_id'] for row in result} == {'1', '2', '3'}
        assert calls == [None, 'p1', 'p2']
        assert await venue.fetch_fee_fills(TS, TS + 5) == result
        assert calls == [None, 'p1', 'p2']
        now[0] = 120
        pages[None] = {'trades': [trade(4, TS + 4)]}
        late = await venue.fetch_fee_fills(TS, TS + 5)
        assert late[0]['exchange_trade_id'] == '4'
        assert calls[-1] is None
    asyncio.run(scenario())


def test_trade_scan_stops_after_descending_window_is_passed():
    venue = lighter_venue()
    venue.quote_decimals = 6
    calls = []
    async def get(path, params=None, headers=None):
        calls.append(params.get('cursor'))
        return {'trades': [trade(1, TS - 2)], 'next_cursor': 'irrelevant-older'}
    venue._get = get
    assert asyncio.run(venue.fetch_fee_fills(TS, TS + 5)) == []
    assert calls == [None]


def test_funding_dynamic_until_resumes_fixed_window_then_incremental_tail():
    venue = lighter_venue()
    calls = []
    pages = {None: 1, 'p1': 2, 'p2': 3}
    async def get(path, params=None, headers=None):
        calls.append((params['start_timestamp'], params['end_timestamp'], params.get('cursor')))
        if params['start_timestamp'] == TS + 10:
            return {'code': 200, 'position_fundings': [lighter_event(TS + 15, '4', identity=4)]}
        cursor = params.get('cursor')
        identity = pages[cursor]
        response = {'code': 200, 'position_fundings': [lighter_event(TS + identity, '1', identity=identity)]}
        if identity < 3:
            response['next_cursor'] = f'p{identity}'
        return response
    venue._get = get
    async def scenario():
        with pytest.raises(LighterHistoryPending):
            await venue.fetch_funding_settlements(TS, TS + 10)
        with pytest.raises(LighterHistoryPending):
            await venue.fetch_funding_settlements(TS, TS + 20)
        rows = await venue.fetch_funding_settlements(TS, TS + 20)
        assert sum((row.amount_usd for row in rows), D(0)) == 7
        assert calls == [(TS, TS + 10, None), (TS, TS + 10, 'p1'),
                         (TS, TS + 10, 'p2'), (TS + 10, TS + 20, None)]
    asyncio.run(scenario())


def test_failed_funding_page_retries_same_cursor_without_partial_cash():
    venue = lighter_venue()
    calls = []
    fail = [True]
    async def get(path, params=None, headers=None):
        cursor = params.get('cursor')
        calls.append(cursor)
        if cursor is None:
            return {'code': 200, 'position_fundings': [lighter_event(identity=1)], 'next_cursor': 'p1'}
        if fail[0]:
            fail[0] = False
            raise OSError('offline read failure')
        return {'code': 200, 'position_fundings': [lighter_event(amount='2', identity=2)]}
    venue._get = get
    async def scenario():
        with pytest.raises(OSError):
            await venue.fetch_funding_payments(TS - 1, TS + 1)
        assert await venue.fetch_funding_payments(TS - 1, TS + 1) == 2
        assert calls == [None, 'p1', 'p1']
    asyncio.run(scenario())


def test_canonical_and_analytics_tails_share_fair_attempt_clock(tmp_path, monkeypatch):
    runtime, _, _, journal = setup_runtime(tmp_path)
    mono = [0.0]
    reads = []
    journal.provisional_cycles = lambda _: [{'cycle_id': 'a'}, {'cycle_id': 'b'}]
    journal.pending_economics_cycles = lambda _: [{'cycle_id': 'a'}, {'cycle_id': 'c'}]
    monkeypatch.setattr('entropy_arb.v3.runtime.time.monotonic', lambda: mono[0])
    async def incomplete(cycle_id):
        reads.append((cycle_id, mono[0]))
        raise LighterHistoryPending('more pages')
    runtime._reconcile_cycle_accounting = incomplete
    async def scenario():
        for stamp in (0, 5, 10, 15, 119, 120, 125, 130):
            mono[0] = stamp
            try:
                await runtime._reconcile_pending_economics()
            except LighterHistoryPending:
                pass
        assert reads == [('a', 0), ('b', 5), ('c', 10),
                         ('a', 120), ('b', 125), ('c', 130)]
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_incremental_funding_reads_do_not_postpone_full_late_posting_scan(monkeypatch):
    venue = lighter_venue()
    now = [0.0]
    calls = []
    monkeypatch.setattr(adapter, 'time', SimpleNamespace(monotonic=lambda: now[0], time=time.time))
    async def get(path, params=None, headers=None):
        calls.append(params['start_timestamp'])
        late = now[0] >= 120 and params['start_timestamp'] == TS
        return {'code': 200, 'position_fundings':
                [lighter_event(TS + 1, '3', identity=1)] if late else []}
    venue._get = get
    async def scenario():
        for elapsed in (0, 30, 60, 90):
            now[0] = elapsed
            assert await venue.fetch_funding_payments(TS, TS + elapsed + 10) == 0
        now[0] = 120
        assert await venue.fetch_funding_payments(TS, TS + 130) == 3
        assert calls == [TS, TS + 10, TS + 40, TS + 70, TS]
    asyncio.run(scenario())


def test_funding_cache_filters_to_the_requested_window():
    venue = lighter_venue()
    calls = []
    async def get(path, params=None, headers=None):
        calls.append(params)
        return {'code': 200, 'position_fundings': [
            lighter_event(TS + 1, '1', identity=1),
            lighter_event(TS + 9, '2', identity=2)]}
    venue._get = get
    async def scenario():
        assert await venue.fetch_funding_payments(TS, TS + 10) == 3
        assert await venue.fetch_funding_payments(TS, TS + 5) == 1
        assert len(calls) == 1
    asyncio.run(scenario())


def test_concurrent_newer_window_is_not_claimed_complete_by_waiting_caller():
    venue = lighter_venue()
    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()
        async def get(path, params=None, headers=None):
            started.set()
            await release.wait()
            return {'code': 200, 'position_fundings': []}
        venue._get = get
        first = asyncio.create_task(venue.fetch_funding_payments(TS, TS + 10))
        await asyncio.wait_for(started.wait(), .2)
        newer = asyncio.create_task(venue.fetch_funding_payments(TS, TS + 20))
        await asyncio.sleep(0)
        release.set()
        assert await first == 0
        with pytest.raises(LighterHistoryPending):
            await newer
        assert await venue.fetch_funding_payments(TS, TS + 20) == 0
    asyncio.run(scenario())
