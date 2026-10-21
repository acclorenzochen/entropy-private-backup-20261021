import asyncio
import json

import pytest

from entropy_arb import feeds
from entropy_arb.book import OrderBook


def snapshot(nonce=10, channel="order_book/139"):
    return {
        "type": "subscribed/order_book",
        "channel": channel,
        "order_book": {
            "nonce": nonce,
            "bids": [{"price": "99", "size": "1"}],
            "asks": [{"price": "101", "size": "1"}],
        },
    }


def gap(nonce=12):
    return {
        "type": "update/order_book",
        "channel": "order_book/139",
        "order_book": {"begin_nonce": nonce, "nonce": nonce},
    }


class FakeWS:
    def __init__(self, clock, frames=()):
        self.clock = clock
        self.frames = iter(frames)
        self.sent = []

    async def __aenter__(self):
        self.started = self.clock[0]
        return self

    async def __aexit__(self, *args):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            elapsed, frame = next(self.frames)
        except StopIteration:
            raise RuntimeError("simulated disconnect")
        self.clock[0] = self.started + elapsed
        return json.dumps(frame)

    async def send(self, raw):
        self.sent.append(json.loads(raw))


def run_connections(monkeypatch, sessions):
    clock = [100.0]
    stop = asyncio.Event()
    sleeps = []
    sockets = [FakeWS(clock, frames) for frames in sessions]
    pending = iter(sockets)
    monkeypatch.setattr(feeds.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(feeds, "ws_connect", lambda *args, **kwargs: next(pending))

    async def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds
        if len(sleeps) == len(sockets):
            stop.set()

    monkeypatch.setattr(feeds.asyncio, "sleep", sleep)
    feed = feeds.LighterBookFeed("test", "unused", 139, OrderBook(), lambda: None)
    asyncio.run(feed.run(stop))
    assert not feed.book.ready
    return sleeps, sockets


def test_connected_frames_do_not_reset_fast_flap_backoff(monkeypatch):
    sleeps, sockets = run_connections(monkeypatch, [
        [(0.0, {"type": "connected"}), (0.1, {"type": "ping"})]
        for _ in range(7)
    ])
    assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
    assert all(ws.sent == [
        {"type": "subscribe", "channel": "order_book/139"},
        {"type": "pong"},
    ] for ws in sockets)


@pytest.mark.parametrize("stable_frame,expected", [
    ({"type": "ping"}, [1.0, 2.0, 4.0, 8.0]),
    (snapshot(channel="order_book/140"), [1.0, 2.0, 4.0, 8.0]),
    (snapshot(), [1.0, 2.0, 1.0, 2.0]),
])
def test_reset_requires_stable_connection_and_valid_snapshot(
        monkeypatch, stable_frame, expected):
    sleeps, _ = run_connections(monkeypatch, [
        [(0.0, {"type": "connected"})],
        [(0.0, {"type": "connected"})],
        [(0.0, {"type": "connected"}), (30.0, stable_frame)],
        [(0.0, {"type": "connected"}), (0.1, snapshot())],
    ])
    assert sleeps == expected


@pytest.mark.parametrize("elapsed,blocked", [(4.999, True), (5.0, False)])
def test_gap_cooldown_clears_book_before_reconnect(monkeypatch, elapsed, blocked):
    clock = [100.0]
    monkeypatch.setattr(feeds.time, "monotonic", lambda: clock[0])
    book = OrderBook()
    feed = feeds.LighterBookFeed("test", "unused", 139, book, lambda: None)
    ws = FakeWS(clock)

    async def scenario():
        await feed._handle_book(ws, snapshot(), snapshot=True)
        assert book.ready
        await feed._handle_book(ws, gap(), snapshot=False)
        assert not book.ready and not book.bids and not book.asks
        assert len(ws.sent) == 2
        await feed._handle_book(ws, snapshot(20), snapshot=True)
        clock[0] += elapsed
        if blocked:
            with pytest.raises(RuntimeError, match="resubscribe cooldown"):
                await feed._handle_book(ws, gap(22), snapshot=False)
        else:
            await feed._handle_book(ws, gap(22), snapshot=False)
        assert not book.ready and not book.bids and not book.asks
        assert not feed._synced and feed._nonce is None
        assert len(ws.sent) == (2 if blocked else 4)
        await feed._handle_book(ws, gap(23), snapshot=False)
        assert not book.ready and not book.bids and not book.asks

    asyncio.run(scenario())


def test_gap_cooldown_is_reset_for_new_connection(monkeypatch):
    sessions = [
        [(0.0, {"type": "connected"}), (0.1, snapshot()), (0.2, gap())]
        for _ in range(2)
    ]
    sleeps, sockets = run_connections(monkeypatch, sessions)
    assert sleeps == [1.0, 2.0]
    assert all([frame["type"] for frame in ws.sent] == [
        "subscribe", "unsubscribe", "subscribe",
    ] for ws in sockets)
