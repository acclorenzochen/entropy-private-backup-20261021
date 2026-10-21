"""zkLighter venue adapter (Lighter mainnet / Lighter Robinhood chain).

Market data and account state come from Lighter's public REST + websocket
APIs via plain aiohttp/websockets, so --record-only data collection works
without the SDK. Trading lazily imports the official `lighter` SDK
(https://github.com/elliottech/lighter-python) for transaction signing only.

Market orders carry mandatory avg-execution-price protection and settle
asynchronously on the authenticated account_orders websocket; send_taker()
hides that behind the same result shape the HL venue returns:
{status, filled_base, avg_px, err, unresolved}.
"""
from __future__ import annotations

import asyncio
from contextvars import ContextVar
import json
import logging
import math
import socket
import time
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from urllib.parse import urlsplit

import aiohttp
from aiohttp import ClientSession

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect  # type: ignore

from .book import OrderBook
from .config import VenueConf
from .lighter_requests import RequestDeferred, RequestTicket, shared_budget
from .order_timing import (HTTP_TIMING_CONTEXT, OrderTiming, OrderTimingCollector,
                          attach_http_timing, integer, source_name, stamp, timeout_metadata)
from .feeds import LighterBookFeed
from .funding import (FundingRiskSnapshot, FundingSettlement, FundingSnapshot,
                      funding_decimal, funding_timestamp, funding_window)

log = logging.getLogger("lighter")

OPEN_STATUSES = {"in-progress", "pending", "open"}
TERMINAL_STATUSES = {'filled', 'canceled', 'cancelled', 'rejected', 'expired',
    'canceled-post-only', 'canceled-reduce-only', 'canceled-position-not-allowed',
    'canceled-margin-not-allowed', 'canceled-too-much-slippage',
    'canceled-not-enough-liquidity', 'canceled-self-trade', 'canceled-expired',
    'canceled-oco', 'canceled-child', 'canceled-liquidation', 'canceled-invalid-balance'}
AUTH_REFRESH_SEC = 8 * 60
REST_TIMEOUT = 10.0
FUNDING_HISTORY_PAGE_SIZE = 100
FUNDING_HISTORY_MAX_PAGES = 100
MAX_CLIENT_ORDER_INDEX = (1 << 48) - 1
ACCOUNT_WS_MAX_AGE_SEC = 20.0
ACCOUNT_WS_HEALTH_SEC = 5.0
ACCOUNT_REST_AUDIT_SEC = 120.0
ACCOUNT_REST_CACHE_SEC = 2.0
LEVERAGE_VERIFY_TIMEOUT_SEC = 30.0
HISTORY_PAGES_PER_READ = 2
HISTORY_CACHE_SEC = 120.0
HISTORY_CACHE_WINDOWS = 128
SDK_REQUEST_CONTEXT = ContextVar("lighter_sdk_request", default=None)
SDK_HTTP_TIMING_CONTEXT = HTTP_TIMING_CONTEXT


class LighterHistoryPending(RuntimeError):
    """A bounded history scan has more pages; keep accounting provisional."""


class LighterWafBlocked(RuntimeError):
    """The account REST endpoint was replaced by an AWS WAF challenge."""


class LighterAccountStale(RuntimeError):
    """A REST account response exceeded the evidence freshness limit."""


@dataclass(frozen=True)
class AccountSnapshot:
    equity: float
    available: float
    positions: tuple[dict, ...]
    received_ts: float
    source: str
    account_index: int | None = None


@dataclass(frozen=True)
class WsAccountSnapshot:
    equity: float
    available: float
    positions: tuple[dict, ...]
    received_ts: float
    generation: int
    authoritative: bool = True
    source: str = "lighter_account_all_ws"


@dataclass(frozen=True)
class VenuePositionSnapshot:
    position: float
    received_ts: float
    authoritative: bool
    source: str
    generation: int | None = None


@dataclass
class DispatchToken:
    venue: 'LighterVenue'
    order_ref: str
    ticket: RequestTicket
    priority: int
    claimed: bool = False
    released: bool = False

    def release(self) -> None:
        """Release an abandoned preparation, never a running SDK submission."""
        if not self.claimed:
            self._finish()

    def _finish(self) -> None:
        if not self.released:
            self.released = True
            self.ticket.budget.release_reserved(self.ticket)
            self.venue._submission_lock.release()


class AccountOrdersFeed:
    """Authenticated stream of our own order updates (settlement channel)."""

    def __init__(self, name: str, ws_url: str, market_id: int,
                 account_index: int, signer, notify=None, timing_collector=None) -> None:
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.account_index = account_index
        self.signer = signer
        self.notify = notify
        self.timing_collector = timing_collector
        self.timing_dropped = 0
        self.ready = asyncio.Event()
        self.account_ready = asyncio.Event()
        self.account_snapshot: Optional[WsAccountSnapshot] = None
        self._account_positions: dict[str, dict] = {}
        self._account_assets: dict[str, dict] = {}
        self.connection_generation = 0
        self.evidence_revision = 0
        self.last_liveness_ts = 0.0
        self._pending: dict[int, asyncio.Future] = {}
        self._terminal: OrderedDict[int, dict] = OrderedDict()
        self._terminal_changed = asyncio.Event()
        self.order_updates: OrderedDict[int, dict] = OrderedDict()
        self.unidentified_order_revision = 0
        self._registered_orders: OrderedDict[int, dict] = OrderedDict()

    def register_order(self, coi, *, requested_base, is_ask, on_progress=None):
        quantity = Decimal(str(requested_base))
        if not quantity.is_finite() or quantity <= 0 or type(is_ask) is not bool:
            raise ValueError('invalid registered order identity')
        if on_progress is not None and (not callable(on_progress) or asyncio.iscoroutinefunction(on_progress)):
            raise ValueError('order progress callback must be synchronous')
        identity = (self.account_index, self.market_id, is_ask, quantity)
        previous = self._registered_orders.get(coi)
        if previous is not None:
            if previous['identity'] != identity:
                raise ValueError('client order index already belongs to another intent')
            if on_progress is not None:
                previous['on_progress'] = on_progress
            if previous.get('callback_error') is not None:
                raise previous['callback_error']
            return
        if coi in self._terminal:
            raise ValueError('client order index has unbound historical terminal evidence')
        while len(self._registered_orders) >= 512:
            retired = next((key for key, row in self._registered_orders.items()
                            if row.get('last', {}).get('terminal')), None)
            if retired is None:
                raise RuntimeError('registered pending-order capacity exhausted')
            self._registered_orders.pop(retired)
        self._registered_orders[coi] = dict(identity=identity, on_progress=on_progress)

    def progress(self, coi):
        registered = self._registered_orders.get(coi)
        value = registered.get('last') if registered is not None else None
        return dict(value) if value is not None else None

    def _trusted_registered_order(self, coi, order, *, channel=None,
                                  source='account_orders_ws'):
        registered = self._registered_orders.get(coi)
        if registered is None:
            return True
        account, market, is_ask, quantity = registered['identity']
        if source == 'account_orders_ws':
            if channel != f'account_orders:{market}:{account}':
                return False
        for field, expected in (('owner_account_index', account), ('market_index', market), ('is_ask', is_ask)):
            if field in order and (type(order[field]) is not type(expected) or order[field] != expected):
                return False
        if 'initial_base_amount' in order:
            try:
                if Decimal(str(order['initial_base_amount'])) != quantity:
                    return False
            except (ValueError, ArithmeticError):
                return False
        identifier = integer(order.get('order_index', order.get('order_id')))
        if identifier is not None:
            previous = registered.get('exchange_order_id')
            if previous is not None and previous != identifier:
                return False
            registered['exchange_order_id'] = identifier
        return True

    def watch(self, coi: int) -> asyncio.Future:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        if coi in self._terminal:
            fut.set_result(self._terminal[coi])
            return fut
        self._pending[coi] = fut
        return fut

    def unwatch(self, coi: int) -> None:
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.cancel()

    def terminal(self, coi: int) -> Optional[dict]:
        """Return a cached terminal update, if the stream saw one."""
        return self._terminal.get(coi)

    def _timing(self, coi):
        try:
            trace = self.timing_collector.get(coi) if self.timing_collector is not None else None
            return trace if isinstance(trace, OrderTiming) else None
        except Exception:
            self.timing_dropped += 1
            return None

    def _resolve(self, coi: int, info: dict, *, source=None, terminal=True, received=None) -> None:
        info = dict(info)
        registered = self._registered_orders.get(coi)
        previous = registered.get('last') if registered is not None else self._terminal.get(coi)
        filled = float(info['filled_base'])
        if not math.isfinite(filled) or filled < 0:
            return
        if registered is not None and Decimal(str(filled)) > registered['identity'][3]:
            raise ValueError('registered order cumulative fill exceeds requested quantity')
        if previous is not None:
            prior_fill = previous['filled_base']
            if filled < prior_fill:
                if not terminal or previous.get('terminal', True):
                    return
                info.update(filled_base=prior_fill, avg_px=previous.get('avg_px'),
                    filled_quote=previous.get('filled_quote'))
            elif filled == prior_fill and previous.get('filled_quote') is not None:
                quote = info.get('filled_quote')
                if quote is None or quote < previous['filled_quote']:
                    info.update(avg_px=previous.get('avg_px'), filled_quote=previous['filled_quote'])
            if previous.get('terminal', True):
                terminal = True
                if info['status'] not in TERMINAL_STATUSES:
                    info['status'] = previous['status']
        when = stamp() if received is None else received
        info.update(order_ref=str(coi), terminal=terminal, unresolved=not terminal,
            confirmation_source=source or info.get('confirmation_source', 'account_orders_ws'),
            observed_ts=when['wall_ts'], observed_mono_ns=when['mono_ns'],
            generation=self.connection_generation if source != 'accountOrders_rest' else None)
        if previous is not None and all(info.get(key) == previous.get(key)
                for key in ('status', 'filled_base', 'avg_px', 'terminal', 'filled_quote', 'confirmation_source')):
            return
        trace = self._timing(coi)
        if trace is not None and terminal:
            trace.mark('terminal_published_first', first=True)
            trace.mark('terminal_published')
        if registered is not None:
            registered['last'] = dict(info)
            callback = registered['on_progress']
            if callback is not None:
                try:
                    callback(dict(info))
                except Exception as exc:
                    registered['callback_error'] = exc
                    future = self._pending.pop(coi, None)
                    if future is not None and not future.done():
                        future.set_exception(exc)
                    return  # The foreground receives the financial failure; HTTP is not cancelled.
        if trace is not None and terminal:
            trace.data['terminal_evidence_source'] = source_name(source)
            trace.result(status=info['status'], filled_base=info['filled_base'],
                average=info.get('avg_px'), source=info.get('confirmation_source'))
            if 'runtime_consumed' in trace.data['stages']:
                trace.emit()
        if not terminal:
            return
        self._terminal[coi] = info
        self._terminal_changed.set()
        while len(self._terminal) > 512:
            self._terminal.popitem(last=False)
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.set_result(info)

    async def wait_terminal(self, coi: int) -> dict:
        while True:
            self._terminal_changed.clear()
            cached = self.terminal(coi)
            if cached is not None:
                return cached
            await self._terminal_changed.wait()

    def _handle_orders(self, msg: dict, *, received=None, parsed=None, boundary='handler_call') -> None:
        received = stamp() if received is None else received
        parsed = stamp() if parsed is None else parsed
        self.evidence_revision += 1
        orders = msg.get('orders') or {}
        if not isinstance(orders, dict):
            self.unidentified_order_revision += 1
            return
        for market, lst in orders.items():
            try:
                if int(market) != self.market_id:
                    continue
            except (TypeError, ValueError):
                self.unidentified_order_revision += 1
                continue
            if not isinstance(lst, list):
                self.unidentified_order_revision += 1
                continue
            for o in lst or []:
                if not isinstance(o, dict):
                    self.unidentified_order_revision += 1
                    continue
                status = str(o.get("status", ""))
                try:
                    coi = int(o.get("client_order_index"))
                    if not 0 < coi <= MAX_CLIENT_ORDER_INDEX:
                        raise ValueError('invalid order index')
                except (TypeError, ValueError):
                    self.unidentified_order_revision += 1
                    continue
                if not self._trusted_registered_order(coi, o, channel=msg.get('channel')):
                    self.unidentified_order_revision += 1
                    continue
                self.order_updates[coi] = {
                    'client_order_index': coi, 'status': status,
                    'filled_base': None, 'received_ts': time.time(),
                    'revision': self.evidence_revision,
                    'generation': self.connection_generation}
                self.order_updates.move_to_end(coi)
                while len(self.order_updates) > 512:
                    self.order_updates.popitem(last=False)
                try:
                    if type(o.get('filled_base_amount')) not in (str, int, float):
                        raise ValueError('missing filled quantity')
                    fb = float(o['filled_base_amount'])
                    if not math.isfinite(fb) or fb < 0:
                        raise ValueError('invalid filled quantity')
                except (TypeError, ValueError):
                    self.unidentified_order_revision += 1
                    continue
                self.order_updates[coi]['filled_base'] = fb
                trace = self._timing(coi)
                if trace is not None:
                    envelope = integer(msg.get('timestamp'))
                    if envelope is not None and envelope > 0:
                        trace.data['ws_envelope_published_ms'] = envelope
                        trace.data['ws_envelope_timestamp_source'] = 'WebSocket envelope.timestamp:API_server_frame_generation_milliseconds'
                    trace.identity(o, 'account_orders_ws:Order')
                    trace.order(o, received=received, parsed=parsed,
                                boundary=boundary, source='account_orders_ws')
                raw_quote = o.get('filled_quote_amount')
                try:
                    fq = float(raw_quote) if raw_quote is not None else None
                    if fq is not None and (not math.isfinite(fq) or fq < 0):
                        raise ValueError('invalid cumulative quote')
                except (TypeError, ValueError):
                    self.unidentified_order_revision += 1
                    continue
                status = status.strip().lower()
                if coi not in self._registered_orders and status not in TERMINAL_STATUSES:
                    continue
                self._resolve(coi, {"status": status, "filled_base": fb,
                                    "filled_quote": fq,
                                    "avg_px": (fq / fb) if fb > 0 and fq is not None and fq > 0 else None},
                    source='account_orders_ws', terminal=status in TERMINAL_STATUSES, received=received)

    def _handle_trades(self, msg, *, received, parsed, boundary):
        try:
            self._observe_trades(msg, received=received, parsed=parsed, boundary=boundary)
        except Exception:
            self.timing_dropped += 1

    def _observe_trades(self, msg, *, received, parsed, boundary):
        if self.timing_collector is None:
            return
        trades = msg.get('trades')
        rows = trades.get(str(self.market_id), []) if type(trades) is dict else []
        if type(rows) is not list:
            return
        self.timing_collector.trade_rows_dropped += max(0, len(rows) - 256)
        changed = {}
        for row in rows[:256]:
            if type(row) is not dict or integer(row.get('market_id')) != self.market_id:
                continue
            for side in ('ask', 'bid'):
                if integer(row.get(side + '_account_id')) != self.account_index:
                    continue
                coi = integer(row.get(side + '_client_id_str', row.get(side + '_client_id')))
                trace = self._timing(coi) if coi is not None else None
                if trace is not None:
                    envelope = integer(msg.get('timestamp'))
                    if envelope is not None and envelope > 0:
                        trace.data['ws_trade_envelope_published_ms'] = envelope
                        trace.data['ws_trade_envelope_timestamp_source'] = 'WebSocket envelope.timestamp:API_server_frame_generation_milliseconds'
                    trace.trade(row, order_id=row.get(side + '_id_str', row.get(side + '_id')),
                                received=received, parsed=parsed, boundary=boundary)
                    changed[coi] = trace
        for trace in changed.values():
            if 'runtime_consumed' in trace.data['stages']:
                trace.emit()

    def _invalidate_account(self) -> None:
        self.evidence_revision += 1
        self.account_ready.clear()
        self.account_snapshot = None
        self._account_positions.clear()
        self._account_assets.clear()

    def _handle_account(self, msg: dict, *, received=None, parsed=None, boundary='handler_call') -> None:
        self._handle_trades(msg, received=stamp() if received is None else received,
                            parsed=stamp() if parsed is None else parsed, boundary=boundary)
        self.evidence_revision += 1
        subscribed = msg.get("type") == "subscribed/account_all"
        positions = msg.get("positions")
        assets = msg.get("assets")
        if (positions is not None and not isinstance(positions, dict)) \
                or (assets is not None and not isinstance(assets, dict)) \
                or (subscribed and (positions is None or assets is None)) \
                or (not subscribed and self.account_snapshot is None):
            self._invalidate_account()
            log.warning("[%s] malformed account_all frame; state is UNKNOWN", self.name)
            return
        try:
            if subscribed:
                self._account_positions = {
                    str(key): dict(row) for key, row in positions.items()
                    if isinstance(row, dict)}
                self._account_assets = {
                    str(key): dict(row) for key, row in assets.items()
                    if isinstance(row, dict)}
                if len(self._account_positions) != len(positions) \
                        or len(self._account_assets) != len(assets):
                    raise TypeError("invalid account_all snapshot row")
            else:
                for target, update in ((self._account_positions, positions),
                                       (self._account_assets, assets)):
                    for key, row in (update or {}).items():
                        if not isinstance(row, dict):
                            raise TypeError("invalid account_all update row")
                        target[str(key)] = {**target.get(str(key), {}), **row}
            rows = tuple(self._account_positions.values())
            if not self._account_assets:
                raise ValueError("account_all assets missing")
            equity = 0.0
            for row in self._account_assets.values():
                balance = float(row["margin_balance"])
                multiplier = float(row.get("multiplier", 1.0))
                if not math.isfinite(balance) or not math.isfinite(multiplier) \
                        or multiplier <= 0:
                    raise ValueError("invalid account_all asset value")
                equity += balance * multiplier
            if not math.isfinite(equity):
                raise ValueError("invalid account_all equity")
        except (TypeError, ValueError, KeyError, AttributeError):
            self._invalidate_account()
            log.warning("[%s] invalid account_all values; state is UNKNOWN", self.name)
            return
        self.account_snapshot = WsAccountSnapshot(
            equity=equity, available=equity, positions=rows,
            received_ts=time.time(), generation=self.connection_generation)
        self.account_ready.set()
        if self.notify is not None:
            self.notify()

    def fresh_account_snapshot(
            self, max_age_sec: float = ACCOUNT_WS_MAX_AGE_SEC) -> Optional[WsAccountSnapshot]:
        snapshot = self.account_snapshot
        if snapshot is None or not self.account_ready.is_set() \
                or not snapshot.authoritative \
                or snapshot.generation != self.connection_generation \
                or time.time() - self.last_liveness_ts > max_age_sec:
            return None
        return snapshot

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                auth, err = self.signer.create_auth_token_with_expiry()
                if err is not None:
                    raise RuntimeError(f"auth token: {err}")
                connected_at = time.time()
                stable_since = time.monotonic()
                async with ws_connect(self.ws_url, max_size=2**23,
                                      open_timeout=10, ping_interval=15,
                                      ping_timeout=15, family=socket.AF_INET) as ws:
                    self.connection_generation += 1
                    self._invalidate_account()
                    self.last_liveness_ts = time.time()
                    while not stop.is_set():
                        try:
                            raw = await asyncio.wait_for(
                                ws.recv(), timeout=ACCOUNT_WS_HEALTH_SEC)
                            raw_received = stamp()
                        except asyncio.TimeoutError:
                            pong = await ws.ping()
                            await asyncio.wait_for(pong, timeout=ACCOUNT_WS_HEALTH_SEC)
                            self.last_liveness_ts = time.time()
                            if (time.time() - connected_at > AUTH_REFRESH_SEC
                                    and not self._pending):
                                log.info("[%s] refreshing account ws auth", self.name)
                                break
                            continue
                        if (time.monotonic() - stable_since >= 30.0
                                and self.ready.is_set() and self.account_ready.is_set()):
                            backoff = 1.0
                        self.last_liveness_ts = time.time()
                        msg = json.loads(raw)
                        parsed = stamp()
                        t = msg.get("type")
                        if t in ("subscribed/account_orders", "update/account_orders"):
                            if t.startswith("subscribed"):
                                log.info("[%s] account orders stream ready", self.name)
                            self.ready.set()
                            self._handle_orders(msg, received=raw_received,
                                                parsed=parsed, boundary='ws_recv')
                        elif t in ("subscribed/account_all", "update/account_all"):
                            if t.startswith("subscribed"):
                                log.info("[%s] account state stream ready", self.name)
                            self._handle_account(msg, received=raw_received,
                                                 parsed=parsed, boundary='ws_recv')
                        elif t == "connected":
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "channel": f"account_orders/{self.market_id}/"
                                           f"{self.account_index}",
                                "auth": auth}))
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "channel": f"account_all/{self.account_index}"}))
                        elif t == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        if stop.is_set():
                            break
                        if (time.time() - connected_at > AUTH_REFRESH_SEC
                                and not self._pending):
                            log.info("[%s] refreshing account ws auth", self.name)
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] account ws error: %s — retry in %.0fs",
                            self.name, e, backoff)
                self.ready.clear()
                self._invalidate_account()
                if stop.is_set():
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            self.ready.clear()
            self._invalidate_account()


class LighterVenue:
    supports_submission_guard = True
    supports_order_progress = True
    # send_taker below uses ordinary MARKET/IOC and supplies no integrator parameters.
    fee_policy_order_type = 'market_ioc_no_integrator'
    kind = "lighter"
    supports_dispatch_token = True

    def __init__(self, conf: VenueConf, session: aiohttp.ClientSession,
                 settle_timeout_sec: float) -> None:
        assert conf.lighter_profile is not None
        self.conf = conf
        self.key = conf.key
        self.name = conf.label
        self.session = session
        self.settle_timeout = settle_timeout_sec
        self.profile = conf.lighter_profile
        self.book = OrderBook()
        self.position = 0.0
        self.cash = 0.0
        self.volume_usd = 0.0     # cumulative filled notional this session
        self.equity = None
        self.free = None
        self.margin_state = "unknown"
        self.margin_equity_usd = None
        self.margin_available_usd = None
        self.margin_available_ratio = None
        self.margin_updated_ts = None
        self.margin_source = None
        self.start_equity = None
        self.fee_bps = conf.fee_bps
        self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.market_id = -1
        self.price_decimals = 2
        self.size_decimals = 4
        self.quote_decimals = 6
        self.min_base = 0.0
        self.min_quote = 10.0
        self.signer = None
        self.orders_feed: Optional[AccountOrdersFeed] = None
        self._coi = int(time.time() * 1000)
        self._submit_tasks: set[asyncio.Task] = set()
        self._submission_lock = asyncio.Lock()
        self._fee_auth_token: Optional[str] = None
        self._fee_auth_expires_at = 0.0
        self.funding: Optional[FundingSnapshot] = None
        self._account_cache = None
        self._account_task = None
        self._account_epoch = 0
        self._account_feed_revision = None
        self._order_lookups = OrderedDict()
        self._history_scans = OrderedDict()

    # ------------------------------------------------------------------ REST

    def _request_budget(self):
        return shared_budget(
            self.profile.api_url,
            limit=getattr(self.conf, 'lighter_request_limit', 48),
            reserve=getattr(self.conf, 'lighter_realtime_reserve', 12),
            history_limit=getattr(self.conf, 'lighter_history_limit', 6))

    def request_diagnostics(self) -> dict:
        result = self._request_budget().snapshot()
        result['account_cache_sec'] = ACCOUNT_REST_CACHE_SEC
        result['history_scan_windows'] = len(getattr(self, '_history_scans', {}))
        return result

    def configure_order_timing(self, sink, clock_session_id):
        try:
            self._order_timing_collector = OrderTimingCollector(sink, clock_session_id)
            if self.orders_feed is not None:
                self.orders_feed.timing_collector = self._order_timing_collector
        except Exception:
            self._order_timing_collector = None

    def order_timing(self, order_ref, *, create=False, **metadata):
        try:
            collector = getattr(self, '_order_timing_collector', None)
            trace = collector.get(order_ref, create=create) if collector is not None else None
            if trace is not None:
                trace.annotate(**metadata)
            return trace
        except Exception:
            return None

    def _http_session(self):
        # Shared host sessions may also serve Entropy. Own the Lighter REST
        # pool so disabling invisible GET retries doesn't change that venue.
        if not isinstance(self.session, ClientSession):
            return self.session
        session = getattr(self, '_lighter_http_session', None)
        if session is None:
            session = self._lighter_http_session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(family=socket.AF_INET), trust_env=True)
            if hasattr(session, '_retry_connection'):
                session._retry_connection = False
        return session

    def _check_http_limit(self, status, headers) -> None:
        reason = self._request_budget().defer(status, headers)
        if reason == 'captcha':
            raise LighterWafBlocked('AWS WAF CAPTCHA; shared request circuit open')

    async def _get(self, path: str, params: Optional[dict] = None,
                   headers: Optional[dict] = None):
        priority = (2 if path.endswith(('/trades', '/positionFunding', '/status'))
                    else 0 if path.endswith('/accountOrders') else 1)
        async with self._request_budget().request(priority=priority, endpoint=path):
            async with self._http_session().get(
                    self.profile.api_url + path, params=params,
                    headers=headers, allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT)) as r:
                self._check_http_limit(r.status, r.headers)
                r.raise_for_status()
                self._request_budget().record_success()
                return await r.json()

    def _wrap_sdk_requests(self) -> None:
        self._wrap_sdk_nonce()
        self._wrap_sdk_signing()
        rest = self.signer.api_client.rest_client
        attach_http_timing(rest.pool_manager)
        original = rest.request
        # Prevent invisible GET retries; every application dispatch is budgeted.
        rest.retry_client = None
        if hasattr(rest.pool_manager, '_retry_connection'):
            rest.pool_manager._retry_connection = False
        pool_request = rest.pool_manager.request
        async def no_redirects(*args, **kwargs):
            kwargs.setdefault('allow_redirects', False)
            timing_context = SDK_HTTP_TIMING_CONTEXT.get()
            trace, row = timing_context if timing_context is not None else (None, None)
            if row is not None:
                trace.request_mark(row, 'pool_wrapper_start')
            try:
                return await pool_request(*args, **kwargs)
            finally:
                if row is not None:
                    trace.request_mark(row, 'pool_wrapper_end')
        rest.pool_manager.request = no_redirects
        async def request(method, url, *args, **kwargs):
            path = urlsplit(url).path
            context = SDK_REQUEST_CONTEXT.get()
            priority = context['priority'] if context is not None else (2 if path.endswith('/status') else 1)
            if context is not None and method.upper() == 'POST':
                guard = context.get('increase_guard')
                reason = guard(context['order_ref']) if guard is not None else None
                if reason:
                    raise RequestDeferred(f'submission_guard:{reason}', 0)
            budget = self._request_budget()
            async def dispatch():
                if context is not None and method.upper() == 'POST':
                    guard = context.get('increase_guard')
                    reason = guard(context['order_ref']) if guard is not None else None
                    if reason:
                        raise RequestDeferred(f'submission_guard:{reason}', 0)
                    context['post_started'] = True
                    self._invalidate_account_cache()
                kwargs.setdefault('_request_timeout', REST_TIMEOUT)
                measured_timeout, timeout_kind, timeout_known = timeout_metadata(kwargs['_request_timeout'])
                trace = context.get('timing_trace') if context is not None else None
                row = (trace.request('http_requests', method=method.upper(),
                    endpoint=path if path in {'/api/v1/sendTx', '/api/v1/nextNonce'} else 'other',
                    request_timeout=measured_timeout, request_timeout_kind=timeout_kind,
                    request_timeout_known=timeout_known) if trace is not None else None)
                timing_token = SDK_HTTP_TIMING_CONTEXT.set((trace, row))
                try:
                    response = await original(method, url, *args, **kwargs)
                    if row is not None:
                        row['status_code'] = response.status if type(response.status) is int else None
                except BaseException as exc:
                    if row is not None:
                        row['exception_type'] = type(exc).__name__
                        row['outcome'] = 'timeout' if isinstance(exc, asyncio.TimeoutError) else 'exception'
                    raise
                finally:
                    if row is not None:
                        trace.request_mark(row, 'end')
                    SDK_HTTP_TIMING_CONTEXT.reset(timing_token)
                try:
                    self._check_http_limit(response.status, response.getheaders())
                except LighterWafBlocked:
                    await response.read()
                    response.response.release()
                    raise
                if 200 <= response.status < 300:
                    budget.record_success()
                return response
            if context is not None and context['ticket'].remaining:
                budget.consume_reserved(context['ticket'], endpoint=path)
                return await dispatch()
            async with budget.request(priority=priority, endpoint=path):
                return await dispatch()
        rest.request = request

    def _wrap_sdk_nonce(self) -> None:
        """Observe the existing SDK's allocation; never choose a nonce here."""
        manager = getattr(self.signer, 'nonce_manager', None)
        if manager is None or getattr(manager, '_entropy_nonce_observed', False):
            return
        next_nonce = getattr(manager, 'async_next_nonce', None)
        if callable(next_nonce):
            async def allocate(*args, **kwargs):
                context = SDK_REQUEST_CONTEXT.get()
                before = dict(getattr(manager, 'nonce', {}))
                trace = context.get('timing_trace') if context is not None else None
                def measured(stage):
                    try:
                        if trace is not None:
                            trace.mark(stage, first=True)
                    except Exception:
                        pass
                measured('sdk_nonce_select_start')
                try:
                    key, nonce = await next_nonce(*args, **kwargs)
                    if context is not None:
                        context['nonce_allocation'] = (key, nonce, before.get(key), key in before)
                    return key, nonce
                finally:
                    measured('sdk_nonce_select_end')
            manager.async_next_nonce = allocate
        acknowledge = getattr(manager, 'acknowledge_failure', None)
        if callable(acknowledge):
            def failed(key):
                context = SDK_REQUEST_CONTEXT.get()
                if context is not None and context.get('post_started'):
                    context['nonce_failure_suppressed'] = True
                    return
                return acknowledge(key)
            manager.acknowledge_failure = failed
        manager._entropy_nonce_observed = True

        refresh = getattr(manager, 'async_hard_refresh_nonce', None)
        if callable(refresh):
            async def recover(key, *args, **kwargs):
                context = SDK_REQUEST_CONTEXT.get()
                prior = manager.nonce.get(key)
                try:
                    return await refresh(key, *args, **kwargs)
                finally:
                    # SDK recovery still owns the read and the per-key lock.
                    # A remotely submitted allocation may never be reused.
                    if context is not None and context.get('post_started') and prior is not None:
                        manager.nonce[key] = max(prior, manager.nonce.get(key, prior))
            manager.async_hard_refresh_nonce = recover

    def _wrap_sdk_signing(self) -> None:
        sign = getattr(self.signer, 'sign_create_order', None)
        if not callable(sign) or getattr(self.signer, '_entropy_signing_observed', False):
            return
        def measured(*args, **kwargs):
            context = SDK_REQUEST_CONTEXT.get()
            trace = context.get('timing_trace') if context is not None else None
            if trace is not None:
                trace.mark('sdk_sign_start', first=True)
            try:
                return sign(*args, **kwargs)
            finally:
                if trace is not None:
                    trace.mark('sdk_sign_end', first=True)
        self.signer.sign_create_order = measured
        self.signer._entropy_signing_observed = True

    @staticmethod
    def _restore_local_nonce(manager, context):
        allocation = context.get('nonce_allocation')
        if manager is None or allocation is None or context.get('post_started'):
            return
        key, allocated, previous, had_previous = allocation
        if manager.nonce.get(key) != allocated:
            return  # SDK recovery or a later allocation already owns this key.
        if had_previous:
            manager.nonce[key] = previous
        else:
            manager.nonce.pop(key, None)

    async def prepare_signer(self) -> None:
        # The native check performs HTTP. Reserve its two-call worst case and
        # run it off the event loop; the async SDK transport is wrapped below.
        self.init_signer(check_client=False)
        async with self._request_budget().request(
                priority=1, cost=2, endpoint='/native/check_client_upper_bound'):
            err = await asyncio.to_thread(self.signer.check_client)
        if err is not None:
            raise RuntimeError(f'[{self.name}] API key check failed')

    # ------------------------------------------------------------- lifecycle

    async def load_market(self) -> None:
        # Startup metadata only: never retry a signer, order or account request.
        for attempt in range(1, 4):
            try:
                data = await self._get("/api/v1/orderBooks")
                break
            except asyncio.TimeoutError as exc:
                if attempt == 3:
                    raise TimeoutError(
                        f"[{self.name}] public GET /api/v1/orderBooks timed out "
                        f"after 3 attempts ({REST_TIMEOUT:g}s each); startup blocked") from exc
                log.warning(
                    "[%s] public GET /api/v1/orderBooks timed out (attempt %d/3, "
                    "%gs timeout); retrying metadata in %ds",
                    self.name, attempt, REST_TIMEOUT, attempt)
                await asyncio.sleep(attempt)
        for ob in data.get("order_books") or []:
            if ob.get("symbol") != self.conf.symbol:
                continue
            if ob.get("status") != "active":
                raise RuntimeError(f"[{self.name}] market status={ob.get('status')}")
            self.market_id = int(ob["market_id"])
            self.price_decimals = int(ob["supported_price_decimals"])
            self.size_decimals = int(ob["supported_size_decimals"])
            self.quote_decimals = int(ob.get("supported_quote_decimals", 6))
            self.min_base = float(ob["min_base_amount"])
            self.min_quote = float(ob["min_quote_amount"])
            log.info("[%s] %s market_id=%d px_dec=%d sz_dec=%d min_base=%s "
                     "min_quote=%s taker_fee=%s", self.name, ob["symbol"],
                     self.market_id, self.price_decimals, self.size_decimals,
                     ob["min_base_amount"], ob["min_quote_amount"],
                     ob.get("taker_fee"))
            return
        raise RuntimeError(f"[{self.name}] {self.conf.symbol} not found on "
                           f"{self.profile.name}")

    def init_signer(self, *, check_client: bool = True) -> None:
        c = self.conf.lighter_creds
        assert c is not None and c.complete, f"[{self.name}] missing credentials"
        try:
            from lighter import SignerClient
        except ImportError as e:
            raise RuntimeError(
                "live trading on Lighter needs the official SDK — "
                "pip install -r requirements-live.txt "
                "(git+https://github.com/elliottech/lighter-python.git)") from e
        signer = SignerClient(
            url=self.profile.api_url,
            account_index=c.account_index,
            api_private_keys={c.api_key_index: c.api_private_key},
            chain_id=self.profile.chain_id,
        )
        if check_client:
            err = signer.check_client()
            if err is not None:
                raise RuntimeError(f"[{self.name}] API key check failed")
        self.signer = signer
        self._wrap_sdk_requests()
        log.info("[%s] signer ready (account %d)", self.name, c.account_index)

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        tasks = [asyncio.create_task(
            LighterBookFeed(self.name, self.profile.ws_url, self.market_id,
                            self.book, notify).run(stop),
            name=f"book-{self.key}")]
        if live:
            self.orders_feed = AccountOrdersFeed(
                self.name, self.profile.ws_url, self.market_id,
                self.conf.lighter_creds.account_index, self.signer, notify)
            self.orders_feed.timing_collector = getattr(self, '_order_timing_collector', None)
            tasks.append(asyncio.create_task(self.orders_feed.run(stop),
                                             name=f"acct-{self.key}"))
            tasks.append(asyncio.create_task(self._audit_account_loop(stop),
                                             name=f"acct-audit-{self.key}"))
        return tasks

    def ready_to_trade(self) -> bool:
        return (self.orders_feed is not None and self.orders_feed.ready.is_set()
                and self.orders_feed.account_ready.is_set()
                and not getattr(self, "_submit_tasks", None))

    async def set_leverage(self, leverage: int) -> None:
        """Set and verify cross-margin leverage for this market."""
        assert self.signer is not None
        _tx, resp, err = await self.signer.update_leverage(
            self.market_id, 0, leverage)
        if err is not None or getattr(resp, "code", None) != 200:
            msg = str(err) if err is not None else (
                f"code={getattr(resp, 'code', None)} "
                f"msg={getattr(resp, 'message', None)}")
            raise RuntimeError(f"[{self.name}] leverage update failed: {msg}")
        expected_imf = 100.0 / leverage
        self._invalidate_account_cache()
        acknowledged_ts = time.time()
        deadline = time.monotonic() + LEVERAGE_VERIFY_TIMEOUT_SEC
        reason = 'margin settings not observed'
        for attempt in range(20):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            delay = min(0.5 * 2 ** min(attempt, 2), 2.0)
            try:
                feed = self.orders_feed
                snapshot = (feed.fresh_account_snapshot(ACCOUNT_WS_HEALTH_SEC)
                            if feed is not None else None)
                if snapshot is None or snapshot.received_ts <= acknowledged_ts \
                        or time.time() - snapshot.received_ts > ACCOUNT_WS_MAX_AGE_SEC:
                    self._account_rest_allowed()
                    revision = ((feed.connection_generation, feed.evidence_revision)
                                if feed is not None else None)
                    # Own this read so timeout/cancellation cleans it up without
                    # cancelling another caller's shared account snapshot task.
                    snapshot = await asyncio.wait_for(
                        self._read_account_snapshot(self._account_epoch, revision),
                        timeout=min(REST_TIMEOUT, remaining))
                if time.monotonic() >= deadline:
                    break
                for p in snapshot.positions:
                    if int(p.get("market_id", -1)) != self.market_id:
                        continue
                    actual_imf = float(p.get("initial_margin_fraction") or 0.0)
                    margin_mode = int(p.get("margin_mode") or 0)
                    if abs(actual_imf - expected_imf) <= 0.05 and margin_mode == 0:
                        log.info("[%s] leverage set to %dx (cross, IMF %.2f%%)",
                                 self.name, leverage, actual_imf)
                        return
                reason = 'margin settings not observed'
            except RequestDeferred as exc:
                reason = f'request deferred ({exc.reason})'
                delay = max(delay, exc.retry_after)
            except (LighterAccountStale, asyncio.TimeoutError,
                    aiohttp.ClientConnectionError) as exc:
                reason = type(exc).__name__
            except aiohttp.ClientResponseError as exc:
                if exc.status not in (405, 408, 429, 500, 502, 503, 504):
                    raise
                reason = f'HTTP {exc.status}'
                delay = max(delay, self._request_budget().snapshot()['blocked_for_sec'])
            remaining = deadline - time.monotonic()
            if attempt == 19 or delay >= remaining:
                break
            await asyncio.sleep(delay)
        raise RuntimeError(
            f"[{self.name}] leverage update could not be verified; "
            f"startup blocked ({reason}; verification limit {LEVERAGE_VERIFY_TIMEOUT_SEC:g}s)")

    async def warm_http(self) -> None:
        """Warm the same SDK pool that carries sendTx without selecting a nonce."""
        try:
            if self.signer is None:
                await self._get('/api/v1/status')
                return
            response = await self.signer.api_client.rest_client.request(
                'GET', self.profile.api_url + '/api/v1/status')
            try:
                await response.read()
                if not 200 <= response.status < 300:
                    raise RuntimeError('SDK keepalive HTTP failure')
            finally:
                response.response.release()
        except Exception as e:
            log.debug("[%s] keepalive ping failed: %r", self.name, e)

    async def fetch_funding(self, *, include_price: bool = False) -> FundingSnapshot:
        """Normalize the official comparison feed's 8h rate to bps/hour.

        elliottech/lighter-agent-kit references/schemas-read.md documents
        /funding-rates as 8-hour-equivalent, despite hourly actual payments.
        """
        started = time.time()
        data = await self._get("/api/v1/funding-rates")
        if not isinstance(data, dict) or data.get("code") != 200:
            raise RuntimeError(f"[{self.name}] funding rate request failed")
        rows = data.get("funding_rates")
        if not isinstance(rows, list):
            raise RuntimeError(f"[{self.name}] funding rates missing")
        matches = []
        for row in rows:
            if not isinstance(row, dict):
                raise RuntimeError(f"[{self.name}] malformed funding rate")
            if (row.get("market_id") != self.market_id
                    or row.get("exchange") != "lighter"
                    or row.get("symbol") != self.conf.symbol):
                continue
            matches.append(row)
        if len(matches) != 1:
            raise RuntimeError(f"[{self.name}] Lighter funding market unavailable")
        rate = float(funding_decimal(matches[0].get("rate"), "rate")) * 1e4 / 8
        if not math.isfinite(rate):
            raise RuntimeError(f"[{self.name}] non-finite funding rate")
        index = None
        if include_price:
            details = await self._get('/api/v1/orderBookDetails', params={'market_id': self.market_id})
            if not isinstance(details, dict) or details.get('code') != 200:
                raise RuntimeError(f'[{self.name}] funding index request failed')
            rows = details.get('order_book_details')
            if not isinstance(rows, list):
                raise RuntimeError(f'[{self.name}] funding index missing')
            prices = [r for r in rows if isinstance(r, dict) and r.get('market_id') == self.market_id
                      and r.get('symbol') == self.conf.symbol and r.get('market_type') == 'perp']
            if len(prices) != 1:
                raise RuntimeError(f'[{self.name}] funding index market unavailable')
            index = funding_decimal(prices[0].get('index_price'), 'index_price')
        snap = FundingSnapshot(rate, started if include_price else time.time(),
                               f"lighter:{self.profile.name}", index)
        self.funding = snap
        return snap

    def _fee_auth_header(self) -> dict:
        if self.signer is None:
            raise RuntimeError(f"[{self.name}] fee history requires signer")
        now = time.time()
        if (self._fee_auth_token is None
                or now >= self._fee_auth_expires_at):
            token, err = self.signer.create_auth_token_with_expiry()
            if err is not None:
                raise RuntimeError(f"[{self.name}] fee history auth: {err}")
            self._fee_auth_token = token
            self._fee_auth_expires_at = now + AUTH_REFRESH_SEC - 30.0
        return {"authorization": self._fee_auth_token}

    def _fee_to_usd(self, raw_fee) -> Optional[float]:
        """Normalize Lighter's quote-denominated integer fee field.

        The official SDK exposes fee fields as quote-precision integers, while
        some REST deployments serialize a decimal quote amount.  Handle both
        forms so a venue response cannot silently become a thousand-fold fee.
        """
        if raw_fee in (None, ""):
            return None
        try:
            value = float(raw_fee)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(value):
            return None
        text = str(raw_fee).strip().lower()
        if isinstance(raw_fee, int) or (text and "." not in text
                                         and "e" not in text):
            value /= 10 ** self.quote_decimals
        return value

    def _history_scan(self, kind: str, since: float, until: float) -> dict:
        scans = getattr(self, '_history_scans', None)
        if scans is None:
            scans = self._history_scans = OrderedDict()
        key = (kind, self.market_id, float(since))
        state = scans.get(key)
        if state is not None and state['complete']:
            if time.monotonic() - state['full_scan_mono'] >= HISTORY_CACHE_SEC:
                state = None  # Late postings require an occasional full refresh.
            elif until > state['until']:
                state.update(scan_since=state['until'], until=until,
                             cursor=None, seen=set(), pages=0, complete=False)
        if state is None:
            state = dict(scan_since=since, until=until, cursor=None, seen=set(),
                         pages=0, rows={}, complete=False, full_scan_mono=None,
                         lock=asyncio.Lock())
            scans[key] = state
        scans.move_to_end(key)
        while len(scans) > HISTORY_CACHE_WINDOWS:
            scans.popitem(last=False)
        return state

    @staticmethod
    def _finish_history_scan(state: dict, requested_until: float) -> None:
        state['complete'] = True
        if state['full_scan_mono'] is None:
            state['full_scan_mono'] = time.monotonic()
        if state['until'] < requested_until:
            raise LighterHistoryPending('history snapshot complete; newer window queued')

    async def fetch_fee_fills(self, since_ts: float,
                              until_ts: Optional[float] = None) -> list[dict]:
        """Fetch authenticated Lighter trades and select this account's leg."""
        c = self.conf.lighter_creds
        if c is None or c.account_index is None or self.market_id < 0:
            return []
        until = time.time() if until_ts is None else float(until_ts)
        params = {
            "sort_by": "timestamp",
            "limit": 100,
            "account_index": c.account_index,
            "market_id": self.market_id,
            # The current Lighter API only accepts descending order for this
            # endpoint. We sort the bounded response locally before parsing.
            "sort_dir": "desc",
            "role": "all",
            "type": "all",
            # aiohttp's query encoder accepts scalar strings/numbers, not
            # Python booleans. Lighter treats the string form identically.
            "aggregate": "false",
        }
        state = self._history_scan('trades', float(since_ts), until)
        async with state['lock']:
            rows = await self._read_fee_pages(state, params, float(since_ts), until)
            return [row for row in rows if float(since_ts) - 1.0 <= row['ts'] <= until + 1.0]

    async def _read_fee_pages(self, state, params, since_ts, until):
        if state['complete']:
            self._finish_history_scan(state, until)
            return list(state['rows'].values())
        result = []
        cursor = state['cursor']
        c = self.conf.lighter_creds
        oldest = None
        for _ in range(HISTORY_PAGES_PER_READ):
            if state['pages'] >= 20:
                raise RuntimeError('trade history exceeds bounded 20-page scan')
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/api/v1/trades", params=params,
                                   headers=self._fee_auth_header())
            received_ts = time.time()
            if not isinstance(data, dict):
                raise RuntimeError('trade history response unavailable')
            rows = data.get("trades") or data.get("data") or []
            if isinstance(rows, dict):
                rows = rows.get("trades") or []
            for trade in rows:
                if not isinstance(trade, dict):
                    continue
                try:
                    market_value = trade.get("market_id")
                    if market_value is not None and (isinstance(market_value, bool)
                            or str(market_value) != str(int(market_value))):
                        continue
                    trade_market = int(market_value) if market_value is not None else None
                except (TypeError, ValueError):
                    continue
                if trade_market is not None and trade_market != self.market_id:
                    continue
                try:
                    ts = float(trade.get("timestamp", trade.get("time")))
                    if ts > 10_000_000_000:
                        ts /= 1000.0
                    quantity_exact = funding_decimal(trade.get("size", trade.get(
                        "filled_base_amount")), "fill size").copy_abs()
                    price_exact = funding_decimal(trade.get("price", trade.get("px")),
                                                  "fill price")
                    qty, price = float(quantity_exact), float(price_exact)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(ts) or not math.isfinite(qty) \
                        or not math.isfinite(price) or qty <= 0 or price <= 0:
                    continue
                oldest = ts if oldest is None else min(oldest, ts)
                if ts < state['scan_since'] - 1.0 or ts > state['until'] + 1.0:
                    continue

                def _account_id(value):
                    try:
                        if isinstance(value, bool) or str(value) != str(int(value)):
                            return None
                        return int(value)
                    except (TypeError, ValueError):
                        return None

                account = int(c.account_index)
                ask_account = _account_id(trade.get("ask_account_id",
                                                     trade.get("ask_account_index")))
                bid_account = _account_id(trade.get("bid_account_id",
                                                     trade.get("bid_account_index")))
                maker_value = str(trade.get("is_maker_ask", "")).lower()
                maker_ask = (True if maker_value in {"1", "true", "yes"} else
                             False if maker_value in {"0", "false", "no"} else None)
                if ask_account == account:
                    side = "sell"
                    order_ref = trade.get("ask_client_id_str",
                                          trade.get("ask_client_id"))
                    exchange_order_id = trade.get("ask_id_str") or trade.get("ask_id")
                    role = ("maker" if maker_ask else "taker") if maker_ask is not None else None
                    closed_pnl = trade.get("ask_account_pnl")
                elif bid_account == account:
                    side = "buy"
                    order_ref = trade.get("bid_client_id_str",
                                          trade.get("bid_client_id"))
                    exchange_order_id = trade.get("bid_id_str") or trade.get("bid_id")
                    role = ("taker" if maker_ask else "maker") if maker_ask is not None else None
                    closed_pnl = trade.get("bid_account_pnl")
                else:
                    continue
                raw_fee = trade.get(f"{role}_fee") if role is not None else None
                trade_id = trade.get("trade_id_str")
                if trade_id in (None, ""):
                    trade_id = trade.get("trade_id")
                if trade_id in (None, ""):
                    trade_id = trade.get("id")
                key = (f"lighter:{trade_market}:{trade_id}"
                       if trade_market is not None and trade_id not in (None, "") else None)
                try:
                    fee_amount = (float(raw_fee)
                                  if raw_fee not in (None, "") else None)
                except (TypeError, ValueError):
                    fee_amount = None
                if fee_amount is not None and not math.isfinite(fee_amount):
                    fee_amount = None
                amounts = {}
                for name, value in (("closed_pnl", closed_pnl),
                                    ("position_size_before", trade.get(
                                        f"{role}_position_size_before") if role is not None else None)):
                    try:
                        amounts[name] = (str(funding_decimal(value, name))
                                         if value not in (None, "") else None)
                    except ValueError:
                        amounts[name] = None
                sign_changed = (trade.get(f"{role}_position_sign_changed")
                                if role is not None else None)
                execution_type = trade.get("type")
                result.append({
                    "exchange_fill_key": key,
                    "exchange_trade_id": trade_id,
                    "exchange_order_id": (str(exchange_order_id)
                                          if exchange_order_id not in (None, "") else None),
                    "order_ref": (None if order_ref in (None, "")
                                   else str(order_ref)),
                    "source_account": str(account),
                    "source_market": str(trade_market) if trade_market is not None else None,
                    "source_profile": getattr(getattr(self, 'profile', None), 'name', None),
                    "source_dex": None,
                    "source": "lighter:trades",
                    "evidence_received_ts": received_ts,
                    "ts": ts,
                    "side": side,
                    "qty": qty,
                    "price": price,
                    "quantity_exact": str(quantity_exact),
                    "price_exact": str(price_exact),
                    "start_position": None,  # SDK size-before has no explicit signed-position contract.
                    **amounts,
                    "position_sign_changed": sign_changed if type(sign_changed) is bool else None,
                    "account_role": role,
                    "direction": None,
                    "is_reduction": None,
                    "execution_type": execution_type,
                    "liquidation": ({"type": execution_type}
                                    if execution_type == "liquidation" else None),
                    "fee_amount": fee_amount,
                    "fee_usd": self._fee_to_usd(raw_fee),
                    "fee_token": "USDC",
                    "raw": trade,
                })
            next_cursor = (data.get("next_cursor") or
                           data.get("nextCursor") or data.get("cursor"))
            for index, fill in enumerate(result):
                # No stable trade ID means no deduplication claim. Keep each
                # appearance as an unbound fact instead of merging equal fills.
                key = fill['exchange_fill_key'] or ('unidentified', state['pages'], index)
                state['rows'][key] = fill
            result.clear()
            state['pages'] += 1
            if not next_cursor or not rows or (oldest is not None and oldest < state['scan_since'] - 1.0):
                self._finish_history_scan(state, until)
                return list(state['rows'].values())
            if str(next_cursor) in state['seen'] or str(next_cursor) == str(cursor):
                raise RuntimeError('trade history pagination stalled')
            cursor = str(next_cursor)
            state['seen'].add(cursor)
            state['cursor'] = cursor
        raise LighterHistoryPending('trade history continuation queued')

    async def fetch_funding_settlements(
            self, since_ts: float,
            until_ts: Optional[float] = None) -> list[FundingSettlement]:
        """Read positionFunding: epoch seconds and decimal quote/USD change.

        SDK PositionFunding/PositionFundings define funding_id, change and
        next_cursor; OpenAPI's timestamp example is 1640995200 (seconds).
        No rate-based amounts or zero-rate settlement evidence are inferred.
        Private accounts require the existing read-history auth token.
        """
        c = self.conf.lighter_creds
        if c is None or c.account_index is None or self.market_id < 0:
            raise RuntimeError(f"[{self.name}] account unavailable")
        since, until = funding_window(
            since_ts, time.time() if until_ts is None else until_ts)
        state = self._history_scan('funding', since, until)
        async with state['lock']:
            rows = await self._read_funding_pages(state, since, until, c)
            return [row for row in rows if since <= row.ts <= until]

    async def _read_funding_pages(self, state, since, until, c):
        if state['complete']:
            self._finish_history_scan(state, until)
            return sorted(state['rows'].values(), key=lambda event: (event.ts, event.event_id))
        params = {
            "account_index": c.account_index, "limit": FUNDING_HISTORY_PAGE_SIZE,
            "side": "all", "start_timestamp": math.floor(state['scan_since']),
            "end_timestamp": math.ceil(state['until']),
            "market_ids": str(self.market_id),
        }
        headers = self._fee_auth_header()
        if state['cursor'] is not None:
            params['cursor'] = state['cursor']
        events: dict[str, FundingSettlement] = {}
        for _ in range(HISTORY_PAGES_PER_READ):
            if state['pages'] >= FUNDING_HISTORY_MAX_PAGES:
                raise RuntimeError('funding history pagination limit reached')
            data = await self._get("/api/v1/positionFunding", params=dict(params),
                                   headers=headers)
            if not isinstance(data, dict) or data.get("code") != 200:
                raise RuntimeError(f"[{self.name}] funding history request failed")
            rows = data.get("position_fundings")
            if not isinstance(rows, list):
                raise RuntimeError(f"[{self.name}] funding history rows missing")
            for row in rows:
                if (not isinstance(row, dict)
                        or type(row.get("market_id")) is not int):
                    raise RuntimeError(f"[{self.name}] funding history market missing")
                if row["market_id"] != self.market_id:
                    continue
                if type(row.get("timestamp")) is not int:
                    raise ValueError("funding timestamp must be integer epoch seconds")
                ts = funding_timestamp(row["timestamp"])
                if not since <= ts <= state['until']:
                    continue
                identity = row.get("funding_id")
                if type(identity) is not int or identity < 0:
                    raise RuntimeError(f"[{self.name}] funding identity missing")
                qty = None
                if row.get("position_size") is not None:
                    qty = funding_decimal(row["position_size"], "position_size")
                    side = row.get("position_side")
                    if qty < 0 or side not in {"long", "short"}:
                        raise ValueError("invalid funding position size or side")
                    if side == "short":
                        qty = -qty
                event = FundingSettlement(
                    event_id=f"lighter:{self.profile.name}:{c.account_index}:{self.market_id}:{identity}",
                    ts=ts, amount_usd=funding_decimal(row.get("change"), "change"),
                    position_qty=qty,
                    source=f"lighter:{self.profile.name}:positionFunding")
                previous = events.get(event.event_id) or state['rows'].get(event.event_id)
                if previous is not None and previous != event:
                    raise RuntimeError(f"[{self.name}] conflicting funding history")
                events[event.event_id] = event
            cursor = data.get("next_cursor")
            if cursor in (None, ""):
                if len(rows) >= FUNDING_HISTORY_PAGE_SIZE:
                    raise RuntimeError(f"[{self.name}] full funding page has no cursor")
                state['rows'].update(events)
                state['pages'] += 1
                self._finish_history_scan(state, until)
                return sorted(state['rows'].values(), key=lambda event: (event.ts, event.event_id))
            if not isinstance(cursor, str) or cursor in state['seen'] or not rows:
                raise RuntimeError(f"[{self.name}] funding history pagination stalled")
            state['rows'].update(events)
            state['pages'] += 1
            state['seen'].add(cursor)
            state['cursor'] = cursor
            params["cursor"] = cursor
        raise LighterHistoryPending('funding history continuation queued')

    async def fetch_funding_payments(self, since_ts: float,
                                     until_ts: Optional[float] = None) -> float:
        """Return the same signed cash-flow total, across all history pages."""
        events = await self.fetch_funding_settlements(since_ts, until_ts)
        return float(sum((event.amount_usd for event in events), Decimal(0)))

    async def fetch_funding_risk(self) -> FundingRiskSnapshot:
        if self.market_id < 0:
            raise RuntimeError(f"[{self.name}] funding risk market unavailable")
        started = time.time()
        account, data = await asyncio.gather(
            self.fetch_account_snapshot(),
            self._get("/api/v1/orderBookDetails", params={"market_id": self.market_id}))
        credentials = self.conf.lighter_creds
        account_index = getattr(account, 'account_index', None)
        if (credentials is None or type(credentials.account_index) is not int
                or type(account_index) is not int
                or account_index != credentials.account_index):
            raise RuntimeError(f"[{self.name}] funding risk account identity unavailable or mismatched")
        account_ts = funding_timestamp(account.received_ts)
        # These official REST schemas have no server snapshot timestamp. Use
        # the oldest local observation, never book age or market created_at.
        ts = min(account_ts, started)
        now = time.time()
        if account_ts > now or not 0 <= now - ts <= ACCOUNT_WS_MAX_AGE_SEC:
            raise RuntimeError(f"[{self.name}] funding risk snapshot stale or future")
        if not isinstance(account.positions, (tuple, list)):
            raise RuntimeError(f"[{self.name}] funding risk positions missing")
        positions = []
        for row in account.positions:
            if not isinstance(row, dict) or type(row.get("market_id")) is not int:
                raise RuntimeError(f"[{self.name}] malformed funding risk position")
            if row["market_id"] == self.market_id:
                positions.append(row)
        if len(positions) > 1:
            raise RuntimeError(f"[{self.name}] duplicate funding risk position")
        qty, liquidation = Decimal(0), None
        if positions:
            position = positions[0]
            qty = funding_decimal(position.get("position"), "position")
            sign = position.get("sign")
            if qty < 0 or type(sign) is not int or sign not in {-1, 1}:
                raise ValueError("invalid funding risk position or sign")
            qty *= sign
            if qty != 0 and position.get("liquidation_price") is not None:
                liquidation = funding_decimal(position["liquidation_price"], "liquidation_price")
                if liquidation == 0:
                    liquidation = None  # no reported threshold, not proof of safety
        if not isinstance(data, dict) or data.get("code") != 200:
            raise RuntimeError(f"[{self.name}] funding risk mark request failed")
        details = data.get("order_book_details")
        if not isinstance(details, list):
            raise RuntimeError(f"[{self.name}] funding risk book details missing")
        marks = [row for row in details if isinstance(row, dict)
                 and row.get("market_id") == self.market_id
                 and row.get("symbol") == self.conf.symbol
                 and row.get("market_type") == "perp"]
        if len(marks) != 1:
            raise RuntimeError(f"[{self.name}] funding risk mark unavailable")
        return FundingRiskSnapshot(
            qty, funding_decimal(marks[0].get("mark_price"), "mark_price"),
            liquidation, ts,
            f"lighter:{self.profile.name}:account:{account.source}+orderBookDetails")

    # ------------------------------------------------------------ price grid

    def px_round(self, px: float, round_up: bool) -> float:
        f = 10 ** self.price_decimals
        v = math.ceil(px * f - 1e-9) / f if round_up else math.floor(px * f + 1e-9) / f
        return round(v, 8)

    # ------------------------------------------------------------- execution

    def _next_coi(self, order_ref: Optional[str] = None) -> int:
        if order_ref not in (None, ""):
            try:
                value = int(order_ref)
                if 0 < value <= MAX_CLIENT_ORDER_INDEX:
                    self._coi = max(self._coi, value)
                    return value
                if value > MAX_CLIENT_ORDER_INDEX:
                    log.warning("[%s] ignoring out-of-range client order "
                                "index %s", self.name, value)
            except (TypeError, ValueError):
                pass
        if self._coi >= MAX_CLIENT_ORDER_INDEX:
            raise RuntimeError("Lighter client order index exhausted")
        self._coi += 1
        return self._coi

    def _submission_done(self, task: asyncio.Task) -> None:
        self._submit_tasks.discard(task)
        prepared = getattr(task, '_lighter_dispatch', None)
        if prepared is not None:
            # A task cancelled before its first turn never enters async finally.
            prepared._finish()
        context = getattr(task, '_lighter_context', {})
        trace = context.get('timing_trace')
        if trace is not None:
            trace.mark('submit_wrapper_done')
            if 'runtime_consumed' in trace.data['stages']:
                trace.emit()
        if context.get('post_started'):
            self._invalidate_account_cache()
        # A WS terminal may win the race; still drain the SDK request so its
        # nonce bookkeeping finishes. Always retrieve background exceptions.
        if not task.cancelled():
            exc = task.exception()
            if exc is not None:
                if not context.get('post_started') and isinstance(exc, RequestDeferred):
                    log.debug('[%s] order locally deferred before POST: %s', self.name, exc.reason)
                    return
                log.warning("[%s] order submission ended with %s; use account "
                            "order confirmation to resolve outcome",
                            self.name, type(exc).__name__)

    async def wait_for_submission(self) -> None:
        """Drain prior nonce bookkeeping before selecting a new hedge target."""
        pending = tuple(getattr(self, '_submit_tasks', ()))
        if pending:
            await asyncio.shield(asyncio.gather(*pending, return_exceptions=True))

    def _submission_cost(self) -> int:
        """Inspect SDK cache only; SDK remains the sole nonce selector."""
        manager = getattr(self.signer, 'nonce_manager', None)
        cached = getattr(manager, 'nonce', {})
        if type(manager).__name__ == 'ApiNonceManager':
            return 2
        keys = getattr(manager, 'api_keys_list', ())
        if keys:
            next_key = keys[(manager.current + 1) % len(keys)]
            return 1 if next_key in cached else 2
        # Some adapters expose a single cached key without rotation metadata.
        return 1 if len(cached) == 1 else 2

    def reserve_completion(self) -> RequestTicket:
        """Preflight the second leg locally, before creating first-leg exposure."""
        return self._request_budget().reserve(self._submission_cost(), priority=0)

    def release_completion(self, ticket: RequestTicket | None) -> None:
        if ticket is not None:
            owner = ticket.owner
            if owner is not None:
                owner.release()
            else:
                self._request_budget().release_reserved(ticket)

    async def prepare_dispatch(self, *, order_ref: str,
                               reduce_only: bool = False, completion: bool = False,
                               completion_ticket: RequestTicket | None = None) -> DispatchToken:
        """Own nonce bookkeeping and physical admission before journaling a send."""
        if not hasattr(self, '_submission_lock'):
            self._submission_lock = asyncio.Lock()
        trace = self.order_timing(order_ref, create=True)
        if trace is not None:
            trace.mark('prepare_start', first=True)
            trace.mark('prepare_start_last')
            trace.data['prepare_attempts'] = trace.data.get('prepare_attempts', 0) + 1
        await self._submission_lock.acquire()
        if trace is not None:
            trace.mark('submission_lock_acquired', first=True)
        ticket = completion_ticket
        try:
            await self.wait_for_submission()
            if trace is not None:
                trace.mark('prior_submission_drained', first=True)
            priority = 0 if reduce_only or completion or ticket is not None else 1
            cost = self._submission_cost()
            budget = self._request_budget()
            if ticket is None:
                ticket = budget.reserve(cost, priority=priority)
            else:
                if ticket.budget is not budget or any(
                        event.priority != priority for event in ticket.events
                        if event.state == 'reserved'):
                    raise ValueError('invalid completion ticket')
                budget.renew_reserved(ticket, cost)
            prepared = DispatchToken(self, str(order_ref), ticket, priority)
            ticket.owner = prepared
            if trace is not None:
                trace.mark('prepare_end', first=True)
                trace.mark('prepare_end_last')
            return prepared
        except BaseException:
            if ticket is not None and ticket.budget is self._request_budget():
                self._request_budget().release_reserved(ticket)
            self._submission_lock.release()
            raise

    @staticmethod
    def _deferred_result(order_ref, exc: RequestDeferred) -> dict:
        return {'status': 'deferred', 'filled_base': 0.0, 'avg_px': None,
                'fee_usd': 0.0, 'err': f'proved_local_unsent: {exc.reason}',
                'unresolved': False, 'order_ref': str(order_ref),
                'deferred': True, 'retry_after': exc.retry_after,
                'post_started': False, 'proved_local_unsent': True}

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False,
                         order_ref: Optional[str] = None,
                         completion: bool = False,
                         completion_ticket: RequestTicket | None = None,
                         dispatch_token: DispatchToken | None = None,
                         on_progress=None) -> dict:
        """Market order with avg-price protection; settle via account ws."""
        assert self.signer is not None
        from lighter import SignerClient
        if not hasattr(self, "_submit_tasks"):
            self._submit_tasks = set()
        coi = self._next_coi(order_ref)
        trace = self.order_timing(coi, create=True, market_id=self.market_id,
            side='buy' if is_buy else 'sell', requested_base=qty,
            limit_px=limit_px, reduce_only=reduce_only)
        try:
            prepared = dispatch_token or await self.prepare_dispatch(
                order_ref=str(coi), reduce_only=reduce_only, completion=completion,
                completion_ticket=completion_ticket)
        except RequestDeferred as exc:
            return self._deferred_result(coi, exc)
        if prepared.venue is not self or prepared.order_ref != str(coi) \
                or prepared.claimed or prepared.released:
            if prepared.venue is self:
                prepared.release()
            raise ValueError('invalid dispatch token')
        ticket = prepared.ticket
        try:
            base_amount = int(round(qty * 10 ** self.size_decimals))
            price = int(round(limit_px * 10 ** self.price_decimals))
            register = getattr(self.orders_feed, 'register_order', None)
            if register is not None:
                register(coi, requested_base=Decimal(base_amount).scaleb(-self.size_decimals),
                    is_ask=not is_buy, on_progress=on_progress)
            fut = self.orders_feed.watch(coi) if self.orders_feed else None
        except BaseException:
            prepared.release()
            raise
        started = time.perf_counter()

        def confirmed(info):
            self._invalidate_account_cache()
            if trace is not None:
                trace.result(status=info['status'], filled_base=info['filled_base'],
                    average=info.get('avg_px'), source=info.get('confirmation_source', 'account_orders_ws'))
                trace.mark('send_taker_return')
            return {"status": info["status"], "filled_base": info["filled_base"],
                    "avg_px": info.get("avg_px"), "err": None,
                    "unresolved": False, "order_ref": str(coi),
                    "confirmation_source": info.get('confirmation_source', 'account_orders_ws'),
                    "confirmation_ms": round((time.perf_counter() - started) * 1000, 3)}

        context = {'ticket': ticket, 'post_started': False, 'priority': prepared.priority,
                   'order_ref': str(coi),
                   'timing_trace': trace,
                   'increase_guard': (getattr(self, 'submission_guard', None)
                                      if not reduce_only else None)}
        async def submit():
            if trace is not None:
                trace.mark('submit_wrapper_start', first=True)
            token = SDK_REQUEST_CONTEXT.set(context)
            manager = getattr(self.signer, 'nonce_manager', None)
            try:
                return await self.signer.create_order(
                    market_index=self.market_id, client_order_index=coi,
                    base_amount=base_amount, price=price, is_ask=not is_buy,
                    order_type=SignerClient.ORDER_TYPE_MARKET,
                    time_in_force=SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                    reduce_only=reduce_only, order_expiry=SignerClient.DEFAULT_IOC_EXPIRY)
            except BaseException:
                if not context['post_started']:
                    self._restore_local_nonce(manager, context)
                raise
            finally:
                SDK_REQUEST_CONTEXT.reset(token)
                prepared._finish()
        prepared.claimed = True
        submission = asyncio.create_task(submit(), name=f'lighter-submit-{coi}')
        submission._lighter_context = context
        submission._lighter_dispatch = prepared
        self._submit_tasks.add(submission)
        submission.add_done_callback(self._submission_done)
        try:
            if fut is not None:
                await asyncio.wait({submission, fut},
                                   return_when=asyncio.FIRST_COMPLETED)
                # If both completed together, authoritative terminal wins.
                if fut.done() and not fut.cancelled():
                    return confirmed(fut.result())
            result = await self._await_submission(submission, fut, coi, confirmed)
            if trace is not None:
                trace.mark('send_taker_return')
                trace.result(status=result['status'], filled_base=result['filled_base'],
                    average=result.get('avg_px'), source=result.get('confirmation_source'),
                    timeout_reason='settlement_wait_timeout' if result['status'] == 'timeout' else None)
            return result
        finally:
            if fut is not None:
                self.orders_feed.unwatch(coi)

    async def _await_submission(self, submission, fut, coi, confirmed) -> dict:
        def websocket_terminal():
            if fut is not None and fut.done() and not fut.cancelled():
                return fut.result()
            feed = self.orders_feed
            return feed.terminal(coi) if feed is not None else None
        try:
            _tx, resp, err = await asyncio.shield(submission)
        except Exception as e:
            observed = websocket_terminal()
            if observed is not None:
                return confirmed(observed)
            trace = submission._lighter_context.get('timing_trace')
            if trace is not None:
                trace.data['submit_exception_type'] = type(e).__name__[:64]
                if isinstance(e, asyncio.TimeoutError):
                    trace.data['timeout_reason'] = 'sdk_request_timeout'
            if not submission._lighter_context['post_started']:
                if isinstance(e, RequestDeferred):
                    return self._deferred_result(coi, e)
                blocked = self._request_budget().snapshot()
                if blocked['blocked_for_sec'] > 0:
                    return self._deferred_result(coi, RequestDeferred(
                        blocked['blocked_reason'], blocked['blocked_for_sec']))
            if fut is not None:
                self.orders_feed.unwatch(coi)
            msg = f"{type(e).__name__}: {e}"
            if getattr(e, "status", None) == 429 or "(429)" in str(e):
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": True, "order_ref": str(coi)}
        observed = websocket_terminal()
        if observed is not None:
            return confirmed(observed)
        if err is not None or (getattr(resp, "code", 200) or 200) != 200:
            if fut is not None:
                self.orders_feed.unwatch(coi)
            msg = str(err) if err is not None else \
                f"tx rejected code={resp.code} msg={getattr(resp, 'message', None)}"
            if "rate limit" in msg.lower():
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": True, "order_ref": str(coi)}
        if fut is None:
            return {"status": "sent-unconfirmed", "filled_base": 0.0,
                    "avg_px": None, "err": None, "unresolved": True,
                    "order_ref": str(coi)}
        try:
            trace = submission._lighter_context.get('timing_trace')
            if trace is not None:
                trace.mark('settlement_wait_start', first=True)
                trace.data['settlement_timeout_seconds'] = self.settle_timeout
            info = await asyncio.wait_for(fut, timeout=self.settle_timeout)
            return confirmed(info)
        except asyncio.TimeoutError:
            self.orders_feed.unwatch(coi)
            log.warning("[%s] no settle confirmation for coi %d in %.1fs",
                        self.name, coi, self.settle_timeout)
            return {"status": "timeout", "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True, "order_ref": str(coi)}
        finally:
            if trace is not None:
                trace.mark('settlement_wait_end')

    def register_order_identity(self, order_ref, *, qty: float, is_buy: bool) -> None:
        feed = self.orders_feed
        coi = int(order_ref)
        if feed is not None and feed.terminal(coi) is None:
            feed.register_order(coi, requested_base=qty, is_ask=not is_buy)

    async def resolve_order(self, order_ref, timeout: Optional[float] = None) -> dict:
        """Resolve a timed-out order without submitting a duplicate."""
        try:
            coi = int(order_ref)
        except (TypeError, ValueError):
            return {"status": "unresolved", "filled_base": 0.0,
                    "avg_px": None, "err": "invalid order reference",
                    "unresolved": True, "order_ref": order_ref}
        if not 0 < coi <= MAX_CLIENT_ORDER_INDEX:
            return {"status": "rejected", "filled_base": 0.0,
                    "avg_px": None,
                    "err": "invalid Lighter client order index",
                    "unresolved": False, "order_ref": order_ref}
        feed = self.orders_feed
        if feed is None:
            return {"status": "unresolved", "filled_base": 0.0,
                    "avg_px": None, "err": "account order stream unavailable",
                    "unresolved": True, "order_ref": order_ref}
        cached = feed.terminal(coi)
        if cached is not None:
            return {"status": cached["status"],
                    "filled_base": cached["filled_base"],
                    "avg_px": cached.get("avg_px"), "err": None,
                    "unresolved": False, "order_ref": order_ref}
        lookups = getattr(self, '_order_lookups', None)
        if lookups is None:
            lookups = self._order_lookups = OrderedDict()
        entry = lookups.get(coi)
        now = time.monotonic()
        if entry is None or entry[0].done():
            task = asyncio.create_task(self._lookup_order(coi))
            # Retrieve errors even when a WS terminal wins or all callers cancel.
            task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
            entry = lookups[coi] = (task, now)
            lookups.move_to_end(coi)
            while len(lookups) > 512:
                key, old = next(iter(lookups.items()))
                if not old[0].done():
                    break
                lookups.pop(key)
        task = entry[0]
        wait_for = min(self.settle_timeout if timeout is None else max(float(timeout), 0.0), 1.0)
        wait_terminal = getattr(feed, 'wait_terminal', None)
        websocket = asyncio.create_task(wait_terminal(coi)) if wait_terminal is not None else None
        try:
            await asyncio.wait({task, websocket} if websocket is not None else {task},
                               timeout=wait_for, return_when=asyncio.FIRST_COMPLETED)
            info = task.result() if task.done() else None
        finally:
            if websocket is not None:
                websocket.cancel()
                await asyncio.gather(websocket, return_exceptions=True)
        cached = feed.terminal(coi)
        if cached is not None:
            return {'status': cached['status'], 'filled_base': cached['filled_base'],
                    'avg_px': cached.get('avg_px'), 'err': None,
                    'unresolved': False, 'order_ref': order_ref}
        return info or {'status': 'unresolved', 'filled_base': 0.0, 'avg_px': None,
                       'err': 'order lookup pending',
                       'unresolved': True, 'order_ref': order_ref}

    async def _lookup_order(self, coi: int) -> dict | None:
        trace = self.order_timing(coi)
        lookup = trace.request('lookups', source='accountOrders_rest') if trace is not None else None
        try:
            data = await self._get('/api/v1/accountOrders', params={
                'client_order_indexes': str(coi),
                'account_index': self.conf.lighter_creds.account_index,
            }, headers=self._fee_auth_header())
            rows = data.get('orders') or data.get('data') or []
            if isinstance(rows, dict):
                rows = rows.get('orders') or []
            for order in rows:
                if int(order.get('client_order_index', -1)) != coi:
                    continue
                if not self.orders_feed._trusted_registered_order(
                        coi, order, source='accountOrders_rest'):
                    continue
                filled = float(order['filled_base_amount'])
                if not math.isfinite(filled) or filled < 0:
                    raise ValueError('invalid cumulative REST fill')
                raw_quote = order.get('filled_quote_amount')
                quote = float(raw_quote) if raw_quote is not None else None
                if quote is not None and (not math.isfinite(quote) or quote < 0):
                    raise ValueError('invalid cumulative REST quote')
                status = str(order.get('status') or 'unresolved')
                terminal = status.strip().lower() in TERMINAL_STATUSES
                info = {'status': status, 'filled_base': filled,
                        'filled_quote': quote,
                        'avg_px': quote / filled if filled > 0 and quote is not None and quote > 0 else None,
                        'err': None, 'unresolved': not terminal, 'order_ref': str(coi)}
                if trace is not None:
                    trace.identity(order, 'accountOrders_rest:Order')
                    if lookup is not None:
                        lookup['result_status'] = status[:128]
                    trace.result(status=status, filled_base=filled,
                        average=info['avg_px'], source='accountOrders_rest')
                if terminal:
                    self.orders_feed._resolve(coi, info, source='accountOrders_rest')
                    self._invalidate_account_cache()
                return info
        except RequestDeferred as exc:
            if lookup is not None:
                lookup['outcome'] = 'deferred'
            return {'status': 'unresolved', 'filled_base': 0.0, 'avg_px': None,
                    'err': f'order lookup deferred: {exc.reason}',
                    'unresolved': True, 'order_ref': str(coi),
                    'retry_after': exc.retry_after}
        except Exception as exc:
            if lookup is not None:
                lookup['exception_type'] = type(exc).__name__
                lookup['outcome'] = 'timeout' if isinstance(exc, asyncio.TimeoutError) else 'exception'
            log.debug('[%s] accountOrders recovery deferred: %s', self.name, type(exc).__name__)
        finally:
            if lookup is not None:
                lookup['end'] = stamp()
                if 'runtime_consumed' in trace.data['stages']:
                    trace.emit()
        return None

    # -------------------------------------------------------------- accounts

    @staticmethod
    def _as_dict(value) -> dict:
        if isinstance(value, dict):
            return value
        convert = getattr(value, "to_dict", None)
        if callable(convert):
            result = convert()
            if isinstance(result, dict):
                return result
        raise TypeError(f"unsupported account response {type(value).__name__}")

    async def _raw_account(self) -> Optional[dict]:
        c = self.conf.lighter_creds
        if c is None or c.account_index is None:
            return None
        data = await self._get("/api/v1/account",
                               params={"by": "index",
                                       "value": str(c.account_index)})
        accounts = data.get("accounts") if isinstance(data, dict) else None
        if not isinstance(accounts, list) or not accounts:
            return None
        if len(accounts) != 1:
            raise RuntimeError(f"[{self.name}] account response identity is ambiguous")
        account = self._as_dict(accounts[0])
        self._snapshot_account_index(account)
        return account

    def _snapshot_account_index(self, account: dict) -> int | None:
        indices = [account[key] for key in ('index', 'account_index') if key in account]
        if not indices:
            return None
        expected = self.conf.lighter_creds.account_index
        if any(type(index) is not int or index != expected for index in indices):
            raise RuntimeError(f"[{self.name}] account response identity mismatch")
        return expected

    def _account_rest_allowed(self) -> None:
        snapshot = self._request_budget().snapshot()
        if snapshot['blocked_reason'] == 'captcha':
            raise LighterWafBlocked('AWS WAF CAPTCHA; shared request circuit open')

    @staticmethod
    def _position_from_rows(positions, market_id: int) -> float:
        rows = positions.values() if isinstance(positions, dict) else positions
        for row in rows or ():
            if not isinstance(row, dict):
                continue
            try:
                if int(row.get("market_id", -1)) != market_id:
                    continue
                return (float(row.get("sign") or 1.0)
                        * float(row.get("position") or 0.0))
            except (TypeError, ValueError):
                raise RuntimeError("invalid position in account snapshot")
        return 0.0

    def _invalidate_account_cache(self) -> None:
        self._account_epoch = getattr(self, '_account_epoch', 0) + 1
        self._account_cache = None

    async def _read_account_snapshot(self, epoch: int, revision) -> AccountSnapshot:
        started_ts = time.time()
        account = await self._raw_account()
        if account is None or not isinstance(account.get('positions'), list):
            raise RuntimeError(f'[{self.name}] account positions unavailable')
        try:
            equity = float(account['total_asset_value'])
            if not math.isfinite(equity):
                raise ValueError('non-finite equity')
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f'[{self.name}] account equity unavailable') from exc
        feed = getattr(self, 'orders_feed', None)
        current_revision = ((feed.connection_generation, feed.evidence_revision)
                            if feed is not None else None)
        if epoch != getattr(self, '_account_epoch', 0) or revision != current_revision:
            raise RuntimeError('account read superseded by new order evidence')
        snapshot = AccountSnapshot(
            equity=equity, available=float(account.get('available_balance') or 0.0),
            positions=tuple(self._as_dict(row) for row in account['positions']),
            received_ts=started_ts, source='raw_rest_fallback',
            account_index=self._snapshot_account_index(account))
        if time.time() - started_ts >= ACCOUNT_REST_CACHE_SEC:
            raise LighterAccountStale('account REST response exceeded cache freshness')
        self._account_cache = (snapshot, time.monotonic() - max(0, time.time() - started_ts))
        return snapshot

    async def fetch_account_snapshot(self, *, force_refresh: bool = False) -> AccountSnapshot:
        feed = getattr(self, 'orders_feed', None)
        revision = ((feed.connection_generation, feed.evidence_revision)
                    if feed is not None else None)
        if revision != getattr(self, '_account_feed_revision', None):
            self._account_feed_revision = revision
            self._invalidate_account_cache()
        cached = getattr(self, '_account_cache', None)
        if not force_refresh and cached is not None \
                and time.monotonic() - cached[1] < ACCOUNT_REST_CACHE_SEC:
            return cached[0]  # Keep the original evidence timestamp.
        epoch = getattr(self, '_account_epoch', 0)
        active = getattr(self, '_account_task', None)
        if active is None or active[0].done() or active[1] != epoch:
            task = asyncio.create_task(self._read_account_snapshot(epoch, revision))
            task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
            self._account_task = (task, epoch)
        else:
            task = active[0]
        return await asyncio.shield(task)

    async def _account(self) -> Optional[dict]:
        snapshot = await self.fetch_account_snapshot()
        return {
            "total_asset_value": snapshot.equity,
            "available_balance": snapshot.available,
            "positions": list(snapshot.positions),
        }

    async def fetch_equity(self):
        feed = getattr(self, "orders_feed", None)
        snapshot = feed.fresh_account_snapshot() if feed is not None else None
        if snapshot is None:
            snapshot = await self.fetch_account_snapshot()
        return snapshot.equity, snapshot.available

    async def fetch_position_snapshot(self, *, force_rest: bool = False) -> VenuePositionSnapshot:
        feed = getattr(self, "orders_feed", None)
        snapshot = (feed.fresh_account_snapshot()
                    if feed is not None and not force_rest else None)
        if snapshot is not None:
            return VenuePositionSnapshot(
                self._position_from_rows(snapshot.positions, self.market_id),
                snapshot.received_ts, True, snapshot.source, snapshot.generation)
        rest = (await self.fetch_account_snapshot(force_refresh=True) if force_rest
                else await self.fetch_account_snapshot())
        return VenuePositionSnapshot(
            self._position_from_rows(rest.positions, self.market_id),
            rest.received_ts, True, f"lighter_account_rest:{rest.source}")

    async def fetch_position(self) -> float:
        return (await self.fetch_position_snapshot()).position

    async def _audit_account_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=ACCOUNT_REST_AUDIT_SEC)
                continue
            except asyncio.TimeoutError:
                pass
            try:
                rest = await self.fetch_account_snapshot(force_refresh=True)
                feed = self.orders_feed
                ws = feed.fresh_account_snapshot() if feed is not None else None
                if ws is not None and self._position_from_rows(
                        ws.positions, self.market_id) != self._position_from_rows(
                            rest.positions, self.market_id):
                    log.error("[%s] account WS/REST position mismatch; WS invalidated",
                              self.name)
                    feed._invalidate_account()
            except asyncio.CancelledError:
                raise
            except LighterWafBlocked:
                # The websocket remains authoritative; the circuit breaker
                # prevents audit failures from becoming a REST request storm.
                pass
            except Exception as exc:
                log.warning("[%s] account REST audit failed: %s", self.name, exc)

    async def close(self) -> None:
        reads = [item[0] for item in getattr(self, '_order_lookups', {}).values() if not item[0].done()]
        account = getattr(self, '_account_task', None)
        if account is not None and not account[0].done():
            reads.append(account[0])
        for task in reads:
            task.cancel()
        await asyncio.gather(*reads, return_exceptions=True)
        pending = tuple(getattr(self, "_submit_tasks", ()))
        if pending:
            # Shutdown only: give requests time to finish before closing SDK.
            _, unfinished = await asyncio.wait(pending, timeout=REST_TIMEOUT)
            for task in unfinished:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        http = getattr(self, '_lighter_http_session', None)
        if http is not None:
            await http.close()
        if self.signer is not None:
            try:
                await self.signer.api_client.close()
            except Exception:
                pass
