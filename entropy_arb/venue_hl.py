"""Hyperliquid HIP-3 dex venue adapter (Entropy = dex "io", trade.xyz = "xyz").

Market metadata, account state and order posting use Hyperliquid's public
/info and /exchange REST endpoints via plain aiohttp; the book comes from the
OFFICIAL websocket (see feeds.HLBookFeed). Trading lazily imports the
official `hyperliquid-python-sdk` signing helpers + eth_account —
--record-only data collection needs neither.

IOC limit orders settle synchronously in the /exchange response; unknown
outcomes (timeout/5xx) fall back to orderStatus-by-cloid polling inside
send_taker(), so the engine sees the same unified result shape as the Lighter
venue: {status, filled_base, avg_px, err, unresolved}.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import socket
import time
from collections import OrderedDict
from decimal import Decimal
from typing import Optional

import aiohttp

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect  # type: ignore

from .book import OrderBook
from .config import VenueConf
from .feeds import HLBookFeed
from .funding import (FundingRiskSnapshot, FundingSettlement, FundingSnapshot,
                      funding_decimal, funding_timestamp, funding_window)
from .order_timing import (HTTP_TIMING_CONTEXT, OrderTimingCollector,
                          attach_http_timing, stamp)

log = logging.getLogger("hl")

INFO_TIMEOUT = 10.0
FUNDING_HISTORY_PAGE_SIZE = 500
FUNDING_HISTORY_MAX_PAGES = 100
FUNDING_RISK_MAX_AGE_SEC = 20.0
STABLE_FEE_TOKENS = {"USD", "USDC", "USDC.E", "USDT", "USDE"}
OPEN_ORDER_STATUSES = {"open", "triggered"}
TERMINAL_ORDER_STATUSES = {
    "filled", "canceled", "rejected", "margincanceled", "reduceonlycanceled",
    "selftradecanceled", "scheduledcancel", "vaultwithdrawalcanceled",
    "openinterestcapcanceled", "siblingfilledcanceled", "delistedcanceled",
    "liquidatedcanceled", "tickrejected", "mintradentlrejected",
    "perpmarginrejected", "reduceonlyrejected", "badalopxrejected",
    "badtriggerpxrejected", "marketordernoliquidityrejected",
    "positionincreaseatopeninterestcaprejected", "positionflipatopeninterestcaprejected",
    "tooaggressiveatopeninterestcaprejected", "openinterestincreaserejected",
    "insufficientspotbalancerejected", "oraclerejected", "perpmaxpositionrejected",
}


def _order_filled_size(order: dict) -> float:
    """Missing or contradictory quantities are not evidence of zero fills."""
    original, remaining = float(order["origSz"]), float(order["sz"])
    if not (math.isfinite(original) and math.isfinite(remaining)
            and 0 <= remaining <= original):
        raise ValueError("invalid cumulative order quantity")
    return original - remaining


class HLOrdersFeed:
    """Authoritative order/fill stream keyed by the submitted client id.

    Hyperliquid's order update carries ``cloid`` and ``oid`` while the fill
    update carries ``oid`` and the actual price. Combining both lets the
    engine hedge an IOC as soon as the block is streamed instead of waiting
    for the slower HTTP response body.
    """

    def __init__(self, name: str, ws_url: str, user: str, coin: str) -> None:
        self.name = name
        self.ws_url = ws_url
        self.user = user.lower()
        self.coin = coin
        self.ready = asyncio.Event()
        self.connection_generation = 0
        self.evidence_revision = 0
        self._pending: dict[str, set[asyncio.Future]] = {}
        self._terminal: OrderedDict[str, dict] = OrderedDict()
        self._orders: OrderedDict[int, dict] = OrderedDict()
        self._fills: OrderedDict[int, dict] = OrderedDict()
        self._fill_ids: dict[int, set[str]] = {}
        self._pinned: set[str] = set()
        # Retired IDs cannot be reconstructed from a partial replay. Queries
        # for evicted history fall back to orderStatus instead.
        self._retired_oids: set[int] = set()
        self.timing_collector = None

    def _observe_timing(self, cloid, *, received, parsed, boundary):
        try:
            trace = self.timing_collector.get(self._key(cloid)) if self.timing_collector is not None else None
            if trace is not None:
                trace.observe_ws(received=received, parsed=parsed, boundary=boundary, source='order_fills_ws')
        except Exception:
            pass

    @staticmethod
    def _key(value) -> str:
        return str(value or "").strip().lower()

    def _prune_history(self, limit: int = 512) -> None:
        for key in list(self._terminal):
            if len(self._terminal) <= limit:
                break
            if key in self._pinned or key in self._pending:
                continue
            info = self._terminal.pop(key)
            oid = info.get("oid")
            if oid is not None:
                self._retired_oids.add(oid)
                self._orders.pop(oid, None)
                self._fills.pop(oid, None)
                self._fill_ids.pop(oid, None)

    def release_evidence(self, cloid) -> None:
        """Owner may release only after durable terminal application/recovery.

        Unwatching merely cancels a reader and intentionally does not release
        evidence. Unresolved orders and fills without an order stay pinned.
        """
        self._pinned.discard(self._key(cloid))
        self._prune_history()

    def watch(self, cloid) -> asyncio.Future:
        key = self._key(cloid)
        self._pinned.add(key)
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        if key in self._terminal:
            future.set_result(self._terminal[key])
        else:
            self._pending.setdefault(key, set()).add(future)
        return future

    def unwatch(self, cloid, future: asyncio.Future | None = None) -> None:
        key = self._key(cloid)
        pending = self._pending.get(key, set())
        removing = {future} if future is not None else set(pending)
        for waiter in removing:
            pending.discard(waiter)
            if not waiter.done():
                waiter.cancel()
        if not pending:
            self._pending.pop(key, None)

    def terminal(self, cloid) -> Optional[dict]:
        return self._terminal.get(self._key(cloid))

    def _resolve(self, cloid, info: dict) -> None:
        key = self._key(cloid)
        if not key:
            return
        try:
            trace = self.timing_collector.get(key) if self.timing_collector is not None else None
            if trace is not None:
                trace.mark('terminal_published_first', first=True)
                trace.mark('terminal_published')
                trace.result(status=info['status'], filled_base=info.get('filled_base', 0),
                    average=info.get('avg_px'), source='order_fills_ws')
                if 'runtime_consumed' in trace.data['stages']:
                    trace.emit()
        except Exception:
            pass
        self._terminal[key] = info
        self._prune_history()
        for future in self._pending.pop(key, ()):
            if not future.done():
                future.set_result(info)

    def _maybe_resolve(self, oid: int) -> None:
        order = self._orders.get(oid)
        if not order or order["status"] not in TERMINAL_ORDER_STATUSES:
            return
        declared = order.get("filled_base") or 0.0
        fill = self._fills.get(oid)
        if fill is None and declared > 1e-12:
            return
        # An aggregate without stable trade IDs, or one that disagrees with
        # the order's cumulative quantity, cannot prove a WS terminal fill.
        if (fill is not None and (fill.get('missing_identity')
                or abs(float(fill['filled_base']) - declared) > 1e-9)):
            return
        filled = float(fill["filled_base"] if fill is not None else declared)
        average = (float(fill["filled_quote"]) / filled
                   if fill is not None and filled > 0 else None)
        self._resolve(order["cloid"], {
            "status": order["status"], "filled_base": filled,
            "avg_px": average, "oid": oid,
            "exchange_fill_ts": (fill.get("exchange_fill_ts")
                                 if fill is not None else None),
            "exchange_last_fill_ts": (fill.get("exchange_last_fill_ts")
                                      if fill is not None else None),
        })

    def _handle_orders(self, data, *, received=None, parsed=None, boundary='handler_call') -> None:
        received = stamp() if received is None else received
        parsed = received if parsed is None else parsed
        for item in data if isinstance(data, list) else []:
            order = item.get("order") if isinstance(item, dict) else None
            if not isinstance(order, dict) or order.get("coin") != self.coin:
                continue
            # Even a rejected replay invalidates a position handoff lease.
            self.evidence_revision += 1
            cloid = self._key(order.get("cloid"))
            if not cloid:
                continue
            try:
                oid = int(order["oid"])
                filled = _order_filled_size(order)
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            if oid in self._retired_oids:
                continue
            previous = self._orders.get(oid)
            if previous and (previous["cloid"] != cloid
                             or previous["filled_base"] > filled
                             or previous["status"] in TERMINAL_ORDER_STATUSES):
                continue
            fill = self._fills.get(oid)
            if fill is not None and '_timing_received' in fill:
                self._observe_timing(cloid, received=fill['_timing_received'],
                    parsed=fill['_timing_parsed'], boundary=fill['_timing_boundary'])
            self._observe_timing(cloid, received=received, parsed=parsed, boundary=boundary)
            self._orders[oid] = {
                "cloid": cloid,
                "status": str(item.get("status") or "").strip().lower(),
                "filled_base": filled,
            }
            self._maybe_resolve(oid)

    def _handle_fills(self, data, *, received=None, parsed=None, boundary='handler_call') -> None:
        received = stamp() if received is None else received
        parsed = received if parsed is None else parsed
        if not isinstance(data, dict):
            return
        for fill in data.get("fills") or []:
            if not isinstance(fill, dict) or fill.get("coin") != self.coin:
                continue
            self.evidence_revision += 1
            try:
                oid = int(fill["oid"])
                qty = float(fill["sz"])
                price = float(fill["px"])
                exchange_fill_ts = (float(fill["time"]) / 1000
                                    if fill.get("time") is not None else None)
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            if (oid in self._retired_oids or qty <= 0 or price <= 0
                    or not math.isfinite(qty) or not math.isfinite(price)):
                continue
            tid = fill.get('tid')
            if tid not in (None, ''):
                identity = str(tid)
                seen = self._fill_ids.setdefault(oid, set())
                if identity in seen:
                    continue
                seen.add(identity)
            aggregate = self._fills.setdefault(
                oid, {"filled_base": 0.0, "filled_quote": 0.0})
            if '_timing_received' not in aggregate:
                aggregate.update(_timing_received=received, _timing_parsed=parsed, _timing_boundary=boundary)
            order = self._orders.get(oid)
            if order is not None:
                self._observe_timing(order['cloid'], received=received, parsed=parsed, boundary=boundary)
            if tid in (None, ''):
                aggregate['missing_identity'] = True
                continue
            aggregate["filled_base"] += qty
            aggregate["filled_quote"] += qty * price
            if exchange_fill_ts is not None:
                previous = aggregate.get("exchange_fill_ts")
                aggregate["exchange_fill_ts"] = min(
                    previous, exchange_fill_ts) if previous is not None else exchange_fill_ts
                previous_last = aggregate.get("exchange_last_fill_ts")
                aggregate["exchange_last_fill_ts"] = max(
                    previous_last, exchange_fill_ts) if previous_last is not None else exchange_fill_ts
            self._fills.move_to_end(oid)
            self._maybe_resolve(oid)

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        subscriptions = (
            {"type": "orderUpdates", "user": self.user},
            {"type": "userFills", "user": self.user,
             "aggregateByTime": True},
        )
        while not stop.is_set():
            pinger = None
            acknowledged: set[str] = set()
            try:
                async with ws_connect(
                        self.ws_url, max_size=2**23, open_timeout=10,
                        ping_interval=15, ping_timeout=15,
                        family=socket.AF_INET) as ws:
                    self.connection_generation += 1
                    for subscription in subscriptions:
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": subscription,
                        }))

                    async def ping() -> None:
                        while True:
                            await asyncio.sleep(5)
                            await ws.send(json.dumps({"method": "ping"}))

                    pinger = asyncio.create_task(ping())
                    async for raw in ws:
                        received = stamp()
                        backoff = 1.0
                        message = json.loads(raw)
                        parsed = stamp()
                        channel = message.get("channel")
                        if channel == "subscriptionResponse":
                            subscription = ((message.get("data") or {}).get(
                                "subscription") or {})
                            kind = subscription.get("type")
                            if kind in {"orderUpdates", "userFills"}:
                                acknowledged.add(kind)
                            if acknowledged == {"orderUpdates", "userFills"}:
                                if not self.ready.is_set():
                                    log.info("[%s] order/fill stream ready",
                                             self.name)
                                self.ready.set()
                        elif channel == "orderUpdates":
                            self._handle_orders(message.get("data"), received=received, parsed=parsed, boundary='ws_recv')
                        elif channel == "userFills":
                            self._handle_fills(message.get("data"), received=received, parsed=parsed, boundary='ws_recv')
                        if stop.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("[%s] order/fill ws error: %s — retry in %.0fs",
                            self.name, exc, backoff)
            finally:
                self.ready.clear()
                if pinger is not None:
                    pinger.cancel()
            if stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


class NonceAllocator:
    def __init__(self) -> None:
        self._last = 0

    def next(self) -> int:
        self._last = max(self._last + 1, int(time.time() * 1000))
        return self._last


class HLAccount:
    def __init__(self, private_key: str, account_address: Optional[str],
                 api_url: str) -> None:
        from eth_account import Account
        self.wallet = Account.from_key(private_key)
        self.query_address = (account_address or self.wallet.address).lower()
        self.is_mainnet = api_url == "https://api.hyperliquid.xyz"
        self.nonces = NonceAllocator()

    def describe(self) -> str:
        s = f"signer={self.wallet.address} account={self.query_address}"
        if self.wallet.address.lower() != self.query_address:
            s += " (agent mode)"
        return s


class HLVenue:
    supports_open_first_fast_resolve = True
    supports_submission_guard = True
    kind = "hl"

    def __init__(self, conf: VenueConf, api_url: str, ws_url: str,
                 session: aiohttp.ClientSession, settle_timeout_sec: float) -> None:
        self.conf = conf
        self.key = conf.key
        self.name = conf.label
        self.api_url = api_url
        self.ws_url = ws_url
        self.session = session
        attach_http_timing(session)
        self.settle_timeout = settle_timeout_sec
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
        self.include_core_equity = True  # cleared when two venues share one account
        self.fee_bps = conf.fee_bps
        self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.account: Optional[HLAccount] = None
        self.coin = ""
        self.asset_id = -1
        self.max_leverage = 0
        self.only_isolated = False
        self.size_decimals = 0
        self.min_base = 0.0
        self.min_quote = 10.0
        self._cloid = int(time.time() * 1000)
        self._signing = None      # lazy hyperliquid-sdk signing module
        self.funding: Optional[FundingSnapshot] = None
        self._account_abstraction = None
        self.orders_feed: Optional[HLOrdersFeed] = None
        self._submit_tasks: set[asyncio.Task] = set()
        self._exchange_timings: dict[int, dict[str, float]] = {}

    def configure_order_timing(self, sink, clock_session_id):
        try:
            self._order_timing_collector = OrderTimingCollector(sink, clock_session_id, venue='entropy')
            attach_http_timing(self.session)
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

    async def _info(self, payload: dict):
        async with self.session.post(
                self.api_url + "/info", json=payload,
                timeout=aiohttp.ClientTimeout(total=INFO_TIMEOUT)) as r:
            r.raise_for_status()
            return await r.json()

    async def load_market(self) -> None:
        dexs = await self._info({"type": "perpDexs"})
        names = [(d or {}).get("name", "") for d in dexs]
        if self.conf.hl_dex not in names:
            raise RuntimeError(f"[{self.name}] dex '{self.conf.hl_dex}' not "
                               f"found on Hyperliquid (available: "
                               f"{[n for n in names if n][:20]}...)")
        dex_index = names.index(self.conf.hl_dex)
        meta = await self._info({"type": "meta", "dex": self.conf.hl_dex})
        want = f"{self.conf.hl_dex}:{self.conf.symbol}"
        for idx, a in enumerate(meta["universe"]):
            if a["name"] not in (want, self.conf.symbol):
                continue
            if a.get("isDelisted"):
                raise RuntimeError(f"[{self.name}] {a['name']} is delisted")
            self.coin = a["name"]
            self.asset_id = 110000 + (dex_index - 1) * 10000 + idx
            self.size_decimals = int(a["szDecimals"])
            self.max_leverage = int(a.get("maxLeverage") or 1)
            self.only_isolated = bool(a.get("onlyIsolated"))
            self.min_base = 10 ** -self.size_decimals
            log.info("[%s] %s asset_id=%d szDecimals=%d maxLev=%sx %s",
                     self.name, self.coin, self.asset_id, self.size_decimals,
                     self.max_leverage,
                     "isolated-only" if self.only_isolated else "")
            return
        raise RuntimeError(f"[{self.name}] {want} not found")

    def init_signer(self) -> None:
        c = self.conf.hl_creds
        assert c is not None and c.complete, f"[{self.name}] missing credentials"
        try:
            from hyperliquid.utils import signing as hl_signing
        except ImportError as e:
            raise RuntimeError(
                "live trading on Hyperliquid needs the official SDK — "
                "pip install -r requirements-live.txt "
                "(hyperliquid-python-sdk)") from e
        self._signing = hl_signing
        self.account = HLAccount(c.private_key, c.account_address, self.api_url)
        log.info("[%s] %s", self.name, self.account.describe())

    def share_nonces_with(self, other: "HLVenue") -> None:
        """One signer address must use one nonce sequence."""
        if (self.account and other.account and
                self.account.wallet.address == other.account.wallet.address):
            other.account.nonces = self.account.nonces
            log.info("[%s]/[%s] same signer — shared nonce allocator",
                     self.name, other.name)

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        tasks = [asyncio.create_task(
            HLBookFeed(self.name, self.ws_url, self.coin, self.book,
                       notify).run(stop),
            name=f"book-{self.key}")]
        if live:
            assert self.account is not None
            self.orders_feed = HLOrdersFeed(
                self.name, self.ws_url, self.account.query_address, self.coin)
            self.orders_feed.timing_collector = getattr(self, '_order_timing_collector', None)
            tasks.append(asyncio.create_task(
                self.orders_feed.run(stop), name=f"acct-{self.key}"))
        return tasks

    def ready_to_trade(self) -> bool:
        feed = getattr(self, "orders_feed", None)
        return (self.account is not None and feed is not None
                and feed.ready.is_set()
                and not getattr(self, "_submit_tasks", None))

    async def set_leverage(self, leverage: int) -> None:
        """Set leverage explicitly; live startup fails closed on uncertainty."""
        assert self.account is not None and self._signing is not None
        if not 1 <= leverage <= self.max_leverage:
            raise RuntimeError(
                f"[{self.name}] leverage {leverage}x outside market range "
                f"1..{self.max_leverage}x")
        action = {"type": "updateLeverage", "asset": self.asset_id,
                  "isCross": not self.only_isolated, "leverage": leverage}
        nonce = self.account.nonces.next()
        sig = self._signing.sign_l1_action(
            self.account.wallet, action, None, nonce, None,
            self.account.is_mainnet)
        payload = {"action": action, "nonce": nonce, "signature": sig,
                   "vaultAddress": None, "expiresAfter": None}
        body, err, unresolved = await self._post_exchange(payload)
        self._exchange_timings.pop(nonce, None)
        if err is not None:
            raise RuntimeError(f"[{self.name}] leverage update failed: {err}")
        if unresolved:
            raise RuntimeError(
                f"[{self.name}] leverage update outcome unknown; startup blocked")
        if not isinstance(body, dict) or body.get("status") != "ok":
            raise RuntimeError(
                f"[{self.name}] leverage update rejected: {str(body)[:200]}")
        mode = "isolated" if self.only_isolated else "cross"
        for _ in range(20):
            data = await self._info({"type": "activeAssetData",
                                     "user": self.account.query_address,
                                     "coin": self.coin})
            actual = data.get("leverage") if isinstance(data, dict) else None
            if isinstance(actual, dict) and actual.get("value") == leverage \
                    and actual.get("type") == mode:
                log.info("[%s] leverage set to %dx (%s)", self.name, leverage, mode)
                return
            await asyncio.sleep(0.25)
        raise RuntimeError(
            f"[{self.name}] leverage update could not be verified; startup blocked")

    async def assert_exchange_tradeable(self) -> None:
        status = await self._info({"type": "exchangeStatus"})
        special = status.get("specialStatuses") if isinstance(status, dict) else None
        if special:
            raise RuntimeError(
                f"[{self.name}] exchange special status blocks live startup: "
                f"{str(special)[:200]}")

    async def warm_http(self) -> None:
        """Order-path keepalive ping (driven by the engine's keepalive loop)."""
        try:
            await self._info({"type": "exchangeStatus"})
        except Exception as e:
            log.debug("[%s] keepalive ping failed: %r", self.name, e)

    async def fetch_funding(self, *, include_price: bool = False) -> FundingSnapshot:
        """Fetch current HIP-3 funding and normalize it to bps/hour."""
        started = time.time()
        data = await self._info({"type": "metaAndAssetCtxs",
                                 "dex": self.conf.hl_dex})
        if not isinstance(data, list) or len(data) < 2:
            raise RuntimeError(f"[{self.name}] malformed funding response")
        meta, contexts = data[0], data[1]
        for asset, ctx in zip(meta.get("universe") or [], contexts or []):
            if asset.get("name") not in (self.coin, self.conf.symbol,
                                           f"{self.conf.hl_dex}:{self.conf.symbol}"):
                continue
            rate = float(ctx["funding"]) * 1e4
            if not math.isfinite(rate):
                raise RuntimeError(f"[{self.name}] non-finite funding rate")
            index = funding_decimal(ctx.get('oraclePx'), 'oraclePx') if include_price else None
            snap = FundingSnapshot(rate, started if include_price else time.time(),
                                   f"hyperliquid:{self.conf.hl_dex}", index)
            self.funding = snap
            return snap
        raise RuntimeError(f"[{self.name}] funding asset {self.coin} not found")

    async def fetch_fee_fills(self, since_ts: float,
                              until_ts: Optional[float] = None) -> list[dict]:
        """Fetch the account's authoritative Hyperliquid fill fees.

        ``userFillsByTime`` is intentionally used instead of inferring fees
        from order responses: the fill record carries the actual fee and fee
        token charged by the matching engine.  The returned shape is shared
        with the Lighter adapter and consumed by ``TradeLedger``.
        """
        address = self._query_address()
        if not address:
            return []
        until = time.time() if until_ts is None else float(until_ts)
        payload = {
            "type": "userFillsByTime",
            "user": address,
            "startTime": int(max(0.0, float(since_ts)) * 1000),
            "endTime": int(max(0.0, until) * 1000),
        }
        if self.conf.hl_dex:
            payload["dex"] = self.conf.hl_dex
        data = await self._info(payload)
        received_ts = time.time()
        if not isinstance(data, list):
            data = data.get("fills") if isinstance(data, dict) else []
        result = []
        for fill in data or []:
            if not isinstance(fill, dict):
                continue
            coin = str(fill.get("coin") or "")
            if coin and coin != self.coin:
                continue
            try:
                ts = float(fill.get("time")) / 1000.0
                quantity_exact = funding_decimal(fill.get("sz"), "fill size").copy_abs()
                price_exact = funding_decimal(fill.get("px"), "fill price")
                qty, price = float(quantity_exact), float(price_exact)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(ts) or not math.isfinite(qty) \
                    or not math.isfinite(price) or qty <= 0 or price <= 0:
                continue
            if ts < float(since_ts) - 1.0 or ts > until + 1.0:
                continue
            side_value = str(fill.get("side") or "").strip().lower()
            if side_value in {"b", "buy", "bid"}:
                side = "buy"
            elif side_value in {"a", "ask", "sell"}:
                side = "sell"
            else:
                continue
            fee = None
            try:
                fee = float(fill.get("fee")) if fill.get("fee") is not None \
                    else None
            except (TypeError, ValueError):
                fee = None
            if fee is not None and not math.isfinite(fee):
                fee = None
            fee_token = str(fill.get("feeToken") or "").strip() or None
            fee_usd = (fee if fee is not None and
                       (not fee_token or fee_token.upper() in STABLE_FEE_TOKENS)
                       else None)
            trade_id = fill.get("tid", fill.get("tradeId", fill.get("id")))
            order_ref = fill.get("cloid") or fill.get("clientOrderId")
            key = (f"hyperliquid:{self.conf.hl_dex}:{trade_id}"
                   if coin and trade_id not in (None, "") else None)
            amounts = {}
            for name, value in (("start_position", fill.get("startPosition")),
                                ("closed_pnl", fill.get("closedPnl"))):
                try:
                    amounts[name] = (str(funding_decimal(value, name))
                                     if value not in (None, "") else None)
                except ValueError:
                    amounts[name] = None
            direction = str(fill.get("dir") or "").strip() or None
            reduction = {"Close Long": True, "Close Short": True,
                         "Open Long": False, "Open Short": False}.get(direction)
            exchange_order_id = fill.get("oid")
            result.append({
                "exchange_fill_key": key,
                "exchange_trade_id": trade_id,
                "exchange_order_id": (str(exchange_order_id)
                                      if exchange_order_id not in (None, "") else None),
                "order_ref": order_ref,
                "source_account": str(address).lower(),
                "source_market": coin or None,
                "source_profile": None,
                "source_dex": self.conf.hl_dex,
                "source": "hyperliquid:userFillsByTime",
                "evidence_received_ts": received_ts,
                "ts": ts,
                "side": side,
                "qty": qty,
                "price": price,
                "quantity_exact": str(quantity_exact),
                "price_exact": str(price_exact),
                **amounts,
                "direction": direction,
                "is_reduction": reduction,
                "liquidation": fill.get("liquidation"),
                "fee_amount": fee,
                "fee_usd": fee_usd,
                "fee_token": fee_token,
                "raw": fill,
            })
        return result

    async def fetch_funding_settlements(
            self, since_ts: float,
            until_ts: Optional[float] = None) -> list[FundingSettlement]:
        """Read userFunding (ms times, decimal USDC), including explicit zeros.

        Time pagination is inclusive, so the boundary is replayed and deduped.
        A saturated single timestamp or exhausted budget cannot prove complete
        history and raises instead of returning a misleading partial result.
        The official API does not guarantee ledger rows for zero-rate rounds.
        """
        address = self._query_address()
        if not address or not self.coin:
            raise RuntimeError(f"[{self.name}] account or funding market unavailable")
        since, until = funding_window(
            since_ts, time.time() if until_ts is None else until_ts)
        payload = {"type": "userFunding", "user": address,
                   "startTime": math.floor(since * 1000),
                   "endTime": math.ceil(until * 1000)}
        # userFunding is account-wide; unlike clearinghouseState, its official
        # request schema has no dex parameter. Filter the canonical coin below.
        events: dict[str, FundingSettlement] = {}
        for _ in range(FUNDING_HISTORY_MAX_PAGES):
            rows = await self._info(dict(payload))
            if not isinstance(rows, list):
                raise RuntimeError(f"[{self.name}] malformed funding history")
            latest_ms = payload["startTime"]
            for row in rows:
                if not isinstance(row, dict):
                    raise RuntimeError(f"[{self.name}] malformed funding history row")
                raw_ts = funding_decimal(row["time"], "time")
                if raw_ts != raw_ts.to_integral_value():
                    raise ValueError("funding history time must be integer milliseconds")
                ts = funding_timestamp(raw_ts, milliseconds=True)
                if raw_ts <= payload["endTime"]:
                    latest_ms = max(latest_ms, int(raw_ts))
                delta = row.get("delta")
                if not isinstance(delta, dict):
                    raise RuntimeError(f"[{self.name}] funding delta missing")
                if delta.get("coin") != self.coin or not since <= ts <= until:
                    continue
                if delta.get("type") != "funding":
                    raise RuntimeError(f"[{self.name}] invalid funding delta type")
                identity = row.get("hash")
                if not isinstance(identity, str) or not identity.strip():
                    raise RuntimeError(f"[{self.name}] funding hash missing")
                event = FundingSettlement(
                    event_id=f"hyperliquid:{address.lower()}:{self.coin}:{int(raw_ts)}:{identity}",
                    ts=ts, amount_usd=funding_decimal(delta["usdc"], "usdc"),
                    position_qty=(funding_decimal(delta["szi"], "szi")
                                  if delta.get("szi") is not None else None),
                    source=f"hyperliquid:{self.conf.hl_dex}:userFunding")
                previous = events.get(event.event_id)
                if previous is not None and previous != event:
                    raise RuntimeError(f"[{self.name}] conflicting funding history")
                events[event.event_id] = event
            if len(rows) < FUNDING_HISTORY_PAGE_SIZE:
                return sorted(events.values(), key=lambda event: (event.ts, event.event_id))
            if latest_ms <= payload["startTime"]:
                raise RuntimeError(f"[{self.name}] funding history pagination stalled")
            payload["startTime"] = latest_ms
        raise RuntimeError(f"[{self.name}] funding history pagination limit reached")

    async def fetch_funding_payments(self, since_ts: float,
                                     until_ts: Optional[float] = None) -> float:
        """Return the same signed cash-flow total, across all history pages."""
        events = await self.fetch_funding_settlements(since_ts, until_ts)
        return float(sum((event.amount_usd for event in events), Decimal(0)))

    async def fetch_funding_risk(self) -> FundingRiskSnapshot:
        address = self._query_address()
        if not address or not self.coin:
            raise RuntimeError(f"[{self.name}] account or funding market unavailable")
        started = time.time()
        state, data = await asyncio.gather(
            self._info({"type": "clearinghouseState", "user": address,
                        "dex": self.conf.hl_dex}),
            self._info({"type": "metaAndAssetCtxs", "dex": self.conf.hl_dex}))
        if not isinstance(state, dict) or not isinstance(state.get("assetPositions"), list):
            raise RuntimeError(f"[{self.name}] funding risk positions missing")
        state_ts = funding_timestamp(state["time"], milliseconds=True)
        ts = min(state_ts, started)  # asset contexts have no exchange timestamp
        now = time.time()
        if state_ts > now or not 0 <= now - ts <= FUNDING_RISK_MAX_AGE_SEC:
            raise RuntimeError(f"[{self.name}] funding risk snapshot stale or future")
        positions = []
        for row in state["assetPositions"]:
            if not isinstance(row, dict) or not isinstance(row.get("position"), dict):
                raise RuntimeError(f"[{self.name}] malformed funding risk position")
            coin = row["position"].get("coin")
            if not isinstance(coin, str) or not coin:
                raise RuntimeError(f"[{self.name}] funding risk position market missing")
            if coin == self.coin:
                positions.append(row["position"])
        if len(positions) > 1:
            raise RuntimeError(f"[{self.name}] duplicate funding risk position")
        qty = funding_decimal(positions[0]["szi"], "szi") if positions else Decimal(0)
        liquidation = None
        if qty != 0 and positions[0].get("liquidationPx") is not None:
            liquidation = funding_decimal(positions[0]["liquidationPx"], "liquidationPx")
        if (not isinstance(data, list) or len(data) != 2
                or not isinstance(data[0], dict) or not isinstance(data[1], list)):
            raise RuntimeError(f"[{self.name}] malformed funding risk asset contexts")
        universe = data[0].get("universe")
        if not isinstance(universe, list) or len(universe) != len(data[1]):
            raise RuntimeError(f"[{self.name}] incomplete funding risk asset contexts")
        marks = [ctx for asset, ctx in zip(universe, data[1])
                 if asset.get("name") == self.coin]
        if len(marks) != 1:
            raise RuntimeError(f"[{self.name}] funding risk mark unavailable")
        return FundingRiskSnapshot(
            qty, funding_decimal(marks[0]["markPx"], "markPx"), liquidation, ts,
            f"hyperliquid:{self.conf.hl_dex}:clearinghouseState+metaAndAssetCtxs")

    # ------------------------------------------------------------ price grid

    def px_round(self, px: float, round_up: bool) -> float:
        if px <= 0:
            return px
        max_dec = max(0, 6 - self.size_decimals)
        sig_dec = 4 - math.floor(math.log10(px))
        dec = max(0, min(max_dec, sig_dec))
        f = 10.0 ** dec
        v = math.ceil(px * f - 1e-9) / f if round_up else math.floor(px * f + 1e-9) / f
        return round(v, 8)

    # ------------------------------------------------------------- execution

    def _next_cloid(self, order_ref: Optional[str] = None):
        from hyperliquid.utils.types import Cloid
        if order_ref not in (None, ""):
            try:
                value = int(order_ref)
                if value > 0:
                    self._cloid = max(self._cloid, value)
                    return Cloid.from_int(value)
            except (TypeError, ValueError):
                return Cloid.from_str(str(order_ref))
        self._cloid += 1
        return Cloid.from_int(self._cloid)

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False,
                         order_ref: Optional[str] = None,
                         resolve_timeout: Optional[float] = None,
                         timing: Optional[dict[str, object]] = None) -> dict:
        assert self.account is not None and self.asset_id >= 0
        s = self._signing
        cloid = self._next_cloid(order_ref)
        raw_cloid = cloid.to_raw()
        trace = self.order_timing(raw_cloid, create=True, side='buy' if is_buy else 'sell',
            requested_base=qty, limit_px=limit_px, reduce_only=reduce_only)
        if trace is not None:
            trace.mark('prepare_start', first=True)
            trace.mark('submit_wrapper_start', first=True)
        order_req = {"coin": self.coin, "is_buy": is_buy, "sz": round(qty, 8),
                     "limit_px": limit_px,
                     "order_type": {"limit": {"tif": "Ioc"}},
                     "reduce_only": reduce_only, "cloid": cloid}
        try:
            wire = s.order_request_to_order_wire(order_req, self.asset_id)
            action = s.order_wires_to_order_action([wire])
            if trace is not None:
                trace.mark('sdk_nonce_select_start', first=True)
            try:
                nonce = self.account.nonces.next()
            finally:
                if trace is not None:
                    trace.mark('sdk_nonce_select_end', first=True)
            if trace is not None:
                trace.mark('sdk_sign_start', first=True)
            try:
                sig = s.sign_l1_action(self.account.wallet, action, None, nonce,
                                       None, self.account.is_mainnet)
            finally:
                if trace is not None:
                    trace.mark('sdk_sign_end', first=True)
            payload = {"action": action, "nonce": nonce, "signature": sig,
                       "vaultAddress": None, "expiresAfter": None}
        except Exception as e:
            if trace is not None:
                trace.mark('submit_wrapper_done')
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": f"signing failed: {e!r}", "unresolved": False,
                    "order_ref": cloid.to_raw()}

        if trace is not None:
            trace.mark('prepare_end', first=True)
        future = (self.orders_feed.watch(raw_cloid)
                  if self.orders_feed is not None else None)
        started = time.perf_counter()

        def confirmed(info: dict) -> dict:
            if trace is not None:
                trace.mark('send_taker_return')
                trace.result(status=info['status'], filled_base=info.get('filled_base', 0),
                    average=info.get('avg_px'), source='order_fills_ws')
            result = {"status": info["status"],
                    "filled_base": float(info.get("filled_base") or 0.0),
                    "avg_px": info.get("avg_px"), "err": None,
                    "unresolved": False, "order_ref": raw_cloid,
                    "confirmation_source": "order_fills_ws",
                    "confirmation_ms": round(
                        (time.perf_counter() - started) * 1000, 3)}
            if info.get('exchange_fill_ts') is not None:
                result['exchange_fill_ts'] = info['exchange_fill_ts']
            if info.get('exchange_last_fill_ts') is not None:
                result['exchange_last_fill_ts'] = info['exchange_last_fill_ts']
            return result

        deferred_reply = None

        async def submit():
            nonlocal deferred_reply
            guard = getattr(self, 'submission_guard', None)
            reason = guard(raw_cloid) if guard is not None and not reduce_only else None
            if reason:
                deferred_reply = {
                    "status": "deferred", "filled_base": 0.0, "avg_px": None,
                    "err": f"submission_guard:{reason}", "unresolved": False,
                    "order_ref": raw_cloid, "deferred": True, "retry_after": 0.0,
                    "post_started": False, "proved_local_unsent": True}
                return deferred_reply
            row = (trace.request('http_requests', method='POST', endpoint='/exchange',
                request_timeout=INFO_TIMEOUT) if trace is not None else None)
            token = HTTP_TIMING_CONTEXT.set((trace, row) if row is not None else None)
            try:
                return await self._post_exchange(payload)
            finally:
                if row is not None:
                    trace.request_mark(row, 'end')
                HTTP_TIMING_CONTEXT.reset(token)

        submission = asyncio.create_task(submit(), name=f"hl-submit-{raw_cloid}")
        self._submit_tasks.add(submission)

        def submission_done(task: asyncio.Task) -> None:
            self._submit_tasks.discard(task)
            http_timing = getattr(self, '_exchange_timings', {}).pop(nonce, {})
            if timing is not None:
                timing.update(http_timing)
            if trace is not None:
                trace.mark('submit_wrapper_done')
                if 'runtime_consumed' in trace.data['stages']:
                    trace.emit()
            if not task.cancelled():
                try:
                    task.exception()
                except Exception:
                    pass

        submission.add_done_callback(submission_done)
        try:
            if future is not None:
                await asyncio.wait({submission, future},
                                   return_when=asyncio.FIRST_COMPLETED)
                if future.done() and not future.cancelled():
                    if deferred_reply is not None:
                        return deferred_reply
                    return confirmed(future.result())
            reply = await asyncio.shield(submission)
            if isinstance(reply, dict):
                return reply
            body, err, unresolved = reply
            observed = self.orders_feed.terminal(raw_cloid) if self.orders_feed is not None else None
            if observed is not None:
                return confirmed(observed)
            http_timing = getattr(self, '_exchange_timings', {}).pop(nonce, {})
            if timing is not None:
                timing.update(http_timing)
        finally:
            if future is not None:
                self.orders_feed.unwatch(raw_cloid, future)
        if err is not None:
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": err, "unresolved": unresolved,
                    "order_ref": raw_cloid}
        if not unresolved:
            res = self._parse(body)
            if not res.get("unresolved"):
                if trace is not None:
                    trace.mark('terminal_published_first', first=True)
                    trace.mark('terminal_published')
                    trace.mark('send_taker_return')
                    trace.result(status=res['status'], filled_base=res['filled_base'],
                        average=res.get('avg_px'), source='exchange_http')
                res["order_ref"] = raw_cloid
                res["confirmation_source"] = "exchange_http"
                res["confirmation_ms"] = round(
                    (time.perf_counter() - started) * 1000, 3)
                return res
        # A transport ACK is not a fill. Resolve the same client id only.
        if timing is not None:
            timing['resolve_start_ts'] = time.time()
        if trace is not None:
            trace.mark('settlement_wait_start', first=True)
        try:
            resolved = await self.resolve_order(raw_cloid, timeout=resolve_timeout)
        finally:
            if trace is not None:
                trace.mark('settlement_wait_end')
                trace.mark('send_taker_return')
        if timing is not None:
            timing['resolve_end_ts'] = time.time()
        return resolved

    async def resolve_order(self, order_ref, timeout: Optional[float] = None) -> dict:
        """Resolve a previously unknown IOC by its client order id."""
        assert self.account is not None
        feed = getattr(self, "orders_feed", None)
        if feed is not None:
            cached = feed.terminal(order_ref)
            if cached is not None:
                return {**cached, "err": None, "unresolved": False,
                        "order_ref": order_ref,
                        "confirmation_source": "order_fills_ws"}
        deadline = time.monotonic() + (
            self.settle_timeout if timeout is None else max(float(timeout), 0.0))
        delay = 0.1
        future = feed.watch(order_ref) if feed is not None else None
        try:
            while True:
                if future is not None and future.done() and not future.cancelled():
                    return {**future.result(), "err": None, "unresolved": False,
                            "order_ref": order_ref,
                            "confirmation_source": "order_fills_ws"}
                poll = asyncio.create_task(self._info({
                    "type": "orderStatus", "user": self.account.query_address,
                    "oid": order_ref}))
                try:
                    waiting = {poll, future} if future is not None else {poll}
                    done, _ = await asyncio.wait(
                        waiting, timeout=max(0, deadline - time.monotonic()),
                        return_when=asyncio.FIRST_COMPLETED)
                    if future is not None and future in done and not future.cancelled():
                        return {**future.result(), "err": None, "unresolved": False,
                                "order_ref": order_ref,
                                "confirmation_source": "order_fills_ws"}
                    try:
                        st = poll.result() if poll in done else None
                    except Exception:
                        st = None
                        delay = 0.5
                finally:
                    if not poll.done():
                        poll.cancel()
                        await asyncio.gather(poll, return_exceptions=True)
                if isinstance(st, dict) and st.get("status") == "order":
                    o = st.get("order") or {}
                    if not isinstance(o, dict):
                        o = {}
                    status = str(o.get("status", "")).strip().lower()
                    inner = o.get("order")
                    try:
                        filled = _order_filled_size(inner)
                        average = (float(inner["avgPx"])
                                   if inner.get("avgPx") is not None else None)
                        if average is not None and (not math.isfinite(average)
                                                    or average <= 0):
                            raise ValueError("invalid average price")
                        if (inner.get("cloid") is not None
                                and HLOrdersFeed._key(inner["cloid"])
                                != HLOrdersFeed._key(order_ref)):
                            raise ValueError("order identity mismatch")
                    except (KeyError, TypeError, ValueError, OverflowError):
                        filled = None
                    if status in TERMINAL_ORDER_STATUSES and filled is not None:
                        return {"status": status, "filled_base": filled,
                                "avg_px": average,
                                "err": None, "unresolved": False,
                                "order_ref": order_ref,
                                "confirmation_source": "order_status_poll"}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                if future is not None:
                    await asyncio.wait({future}, timeout=min(delay, remaining))
                else:
                    await asyncio.sleep(min(delay, remaining))
                delay = min(delay * 2, 0.5)
        finally:
            if future is not None:
                feed.unwatch(order_ref, future)
        return {"status": "timeout", "filled_base": 0.0, "avg_px": None,
                "err": None, "unresolved": True, "order_ref": order_ref}

    async def _post_exchange(self, payload: dict):
        timing = {'http_post_start': time.time(),
                  'http_post_start_mono_ns': time.monotonic_ns()}
        self._exchange_timings[payload['nonce']] = timing
        try:
            async with self.session.post(
                    self.api_url + "/exchange", json=payload,
                    timeout=aiohttp.ClientTimeout(total=INFO_TIMEOUT)) as r:
                timing['http_headers_received'] = time.time()
                timing['http_headers_received_mono_ns'] = time.monotonic_ns()
                text = await r.text()
                timing['http_body_received'] = time.time()
                timing['http_body_received_mono_ns'] = time.monotonic_ns()
                timing['http_result_ts'] = timing['http_body_received']
                if r.status == 429:
                    return None, f"RATE_LIMITED: HTTP 429 {text[:150]}", True
                if 400 <= r.status < 500:
                    # A proxy/client HTTP error (e.g. 408) does not prove the
                    # signed action was rejected. Only an exchange rejection
                    # body can establish terminal zero after submission.
                    body = json.loads(text)
                    parsed = self._parse(body)
                    if parsed["status"] == "send-failed" and not parsed["unresolved"]:
                        return body, None, False
                    return None, None, True
                if r.status >= 500:
                    return None, None, True
                return json.loads(text), None, False
        except (asyncio.TimeoutError, aiohttp.ClientError, json.JSONDecodeError):
            timing['http_result_ts'] = time.time()
            return None, None, True

    @staticmethod
    def _parse(body: dict) -> dict:
        def unknown(msg: str) -> dict:
            # Zero is the known cumulative amount, never a terminal assertion.
            return {"status": "unknown", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": True}

        def fail(msg: str) -> dict:
            low = msg.lower()
            if "rate limit" in low or "too many" in low:
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": False}
        if not isinstance(body, dict):
            return unknown("malformed exchange response")
        if body.get("status") == "err":
            msg = body.get("response")
            return (fail(msg) if isinstance(msg, str) and msg.strip()
                    else unknown("malformed exchange rejection"))
        if body.get("status") != "ok":
            return unknown("unexpected exchange response")
        try:
            statuses = body["response"]["data"]["statuses"]
            if not isinstance(statuses, list) or len(statuses) != 1:
                return unknown("malformed exchange statuses")
            st = statuses[0]
        except (KeyError, IndexError, TypeError):
            return unknown("malformed exchange response")
        if not isinstance(st, dict) or len(st) != 1:
            return unknown("malformed exchange status")
        if "filled" in st:
            f = st["filled"]
            try:
                filled, average = float(f["totalSz"]), float(f["avgPx"])
                oid = int(f["oid"])
                if (not math.isfinite(filled) or filled <= 0
                        or not math.isfinite(average) or average <= 0 or oid < 0):
                    raise ValueError("invalid filled response")
            except (KeyError, TypeError, ValueError, OverflowError):
                return unknown("malformed exchange fill")
            return {"status": "filled",
                    "filled_base": filled, "avg_px": average, "oid": oid,
                    "err": None, "unresolved": False}
        if "error" in st:
            msg = st["error"]
            if not isinstance(msg, str) or not msg.strip():
                return unknown("malformed order rejection")
            if "could not immediately match" in msg.lower():
                return {"status": "canceled", "filled_base": 0.0, "avg_px": None,
                        "err": None, "unresolved": False,
                        "exchange_reason": msg}
            return fail(msg)
        if "resting" in st:
            return {"status": "resting?", "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True}
        return unknown("unknown exchange order status")

    # -------------------------------------------------------------- accounts

    def _query_address(self):
        if self.account is not None:
            return self.account.query_address
        c = self.conf.hl_creds
        return c.account_address.lower() if c and c.account_address else None

    async def _unified_available_margin(self, addr: str) -> Optional[float]:
        """Return unified-account free collateral after maintenance margin."""
        mode = getattr(self, "_account_abstraction", None)
        if mode is None:
            mode = await self._info({"type": "userAbstraction", "user": addr})
            mode = mode if isinstance(mode, str) else "unknown"
            self._account_abstraction = mode
        if mode not in {"unifiedAccount", "portfolioMargin"}:
            return None

        state = await self._info({"type": "spotClearinghouseState",
                                  "user": addr})
        # Token index 0 is USDC, the collateral token for the configured IO
        # market.  This field already reflects holds and maintenance margin.
        for row in state.get("tokenToAvailableAfterMaintenance") or []:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            try:
                if int(row[0]) == 0:
                    return float(row[1])
            except (TypeError, ValueError):
                continue
        return None

    async def fetch_equity(self):
        """Unified account equity via the portfolio endpoint — the same
        Portfolio Value the HL UI shows. Falls back to summing clearinghouse
        buckets if the endpoint shape changes. When both venues share one HL
        account (include_core_equity cleared on the hedge), that venue reports
        only its dex bucket to avoid double-counting."""
        addr = self._query_address()
        if addr is None:
            return None
        self.margin_source = "account_equity_poll"
        portfolio_equity = None
        if self.include_core_equity:
            try:
                p = await self._info({"type": "portfolio", "user": addr})
                for period, d in p:
                    if period == "day":
                        hist = d.get("accountValueHistory") or []
                        if hist:
                            portfolio_equity = float(hist[-1][1])
                            break
            except Exception as e:
                log.debug("[%s] portfolio fetch failed, falling back: %r",
                          self.name, e)
        dexs = [self.conf.hl_dex] + ([""] if self.include_core_equity else [])
        eq = fr = 0.0
        for dex in dexs:
            st = await self._info({"type": "clearinghouseState", "user": addr,
                                   "dex": dex})
            ms = st.get("marginSummary") or {}
            eq += float(ms.get("accountValue") or 0.0)
            fr += float(st.get("withdrawable") or 0.0)
        try:
            unified_available = await self._unified_available_margin(addr)
        except Exception as e:
            log.debug("[%s] unified margin fetch failed, using withdrawable: %r",
                      self.name, e)
            unified_available = None
        if unified_available is not None:
            fr = unified_available
            self.margin_source = "unified_account"
        return (portfolio_equity if portfolio_equity is not None else eq), fr

    async def fetch_position(self) -> float:
        addr = self._query_address()
        assert addr is not None
        st = await self._info({"type": "clearinghouseState", "user": addr,
                               "dex": self.conf.hl_dex})
        for ap in st.get("assetPositions") or []:
            pos = ap.get("position") or {}
            if pos.get("coin") == self.coin:
                return float(pos.get("szi") or 0.0)
        return 0.0

    async def close(self) -> None:
        # A WS terminal can release business while this bounded HTTP request
        # still owns the shared session and its final diagnostics.
        pending = tuple(self._submit_tasks)
        if pending:
            await asyncio.shield(asyncio.gather(*pending, return_exceptions=True))
