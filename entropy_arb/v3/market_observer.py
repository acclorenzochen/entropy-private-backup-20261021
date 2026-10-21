"""Read-only, executable market snapshots for the separate V3 recorder."""
from __future__ import annotations

import asyncio
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid
from typing import Any, Callable, Mapping

from .core import BPS, Direction, Instrument, Settings
from .runtime import RuntimeHalt, RuntimeVenues, _minimum_units, market_from_venues


def source_identity() -> dict[str, Any]:
    """Fingerprint recorder dependencies, including dirty source bytes."""
    root = Path(__file__).resolve().parents[2]
    paths = ['entropy_arb/v3/market_observer.py', 'entropy_arb/v3/runtime.py',
             'entropy_arb/v3/core.py', 'entropy_arb/v3/reference.py',
             'entropy_arb/book.py', 'entropy_arb/feeds.py', 'main_v3.py']
    hashes = {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
              for name in paths}
    digest = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
    return {'source_version': 'sha256:' + digest, 'source_files': hashes,
            'source_hash_evidence': 'disk_files_at_recorder_initialization'}


def quote_snapshot(snapshot: Mapping[str, Any], units: int,
                   direction: Direction) -> dict[str, Any]:
    """Quote a fixed episode quantity independently of the new-entry grid."""
    if type(units) is not int or units <= 0:
        raise ValueError('positive integer units required')
    if snapshot.get('valid') is not True:
        return {'state': 'session_gap', 'basis_bps': None}
    sides = ('asks', 'bids') if direction is Direction.LONG_ENTROPY else ('bids', 'asks')
    cash = []
    try:
        step = Decimal(snapshot['instrument']['quantity_step'])
        for venue, side in zip(('entropy', 'lighter'), sides):
            levels = snapshot[venue][side]
            if not levels:
                return {'state': 'missing_snapshot', 'basis_bps': None}
            remaining, total = units, Decimal(0)
            for price, available in levels:
                price = Decimal(price)
                if not price.is_finite() or price <= 0 or type(available) is not int or available <= 0:
                    raise ValueError('invalid depth')
                take = min(remaining, available)
                total += price * take * step
                remaining -= take
                if not remaining:
                    break
            if remaining:
                return {'state': 'depth_not_covered', 'basis_bps': None}
            cash.append(total)
        basis = (cash[0] / cash[1] - 1) * BPS
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return {'state': 'missing_snapshot', 'basis_bps': None}
    minimum = snapshot.get('minimum_entry_units')
    cap = snapshot.get('max_notional_usd')
    legal = (units >= minimum and max(cash) <= Decimal(cap)
             if minimum is not None and cap is not None else None)
    return {'state': 'known_quote', 'basis_bps': str(basis),
            'new_entry_legal': legal,
            'entry_quality': ('quantity_not_legal_for_new_entry' if legal is False
                              else 'known_quote' if legal else 'unknown')}


class MarketSampler:
    def __init__(self, pair_id: str, settings: Settings,
                 venues: RuntimeVenues, instrument: Instrument,
                 config_hash: str, session_id: str | None = None) -> None:
        self.pair_id, self.settings = pair_id, settings
        self.venues, self.instrument = venues, instrument
        self.config_hash = config_hash
        self.session_id = session_id or uuid.uuid4().hex
        self.sequence = 0
        self.last_emit_mono = 0.0
        self.last_revisions: tuple | None = None
        self.source = source_identity()
        self.market_valid = False
        self._quality = None
        self._generations = None
        self._history: deque[tuple[float, bytes]] = deque(maxlen=512)
        self.history_evicted = 0
        self.history_gaps = 0

    def history(self) -> tuple[dict[str, Any], ...]:
        # Stored bytes cannot be mutated through the returned snapshots.
        return tuple(json.loads(payload) for _, payload in self._history)

    def _remember(self, event, mono):
        while self._history and (mono - self._history[0][0] > 30 or
                                 len(self._history) == self._history.maxlen):
            self._history.popleft()
            self.history_evicted += 1
        if event['continuity_break']:
            self.history_gaps += 1
        self._history.append((mono, json.dumps(event).encode()))
        event['history_coverage'] = {
            'start_mono_ns': int(self._history[0][0] * 1e9),
            'end_mono_ns': int(mono * 1e9), 'samples': len(self._history),
            'evicted_total': self.history_evicted, 'gap_total': self.history_gaps}
        return event

    def sample(self, *, now: float | None = None,
               mono: float | None = None) -> dict[str, Any] | None:
        now = time.time() if now is None else now
        mono = time.monotonic() if mono is None else mono
        generations = tuple(v.book.connection_generation for v in
                            (self.venues.entropy, self.venues.hedge))
        try:
            market = market_from_venues(
                self.venues, self.instrument, now=now, settings=self.settings)
            quality = None
        except (RuntimeHalt, ValueError) as exc:
            market, quality = None, str(exc)
        self.market_valid = market is not None
        boundary = (generations != self._generations or quality != self._quality)
        clock_gap = bool(self.sequence and
                         (mono <= self.last_emit_mono or mono - self.last_emit_mono > 2))
        boundary = boundary or clock_gap
        revisions = ((market.entropy.bids, market.entropy.asks,
                      market.hedge.bids, market.hedge.asks) if market else None)
        changed = revisions != self.last_revisions
        if self.sequence and not boundary and mono - self.last_emit_mono < (0.1 if changed else 1.0):
            return None
        self.sequence += 1
        self.last_emit_mono = mono
        self.last_revisions = revisions
        self._quality, self._generations = quality, generations
        common = {
            'kind': 'v3_market_snapshot', 'schema_version': 2,
            'pair_id': self.pair_id, 'session_id': self.session_id,
            'sample_seq': self.sequence, 'config_hash': self.config_hash,
            'source_version': self.source['source_version'],
            'capture_wall_ts': now, 'capture_mono_ns': int(mono * 1e9),
            'valid': self.market_valid, 'quality_reason': quality,
            'coverage_reason': 'capture_clock_gap' if clock_gap else None,
            'continuity_break': boundary and self.sequence > 1,
            'event_type': ('quality_boundary' if boundary else
                           'book_change' if changed else 'heartbeat'),
        }
        if market is None:
            for name, venue in (('entropy', self.venues.entropy), ('lighter', self.venues.hedge)):
                raw = venue.book
                common[name] = {'connection_generation': raw.connection_generation,
                                'received_wall_ts': raw.last_update_ts or None,
                                'received_mono_ns': raw.last_update_mono_ns or None,
                                'stream_alive_ts': raw.alive_ts}
            return self._remember({**common, 'quotes': []}, mono)
        max_price = max(market.entropy.mid, market.hedge.mid)
        cap = int((self.settings.max_notional_usd / max_price /
                   self.instrument.quantity_step).to_integral_value(rounding=ROUND_FLOOR))
        minimum = max(_minimum_units(self.venues.entropy, self.instrument,
                                     market.entropy.mid),
                      _minimum_units(self.venues.hedge, self.instrument,
                                     market.hedge.mid))
        depths = [sum(q for _, q in side) for side in (
            market.entropy.bids, market.entropy.asks,
            market.hedge.bids, market.hedge.asks)]
        uppers = (min(cap, depths[1], depths[2]), min(cap, depths[0], depths[3]))
        candidates = sorted({q for upper in uppers if upper >= minimum
                             for q in (minimum, max(minimum, upper // 2), upper)})
        quotes = []
        for units in candidates:
            row: dict[str, Any] = {'units': units}
            for direction, name in ((Direction.LONG_ENTROPY, 'buy_entropy_basis_bps'),
                                    (Direction.SHORT_ENTROPY, 'sell_entropy_basis_bps')):
                side = direction.entropy_side
                try:
                    entropy_cash, _ = market.entropy.quote(
                        side, units, self.instrument.quantity_step)
                    hedge_cash, _ = market.hedge.quote(
                        side.opposite, units, self.instrument.quantity_step)
                    row[name] = (str((entropy_cash / hedge_cash - 1) * BPS)
                                 if max(entropy_cash, hedge_cash)
                                 <= self.settings.max_notional_usd else None)
                except ValueError:
                    row[name] = None
            quotes.append(row)
        def side_data(book, venue):
            raw = venue.book
            return {
                'exchange_ts': getattr(raw, 'exchange_ts', None),
                'received_wall_ts': raw.last_update_ts or None,
                'received_mono_ns': raw.last_update_mono_ns or None,
                'connection_generation': raw.connection_generation,
                'book_revision': book.revision,
                'stream_alive_ts': raw.alive_ts,
                'last_book_update_ts': raw.last_update_ts,
                'bids': [[str(px), units] for px, units in book.bids],
                'asks': [[str(px), units] for px, units in book.asks],
                'depth_coverage_units': {
                    'bids': sum(units for _, units in book.bids),
                    'asks': sum(units for _, units in book.asks)},
            }
        return self._remember({
            **common,
            'minimum_entry_units': minimum,
            'max_notional_usd': str(self.settings.max_notional_usd),
            'candidate_units': candidates,
            'live_quantity_policy': 'choose_open_plan:descending_common_depth_cap',
            'live_quantity_upper_units': min(cap, *depths),
            'live_target_units': None,
            'live_target_reason': 'requires_reference_and_open_decision',
            'instrument': {
                'quantity_step': str(self.instrument.quantity_step),
                'entropy_tick': str(self.instrument.entropy_tick),
                'hedge_tick': str(self.instrument.hedge_tick),
                'entropy_fee_bps': str(self.instrument.entropy_fee_bps),
                'hedge_fee_bps': str(self.instrument.hedge_fee_bps),
                'entropy_min_base': str(self.venues.entropy.min_base),
                'entropy_min_quote': str(self.venues.entropy.min_quote),
                'lighter_min_base': str(self.venues.hedge.min_base),
                'lighter_min_quote': str(self.venues.hedge.min_quote),
            },
            'capture_wall_ts': now, 'capture_mono_ns': int(mono * 1e9),
            'valid': True, 'quality_reason': None,
            'mid_basis_bps': str(market.mid_basis_bps),
            'entropy': side_data(market.entropy, self.venues.entropy),
            'lighter': side_data(market.hedge, self.venues.hedge),
            'quotes': quotes,
        }, mono)


class ObserverFileWriter:
    """Coalesce ordinary checkpoint/status writes off the event loop.

    Callbacks must only write ordinary files, never a trading Journal. At most
    one pending immutable argument snapshot is retained per configured key.
    """

    def __init__(self, intervals: Mapping[str, float]) -> None:
        if not intervals or any(value <= 0 for value in intervals.values()):
            raise ValueError('positive write intervals required')
        self.intervals = dict(intervals)
        self.pending: dict[str, tuple[Callable, tuple, dict]] = {}
        self.last_write: dict[str, float] = {}
        self.errors = 0
        self.coalesced = 0
        self.last_error: str | None = None
        self._wake = asyncio.Event()
        self._task = None
        self._closed = False

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError('file writer already started')
        self._task = asyncio.create_task(self._run(), name='observer-file-writer')

    def publish(self, key: str, callback: Callable, *args, **kwargs) -> None:
        if key not in self.intervals:
            raise ValueError('unconfigured file writer key')
        if self._closed or self._task is None:
            raise RuntimeError('file writer is not running')
        self.coalesced += key in self.pending
        self.pending[key] = (callback, deepcopy(args), deepcopy(kwargs))
        self._wake.set()

    async def close(self) -> None:
        self._closed = True
        self._wake.set()
        if self._task is not None:
            await self._task

    async def _run(self) -> None:
        while not self._closed or self.pending:
            self._wake.clear()
            now = time.monotonic()
            ready = [key for key in self.pending if self._closed or
                     now - self.last_write.get(key, -float('inf')) >= self.intervals[key]]
            for key in ready:
                callback, args, kwargs = self.pending.pop(key)
                try:
                    await asyncio.to_thread(callback, *args, **kwargs)
                except Exception as exc:
                    self.errors += 1
                    self.last_error = f'{type(exc).__name__}: {exc}'
                self.last_write[key] = time.monotonic()
            if self._closed:
                continue
            delay = min((max(0, self.last_write.get(key, -float('inf')) +
                             self.intervals[key] - time.monotonic())
                         for key in self.pending), default=None)
            try:
                await asyncio.wait_for(self._wake.wait(), delay)
            except asyncio.TimeoutError:
                pass


class BufferedJSONLObserver:
    """Bounded, best-effort single writer for the separate record-only process."""

    def __init__(self, path: str | Path, header: Mapping[str, Any], *,
                 queue_size: int = 4096, batch_size: int = 128,
                 flush_seconds: float = 1.0, rotate_seconds: float = 3600.0,
                 max_file_bytes: int = 64 * 1024 * 1024,
                 retain_seconds: float = 72 * 3600.0,
                 max_total_bytes: int = 1024 * 1024 * 1024,
                 append_only: bool = False) -> None:
        if (min(queue_size, batch_size, max_file_bytes, max_total_bytes) <= 0 or
                min(flush_seconds, rotate_seconds, retain_seconds) <= 0 or
                max_total_bytes < max_file_bytes):
            raise ValueError("observer limits must be positive and total >= file size")
        self.path = Path(path)
        self.append_only = append_only
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=queue_size)
        self.batch_size, self.flush_seconds = batch_size, flush_seconds
        self.rotate_seconds, self.max_file_bytes = rotate_seconds, max_file_bytes
        self.retain_seconds, self.max_total_bytes = retain_seconds, max_total_bytes
        self.identity = {key: header[key] for key in
                         ('pair_id', 'session_id', 'config_hash') if key in header}
        source = source_identity()
        self.identity['source_version'] = source['source_version']
        self.header = self._encode({"source_ts": time.time(), **dict(header), **source})
        self._archive_pattern = re.compile(
            rf"{re.escape(self.path.stem)}\.\d{{8}}T\d{{6}}Z-\d+{re.escape(self.path.suffix)}")
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._active_size = 0
        self._active_started_at = 0.0
        self._active_has_events = False
        self._needs_header = True
        self._last_prune = 0.0
        self.dropped_events = 0
        self.last_error: str | None = None
        self._event_seq = 0
        self._sample_seq = 0
        self._reported_dropped = 0

    def _gap(self) -> bytes:
        return self._encode({**self.identity, 'kind': 'v3_observer_gap',
                             'valid': False, 'quality_reason': 'observer_dropped_events',
                             'observer_dropped_total': self.dropped_events,
                             'observer_event_seq': self._event_seq,
                             'capture_wall_ts': time.time(),
                             'capture_mono_ns': time.monotonic_ns()})

    @staticmethod
    def _encode(event: Mapping[str, Any]) -> bytes:
        return (json.dumps(event, sort_keys=True, ensure_ascii=False,
                           default=str) + "\n").encode("utf-8")

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("observer writer already started")
        self._task = asyncio.create_task(self._run(), name="record-only-jsonl-writer")

    def publish(self, event: Mapping[str, Any]) -> None:
        event = dict(event)
        if event.get('kind') == 'v3_market_snapshot':
            # Include bootstrap invalids emitted before an instrument/sampler
            # exists in the same recorder sequence as later valid snapshots.
            self._sample_seq += 1
            event['sample_seq'] = self._sample_seq
        gap = b''
        if self.dropped_events > self._reported_dropped:
            self._event_seq += 1
            gap = self._gap()
        self._event_seq += 1
        if self._closed or self._task is None:
            self.dropped_events += 1
            return
        try:
            payload = self._encode({**self.identity, "source_ts": time.time(),
                                    "observer_dropped_total": self.dropped_events,
                                    **dict(event),
                                    'observer_event_seq': self._event_seq})
            payload = gap + payload
            self.queue.put_nowait(payload)
            self._reported_dropped = self.dropped_events
        except (asyncio.QueueFull, TypeError, ValueError) as exc:
            self.dropped_events += 1
            self.last_error = f"{type(exc).__name__}: {exc}"

    async def close(self) -> None:
        if self._task is None or self._closed:
            return
        self._closed = True
        if self.dropped_events > self._reported_dropped:
            self._event_seq += 1
            await self.queue.put(self._gap())
            self._reported_dropped = self.dropped_events
        await self.queue.put(None)
        await self._task

    def abort(self) -> None:
        """Bounded owner shutdown; an in-flight file worker may finish later."""
        self._closed = True
        self.dropped_events += self.queue.qsize()
        self.last_error = 'flush_incomplete'
        if self._task is not None:
            self._task.cancel()

    async def _run(self) -> None:
        try:
            await asyncio.to_thread(self._prune, time.time())
        except OSError as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
        loop = asyncio.get_running_loop()
        while True:
            item = await self.queue.get()
            if item is None:
                break
            batch = [item]
            deadline = loop.time() + self.flush_seconds
            closing = False
            while len(batch) < self.batch_size:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(self.queue.get(), remaining)
                except asyncio.TimeoutError:
                    break
                if item is None:
                    closing = True
                    break
                batch.append(item)
            try:
                self.last_error = await asyncio.to_thread(self._write_batch, batch)
            except OSError as exc:
                self.dropped_events += len(batch)
                self.last_error = f"{type(exc).__name__}: {exc}"
            if closing:
                break

    def _write_batch(self, batch: list[bytes]) -> str | None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self._active_started_at:
            if self.path.exists():
                if not self.path.is_file():
                    raise IsADirectoryError(self.path)
                stat = self.path.stat()
                self._active_size = stat.st_size
                self._active_started_at = stat.st_mtime
                self._active_has_events = stat.st_size > 0
            else:
                self._active_started_at = time.time()
        rotated = False
        fh = None
        try:
            for line in batch:
                now = time.time()
                if not self.append_only and self._active_has_events and (
                        now - self._active_started_at >= self.rotate_seconds or
                        self._active_size + len(line) +
                        (len(self.header) if self._needs_header else 0)
                        > self.max_file_bytes):
                    if fh is not None:
                        fh.close()
                        fh = None
                    stamp = datetime.fromtimestamp(now, timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    archive = self.path.with_name(
                        f"{self.path.stem}.{stamp}-{time.time_ns()}{self.path.suffix}")
                    os.replace(self.path, archive)
                    self._active_size = 0
                    self._active_started_at = now
                    self._active_has_events = False
                    self._needs_header = True
                    rotated = True
                if fh is None:
                    fh = self.path.open("ab")
                if self._needs_header:
                    fh.write(self.header)
                    self._active_size += len(self.header)
                    self._needs_header = False
                fh.write(line)
                self._active_size += len(line)
                self._active_has_events = True
        finally:
            if fh is not None:
                fh.close()
        now = time.time()
        if rotated or now - self._last_prune >= 60:
            try:
                self._prune(now)
            except OSError as exc:
                return f"{type(exc).__name__}: {exc}"
            self._last_prune = now
        return None

    def _prune(self, now: float) -> None:
        if self.append_only:
            return
        if not self.path.parent.exists():
            return
        archives = [p for p in self.path.parent.iterdir()
                    if self._archive_pattern.fullmatch(p.name) and p.is_file()]
        archives.sort(key=lambda p: p.stat().st_mtime)
        for archive in archives:
            if now - archive.stat().st_mtime > self.retain_seconds:
                archive.unlink()
        archives = [p for p in archives if p.exists()]
        total = sum(p.stat().st_size for p in archives)
        if self.path.is_file():
            total += self.path.stat().st_size
        for archive in archives:
            if total <= self.max_total_bytes:
                break
            size = archive.stat().st_size
            archive.unlink()
            total -= size
