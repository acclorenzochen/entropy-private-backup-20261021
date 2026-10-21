"""Fast re-anchoring, independent of trading/sampling permissions."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
import math
import json
import os
from pathlib import Path
import statistics
from typing import Mapping, Any

from .core import Settings, dec


def load_checkpoint(path: str | Path) -> dict[str, Any] | None:
    try:
        value = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def save_checkpoint(path: str | Path, checkpoint: Mapping[str, Any]) -> None:
    if not checkpoint:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f'.{target.name}.{os.getpid()}.tmp')
    with temporary.open('w', encoding='utf-8') as handle:
        json.dump(dict(checkpoint), handle, sort_keys=True, ensure_ascii=False)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)


@dataclass(frozen=True)
class ReferenceView:
    usable: bool
    center_bps: Decimal | None
    source: str
    live_samples: int
    live_span_seconds: float


class FastReference:
    """At most one sample per second. A stale connection resets live evidence.

    Fresh checkpoint only selects a SHORT validation period. Initialization
    ALWAYS anchors directly to a new-live-sample median, never slowly slews
    from a historic seed. Once initialized, a 5-sample median feeds an EWMA.
    All timestamps are UTC seconds supplied by the host; no lookahead clock.
    """
    VERSION = 1

    def __init__(self, pair_id: str, cfg: Settings,
                 checkpoint: Mapping[str, Any] | None = None,
                 *, started_at: float = 0.0) -> None:
        self.pair_id, self.cfg = pair_id, cfg
        self._samples: deque[tuple[float, Decimal]] = deque(maxlen=256)
        self._center: Decimal | None = None
        self._last_seen: float | None = None
        self._last_bucket: int | None = None
        self._updated_at: float | None = None
        self._warm = False
        self._market_valid = False
        if not math.isfinite(started_at):
            raise ValueError('invalid start time')
        self._started_at = started_at
        if checkpoint:
            try:
                age = started_at - float(checkpoint['ts'])
                dec(checkpoint['center_bps'])
                self._warm = bool(checkpoint['pair_id'] == pair_id
                                  and checkpoint['version'] == self.VERSION
                                  and 0 <= age <= cfg.checkpoint_max_age_seconds)
            except (ValueError, KeyError, TypeError, ArithmeticError):
                self._warm = False

    def invalidate(self) -> None:
        # No economic veto is latched. Fresh samples can rebuild without restart.
        self._samples.clear()
        self._center = None
        self._last_bucket = None
        self._last_seen = None
        self._updated_at = None
        self._warm = False
        self._market_valid = False

    def observe(self, ts: float, basis_bps: Decimal, *, valid: bool = True) -> ReferenceView:
        if not math.isfinite(ts) or ts < self._started_at or not basis_bps.is_finite():
            raise ValueError('invalid observation')
        if self._last_seen is not None and ts < self._last_seen:
            raise ValueError('out-of-order observation')
        if not valid:
            self._market_valid = False
            if self._last_seen is None or ts - self._last_seen > self.cfg.reset_gap_seconds:
                self.invalidate()
            return ReferenceView(False, self._center, 'invalid_market', len(self._samples), 0.0)
        if self._last_seen is not None and ts - self._last_seen > self.cfg.reset_gap_seconds:
            self.invalidate()
        self._market_valid = True
        bucket = math.floor(ts)
        self._last_seen = ts
        if bucket == self._last_bucket:
            return self.view(ts)
        self._last_bucket = bucket
        self._samples.append((ts, basis_bps))
        # Span and unique samples both matter; fast repeated scans cannot warm it.
        span = ts - self._samples[0][0]
        minimum = self.cfg.warm_samples if self._warm else self.cfg.cold_samples
        seconds = self.cfg.warm_seconds if self._warm else self.cfg.cold_seconds
        if self._center is None:
            if len(self._samples) >= minimum and span >= seconds:
                self._center = statistics.median(v for _, v in self._samples)
                self._updated_at = ts
        else:
            dt = ts - (self._updated_at if self._updated_at is not None else ts)
            alpha = dec(1 - 2 ** (-dt / self.cfg.reference_half_life_seconds))
            target = statistics.median(v for _, v in list(self._samples)[-5:])
            self._center += alpha * (target - self._center)
            self._updated_at = ts
        return self.view(ts)

    def restore_market_valid(self, now: float) -> ReferenceView:
        """Use a host-validated fresh snapshot without adding a reference sample."""
        if (not math.isfinite(now) or now < self._started_at
                or (self._last_seen is not None and now < self._last_seen)):
            raise ValueError('invalid clock')
        if self._last_seen is not None:
            if now - self._last_seen > self.cfg.reset_gap_seconds:
                self.invalidate()
            else:
                self._market_valid = True
        return self.view(now)

    def view(self, now: float) -> ReferenceView:
        if not math.isfinite(now):
            raise ValueError('invalid clock')
        span = self._samples[-1][0] - self._samples[0][0] if self._samples else 0.0
        age = now - self._last_seen if self._last_seen is not None else math.inf
        usable = self._market_valid and self._center is not None and 0 <= age <= self.cfg.reset_gap_seconds
        return ReferenceView(usable, self._center,
                             'live_reanchored' if self._center is not None else 'collecting',
                             len(self._samples), span)

    def checkpoint(self) -> dict[str, Any]:
        if self._center is None or self._updated_at is None:
            return {}
        return {'version': self.VERSION, 'pair_id': self.pair_id,
                'center_bps': str(self._center), 'ts': self._updated_at}
