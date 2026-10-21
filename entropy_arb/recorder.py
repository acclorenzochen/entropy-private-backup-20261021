"""Automatic 1-minute orderbook data recorder.

While the bot runs (live or --record-only), both venues' actual order books
are sampled once per second and aggregated into one CSV row per minute.
This is the dataset users analyze (tools/analyze.py) to choose
thresholds.midline_bps / upper_bps / lower_bps for config.yaml.

Definitions (all in bps, fees NOT included — the engine adds fees on top):

    premium    = (entropy_mid / hedge_mid - 1) * 1e4
                 the mid-to-mid premium of Entropy over the hedge venue;
                 its long-run center is what midline_bps hardcodes.
    sell_edge  = (entropy_bid / hedge_ask - 1) * 1e4
                 the EXECUTABLE premium for SELL-entropy/BUY-hedge; the
                 engine fires this direction when sell_edge clears
                 midline_bps + upper_bps (plus fees).
    buy_edge   = (hedge_bid / entropy_ask - 1) * 1e4
                 the executable premium for BUY-entropy/SELL-hedge; fires
                 when buy_edge clears lower_bps - midline_bps (plus fees).

Bid/ask columns are the minute's last fresh sample (close). A row is only
written for minutes with at least one sample where both books were fresh;
`samples` says how many of the ~60 seconds qualified.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Iterable, Optional

from .book import OrderBook
from .funding import FundingSnapshot
from .signal import (PremiumModelSnapshot, PremiumSampleQuality,
                     check_premium_sample_quality)

log = logging.getLogger("recorder")

HEADER = ["minute_ts", "time_utc",
          "entropy_bid", "entropy_ask", "hedge_bid", "hedge_ask",
          "premium_open_bps", "premium_high_bps", "premium_low_bps",
          "premium_close_bps", "premium_mean_bps", "premium_std_bps",
          "sell_edge_mean_bps", "sell_edge_max_bps",
          "buy_edge_mean_bps", "buy_edge_max_bps", "samples"]

SECONDS_HEADER = [
    "ts", "time_utc", "entropy_bid", "entropy_ask", "entropy_bid_size",
    "entropy_ask_size", "hedge_bid", "hedge_ask", "hedge_bid_size",
    "hedge_ask_size", "entropy_mid", "hedge_mid", "premium_mid_bps",
    "sell_edge_bps", "buy_edge_bps", "entropy_funding_bps_h",
    "hedge_funding_bps_h", "active_midline_bps", "short_center_bps",
    "long_center_bps", "robust_sigma_bps", "regime_shift",
    "short_entry_bps", "short_exit_bps", "long_entry_bps",
    "long_exit_bps", "effective_upper_bps", "effective_lower_bps",
    "volatility_guard", "cycle_active", "cycle_anchored", "cycle_direction",
    "cycle_entry_midline_bps", "cycle_entry_sigma_bps",
    "cycle_midline_drift_bps", "cycle_add_blocked", "pair_state",
    "net_pnl_usd", "turnover_usd", "entropy_book_age_ms",
    "hedge_book_age_ms", "cross_venue_skew_ms", "premium_sample_valid",
    "premium_sample_reject_reason", "midline_lag_bps", "midline_lag_state",
    "midline_move_speed_bps_per_10m", "sigma_center_bps", "sigma_window_sec",
    "premium_data_degraded", "premium_data_tier",
    "dynamic_midline_readiness", "dynamic_midline_tradable",
    "premium_data_uncertainty_bps", "reference_uncertainty_bps",
    "model_uncertainty_bps"]


class _MinuteAgg:
    __slots__ = ("minute", "n", "p_open", "p_high", "p_low", "p_close",
                 "p_sum", "p_sumsq", "s_sum", "s_max", "b_sum", "b_max",
                 "e_bid", "e_ask", "h_bid", "h_ask")

    def __init__(self, minute: int) -> None:
        self.minute = minute
        self.n = 0
        self.p_open = self.p_high = self.p_low = self.p_close = 0.0
        self.p_sum = self.p_sumsq = 0.0
        self.s_sum = 0.0
        self.s_max = -math.inf
        self.b_sum = 0.0
        self.b_max = -math.inf
        self.e_bid = self.e_ask = self.h_bid = self.h_ask = 0.0

    def add(self, e_bid: float, e_ask: float, h_bid: float, h_ask: float) -> None:
        e_mid = (e_bid + e_ask) / 2.0
        h_mid = (h_bid + h_ask) / 2.0
        prem = (e_mid / h_mid - 1.0) * 1e4
        sell_edge = (e_bid / h_ask - 1.0) * 1e4
        buy_edge = (h_bid / e_ask - 1.0) * 1e4
        if self.n == 0:
            self.p_open = self.p_high = self.p_low = prem
        self.n += 1
        self.p_high = max(self.p_high, prem)
        self.p_low = min(self.p_low, prem)
        self.p_close = prem
        self.p_sum += prem
        self.p_sumsq += prem * prem
        self.s_sum += sell_edge
        self.s_max = max(self.s_max, sell_edge)
        self.b_sum += buy_edge
        self.b_max = max(self.b_max, buy_edge)
        self.e_bid, self.e_ask, self.h_bid, self.h_ask = e_bid, e_ask, h_bid, h_ask

    def row(self) -> list:
        mean = self.p_sum / self.n
        var = max(self.p_sumsq / self.n - mean * mean, 0.0)
        ts = self.minute * 60
        return [ts,
                datetime.fromtimestamp(ts, tz=timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
                f"{self.e_bid:.10g}", f"{self.e_ask:.10g}",
                f"{self.h_bid:.10g}", f"{self.h_ask:.10g}",
                f"{self.p_open:.3f}", f"{self.p_high:.3f}",
                f"{self.p_low:.3f}", f"{self.p_close:.3f}",
                f"{mean:.3f}", f"{math.sqrt(var):.3f}",
                f"{self.s_sum / self.n:.3f}", f"{self.s_max:.3f}",
                f"{self.b_sum / self.n:.3f}", f"{self.b_max:.3f}",
                self.n]


class MinuteRecorder:
    def __init__(self, path: str, entropy_book: OrderBook, hedge_book: OrderBook,
                 staleness_sec: float, interval_sec: float = 1.0,
                 ledger=None) -> None:
        self.path = path
        self.entropy_book = entropy_book
        self.hedge_book = hedge_book
        self.staleness_sec = staleness_sec
        self.interval_sec = interval_sec
        self.ledger = ledger
        self.rows_written = 0
        self._agg: Optional[_MinuteAgg] = None
        self._fh = None
        self._writer = None

    def _open(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            # never append rows under a different schema's header
            with open(self.path) as fh0:
                if fh0.readline().strip() != ",".join(HEADER):
                    log.warning("%s has an old header — rotated to %s.old",
                                self.path, self.path)
                    os.replace(self.path, self.path + ".old")
        new = not os.path.exists(self.path) or os.path.getsize(self.path) == 0
        self._fh = open(self.path, "a", newline="")
        self._writer = csv.writer(self._fh)
        if new:
            self._writer.writerow(HEADER)
            self._fh.flush()
        log.info("recording 1-minute orderbook data -> %s", self.path)

    def _flush_agg(self) -> None:
        if self._agg is None or self._agg.n == 0:
            self._agg = None
            return
        if self._writer is None:
            self._open()
        row = self._agg.row()
        self._writer.writerow(row)
        self._fh.flush()
        if self.ledger is not None:
            try:
                self.ledger.record_minute_sample(
                    ts=float(row[0]),
                    payload=dict(zip(HEADER, row)),
                    source_key=f"minute:{int(float(row[0]))}")
            except Exception:
                # Market history is best effort; order/fill checkpoints are
                # the fail-closed persistence path in the engine.
                log.exception("SQLite minute sample write failed")
        self.rows_written += 1
        self._agg = None

    def sample(self, now: Optional[float] = None) -> None:
        """Take one sample; call ~1/sec. Rolls the minute over as needed."""
        now = time.time() if now is None else now
        minute = int(now // 60)
        if self._agg is not None and self._agg.minute != minute:
            self._flush_agg()
        if not (self.entropy_book.is_fresh(self.staleness_sec)
                and self.hedge_book.is_fresh(self.staleness_sec)):
            return
        e_bid, e_ask = self.entropy_book.best_bid(), self.entropy_book.best_ask()
        h_bid, h_ask = self.hedge_book.best_bid(), self.hedge_book.best_ask()
        if None in (e_bid, e_ask, h_bid, h_ask):
            return
        if self._agg is None:
            self._agg = _MinuteAgg(minute)
        self._agg.add(e_bid, e_ask, h_bid, h_ask)

    def close(self) -> None:
        """Flush the partial minute and close the file (call on shutdown)."""
        self._flush_agg()
        if self._fh is not None:
            self._fh.close()
            self._fh = self._writer = None

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    self.sample()
                except Exception:
                    log.exception("recorder sample failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval_sec)
                except asyncio.TimeoutError:
                    pass
        finally:
            self.close()
            log.info("recorder stopped — %d minute row(s) written to %s",
                     self.rows_written, self.path)


def bootstrap_premium_samples(path: str, now: float,
                              window_sec: float) -> Iterable[tuple[float, float]]:
    """Yield valid historical premiums in the requested trailing window."""
    cutoff = now - window_sec
    try:
        with open(path, newline="") as fh:
            for row in csv.DictReader(fh):
                try:
                    ts = float(row["ts"])
                    premium = float(row["premium_mid_bps"])
                except (KeyError, TypeError, ValueError):
                    continue
                valid = str(row.get("premium_sample_valid", "true")) \
                    .strip().lower()
                if valid in {"0", "false", "no", "invalid"}:
                    continue
                if cutoff <= ts <= now and math.isfinite(premium):
                    yield ts, premium
    except FileNotFoundError:
        return


class SecondRecorder:
    """One fresh top-of-book row per wall-clock second."""

    def __init__(self, path: str, entropy, hedge, staleness_sec: float,
                 snapshot_getter, interval_sec: float = 1.0,
                 strategy_getter=None, pnl_getter=None,
                 max_book_age_ms: float = 1200.0,
                 max_cross_venue_skew_ms: float = 500.0,
                 quality_getter=None, ledger=None) -> None:
        self.path = path
        self.entropy = entropy
        self.hedge = hedge
        self.staleness_sec = staleness_sec
        self.snapshot_getter = snapshot_getter
        self.strategy_getter = strategy_getter
        self.pnl_getter = pnl_getter
        self.max_book_age_ms = max_book_age_ms
        self.max_cross_venue_skew_ms = max_cross_venue_skew_ms
        self.quality_getter = quality_getter
        self.ledger = ledger
        self.interval_sec = interval_sec
        self.rows_written = 0
        self._last_second: Optional[int] = None
        self._fh = None
        self._writer = None

    def _open(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            with open(self.path) as fh0:
                if fh0.readline().strip() != ",".join(SECONDS_HEADER):
                    os.replace(self.path, self.path + ".old")
        new = not os.path.exists(self.path) or os.path.getsize(self.path) == 0
        self._fh = open(self.path, "a", newline="")
        self._writer = csv.writer(self._fh)
        if new:
            self._writer.writerow(SECONDS_HEADER)
            self._fh.flush()

    @staticmethod
    def _rate(v) -> str:
        snap: Optional[FundingSnapshot] = getattr(v, "funding", None)
        return "" if snap is None else f"{snap.rate_bps_per_hour:.6f}"

    @staticmethod
    def _opt(v: Optional[float]) -> str:
        return "" if v is None else f"{v:.6f}"

    def sample(self, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        second = int(now)
        if self._last_second is not None and second <= self._last_second:
            return False
        eb = self.entropy.book.sorted_bids()
        ea = self.entropy.book.sorted_asks()
        hb = self.hedge.book.sorted_bids()
        ha = self.hedge.book.sorted_asks()
        if not (eb and ea and hb and ha):
            return False
        quality: PremiumSampleQuality
        if self.quality_getter is not None:
            quality = self.quality_getter(now)
        else:
            quality = check_premium_sample_quality(
                now,
                self.entropy.book.last_update_ts
                if self.entropy.book.ready else None,
                self.hedge.book.last_update_ts
                if self.hedge.book.ready else None,
                self.max_book_age_ms, self.max_cross_venue_skew_ms)
        self._last_second = second
        e_mid = (eb[0][0] + ea[0][0]) / 2.0
        h_mid = (hb[0][0] + ha[0][0]) / 2.0
        premium = (e_mid / h_mid - 1.0) * 1e4
        sell_edge = (eb[0][0] / ha[0][0] - 1.0) * 1e4
        buy_edge = (hb[0][0] / ea[0][0] - 1.0) * 1e4
        snap: PremiumModelSnapshot = self.snapshot_getter(now)
        strategy = self.strategy_getter(now) if self.strategy_getter else {}
        pnl = self.pnl_getter() if self.pnl_getter else {}
        if self._writer is None:
            self._open()
        row = [
            f"{now:.3f}", datetime.fromtimestamp(now, tz=timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ"),
            eb[0][0], ea[0][0], eb[0][1], ea[0][1],
            hb[0][0], ha[0][0], hb[0][1], ha[0][1],
            e_mid, h_mid, f"{premium:.6f}", f"{sell_edge:.6f}",
            f"{buy_edge:.6f}", self._rate(self.entropy),
            self._rate(self.hedge), f"{snap.active_midline_bps:.6f}",
            self._opt(snap.short_center_bps), self._opt(snap.long_center_bps),
            self._opt(snap.robust_sigma_bps), int(snap.regime_shift),
            self._opt(strategy.get("short_entry_bps")),
            self._opt(strategy.get("short_exit_bps")),
            self._opt(strategy.get("long_entry_bps")),
            self._opt(strategy.get("long_exit_bps")),
            self._opt(strategy.get("effective_upper_bps")),
            self._opt(strategy.get("effective_lower_bps")),
            int(bool(strategy.get("volatility_guard", False))),
            int(bool(strategy.get("cycle_active", False))),
            int(bool(strategy.get("cycle_anchored", False))),
            strategy.get("cycle_direction") or "",
            self._opt(strategy.get("cycle_entry_midline_bps")),
            self._opt(strategy.get("cycle_entry_sigma_bps")),
            self._opt(strategy.get("cycle_midline_drift_bps")),
            int(bool(strategy.get("cycle_add_blocked", False))),
            strategy.get("pair_state", ""),
            self._opt(pnl.get("net_pnl_usd")), "",
            self._opt(quality.entropy_age_ms),
            self._opt(quality.hedge_age_ms),
            self._opt(quality.cross_venue_skew_ms),
            str(bool(quality.valid)).lower(), quality.reason or "",
            self._opt(snap.midline_lag_bps),
            str(getattr(snap.midline_lag_state, "value",
                        snap.midline_lag_state or "normal")),
            self._opt(snap.midline_move_speed_bps_per_10m),
            self._opt(snap.sigma_center_bps),
            self._opt(snap.sigma_window_sec),
            int(bool(strategy.get("premium_data_degraded", False))),
            strategy.get("premium_data_tier", "GOOD"),
            strategy.get("dynamic_midline_readiness", "UNAVAILABLE"),
            int(bool(strategy.get("dynamic_midline_tradable", False))),
            self._opt(strategy.get("premium_data_uncertainty_bps")),
            self._opt(strategy.get("reference_uncertainty_bps")),
            self._opt(strategy.get("model_uncertainty_bps"))]
        self._writer.writerow(row)
        self._fh.flush()
        if self.ledger is not None:
            try:
                self.ledger.record_market_sample(
                    ts=now, payload=dict(zip(SECONDS_HEADER, row)),
                    source_key=f"live:{second}")
            except Exception:
                log.exception("SQLite second sample write failed")
        self.rows_written += 1
        return True

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = self._writer = None

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    self.sample()
                except Exception:
                    log.exception("second recorder sample failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval_sec)
                except asyncio.TimeoutError:
                    pass
        finally:
            self.close()
