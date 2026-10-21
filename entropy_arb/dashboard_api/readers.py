from __future__ import annotations

import csv
import json
import math
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from ..trade_ledger import TradeLedger


RANGE_SECONDS = {"1h": 3600, "6h": 21600, "24h": 86400}
# Presentation budgets only. The SQLite ledger remains the source of truth;
# the browser does not need thousands of SVG vertices for a 1h/24h overview.
MAX_HISTORY_POINTS = 720
MAX_PNL_POINTS = 720


class SnapshotUnavailable(Exception):
    pass


def _number(value) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def downsample_points(points: list[dict], limit: int,
                      value_key: str) -> list[dict]:
    """Keep chart payloads bounded while preserving extrema and endpoints.

    The database remains the source of truth.  This is only a presentation
    budget for the browser: each bucket contributes its low/high point, then
    evenly spaced points fill gaps when a series is monotonic or flat.
    """
    if len(points) <= limit:
        return points
    if limit < 3:
        return points[:limit]

    selected = {0, len(points) - 1}
    interior = len(points) - 2
    bucket_count = max(1, (limit - 2) // 2)

    def point_value(index: int) -> float:
        value = _number(points[index].get(value_key))
        return value if value is not None else 0.0

    for bucket in range(bucket_count):
        start = 1 + (bucket * interior) // bucket_count
        end = 1 + ((bucket + 1) * interior) // bucket_count
        if end <= start:
            continue
        indices = range(start, end)
        low = min(indices, key=point_value)
        high = max(indices, key=point_value)
        selected.update((low, high))

    # Flat/monotonic buckets can select fewer than two unique points. Fill
    # deterministically so the chart still uses the available resolution.
    if len(selected) < limit:
        for slot in range(1, limit - 1):
            index = round(slot * (len(points) - 1) / (limit - 1))
            selected.add(index)
            if len(selected) >= limit:
                break
    return [points[index] for index in sorted(selected)]


def read_status(path: Path, now: Optional[float] = None) -> dict:
    now = time.time() if now is None else now
    try:
        with path.open(encoding="utf-8") as fh:
            payload = json.load(fh)
    except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
        raise SnapshotUnavailable from exc
    if not isinstance(payload, dict) or _number(payload.get("ts")) is None:
        raise SnapshotUnavailable
    age = max(0.0, now - float(payload["ts"]))
    return {**payload, "server_now": now, "snapshot_age_sec": age,
            "is_stale": age > 5.0}


def read_history(path: Path, range_name: str,
                 now: Optional[float] = None) -> dict:
    now = time.time() if now is None else now
    cutoff = now - RANGE_SECONDS[range_name]
    bucket_sec = {"1h": 2, "6h": 12, "24h": 44}[range_name]
    buckets: dict[int, dict] = {}
    try:
        with path.open(newline="") as fh:
            for row in csv.DictReader(fh):
                ts = _number(row.get("ts"))
                premium = _number(row.get("premium_mid_bps"))
                if ts is None or premium is None or ts < cutoff or ts > now:
                    continue
                point = {
                    "ts": ts, "premium_bps": premium,
                    "active_midline_bps": _number(
                        row.get("active_midline_bps")),
                    "short_entry_bps": _number(row.get("short_entry_bps")),
                    "short_exit_bps": _number(row.get("short_exit_bps")),
                    "long_entry_bps": _number(row.get("long_entry_bps")),
                    "long_exit_bps": _number(row.get("long_exit_bps")),
                    "short_center_bps": _number(row.get("short_center_bps")),
                }
                buckets[int(ts // bucket_sec)] = point
    except (FileNotFoundError, OSError, csv.Error):
        pass
    points = sorted(buckets.values(), key=lambda p: p["ts"])
    return {"range": range_name,
            "points": downsample_points(points, MAX_HISTORY_POINTS,
                                         "premium_bps")}


def _leg_status(value, fill_qty: float) -> str:
    status = str(value or "").strip().lower().replace("-", "_")
    if status:
        return status
    return "filled" if fill_qty > 0 else "unknown"


def _trade_outcome(qty: float, buy_fill: float, sell_fill: float) -> str:
    tolerance = max(1e-8, qty * 1e-4)
    buy_filled = buy_fill > tolerance
    sell_filled = sell_fill > tolerance
    if not buy_filled and not sell_filled:
        return "no_fill"
    if not buy_filled or not sell_filled:
        return "one_leg"
    if abs(buy_fill - sell_fill) > tolerance:
        return "one_leg"
    if buy_fill + tolerance >= qty and sell_fill + tolerance >= qty:
        return "paired"
    return "partial_paired"


def _normalized_leg(row: dict, venue: str, qty: float,
                    buy_fill: float, sell_fill: float,
                    pnl_status: str = "unavailable") -> dict:
    buy_venue = str(row.get("buy_venue") or "").strip().lower()
    sell_venue = str(row.get("sell_venue") or "").strip().lower()
    hedge_aliases = {"hedge", "lighter", "rh", "xyz"}
    tokens = hedge_aliases if venue == "hedge" else {venue}
    if any(token in buy_venue for token in tokens):
        return {"side": "buy", "planned_qty": qty, "fill_qty": buy_fill,
                "status": _leg_status(row.get("buy_status"), buy_fill),
                "fee_usd": None, "fee_status": "none",
                "gross_realized_pnl_usd": None,
                "net_realized_pnl_usd": None, "pnl_status": pnl_status}
    if any(token in sell_venue for token in tokens):
        return {"side": "sell", "planned_qty": qty, "fill_qty": sell_fill,
                "status": _leg_status(row.get("sell_status"), sell_fill),
                "fee_usd": None, "fee_status": "none",
                "gross_realized_pnl_usd": None,
                "net_realized_pnl_usd": None, "pnl_status": pnl_status}
    return {"side": "unknown", "planned_qty": qty, "fill_qty": 0.0,
            "status": "unknown", "fee_usd": None, "fee_status": "none",
            "gross_realized_pnl_usd": None,
            "net_realized_pnl_usd": None, "pnl_status": pnl_status}


def read_trades(path: Path, limit: int,
                coverage_started_ts: Optional[float] = None,
                offset: int = 0) -> dict:
    page_limit = max(1, min(int(limit), 100))
    page_offset = max(0, int(offset))
    rows: deque[dict] = deque(maxlen=page_offset + page_limit)
    summary = {
        "coverage_started_ts": coverage_started_ts,
        "attempt_count": 0,
        "matched_count": 0,
        "paired_count": 0,
        "partial_paired_count": 0,
        "one_leg_count": 0,
        "no_fill_count": 0,
        "matched_rate": 0.0,
        "paired_fill_edge_usd": 0.0,
    }
    coverage_count = 0
    try:
        with path.open(newline="") as fh:
            for row in csv.DictReader(fh):
                ts = _number(row.get("ts"))
                if (ts is None or (coverage_started_ts is not None
                                   and ts < coverage_started_ts)):
                    continue
                rows.append(row)
                coverage_count += 1
                qty = _number(row.get("qty")) or 0.0
                buy_fill = abs(_number(row.get("buy_fill")) or 0.0)
                sell_fill = abs(_number(row.get("sell_fill")) or 0.0)
                outcome = _trade_outcome(qty, buy_fill, sell_fill)
                summary["attempt_count"] += 1
                summary[f"{outcome}_count"] += 1
                if outcome in {"paired", "partial_paired"}:
                    summary["matched_count"] += 1
                    summary["paired_fill_edge_usd"] += (
                        _number(row.get("fill_edge_usd")) or 0.0)
    except (FileNotFoundError, OSError, csv.Error):
        return {"summary": summary, "total_count": 0,
                "offset": page_offset, "limit": page_limit,
                "has_more": False, "trades": []}

    if summary["attempt_count"]:
        summary["matched_rate"] = (
            summary["matched_count"] / summary["attempt_count"])

    trades = []
    for row in list(reversed(rows))[page_offset:page_offset + page_limit]:
        ts = _number(row.get("ts"))
        if ts is None:
            continue
        buy_notional = _number(row.get("buy_notional")) or 0.0
        sell_notional = _number(row.get("sell_notional")) or 0.0
        notional = (abs(buy_notional) + abs(sell_notional)) / 2.0
        qty = _number(row.get("qty")) or 0.0
        buy_fill = abs(_number(row.get("buy_fill")) or 0.0)
        sell_fill = abs(_number(row.get("sell_fill")) or 0.0)
        fill_edge_usd = _number(row.get("fill_edge_usd"))
        fill_edge_bps = (fill_edge_usd / notional * 1e4
                         if fill_edge_usd is not None and notional else None)
        action = str(row.get("action") or "").lower()
        outcome = _trade_outcome(qty, buy_fill, sell_fill)
        pnl_status = ("not_closed" if action in {"open", "add"}
                      else "unavailable")
        reduce_only = str(row.get("reduce_only", "")).lower() \
            in {"1", "true", "yes"}
        trades.append({
            "ts": ts,
            "action": action,
            "direction": str(row.get("direction") or "").lower(),
            "notional_usd": notional,
            "qty": qty,
            "threshold_bps": _number(row.get("threshold_bps")),
            "net_edge_bps": _number(row.get("net_marginal_edge_bps")),
            "fill_edge_bps": fill_edge_bps,
            "fill_edge_usd": fill_edge_usd,
            "outcome": outcome,
            "buy_venue": str(row.get("buy_venue") or "").lower(),
            "sell_venue": str(row.get("sell_venue") or "").lower(),
            "entropy_leg": _normalized_leg(
                row, "entropy", qty, buy_fill, sell_fill, pnl_status),
            "lighter_leg": _normalized_leg(
                row, "hedge", qty, buy_fill, sell_fill, pnl_status),
            "gross_realized_pnl_usd": None,
            "net_realized_pnl_usd": None,
            "pnl_status": pnl_status,
            "reduce_only": reduce_only,
        })
    return {"summary": summary, "total_count": coverage_count,
            "offset": page_offset, "limit": page_limit,
            "has_more": page_offset + len(trades) < coverage_count,
            "trades": trades}


def read_pnl(seconds_path: Path, status_path: Path, day_timezone: str,
             now: Optional[float] = None) -> dict:
    now = time.time() if now is None else now
    tz = ZoneInfo(day_timezone)
    today = datetime.fromtimestamp(now, tz=tz).date()
    points = []
    try:
        with seconds_path.open(newline="") as fh:
            for row in csv.DictReader(fh):
                ts = _number(row.get("ts"))
                pnl = _number(row.get("net_pnl_usd"))
                if (ts is not None and pnl is not None
                        and datetime.fromtimestamp(ts, tz=tz).date() == today):
                    points.append({"ts": ts, "net_pnl_usd": pnl})
    except (FileNotFoundError, OSError, csv.Error):
        pass

    summary = {"net_pnl_usd": None, "gross_pnl_usd": None,
               "fees_usd": None, "fees_status": "none",
               "actual_fee_usd": None, "estimated_fee_usd": None,
               "funding_pnl_usd": None}
    try:
        current = read_status(status_path, now)["today"]
        summary = {key: current.get(key) for key in summary}
        if summary["fees_status"] is None:
            summary["fees_status"] = "none"
    except SnapshotUnavailable:
        pass
    return {"range": "today", "summary": summary,
            "points": downsample_points(points, MAX_PNL_POINTS,
                                         "net_pnl_usd"),
            # The legacy CSV has no durable absolute-equity baseline. Keep
            # this explicit instead of presenting today's curve as lifetime.
            "lifetime_points": []}


def read_pnl_from_ledger(ledger: TradeLedger, status_path: Path,
                         day_timezone: str,
                         now: Optional[float] = None) -> dict:
    """Read durable PnL points, keeping the status snapshot as the headline."""
    now = time.time() if now is None else now
    tz = ZoneInfo(day_timezone)
    today = datetime.fromtimestamp(now, tz=tz).date()
    since = datetime.combine(today, datetime.min.time(), tzinfo=tz).timestamp()
    points = downsample_points(
        ledger.pnl_points(since=since, until=now, limit=MAX_PNL_POINTS),
        MAX_PNL_POINTS,
        "net_pnl_usd")
    lifetime_points = downsample_points(
        ledger.lifetime_pnl_points(until=now, limit=MAX_PNL_POINTS),
        MAX_PNL_POINTS,
        "net_pnl_usd")
    summary = {"net_pnl_usd": None, "gross_pnl_usd": None,
               "fees_usd": None, "fees_status": "none",
               "actual_fee_usd": None, "estimated_fee_usd": None,
               "funding_pnl_usd": None,
               "realized_pnl_usd": None, "unrealized_pnl_usd": None,
               "ledger_net_pnl_usd": None, "unexplained_usd": None}
    try:
        snapshot = read_status(status_path, now)
        current = snapshot["today"]
        summary.update({key: current.get(key) for key in (
            "net_pnl_usd", "gross_pnl_usd", "fees_usd",
            "fees_status", "actual_fee_usd", "estimated_fee_usd",
            "funding_pnl_usd", "realized_pnl_usd", "unrealized_pnl_usd")})
        if summary["fees_status"] is None:
            summary["fees_status"] = "none"
        attribution = snapshot.get("attribution") or {}
        day = attribution.get("day") or {}
        summary.update({"ledger_net_pnl_usd": day.get("ledger_net_pnl_usd"),
                        "unexplained_usd": day.get("unexplained_usd")})
    except (SnapshotUnavailable, KeyError, TypeError):
        pass
    return {"range": "today", "summary": summary, "points": points,
            "lifetime_points": lifetime_points}


def read_dashboard(root: Path, ledger: TradeLedger, range_name: str,
                   timezone: str, now: Optional[float] = None,
                   status_path: Optional[Path] = None) -> dict:
    """Assemble one coherent dashboard response from the shared sources."""
    now = time.time() if now is None else now
    status_path = status_path or (root / "status.json")
    seconds_path = root / "seconds.csv"
    trades_path = root / "trades.csv"
    status = read_status(status_path, now)
    if ledger.has_data("market_samples"):
        raw_history = ledger.history(range_name, now)
        history = {
            **raw_history,
            "points": downsample_points(raw_history["points"],
                                         MAX_HISTORY_POINTS, "premium_bps"),
        }
    else:
        history = read_history(seconds_path, range_name, now)
    today = status.get("today") or {}
    # The dashboard's trade table is a daily audit view.  The session
    # baseline is still exposed in PnL, but using it here made a restart hide
    # the trades from earlier in the same local day while the KPI still
    # counted them.
    coverage = (today.get("day_started_ts")
                or today.get("session_started_ts"))
    trades = (ledger.list_trades(20, coverage)
              if ledger.has_data("trade_attempts")
              else read_trades(trades_path, 20, coverage))
    pnl = (read_pnl_from_ledger(ledger, status_path, timezone, now)
           if ledger.has_data("pnl_snapshots")
           else read_pnl(seconds_path, status_path, timezone, now))
    return {"as_of_ts": status["ts"], "status": status,
            "history": history, "trades": trades, "pnl": pnl}
