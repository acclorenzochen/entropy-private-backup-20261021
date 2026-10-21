"""SQLite-backed execution, fill, reconciliation and PnL ledger.

The trading loop remains the source of live decisions.  This module is the
durable audit trail behind those decisions: every execution checkpoint and
fill is idempotent, legacy CSVs can be imported without duplicating rows, and
the dashboard can use the same data model as the engine.

Only the Python standard library is used so record-only and recovery paths do
not acquire another runtime dependency.
"""
from __future__ import annotations

import csv
import errno
import hashlib
import json
import logging
import math
import sqlite3
import statistics
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows has no fcntl
    fcntl = None

from .execution_state import ExecutionPhase, ExecutionState
from .control_plane import is_clean_circle

log = logging.getLogger("trade-ledger")

SCHEMA_VERSION = 9
ZERO_FEE_SOURCE = "venue_zero_fee"

# Keep SQLite contention bounded.  A critical execution checkpoint may wait a
# few short rounds before failing closed; telemetry is allowed to give up
# sooner so it never holds up the trading loop.  The sidecar lock serializes
# cooperating writer processes before they enter SQLite's write transaction.
SQLITE_BUSY_TIMEOUT_MS = 500
SQLITE_WRITE_RETRY_DELAYS_SEC = (0.02, 0.05, 0.1, 0.2)
SQLITE_CRITICAL_WRITE_ATTEMPTS = 5
SQLITE_NONCRITICAL_WRITE_ATTEMPTS = 3

CYCLE_COLUMNS = (
    "cycle_id", "updated_ts", "status", "direction",
    "open_signal_ts", "open_first_fill_ts", "hedge_completed_ts",
    "reduce_decision_ts", "reduce_first_fill_ts", "flat_ts", "hold_sec",
    "open_fill_edge_usd", "open_fee_usd", "predicted_close_pnl_usd",
    "close_limit_pnl_usd", "actual_close_fill_edge_usd", "close_fee_usd",
    "total_cycle_pnl_usd", "exit_mode", "exit_reason",
    "first_leg_slippage_bps", "second_leg_slippage_bps",
    "hedge_reserve_breach", "recovery_count", "emergency_flag",
    "partial_first_fill", "notional_usd", "payload_json",
)


def _number(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _is_sqlite_busy(exc: BaseException) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return "database is locked" in message or \
        "database table is locked" in message or \
        "database schema is locked" in message or \
        "database is busy" in message


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {
        "1", "true", "yes", "success", "filled"
    }


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {},
                      ensure_ascii=False, allow_nan=False,
                      separators=(",", ":"))


def _outcome(qty: float, buy_fill: float, sell_fill: float) -> str:
    tolerance = max(1e-8, abs(qty) * 1e-4)
    buy = buy_fill > 0
    sell = sell_fill > 0
    if not buy and not sell:
        return "no_fill"
    if not buy or not sell or abs(buy_fill - sell_fill) > tolerance:
        return "one_leg"
    if buy_fill + tolerance >= qty and sell_fill + tolerance >= qty:
        return "paired"
    return "partial_paired"


LEGACY_COPY_WINDOW_SEC = 2.0
# Completion time is needed to recognize slow executions' CSV mirrors.
# Correlated primary-key lookup avoids changing or deleting historical rows.
TRADE_ATTEMPT_SELECT = """SELECT trade_attempts.*,
    (SELECT final_ts FROM executions
      WHERE executions.execution_id=trade_attempts.execution_id) AS completion_ts
    FROM trade_attempts"""


def _trade_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        return row[key]
    except (IndexError, KeyError, TypeError):
        return default


def _reported_outcome(row: Any) -> str:
    outcome = str(_trade_value(row, "outcome", "") or "").lower()
    buy_fill = float(_trade_value(row, "buy_fill", 0) or 0)
    sell_fill = float(_trade_value(row, "sell_fill", 0) or 0)
    if (outcome not in {"paired", "partial_paired", "one_leg", "no_fill"}
            or outcome == "no_fill" and (buy_fill > 0 or sell_fill > 0)
            or outcome == "paired"):
        return _outcome(float(_trade_value(row, "qty", 0) or 0), buy_fill, sell_fill)
    return outcome


def _canonical_venue(value: Any) -> str:
    venue = str(value or "").strip().lower()
    if "entropy" in venue:
        return "entropy"
    if any(token in venue for token in ("hedge", "lighter", "rh", "xyz")):
        return "hedge"
    return venue


def _close_trade_number(left: Any, right: Any, *, absolute: float,
                        relative: float = 0.0) -> bool:
    left_number = _number(left)
    right_number = _number(right)
    if left_number is None or right_number is None:
        return left_number is None and right_number is None
    return math.isclose(left_number, right_number,
                        rel_tol=relative, abs_tol=absolute)


def _optional_trade_number_matches(left: Any, right: Any, *, absolute: float,
                                   relative: float = 0.0) -> bool:
    if left in (None, "") or right in (None, ""):
        return True
    return _close_trade_number(left, right, absolute=absolute,
                               relative=relative)


def _trade_rows_match_legacy_copy(left: Any, right: Any) -> bool:
    """Return whether a legacy CSV row mirrors a live ledger attempt.

    Legacy CSV rows do not carry the live execution id, and their numeric
    fields are rounded.  The comparison therefore uses the stable execution
    shape plus a short completion-time window, and only crosses the
    ``legacy_csv``/non-legacy source boundary.
    """
    left_source = str(_trade_value(left, "source", "") or "").lower()
    right_source = str(_trade_value(right, "source", "") or "").lower()
    if (left_source == "legacy_csv") == (right_source == "legacy_csv"):
        return False
    legacy, live = ((left, right) if left_source == "legacy_csv"
                    else (right, left))
    legacy_ts = _number(_trade_value(legacy, "ts"))
    live_ts = _number(_trade_value(live, "completion_ts"))
    if live_ts is None:
        live_ts = _number(_trade_value(live, "ts"))
    if (legacy_ts is None or live_ts is None
            or abs(legacy_ts - live_ts) > LEGACY_COPY_WINDOW_SEC):
        return False
    if _reported_outcome(legacy) != _reported_outcome(live):
        return False
    for key in ("action", "direction"):
        if (str(_trade_value(legacy, key, "") or "").lower()
                != str(_trade_value(live, key, "") or "").lower()):
            return False
    if bool(_trade_value(legacy, "reduce_only", False)) != bool(
            _trade_value(live, "reduce_only", False)):
        return False
    if (_canonical_venue(_trade_value(legacy, "buy_venue"))
            != _canonical_venue(_trade_value(live, "buy_venue"))
            or _canonical_venue(_trade_value(legacy, "sell_venue"))
            != _canonical_venue(_trade_value(live, "sell_venue"))):
        return False
    for key in ("qty", "buy_fill", "sell_fill"):
        if not _close_trade_number(_trade_value(legacy, key),
                                   _trade_value(live, key),
                                   absolute=1e-5, relative=1e-4):
            return False
    for key in ("buy_notional", "sell_notional"):
        if not _close_trade_number(_trade_value(legacy, key),
                                   _trade_value(live, key),
                                   absolute=.05, relative=5e-4):
            return False
    for key, absolute in (("threshold_bps", .01),
                          ("net_edge_bps", .02),
                          ("fill_edge_usd", .002)):
        if not _optional_trade_number_matches(
                _trade_value(legacy, key), _trade_value(live, key),
                absolute=absolute, relative=5e-4):
            return False
    return True


def _prefer_trade_row(candidate: Any, current: Any) -> bool:
    """Prefer the live execution row over its legacy CSV mirror."""
    candidate_source = str(_trade_value(candidate, "source", "") or "").lower()
    current_source = str(_trade_value(current, "source", "") or "").lower()
    return candidate_source != "legacy_csv" and current_source == "legacy_csv"


def _deduplicate_trade_rows(rows: list[Any]) -> list[Any]:
    """Collapse legacy/live copies while preserving the live row."""
    unique: list[Any] = []
    for row in rows:
        matches = [
            (index, abs(float(_trade_value(row, "ts", 0.0))
                       - float(_trade_value(existing, "ts", 0.0))))
            for index, existing in enumerate(unique)
            if _trade_rows_match_legacy_copy(row, existing)
        ]
        if not matches:
            unique.append(row)
            continue
        duplicate_index = min(matches, key=lambda item: item[1])[0]
        if _prefer_trade_row(row, unique[duplicate_index]):
            unique[duplicate_index] = row
    return sorted(unique,
                  key=lambda row: (float(_trade_value(row, "ts", 0.0)),
                                   str(_trade_value(row, "trade_key", ""))),
                  reverse=True)


class TradeLedger:
    """Small durable ledger with one connection per engine/API process."""

    def __init__(self, path: str | Path = ":memory:", *,
                 read_only: bool = False) -> None:
        self.path = str(path)
        self.read_only = bool(read_only)
        connection_path: str = self.path
        uri = False
        if self.read_only and self.path != ":memory:":
            path_obj = Path(self.path).resolve()
            if not path_obj.exists():
                raise FileNotFoundError(self.path)
            connection_path = f"{path_obj.as_uri()}?mode=ro"
            uri = True
        elif self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._write_lock_file = None
        if (self.path != ":memory:" and not self.read_only
                and fcntl is not None):
            lock_path = Path(self.path).with_name(
                Path(self.path).name + ".write.lock")
            self._write_lock_file = lock_path.open("a+")
        self._lock = threading.RLock()
        self._write_stats = {
            "sqlite_busy_count": 0,
            "sqlite_busy_critical_count": 0,
            "sqlite_busy_noncritical_count": 0,
            "sqlite_busy_last_ts": None,
        }
        self._conn = sqlite3.connect(
            connection_path, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None, check_same_thread=False, uri=uri)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        self._conn.execute("PRAGMA foreign_keys=ON")
        if self.read_only:
            # A dashboard connection must be incapable of becoming a writer,
            # even if a future read path accidentally issues a mutating query.
            self._conn.execute("PRAGMA query_only=ON")
        elif self.path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        if not self.read_only:
            self._init_schema()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            finally:
                if self._write_lock_file is not None:
                    self._write_lock_file.close()
                    self._write_lock_file = None

    def _note_sqlite_busy(self, *, critical: bool) -> None:
        self._write_stats["sqlite_busy_count"] += 1
        key = ("sqlite_busy_critical_count" if critical
               else "sqlite_busy_noncritical_count")
        self._write_stats[key] += 1
        self._write_stats["sqlite_busy_last_ts"] = time.time()

    def write_stats(self) -> dict:
        """Return bounded write-contention counters for diagnostics."""
        with self._lock:
            return dict(self._write_stats)

    def _acquire_process_write_lock(self, *, critical: bool) -> bool:
        """Acquire the per-database writer lock without waiting forever."""
        handle = self._write_lock_file
        if handle is None or fcntl is None:
            return False
        timeout = 1.0 if critical else 0.25
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                    raise
                if time.monotonic() >= deadline:
                    raise sqlite3.OperationalError("database is locked")
                time.sleep(0.02)

    def _release_process_write_lock(self, held: bool) -> None:
        if held and self._write_lock_file is not None and fcntl is not None:
            fcntl.flock(self._write_lock_file.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def _write(self, *, critical: bool = True):
        with self._lock:
            attempts = (SQLITE_CRITICAL_WRITE_ATTEMPTS if critical
                        else SQLITE_NONCRITICAL_WRITE_ATTEMPTS)
            process_lock_held = False
            for attempt in range(attempts):
                try:
                    process_lock_held = self._acquire_process_write_lock(
                        critical=critical)
                    self._conn.execute("BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError as exc:
                    self._release_process_write_lock(process_lock_held)
                    process_lock_held = False
                    if not _is_sqlite_busy(exc):
                        raise
                    self._note_sqlite_busy(critical=critical)
                    if attempt + 1 >= attempts:
                        raise
                    delay = SQLITE_WRITE_RETRY_DELAYS_SEC[
                        min(attempt, len(SQLITE_WRITE_RETRY_DELAYS_SEC) - 1)]
                    time.sleep(delay)
                except BaseException:
                    self._release_process_write_lock(process_lock_held)
                    raise
            try:
                yield
            except BaseException:
                self._conn.rollback()
                raise
            else:
                for attempt in range(attempts):
                    try:
                        self._conn.commit()
                        break
                    except sqlite3.OperationalError as exc:
                        if not _is_sqlite_busy(exc):
                            raise
                        self._note_sqlite_busy(critical=critical)
                        if attempt + 1 >= attempts:
                            self._conn.rollback()
                            raise
                        delay = SQLITE_WRITE_RETRY_DELAYS_SEC[
                            min(attempt, len(SQLITE_WRITE_RETRY_DELAYS_SEC) - 1)]
                        time.sleep(delay)
            finally:
                self._release_process_write_lock(process_lock_held)

    @contextmanager
    def _noncritical_write(self, operation: str):
        transaction = self._write(critical=False)
        try:
            transaction.__enter__()
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_busy(exc):
                raise
            log.warning("SQLite noncritical write skipped (%s): %s",
                        operation, exc)
            yield False
            return
        try:
            yield True
        except BaseException as exc:
            transaction.__exit__(type(exc), exc, exc.__traceback__)
            if _is_sqlite_busy(exc):
                log.warning("SQLite noncritical write skipped (%s): %s",
                            operation, exc)
                return
            raise
        else:
            try:
                transaction.__exit__(None, None, None)
            except sqlite3.OperationalError as exc:
                if not _is_sqlite_busy(exc):
                    raise
                log.warning("SQLite noncritical write skipped (%s): %s",
                            operation, exc)

    def _init_schema(self) -> None:
        statements = [
            """CREATE TABLE IF NOT EXISTS ledger_meta (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS executions (
                execution_id TEXT PRIMARY KEY,
                created_ts REAL NOT NULL,
                updated_ts REAL NOT NULL,
                action TEXT NOT NULL,
                direction TEXT NOT NULL,
                planned_qty REAL NOT NULL,
                entropy_is_buy INTEGER NOT NULL,
                hedge_is_buy INTEGER NOT NULL,
                reduce_only INTEGER NOT NULL,
                phase TEXT NOT NULL,
                first_filled REAL NOT NULL,
                hedge_filled REAL NOT NULL,
                pending_leg TEXT,
                pending_order_ref TEXT,
                pending_filled REAL NOT NULL,
                retry_count INTEGER NOT NULL,
                last_error TEXT,
                status TEXT NOT NULL,
                outcome TEXT,
                state_json TEXT NOT NULL,
                context_json TEXT NOT NULL,
                final_ts REAL
            )""",
            """CREATE INDEX IF NOT EXISTS executions_active_idx
                ON executions(status, updated_ts DESC)""",
            """CREATE TABLE IF NOT EXISTS execution_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                ts REAL NOT NULL,
                event_type TEXT NOT NULL,
                phase TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS execution_events_idx
                ON execution_events(execution_id, ts)""",
            """CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                leg TEXT NOT NULL,
                venue TEXT NOT NULL,
                order_ref TEXT NOT NULL,
                side TEXT NOT NULL,
                planned_qty REAL NOT NULL,
                limit_px REAL,
                reduce_only INTEGER NOT NULL,
                status TEXT NOT NULL,
                filled_qty REAL NOT NULL,
                avg_px REAL,
                unresolved INTEGER NOT NULL,
                created_ts REAL NOT NULL,
                updated_ts REAL NOT NULL,
                UNIQUE(venue, order_ref)
            )""",
            """CREATE INDEX IF NOT EXISTS orders_execution_idx
                ON orders(execution_id, leg, updated_ts)""",
            """CREATE TABLE IF NOT EXISTS execution_cost_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                phase TEXT NOT NULL,
                ts REAL NOT NULL,
                requested_qty REAL NOT NULL,
                filled_qty REAL NOT NULL,
                is_buy INTEGER,
                quoted_top_px REAL,
                quoted_depth_avg_px REAL,
                quoted_marginal_px REAL,
                limit_px REAL,
                avg_fill_px REAL,
                latency_sec REAL,
                incremental_execution_cost_bps REAL,
                all_in_cost_bps REAL,
                all_in_execution_cost_bps REAL,
                realized_vs_marginal_bps REAL,
                realized_slippage_bps REAL,
                adverse_move_bps REAL,
                latency_adverse_move_bps REAL,
                depth_cost_bps REAL,
                outcome TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS execution_cost_phase_time_idx
                ON execution_cost_observations(phase, ts DESC)""",
            """CREATE TABLE IF NOT EXISTS opportunity_events (
                event_id TEXT PRIMARY KEY,
                opportunity_id TEXT NOT NULL,
                campaign_id TEXT,
                cycle_id TEXT,
                ts REAL NOT NULL,
                direction TEXT,
                stage TEXT NOT NULL,
                reason TEXT,
                payload_json TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS opportunity_events_time_idx
                ON opportunity_events(ts DESC)""",
            """CREATE INDEX IF NOT EXISTS opportunity_events_opportunity_idx
                ON opportunity_events(opportunity_id, ts, stage)""",
            """CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fill_key TEXT NOT NULL UNIQUE,
                execution_id TEXT NOT NULL,
                leg TEXT NOT NULL,
                venue TEXT NOT NULL,
                order_ref TEXT,
                ts REAL NOT NULL,
                side TEXT NOT NULL,
                qty REAL NOT NULL,
                price REAL NOT NULL,
                fee_usd REAL NOT NULL,
                gross_realized_pnl_usd REAL NOT NULL,
                position_before REAL,
                position_after REAL,
                source TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                fee_source TEXT NOT NULL DEFAULT 'estimated_config',
                fee_token TEXT,
                fee_updated_ts REAL
            )""",
            """CREATE TABLE IF NOT EXISTS fill_matches (
                opening_fill_id INTEGER NOT NULL,
                closing_fill_id INTEGER NOT NULL,
                signed_open_qty REAL NOT NULL,
                PRIMARY KEY(opening_fill_id, closing_fill_id)
            )""",
            """CREATE INDEX IF NOT EXISTS fill_matches_close_idx
                ON fill_matches(closing_fill_id)""",
            """CREATE INDEX IF NOT EXISTS fills_time_idx ON fills(ts)""",
            """CREATE INDEX IF NOT EXISTS fills_execution_idx
                ON fills(execution_id, ts)""",
            """CREATE TABLE IF NOT EXISTS exchange_fills (
                exchange_fill_key TEXT PRIMARY KEY,
                venue TEXT NOT NULL,
                exchange_trade_id TEXT,
                exchange_order_ref TEXT,
                ts REAL NOT NULL,
                side TEXT NOT NULL,
                qty REAL NOT NULL,
                price REAL NOT NULL,
                fee_amount REAL,
                fee_usd REAL,
                fee_token TEXT,
                raw_json TEXT NOT NULL,
                matched_fill_id INTEGER,
                first_seen_ts REAL NOT NULL,
                updated_ts REAL NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS exchange_fills_time_idx
                ON exchange_fills(venue, ts)""",
            """CREATE INDEX IF NOT EXISTS exchange_fills_order_idx
                ON exchange_fills(venue, exchange_order_ref, side, ts)""",
            """CREATE TABLE IF NOT EXISTS inventory_lots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venue TEXT NOT NULL,
                qty_remaining REAL NOT NULL,
                price REAL NOT NULL,
                fee_remaining REAL NOT NULL,
                opened_ts REAL NOT NULL,
                source_fill_id INTEGER,
                source TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS lots_venue_idx
                ON inventory_lots(venue, opened_ts, id)""",
            """CREATE TABLE IF NOT EXISTS position_reconciliations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                venue TEXT NOT NULL,
                local_qty REAL NOT NULL,
                exchange_qty REAL NOT NULL,
                delta_qty REAL NOT NULL,
                mark_px REAL,
                status TEXT NOT NULL,
                source TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS reconciliation_time_idx
                ON position_reconciliations(ts)""",
            """CREATE TABLE IF NOT EXISTS funding_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                venue TEXT NOT NULL,
                position_qty REAL NOT NULL,
                mark_px REAL NOT NULL,
                rate_bps_h REAL NOT NULL,
                pnl_usd REAL NOT NULL,
                source TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS funding_venue_time_idx
                ON funding_events(venue,ts)""",
            """CREATE TABLE IF NOT EXISTS pnl_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_second INTEGER NOT NULL UNIQUE,
                ts REAL NOT NULL,
                equity_net_pnl_usd REAL,
                ledger_net_pnl_usd REAL,
                gross_realized_usd REAL,
                unrealized_usd REAL,
                fees_usd REAL,
                funding_pnl_usd REAL,
                unexplained_usd REAL,
                source TEXT NOT NULL,
                equity_total_usd REAL
            )""",
            """CREATE INDEX IF NOT EXISTS pnl_time_idx ON pnl_snapshots(ts)""",
            """CREATE TABLE IF NOT EXISTS trade_attempts (
                trade_key TEXT PRIMARY KEY,
                execution_id TEXT,
                ts REAL NOT NULL,
                source TEXT NOT NULL,
                action TEXT NOT NULL,
                direction TEXT NOT NULL,
                qty REAL NOT NULL,
                buy_notional REAL NOT NULL,
                sell_notional REAL NOT NULL,
                buy_fill REAL NOT NULL,
                sell_fill REAL NOT NULL,
                buy_status TEXT NOT NULL,
                sell_status TEXT NOT NULL,
                buy_venue TEXT NOT NULL,
                sell_venue TEXT NOT NULL,
                threshold_bps REAL,
                net_edge_bps REAL,
                fill_edge_usd REAL,
                outcome TEXT NOT NULL,
                reduce_only INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS trade_attempts_time_idx
                ON trade_attempts(ts DESC)""",
            """CREATE TABLE IF NOT EXISTS cycle_records (
                cycle_id TEXT PRIMARY KEY,
                updated_ts REAL NOT NULL,
                status TEXT NOT NULL,
                direction TEXT,
                open_signal_ts REAL,
                open_first_fill_ts REAL,
                hedge_completed_ts REAL,
                reduce_decision_ts REAL,
                reduce_first_fill_ts REAL,
                flat_ts REAL,
                hold_sec REAL,
                open_fill_edge_usd REAL,
                open_fee_usd REAL,
                predicted_close_pnl_usd REAL,
                close_limit_pnl_usd REAL,
                actual_close_fill_edge_usd REAL,
                close_fee_usd REAL,
                total_cycle_pnl_usd REAL,
                exit_mode TEXT,
                exit_reason TEXT,
                first_leg_slippage_bps REAL,
                second_leg_slippage_bps REAL,
                hedge_reserve_breach INTEGER NOT NULL DEFAULT 0,
                recovery_count INTEGER NOT NULL DEFAULT 0,
                emergency_flag INTEGER NOT NULL DEFAULT 0,
                partial_first_fill INTEGER NOT NULL DEFAULT 0,
                notional_usd REAL,
                payload_json TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS cycle_records_time_idx
                ON cycle_records(open_first_fill_ts DESC)""",
            """CREATE TABLE IF NOT EXISTS market_samples (
                source_key TEXT PRIMARY KEY,
                ts REAL NOT NULL,
                payload_json TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS market_samples_time_idx
                ON market_samples(ts)""",
            """CREATE TABLE IF NOT EXISTS model_state (
                model_key TEXT PRIMARY KEY,
                updated_ts REAL NOT NULL,
                state_json TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS minute_samples (
                source_key TEXT PRIMARY KEY,
                ts REAL NOT NULL,
                payload_json TEXT NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS minute_samples_time_idx
                ON minute_samples(ts)""",
            """CREATE TABLE IF NOT EXISTS legacy_imports (
                source_key TEXT PRIMARY KEY,
                source_file TEXT NOT NULL,
                source_row INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                imported_ts REAL NOT NULL
            )""",
        ]
        with self._write():
            for statement in statements:
                self._conn.execute(statement)
            columns = {
                row[1] for row in self._conn.execute(
                    "PRAGMA table_info(pnl_snapshots)").fetchall()
            }
            if "equity_total_usd" not in columns:
                self._conn.execute(
                    "ALTER TABLE pnl_snapshots ADD COLUMN equity_total_usd REAL")
            fill_columns = {
                row[1] for row in self._conn.execute(
                    "PRAGMA table_info(fills)").fetchall()
            }
            if "fee_source" not in fill_columns:
                self._conn.execute(
                    "ALTER TABLE fills ADD COLUMN fee_source TEXT NOT NULL "
                    "DEFAULT 'estimated_config'")
            if "fee_token" not in fill_columns:
                self._conn.execute(
                    "ALTER TABLE fills ADD COLUMN fee_token TEXT")
            if "fee_updated_ts" not in fill_columns:
                self._conn.execute(
                    "ALTER TABLE fills ADD COLUMN fee_updated_ts REAL")
            cost_columns = {
                row[1] for row in self._conn.execute(
                    "PRAGMA table_info(execution_cost_observations)").fetchall()
            }
            if "all_in_cost_bps" not in cost_columns:
                self._conn.execute(
                    "ALTER TABLE execution_cost_observations "
                    "ADD COLUMN all_in_cost_bps REAL")
            if "realized_vs_marginal_bps" not in cost_columns:
                self._conn.execute(
                    "ALTER TABLE execution_cost_observations "
                    "ADD COLUMN realized_vs_marginal_bps REAL")
            if "quoted_depth_avg_px" not in cost_columns:
                self._conn.execute(
                    "ALTER TABLE execution_cost_observations "
                    "ADD COLUMN quoted_depth_avg_px REAL")
            if "incremental_execution_cost_bps" not in cost_columns:
                self._conn.execute(
                    "ALTER TABLE execution_cost_observations "
                    "ADD COLUMN incremental_execution_cost_bps REAL")
            if "is_buy" not in cost_columns:
                self._conn.execute(
                    "ALTER TABLE execution_cost_observations "
                    "ADD COLUMN is_buy INTEGER")
            if "all_in_execution_cost_bps" not in cost_columns:
                self._conn.execute(
                    "ALTER TABLE execution_cost_observations "
                    "ADD COLUMN all_in_execution_cost_bps REAL")
            if "latency_adverse_move_bps" not in cost_columns:
                self._conn.execute(
                    "ALTER TABLE execution_cost_observations "
                    "ADD COLUMN latency_adverse_move_bps REAL")
            self._conn.execute(
                "UPDATE fills SET fee_source='not_applicable' "
                "WHERE source='position_baseline' AND fee_source="
                "'estimated_config'")
            self._conn.execute(
                "INSERT INTO ledger_meta(key,value) VALUES('schema_version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),))

    # ---------------------------------------------------------- checkpoints

    def checkpoint_execution(self, state: ExecutionState,
                             context: Optional[dict] = None,
                             event_type: str = "checkpoint",
                             payload: Optional[dict] = None) -> None:
        context = context or {}
        state.touch()
        data = state.to_dict()
        now = state.updated_ts
        status = "complete" if state.terminal else "active"
        with self._write():
            self._conn.execute(
                """INSERT INTO executions(
                    execution_id,created_ts,updated_ts,action,direction,
                    planned_qty,entropy_is_buy,hedge_is_buy,reduce_only,phase,
                    first_filled,hedge_filled,pending_leg,pending_order_ref,
                    pending_filled,retry_count,last_error,status,outcome,
                    state_json,context_json,final_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(execution_id) DO UPDATE SET
                    updated_ts=excluded.updated_ts, action=excluded.action,
                    direction=excluded.direction, planned_qty=excluded.planned_qty,
                    entropy_is_buy=excluded.entropy_is_buy,
                    hedge_is_buy=excluded.hedge_is_buy,
                    reduce_only=excluded.reduce_only, phase=excluded.phase,
                    first_filled=excluded.first_filled,
                    hedge_filled=excluded.hedge_filled,
                    pending_leg=excluded.pending_leg,
                    pending_order_ref=excluded.pending_order_ref,
                    pending_filled=excluded.pending_filled,
                    retry_count=excluded.retry_count,
                    last_error=excluded.last_error, status=excluded.status,
                    state_json=excluded.state_json,
                    context_json=CASE WHEN excluded.context_json='{}'
                                      THEN executions.context_json
                                      ELSE excluded.context_json END,
                    final_ts=excluded.final_ts""",
                (state.cycle_id, state.created_ts, now, state.action,
                 state.direction, state.planned_qty, int(state.entropy_is_buy),
                 int(state.hedge_is_buy), int(state.reduce_only), state.phase.value,
                 state.first_filled, state.hedge_filled, state.pending_leg,
                 state.pending_order_ref, state.pending_filled, state.retry_count,
                 state.last_error, status, None if not state.terminal else now,
                 _json(data), _json(context),
                 now if state.terminal else None))
            self._conn.execute(
                """INSERT INTO execution_events(
                    execution_id,ts,event_type,phase,payload_json)
                   VALUES(?,?,?,?,?)""",
                (state.cycle_id, now, event_type, state.phase.value,
                 _json(payload or data)))

    def complete_execution(self, state: ExecutionState, outcome: str,
                           context: Optional[dict] = None) -> None:
        state.transition(ExecutionPhase.VERIFIED, f"complete:{outcome}")
        self.checkpoint_execution(state, context=context,
                                  event_type="completed",
                                  payload={"outcome": outcome})
        with self._write():
            self._conn.execute(
                "UPDATE executions SET status='complete', outcome=?, "
                "final_ts=?, phase='verified' WHERE execution_id=?",
                (outcome, state.updated_ts, state.cycle_id))

    def load_active_execution(self) -> Optional[tuple[ExecutionState, dict]]:
        with self._lock:
            row = self._conn.execute(
                """SELECT state_json,context_json FROM executions
                   WHERE status='active' ORDER BY updated_ts DESC LIMIT 1"""
            ).fetchone()
        if row is None:
            return None
        try:
            return ExecutionState.from_dict(json.loads(row["state_json"])), \
                json.loads(row["context_json"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError, KeyError) as exc:
            log.error("invalid active execution checkpoint: %r", exc)
            return None

    # --------------------------------------------------------------- orders

    def order_planned_qty(self, *, venue: str,
                          order_ref: Optional[str]) -> Optional[float]:
        """Return the durable quantity cap for one client order."""
        if order_ref in (None, ""):
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT planned_qty FROM orders WHERE venue=? AND order_ref=?",
                (str(venue), str(order_ref))).fetchone()
        if row is None:
            return None
        planned = _number(row["planned_qty"])
        return max(planned, 0.0) if planned is not None else None

    def order_filled_qty(self, *, venue: str,
                         order_ref: Optional[str]) -> Optional[float]:
        """Return the durable cumulative quantity already applied."""
        if order_ref in (None, ""):
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT filled_qty,planned_qty FROM orders "
                "WHERE venue=? AND order_ref=?",
                (str(venue), str(order_ref))).fetchone()
        if row is None:
            return None
        filled = max(_number(row["filled_qty"]) or 0.0, 0.0)
        planned = max(_number(row["planned_qty"]) or 0.0, 0.0)
        return min(filled, planned) if planned > 0 else filled

    def _repair_order_overfill_locked(self, *, venue: str, order_ref: str,
                                      planned_qty: float, ts: float) -> dict:
        """Remove safe duplicate cumulative rows from one order.

        A cumulative status response is not a new fill.  Only rows created by
        status/recovery paths and carrying a cumulative marker are eligible;
        exchange-fill-history rows are immutable facts and are never trimmed.
        A row is repaired only when its residual inventory lot is still fully
        identifiable.  If a duplicate already participated in a later fill,
        the method leaves it for the normal reconciliation/orphan path rather
        than manufacturing a historical cost basis.
        """
        planned_qty = max(float(planned_qty), 0.0)
        if planned_qty <= 0:
            return {"repaired_rows": 0, "repaired_qty": 0.0,
                    "baseline_qty": 0.0}
        rows = self._conn.execute(
            """SELECT f.* FROM fills f
                WHERE f.venue=? AND f.order_ref=?
                ORDER BY f.ts,f.id""",
            (str(venue), str(order_ref))).fetchall()
        total_qty = sum(max(float(row["qty"] or 0.0), 0.0) for row in rows
                        if row["source"] != "duplicate_cumulative_ignored")
        tolerance = max(1e-8, planned_qty * 1e-4)
        excess = total_qty - planned_qty
        if excess <= tolerance:
            return {"repaired_rows": 0, "repaired_qty": 0.0,
                    "baseline_qty": 0.0}

        repaired_rows = 0
        repaired_qty = 0.0
        baseline_qty = 0.0
        eligible_sources = {"order_status", "fill_recovery"}
        for row in reversed(rows):
            if excess <= tolerance:
                break
            if row["source"] not in eligible_sources:
                continue
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                metadata = {}
            if not isinstance(metadata, dict) \
                    or metadata.get("cumulative_filled") is None:
                continue
            qty = max(float(row["qty"] or 0.0), 0.0)
            if qty <= 0 or qty > excess + tolerance:
                continue
            lots = self._conn.execute(
                "SELECT id,qty_remaining FROM inventory_lots "
                "WHERE source_fill_id=?", (int(row["id"]),)).fetchall()
            residual_signed = sum(float(lot["qty_remaining"] or 0.0)
                                  for lot in lots)
            side_sign = 1.0 if str(row["side"]).lower() == "buy" else -1.0
            signed_qty = side_sign * qty
            consumed_signed = signed_qty - residual_signed
            if abs(residual_signed) + tolerance < qty:
                # The duplicate closed some older inventory.  It is safe to
                # restore only the consumed amount as explicitly unknown
                # baseline inventory; its original cost is no longer present.
                if abs(consumed_signed) > tolerance:
                    self._conn.execute(
                        """INSERT INTO inventory_lots(
                            venue,qty_remaining,price,fee_remaining,opened_ts,
                            source_fill_id,source)
                           VALUES(?,?,?,?,?,?,?)""",
                        (str(venue), -consumed_signed,
                         float(row["price"]), 0.0, float(ts), None,
                         "position_baseline"))
                    baseline_qty += abs(consumed_signed)
            self._conn.execute(
                "DELETE FROM inventory_lots WHERE source_fill_id=?",
                (int(row["id"]),))
            metadata.update({
                "duplicate_cumulative_ignored": True,
                "ignored_qty": qty,
                "repair_reason": "cumulative_overfill_above_order_plan",
                "repaired_at": float(ts),
            })
            self._conn.execute(
                """UPDATE fills SET qty=0.0,fee_usd=0.0,
                          gross_realized_pnl_usd=0.0,source=?,metadata_json=?
                       WHERE id=?""",
                ("duplicate_cumulative_ignored", _json(metadata),
                 int(row["id"])))
            self._conn.execute(
                """INSERT INTO execution_events(
                       execution_id,ts,event_type,phase,payload_json)
                   VALUES(?,?,?,?,?)""",
                (str(row["execution_id"]), float(ts),
                 "cumulative_overfill_repaired", "reconciling",
                 _json({"venue": str(venue), "order_ref": str(order_ref),
                        "fill_id": int(row["id"]), "ignored_qty": qty,
                        "planned_qty": planned_qty,
                        "reported_cumulative_filled": metadata.get(
                            "cumulative_filled")})))
            excess -= qty
            repaired_rows += 1
            repaired_qty += qty
        if repaired_rows:
            self._conn.execute(
                "UPDATE orders SET filled_qty=MIN(filled_qty,planned_qty), "
                "updated_ts=? WHERE venue=? AND order_ref=?",
                (float(ts), str(venue), str(order_ref)))
        return {"repaired_rows": repaired_rows,
                "repaired_qty": repaired_qty,
                "baseline_qty": baseline_qty,
                "remaining_excess_qty": max(excess, 0.0)}

    def repair_cumulative_overfills(self) -> dict:
        """Repair safe historical cumulative-overfill artifacts once at boot.

        The operation is idempotent and preserves the original response in
        ``fills`` as an explicitly ignored audit row.  It is intentionally
        conservative for rows whose duplicate quantity has already been
        consumed by a later fill.
        """
        result = {"orders_checked": 0, "repaired_rows": 0,
                  "repaired_qty": 0.0, "baseline_qty": 0.0,
                  "remaining_excess_qty": 0.0}
        now = time.time()
        with self._write():
            rows = self._conn.execute(
                """SELECT o.venue,o.order_ref,o.planned_qty
                     FROM orders o JOIN fills f
                       ON f.venue=o.venue AND f.order_ref=o.order_ref
                    WHERE o.order_ref IS NOT NULL AND o.planned_qty>0
                      AND f.source!='duplicate_cumulative_ignored'
                    GROUP BY o.venue,o.order_ref,o.planned_qty
                   HAVING SUM(ABS(f.qty)) >
                          MAX(1e-8,o.planned_qty * 1e-4) + o.planned_qty"""
            ).fetchall()
            for row in rows:
                result["orders_checked"] += 1
                repaired = self._repair_order_overfill_locked(
                    venue=str(row["venue"]), order_ref=str(row["order_ref"]),
                    planned_qty=float(row["planned_qty"]), ts=now)
                for key in ("repaired_rows", "repaired_qty", "baseline_qty",
                            "remaining_excess_qty"):
                    result[key] += repaired.get(key, 0)
        return result

    def record_order(self, *, execution_id: str, leg: str, venue: str,
                     order_ref: Optional[str], side: str, planned_qty: float,
                     limit_px: Optional[float], reduce_only: bool,
                     status: str, filled_qty: float,
                     avg_px: Optional[float], unresolved: bool,
                     ts: Optional[float] = None) -> None:
        if order_ref in (None, ""):
            return
        now = time.time() if ts is None else ts
        planned_qty = max(float(planned_qty), 0.0)
        filled_qty = min(max(float(filled_qty), 0.0), planned_qty)
        with self._write():
            self._conn.execute(
                """INSERT INTO orders(
                    execution_id,leg,venue,order_ref,side,planned_qty,limit_px,
                    reduce_only,status,filled_qty,avg_px,unresolved,created_ts,
                    updated_ts)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(venue,order_ref) DO UPDATE SET
                    execution_id=excluded.execution_id, leg=excluded.leg,
                    side=excluded.side, planned_qty=excluded.planned_qty,
                    limit_px=excluded.limit_px, reduce_only=excluded.reduce_only,
                    status=CASE WHEN orders.filled_qty>excluded.filled_qty
                                THEN orders.status ELSE excluded.status END,
                    filled_qty=MIN(MAX(orders.filled_qty,excluded.filled_qty),
                                   excluded.planned_qty),
                    avg_px=COALESCE(excluded.avg_px,orders.avg_px),
                    unresolved=CASE
                        WHEN excluded.unresolved=0 THEN 0
                        WHEN orders.filled_qty>excluded.filled_qty
                            THEN orders.unresolved
                        ELSE 1 END,
                    updated_ts=excluded.updated_ts""",
                (execution_id, leg, venue, str(order_ref), side, planned_qty,
                 limit_px, int(reduce_only), status, filled_qty, avg_px,
                 int(unresolved), now, now))

    def settle_reconciled_order(self, *, venue: str, order_ref: str,
                                filled_qty: float,
                                avg_px: Optional[float] = None,
                                status: str = "reconciled_position",
                                ts: Optional[float] = None) -> bool:
        """Clear an order's unresolved flag after account-position proof."""
        if order_ref in (None, ""):
            return False
        now = time.time() if ts is None else ts
        with self._write():
            order = self._conn.execute(
                "SELECT planned_qty FROM orders WHERE venue=? AND order_ref=?",
                (str(venue), str(order_ref))).fetchone()
            settled_qty = max(float(filled_qty), 0.0)
            if order is not None:
                settled_qty = min(settled_qty,
                                  max(float(order["planned_qty"] or 0.0), 0.0))
            cur = self._conn.execute(
                """UPDATE orders SET status=?, filled_qty=?,
                          avg_px=COALESCE(?, avg_px), unresolved=0,
                          updated_ts=? WHERE venue=? AND order_ref=?""",
                (status, settled_qty, avg_px, now,
                 str(venue), str(order_ref)))
            return cur.rowcount > 0

    # --------------------------------------------------------------- fills

    def record_fill(self, *, fill_key: str, execution_id: str, leg: str,
                    venue: str, order_ref: Optional[str], ts: float,
                    side: str, qty: float, price: float, fee_usd: float = 0.0,
                    position_before: Optional[float] = None,
                    position_after: Optional[float] = None,
                    source: str = "order_confirmation",
                    fee_source: str = "estimated_config",
                    fee_token: Optional[str] = None,
                    metadata: Optional[dict] = None) -> bool:
        qty = abs(float(qty))
        price = float(price)
        if not fill_key or qty <= 0 or not math.isfinite(price) or price <= 0:
            return False
        side = "buy" if str(side).lower() == "buy" else "sell"
        signed = qty if side == "buy" else -qty
        fee_usd = _number(fee_usd)
        fee_usd = 0.0 if fee_usd is None else fee_usd
        fee_source = str(fee_source or "estimated_config")
        if source == "position_baseline":
            fee_source = "not_applicable"
            fee_token = None
        with self._write():
            return self._record_fill_locked(
                fill_key=fill_key, execution_id=execution_id, leg=leg,
                venue=venue, order_ref=order_ref, ts=float(ts), side=side,
                qty=qty, price=price, fee_usd=fee_usd,
                position_before=position_before, position_after=position_after,
                source=source, fee_source=fee_source, fee_token=fee_token,
                metadata=metadata)

    def _record_fill_locked(self, *, fill_key: str, execution_id: str,
                            leg: str, venue: str, order_ref: Optional[str],
                            ts: float, side: str, qty: float, price: float,
                            fee_usd: float = 0.0,
                            position_before: Optional[float] = None,
                            position_after: Optional[float] = None,
                            source: str = "order_confirmation",
                            fee_source: str = "estimated_config",
                            fee_token: Optional[str] = None,
                            metadata: Optional[dict] = None) -> bool:
        """Insert a validated fill while the write transaction is held."""
        exists = self._conn.execute(
            "SELECT 1 FROM fills WHERE fill_key=?", (fill_key,)).fetchone()
        if exists is not None:
            return False
        signed = qty if side == "buy" else -qty
        gross, new_lots, matches = self._apply_inventory_fill(
            venue, signed, price, fee_usd, float(ts), source=source)
        cur = self._conn.execute(
            """INSERT INTO fills(
                fill_key,execution_id,leg,venue,order_ref,ts,side,qty,
                price,fee_usd,gross_realized_pnl_usd,position_before,
                position_after,source,metadata_json,fee_source,fee_token,
                fee_updated_ts)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (fill_key, execution_id, leg, venue, order_ref, ts, side, qty,
             price, fee_usd, gross, position_before, position_after, source,
             _json(metadata or {}), fee_source, fee_token,
             time.time() if fee_source == "exchange_api" else None))
        fill_id = cur.lastrowid
        self._conn.executemany(
            "INSERT INTO fill_matches VALUES(?,?,?)",
            [(opening_id, fill_id, matched_qty)
             for opening_id, matched_qty in matches])
        for lot_qty, lot_fee in new_lots:
            self._conn.execute(
                """INSERT INTO inventory_lots(
                    venue,qty_remaining,price,fee_remaining,opened_ts,
                    source_fill_id,source)
                VALUES(?,?,?,?,?,?,?)""",
                (venue, lot_qty, price, lot_fee, ts, fill_id, source))
        return True

    def execution_orders(self, execution_id: str) -> list[dict]:
        """Return persisted order intents for one unfinished execution."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM orders WHERE execution_id=? "
                "ORDER BY created_ts,id", (str(execution_id),)).fetchall()
        return [dict(row) for row in rows]

    def inventory_status(self, venue: str) -> dict:
        """Return the ledger position and whether its cost basis is trusted."""
        venue = str(venue)
        with self._lock:
            rows = self._conn.execute(
                """SELECT l.qty_remaining,l.source,l.source_fill_id,
                          f.metadata_json,f.source AS fill_source
                     FROM inventory_lots l
                LEFT JOIN fills f ON f.id=l.source_fill_id
                    WHERE l.venue=? ORDER BY l.opened_ts,l.id""",
                (venue,)).fetchall()
        qty = sum(float(row["qty_remaining"]) for row in rows)
        known = True
        for row in rows:
            if row["source"] in {"position_baseline", "restart_reconciliation"}:
                known = False
                break
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                metadata = {}
            if not isinstance(metadata, dict):
                metadata = {}
            if metadata.get("price_source") in {
                    "limit_fallback", "position_mark", "mark_price"}:
                known = False
                break
        return {"venue": venue, "qty": qty, "known": known,
                "lot_count": len(rows)}

    def execution_cost_ready(self, execution_id: str) -> bool:
        """Whether every fill recorded for an execution has exact cost data."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT f.*,o.avg_px AS order_avg_px
                     FROM fills f
                LEFT JOIN orders o ON o.venue=f.venue
                                  AND o.order_ref=f.order_ref
                    WHERE f.execution_id=? ORDER BY f.ts,f.id""",
                (str(execution_id),)).fetchall()
        if not rows:
            return False
        for row in rows:
            if row["source"] == "duplicate_cumulative_ignored" \
                    or float(row["qty"] or 0.0) <= 0:
                continue
            if row["source"] in {"position_baseline", "restart_reconciliation"}:
                return False
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                metadata = {}
            if not isinstance(metadata, dict):
                metadata = {}
            price_source = metadata.get("price_source")
            if price_source in {"limit_fallback", "position_mark", "mark_price"}:
                return False
            if (metadata.get("cumulative_filled") is not None
                    and not price_source and row["order_avg_px"] is None):
                return False
            if not _number(row["price"]) or float(row["price"]) <= 0:
                return False
        return True

    def execution_authoritative_fill_quantities(self,
                                                execution_id: str) -> dict[str, float]:
        """Return trusted filled quantity aggregated by execution leg.

        Position observations and limit-price fallback rows establish neither
        an authoritative fill quantity nor a cost basis.  Cumulative rows are
        trusted only when their order row supplies an exchange average.
        ``record_cumulative_fill`` stores only the incremental quantity.  The
        per-order cap below is still applied because older rows may contain a
        duplicated cumulative snapshot from the confirmation/status race.
        """
        totals = {"first": 0.0, "hedge": 0.0}
        capped_order_totals: dict[tuple[str, str, str], float] = {}
        with self._lock:
            rows = self._conn.execute(
                """SELECT f.leg,f.venue,f.qty,f.price,f.source,f.metadata_json,
                          f.order_ref,o.avg_px AS order_avg_px,
                          o.planned_qty AS order_planned_qty
                     FROM fills f
                LEFT JOIN orders o ON o.venue=f.venue
                                  AND o.order_ref=f.order_ref
                    WHERE f.execution_id=? ORDER BY f.ts,f.id""",
                (str(execution_id),)).fetchall()
        for row in rows:
            leg = str(row["leg"] or "")
            if leg not in totals:
                continue
            if row["source"] in {"position_baseline", "restart_reconciliation"}:
                continue
            if row["source"] == "duplicate_cumulative_ignored":
                continue
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                metadata = {}
            if not isinstance(metadata, dict):
                metadata = {}
            price_source = metadata.get("price_source")
            if price_source in {"limit_fallback", "position_mark", "mark_price"}:
                continue
            if (metadata.get("cumulative_filled") is not None
                    and not price_source and row["order_avg_px"] is None):
                continue
            quantity = _number(row["qty"])
            price = _number(row["price"])
            if quantity is None or price is None or quantity <= 0 or price <= 0:
                continue
            order_ref = str(row["order_ref"] or "")
            planned = _number(row["order_planned_qty"])
            if order_ref and planned is not None and planned > 0:
                # The source is not part of the order identity.  Keep the
                # cap keyed by venue/order/leg instead so mixed response
                # sources cannot bypass it.
                key = (leg, str(row["venue"] or ""), order_ref)
                remaining = max(planned - capped_order_totals.get(key, 0.0),
                                0.0)
                accepted = min(quantity, remaining)
                capped_order_totals[key] = (
                    capped_order_totals.get(key, 0.0) + accepted)
                totals[leg] += accepted
            else:
                totals[leg] += quantity
        return totals

    def record_cumulative_fill(
            self, *, execution_id: str, leg: str, venue: str,
            order_ref: Optional[str], side: str, observed_qty: float,
            avg_px: float, fee_usd: Optional[float] = None,
            fee_source: str = "estimated_config",
            fee_token: Optional[str] = None,
            ts: Optional[float] = None,
            position_before: Optional[float] = None,
            position_after: Optional[float] = None,
            source: str = "fill_recovery",
            metadata: Optional[dict] = None) -> dict:
        """Record an exchange cumulative fill without double-counting.

        Order APIs report cumulative quantity and VWAP.  This method compares
        that cumulative fact with local fills for the same client reference,
        inserts only the missing quantity at the implied incremental price,
        and repairs older limit-price fallback rows when an authoritative VWAP
        becomes available.  A position observation alone never reaches here.
        """
        try:
            reported_qty = abs(float(observed_qty))
            target_qty = reported_qty
            average = float(avg_px)
        except (TypeError, ValueError):
            return {"known": False, "reason": "invalid_cumulative_fill",
                    "observed_qty": 0.0, "reported_qty": 0.0,
                    "recovered_qty": 0.0}
        if (target_qty <= 0 or not math.isfinite(target_qty)
                or average <= 0 or not math.isfinite(average)):
            return {"known": False, "reason": "invalid_cumulative_fill",
                    "observed_qty": target_qty, "reported_qty": target_qty,
                    "recovered_qty": 0.0}
        side = "buy" if str(side).lower() == "buy" else "sell"
        order_ref = None if order_ref in (None, "") else str(order_ref)
        now = time.time() if ts is None else float(ts)
        total_fee = _number(fee_usd)
        total_fee = 0.0 if total_fee is None else max(total_fee, 0.0)
        tolerance = max(1e-8, target_qty * 1e-4)
        with self._write():
            planned_limit = None
            if order_ref is not None:
                order = self._conn.execute(
                    "SELECT planned_qty FROM orders WHERE venue=? "
                    "AND order_ref=?", (str(venue), order_ref)).fetchone()
                if order is not None:
                    planned_limit = max(float(order["planned_qty"] or 0.0),
                                        0.0)
                    if planned_limit > 0:
                        repair = self._repair_order_overfill_locked(
                            venue=str(venue), order_ref=order_ref,
                            planned_qty=planned_limit, ts=now)
                        if repair.get("repaired_rows"):
                            tolerance = max(1e-8, target_qty * 1e-4)
                        target_qty = min(target_qty, planned_limit)
            if order_ref is None:
                rows = self._conn.execute(
                    """SELECT * FROM fills
                         WHERE execution_id=? AND leg=? AND venue=? AND side=?
                         ORDER BY ts,id""",
                    (str(execution_id), str(leg), str(venue), side)).fetchall()
            else:
                rows = self._conn.execute(
                    """SELECT * FROM fills
                         WHERE venue=? AND order_ref=? AND side=?
                         ORDER BY ts,id""",
                    (str(venue), order_ref, side)).fetchall()

            # Older engine versions could persist the order limit as a fill
            # price after a response timeout.  Replace only those rows, while
            # preserving any exact fills and their FIFO quantity.
            fallback_rows = []
            exact_qty = 0.0
            exact_cost = 0.0
            fee_existing = 0.0
            for row in rows:
                try:
                    item_meta = json.loads(row["metadata_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    item_meta = {}
                if not isinstance(item_meta, dict):
                    item_meta = {}
                if item_meta.get("price_source") in {
                        "limit_fallback", "position_mark", "mark_price"}:
                    fallback_rows.append((row, item_meta))
                else:
                    exact_qty += abs(float(row["qty"]))
                    exact_cost += abs(float(row["qty"])) * float(row["price"])
                fee_existing += max(float(row["fee_usd"] or 0.0), 0.0)

            fallback_qty = sum(abs(float(row["qty"])) for row, _ in fallback_rows)
            prior_cost = exact_cost + sum(
                abs(float(row["qty"])) * float(row["price"])
                for row, _ in fallback_rows)
            prior_fee = fee_existing
            existing_qty = exact_qty + fallback_qty
            repriced_executions = {str(execution_id)}
            if fallback_rows and existing_qty <= target_qty + tolerance:
                replacement_cost = target_qty * average - exact_cost
                replacement_px = (replacement_cost / (target_qty - exact_qty)
                                  if target_qty > exact_qty else average)
                if not math.isfinite(replacement_px) or replacement_px <= 0:
                    replacement_px = average
                for row, item_meta in fallback_rows:
                    repriced_executions.update(self._reprice_fill_matches(row, replacement_px))
                    item_meta.update({"cumulative_filled": target_qty,
                                      "price_source": "exchange_average",
                                      "recovered_at": now})
                    self._conn.execute(
                        "UPDATE fills SET price=?,metadata_json=?,source=? "
                        "WHERE id=?",
                        (replacement_px, _json(item_meta), source, int(row["id"])))
                    self._conn.execute(
                        "UPDATE inventory_lots SET price=? WHERE source_fill_id=?",
                        (replacement_px, int(row["id"])))
                exact_cost += fallback_qty * replacement_px

            existing_qty = exact_qty + fallback_qty
            recovered_qty = 0.0
            if existing_qty < target_qty - tolerance:
                delta = target_qty - existing_qty
                incremental_px = ((target_qty * average - exact_cost) / delta
                                  if delta > 0 else average)
                if not math.isfinite(incremental_px) or incremental_px <= 0:
                    incremental_px = average
                incremental_fee = max(total_fee - fee_existing, 0.0)
                fill_key = (f"recovered:{execution_id}:{leg}:{venue}:"
                            f"{order_ref or 'no-ref'}:{target_qty:.12g}")
                row_meta = dict(metadata or {})
                row_meta.update({"cumulative_filled": target_qty,
                                 "price_source": "exchange_average",
                                 "recovered_at": now})
                inserted = self._record_fill_locked(
                    fill_key=fill_key, execution_id=str(execution_id),
                    leg=str(leg), venue=str(venue), order_ref=order_ref,
                    ts=now, side=side, qty=delta, price=incremental_px,
                    fee_usd=incremental_fee, position_before=position_before,
                    position_after=position_after, source=source,
                    fee_source=str(fee_source or "estimated_config"),
                    fee_token=fee_token, metadata=row_meta)
                if inserted:
                    recovered_qty = delta

            cost_changed = bool(fallback_rows) or (
                existing_qty < target_qty - tolerance)
            notional_delta = (target_qty * average - prior_cost
                              if cost_changed else 0.0)
            fee_delta = (total_fee - prior_fee if cost_changed else 0.0)

            if order_ref is not None:
                self._conn.execute(
                    """UPDATE orders SET filled_qty=MAX(filled_qty,?),
                              avg_px=?, unresolved=0, status=?, updated_ts=?
                           WHERE venue=? AND order_ref=?""",
                    (min(target_qty, planned_limit)
                     if planned_limit is not None and planned_limit > 0
                     else target_qty,
                     average, "recovered", now, str(venue),
                     order_ref))
            if cost_changed:
                self._refresh_completed_cycle_financials(repriced_executions)
            final_qty = max(existing_qty, target_qty)
            if planned_limit is not None and planned_limit > 0:
                final_qty = min(final_qty, planned_limit)
            return {
                "known": True,
                "reason": "exchange_average",
                "observed_qty": target_qty,
                "reported_qty": reported_qty,
                "capped_to_order_plan": bool(
                    planned_limit is not None and reported_qty > target_qty),
                "recorded_qty": final_qty,
                "recovered_qty": recovered_qty,
                "avg_px": average,
                "fee_usd": total_fee,
                "fee_source": str(fee_source or "estimated_config"),
                "order_ref": order_ref,
                "notional_delta_usd": notional_delta,
                "fee_delta_usd": fee_delta,
            }

    def recover_order_fills(
            self, *, execution_id: str, leg: str, venue: str,
            order_ref: Optional[str], side: str,
            exchange_rows: Iterable[dict] = (),
            observed_qty: Optional[float] = None,
            avg_px: Optional[float] = None,
            fee_usd: Optional[float] = None,
            fee_source: str = "estimated_config",
            fee_token: Optional[str] = None,
            submit_ts: Optional[float] = None,
            ts: Optional[float] = None,
            position_before: Optional[float] = None,
            position_after: Optional[float] = None) -> dict:
        """Recover one order from exchange fill history plus order status."""
        order_ref = None if order_ref in (None, "") else str(order_ref)
        side = "buy" if str(side).lower() == "buy" else "sell"
        normalised = []
        for index, row in enumerate(exchange_rows or ()):
            item = _normalise_exchange_fill(venue, row, index)
            if item is None:
                continue
            matches_ref = (order_ref is not None
                           and item["order_ref"] == order_ref)
            matches_window = (order_ref is None or item["order_ref"] is None) \
                and submit_ts is not None \
                and abs(item["ts"] - float(submit_ts)) <= 180.0
            if item["side"] == side and (matches_ref or matches_window):
                normalised.append(item)

        # Persist the raw exchange facts before deriving the cumulative row.
        if normalised:
            self.reconcile_exchange_fills(venue, [
                {"exchange_fill_key": item["key"],
                 "exchange_trade_id": item["trade_id"],
                 "order_ref": item["order_ref"], "ts": item["ts"],
                 "side": item["side"], "qty": item["qty"],
                 "price": item["price"], "fee_amount": item["fee_amount"],
                 "fee_usd": item["fee_usd"], "fee_token": item["fee_token"],
                 "raw": json.loads(item["raw_json"])}
                for item in normalised])

        history_qty = sum(item["qty"] for item in normalised)
        history_cost = sum(item["qty"] * item["price"] for item in normalised)
        history_fees = [item["fee_usd"] for item in normalised]
        history_fee_known = bool(history_fees) and all(
            value is not None for value in history_fees)
        target_qty = max(history_qty, abs(float(observed_qty or 0.0)))
        average = ((history_cost / history_qty) if history_qty > 0
                   else _number(avg_px))
        if average is None or average <= 0 or target_qty <= 0:
            return {"known": False, "reason": "no_exchange_fill",
                    "observed_qty": max(target_qty, 0.0),
                    "order_ref": order_ref, "history_count": len(normalised)}
        total_fee = (sum(float(value) for value in history_fees)
                     if history_fee_known else _number(fee_usd))
        if total_fee is None:
            total_fee = 0.0
        source = "exchange_fill_history" if history_qty > 0 \
            else "order_status"
        result = self.record_cumulative_fill(
            execution_id=execution_id, leg=leg, venue=venue,
            order_ref=order_ref, side=side, observed_qty=target_qty,
            avg_px=average, fee_usd=total_fee,
            fee_source=("exchange_api" if history_fee_known else fee_source),
            fee_token=(next((item["fee_token"] for item in normalised
                             if item["fee_token"]), fee_token)),
            ts=ts, position_before=position_before,
            position_after=position_after, source=source,
            metadata={"history_count": len(normalised),
                      "history_qty": history_qty,
                      "price_source": "exchange_fill" if history_qty > 0
                      else "exchange_average"})
        result.update({"history_count": len(normalised),
                       "history_qty": history_qty, "source": source})
        return result

    def _reprice_fill_matches(self, fill, new_price: float) -> set[str]:
        """Keep realized PnL on its closing fill when an original price arrives.

        Match provenance is recorded for new fills. Older completed cycles
        additionally use balanced signed cashflows in cycle_financials; we do
        not invent FIFO matches for historical reconciliation baselines.
        """
        delta = new_price - float(fill["price"])
        if abs(delta) <= 1e-12:
            return set()
        matches = self._conn.execute(
            "SELECT m.*,f.execution_id FROM fill_matches m "
            "JOIN fills f ON f.id=m.closing_fill_id "
            "WHERE m.opening_fill_id=? OR m.closing_fill_id=?",
            (fill["id"], fill["id"])).fetchall()
        for match in matches:
            correction = float(match["signed_open_qty"]) * delta
            if match["opening_fill_id"] == fill["id"]:
                correction = -correction
            self._conn.execute(
                "UPDATE fills SET gross_realized_pnl_usd="
                "gross_realized_pnl_usd+? WHERE id=?",
                (correction, match["closing_fill_id"]))
        # The caller refreshes the cycles after persisting the new price.
        return {str(match["execution_id"]) for match in matches}

    def _refresh_completed_cycle_financials(self, execution_ids: Iterable[str]) -> None:
        """Refresh materialized cycle PnL inside the caller's write transaction."""
        affected = {str(value) for value in execution_ids if value}
        if not affected:
            return
        rows = self._conn.execute(
            "SELECT * FROM cycle_records WHERE flat_ts IS NOT NULL").fetchall()
        for row in rows:
            payload = json.loads(row["payload_json"] or "{}")
            ids = payload.get("execution_ids", [])
            if not affected.intersection(ids):
                continue
            financials = self.cycle_financials(
                ids, opened_ts=row["open_first_fill_ts"], until=row["flat_ts"])
            payload.update({"total_cycle_pnl_usd": financials["net_pnl_usd"],
                            "realized_cycle_pnl_usd": financials["net_pnl_usd"],
                            "cycle_fees_usd": financials["fees_usd"],
                            "fee_accounting_status": financials["fee_accounting_status"],
                            "accounting_status": "provisional",
                            "accounting_updated_ts": time.time()})
            placeholders = ",".join("?" for _ in ids)
            action_fees = self._conn.execute(
                "SELECT e.action,SUM(f.fee_usd) AS fee FROM fills f "
                "JOIN executions e ON e.execution_id=f.execution_id "
                f"WHERE f.execution_id IN ({placeholders}) "
                "AND f.source!='position_baseline' GROUP BY e.action", ids).fetchall()
            phase_fees: dict[str, float] = {}
            for action_fee in action_fees:
                column = ("open_fee_usd" if str(action_fee["action"]).lower() == "open"
                          else "close_fee_usd")
                phase_fees[column] = phase_fees.get(column, 0.0) + float(action_fee["fee"] or 0.0)
            payload.update(phase_fees)
            # Fee completeness alone cannot prove fills/funding are complete.
            self._conn.execute(
                "UPDATE cycle_records SET total_cycle_pnl_usd=?,open_fee_usd=?,"
                "close_fee_usd=?,payload_json=?,updated_ts=? WHERE cycle_id=?",
                (financials["net_pnl_usd"], payload.get("open_fee_usd", row["open_fee_usd"]),
                 payload.get("close_fee_usd", row["close_fee_usd"]), _json(payload),
                 time.time(), row["cycle_id"]))

    def mark_zero_fee_venue(self, venue: str) -> int:
        """Make a venue-wide zero-fee policy explicit in the ledger.

        Lighter currently charges no trading fee for this deployment.  Older
        rows may still contain the provisional config estimate, so clear both
        the fill fee and any fee carried by an open FIFO lot.  The operation is
        idempotent and deliberately leaves position-baseline rows untouched.
        """
        venue_key = str(venue or "").strip().lower()
        if not venue_key:
            return 0
        now = time.time()
        updated = 0
        with self._write():
            rows = self._conn.execute(
                "SELECT id,execution_id FROM fills WHERE LOWER(venue)=? "
                "AND source!='position_baseline'", (venue_key,)).fetchall()
            for row in rows:
                fill_id = int(row["id"])
                changed = self._conn.execute(
                    "UPDATE fills SET fee_usd=0.0, fee_source=?, "
                    "fee_token=NULL, fee_updated_ts=? WHERE id=? "
                    "AND (fee_usd!=0 OR fee_source!=? OR fee_token IS NOT NULL)",
                    (ZERO_FEE_SOURCE, now, fill_id, ZERO_FEE_SOURCE)).rowcount
                updated += int(changed or 0)
                self._conn.execute(
                    "UPDATE inventory_lots SET fee_remaining=0.0 "
                    "WHERE source_fill_id=?", (fill_id,))
            self._conn.execute(
                "UPDATE exchange_fills SET fee_usd=0.0, fee_token=NULL "
                "WHERE LOWER(venue)=?", (venue_key,))
            self._refresh_completed_cycle_financials(
                row["execution_id"] for row in rows)
        return updated

    def reconcile_exchange_fills(
            self, venue: str, exchange_rows: Iterable[dict], *,
            now: Optional[float] = None,
            cash_since_ts: Optional[float] = None,
            match_window_sec: float = 180.0) -> dict:
        """Persist exchange fills and replace config fee estimates safely.

        The order-confirmation path records a fee estimate immediately so the
        strategy never waits on a reporting endpoint.  This method is the
        slower audit path: exchange fills are stored under their own stable
        key, then matched to local fills by ``order_ref`` (or by a tight
        time/price/quantity fallback).  A local fill is updated only when the
        exchange data covers the whole local order quantity and every fee in
        that group is known.  Replaying the same API page is therefore
        idempotent and cannot double-charge the ledger.
        """
        now = time.time() if now is None else float(now)
        normalised = []
        for index, row in enumerate(exchange_rows or ()):
            item = _normalise_exchange_fill(venue, row, index)
            if item is not None:
                normalised.append(item)

        result = {
            "fetched_count": len(normalised),
            "stored_count": 0,
            "actual_fill_count": 0,
            "unmatched_count": 0,
            "actual_fee_usd": 0.0,
            "fee_delta_usd": 0.0,
            "cash_adjustment_usd": 0.0,
        }
        if not normalised:
            return result

        with self._write():
            for item in normalised:
                existing = self._conn.execute(
                    "SELECT 1 FROM exchange_fills "
                    "WHERE exchange_fill_key=?", (item["key"],)
                ).fetchone()
                if existing is None:
                    result["stored_count"] += 1
                self._conn.execute(
                    """INSERT INTO exchange_fills(
                           exchange_fill_key,venue,exchange_trade_id,
                           exchange_order_ref,ts,side,qty,price,fee_amount,
                           fee_usd,fee_token,raw_json,matched_fill_id,
                           first_seen_ts,updated_ts)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(exchange_fill_key) DO UPDATE SET
                           venue=excluded.venue,
                           exchange_trade_id=excluded.exchange_trade_id,
                           exchange_order_ref=excluded.exchange_order_ref,
                           ts=excluded.ts,side=excluded.side,qty=excluded.qty,
                           price=excluded.price,fee_amount=excluded.fee_amount,
                           fee_usd=excluded.fee_usd,fee_token=excluded.fee_token,
                           raw_json=excluded.raw_json,
                           updated_ts=excluded.updated_ts""",
                    (item["key"], item["venue"], item["trade_id"],
                     item["order_ref"], item["ts"], item["side"],
                     item["qty"], item["price"], item["fee_amount"],
                     item["fee_usd"], item["fee_token"], item["raw_json"],
                     None, item["ts"], now))

            matched_local_ids: set[int] = set()

            def local_for_order(order_ref: str, side: str) -> list[sqlite3.Row]:
                return self._conn.execute(
                    """SELECT id,ts,qty,price,fee_usd,fee_source
                         FROM fills
                        WHERE venue=? AND order_ref=? AND side=?
                          AND source!='position_baseline'
                        ORDER BY ts,id""",
                    (venue, order_ref, side)).fetchall()

            def update_group(exchange_group: list[dict],
                             local_rows: list[sqlite3.Row]) -> bool:
                if not exchange_group or not local_rows:
                    return False
                if any(item["fee_usd"] is None for item in exchange_group):
                    return False
                exchange_qty = sum(item["qty"] for item in exchange_group)
                local_qty = sum(abs(float(row["qty"])) for row in local_rows)
                tolerance = max(1e-8, local_qty * 1e-4)
                if local_qty <= 0 or abs(exchange_qty - local_qty) > tolerance:
                    return False

                fee_total = sum(float(item["fee_usd"])
                                for item in exchange_group)
                notionals = [abs(float(row["qty"])) * float(row["price"])
                             for row in local_rows]
                weight_total = sum(notionals)
                if weight_total <= 0:
                    notionals = [abs(float(row["qty"])) for row in local_rows]
                    weight_total = sum(notionals)
                if weight_total <= 0:
                    return False
                tokens = {str(item["fee_token"] or "").strip()
                          for item in exchange_group
                          if str(item["fee_token"] or "").strip()}
                fee_token = next(iter(tokens)) if len(tokens) == 1 else (
                    "MIXED" if len(tokens) > 1 else None)
                first_fill_id = int(local_rows[0]["id"])
                for item in exchange_group:
                    self._conn.execute(
                        "UPDATE exchange_fills SET matched_fill_id=? "
                        "WHERE exchange_fill_key=?",
                        (first_fill_id, item["key"]))
                for row, weight in zip(local_rows, notionals):
                    fill_id = int(row["id"])
                    old_fee = float(row["fee_usd"] or 0.0)
                    new_fee = fee_total * weight / weight_total
                    delta_fee = new_fee - old_fee
                    self._conn.execute(
                        "UPDATE fills SET fee_usd=?, fee_source=?, "
                        "fee_token=?, fee_updated_ts=? WHERE id=?",
                        (new_fee, "exchange_api", fee_token, now, fill_id))
                    if abs(delta_fee) > 1e-15:
                        original_qty = abs(float(row["qty"]))
                        if original_qty > 0:
                            lots = self._conn.execute(
                                "SELECT id,qty_remaining FROM inventory_lots "
                                "WHERE source_fill_id=?", (fill_id,)
                            ).fetchall()
                            for lot in lots:
                                share = abs(float(lot["qty_remaining"])) \
                                    / original_qty
                                self._conn.execute(
                                    "UPDATE inventory_lots SET "
                                    "fee_remaining=fee_remaining+? WHERE id=?",
                                    (delta_fee * share, int(lot["id"])))
                        result["fee_delta_usd"] += delta_fee
                        if (cash_since_ts is not None
                                and float(row["ts"]) >= float(cash_since_ts)):
                            result["cash_adjustment_usd"] += delta_fee
                    result["actual_fill_count"] += 1
                    result["actual_fee_usd"] += new_fee
                    matched_local_ids.add(fill_id)
                return True

            grouped: dict[tuple[str, str], list[dict]] = {}
            no_order_ref: list[dict] = []
            for item in normalised:
                order_ref = item["order_ref"]
                if order_ref:
                    grouped.setdefault((order_ref, item["side"]), []).append(item)
                else:
                    no_order_ref.append(item)

            for (order_ref, side), exchange_group in grouped.items():
                local_rows = local_for_order(order_ref, side)
                if not update_group(exchange_group, local_rows):
                    result["unmatched_count"] += len(exchange_group)

            # Some reporting endpoints omit the client order id.  It is safe
            # to use this fallback only when quantity, price and timestamp all
            # identify one local fill unambiguously.
            for item in sorted(no_order_ref, key=lambda value: value["ts"]):
                candidates = self._conn.execute(
                    """SELECT id,ts,qty,price,fee_usd,fee_source
                         FROM fills
                        WHERE venue=? AND side=?
                          AND source!='position_baseline'
                          AND ABS(ts-?)<=?
                        ORDER BY ABS(ts-?),id""",
                    (venue, item["side"], item["ts"],
                     max(float(match_window_sec), 0.0), item["ts"])
                ).fetchall()
                candidate = None
                for row in candidates:
                    if int(row["id"]) in matched_local_ids:
                        continue
                    price = float(row["price"] or 0.0)
                    if price <= 0 or abs(price - item["price"]) \
                            > max(1e-8, price * 0.01):
                        continue
                    if abs(abs(float(row["qty"])) - item["qty"]) \
                            > max(1e-8, abs(float(row["qty"])) * 1e-4):
                        continue
                    candidate = row
                    break
                if not update_group([item], [candidate] if candidate else []):
                    result["unmatched_count"] += 1

            if matched_local_ids:
                placeholders = ",".join("?" for _ in matched_local_ids)
                executions = self._conn.execute(
                    f"SELECT DISTINCT execution_id FROM fills WHERE id IN ({placeholders})",
                    list(matched_local_ids)).fetchall()
                self._refresh_completed_cycle_financials(row[0] for row in executions)
        return result

    def _apply_inventory_fill(self, venue: str, signed_qty: float,
                              price: float, fee_usd: float, ts: float,
                              *, source: str) -> tuple[float, list[tuple[float, float]], list[tuple[int, float]]]:
        """Apply one signed fill to FIFO lots and return gross realized PnL."""
        remaining = signed_qty
        gross = 0.0
        matches = []
        rows = self._conn.execute(
            """SELECT id,qty_remaining,price,fee_remaining,source,source_fill_id
               FROM inventory_lots WHERE venue=? ORDER BY opened_ts,id""",
            (venue,)).fetchall()
        for row in rows:
            lot_qty = float(row["qty_remaining"])
            if remaining == 0 or lot_qty == 0 or lot_qty * remaining >= 0:
                continue
            close = min(abs(remaining), abs(lot_qty))
            # A reconciliation baseline is deliberately marked at the
            # exchange price and carries no historical cost basis.  Neither
            # creating nor consuming that unknown inventory may manufacture a
            # trading gain/loss in the fill ledger.  Actual fills closing an
            # actual lot remain fully FIFO-accounted.
            if source != "position_baseline" \
                    and row["source"] != "position_baseline":
                if row["source_fill_id"] is not None:
                    matches.append((int(row["source_fill_id"]),
                                    close if lot_qty > 0 else -close))
                if lot_qty > 0 and remaining < 0:
                    gross += (price - float(row["price"])) * close
                elif lot_qty < 0 and remaining > 0:
                    gross += (float(row["price"]) - price) * close
            lot_ratio = close / abs(lot_qty)
            new_qty = lot_qty + (-close if lot_qty > 0 else close)
            new_fee = max(float(row["fee_remaining"]) * (1 - lot_ratio), 0.0)
            if abs(new_qty) <= 1e-12:
                self._conn.execute("DELETE FROM inventory_lots WHERE id=?",
                                   (row["id"],))
            else:
                self._conn.execute(
                    "UPDATE inventory_lots SET qty_remaining=?,fee_remaining=? "
                    "WHERE id=?", (new_qty, new_fee, row["id"]))
            remaining += -close if remaining > 0 else close

        if abs(remaining) <= 1e-12:
            return gross, [], matches
        opened_fee = fee_usd * abs(remaining) / abs(signed_qty)
        return gross, [(remaining, opened_fee)], matches

    def preview_close_cost(self, *, venue: str, position: float, qty: float,
                           now: float) -> dict:
        """Read the exact FIFO inventory a proposed close would consume.

        No mutations and no reconstructed cost from a reconciliation mark.
        Remaining opening fees are updated by the existing exchange fee sync.
        Funding debits are allocated per unit over that unit's lifetime;
        credits are deliberately not used to justify a profitable exit.
        """
        result = {"ready": False, "reason": "inventory_mismatch",
                  "entry_cash_usd": 0.0, "entry_fees_usd": 0.0,
                  "funding_debit_usd": 0.0, "oldest_ts": now,
                  "age_qty_hours": 0.0,
                  "fee_estimated": False}
        if (not all(math.isfinite(x) for x in (position, qty, now))
                or qty <= 0 or abs(position) + 1e-9 < qty):
            return result
        with self._lock:
            rows = self._conn.execute(
                """SELECT l.*, f.fee_source, f.metadata_json,
                          f.order_ref AS fill_order_ref
                   FROM inventory_lots l
                   LEFT JOIN fills f ON f.id=l.source_fill_id
                   WHERE l.venue=? ORDER BY l.opened_ts,l.id""",
                (venue,)).fetchall()
            if abs(sum(float(r["qty_remaining"]) for r in rows)
                   - position) > 1e-8:
                return result
            remaining = qty
            for row in rows:
                lot_qty = float(row["qty_remaining"])
                if abs(lot_qty) < 1e-12:
                    continue
                if lot_qty * position <= 0:
                    return result
                if remaining <= 1e-12:
                    break
                try:
                    meta = json.loads(row["metadata_json"] or "{}")
                except (ValueError, TypeError):
                    meta = {"price_source": "limit_fallback"}
                if not isinstance(meta, dict):
                    meta = {"price_source": "limit_fallback"}
                # Older confirmations did not label fallback prices. Require
                # evidence of an exchange average before trusting those lots.
                if not meta.get("price_source") and "cumulative_filled" in meta:
                    order = self._conn.execute(
                        "SELECT avg_px FROM orders WHERE venue=? AND order_ref=?",
                        (venue, row["fill_order_ref"])).fetchone()
                    if order is None or not order["avg_px"]:
                        result["reason"] = "unknown_entry_cost"
                        return result
                if (row["source"] in ("position_baseline", "restart_reconciliation")
                        or (not meta.get("price_source")
                            and meta.get("previous_cumulative_filled", 0) > 0)
                        or meta.get("price_source") == "limit_fallback"):
                    result["reason"] = "unknown_entry_cost"
                    return result
                take = min(remaining, abs(lot_qty))
                price = float(row["price"])
                if not math.isfinite(price) or price <= 0:
                    result["reason"] = "unknown_entry_cost"
                    return result
                result["entry_cash_usd"] += (
                    -1 if lot_qty > 0 else 1) * take * price
                result["entry_fees_usd"] += max(
                    float(row["fee_remaining"]), 0.0) * take / abs(lot_qty)
                result["oldest_ts"] = min(result["oldest_ts"], row["opened_ts"])
                result["age_qty_hours"] += take * max(
                    0.0, now - row["opened_ts"]) / 3600
                result["fee_estimated"] |= row["fee_source"] not in (
                    "exchange_api", "venue_zero_fee")
                debit = self._conn.execute(
                    """SELECT COALESCE(SUM(-pnl_usd/ABS(position_qty)),0)
                       FROM funding_events WHERE venue=? AND ts>=? AND ts<=?
                       AND pnl_usd<0 AND ABS(position_qty)>0""",
                    (venue, row["opened_ts"], now)).fetchone()[0]
                result["funding_debit_usd"] += take * float(debit)
                remaining -= take
        result["ready"] = remaining <= 1e-9
        result["reason"] = "ready" if result["ready"] else "inventory_mismatch"
        return result

    def ensure_position_baseline(self, *, venue: str, exchange_qty: float,
                                 mark_px: Optional[float], ts: float,
                                 tolerance: float = 1e-8) -> bool:
        """Anchor unknown/restarted inventory at its reconciliation mark.

        A baseline is not presented as trading PnL. It only prevents a
        pre-existing position (or an external manual fill) from appearing as
        a fabricated gain/loss when the new ledger starts observing it.
        """
        if mark_px is None or not math.isfinite(float(mark_px)) \
                or float(mark_px) <= 0:
            return False
        with self._lock:
            current = self._conn.execute(
                "SELECT COALESCE(SUM(qty_remaining),0) FROM inventory_lots "
                "WHERE venue=?", (venue,)).fetchone()[0]
        delta = float(exchange_qty) - float(current)
        if abs(delta) <= tolerance:
            return False
        direction = "buy" if delta > 0 else "sell"
        key = f"baseline:{venue}:{int(ts)}:{float(exchange_qty):.12g}"
        return self.record_fill(
            fill_key=key, execution_id=f"reconcile:{venue}", leg="baseline",
            venue=venue, order_ref=None, ts=ts, side=direction,
            qty=abs(delta), price=float(mark_px), source="position_baseline",
            metadata={"ledger_qty_before": current,
                      "exchange_qty": exchange_qty})

    # --------------------------------------------------------- reconciliation

    def record_reconciliation(self, *, ts: float, venue: str,
                              local_qty: float, exchange_qty: float,
                              mark_px: Optional[float], status: str,
                              source: str = "exchange_position") -> None:
        with self._write():
            self._conn.execute(
                """INSERT INTO position_reconciliations(
                    ts,venue,local_qty,exchange_qty,delta_qty,mark_px,status,source)
                VALUES(?,?,?,?,?,?,?,?)""",
                (ts, venue, local_qty, exchange_qty,
                 exchange_qty - local_qty, mark_px, status, source))

    def record_funding_event(self, *, ts: float, venue: str,
                             position_qty: float, mark_px: float,
                             rate_bps_h: float, pnl_usd: float,
                             source: str = "estimated_rate") -> None:
        if not all(math.isfinite(float(x)) for x in (
                ts, position_qty, mark_px, rate_bps_h, pnl_usd)):
            return
        with self._noncritical_write("funding_event") as acquired:
            if not acquired:
                return
            self._conn.execute(
                """INSERT INTO funding_events(
                    ts,venue,position_qty,mark_px,rate_bps_h,pnl_usd,source)
                VALUES(?,?,?,?,?,?,?)""",
                (ts, venue, position_qty, mark_px, rate_bps_h, pnl_usd, source))

    # --------------------------------------------------------------- trades

    def record_trade_attempt(self, payload: dict, *, source: str = "engine") -> None:
        key = str(payload.get("trade_key") or payload.get("execution_id")
                  or f"trade:{payload.get('ts')}:{payload.get('direction')}")
        qty = float(payload.get("qty") or 0.0)
        buy_fill = abs(float(payload.get("buy_fill") or 0.0))
        sell_fill = abs(float(payload.get("sell_fill") or 0.0))
        row = {
            "trade_key": key,
            "execution_id": payload.get("execution_id"),
            "ts": float(payload.get("ts") or time.time()),
            "source": source,
            "action": str(payload.get("action") or "").lower(),
            "direction": str(payload.get("direction") or "").lower(),
            "qty": qty,
            "buy_notional": float(payload.get("buy_notional") or 0.0),
            "sell_notional": float(payload.get("sell_notional") or 0.0),
            "buy_fill": buy_fill,
            "sell_fill": sell_fill,
            "buy_status": str(payload.get("buy_status") or "unknown"),
            "sell_status": str(payload.get("sell_status") or "unknown"),
            "buy_venue": str(payload.get("buy_venue") or "").lower(),
            "sell_venue": str(payload.get("sell_venue") or "").lower(),
            "threshold_bps": _number(payload.get("threshold_bps")),
            "net_edge_bps": _number(payload.get("net_edge_bps")),
            "fill_edge_usd": _number(payload.get("fill_edge_usd")),
            "outcome": str(payload.get("outcome") or
                            _outcome(qty, buy_fill, sell_fill)),
            "reduce_only": int(bool(payload.get("reduce_only"))),
        }
        with self._noncritical_write("trade_attempt") as acquired:
            if not acquired:
                return
            self._conn.execute(
                """INSERT INTO trade_attempts(
                    trade_key,execution_id,ts,source,action,direction,qty,
                    buy_notional,sell_notional,buy_fill,sell_fill,buy_status,
                    sell_status,buy_venue,sell_venue,threshold_bps,net_edge_bps,
                    fill_edge_usd,outcome,reduce_only,payload_json)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(trade_key) DO UPDATE SET
                    execution_id=excluded.execution_id,ts=excluded.ts,
                    source=excluded.source,action=excluded.action,
                    direction=excluded.direction,qty=excluded.qty,
                    buy_notional=excluded.buy_notional,
                    sell_notional=excluded.sell_notional,
                    buy_fill=excluded.buy_fill,sell_fill=excluded.sell_fill,
                    buy_status=excluded.buy_status,sell_status=excluded.sell_status,
                    buy_venue=excluded.buy_venue,sell_venue=excluded.sell_venue,
                    threshold_bps=excluded.threshold_bps,
                    net_edge_bps=excluded.net_edge_bps,
                    fill_edge_usd=excluded.fill_edge_usd,
                    outcome=excluded.outcome,reduce_only=excluded.reduce_only,
                    payload_json=excluded.payload_json""",
                (row["trade_key"], row["execution_id"], row["ts"],
                 row["source"], row["action"], row["direction"], row["qty"],
                 row["buy_notional"], row["sell_notional"], row["buy_fill"],
                 row["sell_fill"], row["buy_status"], row["sell_status"],
                 row["buy_venue"], row["sell_venue"], row["threshold_bps"],
                 row["net_edge_bps"], row["fill_edge_usd"], row["outcome"],
                row["reduce_only"], _json(payload)))

    def record_opportunity_event(self, *, opportunity_id: str, stage: str,
                                 ts: Optional[float] = None,
                                 campaign_id: Optional[str] = None,
                                 cycle_id: Optional[str] = None,
                                 direction: Optional[str] = None,
                                 reason: Optional[str] = None,
                                 payload: Optional[dict] = None,
                                 event_id: Optional[str] = None) -> str:
        """Persist one idempotent opportunity-funnel stage transition.

        The event identity includes the campaign where applicable. Repeated
        scans therefore update no rows, while a second bounded campaign for
        the same signal remains visible as a separate campaign event.
        """
        opportunity = str(opportunity_id or "").strip()
        stage_value = str(stage or "").strip().lower()
        if not opportunity or not stage_value:
            raise ValueError("opportunity event requires id and stage")
        stable = "|".join((opportunity, str(campaign_id or ""),
                           str(cycle_id or ""), stage_value))
        stable_id = event_id or hashlib.sha256(stable.encode()).hexdigest()[:32]
        event_ts = _number(ts) or time.time()
        event_payload = payload if isinstance(payload, dict) else {}
        with self._noncritical_write("opportunity_event") as acquired:
            if not acquired:
                return str(stable_id)
            self._conn.execute(
                """INSERT OR IGNORE INTO opportunity_events(
                    event_id,opportunity_id,campaign_id,cycle_id,ts,direction,
                    stage,reason,payload_json)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (str(stable_id), opportunity,
                 None if campaign_id in (None, "") else str(campaign_id),
                 None if cycle_id in (None, "") else str(cycle_id),
                 event_ts,
                 None if direction in (None, "") else str(direction),
                 stage_value,
                 None if reason in (None, "") else str(reason),
                 _json(event_payload)))
        return str(stable_id)

    def load_opportunity_events(self, *, since: Optional[float] = None,
                                until: Optional[float] = None,
                                opportunity_id: Optional[str] = None,
                                limit: int = 5000) -> list[dict]:
        """Load durable funnel transitions in chronological order."""
        clauses = []
        args: list[Any] = []
        if since is not None:
            clauses.append("ts>=?")
            args.append(float(since))
        if until is not None:
            clauses.append("ts<=?")
            args.append(float(until))
        if opportunity_id:
            clauses.append("opportunity_id=?")
            args.append(str(opportunity_id))
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        args.append(max(min(int(limit), 20000), 1))
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT event_id,opportunity_id,campaign_id,cycle_id,ts,
                           direction,stage,reason,payload_json
                      FROM opportunity_events {where}
                     ORDER BY ts ASC,event_id ASC LIMIT ?""", args).fetchall()
        result = []
        for row in rows:
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            result.append({
                "event_id": row["event_id"],
                "opportunity_id": row["opportunity_id"],
                "campaign_id": row["campaign_id"],
                "cycle_id": row["cycle_id"], "ts": row["ts"],
                "direction": row["direction"], "stage": row["stage"],
                "reason": row["reason"], "payload": payload,
            })
        return result

    def has_data(self, table: Optional[str] = None) -> bool:
        """Return whether SQLite has anything useful for the dashboard."""
        allowed = {"fills", "trade_attempts", "market_samples",
                   "minute_samples", "pnl_snapshots", "model_state",
                   "cycle_records", "execution_cost_observations",
                   "opportunity_events"}
        if table is not None and table not in allowed:
            raise ValueError(f"unsupported ledger table: {table}")
        expression = (f"EXISTS(SELECT 1 FROM {table})" if table else
                      "EXISTS(SELECT 1 FROM fills) OR "
                      "EXISTS(SELECT 1 FROM trade_attempts) OR "
                      "EXISTS(SELECT 1 FROM market_samples) OR "
                      "EXISTS(SELECT 1 FROM minute_samples) OR "
                      "EXISTS(SELECT 1 FROM pnl_snapshots) OR "
                      "EXISTS(SELECT 1 FROM model_state) OR "
                      "EXISTS(SELECT 1 FROM cycle_records) OR "
                      "EXISTS(SELECT 1 FROM execution_cost_observations) OR "
                      "EXISTS(SELECT 1 FROM opportunity_events)")
        with self._lock:
            row = self._conn.execute(
                f"SELECT {expression}").fetchone()
        return bool(row[0])

    def record_market_sample(self, *, ts: float, payload: dict,
                             source_key: Optional[str] = None) -> None:
        """Upsert one second-level market sample.

        The wall-clock second is the idempotency key.  Replaying a recorder row
        or restarting immediately therefore cannot duplicate history.
        """
        key = source_key or f"live:{int(float(ts))}"
        with self._noncritical_write("market_sample") as acquired:
            if not acquired:
                return
            self._conn.execute(
                """INSERT INTO market_samples(source_key,ts,payload_json)
                   VALUES(?,?,?)
                   ON CONFLICT(source_key) DO UPDATE SET
                     ts=excluded.ts,payload_json=excluded.payload_json""",
                (key, float(ts), _json(payload)))

    def save_model_state(self, model_key: str, state: dict,
                         ts: Optional[float] = None) -> None:
        """Persist one bounded model snapshot atomically."""
        key = str(model_key or "").strip()
        if not key:
            raise ValueError("model_key is required")
        now = time.time() if ts is None else float(ts)
        if not math.isfinite(now):
            raise ValueError("model state timestamp must be finite")
        with self._noncritical_write("model_state") as acquired:
            if not acquired:
                return
            self._conn.execute(
                """INSERT INTO model_state(model_key,updated_ts,state_json)
                   VALUES(?,?,?)
                   ON CONFLICT(model_key) DO UPDATE SET
                     updated_ts=excluded.updated_ts,
                     state_json=excluded.state_json""",
                (key, now, _json(state)))

    def load_model_state(self, model_key: str) -> Optional[tuple[float, dict]]:
        """Load a model snapshot as ``(saved_ts, state)`` if valid."""
        key = str(model_key or "").strip()
        if not key:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT updated_ts,state_json FROM model_state "
                "WHERE model_key=?", (key,)).fetchone()
        if row is None:
            return None
        try:
            saved = float(row["updated_ts"])
            state = json.loads(row["state_json"] or "{}")
            if not math.isfinite(saved) or not isinstance(state, dict):
                return None
            return saved, state
        except (TypeError, ValueError, json.JSONDecodeError):
            log.error("invalid model state checkpoint for %s", key)
            return None

    def load_valid_premium_samples(self, *, since: float,
                                   until: Optional[float] = None,
                                   limit: int = 20000
                                   ) -> list[tuple[float, float]]:
        """Load valid second-level premium samples for model rehydration.

        SQLite is the durable source of truth for recorder history.  Keep the
        query bounded and validate the quality flag in Python because older
        SQLite files may not have SQLite's JSON1 extension enabled.
        """
        start = float(since)
        end = time.time() if until is None else float(until)
        if not (math.isfinite(start) and math.isfinite(end)):
            return []
        if end < start:
            start, end = end, start
        row_limit = max(min(int(limit), 200000), 1)
        with self._lock:
            rows = self._conn.execute(
                """SELECT ts,payload_json FROM market_samples
                   WHERE ts>=? AND ts<=? ORDER BY ts ASC LIMIT ?""",
                (start, end, row_limit)).fetchall()
        result: list[tuple[float, float]] = []
        for row in rows:
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            valid = payload.get("premium_sample_valid")
            if isinstance(valid, str):
                valid = valid.strip().lower() in {
                    "1", "true", "yes", "valid"
                }
            elif isinstance(valid, (int, float, bool)):
                valid = bool(valid)
            else:
                valid = False
            if not valid:
                continue
            ts = _number(row["ts"])
            premium = _number(payload.get("premium_mid_bps"))
            if ts is not None and premium is not None:
                result.append((ts, premium))
        return result

    # ------------------------------------------------------ execution costs

    def record_execution_cost_observation(self, observation: Any) -> None:
        """Persist one bounded-model sample without requiring a model import."""
        def value(name: str, default: Any = None) -> Any:
            if isinstance(observation, dict):
                return observation.get(name, default)
            return getattr(observation, name, default)

        phase = str(value("phase", "") or "").strip().lower()
        if phase not in {"open_first", "open_hedge", "close_first", "close_hedge"}:
            raise ValueError(f"unsupported execution-cost phase: {phase!r}")
        metadata = value("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        raw_is_buy = value("is_buy", metadata.get("is_buy"))
        if raw_is_buy is None:
            is_buy = None
        elif isinstance(raw_is_buy, str):
            is_buy = (None if raw_is_buy.strip().lower() in {"", "none", "null"}
                      else int(raw_is_buy.strip().lower()
                               in {"1", "true", "yes", "buy"}))
        else:
            is_buy = int(bool(raw_is_buy))
        numeric_names = (
            "ts", "requested_qty", "filled_qty", "quoted_top_px",
            "quoted_depth_avg_px", "quoted_marginal_px", "limit_px",
            "avg_fill_px", "latency_sec", "incremental_execution_cost_bps",
            "all_in_cost_bps", "all_in_execution_cost_bps",
            "realized_vs_marginal_bps", "realized_slippage_bps",
            "adverse_move_bps", "latency_adverse_move_bps",
            "depth_cost_bps")
        numbers: dict[str, Optional[float]] = {}
        for name in numeric_names:
            raw = value(name)
            if raw is None:
                numbers[name] = None
                continue
            try:
                number = float(raw)
            except (TypeError, ValueError):
                numbers[name] = None
                continue
            numbers[name] = number if math.isfinite(number) else None
        if numbers["ts"] is None:
            numbers["ts"] = time.time()
        if numbers["requested_qty"] is None or numbers["filled_qty"] is None:
            raise ValueError("execution-cost quantities must be finite")
        if numbers["all_in_execution_cost_bps"] is None:
            numbers["all_in_execution_cost_bps"] = numbers["all_in_cost_bps"]
        if numbers["latency_adverse_move_bps"] is None:
            numbers["latency_adverse_move_bps"] = numbers["adverse_move_bps"]
        with self._noncritical_write("execution_cost_observation") as acquired:
            if not acquired:
                return
            self._conn.execute(
                """INSERT INTO execution_cost_observations(
                    execution_id,phase,ts,requested_qty,filled_qty,
                    is_buy,
                    quoted_top_px,quoted_depth_avg_px,quoted_marginal_px,
                    limit_px,avg_fill_px,latency_sec,
                    incremental_execution_cost_bps,all_in_cost_bps,
                    all_in_execution_cost_bps,
                    realized_vs_marginal_bps,
                    realized_slippage_bps,adverse_move_bps,
                    latency_adverse_move_bps,depth_cost_bps,
                    outcome,metadata_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (str(value("execution_id", "") or ""), phase,
                 numbers["ts"], max(numbers["requested_qty"], 0.0),
                 max(numbers["filled_qty"], 0.0), is_buy,
                 numbers["quoted_top_px"],
                 numbers["quoted_depth_avg_px"],
                 numbers["quoted_marginal_px"], numbers["limit_px"],
                 numbers["avg_fill_px"], numbers["latency_sec"],
                 numbers["incremental_execution_cost_bps"],
                 numbers["all_in_cost_bps"],
                 numbers["all_in_execution_cost_bps"],
                 numbers["realized_vs_marginal_bps"],
                 numbers["realized_slippage_bps"], numbers["adverse_move_bps"],
                 numbers["latency_adverse_move_bps"],
                 numbers["depth_cost_bps"],
                 str(value("outcome", "unknown") or "unknown"),
                 _json(metadata)))

    def load_execution_cost_observations(
            self, *, phases: Optional[Iterable[str]] = None,
            limit: int = 200) -> list[dict]:
        """Load newest cost samples, suitable for a bounded restart restore."""
        wanted = (None if phases is None
                  else [str(item).lower() for item in phases])
        valid = {"open_first", "open_hedge", "close_first", "close_hedge"}
        if wanted is not None and any(item not in valid for item in wanted):
            raise ValueError("unsupported execution-cost phase")
        count = max(min(int(limit), 2000), 1)
        where = ""
        args: list[Any] = []
        if wanted:
            where = "WHERE phase IN (" + ",".join("?" for _ in wanted) + ")"
            args.extend(wanted)
        args.append(count)
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT execution_id,phase,ts,requested_qty,filled_qty,
                           is_buy,
                           quoted_top_px,quoted_depth_avg_px,quoted_marginal_px,
                           limit_px,avg_fill_px,latency_sec,
                           incremental_execution_cost_bps,all_in_cost_bps,
                           all_in_execution_cost_bps,
                           realized_vs_marginal_bps,realized_slippage_bps,
                           adverse_move_bps,latency_adverse_move_bps,
                           depth_cost_bps,outcome,metadata_json
                      FROM execution_cost_observations {where}
                     ORDER BY ts DESC,id DESC LIMIT ?""", args).fetchall()
        result = []
        for row in rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                metadata = {}
            result.append({
                "execution_id": row["execution_id"], "phase": row["phase"],
                "ts": row["ts"], "requested_qty": row["requested_qty"],
                "filled_qty": row["filled_qty"],
                "is_buy": (None if row["is_buy"] is None
                           else bool(row["is_buy"])),
                "quoted_top_px": row["quoted_top_px"],
                "quoted_depth_avg_px": row["quoted_depth_avg_px"],
                "quoted_marginal_px": row["quoted_marginal_px"],
                "limit_px": row["limit_px"], "avg_fill_px": row["avg_fill_px"],
                "latency_sec": row["latency_sec"],
                "incremental_execution_cost_bps": row[
                    "incremental_execution_cost_bps"],
                "all_in_cost_bps": row["all_in_cost_bps"],
                "all_in_execution_cost_bps": (
                    row["all_in_execution_cost_bps"]
                    if row["all_in_execution_cost_bps"] is not None
                    else row["all_in_cost_bps"]),
                "realized_vs_marginal_bps": row["realized_vs_marginal_bps"],
                "realized_slippage_bps": row["realized_slippage_bps"],
                "adverse_move_bps": row["adverse_move_bps"],
                "latency_adverse_move_bps": (
                    row["latency_adverse_move_bps"]
                    if row["latency_adverse_move_bps"] is not None
                    else row["adverse_move_bps"]),
                "depth_cost_bps": row["depth_cost_bps"],
                "outcome": row["outcome"], "metadata": metadata,
            })
        return result

    def backfill_execution_cost_observations(self, *, limit: int = 500) -> dict:
        """Backfill only explicitly auditable execution-cost payloads.

        Older execution contexts usually contain an order limit but not the
        quote that was executable immediately before submission.  A limit is
        not a quote, so those rows are intentionally skipped.  A producer may
        opt in a durable context item with ``quote_source`` set to
        ``execution_snapshot`` (or ``reliable=True``) and an explicit
        ``quoted_depth_avg_px``; the method is
        idempotent and returns its skip/insert counts for startup telemetry.
        """
        phases = {"open_first", "open_hedge", "close_first", "close_hedge"}
        max_rows = max(min(int(limit), 5000), 1)
        with self._lock:
            rows = self._conn.execute(
                "SELECT execution_id,context_json FROM executions "
                "ORDER BY updated_ts DESC LIMIT ?", (max_rows,)).fetchall()
        inserted = skipped = 0
        for row in rows:
            try:
                context = json.loads(row["context_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                skipped += 1
                continue
            if not isinstance(context, dict):
                skipped += 1
                continue
            raw = context.get("execution_cost_observations")
            if isinstance(raw, dict):
                items = []
                for phase, value in raw.items():
                    if isinstance(value, list):
                        items.extend({"phase": phase, **item}
                                     for item in value if isinstance(item, dict))
                    elif isinstance(value, dict):
                        items.append({"phase": phase, **value})
            elif isinstance(raw, list):
                items = [item for item in raw if isinstance(item, dict)]
            else:
                continue
            for item in items:
                phase = str(item.get("phase") or "").lower()
                source = str(item.get("quote_source") or "").lower()
                reliable = bool(item.get("reliable")) or source in {
                    "execution_snapshot", "book_snapshot", "pre_send_book"
                }
                top = _number(item.get("quoted_top_px"))
                depth_avg = _number(item.get("quoted_depth_avg_px"))
                marginal = _number(item.get("quoted_marginal_px"))
                average = _number(item.get("avg_fill_px"))
                requested = _number(item.get("requested_qty"))
                filled = _number(item.get("filled_qty"))
                is_buy = item.get("is_buy")
                ts = _number(item.get("ts"))
                if (phase not in phases or not reliable or top is None
                        or top <= 0 or depth_avg is None or depth_avg <= 0
                        or average is None or average <= 0
                        or requested is None or requested <= 0
                        or filled is None or filled <= 0
                        or ts is None or is_buy is None):
                    skipped += 1
                    continue
                try:
                    buy = (is_buy.strip().lower() in
                           {"1", "true", "yes", "buy"}
                           if isinstance(is_buy, str) else bool(is_buy))
                    all_in = ((average / top - 1.0) * 1e4 if buy
                              else (1.0 - average / top) * 1e4)
                    incremental = ((average / depth_avg - 1.0) * 1e4 if buy
                                   else (1.0 - average / depth_avg) * 1e4)
                    reference = (marginal if marginal and marginal > 0 else top)
                    vs_marginal = ((average / reference - 1.0) * 1e4 if buy
                                   else (1.0 - average / reference) * 1e4)
                    depth = ((depth_avg / top - 1.0) * 1e4 if buy
                             else (1.0 - depth_avg / top) * 1e4)
                except (TypeError, ZeroDivisionError):
                    skipped += 1
                    continue
                values = (all_in, incremental, vs_marginal, depth)
                if not all(math.isfinite(value) for value in values):
                    skipped += 1
                    continue
                if isinstance(is_buy, str):
                    buy = is_buy.strip().lower() in {"1", "true", "yes", "buy"}
                else:
                    buy = bool(is_buy)
                all_in = max(all_in, 0.0)
                incremental = max(incremental, 0.0)
                vs_marginal = max(vs_marginal, 0.0)
                depth = max(depth, 0.0)
                adverse_move = _number(item.get("latency_adverse_move_bps"))
                if adverse_move is None:
                    adverse_move = _number(item.get("adverse_move_bps"))
                execution_id = str(row["execution_id"] or "")
                with self._lock:
                    exists = self._conn.execute(
                        "SELECT 1 FROM execution_cost_observations "
                        "WHERE execution_id=? AND phase=? AND ts=? LIMIT 1",
                        (execution_id, phase, ts)).fetchone()
                    if exists:
                        continue
                    self._conn.execute(
                        """INSERT INTO execution_cost_observations(
                            execution_id,phase,ts,requested_qty,filled_qty,
                            is_buy,
                            quoted_top_px,quoted_depth_avg_px,
                            quoted_marginal_px,limit_px,avg_fill_px,latency_sec,
                            incremental_execution_cost_bps,all_in_cost_bps,
                            all_in_execution_cost_bps,
                            realized_vs_marginal_bps,realized_slippage_bps,
                            adverse_move_bps,latency_adverse_move_bps,
                            depth_cost_bps,outcome,metadata_json)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (execution_id, phase, ts, requested, filled, int(buy), top,
                         depth_avg, marginal, _number(item.get("limit_px")),
                         average, _number(item.get("latency_sec")), incremental,
                         all_in, all_in,
                         vs_marginal, all_in, _number(item.get(
                             "adverse_move_bps")), adverse_move, depth,
                         str(item.get("outcome") or "backfilled"),
                         _json({**item, "backfill_source": "durable_context"})))
                    inserted += 1
        return {"scanned": len(rows), "inserted": inserted,
                "skipped_unreliable": skipped}

    def recent_cycle_outcomes(self, *, limit: int = 200,
                              now: Optional[float] = None,
                              since: Optional[float] = None,
                              direction: Optional[str] = None,
                              policy_key: Optional[str] = None) -> dict:
        """Bounded completed outcomes plus censored inventory ages as of now.

        Losses and recovery exits are retained. Policy keys live in the cycle
        payload, so old unversioned records do not match a requested version.
        """
        end = time.time() if now is None else float(now)
        clauses = ["open_first_fill_ts<=?"]
        args: list[Any] = [end]
        if since is not None:
            clauses.append("(flat_ts IS NULL OR flat_ts>=?)")
            args.append(float(since))
        if direction is not None:
            clauses.append("direction=?")
            args.append(str(direction))
        if policy_key is not None:
            clauses.append("json_extract(payload_json,'$.policy_key')=?")
            args.append(str(policy_key))
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM cycle_records WHERE " + " AND ".join(clauses)
                + " ORDER BY COALESCE(flat_ts,?) DESC LIMIT ?",
                [*args, end, max(1, min(int(limit), 2000))]).fetchall()
        outcomes, active_ages = [], []
        for row in rows:
            opened = float(row["open_first_fill_ts"])
            flat = _number(row["flat_ts"])
            if flat is None or flat > end:
                active_ages.append(max(end - opened, 0.0))
                continue
            payload = json.loads(row["payload_json"] or "{}")
            outcomes.append({
                "cycle_id": row["cycle_id"], "flat_ts": flat,
                "net_pnl_usd": _number(row["total_cycle_pnl_usd"]),
                "notional_usd": _number(row["notional_usd"]),
                "hold_sec": max(flat - opened, 0.0),
                "exit_mode": row["exit_mode"], "exit_reason": row["exit_reason"],
                "recovery_count": row["recovery_count"],
                "entry_projected_cycle_pnl_usd": payload.get("entry_projected_cycle_pnl_usd", payload.get("entry_expected_cycle_pnl_usd")),
                "entry_reference_state": payload.get("entry_reference_state"),
                "accounting_status": payload.get("accounting_status", "provisional"),
            })
        return {"count": len(outcomes), "outcomes": outcomes,
                "active_count": len(active_ages),
                "active_ages_sec": active_ages,
                "max_active_age_sec": max(active_ages, default=0.0)}

    def cycle_duration_stats(self, *, limit: int = 200,
                             now: Optional[float] = None,
                             since: Optional[float] = None,
                             direction: Optional[str] = None,
                             policy_key: Optional[str] = None) -> dict:
        """Duration estimates with explicit, non-completed survival exposure."""
        result = self.recent_cycle_outcomes(
            limit=limit, now=now, since=since, direction=direction,
            policy_key=policy_key)
        values = sorted(row["hold_sec"] for row in result["outcomes"])
        def percentile(p: float) -> Optional[float]:
            if not values:
                return None
            position = p * (len(values) - 1)
            low, high = math.floor(position), math.ceil(position)
            return values[low] + (values[high] - values[low]) * (position - low)
        return {"count": len(values), "p75_sec": percentile(.75),
                "p90_sec": percentile(.90),
                "active_count": result["active_count"],
                "max_active_age_sec": result["max_active_age_sec"],
                "active_ages_sec": result["active_ages_sec"]}

    def execution_cost_summary(
            self, execution_ids: Optional[Iterable[str]] = None) -> dict:
        """Aggregate realized execution-cost samples for cycle EV telemetry."""
        ids = [str(value) for value in (execution_ids or ()) if value]
        where = ""
        args: list[Any] = []
        if ids:
            where = "WHERE execution_id IN (" + ",".join("?" for _ in ids) + ")"
            args.extend(ids)
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT phase,requested_qty,filled_qty,quoted_top_px,
                           quoted_depth_avg_px,incremental_execution_cost_bps,
                           all_in_cost_bps,realized_vs_marginal_bps,
                           realized_slippage_bps,adverse_move_bps,
                           depth_cost_bps,latency_sec
                      FROM execution_cost_observations {where}
                     ORDER BY ts,id""", args).fetchall()
        phases = {phase: {"observation_count": 0, "filled_count": 0,
                          "filled_qty": 0.0, "realized_cost_usd": 0.0,
                          "incremental_cost_usd": 0.0,
                          "total_cost_usd": 0.0,
                          "all_in_cost_usd": 0.0,
                          "incremental_execution_cost_bps": [],
                          "all_in_cost_bps": [],
                          "realized_vs_marginal_bps": [],
                          "realized_slippage_bps": [],
                          "adverse_move_bps": [], "depth_cost_bps": [],
                          "latency_sec": []}
                  for phase in ("open_first", "open_hedge",
                                "close_first", "close_hedge")}
        for row in rows:
            phase = str(row["phase"] or "")
            if phase not in phases:
                continue
            bucket = phases[phase]
            bucket["observation_count"] += 1
            filled = max(float(row["filled_qty"] or 0.0), 0.0)
            bucket["filled_qty"] += filled
            if filled > 0:
                bucket["filled_count"] += 1
                top = _number(row["quoted_top_px"])
                depth_avg = _number(row["quoted_depth_avg_px"])
                incremental = _number(
                    row["incremental_execution_cost_bps"])
                all_in = (_number(row["all_in_cost_bps"])
                          if row["all_in_cost_bps"] is not None else
                          _number(row["realized_slippage_bps"]))
                if depth_avg is not None and incremental is not None:
                    amount = filled * depth_avg * max(incremental, 0.0) / 1e4
                    # Depth is already in the walk-depth cash flow. This is
                    # the only execution cost charged to economic PnL.
                    bucket["realized_cost_usd"] += amount
                    bucket["incremental_cost_usd"] += amount
                    bucket["total_cost_usd"] += amount
                if top is not None and all_in is not None:
                    bucket["all_in_cost_usd"] += (
                        filled * top * max(all_in, 0.0) / 1e4)
            for key, column in (("incremental_execution_cost_bps",
                                 "incremental_execution_cost_bps"),
                                ("all_in_cost_bps", "all_in_cost_bps"),
                                ("realized_vs_marginal_bps",
                                 "realized_vs_marginal_bps"),
                                ("realized_slippage_bps", "all_in_cost_bps"),
                                ("adverse_move_bps", "adverse_move_bps"),
                                ("depth_cost_bps", "depth_cost_bps"),
                                ("latency_sec", "latency_sec")):
                value = _number(row[column])
                if value is None and key in {"all_in_cost_bps",
                                             "realized_slippage_bps"}:
                    value = _number(row["realized_slippage_bps"])
                if value is not None:
                    bucket[key].append(value)

        def percentile(values: list[float], p: float) -> Optional[float]:
            if not values:
                return None
            values = sorted(values)
            if len(values) == 1:
                return values[0]
            position = p * (len(values) - 1)
            low, high = math.floor(position), math.ceil(position)
            return values[low] if low == high else values[low] + (
                values[high] - values[low]) * (position - low)

        total_incremental_cost = 0.0
        for bucket in phases.values():
            total_incremental_cost += bucket["incremental_cost_usd"]
            for key in ("incremental_execution_cost_bps", "all_in_cost_bps",
                        "realized_vs_marginal_bps",
                        "realized_slippage_bps", "adverse_move_bps",
                        "depth_cost_bps", "latency_sec"):
                values = bucket.pop(key)
                bucket[f"p95_{key}"] = percentile(values, .95)
            bucket["fill_rate"] = (
                bucket["filled_count"] / bucket["observation_count"]
                if bucket["observation_count"] else None)
        total_all_in_cost = sum(
            bucket["all_in_cost_usd"] for bucket in phases.values())
        total_cost = sum(
            bucket["total_cost_usd"] for bucket in phases.values())
        return {"phases": phases, "realized_cost_usd": total_incremental_cost,
                "incremental_cost_usd": total_incremental_cost,
                "all_in_cost_usd": total_all_in_cost,
                "total_cost_usd": total_cost,
                "observation_count": len(rows)}

    def record_minute_sample(self, *, ts: float, payload: dict,
                             source_key: Optional[str] = None) -> None:
        key = source_key or f"live:{int(float(ts))}"
        with self._noncritical_write("minute_sample") as acquired:
            if not acquired:
                return
            self._conn.execute(
                """INSERT INTO minute_samples(source_key,ts,payload_json)
                   VALUES(?,?,?)
                   ON CONFLICT(source_key) DO UPDATE SET
                     ts=excluded.ts,payload_json=excluded.payload_json""",
                (key, float(ts), _json(payload)))

    def execution_fill_edge(self, execution_id: str) -> Optional[float]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT side,qty,price,fee_usd FROM fills "
                "WHERE execution_id=? ORDER BY ts,id", (execution_id,)).fetchall()
        buy_qty = sell_qty = buy_cost = sell_proceeds = 0.0
        buy_fee = sell_fee = 0.0
        for row in rows:
            notional = float(row["qty"]) * float(row["price"])
            if row["side"] == "buy":
                buy_qty += float(row["qty"]); buy_cost += notional
                buy_fee += float(row["fee_usd"])
            else:
                sell_qty += float(row["qty"]); sell_proceeds += notional
                sell_fee += float(row["fee_usd"])
        matched = min(buy_qty, sell_qty)
        if matched <= 0:
            return None
        buy_avg = buy_cost / buy_qty
        sell_avg = sell_proceeds / sell_qty
        return matched * (sell_avg - buy_avg) \
            - buy_fee * matched / buy_qty - sell_fee * matched / sell_qty

    # --------------------------------------------------------- cycle records

    def upsert_cycle(self, payload: dict) -> None:
        """Persist one complete inventory cycle without losing extra telemetry."""
        cycle_id = str(payload.get("cycle_id") or "").strip()
        if not cycle_id:
            raise ValueError("cycle record requires cycle_id")
        values: list[Any] = []
        for column in CYCLE_COLUMNS:
            if column == "cycle_id":
                values.append(cycle_id)
            elif column == "updated_ts":
                values.append(_number(payload.get(column)) or time.time())
            elif column == "status":
                values.append(str(payload.get(column) or "active"))
            elif column in {"direction", "exit_mode", "exit_reason"}:
                value = payload.get(column)
                values.append(None if value in (None, "") else str(value))
            elif column in {"hedge_reserve_breach", "emergency_flag",
                            "partial_first_fill"}:
                values.append(int(bool(payload.get(column))))
            elif column == "recovery_count":
                values.append(max(0, int(payload.get(column) or 0)))
            elif column == "payload_json":
                values.append(_json(payload))
            else:
                values.append(_number(payload.get(column)))
        columns = ",".join(CYCLE_COLUMNS)
        updates = ",".join(
            f"{column}=excluded.{column}" for column in CYCLE_COLUMNS
            if column != "cycle_id")
        with self._noncritical_write("cycle_record") as acquired:
            if not acquired:
                return
            self._conn.execute(
                f"""INSERT INTO cycle_records({columns})
                    VALUES({','.join('?' for _ in CYCLE_COLUMNS)})
                    ON CONFLICT(cycle_id) DO UPDATE SET {updates}""",
                values)

    def load_cycle(self, cycle_id: str) -> Optional[dict]:
        """Load a cycle record, including forward-compatible JSON fields."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM cycle_records WHERE cycle_id=?", (str(cycle_id),)
            ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        for column in CYCLE_COLUMNS:
            if column == "payload_json":
                continue
            value = row[column]
            if column in {"hedge_reserve_breach", "emergency_flag",
                          "partial_first_fill"}:
                value = bool(value)
            payload[column] = value
        return payload

    def cycle_financials(self, execution_ids: Iterable[str], *,
                         opened_ts: Optional[float] = None,
                         until: Optional[float] = None) -> dict:
        """Aggregate authoritative fills and funding for one inventory cycle."""
        ids = [str(value) for value in execution_ids if value]
        end = time.time() if until is None else float(until)
        gross = fees = 0.0
        fill_count = provisional_fee_count = 0
        if ids:
            placeholders = ",".join("?" for _ in ids)
            with self._lock:
                rows = self._conn.execute(
                    f"""SELECT gross_realized_pnl_usd,fee_usd,fee_source,venue,side,qty,price
                           FROM fills
                          WHERE execution_id IN ({placeholders})
                            AND source!='position_baseline'""", ids).fetchall()
            balances = {}
            cashflow = 0.0
            for row in rows:
                signed = float(row["qty"]) * (1 if row["side"] == "buy" else -1)
                balances[row["venue"]] = balances.get(row["venue"], 0.0) + signed
                cashflow -= signed * float(row["price"])
                gross += float(row["gross_realized_pnl_usd"] or 0.0)
                fees += float(row["fee_usd"] or 0.0)
                fill_count += 1
                provisional_fee_count += row["fee_source"] not in {
                    "exchange_api", ZERO_FEE_SOURCE, "not_applicable"}
            if rows and all(abs(qty) <= 1e-10 for qty in balances.values()):
                gross = cashflow
        funding = 0.0
        with self._lock:
            if opened_ts is None:
                row = self._conn.execute(
                    "SELECT COALESCE(SUM(pnl_usd),0) FROM funding_events "
                    "WHERE ts<=?", (end,)).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COALESCE(SUM(pnl_usd),0) FROM funding_events "
                    "WHERE ts>=? AND ts<=?", (float(opened_ts), end)
                ).fetchone()
        funding = float(row[0] or 0.0)
        return {
            "gross_realized_usd": gross,
            "fees_usd": fees,
            "funding_pnl_usd": funding,
            "net_pnl_usd": gross - fees + funding,
            "fill_count": fill_count,
            "provisional_fee_count": provisional_fee_count,
            "fee_accounting_status": ("final" if fill_count and not provisional_fee_count
                                      else "provisional"),
        }

    def cycle_summary(self, *, since: Optional[float], until: float,
                      order_budget_block_sec: float = 0.0) -> dict:
        """Completion/PnL KPIs use flat time; conversion uses the open cohort.

        Explicit since/until defines the full observation window, including
        idle time. Historical queries never count a later close as completed.
        """
        start = 0.0 if since is None else float(since)
        end = float(until)
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM cycle_records
                    WHERE open_first_fill_ts IS NOT NULL
                      AND open_first_fill_ts>=? AND open_first_fill_ts<=?
                    ORDER BY open_first_fill_ts ASC""", (start, end)
            ).fetchall()
        rows = list(rows)
        cohort_completed = [row for row in rows
                            if row["flat_ts"] is not None
                            and float(row["flat_ts"]) <= end]
        with self._lock:
            completed = self._conn.execute(
                "SELECT * FROM cycle_records WHERE open_first_fill_ts<=? "
                "AND flat_ts>=? AND flat_ts<=? ORDER BY flat_ts",
                (end, start, end)).fetchall()
        def _row_mapping(row: Any) -> dict:
            return {key: row[key] for key in row.keys()}

        clean_completed = [row for row in completed
                           if is_clean_circle(_row_mapping(row))]
        recovery_completed = len(completed) - len(clean_completed)
        first_open_ts = min(
            (_number(row["open_first_fill_ts"]) for row in rows),
            default=None)
        circle_hours = (
            max(end - (start if since is not None else first_open_ts), 0.0) / 3600.0
            if since is not None or first_open_ts is not None else 0.0)

        payloads: dict[int, dict] = {}
        for row in [*rows, *completed]:
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            payloads[id(row)] = payload if isinstance(payload, dict) else {}

        campaigns = [payloads[id(row)] for row in rows
                     if payloads[id(row)].get("exit_opportunity_id")]
        filled_campaigns = [payload for payload in campaigns
                            if (_number(payload.get("first_fill_qty")) or 0.0) > 0]
        first_attempt_fills = [payload for payload in filled_campaigns
                               if int(payload.get("retry_count") or 0) == 0]
        fill_durations = [value for value in (
            _number(payload.get("campaign_duration_ms"))
            for payload in filled_campaigns) if value is not None]
        retry_values = [int(payload.get("retry_count") or 0)
                        for payload in filled_campaigns]

        def values(column: str, selected: Iterable[Any] = rows) -> list[float]:
            out: list[float] = []
            for row in selected:
                value = _number(row[column])
                if value is not None:
                    out.append(value)
            return out

        durations = values("hold_sec", completed)
        durations.sort()
        p90_duration_index = max(0, math.ceil(len(durations) * 0.90) - 1)
        unhedged = []
        for row in rows:
            first = _number(row["open_first_fill_ts"])
            hedged = _number(row["hedge_completed_ts"])
            if first is not None and hedged is not None and hedged <= end:
                unhedged.append(max(hedged - first, 0.0))
        unhedged.sort()
        p95_index = max(0, math.ceil(len(unhedged) * 0.95) - 1)

        def exit_bucket(row: Any) -> str:
            """Classify one completed cycle into one mutually exclusive bucket.

            ``exit_mode`` is the current source of truth.  The reason and
            emergency flag are fallbacks for cycles written by older bot
            versions, so the KPI remains restart-safe across migrations.
            """
            mode = str(row["exit_mode"] or "").strip().lower()
            reason = str(row["exit_reason"] or "").strip().lower()
            if mode in {"manual", "manual_flatten"} or reason == "manual_flatten":
                return "manual"
            if mode in {"max_hold", "max_hold_exit"} or reason == "max_hold_exit":
                return "max_hold"
            if mode in {"hard_timeout", "hard_timeout_release"} or reason in {
                    "hard_timeout", "hard_timeout_release"}:
                return "hard_timeout"
            if (mode in {"regime_stop", "risk"}
                    or reason in {"regime_stop", "adverse_drift_exit"}):
                return "regime"
            if (mode in {"execution_emergency", "emergency", "forced"}
                    or bool(row["emergency_flag"])):
                return "execution"
            if (mode in {"sampling", "sampling_exit"}
                    or reason == "sampling_exit"):
                return "sampling"
            if mode in {"stale", "stale_exit"} or reason == "stale_exit":
                return "stale"
            if mode in {"fast", "fast_exit"} or reason == "fast_exit":
                return "fast"
            return "normal"

        buckets = {id(row): exit_bucket(row) for row in completed}
        normal = [row for row in completed if buckets[id(row)] == "normal"]
        fast = [row for row in completed if buckets[id(row)] == "fast"]
        sampling = [row for row in completed
                    if buckets[id(row)] == "sampling"]
        stale = [row for row in completed if buckets[id(row)] == "stale"]
        regime = [row for row in completed if buckets[id(row)] == "regime"]
        max_hold = [row for row in completed
                    if buckets[id(row)] == "max_hold"]
        hard_timeout = [row for row in completed
                        if buckets[id(row)] == "hard_timeout"]
        execution = [row for row in completed
                     if buckets[id(row)] == "execution"]
        manual = [row for row in completed if buckets[id(row)] == "manual"]
        forced = regime + max_hold + hard_timeout + execution

        hold_counts = {"loss_recovery_hold": 0, "long_hold": 0,
                       "forced_release_pending": 0}
        for row in rows:
            hold_state = payloads[id(row)].get("hold_state")
            if hold_state in hold_counts:
                hold_counts[hold_state] += 1

        def average(column: str, selected: Iterable[Any]) -> Optional[float]:
            numbers = values(column, selected)
            return statistics.fmean(numbers) if numbers else None

        gap = []
        for row in completed:
            actual = _number(row["total_cycle_pnl_usd"])
            planned = _number(payloads[id(row)].get(
                "entry_expected_cycle_pnl_usd"))
            if planned is None:
                planned = _number(row["predicted_close_pnl_usd"])
            if actual is not None and planned is not None:
                gap.append(actual - planned)
        normal_pnl = values("total_cycle_pnl_usd", normal)
        completed_count = len(completed)

        def rate(selected: Iterable[Any]) -> float:
            return len(list(selected)) / completed_count if completed_count else 0.0

        def rolling_metrics(limit: int) -> dict:
            selected = completed[-limit:]
            pnl = [value for value in (
                _number(row["total_cycle_pnl_usd"]) for row in selected)
                    if value is not None]
            gaps = []
            capital_hours = 0.0
            for row in selected:
                actual = _number(row["total_cycle_pnl_usd"])
                predicted = _number(payloads[id(row)].get(
                    "entry_expected_cycle_pnl_usd"))
                if predicted is None:
                    predicted = _number(row["predicted_close_pnl_usd"])
                if actual is not None and predicted is not None:
                    gaps.append(actual - predicted)
                notional = _number(row["notional_usd"])
                hold = _number(row["hold_sec"])
                if notional is not None and hold is not None:
                    capital_hours += max(notional, 0.0) * max(hold, 0.0) / 3600.0
            return {
                "count": len(pnl),
                "mean_pnl_usd": statistics.fmean(pnl) if pnl else None,
                "median_pnl_usd": statistics.median(pnl) if pnl else None,
                "win_rate": (sum(value >= -1e-12 for value in pnl) / len(pnl)
                             if pnl else None),
                "ev_per_capital_hour_usd": (
                    sum(pnl) / capital_hours if capital_hours > 0 else None),
                "prediction_error_mean_usd": (
                    statistics.fmean(gaps) if gaps else None),
            }

        # These opportunity counts are intentionally derived from durable
        # execution rows.  The live engine may expose richer in-memory
        # counters, but a restart-safe summary must not depend on RAM.
        with self._lock:
            attempt_rows = self._conn.execute(
                """SELECT execution_id,ts,action,outcome,payload_json
                     FROM trade_attempts
                    WHERE ts>=? AND ts<=?
                    ORDER BY ts ASC""", (start, end)).fetchall()
        open_attempts = [row for row in attempt_rows
                         if str(row["action"] or "").lower() in {"open", "add"}]
        try:
            opportunity_events = self.load_opportunity_events(
                since=start, until=end, limit=20000)
        except Exception:
            # A legacy database may be mid-migration.  The attempt/cycle
            # fallback below still keeps the dashboard readable.
            opportunity_events = []

        def _payload(row: Any) -> dict:
            try:
                value = json.loads(row["payload_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                value = {}
            return value if isinstance(value, dict) else {}

        attempt_payloads = {id(row): _payload(row) for row in attempt_rows}

        def _truth(value: Any) -> Optional[bool]:
            if value is None:
                return None
            if isinstance(value, bool):
                return value
            return _bool(value)

        def _funnel_for_window(window_start: float,
                               window_end: float) -> dict:
            """Build a monotonic, restart-safe execution funnel.

            A signal episode is identified by the durable opportunity id;
            retries are campaigns under that opportunity.  Completed cycle
            records are attached back to their opening execution, and missing
            historical opportunity metadata is represented by a synthetic
            cycle id instead of being silently dropped.
            """
            window_events = [event for event in opportunity_events
                             if window_start <= float(event.get("ts") or 0.0)
                             <= window_end]
            open_stage_names = {
                "statistical_signal", "economic_qualified",
                "economic_rejected", "campaign_started", "first_fill",
                "hedge_complete", "cycle_complete",
            }

            def _event_scope(event: dict) -> str:
                payload = event.get("payload")
                if isinstance(payload, dict) and payload.get("scope"):
                    return str(payload["scope"]).lower()
                stage = str(event.get("stage") or "").lower()
                opportunity_id = str(event.get("opportunity_id") or "")
                return ("exit" if stage.startswith("exit_")
                        or ":exit-" in opportunity_id else "open")

            durable_open_events = [event for event in window_events
                                   if str(event.get("stage") or "").lower()
                                   in open_stage_names
                                   and _event_scope(event) != "exit"]
            if durable_open_events:
                signal_opportunities = {
                    str(event.get("opportunity_id"))
                    for event in durable_open_events
                    if event.get("opportunity_id")
                    and str(event.get("stage") or "").lower()
                    in {"statistical_signal", "economic_qualified",
                       "economic_rejected"}
                }
                economic_opportunities = {
                    str(event.get("opportunity_id"))
                    for event in durable_open_events
                    if event.get("opportunity_id")
                    and str(event.get("stage") or "").lower()
                    == "economic_qualified"
                }
                signal_opportunities.update(
                    str(event.get("opportunity_id"))
                    for event in durable_open_events
                    if event.get("opportunity_id")
                    and str(event.get("stage") or "").lower()
                    in {"campaign_started", "first_fill", "hedge_complete"}
                )
                campaigns_by_id: dict[str, str] = {}
                for event in durable_open_events:
                    stage = str(event.get("stage") or "").lower()
                    if stage not in {"campaign_started", "first_fill",
                                     "hedge_complete"}:
                        continue
                    campaign_id = str(event.get("campaign_id")
                                      or event.get("event_id") or "")
                    opportunity_id = str(event.get("opportunity_id") or "")
                    if campaign_id:
                        campaigns_by_id.setdefault(campaign_id,
                                                    opportunity_id)
                economic_campaigns = {
                    campaign_id for campaign_id, opportunity_id
                    in campaigns_by_id.items()
                    if opportunity_id in economic_opportunities
                }
                fill_campaigns = {
                    str(event.get("campaign_id") or event.get("event_id"))
                    for event in durable_open_events
                    if str(event.get("stage") or "").lower() == "first_fill"
                }
                hedge_campaigns = {
                    str(event.get("campaign_id") or event.get("event_id"))
                    for event in durable_open_events
                    if str(event.get("stage") or "").lower()
                    == "hedge_complete"
                }
                complete_cycles = {
                    str(event.get("cycle_id") or event.get("campaign_id")
                        or event.get("event_id"))
                    for event in durable_open_events
                    if str(event.get("stage") or "").lower()
                    == "cycle_complete"
                }
                # Stage events are the source of truth. Do not clamp one
                # stage to another with min(): a missing transition is itself
                # an observable execution defect.
                campaign_count = len(economic_campaigns)
                open_fill_count = len(fill_campaigns & economic_campaigns)
                hedge_complete_count = len(hedge_campaigns
                                           & economic_campaigns)
                complete_cycle_count = len(complete_cycles)

                exit_events = [event for event in window_events
                               if _event_scope(event) == "exit"]
                exit_opportunities = {
                    str(event.get("opportunity_id"))
                    for event in exit_events if event.get("opportunity_id")
                }
                exit_campaigns = {
                    str(event.get("campaign_id") or event.get("event_id"))
                    for event in exit_events
                    if str(event.get("stage") or "").lower()
                    == "exit_campaign_started"
                }
                exit_fills = {
                    str(event.get("campaign_id") or event.get("event_id"))
                    for event in exit_events
                    if str(event.get("stage") or "").lower() == "exit_fill"
                }
                completed_exits = {
                    str(event.get("cycle_id") or event.get("campaign_id")
                        or event.get("event_id"))
                    for event in exit_events
                    if str(event.get("stage") or "").lower()
                    == "cycle_complete"
                }
                exit_campaign_count = len(exit_campaigns)
                return {
                    "signal_episode_count": len(signal_opportunities),
                    "economic_opportunity_count": len(
                        economic_opportunities),
                    "open_campaign_count": campaign_count,
                    "open_fill_count": open_fill_count,
                    "hedge_complete_count": hedge_complete_count,
                    "complete_cycle_count": complete_cycle_count,
                    "open_campaign_fill_rate": (
                        open_fill_count / campaign_count
                        if campaign_count else 0.0),
                    "hedge_completion_rate": (
                        hedge_complete_count / open_fill_count
                        if open_fill_count else 0.0),
                    "exit_opportunity_count": len(exit_opportunities),
                    "exit_campaign_count": exit_campaign_count,
                    "exit_fill_count": len(exit_fills),
                    "exit_campaign_fill_rate": (
                        len(exit_fills) / exit_campaign_count
                        if exit_campaign_count else 0.0),
                    "completed_exit_count": len(completed_exits),
                }
            window_attempts = [row for row in open_attempts
                               if window_start <= float(row["ts"] or 0.0)
                               <= window_end]
            window_cycles = [row for row in rows
                             if window_start <= (_number(
                                 row["open_first_fill_ts"]) or -1.0)
                             <= window_end]
            window_completed_cycles = [row for row in completed
                                       if window_start <= float(row["flat_ts"])
                                       <= window_end]
            opportunities: dict[str, dict] = {}
            campaigns_by_id: dict[str, dict] = {}
            attempts_by_execution: dict[str, list[str]] = {}

            for row in window_attempts:
                payload = attempt_payloads[id(row)]
                execution_id = str(row["execution_id"] or "")
                opportunity_id = str(
                    payload.get("open_opportunity_id") or execution_id
                    or f"signal:{row['ts']}")
                campaign_id = str(
                    payload.get("open_campaign_id") or execution_id
                    or opportunity_id)
                expected = _number(payload.get("expected_cycle_pnl_usd"))
                floor = _number(payload.get("min_expected_cycle_profit_usd"))
                economic = _truth(payload.get("economic_opportunity"))
                if economic is None:
                    economic = (expected is not None
                                and expected >= (floor or 0.0))
                filled = str(row["outcome"] or "").lower() in {
                    "paired", "partial_paired", "one_leg"}
                opportunities.setdefault(opportunity_id, {
                    "economic": False, "campaign_ids": set()})
                opportunities[opportunity_id]["economic"] = bool(
                    opportunities[opportunity_id]["economic"] or economic)
                opportunities[opportunity_id]["campaign_ids"].add(campaign_id)
                item = campaigns_by_id.setdefault(campaign_id, {
                    "opportunity_id": opportunity_id,
                    "economic": False, "filled": False,
                    "hedge_complete": False, "complete": False,
                })
                item["economic"] = bool(item["economic"] or economic)
                item["filled"] = bool(item["filled"] or filled)
                if execution_id:
                    attempts_by_execution.setdefault(execution_id, []).append(
                        campaign_id)

            # A cycle row is proof that the opening slice reached the durable
            # lifecycle.  Attach it to the attempt when possible; otherwise
            # synthesize one canonical opportunity/campaign for old records.
            for row in window_cycles:
                cycle_id = str(row["cycle_id"] or "")
                campaign_ids = list(attempts_by_execution.get(cycle_id, ()))
                if not campaign_ids:
                    opportunity_id = f"cycle:{cycle_id}:open"
                    campaign_id = f"cycle:{cycle_id}:campaign"
                    opportunities.setdefault(opportunity_id, {
                        "economic": True, "campaign_ids": set()})
                    opportunities[opportunity_id]["economic"] = True
                    opportunities[opportunity_id]["campaign_ids"].add(
                        campaign_id)
                    campaigns_by_id[campaign_id] = {
                        "opportunity_id": opportunity_id,
                        "economic": True, "filled": True,
                        "hedge_complete": False, "complete": False,
                    }
                    campaign_ids = [campaign_id]
                for campaign_id in campaign_ids:
                    item = campaigns_by_id.setdefault(campaign_id, {
                        "opportunity_id": "cycle:" + cycle_id,
                        "economic": True, "filled": True,
                        "hedge_complete": False, "complete": False,
                    })
                    # If a pre-v7 row has no economics payload, the durable
                    # cycle itself is the conservative evidence that an open
                    # was admitted and filled.
                    item["economic"] = True
                    item["filled"] = True
                    item["hedge_complete"] = bool(
                        item["hedge_complete"] or
                        _number(row["hedge_completed_ts"]) is not None)
                    item["complete"] = bool(
                        item["complete"] or row["flat_ts"] is not None)

            economic_opportunities = {
                key for key, value in opportunities.items()
                if value["economic"]}
            economic_campaigns = {
                key: value for key, value in campaigns_by_id.items()
                if value["economic"]
                and value["opportunity_id"] in economic_opportunities}
            open_fills = {key for key, value in economic_campaigns.items()
                          if value["filled"]}
            hedge_complete = {key for key, value in economic_campaigns.items()
                              if value["hedge_complete"]}
            complete_cycles = {key for key, value in economic_campaigns.items()
                               if value["complete"]}
            # The fallback is also a diagnostic surface. Do not clamp one
            # stage to another: a missing transition is the defect the
            # dashboard must expose.
            campaign_count = len(economic_campaigns)
            open_fill_count = len(open_fills)
            hedge_complete_count = len(hedge_complete)
            complete_cycle_count = len(complete_cycles)

            # Exit counters intentionally use one campaign per distinct exit
            # opportunity.  A completed cycle without explicit campaign
            # telemetry gets a synthetic exit opportunity, so the displayed
            # exit funnel cannot claim more completed cycles than exits.
            exit_opportunities: set[str] = set()
            exit_campaigns: set[str] = set()
            exit_fills: set[str] = set()
            for row in window_cycles:
                payload = payloads[id(row)]
                cycle_id = str(row["cycle_id"] or "")
                exit_id = str(payload.get("exit_opportunity_id")
                               or f"cycle:{cycle_id}:exit")
                exit_opportunities.add(exit_id)
                exit_campaigns.add(exit_id)
                if (row["flat_ts"] is not None
                        or (_number(payload.get("first_fill_qty")) or 0.0) > 0):
                    exit_fills.add(exit_id)
            exit_campaign_count = len(exit_campaigns)
            completed_exit_count = len(window_completed_cycles)
            return {
                "signal_episode_count": len(opportunities),
                "economic_opportunity_count": len(economic_opportunities),
                "open_campaign_count": campaign_count,
                "open_fill_count": open_fill_count,
                "hedge_complete_count": hedge_complete_count,
                "complete_cycle_count": complete_cycle_count,
                "open_campaign_fill_rate": (
                    open_fill_count / campaign_count if campaign_count else 0.0),
                "hedge_completion_rate": (
                    hedge_complete_count / open_fill_count
                    if open_fill_count else 0.0),
                "exit_opportunity_count": len(exit_opportunities),
                "exit_campaign_count": exit_campaign_count,
                "exit_fill_count": len(exit_fills),
                "exit_campaign_fill_rate": (
                    len(exit_fills) /
                    exit_campaign_count if exit_campaign_count else 0.0),
                "completed_exit_count": completed_exit_count,
            }

        funnel = _funnel_for_window(start, end)
        window_limits = {"session": None, "15m": 900.0, "1h": 3600.0,
                         "6h": 21600.0, "24h": 86400.0}
        funnel_windows = {
            name: _funnel_for_window(
                start if limit is None else max(start, end - limit), end)
            for name, limit in window_limits.items()
        }

        return {
            "record_count": len(rows),
            "completed_count": len(completed),
            "clean_circle_count": len(clean_completed),
            "recovery_circle_count": recovery_completed,
            "clean_circle_rate": (
                len(clean_completed) / len(completed)
                if completed else 0.0),
            "clean_circles_per_hour": (
                len(clean_completed) / circle_hours
                if circle_hours > 0.0 else 0.0),
            "active_count": len(rows) - len(cohort_completed),
            "cohort_completed_count": len(cohort_completed),
            "completion_rate": (len(cohort_completed) / len(rows) if rows else 0.0),
            "cycles_per_hour": len(completed) / circle_hours if circle_hours > 0 else 0.0,
            "observation_hours": circle_hours,
            "median_cycle_duration_sec": (
                statistics.median(durations) if durations else None),
            "p90_cycle_duration_sec": (
                durations[p90_duration_index] if durations else None),
            "p95_unhedged_sec": (unhedged[p95_index] if unhedged else None),
            "normal_win_rate": (
                sum(value >= -1e-12 for value in normal_pnl) / len(normal_pnl)
                if normal_pnl else None),
            "fast_avg_pnl_usd": average("total_cycle_pnl_usd", fast),
            "sampling_avg_pnl_usd": average(
                "total_cycle_pnl_usd", sampling),
            "stale_avg_pnl_usd": average("total_cycle_pnl_usd", stale),
            "forced_avg_pnl_usd": average("total_cycle_pnl_usd", forced),
            "planned_realized_gap_usd": (
                statistics.fmean(gap) if gap else None),
            "partial_first_fill_rate": (
                sum(bool(row["partial_first_fill"]) for row in rows) / len(rows)
                if rows else 0.0),
            "recovery_rate": (
                sum(int(row["recovery_count"] or 0) > 0 for row in rows)
                / len(rows) if rows else 0.0),
            "emergency_exit_rate": (
                sum(bool(row["emergency_flag"]) for row in rows) / len(rows)
                if rows else 0.0),
            "normal_exit_rate": rate(normal),
            "fast_exit_rate": rate(fast),
            "sampling_exit_rate": rate(sampling),
            "stale_exit_rate": rate(stale),
            "regime_stop_rate": rate(regime),
            "max_hold_exit_rate": rate(max_hold),
            "hard_timeout_exit_rate": rate(hard_timeout),
            "execution_emergency_rate": rate(execution),
            "manual_flatten_rate": rate(manual),
            "loss_recovery_hold_count": hold_counts["loss_recovery_hold"],
            "long_hold_count": hold_counts["long_hold"],
            "forced_release_pending_count": hold_counts[
                "forced_release_pending"],
            "exit_opportunity_count": len(campaigns),
            "exit_campaign_count": len(campaigns),
            "first_attempt_fill_rate": (
                len(first_attempt_fills) / len(campaigns)
                if campaigns else 0.0),
            "campaign_fill_rate": (
                len(filled_campaigns) / len(campaigns)
                if campaigns else 0.0),
            "median_retries_to_fill": (
                statistics.median(retry_values) if retry_values else None),
            "opportunity_to_fill_ms": (
                statistics.median(fill_durations)
                if fill_durations else None),
            "missed_profitable_exit_count": sum(
                str(payload.get("final_result") or "") in {
                    "pnl_deteriorated", "stale_book", "zero_fill"
                } for payload in campaigns),
            "order_budget_block_sec": max(float(order_budget_block_sec), 0.0),
            "rolling_20": rolling_metrics(20),
            "rolling_50": rolling_metrics(50),
            "funnel": funnel,
            "funnel_windows": funnel_windows,
            "signal_episode_count": funnel["signal_episode_count"],
            "economic_opportunity_count": funnel[
                "economic_opportunity_count"],
            "open_campaign_count": funnel["open_campaign_count"],
            "open_fill_count": funnel["open_fill_count"],
            "open_campaign_fill_rate": funnel["open_campaign_fill_rate"],
            "hedge_complete_count": funnel["hedge_complete_count"],
            "hedge_completion_rate": funnel["hedge_completion_rate"],
            "exit_opportunity_count": funnel["exit_opportunity_count"],
            "exit_campaign_count": funnel["exit_campaign_count"],
            "exit_fill_count": funnel["exit_fill_count"],
            "exit_campaign_fill_rate": funnel["exit_campaign_fill_rate"],
            "complete_cycle_count": funnel["complete_cycle_count"],
        }

    # ------------------------------------------------------------- migration

    def migrate_legacy(self, *, trades_paths: Iterable[str | Path] = (),
                        seconds_paths: Iterable[str | Path] = (),
                        minutes_paths: Iterable[str | Path] = ()) -> dict:
        """Idempotently import existing CSV rows into SQLite."""
        counts = {"trades": 0, "seconds": 0, "minutes": 0}
        for path in _unique_paths(trades_paths):
            counts["trades"] += self._migrate_trade_csv(path)
        for path in _unique_paths(seconds_paths):
            counts["seconds"] += self._migrate_sample_csv(path, "market_samples")
        for path in _unique_paths(minutes_paths):
            counts["minutes"] += self._migrate_sample_csv(path, "minute_samples")
        if any(counts.values()):
            log.info("SQLite legacy migration: %s", counts)
        return counts

    def _migrate_sample_csv(self, path: Path, table: str) -> int:
        if not path.exists():
            return 0
        imported = 0
        try:
            with path.open(newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                # Keep the file cursor lazy.  ``seconds.csv`` can be large
                # on a long-running bot; materialising it with list(reader)
                # caused every API or bot restart to retain a large transient
                # allocation even though only one row is needed at a time.
                with self._write():
                    for row_num, row in enumerate(reader, 2):
                        ts = _number(row.get("ts"))
                        if ts is None:
                            ts = _number(row.get("minute_ts"))
                        if ts is None:
                            continue
                        key = _source_key(path, row_num)
                        if self._already_imported(key):
                            continue
                        self._conn.execute(
                            f"INSERT OR IGNORE INTO {table}(source_key,ts,payload_json) "
                            "VALUES(?,?,?)", (key, ts, _json(row)))
                        self._mark_imported(key, path, row_num, table)
                        imported += 1
        except (OSError, csv.Error, UnicodeError):
            return imported
        return imported

    def _migrate_trade_csv(self, path: Path) -> int:
        if not path.exists():
            return 0
        try:
            with path.open(newline="", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
        except (OSError, csv.Error, UnicodeError):
            return 0
        imported = 0
        with self._write():
            for row_num, row in enumerate(rows, 2):
                ts = _number(row.get("ts"))
                if ts is None:
                    continue
                source_key = _source_key(path, row_num)
                if self._already_imported(source_key):
                    continue
                qty = _number(row.get("qty")) or 0.0
                buy_fill = abs(_number(row.get("buy_fill")) or 0.0)
                sell_fill = abs(_number(row.get("sell_fill")) or 0.0)
                payload = {
                    "execution_id": f"legacy:{hashlib.sha1(source_key.encode()).hexdigest()[:16]}",
                    "ts": ts,
                    "action": row.get("action") or "",
                    "direction": row.get("direction") or "",
                    "qty": qty,
                    "buy_notional": _number(row.get("buy_notional")) or 0.0,
                    "sell_notional": _number(row.get("sell_notional")) or 0.0,
                    "buy_fill": buy_fill,
                    "sell_fill": sell_fill,
                    "buy_status": row.get("buy_status") or "unknown",
                    "sell_status": row.get("sell_status") or "unknown",
                    "buy_venue": row.get("buy_venue") or "",
                    "sell_venue": row.get("sell_venue") or "",
                    "threshold_bps": _number(row.get("threshold_bps")),
                    "net_edge_bps": _number(row.get("net_marginal_edge_bps")),
                    "fill_edge_usd": _number(row.get("fill_edge_usd")),
                    "reduce_only": _bool(row.get("reduce_only")),
                }
                payload["outcome"] = _outcome(qty, buy_fill, sell_fill)
                key = payload["execution_id"]
                legacy_row = {**payload, "trade_key": key,
                              "source": "legacy_csv"}
                if self._has_live_trade_copy(legacy_row):
                    self._mark_imported(source_key, path, row_num,
                                        "trade_attempts")
                    continue
                self._conn.execute(
                    """INSERT OR IGNORE INTO trade_attempts(
                        trade_key,execution_id,ts,source,action,direction,qty,
                        buy_notional,sell_notional,buy_fill,sell_fill,buy_status,
                        sell_status,buy_venue,sell_venue,threshold_bps,net_edge_bps,
                        fill_edge_usd,outcome,reduce_only,payload_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (key, payload["execution_id"], ts, "legacy_csv",
                     str(payload["action"]).lower(),
                     str(payload["direction"]).lower(), qty,
                     payload["buy_notional"], payload["sell_notional"],
                     buy_fill, sell_fill, str(payload["buy_status"]),
                     str(payload["sell_status"]),
                     str(payload["buy_venue"]).lower(),
                     str(payload["sell_venue"]).lower(),
                     payload["threshold_bps"], payload["net_edge_bps"],
                     payload["fill_edge_usd"], payload["outcome"],
                     int(payload["reduce_only"]), _json(row)))
                self._mark_imported(source_key, path, row_num,
                                    "trade_attempts")
                imported += 1
        return imported

    def _has_live_trade_copy(self, legacy_row: dict) -> bool:
        ts = _number(legacy_row.get("ts"))
        if ts is None:
            return False
        candidates = self._conn.execute(
            f"""{TRADE_ATTEMPT_SELECT}
                WHERE source!='legacy_csv'
                  AND action=? AND direction=?
                  AND COALESCE(completion_ts, ts) BETWEEN ? AND ?""",
            (str(legacy_row.get("action") or "").lower(),
             str(legacy_row.get("direction") or "").lower(),
             ts - LEGACY_COPY_WINDOW_SEC, ts + LEGACY_COPY_WINDOW_SEC),
        ).fetchall()
        return any(_trade_rows_match_legacy_copy(legacy_row, candidate)
                   for candidate in candidates)

    def _already_imported(self, key: str) -> bool:
        return self._conn.execute(
            "SELECT 1 FROM legacy_imports WHERE source_key=?", (key,)
        ).fetchone() is not None

    def _mark_imported(self, key: str, path: Path, row_num: int,
                       table: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO legacy_imports("
            "source_key,source_file,source_row,table_name,imported_ts) "
            "VALUES(?,?,?,?,?)",
            (key, str(path.resolve()), row_num, table, time.time()))

    # -------------------------------------------------------------- queries

    def list_trades(self, limit: int = 20,
                    coverage_started_ts: Optional[float] = None,
                    offset: int = 0) -> dict:
        since = coverage_started_ts
        page_limit = max(1, min(int(limit), 100))
        page_offset = max(0, int(offset))
        with self._lock:
            where = "WHERE (? IS NULL OR ts >= ?)"
            args = (since, since)
            raw_rows = self._conn.execute(
                f"{TRADE_ATTEMPT_SELECT} {where} "
                "ORDER BY ts DESC, trade_key DESC", args).fetchall()
        all_rows = _deduplicate_trade_rows(raw_rows)
        rows = all_rows[page_offset:page_offset + page_limit]
        summary = {
            "coverage_started_ts": coverage_started_ts,
            "attempt_count": len(all_rows), "matched_count": 0,
            "paired_count": 0, "partial_paired_count": 0,
            "one_leg_count": 0, "no_fill_count": 0, "matched_rate": 0.0,
            "paired_fill_edge_usd": 0.0,
        }
        for row in all_rows:
            outcome = _reported_outcome(row)
            summary[f"{outcome}_count"] += 1
            if outcome in {"paired", "partial_paired"}:
                summary["matched_count"] += 1
                summary["paired_fill_edge_usd"] += (
                    float(row["fill_edge_usd"] or 0.0))
        if summary["attempt_count"]:
            summary["matched_rate"] = (summary["matched_count"]
                                        / summary["attempt_count"])
        execution_ids = [str(row["execution_id"]) for row in rows
                         if row["execution_id"]]
        fee_by_execution: dict[str, dict[str, dict[str, float]]] = {}
        if execution_ids:
            placeholders = ",".join("?" for _ in execution_ids)
            with self._lock:
                fee_rows = self._conn.execute(
                    f"""SELECT execution_id,venue,fee_usd,fee_source,
                              gross_realized_pnl_usd
                         FROM fills
                        WHERE execution_id IN ({placeholders})
                          AND source!='position_baseline'""",
                    execution_ids).fetchall()
            for fee_row in fee_rows:
                execution_id = str(fee_row["execution_id"])
                venue = str(fee_row["venue"] or "")
                bucket = fee_by_execution.setdefault(
                    execution_id, {}).setdefault(
                        venue, {"fee_usd": 0.0, "count": 0.0,
                                "actual_count": 0.0,
                                "gross_realized_pnl_usd": 0.0})
                bucket["fee_usd"] += float(fee_row["fee_usd"] or 0.0)
                bucket["count"] += 1.0
                bucket["gross_realized_pnl_usd"] += float(
                    fee_row["gross_realized_pnl_usd"] or 0.0)
                if fee_row["fee_source"] == "exchange_api":
                    bucket["actual_count"] += 1.0
        return {"summary": summary,
                "total_count": len(all_rows),
                "offset": page_offset,
                "limit": page_limit,
                "has_more": page_offset + len(rows) < len(all_rows),
                "trades": [
                    _trade_dict(row, fee_by_execution.get(
                        str(row["execution_id"]), {})) for row in rows]}

    def trade_summary(self, *, since: Optional[float], until: float,
                      day_timezone: str) -> dict:
        """Return one authoritative count/turnover/last-trade snapshot."""
        if since is None:
            tz = ZoneInfo(day_timezone)
            local_day = datetime.fromtimestamp(until, tz=tz).date()
            since = datetime.combine(
                local_day, datetime.min.time(), tzinfo=tz).timestamp()
        with self._lock:
            attempts = self._conn.execute(
                f"""{TRADE_ATTEMPT_SELECT}
                   WHERE ts>=? AND ts<=?
                   ORDER BY ts ASC, trade_key ASC""",
                (since, until)).fetchall()
            turnover = self._conn.execute(
                """SELECT COALESCE(SUM(qty * price),0) FROM fills
                   WHERE ts>=? AND ts<=? AND source!='position_baseline'""",
                (since, until)).fetchone()[0]
        attempts = list(reversed(_deduplicate_trade_rows(attempts)))
        last = None
        if attempts:
            row = attempts[-1]
            last = {"ts": float(row["ts"]),
                    "action": str(row["action"] or "").lower() or None,
                    "direction": str(row["direction"] or "").lower() or None}
        return {"trade_count": len(attempts),
                "attempt_count": len(attempts),
                "turnover_usd": float(turnover or 0.0),
                "last_trade": last}

    def activity_summary(self, *, since: Optional[float], until: float) -> dict:
        """Summarize recorded executions and fills for a time window.

        This is intentionally based on the durable tables rather than the
        engine's in-memory counters.  A report after a restart therefore
        includes the same trade count and Entropy volume as the dashboard.
        ``entropy_single_leg_*`` means Entropy fills belonging to an attempt
        whose final outcome was one-leg or partial-paired.
        """
        start = 0.0 if since is None else float(since)
        end = float(until)
        if end < start:
            start, end = end, start

        outcome_names = ("paired", "partial_paired", "one_leg", "no_fill")
        counts = {f"{name}_count": 0 for name in outcome_names}
        with self._lock:
            attempts = self._conn.execute(
                f"""{TRADE_ATTEMPT_SELECT}
                    WHERE ts>=? AND ts<=?
                    ORDER BY ts ASC, trade_key ASC""", (start, end)
            ).fetchall()
            fills = self._conn.execute(
                """SELECT execution_id,venue,qty,price,fee_usd,
                          gross_realized_pnl_usd
                     FROM fills
                    WHERE ts>=? AND ts<=?
                      AND source!='position_baseline'""", (start, end)
            ).fetchall()
            funding = self._conn.execute(
                """SELECT COALESCE(SUM(pnl_usd),0) FROM funding_events
                    WHERE ts>=? AND ts<=?""", (start, end)).fetchone()[0]
        attempts = list(reversed(_deduplicate_trade_rows(attempts)))

        unpaired_execution_ids: set[str] = set()
        last_trade = None
        for row in attempts:
            qty = float(row["qty"] or 0.0)
            buy_fill = abs(float(row["buy_fill"] or 0.0))
            sell_fill = abs(float(row["sell_fill"] or 0.0))
            outcome = _reported_outcome(row)
            counts[f"{outcome}_count"] += 1
            if outcome in {"one_leg", "partial_paired"}:
                execution_id = row["execution_id"]
                if execution_id:
                    unpaired_execution_ids.add(str(execution_id))
            last_trade = {
                "ts": float(row["ts"]),
                "action": str(row["action"] or "").lower() or None,
                "direction": str(row["direction"] or "").lower() or None,
            }

        gross_realized = fees = turnover = 0.0
        entropy_qty = entropy_notional = 0.0
        entropy_single_qty = entropy_single_notional = 0.0
        fill_count = entropy_fill_count = 0
        entropy_single_fill_count = 0
        for row in fills:
            qty = abs(float(row["qty"] or 0.0))
            price = float(row["price"] or 0.0)
            notional = qty * price
            fill_count += 1
            turnover += notional
            gross_realized += float(row["gross_realized_pnl_usd"] or 0.0)
            if str(row["venue"] or "").lower() != "entropy":
                continue
            # The dashboard and reports use the configured fee-bearing leg.
            # Lighter is a zero-fee hedge venue for this deployment.
            fees += float(row["fee_usd"] or 0.0)
            entropy_fill_count += 1
            entropy_qty += qty
            entropy_notional += notional
            if str(row["execution_id"] or "") in unpaired_execution_ids:
                entropy_single_fill_count += 1
                entropy_single_qty += qty
                entropy_single_notional += notional

        funding = float(funding or 0.0)
        matched = counts["paired_count"] + counts["partial_paired_count"]
        attempt_count = len(attempts)
        return {
            "since": since,
            "until": end,
            "trade_count": attempt_count,
            "recorded_trade_count": attempt_count,
            "attempt_count": attempt_count,
            "paired_count": counts["paired_count"],
            "partial_paired_count": counts["partial_paired_count"],
            "one_leg_count": counts["one_leg_count"],
            "no_fill_count": counts["no_fill_count"],
            "matched_count": matched,
            "matched_rate": (matched / attempt_count
                              if attempt_count else 0.0),
            "fill_count": fill_count,
            "turnover_usd": turnover,
            "gross_realized_pnl_usd": gross_realized,
            "fees_usd": fees,
            "funding_pnl_usd": funding,
            "realized_net_pnl_usd": gross_realized - fees + funding,
            "entropy_fill_count": entropy_fill_count,
            "entropy_qty": entropy_qty,
            "entropy_notional_usd": entropy_notional,
            "entropy_single_leg_fill_count": entropy_single_fill_count,
            "entropy_single_leg_qty": entropy_single_qty,
            "entropy_single_leg_notional_usd": entropy_single_notional,
            "last_trade": last_trade,
        }

    def equity_pnl_change(self, *, since: float, until: float
                          ) -> Optional[float]:
        """Return the account-equity PnL delta between two snapshot times.

        ``pnl_snapshots.equity_net_pnl_usd`` is relative to the local-day
        baseline, so subtracting two samples gives an hourly change without
        mixing in the day's earlier activity.  If the start boundary has no
        sample yet, the first sample inside the window is used.
        """
        start = float(since)
        end = float(until)
        if end < start:
            start, end = end, start
        with self._lock:
            start_row = self._conn.execute(
                """SELECT ts,equity_total_usd,equity_net_pnl_usd
                     FROM pnl_snapshots
                    WHERE ts<=? AND ts>=?
                      AND (equity_total_usd IS NOT NULL
                           OR equity_net_pnl_usd IS NOT NULL)
                    ORDER BY ts DESC LIMIT 1""", (start, 0.0)
            ).fetchone()
            if start_row is None:
                start_row = self._conn.execute(
                    """SELECT ts,equity_total_usd,equity_net_pnl_usd
                         FROM pnl_snapshots
                        WHERE ts>=? AND ts<=?
                          AND (equity_total_usd IS NOT NULL
                               OR equity_net_pnl_usd IS NOT NULL)
                        ORDER BY ts ASC LIMIT 1""", (start, end)
                ).fetchone()
            end_row = self._conn.execute(
                """SELECT ts,equity_total_usd,equity_net_pnl_usd
                     FROM pnl_snapshots
                    WHERE ts>=? AND ts<=?
                      AND (equity_total_usd IS NOT NULL
                           OR equity_net_pnl_usd IS NOT NULL)
                    ORDER BY ts DESC LIMIT 1""", (start, end)
            ).fetchone()
        if start_row is None or end_row is None:
            return None
        # Absolute equity survives a local-midnight daily-baseline reset.
        # Older rows may not have it, so retain the v1 fallback.
        if (start_row["equity_total_usd"] is not None
                and end_row["equity_total_usd"] is not None):
            return float(end_row["equity_total_usd"]
                         - start_row["equity_total_usd"])
        if (start_row["equity_net_pnl_usd"] is None
                or end_row["equity_net_pnl_usd"] is None):
            return None
        return float(end_row["equity_net_pnl_usd"]
                     - start_row["equity_net_pnl_usd"])

    def equity_lifetime(self, *, now: Optional[float] = None,
                        current_equity: Optional[float] = None) -> dict:
        """Return the durable all-time equity baseline and current delta.

        The engine's session baseline intentionally resets on restart.  The
        dashboard's lifetime view instead anchors to the earliest persisted
        absolute equity sample, so a restart cannot make the total PnL appear
        to return to zero.  ``current_equity`` may be a fresher live value
        than the last SQLite snapshot.
        """
        end = time.time() if now is None else float(now)
        with self._lock:
            first = self._conn.execute(
                """SELECT ts,equity_total_usd FROM pnl_snapshots
                   WHERE ts<=? AND equity_total_usd IS NOT NULL
                   ORDER BY ts,id LIMIT 1""", (end,)).fetchone()
            latest = self._conn.execute(
                """SELECT ts,equity_total_usd FROM pnl_snapshots
                   WHERE ts<=? AND equity_total_usd IS NOT NULL
                   ORDER BY ts DESC,id DESC LIMIT 1""", (end,)).fetchone()
            count = self._conn.execute(
                """SELECT COUNT(*) FROM pnl_snapshots
                   WHERE ts<=? AND equity_total_usd IS NOT NULL""",
                (end,)).fetchone()[0]

        start_ts = float(first["ts"]) if first is not None else None
        start_equity = (_finite_or_none(first["equity_total_usd"])
                        if first is not None else None)
        current = _finite_or_none(current_equity)
        if current is None and latest is not None:
            current = _finite_or_none(latest["equity_total_usd"])
        change = (current - start_equity
                  if current is not None and start_equity is not None
                  else None)
        return {
            "start_ts": start_ts,
            "end_ts": end,
            "start_equity_usd": start_equity,
            "current_equity_usd": current,
            "change_usd": _finite_or_none(change),
            "coverage_complete": start_equity is not None and current is not None,
            "source": "sqlite:pnl_snapshots",
            "sample_count": int(count or 0),
        }

    def history(self, range_name: str, now: Optional[float] = None) -> dict:
        ranges = {"1h": (3600, 2), "6h": (21600, 12), "24h": (86400, 44)}
        seconds, bucket = ranges[range_name]
        now = time.time() if now is None else now
        cutoff = now - seconds
        points: dict[int, dict] = {}
        # Select only the newest sample in each display bucket inside SQLite.
        # The old path parsed every second-level JSON row while holding a read
        # cursor; on a long-lived database that kept a snapshot open for the
        # whole dashboard request and unnecessarily contended with writers.
        with self._lock:
            rows = self._conn.execute(
                """WITH ranked AS (
                       SELECT ts,payload_json,
                              ROW_NUMBER() OVER (
                                  PARTITION BY CAST(ts / ? AS INTEGER)
                                  ORDER BY ts DESC,source_key DESC
                              ) AS row_number
                         FROM market_samples
                        WHERE ts>=? AND ts<=?
                   )
                   SELECT ts,payload_json FROM ranked
                    WHERE row_number=1 ORDER BY ts""",
                (bucket, cutoff, now)).fetchall()
        for row in rows:
            try:
                raw = json.loads(row["payload_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            premium = _number(raw.get("premium_mid_bps"))
            if premium is None:
                continue
            points[int(float(row["ts"]) // bucket)] = {
                "ts": float(row["ts"]),
                "premium_bps": premium,
                "active_midline_bps": _number(raw.get(
                    "active_midline_bps")),
                "short_entry_bps": _number(raw.get("short_entry_bps")),
                "short_exit_bps": _number(raw.get("short_exit_bps")),
                "long_entry_bps": _number(raw.get("long_entry_bps")),
                "long_exit_bps": _number(raw.get("long_exit_bps")),
                "short_center_bps": _number(raw.get("short_center_bps")),
            }
        return {"range": range_name,
                "points": sorted(points.values(), key=lambda x: x["ts"])}

    def record_pnl_snapshot(self, *, ts: float,
                            equity_net_pnl_usd: Optional[float],
                            attribution: dict,
                            equity_total_usd: Optional[float] = None) -> None:
        day = attribution.get("day") or {}
        with self._noncritical_write("pnl_snapshot") as acquired:
            if not acquired:
                return
            self._conn.execute(
                """INSERT INTO pnl_snapshots(
                    sample_second,ts,equity_net_pnl_usd,ledger_net_pnl_usd,
                    gross_realized_usd,unrealized_usd,fees_usd,funding_pnl_usd,
                    unexplained_usd,source,equity_total_usd)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(sample_second) DO UPDATE SET
                    ts=excluded.ts,equity_net_pnl_usd=excluded.equity_net_pnl_usd,
                    ledger_net_pnl_usd=excluded.ledger_net_pnl_usd,
                    gross_realized_usd=excluded.gross_realized_usd,
                    unrealized_usd=excluded.unrealized_usd,
                    fees_usd=excluded.fees_usd,
                    funding_pnl_usd=excluded.funding_pnl_usd,
                    unexplained_usd=excluded.unexplained_usd,
                    source=excluded.source,
                    equity_total_usd=COALESCE(excluded.equity_total_usd,
                                             pnl_snapshots.equity_total_usd)""",
                (int(ts), ts, equity_net_pnl_usd,
                 day.get("ledger_net_pnl_usd"), day.get("gross_realized_usd"),
                 day.get("unrealized_usd"), day.get("fees_usd"),
                 day.get("funding_pnl_usd"), day.get("unexplained_usd"),
                 attribution.get("source", "equity+fill_ledger"),
                 equity_total_usd))

    def _bounded_pnl_rows(self, *, column: str, since: float, until: float,
                          limit: Optional[int] = None) -> list[dict]:
        """Read a PnL series without materialising an unbounded history.

        The default keeps the original full-history behaviour for callers
        that need an audit export.  Dashboard callers pass a display limit;
        SQLite then keeps only the first/last and low/high extrema from each
        time bucket, so a long-lived dashboard request never builds hundreds
        of thousands of Python rows.
        """
        if column not in {"equity_net_pnl_usd", "equity_total_usd"}:
            raise ValueError(f"unsupported PnL column: {column}")
        start = float(since)
        end = float(until)
        if end < start:
            start, end = end, start

        with self._lock:
            first = self._conn.execute(
                f"""SELECT id,ts,{column} AS value FROM pnl_snapshots
                    WHERE ts>=? AND ts<=? AND {column} IS NOT NULL
                    ORDER BY ts,id LIMIT 1""", (start, end)).fetchone()
            if first is None:
                return []
            last = self._conn.execute(
                f"""SELECT id,ts,{column} AS value FROM pnl_snapshots
                    WHERE ts>=? AND ts<=? AND {column} IS NOT NULL
                    ORDER BY ts DESC,id DESC LIMIT 1""", (start, end)).fetchone()
            cap = None if limit is None else max(3, int(limit))
            count = None
            if cap is not None:
                count = self._conn.execute(
                    f"""SELECT COUNT(*) FROM pnl_snapshots
                        WHERE ts>=? AND ts<=? AND {column} IS NOT NULL""",
                    (start, end)).fetchone()[0]

            if cap is None or count <= cap:
                rows = self._conn.execute(
                    f"""SELECT id,ts,{column} AS value FROM pnl_snapshots
                        WHERE ts>=? AND ts<=? AND {column} IS NOT NULL
                        ORDER BY ts,id""", (start, end)).fetchall()
            else:
                bucket_count = max(1, (cap - 2) // 2)
                first_ts = float(first["ts"])
                last_ts = float(last["ts"])
                bucket_width = max(
                    (last_ts - first_ts) / bucket_count, 1e-9)
                rows = self._conn.execute(
                    f"""WITH bucketed AS (
                            SELECT id,ts,{column} AS value,
                                   CAST((ts-?)/? AS INTEGER) AS bucket
                              FROM pnl_snapshots
                             WHERE ts>=? AND ts<=?
                               AND {column} IS NOT NULL
                        ), ranked AS (
                            SELECT id,ts,value,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY bucket
                                       ORDER BY value ASC,ts ASC,id ASC
                                   ) AS low_rank,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY bucket
                                       ORDER BY value DESC,ts DESC,id DESC
                                   ) AS high_rank
                              FROM bucketed
                        )
                        SELECT id,ts,value FROM ranked
                         WHERE low_rank=1 OR high_rank=1
                         ORDER BY ts,id""",
                    (first_ts, bucket_width, start, end)).fetchall()

            selected = {int(first["id"]): first, int(last["id"]): last}
            selected.update({int(row["id"]): row for row in rows})
            ordered = sorted(selected.values(),
                             key=lambda row: (float(row["ts"]),
                                              int(row["id"])))

        points = []
        for row in ordered:
            value = _finite_or_none(row["value"])
            if value is not None:
                points.append({"ts": float(row["ts"]), "value": value})
        return points

    def pnl_points(self, *, since: float, until: float,
                   limit: Optional[int] = None) -> list[dict]:
        return [{"ts": row["ts"], "net_pnl_usd": row["value"]}
                for row in self._bounded_pnl_rows(
                    column="equity_net_pnl_usd", since=since, until=until,
                    limit=limit)]

    def lifetime_pnl_points(self, *, until: Optional[float] = None,
                            limit: Optional[int] = None) -> list[dict]:
        """Return absolute-equity PnL points from the durable lifetime base."""
        end = time.time() if until is None else float(until)
        rows = self._bounded_pnl_rows(
            column="equity_total_usd", since=0.0, until=end, limit=limit)
        if not rows:
            return []
        baseline = rows[0]["value"]
        return [{"ts": row["ts"], "net_pnl_usd": row["value"] - baseline}
                for row in rows]

    # ----------------------------------------------------------- attribution

    def attribution(self, *, now: float, day_start_ts: Optional[float],
                    session_start_ts: Optional[float],
                    marks: dict[str, Optional[float]],
                    equity_day: Optional[float],
                    equity_session: Optional[float],
                    lifetime_start_ts: Optional[float] = None,
                    equity_lifetime: Optional[float] = None) -> dict:
        day = self._attribution_window(day_start_ts, now, marks, equity_day)
        session = self._attribution_window(session_start_ts, now, marks,
                                            equity_session)
        lifetime = self._attribution_window(
            lifetime_start_ts, now, marks, equity_lifetime)
        return {
            "as_of_ts": now,
            "source": "equity+fill_ledger",
            "day": day,
            "session": session,
            "lifetime": lifetime,
        }

    def _attribution_window(self, since: Optional[float], until: float,
                            marks: dict[str, Optional[float]],
                            equity_net: Optional[float]) -> dict:
        start = 0.0 if since is None else float(since)
        with self._lock:
            row = self._conn.execute(
                """SELECT COALESCE(SUM(gross_realized_pnl_usd),0),
                          COALESCE(SUM(fee_usd),0),
                          COUNT(*),
                          COALESCE(SUM(CASE WHEN fee_source!=?
                                            THEN 1 ELSE 0 END),0),
                          COALESCE(SUM(CASE WHEN fee_source='exchange_api'
                                            THEN 1 ELSE 0 END),0),
                          COALESCE(SUM(CASE WHEN fee_source NOT IN
                                            ('exchange_api',?)
                                            THEN 1 ELSE 0 END),0),
                          COALESCE(SUM(CASE WHEN fee_source='exchange_api'
                                            THEN fee_usd ELSE 0 END),0),
                          COALESCE(SUM(CASE WHEN fee_source NOT IN
                                            ('exchange_api',?)
                                            THEN fee_usd ELSE 0 END),0)
                   FROM fills
                   WHERE ts>=? AND ts<=? AND source!='position_baseline'""",
                (ZERO_FEE_SOURCE, ZERO_FEE_SOURCE, ZERO_FEE_SOURCE,
                 start, until)).fetchone()
            funding = self._conn.execute(
                "SELECT COALESCE(SUM(pnl_usd),0) FROM funding_events "
                "WHERE ts>=? AND ts<=?", (start, until)).fetchone()[0]
            all_attempt_rows = self._conn.execute(
                f"""{TRADE_ATTEMPT_SELECT}
                    WHERE ts>=? AND ts<=?
                    ORDER BY ts DESC, trade_key DESC""",
                (start, until)).fetchall()
            legacy_rows = _deduplicate_trade_rows(all_attempt_rows)
            legacy_edge = sum(
                float(row["fill_edge_usd"] or 0.0)
                for row in legacy_rows
                if str(row["source"] or "").lower() == "legacy_csv")
            unrealized = self._unrealized(marks)
        gross = float(row[0] or 0.0)
        fees = float(row[1] or 0.0)
        fill_count = int(row[2] or 0)
        fee_fill_count = int(row[3] or 0)
        actual_fill_count = int(row[4] or 0)
        estimated_fill_count = int(row[5] or 0)
        actual_fee = float(row[6] or 0.0)
        estimated_fee = float(row[7] or 0.0)
        funding = float(funding or 0.0)
        unrealized = float(unrealized)
        ledger_net = gross + unrealized - fees + funding
        unexplained = (None if equity_net is None
                       else float(equity_net) - ledger_net)
        if not fee_fill_count:
            fee_status = "none"
        elif actual_fill_count == fee_fill_count:
            fee_status = "actual"
        elif actual_fill_count:
            fee_status = "mixed"
        else:
            fee_status = "estimated"
        confidence = ("exact_fill_ledger"
                      if fill_count and fee_status in {"actual", "none"}
                      else ("fill_ledger_fees_pending" if fill_count
                            else "equity_only"))
        if legacy_edge and not fill_count:
            confidence = "legacy_edge_plus_equity"
        return {
            "equity_net_pnl_usd": _finite_or_none(equity_net),
            "ledger_net_pnl_usd": ledger_net,
            "gross_realized_usd": gross,
            "unrealized_usd": unrealized,
            "fees_usd": fees,
            "fees_status": fee_status,
            "actual_fee_usd": actual_fee,
            "estimated_fee_usd": estimated_fee,
            "actual_fee_fill_count": actual_fill_count,
            "estimated_fee_fill_count": estimated_fill_count,
            "funding_pnl_usd": funding,
            "unexplained_usd": _finite_or_none(unexplained),
            "legacy_unattributed_edge_usd": float(legacy_edge or 0.0),
            "fill_count": int(fill_count),
            "coverage_start_ts": since,
            "confidence": confidence,
        }

    def _unrealized(self, marks: dict[str, Optional[float]]) -> float:
        total = 0.0
        rows = self._conn.execute(
            "SELECT venue,qty_remaining,price FROM inventory_lots").fetchall()
        for row in rows:
            mark = _number(marks.get(row["venue"]))
            if mark is None:
                continue
            qty = float(row["qty_remaining"])
            price = float(row["price"])
            total += (mark - price) * qty
        return total


def _normalise_exchange_fill(venue: str, row: Any,
                             index: int = 0) -> Optional[dict]:
    """Convert an adapter row into the ledger's exchange-fill contract."""
    if not isinstance(row, dict):
        return None
    ts = _number(row.get("ts", row.get("timestamp", row.get("time"))))
    qty = _number(row.get("qty", row.get("size", row.get("filled_qty"))))
    price = _number(row.get("price", row.get("px")))
    if ts is None or qty is None or price is None or abs(qty) <= 0 \
            or price <= 0:
        return None
    # Adapters normally return seconds, but accepting milliseconds here makes
    # the ledger safe for direct use with raw REST fixtures too.
    if ts > 10_000_000_000:
        ts /= 1000.0
    side_value = str(row.get("side") or "").strip().lower()
    if side_value in {"b", "buy", "bid", "long"}:
        side = "buy"
    elif side_value in {"a", "ask", "sell", "short"}:
        side = "sell"
    else:
        return None
    order_ref = row.get("order_ref", row.get("client_order_id"))
    order_ref = None if order_ref in (None, "") else str(order_ref)
    trade_id = row.get("exchange_trade_id", row.get("trade_id"))
    if trade_id in (None, ""):
        trade_id = row.get("tid", row.get("id"))
    trade_id = None if trade_id in (None, "") else str(trade_id)
    explicit_key = row.get("exchange_fill_key", row.get("fill_key"))
    if explicit_key not in (None, ""):
        key = str(explicit_key)
    elif trade_id:
        key = f"{venue}:{trade_id}"
    else:
        try:
            raw_for_key = _json(row.get("raw", row))
        except (TypeError, ValueError):
            raw_for_key = repr(row.get("raw", row))
        digest = hashlib.sha256(raw_for_key.encode()).hexdigest()
        key = f"{venue}:anonymous:{digest}"
    fee_amount = _number(row.get("fee_amount", row.get("fee")))
    fee_usd = _number(row.get("fee_usd"))
    fee_token = row.get("fee_token", row.get("feeToken"))
    fee_token = None if fee_token in (None, "") else str(fee_token)
    try:
        raw_json = _json(row.get("raw", row))
    except (TypeError, ValueError):
        raw_json = _json({"unserializable_row": repr(row), "index": index})
    return {
        "key": key,
        "venue": str(venue),
        "trade_id": trade_id,
        "order_ref": order_ref,
        "ts": ts,
        "side": side,
        "qty": abs(qty),
        "price": price,
        "fee_amount": fee_amount,
        "fee_usd": fee_usd,
        "fee_token": fee_token,
        "raw_json": raw_json,
    }


def _finite_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    number = _number(value)
    return number


def _source_key(path: Path, row_num: int) -> str:
    return f"{path.resolve()}::{row_num}"


def _unique_paths(paths: Iterable[str | Path]) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for value in paths:
        path = Path(value)
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            out.append(path)
    return out


def _fee_status(count: float, actual_count: float) -> str:
    if count <= 0:
        return "none"
    if actual_count >= count:
        return "actual"
    if actual_count > 0:
        return "mixed"
    return "estimated"


def _leg(row: sqlite3.Row, venue_tokens: tuple[str, ...], side: str,
         qty: float, fee_summary: Optional[dict] = None,
         include_fee: bool = True) -> dict:
    buy_venue = str(row["buy_venue"] or "")
    sell_venue = str(row["sell_venue"] or "")
    if any(token in buy_venue for token in venue_tokens):
        result = {"side": "buy", "planned_qty": qty,
                  "fill_qty": float(row["buy_fill"] or 0.0),
                  "status": str(row["buy_status"] or "unknown")}
        if include_fee:
            venue_key = next((key for key in fee_summary or {}
                              if key and key in buy_venue), None)
            fee = (fee_summary or {}).get(venue_key or "", {})
            result.update({
                "fee_usd": (float(fee.get("fee_usd", 0.0))
                            if fee.get("count", 0.0) else None),
                "fee_status": _fee_status(fee.get("count", 0.0),
                                            fee.get("actual_count", 0.0))})
        else:
            result.update({"fee_usd": None, "fee_status": "none"})
        return result
    if any(token in sell_venue for token in venue_tokens):
        result = {"side": "sell", "planned_qty": qty,
                  "fill_qty": float(row["sell_fill"] or 0.0),
                  "status": str(row["sell_status"] or "unknown")}
        if include_fee:
            venue_key = next((key for key in fee_summary or {}
                              if key and key in sell_venue), None)
            fee = (fee_summary or {}).get(venue_key or "", {})
            result.update({
                "fee_usd": (float(fee.get("fee_usd", 0.0))
                            if fee.get("count", 0.0) else None),
                "fee_status": _fee_status(fee.get("count", 0.0),
                                            fee.get("actual_count", 0.0))})
        else:
            result.update({"fee_usd": None, "fee_status": "none"})
        return result
    return {"side": "unknown", "planned_qty": qty, "fill_qty": 0.0,
            "status": "unknown", "fee_usd": None, "fee_status": "none"}


def _venue_pnl(fill_summary: Optional[dict[str, dict]],
                venue_tokens: tuple[str, ...]) -> Optional[dict[str, float]]:
    """Aggregate FIFO PnL and fees for one venue in one execution.

    ``fills.gross_realized_pnl_usd`` is assigned when a fill consumes a FIFO
    lot, so a reduction attempt is the durable boundary for the PnL
    attribution shown in the dashboard. Position-baseline fills are excluded
    before this helper is called.
    """
    tokens = tuple(str(token).lower() for token in venue_tokens)
    matching = [bucket for venue, bucket in (fill_summary or {}).items()
                if any(token in str(venue).lower() for token in tokens)]
    if not matching:
        return None
    return {
        "gross_realized_pnl_usd": sum(
            float(bucket.get("gross_realized_pnl_usd", 0.0) or 0.0)
            for bucket in matching),
        "fee_usd": sum(float(bucket.get("fee_usd", 0.0) or 0.0)
                       for bucket in matching),
        "fill_count": sum(float(bucket.get("count", 0.0) or 0.0)
                           for bucket in matching),
    }


def _pnl_fields(*, outcome: str, leg_fill_qty: float,
                venue_pnl: Optional[dict[str, float]],
                closing: bool) -> dict:
    """Return explicit per-leg PnL fields for the public API contract."""
    if not closing:
        return {"gross_realized_pnl_usd": None,
                "net_realized_pnl_usd": None,
                "pnl_status": "not_closed"}
    if venue_pnl is None or venue_pnl.get("fill_count", 0.0) <= 0:
        status = ("no_fill" if outcome == "no_fill" and leg_fill_qty <= 0
                  else "unavailable")
        return {"gross_realized_pnl_usd": None,
                "net_realized_pnl_usd": None, "pnl_status": status}
    gross = float(venue_pnl.get("gross_realized_pnl_usd", 0.0) or 0.0)
    fee = float(venue_pnl.get("fee_usd", 0.0) or 0.0)
    return {"gross_realized_pnl_usd": gross,
            "net_realized_pnl_usd": gross - fee,
            "pnl_status": "ledger"}


def _trade_dict(row: sqlite3.Row,
                fee_summary: Optional[dict[str, dict]] = None) -> dict:
    qty = float(row["qty"] or 0.0)
    buy_notional = float(row["buy_notional"] or 0.0)
    sell_notional = float(row["sell_notional"] or 0.0)
    notional = (abs(buy_notional) + abs(sell_notional)) / 2.0
    fill_edge = _number(row["fill_edge_usd"])
    fill_edge_bps = (fill_edge / notional * 1e4
                     if fill_edge is not None and notional else None)
    fee_summary = fee_summary or {}
    entropy_fees = {
        key: value for key, value in fee_summary.items()
        if "entropy" in str(key).lower()
    }
    fee_usd = sum(float(value.get("fee_usd", 0.0))
                  for value in entropy_fees.values())
    fee_count = sum(float(value.get("count", 0.0))
                    for value in entropy_fees.values())
    actual_fee_count = sum(float(value.get("actual_count", 0.0))
                           for value in entropy_fees.values())
    action = str(row["action"] or "").lower()
    outcome = _reported_outcome(row)
    closing = action in {"reduce", "hedge"} or bool(row["reduce_only"])
    entropy_leg = _leg(row, ("entropy",), "buy", qty, fee_summary)
    lighter_leg = _leg(row, ("hedge", "lighter", "rh", "xyz"),
                       "sell", qty, fee_summary, include_fee=False)
    entropy_pnl = _pnl_fields(
        outcome=outcome, leg_fill_qty=float(entropy_leg["fill_qty"]),
        venue_pnl=_venue_pnl(fee_summary, ("entropy",)), closing=closing)
    lighter_pnl = _pnl_fields(
        outcome=outcome, leg_fill_qty=float(lighter_leg["fill_qty"]),
        venue_pnl=_venue_pnl(fee_summary,
                             ("hedge", "lighter", "rh", "xyz")),
        closing=closing)
    entropy_leg.update(entropy_pnl)
    lighter_leg.update(lighter_pnl)
    leg_pnls = (entropy_pnl, lighter_pnl)
    has_both_leg_pnl = all(
        pnl["gross_realized_pnl_usd"] is not None for pnl in leg_pnls)
    gross_total = (sum(float(pnl["gross_realized_pnl_usd"] or 0.0)
                       for pnl in leg_pnls)
                   if has_both_leg_pnl else None)
    net_total = (sum(float(pnl["net_realized_pnl_usd"] or 0.0)
                     for pnl in leg_pnls)
                 if has_both_leg_pnl else None)
    if not closing:
        pnl_status = "not_closed"
    elif has_both_leg_pnl:
        pnl_status = "ledger" if outcome == "paired" else "partial_ledger"
    elif any(pnl["gross_realized_pnl_usd"] is not None for pnl in leg_pnls):
        pnl_status = "partial_ledger"
    elif outcome == "no_fill":
        pnl_status = "no_fill"
    else:
        pnl_status = "unavailable"
    return {
        "ts": float(row["ts"]),
        "execution_id": row["execution_id"],
        "source": row["source"],
        "action": str(row["action"] or ""),
        "direction": str(row["direction"] or ""),
        "notional_usd": notional,
        "qty": qty,
        "threshold_bps": _number(row["threshold_bps"]),
        "net_edge_bps": _number(row["net_edge_bps"]),
        "fill_edge_bps": fill_edge_bps,
        "fill_edge_usd": fill_edge,
        "fees_usd": fee_usd if fee_count else None,
        "fees_status": _fee_status(fee_count, actual_fee_count),
        "outcome": outcome,
        "buy_venue": str(row["buy_venue"] or ""),
        "sell_venue": str(row["sell_venue"] or ""),
        "gross_realized_pnl_usd": gross_total,
        "net_realized_pnl_usd": net_total,
        "pnl_status": pnl_status,
        "entropy_leg": entropy_leg,
        "lighter_leg": lighter_leg,
        "reduce_only": bool(row["reduce_only"]),
    }
