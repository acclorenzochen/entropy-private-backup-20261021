"""Independent, read-only V3 dashboard source collector.

No production Journal constructor or trading runtime is imported here.  A
collector owns short read transactions and publishes immutable snapshots to
HTTP handlers.  The optional market cache belongs only to the dashboard.
"""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import closing
from decimal import Decimal, InvalidOperation
from functools import lru_cache
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Iterator


TABLES = (
    "metadata", "batches", "cycles", "live_cycles", "orders", "live_steps",
    "cycle_economics", "execution_economics_steps", "request_events",
    "funding_cycles", "funding_settlements", "fee_fills",
)
SUPPORTED_ECONOMICS_SCHEMA = {8}


@dataclass(frozen=True)
class SourcePaths:
    journal: Path
    live_status: Path
    live_manifest: Path
    live_diagnostics: Path
    recorder_status: Path
    recorder_manifest: Path
    recorder_data_dir: Path
    reports_dir: Path
    cache_dir: Path

    @classmethod
    def from_env(cls) -> "SourcePaths":
        root = Path(os.getenv("V3_DATA_DIR", "logs/v3"))
        return cls(
            journal=Path(os.getenv("V3_JOURNAL_PATH", str(root / "journal.sqlite3"))),
            live_status=Path(os.getenv("V3_LIVE_STATUS_PATH", str(root / "status.json"))),
            live_manifest=Path(os.getenv("V3_LIVE_MANIFEST_PATH", str(root / "status.effective_manifest.json"))),
            live_diagnostics=Path(os.getenv("V3_LIVE_DIAGNOSTICS_PATH", str(root / "dashboard-diagnostics.json"))),
            recorder_status=Path(os.getenv("V3_RECORDER_STATUS_PATH", str(root / "record-status.edge.json"))),
            recorder_manifest=Path(os.getenv("V3_RECORDER_MANIFEST_PATH", str(root / "record-status.edge.effective_manifest.json"))),
            recorder_data_dir=Path(os.getenv("V3_RECORDER_DATA_DIR", str(root))),
            reports_dir=Path(os.getenv("V3_REPORTS_DIR", "reports/dashboard-v3")),
            cache_dir=Path(os.getenv("DASHBOARD_V3_CACHE_DIR", "logs/dashboard-v3")),
        )


def number(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except (ValueError, InvalidOperation):
        return None
    return result if result.is_finite() else None


def json_source(path: Path, *, max_bytes: int = 2_000_000) -> dict[str, Any]:
    read_at = time.time()
    info: dict[str, Any] = {"read_at": read_at, "observed_at": None,
                            "quality": "unavailable", "reason": None, "schema": None,
                            "session": None, "data": None}
    try:
        if path.stat().st_size > max_bytes:
            raise ValueError("source_too_large")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("not_an_object")
        info.update(data=value, observed_at=value.get("source_ts") or value.get("capture_wall_ts"),
                    schema=value.get("schema_version") or value.get("manifest_version"),
                    session=value.get("session_id") or (
                        f"{value.get('process_id')}:{value.get('start_ts')}:{value.get('build_hash')}"
                        if value.get("start_ts") else None), quality="available")
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        info["reason"] = "missing" if isinstance(exc, FileNotFoundError) else type(exc).__name__
    return info


def manual_exit_source(path: Path) -> dict[str, Any]:
    """Collect only display evidence; never publish account bindings or errors."""
    source = json_source(path)
    raw = source.get('data')
    if raw is None:
        return source
    text_fields = ('mode', 'cycle_id', 'batch_id', 'pair_identity', 'config_hash',
                   'current_state', 'outcome', 'reason', 'account_scope_hash')
    numeric_fields = ('source_ts', 'start_ts', 'position_observed_ts',
                      'quantity_step', 'net_pnl_usd', 'entropy_units', 'hedge_units',
                      'updated_ts', 'last_authoritative_ts', 'filled_units')
    boolean_fields = ('success', 'accounting_final', 'fees_complete',
                      'funding_complete', 'entries_remain_closed', 'authoritative',
                      'position_evidence_authoritative', 'reduce_only')
    def scalar(key: str, value: Any) -> Any:
        if key in numeric_fields:
            parsed = number(value)
            if not isinstance(value, bool) and parsed is not None and -10**18 <= parsed <= 10**18:
                return value
        elif key in boolean_fields:
            if type(value) is bool or type(value) is int and value in (0, 1):
                return bool(value)
        elif (isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_.:-]{1,200}', value)
              and not re.search(r'secret|token|password|credential', value, re.I)):
            return value
        return None
    clean = {key: scalar(key, raw[key]) for key in text_fields + numeric_fields + boolean_fields
             if key in raw and scalar(key, raw[key]) is not None}
    for name, fields in (
            ('remaining', ('entropy_units', 'hedge_units', 'authoritative')),
            ('active_cycle', ('cycle_id', 'batch_id', 'phase', 'entropy_units',
                              'hedge_units', 'updated_ts', 'last_authoritative_ts',
                              'position_evidence_authoritative'))):
        value = raw.get(name)
        if isinstance(value, dict):
            clean[name] = {key: scalar(key, value[key]) for key in fields
                           if key in value and scalar(key, value[key]) is not None}
        elif name in raw and value is None:
            clean[name] = None
    if isinstance(raw.get('pending'), list) and len(raw['pending']) <= 100:
        fields = ('order_ref', 'venue', 'state', 'reduce_only', 'filled_units')
        clean['pending'] = [{key: scalar(key, row[key]) for key in fields
                             if key in row and scalar(key, row[key]) is not None}
                            for row in raw['pending'] if isinstance(row, dict)]
        if len(clean['pending']) != len(raw['pending']):
            clean.pop('pending')
    source['data'] = clean
    source.update(observed_at=clean.get('source_ts'), schema=None, session=None)
    return source


def journal_source(path: Path) -> dict[str, Any]:
    """Read all bounded fact tables in one short, query-only transaction."""
    result: dict[str, Any] = {"read_at": time.time(), "observed_at": None,
                              "quality": "unavailable", "reason": None,
                              "schema": None, "session": None, "data": None}
    if not path.is_file():
        result["reason"] = "missing"
        return result
    connection: sqlite3.Connection | None = None
    try:
        # mode=ro neither creates the file nor enables migrations.  Do not use
        # immutable=1 for the live WAL database.
        connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True,
                                     timeout=0.5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "metadata" not in tables:
            raise ValueError("missing_metadata")
        metadata = dict(connection.execute("SELECT key, value FROM metadata"))
        schema = number(metadata.get("economics_schema_version"))
        result["schema"] = int(schema) if schema is not None else None
        if result["schema"] not in SUPPORTED_ECONOMICS_SCHEMA:
            result.update(quality="unsupported_schema", reason="economics_schema_version")
            return result
        data = {table: [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]
                if table in tables else [] for table in TABLES if table != "metadata"}
        data["metadata"] = metadata
        latest = max((float(row["updated_ts"]) for row in data["live_cycles"]
                      if row.get("updated_ts") is not None), default=None)
        result.update(data=data, observed_at=latest, quality="available",
                      session=metadata.get("identity"))
    except (sqlite3.Error, OSError, ValueError) as exc:
        result.update(quality="unavailable", reason=type(exc).__name__)
    finally:
        if connection is not None:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            connection.close()
    return result


def _quote(levels: Any, units: int) -> Decimal | None:
    if not isinstance(levels, list) or units <= 0:
        return None
    remaining = units
    cash = Decimal(0)
    for level in levels:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            return None
        px = number(level[0])
        qty = level[1] if type(level[1]) is int else number(level[1])
        if (px is None or qty is None or px <= 0 or qty <= 0
                or not isinstance(qty, int) and qty != qty.to_integral_value()):
            return None
        if remaining == units and qty >= units:
            return px
        take = min(int(qty), remaining)
        cash += px * take
        remaining -= take
        if not remaining:
            return cash / units
    return None


def compact_market(row: dict[str, Any]) -> dict[str, Any]:
    """Retain enough depth for normal fixed-q research without raw 270-level books.

    If a requested q needs omitted levels, the API reports an explicit cache
    depth gap; it does not claim the exchange itself lacked depth.
    """
    keep = ("kind", "schema_version", "pair_id", "session_id", "sample_seq",
            "capture_wall_ts", "valid", "quality_reason", "continuity_break",
            "coverage_reason", "reference_bps", "mid_basis_bps",
            "reference_usable", "reference_source", "config_hash",
            "minimum_entry_units", "_dashboard_cache_key")
    result = {key: row.get(key) for key in keep if key in row}
    instrument = row.get('instrument')
    step = number(instrument.get('quantity_step')) if isinstance(instrument, dict) else None
    if step is None and row.get("quantity_step_source") == "recorded_instrument":
        step = number(row.get("quantity_step"))
    if step is not None and step > 0:
        result.update(quantity_step=str(step), quantity_step_source="recorded_instrument")
    truncated = bool(row.get("_depth_truncated"))
    for venue in ("entropy", "lighter"):
        book = row.get(venue)
        if isinstance(book, dict):
            result[venue] = {}
            for side in ("bids", "asks"):
                levels = book.get(side)
                if isinstance(levels, list):
                    truncated |= len(levels) > 32
                    result[venue][side] = levels[:32]
    result["_depth_truncated"] = truncated
    return result


def compact_live_market(row: dict[str, Any]) -> dict[str, Any] | None:
    """Only persisted summary evidence; never promote it to executable depth."""
    pair = row.get("pair_id") or row.get("pair_identity")
    ts = number(row.get("capture_wall_ts", row.get("source_ts")))
    if (row.get("kind") != "v3_market" or ts is None or ts <= 0 or ts > 253402300800
            or not isinstance(pair, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", pair)):
        return None
    def bps(value):
        if isinstance(value, bool) or isinstance(value, str) and len(value) > 128:
            return None
        parsed = number(value)
        return str(parsed) if parsed is not None and abs(parsed) <= 10**12 else None
    session = row.get("session_id")
    session = session if isinstance(session, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", session) else None
    usable = row.get("reference_usable") is True
    mid = bps(row.get("mid_basis_bps", row.get("basis_bps")))
    result = {"kind": "v3_market", "pair_id": pair, "session_id": session,
              "capture_wall_ts": float(ts), "mid_basis_bps": mid,
              "reference_bps": bps(row.get("reference_bps")) if usable else None,
              "reference_usable": usable, "valid": mid is not None,
              "continuity_break": row.get("continuity_break") is True}
    for key in ("_source_span_id", "_dashboard_cache_key"):
        if isinstance(row.get(key), str):
            result[key] = row[key]
    return result


@lru_cache(maxsize=4096)
def _chart_bps(value):
    if value is None:
        return None
    try:
        return str(value.quantize(Decimal('0.000001')))
    except InvalidOperation:
        return None


@lru_cache(maxsize=4096)
def _quote_math(e_buy, e_sell, h_buy, h_sell, reference):
    # Immutable input prices/reference fully determine this display math.
    # Bounded memoization reuses the repeated prices between book changes;
    # every frame still validates/quotes its own quantity and retained depth.
    buy = _chart_bps((e_buy/h_sell-1)*10000) if e_buy is not None and h_sell is not None else None
    sell = _chart_bps((e_sell/h_buy-1)*10000) if e_sell is not None and h_buy is not None else None
    return (buy, sell, _chart_bps(reference),
            _chart_bps(reference-Decimal(buy)) if reference is not None and buy is not None else None,
            _chart_bps(Decimal(sell)-reference) if reference is not None and sell is not None else None)


def requote_market(row: dict[str, Any], units: int, entry_band: Any = None) -> dict[str, Any]:
    """Fixed-q research quote from one recorder frame, never a live decision."""
    mid = number(row.get("mid_basis_bps"))
    out = {"ts": row.get("capture_wall_ts"), "session_id": row.get("session_id"),
           "sample_seq": row.get("sample_seq"), "valid": bool(row.get("valid")),
           "quality_reason": row.get("quality_reason"),
           "schema_version": row.get("schema_version"),
           "continuity_break": bool(row.get("continuity_break")),
           "buy_basis_bps": None, "sell_basis_bps": None,
           "mid_basis_bps": _chart_bps(mid),
           "reference_bps": None, "long_edge_bps": None, "short_edge_bps": None,
           "entry_band_bps": str(entry_band) if entry_band is not None else None}
    if not out["valid"]:
        return out
    entropy, hedge = row.get("entropy"), row.get("lighter")
    if not isinstance(entropy, dict) or not isinstance(hedge, dict):
        out["quality_reason"] = "missing_book"
        return out
    e_buy, e_sell = _quote(entropy.get("asks"), units), _quote(entropy.get("bids"), units)
    h_buy, h_sell = _quote(hedge.get("asks"), units), _quote(hedge.get("bids"), units)
    reference = number(row.get("reference_bps")) if row.get('reference_usable') is not False else None
    if reference is None and row.get('reference_usable') is not False and isinstance(row.get("reference"), dict):
        reference = number(row["reference"].get("basis_bps"))
    out.update(zip(('buy_basis_bps', 'sell_basis_bps', 'reference_bps',
                    'long_edge_bps', 'short_edge_bps'), _quote_math(e_buy, e_sell, h_buy, h_sell, reference)))
    if out["buy_basis_bps"] is None and out["sell_basis_bps"] is None:
        out["quality_reason"] = ("cache_depth_truncated_for_q" if row.get("_depth_truncated")
                                 else "insufficient_depth_for_fixed_q")
    return out


class MarketTail:
    """Tail rotating recorder files by inode and byte offset, including partial lines."""

    samples_table = "market_samples"
    checkpoints_table = "source_checkpoints"
    backfill_table = "prefix_backfill"
    file_glob = "record-observer.edge*.jsonl"
    active_name = "record-observer.edge.jsonl"
    compact = staticmethod(compact_market)

    def __init__(self, directory: Path, cache_dir: Path) -> None:
        self.directory = directory
        self.cache_dir = cache_dir
        self.offsets: dict[tuple[int, int], int] = {}
        self.fragments: dict[tuple[int, int], bytes] = {}
        self.seen: set[tuple[str, str, int]] = set()
        self.last_sequence: dict[tuple[str, str], int] = {}
        self.last_timestamp: dict[tuple[str, str], float] = {}
        self.prefix_targets: dict[tuple[int, int], int] = {}
        self.points: list[dict[str, Any]] = []
        self.quality = "unavailable"
        self.reason: str | None = "missing"
        self.observed_at: float | None = None
        self.schema: int | None = None
        self.legacy_seen = False
        self.unsupported_schemas: set[str] = set()
        self.backfill_complete = False
        self._cache_loaded = False
        self.cache_error: str | None = None
        self.revision = 0

    def _accept(self, row, identity, offset):
        return row.get("kind") == "v3_market_snapshot"

    def _load_state(self, db):
        pass

    def _save_state(self, db):
        pass

    def _truncated(self, identity):
        pass

    def _cache_connection(self) -> sqlite3.Connection:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.cache_dir / "readmodel.sqlite3", timeout=0.5)
        # Long fixed-q history reads must not block the collector's inserts.
        # This WAL belongs only to the disposable dashboard cache, never the
        # production Journal.
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(f"CREATE TABLE IF NOT EXISTS {self.samples_table}("
                   "sample_key TEXT PRIMARY KEY, capture_ts REAL, payload TEXT NOT NULL)")
        db.execute(f"CREATE INDEX IF NOT EXISTS {self.samples_table}_ts ON {self.samples_table}(capture_ts)")
        db.execute(f"CREATE TABLE IF NOT EXISTS {self.checkpoints_table}("
                   "file_id TEXT PRIMARY KEY, byte_offset INTEGER NOT NULL, fragment BLOB NOT NULL)")
        db.execute(f"CREATE TABLE IF NOT EXISTS {self.backfill_table}("
                   "file_id TEXT PRIMARY KEY, target_offset INTEGER NOT NULL)")
        return db

    def _load_cache(self) -> None:
        if self._cache_loaded:
            return
        self._cache_loaded = True
        try:
            with closing(self._cache_connection()) as db, db:
                for file_id, offset, fragment in db.execute(
                        f"SELECT file_id, byte_offset, fragment FROM {self.checkpoints_table}"):
                    device, inode = (int(part) for part in file_id.split(":"))
                    self.offsets[(device, inode)] = offset
                    self.fragments[(device, inode)] = bytes(fragment)
                for file_id, target in db.execute(
                        f"SELECT file_id, target_offset FROM {self.backfill_table}"):
                    device, inode = (int(part) for part in file_id.split(":"))
                    self.prefix_targets[(device, inode)] = target
                rows = db.execute(f"SELECT payload FROM {self.samples_table} "
                                  "ORDER BY capture_ts DESC LIMIT 5000").fetchall()
                self._load_state(db)
            for (payload,) in reversed(rows):
                row = self.compact(json.loads(payload))
                if row is None:
                    continue
                self.points.append(row)
                if row.get("schema_version") == 2 and isinstance(row.get("sample_seq"), int):
                    key = (str(row.get("pair_id")), str(row.get("session_id")),
                           row["sample_seq"])
                    self.seen.add(key)
                    self.last_sequence[key[:2]] = max(
                        self.last_sequence.get(key[:2], 0), key[2])
                stamp = number(row.get("capture_wall_ts"))
                if stamp is not None:
                    self.observed_at = max(self.observed_at or 0, float(stamp))
                    self.last_timestamp[(str(row.get('pair_id')), str(row.get('session_id') or row.get('_source_span_id')))] = float(stamp)
                self.schema = row.get("schema_version")
                self.legacy_seen |= self.schema == 1
            if rows:
                self.quality, self.reason = "available", None
        except (OSError, sqlite3.Error, ValueError, json.JSONDecodeError) as exc:
            self.cache_error = type(exc).__name__

    def _persist(self, fresh: list[dict[str, Any]]) -> None:
        try:
            with closing(self._cache_connection()) as db, db:
                db.executemany(
                    f"INSERT OR IGNORE INTO {self.samples_table} VALUES (?, ?, ?)",
                    ((row["_dashboard_cache_key"], row.get("capture_wall_ts"),
                      json.dumps(row, separators=(",", ":"), ensure_ascii=False))
                     for row in fresh))
                db.executemany(f"INSERT OR REPLACE INTO {self.checkpoints_table} VALUES (?, ?, ?)",
                               ((f"{dev}:{ino}", offset, self.fragments.get((dev, ino), b""))
                                for (dev, ino), offset in self.offsets.items()))
                db.execute(f"DELETE FROM {self.backfill_table}")
                db.executemany(f"INSERT INTO {self.backfill_table} VALUES (?, ?)",
                               ((f"{dev}:{ino}", target)
                                for (dev, ino), target in self.prefix_targets.items()))
                self._save_state(db)
            self.cache_error = None
            if fresh:
                self.revision += 1
        except (OSError, sqlite3.Error) as exc:
            self.cache_error = type(exc).__name__

    def iter_window(self, from_ts: float | None, to_ts: float | None,
                    pair: str | None = None) -> Iterator[dict[str, Any]]:
        """Stream cached depth in time order without loading raw books into RAM.

        The cache is dashboard-owned.  HTTP requests never create or mutate it;
        a missing cache is handled by the caller's published memory snapshot.
        """
        db_path = self.cache_dir / "readmodel.sqlite3"
        if not db_path.is_file():
            raise FileNotFoundError(db_path)
        with closing(sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True,
                                     timeout=0.5)) as db:
            db.execute("PRAGMA query_only=ON")
            bounds = [(clause, value) for clause, value in (
                ("capture_ts >= ?", from_ts), ("capture_ts < ?", to_ts)) if value is not None]
            where = " WHERE " + " AND ".join(clause for clause, _ in bounds) if bounds else ""
            rows = db.execute(
                f"SELECT payload FROM {self.samples_table}" + where + " ORDER BY capture_ts, sample_key",
                [value for _, value in bounds],
            )
            previous_session: tuple[str, str] | None = None
            previous_seq: int | None = None
            for (payload,) in rows:
                raw = json.loads(payload)
                # Existing compact recorder rows already contain the retained
                # depth and provenance fields. Do not clone four books again
                # for every HTTP frame. Older rich cache rows still compact.
                row = (raw if self.samples_table == 'market_samples'
                    and type(raw.get('_depth_truncated')) is bool and 'instrument' not in raw
                    else self.compact(raw))
                if row is None:
                    continue
                if pair is None or row.get("pair_id") == pair:
                    session = (str(row.get("pair_id")), str(row.get("session_id") or row.get("_source_span_id")))
                    seq = row.get("sample_seq")
                    # Cold-start prefix backfill is read after the current tail.
                    # Its persisted sequence/clock breaks describe read order,
                    # not capture order. Re-evaluate those in time order here.
                    artifact = row.get("coverage_reason") in {
                        "sequence_gap_or_reordered_backfill", "capture_clock_rollback"}
                    continuity_break = bool(row.get("continuity_break")) and not artifact
                    if previous_session is not None:
                        if session != previous_session or (
                                row.get("schema_version") == 2 and isinstance(seq, int)
                                and previous_seq is not None and seq != previous_seq + 1):
                            continuity_break = True
                    row["continuity_break"] = continuity_break
                    if artifact:
                        row["coverage_reason"] = (
                            "sequence_gap_in_capture_order" if continuity_break else None)
                    previous_session = session
                    previous_seq = seq if isinstance(seq, int) else None
                    yield row

    def window_signature(self, from_ts, to_ts):
        """Append-only collector cache identity; stable for an unchanged historical range."""
        path = self.cache_dir / "readmodel.sqlite3"
        with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=0.5)) as db:
            db.execute("PRAGMA query_only=ON")
            bounds = [(clause, value) for clause, value in (("capture_ts >= ?", from_ts),
                        ("capture_ts < ?", to_ts)) if value is not None]
            where = " WHERE " + " AND ".join(c for c, _ in bounds) if bounds else ""
            return (self.revision, *db.execute(
                f"SELECT COUNT(*), MIN(capture_ts), MAX(capture_ts) FROM {self.samples_table}" + where,
                [value for _, value in bounds]).fetchone())

    def poll(self, *, max_bytes: int = 2_000_000) -> None:
        self._load_cache()
        fresh: list[dict[str, Any]] = []
        try:
            paths = [p for p in self.directory.glob(self.file_glob) if p.is_file()]
        except OSError as exc:
            self.reason = type(exc).__name__
            return
        if not paths:
            self.quality, self.reason = ("partial" if self.points else "unavailable"), "missing"
            self.backfill_complete = True
            return
        # The active file first makes the live view useful during cold backfill.
        paths.sort(key=lambda p: (p.name != self.active_name, p.name))
        work: list[tuple[Path, bool]] = []
        for path in paths:
            work.append((path, False))
            try:
                stat = path.stat()
                if (stat.st_dev, stat.st_ino) in self.prefix_targets:
                    work.append((path, True))
            except OSError:
                continue
        budget = max_bytes
        more = False
        for path, prefix in work:
            if budget <= 0:
                more = True
                break
            try:
                stat = path.stat()
                actual_identity = (stat.st_dev, stat.st_ino)
                identity = (stat.st_dev, -stat.st_ino) if prefix else actual_identity
                offset = self.offsets.get(identity, 0)
                if (not prefix and identity not in self.offsets and
                        path.name == self.active_name and
                        stat.st_size > max_bytes):
                    # Cold start from the current tail; older bytes are marked
                    # uncovered until a separate backfill catches up.
                    offset = stat.st_size - max_bytes
                    with path.open("rb") as source:
                        source.seek(offset)
                        source.readline()
                        offset = source.tell()
                    self.prefix_targets[actual_identity] = offset
                    more = True
                if stat.st_size < offset:
                    offset = 0
                    self.fragments.pop(identity, None)
                    self._truncated(identity)
                with path.open("rb") as source:
                    source.seek(offset)
                    read_size = (min(budget, self.prefix_targets.get(actual_identity, 0) - offset)
                                 if prefix else budget)
                    chunk = source.read(max(0, read_size))
                self.offsets[identity] = offset + len(chunk)
                budget -= len(chunk)
                if (prefix and self.offsets[identity] <
                        self.prefix_targets.get(actual_identity, 0)) or (
                        not prefix and self.offsets[identity] < stat.st_size):
                    more = True
                prior_fragment = self.fragments.pop(identity, b"")
                line_offset = offset - len(prior_fragment)
                raw = prior_fragment + chunk
                lines = raw.split(b"\n")
                if raw and not raw.endswith(b"\n"):
                    self.fragments[identity] = lines.pop()
                if prefix and self.offsets[identity] >= self.prefix_targets[actual_identity]:
                    self.prefix_targets.pop(actual_identity, None)
                for line in lines:
                    if not line:
                        line_offset += 1
                        continue
                    try:
                        row = json.loads(line)
                    except (ValueError, UnicodeError):
                        line_offset += len(line) + 1
                        continue
                    if not isinstance(row, dict) or not self._accept(row, identity, line_offset):
                        line_offset += len(line) + 1
                        continue
                    schema = row.get("schema_version")
                    if self.samples_table == "market_samples" and schema not in (1, 2):
                        self.unsupported_schemas.add(str(schema))
                        line_offset += len(line) + 1
                        continue
                    if schema == 2:
                        seq = row.get("sample_seq")
                        if not isinstance(seq, int):
                            row["continuity_break"] = True
                        else:
                            pair_session = (str(row.get("pair_id")), str(row.get("session_id")))
                            key = (*pair_session, seq)
                            if key in self.seen:
                                line_offset += len(line) + 1
                                continue
                            self.seen.add(key)
                            previous = self.last_sequence.get(pair_session)
                            if previous is not None and seq != previous + 1:
                                row["continuity_break"] = True
                                row["coverage_reason"] = "sequence_gap_or_reordered_backfill"
                            self.last_sequence[pair_session] = seq
                            stamp = number(row.get("capture_wall_ts"))
                            before = self.last_timestamp.get(pair_session)
                            if stamp is not None:
                                if before is not None and float(stamp) < before:
                                    row["continuity_break"] = True
                                    row["coverage_reason"] = "capture_clock_rollback"
                                self.last_timestamp[pair_session] = float(stamp)
                    elif schema == 1:
                        row["continuity_break"] = True  # sequence continuity unproven
                    row["_dashboard_cache_key"] = row.get("_dashboard_cache_key") or (
                        f"v2:{row.get('pair_id')}:{row.get('session_id')}:{row.get('sample_seq')}"
                        if schema == 2 and isinstance(row.get("sample_seq"), int)
                        else f"v1:{identity[0]}:{identity[1]}:{line_offset}")
                    row = self.compact(row)
                    if row is None:
                        line_offset += len(line) + 1
                        continue
                    self.points.append(row)
                    fresh.append(row)
                    line_offset += len(line) + 1
                    self.schema = schema
                    stamp = number(row.get("capture_wall_ts"))
                    if stamp is not None:
                        self.observed_at = max(self.observed_at or 0, float(stamp))
                    self.quality, self.reason = "available", None
                    if schema == 1:
                        self.legacy_seen = True
            except OSError as exc:
                self.quality, self.reason = "partial", type(exc).__name__
        self.backfill_complete = not more and not self.prefix_targets
        if not self.points and self.backfill_complete and self.reason == 'missing':
            self.reason = 'no_samples'
        if self.legacy_seen and self.quality == "available":
            self.quality, self.reason = "partial", "legacy_schema1_sequence_unproven"
        if self.unsupported_schemas:
            self.quality, self.reason = "unsupported_schema", ",".join(sorted(self.unsupported_schemas))
        self._persist(fresh)
        # Full retained history remains in the dashboard-owned disk cache.
        if len(self.points) > 5_000:
            self.points.sort(key=lambda row: float(row.get("capture_wall_ts") or 0))
            self.points = self.points[-5_000:]
        if len(self.seen) > 100_000:
            self.seen = {(str(row.get("pair_id")), str(row.get("session_id")),
                          row["sample_seq"])
                         for row in self.points if row.get("schema_version") == 2
                         and isinstance(row.get("sample_seq"), int)}


class LiveMarketTail(MarketTail):
    """Persist white-listed live mid/reference summaries in separate cache tables."""

    samples_table = "live_market_samples"
    checkpoints_table = "live_source_checkpoints"
    backfill_table = "live_prefix_backfill"
    file_glob = "observer*.jsonl"
    active_name = "observer.jsonl"
    compact = staticmethod(compact_live_market)

    def __init__(self, directory, cache_dir):
        super().__init__(directory, cache_dir)
        self._startup_boundary = set()

    def _cache_connection(self):
        db = super()._cache_connection()
        db.execute('CREATE TABLE IF NOT EXISTS live_pending_boundaries(file_id TEXT PRIMARY KEY)')
        return db

    def _load_state(self, db):
        self._startup_boundary = {tuple(int(part) for part in row[0].split(':'))
                                  for row in db.execute('SELECT file_id FROM live_pending_boundaries')}

    def _save_state(self, db):
        db.execute('DELETE FROM live_pending_boundaries')
        db.executemany('INSERT INTO live_pending_boundaries VALUES (?)',
                       [(f'{dev}:{ino}',) for dev, ino in self._startup_boundary])

    def _truncated(self, identity):
        self._startup_boundary.add(identity)

    def _accept(self, row, identity, offset):
        if row.get("kind") == "v3_batch_startup":
            self._startup_boundary.add(identity)
            return False
        clean = compact_live_market(row)
        if clean is None:
            return False
        span = f"file:{identity[0]}:{abs(identity[1])}"
        clean.update(_source_span_id=span,
                     _dashboard_cache_key=f"live:{identity[0]}:{abs(identity[1])}:{offset}:{clean['capture_wall_ts']}")
        # Prefix backfill has its own read-order clock, never compared to the
        # freshly bootstrapped tail. A real rollback within either is retained.
        clock_span = clean['session_id'] or span
        clock_key = (clean['pair_id'], clock_span if identity[1] >= 0 else clock_span + ':prefix')
        before = self.last_timestamp.get(clock_key)
        if before is not None and clean['capture_wall_ts'] < before:
            clean['continuity_break'] = True
        self.last_timestamp[clock_key] = clean['capture_wall_ts']
        if identity in self._startup_boundary:
            clean["continuity_break"] = True
            self._startup_boundary.discard(identity)
        row.clear()
        row.update(clean)
        return True


class V3ReadModel:
    def __init__(self, paths: SourcePaths | None = None) -> None:
        self.paths = paths or SourcePaths.from_env()
        self.market = MarketTail(self.paths.recorder_data_dir, self.paths.cache_dir)
        self.live_market = LiveMarketTail(self.paths.live_status.parent, self.paths.cache_dir)
        self.snapshot: dict[str, Any] = {"id": "uncollected", "generated_at": time.time(),
                                         "sources": {}, "journal": None, "market": [], "live_market": []}
        self.sequence = 0

    def refresh(self) -> None:
        sources = {
            "live": json_source(self.paths.live_status),
            "live_manifest": json_source(self.paths.live_manifest),
            "live_diagnostics": json_source(self.paths.live_diagnostics),
            "manual_exit": manual_exit_source(self.paths.live_status.parent / 'manual-exit-status.json'),
            "recorder": json_source(self.paths.recorder_status),
            "recorder_manifest": json_source(self.paths.recorder_manifest),
            "journal": journal_source(self.paths.journal),
        }
        preflight = self.paths.live_status.parent / 'public-preflight-status.json'
        if preflight.is_file():
            sources['public_preflight'] = json_source(preflight)
        self.market.poll()
        self.live_market.poll()
        sources["market"] = {"read_at": time.time(), "observed_at": self.market.observed_at,
                             "quality": self.market.quality, "reason": self.market.reason,
                             "schema": self.market.schema, "session": None,
                             "backfill_complete": self.market.backfill_complete,
                             "cache_error": self.market.cache_error}
        sources["live_market"] = {"read_at": time.time(), "observed_at": self.live_market.observed_at,
            "quality": self.live_market.quality, "reason": self.live_market.reason,
            "schema": None, "session": None, "backfill_complete": self.live_market.backfill_complete,
            "cache_error": self.live_market.cache_error}
        self.sequence += 1
        self.snapshot = {"id": f"v3-{self.sequence}", "generated_at": time.time(),
                         "sources": sources, "journal": sources["journal"].get("data"),
                         "market": list(self.market.points), "live_market": list(self.live_market.points)}


def public_sources(snapshot: dict[str, Any]) -> dict[str, Any]:
    now = snapshot["generated_at"]
    out = {}
    for name, source in snapshot["sources"].items():
        observed = number(source.get("observed_at"))
        age = float(Decimal(str(now)) - observed) if observed is not None else None
        out[name] = {key: source.get(key) for key in (
            "read_at", "observed_at", "quality", "reason", "schema", "session",
            "backfill_complete", "cache_error")}
        out[name]["age_seconds"] = age
        out[name]["age_s"] = age
        if age is not None and age < -5:
            out[name]["quality"] = "clock_anomaly"
        if name in ("live", "recorder") and age is not None and age > 10:
            out[name]["quality"] = "stale"
        if name in ("market", "live_market", "live_diagnostics") and age is not None and age > 5:
            out[name]["quality"] = "stale"
    return out
