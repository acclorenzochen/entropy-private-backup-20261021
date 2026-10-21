"""Single-writer V3 intent and sampling ledger; isolated from legacy databases.

No order is sent here. SUBMITTING/UNKNOWN survive restarts. Missing execution
fees/prices are NULL, never silently zero. Intended for one supervised process.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Iterable, Iterator
from uuid import uuid4

from .core import Side, dec


class PendingOrder(RuntimeError):
    pass


@dataclass(frozen=True)
class Intent:
    order_ref: str
    cycle_id: str
    venue: str
    side: Side
    units: int
    limit_px: Decimal
    reduce_only: bool = False

    def __post_init__(self) -> None:
        if not self.order_ref or not self.cycle_id or self.venue not in {'entropy', 'hedge'}:
            raise ValueError('invalid intent identity')
        if type(self.units) is not int or self.units <= 0 or self.limit_px <= 0 \
                or not self.limit_px.is_finite() or not isinstance(self.side, Side):
            raise ValueError('invalid intent payload')


@dataclass(frozen=True)
class TerminalReceipt:
    order_ref: str
    venue: str
    side: Side
    requested_units: int
    cumulative_filled_units: int
    confirmation_source: str
    observed_ts: float
    fill_identity_complete: bool
    fill_quantity_verified: bool


@dataclass(frozen=True)
class Result:
    terminal: bool
    cumulative_units: int
    avg_px: Decimal | None = None
    fee_usd: Decimal | None = None
    note: str = ''
    receipt: TerminalReceipt | None = None
    deferred: bool = False
    retry_after: float = 0.0

    def __post_init__(self) -> None:
        if type(self.terminal) is not bool or type(self.cumulative_units) is not int \
                or self.cumulative_units < 0:
            raise ValueError('invalid result')
        if self.avg_px is not None and (not self.avg_px.is_finite() or self.avg_px <= 0):
            raise ValueError('invalid average price')
        if self.fee_usd is not None and not self.fee_usd.is_finite():
            raise ValueError('invalid fee')
        if self.receipt is not None and (
                not self.terminal or
                self.receipt.cumulative_filled_units != self.cumulative_units):
            raise ValueError('receipt does not match terminal result')
        if type(self.deferred) is not bool or not math.isfinite(self.retry_after) \
                or self.retry_after < 0:
            raise ValueError('invalid dispatch deferral')
        if self.deferred and (self.terminal or self.cumulative_units
                              or self.receipt is not None):
            raise ValueError('deferred dispatch must be proved unsent')


class Journal:
    VERSION = '1'
    BASIS_PROFIT_PENDING = '{"state":"opening_pending"}'
    OPERATOR_INTENTS = frozenset({'RUNNING', 'STOPPING', 'STOPPED'})
    OPERATOR_OPERATION_STATES = frozenset({
        'accepted', 'executing', 'flat_pending_accounting', 'completed', 'blocked',
    })
    CLOSE_REASONS = frozenset({
        'REALIZED_LOSS_BUDGET', 'SINGLE_CYCLE_REVIEW_LOSS',
        'ACCOUNT_DRAWDOWN', 'MANUAL_STOP', 'FUNDING_EARLY_EXIT',
        'POSITION_RISK',
    })

    def __init__(self, path: str | Path, *, identity: str) -> None:
        if not identity:
            raise ValueError('pair + account + deployment identity required')
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        # Advisory lifetime lock prevents two cooperating V3 senders sharing state.
        # Integration must additionally share the account lock with LEGACY/MAKER.
        import fcntl
        self._lock_file = open(str(p) + '.owner.lock', 'a+b')
        try:
            fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._lock_file.close()
            raise RuntimeError('another V3 writer owns this journal') from exc
        try:
            if p.exists() and p.stat().st_size:
                # Reject an incompatible database before WAL mode, CREATE TABLE
                # or additive ALTER TABLE can change any bytes.
                with sqlite3.connect(f'file:{p.resolve().as_posix()}?mode=ro',
                                     uri=True) as check:
                    has_metadata = check.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata'"
                    ).fetchone()
                    if has_metadata:
                        stored = dict(check.execute('SELECT key,value FROM metadata'))
                        if stored.get('version') != self.VERSION:
                            raise ValueError('journal version mismatch; no automatic migration')
                        if stored.get('identity') != identity:
                            raise ValueError('journal identity mismatch; no automatic migration')
                        if int(stored.get('economics_schema_version', '0')) > 8:
                            raise ValueError('journal economics schema is newer than this runtime')
                        if int(stored.get('fee_schema_version', '0')) > 1:
                            raise ValueError('journal fee schema is newer than this runtime')
                    elif check.execute("SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1").fetchone():
                        raise ValueError('existing journal has no verifiable metadata identity')
            self.db = sqlite3.connect(p, timeout=2.0, isolation_level=None)
            self.db.row_factory = sqlite3.Row
            self.db.execute('PRAGMA journal_mode=WAL')
            self.db.execute('PRAGMA synchronous=FULL')
            self.db.executescript('''
                CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS operator_controls(
                  scope_key TEXT PRIMARY KEY, intent TEXT NOT NULL
                    CHECK(intent IN ('RUNNING','STOPPING','STOPPED')),
                  intent_revision INTEGER NOT NULL CHECK(intent_revision>=0),
                  updated_ts REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS operator_operations(
                  operation_id TEXT PRIMARY KEY, scope_key TEXT NOT NULL,
                  idempotency_key TEXT NOT NULL, request_fingerprint TEXT NOT NULL,
                  action TEXT NOT NULL CHECK(action IN ('start','stop','close')),
                  expected_cycle_id TEXT, state TEXT NOT NULL
                    CHECK(state IN ('accepted','executing','flat_pending_accounting','completed','blocked')),
                  receipt_json TEXT NOT NULL, evidence_json TEXT NOT NULL,
                  accepted_ts REAL NOT NULL, updated_ts REAL NOT NULL,
                  UNIQUE(scope_key,idempotency_key));
                CREATE TABLE IF NOT EXISTS operator_parameter_saves(
                  scope_key TEXT NOT NULL,idempotency_key TEXT NOT NULL,
                  request_fingerprint TEXT NOT NULL,receipt_json TEXT NOT NULL,
                  PRIMARY KEY(scope_key,idempotency_key));
                CREATE TABLE IF NOT EXISTS orders(
                  order_ref TEXT PRIMARY KEY, cycle_id TEXT NOT NULL, venue TEXT NOT NULL,
                  side TEXT NOT NULL, requested_units INTEGER NOT NULL, limit_px TEXT NOT NULL,
                  reduce_only INTEGER NOT NULL, state TEXT NOT NULL,
                  filled_units INTEGER NOT NULL DEFAULT 0, avg_px TEXT, fee_usd TEXT, note TEXT);
                CREATE TABLE IF NOT EXISTS batches(
                  batch_id TEXT PRIMARY KEY, max_position_cycles INTEGER NOT NULL,
                  loss_budget TEXT NOT NULL, closed_for_entries INTEGER NOT NULL DEFAULT 0,
                  close_reason TEXT, close_ts REAL, close_value TEXT,
                  close_threshold TEXT, close_cycle_id TEXT);
                CREATE TABLE IF NOT EXISTS opening_experiments(
                  experiment_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL UNIQUE,
                  contract_json TEXT NOT NULL, armed INTEGER NOT NULL DEFAULT 1,
                  opportunity_seq INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS opening_attempts(
                  experiment_id TEXT NOT NULL REFERENCES opening_experiments(experiment_id),
                  slot INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 20),
                  group_name TEXT NOT NULL CHECK(group_name IN ('A','B')),
                  opportunity_id TEXT NOT NULL, cycle_id TEXT NOT NULL UNIQUE,
                  pair_plan_json TEXT NOT NULL, first_ref TEXT NOT NULL, hedge_ref TEXT,
                  terminal_flat INTEGER NOT NULL DEFAULT 0, outcome TEXT,
                  allocated_ts REAL NOT NULL, finished_ts REAL,
                  PRIMARY KEY(experiment_id,slot), UNIQUE(experiment_id,opportunity_id));
                CREATE TABLE IF NOT EXISTS cycles(
                  cycle_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, result_kind TEXT,
                  pnl_usd TEXT, complete_accounting INTEGER NOT NULL DEFAULT 0,
                  accounting_final INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS live_cycles(
                  cycle_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL,
                  direction TEXT NOT NULL, planned_units INTEGER NOT NULL,
                  reference_bps TEXT NOT NULL, entry_basis_bps TEXT NOT NULL,
                  entropy_units INTEGER NOT NULL DEFAULT 0,
                  hedge_units INTEGER NOT NULL DEFAULT 0,
                  cash_net_usd TEXT NOT NULL DEFAULT '0',
                  accounting_final INTEGER NOT NULL DEFAULT 1,
                  live_entry_basis_bps TEXT,
                  live_entry_basis_source TEXT,
                  position_evidence_authoritative INTEGER NOT NULL DEFAULT 0,
                  last_authoritative_ts REAL,
                  phase TEXT NOT NULL, started_ts REAL NOT NULL,
                  updated_ts REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS funding_cycles(
                  cycle_id TEXT PRIMARY KEY REFERENCES live_cycles(cycle_id),
                  hedged_ts REAL,
                  negative_sample_count INTEGER NOT NULL DEFAULT 0,
                  negative_since_ts REAL, last_sample_ts REAL,
                  last_net_usd_per_hour TEXT,
                  last_complete_poll_ts REAL,
                  trade_cash_net_usd TEXT,
                  trade_accounting_complete INTEGER NOT NULL DEFAULT 0,
                  closed_poll_complete INTEGER NOT NULL DEFAULT 0,
                  exit_reason TEXT, completed INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS funding_settlements(
                  venue TEXT NOT NULL CHECK(venue IN ('entropy','hedge')),
                  event_id TEXT NOT NULL,
                  cycle_id TEXT NOT NULL REFERENCES funding_cycles(cycle_id),
                  ts REAL NOT NULL, amount_usd TEXT NOT NULL,
                  position_qty TEXT, source TEXT NOT NULL,
                  PRIMARY KEY(venue,event_id));
                CREATE INDEX IF NOT EXISTS funding_settlements_cycle
                  ON funding_settlements(cycle_id);
                CREATE TABLE IF NOT EXISTS fee_fills(
                  venue TEXT NOT NULL CHECK(venue IN ('entropy','hedge')),
                  exchange_fill_key TEXT NOT NULL,
                  cycle_id TEXT NOT NULL REFERENCES cycles(cycle_id),
                  order_ref TEXT NOT NULL REFERENCES orders(order_ref),
                  units INTEGER NOT NULL CHECK(units>0), price TEXT NOT NULL,
                  ts REAL NOT NULL, fee_usd TEXT,
                  PRIMARY KEY(venue,exchange_fill_key));
                CREATE INDEX IF NOT EXISTS fee_fills_cycle ON fee_fills(cycle_id);
                CREATE TABLE IF NOT EXISTS external_cycle_bindings(
                  cycle_id TEXT NOT NULL, venue TEXT NOT NULL,
                  binding_hash TEXT NOT NULL, account TEXT NOT NULL, market TEXT NOT NULL,
                  witness_order_ref TEXT, created_ts REAL NOT NULL,
                  PRIMARY KEY(cycle_id,venue));
                CREATE TABLE IF NOT EXISTS external_fills(
                  venue TEXT NOT NULL, exchange_fill_key TEXT NOT NULL,
                  order_ref TEXT NOT NULL UNIQUE, cycle_id TEXT NOT NULL,
                  binding_hash TEXT NOT NULL, account TEXT NOT NULL, market TEXT NOT NULL,
                  exchange_order_id TEXT NOT NULL, trade_id TEXT NOT NULL,
                  side TEXT NOT NULL, units INTEGER NOT NULL, price TEXT NOT NULL,
                  fee_usd TEXT, ts REAL NOT NULL, start_position TEXT NOT NULL,
                  quantity_step TEXT NOT NULL, raw_evidence_json TEXT NOT NULL,
                  fee_evidence_json TEXT, recorded_ts REAL NOT NULL,
                  PRIMARY KEY(venue,exchange_fill_key),
                  UNIQUE(venue,account,market,trade_id));
                CREATE INDEX IF NOT EXISTS external_fills_cycle ON external_fills(cycle_id);
                CREATE TABLE IF NOT EXISTS request_events(
                  venue TEXT NOT NULL, used_ts REAL NOT NULL,
                  weight INTEGER NOT NULL, request_kind TEXT, cycle_id TEXT,
                  PRIMARY KEY(venue,used_ts));
                CREATE TABLE IF NOT EXISTS live_steps(
                  step_key TEXT PRIMARY KEY, cycle_id TEXT NOT NULL,
                  order_ref TEXT NOT NULL UNIQUE, venue TEXT NOT NULL,
                  side TEXT NOT NULL, requested_units INTEGER NOT NULL,
                  reduce_only INTEGER NOT NULL, applied INTEGER NOT NULL DEFAULT 0,
                  result_units INTEGER, cash_delta_usd TEXT,
                  accounting_final INTEGER,
                  execution_group_id TEXT, parent_step_key TEXT);
                CREATE TABLE IF NOT EXISTS cycle_economics(
                  cycle_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL,
                  telemetry_version INTEGER NOT NULL, direction TEXT NOT NULL,
                  signal_ts REAL NOT NULL, decision_ts REAL,
                  reference_bps TEXT NOT NULL,
                  signal_executable_basis_bps TEXT NOT NULL,
                  entry_dislocation_bps TEXT NOT NULL,
                  planned_units INTEGER NOT NULL, planned_notional_usd TEXT,
                  first_fill_mark_basis_bps TEXT, actual_entry_basis_bps TEXT,
                  first_fill_observed_ts REAL, first_fill_exchange_ts REAL,
                  first_fill_observation_lag_ms TEXT,
                  live_entry_basis_bps TEXT, live_entry_basis_source TEXT,
                  exit_anchor_basis_bps TEXT, exit_anchor_source TEXT,
                  anchor_error_bps TEXT,
                  position_check_start_ts REAL, position_check_end_ts REAL,
                  cycle_persist_start_ts REAL, cycle_persist_end_ts REAL,
                  benchmark_start_ts REAL, benchmark_end_ts REAL,
                  intent_prepare_start_ts REAL, intent_prepare_end_ts REAL,
                  submit_start_ts REAL, submit_result_ts REAL,
                  pre_submit_ts REAL, pre_submit_basis_bps TEXT,
                  pre_submit_dislocation_bps TEXT,
                  first_leg_cost_bps TEXT, hedge_cost_bps TEXT,
                  signal_to_first_fill_decay_bps TEXT,
                  first_fill_to_hedged_decay_bps TEXT, total_entry_decay_bps TEXT,
                  max_favorable_reversion_bps TEXT,
                  max_favorable_reversion_ts REAL, max_reversion_flush_ts REAL,
                  exit_trigger_ts REAL, exit_trigger_reason TEXT,
                  exit_trigger_basis_bps TEXT,
                  exit_trigger_reversion_bps TEXT,
                  exit_trigger_executable_pnl_usd TEXT,
                  entropy_close_submit_ts REAL, entropy_close_fill_ts REAL,
                  entropy_close_fill_px TEXT, entropy_close_fill_units INTEGER,
                  paired_basis_at_entropy_fill TEXT,
                  lighter_close_submit_ts REAL, lighter_close_fill_ts REAL,
                  lighter_close_fill_px TEXT, lighter_close_fill_units INTEGER,
                  actual_paired_exit_basis TEXT,
                  trigger_to_entropy_fill_ms TEXT,
                  entropy_fill_to_lighter_submit_ms TEXT,
                  lighter_submit_to_fill_ms TEXT,
                  trigger_to_entropy_fill_decay_bps TEXT,
                  interleg_decay_bps TEXT,
                  total_exit_execution_decay_bps TEXT,
                  actual_exit_basis_bps TEXT, actual_reversion_bps TEXT,
                  actual_entry_notional_usd TEXT,
                  actual_fees_usd TEXT, entropy_fees_usd TEXT, hedge_fees_usd TEXT,
                  actual_fees_bps TEXT,
                  actual_funding_usd TEXT, entropy_funding_usd TEXT,
                  hedge_funding_usd TEXT, actual_funding_bps TEXT,
                  finalized_pnl_usd TEXT, finalized_pnl_bps TEXT,
                  break_even_reversion_bps TEXT, result_kind TEXT,
                  recovery_used INTEGER NOT NULL DEFAULT 0,
                  accounting_final INTEGER NOT NULL DEFAULT 0,
                  clean_summary_eligible INTEGER NOT NULL DEFAULT 0,
                  telemetry_incomplete INTEGER NOT NULL DEFAULT 1,
                  signal_mono_ns INTEGER, signal_clock_session_id TEXT,
                  exit_trigger_mono_ns INTEGER, exit_trigger_clock_session_id TEXT,
                  authoritative_reconcile_complete_ts REAL,
                  authoritative_reconcile_complete_mono_ns INTEGER,
                  authoritative_reconcile_clock_session_id TEXT,
                  created_ts REAL NOT NULL, updated_ts REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS execution_economics_steps(
                  step_key TEXT PRIMARY KEY, cycle_id TEXT NOT NULL,
                  purpose TEXT NOT NULL, venue TEXT NOT NULL, attempt INTEGER NOT NULL,
                  order_ref TEXT NOT NULL,
                  side TEXT NOT NULL, requested_units INTEGER NOT NULL,
                  room_bps TEXT NOT NULL, hard_cap_bps TEXT NOT NULL,
                  submit_ts REAL NOT NULL, top_px TEXT NOT NULL,
                  marginal_px TEXT NOT NULL, limit_px TEXT NOT NULL,
                  paired_executable_basis_bps TEXT NOT NULL,
                  reference_bps TEXT NOT NULL, directional_dislocation_bps TEXT NOT NULL,
                  result_ts REAL, result_units INTEGER, provisional_avg_px TEXT,
                  result_paired_executable_basis_bps TEXT,
                  result_directional_dislocation_bps TEXT,
                  actual_fill_ts REAL, actual_fill_vwap TEXT, actual_fee_usd TEXT,
                  submit_start_ts REAL, http_post_start REAL,
                  http_headers_received REAL, http_body_received REAL,
                  http_result_ts REAL, terminal_result_ts REAL,
                  confirmation_source TEXT, result_status TEXT,
                  resolve_start_ts REAL, resolve_end_ts REAL,
                  exchange_fill_ts REAL,
                  execution_cost_bps TEXT, book_revision_entropy TEXT NOT NULL,
                  book_revision_hedge TEXT NOT NULL,
                  execution_group_id TEXT, parent_step_key TEXT,
                  telemetry_version INTEGER, phase_kind TEXT,
                  clock_session_id TEXT, quantity_step TEXT,
                  original_signal_ts REAL, original_signal_basis_bps TEXT,
                  attempt_decision_ts REAL, attempt_decision_mono_ns INTEGER,
                  attempt_decision_basis_bps TEXT, decision_units INTEGER,
                  decision_quote_json TEXT,
                  submit_quote_ts REAL, submit_quote_json TEXT,
                  submit_basis_bps TEXT, submit_start_mono_ns INTEGER,
                  http_post_start_mono_ns INTEGER,
                  terminal_observed_ts REAL, terminal_observed_mono_ns INTEGER,
                  exchange_first_fill_ts REAL, exchange_last_fill_ts REAL,
                  fill_mark_quote_json TEXT, fill_mark_time_source TEXT,
                  execution_path TEXT, measurement_quality TEXT,
                  missing_fields_json TEXT,
                  authoritative_reconcile_complete_ts REAL,
                  authoritative_reconcile_complete_mono_ns INTEGER);
            ''')
            additive_columns = {
                'orders': {
                    'known_fee_usd': 'TEXT',
                    'fee_covered_units': 'INTEGER',
                    'fee_source': 'TEXT',
                    'had_unknown': 'INTEGER',
                    'terminal_recorded_ts': 'REAL',
                    'first_unknown_ts': 'REAL',
                    'submitted_ts': 'REAL',
                    'terminal_received_ts': 'REAL',
                    'terminal_received_mono_ns': 'INTEGER',
                    'terminal_clock_session_id': 'TEXT',
                    'terminal_source': 'TEXT',
                    'terminal_status': 'TEXT',
                },
                'cycles': {
                    'completed_ts': 'REAL',
                    'pending_known_cash_usd': 'TEXT',
                    'pending_cost_estimate_usd': 'TEXT',
                },
                'funding_cycles': {
                    'trade_gross_cash_usd': 'TEXT',
                    'known_cash_result_usd': 'TEXT',
                    'fee_evidence_accounting': 'INTEGER NOT NULL DEFAULT 0',
                    'last_complete_poll_ts': 'REAL',
                    'trade_cash_net_usd': 'TEXT',
                    'trade_accounting_complete': 'INTEGER NOT NULL DEFAULT 0',
                    'closed_poll_complete': 'INTEGER NOT NULL DEFAULT 0',
                },
                'batches': {
                    'close_reason': 'TEXT', 'close_ts': 'REAL',
                    'close_value': 'TEXT', 'close_threshold': 'TEXT',
                    'close_cycle_id': 'TEXT',
                },
                'live_cycles': {
                    'basis_exit_requested_ts': 'REAL',
                    'risk_exit_reason': 'TEXT',
                    'basis_profit_basis_json': 'TEXT',
                    'recovery_used': 'INTEGER NOT NULL DEFAULT 0',
                    'live_entry_basis_bps': 'TEXT',
                    'live_entry_basis_source': 'TEXT',
                    'position_evidence_authoritative': 'INTEGER NOT NULL DEFAULT 0',
                    'last_authoritative_ts': 'REAL',
                },
                'live_steps': {
                    'quantity_step': 'TEXT',
                    'baseline_entropy_units': 'INTEGER',
                    'baseline_hedge_units': 'INTEGER',
                    'execution_group_id': 'TEXT',
                    'parent_step_key': 'TEXT',
                    'abort_reason': 'TEXT',
                    'trigger_ts': 'REAL',
                    'trigger_basis_bps': 'TEXT',
                    'trigger_mono_ns': 'INTEGER',
                    'trigger_clock_session_id': 'TEXT',
                },
                'request_events': {
                    'request_kind': 'TEXT', 'cycle_id': 'TEXT',
                },
                'cycle_economics': {
                    'signal_mono_ns': 'INTEGER',
                    'signal_clock_session_id': 'TEXT',
                    'exit_trigger_mono_ns': 'INTEGER',
                    'exit_trigger_clock_session_id': 'TEXT',
                    'authoritative_reconcile_complete_ts': 'REAL',
                    'authoritative_reconcile_complete_mono_ns': 'INTEGER',
                    'authoritative_reconcile_clock_session_id': 'TEXT',
                    'live_entry_basis_bps': 'TEXT',
                    'live_entry_basis_source': 'TEXT',
                    'exit_anchor_basis_bps': 'TEXT',
                    'exit_anchor_source': 'TEXT',
                    'anchor_error_bps': 'TEXT',
                    'position_check_start_ts': 'REAL',
                    'position_check_end_ts': 'REAL',
                    'cycle_persist_start_ts': 'REAL',
                    'cycle_persist_end_ts': 'REAL',
                    'benchmark_start_ts': 'REAL',
                    'benchmark_end_ts': 'REAL',
                    'intent_prepare_start_ts': 'REAL',
                    'intent_prepare_end_ts': 'REAL',
                    'submit_start_ts': 'REAL',
                    'submit_result_ts': 'REAL',
                    'pre_submit_ts': 'REAL',
                    'pre_submit_basis_bps': 'TEXT',
                    'pre_submit_dislocation_bps': 'TEXT',
                    'decision_ts': 'REAL',
                    'exit_trigger_basis_bps': 'TEXT',
                    'entropy_close_submit_ts': 'REAL',
                    'entropy_close_fill_ts': 'REAL',
                    'entropy_close_fill_px': 'TEXT',
                    'entropy_close_fill_units': 'INTEGER',
                    'paired_basis_at_entropy_fill': 'TEXT',
                    'lighter_close_submit_ts': 'REAL',
                    'lighter_close_fill_ts': 'REAL',
                    'lighter_close_fill_px': 'TEXT',
                    'lighter_close_fill_units': 'INTEGER',
                    'actual_paired_exit_basis': 'TEXT',
                    'trigger_to_entropy_fill_ms': 'TEXT',
                    'entropy_fill_to_lighter_submit_ms': 'TEXT',
                    'lighter_submit_to_fill_ms': 'TEXT',
                    'trigger_to_entropy_fill_decay_bps': 'TEXT',
                    'interleg_decay_bps': 'TEXT',
                    'total_exit_execution_decay_bps': 'TEXT',
                },
                'execution_economics_steps': {
                    'terminal_clock_session_id': 'TEXT',
                    'runtime_consumed_ts': 'REAL',
                    'runtime_consumed_mono_ns': 'INTEGER',
                    'runtime_consumed_clock_session_id': 'TEXT',
                    'telemetry_version': 'INTEGER',
                    'phase_kind': 'TEXT',
                    'clock_session_id': 'TEXT',
                    'quantity_step': 'TEXT',
                    'original_signal_ts': 'REAL',
                    'original_signal_basis_bps': 'TEXT',
                    'attempt_decision_ts': 'REAL',
                    'attempt_decision_mono_ns': 'INTEGER',
                    'attempt_decision_basis_bps': 'TEXT',
                    'decision_units': 'INTEGER',
                    'decision_quote_json': 'TEXT',
                    'submit_quote_ts': 'REAL',
                    'submit_quote_json': 'TEXT',
                    'submit_basis_bps': 'TEXT',
                    'submit_start_mono_ns': 'INTEGER',
                    'http_post_start_mono_ns': 'INTEGER',
                    'terminal_observed_ts': 'REAL',
                    'terminal_observed_mono_ns': 'INTEGER',
                    'exchange_first_fill_ts': 'REAL',
                    'exchange_last_fill_ts': 'REAL',
                    'fill_mark_quote_json': 'TEXT',
                    'fill_mark_time_source': 'TEXT',
                    'execution_path': 'TEXT',
                    'measurement_quality': 'TEXT',
                    'missing_fields_json': 'TEXT',
                    'authoritative_reconcile_complete_ts': 'REAL',
                    'authoritative_reconcile_complete_mono_ns': 'INTEGER',
                    'execution_group_id': 'TEXT',
                    'parent_step_key': 'TEXT',
                    'order_ref': 'TEXT',
                    'submit_start_ts': 'REAL',
                    'http_post_start': 'REAL',
                    'http_headers_received': 'REAL',
                    'http_body_received': 'REAL',
                    'http_result_ts': 'REAL',
                    'terminal_result_ts': 'REAL',
                    'confirmation_source': 'TEXT',
                    'result_status': 'TEXT',
                    'resolve_start_ts': 'REAL',
                    'resolve_end_ts': 'REAL',
                    'exchange_fill_ts': 'REAL',
                },
            }
            for table, columns in additive_columns.items():
                existing = {row[1] for row in self.db.execute(
                    f'PRAGMA table_info({table})')}
                for name, declaration in columns.items():
                    if name not in existing:
                        self.db.execute(
                            f'ALTER TABLE {table} ADD COLUMN {name} {declaration}')
            self.db.execute('''UPDATE execution_economics_steps SET order_ref=(
                SELECT order_ref FROM live_steps
                WHERE live_steps.step_key=execution_economics_steps.step_key)
                WHERE order_ref IS NULL''')
            self.db.execute('''UPDATE cycle_economics SET decision_ts=signal_ts
                WHERE decision_ts IS NULL''')
            with self.transaction():
                for k, v in (('version', self.VERSION), ('identity', identity)):
                    row = self.db.execute('SELECT value FROM metadata WHERE key=?', (k,)).fetchone()
                    if row and row[0] != v:
                        raise ValueError(f'journal {k} mismatch; no automatic migration')
                    self.db.execute('INSERT OR IGNORE INTO metadata VALUES (?,?)', (k, v))
                schema = self.db.execute(
                    "SELECT value FROM metadata WHERE key='economics_schema_version'"
                ).fetchone()
                if schema and int(schema[0]) > 8:
                    raise ValueError('journal economics schema is newer than this runtime')
                self.db.execute('''INSERT INTO metadata(key,value) VALUES
                    ('economics_schema_version','8') ON CONFLICT(key) DO UPDATE
                    SET value=excluded.value''')
                self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('fee_schema_version','1')")
        except BaseException:
            if hasattr(self, 'db'):
                self.db.close()
            self._lock_file.close()
            raise

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            if self.db.in_transaction:
                self.db.execute('ROLLBACK')
            raise

    def close(self) -> None:
        self.db.close()
        self._lock_file.close()

    @staticmethod
    def _operator_key(value: str) -> str:
        if not isinstance(value, str) or not value:
            raise ValueError('nonempty operator identity required')
        return value

    def ensure_operator_control(self, scope_key: str, *,
                                initial_intent: str = 'RUNNING') -> dict:
        scope_key = self._operator_key(scope_key)
        if initial_intent not in self.OPERATOR_INTENTS:
            raise ValueError('invalid operator intent')
        with self.transaction():
            self.db.execute('''INSERT OR IGNORE INTO operator_controls
                (scope_key,intent,intent_revision,updated_ts) VALUES (?,?,0,?)''',
                (scope_key, initial_intent, time.time()))
            return self.operator_control_state(scope_key)

    def operator_control_state(self, scope_key: str) -> dict:
        row = self.db.execute('''SELECT intent,intent_revision,updated_ts
            FROM operator_controls WHERE scope_key=?''',
            (self._operator_key(scope_key),)).fetchone()
        if row is None:
            raise KeyError(scope_key)
        return dict(row)

    def pause_operator_boot(self, scope_key: str) -> dict:
        """A new owner process requires a fresh manual start, retaining recovery."""
        with self.transaction():
            self.db.execute('''UPDATE operator_controls SET intent='STOPPING',
                intent_revision=intent_revision+1,updated_ts=?
                WHERE scope_key=? AND intent='RUNNING' ''',
                (time.time(), self._operator_key(scope_key)))
            return self.operator_control_state(scope_key)

    @staticmethod
    def _operator_operation_row(row: sqlite3.Row) -> dict:
        operation = dict(row)
        operation['receipt'] = json.loads(operation.pop('receipt_json'))
        operation['evidence'] = json.loads(operation.pop('evidence_json'))
        control = operation['receipt']['control']
        operation['intent'] = control['intent']
        operation['intent_revision'] = control['intent_revision']
        return operation

    def operator_operation(self, scope_key: str, operation_id: str | None = None, *,
                           idempotency_key: str | None = None) -> dict | None:
        scope_key = self._operator_key(scope_key)
        if (operation_id is None) == (idempotency_key is None):
            raise ValueError('one operation identity required')
        column, value = ('operation_id', operation_id) if operation_id is not None \
            else ('idempotency_key', idempotency_key)
        row = self.db.execute(f'SELECT * FROM operator_operations WHERE scope_key=? AND {column}=?',
                              (scope_key, self._operator_key(value))).fetchone()
        return self._operator_operation_row(row) if row is not None else None

    def operator_pending_operations(self, scope_key: str) -> list[dict]:
        return [self._operator_operation_row(row) for row in self.db.execute('''
            SELECT * FROM operator_operations WHERE scope_key=? AND state!='completed'
            ORDER BY accepted_ts,rowid''', (self._operator_key(scope_key),))]

    def operator_parameters(self, scope_key: str) -> dict | None:
        row = self.db.execute('SELECT value FROM metadata WHERE key=?',
            ('operator_parameters:' + self._operator_key(scope_key),)).fetchone()
        return json.loads(row['value']) if row is not None else None

    def _write_operator_parameters(self, scope_key: str, document: dict) -> None:
        self.db.execute('''INSERT INTO metadata(key,value) VALUES (?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value''',
            ('operator_parameters:' + self._operator_key(scope_key),
             json.dumps(document, sort_keys=True, allow_nan=False)))

    def ensure_operator_parameters(self, scope_key: str, document: dict) -> dict:
        with self.transaction():
            current = self.operator_parameters(scope_key)
            if current is None:
                self._write_operator_parameters(scope_key, document)
                current = document
            return current

    def operator_parameter_save(self, scope_key: str, idempotency_key: str) -> dict | None:
        row = self.db.execute('''SELECT request_fingerprint,receipt_json
            FROM operator_parameter_saves WHERE scope_key=? AND idempotency_key=?''',
            (self._operator_key(scope_key), self._operator_key(idempotency_key))).fetchone()
        return dict(request_fingerprint=row['request_fingerprint'],
            receipt=json.loads(row['receipt_json'])) if row is not None else None

    @staticmethod
    def _operator_parameter_flat_evidence(evidence: dict | None) -> bool:
        if type(evidence) is not dict:
            return False
        observed = evidence.get('position_observed_ts')
        return (evidence.get('position_authoritative') is True
            and type(observed) in (int, float) and math.isfinite(observed)
            and 0 <= time.time() - observed <= 15
            and all(type(evidence.get(key)) is int and evidence[key] == 0
                    for key in ('entropy_units', 'hedge_units'))
            and evidence.get('active_cycle_id') is None and evidence.get('pending_orders') == [])

    def save_operator_parameters(self, scope_key: str, *, operator_scope: str,
                                 expected_revision: int, record: dict,
                                 idempotency_key: str, request_fingerprint: str,
                                 confirmation_evidence: dict, document_limits: dict | None = None,
                                 opening_fields_present: bool | None = None) -> dict:
        with self.transaction():
            prior = self.operator_parameter_save(scope_key, idempotency_key)
            if prior is not None:
                if prior['request_fingerprint'] != request_fingerprint:
                    raise ValueError('parameters_idempotency_payload_changed')
                if (opening_fields_present is not None
                        and 'opening_fields_present' in prior['receipt']
                        and prior['receipt']['opening_fields_present'] != opening_fields_present):
                    raise ValueError('parameters_idempotency_payload_changed')
                return prior['receipt']
            if (not self._operator_parameter_flat_evidence(confirmation_evidence)
                    or self.operator_control_state(operator_scope)['intent'] != 'STOPPED'
                    or self.pending() or self.active_live_cycle() is not None):
                raise ValueError('parameters_require_stopped_flat')
            document = self.operator_parameters(scope_key)
            latest = document['saved'] or document['current']
            if type(expected_revision) is not int or expected_revision != latest['revision']:
                raise ValueError('parameters_revision_changed')
            if record['revision'] != expected_revision + 1:
                raise ValueError('parameters_revision_changed')
            if document_limits is not None:
                from .parameter_config import upgrade_document
                document = upgrade_document(document, document_limits)
            document['saved'] = record
            self._write_operator_parameters(scope_key, document)
            receipt = dict(revision=record['revision'], config_hash=record['config_hash'])
            if opening_fields_present is not None:
                receipt['opening_fields_present'] = opening_fields_present
            self.db.execute('''INSERT INTO operator_parameter_saves
                (scope_key,idempotency_key,request_fingerprint,receipt_json) VALUES (?,?,?,?)''',
                (scope_key, self._operator_key(idempotency_key),
                 self._operator_key(request_fingerprint), json.dumps(receipt, sort_keys=True)))
            return receipt

    def commit_operator_command(self, scope_key: str, *, action: str,
                                expected_revision: int, idempotency_key: str,
                                request_fingerprint: str,
                                expected_cycle_id: str | None = None,
                                confirmation_evidence: dict | None = None,
                                parameters_scope: str | None = None,
                                parameters_revision: int | None = None,
                                parameters_hash: str | None = None,
                                parameters_limits: dict | None = None,
                                now: float | None = None) -> dict:
        scope_key = self._operator_key(scope_key)
        idempotency_key = self._operator_key(idempotency_key)
        request_fingerprint = self._operator_key(request_fingerprint)
        with self.transaction():
            prior = self.operator_operation(scope_key, idempotency_key=idempotency_key)
            if prior is not None:
                if prior['request_fingerprint'] != request_fingerprint:
                    raise ValueError('operator idempotency key payload changed')
                return prior
            if action not in {'start', 'stop', 'close'}:
                raise ValueError('invalid operator action')
            if confirmation_evidence is not None and not isinstance(confirmation_evidence, dict):
                raise ValueError('operator confirmation evidence must be a snapshot dict')
            control_before = self.operator_control_state(scope_key)
            if type(expected_revision) is not int or expected_revision < 0 \
                    or expected_revision != control_before['intent_revision']:
                raise ValueError('operator intent revision changed')
            committed_ts = self._funding_time(time.time() if now is None else now)
            cycle_before = None
            orders = []
            if action == 'close':
                self._operator_key(expected_cycle_id)
                cycle_before = self.active_live_cycle()
                if cycle_before is None or cycle_before['cycle_id'] != expected_cycle_id:
                    raise PendingOrder('operator close target is no longer active')
                orders = self.cycle_orders(expected_cycle_id)
                self._latch_basis_exit(expected_cycle_id, requested_ts=committed_ts)
            elif expected_cycle_id is not None:
                raise ValueError('only close accepts a cycle target')
            parameter_record = None
            if action == 'start' and parameters_scope is not None:
                document = self.operator_parameters(parameters_scope)
                if document is None:
                    raise ValueError('parameters_persistence_unavailable')
                if parameters_revision is None and parameters_hash is None:
                    if document['saved'] is not None:
                        raise ValueError('saved_parameters_require_explicit_start')
                else:
                    parameter_record = document['saved'] or document['current']
                    if (type(parameters_revision) is not int
                            or parameters_revision != parameter_record['revision']
                            or parameters_hash != parameter_record['config_hash']):
                        raise ValueError('parameters_revision_changed')
                    if (control_before['intent'] != 'STOPPED' or self.pending()
                            or self.active_live_cycle() is not None
                            or not self._operator_parameter_flat_evidence(confirmation_evidence)):
                        raise ValueError('parameters_require_stopped_flat')
                    if confirmation_evidence.get('risk_locks'):
                        raise ValueError('risk_locked')
                    if parameters_limits is not None:
                        from .parameter_config import upgrade_document
                        document = upgrade_document(document, parameters_limits)
                        parameter_record = document['saved'] or document['current']
                    document['current'], document['saved'] = parameter_record, None
                    self._write_operator_parameters(parameters_scope, document)
            intent = {'start': 'RUNNING', 'stop': 'STOPPING'}.get(action, control_before['intent'])
            self.db.execute('''UPDATE operator_controls SET intent=?,
                intent_revision=intent_revision+1,updated_ts=? WHERE scope_key=?''',
                (intent, committed_ts, scope_key))
            control = self.operator_control_state(scope_key)
            operation_id = str(uuid4())
            receipt = {
                'operation_id': operation_id, 'scope_key': scope_key, 'action': action,
                'idempotency_key': idempotency_key, 'request_fingerprint': request_fingerprint,
                'expected_revision': expected_revision, 'expected_cycle_id': expected_cycle_id,
                'accepted_ts': committed_ts, 'control_before': control_before, 'control': control,
                'confirmation_evidence': confirmation_evidence,
                'cycle_before': cycle_before,
                'cycle': self.active_live_cycle() if cycle_before is not None else None,
                'orders': orders,
            }
            if parameter_record is not None:
                receipt['parameters'] = parameter_record
            self.db.execute('''INSERT INTO operator_operations
                (operation_id,scope_key,idempotency_key,request_fingerprint,action,
                 expected_cycle_id,state,receipt_json,evidence_json,accepted_ts,updated_ts)
                VALUES (?,?,?,?,?,?,?,?,'{}',?,?)''',
                (operation_id, scope_key, idempotency_key, request_fingerprint, action,
                 expected_cycle_id, 'completed' if parameter_record is not None else 'accepted',
                 json.dumps(receipt, sort_keys=True, allow_nan=False),
                 committed_ts, committed_ts))
            return self.operator_operation(scope_key, operation_id)

    def advance_operator_stop(self, scope_key: str, *, authoritative_flat: bool,
                              now: float | None = None) -> dict:
        """The owner supplies fresh 0/0 proof; pending orders/cycles are checked here."""
        scope_key = self._operator_key(scope_key)
        if type(authoritative_flat) is not bool:
            raise ValueError('explicit authoritative flat proof required')
        with self.transaction():
            control = self.operator_control_state(scope_key)
            if (control['intent'] == 'STOPPING' and authoritative_flat
                    and not self.pending() and self.active_live_cycle() is None):
                updated_ts = self._funding_time(time.time() if now is None else now)
                self.db.execute('''UPDATE operator_controls SET intent='STOPPED',
                    intent_revision=intent_revision+1,updated_ts=? WHERE scope_key=?''',
                    (updated_ts, scope_key))
                return self.operator_control_state(scope_key)
            return control

    def update_operator_operation(self, scope_key: str, operation_id: str, *,
                                  state: str, evidence: dict,
                                  now: float | None = None) -> dict:
        scope_key = self._operator_key(scope_key)
        operation_id = self._operator_key(operation_id)
        if state not in self.OPERATOR_OPERATION_STATES or not isinstance(evidence, dict):
            raise ValueError('invalid operator operation progress')
        evidence_json = json.dumps(evidence, sort_keys=True, allow_nan=False)
        updated_ts = self._funding_time(time.time() if now is None else now)
        with self.transaction():
            prior = self.operator_operation(scope_key, operation_id)
            if prior is None:
                raise KeyError(operation_id)
            if prior['state'] == 'completed' and state != 'completed':
                raise ValueError('completed operator operation cannot regress')
            self.db.execute('''UPDATE operator_operations SET state=?,evidence_json=?,updated_ts=?
                WHERE scope_key=? AND operation_id=?''',
                (state, evidence_json, updated_ts, scope_key, operation_id))
            return self.operator_operation(scope_key, operation_id)

    def prepare(self, intent: Intent) -> None:
        with self.transaction():
            pending = list(self.db.execute(
                "SELECT order_ref,cycle_id,reduce_only FROM orders "
                "WHERE venue=? AND state!='TERMINAL'",
                (intent.venue,)))
            # A close outcome can remain UNKNOWN while authoritative venue
            # positions already prove an exact residual on that same venue.
            # Permit only a same-cycle reduce-only follow-up in that case;
            # reduce-only at the exchange prevents the old and new requests
            # from combining into a reversed position.  Opening intents and
            # cross-cycle overlap remain fail-closed.
            safe_reduce_overlap = (
                intent.reduce_only and pending
                and all(bool(row['reduce_only'])
                        and row['cycle_id'] == intent.cycle_id
                        for row in pending))
            if pending and not safe_reduce_overlap:
                raise PendingOrder(
                    f'unresolved {intent.venue} order {pending[0]["order_ref"]}')
            self.db.execute('''INSERT INTO orders
                (order_ref,cycle_id,venue,side,requested_units,limit_px,reduce_only,state,had_unknown)
                VALUES (?,?,?,?,?,?,?,'PREPARED',0)''',
                (intent.order_ref, intent.cycle_id, intent.venue, intent.side.value,
                 intent.units, str(intent.limit_px), int(intent.reduce_only)))

    def mark_submitting(self, order_ref: str) -> None:
        with self.transaction():
            cursor = self.db.execute("UPDATE orders SET state='SUBMITTING' "
                                     "WHERE order_ref=? AND state='PREPARED'", (order_ref,))
            if cursor.rowcount != 1:
                raise PendingOrder('intent is not a fresh PREPARED intent')

    def prepare_submitting(self, intent: Intent) -> None:
        with self.transaction():
            self._insert_submitting(intent)

    def _insert_submitting(self, intent: Intent) -> None:
        """Atomically persist a fresh intent as possibly submitted."""
        pending = list(self.db.execute(
            "SELECT order_ref,cycle_id,reduce_only FROM orders "
            "WHERE venue=? AND state!='TERMINAL'", (intent.venue,)))
        safe_reduce_overlap = (
            intent.reduce_only and pending
            and all(bool(row['reduce_only'])
                    and row['cycle_id'] == intent.cycle_id
                    for row in pending))
        if pending and not safe_reduce_overlap:
            raise PendingOrder(
                f'unresolved {intent.venue} order {pending[0]["order_ref"]}')
        self.db.execute('''INSERT INTO orders
            (order_ref,cycle_id,venue,side,requested_units,limit_px,
             reduce_only,state,had_unknown) VALUES (?,?,?,?,?,?,?,'SUBMITTING',0)''',
            (intent.order_ref, intent.cycle_id, intent.venue,
             intent.side.value, intent.units, str(intent.limit_px),
             int(intent.reduce_only)))

    def prepare_dispatch(self, intent: Intent, *, step: dict,
                         request: dict | None = None) -> dict | None:
        """Commit the step, logical ticket and possible submission together."""
        identity = dict(cycle_id=intent.cycle_id, order_ref=intent.order_ref,
                        venue=intent.venue, side=intent.side.value,
                        requested_units=intent.units, reduce_only=intent.reduce_only)
        if any(key in step and step[key] != value for key, value in identity.items()):
            raise ValueError('dispatch step does not match intent')
        arguments = {**identity, **step}
        request = dict(request) if request is not None else None
        if request is not None:
            if request.pop('venue', intent.venue) != intent.venue \
                    or request.pop('cycle_id', intent.cycle_id) != intent.cycle_id:
                raise ValueError('dispatch request does not match intent')
        with self.transaction():
            event_ts = None
            if request is not None:
                event_ts = self._insert_request_event(
                    intent.venue, cycle_id=intent.cycle_id, **request)
                if event_ts is None:
                    return None
            prepared = self._ensure_live_step(**arguments)
            if prepared['applied']:
                raise PendingOrder('dispatch step was already applied')
            self._insert_submitting(intent)
            prepared['request_event_ts'] = event_ts
            return prepared

    def discard_unsent_dispatch(self, intent: Intent, *,
                                request_event_ts: float | None = None) -> None:
        """Only a transport's explicit no-POST proof can undo SUBMITTING."""
        with self.transaction():
            row = self.order(intent.order_ref)
            if (row['state'] != 'SUBMITTING' or row['filled_units']
                    or row['had_unknown'] or row['submitted_ts'] is not None
                    or row['cycle_id'] != intent.cycle_id or row['venue'] != intent.venue):
                raise PendingOrder('possibly submitted order cannot be discarded')
            step = self.db.execute(
                'SELECT * FROM live_steps WHERE order_ref=?', (intent.order_ref,)).fetchone()
            if step is not None and step['applied']:
                raise PendingOrder('applied dispatch cannot be discarded')
            self.db.execute('DELETE FROM orders WHERE order_ref=?', (intent.order_ref,))
            self.db.execute('DELETE FROM live_steps WHERE order_ref=?', (intent.order_ref,))
            if request_event_ts is not None:
                self.db.execute('''DELETE FROM request_events
                    WHERE venue=? AND used_ts=? AND cycle_id=?''',
                    (intent.venue, request_event_ts, intent.cycle_id))

    def record(self, order_ref: str, result: Result, *,
               submitted_ts: float | None = None,
               timing: dict | None = None,
               clock_session_id: str | None = None,
               known_partial: bool = False) -> None:
        if result.deferred:
            raise ValueError('proved-unsent deferral is not an order outcome')
        with self.transaction():
            old = self.order(order_ref)
            unknown = not result.terminal and not known_partial
            state = ('TERMINAL' if result.terminal else
                     'SUBMITTING' if known_partial and old['state'] != 'UNKNOWN' else 'UNKNOWN')
            if result.cumulative_units < old['filled_units'] \
                    or result.cumulative_units > old['requested_units']:
                raise ValueError('cumulative filled units regressed/exceeded request')
            if old['state'] == 'TERMINAL' and (
                not result.terminal or result.cumulative_units != old['filled_units']
            ):
                raise ValueError('terminal fill quantity/state changed')
            # A previous VWAP only proves the price of its cumulative quantity.
            avg = (str(result.avg_px) if result.avg_px is not None else
                   old['avg_px'] if result.cumulative_units == old['filled_units'] else None)
            fills = list(self.db.execute('SELECT units,price FROM fee_fills WHERE order_ref=?',
                                         (order_ref,)))
            if result.cumulative_units > 0 and sum(row['units'] for row in fills) == result.cumulative_units:
                # Full price coverage is authoritative even when some fees are unknown.
                avg = str(sum((row['units'] * dec(row['price']) for row in fills),
                              Decimal('0')) / result.cumulative_units)
            known_fee = (str(result.fee_usd) if result.fee_usd is not None
                         else old['known_fee_usd'])
            covered = (result.cumulative_units if result.fee_usd is not None
                       else old['fee_covered_units'])
            source = 'cumulative' if result.fee_usd is not None else old['fee_source']
            if (old['fee_source'] == 'fill_history'
                    and old['fee_covered_units'] == result.cumulative_units):
                # A repeated receipt cannot replace a complete identified history.
                known_fee, covered, source = (old['known_fee_usd'],
                                             old['fee_covered_units'], old['fee_source'])
            fee = known_fee if covered == result.cumulative_units else None
            self.db.execute('''UPDATE orders SET state=?,filled_units=?,avg_px=?,fee_usd=?,note=?,
                            known_fee_usd=?,fee_covered_units=?,fee_source=?,
                            had_unknown=CASE WHEN ? THEN 1 ELSE had_unknown END,
                            terminal_recorded_ts=COALESCE(terminal_recorded_ts,?)
                            WHERE order_ref=?''',
                            (state,
                             result.cumulative_units, avg, fee, result.note,
                             known_fee, covered, source,
                             int(unknown or old['state'] == 'UNKNOWN'),
                             time.time() if result.terminal else None,
                             order_ref))
            if fee is None:
                self.db.execute('''UPDATE execution_economics_steps SET actual_fee_usd=NULL
                    WHERE order_ref=?''', (order_ref,))
            timing = timing or {}
            self.db.execute('''UPDATE orders SET
                first_unknown_ts=COALESCE(first_unknown_ts,?),
                submitted_ts=COALESCE(submitted_ts,?),
                terminal_received_ts=COALESCE(terminal_received_ts,?),
                terminal_received_mono_ns=COALESCE(terminal_received_mono_ns,?),
                terminal_clock_session_id=COALESCE(terminal_clock_session_id,?),
                terminal_source=COALESCE(terminal_source,?),
                terminal_status=COALESCE(terminal_status,?) WHERE order_ref=?''',
                (time.time() if unknown else None, submitted_ts,
                 timing.get('terminal_result_ts') if result.terminal else None,
                 timing.get('terminal_result_mono_ns') if result.terminal else None,
                 clock_session_id if result.terminal and timing.get('terminal_result_mono_ns') else None,
                 timing.get('confirmation_source') if result.terminal else None,
                 timing.get('result_status') if result.terminal else None, order_ref))
            self._invalidate_zero_fee_finality(old['cycle_id'])

    def order(self, ref: str) -> sqlite3.Row:
        row = self.db.execute('SELECT * FROM orders WHERE order_ref=?', (ref,)).fetchone()
        if row is None:
            raise KeyError(ref)
        return row

    def pending(self) -> list[dict]:
        return [dict(r) for r in self.db.execute("SELECT * FROM orders WHERE state!='TERMINAL'")]

    def cycle_orders(self, cycle_id: str) -> list[dict]:
        return [dict(r) for r in self.db.execute(
            'SELECT rowid AS sequence,* FROM orders WHERE cycle_id=? ORDER BY rowid',
            (cycle_id,))]

    def record_fee_fills(self, cycle_id: str, venue: str, fills: Iterable[dict]) -> None:
        """Persist identified fills independently of the other venue and missing fees."""
        if venue not in {'entropy', 'hedge'}:
            raise ValueError('invalid fee venue')
        with self.transaction():
            self._record_fee_fills(cycle_id, venue, fills)

    def _record_fee_fills(self, cycle_id: str, venue: str, fills: Iterable[dict], *,
                          refresh_all: bool = True) -> None:
        refs = ({row['order_ref'] for row in self.cycle_orders(cycle_id)
                 if row['venue'] == venue and row['filled_units'] > 0}
                if refresh_all else set())
        for fill in fills:
            key, ref, units = (fill.get(name) for name in
                               ('exchange_fill_key', 'order_ref', 'units'))
            if not isinstance(key, str) or not key or type(units) is not int or units <= 0:
                raise ValueError('identified positive fee fill required')
            order = self.order(ref)
            if (order['cycle_id'] != cycle_id or order['venue'] != venue
                    or fill.get('side') != order['side']):
                raise ValueError('fee fill order identity mismatch')
            price = dec(fill['price'])
            if price <= 0:
                raise ValueError('fee fill price must be positive')
            ts = self._funding_time(fill['ts'])
            fee = dec(fill['fee_usd']) if fill.get('fee_usd') is not None else None
            old = self.db.execute('''SELECT * FROM fee_fills
                WHERE venue=? AND exchange_fill_key=?''', (venue, key)).fetchone()
            if old and (old['cycle_id'] != cycle_id or old['order_ref'] != ref
                        or old['units'] != units or dec(old['price']) != price or old['ts'] != ts
                        or (old['fee_usd'] is not None and fee is not None
                            and dec(old['fee_usd']) != fee)):
                raise ValueError('conflicting fee fill identity or amount')
            self.db.execute('''INSERT INTO fee_fills
                (venue,exchange_fill_key,cycle_id,order_ref,units,price,ts,fee_usd)
                VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(venue,exchange_fill_key)
                DO UPDATE SET fee_usd=COALESCE(fee_fills.fee_usd,excluded.fee_usd)''',
                (venue, key, cycle_id, ref, units, str(price), ts,
                 str(fee) if fee is not None else None))
            refs.add(ref)
        for ref in refs:
            order = self.order(ref)
            if order['fee_covered_units'] != order['filled_units']:
                self.db.execute('UPDATE orders SET fee_usd=NULL WHERE order_ref=?', (ref,))
                self.db.execute('''UPDATE execution_economics_steps SET actual_fee_usd=NULL
                    WHERE order_ref=?''', (ref,))
            rows = list(self.db.execute('SELECT * FROM fee_fills WHERE order_ref=?', (ref,)))
            units = sum(row['units'] for row in rows)
            if units > order['filled_units']:
                raise ValueError('fee fills exceed recorded quantity')
            if units == order['filled_units'] and units > 0:
                average = sum((dec(row['price']) * row['units'] for row in rows),
                              Decimal('0')) / units
                self.db.execute('UPDATE orders SET avg_px=? WHERE order_ref=?', (str(average), ref))
                if all(row['fee_usd'] is not None for row in rows):
                    fee = sum((dec(row['fee_usd']) for row in rows), Decimal('0'))
                    self.db.execute('''UPDATE orders SET fee_usd=?,known_fee_usd=?,
                        fee_covered_units=?,fee_source='fill_history' WHERE order_ref=?''',
                        (str(fee), str(fee), units, ref))
        self._invalidate_zero_fee_finality(cycle_id)

    @staticmethod
    def external_fill_order_ref(venue: str, exchange_fill_key: str) -> str:
        if venue not in {'entropy', 'hedge'} or not isinstance(exchange_fill_key, str) or not exchange_fill_key:
            raise ValueError('external fill identity required')
        digest = hashlib.sha256((venue + '\0' + exchange_fill_key).encode()).hexdigest()
        return f'external:{venue}:{digest}'

    def bind_external_cycle(self, cycle_id: str, *, binding_hash: str,
                            venue: str, account: str, market: str,
                            witness_order_ref: str | None = None) -> None:
        """Bind after runtime verifies actual venue and history coverage.

        Runtime matches the legacy witness cloid, side, quantity and price to
        fetched history. A new empty venue can instead bind at cycle birth.
        """
        if (venue not in {'entropy', 'hedge'} or not isinstance(binding_hash, str)
                or len(binding_hash) != 64 or any(c not in '0123456789abcdef' for c in binding_hash)
                or not isinstance(account, str) or not account
                or not isinstance(market, str) or not market):
            raise ValueError('external account and market binding required')
        with self.transaction():
            old = self.db.execute('SELECT * FROM external_cycle_bindings WHERE cycle_id=? AND venue=?',
                                  (cycle_id, venue)).fetchone()
            if old is not None:
                if (old['binding_hash'], old['account'], old['market']) != (binding_hash, account, market):
                    raise ValueError('external cycle binding changed')
                return
            cycle = self.db.execute('SELECT * FROM live_cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
            if cycle is None or cycle['phase'] == 'DONE':
                raise ValueError('active external cycle required')
            if witness_order_ref is not None:
                witness = self.order(witness_order_ref)
                if (witness['cycle_id'] != cycle_id or witness['venue'] != venue
                        or witness['state'] != 'TERMINAL' or witness['filled_units'] <= 0
                        or witness['avg_px'] is None or witness_order_ref.startswith('external:')):
                    raise ValueError('original external binding witness required')
            elif (cycle['phase'] != 'OPEN_FIRST' or self.db.execute(
                    'SELECT 1 FROM orders WHERE cycle_id=? AND venue=?', (cycle_id, venue)).fetchone()):
                raise ValueError('legacy external cycle requires original fill witness')
            self.db.execute('INSERT INTO external_cycle_bindings VALUES (?,?,?,?,?,?,?)',
                            (cycle_id, venue, binding_hash, account, market, witness_order_ref, time.time()))

    def external_fills(self, cycle_id: str) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            'SELECT * FROM external_fills WHERE cycle_id=? ORDER BY ts,rowid', (cycle_id,))]

    def external_inventory_at(self, cycle_id: str, venue: str, ts: float,
                              quantity_step: Decimal) -> int:
        inventory = 0
        for order in self.cycle_orders(cycle_id):
            if order['venue'] != venue or not order['filled_units']:
                continue
            step = self.db.execute('SELECT quantity_step FROM live_steps WHERE order_ref=?',
                                   (order['order_ref'],)).fetchone()
            external = self.db.execute('SELECT ts FROM external_fills WHERE order_ref=?',
                                       (order['order_ref'],)).fetchone()
            covered_ts = external['ts'] if external is not None else order['terminal_received_ts']
            if (step is None or step['quantity_step'] is None
                    or dec(step['quantity_step']) != quantity_step or order['state'] != 'TERMINAL'
                    or covered_ts is None):
                raise ValueError('external fill lacks persistent original inventory evidence')
            if covered_ts > ts:
                submitted = order['submitted_ts']
                if (external is None and order['reduce_only'] and submitted is not None
                        and math.isfinite(submitted) and submitted > ts):
                    continue  # A later residual close cannot erase the earlier inventory chain.
                raise ValueError('external fill lacks persistent original inventory evidence')
            inventory += Side(order['side']).sign * order['filled_units']
        return inventory

    def record_external_fill(self, cycle_id: str, *, binding_hash: str,
                             venue: str, account: str, market: str,
                             exchange_order_id: str, exchange_fill_key: str,
                             trade_id: str, side: Side, units: int,
                             price: Decimal, fee_usd: Decimal | None, ts: float,
                             start_position: Decimal, quantity_step: Decimal,
                             raw_evidence: dict) -> bool:
        """Append a verified external reduction atomically, never a submission.

        Persistent order inventory proves attribution. Current authoritative
        positions remain untouched: an account snapshot may cover this history.
        Replay can enrich a missing fee without applying the trade twice.
        """
        ref = self.external_fill_order_ref(venue, exchange_fill_key)
        if (not isinstance(side, Side) or type(units) is not int or units <= 0
                or not isinstance(exchange_order_id, str) or not exchange_order_id
                or not isinstance(trade_id, str) or not trade_id or not isinstance(raw_evidence, dict)
                or type(raw_evidence.get('exchange_order_id')) not in (str, int)
                or type(raw_evidence.get('exchange_trade_id')) not in (str, int)):
            raise ValueError('identified external trade required')
        try:
            price, start_position, quantity_step = map(dec, (price, start_position, quantity_step))
            fee = dec(fee_usd) if fee_usd is not None else None
            evidence_quantity = dec(raw_evidence.get('quantity_exact'))
            evidence_price = dec(raw_evidence.get('price_exact'))
            evidence_position = dec(raw_evidence.get('start_position'))
            evidence_fee = dec(raw_evidence['fee_usd']) if raw_evidence.get('fee_usd') is not None else None
        except (ArithmeticError, TypeError) as exc:
            raise ValueError('invalid external decimal evidence') from exc
        if (not all(v.is_finite() for v in (price, start_position, quantity_step))
                or price <= 0 or quantity_step <= 0 or not start_position
                or (fee is not None and not fee.is_finite())):
            raise ValueError('invalid external trade amounts')
        ts = self._funding_time(ts)
        observed_ts = self._funding_time(raw_evidence.get('evidence_received_ts'))
        quantity = units * quantity_step
        if observed_ts < ts or side.sign * start_position >= 0 or quantity > abs(start_position):
            raise ValueError('external fill must only reduce existing inventory')
        if (raw_evidence.get('source_account') != account
                or raw_evidence.get('source_market') != market
                or raw_evidence.get('exchange_fill_key') != exchange_fill_key
                or str(raw_evidence.get('exchange_order_id')) != exchange_order_id
                or str(raw_evidence.get('exchange_trade_id')) != trade_id
                or raw_evidence.get('side') != side.value
                or raw_evidence.get('is_reduction') is not True
                or raw_evidence.get('ts') != ts
                or evidence_quantity != quantity or evidence_price != price
                or evidence_position != start_position or evidence_fee != fee
                or not isinstance(raw_evidence.get('raw'), dict)):
            raise ValueError('external normalized evidence mismatch')
        raw_json = json.dumps(raw_evidence, sort_keys=True, separators=(',', ':'), allow_nan=False)
        fact = (cycle_id, binding_hash, account, market, exchange_order_id, trade_id,
                side.value, units, str(price), ts, str(start_position), str(quantity_step))
        with self.transaction():
            binding = self.db.execute('SELECT * FROM external_cycle_bindings WHERE cycle_id=? AND venue=?',
                                      (cycle_id, venue)).fetchone()
            if binding is None or (binding['binding_hash'], binding['account'], binding['market']) != (
                    binding_hash, account, market):
                raise ValueError('external trade lacks matching cycle binding')
            old = self.db.execute('SELECT * FROM external_fills WHERE venue=? AND exchange_fill_key=?',
                                  (venue, exchange_fill_key)).fetchone()
            if old is not None:
                previous = tuple(old[k] for k in ('cycle_id', 'binding_hash', 'account', 'market',
                    'exchange_order_id', 'trade_id', 'side', 'units', 'price', 'ts', 'start_position', 'quantity_step'))
                for index in (8, 10, 11):
                    if dec(previous[index]) != dec(fact[index]):
                        raise ValueError('conflicting external fill amounts')
                if previous[:8] != fact[:8] or previous[9] != fact[9]:
                    raise ValueError('conflicting external fill identity')
                if old['fee_usd'] is not None and fee is not None and dec(old['fee_usd']) != fee:
                    raise ValueError('conflicting external fill fee')
                if old['fee_usd'] is None and fee is not None:
                    self._record_fee_fills(cycle_id, venue, [dict(exchange_fill_key=exchange_fill_key,
                        order_ref=ref, side=side.value, units=units, price=price, ts=ts, fee_usd=fee)],
                        refresh_all=False)
                    self.db.execute('UPDATE external_fills SET fee_usd=?,fee_evidence_json=? WHERE order_ref=?',
                                    (str(fee), raw_json, ref))
                    step = self.db.execute('SELECT cash_delta_usd FROM live_steps WHERE order_ref=?', (ref,)).fetchone()
                    self.db.execute('UPDATE live_steps SET cash_delta_usd=?,accounting_final=1 WHERE order_ref=?',
                                    (str(dec(step['cash_delta_usd']) - fee), ref))
                    cycle = self.db.execute('SELECT cash_net_usd FROM live_cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
                    self.db.execute('UPDATE live_cycles SET cash_net_usd=? WHERE cycle_id=?',
                                    (str(dec(cycle['cash_net_usd']) - fee), cycle_id))
                return False
            if self.db.execute('SELECT 1 FROM external_fills WHERE venue=? AND account=? AND market=? AND trade_id=?',
                               (venue, account, market, trade_id)).fetchone():
                raise ValueError('external trade identity already has another fill key')
            if self.db.execute('SELECT 1 FROM fee_fills WHERE venue=? AND exchange_fill_key=?',
                               (venue, exchange_fill_key)).fetchone() or self.db.execute(
                    'SELECT 1 FROM orders WHERE order_ref=?', (ref,)).fetchone():
                raise ValueError('external fill already belongs to another order')
            if self.db.execute("SELECT 1 FROM orders WHERE venue=? AND state!='TERMINAL'", (venue,)).fetchone():
                raise PendingOrder('same venue pending order prevents external attribution')
            candidates = list(self.db.execute('''SELECT l.*,c.completed_ts FROM live_cycles l
                JOIN cycles c USING(cycle_id) WHERE l.started_ts<=?
                AND (c.completed_ts IS NULL OR c.completed_ts>=?)
                AND NOT EXISTS (SELECT 1 FROM live_cycles n WHERE n.started_ts>l.started_ts AND n.started_ts<=?)''',
                (ts, ts, ts)))
            if len(candidates) != 1 or candidates[0]['cycle_id'] != cycle_id or candidates[0]['phase'] == 'DONE':
                raise ValueError('external fill has no unique active cycle time window')
            cycle = candidates[0]
            inventory = self.external_inventory_at(cycle_id, venue, ts, quantity_step)
            if inventory * quantity_step != start_position:
                raise ValueError('external fill start position does not match inventory chain')
            prior_time = self.db.execute('SELECT MAX(ts) FROM external_fills WHERE cycle_id=? AND venue=?',
                                         (cycle_id, venue)).fetchone()[0]
            if prior_time is not None and ts < prior_time:
                raise ValueError('external fill chronology regressed')
            cash = -side.sign * quantity * price - (fee if fee is not None else Decimal('0'))
            self.db.execute('''INSERT INTO orders(order_ref,cycle_id,venue,side,requested_units,limit_px,
                reduce_only,state,filled_units,avg_px,note,terminal_recorded_ts,terminal_received_ts,
                terminal_source,terminal_status) VALUES (?,?,?,?,?,?,1,'TERMINAL',?,?,?, ?,?,'external_fill_history','external_reduction')''',
                (ref, cycle_id, venue, side.value, units, str(price), units, str(price),
                 'external_reduction_fact; not a robot submission', observed_ts, observed_ts))
            step_key = f'{cycle_id}:{ref}'
            self._ensure_live_step(step_key=step_key, cycle_id=cycle_id, order_ref=ref,
                venue=venue, side=side.value, requested_units=units, reduce_only=True,
                execution_group_id=step_key, quantity_step=quantity_step)
            self._record_fee_fills(cycle_id, venue, [dict(exchange_fill_key=exchange_fill_key,
                order_ref=ref, side=side.value, units=units, price=price, ts=ts, fee_usd=fee)],
                refresh_all=False)
            self.db.execute('''UPDATE live_steps SET applied=1,result_units=?,cash_delta_usd=?,
                accounting_final=? WHERE step_key=?''', (units, str(cash), int(fee is not None), step_key))
            self.db.execute('''UPDATE live_cycles SET cash_net_usd=?,accounting_final=0,
                recovery_used=1,updated_ts=MAX(updated_ts,?) WHERE cycle_id=?''',
                (str(dec(cycle['cash_net_usd']) + cash), observed_ts, cycle_id))
            self.db.execute('''UPDATE cycle_economics SET recovery_used=1,
                clean_summary_eligible=0,telemetry_incomplete=1,updated_ts=MAX(updated_ts,?)
                WHERE cycle_id=?''', (observed_ts, cycle_id))
            self.db.execute('''INSERT INTO external_fills VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (venue, exchange_fill_key, ref, cycle_id, binding_hash, account, market,
                 exchange_order_id, trade_id, side.value, units, str(price),
                 str(fee) if fee is not None else None, ts, str(start_position), str(quantity_step),
                 raw_json, None, observed_ts))
            return True

    def authorize_lighter_zero_fee(self, cycle_id: str, *, policy: dict) -> bool:
        """Audit a user policy separately; never replace original NULL fee evidence."""
        evidence = self._flat_basis_orders(cycle_id)
        if evidence is None or evidence[0]['batch_id'] != policy['batch_id']:
            return False
        orders = [row for row in evidence[1] if row['venue'] == 'hedge' and row['filled_units']]
        if not orders:
            return False
        key = f'lighter_zero_fee:{cycle_id}'
        scope = {**policy, 'order_units': {row['order_ref']: row['filled_units'] for row in orders}}
        with self.transaction():
            stored = self.db.execute('SELECT value FROM metadata WHERE key=?', (key,)).fetchone()
            if stored is not None:
                previous = json.loads(stored['value'])
                if any(previous.get(name) != value for name, value in scope.items()):
                    raise ValueError('conflicting Lighter zero-fee policy')
                # Existing grants remain auditable; actual per-order fees take priority below.
                return True
            else:
                fills = [dict(row) for row in self.db.execute(
                    "SELECT * FROM fee_fills WHERE cycle_id=? AND venue='hedge'", (cycle_id,))]
                if (any(row[name] is not None and dec(row[name]) != 0
                        for row in orders for name in ('fee_usd', 'known_fee_usd'))
                        or any(row['fee_usd'] is not None and dec(row['fee_usd']) != 0 for row in fills)):
                    return False  # Do not grant over contradictory actual evidence.
                audit = {**scope, 'authorized_ts': time.time(),
                         'original_orders': [{name: row[name] for name in (
                             'order_ref', 'fee_usd', 'known_fee_usd', 'fee_covered_units', 'fee_source')}
                             for row in orders],
                         'original_fill_fees': [{name: row[name] for name in (
                             'exchange_fill_key', 'order_ref', 'fee_usd')} for row in fills]}
                self.db.execute('INSERT INTO metadata(key,value) VALUES (?,?)',
                                (key, json.dumps(audit, sort_keys=True)))
        return True

    def _invalidate_zero_fee_finality(self, cycle_id: str) -> None:
        row = self.db.execute('''SELECT c.accounting_final,c.pnl_usd,e.actual_fees_usd FROM cycles c
            LEFT JOIN cycle_economics e USING(cycle_id) WHERE c.cycle_id=?''', (cycle_id,)).fetchone()
        if (row is None or not row['accounting_final']
                or not self.db.execute('SELECT 1 FROM metadata WHERE key=?',
                                      (f'lighter_zero_fee:{cycle_id}',)).fetchone()):
            return
        fees = self.fee_state(cycle_id)
        if (fees['fees_complete'] and row['actual_fees_usd'] is not None
                and fees['known_fees_usd'] == dec(row['actual_fees_usd'])):
            return
        # Preserve already confirmed funding in known risk cash while finality is revoked.
        # Adjust only the changed fees; a failed funding re-read cannot erase prior cash.
        known_pnl = dec(row['pnl_usd'])
        if row['actual_fees_usd'] is not None:
            known_pnl += dec(row['actual_fees_usd']) - fees['known_fees_usd']
        self.db.execute('''UPDATE cycles SET accounting_final=0,complete_accounting=1,
            pnl_usd=?,pending_known_cash_usd=NULL,pending_cost_estimate_usd=NULL
            WHERE cycle_id=?''', (str(known_pnl), cycle_id))
        self.db.execute('UPDATE live_cycles SET accounting_final=0 WHERE cycle_id=?', (cycle_id,))
        self.db.execute('''UPDATE cycle_economics SET accounting_final=0,finalized_pnl_usd=NULL,
            finalized_pnl_bps=NULL,clean_summary_eligible=0,telemetry_incomplete=1
            WHERE cycle_id=?''', (cycle_id,))

    def fee_state(self, cycle_id: str, *, use_zero_fee_policy: bool = True) -> dict:
        """One evidence set per order; prefix and fill fees are never added together."""
        fills = {}
        for row in self.db.execute('SELECT * FROM fee_fills WHERE cycle_id=?', (cycle_id,)):
            fills.setdefault(row['order_ref'], []).append(row)
        policy_row = (self.db.execute('SELECT value FROM metadata WHERE key=?',
                                     (f'lighter_zero_fee:{cycle_id}',)).fetchone()
                      if use_zero_fee_policy else None)
        policy = json.loads(policy_row['value']) if policy_row else None
        # Policy only covers the originally authorized, terminal flat quantity/price evidence.
        if policy and self._flat_basis_orders(cycle_id) is None:
            policy = None
        known = {'entropy': Decimal('0'), 'hedge': Decimal('0')}
        complete = {'entropy': True, 'hedge': True}
        histories = {'entropy': True, 'hedge': True}
        evidence_complete = True
        orders = {}
        for order in self.cycle_orders(cycle_id):
            units = order['filled_units']
            if not units:
                continue
            rows = fills.get(order['order_ref'], [])
            covered_rows = [row for row in rows if row['fee_usd'] is not None]
            full = (order['fee_usd'] is not None and order['fee_covered_units'] == units)
            history = (sum(row['units'] for row in rows) == units
                       and len(covered_rows) == len(rows) and order['state'] == 'TERMINAL')
            policy_covered = bool(
                policy and order['venue'] == 'hedge'
                and policy['order_units'].get(order['order_ref']) == units)
            policy_zero = bool(policy_covered
                and all(order[key] is None or dec(order[key]) == 0
                        for key in ('fee_usd', 'known_fee_usd'))
                and all(row['fee_usd'] is None or dec(row['fee_usd']) == 0 for row in rows))
            if policy_zero:
                amount, covered, source = Decimal('0'), units, 'user_policy_zero_fee'
                full = True
            elif full:
                amount, covered, source = dec(order['fee_usd']), units, order['fee_source']
            elif covered_rows and sum(row['units'] for row in covered_rows) >= (
                    order['fee_covered_units'] or 0):
                amount = sum((dec(row['fee_usd']) for row in covered_rows), Decimal('0'))
                covered, source = sum(row['units'] for row in covered_rows), 'fill_history_partial'
            elif order['known_fee_usd'] is not None and order['fee_covered_units'] is not None:
                amount, covered, source = (dec(order['known_fee_usd']), order['fee_covered_units'],
                                          'cumulative_partial')
            else:
                amount, covered, source = Decimal('0'), 0, 'unknown'
            full = full and order['state'] == 'TERMINAL'
            orders[order['order_ref']] = {'known_fee_usd': amount, 'fee_covered_units': covered,
                                        'fee_complete': full, 'history_complete': history,
                                        'fee_source': source,
                                        'fee_policy_source': policy['source'] if policy_covered else None}
            evidence_complete &= history or (policy_covered and full)
            known[order['venue']] += amount
            complete[order['venue']] &= full
            histories[order['venue']] &= history
        return {'known_fees_by_venue': known, 'fees_complete_by_venue': complete,
                'known_fees_usd': sum(known.values(), Decimal('0')),
                'fees_complete': all(complete.values()), 'history_complete': all(histories.values()),
                'accounting_evidence_complete': evidence_complete, 'order_fees': orders}

    def cancel_unsent_prepared(self, ref: str) -> None:
        # PREPARED -> SUBMITTING is durably committed BEFORE adapter invocation.
        # Therefore only PREPARED can be proven unsent using this journal alone.
        row = self.order(ref)
        if row['state'] != 'PREPARED':
            raise PendingOrder('possibly submitted intent requires exchange evidence')
        self.record(ref, Result(True, 0, note='proved_unsent_prepared'))

    def ensure_batch(self, batch_id: str, *, max_position_cycles: int | None,
                     loss_budget_usd: Decimal | None) -> None:
        if not batch_id or (max_position_cycles is not None
                and (type(max_position_cycles) is not int or max_position_cycles < 1)) \
                or (loss_budget_usd is not None
                    and (not loss_budget_usd.is_finite() or loss_budget_usd <= 0)):
            raise ValueError('invalid explicit batch authorization')
        with self.transaction():
            row = self.db.execute('SELECT * FROM batches WHERE batch_id=?', (batch_id,)).fetchone()
            if row:
                budget = None if row['loss_budget'] == 'disabled' else dec(row['loss_budget'])
                cap = None if row['max_position_cycles'] == 'disabled' else row['max_position_cycles']
                if cap != max_position_cycles or budget != loss_budget_usd:
                    raise ValueError('cannot silently reset/enlarge existing batch authorization')
            self.db.execute('''INSERT OR IGNORE INTO batches
                (batch_id,max_position_cycles,loss_budget,closed_for_entries)
                VALUES (?,?,?,0)''',
                            (batch_id, 'disabled' if max_position_cycles is None else max_position_cycles,
                             'disabled' if loss_budget_usd is None else str(loss_budget_usd)))

    def batch_state(self, batch_id: str) -> dict:
        row = self.db.execute('SELECT * FROM batches WHERE batch_id=?', (batch_id,)).fetchone()
        if row is None:
            raise KeyError(batch_id)
        budget = None if row['loss_budget'] == 'disabled' else dec(row['loss_budget'])
        cap = None if row['max_position_cycles'] == 'disabled' else row['max_position_cycles']
        if cap is not None and (type(cap) is not int or cap < 1):
            raise ValueError('invalid persisted position cycle cap')
        cycles = list(self.db.execute('''SELECT c.*,f.known_cash_result_usd,
            COALESCE(f.trade_accounting_complete=1 AND l.phase='DONE'
                AND l.entropy_units=0 AND l.hedge_units=0
                AND l.position_evidence_authoritative=1
                AND f.closed_poll_complete=1
                AND ((f.completed=1 AND f.exit_reason='two_settlements_each'
                        AND (SELECT COUNT(*) FROM (
                            SELECT venue FROM funding_settlements s
                            WHERE s.cycle_id=c.cycle_id AND s.ts>f.hedged_ts
                            GROUP BY venue HAVING COUNT(DISTINCT CAST(s.ts/3600 AS INTEGER))>=2
                        ))=2)
                    OR (c.result_kind='zero_fill' AND f.hedged_ts IS NULL
                        AND f.exit_reason IS NULL)),0) AS funding_reentry_ready
            FROM cycles c LEFT JOIN funding_cycles f USING(cycle_id)
            LEFT JOIN live_cycles l USING(cycle_id) WHERE c.batch_id=?''', (batch_id,)))
        complete = [r for r in cycles if r['complete_accounting']]
        # Pending cash and configured-fee estimates are never final PnL.
        basis_ready = {r['cycle_id'] for r in cycles
                       if not r['accounting_final'] and self._basis_accounting_ready(r['cycle_id'])}
        known_results = [dec(r['pending_known_cash_usd']) if r['cycle_id'] in basis_ready else
                         dec(r['pnl_usd']) if r['complete_accounting'] else
                         dec(r['known_cash_result_usd']) for r in cycles
                         if r['cycle_id'] in basis_ready or r['complete_accounting']
                         or r['known_cash_result_usd'] is not None]
        known_used = sum((max(-pnl, Decimal('0')) for pnl in known_results), Decimal('0'))
        estimates = sum((dec(r['pending_cost_estimate_usd']) for r in cycles
                        if r['cycle_id'] in basis_ready), Decimal('0'))
        used = known_used + estimates
        sample_count = sum(r['result_kind'] != 'zero_fill' for r in cycles)
        pending_accounting = sum(not r['complete_accounting'] or not r['accounting_final']
                                 for r in cycles)
        # Basis rounds require verified flat execution and separate configured-fee estimates.
        # Funding rounds retain their existing settlement/fee admission evidence.
        blocking_accounting = sum(
            r['cycle_id'] not in basis_ready and (
                not r['complete_accounting']
                or (not r['accounting_final'] and not r['funding_reentry_ready']
                    and not self.zero_execution_reentry_ready(r['cycle_id'])))
            for r in cycles)
        reason = None
        value = threshold = cycle_id = None
        if row['closed_for_entries']:
            reason = row['close_reason'] or 'LEGACY_UNATTRIBUTED'
            value, threshold, cycle_id = (row['close_value'],
                                          row['close_threshold'], row['close_cycle_id'])
        elif cap is not None and sample_count >= cap:
            reason = 'MAX_POSITION_CYCLES'
            value, threshold = sample_count, cap
        elif budget is not None and used >= budget:
            reason = ('PENDING_ACCOUNTING_ESTIMATE_BUDGET' if estimates
                      else 'REALIZED_LOSS_BUDGET')
            value, threshold = used, budget
        elif blocking_accounting:
            reason = 'PENDING_ACCOUNTING'
            value = blocking_accounting
        return {
            'attempts': len(cycles), 'position_cycles': sample_count,
            'pnl_usd': sum((dec(r['pnl_usd']) for r in complete), Decimal('0')),
            'loss_used_usd': used, 'loss_budget_usd': budget,
            'known_loss_used_usd': known_used,
            'pending_cost_estimate_usd': estimates,
            'max_position_cycles': cap,
            'pending_cycles': sum(r['result_kind'] is None for r in cycles),
            'pending_accounting': pending_accounting,
            'blocking_pending_accounting': blocking_accounting,
            'closed_for_entries': bool(row['closed_for_entries']),
            'close_reason': row['close_reason'], 'close_ts': row['close_ts'],
            'can_open': reason is None, 'block_reason': reason,
            'block_value': value, 'block_threshold': threshold,
            'block_cycle_id': cycle_id,
        }

    def _flat_basis_orders(self, cycle_id: str) -> tuple[dict, list[dict]] | None:
        row = self.db.execute('''SELECT c.result_kind,c.accounting_final AS final_accounting,l.* FROM cycles c
            JOIN live_cycles l USING(cycle_id) WHERE c.cycle_id=?''', (cycle_id,)).fetchone()
        if (row is None or row['result_kind'] is None or row['phase'] != 'DONE'
                or row['entropy_units'] or row['hedge_units']
                or not row['position_evidence_authoritative']
                or self.db.execute('SELECT 1 FROM funding_cycles WHERE cycle_id=?',
                                   (cycle_id,)).fetchone()):
            return None
        orders = [dict(r) for r in self.db.execute('''SELECT o.*,s.quantity_step
            FROM orders o LEFT JOIN live_steps s USING(order_ref) WHERE o.cycle_id=?''',
                                                 (cycle_id,))]
        balance = {'entropy': Decimal('0'), 'hedge': Decimal('0')}
        try:
            for order in orders:
                if order['state'] != 'TERMINAL':
                    return None
                if order['filled_units']:
                    step, price = dec(order['quantity_step']), dec(order['avg_px'])
                    if step <= 0 or price <= 0:
                        return None
                    balance[order['venue']] += Side(order['side']).sign * order['filled_units'] * step
        except (ValueError, TypeError, KeyError, ArithmeticError):
            return None
        return (dict(row), orders) if not any(balance.values()) else None

    def _basis_accounting_ready(self, cycle_id: str) -> bool:
        row = self.db.execute('SELECT * FROM cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
        if row is None or row['pending_known_cash_usd'] is None or row['pending_cost_estimate_usd'] is None:
            return False
        try:
            dec(row['pending_known_cash_usd'])
            return dec(row['pending_cost_estimate_usd']) >= 0 and self._flat_basis_orders(cycle_id) is not None
        except (ValueError, TypeError, ArithmeticError):
            return False

    def estimate_basis_accounting(self, cycle_id: str, *, fee_bps: dict[str, Decimal]) -> bool:
        """Keep configured-fee estimates separate from unknown actual costs/final PnL.

        Configured rates are estimates, not upper bounds on missing fees/funding.
        """
        evidence = self._flat_basis_orders(cycle_id)
        if evidence is None:
            return False
        cycle, orders = evidence
        if cycle['final_accounting']:
            return True
        prior = self.db.execute('SELECT complete_accounting,pending_known_cash_usd FROM cycles '
                                'WHERE cycle_id=?', (cycle_id,)).fetchone()
        if (prior['complete_accounting'] and prior['pending_known_cash_usd'] is None
                and self.db.execute('SELECT 1 FROM metadata WHERE key=?',
                                    (f'lighter_zero_fee:{cycle_id}',)).fetchone()):
            # Fee correction of a former final must recheck funding before reentry.
            return False
        gross = estimate = Decimal('0')
        fees = self.fee_state(cycle_id)
        for order in orders:
            if not order['filled_units']:
                continue
            key = order['venue']
            qty = order['filled_units'] * dec(order['quantity_step'])
            price = dec(order['avg_px'])
            notional = qty * price
            gross += notional if order['side'] == 'sell' else -notional
            if not fees['order_fees'][order['order_ref']]['fee_complete']:
                estimate += notional * max(Decimal('0'), dec(fee_bps[key])) / Decimal('10000')
        if any(order['filled_units'] for order in orders):
            start, end = cycle['started_ts'], cycle['last_authoritative_ts']
            if (not isinstance(start, (int, float)) or not isinstance(end, (int, float))
                    or not math.isfinite(start) or not math.isfinite(end) or start < 0 or end < start):
                return False
        with self.transaction():
            self.db.execute('''UPDATE cycles SET pending_known_cash_usd=?,pending_cost_estimate_usd=?
                WHERE cycle_id=? AND accounting_final=0''',
                            (str(gross - fees['known_fees_usd']), str(estimate), cycle_id))
        return True

    def _check_unresolved_cycles(self) -> None:
        batches = self.db.execute('''SELECT DISTINCT batch_id FROM cycles
            WHERE result_kind IS NULL OR complete_accounting=0 OR accounting_final=0''').fetchall()
        for row in batches:
            state = self.batch_state(row['batch_id'])
            if (state['pending_cycles'] or state['blocking_pending_accounting']
                    or (state['loss_budget_usd'] is not None
                        and state['loss_used_usd'] >= state['loss_budget_usd'])):
                raise PendingOrder('another cycle has unresolved exposure/accounting or exhausted estimated budget')

    def close_entries(self, batch_id: str, reason: str, *,
                      value: Decimal | None = None,
                      threshold: Decimal | None = None,
                      cycle_id: str | None = None) -> None:
        if reason not in self.CLOSE_REASONS:
            raise ValueError('invalid batch close reason')
        if any(item is not None and not item.is_finite()
               for item in (value, threshold)):
            raise ValueError('invalid batch close value')
        with self.transaction():
            self._close_entries(batch_id, reason, value=value,
                                threshold=threshold, cycle_id=cycle_id)

    def _close_entries(self, batch_id: str, reason: str, *,
                       value: Decimal | None = None,
                       threshold: Decimal | None = None,
                       cycle_id: str | None = None) -> None:
        cursor = self.db.execute('''UPDATE batches SET
            closed_for_entries=1,close_reason=?,close_ts=?,
            close_value=?,close_threshold=?,close_cycle_id=?
            WHERE batch_id=? AND closed_for_entries=0''',
            (reason, time.time(), str(value) if value is not None else None,
             str(threshold) if threshold is not None else None,
             cycle_id, batch_id))
        if cursor.rowcount == 0 and self.db.execute(
                'SELECT 1 FROM batches WHERE batch_id=?',
                (batch_id,)).fetchone() is None:
            raise KeyError(batch_id)

    def zero_execution_reentry_ready(self, cycle_id: str) -> bool:
        """A read-only exception for proved zero execution, never final accounting."""
        row = self.db.execute('''SELECT c.*,l.phase,l.entropy_units,l.hedge_units,
            l.position_evidence_authoritative,l.cash_net_usd,l.basis_profit_basis_json FROM cycles c
            JOIN live_cycles l USING(cycle_id) WHERE c.cycle_id=?''', (cycle_id,)).fetchone()
        if (row is None or row['result_kind'] != 'zero_fill' or row['phase'] != 'DONE'
                or row['position_evidence_authoritative'] != 1
                or row['entropy_units'] != 0 or row['hedge_units'] != 0
                or row['complete_accounting'] != 1 or row['pnl_usd'] is None
                or row['cash_net_usd'] is None
                or row['basis_profit_basis_json'] not in (None, self.BASIS_PROFIT_PENDING)):
            return False
        try:
            def zero_or_missing(value):
                return value is None or dec(value) == 0
            if not all(zero_or_missing(row[key]) for key in
                    ('pnl_usd', 'cash_net_usd', 'pending_known_cash_usd', 'pending_cost_estimate_usd')):
                return False
            orders = list(self.db.execute('''SELECT o.* FROM orders o
                WHERE o.cycle_id=? OR o.order_ref IN
                    (SELECT order_ref FROM live_steps WHERE cycle_id=? UNION
                     SELECT order_ref FROM execution_economics_steps WHERE cycle_id=?)''',
                                          (cycle_id, cycle_id, cycle_id)))
            for order in orders:
                if (order['cycle_id'] != cycle_id or order['state'] != 'TERMINAL'
                        or order['filled_units'] != 0 or not all(zero_or_missing(order[key])
                            for key in ('avg_px', 'fee_usd', 'known_fee_usd', 'fee_covered_units'))):
                    return False
            refs = {order['order_ref'] for order in orders}
            steps = list(self.db.execute('''SELECT * FROM live_steps WHERE cycle_id=? OR order_ref IN
                (SELECT order_ref FROM orders WHERE cycle_id=?)''', (cycle_id, cycle_id)))
            for step in steps:
                if (step['cycle_id'] != cycle_id or step['order_ref'] not in refs
                        or not all(zero_or_missing(step[key]) for key in ('result_units', 'cash_delta_usd'))):
                    return False
            if self.db.execute('''SELECT 1 FROM fee_fills WHERE cycle_id=? OR order_ref IN
                (SELECT order_ref FROM orders WHERE cycle_id=?) LIMIT 1''', (cycle_id, cycle_id)).fetchone():
                return False
            for table in ('funding_settlements', 'funding_cycles'):
                if self.db.execute(f'SELECT 1 FROM {table} WHERE cycle_id=? LIMIT 1', (cycle_id,)).fetchone():
                    return False
            economics = self.db.execute('SELECT * FROM cycle_economics WHERE cycle_id=?', (cycle_id,)).fetchone()
            if economics is not None:
                if not all(zero_or_missing(economics[key]) for key in ('actual_fees_usd',
                        'entropy_fees_usd', 'hedge_fees_usd', 'actual_funding_usd',
                        'entropy_funding_usd', 'hedge_funding_usd', 'finalized_pnl_usd',
                        'actual_entry_notional_usd', 'entropy_close_fill_units', 'lighter_close_fill_units')):
                    return False
                if any(economics[key] is not None for key in ('first_fill_observed_ts',
                        'first_fill_exchange_ts', 'actual_entry_basis_bps', 'actual_exit_basis_bps',
                        'entropy_close_fill_px', 'lighter_close_fill_px')):
                    return False
            for step in self.db.execute('''SELECT * FROM execution_economics_steps WHERE cycle_id=?
                OR order_ref IN (SELECT order_ref FROM orders WHERE cycle_id=?)
                OR step_key IN (SELECT step_key FROM live_steps WHERE cycle_id=?)''',
                                        (cycle_id, cycle_id, cycle_id)):
                if (step['cycle_id'] != cycle_id or step['order_ref'] not in refs
                        or not zero_or_missing(step['result_units']) or not zero_or_missing(step['actual_fee_usd'])
                        or any(step[key] is not None for key in ('actual_fill_ts', 'actual_fill_vwap',
                            'exchange_fill_ts', 'exchange_first_fill_ts', 'exchange_last_fill_ts'))
                        ):
                    return False
        except (ValueError, TypeError, ArithmeticError):
            return False
        return True

    def reopen_entries(self, batch_id: str, *, evidence: object) -> None:
        # The caller must supply a fresh authoritative venue snapshot; journal
        # positions alone cannot prove that both accounts are flat now.
        observed_ts = getattr(evidence, 'observed_ts', None)
        with self.transaction():
            if (getattr(evidence, 'fully_authoritative', None) is not True
                    or any(type(getattr(evidence, key, None)) is not int or getattr(evidence, key) != 0
                           for key in ('entropy_units', 'hedge_units'))
                    or type(observed_ts) not in (int, float) or not math.isfinite(observed_ts)
                    or not 0 <= time.time() - observed_ts <= 10):
                raise PendingOrder('fresh authoritative 0/0 positions required')
            if self.pending() or self.db.execute(
                    "SELECT 1 FROM live_cycles WHERE phase!='DONE' LIMIT 1"
            ).fetchone() or any(not self.zero_execution_reentry_ready(row['cycle_id'])
                for row in self.db.execute('SELECT cycle_id FROM cycles WHERE result_kind IS NULL '
                                          'OR complete_accounting=0 OR accounting_final=0')):
                raise PendingOrder('active cycle, pending order or accounting')
            state = self.batch_state(batch_id)
            if state['max_position_cycles'] is not None and state['position_cycles'] >= state['max_position_cycles']:
                raise PendingOrder('MAX_POSITION_CYCLES requires new authorization')
            if state['loss_budget_usd'] is not None \
                    and state['loss_used_usd'] >= state['loss_budget_usd']:
                raise PendingOrder('REALIZED_LOSS_BUDGET requires new authorization')
            self.db.execute('''UPDATE batches SET closed_for_entries=0,
                close_reason=NULL,close_ts=NULL,close_value=NULL,
                close_threshold=NULL,close_cycle_id=NULL WHERE batch_id=?''',
                (batch_id,))

    def provisional_cycles(self, batch_id: str) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            '''SELECT c.*,l.started_ts,l.updated_ts FROM cycles c
               JOIN live_cycles l USING(cycle_id)
               WHERE c.batch_id=? AND c.result_kind IS NOT NULL
                 AND c.accounting_final=0 ORDER BY l.started_ts''', (batch_id,))]

    def finalize_cycle_accounting(self, cycle_id: str, pnl_usd: Decimal) -> None:
        if not pnl_usd.is_finite():
            raise ValueError('non-finite final pnl')
        with self.transaction():
            row = self.db.execute('SELECT batch_id,result_kind FROM cycles WHERE cycle_id=?',
                                  (cycle_id,)).fetchone()
            if row is None or row['result_kind'] is None:
                raise PendingOrder('only completed cycles can be reconciled')
            self.db.execute('''UPDATE cycles SET pnl_usd=?,complete_accounting=1,
                               accounting_final=1,pending_known_cash_usd=NULL,
                               pending_cost_estimate_usd=NULL WHERE cycle_id=?''',
                            (str(pnl_usd), cycle_id))
            state = self.batch_state(row['batch_id'])
            budget = state['loss_budget_usd']
            if budget is not None and state['known_loss_used_usd'] >= budget:
                self._close_entries(
                    row['batch_id'], 'REALIZED_LOSS_BUDGET',
                    value=state['known_loss_used_usd'], threshold=budget,
                    cycle_id=cycle_id)

    def begin_cycle(self, batch_id: str, cycle_id: str) -> None:
        with self.transaction():
            if self.pending() or not self.batch_state(batch_id)['can_open']:
                raise PendingOrder('pending exposure/accounting or batch closed')
            # All batches share one slot; do not evade a pending previous batch.
            self._check_unresolved_cycles()
            self.db.execute('INSERT INTO cycles(cycle_id,batch_id) VALUES (?,?)', (cycle_id,batch_id))

    def create_live_cycle(self, *, batch_id: str, cycle_id: str, direction: str,
                          planned_units: int, reference_bps: Decimal,
                          entry_basis_bps: Decimal, started_ts: float,
                          planned_notional_usd: Decimal | None = None,
                          signal_ts: float | None = None,
                          signal_mono_ns: int | None = None,
                          signal_clock_session_id: str | None = None,
                          position_check_start_ts: float | None = None,
                          position_check_end_ts: float | None = None,
                          cycle_persist_start_ts: float | None = None) -> None:
        with self.transaction():
            self._create_live_cycle(
                batch_id=batch_id, cycle_id=cycle_id, direction=direction,
                planned_units=planned_units, reference_bps=reference_bps,
                entry_basis_bps=entry_basis_bps, started_ts=started_ts,
                planned_notional_usd=planned_notional_usd, signal_ts=signal_ts,
                signal_mono_ns=signal_mono_ns,
                signal_clock_session_id=signal_clock_session_id,
                position_check_start_ts=position_check_start_ts,
                position_check_end_ts=position_check_end_ts,
                cycle_persist_start_ts=cycle_persist_start_ts)

    def _create_live_cycle(self, *, batch_id: str, cycle_id: str, direction: str,
                          planned_units: int, reference_bps: Decimal,
                          entry_basis_bps: Decimal, started_ts: float,
                          planned_notional_usd: Decimal | None = None,
                          signal_ts: float | None = None,
                          signal_mono_ns: int | None = None,
                          signal_clock_session_id: str | None = None,
                          position_check_start_ts: float | None = None,
                          position_check_end_ts: float | None = None,
                          cycle_persist_start_ts: float | None = None) -> None:
        if direction not in {'long_entropy', 'short_entropy'} or planned_units < 1:
            raise ValueError('invalid live cycle')
        if self.pending() or not self.batch_state(batch_id)['can_open']:
            raise PendingOrder('pending exposure/accounting or batch closed')
        self._check_unresolved_cycles()
        if self.db.execute('SELECT 1 FROM live_cycles WHERE phase != ? LIMIT 1',
                           ('DONE',)).fetchone():
            raise PendingOrder('another live cycle is active')
        self.db.execute('INSERT INTO cycles(cycle_id,batch_id) VALUES (?,?)',
                        (cycle_id, batch_id))
        self.db.execute('''INSERT INTO live_cycles
            (cycle_id,batch_id,direction,planned_units,reference_bps,
             entry_basis_bps,accounting_final,phase,started_ts,updated_ts,basis_profit_basis_json)
            VALUES (?,?,?,?,?,?,0,'OPEN_FIRST',?,?,?)''',
            (cycle_id, batch_id, direction, planned_units,
             str(reference_bps), str(entry_basis_bps), started_ts, started_ts,
             self.BASIS_PROFIT_PENDING))
        dislocation = (reference_bps - entry_basis_bps
                       if direction == 'long_entropy'
                       else entry_basis_bps - reference_bps)
        frozen_signal_ts = started_ts if signal_ts is None else signal_ts
        self.db.execute('''INSERT INTO cycle_economics(
            cycle_id,batch_id,telemetry_version,direction,signal_ts,decision_ts,
            signal_mono_ns,signal_clock_session_id,
            reference_bps,signal_executable_basis_bps,entry_dislocation_bps,
            planned_units,planned_notional_usd,position_check_start_ts,
            position_check_end_ts,cycle_persist_start_ts,created_ts,updated_ts)
            VALUES (?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (cycle_id, batch_id, direction, frozen_signal_ts, frozen_signal_ts,
             signal_mono_ns, signal_clock_session_id,
             str(reference_bps),
             str(entry_basis_bps), str(dislocation), planned_units,
             str(planned_notional_usd) if planned_notional_usd is not None else None,
             position_check_start_ts, position_check_end_ts,
             cycle_persist_start_ts, started_ts, started_ts))

    def ensure_opening_experiment(self, experiment_id: str, *, batch_id: str,
                                  contract: dict) -> None:
        if not experiment_id or not isinstance(experiment_id, str):
            raise ValueError('opening experiment identity required')
        frozen = json.dumps(contract, sort_keys=True, separators=(',', ':'), default=str)
        with self.transaction():
            batch = self.batch_state(batch_id)
            if batch['loss_budget_usd'] is not None or batch['max_position_cycles'] != 20:
                raise ValueError('opening experiment requires its own twenty-attempt disabled-budget batch')
            row = self.db.execute('SELECT * FROM opening_experiments WHERE experiment_id=?',
                                  (experiment_id,)).fetchone()
            if row is not None:
                if row['batch_id'] != batch_id or row['contract_json'] != frozen:
                    raise ValueError('opening experiment contract changed across restart')
                return
            if self.db.execute('SELECT 1 FROM cycles WHERE batch_id=?', (batch_id,)).fetchone():
                raise ValueError('opening experiment requires a new dedicated batch')
            self.db.execute('INSERT INTO opening_experiments(experiment_id,batch_id,contract_json) VALUES (?,?,?)',
                            (experiment_id, batch_id, frozen))

    def opening_experiment_state(self, experiment_id: str) -> dict:
        row = self.db.execute('SELECT * FROM opening_experiments WHERE experiment_id=?',
                              (experiment_id,)).fetchone()
        if row is None:
            raise KeyError(experiment_id)
        attempts = [dict(x) for x in self.db.execute(
            'SELECT * FROM opening_attempts WHERE experiment_id=? ORDER BY slot', (experiment_id,))]
        return {**dict(row), 'attempts': len(attempts),
                'groups': {g: sum(x['group_name'] == g for x in attempts) for g in ('A', 'B')},
                'finished': len(attempts) == 20 and bool(attempts[-1]['terminal_flat']),
                'current_attempt': attempts[-1] if attempts else None}

    def rearm_opening_opportunity(self, experiment_id: str) -> None:
        with self.transaction():
            self.db.execute('UPDATE opening_experiments SET armed=1 WHERE experiment_id=?',
                            (experiment_id,))

    def opening_attempt(self, cycle_id: str) -> dict | None:
        row = self.db.execute('SELECT * FROM opening_attempts WHERE cycle_id=?', (cycle_id,)).fetchone()
        return dict(row) if row is not None else None

    def _prepare_opening_intents(self, plans: list[tuple[Intent, dict]]) -> dict[str, dict]:
        prepared = {}
        for intent, step in plans:
            arguments = dict(cycle_id=intent.cycle_id, order_ref=intent.order_ref,
                venue=intent.venue, side=intent.side.value, requested_units=intent.units,
                reduce_only=False, **step)
            if intent.reduce_only:
                raise ValueError('opening intent cannot be reduce-only')
            prepared[intent.order_ref] = self._ensure_live_step(**arguments)
            self._insert_submitting(intent)
        return prepared

    def prepare_standard_opening_pair(self, *, cycle: dict,
                                      plans: list[tuple[Intent, dict]],
                                      request: dict, initial: bool = False) -> dict | None:
        """Atomically admit two ordinary IOC identities, including a zero-fill retry."""
        with self.transaction():
            side = Side.BUY if cycle['direction'] == 'long_entropy' else Side.SELL
            groups = {step.get('execution_group_id') for _, step in plans}
            group = next(iter(groups), None)
            if (len(plans) != 2 or {i.venue for i, _ in plans} != {'entropy', 'hedge'}
                    or len(groups) != 1 or not isinstance(group, str) or not group.startswith(
                        f"{cycle['cycle_id']}:open_pair:")
                    or any(i.cycle_id != cycle['cycle_id'] or i.units != cycle['planned_units']
                        or i.reduce_only or i.side is not (side if i.venue == 'entropy' else side.opposite)
                        for i, _ in plans)):
                raise ValueError('ordinary opening requires one equal paired intent plan')
            if not initial:
                current = self.active_live_cycle()
                orders = self.cycle_orders(cycle['cycle_id'])
                batch = self.batch_state(cycle['batch_id'])
                if (current is None or current['cycle_id'] != cycle['cycle_id']
                        or current['direction'] != cycle['direction']
                        or current['basis_exit_requested_ts'] is not None or self.pending()
                        or current['entropy_units'] or current['hedge_units']
                        or not orders or any(o['filled_units'] or o['state'] != 'TERMINAL' for o in orders)
                        or self.opening_attempt(cycle['cycle_id']) is not None
                        or batch['closed_for_entries']
                        or (batch['loss_budget_usd'] is not None
                            and batch['loss_used_usd'] >= batch['loss_budget_usd'])):
                    raise PendingOrder('ordinary paired retry requires terminal zero-fill evidence')
            event_ts = self._insert_request_event('entropy', cycle_id=cycle['cycle_id'], **request)
            if event_ts is None:
                return None
            if initial:
                self._create_live_cycle(**cycle)
            else:
                self.db.execute('UPDATE live_cycles SET planned_units=? WHERE cycle_id=?',
                                (cycle['planned_units'], cycle['cycle_id']))
            prepared = self._prepare_opening_intents(plans)
            self.db.execute("UPDATE live_cycles SET phase='OPEN_HEDGE' WHERE cycle_id=?", (cycle['cycle_id'],))
            first = next(i.order_ref for i, _ in plans if i.venue == 'entropy')
            prepared[first]['request_event_ts'] = event_ts
            return prepared

    def admit_opening_attempt(self, experiment_id: str, *, cycle: dict,
                              plans: list[tuple[Intent, dict]], pair_plan: dict,
                              request: dict) -> dict | None:
        """One durable admission: slot, opportunity, cycle, plan and possible POSTs."""
        with self.transaction():
            state = self.opening_experiment_state(experiment_id)
            previous = state['current_attempt']
            if (not state['armed'] or state['attempts'] >= 20
                    or previous is not None and not previous['terminal_flat']):
                raise PendingOrder('opening experiment is not ready for an independent opportunity')
            if cycle['batch_id'] != state['batch_id']:
                raise ValueError('opening experiment batch identity changed')
            slot = state['attempts'] + 1
            group = 'A' if slot % 2 else 'B'
            side = Side.BUY if cycle['direction'] == 'long_entropy' else Side.SELL
            expected = {'entropy', 'hedge'} if group == 'A' else {'entropy'}
            if ({intent.venue for intent, _ in plans} != expected
                    or len(plans) != len(expected)
                    or any(intent.cycle_id != cycle['cycle_id']
                           or intent.units != cycle['planned_units']
                           or intent.side is not (side if intent.venue == 'entropy' else side.opposite)
                           for intent, _ in plans)):
                raise ValueError('opening admission has an invalid paired intent plan')
            if pair_plan != {'entropy_units': cycle['planned_units'],
                    'hedge_units': cycle['planned_units'] if group == 'A' else 'terminal_entropy_unmatched',
                    'entropy_side': side.value, 'hedge_side': side.opposite.value}:
                raise ValueError('opening paired plan changed')
            # A denial creates neither a cycle nor a slot.
            event_ts = self._insert_request_event('entropy', cycle_id=cycle['cycle_id'], **request)
            if event_ts is None:
                return None
            self._create_live_cycle(**cycle)
            prepared = self._prepare_opening_intents(plans)
            first = next(intent.order_ref for intent, _ in plans if intent.venue == 'entropy')
            hedge = next((intent.order_ref for intent, _ in plans if intent.venue == 'hedge'), None)
            opportunity = f"{experiment_id}:{state['opportunity_seq'] + 1}"
            self.db.execute('''INSERT INTO opening_attempts(experiment_id,slot,group_name,
                opportunity_id,cycle_id,pair_plan_json,first_ref,hedge_ref,allocated_ts)
                VALUES (?,?,?,?,?,?,?,?,?)''', (experiment_id, slot, group, opportunity,
                    cycle['cycle_id'], json.dumps(pair_plan, sort_keys=True), first, hedge, cycle['started_ts']))
            self.db.execute('UPDATE opening_experiments SET armed=0,opportunity_seq=opportunity_seq+1 WHERE experiment_id=?',
                            (experiment_id,))
            if group == 'A':
                self.db.execute("UPDATE live_cycles SET phase='OPEN_HEDGE' WHERE cycle_id=?", (cycle['cycle_id'],))
            prepared[first]['request_event_ts'] = event_ts
            return prepared

    def prepare_opening_pair(self, cycle_id: str, *, plans: list[tuple[Intent, dict]],
                             request: dict) -> dict | None:
        """A zero-fill IOC retry stays in the original slot and opportunity."""
        with self.transaction():
            attempt = self.opening_attempt(cycle_id)
            cycle = self.active_live_cycle()
            orders = self.cycle_orders(cycle_id)
            if (attempt is None or attempt['group_name'] != 'A' or attempt['terminal_flat']
                    or cycle is None or cycle['cycle_id'] != cycle_id
                    or cycle['basis_exit_requested_ts'] is not None or self.pending()
                    or cycle['entropy_units'] or cycle['hedge_units']
                    or self.batch_state(cycle['batch_id'])['closed_for_entries']
                    or any(o['filled_units'] or o['had_unknown'] for o in orders)
                    or {i.venue for i, _ in plans} != {'entropy', 'hedge'} or len(plans) != 2
                    or any(i.units != cycle['planned_units'] or i.cycle_id != cycle_id
                        or i.side is not ((Side.BUY if cycle['direction'] == 'long_entropy' else Side.SELL)
                            if i.venue == 'entropy' else (Side.SELL if cycle['direction'] == 'long_entropy' else Side.BUY))
                        for i, _ in plans)):
                raise PendingOrder('paired retry requires terminal zero-fill opening evidence')
            event_ts = self._insert_request_event('entropy', cycle_id=cycle_id, **request)
            if event_ts is None:
                return None
            prepared = self._prepare_opening_intents(plans)
            first = next(i.order_ref for i, _ in plans if i.venue == 'entropy')
            hedge = next(i.order_ref for i, _ in plans if i.venue == 'hedge')
            self.db.execute('UPDATE opening_attempts SET first_ref=?,hedge_ref=? WHERE cycle_id=?',
                            (first, hedge, cycle_id))
            prepared[first]['request_event_ts'] = event_ts
            return prepared

    def finish_opening_attempt(self, cycle_id: str, *, outcome: str) -> None:
        with self.transaction():
            attempt = self.opening_attempt(cycle_id)
            if attempt is None:
                return
            cycle = self.db.execute('SELECT * FROM live_cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
            orders = self.cycle_orders(cycle_id)
            known = {key: sum(Side(o['side']).sign * o['filled_units'] for o in orders if o['venue'] == key)
                     for key in ('entropy', 'hedge')}
            latest = max((o['terminal_recorded_ts'] or 0 for o in orders), default=0)
            if (cycle['phase'] != 'DONE' or cycle['entropy_units'] or cycle['hedge_units']
                    or not cycle['position_evidence_authoritative']
                    or cycle['last_authoritative_ts'] is None or cycle['last_authoritative_ts'] < latest
                    or any(o['state'] != 'TERMINAL' for o in orders) or any(known.values())):
                raise PendingOrder('opening attempt requires terminal authoritative conserved flat')
            if outcome not in {'zero', 'failed', 'partial_repaired', 'filled',
                               'unpaired_repaired', 'unknown_resolved'}:
                raise ValueError('invalid opening attempt outcome')
            if attempt['terminal_flat']:
                if attempt['outcome'] != outcome:
                    raise ValueError('opening attempt outcome changed')
                return
            self.db.execute('UPDATE opening_attempts SET terminal_flat=1,outcome=?,finished_ts=? WHERE cycle_id=?',
                            (outcome, time.time(), cycle_id))

    def record_execution_benchmark(self, *, step_key: str, cycle_id: str,
                                   purpose: str, venue: str, attempt: int,
                                   order_ref: str,
                                   side: str, requested_units: int,
                                   room_bps: Decimal, hard_cap_bps: Decimal,
                                   submit_ts: float, top_px: Decimal,
                                   marginal_px: Decimal, limit_px: Decimal,
                                   paired_executable_basis_bps: Decimal,
                                   reference_bps: Decimal,
                                   directional_dislocation_bps: Decimal,
                                   book_revision_entropy: str,
                                   book_revision_hedge: str,
                                   open_timing: dict[str, object] | None = None) -> None:
        values = (
            step_key, cycle_id, purpose, venue, attempt, order_ref, side, requested_units,
            str(room_bps), str(hard_cap_bps), submit_ts, str(top_px),
            str(marginal_px), str(limit_px), str(paired_executable_basis_bps),
            str(reference_bps), str(directional_dislocation_bps),
            book_revision_entropy, book_revision_hedge,
        )
        with self.transaction():
            live = self.db.execute(
                'SELECT execution_group_id,parent_step_key FROM live_steps WHERE step_key=?',
                (step_key,)).fetchone()
            group_id = live['execution_group_id'] if live is not None else None
            parent_key = live['parent_step_key'] if live is not None else None
            row = self.db.execute(
                'SELECT * FROM execution_economics_steps WHERE step_key=?',
                (step_key,)).fetchone()
            if row is None:
                self.db.execute('''INSERT INTO execution_economics_steps(
                    step_key,cycle_id,purpose,venue,attempt,order_ref,side,requested_units,
                    room_bps,hard_cap_bps,submit_ts,top_px,marginal_px,limit_px,
                    paired_executable_basis_bps,reference_bps,
                    directional_dislocation_bps,book_revision_entropy,
                    book_revision_hedge,execution_group_id,parent_step_key)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (*values, group_id, parent_key))
            else:
                actual = tuple(row[key] for key in (
                    'step_key', 'cycle_id', 'purpose', 'venue', 'attempt', 'order_ref', 'side',
                    'requested_units', 'room_bps', 'hard_cap_bps', 'submit_ts',
                    'top_px', 'marginal_px', 'limit_px',
                    'paired_executable_basis_bps', 'reference_bps',
                    'directional_dislocation_bps', 'book_revision_entropy',
                    'book_revision_hedge'))
                if actual != values:
                    raise ValueError('execution benchmark changed across restart')
            if open_timing:
                allowed = {
                    'cycle_persist_end_ts', 'benchmark_start_ts',
                    'benchmark_end_ts', 'intent_prepare_start_ts',
                    'intent_prepare_end_ts', 'submit_start_ts',
                }
                if set(open_timing) - allowed:
                    raise ValueError('invalid open timing fields')
                encoded = [str(value) if isinstance(value, Decimal) else value
                           for value in open_timing.values()]
                assignments = ','.join(f'{name}=COALESCE({name},?)'
                                       for name in open_timing)
                self.db.execute(
                    f'UPDATE cycle_economics SET {assignments} WHERE cycle_id=?',
                    (*encoded, cycle_id))

    def record_submit_result_timing(self, cycle_id: str, *, submit_result_ts: float,
                                    submit_start_ts: float | None = None,
                                    intent_prepare_start_ts: float | None = None,
                                    intent_prepare_end_ts: float | None = None) -> None:
        with self.transaction():
            self.db.execute('''UPDATE cycle_economics SET
                intent_prepare_start_ts=COALESCE(intent_prepare_start_ts,?),
                intent_prepare_end_ts=COALESCE(intent_prepare_end_ts,?),
                submit_start_ts=COALESCE(submit_start_ts,?),
                submit_result_ts=COALESCE(submit_result_ts,?),updated_ts=?
                WHERE cycle_id=?''',
                (intent_prepare_start_ts, intent_prepare_end_ts,
                 submit_start_ts, submit_result_ts, submit_result_ts, cycle_id))

    def set_live_entry_basis(self, cycle_id: str, *, basis_bps: Decimal,
                             source: str, updated_ts: float) -> None:
        if source not in {'exchange_fills', 'terminal_vwap', 'persisted_step_vwap',
                          'canonical_order_vwap'}:
            raise ValueError('invalid live entry basis source')
        with self.transaction():
            cursor = self.db.execute('''UPDATE live_cycles SET
                live_entry_basis_bps=?,live_entry_basis_source=?,updated_ts=?
                WHERE cycle_id=?''',
                (str(basis_bps), source, updated_ts, cycle_id))
            if cursor.rowcount != 1:
                raise KeyError(cycle_id)
            self.db.execute('''UPDATE cycle_economics SET
                live_entry_basis_bps=?,live_entry_basis_source=?,updated_ts=?
                WHERE cycle_id=?''',
                (str(basis_bps), source, updated_ts, cycle_id))

    def record_execution_result(self, step_key: str, *, result_ts: float,
                                result_units: int, provisional_avg_px: Decimal | None,
                                actual_fee_usd: Decimal | None,
                                execution_cost_bps: Decimal | None,
                                result_paired_executable_basis_bps: Decimal | None = None,
                                result_directional_dislocation_bps: Decimal | None = None) -> None:
        with self.transaction():
            row = self.db.execute(
                'SELECT result_units FROM execution_economics_steps WHERE step_key=?',
                (step_key,)).fetchone()
            if row is None:
                raise KeyError(step_key)
            if row['result_units'] is not None and row['result_units'] != result_units:
                raise ValueError('execution result changed across restart')
            self.db.execute('''UPDATE execution_economics_steps SET
                result_ts=?,result_units=?,provisional_avg_px=?,actual_fee_usd=?,
                execution_cost_bps=?,result_paired_executable_basis_bps=?,
                result_directional_dislocation_bps=? WHERE step_key=?''',
                (result_ts, result_units,
                 str(provisional_avg_px) if provisional_avg_px is not None else None,
                 str(actual_fee_usd) if actual_fee_usd is not None else None,
                 str(execution_cost_bps) if execution_cost_bps is not None else None,
                 (str(result_paired_executable_basis_bps)
                  if result_paired_executable_basis_bps is not None else None),
                 (str(result_directional_dislocation_bps)
                  if result_directional_dislocation_bps is not None else None),
                 step_key))

    def record_execution_attempt_timing(self, step_key: str, **timing: object) -> None:
        allowed = {
            'submit_start_ts', 'http_post_start', 'http_headers_received',
            'http_body_received', 'http_result_ts', 'terminal_result_ts',
            'confirmation_source', 'result_status', 'resolve_start_ts',
            'resolve_end_ts', 'exchange_fill_ts',
        }
        if not timing or set(timing) - allowed:
            raise ValueError('invalid execution attempt timing fields')
        values = {name: value for name, value in timing.items() if value is not None}
        if not values:
            return
        assignments = ','.join(
            f'{name}=?' if name in {'result_status', 'confirmation_source',
                                  'terminal_result_ts', 'resolve_end_ts'}
            else f'{name}=COALESCE({name},?)' for name in values)
        with self.transaction():
            cursor = self.db.execute(
                f'UPDATE execution_economics_steps SET {assignments} WHERE step_key=?',
                (*values.values(), step_key))
            if cursor.rowcount != 1:
                raise KeyError(step_key)

    def record_execution_measurement(self, step_key: str,
                                     **fields: object) -> None:
        allowed = {
            'terminal_clock_session_id', 'runtime_consumed_ts',
            'runtime_consumed_mono_ns', 'runtime_consumed_clock_session_id',
            'telemetry_version', 'phase_kind', 'clock_session_id', 'quantity_step',
            'original_signal_ts', 'original_signal_basis_bps',
            'attempt_decision_ts', 'attempt_decision_mono_ns',
            'attempt_decision_basis_bps', 'decision_units', 'decision_quote_json',
            'submit_quote_ts', 'submit_quote_json', 'submit_basis_bps',
            'submit_start_mono_ns', 'http_post_start_mono_ns',
            'terminal_observed_ts', 'terminal_observed_mono_ns',
            'exchange_first_fill_ts', 'exchange_last_fill_ts',
            'fill_mark_quote_json', 'fill_mark_time_source', 'execution_path',
            'measurement_quality', 'missing_fields_json',
            'authoritative_reconcile_complete_ts',
            'authoritative_reconcile_complete_mono_ns',
        }
        if not fields or set(fields) - allowed:
            raise ValueError('invalid execution measurement fields')
        values = {}
        for name, value in fields.items():
            if value is None:
                continue
            if isinstance(value, (dict, list)):
                value = json.dumps(value, sort_keys=True, separators=(',', ':'))
            elif isinstance(value, Decimal):
                value = str(value)
            values[name] = value
        if not values:
            return
        replacements = {'measurement_quality', 'missing_fields_json',
                        'runtime_consumed_ts', 'runtime_consumed_mono_ns',
                        'runtime_consumed_clock_session_id'}
        assignments = ','.join(
            f'{name}=?' if name in replacements else f'{name}=COALESCE({name},?)'
            for name in values)
        with self.transaction():
            cursor = self.db.execute(
                f'UPDATE execution_economics_steps SET {assignments} WHERE step_key=?',
                (*values.values(), step_key))
            if cursor.rowcount != 1:
                raise KeyError(step_key)

    def finalize_execution_fill(self, step_key: str, *, fill_ts: float | None,
                                fill_vwap: Decimal, fee_usd: Decimal | None) -> None:
        with self.transaction():
            row = self.db.execute('''SELECT side,top_px FROM
                execution_economics_steps WHERE step_key=?''', (step_key,)).fetchone()
            if row is None:
                raise KeyError(step_key)
            if row['side'] == 'buy':
                cost = (fill_vwap / dec(row['top_px']) - 1) * Decimal('10000')
            else:
                cost = (dec(row['top_px']) / fill_vwap - 1) * Decimal('10000')
            cursor = self.db.execute('''UPDATE execution_economics_steps SET
                actual_fill_ts=?,exchange_fill_ts=COALESCE(exchange_fill_ts,?),
                actual_fill_vwap=?,actual_fee_usd=?,execution_cost_bps=?
                WHERE step_key=?''',
                (fill_ts, fill_ts, str(fill_vwap),
                 str(fee_usd) if fee_usd is not None else None, str(cost), step_key))
            if cursor.rowcount != 1:
                raise KeyError(step_key)

    def capture_entry_economics(self, cycle_id: str, *, actual_entry_basis_bps: Decimal,
                                first_fill_mark_basis_bps: Decimal | None,
                                first_fill_observed_ts: float | None,
                                first_fill_exchange_ts: float | None,
                                first_leg_cost_bps: Decimal | None,
                                hedge_cost_bps: Decimal | None,
                                updated_ts: float) -> None:
        with self.transaction():
            row = self.db.execute(
                'SELECT entry_dislocation_bps FROM cycle_economics WHERE cycle_id=?',
                (cycle_id,)).fetchone()
            if row is None:
                raise KeyError(cycle_id)
            cycle = self.db.execute(
                'SELECT direction,reference_bps FROM live_cycles WHERE cycle_id=?',
                (cycle_id,)).fetchone()
            reference = dec(cycle['reference_bps'])
            first_dislocation = (None if first_fill_mark_basis_bps is None else
                                 reference - first_fill_mark_basis_bps
                                 if cycle['direction'] == 'long_entropy'
                                 else first_fill_mark_basis_bps - reference)
            entry_dislocation = (reference - actual_entry_basis_bps
                                 if cycle['direction'] == 'long_entropy'
                                 else actual_entry_basis_bps - reference)
            signal_dislocation = dec(row['entry_dislocation_bps'])
            lag_ms = (Decimal(str(abs(first_fill_observed_ts - first_fill_exchange_ts)))
                      * 1000 if first_fill_observed_ts is not None
                      and first_fill_exchange_ts is not None else None)
            self.db.execute('''UPDATE cycle_economics SET
                first_fill_mark_basis_bps=?,actual_entry_basis_bps=?,
                first_fill_observed_ts=?,first_fill_exchange_ts=?,
                first_fill_observation_lag_ms=?,
                first_leg_cost_bps=?,hedge_cost_bps=?,
                signal_to_first_fill_decay_bps=?,
                first_fill_to_hedged_decay_bps=?,total_entry_decay_bps=?,
                updated_ts=? WHERE cycle_id=?''',
                (str(first_fill_mark_basis_bps) if first_fill_mark_basis_bps is not None else None, str(actual_entry_basis_bps),
                 first_fill_observed_ts, first_fill_exchange_ts,
                 str(lag_ms) if lag_ms is not None else None,
                 str(first_leg_cost_bps) if first_leg_cost_bps is not None else None,
                 str(hedge_cost_bps) if hedge_cost_bps is not None else None,
                 str(signal_dislocation - first_dislocation) if first_dislocation is not None else None,
                 str(first_dislocation - entry_dislocation) if first_dislocation is not None else None,
                 str(signal_dislocation - entry_dislocation), updated_ts, cycle_id))

    def update_max_reversion(self, cycle_id: str, reversion_bps: Decimal, *,
                             observed_ts: float, force: bool = False) -> None:
        with self.transaction():
            row = self.db.execute('''SELECT max_favorable_reversion_bps,
                max_reversion_flush_ts FROM cycle_economics WHERE cycle_id=?''',
                (cycle_id,)).fetchone()
            if row is None:
                raise KeyError(cycle_id)
            current = (dec(row['max_favorable_reversion_bps'])
                       if row['max_favorable_reversion_bps'] is not None else None)
            if current is not None and reversion_bps <= current:
                return
            self.db.execute('''UPDATE cycle_economics SET
                max_favorable_reversion_bps=?,max_favorable_reversion_ts=?,
                max_reversion_flush_ts=?,updated_ts=? WHERE cycle_id=?''',
                (str(reversion_bps), observed_ts, observed_ts, observed_ts, cycle_id))

    def mark_recovery_used(self, cycle_id: str, *, updated_ts: float) -> None:
        with self.transaction():
            cursor = self.db.execute('''UPDATE live_cycles SET
                recovery_used=1,updated_ts=? WHERE cycle_id=?''',
                (updated_ts, cycle_id))
            if cursor.rowcount != 1:
                raise KeyError(cycle_id)
            self.db.execute('''UPDATE cycle_economics SET
                recovery_used=1,updated_ts=? WHERE cycle_id=?''',
                (updated_ts, cycle_id))

    def record_exit_trigger(self, cycle_id: str, *, trigger_ts: float, reason: str,
                            trigger_basis_bps: Decimal | None, reversion_bps: Decimal | None,
                            executable_pnl_usd: Decimal | None,
                            anchor_basis_bps: Decimal | None = None,
                            anchor_source: str | None = None,
                            trigger_mono_ns: int | None = None,
                            trigger_clock_session_id: str | None = None) -> None:
        with self.transaction():
            self.db.execute('''UPDATE cycle_economics SET exit_trigger_ts=COALESCE(
                exit_trigger_ts,?),exit_trigger_reason=COALESCE(exit_trigger_reason,?),
                exit_trigger_basis_bps=COALESCE(exit_trigger_basis_bps,?),
                exit_trigger_reversion_bps=COALESCE(exit_trigger_reversion_bps,?),
                exit_trigger_executable_pnl_usd=COALESCE(
                    exit_trigger_executable_pnl_usd,?),
                exit_anchor_basis_bps=COALESCE(exit_anchor_basis_bps,?),
                exit_anchor_source=COALESCE(exit_anchor_source,?),
                exit_trigger_mono_ns=COALESCE(exit_trigger_mono_ns,?),
                exit_trigger_clock_session_id=COALESCE(exit_trigger_clock_session_id,?),
                updated_ts=? WHERE cycle_id=?''',
                (trigger_ts, reason,
                 str(trigger_basis_bps) if trigger_basis_bps is not None else None,
                 str(reversion_bps) if reversion_bps is not None else None,
                 str(executable_pnl_usd) if executable_pnl_usd is not None else None,
                 str(anchor_basis_bps) if anchor_basis_bps is not None else None,
                 anchor_source, trigger_mono_ns, trigger_clock_session_id,
                 trigger_ts, cycle_id))
            self._refresh_close_metrics(cycle_id)

    def record_authoritative_reconcile(self, cycle_id: str, *,
                                       observed_ts: float, mono_ns: int,
                                       clock_session_id: str,
                                       execution_group_id: str | None = None) -> None:
        with self.transaction():
            cursor = self.db.execute('''UPDATE cycle_economics SET
                authoritative_reconcile_complete_ts=COALESCE(
                    authoritative_reconcile_complete_ts,?),
                authoritative_reconcile_complete_mono_ns=COALESCE(
                    authoritative_reconcile_complete_mono_ns,?),
                authoritative_reconcile_clock_session_id=COALESCE(
                    authoritative_reconcile_clock_session_id,?),
                updated_ts=? WHERE cycle_id=?''',
                (observed_ts, mono_ns, clock_session_id, observed_ts, cycle_id))
            if cursor.rowcount != 1:
                raise KeyError(cycle_id)
            if execution_group_id is not None:
                self.db.execute('''UPDATE execution_economics_steps SET
                    authoritative_reconcile_complete_ts=COALESCE(
                        authoritative_reconcile_complete_ts,?),
                    authoritative_reconcile_complete_mono_ns=COALESCE(
                        authoritative_reconcile_complete_mono_ns,?)
                    WHERE cycle_id=? AND execution_group_id=?''',
                    (observed_ts, mono_ns, cycle_id, execution_group_id))

    def record_close_submit(self, cycle_id: str, *, venue: str,
                            submit_ts: float) -> None:
        column = ('entropy_close_submit_ts' if venue == 'entropy'
                  else 'lighter_close_submit_ts' if venue == 'hedge' else None)
        if column is None:
            raise ValueError('invalid close venue')
        with self.transaction():
            cursor = self.db.execute(
                f'''UPDATE cycle_economics SET {column}=COALESCE({column},?),
                    updated_ts=? WHERE cycle_id=?''',
                (submit_ts, submit_ts, cycle_id))
            if cursor.rowcount != 1:
                raise KeyError(cycle_id)
            self._refresh_close_metrics(cycle_id)

    def record_close_fill(self, cycle_id: str, *, venue: str, fill_ts: float,
                          fill_px: Decimal, fill_units: int,
                          paired_basis_at_entropy_fill: Decimal | None = None) -> None:
        if venue not in {'entropy', 'hedge'} or fill_units <= 0 or fill_px <= 0:
            raise ValueError('invalid close fill')
        prefix = 'entropy' if venue == 'entropy' else 'lighter'
        with self.transaction():
            cursor = self.db.execute(f'''UPDATE cycle_economics SET
                {prefix}_close_fill_ts=COALESCE({prefix}_close_fill_ts,?),
                {prefix}_close_fill_px=COALESCE({prefix}_close_fill_px,?),
                {prefix}_close_fill_units=COALESCE({prefix}_close_fill_units,?),
                paired_basis_at_entropy_fill=COALESCE(
                    paired_basis_at_entropy_fill,?),updated_ts=? WHERE cycle_id=?''',
                (fill_ts, str(fill_px), fill_units,
                 (str(paired_basis_at_entropy_fill)
                  if paired_basis_at_entropy_fill is not None else None),
                 fill_ts, cycle_id))
            if cursor.rowcount != 1:
                raise KeyError(cycle_id)
            if venue == 'hedge':
                row = self.db.execute('''SELECT entropy_close_fill_px
                    FROM cycle_economics WHERE cycle_id=?''', (cycle_id,)).fetchone()
                if row and row['entropy_close_fill_px'] is not None:
                    actual = (dec(row['entropy_close_fill_px']) / fill_px - 1) \
                        * Decimal('10000')
                    self.db.execute('''UPDATE cycle_economics SET
                        actual_paired_exit_basis=COALESCE(
                            actual_paired_exit_basis,?) WHERE cycle_id=?''',
                        (str(actual), cycle_id))
            self._refresh_close_metrics(cycle_id)

    def finalize_close_execution(self, cycle_id: str, *,
                                 entropy_fill_ts: float | None,
                                 entropy_fill_px: Decimal,
                                 entropy_fill_units: int,
                                 lighter_fill_ts: float | None,
                                 lighter_fill_px: Decimal,
                                 lighter_fill_units: int,
                                 actual_paired_exit_basis: Decimal,
                                 updated_ts: float) -> None:
        with self.transaction():
            cursor = self.db.execute('''UPDATE cycle_economics SET
                entropy_close_fill_ts=COALESCE(?,entropy_close_fill_ts),
                entropy_close_fill_px=?,entropy_close_fill_units=?,
                lighter_close_fill_ts=COALESCE(?,lighter_close_fill_ts),
                lighter_close_fill_px=?,lighter_close_fill_units=?,
                actual_paired_exit_basis=?,updated_ts=? WHERE cycle_id=?''',
                (entropy_fill_ts, str(entropy_fill_px), entropy_fill_units,
                 lighter_fill_ts, str(lighter_fill_px), lighter_fill_units,
                 str(actual_paired_exit_basis), updated_ts, cycle_id))
            if cursor.rowcount != 1:
                raise KeyError(cycle_id)
            self._refresh_close_metrics(cycle_id)

    def _refresh_close_metrics(self, cycle_id: str) -> None:
        row = self.db.execute('''SELECT e.*,l.direction FROM cycle_economics e
            JOIN live_cycles l USING(cycle_id) WHERE e.cycle_id=?''',
            (cycle_id,)).fetchone()
        if row is None:
            raise KeyError(cycle_id)
        values: dict[str, str] = {}
        timing = (
            ('trigger_to_entropy_fill_ms', 'exit_trigger_ts',
             'entropy_close_fill_ts'),
            ('entropy_fill_to_lighter_submit_ms', 'entropy_close_fill_ts',
             'lighter_close_submit_ts'),
            ('lighter_submit_to_fill_ms', 'lighter_close_submit_ts',
             'lighter_close_fill_ts'),
        )
        for target, start, end in timing:
            if row[start] is not None and row[end] is not None:
                values[target] = str(
                    (dec(row[end]) - dec(row[start])) * Decimal('1000'))
        trigger = row['exit_trigger_basis_bps']
        interleg = row['paired_basis_at_entropy_fill']
        actual = row['actual_paired_exit_basis']
        sign = Decimal('1') if row['direction'] == 'long_entropy' else Decimal('-1')
        if trigger is not None and interleg is not None:
            values['trigger_to_entropy_fill_decay_bps'] = str(
                (dec(trigger) - dec(interleg)) * sign)
        if interleg is not None and actual is not None:
            values['interleg_decay_bps'] = str(
                (dec(interleg) - dec(actual)) * sign)
        if trigger is not None and actual is not None:
            values['total_exit_execution_decay_bps'] = str(
                (dec(trigger) - dec(actual)) * sign)
        if values:
            assignments = ','.join(f'{name}=?' for name in values)
            self.db.execute(
                f'UPDATE cycle_economics SET {assignments} WHERE cycle_id=?',
                (*values.values(), cycle_id))

    def finalize_cycle_economics(self, cycle_id: str, **values: object) -> None:
        allowed = {
            'actual_entry_basis_bps', 'actual_exit_basis_bps',
            'actual_reversion_bps', 'actual_entry_notional_usd',
            'actual_fees_usd', 'entropy_fees_usd', 'hedge_fees_usd',
            'actual_fees_bps', 'actual_funding_usd', 'entropy_funding_usd',
            'hedge_funding_usd', 'actual_funding_bps', 'finalized_pnl_usd',
            'finalized_pnl_bps', 'break_even_reversion_bps', 'result_kind',
            'accounting_final', 'clean_summary_eligible', 'telemetry_incomplete',
            'anchor_error_bps', 'updated_ts',
        }
        if not values or set(values) - allowed:
            raise ValueError('invalid economics finalization fields')
        encoded = []
        for value in values.values():
            if isinstance(value, Decimal):
                encoded.append(str(value))
            elif isinstance(value, bool):
                encoded.append(int(value))
            else:
                encoded.append(value)
        assignments = ','.join(f'{name}=?' for name in values)
        with self.transaction():
            cursor = self.db.execute(
                f'UPDATE cycle_economics SET {assignments} WHERE cycle_id=?',
                (*encoded, cycle_id))
            if cursor.rowcount != 1:
                raise KeyError(cycle_id)

    def economics_rows(self, *, batch_id: str | None = None) -> list[dict]:
        query = 'SELECT * FROM cycle_economics'
        params: tuple[str, ...] = ()
        if batch_id is not None:
            query += ' WHERE batch_id=?'
            params = (batch_id,)
        query += ' ORDER BY signal_ts'
        return [dict(row) for row in self.db.execute(query, params)]

    def pending_economics_cycles(self, batch_id: str) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            '''SELECT c.*,l.started_ts,l.updated_ts FROM cycles c
               JOIN live_cycles l USING(cycle_id)
               JOIN cycle_economics e USING(cycle_id)
               WHERE c.batch_id=? AND c.accounting_final=1
                 AND e.accounting_final=0 ORDER BY l.started_ts''', (batch_id,))]

    def economics_steps(self, cycle_id: str) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            'SELECT * FROM execution_economics_steps WHERE cycle_id=? ORDER BY submit_ts',
            (cycle_id,))]

    def economics_step(self, step_key: str) -> dict | None:
        row = self.db.execute(
            'SELECT * FROM execution_economics_steps WHERE step_key=?',
            (step_key,)).fetchone()
        return dict(row) if row else None

    def active_live_cycle(self) -> dict | None:
        row = self.db.execute(
            "SELECT * FROM live_cycles WHERE phase!='DONE' ORDER BY started_ts LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _funding_time(value: float) -> float:
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError('invalid funding time')
        return float(value)

    def ensure_funding_cycle(self, cycle_id: str, *,
                             hedged_ts: float | None = None) -> dict:
        if hedged_ts is not None:
            hedged_ts = self._funding_time(hedged_ts)
        with self.transaction():
            if self.db.execute('SELECT 1 FROM live_cycles WHERE cycle_id=?',
                               (cycle_id,)).fetchone() is None:
                raise KeyError(cycle_id)
            self.db.execute('''INSERT OR IGNORE INTO funding_cycles
                (cycle_id,hedged_ts) VALUES (?,?)''', (cycle_id, hedged_ts))
            if hedged_ts is not None:
                self.db.execute('''UPDATE funding_cycles SET hedged_ts=?
                    WHERE cycle_id=? AND hedged_ts IS NULL''', (hedged_ts, cycle_id))
            return self.funding_state(cycle_id)

    def funding_state(self, cycle_id: str) -> dict | None:
        row = self.db.execute('SELECT * FROM funding_cycles WHERE cycle_id=?',
                              (cycle_id,)).fetchone()
        if row is None:
            return None
        state = dict(row)
        state['completed'] = bool(state['completed'])
        state['trade_accounting_complete'] = bool(state['trade_accounting_complete'])
        state['closed_poll_complete'] = bool(state['closed_poll_complete'])
        state['fee_evidence_accounting'] = bool(state['fee_evidence_accounting'])
        for field in ('last_net_usd_per_hour', 'trade_cash_net_usd', 'trade_gross_cash_usd',
                      'known_cash_result_usd'):
            if state[field] is not None:
                state[field] = dec(state[field])
        funding = {'entropy': Decimal('0'), 'hedge': Decimal('0')}
        periods: dict[str, set[int]] = {'entropy': set(), 'hedge': set()}
        # This connection is owned by one writer; synchronous reads also work
        # inside the write methods' transactions without opening a nested one.
        for settlement in self.db.execute(
                'SELECT venue,ts,amount_usd FROM funding_settlements WHERE cycle_id=?',
                (cycle_id,)):
            venue = settlement['venue']
            funding[venue] += dec(settlement['amount_usd'])
            if state['hedged_ts'] is not None and settlement['ts'] > state['hedged_ts']:
                periods[venue].add(math.floor(settlement['ts'] / 3600))
        state['settlement_counts'] = {venue: len(hours) for venue, hours in periods.items()}
        state['funding_by_venue'] = funding
        state['actual_funding_usd'] = funding['entropy'] + funding['hedge']
        state.update(self.fee_state(cycle_id))
        return state

    def record_funding_settlements(self, cycle_id: str, venue: str,
                                   settlements: Iterable[object]) -> None:
        if venue not in {'entropy', 'hedge'}:
            raise ValueError('invalid funding venue')
        with self.transaction():
            if self.db.execute('SELECT 1 FROM funding_cycles WHERE cycle_id=?',
                               (cycle_id,)).fetchone() is None:
                raise KeyError(cycle_id)
            for settlement in settlements:
                event_id = settlement.event_id
                stamp = self._funding_time(settlement.ts)
                amount = dec(settlement.amount_usd)
                quantity = getattr(settlement, 'position_qty', None)
                quantity = dec(quantity) if quantity is not None else None
                source = getattr(settlement, 'source', '')
                if not isinstance(event_id, str) or not event_id.strip() \
                        or not isinstance(source, str):
                    raise ValueError('invalid funding settlement identity/source')
                old = self.db.execute('''SELECT * FROM funding_settlements
                    WHERE venue=? AND event_id=?''', (venue, event_id)).fetchone()
                if old is not None:
                    actual = (old['cycle_id'], old['ts'], dec(old['amount_usd']),
                              dec(old['position_qty']) if old['position_qty'] is not None
                              else None, old['source'])
                    if actual != (cycle_id, stamp, amount, quantity, source):
                        raise ValueError('funding settlement changed across retransmission')
                    continue
                self.db.execute('''INSERT INTO funding_settlements
                    (venue,event_id,cycle_id,ts,amount_usd,position_qty,source)
                    VALUES (?,?,?,?,?,?,?)''',
                    (venue, event_id, cycle_id, stamp, str(amount),
                     str(quantity) if quantity is not None else None, source))

    def record_funding_sample(self, cycle_id: str, *, sample_ts: float,
                              net_usd_per_hour: Decimal | None,
                              max_gap_seconds: float = 45) -> dict:
        sample_ts = self._funding_time(sample_ts)
        max_gap_seconds = self._funding_time(max_gap_seconds)
        net = dec(net_usd_per_hour) if net_usd_per_hour is not None else None
        with self.transaction():
            row = self.db.execute('SELECT * FROM funding_cycles WHERE cycle_id=?',
                                  (cycle_id,)).fetchone()
            if row is None:
                raise KeyError(cycle_id)
            last = row['last_sample_ts']
            if last is None or sample_ts > last:
                count, since = 0, None
                if net is not None and net < 0:
                    continuous = (last is not None
                                  and sample_ts - last <= max_gap_seconds
                                  and row['negative_sample_count'] > 0)
                    count = row['negative_sample_count'] + 1 if continuous else 1
                    since = row['negative_since_ts'] if continuous else sample_ts
                self.db.execute('''UPDATE funding_cycles SET
                    negative_sample_count=?,negative_since_ts=?,last_sample_ts=?,
                    last_net_usd_per_hour=? WHERE cycle_id=?''',
                    (count, since, sample_ts, str(net) if net is not None else None,
                     cycle_id))
            return self.funding_state(cycle_id)

    def set_funding_exit(self, cycle_id: str, *, reason: str, completed: bool) -> None:
        if not isinstance(reason, str) or not reason.strip() or type(completed) is not bool:
            raise ValueError('invalid funding exit')
        with self.transaction():
            cursor = self.db.execute('''UPDATE funding_cycles SET
                exit_reason=?,completed=? WHERE cycle_id=?''',
                (reason, int(completed), cycle_id))
            if cursor.rowcount != 1:
                raise KeyError(cycle_id)

    def record_funding_poll(self, cycle_id: str, *, polled_ts: float,
                            entropy_ok: bool, hedge_ok: bool,
                            closed_window: bool = False) -> None:
        polled_ts = self._funding_time(polled_ts)
        if any(type(value) is not bool for value in (entropy_ok, hedge_ok, closed_window)):
            raise ValueError('invalid funding poll outcome')
        with self.transaction():
            if self.db.execute('SELECT 1 FROM funding_cycles WHERE cycle_id=?',
                               (cycle_id,)).fetchone() is None:
                raise KeyError(cycle_id)
            if entropy_ok and hedge_ok:
                # Query success is diagnostic evidence, never a settlement.
                self.db.execute('''UPDATE funding_cycles SET
                    last_complete_poll_ts=MAX(COALESCE(last_complete_poll_ts,0),?),
                    closed_poll_complete=MAX(closed_poll_complete,
                        ? AND EXISTS(SELECT 1 FROM live_cycles l
                            WHERE l.cycle_id=? AND l.phase='DONE'))
                    WHERE cycle_id=?''',
                    (polled_ts, int(closed_window), cycle_id, cycle_id))

    def update_funding_pnl(self, cycle_id: str, net_pnl_usd: Decimal) -> None:
        net_pnl_usd = dec(net_pnl_usd)
        with self.transaction():
            row = self.db.execute('''SELECT c.batch_id,c.result_kind FROM cycles c
                JOIN funding_cycles f USING(cycle_id) WHERE c.cycle_id=?''',
                (cycle_id,)).fetchone()
            if row is None or row['result_kind'] is None:
                raise PendingOrder('only completed funding cycles can be reconciled')
            self.db.execute('''UPDATE cycles SET pnl_usd=?,complete_accounting=1,
                accounting_final=0 WHERE cycle_id=?''', (str(net_pnl_usd), cycle_id))
            # As in finalize_cycle_accounting, replace this cycle's PnL and
            # recompute budget usage; repeated polls cannot add the loss twice.
            state = self.batch_state(row['batch_id'])
            budget = state['loss_budget_usd']
            if budget is not None and state['known_loss_used_usd'] >= budget:
                self._close_entries(row['batch_id'], 'REALIZED_LOSS_BUDGET',
                                    value=state['known_loss_used_usd'], threshold=budget,
                                    cycle_id=cycle_id)

    def refresh_funding_pnl(self, cycle_id: str, *,
                            trade_cash_net_usd: Decimal | None = None,
                            trade_accounting_complete: bool = False,
                            trade_gross_cash_usd=...) -> Decimal | None:
        """Replace provisional cash from its baseline and deduplicated evidence.

        An explicit gross argument (including None) enables fee-evidence accounting;
        omitted gross preserves the legacy net baseline without subtracting fees twice.
        Missing fees leave net unknown while known cash can still consume the budget.
        """
        if type(trade_accounting_complete) is not bool or (
                trade_accounting_complete and trade_cash_net_usd is None
                and trade_gross_cash_usd is ...):
            raise ValueError('verified trading accounting requires an explicit cash baseline')
        if trade_cash_net_usd is not None:
            trade_cash_net_usd = dec(trade_cash_net_usd)
        if trade_gross_cash_usd is not ... and trade_gross_cash_usd is not None:
            trade_gross_cash_usd = dec(trade_gross_cash_usd)
        with self.transaction():
            row = self.db.execute('''SELECT c.batch_id,c.result_kind,
                f.trade_cash_net_usd,f.trade_gross_cash_usd,f.fee_evidence_accounting,
                l.cash_net_usd FROM cycles c
                JOIN funding_cycles f USING(cycle_id)
                JOIN live_cycles l USING(cycle_id) WHERE c.cycle_id=?''',
                (cycle_id,)).fetchone()
            if row is None or row['result_kind'] is None:
                raise PendingOrder('only completed funding cycles can be reconciled')
            state = self.funding_state(cycle_id)
            if trade_gross_cash_usd is not ... or row['fee_evidence_accounting']:
                gross = (trade_gross_cash_usd if trade_gross_cash_usd is not ... else
                         dec(row['trade_gross_cash_usd']) if row['trade_gross_cash_usd'] is not None
                         else None)
                known_cash = gross - state['known_fees_usd'] if gross is not None else None
                trade_cash_net_usd = known_cash if state['fees_complete'] else None
                known_result = (known_cash + state['actual_funding_usd']
                                if known_cash is not None else None)
                net_pnl_usd = known_result if trade_cash_net_usd is not None else None
                complete = gross is not None and state['history_complete'] and state['fees_complete']
                self.db.execute('''UPDATE funding_cycles SET trade_gross_cash_usd=?,
                    known_cash_result_usd=?,trade_cash_net_usd=?,trade_accounting_complete=?,
                    fee_evidence_accounting=1 WHERE cycle_id=?''',
                    (str(gross) if gross is not None else None,
                     str(known_result) if known_result is not None else None,
                     str(trade_cash_net_usd) if trade_cash_net_usd is not None else None,
                     int(complete), cycle_id))
            else:
                # Preserve legacy NET semantics; never infer a gross baseline from it.
                if trade_cash_net_usd is None:
                    saved = row['trade_cash_net_usd']
                    trade_cash_net_usd = dec(row['cash_net_usd'] if saved is None else saved)
                self.db.execute('''UPDATE funding_cycles SET trade_cash_net_usd=?,
                    trade_accounting_complete=MAX(trade_accounting_complete,?) WHERE cycle_id=?''',
                    (str(trade_cash_net_usd), int(trade_accounting_complete), cycle_id))
                net_pnl_usd = trade_cash_net_usd + state['actual_funding_usd']
            # Keep baseline, PnL and budget closure atomic. update_funding_pnl
            # opens its own transaction, so apply the same SQL here directly.
            self.db.execute('''UPDATE cycles SET pnl_usd=?,complete_accounting=?,
                accounting_final=0 WHERE cycle_id=?''',
                (str(net_pnl_usd) if net_pnl_usd is not None else None,
                 int(net_pnl_usd is not None), cycle_id))
            state = self.batch_state(row['batch_id'])
            budget = state['loss_budget_usd']
            if budget is not None and state['known_loss_used_usd'] >= budget:
                self._close_entries(row['batch_id'], 'REALIZED_LOSS_BUDGET',
                                    value=state['known_loss_used_usd'], threshold=budget,
                                    cycle_id=cycle_id)
            return net_pnl_usd

    def _basis_exit_phase(self, cycle: sqlite3.Row, *, phase: str,
                          entropy_units: int, hedge_units: int,
                          authoritative: bool, authoritative_ts: float | None) -> str:
        if cycle['basis_exit_requested_ts'] is None:
            return phase
        if cycle['phase'] != 'DONE' and phase != 'DONE':
            return 'BASIS_EXIT'
        if entropy_units or hedge_units or not authoritative or authoritative_ts is None \
                or self.pending():
            raise PendingOrder('latched exit requires authoritative flat and terminal orders for DONE')
        authoritative_ts = self._funding_time(authoritative_ts)
        balance = {'entropy': Decimal('0'), 'hedge': Decimal('0')}
        orders = self.cycle_orders(cycle['cycle_id'])
        common_step = None
        for order in orders:
            if (order['state'] != 'TERMINAL'
                    or (order['terminal_received_ts'] is not None
                        and order['terminal_received_ts'] > authoritative_ts)):
                raise PendingOrder('latched exit terminal evidence is not covered')
            if order['filled_units']:
                step = self.db.execute('SELECT quantity_step FROM live_steps WHERE order_ref=?',
                                       (order['order_ref'],)).fetchone()
                # Legacy prepare/record orders have no step row. Their units
                # share this cycle's common grid; an existing incomplete step
                # is damaged evidence and must not receive this fallback.
                if step is None:
                    continue
                try:
                    grid = dec(step['quantity_step']) if step['quantity_step'] is not None else None
                except (ValueError, TypeError, ArithmeticError) as exc:
                    raise PendingOrder('latched exit has invalid persistent quantity evidence') from exc
                if grid is None or grid <= 0:
                    raise PendingOrder('latched exit lacks persistent quantity evidence')
                if common_step is not None and grid != common_step:
                    raise PendingOrder('latched exit has conflicting persistent quantity grids')
                common_step = grid
        if common_step is None and any(order['filled_units'] for order in orders):
            raise PendingOrder('latched exit lacks persistent quantity evidence')
        for order in orders:
            if order['filled_units']:
                balance[order['venue']] += (Side(order['side']).sign * order['filled_units']
                                             * common_step)
        if any(balance.values()):
            raise PendingOrder('latched exit order quantities are not conserved')
        return 'DONE'

    def update_live_cycle(self, cycle_id: str, *, phase: str | None = None,
                          entropy_units: int | None = None,
                          hedge_units: int | None = None,
                          cash_delta_usd: Decimal = Decimal('0'),
                          accounting_final: bool = True,
                          position_evidence_authoritative: bool | None = None,
                          last_authoritative_ts: float | None = None,
                          updated_ts: float) -> None:
        if not cash_delta_usd.is_finite():
            raise ValueError('invalid cash delta')
        with self.transaction():
            row = self.db.execute('SELECT * FROM live_cycles WHERE cycle_id=?',
                                  (cycle_id,)).fetchone()
            if row is None:
                raise KeyError(cycle_id)
            e_units = row['entropy_units'] if entropy_units is None else entropy_units
            h_units = row['hedge_units'] if hedge_units is None else hedge_units
            if type(e_units) is not int or type(h_units) is not int:
                raise ValueError('positions must remain integer units')
            next_phase = row['phase'] if phase is None else phase
            if next_phase not in {
                'OPEN_FIRST', 'OPEN_HEDGE', 'OPEN', 'CLOSE_FIRST',
                'CLOSE_HEDGE', 'RECOVERY', 'DONE',
                'BASIS_EXIT',
            }:
                raise ValueError('invalid live phase')
            cash = dec(row['cash_net_usd']) + cash_delta_usd
            final = bool(row['accounting_final']) and accounting_final
            evidence_authoritative = (row['position_evidence_authoritative']
                if position_evidence_authoritative is None
                else int(position_evidence_authoritative))
            authoritative_ts = (row['last_authoritative_ts']
                if last_authoritative_ts is None else last_authoritative_ts)
            next_phase = self._basis_exit_phase(row, phase=next_phase,
                entropy_units=e_units, hedge_units=h_units,
                authoritative=bool(evidence_authoritative), authoritative_ts=authoritative_ts)
            self.db.execute('''UPDATE live_cycles SET phase=?,entropy_units=?,
                hedge_units=?,cash_net_usd=?,accounting_final=?,
                position_evidence_authoritative=?,last_authoritative_ts=?,updated_ts=?
                WHERE cycle_id=?''',
                (next_phase, e_units, h_units, str(cash), int(final),
                 evidence_authoritative, authoritative_ts, updated_ts, cycle_id))

    def lock_basis_profit_basis(self, cycle_id: str, *, quantity_step: Decimal,
                               locked_ts: float) -> bool:
        """Freeze the actual matched opening inventory's average leg notional.

        Pre-open excess reductions remove inventory at its average opening
        cost. Their realized price cash remains in the cycle's exit numerator.
        A missing historical basis is never reconstructed from a later residual.
        """
        quantity_step = dec(quantity_step)
        if quantity_step <= 0:
            raise ValueError('positive quantity step required')
        with self.transaction():
            cycle = self.db.execute('SELECT * FROM live_cycles WHERE cycle_id=?',
                                    (cycle_id,)).fetchone()
            if cycle is None:
                raise KeyError(cycle_id)
            if cycle['basis_exit_requested_ts'] is not None or cycle['phase'] in {'BASIS_EXIT', 'DONE'}:
                return False
            if cycle['basis_profit_basis_json'] is None:
                return False
            if cycle['basis_profit_basis_json'] != self.BASIS_PROFIT_PENDING:
                self.db.execute("UPDATE live_cycles SET phase='OPEN',updated_ts=? WHERE cycle_id=?",
                                (locked_ts, cycle_id))
                return True
            if cycle['phase'] not in {'OPEN_FIRST', 'OPEN_HEDGE', 'RECOVERY', 'OPEN'}:
                return False
            expected_sign = 1 if cycle['direction'] == 'long_entropy' else -1
            matched = cycle['entropy_units'] * expected_sign
            if matched <= 0 or cycle['hedge_units'] != -cycle['entropy_units']:
                return False
            inventory = {'entropy': [0, Decimal('0')], 'hedge': [0, Decimal('0')]}
            evidence = []
            for order in self.cycle_orders(cycle_id):
                if order['state'] != 'TERMINAL':
                    return False
                units = order['filled_units']
                if units:
                    try:
                        price = dec(order['avg_px']) if order['avg_px'] is not None else None
                    except (ValueError, TypeError, ArithmeticError):
                        price = None
                    if price is None or price <= 0:
                        return False
                    step = self.db.execute('SELECT quantity_step FROM live_steps WHERE order_ref=?',
                                           (order['order_ref'],)).fetchone()
                    try:
                        grid = dec(step['quantity_step']) if step is not None and step['quantity_step'] is not None else None
                    except (ValueError, TypeError, ArithmeticError):
                        grid = None
                    if grid != quantity_step:
                        return False
                    same_direction = Side(order['side']).sign == (expected_sign if order['venue'] == 'entropy' else -expected_sign)
                    qty, cost = inventory[order['venue']]
                    if same_direction:
                        if order['reduce_only']:
                            return False
                        inventory[order['venue']] = [qty + units, cost + price * units]
                    else:
                        if not order['reduce_only'] or units > qty:
                            return False
                        inventory[order['venue']] = [qty - units, cost * (qty - units) / qty]
                evidence.append([order['order_ref'], units, order['avg_px'], order['side'],
                                 order['venue'], bool(order['reduce_only'])])
            if any(qty != matched or cost <= 0 for qty, cost in inventory.values()):
                return False
            notional = sum((cost * quantity_step for qty, cost in inventory.values()), Decimal('0')) / 2
            basis = {'matched_units': matched, 'quantity_step': str(quantity_step),
                     'entry_notional_usd': str(notional), 'locked_ts': locked_ts,
                     'entry_inventory_cost_units': {key: str(cost) for key, (qty, cost) in inventory.items()},
                     'canonical_orders': evidence}
            self.db.execute("UPDATE live_cycles SET basis_profit_basis_json=?,phase='OPEN',updated_ts=? WHERE cycle_id=?",
                            (json.dumps(basis, sort_keys=True, separators=(',', ':')), locked_ts, cycle_id))
            return True

    def latch_basis_exit(self, cycle_id: str, *, requested_ts: float,
                         reason: str | None = None) -> None:
        """Commit the whole-pair close intent before either external submission."""
        with self.transaction():
            self._latch_basis_exit(cycle_id, requested_ts=requested_ts, reason=reason)

    def _latch_basis_exit(self, cycle_id: str, *, requested_ts: float,
                          reason: str | None = None) -> None:
        requested_ts = self._funding_time(requested_ts)
        if reason is not None and (not isinstance(reason, str) or not reason.strip()):
            raise ValueError('nonempty risk exit reason required')
        self.db.execute('''UPDATE live_cycles SET phase='BASIS_EXIT',
            basis_exit_requested_ts=COALESCE(basis_exit_requested_ts,?),
            risk_exit_reason=COALESCE(risk_exit_reason,?),updated_ts=?
            WHERE cycle_id=? AND phase!='DONE' ''',
            (requested_ts, reason, requested_ts, cycle_id))

    def ensure_live_step(self, *, step_key: str, cycle_id: str, order_ref: str,
                         venue: str, side: str, requested_units: int,
                         reduce_only: bool,
                         execution_group_id: str | None = None,
                         parent_step_key: str | None = None,
                         quantity_step: Decimal | None = None) -> dict:
        with self.transaction():
            return self._ensure_live_step(
                step_key=step_key, cycle_id=cycle_id, order_ref=order_ref,
                venue=venue, side=side, requested_units=requested_units,
                reduce_only=reduce_only, execution_group_id=execution_group_id,
                parent_step_key=parent_step_key, quantity_step=quantity_step)

    def _ensure_live_step(self, *, step_key: str, cycle_id: str, order_ref: str,
                         venue: str, side: str, requested_units: int,
                         reduce_only: bool,
                         execution_group_id: str | None = None,
                         parent_step_key: str | None = None,
                         quantity_step: Decimal | None = None) -> dict:
        if not step_key or not order_ref or venue not in {'entropy', 'hedge'} \
                or side not in {'buy', 'sell'} or requested_units < 1:
            raise ValueError('invalid live step')
        if quantity_step is not None:
            quantity_step = dec(quantity_step)
            if quantity_step <= 0:
                raise ValueError('invalid live quantity step')
        if parent_step_key is not None:
            parent = self.db.execute(
                'SELECT cycle_id,venue,execution_group_id FROM live_steps WHERE step_key=?',
                (parent_step_key,)).fetchone()
            if parent is None or parent['cycle_id'] != cycle_id \
                    or parent['venue'] != 'entropy' or venue != 'hedge' \
                    or (execution_group_id != parent_step_key and (
                        execution_group_id != parent['execution_group_id']
                        or not (execution_group_id or '').startswith(f'{cycle_id}:open_pair:'))):
                raise ValueError('invalid hedge parent step')
        row = self.db.execute('SELECT * FROM live_steps WHERE step_key=?',
                              (step_key,)).fetchone()
        expected = (cycle_id, order_ref, venue, side, requested_units,
                    int(reduce_only), execution_group_id, parent_step_key)
        if row:
            if (quantity_step is not None and row['quantity_step'] is not None
                    and dec(row['quantity_step']) != quantity_step):
                raise ValueError('live quantity step changed across restart')
            actual = (row['cycle_id'], row['order_ref'], row['venue'],
                      row['side'], row['requested_units'], row['reduce_only'],
                      row['execution_group_id'], row['parent_step_key'])
            if actual[:6] != expected[:6]:
                raise ValueError('live step identity changed across restart')
            if actual[6:] != expected[6:]:
                if (actual[6:] == (None, None)
                        and (execution_group_id == step_key
                             or parent_step_key is not None)):
                    # Pre-group steps may be resumed after schema upgrade.
                    # The validated parent or deterministic self-group can
                    # be added without changing order/fill identity.
                    self.db.execute('''UPDATE live_steps SET
                        execution_group_id=?,parent_step_key=?
                        WHERE step_key=?''',
                        (execution_group_id, parent_step_key, step_key))
                    self.db.execute('''UPDATE execution_economics_steps
                        SET execution_group_id=COALESCE(execution_group_id,?),
                            parent_step_key=COALESCE(parent_step_key,?)
                        WHERE step_key=?''',
                        (execution_group_id, parent_step_key, step_key))
                else:
                    raise ValueError('live step group changed across restart')
        else:
            self.db.execute('''INSERT INTO live_steps
                (step_key,cycle_id,order_ref,venue,side,requested_units,reduce_only,
                 execution_group_id,parent_step_key,quantity_step)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (step_key, *expected,
                 str(quantity_step) if quantity_step is not None else None))
            self.db.execute('''UPDATE live_steps SET
                baseline_entropy_units=(SELECT entropy_units FROM live_cycles WHERE cycle_id=?),
                baseline_hedge_units=(SELECT hedge_units FROM live_cycles WHERE cycle_id=?)
                WHERE step_key=?''', (cycle_id, cycle_id, step_key))
        return dict(self.db.execute(
            'SELECT * FROM live_steps WHERE step_key=?', (step_key,)).fetchone())

    def live_step(self, step_key: str) -> dict:
        row = self.db.execute('SELECT * FROM live_steps WHERE step_key=?',
                              (step_key,)).fetchone()
        if row is None:
            raise KeyError(step_key)
        return dict(row)

    def mark_step_abort(self, step_key: str, reason: str) -> None:
        with self.transaction():
            self.db.execute('UPDATE live_steps SET abort_reason=? WHERE step_key=?',
                            (reason, step_key))

    def bind_step_trigger(self, step_key: str, trigger: dict) -> None:
        with self.transaction():
            self.db.execute('''UPDATE live_steps SET
                trigger_ts=COALESCE(trigger_ts,?),
                trigger_basis_bps=COALESCE(trigger_basis_bps,?),
                trigger_mono_ns=COALESCE(trigger_mono_ns,?),
                trigger_clock_session_id=COALESCE(trigger_clock_session_id,?)
                WHERE step_key=?''',
                (trigger.get('ts'), str(trigger['basis']) if trigger.get('basis') is not None else None,
                 trigger.get('mono_ns'), trigger.get('session_id'), step_key))

    def live_steps(self, cycle_id: str) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            'SELECT * FROM live_steps WHERE cycle_id=? ORDER BY rowid',
            (cycle_id,))]

    def apply_live_step(self, step_key: str, *, result_units: int,
                        cash_delta_usd: Decimal, accounting_final: bool,
                        entropy_units: int, hedge_units: int,
                        next_phase: str, updated_ts: float,
                        position_evidence_authoritative: bool = True,
                        last_authoritative_ts: float | None = None) -> None:
        """Atomically apply one terminal exchange result to its live cycle."""
        if result_units < 0 or not cash_delta_usd.is_finite():
            raise ValueError('invalid step result')
        with self.transaction():
            step = self.db.execute('SELECT * FROM live_steps WHERE step_key=?',
                                   (step_key,)).fetchone()
            if step is None:
                raise KeyError(step_key)
            if step['applied']:
                if step['result_units'] != result_units \
                        or dec(step['cash_delta_usd']) != cash_delta_usd:
                    raise ValueError('applied live step result changed')
                return
            cycle = self.db.execute('SELECT * FROM live_cycles WHERE cycle_id=?',
                                    (step['cycle_id'],)).fetchone()
            if cycle is None:
                raise KeyError(step['cycle_id'])
            next_phase = self._basis_exit_phase(cycle, phase=next_phase,
                entropy_units=entropy_units, hedge_units=hedge_units,
                authoritative=position_evidence_authoritative,
                authoritative_ts=(cycle['last_authoritative_ts']
                    if last_authoritative_ts is None else last_authoritative_ts))
            cash = dec(cycle['cash_net_usd']) + cash_delta_usd
            final = bool(cycle['accounting_final']) and accounting_final
            self.db.execute('''UPDATE live_cycles SET phase=?,entropy_units=?,
                hedge_units=?,cash_net_usd=?,accounting_final=?,
                position_evidence_authoritative=?,last_authoritative_ts=COALESCE(?,last_authoritative_ts),
                updated_ts=?
                WHERE cycle_id=?''',
                (next_phase, entropy_units, hedge_units, str(cash), int(final),
                 int(position_evidence_authoritative), last_authoritative_ts,
                 updated_ts, step['cycle_id']))
            self.db.execute('''UPDATE live_steps SET applied=1,result_units=?,
                cash_delta_usd=?,accounting_final=? WHERE step_key=?''',
                (result_units, str(cash_delta_usd), int(accounting_final), step_key))

    def reserve_request(self, venue: str, *, now: float, limit: int,
                        reserve_for_completion: int, completion: bool,
                        request_kind: str | None = None,
                        cycle_id: str | None = None) -> bool:
        with self.transaction():
            return self._insert_request_event(
                venue, now=now, limit=limit,
                reserve_for_completion=reserve_for_completion, completion=completion,
                request_kind=request_kind, cycle_id=cycle_id) is not None

    def _insert_request_event(self, venue: str, *, now: float, limit: int,
                              reserve_for_completion: int, completion: bool,
                              request_kind: str | None = None,
                              cycle_id: str | None = None) -> float | None:
        """Persist the shared 60-second send budget across process restarts."""
        if venue not in {'entropy', 'hedge'} or limit < 1 \
                or not 0 <= reserve_for_completion < limit \
                or request_kind not in {None, 'open_first', 'open_hedge',
                                        'close_first', 'close_hedge', 'recovery'}:
            raise ValueError('invalid request budget')
        used = self.db.execute(
            'SELECT COALESCE(SUM(weight),0) FROM request_events '
            'WHERE venue=? AND used_ts>?',
            (venue, now - 60.0)).fetchone()[0]
        ceiling = limit if completion else limit - reserve_for_completion
        if used + 1 > ceiling:
            return None
        stamp = now
        while self.db.execute(
                'SELECT 1 FROM request_events WHERE venue=? AND used_ts=?',
                (venue, stamp)).fetchone():
            stamp = math.nextafter(stamp, math.inf)
        self.db.execute('''INSERT INTO request_events
            (venue,used_ts,weight,request_kind,cycle_id)
            VALUES (?,?,1,?,?)''',
            (venue, stamp, request_kind, cycle_id))
        return stamp

    def finish_cycle(self, cycle_id: str, *, kind: str, net_pnl_usd: Decimal | None,
                     positions_verified_flat: bool, all_orders_terminal: bool,
                     accounting_final: bool = True, conservative_pnl_bound: bool = False) -> None:
        if kind not in {'clean', 'repair', 'unwound', 'zero_fill'}:
            raise ValueError('unrecognized result kind')
        if not positions_verified_flat or not all_orders_terminal or self.pending():
            raise PendingOrder('cannot complete without authoritative per-venue FLAT and terminal orders')
        if net_pnl_usd is not None and not accounting_final and not conservative_pnl_bound:
            raise ValueError('provisional PnL must be an explicit conservative bound')
        if net_pnl_usd is not None and not net_pnl_usd.is_finite():
            raise ValueError('non-finite pnl')
        with self.transaction():
            row = self.db.execute('SELECT * FROM cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
            if row is None:
                raise KeyError(cycle_id)
            if row['result_kind'] is not None and row['result_kind'] != kind:
                raise ValueError('cannot relabel a repair as clean')
            self.db.execute('''UPDATE cycles SET result_kind=?,pnl_usd=?,complete_accounting=?,accounting_final=?,
                               completed_ts=COALESCE(completed_ts,?)
                               WHERE cycle_id=?''',
                            (kind, str(net_pnl_usd) if net_pnl_usd is not None else None,
                             int(net_pnl_usd is not None), int(accounting_final and net_pnl_usd is not None),
                             time.time(), cycle_id))
            batch_id = row['batch_id']
            state = self.batch_state(batch_id)
            budget = state['loss_budget_usd']
            if budget is not None and state['known_loss_used_usd'] >= budget:
                self._close_entries(
                    batch_id, 'REALIZED_LOSS_BUDGET',
                    value=state['known_loss_used_usd'], threshold=budget,
                    cycle_id=cycle_id)

    def observe_open_risk(self, batch_id: str, *,
                          active_cycle_net_pnl_usd: Decimal,
                          cycle_id: str | None = None) -> bool:
        """Stop entries on V3 realized losses plus priced open exposure only."""
        if not active_cycle_net_pnl_usd.is_finite():
            raise ValueError('invalid risk observation')
        state = self.batch_state(batch_id)
        budget = state['loss_budget_usd']
        projected = state['loss_used_usd'] + max(-active_cycle_net_pnl_usd,
                                                 Decimal('0'))
        if budget is not None and projected >= budget:
            self.close_entries(batch_id, 'SINGLE_CYCLE_REVIEW_LOSS',
                               value=active_cycle_net_pnl_usd,
                               threshold=-(budget - state['loss_used_usd']),
                               cycle_id=cycle_id)
            return False
        return self.batch_state(batch_id)['can_open']
