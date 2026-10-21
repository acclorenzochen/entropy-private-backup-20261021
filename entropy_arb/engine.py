"""Two-venue arbitrage engine: Entropy vs one hedge venue.

The signal is a position-aware band around a robust rolling midline:

    SELL entropy / BUY hedge  when executable premium >= midline + upper (+fees)
    BUY entropy / SELL hedge  when executable premium <= midline - lower (+fees)

OPEN/ADD uses statistical bands and projected whole-cycle economics. Normal
REDUCE uses remaining FIFO costs when profit_guard is enabled. Emergency
recovery never depends on profitability. Around the signal: persistence,
per-venue inventory ladder + position caps, per-venue order budgets and
reactive rate-limit exclusion, net-delta hedging, venue-outage pausing with
probing, and periodic on-chain reconciliation. There is no paper mode: the
bot either trades live or runs --record-only (data collection, no strategy).
Both venues' books are recorded to 1-minute bars and 1-second top-of-book rows.
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import math
import os
import socket
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field, replace
from typing import Dict, List, Optional

import aiohttp

from .book import ArbPlan, floor_step, plan_arb, walk_depth
from .config import (Config, ConfigError, load_config,
                     merge_hot_reloadable_config)
from .cycle_policy import evaluate_cycle_exit
from .cycle_state import load_cycle_state, safe_write_cycle_state
from .dashboard_state import (EquityPnlTracker, build_dashboard_snapshot,
                              safe_write_dashboard_snapshot)
from .execution_state import (
    EXECUTION_SAFETY_FLAT,
    HEDGE_COMPLETION_NOT_STARTED,
    HEDGE_COMPLETION_PENDING,
    HEDGE_ORDER_UNKNOWN,
    POSITION_COVERED_CONFIRMED,
    POSITION_COVERED_UNANCHORED,
    ExecutionPhase,
    ExecutionState,
    RecoveryStatus,
)
from .execution_cost import ExecutionCostModel
from .control_plane import (choose_execution_room, is_clean_circle,
                            fill_probability_bucket)
from .funding import expected_pair_funding_cost_bps
from .recorder import (MinuteRecorder, SecondRecorder,
                       bootstrap_premium_samples)
from .trade_ledger import TradeLedger
from .retro_farming import build_shadow_retro_snapshot
from .wechat_clawbot import WeChatHourlyReporter
from .signal import (PairState, PremiumModelSnapshot, RollingPremiumModel,
                     InventoryCycle, StrategyAction, PremiumDataHealth,
                     PremiumSampleQuality, PremiumDataTier,
                     PremiumReadiness, MidlineLagState,
                     compute_adaptive_entry_band,
                     compute_cycle_midline_drift,
                     cycle_direction_for_pair_state,
                     entry_threshold_long, entry_threshold_short,
                     exit_threshold_long, exit_threshold_short,
                     is_cycle_add_blocked, is_volatility_guarded,
                     check_premium_sample_quality, pair_state, paired_qty)
from .venue_hl import HLVenue
from .venue_lighter import LighterVenue

log = logging.getLogger("engine")

CSV_HEADER = ["ts", "direction", "action", "pair_state_before",
              "pair_state_after", "reduce_only",
              "buy_venue", "sell_venue", "qty",
              "buy_limit", "sell_limit", "buy_notional", "sell_notional",
              "exp_edge_usd", "gross_edge_usd", "marginal_premium_bps",
              "midline_bps", "inv_add_bps", "ok", "buy_fill", "sell_fill",
              "buy_status", "sell_status", "fill_edge_usd",
              "active_midline_bps", "short_center_bps", "long_center_bps",
              "robust_sigma_bps", "regime_shift", "threshold_bps",
              "effective_entry_band_bps", "cycle_entry_midline_bps",
              "cycle_entry_reference_mode", "cycle_entry_active_midline_bps",
              "cycle_entry_dynamic_center_bps",
              "cycle_entry_sigma_bps", "cycle_midline_drift_bps",
              "volatility_guard", "cycle_drift_guard",
              "entropy_funding_bps_h", "hedge_funding_bps_h",
              "expected_funding_cost_bps", "net_marginal_edge_bps",
              "edge_surplus_bps", "per_leg_slippage_bps",
              "midline_lag_bps", "midline_lag_state",
              "premium_sample_valid", "cross_venue_skew_ms"]
BALANCE_POLL_SEC = 30.0
FEE_RECONCILIATION_POLL_SEC = 10.0
FEE_RECONCILIATION_LOOKBACK_SEC = 15 * 60.0
FEE_RECONCILIATION_INITIAL_LOOKBACK_SEC = 7 * 24 * 3600.0
RECOVERY_BURST_ATTEMPTS = 3
RECOVERY_RETRY_DELAY_SEC = 0.05
MAX_STABLE_ORDER_REF = (1 << 48) - 1
MODEL_STATE_KEY = "premium_model"
MODEL_RESTORE_VALIDATION_SEC = 60.0
BLOCKED_ENTRY_SIGMA_MULT = 1.5
BOOTSTRAP_RECOVERY_GRACE_SEC = 30.0
BOOTSTRAP_PERMANENT_DISABLE_REASONS = {
    "cumulative_loss_budget",
    "cycle_loss_budget",
    "max_complete_cycles",
    "manual_disable",
}


def _safe_float(value) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _nearest_percentile(values: list[float], percentile: float) -> Optional[float]:
    """Return a conservative nearest-rank percentile for bounded samples."""
    if not values:
        return None
    ordered = sorted(float(value) for value in values
                     if math.isfinite(float(value)))
    if not ordered:
        return None
    index = min(len(ordered) - 1,
                max(0, math.ceil(len(ordered) * float(percentile)) - 1))
    return ordered[index]


def _choose_open_execution_limit(*, economic_bound: float,
                                 physical_bound: float,
                                 current_executable: float,
                                 latency_required_room_bps: float,
                                 is_buy: bool) -> dict:
    """Combine an economic limit with a physical hard bound.

    The economic limit is the maximum price the cycle can afford.  The
    physical limit is an independent safety cap.  A latency room check can
    reject a stale/too-tight quote, but it must not widen either bound.
    """
    values = (economic_bound, physical_bound, current_executable,
              latency_required_room_bps)
    if not all(math.isfinite(float(value)) for value in values):
        return {"allowed": False, "reason": "invalid_execution_bound"}
    if economic_bound <= 0 or physical_bound <= 0 or current_executable <= 0:
        return {"allowed": False, "reason": "invalid_execution_bound"}
    limit = (min(economic_bound, physical_bound) if is_buy
             else max(economic_bound, physical_bound))
    marketable = (limit + 1e-12 >= current_executable if is_buy
                  else limit - 1e-12 <= current_executable)
    room = ((limit / current_executable - 1.0) * 1e4 if is_buy
            else (current_executable / limit - 1.0) * 1e4)
    room = max(room, 0.0) if math.isfinite(room) else 0.0
    required = max(float(latency_required_room_bps), 0.0)
    allowed = marketable and room + 1e-9 >= required
    return {
        "allowed": allowed,
        "reason": ("ready" if allowed else
                   "economic_bound_not_marketable" if not marketable else
                   "insufficient_latency_execution_room"),
        "limit_px": limit,
        "marketable": marketable,
        "room_bps": room,
        "latency_required_room_bps": required,
    }


@dataclass
class TradeCandidate:
    direction: str
    action: StrategyAction
    buy: object
    sell: object
    threshold_bps: float
    cap_notional: float
    reduce_only: bool
    funding_cost_bps: float
    pair_state_before: PairState
    effective_entry_band_bps: Optional[float] = None
    cycle_entry_midline_bps: Optional[float] = None
    cycle_entry_sigma_bps: Optional[float] = None
    cycle_midline_drift_bps: Optional[float] = None
    volatility_guard: bool = False
    cycle_drift_guard: bool = False
    cycle_entry_reference_mode: Optional[str] = None
    cycle_entry_active_midline_bps: Optional[float] = None
    cycle_entry_dynamic_center_bps: Optional[float] = None
    reference_state: Optional[str] = None
    reference_confidence: Optional[float] = None
    required_deviation_bps: Optional[float] = None
    signal_deviation_bps: Optional[float] = None
    extreme_dislocation: bool = False


@dataclass
class PlannedTrade:
    candidate: TradeCandidate
    plan: ArbPlan
    net_marginal_edge_bps: float
    edge_surplus_bps: float
    per_leg_slippage_bps: float
    buy_bound: float
    sell_bound: float
    cycle_budget: dict = field(default_factory=dict)

    def __iter__(self):
        """Keep the former _scan() tuple contract for small integrations."""
        yield self.candidate.buy
        yield self.candidate.sell
        yield self.plan


class Engine:
    # A cached position can briefly look flat while an exchange is settling.
    # Require the flat state to persist before treating an external/manual
    # close as the end of a stale unmatched execution.
    EXTERNAL_FLAT_CONFIRMATION_SEC = 2.0
    # An OPEN opportunity stays locked until the basis returns inside the
    # entry threshold by this margin. A fixed reset zone avoids treating
    # every small basis move as a new opportunity.
    OPEN_SIGNAL_RESET_BPS = 1.0
    MAX_OPEN_OPPORTUNITY_CACHE = 128

    def __init__(self, cfg: Config, record_only: bool = False,
                 config_file: Optional[str] = None,
                 env_file: str = ".env",
                 ledger_path: Optional[str] = None,
                 strategy_name: str = "legacy",
                 flatten_only: bool = False,
                 max_complete_cycles: Optional[int] = None,
                 max_run_loss_usd: Optional[float] = None) -> None:
        self.cfg = cfg
        self.strategy_name = strategy_name
        self.record_only = record_only
        self.flatten_only = bool(flatten_only)
        self.max_complete_cycles = (None if max_complete_cycles is None
                                     else max(int(max_complete_cycles), 1))
        self.max_run_loss_usd = (None if max_run_loss_usd is None
                                 else max(float(max_run_loss_usd), 0.0))
        self.completed_cycle_count = 0
        self._counted_cycle_ids: set[str] = set()
        self._accounted_run_cycle_ids: set[str] = set()
        self.run_realized_pnl_usd = 0.0
        # The watcher is opt-in for integrations that construct Engine
        # directly.  The CLI passes the actual --config path.
        self.config_file = (os.path.abspath(config_file)
                            if config_file else None)
        self.env_file = env_file
        # Direct unit/integration constructors use an in-memory ledger unless
        # a path is explicitly supplied. The CLI always passes config_file and
        # therefore gets the configured durable database.
        self.ledger_path = (ledger_path if ledger_path is not None else
                            (cfg.persistence.sqlite_path
                             if config_file else ":memory:"))
        self.ledger = TradeLedger(self.ledger_path)
        self.retro_farming_snapshot: dict = {
            "schema_version": 1,
            "assumption": "hypothetical_retro_score_coverage_not_official_points",
            "status": "not_loaded",
            "shadow_canary_plan": {
                "mode": "shadow", "status": "not_loaded",
                "eligible": False, "should_execute": False,
            },
        }
        # Execution-cost samples are durable and bounded separately from the
        # fill ledger.  They are restored before the first scan so a restart
        # does not immediately revert to a physical worst-case reserve.
        self.execution_cost_model = ExecutionCostModel(self.ledger)
        restored_cost_samples = self.execution_cost_model.restore()
        if restored_cost_samples:
            log.info("execution cost model restored: %d samples",
                     restored_cost_samples)
        if self.ledger_path != ":memory:" and cfg.persistence.migrate_legacy:
            self.ledger.migrate_legacy(
                trades_paths=(cfg.trades_csv, cfg.trades_csv + ".old"),
                seconds_paths=(cfg.recorder_seconds_csv,
                               cfg.recorder_seconds_csv + ".old"),
                minutes_paths=(cfg.recorder_csv, cfg.recorder_csv + ".old"))
        if self.ledger_path != ":memory:":
            backfill = self.ledger.backfill_execution_cost_observations()
            if backfill.get("inserted"):
                log.info("execution cost backfill: %s", backfill)
                restored_cost_samples = self.execution_cost_model.restore()
        repaired_overfills = self.ledger.repair_cumulative_overfills()
        if repaired_overfills.get("repaired_rows"):
            log.warning("cumulative fill overfill repair: %s",
                        repaired_overfills)
        self._restored_execution = self.ledger.load_active_execution()
        self._last_config_file_signature = None
        self.last_config_reload_ts: Optional[float] = None
        self.last_config_reload_error: Optional[str] = None
        self.config_restart_required = False
        self.config_restart_required_paths: list[str] = []
        self.session: Optional[aiohttp.ClientSession] = None
        self.wechat_reporter: Optional[WeChatHourlyReporter] = None
        self.entropy = None
        self.hedge = None
        self.venues: Dict[str, object] = {}
        self._venues_closed = False
        self.recorder: Optional[MinuteRecorder] = None
        self.seconds_recorder: Optional[SecondRecorder] = None
        self.markets_ready = False
        self.stop = asyncio.Event()
        self._update_evt = asyncio.Event()
        self._reconcile_evt = asyncio.Event()
        # per-venue locks: an execution holds both; a reconcile holds one, so
        # a chain read can never race an in-flight order on that venue
        self._venue_locks: Dict[str, asyncio.Lock] = {}
        self._recovery_lock = asyncio.Lock()
        self._exec_tasks: set = set()
        self._recovery_poll_sec = 0.5
        # Recovery must not inspect an order while the foreground execution
        # call is still awaiting its response.
        self._execution_inflight = False
        # Exchange positions can prove quantity, but never historical cost.
        # Keep this separate from ``active_execution`` so an external/manual
        # inventory mismatch cannot accidentally become a normal cycle.
        self._orphaned_inventory = False
        self._orphaned_since_ts: Optional[float] = None
        # Emergency slippage escalation belongs to one flatten campaign, not
        # to a venue for the lifetime of the process.  A new risk exit starts
        # at the first bound again, while repeated calls for the same residual
        # inventory continue the existing campaign.
        self._emergency_campaign: Optional[dict] = None
        self._emergency_pending: Dict[str, dict] = {}
        # A strategy execution is not complete when the first API call
        # returns.  Keep its identity until both legs (or an explicit
        # reduce-only recovery) have been verified balanced.
        self.active_execution: Optional[ExecutionState] = None
        self._active_trade: Optional[PlannedTrade] = None
        self._pending_execution_record: Optional[dict] = None
        self.last_execution_outcome: Optional[str] = None
        self.halted = False
        self.consec_errors = 0
        self.last_trade_ts = 0.0
        self.trades = 0
        self.hedges = 0
        self.total_exp_edge = 0.0
        self.total_fill_edge = 0.0
        self.start_ts = time.time()
        bootstrap_defaults = {
            "version": 1,
            "started_ts": self.start_ts,
            "disabled": False,
            "disabled_reason": None,
            "suspended": False,
            "suspended_reason": None,
            "suspended_ts": None,
            "resume_after_ts": None,
            "resumed_ts": None,
            "accepted_count": 0,
            "candidate_count": 0,
            "accepted_cycle_count": 0,
            "realized_pnl_usd": 0.0,
            "phase_samples": {
                "open_first": 0, "open_hedge": 0,
                "close_first": 0, "close_hedge": 0,
            },
            "green_count": 0,
            "yellow_count": 0,
            "red_count": 0,
            "last_color": None,
            "last_decision_ts": None,
            "best_expected_pnl_usd": None,
            "worst_accepted_expected_pnl_usd": None,
        }
        self._bootstrap_state = bootstrap_defaults
        restored_bootstrap = self.ledger.load_model_state("bootstrap")
        if restored_bootstrap is not None:
            _, saved_bootstrap = restored_bootstrap
            if isinstance(saved_bootstrap, dict):
                self._bootstrap_state.update({
                    key: saved_bootstrap[key]
                    for key in bootstrap_defaults
                    if key in saved_bootstrap
                })
                saved_samples = saved_bootstrap.get("phase_samples")
                if isinstance(saved_samples, dict):
                    self._bootstrap_state["phase_samples"] = {
                        phase: max(int(saved_samples.get(phase, 0) or 0), 0)
                        for phase in bootstrap_defaults["phase_samples"]
                    }
        self._bootstrap_state["phase_samples"] = {
            phase: max(int((self._bootstrap_state.get("phase_samples") or
                            {}).get(phase, 0) or 0), 0)
            for phase in bootstrap_defaults["phase_samples"]
        }
        # Older runs persisted the recovery mismatch as a permanent
        # auto-disable. Migrate only that safety latch to the new
        # suspend-until-reconciled state; loss/cycle/manual disables remain
        # permanent by design.
        if (self._bootstrap_state.get("disabled")
                and self._bootstrap_state.get("disabled_reason")
                == "critical_recovery_mismatch"):
            legacy_ts = (_safe_float(
                self._bootstrap_state.get("disabled_ts"))
                         or self.start_ts)
            self._bootstrap_state.update({
                "disabled": False,
                "disabled_reason": None,
                "suspended": True,
                "suspended_reason": "critical_recovery_mismatch",
                "suspended_ts": legacy_ts,
                "resume_after_ts": max(
                    self.start_ts,
                    legacy_ts + BOOTSTRAP_RECOVERY_GRACE_SEC),
            })
            self._persist_bootstrap_state()
        self._maybe_auto_disable_bootstrap()
        self.pnl_tracker = EquityPnlTracker(
            cfg.dashboard_pnl_state_json, cfg.dashboard_day_timezone)
        self._last_skiplog = 0.0
        self._poke_due: Optional[float] = None
        # per-direction persistence arming: direction key -> first-seen ts
        self._armed: Dict[str, Optional[float]] = {}
        self._step = 1e-4
        self._min_base = 0.0
        self._min_notional = 10.0
        self._mtm_baseline: Optional[float] = None
        # proactive per-venue send budget: timestamps of recent order sends
        self._sends: Dict[str, deque] = {}
        self._action_sends: dict[tuple[str, str], deque] = {}
        # reactive per-venue throttle: venue key -> excluded until
        self._venue_limited_until: Dict[str, float] = {}
        self._order_budget_blocked_since: Optional[float] = None
        self._order_budget_block_total_sec = 0.0
        self._next_order_budget_available_ts: Optional[float] = None
        # venue outage tracking: key -> down-since ts; a down venue pauses
        # trading and is probed every venue_probe_sec until it answers
        self._venue_down: Dict[str, float] = {}
        self._venue_probe_at: Dict[str, float] = {}
        self._venue_fetch_fails: Dict[str, int] = {}
        # execution id -> first observation time of both venues being flat
        self._external_flatten_seen_at: Dict[str, float] = {}
        # Runtime-only persistence timer for the direction-aware adverse-drift
        # exit. It is intentionally reset when the threshold is no longer met.
        self._adverse_drift_started_ts: Optional[float] = None
        # Economic hold state is separate from the execution lifecycle.  It
        # survives a scan without creating a reduce order, and is cleared when
        # the paired inventory is flat.
        self._cycle_hold_state: Optional[str] = None
        self._cycle_hold_state_ts: Optional[float] = None
        self._cycle_budget_persist_ts = 0.0
        self._last_cycle_budget: dict = {}
        # Bounded top-of-book history used only to estimate the adverse move
        # while a close hedge is being submitted.  It is deliberately small
        # and runtime-only; the durable audit trail remains SQLite.
        self._hedge_quote_history: deque[dict] = deque(maxlen=4096)
        self._last_hedge_quote_ts: Optional[float] = None
        self._hedge_latency_sec_ewma = 0.5
        self._hedge_latency_samples_sec: deque[float] = deque(maxlen=64)
        # Bounded Entropy top-of-book history used to price an OPEN first leg
        # for the observed submit-to-result latency.
        self._open_quote_history: deque[dict] = deque(maxlen=4096)
        self._last_open_quote_ts: Optional[float] = None
        self._open_latency_sec_ewma = 0.8
        self._open_latency_samples_sec: deque[float] = deque(maxlen=64)
        self._open_fill_probability: dict[str, dict[str, int]] = {
            bucket: {"attempts": 0, "fills": 0}
            for bucket in ("0-2", "2-4", "4-6", "6-8", "8-10", "10+")
        }
        self._close_fill_probability: dict[str, dict[str, int]] = {
            bucket: {"attempts": 0, "fills": 0}
            for bucket in ("0-2", "2-4", "4-6", "6-8", "8-10", "10+")
        }
        # The sequence is process-local; scope it with a run id so durable
        # funnel event ids cannot collide after a restart.
        self.run_id = uuid.uuid4().hex[:12]
        self._open_opportunity_seq = 0
        self._open_opportunities: Dict[str, dict] = {}
        self._open_outcome_history: deque[dict] = deque(maxlen=256)
        self._open_degraded_until: Dict[str, float] = {}
        self._last_open_opportunity: Optional[dict] = None
        self._open_campaign: Optional[dict] = None
        self._open_campaign_telemetry: dict = {
            "open_opportunity_count": 0,
            "open_send_count": 0,
            "open_first_attempt_fill_count": 0,
            "open_campaign_fill_count": 0,
            "open_first_attempt_fill_rate": 0.0,
            "open_campaign_fill_rate": 0.0,
            "open_zero_fill_count": 0,
            "retry_count_total": 0,
            "attempts_to_fill": [],
            "opportunity_to_fill_ms": [],
            "orders_per_successful_open": 0.0,
        }
        self._open_block_telemetry: dict = {
            "active_reason": None,
            "active_since_ts": None,
            "last_reason": None,
            "last_ts": None,
            "count_by_reason": {},
            "seconds_by_reason": {},
        }
        self._exit_opportunity_seq = 0
        self._exit_campaign: Optional[dict] = None
        self._last_exit_campaign: Optional[dict] = None
        self._exit_campaign_telemetry: dict = {
            "exit_opportunity_count": 0,
            "exit_campaign_count": 0,
            "first_attempt_fill_count": 0,
            "campaign_fill_count": 0,
            "first_attempt_fill_rate": 0.0,
            "campaign_fill_rate": 0.0,
            "retry_count_total": 0,
            "opportunity_to_fill_ms": [],
            "missed_profitable_exit_count": 0,
        }
        # per-execution records for the dashboard (newest last)
        self.recent_trades: deque = deque(maxlen=50)
        self.cycle = (load_cycle_state(cfg.cycle_exit.state_json)
                      if cfg.cycle_exit.enabled else InventoryCycle())
        self._cycle_telemetry: dict = {}
        self._last_cycle_telemetry: dict = {}
        if self.cycle.cycle_id:
            restored_cycle = self.ledger.load_cycle(self.cycle.cycle_id)
            if restored_cycle:
                self._cycle_telemetry = restored_cycle
                restored_hold = restored_cycle.get("hold_state")
                if restored_hold in {"loss_recovery_hold", "long_hold",
                                     "forced_release_pending",
                                     "hard_timeout_release"}:
                    self._cycle_hold_state = restored_hold
                    self._cycle_hold_state_ts = _safe_float(
                        restored_cycle.get("hold_state_ts"))
        self.close_cost_ewma_bps: Optional[float] = None
        self.premium_health = PremiumDataHealth(
            cfg.premium_model.degraded_after_sec)
        self.last_premium_sample_quality: Optional[PremiumSampleQuality] = None
        model_warmup_sec, model_min_samples = self._dynamic_model_warmup(cfg)
        self._model_warmup_sec = model_warmup_sec
        self._model_min_samples = model_min_samples
        self._model_history_source = "seed"
        self._model_history_samples = 0
        self.premium_model = RollingPremiumModel(
            short_window_sec=cfg.dynamic_short_window_sec,
            long_window_sec=cfg.dynamic_long_window_sec,
            warmup_sec=model_warmup_sec,
            min_samples=model_min_samples,
            shift_min_bps=cfg.dynamic_shift_min_bps,
            shift_sigma_mult=cfg.dynamic_shift_sigma_mult,
            recovery_ratio=cfg.dynamic_recovery_ratio,
            recovery_sec=cfg.dynamic_recovery_sec,
            seed_midline_bps=cfg.midline_bps,
            max_move_bps_per_10m=cfg.dynamic_max_move_bps_per_10m,
            base_move_bps_per_10m=cfg.dynamic_base_move_bps_per_10m,
            accelerated_move_bps_per_10m=(
                cfg.dynamic_accelerated_move_bps_per_10m),
            absolute_max_move_bps_per_10m=(
                cfg.dynamic_absolute_max_move_bps_per_10m),
            sigma_window_sec=cfg.adaptive_bands.sigma_window_sec,
            midline_lag_guard_enabled=cfg.midline_lag_guard.enabled,
            midline_lag_warn_bps=cfg.midline_lag_guard.warn_bps,
            midline_lag_block_bps=cfg.midline_lag_guard.block_bps,
            midline_lag_persist_sec=cfg.midline_lag_guard.persist_sec,
            midline_lag_accelerate_bps=cfg.midline_lag_guard.accelerate_bps,
            midline_lag_accelerate_persist_sec=(
                cfg.midline_lag_guard.accelerate_persist_sec),
            midline_lag_recovery_sec=cfg.midline_lag_guard.recovery_sec,
            midline_lag_recovery_bps=cfg.midline_lag_guard.recovery_bps)
        self._model_restore_until = 0.0
        self._model_state_loaded = False
        self._last_model_state_save_ts = 0.0
        restored_close_cost = self.ledger.load_model_state("close_cost_ewma")
        if restored_close_cost is not None:
            _, close_cost_state = restored_close_cost
            self.close_cost_ewma_bps = _safe_float(
                close_cost_state.get("bps"))
        restored_model = self.ledger.load_model_state(MODEL_STATE_KEY)
        if restored_model is not None:
            saved_ts, state = restored_model
            restore_now = time.time()
            state_age = max(restore_now - saved_ts, 0.0)
            restored_count = self.premium_model.restore_state(
                state, restore_now)
            self._model_state_loaded = bool(
                restored_count > 0 or self.premium_model.has_prior)
            if self._model_state_loaded:
                self._model_history_source = "checkpoint"
                self._model_history_samples = restored_count
                self._model_restore_until = (
                    restore_now + MODEL_RESTORE_VALIDATION_SEC)
                log.info("premium model restored from SQLite: %d samples "
                         "(checkpoint age %.0fs); OPEN validation %.0fs",
                         restored_count, state_age,
                         MODEL_RESTORE_VALIDATION_SEC)
            else:
                log.info("premium model checkpoint had no usable samples "
                         "(age %.0fs); historical backfill required", state_age)
        self.model_snapshot = self.premium_model.snapshot(time.time())
        self.last_selected: Optional[PlannedTrade] = None
        self.profit_checks: dict[str, dict] = {}
        self._recovery_context: dict = {}
        self._last_funding_accrual_ts: Optional[float] = None
        self._funding_uncertainty_bps = 0.0
        self.startup_reconciled = False
        self.reconciliation = {
            "status": "not_started",
            "last_ts": None,
            "startup_reconciled": False,
            "net_delta_base": None,
            "venues": {},
            "last_error": None,
        }
        self.fee_reconciliation = {
            "status": "disabled" if self.record_only else "starting",
            "initial_sync_done": False,
            "last_ts": None,
            "last_success_ts": None,
            "last_error": None,
            "fetched_count": 0,
            "stored_count": 0,
            "actual_fill_count": 0,
            "unmatched_count": 0,
            "actual_fee_usd": 0.0,
            "fee_delta_usd": 0.0,
            "cash_adjustment_usd": 0.0,
        }

    # ------------------------------------------------------------- utilities

    def _bootstrap_cfg(self):
        return getattr(self.cfg, "bootstrap", None)

    def _dynamic_model_warmup(self, cfg: Optional[Config] = None
                              ) -> tuple[float, int]:
        """Return the effective dynamic-model warmup for this run.

        Bootstrap is an economic sampling policy only.  Model readiness is
        therefore identical for every execution mode; a short bootstrap
        warmup would make the first sampled cycles use a different signal
        model from normal trading.
        """
        target = self.cfg if cfg is None else cfg
        warmup = float(getattr(target, "dynamic_warmup_sec", 1800.0))
        minimum = int(getattr(target, "dynamic_min_samples", 600))
        return max(warmup, 0.0), max(minimum, 1)

    def _bootstrap_active(self) -> bool:
        """Return whether the bounded exploratory economics are in force."""
        cfg = self._bootstrap_cfg()
        state = getattr(self, "_bootstrap_state", {}) or {}
        return bool(cfg is not None and getattr(cfg, "enabled", False)
                    and not getattr(self, "record_only", False)
                    and not getattr(self, "flatten_only", False)
                    and not state.get("disabled", False)
                    and not state.get("suspended", False))

    def _apply_dynamic_model_warmup(self) -> None:
        """Apply the effective model readiness policy after a mode change."""
        model = getattr(self, "premium_model", None)
        if model is None:
            return
        warmup, minimum = self._dynamic_model_warmup()
        model.warmup_sec = warmup
        model.min_samples = minimum
        self._model_warmup_sec = warmup
        self._model_min_samples = minimum

    def _bootstrap_policy_active(
            self, action: Optional[StrategyAction] = None) -> bool:
        """Return whether an action belongs to a bootstrap cycle.

        CLOSE/HEDGE must retain the bootstrap execution policy after the OPEN
        budget has been disabled, otherwise a cycle started in exploration can
        silently fall back to the slower normal campaign halfway through.
        """
        action_value = (action.value if isinstance(action, StrategyAction)
                        else None if action is None else str(action).lower())
        if action_value in {StrategyAction.OPEN.value, StrategyAction.ADD.value} \
                or action is None:
            return self._bootstrap_active()
        if action_value != StrategyAction.REDUCE.value:
            return self._bootstrap_active()
        context = getattr(self, "_recovery_context", {}) or {}
        budget = context.get("cycle_budget") or {}
        if budget.get("entry_mode") == "bootstrap":
            return True
        record = getattr(self, "_cycle_telemetry", {}) or {}
        return record.get("entry_mode") == "bootstrap"

    def _open_execution_room_min_bps(
            self, *, bootstrap_active: Optional[bool] = None) -> float:
        """Return the diagnostic minimum room for the OPEN first leg.

        The argument remains for compatibility with older callers, but
        bootstrap must not override normal execution parameters.
        """
        policy = getattr(self.cfg, "reduce_execution", None)
        normal = _safe_float(getattr(
            policy, "open_execution_room_min_bps", 0.0))
        room = 0.0 if normal is None else normal
        return max(room, 0.0)

    def _economic_floor_usd(self, notional: float) -> float:
        """Scale the whole-cycle floor while keeping legacy callers valid."""
        economics = getattr(self.cfg, "economic_execution", None)
        usd = _safe_float(getattr(economics,
                                  "min_expected_cycle_profit_usd", None))
        bps = _safe_float(getattr(economics,
                                  "min_expected_cycle_profit_bps", None))
        if usd is None:
            usd = 0.003
        if bps is None:
            return max(usd, 0.0)
        return max(usd, max(float(notional), 0.0) * max(bps, 0.0) / 1e4)

    def _bootstrap_floor_usd(self, notional: float) -> float:
        """Return the larger (less permissive) bootstrap loss floor."""
        cfg = self._bootstrap_cfg()
        if cfg is None:
            return -0.0
        value = max(float(notional), 0.0)
        return -max(
            min(float(cfg.max_expected_loss_usd),
                value * float(cfg.max_expected_loss_bps) / 1e4),
            0.0)

    def _persist_bootstrap_state(self) -> None:
        try:
            self.ledger.save_model_state(
                "bootstrap", dict(self._bootstrap_state), ts=time.time())
        except Exception:
            log.warning("bootstrap state checkpoint failed", exc_info=True)

    def _bootstrap_snapshot(self) -> dict:
        """Expose bounded bootstrap state for the API/dashboard."""
        cfg = self._bootstrap_cfg()
        state = getattr(self, "_bootstrap_state", {}) or {}
        configured = bool(cfg is not None and getattr(cfg, "enabled", False))
        active = self._bootstrap_active()
        if active:
            mode = "bootstrap"
        elif bool(state.get("disabled")):
            mode = "normal_after_auto_disable"
        elif bool(state.get("suspended")):
            mode = "suspended"
        elif configured:
            mode = "disabled"
        else:
            mode = "normal"
        realized = _safe_float(state.get("realized_pnl_usd")) or 0.0
        loss_budget = (_safe_float(
            getattr(cfg, "cumulative_loss_budget_usd", 0.02))
            if cfg is not None else 0.02)
        loss_budget = max(loss_budget or 0.0, 0.0)
        remaining = max(loss_budget + min(realized, 0.0), 0.0)
        samples = dict(state.get("phase_samples") or {})
        phases = {phase: max(int(samples.get(phase, 0) or 0), 0)
                  for phase in ("open_first", "open_hedge",
                                "close_first", "close_hedge")}
        expected = _safe_float(state.get("last_expected_pnl_usd"))
        return {
            "configured": configured,
            "active": active,
            "execution_policy": "normal",
            "execution_overrides_active": False,
            "mode": mode,
            "disabled": bool(state.get("disabled", False)),
            "disabled_reason": state.get("disabled_reason"),
            "suspended": bool(state.get("suspended", False)),
            "suspended_reason": state.get("suspended_reason"),
            "suspended_ts": _safe_float(state.get("suspended_ts")),
            "resume_after_ts": _safe_float(state.get("resume_after_ts")),
            "resumed_ts": _safe_float(state.get("resumed_ts")),
            "target_complete_cycles": int(getattr(
                cfg, "target_complete_cycles", 5) if cfg is not None else 5),
            "max_complete_cycles": int(getattr(
                cfg, "max_complete_cycles", 10) if cfg is not None else 10),
            "complete_cycles": max(int(state.get("accepted_cycle_count", 0)
                                       or 0), 0),
            "min_execution_samples_per_phase": int(getattr(
                cfg, "min_execution_samples_per_phase", 3)
                if cfg is not None else 3),
            "samples_per_phase": phases,
            "realized_pnl_usd": realized,
            "exploration_pnl_usd": realized,
            "cumulative_loss_budget_usd": loss_budget,
            "loss_budget_remaining_usd": remaining,
            "accepted_count": max(int(state.get("accepted_count", 0) or 0), 0),
            "candidate_count": max(int(state.get("candidate_count", 0) or 0), 0),
            "green_count": max(int(state.get("green_count", 0) or 0), 0),
            "yellow_count": max(int(state.get("yellow_count", 0) or 0), 0),
            "red_count": max(int(state.get("red_count", 0) or 0), 0),
            "last_color": state.get("last_color"),
            "last_expected_pnl_usd": expected,
            "best_expected_pnl_usd": _safe_float(
                state.get("best_expected_pnl_usd")),
            "worst_accepted_expected_pnl_usd": _safe_float(
                state.get("worst_accepted_expected_pnl_usd")),
            "max_order_notional_usd": _safe_float(getattr(
                cfg, "max_order_notional_usd", 15.0) if cfg is not None else 15.0),
            "max_expected_loss_bps": _safe_float(getattr(
                cfg, "max_expected_loss_bps", 1.0) if cfg is not None else 1.0),
            "max_expected_loss_usd": _safe_float(getattr(
                cfg, "max_expected_loss_usd", 0.0025) if cfg is not None else 0.0025),
            "max_cycle_loss_usd": _safe_float(getattr(
                cfg, "max_cycle_loss_usd", 0.006) if cfg is not None else 0.006),
            "min_gross_after_fee_bps": _safe_float(getattr(
                cfg, "min_gross_after_fee_bps", -0.5)
                if cfg is not None else -0.5),
            "empirical_full_weight_samples": int(getattr(
                cfg, "empirical_full_weight_samples", 6)
                if cfg is not None else 5),
            "open_execution_room_min_bps": _safe_float(getattr(
                cfg, "open_execution_room_min_bps", 0.5)
                if cfg is not None else 0.5),
            "open_target_execution_room_bps": _safe_float(getattr(
                cfg, "open_target_execution_room_bps", 3.0)
                if cfg is not None else 3.0),
            "open_sampling_execution_cap_bps": _safe_float(getattr(
                cfg, "open_sampling_execution_cap_bps", 5.0)
                if cfg is not None else 5.0),
            "close_min_execution_room_bps": _safe_float(getattr(
                cfg, "close_min_execution_room_bps", 1.0)
                if cfg is not None else 1.0),
            "close_target_execution_room_bps": _safe_float(getattr(
                cfg, "close_target_execution_room_bps", 2.0)
                if cfg is not None else 2.0),
            "fast_exit_after_sec": _safe_float(getattr(
                cfg, "fast_exit_after_sec", 10.0) if cfg is not None else 10.0),
            "fast_exit_min_profit_usd": _safe_float(getattr(
                cfg, "fast_exit_min_profit_usd", -0.002)
                if cfg is not None else -0.002),
            "sampling_exit_after_sec": _safe_float(getattr(
                cfg, "sampling_exit_after_sec", 30.0)
                if cfg is not None else 30.0),
            "sampling_exit_floor_usd": _safe_float(getattr(
                cfg, "sampling_exit_floor_usd", -0.004)
                if cfg is not None else -0.004),
            "stale_exit_after_sec": _safe_float(getattr(
                cfg, "stale_exit_after_sec", 120.0)
                if cfg is not None else 120.0),
            "stale_exit_floor_usd": _safe_float(getattr(
                cfg, "stale_exit_floor_usd", -0.006)
                if cfg is not None else -0.006),
            "open_campaign_sec": _safe_float(getattr(
                cfg, "open_campaign_sec", 5.0) if cfg is not None else 5.0),
            "close_campaign_sec": _safe_float(getattr(
                cfg, "close_campaign_sec", 5.0) if cfg is not None else 5.0),
            "open_campaign_max_attempts": int(getattr(
                cfg, "open_campaign_max_attempts", 4) if cfg is not None else 4),
            "close_campaign_max_attempts": int(getattr(
                cfg, "close_campaign_max_attempts", 5) if cfg is not None else 5),
            "entropy_order_budget_per_min": int(getattr(
                cfg, "entropy_order_budget_per_min", 10)
                if cfg is not None else 10),
            "hedge_order_budget_per_min": int(getattr(
                cfg, "hedge_order_budget_per_min", 10)
                if cfg is not None else 10),
            "max_campaigns_per_signal_episode": int(getattr(
                cfg, "max_campaigns_per_signal_episode", 3)
                if cfg is not None else 3),
            "reopen_improvement_bps": _safe_float(getattr(
                cfg, "reopen_improvement_bps", 2.0)
                if cfg is not None else 2.0),
            "last_decision_ts": _safe_float(state.get("last_decision_ts")),
        }

    def _maybe_auto_disable_bootstrap(self, reason: Optional[str] = None) -> bool:
        cfg = self._bootstrap_cfg()
        state = getattr(self, "_bootstrap_state", {}) or {}
        if (cfg is None or not getattr(cfg, "enabled", False)
                or state.get("disabled")):
            return False

        # A critical recovery mismatch blocks only new exploratory opens for
        # this run. Once the exchanges are flat and fully reconciled, the
        # guarded resume path below can re-enable bootstrap automatically.
        if reason == "critical_recovery_mismatch":
            if (state.get("suspended")
                    and state.get("suspended_reason") == reason):
                return False
            now = time.time()
            state.update({
                "suspended": True,
                "suspended_reason": reason,
                "suspended_ts": now,
                "resume_after_ts": now + BOOTSTRAP_RECOVERY_GRACE_SEC,
                "disabled": False,
                "disabled_reason": None,
            })
            self._persist_bootstrap_state()
            self._apply_dynamic_model_warmup()
            log.warning("bootstrap mode suspended: %s", reason)
            return True

        if (not getattr(cfg, "auto_disable", True)
                and reason != "manual_disable"):
            return False
        if reason is None and max(int(state.get("accepted_cycle_count", 0) or 0), 0) >= int(
                getattr(cfg, "max_complete_cycles", 10)):
            reason = "max_complete_cycles"
        realized = _safe_float(state.get("realized_pnl_usd")) or 0.0
        loss_budget = float(getattr(cfg, "cumulative_loss_budget_usd", 0.02))
        if reason is None and realized <= -max(loss_budget, 0.0):
            reason = "cumulative_loss_budget"
        if reason is None:
            return False
        if reason not in BOOTSTRAP_PERMANENT_DISABLE_REASONS:
            return False
        state["disabled"] = True
        state["suspended"] = False
        state["suspended_reason"] = None
        state["disabled_reason"] = str(reason)
        state["disabled_ts"] = time.time()
        self._persist_bootstrap_state()
        # Re-apply the normal readiness parameters after the bounded sampling
        # policy ends. This is a parameter transition, not a history reset.
        self._apply_dynamic_model_warmup()
        log.warning("bootstrap mode auto-disabled: %s", reason)
        return True

    def _maybe_resume_bootstrap(self, now: Optional[float] = None) -> bool:
        """Resume exploratory opens only after a clean, stable recovery."""
        cfg = self._bootstrap_cfg()
        state = getattr(self, "_bootstrap_state", {}) or {}
        if (cfg is None or not getattr(cfg, "enabled", False)
                or state.get("disabled") or not state.get("suspended")):
            return False
        now = time.time() if now is None else float(now)
        resume_after = _safe_float(state.get("resume_after_ts"))
        if resume_after is not None and now + 1e-9 < resume_after:
            return False
        if not getattr(self, "startup_reconciled", False):
            return False
        if not self._positions_flat():
            return False
        if getattr(self, "reconciliation", {}).get("status") != "synced":
            return False
        if (getattr(self, "active_execution", None) is not None
                or getattr(self, "_restored_execution", None) is not None
                or getattr(self, "_pending_execution_record", None) is not None
                or bool(getattr(self, "_emergency_pending", {}))
                or getattr(self, "_orphaned_inventory", False)):
            return False
        margin = self._margin_snapshot()
        if margin.get("hard_risk") or margin.get("open_blocked"):
            return False

        state.update({
            "suspended": False,
            "suspended_reason": None,
            "resumed_ts": now,
            "disabled_reason": None,
        })
        self._persist_bootstrap_state()
        self._apply_dynamic_model_warmup()
        if getattr(self, "premium_model", None) is not None:
            self._model_restore_until = max(
                float(getattr(self, "_model_restore_until", 0.0)),
                now + MODEL_RESTORE_VALIDATION_SEC)
        log.info("bootstrap mode resumed after safe reconciliation")
        return True

    def _bootstrap_note_decision(self, trade: PlannedTrade) -> None:
        """Durably classify one accepted OPEN/ADD decision."""
        if (not self._bootstrap_active()
                or trade.candidate.action not in {
                    StrategyAction.OPEN, StrategyAction.ADD}):
            return
        budget = trade.cycle_budget or {}
        if budget.get("_bootstrap_decision_counted"):
            return
        state = self._bootstrap_state
        state["candidate_count"] = int(state.get("candidate_count", 0) or 0) + 1
        expected = _safe_float(budget.get("expected_cycle_pnl_usd",
                                            budget.get("net_profit_usd")))
        notional = max(_safe_float(trade.plan.buy_notional) or 0.0,
                       _safe_float(trade.plan.sell_notional) or 0.0)
        if expected is None or notional <= 0:
            return
        production_floor = _safe_float(
            budget.get("production_floor_usd"))
        if production_floor is None:
            production_floor = self._economic_floor_usd(notional)
        floor = _safe_float(budget.get("sampling_floor_usd"))
        if floor is None:
            floor = self._bootstrap_floor_usd(notional)
        color = ("GREEN" if expected >= production_floor else
                 "YELLOW" if expected >= floor else "RED")
        budget["_bootstrap_decision_counted"] = True
        budget["entry_mode"] = "bootstrap"
        budget["bootstrap_expected_pnl"] = expected
        budget["bootstrap_floor_usd"] = floor
        budget["production_floor_usd"] = production_floor
        budget["bootstrap_color"] = color
        state["accepted_count"] = int(state.get("accepted_count", 0) or 0) + 1
        state["last_color"] = color
        state["last_expected_pnl_usd"] = expected
        state["last_decision_ts"] = time.time()
        state[f"{color.lower()}_count"] = int(
            state.get(f"{color.lower()}_count", 0) or 0) + 1
        best = _safe_float(state.get("best_expected_pnl_usd"))
        worst = _safe_float(state.get("worst_accepted_expected_pnl_usd"))
        state["best_expected_pnl_usd"] = expected if best is None else max(best, expected)
        state["worst_accepted_expected_pnl_usd"] = (
            expected if worst is None else min(worst, expected))
        self._persist_bootstrap_state()

    def _bootstrap_note_phase_sample(self, observation) -> None:
        phase = str(getattr(observation, "phase", "") or "")
        if phase not in {"open_first", "open_hedge", "close_first", "close_hedge"}:
            return
        if float(getattr(observation, "filled_qty", 0.0) or 0.0) <= 0:
            return
        if getattr(observation, "incremental_cost_bps", None) is None:
            return
        metadata = getattr(observation, "metadata", {}) or {}
        if metadata.get("entry_mode") != "bootstrap":
            return
        samples = self._bootstrap_state.setdefault("phase_samples", {})
        samples[phase] = int(samples.get(phase, 0) or 0) + 1
        self._persist_bootstrap_state()
        self._maybe_auto_disable_bootstrap()

    def _bootstrap_note_completed_cycle(self, record: dict) -> None:
        if (record.get("entry_mode") != "bootstrap"
                or record.get("bootstrap_accounted")):
            return
        actual = _safe_float(record.get("total_cycle_pnl_usd"))
        if actual is None:
            return
        record["bootstrap_realized_pnl"] = actual
        record["bootstrap_accounted"] = True
        state = self._bootstrap_state
        state["accepted_cycle_count"] = int(
            state.get("accepted_cycle_count", 0) or 0) + 1
        state["realized_pnl_usd"] = (
            _safe_float(state.get("realized_pnl_usd")) or 0.0) + actual
        self._persist_bootstrap_state()
        cfg = self._bootstrap_cfg()
        cycle_loss_budget = (_safe_float(
            getattr(cfg, "max_cycle_loss_usd", None)) if cfg is not None
            else None)
        if (cycle_loss_budget is not None
                and actual <= -max(cycle_loss_budget, 0.0) + 1e-12):
            self._maybe_auto_disable_bootstrap("cycle_loss_budget")
        else:
            self._maybe_auto_disable_bootstrap()

    def _vlock(self, key: str) -> asyncio.Lock:
        lock = self._venue_locks.get(key)
        if lock is None:
            lock = self._venue_locks[key] = asyncio.Lock()
        return lock

    @staticmethod
    def _fill_fee_source(venue) -> str:
        """Return the fee evidence policy used for a venue fill."""
        return ("venue_zero_fee" if getattr(venue, "kind", "") == "lighter"
                else "estimated_config")

    def _action_budget(self, v, action: Optional[StrategyAction]) -> tuple[deque, int]:
        name = "open" if action in {StrategyAction.OPEN, StrategyAction.ADD} else "reduce"
        dq = self._action_sends.setdefault((v.key, name), deque())
        now = time.time()
        while dq and now - dq[0] >= 60.0:
            dq.popleft()
        limit = int(getattr(self.cfg.reduce_execution,
                            name + "_attempts_per_min", 8 if name == "open" else 10))
        return dq, max(limit, 1)

    def _venue_rate_ok(self, v, action: Optional[StrategyAction] = None) -> bool:
        """A venue send window and an independent per-action strategy budget."""
        return self._venue_budget_next_ts(v, action) is None

    def _venue_order_budget_limit(self, v,
                                  action: Optional[StrategyAction] = None) -> int:
        policy = self.cfg.reduce_execution
        hard_ceiling = max(1, int(v.orders_per_min))
        recovery = max(0, int(policy.recovery_reserve_per_min))
        if action in {StrategyAction.OPEN, StrategyAction.ADD}:
            # OPEN cannot consume the capacity reserved for a paired exit.
            reserve = max(recovery, int(policy.reserve_reduce_slots))
        elif action == StrategyAction.REDUCE:
            reserve = 1 if recovery else 0
        else:
            reserve = 0
        return max(hard_ceiling - reserve, 0)

    def _venue_budget_next_ts(self, v,
                              action: Optional[StrategyAction] = None) -> Optional[float]:
        dq = self._sends.setdefault(v.key, deque())
        now = time.time()
        while dq and now - dq[0] >= 60.0:
            dq.popleft()
        limit = self._venue_order_budget_limit(v, action)
        if limit <= 0:
            # Configuration cannot afford a complete cycle. Never invent an
            # extra exchange slot; report and recheck at a bounded cadence.
            return now + 60.0
        waiting = [float(dq[len(dq) - limit] + 60.0)] if len(dq) >= limit else []
        if action in {StrategyAction.OPEN, StrategyAction.ADD, StrategyAction.REDUCE}:
            action_sends, action_limit = self._action_budget(v, action)
            if len(action_sends) >= action_limit:
                waiting.append(float(action_sends[len(action_sends) - action_limit] + 60.0))
        return max(waiting) if waiting else None

    def _note_order_budget_block(self, venues: tuple, action: StrategyAction) -> None:
        now = time.time()
        next_values = [self._venue_budget_next_ts(v, action) for v in venues]
        next_values = [value for value in next_values if value is not None]
        if not next_values:
            return
        if self._order_budget_blocked_since is None:
            self._order_budget_blocked_since = now
        # Both legs must be available before a new cycle can be attempted.
        self._next_order_budget_available_ts = max(next_values)
        self._schedule_poke(max(self._next_order_budget_available_ts - now,
                                0.01))

    def _clear_order_budget_block(self) -> None:
        if self._order_budget_blocked_since is None:
            return
        self._order_budget_block_total_sec += max(
            time.time() - self._order_budget_blocked_since, 0.0)
        self._order_budget_blocked_since = None
        self._next_order_budget_available_ts = None

    def _record_open_blocker(self, reason: Optional[str],
                             now: Optional[float] = None) -> None:
        """Track one primary OPEN blocker without retaining scan history."""
        now = time.time() if now is None else float(now)
        if not math.isfinite(now):
            return
        telemetry = self._open_block_telemetry
        wanted = (str(reason) if reason else None)
        active = telemetry.get("active_reason")
        if active != wanted:
            active_since = _safe_float(telemetry.get("active_since_ts"))
            if active and active_since is not None:
                seconds = telemetry["seconds_by_reason"]
                seconds[active] = seconds.get(active, 0.0) + max(
                    now - active_since, 0.0)
            telemetry["active_reason"] = wanted
            telemetry["active_since_ts"] = now if wanted else None
        if wanted:
            counts = telemetry["count_by_reason"]
            counts[wanted] = counts.get(wanted, 0) + 1
            telemetry["last_reason"] = wanted
            telemetry["last_ts"] = now

    def _open_block_telemetry_snapshot(self,
                                        now: Optional[float] = None) -> dict:
        """Return bounded blocker counts and durations for the dashboard."""
        now = time.time() if now is None else float(now)
        telemetry = self._open_block_telemetry
        seconds = dict(telemetry.get("seconds_by_reason") or {})
        active = telemetry.get("active_reason")
        active_since = _safe_float(telemetry.get("active_since_ts"))
        if active and active_since is not None:
            seconds[active] = seconds.get(active, 0.0) + max(
                now - active_since, 0.0)
        return {
            "active_reason": active,
            "active_since_ts": active_since,
            "active_duration_sec": (
                max(now - active_since, 0.0)
                if active and active_since is not None else 0.0),
            "last_reason": telemetry.get("last_reason"),
            "last_ts": _safe_float(telemetry.get("last_ts")),
            "count_by_reason": dict(telemetry.get("count_by_reason") or {}),
            "seconds_by_reason": seconds,
            "total_blocked_sec": sum(seconds.values()),
        }

    @staticmethod
    def _open_policy_block_reason(values: dict,
                                  state: Optional[PairState] = None) \
            -> Optional[str]:
        """Return the first actionable reason a new OPEN is not eligible."""
        if state == PairState.UNBALANCED:
            return "unbalanced"
        if values.get("premium_data_degraded"):
            return "premium_data_degraded"
        if (values.get("dynamic_midline_tradable") is False
                and values.get("reference_valid") is False):
            return "dynamic_midline_warmup"
        if values.get("margin_open_blocked"):
            return "margin_risk"
        if (values.get("funding_available") is False
                and values.get("funding_on_unavailable") == "block_open"):
            return "funding_stale"
        if values.get("reference_valid") is False:
            return "reference_invalid"
        return None

    def _order_budget_block_sec(self, now: Optional[float] = None) -> float:
        now = time.time() if now is None else float(now)
        current = (max(now - self._order_budget_blocked_since, 0.0)
                   if self._order_budget_blocked_since is not None else 0.0)
        return self._order_budget_block_total_sec + current

    def _order_budget_snapshot(self, now: Optional[float] = None) -> dict:
        now = time.time() if now is None else float(now)
        venues = {}
        for venue in self.venues.values():
            dq = self._sends.setdefault(venue.key, deque())
            while dq and now - dq[0] > 60.0:
                dq.popleft()
            venues[venue.key] = {
                "used_last_60_sec": len(dq),
                "open_sends_last_60_sec": len(self._action_budget(venue, StrategyAction.OPEN)[0]),
                "reduce_sends_last_60_sec": len(self._action_budget(venue, StrategyAction.REDUCE)[0]),
                "configured_per_min": int(venue.orders_per_min),
                "venue_hard_ceiling": max(1, int(venue.orders_per_min)),
                "open_limit": self._venue_order_budget_limit(
                    venue, StrategyAction.OPEN),
                "reduce_limit": self._venue_order_budget_limit(
                    venue, StrategyAction.REDUCE),
                "open_next_available_ts": self._venue_budget_next_ts(
                    venue, StrategyAction.OPEN),
                "reduce_next_available_ts": self._venue_budget_next_ts(
                    venue, StrategyAction.REDUCE),
                "strategy_open_limit": max(1, int(getattr(
                    getattr(self.cfg, "reduce_execution", None),
                    "open_attempts_per_min", 8))),
                "strategy_reduce_limit": max(1, int(getattr(
                    getattr(self.cfg, "reduce_execution", None),
                    "reduce_attempts_per_min", 10))),
                "recovery_reserve_per_min": max(0, int(getattr(
                    getattr(self.cfg, "reduce_execution", None),
                    "recovery_reserve_per_min", 2))),
                # Kept for dashboard/API compatibility while the new fields
                # provide the unambiguous meaning.
                "reserved_reduce_slots": max(0, int(self.cfg.reduce_execution.reserve_reduce_slots)),
            }
        return {
            "block_sec": self._order_budget_block_sec(now),
            "next_available_ts": self._next_order_budget_available_ts,
            "venues": venues,
            "sqlite": self.ledger.write_stats(),
        }

    def _execution_persist_sec(self, action: StrategyAction) -> float:
        if not getattr(self.cfg, "action_specific_execution", False):
            return self.cfg.premium_persist_sec
        return (self.cfg.reduce_persist_sec
                if action == StrategyAction.REDUCE
                else self.cfg.open_persist_sec)

    def _execution_cooldown_sec(self, action: StrategyAction) -> float:
        if not getattr(self.cfg, "action_specific_execution", False):
            return self.cfg.cooldown_sec
        return (self.cfg.reduce_cooldown_sec
                if action == StrategyAction.REDUCE
                else self.cfg.open_cooldown_sec)

    def _first_leg_slippage_bps(self, action: StrategyAction) -> float:
        if not getattr(self.cfg, "action_specific_execution", False):
            return self.cfg.leg_slippage_bps
        return (self.cfg.reduce_first_leg_slippage_bps
                if action == StrategyAction.REDUCE
                else self.cfg.open_first_leg_slippage_bps)

    def _second_leg_slippage_bps(self, action: str | StrategyAction) -> float:
        if not getattr(self.cfg, "action_specific_execution", False):
            return self.cfg.second_leg_slippage_bps
        value = (action.value if isinstance(action, StrategyAction)
                 else str(action))
        return (self.cfg.reduce_second_leg_slippage_bps
                if value == StrategyAction.REDUCE.value
                else self.cfg.open_second_leg_slippage_bps)

    @staticmethod
    def _execution_cost_phase(action: str | StrategyAction, leg: str) -> str:
        value = action.value if isinstance(action, StrategyAction) else str(action)
        prefix = "close" if value == StrategyAction.REDUCE.value else "open"
        return f"{prefix}_{'first' if leg == 'first' else 'hedge'}"

    def _execution_percentile(self, phase: str) -> float:
        economics = getattr(self.cfg, "economic_execution", None)
        name = f"{phase}_cost_percentile"
        return min(max(float(getattr(economics, name, .75)), 0.01), 1.0)

    def _execution_cost_fallback_bps(self, phase: str) -> float:
        """Use a small conservative economic fallback, never a hard cap."""
        if phase == "open_first":
            value = self._first_leg_slippage_bps(StrategyAction.OPEN)
        elif phase == "open_hedge":
            value = self._second_leg_slippage_bps(StrategyAction.OPEN)
        elif phase == "close_first":
            value = self._first_leg_slippage_bps(StrategyAction.REDUCE)
        else:
            value = self._second_leg_slippage_bps(StrategyAction.REDUCE)
        # A physical bound is often 7–12 bps in this deployment. Charging all
        # of that as the expected cost would make every normal opportunity
        # look negative before the first real sample is available.
        return min(max(float(value), 0.0), 2.0)

    def _execution_cost_estimate(self, phase: str, *, venue=None,
                                 is_buy: Optional[bool] = None,
                                 qty: float = 0.0,
                                 latency_adverse_bps: Optional[float] = None,
                                 now: Optional[float] = None) -> dict:
        """Return a live-prior/blended incremental execution-cost estimate.

        The walk-depth quote is already in the projected cash flow.  Only the
        expected incremental fill-vs-depth-average cost belongs in economics;
        hard caps and physical stress reserves remain separate protections.
        """
        phase = str(phase)
        first_leg = phase.endswith("_first")
        # Bootstrap is not an execution model.  Use the same empirical
        # weighting and conservative residuals for sampled and normal cycles.
        residual = 0.35 if first_leg else 0.50
        quote = None
        if venue is not None and is_buy is not None and qty > 0:
            quote = self._quote_snapshot(venue, bool(is_buy), float(qty))
        top = _safe_float(quote.get("top_px")) if quote else None
        depth_avg = (_safe_float(quote.get("depth_avg_px"))
                     if quote else None)
        marginal = _safe_float(quote.get("marginal_px")) if quote else None
        depth = 0.0
        if top is not None and depth_avg is not None and top > 0:
            depth = ((depth_avg / top - 1.0) * 1e4 if is_buy
                     else (1.0 - depth_avg / top) * 1e4)
            depth = max(depth, 0.0) if math.isfinite(depth) else 0.0
        if latency_adverse_bps is None:
            current = time.time() if now is None else float(now)
            if first_leg:
                latency_adverse_bps = (
                    self._open_adverse_move_economic_p75(bool(is_buy), current))
            else:
                latency_adverse_bps = self._hedge_adverse_move_economic_p75(
                    bool(is_buy), current)
        adverse = max(_safe_float(latency_adverse_bps) or 0.0, 0.0)
        percentile = self._execution_percentile(phase)
        return self.execution_cost_model.estimate(
            phase, percentile=percentile,
            prior_bps=None,
            live_depth_bps=depth, latency_adverse_bps=adverse,
            small_prior_bps=residual,
            now=time.time() if now is None else float(now),
            is_buy=is_buy, qty=qty)

    def _execution_hard_cap_bps(self, action: str | StrategyAction,
                                leg: str) -> float:
        policy = getattr(self.cfg, "reduce_execution", None)
        value = action.value if isinstance(action, StrategyAction) else str(action)
        if value != StrategyAction.REDUCE.value and leg == "first":
            return max(float(getattr(policy, "open_first_leg_hard_cap_bps", 10.0)), 0.0)
        if value != StrategyAction.REDUCE.value and leg == "hedge":
            return max(float(getattr(policy, "open_second_leg_hard_cap_bps", 12.0)), 0.0)
        return max(self._first_leg_slippage_bps(StrategyAction.REDUCE)
                   if leg == "first"
                   else self._second_leg_slippage_bps(StrategyAction.REDUCE), 0.0)

    @staticmethod
    def _quote_snapshot(venue, is_buy: bool, qty: float) -> Optional[dict]:
        levels = (venue.book.sorted_asks() if is_buy
                  else venue.book.sorted_bids())
        if not levels or qty <= 0:
            return None
        top = _safe_float(levels[0][0])
        if top is None or top <= 0:
            return None
        total = sum(max(float(size), 0.0) for _, size in levels)
        marginal = top
        depth_avg = None
        notional = None
        if total + 1e-12 >= qty:
            marginal, notional = walk_depth(levels, qty)
            marginal = _safe_float(marginal) or top
            if notional is not None and qty > 0:
                depth_avg = _safe_float(notional / qty)
        return {"ts": time.time(), "top_px": top,
                "depth_avg_px": depth_avg,
                "marginal_px": marginal,
                "qty": float(qty), "notional": notional,
                "quote_source": "pre_send_book"}

    def _execution_adverse_move_bps(self, venue, is_buy: bool,
                                    quoted_top_px: Optional[float],
                                    submit_ts: float,
                                    terminal_ts: float) -> float:
        if quoted_top_px is None or quoted_top_px <= 0:
            return 0.0
        history = (self._open_quote_history if venue is self.entropy
                   else self._hedge_quote_history)
        rows = [row for row in history
                if submit_ts <= float(row.get("ts", 0.0)) <= terminal_ts]
        if rows:
            key = "ask" if is_buy else "bid"
            current = _safe_float(rows[-1].get(key))
        else:
            current = (_safe_float(venue.book.best_ask()) if is_buy
                       else _safe_float(venue.book.best_bid()))
        if current is None or current <= 0:
            return 0.0
        move = ((current / quoted_top_px - 1.0) * 1e4 if is_buy
                else (quoted_top_px / current - 1.0) * 1e4)
        return max(move, 0.0) if math.isfinite(move) else 0.0

    def _record_execution_cost(self, *, action: str | StrategyAction,
                               leg: str, venue, is_buy: bool, qty: float,
                               quote: Optional[dict], limit_px: Optional[float],
                               info: dict, submit_ts: float,
                               terminal_ts: Optional[float] = None) -> None:
        if quote is None:
            return
        terminal = time.time() if terminal_ts is None else float(terminal_ts)
        latency = max(terminal - float(quote.get("ts", submit_ts)), 0.0)
        phase = self._execution_cost_phase(action, leg)
        budget = self._recovery_context.get("cycle_budget") or {}
        entry_mode = (budget.get("entry_mode")
                      or self._cycle_telemetry.get("entry_mode"))
        observation = self.execution_cost_model.observe(
            phase=phase,
            execution_id=(self.active_execution.cycle_id
                          if self.active_execution is not None else ""),
            requested_qty=qty,
            filled_qty=self._fill_amount(info),
            quoted_top_px=quote.get("top_px"),
            quoted_depth_avg_px=quote.get("depth_avg_px"),
            quoted_marginal_px=quote.get("marginal_px"),
            limit_px=limit_px,
            avg_fill_px=info.get("avg_px"),
            latency_sec=latency,
            adverse_move_bps=self._execution_adverse_move_bps(
                venue, is_buy, _safe_float(quote.get("top_px")),
                submit_ts, terminal),
            outcome=str(info.get("status") or
                        ("error" if info.get("err") else "unknown")),
            is_buy=is_buy,
            metadata={"action": (action.value if isinstance(action, StrategyAction)
                                  else str(action)),
                      "leg": leg, "venue": getattr(venue, "key", ""),
                      "quote_source": quote.get("quote_source",
                                                  "pre_send_book"),
                      "entry_mode": entry_mode or "normal"},
            ts=terminal,
        )
        if entry_mode == "bootstrap":
            self._bootstrap_note_phase_sample(observation)

    def _record_interleg_cost(self, *, action: str | StrategyAction,
                              is_buy: bool, qty: float,
                              initial_quote: Optional[dict],
                              hedge_quote: Optional[dict],
                              started_ts: float, observed_ts: float) -> None:
        """Measure hedge-book drift while the first-leg result is pending."""
        if initial_quote is None or hedge_quote is None or qty <= 0:
            return
        value = action.value if isinstance(action, StrategyAction) else str(action)
        phase = ("close_interleg" if value == StrategyAction.REDUCE.value
                 else "open_interleg")
        current = (_safe_float(hedge_quote.get("depth_avg_px"))
                   or _safe_float(hedge_quote.get("top_px")))
        if current is None:
            return
        self.execution_cost_model.observe(
            phase=phase,
            execution_id=(self.active_execution.cycle_id
                          if self.active_execution is not None else ""),
            requested_qty=qty, filled_qty=qty, is_buy=is_buy,
            quoted_top_px=initial_quote.get("top_px"),
            quoted_depth_avg_px=initial_quote.get("depth_avg_px"),
            quoted_marginal_px=initial_quote.get("marginal_px"),
            avg_fill_px=current,
            latency_sec=max(observed_ts - started_ts, 0.0),
            outcome="observed",
            metadata={"action": value, "leg": "interleg",
                      "venue": getattr(self.hedge, "key", ""),
                      "quote_source": "first_submit_to_hedge_submit"},
            ts=observed_ts,
        )

    @staticmethod
    def _fill_probability_bucket(room_bps: Optional[float]) -> str:
        value = _safe_float(room_bps)
        value = max(value or 0.0, 0.0)
        if value < 2.0:
            return "0-2"
        if value < 4.0:
            return "2-4"
        if value < 6.0:
            return "4-6"
        if value < 8.0:
            return "6-8"
        if value < 10.0:
            return "8-10"
        return "10+"

    def _note_open_fill_probability(self, room_bps: Optional[float],
                                    filled: bool) -> None:
        """Record bounded OPEN first-leg fill telemetry, never an admission gate."""
        bucket = self._fill_probability_bucket(room_bps)
        row = self._open_fill_probability.setdefault(
            bucket, {"attempts": 0, "fills": 0})
        row["attempts"] = int(row.get("attempts") or 0) + 1
        if filled:
            row["fills"] = int(row.get("fills") or 0) + 1

    def _note_close_fill_probability(self, room_bps: Optional[float],
                                     filled: bool) -> None:
        """Record REDUCE first-leg fill telemetry for the close optimizer."""
        bucket = fill_probability_bucket(float(room_bps or 0.0))
        row = self._close_fill_probability.setdefault(
            bucket, {"attempts": 0, "fills": 0})
        row["attempts"] = int(row.get("attempts") or 0) + 1
        if filled:
            row["fills"] = int(row.get("fills") or 0) + 1

    def _open_fill_probability_snapshot(self) -> dict:
        result = {}
        for bucket in ("0-2", "2-4", "4-6", "6-8", "8-10", "10+"):
            row = self._open_fill_probability.get(bucket) or {}
            attempts = int(row.get("attempts") or 0)
            fills = int(row.get("fills") or 0)
            result[bucket] = {
                "attempts": attempts,
                "fills": fills,
                "fill_rate": fills / attempts if attempts else None,
            }
        return result

    def _close_fill_probability_snapshot(self) -> dict:
        result = {}
        for bucket in ("0-2", "2-4", "4-6", "6-8", "8-10", "10+"):
            row = self._close_fill_probability.get(bucket) or {}
            attempts = int(row.get("attempts") or 0)
            fills = int(row.get("fills") or 0)
            result[bucket] = {
                "attempts": attempts,
                "fills": fills,
                "fill_rate": fills / attempts if attempts else None,
            }
        return result

    def _cycle_policy_key(self) -> str:
        """Scope learned outcomes to the actual market and economic policy."""
        exit_policy = asdict(self.cfg.cycle_exit)
        exit_policy.pop("state_json", None)  # storage location is not strategy policy
        payload = {
            "version": "cycle-review-v3", "strategy": self.strategy_name,
            "venues": [(v.key, conf.symbol, conf.kind, v.fee_bps, v.cap_usd,
                        v.orders_per_min, v.min_base, v.min_quote, v.size_decimals)
                       for v, conf in ((self.entropy, self.cfg.entropy),
                                       (self.hedge, self.cfg.hedge))],
            "signal": {name: value for name, value in vars(self.cfg).items()
                       if name.startswith(("dynamic_", "funding_"))
                       and isinstance(value, (int, float, bool, str))},
            "thresholds": [self.cfg.midline_bps, self.cfg.upper_bps,
                           self.cfg.lower_bps, self.cfg.exit_bps,
                           self.cfg.allow_sell_entropy_open,
                           self.cfg.allow_buy_entropy_open],
            "adaptive_bands": asdict(self.cfg.adaptive_bands),
            "premium_model": asdict(self.cfg.premium_model),
            "reference": asdict(self.cfg.midline_lag_guard),
            "exit": exit_policy,
            "execution": asdict(self.cfg.reduce_execution),
            "economics": asdict(self.cfg.economic_execution),
            "profit_guard": asdict(self.cfg.profit_guard),
            "bootstrap": asdict(self.cfg.bootstrap),
            "sizing": [self.cfg.max_order_notional, self.cfg.min_order_notional,
                       self.cfg.take_fraction, self.cfg.net_tolerance_base],
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:20]

    def _cycle_learning(self, direction: Optional[str] = None) -> dict:
        now = time.time()
        key = (self._cycle_policy_key(), direction)
        cache = getattr(self, "_cycle_learning_cache", {})
        previous = cache.get(key)
        if previous and now - previous[0] < 2.0:
            return previous[1]
        kwargs = dict(limit=200, now=now, since=now - 86400.0,
                      direction=direction, policy_key=key[0])
        data = {"duration": self.ledger.cycle_duration_stats(**kwargs),
                "outcomes": self.ledger.recent_cycle_outcomes(**kwargs)}
        self._cycle_learning_cache = {k: v for k, v in cache.items() if k[0] == key[0]}
        self._cycle_learning_cache[key] = (now, data)
        return data

    def _expected_cycle_hold_sec(self, direction: Optional[str] = None) -> float:
        """Policy-scoped duration, with an exit-policy prior and live censoring."""
        policy = self.cfg.cycle_exit
        fallback = (max(float(policy.fast_exit_after_sec), 1.0)
                    if policy.enabled and policy.mode == "fill_pnl" else
                    max(float(self.cfg.funding_expected_hold_hours) * 3600.0, 1.0))
        stats = self._cycle_learning(direction)["duration"]
        observed = _safe_float(stats.get("p90_sec"))
        weight = min(float(stats.get("count", 0)) / 5.0, 1.0)
        estimate = fallback if observed is None else observed * weight + fallback * (1 - weight)
        # An unfinished cycle is censored, not a short successful cycle.
        return max(estimate, float(stats.get("max_active_age_sec") or 0.0), 1.0)

    def _calibrate_cycle_projection(self, projected: float, notional: float,
                                    direction: str, reference_state: Optional[str] = None) -> dict:
        rows = self._cycle_learning(direction)["outcomes"].get("outcomes", [])
        pairs = []
        risks = 0
        for row in rows:
            if reference_state is not None and row.get("entry_reference_state") != reference_state:
                continue
            actual = _safe_float(row.get("net_pnl_usd"))
            predicted = _safe_float(row.get("entry_projected_cycle_pnl_usd"))
            size = _safe_float(row.get("notional_usd"))
            if actual is None or predicted is None or not size or size <= 0:
                continue
            pairs.append((predicted / size * 1e4, actual / size * 1e4))
            risks += bool(row.get("recovery_count") or row.get("exit_mode") in
                          {"forced", "risk", "regime_stop", "hard_timeout", "margin_risk",
                           "emergency", "execution_emergency", "manual_flatten"})
        count = len(pairs)
        projected_bps = projected / notional * 1e4
        slope = None
        mean_actual = None
        confidence_margin = 0.0
        if count >= 10:
            mean_predicted = sum(x for x, _ in pairs) / count
            mean_actual = sum(y for _, y in pairs) / count
            predicted_ss = sum((x - mean_predicted) ** 2 for x, _ in pairs)
            covariance = sum((x - mean_predicted) * (y - mean_actual)
                             for x, y in pairs)
            # A negative or absent relationship has no ranking value.  Do not
            # extrapolate it into an apparently profitable entry forecast.
            slope = min(max(covariance / predicted_ss, 0.0), 1.0) \
                if predicted_ss > 1e-12 else 0.0
            fitted = mean_actual + slope * (projected_bps - mean_predicted)
            residuals = [y - (mean_actual + slope * (x - mean_predicted))
                         for x, y in pairs]
            residual_variance = (sum(value * value for value in residuals)
                                 / max(count - 2, 1))
            # One-sided 95% confidence bound for the expected outcome.  The
            # model may become more conservative, never more optimistic than
            # the independently costed projection.
            confidence_margin = 1.645 * math.sqrt(residual_variance / count)
            calibrated_bps = min(projected_bps,
                                 fitted - confidence_margin)
            correction = calibrated_bps - projected_bps
        else:
            errors = [actual - predicted for predicted, actual in pairs]
            mean_error = sum(errors) / count if count else 0.0
            variance = (sum((value - mean_error) ** 2 for value in errors)
                        / (count - 1) if count > 1 else 0.0)
            weight = min(count / 10.0, 1.0)
            correction = weight * (
                min(mean_error, 0.0) - math.sqrt(variance / count)
                if count else 0.0)
        return {"projected_cycle_pnl_usd": projected,
                "calibrated_cycle_pnl_usd": projected + notional * correction / 1e4,
                "cycle_model_source": "calibrated_projection" if count >= 10 else "projection_prior",
                "cycle_model_reference_state": reference_state,
                "cycle_model_samples": count, "cycle_model_empirical_ready": count >= 10,
                "cycle_model_correction_bps": correction,
                "cycle_model_slope": slope,
                "cycle_model_mean_actual_bps": mean_actual,
                "cycle_model_confidence_margin_bps": confidence_margin,
                "cycle_model_risk_exit_rate": risks / count if count else None,
                "cycle_policy_key": self._cycle_policy_key()}

    def _no_fill_retry_sec(self, action: StrategyAction) -> float:
        if not getattr(self.cfg, "action_specific_execution", False):
            return self.cfg.no_fill_retry_sec
        return (self.cfg.reduce_no_fill_retry_sec
                if action == StrategyAction.REDUCE
                else self.cfg.open_no_fill_retry_sec)

    def _reduce_first_leg_slippage(self, attempt: int) -> float:
        if not getattr(self.cfg, "action_specific_execution", False):
            return self.cfg.leg_slippage_bps
        schedule = self.cfg.reduce_retry_escalation_bps
        if schedule:
            return schedule[min(max(attempt, 0), len(schedule) - 1)]
        return self.cfg.reduce_first_leg_slippage_bps

    def _venue_limited(self, v) -> bool:
        return time.time() < self._venue_limited_until.get(v.key, 0.0)

    def _mark_limited(self, v) -> None:
        self._venue_limited_until[v.key] = time.time() + self.cfg.rate_limit_pause_sec
        log.warning("[%s] rate limited — trading paused for %.0fs",
                    v.name, self.cfg.rate_limit_pause_sec)

    def _record_send(self, v, action: Optional[StrategyAction | str] = None) -> None:
        now = time.time()
        self._sends.setdefault(v.key, deque()).append(now)
        name = action.value if isinstance(action, StrategyAction) else action
        if name in {"open", "add", "reduce"}:
            name = "open" if name == "add" else name
            self._action_sends.setdefault((v.key, name), deque()).append(now)

    def request_stop(self) -> None:
        self.stop.set()
        self._update_evt.set()
        self._reconcile_evt.set()

    @staticmethod
    def _config_signature(path: str):
        """Return a cheap signature that also notices atomic file replaces."""
        try:
            stat = os.stat(path)
        except OSError:
            return None
        return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)

    def _apply_hot_reload(self, updated: Config,
                          changed: tuple[str, ...],
                          ignored: tuple[str, ...]) -> None:
        """Apply a validated runtime config without touching live venues."""
        model_warmup_sec, model_min_samples = self._dynamic_model_warmup(updated)
        self.premium_model.reconfigure(
            short_window_sec=updated.dynamic_short_window_sec,
            long_window_sec=updated.dynamic_long_window_sec,
            warmup_sec=model_warmup_sec,
            min_samples=model_min_samples,
            shift_min_bps=updated.dynamic_shift_min_bps,
            shift_sigma_mult=updated.dynamic_shift_sigma_mult,
            recovery_ratio=updated.dynamic_recovery_ratio,
            recovery_sec=updated.dynamic_recovery_sec,
            seed_midline_bps=updated.midline_bps,
            base_move_bps_per_10m=(
                updated.dynamic_base_move_bps_per_10m),
            accelerated_move_bps_per_10m=(
                updated.dynamic_accelerated_move_bps_per_10m),
            absolute_max_move_bps_per_10m=(
                updated.dynamic_absolute_max_move_bps_per_10m),
            sigma_window_sec=updated.adaptive_bands.sigma_window_sec,
            midline_lag_guard_enabled=updated.midline_lag_guard.enabled,
            midline_lag_warn_bps=updated.midline_lag_guard.warn_bps,
            midline_lag_block_bps=updated.midline_lag_guard.block_bps,
            midline_lag_persist_sec=updated.midline_lag_guard.persist_sec,
            midline_lag_accelerate_bps=(
                updated.midline_lag_guard.accelerate_bps),
            midline_lag_accelerate_persist_sec=(
                updated.midline_lag_guard.accelerate_persist_sec),
            midline_lag_recovery_sec=updated.midline_lag_guard.recovery_sec,
            midline_lag_recovery_bps=updated.midline_lag_guard.recovery_bps,
        )
        self.premium_health.degraded_after_sec = (
            updated.premium_model.degraded_after_sec)
        if self.seconds_recorder is not None:
            self.seconds_recorder.max_book_age_ms = (
                updated.premium_model.max_book_age_ms)
            self.seconds_recorder.max_cross_venue_skew_ms = (
                updated.premium_model.max_cross_venue_skew_ms)
        self.cfg = updated
        self._model_warmup_sec = model_warmup_sec
        self._model_min_samples = model_min_samples
        if updated.dynamic_midline_enabled:
            # Enabling the model or changing its effective warmup can reuse
            # valid durable samples; no synthetic observations are created.
            self._backfill_premium_model(time.time())
        self.model_snapshot = self.premium_model.snapshot(time.time())
        self.last_config_reload_ts = time.time()
        self.last_config_reload_error = None
        if ignored:
            self.config_restart_required = True
            self.config_restart_required_paths = sorted(set(
                self.config_restart_required_paths).union(ignored))
        log.warning("[CONFIG] hot reload applied: %s%s",
                    ", ".join(changed),
                    ("; restart required for: " + ", ".join(ignored)
                     if ignored else ""))
        # A changed threshold must wake a strategy that was waiting for a
        # market update; an in-flight PlannedTrade remains unchanged.
        self._update_evt.set()

    async def _config_reload_loop(self) -> None:
        """Watch the YAML file and apply validated strategy changes."""
        path = self.config_file
        if not path:
            return
        last_signature = self._config_signature(path)
        last_failed_signature = None
        self._last_config_file_signature = last_signature
        log.info("[CONFIG] hot reload watching %s every %.1fs",
                 path, self.cfg.hot_reload.poll_sec)
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(
                    self.stop.wait(), timeout=self.cfg.hot_reload.poll_sec)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            signature = self._config_signature(path)
            if signature == last_signature:
                continue
            # An editor can expose a partially-written file briefly.  Avoid
            # retry/log spam for the same bad write, but retry on the next
            # atomic save.
            if signature == last_failed_signature:
                continue
            if signature is None:
                self.last_config_reload_error = "config file is missing"
                last_failed_signature = signature
                log.error("[CONFIG] hot reload rejected: %s is missing",
                          path)
                continue
            try:
                candidate = load_config(
                    path, self.env_file, symbol=self.cfg.symbol,
                    hedge_venue=self.cfg.hedge_venue)
                updated, changed, ignored = merge_hot_reloadable_config(
                    self.cfg, candidate)
            except Exception as exc:
                self.last_config_reload_error = str(exc)[:500]
                last_failed_signature = signature
                log.error("[CONFIG] hot reload rejected for %s: %s",
                          path, exc)
                continue

            last_signature = signature
            last_failed_signature = None
            self._last_config_file_signature = signature
            if changed or ignored:
                self._apply_hot_reload(updated, changed, ignored)
            else:
                log.info("[CONFIG] file changed; no runtime settings changed")

    # ------------------------------------------------------------- lifecycle

    async def _close_venues(self) -> None:
        """Close every venue exactly once, including failed startup paths."""
        if self._venues_closed:
            return
        self._venues_closed = True
        for venue in tuple(self.venues.values()):
            try:
                await venue.close()
            except Exception:
                # Cleanup must not hide the original startup/order exception.
                log.warning("venue close failed during shutdown", exc_info=True)

    async def run(self) -> None:
        # Long keepalive so order-path connections survive quiet spells; the
        # keepalive loop pings inside this window to hold them open.
        # The host currently has a broken IPv6 route to Hyperliquid.  Pin the
        # shared REST connector to IPv4 so startup/account reconciliation does
        # not fail before the strategy can begin.
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            family=socket.AF_INET, keepalive_timeout=75.0, ttl_dns_cache=300))
        try:
            await self._run_inner()
        finally:
            await self._close_venues()
            if self.session is not None and not self.session.closed:
                await self.session.close()
            self.ledger.close()

    def _make_venue(self, vc):
        if vc.kind == "lighter":
            return LighterVenue(vc, self.session, self.cfg.settle_timeout_sec)
        return HLVenue(vc, self.cfg.hl_api_url, self.cfg.hl_ws_url,
                       self.session, self.cfg.settle_timeout_sec)

    async def _wait_for_market_data(self) -> None:
        """Wait briefly for both books and the authenticated order stream."""
        deadline = time.time() + max(5.0, min(30.0,
                                               self.cfg.settle_timeout_sec + 5.0))
        while not self.stop.is_set() and time.time() < deadline:
            books_ready = all(self._book_has_top(v)
                              for v in self.venues.values())
            order_paths_ready = all(v.ready_to_trade()
                                    for v in self.venues.values())
            if books_ready and order_paths_ready:
                return
            await asyncio.sleep(0.1)
        if not self.stop.is_set():
            log.warning("startup recovery waited for market data but timed out; "
                        "strategy remains gated until books are ready")

    def _checkpoint_execution(self, state: ExecutionState,
                              event_type: str = "checkpoint",
                              payload: Optional[dict] = None) -> None:
        """Persist execution progress and fail closed if the ledger is lost."""
        try:
            self.ledger.checkpoint_execution(
                state, context=self._recovery_context,
                event_type=event_type, payload=payload)
        except Exception as exc:
            # Continuing without a durable recovery point can create a second
            # first leg after a crash. Keep the current state in memory for
            # emergency recovery, but stop new strategy entries.
            self.halted = True
            log.critical("execution ledger checkpoint failed: %r — "
                         "new trading halted", exc)

    def _execution_context(self, trade: Optional[PlannedTrade] = None) -> dict:
        if trade is None:
            return dict(self._recovery_context)
        candidate, plan = trade.candidate, trade.plan
        return {
            "buy_venue": candidate.buy.key,
            "sell_venue": candidate.sell.key,
            "buy_notional": plan.buy_notional,
            "sell_notional": plan.sell_notional,
            "buy_limit": plan.buy_limit,
            "sell_limit": plan.sell_limit,
            "exp_edge_usd": plan.exp_edge_usd,
            "gross_edge_usd": plan.gross_edge_usd,
            "marginal_premium_bps": plan.marginal_premium_bps,
            "threshold_bps": candidate.threshold_bps,
            "net_edge_bps": trade.net_marginal_edge_bps,
            "edge_surplus_bps": trade.edge_surplus_bps,
            "per_leg_slippage_bps": trade.per_leg_slippage_bps,
            "cycle_budget": dict(trade.cycle_budget),
            "entry_mode": trade.cycle_budget.get("entry_mode", "normal"),
            "bootstrap_expected_pnl": trade.cycle_budget.get(
                "bootstrap_expected_pnl"),
            "bootstrap_floor_usd": trade.cycle_budget.get(
                "bootstrap_floor_usd"),
            "statistical_threshold_bps": trade.cycle_budget.get(
                "statistical_threshold_bps", candidate.threshold_bps),
            "economic_required_dislocation_bps": trade.cycle_budget.get(
                "economic_required_dislocation_bps"),
            "effective_entry_distance_bps": trade.cycle_budget.get(
                "effective_entry_distance_bps"),
            "first_leg_execution_room_bps": trade.cycle_budget.get(
                "first_leg_execution_room_bps"),
            "latency_required_room_bps": trade.cycle_budget.get(
                "latency_required_room_bps"),
            "actual_entropy_limit_px": trade.cycle_budget.get(
                "actual_entropy_limit_px"),
            "pair_state_before": candidate.pair_state_before.value,
            "effective_entry_band_bps": candidate.effective_entry_band_bps,
            "cycle_entry_midline_bps": candidate.cycle_entry_midline_bps,
            "cycle_entry_reference_bps": candidate.cycle_entry_midline_bps,
            "cycle_entry_reference_mode": candidate.cycle_entry_reference_mode,
            "cycle_entry_active_midline_bps": (
                candidate.cycle_entry_active_midline_bps),
            "cycle_entry_dynamic_center_bps": (
                candidate.cycle_entry_dynamic_center_bps),
            "cycle_entry_sigma_bps": candidate.cycle_entry_sigma_bps,
            "reference_state": candidate.reference_state,
            "reference_confidence": candidate.reference_confidence,
            "required_deviation_bps": candidate.required_deviation_bps,
            "signal_deviation_bps": candidate.signal_deviation_bps,
            "extreme_dislocation": candidate.extreme_dislocation,
            "cycle_midline_drift_bps": candidate.cycle_midline_drift_bps,
            "volatility_guard": candidate.volatility_guard,
            "cycle_drift_guard": candidate.cycle_drift_guard,
            "inventory_cycle_id": self.cycle.cycle_id,
            "reduce_only": candidate.reduce_only,
        }

    def _ledger_reconcile_target(self, venue, exchange_qty: float) -> float:
        """Return exchange position; it is never a historical fill."""
        return float(exchange_qty)

    def _set_recovery_status(self, state: ExecutionState,
                             status: RecoveryStatus | str,
                             reason: Optional[str] = None) -> None:
        try:
            wanted = (status if isinstance(status, RecoveryStatus)
                      else RecoveryStatus(str(status)))
        except ValueError:
            wanted = RecoveryStatus.RECOVERING_FILL
        current = getattr(state, "recovery_status", RecoveryStatus.NORMAL)
        try:
            current = (current if isinstance(current, RecoveryStatus)
                       else RecoveryStatus(str(current)))
        except ValueError:
            current = RecoveryStatus.RECOVERING_FILL
        if (current == wanted and
                (reason is None or getattr(state, "recovery_reason", None) == reason)):
            return
        state.set_recovery(wanted, reason)
        self._checkpoint_execution(
            state, event_type="recovery_status",
            payload={"recovery_status": wanted.value,
                     "reason": reason or state.recovery_reason})

    def _set_hedge_completion_state(self, state: ExecutionState,
                                    wanted: str,
                                    reason: Optional[str] = None) -> None:
        """Persist the safety state of the second-leg completion path.

        Order evidence and position evidence are intentionally not collapsed
        into one boolean.  In particular, a position-covered execution may
        safely suppress a duplicate hedge while its fill price/fee is still
        being recovered from the exchange.
        """
        allowed = {
            HEDGE_COMPLETION_NOT_STARTED, HEDGE_COMPLETION_PENDING,
            HEDGE_ORDER_UNKNOWN, POSITION_COVERED_UNANCHORED,
            POSITION_COVERED_CONFIRMED, EXECUTION_SAFETY_FLAT,
        }
        wanted = str(wanted)
        if wanted not in allowed:
            wanted = HEDGE_ORDER_UNKNOWN
        current = str(getattr(
            state, "hedge_completion_state", HEDGE_COMPLETION_NOT_STARTED))
        if current == wanted:
            return
        state.hedge_completion_state = wanted
        state.touch()
        self._checkpoint_execution(
            state, event_type="hedge_completion_state",
            payload={
                "hedge_completion_state": wanted,
                "reason": reason,
                "pending_leg": state.pending_leg,
                "pending_order_ref": state.pending_order_ref,
                "first_filled": state.first_filled,
                "hedge_filled": state.hedge_filled,
                "remaining_hedge_qty": state.remaining_hedge,
            })
        log.info(
            "[HEDGE COMPLETION] execution_id=%s state=%s reason=%s "
            "target=%.8g local_hedge=%.8g remaining=%.8g pending=%s",
            state.cycle_id, wanted, reason or "none", state.first_filled,
            state.hedge_filled, state.remaining_hedge,
            state.pending_order_ref or "none")

    def _update_hedge_completion_evidence(
            self, state: ExecutionState, *, order_info: Optional[dict] = None,
            reason: Optional[str] = None) -> str:
        """Recompute hedge safety from current order and position evidence."""
        has_execution = bool(
            state.first_filled > 0 or state.hedge_filled > 0
            or state.pending_leg or state.pending_order_ref)
        if not has_execution:
            self._set_hedge_completion_state(
                state, HEDGE_COMPLETION_NOT_STARTED, reason)
            return HEDGE_COMPLETION_NOT_STARTED

        if self._positions_flat(self._execution_flat_tolerance(state)):
            self._set_hedge_completion_state(
                state, EXECUTION_SAFETY_FLAT, reason or "positions_flat")
            return EXECUTION_SAFETY_FLAT

        if self._exchange_position_covers_hedge(state):
            self._sync_execution_quantities_from_evidence(state)
            accounting_ready = (
                self._execution_cost_ready(state)
                and self._execution_fill_coverage_ready(state))
            wanted = (POSITION_COVERED_CONFIRMED if accounting_ready
                      else POSITION_COVERED_UNANCHORED)
            self._set_hedge_completion_state(
                state, wanted,
                reason or "exchange_positions_cover_hedge")
            return wanted

        unresolved = bool((order_info or {}).get("unresolved"))
        if state.pending_leg == "hedge":
            wanted = (HEDGE_ORDER_UNKNOWN if unresolved
                      else HEDGE_COMPLETION_PENDING)
            self._set_hedge_completion_state(
                state, wanted, reason or "hedge_order_pending")
            return wanted

        if state.pending_leg == "first":
            wanted = (HEDGE_COMPLETION_PENDING if state.first_filled > 0
                      else HEDGE_COMPLETION_NOT_STARTED)
            self._set_hedge_completion_state(
                state, wanted, reason or "first_order_pending")
            return wanted

        if state.first_filled > 0 and state.remaining_hedge > 1e-12:
            self._set_hedge_completion_state(
                state, HEDGE_COMPLETION_PENDING,
                reason or "hedge_remainder_pending")
            return HEDGE_COMPLETION_PENDING

        accounting_ready = (
            self._execution_cost_ready(state)
            and self._execution_fill_coverage_ready(state))
        wanted = (POSITION_COVERED_CONFIRMED if accounting_ready
                  else POSITION_COVERED_UNANCHORED)
        self._set_hedge_completion_state(state, wanted, reason)
        return wanted

    def _execution_flat_tolerance(self, state: Optional[ExecutionState] = None) -> float:
        """Use the finest venue step, independently of net-delta tolerance.

        ``state`` remains accepted for existing recovery callers. A coarser
        shared order step must not hide an executable fill on the finer venue.
        """
        steps = [10.0 ** -int(v.size_decimals) for v in self.venues.values()
                 if hasattr(v, "size_decimals")]
        step = min(steps, default=max(float(self._step), 1e-12))
        return min(float(self.cfg.net_tolerance_base), step / 2)

    def _positions_flat(self, tolerance: Optional[float] = None) -> bool:
        tolerance = (self._execution_flat_tolerance() if tolerance is None
                     else min(float(tolerance), self._execution_flat_tolerance()))
        return bool(self.venues) and all(
            abs(float(v.position)) <= tolerance for v in self.venues.values())

    def _inventory_flatness(self) -> dict:
        positions = {}
        for v in self.venues.values():
            step = 10.0 ** -int(getattr(v, "size_decimals", 8))
            qty = floor_step(abs(float(v.position)), step)
            px = v.book.best_ask() if v.position < 0 else v.book.best_bid()
            base, quote = self._taker_minimums(v, reduce_only=True, qty=qty)
            dust = (abs(v.position) > self._execution_flat_tolerance() and
                    (qty < base or (px is not None and qty * px < quote)))
            positions[v.key] = {"position_base": v.position, "dust": dust,
                                "dust_notional_usd": abs(v.position) * px if dust and px else 0.0}
        return {"strict_flat": self._positions_flat(), "venues": positions,
                "has_dust": any(v["dust"] for v in positions.values())}

    def _pair_positions_balanced(self, tolerance: Optional[float] = None) -> bool:
        """Return whether the two venue positions are opposite and matched."""
        tolerance = (self.cfg.net_tolerance_base if tolerance is None
                     else float(tolerance))
        positions = [float(v.position) for v in self.venues.values()]
        if len(positions) < 2:
            return False
        if all(abs(value) <= tolerance for value in positions):
            return True
        if abs(sum(positions)) > tolerance:
            return False
        nonzero = [value for value in positions if abs(value) > tolerance]
        return bool(nonzero) and any(value > tolerance for value in nonzero) \
            and any(value < -tolerance for value in nonzero)

    def _mark_orphaned(self, reason: str,
                       state: Optional[ExecutionState] = None) -> None:
        now = time.time()
        first = not self._orphaned_inventory
        self._orphaned_inventory = True
        # A critical reconciliation mismatch invalidates the bounded
        # exploration assumption.  Stop new bootstrap OPEN decisions while
        # retaining the normal recovery path for the existing inventory.
        self._maybe_auto_disable_bootstrap("critical_recovery_mismatch")
        if self._orphaned_since_ts is None:
            self._orphaned_since_ts = now
        state = state or self.active_execution
        if state is not None:
            self._set_recovery_status(
                state, RecoveryStatus.ORPHANED_INVENTORY, reason)
        if first:
            for venue in self.venues.values():
                if abs(float(venue.position)) > self.cfg.net_tolerance_base:
                    log.warning("[ORPHAN] %s position=%+.8g reason=%s age=0s",
                                venue.key, venue.position, reason)

    def _clear_orphaned_if_flat(self) -> None:
        if not self._orphaned_inventory or not self._positions_flat():
            return
        self._orphaned_inventory = False
        self._orphaned_since_ts = None
        log.info("[ORPHAN] all venue positions are flat; recovery cleared")

    def _recovery_age(self, state: Optional[ExecutionState] = None) -> float:
        started = (getattr(state, "recovery_started_ts", None)
                   if state is not None else self._orphaned_since_ts)
        return max(time.time() - float(started), 0.0) if started else 0.0

    @staticmethod
    def _recovery_backoff_sec(attempt: int) -> float:
        """Exponential retry delay capped at five seconds."""
        exponent = min(max(int(attempt) - 1, 0), 4)
        return min(0.5 * (2 ** exponent), 5.0)

    def _defer_recovery(self, state: ExecutionState,
                        reason: Optional[str] = None) -> None:
        """Persist the next recovery time so duplicate callers stay single-flight."""
        delay = self._recovery_backoff_sec(max(state.recovery_attempt, 1))
        now = time.time()
        state.recovery_backoff_sec = delay
        state.recovery_next_retry_ts = now + delay
        if reason:
            state.recovery_reason = reason
        state.touch()
        self._checkpoint_execution(
            state, event_type="recovery_deferred",
            payload={"reason": reason or state.recovery_reason,
                     "attempt": state.recovery_attempt,
                     "backoff_sec": delay,
                     "next_retry_ts": state.recovery_next_retry_ts})
        self._schedule_poke(delay)

    def _execution_cost_ready(self, state: ExecutionState) -> bool:
        try:
            return self.ledger.execution_cost_ready(state.cycle_id)
        except Exception as exc:
            log.warning("[EXEC %s] cost readiness check failed: %r",
                        state.cycle_id, exc)
            return False

    @staticmethod
    def _fill_coverage_tolerance(target: float) -> float:
        """Use a quantity tolerance, not the much larger net-delta tolerance."""
        return max(1e-8, abs(float(target)) * 1e-4)

    def _exchange_observed_hedge_qty(self, state: ExecutionState) -> float:
        """Return hedge quantity proven by the post-send exchange positions."""
        first_target = float(state.first_filled)
        if first_target <= 0 or state.first_position_before is None \
                or state.hedge_position_before is None:
            return 0.0
        first_sign = 1.0 if state.entropy_is_buy else -1.0
        hedge_sign = 1.0 if state.hedge_is_buy else -1.0
        first_delta = (float(self.entropy.position)
                       - float(state.first_position_before)) * first_sign
        hedge_delta = (float(self.hedge.position)
                       - float(state.hedge_position_before)) * hedge_sign
        tolerance = self._fill_coverage_tolerance(first_target)
        if first_delta + tolerance < first_target or hedge_delta <= 0:
            return 0.0
        if hedge_delta > first_target + tolerance:
            return 0.0
        return min(hedge_delta, first_target)

    def _execution_fill_coverage_ready(self, state: ExecutionState) -> bool:
        """Require authoritative ledger quantities for every actual leg fill."""
        try:
            quantities = self.ledger.execution_authoritative_fill_quantities(
                state.cycle_id)
        except Exception as exc:
            log.warning("[EXEC %s] fill coverage check failed: %r",
                        state.cycle_id, exc)
            return False
        first_target = float(state.first_filled)
        hedge_target = max(float(state.hedge_filled),
                           self._exchange_observed_hedge_qty(state))
        return (
            float(quantities.get("first", 0.0))
            + self._fill_coverage_tolerance(first_target) >= first_target
            and float(quantities.get("hedge", 0.0))
            + self._fill_coverage_tolerance(hedge_target) >= hedge_target
        )

    def _sync_execution_quantities_from_evidence(self,
                                                 state: ExecutionState) -> None:
        """Bring checkpoint quantities up to authoritative recovery evidence."""
        observed_hedge = self._exchange_observed_hedge_qty(state)
        if observed_hedge > state.hedge_filled:
            state.hedge_filled = observed_hedge
            state.touch()

    def _cycle_adverse_drift_bps(self, candidate: TradeCandidate) -> float:
        """Convert signed midline drift into the cycle's adverse direction."""
        drift = candidate.cycle_midline_drift_bps
        if drift is None:
            return 0.0
        try:
            drift = float(drift)
        except (TypeError, ValueError):
            return 0.0
        direction = self.cycle.direction
        if direction not in {"long_entropy", "short_entropy"}:
            direction = cycle_direction_for_pair_state(candidate.pair_state_before)
        if direction == "long_entropy":
            return max(-drift, 0.0)
        if direction == "short_entropy":
            return max(drift, 0.0)
        return 0.0

    def _adverse_drift_persistence(self, candidate: TradeCandidate,
                                   now: float) -> tuple[float, float, Optional[float]]:
        """Track only continuous time above the signed adverse threshold."""
        cfg = self.cfg.cycle_exit
        adverse = self._cycle_adverse_drift_bps(candidate)
        if (not cfg.adverse_drift_enabled
                or candidate.action != StrategyAction.REDUCE
                or adverse < cfg.adverse_drift_bps):
            self._adverse_drift_started_ts = None
            return adverse, 0.0, None
        if self._adverse_drift_started_ts is None:
            self._adverse_drift_started_ts = now
        persisted = max(now - self._adverse_drift_started_ts, 0.0)
        return adverse, persisted, self._adverse_drift_started_ts

    @staticmethod
    def _clamp_execution_quantities(state: ExecutionState) -> dict:
        """Keep checkpoint quantities inside the one-order plan.

        A venue can report the same cumulative quantity through multiple
        endpoints.  The state machine must never turn that repeated fact into
        a second fill or a negative hedge remainder.
        """
        planned = max(float(state.planned_qty or 0.0), 0.0)
        first = min(max(float(state.first_filled or 0.0), 0.0), planned)
        hedge = min(max(float(state.hedge_filled or 0.0), 0.0), first)
        pending = min(max(float(state.pending_filled or 0.0), 0.0), planned)
        changed = {
            "first_filled": first != float(state.first_filled or 0.0),
            "hedge_filled": hedge != float(state.hedge_filled or 0.0),
            "pending_filled": pending != float(state.pending_filled or 0.0),
        }
        state.first_filled = first
        state.hedge_filled = hedge
        state.pending_filled = pending
        return {key: value for key, value in changed.items() if value}

    def _restore_execution_after_reconcile(self) -> None:
        """Attach the persisted execution after exchange positions are known.

        Startup position reads establish current exposure only.  They do not
        settle a missing order or create a mark-priced fill; the order/fill
        recovery path must obtain the exchange's actual fill facts.
        """
        restored = self._restored_execution
        self._restored_execution = None
        if restored is None:
            return
        state, context = restored
        if state.terminal:
            return
        clamped = self._clamp_execution_quantities(state)
        if clamped:
            state.last_error = "checkpoint quantities clamped to order plan"
            log.error("[EXEC %s] repaired checkpoint quantities: %s",
                      state.cycle_id, clamped)
        self.active_execution = state
        self._recovery_context = context or {}
        # The production caller invokes this after strict startup position
        # reconciliation.  Direct recovery callers may attach the checkpoint
        # before that read; in that case an authoritative order fill must
        # still update the local working position exactly once.
        self._recovery_context["startup_positions_loaded"] = bool(
            self.startup_reconciled)
        self._set_recovery_status(
            state, RecoveryStatus.RECOVERING_FILL, "startup_restore")
        self._checkpoint_execution(state, event_type="startup_restore")
        log.warning("[EXEC %s] resumed after restart: phase=%s first=%.6g "
                    "hedge=%.6g pending=%s", state.cycle_id,
                    state.phase.value, state.first_filled, state.hedge_filled,
                    state.pending_leg or "none")

    def _reconcile_execution_positions(
            self, state: ExecutionState,
            *, source: str = "restart_reconciliation") -> bool:
        """Compare positions for diagnostics without manufacturing a fill.

        A position delta is useful evidence that an order may have filled, but
        it cannot provide the historical price, fee, or retry-level identity.
        Actual fill history is recovered by ``recover_missing_fill``.
        """
        for leg, venue, before, is_buy, old_fill, avg_px, order_ref in (
                ("first", self.entropy, state.first_position_before,
                 state.entropy_is_buy, state.first_filled,
                 state.first_avg_px, state.first_order_ref),
                ("hedge", self.hedge, state.hedge_position_before,
                 state.hedge_is_buy, state.hedge_filled,
                 state.hedge_avg_px, state.hedge_order_ref)):
            if before is None:
                continue
            direction = 1.0 if is_buy else -1.0
            observed = max((venue.position - before) * direction, 0.0)
            if observed + self.cfg.net_tolerance_base < old_fill:
                state.last_error = (
                    f"{leg} exchange delta {observed:.8g} below checkpoint "
                    f"{old_fill:.8g}")
                state.transition(ExecutionPhase.RECONCILING,
                                 f"{leg}_position_mismatch")
                continue
            if observed > old_fill + self.cfg.net_tolerance_base:
                log.info("[EXEC %s] position suggests %s fill %.8g; "
                         "waiting for exchange fill history", state.cycle_id,
                         leg, observed)
        return False

    def _settle_execution_from_positions(
            self, state: ExecutionState,
            *, source: str = "position_reconciliation") -> bool:
        """Compatibility shim: positions alone never settle an order."""
        self._reconcile_execution_positions(state, source=source)
        log.warning("[EXEC %s] refusing position-only settlement for %s; "
                    "exchange fill facts are required", state.cycle_id,
                    state.pending_order_ref or "unknown order")
        return False

    async def _startup_recovery(self) -> None:
        """Finish the durable execution or flatten any unmatched startup delta."""
        self.reconciliation["status"] = "recovering"
        if self.active_execution is not None:
            await self._recover_active_execution(immediate=True)
        if self.active_execution is None and self._orphaned_inventory:
            await self._emergency_exit_positions("orphaned_inventory")
        if self.active_execution is None and not self._orphaned_inventory:
            await self._maybe_hedge()
        net = sum(v.position for v in self.venues.values())
        self.reconciliation["net_delta_base"] = net
        if self.active_execution is not None:
            self.reconciliation["status"] = "recovery_pending"
        elif self._positions_flat():
            self.reconciliation["status"] = "synced"
        elif self._orphaned_inventory:
            self.reconciliation["status"] = "orphaned_inventory"
        elif self._pair_positions_balanced():
            self.reconciliation["status"] = "positions_open"
        else:
            self.reconciliation["status"] = "blocked"
            log.critical("startup reconciliation left non-flat positions "
                         "(net %+.6g); strategy will remain blocked", net)

    async def _flatten_only_run(self) -> bool:
        """Flatten existing positions without starting the strategy loop."""
        deadline = time.time() + max(
            60.0, self.cfg.settle_timeout_sec * 4.0)
        while not self.stop.is_set() and time.time() < deadline:
            await self._recover_emergency_pending()
            if self._positions_flat():
                self._clear_orphaned_if_flat()
                self._sync_cycle_with_positions()
                self._write_dashboard_snapshot_now()
                log.warning("[FLATTEN-ONLY] all venue positions are flat")
                return True
            await self._emergency_exit_positions("manual_flatten")
            if self._positions_flat():
                self._clear_orphaned_if_flat()
                self._sync_cycle_with_positions()
                self._write_dashboard_snapshot_now()
                log.warning("[FLATTEN-ONLY] all venue positions are flat")
                return True
            await self._reconcile_positions(hedge=False)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
        flat = self._positions_flat()
        if not flat:
            log.critical("[FLATTEN-ONLY] timeout with positions: %s",
                         " ".join(f"{v.name}={v.position:+.8g}"
                                  for v in self.venues.values()))
        return flat

    async def _execution_recovery_loop(self) -> None:
        """Continuously service the hedge remainder independent of book ticks."""
        while not self.stop.is_set():
            if self.active_execution is not None:
                try:
                    await self._recover_active_execution(immediate=False)
                    if self.active_execution is None:
                        self._update_evt.set()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("execution recovery loop failed")
            try:
                await asyncio.wait_for(self.stop.wait(),
                                       timeout=self._recovery_poll_sec)
            except asyncio.TimeoutError:
                pass

    async def _run_inner(self) -> None:
        cfg = self.cfg
        self.entropy = self._make_venue(cfg.entropy)
        self.venues = {"entropy": self.entropy}
        self.hedge = self._make_venue(cfg.hedge)
        self.venues["hedge"] = self.hedge
        if self.hedge.kind == "lighter":
            cleared = self.ledger.mark_zero_fee_venue(self.hedge.key)
            if cleared:
                log.info("[%s] cleared %d historical provisional fees: "
                         "zero-fee venue policy", self.hedge.name, cleared)
        await asyncio.gather(self.entropy.load_market(), self.hedge.load_market())
        self.markets_ready = True

        live = not self.record_only
        if live:
            if not cfg.creds_complete:
                raise RuntimeError(
                    "live trading needs credentials for both venues in .env "
                    "(see .env.example); use --record-only to run without "
                    "them / 实盘需要在 .env 中配置两个交易所的密钥，仅采集数据"
                    "请用 --record-only")
            self.entropy.init_signer()
            prepare = getattr(self.hedge, 'prepare_signer', None)
            if prepare is not None:
                await prepare()
            else:
                self.hedge.init_signer()
            if self.hedge.kind == "hl":
                self.entropy.share_nonces_with(self.hedge)
            for v in self.venues.values():
                if v.kind == "hl":
                    await v.assert_exchange_tradeable()
            await asyncio.gather(*(
                v.set_leverage(v.conf.leverage)
                for v in self.venues.values()
                if v.conf.leverage is not None))
        if (self.hedge.kind == "hl"
                and self.entropy._query_address()
                and self.entropy._query_address() == self.hedge._query_address()):
            self.hedge.include_core_equity = False  # shared account: count once

        self._step = 10 ** -min(self.entropy.size_decimals,
                                self.hedge.size_decimals)
        self._min_base = max(self.entropy.min_base, self.hedge.min_base,
                             self._step)
        self._min_notional = max(cfg.min_order_notional,
                                 self.entropy.min_quote, self.hedge.min_quote)
        log.info("pair ENTROPY(%s)-%s(%s): midline=%+.2fbps band=[-%.2f, +%.2f] "
                 "open_sides=%s fees=%.2f+%.2f step=%g min_ntl=$%g",
                 self.entropy.conf.symbol, self.hedge.name,
                 self.hedge.conf.symbol, cfg.midline_bps, cfg.lower_bps,
                 cfg.upper_bps,
                 ",".join(side for side, enabled in (
                     ("sell_entropy", cfg.allow_sell_entropy_open),
                     ("buy_entropy", cfg.allow_buy_entropy_open)) if enabled)
                 or "none",
                 self.entropy.fee_bps, self.hedge.fee_bps,
                 self._step, self._min_notional)

        if cfg.dynamic_midline_enabled:
            now = time.time()
            self._backfill_premium_model(now)

        if self.record_only:
            log.warning("RECORD-ONLY — collecting minute data, no strategy, "
                        "no orders")
        else:
            log.warning("LIVE — real orders will be sent (use --record-only "
                        "for credential-less data collection)")

        tasks: List[asyncio.Task] = []
        for v in self.venues.values():
            tasks += v.start_tasks(self.stop, self._update_evt.set, live)
        if cfg.recorder_enabled or self.record_only:
            self.recorder = MinuteRecorder(
                cfg.recorder_csv, self.entropy.book, self.hedge.book,
                cfg.staleness_sec, ledger=self.ledger)
            tasks.append(asyncio.create_task(self.recorder.run(self.stop),
                                             name="recorder"))
            self.seconds_recorder = SecondRecorder(
                cfg.recorder_seconds_csv, self.entropy, self.hedge,
                cfg.staleness_sec, self._premium_snapshot,
                strategy_getter=self._dashboard_strategy_values,
                pnl_getter=self.pnl_tracker.snapshot,
                max_book_age_ms=cfg.premium_model.max_book_age_ms,
                max_cross_venue_skew_ms=(
                    cfg.premium_model.max_cross_venue_skew_ms),
                ledger=self.ledger)
            tasks.append(asyncio.create_task(self.seconds_recorder.run(self.stop),
                                             name="seconds-recorder"))
        tasks.append(asyncio.create_task(self._premium_model_loop(),
                                         name="premium-model"))
        if self.config_file and cfg.hot_reload.enabled:
            tasks.append(asyncio.create_task(self._config_reload_loop(),
                                             name="config-reload"))
        if cfg.funding_enabled:
            tasks.append(asyncio.create_task(self._funding_loop(),
                                             name="funding"))
        if not self.record_only:
            tasks.append(asyncio.create_task(self._balance_loop(),
                                             name="balances"))
            tasks.append(asyncio.create_task(self._http_keepalive_loop(),
                                             name="keepalive"))
        tasks.append(asyncio.create_task(self._status_loop(), name="status"))
        if cfg.wechat_clawbot.enabled:
            self.wechat_reporter = WeChatHourlyReporter(
                self, cfg.wechat_clawbot, self.session)
            tasks.append(asyncio.create_task(
                self.wechat_reporter.run(self.stop),
                name="wechat-hourly-reporter"))
        if cfg.web_dashboard_enabled:
            if self.ledger_path != ":memory:":
                tasks.append(asyncio.create_task(
                    self._retro_farming_snapshot_loop(),
                    name="retro-farming-shadow"))
            tasks.append(asyncio.create_task(self._dashboard_snapshot_loop(),
                                             name="web-dashboard-snapshot"))
        if live:
            # Feed tasks must be alive before startup recovery can price an
            # emergency hedge. No strategy task is started until the two
            # venues have been reconciled and any durable execution checkpoint
            # has had a chance to continue.
            try:
                await self._wait_for_market_data()
                await self._reconcile_positions(hedge=False, strict=True)
                log.info("starting positions: %s (net %+.6g)",
                         " ".join(f"{v.name}={v.position:+.6g}"
                                  for v in self.venues.values()),
                         sum(v.position for v in self.venues.values()))
                self._restore_execution_after_reconcile()
                await self._startup_recovery()
                if self.flatten_only:
                    flat = await self._flatten_only_run()
                    if not flat:
                        self.halted = True
                    self.request_stop()
                else:
                    tasks.append(asyncio.create_task(
                        self._execution_recovery_loop(),
                        name="execution-recovery"))
                    tasks.append(asyncio.create_task(self._strategy_loop(),
                                                     name="strategy"))
                    tasks.append(asyncio.create_task(self._reconcile_loop(),
                                                     name="reconcile"))
                    tasks.append(asyncio.create_task(
                        self._fee_reconciliation_loop(),
                        name="fee-reconciliation"))
            except BaseException:
                self.stop.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self._close_venues()
                raise

        await self.stop.wait()
        if self._exec_tasks:  # let in-flight executions settle, never cancel
            log.info("waiting for %d in-flight execution(s) to settle",
                     len(self._exec_tasks))
            await asyncio.wait(self._exec_tasks,
                               timeout=cfg.settle_timeout_sec + 2.0)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._sync_cycle_with_positions()
        self._write_dashboard_snapshot_now()
        self._persist_model_state(time.time())
        await self._close_venues()
        log.info("shutdown — %d trades, %d hedges, exp edge $%.4f, "
                 "fill edge $%.4f", self.trades, self.hedges,
                 self.total_exp_edge, self.total_fill_edge)

    @staticmethod
    def _book_has_top(venue) -> bool:
        book = getattr(venue, "book", None)
        return bool(book and getattr(book, "ready", False)
                    and book.bids and book.asks)

    def _current_premium_sample_quality(self, now: float) -> PremiumSampleQuality:
        entropy_ts = (self.entropy.book.last_update_ts
                      if self._book_has_top(self.entropy) else None)
        hedge_ts = (self.hedge.book.last_update_ts
                    if self._book_has_top(self.hedge) else None)
        return check_premium_sample_quality(
            now, entropy_ts, hedge_ts,
            self.cfg.premium_model.max_book_age_ms,
            self.cfg.premium_model.max_cross_venue_skew_ms)

    def _hard_market_data_age_sec(self) -> float:
        """Return the physical stale boundary for the order path."""
        configured = max(float(self.cfg.staleness_sec), 0.0)
        quality = max(float(self.cfg.premium_model.max_book_age_ms) / 1000.0,
                     0.0)
        # The premium quality limit is the GOOD/DEGRADED boundary.  The
        # execution loop retains the wider configured feed-staleness boundary
        # as the hard safety limit, never the soft quality threshold.
        return max(configured, quality * 2.0)

    def _hard_cross_skew_ms(self) -> float:
        """Return the bounded hard cross-venue timestamp boundary."""
        soft = max(float(self.cfg.premium_model.max_cross_venue_skew_ms), 0.0)
        return max(soft * 2.0, soft + 500.0)

    @staticmethod
    def _snapshot_readiness(snap: PremiumModelSnapshot) -> PremiumReadiness:
        value = getattr(snap, "readiness", PremiumReadiness.UNAVAILABLE)
        if isinstance(value, PremiumReadiness):
            return value
        try:
            return PremiumReadiness(str(value))
        except ValueError:
            # Old direct integrations only supplied ``ready``. Treat those
            # snapshots as normal when they explicitly opt into readiness.
            return (PremiumReadiness.NORMAL if bool(getattr(snap, "ready", False))
                    else PremiumReadiness.UNAVAILABLE)

    @classmethod
    def _snapshot_tradable(cls, snap: PremiumModelSnapshot) -> bool:
        return bool(getattr(snap, "is_tradable", False)
                    or cls._snapshot_readiness(snap) !=
                    PremiumReadiness.UNAVAILABLE)

    async def _premium_model_loop(self) -> None:
        while not self.stop.is_set():
            now = time.time()
            if self.cfg.dynamic_midline_enabled:
                quality = self._current_premium_sample_quality(now)
                premium = self.premium_bps() if quality.valid else None
                if quality.valid and premium is not None:
                    # Invalid samples are deliberately absent from both the
                    # center windows and the independent sigma window.
                    self.premium_model.observe(now, premium)
                elif quality.valid:
                    # A complete timestamp pair without usable top-of-book is
                    # still a missing sample, never a forward-filled one.
                    quality = check_premium_sample_quality(
                        now, None, None,
                        self.cfg.premium_model.max_book_age_ms,
                        self.cfg.premium_model.max_cross_venue_skew_ms)
                self.last_premium_sample_quality = quality
                self.premium_health.observe(now, quality)
            self.model_snapshot = self.premium_model.snapshot(now)
            # The close reserve uses a short, bounded history of the hedge
            # top-of-book.  Sample it on the same cadence as the market model,
            # not only when an order is being planned, otherwise the p95
            # adverse-move term would be based on sparse execution moments.
            self._record_hedge_quote(now)
            if now - getattr(self, "_last_model_state_save_ts", 0.0) >= 30.0:
                self._persist_model_state(now)
            try:
                await asyncio.wait_for(self.stop.wait(),
                                       timeout=self.cfg.dynamic_sample_sec)
            except asyncio.TimeoutError:
                pass

    def _premium_snapshot(self, now: float) -> PremiumModelSnapshot:
        self.model_snapshot = self.premium_model.snapshot(now)
        return self.model_snapshot

    def _backfill_premium_model(self, now: Optional[float] = None
                                ) -> PremiumModelSnapshot:
        """Rehydrate an insufficient checkpoint from durable recorder data."""
        now = time.time() if now is None else float(now)
        snapshot = self.premium_model.snapshot(now)
        if snapshot.readiness == PremiumReadiness.NORMAL:
            self._model_history_samples = snapshot.sample_count
            return snapshot

        sources: list[str] = []
        cutoff = now - self.premium_model.long_window_sec
        try:
            sqlite_samples = self.ledger.load_valid_premium_samples(
                since=cutoff, until=now)
        except Exception as exc:
            sqlite_samples = []
            log.warning("premium model SQLite history backfill failed: %s", exc)
        if sqlite_samples:
            self.premium_model.merge_history(sqlite_samples, now)
            sources.append("sqlite")

        snapshot = self.premium_model.snapshot(now)
        if not snapshot.ready:
            # Keep CSV as a compatibility fallback for databases created
            # before market_samples was enabled or for a partially migrated
            # ledger. It is still filtered by the same rolling window and
            # valid-sample flag in bootstrap_premium_samples().
            csv_samples = bootstrap_premium_samples(
                self.cfg.recorder_seconds_csv, now,
                self.premium_model.long_window_sec)
            if self.premium_model.merge_history(csv_samples, now):
                sources.append("csv")
            snapshot = self.premium_model.snapshot(now)

        self._model_history_samples = snapshot.sample_count
        if sources:
            prior = (self._model_history_source
                     if self._model_history_source != "seed" else None)
            self._model_history_source = "+".join(
                [item for item in (prior, *sources) if item])
        if snapshot.readiness == PremiumReadiness.NORMAL:
            self._model_restore_until = max(
                self._model_restore_until,
                now + MODEL_RESTORE_VALIDATION_SEC)
            log.info("premium model history backfilled: %d samples from %s; "
                     "normal %.0fs/%d samples; cold %.0fs/%d; "
                     "OPEN validation %.0fs",
                     snapshot.sample_count,
                     self._model_history_source,
                     self.premium_model.warmup_sec,
                     self.premium_model.min_samples,
                     self.premium_model.cold_start_warmup_sec,
                     self.premium_model.cold_start_min_samples,
                     MODEL_RESTORE_VALIDATION_SEC)
        else:
            log.info("premium model history readiness=%s: %d samples from %s; "
                     "normal %.0fs/%d, cold %.0fs/%d",
                     snapshot.readiness.value,
                     snapshot.sample_count, self._model_history_source,
                     self.premium_model.warmup_sec,
                     self.premium_model.min_samples,
                     self.premium_model.cold_start_warmup_sec,
                     self.premium_model.cold_start_min_samples)
        return snapshot

    def _persist_model_state(self, now: Optional[float] = None) -> None:
        """Checkpoint the bounded signal model without touching order state."""
        now = time.time() if now is None else float(now)
        try:
            self.ledger.save_model_state(
                MODEL_STATE_KEY, self.premium_model.export_state(now), ts=now)
            self._last_model_state_save_ts = now
        except Exception as exc:
            # A stale model can be rebuilt, but silently losing a warm state
            # makes a restart trade on the seed.  Fail closed for new orders.
            self.halted = True
            log.critical("premium model checkpoint failed: %r — "
                         "new trading halted", exc)

    async def _funding_loop(self) -> None:
        while not self.stop.is_set():
            now = time.time()
            results = await asyncio.gather(
                *(v.fetch_funding() for v in self.venues.values()),
                return_exceptions=True)
            for v, result in zip(self.venues.values(), results):
                if isinstance(result, BaseException):
                    log.warning("[%s] funding refresh failed: %r", v.name, result)
            accrual_from = self._last_funding_accrual_ts
            self._last_funding_accrual_ts = now
            if accrual_from is not None and now > accrual_from:
                hours = (now - accrual_from) / 3600.0
                for v in self.venues.values():
                    rate = getattr(getattr(v, "funding", None),
                                   "rate_bps_per_hour", None)
                    mark = v.book.mid()
                    if rate is None or mark is None:
                        continue
                    # Positive funding means a long pays and a short earns.
                    pnl = -(v.position * mark * float(rate) / 1e4) * hours
                    try:
                        self.ledger.record_funding_event(
                            ts=now, venue=v.key, position_qty=v.position,
                            mark_px=mark, rate_bps_h=float(rate),
                            pnl_usd=pnl, source="estimated_funding")
                    except Exception:
                        log.exception("[%s] funding ledger write failed", v.name)
            try:
                await asyncio.wait_for(self.stop.wait(),
                                       timeout=self.cfg.funding_poll_sec)
            except asyncio.TimeoutError:
                pass

    # --------------------------------------------------------------- signals

    def _inv_add_bps(self, buy, sell) -> float:
        """Inventory ladder: a surcharge that grows once a venue's position
        passes floor_frac of its cap in the direction the trade would add to
        (buying adds when that venue is >= flat long; selling adds when the
        venue is <= flat short). Max of the two venues' ramps."""
        scale = self.cfg.inventory_scale_bps
        if scale <= 0:
            return 0.0
        floor = min(max(self.cfg.inventory_floor_frac, 0.0), 0.99)

        def ramp(v, adding: bool) -> float:
            if not adding:
                return 0.0
            ref = v.book.mid()
            if ref is None:
                return 0.0
            u = min(abs(v.position) * ref / v.cap_usd, 1.0)
            if u <= floor:
                return 0.0
            return scale * (u - floor) / (1.0 - floor)

        return max(ramp(buy, buy.position >= 0), ramp(sell, sell.position <= 0))

    def _effective_entry_bands(self, snap: PremiumModelSnapshot) -> tuple[float, float]:
        """Return the current short/long entry bands in bps."""
        cfg = self.cfg
        adaptive = cfg.adaptive_bands
        if not adaptive.enabled:
            return cfg.upper_bps, cfg.lower_bps
        return (
            compute_adaptive_entry_band(
                base_bps=cfg.upper_bps,
                configured_min_bps=adaptive.min_upper_bps,
                robust_sigma_bps=snap.robust_sigma_bps,
                sigma_mult=adaptive.entry_sigma_mult,
                max_bps=adaptive.max_upper_bps,
            ),
            compute_adaptive_entry_band(
                base_bps=cfg.lower_bps,
                configured_min_bps=adaptive.min_lower_bps,
                robust_sigma_bps=snap.robust_sigma_bps,
                sigma_mult=adaptive.entry_sigma_mult,
                max_bps=adaptive.max_lower_bps,
            ),
        )

    def _eff_threshold(self, buy, sell) -> float:
        """Compatibility helper for an OPEN/ADD threshold without funding."""
        snap = self.model_snapshot
        midline = (snap.active_midline_bps if self.cfg.dynamic_midline_enabled
                   else self.cfg.midline_bps)
        upper, lower = self._effective_entry_bands(snap)
        inventory = self._inv_add_bps(buy, sell)
        if sell.key == "entropy":
            return entry_threshold_short(midline, upper, inventory)
        return entry_threshold_long(midline, lower, inventory)

    def _cycle_exit_midline(self, direction: str, current_midline: float) -> float:
        cycle = self.cycle
        if (self.cfg.cycle_exit.enabled and cycle.active and cycle.anchored
                and cycle.direction == direction):
            reference = (cycle.entry_reference_bps
                         if cycle.entry_reference_bps is not None
                         else cycle.entry_midline_bps)
            if reference is not None:
                return reference
        return current_midline

    def _cycle_current_reference_bps(
            self, snap: Optional[PremiumModelSnapshot],
            fallback: Optional[float] = None) -> Optional[float]:
        """Return the current value in the cycle's original reference space.

        A cycle entered on a dynamic or blended center must be compared with
        that same coordinate.  If the robust center is temporarily absent,
        use the model's last valid active midline rather than the configured
        seed, so a data gap cannot manufacture drift.
        """
        cycle = self.cycle
        mode = cycle.entry_reference_mode or "active_midline"
        if snap is None:
            return fallback
        active = _safe_float(snap.active_midline_bps)
        dynamic = self._dynamic_center_bps(snap)
        if mode == "configured_midline":
            return float(self.cfg.midline_bps)
        if mode == "dynamic_center":
            return dynamic if dynamic is not None else active
        if mode == "blended_midline":
            if active is not None and dynamic is not None:
                return (active + dynamic) / 2.0
            return active
        return active

    def _cycle_drift(self, current_midline: float,
                     snap: Optional[PremiumModelSnapshot] = None) \
            -> Optional[float]:
        cycle = self.cycle
        entry_reference = (cycle.entry_reference_bps
                           if cycle.entry_reference_bps is not None
                           else cycle.entry_midline_bps)
        if (self.cfg.cycle_exit.enabled and cycle.active and cycle.anchored
                and entry_reference is not None):
            current_reference = self._cycle_current_reference_bps(
                snap, current_midline)
            if current_reference is None:
                return None
            return compute_cycle_midline_drift(
                current_reference, entry_reference)
        return None

    def _cycle_add_blocked(self, state: PairState,
                           drift_bps: Optional[float]) -> bool:
        if not self.cfg.cycle_exit.enabled or state not in (
                PairState.SHORT_ENTROPY, PairState.LONG_ENTROPY):
            return False
        if self._cycle_hold_state in {
                "loss_recovery_hold", "long_hold",
                "forced_release_pending"}:
            return True
        expected = cycle_direction_for_pair_state(state)
        cycle = self.cycle
        if (not cycle.active or cycle.direction != expected
                or not cycle.anchored):
            return True
        return drift_bps is not None and is_cycle_add_blocked(
            drift_bps, self.cfg.cycle_exit.max_midline_drift_bps)

    @staticmethod
    def _lag_state(value) -> MidlineLagState:
        if isinstance(value, MidlineLagState):
            return value
        try:
            return MidlineLagState(str(value))
        except ValueError:
            return MidlineLagState.NORMAL

    @staticmethod
    def _cycle_direction_for_signal(direction: str) -> Optional[str]:
        if direction == "buy_entropy":
            return "long_entropy"
        if direction == "sell_entropy":
            return "short_entropy"
        return direction if direction in InventoryCycle.VALID_DIRECTIONS else None

    def _reference_center_bps(self, snap: PremiumModelSnapshot) -> Optional[float]:
        lag_state = self._lag_state(snap.midline_lag_state)
        return self._entry_reference_bps(snap, lag_state)[0]

    @staticmethod
    def _dynamic_center_bps(snap: PremiumModelSnapshot) -> Optional[float]:
        """Return the robust center used when the active midline is stale."""
        centers = [_safe_float(value) for value in (
            snap.short_center_bps, snap.long_center_bps)]
        centers = [value for value in centers if value is not None]
        if not centers:
            return None
        return sum(centers) / len(centers)

    def _entry_reference_bps(
            self, snap: PremiumModelSnapshot,
            lag_state: MidlineLagState) -> tuple[float, str, Optional[float]]:
        """Return the unified entry reference used by signal and execution."""
        policy = self._reference_policy(snap, lag_state)
        return (policy["reference_bps"], policy["reference_mode"],
                policy["dynamic_center_bps"])

    def _reference_policy(self, snap: PremiumModelSnapshot,
                          lag_state: MidlineLagState,
                          current_premium: Optional[float] = None) -> dict:
        """Map model health to a reference, requirement and confidence.

        A regime shift is not itself a risk event.  It changes the coordinate
        used for a new entry and raises the deviation requirement.  Only an
        invalid reference/model state is a hard data gate; this keeps a real
        dislocation tradable without treating a stale seed as truth.
        """
        policy = getattr(self.cfg, "reference_policy", None)
        enabled = bool(getattr(policy, "enabled", True))
        dynamic_enabled = bool(self.cfg.dynamic_midline_enabled)
        configured = float(self.cfg.midline_bps)
        active = (_safe_float(snap.active_midline_bps)
                  if dynamic_enabled else configured)
        active = configured if active is None else active
        dynamic_center = self._dynamic_center_bps(snap)
        dynamic_values = (_safe_float(snap.short_center_bps),
                          _safe_float(snap.long_center_bps))
        dynamic_valid = all(value is not None for value in dynamic_values)
        readiness = self._snapshot_readiness(snap)
        snapshot_tradable = self._snapshot_tradable(snap)

        state = "normal"
        if dynamic_enabled and not snapshot_tradable:
            state = "invalid"
        elif enabled and dynamic_enabled:
            if lag_state == MidlineLagState.BLOCKED:
                state = "blocked"
            elif lag_state == MidlineLagState.ACCELERATING:
                state = "accelerating"
            elif bool(snap.regime_shift):
                state = "shifting"
            elif lag_state == MidlineLagState.WARNING:
                state = "warning"
            elif readiness == PremiumReadiness.COLD_DEGRADED:
                state = "cold_degraded"

        if state == "accelerating":
            mode = str(getattr(policy, "accelerating_mode",
                               "blended_midline"))
            sigma_mult = float(getattr(policy, "accelerating_sigma_mult",
                                       1.5))
            min_deviation = 0.0
        elif state == "shifting":
            mode = str(getattr(policy, "shifting_mode", "dynamic_center"))
            sigma_mult = float(getattr(policy, "shifting_sigma_mult", 2.0))
            min_deviation = float(getattr(
                policy, "shifting_min_deviation_bps", 2.0))
        elif state == "blocked":
            mode = str(getattr(policy, "blocked_mode", "dynamic_center"))
            sigma_mult = float(getattr(policy, "blocked_sigma_mult", 2.0))
            min_deviation = float(getattr(
                policy, "blocked_min_deviation_bps", 2.5))
        elif state == "invalid":
            mode = "invalid"
            sigma_mult = 0.0
            min_deviation = 0.0
        else:
            mode = ("configured_midline" if not dynamic_enabled
                    else "active_midline")
            sigma_mult = 0.0
            min_deviation = 0.0

        if state in {"accelerating", "shifting", "blocked"}:
            if mode == "dynamic_center" and dynamic_center is not None:
                reference = dynamic_center
            elif mode == "blended_midline" and dynamic_center is not None:
                reference = (active + dynamic_center) / 2.0
            else:
                reference = active
        else:
            reference = configured if not dynamic_enabled else active

        sigma = _safe_float(snap.robust_sigma_bps)
        sigma_requirement = ((sigma_mult * sigma)
                             if sigma is not None and sigma > 0 else 0.0)
        required_deviation = max(min_deviation, sigma_requirement)
        requires_dynamic = state in {"accelerating", "shifting", "blocked"}
        reference_valid = (math.isfinite(reference)
                           and (not dynamic_enabled or snapshot_tradable))
        if requires_dynamic and not dynamic_valid:
            reference_valid = False
        # A non-zero sigma requirement without a measured sigma is not a
        # reason to guess.  The normal/warning states can still use the active
        # midline while the model is healthy enough for those states.
        if requires_dynamic and sigma_requirement > 0 and sigma is None:
            reference_valid = False

        signal_deviation = (abs(float(current_premium) - reference)
                            if current_premium is not None
                            and math.isfinite(reference) else None)
        dynamic_deviation = (
            abs(float(current_premium) - dynamic_center)
            if current_premium is not None and dynamic_center is not None
            else None)
        extreme_sigma_mult = float(getattr(
            policy, "extreme_sigma_mult", 3.0))
        extreme_threshold = ((extreme_sigma_mult * sigma)
                             if sigma is not None and sigma > 0 else None)
        extreme_dislocation = bool(
            enabled and dynamic_enabled
            and bool(getattr(policy, "extreme_enabled", True))
            and state in {"shifting", "blocked"}
            and dynamic_valid and snapshot_tradable
            and dynamic_deviation is not None
            and extreme_threshold is not None
            and dynamic_deviation >= extreme_threshold)
        # Deviation requirements remain useful telemetry, but are no longer a
        # second hard gate.  The statistical band plus whole-cycle EV decide
        # whether an otherwise valid reference is worth trading.
        policy_allowed = reference_valid

        confidence = {
            "normal": 1.0,
            "warning": 0.5,
            "cold_degraded": 0.35,
            "accelerating": 0.25,
            "shifting": 0.25,
            "blocked": 0.25,
            "invalid": 0.0,
        }[state]
        if not reference_valid:
            confidence = 0.0
        if state == "cold_degraded":
            model_uncertainty = max(
                _safe_float(getattr(snap, "model_uncertainty_bps", 0.0))
                or 0.0, 0.0)
        else:
            model_uncertainty = max(
                _safe_float(getattr(snap, "model_uncertainty_bps", 0.0))
                or 0.0, 0.0)
        reference_uncertainty = {
            "normal": 0.0,
            "warning": max(float(getattr(policy, "warning_penalty_bps", 0.5)),
                            0.5),
            "cold_degraded": 0.0,
            "accelerating": max(1.0, sigma or 0.0),
            "shifting": max(1.0, sigma or 0.0),
            "blocked": max(1.0, sigma or 0.0),
            "invalid": 0.0,
        }[state]
        return {
            "reference_state": state,
            "readiness": readiness.value,
            "reference_mode": mode,
            "reference_bps": reference,
            "dynamic_center_bps": dynamic_center,
            "dynamic_center_valid": dynamic_valid,
            "reference_valid": reference_valid,
            "reference_confidence": confidence,
            "required_sigma_mult": sigma_mult,
            "min_deviation_bps": min_deviation,
            "required_deviation_bps": required_deviation,
            "signal_deviation_bps": signal_deviation,
            "dynamic_deviation_bps": dynamic_deviation,
            "extreme_dislocation": extreme_dislocation,
            "extreme_threshold_bps": extreme_threshold,
            # Kept as a stable dashboard field; it is intentionally no longer
            # added to the statistical entry band.
            "entry_penalty_bps": 0.0,
            "reference_uncertainty_bps": reference_uncertainty,
            "model_uncertainty_bps": model_uncertainty,
            "policy_allowed": policy_allowed,
        }

    def _current_basis_bps(self) -> Optional[float]:
        entropy_mid = self.entropy.book.mid()
        hedge_mid = self.hedge.book.mid()
        if entropy_mid is None or hedge_mid in (None, 0):
            return None
        basis = (entropy_mid / hedge_mid - 1.0) * 1e4
        return basis if math.isfinite(basis) else None

    def _lag_open_blocked(self, lag_state: MidlineLagState) -> bool:
        guard = self.cfg.midline_lag_guard
        return ((lag_state == MidlineLagState.ACCELERATING
                 and guard.block_accelerating)
                or (lag_state == MidlineLagState.BLOCKED
                    and guard.block_blocked))

    def _reentry_status(self, direction: str, now: float,
                        lag_state: Optional[MidlineLagState] = None) -> dict:
        """Return the direction-specific post-risk-exit gate state."""
        guard = getattr(self.cfg, "reentry_guard", None)
        last_direction = self.cycle.last_risk_exit_direction
        result = {
            "enabled": bool(guard and guard.enabled),
            "blocked": False,
            "reason": None,
            "uncertainty_bps": 0.0,
            "direction": direction,
            "last_risk_exit_direction": last_direction,
            "last_risk_exit_ts": self.cycle.last_risk_exit_ts,
            "cooldown_remaining_sec": 0.0,
            "basis_reset_seen": bool(self.cycle.reentry_reset_seen),
            "same_direction_only": bool(
                guard.same_direction_only if guard else True),
        }
        if guard is None or not guard.enabled or not last_direction:
            return result

        cycle_direction = self._cycle_direction_for_signal(direction)
        same_direction = (not guard.same_direction_only
                          or cycle_direction == last_direction)
        if not same_direction:
            result["reason"] = "opposite_direction"
            return result

        now = float(now)
        last_ts = _safe_float(self.cycle.last_risk_exit_ts)
        elapsed = max(now - last_ts, 0.0) if last_ts is not None else 0.0
        remaining = max(guard.post_risk_exit_cooldown_sec - elapsed, 0.0)
        result["cooldown_remaining_sec"] = remaining
        if remaining > 0:
            result["blocked"] = True
            result["reason"] = "post_risk_exit_cooldown"
            cooldown = max(float(guard.post_risk_exit_cooldown_sec), 1e-9)
            result["uncertainty_bps"] = min(
                2.0, max(0.0, 2.0 * remaining / cooldown))
            return result
        if not self.cycle.reentry_reset_seen:
            result["blocked"] = True
            result["reason"] = "basis_reset_pending"
            result["uncertainty_bps"] = 1.0
            return result
        current_lag = (lag_state if lag_state is not None
                        else self._lag_state(self.model_snapshot.midline_lag_state))
        if self._lag_open_blocked(current_lag):
            result["blocked"] = True
            result["reason"] = "midline_lag"
            result["uncertainty_bps"] = 0.5
            return result
        return result

    def _reentry_snapshot(self, now: float,
                          lag_state: MidlineLagState) -> dict:
        statuses = {
            direction: self._reentry_status(direction, now, lag_state)
            for direction in ("sell_entropy", "buy_entropy")
        }
        blocked = [direction for direction, status in statuses.items()
                   if status["blocked"]]
        return {
            "enabled": bool(getattr(self.cfg, "reentry_guard", None)
                            and self.cfg.reentry_guard.enabled),
            "blocked": len(blocked) == len(statuses),
            "blocked_directions": blocked,
            "last_risk_exit_direction": self.cycle.last_risk_exit_direction,
            "last_risk_exit_ts": self.cycle.last_risk_exit_ts,
            "last_risk_exit_basis_bps": self.cycle.last_risk_exit_basis_bps,
            "basis_reset_seen": bool(self.cycle.reentry_reset_seen),
            "basis_reset_ts": self.cycle.reentry_reset_ts,
            "uncertainty_bps": max(
                (max(float(status.get("uncertainty_bps") or 0.0), 0.0)
                 for status in statuses.values()), default=0.0),
            "statuses": statuses,
        }

    def _maybe_mark_reentry_basis_reset(self, now: float,
                                        values: dict) -> bool:
        """Persist the neutral-basis observation required before re-entry."""
        guard = getattr(self.cfg, "reentry_guard", None)
        if (guard is None or not guard.enabled
                or not self.cycle.last_risk_exit_direction
                or self.cycle.reentry_reset_seen
                or values.get("reference_confidence", 0.0) <= 0):
            return False
        basis = _safe_float(values.get("current_premium_bps"))
        center = _safe_float(values.get("reference_center_bps"))
        if basis is None or center is None:
            return False
        if abs(basis - center) > guard.basis_reset_bps:
            return False
        self.cycle.reentry_reset_seen = True
        self.cycle.reentry_reset_ts = float(now)
        self._persist_cycle()
        log.info("[REENTRY] basis reset observed direction=%s basis=%+.3fbps "
                 "center=%+.3fbps", self.cycle.last_risk_exit_direction,
                 basis, center)
        return True

    def _register_reentry_after_risk_exit(self, record: dict,
                                          now: Optional[float] = None) -> None:
        """Save the completed risk exit as the next re-entry campaign anchor."""
        reason = str(record.get("risk_exit_type")
                     or record.get("exit_reason") or "")
        if reason == "adverse_drift_exit":
            reason = "regime_stop"
        if reason not in {"regime_stop", "max_hold", "hard_timeout",
                          "manual_flatten", "execution_emergency",
                          "margin_risk"}:
            return
        direction = self._cycle_direction_for_signal(
            str(record.get("cycle_direction") or record.get("direction") or ""))
        if direction not in InventoryCycle.VALID_DIRECTIONS:
            return
        exit_ts = _safe_float(record.get("flat_ts"))
        if exit_ts is None:
            exit_ts = time.time() if now is None else float(now)
        if (self.cycle.last_risk_exit_direction == direction
                and self.cycle.last_risk_exit_ts == exit_ts):
            return
        basis = _safe_float(record.get("risk_exit_basis_bps"))
        if basis is None:
            basis = self._current_basis_bps()
        self.cycle.last_risk_exit_direction = direction
        self.cycle.last_risk_exit_ts = exit_ts
        self.cycle.last_risk_exit_basis_bps = basis
        self.cycle.reentry_reset_seen = False
        self.cycle.reentry_reset_ts = None
        self._persist_cycle()

    @staticmethod
    def _exit_telemetry(reason: str, *, execution_emergency: bool = False,
                        risk_exit: bool = False) -> tuple[str, Optional[str], bool]:
        """Map execution reasons to mutually exclusive cycle KPI buckets."""
        reason = str(reason or "")
        if execution_emergency or reason in {
                "orphaned_inventory", "emergency_order_recovery",
                "emergency_recovery_exit", "recovery_failure"}:
            return "execution_emergency", "execution_emergency", True
        if reason == "normal_profit":
            return "normal_profit", None, False
        if reason == "fast_exit":
            return "fast_exit", None, False
        if reason == "sampling_exit":
            return "sampling_exit", None, False
        if reason == "stale_exit":
            return "stale_exit", None, False
        if reason in {"regime_stop", "regime_stop_release"}:
            return "regime_stop", "regime_stop", False
        if reason in {"adverse_drift_exit", "adverse_drift_release"}:
            return "regime_stop", "adverse_drift", False
        if reason == "pnl_loss_stop_release":
            return "risk", "pnl_loss_stop", False
        if reason in {"max_hold_exit", "max_hold"}:
            return "max_hold", "max_hold", False
        if reason in {"hard_timeout", "hard_timeout_release"}:
            return "hard_timeout", "hard_timeout", False
        if reason == "margin_risk":
            return "margin_risk", "margin_risk", True
        if reason == "manual_flatten":
            return "manual_flatten", "manual_flatten", False
        if risk_exit:
            return "execution_emergency", "execution_emergency", True
        return "normal_profit", None, False

    def _persist_cycle(self) -> None:
        if self.cfg.cycle_exit.enabled:
            safe_write_cycle_state(self.cfg.cycle_exit.state_json, self.cycle)

    def _observe_cycle_budget(self, candidate: TradeCandidate,
                              budget: dict) -> None:
        """Record hold/recovery observations without creating an order.

        The budget is evaluated on every scan, but SQLite writes are throttled
        to one per second unless the hold state changes. This keeps the
        dashboard restart-safe without turning a market-data loop into a
        persistence bottleneck.
        """
        if (candidate.action != StrategyAction.REDUCE
                or not (self.cfg.cycle_exit.enabled
                        and self.cfg.cycle_exit.mode == "fill_pnl")):
            return
        if "held_sec" not in budget:
            return
        now = time.time()
        self._last_cycle_budget = dict(budget)
        hold_state = budget.get("hold_state")
        if hold_state is not None or budget.get("allowed"):
            if hold_state != self._cycle_hold_state:
                self._cycle_hold_state_ts = now if hold_state is not None else None
            self._cycle_hold_state = hold_state
        cycle_id = self.cycle.cycle_id
        if not cycle_id or not self.cycle.active:
            return
        record = (self._cycle_telemetry
                  if self._cycle_telemetry.get("cycle_id") == cycle_id
                  else self.ledger.load_cycle(cycle_id) or {})
        if not record:
            record = {
                "cycle_id": cycle_id,
                "status": "active",
                "direction": self.cycle.direction,
                "cycle_direction": self.cycle.direction,
                "execution_ids": [],
            }
        previous_state = record.get("hold_state")
        record["hold_state"] = hold_state
        record["hold_state_ts"] = (now if hold_state != previous_state
                                    else record.get("hold_state_ts"))
        record["last_protected_pnl_usd"] = _safe_float(
            budget.get("net_profit_usd"))
        if budget.get("bootstrap_stale_hold"):
            record["bootstrap_stale_hold"] = True
        net = _safe_float(budget.get("net_profit_usd"))
        old_min = _safe_float(record.get("min_cycle_pnl_usd"))
        if net is not None:
            record["min_cycle_pnl_usd"] = (net if old_min is None
                                            else min(old_min, net))
            opened = _safe_float(record.get("open_first_fill_ts"))
            if opened is None:
                opened = self.cycle.opened_ts
            held = _safe_float(budget.get("held_sec"))
            if held is None and opened is not None:
                held = max(now - opened, 0.0)
            if net < 0 and held is not None:
                record["time_underwater_sec"] = max(
                    _safe_float(record.get("time_underwater_sec")) or 0.0,
                    held)
            if old_min is not None and old_min < 0 and held is not None:
                fast_exit_min = _safe_float(
                    budget.get("fast_exit_min_profit_usd"))
                if fast_exit_min is None:
                    fast_exit_min = self.cfg.cycle_exit.fast_exit_min_profit_usd
                if (net >= fast_exit_min
                        and record.get("time_to_recover_to_fast_sec") is None):
                    record["time_to_recover_to_fast_sec"] = held
                if (net >= self.cfg.cycle_exit.normal_min_profit_usd
                        and record.get("time_to_recover_to_profit_sec") is None):
                    record["time_to_recover_to_profit_sec"] = held
        drift = _safe_float(candidate.cycle_midline_drift_bps)
        if drift is not None:
            record["max_basis_deviation_bps"] = max(
                _safe_float(record.get("max_basis_deviation_bps")) or 0.0,
                abs(drift))
        record["updated_ts"] = now
        record["last_budget_ts"] = now
        self._cycle_telemetry = record
        state_changed = hold_state != previous_state
        if not state_changed and now - self._cycle_budget_persist_ts < 1.0:
            return
        self._cycle_budget_persist_ts = now
        try:
            self.ledger.upsert_cycle(record)
        except Exception as exc:
            self.halted = True
            log.critical("[CYCLE %s] hold telemetry write failed: %r",
                         cycle_id, exc)

    def _ensure_cycle_telemetry(self, state: ExecutionState,
                                trade: Optional[PlannedTrade] = None) -> Optional[dict]:
        """Return the durable record for the inventory cycle behind an execution."""
        action = str(state.action or "").lower()
        cycle_id = (state.cycle_id if action == StrategyAction.OPEN.value
                    else self._recovery_context.get("inventory_cycle_id")
                    or self.cycle.cycle_id)
        if not cycle_id:
            return None
        cycle_id = str(cycle_id)
        if self._cycle_telemetry.get("cycle_id") != cycle_id:
            self._cycle_telemetry = self.ledger.load_cycle(cycle_id) or {}
        now = time.time()
        if not self._cycle_telemetry:
            direction = (trade.candidate.direction if trade is not None
                         else state.direction)
            self._cycle_telemetry = {
                "cycle_id": cycle_id,
                "updated_ts": now,
                "status": "opening" if action == StrategyAction.OPEN.value
                else "active",
                "direction": direction,
                "cycle_direction": self._cycle_direction_for_signal(direction),
                "execution_ids": [],
            }
        record = self._cycle_telemetry
        record["cycle_id"] = cycle_id
        record["updated_ts"] = now
        ids = [str(value) for value in record.get("execution_ids", []) if value]
        if state.cycle_id not in ids:
            ids.append(state.cycle_id)
        record["execution_ids"] = ids
        if trade is not None:
            candidate, plan = trade.candidate, trade.plan
            budget = trade.cycle_budget or {}
            record.setdefault("direction", candidate.direction)
            record.setdefault("cycle_direction",
                              self._cycle_direction_for_signal(
                                  candidate.direction))
            record.setdefault("notional_usd", max(
                float(plan.buy_notional), float(plan.sell_notional)))
            if action == StrategyAction.OPEN.value:
                record.setdefault("policy_key", self._cycle_policy_key())
                record.setdefault("entry_projected_cycle_pnl_usd", budget.get("projected_cycle_pnl_usd"))
                record.setdefault("entry_reference_state", candidate.reference_state)
                record.setdefault(
                    "entry_mode",
                    str(budget.get("entry_mode")
                        or ("bootstrap" if self._bootstrap_active()
                            else "normal")))
                record["bootstrap_expected_pnl"] = _safe_float(
                    budget.get("bootstrap_expected_pnl",
                              budget.get("expected_cycle_pnl_usd")))
                record["bootstrap_floor_usd"] = _safe_float(
                    budget.get("bootstrap_floor_usd",
                              budget.get("economic_floor_usd")))
                record["bootstrap_exploration"] = bool(
                    budget.get("bootstrap_exploration", False))
                record["bootstrap_cost_model_source"] = budget.get(
                    "bootstrap_cost_model_source")
                record.setdefault("open_signal_ts", now)
                if budget.get("net_profit_usd") is not None:
                    record["predicted_close_pnl_usd"] = budget.get(
                        "net_profit_usd")
                    record["entry_expected_cycle_pnl_usd"] = budget.get(
                        "expected_cycle_pnl_usd", budget.get("net_profit_usd"))
                record["entry_signal_edge_bps"] = _safe_float(
                    budget.get("signal_edge_bps"))
                record["entry_expected_execution_costs_bps"] = dict(
                    budget.get("expected_execution_costs_bps") or {})
                record["entry_expected_execution_cost_usd"] = _safe_float(
                    budget.get("expected_execution_cost_usd"))
                record["entry_expected_funding_cost_usd"] = _safe_float(
                    budget.get("funding_cost_usd"))
                record["entry_expected_hold_sec"] = _safe_float(
                    budget.get("expected_hold_sec"))
                record["entry_economic_safety_margin_usd"] = _safe_float(
                    budget.get("economic_safety_margin_usd"))
                record.setdefault("exit_mode", None)
            elif action == StrategyAction.REDUCE.value:
                record["reduce_decision_ts"] = now
                if budget.get("net_profit_usd") is not None:
                    record["predicted_close_pnl_usd"] = budget.get(
                        "net_profit_usd")
                if budget.get("limit_net_profit_usd") is not None:
                    record["close_limit_pnl_usd"] = budget.get(
                        "limit_net_profit_usd")
                reason = str(budget.get("exit_reason")
                             or budget.get("reason") or "")
                exit_mode, risk_type, execution_emergency = (
                    self._exit_telemetry(
                        reason,
                        risk_exit=bool(budget.get("risk_exit")
                                       or budget.get("exit_class") == "risk")))
                record["exit_mode"] = exit_mode
                if risk_type:
                    record["risk_exit_type"] = risk_type
                    if risk_type in {"regime_stop", "max_hold", "hard_timeout",
                                     "margin_risk"}:
                        record.setdefault("risk_exit_basis_bps",
                                          self._current_basis_bps())
                record["emergency_flag"] = bool(
                    record.get("emergency_flag") or execution_emergency)
                record["exit_reason"] = reason or None
                if budget.get("net_profit_usd") is not None:
                    record["exit_expected_pnl_usd"] = budget.get(
                        "net_profit_usd")
                if budget.get("hold_state"):
                    record["hold_state"] = budget.get("hold_state")
                if budget.get("net_profit_usd") is not None:
                    record["last_protected_pnl_usd"] = budget.get(
                        "net_profit_usd")
            record["first_leg_slippage_bps"] = trade.per_leg_slippage_bps
            record["second_leg_slippage_bps"] = self._second_leg_slippage_bps(
                candidate.action)
        try:
            self.ledger.upsert_cycle(record)
        except Exception as exc:
            self.halted = True
            log.critical("[CYCLE %s] telemetry write failed: %r", cycle_id, exc)
        return record

    def _update_cycle_telemetry(self, state: ExecutionState,
                                *, outcome: Optional[str] = None) -> None:
        """Persist fill timestamps and final cycle economics."""
        record = self._ensure_cycle_telemetry(state)
        if record is None:
            return
        cycle_id = str(record.get("cycle_id") or state.cycle_id)
        now = time.time()
        action = str(state.action or "").lower()
        if state.first_filled > 0:
            if action == StrategyAction.OPEN.value:
                record.setdefault("open_first_fill_ts", now)
                record["partial_first_fill"] = bool(
                    state.first_filled + self._fill_coverage_tolerance(
                        state.planned_qty) < state.planned_qty)
            elif action == StrategyAction.REDUCE.value:
                record.setdefault("reduce_first_fill_ts", now)
        if state.hedge_filled > 0 and state.remaining_hedge <= self._step / 2:
            record.setdefault("hedge_completed_ts", now)
        record["hedge_reserve_breach"] = bool(
            record.get("hedge_reserve_breach") or state.hedge_reserve_breach)
        record["recovery_count"] = max(
            int(record.get("recovery_count") or 0),
            int(state.recovery_attempt or 0))
        if action == StrategyAction.REDUCE.value:
            budget = self._recovery_context.get("cycle_budget") or {}
            if "hedge_completion_mode" in budget:
                record["hedge_completion_mode"] = bool(
                    budget.get("hedge_completion_mode"))
            for key in ("hedge_completion_hard_cap_bps",
                        "actual_hedge_bound_px"):
                if budget.get(key) is not None:
                    record[key] = budget.get(key)
        if action in {StrategyAction.OPEN.value, StrategyAction.ADD.value}:
            budget = self._recovery_context.get("cycle_budget") or {}
            if budget.get("open_opportunity_id"):
                record["open_opportunity_id"] = budget.get(
                    "open_opportunity_id")
            campaign = self._recovery_context.get("open_campaign") or {}
            if campaign.get("open_campaign_id"):
                record["open_campaign_id"] = campaign.get(
                    "open_campaign_id")
        if action == StrategyAction.REDUCE.value:
            exit_opportunity = self._recovery_context.get(
                "exit_opportunity_id")
            if exit_opportunity:
                record["exit_opportunity_id"] = exit_opportunity
            exit_campaign = self._recovery_context.get("exit_campaign") or {}
            if exit_campaign.get("exit_campaign_id"):
                record["exit_campaign_id"] = exit_campaign.get(
                    "exit_campaign_id")
        if state.first_avg_px is not None:
            first_reference = _safe_float(
                self._recovery_context.get("buy_limit" if state.entropy_is_buy
                                           else "sell_limit"))
            if first_reference and first_reference > 0:
                record["first_leg_slippage_bps"] = (
                    (state.first_avg_px / first_reference - 1.0) * 1e4
                    if state.entropy_is_buy else
                    (1.0 - state.first_avg_px / first_reference) * 1e4)
        if state.hedge_avg_px is not None and state.hedge_limit_px:
            record["second_leg_slippage_bps"] = (
                (state.hedge_avg_px / state.hedge_limit_px - 1.0) * 1e4
                if state.hedge_is_buy else
                (1.0 - state.hedge_avg_px / state.hedge_limit_px) * 1e4)
        if state.recovery_status == RecoveryStatus.EMERGENCY_EXIT:
            mode, risk_type, _ = self._exit_telemetry(
                record.get("exit_reason") or "recovery_failure",
                execution_emergency=True)
            record["emergency_flag"] = True
            record["exit_mode"] = mode
            if risk_type:
                record["risk_exit_type"] = risk_type
        if outcome in {"flattened", "emergency_exit"}:
            mode, risk_type, _ = self._exit_telemetry(
                record.get("exit_reason") or "recovery_failure",
                execution_emergency=True)
            record["emergency_flag"] = True
            record["exit_mode"] = mode
            if risk_type:
                record["risk_exit_type"] = risk_type
        if action == StrategyAction.REDUCE.value and self._positions_flat():
            record["flat_ts"] = now
            opened = _safe_float(record.get("open_first_fill_ts"))
            if opened is not None:
                record["hold_sec"] = max(now - opened, 0.0)
            record["status"] = "completed"
            ids = record.get("execution_ids", [])
            financials = self.ledger.cycle_financials(
                ids, opened_ts=opened, until=now)
            record["fee_accounting_status"] = financials["fee_accounting_status"]
            record["accounting_status"] = "provisional"  # funding/fill completeness is separate
            record["cycle_fees_usd"] = financials["fees_usd"]
            record["total_cycle_pnl_usd"] = financials["net_pnl_usd"]
            record["realized_cycle_pnl_usd"] = financials["net_pnl_usd"]
            self._bootstrap_note_completed_cycle(record)
            try:
                execution_costs = self.ledger.execution_cost_summary(ids)
            except Exception:
                execution_costs = {}
            if execution_costs:
                record["realized_execution_costs"] = execution_costs.get(
                    "phases", {})
                record["realized_execution_cost_usd"] = execution_costs.get(
                    "total_cost_usd")
                record["realized_execution_observation_count"] = (
                    execution_costs.get("observation_count", 0))
            record["close_fee_usd"] = self.ledger.cycle_financials(
                [state.cycle_id], until=now)["fees_usd"]
            record["actual_close_fill_edge_usd"] = (
                self.ledger.execution_fill_edge(state.cycle_id))
            open_opportunity_id = (record.get("open_opportunity_id")
                                   or f"cycle:{cycle_id}:open")
            open_campaign_id = (record.get("open_campaign_id")
                                or f"cycle:{cycle_id}:campaign")
            self._record_opportunity_event(
                opportunity_id=open_opportunity_id,
                campaign_id=open_campaign_id, cycle_id=cycle_id,
                direction=record.get("direction") or state.direction,
                stage="cycle_complete", payload={"scope": "open",
                                                   "status": "flat"})
            exit_opportunity_id = (record.get("exit_opportunity_id")
                                   or f"cycle:{cycle_id}:exit")
            exit_campaign_id = (record.get("exit_campaign_id")
                                or f"{exit_opportunity_id}:campaign")
            self._record_opportunity_event(
                opportunity_id=exit_opportunity_id,
                campaign_id=exit_campaign_id, cycle_id=cycle_id,
                direction=record.get("direction") or state.direction,
                stage="cycle_complete", payload={"scope": "exit",
                                                   "status": "flat"})
            predicted = _safe_float(record.get("predicted_close_pnl_usd"))
            actual = _safe_float(record.get("total_cycle_pnl_usd"))
            entry_predicted = _safe_float(
                record.get("entry_expected_cycle_pnl_usd"))
            if entry_predicted is not None and actual is not None:
                record["prediction_error_usd"] = actual - entry_predicted
            notional = _safe_float(record.get("notional_usd"))
            if (str(record.get("exit_mode") or "")
                    in {"normal", "normal_profit"}
                    and not record.get("emergency_flag")
                    and predicted is not None and actual is not None
                    and notional is not None and notional > 0):
                realized_cost = max(predicted - actual, 0.0) / notional * 1e4
                alpha = 0.25
                self.close_cost_ewma_bps = (
                    realized_cost if self.close_cost_ewma_bps is None
                    else alpha * realized_cost
                    + (1.0 - alpha) * self.close_cost_ewma_bps)
                try:
                    self.ledger.save_model_state(
                        "close_cost_ewma",
                        {"bps": self.close_cost_ewma_bps}, ts=now)
                except Exception:
                    log.exception("[CYCLE] close-cost EWMA checkpoint failed")
            self._register_reentry_after_risk_exit(record, now)
            self._last_cycle_telemetry = dict(record)
            self._maybe_stop_after_complete_cycle(record)
        elif action == StrategyAction.OPEN.value and outcome == "paired":
            record["status"] = "active"
            record["open_fill_edge_usd"] = (
                self.ledger.execution_fill_edge(state.cycle_id))
            record["open_fee_usd"] = self.ledger.cycle_financials(
                [state.cycle_id], until=now)["fees_usd"]
        elif action == StrategyAction.OPEN.value and outcome == "no_fill":
            record["status"] = "aborted"
        elif outcome == "one_leg" and self._positions_flat():
            record["status"] = "externally_flat"
        record["updated_ts"] = now
        try:
            self.ledger.upsert_cycle(record)
        except Exception as exc:
            self.halted = True
            log.critical("[CYCLE %s] final telemetry write failed: %r",
                         record.get("cycle_id"), exc)

    def _maybe_stop_after_complete_cycle(self, record: dict) -> None:
        """Apply run loss and strict clean-circle limits while already flat."""
        limit = self.max_complete_cycles
        cycle_id = str(record.get("cycle_id") or "")
        if (not cycle_id or record.get("status") != "completed"
                or record.get("open_first_fill_ts") is None):
            return
        if cycle_id not in self._accounted_run_cycle_ids:
            self._accounted_run_cycle_ids.add(cycle_id)
            actual = _safe_float(record.get("total_cycle_pnl_usd"))
            if actual is not None:
                self.run_realized_pnl_usd += actual
            if (self.max_run_loss_usd is not None
                    and self.run_realized_pnl_usd
                    <= -self.max_run_loss_usd + 1e-12):
                log.warning("[RUN LIMIT] realized PnL $%.6f reached loss "
                            "budget $%.6f; stopping while flat",
                            self.run_realized_pnl_usd,
                            self.max_run_loss_usd)
                self.request_stop()
        if (limit is None or cycle_id in self._counted_cycle_ids
                or not is_clean_circle(record)):
            return
        self._counted_cycle_ids.add(cycle_id)
        self.completed_cycle_count += 1
        log.warning("[RUN LIMIT] complete cycle %d/%d: %s",
                    self.completed_cycle_count, limit, cycle_id)
        if self.completed_cycle_count >= limit:
            log.warning("[RUN LIMIT] reached %d complete cycles; stopping",
                        limit)
            self.request_stop()

    def _mark_hedge_reserve_breach(self, state: ExecutionState) -> None:
        """Record a close-cost reserve breach without cancelling the hedge."""
        if not state.reduce_only or state.hedge_reserve_breach:
            return
        reserved = _safe_float(
            self._recovery_context.get("cycle_budget", {}).get(
                "hedge_reserved_limit_px"))
        current, _ = self._hedge_bound_for_side(
            state.hedge_is_buy, max(float(state.first_filled), 0.0), time.time())
        if reserved is None or current is None:
            return
        breached = (current > reserved + 1e-12 if state.hedge_is_buy
                    else current < reserved - 1e-12)
        if not breached:
            return
        state.hedge_reserve_breach = True
        state.hedge_reserve_breach_ts = time.time()
        self._checkpoint_execution(
            state, event_type="hedge_reserve_breach",
            payload={"reserved_limit_px": reserved,
                     "current_limit_px": current})
        log.warning("[EXEC %s] hedge reserve breached: reserved=%s current=%s; "
                    "continuing reduce-only hedge", state.cycle_id,
                    reserved, current)

    def _sync_cycle_with_positions(self) -> None:
        """Align cycle metadata to reconciled positions without inventing an anchor."""
        if not self.cfg.cycle_exit.enabled:
            return
        state = self._pair_state()
        before = self.cycle.to_dict()
        if state == PairState.FLAT:
            self._adverse_drift_started_ts = None
            self._cycle_hold_state = None
            self._cycle_hold_state_ts = None
            if self.cfg.cycle_exit.reset_on_flat:
                self.cycle.reset()
        else:
            direction = cycle_direction_for_pair_state(state)
            if direction is None:
                self._adverse_drift_started_ts = None
                # An unbalanced pair has no valid cycle direction.  Keep the
                # venue positions as the source of truth and discard any old
                # anchor so it cannot authorize an ADD after recovery.
                self.cycle.reset()
            elif (not self.cycle.active or self.cycle.direction != direction):
                self._adverse_drift_started_ts = None
                self.cycle = InventoryCycle(
                    active=True, anchored=False, direction=direction,
                    peak_paired_qty=paired_qty(self.entropy.position,
                                               self.hedge.position))
            else:
                self.cycle.active = True
                self.cycle.peak_paired_qty = max(
                    self.cycle.peak_paired_qty,
                    paired_qty(self.entropy.position, self.hedge.position))
                if self._cycle_hold_state is None:
                    restored_hold = self._cycle_telemetry.get("hold_state")
                    if restored_hold in {
                            "loss_recovery_hold", "long_hold",
                            "forced_release_pending", "hard_timeout_release"}:
                        self._cycle_hold_state = restored_hold
                        self._cycle_hold_state_ts = _safe_float(
                            self._cycle_telemetry.get("hold_state_ts"))
        if self.cycle.to_dict() != before:
            self._persist_cycle()

    def _headroom(self, buy, sell, ref_px: float) -> float:
        hb = buy.cap_usd - buy.position * ref_px
        hs = sell.cap_usd + sell.position * ref_px
        return min(hb, hs)

    @staticmethod
    def _taker_minimums(venue, *, reduce_only: bool = False,
                        qty: Optional[float] = None) -> tuple[float, float]:
        step = 10.0 ** -int(getattr(venue, "size_decimals", 8))
        if getattr(venue, "kind", None) == "lighter":
            # Lighter trading docs: market metadata minima apply to makers.
            return step, 0.0
        base, quote = float(venue.min_base), float(venue.min_quote)
        if (getattr(venue, "kind", None) == "hl" and reduce_only
                and qty is not None and abs(venue.position) > 0
                and abs(qty - abs(venue.position)) <= step * 1e-6):
            # Submit exact full-position reduce-only dust closes to the venue;
            # partial closes and every exposure-increasing order retain minima.
            quote = 0.0
        return max(base, step), quote

    def _plan(self, buy, sell, cap_notional: float,
              threshold_bps: Optional[float] = None, *, reduce_only: bool = False):
        close_qty = min(abs(buy.position), abs(sell.position)) if reduce_only else None
        minima = [self._taker_minimums(v, reduce_only=reduce_only, qty=close_qty)
                  for v in (buy, sell)]
        if reduce_only and close_qty:
            full_buy = self._depth_quote(buy, True, close_qty)
            full_sell = self._depth_quote(sell, False, close_qty)
            if full_buy and full_sell:
                full_notional = max(full_buy[1], full_sell[1])
                if full_notional <= self.cfg.max_order_notional + 1e-8:
                    # The candidate cap is marked at mid; a one-step tail can
                    # otherwise round to zero before the later full-close fix.
                    cap_notional = max(cap_notional, full_notional)
        plan, reason = plan_arb(
            buy.book, sell.book,
            threshold_bps=(self._eff_threshold(buy, sell)
                           if threshold_bps is None else threshold_bps),
            buy_fee_bps=buy.fee_bps, sell_fee_bps=sell.fee_bps,
            take_fraction=1.0 if reduce_only else self.cfg.take_fraction,
            cap_notional=cap_notional,
            min_base=max(m[0] for m in minima) if reduce_only else self._min_base,
            min_notional=(max(m[1] for m in minima)
                          if reduce_only else self._min_notional),
            size_step=self._step,
        )
        if plan is not None and reduce_only:
            for venue, notional in ((buy, plan.buy_notional), (sell, plan.sell_notional)):
                base, quote = self._taker_minimums(venue, reduce_only=True, qty=plan.qty)
                if plan.qty + 1e-12 < base or notional + 1e-8 < quote:
                    return None, "min_notional"
        return plan, reason

    def _pair_state(self) -> PairState:
        return pair_state(self.entropy.position, self.hedge.position,
                          self._execution_flat_tolerance())

    def _margin_snapshot(self) -> dict:
        """Aggregate venue margin observations for the open/risk gates.

        Adapters publish raw equity/free collateral after each account poll.
        Test doubles and older adapters without those fields are treated as
        unavailable rather than inventing a margin ratio; the configured
        ``on_unavailable`` policy decides whether that blocks new exposure.
        """
        guard = getattr(self.cfg, "margin_risk", None)
        if guard is None or not guard.enabled:
            return {"state": "safe", "known": False,
                    "available_ratio": None, "venues": {},
                    "open_blocked": False, "hard_risk": False}
        observations = {}
        for key, venue in self.venues.items():
            if not hasattr(venue, "margin_state"):
                continue
            state = str(getattr(venue, "margin_state", "unknown") or "unknown")
            if state not in {"safe", "warning", "critical", "unknown"}:
                state = "unknown"
            ratio = _safe_float(getattr(venue, "margin_available_ratio", None))
            observations[key] = {
                "state": state,
                "available_ratio": ratio,
                "equity_usd": _safe_float(
                    getattr(venue, "margin_equity_usd", None)),
                "available_usd": _safe_float(
                    getattr(venue, "margin_available_usd", None)),
                "updated_ts": _safe_float(
                    getattr(venue, "margin_updated_ts", None)),
                "source": getattr(venue, "margin_source", None),
            }
        if not observations:
            # Compatibility with lightweight venue doubles used by offline
            # tests. A missing adapter field is not evidence of danger.
            return {"state": "safe", "known": False,
                    "available_ratio": None, "venues": {},
                    "open_blocked": False, "hard_risk": False}
        states = [item["state"] for item in observations.values()]
        state = ("critical" if "critical" in states else
                 "warning" if "warning" in states else
                 "unknown" if "unknown" in states else "safe")
        ratios = [item["available_ratio"] for item in observations.values()
                  if item["available_ratio"] is not None]
        # Warning collateral is an economic uncertainty/cost, not a safety
        # veto.  Only critical margin (or explicitly unavailable data) may
        # prevent new exposure.
        open_blocked = state == "critical" or (
            state == "unknown" and guard.on_unavailable == "block_open")
        return {
            "state": state,
            "known": all(item["state"] != "unknown"
                          for item in observations.values()),
            "available_ratio": min(ratios) if ratios else None,
            "venues": observations,
            "open_blocked": open_blocked,
            "hard_risk": state == "critical",
        }

    def _update_margin_snapshot(self, venue, equity, available) -> None:
        """Convert one account poll into a bounded margin-risk observation."""
        guard = getattr(self.cfg, "margin_risk", None)
        if guard is None or not guard.enabled:
            return
        venue.margin_equity_usd = _safe_float(equity)
        venue.margin_available_usd = _safe_float(available)
        venue.margin_updated_ts = time.time()
        venue.margin_source = (getattr(venue, "margin_source", None)
                               or "account_equity_poll")
        if venue.margin_equity_usd is None or venue.margin_equity_usd <= 0:
            venue.margin_available_ratio = None
            venue.margin_state = ("critical" if venue.margin_equity_usd is not None
                                  else "unknown")
            return
        if venue.margin_available_usd is None:
            venue.margin_available_ratio = None
            venue.margin_state = "unknown"
            return
        ratio = venue.margin_available_usd / venue.margin_equity_usd
        if not math.isfinite(ratio):
            venue.margin_available_ratio = None
            venue.margin_state = "unknown"
            return
        venue.margin_available_ratio = max(ratio, 0.0)
        venue.margin_state = (
            "critical" if ratio <= guard.critical_available_ratio else
            "warning" if ratio <= guard.warning_available_ratio else "safe")

    def _lifecycle_state(self) -> str:
        """Expose the single execution lifecycle without inferring from PnL."""
        state = self.active_execution
        if state is not None:
            if state.recovery_status == RecoveryStatus.EMERGENCY_EXIT:
                return "emergency"
            if state.phase in {ExecutionPhase.RECONCILING,
                               ExecutionPhase.FLATTENING}:
                return "recovering"
            if state.phase == ExecutionPhase.HEDGE_LEG:
                return "hedging"
            return "reducing" if state.reduce_only else "opening"
        if self._orphaned_inventory or self._emergency_pending:
            return "recovering"
        current = self._pair_state()
        if current == PairState.FLAT:
            return "flat"
        if current == PairState.UNBALANCED:
            return "recovering"
        if self._cycle_hold_state in {
                "loss_recovery_hold", "long_hold", "forced_release_pending"}:
            return self._cycle_hold_state
        return "hedged"

    def _funding_rates(self, now: float):
        """Return the last estimable rates and a bounded staleness premium."""
        if not self.cfg.funding_enabled:
            self._funding_uncertainty_bps = 0.0
            return True, 0.0, 0.0
        snapshots = (getattr(self.entropy, "funding", None),
                      getattr(self.hedge, "funding", None))
        rates = []
        ages = []
        for snapshot in snapshots:
            rate = _safe_float(getattr(snapshot, "rate_bps_per_hour", None))
            ts = _safe_float(getattr(snapshot, "ts", None))
            if rate is None or ts is None:
                self._funding_uncertainty_bps = 0.0
                return False, None, None
            rates.append(rate)
            ages.append(max(float(now) - ts, 0.0))
        stale_after = max(float(self.cfg.funding_stale_sec), 0.0)
        stale_age = max(max(ages) - stale_after, 0.0)
        # A stale snapshot remains usable, but its uncertainty is charged once
        # in whole-cycle economics instead of becoming a repeated signal gate.
        self._funding_uncertainty_bps = (
            max(0.25, min(5.0, max(abs(rate) for rate in rates) * 0.25))
            if stale_age > 0.0 else 0.0)
        return True, rates[0], rates[1]

    def _funding_costs(self, now: float):
        available, er, hr = self._funding_rates(now)
        if not available:
            return False, 0.0, 0.0
        if not self.cfg.funding_enabled:
            return True, 0.0, 0.0
        hold = self.cfg.funding_expected_hold_hours
        short = expected_pair_funding_cost_bps(er, hr, hold,
                                                short_entropy=True)
        long = expected_pair_funding_cost_bps(er, hr, hold,
                                               short_entropy=False)
        return True, short, long

    def _dashboard_strategy_values(self, now: float) -> dict:
        """Current thresholds in premium-chart coordinates.

        The strategy and recorder both consume this helper, keeping the web
        display tied to the exact live inventory/funding adjustments.
        """
        snap = self._premium_snapshot(now)
        mid = (snap.active_midline_bps if self.cfg.dynamic_midline_enabled
               else self.cfg.midline_bps)
        quality = (self._current_premium_sample_quality(now)
                   if self.cfg.dynamic_midline_enabled else None)
        lag_state = self._lag_state(snap.midline_lag_state)
        readiness = self._snapshot_readiness(snap)
        quality_tier = (quality.data_tier if quality is not None
                        else PremiumDataTier.GOOD)
        premium_data_degraded = (
            self.cfg.dynamic_midline_enabled
            and (quality_tier == PremiumDataTier.INVALID
                 or self.premium_health.is_degraded(now)))
        upper, lower = self._effective_entry_bands(snap)
        current_premium = self._current_basis_bps()
        reference_policy = self._reference_policy(
            snap, lag_state, current_premium)
        # Reference uncertainty is priced in whole-cycle economics. It does
        # not get added to the statistical threshold a second time.
        lag_penalty = 0.0
        entry_midline = reference_policy["reference_bps"]
        entry_reference_mode = reference_policy["reference_mode"]
        dynamic_center = reference_policy["dynamic_center_bps"]
        required_deviation = reference_policy["required_deviation_bps"]
        open_upper = upper
        open_lower = lower
        reference_confidence = reference_policy["reference_confidence"]
        reference_valid = (reference_policy["reference_valid"]
                           and quality_tier != PremiumDataTier.INVALID
                           and (quality is None or quality.valid))
        available, short_cost, long_cost = self._funding_costs(now)
        short_inv = self._inv_add_bps(self.hedge, self.entropy)
        long_inv = self._inv_add_bps(self.entropy, self.hedge)
        state = self._pair_state()
        margin = self._margin_snapshot()
        cycle_hold_state = (None if state == PairState.FLAT else
                            self._cycle_hold_state or
                            self._cycle_telemetry.get("hold_state"))
        volatility_guard = (
            self.cfg.adaptive_bands.enabled
            and is_volatility_guarded(
                snap.robust_sigma_bps,
                self.cfg.adaptive_bands.max_sigma_bps))
        drift = self._cycle_drift(mid, snap)
        cycle_drift_guard = (
            self.cfg.cycle_exit.enabled
            and drift is not None
            and is_cycle_add_blocked(
                drift, self.cfg.cycle_exit.max_midline_drift_bps))
        cycle_add_blocked = self._cycle_add_blocked(state, drift)
        short_exit_midline = self._cycle_exit_midline(
            "short_entropy", mid)
        long_exit_midline = self._cycle_exit_midline(
            "long_entropy", mid)
        # Entry thresholds are statistical/raw-basis coordinates.  Funding is
        # charged once by the whole-cycle economic budget and must not move
        # the signal gate or get counted again here.
        short_entry = entry_threshold_short(
            entry_midline, open_upper, short_inv)
        long_entry_signal = entry_threshold_long(
            entry_midline, open_lower, long_inv)
        raw_short_entry = short_entry
        raw_long_entry = long_entry_signal
        blocked_entry_deviation = reference_policy["dynamic_deviation_bps"]
        if blocked_entry_deviation is None:
            blocked_entry_deviation = reference_policy["signal_deviation_bps"]
        blocked_entry_required = required_deviation
        blocked_entry_allowed = bool(
            reference_valid and reference_policy["policy_allowed"])
        policy_state = reference_policy["reference_state"]
        policy_gate = policy_state in {
            "warning", "cold_degraded", "accelerating", "blocked", "shifting"
        }
        open_lag_blocked = bool(policy_gate and not blocked_entry_allowed)
        sell_edge = ((self.entropy.book.best_bid()
                      / self.hedge.book.best_ask() - 1.0) * 1e4
                     if self.entropy.book.best_bid()
                     and self.hedge.book.best_ask() else None)
        buy_edge = ((self.hedge.book.best_bid()
                     / self.entropy.book.best_ask() - 1.0) * 1e4
                    if self.hedge.book.best_bid()
                    and self.entropy.book.best_ask() else None)
        return {
            "active_midline_bps": mid,
            "short_center_bps": snap.short_center_bps,
            "long_center_bps": snap.long_center_bps,
            "sigma_center_bps": snap.sigma_center_bps,
            "robust_sigma_bps": snap.robust_sigma_bps,
            "sigma_sample_count": snap.sigma_sample_count,
            "sigma_window_sec": snap.sigma_window_sec,
            "midline_lag_bps": snap.midline_lag_bps,
            "midline_lag_state": lag_state.value,
            "midline_lag_blocked": self._lag_open_blocked(lag_state),
            "open_lag_blocked": open_lag_blocked,
            "warning_entry_penalty_bps": lag_penalty,
            "open_upper_bps": open_upper,
            "open_lower_bps": open_lower,
            "allow_sell_entropy_open": self.cfg.allow_sell_entropy_open,
            "allow_buy_entropy_open": self.cfg.allow_buy_entropy_open,
            "reference_error_bps": snap.midline_lag_bps,
            "reference_confidence": reference_confidence,
            "reference_state": reference_policy["reference_state"],
            "reference_readiness": readiness.value,
            "reference_valid": reference_valid,
            "reference_uncertainty_bps": reference_policy[
                "reference_uncertainty_bps"],
            "model_uncertainty_bps": reference_policy[
                "model_uncertainty_bps"],
            "required_sigma_mult": reference_policy["required_sigma_mult"],
            "required_deviation_bps": required_deviation,
            "signal_deviation_bps": reference_policy[
                "signal_deviation_bps"],
            "extreme_dislocation": reference_policy["extreme_dislocation"],
            "extreme_threshold_bps": reference_policy[
                "extreme_threshold_bps"],
            "entry_penalty_bps": lag_penalty,
            "reference_center_bps": entry_midline,
            "entry_reference_midline_bps": entry_midline,
            "entry_reference_mode": entry_reference_mode,
            "dynamic_center_bps": dynamic_center,
            "blocked_entry_deviation_bps": blocked_entry_deviation,
            "blocked_entry_required_bps": blocked_entry_required,
            "blocked_entry_allowed": blocked_entry_allowed,
            "current_premium_bps": current_premium,
            "effective_open_surplus_bps": {
                "sell_entropy": (None if sell_edge is None else
                                  sell_edge - raw_short_entry - lag_penalty),
                "buy_entropy": (None if buy_edge is None else
                                 buy_edge - raw_long_entry - lag_penalty),
            },
            "distance_to_threshold_bps": {
                "sell_entropy": (None if sell_edge is None else
                                  sell_edge - raw_short_entry - lag_penalty),
                "buy_entropy": (None if buy_edge is None else
                                 buy_edge - raw_long_entry - lag_penalty),
            },
            "reentry_guard": self._reentry_snapshot(now, lag_state),
            "margin_state": margin["state"],
            "margin_open_blocked": margin["open_blocked"],
            "margin_available_ratio": margin["available_ratio"],
            "margin_known": margin["known"],
            "margin_hard_risk": margin["hard_risk"],
            "margin_venues": margin["venues"],
            "midline_move_speed_bps_per_10m": (
                snap.midline_move_speed_bps_per_10m),
            # ``ready`` remains the NORMAL/full-confidence compatibility
            # flag.  The explicit tradable/readiness fields drive admission.
            "dynamic_midline_ready": snap.ready,
            "dynamic_midline_tradable": self._snapshot_tradable(snap),
            "dynamic_midline_readiness": readiness.value,
            "dynamic_midline_history_sec": snap.history_sec,
            "dynamic_midline_latest_sample_age_sec": (
                snap.latest_sample_age_sec),
            "dynamic_midline_warmup_sec": self.premium_model.warmup_sec,
            "dynamic_midline_min_samples": self.premium_model.min_samples,
            "dynamic_midline_history_source": self._model_history_source,
            "dynamic_midline_history_samples": self._model_history_samples,
            "regime_shift": snap.regime_shift,
            "effective_upper_bps": upper,
            "effective_lower_bps": lower,
            "volatility_guard": volatility_guard,
            "short_entry_bps": short_entry,
            "short_exit_bps": short_exit_midline + self.cfg.exit_bps,
            "short_reduce_threshold_bps": exit_threshold_short(
                short_exit_midline, self.cfg.exit_bps),
            "long_entry_bps": -long_entry_signal,
            "long_exit_bps": long_exit_midline - self.cfg.exit_bps,
            "long_reduce_threshold_bps": exit_threshold_long(
                long_exit_midline, self.cfg.exit_bps),
            "pair_state": state.value,
            "funding_available": available,
            "funding_on_unavailable": self.cfg.funding_on_unavailable,
            "short_entropy_cost_bps": short_cost,
            "long_entropy_cost_bps": long_cost,
            "cycle_active": self.cycle.active,
            "cycle_anchored": self.cycle.anchored,
            "cycle_direction": self.cycle.direction,
            "cycle_entry_midline_bps": self.cycle.entry_midline_bps,
            "cycle_entry_reference_bps": self.cycle.entry_reference_bps,
            "cycle_entry_reference_mode": self.cycle.entry_reference_mode,
            "cycle_entry_active_midline_bps": (
                self.cycle.entry_active_midline_bps),
            "cycle_entry_dynamic_center_bps": (
                self.cycle.entry_dynamic_center_bps),
            "cycle_entry_sigma_bps": self.cycle.entry_sigma_bps,
            "cycle_entry_band_bps": self.cycle.entry_band_bps,
            "cycle_midline_drift_bps": drift,
            "cycle_add_blocked": cycle_add_blocked,
            "cycle_drift_guard": cycle_drift_guard,
            "entropy_book_age_ms": (quality.entropy_age_ms
                                     if quality is not None else None),
            "hedge_book_age_ms": (quality.hedge_age_ms
                                   if quality is not None else None),
            "cross_venue_skew_ms": (quality.cross_venue_skew_ms
                                     if quality is not None else None),
            "premium_sample_valid": (quality.valid
                                      if quality is not None else None),
            "premium_data_tier": (quality_tier.value
                                   if quality is not None else "GOOD"),
            "premium_data_uncertainty_bps": (
                max(float(quality.uncertainty_bps or 0.0), 0.0)
                if quality is not None else 0.0),
            "premium_sample_reject_reason": (
                quality.reason if quality is not None else None),
            "premium_data_degraded": premium_data_degraded,
            "premium_data_invalid": premium_data_degraded,
            "dynamic_midline_validation_remaining_sec": max(
                self._model_restore_until - now, 0.0),
            "premium_data_invalid_duration_sec": (
                self.premium_health.invalid_duration_sec(now)
                if self.cfg.dynamic_midline_enabled else 0.0),
            "cycle_allow_add": self.cfg.cycle_allow_add,
            "cycle_max_active_pairs": self.cfg.cycle_max_active_pairs,
            "cycle_hold_state": cycle_hold_state,
            "cycle_hold_state_ts": self._cycle_hold_state_ts,
            "reentry_uncertainty_bps": max(
                float((self._reentry_snapshot(now, lag_state)
                       .get("uncertainty_bps") or 0.0)), 0.0),
            "margin_uncertainty_bps": (
                0.5 if margin.get("state") == "warning" else 0.0),
        }

    def _build_candidates(self, now: float) -> List[TradeCandidate]:
        state = self._pair_state()
        if state == PairState.UNBALANCED:
            self._record_open_blocker("unbalanced", now)
            self._skiplog("strategy blocked: unbalanced pair positions")
            return []
        values = self._dashboard_strategy_values(now)
        if self._maybe_mark_reentry_basis_reset(now, values):
            lag_state = self._lag_state(values["midline_lag_state"])
            values["reentry_guard"] = self._reentry_snapshot(now, lag_state)
        short_cost = values["short_entropy_cost_bps"]
        long_cost = values["long_entropy_cost_bps"]
        base_exposure_ok = (not values["margin_open_blocked"]
                            and not values["premium_data_degraded"]
                            and values["reference_valid"]
                            and (not self.cfg.dynamic_midline_enabled
                                 or values["dynamic_midline_tradable"]))
        # Lag/regime/volatility are signal diagnostics and soft economics.
        # Invalid data, unsafe positions and margin remain hard gates.
        add_exposure_ok = base_exposure_ok
        open_exposure_ok = base_exposure_ok
        funding_open_ok = (values["funding_available"]
                           or self.cfg.funding_on_unavailable != "block_open")
        out: List[TradeCandidate] = []
        if state == PairState.FLAT:
            policy_reason = self._open_policy_block_reason(values, state)
            if policy_reason is not None:
                self._record_open_blocker(policy_reason, now)
                return []

        def add(direction, action, buy, sell, threshold, cap, reduce,
                cost=0.0, band=None):
            is_open = action == StrategyAction.OPEN
            if is_open:
                band = (values["open_upper_bps"]
                        if direction == "sell_entropy"
                        else values["open_lower_bps"])
            entry_midline = (values["entry_reference_midline_bps"]
                             if is_open
                             else values["cycle_entry_reference_bps"])
            entry_sigma = (values["robust_sigma_bps"]
                           if is_open
                           else values["cycle_entry_sigma_bps"])
            out.append(TradeCandidate(
                direction, action, buy, sell, threshold, max(cap, 0.0),
                reduce, cost, state, band, entry_midline, entry_sigma,
                values["cycle_midline_drift_bps"],
                values["volatility_guard"], values["cycle_drift_guard"],
                (values["entry_reference_mode"] if is_open else
                 values["cycle_entry_reference_mode"]),
                (values["active_midline_bps"] if is_open else
                 values["cycle_entry_active_midline_bps"]),
                (values["dynamic_center_bps"] if is_open else
                 values["cycle_entry_dynamic_center_bps"]),
                values["reference_state"] if is_open else None,
                values["reference_confidence"] if is_open else None,
                values["required_deviation_bps"] if is_open else None,
                values["signal_deviation_bps"] if is_open else None,
                values["extreme_dislocation"] if is_open else False))

        if state == PairState.SHORT_ENTROPY:
            qty = paired_qty(self.entropy.position, self.hedge.position)
            ref = self.entropy.book.mid() or self.hedge.book.mid() or 0.0
            add("buy_entropy", StrategyAction.REDUCE, self.entropy, self.hedge,
                values["short_reduce_threshold_bps"],
                min(self.cfg.max_order_notional, qty * ref), True,
                band=values["effective_upper_bps"])
            if (self.cfg.cycle_allow_add and add_exposure_ok
                    and funding_open_ok
                    and self.cfg.allow_sell_entropy_open
                    and not values["cycle_add_blocked"]):
                add("sell_entropy", StrategyAction.ADD, self.hedge, self.entropy,
                    values["short_entry_bps"],
                    self.cfg.max_order_notional, False, short_cost,
                    values["effective_upper_bps"])
        elif state == PairState.LONG_ENTROPY:
            qty = paired_qty(self.entropy.position, self.hedge.position)
            ref = self.entropy.book.mid() or self.hedge.book.mid() or 0.0
            add("sell_entropy", StrategyAction.REDUCE, self.hedge, self.entropy,
                values["long_reduce_threshold_bps"],
                min(self.cfg.max_order_notional, qty * ref), True,
                band=values["effective_lower_bps"])
            if (self.cfg.cycle_allow_add and add_exposure_ok
                    and funding_open_ok
                    and self.cfg.allow_buy_entropy_open
                    and not values["cycle_add_blocked"]):
                add("buy_entropy", StrategyAction.ADD, self.entropy, self.hedge,
                    -values["long_entry_bps"],
                    self.cfg.max_order_notional, False, long_cost,
                    values["effective_lower_bps"])
        elif open_exposure_ok:
            if funding_open_ok and self.cfg.allow_sell_entropy_open:
                add("sell_entropy", StrategyAction.OPEN, self.hedge, self.entropy,
                    values["short_entry_bps"],
                    self.cfg.max_order_notional, False, short_cost,
                    values["effective_upper_bps"])
            if funding_open_ok and self.cfg.allow_buy_entropy_open:
                add("buy_entropy", StrategyAction.OPEN, self.entropy, self.hedge,
                    -values["long_entry_bps"],
                    self.cfg.max_order_notional, False, long_cost,
                    values["effective_lower_bps"])
        if state == PairState.FLAT:
            if out:
                self._record_open_blocker(None, now)
        return out

    @staticmethod
    def _depth_quote(venue, is_buy: bool, qty: float):
        levels = (venue.book.sorted_asks() if is_buy
                  else venue.book.sorted_bids())
        if not levels or sum(sz for _, sz in levels) + 1e-12 < qty:
            return None
        return walk_depth(levels, qty)

    def _record_hedge_quote(self, now: Optional[float] = None) -> None:
        """Keep a bounded top-of-book history for close-hedge risk pricing."""
        if self.hedge is None or not hasattr(self.hedge, "book"):
            return
        bid = _safe_float(self.hedge.book.best_bid())
        ask = _safe_float(self.hedge.book.best_ask())
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return
        ts = time.time() if now is None else float(now)
        if not math.isfinite(ts):
            return
        if self._last_hedge_quote_ts is not None and ts < self._last_hedge_quote_ts:
            return
        row = {"ts": ts, "bid": bid, "ask": ask,
               "mid": (bid + ask) / 2.0}
        if (self._hedge_quote_history
                and ts == self._hedge_quote_history[-1]["ts"]):
            self._hedge_quote_history[-1] = row
        else:
            self._hedge_quote_history.append(row)
        self._last_hedge_quote_ts = ts

    def _record_open_quote(self, now: Optional[float] = None) -> None:
        """Keep a bounded Entropy quote history for OPEN latency risk."""
        if self.entropy is None or not hasattr(self.entropy, "book"):
            return
        bid = _safe_float(self.entropy.book.best_bid())
        ask = _safe_float(self.entropy.book.best_ask())
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return
        ts = time.time() if now is None else float(now)
        if not math.isfinite(ts):
            return
        if self._last_open_quote_ts is not None and ts < self._last_open_quote_ts:
            return
        row = {"ts": ts, "bid": bid, "ask": ask,
               "mid": (bid + ask) / 2.0}
        if (self._open_quote_history
                and ts == self._open_quote_history[-1]["ts"]):
            self._open_quote_history[-1] = row
        else:
            self._open_quote_history.append(row)
        self._last_open_quote_ts = ts

    def _open_latency_horizon_sec(self, percentile: float = 0.90) -> float:
        """Return a bounded latency horizon for the next Entropy quote."""
        samples = sorted(
            value for value in self._open_latency_samples_sec
            if math.isfinite(value) and 0.05 <= value <= 5.0)
        if len(samples) >= 3:
            return (_nearest_percentile(samples, percentile)
                    or samples[-1])
        return min(max(float(self._open_latency_sec_ewma), 0.05), 5.0)

    @staticmethod
    def _latency_adverse_samples(history: deque, is_buy: bool,
                                  latency: float, now: float,
                                  window: float) -> list[float]:
        """Build a complete adverse-move sample set, including zeroes."""
        rows = sorted(
            (row for row in history
             if 0 <= now - float(row.get("ts", 0.0)) <= window),
            key=lambda row: float(row.get("ts", 0.0)))
        moves: list[float] = []
        future_index = 1
        for index, previous in enumerate(rows[:-1]):
            future_index = max(future_index, index + 1)
            target_ts = float(previous["ts"]) + latency
            while (future_index < len(rows)
                   and float(rows[future_index]["ts"]) < target_ts):
                future_index += 1
            if future_index >= len(rows):
                break
            current = rows[future_index]
            if float(current["ts"]) <= float(previous["ts"]):
                continue
            try:
                if is_buy:
                    move = (float(current["ask"]) /
                            float(previous["ask"]) - 1.0) * 1e4
                else:
                    move = (float(previous["bid"]) /
                            float(current["bid"]) - 1.0) * 1e4
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                continue
            if math.isfinite(move):
                # A favourable move proves the window was observed, but must
                # not create a negative future reserve.
                moves.append(max(move, 0.0))
        return moves

    def _open_adverse_move(self, is_buy: bool, percentile: float,
                           *, latency_percentile: float,
                           now: Optional[float] = None) -> float:
        now = time.time() if now is None else float(now)
        latency = self._open_latency_horizon_sec(latency_percentile)
        window = max(10.0, latency * 8.0)
        moves = self._latency_adverse_samples(
            self._open_quote_history, is_buy, latency, now, window)
        return max(_nearest_percentile(moves, percentile) or 0.0, 0.0)

    def _open_adverse_move_economic_p75(self, is_buy: bool,
                                        now: Optional[float] = None) -> float:
        """Economic latency adverse movement (all windows, P75)."""
        return self._open_adverse_move(
            is_buy, .75, latency_percentile=.75, now=now)

    def _open_adverse_move_economic_p50(self, is_buy: bool,
                                        now: Optional[float] = None) -> float:
        """Bootstrap economic latency adverse movement (P50/P50)."""
        return self._open_adverse_move(
            is_buy, .50, latency_percentile=.50, now=now)

    def _open_adverse_move_stress_p95(self, is_buy: bool,
                                      now: Optional[float] = None) -> float:
        """Physical latency stress reserve (all windows, P95)."""
        return self._open_adverse_move(
            is_buy, .95, latency_percentile=.95, now=now)

    def _open_adverse_move_stress_p99(self, is_buy: bool,
                                      now: Optional[float] = None) -> float:
        """Physical latency stress diagnostic (all windows, P99)."""
        return self._open_adverse_move(
            is_buy, .99, latency_percentile=.95, now=now)

    def _open_adverse_move_p90(self, is_buy: bool,
                               now: Optional[float] = None) -> float:
        """Compatibility alias for the physical P95 stress reserve."""
        return self._open_adverse_move_stress_p95(is_buy, now)

    def _open_first_leg_metrics(self, candidate: TradeCandidate,
                                plan: ArbPlan,
                                now: Optional[float] = None) -> dict:
        """Return the dynamic Entropy first-leg bound and its audit inputs."""
        now = time.time() if now is None else float(now)
        self._record_open_quote(now)
        latency_horizon = self._open_latency_horizon_sec(.75)
        stress_latency_horizon = self._open_latency_horizon_sec(.95)
        entropy_is_buy = candidate.buy is self.entropy
        reference = plan.buy_limit if entropy_is_buy else plan.sell_limit
        fixed = self._first_leg_slippage_bps(candidate.action)
        policy = getattr(self.cfg, "reduce_execution", None)
        dynamic = getattr(policy, "dynamic_open_first_leg", None)
        enabled = bool(dynamic is not None and dynamic.enabled
                       and candidate.action in {
                           StrategyAction.OPEN, StrategyAction.ADD})
        top = (_safe_float(self.entropy.book.best_ask()) if entropy_is_buy
               else _safe_float(self.entropy.book.best_bid()))
        room_min_bps = self._open_execution_room_min_bps()
        if reference is None or reference <= 0 or not math.isfinite(reference):
            return {"enabled": enabled, "bound_px": None,
                    "first_leg_room_bps": 0.0,
                    "latency_estimate_sec": latency_horizon,
                    "latency_horizon_sec": latency_horizon,
                    "stress_latency_horizon_sec": stress_latency_horizon,
                    "adverse_move_economic_p75_bps": 0.0,
                    "adverse_move_stress_p95_bps": 0.0,
                    "adverse_move_stress_p99_bps": 0.0,
                    "adverse_move_p90_bps": 0.0,
                    "adverse_move_reserve_bps": fixed,
                    "latency_required_room_bps": room_min_bps,
                    "safety_margin_bps": 0.0, "top_px": top}

        if not enabled:
            reserve = max(float(fixed), 0.0)
            raw = (reference * (1.0 + reserve / 1e4)
                   if entropy_is_buy else
                   reference * (1.0 - reserve / 1e4))
            bound = self.entropy.px_round(raw, round_up=not entropy_is_buy)
            return {
                "enabled": False, "bound_px": bound,
                "first_leg_room_bps": abs(bound / reference - 1.0) * 1e4,
                "latency_estimate_sec": latency_horizon,
                "latency_horizon_sec": latency_horizon,
                "stress_latency_horizon_sec": stress_latency_horizon,
                "adverse_move_economic_p75_bps": 0.0,
                "adverse_move_stress_p95_bps": 0.0,
                "adverse_move_stress_p99_bps": 0.0,
                "adverse_move_p90_bps": 0.0,
                "adverse_move_reserve_bps": reserve,
                "latency_required_room_bps": room_min_bps,
                "safety_margin_bps": 0.0, "top_px": top,
                "source": "fixed_plan_bound",
            }

        economic_adverse = self._open_adverse_move_economic_p75(
            entropy_is_buy, now)
        adverse = self._open_adverse_move_stress_p95(entropy_is_buy, now)
        stress_p99 = self._open_adverse_move_stress_p99(entropy_is_buy, now)
        reserve = min(max(
            adverse + max(float(dynamic.safety_margin_bps), 0.0),
            float(dynamic.min_bps)), float(dynamic.max_bps))
        latency_required = max(
            economic_adverse + max(float(dynamic.safety_margin_bps), 0.0),
            room_min_bps,
            0.0)
        base = top if top is not None and top > 0 else reference
        raw = (base * (1.0 + reserve / 1e4) if entropy_is_buy
               else base * (1.0 - reserve / 1e4))
        dynamic_bound = self.entropy.px_round(
            raw, round_up=not entropy_is_buy)
        # The plan's marginal level is still required for the requested
        # quantity; the dynamic quote may only add latency room on top of it.
        bound = (max(reference, dynamic_bound) if entropy_is_buy
                 else min(reference, dynamic_bound))
        return {
            "enabled": True, "bound_px": bound,
            "first_leg_room_bps": abs(bound / reference - 1.0) * 1e4,
            "latency_estimate_sec": latency_horizon,
            "latency_horizon_sec": latency_horizon,
            "stress_latency_horizon_sec": stress_latency_horizon,
            "adverse_move_economic_p75_bps": economic_adverse,
            "adverse_move_stress_p95_bps": adverse,
            "adverse_move_stress_p99_bps": stress_p99,
            "adverse_move_p90_bps": adverse,
            "adverse_move_reserve_bps": reserve,
            "latency_required_room_bps": latency_required,
            "safety_margin_bps": dynamic.safety_margin_bps,
            "top_px": top, "raw_bound_px": raw,
            "source": "top_plus_stress_p95",
        }

    def _build_pnl_budgeted_open_bound(self, candidate: TradeCandidate,
                                       plan: ArbPlan, economics: dict,
                                       first: dict, now: float) -> Optional[dict]:
        """Build an Entropy first-leg bound from expected whole-cycle PnL.

        ``economics`` uses expected p75/p90 costs.  The physical cap is a
        separate outer limit used only to stop the IOC from crossing an
        unsafe price after a volatile move.
        """
        expected_pnl = _safe_float(economics.get("net_profit_usd"))
        floor = _safe_float(economics.get("economic_floor_usd"))
        if floor is None:
            floor = self._economic_floor_usd(
                max(float(plan.buy_notional), float(plan.sell_notional), 0.0))
        if expected_pnl is None:
            return None
        notional = max(float(plan.buy_notional), float(plan.sell_notional), 0.0)
        qty = float(plan.qty)
        if qty <= 0 or notional <= 0 or expected_pnl < floor - 1e-10:
            return None
        production_floor = _safe_float(economics.get("production_floor_usd"))
        if production_floor is None:
            production_floor = self._economic_floor_usd(notional)
        sampling_floor = _safe_float(economics.get("sampling_floor_usd"))
        if sampling_floor is None:
            sampling_floor = floor
        bootstrap_active = bool(economics.get("bootstrap_active"))
        bootstrap_yellow = bool(
            bootstrap_active and expected_pnl < production_floor - 1e-10
            and expected_pnl >= sampling_floor - 1e-10)
        room_floor = production_floor if not bootstrap_yellow else sampling_floor
        entropy_is_buy = candidate.buy is self.entropy
        reference = plan.buy_limit if entropy_is_buy else plan.sell_limit
        if reference is None or reference <= 0:
            return None
        price_room_usd = max(expected_pnl - room_floor, 0.0)
        entropy_fee = plan.buy_fee if entropy_is_buy else plan.sell_fee
        if entropy_is_buy:
            raw_economic = reference + price_room_usd / (qty * (1.0 + entropy_fee))
        else:
            raw_economic = reference - price_room_usd / (qty * (1.0 - entropy_fee))
        if not math.isfinite(raw_economic) or raw_economic <= 0:
            return None
        economic_bound = self.entropy.px_round(
            raw_economic, round_up=not entropy_is_buy)
        physical_hard_cap_bps = self._execution_hard_cap_bps(
            candidate.action, "first")
        physical_hard_cap = self._live_taker_bound(
            self.entropy, entropy_is_buy, physical_hard_cap_bps)
        dynamic_bound = _safe_float(first.get("bound_px"))
        # The dynamic first-leg model describes the minimum room required to
        # survive the observed submit-to-result latency.  It is not an
        # economic price ceiling/floor and must never clip a profitable
        # economic bound.  Keep it as a diagnostic and admission requirement.
        latency_required_room_bps = max(
            _safe_float(first.get("latency_required_room_bps")) or 0.0,
            self._open_execution_room_min_bps(
                bootstrap_active=bool(economics.get("bootstrap_active"))),
            0.0)
        execution_quote = self._quote_snapshot(
            self.entropy, entropy_is_buy, qty)
        current_executable = (_safe_float(execution_quote.get("marginal_px"))
                              if execution_quote else None)
        if current_executable is None or current_executable <= 0:
            current_executable = reference
        # Keep the dynamic reserve visible in telemetry.  It is used by the
        # latency admission check above, not as another price cap.
        stress_bps = max(
            _safe_float(first.get("adverse_move_reserve_bps"))
            or _safe_float(first.get("adverse_move_stress_p95_bps"))
            or _safe_float(first.get("adverse_move_p90_bps")) or 0.0, 0.0)
        stress_raw = (current_executable * (1.0 + stress_bps / 1e4)
                      if entropy_is_buy else
                      current_executable * (1.0 - stress_bps / 1e4))
        stress_bound = self.entropy.px_round(
            stress_raw, round_up=not entropy_is_buy)
        if physical_hard_cap is None:
            return None
        # Stress is an admission/diagnostic signal.  It must not narrow the
        # economic IOC room; only the absolute hard cap is a physical limit.
        physical_bound = physical_hard_cap
        selection = _choose_open_execution_limit(
            economic_bound=economic_bound,
            physical_bound=physical_bound,
            current_executable=current_executable,
            # Require enough executable room for the observed submit latency;
            # neither the economic nor physical bound may be widened here.
            latency_required_room_bps=latency_required_room_bps,
            is_buy=entropy_is_buy)
        affordable_room_before_sampling = max(
            _safe_float(selection.get("room_bps")) or 0.0, 0.0)
        sampling_target = 0.0
        sampling_cap = 0.0
        requested_room = affordable_room_before_sampling
        if bootstrap_yellow:
            bootstrap_cfg = self._bootstrap_cfg()
            sampling_target = max(float(getattr(
                bootstrap_cfg, "open_target_execution_room_bps", 3.0)), 0.0)
            sampling_cap = max(float(getattr(
                bootstrap_cfg, "open_sampling_execution_cap_bps", 5.0)), 0.0)
            requested_room = min(
                max(affordable_room_before_sampling, sampling_target),
                sampling_cap, physical_hard_cap_bps)
            if requested_room > 0.0:
                sampling_raw = (current_executable *
                                (1.0 + requested_room / 1e4)
                                if entropy_is_buy else
                                current_executable *
                                (1.0 - requested_room / 1e4))
                sampling_bound = self.entropy.px_round(
                    sampling_raw, round_up=not entropy_is_buy)
                selection = _choose_open_execution_limit(
                    economic_bound=sampling_bound,
                    physical_bound=physical_bound,
                    current_executable=current_executable,
                    latency_required_room_bps=latency_required_room_bps,
                    is_buy=entropy_is_buy)
        if not selection.get("allowed"):
            return None
        entropy_limit = _safe_float(selection.get("limit_px"))
        if entropy_limit is None or entropy_limit <= 0:
            return None
        marketable = bool(selection.get("marketable"))
        first_leg_execution_room = max(
            _safe_float(selection.get("room_bps")) or 0.0, 0.0)
        hold_sec = _safe_float(economics.get("expected_hold_sec"))
        if hold_sec is None:
            hold_fn = getattr(self, "_expected_cycle_hold_sec", None)
            try:
                hold_sec = float(hold_fn()) if callable(hold_fn) else 1.0
            except (AttributeError, TypeError, ValueError):
                hold_sec = 1.0
        optimizer = choose_execution_room(
            phase="open",
            affordable_room_bps=first_leg_execution_room,
            hard_cap_bps=physical_hard_cap_bps,
            fill_stats=getattr(self, "_open_fill_probability", {}),
            base_cycle_pnl_bps=(
                expected_pnl / notional * 1e4
                + (_safe_float((economics.get(
                    "expected_execution_costs_bps") or {}).get(
                        "open_first")) or 0.0)
                if notional > 0 else 0.0),
            capital_lock_sec=hold_sec,
            expected_incremental_cost_bps=(
                _safe_float((economics.get(
                    "expected_execution_costs_bps") or {}).get(
                        "open_first")) or 0.0))
        optimizer_room = max(float(optimizer.get("room_bps") or 0.0), 0.0)
        # The optimizer may spend less than the available room, but the
        # bootstrap sampling target is a lower bound for a YELLOW OPEN.  Keep
        # the requested room bounded before converting it back to a tick-sized
        # price; the final telemetry is always recomputed from that price.
        minimum_room = min(
            max(latency_required_room_bps,
                sampling_target if bootstrap_yellow else 0.0),
            requested_room)
        selected_room_request = min(
            max(optimizer_room, minimum_room), requested_room)

        def _bound_at_room(room_bps: float) -> float:
            raw = (current_executable * (1.0 + room_bps / 1e4)
                   if entropy_is_buy else
                   current_executable * (1.0 - room_bps / 1e4))
            return self.entropy.px_round(raw, round_up=not entropy_is_buy)

        # The original selection is the outer economic/sampling bound.  A
        # repriced optimizer result can only be less aggressive than it, and
        # it can never cross the independent physical cap.
        selected_bound = _bound_at_room(selected_room_request)
        if entropy_is_buy:
            entropy_limit = min(entropy_limit, selected_bound,
                                physical_bound)
        else:
            entropy_limit = max(entropy_limit, selected_bound,
                                physical_bound)
        marketable = (entropy_limit + 1e-12 >= current_executable
                      if entropy_is_buy else
                      entropy_limit - 1e-12 <= current_executable)
        first_leg_execution_room = max(
            ((entropy_limit / current_executable - 1.0) * 1e4
             if entropy_is_buy else
             (current_executable / entropy_limit - 1.0) * 1e4), 0.0)
        # ``selected_room`` is deliberately the realised, tick-sized room;
        # keeping a separate request above makes the cap/target audit clear.
        selected_room = first_leg_execution_room
        # Reprice the hedge with its physical bound for the actual order
        # path. Its expected bound is used to expose the economic reserve.
        hedge_is_buy = not entropy_is_buy
        open_hedge_hard_cap = self._execution_hard_cap_bps(
            candidate.action, "hedge")
        hedge_limit, hedge_physical = self._hedge_bound_for_side(
            hedge_is_buy, qty, now, expected=False, phase="open_hedge",
            physical_max_bps=open_hedge_hard_cap)
        hedge_expected, hedge_expected_metrics = self._hedge_bound_for_side(
            hedge_is_buy, qty, now, expected=True, phase="open_hedge",
            physical_max_bps=open_hedge_hard_cap)
        if hedge_limit is None or hedge_expected is None:
            return None
        buy_bound = entropy_limit if entropy_is_buy else hedge_limit
        sell_bound = hedge_limit if entropy_is_buy else entropy_limit
        if buy_bound <= 0 or sell_bound <= 0:
            return None
        protected_edge = ((sell_bound * (1.0 - plan.sell_fee))
                          / (buy_bound * (1.0 + plan.buy_fee)) - 1.0) * 1e4
        economic_buy = economic_bound if entropy_is_buy else hedge_expected
        economic_sell = hedge_expected if entropy_is_buy else entropy_limit
        economic_edge = ((economic_sell * (1.0 - plan.sell_fee))
                         / (economic_buy * (1.0 + plan.buy_fee)) - 1.0) * 1e4
        economic_surplus_bps = max(expected_pnl - floor, 0.0) \
            / notional * 1e4
        return {
            **first,
            "entropy_limit_px": entropy_limit,
            "economic_entropy_limit_px": economic_bound,
            "physical_entropy_limit_px": physical_bound,
            "physical_stress_bound_px": stress_bound,
            "physical_hard_cap_px": physical_hard_cap,
            "hedge_limit_px": hedge_limit,
            "economic_hedge_limit_px": hedge_expected,
            "physical_hedge_limit_px": hedge_limit,
            "hedge_expected_reserve_bps": hedge_expected_metrics.get(
                "expected_reserve_bps"),
            "hedge_physical_reserve_bps": hedge_physical.get("reserve_bps"),
            "physical_hedge_hard_cap_bps": open_hedge_hard_cap,
            "buy_bound_px": buy_bound,
            "sell_bound_px": sell_bound,
            "signal_edge_bps": plan.marginal_premium_bps,
            "quoted_edge_bps": ((plan.sell_limit * (1.0 - plan.sell_fee)
                                 / (plan.buy_limit * (1.0 + plan.buy_fee))
                                 - 1.0) * 1e4),
            "protected_open_edge_bps": protected_edge,
            "economic_open_edge_bps": economic_edge,
            "required_entry_edge_bps": (
                candidate.threshold_bps + self.cfg.min_edge_surplus_bps),
            "execution_room_bps": first_leg_execution_room,
            "first_leg_room_bps": first_leg_execution_room,
            "first_leg_execution_room_bps": first_leg_execution_room,
            "physical_execution_room_bps": first_leg_execution_room,
            "latency_required_room_bps": latency_required_room_bps,
            "optimized_execution_room_bps": selected_room,
            "optimizer_requested_execution_room_bps": selected_room_request,
            "affordable_room_before_sampling_bps": (
                affordable_room_before_sampling),
            "sampling_target_execution_room_bps": sampling_target,
            "sampling_cap_execution_room_bps": sampling_cap,
            "sampling_yellow": bootstrap_yellow,
            "bootstrap_yellow": bootstrap_yellow,
            "production_floor_usd": production_floor,
            "sampling_floor_usd": sampling_floor,
            "room_floor_usd": room_floor,
            "optimizer_fill_probability": optimizer.get("fill_probability"),
            "optimizer_score": optimizer.get("score"),
            "optimizer_expected_incremental_cost_bps": optimizer.get(
                "expected_incremental_cost_bps"),
            "optimizer_bucket": optimizer.get("bucket"),
            "optimizer_reason": optimizer.get("reason"),
            "economic_surplus_bps": economic_surplus_bps,
            "economic_room_bps": economic_surplus_bps,
            "expected_cycle_pnl_usd": expected_pnl,
            "economic_floor_usd": floor,
            "available_budget_usd": price_room_usd,
            "economic_surplus_usd": price_room_usd,
            "entry_mode": economics.get("entry_mode", "normal"),
            "bootstrap_active": bool(economics.get("bootstrap_active")),
            "bootstrap_exploration": bool(
                economics.get("bootstrap_exploration")),
            "bootstrap_gross_after_fee_bps": economics.get(
                "bootstrap_gross_after_fee_bps"),
            "bootstrap_gross_ok": economics.get("bootstrap_gross_ok", True),
            "bootstrap_cost_model_source": economics.get(
                "bootstrap_cost_model_source"),
            "economic_bound_marketable": bool(marketable),
            "economic_bound_not_marketable": not marketable,
            "physical_hard_cap_bps": physical_hard_cap_bps,
            "open_execution_room_min_bps": (
                self._open_execution_room_min_bps(
                    bootstrap_active=bool(economics.get(
                        "bootstrap_active")))),
            "expected_open_first_cost_bps": _safe_float(
                (economics.get("expected_execution_costs_bps") or {}).get(
                    "open_first")),
            "expected_open_hedge_cost_bps": _safe_float(
                (economics.get("expected_execution_costs_bps") or {}).get(
                    "open_hedge")),
            "expected_close_first_cost_bps": _safe_float(
                (economics.get("expected_execution_costs_bps") or {}).get(
                    "close_first")),
            "expected_close_hedge_cost_bps": _safe_float(
                (economics.get("expected_execution_costs_bps") or {}).get(
                    "close_hedge")),
            "expected_funding_cost_usd": _safe_float(
                economics.get("funding_cost_usd")),
            "economic_entropy_bound_px": economic_bound,
            "physical_entropy_bound_px": physical_bound,
            "actual_entropy_limit_px": entropy_limit,
            "expected_hedge_bound_px": hedge_expected,
            "physical_hedge_bound_px": hedge_limit,
            "current_entropy_executable_px": current_executable,
            "dynamic_bound_px": dynamic_bound,
            "latency_bound_px": dynamic_bound,
        }

    def _protected_open_metrics(self, candidate: TradeCandidate,
                                plan: ArbPlan,
                                now: Optional[float] = None) -> Optional[dict]:
        """Recheck OPEN economics using expected costs and physical bounds."""
        now = time.time() if now is None else float(now)
        first = self._open_first_leg_metrics(candidate, plan, now)
        economics = self._cycle_profit_budget(candidate, plan)
        result = self._build_pnl_budgeted_open_bound(
            candidate, plan, economics, first, now)
        if result is None:
            # Keep a diagnostic payload even when the economic bound cannot be
            # built; the caller supplies the final blocker reason.
            result = dict(first)
            economic_floor = _safe_float(
                economics.get("economic_floor_usd"))
            if economic_floor is None:
                economic_floor = self._economic_floor_usd(
                    max(float(plan.buy_notional), float(plan.sell_notional), 0.0))
            result.update(
                expected_cycle_pnl_usd=_safe_float(
                    economics.get("net_profit_usd")),
                economic_floor_usd=economic_floor,
                economic_bound_marketable=False,
                economic_bound_not_marketable=True,
                entry_mode=economics.get("entry_mode", "normal"),
                bootstrap_active=bool(economics.get("bootstrap_active")),
                bootstrap_exploration=bool(
                    economics.get("bootstrap_exploration")),
                bootstrap_gross_after_fee_bps=economics.get(
                    "bootstrap_gross_after_fee_bps"),
                bootstrap_gross_ok=economics.get("bootstrap_gross_ok", True),
                bootstrap_cost_model_source=economics.get(
                    "bootstrap_cost_model_source"))
        result["economic_costs"] = economics.get(
            "expected_execution_costs_bps", {})
        result["economic_cost_details"] = economics.get(
            "expected_execution_cost_details", {})
        result["economic_execution_cost_usd"] = economics.get(
            "expected_execution_cost_usd", 0.0)
        result["economic_required_dislocation_bps"] = economics.get(
            "economic_required_dislocation_bps")
        result["effective_entry_distance_bps"] = economics.get(
            "effective_entry_distance_bps")
        return result

    def _open_opportunity_key(self, candidate: TradeCandidate) -> str:
        # The opportunity identity is the directional signal itself. Basis
        # buckets are deliberately not part of it: a sustained trend must
        # remain locked until it returns through the reset zone.
        return str(candidate.direction)

    def _open_signal_distance_bps(
            self, candidate: TradeCandidate,
            threshold_bps: Optional[float] = None,
            basis_bps: Optional[float] = None) -> Optional[float]:
        threshold = (_safe_float(candidate.threshold_bps)
                     if threshold_bps is None else _safe_float(threshold_bps))
        if basis_bps is None:
            basis_bps = _safe_float(self._current_basis_bps())
        if threshold is None or basis_bps is None:
            return None
        if candidate.direction == "sell_entropy":
            return basis_bps - threshold
        if candidate.direction == "buy_entropy":
            # ``buy_entropy`` stores the positive deviation magnitude in
            # ``candidate.threshold_bps`` while its signal is on the
            # negative-basis side. Keep the opportunity state in one
            # direction-oriented coordinate so neutral basis correctly
            # resets a completed long-side opportunity.
            return -basis_bps - threshold
        return None

    def _update_open_opportunity_state(self, candidate: TradeCandidate,
                                       opportunity: dict,
                                       now: float) -> None:
        """Advance the directional OPEN opportunity through its reset zone."""
        basis = _safe_float(self._current_basis_bps())
        threshold = _safe_float(opportunity.get("entry_threshold_bps"))
        if threshold is None:
            threshold = _safe_float(candidate.threshold_bps)
        distance = self._open_signal_distance_bps(
            candidate, threshold_bps=threshold, basis_bps=basis)
        if basis is None or threshold is None or distance is None:
            return
        reset_margin = max(float(opportunity.get(
            "reset_margin_bps", self.OPEN_SIGNAL_RESET_BPS)), 0.0)
        reset_threshold = (threshold - reset_margin
                           if candidate.direction == "sell_entropy"
                           else -threshold + reset_margin)
        opportunity.update(
            last_seen_ts=now,
            last_basis_bps=basis,
            signal_distance_bps=distance,
            reset_threshold_bps=reset_threshold,
        )
        if distance <= -reset_margin:
            opportunity.update(
                reset_required=True,
                state="ready",
                reset_seen_ts=now,
            )

    def _mark_open_signal_reset(self, key: str,
                                candidate: Optional[TradeCandidate] = None,
                                now: Optional[float] = None) -> None:
        opportunity = self._open_opportunities.get(key)
        if opportunity is None:
            return
        if candidate is not None:
            self._update_open_opportunity_state(
                candidate, opportunity,
                time.time() if now is None else float(now))
        else:
            # Keep direct integrations source-compatible. Callers that have a
            # candidate use the reset-zone check above; an explicit legacy
            # reset call is already an assertion that the signal disappeared.
            opportunity["reset_required"] = True
            opportunity["cooldown_until"] = 0.0

    def _record_opportunity_event(self, *, opportunity_id: Optional[str],
                                  stage: str,
                                  campaign_id: Optional[str] = None,
                                  cycle_id: Optional[str] = None,
                                  direction: Optional[str] = None,
                                  reason: Optional[str] = None,
                                  payload: Optional[dict] = None,
                                  ts: Optional[float] = None) -> None:
        """Persist one small, idempotent stage transition for the funnel."""
        if not opportunity_id:
            return
        safe_payload = {}
        for key, value in (payload or {}).items():
            if value is None or isinstance(value, (str, int, bool)):
                safe_payload[str(key)] = value
            elif isinstance(value, float) and math.isfinite(value):
                safe_payload[str(key)] = value
        try:
            self.ledger.record_opportunity_event(
                opportunity_id=str(opportunity_id), stage=stage,
                campaign_id=campaign_id, cycle_id=cycle_id,
                direction=direction, reason=reason, payload=safe_payload,
                ts=ts)
        except Exception:
            # Telemetry must never turn a confirmed order into a duplicate
            # retry. The execution ledger/checkpoint remains fail-closed.
            log.warning("[OPPORTUNITY] event write failed: %s/%s",
                        opportunity_id, stage, exc_info=True)

    def _record_execution_stage(self, state: ExecutionState, stage: str,
                                *, reason: Optional[str] = None,
                                payload: Optional[dict] = None) -> None:
        """Map an execution observation to its durable funnel identity."""
        context = self._recovery_context or {}
        budget = context.get("cycle_budget") or {}
        action = str(state.action or "").lower()
        if action in {StrategyAction.OPEN.value, StrategyAction.ADD.value}:
            opportunity_id = (budget.get("open_opportunity_id")
                              or context.get("open_opportunity_id"))
            campaign = context.get("open_campaign") or {}
            campaign_id = (campaign.get("open_campaign_id")
                           or budget.get("open_campaign_id"))
        else:
            opportunity_id = (context.get("exit_opportunity_id")
                              or budget.get("exit_opportunity_id"))
            campaign = context.get("exit_campaign") or {}
            campaign_id = (campaign.get("exit_campaign_id")
                           or budget.get("exit_campaign_id"))
        if not opportunity_id:
            return
        fields = {"action": action, "filled_qty": _safe_float(
            state.first_filled), "hedge_filled_qty": _safe_float(
                state.hedge_filled)}
        fields.update(payload or {})
        self._record_opportunity_event(
            opportunity_id=opportunity_id, campaign_id=campaign_id,
            cycle_id=state.cycle_id, direction=state.direction,
            stage=stage, reason=reason, payload=fields)

    def _ensure_signal_opportunity(self, candidate: TradeCandidate,
                                   now: Optional[float] = None) -> dict:
        """Create the durable identity for one directional signal episode."""
        now = time.time() if now is None else float(now)
        key = self._open_opportunity_key(candidate)
        opportunity = self._open_opportunities.get(key)
        if opportunity is None or opportunity.get("reset_required"):
            self._open_opportunity_seq += 1
            opportunity = {
                "opportunity_id": (
                    f"open:{self.run_id}:{self._open_opportunity_seq}:"
                    f"{candidate.direction}"),
                "opportunity_key": key,
                "direction": candidate.direction,
                "reference_mode": candidate.cycle_entry_reference_mode,
                "created_ts": now,
                "last_seen_ts": now,
                "reset_required": False,
                "state": "active",
                "entry_threshold_bps": candidate.threshold_bps,
                "reset_margin_bps": self.OPEN_SIGNAL_RESET_BPS,
                "basis_at_entry_bps": self._current_basis_bps(),
                "cooldown_until": 0.0,
                "send_count": 0,
                "attempt_count": 0,
                "campaign_count": 0,
                "last_campaign_distance_bps": None,
                "max_campaigns_per_signal_episode": int(
                    getattr(getattr(self.cfg, "reduce_execution", None),
                            "max_campaigns_per_signal_episode", 2) or 2),
                "last_result": None,
            }
            self._open_opportunities[key] = opportunity
            if len(self._open_opportunities) > self.MAX_OPEN_OPPORTUNITY_CACHE:
                oldest_key = min(
                    self._open_opportunities,
                    key=lambda item: float(
                        self._open_opportunities[item].get("last_seen_ts", 0.0)
                        or 0.0))
                if oldest_key != key:
                    self._open_opportunities.pop(oldest_key, None)
            self._open_campaign_telemetry["open_opportunity_count"] += 1
        self._update_open_opportunity_state(candidate, opportunity, now)
        self._record_opportunity_event(
            opportunity_id=opportunity["opportunity_id"],
            stage="statistical_signal", direction=candidate.direction,
            reason="signal_detected",
            payload={"action": candidate.action.value,
                     "threshold_bps": _safe_float(candidate.threshold_bps),
                     "signal_distance_bps": _safe_float(
                         candidate.signal_deviation_bps),
                     "reference_state": candidate.reference_state})
        self._last_open_opportunity = dict(opportunity)
        return opportunity

    def _open_opportunity_blocked(self, candidate: TradeCandidate,
                                  now: float) -> bool:
        # A directional OPEN is only legal from a genuinely flat inventory.
        # Keep every unresolved execution/recovery state fail-closed even if
        # an exchange position read happens to look flat for the moment.
        if (candidate.action == StrategyAction.OPEN
                and not self._positions_flat()):
            return True
        if (self.active_execution is not None
                or self._pending_execution_record is not None
                or self._restored_execution is not None
                or self._orphaned_inventory
                or self._emergency_pending):
            return True
        key = self._open_opportunity_key(candidate)
        opportunity = self._open_opportunities.get(key)
        if opportunity is None:
            return False
        self._update_open_opportunity_state(candidate, opportunity, now)
        if opportunity.get("reset_required"):
            return False
        if float(opportunity.get("cooldown_until") or 0.0) > now:
            return True
        # A selected opportunity is locked only while its result is still
        # unknown.  Terminal zero-fill/rejection results may be retried on
        # the next valid scan; the per-venue rate budget remains the control
        # on sustained signals.  Unknown orders stay blocked so a retry can
        # never duplicate an order whose exchange state is unresolved.
        last_result = str(opportunity.get("last_result") or "").lower()
        if opportunity.get("selected_for_execution") and not last_result:
            return True
        if last_result in {"unknown", "unresolved", "pending"}:
            return True
        successful_results = {"filled", "paired", "partial_paired", "one_leg"}
        if last_result not in successful_results:
            return False
        # A successful OPEN reserves the direction only while it still owns
        # inventory. Once every venue is flat, a sustained signal may start a
        # new cycle without waiting for the old basis reset zone. Mark the
        # prior identity reset so the next selection gets a fresh funnel id.
        if (candidate.action == StrategyAction.OPEN
                and self._positions_flat()):
            opportunity.update(reset_required=True, state="ready",
                               reset_seen_ts=now)
            self._last_open_opportunity = dict(opportunity)
            return False
        # ADDs remain governed by the existing directional dedupe while
        # inventory is open; normal pair/inventory rules decide their legality.
        return True

    def _ensure_open_opportunity(self, candidate: TradeCandidate,
                                 trade: PlannedTrade,
                                 now: Optional[float] = None) -> dict:
        """Deduplicate a sustained OPEN signal into one opportunity id."""
        now = time.time() if now is None else float(now)
        opportunity = self._ensure_signal_opportunity(candidate, now)
        key = opportunity["opportunity_key"]
        opportunity.update(
            last_seen_ts=now,
            selected_for_execution=True,
            reference_bps=candidate.cycle_entry_midline_bps,
            basis_bps=self._current_basis_bps(),
            threshold_bps=candidate.threshold_bps,
            quoted_edge_bps=trade.cycle_budget.get("quoted_edge_bps"),
            protected_open_edge_bps=trade.cycle_budget.get(
                "protected_open_edge_bps"),
            execution_room_bps=trade.cycle_budget.get("execution_room_bps"),
            statistical_threshold_bps=trade.cycle_budget.get(
                "statistical_threshold_bps", candidate.threshold_bps),
            economic_required_dislocation_bps=trade.cycle_budget.get(
                "economic_required_dislocation_bps"),
            effective_entry_distance_bps=trade.cycle_budget.get(
                "effective_entry_distance_bps"),
            economic_surplus_bps=trade.cycle_budget.get(
                "economic_surplus_bps"),
            latency_required_room_bps=trade.cycle_budget.get(
                "latency_required_room_bps"),
            first_leg_execution_room_bps=trade.cycle_budget.get(
                "first_leg_execution_room_bps"),
            physical_hard_cap_bps=trade.cycle_budget.get(
                "physical_hard_cap_bps"),
            actual_entropy_limit_px=trade.cycle_budget.get(
                "actual_entropy_limit_px"),
            campaign_result=trade.cycle_budget.get("campaign_result"),
        )
        trade.cycle_budget["open_opportunity_id"] = opportunity[
            "opportunity_id"]
        trade.cycle_budget["open_opportunity_key"] = key
        self._last_open_opportunity = dict(opportunity)
        return opportunity

    def _start_open_campaign(self, state: ExecutionState,
                             trade: PlannedTrade) -> Optional[dict]:
        """Start one bounded opportunity-level OPEN retry campaign."""
        if trade.candidate.action not in {StrategyAction.OPEN, StrategyAction.ADD}:
            return None
        policy = getattr(self.cfg, "reduce_execution", None)
        duration = float(getattr(
            policy, "open_retry_campaign_sec", 0.0) or 0.0)
        interval = float(getattr(
            policy, "open_retry_interval_sec", 0.25) or 0.25)
        max_attempts = int(getattr(
            policy, "open_retry_max_attempts", 1) or 1)
        if duration <= 0 or interval <= 0 or max_attempts <= 1:
            return None
        key = trade.cycle_budget.get("open_opportunity_key")
        opportunity = (self._open_opportunities.get(key) if key else None)
        opportunity_id = trade.cycle_budget.get("open_opportunity_id")
        if opportunity is None or not opportunity_id:
            opportunity = self._ensure_open_opportunity(trade.candidate, trade)
            key = opportunity["opportunity_key"]
            opportunity_id = opportunity["opportunity_id"]
        campaign_count = int(opportunity.get("campaign_count") or 0)
        max_campaigns = int(getattr(
            policy, "max_campaigns_per_signal_episode", 2) or 2)
        # Campaign count is retained for audit identity only.  Sustained
        # signals are controlled by the per-minute send budget, not by a
        # lifetime campaign cap or a required basis improvement.
        campaign_count += 1
        opportunity["campaign_count"] = campaign_count
        opportunity["max_campaigns_per_signal_episode"] = max_campaigns
        opportunity["last_campaign_distance_bps"] = _safe_float(
            opportunity.get("signal_distance_bps"))
        opportunity["last_campaign_ts"] = time.time()
        self._last_open_opportunity = dict(opportunity)
        now_mono = time.monotonic()
        campaign = {
            "open_opportunity_id": opportunity_id,
            "open_opportunity_key": key,
            "open_campaign_id": (
                f"{opportunity_id}:campaign:{campaign_count}"),
            "direction": trade.candidate.direction,
            "started_ts": time.time(),
            "started_monotonic": now_mono,
            "deadline_monotonic": now_mono + duration,
            "duration_sec": duration,
            "retry_interval_sec": interval,
            "max_attempts": max_attempts,
            "duration_ms": None,
            "retry_count": 0,
            "send_count": 0,
            "campaign_count": campaign_count,
            "first_fill_qty": 0.0,
            "final_result": None,
            "attempts": [],
            "last_plan": {
                "protected_open_edge_bps": trade.cycle_budget.get(
                    "protected_open_edge_bps"),
                "required_entry_edge_bps": trade.cycle_budget.get(
                    "required_entry_edge_bps"),
                "execution_room_bps": trade.cycle_budget.get(
                    "execution_room_bps"),
                "first_leg_execution_room_bps": trade.cycle_budget.get(
                    "first_leg_execution_room_bps"),
                "latency_required_room_bps": trade.cycle_budget.get(
                    "latency_required_room_bps"),
                "economic_surplus_bps": trade.cycle_budget.get(
                    "economic_surplus_bps"),
                "economic_required_dislocation_bps": trade.cycle_budget.get(
                    "economic_required_dislocation_bps"),
                "entropy_limit_px": trade.cycle_budget.get(
                    "entropy_limit_px"),
            },
        }
        self._open_campaign = campaign
        self._recovery_context["open_campaign"] = dict(campaign)
        opportunity["state"] = "campaign"
        self._record_opportunity_event(
            opportunity_id=opportunity_id,
            campaign_id=campaign["open_campaign_id"],
            direction=trade.candidate.direction,
            stage="campaign_started",
            payload={
                "campaign_count": campaign_count,
                "requested_qty": _safe_float(trade.plan.qty),
                "duration_sec": duration,
                "max_attempts": max_attempts,
            })
        return campaign

    def _open_retry_eligibility(self, trade: PlannedTrade) -> tuple[bool, str]:
        """Check the current protected OPEN floor before another IOC."""
        candidate = trade.candidate
        policy = getattr(self.cfg, "reduce_execution", None)
        dynamic = getattr(policy, "dynamic_open_first_leg", None)
        budget = trade.cycle_budget or {}
        bootstrap_mode = (budget.get("entry_mode") == "bootstrap"
                          or self._bootstrap_active())
        if (candidate.action not in {StrategyAction.OPEN, StrategyAction.ADD}
                or ((dynamic is None or not dynamic.enabled)
                    and not bootstrap_mode)):
            return True, ""
        economic_floor = _safe_float(budget.get("economic_floor_usd"))
        if economic_floor is None:
            economic_floor = self._economic_floor_usd(
                max(float(trade.plan.buy_notional),
                    float(trade.plan.sell_notional), 0.0))
        expected_pnl = _safe_float(budget.get("expected_cycle_pnl_usd"))
        if expected_pnl is not None:
            if not bootstrap_mode and expected_pnl < -1e-10:
                return False, "expected_pnl_negative"
            if expected_pnl + 1e-10 < economic_floor:
                return False, "expected_cycle_pnl_below_floor"
            if (bootstrap_mode
                    and not bool(budget.get("bootstrap_gross_ok", True))):
                return False, "bootstrap_gross_below_floor"
            if budget.get("economic_bound_not_marketable"):
                return False, "economic_bound_not_marketable"
            return True, ""
        protected_edge = _safe_float(
            budget.get("protected_open_edge_bps"))
        required_edge = _safe_float(
            budget.get("required_entry_edge_bps"))
        if (protected_edge is None or required_edge is None
                or protected_edge + 1e-9 < required_edge):
            return False, "protected_entry_failed"
        return True, ""

    def _finish_open_campaign(self, state: ExecutionState,
                              final_result: str) -> None:
        campaign = self._open_campaign
        if campaign is None:
            return
        now = time.time()
        campaign = dict(campaign)
        campaign["duration_ms"] = max(
            (time.monotonic() - campaign["started_monotonic"]) * 1000.0, 0.0)
        campaign["final_result"] = final_result
        campaign["first_fill_qty"] = float(state.first_filled)
        campaign["retry_count"] = max(int(campaign.get("send_count") or 0) - 1, 0)
        telemetry = self._open_campaign_telemetry
        send_count = int(campaign.get("send_count") or 0)
        telemetry["open_send_count"] += send_count
        telemetry["retry_count_total"] += campaign["retry_count"]
        filled = campaign["first_fill_qty"] > 0
        if filled:
            telemetry["open_campaign_fill_count"] += 1
            if campaign["retry_count"] == 0:
                telemetry["open_first_attempt_fill_count"] += 1
            telemetry["attempts_to_fill"].append(send_count)
            telemetry["opportunity_to_fill_ms"].append(
                campaign["duration_ms"])
        elif final_result == "zero_fill":
            telemetry["open_zero_fill_count"] += 1
        opportunity_count = telemetry["open_opportunity_count"]
        campaigns = telemetry["open_campaign_fill_count"] + telemetry[
            "open_zero_fill_count"]
        telemetry["open_first_attempt_fill_rate"] = (
            telemetry["open_first_attempt_fill_count"] / opportunity_count
            if opportunity_count else 0.0)
        telemetry["open_campaign_fill_rate"] = (
            telemetry["open_campaign_fill_count"] / campaigns
            if campaigns else 0.0)
        telemetry["orders_per_successful_open"] = (
            telemetry["open_send_count"] / telemetry["open_campaign_fill_count"]
            if telemetry["open_campaign_fill_count"] else 0.0)
        telemetry["attempts_to_fill"] = telemetry["attempts_to_fill"][-256:]
        telemetry["opportunity_to_fill_ms"] = telemetry[
            "opportunity_to_fill_ms"][-256:]
        key = campaign.get("open_opportunity_key")
        opportunity = self._open_opportunities.get(key)
        if opportunity is not None:
            zero_fill = final_result == "zero_fill"
            was_reset = bool(opportunity.get("reset_required"))
            opportunity.update(
                last_result=final_result, last_finished_ts=now,
                send_count=send_count, attempt_count=send_count,
                reset_required=was_reset,
                state="ready" if was_reset else "locked",
                # A terminal zero-fill may be retried quickly; the venue
                # order budget and campaign limits remain the safety control.
                cooldown_until=(now + RECOVERY_RETRY_DELAY_SEC if zero_fill
                                else 0.0))
            self._last_open_opportunity = dict(opportunity)
        self._open_outcome_history.append({
            "ts": now,
            "direction": campaign.get("direction"),
            "filled": filled,
            "attempts": send_count,
        })
        cutoff = now - 60.0
        while (self._open_outcome_history
               and float(self._open_outcome_history[0].get("ts") or 0.0)
               < cutoff):
            self._open_outcome_history.popleft()
        direction = campaign.get("direction")
        campaign["telemetry"] = {
            "send_count": send_count,
            "retry_count": campaign["retry_count"],
            "first_fill_qty": campaign["first_fill_qty"],
        }
        self._last_open_opportunity = {
            **(self._last_open_opportunity or {}), **campaign}
        self._recovery_context["open_campaign"] = dict(campaign)
        self._open_campaign = None

    def _hedge_latency_horizon_sec(self, percentile: float = 0.90) -> float:
        """Return a bounded first-result-to-hedge-terminal horizon."""
        samples = sorted(
            value for value in self._hedge_latency_samples_sec
            if math.isfinite(value) and 0.05 <= value <= 5.0)
        if len(samples) >= 3:
            return (_nearest_percentile(samples, percentile)
                    or samples[-1])
        return min(max(float(self._hedge_latency_sec_ewma), 0.05), 5.0)

    def _hedge_adverse_move(self, is_buy: bool, percentile: float,
                            *, latency_percentile: float,
                            now: Optional[float] = None) -> float:
        now = time.time() if now is None else float(now)
        latency = self._hedge_latency_horizon_sec(latency_percentile)
        window = max(10.0, latency * 8.0)
        moves = self._latency_adverse_samples(
            self._hedge_quote_history, is_buy, latency, now, window)
        return max(_nearest_percentile(moves, percentile) or 0.0, 0.0)

    def _hedge_adverse_move_economic_p75(self, is_buy: bool,
                                         now: Optional[float] = None) -> float:
        """Economic hedge latency adverse movement (all windows, P75)."""
        return self._hedge_adverse_move(
            is_buy, .75, latency_percentile=.75, now=now)

    def _interleg_adverse_move_economic_p75(
            self, is_buy: bool, now: Optional[float] = None) -> float:
        """Hedge-book P75 move during first-leg confirmation latency."""
        now = time.time() if now is None else float(now)
        latency = self._open_latency_horizon_sec(.75)
        window = max(10.0, latency * 8.0)
        moves = self._latency_adverse_samples(
            self._hedge_quote_history, is_buy, latency, now, window)
        return max(_nearest_percentile(moves, .75) or 0.0, 0.0)

    def _hedge_adverse_move_economic_p50(self, is_buy: bool,
                                         now: Optional[float] = None) -> float:
        """Bootstrap economic hedge latency adverse movement (P50/P50)."""
        return self._hedge_adverse_move(
            is_buy, .50, latency_percentile=.50, now=now)

    def _hedge_adverse_move_stress_p95(self, is_buy: bool,
                                       now: Optional[float] = None) -> float:
        """Physical hedge latency stress reserve (all windows, P95)."""
        return self._hedge_adverse_move(
            is_buy, .95, latency_percentile=.95, now=now)

    def _hedge_adverse_move_stress_p99(self, is_buy: bool,
                                       now: Optional[float] = None) -> float:
        """Physical hedge latency stress diagnostic (all windows, P99)."""
        return self._hedge_adverse_move(
            is_buy, .99, latency_percentile=.95, now=now)

    def _hedge_adverse_move_p95(self, is_buy: bool,
                                now: Optional[float] = None) -> float:
        """Compatibility alias for the physical P95 stress reserve."""
        return self._hedge_adverse_move_stress_p95(is_buy, now)

    def _hedge_reserve_metrics(self, is_buy: bool, qty: float,
                               now: Optional[float] = None,
                               *, phase: str = "close_hedge",
                               physical_max_bps: Optional[float] = None) -> dict:
        """Return the live second-leg reserve used by economic REDUCE."""
        now = time.time() if now is None else float(now)
        reserve_cfg = getattr(
            getattr(self.cfg, "reduce_execution", None),
            "dynamic_hedge_reserve", None)
        legacy_bps = self._second_leg_slippage_bps(StrategyAction.REDUCE)
        estimate = self._execution_cost_estimate(
            phase, venue=self.hedge, is_buy=is_buy, qty=qty, now=now)
        expected_cost = estimate["expected_cost_bps"]
        physical_cap = (_safe_float(physical_max_bps)
                        if physical_max_bps is not None else None)
        if physical_cap is not None:
            physical_cap = max(physical_cap, 0.0)
        if reserve_cfg is None or not reserve_cfg.enabled:
            return {
                "enabled": False,
                "reserve_bps": max(legacy_bps, physical_cap or 0.0),
                "expected_cost_bps": expected_cost,
                "expected_reserve_bps": expected_cost,
                "depth_cost_bps": 0.0,
                "adverse_move_economic_p75_bps": 0.0,
                "adverse_move_stress_p95_bps": 0.0,
                "adverse_move_stress_p99_bps": 0.0,
                "adverse_move_p95_bps": 0.0,
                "safety_margin_bps": 0.0,
                "physical_hard_cap_bps": physical_cap,
                "source": "fixed_second_leg_slippage",
                "cost_model": estimate,
            }
        top = (_safe_float(self.hedge.book.best_ask()) if is_buy
               else _safe_float(self.hedge.book.best_bid()))
        quote = (self._depth_quote(self.hedge, is_buy, qty)
                 if qty > 0 else None)
        marginal = _safe_float(quote[0]) if quote is not None else top
        depth_avg = (quote[1] / qty if quote is not None and qty > 0
                     else top)
        depth_cost = 0.0
        if top is not None and depth_avg is not None and top > 0:
            depth_cost = ((depth_avg / top - 1.0) * 1e4 if is_buy
                          else (1.0 - depth_avg / top) * 1e4)
            depth_cost = max(depth_cost, 0.0)
        economic_adverse = self._hedge_adverse_move_economic_p75(is_buy, now)
        adverse = self._hedge_adverse_move_stress_p95(is_buy, now)
        stress_p99 = self._hedge_adverse_move_stress_p99(is_buy, now)
        raw = depth_cost + adverse + max(reserve_cfg.safety_margin_bps, 0.0)
        reserve_max = max(float(reserve_cfg.max_bps), physical_cap or 0.0)
        reserve = min(max(raw, reserve_cfg.min_bps), reserve_max)
        # The expected bound is economic only.  It must not inherit the
        # physical depth/stress reserve, otherwise the worst-case protection
        # is charged twice in projected PnL.
        expected_reserve = max(
            expected_cost + max(float(reserve_cfg.safety_margin_bps), 0.0), 0.0)
        return {
            "enabled": True,
            "reserve_bps": reserve,
            "expected_cost_bps": expected_cost,
            "cost_model": estimate,
            "cost_source": estimate.get("source"),
            "empirical_weight": estimate.get("empirical_weight", 0.0),
            "usable_samples": estimate.get("usable_samples", 0),
            "expected_reserve_bps": expected_reserve,
            "depth_cost_bps": depth_cost,
            "adverse_move_economic_p75_bps": economic_adverse,
            "adverse_move_stress_p95_bps": adverse,
            "adverse_move_stress_p99_bps": stress_p99,
            "adverse_move_p95_bps": adverse,
            "safety_margin_bps": reserve_cfg.safety_margin_bps,
            "reserve_min_bps": reserve_cfg.min_bps,
            "reserve_max_bps": reserve_cfg.max_bps,
            "physical_hard_cap_bps": physical_cap,
            "top_px": top,
            "marginal_px": marginal,
            "quote_notional": (quote[1] if quote is not None else None),
            "source": "depth_plus_latency_stress_p95",
        }

    def _hedge_bound_for_side(self, is_buy: bool, qty: float,
                              now: Optional[float] = None, *,
                              expected: bool = False,
                              executable: bool = False,
                              phase: str = "close_hedge",
                              physical_max_bps: Optional[float] = None
                              ) -> tuple[Optional[float], dict]:
        """Price the hedge leg with a legal, fill-safe limit."""
        metrics = self._hedge_reserve_metrics(
            is_buy, qty, now, phase=phase,
            physical_max_bps=physical_max_bps)
        if not metrics["enabled"]:
            reserve = (metrics["expected_reserve_bps"] if expected
                       else metrics["reserve_bps"])
            bound = self._live_taker_bound(
                self.hedge, is_buy, reserve)
            metrics["bound_px"] = bound
            metrics["bound_basis"] = "expected" if expected else "physical"
            return bound, metrics
        # The reserve already includes the current depth cost relative to the
        # top of book. Start from top-of-book here so that depth is not
        # charged twice when converting the reserve into a worst-case limit.
        base = (_safe_float(metrics.get("top_px"))
                or _safe_float(metrics.get("marginal_px")))
        if base is None or base <= 0:
            metrics["bound_px"] = None
            return None, metrics
        if expected and qty > 0:
            quote_notional = _safe_float(metrics.get("quote_notional"))
            if quote_notional is not None:
                base = quote_notional / qty
        reserve = float(metrics["expected_reserve_bps" if expected
                             else "reserve_bps"]) / 1e4
        metrics["bound_basis"] = "expected" if expected else "physical"
        raw = base * (1.0 + reserve) if is_buy else base * (1.0 - reserve)
        if expected and executable:
            marginal = _safe_float(metrics.get("marginal_px"))
            if marginal is not None:
                raw = max(raw, marginal) if is_buy else min(raw, marginal)
        bound = self.hedge.px_round(raw, round_up=not is_buy)
        metrics["raw_bound_px"] = raw
        metrics["bound_px"] = bound
        return bound, metrics

    def _cycle_profit_budget(self, candidate: TradeCandidate,
                             plan: ArbPlan) -> dict:
        """Conservative economics for this slice, never a guaranteed PnL.

        OPEN/ADD assumes a return to the model exit target, preserving the
        currently observable reverse-book depth costs. REDUCE uses actual
        remaining FIFO lots; prior realized profits cannot subsidize a loss.
        Emergency hedges/recovery do not call this method.
        """
        now = time.time()
        self._record_hedge_quote(now)
        guard = self.cfg.profit_guard
        fill_pnl_mode = (self.cfg.cycle_exit.enabled
                         and self.cfg.cycle_exit.mode == "fill_pnl"
                         and candidate.action == StrategyAction.REDUCE)
        configured_floor = (self.cfg.cycle_exit.normal_min_profit_usd
                            if fill_pnl_mode
                            else (guard.min_profit_usd
                                  if guard.enabled else 0.0))
        out = {"checked_ts": now, "action": candidate.action.value,
               "direction": candidate.direction, "qty": plan.qty,
               "allowed": False, "reason": "invalid_economics",
               "min_profit_usd": configured_floor,
               "exit_mode": ("fill_pnl" if fill_pnl_mode
                             else ("profit_guard" if guard.enabled else "none")),
               "exit_reason": None, "hold_state": None,
               "uncertainty_bps": 0.0, "uncertainty_usd": 0.0}
        if not all(math.isfinite(x) and x > 0 for x in (
                plan.qty, plan.buy_notional, plan.sell_notional)):
            return out
        max_age_sec = self._hard_market_data_age_sec()
        books_fresh = all(
            v.book.is_fresh(max_age_sec)
            and 0 <= now - v.book.last_update_ts <= max_age_sec
            for v in (candidate.buy, candidate.sell))
        cross_skew_ms = abs(candidate.buy.book.last_update_ts
                            - candidate.sell.book.last_update_ts) * 1000
        out.update(books_fresh=books_fresh,
                   cross_venue_skew_ms=cross_skew_ms,
                   max_cross_venue_skew_ms=self._hard_cross_skew_ms())
        # Keep the economics path available for a risk REDUCE decision.  A
        # skewed pair can still be flattened, but only after both cached books
        # are fresh; OPEN/ADD and economic REDUCE are gated below.
        if not books_fresh:
            return {**out, "reason": "stale_or_skewed_books"}
        closing = candidate.action == StrategyAction.REDUCE
        bootstrap_active = bool(not closing and self._bootstrap_active())
        bootstrap_cfg = self._bootstrap_cfg()
        cycle_entry_mode = (str(self._cycle_telemetry.get("entry_mode")
                                or self._recovery_context.get("entry_mode")
                                or "") if closing else "")
        bootstrap_cycle = cycle_entry_mode == "bootstrap"
        if closing != candidate.reduce_only:
            return {**out, "reason": "invalid_reduce_flag"}
        short = (candidate.direction == "buy_entropy" if closing
                 else candidate.direction == "sell_entropy")
        snap = self._premium_snapshot(now)
        lag_state = self._lag_state(snap.midline_lag_state)
        reference_policy = self._reference_policy(
            snap, lag_state, self._current_basis_bps())
        quality = (self._current_premium_sample_quality(now)
                   if self.cfg.dynamic_midline_enabled else None)
        data_uncertainty = (max(float(quality.uncertainty_bps or 0.0), 0.0)
                            if quality is not None
                            and quality.data_tier != PremiumDataTier.INVALID
                            else 0.0)
        reference_uncertainty = max(float(reference_policy.get(
            "reference_uncertainty_bps") or 0.0), 0.0)
        model_uncertainty = max(float(reference_policy.get(
            "model_uncertainty_bps") or 0.0), 0.0)
        reentry_uncertainty = 0.0
        if not closing:
            reentry_uncertainty = max(float(self._reentry_status(
                candidate.direction, now, lag_state).get(
                    "uncertainty_bps") or 0.0), 0.0)
        margin_uncertainty = 0.5 if self._margin_snapshot().get(
            "state") == "warning" else 0.0
        uncertainty_bps = (data_uncertainty + reference_uncertainty
                           + model_uncertainty + reentry_uncertainty
                           + margin_uncertainty)
        out.update(
            data_uncertainty_bps=data_uncertainty,
            reference_uncertainty_bps=reference_uncertainty,
            model_uncertainty_bps=model_uncertainty,
            reentry_uncertainty_bps=reentry_uncertainty,
            margin_uncertainty_bps=margin_uncertainty,
            uncertainty_bps=uncertainty_bps)
        funding_available, er, hr = self._funding_rates(now)
        funding_missing = self.cfg.funding_enabled and not funding_available
        if (funding_missing and not closing
                and self.cfg.funding_on_unavailable == "block_open"):
            return {**out, "reason": "funding_unavailable"}
        notional = max(plan.buy_notional, plan.sell_notional)
        quoted_exit_cash = 0.0
        protected_exit_cash = 0.0
        hedge_reserve_loss_usd = 0.0
        if closing:
            hedge_is_buy = candidate.buy is not self.entropy
            hedge_bound, hedge_metrics = self._hedge_bound_for_side(
                hedge_is_buy, plan.qty, now, expected=True,
                phase="close_hedge")
            costs = [self.ledger.preview_close_cost(
                venue=v.key, position=v.position, qty=plan.qty, now=now)
                for v in (self.entropy, self.hedge)]
            for cost in costs:
                if not cost["ready"]:
                    return {**out, "reason": cost["reason"]}
            if ((short and not (self.entropy.position < 0 < self.hedge.position))
                    or (not short and not (self.hedge.position < 0 < self.entropy.position))):
                return {**out, "reason": "inventory_direction_mismatch"}
            entry_cash = sum(c["entry_cash_usd"] for c in costs)
            entry_fees = sum(c["entry_fees_usd"] for c in costs)
            exit_cash = plan.gross_edge_usd
            quoted_exit_cash = exit_cash
            protected_exit_cash = exit_cash
            if (hedge_bound is not None
                    and hedge_metrics.get("quote_notional") is not None):
                current_hedge_cash = float(hedge_metrics["quote_notional"])
                worst_hedge_cash = plan.qty * float(hedge_bound)
                hedge_reserve_loss_usd = max(
                    (worst_hedge_cash - current_hedge_cash)
                    if hedge_is_buy else
                    (current_hedge_cash - worst_hedge_cash), 0.0)
                protected_exit_cash = exit_cash - hedge_reserve_loss_usd
            exit_fees = (plan.buy_notional * plan.buy_fee
                         + plan.sell_notional * plan.sell_fee)
            # A tiny old tail must not age a large freshly added slice.
            held_hours = max(c["age_qty_hours"] / plan.qty for c in costs)
            funding_debit = sum(c["funding_debit_usd"] for c in costs)
            # In soft fill-PnL mode execution slippage is handled by the
            # action-aware IOC bounds.  Charging the same buffer here would
            # make the $0 floor secretly behave like a hard $0.07 floor on a
            # $60 slice.
            buffer_bps = (0.0 if fill_pnl_mode or not guard.enabled
                          or guard.mode == "soft"
                          else guard.exit_buffer_bps)
            buffer_usd = notional * buffer_bps / 1e4
            out["entry_fee_estimated"] = any(c["fee_estimated"] for c in costs)
            out["cost_basis"] = "fifo_remaining_lots"
            out.update(
                dynamic_hedge_reserve_bps=hedge_metrics.get("reserve_bps"),
                hedge_depth_cost_bps=hedge_metrics.get("depth_cost_bps", 0.0),
                hedge_adverse_move_p95_bps=hedge_metrics.get(
                    "adverse_move_p95_bps", 0.0),
                hedge_safety_margin_bps=hedge_metrics.get(
                    "safety_margin_bps", 0.0),
                hedge_reserve_source=hedge_metrics.get("source"),
                hedge_bound_px=hedge_bound,
                hedge_reserve_loss_usd=hedge_reserve_loss_usd)
        else:
            # Unknown/manual inventory must not be averaged into a new ADD.
            if candidate.action == StrategyAction.ADD:
                for v in (self.entropy, self.hedge):
                    known = self.ledger.preview_close_cost(
                        venue=v.key, position=v.position,
                        qty=abs(v.position), now=now)
                    if not known["ready"]:
                        return {**out, "reason": known["reason"]}
            em, hm = self.entropy.book.mid(), self.hedge.book.mid()
            center = candidate.cycle_entry_midline_bps
            if center is None:
                center = (self.model_snapshot.active_midline_bps
                          if self.cfg.dynamic_midline_enabled
                          else self.cfg.midline_bps)
            target = center + (self.cfg.exit_bps if short else -self.cfg.exit_bps)
            eq = self._depth_quote(self.entropy, short, plan.qty)
            hq = self._depth_quote(self.hedge, not short, plan.qty)
            if not em or not hm or eq is None or hq is None:
                return {**out, "reason": "insufficient_exit_depth"}
            # Shift Entropy mid to the target basis, retaining the spread and
            # depth paid to close both venues. The hedge price level is a
            # scenario anchor, not a future price prediction.
            entropy_exit = eq[1] + plan.qty * (hm * (1 + target / 1e4) - em)
            hedge_exit = hq[1]
            if entropy_exit <= 0:
                return out
            entry_cash = plan.gross_edge_usd
            entry_fees = (plan.buy_notional * plan.buy_fee
                          + plan.sell_notional * plan.sell_fee)
            exit_cash = (hedge_exit - entropy_exit if short
                         else entropy_exit - hedge_exit)
            exit_fees = (entropy_exit * self.entropy.fee_bps
                         + hedge_exit * self.hedge.fee_bps) / 1e4
            held_hours = self._expected_cycle_hold_sec(candidate.direction) / 3600.0
            funding_debit = 0.0
            buffer_bps = (0.0 if not guard.enabled or guard.mode == "soft"
                          else guard.entry_buffer_bps + guard.exit_buffer_bps)
            buffer_usd = notional * buffer_bps / 1e4
            quoted_exit_cash = exit_cash
            protected_exit_cash = exit_cash
            out.update(target_premium_bps=target, cost_basis="projected_round_trip")
        out.update(
            entry_mode="bootstrap" if bootstrap_active else (
                "bootstrap" if bootstrap_cycle else "normal"),
            bootstrap_active=bootstrap_active,
            bootstrap_cycle=bootstrap_cycle)
        entropy_is_buy = candidate.buy is self.entropy
        # For an OPEN candidate the projected round trip closes the Entropy
        # leg in the opposite direction.  Using the entry direction for the
        # close phases would query the wrong side of the book and under/over-
        # state the all-in reserve depending on the signal direction.  For a
        # REDUCE candidate the direction already describes the actual close.
        close_entropy_is_buy = entropy_is_buy if closing else not entropy_is_buy
        expected_costs_bps: dict[str, float] = {}
        expected_cost_details: dict[str, dict] = {}
        economic_execution_cost_usd = 0.0
        economic_safety_usd = 0.0
        economics = getattr(self.cfg, "economic_execution", None)
        phase_specs = {
            "open_first": (self.entropy, entropy_is_buy),
            "open_interleg": (self.hedge, not entropy_is_buy),
            "open_hedge": (self.hedge, not entropy_is_buy),
            "close_first": (self.entropy, close_entropy_is_buy),
            "close_interleg": (self.hedge, not close_entropy_is_buy),
            "close_hedge": (self.hedge, not close_entropy_is_buy),
        }
        for phase, (venue, is_buy) in phase_specs.items():
            interleg_adverse = (
                self._interleg_adverse_move_economic_p75(is_buy, now)
                if phase.endswith("_interleg") else None)
            estimate = self._execution_cost_estimate(
                phase, venue=venue, is_buy=is_buy, qty=plan.qty, now=now,
                latency_adverse_bps=interleg_adverse)
            expected_cost_details[phase] = estimate
            expected_costs_bps[phase] = estimate["expected_cost_bps"]
        if not closing:
            economic_execution_cost_usd = notional * sum(
                expected_costs_bps.values()) / 1e4
            safety_bps = max(
                float(getattr(economics, "safety_margin_bps", 0.5)), 0.0)
            economic_safety_usd = notional * safety_bps / 1e4
        economic_uncertainty_usd = (notional * uncertainty_bps / 1e4
                                    if not closing else 0.0)
        funding_reserve = 0.0
        if self.cfg.funding_enabled:
            if funding_missing:
                cost_bps = self.cfg.funding_max_adverse_cost_bps * max(
                    1.0, held_hours / max(self.cfg.funding_expected_hold_hours, 1e-9))
            else:
                cost_bps = expected_pair_funding_cost_bps(
                    er, hr, held_hours, short_entropy=short)
                cost_bps += max(float(getattr(
                    self, "_funding_uncertainty_bps", 0.0)), 0.0)
            funding_reserve = notional * max(cost_bps, 0.0) / 1e4
        funding_cost = max(funding_debit, funding_reserve)
        gross_after_fee_usd = (entry_cash + quoted_exit_cash
                               - entry_fees - exit_fees)
        gross_after_fee_bps = (gross_after_fee_usd / notional * 1e4
                               if notional > 0 else None)
        production_floor = max(
            self._economic_floor_usd(notional),
            float(configured_floor) if math.isfinite(
                float(configured_floor)) else 0.0)
        bootstrap_floor = (self._bootstrap_floor_usd(notional)
                           if bootstrap_active else None)
        sampling_floor = (bootstrap_floor if bootstrap_active
                          else production_floor)
        bootstrap_gross_floor = (float(getattr(
            bootstrap_cfg, "min_gross_after_fee_bps", -0.5))
            if bootstrap_cfg is not None else -0.5)
        bootstrap_gross_ok = (not bootstrap_active
                              or (gross_after_fee_bps is not None
                                  and gross_after_fee_bps
                                  >= bootstrap_gross_floor - 1e-10))
        quoted_net = (entry_cash + quoted_exit_cash - entry_fees
                      - exit_fees - funding_cost - buffer_usd
                      - economic_execution_cost_usd - economic_safety_usd)
        net = (entry_cash + protected_exit_cash - entry_fees
               - exit_fees - funding_cost - buffer_usd
               - economic_execution_cost_usd - economic_safety_usd
               - economic_uncertainty_usd)
        quoted_net -= economic_uncertainty_usd
        if not closing:
            calibration = self._calibrate_cycle_projection(
                net, notional, candidate.direction, candidate.reference_state)
            adjustment = calibration["calibrated_cycle_pnl_usd"] - net
            net += adjustment
            quoted_net += adjustment
            out.update(calibration)
        if not all(math.isfinite(x) for x in (
                net, quoted_net, entry_cash, exit_cash, entry_fees, exit_fees,
                funding_cost, buffer_usd)):
            return out
        profit_floor = (self.cfg.cycle_exit.normal_min_profit_usd
                        if fill_pnl_mode
                        else (guard.min_profit_usd if guard.enabled else 0.0))
        if not closing:
            profit_floor = sampling_floor
        dynamic_floor_bps = 0.0
        dynamic_floor_usd = 0.0
        held_sec = 0.0
        if fill_pnl_mode:
            opened_ts = self.cycle.opened_ts
            held_sec = (max(now - opened_ts, 0.0)
                        if opened_ts is not None else 0.0)
            dynamic_floor_usd = min(
                profit_floor, self.cfg.cycle_exit.fast_exit_min_profit_usd
            ) if held_sec >= self.cfg.cycle_exit.fast_exit_after_sec else profit_floor
            dynamic_floor_bps = dynamic_floor_usd / notional * 1e4
            profit_floor = dynamic_floor_usd
        allowed = (net + 1e-10 >= profit_floor
                   and bootstrap_gross_ok)
        if bootstrap_active and not bootstrap_gross_ok:
            budget_reason = "bootstrap_gross_below_floor"
        else:
            budget_reason = "ready" if allowed else "cycle_profit_below_floor"
        current_signal_distance = _safe_float(
            candidate.signal_deviation_bps)
        if current_signal_distance is None and not closing:
            current_signal_distance = _safe_float(
                self._open_signal_distance_bps(candidate))
        current_signal_distance = max(current_signal_distance or 0.0, 0.0)
        shortfall_usd = max(profit_floor - net, 0.0)
        additional_dislocation_bps = (
            shortfall_usd / notional * 1e4 if notional > 0 else 0.0)
        economic_required_dislocation_bps = (
            current_signal_distance + additional_dislocation_bps)
        forced = False
        risk_exit = False
        exit_class = None
        risk_exit_type = None
        hold_state = None
        if fill_pnl_mode:
            drift = float(candidate.cycle_midline_drift_bps or 0.0)
            lag_state = self._lag_state(self.model_snapshot.midline_lag_state)
            regime_lag_ok = (lag_state in {
                MidlineLagState.WARNING,
                MidlineLagState.ACCELERATING,
                MidlineLagState.BLOCKED,
            } if self.cfg.cycle_exit.regime_stop_require_midline_warning
                else True)
            adverse, adverse_persisted, adverse_started = (
                self._adverse_drift_persistence(candidate, now))
            loss_limit_usd = (
                notional * self.cfg.cycle_exit.max_cycle_loss_bps / 1e4
                if self.cfg.cycle_exit.max_cycle_loss_bps is not None else None)
            decision = evaluate_cycle_exit(
                self.cfg.cycle_exit, net_pnl_usd=net, notional_usd=notional,
                held_sec=held_sec, adverse_drift_bps=adverse,
                adverse_persist_sec=adverse_persisted,
                regime_shift=bool(snap.regime_shift),
                regime_warning=regime_lag_ok)
            allowed, profit_floor = decision["allowed"], decision["floor_usd"]
            exit_reason, exit_class = decision["reason"], decision["exit_class"]
            forced, risk_exit = decision["forced"], decision["risk_exit"]
            risk_exit_type, hold_state = decision["risk_exit_type"], decision["hold_state"]
            fast_exit_after = self.cfg.cycle_exit.fast_exit_after_sec
            fast_exit_min = self.cfg.cycle_exit.fast_exit_min_profit_usd
            sampling_exit_after = sampling_exit_floor = None
            stale_exit_after = stale_exit_floor = None
            out.update(held_sec=held_sec, midline_drift_bps=drift,
                       adverse_drift_bps=adverse,
                       adverse_drift_started_ts=adverse_started,
                       adverse_drift_persist_sec=adverse_persisted,
                       loss_limit_usd=loss_limit_usd,
                       midline_lag_state=lag_state.value,
                       regime_stop_lag_ok=regime_lag_ok,
                       allowed_floor_usd=profit_floor, forced=forced,
                       dynamic_exit_floor_bps=dynamic_floor_bps,
                       dynamic_exit_floor_usd=dynamic_floor_usd,
                       fast_exit_after_sec=fast_exit_after,
                       fast_exit_min_profit_usd=fast_exit_min,
                       sampling_exit_after_sec=sampling_exit_after,
                       sampling_exit_floor_usd=sampling_exit_floor,
                       stale_exit_after_sec=stale_exit_after,
                       stale_exit_floor_usd=stale_exit_floor,
                       bootstrap_stale_hold=False,
                       risk_exit=risk_exit, risk_exit_type=risk_exit_type,
                       exit_class=exit_class, hold_state=hold_state,
                       adverse_drift_exit_enabled=(
                           self.cfg.cycle_exit.adverse_drift_exit_enabled
                           or self.cfg.cycle_exit.adverse_drift_enabled),
                       pnl_loss_stop_enabled=(
                           self.cfg.cycle_exit.pnl_loss_stop_enabled))
        else:
            exit_reason = "ready" if allowed else "cycle_profit_below_floor"
        if cross_skew_ms > self._hard_cross_skew_ms() \
                and not risk_exit:
            allowed = False
            exit_reason = "stale_or_skewed_books"
            budget_reason = exit_reason
        out.update(entry_cash_usd=entry_cash, exit_cash_usd=exit_cash,
                   quoted_exit_cash_usd=quoted_exit_cash,
                   protected_exit_cash_usd=protected_exit_cash,
                   entry_fees_usd=entry_fees, exit_fees_usd=exit_fees,
                   funding_cost_usd=funding_cost, funding_estimated=True,
                   funding_uncertainty_bps=max(float(getattr(
                       self, "_funding_uncertainty_bps", 0.0)), 0.0),
                   execution_buffer_usd=buffer_usd,
                   economic_uncertainty_usd=economic_uncertainty_usd,
                   expected_execution_costs_bps=expected_costs_bps,
                   expected_execution_cost_details=expected_cost_details,
                   expected_execution_cost_usd=economic_execution_cost_usd,
                   economic_safety_margin_usd=economic_safety_usd,
                   statistical_threshold_bps=candidate.threshold_bps,
                   economic_current_signal_distance_bps=(
                       current_signal_distance),
                   economic_shortfall_usd=shortfall_usd,
                   economic_additional_dislocation_bps=(
                       additional_dislocation_bps),
                   economic_required_dislocation_bps=(
                       economic_required_dislocation_bps),
                   effective_entry_distance_bps=(
                       _safe_float(candidate.signal_deviation_bps)
                       if candidate.signal_deviation_bps is not None else
                       (_safe_float(self._open_signal_distance_bps(candidate))
                        if not closing else None)),
                   economic_opportunity=(net >= profit_floor - 1e-10
                                         and bootstrap_gross_ok),
                   production_floor_usd=production_floor,
                   sampling_floor_usd=sampling_floor,
                   bootstrap_color=("GREEN" if net >= production_floor
                                    else "YELLOW" if bootstrap_active
                                    and net >= sampling_floor else "RED"),
                   bootstrap_floor_usd=bootstrap_floor,
                   economic_floor_usd=profit_floor,
                   bootstrap_exploration=(bootstrap_active
                                          and net < production_floor),
                   bootstrap_gross_after_fee_bps=gross_after_fee_bps,
                   bootstrap_gross_ok=bootstrap_gross_ok,
                   bootstrap_min_gross_after_fee_bps=(
                       bootstrap_gross_floor if bootstrap_active else None),
                   bootstrap_cost_model_source=";".join(
                       f"{phase}:{details.get('source', 'unknown')}"
                       for phase, details in expected_cost_details.items()),
                   expected_hold_sec=(held_hours * 3600.0 if not closing else None),
                   quoted_net_profit_usd=quoted_net, net_profit_usd=net,
                   cycle_executable_net_pnl=net,
                   predicted_close_pnl_usd=net,
                   allowed=allowed,
                   reason=(budget_reason if not fill_pnl_mode else exit_reason),
                   exit_reason=exit_reason)
        return out

    def _build_pnl_protected_close_bound(
            self, candidate: TradeCandidate, plan: ArbPlan, budget: dict,
            floor_usd: float, *, apply_physical_bound: bool) \
            -> Optional[PlannedTrade]:
        """Build a close IOC bound from the remaining economic budget.

        ``close_min_execution_room_bps`` is a diagnostic target only.  Once a
        positive whole-cycle budget remains, the close attempt gets that
        budget as executable room and is limited only by the absolute physical
        hard cap.  The follow-up hedge is handled independently by the hedge
        completion state machine.
        """
        decision_reason = (budget.get("exit_reason")
                           or budget.get("reason"))
        entropy_buy = candidate.buy is self.entropy
        hedge_bound, hedge_metrics = self._hedge_bound_for_side(
            not entropy_buy, plan.qty, time.time(), expected=True,
            phase="close_hedge", executable=True)
        if hedge_bound is None or hedge_bound <= 0:
            budget.update(allowed=False, reason="missing_hedge_bound")
            return None
        budget.update(
            dynamic_hedge_reserve_bps=hedge_metrics.get("reserve_bps"),
            hedge_depth_cost_bps=hedge_metrics.get("depth_cost_bps", 0.0),
            hedge_adverse_move_p95_bps=hedge_metrics.get(
                "adverse_move_p95_bps", 0.0),
            hedge_safety_margin_bps=hedge_metrics.get(
                "safety_margin_bps", 0.0),
            hedge_reserve_source=hedge_metrics.get("source"),
            hedge_bound_px=hedge_bound)
        try:
            fixed_cash = (float(budget["entry_cash_usd"])
                          - float(budget["entry_fees_usd"])
                          - float(budget["funding_cost_usd"])
                          - float(budget["execution_buffer_usd"]))
            floor = float(floor_usd)
            qty = float(plan.qty)
        except (KeyError, TypeError, ValueError):
            budget.update(allowed=False, reason="invalid_close_budget")
            return None
        projected_pnl = _safe_float(budget.get("net_profit_usd"))
        remaining_notional = max(
            abs(float(plan.buy_notional or 0.0)),
            abs(float(plan.sell_notional or 0.0)),
            qty * max(float(plan.buy_limit or 0.0),
                      float(plan.sell_limit or 0.0)))
        if (qty <= 0 or projected_pnl is None
                or remaining_notional <= 0
                or not all(math.isfinite(value) for value in (
                    fixed_cash, floor, qty, projected_pnl,
                    remaining_notional))):
            budget.update(allowed=False, reason="invalid_close_budget")
            return None

        economic_budget_usd = projected_pnl - floor
        hedge_loss_vs_plan_usd = max(
            (plan.sell_limit - hedge_bound) * qty if entropy_buy
            else (hedge_bound - plan.buy_limit) * qty, 0.0)
        marginal_net = fixed_cash + qty * (
            (hedge_bound if entropy_buy else plan.sell_limit) * (1 - plan.sell_fee)
            - (plan.buy_limit if entropy_buy else hedge_bound) * (1 + plan.buy_fee))
        first_leg_budget_usd = max(min(marginal_net - floor, economic_budget_usd), 0.0)
        exit_budget_bps = (economic_budget_usd / remaining_notional * 1e4
                           if remaining_notional > 0 else 0.0)
        first_leg_exit_budget_bps = (
            first_leg_budget_usd / remaining_notional * 1e4
            if remaining_notional > 0 else 0.0)
        budget.update(
            projected_complete_cycle_pnl_usd=projected_pnl,
            required_exit_floor_usd=floor,
            exit_economic_budget_usd=economic_budget_usd,
            hedge_budget_consumed_usd=hedge_loss_vs_plan_usd,
            first_leg_exit_budget_usd=first_leg_budget_usd,
            first_leg_exit_budget_bps=first_leg_exit_budget_bps,
            exit_budget_bps=exit_budget_bps,
            remaining_notional_usd=remaining_notional)
        if (not math.isfinite(exit_budget_bps)
                or not math.isfinite(first_leg_exit_budget_bps)
                or not math.isfinite(first_leg_budget_usd)
                or economic_budget_usd <= 0.0):
            budget.update(allowed=False,
                          reason="close_pnl_budget_insufficient")
            return None

        # ``plan.buy_limit``/``sell_limit`` are refreshed marginal executable
        # prices, not the stale signal snapshot.  Spend only the positive
        # remaining cycle budget around that price, then apply the configured
        # absolute cap as the sole physical constraint.
        if entropy_buy:
            economic_raw = plan.buy_limit * (
                1.0 + first_leg_exit_budget_bps / 1e4)
            economic_bound = self.entropy.px_round(
                economic_raw, round_up=False)
            economic_bound = max(economic_bound, plan.buy_limit)
            hard_cap_bps = self._execution_hard_cap_bps(
                StrategyAction.REDUCE, "first")
            physical_bound = self.entropy.px_round(
                plan.buy_limit * (1.0 + hard_cap_bps / 1e4),
                round_up=False)
            physical_bound = max(physical_bound, plan.buy_limit)
            entropy_bound = (min(economic_bound, physical_bound)
                             if apply_physical_bound else economic_bound)
            buy_bound, sell_bound = entropy_bound, hedge_bound
        else:
            economic_raw = plan.sell_limit * (
                1.0 - first_leg_exit_budget_bps / 1e4)
            economic_bound = self.entropy.px_round(
                economic_raw, round_up=True)
            economic_bound = min(economic_bound, plan.sell_limit)
            hard_cap_bps = self._execution_hard_cap_bps(
                StrategyAction.REDUCE, "first")
            physical_bound = self.entropy.px_round(
                plan.sell_limit * (1.0 - hard_cap_bps / 1e4),
                round_up=True)
            physical_bound = min(physical_bound, plan.sell_limit)
            entropy_bound = (max(economic_bound, physical_bound)
                             if apply_physical_bound else economic_bound)
            buy_bound, sell_bound = hedge_bound, entropy_bound

        limit_net = fixed_cash + qty * (
            sell_bound * (1 - plan.sell_fee)
            - buy_bound * (1 + plan.buy_fee))
        if entropy_buy:
            reference_px = float(plan.buy_limit)
            close_room_bps = ((entropy_bound / reference_px - 1.0) * 1e4
                              if reference_px > 0 else 0.0)
        else:
            reference_px = float(plan.sell_limit)
            close_room_bps = ((1.0 - entropy_bound / reference_px) * 1e4
                              if reference_px > 0 else 0.0)
        close_room_bps = max(close_room_bps, 0.0)
        close_optimizer = choose_execution_room(
            phase="close",
            affordable_room_bps=close_room_bps,
            hard_cap_bps=hard_cap_bps,
            fill_stats=getattr(self, "_close_fill_probability", {}),
            base_cycle_pnl_bps=(
                projected_pnl / remaining_notional * 1e4
                + (_safe_float((budget.get(
                    "expected_execution_costs_bps") or {}).get(
                        "close_first")) or 0.0)
                if remaining_notional > 0 else 0.0),
            capital_lock_sec=float(budget.get("held_sec") or 1.0),
            expected_incremental_cost_bps=(
                _safe_float((budget.get(
                    "expected_execution_costs_bps") or {}).get(
                        "close_first")) or 0.0))
        selected_close_room = max(float(
            close_optimizer.get("room_bps") or 0.0), 0.0)
        active_campaign = getattr(self, "_exit_campaign", None) or {}
        previous_room = _safe_float(
            active_campaign.get("close_execution_room_bps"))
        if previous_room is not None:
            # A zero-fill retry must not become less aggressive than the
            # prior attempt in the same close campaign.  The optimizer may
            # still widen within the positive economic/physical budget.
            selected_close_room = max(
                selected_close_room,
                min(previous_room, close_room_bps))
        if 0.0 < selected_close_room + 1e-9 < close_room_bps:
            if entropy_buy:
                entropy_bound = self.entropy.px_round(
                    reference_px * (1.0 + selected_close_room / 1e4),
                    round_up=False)
                entropy_bound = max(entropy_bound, plan.buy_limit)
                buy_bound = entropy_bound
            else:
                entropy_bound = self.entropy.px_round(
                    reference_px * (1.0 - selected_close_room / 1e4),
                    round_up=True)
                entropy_bound = min(entropy_bound, plan.sell_limit)
                sell_bound = entropy_bound
            close_room_bps = selected_close_room
        # Bootstrap does not change close aggressiveness.  Keep these fields
        # as diagnostics for API compatibility, but never use them as a
        # minimum-room or target-room execution override.
        close_room_min_bps = 0.0
        close_room_target_bps = None
        # Do not treat a floating-point/tick-rounding sliver as a real
        # positive exit budget.  A close must clear the floor by at least one
        # micro-dollar; this keeps the fast-exit path conservative while
        # remaining immaterial at normal order sizes.
        limit_net = fixed_cash + qty * (
            sell_bound * (1 - plan.sell_fee) - buy_bound * (1 + plan.buy_fee))
        allowed = (buy_bound >= plan.buy_limit
                   and sell_bound <= plan.sell_limit
                   and limit_net >= floor + 1e-9)
        budget.update(
            allowed=allowed,
            reason=("ready" if allowed else
                    "close_pnl_budget_insufficient"),
            exit_reason=decision_reason,
            limit_net_profit_usd=limit_net,
            close_limit_pnl_usd=limit_net,
            cycle_executable_net_pnl=limit_net,
            close_execution_room_bps=close_room_bps,
            optimized_execution_room_bps=selected_close_room,
            optimizer_fill_probability=close_optimizer.get(
                "fill_probability"),
            optimizer_score=close_optimizer.get("score"),
            optimizer_expected_incremental_cost_bps=close_optimizer.get(
                "expected_incremental_cost_bps"),
            optimizer_bucket=close_optimizer.get("bucket"),
            optimizer_reason=close_optimizer.get("reason"),
            close_execution_room_min_bps=close_room_min_bps,
            close_execution_room_target_bps=close_room_target_bps,
            entropy_limit_px=entropy_bound,
            hedge_reserved_limit_px=hedge_bound,
            physical_hard_cap_bps=hard_cap_bps,
            physical_hard_cap_px=physical_bound,
            # Keep the historical field stable for dashboard/API consumers;
            # the detailed fields above identify the optimizer and cap path.
            limit_basis=("pnl_protected" if apply_physical_bound
                         else "economic_budget"),
            limit_basis_detail=("economic_budget_and_hard_cap"
                                if apply_physical_bound
                                else "economic_budget"))
        if not allowed:
            return None
        return PlannedTrade(candidate, plan,
                            (plan.sell_limit * (1.0 - plan.sell_fee)
                             / (plan.buy_limit * (1.0 + plan.buy_fee)) - 1.0)
                            * 1e4,
                            ((plan.sell_limit * (1.0 - plan.sell_fee)
                              / (plan.buy_limit * (1.0 + plan.buy_fee)) - 1.0)
                             * 1e4 - candidate.threshold_bps),
                            close_room_bps, buy_bound, sell_bound, budget)

    def _execution_bounds(self, candidate: TradeCandidate,
                          plan: ArbPlan) -> Optional[PlannedTrade]:
        net_ratio = (plan.sell_limit * (1.0 - plan.sell_fee)
                     / (plan.buy_limit * (1.0 + plan.buy_fee)))
        net_edge = (net_ratio - 1.0) * 1e4
        surplus = net_edge - candidate.threshold_bps
        reduce = candidate.action == StrategyAction.REDUCE
        fill_pnl_close = (reduce and self.cfg.cycle_exit.enabled
                          and self.cfg.cycle_exit.mode == "fill_pnl")
        hard_profit_close = (reduce and self.cfg.profit_guard.enabled
                             and self.cfg.profit_guard.mode == "hard"
                             and not fill_pnl_close)
        profit_close = reduce and (
            fill_pnl_close or self.cfg.profit_guard.enabled)
        close_cost_feedback = max(float(self.close_cost_ewma_bps or 0.0), 0.0)
        required_entry_surplus = (self.cfg.min_edge_surplus_bps
                                  + 0.30 * close_cost_feedback)
        open_action = candidate.action in {
            StrategyAction.OPEN, StrategyAction.ADD}
        bootstrap_active = bool(open_action and self._bootstrap_active())
        cycle_entry_mode = ""
        if reduce:
            cycle_entry_mode = str(
                self._cycle_telemetry.get("entry_mode")
                or self._recovery_context.get("entry_mode") or "")
        bootstrap_cycle = cycle_entry_mode == "bootstrap"
        signal_opportunity = (self._ensure_signal_opportunity(candidate)
                              if open_action else None)

        def record_open_economic(stage: str,
                                 reason: Optional[str] = None) -> None:
            if signal_opportunity is None:
                return
            self._record_opportunity_event(
                opportunity_id=signal_opportunity.get("opportunity_id"),
                stage=stage, direction=candidate.direction, reason=reason,
                payload={
                    "action": candidate.action.value,
                    "expected_cycle_pnl_usd": _safe_float(
                        budget.get("expected_cycle_pnl_usd")),
                    "economic_floor_usd": _safe_float(
                        budget.get("economic_floor_usd")),
                    "economic_required_dislocation_bps": _safe_float(
                        budget.get("economic_required_dislocation_bps")),
                })
        # OPEN/ADD admission is decided by the whole-cycle economic budget
        # below.  The legacy raw-surplus gate charged an entry-only edge
        # against the opportunity before expected execution costs, funding,
        # fees, and the exit leg were considered, which could reject valid
        # opportunities and made the cost model advisory rather than
        # authoritative.  Keep the compatibility gate for non-open paths.
        if (not open_action and not profit_close
                and surplus + 1e-9 < required_entry_surplus):
            return None
        budget = {
            "close_cost_ewma_bps": close_cost_feedback,
            "required_entry_surplus_bps": required_entry_surplus,
            "signal_edge_bps": plan.marginal_premium_bps,
            "quoted_edge_bps": net_edge,
            "entry_mode": "bootstrap" if bootstrap_active else "normal",
            "bootstrap_active": bootstrap_active,
            "bootstrap_cycle": bootstrap_cycle,
        }
        if signal_opportunity is not None:
            budget.update(
                open_opportunity_id=signal_opportunity.get("opportunity_id"),
                open_opportunity_key=signal_opportunity.get(
                    "opportunity_key"))
        if open_action:
            # OPEN/ADD must be evaluated with the same whole-cycle economics
            # that the dashboard calls ``expected_cycle_pnl``.  The old
            # ``plan.exp_edge_usd`` value is only the entry slice edge and can
            # be negative even when the projected round trip is positive.
            economics = self._cycle_profit_budget(candidate, plan)
            budget = {**budget, **economics}
            expected_cycle_pnl = _safe_float(
                economics.get("net_profit_usd"))
            budget["expected_cycle_pnl_usd"] = expected_cycle_pnl
            self.profit_checks[
                f"{candidate.action.value}:{candidate.direction}"] = budget
            if expected_cycle_pnl is None:
                budget.update(allowed=False,
                              reason="expected_cycle_pnl_unavailable")
                record_open_economic("economic_rejected",
                                     "economic_pnl_below_floor")
                self._skiplog(
                    "[OPEN ECONOMICS] action=%s direction=%s skipped: "
                    "expected cycle PnL unavailable (%s)",
                    candidate.action.value, candidate.direction,
                    economics.get("reason"))
                return None
            if not economics.get("books_fresh", True):
                budget.update(allowed=False, reason="stale_or_skewed_books")
                record_open_economic("economic_rejected", "book_stale")
                return None
            if (_safe_float(economics.get("cross_venue_skew_ms")) or 0.0) \
                    > float(economics.get("max_cross_venue_skew_ms", 0.0)):
                budget.update(allowed=False, reason="stale_or_skewed_books")
                record_open_economic("economic_rejected", "cross_skew")
                return None
            economic_floor = self._economic_floor_usd(
                max(plan.buy_notional, plan.sell_notional, 0.0))
            economics_floor = _safe_float(
                economics.get("economic_floor_usd"))
            if economics_floor is not None:
                economic_floor = economics_floor
            if (bootstrap_active
                    and not bool(economics.get("bootstrap_gross_ok", True))):
                budget.update(allowed=False,
                              reason="bootstrap_gross_below_floor")
                record_open_economic("economic_rejected",
                                     "bootstrap_gross_below_floor")
                return None
            if not bootstrap_active and expected_cycle_pnl < -1e-10:
                budget.update(allowed=False,
                              reason="expected_pnl_negative")
                record_open_economic("economic_rejected",
                                     "economic_pnl_below_floor")
                self._skiplog(
                    "[OPEN ECONOMICS] action=%s direction=%s skipped: "
                    "expected cycle PnL=$%.6f",
                    candidate.action.value, candidate.direction,
                    expected_cycle_pnl)
                return None
            if expected_cycle_pnl + 1e-10 < economic_floor:
                budget.update(allowed=False,
                              reason="expected_cycle_pnl_below_floor",
                              economic_floor_usd=economic_floor)
                record_open_economic("economic_rejected",
                                     "economic_pnl_below_floor")
                self._skiplog(
                    "[OPEN ECONOMICS] action=%s direction=%s skipped: "
                    "expected cycle PnL=$%.6f below floor=$%.6f",
                    candidate.action.value, candidate.direction,
                    expected_cycle_pnl, economic_floor)
                return None
            if self.cfg.profit_guard.enabled and not economics.get("allowed"):
                record_open_economic("economic_rejected",
                                     "economic_pnl_below_floor")
                self._skiplog(
                    "[PROFIT GUARD] action=%s direction=%s exit_mode=%s "
                    "allowed_floor_usd=%s expected_cycle_pnl=%s reason=%s",
                    candidate.action.value, candidate.direction,
                    economics.get("exit_mode"),
                    economics.get("min_profit_usd"), expected_cycle_pnl,
                    economics.get("reason"))
                return None

            # Reprice every OPEN/ADD through the same economic/physical
            # budget, even when dynamic first-leg widening is disabled. The
            # expected PnL is the admission test; protected edge is only an
            # audit metric and must not be compared with the raw threshold.
            policy = getattr(self.cfg, "reduce_execution", None)
            protected = self._protected_open_metrics(candidate, plan)
            if protected is None:
                budget.update(allowed=False,
                              reason="protected_open_recheck_failed")
                record_open_economic("economic_rejected",
                                     "execution_room_insufficient")
                return None
            budget.update(protected)
            if "expected_cycle_pnl_usd" in protected:
                protected_pnl = _safe_float(
                    protected.get("expected_cycle_pnl_usd"))
                protected_floor = float(protected.get(
                    "economic_floor_usd", economic_floor))
                protected_bootstrap = bool(protected.get(
                    "bootstrap_active", bootstrap_active))
                if protected_pnl is None or (
                        not protected_bootstrap and protected_pnl < -1e-10):
                    budget.update(allowed=False,
                                  reason="expected_pnl_negative")
                    record_open_economic("economic_rejected",
                                         "economic_pnl_below_floor")
                    return None
                if protected_pnl + 1e-10 < protected_floor:
                    budget.update(allowed=False,
                                  reason="expected_cycle_pnl_below_floor")
                    record_open_economic("economic_rejected",
                                  "economic_pnl_below_floor")
                    return None
                if (protected_bootstrap
                        and not bool(protected.get("bootstrap_gross_ok", True))):
                    budget.update(allowed=False,
                                  reason="bootstrap_gross_below_floor")
                    record_open_economic("economic_rejected",
                                         "bootstrap_gross_below_floor")
                    return None
                if protected.get("economic_bound_not_marketable"):
                    budget.update(allowed=False,
                                  reason="economic_bound_not_marketable")
                    record_open_economic("economic_rejected",
                                         "execution_room_insufficient")
                    self._skiplog(
                        "[OPEN EXECUTION] action=%s direction=%s skipped: "
                        "economic first-leg bound is not marketable",
                        candidate.action.value, candidate.direction)
                    return None
                buy_bound = _safe_float(protected.get("buy_bound_px"))
                sell_bound = _safe_float(protected.get("sell_bound_px"))
                if buy_bound is None or sell_bound is None:
                    budget.update(allowed=False,
                                  reason="protected_open_recheck_failed")
                    record_open_economic("economic_rejected",
                                         "execution_room_insufficient")
                    return None
                budget["allowed"] = True
                record_open_economic("economic_qualified")
                self._observe_cycle_budget(candidate, budget)
                first_room = max(float(protected.get(
                                       "first_leg_execution_room_bps"
                                       ) or protected.get("first_leg_room_bps")
                                       or protected.get("execution_room_bps")
                                       or 0.0), 0.0)
                budget["first_leg_execution_room_bps"] = first_room
                # Keep the legacy field as the configured protection value;
                # the actual live room is exposed separately in the budget.
                return PlannedTrade(
                    candidate, plan, net_edge, surplus,
                    self._first_leg_slippage_bps(candidate.action),
                    buy_bound, sell_bound, budget)

            # Source compatibility for older integrations that override the
            # protected-metrics hook and still return the old edge/room shape.
            required = _safe_float(protected.get("required_entry_edge_bps"))
            protected_edge = _safe_float(
                protected.get("protected_open_edge_bps"))
            room = _safe_float(protected.get("execution_room_bps"))
            if (protected_edge is None or required is None
                    or protected_edge + 1e-9 < required):
                budget.update(allowed=False,
                              reason="protected_open_edge_below_threshold")
                record_open_economic("economic_rejected",
                                     "economic_pnl_below_floor")
                return None
            if protected.get("buy_bound_px") is None \
                    or protected.get("sell_bound_px") is None:
                record_open_economic("economic_rejected",
                                     "execution_room_insufficient")
                return None
            first_room = max(float(protected.get("first_leg_room_bps")
                                   or protected.get("execution_room_bps")
                                   or 0.0), 0.0)
            budget["first_leg_execution_room_bps"] = first_room
            record_open_economic("economic_qualified")
            return PlannedTrade(
                candidate, plan, net_edge, surplus,
                self._first_leg_slippage_bps(candidate.action),
                protected["buy_bound_px"],
                protected["sell_bound_px"], budget)

        if self.cfg.profit_guard.enabled or fill_pnl_close:
            budget = self._cycle_profit_budget(candidate, plan)
            self.profit_checks[f"{candidate.action.value}:{candidate.direction}"] = budget
            self._observe_cycle_budget(candidate, budget)
            if not budget["allowed"]:
                self._skiplog(
                    "[PROFIT GUARD] action=%s direction=%s exit_mode=%s "
                    "allowed_floor_usd=%s quoted_net=%s protected_net=%s "
                    "held_sec=%s reason=%s",
                    candidate.action.value, candidate.direction,
                    budget.get("exit_mode"),
                    budget.get("allowed_floor_usd",
                               budget.get("min_profit_usd")),
                    budget.get("net_profit_usd"),
                    budget.get("limit_net_profit_usd"),
                    budget.get("held_sec"), budget.get("reason"))
                return None
        if fill_pnl_close and not budget.get("risk_exit"):
            protected = self._build_pnl_protected_close_bound(
                candidate, plan, budget,
                budget.get("allowed_floor_usd", self.cfg.cycle_exit.normal_min_profit_usd),
                apply_physical_bound=True)
            if protected is None:
                self._skiplog(
                    "[PROFIT GUARD] action=%s direction=%s exit_mode=%s "
                    "allowed_floor_usd=%s quoted_net=%s protected_net=%s "
                    "held_sec=%s reason=%s",
                    candidate.action.value, candidate.direction,
                    budget.get("exit_mode"),
                    budget.get("allowed_floor_usd",
                               budget.get("min_profit_usd")),
                    budget.get("net_profit_usd"),
                    budget.get("limit_net_profit_usd"),
                    budget.get("held_sec"), budget.get("reason"))
            return protected
        if hard_profit_close:
            protected = self._build_pnl_protected_close_bound(
                candidate, plan, budget,
                self.cfg.profit_guard.min_profit_usd,
                apply_physical_bound=True)
            if protected is None:
                self._skiplog(
                    "[PROFIT GUARD] action=%s direction=%s exit_mode=%s "
                    "allowed_floor_usd=%s quoted_net=%s protected_net=%s "
                    "held_sec=%s reason=%s",
                    candidate.action.value, candidate.direction,
                    budget.get("exit_mode"),
                    budget.get("allowed_floor_usd",
                               budget.get("min_profit_usd")),
                    budget.get("net_profit_usd"),
                    budget.get("limit_net_profit_usd"),
                    budget.get("held_sec"), budget.get("reason"))
            return protected
        # Entropy is the first leg. Its IOC protection is deliberately fixed:
        # the former surplus split routinely produced <0.4 bps limits, which
        # canceled before the two venues could settle. Whole-cycle economics
        # is checked separately above; these limits do not guarantee profit.
        per_leg = self._first_leg_slippage_bps(candidate.action)
        slip = per_leg / 1e4
        buy_raw = plan.buy_limit * (1 + slip)
        sell_raw = plan.sell_limit * (1 - slip)
        buy_round = getattr(candidate.buy, "px_round", None)
        sell_round = getattr(candidate.sell, "px_round", None)
        buy_bound = (buy_round(buy_raw, round_up=False)
                     if buy_round else buy_raw)
        sell_bound = (sell_round(sell_raw, round_up=True)
                      if sell_round else sell_raw)
        # A coarse tick must not silently enlarge the adverse budget. A bound
        # that rounds through the planned marginal level is not executable.
        if buy_bound < plan.buy_limit or sell_bound > plan.sell_limit:
            return None
        buy_room = (buy_bound / plan.buy_limit - 1.0) * 1e4
        sell_room = (1.0 - sell_bound / plan.sell_limit) * 1e4
        if buy_room > per_leg + 1e-7 or sell_room > per_leg + 1e-7:
            return None
        return PlannedTrade(candidate, plan, net_edge, surplus, per_leg,
                            buy_bound, sell_bound, budget)

    # -------------------------------------------------------------- strategy

    async def _strategy_loop(self) -> None:
        while not self.stop.is_set():
            await self._update_evt.wait()
            self._update_evt.clear()
            if self.stop.is_set():
                break
            try:
                await self._evaluate()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("evaluate failed")

    def _schedule_poke(self, delay: float) -> None:
        loop = asyncio.get_running_loop()
        due = loop.time() + max(delay, 0.01)
        if self._poke_due is not None and self._poke_due <= due + 0.02:
            return

        def _fire() -> None:
            self._poke_due = None
            self._update_evt.set()

        self._poke_due = due
        loop.call_at(due, _fire)

    def _skiplog(self, fmt: str, *args) -> None:
        now = time.time()
        if now - self._last_skiplog >= 2.0:
            self._last_skiplog = now
            log.info(fmt, *args)

    async def _evaluate(self) -> None:
        cfg = self.cfg
        if self.halted:
            return
        if not self.record_only and not self.startup_reconciled:
            self._skiplog("strategy blocked: startup reconciliation pending")
            return
        if self.active_execution is not None:
            # A timed-out or mismatched cycle owns the strategy until its
            # pending order is resolved and the pair is verified balanced.
            # Recovery is allowed to continue; a second independent cycle is
            # not.
            await self._recover_active_execution(immediate=False)
            return
        if (not self.record_only
                and self.reconciliation.get("status") == "blocked"):
            self._skiplog("strategy blocked: reconciliation left net delta")
            return
        if self._orphaned_inventory or self._emergency_pending:
            # Unknown-cost or externally discovered inventory is never a
            # normal REDUCE/OPEN candidate. Keep servicing reduce-only exit
            # orders until every venue is individually flat.
            await self._emergency_exit_positions(
                "orphaned_inventory" if self._orphaned_inventory
                else "emergency_order_recovery")
            return
        now = time.time()
        margin = self._margin_snapshot()
        if (not self.record_only and margin["hard_risk"]
                and not self._positions_flat()):
            log.error("[MARGIN] critical margin state; starting hard-risk "
                      "reduce-only recovery")
            await self._emergency_exit_positions("margin_risk")
            return
        best = self._scan(now)
        if best is None:
            return
        cooldown = (0.0 if best.cycle_budget.get("risk_exit") else
                    self._execution_cooldown_sec(best.candidate.action))
        if now - self.last_trade_ts < cooldown:
            self._schedule_poke(cooldown - (now - self.last_trade_ts))
            return
        buy, sell = best.candidate.buy, best.candidate.sell
        # _scan verified both locks free and nothing ran since (no awaits),
        # so these acquires take the no-suspension fast path
        await self._vlock(buy.key).acquire()
        await self._vlock(sell.key).acquire()
        # run as a task so a shutdown cancels the strategy loop's await, never
        # the in-flight execution itself (both legs must settle)
        t = asyncio.create_task(self._execute_locked(best))
        self._exec_tasks.add(t)
        t.add_done_callback(self._exec_tasks.discard)
        await asyncio.shield(t)

    async def _execute_locked(self, trade: PlannedTrade) -> None:
        """Run one execution while holding both venue locks (acquired by the
        caller), then release them and settle the aftermath: unresolved
        outcomes escalate to reconcile, everything else gets a net-delta
        check."""
        unresolved = False
        buy, sell = trade.candidate.buy, trade.candidate.sell
        self._execution_inflight = True
        try:
            unresolved = await self._execute(trade)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("execute failed")
        finally:
            self._execution_inflight = False
            self._vlock(buy.key).release()
            self._vlock(sell.key).release()
        # Recovery must run after the two venue locks are released: the
        # recovery path may need to send another order on the hedge venue.
        if self.active_execution is not None:
            await self._recover_active_execution(immediate=True)
            if self.active_execution is not None:
                self._reconcile_evt.set()
        elif unresolved:
            self._reconcile_evt.set()
        else:
            await self._maybe_hedge()
        self._update_evt.set()  # freed venues may have a queued opportunity

    def _scan(self, now: float):
        """Choose legal actions from reconciled position state, then price."""
        cfg = self.cfg
        self._record_open_quote(now)
        flat_scan = self._pair_state() == PairState.FLAT
        scan_block_reason: Optional[str] = None

        def note_block(reason: str) -> None:
            nonlocal scan_block_reason
            if scan_block_reason is None:
                scan_block_reason = reason

        if self.active_execution is not None:
            if flat_scan:
                self._record_open_blocker("execution_recovery", now)
            self._skiplog("strategy blocked: execution %s is %s",
                          self.active_execution.cycle_id,
                          self.active_execution.phase.value)
            self.last_selected = None
            return None
        candidates = self._build_candidates(now)
        legal_keys = {f"{c.action.value}:{c.direction}" for c in candidates}
        for key in list(self._armed):
            if key not in legal_keys:
                self._armed.pop(key, None)
        planned: List[PlannedTrade] = []
        for candidate in candidates:
            buy, sell = candidate.buy, candidate.sell
            dkey = f"{candidate.action.value}:{candidate.direction}"
            hard_age_sec = self._hard_market_data_age_sec()
            if not (buy.book.is_fresh(hard_age_sec)
                    and sell.book.is_fresh(hard_age_sec)):
                if flat_scan:
                    note_block("book_stale")
                continue
            if not (buy.ready_to_trade() and sell.ready_to_trade()):
                if flat_scan:
                    note_block("venue_not_ready")
                continue
            if self._venue_down:
                if flat_scan:
                    note_block("venue_degraded")
                continue  # a venue in outage pauses the (only) pair
            if self._vlock(buy.key).locked() or self._vlock(sell.key).locked():
                if flat_scan:
                    note_block("execution_recovery")
                continue  # mid-execution or mid-reconcile
            if self._venue_limited(buy) or self._venue_limited(sell):
                if flat_scan:
                    note_block("venue_degraded")
                continue  # reactive 429 exclusion
            if not (self._venue_rate_ok(buy, candidate.action)
                    and self._venue_rate_ok(sell, candidate.action)):
                self._note_order_budget_block(
                    (buy, sell), candidate.action)
                if flat_scan:
                    note_block("order_budget")
                self._skiplog("%s deferred: venue order budget exhausted", dkey)
                continue
            # never refire into books that predate the venue's own last trade
            if (buy.book.last_update_ts <= buy.last_traded_ts
                    or sell.book.last_update_ts <= sell.last_traded_ts):
                if flat_scan:
                    note_block("book_stale")
                continue
            min_notional = (max(self._taker_minimums(v, reduce_only=True,
                                qty=min(abs(buy.position), abs(sell.position)))[1]
                                for v in (buy, sell))
                            if candidate.reduce_only else self._min_notional)
            if candidate.cap_notional < min_notional:
                if flat_scan:
                    note_block("min_notional")
                continue
            # With the profit guard, normal exits are driven by actual FIFO
            # economics, not by whether the statistical midline was crossed.
            plan_threshold = (-9999.0 if candidate.action == StrategyAction.REDUCE
                              and (cfg.profit_guard.enabled
                                   or (cfg.cycle_exit.enabled
                                       and cfg.cycle_exit.mode == "fill_pnl"))
                              else candidate.threshold_bps)
            plan, reason = self._plan(buy, sell, candidate.cap_notional,
                                      plan_threshold, reduce_only=candidate.reduce_only)
            edge_present = reason not in ("no_edge", "empty_book")
            if not edge_present:
                if flat_scan:
                    note_block("no_executable_edge")
                if candidate.action in {StrategyAction.OPEN, StrategyAction.ADD}:
                    self._mark_open_signal_reset(
                        self._open_opportunity_key(candidate), candidate, now)
                self._armed[dkey] = None
                continue
            if plan is None:
                if flat_scan:
                    note_block("no_executable_edge")
                self._armed[dkey] = None
                continue
            if (candidate.action in {StrategyAction.OPEN, StrategyAction.ADD}
                    and self._open_opportunity_blocked(candidate, now)):
                self._armed[dkey] = None
                self._skiplog(
                    "%s deferred: same OPEN opportunity is cooling or "
                    "waiting for signal reset", dkey)
                if flat_scan:
                    note_block("opportunity_locked")
                continue
            if candidate.action == StrategyAction.REDUCE:
                close_qty = floor_step(min(plan.qty, abs(self.entropy.position),
                                           abs(self.hedge.position)), self._step)
                if close_qty <= 0:
                    continue
                remaining_qty = floor_step(min(abs(self.entropy.position),
                                               abs(self.hedge.position)), self._step)
                full_buy = self._depth_quote(buy, True, remaining_qty)
                full_sell = self._depth_quote(sell, False, remaining_qty)
                if (remaining_qty > close_qty and full_buy and full_sell
                        and max(full_buy[1], full_sell[1]) <= cfg.max_order_notional + 1e-8):
                    close_qty = remaining_qty
                if abs(close_qty - plan.qty) > 1e-12:
                    b = self._depth_quote(buy, True, close_qty)
                    s = self._depth_quote(sell, False, close_qty)
                    if b is None or s is None:
                        continue
                    plan = replace(plan, qty=close_qty, buy_limit=b[0],
                                   sell_limit=s[0], buy_notional=b[1],
                                   sell_notional=s[1],
                                   marginal_premium_bps=(s[0] / b[0] - 1) * 1e4)
                minima = [self._taker_minimums(v, reduce_only=True, qty=close_qty)
                          for v in (buy, sell)]
                if (close_qty < max(m[0] for m in minima) or
                        plan.buy_notional + 1e-8 < minima[0][1] or
                        plan.sell_notional + 1e-8 < minima[1][1]):
                    continue
            if (candidate.action == StrategyAction.ADD
                    and self._cycle_hold_state in {
                        "loss_recovery_hold", "long_hold",
                        "forced_release_pending"}):
                continue
            if candidate.action != StrategyAction.REDUCE:
                headroom = self._headroom(buy, sell, plan.buy_limit)
                if headroom < plan.buy_notional:
                    plan, _ = self._plan(
                        buy, sell, min(candidate.cap_notional, headroom),
                        candidate.threshold_bps)
                if plan is None:
                    self._skiplog("%s blocked by position caps (headroom $%.0f)",
                                  dkey, max(headroom, 0.0))
                    if flat_scan:
                        note_block("position_cap")
                    continue
            trade = self._execution_bounds(candidate, plan)
            if trade is None:
                if flat_scan:
                    note_block("protected_edge")
                self._armed[dkey] = None
                continue
            cross_skew_ms = abs(buy.book.last_update_ts
                                - sell.book.last_update_ts) * 1000
            forced_reduce = (candidate.action == StrategyAction.REDUCE
                             and bool(trade.cycle_budget.get("risk_exit")))
            if (cross_skew_ms > self._hard_cross_skew_ms()
                    and not forced_reduce):
                self._armed[dkey] = None
                self._skiplog("%s blocked by cross-venue book skew %.1fms",
                              dkey, cross_skew_ms)
                if flat_scan:
                    note_block("cross_skew")
                continue
            armed = self._armed.get(dkey)
            persist_sec = self._execution_persist_sec(candidate.action)
            if forced_reduce:
                armed, persist_sec = now, 0.0
            if armed is None:
                self._armed[dkey] = now
                self._schedule_poke(persist_sec)
                continue
            if now - armed < persist_sec:
                self._schedule_poke(persist_sec - (now - armed))
                continue
            if candidate.action in {StrategyAction.OPEN, StrategyAction.ADD}:
                # Do not create a dedupe record while a signal is merely being
                # armed.  A pre-send economic failure must leave the signal
                # eligible for a later valid scan; only a selected opportunity
                # (and its bounded order campaign) owns the dedupe key.
                self._ensure_open_opportunity(candidate, trade, now)
            planned.append(trade)
        if not planned:
            if flat_scan and scan_block_reason is not None:
                self._record_open_blocker(scan_block_reason, now)
            self.last_selected = None
            return None
        self._clear_order_budget_block()
        if flat_scan:
            self._record_open_blocker(None, now)
        reductions = [p for p in planned
                      if p.candidate.action == StrategyAction.REDUCE]
        pool = reductions or planned
        best = max(pool, key=lambda p: (p.edge_surplus_bps,
                                        p.plan.exp_edge_usd))
        self.last_selected = best
        return best

    # ------------------------------------------------------------- execution

    @staticmethod
    def _failed_send(exc: BaseException) -> dict:
        # An unexpected adapter exception does not prove rejection. The
        # persisted client reference must be resolved before any new send.
        return {"status": "send-failed", "filled_base": 0.0,
                "avg_px": None, "err": repr(exc), "unresolved": True}

    def _live_taker_bound(self, venue, is_buy: bool,
                          slippage_bps: float) -> Optional[float]:
        """Price-protected bound from the freshest book for a follow-up leg."""
        ref = venue.book.best_ask() if is_buy else venue.book.best_bid()
        if ref is None:
            return None
        slip = slippage_bps / 1e4
        raw = ref * (1 + slip) if is_buy else ref * (1 - slip)
        return venue.px_round(raw, round_up=False if is_buy else True)

    def _begin_execution(self, trade: PlannedTrade) -> ExecutionState:
        candidate = trade.candidate
        self._bootstrap_note_decision(trade)
        entropy_is_buy = candidate.buy is self.entropy
        state = ExecutionState(
            cycle_id=f"{int(time.time() * 1000)}-{self.trades + len(self._exec_tasks)}",
            action=candidate.action.value,
            direction=candidate.direction,
            planned_qty=trade.plan.qty,
            entropy_is_buy=entropy_is_buy,
            hedge_is_buy=not entropy_is_buy,
            reduce_only=candidate.reduce_only,
            first_limit_px=(trade.buy_bound if entropy_is_buy
                            else trade.sell_bound),
            first_position_before=self.entropy.position,
            hedge_position_before=self.hedge.position,
        )
        self.active_execution = state
        self._active_trade = trade
        self._recovery_context = self._execution_context(trade)
        self._recovery_context.update({
            "execution_id": state.cycle_id,
            "created_ts": state.created_ts,
            "inventory_cycle_id": (
                state.cycle_id if candidate.action == StrategyAction.OPEN
                else self.cycle.cycle_id),
        })
        self._ensure_cycle_telemetry(state, trade)
        self._pending_execution_record = None
        self.last_execution_outcome = None
        self._checkpoint_execution(
            state, event_type="created",
            payload={"planned_qty": state.planned_qty,
                     "first_position_before": state.first_position_before,
                     "hedge_position_before": state.hedge_position_before})
        return state

    def _restore_cycle_anchor_from_execution(self, state: ExecutionState,
                                             outcome: str) -> None:
        """Restore cycle metadata only after exact fills and pair proof."""
        if (not self.cfg.cycle_exit.enabled
                or outcome not in {"paired", "partial_paired"}
                or not self._pair_positions_balanced()
                or not self._execution_cost_ready(state)
                or not self._execution_fill_coverage_ready(state)):
            return
        action = str(state.action or "").lower()
        if action == StrategyAction.OPEN.value:
            direction = cycle_direction_for_pair_state(self._pair_state())
            if direction is None:
                return
            context = self._recovery_context
            entry_reference = context.get("cycle_entry_reference_bps")
            entry_midline = (entry_reference
                             if entry_reference is not None
                             else context.get("cycle_entry_midline_bps"))
            if entry_midline is None:
                entry_midline = context.get("active_midline_bps")
            if entry_midline is None:
                entry_midline = self.model_snapshot.active_midline_bps
            entry_band = context.get("effective_entry_band_bps")
            if entry_band is None:
                entry_band = 0.0
            self.cycle.start(
                direction=direction, opened_ts=state.created_ts,
                entry_midline_bps=float(entry_midline),
                entry_sigma_bps=context.get("cycle_entry_sigma_bps"),
                entry_band_bps=float(entry_band),
                entry_threshold_bps=float(
                    context.get("threshold_bps") or 0.0),
                paired_qty=min(state.first_filled, state.hedge_filled),
                cycle_id=state.cycle_id,
                entry_reference_bps=entry_reference,
                entry_reference_mode=context.get(
                    "cycle_entry_reference_mode"),
                entry_active_midline_bps=context.get(
                    "cycle_entry_active_midline_bps"),
                entry_dynamic_center_bps=context.get(
                    "cycle_entry_dynamic_center_bps"))
        elif action == StrategyAction.ADD.value:
            if self.cycle.active and self.cycle.anchored:
                self.cycle.add_count += 1
                self.cycle.peak_paired_qty = max(
                    self.cycle.peak_paired_qty,
                    min(state.first_filled, state.hedge_filled))
        elif action == StrategyAction.REDUCE.value:
            if self.cycle.active:
                self.cycle.reduce_count += 1
        self._persist_cycle()

    def _finish_execution(self, outcome: str) -> None:
        state = self.active_execution
        self.last_execution_outcome = outcome
        if state is not None:
            if (not state.hedge_order_ref and state.hedge_filled == 0
                    and state.pending_leg != "hedge"):
                state.hedge_order_status = state.hedge_order_status or "not-sent"
            if outcome == "no_fill":
                self._armed[f"{state.action}:{state.direction}"] = None
                self.entropy.last_traded_ts = self.hedge.last_traded_ts = time.time()
            state.transition(ExecutionPhase.VERIFIED, f"complete:{outcome}")
            self._update_cycle_telemetry(state, outcome=outcome)
        if self._pending_execution_record is not None:
            self._finalize_execution_record()
        elif state is not None:
            self._finalize_recovered_execution_record(state, outcome)
        if state is not None:
            try:
                self.ledger.complete_execution(
                    state, outcome, context=self._recovery_context)
            except Exception as exc:
                self.halted = True
                log.critical("[EXEC %s] completion ledger write failed: %r",
                             state.cycle_id, exc)
        self.active_execution = None
        self._active_trade = None
        self._recovery_context = {}

    def _finalize_recovered_execution_record(self, state: ExecutionState,
                                             outcome: str) -> None:
        """Create the missing attempt row when a cycle finished after restart."""
        self._restore_cycle_anchor_from_execution(state, outcome)
        context = dict(self._recovery_context)
        entropy_fill = state.first_filled
        hedge_fill = state.hedge_filled
        if state.entropy_is_buy:
            buy_fill, sell_fill = entropy_fill, hedge_fill
        else:
            buy_fill, sell_fill = hedge_fill, entropy_fill
        fill_edge = self.ledger.execution_fill_edge(state.cycle_id)
        budget = context.get("cycle_budget") or {}
        open_campaign = context.get("open_campaign") or {}
        payload = {
            "trade_key": state.cycle_id,
            "execution_id": state.cycle_id,
            "ts": state.created_ts,
            "action": state.action,
            "direction": state.direction,
            "qty": state.planned_qty,
            "buy_notional": context.get("buy_notional", 0.0),
            "sell_notional": context.get("sell_notional", 0.0),
            "buy_fill": buy_fill,
            "sell_fill": sell_fill,
            "buy_status": (state.first_order_status
                           if state.entropy_is_buy
                           else state.hedge_order_status) or "recovered",
            "sell_status": (state.hedge_order_status
                            if state.entropy_is_buy
                            else state.first_order_status) or "recovered",
            "buy_venue": context.get(
                "buy_venue", "entropy" if state.entropy_is_buy else "hedge"),
            "sell_venue": context.get(
                "sell_venue", "hedge" if state.entropy_is_buy else "entropy"),
            "threshold_bps": context.get("threshold_bps"),
            "net_edge_bps": context.get("net_edge_bps"),
            "fill_edge_usd": fill_edge,
            "outcome": outcome if outcome in {
                "paired", "partial_paired", "one_leg", "no_fill"
            } else "one_leg",
            "reduce_only": state.reduce_only,
            "recovered_after_restart": True,
            # Keep the opportunity/economics decision beside the final
            # outcome so the durable funnel does not have to reconstruct it
            # from transient process state after a restart.
            "open_opportunity_id": budget.get("open_opportunity_id"),
            "open_opportunity_key": budget.get("open_opportunity_key"),
            "open_campaign_id": (open_campaign.get("open_campaign_id")
                                  or budget.get("open_opportunity_id")),
            "economic_opportunity": budget.get("economic_opportunity"),
            "expected_cycle_pnl_usd": budget.get(
                "expected_cycle_pnl_usd", budget.get("net_profit_usd")),
            "min_expected_cycle_profit_usd": budget.get(
                "economic_floor_usd"),
            "statistical_threshold_bps": budget.get(
                "statistical_threshold_bps"),
            "economic_required_dislocation_bps": budget.get(
                "economic_required_dislocation_bps"),
            "effective_entry_distance_bps": budget.get(
                "effective_entry_distance_bps"),
            "economic_surplus_bps": budget.get("economic_surplus_bps"),
            "latency_required_room_bps": budget.get(
                "latency_required_room_bps"),
            "first_leg_execution_room_bps": budget.get(
                "first_leg_execution_room_bps"),
            "physical_hard_cap_bps": budget.get("physical_hard_cap_bps"),
            "actual_entropy_limit_px": budget.get("actual_entropy_limit_px"),
            "campaign_result": open_campaign.get("final_result"),
        }
        try:
            self.ledger.record_trade_attempt(payload, source="recovered")
        except Exception as exc:
            self.halted = True
            log.critical("[EXEC %s] recovered trade ledger write failed: %r",
                         state.cycle_id, exc)
        self.recent_trades.append({
            "ts": state.created_ts, "execution_id": state.cycle_id,
            "direction": state.direction, "qty": state.planned_qty,
            "notional": (abs(float(context.get("buy_notional") or 0.0))
                          + abs(float(context.get("sell_notional") or 0.0))) / 2,
            "prem_bps": context.get("marginal_premium_bps"),
            "exp": context.get("exp_edge_usd"), "fill": fill_edge,
            "status": f"recovered/{outcome}",
            "ok": outcome == "paired", "action": state.action})

    def _finalize_execution_record(self) -> None:
        """Write one strategy-attempt record after recovery is terminal.

        A recovery may run after the original execution task releases its
        venue locks, so bookkeeping cannot be done only at the end of the
        first API call.  The record is intentionally emitted once, with the
        final confirmed quantities.
        """
        record = self._pending_execution_record
        if record is None:
            return
        state: ExecutionState = record["state"]
        trade: PlannedTrade = record["trade"]
        candidate = trade.candidate
        outcome = self.last_execution_outcome
        entropy_fill = state.first_filled
        hedge_fill = state.hedge_filled
        if state.entropy_is_buy:
            final_bfill, final_sfill = entropy_fill, hedge_fill
        else:
            final_bfill, final_sfill = hedge_fill, entropy_fill
        final_matched = min(final_bfill, final_sfill)
        final_paired = outcome == "paired"
        if final_paired:
            self.consec_errors = 0
            self.trades += 1
            self.total_exp_edge += (
                trade.plan.exp_edge_usd * final_matched / trade.plan.qty)
        self._update_cycle_after_execution(candidate, final_matched,
                                           final_paired, state=state)
        initial_paired = record["initial_paired"]
        status = f"{record['buy_status']}/{record['sell_status']}"
        if final_paired and not initial_paired:
            status += "/recovered"
        fill_edge = self.ledger.execution_fill_edge(state.cycle_id)
        if fill_edge is None:
            fill_edge = record["fill_edge"]
        budget = trade.cycle_budget or {}
        open_campaign = self._recovery_context.get("open_campaign") or {}
        try:
            self.ledger.record_trade_attempt({
                "trade_key": state.cycle_id,
                "execution_id": state.cycle_id,
                "ts": record.get("ts", state.created_ts),
                "action": candidate.action.value,
                "direction": candidate.direction,
                "qty": trade.plan.qty,
                "buy_notional": trade.plan.buy_notional,
                "sell_notional": trade.plan.sell_notional,
                "buy_fill": final_bfill,
                "sell_fill": final_sfill,
                "buy_status": state.first_order_status
                if state.entropy_is_buy else state.hedge_order_status,
                "sell_status": state.hedge_order_status
                if state.entropy_is_buy else state.first_order_status,
                "buy_venue": candidate.buy.key,
                "sell_venue": candidate.sell.key,
                "threshold_bps": candidate.threshold_bps,
                "net_edge_bps": trade.net_marginal_edge_bps,
                "fill_edge_usd": fill_edge,
                "outcome": outcome if outcome in {
                    "paired", "partial_paired", "one_leg", "no_fill"
                } else "one_leg",
                "reduce_only": candidate.reduce_only,
                "csv_status": status,
                # These fields are deliberately flattened into the attempt
                # payload.  The dashboard can then report signal → economic
                # opportunity → campaign → fill without trusting RAM.
                "open_opportunity_id": budget.get("open_opportunity_id"),
                "open_opportunity_key": budget.get("open_opportunity_key"),
                "open_campaign_id": (open_campaign.get("open_campaign_id")
                                      or budget.get("open_opportunity_id")),
                "economic_opportunity": budget.get("economic_opportunity"),
                "expected_cycle_pnl_usd": budget.get(
                    "expected_cycle_pnl_usd", budget.get("net_profit_usd")),
                "min_expected_cycle_profit_usd": budget.get(
                    "economic_floor_usd"),
                "statistical_threshold_bps": budget.get(
                    "statistical_threshold_bps"),
                "economic_required_dislocation_bps": budget.get(
                    "economic_required_dislocation_bps"),
                "effective_entry_distance_bps": budget.get(
                    "effective_entry_distance_bps"),
                "economic_surplus_bps": budget.get("economic_surplus_bps"),
                "latency_required_room_bps": budget.get(
                    "latency_required_room_bps"),
                "first_leg_execution_room_bps": budget.get(
                    "first_leg_execution_room_bps"),
                "physical_hard_cap_bps": budget.get(
                    "physical_hard_cap_bps"),
                "actual_entropy_limit_px": budget.get(
                    "actual_entropy_limit_px"),
                "campaign_result": open_campaign.get("final_result"),
            }, source="engine")
        except Exception as exc:
            self.halted = True
            log.critical("[EXEC %s] trade ledger write failed: %r",
                         state.cycle_id, exc)
        self._record_trade(candidate.direction, trade.plan,
                           fill_edge if final_paired or not record["unresolved"]
                           else None,
                           status, final_paired, candidate.action,
                           execution_id=state.cycle_id)
        self._log_csv(trade, final_paired, final_bfill, final_sfill,
                      record["buy_status"], record["sell_status"], fill_edge,
                      record["inv_bps"])
        self._pending_execution_record = None

    @staticmethod
    def _fill_amount(info: dict) -> float:
        try:
            value = float(info.get("filled_base") or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return max(value, 0.0)

    @staticmethod
    def _order_ref(info: dict):
        return info.get("order_ref") or info.get("client_order_id")

    @staticmethod
    def _deterministic_order_ref(state: ExecutionState, leg: str,
                                 attempt: int = 0) -> str:
        """Return a stable client id for a send intent.

        The intent is checkpointed before the network call.  If the process
        dies after an exchange accepts the order but before the response is
        observed, restart recovery can resolve the same client id instead of
        submitting a second order.  The numeric form is accepted by both
        Hyperliquid cloids and Lighter client order indexes.  Lighter's
        client order index is limited to 48 bits, so the reference stays
        inside that range.
        """
        raw = f"entropy-arb:{state.cycle_id}:{leg}:{int(attempt)}".encode()
        return Engine._stable_order_ref(raw)

    @staticmethod
    def _stable_order_ref(raw: bytes | str) -> str:
        if isinstance(raw, str):
            raw = raw.encode()
        # Six bytes is exactly the legal 48-bit Lighter range and remains
        # deterministic across restarts for the same execution intent.
        value = int.from_bytes(hashlib.sha256(raw).digest()[:6], "big")
        return str(max(min(value, MAX_STABLE_ORDER_REF), 1))

    def _prepare_order_send(self, state: ExecutionState, leg: str,
                            order_ref: str, *, limit_px: float,
                            qty: float) -> None:
        """Persist an order identity before calling either venue."""
        order_plans = self._recovery_context.setdefault("pending_order_qty", {})
        order_plans[f"{leg}:{order_ref}"] = max(float(qty), 0.0)
        if leg == "first":
            state.first_order_ref = str(order_ref)
            state.first_limit_px = limit_px
        else:
            state.hedge_order_ref = str(order_ref)
            state.hedge_limit_px = limit_px
        state.set_pending(leg, order_ref)
        if leg == "hedge":
            self._set_hedge_completion_state(
                state, HEDGE_COMPLETION_PENDING, "hedge_send_intent")
        self._checkpoint_execution(
            state, event_type=f"{leg}_send_intent",
            payload={"leg": leg, "order_ref": str(order_ref),
                     "qty": qty, "limit_px": limit_px})

    def _position_is_balanced(self, tolerance: float) -> bool:
        # Kept as the legacy name used by the execution path.  It means a
        # matched bilateral pair, not "flat"; callers that need flatness must
        # use _positions_flat so a +q/-q pair is never mistaken for no risk.
        return self._pair_positions_balanced(tolerance)

    def _external_flatten_status(self, state: ExecutionState) -> Optional[bool]:
        """Return whether a stale unmatched execution was externally flat.

        ``active_execution`` is intentionally fail-closed: it survives a
        restart until the bot can account for both legs.  A manual close is
        outside that state machine, so the exchange positions can be flat
        while the checkpoint still claims that a hedge is owed.  Only treat
        that as resolved when there is no pending client order, all venues
        are individually flat, no venue is currently marked down, and the
        flat observation has persisted for a short confirmation window.

        ``None`` means this is not an external-flat case; ``False`` means
        confirmation is still in progress; ``True`` means it is safe to
        finalize the unmatched execution without sending another order.
        """
        key = state.cycle_id
        tolerance = self._execution_flat_tolerance(state)
        if (state.remaining_hedge <= tolerance
                or state.pending_leg is not None
                or state.pending_order_ref is not None
                or not self.venues
                or self._venue_down
                or any(abs(v.position) > tolerance
                       for v in self.venues.values())):
            self._external_flatten_seen_at.pop(key, None)
            return None

        now = time.time()
        first_seen = self._external_flatten_seen_at.setdefault(key, now)
        if now - first_seen < self.EXTERNAL_FLAT_CONFIRMATION_SEC:
            return False
        return True

    def _mark_reconciliation_synced_if_flat(self) -> None:
        """Publish the flat result immediately after external recovery."""
        net = sum(v.position for v in self.venues.values())
        self.reconciliation["net_delta_base"] = net
        if not self._venue_down and self._positions_flat():
            self.reconciliation["status"] = "synced"

    def _apply_leg_fill(self, venue, *, is_buy: bool, fill: float,
                        info: dict, fallback_px: float) -> None:
        """Apply one newly observed fill to the local working position."""
        if fill <= 0:
            return
        px = info.get("avg_px") or fallback_px
        try:
            px = float(px)
        except (TypeError, ValueError):
            px = float(fallback_px)
        fee = venue.fee_bps / 1e4
        venue.position += fill if is_buy else -fill
        if is_buy:
            venue.cash -= fill * px * (1 + fee)
        else:
            venue.cash += fill * px * (1 - fee)
        venue.volume_usd += fill * px

    def _observe_execution_result(self, state: ExecutionState, leg: str,
                                  venue, *, is_buy: bool, info: dict,
                                  fallback_px: float,
                                  apply_position: bool = True) -> None:
        """Record only the delta since the last update for one order.

        Exchange confirmations are cumulative for a client order.  Keeping
        ``pending_filled`` prevents a late confirmation from being applied a
        second time to the local position.
        """
        reported_observed = self._fill_amount(info)
        known_ref = (self._order_ref(info)
                     or (state.pending_order_ref
                         if state.pending_leg == leg else None)
                     or (state.first_order_ref if leg == "first"
                         else state.hedge_order_ref))
        durable_previous = 0.0
        order_plan_qty = None
        if known_ref is not None:
            try:
                durable_previous = (self.ledger.order_filled_qty(
                    venue=venue.key, order_ref=known_ref) or 0.0)
                order_plan_qty = self.ledger.order_planned_qty(
                    venue=venue.key, order_ref=known_ref)
            except Exception:
                durable_previous = 0.0
        if order_plan_qty is None:
            order_plan_qty = _safe_float(
                (self._recovery_context.get("pending_order_qty") or {}).get(
                    f"{leg}:{known_ref}"))
        previous = max(
            state.pending_filled if state.pending_leg == leg else 0.0,
            durable_previous)
        fill_cap = max(float(order_plan_qty
                             if order_plan_qty is not None
                             else state.planned_qty or 0.0), 0.0)
        if leg == "hedge":
            fill_cap = min(fill_cap, max(float(state.first_filled or 0.0), 0.0))
        observed = min(max(reported_observed, 0.0), fill_cap)
        previous = min(max(previous, 0.0), fill_cap)
        remaining_total = max(float(state.planned_qty or 0.0) - (
            float(state.first_filled or 0.0) if leg == "first"
            else float(state.hedge_filled or 0.0)), 0.0)
        delta = min(max(observed - previous, 0.0), remaining_total)
        if known_ref is not None and self._order_ref(info) is None:
            info = {**info, "order_ref": known_ref}
        previous_average = (state.first_avg_px if leg == "first"
                            else state.hedge_avg_px)
        state.set_order_result(leg, info, limit_px=fallback_px)
        order_ref = self._order_ref(info)
        try:
            self.ledger.record_order(
                execution_id=state.cycle_id, leg=leg, venue=venue.key,
                order_ref=order_ref,
                side="buy" if is_buy else "sell",
                planned_qty=fill_cap, limit_px=fallback_px,
                reduce_only=state.reduce_only,
                status=str(info.get("status") or "unknown"),
                filled_qty=observed,
                avg_px=(float(info["avg_px"])
                        if info.get("avg_px") is not None else None),
                unresolved=bool(info.get("unresolved")))
        except Exception as exc:
            self.halted = True
            log.critical("[EXEC %s] order ledger write failed: %r",
                         state.cycle_id, exc)
        avg = info.get("avg_px")
        try:
            average = float(avg) if avg is not None else None
        except (TypeError, ValueError):
            average = None
        price_known = (average is not None and math.isfinite(average)
                       and average > 0)
        position_before = venue.position
        cash_before = venue.cash
        volume_before = venue.volume_usd
        if delta:
            # The working position may use the limit as a temporary cash mark,
            # but a missing average is deliberately not written to the cost
            # ledger.  A later resolver/history response repairs the exact
            # cumulative quantity through record_cumulative_fill().
            fill_info = dict(info)
            fill_info["avg_px"] = average if price_known else fallback_px
            if apply_position:
                self._apply_leg_fill(venue, is_buy=is_buy, fill=delta,
                                     info=fill_info, fallback_px=fallback_px)
            if leg == "first":
                state.first_filled += delta
            else:
                state.hedge_filled += delta
            self._clamp_execution_quantities(state)

        cumulative_result = None
        if observed > 0 and price_known:
            fee_value = info.get("fee_usd")
            try:
                fee_value = (float(fee_value) if fee_value is not None else
                             observed * average * venue.fee_bps / 1e4)
            except (TypeError, ValueError):
                fee_value = observed * average * venue.fee_bps / 1e4
            try:
                cumulative_result = self.ledger.record_cumulative_fill(
                    execution_id=state.cycle_id, leg=leg, venue=venue.key,
                    order_ref=order_ref, side="buy" if is_buy else "sell",
                    observed_qty=observed, avg_px=average,
                    fee_usd=fee_value,
                    fee_source=("exchange_api" if info.get("fee_usd") is not None
                                else self._fill_fee_source(venue)),
                    ts=time.time(),
                    position_before=(position_before if apply_position else None),
                    position_after=(venue.position if apply_position else None),
                    source="order_confirmation",
                    metadata={"cumulative_filled": observed,
                              "previous_cumulative_filled": previous,
                              "price_source": "exchange_average"})
            except Exception as exc:
                self.halted = True
                log.critical("[EXEC %s] fill ledger write failed: %r",
                             state.cycle_id, exc)

        elif delta and fallback_px and math.isfinite(float(fallback_px)) \
                and float(fallback_px) > 0:
            # Keep the inventory quantity auditable when the exchange confirms
            # a fill but omits its average.  The limit price is explicitly
            # untrusted and preview_close_cost will reject it until a later
            # order/history response repairs the row.
            try:
                fallback_fee = (delta * float(fallback_px)
                                * venue.fee_bps / 1e4)
                self.ledger.record_fill(
                    fill_key=(f"unpriced:{state.cycle_id}:{leg}:"
                              f"{order_ref or 'no-ref'}:{observed:.12g}"),
                    execution_id=state.cycle_id, leg=leg, venue=venue.key,
                    order_ref=order_ref, ts=time.time(),
                    side="buy" if is_buy else "sell", qty=delta,
                    price=float(fallback_px), fee_usd=fallback_fee,
                    position_before=(position_before if apply_position
                                     else None),
                    position_after=(venue.position if apply_position
                                    else None),
                    source="order_confirmation_unpriced",
                    fee_source=self._fill_fee_source(venue),
                    metadata={"cumulative_filled": observed,
                              "previous_cumulative_filled": previous,
                              "price_source": "limit_fallback"})
            except Exception as exc:
                self.halted = True
                log.critical("[EXEC %s] unpriced fill ledger write failed: %r",
                             state.cycle_id, exc)

        # A cumulative VWAP may imply a different incremental price than the
        # latest response's VWAP.  Correct the local cash/volume mark to the
        # same authoritative delta used by the FIFO ledger.  This also repairs
        # a prior unpriced/fallback confirmation once its average arrives.
        if apply_position and cumulative_result:
            notional_delta = float(
                cumulative_result.get("notional_delta_usd") or 0.0)
            fee_delta = float(cumulative_result.get("fee_delta_usd") or 0.0)
            signed_cash_delta = ((-1.0 if is_buy else 1.0) * notional_delta
                                 + (-1.0 if is_buy else -1.0) * fee_delta)
            provisional_cash_delta = venue.cash - cash_before
            venue.cash += signed_cash_delta - provisional_cash_delta
            authoritative_volume_delta = notional_delta
            provisional_volume_delta = venue.volume_usd - volume_before
            venue.volume_usd += (authoritative_volume_delta
                                 - provisional_volume_delta)

        state.last_error = info.get("err") or state.last_error
        self._mark_hedge_reserve_breach(state)
        self._update_cycle_telemetry(state)
        if leg == "first" and state.first_filled > 0:
            self._record_execution_stage(
                state,
                "first_fill" if str(state.action).lower()
                in {StrategyAction.OPEN.value, StrategyAction.ADD.value}
                else "exit_fill",
                payload={"order_ref": order_ref,
                         "observed_filled": observed})
        if (leg == "hedge" and state.hedge_filled > 0
                and state.remaining_hedge <= self._step / 2):
            self._record_execution_stage(
                state, "hedge_complete",
                payload={"order_ref": order_ref,
                         "remaining_hedge_qty": state.remaining_hedge})
        if info.get("unresolved"):
            if state.pending_leg != leg:
                state.set_pending(leg, self._order_ref(info))
            elif state.pending_order_ref is None:
                state.pending_order_ref = self._order_ref(info)
            state.pending_filled = max(previous, observed)
            state.transition(ExecutionPhase.RECONCILING,
                             f"{leg}_order_unresolved")
            self._checkpoint_execution(
                state, event_type="order_unresolved",
                payload={"leg": leg, "order_ref": order_ref,
                         "observed_filled": observed,
                         "reported_filled": reported_observed})
            self._update_hedge_completion_evidence(
                state, order_info=info, reason=f"{leg}_order_unresolved")
            return

        if state.pending_leg == leg:
            state.clear_pending()
        if leg == "first" and state.first_filled > 0:
            state.transition(ExecutionPhase.HEDGE_LEG, "first_leg_observed")
        else:
            state.touch()
        self._checkpoint_execution(
            state, event_type="fill_observed" if delta else "order_settled",
            payload={"leg": leg, "order_ref": order_ref,
                     "observed_filled": observed, "delta_filled": delta,
                     "reported_filled": reported_observed,
                     "status": info.get("status"),
                     "exchange_reason": info.get("exchange_reason")})
        self._update_hedge_completion_evidence(
            state, order_info=info, reason=f"{leg}_order_settled")

    async def recover_missing_fill(self, state: ExecutionState,
                                   leg: str,
                                   *,
                                   apply_position: bool = True) -> bool:
        """Recover one order from status and authoritative fill history.

        The resolver supplies order status/cumulative quantity.  When the
        final status is delayed or incomplete, the venue's fill-history API is
        queried for the same client reference.  Position reconciliation is
        intentionally not a substitute for either source.
        """
        order_ref = state.pending_order_ref
        if leg not in {"first", "hedge"} or not order_ref:
            state.last_error = "unresolved order has no client reference"
            return False
        venue = self.entropy if leg == "first" else self.hedge
        apply_position_requested = apply_position
        reconciled_venues = self._recovery_context.setdefault(
            "position_reconciled_venues", [])
        if venue.key in reconciled_venues:
            # The reconcile loop already adopted this exchange quantity into
            # the working position.  Apply the recovered cost to the ledger,
            # but do not add the same quantity to the in-memory position.
            apply_position = False
        resolver = getattr(venue, "resolve_order", None)
        if not callable(resolver):
            state.last_error = f"{venue.name} has no order resolver"
            return False
        try:
            info = await resolver(
                order_ref, timeout=min(self.cfg.settle_timeout_sec, 1.0))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.last_error = repr(exc)
            log.warning("[EXEC %s] resolve %s order failed: %r",
                        state.cycle_id, leg, exc)
            return False

        submit_ts = state.created_ts
        for order in self.ledger.execution_orders(state.cycle_id):
            if (str(order.get("venue")) == str(venue.key)
                    and str(order.get("order_ref")) == str(order_ref)):
                submit_ts = float(order.get("created_ts") or submit_ts)
                break
        history = []
        fetcher = getattr(venue, "fetch_fee_fills", None)
        if callable(fetcher):
            try:
                history = await fetcher(
                    max(0.0, submit_ts - 1.0), time.time())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("[EXEC %s] fill history %s query failed: %r",
                            state.cycle_id, leg, exc)

        is_buy = (state.entropy_is_buy if leg == "first"
                  else state.hedge_is_buy)
        average = info.get("avg_px")
        reported_observed = self._fill_amount(info)
        order_cap = self.ledger.order_planned_qty(
            venue=venue.key, order_ref=order_ref)
        if order_cap is None:
            order_cap = max(float(state.planned_qty or 0.0), 0.0)
        if leg == "hedge":
            order_cap = min(order_cap,
                            max(float(state.first_filled or 0.0), 0.0))
        observed = min(max(reported_observed, 0.0), order_cap)
        reported_fee = info.get("fee_usd")
        if reported_fee is None and average is not None:
            try:
                reported_fee = (observed * float(average) * venue.fee_bps / 1e4)
            except (TypeError, ValueError):
                reported_fee = None
        recovered = self.ledger.recover_order_fills(
            execution_id=state.cycle_id, leg=leg, venue=venue.key,
            order_ref=order_ref, side="buy" if is_buy else "sell",
            exchange_rows=history, observed_qty=observed,
            avg_px=average,
            fee_usd=reported_fee,
            fee_source=self._fill_fee_source(venue),
            submit_ts=submit_ts, ts=time.time())
        if recovered.get("known"):
            observed = min(
                max(observed, float(recovered.get("observed_qty") or 0.0)),
                order_cap)
            average = recovered.get("avg_px") or average
            info = {**info, "filled_base": observed, "avg_px": average,
                    "unresolved": False,
                    "status": info.get("status") or "recovered"}

            # ``recover_order_fills`` updates the durable order row before
            # the state observer runs.  Therefore the observer quite
            # correctly sees the recovered quantity as an already-known
            # cumulative value and would otherwise leave the in-memory leg
            # quantity unchanged.  Advance the state from the pending-order
            # cumulative value first, then let the observer settle/clear the
            # order without applying the same position delta twice.
            pending_before = max(
                state.pending_filled if state.pending_leg == leg else 0.0,
                0.0)
            current_leg_filled = (state.first_filled if leg == "first"
                                  else state.hedge_filled)
            leg_remaining = max(float(state.planned_qty or 0.0)
                                - float(current_leg_filled or 0.0), 0.0)
            if leg == "hedge":
                leg_remaining = min(
                    leg_remaining,
                    max(float(state.first_filled or 0.0)
                        - float(state.hedge_filled or 0.0), 0.0))
            recovered_state_delta = min(
                max(observed - pending_before, 0.0), leg_remaining)
            if recovered_state_delta > 0:
                if apply_position:
                    self._apply_leg_fill(
                        venue, is_buy=is_buy, fill=recovered_state_delta,
                        info={**info, "avg_px": average},
                        fallback_px=average or 0.0)
                if leg == "first":
                    state.first_filled += recovered_state_delta
                else:
                    state.hedge_filled += recovered_state_delta
                self._clamp_execution_quantities(state)
            if state.pending_leg == leg:
                state.pending_filled = max(pending_before, observed)
            # The position delta above is now applied (or was already adopted
            # by reconciliation), so the observer must not apply it again.
            apply_position = False

        # The foreground execution can settle the same order while the
        # resolver is awaiting the exchange. Never apply a stale result to a
        # different leg or a pending slot that has already been cleared.
        if (state.pending_leg != leg or state.pending_order_ref != order_ref):
            return state.pending_leg is None
        if leg == "first":
            fallback = state.first_limit_px
            if fallback is None and self._active_trade is not None:
                fallback = (self._active_trade.plan.buy_limit
                            if state.entropy_is_buy
                            else self._active_trade.plan.sell_limit)
            fallback = fallback or self.entropy.book.mid() or 0.0
        else:
            fallback = state.hedge_limit_px
            fallback = (fallback
                        or self._live_taker_bound(
                            self.hedge, state.hedge_is_buy,
                            self._second_leg_slippage_bps(state.action))
                        or self.hedge.book.mid() or 0.0)
        self._observe_execution_result(
            state, leg, venue, is_buy=is_buy, info=info,
            fallback_px=fallback,
            apply_position=apply_position)
        if not info.get("unresolved") and venue.key in reconciled_venues:
            reconciled_venues[:] = [
                item for item in reconciled_venues if item != venue.key]
        if (apply_position_requested is False and not info.get("unresolved")
                and self._recovery_context.get("startup_positions_loaded")):
            # Startup reconciliation already adopted the exchange position.
            # Any retry submitted after this authoritative result is a new
            # runtime delta and must update the working position normally.
            self._recovery_context["startup_positions_loaded"] = False
        source = "exchange_fill_history" if recovered.get("history_count") else (
            "order_status" if recovered.get("known") else "none")
        log.info("[FILL RECOVERY] execution_id=%s order_ref=%s "
                 "expected_qty=%.8g recovered_qty=%.8g avg_px=%s "
                 "fee=%s source=%s", state.cycle_id, order_ref,
                 state.planned_qty, observed, average,
                 recovered.get("fee_usd", info.get("fee_usd")), source)
        return not info.get("unresolved")

    async def _resolve_pending_order(self, state: ExecutionState) -> bool:
        """Resolve one unknown order, without ever submitting a duplicate."""
        pending_leg = state.pending_leg
        if not pending_leg:
            return True
        apply_position = not bool(
            self._recovery_context.get("startup_positions_loaded"))
        return await self.recover_missing_fill(
            state, pending_leg, apply_position=apply_position)

    async def _send_hedge_remainder(self, state: ExecutionState) -> bool:
        """Retry only the still-unhedged quantity on the original hedge leg."""
        self._mark_hedge_reserve_breach(state)
        if state.pending_leg:
            return False
        # A late position read can prove that the original hedge already
        # landed even when its order response was lost.  Never turn that
        # evidence into another remainder order.
        if (self._positions_flat(self._execution_flat_tolerance(state))
                or self._exchange_position_covers_hedge(state)):
            self._update_hedge_completion_evidence(
                state, reason="hedge_remainder_already_covered")
            return False
        qty = floor_step(state.remaining_hedge, self._step)
        if qty < self._taker_minimums(self.hedge, reduce_only=state.reduce_only, qty=qty)[0]:
            return False
        if self.hedge.key in self._venue_down or self._venue_limited(self.hedge):
            return False
        if not self.hedge.book.is_fresh(self.cfg.staleness_sec):
            return False
        limit = self._live_taker_bound(
            self.hedge, state.hedge_is_buy,
            self._execution_hard_cap_bps(state.action, "hedge"))
        if not self._hedge_order_legal(qty, limit):
            return False
        lock = self._vlock(self.hedge.key)
        if lock.locked():
            return False
        await lock.acquire()
        try:
            # The lock wait can make the first quote stale. Reprice both the
            # hard recovery bound and the cost-model snapshot immediately
            # before the retry send.
            limit = self._live_taker_bound(
                self.hedge, state.hedge_is_buy,
                self._execution_hard_cap_bps(state.action, "hedge"))
            if not self._hedge_order_legal(qty, limit):
                return False
            state.retry_count += 1
            state.hedge_limit_px = limit
            hedge_quote = self._quote_snapshot(
                self.hedge, state.hedge_is_buy, qty)
            order_ref = self._deterministic_order_ref(
                state, "hedge", state.retry_count)
            self._prepare_order_send(
                state, "hedge", order_ref, limit_px=limit, qty=qty)
            self._checkpoint_execution(
                state, event_type="hedge_retry_intent",
                payload={"qty": qty, "limit_px": limit,
                         "retry_count": state.retry_count,
                         "order_ref": order_ref})
            log.warning("[EXEC %s] retry hedge %s %.6g on %s @%.6g "
                        "(remaining %.6g, attempt %d)",
                        state.cycle_id,
                        "BUY" if state.hedge_is_buy else "SELL", qty,
                        self.hedge.name, limit, state.remaining_hedge,
                        state.retry_count)
            self._record_send(self.hedge)
            hedge_submit_ts = time.time()
            try:
                info = await self.hedge.send_taker(
                    is_buy=state.hedge_is_buy, qty=qty, limit_px=limit,
                    reduce_only=state.reduce_only, order_ref=order_ref)
            except Exception as exc:
                info = self._failed_send(exc)
            self._record_execution_cost(
                action=state.action, leg="hedge", venue=self.hedge,
                is_buy=state.hedge_is_buy, qty=qty, quote=hedge_quote,
                limit_px=limit, info=info, submit_ts=hedge_submit_ts,
                terminal_ts=time.time())
            self._observe_execution_result(
                state, "hedge", self.hedge, is_buy=state.hedge_is_buy,
                info=info, fallback_px=limit)
            self.hedge.last_traded_ts = time.time()
            if info.get("err"):
                log.error("[EXEC %s] hedge retry failed: %s",
                          state.cycle_id, info["err"])
            return True
        finally:
            lock.release()

    @staticmethod
    def _ceil_step(value: float, step: float) -> float:
        """Round a recovery quantity up without losing one exchange step."""
        if value <= 0:
            return 0.0
        if step <= 0:
            return float(value)
        return round(math.ceil(value / step - 1e-10) * step, 12)

    def _min_order_qty(self, venue, limit_px: Optional[float]) -> float:
        """Minimum legal quantity at a venue for the current execution price."""
        if limit_px is None or limit_px <= 0 or not math.isfinite(limit_px):
            return math.inf
        base, quote = self._taker_minimums(venue)
        return max(base, self._ceil_step(quote / limit_px, self._step))

    def _hedge_order_legal(self, qty: float,
                           limit_px: Optional[float]) -> bool:
        """Check the hedge venue's base and quote minimums for one order."""
        if limit_px is None or qty <= 0:
            return False
        reduce_only = bool(self.active_execution and self.active_execution.reduce_only)
        base, quote = self._taker_minimums(self.hedge, reduce_only=reduce_only, qty=qty)
        return qty + 1e-12 >= base and qty * limit_px + 1e-8 >= quote

    def _first_leg_capacity(self, state: ExecutionState,
                            first_limit: float,
                            hedge_limit: float) -> float:
        """Return extra first-leg quantity that can still be carried safely."""
        if state.reduce_only:
            # A reduce-only top-up may only consume the remaining position on
            # the first venue.  It never turns recovery into an add.
            return max(abs(float(self.entropy.position)), 0.0)

        if state.entropy_is_buy:
            first_headroom = (self.entropy.cap_usd
                              - self.entropy.position * first_limit)
        else:
            first_headroom = (self.entropy.cap_usd
                              + self.entropy.position * first_limit)
        if state.hedge_is_buy:
            hedge_headroom = (self.hedge.cap_usd
                              - self.hedge.position * hedge_limit)
        else:
            hedge_headroom = (self.hedge.cap_usd
                              + self.hedge.position * hedge_limit)
        return max(min(first_headroom / first_limit,
                       hedge_headroom / hedge_limit), 0.0)

    async def _top_up_first_leg_for_hedge(self,
                                          state: ExecutionState) -> bool:
        """Aggregate a too-small first fill until the hedge is exchange-valid.

        An IOC can legally return a fill smaller than the hedge venue's
        minimum order.  Sending that remainder would be rejected; leaving it
        active would block every later cycle.  A bounded-by-capacity top-up on
        the already-filled first leg keeps one execution identity, and the
        normal hedge path then sends the full actual remainder.  This is a
        recovery action, so it uses the emergency price bound and never
        relies on a new premium signal.
        """
        if (state.pending_leg or state.first_topup_count >= 1
                or state.first_filled <= max(self.cfg.net_tolerance_base,
                                             self._step)
                or state.remaining_hedge <= 0):
            return False
        if self.entropy.key in self._venue_down or self.hedge.key in self._venue_down:
            return False
        if self._venue_limited(self.entropy) or self._venue_limited(self.hedge):
            return False
        if (not self.entropy.book.is_fresh(self.cfg.staleness_sec)
                or not self.hedge.book.is_fresh(self.cfg.staleness_sec)):
            return False

        hedge_limit = self._live_taker_bound(
            self.hedge, state.hedge_is_buy,
            self._second_leg_slippage_bps(state.action))
        current_hedge_qty = floor_step(state.remaining_hedge, self._step)
        if self._hedge_order_legal(current_hedge_qty, hedge_limit):
            return False
        if hedge_limit is None or hedge_limit <= 0:
            return False

        first_limit = self._live_taker_bound(
            self.entropy, state.entropy_is_buy,
            max(self.cfg.emergency_slippage_bps,
                self._first_leg_slippage_bps(
                    StrategyAction.REDUCE if state.reduce_only
                    else StrategyAction.OPEN)))
        if first_limit is None or first_limit <= 0:
            return False

        min_hedge_qty = self._min_order_qty(self.hedge, hedge_limit)
        # Add enough first-leg quantity so the *remaining* hedge, not merely
        # the total execution, is a legal order.  This also handles a partial
        # hedge that left a tiny remainder.
        target_first_filled = self._ceil_step(
            state.hedge_filled + min_hedge_qty, self._step)
        needed = max(target_first_filled - state.first_filled, 0.0)
        qty = max(self._min_order_qty(self.entropy, first_limit),
                  self._ceil_step(needed, self._step))
        if state.reduce_only:
            # A REDUCE top-up may only consume the original close slice. It
            # must never turn a partial close into an add or a position flip.
            remaining_planned = max(state.planned_qty - state.first_filled, 0.0)
            qty = min(qty, floor_step(remaining_planned, self._step))
            if qty < self._min_order_qty(self.entropy, first_limit):
                return False
        capacity = self._first_leg_capacity(state, first_limit, hedge_limit)
        if qty > capacity + self._step / 2:
            log.error("[EXEC %s] cannot aggregate partial first fill: "
                      "need %.8g, capacity %.8g", state.cycle_id, qty,
                      capacity)
            return False
        if (self.cfg.max_order_notional > 0
                and qty * first_limit > self.cfg.max_order_notional + 1e-8):
            log.error("[EXEC %s] first-leg recovery top-up %.8g exceeds "
                      "max order notional", state.cycle_id, qty)
            return False

        last = state.first_topup_last_ts
        retry_gap = max(self._no_fill_retry_sec(
            StrategyAction.REDUCE if state.reduce_only
            else StrategyAction.OPEN), 0.5)
        if last is not None and time.time() - last < retry_gap:
            return False
        lock = self._vlock(self.entropy.key)
        if lock.locked():
            return False
        await lock.acquire()
        try:
            if state.pending_leg or state.first_topup_count >= 1:
                return False
            if state.reduce_only:
                remaining_planned = max(
                    state.planned_qty - state.first_filled, 0.0)
                qty = min(qty, floor_step(remaining_planned, self._step))
                if qty < self._min_order_qty(self.entropy, first_limit):
                    return False
            # Re-check the position cap after waiting for the venue lock.
            capacity = self._first_leg_capacity(state, first_limit, hedge_limit)
            if qty > capacity + self._step / 2:
                return False
            first_quote = self._quote_snapshot(
                self.entropy, state.entropy_is_buy, qty)
            state.first_topup_count += 1
            state.first_topup_last_ts = time.time()
            order_ref = self._deterministic_order_ref(
                state, "first_topup", state.first_topup_count)
            self._prepare_order_send(
                state, "first", order_ref, limit_px=first_limit, qty=qty)
            self._checkpoint_execution(
                state, event_type="first_topup_intent",
                payload={"qty": qty, "limit_px": first_limit,
                         "topup_count": state.first_topup_count,
                         "order_ref": order_ref,
                         "required_hedge_qty": min_hedge_qty})
            log.warning("[EXEC %s] aggregate first leg %s %.6g on %s @%.6g "
                        "after partial fill %.6g (need hedgeable remainder "
                        "%.6g, top-up #%d)", state.cycle_id,
                        "BUY" if state.entropy_is_buy else "SELL", qty,
                        self.entropy.name, first_limit, state.first_filled,
                        min_hedge_qty, state.first_topup_count)
            self._record_send(self.entropy)
            first_submit_ts = time.time()
            try:
                info = await self.entropy.send_taker(
                    is_buy=state.entropy_is_buy, qty=qty,
                    limit_px=first_limit, reduce_only=state.reduce_only,
                    order_ref=order_ref)
            except Exception as exc:
                info = self._failed_send(exc)
            self._record_execution_cost(
                action=state.action, leg="first", venue=self.entropy,
                is_buy=state.entropy_is_buy, qty=qty, quote=first_quote,
                limit_px=first_limit, info=info,
                submit_ts=first_submit_ts, terminal_ts=time.time())
            self._observe_execution_result(
                state, "first", self.entropy, is_buy=state.entropy_is_buy,
                info=info, fallback_px=first_limit)
            self.entropy.last_traded_ts = time.time()
            if info.get("err"):
                log.error("[EXEC %s] first-leg top-up failed: %s",
                          state.cycle_id, info["err"])
            return True
        finally:
            lock.release()

    def _exchange_position_covers_hedge(self, state: ExecutionState) -> bool:
        """Return whether account positions already contain the full hedge."""
        tolerance = self.cfg.net_tolerance_base
        if state.first_filled <= tolerance:
            return False
        if state.first_position_before is None \
                or state.hedge_position_before is None:
            return False
        first_sign = 1.0 if state.entropy_is_buy else -1.0
        hedge_sign = 1.0 if state.hedge_is_buy else -1.0
        expected_first = state.first_position_before \
            + first_sign * state.first_filled
        expected_hedge = state.hedge_position_before \
            + hedge_sign * state.first_filled
        return (abs(self.entropy.position - expected_first) <= tolerance
                and abs(self.hedge.position - expected_hedge) <= tolerance)

    async def _recover_execution_cost_history(self,
                                               state: ExecutionState) -> bool:
        """Repair terminal fills whose first response omitted average price."""
        if (self._execution_cost_ready(state)
                and self._execution_fill_coverage_ready(state)):
            return True
        recovered_any = False
        for leg, venue, is_buy, filled, order_ref, average, status in (
                ("first", self.entropy, state.entropy_is_buy,
                 state.first_filled, state.first_order_ref,
                 state.first_avg_px, state.first_order_status),
                ("hedge", self.hedge, state.hedge_is_buy,
                 state.hedge_filled, state.hedge_order_ref,
                 state.hedge_avg_px, state.hedge_order_status)):
            if filled <= 0 or not order_ref:
                continue
            info = {"filled_base": filled, "avg_px": average,
                    "status": status, "fee_usd": None}
            resolver = getattr(venue, "resolve_order", None)
            if callable(resolver):
                try:
                    resolved = await resolver(
                        order_ref, timeout=min(self.cfg.settle_timeout_sec, 1.0))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("[EXEC %s] cost resolve %s failed: %r",
                                state.cycle_id, leg, exc)
                else:
                    info.update(resolved or {})
            submit_ts = state.created_ts
            for order in self.ledger.execution_orders(state.cycle_id):
                if (str(order.get("venue")) == str(venue.key)
                        and str(order.get("order_ref")) == str(order_ref)):
                    submit_ts = float(order.get("created_ts") or submit_ts)
                    break
            history = []
            fetcher = getattr(venue, "fetch_fee_fills", None)
            if callable(fetcher):
                try:
                    history = await fetcher(
                        max(0.0, submit_ts - 1.0), time.time())
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("[EXEC %s] cost history %s failed: %r",
                                state.cycle_id, leg, exc)
            order_cap = self.ledger.order_planned_qty(
                venue=venue.key, order_ref=order_ref)
            if order_cap is None:
                order_cap = max(float(state.planned_qty or 0.0), 0.0)
            if leg == "hedge":
                order_cap = min(order_cap,
                                max(float(state.first_filled or 0.0), 0.0))
            observed = min(
                max(float(filled or 0.0), self._fill_amount(info)), order_cap)
            result = self.ledger.recover_order_fills(
                execution_id=state.cycle_id, leg=leg, venue=venue.key,
                order_ref=order_ref,
                side="buy" if is_buy else "sell",
                exchange_rows=history, observed_qty=observed,
                avg_px=info.get("avg_px"), fee_usd=(
                    info.get("fee_usd") if info.get("fee_usd") is not None
                    else ((observed * float(info["avg_px"]) * venue.fee_bps / 1e4)
                          if info.get("avg_px") is not None else None)),
                fee_source=self._fill_fee_source(venue),
                submit_ts=submit_ts, ts=time.time())
            if not result.get("known"):
                continue
            average = result.get("avg_px") or info.get("avg_px")
            recovered_qty = min(
                float(result.get("observed_qty") or 0.0), order_cap)
            if leg == "first":
                state.first_filled = min(
                    max(state.first_filled, recovered_qty), state.planned_qty)
                state.first_avg_px = float(average)
                state.first_order_status = str(
                    info.get("status") or "recovered")
            else:
                state.hedge_filled = min(
                    max(state.hedge_filled, recovered_qty),
                    state.first_filled)
                state.hedge_avg_px = float(average)
                state.hedge_order_status = str(
                    info.get("status") or "recovered")
            recovered_any = True
            log.info("[FILL RECOVERY] execution_id=%s order_ref=%s "
                     "expected_qty=%.8g recovered_qty=%.8g avg_px=%s "
                     "fee=%s source=%s", state.cycle_id, order_ref,
                     filled, result.get("observed_qty"), average,
                     result.get("fee_usd"), result.get("source", "order_status"))
        if recovered_any:
            self._checkpoint_execution(
                state, event_type="cost_history_recovered",
                payload={"cost_ready": self._execution_cost_ready(state)})
        return self._execution_cost_ready(state)

    def _recovery_orders_terminal(self, state: ExecutionState) -> bool:
        """Position coverage alone cannot authorize a competing exit order."""
        if state.pending_leg or state.pending_order_ref:
            return False
        orders = self.ledger.execution_orders(state.cycle_id)
        if not state.first_order_ref or not state.hedge_order_ref:
            return False
        refs = {(self.entropy.key, str(state.first_order_ref)),
                (self.hedge.key, str(state.hedge_order_ref))}
        terminal = {"filled", "canceled", "cancelled", "rejected", "expired",
                    "settled", "recovered"}
        return refs.issubset({
            (str(order.get("venue")), str(order.get("order_ref")))
            for order in orders
            if not order.get("unresolved")
            and str(order.get("status") or "").lower() in terminal
        }) and all(not order.get("unresolved")
                   and str(order.get("status") or "").lower() in terminal
                   for order in orders)

    async def _release_covered_recovery(self, state: ExecutionState) -> bool:
        """Exit risk while retaining the original execution for late costing."""
        reason = self._recovery_context["covered_recovery_exit"]
        self._set_recovery_status(state, RecoveryStatus.EMERGENCY_EXIT, reason)
        state.transition(ExecutionPhase.FLATTENING, reason)
        self._checkpoint_execution(state, event_type="covered_recovery_exit")
        await self._emergency_exit_positions(reason)
        if (self._positions_flat() and not self._emergency_pending
                and self._execution_cost_ready(state)
                and self._execution_fill_coverage_ready(state)):
            self._clear_orphaned_if_flat()
            self._finish_execution("flattened")
            self._mark_reconciliation_synced_if_flat()
            return True
        # Keep references and context durable. Subsequent recovery only repairs
        # accounting or continues reduce-only; it must never re-hedge a closed leg.
        self._defer_recovery(state, "risk_exit_waiting_fill_facts")
        self._reconcile_evt.set()
        return False

    async def _recover_active_execution(self, *, immediate: bool) -> bool:
        """Advance the active cycle without creating a second strategy cycle.

        A short burst handles common IOC cancellations immediately.  If the
        exchange outcome is still unknown, the state remains RECONCILING and
        the normal reconcile loop retries it later.  This is deliberately a
        local exposure guard, not a global strategy circuit breaker.
        """
        async with self._recovery_lock:
            state = self.active_execution
            if state is None:
                return True
            if self._execution_inflight:
                # The foreground _execute call still owns its pending order.
                return False
            next_retry = _safe_float(state.recovery_next_retry_ts)
            now = time.time()
            if next_retry is not None and next_retry > now:
                # The foreground caller may immediately re-check an
                # unresolved *original* order after receiving a new resolver
                # result. This does not submit a duplicate; all retry sends
                # (retry_count > 0) remain behind the durable backoff.
                foreground_resolve = (immediate and bool(state.pending_leg)
                                      and state.retry_count == 0)
                hard_risk = (not self.record_only
                             and self._margin_snapshot()["hard_risk"]
                             and self._recovery_orders_terminal(state))
                if not foreground_resolve and not hard_risk:
                    self._schedule_poke(next_retry - now)
                    return False
                state.recovery_next_retry_ts = None
            state.recovery_attempt = max(int(state.recovery_attempt or 0), 0) + 1
            state.recovery_next_retry_ts = None
            state.recovery_backoff_sec = self._recovery_backoff_sec(
                state.recovery_attempt)
            state.touch()
            self._checkpoint_execution(
                state, event_type="recovery_attempt",
                payload={"attempt": state.recovery_attempt})
            self._set_recovery_status(
                state, RecoveryStatus.RECOVERING_FILL, "execution_recovery")
            attempts = RECOVERY_BURST_ATTEMPTS if immediate else 1
            for attempt in range(attempts):
                if self._recovery_context.get("covered_recovery_exit") and not self._positions_flat():
                    return await self._release_covered_recovery(state)
                terminal_covered = (self._recovery_orders_terminal(state)
                                    and self._exchange_position_covers_hedge(state)
                                    and not self._positions_flat())
                risk_due = (self._margin_snapshot()["hard_risk"]
                            or max(self._recovery_age(state), self._recovery_age())
                            >= self.cfg.recovery_timeout_sec)
                if terminal_covered and risk_due and (
                        not self._execution_cost_ready(state)
                        or not self._execution_fill_coverage_ready(state)):
                    self._recovery_context["covered_recovery_exit"] = (
                        "margin_risk" if self._margin_snapshot()["hard_risk"]
                        else "emergency_recovery_exit")
                    return await self._release_covered_recovery(state)
                if (not state.pending_leg
                        and (self._margin_snapshot()["hard_risk"]
                             or not self._execution_cost_ready(state)
                             or not self._execution_fill_coverage_ready(state))):
                    self._sync_execution_quantities_from_evidence(state)
                    await self._recover_execution_cost_history(state)
                if self._recovery_context.get("covered_recovery_exit"):
                    return await self._release_covered_recovery(state)
                position_hedged = False
                if state.pending_leg:
                    if not await self._resolve_pending_order(state):
                        # A position read can prove that the pending hedge
                        # already happened, but it cannot provide its price or
                        # fee. Never resend that quantity; wait for fill
                        # history and keep the execution in recovery until its
                        # cost and quantity coverage are authoritative.
                        position_hedged = self._exchange_position_covers_hedge(
                            state)
                        self._update_hedge_completion_evidence(
                            state, reason=(
                                "pending_hedge_position_covered"
                                if position_hedged
                                else "pending_order_unresolved"))
                        if not position_hedged:
                            state.phase = ExecutionPhase.RECONCILING
                            self._reconcile_evt.set()
                            self._defer_recovery(state, "pending_order_unresolved")
                            return False
                        log.warning(
                            "[EXEC %s] exchange positions cover hedge; "
                            "suppressing duplicate retry until fill history",
                            state.cycle_id)
                if not position_hedged:
                    position_hedged = self._exchange_position_covers_hedge(state)
                self._update_hedge_completion_evidence(
                    state, reason=("recovery_position_covered"
                                   if position_hedged
                                   else "recovery_evidence_checked"))
                if state.pending_leg and not position_hedged:
                    self._defer_recovery(state, "pending_order_unresolved")
                    return False
                if state.first_filled <= 0 and state.hedge_filled <= 0:
                    self._finish_execution("no_fill")
                    return True

                external_flat = self._external_flatten_status(state)
                if external_flat is not None:
                    if not external_flat:
                        state.transition(ExecutionPhase.RECONCILING,
                                         "external_flat_pending_confirmation")
                        self._reconcile_evt.set()
                        self._skiplog(
                            "[EXEC %s] both venues are flat; waiting %.1fs "
                            "to confirm external close",
                            state.cycle_id,
                            self.EXTERNAL_FLAT_CONFIRMATION_SEC)
                        self._defer_recovery(
                            state, "external_flat_pending_confirmation")
                        return False
                    self._external_flatten_seen_at.pop(state.cycle_id, None)
                    state.transition(ExecutionPhase.RECONCILING,
                                     "external_flatten_confirmed")
                    self._checkpoint_execution(
                        state, event_type="external_flatten_confirmed",
                        payload={"reason": "both_venues_flat",
                                 "remaining_hedge": state.remaining_hedge})
                    log.warning(
                        "[EXEC %s] external/manual flat confirmed; "
                        "finalizing stale unmatched execution without hedge",
                        state.cycle_id)
                    self._finish_execution("one_leg")
                    self._mark_reconciliation_synced_if_flat()
                    return True

                state.transition(ExecutionPhase.HEDGE_LEG,
                                 "continue_hedge_recovery")
                # ``net_tolerance_base`` is a reporting tolerance, not a
                # reason to skip a hedgeable exchange fill.  A tiny but
                # exchange-valid first fill still needs its opposite leg.
                if state.remaining_hedge > 1e-12 and not position_hedged:
                    topped_up = await self._top_up_first_leg_for_hedge(state)
                    if state.pending_leg:
                        self._reconcile_evt.set()
                        self._defer_recovery(state, "topup_order_unresolved")
                        return False
                    if topped_up:
                        if attempt + 1 < attempts:
                            await asyncio.sleep(RECOVERY_RETRY_DELAY_SEC)
                        continue
                    sent = await self._send_hedge_remainder(state)
                    if state.pending_leg:
                        self._reconcile_evt.set()
                        self._defer_recovery(state, "hedge_order_unresolved")
                        return False
                    if sent and state.remaining_hedge > 1e-12:
                        if attempt + 1 < attempts:
                            await asyncio.sleep(RECOVERY_RETRY_DELAY_SEC)
                        continue

                if ((state.remaining_hedge <= self.cfg.net_tolerance_base
                     or position_hedged)
                        and self._pair_positions_balanced(
                            self.cfg.net_tolerance_base)):
                    cost_ready = self._execution_cost_ready(state)
                    coverage_ready = self._execution_fill_coverage_ready(state)
                    if (cost_ready and coverage_ready
                            and not state.pending_leg
                            and not state.pending_order_ref):
                        self._sync_execution_quantities_from_evidence(state)
                        state.last_error = None
                        self._set_recovery_status(
                            state, RecoveryStatus.NORMAL,
                            "authoritative_fills_recovered")
                        outcome = ("paired" if state.hedge_filled > 0
                                   and abs(state.first_filled - state.hedge_filled)
                                   < self._step / 2 else "one_leg")
                        log.info("[CYCLE EXIT] mode=recovered_pair held_sec=%.3f "
                                 "estimated_net_pnl=%s", self._recovery_age(state),
                                 self.ledger.execution_fill_edge(state.cycle_id))
                        self._finish_execution(outcome)
                        return True
                    state.transition(
                        ExecutionPhase.RECONCILING,
                        "awaiting_authoritative_fill_coverage")
                    self._set_recovery_status(
                        state, RecoveryStatus.RECOVERING_FILL,
                        "awaiting_authoritative_fill_coverage")
                    self._reconcile_evt.set()

                # Missing cost or quantity coverage may block completion, but
                # never the risk exit. Flatten both venues rather than
                # reducing only net exposure and leaving an unaccounted pair.
                recovery_age = max(self._recovery_age(state),
                                   self._recovery_age())
                # A position read is a safety proof, not a fill-cost proof.
                # Once it proves the target is covered (or both venues are
                # flat), continue background accounting recovery and never
                # create an emergency duplicate order.
                position_covered_now = (
                    position_hedged
                    or self._exchange_position_covers_hedge(state))
                flat_now = self._positions_flat(
                    self._execution_flat_tolerance(state))
                if recovery_age >= self.cfg.recovery_timeout_sec \
                        and not position_covered_now and not flat_now:
                    reason = ("unknown_entry_cost_timeout"
                              if not self._execution_cost_ready(state)
                              else ("hedge_fill_coverage_timeout"
                                    if not self._execution_fill_coverage_ready(
                                        state)
                                    else "hedge_remainder_unsettled"))
                    self._mark_orphaned(reason, state)
                    self._set_recovery_status(
                        state, RecoveryStatus.EMERGENCY_EXIT, reason)
                    await self._emergency_exit_positions(
                        "emergency_recovery_exit")
                    if self._positions_flat():
                        self._clear_orphaned_if_flat()
                        self._finish_execution("flattened")
                        self._mark_reconciliation_synced_if_flat()
                        return True
                elif recovery_age >= self.cfg.recovery_timeout_sec:
                    state.phase = ExecutionPhase.RECONCILING
                    self._reconcile_evt.set()
                    self._defer_recovery(
                        state, "position_covered_waiting_fill_facts"
                        if position_covered_now else "flat_waiting_order_resolution")
                    return False
                if attempt + 1 < attempts:
                    await asyncio.sleep(RECOVERY_RETRY_DELAY_SEC)

            state.phase = ExecutionPhase.RECONCILING
            self._reconcile_evt.set()
            log.warning("[EXEC %s] remains unresolved: phase=%s first=%.6g "
                        "hedge=%.6g remaining=%.6g recovery=%s error=%s",
                        state.cycle_id, state.phase.value, state.first_filled,
                        state.hedge_filled, state.remaining_hedge,
                        state.recovery_status.value, state.last_error or "none")
            self._defer_recovery(state, state.recovery_reason)
            return False

    def _refresh_execution_plan(self, trade: PlannedTrade) -> Optional[PlannedTrade]:
        """Reprice the exact planned quantity immediately before a send.

        ``_scan`` is intentionally allowed to be slightly stale because it
        only selects an opportunity.  The send path is not: both latest
        cached books are walked again under the venue locks and the same
        quantity is rejected if either book no longer supports it.
        """
        candidate, plan = trade.candidate, trade.plan
        max_age_sec = self._hard_market_data_age_sec()
        now = time.time()
        self._record_hedge_quote(now)
        if not all(
                book.is_fresh(max_age_sec)
                and 0 <= now - book.last_update_ts <= max_age_sec
                for book in (candidate.buy.book, candidate.sell.book)):
            return None
        cross_skew_ms = abs(candidate.buy.book.last_update_ts
                            - candidate.sell.book.last_update_ts) * 1000
        forced_reduce = (candidate.action == StrategyAction.REDUCE
                         and bool(trade.cycle_budget.get("risk_exit")))
        if (cross_skew_ms > self._hard_cross_skew_ms()
                and not forced_reduce):
            return None
        qty = float(plan.qty)
        self._record_open_quote(now)
        buy_quote = self._depth_quote(candidate.buy, True, qty)
        sell_quote = self._depth_quote(candidate.sell, False, qty)
        if buy_quote is None or sell_quote is None:
            return None
        buy_px, buy_notional = buy_quote
        sell_px, sell_notional = sell_quote
        best_ask = candidate.buy.book.best_ask()
        best_bid = candidate.sell.book.best_bid()
        if best_ask is None or best_bid is None:
            return None
        refreshed_plan = replace(
            plan, buy_limit=buy_px, sell_limit=sell_px,
            buy_notional=buy_notional, sell_notional=sell_notional,
            top_premium_bps=(best_bid / best_ask - 1.0) * 1e4,
            marginal_premium_bps=(sell_px / buy_px - 1.0) * 1e4)
        if not candidate.reduce_only:
            headroom = self._headroom(candidate.buy, candidate.sell, buy_px)
            if headroom < buy_notional - 1e-8:
                return None
        refreshed = self._execution_bounds(candidate, refreshed_plan)
        if (refreshed is not None
                and candidate.action in {StrategyAction.OPEN,
                                         StrategyAction.ADD}):
            # The opportunity identity belongs to the signal, not to one
            # re-priced plan.  Carry it across campaign attempts so the
            # durable telemetry remains one opportunity.
            for key in ("open_opportunity_id", "open_opportunity_key"):
                if trade.cycle_budget.get(key) is not None:
                    refreshed.cycle_budget[key] = trade.cycle_budget[key]
            opportunity_key = refreshed.cycle_budget.get(
                "open_opportunity_key")
            if opportunity_key:
                opportunity = self._open_opportunities.get(opportunity_key)
                if opportunity is not None:
                    opportunity.update(
                        last_seen_ts=now,
                        quoted_edge_bps=refreshed.cycle_budget.get(
                            "quoted_edge_bps"),
                        protected_open_edge_bps=refreshed.cycle_budget.get(
                            "protected_open_edge_bps"),
                        execution_room_bps=refreshed.cycle_budget.get(
                            "execution_room_bps"),
                        economic_required_dislocation_bps=refreshed.cycle_budget.get(
                            "economic_required_dislocation_bps"),
                        effective_entry_distance_bps=refreshed.cycle_budget.get(
                            "effective_entry_distance_bps"),
                        latency_required_room_bps=refreshed.cycle_budget.get(
                            "latency_required_room_bps"),
                        first_leg_execution_room_bps=refreshed.cycle_budget.get(
                            "first_leg_execution_room_bps"),
                        actual_entropy_limit_px=refreshed.cycle_budget.get(
                            "actual_entropy_limit_px"))
                    self._last_open_opportunity = dict(opportunity)
        return refreshed

    @staticmethod
    def _is_economic_exit(trade: PlannedTrade) -> bool:
        budget = trade.cycle_budget or {}
        return (trade.candidate.action == StrategyAction.REDUCE
                and (budget.get("exit_class") == "economic"
                     or budget.get("exit_reason") in {
                         "normal_profit", "fast_exit", "sampling_exit",
                         "stale_exit"}))

    def _start_exit_campaign(self, state: ExecutionState,
                             trade: PlannedTrade) -> Optional[dict]:
        """Start one bounded economic close opportunity."""
        cfg = getattr(self.cfg, "reduce_execution", None)
        budget = trade.cycle_budget or {}
        # Every cycle uses the same close campaign.  Bootstrap only changes
        # whether a bounded negative-EV OPEN may be sampled; it never grants
        # a faster retry loop or a different exit budget.
        duration = float(getattr(cfg, "exit_campaign_sec", 0.0) or 0.0)
        interval = float(getattr(cfg, "retry_interval_sec", 0.15) or 0.15)
        max_attempts = int(getattr(cfg, "exit_campaign_max_attempts", 4)
                           or 4)
        if duration <= 0 or interval <= 0 or not self._is_economic_exit(trade):
            return None
        self._exit_opportunity_seq += 1
        opportunity_id = f"{state.cycle_id}:exit-{self._exit_opportunity_seq}"
        campaign_id = f"{opportunity_id}:campaign"
        campaign = {
            "exit_opportunity_id": opportunity_id,
            "exit_campaign_id": campaign_id,
            "exit_class": trade.cycle_budget.get("exit_reason") or
            trade.cycle_budget.get("exit_class") or "economic",
            "started_ts": time.time(),
            "started_monotonic": time.monotonic(),
            "deadline_monotonic": time.monotonic() + duration,
            "duration_sec": duration,
            "retry_interval_sec": interval,
            "max_attempts": max_attempts,
            "duration_ms": None,
            "retry_count": 0,
            "first_fill_qty": 0.0,
            "final_result": None,
            "cycle_id": state.cycle_id,
            "first_attempt_limit_px": state.first_limit_px,
            "quoted_cycle_pnl": trade.cycle_budget.get(
                "quoted_net_profit_usd"),
            "protected_cycle_pnl": trade.cycle_budget.get(
                "net_profit_usd"),
            "dynamic_hedge_reserve_bps": trade.cycle_budget.get(
                "dynamic_hedge_reserve_bps"),
            "first_leg_room_bps": trade.cycle_budget.get(
                "first_leg_execution_room_bps", trade.per_leg_slippage_bps),
            "close_execution_room_bps": trade.cycle_budget.get(
                "close_execution_room_bps"),
            "close_execution_room_min_bps": trade.cycle_budget.get(
                "close_execution_room_min_bps"),
            "close_execution_room_target_bps": trade.cycle_budget.get(
                "close_execution_room_target_bps"),
        }
        self._exit_campaign = campaign
        self._exit_campaign_telemetry["exit_opportunity_count"] += 1
        self._exit_campaign_telemetry["exit_campaign_count"] += 1
        self._recovery_context["exit_opportunity_id"] = opportunity_id
        self._recovery_context["exit_class"] = campaign["exit_class"]
        self._recovery_context["exit_campaign"] = dict(campaign)
        self._record_opportunity_event(
            opportunity_id=opportunity_id, campaign_id=campaign_id,
            cycle_id=state.cycle_id, direction=state.direction,
            stage="exit_qualified", reason=campaign["exit_class"],
            payload={
                "exit_reason": trade.cycle_budget.get("exit_reason"),
                "quoted_cycle_pnl_usd": _safe_float(
                    campaign.get("quoted_cycle_pnl")),
                "protected_cycle_pnl_usd": _safe_float(
                    campaign.get("protected_cycle_pnl")),
            })
        self._record_opportunity_event(
            opportunity_id=opportunity_id, campaign_id=campaign_id,
            cycle_id=state.cycle_id, direction=state.direction,
            stage="exit_campaign_started", payload={
                "duration_sec": duration,
                "retry_interval_sec": interval,
                "max_attempts": max_attempts,
            })
        return campaign

    def _exit_campaign_stop_reason(self) -> str:
        """Classify why a profitable exit campaign stopped before a fill."""
        max_age_sec = self._hard_market_data_age_sec()
        now = time.time()
        if not all(
                v.book.is_fresh(max_age_sec)
                and 0 <= now - v.book.last_update_ts <= max_age_sec
                for v in (self.entropy, self.hedge)):
            return "stale_book"
        cross_skew_ms = abs(self.entropy.book.last_update_ts
                            - self.hedge.book.last_update_ts) * 1000.0
        if cross_skew_ms > self._hard_cross_skew_ms():
            return "stale_book"
        budget = self._last_cycle_budget or {}
        if budget and not budget.get("allowed", True):
            return "pnl_deteriorated"
        return "zero_fill"

    def _finish_exit_campaign(self, state: ExecutionState,
                              final_result: str) -> None:
        campaign = self._exit_campaign
        if campaign is None:
            return
        now = time.time()
        duration_ms = max(
            (time.monotonic() - campaign["started_monotonic"]) * 1000.0, 0.0)
        campaign = dict(campaign)
        campaign["duration_ms"] = duration_ms
        campaign["final_result"] = final_result
        campaign["first_fill_qty"] = float(state.first_filled)
        campaign["retry_count"] = int(campaign.get("retry_count") or 0)
        self._last_exit_campaign = campaign
        self._exit_campaign = None
        telemetry = self._exit_campaign_telemetry
        first_attempt_filled = campaign["first_fill_qty"] > 0 and (
            campaign["retry_count"] == 0)
        # A partial fill on the first IOC still counts as a first-attempt
        # success; the lifecycle then moves to hedge/recovery as usual.
        if campaign["first_fill_qty"] > 0:
            if first_attempt_filled:
                telemetry["first_attempt_fill_count"] += 1
            telemetry["campaign_fill_count"] += 1
            telemetry["opportunity_to_fill_ms"].append(duration_ms)
        elif final_result in {"pnl_deteriorated", "stale_book", "zero_fill"}:
            telemetry["missed_profitable_exit_count"] += 1
        telemetry["retry_count_total"] += campaign["retry_count"]
        opportunity_count = telemetry["exit_opportunity_count"]
        campaign_count = telemetry["exit_campaign_count"]
        telemetry["first_attempt_fill_rate"] = (
            telemetry["first_attempt_fill_count"] / opportunity_count
            if opportunity_count else 0.0)
        telemetry["campaign_fill_rate"] = (
            telemetry["campaign_fill_count"] / campaign_count
            if campaign_count else 0.0)
        # Keep the in-memory metric bounded even if a process runs for weeks.
        telemetry["opportunity_to_fill_ms"] = (
            telemetry["opportunity_to_fill_ms"][-256:])
        self._recovery_context["exit_campaign"] = dict(campaign)
        cycle_id = self.cycle.cycle_id
        if cycle_id:
            record = (self._cycle_telemetry
                      if self._cycle_telemetry.get("cycle_id") == cycle_id
                      else self.ledger.load_cycle(cycle_id) or {})
            if record:
                record.update({
                    "exit_opportunity_id": campaign["exit_opportunity_id"],
                    "exit_campaign_id": campaign.get("exit_campaign_id"),
                    "exit_class": campaign["exit_class"],
                    "quoted_cycle_pnl": campaign["quoted_cycle_pnl"],
                    "protected_cycle_pnl": campaign["protected_cycle_pnl"],
                    "dynamic_hedge_reserve_bps": campaign[
                        "dynamic_hedge_reserve_bps"],
                    "first_leg_room_bps": campaign["first_leg_room_bps"],
                    "retry_count": campaign["retry_count"],
                    "campaign_duration_ms": duration_ms,
                    "first_fill_qty": campaign["first_fill_qty"],
                    "final_result": final_result,
                    "updated_ts": now,
                })
                self._cycle_telemetry = record
                try:
                    self.ledger.upsert_cycle(record)
                except Exception:
                    log.exception("[CYCLE %s] exit campaign telemetry write failed",
                                  cycle_id)

    async def _execute(self, trade: PlannedTrade) -> bool:
        """Fill Entropy first, then hedge its actual fill on the other venue.

        Both venue locks are held by the caller. An Entropy zero-fill never
        sends the Lighter leg, eliminating the dominant observed loss path.
        Returns True while the execution state still needs reconciliation.
        """
        if self.halted or self.active_execution is not None:
            return False
        execution_started = time.perf_counter()
        # Reprice the exact quantity before every first-leg send, regardless
        # of whether a profit guard is enabled.  The scan is a signal only;
        # this is the final executable decision under both venue locks.
        refreshed_trade = self._refresh_execution_plan(trade)
        if refreshed_trade is None:
            return False
        trade = refreshed_trade
        cfg = self.cfg
        candidate, plan = trade.candidate, trade.plan
        buy, sell = candidate.buy, candidate.sell
        state = self._begin_execution(trade)
        campaign = self._start_exit_campaign(state, trade)
        open_campaign = self._start_open_campaign(state, trade)
        if (open_campaign is None
                and candidate.action in {StrategyAction.OPEN,
                                         StrategyAction.ADD}):
            # A direct one-shot execution is still a campaign for durable
            # accounting. Production config enables the bounded retry
            # campaign; this fallback keeps tests and legacy configs honest.
            opportunity_id = trade.cycle_budget.get("open_opportunity_id")
            if opportunity_id:
                synthetic_id = (
                    f"{opportunity_id}:campaign:{state.cycle_id}")
                opportunity_key = trade.cycle_budget.get(
                    "open_opportunity_key")
                opportunity = (self._open_opportunities.get(opportunity_key)
                                if opportunity_key else None)
                campaign_count = int(
                    (opportunity or {}).get("campaign_count") or 0) + 1
                if opportunity is not None:
                    opportunity.update(
                        campaign_count=campaign_count,
                        last_campaign_distance_bps=_safe_float(
                            opportunity.get("signal_distance_bps")),
                        last_campaign_ts=time.time(),
                        state="campaign")
                    self._last_open_opportunity = dict(opportunity)
                synthetic_campaign = {
                    "open_opportunity_id": opportunity_id,
                    "open_opportunity_key": trade.cycle_budget.get(
                        "open_opportunity_key"),
                    "open_campaign_id": synthetic_id,
                    "direction": candidate.direction,
                    "started_ts": state.created_ts,
                    "started_monotonic": time.monotonic(),
                    "deadline_monotonic": time.monotonic(),
                    "duration_ms": None,
                    "campaign_count": campaign_count,
                    "send_count": 0,
                    "retry_count": 0,
                    "first_fill_qty": 0.0,
                    "final_result": None,
                    "attempts": [],
                    "retry_enabled": False,
                }
                self._open_campaign = synthetic_campaign
                open_campaign = synthetic_campaign
                self._recovery_context["open_campaign"] = dict(
                    synthetic_campaign)
                self._record_opportunity_event(
                    opportunity_id=opportunity_id, campaign_id=synthetic_id,
                    cycle_id=state.cycle_id, direction=candidate.direction,
                    stage="campaign_started", payload={
                        "campaign_count": campaign_count,
                        "requested_qty": _safe_float(trade.plan.qty),
                        "max_attempts": 1,
                    })
        latency = {}
        self._recovery_context["latency_ms"] = latency
        inv_bps = (0.0 if candidate.action == StrategyAction.REDUCE else
                   self._inv_add_bps(buy, sell))
        direction = candidate.direction
        self.last_trade_ts = time.time()
        log.info("[ARB] %s: BUY %s %.6g @<=%.6g | SELL %s @>=%.6g | "
                 "take $%.0f of $%.0f | prem %.2fbps | exp $%.4f",
                 direction, buy.name, plan.qty, plan.buy_limit, sell.name,
                 plan.sell_limit, plan.buy_notional, plan.q_max_notional,
                 plan.marginal_premium_bps, plan.exp_edge_usd)
        log.info("[BUDGET] action=%s net=%.3fbps threshold=%.3f surplus=%.3f "
                 "first_slip=%.3fbps", candidate.action.value,
                 trade.net_marginal_edge_bps, candidate.threshold_bps,
                 trade.edge_surplus_bps, trade.per_leg_slippage_bps)
        budget = trade.cycle_budget or {}
        entropy_is_buy = buy is self.entropy
        entropy_limit = trade.buy_bound if entropy_is_buy else trade.sell_bound
        if candidate.action == StrategyAction.REDUCE:
            log.info(
                "[CLOSE LIMIT] quoted_cycle_pnl=$%s protected_cycle_pnl=$%s "
                "allowed_floor=$%s economic_budget=$%s "
                "economic_room=%.4fbps actual_room=%.4fbps "
                "attempt_no=%d hard_cap=%.4fbps first=%s hedge=%s "
                "limit_net=$%s",
                budget.get("quoted_net_profit_usd"),
                budget.get("net_profit_usd"),
                budget.get("required_exit_floor_usd",
                           budget.get("allowed_floor_usd")),
                budget.get("exit_economic_budget_usd"),
                float(budget.get("exit_budget_bps") or 0.0),
                float(budget.get("close_execution_room_bps") or 0.0),
                1, float(budget.get("physical_hard_cap_bps") or 0.0),
                entropy_limit, budget.get("hedge_reserved_limit_px"),
                budget.get("limit_net_profit_usd"))
        elif candidate.action in {StrategyAction.OPEN, StrategyAction.ADD}:
            log.info(
                "[OPEN LIMIT] projected_cycle_pnl=$%s required_floor=$%s "
                "available_budget=$%s economic_room=%.4fbps "
                "latency_required_room=%.4fbps hard_cap=%.4fbps "
                "actual_room=%.4fbps first=%s",
                budget.get("expected_cycle_pnl_usd",
                           budget.get("net_profit_usd")),
                budget.get("economic_floor_usd"),
                budget.get("available_budget_usd",
                           budget.get("economic_surplus_usd")),
                float(budget.get("economic_room_bps") or 0.0),
                float(budget.get("latency_required_room_bps") or 0.0),
                float(budget.get("physical_hard_cap_bps") or 0.0),
                float(budget.get("first_leg_execution_room_bps") or
                      budget.get("execution_room_bps") or 0.0),
                entropy_limit)
        reduce_campaign = candidate.action == StrategyAction.REDUCE
        open_retry_campaign = (open_campaign is not None
                               and bool(open_campaign.get(
                                   "retry_enabled", True)))
        campaign_enabled = campaign is not None
        retry_campaign = campaign or open_campaign
        campaign_interval = (float(retry_campaign.get("retry_interval_sec"))
                             if retry_campaign is not None
                             and retry_campaign.get("retry_interval_sec") is not None
                             else (float(cfg.reduce_execution.exit_campaign_retry_interval_sec)
                                   if campaign_enabled else
                                   float(cfg.reduce_execution.open_retry_interval_sec)
                                   if open_retry_campaign else 0.0))
        campaign_deadline = (float(retry_campaign["deadline_monotonic"])
                             if retry_campaign is not None else None)
        max_first_attempts = (
            cfg.max_reduce_retry_attempts if reduce_campaign else
            int(open_campaign.get("max_attempts") or
                cfg.reduce_execution.open_retry_max_attempts)
            if open_retry_campaign else 1)
        if campaign_enabled:
            campaign_duration = float(retry_campaign.get(
                "duration_sec", cfg.reduce_execution.exit_campaign_sec) or 0.0)
            configured_max_attempts = int(retry_campaign.get(
                "max_attempts", getattr(cfg.reduce_execution,
                                         "exit_campaign_max_attempts", 4))
                or 1)
            campaign_attempts = max(
                1, math.ceil(campaign_duration
                              / campaign_interval) + 1)
            max_first_attempts = min(
                max_first_attempts, campaign_attempts,
                configured_max_attempts)
        einfo = {"status": "not-sent", "filled_base": 0.0,
                 "avg_px": None, "err": None, "unresolved": False}
        first_returned = time.perf_counter()
        first_attempts = 0
        initial_hedge_quote = None
        campaign_stop_reason: Optional[str] = None
        for attempt in range(max_first_attempts):
            if attempt > 0:
                if (retry_campaign is not None and campaign_deadline is not None
                        and time.monotonic() >= campaign_deadline):
                    campaign_stop_reason = "zero_fill"
                    break
                if open_retry_campaign:
                    # A campaign is deliberately short and evenly paced. An
                    # exponential backoff would spend most of its window
                    # waiting after a no-fill and reduce completion rate.
                    delay = campaign_interval
                else:
                    delay = (campaign_interval if campaign_enabled
                             else self._no_fill_retry_sec(candidate.action))
                if delay > 0:
                    if (retry_campaign is not None
                            and campaign_deadline is not None):
                        delay = min(delay, max(
                            campaign_deadline - time.monotonic(), 0.0))
                    if delay > 0:
                        await asyncio.sleep(delay)
                if (retry_campaign is not None and campaign_deadline is not None
                        and time.monotonic() >= campaign_deadline):
                    campaign_stop_reason = "zero_fill"
                    break
                refreshed_trade = self._refresh_execution_plan(trade)
                if refreshed_trade is None:
                    campaign_stop_reason = (
                        "protected_entry_failed" if open_retry_campaign else
                        self._exit_campaign_stop_reason())
                    break
                trade = refreshed_trade
                self._active_trade = trade
                self._recovery_context.update(self._execution_context(trade))
                plan = trade.plan
                if campaign is not None:
                    campaign["quoted_cycle_pnl"] = trade.cycle_budget.get(
                        "quoted_net_profit_usd")
                    campaign["protected_cycle_pnl"] = trade.cycle_budget.get(
                        "net_profit_usd")
                    campaign["dynamic_hedge_reserve_bps"] = trade.cycle_budget.get(
                        "dynamic_hedge_reserve_bps")
                    campaign["first_leg_room_bps"] = trade.cycle_budget.get(
                        "first_leg_execution_room_bps",
                        trade.per_leg_slippage_bps)
                    campaign["close_execution_room_bps"] = trade.cycle_budget.get(
                        "close_execution_room_bps")
                    campaign["close_execution_room_min_bps"] = trade.cycle_budget.get(
                        "close_execution_room_min_bps")
                    campaign["close_execution_room_target_bps"] = trade.cycle_budget.get(
                        "close_execution_room_target_bps")
                    self._recovery_context["exit_campaign"] = dict(campaign)
                if open_campaign is not None:
                    latest_plan = {
                        "protected_open_edge_bps": trade.cycle_budget.get(
                            "protected_open_edge_bps"),
                        "required_entry_edge_bps": trade.cycle_budget.get(
                            "required_entry_edge_bps"),
                        "execution_room_bps": trade.cycle_budget.get(
                            "execution_room_bps"),
                        "first_leg_execution_room_bps": trade.cycle_budget.get(
                            "first_leg_execution_room_bps"),
                        "latency_required_room_bps": trade.cycle_budget.get(
                            "latency_required_room_bps"),
                        "economic_surplus_bps": trade.cycle_budget.get(
                            "economic_surplus_bps"),
                        "entropy_limit_px": trade.cycle_budget.get(
                            "entropy_limit_px"),
                        "economic_required_dislocation_bps": trade.cycle_budget.get(
                            "economic_required_dislocation_bps"),
                    }
                    eligible, reason = self._open_retry_eligibility(trade)
                    if not eligible:
                        # A retry may be less attractive than the prior
                        # snapshot. It is still valid while the current
                        # protected floor and execution-room floor hold.
                        campaign_stop_reason = reason
                        open_campaign["last_plan"] = latest_plan
                        self._recovery_context["open_campaign"] = dict(
                            open_campaign)
                        break
                    open_campaign["last_plan"] = latest_plan
                    self._recovery_context["open_campaign"] = dict(
                        open_campaign)

            budget_action = (StrategyAction.REDUCE if reduce_campaign
                             else StrategyAction.OPEN)
            budget_venues = (self.entropy, self.hedge)
            if (reduce_campaign
                    and not self._venue_rate_ok(
                        self.entropy, StrategyAction.REDUCE)):
                log.warning(
                    "[EXEC %s] REDUCE retry budget exhausted; preserving "
                    "one recovery slot", state.cycle_id)
                break
            if (open_retry_campaign
                    and not all(self._venue_rate_ok(v, budget_action)
                                for v in budget_venues)):
                campaign_stop_reason = "open_budget_reserved"
                log.info("[EXEC %s] OPEN retry budget reserved for "
                         "REDUCE/recovery", state.cycle_id)
                break

            # The reduction campaign widens only after a known zero-fill;
            # unresolved transport outcomes stay pinned to their ref and go
            # through the existing resolver instead of being duplicated.
            economic_reduce = (reduce_campaign and self._is_economic_exit(trade))
            if (not reduce_campaign
                    or trade.cycle_budget.get("entropy_limit_px") is not None):
                entropy_limit = (trade.buy_bound if entropy_is_buy
                                 else trade.sell_bound)
            elif economic_reduce:
                campaign_stop_reason = self._exit_campaign_stop_reason()
                break
            else:
                first_slip = (self._reduce_first_leg_slippage(attempt)
                              if reduce_campaign
                              else self._first_leg_slippage_bps(
                                  candidate.action))
                first_raw = (plan.buy_limit * (1 + first_slip / 1e4)
                             if entropy_is_buy
                             else plan.sell_limit * (1 - first_slip / 1e4))
                entropy_limit = self.entropy.px_round(
                    first_raw, round_up=not entropy_is_buy)
            first_quote = self._quote_snapshot(
                self.entropy, entropy_is_buy, plan.qty)
            initial_hedge_quote = self._quote_snapshot(
                self.hedge, not entropy_is_buy, plan.qty)
            first_ref = self._deterministic_order_ref(
                state, "first", attempt)
            self._prepare_order_send(
                state, "first", first_ref, limit_px=entropy_limit,
                qty=plan.qty)
            self._record_send(self.entropy, candidate.action)
            first_submit_ts = time.time()
            first_sent = time.perf_counter()
            if first_attempts == 0:
                latency["preflight_to_first_submit"] = round(
                    (first_sent - execution_started) * 1000, 3)
            first_attempts += 1
            if campaign is not None:
                campaign["retry_count"] = attempt
            if open_campaign is not None:
                open_campaign["send_count"] += 1
                open_campaign["retry_count"] = attempt
                open_campaign["attempts"].append({
                    "attempt": attempt,
                    "order_ref": first_ref,
                    "submitted_ts": time.time(),
                    "limit_px": entropy_limit,
                    "qty": plan.qty,
                })
                opportunity = self._open_opportunities.get(
                    open_campaign.get("open_opportunity_key"))
                if opportunity is not None:
                    opportunity.update(
                        send_count=open_campaign["send_count"],
                        attempt_count=open_campaign["send_count"],
                        last_attempt=attempt,
                        last_limit_px=entropy_limit)
                    self._last_open_opportunity = dict(opportunity)
            try:
                einfo = await self.entropy.send_taker(
                    is_buy=entropy_is_buy, qty=plan.qty,
                    limit_px=entropy_limit,
                    reduce_only=candidate.reduce_only, order_ref=first_ref)
            except Exception as exc:
                einfo = self._failed_send(exc)

            first_returned = time.perf_counter()
            self._record_execution_cost(
                action=candidate.action, leg="first", venue=self.entropy,
                is_buy=entropy_is_buy, qty=plan.qty, quote=first_quote,
                limit_px=entropy_limit, info=einfo,
                submit_ts=first_submit_ts, terminal_ts=time.time())
            latency["first_submit_to_result"] = round(
                (first_returned - first_sent) * 1000, 3)
            if (candidate.action in {StrategyAction.OPEN, StrategyAction.ADD}
                    and latency["first_submit_to_result"] > 0):
                sample = latency["first_submit_to_result"] / 1000.0
                if math.isfinite(sample):
                    self._open_latency_samples_sec.append(
                        min(max(sample, 0.05), 5.0))
                    self._open_latency_sec_ewma = (
                        0.25 * min(max(sample, 0.05), 5.0)
                        + 0.75 * self._open_latency_sec_ewma)
            if open_campaign is not None and open_campaign["attempts"]:
                open_campaign["attempts"][-1].update({
                    "returned_ts": time.time(),
                    "status": einfo.get("status"),
                    "filled_qty": self._fill_amount(einfo),
                    "avg_px": einfo.get("avg_px"),
                    "error": einfo.get("err"),
                    "unresolved": bool(einfo.get("unresolved")),
                })
                self._recovery_context["open_campaign"] = dict(
                    open_campaign)
            self._observe_execution_result(
                state, "first", self.entropy, is_buy=entropy_is_buy,
                info=einfo, fallback_px=entropy_limit)
            if candidate.action in {StrategyAction.OPEN, StrategyAction.ADD}:
                self._note_open_fill_probability(
                    trade.cycle_budget.get("first_leg_execution_room_bps",
                                            trade.cycle_budget.get(
                                                "execution_room_bps")),
                    self._fill_amount(einfo) > 0)
            elif candidate.action == StrategyAction.REDUCE:
                self._note_close_fill_probability(
                    trade.cycle_budget.get("close_execution_room_bps",
                                            trade.cycle_budget.get(
                                                "execution_room_bps")),
                    self._fill_amount(einfo) > 0)
            if state.first_filled > 0 or state.pending_leg:
                break
        hinfo = {"status": "not-sent", "filled_base": 0.0,
                 "avg_px": None, "err": None, "unresolved": False}
        entropy_fill = state.first_filled
        if entropy_fill > 0 and not state.pending_leg:
            hedge_qty = floor_step(min(entropy_fill, plan.qty), self._step)
            hedge_is_buy = not entropy_is_buy
            state.hedge_is_buy = hedge_is_buy
            if candidate.action == StrategyAction.REDUCE:
                # Once the first close leg has filled, completion of the
                # opposite position is the safety objective.  Do not reapply
                # the economic reserve or a profit-floor bound here: those
                # are admission diagnostics, while this order must complete
                # the already-started reduce-only execution.
                hedge_cap_bps = self._execution_hard_cap_bps(
                    StrategyAction.REDUCE, "hedge")
                hedge_limit = self._live_taker_bound(
                    self.hedge, hedge_is_buy, hedge_cap_bps)
                trade.cycle_budget.update(
                    hedge_completion_mode=True,
                    hedge_completion_hard_cap_bps=hedge_cap_bps,
                    actual_hedge_bound_px=hedge_limit)
                self._recovery_context["cycle_budget"] = dict(
                    trade.cycle_budget)
                log.info(
                    "[HEDGE COMPLETION] execution_id=%s target=%.8g "
                    "limit=%.8g hard_cap_bps=%.3f",
                    state.cycle_id, hedge_qty, hedge_limit or 0.0,
                    hedge_cap_bps)
            else:
                hedge_limit = self._live_taker_bound(
                    self.hedge, hedge_is_buy,
                    self._execution_hard_cap_bps(candidate.action, "hedge"))
            if self._hedge_order_legal(hedge_qty, hedge_limit):
                log.info("[SECOND LEG] %s %.6g on %s @%.6g after Entropy "
                         "fill %.6g", "BUY" if hedge_is_buy else "SELL",
                         hedge_qty, self.hedge.name, hedge_limit, entropy_fill)
                hedge_ref = self._deterministic_order_ref(state, "hedge", 0)
                self._prepare_order_send(
                    state, "hedge", hedge_ref, limit_px=hedge_limit,
                    qty=hedge_qty)
                self._record_send(self.hedge, candidate.action)
                hedge_quote = self._quote_snapshot(
                    self.hedge, hedge_is_buy, hedge_qty)
                hedge_submit_ts = time.time()
                self._record_interleg_cost(
                    action=candidate.action, is_buy=hedge_is_buy,
                    qty=hedge_qty, initial_quote=initial_hedge_quote,
                    hedge_quote=hedge_quote, started_ts=first_submit_ts,
                    observed_ts=hedge_submit_ts)
                hedge_sent = time.perf_counter()
                latency["first_result_to_hedge_submit"] = round(
                    (hedge_sent - first_returned) * 1000, 3)
                try:
                    hinfo = await self.hedge.send_taker(
                        is_buy=hedge_is_buy, qty=hedge_qty,
                        limit_px=hedge_limit,
                        reduce_only=candidate.reduce_only, order_ref=hedge_ref)
                except Exception as exc:
                    hinfo = self._failed_send(exc)
                hedge_terminal_ts = time.time()
                self._record_execution_cost(
                    action=candidate.action, leg="hedge", venue=self.hedge,
                    is_buy=hedge_is_buy, qty=hedge_qty, quote=hedge_quote,
                    limit_px=hedge_limit, info=hinfo,
                    submit_ts=hedge_submit_ts,
                    terminal_ts=hedge_terminal_ts)
                latency["hedge_submit_to_result"] = round(
                    (time.perf_counter() - hedge_sent) * 1000, 3)
                hedge_latency_ms = (latency.get("first_result_to_hedge_submit", 0.0)
                                    + latency.get("hedge_submit_to_result", 0.0))
                if hedge_latency_ms > 0 and math.isfinite(hedge_latency_ms):
                    self._hedge_latency_samples_sec.append(
                        min(max(hedge_latency_ms / 1000.0, 0.05), 5.0))
                    self._hedge_latency_sec_ewma = (
                        0.25 * max(hedge_latency_ms / 1000.0, 0.05)
                        + 0.75 * self._hedge_latency_sec_ewma)
                self._observe_execution_result(
                    state, "hedge", self.hedge, is_buy=hedge_is_buy,
                    info=hinfo, fallback_px=hedge_limit)
            else:
                hinfo["status"] = "awaiting_topup"
                state.hedge_order_status = "awaiting_topup"
                log.warning("[EXEC %s] first fill %.8g is below the hedge "
                            "minimum; recovery will aggregate the first leg",
                            state.cycle_id, entropy_fill)

        latency["attempt_to_results"] = round(
            (time.perf_counter() - execution_started) * 1000, 3)
        log.info("[LATENCY %s] ms=%s first_status=%s hedge_status=%s",
                 state.cycle_id, latency, einfo["status"], hinfo["status"])
        if entropy_is_buy:
            binfo, sinfo = einfo, hinfo
        else:
            binfo, sinfo = hinfo, einfo
        for v, info, side in ((buy, binfo, "buy"), (sell, sinfo, "sell")):
            if info.get("err"):
                log.error("[%s] %s leg: %s", v.name, side, info["err"])
        bfill = self._fill_amount(binfo)
        sfill = self._fill_amount(sinfo)

        matched = min(bfill, sfill)
        fill_edge = 0.0
        if matched > 0 and binfo.get("avg_px") and sinfo.get("avg_px"):
            fill_edge = matched * (sinfo["avg_px"] * (1 - plan.sell_fee)
                                   - binfo["avg_px"] * (1 + plan.buy_fee))
            self.total_fill_edge += fill_edge
        log.info("[SETTLED] %s: buy %s %s %.6g/%.6g | sell %s %s %.6g/%.6g | "
                 "matched %.6g | fill edge $%.4f", direction,
                 buy.name, binfo["status"], bfill, plan.qty,
                 sell.name, sinfo["status"], sfill, plan.qty, matched, fill_edge)
        buy.last_traded_ts = sell.last_traded_ts = time.time()

        unresolved = bool(binfo.get("unresolved") or sinfo.get("unresolved"))
        hard_err = (binfo.get("err") is not None
                    or sinfo.get("err") is not None)
        rate_limited = False
        for v, info in ((buy, binfo), (sell, sinfo)):
            if str(info.get("err", "")).startswith("RATE_LIMITED"):
                rate_limited = True
                self._mark_limited(v)
            elif "margin" in str(info.get("status", "")).lower():
                log.warning("[%s] margin rejection — collateral exhausted, "
                            "pausing venue", v.name)
                self._mark_limited(v)
        paired_ok = (not hard_err and not unresolved and matched > 0
                     and abs(bfill - sfill) < self._step / 2
                     and not state.pending_leg
                     and not state.pending_order_ref
                     and self._execution_cost_ready(state)
                     and self._execution_fill_coverage_ready(state))
        if paired_ok or entropy_fill == 0:
            self.consec_errors = 0
        elif not rate_limited and (hard_err or unresolved):
            # Preserve only the pre-existing transport-error containment. A
            # fill mismatch is flattened but does not feed a new failure-rate
            # circuit breaker.
            self.consec_errors += 1
            if self.consec_errors >= cfg.max_consecutive_errors:
                self.halted = True
                log.critical("HALTED after %d consecutive transport problems "
                             "— flatten manually and restart",
                             self.consec_errors)

        self._pending_execution_record = {
            "trade": trade,
            "state": state,
            "buy_status": binfo["status"],
            "sell_status": sinfo["status"],
            "initial_paired": paired_ok,
            "unresolved": unresolved,
            "fill_edge": fill_edge,
            "inv_bps": inv_bps,
        }
        if campaign is not None:
            if paired_ok or entropy_fill > 0:
                campaign_result = "filled"
            elif state.pending_leg:
                campaign_result = "unknown"
            else:
                campaign_result = campaign_stop_reason or "zero_fill"
            self._finish_exit_campaign(state, campaign_result)
        if open_campaign is not None:
            if entropy_fill > 0:
                open_result = "filled"
            elif state.pending_leg:
                open_result = "unknown"
            else:
                open_result = campaign_stop_reason or "zero_fill"
            self._finish_open_campaign(state, open_result)
        if paired_ok:
            self._finish_execution("paired")
        elif entropy_fill == 0 and not einfo.get("unresolved"):
            self._finish_execution("no_fill")
        else:
            state.phase = ExecutionPhase.RECONCILING
            if not state.pending_leg:
                # A terminal partial/cancelled hedge can be retried safely;
                # an unresolved order is held until its client reference is
                # resolved and is never duplicated in this branch.
                state.pending_leg = None
            # The production caller still owns both venue locks here. Direct
            # unit/integration callers do not, so recover immediately only in
            # the latter case; _execute_locked continues it after releasing.
            locks_held = (self._vlock(buy.key).locked()
                          or self._vlock(sell.key).locked())
            if not locks_held:
                await self._recover_active_execution(immediate=True)
        action_cooldown = self._execution_cooldown_sec(candidate.action)
        retry = (self._no_fill_retry_sec(candidate.action)
                 if entropy_fill == 0 else action_cooldown)
        self.last_trade_ts = (time.time()
                              - max(action_cooldown - retry, 0.0))
        return self.active_execution is not None

    def _update_cycle_after_execution(self, candidate: TradeCandidate,
                                      matched: float, paired_ok: bool,
                                      state: Optional[ExecutionState] = None) -> None:
        """Persist entry context and counters only for paired outcomes."""
        if not self.cfg.cycle_exit.enabled or not paired_ok or matched <= 0:
            return
        pair_state = self._pair_state()
        if candidate.action == StrategyAction.OPEN:
            direction = cycle_direction_for_pair_state(pair_state)
            if direction is not None:
                self.cycle.start(
                    direction=direction,
                    opened_ts=time.time(),
                    entry_midline_bps=(candidate.cycle_entry_midline_bps
                                       if candidate.cycle_entry_midline_bps is not None
                                       else self.model_snapshot.active_midline_bps),
                    entry_sigma_bps=candidate.cycle_entry_sigma_bps,
                    entry_band_bps=(candidate.effective_entry_band_bps
                                    if candidate.effective_entry_band_bps is not None
                                    else 0.0),
                    entry_threshold_bps=candidate.threshold_bps,
                    paired_qty=matched,
                    cycle_id=(state.cycle_id if state is not None else None),
                    entry_reference_bps=candidate.cycle_entry_midline_bps,
                    entry_reference_mode=candidate.cycle_entry_reference_mode,
                    entry_active_midline_bps=(
                        candidate.cycle_entry_active_midline_bps),
                    entry_dynamic_center_bps=(
                        candidate.cycle_entry_dynamic_center_bps))
        elif candidate.action == StrategyAction.ADD:
            if self.cycle.active and self.cycle.anchored:
                self.cycle.add_count += 1
                self.cycle.peak_paired_qty = max(
                    self.cycle.peak_paired_qty, matched)
        elif candidate.action == StrategyAction.REDUCE:
            if self.cycle.active:
                self.cycle.reduce_count += 1
        self._persist_cycle()

    def _record_trade(self, direction: str, plan: ArbPlan, fill_edge,
                      status: str, ok: bool, action: StrategyAction,
                      execution_id: Optional[str] = None) -> None:
        self.recent_trades.append({
            "ts": time.time(), "execution_id": execution_id,
            "direction": direction, "qty": plan.qty,
            "notional": plan.buy_notional,
            "prem_bps": plan.marginal_premium_bps,
            "exp": plan.exp_edge_usd, "fill": fill_edge, "status": status,
            "ok": ok, "action": action.value})

    async def _maybe_hedge(self) -> None:
        net = sum(v.position for v in self.venues.values())
        if abs(net) > self.cfg.net_tolerance_base:
            await self._hedge(net)

    async def _recover_emergency_pending(self) -> None:
        """Resolve emergency IOC intents before submitting another one."""
        for key, pending in list(self._emergency_pending.items()):
            venue = self.venues.get(pending["venue"])
            if venue is None:
                continue
            resolver = getattr(venue, "resolve_order", None)
            if not callable(resolver):
                continue
            order_ref = pending["order_ref"]
            try:
                info = await resolver(
                    order_ref, timeout=min(self.cfg.settle_timeout_sec, 1.0))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("[EMERGENCY EXIT] resolve %s failed: %r",
                            order_ref, exc)
                continue
            info = {**info, "order_ref": order_ref}
            observed = self._fill_amount(info)
            history = []
            fetcher = getattr(venue, "fetch_fee_fills", None)
            if callable(fetcher):
                try:
                    history = await fetcher(
                        max(0.0, pending["created_ts"] - 1.0), time.time())
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("[EMERGENCY EXIT] fill history %s failed: %r",
                                order_ref, exc)
            recovered = self.ledger.recover_order_fills(
                execution_id=pending["execution_id"], leg="emergency",
                venue=venue.key, order_ref=order_ref,
                side="buy" if pending["is_buy"] else "sell",
                exchange_rows=history, observed_qty=observed,
                avg_px=info.get("avg_px"), fee_usd=(
                    info.get("fee_usd") if info.get("fee_usd") is not None
                    else ((observed * float(info["avg_px"]) * venue.fee_bps / 1e4)
                          if info.get("avg_px") is not None else None)),
                fee_source=self._fill_fee_source(venue),
                submit_ts=pending["created_ts"], ts=time.time(),
                position_before=pending["position_before"])
            if recovered.get("known") and pending.get("applied_qty", 0.0) > 0:
                # The first emergency response may have used its protected
                # limit as a temporary mark because no average was returned.
                # Bring the in-memory cash/volume to the same exact
                # incremental cost that repaired the fallback lot.
                notional_delta = float(
                    recovered.get("notional_delta_usd") or 0.0)
                fee_delta = float(recovered.get("fee_delta_usd") or 0.0)
                signed_cash_delta = (
                    (-1.0 if pending["is_buy"] else 1.0) * notional_delta
                    + (-1.0 if pending["is_buy"] else -1.0) * fee_delta)
                venue.cash += signed_cash_delta
                venue.volume_usd += notional_delta
            if recovered.get("known"):
                observed = max(observed,
                               float(recovered.get("observed_qty") or 0.0))
                info = {**info, "filled_base": observed,
                        "avg_px": recovered.get("avg_px") or info.get("avg_px"),
                        "unresolved": False}
            applied = float(pending.get("applied_qty") or 0.0)
            extra = max(observed - applied, 0.0)
            if extra > self.cfg.net_tolerance_base:
                sign = 1.0 if pending["is_buy"] else -1.0
                expected_before_extra = (
                    pending["position_before"] + sign * applied)
                if abs(venue.position - expected_before_extra) <= self.cfg.net_tolerance_base:
                    fill_info = dict(info)
                    fill_info["avg_px"] = (info.get("avg_px")
                                            or pending["limit_px"])
                    self._apply_leg_fill(
                        venue, is_buy=pending["is_buy"], fill=extra,
                        info=fill_info, fallback_px=pending["limit_px"])
                else:
                    log.warning("[EMERGENCY EXIT] %s position changed while "
                                "recovering: expected %.8g got %.8g",
                                venue.name, expected_before_extra,
                                venue.position)
            pending["applied_qty"] = max(applied, observed)
            try:
                self.ledger.record_order(
                    execution_id=pending["execution_id"], leg="emergency",
                    venue=venue.key, order_ref=order_ref,
                    side="buy" if pending["is_buy"] else "sell",
                    planned_qty=pending["qty"], limit_px=pending["limit_px"],
                    reduce_only=True, status=str(info.get("status") or
                                                 "recovered"),
                    filled_qty=observed,
                    avg_px=(float(info["avg_px"])
                            if info.get("avg_px") is not None else None),
                    unresolved=bool(info.get("unresolved")))
            except Exception:
                log.exception("[EMERGENCY EXIT] order ledger update failed")
            terminal_no_fill = (not info.get("unresolved") and observed <= 0)
            if recovered.get("known") or terminal_no_fill:
                self._emergency_pending.pop(key, None)
                log.info("[FILL RECOVERY] execution_id=%s order_ref=%s "
                         "expected_qty=%.8g recovered_qty=%.8g avg_px=%s "
                         "fee=%s source=%s", pending["execution_id"],
                         order_ref, pending["qty"], observed,
                         info.get("avg_px"), info.get("fee_usd"),
                         recovered.get("source", "order_status"))

    def _ensure_emergency_campaign(self, reason: str,
                                   cycle_id: Optional[str]) -> dict:
        """Return the retry budget for one residual-inventory campaign."""
        key = (str(reason), str(cycle_id or "inventory"))
        campaign = self._emergency_campaign
        if campaign is None or campaign.get("key") != key:
            campaign = {
                "id": f"{key[0]}:{key[1]}:{int(time.time() * 1000)}",
                "key": key,
                "attempts": {},
            }
            self._emergency_campaign = campaign
            log.info("[EMERGENCY EXIT] new campaign=%s reason=%s cycle=%s",
                     campaign["id"], reason, cycle_id or "inventory")
        return campaign

    async def _emergency_exit_positions(self, reason: str) -> bool:
        """Flatten every non-zero venue position with reduce-only IOC orders.

        This path intentionally ignores cross-venue premium, profit floors and
        the normal send budget.  It still requires the target venue's own book
        to be fresh and never submits a second order while an earlier emergency
        reference is unresolved.
        """
        await self._recover_emergency_pending()
        cfg = self.cfg
        tolerance = self._execution_flat_tolerance(self.active_execution)
        if self._positions_flat(tolerance):
            # Flatness is already the desired terminal safety result.  Do not
            # create a campaign or submit a fresh reduce-only order merely
            # because an old recovery path reached this method.
            state = self.active_execution
            if state is not None:
                self._set_hedge_completion_state(
                    state, EXECUTION_SAFETY_FLAT,
                    "emergency_exit_requested_while_flat")
            self._mark_reconciliation_synced_if_flat()
            log.info("[EMERGENCY EXIT] already flat; no order submitted "
                     "reason=%s", reason)
            return True
        cycle_state = self.active_execution
        cycle_record = (self._ensure_cycle_telemetry(cycle_state)
                        if cycle_state is not None else
                        (self._cycle_telemetry
                         if self.cycle.cycle_id else None))
        campaign = self._ensure_emergency_campaign(
            reason, cycle_record.get("cycle_id") if cycle_record else None)
        if cycle_record is not None:
            cycle_record["reduce_decision_ts"] = (
                cycle_record.get("reduce_decision_ts") or time.time())
            exit_mode, risk_type, execution_emergency = self._exit_telemetry(
                reason)
            cycle_record["exit_mode"] = exit_mode
            cycle_record["exit_reason"] = reason
            cycle_record["emergency_flag"] = bool(
                cycle_record.get("emergency_flag") or execution_emergency)
            cycle_record["emergency_campaign_id"] = campaign["id"]
            if risk_type:
                cycle_record["risk_exit_type"] = risk_type
                if risk_type in {"regime_stop", "max_hold", "hard_timeout",
                                 "margin_risk"}:
                    cycle_record.setdefault("risk_exit_basis_bps",
                                           self._current_basis_bps())
            try:
                self.ledger.upsert_cycle(cycle_record)
            except Exception:
                log.exception("[CYCLE] emergency telemetry write failed")
        for venue in self.venues.values():
            position = float(venue.position)
            if abs(position) <= tolerance:
                campaign["attempts"][venue.key] = 0
                continue
            if any(item.get("venue") == venue.key
                   for item in self._emergency_pending.values()):
                continue
            if venue.key in self._venue_down:
                log.warning("[EMERGENCY EXIT] venue=%s unavailable reason=%s",
                            venue.key, reason)
                continue
            if not venue.book.is_fresh(cfg.staleness_sec):
                log.warning("[EMERGENCY EXIT] venue=%s book_stale reason=%s",
                            venue.key, reason)
                continue
            lock = self._vlock(venue.key)
            if lock.locked():
                continue
            is_buy = position < 0
            qty = floor_step(abs(position), 10.0 ** -int(venue.size_decimals))
            base_min, quote_min = self._taker_minimums(venue, reduce_only=True, qty=qty)
            if qty < base_min:
                log.warning("[EMERGENCY EXIT] venue=%s dust=%.8g reason=%s",
                            venue.key, position, reason)
                continue

            # If the current position has no authoritative lot, create an
            # explicitly unknown baseline now. This is only reached by the
            # orphan/emergency path, never by a normal startup observation.
            try:
                inventory = self.ledger.inventory_status(venue.key)
                if abs(float(inventory["qty"]) - position) > tolerance:
                    self.ledger.ensure_position_baseline(
                        venue=venue.key, exchange_qty=position,
                        mark_px=venue.book.mid(), ts=time.time(),
                        tolerance=tolerance)
            except Exception:
                log.exception("[EMERGENCY EXIT] baseline preparation failed "
                             "for %s", venue.name)

            attempt = int(campaign["attempts"].get(venue.key, 0))
            schedule = tuple(getattr(cfg, "reduce_retry_escalation_bps", ()))
            slippage = (schedule[attempt] if attempt < len(schedule)
                        else cfg.emergency_slippage_bps)
            ref = (venue.book.best_ask() if is_buy
                   else venue.book.best_bid())
            if ref is None:
                continue
            raw = ref * (1 + slippage / 1e4) if is_buy \
                else ref * (1 - slippage / 1e4)
            limit = venue.px_round(raw, round_up=not is_buy)
            if qty * limit < quote_min:
                log.warning("[EMERGENCY EXIT] venue=%s qty=%.8g below minimum "
                            "notional reason=%s", venue.key, qty, reason)
                continue

            await lock.acquire()
            try:
                # Re-read the position after acquiring the venue lock. A
                # reconcile may have flattened it while this task waited.
                position = float(venue.position)
                if abs(position) <= tolerance:
                    continue
                is_buy = position < 0
                qty = floor_step(abs(position), 10.0 ** -int(venue.size_decimals))
                base_min, quote_min = self._taker_minimums(venue, reduce_only=True, qty=qty)
                if qty < base_min or qty * limit < quote_min:
                    continue
                execution_id = (
                    f"emergency-exit:{reason}:{venue.key}:"
                    f"{int(time.time() * 1000)}:{attempt}")
                if cycle_record is not None:
                    ids = [str(value) for value in cycle_record.get(
                        "execution_ids", []) if value]
                    if execution_id not in ids:
                        ids.append(execution_id)
                    cycle_record["execution_ids"] = ids
                order_ref = self._stable_order_ref(execution_id)
                created_ts = time.time()
                self.ledger.record_order(
                    execution_id=execution_id, leg="emergency",
                    venue=venue.key, order_ref=order_ref,
                    side="buy" if is_buy else "sell", planned_qty=qty,
                    limit_px=limit, reduce_only=True, status="intent",
                    filled_qty=0.0, avg_px=None, unresolved=True,
                    ts=created_ts)
                self._record_send(venue)
                try:
                    info = await venue.send_taker(
                        is_buy=is_buy, qty=qty, limit_px=limit,
                        reduce_only=True, order_ref=order_ref)
                except Exception as exc:
                    info = self._failed_send(exc)
                info = {**info, "order_ref": order_ref}
                fill = self._fill_amount(info)
                avg = info.get("avg_px")
                try:
                    avg = float(avg) if avg is not None else None
                except (TypeError, ValueError):
                    avg = None
                position_before = position
                applied = 0.0
                if fill > 0:
                    fill_info = dict(info)
                    fill_info["avg_px"] = avg or limit
                    self._apply_leg_fill(
                        venue, is_buy=is_buy, fill=fill,
                        info=fill_info, fallback_px=limit)
                    applied = fill
                    if avg is not None and math.isfinite(avg) and avg > 0:
                        try:
                            self.ledger.record_cumulative_fill(
                                execution_id=execution_id, leg="emergency",
                                venue=venue.key, order_ref=order_ref,
                                side="buy" if is_buy else "sell",
                                observed_qty=fill, avg_px=avg,
                                fee_usd=info.get("fee_usd"),
                                fee_source=("exchange_api"
                                            if info.get("fee_usd") is not None
                                            else self._fill_fee_source(venue)),
                                ts=time.time(),
                                position_before=position_before,
                                position_after=venue.position,
                                source="emergency_exit",
                                metadata={"reason": reason,
                                          "price_source": "exchange_average"})
                        except Exception:
                            log.exception("[EMERGENCY EXIT] fill ledger write "
                                          "failed")
                    else:
                        try:
                            self.ledger.record_fill(
                                fill_key=(f"unpriced:{execution_id}:emergency:"
                                          f"{order_ref}:{fill:.12g}"),
                                execution_id=execution_id, leg="emergency",
                                venue=venue.key, order_ref=order_ref,
                                ts=time.time(),
                                side="buy" if is_buy else "sell", qty=fill,
                                price=limit,
                                fee_usd=(fill * limit * venue.fee_bps / 1e4),
                                position_before=position_before,
                                position_after=venue.position,
                                source="emergency_exit_unpriced",
                                fee_source=self._fill_fee_source(venue),
                                metadata={"reason": reason,
                                          "price_source": "limit_fallback"})
                        except Exception:
                            log.exception("[EMERGENCY EXIT] unpriced fill "
                                          "ledger write failed")
                unresolved = bool(info.get("unresolved"))
                if unresolved or (fill > 0 and avg is None):
                    self._emergency_pending[
                        f"{venue.key}:{order_ref}"] = {
                            "execution_id": execution_id,
                            "venue": venue.key, "order_ref": order_ref,
                            "is_buy": is_buy, "qty": qty,
                            "limit_px": limit, "created_ts": created_ts,
                            "position_before": position_before,
                            "applied_qty": applied,
                        }
                else:
                    self.ledger.record_order(
                        execution_id=execution_id, leg="emergency",
                        venue=venue.key, order_ref=order_ref,
                        side="buy" if is_buy else "sell", planned_qty=qty,
                        limit_px=limit, reduce_only=True,
                        status=str(info.get("status") or "settled"),
                        filled_qty=fill, avg_px=avg, unresolved=False)
                if info.get("err"):
                    log.error("[EMERGENCY EXIT] venue=%s error=%s",
                              venue.key, info["err"])
                log.warning("[EMERGENCY EXIT] reason=%s venue=%s qty=%.8g "
                            "slippage_bound=%.3fbps attempt=%d status=%s "
                            "fill=%.8g", reason, venue.key, qty, slippage,
                            attempt, info.get("status"), fill)
                try:
                    self.ledger.record_trade_attempt({
                        "trade_key": execution_id,
                        "execution_id": execution_id,
                        "ts": created_ts, "action": "emergency_exit",
                        "direction": ("buy_" if is_buy else "sell_")
                        + venue.key, "qty": qty,
                        "buy_notional": fill * (avg or limit)
                        if is_buy else 0.0,
                        "sell_notional": fill * (avg or limit)
                        if not is_buy else 0.0,
                        "buy_fill": fill if is_buy else 0.0,
                        "sell_fill": fill if not is_buy else 0.0,
                        "buy_status": info.get("status")
                        if is_buy else "not-sent",
                        "sell_status": info.get("status")
                        if not is_buy else "not-sent",
                        "buy_venue": venue.key if is_buy else "",
                        "sell_venue": venue.key if not is_buy else "",
                        "fill_edge_usd": None,
                        "outcome": "one_leg" if fill > 0 else "no_fill",
                        "reduce_only": True,
                    }, source="emergency_exit")
                except Exception:
                    log.exception("[EMERGENCY EXIT] attempt ledger write failed")
                venue.last_traded_ts = time.time()
                campaign["attempts"][venue.key] = attempt + 1
            finally:
                lock.release()
        flat = self._positions_flat(tolerance)
        if flat:
            self._clear_orphaned_if_flat()
            if cycle_record is not None and cycle_record.get(
                    "open_first_fill_ts") is not None:
                now = time.time()
                cycle_record["flat_ts"] = now
                opened = _safe_float(cycle_record.get("open_first_fill_ts"))
                if opened is not None:
                    cycle_record["hold_sec"] = max(now - opened, 0.0)
                cycle_record["status"] = "completed"
                financials = self.ledger.cycle_financials(
                    cycle_record.get("execution_ids", []),
                    opened_ts=opened, until=now)
                cycle_record["fee_accounting_status"] = financials["fee_accounting_status"]
                cycle_record["accounting_status"] = "provisional"
                cycle_record["cycle_fees_usd"] = financials["fees_usd"]
                cycle_record["total_cycle_pnl_usd"] = financials[
                    "net_pnl_usd"]
                cycle_record["close_fee_usd"] = financials["fees_usd"]
                cycle_record["updated_ts"] = now
                self._register_reentry_after_risk_exit(cycle_record, now)
                self._last_cycle_telemetry = dict(cycle_record)
                try:
                    self.ledger.upsert_cycle(cycle_record)
                except Exception:
                    log.exception("[CYCLE] emergency completion write failed")
                self._maybe_stop_after_complete_cycle(cycle_record)
            self._emergency_campaign = None
        return flat

    async def _hedge(self, net: float) -> None:
        """Reduce the venue that carries the imbalance back toward net zero
        (reduce-only taker with emergency_slippage_bps price protection)."""
        cfg = self.cfg
        is_sell = net > 0
        sgn = 1.0 if net > 0 else -1.0
        emergency_slippage = (cfg.emergency_slippage_bps
                              if getattr(cfg, "action_specific_execution", False)
                              else cfg.hedge_slippage_bps)
        slip = emergency_slippage / 1e4
        for v in sorted(self.venues.values(),
                        key=lambda x: (self._venue_limited(x), -x.position * sgn)):
            if v.position * sgn <= 0:
                continue
            if v.key in self._venue_down \
                    or not v.book.is_fresh(cfg.staleness_sec):
                continue  # unreachable or blind: cannot hedge here
            lk = self._vlock(v.key)
            if lk.locked():
                continue
            qty = floor_step(min(abs(net), abs(v.position)), self._step)
            base_min, quote_min = self._taker_minimums(v, reduce_only=True, qty=qty)
            if qty < base_min:
                continue
            ref = v.book.best_bid() if is_sell else v.book.best_ask()
            if ref is None:
                continue
            limit = v.px_round(ref * (1 - slip), False) if is_sell \
                else v.px_round(ref * (1 + slip), True)
            if qty * limit < quote_min:
                continue
            await lk.acquire()  # verified free, no awaits since: fast path
            try:
                execution_id = f"emergency-hedge:{int(time.time() * 1000)}"
                order_ref = self._stable_order_ref(execution_id)
                log.warning("[HEDGE] net %+.6g — %s %.6g on %s @%.6g",
                            net, "SELL" if is_sell else "BUY", qty, v.name, limit)
                self.hedges += 1
                self._record_send(v)  # counts toward the budget, never blocked
                try:
                    info = await v.send_taker(
                        is_buy=not is_sell, qty=qty, limit_px=limit,
                        reduce_only=True, order_ref=order_ref)
                except Exception as exc:
                    info = self._failed_send(exc)
                    info["order_ref"] = order_ref
                order_ref = self._order_ref(info)
                try:
                    self.ledger.record_order(
                        execution_id=execution_id, leg="emergency",
                        venue=v.key, order_ref=order_ref,
                        side="buy" if not is_sell else "sell",
                        planned_qty=qty, limit_px=limit, reduce_only=True,
                        status=str(info.get("status") or "unknown"),
                        filled_qty=self._fill_amount(info),
                        avg_px=(float(info["avg_px"])
                                if info.get("avg_px") is not None else None),
                        unresolved=bool(info.get("unresolved")))
                except Exception:
                    log.exception("[HEDGE] order ledger write failed")
                if info.get("err") or info.get("unresolved"):
                    log.error("[HEDGE] %s: %s", v.name,
                              info.get("err") or "unresolved")
                    if str(info.get("err", "")).startswith("RATE_LIMITED"):
                        self._mark_limited(v)
                    self._reconcile_evt.set()
                else:
                    fill = self._fill_amount(info)
                    position_before = v.position
                    self._apply_leg_fill(
                        v, is_buy=not is_sell, fill=fill, info=info,
                        fallback_px=limit)
                    if fill:
                        px = info.get("avg_px") or limit
                        try:
                            self.ledger.record_fill(
                                fill_key=(f"{execution_id}:{v.key}:"
                                          f"{fill:.12g}"),
                                execution_id=execution_id, leg="emergency",
                                venue=v.key, order_ref=order_ref, ts=time.time(),
                                side="buy" if not is_sell else "sell",
                                qty=fill, price=float(px),
                                fee_usd=fill * float(px) * v.fee_bps / 1e4,
                                fee_source=self._fill_fee_source(v),
                                position_before=position_before,
                                position_after=v.position,
                                source="emergency_hedge",
                                metadata={"net_before": net})
                        except Exception:
                            log.exception("[HEDGE] fill ledger write failed")
                        try:
                            self.ledger.record_trade_attempt({
                                "trade_key": execution_id,
                                "execution_id": execution_id,
                                "ts": time.time(), "action": "hedge",
                                "direction": "emergency_reduce",
                                "qty": fill,
                                "buy_notional": fill * float(px)
                                if not is_sell else 0.0,
                                "sell_notional": fill * float(px)
                                if is_sell else 0.0,
                                "buy_fill": fill if not is_sell else 0.0,
                                "sell_fill": fill if is_sell else 0.0,
                                "buy_status": info.get("status")
                                if not is_sell else "not-sent",
                                "sell_status": info.get("status")
                                if is_sell else "not-sent",
                                "buy_venue": v.key if not is_sell else "",
                                "sell_venue": v.key if is_sell else "",
                                "fill_edge_usd": None,
                                "outcome": "one_leg",
                                "reduce_only": True,
                            }, source="emergency_hedge")
                        except Exception:
                            log.exception("[HEDGE] attempt ledger write failed")
                    log.info("[HEDGE SETTLED] %s %s %.6g/%.6g",
                             v.name, info["status"], fill, qty)
                v.last_traded_ts = time.time()
            finally:
                lk.release()
            return
        log.warning("[HEDGE] net %+.6g below hedgeable minimum — carrying "
                    "(next reconcile retries)", net)

    # --------------------------------------------------- reconcile / status

    # Lighter's REST account state lags its ws settlements; overwriting a
    # venue that traded seconds ago "restores" stale positions and triggers
    # phantom hedge oscillations. Grace-guard + venue lock prevent that.
    RECONCILE_GRACE_SEC = 5.0

    async def _reconcile_positions(self, hedge: bool,
                                   strict: bool = False) -> None:
        now = time.time()
        if strict:
            self.reconciliation["status"] = "syncing"
        vs = []
        for v in self.venues.values():
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                continue  # just traded: chain read would be stale
            if v.key in self._venue_down \
                    and now < self._venue_probe_at.get(v.key, 0.0):
                continue  # down venue: probe only every venue_probe_sec
            vs.append(v)
        if not vs:
            return
        got = await asyncio.gather(
            *(self._reconcile_venue(v, strict) for v in vs),
            return_exceptions=True)
        for r in got:
            if isinstance(r, BaseException):
                raise r  # strict startup: fail loudly
        if strict:
            self.startup_reconciled = True
            self.reconciliation["startup_reconciled"] = True
        if self.active_execution is not None:
            self._update_hedge_completion_evidence(
                self.active_execution, reason="position_reconciliation")
        if self.active_execution is not None:
            self.reconciliation["status"] = "recovery_pending"
        elif self._orphaned_inventory:
            self.reconciliation["status"] = "orphaned_inventory"
        elif self.startup_reconciled and self._positions_flat():
            self.reconciliation["status"] = "synced"
        elif self.startup_reconciled and self._pair_positions_balanced():
            self.reconciliation["status"] = "positions_open"
        elif self.startup_reconciled:
            self.reconciliation["status"] = "blocked"
            self.reconciliation["net_delta_base"] = sum(
                v.position for v in self.venues.values())
        if hedge:
            if self._orphaned_inventory:
                await self._emergency_exit_positions("orphaned_inventory")
            elif self.active_execution is None:
                await self._maybe_hedge()
            net = sum(v.position for v in self.venues.values())
            self.reconciliation["net_delta_base"] = net
            if self.active_execution is not None:
                self.reconciliation["status"] = "recovery_pending"
            elif self._orphaned_inventory:
                self.reconciliation["status"] = "orphaned_inventory"
            elif self._positions_flat():
                self.reconciliation["status"] = "synced"
            elif self._pair_positions_balanced():
                self.reconciliation["status"] = "positions_open"
        self._sync_cycle_with_positions()
        self._maybe_resume_bootstrap(now)

    async def _reconcile_venue(self, v, strict: bool) -> bool:
        async with self._vlock(v.key):
            now = time.time()
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                return False  # traded while waiting for the lock
            try:
                r = await v.fetch_position()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if strict:
                    raise RuntimeError(
                        f"[{v.name}] cannot fetch starting position: {e!r}")
                # exchange unreachable (e.g. scheduled maintenance): pause
                # trading and keep probing until it answers again
                n = self._venue_fetch_fails.get(v.key, 0) + 1
                self._venue_fetch_fails[v.key] = n
                self._venue_probe_at[v.key] = now + self.cfg.venue_probe_sec
                self.reconciliation["status"] = "error"
                self.reconciliation["last_error"] = f"{v.name}: {e!r}"
                self.reconciliation["last_ts"] = now
                if n >= 3 and v.key not in self._venue_down:
                    self._venue_down[v.key] = now
                    log.critical("[%s] API unreachable (%d attempts) — "
                                 "trading PAUSED; probing every %.0fs until "
                                 "it recovers", v.name, n,
                                 self.cfg.venue_probe_sec)
                elif v.key not in self._venue_down:
                    log.warning("[%s] position fetch failed (%d): %r",
                                v.name, n, e)
                return False
            if v.key in self._venue_down:
                log.warning("[%s] API recovered after %.0fs outage — "
                            "trading RESUMED", v.name,
                            now - self._venue_down.pop(v.key))
                self._update_evt.set()
            self._venue_fetch_fails[v.key] = 0
            local_qty = v.position
            delta = r - local_qty
            if abs(delta) > 1e-12:
                if abs(delta) > self.cfg.net_tolerance_base:
                    log.warning("[%s] reconcile: chain %+.6g vs local %+.6g "
                                "— adopting chain", v.name, r, v.position)
                mid = v.book.mid()
                if mid is not None:
                    v.cash -= delta * mid
                v.position = r
                if self.active_execution is not None:
                    reconciled = self._recovery_context.setdefault(
                        "position_reconciled_venues", [])
                    if v.key not in reconciled:
                        reconciled.append(v.key)
            mark = v.book.mid()
            try:
                self.ledger.record_reconciliation(
                    ts=now, venue=v.key, local_qty=local_qty,
                    exchange_qty=r, mark_px=mark,
                    status=("matched" if abs(delta) <= 1e-12 else "adopted"))
                recovery_owned = (self.active_execution is not None
                                  or self._restored_execution is not None)
                if not recovery_owned and abs(r) > self.cfg.net_tolerance_base:
                    inventory = self.ledger.inventory_status(v.key)
                    ledger_qty = float(inventory.get("qty") or 0.0)
                    if (abs(ledger_qty - r) > self.cfg.net_tolerance_base
                            or not inventory.get("known")):
                        # Do not turn a startup position delta into a
                        # mark-priced fill. It is an orphan until its cost is
                        # recovered or the reduce-only emergency path exits.
                        self._mark_orphaned(
                            f"{v.key}_position_without_fill_history")
            except Exception as exc:
                self.halted = True
                log.critical("[%s] durable reconciliation write failed: %r — "
                             "new trading halted", v.name, exc)
            self.reconciliation["venues"][v.key] = {
                "local_base": local_qty,
                "exchange_base": r,
                "delta_base": delta,
                "mark_px": mark,
                "status": ("matched" if abs(delta) <= 1e-12 else "adopted"),
                "ts": now,
            }
            self.reconciliation["last_ts"] = now
            self.reconciliation["net_delta_base"] = sum(
                venue.position for venue in self.venues.values())
            return True

    async def _reconcile_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self._reconcile_evt.wait(),
                                       timeout=self.cfg.reconcile_sec)
                self._reconcile_evt.clear()
                await asyncio.sleep(1.0)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            try:
                if self.active_execution is not None:
                    # An active cycle owns recovery.  Resolve its client
                    # order first, then refresh chain positions, then make
                    # one more recovery pass.  The generic hedge path must
                    # not race or duplicate a still-unknown order.
                    await self._recover_active_execution(immediate=False)
                    if self.active_execution is not None:
                        await self._reconcile_positions(hedge=False)
                    if self.active_execution is not None:
                        await self._recover_active_execution(immediate=False)
                else:
                    await self._reconcile_positions(hedge=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconcile failed")

    async def _fee_reconciliation_loop(self) -> None:
        """Refresh authoritative per-fill fees without blocking trading."""
        while not self.stop.is_set():
            started = time.time()
            lookback_sec = (FEE_RECONCILIATION_LOOKBACK_SEC
                            if self.fee_reconciliation.get(
                                "initial_sync_done") else
                            FEE_RECONCILIATION_INITIAL_LOOKBACK_SEC)
            fetched = stored = actual = unmatched = 0
            actual_fee = fee_delta = cash_adjustment = 0.0
            errors = []
            for venue in self.venues.values():
                if getattr(venue, "kind", "") == "lighter":
                    # Lighter is configured as a zero-fee hedge venue here;
                    # there is nothing to回查 and it must not hold the
                    # Entropy fee status in a pending state.
                    continue
                fetcher = getattr(venue, "fetch_fee_fills", None)
                if not callable(fetcher):
                    errors.append(f"{venue.key}:unsupported")
                    continue
                try:
                    rows = await fetcher(
                        max(0.0, started - lookback_sec),
                        started)
                    result = self.ledger.reconcile_exchange_fills(
                        venue.key, rows, now=time.time(),
                        cash_since_ts=self.start_ts)
                    fetched += int(result.get("fetched_count", 0))
                    stored += int(result.get("stored_count", 0))
                    actual += int(result.get("actual_fill_count", 0))
                    unmatched += int(result.get("unmatched_count", 0))
                    actual_fee += float(result.get("actual_fee_usd", 0.0))
                    fee_delta += float(result.get("fee_delta_usd", 0.0))
                    venue_cash_adjustment = float(
                        result.get("cash_adjustment_usd", 0.0))
                    cash_adjustment += venue_cash_adjustment
                    if venue_cash_adjustment:
                        venue.cash -= venue_cash_adjustment
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    errors.append(f"{venue.key}:{type(exc).__name__}: {exc}")
                    log.warning("[%s] fee reconciliation failed: %s",
                                venue.name, exc)

            # A positive fee delta means the initial config estimate was too
            # low. The per-venue correction above moves the in-memory session
            # cash mark in the same direction as the exchange account;
            # historical fills deliberately do not touch session cash.
            self.fee_reconciliation.update({
                "status": "error" if errors else "ok",
                "initial_sync_done": (not errors),
                "last_ts": time.time(),
                "last_success_ts": None if errors else time.time(),
                "last_error": "; ".join(errors) if errors else None,
                "fetched_count": fetched,
                "stored_count": stored,
                "actual_fill_count": actual,
                "unmatched_count": unmatched,
                "actual_fee_usd": actual_fee,
                "fee_delta_usd": fee_delta,
                "cash_adjustment_usd": cash_adjustment,
            })
            if fee_delta:
                try:
                    self._record_pnl_snapshot()
                except Exception:
                    log.exception("fee reconciliation PnL snapshot failed")
            try:
                await asyncio.wait_for(
                    self.stop.wait(), timeout=FEE_RECONCILIATION_POLL_SEC)
            except asyncio.TimeoutError:
                pass

    async def _balance_loop(self) -> None:
        while not self.stop.is_set():
            equities = []
            for v in self.venues.values():
                try:
                    got = await v.fetch_equity()
                    if got is not None:
                        v.equity, v.free = got
                        self._update_margin_snapshot(v, v.equity, v.free)
                        if v.start_equity is None:
                            v.start_equity = v.equity
                        equities.append(v.equity)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.debug("[%s] equity poll failed: %r", v.name, e)
            if len(equities) == len(self.venues):
                try:
                    self.pnl_tracker.observe(equities)
                    # Keep equity history available even when the terminal or
                    # web dashboard is disabled.  Hourly ClawBot reports use
                    # these durable samples for the account-PnL delta.
                    self._record_pnl_snapshot()
                except Exception:
                    log.exception("PnL equity baseline update failed")
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=BALANCE_POLL_SEC)
            except asyncio.TimeoutError:
                pass

    async def _http_keepalive_loop(self) -> None:
        if self.cfg.http_keepalive_sec <= 0:
            return
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(),
                                       timeout=self.cfg.http_keepalive_sec)
                return
            except asyncio.TimeoutError:
                pass
            await asyncio.gather(*(v.warm_http() for v in self.venues.values()),
                                 return_exceptions=True)

    def account_delta(self) -> Optional[float]:
        """Change in real account equity since start (both venues)."""
        total = 0.0
        for v in self.venues.values():
            if v.equity is None or v.start_equity is None:
                return None
            total += v.equity - v.start_equity
        return total

    def _record_pnl_snapshot(self, now: Optional[float] = None) -> None:
        """Persist the current account PnL for report/dashboard consumers."""
        if not self.venues:
            return
        now = time.time() if now is None else float(now)
        pnl = self.pnl_tracker.snapshot()
        marks = {}
        for key, venue in self.venues.items():
            try:
                mark = venue.book.mid()
            except Exception:
                mark = None
            marks[key] = mark
            marks[getattr(venue, "key", key)] = mark
        lifetime = self.ledger.equity_lifetime(
            now=now, current_equity=pnl.get("current_equity_usd"))
        attribution = self.ledger.attribution(
            now=now,
            day_start_ts=pnl.get("day_started_ts"),
            session_start_ts=pnl.get("session_started_ts"),
            lifetime_start_ts=lifetime.get("start_ts"),
            marks=marks,
            equity_day=pnl.get("net_pnl_usd"),
            equity_session=pnl.get("session_pnl_usd"),
            equity_lifetime=lifetime.get("change_usd"),
        )
        self.ledger.record_pnl_snapshot(
            ts=now,
            equity_net_pnl_usd=pnl.get("net_pnl_usd"),
            attribution=attribution,
            equity_total_usd=pnl.get("current_equity_usd"))

    def session_pnl(self) -> Optional[float]:
        total = 0.0
        for v in self.venues.values():
            m = v.book.mid()
            if m is None:
                return None
            total += v.cash + v.position * m
        if self._mtm_baseline is None:
            self._mtm_baseline = total
        return total - self._mtm_baseline

    def premium_bps(self) -> Optional[float]:
        em, hm = self.entropy.book.mid(), self.hedge.book.mid()
        if not (em and hm):
            return None
        return (em / hm - 1.0) * 1e4

    async def _status_loop(self) -> None:
        while not self.stop.is_set():
            cfg = self.cfg
            try:
                await asyncio.sleep(cfg.status_interval_sec)
            except asyncio.CancelledError:
                raise
            books = " | ".join(
                f"{v.name} {v.book.best_bid() or '—'}/{v.book.best_ask() or '—'}"
                + ("" if v.book.is_fresh(cfg.staleness_sec) else " STALE")
                + (" RATE-LTD" if self._venue_limited(v) else "")
                + (" DOWN" if v.key in self._venue_down else "")
                for v in self.venues.values())
            prem = self.premium_bps()
            prem_s = f"{prem:+.2f}" if prem is not None else "—"
            pos = " ".join(f"{v.name} {v.position:+.6g}"
                           for v in self.venues.values())
            net = sum(v.position for v in self.venues.values())
            pnl = self.session_pnl()
            rec = (f" | rec {self.recorder.rows_written} rows"
                   if self.recorder else "")
            snap = self.model_snapshot
            strategy = self._dashboard_strategy_values(time.time())
            mid = strategy["active_midline_bps"]
            er = getattr(getattr(self.entropy, "funding", None),
                         "rate_bps_per_hour", None)
            hr = getattr(getattr(self.hedge, "funding", None),
                         "rate_bps_per_hour", None)
            log.info("[status] %s | prem %s bps (band %+.2f..%+.2f "
                     "effective %+.2f..%+.2f) | "
                     "state %s mid=%+.2f short=%s long=%s sigma=%s "
                     "lag=%s(%s) speed=%.2f data=%s regime=%s "
                     "cycle=%s drift=%s "
                     "funding=%s/%s bps-h | pos %s "
                     "net %+.6g | trades %d hedges %d | MTM %s expEdge $%.4f "
                     "fillEdge $%.4f%s%s",
                     books, prem_s, mid - cfg.lower_bps, mid + cfg.upper_bps,
                     mid - strategy["effective_lower_bps"],
                     mid + strategy["effective_upper_bps"],
                     self._pair_state().value, mid,
                     snap.short_center_bps, snap.long_center_bps,
                     snap.robust_sigma_bps,
                     snap.midline_lag_bps, snap.midline_lag_state.value,
                     snap.midline_move_speed_bps_per_10m,
                     ("DEGRADED" if strategy["premium_data_degraded"]
                      else ("OK" if strategy["premium_sample_valid"]
                            else "INVALID")),
                     "SHIFT/REDUCE_ONLY" if snap.regime_shift else "NORMAL",
                     ("LAG_BLOCK" if strategy["open_lag_blocked"]
                      else ("VOLATILITY_GUARD" if strategy["volatility_guard"]
                            else ("CYCLE_DRIFT" if strategy["cycle_drift_guard"]
                                  else ("UNANCHORED" if strategy["cycle_add_blocked"]
                                        else "OK")))),
                     strategy["cycle_midline_drift_bps"], er, hr, pos, net,
                     self.trades,
                     self.hedges,
                     f"${pnl:+.4f}" if pnl is not None else "—",
                     self.total_exp_edge, self.total_fill_edge, rec,
                     " *** HALTED ***" if self.halted else "")

    def _write_dashboard_snapshot_now(self) -> None:
        """Publish one best-effort snapshot during shutdown/recovery."""
        if not self.cfg.web_dashboard_enabled:
            return
        try:
            payload = build_dashboard_snapshot(self)
            safe_write_dashboard_snapshot(
                self.cfg.dashboard_status_json, payload)
            attribution = payload.get("attribution")
            if attribution is not None:
                self.ledger.record_pnl_snapshot(
                    ts=payload["ts"],
                    equity_net_pnl_usd=payload["today"].get("net_pnl_usd"),
                    attribution=attribution,
                    equity_total_usd=payload.get("equity", {}).get(
                        "current_equity_usd"))
        except Exception:
            log.exception("dashboard snapshot build failed")

    async def _dashboard_snapshot_loop(self) -> None:
        """Publish independently; snapshot failures can never stop trading."""
        while not self.stop.is_set():
            try:
                self._write_dashboard_snapshot_now()
            except asyncio.CancelledError:
                raise
            try:
                await asyncio.wait_for(
                    self.stop.wait(), timeout=self.cfg.dashboard_snapshot_sec)
            except asyncio.TimeoutError:
                pass

    async def _retro_farming_snapshot_loop(self) -> None:
        """Refresh contribution telemetry off the strategy event loop."""
        while not self.stop.is_set():
            try:
                snapshot = await asyncio.to_thread(
                    build_shadow_retro_snapshot, self.ledger_path)
                snapshot["status"] = "ready"
                self.retro_farming_snapshot = snapshot
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                previous = dict(self.retro_farming_snapshot)
                previous.update({
                    "status": "error",
                    "last_error": f"{type(exc).__name__}: {exc}",
                })
                self.retro_farming_snapshot = previous
                log.warning("retro farming shadow refresh failed: %s", exc)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                pass

    def _log_csv(self, trade: PlannedTrade, ok: bool, bfill, sfill,
                 bstatus, sstatus, fill_edge, inv_bps) -> None:
        try:
            candidate, plan = trade.candidate, trade.plan
            buy, sell = candidate.buy, candidate.sell
            snap = self.model_snapshot
            er = getattr(getattr(self.entropy, "funding", None),
                         "rate_bps_per_hour", None)
            hr = getattr(getattr(self.hedge, "funding", None),
                         "rate_bps_per_hour", None)

            def opt(v):
                return "" if v is None else f"{v:.6f}"

            path = self.cfg.trades_csv
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)
            if os.path.exists(path):
                with open(path) as fh0:
                    if fh0.readline().strip() != ",".join(CSV_HEADER):
                        os.replace(path, path + ".old")
            new = not os.path.exists(path)
            with open(path, "a", newline="") as fh:
                w = csv.writer(fh)
                if new:
                    w.writerow(CSV_HEADER)
                w.writerow([f"{time.time():.3f}",
                            candidate.direction, candidate.action.value,
                            candidate.pair_state_before.value,
                            self._pair_state().value, int(candidate.reduce_only),
                            buy.name, sell.name, f"{plan.qty:.8g}",
                            plan.buy_limit, plan.sell_limit,
                            f"{plan.buy_notional:.2f}", f"{plan.sell_notional:.2f}",
                            f"{plan.exp_edge_usd:.4f}", f"{plan.gross_edge_usd:.4f}",
                            f"{plan.marginal_premium_bps:.3f}",
                            f"{snap.active_midline_bps:.3f}",
                            f"{inv_bps:.3f}", int(ok), f"{bfill:.8g}",
                            f"{sfill:.8g}", bstatus, sstatus, f"{fill_edge:.4f}",
                            f"{snap.active_midline_bps:.6f}",
                            opt(snap.short_center_bps), opt(snap.long_center_bps),
                            opt(snap.robust_sigma_bps), int(snap.regime_shift),
                            f"{candidate.threshold_bps:.6f}",
                            opt(candidate.effective_entry_band_bps),
                            opt(candidate.cycle_entry_midline_bps),
                            candidate.cycle_entry_reference_mode or "",
                            opt(candidate.cycle_entry_active_midline_bps),
                            opt(candidate.cycle_entry_dynamic_center_bps),
                            opt(candidate.cycle_entry_sigma_bps),
                            opt(candidate.cycle_midline_drift_bps),
                            int(candidate.volatility_guard),
                            int(candidate.cycle_drift_guard),
                            opt(er), opt(hr),
                            f"{candidate.funding_cost_bps:.6f}",
                            f"{trade.net_marginal_edge_bps:.6f}",
                            f"{trade.edge_surplus_bps:.6f}",
                            f"{trade.per_leg_slippage_bps:.6f}",
                            opt(snap.midline_lag_bps),
                            str(getattr(snap.midline_lag_state, "value",
                                        snap.midline_lag_state or "normal")),
                            ("" if self.last_premium_sample_quality is None
                             else str(bool(self.last_premium_sample_quality.valid))
                             .lower()),
                            opt(None if self.last_premium_sample_quality is None
                                else self.last_premium_sample_quality
                                .cross_venue_skew_ms)])
        except Exception:
            log.exception("csv write failed")
