from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Venues(StrictModel):
    entropy: str
    hedge: str


class Positions(StrictModel):
    entropy_base: float
    hedge_base: float
    net_delta_base: float
    net_delta_usd: Optional[float]


class EquityVenue(StrictModel):
    start_equity_usd: Optional[float] = None
    current_equity_usd: Optional[float] = None
    change_usd: Optional[float] = None


class EquityLifetime(StrictModel):
    start_ts: Optional[float] = None
    end_ts: Optional[float] = None
    start_equity_usd: Optional[float] = None
    current_equity_usd: Optional[float] = None
    change_usd: Optional[float] = None
    coverage_complete: bool = False
    source: str = "unavailable"
    sample_count: int = 0


class EquitySnapshot(StrictModel):
    start_ts: Optional[float] = None
    complete: bool = False
    start_equity_usd: Optional[float] = None
    current_equity_usd: Optional[float] = None
    change_usd: Optional[float] = None
    venues: dict[str, EquityVenue]
    lifetime: Optional[EquityLifetime] = None


class Market(StrictModel):
    entropy_bid: Optional[float]
    entropy_ask: Optional[float]
    hedge_bid: Optional[float]
    hedge_ask: Optional[float]
    entropy_mid: Optional[float]
    hedge_mid: Optional[float]
    premium_mid_bps: Optional[float]
    sell_edge_bps: Optional[float]
    buy_edge_bps: Optional[float]


class Strategy(StrictModel):
    profit_guard: dict = Field(default_factory=dict)
    seed_midline_bps: float
    active_midline_bps: float
    short_center_bps: Optional[float]
    long_center_bps: Optional[float]
    robust_sigma_bps: Optional[float]
    short_entry_bps: float
    short_exit_bps: float
    long_entry_bps: float
    long_exit_bps: float
    effective_upper_bps: float
    effective_lower_bps: float
    volatility_guard: bool
    cycle_active: bool
    cycle_anchored: bool
    cycle_direction: Optional[str]
    cycle_entry_midline_bps: Optional[float]
    cycle_entry_reference_bps: Optional[float] = None
    cycle_entry_reference_mode: Optional[str] = None
    cycle_entry_active_midline_bps: Optional[float] = None
    cycle_entry_dynamic_center_bps: Optional[float] = None
    cycle_entry_sigma_bps: Optional[float]
    cycle_entry_band_bps: Optional[float]
    cycle_midline_drift_bps: Optional[float]
    cycle_add_blocked: bool
    cycle_drift_guard: bool
    dynamic_midline_ready: bool
    regime_shift: bool
    sigma_center_bps: Optional[float] = None
    sigma_sample_count: int = 0
    sigma_window_sec: Optional[float] = None
    midline_lag_bps: Optional[float] = None
    midline_lag_state: str = "normal"
    midline_lag_blocked: bool = False
    open_lag_blocked: bool = False
    entry_reference_midline_bps: Optional[float] = None
    entry_reference_mode: str = "active_midline"
    dynamic_center_bps: Optional[float] = None
    blocked_entry_deviation_bps: Optional[float] = None
    blocked_entry_required_bps: Optional[float] = None
    blocked_entry_allowed: bool = True
    midline_move_speed_bps_per_10m: float = 0.0
    entropy_book_age_ms: Optional[float] = None
    hedge_book_age_ms: Optional[float] = None
    cross_venue_skew_ms: Optional[float] = None
    premium_sample_valid: Optional[bool] = None
    premium_sample_reject_reason: Optional[str] = None
    premium_data_degraded: bool = False
    premium_data_invalid: bool = False
    premium_data_tier: str = "GOOD"
    premium_data_uncertainty_bps: float = 0.0
    premium_data_invalid_duration_sec: float = 0.0
    dynamic_midline_validation_remaining_sec: float = 0.0
    dynamic_midline_warmup_sec: float = 1800.0
    dynamic_midline_min_samples: int = 600
    dynamic_midline_tradable: bool = False
    dynamic_midline_readiness: str = "UNAVAILABLE"
    dynamic_midline_history_sec: Optional[float] = None
    dynamic_midline_latest_sample_age_sec: Optional[float] = None
    dynamic_midline_history_source: str = "unknown"
    dynamic_midline_history_samples: int = 0
    cycle_allow_add: bool = True
    cycle_max_active_pairs: int = 1
    cycle_exit_mode: str = "entry_midline"
    cycle_exit_normal_min_profit_usd: float = 0.0
    cycle_exit_fast_after_sec: float = 120.0
    cycle_exit_fast_min_profit_usd: float = -0.01
    cycle_exit_max_hold_sec: float = 600.0
    cycle_exit_long_hold_after_sec: float = 600.0
    cycle_exit_hard_timeout_enabled: bool = True
    cycle_exit_hard_timeout_sec: float = 3600.0
    cycle_exit_hard_timeout_min_profit_usd: float = -0.02
    cycle_exit_pnl_loss_stop_enabled: bool = False
    cycle_exit_adverse_drift_exit_enabled: bool = False
    cycle_exit_max_cycle_loss_bps: Optional[float] = None
    cycle_hold_state: Optional[str] = None
    cycle_hold_state_ts: Optional[float] = None
    margin_state: str = "unknown"
    margin_available_ratio: Optional[float] = None
    margin_known: bool = False
    reference_uncertainty_bps: float = 0.0
    model_uncertainty_bps: float = 0.0
    reentry_uncertainty_bps: float = 0.0
    margin_uncertainty_bps: float = 0.0
    warning_entry_penalty_bps: float = 0.0
    reference_error_bps: Optional[float] = None
    reference_confidence: float = 1.0
    reference_state: str = "normal"
    reference_valid: bool = True
    required_sigma_mult: float = 0.0
    required_deviation_bps: Optional[float] = None
    signal_deviation_bps: Optional[float] = None
    extreme_dislocation: bool = False
    extreme_threshold_bps: Optional[float] = None
    entry_penalty_bps: float = 0.0
    open_upper_bps: Optional[float] = None
    open_lower_bps: Optional[float] = None
    open_state: str = "allowed"
    open_block_reason: Optional[str] = None
    effective_open_surplus_bps: dict = Field(default_factory=dict)
    distance_to_threshold_bps: dict[str, Optional[float]] = Field(
        default_factory=dict)
    reentry_guard: dict = Field(default_factory=dict)


class Funding(StrictModel):
    entropy_bps_h: Optional[float]
    hedge_bps_h: Optional[float]
    short_entropy_cost_bps: float
    long_entropy_cost_bps: float
    available: bool
    stale: bool


class Risk(StrictModel):
    open_blocked: bool
    open_block_reason: Optional[str]
    open_block_class: Optional[str] = None
    open_state: str = "allowed"
    reduce_allowed: bool
    net_tolerance_base: float
    margin_state: str = "unknown"
    margin_open_blocked: bool = False
    margin_hard_risk: bool = False
    margin_available_ratio: Optional[float] = None
    margin_known: bool = False
    margin_venues: dict = Field(default_factory=dict)


class Today(StrictModel):
    net_pnl_usd: Optional[float]
    session_pnl_usd: Optional[float]
    day_start_equity_usd: Optional[float] = None
    session_start_equity_usd: Optional[float] = None
    current_equity_usd: Optional[float] = None
    day_started_ts: Optional[float]
    session_started_ts: Optional[float]
    coverage_complete: bool
    equity_updated_ts: Optional[float]
    # Defaults keep the API readable during a rolling upgrade when the
    # dashboard process sees a snapshot written by the previous bot binary.
    realized_pnl_usd: Optional[float] = None
    unrealized_pnl_usd: Optional[float] = None
    gross_pnl_usd: Optional[float] = None
    fees_usd: Optional[float] = None
    fees_status: str = "none"
    actual_fee_usd: Optional[float] = None
    estimated_fee_usd: Optional[float] = None
    actual_fee_fill_count: int = 0
    estimated_fee_fill_count: int = 0
    funding_pnl_usd: Optional[float] = None
    turnover_usd: float
    trade_count: int


class ExecutionLeg(StrictModel):
    venue: str
    side: Literal["buy", "sell"]
    planned_qty: float
    filled_qty: float
    status: Optional[str] = None
    order_ref: Optional[str] = None
    limit_px: Optional[float] = None
    avg_px: Optional[float] = None
    position_before: Optional[float] = None
    position_now: Optional[float] = None


class ExecutionSnapshot(StrictModel):
    execution_id: str
    phase: str
    action: str
    direction: str
    planned_qty: float
    first: ExecutionLeg
    hedge: ExecutionLeg
    remaining_hedge_qty: float
    pending_leg: Optional[Literal["first", "hedge"]] = None
    pending_order_ref: Optional[str] = None
    pending_filled_qty: float
    retry_count: int
    first_topup_count: int = 0
    recovery_status: Optional[str] = None
    recovery_started_ts: Optional[float] = None
    recovery_reason: Optional[str] = None
    recovery_next_retry_ts: Optional[float] = None
    recovery_attempt: int = 0
    recovery_backoff_sec: float = 0.0
    hedge_reserve_breach: bool = False
    hedge_reserve_breach_ts: Optional[float] = None
    hedge_completion_state: str = "not_started"
    last_error: Optional[str] = None
    created_ts: float
    updated_ts: float
    exit_campaign: Optional[dict] = None
    open_campaign: Optional[dict] = None


class ReconciliationVenue(StrictModel):
    local_base: Optional[float] = None
    exchange_base: Optional[float] = None
    delta_base: Optional[float] = None
    mark_px: Optional[float] = None
    status: Optional[str] = None
    ts: Optional[float] = None


class ReconciliationSnapshot(StrictModel):
    status: str
    last_ts: Optional[float] = None
    startup_reconciled: bool
    net_delta_base: Optional[float] = None
    last_error: Optional[str] = None
    venues: dict[str, ReconciliationVenue]


class FeeReconciliation(StrictModel):
    status: str = "unknown"
    initial_sync_done: bool = False
    last_ts: Optional[float] = None
    last_success_ts: Optional[float] = None
    last_error: Optional[str] = None
    fetched_count: int = 0
    stored_count: int = 0
    actual_fill_count: int = 0
    unmatched_count: int = 0
    actual_fee_usd: float = 0.0
    fee_delta_usd: float = 0.0
    cash_adjustment_usd: float = 0.0


class ExecutionCostPhase(StrictModel):
    phase: str
    observation_count: int = 0
    filled_count: int = 0
    fill_rate: Optional[float] = None
    ready: bool = False
    # Economic admission uses only the incremental fill-vs-depth-average
    # cost. The legacy pXX_cost fields remain for old clients.
    p50_incremental_execution_cost_bps: Optional[float] = None
    p75_incremental_execution_cost_bps: Optional[float] = None
    p90_incremental_execution_cost_bps: Optional[float] = None
    p95_incremental_execution_cost_bps: Optional[float] = None
    p50_cost_bps: Optional[float] = None
    p75_cost_bps: Optional[float] = None
    p90_cost_bps: Optional[float] = None
    p95_cost_bps: Optional[float] = None
    p50_latency_sec: Optional[float] = None
    p75_latency_sec: Optional[float] = None
    p90_latency_sec: Optional[float] = None
    p95_latency_sec: Optional[float] = None
    p50_realized_slippage_bps: Optional[float] = None
    p75_realized_slippage_bps: Optional[float] = None
    p90_realized_slippage_bps: Optional[float] = None
    p95_realized_slippage_bps: Optional[float] = None
    p75_adverse_move_bps: Optional[float] = None
    p90_adverse_move_bps: Optional[float] = None
    p95_adverse_move_bps: Optional[float] = None
    p50_adverse_move_bps: Optional[float] = None
    p99_adverse_move_bps: Optional[float] = None
    p95_depth_cost_bps: Optional[float] = None
    # All-in top-to-average cost, depth cost, and adverse movement are
    # attribution/stress diagnostics; they are not charged again in economics.
    empirical_ready: bool = False
    usable_samples: int = 0
    source: str = "cold_start_prior"
    empirical_weight: float = 0.0
    empirical_full_weight_samples: int = 10
    expected_cost_bps: Optional[float] = None
    empirical_cost_bps: Optional[float] = None
    prior_cost_bps: Optional[float] = None
    p50_all_in_cost_bps: Optional[float] = None
    p75_all_in_cost_bps: Optional[float] = None
    p90_all_in_cost_bps: Optional[float] = None
    p95_all_in_cost_bps: Optional[float] = None
    p50_all_in_execution_cost_bps: Optional[float] = None
    p75_all_in_execution_cost_bps: Optional[float] = None
    p90_all_in_execution_cost_bps: Optional[float] = None
    p95_all_in_execution_cost_bps: Optional[float] = None
    p50_latency_adverse_move_bps: Optional[float] = None
    p75_latency_adverse_move_bps: Optional[float] = None
    p90_latency_adverse_move_bps: Optional[float] = None
    p95_latency_adverse_move_bps: Optional[float] = None
    p99_latency_adverse_move_bps: Optional[float] = None
    p50_realized_vs_marginal_bps: Optional[float] = None
    p75_realized_vs_marginal_bps: Optional[float] = None
    p90_realized_vs_marginal_bps: Optional[float] = None
    p95_realized_vs_marginal_bps: Optional[float] = None


class ExecutionCostSnapshot(StrictModel):
    ready: bool = False
    observation_count: int = 0
    max_observations_per_phase: int = 200
    phases: dict[str, ExecutionCostPhase] = Field(default_factory=dict)


class AttributionWindow(StrictModel):
    equity_net_pnl_usd: Optional[float] = None
    ledger_net_pnl_usd: float
    gross_realized_usd: float
    unrealized_usd: float
    fees_usd: float
    fees_status: str = "none"
    actual_fee_usd: float = 0.0
    estimated_fee_usd: float = 0.0
    actual_fee_fill_count: int = 0
    estimated_fee_fill_count: int = 0
    funding_pnl_usd: float
    unexplained_usd: Optional[float] = None
    legacy_unattributed_edge_usd: float
    fill_count: int
    coverage_start_ts: Optional[float] = None
    confidence: str


class Attribution(StrictModel):
    as_of_ts: float
    source: str
    day: AttributionWindow
    session: AttributionWindow
    lifetime: Optional[AttributionWindow] = None


class LastTrade(StrictModel):
    ts: float
    action: Optional[str]
    direction: Optional[str]


class StatusResponse(StrictModel):
    schema_version: Literal[1]
    ts: float
    time_utc: str
    bot_status: Literal["running", "error", "stopped"]
    bot_started_ts: float
    strategy_name: str = "legacy"
    lifecycle: str = "flat"
    status_changed_ts: Optional[float] = None
    venues: Venues
    symbol: str
    pair_state: Literal["flat", "short_entropy", "long_entropy", "unbalanced"]
    regime: Literal["normal", "reduce_only"]
    positions: Positions
    inventory_flatness: dict = Field(default_factory=dict)
    equity: Optional[EquitySnapshot] = None
    market: Market
    strategy: Strategy
    open_state: str = "allowed"
    funding: Funding
    risk: Risk
    today: Today
    last_trade: Optional[LastTrade]
    server_now: float
    snapshot_age_sec: float
    is_stale: bool
    execution_policy: dict = Field(default_factory=dict)
    execution: Optional[ExecutionSnapshot] = None
    open_execution: dict = Field(default_factory=dict)
    close_execution: dict = Field(default_factory=dict)
    reconciliation: Optional[ReconciliationSnapshot] = None
    fee_reconciliation: Optional[FeeReconciliation] = None
    attribution: Optional[Attribution] = None
    retro_farming: dict = Field(default_factory=dict)
    execution_costs: Optional[ExecutionCostSnapshot] = None
    cycle: Optional[dict] = None
    cycle_kpis: dict = Field(default_factory=dict)
    order_budget: dict = Field(default_factory=dict)
    open_block_telemetry: dict = Field(default_factory=dict)
    config_restart_required: bool = False
    config_restart_required_paths: list[str] = Field(default_factory=list)


class HistoryPoint(StrictModel):
    ts: float
    premium_bps: float
    active_midline_bps: Optional[float]
    short_entry_bps: Optional[float]
    short_exit_bps: Optional[float]
    long_entry_bps: Optional[float]
    long_exit_bps: Optional[float]
    short_center_bps: Optional[float] = None


class HistoryResponse(StrictModel):
    range: Literal["1h", "6h", "24h"]
    points: list[HistoryPoint]


class Trade(StrictModel):
    ts: float
    action: str
    direction: str
    notional_usd: float
    qty: float
    threshold_bps: Optional[float]
    net_edge_bps: Optional[float]
    fill_edge_bps: Optional[float]
    fill_edge_usd: Optional[float]
    outcome: Literal["paired", "partial_paired", "one_leg", "no_fill"]
    buy_venue: str
    sell_venue: str
    entropy_leg: "TradeLeg"
    lighter_leg: "TradeLeg"
    reduce_only: bool
    execution_id: Optional[str] = None
    source: Optional[str] = None
    fees_usd: Optional[float] = None
    fees_status: str = "pending"
    gross_realized_pnl_usd: Optional[float] = None
    net_realized_pnl_usd: Optional[float] = None
    pnl_status: str = "unavailable"


class TradeLeg(StrictModel):
    side: Literal["buy", "sell", "unknown"]
    planned_qty: float
    fill_qty: float
    status: str
    fee_usd: Optional[float] = None
    fee_status: str = "pending"
    gross_realized_pnl_usd: Optional[float] = None
    net_realized_pnl_usd: Optional[float] = None
    pnl_status: str = "unavailable"


class TradesSummary(StrictModel):
    coverage_started_ts: Optional[float]
    attempt_count: int
    matched_count: int
    paired_count: int
    partial_paired_count: int
    one_leg_count: int
    no_fill_count: int
    matched_rate: float
    paired_fill_edge_usd: float


class TradesResponse(StrictModel):
    summary: TradesSummary
    trades: list[Trade]
    total_count: int = 0
    offset: int = 0
    limit: int = 20
    has_more: bool = False


class PnlSummary(StrictModel):
    net_pnl_usd: Optional[float]
    gross_pnl_usd: Optional[float]
    fees_usd: Optional[float]
    fees_status: str = "none"
    actual_fee_usd: Optional[float] = None
    estimated_fee_usd: Optional[float] = None
    funding_pnl_usd: Optional[float]
    realized_pnl_usd: Optional[float] = None
    unrealized_pnl_usd: Optional[float] = None
    ledger_net_pnl_usd: Optional[float] = None
    unexplained_usd: Optional[float] = None


class PnlPoint(StrictModel):
    ts: float
    net_pnl_usd: float


class PnlResponse(StrictModel):
    range: Literal["today"]
    summary: PnlSummary
    points: list[PnlPoint]
    lifetime_points: list[PnlPoint] = Field(default_factory=list)


class DashboardResponse(StrictModel):
    as_of_ts: float
    status: StatusResponse
    history: HistoryResponse
    pnl: PnlResponse
    trades: TradesResponse
