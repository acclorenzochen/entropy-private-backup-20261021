export type PairState = "flat" | "short_entropy" | "long_entropy" | "unbalanced";
export type Regime = "normal" | "reduce_only";

export interface ReentryGuardSnapshot {
  enabled: boolean;
  blocked: boolean;
  blocked_directions: string[];
  last_risk_exit_direction: string | null;
  last_risk_exit_ts: number | null;
  last_risk_exit_basis_bps: number | null;
  cooldown_remaining_sec?: number;
  basis_reset_seen: boolean;
  basis_reset_ts: number | null;
  uncertainty_bps?: number;
  statuses: Record<string, Record<string, unknown>>;
}

export interface StatusResponse {
  schema_version: 1;
  ts: number;
  time_utc: string;
  bot_status: "running" | "error" | "stopped";
  bot_started_ts: number;
  strategy_name: string;
  lifecycle: string;
  status_changed_ts?: number | null;
  venues: { entropy: string; hedge: string };
  symbol: string;
  pair_state: PairState;
  regime: Regime;
  open_state?: "allowed" | "soft_restricted" | "hard_blocked";
  positions: {
    entropy_base: number;
    hedge_base: number;
    net_delta_base: number;
    net_delta_usd: number | null;
  };
  equity: EquitySnapshot | null;
  market: {
    entropy_bid: number | null; entropy_ask: number | null;
    hedge_bid: number | null; hedge_ask: number | null;
    entropy_mid: number | null; hedge_mid: number | null;
    premium_mid_bps: number | null; sell_edge_bps: number | null;
    buy_edge_bps: number | null;
  };
  strategy: {
    seed_midline_bps: number; active_midline_bps: number;
    short_center_bps: number | null; long_center_bps: number | null;
    robust_sigma_bps: number | null;
    short_entry_bps: number; short_exit_bps: number;
    long_entry_bps: number; long_exit_bps: number;
    effective_upper_bps: number; effective_lower_bps: number;
    volatility_guard: boolean;
    cycle_active: boolean; cycle_anchored: boolean;
    cycle_direction: string | null;
    cycle_entry_midline_bps: number | null;
    cycle_entry_reference_bps?: number | null;
    cycle_entry_reference_mode?: string | null;
    cycle_entry_active_midline_bps?: number | null;
    cycle_entry_dynamic_center_bps?: number | null;
    cycle_entry_sigma_bps: number | null;
    cycle_entry_band_bps: number | null;
    cycle_midline_drift_bps: number | null;
    cycle_add_blocked: boolean; cycle_drift_guard: boolean;
    dynamic_midline_ready: boolean; regime_shift: boolean;
    sigma_center_bps: number | null; sigma_sample_count: number;
    sigma_window_sec: number | null; midline_lag_bps: number | null;
    midline_lag_state: "normal" | "warning" | "accelerating" | "blocked";
    midline_lag_blocked: boolean;
    open_lag_blocked: boolean;
    entry_reference_midline_bps: number | null;
    entry_reference_mode: string;
    dynamic_center_bps: number | null;
    blocked_entry_deviation_bps: number | null;
    blocked_entry_required_bps: number | null;
    blocked_entry_allowed: boolean;
    warning_entry_penalty_bps: number;
    reference_error_bps: number | null;
    reference_confidence: number;
    reference_state?: "normal" | "warning" | "cold_degraded" | "accelerating" | "shifting" | "blocked";
    reference_valid?: boolean;
    required_sigma_mult?: number;
    required_deviation_bps?: number | null;
    signal_deviation_bps?: number | null;
    extreme_dislocation?: boolean;
    extreme_threshold_bps?: number | null;
    entry_penalty_bps?: number;
    open_upper_bps?: number | null; open_lower_bps?: number | null;
    open_state?: "allowed" | "soft_restricted" | "hard_blocked";
    open_block_reason?: string | null;
    effective_open_surplus_bps: Record<string, number | null>;
    distance_to_threshold_bps?: Record<string, number | null>;
    reentry_guard: ReentryGuardSnapshot;
    midline_move_speed_bps_per_10m: number;
    entropy_book_age_ms: number | null; hedge_book_age_ms: number | null;
    cross_venue_skew_ms: number | null; premium_sample_valid: boolean | null;
    premium_sample_reject_reason: string | null; premium_data_degraded: boolean;
    premium_data_invalid?: boolean;
    premium_data_tier?: "GOOD" | "DEGRADED" | "INVALID" | string;
    premium_data_uncertainty_bps?: number;
    premium_data_invalid_duration_sec?: number;
    dynamic_midline_validation_remaining_sec: number;
    dynamic_midline_warmup_sec: number;
    dynamic_midline_min_samples: number;
    dynamic_midline_tradable?: boolean;
    dynamic_midline_readiness?: "UNAVAILABLE" | "COLD_DEGRADED" | "NORMAL" | string;
    dynamic_midline_history_sec?: number | null;
    dynamic_midline_latest_sample_age_sec?: number | null;
    dynamic_midline_history_source: string;
    dynamic_midline_history_samples: number;
    cycle_allow_add: boolean; cycle_max_active_pairs: number;
    cycle_exit_mode: string;
    cycle_exit_normal_min_profit_usd: number;
    cycle_exit_fast_after_sec: number;
    cycle_exit_fast_min_profit_usd: number;
    cycle_exit_max_hold_sec: number;
    cycle_exit_long_hold_after_sec?: number;
    cycle_exit_hard_timeout_enabled?: boolean;
    cycle_exit_hard_timeout_sec?: number;
    cycle_exit_hard_timeout_min_profit_usd?: number;
    cycle_exit_pnl_loss_stop_enabled?: boolean;
    cycle_exit_adverse_drift_exit_enabled?: boolean;
    cycle_exit_max_cycle_loss_bps: number | null;
    cycle_hold_state?: string | null;
    cycle_hold_state_ts?: number | null;
    margin_state?: "safe" | "warning" | "critical" | "unknown";
    margin_available_ratio?: number | null;
    margin_known?: boolean;
    reference_uncertainty_bps?: number;
    model_uncertainty_bps?: number;
    reentry_uncertainty_bps?: number;
    margin_uncertainty_bps?: number;
    profit_guard: Record<string, unknown>;
  };
  funding: {
    entropy_bps_h: number | null; hedge_bps_h: number | null;
    short_entropy_cost_bps: number; long_entropy_cost_bps: number;
    available: boolean; stale: boolean;
  };
  risk: {
    open_blocked: boolean; open_block_reason: string | null;
    open_block_class?: "hard" | "soft" | null;
    open_state?: "allowed" | "soft_restricted" | "hard_blocked";
    reduce_allowed: boolean; net_tolerance_base: number;
    margin_state?: "safe" | "warning" | "critical" | "unknown";
    margin_open_blocked?: boolean; margin_hard_risk?: boolean;
    margin_available_ratio?: number | null; margin_known?: boolean;
    margin_venues?: Record<string, unknown>;
  };
  today: {
    net_pnl_usd: number | null; realized_pnl_usd: number | null;
    session_pnl_usd: number | null; day_started_ts: number | null;
    session_started_ts: number | null; coverage_complete: boolean;
    equity_updated_ts: number | null;
    day_start_equity_usd: number | null;
    session_start_equity_usd: number | null;
    current_equity_usd: number | null;
    unrealized_pnl_usd: number | null; gross_pnl_usd: number | null;
    fees_usd: number | null; fees_status: "actual" | "mixed" | "estimated" | "none";
    actual_fee_usd: number | null; estimated_fee_usd: number | null;
    actual_fee_fill_count: number; estimated_fee_fill_count: number;
    funding_pnl_usd: number | null;
    turnover_usd: number; trade_count: number;
  };
  last_trade: { ts: number; action: string | null; direction: string | null } | null;
  server_now: number; snapshot_age_sec: number; is_stale: boolean;
  execution_policy: ExecutionPolicy;
  execution: ExecutionSnapshot | null;
  open_execution: OpenExecutionSnapshot;
  reconciliation: ReconciliationSnapshot | null;
  fee_reconciliation: FeeReconciliation | null;
  attribution: Attribution | null;
  retro_farming?: RetroFarmingSnapshot;
  cycle: Record<string, unknown> | null;
  cycle_kpis: CycleKpis;
  execution_costs?: ExecutionCostSnapshot;
  order_budget: Record<string, unknown>;
  open_block_telemetry?: {
    active_reason?: string | null; active_since_ts?: number | null;
    active_duration_sec?: number; last_reason?: string | null;
    last_ts?: number | null; count_by_reason?: Record<string, number>;
    seconds_by_reason?: Record<string, number>; total_blocked_sec?: number;
  };
  config_restart_required: boolean;
  config_restart_required_paths: string[];
}

export interface RetroCanaryPlan {
  mode: "disabled" | "shadow" | "active";
  status: string;
  eligible: boolean;
  should_execute: boolean;
  selected_bucket?: string;
  required_strategy?: string;
  priority?: string[];
  reasons?: string[];
  loss_budget?: Record<string, number>;
}

export interface RetroFarmingSnapshot {
  status?: string;
  assumption: string;
  strict_cycles?: number;
  raw_entropy_volume_usd?: number;
  growth_adjusted_volume_usd?: number;
  entropy_fee_contribution_usd?: number;
  maker_volume_usd?: number;
  oi_usd_hours?: number;
  active_utc_days?: number;
  active_utc_weeks?: number;
  contribution_coverage?: Record<string, number>;
  shadow_canary_plan: RetroCanaryPlan;
}

export interface EquityVenueSnapshot {
  start_equity_usd: number | null;
  current_equity_usd: number | null;
  change_usd: number | null;
}

export interface EquityLifetimeSnapshot {
  start_ts: number | null;
  end_ts: number | null;
  start_equity_usd: number | null;
  current_equity_usd: number | null;
  change_usd: number | null;
  coverage_complete: boolean;
  source: string;
  sample_count: number;
}

export interface EquitySnapshot {
  start_ts: number | null;
  complete: boolean;
  start_equity_usd: number | null;
  current_equity_usd: number | null;
  change_usd: number | null;
  venues: Record<string, EquityVenueSnapshot>;
  lifetime: EquityLifetimeSnapshot | null;
}

export interface ExecutionLeg {
  venue: string; side: "buy" | "sell"; planned_qty: number;
  filled_qty: number; status: string | null; order_ref: string | null;
  limit_px: number | null; avg_px: number | null;
  position_before: number | null; position_now: number | null;
}
export interface ExitCampaign {
  exit_opportunity_id?: string;
  exit_campaign_id?: string;
  exit_class?: string;
  retry_count?: number;
  first_fill_qty?: number;
  duration_ms?: number | null;
  final_result?: string | null;
  quoted_cycle_pnl?: number | null;
  protected_cycle_pnl?: number | null;
  dynamic_hedge_reserve_bps?: number | null;
}
export interface OpenCampaign {
  open_opportunity_id?: string;
  open_opportunity_key?: string;
  open_campaign_id?: string;
  direction?: string;
  retry_count?: number;
  send_count?: number;
  first_fill_qty?: number;
  duration_ms?: number | null;
  final_result?: string | null;
  attempts?: Array<Record<string, unknown>>;
}
export interface ExecutionSnapshot {
  execution_id: string; phase: string; action: string; direction: string;
  planned_qty: number; first: ExecutionLeg; hedge: ExecutionLeg;
  remaining_hedge_qty: number; pending_leg: "first" | "hedge" | null;
  pending_order_ref: string | null; pending_filled_qty: number;
  retry_count: number; first_topup_count: number; last_error: string | null;
  recovery_status: string | null; recovery_started_ts: number | null;
  recovery_reason: string | null; recovery_next_retry_ts: number | null;
  recovery_attempt: number; recovery_backoff_sec: number;
  hedge_reserve_breach: boolean; hedge_reserve_breach_ts: number | null;
  hedge_completion_state: string;
  created_ts: number; updated_ts: number;
  exit_campaign?: ExitCampaign | null;
  open_campaign?: OpenCampaign | null;
}
export interface BootstrapSnapshot {
  configured: boolean; active: boolean; mode: string;
  disabled: boolean; disabled_reason: string | null;
  suspended: boolean; suspended_reason: string | null;
  suspended_ts: number | null; resume_after_ts: number | null;
  resumed_ts: number | null;
  target_complete_cycles: number; max_complete_cycles: number;
  complete_cycles: number;
  min_execution_samples_per_phase: number;
  samples_per_phase: Record<string, number>;
  realized_pnl_usd: number; exploration_pnl_usd: number;
  cumulative_loss_budget_usd: number;
  loss_budget_remaining_usd: number;
  accepted_count: number; green_count: number; yellow_count: number;
  red_count: number; last_color: string | null;
  last_expected_pnl_usd: number | null;
  best_expected_pnl_usd: number | null;
  worst_accepted_expected_pnl_usd: number | null;
  max_order_notional_usd: number | null;
  max_expected_loss_bps: number | null;
  max_expected_loss_usd: number | null;
  max_cycle_loss_usd: number | null;
  min_gross_after_fee_bps: number | null;
  empirical_full_weight_samples: number | null;
  open_execution_room_min_bps: number | null;
  open_target_execution_room_bps: number | null;
  close_min_execution_room_bps: number | null;
  close_target_execution_room_bps: number | null;
  fast_exit_after_sec: number | null;
  fast_exit_min_profit_usd: number | null;
  sampling_exit_after_sec: number | null;
  sampling_exit_floor_usd: number | null;
  stale_exit_after_sec: number | null;
  stale_exit_floor_usd: number | null;
  open_campaign_sec: number | null;
  close_campaign_sec: number | null;
  open_campaign_max_attempts: number | null;
  close_campaign_max_attempts: number | null;
  entropy_order_budget_per_min: number | null;
  hedge_order_budget_per_min: number | null;
  max_campaigns_per_signal_episode: number | null;
  reopen_improvement_bps: number | null;
  complete_cycles_per_hour?: number;
  open_campaigns_per_hour?: number;
  open_fill_rate_per_hour?: number;
  hedge_completion_rate_per_hour?: number;
  close_opportunities_per_hour?: number;
  close_fill_rate_per_hour?: number;
  completion_rate?: number;
  median_hold_sec?: number | null;
  p90_hold_sec?: number | null;
  mean_cycle_pnl_usd?: number | null;
  median_cycle_pnl_usd?: number | null;
  total_bootstrap_pnl_usd?: number | null;
  last_decision_ts: number | null;
}
export interface OpenExecutionSnapshot {
  latency_estimate_sec: number | null;
  adverse_move_economic_p75_bps?: number | null;
  adverse_move_stress_p95_bps?: number | null;
  adverse_move_stress_p99_bps?: number | null;
  adverse_move_p90_bps: number | null;
  telemetry: Record<string, unknown>;
  active_campaign: OpenCampaign | null;
  last_opportunity: Record<string, unknown> | null;
  degraded_until: Record<string, number>;
  bootstrap?: BootstrapSnapshot;
}

export interface CycleKpis {
  record_count: number; completed_count: number; active_count: number;
  clean_circle_count?: number; recovery_circle_count?: number;
  clean_circle_rate?: number; clean_circles_per_hour?: number;
  completion_rate: number; median_cycle_duration_sec: number | null;
  p90_cycle_duration_sec?: number | null;
  p95_unhedged_sec: number | null; normal_win_rate: number | null;
  fast_avg_pnl_usd: number | null; forced_avg_pnl_usd: number | null;
  sampling_avg_pnl_usd?: number | null; stale_avg_pnl_usd?: number | null;
  planned_realized_gap_usd: number | null; partial_first_fill_rate: number;
  recovery_rate: number; emergency_exit_rate: number;
  normal_exit_rate: number; fast_exit_rate: number;
  sampling_exit_rate?: number; stale_exit_rate?: number;
  regime_stop_rate: number; max_hold_exit_rate: number;
  hard_timeout_exit_rate?: number;
  loss_recovery_hold_count?: number; long_hold_count?: number;
  forced_release_pending_count?: number;
  execution_emergency_rate: number; manual_flatten_rate: number;
  exit_opportunity_count?: number; exit_campaign_count?: number;
  first_attempt_fill_rate?: number; campaign_fill_rate?: number;
  median_retries_to_fill?: number | null;
  opportunity_to_fill_ms?: number | null;
  missed_profitable_exit_count?: number;
  order_budget_block_sec: number;
  signal_episode_count?: number; economic_opportunity_count?: number;
  open_campaign_count?: number; open_campaign_fill_rate?: number;
  open_fill_count?: number; hedge_complete_count?: number;
  hedge_completion_rate?: number; exit_campaign_fill_rate?: number;
  complete_cycle_count?: number; exit_fill_count?: number;
  funnel?: FunnelWindow; funnel_windows?: Record<string, FunnelWindow>;
  rolling_20?: RollingCycleMetrics; rolling_50?: RollingCycleMetrics;
  execution_cost_model?: ExecutionCostSnapshot;
  current_cycle_execution_costs?: ExecutionCostCycleSummary;
  bootstrap?: BootstrapSnapshot;
}
export interface FunnelWindow {
  signal_episode_count: number; economic_opportunity_count: number;
  open_campaign_count: number; open_fill_count: number;
  hedge_complete_count: number; complete_cycle_count: number;
  open_campaign_fill_rate: number; hedge_completion_rate: number;
  exit_opportunity_count: number; exit_campaign_count: number;
  exit_fill_count: number; exit_campaign_fill_rate: number;
  completed_exit_count: number;
}
export interface RollingCycleMetrics {
  count: number; mean_pnl_usd: number | null; median_pnl_usd: number | null;
  win_rate: number | null; ev_per_capital_hour_usd: number | null;
  prediction_error_mean_usd: number | null;
}
export interface ExecutionCostPhaseStats {
  phase: string; observation_count: number; filled_count: number;
  fill_rate: number | null; ready: boolean;
  p50_incremental_execution_cost_bps: number | null;
  p75_incremental_execution_cost_bps: number | null;
  p90_incremental_execution_cost_bps: number | null;
  p95_incremental_execution_cost_bps: number | null;
  p50_cost_bps: number | null; p75_cost_bps: number | null;
  p90_cost_bps: number | null; p95_cost_bps: number | null;
  p50_latency_sec: number | null; p75_latency_sec: number | null;
  p90_latency_sec: number | null; p95_latency_sec: number | null;
  p50_realized_slippage_bps: number | null;
  p75_realized_slippage_bps: number | null;
  p90_realized_slippage_bps: number | null;
  p95_realized_slippage_bps: number | null;
  p75_adverse_move_bps: number | null;
  p90_adverse_move_bps: number | null;
  p95_adverse_move_bps: number | null;
  p50_adverse_move_bps: number | null; p99_adverse_move_bps: number | null;
  p95_depth_cost_bps: number | null;
  empirical_ready: boolean; usable_samples: number; source: string;
  empirical_weight: number; expected_cost_bps: number | null;
  empirical_cost_bps: number | null; prior_cost_bps: number | null;
  p50_all_in_cost_bps: number | null; p75_all_in_cost_bps: number | null;
  p90_all_in_cost_bps: number | null; p95_all_in_cost_bps: number | null;
  p50_all_in_execution_cost_bps: number | null; p75_all_in_execution_cost_bps: number | null;
  p90_all_in_execution_cost_bps: number | null; p95_all_in_execution_cost_bps: number | null;
  p50_latency_adverse_move_bps: number | null; p75_latency_adverse_move_bps: number | null;
  p90_latency_adverse_move_bps: number | null; p95_latency_adverse_move_bps: number | null;
  p99_latency_adverse_move_bps: number | null;
  p50_realized_vs_marginal_bps: number | null;
  p75_realized_vs_marginal_bps: number | null;
  p90_realized_vs_marginal_bps: number | null;
  p95_realized_vs_marginal_bps: number | null;
}
export interface ExecutionCostSnapshot {
  ready: boolean; observation_count: number;
  max_observations_per_phase: number;
  phases: Record<string, ExecutionCostPhaseStats>;
}
export interface ExecutionCostCycleSummary {
  phases: Record<string, Record<string, number | null>>;
  realized_cost_usd: number; all_in_cost_usd?: number; total_cost_usd?: number;
  observation_count: number;
}
export interface ExecutionPolicy {
  open_persist_sec: number; reduce_persist_sec: number;
  open_cooldown_sec: number; reduce_cooldown_sec: number;
  open_first_leg_slippage_bps: number;
  reduce_first_leg_slippage_bps: number;
  open_second_leg_slippage_bps: number;
  reduce_second_leg_slippage_bps: number;
  open_no_fill_retry_sec: number; reduce_no_fill_retry_sec: number;
  emergency_slippage_bps: number; max_reduce_retry_attempts: number;
  reduce_retry_escalation_bps: number[];
  exit_campaign_sec?: number;
  exit_campaign_retry_interval_sec?: number;
  dynamic_hedge_reserve?: {
    enabled: boolean; min_bps: number; max_bps: number;
    adverse_move_percentile: number; safety_margin_bps: number;
  };
  dynamic_open_first_leg?: {
    enabled: boolean; min_bps: number; max_bps: number;
    adverse_move_percentile: number; safety_margin_bps: number;
  };
  open_retry_campaign_sec?: number;
  open_retry_interval_sec?: number;
  open_retry_max_attempts?: number;
  open_execution_room_min_bps?: number;
  reserve_reduce_slots?: number;
  open_first_leg_hard_cap_bps?: number; open_second_leg_hard_cap_bps?: number;
  exit_campaign_max_attempts?: number; reopen_improvement_bps?: number;
  max_campaigns_per_signal_episode?: number;
  economic_execution?: {
    min_expected_cycle_profit_usd: number;
    open_first_cost_percentile: number; open_hedge_cost_percentile: number;
    close_first_cost_percentile: number; close_hedge_cost_percentile: number;
    safety_margin_bps: number;
  };
  bootstrap?: BootstrapSnapshot;
}
export interface ReconciliationVenue {
  local_base: number | null; exchange_base: number | null;
  delta_base: number | null; mark_px: number | null; status: string | null;
  ts: number | null;
}
export interface ReconciliationSnapshot {
  status: string; last_ts: number | null; startup_reconciled: boolean;
  net_delta_base: number | null; last_error: string | null;
  venues: Record<string, ReconciliationVenue>;
}
export interface FeeReconciliation {
  status: string; initial_sync_done: boolean;
  last_ts: number | null; last_success_ts: number | null;
  last_error: string | null; fetched_count: number; stored_count: number;
  actual_fill_count: number; unmatched_count: number;
  actual_fee_usd: number; fee_delta_usd: number; cash_adjustment_usd: number;
}
export interface AttributionWindow {
  equity_net_pnl_usd: number | null; ledger_net_pnl_usd: number;
  gross_realized_usd: number; unrealized_usd: number; fees_usd: number;
  fees_status: "actual" | "mixed" | "estimated" | "none";
  actual_fee_usd: number; estimated_fee_usd: number;
  actual_fee_fill_count: number; estimated_fee_fill_count: number;
  funding_pnl_usd: number; unexplained_usd: number | null;
  legacy_unattributed_edge_usd: number; fill_count: number;
  coverage_start_ts: number | null; confidence: string;
}
export interface Attribution {
  as_of_ts: number; source: string; day: AttributionWindow;
  session: AttributionWindow; lifetime: AttributionWindow | null;
}

export interface HistoryPoint {
  ts: number; premium_bps: number; active_midline_bps: number | null;
  short_entry_bps: number | null; short_exit_bps: number | null;
  long_entry_bps: number | null; long_exit_bps: number | null;
  short_center_bps?: number | null;
}
export interface HistoryResponse { range: "1h" | "6h" | "24h"; points: HistoryPoint[] }

export interface Trade {
  ts: number; action: string; direction: string; notional_usd: number;
  qty: number; threshold_bps: number | null; net_edge_bps: number | null;
  fill_edge_bps: number | null; fill_edge_usd: number | null;
  fees_usd: number | null; fees_status: "actual" | "mixed" | "estimated" | "pending" | "none";
  outcome: "paired" | "partial_paired" | "one_leg" | "no_fill";
  buy_venue: string; sell_venue: string;
  entropy_leg: TradeLeg; lighter_leg: TradeLeg; reduce_only: boolean;
  gross_realized_pnl_usd?: number | null;
  net_realized_pnl_usd?: number | null;
  pnl_status?: string;
  execution_id?: string | null; source?: string | null;
}
export interface TradeLeg {
  side: "buy" | "sell" | "unknown"; planned_qty: number;
  fill_qty: number; status: string; fee_usd?: number | null;
  fee_status?: "actual" | "mixed" | "estimated" | "pending" | "none";
  gross_realized_pnl_usd?: number | null;
  net_realized_pnl_usd?: number | null;
  pnl_status?: string;
}
export interface TradesSummary {
  coverage_started_ts: number | null; attempt_count: number;
  matched_count: number; paired_count: number; partial_paired_count: number;
  one_leg_count: number; no_fill_count: number; matched_rate: number;
  paired_fill_edge_usd: number;
}
export interface TradesResponse {
  summary: TradesSummary; trades: Trade[]; total_count: number;
  offset: number; limit: number; has_more: boolean;
}

export interface PnlResponse {
  range: "today";
  summary: { net_pnl_usd: number | null; gross_pnl_usd: number | null;
    fees_usd: number | null; fees_status?: string;
    actual_fee_usd?: number | null; estimated_fee_usd?: number | null;
    funding_pnl_usd: number | null;
    realized_pnl_usd?: number | null; unrealized_pnl_usd?: number | null;
    ledger_net_pnl_usd?: number | null; unexplained_usd?: number | null };
  points: { ts: number; net_pnl_usd: number }[];
  lifetime_points: { ts: number; net_pnl_usd: number }[];
}

export interface DashboardResponse {
  as_of_ts: number; status: StatusResponse; history: HistoryResponse;
  pnl: PnlResponse; trades: TradesResponse;
}
