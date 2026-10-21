export type Quality = "good" | "valid" | "partial" | "stale" | "invalid" | "unavailable" | "unknown" | string;
export type GateStatus = "pass" | "fail" | "not_evaluated" | "unknown" | "not_applicable";
export type Decimal = string | null;

export interface SourceInfo {
  observed_at?: number | null;
  read_at?: number | null;
  age_s?: number | null;
  schema?: string | number | null;
  session?: string | null;
  quality?: Quality | null;
  reason?: string | null;
  build_hash?: string | null;
  config_hash?: string | null;
  pair?: string | null;
  market_change_at?: number | null;
  stream_alive_at?: number | null;
  captured_at?: number | null;
  gaps?: number | null;
}

export interface V3Meta {
  schema_version: number;
  snapshot_id: string;
  generated_at: number;
  window?: { scope: string; from_ts: number | null; to_ts: number | null; timezone: string; cohort_basis?: string | null; batch_id?: string | null; session?: string | null };
  deployment_windows?: Record<string, NonNullable<V3Meta["window"]>>;
  sources?: Record<string, SourceInfo>;
  consistency?: "consistent" | "transitioning" | "partial" | "unknown";
  warnings?: string[];
}
export interface Envelope<T> { meta: V3Meta; data: T }

export interface Alert {
  id: string; category?: string | null; severity?: string | null; message: string;
  source?: string | null; observed_at?: number | null; quality?: Quality | null;
}
export interface EvidenceValue {
  units?: Decimal; base_quantity?: Decimal; source?: string | null;
  observed_at?: number | null; reason?: string | null;
}
export interface PositionSide { authoritative?: EvidenceValue | null; expected?: EvidenceValue | null; journal?: EvidenceValue | null }
export interface EntryDirection {
  side: string; q_units?: Decimal; buy_or_sell_basis_bps?: Decimal; edge_bps?: Decimal;
  band_bps?: Decimal; scenario_bps?: Decimal; floor_bps?: Decimal; reject_stage?: string | null;
  gates?: { name: string; status: GateStatus; reason?: string | null }[];
}
export interface EntryDiagnostic {
  source: "live"; evaluated_at?: number | null; summary_reason?: string | null;
  directions?: EntryDirection[];
}
export interface ExitDiagnostic {
  source: "live"; evaluated_at?: number | null; anchor_bps?: Decimal;
  anchor_source?: string | null; executable_net_usd?: Decimal; reversion_bps?: Decimal;
  executable_gross_usd?: Decimal; normal_exit_min_gross_profit_usd?: Decimal;
  normal_exit_rule?: GateStatus | boolean | null;
  last_attempt?: { at?: number | null; order_ref?: string | null; state?: string | null; filled_units?: Decimal; reason?: string | null } | null;
}
export interface FundingState {
  cycle_id?: string | null; hedged_ts?: number | null;
  settlement_counts?: { entropy?: number | null; hedge?: number | null };
  funding_by_venue?: { entropy?: Decimal; hedge?: Decimal }; actual_funding_usd?: Decimal;
  negative_sample_count?: number | null; negative_since_ts?: number | null;
  last_sample_ts?: number | null; last_net_usd_per_hour?: Decimal;
  last_complete_poll_ts?: number | null; completed?: boolean | number | null;
  exit_reason?: string | null; trade_accounting_complete?: boolean | number | null;
  closed_poll_complete?: boolean | number | null;
  settlements?: { venue: string; event_id: string; ts: number; amount_usd: Decimal; source: string }[];
}
export interface FundingDashboard {
  strategy_mode: "funding_hedge"; pair_identity?: string | null; live_quality: Quality;
  policy: { configured?: boolean; max_notional_usd?: Decimal;
    funding_settlements_required?: number | null; funding_stale_seconds?: number | null;
    funding_reversal_samples?: number | null; funding_reversal_seconds?: number | null;
    funding_remaining_cost_limit_usd?: Decimal; funding_round_loss_limit_usd?: Decimal;
    funding_liquidation_distance?: Decimal; funding_risk_stale_seconds?: number | null };
  configured_leverage?: Record<string, number> | null; applied_leverage?: Record<string, number> | null;
  common_quantity_step?: Decimal; accounting_quantity_step?: Decimal;
  ledger_quantities?: Record<string, Decimal>;
  cycle?: { cycle_id: string; phase?: string; direction?: string; started_ts?: number } | null;
  state?: FundingState | null; state_source?: string | null;
  net_usd_per_hour?: Decimal; rate_quality?: Quality; expected_remaining_cost_usd?: Decimal;
  rates?: Record<string, { rate_bps_per_hour?: Decimal; index_price?: Decimal;
    source?: string | null; source_ts?: number | null; quality: Quality }>;
  risk?: Record<string, { position_qty?: Decimal; mark_price?: Decimal; liquidation_price?: Decimal;
    distance?: Decimal; source?: string | null; source_ts?: number | null; quality: Quality }>;
  batch?: { batch_id?: string | null; position_cycles?: number | null;
    max_position_cycles?: number | "disabled" | null; closed_for_entries?: boolean | number | null;
    close_reason?: string | null; loss_used_usd?: Decimal; loss_budget_usd?: Decimal };
  public_preflight?: { ok?: boolean | null; source_ts?: number | null; error?: string | null;
    account_verified?: boolean | null; settlements_verified?: boolean | null;
    accounting_delay_verified?: boolean | null } | null;
}
export interface CycleSummary {
  source_id?: "basis" | "funding"; symbol?: string; strategy_mode?: string;
  first_fill_at?: number | null; flat_confirmed_at?: number | null; duration_state?: "closed" | "active" | "unknown";
  first_fill_confirmed_at?: number | null; estimated_hold_seconds?: number | null; confirmation_duration_state?: "closed" | "active" | "unknown";
  first_fill_missing_order_refs?: string[];
  cycle_id: string; started_at?: number | null; paired_at?: number | null; completed_at?: number | null;
  side?: string | null; planned_units?: Decimal; actual_units?: Decimal; phase?: string | null;
  result_kind?: string | null; hold_seconds?: number | null; open_zero_fill_count?: number | null;
  close_zero_fill_count?: number | null; finalized_net_usd?: Decimal;
  accounting_state?: string | null; quality?: Quality | null; had_unknown?: boolean | null;
  inherited?: boolean | null; entropy_units?: Decimal; hedge_units?: Decimal;
  executable_net_usd?: Decimal; has_confirmed_fill?: boolean;
  paired_opened?: boolean; paired_round_trip?: boolean;
  largest_negative_component?: string | null;
  funding_state?: FundingState | null;
}
export interface Attribution {
  A?: Decimal; B?: Decimal; C?: Decimal; D?: Decimal;
  a_to_b_bps?: Decimal; b_to_c_bps?: Decimal; c_to_d_bps?: Decimal;
  a_to_b_usd?: Decimal; b_to_c_usd?: Decimal; c_to_d_usd?: Decimal;
  fees_usd?: Decimal; funding_usd?: Decimal; reason?: string | null;
}
export interface Attempt {
  order_ref: string; venue?: string | null; side?: string | null; reduce_only?: boolean | 0 | 1 | null;
  requested_units?: Decimal; filled_units?: Decimal; remaining_units?: Decimal;
  limit_px?: Decimal; avg_fill_px?: Decimal; room_bps?: Decimal; cap_bps?: Decimal;
  state?: string | null; had_unknown?: boolean | null; submitted_at?: number | null;
  terminal_at?: number | null; confirmation_source?: string | null;
  updated_at?: number | null; reason?: string | null;
}
export interface ExecutionGroup {
  group_id: string; cycle_id?: string | null; purpose?: string | null;
  side?: string | null; matched_units?: Decimal; state?: string | null;
  quality?: Quality | null; attribution?: Attribution | null; attempts?: Attempt[];
}
export interface Deployment {
  source_id: "basis" | "funding"; mode: "basis" | "funding"; label: string; symbols: string[]; pair_identity?: string | null;
  generated_at: number; snapshot_id: string; status: NonNullable<OverviewData["live"]>; source_health: Record<string, SourceInfo>;
  batches: { batch_id: string }[]; batch?: SystemData["batch"]; current_session_attempts?: number; current_session_complete?: number; current_session_zero_fill?: number;
  overview?: OverviewData | null; window?: V3Meta["window"]; reason?: string;
  manual_exit?: ManualExit | null;
}
export interface ManualExit {
  cycle_id?: string; batch_id?: string; state: "complete" | "accounting_pending" | "unknown" | "partial" | "executing";
  source_ts?: number | null; position_observed_ts?: number | null;
  remaining?: { entropy_units?: number | null; hedge_units?: number | null; authoritative?: boolean };
  pending?: { order_ref: string; venue: string; state: string; reduce_only: boolean; filled_units?: number | null }[];
  legs?: { venue: string; initial_units?: number | null; closed_units?: number | null; remaining_units?: number | null; state: string }[];
  accounting_final?: boolean; net_pnl_usd?: string | null; supersedes_live?: boolean; reason?: string | null;
}
export interface OverviewData {
  deployments?: Deployment[];
  profit?: ProfitOverview | null;
  funding?: FundingDashboard | null;
  kpis?: {
    finalized_net_pnl_usd?: Decimal; pending_accounting_cycles?: number | null;
    paired_round_trips?: number | null; paired_opened_cycles?: number | null;
    losing_cycles?: number | null;
    open_campaigns_submitted?: number | null; terminal_zero_fill_campaigns?: number | null;
    campaign_success_rate?: Decimal; current_executable_net_usd?: Decimal;
  };
  live?: { current_state?: string | null; reason?: string | null; source_ts?: number | null;
    result_kind?: string | null;
    close_reason?: string | null; closed_for_entries?: boolean | null;
    start_ts?: number | null; process_id?: string | null; batch_id?: string | null;
    cycle_id?: string | null; phase?: string | null; session_id?: string | null;
    position_cycles?: number | null; max_position_cycles?: number | null;
    loss_used_usd?: Decimal; loss_budget_usd?: Decimal;
    max_position_cycles_disabled?: boolean; loss_budget_disabled?: boolean;
    pair?: string | null };
  active_cycle?: (CycleSummary & {
    opened_at?: number | null; base_quantity?: Decimal; entry_anchor_bps?: Decimal;
    anchor_source?: string | null; valuation_as_of?: number | null;
    fees_estimated?: boolean | null;
  }) | null;
  recent_cycles?: CycleSummary[]; diagnostic?: Record<string, unknown> | null;
  missing_reason?: string | null;
}
export interface ProfitAmounts {
  accounting?: "pending" | "provisional" | "final";
  realized_usd: Decimal; funding_usd: Decimal; fees_usd: Decimal; net_usd: Decimal;
  known_realized_usd: string; known_funding_usd: string; known_fees_usd: string;
  pending_realized: number; pending_funding: number; pending_fees: number;
}
export interface ProfitCycle extends ProfitAmounts {
  source_id?: "basis" | "funding"; strategy_mode?: string; first_fill_at?: number | null; flat_confirmed_at?: number | null; hold_seconds?: number | null; duration_state?: "closed" | "active" | "unknown";
  first_fill_confirmed_at?: number | null; estimated_hold_seconds?: number | null; confirmation_duration_state?: "closed" | "active" | "unknown"; has_confirmed_fill?: boolean;
  cycle_id: string; symbol: string; pair_identity?: string | null;
  side?: string | null; phase?: string | null; started_at?: number | null; completed_at?: number | null;
  paired_round_trip?: boolean; inherited?: boolean; entropy_units?: number; hedge_units?: number;
  order_count: number; quantity_step?: Decimal; accounting: "final" | "provisional";
  order_fees: { order_ref: string; fee_usd: Decimal; known_fee_usd?: Decimal }[];
  fee_basis: "cycle_complete" | "order_confirmation"; unrealized_usd: Decimal;
  active_realized_usd: Decimal; executable_gross_usd: Decimal;
}
export interface ProfitOverview {
  view?: string; counts?: Record<string, number>; coverage_missing_sources?: number;
  available: boolean; reason?: string; strategy_mode?: string; symbol?: string; pair_identity?: string | null;
  totals?: ProfitAmounts & { accounting: "pending" | "provisional" | "final";
    cycle_count: number; complete_cycles: number; pending_cycles: number };
  symbols: (ProfitAmounts & { symbol: string; cycle_count: number; order_count: number })[];
  daily: (ProfitAmounts & { day: string })[]; cycles: ProfitCycle[];
  holding?: { cycle_count: number; unrealized_usd: Decimal; cycles: ProfitCycle[] }; rules?: string;
}
export interface MarketPoint {
  pair_id?: string | null; reference_usable?: boolean | null;
  ts: number; valid?: boolean | null; continuity_break?: boolean | null; quality_reason?: string | null;
  session_id?: string | null; quality?: Quality | null; segment_id?: string | number | null;
  buy_basis_bps?: Decimal; sell_basis_bps?: Decimal; mid_basis_bps?: Decimal;
  reference_bps?: Decimal; long_edge_bps?: Decimal; short_edge_bps?: Decimal;
  entry_band_bps?: Decimal; cycle_id?: string | null;
}
export interface MarketData {
  units?: Decimal; units_source?: string; points?: MarketPoint[];
  quantity_step?: Decimal; live_points?: MarketPoint[];
  live_coverage?: Record<string, unknown> | null;
  series_sources?: Record<string, Record<string, unknown>>;
  episodes?: { start_ts: number; end_ts: number; direction: string; units: number;
    max_edge_bps: Decimal; sample_count: number; quality: Quality;
    live_decision_link: string; created_cycle_id?: string | null }[];
  episodes_truncated?: boolean;
  coverage?: Record<string, unknown> | null;
  diagnostic?: Record<string, unknown> | null;
}
export interface CyclesData { items?: CycleSummary[]; next_cursor?: string | null; total?: number | null; reason?: string | null }
export interface CycleDetailData {
  profit?: ProfitCycle | null;
  cycle?: CycleSummary | null;
  groups?: (ExecutionGroup & { kind?: string | null })[];
  pnl_breakdown?: { trade_cash_usd: Decimal; fees_usd: Decimal;
    funding_usd: Decimal; net_usd: Decimal } | null;
  economics?: Record<string, unknown> | null;
  diagnostic?: Record<string, unknown> | null;
}
export interface ExecutionData {
  summary?: Record<string, unknown> | null;
  groups?: ExecutionGroup[];
}
export interface EconomicsData {
  finalized_net_pnl_usd?: Decimal;
  completed_cycles?: number | null; pending_accounting_cycles?: number | null;
  cumulative?: { ts: number; pnl_usd?: Decimal; cumulative_pnl_usd?: Decimal }[];
  current_executable_net_usd?: Decimal;
  attribution?: Record<string, unknown> | null;
}
export interface ResearchData {
  reason?: string | null;
  reports?: { id: string; title?: string | null; status?: string | null; reason?: string | null;
    sample_count?: number | null; train_range?: string | null;
    validation_range?: string | null; holdout_range?: string | null;
    candidates?: { label: string; metrics?: Record<string, string | number | null>;
      quality?: Quality | null; reason?: string | null }[];
    download_url?: string | null }[];
}
export interface SystemData {
  funding?: FundingDashboard | null;
  sources?: Record<string, SourceInfo>;
  live?: Record<string, unknown> | null; recorder?: Record<string, unknown> | null;
  compatibility?: { status?: string | null; shared_core_hash_match?: boolean | null;
    config_match?: boolean | null; pair_match?: boolean | null; schema_match?: boolean | null;
    reason?: string | null } | null;
  config?: { group: string; key: string; label?: string | null;
    configured?: string | number | boolean | null; authorized?: string | number | boolean | null;
    effective?: string | number | boolean | null; source?: string | null;
    configured_present?: boolean; effective_present?: boolean;
    explanation?: string | null }[] | Record<string, unknown> | null;
  batch?: { batch_id?: string | null; position_cycles?: number | null;
    max_position_cycles?: number | "disabled" | null; loss_used_usd?: Decimal; loss_budget_usd?: Decimal;
    closed_for_entries?: boolean | null; close_reason?: string | null;
    close_value?: Decimal; close_threshold?: Decimal; close_cycle_id?: string | null } | null;
  warnings?: string[]; capabilities?: { name: string; status: string; reason?: string | null }[] | Record<string, unknown> | null;
}
export interface HealthData { api?: { status?: string | null; observed_at?: number | null }; source_health?: Record<string, SourceInfo> }
