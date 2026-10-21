"""Configuration: strategy from a YAML file, credentials from .env, market
selection (symbol + hedge venue) from the command line.

The split is deliberate: config.yaml IS the strategy (thresholds, sizing,
risk) and is safe to share/commit as an example; .env holds only secrets;
which markets to trade is stated explicitly on every start (--symbol,
--hedge). Every YAML key is validated against the schema below, so a typo
is an error rather than a setting that silently does nothing.

Threshold model (entry/exit bands around a seeded rolling center):

    premium_bps = (entropy_price / hedge_price - 1) * 10_000

    SELL entropy / BUY hedge  fires when the executable premium
        (entropy bid over hedge ask) >= midline_bps + upper_bps
    BUY entropy / SELL hedge  fires when the executable premium
        (entropy ask under hedge bid) <= midline_bps - lower_bps

    With profit_guard enabled, normal exits use actual FIFO cost and current
    executable depth; exit_bps is the entry scenario's reversion target.
    Emergency hedges/recovery are never blocked by a profit condition.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field, replace
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from dotenv import load_dotenv

HL_API_URL = "https://api.hyperliquid.xyz"
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"   # official ws — the only HL feed used

HEDGE_VENUES = ("lighter", "lighter-rh", "tradexyz")


@dataclass(frozen=True)
class LighterProfile:
    name: str
    api_url: str
    ws_url: str
    chain_id: int


# Endpoint profiles for the two supported zkLighter deployments (these match
# lighter-python's lighter.endpoint_profiles, duplicated here so --record-only
# data collection works without the SDK installed).
LIGHTER_PROFILES: Dict[str, LighterProfile] = {
    "lighter": LighterProfile(
        "mainnet", "https://mainnet.zklighter.elliot.ai",
        "wss://mainnet.zklighter.elliot.ai/stream", 304),
    "lighter-rh": LighterProfile(
        "robinhood", "https://api.rh.lighter.xyz",
        "wss://api.rh.lighter.xyz/stream", 466324),
}


@dataclass
class LighterCreds:
    account_index: Optional[int]
    api_key_index: Optional[int]
    api_private_key: Optional[str]

    @property
    def complete(self) -> bool:
        return (self.account_index is not None and self.api_key_index is not None
                and bool(self.api_private_key))


@dataclass
class HLCreds:
    private_key: Optional[str]
    account_address: Optional[str]

    @property
    def complete(self) -> bool:
        return bool(self.private_key)


@dataclass(frozen=True)
class AdaptiveBandsConfig:
    enabled: bool
    entry_sigma_mult: float
    min_upper_bps: float
    min_lower_bps: float
    max_upper_bps: float
    max_lower_bps: float
    max_sigma_bps: float
    sigma_window_sec: float = 1800.0


@dataclass(frozen=True)
class PremiumModelConfig:
    max_book_age_ms: float = 1200.0
    max_cross_venue_skew_ms: float = 500.0
    degraded_after_sec: float = 5.0


@dataclass(frozen=True)
class MidlineLagGuardConfig:
    enabled: bool = False
    warn_bps: float = 1.5
    block_bps: float = 3.0
    persist_sec: float = 900.0
    accelerate_bps: float = 2.0
    accelerate_persist_sec: float = 300.0
    recovery_sec: float = 90.0
    recovery_bps: float = 2.0
    warning_entry_penalty_bps: float = 1.5
    block_accelerating: bool = True
    block_blocked: bool = True


@dataclass(frozen=True)
class ReferencePolicyConfig:
    """Translate model uncertainty into a reference and entry requirement."""

    enabled: bool = True
    warning_penalty_bps: float = 1.0
    accelerating_mode: str = "blended_midline"
    accelerating_sigma_mult: float = 1.5
    shifting_mode: str = "dynamic_center"
    shifting_sigma_mult: float = 2.0
    shifting_min_deviation_bps: float = 2.0
    blocked_mode: str = "dynamic_center"
    blocked_sigma_mult: float = 2.0
    blocked_min_deviation_bps: float = 2.5
    extreme_enabled: bool = True
    extreme_sigma_mult: float = 3.0


@dataclass(frozen=True)
class CycleExitConfig:
    enabled: bool
    anchor_mode: str
    max_midline_drift_bps: float
    reset_on_flat: bool
    state_json: str
    mode: str = "entry_midline"
    normal_min_profit_usd: float = 0.0
    fast_exit_after_sec: float = 30.0
    fast_exit_min_profit_usd: float = -0.01
    # ``max_hold_sec`` is retained as a config/API compatibility alias for
    # the long-hold observation threshold. It is not an exit trigger.
    max_hold_sec: float = 600.0
    long_hold_after_sec: float = 600.0
    hard_timeout_enabled: bool = True
    hard_timeout_sec: float = 3600.0
    # Compatibility telemetry only: a risk deadline cannot wait for this floor.
    hard_timeout_min_profit_usd: float = -0.02
    pnl_loss_stop_enabled: bool = False
    max_cycle_loss_bps: Optional[float] = None
    adverse_drift_exit_enabled: bool = False
    # Compatibility alias: either adverse-drift switch enables the same rule.
    adverse_drift_enabled: bool = False
    adverse_drift_min_age_sec: float = 30.0
    adverse_drift_min_loss_usd: float = -0.03
    adverse_drift_bps: float = 5.0
    adverse_drift_persist_sec: float = 5.0
    regime_stop_enabled: bool = False
    regime_stop_min_age_sec: float = 90.0
    regime_stop_min_loss_usd: float = -0.03
    regime_stop_require_midline_warning: bool = True


@dataclass(frozen=True)
class DynamicHedgeReserveConfig:
    """Bound the adverse cost of the second leg during a close retry."""

    enabled: bool = False
    min_bps: float = 2.0
    max_bps: float = 8.0
    adverse_move_percentile: float = 0.95
    safety_margin_bps: float = 1.0


@dataclass(frozen=True)
class DynamicOpenFirstLegConfig:
    """Bound the Entropy first leg with recent submit-latency risk."""

    enabled: bool = False
    min_bps: float = 3.5
    max_bps: float = 8.0
    adverse_move_percentile: float = 0.90
    safety_margin_bps: float = 0.5


@dataclass(frozen=True)
class EconomicExecutionConfig:
    """Expected execution costs used by the OPEN whole-cycle budget.

    These percentiles describe tail diagnostics; the economic cost uses a
    time-weighted mean blended with live conditions. Physical order caps remain
    in :class:`ReduceExecutionConfig` and are applied only at send time.
    """

    # The bps floor scales with notional; the USD floor protects tiny orders.
    # Keep the legacy USD field for hot-reload and direct integrations.
    min_expected_cycle_profit_bps: float = 0.5
    min_expected_cycle_profit_usd: float = 0.0002
    open_first_cost_percentile: float = 0.75
    open_hedge_cost_percentile: float = 0.90
    close_first_cost_percentile: float = 0.75
    close_hedge_cost_percentile: float = 0.90
    safety_margin_bps: float = 0.5


@dataclass(frozen=True)
class BootstrapConfig:
    """Bounded full-cycle cold-start exploration policy.

    Bootstrap may spend a small, explicit amount of expected PnL to collect
    complete ``OPEN -> HEDGE -> REDUCE -> HEDGE -> FLAT`` observations.
    Execution, model readiness, sizing, retry campaigns and recovery safety
    always use the normal strategy policy; the legacy execution fields below
    remain parseable for hot-reload/API compatibility but are ignored.
    """

    enabled: bool = False
    mode: str = "full_cycle_sampling"
    target_complete_cycles: int = 5
    max_complete_cycles: int = 10
    min_execution_samples_per_phase: int = 3
    max_order_notional_usd: float = 15.0
    max_expected_loss_bps: float = 2.0
    max_expected_loss_usd: float = 0.004
    max_cycle_loss_usd: float = 0.006
    cumulative_loss_budget_usd: float = 0.05
    auto_disable: bool = True
    min_gross_after_fee_bps: float = -1.0
    first_leg_residual_bps: float = 0.10
    hedge_leg_residual_bps: float = 0.15
    empirical_full_weight_samples: int = 6
    # Deprecated compatibility fields; model readiness is never shortened by
    # bootstrap mode.
    dynamic_midline_warmup_sec: float = 180.0
    dynamic_midline_min_samples: int = 120
    open_execution_room_min_bps: float = 0.5
    # Used only for a bootstrap YELLOW OPEN; normal execution is unchanged.
    open_target_execution_room_bps: float = 3.0
    open_sampling_execution_cap_bps: float = 5.0
    open_campaign_sec: float = 5.0
    open_campaign_max_attempts: int = 4
    open_retry_interval_sec: float = 0.05
    open_hedge_hard_cap_bps: float = 15.0
    fast_exit_after_sec: float = 10.0
    fast_exit_min_profit_usd: float = -0.002
    sampling_exit_after_sec: float = 30.0
    sampling_exit_floor_usd: float = -0.004
    stale_exit_after_sec: float = 120.0
    stale_exit_floor_usd: float = -0.006
    close_min_execution_room_bps: float = 1.0
    close_target_execution_room_bps: float = 2.0
    close_campaign_sec: float = 5.0
    close_campaign_max_attempts: int = 5
    close_retry_interval_sec: float = 0.05
    close_hedge_hard_cap_bps: float = 15.0
    entropy_order_budget_per_min: int = 10
    hedge_order_budget_per_min: int = 10
    max_campaigns_per_signal_episode: int = 3
    reopen_improvement_bps: float = 2.0


@dataclass(frozen=True)
class ReduceExecutionConfig:
    """Execution policy shared by REDUCE protection and OPEN campaigns.

    The object keeps its historical Python name for compatibility with direct
    integrations.  The YAML/API source of truth is the existing ``execution``
    section; there is intentionally no top-level ``reduce_execution`` key.
    """

    # Zero preserves legacy retry behavior when the section is omitted. The
    # production config opts into the three-second campaign explicitly.
    exit_campaign_sec: float = 0.0
    retry_interval_sec: float = 0.15
    dynamic_hedge_reserve: DynamicHedgeReserveConfig = field(
        default_factory=DynamicHedgeReserveConfig)
    dynamic_open_first_leg: DynamicOpenFirstLegConfig = field(
        default_factory=DynamicOpenFirstLegConfig)
    open_retry_campaign_sec: float = 0.0
    open_retry_interval_sec: float = 0.25
    open_retry_max_attempts: int = 1
    open_execution_room_min_bps: float = 0.0
    reserve_reduce_slots: int = 2
    # Strategy attempt budgets are separate from each venue's hard API ceiling.
    # Recovery/reconciliation sends consume the reserved capacity first.
    open_attempts_per_min: int = 8
    reduce_attempts_per_min: int = 10
    recovery_reserve_per_min: int = 2
    open_first_leg_hard_cap_bps: float = 10.0
    open_second_leg_hard_cap_bps: float = 12.0
    exit_campaign_max_attempts: int = 4
    reopen_improvement_bps: float = 3.0
    max_campaigns_per_signal_episode: int = 2
    # Canonical YAML/API name is ``exit_campaign_retry_interval_sec``.  This
    # constructor alias keeps older unit/integration callers source-compatible.
    exit_campaign_retry_interval_sec: Optional[float] = None

    def __post_init__(self) -> None:
        interval = self.exit_campaign_retry_interval_sec
        if interval is None:
            interval = self.retry_interval_sec
        object.__setattr__(self, "exit_campaign_retry_interval_sec",
                           float(interval))
        object.__setattr__(self, "retry_interval_sec", float(interval))


@dataclass(frozen=True)
class ReentryGuardConfig:
    """Prevent immediate same-direction re-entry after a risk exit."""

    enabled: bool = True
    post_risk_exit_cooldown_sec: float = 30.0
    basis_reset_bps: float = 1.0
    same_direction_only: bool = True


@dataclass(frozen=True)
class MarginRiskConfig:
    """Account-level margin guard independent from cycle PnL."""

    enabled: bool = True
    warning_available_ratio: float = 0.20
    critical_available_ratio: float = 0.05
    on_unavailable: str = "ignore"


@dataclass(frozen=True)
class ProfitGuardConfig:
    """Whole-slice economics; buffers are per paired execution, not per leg."""

    enabled: bool = False
    min_profit_usd: float = 0.01
    entry_buffer_bps: float = 12.0
    exit_buffer_bps: float = 12.0
    mode: str = "hard"


@dataclass(frozen=True)
class HotReloadConfig:
    """Controls the startup-enabled strategy configuration watcher."""

    enabled: bool = True
    poll_sec: float = 2.0


@dataclass(frozen=True)
class PersistenceConfig:
    """Durable runtime data and single-process ownership settings."""

    sqlite_path: str = "logs/entropy_arb.sqlite3"
    migrate_legacy: bool = True
    lock_file: str = "logs/entropy-arb.lock"


@dataclass(frozen=True)
class WeChatClawBotConfig:
    """Safe, non-secret settings for hourly WeChat ClawBot reports.

    The bot token is either read from the environment for backward
    compatibility or from the local QR-login credentials file.  This config
    only contains paths and transport settings, so a committed YAML file can
    never contain a messaging credential.
    """

    enabled: bool = False
    base_url: str = "https://ilinkai.weixin.qq.com"
    interval_sec: float = 3600.0
    poll_timeout_sec: float = 35.0
    request_timeout_sec: float = 45.0
    state_file: str = "logs/wechat_clawbot_state.json"
    credentials_file: str = "logs/wechat_clawbot_credentials.json"
    send_startup: bool = False


@dataclass
class VenueConf:
    key: str                  # "entropy" | "hedge"
    kind: str                 # "hl" | "lighter"
    label: str                # human name for logs, e.g. "ENTROPY", "RH"
    symbol: str
    fee_bps: float
    cap_usd: float
    orders_per_min: int
    leverage: Optional[int] = None  # explicitly enforced at live startup
    # hl
    hl_dex: str = ""
    hl_creds: Optional[HLCreds] = None
    # lighter
    lighter_profile: Optional[LighterProfile] = None
    lighter_creds: Optional[LighterCreds] = None
    lighter_request_limit: int = 48
    lighter_realtime_reserve: int = 12
    lighter_history_limit: int = 6


@dataclass
class Config:
    symbol: str
    hedge_venue: str
    entropy: VenueConf
    hedge: VenueConf
    # thresholds (the whole signal)
    midline_bps: float
    upper_bps: float
    lower_bps: float
    exit_bps: float
    allow_sell_entropy_open: bool
    allow_buy_entropy_open: bool
    # dynamic midline
    dynamic_midline_enabled: bool
    dynamic_sample_sec: float
    dynamic_short_window_sec: float
    dynamic_long_window_sec: float
    dynamic_warmup_sec: float
    dynamic_min_samples: int
    dynamic_shift_min_bps: float
    dynamic_shift_sigma_mult: float
    dynamic_max_move_bps_per_10m: float
    dynamic_base_move_bps_per_10m: float
    dynamic_accelerated_move_bps_per_10m: float
    dynamic_absolute_max_move_bps_per_10m: float
    dynamic_recovery_ratio: float
    dynamic_recovery_sec: float
    premium_model: PremiumModelConfig
    adaptive_bands: AdaptiveBandsConfig
    midline_lag_guard: MidlineLagGuardConfig
    cycle_exit: CycleExitConfig
    reentry_guard: ReentryGuardConfig
    margin_risk: MarginRiskConfig
    profit_guard: ProfitGuardConfig
    hot_reload: HotReloadConfig
    persistence: PersistenceConfig
    wechat_clawbot: WeChatClawBotConfig
    # funding
    funding_enabled: bool
    funding_poll_sec: float
    funding_stale_sec: float
    funding_expected_hold_hours: float
    funding_max_adverse_cost_bps: float
    funding_on_unavailable: str
    # sizing
    take_fraction: float
    max_order_notional: float
    min_order_notional: float
    # inventory ladder
    inventory_scale_bps: float
    inventory_floor_frac: float
    # one paired cycle at a time; ADD is an explicit opt-in
    cycle_allow_add: bool
    cycle_max_active_pairs: int
    # execution
    premium_persist_sec: float
    cooldown_sec: float
    settle_timeout_sec: float
    recovery_timeout_sec: float
    leg_slippage_bps: float
    second_leg_slippage_bps: float
    min_edge_surplus_bps: float
    hedge_slippage_bps: float
    no_fill_retry_sec: float
    net_tolerance_base: float
    max_consecutive_errors: int
    rate_limit_pause_sec: float
    staleness_sec: float
    reconcile_sec: float
    venue_probe_sec: float
    http_keepalive_sec: float
    # action-aware execution (legacy fields above remain compatibility aliases)
    open_persist_sec: float
    reduce_persist_sec: float
    open_cooldown_sec: float
    reduce_cooldown_sec: float
    open_first_leg_slippage_bps: float
    reduce_first_leg_slippage_bps: float
    open_second_leg_slippage_bps: float
    reduce_second_leg_slippage_bps: float
    open_no_fill_retry_sec: float
    reduce_no_fill_retry_sec: float
    emergency_slippage_bps: float
    max_reduce_retry_attempts: int
    reduce_retry_escalation_bps: tuple[float, ...]
    action_specific_execution: bool
    # recorder
    recorder_enabled: bool
    recorder_csv: str
    recorder_seconds_csv: str
    # logging
    log_level: str
    status_interval_sec: float
    trades_csv: str
    dashboard: bool
    log_file: str
    # runtime
    hl_api_url: str = HL_API_URL
    hl_ws_url: str = HL_WS_URL
    web_dashboard_enabled: bool = False
    dashboard_status_json: str = "logs/status.json"
    dashboard_pnl_state_json: str = "logs/pnl_state.json"
    dashboard_api_host: str = "127.0.0.1"
    dashboard_api_port: int = 8765
    dashboard_snapshot_sec: float = 1.0
    dashboard_day_timezone: str = "Asia/Taipei"
    reduce_execution: ReduceExecutionConfig = field(
        default_factory=ReduceExecutionConfig)
    reference_policy: ReferencePolicyConfig = field(
        default_factory=ReferencePolicyConfig)
    economic_execution: EconomicExecutionConfig = field(
        default_factory=EconomicExecutionConfig)
    bootstrap: BootstrapConfig = field(default_factory=BootstrapConfig)

    @property
    def creds_complete(self) -> bool:
        for v in (self.entropy, self.hedge):
            if v.kind == "hl" and not (v.hl_creds and v.hl_creds.complete):
                return False
            if v.kind == "lighter" and not (v.lighter_creds
                                            and v.lighter_creds.complete):
                return False
        return True


# ----------------------------------------------------------------- YAML layer

# Schema: nested dict of key -> type (or nested dict). Unknown keys are errors.
_SCHEMA: Dict[str, Any] = {
    "lighter_requests": {
        "limit_per_min": int,
        "realtime_reserve": int,
        "history_per_min": int,
    },
    "profit_guard": {
        "enabled": bool,
        "min_profit_usd": float,
        "entry_buffer_bps": float,
        "exit_buffer_bps": float,
        "mode": str,
    },
    "thresholds": {
        "midline_bps": float,
        "upper_bps": float,
        "lower_bps": float,
        "exit_bps": float,
        "allow_sell_entropy_open": bool,
        "allow_buy_entropy_open": bool,
    },
    "dynamic_midline": {
        "enabled": bool,
        "sample_sec": float,
        "short_window_sec": float,
        "long_window_sec": float,
        "warmup_sec": float,
        "min_samples": int,
        "shift_min_bps": float,
        "shift_sigma_mult": float,
        "max_move_bps_per_10m": float,
        "base_move_bps_per_10m": float,
        "accelerated_move_bps_per_10m": float,
        "absolute_max_move_bps_per_10m": float,
        "recovery_ratio": float,
        "recovery_sec": float,
    },
    "premium_model": {
        "max_book_age_ms": float,
        "max_cross_venue_skew_ms": float,
        "degraded_after_sec": float,
    },
    "adaptive_bands": {
        "enabled": bool,
        "entry_sigma_mult": float,
        "min_upper_bps": float,
        "min_lower_bps": float,
        "max_upper_bps": float,
        "max_lower_bps": float,
        "max_sigma_bps": float,
        "sigma_window_sec": float,
    },
    "midline_lag_guard": {
        "enabled": bool,
        "warn_bps": float,
        "block_bps": float,
        "persist_sec": float,
        "accelerate_bps": float,
        "accelerate_persist_sec": float,
        "recovery_sec": float,
        "recovery_bps": float,
        "warning_entry_penalty_bps": float,
        "block_accelerating": bool,
        "block_blocked": bool,
    },
    "reference_policy": {
        "enabled": bool,
        "warning_penalty_bps": float,
        "accelerating": {
            "mode": str,
            "sigma_mult": float,
        },
        "shifting": {
            "mode": str,
            "sigma_mult": float,
            "min_deviation_bps": float,
        },
        "blocked": {
            "mode": str,
            "sigma_mult": float,
            "min_deviation_bps": float,
        },
        "extreme_dislocation": {
            "enabled": bool,
            "sigma_mult": float,
        },
    },
    "economic_execution": {
        "min_expected_cycle_profit_bps": float,
        "min_expected_cycle_profit_usd": float,
        "open_first_cost_percentile": float,
        "open_hedge_cost_percentile": float,
        "close_first_cost_percentile": float,
        "close_hedge_cost_percentile": float,
        "safety_margin_bps": float,
    },
    "bootstrap": {
        "enabled": bool,
        "mode": str,
        "target_complete_cycles": int,
        "max_complete_cycles": int,
        "min_execution_samples_per_phase": int,
        "max_order_notional_usd": float,
        "max_expected_loss_bps": float,
        "max_expected_loss_usd": float,
        "max_cycle_loss_usd": float,
        "cumulative_loss_budget_usd": float,
        "auto_disable": bool,
        "min_gross_after_fee_bps": float,
        "first_leg_residual_bps": float,
        "hedge_leg_residual_bps": float,
        "empirical_full_weight_samples": int,
        "dynamic_midline_warmup_sec": float,
        "dynamic_midline_min_samples": int,
        "open_execution_room_min_bps": float,
        "open_target_execution_room_bps": float,
        "open_sampling_execution_cap_bps": float,
        "open_campaign_sec": float,
        "open_campaign_max_attempts": int,
        "open_retry_interval_sec": float,
        "open_hedge_hard_cap_bps": float,
        "fast_exit_after_sec": float,
        "fast_exit_min_profit_usd": float,
        "sampling_exit_after_sec": float,
        "sampling_exit_floor_usd": float,
        "stale_exit_after_sec": float,
        "stale_exit_floor_usd": float,
        "close_min_execution_room_bps": float,
        "close_target_execution_room_bps": float,
        "close_campaign_sec": float,
        "close_campaign_max_attempts": int,
        "close_retry_interval_sec": float,
        "close_hedge_hard_cap_bps": float,
        "entropy_order_budget_per_min": int,
        "hedge_order_budget_per_min": int,
        "max_campaigns_per_signal_episode": int,
        "reopen_improvement_bps": float,
    },
    "cycle_exit": {
        "enabled": bool,
        "anchor_mode": str,
        "max_midline_drift_bps": float,
        "reset_on_flat": bool,
        "state_json": str,
        "mode": str,
        "normal_min_profit_usd": float,
        "fast_exit_after_sec": float,
        "fast_exit_min_profit_usd": float,
        "max_hold_sec": float,
        "long_hold_after_sec": float,
        "hard_timeout_enabled": bool,
        "hard_timeout_sec": float,
        "hard_timeout_min_profit_usd": float,
        "pnl_loss_stop_enabled": bool,
        "max_cycle_loss_bps": (float, type(None)),
        "adverse_drift_exit_enabled": bool,
        "adverse_drift_enabled": bool,
        "adverse_drift_min_age_sec": float,
        "adverse_drift_min_loss_usd": float,
        "adverse_drift_bps": float,
        "adverse_drift_persist_sec": float,
        "regime_stop_enabled": bool,
        "regime_stop_min_age_sec": float,
        "regime_stop_min_loss_usd": float,
        "regime_stop_require_midline_warning": bool,
    },
    "reentry_guard": {
        "enabled": bool,
        "post_risk_exit_cooldown_sec": float,
        "basis_reset_bps": float,
        "same_direction_only": bool,
    },
    "margin_risk": {
        "enabled": bool,
        "warning_available_ratio": float,
        "critical_available_ratio": float,
        "on_unavailable": str,
    },
    "cycle": {
        "allow_add": bool,
        "max_active_pairs": int,
    },
    "funding": {
        "enabled": bool,
        "poll_sec": float,
        "stale_sec": float,
        "expected_hold_hours": float,
        "max_adverse_cost_bps": float,
        "on_unavailable": str,
    },
    "entropy": {
        "dex": str,
        "taker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
        "leverage": int,
    },
    "hedge": {
        "taker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
        "leverage": int,
    },
    "sizing": {
        "take_fraction": float,
        "max_order_notional_usd": float,
        "min_order_notional_usd": float,
    },
    "inventory": {
        "scale_bps": float,
        "floor_frac": float,
    },
    "execution": {
        "premium_persist_sec": float,
        "cooldown_sec": float,
        "settle_timeout_sec": float,
        "recovery_timeout_sec": float,
        "leg_slippage_bps": float,
        "second_leg_slippage_bps": float,
        "min_edge_surplus_bps": float,
        "hedge_slippage_bps": float,
        "no_fill_retry_sec": float,
        "net_tolerance_base": float,
        "max_consecutive_errors": int,
        "rate_limit_pause_sec": float,
        "staleness_sec": float,
        "reconcile_sec": float,
        "venue_probe_sec": float,
        "http_keepalive_sec": float,
        "open_persist_sec": float,
        "reduce_persist_sec": float,
        "open_cooldown_sec": float,
        "reduce_cooldown_sec": float,
        "open_first_leg_slippage_bps": float,
        "reduce_first_leg_slippage_bps": float,
        "open_second_leg_slippage_bps": float,
        "reduce_second_leg_slippage_bps": float,
        "open_no_fill_retry_sec": float,
        "reduce_no_fill_retry_sec": float,
        "emergency_slippage_bps": float,
        "max_reduce_retry_attempts": int,
        "reduce_retry_escalation_bps": list,
        "exit_campaign_sec": float,
        "exit_campaign_retry_interval_sec": float,
        "dynamic_hedge_reserve": {
            "enabled": bool,
            "min_bps": float,
            "max_bps": float,
            "adverse_move_percentile": float,
            "safety_margin_bps": float,
        },
        "dynamic_open_first_leg": {
            "enabled": bool,
            "min_bps": float,
            "max_bps": float,
            "adverse_move_percentile": float,
            "safety_margin_bps": float,
        },
        "open_retry_campaign_sec": float,
        "open_retry_interval_sec": float,
        "open_retry_max_attempts": int,
        "open_execution_room_min_bps": float,
        "reserve_reduce_slots": int,
        "open_attempts_per_min": int,
        "reduce_attempts_per_min": int,
        "recovery_reserve_per_min": int,
        "open_first_leg_hard_cap_bps": float,
        "open_second_leg_hard_cap_bps": float,
        "exit_campaign_max_attempts": int,
        "reopen_improvement_bps": float,
        "max_campaigns_per_signal_episode": int,
    },
    "recorder": {
        "enabled": bool,
        "csv": str,
        "seconds_csv": str,
    },
    "dashboard": {
        "enabled": bool,
        "status_json": str,
        "pnl_state_json": str,
        "api_host": str,
        "api_port": int,
        "snapshot_sec": float,
        "day_timezone": str,
    },
    "logging": {
        "level": str,
        "status_interval_sec": float,
        "trades_csv": str,
        "dashboard": bool,
        "file": str,
    },
    "hot_reload": {
        "enabled": bool,
        "poll_sec": float,
    },
    "persistence": {
        "sqlite_path": str,
        "migrate_legacy": bool,
        "lock_file": str,
    },
    "wechat_clawbot": {
        "enabled": bool,
        "base_url": str,
        "interval_sec": float,
        "poll_timeout_sec": float,
        "request_timeout_sec": float,
        "state_file": str,
        "credentials_file": str,
        "send_startup": bool,
    },
}


class ConfigError(ValueError):
    pass


def _validate(node: Any, schema: Dict[str, Any], path: str = "") -> None:
    if not isinstance(node, dict):
        raise ConfigError(f"'{path or '<root>'}' must be a mapping")
    for key, val in node.items():
        here = f"{path}.{key}" if path else str(key)
        if key not in schema:
            raise ConfigError(f"unknown config key '{here}' "
                              f"(valid: {', '.join(sorted(schema))})")
        want = schema[key]
        if isinstance(want, dict):
            _validate(val, want, here)
        elif isinstance(want, tuple):
            # A nullable numeric is used for deprecated limits that must be
            # explicitly disabled in the corrected cycle-exit policy.
            if val is None and type(None) in want:
                continue
            if float in want:
                if not isinstance(val, (int, float)) or isinstance(val, bool):
                    raise ConfigError(f"'{here}' must be a number or null, "
                                      f"got {val!r}")
            elif not isinstance(val, want):
                raise ConfigError(f"'{here}' has an invalid type")
        elif want is float:
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be a number, got {val!r}")
        elif want is int:
            if not isinstance(val, int) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be an integer, got {val!r}")
        elif want is bool:
            if not isinstance(val, bool):
                raise ConfigError(f"'{here}' must be true/false, got {val!r}")
        elif want is str:
            if not isinstance(val, str):
                raise ConfigError(f"'{here}' must be a string, got {val!r}")
        elif want is list:
            if not isinstance(val, list):
                raise ConfigError(f"'{here}' must be a list, got {val!r}")
            if any(isinstance(item, bool)
                   or not isinstance(item, (int, float)) for item in val):
                raise ConfigError(f"'{here}' must contain only numbers")


def _get(d: dict, section: str, key: str, default):
    return (d.get(section) or {}).get(key, default)


# ------------------------------------------------------------------ env layer

def _env_s(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v.strip() if v not in (None, "") else None


def _env_i(name: str) -> Optional[int]:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else None


# -------------------------------------------------------------------- loading

def load_config(config_file: str = "config.yaml", env_file: str = ".env", *,
                symbol: str, hedge_venue: str) -> Config:
    load_dotenv(env_file)
    try:
        with open(config_file) as fh:
            raw = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        raise ConfigError(
            f"config file '{config_file}' not found — copy config.example.yaml "
            f"to config.yaml and edit it / 未找到配置文件，请先复制 "
            f"config.example.yaml 为 config.yaml 并修改")
    _validate(raw, _SCHEMA)

    symbol = (symbol or "").strip()
    if not symbol:
        raise ConfigError("--symbol is required, e.g. --symbol SNDK / "
                          "必须用 --symbol 指定交易品种")
    if hedge_venue not in HEDGE_VENUES:
        raise ConfigError(
            f"--hedge must be one of {list(HEDGE_VENUES)}, got "
            f"{hedge_venue!r} / --hedge 必须是 {list(HEDGE_VENUES)} 之一")

    thr = raw.get("thresholds") or {}
    for k in ("midline_bps", "upper_bps", "lower_bps"):
        if k not in thr:
            raise ConfigError(f"'thresholds.{k}' is required — derive it from "
                              f"recorded minute data / 必须填写，请用采集的分钟"
                              f"数据计算后填入")
    upper, lower = float(thr["upper_bps"]), float(thr["lower_bps"])
    if upper <= 0 or lower <= 0:
        raise ConfigError("thresholds.upper_bps and lower_bps must be > 0 "
                          "(the round trip nets upper+lower bps after fees)")
    exit_bps = float(thr.get("exit_bps", min(1.0, min(upper, lower) / 2.0)))
    if exit_bps < 0 or exit_bps >= min(upper, lower):
        raise ConfigError("thresholds.exit_bps must be >= 0 and smaller than "
                          "both thresholds.upper_bps and lower_bps")

    dynamic_sample_sec = float(_get(raw, "dynamic_midline", "sample_sec", 1.0))
    dynamic_short = float(_get(raw, "dynamic_midline", "short_window_sec", 1200.0))
    dynamic_long = float(_get(raw, "dynamic_midline", "long_window_sec", 10800.0))
    dynamic_warmup = float(_get(raw, "dynamic_midline", "warmup_sec", 1800.0))
    dynamic_min_samples = int(_get(raw, "dynamic_midline", "min_samples", 600))
    dynamic_shift_min = float(_get(raw, "dynamic_midline", "shift_min_bps", 3.0))
    dynamic_shift_mult = float(_get(raw, "dynamic_midline", "shift_sigma_mult", 2.0))
    dynamic_legacy_move = _get(raw, "dynamic_midline",
                               "max_move_bps_per_10m", None)
    dynamic_base = float(_get(
        raw, "dynamic_midline", "base_move_bps_per_10m",
        0.5 if dynamic_legacy_move is None else dynamic_legacy_move))
    dynamic_accelerated = float(_get(
        raw, "dynamic_midline", "accelerated_move_bps_per_10m",
        dynamic_base))
    dynamic_absolute = float(_get(
        raw, "dynamic_midline", "absolute_max_move_bps_per_10m",
        max(dynamic_base, dynamic_accelerated)))
    dynamic_recovery_ratio = float(_get(raw, "dynamic_midline", "recovery_ratio", 0.6))
    dynamic_recovery_sec = float(_get(raw, "dynamic_midline", "recovery_sec", 600.0))
    if dynamic_sample_sec <= 0 or dynamic_short <= 0 or dynamic_long <= dynamic_short:
        raise ConfigError("dynamic_midline sample/window values must be > 0 and "
                          "long_window_sec must exceed short_window_sec")
    if dynamic_warmup < 0 or dynamic_min_samples < 1 or dynamic_shift_min < 0 \
            or dynamic_shift_mult <= 0 or dynamic_base < 0 \
            or dynamic_accelerated < dynamic_base \
            or dynamic_absolute < dynamic_accelerated \
            or not 0 < dynamic_recovery_ratio < 1 or dynamic_recovery_sec < 0:
        raise ConfigError("invalid dynamic_midline limits")

    premium_max_age = float(_get(
        raw, "premium_model", "max_book_age_ms", 1200.0))
    premium_max_skew = float(_get(
        raw, "premium_model", "max_cross_venue_skew_ms", 500.0))
    premium_degraded_after = float(_get(
        raw, "premium_model", "degraded_after_sec", 5.0))
    if premium_max_age <= 0 or premium_max_skew < 0 \
            or premium_degraded_after <= 0:
        raise ConfigError("invalid premium_model limits")

    adaptive_enabled = bool(_get(raw, "adaptive_bands", "enabled", False))
    entry_sigma_mult = float(
        _get(raw, "adaptive_bands", "entry_sigma_mult", 2.0))
    min_upper_bps = float(
        _get(raw, "adaptive_bands", "min_upper_bps", upper))
    min_lower_bps = float(
        _get(raw, "adaptive_bands", "min_lower_bps", lower))
    max_upper_bps = float(
        _get(raw, "adaptive_bands", "max_upper_bps", 15.0))
    max_lower_bps = float(
        _get(raw, "adaptive_bands", "max_lower_bps", 15.0))
    max_sigma_bps = float(
        _get(raw, "adaptive_bands", "max_sigma_bps", 8.0))
    sigma_window_sec = float(
        _get(raw, "adaptive_bands", "sigma_window_sec", 1800.0))
    if (entry_sigma_mult <= 0 or min_upper_bps < 0 or min_lower_bps < 0
            or max_upper_bps < max(upper, min_upper_bps)
            or max_lower_bps < max(lower, min_lower_bps)
            or max_sigma_bps <= 0 or sigma_window_sec <= 0):
        raise ConfigError("invalid adaptive_bands limits")

    lag_enabled = bool(_get(raw, "midline_lag_guard", "enabled", False))
    lag_warn = float(_get(raw, "midline_lag_guard", "warn_bps", 1.5))
    lag_block = float(_get(raw, "midline_lag_guard", "block_bps", 3.0))
    lag_persist = float(_get(raw, "midline_lag_guard", "persist_sec", 900.0))
    lag_accelerate = float(
        _get(raw, "midline_lag_guard", "accelerate_bps", 2.0))
    lag_accelerate_persist = float(_get(
        raw, "midline_lag_guard", "accelerate_persist_sec", 300.0))
    lag_recovery = float(
        _get(raw, "midline_lag_guard", "recovery_sec", 90.0))
    lag_recovery_bps = float(
        _get(raw, "midline_lag_guard", "recovery_bps", 2.0))
    lag_warning_penalty = float(_get(
        raw, "midline_lag_guard", "warning_entry_penalty_bps", 1.5))
    lag_block_accelerating = bool(_get(
        raw, "midline_lag_guard", "block_accelerating", True))
    lag_block_blocked = bool(_get(
        raw, "midline_lag_guard", "block_blocked", True))
    if (lag_warn < 0 or lag_block <= lag_warn or lag_persist <= 0
            or lag_accelerate < lag_warn
            or lag_accelerate_persist <= 0 or lag_recovery <= 0
            or not math.isfinite(lag_recovery_bps)
            or lag_recovery_bps < 0
            or not math.isfinite(lag_warning_penalty)
            or lag_warning_penalty < 0):
        raise ConfigError("invalid midline_lag_guard limits")

    reference_raw = raw.get("reference_policy") or {}
    accelerating_raw = reference_raw.get("accelerating") or {}
    shifting_raw = reference_raw.get("shifting") or {}
    blocked_raw = reference_raw.get("blocked") or {}
    extreme_raw = reference_raw.get("extreme_dislocation") or {}
    reference_policy = ReferencePolicyConfig(
        enabled=bool(reference_raw.get("enabled", True)),
        warning_penalty_bps=float(reference_raw.get(
            "warning_penalty_bps", lag_warning_penalty)),
        accelerating_mode=str(accelerating_raw.get(
            "mode", "blended_midline")),
        accelerating_sigma_mult=float(accelerating_raw.get(
            "sigma_mult", 1.5)),
        shifting_mode=str(shifting_raw.get("mode", "dynamic_center")),
        shifting_sigma_mult=float(shifting_raw.get("sigma_mult", 2.0)),
        shifting_min_deviation_bps=float(shifting_raw.get(
            "min_deviation_bps", 2.0)),
        blocked_mode=str(blocked_raw.get("mode", "dynamic_center")),
        blocked_sigma_mult=float(blocked_raw.get("sigma_mult", 2.0)),
        blocked_min_deviation_bps=float(blocked_raw.get(
            "min_deviation_bps", 2.5)),
        extreme_enabled=bool(extreme_raw.get("enabled", True)),
        extreme_sigma_mult=float(extreme_raw.get("sigma_mult", 3.0)),
    )
    valid_reference_modes = {"active_midline", "blended_midline",
                             "dynamic_center"}
    if (reference_policy.warning_penalty_bps < 0
            or reference_policy.accelerating_mode not in valid_reference_modes
            or reference_policy.shifting_mode not in valid_reference_modes
            or reference_policy.blocked_mode not in valid_reference_modes
            or not math.isfinite(reference_policy.warning_penalty_bps)
            or not math.isfinite(reference_policy.accelerating_sigma_mult)
            or reference_policy.accelerating_sigma_mult <= 0
            or not math.isfinite(reference_policy.shifting_sigma_mult)
            or reference_policy.shifting_sigma_mult <= 0
            or not math.isfinite(reference_policy.shifting_min_deviation_bps)
            or reference_policy.shifting_min_deviation_bps < 0
            or not math.isfinite(reference_policy.blocked_sigma_mult)
            or reference_policy.blocked_sigma_mult <= 0
            or not math.isfinite(reference_policy.blocked_min_deviation_bps)
            or reference_policy.blocked_min_deviation_bps < 0
            or not math.isfinite(reference_policy.extreme_sigma_mult)
            or reference_policy.extreme_sigma_mult <= 0):
        raise ConfigError("invalid reference_policy limits")

    cycle_enabled = bool(_get(raw, "cycle_exit", "enabled", False))
    cycle_anchor_mode = str(
        _get(raw, "cycle_exit", "anchor_mode", "entry_midline"))
    cycle_drift = float(
        _get(raw, "cycle_exit", "max_midline_drift_bps", 4.0))
    cycle_reset = bool(_get(raw, "cycle_exit", "reset_on_flat", True))
    cycle_state_json = str(
        _get(raw, "cycle_exit", "state_json", "logs/cycle_state.json"))
    cycle_mode = str(_get(raw, "cycle_exit", "mode", "entry_midline"))
    cycle_normal_min = float(_get(
        raw, "cycle_exit", "normal_min_profit_usd", 0.0))
    cycle_fast_after = float(_get(
        raw, "cycle_exit", "fast_exit_after_sec", 30.0))
    cycle_fast_min = float(_get(
        raw, "cycle_exit", "fast_exit_min_profit_usd", -0.01))
    cycle_long_hold = float(_get(
        raw, "cycle_exit", "long_hold_after_sec",
        _get(raw, "cycle_exit", "max_hold_sec", 600.0)))
    # Keep the old field populated with the same value so older integrations
    # do not mistake the observation threshold for a removed exit trigger.
    cycle_max_hold = cycle_long_hold
    cycle_hard_timeout_enabled = bool(_get(
        raw, "cycle_exit", "hard_timeout_enabled", True))
    cycle_hard_timeout = float(_get(
        raw, "cycle_exit", "hard_timeout_sec", 3600.0))
    cycle_hard_timeout_min = float(_get(
        raw, "cycle_exit", "hard_timeout_min_profit_usd", -0.02))
    cycle_pnl_loss_stop = bool(_get(
        raw, "cycle_exit", "pnl_loss_stop_enabled", False))
    cycle_max_loss_raw = _get(raw, "cycle_exit", "max_cycle_loss_bps", None)
    cycle_max_loss_bps = (None if cycle_max_loss_raw is None else
                          float(cycle_max_loss_raw))
    cycle_adverse_exit_enabled = bool(_get(
        raw, "cycle_exit", "adverse_drift_exit_enabled", False))
    # The old spelling remains readable for migration diagnostics only.
    cycle_adverse_enabled = bool(_get(
        raw, "cycle_exit", "adverse_drift_enabled", False))
    cycle_adverse_min_age = float(_get(
        raw, "cycle_exit", "adverse_drift_min_age_sec", 30.0))
    cycle_adverse_min_loss = float(_get(
        raw, "cycle_exit", "adverse_drift_min_loss_usd", -0.03))
    cycle_adverse_bps = float(_get(
        raw, "cycle_exit", "adverse_drift_bps", 5.0))
    cycle_adverse_persist = float(_get(
        raw, "cycle_exit", "adverse_drift_persist_sec", 5.0))
    cycle_regime_enabled = bool(_get(
        raw, "cycle_exit", "regime_stop_enabled", False))
    cycle_regime_min_age = float(_get(
        raw, "cycle_exit", "regime_stop_min_age_sec", 90.0))
    cycle_regime_min_loss = float(_get(
        raw, "cycle_exit", "regime_stop_min_loss_usd", -0.03))
    cycle_regime_require_lag = bool(_get(
        raw, "cycle_exit", "regime_stop_require_midline_warning", True))
    if cycle_anchor_mode != "entry_midline":
        raise ConfigError("cycle_exit.anchor_mode must be 'entry_midline'")
    if cycle_mode not in {"entry_midline", "fill_pnl"}:
        raise ConfigError("cycle_exit.mode must be 'entry_midline' or 'fill_pnl'")
    cycle_nonnegative = (
        cycle_normal_min, cycle_fast_after, cycle_long_hold,
        cycle_hard_timeout, cycle_adverse_min_age, cycle_adverse_bps,
        cycle_adverse_persist, cycle_regime_min_age)
    if (not math.isfinite(cycle_drift) or cycle_drift < 0
            or not cycle_state_json
            or not all(math.isfinite(value) and value >= 0
                       for value in cycle_nonnegative)
            or cycle_max_loss_bps is not None
            and (not math.isfinite(cycle_max_loss_bps)
                 or cycle_max_loss_bps < 0)
            or cycle_hard_timeout_enabled
            and cycle_hard_timeout < cycle_long_hold
            or not math.isfinite(cycle_fast_min)
            or cycle_fast_min > 0
            or not math.isfinite(cycle_hard_timeout_min)
            or cycle_hard_timeout_min > 0
            or not math.isfinite(cycle_adverse_min_loss)
            or cycle_adverse_min_loss > 0
            or not math.isfinite(cycle_regime_min_loss)
                 or cycle_regime_min_loss > 0):
        raise ConfigError("invalid cycle_exit limits")

    execution_raw = raw.get("execution") or {}
    reserve_raw = (execution_raw.get("dynamic_hedge_reserve") or {})
    open_first_raw = (execution_raw.get("dynamic_open_first_leg") or {})
    reduce_campaign_sec = float(
        execution_raw.get("exit_campaign_sec", 0.0))
    reduce_retry_interval = float(
        execution_raw.get("exit_campaign_retry_interval_sec", 0.15))
    reserve_enabled = bool(reserve_raw.get("enabled", False))
    reserve_min_bps = float(reserve_raw.get("min_bps", 2.0))
    reserve_max_bps = float(reserve_raw.get("max_bps", 8.0))
    reserve_percentile = float(
        reserve_raw.get("adverse_move_percentile", 0.95))
    reserve_safety_bps = float(reserve_raw.get("safety_margin_bps", 1.0))
    open_first_enabled = bool(open_first_raw.get("enabled", False))
    open_first_min_bps = float(open_first_raw.get("min_bps", 3.5))
    open_first_max_bps = float(open_first_raw.get("max_bps", 8.0))
    open_first_percentile = float(
        open_first_raw.get("adverse_move_percentile", 0.90))
    open_first_safety_bps = float(
        open_first_raw.get("safety_margin_bps", 0.5))
    open_campaign_sec = float(
        execution_raw.get("open_retry_campaign_sec", 0.0))
    open_retry_interval = float(
        execution_raw.get("open_retry_interval_sec", 0.25))
    open_retry_max_attempts = int(
        execution_raw.get("open_retry_max_attempts", 1))
    open_execution_room_min_bps = float(
        execution_raw.get("open_execution_room_min_bps", 0.0))
    reserve_reduce_slots = int(
        execution_raw.get("reserve_reduce_slots", 2))
    open_attempts_per_min = int(
        execution_raw.get("open_attempts_per_min", 8))
    reduce_attempts_per_min = int(
        execution_raw.get("reduce_attempts_per_min", 10))
    recovery_reserve_per_min = int(
        execution_raw.get("recovery_reserve_per_min", 2))
    open_first_hard_cap = float(
        execution_raw.get("open_first_leg_hard_cap_bps", 10.0))
    open_second_hard_cap = float(
        execution_raw.get("open_second_leg_hard_cap_bps", 12.0))
    exit_campaign_max_attempts = int(
        execution_raw.get("exit_campaign_max_attempts", 4))
    reopen_improvement_bps = float(
        execution_raw.get("reopen_improvement_bps", 3.0))
    max_campaigns_per_signal_episode = int(
        execution_raw.get("max_campaigns_per_signal_episode", 2))
    if (not math.isfinite(reduce_campaign_sec)
            or reduce_campaign_sec < 0
            or not math.isfinite(reduce_retry_interval)
            or reduce_retry_interval <= 0
            or not math.isfinite(reserve_min_bps)
            or reserve_min_bps < 0
            or not math.isfinite(reserve_max_bps)
            or reserve_max_bps < reserve_min_bps
            or not math.isfinite(reserve_percentile)
            or not 0.0 < reserve_percentile <= 1.0
            or not math.isfinite(reserve_safety_bps)
            or reserve_safety_bps < 0
            or not math.isfinite(open_first_min_bps)
            or open_first_min_bps < 0
            or not math.isfinite(open_first_max_bps)
            or open_first_max_bps < open_first_min_bps
            or not math.isfinite(open_first_percentile)
            or not 0.0 < open_first_percentile <= 1.0
            or not math.isfinite(open_first_safety_bps)
            or open_first_safety_bps < 0
            or not math.isfinite(open_campaign_sec)
            or open_campaign_sec < 0
            or not math.isfinite(open_retry_interval)
            or open_retry_interval <= 0
            or open_retry_max_attempts < 1
            or not math.isfinite(open_execution_room_min_bps)
            or open_execution_room_min_bps < 0
            or reserve_reduce_slots < 0
            or open_attempts_per_min < 1
            or reduce_attempts_per_min < 1
            or recovery_reserve_per_min < 0
            or not math.isfinite(open_first_hard_cap)
            or open_first_hard_cap < 0
            or not math.isfinite(open_second_hard_cap)
            or open_second_hard_cap < 0
            or exit_campaign_max_attempts < 1
            or not math.isfinite(reopen_improvement_bps)
            or reopen_improvement_bps < 0
            or max_campaigns_per_signal_episode < 1):
        raise ConfigError("invalid execution campaign/bound limits")
    reduce_execution = ReduceExecutionConfig(
        exit_campaign_sec=reduce_campaign_sec,
        retry_interval_sec=reduce_retry_interval,
        dynamic_hedge_reserve=DynamicHedgeReserveConfig(
            enabled=reserve_enabled,
            min_bps=reserve_min_bps,
            max_bps=reserve_max_bps,
            adverse_move_percentile=reserve_percentile,
            safety_margin_bps=reserve_safety_bps,
        ),
        dynamic_open_first_leg=DynamicOpenFirstLegConfig(
            enabled=open_first_enabled,
            min_bps=open_first_min_bps,
            max_bps=open_first_max_bps,
            adverse_move_percentile=open_first_percentile,
            safety_margin_bps=open_first_safety_bps,
        ),
        open_retry_campaign_sec=open_campaign_sec,
        open_retry_interval_sec=open_retry_interval,
        open_retry_max_attempts=open_retry_max_attempts,
        open_execution_room_min_bps=open_execution_room_min_bps,
        reserve_reduce_slots=reserve_reduce_slots,
        open_attempts_per_min=open_attempts_per_min,
        reduce_attempts_per_min=reduce_attempts_per_min,
        recovery_reserve_per_min=recovery_reserve_per_min,
        open_first_leg_hard_cap_bps=open_first_hard_cap,
        open_second_leg_hard_cap_bps=open_second_hard_cap,
        exit_campaign_max_attempts=exit_campaign_max_attempts,
        reopen_improvement_bps=reopen_improvement_bps,
        max_campaigns_per_signal_episode=max_campaigns_per_signal_episode,
    )

    economic_raw = raw.get("economic_execution") or {}
    economic_min_profit_bps = float(
        economic_raw.get("min_expected_cycle_profit_bps", 0.5))
    economic_min_profit = float(
        economic_raw.get("min_expected_cycle_profit_usd", 0.0002))
    economic_percentiles = tuple(
        float(economic_raw.get(name, default)) for name, default in (
            ("open_first_cost_percentile", 0.75),
            ("open_hedge_cost_percentile", 0.90),
            ("close_first_cost_percentile", 0.75),
            ("close_hedge_cost_percentile", 0.90)))
    economic_safety_bps = float(
        economic_raw.get("safety_margin_bps", 0.5))
    if (not math.isfinite(economic_min_profit_bps)
            or economic_min_profit_bps < 0
            or not math.isfinite(economic_min_profit)
            or economic_min_profit < 0
            or not all(math.isfinite(value) and 0.0 < value <= 1.0
                       for value in economic_percentiles)
            or not math.isfinite(economic_safety_bps)
            or economic_safety_bps < 0):
        raise ConfigError("invalid economic_execution settings")
    economic_execution = EconomicExecutionConfig(
        min_expected_cycle_profit_bps=economic_min_profit_bps,
        min_expected_cycle_profit_usd=economic_min_profit,
        open_first_cost_percentile=economic_percentiles[0],
        open_hedge_cost_percentile=economic_percentiles[1],
        close_first_cost_percentile=economic_percentiles[2],
        close_hedge_cost_percentile=economic_percentiles[3],
        safety_margin_bps=economic_safety_bps,
    )

    bootstrap_raw = raw.get("bootstrap") or {}
    bootstrap = BootstrapConfig(
        enabled=bool(bootstrap_raw.get("enabled", False)),
        mode=str(bootstrap_raw.get("mode", "full_cycle_sampling")),
        target_complete_cycles=int(
            bootstrap_raw.get("target_complete_cycles", 5)),
        max_complete_cycles=int(
            bootstrap_raw.get("max_complete_cycles", 10)),
        min_execution_samples_per_phase=int(
            bootstrap_raw.get("min_execution_samples_per_phase", 3)),
        max_order_notional_usd=float(
            bootstrap_raw.get("max_order_notional_usd", 15.0)),
        max_expected_loss_bps=float(
            bootstrap_raw.get("max_expected_loss_bps", 2.0)),
        max_expected_loss_usd=float(
            bootstrap_raw.get("max_expected_loss_usd", 0.004)),
        max_cycle_loss_usd=float(
            bootstrap_raw.get("max_cycle_loss_usd", 0.006)),
        cumulative_loss_budget_usd=float(
            bootstrap_raw.get("cumulative_loss_budget_usd", 0.05)),
        auto_disable=bool(bootstrap_raw.get("auto_disable", True)),
        min_gross_after_fee_bps=float(
            bootstrap_raw.get("min_gross_after_fee_bps", -1.0)),
        first_leg_residual_bps=float(
            bootstrap_raw.get("first_leg_residual_bps", 0.10)),
        hedge_leg_residual_bps=float(
            bootstrap_raw.get("hedge_leg_residual_bps", 0.15)),
        empirical_full_weight_samples=int(
            bootstrap_raw.get("empirical_full_weight_samples", 6)),
        dynamic_midline_warmup_sec=float(
            bootstrap_raw.get("dynamic_midline_warmup_sec", 180.0)),
        dynamic_midline_min_samples=int(
            bootstrap_raw.get("dynamic_midline_min_samples", 120)),
        open_execution_room_min_bps=float(
            bootstrap_raw.get("open_execution_room_min_bps", 0.5)),
        open_target_execution_room_bps=float(
            bootstrap_raw.get("open_target_execution_room_bps", 3.0)),
        open_sampling_execution_cap_bps=float(
            bootstrap_raw.get("open_sampling_execution_cap_bps", 5.0)),
        open_campaign_sec=float(
            bootstrap_raw.get("open_campaign_sec", 5.0)),
        open_campaign_max_attempts=int(
            bootstrap_raw.get("open_campaign_max_attempts", 4)),
        open_retry_interval_sec=float(
            bootstrap_raw.get("open_retry_interval_sec", 0.05)),
        open_hedge_hard_cap_bps=float(
            bootstrap_raw.get("open_hedge_hard_cap_bps", 15.0)),
        fast_exit_after_sec=float(
            bootstrap_raw.get("fast_exit_after_sec", 10.0)),
        fast_exit_min_profit_usd=float(
            bootstrap_raw.get("fast_exit_min_profit_usd", -0.002)),
        sampling_exit_after_sec=float(
            bootstrap_raw.get("sampling_exit_after_sec", 30.0)),
        sampling_exit_floor_usd=float(
            bootstrap_raw.get("sampling_exit_floor_usd", -0.004)),
        stale_exit_after_sec=float(
            bootstrap_raw.get("stale_exit_after_sec", 120.0)),
        stale_exit_floor_usd=float(
            bootstrap_raw.get("stale_exit_floor_usd", -0.006)),
        close_min_execution_room_bps=float(
            bootstrap_raw.get("close_min_execution_room_bps", 1.0)),
        close_target_execution_room_bps=float(
            bootstrap_raw.get("close_target_execution_room_bps", 2.0)),
        close_campaign_sec=float(
            bootstrap_raw.get("close_campaign_sec", 5.0)),
        close_campaign_max_attempts=int(
            bootstrap_raw.get("close_campaign_max_attempts", 5)),
        close_retry_interval_sec=float(
            bootstrap_raw.get("close_retry_interval_sec", 0.05)),
        close_hedge_hard_cap_bps=float(
            bootstrap_raw.get("close_hedge_hard_cap_bps", 15.0)),
        entropy_order_budget_per_min=int(
            bootstrap_raw.get("entropy_order_budget_per_min", 10)),
        hedge_order_budget_per_min=int(
            bootstrap_raw.get("hedge_order_budget_per_min", 10)),
        max_campaigns_per_signal_episode=int(
            bootstrap_raw.get("max_campaigns_per_signal_episode", 3)),
        reopen_improvement_bps=float(
            bootstrap_raw.get("reopen_improvement_bps", 2.0)),
    )
    bootstrap_floors = (
        bootstrap.fast_exit_min_profit_usd,
        bootstrap.sampling_exit_floor_usd,
        bootstrap.stale_exit_floor_usd,
    )
    bootstrap_floats = (
        bootstrap.max_order_notional_usd,
        bootstrap.max_expected_loss_bps,
        bootstrap.max_expected_loss_usd,
        bootstrap.max_cycle_loss_usd,
        bootstrap.cumulative_loss_budget_usd,
        bootstrap.min_gross_after_fee_bps,
        bootstrap.first_leg_residual_bps,
        bootstrap.hedge_leg_residual_bps,
        bootstrap.dynamic_midline_warmup_sec,
        bootstrap.open_execution_room_min_bps,
        bootstrap.open_target_execution_room_bps,
        bootstrap.open_sampling_execution_cap_bps,
        bootstrap.open_campaign_sec,
        bootstrap.open_retry_interval_sec,
        bootstrap.open_hedge_hard_cap_bps,
        bootstrap.fast_exit_after_sec,
        bootstrap.sampling_exit_after_sec,
        bootstrap.stale_exit_after_sec,
        bootstrap.close_min_execution_room_bps,
        bootstrap.close_target_execution_room_bps,
        bootstrap.close_campaign_sec,
        bootstrap.close_retry_interval_sec,
        bootstrap.close_hedge_hard_cap_bps,
        bootstrap.reopen_improvement_bps,
    )
    if (bootstrap.mode not in {"full_cycle_sampling"}
            or bootstrap.target_complete_cycles < 1
            or bootstrap.max_complete_cycles < bootstrap.target_complete_cycles
            or bootstrap.min_execution_samples_per_phase < 1
            or bootstrap.max_order_notional_usd <= 0
            or bootstrap.max_expected_loss_bps <= 0
            or bootstrap.max_expected_loss_usd <= 0
            or bootstrap.max_cycle_loss_usd <= 0
            or bootstrap.cumulative_loss_budget_usd <= 0
            or bootstrap.first_leg_residual_bps < 0
            or bootstrap.hedge_leg_residual_bps < 0
            or bootstrap.dynamic_midline_warmup_sec < 0
            or bootstrap.dynamic_midline_min_samples < 1
            or bootstrap.open_execution_room_min_bps < 0
            or bootstrap.open_target_execution_room_bps < 0
            or bootstrap.open_sampling_execution_cap_bps < 0
            or bootstrap.open_campaign_sec <= 0
            or bootstrap.open_retry_interval_sec <= 0
            or bootstrap.open_hedge_hard_cap_bps <= 0
            or bootstrap.fast_exit_after_sec < 0
            or bootstrap.sampling_exit_after_sec < bootstrap.fast_exit_after_sec
            or bootstrap.stale_exit_after_sec < bootstrap.sampling_exit_after_sec
            or bootstrap.close_min_execution_room_bps < 0
            or bootstrap.close_target_execution_room_bps < 0
            or bootstrap.close_campaign_sec <= 0
            or bootstrap.close_retry_interval_sec <= 0
            or bootstrap.close_hedge_hard_cap_bps <= 0
            or bootstrap.reopen_improvement_bps < 0
            or bootstrap.open_campaign_max_attempts < 1
            or bootstrap.close_campaign_max_attempts < 1
            or bootstrap.entropy_order_budget_per_min < 1
            or bootstrap.hedge_order_budget_per_min < 1
            or bootstrap.max_campaigns_per_signal_episode < 1
            or bootstrap.empirical_full_weight_samples < 1
            or any(not math.isfinite(value) for value in bootstrap_floors)
            or any(not math.isfinite(value) for value in bootstrap_floats)):
        raise ConfigError("invalid bootstrap settings")

    reentry_enabled = bool(_get(raw, "reentry_guard", "enabled", True))
    reentry_cooldown = float(_get(
        raw, "reentry_guard", "post_risk_exit_cooldown_sec", 30.0))
    reentry_basis_reset = float(_get(
        raw, "reentry_guard", "basis_reset_bps", 1.0))
    reentry_same_direction = bool(_get(
        raw, "reentry_guard", "same_direction_only", True))
    if (not math.isfinite(reentry_cooldown) or reentry_cooldown < 0
            or not math.isfinite(reentry_basis_reset)
            or reentry_basis_reset < 0):
        raise ConfigError("invalid reentry_guard limits")

    margin_enabled = bool(_get(raw, "margin_risk", "enabled", True))
    margin_warning_ratio = float(_get(
        raw, "margin_risk", "warning_available_ratio", 0.20))
    margin_critical_ratio = float(_get(
        raw, "margin_risk", "critical_available_ratio", 0.05))
    margin_on_unavailable = str(_get(
        raw, "margin_risk", "on_unavailable", "ignore"))
    if (not 0 <= margin_critical_ratio < margin_warning_ratio <= 1
            or margin_on_unavailable not in {"ignore", "block_open"}):
        raise ConfigError("invalid margin_risk limits")

    cycle_allow_add = bool(_get(raw, "cycle", "allow_add", True))
    cycle_max_active_pairs = int(
        _get(raw, "cycle", "max_active_pairs", 1))
    if cycle_max_active_pairs < 1:
        raise ConfigError("cycle.max_active_pairs must be >= 1")

    funding_poll = float(_get(raw, "funding", "poll_sec", 60.0))
    funding_stale = float(_get(raw, "funding", "stale_sec", 180.0))
    funding_hold = float(_get(raw, "funding", "expected_hold_hours", 2.0))
    funding_max = float(_get(raw, "funding", "max_adverse_cost_bps", 4.0))
    funding_unavailable = str(_get(raw, "funding", "on_unavailable", "block_open"))
    if funding_poll <= 0 or funding_stale < funding_poll or funding_hold < 0 \
            or funding_max < 0:
        raise ConfigError("invalid funding poll/stale/hold/cost values")
    if funding_unavailable not in {"block_open", "ignore"}:
        raise ConfigError("funding.on_unavailable must be 'block_open' or 'ignore'")

    dashboard_port = int(_get(raw, "dashboard", "api_port", 8765))
    dashboard_snapshot_sec = float(
        _get(raw, "dashboard", "snapshot_sec", 1.0))
    dashboard_timezone = str(
        _get(raw, "dashboard", "day_timezone", "Asia/Taipei"))
    if not 1 <= dashboard_port <= 65535:
        raise ConfigError("dashboard.api_port must be between 1 and 65535")
    if dashboard_snapshot_sec <= 0:
        raise ConfigError("dashboard.snapshot_sec must be > 0")
    try:
        ZoneInfo(dashboard_timezone)
    except ZoneInfoNotFoundError as exc:
        raise ConfigError(
            f"dashboard.day_timezone is not a valid IANA timezone: "
            f"{dashboard_timezone!r}") from exc

    hot_reload_enabled = bool(_get(raw, "hot_reload", "enabled", True))
    hot_reload_poll_sec = float(_get(raw, "hot_reload", "poll_sec", 2.0))
    if hot_reload_poll_sec <= 0:
        raise ConfigError("hot_reload.poll_sec must be > 0")

    sqlite_path = str(_get(raw, "persistence", "sqlite_path",
                           "logs/entropy_arb.sqlite3"))
    migrate_legacy = bool(_get(raw, "persistence", "migrate_legacy", True))
    lock_file = str(_get(raw, "persistence", "lock_file",
                         "logs/entropy-arb.lock"))
    if not sqlite_path or not lock_file:
        raise ConfigError("persistence.sqlite_path and lock_file must be set")

    wechat_enabled = bool(_get(raw, "wechat_clawbot", "enabled", False))
    wechat_base_url = str(_get(
        raw, "wechat_clawbot", "base_url",
        "https://ilinkai.weixin.qq.com")).rstrip("/")
    wechat_interval = float(_get(
        raw, "wechat_clawbot", "interval_sec", 3600.0))
    wechat_poll_timeout = float(_get(
        raw, "wechat_clawbot", "poll_timeout_sec", 35.0))
    wechat_request_timeout = float(_get(
        raw, "wechat_clawbot", "request_timeout_sec", 45.0))
    wechat_state_file = str(_get(
        raw, "wechat_clawbot", "state_file",
        "logs/wechat_clawbot_state.json"))
    wechat_credentials_file = str(_get(
        raw, "wechat_clawbot", "credentials_file",
        "logs/wechat_clawbot_credentials.json"))
    wechat_send_startup = bool(_get(
        raw, "wechat_clawbot", "send_startup", False))
    if (not wechat_base_url.startswith(("http://", "https://"))
            or wechat_interval <= 0 or wechat_poll_timeout <= 0
            or wechat_request_timeout <= 0 or not wechat_state_file
            or not wechat_credentials_file):
        raise ConfigError("invalid wechat_clawbot settings")

    leg_slippage = float(_get(raw, "execution", "leg_slippage_bps", 6.0))
    second_leg_slippage = float(
        _get(raw, "execution", "second_leg_slippage_bps", 6.0))
    premium_persist = float(_get(
        raw, "execution", "premium_persist_sec", 0.3))
    cooldown = float(_get(raw, "execution", "cooldown_sec", 0.0))
    recovery_timeout = float(_get(
        raw, "execution", "recovery_timeout_sec", 30.0))
    open_persist = float(_get(
        raw, "execution", "open_persist_sec", premium_persist))
    reduce_persist = float(_get(
        raw, "execution", "reduce_persist_sec", premium_persist))
    open_cooldown = float(_get(
        raw, "execution", "open_cooldown_sec", cooldown))
    reduce_cooldown = float(_get(
        raw, "execution", "reduce_cooldown_sec", cooldown))
    open_first_slippage = float(_get(
        raw, "execution", "open_first_leg_slippage_bps", leg_slippage))
    reduce_first_slippage = float(_get(
        raw, "execution", "reduce_first_leg_slippage_bps", leg_slippage))
    open_second_slippage = float(_get(
        raw, "execution", "open_second_leg_slippage_bps",
        second_leg_slippage))
    reduce_second_slippage = float(_get(
        raw, "execution", "reduce_second_leg_slippage_bps",
        second_leg_slippage))
    no_fill_retry = float(_get(raw, "execution", "no_fill_retry_sec", 3.0))
    open_no_fill_retry = float(_get(
        raw, "execution", "open_no_fill_retry_sec", no_fill_retry))
    reduce_no_fill_retry = float(_get(
        raw, "execution", "reduce_no_fill_retry_sec", no_fill_retry))
    emergency_slippage = float(_get(
        raw, "execution", "emergency_slippage_bps",
        _get(raw, "execution", "hedge_slippage_bps", 20.0)))
    max_reduce_retries = int(_get(
        raw, "execution", "max_reduce_retry_attempts", 1))
    retry_schedule_raw = _get(
        raw, "execution", "reduce_retry_escalation_bps", [])
    retry_schedule = tuple(float(value) for value in retry_schedule_raw)
    action_specific_execution = any(
        key in (raw.get("execution") or {})
        for key in (
            "open_persist_sec", "reduce_persist_sec", "open_cooldown_sec",
            "reduce_cooldown_sec", "open_first_leg_slippage_bps",
            "reduce_first_leg_slippage_bps", "open_second_leg_slippage_bps",
            "reduce_second_leg_slippage_bps", "open_no_fill_retry_sec",
            "reduce_no_fill_retry_sec", "emergency_slippage_bps",
            "max_reduce_retry_attempts", "reduce_retry_escalation_bps",
            "exit_campaign_sec", "exit_campaign_retry_interval_sec",
            "dynamic_hedge_reserve", "dynamic_open_first_leg",
            "open_retry_campaign_sec", "open_retry_interval_sec",
            "open_retry_max_attempts", "open_execution_room_min_bps",
            "reserve_reduce_slots", "open_first_leg_hard_cap_bps",
            "open_second_leg_hard_cap_bps", "exit_campaign_max_attempts",
            "reopen_improvement_bps", "max_campaigns_per_signal_episode"))
    min_surplus = float(_get(raw, "execution", "min_edge_surplus_bps", 0.5))
    profit_guard = ProfitGuardConfig(
            enabled=bool(_get(raw, "profit_guard", "enabled", False)),
        min_profit_usd=float(_get(raw, "profit_guard", "min_profit_usd", 0.01)),
        entry_buffer_bps=float(_get(raw, "profit_guard", "entry_buffer_bps",
                                   leg_slippage + second_leg_slippage)),
        exit_buffer_bps=float(_get(raw, "profit_guard", "exit_buffer_bps",
                                  leg_slippage + second_leg_slippage)),
        mode=str(_get(raw, "profit_guard", "mode", "hard")),
    )
    if profit_guard.mode not in {"hard", "soft"}:
        raise ConfigError("profit_guard.mode must be 'hard' or 'soft'")
    if any(not math.isfinite(v) or v < 0 for v in (
            profit_guard.min_profit_usd, profit_guard.entry_buffer_bps,
            profit_guard.exit_buffer_bps)):
        raise ConfigError("profit_guard values must be finite and >= 0")
    if (leg_slippage < 0 or second_leg_slippage < 0
            or min_surplus < 0 or premium_persist < 0 or cooldown < 0
            or recovery_timeout <= 0
            or any(value < 0 for value in (
                open_persist, reduce_persist, open_cooldown,
                reduce_cooldown, open_first_slippage, reduce_first_slippage,
                open_second_slippage, reduce_second_slippage,
                no_fill_retry, open_no_fill_retry, reduce_no_fill_retry,
                emergency_slippage))
            or max_reduce_retries < 1
            or any(not math.isfinite(value) or value < 0
                   for value in retry_schedule)):
        raise ConfigError("execution slippage values are outside valid ranges")

    take_fraction = float(_get(raw, "sizing", "take_fraction", 0.5))
    if not 0.0 < take_fraction <= 1.0:
        raise ConfigError("sizing.take_fraction must be in (0, 1] — taking "
                          "more than the profitable depth loses money on the "
                          "tail / 必须在 (0, 1] 之间")

    entropy_dex = _get(raw, "entropy", "dex", "io")
    if hedge_venue == "tradexyz" and entropy_dex == "xyz":
        raise ConfigError("entropy.dex 'xyz' with hedge_venue 'tradexyz' is "
                          "the same market on both legs / 两条腿是同一个市场")

    entropy_leverage = _get(raw, "entropy", "leverage", None)
    hedge_leverage = _get(raw, "hedge", "leverage", None)
    for venue_name, leverage in (("entropy", entropy_leverage),
                                 ("hedge", hedge_leverage)):
        if leverage is not None and not 1 <= leverage <= 100:
            raise ConfigError(
                f"'{venue_name}.leverage' must be an integer in [1, 100]")

    entropy_hl_creds = HLCreds(_env_s("HL_PRIVATE_KEY"),
                               _env_s("HL_ACCOUNT_ADDRESS"))
    entropy = VenueConf(
        key="entropy", kind="hl", label="ENTROPY",
        symbol=symbol,
        fee_bps=float(_get(raw, "entropy", "taker_fee_bps", 0.0)),
        cap_usd=float(_get(raw, "entropy", "max_position_usd", 1000.0)),
        orders_per_min=int(_get(raw, "entropy", "max_orders_per_min", 120)),
        leverage=entropy_leverage,
        hl_dex=entropy_dex,
        hl_creds=entropy_hl_creds,
    )

    if hedge_venue == "tradexyz":
        hedge = VenueConf(
            key="hedge", kind="hl", label="XYZ",
            symbol=symbol,
            fee_bps=float(_get(raw, "hedge", "taker_fee_bps", 1.0)),
            cap_usd=float(_get(raw, "hedge", "max_position_usd", 1000.0)),
            orders_per_min=int(_get(raw, "hedge", "max_orders_per_min", 120)),
            leverage=hedge_leverage,
            hl_dex="xyz",
            hl_creds=HLCreds(
                _env_s("HL_PRIVATE_KEY_XYZ") or _env_s("HL_PRIVATE_KEY"),
                _env_s("HL_ACCOUNT_ADDRESS_XYZ") or _env_s("HL_ACCOUNT_ADDRESS")),
        )
    else:
        hedge = VenueConf(
            key="hedge", kind="lighter",
            label="LIGHTER" if hedge_venue == "lighter" else "RH",
            symbol=symbol,
            fee_bps=float(_get(raw, "hedge", "taker_fee_bps", 0.0)),
            cap_usd=float(_get(raw, "hedge", "max_position_usd", 1000.0)),
            orders_per_min=int(_get(raw, "hedge", "max_orders_per_min", 30)),
            leverage=hedge_leverage,
            lighter_profile=LIGHTER_PROFILES[hedge_venue],
            lighter_creds=LighterCreds(_env_i("LIGHTER_ACCOUNT_INDEX"),
                                       _env_i("LIGHTER_API_KEY_INDEX"),
                                       _env_s("LIGHTER_API_PRIVATE_KEY")),
        )

    request_limit = int(_get(raw, 'lighter_requests', 'limit_per_min', 48))
    request_reserve = int(_get(raw, 'lighter_requests', 'realtime_reserve', 12))
    history_limit = int(_get(raw, 'lighter_requests', 'history_per_min', 6))
    if not (2 <= request_limit <= 60 and 0 <= request_reserve < request_limit
            and 1 <= history_limit <= request_limit - request_reserve):
        raise ConfigError('invalid conservative Lighter request budget (maximum 60/min)')
    hedge.lighter_request_limit = request_limit
    hedge.lighter_realtime_reserve = request_reserve
    hedge.lighter_history_limit = history_limit

    return Config(
        symbol=symbol,
        hedge_venue=hedge_venue,
        entropy=entropy,
        hedge=hedge,
        midline_bps=float(thr["midline_bps"]),
        upper_bps=upper,
        lower_bps=lower,
        exit_bps=exit_bps,
        allow_sell_entropy_open=bool(
            thr.get("allow_sell_entropy_open", True)),
        allow_buy_entropy_open=bool(
            thr.get("allow_buy_entropy_open", True)),
        dynamic_midline_enabled=bool(_get(raw, "dynamic_midline", "enabled", True)),
        dynamic_sample_sec=dynamic_sample_sec,
        dynamic_short_window_sec=dynamic_short,
        dynamic_long_window_sec=dynamic_long,
        dynamic_warmup_sec=dynamic_warmup,
        dynamic_min_samples=dynamic_min_samples,
        dynamic_shift_min_bps=dynamic_shift_min,
        dynamic_shift_sigma_mult=dynamic_shift_mult,
        # Compatibility alias for callers that still read the old single
        # speed field.  New code uses the three explicit speeds below.
        dynamic_max_move_bps_per_10m=dynamic_base,
        dynamic_base_move_bps_per_10m=dynamic_base,
        dynamic_accelerated_move_bps_per_10m=dynamic_accelerated,
        dynamic_absolute_max_move_bps_per_10m=dynamic_absolute,
        dynamic_recovery_ratio=dynamic_recovery_ratio,
        dynamic_recovery_sec=dynamic_recovery_sec,
        premium_model=PremiumModelConfig(
            max_book_age_ms=premium_max_age,
            max_cross_venue_skew_ms=premium_max_skew,
            degraded_after_sec=premium_degraded_after,
        ),
        adaptive_bands=AdaptiveBandsConfig(
            enabled=adaptive_enabled,
            entry_sigma_mult=entry_sigma_mult,
            min_upper_bps=min_upper_bps,
            min_lower_bps=min_lower_bps,
            max_upper_bps=max_upper_bps,
            max_lower_bps=max_lower_bps,
            max_sigma_bps=max_sigma_bps,
            sigma_window_sec=sigma_window_sec,
        ),
        midline_lag_guard=MidlineLagGuardConfig(
            enabled=lag_enabled,
            warn_bps=lag_warn,
            block_bps=lag_block,
            persist_sec=lag_persist,
            accelerate_bps=lag_accelerate,
            accelerate_persist_sec=lag_accelerate_persist,
            recovery_sec=lag_recovery,
            recovery_bps=lag_recovery_bps,
            warning_entry_penalty_bps=lag_warning_penalty,
            block_accelerating=lag_block_accelerating,
            block_blocked=lag_block_blocked,
        ),
        reference_policy=reference_policy,
        cycle_exit=CycleExitConfig(
            enabled=cycle_enabled,
            anchor_mode=cycle_anchor_mode,
            max_midline_drift_bps=cycle_drift,
            reset_on_flat=cycle_reset,
            state_json=cycle_state_json,
            mode=cycle_mode,
            normal_min_profit_usd=cycle_normal_min,
            fast_exit_after_sec=cycle_fast_after,
            fast_exit_min_profit_usd=cycle_fast_min,
            max_hold_sec=cycle_max_hold,
            long_hold_after_sec=cycle_long_hold,
            hard_timeout_enabled=cycle_hard_timeout_enabled,
            hard_timeout_sec=cycle_hard_timeout,
            hard_timeout_min_profit_usd=cycle_hard_timeout_min,
            pnl_loss_stop_enabled=cycle_pnl_loss_stop,
            max_cycle_loss_bps=cycle_max_loss_bps,
            adverse_drift_exit_enabled=cycle_adverse_exit_enabled,
            adverse_drift_enabled=cycle_adverse_enabled,
            adverse_drift_min_age_sec=cycle_adverse_min_age,
            adverse_drift_min_loss_usd=cycle_adverse_min_loss,
            adverse_drift_bps=cycle_adverse_bps,
            adverse_drift_persist_sec=cycle_adverse_persist,
            regime_stop_enabled=cycle_regime_enabled,
            regime_stop_min_age_sec=cycle_regime_min_age,
            regime_stop_min_loss_usd=cycle_regime_min_loss,
            regime_stop_require_midline_warning=cycle_regime_require_lag,
        ),
        reentry_guard=ReentryGuardConfig(
            enabled=reentry_enabled,
            post_risk_exit_cooldown_sec=reentry_cooldown,
            basis_reset_bps=reentry_basis_reset,
            same_direction_only=reentry_same_direction,
        ),
        margin_risk=MarginRiskConfig(
            enabled=margin_enabled,
            warning_available_ratio=margin_warning_ratio,
            critical_available_ratio=margin_critical_ratio,
            on_unavailable=margin_on_unavailable,
        ),
        profit_guard=profit_guard,
        hot_reload=HotReloadConfig(
            enabled=hot_reload_enabled,
            poll_sec=hot_reload_poll_sec,
        ),
        persistence=PersistenceConfig(
            sqlite_path=sqlite_path,
            migrate_legacy=migrate_legacy,
            lock_file=lock_file,
        ),
        wechat_clawbot=WeChatClawBotConfig(
            enabled=wechat_enabled,
            base_url=wechat_base_url,
            interval_sec=wechat_interval,
            poll_timeout_sec=wechat_poll_timeout,
            request_timeout_sec=wechat_request_timeout,
            state_file=wechat_state_file,
            credentials_file=wechat_credentials_file,
            send_startup=wechat_send_startup,
        ),
        # Old configs remain tradable; the complete example explicitly turns
        # the new fail-closed funding gate on.
        funding_enabled=bool(_get(raw, "funding", "enabled", False)),
        funding_poll_sec=funding_poll,
        funding_stale_sec=funding_stale,
        funding_expected_hold_hours=funding_hold,
        funding_max_adverse_cost_bps=funding_max,
        funding_on_unavailable=funding_unavailable,
        take_fraction=take_fraction,
        max_order_notional=float(_get(raw, "sizing", "max_order_notional_usd", 500.0)),
        min_order_notional=float(_get(raw, "sizing", "min_order_notional_usd", 10.0)),
        inventory_scale_bps=float(_get(raw, "inventory", "scale_bps", 10.0)),
        inventory_floor_frac=float(_get(raw, "inventory", "floor_frac", 0.5)),
        cycle_allow_add=cycle_allow_add,
        cycle_max_active_pairs=cycle_max_active_pairs,
        premium_persist_sec=premium_persist,
        cooldown_sec=cooldown,
        settle_timeout_sec=float(_get(raw, "execution", "settle_timeout_sec", 5.0)),
        recovery_timeout_sec=recovery_timeout,
        leg_slippage_bps=leg_slippage,
        second_leg_slippage_bps=second_leg_slippage,
        min_edge_surplus_bps=min_surplus,
        hedge_slippage_bps=float(_get(raw, "execution", "hedge_slippage_bps", 20.0)),
        no_fill_retry_sec=no_fill_retry,
        net_tolerance_base=float(_get(raw, "execution", "net_tolerance_base", 0.001)),
        max_consecutive_errors=int(_get(raw, "execution", "max_consecutive_errors", 3)),
        rate_limit_pause_sec=float(_get(raw, "execution", "rate_limit_pause_sec", 10.0)),
        staleness_sec=float(_get(raw, "execution", "staleness_sec", 10.0)),
        reconcile_sec=float(_get(raw, "execution", "reconcile_sec", 15.0)),
        venue_probe_sec=float(_get(raw, "execution", "venue_probe_sec", 30.0)),
        http_keepalive_sec=float(_get(raw, "execution", "http_keepalive_sec", 10.0)),
        open_persist_sec=open_persist,
        reduce_persist_sec=reduce_persist,
        open_cooldown_sec=open_cooldown,
        reduce_cooldown_sec=reduce_cooldown,
        open_first_leg_slippage_bps=open_first_slippage,
        reduce_first_leg_slippage_bps=reduce_first_slippage,
        open_second_leg_slippage_bps=open_second_slippage,
        reduce_second_leg_slippage_bps=reduce_second_slippage,
        open_no_fill_retry_sec=open_no_fill_retry,
        reduce_no_fill_retry_sec=reduce_no_fill_retry,
        emergency_slippage_bps=emergency_slippage,
        max_reduce_retry_attempts=max_reduce_retries,
        reduce_retry_escalation_bps=retry_schedule,
        action_specific_execution=action_specific_execution,
        recorder_enabled=bool(_get(raw, "recorder", "enabled", True)),
        recorder_csv=_get(raw, "recorder", "csv", "logs/minutes.csv"),
        recorder_seconds_csv=_get(raw, "recorder", "seconds_csv", "logs/seconds.csv"),
        log_level=str(_get(raw, "logging", "level", "INFO")).upper(),
        status_interval_sec=float(_get(raw, "logging", "status_interval_sec", 30.0)),
        trades_csv=_get(raw, "logging", "trades_csv", "logs/trades.csv"),
        dashboard=bool(_get(raw, "logging", "dashboard", True)),
        log_file=_get(raw, "logging", "file", "logs/engine.log"),
        web_dashboard_enabled=bool(
            _get(raw, "dashboard", "enabled", False)),
        dashboard_status_json=str(
            _get(raw, "dashboard", "status_json", "logs/status.json")),
        dashboard_pnl_state_json=str(
            _get(raw, "dashboard", "pnl_state_json", "logs/pnl_state.json")),
        dashboard_api_host=str(
            _get(raw, "dashboard", "api_host", "127.0.0.1")),
        dashboard_api_port=dashboard_port,
        dashboard_snapshot_sec=dashboard_snapshot_sec,
        dashboard_day_timezone=dashboard_timezone,
        reduce_execution=reduce_execution,
        economic_execution=economic_execution,
        bootstrap=bootstrap,
    )


def merge_hot_reloadable_config(
        current: Config, candidate: Config
) -> tuple[Config, tuple[str, ...], tuple[str, ...]]:
    """Apply only safe runtime settings from ``candidate``.

    The YAML file is parsed and fully validated before this function is
    called.  Venue identity/credentials, leverage, sizing caps, inventory
    rules, and fixed order-path safety settings deliberately remain on
    ``current`` and therefore still require a restart.  The bounded OPEN/
    REDUCE campaign and quote-reserve policy is applied here as a safe runtime
    overlay.  Returning the ignored paths lets
    the engine make that distinction visible in the log instead of silently
    pretending every YAML edit was applied.
    """
    changed: list[str] = []

    def take(path: str, old: Any, new: Any) -> Any:
        if old != new:
            changed.append(path)
        return new

    adaptive = replace(
        current.adaptive_bands,
        enabled=take("adaptive_bands.enabled",
                      current.adaptive_bands.enabled,
                      candidate.adaptive_bands.enabled),
        entry_sigma_mult=take("adaptive_bands.entry_sigma_mult",
                              current.adaptive_bands.entry_sigma_mult,
                              candidate.adaptive_bands.entry_sigma_mult),
        min_upper_bps=take("adaptive_bands.min_upper_bps",
                           current.adaptive_bands.min_upper_bps,
                           candidate.adaptive_bands.min_upper_bps),
        min_lower_bps=take("adaptive_bands.min_lower_bps",
                           current.adaptive_bands.min_lower_bps,
                           candidate.adaptive_bands.min_lower_bps),
        max_upper_bps=take("adaptive_bands.max_upper_bps",
                           current.adaptive_bands.max_upper_bps,
                           candidate.adaptive_bands.max_upper_bps),
        max_lower_bps=take("adaptive_bands.max_lower_bps",
                           current.adaptive_bands.max_lower_bps,
                           candidate.adaptive_bands.max_lower_bps),
        max_sigma_bps=take("adaptive_bands.max_sigma_bps",
                           current.adaptive_bands.max_sigma_bps,
                           candidate.adaptive_bands.max_sigma_bps),
        sigma_window_sec=take("adaptive_bands.sigma_window_sec",
                              current.adaptive_bands.sigma_window_sec,
                              candidate.adaptive_bands.sigma_window_sec),
    )
    premium_model = replace(
        current.premium_model,
        max_book_age_ms=take("premium_model.max_book_age_ms",
                             current.premium_model.max_book_age_ms,
                             candidate.premium_model.max_book_age_ms),
        max_cross_venue_skew_ms=take(
            "premium_model.max_cross_venue_skew_ms",
            current.premium_model.max_cross_venue_skew_ms,
            candidate.premium_model.max_cross_venue_skew_ms),
        degraded_after_sec=take("premium_model.degraded_after_sec",
                                current.premium_model.degraded_after_sec,
                                candidate.premium_model.degraded_after_sec),
    )
    lag_guard = replace(
        current.midline_lag_guard,
        enabled=take("midline_lag_guard.enabled",
                      current.midline_lag_guard.enabled,
                      candidate.midline_lag_guard.enabled),
        warn_bps=take("midline_lag_guard.warn_bps",
                       current.midline_lag_guard.warn_bps,
                       candidate.midline_lag_guard.warn_bps),
        block_bps=take("midline_lag_guard.block_bps",
                       current.midline_lag_guard.block_bps,
                       candidate.midline_lag_guard.block_bps),
        persist_sec=take("midline_lag_guard.persist_sec",
                         current.midline_lag_guard.persist_sec,
                         candidate.midline_lag_guard.persist_sec),
        accelerate_bps=take("midline_lag_guard.accelerate_bps",
                            current.midline_lag_guard.accelerate_bps,
                            candidate.midline_lag_guard.accelerate_bps),
        accelerate_persist_sec=take(
            "midline_lag_guard.accelerate_persist_sec",
            current.midline_lag_guard.accelerate_persist_sec,
            candidate.midline_lag_guard.accelerate_persist_sec),
        recovery_sec=take("midline_lag_guard.recovery_sec",
                          current.midline_lag_guard.recovery_sec,
                          candidate.midline_lag_guard.recovery_sec),
        recovery_bps=take("midline_lag_guard.recovery_bps",
                          current.midline_lag_guard.recovery_bps,
                          candidate.midline_lag_guard.recovery_bps),
        warning_entry_penalty_bps=take(
            "midline_lag_guard.warning_entry_penalty_bps",
            current.midline_lag_guard.warning_entry_penalty_bps,
            candidate.midline_lag_guard.warning_entry_penalty_bps),
        block_accelerating=take(
            "midline_lag_guard.block_accelerating",
            current.midline_lag_guard.block_accelerating,
            candidate.midline_lag_guard.block_accelerating),
        block_blocked=take(
            "midline_lag_guard.block_blocked",
            current.midline_lag_guard.block_blocked,
            candidate.midline_lag_guard.block_blocked),
    )
    reference_policy = replace(
        current.reference_policy,
        enabled=take("reference_policy.enabled",
                      current.reference_policy.enabled,
                      candidate.reference_policy.enabled),
        warning_penalty_bps=take(
            "reference_policy.warning_penalty_bps",
            current.reference_policy.warning_penalty_bps,
            candidate.reference_policy.warning_penalty_bps),
        accelerating_mode=take(
            "reference_policy.accelerating.mode",
            current.reference_policy.accelerating_mode,
            candidate.reference_policy.accelerating_mode),
        accelerating_sigma_mult=take(
            "reference_policy.accelerating.sigma_mult",
            current.reference_policy.accelerating_sigma_mult,
            candidate.reference_policy.accelerating_sigma_mult),
        shifting_mode=take(
            "reference_policy.shifting.mode",
            current.reference_policy.shifting_mode,
            candidate.reference_policy.shifting_mode),
        shifting_sigma_mult=take(
            "reference_policy.shifting.sigma_mult",
            current.reference_policy.shifting_sigma_mult,
            candidate.reference_policy.shifting_sigma_mult),
        shifting_min_deviation_bps=take(
            "reference_policy.shifting.min_deviation_bps",
            current.reference_policy.shifting_min_deviation_bps,
            candidate.reference_policy.shifting_min_deviation_bps),
        blocked_mode=take(
            "reference_policy.blocked.mode",
            current.reference_policy.blocked_mode,
            candidate.reference_policy.blocked_mode),
        blocked_sigma_mult=take(
            "reference_policy.blocked.sigma_mult",
            current.reference_policy.blocked_sigma_mult,
            candidate.reference_policy.blocked_sigma_mult),
        blocked_min_deviation_bps=take(
            "reference_policy.blocked.min_deviation_bps",
            current.reference_policy.blocked_min_deviation_bps,
            candidate.reference_policy.blocked_min_deviation_bps),
        extreme_enabled=take(
            "reference_policy.extreme_dislocation.enabled",
            current.reference_policy.extreme_enabled,
            candidate.reference_policy.extreme_enabled),
        extreme_sigma_mult=take(
            "reference_policy.extreme_dislocation.sigma_mult",
            current.reference_policy.extreme_sigma_mult,
            candidate.reference_policy.extreme_sigma_mult),
    )
    current_execution_policy = current.reduce_execution
    candidate_execution_policy = candidate.reduce_execution
    bootstrap_current = getattr(current, "bootstrap", BootstrapConfig())
    bootstrap_candidate = getattr(candidate, "bootstrap", BootstrapConfig())
    execution_policy_fields = (
        "exit_campaign_sec", "exit_campaign_retry_interval_sec",
        "open_retry_campaign_sec", "open_retry_interval_sec",
        "open_retry_max_attempts", "open_execution_room_min_bps",
        "reserve_reduce_slots", "open_attempts_per_min",
        "reduce_attempts_per_min", "recovery_reserve_per_min",
        "open_first_leg_hard_cap_bps",
        "open_second_leg_hard_cap_bps", "exit_campaign_max_attempts",
        "reopen_improvement_bps", "max_campaigns_per_signal_episode")
    for name in execution_policy_fields:
        if getattr(current_execution_policy, name) != getattr(
                candidate_execution_policy, name):
            changed.append(f"execution.{name}")
    for name in ("enabled", "min_bps", "max_bps",
                 "adverse_move_percentile", "safety_margin_bps"):
        if getattr(current_execution_policy.dynamic_hedge_reserve, name) != \
                getattr(candidate_execution_policy.dynamic_hedge_reserve, name):
            changed.append(f"execution.dynamic_hedge_reserve.{name}")
        if getattr(current_execution_policy.dynamic_open_first_leg, name) != \
                getattr(candidate_execution_policy.dynamic_open_first_leg, name):
            changed.append(f"execution.dynamic_open_first_leg.{name}")
    bootstrap_fields = (
        "enabled", "mode", "target_complete_cycles", "max_complete_cycles",
        "min_execution_samples_per_phase", "max_order_notional_usd",
        "max_expected_loss_bps", "max_expected_loss_usd",
        "max_cycle_loss_usd", "cumulative_loss_budget_usd", "auto_disable",
        "min_gross_after_fee_bps", "first_leg_residual_bps",
        "hedge_leg_residual_bps", "empirical_full_weight_samples",
        "dynamic_midline_warmup_sec", "dynamic_midline_min_samples",
        "open_execution_room_min_bps", "open_target_execution_room_bps",
        "open_sampling_execution_cap_bps",
        "open_campaign_sec", "open_campaign_max_attempts",
        "open_retry_interval_sec", "open_hedge_hard_cap_bps",
        "fast_exit_after_sec", "fast_exit_min_profit_usd",
        "sampling_exit_after_sec", "sampling_exit_floor_usd",
        "stale_exit_after_sec", "stale_exit_floor_usd",
        "close_min_execution_room_bps", "close_target_execution_room_bps",
        "close_campaign_sec", "close_campaign_max_attempts",
        "close_retry_interval_sec", "close_hedge_hard_cap_bps",
        "entropy_order_budget_per_min", "hedge_order_budget_per_min",
        "max_campaigns_per_signal_episode", "reopen_improvement_bps",
    )
    for name in bootstrap_fields:
        if getattr(bootstrap_current, name) != getattr(bootstrap_candidate, name):
            changed.append(f"bootstrap.{name}")
    updated = replace(
        current,
        midline_bps=take("thresholds.midline_bps",
                         current.midline_bps, candidate.midline_bps),
        upper_bps=take("thresholds.upper_bps",
                       current.upper_bps, candidate.upper_bps),
        lower_bps=take("thresholds.lower_bps",
                       current.lower_bps, candidate.lower_bps),
        exit_bps=take("thresholds.exit_bps",
                      current.exit_bps, candidate.exit_bps),
        allow_sell_entropy_open=take(
            "thresholds.allow_sell_entropy_open",
            current.allow_sell_entropy_open,
            candidate.allow_sell_entropy_open),
        allow_buy_entropy_open=take(
            "thresholds.allow_buy_entropy_open",
            current.allow_buy_entropy_open,
            candidate.allow_buy_entropy_open),
        dynamic_midline_enabled=take(
            "dynamic_midline.enabled", current.dynamic_midline_enabled,
            candidate.dynamic_midline_enabled),
        dynamic_sample_sec=take(
            "dynamic_midline.sample_sec", current.dynamic_sample_sec,
            candidate.dynamic_sample_sec),
        dynamic_short_window_sec=take(
            "dynamic_midline.short_window_sec",
            current.dynamic_short_window_sec,
            candidate.dynamic_short_window_sec),
        dynamic_long_window_sec=take(
            "dynamic_midline.long_window_sec",
            current.dynamic_long_window_sec,
            candidate.dynamic_long_window_sec),
        dynamic_warmup_sec=take(
            "dynamic_midline.warmup_sec", current.dynamic_warmup_sec,
            candidate.dynamic_warmup_sec),
        dynamic_min_samples=take(
            "dynamic_midline.min_samples", current.dynamic_min_samples,
            candidate.dynamic_min_samples),
        dynamic_shift_min_bps=take(
            "dynamic_midline.shift_min_bps", current.dynamic_shift_min_bps,
            candidate.dynamic_shift_min_bps),
        dynamic_shift_sigma_mult=take(
            "dynamic_midline.shift_sigma_mult",
            current.dynamic_shift_sigma_mult,
            candidate.dynamic_shift_sigma_mult),
        dynamic_max_move_bps_per_10m=take(
            "dynamic_midline.max_move_bps_per_10m",
            current.dynamic_max_move_bps_per_10m,
            candidate.dynamic_max_move_bps_per_10m),
        dynamic_base_move_bps_per_10m=take(
            "dynamic_midline.base_move_bps_per_10m",
            current.dynamic_base_move_bps_per_10m,
            candidate.dynamic_base_move_bps_per_10m),
        dynamic_accelerated_move_bps_per_10m=take(
            "dynamic_midline.accelerated_move_bps_per_10m",
            current.dynamic_accelerated_move_bps_per_10m,
            candidate.dynamic_accelerated_move_bps_per_10m),
        dynamic_absolute_max_move_bps_per_10m=take(
            "dynamic_midline.absolute_max_move_bps_per_10m",
            current.dynamic_absolute_max_move_bps_per_10m,
            candidate.dynamic_absolute_max_move_bps_per_10m),
        dynamic_recovery_ratio=take(
            "dynamic_midline.recovery_ratio", current.dynamic_recovery_ratio,
            candidate.dynamic_recovery_ratio),
        dynamic_recovery_sec=take(
            "dynamic_midline.recovery_sec", current.dynamic_recovery_sec,
            candidate.dynamic_recovery_sec),
        premium_model=premium_model,
        adaptive_bands=adaptive,
        midline_lag_guard=lag_guard,
        reference_policy=reference_policy,
        profit_guard=ProfitGuardConfig(**{
            name: take(f"profit_guard.{name}",
                       getattr(current.profit_guard, name),
                       getattr(candidate.profit_guard, name))
            for name in ("enabled", "min_profit_usd", "entry_buffer_bps",
                         "exit_buffer_bps", "mode")
        }),
        hot_reload=replace(
            current.hot_reload,
            poll_sec=take("hot_reload.poll_sec",
                           current.hot_reload.poll_sec,
                           candidate.hot_reload.poll_sec),
        ),
        premium_persist_sec=take(
            "execution.premium_persist_sec", current.premium_persist_sec,
            candidate.premium_persist_sec),
        cooldown_sec=take("execution.cooldown_sec", current.cooldown_sec,
                          candidate.cooldown_sec),
        min_edge_surplus_bps=take(
            "execution.min_edge_surplus_bps", current.min_edge_surplus_bps,
            candidate.min_edge_surplus_bps),
        reduce_execution=candidate_execution_policy,
        economic_execution=EconomicExecutionConfig(
            min_expected_cycle_profit_bps=take(
                "economic_execution.min_expected_cycle_profit_bps",
                current.economic_execution.min_expected_cycle_profit_bps,
                candidate.economic_execution.min_expected_cycle_profit_bps),
            min_expected_cycle_profit_usd=take(
                "economic_execution.min_expected_cycle_profit_usd",
                current.economic_execution.min_expected_cycle_profit_usd,
                candidate.economic_execution.min_expected_cycle_profit_usd),
            open_first_cost_percentile=take(
                "economic_execution.open_first_cost_percentile",
                current.economic_execution.open_first_cost_percentile,
                candidate.economic_execution.open_first_cost_percentile),
            open_hedge_cost_percentile=take(
                "economic_execution.open_hedge_cost_percentile",
                current.economic_execution.open_hedge_cost_percentile,
                candidate.economic_execution.open_hedge_cost_percentile),
            close_first_cost_percentile=take(
                "economic_execution.close_first_cost_percentile",
                current.economic_execution.close_first_cost_percentile,
                candidate.economic_execution.close_first_cost_percentile),
            close_hedge_cost_percentile=take(
                "economic_execution.close_hedge_cost_percentile",
                current.economic_execution.close_hedge_cost_percentile,
                candidate.economic_execution.close_hedge_cost_percentile),
            safety_margin_bps=take(
                "economic_execution.safety_margin_bps",
                current.economic_execution.safety_margin_bps,
                candidate.economic_execution.safety_margin_bps),
        ),
        bootstrap=BootstrapConfig(
            enabled=take("bootstrap.enabled", bootstrap_current.enabled,
                          bootstrap_candidate.enabled),
            mode=take("bootstrap.mode", bootstrap_current.mode,
                       bootstrap_candidate.mode),
            target_complete_cycles=take(
                "bootstrap.target_complete_cycles",
                bootstrap_current.target_complete_cycles,
                bootstrap_candidate.target_complete_cycles),
            max_complete_cycles=take(
                "bootstrap.max_complete_cycles",
                bootstrap_current.max_complete_cycles,
                bootstrap_candidate.max_complete_cycles),
            min_execution_samples_per_phase=take(
                "bootstrap.min_execution_samples_per_phase",
                bootstrap_current.min_execution_samples_per_phase,
                bootstrap_candidate.min_execution_samples_per_phase),
            max_order_notional_usd=take(
                "bootstrap.max_order_notional_usd",
                bootstrap_current.max_order_notional_usd,
                bootstrap_candidate.max_order_notional_usd),
            max_expected_loss_bps=take(
                "bootstrap.max_expected_loss_bps",
                bootstrap_current.max_expected_loss_bps,
                bootstrap_candidate.max_expected_loss_bps),
            max_expected_loss_usd=take(
                "bootstrap.max_expected_loss_usd",
                bootstrap_current.max_expected_loss_usd,
                bootstrap_candidate.max_expected_loss_usd),
            max_cycle_loss_usd=take(
                "bootstrap.max_cycle_loss_usd",
                bootstrap_current.max_cycle_loss_usd,
                bootstrap_candidate.max_cycle_loss_usd),
            cumulative_loss_budget_usd=take(
                "bootstrap.cumulative_loss_budget_usd",
                bootstrap_current.cumulative_loss_budget_usd,
                bootstrap_candidate.cumulative_loss_budget_usd),
            auto_disable=take("bootstrap.auto_disable",
                               bootstrap_current.auto_disable,
                               bootstrap_candidate.auto_disable),
            min_gross_after_fee_bps=take(
                "bootstrap.min_gross_after_fee_bps",
                bootstrap_current.min_gross_after_fee_bps,
                bootstrap_candidate.min_gross_after_fee_bps),
            first_leg_residual_bps=take(
                "bootstrap.first_leg_residual_bps",
                bootstrap_current.first_leg_residual_bps,
                bootstrap_candidate.first_leg_residual_bps),
            hedge_leg_residual_bps=take(
                "bootstrap.hedge_leg_residual_bps",
                bootstrap_current.hedge_leg_residual_bps,
                bootstrap_candidate.hedge_leg_residual_bps),
            empirical_full_weight_samples=take(
                "bootstrap.empirical_full_weight_samples",
                bootstrap_current.empirical_full_weight_samples,
                bootstrap_candidate.empirical_full_weight_samples),
            dynamic_midline_warmup_sec=take(
                "bootstrap.dynamic_midline_warmup_sec",
                bootstrap_current.dynamic_midline_warmup_sec,
                bootstrap_candidate.dynamic_midline_warmup_sec),
            dynamic_midline_min_samples=take(
                "bootstrap.dynamic_midline_min_samples",
                bootstrap_current.dynamic_midline_min_samples,
                bootstrap_candidate.dynamic_midline_min_samples),
            open_execution_room_min_bps=take(
                "bootstrap.open_execution_room_min_bps",
                bootstrap_current.open_execution_room_min_bps,
                bootstrap_candidate.open_execution_room_min_bps),
            open_target_execution_room_bps=take(
                "bootstrap.open_target_execution_room_bps",
                bootstrap_current.open_target_execution_room_bps,
                bootstrap_candidate.open_target_execution_room_bps),
            open_sampling_execution_cap_bps=take(
                "bootstrap.open_sampling_execution_cap_bps",
                bootstrap_current.open_sampling_execution_cap_bps,
                bootstrap_candidate.open_sampling_execution_cap_bps),
            open_campaign_sec=take(
                "bootstrap.open_campaign_sec",
                bootstrap_current.open_campaign_sec,
                bootstrap_candidate.open_campaign_sec),
            open_campaign_max_attempts=take(
                "bootstrap.open_campaign_max_attempts",
                bootstrap_current.open_campaign_max_attempts,
                bootstrap_candidate.open_campaign_max_attempts),
            open_retry_interval_sec=take(
                "bootstrap.open_retry_interval_sec",
                bootstrap_current.open_retry_interval_sec,
                bootstrap_candidate.open_retry_interval_sec),
            open_hedge_hard_cap_bps=take(
                "bootstrap.open_hedge_hard_cap_bps",
                bootstrap_current.open_hedge_hard_cap_bps,
                bootstrap_candidate.open_hedge_hard_cap_bps),
            fast_exit_after_sec=take(
                "bootstrap.fast_exit_after_sec",
                bootstrap_current.fast_exit_after_sec,
                bootstrap_candidate.fast_exit_after_sec),
            fast_exit_min_profit_usd=take(
                "bootstrap.fast_exit_min_profit_usd",
                bootstrap_current.fast_exit_min_profit_usd,
                bootstrap_candidate.fast_exit_min_profit_usd),
            sampling_exit_after_sec=take(
                "bootstrap.sampling_exit_after_sec",
                bootstrap_current.sampling_exit_after_sec,
                bootstrap_candidate.sampling_exit_after_sec),
            sampling_exit_floor_usd=take(
                "bootstrap.sampling_exit_floor_usd",
                bootstrap_current.sampling_exit_floor_usd,
                bootstrap_candidate.sampling_exit_floor_usd),
            stale_exit_after_sec=take(
                "bootstrap.stale_exit_after_sec",
                bootstrap_current.stale_exit_after_sec,
                bootstrap_candidate.stale_exit_after_sec),
            stale_exit_floor_usd=take(
                "bootstrap.stale_exit_floor_usd",
                bootstrap_current.stale_exit_floor_usd,
                bootstrap_candidate.stale_exit_floor_usd),
            close_min_execution_room_bps=take(
                "bootstrap.close_min_execution_room_bps",
                bootstrap_current.close_min_execution_room_bps,
                bootstrap_candidate.close_min_execution_room_bps),
            close_target_execution_room_bps=take(
                "bootstrap.close_target_execution_room_bps",
                bootstrap_current.close_target_execution_room_bps,
                bootstrap_candidate.close_target_execution_room_bps),
            close_campaign_sec=take(
                "bootstrap.close_campaign_sec",
                bootstrap_current.close_campaign_sec,
                bootstrap_candidate.close_campaign_sec),
            close_campaign_max_attempts=take(
                "bootstrap.close_campaign_max_attempts",
                bootstrap_current.close_campaign_max_attempts,
                bootstrap_candidate.close_campaign_max_attempts),
            close_retry_interval_sec=take(
                "bootstrap.close_retry_interval_sec",
                bootstrap_current.close_retry_interval_sec,
                bootstrap_candidate.close_retry_interval_sec),
            close_hedge_hard_cap_bps=take(
                "bootstrap.close_hedge_hard_cap_bps",
                bootstrap_current.close_hedge_hard_cap_bps,
                bootstrap_candidate.close_hedge_hard_cap_bps),
            entropy_order_budget_per_min=take(
                "bootstrap.entropy_order_budget_per_min",
                bootstrap_current.entropy_order_budget_per_min,
                bootstrap_candidate.entropy_order_budget_per_min),
            hedge_order_budget_per_min=take(
                "bootstrap.hedge_order_budget_per_min",
                bootstrap_current.hedge_order_budget_per_min,
                bootstrap_candidate.hedge_order_budget_per_min),
            max_campaigns_per_signal_episode=take(
                "bootstrap.max_campaigns_per_signal_episode",
                bootstrap_current.max_campaigns_per_signal_episode,
                bootstrap_candidate.max_campaigns_per_signal_episode),
            reopen_improvement_bps=take(
                "bootstrap.reopen_improvement_bps",
                bootstrap_current.reopen_improvement_bps,
                bootstrap_candidate.reopen_improvement_bps),
        ),
    )

    ignored: list[str] = []
    for section in ("entropy", "hedge", "cycle_exit", "reentry_guard",
                    "margin_risk"):
        if getattr(current, section) != getattr(candidate, section):
            ignored.append(section)
    if (current.cycle_allow_add != candidate.cycle_allow_add
            or current.cycle_max_active_pairs
            != candidate.cycle_max_active_pairs):
        ignored.append("cycle")
    non_hot_groups = {
        "sizing": ("take_fraction", "max_order_notional",
                   "min_order_notional"),
        "inventory": ("inventory_scale_bps", "inventory_floor_frac"),
        "funding": ("funding_enabled", "funding_poll_sec",
                    "funding_stale_sec", "funding_expected_hold_hours",
                    "funding_max_adverse_cost_bps",
                    "funding_on_unavailable"),
        "recorder": ("recorder_enabled", "recorder_csv",
                      "recorder_seconds_csv"),
        "dashboard": ("web_dashboard_enabled", "dashboard_status_json",
                       "dashboard_pnl_state_json", "dashboard_api_host",
                       "dashboard_api_port", "dashboard_snapshot_sec",
                       "dashboard_day_timezone"),
        "wechat_clawbot": ("wechat_clawbot",),
        "logging": ("log_level", "status_interval_sec", "trades_csv",
                     "dashboard", "log_file"),
    }
    for section, names in non_hot_groups.items():
        if any(getattr(current, name) != getattr(candidate, name)
               for name in names):
            ignored.append(section)
    for name in ("symbol", "hedge_venue", "hl_api_url", "hl_ws_url"):
        if getattr(current, name) != getattr(candidate, name):
            ignored.append(name)
    for name in (
            "settle_timeout_sec", "recovery_timeout_sec", "leg_slippage_bps",
            "second_leg_slippage_bps", "hedge_slippage_bps",
            "no_fill_retry_sec", "net_tolerance_base",
            "max_consecutive_errors", "rate_limit_pause_sec",
            "staleness_sec", "reconcile_sec", "venue_probe_sec",
            "http_keepalive_sec", "open_persist_sec", "reduce_persist_sec",
            "open_cooldown_sec", "reduce_cooldown_sec",
            "open_first_leg_slippage_bps",
            "reduce_first_leg_slippage_bps",
            "open_second_leg_slippage_bps",
            "reduce_second_leg_slippage_bps",
            "open_no_fill_retry_sec", "reduce_no_fill_retry_sec",
            "emergency_slippage_bps", "max_reduce_retry_attempts",
            "reduce_retry_escalation_bps"):
        if getattr(current, name) != getattr(candidate, name):
            ignored.append(f"execution.{name}")
    if current.hot_reload.enabled != candidate.hot_reload.enabled:
        ignored.append("hot_reload.enabled")
    if current.persistence != candidate.persistence:
        ignored.append("persistence")

    return updated, tuple(changed), tuple(ignored)
