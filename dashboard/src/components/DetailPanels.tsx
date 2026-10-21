import { number, signed, statusLabel } from "../format";
import type { ReentryGuardSnapshot, StatusResponse } from "../types";

function Row({ label, value, tone }: { label: string; value: string; tone?: "warn" | "ok" }) {
  return <div className="detail-row"><span>{label}</span><strong className={tone ? `tone-${tone}` : ""}>{value}</strong></div>;
}

export function DetailPanels({ status }: { status: StatusResponse }) {
  const s = status.strategy;
  const openLagBlocked = s.open_lag_blocked ?? s.midline_lag_blocked;
  const openState = s.open_state ?? status.open_state
    ?? (status.risk.open_blocked ? "hard_blocked" : "allowed");
  const openReason = s.open_block_reason ?? status.risk.open_block_reason;
  const hardBlocked = openState === "hard_blocked"
    || status.risk.open_block_class === "hard";
  const softRestricted = openState === "soft_restricted"
    || status.risk.open_block_class === "soft";
  const referenceState = s.reference_state ?? s.midline_lag_state;
  const strategyTag = status.config_restart_required
    ? "配置待重启"
    : hardBlocked
      ? `OPEN 硬阻断 · ${openReasonLabel(openReason)}`
      : softRestricted
        ? `OPEN 软限制 · ${referenceStateLabel(referenceState)}`
        : s.cycle_drift_guard
          ? "周期漂移 · 禁止加仓"
          : s.cycle_active && !s.cycle_anchored
            ? "周期未锚定 · 禁止加仓"
            : "OPEN 允许";
  const strategyTagClass = hardBlocked || softRestricted
    || s.cycle_drift_guard || (s.cycle_active && !s.cycle_anchored)
    || status.config_restart_required ? "tag warn" : "tag";
  const execution = status.execution;
  const reconciliation = status.reconciliation;
  const openExecution = status.open_execution;
  const openTelemetry = openExecution?.telemetry ?? {};
  const openPolicy = status.execution_policy.dynamic_open_first_leg;
  const costModel = status.execution_costs;
  const openFirstCost = costModel?.phases?.open_first;
  const openHedgeCost = costModel?.phases?.open_hedge;
  const openFirstP75 = openFirstCost?.p75_incremental_execution_cost_bps ?? openFirstCost?.p75_cost_bps;
  const openFirstP90 = openFirstCost?.p90_incremental_execution_cost_bps ?? openFirstCost?.p90_cost_bps;
  const openHedgeP75 = openHedgeCost?.p75_incremental_execution_cost_bps ?? openHedgeCost?.p75_cost_bps;
  const openHedgeP90 = openHedgeCost?.p90_incremental_execution_cost_bps ?? openHedgeCost?.p90_cost_bps;
  const completionState = execution?.hedge_completion_state ?? "not_started";
  const openCampaigns = Number(openTelemetry.open_opportunity_count ?? 0);
  const openFills = Number(openTelemetry.open_campaign_fill_count ?? 0);
  const openRate = openCampaigns > 0 ? openFills / openCampaigns : null;
  const modelTradable = s.dynamic_midline_tradable ?? s.dynamic_midline_ready;
  const modelReadiness = readinessLabel(s.dynamic_midline_readiness, modelTradable);
  const modelNormal = s.dynamic_midline_readiness == null
    ? s.dynamic_midline_ready
    : s.dynamic_midline_readiness === "NORMAL";
  const premiumDataInvalid = s.premium_data_invalid
    ?? (s.premium_data_tier === "INVALID"
      || (s.premium_data_tier == null && s.premium_data_degraded));
  const premiumSampleRejected = s.premium_sample_valid === false;
  const premiumDataTier = premiumTierLabel(
    s.premium_data_tier, premiumDataInvalid, s.premium_sample_valid);
  const premiumDataUncertainty = s.premium_data_uncertainty_bps ?? 0;
  return <section className="details-grid">
    <article className="detail-panel">
      <div className="panel-head"><h2>策略阈值</h2><span className={strategyTagClass}>{strategyTag}</span></div>
      <div className="threshold-map" aria-label="策略阈值">
        <div><span>做多入场</span><strong>{signed(s.long_entry_bps)}</strong></div>
        <div><span>做多离场</span><strong>{signed(s.long_exit_bps)}</strong></div>
        <div className="datum"><span>动态中枢</span><strong>{signed(s.active_midline_bps)}</strong></div>
        <div><span>做空离场</span><strong>{signed(s.short_exit_bps)}</strong></div>
        <div><span>做空入场</span><strong>{signed(s.short_entry_bps)}</strong></div>
      </div>
      <Row label="动态模型准备度" value={`${modelReadiness} · ${modelTradable ? "可交易" : "OPEN 硬门控"}`} tone={modelTradable && modelNormal ? "ok" : "warn"} />
      <Row label="模型历史 / 最新样本" value={`${number(s.dynamic_midline_history_sec, 0)} / ${number(s.dynamic_midline_latest_sample_age_sec, 1)} 秒 · ${number(s.dynamic_midline_history_samples, 0)} 样本`} tone={modelTradable ? undefined : "warn"} />
      <Row label="短中枢 / 长中枢" value={`${signed(s.short_center_bps)} / ${signed(s.long_center_bps)} 基点`} />
      <Row label="中枢延迟" value={`${signed(s.midline_lag_bps)} 基点 · ${statusLabel(s.midline_lag_state)}`} tone={s.midline_lag_state === "normal" ? undefined : "warn"} />
      <Row label="OPEN 状态" value={openStateLabel(openState, openReason)} tone={hardBlocked || softRestricted ? "warn" : "ok"} />
      <Row label="参考状态 / 模式" value={`${referenceStateLabel(referenceState)} · ${referenceModeLabel(s.entry_reference_mode)}`} tone={referenceState === "normal" ? undefined : "warn"} />
      <Row label="入场参考" value={signed(s.entry_reference_midline_bps)} tone={s.entry_reference_mode !== "active_midline" && s.entry_reference_mode !== "configured_midline" ? "warn" : undefined} />
      <Row label="参考偏离（诊断）" value={`${signed(s.signal_deviation_bps ?? s.blocked_entry_deviation_bps)} / ${signed(s.required_deviation_bps ?? s.blocked_entry_required_bps)} 基点`} tone={s.open_lag_blocked ? "warn" : undefined} />
      <Row label="参考置信度 / 不确定性" value={`${number(s.reference_confidence * 100, 0)}% · ${number(s.reference_uncertainty_bps ?? 0)} + ${number(s.model_uncertainty_bps ?? 0)} 基点`} tone={s.reference_confidence < 1 || (s.reference_uncertainty_bps ?? 0) > 0 || (s.model_uncertainty_bps ?? 0) > 0 ? "warn" : undefined} />
      <Row label="再入场 / 保证金不确定性" value={`${number(s.reentry_uncertainty_bps ?? s.reentry_guard.uncertainty_bps ?? 0)} / ${number(s.margin_uncertainty_bps ?? 0)} 基点`} tone={(s.reentry_uncertainty_bps ?? 0) > 0 || (s.margin_uncertainty_bps ?? 0) > 0 ? "warn" : undefined} />
      <Row label="极端偏离通道" value={s.extreme_dislocation ? `已触发 · ≥ ${signed(s.extreme_threshold_bps)} 基点` : `未触发 · ${number((s.required_sigma_mult ?? 0), 1)}σ 要求`} tone={s.extreme_dislocation ? "ok" : undefined} />
      <Row label="中枢移动速度" value={`${number(s.midline_move_speed_bps_per_10m)} 基点 / 10 分钟`} />
      <Row label="sigma 中心 / 窗口" value={`${signed(s.sigma_center_bps)} / ${s.sigma_window_sec == null ? "--" : `${number(s.sigma_window_sec / 60, 0)} 分钟`}`} />
      <Row label="稳健标准差" value={`${number(s.robust_sigma_bps)} 基点`} />
      <Row label="自适应入场带 · 空 / 多" value={`${number(s.effective_upper_bps)} / ${number(s.effective_lower_bps)} 基点`} tone={s.volatility_guard ? "warn" : undefined} />
      <Row label="OPEN 入场带 · 空 / 多" value={`${number(s.open_upper_bps ?? s.effective_upper_bps)} / ${number(s.open_lower_bps ?? s.effective_lower_bps)} 基点`} />
      <Row label="有效入场余量 · 空 / 多" value={`${signed(s.effective_open_surplus_bps.sell_entropy)} / ${signed(s.effective_open_surplus_bps.buy_entropy)} 基点`} />
      <Row label="距入场阈值 · 空 / 多" value={`${signed(s.distance_to_threshold_bps?.sell_entropy ?? s.effective_open_surplus_bps.sell_entropy)} / ${signed(s.distance_to_threshold_bps?.buy_entropy ?? s.effective_open_surplus_bps.buy_entropy)} 基点`} />
      <Row label="风险退出再入场" value={reentryLabel(s.reentry_guard)} tone={s.reentry_guard.blocked ? "warn" : undefined} />
      <Row label="周期入场中枢" value={s.cycle_anchored ? signed(s.cycle_entry_midline_bps) : s.cycle_active ? "未锚定" : "--"} tone={s.cycle_active && !s.cycle_anchored ? "warn" : undefined} />
      <Row label="当前中枢漂移" value={`${signed(s.cycle_midline_drift_bps)} 基点`} tone={s.cycle_drift_guard ? "warn" : undefined} />
      <Row label="持仓退出状态" value={holdStateLabel(s.cycle_hold_state)} tone={s.cycle_hold_state ? "warn" : undefined} />
      <Row label="退出规则" value={`正常 ≥ 0 · 快速 ≥ ${number(s.cycle_exit_fast_after_sec, 0)}秒 / ${number(s.cycle_exit_fast_min_profit_usd, 2)}`} />
      <Row label="退出重试窗口" value={`${number(status.execution_policy.exit_campaign_sec, 1)} 秒 · 每 ${number(status.execution_policy.exit_campaign_retry_interval_sec, 2)} 秒重报价`} />
      <Row label="动态对冲预留" value={reserveLabel(status.execution_policy.dynamic_hedge_reserve)} />
      <Row label="动态开仓首腿" value={openFirstLegLabel(openPolicy)} />
      <Row label="开仓执行余量" value={`${number(status.execution_policy.open_execution_room_min_bps)} 基点 · 预留 ${number(status.execution_policy.reserve_reduce_slots, 0)} 个减仓位`} />
      <Row label="开仓机会 / 窗口成交率" value={`${number(openCampaigns, 0)} / ${openRate == null ? "--" : `${number(openRate * 100, 1)}%`}`} />
      <Row label="首腿延迟 / 经济不利波动 P75" value={`${number((openExecution?.latency_estimate_sec ?? null), 2)} 秒 / ${number(openExecution?.adverse_move_economic_p75_bps ?? null)} 基点`} />
      <Row label="物理压力不利波动 P95 / P99" value={`${number(openExecution?.adverse_move_stress_p95_bps ?? openExecution?.adverse_move_p90_bps ?? null)} / ${number(openExecution?.adverse_move_stress_p99_bps ?? null)} 基点`} />
      <Row label="物理硬限价 · 首腿 / 对冲" value={`${number(status.execution_policy.open_first_leg_hard_cap_bps)} / ${number(status.execution_policy.open_second_leg_hard_cap_bps)} 基点`} />
      <Row label="经济成本模型样本" value={`${number(costModel?.observation_count, 0)} 条${costModel?.ready ? " · 已就绪" : " · 使用保守回退"}`} tone={costModel?.ready ? "ok" : "warn"} />
      <Row label="开仓增量成本 P75 / P90 · 首腿" value={`${number(openFirstP75)} / ${number(openFirstP90)} 基点`} />
      <Row label="开仓增量成本 P75 / P90 · 对冲" value={`${number(openHedgeP75)} / ${number(openHedgeP90)} 基点`} />
      <Row label="成本模型来源 · 首腿" value={`${costSourceLabel(openFirstCost?.source)} · 实测权重 ${number((openFirstCost?.empirical_weight ?? 0) * 100, 0)}%`} />
      <Row label="成本模型来源 · 对冲" value={`${costSourceLabel(openHedgeCost?.source)} · 实测权重 ${number((openHedgeCost?.empirical_weight ?? 0) * 100, 0)}%`} />
      <Row label="执行生命周期" value={lifecycleLabel(status.lifecycle)} tone={status.lifecycle === "flat" || status.lifecycle === "hedged" ? undefined : "warn"} />
    </article>
    <article className="detail-panel market-panel">
      <div className="panel-head"><h2>实时市场</h2><span className={`tag ${status.funding.stale ? "warn" : ""}`}>{status.funding.stale ? "资金费率已过期" : "盘口实时"}</span></div>
      <div className="book-head"><span>交易场所</span><span>买一价</span><span>卖一价</span></div>
      <div className="book-row"><strong>{status.venues.entropy}</strong><span>{number(status.market.entropy_bid, 4)}</span><span>{number(status.market.entropy_ask, 4)}</span></div>
      <div className="book-row"><strong>{status.venues.hedge}</strong><span>{number(status.market.hedge_bid, 4)}</span><span>{number(status.market.hedge_ask, 4)}</span></div>
      <Row label="卖出套利空间" value={`${signed(status.market.sell_edge_bps)} 基点`} />
      <Row label="买入套利空间" value={`${signed(status.market.buy_edge_bps)} 基点`} />
      <Row label="盘口年龄 · Entropy / 对冲" value={`${number(s.entropy_book_age_ms, 0)} / ${number(s.hedge_book_age_ms, 0)} 毫秒`} tone={premiumDataInvalid || premiumDataTier !== "GOOD" ? "warn" : undefined} />
      <Row label="跨市场时间差" value={`${number(s.cross_venue_skew_ms, 0)} 毫秒`} tone={s.premium_sample_valid === false ? "warn" : undefined} />
      <Row label="溢价样本质量" value={`${premiumDataTier} · 不确定性 ${number(premiumDataUncertainty)} 基点`} tone={premiumDataInvalid || premiumDataTier !== "GOOD" ? "warn" : "ok"} />
      <Row label="数据健康 / 无效持续" value={`${premiumDataInvalid ? "INVALID · 硬门控" : premiumSampleRejected ? "样本拒绝" : "可用"} · ${number(s.premium_data_invalid_duration_sec ?? 0, 1)} 秒`} tone={premiumDataInvalid || premiumSampleRejected ? "warn" : undefined} />
      <Row label="资金费成本 · 做空 / 做多" value={`${signed(status.funding.short_entropy_cost_bps)} / ${signed(status.funding.long_entropy_cost_bps)} 基点`} tone={status.funding.stale ? "warn" : undefined} />
      <Row label="保证金风险" value={marginLabel(status.risk.margin_state, status.risk.margin_available_ratio)} tone={status.risk.margin_state === "safe" ? "ok" : "warn"} />
    </article>
    <article className="detail-panel execution-detail-panel">
      <div className="panel-head"><div><h2>执行状态与仓位对账</h2><p className="panel-subtitle">第一腿成交后持续追踪同一 execution，不会重新开一笔第一腿。</p></div><span className={`tag ${reconciliation?.status !== "synced" ? "warn" : ""}`}>{reconciliation ? reconcileLabel(reconciliation.status) : "等待对账"}</span></div>
      {execution ? <>
        <div className="execution-id"><span>执行编号</span><code>{execution.execution_id}</code><b>{statusLabel(execution.phase)}</b></div>
        <div className="leg-status-grid">
          <ExecutionLeg title="第一腿 · Entropy" leg={execution.first} />
          <ExecutionLeg title="持续对冲 · 对冲所" leg={execution.hedge} />
        </div>
        <Row label="待处理订单" value={execution.pending_leg ? `${execution.pending_leg} · ${execution.pending_order_ref ?? "无引用"}` : "无"} tone={execution.pending_leg ? "warn" : undefined} />
        <Row label="剩余待对冲数量 / 重试" value={`${number(execution.remaining_hedge_qty, 4)} / ${number(execution.retry_count, 0)}`} tone={execution.remaining_hedge_qty > 0 ? "warn" : undefined} />
        <Row label="对冲完成状态" value={completionLabel(completionState)} tone={completionState === "position_covered_confirmed" || completionState === "flat" ? "ok" : "warn"} />
        <Row label="恢复状态 / 退避" value={`${statusLabel(execution.recovery_status ?? "normal")} · ${number(execution.recovery_backoff_sec, 1)} 秒 · 第 ${number(execution.recovery_attempt, 0)} 次`} tone={execution.recovery_status && execution.recovery_status !== "normal" ? "warn" : undefined} />
        {execution.exit_campaign && <Row label="当前退出机会" value={campaignLabel(execution.exit_campaign)} tone="warn" />}
        {execution.open_campaign && <Row label="当前开仓机会" value={openCampaignLabel(execution.open_campaign)} tone="warn" />}
        {execution.recovery_next_retry_ts && <Row label="下一次恢复" value={new Date(execution.recovery_next_retry_ts * 1000).toLocaleTimeString()} tone="warn" />}
        {execution.hedge_reserve_breach && <Row label="对冲成本储备" value="已突破，仍优先完成减仓对冲" tone="warn" />}
        {execution.first_topup_count > 0 && <Row label="第一腿最低名义量补量" value={`${number(execution.first_topup_count, 0)} 次 · 继续聚合后对冲`} tone="warn" />}
        {execution.last_error && <Row label="最近错误" value={execution.last_error} tone="warn" />}
      </> : <div className="execution-idle"><span />当前没有未完成的双腿执行。</div>}
      {reconciliation && <div className="reconcile-table">
        {Object.entries(reconciliation.venues).map(([key, venue]) => <div key={key}><span>{key}</span><small>{number(venue.local_base, 4)} → {number(venue.exchange_base, 4)}</small><b className={Math.abs(venue.delta_base ?? 0) > status.risk.net_tolerance_base ? "tone-warn" : "tone-ok"}>{signed(venue.delta_base, 4)}</b></div>)}
      </div>}
    </article>
  </section>;
}

function ExecutionLeg({ title, leg }: { title: string; leg: NonNullable<StatusResponse["execution"]>["first"] }) {
  const ratio = leg.planned_qty > 0 ? leg.filled_qty / leg.planned_qty : 0;
  return <div className="execution-leg"><div><span>{title}</span><b>{statusLabel(leg.status ?? "待发送")}</b></div><strong>{number(leg.filled_qty, 4)} / {number(leg.planned_qty, 4)}</strong><small>{statusLabel(leg.side)} · 均价 {number(leg.avg_px, 4)} · {ratio >= .9999 ? "已足量" : ratio > 0 ? "部分成交" : "未成交"}</small></div>;
}

function completionLabel(value: string): string {
  return ({
    not_started: "未开始",
    hedge_pending: "等待对冲",
    hedge_order_unknown: "对冲订单未知，禁止重复下单",
    position_covered_unanchored: "仓位已覆盖，等待成交事实",
    position_covered_confirmed: "仓位已覆盖且已确认",
    flat: "两边已空仓",
  } as Record<string, string>)[value] ?? value;
}

function openStateLabel(value: string, reason?: string | null) {
  if (value === "hard_blocked") return `硬阻断 · ${openReasonLabel(reason)}`;
  if (value === "soft_restricted") return `软限制 · ${openReasonLabel(reason)}`;
  return "允许";
}

function referenceStateLabel(value?: string | null) {
  const map: Record<string, string> = {
    normal: "正常",
    warning: "预警",
    cold_degraded: "冷启动降级",
    accelerating: "加速追踪",
    shifting: "中枢切换",
    blocked: "参考阻塞",
  };
  return map[value ?? "normal"] ?? value ?? "正常";
}

function referenceModeLabel(value?: string | null) {
  const map: Record<string, string> = {
    configured_midline: "配置中枢",
    active_midline: "活动中枢",
    blended_midline: "混合中枢",
    dynamic_center: "动态中心",
  };
  return map[value ?? ""] ?? value ?? "未知";
}

function readinessLabel(value?: string | null, tradable = false) {
  const map: Record<string, string> = {
    UNAVAILABLE: "不可用",
    COLD_DEGRADED: "冷启动降级",
    NORMAL: "正常",
  };
  return map[value ?? ""] ?? (tradable ? "可用" : "不可用");
}

function premiumTierLabel(
  value?: string,
  invalid = false,
  sampleValid?: boolean | null,
) {
  if (invalid || value === "INVALID" || sampleValid === false) {
    if (sampleValid === false && !invalid && value == null) return "已拒绝";
    return "INVALID · 硬边界";
  }
  if (value === "DEGRADED") return "DEGRADED · 可用";
  if (value === "GOOD" || sampleValid === true) return "GOOD";
  return "--";
}

function openReasonLabel(value?: string | null) {
  const map: Record<string, string> = {
    unbalanced: "双腿不平衡",
    execution_recovery: "执行恢复中",
    reconciliation_pending: "对账未完成",
    premium_data_degraded: "溢价数据降级",
    dynamic_midline_warmup: "模型预热中",
    dynamic_midline_validation: "模型校验中",
    reference_invalid: "参考无效",
    reference_deviation: "偏离不足",
    margin_risk: "保证金风险",
    book_stale: "盘口过期",
    cross_skew: "跨市场延迟",
    venue_degraded: "交易场所降级",
    order_budget: "订单预算",
    volatility_guard: "波动率保护",
    funding_stale: "资金费率过期",
    funding_too_expensive: "资金费过高",
    reentry_guard: "再入场保护",
    cycle_midline_drift: "周期中枢漂移",
    cycle_unanchored: "周期未锚定",
    no_executable_edge: "可执行边不足",
    protected_edge: "保护边不足",
    opportunity_locked: "机会锁定",
    expected_pnl_negative: "预期盈亏为负",
    expected_cycle_pnl_below_floor: "预期盈亏低于底线",
    economic_bound_not_marketable: "经济保护价不可成交",
    open_execution_room_below_min: "开仓执行余量不足",
    protected_open_edge_below_threshold: "保护边低于阈值",
    halted: "引擎已暂停",
  };
  return value ? map[value] ?? statusLabel(value) : "当前条件";
}

function reconcileLabel(value: string) {
  const map: Record<string, string> = {
    synced: "已对账", syncing: "对账中", recovering: "恢复中",
    recovery_pending: "等待恢复", blocked: "已阻断", error: "对账异常",
  };
  return map[value] ?? value;
}

function lifecycleLabel(value: string) {
  const map: Record<string, string> = {
    flat: "FLAT · 空仓", opening: "OPENING · 开仓中",
    hedging: "HEDGING · 对冲中", hedged: "HEDGED · 双腿完成",
    reducing: "REDUCING · 减仓中", recovering: "RECOVERING · 恢复中",
    emergency: "EMERGENCY · 紧急平仓",
    loss_recovery_hold: "LOSS RECOVERY · 等待亏损恢复",
    long_hold: "LONG HOLD · 长持观察",
    forced_release_pending: "HARD TIMEOUT · 等待较优价",
  };
  return map[value] ?? value;
}

function holdStateLabel(value?: string | null) {
  const map: Record<string, string> = {
    loss_recovery_hold: "亏损恢复等待 · 不普通减仓",
    long_hold: "长期持仓观察 · 不自动平仓",
    forced_release_pending: "硬超时等待 · 保护价未到",
    hard_timeout_release: "硬超时释放 · 保护价已到",
  };
  return value ? map[value] ?? value : "正常等待/未触发";
}

function marginLabel(state?: string, ratio?: number | null) {
  const label: Record<string, string> = {
    safe: "安全", warning: "预警", critical: "危险", unknown: "待获取",
  };
  const suffix = typeof ratio === "number" ? ` · 可用 ${number(ratio * 100, 1)}%` : "";
  return `${label[state ?? "unknown"] ?? state ?? "待获取"}${suffix}`;
}

function reserveLabel(value: StatusResponse["execution_policy"]["dynamic_hedge_reserve"]) {
  if (!value?.enabled) return "未启用";
  return `${number(value.min_bps)}–${number(value.max_bps)} 基点 · 深度 + 历史不利波动`;
}

function openFirstLegLabel(value: StatusResponse["execution_policy"]["dynamic_open_first_leg"]) {
  if (!value?.enabled) return "未启用";
  return `${number(value.min_bps)}–${number(value.max_bps)} 基点 · P${number(value.adverse_move_percentile * 100, 0)} + ${number(value.safety_margin_bps)} 基点`;
}

function costSourceLabel(value?: string) {
  const map: Record<string, string> = {
    cold_start_prior: "冷启动先验",
    blended: "先验 + 实测",
    empirical: "实测样本",
  };
  return map[value ?? ""] ?? value ?? "保守回退";
}

function openCampaignLabel(value: NonNullable<NonNullable<StatusResponse["execution"]>["open_campaign"]>) {
  const result: Record<string, string> = {
    filled: "已成交", zero_fill: "未成交", unknown: "待恢复",
    protected_entry_failed: "保护价失效", open_budget_reserved: "预算保留",
  };
  const outcome = result[value.final_result ?? ""] ?? "进行中";
  return `${outcome} · ${number(value.send_count, 0)} 次发送 · ${number(value.retry_count, 0)} 次重试`;
}

function campaignLabel(value: NonNullable<NonNullable<StatusResponse["execution"]>["exit_campaign"]>) {
  const exitClass: Record<string, string> = {
    normal_profit: "正常退出",
    fast_exit: "快速退出",
  };
  const result: Record<string, string> = {
    filled: "已成交",
    zero_fill: "未成交",
    stale_book: "盘口过期",
    pnl_deteriorated: "盈亏底线变化",
  };
  const label = exitClass[value.exit_class ?? ""] ?? value.exit_class ?? "退出机会";
  const outcome = result[value.final_result ?? ""] ?? "进行中";
  return `${label} · ${outcome} · 第 ${number(value.retry_count, 0)} 次重试`;
}

function reentryLabel(value: ReentryGuardSnapshot) {
  if (!value.enabled) return "未启用";
  if (value.blocked) {
    const directions = Array.isArray(value.blocked_directions)
      ? value.blocked_directions.length
      : 0;
    const remaining = typeof value.cooldown_remaining_sec === "number"
      ? ` · 冷却 ${number(value.cooldown_remaining_sec, 0)} 秒`
      : "";
    return `软不确定性 · ${directions} 个方向${remaining}`;
  }
  if (value.last_risk_exit_direction && !value.basis_reset_seen) {
    return "软不确定性 · 等待基差复位";
  }
  return value.last_risk_exit_direction ? "已解除" : "未触发";
}
