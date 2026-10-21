import { localTime, money, number, statusLabel } from "../format";
import type { Attribution, CycleKpis, FeeReconciliation, StatusResponse, TradesSummary } from "../types";

const FUNNEL_WINDOWS = [
  ["session", "本次运行"],
  ["15m", "15 分钟"],
  ["1h", "1 小时"],
  ["6h", "6 小时"],
  ["24h", "24 小时"],
] as const;

const COST_PHASES = [
  ["open_first", "OPEN 首腿"],
  ["open_hedge", "OPEN 对冲"],
  ["close_first", "CLOSE 首腿"],
  ["close_hedge", "CLOSE 对冲"],
] as const;

export function ExecutionDiagnosis({ summary, today, attribution, feeReconciliation, cycleKpis }: {
  summary: TradesSummary | null;
  today: StatusResponse["today"];
  attribution: Attribution | null;
  feeReconciliation: FeeReconciliation | null;
  cycleKpis: CycleKpis;
}) {
  const lifetime = attribution?.lifetime;
  const window = lifetime ?? attribution?.session;
  const scopeLabel = lifetime ? "全生命周期" : "本次运行";
  const coverageTs = window?.coverage_start_ts ?? summary?.coverage_started_ts;
  const coverage = coverageTs
    ? `${scopeLabel}起点 ${localTime(coverageTs)}`
    : "等待账户盈亏起点";
  const confidence = window?.confidence === "exact_fill_ledger"
    ? "成交级账本已覆盖"
    : window?.confidence === "fill_ledger_fees_pending"
      ? "成交级账本待补全"
    : window?.confidence === "legacy_edge_plus_equity"
      ? "含历史 CSV 估算"
      : window?.confidence === "equity_only"
        ? "仅账户权益"
        : "等待成交级数据";
  const feeStatus = window?.fees_status ?? today.fees_status;
  const feeSourceNote = feeStatus === "actual"
    ? "交易所逐笔成交接口已核实"
    : feeStatus === "mixed"
      ? "部分已核实，其余仍按配置估算"
      : feeStatus === "estimated"
        ? "等待交易所逐笔成交回查"
        : "暂无成交手续费";
  const feePollNote = feeReconciliation?.last_error
    ? `回查异常：${feeReconciliation.last_error}`
    : feeReconciliation?.status
      ? `回查${statusLabel(feeReconciliation.status)}`
      : "等待回查任务";
  const rolling = cycleKpis.rolling_20;
  const funnel = cycleKpis;

  return <section className="diagnosis-section" aria-labelledby="diagnosis-title">
    <div className="section-head">
      <div><h2 id="diagnosis-title">{scopeLabel}：盈亏归因与执行质量</h2><p>账户权益是最终净盈亏；成交级账本拆出已实现、浮动、手续费、资金费，未归因项是两者差额。</p></div>
      <span className="coverage-label">{coverage} · {confidence}</span>
    </div>
    <div className="diagnosis-grid">
      <div className="attribution-panel">
        <div className="diagnosis-metrics">
          <Metric label={`${scopeLabel}净盈亏`} value={money(window?.equity_net_pnl_usd ?? null)} note="两家交易所权益变化" tone={tone(window?.equity_net_pnl_usd ?? null)} />
          <Metric label="账本净盈亏" value={money(window?.ledger_net_pnl_usd ?? null)} note="成交 + 浮动 − 费用 + 资金费" tone={tone(window?.ledger_net_pnl_usd ?? null)} />
          <Metric label="已实现 / 浮动" value={`${money(window?.gross_realized_usd ?? null)} / ${money(window?.unrealized_usd ?? null)}`} note="成交级 FIFO 账本" tone={tone(window?.gross_realized_usd ?? null)} />
          <Metric label={`Entropy 手续费（${statusLabel(feeStatus)}） / 资金费`} value={`${money(window?.fees_usd ?? null)} / ${money(window?.funding_pnl_usd ?? null)}`} note={`${feeSourceNote} · ${feePollNote}`} tone={tone((window?.funding_pnl_usd ?? 0) - (window?.fees_usd ?? 0))} />
          <Metric label="未归因权益变化" value={money(window?.unexplained_usd ?? null)} note="权益净盈亏 − 账本净盈亏" tone={tone(window?.unexplained_usd ?? null)} />
        </div>
        <div className="diagnosis-scopes" aria-label="不同时间口径的净盈亏">
          <span>今日净盈亏 <b className={tone(today.net_pnl_usd)}>{money(today.net_pnl_usd)}</b></span>
          <span>本次运行 <b className={tone(today.session_pnl_usd)}>{money(today.session_pnl_usd)}</b></span>
        </div>
        <p className="attribution-note"><i aria-hidden="true" />成交级账本按每笔 fill 记录手续费和 FIFO 已实现盈亏；未归因项保留为交易所权益与本地账本之间的可见差额，不再把它误称为“成交价差”。</p>
      </div>
      <div className="execution-panel">
        <div className="execution-score">
          <span>双腿匹配率</span>
          <strong>{summary ? `${number(summary.matched_rate * 100, 1)}%` : "--"}</strong>
          <small>{summary ? `${summary.matched_count} / ${summary.attempt_count} 笔` : "等待成交记录"}</small>
        </div>
        <div className="execution-counts">
          <Count label="完整双腿" value={summary?.paired_count} />
          <Count label="部分但匹配" value={summary?.partial_paired_count} />
          <Count label="单腿 / 失衡" value={summary?.one_leg_count} alert />
          <Count label="两腿零成交" value={summary?.no_fill_count} />
        </div>
      </div>
    </div>
    <div className="cycle-quality" aria-label="完整周期执行质量">
      <div className="cycle-quality-head"><h3>完整周期质量</h3><span>只统计已成交的开仓周期，数据来自 SQLite 周期账本</span></div>
      <div className="cycle-quality-grid">
        <Quality label="完成率" value={percent(cycleKpis.completion_rate)} note={`${cycleKpis.completed_count} / ${cycleKpis.record_count} 周期`} />
        <Quality label="周期中位 / P90 时长" value={`${seconds(cycleKpis.median_cycle_duration_sec)} / ${seconds(cycleKpis.p90_cycle_duration_sec)}`} note="开仓首填 → 空仓" />
        <Quality label="未对冲 P95" value={seconds(cycleKpis.p95_unhedged_sec)} note="首腿首填 → 对冲完成" />
        <Quality label="正常退出胜率" value={percent(cycleKpis.normal_win_rate)} note="净盈亏 ≥ 0" />
        <Quality label="快速 / 采样退出均值" value={`${money(cycleKpis.fast_avg_pnl_usd)} / ${money(cycleKpis.sampling_avg_pnl_usd)}`} note="Bootstrap 退出分层" />
        <Quality label="强制退出均值" value={money(cycleKpis.forced_avg_pnl_usd)} note="风险/紧急退出" />
        <Quality label="正常 / 快速 / 过期退出率" value={`${percent(cycleKpis.normal_exit_rate)} / ${percent(cycleKpis.fast_exit_rate)} / ${percent(cycleKpis.stale_exit_rate)}`} note="已完成周期" />
        <Quality label="中枢风险 / 硬超时率" value={`${percent(cycleKpis.regime_stop_rate)} / ${percent(cycleKpis.hard_timeout_exit_rate)}`} note="旧中枢风险仅作历史兼容；硬超时需保护价" />
        <Quality label="亏损恢复 / 长持观察" value={`${cycleKpis.loss_recovery_hold_count ?? 0} / ${cycleKpis.long_hold_count ?? 0}`} note="当前或历史周期持仓状态" />
        <Quality label="执行紧急 / 手动率" value={`${percent(cycleKpis.execution_emergency_rate)} / ${percent(cycleKpis.manual_flatten_rate)}`} note="非经济退出分类" />
        <Quality label="计划-实际差额" value={money(cycleKpis.planned_realized_gap_usd)} note="实际周期净盈亏 − 预测" />
        <Quality label="滚动 20 EV" value={money(rolling?.mean_pnl_usd)} note={`${number(rolling?.count, 0)} 周期 · 胜率 ${percent(rolling?.win_rate)}`} />
        <Quality label="资本小时 EV" value={money(rolling?.ev_per_capital_hour_usd)} note="实际周期净盈亏 / 名义资金小时" />
        <Quality label="机会漏斗 · 信号 / 经济" value={`${number(funnel.signal_episode_count, 0)} / ${number(funnel.economic_opportunity_count, 0)}`} note="按持久化执行记录去重" />
        <Quality label="开仓 / 对冲完成率" value={`${percent(funnel.open_campaign_fill_rate)} / ${percent(funnel.hedge_completion_rate)}`} note="经济机会 → 首腿成交 → 双腿完成" />
        <Quality label="恢复 / 紧急率" value={`${percent(cycleKpis.recovery_rate)} / ${percent(cycleKpis.emergency_exit_rate)}`} note={`预算阻塞 ${seconds(cycleKpis.order_budget_block_sec)}`} />
        <Quality label="退出机会 / 窗口成交率" value={`${number(cycleKpis.exit_campaign_count, 0)} / ${percent(cycleKpis.campaign_fill_rate)}`} note="动态重报价窗口" />
        <Quality label="首轮成交率" value={percent(cycleKpis.first_attempt_fill_rate)} note="第一次 IOC 即成交" />
        <Quality label="成交中位重试" value={cycleKpis.median_retries_to_fill == null ? "--" : `${number(cycleKpis.median_retries_to_fill, 0)} 次`} note="退出机会 → 成交" />
        <Quality label="错过盈利退出" value={`${number(cycleKpis.missed_profitable_exit_count, 0)} 笔`} note="保护价或盘口未满足" />
      </div>
      {cycleKpis.bootstrap?.configured && <BootstrapPanel snapshot={cycleKpis.bootstrap} />}
      <div className="funnel-window-strip" aria-label="分时间窗口的机会漏斗">
        {FUNNEL_WINDOWS.map(([key, label]) => {
          const windowData = cycleKpis.funnel_windows?.[key];
          return <div className="funnel-window" key={key}>
            <span>{label}</span>
            <strong>{windowData
              ? `${windowData.signal_episode_count} → ${windowData.economic_opportunity_count} → ${windowData.open_campaign_count} → ${windowData.open_fill_count} → ${windowData.hedge_complete_count} → ${windowData.complete_cycle_count}`
              : "--"}</strong>
            <small>信号 → 经济 → campaign → 首腿 → 对冲 → 完成</small>
          </div>;
        })}
      </div>
      <div className="cost-model-section">
        <div className="cost-model-head"><h3>执行成本模型：经济增量 / 物理全包</h3><span>经济准入只计成交均价相对深度均价的增量；全包、深度与不利波动用于归因和压力上限</span></div>
        <div className="cost-model-scroll">
          <div className="cost-model-table" role="table" aria-label="四阶段执行成本与压力分布">
            <div className="cost-model-row cost-model-header" role="row">
              <span>阶段</span><span>样本 / 成交率</span><span>经济增量 P50 / P75 / P90</span><span>物理全包 P50 / P90</span><span>延迟 P50 / P90</span><span>来源 / 实测权重</span>
            </div>
            {COST_PHASES.map(([key, label]) => {
              const phase = cycleKpis.execution_cost_model?.phases?.[key];
              const allInP50 = phase?.p50_all_in_execution_cost_bps
                ?? phase?.p50_all_in_cost_bps;
              const allInP90 = phase?.p90_all_in_execution_cost_bps
                ?? phase?.p90_all_in_cost_bps;
              const incremental = phase && [
                phase.p50_incremental_execution_cost_bps ?? phase.p50_cost_bps,
                phase.p75_incremental_execution_cost_bps ?? phase.p75_cost_bps,
                phase.p90_incremental_execution_cost_bps ?? phase.p90_cost_bps,
              ];
              return <div className="cost-model-row" role="row" key={key}>
                <strong>{label}</strong>
                <span>{phase ? `${number(phase.observation_count, 0)} / ${percent(phase.fill_rate)}` : "--"}</span>
                <span>{incremental ? `${number(incremental[0])} / ${number(incremental[1])} / ${number(incremental[2])} bp` : "--"}</span>
                <span>{phase ? `${number(allInP50)} / ${number(allInP90)} bp` : "--"}</span>
                <span>{phase ? `${number(phase.p50_latency_sec)} / ${number(phase.p90_latency_sec)} 秒` : "--"}</span>
                <span>{phase ? `${costSourceLabel(phase.source)} / ${number(phase.empirical_weight * 100, 0)}%` : "--"}</span>
              </div>;
            })}
          </div>
        </div>
      </div>
    </div>
  </section>;
}

function BootstrapPanel({ snapshot }: { snapshot: NonNullable<CycleKpis["bootstrap"]> }) {
  const samples = snapshot.samples_per_phase;
  const sampleText = ["open_first", "open_hedge", "close_first", "close_hedge"]
    .map((phase) => samples[phase] ?? 0)
    .join(" / ");
  const mode = bootstrapModeLabel(snapshot.mode);
  const color = snapshot.last_color?.toLowerCase() ?? "pending";
  const acceptedFloor = snapshot.worst_accepted_expected_pnl_usd == null
    ? "--"
    : money(snapshot.worst_accepted_expected_pnl_usd);

  return <div className="bootstrap-panel" aria-label="激进冷启动状态">
    <div className="bootstrap-panel-head">
      <div><h3>激进冷启动</h3><span>只放宽经济准入与小额仓位；物理限价、对冲与恢复保护保持不变</span></div>
      <div className="bootstrap-state">
        <b className={`bootstrap-color ${color}`}>{snapshot.last_color ?? "未决策"}</b>
        <span>{mode}</span>
      </div>
    </div>
    <div className="bootstrap-grid">
      <Quality label="完整周期" value={`${snapshot.complete_cycles} / ${snapshot.max_complete_cycles}`} note={`阶段目标 ${snapshot.target_complete_cycles}；达到上限自动退出`} />
      <Quality label="最近 1 小时完整周期" value={`${snapshot.complete_cycles_per_hour ?? 0} 笔`} note="持久化 flat-to-flat 周期" />
      <Quality label="OPEN campaign / 首填率" value={`${snapshot.open_campaigns_per_hour ?? 0} / ${percent(snapshot.open_fill_rate_per_hour)}`} note="最近 1 小时" />
      <Quality label="对冲完成率" value={percent(snapshot.hedge_completion_rate_per_hour)} note="OPEN 首填 → 双腿完成" />
      <Quality label="CLOSE 机会 / 成交率" value={`${snapshot.close_opportunities_per_hour ?? 0} / ${percent(snapshot.close_fill_rate_per_hour)}`} note="最近 1 小时动态重报价窗口" />
      <Quality label="持仓中位 / P90" value={`${seconds(snapshot.median_hold_sec)} / ${seconds(snapshot.p90_hold_sec)}`} note="开仓首填 → 空仓" />
      <Quality label="周期均值 / 中位盈亏" value={`${money(snapshot.mean_cycle_pnl_usd)} / ${money(snapshot.median_cycle_pnl_usd)}`} note="最近 20 个完成周期" />
      <Quality label="阶段样本" value={sampleText} note={`OPEN首腿 / OPEN对冲 / CLOSE首腿 / CLOSE对冲；每阶段 ≥${snapshot.min_execution_samples_per_phase}`} />
      <Quality label="Bootstrap 总盈亏" value={money(snapshot.total_bootstrap_pnl_usd ?? snapshot.exploration_pnl_usd)} note={`剩余亏损预算 ${money(snapshot.loss_budget_remaining_usd)}`} />
      <Quality label="接受决策" value={`${snapshot.accepted_count} 笔`} note={`GREEN ${snapshot.green_count} · YELLOW ${snapshot.yellow_count} · RED ${snapshot.red_count}`} />
      <Quality label="预期盈亏范围" value={`${money(snapshot.best_expected_pnl_usd)} ~ ${acceptedFloor}`} note={`允许单笔底线 ${money(-Math.min(snapshot.max_expected_loss_usd ?? 0, (snapshot.max_order_notional_usd ?? 0) * (snapshot.max_expected_loss_bps ?? 0) / 10000))}`} />
      <Quality label="准入状态" value={snapshot.active ? "可探索" : snapshot.suspended ? "安全恢复中" : snapshot.disabled ? "已自动退出" : "正常模式"} note={snapshot.suspended_reason ? `挂起：${snapshot.suspended_reason}；平仓、对账和安全检查通过后自动恢复` : snapshot.disabled_reason ?? `回合后恢复正常；最低毛收益 ${number(snapshot.min_gross_after_fee_bps ?? 0, 2)} bp`} />
    </div>
  </div>;
}

function Metric({ label, value, note, tone }: { label: string; value: string; note: string; tone: string }) {
  return <div className="diagnosis-metric"><span>{label}</span><strong className={tone}>{value}</strong><small>{note}</small></div>;
}

function Count({ label, value, alert = false }: { label: string; value: number | undefined; alert?: boolean }) {
  return <div className={alert && value ? "is-alert" : ""}><span>{label}</span><strong>{value ?? "--"}</strong></div>;
}

function tone(value: number | null) {
  if (value == null || Math.abs(value) < .005) return "";
  return value > 0 ? "tone-ok" : "tone-loss";
}

function Quality({ label, value, note }: { label: string; value: string; note: string }) {
  return <div className="quality-metric"><span>{label}</span><strong>{value}</strong><small>{note}</small></div>;
}

function percent(value: number | null | undefined) {
  return value == null ? "--" : `${number(value * 100, 1)}%`;
}

function seconds(value: number | null | undefined) {
  return value == null ? "--" : `${number(value, 1)} 秒`;
}

function costSourceLabel(value?: string) {
  const map: Record<string, string> = {
    cold_start_prior: "冷启动先验",
    blended: "先验 + 实测",
    empirical: "实测样本",
  };
  return map[value ?? ""] ?? value ?? "保守回退";
}

function bootstrapModeLabel(value: string) {
  const map: Record<string, string> = {
    bootstrap: "进行中",
    suspended: "安全恢复中",
    normal_after_auto_disable: "已回到正常",
    disabled: "已停用",
    normal: "未启用",
  };
  return map[value] ?? value;
}
