import type { ReactNode } from "react";
import { DataNotice, DiagnosticSummary, Empty, Section, SourceTable } from "./Common";
import { withQuery } from "./client";
import { batchLimit, exact, money, time } from "./format";
import { commonQuery, queryKey, routeHash, type RouteState } from "./router";
import type { Deployment, OverviewData, V3Meta } from "./types";
import { useResource } from "./useResource";
import FundingPanel, { fundingReasons } from "./FundingPanel";
import ProfitOverview from "./ProfitOverview";
import EconomicsSummary from "./EconomicsSummary";

const closeReasons: Record<string, string> = {
  ACCOUNT_DRAWDOWN: "账户回撤保护", REALIZED_LOSS_BUDGET: "累计亏损达到预算",
  SINGLE_CYCLE_REVIEW_LOSS: "单圈退出估值触发复核，阻止新开仓", MAX_POSITION_CYCLES: "本批次交易圈数已达上限",
  MANUAL_STOP: "人工暂停",
};
function DeploymentStatus({ deployment, route, paused, stale }: { deployment: Deployment; route: RouteState; paused: boolean; stale: boolean }) {
  const live = deployment.status;
  const exited = deployment.manual_exit?.supersedes_live && ["complete", "accounting_pending"].includes(deployment.manual_exit.state);
  const age = live?.source_ts != null ? deployment.generated_at - live.source_ts : null;
  const fresh = age != null && age >= 0 && age <= 10 && !stale;
  const blocked = live?.current_state === "BATCH_CLOSED" || live?.closed_for_entries === true;
  const reason = live?.close_reason || live?.reason || "";
  const batch = deployment.batch;
  const meta: V3Meta = { schema_version: 3, snapshot_id: `${deployment.source_id}:${deployment.snapshot_id}`, generated_at: deployment.generated_at, sources: deployment.source_health, window: deployment.window };
  return <section className={`v3-deployment-status ${blocked || !fresh ? "needs-attention" : ""}`} aria-label={`${deployment.label}运行状态`}>
    <div className="v3-profit-status"><span className="v3-profit-status-dot" /><strong>{deployment.label} · {deployment.symbols.join(" / ") || "币种待确认"}</strong><span>{exited ? "退出时已平仓 · 新开仓关闭" : paused ? "页面刷新暂停" : !fresh ? "运行状态过期 / 待确认" : blocked ? "新开仓暂停" : live?.current_state || "待确认"}</span><small>实盘状态 {time(live?.source_ts, route.timezone)}</small></div>
    <p>{closeReasons[reason] || fundingReasons[reason] || reason || deployment.reason || "状态原因待确认"}{!fresh ? " · 状态文件不能证明进程仍在运行" : ""}</p>
    <div className="v3-deployment-facts"><span>启动 {time(live?.start_ts, route.timezone)} · 本次启动后 {deployment.current_session_attempts ?? "未知"} 次尝试 / {deployment.current_session_complete ?? "未知"} 个完整圈 / {deployment.current_session_zero_fill ?? "未知"} 次零成交</span><span>批次累计 {exact(batch?.position_cycles)} 圈 · 圈数上限 {batchLimit(batch?.max_position_cycles)} · 累计亏损 {money(batch?.loss_used_usd)} · 亏损预算 {batchLimit(batch?.loss_budget_usd, " USD")}</span><span>Recorder {deployment.source_health.recorder?.quality || "待确认"} · 仅影响历史观察，不能代表实盘盘口新鲜度</span></div>
    {reason === "SINGLE_CYCLE_REVIEW_LOSS" ? <p className="v3-pending">触发时可执行估值 {exact(batch?.close_value)} USD / 阈值 {exact(batch?.close_threshold)} USD。它与最终净结果是不同证据；批次仍阻止新开仓。</p> : null}
    <details className="v3-trader-more"><summary>此部署的双腿风险、诊断与来源</summary>
      {deployment.overview?.funding ? <FundingPanel data={deployment.overview.funding} live={live} timezone={route.timezone} paused={paused} unavailable={stale || !fresh} /> : <Section title="当前运行诊断"><DiagnosticSummary data={deployment.overview?.diagnostic} timezone={route.timezone} /></Section>}
      <div className="v3-trader-secondary-link"><a href={routeHash({ ...route, mode: deployment.mode, source: "", page: "cycles", view: "unfilled", cycle: "", cursor: "" })}>此部署未成交尝试与订单诊断 →</a></div>
      <DataNotice meta={meta} stale={stale} timezone={route.timezone} />
      <Section title="数据来源 · 各来源独立"><SourceTable sources={deployment.source_health} timezone={route.timezone} /></Section>
    </details>
  </section>;
}
export function RuntimeSummary({ deployment, stale, paused, timezone }: { deployment: Deployment; stale: boolean; paused: boolean; timezone: RouteState["timezone"] }) {
  const live = deployment.status;
  const exited = deployment.manual_exit?.supersedes_live && ["complete", "accounting_pending"].includes(deployment.manual_exit.state);
  const age = live?.source_ts != null && Number.isFinite(live.source_ts) && live.source_ts > 0 ? deployment.generated_at - live.source_ts : null;
  const fresh = age != null && age >= 0 && age <= 10 && !stale;
  const blocked = live?.closed_for_entries || live?.current_state === "BATCH_CLOSED";
  const reason = live?.close_reason || live?.reason || "";
  const states: Record<string, string> = { READY: "程序在线 · 等待开仓条件", FLAT: "本轮已平仓", STARTING: "正在启动", WAITING_FOR_MARKET: "等待行情更新", COLLECTING_REFERENCE: "正在收集行情", RECONCILING: "正在核对仓位", HOLDING: "正在持仓", HOLDING_DEGRADED_POSITION_EVIDENCE: "持仓中 · 仓位证据待核对", HOLDING_FUNDING: "持仓等待资金费结算", EXITING: "正在平仓", RESOLVING_ORDERS: "正在确认订单结果", RECONCILE_REQUIRED: "仓位或订单待核对", ACCOUNTING_PENDING: "交易已结束，账务待核对", HALTED: "程序已停止", STOPPING: "正在暂停新开仓", STOPPED: "新开仓已暂停", REVIEW_REQUIRED: "交易暂停，需复核", SESSION_COMPLETE: "本次运行已结束", BATCH_COMPLETE: "本批次已结束", PREFLIGHT: "正在预检，未确认交易运行", PREFLIGHT_OK: "预检通过，未确认交易运行" };
  const reasons: Record<string, string> = {
    no_send_entry_check: "当前未通过开仓检查，具体原因未上报",
    no_send_scenario_check: "当前未通过开仓检查，具体原因未上报",
    local_flat_state_unavailable: "本地空仓证据尚未确认，暂不开仓",
    fresh_authoritative_flat_lease_unavailable: "空仓状态尚未确认，暂不开仓",
    OPEN_SIGNAL_DROPPED_RATE_LIMIT: "下单频率受限，暂不开仓",
    lighter_http_request_deferred: "Lighter 请求暂缓，等待恢复后再检查开仓",
    resource_wait_candidate_dropped: "下单资源尚未就绪，等待下一次开仓检查",
    completion_resource_unavailable: "对冲下单资源尚未就绪，暂不开仓",
    opening_depth_outside_hard_cap: "开仓盘口深度超出价格保护上限",
    "hedge book is not fresh": "Lighter 行情已过期，暂不开仓",
    "entropy book is not fresh": "Entropy 行情已过期，暂不开仓",
    collecting: "参考线尚未就绪，正在积累有效行情",
    operator_running: "已恢复开仓检查，等待下一次有效条件",
    operator_stopping: "已请求暂停新开仓，继续管理已有仓位",
    operator_stopped: "新开仓已暂停，等待手动启动",
    hold: "正在管理持仓，尚未达到正常退出收益门槛",
    hold_missing_price_evidence: "成交价格证据未齐，继续核对持仓",
    hold_missing_entry_notional: "实际开仓金额证据未齐，继续核对持仓",
    gross_price_profit_exit: "已达到正常退出收益门槛，正在平仓",
    basis_exit_reduce_only_ioc: "正在提交平仓订单并核对剩余仓位",
    basis_exit_position_confirmation_required: "正在确认双腿仓位，再继续平仓",
    reduce_only_leg_retry_required: "平仓尚未完成，正在处理剩余仓位",
    same_id_recovery: "正在查询原订单结果，等待确认",
    startup_position_and_order_recovery: "正在恢复并核对仓位与未终结订单",
    loading_markets_and_account_streams: "正在加载市场与账户状态",
    PENDING_ACCOUNTING: "历史交易尚待核算，暂不开新仓",
    PENDING_ACCOUNTING_ESTIMATE_BUDGET: "待核算交易占用风险预算，暂不开新仓",
  };
  const stateReasons: Record<string, string> = { COLLECTING_REFERENCE: "参考线尚未就绪，正在积累有效行情", WAITING_FOR_MARKET: "等待新鲜且有效的双腿行情", HOLDING: "正在管理当前持仓，等待退出条件", HOLDING_DEGRADED_POSITION_EVIDENCE: "持仓证据暂不完整，正在核对", HOLDING_FUNDING: "正在管理持仓并等待资金费结算", EXITING: "正在处理退出并确认双腿归零", RECONCILING: "正在核对仓位与订单", RESOLVING_ORDERS: "正在确认未终结订单结果", ACCOUNTING_PENDING: "正在等待成交与账务核对", STOPPING: "已请求暂停新开仓，继续管理已有仓位", STOPPED: "新开仓已暂停，等待手动启动", READY: "等待开仓条件，具体原因未上报" };
  const unfilled = live?.current_state === "FLAT" && reason === "cycle_complete" && live.result_kind === "zero_fill";
  const label = exited ? "退出时已平仓 · 新开仓关闭" : paused ? "页面暂停更新 · 显示上次状态" : stale ? "数据读取异常 · 当前状态待确认" : !fresh ? "状态过期 · 是否在线未知" : blocked ? "已暂停新交易" : unfilled ? "本次未成交尝试已结束" : states[live?.current_state || ""] || "当前状态待确认";
  const explanation = exited ? `权威退出 ${time(deployment.manual_exit?.position_observed_ts, timezone)} · 实时仓位待查询` : paused ? `暂停更新 · 上次状态 ${time(live?.source_ts, timezone)}` : !fresh ? age == null ? "尚未收到有效运行状态时间" : age < 0 ? `状态时间晚于快照，当前原因待确认 · ${time(live?.source_ts, timezone)}` : `最后状态 ${time(live?.source_ts, timezone)} · 当前原因待确认` : unfilled ? "开仓 IOC 零成交，本次尝试已结束" : closeReasons[reason] || reasons[reason] || fundingReasons[reason] || stateReasons[live?.current_state || ""] || "具体运行原因未上报";
  return <div className={blocked || !fresh || paused ? "v3-pending" : ""}><strong>{deployment.symbols.join(" / ") || "标的待确认"} · {deployment.mode === "basis" ? "普通对冲" : "资金费对冲"}</strong><span>{label}</span><small>{explanation}</small></div>;
}
export default function OverviewPage({ route, paused, update, toolbar, refresh = 0 }: { route: RouteState; paused: boolean; update: (patch: Partial<RouteState>) => void; toolbar?: ReactNode; refresh?: number }) {
  const listRoute = { ...route, source: "" as const };
  const result = useResource<OverviewData>(`overview|${queryKey(listRoute)}`, () => withQuery("overview", commonQuery(listRoute)), 2_000, paused, refresh);
  const data = result.envelope?.data;
  const meta = result.envelope?.meta;
  return <div className="v3-page v3-trader-home v3-profit-home v3-readable">
    {result.error || result.stale ? <p className="v3-form-error" role="alert">数据读取异常：{result.error || "保留上次快照，状态待确认"}</p> : null}
    {!data ? <>{toolbar}<Empty>{result.loading ? "正在读取 V3 交易事实…" : "V3 数据不可用；不会使用旧账本补齐。"}</Empty></> : <>
      <ProfitOverview data={data.profit} meta={meta} route={route} paused={paused} update={update} refresh={refresh} toolbar={toolbar} runtime={data.deployments?.map(deployment => <RuntimeSummary key={deployment.source_id} deployment={deployment} stale={result.stale} paused={paused} timezone={route.timezone} />) || <strong>{data.live?.current_state || "运行状态待确认"} · {closeReasons[data.live?.close_reason || ""] || data.live?.reason}</strong>} summary={<EconomicsSummary route={route} paused={paused} refresh={refresh} deployments={data.deployments || []} />} evidence={data.deployments?.map(deployment => <DeploymentStatus key={deployment.source_id} deployment={deployment} route={route} paused={paused} stale={result.stale} />)} />
    </>}
  </div>;
}
