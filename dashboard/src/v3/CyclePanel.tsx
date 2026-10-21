import { useEffect, useRef, useState } from "react";
import { DataNotice, DiagnosticSummary, Empty, KV, QualityBadge, Section } from "./Common";
import { withQuery } from "./client";
import { basis, cycleTiming, duration, exact, money, textField, time } from "./format";
import { commonQuery, queryKey, type RouteState } from "./router";
import type { Attempt, CycleDetailData, ExecutionGroup } from "./types";
import { useResource } from "./useResource";
import { amount, accountingLabel } from "./ProfitOverview";
import ExecutionProcess from "./ExecutionProcess";
import { deriveExecutionEvidence } from "./executionEvidence";
import { fundingReasons } from "./FundingPanel";
import { cycleOutcome } from "./TraderCycleTable";
import "./CyclePanel.css";

function AttributionPanel({ group }: { group: ExecutionGroup }) {
  const attribution = group.attribution;
  return <div className="v3-attribution"><div className="v3-kv-grid">
    <KV label="A 决策配对报价" value={basis(attribution?.A)} /><KV label="B 发单前报价" value={basis(attribution?.B)} />
    <KV label="C 首腿回报观察时估值" value={basis(attribution?.C)} /><KV label="D 两腿实际匹配成交" value={basis(attribution?.D)} />
    <KV label="A→B" value={`${basis(attribution?.a_to_b_bps)} / ${money(attribution?.a_to_b_usd)}`} />
    <KV label="B→C" value={`${basis(attribution?.b_to_c_bps)} / ${money(attribution?.b_to_c_usd)}`} />
    <KV label="C→D" value={`${basis(attribution?.c_to_d_bps)} / ${money(attribution?.c_to_d_usd)}`} />
    <KV label="匹配数量" value={exact(group.matched_units, " units")} />
  </div><p className="v3-muted">费用 {money(attribution?.fees_usd)} · 资金费 {money(attribution?.funding_usd)}；这些是独立归因字段，不会从最终净收益再次扣除。{attribution?.reason || ""}</p></div>;
}

function AttemptTable({ attempts, timezone }: { attempts?: Attempt[]; timezone: RouteState["timezone"] }) {
  if (!attempts?.length) return <Empty>没有可验证的 attempt 记录；不补造提交或成交。</Empty>;
  return <div className="v3-table-wrap"><table className="v3-table v3-table-dense"><thead><tr><th>order_ref / 来源</th><th>交易所 / 方向</th><th>请求 → 成交 / 剩余</th><th>限价 / 成交均价</th><th>状态</th><th>证据时间</th></tr></thead><tbody>
    {attempts.map((attempt, index) => <tr key={`${attempt.order_ref}-${index}`}><td><code title={attempt.order_ref}>{attempt.order_ref || "未登记"}</code><small>{attempt.confirmation_source || "确认来源未知"}</small></td>
      <td>{exact(attempt.venue)} / {exact(attempt.side)}<small>{attempt.reduce_only == null ? "reduce-only 未知" : attempt.reduce_only ? "reduce-only" : "非 reduce-only"}</small></td>
      <td>{exact(attempt.requested_units)} → {exact(attempt.filled_units)} / {exact(attempt.remaining_units)}<small>room {basis(attempt.room_bps)} · cap {basis(attempt.cap_bps)}</small></td>
      <td>{exact(attempt.limit_px)} / {exact(attempt.avg_fill_px)}</td><td><QualityBadge value={attempt.state} reason={attempt.reason} />{attempt.had_unknown ? <small>曾 UNKNOWN</small> : null}</td>
      <td>提交 {time(attempt.submitted_at, timezone)}<small>终态 {time(attempt.terminal_at, timezone)}</small></td></tr>)}
  </tbody></table></div>;
}

function GroupButtons({ groups, selected, onSelect }: {
  groups: (ExecutionGroup & { kind?: string | null })[];
  selected: string | null; onSelect: (id: string) => void;
}) {
  return <div className="v3-group-list">{groups.map(group => <button type="button" className={selected === group.group_id ? "is-selected" : ""} key={group.group_id} onClick={() => onSelect(group.group_id)}>
    <span>{group.kind === "open" ? "开仓" : group.kind === "close" ? "平仓" : "恢复 / 未归类"}</span>
    <strong>{group.group_id}</strong><small>{exact(group.matched_units, " 配对 units")} · {group.attempts?.length ?? "—"} 次尝试</small>
  </button>)}</div>;
}

function uniqueFills(data: CycleDetailData): Attempt[] {
  const orders = new Map<string, Attempt>();
  for (const group of data.groups || []) for (const order of group.attempts || []) {
    if (!(Number(order.filled_units) > 0)) continue;
    const key = `${order.venue}:${order.order_ref}`;
    const previous = orders.get(key);
    if (!previous || Number(order.filled_units) > Number(previous.filled_units)) orders.set(key, order);
  }
  return [...orders.values()].sort((a, b) => (a.terminal_at || a.submitted_at || 0) - (b.terminal_at || b.submitted_at || 0));
}
function price(orders: Attempt[], venue: string, closing: boolean): string {
  const matches = orders.filter(order => order.venue === venue && order.reduce_only != null && Boolean(order.reduce_only) === closing);
  if (!matches.length || matches.some(order => order.avg_fill_px == null || !Number.isFinite(Number(order.avg_fill_px)))) return "待确认";
  const units = matches.reduce((sum, order) => sum + Number(order.filled_units), 0);
  return amount(String(matches.reduce((sum, order) => sum + Number(order.avg_fill_px) * Number(order.filled_units), 0) / units), false);
}
const reasons: Record<string, string> = {
  gross_price_profit_exit: "价格盈亏达到退出条件", funding_reversal: "资金费方向反转",
  funding_settlements_complete: "达到资金费结算次数", funding_liquidation_risk: "清算距离保护",
  funding_risk_stale: "风险数据过期", funding_round_loss_limit: "本轮亏损保护",
  funding_remaining_cost: "剩余收益不足以覆盖退出成本", manual_stop: "人工停止",
};
function ExitReason({ data }: { data: CycleDetailData }) {
  const economics = data.economics;
  const reason = textField(economics, "exit_trigger_reason") || data.cycle?.funding_state?.exit_reason;
  const trigger = Number(textField(economics, "exit_trigger_ts"));
  const close = deriveExecutionEvidence(data).stages.find(stage => stage.id === "close")!;
  return <div className="v3-panel-content">
    <div className="v3-panel-callout"><strong>{reason ? reasons[reason] || fundingReasons[reason] || "按本轮记录的条件退出" : "退出原因未完整记录"}</strong><p>{reason ? `本轮持久化原因：${reason}` : "没有该轮历史证据，不能用当前实盘诊断替代。"}</p></div>
    <dl className="v3-panel-facts"><div><dt>触发时间（北京）</dt><dd>{time(trigger, "Asia/Taipei")}</dd></div><div><dt>触发时可执行净估值</dt><dd>{amount(textField(economics, "exit_trigger_executable_pnl_usd"))} U</dd></div><div><dt>平仓执行方式</dt><dd>{close.mode === "parallel" ? "两腿并行 · 共用退出触发" : close.mode === "sequential" ? "两腿按顺序执行" : close.legs.length ? "有平仓成交 · 执行先后待确认" : "尚无平仓证据"}</dd></div><div><dt>平仓确认</dt><dd>{time(data.cycle?.flat_confirmed_at, "Asia/Taipei")}</dd></div></dl>
    <p className="v3-muted">触发时估值与最终成交结果属于不同时点。详细成交差异见「成交过程」；手续费与资金费只计入净结果一次。</p>
    {Number(data.cycle?.close_zero_fill_count) > 0 ? <p>本轮有 {exact(data.cycle?.close_zero_fill_count)} 次确认零成交的平仓尝试；零成交不会累计收益。</p> : null}
  </div>;
}
function FundingSettlements({ data, timezone }: { data: CycleDetailData; timezone: RouteState["timezone"] }) {
  const funding = data.cycle?.funding_state;
  if (!funding) return null;
  return <details className="v3-panel-funding"><summary>本轮资金费结算</summary><div className="v3-kv-grid">
    <KV label="Entropy / Lighter 次数" value={`${exact(funding.settlement_counts?.entropy)} / ${exact(funding.settlement_counts?.hedge)}`} />
    <KV label="实际净资金费" value={money(funding.actual_funding_usd)} />
    <KV label="结算周期" value={funding.completed == null ? "待确认" : funding.completed ? "两周期完成" : "两周期未完成"} />
    <KV label="交易 / 关闭核账" value={`${funding.trade_accounting_complete == null ? "未知" : funding.trade_accounting_complete ? "完成" : "待核对"} / ${funding.closed_poll_complete == null ? "未知" : funding.closed_poll_complete ? "完成" : "待核对"}`} />
  </div>{funding.settlements?.length ? <div className="v3-table-wrap"><table className="v3-table v3-table-dense"><thead><tr><th>平台 / 入账时间</th><th>实际资金费</th><th>来源 / 事件</th></tr></thead><tbody>{funding.settlements.map(item => <tr key={`${item.venue}:${item.event_id}`}><td>{item.venue === "entropy" ? "Entropy" : "Lighter"}<small>{time(item.ts, timezone)}</small></td><td>{money(item.amount_usd)}</td><td>{item.source}<small>{item.event_id}</small></td></tr>)}</tbody></table></div> : <p className="v3-muted">本轮未提供逐笔结算证据，不能把缺失记录视为零。</p>}<p className="v3-muted">实际入账与最终核账分开；提前退出不表示两周期完成。</p></details>;
}

export default function CyclePanel({ route, paused, refresh, update }: {
  route: RouteState; paused: boolean; refresh: number; update: (patch: Partial<RouteState>) => void;
}) {
  const source = route.source;
  const identity = `${source}:${route.cycle}`;
  const result = useResource<CycleDetailData>(`cycle-panel|${identity}|${queryKey(route)}`, () => withQuery(`cycles/${encodeURIComponent(route.cycle)}`, commonQuery(route)), 5_000, paused, refresh);
  const candidate = result.envelope?.data;
  const data = candidate?.cycle?.cycle_id === route.cycle && candidate.cycle.source_id === source ? candidate : undefined;
  const profit = data?.profit;
  const cycle = data?.cycle;
  const orders = data ? uniqueFills(data) : [];
  const fees = new Map((profit?.order_fees || []).map(order => [order.order_ref, order.fee_usd]));
  const content = useRef<HTMLDivElement>(null);
  const panel = useRef<HTMLElement>(null);
  const restoreFocus = useRef<HTMLElement | null>(null);
  const currentIdentity = useRef(identity);
  currentIdentity.current = identity;
  const [compact, setCompact] = useState(() => window.matchMedia("(max-width: 1000px)").matches);
  const [copyStatus, setCopyStatus] = useState("");
  const close = () => update({ cycle: "", source: "", group: "", detail: "legs" });
  useEffect(() => {
    const media = window.matchMedia("(max-width: 1000px)");
    const changed = () => setCompact(media.matches);
    media.addEventListener("change", changed);
    return () => media.removeEventListener("change", changed);
  }, []);
  useEffect(() => {
    const active = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    if (active && !panel.current?.contains(active)) restoreFocus.current = active;
    panel.current?.focus({ preventScroll: true });
    setCopyStatus("");
  }, [identity]);
  useEffect(() => () => { if (restoreFocus.current?.isConnected) restoreFocus.current.focus({ preventScroll: true }); }, []);
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => { if (event.key === "Escape") { event.preventDefault(); close(); } };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [update]);
  useEffect(() => {
    if (!compact) return;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { document.body.style.overflow = previous; };
  }, [compact]);
  const copyDiagnostic = async () => {
    const requestedIdentity = identity;
    setCopyStatus("正在读取诊断…");
    try {
      const query = commonQuery(route);
      query.set("cycle_id", route.cycle);
      const response = await fetch(`/api/v3/${withQuery("diagnostic-export", query)}`, { method: "GET", headers: { Accept: "application/json" }, cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const diagnostic = await response.text();
      if (currentIdentity.current !== requestedIdentity) return;
      await navigator.clipboard.writeText(diagnostic);
      if (currentIdentity.current === requestedIdentity) setCopyStatus("已复制只读诊断（包含来源时间）");
    } catch (error) { if (currentIdentity.current === requestedIdentity) setCopyStatus(`复制失败：${error instanceof Error ? error.message : "未知错误"}`); }
  };
  const groups = data?.groups || [];
  const selected = groups.find(group => group.group_id === route.group) || null;
  const zeroGroups = groups.filter(group => group.attempts?.length && group.attempts.every(attempt => attempt.state === "TERMINAL" && attempt.submitted_at != null && Number(attempt.filled_units) === 0));
  const mainGroups = groups.filter(group => !zeroGroups.includes(group));
  // New subjects/tabs start at the top; refresh preserves both scroll positions.
  useEffect(() => { content.current?.scrollTo({ top: 0 }); }, [identity, route.detail]);
  const status = cycle?.paired_round_trip ? "双腿已平仓" : cycle ? cycleOutcome(cycle) : "状态待确认";
  const timing = cycleTiming(cycle);
  return <>{compact ? <button type="button" className="v3-detail-backdrop" tabIndex={-1} aria-label="收起本轮详情" onClick={close} /> : null}<aside ref={panel} tabIndex={-1} id="cycle-panel" className="v3-cycle-panel" role={compact ? "dialog" : "region"} aria-modal={compact ? true : undefined} aria-label="本轮详情" data-identity={identity} onKeyDown={event => {
    if (!compact || event.key !== "Tab") return;
    const focusable = Array.from(panel.current?.querySelectorAll<HTMLElement>('button:not(:disabled), a[href], input, select, textarea, summary, [tabindex="0"]') || []).filter(element => element.getClientRects().length);
    const first = focusable[0], last = focusable[focusable.length - 1];
    if (event.shiftKey && (document.activeElement === first || document.activeElement === panel.current)) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && (document.activeElement === last || document.activeElement === panel.current)) { event.preventDefault(); first?.focus(); }
  }}>
    <header className="v3-panel-head"><div><span>本轮详情</span><h2>{cycle?.symbol || "正在读取"} <small>{source === "funding" ? "资金费对冲" : "普通对冲"}</small></h2></div><button type="button" aria-label="关闭本轮详情" onClick={close}>×</button></header>
    {result.error || result.stale ? <p className="v3-panel-error" role="alert">详情刷新异常：{result.error || "显示上次证据"}</p> : null}
    <div className="v3-panel-scroll" ref={content}>
    {!data ? <Empty>{result.loading ? "正在读取这一轮的证据…" : "详情暂不可用，请刷新后重试。"}</Empty> : <>
      <div className={`v3-panel-status ${cycle?.paired_round_trip ? "v3-good" : "v3-pending"}`}><strong>{status}</strong><span>{cycle?.has_confirmed_fill === false ? "未成交尝试" : `整轮持仓 ${duration(timing.seconds)}`} · {timing.durationLabel}</span></div>
      <div className="v3-panel-legs">{[["entropy", "Entropy"], ["hedge", "Lighter"]].map(([venue, label]) => {
        const opening = orders.filter(order => order.venue === venue && order.reduce_only != null && !order.reduce_only);
        const side = opening[0]?.side;
        const units = opening.reduce((sum, order) => sum + Number(order.filled_units), 0);
        return <div key={venue}><div><strong>{label}</strong><span>{side === "sell" ? "做空" : side === "buy" ? "做多" : "方向待确认"}</span><small>{profit?.quantity_step && units ? `${amount(String(units * Number(profit.quantity_step)), false)} ${cycle?.symbol}` : `${units || "待确认"} units`}</small></div><p><span>{price(orders, venue, false)}</span><small>开仓 → 平仓</small><span>{price(orders, venue, true)}</span></p></div>;
      })}</div>
      <div className="v3-panel-tabs" role="tablist" aria-label="详情内容">{[["legs", "双腿"], ["fills", "成交过程"], ["exit", "退出原因"]].map(([tab, label]) => <button type="button" role="tab" id={`cycle-tab-${tab}`} aria-selected={route.detail === tab} aria-controls="cycle-tab-content" key={tab} onClick={() => update({ detail: tab as RouteState["detail"] })}>{label}</button>)}</div>
      <div id="cycle-tab-content" role="tabpanel" aria-labelledby={`cycle-tab-${route.detail}`}>
        {route.detail === "fills" ? <div className="v3-panel-content"><ExecutionProcess data={data} source={source} />
          <details className="v3-panel-orders"><summary>逐笔成交与订单证据 · {orders.length} 笔</summary>
            {orders.map(order => <div className="v3-panel-order" key={`${order.venue}:${order.order_ref}`}><div><b>{order.venue === "entropy" ? "Entropy" : "Lighter"} · {order.side === "buy" ? "买入" : "卖出"}{order.reduce_only ? "平仓" : "开仓"}</b><span>{time(order.terminal_at, "Asia/Taipei")}</span></div><dl><div><dt>请求 / 累计成交 / 剩余</dt><dd>{exact(order.requested_units)} / {exact(order.filled_units)} / {exact(order.remaining_units)} units</dd></div><div><dt>成交 VWAP</dt><dd>{amount(order.avg_fill_px, false)}</dd></div><div><dt>本单手续费</dt><dd>{orders.some(other => other.venue !== order.venue && other.order_ref === order.order_ref) ? "双所同编号，费用未关联" : fees.get(order.order_ref) == null && profit?.fee_basis === "cycle_complete" ? "单笔未提供，整轮已核账" : amount(fees.get(order.order_ref), false) + " U"}</dd></div></dl><p className="v3-panel-order-state"><QualityBadge value={order.state} reason={order.reason} />{order.had_unknown ? <span>曾 UNKNOWN</span> : null}{order.requested_units != null && Number(order.filled_units) < Number(order.requested_units) ? <span>部分成交</span> : null}</p><code>{order.order_ref}</code></div>)}
            {!orders.length ? <Empty>没有可关联的已确认成交。</Empty> : null}<p>每个订单只取最高累计成交量，部分成交回报不重复累加。</p>
          </details></div> : route.detail === "exit" ? <ExitReason data={data} /> : <div className="v3-panel-content">
          <dl className="v3-panel-facts"><div><dt>{cycle?.first_fill_at != null ? "首笔成交（北京）" : "本地成交确认（北京）"}</dt><dd>{time(timing.firstAt, "Asia/Taipei")}<small>{timing.firstLabel}</small></dd></div><div><dt>两腿平仓确认</dt><dd>{time(cycle?.flat_confirmed_at, "Asia/Taipei")}</dd></div><div><dt>本轮账本 Entropy / Lighter</dt><dd>{exact(cycle?.entropy_units)} / {exact(cycle?.hedge_units)} units</dd></div><div><dt>每个 unit</dt><dd>{exact(profit?.quantity_step)} {cycle?.symbol}</dd></div></dl>
          <div className="v3-panel-callout"><strong>{profit?.net_usd == null ? "本轮净结果待核算" : `本轮净结果 ${amount(profit.net_usd)} U`}</strong><p>{profit?.pending_fees ? `${profit.pending_fees} 笔手续费未齐。已知费用 ${amount(profit.known_fees_usd, false)} U，不能将缺失费用按零计。` : accountingLabel(profit?.accounting)}</p></div>
          <p className="v3-muted">「成交过程」说明在哪一阶段变贵或变便宜；「退出原因」展示本轮历史触发证据。</p>
          <FundingSettlements data={data} timezone={route.timezone} />
          {profit && !cycle?.paired_round_trip ? <p>已实现 {amount(profit.active_realized_usd)} U · 未实现 {amount(profit.unrealized_usd)} U。未实现部分不计入已完成交易总额。</p> : null}
        </div>}
      </div>
      <footer className="v3-panel-accounting"><dl>
        <div><dt>交易盈亏</dt><dd className={Number(profit?.realized_usd) < 0 ? "v3-loss" : "v3-good"}>{profit?.realized_usd == null ? "待核算" : amount(profit.realized_usd) + " U"}</dd></div>
        <div><dt>资金费</dt><dd>{profit?.funding_usd == null ? "待核算" : amount(profit.funding_usd) + " U"}</dd></div>
        <div><dt>手续费</dt><dd>{profit?.fees_usd == null ? "待核算" : amount(String(-Number(profit.fees_usd))) + " U"}</dd></div>
        <div><dt>净收益</dt><dd className={profit?.net_usd == null ? "v3-pending" : Number(profit.net_usd) < 0 ? "v3-loss" : "v3-good"}>{profit?.net_usd == null ? "待核算" : amount(profit.net_usd) + " U"}</dd></div>
      </dl><details><summary>交易编号与来源</summary><code>{identity}</code><p>{accountingLabel(profit?.net_usd == null ? "pending" : profit?.accounting)} · 详情为整轮记录，汇总按筛选日期归属。</p><p>费用按{profit?.fee_basis === "cycle_complete" ? "整轮完成日" : "各订单成交确认日"}归属；资金费按实际入账日归属。</p></details></footer>
      <details className="v3-panel-technical" key={identity} open={route.group ? true : undefined}><summary>执行组、订单异常与诊断证据</summary>
        <DataNotice meta={result.envelope?.meta} error={result.error} stale={result.stale} timezone={route.timezone} />
        <div className="v3-kv-grid"><KV label="阶段 / 核账" value={`${exact(cycle?.phase)} / ${exact(cycle?.accounting_state)}`} /><KV label="结果类型" value={exact(cycle?.result_kind)} /><KV label="计划 / 配对成交" value={`${exact(cycle?.planned_units)} / ${exact(cycle?.actual_units)} units`} /><KV label="本轮账本 E / L" value={`${exact(cycle?.entropy_units)} / ${exact(cycle?.hedge_units)} units`} /><KV label="账本最终净额" value={money(cycle?.finalized_net_usd)} /><KV label="最大账务负项" value={cycle?.largest_negative_component || "待核对"} /><KV label="历史退出估值" value={money(cycle?.executable_net_usd)} /><KV label="周期创建 / 结束" value={`${time(cycle?.started_at, route.timezone)} / ${time(cycle?.completed_at, route.timezone)}`} /><KV label="首笔时间来源" value={timing.firstLabel} /><KV label="缺少交易所首笔时间的订单" value={cycle?.first_fill_missing_order_refs?.length ? cycle.first_fill_missing_order_refs.join(" / ") : cycle?.first_fill_at != null ? "无" : "证据待确认"} /></div>
        {data.pnl_breakdown ? <details><summary>已核对账务公式</summary><p>成交现金结果 {money(data.pnl_breakdown.trade_cash_usd)} − 手续费 {money(data.pnl_breakdown.fees_usd)} + 资金费 {money(data.pnl_breakdown.funding_usd)} = 净额 {money(data.pnl_breakdown.net_usd)}。账务构成不代表执行因果。</p></details> : null}
        <div className="v3-panel-diagnostic-actions"><button type="button" onClick={() => void copyDiagnostic()}>复制只读诊断</button><span role="status">{copyStatus}</span></div>
        <Section title="执行片段" note="UNKNOWN不推断为零成交；部分成交保留真实累计数量。">
          {!groups.length ? <Empty>没有执行组事实记录。</Empty> : <><GroupButtons groups={mainGroups} selected={selected?.group_id || null} onSelect={id => update({ group: selected?.group_id === id ? "" : id })} />{zeroGroups.length ? <details><summary>{zeroGroups.length}组确认零成交尝试</summary><GroupButtons groups={zeroGroups} selected={selected?.group_id || null} onSelect={id => update({ group: selected?.group_id === id ? "" : id })} /></details> : null}</>}
        </Section>
        {selected ? <Section title={`执行组 ${selected.group_id}`} note="A/B/C/D来自后端匹配数量归因；未知阶段不补值。"><AttributionPanel group={selected} /><AttemptTable attempts={selected.attempts} timezone={route.timezone} /></Section> : null}
        <details><summary>原始诊断与会计字段</summary><DiagnosticSummary data={data.diagnostic} timezone={route.timezone} />{data.economics ? <pre className="v3-pre">{JSON.stringify(data.economics, null, 2)}</pre> : null}</details>
      </details>
    </>}
    </div>
  </aside></>;
}
