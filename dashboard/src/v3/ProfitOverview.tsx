import { useEffect, useState, type ReactNode } from "react";
import { Empty, Section } from "./Common";
import { cycleTiming, duration, exact, time } from "./format";
import { queryKey, routeHash, type RouteState } from "./router";
import type { Decimal, ProfitAmounts, ProfitCycle, ProfitOverview as ProfitData, V3Meta } from "./types";
import CyclePanel from "./CyclePanel";
import ExitPosition from "./ExitPosition";
import OperatorControls from "./OperatorControls";

export function amount(value?: Decimal, signed = true): string {
  if (value == null || value === "" || !Number.isFinite(Number(value))) return "待核算";
  const number = Number(value);
  const digits = Math.abs(number) > 0 && Math.abs(number) < 1 ? 6 : 4;
  return `${signed && number > 0 ? "+" : ""}${number.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: digits })}`;
}
function tone(value?: Decimal) { return value == null ? "v3-pending" : Number(value) < 0 ? "v3-loss" : Number(value) > 0 ? "v3-good" : ""; }
export function accountingLabel(value?: string) { return value === "final" ? "最终 · 已核账" : value === "pending" ? "待核算" : "暂估 · 待最终核账"; }
function AmountCells({ row, accounting = true }: { row: ProfitAmounts; accounting?: boolean }) {
  return <><td className={tone(row.funding_usd)}>{amount(row.funding_usd)}</td><td className={tone(row.realized_usd)}>{amount(row.realized_usd)}</td><td className={row.fees_usd == null ? "v3-pending" : ""}>{amount(row.fees_usd, false)}{row.fees_usd == null ? <small>已知手续费 {amount(row.known_fees_usd, false)} · 待补齐</small> : null}</td><td className={tone(row.net_usd)}>{amount(row.net_usd)}{accounting && row.accounting && row.net_usd != null ? <small>{accountingLabel(row.accounting)}</small> : null}</td></>;
}

export default function ProfitOverview({ data, meta, route, paused, update, runtime, toolbar, evidence, summary, refresh = 0 }: { data?: ProfitData | null; meta?: V3Meta; route: RouteState; paused: boolean; update: (patch: Partial<RouteState>) => void; runtime?: ReactNode; toolbar?: ReactNode; evidence?: ReactNode; summary?: ReactNode; refresh?: number }) {
  const [now, setNow] = useState(Date.now() / 1000);
  useEffect(() => { if (paused) return; const timer = window.setInterval(() => setNow(Date.now() / 1000), 1000); return () => window.clearInterval(timer); }, [paused]);
  const isExpanded = (cycle: ProfitCycle) => route.cycle === cycle.cycle_id && route.source === cycle.source_id;
  const [page, setPage] = useState(0);
  const listKey = queryKey({ ...route, source: "" });
  useEffect(() => setPage(0), [listKey]);
  if (!data?.available || !data.totals) return <><div className="v3-runtime-summary">{runtime}</div>{toolbar}<Empty>收益拆分不可用；缺失数据不补零。</Empty>{evidence}</>;
  const totals = data.totals;
  const holding = data.holding;
  const pageCount = Math.max(1, Math.ceil(data.cycles.length / 4));
  const selectedIndex = data.cycles.findIndex(isExpanded);
  const currentPage = Math.min(page, pageCount - 1);
  const items = data.cycles.slice(currentPage * 4, (currentPage + 1) * 4);
  const scopeWindow = meta?.window;
  const scopeName = { session: "最近启动后", batch: "当前批次", today: "今日", custom: "所选时间", "7d": "近 7 天", all: "全部历史", "24h": "最近 24 小时" }[scopeWindow?.scope || route.scope] || route.scope;
  const selectedWindow = scopeWindow || Object.values(meta?.deployment_windows || {})[0];
  const missing = [totals.pending_realized ? "交易盈亏" : "", totals.pending_funding ? "资金费" : "", totals.pending_fees ? "手续费" : ""].filter(Boolean).join("、");
  const view = route.view || "complete";
  const title = ({complete: "已完成交易", open: "进行中的交易", incomplete: "未完整结束的交易", filled: "全部成交交易", unfilled: "未成交尝试", loss: "已核账亏损交易", profit: "已核账盈利交易"} as Record<string, string>)[view] || "交易记录";
  const holdingLink = routeHash({ ...route, page: "cycles", view: "open", cycle: "", source: "", group: "", cursor: "" });
  return <div className="v3-profit" data-testid="profit-overview">
    <div className="v3-overview-heading"><h1>总览</h1>{toolbar}</div>
    <div className={`v3-overview-workspace ${route.cycle && route.source ? "has-detail" : ""}`}><div className="v3-overview-main">
    <div className="v3-runtime-bar"><div className="v3-runtime-summary" aria-label="当前运行状态">{runtime}</div><OperatorControls source={route.mode === "funding" || route.symbol === "NBIS" ? "funding" : "basis"} /><span className="v3-update-time">更新 {time(meta?.generated_at, route.timezone)}</span></div>
    <div className="v3-profit-highlights">
      <div><span>{view === "complete" ? "已完成圈次" : title}</span><strong>{totals.cycle_count}<small> 轮</small></strong><p>{scopeName} · {route.scope === "all" ? "不限时间" : selectedWindow ? `${time(selectedWindow.from_ts, route.timezone)} 起` : "范围待确认"}</p></div>
      <div><span>待核算</span><strong>{totals.pending_cycles}<small> 轮</small></strong><a href={routeHash({ ...route, page: "cycles", view: "filled", flag: "accounting_pending", cycle: "", source: "", group: "", cursor: "" })}>查看核算证据 ›</a></div>
      <div className="v3-net-highlight"><span>净收益 · U</span><strong className={tone(totals.net_usd)}>{amount(totals.net_usd)}</strong><p>{totals.net_usd == null ? data.coverage_missing_sources ? "来源缺失，金额待确认" : `${missing || "核账数据"}未齐` : totals.accounting === "final" ? "已扣手续费 · 已核算" : "已扣已知费用 · 暂估"}</p></div>
    </div>
    <ExitPosition holding={holding} missing={Boolean(data.coverage_missing_sources)} holdingLink={holdingLink} timezone={route.timezone} />
    {route.cycle && selectedIndex < 0 ? <p className="v3-history-context">右侧所选历史交易不在当前筛选范围，详情显示该轮完整记录。</p> : null}
    <Section title={view === "complete" ? "最近交易圈" : title} action={<a href={routeHash({ ...route, page: "cycles", cycle: "", source: "", group: "", cursor: "" })}>共 {data.cycles.length} 圈 · 查看全部 ›</a>}>
      <div className="v3-table-wrap"><table className="v3-table v3-profit-cycles"><thead><tr><th>标的 / 模式</th><th>首笔成交（北京）</th><th>整圈持仓时长</th><th>净结果 · U</th><th>详情</th></tr></thead><tbody>
        {items.map(cycle => { const timing = cycleTiming(cycle, paused ? meta?.generated_at : now); return <tr key={`${cycle.source_id}:${cycle.cycle_id}`} data-cycle={cycle.cycle_id} data-source={cycle.source_id} className={isExpanded(cycle) ? "is-expanded" : undefined}><td><b>{cycle.symbol} / {cycle.strategy_mode === "funding" ? "资金费" : "普通"}</b></td><td>{time(timing.firstAt, "Asia/Taipei")}<small>{timing.firstLabel}</small></td><td>{duration(timing.seconds)}<small>{timing.durationLabel}</small></td><td className={tone(cycle.net_usd)}>{amount(cycle.net_usd)}{cycle.net_usd == null ? <small>费用或核账数据未齐</small> : cycle.accounting !== "final" ? <small>暂估</small> : null}</td><td><button className="v3-profit-expand" type="button" aria-expanded={isExpanded(cycle)} aria-controls="cycle-panel" onClick={() => update({cycle: cycle.cycle_id, source: cycle.source_id || "basis", group: ""})}>查看 ›</button></td></tr>; })}
      </tbody></table></div>
      {!items.length ? <div className="v3-readable-empty"><strong>{view === "complete" ? "所选时间内还没有完成的交易" : "所选时间内暂无这类交易"}</strong><p>{view === "complete" ? "新交易出现后会自动更新。历史交易可通过时间范围查看。" : "清除上方圈次筛选可查看已完成交易。"}</p></div> : null}
      {pageCount > 1 ? <div className="v3-pagination"><button type="button" disabled={currentPage === 0} onClick={() => { setPage(currentPage - 1); update({cycle: "", source: "", group: ""}); }}>上一页</button><span>{currentPage + 1} / {pageCount}</span><button type="button" disabled={currentPage + 1 >= pageCount} onClick={() => { setPage(currentPage + 1); update({cycle: "", source: "", group: ""}); }}>下一页</button></div> : null}
    </Section>
    {summary}
    <details className="v3-secondary-panel" key={queryKey({ ...route, source: "" })}><summary>更多汇总与运行详情</summary>
    <Section title="分币种" note="汇总与圈次 使用同一归属口径；金额单位 USD。">
      {data.symbols.length ? <div className="v3-table-wrap"><table className="v3-table v3-profit-summary"><thead><tr><th>币种 / 双腿</th><th>资金费</th><th>价格盈亏</th><th>手续费</th><th>净结果</th><th>仓位周期</th><th>成交订单</th></tr></thead><tbody>
        {data.symbols.map(row => <tr key={row.symbol}><td><b>{row.symbol}</b><small>Entropy / Lighter</small></td><AmountCells row={row} /><td>{row.cycle_count}</td><td>{row.order_count}</td></tr>)}
        <tr className="v3-profit-total"><td>合计</td><AmountCells row={totals} /><td>{totals.cycle_count}</td><td>{data.symbols.reduce((sum, row) => sum + row.order_count, 0)}</td></tr>
      </tbody></table></div> : <Empty>此范围没有成交或入账记录。</Empty>}
    </Section>
    <Section title="按天（北京时间）" note="当日归属结果 = 当日价格盈亏 + 资金费 − 手续费；不是整轮净额再加一次资金费。">
      {data.daily.length ? <div className="v3-table-wrap"><table className="v3-table v3-profit-summary"><thead><tr><th>日期 · UTC+8</th><th>资金费</th><th>价格盈亏</th><th>手续费</th><th>当日归属结果</th></tr></thead><tbody>{data.daily.map(row => <tr key={row.day}><td>{row.day}</td><AmountCells row={row} /></tr>)}</tbody></table></div> : <Empty>所选范围没有已归属事件。</Empty>}
      <p className="v3-profit-footnote">{data.rules}</p>
    </Section>
    {evidence}
    </details>
    </div>{route.cycle && route.source ? <CyclePanel route={route} paused={paused} refresh={refresh} update={update} /> : null}</div>
  </div>;
}
