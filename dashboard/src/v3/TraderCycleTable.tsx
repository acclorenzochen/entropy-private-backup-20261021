import { cycleTiming, duration, exact, money, time } from "./format";
import type { RouteState } from "./router";
import type { CycleSummary } from "./types";

export function directionLabel(side?: string | null): string {
  if (side === "long_entropy") return "Entropy 多 / Lighter 空";
  if (side === "short_entropy") return "Entropy 空 / Lighter 多";
  return side || "方向待确认";
}

export function cycleOutcome(cycle: CycleSummary): string {
  if (cycle.paired_round_trip) return "完整圈";
  if (cycle.funding_state && cycle.phase === "DONE" && cycle.has_confirmed_fill) {
    return cycle.funding_state.completed ? "已归零 · 两周期完成" : "已归零 · 两周期未完成";
  }
  if (cycle.phase === "DONE" && cycle.paired_opened) return "已归零 · 待核对";
  if (cycle.phase === "DONE") return cycle.has_confirmed_fill ? "已结束 · 非完整圈" : "未成交";
  if (cycle.has_confirmed_fill && !cycle.paired_opened) return "有成交 · 待配平";
  if (cycle.has_confirmed_fill) return "持仓 / 处理中";
  return "未确认成交";
}

export default function TraderCycleTable({ items, route, update }: { items: CycleSummary[]; route: RouteState; update: (patch: Partial<RouteState>) => void }) {
  return <div className="v3-table-wrap"><table className="v3-table v3-trader-table"><thead><tr>
    <th>标的 / 首笔成交</th><th>方向 / 成交与结果</th><th>整圈持仓时长</th><th>最终净结果 · U</th><th>详情</th>
  </tr></thead><tbody>{items.map(cycle => { const timing = cycleTiming(cycle); return <tr key={`${cycle.source_id}:${cycle.cycle_id}`} data-source={cycle.source_id} data-cycle={cycle.cycle_id} className={route.cycle === cycle.cycle_id && route.source === cycle.source_id ? "is-expanded" : undefined}>
    <td><b>{cycle.symbol} · {cycle.strategy_mode === "funding" ? "资金费" : "普通"}</b><small>{time(timing.firstAt, route.timezone)}</small><small>{timing.firstLabel}</small><small>{cycle.flat_confirmed_at ? `权威归零 ${time(cycle.flat_confirmed_at, route.timezone)}` : cycle.phase === "DONE" ? "归零时间证据缺失" : "尚未归零"}</small></td>
    <td>{directionLabel(cycle.side)}<small>{cycle.actual_units != null ? `配对 ${exact(cycle.actual_units, " units")}` : cycle.has_confirmed_fill ? "单腿成交，数量待配对" : "未确认配对成交"}</small><small><span className={cycle.has_confirmed_fill && !cycle.paired_opened && cycle.phase !== "DONE" ? "v3-loss" : ""}>{cycleOutcome(cycle)}</span></small>{cycle.funding_state ? <small>资金费结算 Entropy {exact(cycle.funding_state.settlement_counts?.entropy)} / Lighter {exact(cycle.funding_state.settlement_counts?.hedge)} · {money(cycle.funding_state.actual_funding_usd)}</small> : null}</td>
    <td>{duration(timing.seconds)}<small>{timing.durationLabel}</small></td>
    <td title={cycle.finalized_net_usd == null ? undefined : `${cycle.finalized_net_usd} USD`} className={cycle.finalized_net_usd?.startsWith("-") ? "v3-loss" : ""}>{!cycle.has_confirmed_fill ? "不计入交易盈亏" : cycle.accounting_state === "final" && cycle.finalized_net_usd != null ? `${Number(cycle.finalized_net_usd).toLocaleString("en-US", { minimumFractionDigits:2, maximumFractionDigits:6 })} U` : "待最终会计"}{cycle.finalized_net_usd?.startsWith("-") ? <small>{cycle.largest_negative_component || "分项待核对"}</small> : null}</td>
    <td><button type="button" className="v3-profit-expand" aria-controls="cycle-panel" aria-expanded={route.cycle === cycle.cycle_id && route.source === cycle.source_id} disabled={!cycle.source_id} onClick={() => update({ source: cycle.source_id || "", cycle: cycle.cycle_id, group: "" })}>{!cycle.has_confirmed_fill ? "查看尝试" : cycle.finalized_net_usd?.startsWith("-") ? "查看亏损" : "查看交易"}</button></td>
  </tr>; })}</tbody></table></div>;
}
