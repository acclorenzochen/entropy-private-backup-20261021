import { money, number, signed, statusLabel } from "../format";
import type { StatusResponse } from "../types";

export function KpiRow({ status }: { status: StatusResponse }) {
  const deltaBad = Math.abs(status.positions.net_delta_base) > status.risk.net_tolerance_base;
  const cleanCirclesPerHour = status.cycle_kpis.clean_circles_per_hour;
  const cleanDetail = status.cycle_kpis.clean_circle_count == null
    ? "SQLite 周期账本 · 等待完成周期"
    : `Clean ${number(status.cycle_kpis.clean_circle_count, 0)} · Recovery ${number(status.cycle_kpis.recovery_circle_count, 0)}`;
  const items = [
    { label: "Clean Circle / 小时", value: cleanCirclesPerHour == null ? "--" : number(cleanCirclesPerHour, 2), detail: cleanDetail },
    { label: "当前溢价", value: `${signed(status.market.premium_mid_bps)} 基点`, detail: `动态中枢 ${signed(status.strategy.active_midline_bps)}` },
    { label: "持仓状态", value: statusLabel(status.pair_state), detail: `${signed(status.positions.entropy_base, 4)} / ${signed(status.positions.hedge_base, 4)}` },
    { label: "净敞口", value: signed(status.positions.net_delta_base, 4), detail: money(status.positions.net_delta_usd), alert: deltaBad },
    { label: "今日盈亏", value: money(status.today.net_pnl_usd), detail: `本次运行 ${money(status.today.session_pnl_usd)} · ${number(status.today.trade_count, 0)} 笔` },
  ];
  return <section className="kpi-rail" aria-label="当前指标">
    {items.map(item => <div className={`kpi ${item.alert ? "is-alert" : ""}`} key={item.label}>
      <span>{item.label}</span><strong>{item.value}</strong><small>{item.detail}</small>
    </div>)}
  </section>;
}
