import { memo } from "react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { localTime, money, statusLabel } from "../format";
import type { Attribution, PnlResponse, StatusResponse } from "../types";

const MAX_RENDER_POINTS = 720;

export const PnlChart = memo(function PnlChart({ pnl, today, attribution }: {
  pnl: PnlResponse | null;
  today: StatusResponse["today"];
  attribution: Attribution | null;
}) {
  // The API applies the same budget. Keep a client-side guard so a rolling
  // upgrade or a malformed response can never hand an unbounded series to
  // Recharts.
  const lifetime = attribution?.lifetime;
  const lifetimePoints = pnl?.lifetime_points ?? [];
  const useLifetime = Boolean(lifetime?.equity_net_pnl_usd != null
    && lifetimePoints.length);
  const sourcePoints = useLifetime ? lifetimePoints : (pnl?.points ?? []);
  const points = sourcePoints.length > MAX_RENDER_POINTS
    ? sourcePoints.slice(-MAX_RENDER_POINTS)
    : sourcePoints;
  const headline = useLifetime
    ? lifetime?.equity_net_pnl_usd ?? null
    : today.net_pnl_usd;
  const coverage = useLifetime && lifetime?.coverage_start_ts
    ? `最早可用权益快照自 ${localTime(lifetime.coverage_start_ts)} 起 · ${lifetime.confidence}`
    : today.day_started_ts
      ? `${today.coverage_complete ? "完整自然日" : "首日"}，自 ${localTime(today.day_started_ts)} 起按账户权益计算`
      : "等待两家交易所返回完整账户权益";
  return <section className="pnl-section" aria-labelledby="pnl-title">
    <div className="section-head pnl-head"><div><h2 id="pnl-title">{useLifetime ? "全生命周期净盈亏" : "今日净盈亏"}</h2><p>{coverage}</p></div><strong className={tone(headline)}>{money(headline)}</strong></div>
    {useLifetime && lifetime ? <div className="pnl-breakdown">
      <span>已实现 <b>{money(lifetime.gross_realized_usd)}</b></span>
      <span>浮动 <b>{money(lifetime.unrealized_usd)}</b></span>
      <span>Entropy 手续费（{statusLabel(lifetime.fees_status)}） <b>{money(lifetime.fees_usd)}</b></span>
      <span>资金费 <b>{money(lifetime.funding_pnl_usd)}</b></span>
      <span>今日净盈亏 <b>{money(today.net_pnl_usd)}</b></span>
    </div> : <div className="pnl-breakdown">
      <span>本次运行盈亏 <b>{money(today.session_pnl_usd)}</b></span>
      <span>Entropy 手续费（{statusLabel(today.fees_status)}） <b>{money(today.fees_usd)}</b></span>
      <span>资金费 <b>{money(today.funding_pnl_usd)}</b></span>
      <span>成交额 <b>{money(today.turnover_usd, true)}</b></span>
    </div>}
    <div className="pnl-chart">
      {points.length ? <ResponsiveContainer width="100%" height="100%"><AreaChart data={points} margin={{ top: 8, right: 10, bottom: 0, left: -12 }}>
        <defs><linearGradient id="pnlFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#4f8895" stopOpacity=".28"/><stop offset="100%" stopColor="#4f8895" stopOpacity="0"/></linearGradient></defs>
        <CartesianGrid stroke="#162633" vertical={false}/><XAxis dataKey="ts" tickFormatter={value => localTime(Number(value))} stroke="#516571" tickLine={false} axisLine={false}/><YAxis tickFormatter={v => `$${v}`} stroke="#516571" tickLine={false} axisLine={false}/>
        <Tooltip labelFormatter={v => localTime(Number(v))} formatter={value => [money(Number(value)), "净盈亏"]}/><Area dataKey="net_pnl_usd" stroke="#76a9b4" fill="url(#pnlFill)" dot={false} isAnimationActive={false}/>
      </AreaChart></ResponsiveContainer> : <div className="empty-state"><span />盈亏数据暂不可用，系统不会展示未经计算的估值。</div>}
    </div>
  </section>;
});

function tone(value: number | null) {
  if (value == null || Math.abs(value) < .005) return "";
  return value > 0 ? "tone-ok" : "tone-loss";
}
