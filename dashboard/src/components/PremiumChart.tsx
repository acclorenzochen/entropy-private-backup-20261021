import { memo } from "react";
import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { localTime, signed } from "../format";
import type { HistoryPoint } from "../types";

type Range = "1h" | "6h" | "24h";
const ranges: Range[] = ["1h", "6h", "24h"];
const MAX_RENDER_POINTS = 720;

function ChartTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: HistoryPoint }> }) {
  const point = payload?.[0]?.payload;
  if (!active || !point) return null;
  return <div className="chart-tooltip">
    <time>{localTime(point.ts)}</time>
    <strong>{signed(point.premium_bps)} bps</strong>
    <span>动态中枢 {signed(point.active_midline_bps)}</span>
    <span>短中枢 {signed(point.short_center_bps)}</span>
  </div>;
}

export const PremiumChart = memo(function PremiumChart({ points, range, onRange, loading }: {
  points: HistoryPoint[]; range: Range; onRange: (range: Range) => void; loading: boolean;
}) {
  const renderPoints = points.length > MAX_RENDER_POINTS
    ? points.slice(-MAX_RENDER_POINTS)
    : points;
  return <section className="chart-section" aria-labelledby="premium-title">
    <div className="section-head">
      <div><h1 id="premium-title">跨市场溢价</h1><p>Entropy 中间价相对对冲市场中间价，单位为基点</p></div>
      <div className="range-control" aria-label="溢价历史范围">
        {ranges.map(value => <button key={value} onClick={() => onRange(value)} aria-pressed={range === value}>{value === "1h" ? "1 小时" : value === "6h" ? "6 小时" : "24 小时"}</button>)}
      </div>
    </div>
    <div className="chart-frame">
      {renderPoints.length ? <ResponsiveContainer width="100%" height="100%">
        <LineChart data={renderPoints} margin={{ top: 12, right: 10, bottom: 2, left: -16 }}>
          <CartesianGrid stroke="#162633" vertical={false} />
          <XAxis dataKey="ts" type="number" domain={["dataMin", "dataMax"]} tickFormatter={value => localTime(Number(value))} minTickGap={72} stroke="#516571" tickLine={false} axisLine={false} />
          <YAxis tickFormatter={v => `${v}`} width={54} stroke="#516571" tickLine={false} axisLine={false} domain={["auto", "auto"]} />
          <Tooltip content={<ChartTooltip />} cursor={{ stroke: "#718692", strokeDasharray: "2 4" }} />
          <ReferenceLine y={0} stroke="#354753" />
          <Line dataKey="short_entry_bps" stroke="#8e6936" strokeDasharray="3 5" dot={false} isAnimationActive={false} strokeWidth={1} connectNulls />
          <Line dataKey="short_exit_bps" stroke="#6e5c42" dot={false} isAnimationActive={false} strokeWidth={1} connectNulls />
          <Line dataKey="long_exit_bps" stroke="#365c67" dot={false} isAnimationActive={false} strokeWidth={1} connectNulls />
          <Line dataKey="long_entry_bps" stroke="#355b69" strokeDasharray="3 5" dot={false} isAnimationActive={false} strokeWidth={1} connectNulls />
          <Line dataKey="short_center_bps" stroke="#7d8b91" strokeDasharray="2 5" dot={false} isAnimationActive={false} strokeWidth={1} connectNulls />
          <Line dataKey="active_midline_bps" stroke="#6aa1b4" dot={false} isAnimationActive={false} strokeWidth={1.25} connectNulls />
          <Line dataKey="premium_bps" stroke="#e4edf0" dot={false} isAnimationActive={false} strokeWidth={1.8} />
        </LineChart>
      </ResponsiveContainer> : <div className="empty-state"><span className={loading ? "pulse-line" : ""} />{loading ? "正在加载溢价历史…" : "正在等待市场历史数据…"}</div>}
    </div>
    <div className="chart-legend" aria-label="图表图例"><span className="line-premium">溢价</span><span className="line-mid">动态中枢</span><span>短中枢</span><span className="line-entry">入场线</span><span className="line-exit">离场线</span></div>
  </section>;
});
