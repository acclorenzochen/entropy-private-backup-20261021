import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { DataNotice, Empty, Section } from "./Common";
import { getV3, withQuery } from "./client";
import { exact, finiteChart, money, time } from "./format";
import { commonQuery, queryKey, type RouteState } from "./router";
import type { Deployment, EconomicsData, Envelope } from "./types";
import { useResource } from "./useResource";

interface SourceEconomics { source_id: string; label: string; data: EconomicsData }
export default function EconomicsSummary({ route, paused, deployments, refresh = 0 }: { route: RouteState; paused: boolean; deployments: Deployment[]; refresh?: number }) {
  const listRoute = { ...route, source: "" as const };
  const result = useResource<{ sources: SourceEconomics[] }>(`economics-summary|${queryKey(listRoute)}|${deployments.map(d => d.source_id).join()}`, async signal => {
    const parts = await Promise.all(deployments.map(async deployment => {
      const envelope = await getV3<EconomicsData>(withQuery("economics", commonQuery({ ...listRoute, source: deployment.source_id })), signal);
      return { deployment, envelope };
    }));
    const first = parts[0]?.envelope;
    if (!first) throw new Error("没有可读取的收益来源");
    return { meta: { ...first.meta, generated_at: Math.max(...parts.map(part => part.envelope.meta.generated_at)), warnings: parts.flatMap(part => part.envelope.meta.warnings) }, data: { sources: parts.map(({ deployment, envelope }) => ({ source_id: deployment.source_id, label: deployment.symbols.join(" / "), data: envelope.data })) } } as Envelope<{ sources: SourceEconomics[] }>;
  }, 10_000, paused, refresh);
  const sources = result.envelope?.data.sources || [];
  const rows = new Map<number, Record<string, number | null>>();
  for (const source of sources) for (const point of source.data.cumulative || []) {
    const row = rows.get(point.ts) || { ts: point.ts };
    row[source.source_id] = finiteChart(point.cumulative_pnl_usd); rows.set(point.ts, row);
  }
  const curve = Array.from(rows.values()).sort((a, b) => Number(a.ts) - Number(b.ts));
  // Only carry each source's confirmed cumulative result between its own events.
  const last: Record<string, number | null> = {};
  for (const row of curve) for (const source of sources) { if (Object.hasOwn(row, source.source_id)) last[source.source_id] = row[source.source_id]; else row[source.source_id] = last[source.source_id] ?? null; }
  return <div className="v3-economics-summary"><Section title="累计已核净收益" note="按圈次完成时间累计 · 已扣费用，各来源分别展示。">
    {result.error || result.stale ? <DataNotice compact meta={result.envelope?.meta} error={result.error} stale={result.stale} timezone={route.timezone} /> : null}
    {curve.length ? <div className="v3-chart v3-chart-short"><ResponsiveContainer width="100%" height="100%"><LineChart data={curve} margin={{ top: 12, right: 16, bottom: 6, left: 4 }}><CartesianGrid stroke="#263846" strokeDasharray="3 5" vertical={false} /><XAxis dataKey="ts" type="number" domain={["dataMin", "dataMax"]} tickFormatter={value => time(Number(value), route.timezone)} tick={{ fontSize: 12 }} minTickGap={50} /><YAxis unit=" U" width={70} tick={{ fontSize: 12 }} /><Tooltip labelFormatter={value => time(Number(value), route.timezone)} formatter={value => [value == null ? "未知" : `${Number(value).toFixed(6)} U`]} /><Legend />{sources.map((source, index) => <Line key={source.source_id} dataKey={source.source_id} name={source.label} stroke={index ? "#7db7d3" : "#72c4a7"} strokeWidth={2} dot={curve.length < 20} connectNulls={false} isAnimationActive={false} />)}</LineChart></ResponsiveContainer></div> : <Empty>{result.loading ? "正在读取最终账本…" : "暂无已核算曲线点，缺失数据不记为零。"}</Empty>}
    <details className="v3-secondary-panel"><summary>收益研究汇总与归因</summary><div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>来源</th><th>最终净收益</th><th>完成圈次</th><th>待最终核算</th><th>当前退出估值</th></tr></thead><tbody>{sources.map(source => <tr key={source.source_id}><td>{source.label}</td><td>{money(source.data.finalized_net_pnl_usd)}</td><td>{exact(source.data.completed_cycles)}</td><td>{exact(source.data.pending_accounting_cycles)}</td><td>{money(source.data.current_executable_net_usd)}</td></tr>)}</tbody></table></div>{sources.map(source => <details key={source.source_id}><summary>{source.label} 已记录执行归因</summary><pre className="v3-pre">{JSON.stringify(source.data.attribution || { reason: "归因证据缺失" }, null, 2)}</pre></details>)}</details>
  </Section></div>;
}
