import { useEffect, useMemo, useState, type ReactNode } from "react";
import { CartesianGrid, Legend, Line, LineChart, ReferenceArea, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { DataNotice, Empty, Metric, QualityBadge, Section } from "./Common";
import { withQuery } from "./client";
import { exact, finiteChart, textField, time, timestampField } from "./format";
import { commonQuery, queryKey, type RouteState } from "./router";
import type { Decimal, Deployment, MarketData, MarketPoint } from "./types";
import { useResource } from "./useResource";


function displayBasis(value: Decimal | undefined): string {
  return value == null || !Number.isFinite(Number(value)) ? "未知" : `${Number(value).toFixed(3)} bp`;
}

interface ChartPoint { ts: number; long: number | null; short: number | null; band: number | null; buy: number | null; sell: number | null; mid: number | null; reference: number | null }
const coverageReasons: Record<string, string> = {
  series_not_requested: "切换到该系列后读取覆盖", missing: "没有该系列的采集文件", no_recorder_source: "未采集固定数量盘口深度", recorder_source_missing: "未采集固定数量盘口深度",
  historical_backfill_in_progress: "历史缓存正在回填", no_recorder_samples_in_selected_window: "所选时段没有固定数量采样",
  no_live_samples_in_selected_window: "所选时段没有 live 摘要", window_starts_before_recorder_coverage: "起点早于实际覆盖",
  window_ends_after_recorder_coverage: "终点晚于最后采样", window_starts_before_live_summary_coverage: "起点早于摘要覆盖", window_starts_before_live_coverage: "起点早于摘要覆盖", window_ends_after_live_summary_coverage: "终点晚于最后摘要", window_ends_after_live_coverage: "终点晚于最后摘要",
  recorder_samples_have_gaps_or_invalid_frames: "包含真实断点或无效盘口", live_summary_samples_have_gaps_or_invalid_frames: "包含摘要断点或无效记录", live_samples_have_gaps_or_invalid_frames: "包含摘要断点或无效记录",
  dashboard_cache_read_failed: "历史缓存读取失败，当前覆盖不完整", live_summary_source_stale: "最后摘要已过期", recorder_source_stale: "最后盘口采样已过期", source_stale: "最后采样已过期", missing_live_source: "没有 live 摘要文件",
};
function coverageNotes(coverage: Record<string, unknown> | null | undefined): string {
  if (!coverage) return "覆盖信息待确认";
  const reasons = Array.isArray(coverage?.reasons) ? coverage.reasons as string[] : [textField(coverage, "reason")].filter(Boolean) as string[];
  return reasons.length ? reasons.map(reason => coverageReasons[reason] || reason).join(" · ") : "已读取实际保存的采样";
}
export function chartSeries(points: MarketPoint[], live = false): ChartPoint[] {
  const rows: ChartPoint[] = [];
  let previous: MarketPoint | null = null;
  for (const point of points) {
    // The API propagates gaps from capture order before downsampling. Sample spacing is not a capture gap.
    if (previous && (point.continuity_break === true || (point.session_id && previous.session_id && point.session_id !== previous.session_id) || (point.segment_id != null && previous.segment_id != null && point.segment_id !== previous.segment_id))) {
      rows.push({ ts: (point.ts + previous.ts) / 2, long: null, short: null, band: null, buy: null, sell: null, mid: null, reference: null });
    }
    const valid = live ? point.valid !== false : point.valid === true && point.quality !== "invalid";
    rows.push({ ts: point.ts, long: valid ? finiteChart(point.long_edge_bps) : null, short: valid ? finiteChart(point.short_edge_bps) : null,
      band: valid ? finiteChart(point.entry_band_bps) : null, buy: valid ? finiteChart(point.buy_basis_bps) : null, sell: valid ? finiteChart(point.sell_basis_bps) : null,
      mid: valid ? finiteChart(point.mid_basis_bps) : null, reference: valid && (!live || point.reference_usable === true) ? finiteChart(point.reference_bps) : null });
    previous = point;
  }
  return rows;
}
export default function MarketPage({ route, update, paused, deployments, toolbar, refresh = 0 }: { route: RouteState; update: (patch: Partial<RouteState>) => void; paused: boolean; deployments: Deployment[]; toolbar?: ReactNode; refresh?: number }) {
  const selected = deployments.find(item => item.source_id === route.source) || deployments[0];
  const sourceRoute = { ...route, source: selected?.source_id || "basis" as const };
  const [draftUnits, setDraftUnits] = useState(route.units);
  const [unitError, setUnitError] = useState("");
  const [episodesExpanded, setEpisodesExpanded] = useState(false);
  useEffect(() => { setDraftUnits(route.units); setUnitError(""); }, [route.units, selected?.source_id]);
  const result = useResource<MarketData>(`market|${queryKey(sourceRoute)}|${route.units}|${route.chart === "live" ? "live" : "fixed"}`, () => {
    const query = commonQuery(sourceRoute); if (route.units) query.set("units", route.units); query.set("max_points", "1200"); query.set("series", route.chart === "live" ? "live" : "fixed"); return withQuery("market", query);
  }, 10_000, paused || !selected, refresh);
  const data = result.envelope?.data;
  const live = route.chart === "live";
  const points = live ? data?.live_points || [] : data?.points || [];
  const coverage = live ? data?.live_coverage : data?.coverage;
  const series = useMemo(() => chartSeries(points, live), [points, live]);
  const latest = points.length ? points[points.length - 1] : null;
  const quantityVerified = data?.units != null && (!route.units || Number(data.units) === Number(route.units));
  const submitUnits = () => {
    const units = draftUnits.trim();
    if (units && (!/^\d+$/.test(units) || Number(units) <= 0 || Number(units) > 1_000_000)) { setUnitError("数量需为 1–1,000,000 的整数 units。"); return; }
    setUnitError(""); update({ units });
  };
  const selectedWindow = result.envelope?.meta.window;
  const first = typeof coverage?.from_ts === "number" ? coverage.from_ts : null;
  const last = typeof coverage?.to_ts === "number" ? coverage.to_ts : null;
  const thresholdSource = textField(data?.series_sources?.fixed_q, "entry_band_source");
  return <div className="v3-page v3-market-page"><div className="v3-page-title"><div><h1>价差监控</h1><p>行情摘要与固定数量可成交报价分别展示。</p></div></div>
    {toolbar}<div className="v3-source-tabs" aria-label="监控标的">{deployments.map(item => <button type="button" key={item.source_id} aria-pressed={item.source_id === selected?.source_id} onClick={() => update({ source: item.source_id, units: "", cycle: "", group: "" })}>{item.symbols.join(" / ")} <small>{item.mode === "funding" ? "资金费" : "普通"}</small></button>)}</div>
    {!selected ? <Empty>当前筛选没有监控来源，请调整标的或模式。</Empty> : <>
    <div className="v3-toolbar"><div className="v3-segmented" aria-label="价差系列"><button type="button" aria-pressed={live} onClick={() => update({ chart: "live" })}>Live 中间价</button><button type="button" aria-pressed={route.chart === "edge"} onClick={() => update({ chart: "edge" })}>固定 q 入场偏离</button><button type="button" aria-pressed={route.chart === "basis"} onClick={() => update({ chart: "basis" })}>固定 q 可成交价差</button></div>
      {!live ? <><label>数量 <input aria-label="固定数量 units" type="number" min="1" max="1000000" value={draftUnits} placeholder="展示数量" onChange={event => setDraftUnits(event.target.value)} onKeyDown={event => { if (event.key === "Enter") submitUnits(); }} /></label><button type="button" onClick={submitUnits}>应用数量</button></> : null}</div>
    {unitError ? <p className="v3-form-error" role="alert">{unitError}</p> : null}
    <DataNotice compact meta={result.envelope?.meta} error={result.error} stale={result.stale} timezone={route.timezone} />
    <div className="v3-series-status"><QualityBadge value={textField(coverage, "quality")} /><span>{live ? "Live 已保存摘要 · 无盘口深度" : `独立监控 · ${exact(data?.units, " units")}${data?.quantity_step != null && data?.units != null ? ` = ${(Number(data.units) * Number(data.quantity_step)).toLocaleString("en-US", { maximumFractionDigits: 8 })} ${selected.symbols.join(" / ")}` : " · 基数量待确认"}`}</span>{coverage?.backfill_complete === false ? <span className="v3-pending">历史缓存回填中</span> : null}<small>最新来源 {time(latest?.ts, route.timezone)}</small></div>
    {!live ? <p className="v3-muted">{route.units ? "自定义研究数量" : data?.units_source === "current_paired_position_units" ? "展示数量取自配对账本量" : "展示数量取自最近计划，不代表历史实际下单量"} · 图表不证明策略曾评估或下单。</p> : <p className="v3-muted">中间价与参考线来自实际保存的 live 摘要，不能代表可成交报价或执行结果。</p>}
    <div className="v3-metrics v3-market-metrics">{live ? <><Metric label="中间价差" value={displayBasis(latest?.mid_basis_bps)} /><Metric label="已记录参考线" value={latest?.reference_usable === true ? displayBasis(latest?.reference_bps) : "未知 / 不可用"} /><Metric label="最新摘要" value={time(latest?.ts, route.timezone)} /></> : <><Metric label={route.chart === "edge" ? "Entropy 多头偏离" : "买入 Entropy 价差"} value={displayBasis(route.chart === "edge" ? latest?.long_edge_bps : latest?.buy_basis_bps)} /><Metric label={route.chart === "edge" ? "Entropy 空头偏离" : "卖出 Entropy 价差"} value={displayBasis(route.chart === "edge" ? latest?.short_edge_bps : latest?.sell_basis_bps)} /><Metric label={thresholdSource === "recorded_config" ? "已记录门槛" : "当前参考门槛"} value={displayBasis(latest?.entry_band_bps)} note={thresholdSource === "recorded_config" ? "来自匹配的历史配置" : "历史重算假设，非当时决策证据"} /></>}</div>
    <Section title={live ? "Live 中间价摘要" : route.chart === "edge" ? "固定数量入场偏离" : "固定数量可成交价差"} note="仅真实采集断点、无效记录与已记录边界断线；抽样间距不会制造断点。">
      {!live && data && !quantityVerified ? <Empty>服务端数量与请求不一致，绘图暂停。</Empty> : series.length ? <div className="v3-chart"><ResponsiveContainer width="100%" height="100%"><LineChart data={series} margin={{ top: 18, right: 18, bottom: 8, left: 0 }}><CartesianGrid stroke="#263846" strokeDasharray="3 5" vertical={false} />
        <XAxis dataKey="ts" type="number" domain={[selectedWindow?.from_ts ?? "dataMin", selectedWindow?.to_ts ?? "dataMax"]} tickFormatter={value => time(Number(value), route.timezone)} stroke="#9aabbc" tick={{ fontSize: 12 }} minTickGap={48} /><YAxis width={64} unit=" bp" stroke="#9aabbc" tick={{ fontSize: 12 }} />
        <Tooltip contentStyle={{ background: "#132131", border: "1px solid #39515e", color: "#dce6e9" }} labelFormatter={value => time(Number(value), route.timezone)} formatter={(value, name) => [value == null ? "未知" : `${Number(value).toFixed(3)} bp`, name]} /><Legend />
        {first != null && selectedWindow?.from_ts != null && first - selectedWindow.from_ts > 10 ? <ReferenceArea x1={selectedWindow.from_ts} x2={first} fill="#6b7280" fillOpacity={0.10} label={{ value: "未覆盖", fill: "#9aabbc", fontSize: 12 }} /> : null}
        {last != null && selectedWindow?.to_ts != null && selectedWindow.to_ts - last > 10 ? <ReferenceArea x1={last} x2={selectedWindow.to_ts} fill="#6b7280" fillOpacity={0.10} label={{ value: "无后续采样", fill: "#9aabbc", fontSize: 12 }} /> : null}
        {live ? <><Line dataKey="mid" name="Live 中间价差" stroke="#72c4a7" strokeWidth={2} dot={series.length < 50} connectNulls={false} isAnimationActive={false} /><Line dataKey="reference" name="已保存参考线" stroke="#d7a65f" strokeWidth={1.5} dot={false} connectNulls={false} isAnimationActive={false} /></> : route.chart === "edge" ? <><Line dataKey="long" name="Entropy 多头偏离" stroke="#72c4a7" strokeWidth={2} dot={series.length < 50} connectNulls={false} isAnimationActive={false} /><Line dataKey="short" name="Entropy 空头偏离" stroke="#7db7d3" strokeWidth={2} dot={series.length < 50} connectNulls={false} isAnimationActive={false} /><Line dataKey="band" name="参考门槛（历史重算）" stroke="#d7a65f" strokeDasharray="5 5" dot={false} connectNulls={false} isAnimationActive={false} /></> : <><Line dataKey="buy" name="买入 Entropy / 卖出 Lighter" stroke="#72c4a7" strokeWidth={2} dot={false} connectNulls={false} isAnimationActive={false} /><Line dataKey="sell" name="卖出 Entropy / 买入 Lighter" stroke="#7db7d3" strokeWidth={2} dot={false} connectNulls={false} isAnimationActive={false} /><Line dataKey="reference" name="已保存参考线" stroke="#d7a65f" dot={false} connectNulls={false} isAnimationActive={false} /></>}
      </LineChart></ResponsiveContainer></div> : <Empty>{result.loading ? "正在读取已保存采样…" : live ? "当前范围没有 live 摘要。" : "未采集该标的的固定数量盘口深度；不能从中间价补造可成交报价。"}</Empty>}
    </Section>
    <div className="v3-coverage-strip"><strong>实际覆盖</strong><span>{timestampField(coverage, "from_ts", route.timezone)} → {timestampField(coverage, "to_ts", route.timezone)}</span><span>原始 {exact(textField(coverage, "loaded_rows"))} / 展示 {exact(textField(coverage, "display_points"))} 点</span><span>真实断点 / 无效 {coverage?.loaded_rows ? exact(textField(coverage, "discontinuity_count")) : "未知"}</span><p>{coverageNotes(coverage)}</p></div>
    <details className="v3-secondary-panel"><summary>两套数据的来源与覆盖</summary><div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>系列</th><th>覆盖</th><th>质量</th><th>缺口</th></tr></thead><tbody>{[["Live 中间价摘要", data?.live_coverage], ["固定 q 独立监控", data?.coverage]].map(([name, value]) => { const c = value as Record<string, unknown> | undefined; return <tr key={String(name)}><td>{String(name)}</td><td>{timestampField(c, "from_ts", route.timezone)} → {timestampField(c, "to_ts", route.timezone)}</td><td><QualityBadge value={textField(c, "quality")} /></td><td>{coverageNotes(c)}</td></tr>; })}</tbody></table></div><pre className="v3-pre">{JSON.stringify(data?.series_sources || { reason: "来源元数据待确认" }, null, 2)}</pre></details>
    {!live ? <details className="v3-secondary-panel"><summary>固定 q 观察到的机会片段</summary><p className="v3-muted">按参考门槛重算的研究片段，未证明当时策略评估、物理限价或实际下单。</p>{data?.episodes?.length ? <div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>起止时间</th><th>方向 / 数量</th><th>最大偏离</th><th>采样点</th><th>质量</th></tr></thead><tbody>{data.episodes.slice(0, episodesExpanded ? 50 : 10).map((episode, index) => <tr key={`${episode.start_ts}-${episode.direction}-${index}`}><td>{time(episode.start_ts, route.timezone)}<small>{time(episode.end_ts, route.timezone)}</small></td><td>{episode.direction === "long_entropy" ? "Entropy 多头" : "Entropy 空头"}<small>{episode.units} units</small></td><td>{displayBasis(episode.max_edge_bps)}</td><td>{episode.sample_count}</td><td><QualityBadge value={episode.quality} /></td></tr>)}</tbody></table>{data.episodes.length > 10 ? <button type="button" onClick={() => setEpisodesExpanded(value => !value)}>{episodesExpanded ? "收起" : "查看更多"}</button> : null}{data.episodes_truncated ? <p className="v3-muted">仅返回最近 50 个片段。</p> : null}</div> : <Empty>所选范围没有已观察到的片段；缺口不能判定为没有机会。</Empty>}</details> : null}
    <details className="v3-secondary-panel"><summary>监控诊断证据</summary><p>{textField(data?.diagnostic, "reason") || "当前没有已记录的 live 决策诊断。"}</p><pre className="v3-pre">{JSON.stringify(data?.diagnostic || { reason: "not_recorded" }, null, 2)}</pre></details>
    </>}
  </div>;
}
