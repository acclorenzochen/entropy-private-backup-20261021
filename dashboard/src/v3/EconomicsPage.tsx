import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { DataNotice, Empty, KV, Metric, QualityBadge, Section } from "./Common";
import { withQuery } from "./client";
import { exact, finiteChart, money, time } from "./format";
import { commonQuery, queryKey, type RouteState } from "./router";
import type { EconomicsData, ResearchData } from "./types";
import { useResource } from "./useResource";

export default function EconomicsPage({ route, paused }: { route: RouteState; paused: boolean }) {
  const key = queryKey(route);
  const economics = useResource<EconomicsData>(`economics|${key}`, () => withQuery("economics", commonQuery(route)), 5_000, paused);
  const research = useResource<ResearchData>(`research|${key}`, () => withQuery("research", commonQuery(route)), 30_000, paused);
  const data = economics.envelope?.data;
  const reports = research.envelope?.data.reports || [];
  const curve = (data?.cumulative || []).map(point => ({ ts: point.ts, pnl: finiteChart(point.cumulative_pnl_usd) }));
  return <div className="v3-page"><div className="v3-page-title"><div><span className="v3-eyebrow">ECONOMICS / RESEARCH</span><h1>收益与研究</h1><p>最终交易结果与离线研究分开；当前退出估值不是已实现收益。</p></div></div>
    <DataNotice meta={economics.envelope?.meta} error={economics.error} stale={economics.stale} timezone={route.timezone} />
    {!data ? <Empty>{economics.loading ? "正在读取 canonical 会计…" : "V3 收益不可用；不会用账户余额或旧账本代填。"}</Empty> : <>
      <div className="v3-metrics v3-metrics-four"><Metric label="最终净收益" value={money(data.finalized_net_pnl_usd)} note="canonical；手续费/资金费已计入，不二次扣除" tone={data.finalized_net_pnl_usd?.startsWith("-") ? "loss" : undefined} />
        <Metric label="已完成 cycles" value={exact(data.completed_cycles)} note="不等于完整配对套利循环" />
        <Metric label="待最终会计" value={exact(data.pending_accounting_cycles)} note="未知金额不填 0" />
        <Metric label="当前退出估值" value={money(data.current_executable_net_usd)} note="活动仓位估值，与最终净收益分列" /></div>
      <Section title="最终账本累计净收益" note="按 cycle 完成时间归属，显示当前对账结果；不是历史账户净值，也不是逐笔现金流曲线。">
        {curve.length ? <div className="v3-chart v3-chart-short"><ResponsiveContainer width="100%" height="100%"><LineChart data={curve} margin={{ top: 12, right: 16, bottom: 8, left: 5 }}>
          <CartesianGrid stroke="#263846" strokeDasharray="3 5" vertical={false} /><XAxis dataKey="ts" type="number" domain={["dataMin", "dataMax"]} tickFormatter={value => time(Number(value), route.timezone)} stroke="#8498a3" tick={{ fontSize: 11 }} minTickGap={42} /><YAxis stroke="#8498a3" tick={{ fontSize: 11 }} width={66} unit="$" />
          <Tooltip contentStyle={{ background: "#10202b", border: "1px solid #39515e", color: "#dce6e9" }} labelFormatter={value => time(Number(value), route.timezone)} formatter={value => [value == null ? "—" : `${value} USD`, "累计最终净收益"]} />
          <Line dataKey="pnl" name="累计最终净收益" stroke="#72c4a7" strokeWidth={2} dot={curve.length < 30} connectNulls={false} isAnimationActive={false} />
        </LineChart></ResponsiveContainer></div> : <Empty>没有已完成且最终会计的曲线点；缺少记录不等于收益为零。</Empty>}
      </Section>
      <Section title="执行归因" note="仅展示后端已完成的配对归因；不从 bp 自行推导 USD。">
        {data.attribution && Object.keys(data.attribution).length ? <div className="v3-card"><div className="v3-kv-grid">{Object.entries(data.attribution).map(([name, value]) => <KV key={name} label={name} value={typeof value === "object" ? "见证据" : exact(value as string | number | boolean | null)} />)}</div><details><summary>查看完整只读归因</summary><pre className="v3-pre">{JSON.stringify(data.attribution, null, 2)}</pre></details></div> : <Empty>缺少可验证的归因；不把收益变化归咎于某个执行阶段。</Empty>}
      </Section>
    </>}
    <Section title="离线研究报告" note="候选参数只有查看与下载，不可在此应用到实盘。样本不足时不推荐最优值。">
      <DataNotice meta={research.envelope?.meta} error={research.error} stale={research.stale} timezone={route.timezone} />
      {reports.length ? <div className="v3-report-list">{reports.map(report => <article className="v3-card" key={report.id}><div className="v3-card-top"><h3>{report.title || report.id}</h3><QualityBadge value={report.status} reason={report.reason} /></div>
        <div className="v3-kv-grid"><KV label="样本数" value={exact(report.sample_count)} /><KV label="训练" value={exact(report.train_range)} /><KV label="验证" value={exact(report.validation_range)} /><KV label="最终 holdout" value={exact(report.holdout_range)} /></div>
        {report.candidates?.length ? <div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>候选</th><th>质量</th><th>报告指标</th></tr></thead><tbody>{report.candidates.map((candidate, index) => <tr key={`${candidate.label}-${index}`}><td>{candidate.label}</td><td><QualityBadge value={candidate.quality} reason={candidate.reason} /></td><td>{candidate.metrics ? Object.entries(candidate.metrics).map(([name, value]) => `${name}: ${exact(value)}`).join(" · ") : "未提供"}</td></tr>)}</tbody></table></div> : <p className="v3-muted">{report.reason || "没有已验证候选。"}</p>}
        {report.download_url?.startsWith("/api/v3/") ? <a href={report.download_url} download>下载已生成的报告 ↗</a> : null}
      </article>)}</div> : <Empty>{research.envelope?.data.reason || "没有已生成且可验证的研究报告；INSUFFICIENT_DATA 时不展示参数建议。"}</Empty>}
    </Section>
  </div>;
}
