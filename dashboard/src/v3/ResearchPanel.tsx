import { DataNotice, Empty, KV, QualityBadge, Section } from "./Common";
import { withQuery } from "./client";
import { exact } from "./format";
import { commonQuery, queryKey, type RouteState } from "./router";
import type { ResearchData } from "./types";
import { useResource } from "./useResource";

export default function ResearchPanel({ route, paused, refresh = 0 }: { route: RouteState; paused: boolean; refresh?: number }) {
  const research = useResource<ResearchData>(`research|${queryKey(route)}`, () => withQuery("research", commonQuery(route)), 30_000, paused, refresh);
  const reports = research.envelope?.data.reports || [];
  return <div>
    <Section title="离线研究报告" note="候选参数只有查看与下载，不可在此应用到实盘。样本不足时不推荐最优值。">
      <DataNotice compact meta={research.envelope?.meta} error={research.error} stale={research.stale} timezone={route.timezone} />
      {reports.length ? <div className="v3-report-list">{reports.map(report => <article className="v3-card" key={report.id}><div className="v3-card-top"><h3>{report.title || report.id}</h3><QualityBadge value={report.status} reason={report.reason} /></div>
        <div className="v3-kv-grid"><KV label="样本数" value={exact(report.sample_count)} /><KV label="训练" value={exact(report.train_range)} /><KV label="验证" value={exact(report.validation_range)} /><KV label="最终 holdout" value={exact(report.holdout_range)} /></div>
        {report.candidates?.length ? <div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>候选</th><th>质量</th><th>报告指标</th></tr></thead><tbody>{report.candidates.map((candidate, index) => <tr key={`${candidate.label}-${index}`}><td>{candidate.label}</td><td><QualityBadge value={candidate.quality} reason={candidate.reason} /></td><td>{candidate.metrics ? Object.entries(candidate.metrics).map(([name, value]) => `${name}: ${exact(value)}`).join(" · ") : "未提供"}</td></tr>)}</tbody></table></div> : <p className="v3-muted">{report.reason || "没有已验证候选。"}</p>}
        {report.download_url?.startsWith("/api/v3/") ? <a href={report.download_url} download>下载已生成的报告 ↗</a> : null}
      </article>)}</div> : <Empty>{research.envelope?.data.reason === "INSUFFICIENT_DATA" ? "样本不足，暂无可验证的研究报告，不展示参数建议。" : research.envelope?.data.reason || "没有已生成且可验证的研究报告。"}</Empty>}
    </Section>
  </div>;
}
