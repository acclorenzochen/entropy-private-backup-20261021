import type { ReactNode } from "react";
import { DataNotice, Empty, KV, QualityBadge, Section, SourceTable } from "./Common";
import { withQuery } from "./client";
import { batchLimit, configValue, duration, exact, money, textField, time, timestampField } from "./format";
import { commonQuery, queryKey, type RouteState } from "./router";
import type { Deployment, HealthData, SourceInfo, SystemData } from "./types";
import ResearchPanel from "./ResearchPanel";
import { useResource } from "./useResource";

const primaryConfigLabels: Record<string, string> = { max_notional_usd: "每腿预算 · U", entry_band_bps: "入场偏离 · bp", exit_min_gross_profit_bps: "退出毛利润 · bp", strategy_mode: "策略模式" };
function displayConfig(config: SystemData["config"], compact = false) {
  if (!config) return <Empty>没有 live 有效配置证据；recorder 配置不会冒充实盘授权。</Empty>;
  const rows: { group: string; key: string; label?: string | null; configured?: string | number | boolean | null; authorized?: string | number | boolean | null; effective?: string | number | boolean | null; configured_present?: boolean; effective_present?: boolean; source?: string | null; explanation?: string | null }[] = Array.isArray(config) ? config : Object.entries(config).map(([key, value]) => ({ group: "effective", key, effective_present: true, effective: value === null ? null : typeof value === "object" ? JSON.stringify(value) : value as string | number | boolean | null }));
  return <div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>{compact ? "项目" : "组 / 参数"}</th><th>配置值</th><th>授权值</th><th>生效值</th><th>来源 / 说明</th></tr></thead><tbody>
    {rows.filter(item => !compact || primaryConfigLabels[item.key]).map((item, index) => <tr key={`${item.group}-${item.key}-${index}`}><td>{primaryConfigLabels[item.key] || `${item.group} / ${item.label || item.key}`}</td><td>{configValue(item.key, item.configured, item.configured_present)}</td><td>{configValue(item.key, item.authorized)}</td><td>{configValue(item.key, item.effective, item.effective_present)}</td><td>{item.source === "live_effective_manifest" ? "运行配置快照" : item.source || "—"}<small>{item.explanation || ""}</small></td></tr>)}
  </tbody></table></div>;
}

export default function SystemPage({ route, paused, update, deployments, toolbar, refresh = 0 }: { route: RouteState; paused: boolean; update: (patch: Partial<RouteState>) => void; deployments: Deployment[]; toolbar?: ReactNode; refresh?: number }) {
  const selected = deployments.find(item => item.source_id === route.source) || deployments[0];
  const sourceRoute = { ...route, source: selected?.source_id || "basis" as const };
  const key = queryKey(sourceRoute);
  const system = useResource<SystemData>(`system|${key}`, () => withQuery("system", commonQuery(sourceRoute)), 10_000, paused || !selected, refresh);
  const health = useResource<HealthData>(`health|${key}`, () => withQuery("health", commonQuery(sourceRoute)), 10_000, paused || !selected, refresh);
  const data = system.envelope?.data;
  const sourceNames: Record<string, string> = { live: "运行状态", market: "固定 q 监控", live_market: "Live 摘要", journal: "交易账本", recorder: "监控状态" };
  const combined: Record<string, SourceInfo> = {};
  for (const deployment of deployments) for (const [name, source] of Object.entries(deployment.source_health)) if (sourceNames[name]) combined[`${deployment.symbols.join(" / ")} · ${sourceNames[name]}`] = source;
  const sources = data?.sources || health.envelope?.data.source_health || system.envelope?.meta.sources;
  const marketSources = deployments.flatMap(deployment => ["live_market", "market"].map(name => ({ name: `${deployment.symbols.join(" / ")} · ${name === "live_market" ? "实时摘要" : "固定量深度"}`, use: name === "live_market" ? "中间价差" : "整笔可成交价差", source: deployment.source_health[name] })));
  return <div className="v3-page v3-system-page"><div className="v3-page-title"><div><h1>系统证据</h1><p>源事件时间、采集覆盖与生效配置；API 更新不代表行情更新。</p></div><div className="v3-live-summary"><QualityBadge value={health.envelope?.data.api?.status} /><span>展示 API</span></div></div>
    {toolbar}<div className="v3-segmented" aria-label="系统证据视图"><button type="button" aria-pressed={route.evidence === "sources"} onClick={() => update({ evidence: "sources" })}>来源与配置</button><button type="button" aria-pressed={route.evidence === "research"} onClick={() => update({ evidence: "research" })}>离线研究报告</button></div>
    {route.evidence === "sources" ? <Section title="数据来源与新鲜度" note="实时摘要与固定量深度分别保存，不能互相替代。"><div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>来源</th><th>用途</th><th>最新证据</th><th>状态</th></tr></thead><tbody>{marketSources.map(({ name, use, source }) => <tr key={name}><td>{name}</td><td>{use}</td><td><span title={source?.age_s != null ? `源年龄 ${source.age_s < 1 ? "不足 1 秒" : duration(source.age_s)}` : "源年龄未知"}>{time(source?.observed_at, route.timezone)}</span></td><td><QualityBadge value={source?.quality} reason={source?.reason} />{source?.reason === "missing" ? <small>未采集 / 来源缺失</small> : null}</td></tr>)}</tbody></table></div></Section> : null}
    <div className="v3-source-tabs" aria-label="证据标的">{deployments.map(item => <button type="button" key={item.source_id} aria-pressed={item.source_id === selected?.source_id} onClick={() => update({ source: item.source_id, cycle: "", group: "" })}>{item.symbols.join(" / ")} <small>{item.mode === "funding" ? "资金费" : "普通"}</small></button>)}</div>
    {!selected ? <Empty>当前筛选没有可读取的来源。</Empty> : route.evidence === "research" ? <ResearchPanel route={sourceRoute} paused={paused} refresh={refresh} /> : <>
    <DataNotice compact meta={system.envelope?.meta} error={system.error} stale={system.stale} timezone={route.timezone} />
    {!data ? <Empty>{system.loading ? "正在读取来源与配置…" : "V3 系统信息不可用。"}</Empty> : <>
      <Section title="配置、授权与实际生效" note="配置快照不代表控制 owner 已接通；未知授权保持 —。">{displayConfig(data.config, true)}<details className="v3-secondary-panel"><summary>全部配置参数</summary>{displayConfig(data.config)}</details></Section>
      <details className="v3-secondary-panel"><summary>全部来源、会话与读取时间</summary><SourceTable sources={combined} timezone={route.timezone} /><SourceTable sources={sources} timezone={route.timezone} /></details>
      <div className="v3-layout-two"><Section title="运行与监控兼容性" note="角色 build hash 可以不同；应比较共同核心、配置、pair 与 schema。"><div className="v3-card"><div className="v3-kv-grid">
        <KV label="整体" value={<QualityBadge value={data.compatibility?.status} reason={data.compatibility?.reason} />} />
        <KV label="共同核心" value={data.compatibility?.shared_core_hash_match == null ? "未知" : data.compatibility.shared_core_hash_match ? "一致" : "不一致"} />
        <KV label="有效配置" value={data.compatibility?.config_match == null ? "未知" : data.compatibility.config_match ? "一致" : "不一致"} />
        <KV label="交易对" value={data.compatibility?.pair_match == null ? "未知" : data.compatibility.pair_match ? "一致" : "不一致"} />
        <KV label="schema 能力" value={data.compatibility?.schema_match == null ? "未知" : data.compatibility.schema_match ? "一致" : "不一致"} />
      </div><p className="v3-muted">{data.compatibility?.reason || "未提供兼容性解释。"}</p></div></Section>
        <Section title="批次与开仓约束" note="只读展示，不支持解除 safety latch 或调高额度。"><div className="v3-card"><div className="v3-kv-grid">
          <KV label="批次" value={exact(data.batch?.batch_id)} /><KV label="已用 / 圈数上限" value={`${exact(data.batch?.position_cycles)} / ${batchLimit(data.batch?.max_position_cycles)}`} />
          <KV label="累计亏损 / 预算" value={`${money(data.batch?.loss_used_usd)} / ${batchLimit(data.batch?.loss_budget_usd, " USD")}`} />
          <KV label="禁止新开仓" value={data.batch?.closed_for_entries == null ? "未知" : data.batch.closed_for_entries ? "是" : "否"} />
          <KV label="关闭原因" value={exact(data.batch?.close_reason)} /><KV label="触发值 / 阈值" value={`${exact(data.batch?.close_value)} / ${exact(data.batch?.close_threshold)}`} />
        </div></div></Section></div>
      <div className="v3-layout-two"><Section title="能力与缺口" note="已实现、已启用、最近有数据、证据完整分别判断。">
        {Array.isArray(data.capabilities) && data.capabilities.length ? <div className="v3-card">{data.capabilities.map(item => <div className="v3-kv" key={item.name}><span>{item.name}</span><b><QualityBadge value={item.status} reason={item.reason} /></b></div>)}</div> : data.capabilities && !Array.isArray(data.capabilities) ? <div className="v3-card"><div className="v3-kv-grid">{Object.entries(data.capabilities).map(([name, value]) => <KV key={name} label={name} value={typeof value === "object" ? "见来源详情" : exact(value as string | number | boolean | null)} />)}</div></div> : <Empty>能力矩阵未提供。</Empty>}
      </Section><Section title="运行指纹" note="完整指纹仅用于排查；两个角色的完整 hash 不要求相等。"><div className="v3-card"><div className="v3-kv-grid">
        <KV label="live 会话" value={textField(data.live, "session_id") || textField(data.live, "process_id") || "未知"} />
        <KV label="live build" value={textField(data.live, "build_hash") || "未知"} />
        <KV label="recorder 会话" value={textField(data.recorder, "session_id") || "未知"} />
        <KV label="recorder build" value={textField(data.recorder, "build_hash") || "未知"} />
        <KV label="recorder 最后源时间" value={timestampField(data.recorder, "source_ts", route.timezone)} />
      </div></div></Section></div>
      {data.warnings?.length ? <Section title="来源提示" note="状态未知时不会自动消除提示。"><div className="v3-alerts">{data.warnings.map((warning, index) => <p className="v3-alert" key={`${warning}-${index}`}>{warning}</p>)}</div></Section> : null}
    </>}
    </>}
  </div>;
}
