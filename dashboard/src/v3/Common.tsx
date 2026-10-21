import { useState, type ReactNode } from "react";
import { basis, exact, field, gateLabel, metaLine, money, qualityLabel, textField, time, timestampField } from "./format";
import type { Alert, EntryDiagnostic, EvidenceValue, GateStatus, PositionSide, SourceInfo, V3Meta } from "./types";
import type { RouteState } from "./router";

export function Section({ title, note, children, action }: { title: string; note?: string; children: ReactNode; action?: ReactNode }) {
  return <section className="v3-section"><div className="v3-section-head"><div><h2>{title}</h2>{note ? <p>{note}</p> : null}</div>{action}</div>{children}</section>;
}
export function Metric({ label, value, note, tone }: { label: string; value: ReactNode; note?: string; tone?: "loss" | "good" }) {
  return <div className="v3-metric"><span>{label}</span><strong className={tone ? `v3-${tone}` : ""}>{value}</strong>{note ? <small>{note}</small> : null}</div>;
}
export function KV({ label, value, note }: { label: string; value: ReactNode; note?: string }) {
  return <div className="v3-kv"><span>{label}{note ? <small title={note}> ⓘ</small> : null}</span><b>{value}</b></div>;
}
export function Empty({ children = "该范围暂无已确认记录；如果来源覆盖不完整，不能据此判定为零。" }: { children?: ReactNode }) {
  return <div className="v3-empty">{children}</div>;
}
export function QualityBadge({ value, reason }: { value?: string | null; reason?: string | null }) {
  const normalized = value?.toLowerCase() || "unknown";
  const tone = ["valid", "good", "available", "consistent", "pass"].includes(normalized) ? "good" :
    ["invalid", "unavailable", "fail"].includes(normalized) ? "bad" : "warn";
  return <span className={`v3-badge v3-badge-${tone}`} title={reason || value || "证据质量未知"}>{qualityLabel(value)}{reason ? " ⓘ" : ""}</span>;
}
export function DataNotice({ meta, error, stale, timezone, compact = false }: { meta?: V3Meta | null; error?: string | null; stale?: boolean; timezone: RouteState["timezone"]; compact?: boolean }) {
  return <div className={`v3-data-notice ${error ? "v3-notice-error" : ""}`} role={error ? "alert" : "status"}>
    {error ? <span>{stale ? "显示上次成功快照；" : "V3 数据不可用；"}{error}</span> : null}
    {meta ? <span>{compact ? `展示更新 ${time(meta.generated_at, timezone)}` : `${metaLine(meta, timezone)} · API生成 ${time(meta.generated_at, timezone)}`}</span> : null}
    {meta?.warnings?.length ? <span>{compact ? "部分来源待核对，缺口见系统证据。" : meta.warnings.join(" · ")}</span> : null}
  </div>;
}
export function TraderFreshness({ meta, error, stale, timezone }: { meta?: V3Meta | null; error?: string | null; stale?: boolean; timezone: RouteState["timezone"] }) {
  return <div className={`v3-trader-freshness ${error ? "has-error" : ""}`} role={error ? "alert" : "status"}>
    {error ? (stale ? `显示上次成功数据：${error}` : `数据暂不可用：${error}`) :
      meta ? `看板刷新 ${time(meta.generated_at, timezone)}${meta.consistency !== "consistent" ? " · 部分来源待核对" : ""}` : "正在连接只读数据…"}
  </div>;
}
export function SourceTable({ sources, timezone }: { sources?: Record<string, SourceInfo>; timezone: RouteState["timezone"] }) {
  const rows = Object.entries(sources || {});
  if (!rows.length) return <Empty>没有来源元数据；API 正常响应不代表 live 或 recorder 正常。</Empty>;
  return <div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>来源</th><th>质量</th><th>源事件时间</th><th>读取时间</th><th>源年龄</th><th>会话 / 版本</th><th>说明</th></tr></thead><tbody>
    {rows.map(([name, source]) => <tr key={name}><td>{name}</td><td><QualityBadge value={source.quality} reason={source.reason} /></td>
      <td>{time(source.observed_at, timezone)}</td><td>{time(source.read_at, timezone)}</td><td>{exact(source.age_s, " s")}</td>
      <td title={source.build_hash || undefined}>{exact(source.session)}{source.schema != null ? ` · schema ${source.schema}` : ""}</td>
      <td>{source.reason || "—"}{source.market_change_at || source.stream_alive_at || source.captured_at ? <small>盘口变化 {time(source.market_change_at, timezone)} · 流存活 {time(source.stream_alive_at, timezone)} · 捕获 {time(source.captured_at, timezone)}</small> : null}</td></tr>)}
  </tbody></table></div>;
}
export function Alerts({ alerts, timezone }: { alerts?: Alert[]; timezone: RouteState["timezone"] }) {
  const [hidden, setHidden] = useState<string[]>([]);
  const visible = (alerts || []).filter(alert => !hidden.includes(alert.id));
  if (!visible.length) return <Empty>没有可显示的已确认告警；来源缺失时不能视为风险已解除。</Empty>;
  return <div className="v3-alerts">{visible.map(alert => <div className="v3-alert" key={alert.id}>
    <div><span className="v3-alert-category">{alert.category || "来源状态"} · {alert.severity || "提示"}</span><p>{alert.message}</p>
      <small>{alert.source || "来源未知"} · {time(alert.observed_at, timezone)} · <QualityBadge value={alert.quality} /></small></div>
    <button type="button" className="v3-text-button" title="仅在当前页面暂时隐藏，不更改交易告警" onClick={() => setHidden(ids => [...ids, alert.id])}>本机隐藏</button>
  </div>)}</div>;
}
export function Evidence({ side, label, timezone }: { side?: PositionSide; label: string; timezone: RouteState["timezone"] }) {
  const entry = (title: string, value?: EvidenceValue | null) => <div className="v3-evidence-cell" key={title}><span>{title}</span><strong>{exact(value?.units, " units")}</strong><small>{exact(value?.base_quantity, " base")} · {value?.source || value?.reason || "来源缺失"} · {time(value?.observed_at, timezone)}</small></div>;
  return <div className="v3-evidence"><h3>{label}</h3><div className="v3-evidence-grid">{entry("交易所确认", side?.authoritative)}{entry("成交推导 expected", side?.expected)}{entry("Journal 持久化", side?.journal)}</div></div>;
}
export function EntryDiagnosticPanel({ diagnostic, timezone }: { diagnostic?: EntryDiagnostic | Record<string, unknown> | null; timezone: RouteState["timezone"] }) {
  if (!diagnostic) return <Empty>live 决策 trace 未提供；recorder 的盘口观察不能代替实际拒绝原因。</Empty>;
  const directions = field(diagnostic, "directions");
  const directionList = Array.isArray(directions) ? directions as EntryDiagnostic["directions"] : undefined;
  return <div className="v3-diagnostic"><p className="v3-callout">实盘决策证据 · {timestampField(diagnostic, "evaluated_at", timezone)} · {textField(diagnostic, "summary_reason") || "详细拒绝原因未记录"}</p>
    {directionList?.length ? directionList.map((item, index) => <div className="v3-card" key={`${item.side}-${index}`}><h3>{item.side} · {exact(item.q_units, " units")}</h3>
      <div className="v3-kv-grid"><KV label="可成交价差" value={basis(item.buy_or_sell_basis_bps)} /><KV label="偏离" value={basis(item.edge_bps)} /><KV label="入场 band" value={basis(item.band_bps)} /><KV label="Scenario / floor" value={`${basis(item.scenario_bps)} / ${basis(item.floor_bps)}`} /></div>
      <p className="v3-muted">拒绝阶段：{item.reject_stage || "未记录"}</p>
      {item.gates?.length ? <div className="v3-gates">{item.gates.map((gate, n) => <span key={`${gate.name}-${n}`} title={gate.reason || undefined}>{gate.name}：{gateLabel(gate.status as GateStatus)}</span>)}</div> : null}
    </div>) : <details><summary>查看已返回的只读诊断字段</summary><pre className="v3-pre">{JSON.stringify(diagnostic, null, 2)}</pre></details>}
  </div>;
}
export function DiagnosticSummary({ data, title = "只读诊断", timezone }: { data?: Record<string, unknown> | null; title?: string; timezone: RouteState["timezone"] }) {
  if (!data) return <Empty>尚无 {title}；不能从状态名称推断具体原因。</Empty>;
  const entry = field(data, "entry") || field(data, "entry_diagnostic");
  const exit = field(data, "exit") || field(data, "exit_diagnostic");
  const position = field(data, "position_evidence");
  return <div className="v3-stack">
    {entry ? <EntryDiagnosticPanel diagnostic={entry as Record<string, unknown>} timezone={timezone} /> : null}
    {exit ? <div className="v3-card"><h3>当前退出判断 · live</h3><div className="v3-kv-grid">
      <KV label="本轮全仓可执行价格毛盈亏" value={money(textField(exit, "executable_gross_usd"))} />
      <KV label="费用后估值 · 复盘" value={money(textField(exit, "executable_net_usd"))} />
      <KV label="真实锚点" value={basis(textField(exit, "anchor_bps"))} />
      <KV label="回归量" value={basis(textField(exit, "reversion_bps"))} />
      <KV label="正常退出毛收益门槛" value={money(textField(exit, "normal_exit_min_gross_profit_usd"))} />
      <KV label="毛收益达到门槛" value={gateLabel(field(exit, "normal_exit_rule") as GateStatus)} />
    </div><small className="v3-muted">普通 V3：本轮已实现价格盈亏 + 剩余全仓盘口预计价格盈亏达到上方 live 门槛；手续费、资金费、额外执行缓冲不参与门槛。触发后两腿并发 reduce-only IOC，确认实际仓位后持续补平残量，保留限价保护。风控与残仓恢复优先；资金费模式保留各两次结算规则。锚点来源：{textField(exit, "anchor_source") || "未知"} · 条件由 live 计算，看板不重算</small></div> : null}
    {position ? <div className="v3-card"><h3>仓位事实</h3><Evidence side={field(position, "entropy") as PositionSide} label="Entropy" timezone={timezone} /><Evidence side={field(position, "lighter") as PositionSide} label="Lighter" timezone={timezone} />
      <p className="v3-muted">配平：{textField(position, "balance_state") || "未知"} · 待配平 {exact(textField(position, "residual_units"), " units")}</p></div> : null}
    {!entry && !exit && !position ? <div className="v3-card"><p>{textField(data, "reason") || textField(data, "summary_reason") || "返回了诊断对象，但没有可识别的分层字段。"}</p><details><summary>查看原始只读字段</summary><pre className="v3-pre">{JSON.stringify(data, null, 2)}</pre></details></div> : null}
  </div>;
}
