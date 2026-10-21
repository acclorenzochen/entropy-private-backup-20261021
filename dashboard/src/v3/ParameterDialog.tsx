import { useEffect, useRef } from "react";

export interface ParameterValues {
  entry_band_bps: string; exit_min_gross_profit_bps: string; entropy_exit_room_bps: string;
  lighter_exit_room_bps: string; max_notional_usd: string;
  entropy_open_room_bps?: string | null; lighter_open_room_bps?: string | null;
}
export interface ParameterRevision { revision: number; config_hash: string; values: ParameterValues }
export interface OpeningPolicy { rooms_bps: string[]; hard_cap_bps: string }
export interface OpeningPolicies { entropy: OpeningPolicy; lighter: OpeningPolicy }
export interface OwnerParameters {
  base_config_hash: string; current: ParameterRevision; saved: ParameterRevision | null;
  can_save: boolean; reason_code: string | null; limits: ParameterValues;
  capabilities?: { opening_concessions?: boolean };
  opening_policies?: { base: OpeningPolicies; current: OpeningPolicies; saved: OpeningPolicies | null };
}
export const parameterFields: { key: keyof ParameterValues; label: string; unit: string; opening?: "entropy" | "lighter" }[] = [
  { key: "entry_band_bps", label: "开仓偏离", unit: "bp" },
  { key: "entropy_open_room_bps", label: "Entropy 开仓让价", unit: "bp", opening: "entropy" },
  { key: "lighter_open_room_bps", label: "Lighter 开仓让价", unit: "bp", opening: "lighter" },
  { key: "exit_min_gross_profit_bps", label: "平仓毛盈利", unit: "bp" },
  { key: "entropy_exit_room_bps", label: "Entropy 平仓让价", unit: "bp" },
  { key: "lighter_exit_room_bps", label: "Lighter 平仓让价", unit: "bp" },
  { key: "max_notional_usd", label: "单腿金额", unit: "U" },
];
export function parameterTarget(parameters: OwnerParameters | undefined) { return parameters?.saved || parameters?.current; }
export function parameterIdentity(parameters: OwnerParameters | undefined) {
  const target = parameterTarget(parameters);
  return target ? `${target.revision}:${target.config_hash}` : "";
}
export function openingField(key: keyof ParameterValues) { return key === "entropy_open_room_bps" || key === "lighter_open_room_bps"; }
export function openingConcessionsSupported(parameters: OwnerParameters | undefined) { return parameters?.capabilities?.opening_concessions === true; }
export function normalizedParameterValue(key: keyof ParameterValues, value: string | null | undefined) {
  return openingField(key) && (value == null || !value.trim()) ? null : value;
}
export function sameParameterValues(left: ParameterValues | null, right: ParameterValues | undefined) {
  return Boolean(left && right && parameterFields.every(({ key }) => normalizedParameterValue(key, left[key]) === normalizedParameterValue(key, right[key])));
}
export function parameterPayload(values: ParameterValues, parameters: OwnerParameters): ParameterValues {
  if (!openingConcessionsSupported(parameters) && parameterFields.some(({ key, opening }) => opening && normalizedParameterValue(key, values[key]) != null)) throw new Error("当前 owner 不支持调整开仓让价，请放弃草稿或等待支持该能力的 owner 恢复。");
  const payload = { ...values };
  delete payload.entropy_open_room_bps; delete payload.lighter_open_room_bps;
  if (openingConcessionsSupported(parameters)) {
    payload.entropy_open_room_bps = normalizedParameterValue("entropy_open_room_bps", values.entropy_open_room_bps) ?? null;
    payload.lighter_open_room_bps = normalizedParameterValue("lighter_open_room_bps", values.lighter_open_room_bps) ?? null;
  }
  return payload;
}
export function parameterValidation(values: ParameterValues | null, limits: ParameterValues | undefined, openingSupported = false) {
  if (!values || !limits) return "参数接口不可用，需升级 owner 后配置。";
  for (const { key, label } of parameterFields) {
    const opening = openingField(key);
    if (opening && !openingSupported) {
      if (normalizedParameterValue(key, values[key]) != null) return "当前 owner 不支持调整开仓让价，请放弃草稿或等待支持该能力的 owner 恢复。";
      continue;
    }
    const raw = normalizedParameterValue(key, values[key]);
    if (opening && raw == null) continue;
    const value = Number(raw), limit = Number(limits[key]);
    if (typeof raw !== "string" || !/^\+?(?:\d+(?:\.\d{0,8})?|\.\d{1,8})$/.test(raw.trim()) || !Number.isFinite(value) || (opening ? value < 0 : value <= 0) || !Number.isFinite(limit) || (opening ? limit < 0 : limit <= 0) || value > limit) return `${label}须为${opening ? "不小于 0" : "大于 0"}且不超过 ${limits[key]} 的有限数值，最多 8 位小数。`;
  }
  return "";
}
export function ParameterFacts({ value, title, openingPolicies }: { value: ParameterRevision; title: string; openingPolicies?: OpeningPolicies | null }) {
  return <section className="v3-parameter-revision"><strong>{title} · revision {value.revision}</strong><code>{value.config_hash}</code><p>{parameterFields.map(({ key, label, unit }) => `${label} ${normalizedParameterValue(key, value.values[key]) == null ? "继承原策略" : `${value.values[key]} ${unit}`}`).join(" · ")}</p>{openingPolicies ? <small>开仓策略：Entropy {openingPolicies.entropy.rooms_bps.join(" / ")} bp，上限 {openingPolicies.entropy.hard_cap_bps} bp；Lighter {openingPolicies.lighter.rooms_bps.join(" / ")} bp，上限 {openingPolicies.lighter.hard_cap_bps} bp</small> : <small>开仓策略由原配置继承；owner 未上报具体阶梯。</small>}</section>;
}
const reasons: Record<string, string> = {
  parameters_require_stopped_flat: "仅在已停止、权威双腿空仓且没有待处理订单时可保存。",
  owner_not_stopped: "请先停止并等待权威空仓后保存。",
  position_not_flat: "当前有持仓，参数不能保存或启用。",
  position_not_authoritative: "权威仓位证据不可用，参数不能保存或启用。",
  pending_orders: "当前有待处理订单，参数不能保存或启用。",
  parameters_require_stopped: "请先停止并等待权威空仓后保存或启用。",
  parameters_active_cycle: "当前周期尚未结束，参数不能保存或启用。",
  parameters_pending_orders: "当前有待处理订单，参数不能保存或启用。",
  parameters_position_refresh_required: "权威仓位证据需要刷新，参数不能保存或启用。",
  parameters_require_flat: "当前有持仓，参数不能保存或启用。",
};
export default function ParameterDialog({ parameters, draft, dirty, connected, busy, unknown, error, onChange, onSave, onLookup, onClose, onDiscard }: {
  parameters: OwnerParameters | undefined; draft: ParameterValues | null; dirty: boolean; connected: boolean;
  busy: boolean; unknown: boolean; error: string; onChange: (key: keyof ParameterValues, value: string) => void;
  onSave: () => void; onLookup: () => void; onClose: () => void; onDiscard: () => void;
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => { dialog.current?.showModal(); return () => dialog.current?.close(); }, []);
  const openingSupported = openingConcessionsSupported(parameters);
  const validation = parameterValidation(draft, parameters?.limits, openingSupported);
  return <dialog ref={dialog} className="v3-owner-dialog v3-parameter-dialog" aria-labelledby="v3-parameter-title" onCancel={event => { event.preventDefault(); onClose(); }}>
    <div className="v3-owner-head"><h2 id="v3-parameter-title">普通对冲参数</h2><button type="button" aria-label="收起参数面板" onClick={onClose} disabled={busy}>×</button></div>
    <p>保存后等待启动确认启用。风险锁、批次与授权限额继续有效。</p>
    <p>开仓偏离是相对参考价差的信号门槛。开仓让价留空保留原重试阶梯及上限；填写后，各次重试的让价和价格保护上限固定为该值，不保证成交或盈利。平仓毛盈利按入场单腿名义金额计算。</p>
    {parameters ? <><ParameterFacts value={parameters.current} openingPolicies={parameters.opening_policies?.current} title="当前生效" />{parameters.saved ? <ParameterFacts value={parameters.saved} openingPolicies={parameters.opening_policies?.saved} title="已保存待启用" /> : null}</> : <p className="v3-pending">{connected ? "参数接口不可用，需升级 owner 后配置和启动。" : "owner 未连接，等待用户手动启动程序后核对实际参数。"}</p>}
    <form onSubmit={event => { event.preventDefault(); onSave(); }}>
      <strong className={dirty ? "v3-pending" : "v3-muted"}>{dirty ? "草稿未保存 · 启动已禁用" : parameters?.saved ? "编辑已保存参数" : "编辑当前参数"}</strong>
      {parameters && !openingSupported ? <p className="v3-muted">开仓让价继承原策略 · 当前 owner 不支持调整。</p> : null}
      <div className="v3-parameter-fields">{parameterFields.map(({ key, label, unit, opening }) => <label key={key} htmlFor={`v3-parameter-${key}`}><span>{label} <small>{unit}</small></span><input id={`v3-parameter-${key}`} name={key} type="text" inputMode="decimal" placeholder={opening ? "继承原策略" : undefined} value={draft?.[key] ?? ""} disabled={!parameters || busy || unknown || Boolean(opening && !openingSupported)} onChange={event => onChange(key, event.target.value)} /><small>{opening && !openingSupported ? "仅继承 · 不可调整" : opening ? `0–${parameters?.limits[key] ?? "未知"} ${unit} · 留空继承` : `上限 ${parameters?.limits[key] ?? "未知"} ${unit}`}</small></label>)}</div>
      {!connected ? <p className="v3-pending" role="status">owner 连接不可用，不能保存或启动。</p> : parameters && !parameters.can_save ? <p className="v3-pending" role="status">{reasons[parameters.reason_code || ""] || "仅在已停止、权威双腿空仓且无待处理订单时可保存。"}</p> : null}
      {dirty && validation ? <p className="v3-pending" role="alert">{validation}</p> : null}
      {error ? <p className="v3-pending" role="alert">{error}</p> : null}
      {unknown ? <p className="v3-pending">保存结果 UNKNOWN。只按原幂等键核对，不自动重发保存。</p> : null}
      <div className="v3-owner-actions"><button type="button" onClick={onClose} disabled={busy}>{dirty ? "保留草稿并收起" : "收起"}</button>{dirty && !unknown ? <button type="button" onClick={onDiscard} disabled={busy}>放弃草稿</button> : null}{unknown ? <button type="button" onClick={onLookup} disabled={busy || !connected}>核对原保存</button> : <button type="submit" className="v3-owner-confirm" disabled={busy || !connected || !parameters?.can_save || !dirty || Boolean(validation)}>保存待启用</button>}</div>
    </form>
  </dialog>;
}
