import type { CycleSummary, Decimal, GateStatus, Quality, V3Meta } from "./types";

export function exact(value: string | number | boolean | null | undefined, suffix = ""): string {
  return value === null || value === undefined || value === "" ? "—" : `${String(value)}${suffix}`;
}
export function batchLimit(value: string | number | null | undefined, suffix = ""): string {
  return value === "disabled" ? "已禁用" : exact(value, suffix);
}
export function configValue(key: string, value: string | number | boolean | null | undefined, present?: boolean): string {
  if (key === "single_cycle_review_loss_usd" && value == null) return present === true && value === null ? "已禁用" : "未知";
  return exact(value);
}
export function money(value: Decimal | undefined): string {
  if (value === null || value === undefined || value === "") return "—";
  return `${value} USD`;
}
export function basis(value: Decimal | undefined): string { return exact(value, " bp"); }
export function time(ts: number | null | undefined, timezone: "Asia/Taipei" | "UTC" = "Asia/Taipei"): string {
  if (ts === null || ts === undefined || !Number.isFinite(ts) || ts <= 0) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: timezone, month: "2-digit", day: "2-digit", hour: "2-digit",
    minute: "2-digit", second: "2-digit", hour12: false,
  }).format(ts * 1000);
}
export function duration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) return "—";
  if (seconds < 60) return `${Math.round(seconds)} 秒`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} 分 ${Math.round(seconds % 60)} 秒`;
  return `${Math.floor(seconds / 3600)} 小时 ${Math.floor(seconds % 3600 / 60)} 分`;
}
export function cycleTiming(cycle?: Pick<CycleSummary, "first_fill_at" | "first_fill_confirmed_at" | "hold_seconds" | "estimated_hold_seconds" | "duration_state" | "confirmation_duration_state" | "has_confirmed_fill"> | null, now?: number) {
  const unfilled = cycle?.has_confirmed_fill === false;
  const estimated = cycle?.first_fill_at == null && cycle?.first_fill_confirmed_at != null;
  const firstAt = unfilled ? null : cycle?.first_fill_at ?? cycle?.first_fill_confirmed_at;
  const state = estimated ? cycle?.confirmation_duration_state : cycle?.duration_state;
  const seconds = unfilled ? null : state === "active" && firstAt != null && now != null && now >= firstAt
    ? now - firstAt : estimated ? cycle?.estimated_hold_seconds : cycle?.hold_seconds;
  return { firstAt, seconds,
    firstLabel: unfilled ? "未成交尝试" : firstAt == null ? "成交时间待确认" : estimated ? "本地成交确认 · 缺交易所时间" : "交易所成交时间",
    durationLabel: unfilled ? "未成交，不计持仓" : seconds == null ? "时间证据待确认" : estimated
      ? `估算 · 本地确认${state === "active" ? "起计 · 计时中" : " → 权威归零"}`
      : state === "active" ? "计时中" : "首笔成交 → 权威归零",
  };
}
export function qualityLabel(quality: Quality | null | undefined): string {
  switch (quality) {
    case "good": case "valid": case "available": case "consistent": return "有效";
    case "partial": return "部分可用";
    case "stale": return "已过期";
    case "invalid": return "无效";
    case "unavailable": return "不可用";
    case "not_requested": return "尚未读取";
    default: return quality ? quality : "未知";
  }
}
export function gateLabel(status: GateStatus | boolean | null | undefined): string {
  if (status === true || status === "pass") return "通过";
  if (status === false || status === "fail") return "未通过";
  if (status === "not_evaluated") return "未评估";
  if (status === "not_applicable") return "不适用";
  return "未知";
}
export function metaLine(meta: V3Meta, timezone: "Asia/Taipei" | "UTC"): string {
  const window = meta.window;
  return `${window?.scope === "all" ? "全部历史 · 不限时间" : window ? `${time(window.from_ts, timezone)} → ${time(window.to_ts, timezone)}` : "范围未知"} · 快照 ${exact(meta.snapshot_id)} · ${qualityLabel(meta.consistency)}`;
}
export function finiteChart(value: Decimal | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) && Math.abs(n) < 1e12 ? n : null;
}
export function field(object: unknown, key: string): unknown {
  return object && typeof object === "object" && !Array.isArray(object) ? (object as Record<string, unknown>)[key] : undefined;
}
export function textField(object: unknown, key: string): string | null {
  const value = field(object, key);
  return typeof value === "string" || typeof value === "number" || typeof value === "boolean" ? String(value) : null;
}
export function timestampField(object: unknown, key: string, timezone: "Asia/Taipei" | "UTC"): string {
  const value = field(object, key);
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? time(value, timezone) : "未知";
}
