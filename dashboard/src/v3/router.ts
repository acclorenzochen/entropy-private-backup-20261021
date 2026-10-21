import { useEffect, useState } from "react";

export type Page = "overview" | "market" | "cycles" | "losses" | "economics" | "system";
const pages = new Set<Page>(["overview", "market", "cycles", "losses", "economics", "system"]);
export type Scope = "session" | "batch" | "today" | "7d" | "all" | "24h" | "custom";
const scopes = new Set<Scope>(["session", "batch", "today", "7d", "all", "24h", "custom"]);
export type Mode = "all" | "basis" | "funding";
export type Source = "basis" | "funding";
export interface RouteState {
  page: Page; scope: Scope; timezone: "Asia/Taipei" | "UTC";
  mode: Mode; symbol: string; source: Source | "";
  pair: string; session: string; batch: string; from: string; to: string;
  units: string; chart: "edge" | "basis" | "live"; cycle: string; group: string;
  detail: "legs" | "fills" | "exit";
  direction: string; status: string; flag: string; cursor: string;
  view: string; sort: string;
  evidence: "sources" | "research";
}

export function readRoute(): RouteState {
  const hash = window.location.hash.replace(/^#\/?/, "");
  const [rawPage, rawQuery = ""] = hash.split("?", 2);
  const query = new URLSearchParams(rawQuery);
  const page = rawPage === "losses" ? "cycles" : rawPage === "economics" ? "system" : pages.has(rawPage as Page) ? rawPage as Page : "overview";
  const rawScope = query.get("scope") || "7d";
  const scope = rawScope === "session" && !["basis", "funding"].includes(query.get("mode") || "") ? "7d" : scopes.has(rawScope as Scope) ? rawScope as Scope : "7d";
  const mode = query.get("mode");
  const source = query.get("source");
  return {
    page, scope, timezone: query.get("tz") === "UTC" ? "UTC" : "Asia/Taipei",
    mode: mode === "basis" || mode === "funding" ? mode : "all",
    symbol: query.get("symbol") || "", source: source === "basis" || source === "funding" ? source : "",
    pair: query.get("pair") || "", session: query.get("session") || "",
    batch: query.get("batch") || "", from: query.get("from") || "",
    to: query.get("to") || "", units: query.get("units") || "",
    chart: query.get("chart") === "basis" ? "basis" : query.get("chart") === "edge" ? "edge" : "live",
    cycle: query.get("cycle") || "", group: query.get("group") || "",
    detail: query.get("detail") === "fills" ? "fills" : query.get("detail") === "exit" ? "exit" : "legs",
    direction: query.get("direction") || "", status: query.get("status") || "",
    flag: query.get("flag") || "", cursor: scope === rawScope ? query.get("cursor") || "" : "",
    view: query.get("view") || (rawPage === "losses" ? "loss" : ""), sort: query.get("sort") || "latest",
    evidence: rawPage === "economics" || query.get("evidence") === "research" ? "research" : "sources",
  };
}

const keys: [keyof RouteState, string][] = [
  ["mode", "mode"], ["symbol", "symbol"], ["source", "source"],
  ["scope", "scope"], ["timezone", "tz"], ["pair", "pair"],
  ["session", "session"], ["batch", "batch"], ["from", "from"],
  ["to", "to"], ["units", "units"], ["chart", "chart"],
  ["cycle", "cycle"], ["group", "group"], ["direction", "direction"],
  ["detail", "detail"],
  ["status", "status"], ["flag", "flag"], ["cursor", "cursor"],
  ["view", "view"], ["sort", "sort"],
  ["evidence", "evidence"],
];
export function routeHash(route: RouteState): string {
  const q = new URLSearchParams();
  for (const [key, label] of keys) {
    const value = route[key];
    if (value && !(key === "mode" && value === "all") && !(key === "scope" && value === "7d") && !(key === "timezone" && value === "Asia/Taipei") && !(key === "chart" && value === "live") && !(key === "detail" && value === "legs") && !(key === "evidence" && value === "sources")) q.set(label, value);
  }
  const query = q.toString();
  return `#/${route.page}${query ? `?${query}` : ""}`;
}

export function useHashRoute(): [RouteState, (patch: Partial<RouteState>) => void] {
  const [route, setRoute] = useState(readRoute);
  useEffect(() => {
    const changed = () => setRoute(readRoute());
    window.addEventListener("hashchange", changed);
    window.addEventListener("popstate", changed);
    return () => { window.removeEventListener("hashchange", changed); window.removeEventListener("popstate", changed); };
  }, []);
  const update = (patch: Partial<RouteState>) => {
    const next = { ...readRoute(), ...patch };
    const hash = routeHash(next);
    if (window.location.hash !== hash) window.history.pushState(null, "", hash);
    setRoute(readRoute());
  };
  return [route, update];
}

function wallTimeToUnix(value: string, timezone: RouteState["timezone"]): number | null {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(value)) return null;
  const date = new Date(`${value}:00Z`);
  if (Number.isNaN(date.getTime())) return null;
  return Math.floor(date.getTime() / 1000) - (timezone === "Asia/Taipei" ? 8 * 3600 : 0);
}

export function commonQuery(route: RouteState, nowSeconds = Math.floor(Date.now() / 1000)): URLSearchParams {
  const q = new URLSearchParams();
  q.set("mode", route.mode);
  if (route.symbol) q.set("symbol", route.symbol);
  if (route.source) q.set("source", route.source);
  if (route.view) q.set("view", route.view);
  q.set("scope", route.scope === "24h" ? "custom" : route.scope);
  q.set("timezone", route.timezone);
  if (route.pair) q.set("pair", route.pair);
  if (route.scope === "session" && route.session) q.set("session", route.session);
  if (route.scope === "batch" && route.batch) q.set("batch_id", route.batch);
  if (route.scope === "24h") {
    q.set("from_ts", String(nowSeconds - 24 * 3600));
    q.set("to_ts", String(nowSeconds));
  } else if (route.scope === "custom") {
    const from = wallTimeToUnix(route.from, route.timezone);
    const to = wallTimeToUnix(route.to, route.timezone);
    if (from !== null) q.set("from_ts", String(from));
    if (to !== null) q.set("to_ts", String(to));
  }
  return q;
}

export function queryKey(route: RouteState): string {
  return [route.mode, route.symbol, route.source, route.view, route.scope, route.timezone, route.pair, route.session, route.batch, route.from, route.to].join("|");
}
