// Real DOM checks with in-memory GET snapshots. No owner, socket or trading API.
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import SystemPage from "../src/v3/SystemPage";
import OverviewPage from "../src/v3/OverviewPage";
import FundingPanel from "../src/v3/FundingPanel";
import { batchLimit } from "../src/v3/format";
import { readRoute } from "../src/v3/router";
import type { Deployment, FundingDashboard, SystemData } from "../src/v3/types";
import "../src/styles.css";
import "../src/v3/FourPage.css";

const now = Date.now() / 1000, loss = "3.47865123";
const checks: string[] = [], requests: { method: string; path: string }[] = [], snapshots: { name: string; text: string }[] = [];
const output = document.querySelector<HTMLPreElement>("#results")!, root = createRoot(document.getElementById("test-root")!);
let generation = 0, resume: (() => void) | null = null;
const qa = { checks, requests, snapshots, checkpoint: "", continue: () => { qa.checkpoint = ""; resume?.(); resume = null; } };
(window as unknown as { batchLimitsQA: typeof qa }).batchLimitsQA = qa;
const batch = (limits: "disabled" | "numeric" | "missing" | "null"): NonNullable<SystemData["batch"]> => ({ batch_id: "mock-batch", position_cycles: 94, loss_used_usd: loss, closed_for_entries: false,
  ...(limits === "disabled" ? { max_position_cycles: "disabled", loss_budget_usd: "disabled" } : limits === "numeric" ? { max_position_cycles: 100, loss_budget_usd: "3" } : limits === "null" ? { max_position_cycles: null, loss_budget_usd: null } : {}) });
const funding = (limits: Parameters<typeof batch>[0]): FundingDashboard => ({ strategy_mode: "funding_hedge", live_quality: "available", policy: { configured: true, max_notional_usd: "200", funding_settlements_required: 2, funding_round_loss_limit_usd: null }, batch: batch(limits) });
const deployment = (mode: "basis" | "funding", limits: Parameters<typeof batch>[0]): Deployment => ({ source_id: mode, mode, label: mode === "basis" ? "普通对冲" : "资金费对冲", symbols: [mode === "basis" ? "SNDK" : "NBIS"], snapshot_id: "mock", generated_at: now, source_health: {}, batches: [], batch: batch(limits), status: { source_ts: now - 1, current_state: "READY", reason: "no_send_entry_check" } });
let data: SystemData = {};
let deployments: Deployment[] = [];
window.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
  const url = new URL(String(input), location.origin), method = init?.method || "GET";
  requests.push({ method, path: url.pathname });
  if (url.origin !== location.origin || method !== "GET" || !["/api/v3/system", "/api/v3/health", "/api/v3/overview"].includes(url.pathname)) throw new Error("UNEXPECTED NETWORK BLOCKED");
  const body = url.pathname.endsWith("system") ? data : url.pathname.endsWith("overview") ? { deployments } : { api: { status: "available" } };
  return new Response(JSON.stringify({ meta: { schema_version: 3, snapshot_id: "mock", generated_at: now, sources: {}, consistency: "consistent", warnings: [] }, data: body }), { status: 200, headers: { "Content-Type": "application/json" } });
}) as typeof fetch;
function check(value: unknown, name: string) { if (!value) throw new Error(name); checks.push(name); output.textContent = checks.map(item => `✓ ${item}`).join("\n"); }
const tick = () => new Promise(resolve => setTimeout(resolve, 10));
async function waitFor(test: () => unknown) { const start = Date.now(); while (!test()) { if (Date.now() - start > 5000) throw new Error("DOM update timed out"); await tick(); } }
async function checkpoint(name: string) { if (new URLSearchParams(location.search).has("capture")) { qa.checkpoint = name; await new Promise<void>(resolve => { resume = resolve; }); } }
async function render(name: string, view: "system" | "overview" | "funding", mode: "basis" | "funding", limits: Parameters<typeof batch>[0]) {
  data = { batch: batch(limits), config: { exit_min_gross_profit_bps: "2.5" }, ...(mode === "funding" ? { funding: funding(limits) } : {}) };
  deployments = [deployment(mode, limits)];
  const route = { ...readRoute(), mode, source: mode, evidence: "sources" as const };
  generation++;
  root.render(<StrictMode><main key={generation} data-generation={generation} className="v3-app v3-four-page" style={{ display: "block", padding: 16 }}><h1>批次预算 · 隔离 mock</h1>{view === "system" ? <SystemPage route={route} paused={false} update={() => {}} deployments={deployments} /> : view === "overview" ? <OverviewPage route={route} paused={false} update={() => {}} /> : <FundingPanel data={funding(limits)} live={deployments[0].status} timezone="Asia/Taipei" />}</main></StrictMode>);
  await waitFor(() => document.querySelector<HTMLElement>("[data-generation]")?.dataset.generation === String(generation) && document.querySelector(".v3-kv-grid,.v3-deployment-facts"));
  if (view === "system") await waitFor(() => document.querySelector(".v3-system-page")?.textContent?.includes("94 /"));
  const text = document.querySelector("#test-root")!.textContent || "";
  snapshots.push({ name, text });
  check(text.includes("94") && text.includes(loss), `${name}: 实际94圈及累计亏损原精度保留`);
  check(!/disabled|None|NaN|Infinity|无限/.test(text), `${name}: 不暴露哨兵或伪无限值`);
  if (limits === "disabled") check((text.match(/已禁用/g) || []).length === 2 && !text.includes("100"), `${name}: 两项明确已禁用，不沿用100或数字预算`);
  if (limits === "numeric") check(text.includes("94 / 100") && text.includes("3 USD") && !text.includes("已禁用"), `${name}: 数字限额仍按实际授权展示`);
  if (limits === "missing" || limits === "null") check(!text.includes("已禁用") && text.includes("—"), `${name}: 缺失或未声明null不猜为禁用或零`);
  if (view === "system") check(text.includes("退出毛利润 · bp") && text.includes("2.5"), `${name}: 现有普通盈利标签及2.5参数口径不变`);
}
async function run() {
  check(batchLimit("disabled", " USD") === "已禁用", "显式禁用不会带USD后缀");
  check(batchLimit(null) === "—" && batchLimit(undefined) === "—", "null/缺失证据不等于已禁用");
  check(batchLimit(0) === "0" && batchLimit("0", " USD") === "0 USD", "真实零与禁用分别展示");
  for (const mode of ["basis", "funding"] as const) {
    await render(`${mode}系统两限额禁用`, "system", mode, "disabled"); await checkpoint(`desktop-${mode}-system`);
    for (const limits of ["numeric", "missing", "null"] as const) await render(`${mode}系统${limits}`, "system", mode, limits);
    await render(`${mode}总览两限额禁用`, "overview", mode, "disabled"); await checkpoint(`desktop-${mode}-overview`);
  }
  await render("资金费专页两限额禁用", "funding", "funding", "disabled"); await checkpoint("desktop-funding-panel");
  await render("手机普通系统两限额禁用", "system", "basis", "disabled"); await checkpoint("mobile-basis-system");
  check(requests.every(request => request.method === "GET") && requests.every(request => !request.path.includes("control")), "全部请求在内存mock完成，无POST或owner控制");
  output.dataset.result = "passed"; output.textContent += `\n${checks.length} batch-limit checks passed.`;
}
run().catch(error => { output.dataset.result = "failed"; output.textContent += `\nFAILED: ${error.message}`; });
