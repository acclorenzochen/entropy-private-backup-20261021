// Browser fixture: all API calls are mocked; no real API or trading control is used.
import { useState } from "react";
import { createRoot } from "react-dom/client";
import CyclesPage from "../src/v3/CyclesPage";
import OverviewPage from "../src/v3/OverviewPage";
import { useHashRoute } from "../src/v3/router";
import type { CycleDetailData, CycleSummary, ProfitCycle, ProfitOverview } from "../src/v3/types";
import "../src/styles.css";

const amounts = { realized_usd: "0.03", funding_usd: "0.01", fees_usd: "0.045", net_usd: "-0.005", known_realized_usd: "0.03", known_funding_usd: "0.01", known_fees_usd: "0.045", pending_realized: 0, pending_funding: 0, pending_fees: 0 };
function profit(source: "basis" | "funding"): ProfitCycle {
  return { ...amounts, source_id: source, cycle_id: "shared-id", symbol: source === "basis" ? "TEST-A" : "TEST-B", strategy_mode: source, accounting: "final", order_count: 4, order_fees: [], fee_basis: "cycle_complete", unrealized_usd: null, active_realized_usd: null, executable_gross_usd: null, quantity_step: "0.1", first_fill_at: 100, flat_confirmed_at: 120, hold_seconds: 20, duration_state: "closed", phase: "DONE", paired_round_trip: true };
}
function summary(source: "basis" | "funding"): CycleSummary {
  return { ...profit(source), entropy_units: "0", hedge_units: "0", actual_units: "4", planned_units: "4", has_confirmed_fill: true, paired_opened: true, accounting_state: "final", finalized_net_usd: "-0.005", side: "long_entropy", funding_state: source === "funding" ? { settlement_counts: { entropy: 1, hedge: 0 }, actual_funding_usd: "0.01", completed: false, trade_accounting_complete: true, closed_poll_complete: false, settlements: [{ venue: "entropy", event_id: "settlement-test", ts: 115, amount_usd: "0.01", source: "fixture" }] } : null };
}
const overview: ProfitOverview = { available: true, totals: { ...amounts, accounting: "final", cycle_count: 2, complete_cycles: 2, pending_cycles: 0 }, symbols: [], daily: [], cycles: [profit("basis"), profit("funding")], holding: { cycle_count: 0, unrealized_usd: "0", cycles: [] } };
const listPaths: string[] = [], detailRequests: { source: "basis" | "funding"; resolve: (response: Response) => void; reject: (error: Error) => void }[] = [];
const exports: string[] = [], copied: string[] = [], methods: string[] = [];
const envelope = (data: unknown) => new Response(JSON.stringify({ meta: { schema_version: 3, generated_at: Date.now() / 1000, snapshot_id: "fixture" }, data }), { status: 200 });
window.fetch = ((url: string | URL | Request, init?: RequestInit) => {
  const parsed = new URL(String(url), window.location.href);
  methods.push(init?.method || "GET");
  if (parsed.pathname === "/api/v3/cycles") {
    listPaths.push(parsed.search);
    return Promise.resolve(envelope({ items: [summary("basis"), summary("funding")], total: 4, next_cursor: parsed.searchParams.has("cursor") ? null : "page-two" }));
  }
  if (parsed.pathname === "/api/v3/overview") return Promise.resolve(envelope({ deployments: [], profit: overview }));
  if (parsed.pathname === "/api/v3/diagnostic-export") { exports.push(parsed.search); return Promise.resolve(envelope({ source: parsed.searchParams.get("source"), cycle_id: parsed.searchParams.get("cycle_id"), source_ts: 120 })); }
  if (parsed.pathname === "/api/v3/cycles/shared-id") return new Promise<Response>((resolve, reject) => detailRequests.push({ source: parsed.searchParams.get("source") as "basis" | "funding", resolve, reject }));
  throw new Error(`Unexpected request, blocked by fixture: ${parsed.pathname}`);
}) as typeof fetch;
Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText: async (value: string) => { copied.push(value); } } });
function respond(request: typeof detailRequests[number]) {
  const row = profit(request.source);
  if (request.source === "funding") { row.fees_usd = null; row.net_usd = null; row.pending_fees = 2; }
  const order = (venue: string, closing: boolean) => ({ order_ref: `${venue}-${closing ? "close" : "open"}`, venue, side: (venue === "entropy") !== closing ? "buy" : "sell", reduce_only: closing, requested_units: "4", filled_units: "4", remaining_units: "0", avg_fill_px: "100", state: "TERMINAL", submitted_at: closing ? 117 : 100, terminal_at: closing ? 119 : 102 });
  const data: CycleDetailData = { cycle: summary(request.source), profit: row, economics: { exit_trigger_reason: "manual_stop", exit_trigger_ts: 116 }, diagnostic: { source: "fixture", reason: "test-only" }, groups: [
    { group_id: "open-test", kind: "open", matched_units: "4", attempts: [order("entropy", false), order("hedge", false)], attribution: { A: "1.2", B: "1.0", C: "0.9", D: "0.8" } },
    { group_id: "close-test", kind: "close", attempts: [order("entropy", true), order("hedge", true)] },
    { group_id: "partial-test", kind: "close", attempts: [{ ...order("entropy", true), order_ref: "partial-order", filled_units: "2", remaining_units: "2", had_unknown: true }] },
    { group_id: "unknown-test", kind: "repair", attempts: [{ order_ref: "unknown-order", venue: "hedge", state: "UNKNOWN", requested_units: "3", filled_units: null }] },
    { group_id: "zero-test", kind: "open", attempts: [{ ...order("entropy", false), order_ref: "zero-order", filled_units: "0", remaining_units: "4" }] },
  ] };
  request.resolve(envelope(data));
}
function Harness() {
  const [route, update] = useHashRoute();
  const [refresh, setRefresh] = useState(0);
  return <main className="v3-app v3-readable" style={{ display: "block", maxWidth: 1440, padding: 20, margin: "auto" }}><button id="refresh" onClick={() => setRefresh(n => n + 1)}>测试刷新</button><button id="overview" onClick={() => update({ page: "overview", cycle: "", source: "", group: "", detail: "legs" })}>测试总览</button>{route.page === "overview" ? <OverviewPage route={route} update={update} paused={false} refresh={refresh} /> : <CyclesPage route={route} update={update} paused={false} refresh={refresh} />}</main>;
}
const output = document.querySelector<HTMLPreElement>("#results")!;
const entries: string[] = [];
const tick = () => new Promise(resolve => setTimeout(resolve, 30));
function check(value: unknown, message: string) { if (!value) throw new Error(message); entries.push(`✓ ${message}`); output.textContent = entries.join("\n"); }
async function waitFor(test: () => unknown, message: string) { const start = Date.now(); while (!test()) { if (Date.now() - start > 7000) throw new Error(message); await tick(); } }
function click(selector: string) { const element = document.querySelector<HTMLElement>(selector); if (!element) throw new Error(`Missing ${selector}`); element.click(); }
const panel = () => document.querySelector<HTMLElement>("#cycle-panel");
const table = () => document.querySelector<HTMLTableElement>(".v3-trader-table");
window.history.replaceState(null, "", "#/cycles?scope=all&view=complete&sort=loss&direction=long&flag=unknown");
createRoot(document.getElementById("test-root")!).render(<Harness />);
async function run() {
  await waitFor(table, "Initial trade list");
  click('.v3-pagination button:last-child'); await waitFor(() => window.location.hash.includes("cursor=page-two") && listPaths.length === 2, "Second page");
  const original = table()!, firstRow = original.tBodies[0].rows[0];
  window.scrollTo(0, 80); const initialY = window.scrollY;
  const button = firstRow.querySelector<HTMLButtonElement>("button")!; button.focus({ preventScroll: true }); button.click();
  await waitFor(() => detailRequests.length === 1, "Basis detail request"); const delayed = detailRequests.shift()!;
  check(table() === original && original.tBodies[0].rows[0] === firstRow && window.scrollY === initialY, "打开详情保留表格、原行和页面滚动");
  click('tr[data-source="funding"] button'); await waitFor(() => detailRequests.length === 1, "Funding detail request"); respond(detailRequests.shift()!);
  await waitFor(() => panel()?.textContent?.includes("TEST-B"), "Funding fixture renders"); respond(delayed); await tick();
  check(panel()?.dataset.identity === "funding:shared-id" && !panel()?.textContent?.includes("TEST-A"), "同ID不同来源和迟到响应不会串交易");
  check(window.location.hash.includes("cursor=page-two") && window.location.hash.includes("sort=loss") && window.location.hash.includes("flag=unknown") && listPaths.length === 2, "详情不重取列表，保留页码、排序和执行筛选");
  check(panel()?.textContent?.includes("整轮持仓 20") && panel()?.textContent?.includes("净结果待核算"), "整轮持仓首笔成交至权威归零，缺费用仍待核算");
  click('.v3-panel-funding > summary'); await tick();
  check(panel()?.textContent?.includes("settlement-test") && panel()?.textContent?.includes("两周期未完成"), "资金费逐笔结算和周期状态保留");
  const panelIdentity = panel(); const requestsBeforeTabs = detailRequests.length;
  click('#cycle-tab-fills'); await tick();
  check(panel() === panelIdentity && detailRequests.length === requestsBeforeTabs && panel()?.querySelectorAll('[data-stage]').length === 5, "切成交过程不重挂或重取详情，保留五阶段解释");
  click('.v3-panel-orders > summary'); await tick();
  check(panel()?.textContent?.includes("部分成交") && panel()?.textContent?.includes("曾 UNKNOWN"), "部分成交和UNKNOWN历史在真实订单中显示");
  click('.v3-panel-technical > summary'); await tick(); click('.v3-group-list button'); await tick();
  check(panel()?.textContent?.includes("A 决策配对报价") && panel()?.textContent?.includes("D 两腿实际匹配成交"), "执行组A/B/C/D高级证据保留");
  const unknownButton = Array.from(panel()!.querySelectorAll<HTMLButtonElement>('.v3-group-list button')).find(item => item.textContent?.includes("unknown-test"))!;
  unknownButton.click(); await tick();
  check(panel()?.textContent?.includes("unknown-order") && panel()?.textContent?.includes("UNKNOWN") && panel()?.textContent?.includes("1组确认零成交"), "UNKNOWN订单与确认零成交执行组分别保留");
  click('.v3-panel-diagnostic-actions button'); await waitFor(() => copied.length === 1, "Diagnostic copy");
  check(exports.length === 1 && exports[0].includes("source=funding") && copied[0].includes("shared-id"), "只读诊断复制绑定来源和周期，保留来源时间");
  const scroll = panel()!.querySelector<HTMLElement>('.v3-panel-scroll')!; scroll.scrollTop = 120; const detailScroll = scroll.scrollTop;
  click('#refresh'); await waitFor(() => detailRequests.length === 1, "Refresh detail pending");
  check(panel() === panelIdentity && table() === original && scroll.scrollTop === detailScroll && panel()?.textContent?.includes("TEST-B"), "刷新等待中不闪屏，保留详情、列表与滚动");
  respond(detailRequests.shift()!); await tick();
  check(panel() === panelIdentity && scroll.scrollTop === detailScroll && window.location.hash.includes("detail=fills"), "刷新完成保留页签、展开状态和滚动");
  await waitFor(() => detailRequests.length === 1, "Automatic detail poll"); detailRequests.shift()!.reject(new Error("fixture-refresh-failure")); await tick();
  check(panel()?.textContent?.includes("详情刷新异常") && panel()?.textContent?.includes("TEST-B") && table() === original, "轮询失败保留正确快照并显示异常");
  click('#cycle-tab-exit'); await tick();
  check(panel()?.textContent?.includes("人工停止") && panel() === panelIdentity, "退出原因按本轮历史证据展示");
  document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true })); await tick();
  check(!panel() && table() === original && window.location.hash.includes("cursor=page-two"), "Escape收起共享详情，列表和分页保留");
  click('.v3-pagination button:first-child'); await waitFor(() => !window.location.hash.includes("cursor="), "Previous page");
  check(window.location.hash.includes("scope=all") && window.location.hash.includes("sort=loss"), "详情关闭后上一页仍有效，全局筛选保持");
  click('#overview'); await waitFor(() => document.querySelector('.v3-profit-cycles'), "Overview list");
  click('.v3-profit-cycles tr[data-source="basis"] button'); await waitFor(() => detailRequests.length === 1, "Overview detail request"); respond(detailRequests.shift()!);
  await waitFor(() => panel()?.textContent?.includes("TEST-A"), "Overview shared panel");
  check(panel()?.querySelector('.v3-panel-technical') && Array.from(panel()!.querySelectorAll('[role="tab"]')).map(item => item.textContent).join("/") === "双腿/成交过程/退出原因", "总览复用同一详情组件与三页签、高级证据");
  check(listPaths.every(path => !new URLSearchParams(path).has("source")) && methods.every(method => method === "GET"), "列表不被详情来源缩窄，全部mock请求只读GET");
  const listRect = document.querySelector('.v3-overview-main')!.getBoundingClientRect(), panelRect = panel()!.getBoundingClientRect();
  if (window.innerWidth > 1000) check(panelRect.width >= 420 && panelRect.width <= 480 && listRect.right <= panelRect.left, "宽屏440px详情占独立列，不盖主列表");
  output.dataset.result = "passed"; output.textContent = `${entries.join("\n")}\n${entries.length} unified detail fixture checks passed.`;
}
run().catch(error => { output.dataset.result = "failed"; output.textContent = `${entries.join("\n")}\nFAILED: ${error.message}`; });
