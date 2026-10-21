// Runnable in a real browser after an isolated Vite build. Every row is a test fixture.
import { useState } from "react";
import { createRoot } from "react-dom/client";
import OverviewPage from "../src/v3/OverviewPage";
import { useHashRoute } from "../src/v3/router";
import type { CycleDetailData, ProfitCycle, ProfitOverview as ProfitData } from "../src/v3/types";
import "../src/styles.css";

const amounts = { realized_usd: "0.031170", funding_usd: "0", fees_usd: "0.035973", net_usd: "-0.004803", known_realized_usd: "0.031170", known_funding_usd: "0", known_fees_usd: "0.035973", pending_realized: 0, pending_funding: 0, pending_fees: 0 };
function cycle(source: "basis" | "funding"): ProfitCycle {
  return { ...amounts, source_id: source, cycle_id: "same-id", symbol: source === "basis" ? "TEST-A" : "TEST-B", strategy_mode: source, accounting: "final", order_count: 0, order_fees: [], fee_basis: "cycle_complete", unrealized_usd: null, active_realized_usd: null, executable_gross_usd: null, phase: "DONE", paired_round_trip: true, quantity_step: "0.1", first_fill_at: 100, flat_confirmed_at: 110, hold_seconds: 10 };
}
const fixtures: ProfitData = { available: true, totals: { ...amounts, cycle_count: 2, complete_cycles: 2, pending_cycles: 0, accounting: "final" }, symbols: [], daily: [], cycles: [cycle("basis"), cycle("funding")], holding: { cycle_count: 0, unrealized_usd: "0", cycles: [] } };
let emptyData = false;
const overviewPaths: string[] = [];
const requests: { source: string; resolve: (response: Response) => void; reject: (error: Error) => void }[] = [];
// Ignore aborts deliberately, so delayed stale successes exercise the hook's identity guard.
window.fetch = ((url: string | URL | Request) => new Promise<Response>((resolve, reject) => {
  const parsed = new URL(String(url), window.location.href);
  if (parsed.pathname.endsWith("/overview")) {
    overviewPaths.push(parsed.search);
    resolve(new Response(JSON.stringify({ meta: { schema_version: 3, generated_at: Date.now()/1000, snapshot_id: "test" }, data: { deployments: [], profit: emptyData ? { ...fixtures, cycles: [] } : fixtures } }), {status: 200}));
    return;
  }
  const source = parsed.searchParams.get("source") || "";
  requests.push({ source, resolve, reject });
})) as typeof fetch;
function respond(request: typeof requests[number], missing = false) {
  const source = request.source as "basis" | "funding";
  const profit = cycle(source);
  if (missing) { profit.fees_usd = null; profit.net_usd = null; profit.pending_fees = 4; }
  const order = (venue: string, closing: boolean) => ({ order_ref: closing ? "same-close" : "same-open", venue, side: (venue === "entropy") !== closing ? "sell" : "buy", reduce_only: closing ? 1 as const : 0 as const, filled_units: "4", requested_units: "4", avg_fill_px: "100", state: "TERMINAL" });
  const data: CycleDetailData = { cycle: { ...profit, entropy_units: "0", hedge_units: "0" }, profit, groups: [
    { group_id: "open", kind: "open", attempts: [order("entropy", false), order("hedge", false)] },
    { group_id: "close", kind: "close", attempts: [order("entropy", true), order("hedge", true)] },
    { group_id: "duplicate-partial", kind: "open", attempts: [{ ...order("entropy", false), filled_units: "2" }] },
  ], economics: {} };
  request.resolve(new Response(JSON.stringify({ meta: { schema_version: 3, generated_at: Date.now()/1000, snapshot_id: "test" }, data }), { status: 200 }));
}
function Harness() {
  const [route, update] = useHashRoute();
  const [refresh, setRefresh] = useState(0);
  return <div className="v3-readable v3-app" style={{padding: 24}}><OverviewPage route={route} update={update} paused={false} refresh={refresh} toolbar={<><button id="refresh" onClick={() => setRefresh(n => n+1)}>测试刷新</button><button id="empty" onClick={() => { emptyData = true; setRefresh(n => n+1); }}>测试空数据</button></>} /></div>;
}
const output = document.querySelector<HTMLPreElement>("#results")!;
const entries: string[] = [];
const tick = () => new Promise(resolve => setTimeout(resolve, 30));
function check(condition: unknown, message: string) { if (!condition) throw new Error(message); entries.push(`✓ ${message}`); output.textContent = entries.join("\n"); }
async function waitFor(test: () => unknown, message: string) { const start = Date.now(); while (!test()) { if (Date.now() - start > 7_000) throw new Error(message); await tick(); } }
function click(selector: string) { const element = document.querySelector<HTMLElement>(selector); if (!element) throw new Error(`Missing ${selector}`); element.click(); }
function panel() { return document.querySelector<HTMLElement>("#cycle-panel"); }
function table() { return document.querySelector<HTMLTableElement>(".v3-profit-cycles")!; }
window.history.replaceState(null, "", "#/overview?scope=24h");
createRoot(document.getElementById("test-root")!).render(<Harness />);
async function run() {
  await waitFor(() => table(), "Initial render");
  const original = table(), row = original.tBodies[0].rows[0];
  window.scrollTo(0, 120); const initialY = window.scrollY;
  click('tr[data-source="basis"] button'); await waitFor(() => requests.length === 1, "First request");
  check(table() === original && row === original.tBodies[0].rows[0], "选择详情保留主表和原行 DOM");
  check(window.scrollY === initialY && original.rows.length === 3, "不插入明细行、不移动页面滚动位置");
  const first = requests.shift()!;
  click('tr[data-source="funding"] button'); await waitFor(() => requests.length === 1, "Second request");
  respond(requests.shift()!, true); await waitFor(() => panel()?.textContent?.includes("TEST-B"), "Funding response");
  respond(first); await tick();
  check(panel()?.dataset.identity === "funding:same-id" && !panel()?.textContent?.includes("TEST-A"), "同ID不同source及旧请求晚到不会串轮");
  check(panel()?.textContent?.includes("4 笔手续费未齐") && panel()?.textContent?.includes("净结果待核算"), "缺手续费保持待核算，不能沿用未扣费净额");
  const accounting = panel()!.querySelector<HTMLElement>(".v3-panel-accounting")!;
  const resultRows = Array.from(accounting.querySelectorAll("dl > div"));
  check(resultRows.map(row => row.querySelector("dt")?.textContent).join("/") === "交易盈亏/资金费/手续费/净收益" && resultRows[3].querySelector("dd")?.textContent === "待核算", "底部四项保留顺序与未知净收益，缺费用不补零");
  const labelRects = resultRows.map(row => row.querySelector("dt")!.getBoundingClientRect());
  const amountRects = resultRows.map(row => row.querySelector("dd")!.getBoundingClientRect());
  check(accounting.scrollWidth <= accounting.clientWidth && labelRects.every((rect, i) => rect.height < 24 && amountRects[i].height < 24 && rect.right < amountRects[i].left && (!i || rect.top > labelRects[i - 1].bottom)) && Math.max(...amountRects.map(rect => rect.right)) - Math.min(...amountRects.map(rect => rect.right)) < 1, "旧全局footer样式不能挤压汇总：四行单行、金额右齐、无横向溢出");
  const count = requests.length;
  click('tr[data-source="funding"] button'); click('tr[data-source="funding"] button'); await tick();
  check(panel()?.dataset.identity === "funding:same-id" && requests.length === count, "重复查看保持同一面板，不重复请求或收起");
  click('#cycle-tab-fills'); await tick();
  check(panel()?.querySelector('[data-evidence-identity]')?.getAttribute('data-evidence-identity') === "funding:same-id", "成交明细留在总览并绑定source和cycle");
  check(panel()?.textContent?.includes("缺少关键盘口"), "无盘口时显示无法判断，不补零");
  click('.v3-panel-orders > summary'); await tick();
  check(panel()?.querySelectorAll('.v3-panel-order').length === 4, "逐笔成交按venue和order_ref去重，partial累计2+4只留4");
  check(!panel()?.querySelector('.v3-panel-orders')?.textContent?.includes('2 units') && panel()?.querySelector('.v3-panel-orders')?.textContent?.includes('双所同编号，费用未关联'), "跨venue同编号保留两腿，费用不错误重复关联");
  const originalPanel = panel(); const scroll = panel()!.querySelector<HTMLElement>('.v3-panel-scroll')!;
  scroll.scrollTop = 90; const scrollY = scroll.scrollTop;
  click('#refresh'); await waitFor(() => requests.length === 1, "Manual refresh request");
  check(panel() === originalPanel && table() === original && scroll.scrollTop === scrollY, "手动刷新请求中保留面板、主表和面板滚动位置");
  respond(requests.shift()!, true); await tick();
  check(panel() === originalPanel && original.querySelectorAll('tr.is-expanded').length === 1, "手动刷新完成后保持选中行");
  await waitFor(() => requests.length === 1, "Automatic poll"); respond(requests.shift()!, true); await tick();
  check(panel() === originalPanel && table() === original && window.location.hash.includes('detail=fills'), "自动刷新保留面板和标签，主表不重挂载");
  click('#refresh'); await waitFor(() => requests.length === 1, "Failed refresh"); requests.shift()!.reject(new Error("测试读取失败")); await tick();
  check(panel()?.textContent?.includes("详情刷新异常") && panel()?.textContent?.includes("TEST-B"), "刷新失败保留正确旧证据并标明异常");
  window.history.back(); await waitFor(() => panel()?.querySelector('[aria-selected="true"]')?.id === 'cycle-tab-legs', "Back restores tab");
  check(table() === original && panel()?.dataset.identity === "funding:same-id", "浏览器返回恢复标签且保留主表");
  window.history.back(); await waitFor(() => requests.length === 1, "Back restores previous source"); respond(requests.shift()!); await tick();
  check(panel()?.dataset.identity === 'basis:same-id' && panel()?.textContent?.includes('TEST-A'), "返回恢复之前交易来源，同ID不串账");
  click('[aria-label="关闭本轮详情"]'); await tick();
  check(!panel() && table() === original && !document.querySelector('.has-detail'), "关闭面板恢复宽主表，保留原表DOM");
  click('#empty'); await waitFor(() => original.tBodies[0].rows.length === 0, "Empty overview response");
  check(table() === original && original.tBodies[0].rows.length === 0 && document.body.textContent?.includes('所选时间内还没有完成的交易'), "空数据仍保留主表并显示真实空状态");
  check(overviewPaths.every(path => !new URLSearchParams(path).has("source")), "总览列表请求不受详情source选择影响");
  output.dataset.result = "passed"; output.textContent = `${entries.join('\n')}\n${entries.length} browser fixture checks passed.`;
}
run().catch(error => { output.dataset.result = 'failed'; output.textContent = `${entries.join('\n')}\nFAILED: ${error.message}`; });
