// Browser fixtures only: no real API, owner, or exchange requests.
import { createRoot } from "react-dom/client";
import { flushSync } from "react-dom";
import ProfitOverview from "../src/v3/ProfitOverview";
import TraderCycleTable from "../src/v3/TraderCycleTable";
import CyclePanel from "../src/v3/CyclePanel";
import { readRoute } from "../src/v3/router";
import type { CycleSummary, ProfitCycle, ProfitOverview as ProfitData } from "../src/v3/types";
import "../src/styles.css";

const amounts = { realized_usd: "1", funding_usd: "0", fees_usd: "0.1", net_usd: "0.9", known_realized_usd: "1", known_funding_usd: "0", known_fees_usd: "0.1", pending_realized: 0, pending_funding: 0, pending_fees: 0 };
const common: ProfitCycle = { ...amounts, cycle_id: "base", source_id: "basis", symbol: "TEST", strategy_mode: "basis", phase: "DONE", accounting: "final", order_count: 4, order_fees: [], fee_basis: "cycle_complete", unrealized_usd: null, active_realized_usd: null, executable_gross_usd: null, paired_round_trip: true, has_confirmed_fill: true, flat_confirmed_at: 1791780901, duration_state: "unknown" };
const fixtures: ProfitCycle[] = [
  { ...common, cycle_id: "exchange", first_fill_at: 1791780895, hold_seconds: 6, duration_state: "closed", first_fill_confirmed_at: 1791780896, estimated_hold_seconds: 5, confirmation_duration_state: "closed" },
  { ...common, cycle_id: "local", first_fill_at: null, hold_seconds: null, first_fill_confirmed_at: 1791780896, estimated_hold_seconds: 5, confirmation_duration_state: "closed" },
  { ...common, cycle_id: "legacy", first_fill_at: 1791780895, hold_seconds: 6, duration_state: "closed", has_confirmed_fill: undefined },
  { ...common, cycle_id: "missing", first_fill_at: null, hold_seconds: null },
  { ...common, cycle_id: "unfilled", has_confirmed_fill: false, paired_round_trip: false, order_count: 0, first_fill_at: null, hold_seconds: null },
];
let detail = fixtures[1];
const requests: string[] = [];
window.fetch = (async input => {
  requests.push(String(input));
  return new Response(JSON.stringify({ meta: { schema_version: 3, generated_at: 1791781000, snapshot_id: "mock" }, data: { cycle: detail, profit: detail, groups: [], economics: {} } }), { status: 200 });
}) as typeof fetch;
const root = createRoot(document.getElementById("test-root")!);
const output = document.getElementById("results")!;
const checks: string[] = [];
function check(value: unknown, message: string) { if (!value) throw new Error(message); checks.push(message); output.textContent = checks.join("\n"); }
function renderOverview(cycle: ProfitCycle) {
  const data: ProfitData = { available: true, cycles: [cycle], totals: { ...amounts, cycle_count: 1, complete_cycles: 1, pending_cycles: 0, accounting: "final" }, symbols: [], daily: [], holding: { cycle_count: 0, unrealized_usd: "0", cycles: [] } };
  flushSync(() => root.render(<div className="v3-readable"><ProfitOverview data={data} route={readRoute()} paused update={() => {}} /></div>));
  return document.querySelector<HTMLTableRowElement>(".v3-profit-cycles tbody tr")!;
}
async function run() {
  for (const fixture of fixtures) {
    const row = renderOverview(fixture);
    const start = row.cells[1].textContent || "", hold = row.cells[2].textContent || "";
    if (["exchange", "legacy"].includes(fixture.cycle_id)) {
      check(start.includes("12:54:55") && start.includes("交易所成交时间"), `${fixture.cycle_id}: 北京交易所首笔时间`);
      check(hold.includes("6 秒") && !hold.includes("估算"), `${fixture.cycle_id}: 真实持仓时长`);
    } else if (fixture.cycle_id === "local") {
      check(start.includes("12:54:56") && start.includes("本地成交确认") && start.includes("缺交易所时间"), "local: 确认时间与交易所缺失明确区分");
      check(hold.includes("5 秒") && hold.includes("估算") && hold.includes("权威归零"), "local: 持仓估算有来源标签");
    } else if (fixture.cycle_id === "missing") {
      check(start.includes("—") && start.includes("待确认"), "missing: 不用当前或创建时间补成交");
      check(hold.includes("—") && hold.includes("时间证据待确认"), "missing: 无证据时长保持缺失");
    } else {
      check(start.includes("未成交尝试"), "unfilled: 无成交不叫持仓圈");
      check(hold.includes("不计持仓") && !hold.includes("0 秒"), "unfilled: 不伪造零时长");
    }
    flushSync(() => root.render(<TraderCycleTable items={[fixture as CycleSummary]} route={readRoute()} update={() => {}} />));
    check(document.body.textContent?.includes(fixture.cycle_id === "local" ? "估算" : fixture.cycle_id === "unfilled" ? "不计持仓" : fixture.cycle_id === "missing" ? "时间证据待确认" : "交易所成交时间"), `${fixture.cycle_id}: 交易记录与总览来源一致`);
  }
  for (const fixture of [fixtures[1], fixtures[0], fixtures[4]]) {
    detail = fixture;
    flushSync(() => root.render(null));
    flushSync(() => root.render(<CyclePanel route={{ ...readRoute(), source: "basis", cycle: fixture.cycle_id }} paused={false} refresh={0} update={() => {}} />));
    const started = Date.now();
    while (!document.querySelector('.v3-panel-accounting code')?.textContent?.includes(fixture.cycle_id)) {
      if (Date.now() - started > 5000) throw new Error(`detail timeout: ${fixture.cycle_id}`);
      await new Promise(resolve => setTimeout(resolve, 30));
    }
    const panel = document.querySelector('#cycle-panel')!;
    check(panel.textContent?.includes(fixture.cycle_id === "local" ? "本地成交确认（北京）" : fixture.cycle_id === "unfilled" ? "未成交尝试" : "首笔成交（北京）"), `${fixture.cycle_id}: 详情首笔来源标签一致`);
    check(panel.querySelector('.v3-panel-status')?.textContent?.includes(fixture.cycle_id === "local" ? "估算" : fixture.cycle_id === "unfilled" ? "不计持仓" : "6 秒"), `${fixture.cycle_id}: 详情持仓来源一致`);
  }
  check(requests.every(url => url.includes('/api/v3/cycles/')), "mock: 所有请求由本地fixture拦截");
  Object.assign(window, { cycleTimeQA: { checks, requests } });
  output.dataset.result = "passed";
}
run().catch(error => { output.dataset.result = "failed"; output.textContent = checks.join("\n") + "\nFAILED: " + error.message; });
