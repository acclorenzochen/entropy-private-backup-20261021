// Real DOM acceptance with in-memory snapshots only. No API, owner or orders.
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RuntimeSummary } from "../src/v3/OverviewPage";
import type { Deployment } from "../src/v3/types";
import "../src/styles.css";
import "../src/v3/FourPage.css";

const now = Date.now() / 1000;
const initial = (): Deployment => ({ source_id: "basis", mode: "basis", label: "普通对冲", symbols: ["SNDK"], generated_at: now, snapshot_id: "runtime-summary-mock", source_health: {}, batches: [], status: { current_state: "READY", reason: "no_send_entry_check", source_ts: now - 1 } });
const requests: string[] = [], checks: string[] = [], snapshots: { name: string; label: string; detail: string; text: string }[] = [];
window.fetch = (async (url: string | URL | Request) => { requests.push(String(url)); throw new Error("UNEXPECTED NETWORK BLOCKED: runtime summary must not fetch"); }) as typeof fetch;
const root = createRoot(document.getElementById("test-root")!), output = document.querySelector<HTMLPreElement>("#results")!;
let generation = 0, resume: (() => void) | null = null;
const qa = { checks, requests, snapshots, checkpoint: "", continue: () => { qa.checkpoint = ""; resume?.(); resume = null; } };
(window as unknown as { runtimeSummaryQA: typeof qa }).runtimeSummaryQA = qa;
const tick = () => new Promise(resolve => setTimeout(resolve, 10));
async function waitFor(test: () => unknown, name: string) { const started = Date.now(); while (!test()) { if (Date.now() - started > 3000) throw new Error(name); await tick(); } }
function check(value: unknown, name: string) { if (!value) throw new Error(name); checks.push(name); output.textContent = checks.map(item => `✓ ${item}`).join("\n"); }
async function render(name: string, status: Partial<Deployment["status"]> = {}, override: Partial<Deployment> = {}, props: { stale?: boolean; paused?: boolean; timezone?: "Asia/Taipei" | "UTC" } = {}) {
  const deployment = { ...initial(), ...override, status: { ...initial().status, ...status } };
  generation++;
  root.render(<StrictMode><main data-generation={generation} className="v3-app v3-four-page" style={{ display: "block", minHeight: 260, padding: 24 }}><h1>顶部当前诊断 · 完全离线 mock</h1><div className="v3-runtime-bar"><div className="v3-runtime-summary"><RuntimeSummary deployment={deployment} stale={props.stale || false} paused={props.paused || false} timezone={props.timezone || "Asia/Taipei"} /></div></div></main></StrictMode>);
  await waitFor(() => document.querySelector<HTMLElement>("[data-generation]")?.dataset.generation === String(generation), name);
  const summary = document.querySelector<HTMLElement>(".v3-runtime-summary > div")!;
  const snapshot = { name, label: summary.querySelector("span")?.textContent || "", detail: summary.querySelector("small")?.textContent || "", text: summary.textContent || "" };
  snapshots.push(snapshot);
  check(summary.children.length === 3 && summary.querySelectorAll("strong,span,small").length === 3 && !summary.querySelector("section,details,.v3-card,button,input"), `${name}: 保持strong/span/small单行摘要结构，不新增卡片或动作`);
  return snapshot;
}
async function checkpoint(name: string) { if (new URLSearchParams(location.search).has("capture")) { qa.checkpoint = name; await new Promise<void>(resolve => { resume = resolve; }); } }
async function run() {
  let result = await render("READY / 当前开仓检查未通过");
  check(result.detail === "当前未通过开仓检查，具体原因未上报" && !result.text.includes("价差不足") && !result.text.includes("门槛不足"), "no_send_entry_check只报告当前检查未通过，不擅自推断价差/门槛不足");
  await checkpoint("desktop-ready");
  for (const [reason, venue] of [["entropy book is not fresh", "Entropy"], ["hedge book is not fresh", "Lighter"]]) {
    result = await render(`${venue} 行情过期`, { reason });
    check(result.detail.includes(venue) && /行情.*过期/.test(result.detail) && !result.detail.includes("价差不足"), `${venue} 当前过期行情明确指向对应腿，不误报价差不足`);
  }
  result = await render("收集参考行情", { current_state: "COLLECTING_REFERENCE", reason: "live_reference_pending" });
  check(result.label.includes("收集行情"), "COLLECTING_REFERENCE显示当前收集参考行情状态");
  for (const [state, label] of [["HOLDING", "正在持仓"], ["EXITING", "正在平仓"], ["STOPPING", "暂停"], ["STOPPED", "暂停"]]) {
    result = await render(state, { current_state: state, reason: "" });
    check(result.label.includes(label) && !result.label.includes("程序在线"), `${state}随当前状态更新且不沿用READY在线标签`);
  }
  for (const [reason, detail] of [["operator_stopping", "已请求暂停新开仓"], ["operator_stopped", "等待手动启动"]]) {
    result = await render(`当前owner意图 ${reason}`, { reason });
    check(result.detail.includes(detail), `${reason}明确当前人工意图，不推断开仓门槛`);
  }
  result = await render("持仓退出收益尚未达到门槛", { current_state: "HOLDING", reason: "hold" });
  check(result.detail.includes("尚未达到正常退出收益门槛") && !result.detail.includes("开仓") && !result.detail.includes("价差不足"), "hold是已确认退出收益门槛证据，不能解释为开仓价差不足");
  result = await render("正常退出收益门槛已满足", { current_state: "EXITING", reason: "gross_price_profit_exit" });
  check(result.detail.includes("已达到正常退出收益门槛") && result.label.includes("正在平仓"), "gross_price_profit_exit明确退出门槛已满足及正在平仓");
  result = await render("风控批次暂停", { current_state: "BATCH_CLOSED", closed_for_entries: true, close_reason: "ACCOUNT_DRAWDOWN" });
  check(result.label.includes("暂停") && result.detail.includes("账户回撤保护"), "批次风控暂停显示真实关闭原因，不当作行情条件不足");
  result = await render("人工暂停", { current_state: "READY", closed_for_entries: true, close_reason: "MANUAL_STOP" });
  check(result.label.includes("暂停") && result.detail.includes("人工暂停"), "人工暂停原因优先于READY普通开仓检查");
  result = await render("请求限流", { reason: "OPEN_SIGNAL_DROPPED_RATE_LIMIT" });
  check(/频率|限流/.test(result.detail) && !result.detail.includes("价差不足"), "下单请求限流单独解释，不推断门槛原因");
  result = await render("权威空仓lease不可用", { reason: "fresh_authoritative_flat_lease_unavailable" });
  check(result.detail.includes("空仓") && /未确认|尚未确认/.test(result.detail), "authority lease缺失明确空仓尚未确认");
  for (const reason of ["unrecognized_future_reason", ""]) {
    result = await render(`未知或空原因 ${reason || "empty"}`, { reason });
    check(result.detail.includes("具体原因未上报") && !result.detail.includes("价差不足"), "未知/缺失当前reason明确未上报，不能从READY推断价差不足");
  }
  result = await render("读取失败保留旧状态", {}, {}, { stale: true });
  check(result.label === "数据读取异常 · 当前状态待确认" && result.detail.includes("最后状态") && !result.detail.includes("当前未通过"), "stale读取失败明确当前状态待确认，只显示末次状态时间");
  await checkpoint("desktop-stale");
  for (const [name, source_ts] of [["状态source_ts过期", now - 11], ["source_ts缺失", null], ["source_ts未提供", undefined], ["source_ts未来", now + 30], ["source_ts非有限", Number.NaN], ["source_ts零", 0]] as const) {
    result = await render(name, { source_ts });
    check(result.label.includes("过期") && result.label.includes("未知") && !result.label.includes("程序在线") && !result.detail.includes("当前未通过"), `${name}不能证明当前在线或给出当前拒单结论`);
    if (name === "source_ts未来") check(result.detail.includes("状态时间晚于快照"), "future source_ts明确源时间晚于快照，不冒充在线状态");
  }
  result = await render("页面暂停刷新", {}, {}, { paused: true });
  check(result.label.includes("暂停更新") && result.label.includes("上次状态") && result.detail.includes("暂停更新 · 上次状态") && !result.detail.includes("当前未通过"), "paused明确上次状态时间，不宣称当前开仓拒绝原因");
  for (const state of ["complete", "accounting_pending"] as const) {
    result = await render(`历史manual exit ${state}`, {}, { manual_exit: { state, supersedes_live: true, position_observed_ts: now - 86400, source_ts: now - 86400, remaining: { entropy_units: 0, hedge_units: 0, authoritative: true } } });
    check(result.label.includes("退出时已平仓") && result.detail.includes("实时仓位待查询") && !result.label.includes("当前已平仓"), `${state}仅描述历史退出证据，实时仓位仍待查询`);
  }
  result = await render("未确认manual exit", {}, { manual_exit: { state: "partial", supersedes_live: true, position_observed_ts: now - 60, remaining: { entropy_units: 12, hedge_units: -12, authoritative: true } } });
  check(!result.label.includes("已平仓"), "部分manual exit不能显示已平仓");
  result = await render("旧zero-fill周期不能污染当前READY", {}, { overview: { recent_cycles: [{ cycle_id: "historical-zero-fill", result_kind: "zero_fill", phase: "FLAT", open_zero_fill_count: 7, completed_at: now - 86400 }], diagnostic: { reason: "old_zero_fill", entry: { summary_reason: "old_threshold_not_met", evaluated_at: now - 86400 } } } });
  check(result.detail === "当前未通过开仓检查，具体原因未上报" && !result.text.includes("零成交") && !result.text.includes("old_") && !result.text.includes("价差不足"), "旧recent_cycles/diagnostic零成交与旧门槛拒绝均不能污染当前READY原因");
  result = await render("本次IOC零成交尝试结束", { current_state: "FLAT", reason: "cycle_complete", result_kind: "zero_fill" });
  check(result.detail === "开仓 IOC 零成交，本次尝试已结束" && result.label === "本次未成交尝试已结束", "当前FLAT/cycle_complete/zero_fill明确本次IOC零成交结束，不称持仓已平仓");
  await checkpoint("desktop-current-zero-fill");
  for (const reason of ["entry_band_not_met", "entry_threshold_not_met"]) {
    result = await render(`未上报的开仓门槛细分reason ${reason}`, { reason });
    check(result.detail.includes("具体原因未上报") && !/门槛不足|价差不足/.test(result.detail), `${reason}尚无真实上报映射，仍不可推断门槛不足`);
  }
  result = await render("同一DOM当前原因从检查未通过切换到行情过期", { reason: "hedge book is not fresh" });
  check(result.detail.includes("Lighter") && !result.detail.includes("当前未通过开仓检查"), "连续更新当前reason后，摘要跟随新原因而非保留旧检查信息");
  result = await render("手机READY", {}, {}, { timezone: "UTC" });
  await checkpoint("mobile-ready");
  check(requests.length === 0 && !document.querySelector("button,input,dialog"), "全部检查仅内存props和真实DOM，没有API/owner请求或交易控制");
  output.dataset.result = "passed"; output.textContent = `${checks.map(item => `✓ ${item}`).join("\n")}\n${checks.length} runtime-summary fixture checks passed.`;
}
run().catch(error => { output.dataset.result = "failed"; output.textContent = `${checks.map(item => `✓ ${item}`).join("\n")}\nFAILED: ${error.message}`; });
