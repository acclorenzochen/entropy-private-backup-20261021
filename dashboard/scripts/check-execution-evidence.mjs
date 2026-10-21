import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { deriveExecutionEvidence } from "../src/v3/executionEvidence.ts";

let checked = 0;
function test(name, run) { run(); checked++; console.log(`ok ${checked} - ${name}`); }
function near(actual, expected) {
  assert.equal(typeof actual, "number");
  assert.ok(Number.isFinite(actual) && Math.abs(actual - expected) < 1e-9, `${actual} != ${expected}`);
}
function book(eBuy = 101, hSell = 99, eSell = 105, hBuy = 103) {
  return { quantity_step: "0.1", entropy: { asks: [[eBuy, 10]], bids: [[eSell, 10]] },
    hedge: { bids: [[hSell, 10]], asks: [[hBuy, 10]] } };
}
function attempt(venue, side, closing = false) {
  return {
    order_ref: `${closing ? "close" : "open"}-${venue}`, step_key: `${closing ? "close" : "open"}-${venue}`,
    venue, side, purpose: closing ? `close_pair_${venue}_2` : venue === "entropy" ? "open_first" : "open_hedge",
    reduce_only: closing ? 1 : 0, execution_path: closing ? "parallel" : "standard",
    parent_step_key: !closing && venue === "hedge" ? "open-entropy" : null,
    requested_units: 4, filled_units: 4, result_units: 4, quantity_step: "0.1", state: "TERMINAL",
    avg_fill_px: closing ? venue === "entropy" ? "106" : "104" : venue === "entropy" ? "103" : "98",
    submitted_at: closing ? venue === "entropy" ? 20 : 20.1 : venue === "entropy" ? 10 : 12,
    terminal_observed_ts: closing ? venue === "entropy" ? 22 : 21 : venue === "entropy" ? 11 : 13,
    submit_start_mono_ns: closing ? venue === "entropy" ? 20e9 : 20.1e9 : venue === "entropy" ? 10e9 : 12e9,
    terminal_observed_mono_ns: closing ? venue === "entropy" ? 22e9 : 21e9 : venue === "entropy" ? 11e9 : 13e9,
    clock_session_id: "clock", terminal_clock_session_id: "clock",
    decision_quote_json: JSON.stringify(book(100, 101)), submit_quote_json: JSON.stringify(book()),
    fill_mark_quote_json: JSON.stringify(book(101, 100)), fill_mark_time_source: "terminal_observed_local",
  };
}
function fixture() {
  return {
    cycle: { cycle_id: "cycle", source_id: "basis" },
    groups: [
      { group_id: "open", kind: "open", attempts: [attempt("entropy", "buy"), attempt("hedge", "sell")] },
      { group_id: "close", kind: "close", attempts: [attempt("entropy", "sell", true), attempt("hedge", "buy", true)] },
    ],
    economics: { exit_trigger_ts: 19, exit_trigger_mono_ns: 19e9, exit_trigger_clock_session_id: "clock",
      signal_executable_basis_bps: "3", exit_trigger_reversion_bps: "1.5", total_exit_execution_decay_bps: "0",
      entropy_fill_to_lighter_submit_ms: "-1900" },
  };
}
const stage = (result, id) => result.stages.find(row => row.id === id);

test("同量深度 VWAP 与现金方向正确", () => {
  const data = fixture();
  const e = data.groups[0].attempts[0];
  e.submit_quote_json = JSON.stringify({ ...book(), entropy: { asks: [[100, 2], [102, 8]], bids: [[105, 10]] } });
  const result = deriveExecutionEvidence(data);
  const row = stage(result, "entropy_open").legs[0];
  near(row.expectedVwap, 101); near(row.quantity, 0.4); near(row.priceImpactUsd, -0.8);
  near(stage(result, "hedge_open").cashImpactUsd, -0.4);
  assert.match(result.conclusion, /相对发现机会时的同量盘口/);
});
test("本腿滑点与两腿间盘口移动分开", () => {
  const result = deriveExecutionEvidence(fixture());
  near(stage(result, "hedge_open").cashImpactUsd, -0.4);
  near(stage(result, "hedge_open").marketMovementUsd, -0.4);
  near(stage(result, "entropy_open").marketMovementUsd, 0.4);
  assert.match(stage(result, "entropy_open").marketMovementLabel, /Lighter 缓存盘口/);
  near(stage(result, "opportunity").cashImpactUsd, -1.2);
  near(result.openingImpactUsd, -2.4);
});
test("并发平仓不套 E成交到H发单，分别从共同触发计算", () => {
  const result = deriveExecutionEvidence(fixture());
  const closing = stage(result, "close");
  assert.equal(closing.mode, "parallel");
  near(closing.legs[0].submitToConfirmMs, 2000); near(closing.legs[1].submitToConfirmMs, 900);
  near(closing.legs[0].triggerToConfirmMs, 3000); near(closing.legs[1].triggerToConfirmMs, 2000);
  assert.equal(closing.legs[0].timingIssue, null); assert.equal(closing.legs[1].timingIssue, null);
  assert.ok(!JSON.stringify(result).includes("-1900"));
});
test("历史 fallback 路径和父步骤关联证明顺序平仓", () => {
  const data = fixture(); const [e,h] = data.groups[1].attempts;
  e.execution_path = h.execution_path = "fallback"; h.parent_step_key = e.step_key;
  h.submitted_at = 23; h.submit_start_mono_ns = 23e9;
  h.terminal_observed_ts = 24; h.terminal_observed_mono_ns = 24e9;
  h.submit_quote_json = JSON.stringify(book(101, 99, 105, 101));
  const result = stage(deriveExecutionEvidence(data), "close");
  assert.equal(result.mode, "sequential"); near(result.marketMovementUsd, 0.8);
  assert.match(result.marketMovementLabel, /平仓首腿发单前/);
});
test("真实单调时钟倒退标异常，不能 abs", () => {
  const data = fixture(); data.groups[1].attempts[0].terminal_observed_mono_ns = 18e9;
  const row = stage(deriveExecutionEvidence(data), "close").legs[0];
  assert.equal(row.submitToConfirmMs, null); assert.equal(row.triggerToConfirmMs, null);
  assert.match(row.timingIssue, /时钟证据异常/);
});
test("真顺序路径中对冲早于首腿确认标异常，禁止阶段混加", () => {
  const data = fixture(); data.groups[0].attempts[1].submit_start_mono_ns = 10.5e9;
  const result = deriveExecutionEvidence(data);
  assert.match(stage(result, "hedge_open").legs[0].timingIssue, /顺序开仓时钟证据异常/);
  assert.equal(stage(result, "hedge_open").marketMovementUsd, null); assert.equal(result.openingImpactUsd, null);
});
test("墙钟倒退保留异常，同钟单调耗时仍可核实", () => {
  const data = fixture(); data.groups[1].attempts[0].terminal_observed_ts = 18;
  const row = stage(deriveExecutionEvidence(data), "close").legs[0];
  near(row.submitToConfirmMs, 2000); assert.match(row.timingIssue, /时钟证据异常/);
});
test("不跨时钟会话相减单调时钟", () => {
  const data = fixture(); data.groups[1].attempts[0].terminal_clock_session_id = "other-clock";
  data.groups[1].attempts[0].terminal_observed_mono_ns = 1;
  const row = stage(deriveExecutionEvidence(data), "close").legs[0];
  near(row.submitToConfirmMs, 2000); assert.equal(row.timingBasis, "本机墙钟");
});
test("关键快照缺失不补零，也不拿 limit_px 当可执行盘口", () => {
  const data = fixture(); const row = data.groups[0].attempts[0];
  delete row.submit_quote_json; row.limit_px = "101";
  const result = stage(deriveExecutionEvidence(data), "entropy_open");
  assert.equal(result.cashImpactUsd, null); assert.equal(result.legs[0].expectedVwap, null);
  assert.match(result.summary, /缺少当时盘口/);
});
test("缺数量步长不能从 bp 换算 U", () => {
  const data = fixture(); delete data.groups[0].attempts[0].quantity_step;
  const result = deriveExecutionEvidence(data);
  assert.equal(stage(result, "entropy_open").cashImpactUsd, null);
  assert.equal(stage(result, "holding").cashImpactUsd, null);
  assert.equal(stage(result, "opportunity").cashImpactUsd, null);
});
test("深度不足或非法盘口不能做价格基准", () => {
  const data = fixture(); data.groups[0].attempts[0].submit_quote_json = JSON.stringify({ entropy: { asks: [[100, 2]] } });
  assert.equal(stage(deriveExecutionEvidence(data), "entropy_open").cashImpactUsd, null);
  data.groups[0].attempts[0].submit_quote_json = "{not json}";
  assert.equal(stage(deriveExecutionEvidence(data), "entropy_open").cashImpactUsd, null);
});
test("partial fill 仅按实际同量比较，duplicate cumulative 不重复", () => {
  const data = fixture(); const e = data.groups[0].attempts[0];
  e.filled_units = 2; e.result_units = 2;
  data.groups.push({ group_id: "duplicate", kind: "open", attempts: [structuredClone(e)] });
  const result = stage(deriveExecutionEvidence(data), "entropy_open");
  assert.equal(result.legs.length, 1); assert.equal(result.legs[0].partial, true); near(result.cashImpactUsd, -0.4);
  assert.equal(deriveExecutionEvidence(data).openingImpactUsd, null);
});
test("递增 cumulative 保留最大快照而不把2+4累计为6", () => {
  const data = fixture(); const later = structuredClone(data.groups[0].attempts[0]);
  data.groups[0].attempts[0].filled_units = 2;
  data.groups.push({ group_id: "later", kind: "open", attempts: [later] });
  const result = stage(deriveExecutionEvidence(data), "entropy_open");
  assert.equal(result.legs.length, 1); assert.equal(result.legs[0].filledUnits, 4); near(result.cashImpactUsd, -0.8);
});
test("多次独立对冲不把不同基准合并成整轮金额", () => {
  const data = fixture(); const extra = structuredClone(data.groups[0].attempts[1]);
  extra.order_ref = "second-hedge"; extra.step_key = "second-hedge";
  extra.filled_units = extra.result_units = extra.requested_units = 2;
  data.groups[0].attempts[1].filled_units = data.groups[0].attempts[1].result_units = 2;
  data.groups[0].attempts[1].requested_units = 2;
  data.groups[0].attempts.push(extra);
  const result = deriveExecutionEvidence(data);
  assert.equal(stage(result, "hedge_open").legs.length, 2); assert.equal(result.openingImpactUsd, null);
});
test("订单超过请求量不做同量现金归因", () => {
  const data = fixture(); data.groups[0].attempts[0].filled_units = 5;
  const result = stage(deriveExecutionEvidence(data), "entropy_open");
  assert.equal(result.cashImpactUsd, null); assert.ok(result.limitations.some(row => row.includes("超过请求")));
});
test("未知成交数量不能变成零成交", () => {
  const data = fixture(); delete data.groups[0].attempts[0].filled_units; delete data.groups[0].attempts[0].result_units;
  const result = stage(deriveExecutionEvidence(data), "entropy_open").legs[0];
  assert.equal(result.filledUnits, null); assert.equal(result.priceImpactUsd, null);
});
test("非法字段类型和空白不解析为零数量", () => {
  const data = fixture(); data.groups[0].attempts[0].filled_units = [];
  data.groups[0].attempts[0].quantity_step = " ";
  const row = stage(deriveExecutionEvidence(data), "entropy_open").legs[0];
  assert.equal(row.filledUnits, null); assert.equal(row.quantity, null); assert.equal(row.priceImpactUsd, null);
});
test("不同 source 同ID和不同venue同order_ref保持身份独立", () => {
  const data = fixture(); data.groups[0].attempts[1].order_ref = data.groups[0].attempts[0].order_ref;
  const a = deriveExecutionEvidence(data, "basis"); const b = deriveExecutionEvidence(data, "funding");
  assert.notEqual(a.identity, b.identity);
  assert.notEqual(stage(a, "entropy_open").legs[0].key, stage(a, "hedge_open").legs[0].key);
  assert.notEqual(stage(a, "entropy_open").legs[0].key, stage(b, "entropy_open").legs[0].key);
});
test("执行路径证据缺失不凭发单时间推断顺序或并发", () => {
  const data = fixture(); for (const group of data.groups) for (const row of group.attempts) delete row.execution_path;
  const result = deriveExecutionEvidence(data);
  assert.equal(stage(result, "close").mode, "unknown"); assert.equal(stage(result, "hedge_open").marketMovementUsd, null);
});
test("费用不参与执行价格损耗，不改变净额或输入", () => {
  const data = fixture(); const before = JSON.stringify(data); const a = deriveExecutionEvidence(data);
  assert.equal(JSON.stringify(data), before);
  data.profit = { fees_usd: "9999", net_usd: "-9999" };
  for (const group of data.groups) for (const row of group.attempts) row.actual_fee_usd = "9999";
  assert.deepEqual(deriveExecutionEvidence(data).stages, a.stages);
  assert.equal(data.profit.net_usd, "-9999");
});
test("并发阶段不用错误的中间E成交→H快照拆单腿bp", () => {
  const data = fixture(); data.groups[1].attribution = { b_to_c_bps: "100", c_to_d_bps: "-100" };
  const result = stage(deriveExecutionEvidence(data), "close");
  assert.deepEqual(result.basisChanges, []);
});
test("两腿改善与损耗抵消时仍指出单腿变差", () => {
  const data = fixture();
  data.groups[0].attempts[0].avg_fill_px = "101"; data.groups[0].attempts[1].avg_fill_px = "99";
  delete data.groups[0].attempts[0].decision_quote_json;
  data.groups[1].attempts[0].avg_fill_px = "104"; data.groups[1].attempts[1].avg_fill_px = "102";
  const result = deriveExecutionEvidence(data);
  near(stage(result, "close").cashImpactUsd, 0);
  assert.match(result.conclusion, /两腿平仓（Entropy）少赚 0.400000 U/);
});
test("本腿无损耗但缓存盘口变差时结论指向市场移动", () => {
  const data = fixture();
  data.groups[0].attempts[0].avg_fill_px = "101"; data.groups[0].attempts[1].avg_fill_px = "99";
  delete data.groups[0].attempts[0].decision_quote_json;
  data.groups[1].attempts[0].avg_fill_px = "105"; data.groups[1].attempts[1].avg_fill_px = "103";
  const result = deriveExecutionEvidence(data);
  assert.match(result.conclusion, /Lighter 缓存盘口移动.*少赚 0.400000 U/);
  assert.match(result.conclusion, /不能当作本腿滑点/);
});
test("空证据保留未知和明确限制", () => {
  const result = deriveExecutionEvidence({});
  assert.equal(result.stages.length, 5); assert.ok(result.stages.every(row => row.cashImpactUsd == null));
  assert.match(result.conclusion, /暂时无法判断/);
});
console.log(`${checked} execution evidence checks passed`);

// Optional replay of read-only API snapshots. These expected facts are regression data, never UI data.
const realFacts = {
  "v3-1791642584950-c4ec566e": { opening: -0.034459, eOpen: 0, hOpen: 0.006472, eClose: 0, closeMode: "parallel", closeMovement: null },
  "v3-1791547487010-12a0f5f5": { opening: -0.070586, eOpen: -0.060850, hOpen: 0, eClose: -0.036510, closeMode: "sequential", closeMovement: 0.006085 },
  "v3-1791544178322-1375b860": { opening: -0.014690, eOpen: -0.011300, hOpen: -0.002486, eClose: -0.011300, closeMode: "sequential", closeMovement: 0 },
};
for (const path of process.argv.slice(2)) {
  const snapshot = JSON.parse(readFileSync(path, "utf8"));
  const data = snapshot.data || snapshot;
  const facts = realFacts[data.cycle?.cycle_id];
  assert.ok(facts, "Pass a snapshot of one of the three verified persisted cycles");
  const result = deriveExecutionEvidence(data);
  const e = stage(result, "entropy_open"), h = stage(result, "hedge_open"), c = stage(result, "close");
  near(result.openingImpactUsd, facts.opening); near(e.cashImpactUsd, facts.eOpen); near(h.cashImpactUsd, facts.hOpen);
  near(c.legs.find(row => row.venue === "entropy").priceImpactUsd, facts.eClose);
  assert.equal(c.mode, facts.closeMode);
  if (facts.closeMovement == null) assert.equal(c.marketMovementUsd, null); else near(c.marketMovementUsd, facts.closeMovement);
  assert.ok(c.legs.every(row => row.timingIssue == null && row.submitToConfirmMs >= 0));
  console.log(`verified real snapshot ${result.identity}: opening ${result.openingImpactUsd.toFixed(6)} U`);
}
