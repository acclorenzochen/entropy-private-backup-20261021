import type { Attempt, CycleDetailData } from "./types";

type Venue = "entropy" | "hedge";
type Mode = "sequential" | "parallel" | "unknown";
type EvidenceAttempt = Attempt & Record<string, unknown>;
type RecordValue = Record<string, unknown>;

export interface ExecutionLeg {
  key: string; orderRefs: string[]; venue: Venue; label: string;
  side: "buy" | "sell" | null; direction: string;
  requestedUnits: number | null; filledUnits: number | null; quantity: number | null;
  expectedVwap: number | null; actualVwap: number | null; priceImpactUsd: number | null;
  submitToConfirmMs: number | null; triggerToConfirmMs: number | null;
  timingBasis: string | null; timingIssue: string | null;
  partial: boolean; limitations: string[];
}

export interface ExecutionStage {
  id: "opportunity" | "entropy_open" | "hedge_open" | "holding" | "close";
  title: string; summary: string; mode: Mode;
  cashImpactUsd: number | null; marketMovementUsd: number | null;
  marketMovementLabel: string | null;
  basisChanges: { label: string; value: number }[];
  legs: ExecutionLeg[]; limitations: string[];
}

export interface ExecutionEvidence {
  identity: string; conclusion: string; stages: ExecutionStage[];
  openingImpactUsd: number | null; limitations: string[]; feeNote: string;
}

function number(value: unknown): number | null {
  if ((typeof value !== "number" && typeof value !== "string") || (typeof value === "string" && !value.trim())) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function object(value: unknown): RecordValue | null {
  if (typeof value === "string") {
    try { value = JSON.parse(value); } catch { return null; }
  }
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? value as RecordValue : null;
}

// Persisted depth quantities are integer units, not base asset quantities.
function quote(snapshot: unknown, venue: Venue, side: string | null, units: number | null, step: number | null): number | null {
  const book = object(snapshot);
  if (!book || !side || units == null || !Number.isSafeInteger(units) || units <= 0) return null;
  const snapshotStep = number(book.quantity_step);
  if (snapshotStep != null && snapshotStep !== step) return null;
  const levels = object(book[venue])?.[side === "buy" ? "asks" : "bids"];
  if (!Array.isArray(levels)) return null;
  let remaining = units;
  let cash = 0;
  for (const level of levels) {
    if (!Array.isArray(level) || level.length < 2) return null;
    const price = number(level[0]);
    const quantity = number(level[1]);
    if (price == null || price <= 0 || quantity == null || !Number.isSafeInteger(quantity) || quantity <= 0) return null;
    const take = Math.min(quantity, remaining);
    cash += price * take;
    remaining -= take;
    if (!remaining) return cash / units;
  }
  return null;
}

function filled(row: EvidenceAttempt): number | null {
  const value = number(row.filled_units ?? row.result_units);
  return value != null && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function timestamp(row: EvidenceAttempt, end: boolean): number | null {
  return end
    ? number(row.terminal_observed_ts ?? row.terminal_at ?? row.terminal_recorded_ts)
    : number(row.submit_start_ts ?? row.submitted_at ?? row.submitted_ts ?? row.submit_ts);
}

function elapsed(start: number | null, end: number | null, scale: number): { ms: number | null; issue: string | null } {
  if (start == null || end == null) return { ms: null, issue: null };
  if (end < start) return { ms: null, issue: "时钟证据异常：确认时间早于起点" };
  return { ms: (end - start) / scale, issue: null };
}

function timing(row: EvidenceAttempt, economics: RecordValue, closing: boolean) {
  const session = row.clock_session_id;
  const terminalSession = row.terminal_clock_session_id;
  const start = number(row.submit_start_mono_ns);
  const end = number(row.terminal_observed_mono_ns);
  const wall = elapsed(timestamp(row, false), timestamp(row, true), 0.001);
  const sameClock = typeof session === "string" && Boolean(session) && session === terminalSession;
  const own = sameClock && start != null && end != null ? elapsed(start, end, 1e6) : wall;
  const triggerSession = economics.exit_trigger_clock_session_id;
  const triggerMono = number(economics.exit_trigger_mono_ns);
  const common = closing
    ? (sameClock && session === triggerSession && triggerMono != null && end != null
      ? elapsed(triggerMono, end, 1e6)
      : elapsed(number(economics.exit_trigger_ts), timestamp(row, true), 0.001))
    : { ms: null, issue: null };
  return {
    submitToConfirmMs: own.ms, triggerToConfirmMs: common.ms,
    timingBasis: own.ms == null ? null : sameClock && start != null && end != null ? "同一单调时钟" : "本机墙钟",
    timingIssue: own.issue || common.issue || wall.issue,
  };
}

function leg(row: EvidenceAttempt, identity: string, economics: RecordValue, closing: boolean): ExecutionLeg {
  const venue = row.venue as Venue;
  const side = row.side === "buy" || row.side === "sell" ? row.side : null;
  const units = filled(row);
  const requested = number(row.requested_units);
  const step = number(row.quantity_step);
  const quantity = units != null && step != null && step > 0 ? units * step : null;
  const price = number(row.actual_fill_vwap ?? row.avg_fill_px);
  const actualVwap = units != null && units > 0 && price != null && price > 0 ? price : null;
  const expectedVwap = quote(row.submit_quote_json, venue, side, units, step);
  const limitations: string[] = [];
  if (units == null) limitations.push("成交数量未确认，不能按零成交处理");
  if (units != null && units > 0 && expectedVwap == null) limitations.push("缺少当时同量盘口，无法拆分本腿滑点");
  if (quantity == null && units != null && units > 0) limitations.push("缺少有效数量步长，不能换算 U 金额");
  if (units != null && units > 0 && actualVwap == null) limitations.push("成交均价未确认");
  if (side == null) limitations.push("交易方向未确认");
  const excess = units != null && requested != null && units > requested;
  if (excess) limitations.push("累计成交超过请求数量，证据待核对");
  const clock = timing(row, economics, closing);
  if (clock.timingIssue) limitations.push(clock.timingIssue);
  const terminal = row.state === "TERMINAL";
  const partial = units != null && requested != null && units > 0 && units < requested;
  if (partial) limitations.push("仅比较已确认成交数量，未成交部分不计价格影响");
  if (!terminal && units != null && units > 0) limitations.push("订单尚未终态，价格影响仅覆盖当前累计成交");
  // Positive means more cash received / less cash spent than the same-q submit book.
  const impact = !excess && side && quantity != null && actualVwap != null && expectedVwap != null
    ? (side === "sell" ? 1 : -1) * quantity * (actualVwap - expectedVwap) : null;
  return {
    key: `${identity}:${venue}:${row.order_ref}`, orderRefs: [row.order_ref], venue,
    label: venue === "entropy" ? "Entropy" : "Lighter", side,
    direction: side == null ? "方向未知" : `${side === "buy" ? "买入" : "卖出"}${closing ? "平仓" : "开仓"}`,
    requestedUnits: requested, filledUnits: units, quantity, expectedVwap, actualVwap,
    priceImpactUsd: impact, ...clock, partial, limitations,
  };
}

function sumKnown(legs: ExecutionLeg[]): number | null {
  return legs.length && legs.every(row => row.priceImpactUsd != null)
    ? legs.reduce((total, row) => total + row.priceImpactUsd!, 0) : null;
}

function cashText(value: number): string {
  if (Math.abs(value) < 1e-10) return "价格影响为 0 U";
  return `${value > 0 ? "多赚" : "少赚"} ${Math.abs(value).toFixed(6)} U`;
}

function phase(row: EvidenceAttempt, group: { kind?: string | null; purpose?: string | null }): "open" | "close" | "unknown" {
  const purpose = String(row.purpose ?? group.kind ?? group.purpose ?? "");
  if (purpose.startsWith("open")) return "open";
  if (purpose.startsWith("close")) return "close";
  return row.reduce_only === true || row.reduce_only === 1 ? "close" : "unknown";
}

function mode(rows: EvidenceAttempt[], closing: boolean): Mode {
  if (rows.length >= 2 && rows.every(row => row.execution_path === "parallel")) return "parallel";
  const first = rows.find(row => row.venue === "entropy" && typeof row.step_key === "string");
  const hedges = rows.filter(row => row.venue === "hedge");
  const sequentialPath = closing ? "fallback" : "standard";
  if (first && first.execution_path === sequentialPath && hedges.length && hedges.every(row =>
    row.execution_path === sequentialPath && row.parent_step_key === first.step_key)) return "sequential";
  return "unknown";
}

function sequentialIssue(first: EvidenceAttempt, second: EvidenceAttempt, label: string): string | null {
  const startMono = number(first.terminal_observed_mono_ns);
  const endMono = number(second.submit_start_mono_ns);
  const sameClock = first.terminal_clock_session_id != null && first.terminal_clock_session_id === second.clock_session_id;
  const gap = sameClock && startMono != null && endMono != null
    ? elapsed(startMono, endMono, 1e6) : elapsed(timestamp(first, true), timestamp(second, false), 0.001);
  return gap.issue ? `顺序${label}时钟证据异常：对冲发单早于首腿确认` : null;
}

/** Display comparisons only. Fees and net accounting are never recomputed here. */
export function deriveExecutionEvidence(data: CycleDetailData, source?: string): ExecutionEvidence {
  const identity = `${source || data.cycle?.source_id || "unknown"}:${data.cycle?.cycle_id || "unknown"}`;
  const economics = data.economics || {};
  const orders = new Map<string, { row: EvidenceAttempt; phase: "open" | "close" | "unknown" }>();
  for (const group of data.groups || []) {
    for (const attempt of group.attempts || []) {
      const row = attempt as EvidenceAttempt;
      if (row.venue !== "entropy" && row.venue !== "hedge") continue;
      const key = `${row.venue}:${row.order_ref}`;
      const previous = orders.get(key);
      // An order's cumulative snapshots are alternatives, never additional fills.
      if (!previous || (filled(row) ?? -1) > (filled(previous.row) ?? -1) ||
        (filled(row) === filled(previous.row) && (timestamp(row, true) ?? 0) > (timestamp(previous.row, true) ?? 0))) {
        orders.set(key, { row, phase: phase(row, group) });
      }
    }
  }
  const opening = [...orders.values()].filter(value => value.phase === "open").map(value => value.row);
  const closing = [...orders.values()].filter(value => value.phase === "close").map(value => value.row);
  const openMode = mode(opening, false);
  const closeMode = mode(closing, true);
  const entropyRows = opening.filter(row => row.venue === "entropy");
  const hedgeRows = opening.filter(row => row.venue === "hedge");
  const entropyLegs = entropyRows.map(row => leg(row, identity, economics, false));
  const hedgeLegs = hedgeRows.map(row => leg(row, identity, economics, false));
  const closeLegs = closing.map(row => leg(row, identity, economics, true));
  const first = entropyRows.length === 1 ? entropyRows[0] : null;
  let sequentialClockIssue = false;
  if (openMode === "sequential" && first) {
    hedgeRows.forEach((row, index) => {
      const issue = sequentialIssue(first, row, "开仓");
      if (issue) {
        sequentialClockIssue = true;
        hedgeLegs[index].timingIssue = issue;
        hedgeLegs[index].limitations.push(issue);
      }
    });
  }
  let firstObservationMovementUsd: number | null = null;
  let marketMovementUsd: number | null = null;
  if (openMode === "sequential" && !sequentialClockIssue && first && hedgeRows.length &&
    ["terminal_observed_local", "runtime_consumed_local", "exchange_asof"].includes(String(first.fill_mark_time_source))) {
    const changes = hedgeRows.map(row => {
      const units = filled(row);
      const step = number(row.quantity_step);
      const side = row.side === "buy" || row.side === "sell" ? row.side : null;
      const before = quote(first.fill_mark_quote_json, "hedge", side, units, step);
      const after = quote(row.submit_quote_json, "hedge", side, units, step);
      return before != null && after != null && units != null && step != null && step > 0 && side
        ? (side === "sell" ? 1 : -1) * units * step * (after - before) : null;
    });
    if (changes.every(value => value != null)) marketMovementUsd = changes.reduce<number>((total, value) => total + value!, 0);
    const units = filled(first);
    const step = number(first.quantity_step);
    const side = first.side === "buy" ? "sell" : first.side === "sell" ? "buy" : null;
    const before = quote(first.submit_quote_json, "hedge", side, units, step);
    const after = quote(first.fill_mark_quote_json, "hedge", side, units, step);
    if (before != null && after != null && units != null && step != null && step > 0 && side) {
      firstObservationMovementUsd = (side === "sell" ? 1 : -1) * units * step * (after - before);
    }
  }
  let opportunityCash: number | null = null;
  if (first && hedgeRows.length && number(first.quantity_step) != null) {
    const units = filled(first);
    const step = number(first.quantity_step);
    const side = first.side === "buy" || first.side === "sell" ? first.side : null;
    const opposite = side === "buy" ? "sell" : side === "sell" ? "buy" : null;
    const paired = units != null && units > 0 && step != null && step > 0 && hedgeRows.every(row =>
      row.side === opposite && number(row.quantity_step) === step && filled(row) != null) &&
      hedgeRows.reduce((total, row) => total + (filled(row) ?? 0), 0) === units;
    if (paired && side && opposite) {
      const ea = quote(first.decision_quote_json, "entropy", side, units, step);
      const eb = quote(first.submit_quote_json, "entropy", side, units, step);
      const ha = quote(first.decision_quote_json, "hedge", opposite, units, step);
      const hb = quote(first.submit_quote_json, "hedge", opposite, units, step);
      if (ea != null && eb != null && ha != null && hb != null) opportunityCash =
        (side === "sell" ? 1 : -1) * units! * step! * ((eb - ea) - (hb - ha));
    }
  }
  const bp = (value: unknown, label: string): ExecutionStage["basisChanges"] => {
    const parsed = number(value);
    return parsed == null ? [] : [{ label, value: parsed }];
  };
  const ownStage = (id: "entropy_open" | "hedge_open", title: string, legs: ExecutionLeg[]): ExecutionStage => {
    const impact = sumKnown(legs);
    return { id, title, mode: openMode, cashImpactUsd: impact,
      marketMovementUsd: id === "hedge_open" ? marketMovementUsd : firstObservationMovementUsd,
      marketMovementLabel: id === "hedge_open" ? "首腿结果观察→对冲发单前，Lighter 缓存盘口移动" : "首腿发单前→其结果观察，Lighter 缓存盘口移动",
      summary: impact == null ? "缺少当时盘口，无法拆分本腿滑点" : `相对各笔发单前同量可成交均价，${cashText(impact)}`,
      basisChanges: [], legs, limitations: [...new Set(legs.flatMap(row => row.limitations))] };
  };
  const closeImpact = sumKnown(closeLegs);
  let closeMarketMovementUsd: number | null = null;
  if (closeMode === "sequential") {
    const firstClose = closing.filter(row => row.venue === "entropy");
    const hedgeClose = closing.filter(row => row.venue === "hedge");
    if (firstClose.length === 1) {
      for (const row of hedgeClose) {
        const issue = sequentialIssue(firstClose[0], row, "平仓");
        if (issue) {
          const target = closeLegs.find(item => item.orderRefs[0] === row.order_ref && item.venue === "hedge")!;
          target.timingIssue = issue; target.limitations.push(issue);
        }
      }
    }
    if (closing.length === 2 && firstClose.length === 1 && hedgeClose.length === 1 &&
      closeLegs.every(row => !row.partial && row.timingIssue == null) &&
      filled(firstClose[0]) === filled(hedgeClose[0]) && number(firstClose[0].quantity_step) === number(hedgeClose[0].quantity_step)) {
      const row = hedgeClose[0];
      const units = filled(row);
      const step = number(row.quantity_step);
      const side = row.side === "buy" || row.side === "sell" ? row.side : null;
      const before = quote(firstClose[0].submit_quote_json, "hedge", side, units, step);
      const after = quote(row.submit_quote_json, "hedge", side, units, step);
      if (before != null && after != null && units != null && step != null && step > 0 && side) {
        closeMarketMovementUsd = (side === "sell" ? 1 : -1) * units * step * (after - before);
      }
    }
  }
  const stages: ExecutionStage[] = [
    { id: "opportunity", title: "发现机会", mode: openMode, cashImpactUsd: opportunityCash, marketMovementUsd: null, marketMovementLabel: null,
      summary: opportunityCash == null ? "缺少同量配对盘口，机会到发单前的金额变化无法核实" : `从发现机会到 Entropy 发单前，配对盘口${cashText(opportunityCash)}`,
      basisChanges: bp(economics.signal_executable_basis_bps, "发现机会时的可执行价差"),
      legs: [], limitations: opportunityCash == null ? ["复合价差指标不能直接归为某一腿滑点，也不能仅凭 bp 换算 U"] : [] },
    ownStage("entropy_open", "Entropy 开仓", entropyLegs),
    ownStage("hedge_open", "Lighter 对冲", hedgeLegs),
    { id: "holding", title: "持仓期间价差变化", mode: "unknown", cashImpactUsd: null, marketMovementUsd: null, marketMovementLabel: null,
      summary: "持仓价差变化反映两边市场行情，不归为本腿成交滑点",
      basisChanges: bp(economics.exit_trigger_reversion_bps, "该阶段价差变化（开仓配平到平仓触发）"),
      legs: [], limitations: ["仅有复合价差时保留 bp 说明，不换算或重复计入价格盈亏"] },
    { id: "close", title: "两腿平仓", mode: closeMode, cashImpactUsd: closeImpact, marketMovementUsd: closeMarketMovementUsd,
      marketMovementLabel: closeMode === "sequential" ? "平仓首腿发单前→对冲发单前，Lighter 缓存盘口移动" : null,
      summary: `${closeMode === "parallel" ? "持久化记录确认并发平仓，两腿从共同触发点分别计时。" : closeMode === "unknown" ? "缺少明确执行方式证据，不推断两腿先后。" : "顺序平仓。"}${closeImpact == null ? "缺少当时盘口，无法拆分本腿滑点" : `相对各笔发单前同量可成交均价，${cashText(closeImpact)}`}`,
      basisChanges: [],
      legs: closeLegs, limitations: [...new Set(closeLegs.flatMap(row => row.limitations))] },
  ];
  if (openMode !== "sequential") stages[2].limitations.push("缺少顺序开仓关联证据，无法拆分两腿间盘口移动");
  else if (marketMovementUsd == null) stages[2].limitations.push("缺少首腿确认时或对冲发单前同量盘口，无法拆分两腿间盘口移动");
  const measured = stages.flatMap(stage => stage.legs.map(row => ({ title: `${stage.title}（${row.label}）`, impact: row.priceImpactUsd })));
  const worst = measured.filter(row => row.impact != null && row.impact < -1e-10).sort((a, b) => a.impact! - b.impact!)[0];
  const worstMovement = stages.filter(stage => stage.marketMovementUsd != null && stage.marketMovementUsd < -1e-10)
    .sort((a, b) => a.marketMovementUsd! - b.marketMovementUsd!)[0];
  const allLegs = [...entropyLegs, ...hedgeLegs, ...closeLegs];
  const complete = allLegs.length > 0 && allLegs.every(row => row.priceImpactUsd != null);
  const singlePairedOpening = openMode === "sequential" && opening.length === 2 && entropyRows.length === 1 && hedgeRows.length === 1 &&
    first != null && filled(first) != null && filled(first)! > 0 && filled(first) === filled(hedgeRows[0]) &&
    number(first.quantity_step) != null && number(first.quantity_step) === number(hedgeRows[0].quantity_step) &&
    entropyLegs[0].side != null && entropyLegs[0].side !== hedgeLegs[0].side && opening.every(row =>
      row.state === "TERMINAL" && filled(row) === number(row.requested_units)) &&
    [...entropyLegs, ...hedgeLegs].every(row => row.timingIssue == null);
  const openingParts = [opportunityCash, entropyLegs[0]?.priceImpactUsd, firstObservationMovementUsd,
    marketMovementUsd, hedgeLegs[0]?.priceImpactUsd];
  // These five adjoining same-q comparisons telescope from decision books to actual paired fills.
  // With partial/mismatched quantities or multiple attempts, there is no single additive baseline.
  const openingImpactUsd = singlePairedOpening && openingParts.every(value => value != null)
    ? openingParts.reduce<number>((total, value) => total + value!, 0) : null;
  const conclusion = openingImpactUsd != null
    ? `相对发现机会时的同量盘口，实际开仓${cashText(openingImpactUsd)}。下方分开列出盘口变化与本腿滑点；记录不能证明盘口变化由等待造成。`
    : worst
    ? `${complete ? "同量发单盘口对比中" : "在已能核实的价格对比中"}，${worst.title}${cashText(worst.impact!)}。${complete ? "" : "其余缺少关键盘口的阶段无法拆分。"}`
    : worstMovement ? `${worstMovement.marketMovementLabel}，同量对比${cashText(worstMovement.marketMovementUsd!)}。这属于盘口变化，不能当作本腿滑点或断言由等待造成。`
    : complete ? "已有同量盘口未显示本腿成交价格损耗；两腿间行情变化单独列出。" : "缺少关键盘口，暂时无法判断执行过程在哪里变差。已确认均价和本腿耗时仍可查看。";
  return { identity, conclusion, stages, openingImpactUsd,
    limitations: [...new Set([...stages.flatMap(stage => stage.limitations), ...(firstObservationMovementUsd != null || marketMovementUsd != null || closeMarketMovementUsd != null
      ? ["盘口移动来自持久化缓存快照对比，不能断言由等待造成"] : [])])],
    feeNote: "价格对比不包含手续费或资金费，只用于解释执行过程；不再从已展示的净收益扣除。" };
}
