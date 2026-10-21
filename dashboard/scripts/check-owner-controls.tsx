// Fully offline owner-control acceptance. No real API, engine, socket or orders.
// Build with VITE_V3_OWNER_CONTROL=1; polling is accelerated only in this fixture.
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import OperatorControls from "../src/v3/OperatorControls";
import { localOwnerControlUrl } from "../src/v3/controlOrigin";
import type { OwnerParameters } from "../src/v3/ParameterDialog";
import "../src/styles.css";
import "../src/v3/FourPage.css";

type Action = "start" | "stop" | "close";
type State = { owner_instance_id: string; intent: "RUNNING" | "STOPPING" | "STOPPED"; intent_revision: number; allowed_actions: Action[]; risk_locks: string[]; active_cycle_id: string | null; pair_identity: string; account_binding: string; batch_id: string; config_hash: string; entropy_units: number | null; hedge_units: number | null; quantity_step: string; position_observed_ts: number; position_authoritative: boolean; pending_orders: unknown[]; parameters: OwnerParameters };
type Order = { order_ref: string; state: string; filled_units: number | null; reduce_only: boolean };
type Operation = { operation_id: string; action: Action; state: string; evidence: { accounting_final: boolean; net_pnl_usd: string | null; known_fees_usd: string | null; fees_complete: boolean; legs: { venue: string; initial_units: number; remaining_units: number | null; orders: Order[] }[] } };
type Preview = { preview_id: string; action: Action; expires_at: number; state: State; expected_cycle_id: string | null };
type Request = { path: string; method: string; body: Record<string, string> | null; csrf: string | null };
const initial = (): State => ({ owner_instance_id: "owner-test-a", intent: "RUNNING", intent_revision: 0, allowed_actions: ["start", "stop", "close"], risk_locks: [], active_cycle_id: "mock-cycle", pair_identity: "TEST:Entropy:Lighter", account_binding: "mock-account", batch_id: "mock-batch", config_hash: "mock-config", entropy_units: 257, hedge_units: -257, quantity_step: "0.0001", position_observed_ts: Date.now()/1000, position_authoritative: true, pending_orders: [], parameters: { base_config_hash: "mock-config", current: { revision: 0, config_hash: "mock-config", values: { entry_band_bps: "5", exit_min_gross_profit_bps: "2.5", entropy_exit_room_bps: "12", lighter_exit_room_bps: "12", max_notional_usd: "200" } }, saved: null, can_save: false, reason_code: "parameters_require_stopped_flat", limits: { entry_band_bps: "100", exit_min_gross_profit_bps: "100", entropy_exit_room_bps: "100", lighter_exit_room_bps: "100", max_notional_usd: "200" } } });
let owner = initial(), enabled = true, mode: "normal" | "hold" | "lost-after" | "lost-before" = "normal", commits = 0;
const requests: Request[] = [], previews = new Map<string, Preview>(), operations = new Map<string, Operation>();
let held: (() => void) | null = null;
let heldLookup: (() => void) | null = null, deferLookup = false;
let sessionExpired = false, offline = false, csrfToken = "mock-csrf", sessionVersion = 0;
let disabledReason = "owner_unavailable", deferState = false, deferSession = false;
let heldState: (() => void) | null = null, heldSession: (() => void) | null = null;
let activeSessions = 0, maxActiveSessions = 0;
const snapshot = (data: unknown, status = 200) => new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
const refusal = (code: string, status = 409) => snapshot({ detail: { code, message: `mock:${code}` } }, status);
const result = (operation: Operation) => snapshot({ operation_id: operation.operation_id, operation: structuredClone(operation), state: structuredClone(owner) });
window.fetch = (async (url: string | URL | globalThis.Request, init?: RequestInit) => {
  const parsed = new URL(String(url), window.location.href), method = init?.method || "GET";
  const body = init?.body ? JSON.parse(String(init.body)) as Record<string, string> : null;
  const headers = new Headers(init?.headers);
  requests.push({ path: parsed.pathname, method, body, csrf: headers.get("X-Entropy-CSRF") });
  if (parsed.origin !== location.origin || !parsed.pathname.startsWith("/api/v3-control/")) throw new Error(`UNEXPECTED NETWORK BLOCKED: ${parsed.pathname}`);
  const endpoint = parsed.pathname.replace("/api/v3-control/", "");
  if (offline) throw new TypeError("mock:offline");
  if (endpoint !== "session" && sessionExpired) return refusal("session_expired", 403);
  if (method === "POST" && headers.get("X-Entropy-CSRF") !== csrfToken) return refusal("csrf_failed", 403);
  if (["session", "state"].includes(endpoint) && method === "GET") {
    if (endpoint === "session" && sessionExpired) { csrfToken = `mock-csrf-${++sessionVersion}`; sessionExpired = false; }
    const response = snapshot({ enabled, reason: enabled ? null : `mock:${disabledReason}`, reason_code: enabled ? null : disabledReason, csrf_token: endpoint === "session" ? csrfToken : undefined, state: enabled ? structuredClone(owner) : null });
    if (endpoint === "state" && deferState) return new Promise<Response>(resolve => { deferState = false; heldState = () => { heldState = null; resolve(response); }; });
    if (endpoint === "session") {
      activeSessions++; maxActiveSessions = Math.max(maxActiveSessions, activeSessions);
      try {
        if (deferSession) return await new Promise<Response>(resolve => { deferSession = false; heldSession = () => { heldSession = null; resolve(response); }; });
        return response;
      } finally { activeSessions--; }
    }
    return response;
  }
  if (endpoint === "preview" && method === "POST") {
    if (!enabled) return refusal("owner_unavailable", 503);
    const preview: Preview = { preview_id: crypto.randomUUID(), action: body!.action as Action, expires_at: Date.now()/1000 + 15, state: structuredClone(owner), expected_cycle_id: body!.action === "close" ? body!.expected_cycle_id : null };
    previews.set(preview.preview_id, preview); return snapshot(preview);
  }
  if (endpoint === "confirm" && method === "POST") {
    const preview = previews.get(body!.preview_id);
    if (!preview) return refusal("preview_unavailable");
    if (mode === "lost-before") { mode = "normal"; throw new TypeError("mock:response_lost_before_acceptance"); }
    let operation = operations.get(body!.idempotency_key);
    if (!operation) {
      if (preview.state.owner_instance_id !== owner.owner_instance_id || preview.state.intent_revision !== owner.intent_revision || preview.state.entropy_units !== owner.entropy_units || preview.state.hedge_units !== owner.hedge_units) return refusal("close_evidence_changed");
      commits++;
      if (preview.action !== "close") { owner.intent = preview.action === "start" ? "RUNNING" : "STOPPING"; owner.intent_revision++; }
      operation = { operation_id: crypto.randomUUID(), action: preview.action, state: "executing", evidence: { accounting_final: false, net_pnl_usd: null, known_fees_usd: null, fees_complete: false, legs: ["entropy", "hedge"].map(venue => ({ venue, initial_units: venue === "entropy" ? 257 : -257, remaining_units: venue === "entropy" ? 257 : -257, orders: [{ order_ref: `mock-${venue}`, state: "UNKNOWN", filled_units: 0, reduce_only: true }] })) } };
      operations.set(body!.idempotency_key, operation);
    }
    if (mode === "lost-after") { mode = "normal"; throw new TypeError("mock:response_lost_after_acceptance"); }
    if (mode === "hold") return new Promise<Response>(resolve => { held = () => { mode = "normal"; held = null; resolve(result(operation!)); }; });
    return result(operation);
  }
  if (endpoint.startsWith("operations/by-key/") && method === "GET") {
    if (deferLookup) return new Promise<Response>(resolve => { heldLookup = () => { deferLookup = false; heldLookup = null; resolve(refusal("operation_not_found", 404)); }; });
    const operation = operations.get(endpoint.split("/").at(-1)!);
    return operation ? result(operation) : refusal("operation_not_found", 404);
  }
  if (endpoint.startsWith("operations/") && method === "GET") {
    const operation = [...operations.values()].find(row => row.operation_id === endpoint.split("/").at(-1));
    return operation ? result(operation) : refusal("operation_not_found", 404);
  }
  throw new Error(`UNEXPECTED REQUEST BLOCKED: ${method} ${endpoint}`);
}) as typeof fetch;
const nativeInterval = window.setInterval.bind(window);
window.setInterval = ((handler: TimerHandler, timeout?: number, ...args: unknown[]) => nativeInterval(handler, timeout === 5000 ? 80 : timeout === 2000 ? 60 : timeout, ...args)) as typeof window.setInterval;
const nativeTimeout = window.setTimeout.bind(window), backoffs: number[] = [];
window.setTimeout = ((handler: TimerHandler, timeout?: number, ...args: unknown[]) => {
  if (timeout != null && [1000, 2000, 5000, 10000, 20000].includes(timeout)) { backoffs.push(timeout); timeout /= 50; }
  return nativeTimeout(handler, timeout, ...args);
}) as typeof window.setTimeout;
const root = createRoot(document.getElementById("test-root")!), output = document.querySelector<HTMLPreElement>("#results")!;
const checks: string[] = [], checkpoints: string[] = [];
let generation = 0, resume: (() => void) | null = null;
const qa = { checkpoint: "", checks, requests, get commits() { return commits; }, get owner() { return owner; }, get operations() { return [...operations.values()]; }, continue: () => { qa.checkpoint = ""; resume?.(); resume = null; } };
(window as unknown as { ownerQA: typeof qa }).ownerQA = qa;
const tick = () => new Promise(resolve => setTimeout(resolve, 25));
function check(value: unknown, message: string) { if (!value) throw new Error(message); checks.push(message); output.textContent = checks.map(item => `✓ ${item}`).join("\n"); }
async function waitFor(test: () => unknown, message: string) { const started = Date.now(); while (!test()) { if (Date.now() - started > 7000) throw new Error(message); await tick(); } }
async function checkpoint(name: string) { checkpoints.push(name); if (new URLSearchParams(window.location.search).has("capture")) { qa.checkpoint = name; await new Promise<void>(resolve => { resume = resolve; }); } }
function dialog() { return document.querySelector<HTMLDialogElement>(".v3-owner-dialog"); }
function button(text: string) { return [...document.querySelectorAll<HTMLButtonElement>("button")].find(node => node.textContent === text); }
function click(text: string) { const node = button(text); if (!node) throw new Error(`Missing button: ${text}`); node.focus({ preventScroll: true }); node.click(); }
const confirms = () => requests.filter(row => row.path.endsWith("/confirm"));
function renderSource(source: "basis" | "funding" = "basis") {
  root.render(<StrictMode><div key={generation} data-generation={generation} className="v3-app v3-four-page" style={{ display: "block", minHeight: 300, padding: 24 }}><h1>交易控制 · 完全离线 mock</h1><OperatorControls source={source} /></div></StrictMode>);
}
async function mount(available = true) {
  enabled = available; owner = initial(); commits = 0; mode = "normal"; deferLookup = false; heldLookup = null; previews.clear(); operations.clear(); requests.length = 0; sessionStorage.clear();
  sessionExpired = offline = deferState = deferSession = false; heldState = heldSession = null; csrfToken = "mock-csrf"; sessionVersion = 0; disabledReason = "owner_unavailable"; activeSessions = maxActiveSessions = 0; backoffs.length = 0;
  generation++;
  renderSource();
  await waitFor(() => document.querySelector<HTMLElement>('[data-generation]')?.dataset.generation === String(generation) && requests.some(row => row.path.endsWith('/session')) && button("启动") && (available ? !button("停止")!.disabled : button("启动")?.disabled), "Control initial state");
}
async function preview(action: string) { click(action); await waitFor(() => dialog()?.open && dialog()?.querySelector('.v3-owner-facts'), "Preview dialog"); }
function current() { const operation = [...operations.values()][0]; if (!operation) throw new Error("No mock operation"); return operation; }
async function run() {
  check(localOwnerControlUrl("http://localhost:5186/a?mode=basis#/overview?source=basis", true) === "http://127.0.0.1:5186/a?mode=basis#/overview?source=basis", "localhost入口保留端口、路径、参数与hash，统一为127.0.0.1");
  check(["http://127.0.0.1:5186/", "https://localhost:5186/", "http://example.invalid/", "http://localhost.example.invalid/"] .every(url => localOwnerControlUrl(url, true) === null) && localOwnerControlUrl("http://localhost:5186/", false) === null, "HTTPS、公网、已有127入口及flag关闭不重定向");
  if (import.meta.env.VITE_V3_OWNER_CONTROL !== "1") {
    root.render(<StrictMode><div className="v3-app v3-four-page"><OperatorControls /></div></StrictMode>);
    await waitFor(() => button("平仓"), "Disabled production controls");
    await tick();
    check(requests.length === 0 && [button("启动"), button("停止"), button("平仓")].every(node => node?.disabled), "生产flag默认关闭，不发任何控制请求，三按钮不可用");
    output.dataset.result = "passed"; output.textContent = checks.join("\n"); return;
  }
  await mount();
  const buttons = [button("启动")!, button("停止")!, button("平仓")!], rects = buttons.map(node => node.getBoundingClientRect());
  check(rects.every(rect => Math.abs(rect.top - rects[0].top) < 1), "启动、停止、平仓三个按钮同排");
  for (const action of ["启动", "停止", "平仓"]) {
    if (action === "启动") {
      owner.intent = "STOPPED"; owner.intent_revision++; owner.entropy_units = owner.hedge_units = 0; owner.active_cycle_id = null; owner.parameters.can_save = true;
      document.dispatchEvent(new Event("visibilitychange")); await waitFor(() => !button("启动")?.disabled, "Stopped flat start boundary");
    }
    await preview(action); check(confirms().length === 0 && commits === 0, `${action}仅预检，不提交confirm或改变运行意图`); click("收起"); await waitFor(() => !dialog(), "Close preview");
    if (action === "启动") {
      owner.intent = "RUNNING"; owner.intent_revision++; owner.entropy_units = 257; owner.hedge_units = -257; owner.active_cycle_id = "mock-cycle"; owner.parameters.can_save = false;
      document.dispatchEvent(new Event("visibilitychange")); await waitFor(() => !button("平仓")?.disabled, "Restore active cycle boundary");
    }
  }
  await preview("平仓");
  check(dialog()?.textContent?.includes("预计可成交毛盈亏") && dialog()?.textContent?.includes("未知") && !dialog()?.textContent?.includes("0 U"), "毛估值、费用缺失显示未知，不伪造零净额");
  await checkpoint("preview");
  mode = "hold"; click("确认平仓"); click("确认平仓"); await waitFor(() => held, "Deferred confirmation");
  check(confirms().length === 1 && commits === 1 && Boolean(confirms()[0].body?.idempotency_key), "最终确认双点击只提交一次并保存稳定幂等键");
  held!(); await waitFor(() => dialog()?.querySelector('.v3-owner-progress'), "Initial UNKNOWN progress");
  check(dialog()?.textContent?.includes("UNKNOWN · 等待原订单核对") && dialog()?.textContent?.includes("待核算 · 净值未知"), "UNKNOWN原订单与未知净额明确展示");
  await checkpoint("unknown");
  const operation = current(); operation.evidence.legs[0].remaining_units = 157; operation.evidence.legs[0].orders[0].state = "PARTIAL"; operation.evidence.legs[0].orders[0].filled_units = 100;
  await waitFor(() => dialog()?.textContent?.includes("累计成交 100 units · 部分成交"), "Partial cumulative progress");
  const polls = requests.filter(row => row.path.includes(`/operations/${operation.operation_id}`)).length;
  await waitFor(() => requests.filter(row => row.path.includes(`/operations/${operation.operation_id}`)).length >= polls + 2, "Repeated same cumulative snapshots");
  check(dialog()?.textContent?.includes("累计成交 100 units") && !dialog()?.textContent?.includes("累计成交 200") && dialog()?.querySelectorAll('.v3-owner-legs section:first-child code').length === 1, "重复poll的部分成交累计100保持100，不叠加或新增订单");
  await checkpoint("partial");
  operation.state = "completed"; operation.evidence.accounting_final = true; operation.evidence.fees_complete = true; operation.evidence.net_pnl_usd = "-0.004772"; operation.evidence.known_fees_usd = "0.031";
  operation.evidence.legs.forEach(leg => { leg.remaining_units = 0; leg.orders[0].state = "TERMINAL"; leg.orders[0].filled_units = 257; }); owner.entropy_units = owner.hedge_units = 0; owner.active_cycle_id = null;
  await waitFor(() => dialog()?.textContent?.includes("-0.004772 U · 已核算"), "Completed accounting");
  check(dialog()?.textContent?.includes("完成 · 已核算"), "completed与accounting_final同时明确已核算"); await checkpoint("completed");
  operation.state = "flat_pending_accounting"; operation.evidence.accounting_final = false; operation.evidence.fees_complete = false; operation.evidence.net_pnl_usd = null; operation.evidence.known_fees_usd = "0.032";
  await waitFor(() => dialog()?.textContent?.includes("双腿归零 · 完成待核算") && !dialog()?.textContent?.includes("-0.004772 U"), "Late fee revoked finality");
  check(dialog()?.textContent?.includes("待核算 · 净值未知") && dialog()?.textContent?.includes("已知 0.032 U"), "completed仍持续poll，迟到费用撤销finality并清旧净额，已知费用单列"); await checkpoint("late-fee");
  for (const mutation of ["owner", "revision", "quantity"]) {
    await mount(); await preview("平仓");
    if (mutation === "owner") owner.owner_instance_id = "owner-test-b";
    else if (mutation === "revision") owner.intent_revision++;
    else owner.entropy_units = 258;
    await waitFor(() => Boolean(button("重新预检")) && !button("确认平仓"), `${mutation} invalidates preview`);
    check(confirms().length === 0 && commits === 0, `${mutation}变化后禁最终确认，必须重新预检`);
  }
  await mount(); await preview("平仓"); mode = "lost-after"; click("确认平仓");
  await waitFor(() => dialog()?.querySelector('.v3-owner-progress'), "Lost response by-key recovery");
  check(confirms().length === 1 && commits === 1 && requests.some(row => row.path.includes('/operations/by-key/')), "确认回报丢失后按原键恢复已接受操作，无二次提交");
  click("收起"); await waitFor(() => !dialog(), "Collapse operation"); click("核对操作"); await waitFor(() => dialog()?.querySelector('.v3-owner-progress'), "Reopen operation");
  check(confirms().length === 1 && commits === 1, "收起重开复用同一操作，不重新预检或下发confirm");
  await checkpoint("reopened");
  click("收起"); await waitFor(() => !dialog(), "Collapse reopened operation");
  check(document.activeElement === button("核对操作"), "已知操作重开后收起，焦点回核对操作触发按钮");
  await mount(); await preview("平仓"); mode = "lost-before"; click("确认平仓");
  await waitFor(() => button("核对原操作 / 原键重试") && !button("核对原操作 / 原键重试")!.disabled, "UNKNOWN before acceptance");
  const first = JSON.stringify(confirms()[0].body); click("核对原操作 / 原键重试"); await waitFor(() => dialog()?.querySelector('.v3-owner-progress'), "Replay same confirmation");
  check(confirms().length === 2 && confirms().every(row => JSON.stringify(row.body) === first) && commits === 1, "接受前失响应仅以原preview+幂等键重试，真实mock提交一次");
  await mount(false); await tick();
  check([button("启动"), button("停止"), button("平仓")].every(node => node?.disabled) && !dialog() && confirms().length === 0, "无owner时三按钮不可用，不出现可执行确认"); await checkpoint("unavailable");
  generation++; renderSource("funding");
  await waitFor(() => document.querySelector<HTMLElement>('[data-generation]')?.dataset.generation === String(generation) && document.querySelector('.v3-operator-controls')?.textContent?.includes("资金费控制未接入"), "Funding controls");
  requests.length = 0;
  await tick(); await tick(); await tick(); await tick();
  check(requests.length === 0 && [button("启动"), button("停止"), button("平仓")].every(node => node?.disabled) && !button("核对操作") && !dialog(), "funding来源禁用全部控制，不读取basis owner或旧操作"); await checkpoint("funding");
  await mount(); await preview("平仓"); mode = "hold"; click("确认平仓"); await waitFor(() => held, "In-flight confirmation");
  renderSource("funding"); await waitFor(() => !dialog() && button("平仓")?.disabled, "Source switch clears dialog");
  held!(); await tick(); await tick();
  check([button("启动"), button("停止"), button("平仓")].every(node => node?.disabled) && !dialog() && !button("核对操作"), "切换funding后迟到basis确认回报不能重启跨标的控制");
  await mount(); await preview("平仓"); mode = "lost-before"; click("确认平仓");
  await waitFor(() => button("核对原操作 / 原键重试") && !button("核对原操作 / 原键重试")!.disabled, "Lookup recovery enabled");
  deferLookup = true; click("核对原操作 / 原键重试"); await waitFor(() => heldLookup, "Delayed by-key lookup");
  renderSource("funding"); await waitFor(() => !dialog() && button("平仓")?.disabled, "Lookup source switch");
  heldLookup!(); await tick(); await tick();
  check(confirms().length === 1 && commits === 0 && [button("启动"), button("停止"), button("平仓")].every(node => node?.disabled), "by-key查找中切换funding，旧404回报不得触发confirm重放");
  await mount(); await preview("平仓");
  check(requests.filter(row => row.method === "POST").every(row => row.csrf === "mock-csrf") && requests.every(row => row.path.startsWith('/api/v3-control/')), "全部控制请求由mock拦截，POST使用会话CSRF");
  await checkpoint("mobile");

  await mount(); await preview("停止");
  const previousPosts = requests.filter(row => row.method === "POST").length;
  sessionExpired = true; deferSession = true;
  document.dispatchEvent(new Event("visibilitychange"));
  await waitFor(() => heldSession && button("停止")?.disabled, "Expired session reconnect");
  for (let index = 0; index < 8; index++) document.dispatchEvent(new Event("visibilitychange"));
  await tick(); await tick();
  check(maxActiveSessions === 1 && !button("确认停止") && requests.filter(row => row.method === "POST").length === previousPosts, "会话过期清旧预检，visibility并发刷新仅一个续期GET且不重发POST");
  heldSession!(); await waitFor(() => !button("停止")?.disabled, "Renewed session valid state");
  click("重新预检"); await waitFor(() => button("确认停止"), "Explicit new preview");
  check(requests.filter(row => row.path.endsWith('/preview')).at(-1)?.csrf === "mock-csrf-1" && commits === 0, "续期GET state验证后恢复按钮，新显式预检使用换新的CSRF且不commit");

  await mount(); deferState = true; document.dispatchEvent(new Event("visibilitychange"));
  await waitFor(() => heldState, "Delayed state request");
  const reads = requests.filter(row => row.path.endsWith('/state')).length;
  for (let index = 0; index < 8; index++) document.dispatchEvent(new Event("visibilitychange"));
  await tick(); await tick();
  check(requests.filter(row => row.path.endsWith('/state')).length === reads, "迟到state GET期间visibility与轮询刷新串行去重");
  renderSource("funding"); await waitFor(() => button("停止")?.disabled && document.querySelector('.v3-operator-controls')?.textContent?.includes("资金费控制未接入"), "Delayed state source switch");
  heldState!(); await tick(); await tick();
  check(button("停止")?.disabled && !dialog(), "source切换后迟到GET不能污染跨标的控制");

  await mount();
  offline = true; document.dispatchEvent(new Event("visibilitychange"));
  await waitFor(() => button("停止")?.disabled && document.querySelector('.v3-operator-controls')?.textContent?.includes("重新连接中"), "Offline reconnection display");
  offline = false; await waitFor(() => !button("停止")?.disabled, "Temporary transport recovery");
  check(requests.filter(row => row.method === "POST").length === 0 && requests.some(row => row.path.endsWith('/session')), "服务暂时断连自动GET续期恢复，不发任何交易POST");

  await mount(); enabled = false; document.dispatchEvent(new Event("visibilitychange"));
  await waitFor(() => button("停止")?.disabled, "Owner temporarily unavailable");
  enabled = true; await waitFor(() => !button("停止")?.disabled, "Owner capability recovery");
  check(commits === 0 && requests.filter(row => row.method === "POST").length === 0, "owner_unavailable能力响应也能有界自动恢复且零POST");

  await mount();
  const initialSessions = requests.filter(row => row.path.endsWith('/session')).length;
  offline = true; document.dispatchEvent(new Event("visibilitychange"));
  await waitFor(() => button("重新连接"), "Retry budget exhausted");
  const exhaustedRequests = requests.length;
  await tick(); await tick(); await tick(); await tick();
  for (let index = 0; index < 8; index++) document.dispatchEvent(new Event("visibilitychange"));
  await tick();
  check(requests.length === exhaustedRequests && requests.filter(row => row.path.endsWith('/session')).length - initialSessions === 5 && backoffs.includes(1000) && JSON.stringify(backoffs.filter(delay => delay !== 1000)) === JSON.stringify([2000, 5000, 10000, 20000]), "1/2/5/10/20秒五次退避耗尽停止，轮询与visibility不隐式重启");
  offline = false; deferSession = true; click("重新连接"); click("重新连接");
  await waitFor(() => heldSession, "Manual connection held");
  check(maxActiveSessions === 1 && requests.length === exhaustedRequests + 1, "明确重新连接双点击仍只有一个GET会话");
  heldSession!(); await waitFor(() => !button("停止")?.disabled, "Explicit manual recovery");

  await mount(); enabled = false; disabledReason = "binding_mismatch"; document.dispatchEvent(new Event("visibilitychange"));
  await waitFor(() => button("重新连接"), "Terminal binding refusal");
  const deniedRequests = requests.length;
  await tick(); await tick(); await tick(); await tick();
  check(requests.length === deniedRequests && button("停止")?.disabled, "binding拒绝停止自动重试，鉴权边界保持关闭");

  await mount(); await preview("平仓"); mode = "lost-before"; click("确认平仓");
  await waitFor(() => button("核对原操作 / 原键重试") && !button("核对原操作 / 原键重试")!.disabled, "Unknown confirmation retained");
  const originalKey = confirms()[0].body?.idempotency_key;
  sessionExpired = true; document.dispatchEvent(new Event("visibilitychange"));
  await waitFor(() => csrfToken === "mock-csrf-1" && !button("核对原操作 / 原键重试")?.disabled, "Unknown confirmation session renewal");
  check(confirms().length === 1 && commits === 0 && sessionStorage.getItem("entropy-owner-operation-key") === originalKey && requests.filter(row => row.path.includes('/operations/by-key/')).every(row => row.path.endsWith(String(originalKey))), "未确认POST续期不自动重放，UNKNOWN与原幂等键保留，GET核对不使用被清空key");

  deferLookup = true; click("核对原操作 / 原键重试");
  await waitFor(() => heldLookup, "Old-session delayed lookup");
  sessionExpired = true; document.dispatchEvent(new Event("visibilitychange"));
  await waitFor(() => csrfToken === "mock-csrf-2" && document.querySelector('.v3-operator-controls')?.textContent?.includes("普通 · 运行中"), "Renew session before old lookup reply");
  heldLookup!(); await tick(); await tick();
  check(confirms().length === 1 && commits === 0, "旧会话延迟by-key 404在续期后返回，显式旧重试也不得重放confirm");

  await mount(); await preview("平仓"); click("确认平仓");
  await waitFor(() => dialog()?.querySelector('.v3-owner-progress'), "Known operation before expiry");
  sessionExpired = true; deferSession = true;
  await waitFor(() => heldSession && button("停止")?.disabled, "Operation poll expired session recovery");
  check(confirms().length === 1 && commits === 1 && dialog()?.querySelector('.v3-owner-progress'), "操作poll过期触发同一恢复，保留操作进度且不重发confirm");
  heldSession!(); await waitFor(() => !button("停止")?.disabled, "Operation recovered state");
  check(confirms().length === 1 && commits === 1 && maxActiveSessions === 1, "操作与state并发过期只续期一次，恢复后不重复交易命令");
  click("收起"); await waitFor(() => !dialog(), "Final mock operation collapsed");
  output.dataset.result = "passed"; output.textContent = `${checks.map(item => `✓ ${item}`).join("\n")}\n${checks.length} owner-control fixture checks passed.`;
  output.scrollTop = output.scrollHeight;
}
run().catch(error => { output.dataset.result = "failed"; output.textContent = `${checks.map(item => `✓ ${item}`).join("\n")}\nFAILED: ${error.message}`; });
