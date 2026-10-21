// Fully offline parameter UI checks; every fetch is intercepted before transport.
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import OperatorControls from "../src/v3/OperatorControls";
import { parameterFields, parameterPayload, parameterValidation, type OpeningPolicies, type OwnerParameters, type ParameterValues } from "../src/v3/ParameterDialog";
import "../src/styles.css";
import "../src/v3/FourPage.css";

const values: ParameterValues = { entry_band_bps: "12", exit_min_gross_profit_bps: "2.5", entropy_exit_room_bps: "12", lighter_exit_room_bps: "12", max_notional_usd: "200", entropy_open_room_bps: null, lighter_open_room_bps: null };
const baseOpening: OpeningPolicies = { entropy: { rooms_bps: ["3", "3", "3"], hard_cap_bps: "10" }, lighter: { rooms_bps: ["4", "8", "12"], hard_cap_bps: "12" } };
function effectiveOpening(value: ParameterValues): OpeningPolicies { return { entropy: value.entropy_open_room_bps == null ? structuredClone(baseOpening.entropy) : { rooms_bps: Array(3).fill(value.entropy_open_room_bps), hard_cap_bps: value.entropy_open_room_bps }, lighter: value.lighter_open_room_bps == null ? structuredClone(baseOpening.lighter) : { rooms_bps: Array(3).fill(value.lighter_open_room_bps), hard_cap_bps: value.lighter_open_room_bps } }; }
const mockHash = (revision: number) => "abcdef0123456789"[revision % 16].repeat(64);
const parameters = (): OwnerParameters => ({ base_config_hash: mockHash(15), current: { revision: 0, config_hash: mockHash(0), values: { ...values } }, saved: null, can_save: true, reason_code: null, limits: { entry_band_bps: "100", exit_min_gross_profit_bps: "100", entropy_exit_room_bps: "12", lighter_exit_room_bps: "12", max_notional_usd: "200", entropy_open_room_bps: "10", lighter_open_room_bps: "12" }, capabilities: { opening_concessions: true }, opening_policies: { base: structuredClone(baseOpening), current: effectiveOpening(values), saved: null } });
const initial = () => ({ owner_instance_id: "mock-owner", intent: "STOPPED" as string, intent_revision: 0, allowed_actions: ["start", "stop", "close"], risk_locks: ["MOCK_RISK_LOCK"], active_cycle_id: null as string | null, pair_identity: "MOCK:Entropy:Lighter", account_binding: "mock-account", batch_id: "mock-original-batch", config_hash: mockHash(0), entropy_units: 0, hedge_units: 0, quantity_step: "0.0001", position_observed_ts: Date.now()/1000, position_authoritative: true, pending_orders: [] as unknown[], parameters: parameters() as OwnerParameters | undefined });
type RequestRow = { path: string; method: string; body: Record<string, unknown> | null; csrf: string | null };
let owner = initial(), enabled = true, expired = false, csrf = "mock-csrf", generation = 0;
let mode: "normal" | "hold" | "lost-after" | "lost-before" | "partial" = "normal";
let held: (() => void) | null = null, lookupHeld: (() => void) | null = null, deferLookup = false;
let stateHeld: (() => void) | null = null, deferState = false, activate = false;
let saves = 0, confirmations = 0, savedByKey = new Map<string, unknown>();
const requests: RequestRow[] = [], checks: string[] = [], previews = new Map<string, unknown>();
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const refuse = (code: string, status = 409) => json({ detail: { code, message: `mock:${code}` } }, status);
window.fetch = (async (url: string | URL | globalThis.Request, options?: RequestInit) => {
  const parsed = new URL(String(url), location.href), path = parsed.pathname.replace("/api/v3-control/", ""), method = options?.method || "GET";
  const body = options?.body ? JSON.parse(String(options.body)) as Record<string, unknown> : null;
  const token = new Headers(options?.headers).get("X-Entropy-CSRF");
  requests.push({ path, method, body, csrf: token });
  if (parsed.origin !== location.origin || !parsed.pathname.startsWith("/api/v3-control/")) throw new Error("UNEXPECTED NETWORK BLOCKED");
  if (path !== "session" && expired) return refuse("session_expired", 403);
  if (method === "POST" && token !== csrf) return refuse("csrf_failed", 403);
  if (method === "GET" && ["session", "state"].includes(path)) {
    if (path === "session" && expired) { expired = false; csrf = "mock-csrf-renewed"; }
    const response = json({ enabled, reason: enabled ? null : "mock:owner_unavailable", reason_code: enabled ? null : "owner_unavailable", csrf_token: path === "session" ? csrf : undefined, state: enabled ? structuredClone(owner) : null });
    if (path === "state" && deferState) return new Promise<Response>(resolve => { deferState = false; stateHeld = () => { stateHeld = null; resolve(response); }; });
    return response;
  }
  if (path === "parameters/save" && method === "POST") {
    if (!enabled) return refuse("owner_unavailable", 503);
    if (!owner.parameters?.can_save) return refuse("parameters_require_stopped_flat");
    if (body!.expected_revision !== (owner.parameters.saved || owner.parameters.current).revision) return refuse("stale_parameters_revision");
    if (mode === "lost-before") { mode = "normal"; throw new TypeError("mock:save_response_lost_before"); }
    const key = String(body!.idempotency_key);
    let result = savedByKey.get(key);
    if (!result) {
      saves++; owner.parameters.saved = { revision: (owner.parameters.saved || owner.parameters.current).revision + 1, config_hash: mockHash(saves), values: body!.values as ParameterValues };
      if (owner.parameters.opening_policies) owner.parameters.opening_policies.saved = effectiveOpening(owner.parameters.saved.values);
      result = { parameters: structuredClone(owner.parameters), state: structuredClone(owner), save_receipt: { revision: owner.parameters.saved.revision, config_hash: owner.parameters.saved.config_hash } }; savedByKey.set(key, result);
    }
    if (mode === "lost-after" || mode === "partial") { const partial = mode === "partial"; mode = "normal"; if (partial) return refuse("outcome_unknown", 503); throw new TypeError("mock:save_response_lost_after"); }
    if (mode === "hold") return new Promise<Response>(resolve => { held = () => { held = null; mode = "normal"; resolve(json(result)); }; });
    return json(result);
  }
  if (path.startsWith("parameters/saves/by-key/") && method === "GET") {
    const result = savedByKey.get(path.split("/").at(-1)!);
    const response = result ? json({ ...(result as object), parameters: structuredClone(owner.parameters), state: structuredClone(owner) }) : refuse("parameter_save_not_found", 409);
    if (deferLookup) return new Promise<Response>(resolve => { lookupHeld = () => { deferLookup = false; lookupHeld = null; resolve(response); }; });
    return response;
  }
  if (path === "preview" && method === "POST") {
    const target = owner.parameters?.saved || owner.parameters?.current;
    if (body!.action === "start" && (body!.parameters_revision !== target?.revision || body!.parameters_hash !== target?.config_hash)) return refuse("stale_parameters_revision");
    const preview = { preview_id: crypto.randomUUID(), action: body!.action, expires_at: Date.now()/1000 + 20, state: structuredClone(owner), expected_cycle_id: null };
    previews.set(preview.preview_id, preview); return json(preview);
  }
  if (path === "confirm" && method === "POST") {
    confirmations++;
    if (!activate) return refuse("mock_no_trading_confirmation");
    owner.parameters!.current = owner.parameters!.saved!; owner.parameters!.saved = null; owner.parameters!.can_save = false;
    if (owner.parameters!.opening_policies) { owner.parameters!.opening_policies.current = owner.parameters!.opening_policies.saved!; owner.parameters!.opening_policies.saved = null; }
    owner.config_hash = owner.parameters!.current.config_hash; owner.intent = "RUNNING"; owner.intent_revision++;
    return json({ operation_id: "mock-activation", operation: { operation_id: "mock-activation", action: "start", state: "completed", evidence: {} }, state: structuredClone(owner) });
  }
  if (path === "operations/mock-activation" && method === "GET") return json({ operation_id: "mock-activation", operation: { operation_id: "mock-activation", action: "start", state: "completed", evidence: {} }, state: structuredClone(owner) });
  throw new Error(`UNEXPECTED REQUEST BLOCKED: ${method} ${path}`);
}) as typeof fetch;
const nativeInterval = window.setInterval.bind(window), nativeTimeout = window.setTimeout.bind(window);
window.setInterval = ((handler: TimerHandler, timeout?: number, ...args: unknown[]) => nativeInterval(handler, timeout === 5000 ? 60 : timeout, ...args)) as typeof window.setInterval;
window.setTimeout = ((handler: TimerHandler, timeout?: number, ...args: unknown[]) => nativeTimeout(handler, timeout && [1000, 2000, 5000, 10000, 20000].includes(timeout) ? timeout / 50 : timeout, ...args)) as typeof window.setTimeout;
const root = createRoot(document.getElementById("test-root")!), output = document.querySelector<HTMLPreElement>("#results")!;
const qa = { checks, requests, checkpoint: "", get saves() { return saves; }, get confirmations() { return confirmations; }, continue: () => { qa.checkpoint = ""; resume?.(); resume = null; } };
(window as unknown as { parameterQA: typeof qa }).parameterQA = qa;
let resume: (() => void) | null = null;
const pause = () => new Promise(resolve => setTimeout(resolve, 20));
async function waitFor(test: () => unknown, description: string) { const start = Date.now(); while (!test()) { if (Date.now() - start > 7000) throw new Error(description); await pause(); } }
function check(value: unknown, description: string) { if (!value) throw new Error(description); checks.push(description); output.textContent = checks.map(item => `✓ ${item}`).join("\n"); }
const button = (text: string) => [...document.querySelectorAll<HTMLButtonElement>("button")].find(node => node.textContent === text);
function click(text: string) { const node = button(text); if (!node) throw new Error(`Missing button ${text}`); node.focus(); node.click(); }
const dialog = () => document.querySelector<HTMLDialogElement>(".v3-parameter-dialog");
const postSaves = () => requests.filter(row => row.path === "parameters/save");
function setValue(key: keyof ParameterValues, value: string) { const input = document.querySelector<HTMLInputElement>(`input[name=${key}]`)!; Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, value); input.dispatchEvent(new Event("input", { bubbles: true })); }
function render(source: "basis" | "funding" = "basis") { root.render(<StrictMode><div key={generation} data-generation={generation} className="v3-app v3-four-page" style={{ display: "block", minHeight: 500, padding: 24 }}><h1>参数配置 · 完全离线 mock</h1><OperatorControls source={source} /></div></StrictMode>); }
async function mount(available = true, oldOwner = false, recoverKey = "", legacyFive = false) {
  owner = initial(); if (oldOwner) owner.parameters = undefined;
  if (legacyFive && owner.parameters) { delete owner.parameters.capabilities; delete owner.parameters.opening_policies; delete owner.parameters.current.values.entropy_open_room_bps; delete owner.parameters.current.values.lighter_open_room_bps; delete owner.parameters.limits.entropy_open_room_bps; delete owner.parameters.limits.lighter_open_room_bps; }
  enabled = available; expired = false; csrf = "mock-csrf"; mode = "normal"; held = lookupHeld = stateHeld = null; deferLookup = deferState = activate = false;
  saves = confirmations = 0; requests.length = 0; savedByKey.clear(); previews.clear(); sessionStorage.clear(); if (recoverKey) sessionStorage.setItem("entropy-owner-parameter-key", recoverKey);
  generation++; render();
  await waitFor(() => document.querySelector<HTMLElement>("[data-generation]")?.dataset.generation === String(generation) && (available ? requests.some(row => row.path === "state") && !button("停止")?.disabled : requests.some(row => row.path === "session") && button("启动")?.disabled) && button("参数"), "mount owner");
}
async function open() { click("参数"); await waitFor(() => dialog()?.open, "parameter dialog"); }
async function edit(key: keyof ParameterValues, value: string) { setValue(key, value); await pause(); }
async function checkpoint(name: string) { if (!new URLSearchParams(location.search).has("capture")) return; qa.checkpoint = name; await new Promise<void>(resolve => { resume = resolve; }); }
async function run() {
  const zeroOpeningLimits = { ...parameters().limits, entropy_open_room_bps: "0", lighter_open_room_bps: "0" };
  check(parameterValidation({ ...values, entropy_open_room_bps: "0", lighter_open_room_bps: "0" }, zeroOpeningLimits, true) === "", "纯函数：原开仓上限0允许显式0，与0–0范围一致");
  check(["entropy_open_room_bps", "lighter_open_room_bps"].every(key => Boolean(parameterValidation({ ...values, [key]: "1" }, zeroOpeningLimits, true))), "纯函数：E/L原开仓上限0均拒绝显式1，不能扩大原cap");
  await mount(); await open();
  check(document.querySelectorAll(".v3-parameter-dialog").length === 1 && document.querySelectorAll(".v3-parameter-fields input").length === 7, "参数入口只打开一个七字段弹窗，不堆叠面板");
  check(dialog()?.textContent?.includes("当前生效 · revision 0") && dialog()?.textContent?.includes(mockHash(0)) && parameterFields.every(({ key }) => document.querySelector<HTMLInputElement>(`input[name=${key}]`)?.value === (values[key] ?? "")), "七项预填owner当前值及revision/hash，开仓门槛12与开仓让价继承分开");
  check(postSaves().length === 0 && confirmations === 0, "打开参数无保存、启动或交易副作用");
  check(dialog()?.textContent?.includes("Entropy 3 / 3 / 3 bp，上限 10 bp") && dialog()?.textContent?.includes("Lighter 4 / 8 / 12 bp，上限 12 bp") && dialog()?.textContent?.includes("信号门槛") && dialog()?.textContent?.includes("不保证成交或盈利"), "Null继承完整原开仓阶梯及cap，且信号门槛与允许让价明确区分");
  check(dialog()?.textContent?.includes("留空保留原重试阶梯及上限") && dialog()?.textContent?.includes("各次重试的让价和价格保护上限固定为该值") && ["entropy_open_room_bps", "lighter_open_room_bps"].every(key => document.querySelector<HTMLInputElement>(`input[name=${key}]`)?.value === ""), "说明覆盖全部重试且默认空白，不把原E3/L4首档预填成固定覆盖");
  await checkpoint("desktop-current");
  for (const [key, cap] of [["entropy_open_room_bps", 10], ["lighter_open_room_bps", 12]] as const) {
    for (const invalid of ["NaN", "Infinity", "-1", String(cap + 1), "1.000000001"]) {
      await edit(key, invalid);
      check(button("保存待启用")?.disabled && postSaves().length === 0, `${key}拒绝非有限/负值/超原cap/超8小数: ${invalid}`);
    }
    await edit(key, "0"); check(!button("保存待启用")?.disabled && button("启动")?.disabled, `${key}显式0有效并作为未保存草稿阻止启动`);
    await edit(key, "  "); check(button("保存待启用")?.disabled && !dialog()?.textContent?.includes("草稿未保存"), `${key}空白等同Null继承，不产生伪草稿`);
    await edit(key, "");
  }
  for (const { key } of parameterFields.filter(field => !field.opening)) {
    for (const invalid of ["", "NaN", "Infinity", "-1", "0", "201", "1.000000001"]) {
      await edit(key, invalid);
      check(button("保存待启用")?.disabled && postSaves().length === 0, `${key}拒绝空值/非有限/负值/零/越界: ${invalid || "empty"}`);
    }
    await edit(key, values[key]!);
  }
  for (const key of ["entropy_exit_room_bps", "lighter_exit_room_bps"] as const) {
    await edit(key, "13"); check(button("保存待启用")?.disabled && postSaves().length === 0, `${key}真实12bp上限拒绝13bp`); await edit(key, "12");
  }
  await edit("entry_band_bps", "6"); click("保留草稿并收起"); await waitFor(() => !dialog(), "keep draft close");
  check(button("启动")?.disabled && document.querySelector(".v3-operator-controls")?.textContent?.includes("草稿未保存"), "收起保留草稿，启动继续禁用且解释可见");
  await open(); check(document.querySelector<HTMLInputElement>("input[name=entry_band_bps]")?.value === "6", "重开保留原草稿");
  click("放弃草稿"); await pause(); check(!button("启动")?.disabled && document.querySelector<HTMLInputElement>("input[name=entry_band_bps]")?.value === "12", "显式放弃草稿恢复owner值及启动入口");
  await edit("entry_band_bps", "6"); mode = "hold"; click("保存待启用"); click("保存待启用"); await waitFor(() => held, "save held");
  check(postSaves().length === 1 && saves === 1 && Boolean(postSaves()[0].body?.idempotency_key), "保存双点击只有一次POST及稳定幂等键");
  held!(); await waitFor(() => dialog()?.textContent?.includes("已保存待启用 · revision 1"), "saved pending state");
  check(dialog()?.textContent?.includes("当前生效 · revision 0") && owner.config_hash === mockHash(0) && owner.intent === "STOPPED" && confirmations === 0 && owner.risk_locks[0] === "MOCK_RISK_LOCK" && owner.batch_id === "mock-original-batch", "保存仅待启用，当前hash/停止意图/风险锁/批次保持原值");
  await checkpoint("desktop-saved");
  click("收起"); await waitFor(() => !dialog(), "saved collapse"); click("启动");
  await waitFor(() => button("确认启动"), "start preview");
  const previewRequest = requests.find(row => row.path === "preview");
  check(previewRequest?.body?.parameters_revision === 1 && previewRequest.body.parameters_hash === mockHash(1) && document.querySelector(".v3-owner-dialog")?.textContent?.includes("启动将启用已保存参数") && document.querySelector(".v3-owner-dialog")?.textContent?.includes("开仓偏离 6 bp"), "启动预检绑定保存revision/hash并展示五值与当前生效差异");
  await checkpoint("desktop-start-preview");
  owner.parameters!.saved = { ...owner.parameters!.saved!, revision: 2, config_hash: mockHash(2) };
  await waitFor(() => button("重新预检") && !button("确认启动"), "changed parameter revision invalidates preview");
  check(confirmations === 0, "其他保存revision/hash变化使启动预检失效，无confirm");
  await mount(); await open(); await edit("entry_band_bps", "6"); owner.parameters!.saved = { revision: 1, config_hash: mockHash(3), values: { ...values } }; await pause(); click("保存待启用");
  await waitFor(() => dialog()?.textContent?.includes("stale_parameters_revision"), "stale save refusal");
  check(saves === 0 && postSaves()[0].body?.expected_revision === 0 && confirmations === 0, "草稿绑定原revision，旧revision拒绝保存且不启用");
  for (const reason of ["owner_not_stopped", "position_not_flat", "position_not_authoritative", "pending_orders"]) {
    await mount(); owner.parameters!.can_save = false; owner.parameters!.reason_code = reason;
    owner.intent = reason === "owner_not_stopped" ? "RUNNING" : "STOPPED"; owner.entropy_units = reason === "position_not_flat" ? 12 : 0; owner.position_authoritative = reason !== "position_not_authoritative"; owner.pending_orders = reason === "pending_orders" ? [{}] : [];
    document.dispatchEvent(new Event("visibilitychange")); await pause(); await open(); await edit("entry_band_bps", "6");
    check(button("保存待启用")?.disabled && button("启动")?.disabled && postSaves().length === 0, `${reason}禁保存及启动，有持仓或pending时无参数副作用`);
  }
  for (const loss of ["lost-after", "partial"] as const) {
    await mount(); await open(); await edit("entry_band_bps", "6"); mode = loss; click("保存待启用");
    await waitFor(() => dialog()?.textContent?.includes("已保存待启用 · revision 1"), "save by-key restore");
    check(postSaves().length === 1 && saves === 1 && requests.some(row => row.path.startsWith("parameters/saves/by-key/")) && confirmations === 0, `${loss}只原键GET核对保存结果，不重POST或启动`);
  }
  await mount(); await open(); await edit("entry_band_bps", "6"); mode = "lost-before"; click("保存待启用");
  await waitFor(() => button("核对原保存") && !button("核对原保存")!.disabled, "unknown saving");
  const originalKey = postSaves()[0].body!.idempotency_key;
  expired = true; document.dispatchEvent(new Event("visibilitychange"));
  await waitFor(() => csrf === "mock-csrf-renewed" && !button("核对原保存")!.disabled, "session recovery");
  check(postSaves().length === 1 && saves === 0 && sessionStorage.getItem("entropy-owner-parameter-key") === originalKey && button("启动")?.disabled, "保存UNKNOWN会话GET恢复保留原键及草稿，不自动重放POST，启动禁用");
  click("核对原保存"); await waitFor(() => !button("核对原保存"), "save absent confirmed");
  check(postSaves().length === 1 && saves === 0 && dialog()?.textContent?.includes("原保存未被接受"), "原键未找到核对后需用户重新显式保存，零自动重试");
  await mount(); await open(); await edit("entry_band_bps", "6"); deferState = true; document.dispatchEvent(new Event("visibilitychange")); await waitFor(() => stateHeld, "old state held before save");
  click("保存待启用"); await waitFor(() => dialog()?.textContent?.includes("已保存待启用 · revision 1"), "save accepted ahead of GET");
  stateHeld!(); await pause(); await pause();
  check(dialog()?.textContent?.includes("已保存待启用 · revision 1") && !dialog()?.textContent?.includes("草稿未保存"), "旧stateGET晚于保存rev1回包时不回退saved版本或产生伪草稿");
  click("收起"); await waitFor(() => !dialog(), "race save collapsed"); click("启动"); await waitFor(() => button("确认启动"), "race preview");
  check(requests.filter(row => row.path === "preview").at(-1)?.body?.parameters_revision === 1, "旧GET晚到后启动仍绑定已保存rev1");
  deferState = true; document.dispatchEvent(new Event("visibilitychange")); await waitFor(() => stateHeld, "old state held before activate"); activate = true; click("确认启动");
  await waitFor(() => document.querySelector(".v3-operator-controls")?.textContent?.includes("普通 · 运行中"), "mock activation response accepted");
  stateHeld!(); await pause(); await pause();
  check(document.querySelector(".v3-operator-controls")?.textContent?.includes("普通 · 运行中") && button("启动")?.disabled, "旧GET晚于mock启用完成回包时不回退intent/current版本或恢复启动");
  click("收起"); await waitFor(() => !document.querySelector(".v3-owner-dialog"), "mock activation collapse"); await open();
  check(dialog()?.textContent?.includes("当前生效 · revision 1") && !dialog()?.textContent?.includes("已保存待启用"), "mock启用后当前rev1及hash保持，旧GET不能恢复旧current0/saved1");
  await mount(); await open(); await edit("entry_band_bps", "6"); mode = "lost-before"; click("保存待启用"); await waitFor(() => button("核对原保存") && !button("核对原保存")!.disabled, "receipt unknown before lookup");
  const receiptKey = String(postSaves()[0].body!.idempotency_key);
  savedByKey.set(receiptKey, { save_receipt: { revision: 1, config_hash: mockHash(1) } });
  owner.parameters!.saved = { revision: 2, config_hash: mockHash(2), values: { ...values, entry_band_bps: "7" } };
  click("核对原保存"); await waitFor(() => dialog()?.textContent?.includes("原保存 revision 1 已接受"), "original save receipt newer version");
  check(dialog()?.textContent?.includes("已保存待启用 · revision 2") && document.querySelector<HTMLInputElement>("input[name=entry_band_bps]")?.value === "7", "原保存回执与当前latest不同明确提示，显示真实latest而非误称自己保存值");
  await mount(); await open(); await edit("entry_band_bps", "6"); mode = "lost-before"; deferLookup = true; click("保存待启用"); await waitFor(() => lookupHeld, "late save lookup");
  render("funding"); await waitFor(() => !dialog() && document.querySelector(".v3-operator-controls")?.textContent?.includes("资金费控制未接入"), "funding source");
  lookupHeld!(); await pause();
  check(!button("参数") && button("启动")?.disabled && postSaves().length === 1 && saves === 0, "切换funding清参数入口，迟到basis lookup不能跨来源恢复控制");
  await mount(true, false, "mock-recover-key"); await open();
  check(button("核对原保存") && button("启动")?.disabled && postSaves().length === 0, "重挂载保留UNKNOWN保存键，仅核对不重发");
  await mount(false); await open();
  check(dialog()?.textContent?.includes("owner 连接不可用") && button("保存待启用")?.disabled && button("启动")?.disabled && postSaves().length === 0, "owner离线明确不可保存或启动");
  check(dialog()?.textContent?.includes("owner 未连接，等待用户手动启动") && !dialog()?.textContent?.includes("需升级 owner"), "owner离线缺parameters明确等待手动启动，不误报需升级");
  await waitFor(() => document.querySelector(".v3-operator-controls > span")?.textContent?.includes("owner 未运行，等待手动启动"), "owner unavailable pending manual start");
  check([button("启动"), button("停止"), button("平仓")].every(node => node?.disabled) && postSaves().length === 0 && confirmations === 0, "普通owner_unavailable明确待手动启动，三动作全禁用且零交易POST");
  await checkpoint("desktop-owner-offline");
  await mount(true, true); await open();
  check(dialog()?.textContent?.includes("需升级 owner") && button("启动")?.disabled && !button("停止")?.disabled, "旧owner参数缺失明确迁移，禁新启动但保留停止控制");
  await mount(true, false, "", true); await open();
  check(["entropy_open_room_bps", "lighter_open_room_bps"].every(key => document.querySelector<HTMLInputElement>(`input[name=${key}]`)?.disabled) && dialog()?.textContent?.includes("继承原策略 · 当前 owner 不支持调整") && !dialog()?.textContent?.includes("3 / 3 / 3") && !dialog()?.textContent?.includes("4 / 8 / 12"), "旧五项capability仅显示继承，新开仓输入禁用且不猜原阶梯");
  await edit("entry_band_bps", "6"); click("保存待启用"); await waitFor(() => dialog()?.textContent?.includes("已保存待启用 · revision 1"), "legacy five field save");
  const legacyValues = postSaves()[0].body?.values as ParameterValues;
  check(Object.keys(legacyValues).length === 5 && !Object.hasOwn(legacyValues, "entropy_open_room_bps") && !Object.hasOwn(legacyValues, "lighter_open_room_bps"), "旧五项owner保存仍仅五项payload，不加Null或扩权限");
  await checkpoint("desktop-legacy-five");
  await mount(); await open(); await edit("entropy_open_room_bps", "3"); await edit("lighter_open_room_bps", "9");
  delete owner.parameters!.capabilities; delete owner.parameters!.opening_policies; delete owner.parameters!.current.values.entropy_open_room_bps; delete owner.parameters!.current.values.lighter_open_room_bps; delete owner.parameters!.limits.entropy_open_room_bps; delete owner.parameters!.limits.lighter_open_room_bps;
  expired = true; document.dispatchEvent(new Event("visibilitychange")); await waitFor(() => csrf === "mock-csrf-renewed" && dialog()?.textContent?.includes("当前 owner 不支持调整开仓让价"), "capability downgrade retains draft");
  check(button("保存待启用")?.disabled && button("启动")?.disabled && postSaves().length === 0 && document.querySelector<HTMLInputElement>("input[name=entropy_open_room_bps]")?.value === "3" && document.querySelector<HTMLInputElement>("input[name=lighter_open_room_bps]")?.value === "9", "能力降级保留3/9草稿并拒绝保存/启动，不静默删除新字段或POST五项");
  let refusedPayload = false; try { parameterPayload({ ...owner.parameters!.current.values, entropy_open_room_bps: "0" }, owner.parameters!); } catch { refusedPayload = true; }
  check(refusedPayload, "unsupported payload边界防御拒绝显式0，不当作Null删掉");
  click("放弃草稿"); await pause(); await edit("entry_band_bps", "6"); click("保存待启用"); await waitFor(() => dialog()?.textContent?.includes("已保存待启用 · revision 1"), "explicit discard after downgrade");
  check(Object.keys(postSaves()[0].body!.values as ParameterValues).length === 5, "用户显式放弃降级草稿后，旧owner五项保存才恢复");
  await mount(); await open(); await edit("entropy_open_room_bps", "0"); await edit("lighter_open_room_bps", "12"); click("保存待启用");
  await waitFor(() => dialog()?.textContent?.includes("已保存待启用 · revision 1"), "opening overrides save");
  const customValues = postSaves()[0].body?.values as ParameterValues;
  check(Object.keys(customValues).length === 7 && customValues.entropy_open_room_bps === "0" && customValues.lighter_open_room_bps === "12" && customValues.entry_band_bps === "12", "七项保存精确保留0与显式12，开仓信号门槛仍12");
  check(dialog()?.textContent?.includes("Entropy 0 / 0 / 0 bp，上限 0 bp") && dialog()?.textContent?.includes("Lighter 12 / 12 / 12 bp，上限 12 bp") && owner.parameters!.current.values.entropy_open_room_bps === null && confirmations === 0, "显式X待启用每轮room和cap同X，当前Null基策略仍未变且不启动");
  await checkpoint("desktop-custom-opening-saved");
  click("收起"); await waitFor(() => !dialog(), "custom opening saved collapsed"); click("启动"); await waitFor(() => button("确认启动"), "custom opening start preview");
  check(document.querySelector(".v3-owner-dialog")?.textContent?.includes("Entropy 开仓让价 0 bp") && document.querySelector(".v3-owner-dialog")?.textContent?.includes("Lighter 开仓让价 12 bp") && document.querySelector(".v3-owner-dialog")?.textContent?.includes("Entropy 0 / 0 / 0 bp，上限 0 bp"), "启动确认同时展示七值和真实待启用开仓策略，0不被当Null");
  await checkpoint("desktop-custom-opening-preview");
  activate = true; click("确认启动"); await waitFor(() => owner.intent === "RUNNING" && document.querySelector(".v3-owner-progress") && !button("收起")?.disabled, "custom opening mock activation");
  check(owner.parameters!.current.values.entropy_open_room_bps === "0" && owner.parameters!.current.values.lighter_open_room_bps === "12" && owner.parameters!.opening_policies!.current.entropy.hard_cap_bps === "0" && owner.risk_locks[0] === "MOCK_RISK_LOCK" && owner.batch_id === "mock-original-batch", "内存mock启动后七项当前生效及0cap正确，风险锁与批次保留");
  click("收起"); await waitFor(() => !document.querySelector(".v3-owner-dialog"), "custom opening activation collapsed");
  owner.intent = "STOPPED"; owner.intent_revision++; owner.parameters!.can_save = true; document.dispatchEvent(new Event("visibilitychange")); await waitFor(() => !button("启动")?.disabled, "mock stopped boundary restored"); await open();
  await edit("entropy_open_room_bps", ""); await edit("lighter_open_room_bps", " "); click("保存待启用"); await waitFor(() => dialog()?.textContent?.includes("已保存待启用 · revision 2"), "clear opening overrides save");
  const inheritedValues = postSaves().at(-1)!.body?.values as ParameterValues;
  check(inheritedValues.entropy_open_room_bps === null && inheritedValues.lighter_open_room_bps === null && owner.parameters!.opening_policies!.saved!.entropy.hard_cap_bps === "10" && owner.parameters!.opening_policies!.saved!.lighter.rooms_bps.join(",") === "4,8,12", "清空显式覆盖保存JSON Null，恢复完整base策略而非0或扁平阶梯");
  await mount(); await open(); await edit("entry_band_bps", "6"); await checkpoint("mobile-draft");
  check(requests.every(row => row.method !== "POST" || row.csrf === csrf) && confirmations === 0, "全部请求完全mock，POST沿用会话CSRF，未触发任何实盘confirm");
  output.dataset.result = "passed"; output.textContent = `${checks.map(item => `✓ ${item}`).join("\n")}\n${checks.length} owner-parameter fixture checks passed.`;
}
run().catch(error => { output.dataset.result = "failed"; output.textContent = `${checks.map(item => `✓ ${item}`).join("\n")}\nFAILED: ${error.message}`; });
