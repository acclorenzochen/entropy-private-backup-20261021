import { useEffect, useRef, useState } from "react";
import { time } from "./format";
import ParameterDialog, { ParameterFacts, openingConcessionsSupported, parameterIdentity, parameterPayload, parameterTarget, parameterValidation, sameParameterValues, type OwnerParameters, type ParameterRevision, type ParameterValues } from "./ParameterDialog";

type Action = "start" | "stop" | "close";
interface Order { order_ref?: string; state?: string; filled_units?: number | null; reduce_only?: boolean }
interface OwnerState {
  owner_instance_id: string; intent: "RUNNING" | "STOPPING" | "STOPPED"; intent_revision: number;
  allowed_actions: Action[]; risk_locks: string[]; active_cycle_id: string | null; pair_identity: string;
  account_binding: string; batch_id: string; config_hash: string; entropy_units: number | null;
  hedge_units: number | null; quantity_step: string | null; position_observed_ts: number | null;
  position_authoritative: boolean; pending_orders: Order[];
  parameters?: OwnerParameters;
}
interface Capability { enabled: boolean; reason?: string; reason_code?: string; csrf_token?: string; state: OwnerState | null }
interface Preview { preview_id: string; action: Action; expires_at: number; state: OwnerState; expected_cycle_id: string | null }
interface Operation {
  operation_id: string; action: Action; state: "accepted" | "executing" | "flat_pending_accounting" | "completed" | "blocked";
  evidence: { accounting_final?: boolean; net_pnl_usd?: string | null; known_fees_usd?: string | null; fees_complete?: boolean;
    legs?: { venue: string; initial_units?: number | null; remaining_units?: number | null; orders: Order[] }[] };
}
interface OperationResult { operation: Operation; state: OwnerState; operation_id: string }
interface ParameterResult { parameters: OwnerParameters; state: OwnerState; save_receipt?: { revision: number; config_hash: string } }
class ControlError extends Error {
  constructor(message: string, readonly code: string, readonly status = 0) { super(message); }
}
const labels: Record<Action, string> = { start: "启动", stop: "停止", close: "平仓" };
const intents = { RUNNING: "运行中", STOPPING: "停止开仓 · 管理存量", STOPPED: "已停止开仓" };
const operationLabels = { accepted: "已接受 · 等待管理", executing: "执行中", flat_pending_accounting: "双腿归零 · 完成待核算", completed: "完成", blocked: "受阻 · 请核对" };
const configured = import.meta.env.VITE_V3_OWNER_CONTROL === "1";

async function control<T>(path: string, body?: object, csrf?: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`/api/v3-control/${path}`, {
    method: body ? "POST" : "GET", credentials: "same-origin", cache: "no-store", signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(8000)]) : AbortSignal.timeout(8000),
    headers: body ? { "Content-Type": "application/json", "X-Entropy-CSRF": csrf || "" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await response.json().catch(() => null);
  if (!response.ok || !data) throw new ControlError(data?.detail?.message || "本地控制连接中断，请核对本次结果。", data?.detail?.code || "response_unavailable", response.status);
  return data as T;
}
function temporary(failure: unknown) {
  return !(failure instanceof ControlError) || ["session_expired", "owner_unavailable", "invalid_owner_response"].includes(failure.code)
    || (failure.code === "response_unavailable" && failure.status >= 500);
}
function units(value: number | null | undefined, step: string | null | undefined) {
  if (value == null || !Number.isSafeInteger(value)) return "未知";
  return `${value} units${step && Number.isFinite(Number(step)) ? ` / ${(value * Number(step)).toFixed(8).replace(/(\.\d*?)0+$/, "$1").replace(/\.$/, "")} 标的` : ""}`;
}
function explanation(action: Action, state: OwnerState | null) {
  if (action === "start") return "请求进入运行意图。既有 MANUAL_STOP、风险锁与授权限额继续有效；此操作不会解除任何锁。";
  if (action === "stop") return "立即停止新增 Entropy 敞口；已有持仓的对冲补齐与减仓继续。权威双腿归零且没有待处理订单后，才进入已停止状态。";
  return `仅平当前周期，两腿并发 reduce-only；保留当前运行意图。${state?.intent === "RUNNING" ? "平仓后仍可按已有策略继续开仓。" : state?.intent === "STOPPING" ? "平仓后满足停止条件才转为已停止。" : "不会自动恢复开仓。"}`;
}

export default function OperatorControls({ source = "basis" }: { source?: "basis" | "funding" }) {
  const [capability, setCapability] = useState<Capability>({ enabled: false, reason: "生产 owner 尚未接通，当前控制不可用。", state: null });
  const [preview, setPreview] = useState<Preview | null>(null);
  const [action, setAction] = useState<Action | null>(null);
  const [operation, setOperation] = useState<Operation | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [uncertain, setUncertain] = useState(false);
  const [connection, setConnection] = useState<"connecting" | "ready" | "waiting" | "exhausted" | "blocked">("connecting");
  const [reconnectAttempt, setReconnectAttempt] = useState(0);
  const [now, setNow] = useState(Date.now() / 1000);
  const [parametersOpen, setParametersOpen] = useState(false);
  const [parameterDraft, setParameterDraft] = useState<ParameterValues | null>(null);
  const [parameterBase, setParameterBase] = useState<ParameterRevision | null>(null);
  const [parameterUnknown, setParameterUnknown] = useState(false);
  const [parameterError, setParameterError] = useState("");
  const parameterKey = useRef("");
  const parameterOpener = useRef<HTMLElement | null>(null);
  const lastState = useRef<OwnerState | null>(null);
  const dialog = useRef<HTMLDialogElement>(null);
  const busyRef = useRef(false);
  const opener = useRef<HTMLElement | null>(null);
  const sourceRef = useRef(source);
  const generation = useRef(0);
  if (sourceRef.current !== source) { sourceRef.current = source; generation.current += 1; }
  const keyRef = useRef("");
  const csrf = useRef("");
  const sessionGeneration = useRef(0);
  const connectionReady = useRef(false);
  const reconnect = useRef<(manual?: boolean) => void>(() => {});
  const submitted = useRef<Preview | null>(null);
  const recoverKey = useRef("");
  const state = capability.state;
  const parameters = state?.parameters;
  const parameterDirty = Boolean(parameterDraft && !sameParameterValues(parameterDraft, parameterBase?.values));
  const startBlocked = parameterDirty || parameterUnknown || !parameters || !parameters.can_save;
  const requestBusy = (value: boolean) => { busyRef.current = value; setBusy(value); };
  const current = (epoch: number) => sourceRef.current === "basis" && generation.current === epoch;
  const acceptState = (incoming: OwnerState) => {
    const previous = lastState.current;
    if (previous?.owner_instance_id === incoming.owner_instance_id && (incoming.intent_revision < previous.intent_revision
      || (incoming.parameters?.current.revision ?? -1) < (previous.parameters?.current.revision ?? -1)
      || (parameterTarget(incoming.parameters)?.revision ?? -1) < (parameterTarget(previous.parameters)?.revision ?? -1))) return previous;
    lastState.current = incoming;
    return incoming;
  };
  const apply = (result: OperationResult, epoch: number) => {
    if (!current(epoch)) return;
    setOperation(result.operation); const accepted = acceptState(result.state); setCapability(previous => ({ ...previous, enabled: connectionReady.current && Boolean(csrf.current), state: accepted }));
    setUncertain(false); setError("");
    sessionStorage.setItem("entropy-owner-operation-key", recoverKey.current);
  };
  const reconnectAfter = (failure: unknown, epoch: number) => {
    if (current(epoch) && (temporary(failure) || (failure instanceof ControlError && failure.code === "response_unavailable"))) reconnect.current();
  };
  useEffect(() => {
    generation.current += 1;
    lastState.current = null;
    sessionGeneration.current += 1; csrf.current = ""; connectionReady.current = false;
    keyRef.current = ""; submitted.current = null;
    setAction(null); setOperation(null); setPreview(null); requestBusy(false);
    setParametersOpen(false); setParameterDraft(null); setParameterBase(null); setParameterError("");
    parameterKey.current = source === "basis" ? sessionStorage.getItem("entropy-owner-parameter-key") || "" : "";
    setParameterUnknown(Boolean(parameterKey.current));
    if (source !== "basis") { setCapability({ enabled:false, state:null, reason:"资金费部署尚未接入 owner 控制，不能操作其他标的。" }); return; }
    if (!configured) return;
    let disposed = false;
    const epoch = generation.current;
    const controller = new AbortController();
    let inFlight = false, recovering = false, stopped = false, retries = 0;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    const delays = [1000, 2000, 5000, 10000, 20000];
    const valid = (ticket: number) => !disposed && current(epoch) && ticket === sessionGeneration.current;
    const invalidate = () => {
      sessionGeneration.current += 1; csrf.current = ""; connectionReady.current = false; keyRef.current = "";
      setPreview(null); setCapability(previous => ({ ...previous, enabled: false, state: null }));
    };
    const schedule = () => {
      if (disposed || stopped || inFlight || retryTimer) return;
      if (retries >= delays.length) { stopped = true; recovering = false; setConnection("exhausted"); return; }
      setConnection("waiting"); setReconnectAttempt(retries + 1);
      retryTimer = setTimeout(() => { retryTimer = undefined; void read(true); }, delays[retries++]);
    };
    const beginRecovery = (manual = false) => {
      if (disposed || !current(epoch) || recovering || (stopped && !manual)) return;
      recovering = true; stopped = false; retries = 0;
      invalidate();
      if (manual && !inFlight) void read(true); else schedule();
    };
    reconnect.current = beginRecovery;
    const read = async (renew = false) => {
      if (disposed || stopped || inFlight) return;
      inFlight = true;
      const ticket = sessionGeneration.current;
      try {
        if (renew) {
          setConnection("connecting");
          const session = await control<Capability>("session?source=basis", undefined, undefined, controller.signal);
          if (!valid(ticket)) return;
          if (!session.enabled) throw new ControlError(session.reason || "owner 不可用", session.reason_code || "control_not_configured", 503);
          if (!session.csrf_token) throw new ControlError("本地确认会话不可用。", "invalid_owner_response", 503);
          csrf.current = session.csrf_token;
        }
        const value = await control<Capability>("state?source=basis", undefined, undefined, controller.signal);
        if (!valid(ticket)) return;
        if (!value.enabled) throw new ControlError(value.reason || "owner 不可用", value.reason_code || "control_not_configured", 503);
        connectionReady.current = true; recovering = false; retries = 0;
        setConnection("ready"); setReconnectAttempt(0);
        setCapability({ ...value, state: value.state ? acceptState(value.state) : null });
      } catch (failure) {
        if (!valid(ticket)) return;
        invalidate();
        setCapability({ enabled: false, reason: failure instanceof Error ? failure.message : "owner 不可用", reason_code: failure instanceof ControlError ? failure.code : "response_unavailable", state: null });
        if (temporary(failure)) { recovering = true; }
        else { stopped = true; recovering = false; setConnection("blocked"); }
      } finally {
        inFlight = false;
        if (!disposed && current(epoch) && recovering) schedule();
      }
    };
    void read(true);
    const refresh = () => { if (!document.hidden && !recovering && !stopped) void read(!csrf.current); };
    const timer = setInterval(refresh, 5000);
    document.addEventListener("visibilitychange", refresh);
    recoverKey.current = sessionStorage.getItem("entropy-owner-operation-key") || "";
    return () => { disposed = true; controller.abort(); clearInterval(timer); clearTimeout(retryTimer); document.removeEventListener("visibilitychange", refresh); };
  }, [source]);
  useEffect(() => {
    if (!action) return;
    const node = dialog.current;
    if (node && !node.open) node.showModal();
    const timer = setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => { clearInterval(timer); node?.close(); if (opener.current?.isConnected) opener.current.focus(); };
  }, [action]);
  useEffect(() => {
    if (!action || !operation) return;
    let disposed = false;
    const epoch = generation.current;
    const controller = new AbortController();
    let inFlight = false;
    const read = async () => {
      if (inFlight || !connectionReady.current) return;
      inFlight = true;
      const ticket = sessionGeneration.current;
      try {
        const value = await control<OperationResult>(`operations/${operation.operation_id}?source=basis`, undefined, undefined, controller.signal);
        if (!disposed && ticket === sessionGeneration.current) apply(value, epoch);
      } catch (failure) {
        if (!disposed && current(epoch) && ticket === sessionGeneration.current) { setError(failure instanceof Error ? failure.message : "进度未知"); setUncertain(true); reconnectAfter(failure, epoch); }
      } finally { inFlight = false; }
    };
    // Completed commands still refresh accounting; a late fee can revoke finality.
    const timer = setInterval(() => { if (!document.hidden && !busyRef.current) void read(); }, 2000);
    return () => { disposed = true; controller.abort(); clearInterval(timer); };
  }, [action, operation?.operation_id]);
  const openPreview = async (selected: Action) => {
    if (sourceRef.current !== "basis" || busyRef.current || !capability.enabled || !connectionReady.current || !csrf.current) return;
    if (selected === "start" && startBlocked) return;
    const epoch = generation.current;
    const ticket = sessionGeneration.current;
    if (!action) opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    setAction(selected); setPreview(null); setOperation(null); setError(""); setUncertain(false);
    submitted.current = null; keyRef.current = ""; requestBusy(true);
    try {
      const target = parameterTarget(parameters);
      const value = await control<Preview>("preview", { source: "basis", action: selected, ...(selected === "close" ? { expected_cycle_id: state?.active_cycle_id } : {}), ...(selected === "start" && target ? { parameters_revision: target.revision, parameters_hash: target.config_hash } : {}) }, csrf.current);
      if (!current(epoch) || ticket !== sessionGeneration.current) return;
      setPreview(value); setNow(Date.now() / 1000); keyRef.current = crypto.randomUUID();
      const accepted = acceptState(value.state);
      setCapability(previous => ({ ...previous, state: accepted }));
    } catch (failure) { if (current(epoch) && ticket === sessionGeneration.current) { setError(failure instanceof Error ? failure.message : "预检失败"); reconnectAfter(failure, epoch); } }
    finally { if (current(epoch)) requestBusy(false); }
  };
  const recover = async (replay = false) => {
    if (sourceRef.current !== "basis" || busyRef.current || !recoverKey.current || !connectionReady.current) return;
    const epoch = generation.current;
    const ticket = sessionGeneration.current;
    requestBusy(true);
    try {
      let value: OperationResult;
      try { value = await control<OperationResult>(`operations/by-key/${recoverKey.current}?source=basis`); }
      catch (failure) {
        if (!current(epoch) || ticket !== sessionGeneration.current) return;
        if (!(failure instanceof ControlError) || failure.code !== "operation_not_found" || !replay || !submitted.current || !connectionReady.current || !csrf.current) throw failure;
        value = await control<OperationResult>("confirm", { preview_id: submitted.current.preview_id, idempotency_key: recoverKey.current }, csrf.current);
      }
      if (!current(epoch) || ticket !== sessionGeneration.current) return;
      setAction(value.operation.action); apply(value, epoch);
    } catch (failure) { if (current(epoch)) { setError(failure instanceof Error ? failure.message : "结果未知"); setUncertain(true); reconnectAfter(failure, epoch); } }
    finally { if (current(epoch)) requestBusy(false); }
  };
  const confirm = async () => {
    if (sourceRef.current !== "basis" || busyRef.current || !preview || !keyRef.current || submitted.current || !connectionReady.current || !csrf.current) return;
    if (preview.action === "start" && (startBlocked || parameterIdentity(preview.state.parameters) !== parameterIdentity(parameters))) return;
    const epoch = generation.current;
    const key = keyRef.current;
    requestBusy(true); submitted.current = preview; recoverKey.current = keyRef.current;
    sessionStorage.setItem("entropy-owner-operation-key", keyRef.current);
    try { apply(await control<OperationResult>("confirm", { preview_id: preview.preview_id, idempotency_key: key }, csrf.current), epoch); }
    catch (failure) {
      if (!current(epoch)) return;
      setError(failure instanceof Error ? failure.message : "结果未知");
      const unknown = !(failure instanceof ControlError) || ["outcome_unknown", "response_unavailable", "owner_unavailable"].includes(failure.code);
      setUncertain(unknown);
      reconnectAfter(failure, epoch);
      if (unknown) {
        try { apply(await control<OperationResult>(`operations/by-key/${key}?source=basis`), epoch); } catch (lookupFailure) { reconnectAfter(lookupFailure, epoch); /* Keep the original key visible for recovery. */ }
      } else { submitted.current = null; setPreview(null); }
    } finally { if (current(epoch)) requestBusy(false); }
  };
  const previewState = preview?.state;
  const changed = Boolean(previewState && state && (previewState.owner_instance_id !== state.owner_instance_id || previewState.intent_revision !== state.intent_revision || previewState.active_cycle_id !== state.active_cycle_id || previewState.entropy_units !== state.entropy_units || previewState.hedge_units !== state.hedge_units || previewState.batch_id !== state.batch_id || previewState.config_hash !== state.config_hash || (action === "start" && (parameterIdentity(previewState.parameters) !== parameterIdentity(parameters) || !parameters?.can_save))));
  const expired = Boolean(preview && now >= preview.expires_at);
  const closeDialog = () => { if (!busyRef.current) { dialog.current?.close(); setAction(null); if (opener.current?.isConnected) opener.current.focus(); } };
  const applyParameters = (result: ParameterResult, epoch: number) => {
    if (!current(epoch)) return;
    const accepted = acceptState({ ...result.state, parameters: result.parameters });
    setCapability(previous => ({ ...previous, state: accepted }));
    const target = parameterTarget(accepted.parameters)!;
    const receipt = result.save_receipt;
    setParameterDraft({ ...target.values }); setParameterBase(target); setParameterUnknown(false);
    setParameterError(receipt && (receipt.revision !== target.revision || receipt.config_hash !== target.config_hash) ? `原保存 revision ${receipt.revision} 已接受，但待启用版本已变化，请核对当前参数后启动。` : "");
    parameterKey.current = ""; sessionStorage.removeItem("entropy-owner-parameter-key");
    setPreview(null);
  };
  const lookupParameterSave = async () => {
    if (!current(generation.current) || busyRef.current || !parameterKey.current || !connectionReady.current) return;
    const epoch = generation.current, ticket = sessionGeneration.current;
    requestBusy(true);
    try {
      const result = await control<ParameterResult>(`parameters/saves/by-key/${parameterKey.current}?source=basis`);
      if (current(epoch) && ticket === sessionGeneration.current) applyParameters(result, epoch);
    } catch (failure) {
      if (!current(epoch) || ticket !== sessionGeneration.current) return;
      if (failure instanceof ControlError && failure.code === "parameter_save_not_found") {
        setParameterUnknown(false); parameterKey.current = ""; sessionStorage.removeItem("entropy-owner-parameter-key");
        setParameterError("原保存未被接受。请核对草稿后显式保存；系统不会自动重试。");
      } else { setParameterError(failure instanceof Error ? failure.message : "保存结果未知"); reconnectAfter(failure, epoch); }
    } finally { if (current(epoch)) requestBusy(false); }
  };
  const saveParameters = async () => {
    if (!current(generation.current) || busyRef.current || !capability.enabled || !connectionReady.current || !csrf.current || !parameters?.can_save || !parameterBase || !parameterDraft || !parameterDirty || parameterUnknown || parameterValidation(parameterDraft, parameters.limits, openingConcessionsSupported(parameters))) return;
    const epoch = generation.current, ticket = sessionGeneration.current, key = crypto.randomUUID();
    parameterKey.current = key; sessionStorage.setItem("entropy-owner-parameter-key", key);
    requestBusy(true); setParameterError(""); setParameterUnknown(true); setPreview(null);
    try {
      const result = await control<ParameterResult>("parameters/save", { source: "basis", expected_revision: parameterBase.revision, values: parameterPayload(parameterDraft, parameters), idempotency_key: key }, csrf.current);
      if (current(epoch) && ticket === sessionGeneration.current) applyParameters(result, epoch);
    } catch (failure) {
      if (!current(epoch) || ticket !== sessionGeneration.current) return;
      setParameterError(failure instanceof Error ? failure.message : "保存结果未知");
      const unknown = !(failure instanceof ControlError) || ["outcome_unknown", "response_unavailable", "owner_unavailable", "invalid_owner_response", "session_expired"].includes(failure.code);
      if (!unknown) { setParameterUnknown(false); parameterKey.current = ""; sessionStorage.removeItem("entropy-owner-parameter-key"); }
      if (unknown) {
        try {
          const result = await control<ParameterResult>(`parameters/saves/by-key/${key}?source=basis`);
          if (current(epoch) && ticket === sessionGeneration.current) applyParameters(result, epoch);
        } catch (lookupFailure) { reconnectAfter(failure, epoch); reconnectAfter(lookupFailure, epoch); }
      } else reconnectAfter(failure, epoch);
    } finally { if (current(epoch)) requestBusy(false); }
  };
  const openParameters = () => {
    if (sourceRef.current !== "basis" || busyRef.current) return;
    parameterOpener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    if (!parameterDirty) { const target = parameterTarget(parameters); setParameterDraft(target ? { ...target.values } : null); setParameterBase(target || null); }
    setParametersOpen(true);
  };
  const closeParameters = () => { if (!busyRef.current) { setParametersOpen(false); if (parameterOpener.current?.isConnected) parameterOpener.current.focus(); } };
  return <div className="v3-operator-controls" aria-label="交易控制">
    <span className={capability.enabled ? "v3-good" : "v3-pending"} title={capability.reason} aria-live="polite">{capability.enabled && state ? `普通 · ${intents[state.intent]}` : source === "funding" ? "资金费控制未接入" : capability.reason_code === "owner_unavailable" ? "普通 · owner 未运行，等待手动启动" : configured && (connection === "connecting" || connection === "waiting") ? `普通控制不可用 · 重新连接中${reconnectAttempt ? `（${reconnectAttempt}/5）` : ""}` : "普通控制不可用"}</span>
    {configured && source === "basis" && (connection === "exhausted" || connection === "blocked") ? <button type="button" onClick={() => reconnect.current(true)}>重新连接</button> : null}
    {(["start", "stop", "close"] as Action[]).map(selected => <button type="button" key={selected} disabled={source !== "basis" || !capability.enabled || !state?.allowed_actions.includes(selected) || busy || (selected === "start" && startBlocked) || (selected === "close" && !state.active_cycle_id)} title={!capability.enabled ? capability.reason : selected === "start" && startBlocked ? parameterDirty ? "草稿未保存，请保存或显式放弃草稿后启动。" : parameterUnknown ? "原保存结果未知，请先核对。" : !parameters ? "参数接口不可用，需升级 owner 后启动。" : "请先停止并等待权威空仓、无待处理订单后启动。" : explanation(selected, state)} onClick={() => void openPreview(selected)}>{labels[selected]}</button>)}
    {configured && source === "basis" ? <button type="button" onClick={openParameters} disabled={busy || Boolean(action)}>参数</button> : null}
    {parameterDirty && !parametersOpen && source === "basis" ? <span className="v3-pending">草稿未保存 · 启动已禁用</span> : null}
    {parametersOpen && source === "basis" ? <ParameterDialog parameters={parameters} draft={parameterDraft} dirty={parameterDirty} connected={capability.enabled} busy={busy} unknown={parameterUnknown} error={parameterError} onChange={(key, value) => setParameterDraft(previous => previous ? { ...previous, [key]: value } : previous)} onSave={() => void saveParameters()} onLookup={() => void lookupParameterSave()} onClose={closeParameters} onDiscard={() => { const target = parameterTarget(parameters); setParameterDraft(target ? { ...target.values } : null); setParameterBase(target || null); setParameterError(""); }} /> : null}
    {configured && source === "basis" && (operation || recoverKey.current) ? <button type="button" disabled={busy || !capability.enabled} onClick={() => { opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null; if (operation) setAction(operation.action); else void recover(); }}>核对操作</button> : null}
    {action ? <dialog className="v3-owner-dialog" aria-labelledby="v3-owner-title" ref={dialog} onCancel={event => { event.preventDefault(); closeDialog(); }}>
      <div className="v3-owner-head"><h2 id="v3-owner-title">{labels[action]}{operation ? "进度" : "预检确认"}</h2><button type="button" aria-label="收起控制面板" onClick={closeDialog} disabled={busy}>×</button></div>
      <p>{explanation(action, state)}</p>
      <div className="v3-owner-intent">当前意图：{state ? intents[state.intent] : "未知"}{state?.risk_locks.length ? <span className="v3-pending"> · 风险锁 {state.risk_locks.join("、")}</span> : null}</div>
      {action === "start" && preview?.state.parameters ? <><ParameterFacts value={parameterTarget(preview.state.parameters)!} openingPolicies={preview.state.parameters.saved ? preview.state.parameters.opening_policies?.saved : preview.state.parameters.opening_policies?.current} title={preview.state.parameters.saved ? "启动将启用已保存参数" : "启动使用当前参数"} />{preview.state.parameters.saved ? <p className="v3-pending">当前生效 revision {preview.state.parameters.current.revision} · {preview.state.parameters.current.config_hash}。此次确认将切换到上方已保存参数。</p> : null}</> : null}
      {error ? <p role="alert" className="v3-pending">{error}{uncertain ? " 本次结果 UNKNOWN，请核对原操作。" : ""}</p> : null}
      {preview && !operation ? <><dl className="v3-owner-facts">
        <div><dt>标的 / 周期</dt><dd>{preview.state.pair_identity}<small>{preview.expected_cycle_id || "运行意图"}</small></dd></div>
        <div><dt>账户 / 批次</dt><dd>{preview.state.account_binding}<small>{preview.state.batch_id}</small></dd></div>
        <div><dt>Entropy / Lighter</dt><dd>{units(preview.state.entropy_units, preview.state.quantity_step)}<small>{units(preview.state.hedge_units, preview.state.quantity_step)}</small></dd></div>
        <div><dt>仓位证据</dt><dd>{preview.state.position_authoritative ? "权威账户证据" : "未知 / 不可用"}<small>{time(preview.state.position_observed_ts)} 北京</small></dd></div>
        <div><dt>预计可成交毛盈亏</dt><dd>未知<small>owner 协议未提供可成交估值</small></dd></div>
        <div><dt>费用 / 资金费</dt><dd>未知<small>最终净值以正常账本核算为准</small></dd></div>
      </dl><p className="v3-muted">预检剩余 {Math.max(0, Math.ceil(preview.expires_at - now))} 秒 · {preview.state.pending_orders.length} 笔待处理订单</p>
      {changed || expired ? <p className="v3-pending">{changed ? "owner、运行意图、周期或仓位已变化" : "预检已过期"}，请重新预检。</p> : null}</> : null}
      {operation ? <div className="v3-owner-progress" role="status"><strong>{operationLabels[operation.state]}{operation.state === "completed" && action === "close" ? operation.evidence.accounting_final === true ? " · 已核算" : " · 待核算" : ""}</strong>
        {action === "close" ? <><div className="v3-owner-legs">{(operation.evidence.legs || []).map(leg => <section key={leg.venue}><h3>{leg.venue === "entropy" ? "Entropy" : leg.venue === "hedge" ? "Lighter" : leg.venue}</h3><p>初始 {units(leg.initial_units, state?.quantity_step)}</p><p>剩余 {units(leg.remaining_units, state?.quantity_step)}</p>{leg.orders.map((order, index) => <div key={order.order_ref || index}><span className={order.state === "UNKNOWN" ? "v3-pending" : ""}>{order.state === "UNKNOWN" ? "UNKNOWN · 等待原订单核对" : order.state || "订单状态未知"}</span><small>累计成交 {order.filled_units ?? "未知"} units{order.filled_units != null && order.filled_units > 0 && leg.remaining_units != null && leg.remaining_units !== 0 ? " · 部分成交" : ""}</small><code>{order.order_ref}</code></div>)}</section>)}</div><dl className="v3-owner-facts"><div><dt>本圈净结果</dt><dd>{operation.evidence.accounting_final === true && operation.evidence.net_pnl_usd != null ? `${operation.evidence.net_pnl_usd} U · 已核算` : "待核算 · 净值未知"}</dd></div><div><dt>手续费</dt><dd>{operation.evidence.fees_complete === true ? "已补齐" : "待补齐"}<small>已知 {operation.evidence.known_fees_usd ?? "未知"} U</small></dd></div></dl></> : <p className="v3-muted">以当前 owner 意图与仓位证据判断状态；接受回执不等于引擎已归零。</p>}
        <small>操作编号 {operation.operation_id}</small>
      </div> : null}
      <div className="v3-owner-actions"><button type="button" onClick={closeDialog} disabled={busy}>收起</button>{uncertain ? <button type="button" disabled={busy || !capability.enabled} onClick={() => void recover(true)}>核对原操作 / 原键重试</button> : !operation ? <>{!preview || expired || changed ? <button type="button" disabled={busy || !capability.enabled} onClick={() => void openPreview(action)}>重新预检</button> : <button type="button" className="v3-owner-confirm" disabled={busy || !capability.enabled || Boolean(submitted.current)} onClick={() => void confirm()}>确认{labels[action]}</button>}</> : null}</div>
      {busy ? <p className="v3-muted" role="status">正在核对本地 owner…</p> : null}
    </dialog> : null}
  </div>;
}
