import { lazy, Suspense, useEffect, useState } from "react";
import { routeHash, useHashRoute, type Mode, type Page, type RouteState, type Scope } from "./v3/router";
import { useResource } from "./v3/useResource";
import type { Deployment } from "./v3/types";

const OverviewPage = lazy(() => import("./v3/OverviewPage"));
const MarketPage = lazy(() => import("./v3/MarketPage"));
const CyclesPage = lazy(() => import("./v3/CyclesPage"));
const SystemPage = lazy(() => import("./v3/SystemPage"));

const nav: { page: Page; label: string }[] = [
  { page: "overview", label: "总览" },
  { page: "cycles", label: "交易记录" },
  { page: "market", label: "价差监控" },
  { page: "system", label: "系统证据" },
];
const scopeOptions: { value: Scope; label: string }[] = [
  { value: "7d", label: "近 7 天" }, { value: "all", label: "全部" },
  { value: "today", label: "今天（北京时间）" }, { value: "24h", label: "最近 24 小时" },
  { value: "session", label: "最近一次运行" }, { value: "batch", label: "本批次" },
  { value: "custom", label: "自定义日期" },
];
const cycleViews = [["complete", "完整圈"], ["open", "仍持有"], ["incomplete", "未完整 / 失败"], ["filled", "全部有成交"], ["unfilled", "零成交尝试"], ["loss", "已核亏损"], ["profit", "已核盈利"]];

function localDateTime(timestamp: number, timezone: RouteState["timezone"]): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone, year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hourCycle: "h23",
  }).formatToParts(timestamp);
  const value = (part: string) => parts.find(item => item.type === part)?.value || "00";
  return `${value("year")}-${value("month")}-${value("day")}T${value("hour")}:${value("minute")}`;
}

export default function App() {
  const [route, update] = useHashRoute();
  const [paused, setPaused] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [pairDraft, setPairDraft] = useState(route.pair);
  const deploymentResult = useResource<{ deployments: Deployment[] }>("deployments", () => "deployments", 5_000, paused, refresh);
  const deployments = deploymentResult.envelope?.data.deployments || [];
  const modeDeployments = deployments.filter(item => route.mode === "all" || item.mode === route.mode);
  const symbols = Array.from(new Set(modeDeployments.flatMap(item => item.symbols))).sort();
  const filteredDeployments = modeDeployments.filter(item => !route.symbol || item.symbols.includes(route.symbol));
  const batches = filteredDeployments.flatMap(item => (item.batches || []).map(batch => ({
    value: `${item.source_id}:${batch.batch_id}`, label: `${item.label} · ${batch.batch_id}`,
  })));
  const batchSource = route.batch.split(":", 1)[0];
  const scopedDeployments = filteredDeployments.filter(item => route.scope !== "batch" || !["basis", "funding"].includes(batchSource) || item.source_id === batchSource);
  useEffect(() => { document.title = `${nav.find(item => item.page === route.page)?.label || "运行总览"} · Entropy V3`; }, [route.page]);
  useEffect(() => setPairDraft(route.pair), [route.pair]);
  const setFilter = (patch: Partial<RouteState>) => update({ ...patch, cycle: "", source: "", group: "", cursor: "" });
  const setMode = (mode: Mode) => {
    const matching = deployments.filter(item => mode === "all" || item.mode === mode);
    const keepSymbol = !route.symbol || matching.some(item => item.symbols.includes(route.symbol));
    const keepBatch = !["basis", "funding"].includes(batchSource) || mode === "all" || mode === batchSource;
    setFilter({ mode, scope: mode === "all" && route.scope === "session" ? "7d" : route.scope, symbol: keepSymbol ? route.symbol : "", batch: keepBatch ? route.batch : "", pair: "", session: "" });
  };
  const setScope = (scope: Scope) => {
    if (scope === "custom" && (!route.from || !route.to)) {
      setFilter({ scope, from: localDateTime(Date.now() - 3600_000, route.timezone), to: localDateTime(Date.now(), route.timezone) });
    } else setFilter({ scope });
  };
  const navHash = (page: Page) => routeHash({ ...route, page, view: "", cursor: "", cycle: "", source: "", group: "", direction: "", status: "", flag: "", sort: "latest", evidence: "sources" });
  const pageProps = { route, paused, update };
  const filterBar = <div className="v3-global-filters" aria-label="全局筛选"><label><select aria-label="全局模式" value={route.mode} onChange={event => setMode(event.target.value as Mode)}><option value="all">全部模式</option><option value="basis">普通对冲</option><option value="funding">资金费对冲</option></select></label>
        <label><select aria-label="标的" value={route.symbol} onChange={event => setFilter({ symbol: event.target.value, pair: "" })}><option value="">全部标的</option>{route.symbol && !symbols.includes(route.symbol) ? <option value={route.symbol}>{route.symbol} · 当前范围无来源</option> : null}{symbols.map(symbol => <option key={symbol} value={symbol}>{symbol}</option>)}</select></label>
        <label><select aria-label="时间范围" value={route.scope} onChange={event => setScope(event.target.value as Scope)}>{scopeOptions.filter(option => option.value !== "session" || route.mode !== "all").map(option => <option key={option.value} value={option.value}>{option.value === "today" && route.timezone === "UTC" ? "今天（UTC）" : option.label}</option>)}</select></label>
        {route.view && route.view !== "complete" && (route.page === "overview" || route.page === "cycles") ? <button type="button" className="v3-clear-filters" aria-label="清除圈次筛选，显示已完成交易" onClick={() => setFilter({ view: "complete", direction: "", status: "", flag: "" })}>{cycleViews.find(([value]) => value === route.view)?.[1] || "圈次筛选"} ×</button> : null}
        {route.page === "overview" || route.page === "cycles" ? <label><select aria-label="圈次口径" value={route.view || "complete"} onChange={event => setFilter({ view: event.target.value })}>{cycleViews.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label> : null}
        <details className="v3-range-options"><summary aria-label="更多设置">⋯</summary><div className="v3-filterbar-fields">
        <label>范围时区 <select aria-label="时区" value={route.timezone} onChange={event => setFilter({ timezone: event.target.value as RouteState["timezone"] })}><option value="Asia/Taipei">北京 UTC+8</option><option value="UTC">UTC</option></select></label>
        <button type="button" onClick={() => setPaused(value => !value)} title="只暂停本页面自动刷新，不影响实盘">{paused ? "恢复刷新" : "暂停刷新"}</button>
        <button type="button" onClick={() => setRefresh(value => value + 1)} title="只刷新展示数据">刷新 ↻</button>

<form onSubmit={event => { event.preventDefault(); setFilter({ pair: pairDraft.trim() }); }}><label>交易对 <input value={pairDraft} onChange={event => setPairDraft(event.target.value)} placeholder="当前 pair" aria-label="交易对" /></label><button type="submit">应用</button></form>
        {route.scope === "session" ? <label>会话 ID <input value={route.session} placeholder="最近一次运行" onChange={event => setFilter({ session: event.target.value })} /></label> : null}
        {route.scope === "batch" ? <label>批次 <select aria-label="批次" value={route.batch} onChange={event => setFilter({ batch: event.target.value })}><option value="">各部署当前批次</option>{route.batch && !batches.some(batch => batch.value === route.batch) ? <option value={route.batch}>{route.batch} · 历史批次</option> : null}{batches.map(batch => <option key={batch.value} value={batch.value}>{batch.label}</option>)}</select></label> : null}
        {route.scope === "custom" ? <><label>从 <input type="datetime-local" value={route.from} onChange={event => setFilter({ from: event.target.value })} /></label><label>至 <input type="datetime-local" value={route.to} onChange={event => setFilter({ to: event.target.value })} /></label></> : null}
        <span className="v3-filter-note">收益按所选日期统计；持仓单独显示最新记录。</span></div></details></div>;
  return <div className="v3-app v3-four-page">
    <header className="v3-primary-header"><a className="v3-wordmark" href={navHash("overview")} aria-label="Entropy 交易总览"><span className="v3-wordmark-icon">E</span><strong>ENTROPY</strong></a>
      <nav aria-label="V3 页面导航">{nav.map(item => <a key={item.page} href={navHash(item.page)} aria-current={route.page === item.page ? "page" : undefined}>{item.label}</a>)}</nav>
      <span className="v3-header-note">数据展示 · 控制待接通</span>
    </header>
    <div className="v3-main-shell">
      <main className="v3-content">
        {deploymentResult.error ? <p className="v3-form-error" role="status">来源读取失败：{deploymentResult.error}{deploymentResult.stale ? "；显示上次来源，状态待确认。" : "。"}</p> : null}
        <Suspense fallback={<div className="v3-empty">正在读取页面…</div>}>
          {route.page === "overview" ? <OverviewPage {...pageProps} refresh={refresh} toolbar={filterBar} /> : null}
          {route.page === "cycles" ? <CyclesPage {...pageProps} refresh={refresh} toolbar={filterBar} /> : null}
          {route.page === "market" ? <MarketPage {...pageProps} refresh={refresh} deployments={scopedDeployments} toolbar={filterBar} /> : null}
          {route.page === "system" ? <SystemPage {...pageProps} refresh={refresh} deployments={scopedDeployments} toolbar={filterBar} /> : null}
        </Suspense>
      </main>
      <div className="v3-footer"><span>Entropy</span><span>按来源时间展示 · 未知数据不补零</span></div>
    </div>
  </div>;
}
