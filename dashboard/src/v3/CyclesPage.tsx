import { useEffect, useState, type ReactNode } from "react";
import { Empty, Section, TraderFreshness } from "./Common";
import { withQuery } from "./client";
import { exact } from "./format";
import { commonQuery, queryKey, type RouteState } from "./router";
import TraderCycleTable from "./TraderCycleTable";
import type { CyclesData } from "./types";
import { useResource } from "./useResource";
import CyclePanel from "./CyclePanel";

export default function CyclesPage({ route, update, paused, toolbar, refresh = 0 }: { route: RouteState; update: (patch: Partial<RouteState>) => void; paused: boolean; toolbar?: ReactNode; refresh?: number }) {
  const view = route.view || (route.page === "losses" ? "loss" : "complete");
  const listRoute = { ...route, source: "" as const };
  const filterKey = `${queryKey(listRoute)}|${route.direction}|${route.status}|${route.flag}|${view}|${route.sort}`;
  const [cursorHistory, setCursorHistory] = useState<string[]>([]);
  useEffect(() => setCursorHistory([]), [filterKey]);
  const key = `cycles|${filterKey}|${route.cursor}`;
  const list = useResource<CyclesData>(key, () => {
    const query = commonQuery(listRoute);
    query.set("limit", "50");
    query.set("view", view);
    query.set("sort", route.sort || "latest");
    if (route.direction) query.set("direction", route.direction);
    if (route.status) query.set("status", route.status);
    if (route.flag) query.set("flag", route.flag);
    if (route.cursor) query.set("cursor", route.cursor);
    return withQuery("cycles", query);
  }, 5_000, paused, refresh);
  const items = list.envelope?.data.items || [];
  const setFilter = (patch: Partial<RouteState>) => update({ ...patch, cursor: "", cycle: "", source: "", group: "" });
  return <div className="v3-page v3-trades-page"><div className={`v3-trades-workspace ${route.cycle && route.source ? "has-detail" : ""}`}><div className="v3-trades-list"><div className="v3-page-title"><div><h1>{route.page === "losses" ? "亏损分析" : "交易记录"}</h1></div></div>
    {toolbar}<TraderFreshness meta={list.envelope?.meta} error={list.error} stale={list.stale} timezone={route.timezone} />
    <details className="v3-secondary-panel"><summary>方向、排序与执行证据筛选{route.direction || route.status || route.flag ? " · 已应用" : ""}</summary><div className="v3-toolbar"><label>方向 <select value={route.direction} onChange={event => setFilter({ direction: event.target.value })}><option value="">全部方向</option><option value="long">Entropy 多</option><option value="short">Entropy 空</option></select></label>
      <label>排序 <select value={route.sort} onChange={event => setFilter({ sort: event.target.value })}><option value="latest">最近开始</option><option value="loss">亏损最大</option><option value="profit">盈利最高</option><option value="hold">持仓最久</option></select></label>
      <details className="v3-advanced-filters"><summary>更多筛选</summary><label>状态 <select value={route.status} onChange={event => setFilter({ status: event.target.value })}><option value="">全部</option><option value="open">持有 / 处理中</option><option value="closed">已结束</option></select></label>
        <label>执行证据 <select value={route.flag} onChange={event => setFilter({ flag: event.target.value })}><option value="">全部</option><option value="zero_fill">零成交</option><option value="repair">修复 / unwind</option><option value="unknown">曾 UNKNOWN</option><option value="long_hold">长持仓</option><option value="accounting_pending">待最终会计</option></select></label></details>
      <button type="button" className="v3-clear-filters" onClick={() => setFilter({ view: route.page === "losses" ? "loss" : "complete", direction: "", status: "", flag: "", sort: "latest" })}>重置筛选</button></div></details>
    <Section title={view === "unfilled" ? "未成交尝试" : "成交圈"} note={`${exact(list.envelope?.data.total)} 条符合条件 · 每页最多 50 条；缺失记录不按零处理。`}>
      {!list.envelope ? <Empty>{list.loading ? "正在读取交易记录…" : "交易记录不可用。"}</Empty> : items.length ? <TraderCycleTable items={items} route={route} update={update} /> : <Empty>{list.envelope.data.reason ? "交易账本暂不可用，无法判断记录数量。" : "此范围没有符合条件的记录。"}</Empty>}
      <div className="v3-pagination"><button type="button" disabled={!cursorHistory.length} onClick={() => {
        const previous = cursorHistory[cursorHistory.length - 1];
        setCursorHistory(stack => stack.slice(0, -1)); update({ cursor: previous });
      }}>上一页</button><span>{route.cursor ? "已翻页" : "第一页"}</span><button type="button" disabled={!list.envelope?.data.next_cursor} onClick={() => {
        if (!list.envelope?.data.next_cursor) return;
        setCursorHistory(stack => [...stack, route.cursor]); update({ cursor: list.envelope!.data.next_cursor! });
      }}>下一页</button></div>
    </Section>
    </div>{route.cycle && route.source ? <CyclePanel route={route} update={update} paused={paused} refresh={refresh} /> : null}</div>
  </div>;
}
