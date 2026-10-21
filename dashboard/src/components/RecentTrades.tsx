import { useCallback, useEffect, useMemo, useState } from "react";
import { getTradesPage } from "../api";
import { localTime, money, number, statusLabel } from "../format";
import type { Trade, TradeLeg, TradesResponse } from "../types";

type TradeFilter = "all" | "filled";
const PAGE_SIZE = 100;

export function RecentTrades({ trades }: { trades: TradesResponse }) {
  const [filter, setFilter] = useState<TradeFilter>("all");
  const [todayLoaded, setTodayLoaded] = useState(false);
  const [loadedTrades, setLoadedTrades] = useState<Trade[]>(trades.trades);
  const [totalCount, setTotalCount] = useState(trades.total_count);
  const [nextOffset, setNextOffset] = useState(trades.offset + trades.trades.length);
  const [hasMore, setHasMore] = useState(trades.has_more);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    if (todayLoaded) return;
    setLoadedTrades(trades.trades);
    setTotalCount(trades.total_count);
    setNextOffset(trades.offset + trades.trades.length);
    setHasMore(trades.has_more);
    setLoadError(null);
  }, [todayLoaded, trades]);

  const filteredTrades = useMemo(
    () => filter === "filled"
      ? loadedTrades.filter((trade) => trade.entropy_leg.fill_qty > 0 || trade.lighter_leg.fill_qty > 0)
      : loadedTrades,
    [filter, loadedTrades],
  );
  const filledCount = Math.max(0, trades.summary.attempt_count - trades.summary.no_fill_count);

  const loadToday = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const page = await getTradesPage(PAGE_SIZE, 0);
      setTodayLoaded(true);
      setLoadedTrades(page.trades);
      setTotalCount(page.total_count);
      setNextOffset(page.offset + page.trades.length);
      setHasMore(page.has_more);
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : "读取失败");
    } finally {
      setLoading(false);
    }
  }, []);

  const loadMore = useCallback(async () => {
    if (loading || !hasMore) return;
    setLoading(true);
    setLoadError(null);
    try {
      const page = await getTradesPage(PAGE_SIZE, nextOffset);
      setLoadedTrades((current) => mergeTrades(current, page.trades));
      setTotalCount(page.total_count);
      setNextOffset(page.offset + page.trades.length);
      setHasMore(page.has_more);
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : "读取失败");
    } finally {
      setLoading(false);
    }
  }, [hasMore, loading, nextOffset]);

  const countLabel = todayLoaded
    ? filter === "filled"
      ? `有成交 ${filteredTrades.length} / ${filledCount} 条 · 已加载 ${loadedTrades.length} / ${totalCount} 条`
      : `已加载 ${loadedTrades.length} / ${totalCount} 条`
    : filter === "filled"
      ? `有成交 ${filteredTrades.length} / ${filledCount} 条 · 最近 ${loadedTrades.length} 条`
      : `显示最近 ${loadedTrades.length} 条 · 今日共 ${totalCount} 条`;

  return <section className="trades-section" aria-labelledby="trades-title">
    <div className="section-head">
      <div>
        <h2 id="trades-title">双腿成交明细</h2>
        <p>盈亏来自 API 成交账本的 FIFO 归因；开仓显示未平仓，平仓总额为两腿已实现净盈亏，手续费只统计 Entropy。</p>
      </div>
      <div className="trade-tools">
        <div className="trade-filter" role="group" aria-label="成交记录筛选">
          <button type="button" aria-pressed={filter === "all"} onClick={() => setFilter("all")}>全部</button>
          <button type="button" aria-pressed={filter === "filled"} onClick={() => setFilter("filled")}>有成交</button>
        </div>
        <span className="trade-count" aria-live="polite">{countLabel}</span>
      </div>
    </div>
    {filteredTrades.length ? <div className="table-scroll"><table className="trades-table"><thead><tr><th>时间 / 操作</th><th>Entropy 腿</th><th>Lighter 腿</th><th>执行结果</th><th>Entropy 盈亏</th><th>Lighter 盈亏</th><th>双腿平仓净盈亏</th><th>成交价差（参考）</th><th>手续费（Entropy）</th></tr></thead>
      <tbody>{filteredTrades.map((trade) => <tr key={tradeKey(trade)}>
        <td><time>{localTime(trade.ts)}</time><small className="cell-meta">{statusLabel(trade.action)} · {statusLabel(trade.direction)} · {money(trade.notional_usd)}</small><small className="cell-meta">{trade.execution_id ? `执行 ${trade.execution_id}` : trade.source ?? "历史 CSV"}</small></td>
        <td><LegCell leg={trade.entropy_leg} /></td>
        <td><LegCell leg={trade.lighter_leg} /></td>
        <td><span className={`trade-status ${trade.outcome}`}>{statusLabel(trade.outcome)}</span></td>
        <td><PnlCell pnl={trade.entropy_leg} fallbackStatus={pnlFallbackStatus(trade)} /></td>
        <td><PnlCell pnl={trade.lighter_leg} fallbackStatus={pnlFallbackStatus(trade)} /></td>
        <td><PnlCell gross={trade.gross_realized_pnl_usd} net={trade.net_realized_pnl_usd} status={trade.pnl_status ?? pnlFallbackStatus(trade)} /></td>
        <td className={trade.fill_edge_usd != null && trade.fill_edge_usd < 0 ? "tone-loss" : ""}>{trade.outcome === "paired" || trade.outcome === "partial_paired" ? money(trade.fill_edge_usd) : "--"}</td>
        <td><strong>{money(trade.fees_usd)}</strong><small className="cell-meta">{statusLabel(trade.fees_status)}</small></td>
      </tr>)}</tbody>
    </table></div> : <div className="empty-state trades-empty"><span />{filter === "filled" ? "当前已加载记录中没有实际成交。清除筛选可查看全部记录。" : "今日暂无成交记录。"}</div>}
    {loadError && <p className="trade-load-error" role="alert">加载成交记录失败：{loadError}</p>}
    <div className="trade-load-row">
      {!todayLoaded
        ? <button className="trade-load-more" type="button" onClick={loadToday} disabled={loading}>{loading ? "加载中…" : "加载今天全部"}</button>
        : hasMore
          ? <button className="trade-load-more" type="button" onClick={loadMore} disabled={loading}>{loading ? "加载中…" : "加载更多记录"}</button>
          : <span className="trade-loaded-note">今天的 {totalCount} 条记录已全部加载</span>}
    </div>
  </section>;
}

function mergeTrades(current: Trade[], incoming: Trade[]) {
  const keys = new Set(current.map(tradeKey));
  return [...current, ...incoming.filter((trade) => !keys.has(tradeKey(trade)))];
}

function tradeKey(trade: Trade) {
  return trade.execution_id ?? [trade.ts, trade.action, trade.direction, trade.qty, trade.notional_usd].join(":");
}

function pnlFallbackStatus(trade: Trade) {
  return trade.action === "reduce" || trade.action === "hedge" || trade.reduce_only
    ? "unavailable"
    : "not_closed";
}

function PnlCell({
  pnl,
  gross,
  net,
  status,
  fallbackStatus,
}: {
  pnl?: TradeLeg;
  gross?: number | null;
  net?: number | null;
  status?: string;
  fallbackStatus?: string;
}) {
  const resolvedGross = gross ?? pnl?.gross_realized_pnl_usd;
  const resolvedNet = net ?? pnl?.net_realized_pnl_usd;
  const resolvedStatus = status ?? pnl?.pnl_status ?? fallbackStatus ?? "unavailable";
  if (resolvedNet == null) {
    return <div className="pnl-cell is-unavailable" aria-label={`已实现盈亏：${statusLabel(resolvedStatus)}`}>
      <strong>--</strong>
      <small className="cell-meta">{statusLabel(resolvedStatus)}</small>
    </div>;
  }
  const tone = resolvedNet < 0 ? "tone-loss" : resolvedNet > 0 ? "tone-ok" : "";
  return <div className={`pnl-cell ${tone}`} aria-label={`净已实现盈亏 ${money(resolvedNet)}`}>
    <strong>{money(resolvedNet)}</strong>
    {resolvedGross != null && <small className="cell-meta">毛 {money(resolvedGross)}</small>}
    <small className="cell-meta">{statusLabel(resolvedStatus)}</small>
  </div>;
}

function LegCell({ leg }: { leg: TradeLeg }) {
  const fillRatio = leg.planned_qty > 0 ? leg.fill_qty / leg.planned_qty : 0;
  const fillClass = fillRatio >= .9999 ? "filled" : fillRatio > 0 ? "partial" : "failed";
  return <div className="leg-cell">
    <span><b className={`leg-side ${leg.side}`}>{statusLabel(leg.side)}</b><em className={fillClass}>{statusLabel(leg.status)}</em></span>
    <small>{number(leg.fill_qty, 4)} / {number(leg.planned_qty, 4)}</small>
    {leg.fee_usd != null && <small className="cell-meta">费 {money(leg.fee_usd)} · {statusLabel(leg.fee_status ?? "pending")}</small>}
  </div>;
}
