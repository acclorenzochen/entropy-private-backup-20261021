import { duration, time } from "./format";
import type { ProfitOverview } from "./types";
import type { RouteState } from "./router";

export default function ExitPosition({ holding, missing, holdingLink, timezone }: { holding?: ProfitOverview["holding"]; missing?: boolean; holdingLink: string; timezone: RouteState["timezone"] }) {
  const cycles = holding?.cycles || [];
  return <section className="v3-holdings" aria-label="当前持仓"><div className="v3-holdings-head"><h2>当前持仓 <small>{missing ? "待确认" : `${holding?.cycle_count ?? "未知"} 组`}</small></h2><a href={holdingLink}>持仓详情 ›</a></div>
    {missing ? <p className="v3-pending">部分来源缺失，仓位待核对。</p> : cycles.length ? <div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>标的 / 模式</th><th>持仓时长</th><th>双腿账本量</th><th>浮动盈亏</th></tr></thead><tbody>{cycles.map(cycle => <tr key={`${cycle.source_id}:${cycle.cycle_id}`}><td>{cycle.symbol} / {cycle.strategy_mode === "funding" ? "资金费" : "普通"}</td><td>{duration(cycle.hold_seconds)}</td><td>{cycle.entropy_units ?? "未知"} / {cycle.hedge_units ?? "未知"} units</td><td>{cycle.unrealized_usd == null ? "未知" : `${cycle.unrealized_usd} U`}</td></tr>)}</tbody></table></div> : <p className="v3-muted">账本没有未结束圈次；实时账户仓位待查询。</p>}
    {cycles.length || missing ? <small className="v3-muted">账本记录不等于实时账户查询{cycles[0]?.started_at != null ? ` · ${time(cycles[0].started_at, timezone)}` : ""}</small> : null}
  </section>;
}
