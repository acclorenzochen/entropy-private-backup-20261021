import { Empty, KV, Metric, QualityBadge, Section } from "./Common";
import { batchLimit, duration, exact, money, time } from "./format";
import { directionLabel } from "./TraderCycleTable";
import type { FundingDashboard, OverviewData } from "./types";
import type { RouteState } from "./router";

const states: Record<string, string> = {
  STARTING: "实盘启动检查中", RECONCILING: "仓位与订单恢复中",
  READY: "等待有效开仓条件", HOLDING_FUNDING: "持有对冲 · 等待实际结算",
  EXITING: "双腿 reduce-only 退出中", ACCOUNTING_PENDING: "等待成交与账务核对",
  BATCH_COMPLETE: "已达批次轮数上限", BATCH_CLOSED: "已暂停新开仓",
  RECONCILE_REQUIRED: "需要恢复核对", HALTED: "进程已停止／异常",
  FLAT: "本轮归零 · 等待账务核对", WAITING_FOR_MARKET: "等待有效行情",
};
export const fundingReasons: Record<string, string> = {
  two_settlements_each: "两边各确认足够结算，正常退出",
  funding_reversal_cost: "预计剩余净资金费支出超限",
  liquidation_entropy: "Entropy 强平距离触及阈值",
  liquidation_hedge: "Lighter 强平距离触及阈值",
  risk_data_unavailable: "风险数据缺失，保护退出",
  round_loss_limit: "整轮亏损触及阈值", projected_round_loss_limit: "预计整轮亏损超限",
  unknown_order_recovery: "未知订单恢复，保护退出", FUNDING_EARLY_EXIT: "提前退出，停止本批次新开仓",
  await_actual_settlements: "等待真实资金费账本，不用计时器代替结算",
  authorized_cycle_limit_reached: "授权轮数已达上限，继续迟入账核对",
};
function percent(value?: string | null) {
  return value == null || value === "" || !Number.isFinite(Number(value))
    ? "未知" : `${(Number(value) * 100).toFixed(2)}%`;
}

export default function FundingPanel({ data, live, timezone, unavailable = false, paused = false }: {
  data: FundingDashboard; live?: OverviewData["live"]; timezone: RouteState["timezone"]; unavailable?: boolean; paused?: boolean;
}) {
  const state = data.state;
  const policy = data.policy;
  const counts = state?.settlement_counts;
  const required = policy.funding_settlements_required;
  const fresh = !unavailable && !paused && data.live_quality === "available";
  const negativeSeconds = state?.negative_since_ts != null && state.last_sample_ts != null
    ? Math.max(0, state.last_sample_ts - state.negative_since_ts) : null;
  const exit = state?.exit_reason || data.batch?.close_reason;
  const attention = !fresh || Boolean(exit && exit !== "two_settlements_each")
    || ["RECONCILE_REQUIRED", "HALTED", "BATCH_CLOSED", "ACCOUNTING_PENDING"].includes(live?.current_state || "");
  const label = paused ? "页面刷新已暂停 · 最后已知状态" : !live ? "尚无实盘状态" : !fresh ? "实盘状态已过期 · 仅供历史核对"
    : states[live.current_state || ""] || live.current_state || "状态待确认";
  const progress = (venue: "entropy" | "hedge") => `${exact(counts?.[venue])} / ${exact(required)}`;
  return <div className="v3-funding" data-testid="funding-panel">
    <div className={`v3-trader-status ${attention ? "is-paused" : ""}`}>
      <div><span>NBIS 资金费对冲 · {data.pair_identity || "配对来源待确认"}</span><strong>{label}</strong>
        <p>{exit ? fundingReasons[exit] || exit : fresh ? fundingReasons[live?.reason || ""] || live?.reason || "只读监控" : "保留状态文件不代表进程仍在运行；公共预检不能代替实盘状态。"}</p></div>
      <small>实盘更新时间 {time(live?.source_ts, timezone)}</small>
    </div>
    <div className="v3-metrics v3-metrics-four">
      <Metric label="Entropy 结算进度" value={progress("entropy")} note="完整对冲后，不同实际结算周期" />
      <Metric label="Lighter 结算进度" value={progress("hedge")} note="真实零金额结算也计数；缺失不补零" />
      <Metric label="本轮已入账资金费" value={money(state?.actual_funding_usd)} note="按事件去重 · 非整轮最终净收益" tone={state?.actual_funding_usd?.startsWith("-") ? "loss" : undefined} />
      <Metric label="当前组合净资金费" value={exact(fresh ? data.net_usd_per_hour : null, " USD/h")} note={fresh && data.rate_quality === "available" ? "按实际持仓方向 · 已统一每小时" : "缺失或过期，不按零费率处理"} tone={fresh && data.net_usd_per_hour?.startsWith("-") ? "loss" : undefined} />
    </div>
    <Section title="双腿仓位与强平风险" note={`${directionLabel(data.cycle?.direction)} · 共同数量步长 ${exact(data.common_quantity_step)} · 账本步长 ${exact(data.accounting_quantity_step)}`}>
      <div className="v3-table-wrap"><table className="v3-table v3-funding-table"><thead><tr>
        <th>交易所</th><th>账本数量 / 方向</th><th>杠杆 配置 / 已确认</th><th>资金费率（bp/h）</th><th>标记价 / 强平价</th><th>强平距离</th><th>实际入账资金费</th>
      </tr></thead><tbody>{(["entropy", "hedge"] as const).map(venue => {
        const risk = fresh ? data.risk?.[venue] : undefined;
        const rate = fresh ? data.rates?.[venue] : undefined;
        const qty = data.ledger_quantities?.[venue];
        const distance = risk?.distance;
        const endangered = distance != null && policy.funding_liquidation_distance != null
          && Number(distance) <= Number(policy.funding_liquidation_distance);
        return <tr key={venue}>
          <td>{venue === "entropy" ? "Entropy io:NBIS" : "Lighter 主网 NBIS"}</td>
          <td>{exact(qty, " NBIS")}<small>Journal 持久化，非实时账户快照</small></td>
          <td>{exact(data.configured_leverage?.[venue], "×")} / {exact(data.applied_leverage?.[venue], "×")}<small>已确认值为进程最后报告</small></td>
          <td>{exact(rate?.rate_bps_per_hour)}<small>{rate?.quality === "available" ? time(rate.source_ts, timezone) : "未发布／过期"}</small></td>
          <td>{exact(risk?.mark_price)} / {exact(risk?.liquidation_price)}<small>{risk?.quality === "available" ? time(risk.source_ts, timezone) : "未发布／过期"}</small></td>
          <td className={endangered ? "v3-loss" : ""}>{percent(distance)}<small>{endangered ? "触及风险退出阈值" : distance == null ? "未知不能视为安全" : "仅新鲜风险快照"}</small></td>
          <td>{money(state?.funding_by_venue?.[venue])}</td>
        </tr>;
      })}</tbody></table></div>
      <p className="v3-muted">任一腿距离 ≤ {percent(policy.funding_liquidation_distance)}，策略优先双腿 reduce-only 退出。未发布字段需在下次正常启动加载新代码；不要仅为更新看板而重启持仓进程。</p>
    </Section>
    <div className="v3-layout-two">
      <Section title="逆转采样与退出判断" note="单腿费率变号不等于组合收益转负；不自动翻仓。"><div className="v3-card"><div className="v3-kv-grid">
        <KV label="连续有效负采样" value={`${exact(state?.negative_sample_count)} / ${exact(policy.funding_reversal_samples)} 次`} />
        <KV label="负采样持续时间" value={`${duration(negativeSeconds)} / ${exact(policy.funding_reversal_seconds, " 秒")}`} />
        <KV label="最新有效采样时间" value={time(state?.last_sample_ts, timezone)} />
        <KV label="当前净费率证据" value={<QualityBadge value={fresh ? data.rate_quality : "stale"} />} />
        <KV label="预计剩余净资金费支出" value={money(fresh ? data.expected_remaining_cost_usd : null)} />
        <KV label="逆转支出上限" value={money(policy.funding_remaining_cost_limit_usd)} />
        <KV label="退出原因" value={exit ? fundingReasons[exit] || exit : "尚未记录退出"} />
        <KV label="两周期状态" value={!state ? "未知" : state.completed ? "两周期完成" : "两周期未完成"} />
      </div><p className="v3-muted">满足采样次数和持续时间后，策略才评估剩余支出。未返回估算值不代表 0；预算内保持对冲，超过上限允许提前退出。</p></div></Section>
      <Section title="批次与安全参数" note="看板无开仓、调杠杆、解锁或重置批次功能。"><div className="v3-card"><div className="v3-kv-grid">
        <KV label="已用 / 圈数上限" value={`${exact(data.batch?.position_cycles)} / ${batchLimit(data.batch?.max_position_cycles)}`} />
        <KV label="每腿名义金额上限" value={money(policy.max_notional_usd)} />
        <KV label="整轮亏损阈值" value={policy.configured && policy.funding_round_loss_limit_usd === null ? "未设置" : money(policy.funding_round_loss_limit_usd)} />
        <KV label="累计亏损 / 批次预算" value={`${money(data.batch?.loss_used_usd)} / ${batchLimit(data.batch?.loss_budget_usd, " USD")}`} />
        <KV label="账务成交与费用核对" value={state?.trade_accounting_complete ? "已完成" : "未完成／未知"} />
        <KV label="平仓后两边账本查询" value={state?.closed_poll_complete ? "已确认" : "未完成／未知"} />
      </div><p className="v3-muted">{data.batch?.batch_id || "批次来源未提供"}<br />剩余资金费支出上限不是整轮止损；已入账资金费与预计支出分开，不能相加冒充实际损益。</p></div></Section>
    </div>
    <details className="v3-trader-more"><summary>查看实际结算记录与公共预检</summary>
      <Section title="本轮实际资金费记录" note={`来源 ${data.state_source || "未提供"} · 最近 20 条；金额为零的真实记录保留，按完整对冲后的结算周期计数。`}>
        {state?.settlements?.length ? <div className="v3-table-wrap"><table className="v3-table"><thead><tr><th>交易所</th><th>结算时间</th><th>实际金额</th><th>事件 ID</th></tr></thead><tbody>{state.settlements.map(item => <tr key={`${item.venue}-${item.event_id}`}><td>{item.venue === "entropy" ? "Entropy" : "Lighter"}</td><td>{time(item.ts, timezone)}</td><td>{money(item.amount_usd)}</td><td>{item.event_id}</td></tr>)}</tbody></table></div>
          : <Empty>尚无可展示的实际结算记录；不是缺失费率为零，也不能提前确认周期完成。</Empty>}
      </Section>
      <Section title="公共预检记录" note="这不是实盘账户、真实结算或延迟入账的验证。"><div className="v3-card">
        <p>{!data.public_preflight ? "公共预检记录未提供" : data.public_preflight.ok === true ? "公共预检通过 · 不等于实盘就绪" : "公共预检未通过／未知"} · {time(data.public_preflight?.source_ts, timezone)}</p>
        {data.public_preflight?.error ? <p className="v3-loss">{data.public_preflight.error}</p> : null}
      </div></Section>
    </details>
  </div>;
}
