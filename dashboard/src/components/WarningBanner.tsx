import type { StatusResponse } from "../types";
import { localTime } from "../format";

const reasonCopy: Record<string, string> = {
  unbalanced: "双腿仓位不平衡。系统已禁止新增敞口，但仍允许减仓。",
  execution_recovery: "存在未完成的双腿执行。系统正在持续查询原订单并只对冲剩余数量，恢复完成前不会开启新执行。",
  reconciliation_pending: "仓位对账或重启恢复尚未完成。系统暂不新增敞口，直到交易所仓位与本地账本一致。",
  volatility_guard: "溢价波动过高，已禁止 OPEN / ADD；REDUCE 与紧急对冲仍可执行。",
  cycle_midline_drift: "本轮入场中枢漂移超限，已禁止继续 ADD；REDUCE 与紧急对冲仍可执行。",
  cycle_unanchored: "检测到未保存入场锚点的残仓，已禁止 ADD；REDUCE 与紧急对冲仍可执行。",
  midline_lag: "中枢滞后进入软不确定性状态；不改变统计 OPEN 阈值，不确定性会计入整周期 EV，REDUCE 与紧急对冲仍可执行。",
  reentry_guard: "风险退出后的再入场不确定性仍在：不作为全局安全阻断，继续按统计阈值和整周期 EV 评估。",
  premium_data_degraded: "连续超过 5 秒没有可靠的跨市场溢价样本，已禁止新增敞口；REDUCE 与紧急对冲仍可执行。",
  regime_shift: "检测到溢价中枢漂移，已切换入场参考；不改变统计 OPEN 阈值，不确定性计入整周期 EV，也不会因此自动切换为只减仓。",
  reference_deviation: "当前基差相对入场参考的偏离不足，暂缓 OPEN；达到新的偏离要求后仍可交易。",
  reference_invalid: "动态中心或模型数据不完整，OPEN 已硬阻断；REDUCE 与紧急对冲仍可执行。",
  dynamic_midline_warmup: "动态中枢模型仍在预热，OPEN 已硬阻断；模型就绪后恢复评估。",
  dynamic_midline_validation: "正在做重启后的动态中枢短暂校验，校验窗口内 OPEN 暂时硬阻断；完成后恢复评估。",
  margin_risk: "可用保证金进入临界安全边界，OPEN 已硬阻断；保证金预警本身只作为整周期 EV 不确定性，REDUCE 与紧急对冲仍可执行。",
  book_stale: "盘口数据已过期，OPEN 已硬阻断；数据恢复后继续评估。",
  cross_skew: "两家交易所盘口时间差过大，OPEN 已硬阻断；数据同步后继续评估。",
  venue_degraded: "交易场所暂不可用，OPEN 已硬阻断；REDUCE 与紧急对冲仍可执行。",
  order_budget: "订单预算暂时耗尽，OPEN 等待可用额度；REDUCE 与紧急对冲仍保留优先级。",
  funding_stale: "资金费率数据已过期。两边数据恢复前禁止新增敞口。",
  funding_too_expensive: "两个方向的资金费成本均超过开仓上限。",
  no_executable_edge: "当前没有满足保护价和执行余量的可成交优势，继续等待下一次报价。",
  protected_edge: "保护价下的可执行优势不足，暂不发送 OPEN；REDUCE 与紧急对冲仍可执行。",
  opportunity_locked: "当前方向仍在同一开仓机会窗口内，等待信号先离开再重新进入。",
  halted: "交易引擎已停止。请检查引擎日志后再重启。",
};

export function WarningBanner({ status, requestError }: { status: StatusResponse; requestError: string | null }) {
  const openState = status.strategy.open_state ?? status.open_state
    ?? (status.risk.open_blocked ? "hard_blocked" : "allowed");
  const openReason = status.strategy.open_block_reason
    ?? status.risk.open_block_reason;
  const hardBlocked = openState === "hard_blocked"
    || status.risk.open_block_class === "hard";
  const softRestricted = openState === "soft_restricted"
    || status.risk.open_block_class === "soft"
    || (status.strategy.reference_state != null
      && status.strategy.reference_state !== "normal");
  const referenceState = status.strategy.reference_state
    ?? status.strategy.midline_lag_state;
  const referenceMessage = referenceState === "warning"
    ? "入场参考处于预警状态：当前仍可交易，不改变统计阈值，不确定性计入整周期 EV。"
    : referenceState === "cold_degraded"
      ? "动态中枢已完成冷启动最低历史，当前可交易但仍带有模型不确定性；不改变统计阈值，不确定性计入整周期 EV。"
    : referenceState === "accelerating"
      ? "入场参考正在加速追踪：当前使用活动/动态中枢混合，不改变统计阈值，不确定性计入整周期 EV。"
      : referenceState === "shifting"
        ? "溢价中枢正在切换：OPEN 使用动态中心，不改变统计阈值，不确定性计入整周期 EV。"
        : referenceState === "blocked"
          ? "参考处于高不确定性状态；只要数据仍有效，继续按统计阈值和整周期 EV 评估，不自动切换为只减仓。"
          : reasonCopy[openReason ?? ""];
  const message = status.bot_status === "stopped"
    ? `交易脚本已停止。最后有效快照为 ${localTime(status.ts)}，数据不会继续更新，请勿据此下单。`
    : status.is_stale
      ? `交易脚本状态已失联 ${status.snapshot_age_sec.toFixed(1)} 秒，当前显示最后一次有效数据，请勿据此下单。`
      : status.bot_status === "error"
        ? "交易引擎已暂停，进程仍在运行，当前不会新增仓位。请检查引擎日志和双腿仓位，确认安全后再重启。"
        : status.config_restart_required
          ? `检测到需要重启才能生效的配置：${status.config_restart_required_paths.join("、")}。当前执行继续使用旧配置，请在空仓后重启。`
        : hardBlocked
          ? reasonCopy[openReason ?? ""] ?? "OPEN 已硬阻断，REDUCE 与紧急对冲仍可执行。"
          : softRestricted
            ? referenceMessage ?? reasonCopy[openReason ?? ""] ?? "当前 OPEN 处于软限制，满足整周期 EV 条件后可继续交易。"
            : requestError ? "实时刷新失败，当前显示最近一次有效数据。看板会自动重试。" : null;
  if (!message) return null;
  return <div className="warning" role="status"><span aria-hidden="true">!</span><p>{message}</p></div>;
}
