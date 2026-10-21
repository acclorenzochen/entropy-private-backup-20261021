import type { StatusResponse } from "../types";
import { localTime, statusLabel } from "../format";

export function Header({ status }: { status: StatusResponse }) {
  const healthy = status.bot_status === "running" && !status.is_stale;
  const stopped = status.bot_status === "stopped";
  const disconnected = status.is_stale && !stopped;
  const halted = status.bot_status === "error" && !status.is_stale;
  const stateLabel = stopped ? "已停止"
    : disconnected ? "状态失联"
    : halted ? "已暂停"
    : healthy ? "运行中"
    : "异常";
  return <header className="topbar">
    <div className="brand" aria-label="Entropy Arb">
      <span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>
      <span>ENTROPY ARB</span>
    </div>
    <div className="pair-id">
      <strong>{status.venues.entropy} / {status.venues.hedge}</strong>
      <span>{status.symbol} 永续合约</span>
    </div>
    <div className="header-status">
      <span className={`state-chip ${healthy ? "state-ok" : "state-warn"}`}>
        <i aria-hidden="true" />{stateLabel}
      </span>
      <span className="header-meta">{statusLabel(status.regime)}</span>
      <time dateTime={status.time_utc}>更新于 {localTime(status.ts)}</time>
    </div>
  </header>;
}
