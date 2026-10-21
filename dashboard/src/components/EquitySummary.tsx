import { localTime, money } from "../format";
import type { EquitySnapshot, EquityVenueSnapshot, StatusResponse } from "../types";

export function EquitySummary({ equity, venues, today }: {
  equity: EquitySnapshot | null;
  venues: StatusResponse["venues"];
  today: StatusResponse["today"];
}) {
  const lifetime = equity?.lifetime;
  const hasLifetime = Boolean(lifetime?.coverage_complete
    && lifetime.change_usd != null);
  const baseline = hasLifetime && lifetime?.start_ts
    ? `全生命周期起点：${localTime(lifetime.start_ts)} · ${lifetime.sample_count.toLocaleString()} 个快照`
    : equity?.start_ts
      ? `本次运行起点：${localTime(equity.start_ts)} · 首次完整权益快照`
      : "等待两边完成首次权益采集";

  return <section className="equity-section" aria-labelledby="equity-title">
    <div className="section-head equity-head">
      <div>
        <h2 id="equity-title">全生命周期总盈亏</h2>
        <p>从最早可用的持久化权益快照对比到当前；重启不会重置这条总账。</p>
      </div>
      <span className="coverage-label">{baseline}</span>
    </div>
    <div className="equity-ledger">
      <EquityCell
        label={hasLifetime ? "总盈亏（全生命周期）" : "总盈亏（本次运行）"}
        change={hasLifetime ? lifetime?.change_usd : equity?.change_usd}
        detail={hasLifetime
          ? `当前 ${money(lifetime?.current_equity_usd ?? null)} · 起点 ${money(lifetime?.start_equity_usd ?? null)}`
          : `当前 ${money(equity?.current_equity_usd ?? null)} · 起点 ${money(equity?.start_equity_usd ?? null)}`}
        prominent
      />
      <EquityCell
        label="本次运行盈亏"
        change={equity?.change_usd}
        detail={`当前 ${money(equity?.current_equity_usd ?? null)} · 起点 ${money(equity?.start_equity_usd ?? null)}`}
      />
      <EquityCell
        label="今日净盈亏"
        change={today.net_pnl_usd}
        detail={today.day_started_ts
          ? `自然日自 ${localTime(today.day_started_ts)} 起`
          : "等待今日权益基线"}
      />
    </div>
    <div className="equity-venues" aria-label="本次运行分账户权益变化">
      <span>本次运行分账户权益</span>
      <VenueEquity label={venues.entropy} snapshot={equity?.venues.entropy} />
      <VenueEquity label={venues.hedge} snapshot={equity?.venues.hedge} />
    </div>
    {(!equity?.complete || !hasLifetime) && <p className="equity-note"><i aria-hidden="true" />
      {!equity?.complete
        ? "两边权益尚未同时完成采集，缺失值显示为 --；不会用估算值替代。"
        : "全生命周期基线尚未形成，当前总盈亏暂按本次运行口径显示。"}
    </p>}
  </section>;
}

function EquityCell({ label, change, detail, snapshot, prominent = false }: {
  label: string;
  change?: number | null;
  detail?: string;
  snapshot?: EquityVenueSnapshot;
  prominent?: boolean;
}) {
  const value = snapshot?.change_usd ?? change ?? null;
  const line = detail ?? `${money(snapshot?.start_equity_usd ?? null)} → ${money(snapshot?.current_equity_usd ?? null)}`;
  return <div className={`equity-cell ${prominent ? "equity-cell-total" : ""}`}>
    <span>{label}</span>
    <strong className={pnlTone(value)}>{money(value)}</strong>
    <small>{line}</small>
  </div>;
}

function VenueEquity({ label, snapshot }: {
  label: string;
  snapshot?: EquityVenueSnapshot;
}) {
  return <div className="equity-venue">
    <span>{label}</span>
    <strong className={pnlTone(snapshot?.change_usd ?? null)}>
      {money(snapshot?.change_usd ?? null)}
    </strong>
    <small>{money(snapshot?.start_equity_usd ?? null)} → {money(snapshot?.current_equity_usd ?? null)}</small>
  </div>;
}

function pnlTone(value: number | null) {
  if (value == null || Math.abs(value) < .005) return "";
  return value > 0 ? "tone-ok" : "tone-loss";
}
