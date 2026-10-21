import { money } from "./format";
import type { CycleDetailData } from "./types";
import type { RouteState } from "./router";
import ExecutionProcess from "./ExecutionProcess";

function impactLabel(data: NonNullable<CycleDetailData["pnl_breakdown"]>): string {
  const negatives = [
    ["成交价差现金结果", Number(data.trade_cash_usd)],
    ["手续费", -Number(data.fees_usd)],
    ["资金费", Number(data.funding_usd)],
  ] as const;
  const largest = negatives.filter(([, value]) => Number.isFinite(value) && value < 0)
    .sort((a, b) => a[1] - b[1])[0];
  return largest ? `账务上最大负项：${largest[0]}。` : "已核对账务分项；没有单一负项。";
}

function EvidenceRow({ label, value, note }: { label: string; value: string; note?: string }) {
  return <div className="v3-analysis-row"><span>{label}{note ? <small>{note}</small> : null}</span><strong>{value}</strong></div>;
}

export default function LossAnalysis({ data }: {
  data: CycleDetailData; timezone: RouteState["timezone"];
}) {
  const cycle = data.cycle;
  const breakdown = data.pnl_breakdown;
  const isLoss = cycle?.finalized_net_usd?.startsWith("-");
  return <div className="v3-loss-analysis">
    <div className="v3-analysis-lead"><span>{cycle?.accounting_state === "final" ? "最终净收益" : "会计尚未完成"}</span>
      <strong className={isLoss ? "v3-loss" : ""}>{cycle?.accounting_state === "final" ? money(cycle.finalized_net_usd) : "待确认"}</strong>
      <p>{breakdown ? impactLabel(breakdown) : "当前没有足够的已核对账务分项，不能判断亏损构成。"}</p></div>
    <section className="v3-analysis-section"><h3>钱是怎样算出来的</h3>
      {breakdown ? <><div className="v3-analysis-rows">
        <EvidenceRow label="成交价差现金结果" value={money(breakdown.trade_cash_usd)} note="已确认成交现金流，未扣手续费或资金费" />
        <EvidenceRow label="实际手续费" value={`− ${money(breakdown.fees_usd)}`} />
        <EvidenceRow label="实际资金费" value={money(breakdown.funding_usd)} note="带符号：正数为收入，负数为支出" />
        <EvidenceRow label="最终净收益" value={money(breakdown.net_usd)} />
      </div><p className="v3-muted">分项与最终账本逐项闭合；这是账务构成，不是市场因果判断。</p></>
        : <p className="v3-muted">须待最终会计、手续费和资金费来源齐全且与账本净额一致后显示；缺失不按零处理。</p>}
    </section>
    <ExecutionProcess data={data} source={cycle?.source_id} />
  </div>;
}
