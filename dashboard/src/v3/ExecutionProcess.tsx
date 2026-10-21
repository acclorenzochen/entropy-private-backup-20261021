import { deriveExecutionEvidence, type ExecutionLeg } from "./executionEvidence";
import type { CycleDetailData } from "./types";

function price(value: number | null): string {
  return value == null ? "待确认" : value.toLocaleString("en-US", { maximumFractionDigits: 6 });
}
function impact(value: number | null): string {
  if (value == null) return "无法拆分";
  if (Math.abs(value) < 1e-10) return "无价格差异 · 0 U";
  return `${value < 0 ? "少赚" : "多赚"} ${Math.abs(value).toFixed(6)} U`;
}
function milliseconds(value: number | null): string {
  return value == null ? "待确认" : value >= 1_000 ? `${(value / 1_000).toFixed(3)} 秒` : `${value.toFixed(1)} ms`;
}
function LegEvidence({ leg, parallel }: { leg: ExecutionLeg; parallel: boolean }) {
  return <div className="v3-execution-leg"><h4>{leg.label} · {leg.direction}{leg.partial ? <small>部分成交</small> : null}</h4>
    <dl><div><dt>已确认数量</dt><dd>{leg.quantity == null ? `${leg.filledUnits ?? "待确认"} units` : price(leg.quantity)}</dd></div><div><dt>发单前同量 VWAP</dt><dd>{price(leg.expectedVwap)}</dd></div><div><dt>实际成交 VWAP</dt><dd>{price(leg.actualVwap)}</dd></div><div><dt>本腿价格影响</dt><dd className={leg.priceImpactUsd == null ? "v3-pending" : leg.priceImpactUsd < -1e-10 ? "v3-loss" : "v3-good"}>{impact(leg.priceImpactUsd)}</dd></div><div><dt>发单 → 本机结果确认</dt><dd>{milliseconds(leg.submitToConfirmMs)}</dd></div>{parallel ? <div><dt>共同触发 → 本机确认</dt><dd>{milliseconds(leg.triggerToConfirmMs)}</dd></div> : null}</dl>
    {leg.timingBasis ? <p className="v3-evidence-note">耗时依据：{leg.timingBasis}；确认回报时间不等于交易所成交时刻。</p> : null}
    {leg.limitations.map(note => <p className="v3-evidence-note v3-pending" key={note}>{note}</p>)}
    <details className="v3-order-refs"><summary>关联订单</summary>{leg.orderRefs.map(ref => <code key={ref}>{ref}</code>)}</details>
  </div>;
}
export default function ExecutionProcess({ data, source }: { data: CycleDetailData; source?: string }) {
  const evidence = deriveExecutionEvidence(data, source);
  return <section className="v3-execution-process" aria-label="执行过程在哪里变差" data-evidence-identity={evidence.identity}>
    <div className="v3-execution-conclusion"><span>执行过程在哪里变差</span><strong>{evidence.conclusion}</strong><p>{evidence.feeNote}</p></div>
    <ol className="v3-execution-stages">{evidence.stages.map((stage, index) => <li key={stage.id} data-stage={stage.id}>
      <div className="v3-stage-heading"><span>{index + 1}</span><h3>{stage.title}</h3>{stage.id === "close" ? <small>{stage.mode === "parallel" ? "两腿并行" : stage.mode === "sequential" ? "按顺序执行" : "先后关系待确认"}</small> : null}</div>
      <p>{stage.summary}</p>
      {stage.marketMovementLabel ? <div className="v3-stage-movement"><span>{stage.marketMovementLabel}</span><strong className={stage.marketMovementUsd == null ? "v3-pending" : stage.marketMovementUsd < -1e-10 ? "v3-loss" : "v3-good"}>{impact(stage.marketMovementUsd)}</strong></div> : null}
      {stage.basisChanges.map(change => <p className="v3-evidence-note" key={change.label}>{change.label}：{change.value.toFixed(2)} bp · 双所复合指标</p>)}
      {stage.legs.length ? <details className="v3-stage-details"><summary>{stage.mode === "parallel" ? "共同触发 · 分别查看两腿" : "查看同量盘口、成交与本腿耗时"}</summary><div className={stage.mode === "parallel" ? "v3-parallel-legs" : ""}>{stage.legs.map(leg => <LegEvidence key={leg.key} leg={leg} parallel={stage.mode === "parallel"} />)}</div></details> : <p className="v3-evidence-note">{stage.limitations.join("；")}</p>}
    </li>)}</ol>
    <p className="v3-evidence-note">正向影响表示多赚，负向影响表示少赚。仅比较真实记录中相同成交数量的可执行盘口，不将复合 bp 直接换算成 U。</p>
  </section>;
}
