import { type RouteState } from "./router";

export default function WorkflowContext({ route, sourceLabel, update }: {
  route: RouteState; sourceLabel?: string; update: (patch: Partial<RouteState>) => void;
}) {
  if (!route.cycle) return null;
  return <aside className="v3-workflow-context" aria-label="所选周期工作流">
    <div><strong>正在查看交易 · {sourceLabel || (route.source === "basis" ? "普通 V3" : route.source === "funding" ? "资金费 V3" : "来源待确认")}</strong><code title={route.cycle}>{route.cycle.slice(-8)}</code></div>

    <button type="button" onClick={() => update({ cycle: "", source: "", group: "" })}>清除选择</button>
  </aside>;
}
