import type { Envelope } from "./types";

export class V3RequestError extends Error {
  constructor(message: string, public readonly status?: number) { super(message); }
}

export async function getV3<T>(path: string, signal: AbortSignal): Promise<Envelope<T>> {
  const response = await fetch(`/api/v3/${path}`, {
    method: "GET", headers: { Accept: "application/json" }, signal, cache: "no-store",
  });
  if (!response.ok) {
    let detail = "";
    try {
      const body = await response.json() as { detail?: string; code?: string; message?: string };
      detail = body.detail ?? body.message ?? body.code ?? "";
    } catch { /* A non-JSON error still retains its HTTP status. */ }
    throw new V3RequestError(detail || `V3 接口返回 HTTP ${response.status}`, response.status);
  }
  const envelope = await response.json() as Envelope<T>;
  if (!envelope || typeof envelope !== "object" || !envelope.meta || !envelope.data) {
    throw new V3RequestError("V3 响应缺少 meta 或 data");
  }
  if (envelope.meta.schema_version !== 3) {
    throw new V3RequestError(`不支持的 V3 schema：${String(envelope.meta.schema_version ?? "缺失")}`);
  }
  return envelope;
}

export function withQuery(path: string, query: URLSearchParams): string {
  const suffix = query.toString();
  return suffix ? `${path}?${suffix}` : path;
}
