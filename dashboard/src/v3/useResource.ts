import { useEffect, useState } from "react";
import { getV3 } from "./client";
import type { Envelope } from "./types";

interface Result<T> {
  envelope: Envelope<T> | null;
  loading: boolean;
  error: string | null;
  stale: boolean;
}

export function useResource<T>(key: string, requestPath: (signal: AbortSignal) => string | Promise<Envelope<T>>, intervalMs: number, paused = false, refresh = 0): Result<T> {
  const [record, setRecord] = useState<{ key: string; envelope: Envelope<T> | null; loading: boolean; error: string | null }>({
    key: "", envelope: null, loading: true, error: null,
  });
  useEffect(() => {
    let disposed = false;
    let controller: AbortController | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let generation = 0;
    setRecord(previous => previous.key === key
      ? { ...previous, loading: !paused && !previous.envelope }
      : { key, envelope: null, loading: !paused, error: null });
    const clearTimer = () => { if (timer) { clearTimeout(timer); timer = null; } };
    const run = async () => {
      if (disposed || paused || document.hidden || controller) return;
      clearTimer();
      const active = ++generation;
      const current = new AbortController();
      controller = current;
      let timedOut = false;
      const timeout = setTimeout(() => { timedOut = true; current.abort(); }, 12_000);
      try {
        const request = requestPath(current.signal);
        const envelope = typeof request === "string" ? await getV3<T>(request, current.signal) : await request;
        if (!disposed && active === generation) setRecord({ key, envelope, loading: false, error: null });
      } catch (error) {
        if (!disposed && active === generation && (!current.signal.aborted || timedOut)) {
          setRecord(previous => ({
            key, envelope: previous.key === key ? previous.envelope : null, loading: false,
            error: timedOut ? "V3 请求超时" : error instanceof Error ? error.message : "V3 请求失败",
          }));
        }
      } finally {
        clearTimeout(timeout);
        if (controller === current) controller = null;
        if (!disposed && active === generation && !paused && !document.hidden) timer = setTimeout(run, intervalMs);
      }
    };
    const visibilityChanged = () => {
      if (document.hidden) { clearTimer(); generation += 1; controller?.abort(); controller = null; }
      else run();
    };
    document.addEventListener("visibilitychange", visibilityChanged);
    if (!paused) run();
    else setRecord(previous => ({ ...previous, loading: false }));
    return () => {
      disposed = true;
      generation += 1;
      clearTimer();
      controller?.abort();
      document.removeEventListener("visibilitychange", visibilityChanged);
    };
    // key is the stable identity; requestPath can depend on the current clock for rolling windows.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, intervalMs, paused, refresh]);
  return {
    envelope: record.key === key ? record.envelope : null,
    loading: record.key !== key || record.loading,
    error: record.key === key ? record.error : null,
    stale: Boolean(record.key === key && record.error && record.envelope),
  };
}
