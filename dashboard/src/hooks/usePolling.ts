import { useEffect, useState } from "react";

export function usePolling<T>(loader: (signal: AbortSignal) => Promise<T>, intervalMs: number) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    let controller: AbortController | null = null;
    let timer: number | null = null;
    let inFlight = false;

    const clearTimer = () => {
      if (timer !== null) {
        window.clearTimeout(timer);
        timer = null;
      }
    };

    const run = async () => {
      if (!active || inFlight || document.hidden) return;
      inFlight = true;
      controller = new AbortController();
      try {
        const next = await loader(controller.signal);
        if (active) { setData(next); setError(null); }
      } catch (reason) {
        if (active && !(reason instanceof DOMException && reason.name === "AbortError")) {
          setError(reason instanceof Error ? reason.message : "Data unavailable");
        }
      } finally {
        inFlight = false;
        if (active) {
          setLoading(false);
          // Schedule after the request finishes. This prevents a slow
          // dashboard response from creating overlapping fetch/parse/render
          // work. Do not keep a hidden tab polling or retaining fresh
          // response trees that cannot be rendered.
          if (!document.hidden) timer = window.setTimeout(run, intervalMs);
        }
      }
    };

    const onVisibilityChange = () => {
      if (document.hidden) {
        clearTimer();
        controller?.abort();
        return;
      }
      void run();
    };

    document.addEventListener("visibilitychange", onVisibilityChange);
    void run();
    return () => {
      active = false;
      clearTimer();
      document.removeEventListener("visibilitychange", onVisibilityChange);
      controller?.abort();
    };
  }, [loader, intervalMs]);

  return { data, error, loading };
}
