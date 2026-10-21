import type { DashboardResponse, HistoryResponse, PnlResponse, StatusResponse, TradesResponse } from "./types";

async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, {
    signal,
    cache: "no-store",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => null) as { detail?: string } | null;
    throw new Error(body?.detail ?? `Request failed (${response.status})`);
  }
  return response.json() as Promise<T>;
}

export const getStatus = (signal?: AbortSignal) => get<StatusResponse>("/api/status", signal);
export const getHistory = (range: string, signal?: AbortSignal) =>
  get<HistoryResponse>(`/api/history?range=${range}`, signal);
export const getTradesPage = (limit = 100, offset = 0, signal?: AbortSignal) =>
  get<TradesResponse>(`/api/trades?limit=${limit}&offset=${offset}`, signal);
export const getTrades = (signal?: AbortSignal) => getTradesPage(20, 0, signal);
export const getPnl = (signal?: AbortSignal) => get<PnlResponse>("/api/pnl?range=today", signal);
export const getDashboard = (range: string, signal?: AbortSignal) =>
  get<DashboardResponse>(`/api/dashboard?range=${range}`, signal);
