from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError

from ..trade_ledger import TradeLedger
from .models import (DashboardResponse, HistoryResponse, PnlResponse,
                     StatusResponse, TradesResponse)
from .readers import (MAX_HISTORY_POINTS, RANGE_SECONDS, SnapshotUnavailable,
                      downsample_points, read_history, read_dashboard,
                      read_pnl, read_pnl_from_ledger, read_status,
                      read_trades)


def create_app(log_dir: str | Path | None = None) -> FastAPI:
    root = Path(log_dir or os.getenv("ENTROPY_ARB_LOG_DIR", "logs"))
    status_path = Path(os.getenv(
        "ENTROPY_ARB_STATUS_PATH", str(root / "status.json")))
    seconds_path = root / "seconds.csv"
    trades_path = root / "trades.csv"
    timezone = os.getenv("ENTROPY_ARB_DAY_TIMEZONE", "Asia/Taipei")
    sqlite_path = Path(os.getenv(
        "ENTROPY_ARB_SQLITE_PATH", str(root / "entropy_arb.sqlite3")))
    migrate_legacy = os.getenv("ENTROPY_ARB_MIGRATE_LEGACY", "0").lower() \
        not in {"0", "false", "no"}
    # The dashboard is a reader in normal operation.  Keep the one-time CSV
    # migration behind an explicit opt-in so API refreshes cannot compete with
    # the trading process for SQLite's write lock.
    ledger = TradeLedger(
        sqlite_path,
        read_only=sqlite_path.exists() and not migrate_legacy)
    if migrate_legacy:
        ledger.migrate_legacy(
            trades_paths=(trades_path, Path(str(trades_path) + ".old")),
            seconds_paths=(seconds_path, Path(str(seconds_path) + ".old")),
            minutes_paths=(root / "minutes.csv",
                           Path(str(root / "minutes.csv") + ".old")))

    @asynccontextmanager
    async def lifespan(_api: FastAPI):
        try:
            yield
        finally:
            ledger.close()

    api = FastAPI(title="Entropy Arb Dashboard API", version="1.0.0",
                  lifespan=lifespan)
    api.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)
    api.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_methods=["GET"], allow_headers=["*"],
    )

    def normalise_status_trade_count(payload: dict) -> dict:
        """Keep status counts aligned with the deduplicated trade ledger."""
        if not ledger.has_data("trade_attempts"):
            return payload
        today = payload.get("today") or {}
        coverage = (today.get("day_started_ts")
                    or today.get("session_started_ts"))
        until = float(payload.get("server_now") or payload.get("ts") or 0.0)
        summary = ledger.trade_summary(
            since=coverage, until=until, day_timezone=timezone)
        return {
            **payload,
            "today": {**today, "trade_count": summary["trade_count"]},
            "last_trade": summary["last_trade"],
        }

    @api.middleware("http")
    async def disable_api_cache(request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    @api.get("/api/health")
    def health() -> dict:
        return {"ok": True}

    @api.get("/api/status", response_model=StatusResponse)
    def status() -> StatusResponse:
        try:
            return StatusResponse.model_validate(
                normalise_status_trade_count(read_status(status_path)))
        except (SnapshotUnavailable, ValidationError):
            raise HTTPException(
                status_code=503, detail="status snapshot unavailable")

    @api.get("/api/history", response_model=HistoryResponse)
    def history(range: str = Query("1h")) -> dict:
        if range not in RANGE_SECONDS:
            raise HTTPException(status_code=400, detail="range must be 1h, 6h, or 24h")
        if ledger.has_data("market_samples"):
            history_payload = ledger.history(range)
            return {
                **history_payload,
                "points": downsample_points(
                    history_payload["points"], MAX_HISTORY_POINTS,
                    "premium_bps"),
            }
        return read_history(seconds_path, range)

    @api.get("/api/trades", response_model=TradesResponse)
    def trades(limit: int = Query(20, ge=1, le=100),
               offset: int = Query(0, ge=0)) -> dict:
        coverage_started_ts = None
        try:
            today = read_status(status_path)["today"]
            coverage_started_ts = (today.get("day_started_ts")
                                   or today.get("session_started_ts"))
        except (SnapshotUnavailable, KeyError, TypeError):
            pass
        return (ledger.list_trades(limit, coverage_started_ts, offset)
                if ledger.has_data("trade_attempts")
                else read_trades(trades_path, limit, coverage_started_ts,
                                 offset))

    @api.get("/api/pnl", response_model=PnlResponse)
    def pnl(range: str = Query("today")) -> dict:
        if range != "today":
            raise HTTPException(status_code=400, detail="range must be today")
        return (read_pnl_from_ledger(ledger, status_path, timezone)
                if ledger.has_data("pnl_snapshots")
                else read_pnl(seconds_path, status_path, timezone))

    @api.get("/api/dashboard", response_model=DashboardResponse)
    def dashboard(range: str = Query("1h")) -> dict:
        if range not in RANGE_SECONDS:
            raise HTTPException(status_code=400,
                                detail="range must be 1h, 6h, or 24h")
        try:
            payload = read_dashboard(root, ledger, range, timezone,
                                     status_path=status_path)
            payload["status"] = normalise_status_trade_count(
                payload["status"])
            return payload
        except SnapshotUnavailable:
            raise HTTPException(
                status_code=503, detail="status snapshot unavailable")

    dist = Path(__file__).resolve().parents[2] / "dashboard" / "dist"
    if dist.exists():
        assets = dist / "assets"
        if assets.exists():
            api.mount("/assets", StaticFiles(directory=assets), name="assets")

        @api.get("/{path:path}", include_in_schema=False)
        def frontend(path: str):
            requested = dist / path
            if path and requested.is_file() and dist in requested.resolve().parents:
                return FileResponse(requested)
            return FileResponse(dist / "index.html")

    return api


app = create_app()
