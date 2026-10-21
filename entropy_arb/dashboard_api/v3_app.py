"""Standalone V3 dashboard API.  It never imports the legacy dashboard app."""
from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
import csv
from collections import OrderedDict
from decimal import Decimal
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
from threading import Lock
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .v3_projection import (Window, compatibility, cycle_detail, cycle_facts,
                            economics, funding_dashboard, market_points, live_market_points,
                            market_coverage, market_diagnostic, overview, public_status,
                            resolve_window, trader_cycle_view)
from .v3_profit import profit_overview
from .v3_deployments import deployment_info, qualify, merge_profit
from .v3_readmodel import SourcePaths, V3ReadModel, number, public_sources


class WindowQuery(BaseModel):
    scope: Literal["session", "batch", "today", "1h", "6h", "24h", "7d", "all", "custom"] = "session"
    from_ts: float | None = None
    to_ts: float | None = None
    timezone: Literal["Asia/Taipei", "UTC"] = "Asia/Taipei"
    batch_id: str | None = Field(default=None, max_length=128)
    session: str | None = Field(default=None, max_length=256)
    pair: str | None = Field(default=None, max_length=128)
    mode: Literal['all', 'basis', 'funding'] = 'all'
    symbol: str | None = Field(default=None, max_length=128)
    source: str | None = Field(default=None, max_length=128)
    view: Literal['all', 'filled', 'complete', 'open', 'incomplete', 'loss', 'profit', 'unfilled'] | None = None


class EnvelopeMeta(BaseModel):
    schema_version: Literal[3] = 3
    snapshot_id: str
    generated_at: float
    window: dict[str, Any] | None = None
    sources: dict[str, Any]
    consistency: Literal["consistent", "transitioning", "partial", "unknown"]
    warnings: list[str]
    source_id: str | None = None
    deployment_windows: dict[str, Any] | None = None


class Envelope(BaseModel):
    meta: EnvelopeMeta
    data: dict[str, Any]


def _decode_cursor(cursor: str) -> dict[str, Any]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        result = json.loads(raw)
        if not isinstance(result, dict):
            raise ValueError("invalid_cursor")
        return result
    except (ValueError, UnicodeError, base64.binascii.Error):
        raise HTTPException(status_code=400, detail="invalid_cursor")


def _encode_cursor(payload: dict[str, Any]) -> str:
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")


def _csv_safe(value: Any) -> str:
    text = "" if value is None else str(value)
    return "'" + text if text.lstrip().startswith(("=", "+", "-", "@", "\t", "\r")) else text


def create_app(paths: SourcePaths | None = None, *, start_collector: bool = True,
               deployment_paths: dict[str, SourcePaths] | None = None) -> FastAPI:
    model = V3ReadModel(paths)
    models = {'basis': model}
    if deployment_paths is not None:
        models = {source: V3ReadModel(value) for source, value in deployment_paths.items()}
        model = next(iter(models.values()))
    elif os.getenv('V3_NBIS_DATA_DIR') or (model.paths.journal.parent / 'nbis' / 'journal.sqlite3').is_file():
        root = Path(os.getenv('V3_NBIS_DATA_DIR', str(model.paths.journal.parent / 'nbis')))
        models['funding'] = V3ReadModel(replace(model.paths, journal=root / 'journal.sqlite3',
            live_status=root / 'status.json', live_manifest=root / 'status.effective_manifest.json',
            live_diagnostics=root / 'dashboard-diagnostics.json', recorder_status=root / 'record-status.edge.json',
            recorder_manifest=root / 'record-status.edge.effective_manifest.json', recorder_data_dir=root,
            reports_dir=model.paths.reports_dir / 'nbis', cache_dir=model.paths.cache_dir / 'nbis'))
    unified = len(models) > 1

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # A single worker owns the collector. Production launch uses --workers 1.
        for deployment in models.values():
            await asyncio.to_thread(deployment.refresh)
        async def collect() -> None:
            while True:
                await asyncio.sleep(2)
                for deployment in models.values():
                    try:
                        await asyncio.to_thread(deployment.refresh)
                    except Exception:
                        # Each deployment retains its own last readable snapshot.
                        pass
        task = asyncio.create_task(collect()) if start_collector else None
        try:
            yield
        finally:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    api = FastAPI(title="Entropy V3 Read-Only Dashboard", version="3.0.0",
                  lifespan=lifespan)
    api.state.v3_read_model = model
    api.state.v3_read_models = models
    api.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)

    @api.middleware("http")
    async def read_only(request: Request, call_next):
        if request.url.path.startswith("/api/v3/") and request.method not in {"GET", "HEAD", "OPTIONS"}:
            return Response(status_code=405, headers={"Allow": "GET, HEAD, OPTIONS"})
        token = os.getenv("DASHBOARD_API_TOKEN")
        if token and request.url.path.startswith("/api/v3/") and request.headers.get(
                "x-entropy-dashboard-token") != token:
            return Response(status_code=401)
        response = await call_next(request)
        if request.url.path.startswith("/api/v3/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    def parsed_window(request: Request, snap: dict[str, Any],
                      *, extras: frozenset[str] = frozenset()) -> Window:
        try:
            allowed = set(WindowQuery.model_fields) | extras
            unexpected = set(request.query_params) - allowed
            if unexpected:
                raise ValueError("unsupported_query_parameter")
            query = WindowQuery.model_validate(dict(request.query_params))
            declared_pair = (snap["sources"].get("live_manifest", {}).get("data")
                             or {}).get("pair_identity")
            preflight = snap['sources'].get('public_preflight', {}).get('data') or {}
            declared_pair = declared_pair or preflight.get('pair_identity')
            if query.pair and query.pair != declared_pair:
                raise ValueError("pair_not_available_from_current_sources")
            live = snap["sources"].get("live", {}).get("data")
            window_context = dict(live or preflight) if live or preflight else None
            if window_context is not None and not window_context.get('batch_id'):
                window_context['batch_id'] = ((snap['sources'].get('live_manifest', {}).get('data')
                                              or {}).get('authorization') or {}).get('batch_id')
            values = query.model_dump()
            if query.batch_id and ':' in query.batch_id and query.batch_id.split(':', 1)[0] in models:
                values['batch_id'] = query.batch_id.split(':', 1)[1]
            return resolve_window(values, window_context,
                                  now=max(dep.snapshot["generated_at"] for dep in models.values()) if unified else snap["generated_at"])
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    def envelope(snap: dict[str, Any], data: dict[str, Any],
                 window: Window | None = None) -> Envelope:
        sources = public_sources(snap)
        journal = sources.get("journal", {})
        live = sources.get("live", {})
        warnings = [f"{name}:{source['quality']}" for name, source in sources.items()
                    if source.get("quality") not in {"available"}]
        consistency: str = "consistent"
        live_row = snap["sources"].get("live", {}).get("data") or {}
        cycle_id = live_row.get("cycle_id")
        if journal.get("quality") != "available" or live.get("quality") != "available":
            consistency = "partial"
        elif cycle_id and not any(r["cycle_id"] == cycle_id
                                  for r in (snap.get("journal") or {}).get("live_cycles", [])):
            consistency = "transitioning"
            warnings.append("live_cycle_not_yet_in_journal")
        return Envelope(meta=EnvelopeMeta(
            snapshot_id=snap["id"], generated_at=snap["generated_at"],
            window=window.public() if window else None, sources=sources,
            consistency=consistency, warnings=warnings), data=data)

    def selected_sources(request: Request):
        try:
            query = WindowQuery.model_validate(dict(request.query_params))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if query.source and query.source not in models:
            raise HTTPException(status_code=400, detail='unknown_deployment_source')
        for source_id, deployment in models.items():
            info = deployment_info(source_id, deployment.snapshot)
            if query.mode != 'all' and info['mode'] != query.mode:
                continue
            if query.symbol and query.symbol not in info['symbols']:
                continue
            if unified and query.pair and query.pair != info['pair_identity']:
                continue
            if query.scope == 'batch' and query.batch_id and ':' in query.batch_id:
                prefix = query.batch_id.split(':', 1)[0]
                if prefix in models and prefix != source_id:
                    continue
            yield source_id, deployment, info

    def request_model(request: Request):
        source_id = request.query_params.get('source')
        if source_id:
            if source_id not in models:
                raise HTTPException(status_code=400, detail='unknown_deployment_source')
            return models[source_id]
        selected = list(selected_sources(request))
        if len(selected) == 1:
            return selected[0][1]
        if unified:
            raise HTTPException(status_code=400, detail='source_required_for_deployment_evidence')
        return model

    def root_envelope(parts, data, windows=None):
        snaps = [deployment.snapshot for deployment in models.values()]
        meta = EnvelopeMeta(snapshot_id='|'.join(f'{key}:{models[key].snapshot["id"]}' for key in models),
            generated_at=max(s['generated_at'] for s in snaps), sources={
                f'{source_id}.{name}': value for source_id, _dep, info in parts for name, value in info['source_health'].items()},
            consistency='partial' if any(info['source_health'].get('journal', {}).get('quality') != 'available' for _, _, info in parts) else 'consistent',
            warnings=[f'{source_id}.{name}:{value.get("quality")}' for source_id, _dep, info in parts
                      for name, value in info['source_health'].items() if value.get('quality') != 'available'],
            deployment_windows=windows)
        return Envelope(meta=meta, data=data)

    def view_rows(rows, view, sort='latest'):
        if view == 'incomplete':
            return trader_cycle_view([row for row in rows if row.get('phase') == 'DONE'
                                      and row.get('has_confirmed_fill') and not row.get('paired_round_trip')], 'all', sort)
        return trader_cycle_view(rows, view, sort)

    @api.get('/api/v3/deployments', response_model=Envelope)
    def deployments() -> Envelope:
        parts = [(key, dep, deployment_info(key, dep.snapshot)) for key, dep in models.items()]
        return root_envelope(parts, {'deployments': [info for _, _, info in parts]})

    @api.get('/api/v3/health', response_model=Envelope)
    def health(request: Request) -> Envelope:
        parts = list(selected_sources(request))
        if unified and not request.query_params.get('source') and len(parts) != 1:
            return root_envelope(parts, {'api': {'status': 'available'},
                                        'deployments': [info for _, _, info in parts]})
        selected = request_model(request)
        snap = selected.snapshot
        return envelope(snap, {'api': {'status': 'available', 'observed_at': snap['generated_at']},
                               'source_health': public_sources(snap)})

    @api.get('/api/v3/overview', response_model=Envelope)
    def get_overview(request: Request) -> Envelope:
        parts = list(selected_sources(request))
        profits, views, windows = [], [], {}
        counts = dict(complete=0, open=0, incomplete=0, unfilled=0)
        missing = 0
        view = request.query_params.get('view') or 'complete'
        for source_id, dep, info in parts:
            snap = dep.snapshot
            try:
                window = parsed_window(request, snap)
            except HTTPException as exc:
                if exc.detail != 'session_start_unavailable':
                    raise
                missing += 1
                views.append({**info, 'overview': None, 'window': None, 'reason': exc.detail})
                continue
            windows[source_id] = window.public()
            facts = cycle_facts(snap, window)
            full = overview(snap, window)
            full_profit = full.get('profit') or {}
            if not facts.get('available'):
                missing += 1
            profit_ids = {row['cycle_id'] for row in full_profit.get('cycles', [])}
            cohort = {row['cycle_id']: row for row in facts.get('items', [])
                      if window.scope == 'all' or row['cycle_id'] in profit_ids or window.contains(row.get('started_ts')) or row.get('phase') != 'DONE'}
            for category in counts:
                counts[category] += len(view_rows(list(cohort.values()), category))
            filtered_facts = {**facts, 'items': view_rows(facts.get('items', []), view)}
            profit = profit_overview(snap, window, filtered_facts)
            if profit.get('available'):
                profit['cycles'] = qualify(profit['cycles'], info)
                profit['holding'] = full_profit.get('holding', {'cycles': [], 'cycle_count': 0})
                profit['holding']['cycles'] = qualify(profit['holding']['cycles'], info)
                profits.append(profit)
            views.append({**info, 'overview': full, 'window': window.public()})
        profit = merge_profit(profits, unavailable=missing)
        profit.update(view=view, counts=counts)
        if not unified and views and views[0].get('overview') is not None:
            return envelope(model.snapshot, {**views[0]['overview'], 'profit': profit, 'deployments': views}, window)
        return root_envelope(parts, {'profit': profit, 'deployments': views}, windows)

    # Only compact display results/complete episode summaries are retained.
    # Each deployment/series has two keys. Bounds are the actual stored range,
    # so a moving clock does not force re-quoting unchanged historical frames.
    market_projections = {}
    market_projection_locks = {}

    def build_market_projection(selected_model, snap, window, pair, max_points, units=None):
        live = units is None
        tail = selected_model.live_market if live else selected_model.market
        project = live_market_points if live else market_points
        kwargs = {'max_points': max_points, 'pair': pair}
        if not live:
            kwargs['units'] = units
        try:
            signature = tail.window_signature(window.from_ts, window.to_ts)
            bindings = tuple((name, json.dumps(snap['sources'].get(name, {}).get('data') or {},
                sort_keys=True, default=str)) for name in ('live_manifest', 'recorder_manifest')) if not live else ()
            key = (signature, units, max_points, pair, bindings)
            cache = market_projections.setdefault((id(selected_model), live), OrderedDict())
            if key in cache:
                result = dict(cache[key])
                cache.move_to_end(key)
                old = result['coverage']
                result['coverage'] = market_coverage(snap, window,
                    source_name='live_market' if live else 'market', loaded_rows=old['loaded_rows'],
                    first_ts=old['from_ts'], last_ts=old['to_ts'], discontinuities=old['discontinuity_count'])
                result['coverage']['display_points'] = old['display_points']
            else:
                result = project(snap, window, cached_rows=tail.iter_window(window.from_ts, window.to_ts, pair),
                    cached_row_count=signature[1], **kwargs)
                cache[key] = result
                while len(cache) > 2:
                    cache.popitem(last=False)
            if not live:
                result = {**result, 'diagnostic': market_diagnostic(snap)}
            return result
        except (OSError, sqlite3.Error, ValueError, json.JSONDecodeError) as exc:
            result = project(snap, window, **kwargs)
            if tail.cache_error or result['coverage']['loaded_rows']:
                result['coverage'].update(quality='partial', reason='dashboard_cache_read_failed',
                    cache_read_error=type(exc).__name__)
                result['coverage']['reasons'].append('dashboard_cache_read_failed')
            return result

    def market_projection(selected_model, snap, window, pair, max_points, units=None):
        # A timed-out HTTP caller cannot stop synchronous work already running
        # in a worker. Share one cold computation instead of piling up copies.
        lock = market_projection_locks.setdefault((id(selected_model), units is None), Lock())
        with lock:
            return build_market_projection(selected_model, snap, window, pair, max_points, units)

    def unrequested_coverage(snap, window, source_name):
        return {'quality': 'not_requested', 'loaded_rows': None, 'display_points': 0,
            'requested_from_ts': window.from_ts, 'requested_to_ts': window.to_ts,
            'from_ts': None, 'to_ts': None, 'latest_ts': snap['sources'].get(source_name, {}).get('observed_at'),
            'backfill_complete': snap['sources'].get(source_name, {}).get('backfill_complete'),
            'discontinuity_count': None, 'reason': 'series_not_requested', 'reasons': ['series_not_requested']}

    @api.get("/api/v3/market", response_model=Envelope)
    def get_market(request: Request, units: int | None = Query(None, ge=1, le=1_000_000),
                   max_points: int = Query(1200, ge=50, le=1200),
                   series: Literal['all', 'fixed', 'live'] = Query('all')) -> Envelope:
        selected_model = request_model(request)
        snap = selected_model.snapshot
        window = parsed_window(request, snap, extras=frozenset({"units", "max_points", "series"}))
        pair = request.query_params.get("pair")
        units_source = "custom_display_units"
        if units is None:
            journal = snap.get("journal") or {}
            cycles = sorted(journal.get("live_cycles", []),
                            key=lambda row: row.get("started_ts") or 0, reverse=True)
            active = next((row for row in cycles if row.get("phase") != "DONE"), None)
            if active is not None:
                paired = min(abs(int(active.get("entropy_units") or 0)),
                             abs(int(active.get("hedge_units") or 0)))
                if paired:
                    units, units_source = paired, "current_paired_position_units"
            if units is None and cycles and int(cycles[0].get("planned_units") or 0) > 0:
                units, units_source = int(cycles[0]["planned_units"]), "recent_live_plan_units"
            if units is None:
                latest_market = snap.get("market", [])[-1] if snap.get("market") else {}
                minimum = number(latest_market.get("minimum_entry_units"))
                units = int(minimum) if minimum is not None and 0 < minimum <= 1_000_000 else 1
                units_source = "recorder_minimum_display_units"
        if series != 'live':
            result = market_projection(selected_model, snap, window, pair, max_points, units)
        else:
            result = {'units': units, 'points': [], 'episodes': [], 'episodes_truncated': False,
                'coverage': unrequested_coverage(snap, window, 'market'), 'diagnostic': None,
                'quantity_step': None, 'quantity_step_source': 'not_requested', 'base_quantity': None,
                'entry_band_bps': None, 'entry_band_source': 'not_requested', 'display_entry_band_bps': None}
        live_result = market_projection(selected_model, snap, window, pair, max_points) if series != 'fixed' else {
            'points': [], 'coverage': unrequested_coverage(snap, window, 'live_market')}
        result = {**result, 'live_points': live_result['points'], 'live_coverage': live_result['coverage'],
            'series_sources': {'fixed_q': {'kind': 'recorder_fixed_q', 'depth': True,
                'requested': series != 'live',
                'decision_link': 'unproven', 'entry_band_source': result['entry_band_source'],
                'entry_band_bps': result['entry_band_bps'],
                'display_entry_band_bps': result['display_entry_band_bps']},
                'live_summary': {'kind': 'live_market_summary', 'depth': False, 'decision_link': 'unproven',
                    'requested': series != 'fixed',
                    'continuity_evidence': 'saved_startup_file_session_and_timestamp_boundaries_only',
                    'reconnect_coverage': 'unknown_when_not_recorded'}}}
        result["units_source"] = units_source
        return envelope(snap, result, window)

    @api.get("/api/v3/cycles", response_model=Envelope)
    def get_cycles(request: Request, limit: int = Query(50, ge=1, le=100),
                   cursor: str | None = Query(None, max_length=1024),
                   direction: Literal["long", "short"] | None = None,
                   status: Literal["open", "closed"] | None = None,
                   view: Literal["all", "filled", "complete", "open", "loss",
                                 "profit", "unfilled", "incomplete"] = "all",
                   sort: Literal["latest", "loss", "profit", "hold"] = "latest",
                   flag: Literal["zero_fill", "repair", "unknown", "long_hold",
                                 "accounting_pending"] | None = None) -> Envelope:
        parts = list(selected_sources(request)) if unified else [('basis', model, deployment_info('basis', model.snapshot))]
        items, windows, missing = [], {}, False
        for source_id, dep, info in parts:
            snap = dep.snapshot
            try:
                window = parsed_window(request, snap, extras=frozenset(
                    {'limit', 'cursor', 'direction', 'status', 'flag', 'view', 'sort'}))
            except HTTPException as exc:
                if exc.detail != 'session_start_unavailable' or not unified:
                    raise
                missing = True
                continue
            windows[source_id] = window.public()
            facts = cycle_facts(snap, window)
            if not facts['available']:
                missing = True
                continue
            profit_ids = {row['cycle_id'] for row in profit_overview(snap, window, facts).get('cycles', [])}
            cohort = {row['cycle_id']: row for row in facts['items']
                      if window.scope == 'all' or row['cycle_id'] in profit_ids or window.contains(row.get('started_ts')) or row.get('phase') != 'DONE'}
            items.extend(qualify(cohort.values(), info))
        snap = model.snapshot
        window = next(iter(windows.values()), None)
        if direction:
            items = [row for row in items if direction in str(row.get("direction") or "").lower()]
        if status:
            items = [row for row in items if (row.get("phase") == "DONE") == (status == "closed")]
        if flag == "zero_fill":
            items = [row for row in items if row.get("terminal_zero_fill_campaign")
                     or row.get("result_kind") == "zero_fill"]
        elif flag == "repair":
            items = [row for row in items if row.get("result_kind") in {"repair", "unwound"}]
        elif flag == "unknown":
            items = [row for row in items if row.get("had_unknown") is True
                     or row.get("opening_unknown_attempts", 0) > 0]
        elif flag == "long_hold":
            items = [row for row in items if (row.get("hold_seconds") or 0) > 600]
        elif flag == "accounting_pending":
            items = [row for row in items if row.get("completed_ts")
                     and not row.get("accounting_final")]
        items = view_rows(items, view, sort)
        normalized = dict(request.query_params)
        normalized.pop("cursor", None)
        normalized.pop("limit", None)
        normalized.pop("source", None)  # Selection context does not change the global cohort.
        fingerprint = hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()[:16]
        offset = 0
        if cursor:
            decoded = _decode_cursor(cursor)
            after = decoded.get("after")
            if decoded.get("query") != fingerprint or not isinstance(after, str):
                raise HTTPException(status_code=400, detail="invalid_cursor")
            offset = next((index + 1 for index, row in enumerate(items)
                           if f'{row["source_id"]}:{row["cycle_id"]}' == after), -1)
            if offset < 0:
                raise HTTPException(status_code=409, detail="cursor_expired_refresh")
        next_cursor = (_encode_cursor({"query": fingerprint,
                                      "after": f'{items[offset + limit - 1]["source_id"]}:{items[offset + limit - 1]["cycle_id"]}'})
                       if offset + limit < len(items) else None)
        result = {'items': items[offset:offset + limit], 'next_cursor': next_cursor, 'total': len(items),
                  'reason': 'partial_deployment_coverage' if missing else None}
        if unified:
            return root_envelope(parts, result, windows)
        return envelope(snap, result, parsed_window(request, snap, extras=frozenset(
            {'limit', 'cursor', 'direction', 'status', 'flag', 'view', 'sort'})))

    @api.get("/api/v3/cycles/{cycle_id}", response_model=Envelope)
    def get_cycle(cycle_id: str, request: Request) -> Envelope:
        if len(cycle_id) > 128 or "/" in cycle_id or "\\" in cycle_id:
            raise HTTPException(status_code=400, detail="invalid_cycle_id")
        if set(request.query_params) - set(WindowQuery.model_fields):
            raise HTTPException(status_code=400, detail="unsupported_query_parameter")
        if request.query_params:
            try:
                query = WindowQuery.model_validate(dict(request.query_params))
                declared_pair = (request_model(request).snapshot["sources"].get("live_manifest", {}).get("data")
                                 or {}).get("pair_identity")
                if query.pair and query.pair != declared_pair:
                    raise ValueError("pair_not_available_from_current_sources")
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))
        selected_model = request_model(request)
        snap = selected_model.snapshot
        detail = cycle_detail(snap, cycle_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="cycle_unavailable")
        source_id = next(key for key, dep in models.items() if dep is selected_model)
        detail['cycle'] = qualify([detail['cycle']], deployment_info(source_id, snap))[0]
        full_window = Window('all', None, None, 'Asia/Taipei')
        facts = cycle_facts(snap, full_window)
        profit = profit_overview(snap, full_window, facts)
        detail['profit'] = next((row for row in qualify(profit.get('cycles', []), deployment_info(source_id, snap)) if row['cycle_id'] == cycle_id), None)
        return envelope(snap, detail, parsed_window(request, snap) if request.query_params.get('scope') else None)

    @api.get("/api/v3/execution", response_model=Envelope)
    def get_execution(request: Request) -> Envelope:
        selected_model = request_model(request)
        snap = selected_model.snapshot
        window = parsed_window(request, snap)
        journal = snap.get("journal")
        if journal is None:
            return envelope(snap, {"summary": None, "groups": [],
                                   "reason": "journal_unavailable"}, window)
        selected = {r["cycle_id"] for r in journal["live_cycles"]
                    if (window.scope == 'all' or window.contains(r.get("started_ts")))
                    and (window.batch_id is None or r.get("batch_id") == window.batch_id)}
        groups = []
        for cid in selected:
            detail = cycle_detail(snap, cid)
            if detail:
                groups.extend({"cycle_id": cid, **g} for g in detail["groups"])
        submitted = [r for g in groups for r in g["attempts"] if r.get("submitted")]
        terminal_zero = sum(r.get("order_outcome") == "terminal_zero" for r in submitted)
        positive = sum(r.get("order_outcome") == "terminal_positive" for r in submitted)
        compact_groups = [{"cycle_id": g["cycle_id"], "group_id": g["group_id"],
                           "kind": g["kind"], "attribution": g["attribution"],
                           "attempt_count": len(g["attempts"]),
                           "terminal_zero_attempts": sum(
                               r.get("order_outcome") == "terminal_zero" for r in g["attempts"])}
                          for g in groups]
        durations: list[Decimal] = []
        missing_timing = negative_timing = 0
        clocks: dict[str, int] = {}
        for row in submitted:
            begin_mono, end_mono = number(row.get("submit_start_mono_ns")), number(
                row.get("terminal_observed_mono_ns"))
            if (begin_mono is not None and end_mono is not None and
                    row.get("clock_session_id") and
                    row.get("clock_session_id") == row.get("terminal_clock_session_id")):
                delta, clock = (end_mono - begin_mono) / 1_000_000, "monotonic_ns"
            else:
                begin, end = number(row.get("submit_ts")), number(row.get("terminal_observed_ts"))
                if begin is None or end is None:
                    missing_timing += 1
                    continue
                delta, clock = (end - begin) * 1000, "wall_utc"
            if delta < 0:
                negative_timing += 1
                continue
            durations.append(delta)
            clocks[clock] = clocks.get(clock, 0) + 1
        durations.sort()
        interleg: list[Decimal] = []
        interleg_missing = 0
        for group in groups:
            first = next((r for r in group["attempts"] if r.get("venue") == "entropy"
                          and r.get("order_outcome") == "terminal_positive"), None)
            hedge = next((r for r in group["attempts"] if r.get("venue") == "hedge"
                          and r.get("submitted")), None)
            if first is None:
                continue
            if hedge is None:
                interleg_missing += 1
                continue
            a, b = number(first.get("terminal_observed_mono_ns")), number(
                hedge.get("submit_start_mono_ns"))
            if (a is not None and b is not None and first.get("terminal_clock_session_id")
                    == hedge.get("clock_session_id") and first.get("terminal_clock_session_id")):
                delta = (b - a) / 1_000_000
            else:
                a, b = number(first.get("terminal_observed_ts")), number(hedge.get("submit_ts"))
                if a is None or b is None:
                    interleg_missing += 1
                    continue
                delta = (b - a) * 1000
            if delta >= 0:
                interleg.append(delta)
            else:
                interleg_missing += 1
        interleg.sort()
        def percentile(p: int) -> str | None:
            if not durations:
                return None
            position = Decimal(p) / 100 * (len(durations) - 1)
            lower = int(position)
            upper = min(lower + 1, len(durations) - 1)
            return str(durations[lower] + (durations[upper] - durations[lower]) *
                       (position - lower))
        return envelope(snap, {"summary": {"submitted_attempts": len(submitted),
                                            "terminal_zero_attempts": terminal_zero,
                                            "terminal_positive_attempts": positive,
                                            "terminal_ioc_positive_rate": str(Decimal(positive) / (positive + terminal_zero))
                                            if positive + terminal_zero else None,
                                            "submit_to_terminal_ms": {
                                                "count": len(durations), "eligible_count": len(submitted),
                                                "p50": percentile(50), "p90": percentile(90),
                                                "p95": percentile(95), "missing_count": missing_timing,
                                                "negative_count": negative_timing,
                                                "clock_counts": clocks},
                                            "entropy_to_hedge_submit_ms": {
                                                "count": len(interleg),
                                                "p50": str(interleg[len(interleg) // 2]) if interleg else None,
                                                "p90": str(interleg[min(len(interleg) - 1,
                                                                           int((len(interleg) - 1) * .9))])
                                                if interleg else None,
                                                "missing_count": interleg_missing}},
                               "groups": compact_groups[:200],
                               "truncated": len(groups) > 200}, window)

    @api.get("/api/v3/economics", response_model=Envelope)
    def get_economics(request: Request) -> Envelope:
        selected_model = request_model(request)
        snap = selected_model.snapshot
        window = parsed_window(request, snap)
        return envelope(snap, economics(snap, window), window)

    @api.get("/api/v3/system", response_model=Envelope)
    def get_system(request: Request) -> Envelope:
        selected_model = request_model(request)
        snap = selected_model.snapshot
        window = parsed_window(request, snap)
        source = snap["sources"]
        live = public_status(source.get("live", {}).get("data"))
        rec = public_status(source.get("recorder", {}).get("data"))
        manifest = source.get("live_manifest", {}).get("data") or {}
        rec_manifest = source.get("recorder_manifest", {}).get("data") or {}
        journal = snap.get("journal")
        batch_id = (live or {}).get("batch_id") or (manifest.get('authorization') or {}).get('batch_id')
        batch = next((r for r in (journal or {}).get("batches", [])
                      if r["batch_id"] == batch_id), None)
        if batch is not None:
            batch = {**batch, "position_cycles": (live or {}).get("position_cycles")
                     if (live or {}).get('position_cycles') is not None else sum(
                         r.get('result_kind') != 'zero_fill' for r in journal.get('cycles', [])
                         if r.get('batch_id') == batch_id),
                     "loss_used_usd": (live or {}).get("loss_used_usd"),
                     "loss_budget_usd": batch.get("loss_budget")}
        # Whitelist only effective non-secret fields; never return manifest paths.
        settings = manifest.get("settings") or {}
        configured = manifest.get("configured_settings") or {}
        authorization = manifest.get("authorization") or {}
        signal_keys = {"reference_half_life_seconds", "warm_samples", "cold_samples",
                       "entry_band_bps", "execution_allowance_bps", "sampling_floor_bps"}
        execution_keys = {"open_first_rooms_bps", "open_first_hard_cap_bps",
                          "close_first_rooms_bps", "close_first_hard_cap_bps",
                          "hedge_rooms_bps", "hedge_hard_cap_bps",
                          "entropy_orders_per_min", "hedge_orders_per_min"}
        exit_keys = {"exit_reversion_bps", "exit_loss_allowance_usd",
                     "single_cycle_review_loss_usd", "max_notional_usd"}
        def display(value: Any) -> str | int | float | bool | None:
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False)
            return value
        config = []
        for key in sorted(set(settings) | set(configured)):
            group = ("资金费对冲" if key.startswith('funding_') or key == 'strategy_mode'
                     else "信号" if key in signal_keys else "执行" if key in execution_keys
                     else "退出与风险" if key in exit_keys else "数据与实验")
            authorized = authorization.get(key) if key in {"max_notional_usd"} else None
            config.append({"group": group, "key": key, "configured": display(configured.get(key)),
                           "authorized": display(authorized), "effective": display(settings.get(key)),
                           "source": "live_effective_manifest"})
            if key == "single_cycle_review_loss_usd":
                config[-1].update(configured_present=key in configured,
                                  effective_present=key in settings)
        builds = {"live": manifest.get("build_hash"),
                  "recorder": rec_manifest.get("build_hash")}
        capabilities = [
            {"name": "live 决策诊断", "status": "has_data" if source.get(
                "live_diagnostics", {}).get("quality") == "available" else "not_recorded",
             "reason": source.get("live_diagnostics", {}).get("reason")},
            {"name": "Journal economics schema 8", "status": "supported" if source.get(
                "journal", {}).get("schema") == 8 else "unsupported_or_missing"},
            {"name": "recorder schema 2", "status": "has_data" if source.get(
                "market", {}).get("schema") == 2 else "unknown_or_legacy"},
            {"name": "退出 fast path", "status": "enabled" if settings.get(
                "exit_fast_path_enabled") else "disabled"},
        ]
        return envelope(snap, {"sources": public_sources(snap),
                               "live": live, "recorder": rec,
                               "batch": batch, "config": config, "builds": builds,
                               "compatibility": compatibility(manifest, rec_manifest),
                               "funding": funding_dashboard(snap),
                               "capabilities": capabilities,
                               "warnings": []}, window)

    @api.get("/api/v3/research", response_model=Envelope)
    def get_research(request: Request) -> Envelope:
        selected_model = request_model(request)
        snap = selected_model.snapshot
        try:
            window = parsed_window(request, snap)
        except HTTPException as exc:
            if exc.detail != "session_start_unavailable":
                raise
            # Report availability must not depend on a currently readable
            # live session. Keep the window unknown rather than inventing one.
            window = None
        reports = []
        directory = selected_model.paths.reports_dir
        try:
            paths = sorted(directory.glob("*.json"), key=lambda p: p.stat().st_mtime,
                           reverse=True)[:30]
            for path in paths:
                if not path.is_file() or path.stat().st_size > 2_000_000:
                    continue
                value = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(value, dict):
                    continue
                reports.append({"id": path.stem, "name": path.name,
                                "title": path.stem.replace("_", " "),
                                "generated_at": value.get("generated_at"),
                                "status": value.get("status") or value.get("quality"),
                                "sample_count": value.get("sample_count")})
        except (OSError, ValueError):
            pass
        return envelope(snap, {"reports": reports,
                               "reason": None if reports else "INSUFFICIENT_DATA"}, window)

    @api.get("/api/v3/diagnostic-export")
    def diagnostic_export(request: Request,
                          format: Literal["json", "csv"] = "json",
                          cycle_id: str | None = Query(None, max_length=128)) -> Response:
        selected_model = request_model(request)
        snap = selected_model.snapshot
        window = parsed_window(request, snap, extras=frozenset({"format", "cycle_id"}))
        facts = cycle_facts(snap, window)
        selected = [row for row in facts.get("items" if cycle_id else "created", [])
                    if cycle_id is None or row["cycle_id"] == cycle_id]
        items = selected[:1000]
        records = [{key: row.get(key) for key in (
            "cycle_id", "batch_id", "direction", "phase", "started_ts", "completed_ts",
            "result_kind", "entropy_units", "hedge_units", "paired_opened",
            "paired_round_trip", "submitted_open_campaign",
            "terminal_zero_fill_campaign", "accounting_final", "pnl_usd", "inherited")}
            for row in items]
        export = {"snapshot_id": snap["id"], "as_of": snap["generated_at"],
                  "window": window.public(), "sources": public_sources(snap),
                  "records": records, "truncated": len(selected) > 1000}
        if format == "json":
            return Response(json.dumps(export, ensure_ascii=False), media_type="application/json")
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=list(records[0]) if records else ["cycle_id"])
        writer.writeheader()
        for row in records:
            writer.writerow({key: _csv_safe(value) for key, value in row.items()})
        return Response(out.getvalue(), media_type="text/csv; charset=utf-8")

    # Static frontend is served by this process locally; Sites can proxy only
    # the /api/v3/* allowlist when its own access control is configured.
    dist = Path(__file__).resolve().parents[2] / "dashboard" / "dist"
    if dist.is_dir():
        assets = dist / "assets"
        if assets.is_dir():
            api.mount("/assets", StaticFiles(directory=assets), name="assets")
        @api.get("/")
        def index() -> FileResponse:
            return FileResponse(dist / "index.html")
    return api
