"""Opt-in loopback control adapter. The dashboard API remains GET-only."""
from __future__ import annotations

import asyncio
import ipaddress
import json
import os
from pathlib import Path
import secrets
import sys
import tempfile
import time
from urllib.parse import urlparse
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from .v3_readmodel import json_source, journal_source


BLOCKED = "启动、停止和平仓需引擎拥有者控制协议；当前尚未接线，不会发送订单。"
REASONS = {
    'owner_busy': '交易进程仍持有账户锁。请在原终端停止并等待锁释放；不会接管进程。',
    'preview_expired': '确认已过期，请重新核对两腿。',
    'preview_changed': '账户、周期、数量或订单已变化，请重新核对。',
    'valuation_unavailable': '预计毛盈亏不可用；手续费和资金费待成交后核算。',
    'account_owner_busy': '交易进程仍持有账户锁。请在原终端停止并等待锁释放；不会接管进程。',
    'confirmation_expired': '确认已过期，请重新核对两腿。',
    'confirmation_changed': '账户、周期、数量或订单已变化，请重新核对。',
    'pending_open_requires_reconciliation': '仍有未决开仓订单，请先由原管理者核对；不能按账本数量平仓。',
    'order_inflight_requires_reconciliation': '订单提交结果尚未确认，请先核对原订单。',
    'cycle_already_closed': '所选周期已结束，不会发送新订单。',
}


class Target(BaseModel):
    model_config = ConfigDict(extra='forbid')
    source: str = Field(pattern=r'^basis$')
    cycle_id: str = Field(pattern=r'^[A-Za-z0-9][A-Za-z0-9_.:+-]{0,127}$')


class Confirmation(Target):
    preview_id: UUID
    idempotency_key: UUID
    action: str = Field(pattern=r'^close_both_legs$')


class ExitWorker:
    """Only the operator supplies paths/accounts; browser input never selects argv."""
    def __init__(self):
        # The single-shot CLI permanently stops entries. It cannot implement
        # close-preserving-intent; keep HTTP execution disabled until owner IPC.
        self.enabled = False
        self.repo = Path(__file__).resolve().parents[2]
        self.journal = Path(os.getenv('V3_EXIT_JOURNAL', 'logs/v3/journal.sqlite3'))
        self.status = Path(os.getenv('V3_EXIT_STATUS', 'logs/v3/manual-exit-status.json'))
        self.fixed_args = [
            '--symbol', os.getenv('V3_EXIT_SYMBOL', 'SNDK'), '--hedge', 'lighter',
            '--config', os.getenv('V3_EXIT_CONFIG', 'config.yaml'),
            '--env-file', os.getenv('V3_EXIT_ENV_FILE', '.env'),
            '--v3-config', os.getenv('V3_EXIT_V3_CONFIG', 'config.v3.edge-next.json'),
            '--deployment', os.getenv('V3_EXIT_DEPLOYMENT', 'default'), '--live',
            '--batch-authorization', os.getenv('V3_EXIT_AUTHORIZATION', 'logs/v3/batch.authorization.20260923-002.json'),
            '--journal', str(self.journal), '--status-json', str(self.status),
            '--observer-jsonl', os.getenv('V3_EXIT_OBSERVER', 'logs/v3/observer.jsonl'),
            '--reference-checkpoint', os.getenv('V3_EXIT_CHECKPOINT', 'logs/v3/reference_checkpoint.json'),
            '--expected-hl-account', os.getenv('V3_EXIT_HL_ACCOUNT', ''),
            '--expected-hedge-account-index', os.getenv('V3_EXIT_LIGHTER_ACCOUNT_INDEX', ''),
            '--timeout-seconds', '120', '--accounting-wait-seconds', '10',
        ]
        if self.enabled and (not os.getenv('V3_EXIT_HL_ACCOUNT') or not os.getenv('V3_EXIT_LIGHTER_ACCOUNT_INDEX')):
            raise ValueError('explicit public account bindings required')

    async def run(self, cycle_id, *, receipt=None):
        if not self.enabled:
            raise HTTPException(409, BLOCKED)
        with tempfile.TemporaryDirectory(prefix='entropy-exit-confirm-') as directory:
            extra = ['--preview-only']
            if receipt is not None:
                path = Path(directory) / 'confirmation.json'
                path.write_text(json.dumps(receipt), encoding='utf-8')
                path.chmod(0o600)
                extra = ['--confirmation-json', str(path)]
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(self.repo / 'tools/v3_exit_control.py'),
                *self.fixed_args, f'--expected-cycle-id={cycle_id}', *extra,
                cwd=self.repo, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            # A confirmed exit owns its own bounded recovery loop. Never kill it on an HTTP timeout.
            output, _ = await process.communicate()
            try:
                result = json.loads(output.decode().strip().splitlines()[-1])
            except (ValueError, IndexError, UnicodeError):
                raise HTTPException(409, '退出工具未返回可确认结果；请核对持久订单与仓位。')
            if process.returncode == 2:
                reason = result.get('reason', 'preview_changed')
                raise HTTPException(409, REASONS.get(reason, '预检拒绝：请核对账户、周期、订单与数据来源。'))
            return result

    def progress(self, receipt, started_at):
        journal = journal_source(self.repo / self.journal).get('data') or {}
        status = json_source(self.repo / self.status).get('data') or {}
        cid = receipt['cycle_id']
        cycle = next((r for r in journal.get('live_cycles', []) if r['cycle_id'] == cid), {})
        orders = [r for r in journal.get('orders', []) if r['cycle_id'] == cid]
        fresh = status.get('cycle_id') == cid and (status.get('source_ts') or 0) >= started_at
        remaining = status.get('remaining', {}) if fresh else {}
        legs = []
        for venue in ('entropy', 'hedge'):
            pending = [r for r in orders if r.get('venue') == venue and r.get('state') != 'TERMINAL']
            # Ledger quantities are labelled as such; they never become a live account proof.
            units = remaining.get(f'{venue}_units') if remaining.get('authoritative') else cycle.get(f'{venue}_units')
            initial = abs(receipt[f'{venue}_units'])
            baseline = receipt.get('close_fills_baseline', {}).get(venue)
            close_orders = {r['order_ref']: r for r in orders if r.get('venue') == venue and r.get('reduce_only')}
            valid = isinstance(baseline, dict) and all(ref in close_orders for ref in baseline)
            added = 0
            for ref, order in close_orders.items():
                filled, requested = order.get('filled_units'), order.get('requested_units')
                prior = baseline.get(ref, 0) if isinstance(baseline, dict) else 0
                if (type(filled) is not int or type(requested) is not int or type(prior) is not int
                        or not 0 <= prior <= filled <= requested):
                    valid = False
                else:
                    added += filled - prior
            # Cumulative order receipts prove progress. Position differences alone do not prove fills.
            authoritative = bool(remaining.get('authoritative') and units is not None and not pending)
            if valid and authoritative and abs(units) <= initial and added >= initial - abs(units):
                closed = initial - abs(units)
            else:
                closed = added if valid and added <= initial else None
            state = 'unknown' if not valid or any(r.get('state') in ('UNKNOWN', 'SUBMITTING') for r in pending) else 'executing' if pending else 'closed' if authoritative and units == 0 and closed == initial else 'partial' if closed else 'waiting'
            legs.append({'venue': venue, 'initial_units': initial, 'remaining_units': units,
                         'closed_units': closed, 'state': state})
        return {'legs': legs, 'remaining': remaining, 'position_observed_ts': status.get('position_observed_ts') if fresh else None,
                'progress_basis': 'authoritative_exit' if remaining.get('authoritative') else 'ledger',
                'state': 'unknown' if any(leg['state'] == 'unknown' for leg in legs) else 'partial' if any(leg['state'] == 'partial' for leg in legs) else 'executing'}


def create_app(worker=None, *, origin=None):
    worker = worker or ExitWorker()
    origin = origin or os.getenv('V3_EXIT_UI_ORIGIN', 'http://127.0.0.1:5186')
    parsed = urlparse(origin)
    if parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '[::1]', '::1') or parsed.path or parsed.query or parsed.fragment or parsed.username:
        raise ValueError('UI origin must be an exact loopback HTTP origin')
    control_port = os.getenv('V3_EXIT_CONTROL_PORT', '8786')
    hosts = {parsed.netloc, f'127.0.0.1:{control_port}', f'localhost:{control_port}', f'[::1]:{control_port}'}
    sessions, previews, operations, keys = {}, {}, {}, {}
    gate = asyncio.Lock()
    api = FastAPI(title='Entropy Local Exit Control', docs_url=None, redoc_url=None, openapi_url=None)
    api.state.operations = operations

    @api.middleware('http')
    async def local_only(request: Request, call_next):
        try:
            local = ipaddress.ip_address(request.client.host).is_loopback
        except (ValueError, AttributeError):
            local = False
        if not local or request.headers.get('host') not in hosts:
            return Response(status_code=403)
        supplied_origin = request.headers.get('origin')
        if (supplied_origin and supplied_origin != origin) or (request.method == 'POST' and supplied_origin != origin):
            return Response(status_code=403)
        if request.headers.get('sec-fetch-site') in ('cross-site', 'same-site'):
            return Response(status_code=403)
        if request.method == 'POST':
            session = sessions.get(request.cookies.get('entropy_exit_session'))
            if not session or session['expires_at'] < time.time() or not secrets.compare_digest(request.headers.get('x-entropy-csrf', ''), session['csrf']):
                return Response(status_code=403)
            if request.headers.get('content-type', '').split(';')[0] != 'application/json':
                return Response(status_code=415)
            body = await request.body()
            if len(body) > 2048:
                return Response(status_code=413)
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    def session_id(request):
        sid = request.cookies.get('entropy_exit_session')
        if sid not in sessions or sessions[sid]['expires_at'] < time.time():
            raise HTTPException(403, '本地确认会话已过期，请重新打开确认面板。')
        return sid

    @api.get('/api/v3-control/session')
    def session(request: Request, response: Response):
        now = time.time()
        for sid in list(sessions):
            if sessions[sid]['expires_at'] < now:
                del sessions[sid]
        sid = request.cookies.get('entropy_exit_session')
        if sid not in sessions:
            if len(sessions) >= 64:
                raise HTTPException(429, '本地确认会话过多')
            sid = secrets.token_urlsafe(32)
            sessions[sid] = {'csrf': secrets.token_urlsafe(32), 'expires_at': now + 3600}
        response.set_cookie('entropy_exit_session', sid, httponly=True, samesite='strict', path='/api/v3-control', max_age=3600)
        latest = next(((oid, op) for oid, op in reversed(list(operations.items())) if op['sid'] == sid), None)
        operation = None
        if latest:
            receipt = latest[1]['receipt']
            operation = {'operation_id': latest[0], 'cycle_id': receipt['cycle_id'], 'preview': {
                **{k: receipt.get(k) for k in ('cycle_id', 'batch_id', 'quantity_step', 'entropy_units', 'hedge_units', 'expires_at')},
                'preview_id': latest[1]['preview_id'], 'authoritative': True,
                'position_observed_ts': receipt.get('observed_at'), 'account_binding': receipt['account_scope_hash'][:12],
            }}
        return {'enabled': worker.enabled, 'reason': None if worker.enabled else BLOCKED, 'csrf_token': sessions[sid]['csrf'], 'operation': operation}

    @api.post('/api/v3-control/preview')
    async def preview(target: Target, request: Request):
        sid = session_id(request)
        for pid in list(previews):
            if previews[pid]['receipt']['expires_at'] < time.time() and not previews[pid].get('operation_id'):
                del previews[pid]
        if len(previews) >= 64:
            raise HTTPException(429, '预检过多，请等待已有操作完成')
        result = await worker.run(target.cycle_id)
        receipt = result['receipt']
        if receipt['cycle_id'] != target.cycle_id or receipt['expires_at'] <= time.time():
            raise HTTPException(409, '预检结果已过期或周期不匹配')
        pid = str(uuid4())
        previews[pid] = {'sid': sid, 'target': target.model_dump(), 'receipt': receipt}
        public = {k: result['preview'].get(k) for k in (
            'cycle_id', 'batch_id', 'symbol', 'quantity_step', 'entropy_units', 'hedge_units',
            'position_observed_ts', 'authoritative', 'executable_gross_usd', 'valuation_ts', 'fees_usd', 'funding_usd')}
        return {**public, 'preview_id': pid, 'expires_at': receipt['expires_at'], 'account_binding': receipt['account_scope_hash'][:12]}

    @api.post('/api/v3-control/confirm')
    async def confirm(body: Confirmation, request: Request):
        sid = session_id(request)
        async with gate:
            preview = previews.get(str(body.preview_id))
            if not preview or preview['sid'] != sid or preview['target'] != {'source': body.source, 'cycle_id': body.cycle_id}:
                raise HTTPException(409, '确认与预检账户或周期不匹配')
            key = str(body.idempotency_key)
            if key in keys:
                prior_id, prior_preview = keys[key]
                if prior_preview != str(body.preview_id):
                    raise HTTPException(409, '幂等键已用于另一份确认')
                return {'operation_id': prior_id}
            if preview.get('operation_id'):
                return {'operation_id': preview['operation_id']}
            if preview['receipt']['expires_at'] <= time.time():
                raise HTTPException(409, REASONS['preview_expired'])
            if not worker.enabled:
                raise HTTPException(409, BLOCKED)
            prior = next(((oid, op) for oid, op in reversed(list(operations.items()))
                          if op['receipt']['cycle_id'] == body.cycle_id
                          and op['receipt']['account_scope_hash'] == preview['receipt']['account_scope_hash']), None)
            if prior and (prior[1]['active'] or preview['receipt'].get('observed_at', 0) <= prior[1]['started_at']):
                if prior[1]['sid'] != sid:
                    raise HTTPException(409, '已有同账户周期操作，请在原确认会话查看。')
                preview['operation_id'] = prior[0]
                keys[key] = (prior[0], str(body.preview_id))
                return {'operation_id': prior[0]}
            active = next((oid for oid, op in operations.items() if op['active']), None)
            if active:
                raise HTTPException(409, '已有平仓操作正在核对；请等待当前操作，不会启动第二个管理者。')
            if len(operations) >= 128:
                raise HTTPException(429, '本地操作记录已满；请保留账本证据后重启控制展示服务。')
            oid = str(uuid4())
            op = {'sid': sid, 'preview_id': str(body.preview_id), 'receipt': preview['receipt'], 'started_at': time.time(), 'active': True, 'result': None, 'error': None}
            operations[oid] = op
            keys[key] = (oid, str(body.preview_id))
            preview['operation_id'] = oid
            async def execute():
                try:
                    op['result'] = await worker.run(body.cycle_id, receipt=op['receipt'])
                except Exception as exc:
                    op['error'] = exc.detail if isinstance(exc, HTTPException) else '控制响应中断，请核对持久订单；不会自动重新提交。'
                finally:
                    op['active'] = False
            op['task'] = asyncio.create_task(execute())
            return {'operation_id': oid}

    @api.get('/api/v3-control/operations/{operation_id}')
    def operation(operation_id: UUID, request: Request):
        sid = session_id(request)
        op = operations.get(str(operation_id))
        if not op or op['sid'] != sid:
            raise HTTPException(404, '操作不属于当前本地确认会话')
        progress = worker.progress(op['receipt'], op['started_at'])
        result = op['result'] or {}
        final = bool(result.get('success') and result.get('accounting_final'))
        flat = bool(result.get('success') and result.get('remaining', {}).get('authoritative')
                    and result.get('remaining', {}).get('entropy_units') == 0
                    and result.get('remaining', {}).get('hedge_units') == 0
                    and result.get('active_cycle', False) is None and result.get('pending') == [])
        state = 'complete' if flat and final else 'accounting_pending' if flat else progress['state'] if op['active'] else 'partial' if progress['state'] == 'partial' and not result.get('pending') else 'unknown'
        return {**progress, 'operation_id': str(operation_id), 'active': op['active'], 'state': state,
                'accounting_final': final and flat, 'net_pnl_usd': result.get('net_pnl_usd') if flat and final else None,
                'reason': op['error'], 'entries_remain_closed': result.get('entries_remain_closed'),
                'source_ts': time.time()}

    return api
