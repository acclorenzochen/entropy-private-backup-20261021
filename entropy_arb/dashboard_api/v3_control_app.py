"""Independent loopback HTTP adapter for the single engine owner's v1 protocol.

This adapter never launches an engine, invokes a CLI, or places orders. The
configured owner persists intent, order recovery and idempotency.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from decimal import Decimal
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import time
from typing import Literal
from urllib.parse import urlparse
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator

from entropy_arb.v3.parameter_config import (LEGACY_PARAMETER_FIELDS,
    OPENING_PARAMETER_FIELDS, PARAMETER_FIELDS, validate_parameters)


MAX_FRAME = 16384
BINDING_FIELDS = ('account_binding_hash', 'pair_identity', 'deployment', 'batch_id', 'config_hash')
STATE_FIELDS = ('owner_instance_id', 'source_ts', 'intent', 'intent_revision', 'phase',
                'allowed_actions', 'risk_locks', 'blocked_reason', 'active_cycle_id',
                'entropy_units', 'hedge_units', 'quantity_step', 'position_observed_ts',
                'position_authoritative', 'order_watermark', 'pair_identity', 'deployment',
                'batch_id', 'config_hash', 'parameters')
EVIDENCE_FIELDS = ('intent', 'intent_revision', 'phase', 'risk_locks', 'blocked_reason',
                   'active_cycle_id', 'entropy_units', 'hedge_units', 'position_observed_ts',
                   'position_authoritative', 'quantity_step', 'order_watermark', 'allowed_actions',
                   'cycle_id', 'accounting_final', 'net_pnl_usd', 'fees_complete',
                   'known_fees_usd', 'updated_ts', 'source_ts')
MESSAGES = {
    'owner_unavailable': '当前引擎没有可用的控制 owner；不能从此处启动进程或发送交易指令。',
    'control_not_configured': '本地控制未启用；当前仅展示，不会发送交易指令。',
    'unsupported_source': '该标的尚未接入 owner 控制协议。',
    'unsafe_socket_path': 'owner socket 的本地权限不符合要求，控制不可用。',
    'binding_mismatch': 'owner 账户、标的、部署、批次或配置不匹配。',
    'owner_instance_mismatch': 'owner 实例已变化，请重新预检。',
    'intent_revision_changed': '运行意图已变化，请重新预检。',
    'risk_locked': '既有风险锁阻止启动；此操作不会解除锁。',
    'action_unavailable': '当前状态不允许该动作。',
    'cycle_changed': '当前周期已变化，请重新预检。',
    'position_unavailable': '当前两腿仓位未知，请等待 owner 核对。',
    'position_evidence_stale': '当前仓位证据过期，请等待 owner 核对。',
    'order_watermark_unavailable': '当前订单水位未知，请等待 owner 核对。',
    'close_evidence_changed': '两腿数量或订单水位已变化，请重新预检。',
    'receipt_unavailable': '预检已失效，请重新预检。',
    'receipt_expired': '预检超过 15 秒，请重新预检。',
    'receipt_mismatch': '确认与原预检不匹配。',
    'preview_unavailable': '本地预检不存在或不属于当前确认会话。',
    'idempotency_conflict': '该幂等键已用于另一份确认。',
    'operation_not_found': 'owner 尚未找到该动作回执。',
    'outcome_unknown': '控制响应中断，结果未知；请用原幂等键核对，不能另建动作。',
    'owner_operation_failed': 'owner 处理响应异常，请用原幂等键核对持久回执。',
    'invalid_or_busy_frame': 'owner 响应无效或繁忙，请用原幂等键核对持久回执。',
    'invalid_owner_response': 'owner 返回的证据无法验证，控制不可用。',
    'session_expired': '本地确认会话已过期，请重新打开确认面板。',
    'capacity_exceeded': '本地确认记录已满，请核对已有动作。',
    'parameters_unavailable': '当前 owner 不支持参数版本；请先应用候选并重新启动本地进程。',
    'invalid_parameters': '参数必须有效且在原授权范围内；开仓让价允许为零或留空继承。',
    'opening_parameters_unavailable': '当前 owner 不支持开仓让价参数，请保留继承原策略。',
    'invalid_parameters_revision': '参数版本无效，请刷新后重试。',
    'parameters_revision_changed': '已保存参数版本已变化，请刷新并重新预检。',
    'parameters_require_stopped_flat': '仅在已停止、权威双腿空仓且无待处理订单时可保存或启用参数。',
    'parameter_save_not_found': 'owner 尚未找到该参数保存回执；不会自动重新保存。',
    'parameters_base_mismatch': '参数记录与本次基础配置不匹配，控制不可用。',
    'parameters_require_stopped': '请先停止；参数只能在已停止且双腿空仓时保存或启用。',
    'parameters_active_cycle': '仍有活动周期；请等待平仓与停止完成。',
    'parameters_pending_orders': '仍有待处理订单；请等待 owner 核对。',
    'parameters_position_refresh_required': '权威仓位证据暂不可用，请等待 owner 核对。',
    'parameters_require_flat': '双腿尚未归零，不能保存或启用参数。',
    'parameters_idempotency_payload_changed': '该保存键已用于另一份参数，不能覆盖原回执。',
    'saved_parameters_require_explicit_start': '存在已保存待启用参数，请刷新并确认对应版本。',
    'parameters_initialization_requires_flat': '首次迁移仍有持仓或待处理订单，请先在旧 owner 完成归零。',
}
for _parameter_name in PARAMETER_FIELDS:
    MESSAGES['invalid_parameter_' + _parameter_name] = MESSAGES['invalid_parameters']
    MESSAGES['parameter_out_of_bounds_' + _parameter_name] = MESSAGES['invalid_parameters']
MESSAGES['invalid_parameters_fields'] = MESSAGES['invalid_parameters']


class Refusal(Exception):
    def __init__(self, code, status=409):
        self.code, self.status = code, status


def fail(code, status=409, state=None):
    detail = {'code': code, 'message': MESSAGES.get(code, 'owner 拒绝该动作，请核对当前状态。')}
    if state is not None:
        detail['state'] = public_state(state)
    raise HTTPException(status, detail)


def canonical_uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def encode_frame(payload):
    raw = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode() + b'\n'
    if len(raw) > MAX_FRAME:
        raise Refusal('invalid_owner_response', 503)
    return raw


def decode_frame(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError('duplicate field')
            value[key] = item
        return value
    def invalid_constant(_):
        raise ValueError('nonfinite number')
    try:
        if len(raw) > MAX_FRAME or not raw.endswith(b'\n'):
            raise ValueError('invalid frame')
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)
        if type(value) is not dict:
            raise ValueError('invalid object')
        return value
    except (ValueError, UnicodeError):
        raise Refusal('invalid_owner_response', 503) from None


class SocketOwner:
    """Fixed trusted configuration; no browser input can select a socket or scope."""
    def __init__(self):
        self.enabled = os.getenv('V3_OWNER_CONTROL_ENABLED') == '1'
        self.binding = {key: os.getenv(f'V3_OWNER_{key.upper()}', '') for key in BINDING_FIELDS}
        self.path = Path(os.environ['V3_OWNER_SOCKET']) if os.getenv('V3_OWNER_SOCKET') else None
        if self.enabled and (self.path is None or not self.path.is_absolute()
                             or not all(self.binding.values())):
            raise ValueError('explicit owner socket and all trusted binding fields are required')

    def socket_identity(self):
        try:
            parent, info = self.path.parent.lstat(), self.path.lstat()
        except OSError:
            raise Refusal('owner_unavailable', 503) from None
        if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
                or stat.S_IMODE(parent.st_mode) != 0o700
                or not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600):
            raise Refusal('unsafe_socket_path', 503)
        return info.st_dev, info.st_ino

    async def request(self, payload):
        if not self.enabled:
            raise Refusal('control_not_configured', 503)
        identity = self.socket_identity()
        raw = encode_frame(payload)
        async def exchange():
            reader, writer = await asyncio.open_unix_connection(self.path, limit=MAX_FRAME + 1)
            try:
                if self.socket_identity() != identity:
                    raise Refusal('unsafe_socket_path', 503)
                writer.write(raw)
                await writer.drain()
                return decode_frame(await reader.readline())
            finally:
                writer.close()
                await writer.wait_closed()
        try:
            return await asyncio.wait_for(exchange(), timeout=5)
        except Refusal:
            raise
        except (OSError, TimeoutError, ValueError):
            raise Refusal('owner_unavailable', 503) from None


def public_orders(rows, *, leg=False):
    fields = ('order_ref', 'state', 'filled_units', 'reduce_only') if leg else (
        'order_ref', 'venue', 'state', 'filled_units', 'reduce_only')
    return [{key: deepcopy(row[key]) for key in fields if key in row}
            for row in rows if type(row) is dict] if type(rows) is list else []


def public_state(state):
    result = {key: deepcopy(state[key]) for key in STATE_FIELDS if key in state}
    result['account_binding'] = state['account_binding_hash'][:12]
    result['pending_orders'] = public_orders(state.get('pending_orders'))
    return result


def public_operation(operation):
    result = {key: deepcopy(operation[key]) for key in (
        'operation_id', 'idempotency_key', 'action', 'expected_cycle_id', 'state',
        'intent', 'intent_revision', 'accepted_ts', 'updated_ts') if key in operation}
    evidence = operation.get('evidence') or {}
    safe = {key: deepcopy(evidence[key]) for key in EVIDENCE_FIELDS if key in evidence}
    safe['pending_orders'] = public_orders(evidence.get('pending_orders'))
    safe['legs'] = []
    for leg in evidence.get('legs', []):
        if type(leg) is dict:
            safe['legs'].append({**{key: deepcopy(leg[key]) for key in (
                'venue', 'initial_units', 'remaining_units') if key in leg},
                'orders': public_orders(leg.get('orders'), leg=True)})
    if safe.get('accounting_final') is not True:
        safe['net_pnl_usd'] = None
    result['evidence'] = safe
    return result


class PreviewRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    source: Literal['basis', 'funding']
    action: Literal['start', 'stop', 'close']
    expected_cycle_id: str | None = Field(default=None, pattern=r'^[A-Za-z0-9][A-Za-z0-9_.:+-]{0,255}$')
    parameters_revision: int | None = Field(default=None, ge=0, strict=True)
    parameters_hash: str | None = Field(default=None, pattern=r'^[a-f0-9]{64}$')

    @model_validator(mode='after')
    def cycle_matches_action(self):
        if (self.action == 'close') != bool(self.expected_cycle_id):
            raise ValueError('only close requires expected_cycle_id')
        if ((self.parameters_revision is None) != (self.parameters_hash is None)
                or (self.parameters_revision is not None and self.action != 'start')):
            raise ValueError('only start accepts a complete parameters version')
        return self


class Confirmation(BaseModel):
    model_config = ConfigDict(extra='forbid')
    preview_id: UUID
    idempotency_key: UUID


class ParameterValues(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    entry_band_bps: str = Field(min_length=1, max_length=64)
    exit_min_gross_profit_bps: str = Field(min_length=1, max_length=64)
    entropy_exit_room_bps: str = Field(min_length=1, max_length=64)
    lighter_exit_room_bps: str = Field(min_length=1, max_length=64)
    max_notional_usd: str = Field(min_length=1, max_length=64)
    entropy_open_room_bps: str | None = Field(default=None, min_length=1, max_length=64)
    lighter_open_room_bps: str | None = Field(default=None, min_length=1, max_length=64)

    @model_validator(mode='after')
    def complete_parameter_fields(self):
        if self.model_fields_set not in (LEGACY_PARAMETER_FIELDS, PARAMETER_FIELDS):
            raise ValueError('parameters require exactly five or seven fields')
        return self


class ParameterSave(BaseModel):
    model_config = ConfigDict(extra='forbid')
    source: Literal['basis']
    expected_revision: int = Field(ge=0, strict=True)
    values: ParameterValues
    idempotency_key: UUID


def checked_opening_policies(parameters):
    capabilities = parameters.get('capabilities')
    if (type(capabilities) is not dict or set(capabilities) != {'opening_concessions'}
            or capabilities['opening_concessions'] is not True):
        raise ValueError('opening capability required')
    policies = parameters['opening_policies']
    if type(policies) is not dict or set(policies) != {'base', 'current', 'saved'}:
        raise ValueError('invalid opening policies')
    normalized = {}
    for name in ('base', 'current', 'saved'):
        pair = policies[name]
        if name == 'saved' and parameters.get('saved') is None:
            if pair is not None:
                raise ValueError('unexpected saved policy')
            continue
        if type(pair) is not dict or set(pair) != {'entropy', 'lighter'}:
            raise ValueError('invalid opening pair')
        normalized[name] = {}
        for venue, policy in pair.items():
            if (type(policy) is not dict or set(policy) != {'rooms_bps', 'hard_cap_bps'}
                    or type(policy['rooms_bps']) is not list or not policy['rooms_bps']
                    or any(type(value) is not str or not value or len(value) > 64
                           for value in [*policy['rooms_bps'], policy['hard_cap_bps']])):
                raise ValueError('invalid opening ladder')
            rooms = tuple(Decimal(value) for value in policy['rooms_bps'])
            cap = Decimal(policy['hard_cap_bps'])
            if (not cap.is_finite() or cap < 0 or any(not room.is_finite()
                    or not 0 <= room <= cap for room in rooms) or tuple(sorted(rooms)) != rooms):
                raise ValueError('invalid opening bounds')
            normalized[name][venue] = (rooms, cap)
    for venue in ('entropy', 'lighter'):
        field = venue + '_open_room_bps'
        base = normalized['base'][venue]
        if base[1] != Decimal(parameters['limits'][field]):
            raise ValueError('opening limit differs from base cap')
        for name in ('current', 'saved'):
            record = parameters.get(name)
            if record is None:
                continue
            value = validate_parameters(record['values'], parameters['limits'])[field]
            expected = base if value is None else ((Decimal(value),) * len(base[0]), Decimal(value))
            if normalized[name][venue] != expected:
                raise ValueError('opening policy differs from parameter values')


def create_app(owner=None, *, origin=None, control_port=None):
    owner = owner or SocketOwner()
    origin = origin or os.getenv('V3_OWNER_UI_ORIGIN', 'http://127.0.0.1:5186')
    parsed = urlparse(origin)
    if (parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '::1')
            or parsed.path or parsed.query or parsed.fragment or parsed.username or parsed.password):
        raise ValueError('UI origin must be an exact loopback HTTP origin')
    port = str(control_port or os.getenv('V3_OWNER_CONTROL_PORT', '8786'))
    if not port.isdigit() or not 0 < int(port) < 65536:
        raise ValueError('invalid local control port')
    hosts = {parsed.netloc, f'127.0.0.1:{port}', f'localhost:{port}', f'[::1]:{port}'}
    sessions, previews, confirmations = {}, {}, {}
    trusted_binding = dict(owner.binding)
    gate = asyncio.Lock()
    api = FastAPI(title='Entropy Local Owner Control', docs_url=None, redoc_url=None, openapi_url=None)
    api.state.previews, api.state.confirmations = previews, confirmations

    def common(op, instance=None, **extra):
        return {**owner.binding, 'protocol_version': 1, 'request_id': str(uuid4()),
                'op': op, **({'owner_instance_id': instance} if instance else {}), **extra}

    def checked_state(state):
        try:
            stamp = state.get('source_ts')
            valid = (type(state) is dict and type(state.get('protocol_version')) is int
                     and state['protocol_version'] == 1
                     and canonical_uuid(state.get('owner_instance_id'))
                     and all(state.get(key) == value for key, value in trusted_binding.items()
                             if key != 'config_hash')
                     and state.get('intent') in ('RUNNING', 'STOPPING', 'STOPPED')
                     and type(state.get('intent_revision')) is int and state['intent_revision'] >= 0
                     and type(state.get('allowed_actions')) is list
                     and all(action in ('start', 'stop', 'close') for action in state['allowed_actions'])
                     and type(state.get('risk_locks')) is list
                     and all(type(lock) is str for lock in state['risk_locks'])
                     and type(state.get('position_authoritative')) is bool
                     and all(state.get(key) is None or type(state[key]) is int
                             for key in ('entropy_units', 'hedge_units'))
                     and type(stamp) in (int, float) and math.isfinite(stamp)
                     and -1 <= time.time() - stamp <= 10)
        except (ValueError, TypeError, AttributeError):
            valid = False
        if not valid:
            if type(state) is dict and any(state.get(key) != value for key, value in trusted_binding.items()
                                          if key != 'config_hash'):
                raise Refusal('binding_mismatch', 503)
            raise Refusal('invalid_owner_response', 503)
        parameters = state.get('parameters')
        if parameters is not None:
            try:
                valid_parameters = (type(parameters) is dict
                    and parameters.get('base_config_hash') == trusted_binding['config_hash']
                    and type(parameters.get('can_save')) is bool
                    and type(parameters.get('limits')) is dict)
                fields = set(parameters['current']['values'])
                valid_parameters = valid_parameters and (fields in (LEGACY_PARAMETER_FIELDS,
                    PARAMETER_FIELDS) and set(parameters['limits']) == fields)
                if fields == PARAMETER_FIELDS:
                    checked_opening_policies(parameters)
                elif parameters.get('capabilities') is not None or parameters.get('opening_policies') is not None:
                    raise ValueError('legacy owner cannot declare opening policies')
                for version in (parameters['current'], parameters.get('saved')):
                    if version is None:
                        continue
                    valid_parameters = valid_parameters and (
                        type(version) is dict and type(version.get('revision')) is int
                        and version['revision'] >= 0 and type(version.get('config_hash')) is str
                        and re.fullmatch(r'[a-f0-9]{64}', version['config_hash']) is not None)
                    values = ParameterValues.model_validate(version['values']).model_dump(exclude_unset=True)
                    valid_parameters = valid_parameters and set(values) == fields
                    validate_parameters(values, parameters['limits'])
                    for name in fields:
                        raw_maximum = parameters['limits'][name]
                        maximum = Decimal(raw_maximum) if type(raw_maximum) is str else Decimal('NaN')
                        valid_parameters = valid_parameters and (maximum.is_finite()
                            and (maximum >= 0 if name in OPENING_PARAMETER_FIELDS else maximum > 0))
                valid_parameters = valid_parameters and (
                    parameters['current']['config_hash'] == state.get('config_hash')
                    and (parameters.get('saved') is None or parameters['saved']['revision']
                         > parameters['current']['revision']))
            except (KeyError, ValueError, TypeError, AttributeError, ArithmeticError):
                valid_parameters = False
            if not valid_parameters:
                raise Refusal('binding_mismatch' if type(parameters) is dict and
                              parameters.get('base_config_hash') != trusted_binding['config_hash']
                              else 'invalid_owner_response', 503)
        elif state.get('config_hash') != trusted_binding['config_hash']:
            raise Refusal('binding_mismatch', 503)
        # Only the verified, fixed-scope local owner can advance the config binding.
        owner.binding['config_hash'] = state['config_hash']
        return state

    async def wire(payload):
        result = await owner.request(payload)
        if (type(result) is not dict or result.get('request_id') not in (None, payload['request_id'])
                or (result.get('ok') is True and result.get('request_id') != payload['request_id'])):
            raise Refusal('invalid_owner_response', 503)
        if result.get('ok') is not True:
            reason = result.get('reason')
            code = reason if reason in MESSAGES else 'invalid_owner_response'
            raise Refusal(code, 503 if code in ('owner_operation_failed', 'invalid_or_busy_frame',
                                               'invalid_owner_response') else 409)
        return result

    async def handshake(source='basis'):
        if source != 'basis':
            raise Refusal('unsupported_source', 503)
        result = await wire(common('state'))
        return checked_state(result)

    async def capability(source):
        try:
            state = await handshake(source)
            return {'enabled': True, 'source': source, 'reason_code': None, 'reason': None,
                    'state': public_state(state)}
        except Refusal as exc:
            return {'enabled': False, 'source': source, 'reason_code': exc.code,
                    'reason': MESSAGES.get(exc.code, MESSAGES['owner_unavailable']), 'state': None}

    def session_id(request):
        sid = request.cookies.get('entropy_owner_session')
        if sid not in sessions or sessions[sid]['expires_at'] <= time.time():
            fail('session_expired', 403)
        return sid

    @api.middleware('http')
    async def local_only(request: Request, call_next):
        try:
            local = ipaddress.ip_address(request.client.host).is_loopback
        except (ValueError, AttributeError):
            local = False
        if not local or request.headers.get('host') not in hosts:
            return Response(status_code=403)
        supplied = request.headers.get('origin')
        if (supplied and supplied != origin) or (request.method == 'POST' and supplied != origin):
            return Response(status_code=403)
        if request.headers.get('sec-fetch-site') in ('same-site', 'cross-site'):
            return Response(status_code=403)
        if request.method not in ('GET', 'POST'):
            return Response(status_code=405)
        if request.method == 'POST':
            sid = request.cookies.get('entropy_owner_session')
            value = sessions.get(sid)
            if (not value or value['expires_at'] <= time.time()
                    or not secrets.compare_digest(request.headers.get('x-entropy-csrf', ''), value['csrf'])):
                return Response(status_code=403)
            if request.headers.get('content-type', '').split(';')[0].strip() != 'application/json':
                return Response(status_code=415)
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 2048:
                    return Response(status_code=413)
            request._body = bytes(body)
            # Reject duplicate fields and nonfinite numbers before schema validation.
            try:
                decode_frame(bytes(body) + b'\n')
            except Refusal:
                return Response(status_code=422)
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @api.get('/api/v3-control/session')
    async def session(request: Request, response: Response, source: Literal['basis', 'funding'] = 'basis'):
        now = time.time()
        for sid in list(sessions):
            if sessions[sid]['expires_at'] <= now:
                del sessions[sid]
        sid = request.cookies.get('entropy_owner_session')
        if sid not in sessions:
            if len(sessions) >= 64:
                fail('capacity_exceeded', 429)
            sid = secrets.token_urlsafe(32)
            sessions[sid] = {'csrf': secrets.token_urlsafe(32), 'expires_at': now + 3600}
        response.set_cookie('entropy_owner_session', sid, httponly=True, samesite='strict',
                            path='/api/v3-control', max_age=int(sessions[sid]['expires_at'] - now))
        return {**await capability(source), 'csrf_token': sessions[sid]['csrf']}

    @api.get('/api/v3-control/state')
    async def state(request: Request, source: Literal['basis', 'funding'] = 'basis'):
        session_id(request)
        return await capability(source)

    @api.post('/api/v3-control/preview')
    async def preview(body: PreviewRequest, request: Request):
        sid = session_id(request)
        for pid in list(previews):
            if previews[pid]['expires_at'] <= time.time() and not previews[pid].get('key'):
                del previews[pid]
        if len(previews) >= 128:
            fail('capacity_exceeded', 429)
        try:
            current = await handshake(body.source)
            payload = common('preview', current['owner_instance_id'], action=body.action,
                             intent_revision=current['intent_revision'], expected_cycle_id=body.expected_cycle_id)
            if body.parameters_revision is not None:
                payload.update(parameters_revision=body.parameters_revision,
                               parameters_hash=body.parameters_hash)
            result = await wire(payload)
            state = checked_state(result.get('state'))
            expires = result.get('expires_ts')
            if (state['owner_instance_id'] != current['owner_instance_id']
                    or result.get('action') != body.action
                    or result.get('expected_cycle_id') != body.expected_cycle_id
                    or result.get('intent_revision') != current['intent_revision']
                    or not canonical_uuid(result.get('receipt_id'))
                    or type(expires) not in (float, int) or not math.isfinite(expires)
                    or not 0 < expires - time.time() <= 16):
                raise Refusal('invalid_owner_response', 503)
        except Refusal as exc:
            fail(exc.code, exc.status)
        pid = str(uuid4())
        previews[pid] = {'sid': sid, 'state': deepcopy(state), 'expires_at': expires,
                         'confirm': {key: payload[key] for key in (
                             'action', 'intent_revision', 'expected_cycle_id',
                             'parameters_revision', 'parameters_hash') if key in payload},
                         'receipt_id': result['receipt_id'], 'key': None}
        return {'preview_id': pid, 'action': body.action, 'expected_cycle_id': body.expected_cycle_id,
                'expires_at': expires, 'state': public_state(state), 'valuation': {
                    'executable_gross_usd': None, 'fees_usd': None, 'funding_usd': None,
                    'reason': 'owner_protocol_has_no_valuation'}}

    def operation_result(result, *, key=None, operation_id=None):
        state = checked_state(result.get('state'))
        op = result.get('operation')
        try:
            valid = (type(op) is dict and canonical_uuid(op.get('operation_id'))
                     and canonical_uuid(op.get('idempotency_key'))
                     and op.get('action') in ('start', 'stop', 'close')
                     and op.get('state') in ('accepted', 'executing', 'flat_pending_accounting', 'completed', 'blocked')
                     and (key is None or op['idempotency_key'] == key)
                     and (operation_id is None or op['operation_id'] == operation_id))
        except (ValueError, TypeError):
            valid = False
        if not valid:
            raise Refusal('invalid_owner_response', 503)
        return {'operation_id': op['operation_id'], 'operation': public_operation(op),
                'state': public_state(state)}

    async def lookup(current, *, key=None, operation_id=None):
        field = {'idempotency_key': key} if key else {'operation_id': operation_id}
        result = await wire(common('operation', current['owner_instance_id'], **field))
        return operation_result(result, key=key, operation_id=operation_id)

    @api.post('/api/v3-control/confirm')
    async def confirm(body: Confirmation, request: Request):
        sid, pid, key = session_id(request), str(body.preview_id), str(body.idempotency_key)
        async with gate:
            receipt = previews.get(pid)
            if not receipt or receipt['sid'] != sid:
                fail('preview_unavailable')
            prior = confirmations.get(key)
            if prior and (prior['pid'] != pid or prior['sid'] != sid):
                fail('idempotency_conflict')
            # Repeated clicks on the same preview, even with a new UI key, refer
            # to the original accepted/unknown action, never a second submission.
            key = receipt['key'] or key
            try:
                current = await handshake()
                if receipt['key']:
                    try:
                        return await lookup(current, key=key)
                    except Refusal as exc:
                        if exc.code != 'operation_not_found':
                            raise
                    # Replay only the immutable original receipt/key. The owner
                    # checks durable idempotency before validating receipt expiry.
                else:
                    if receipt['expires_at'] <= time.time():
                        raise Refusal('receipt_expired')
                    if current['owner_instance_id'] != receipt['state']['owner_instance_id']:
                        raise Refusal('owner_instance_mismatch')
                    if len(confirmations) >= 128:
                        raise Refusal('capacity_exceeded', 429)
                    receipt['key'] = key
                    confirmations[key] = {'pid': pid, 'sid': sid}
                payload = common('confirm', current['owner_instance_id'], **receipt['confirm'],
                                 receipt_id=receipt['receipt_id'], idempotency_key=key)
                payload['config_hash'] = receipt['state']['config_hash']
                result = await wire(payload)
                return operation_result(result, key=key)
            except Refusal as exc:
                # Once a frame may have reached the owner, never claim failure
                # implies no action. Retain original key and recover by lookup.
                if receipt['key'] and exc.code in ('owner_unavailable', 'invalid_owner_response',
                                                   'owner_operation_failed', 'invalid_or_busy_frame'):
                    fail('outcome_unknown', 503)
                fail(exc.code, exc.status)

    def parameters_result(result):
        state = checked_state(result.get('state'))
        if state.get('parameters') is None or result.get('parameters') != state['parameters']:
            raise Refusal('invalid_owner_response', 503)
        response = {'parameters': deepcopy(state['parameters']), 'state': public_state(state)}
        if 'revision' in result or 'config_hash' in result:
            revision, digest = result.get('revision'), result.get('config_hash')
            if (type(revision) is not int or revision < 0 or type(digest) is not str
                    or re.fullmatch(r'[a-f0-9]{64}', digest) is None):
                raise Refusal('invalid_owner_response', 503)
            response['save_receipt'] = {'revision': revision, 'config_hash': digest}
        return response

    @api.post('/api/v3-control/parameters/save')
    async def save_parameters(body: ParameterSave, request: Request):
        session_id(request)
        async with gate:
            attempted = False
            try:
                current = await handshake(body.source)
                if current.get('parameters') is None:
                    raise Refusal('parameters_unavailable')
                values = body.values.model_dump(exclude_unset=True)
                if (set(values) == PARAMETER_FIELDS and (current['parameters'].get(
                        'capabilities') or {}).get('opening_concessions') is not True):
                    if any(values[name] is not None for name in OPENING_PARAMETER_FIELDS):
                        raise Refusal('opening_parameters_unavailable')
                    values = {name: value for name, value in values.items()
                              if name in LEGACY_PARAMETER_FIELDS}
                payload = common('parameters_save', current['owner_instance_id'],
                    expected_revision=body.expected_revision, values=values,
                    idempotency_key=str(body.idempotency_key))
                attempted = True
                return parameters_result(await wire(payload))
            except Refusal as exc:
                if attempted and exc.code in ('owner_unavailable', 'invalid_owner_response',
                                               'owner_operation_failed', 'invalid_or_busy_frame'):
                    fail('outcome_unknown', 503)
                fail(exc.code, exc.status)

    @api.get('/api/v3-control/parameters/saves/by-key/{idempotency_key}')
    async def parameter_save(idempotency_key: UUID, request: Request,
                             source: Literal['basis', 'funding'] = 'basis'):
        session_id(request)
        try:
            current = await handshake(source)
            return parameters_result(await wire(common('parameters_lookup',
                current['owner_instance_id'], idempotency_key=str(idempotency_key))))
        except Refusal as exc:
            fail(exc.code, exc.status)

    @api.get('/api/v3-control/operations/by-key/{idempotency_key}')
    async def by_key(idempotency_key: UUID, request: Request, source: Literal['basis', 'funding'] = 'basis'):
        session_id(request)
        try:
            return await lookup(await handshake(source), key=str(idempotency_key))
        except Refusal as exc:
            fail(exc.code, exc.status)

    @api.get('/api/v3-control/operations/{operation_id}')
    async def operation(operation_id: UUID, request: Request, source: Literal['basis', 'funding'] = 'basis'):
        session_id(request)
        try:
            return await lookup(await handshake(source), operation_id=str(operation_id))
        except Refusal as exc:
            fail(exc.code, exc.status)

    return api
