"""Local owner protocol. Callbacks persist intent; this module never trades."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import socket
import stat
import time
import uuid

from .parameter_config import LEGACY_PARAMETER_FIELDS, PARAMETER_FIELDS


PROTOCOL_VERSION = 1
MAX_FRAME_BYTES = 16384
RECEIPT_SECONDS = 15.0
BINDING_FIELDS = frozenset({'account_binding_hash', 'pair_identity', 'deployment',
                            'batch_id', 'config_hash'})
ACTIONS = frozenset({'start', 'stop', 'close'})
PUBLIC_OPERATION_FIELDS = frozenset({'operation_id', 'idempotency_key', 'action',
    'expected_cycle_id', 'state', 'intent', 'intent_revision', 'accepted_ts', 'updated_ts'})
PUBLIC_EVIDENCE_FIELDS = frozenset({'intent', 'intent_revision', 'phase', 'risk_locks',
    'blocked_reason', 'active_cycle_id', 'entropy_units', 'hedge_units',
    'position_observed_ts', 'position_authoritative', 'quantity_step', 'order_watermark',
    'allowed_actions', 'cycle_id', 'accounting_final', 'net_pnl_usd', 'fees_complete',
    'known_fees_usd', 'updated_ts', 'source_ts'})


def public_operation(operation):
    """Keep durable receipts and internal order details off the bounded wire."""
    value = {key: copy.deepcopy(operation[key]) for key in PUBLIC_OPERATION_FIELDS
             if key in operation}
    evidence = operation.get('evidence')
    if type(evidence) is dict:
        safe = {key: copy.deepcopy(evidence[key]) for key in PUBLIC_EVIDENCE_FIELDS
                if key in evidence}
        if type(evidence.get('pending_orders')) is list:
            safe['pending_orders'] = [{key: row[key] for key in
                ('order_ref', 'venue', 'state', 'reduce_only', 'filled_units') if key in row}
                for row in evidence['pending_orders'] if type(row) is dict]
        if type(evidence.get('legs')) is list:
            safe['legs'] = []
            for leg in evidence['legs']:
                if type(leg) is not dict:
                    continue
                item = {key: leg[key] for key in
                        ('venue', 'remaining_units', 'initial_units') if key in leg}
                if type(leg.get('orders')) is list:
                    item['orders'] = [{key: row[key] for key in
                        ('order_ref', 'state', 'filled_units', 'reduce_only') if key in row}
                        for row in leg['orders'] if type(row) is dict]
                safe['legs'].append(item)
        value['evidence'] = safe
    return value


class ControlRefusal(ValueError):
    pass


def _require(value, reason):
    if not value:
        raise ControlRefusal(reason)


def _uuid(value):
    _require(type(value) is str, 'invalid_uuid')
    try:
        _require(str(uuid.UUID(value)) == value, 'invalid_uuid')
    except (ValueError, AttributeError):
        raise ControlRefusal('invalid_uuid') from None
    return value


def _encode(value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'),
                     ensure_ascii=False, allow_nan=False).encode('utf-8') + b'\n'
    _require(len(raw) <= MAX_FRAME_BYTES, 'frame_too_large')
    return raw


def _decode(raw):
    _require(len(raw) <= MAX_FRAME_BYTES and raw.endswith(b'\n'), 'invalid_frame')
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'duplicate_field')
            result[key] = value
        return result
    def invalid_constant(_):
        raise ControlRefusal('invalid_json')
    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)
    except (ValueError, UnicodeError):
        raise ControlRefusal('invalid_json') from None
    _require(type(value) is dict, 'invalid_request')
    return value


class OperatorControl:
    def __init__(self, binding: dict, *, snapshot, commit, operation, wake,
                 save_parameters=None, parameter_save=None):
        _require(type(binding) is dict and set(binding) == BINDING_FIELDS,
                 'invalid_binding')
        _require(all(type(value) is str and value for value in binding.values()),
                 'invalid_binding')
        _require(all(callable(fn) and not inspect.iscoroutinefunction(fn)
                     for fn in (snapshot, commit, operation, wake)), 'callbacks_must_be_sync')
        self.binding = dict(binding)
        self.owner_instance_id = str(uuid.uuid4())
        self._snapshot = snapshot
        self._commit = commit
        self._operation = operation
        self._wake = wake
        _require(all(fn is None or (callable(fn) and not inspect.iscoroutinefunction(fn))
                     for fn in (save_parameters, parameter_save)), 'callbacks_must_be_sync')
        self._save_parameters = save_parameters
        self._parameter_save = parameter_save
        self._receipts = {}
        self._queue = asyncio.Queue(maxsize=128)
        self._server = None
        self._consumer = None
        self._clients = set()
        self._socket_path = None
        self._socket_identity = None

    @staticmethod
    def _sync(fn, *args, **kwargs):
        result = fn(*args, **kwargs)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise ControlRefusal('callbacks_must_be_sync')
        return result

    def _current(self):
        value = self._sync(self._snapshot)
        _require(type(value) is dict, 'invalid_owner_state')
        _require(value.get('intent') in {'RUNNING', 'STOPPING', 'STOPPED'}
                 and type(value.get('intent_revision')) is int
                 and value['intent_revision'] >= 0, 'invalid_owner_state')
        actions = value.get('allowed_actions')
        _require(type(actions) is list and all(a in ACTIONS for a in actions),
                 'invalid_owner_state')
        return copy.deepcopy(value)

    def state(self):
        return self._state(self._current())

    def _state(self, snapshot):
        return {**snapshot, **self.binding, 'protocol_version': PROTOCOL_VERSION,
                'owner_instance_id': self.owner_instance_id, 'source_ts': time.time()}

    def _validate(self, request):
        _require(type(request) is dict, 'invalid_request')
        _encode(request)
        _require(type(request.get('protocol_version')) is int
                 and request['protocol_version'] == PROTOCOL_VERSION, 'protocol_mismatch')
        _uuid(request.get('request_id'))
        op = request.get('op')
        mutable_read = self._save_parameters is not None and op in {
            'state', 'operation', 'parameters_lookup', 'confirm'}
        _require(all(request.get(key) == value for key, value in self.binding.items()
                     if key != 'config_hash' or not mutable_read), 'binding_mismatch')
        common = BINDING_FIELDS | {'protocol_version', 'request_id', 'op', 'owner_instance_id'}
        parameters = {'parameters_revision', 'parameters_hash'}
        fields = {'state': set(), 'preview': {'action', 'intent_revision', 'expected_cycle_id'} | parameters,
                  'confirm': {'action', 'intent_revision', 'expected_cycle_id',
                              'receipt_id', 'idempotency_key'} | parameters,
                  'operation': {'operation_id', 'idempotency_key'},
                  'parameters_save': {'expected_revision', 'values', 'idempotency_key'},
                  'parameters_lookup': {'idempotency_key'}}
        _require(op in fields, 'invalid_op')
        _require(not set(request) - common - fields[op], 'unknown_field')
        if op != 'state' or 'owner_instance_id' in request:
            _require(request.get('owner_instance_id') == self.owner_instance_id,
                     'owner_instance_mismatch')
        if op in {'preview', 'confirm'}:
            _require(request.get('action') in ACTIONS, 'invalid_action')
            _require(type(request.get('intent_revision')) is int
                     and request['intent_revision'] >= 0, 'invalid_revision')
            target = request.get('expected_cycle_id')
            if request['action'] == 'close':
                _require(type(target) is str and 0 < len(target) <= 256, 'invalid_cycle_id')
            else:
                _require(target is None, 'unexpected_cycle_id')
            if parameters & set(request):
                _require(request['action'] == 'start' and parameters <= set(request)
                         and type(request['parameters_revision']) is int
                         and request['parameters_revision'] >= 0
                         and type(request['parameters_hash']) is str
                         and len(request['parameters_hash']) == 64, 'invalid_parameters_revision')
        if op == 'confirm':
            _uuid(request.get('receipt_id'))
            _uuid(request.get('idempotency_key'))
        if op == 'operation':
            _require(('operation_id' in request) != ('idempotency_key' in request),
                     'invalid_operation_lookup')
            _uuid(request.get('operation_id', request.get('idempotency_key')))
        if op in {'parameters_save', 'parameters_lookup'}:
            _require(self._save_parameters is not None and self._parameter_save is not None,
                     'parameters_unavailable')
            _uuid(request.get('idempotency_key'))
        if op == 'parameters_save':
            _require(type(request.get('expected_revision')) is int
                     and request['expected_revision'] >= 0
                     and type(request.get('values')) is dict, 'invalid_parameters')
            _require(set(request['values']) in (LEGACY_PARAMETER_FIELDS, PARAMETER_FIELDS),
                     'invalid_parameters_fields')
        return op

    def _admit(self, request, snapshot):
        _require(request['intent_revision'] == snapshot['intent_revision'], 'intent_revision_changed')
        action = request['action']
        if action == 'start':
            _require(not snapshot.get('risk_locks'), 'risk_locked')
            parameters = snapshot.get('parameters')
            if type(parameters) is dict:
                target = parameters.get('saved') or parameters['current']
                if 'parameters_revision' in request or parameters.get('saved') is not None:
                    _require(request.get('parameters_revision') == target['revision']
                             and request.get('parameters_hash') == target['config_hash'],
                             'parameters_revision_changed')
                    _require(parameters.get('can_save') is True and snapshot['intent'] == 'STOPPED',
                             parameters.get('reason_code') or 'parameters_require_stopped_flat')
        _require(action in snapshot['allowed_actions'], 'action_unavailable')
        if action != 'close':
            return
        _require(request['expected_cycle_id'] == snapshot.get('active_cycle_id'), 'cycle_changed')
        _require(type(snapshot.get('entropy_units')) is int
                 and type(snapshot.get('hedge_units')) is int, 'position_unavailable')
        observed = snapshot.get('position_observed_ts')
        _require(snapshot.get('position_authoritative') is True
                 and type(observed) in (int, float) and math.isfinite(observed)
                 and 0 <= time.time() - observed <= RECEIPT_SECONDS, 'position_evidence_stale')
        _require(snapshot.get('order_watermark') is not None, 'order_watermark_unavailable')

    def _fingerprint(self, request):
        value = {**{key: request.get(key) for key in BINDING_FIELDS}, 'action': request['action'],
                 'intent_revision': request['intent_revision'],
                 'expected_cycle_id': request.get('expected_cycle_id'),
                 'receipt_id': request['receipt_id']}
        value.update({key: request[key] for key in ('parameters_revision', 'parameters_hash')
                      if key in request})
        return hashlib.sha256(_encode(value)).hexdigest()

    def handle(self, request):
        try:
            op = self._validate(request)
            if op == 'state':
                result = self.state()
            elif op == 'operation':
                result = (self._sync(self._operation, request['operation_id'])
                          if 'operation_id' in request else
                          self._sync(self._operation, idempotency_key=request['idempotency_key']))
                _require(type(result) is dict, 'operation_not_found')
                result = {'operation': public_operation(result), 'state': self.state()}
            elif op == 'parameters_save':
                result = self._sync(self._save_parameters,
                    expected_revision=request['expected_revision'], values=request['values'],
                    idempotency_key=request['idempotency_key'])
                _require(type(result) is dict, 'invalid_parameters')
                result = {**result, 'state': self.state()}
            elif op == 'parameters_lookup':
                result = self._sync(self._parameter_save, request['idempotency_key'])
                _require(type(result) is dict, 'parameter_save_not_found')
                result = {**result, 'state': self.state()}
            elif op == 'preview':
                snapshot = self._current()
                self._admit(request, snapshot)
                now = time.monotonic()
                self._receipts = {key: value for key, value in self._receipts.items()
                                  if value['expires_mono'] >= now}
                _require(len(self._receipts) < 128, 'preview_capacity')
                receipt_id = str(uuid.uuid4())
                receipt = {'action': request['action'], 'intent_revision': request['intent_revision'],
                           'expected_cycle_id': request.get('expected_cycle_id'),
                           'snapshot': snapshot, 'expires_mono': now + RECEIPT_SECONDS}
                receipt.update({key: request[key] for key in ('parameters_revision', 'parameters_hash')
                                if key in request})
                self._receipts[receipt_id] = receipt
                result = {'receipt_id': receipt_id, 'expires_ts': time.time() + RECEIPT_SECONDS,
                          'action': receipt['action'], 'expected_cycle_id': receipt['expected_cycle_id'],
                          'intent_revision': receipt['intent_revision'], 'state': self._state(snapshot)}
            else:
                fingerprint = self._fingerprint(request)
                prior = self._sync(self._operation, idempotency_key=request['idempotency_key'])
                if prior is not None:
                    _require(type(prior) is dict and prior.get('request_fingerprint') == fingerprint,
                             'idempotency_conflict')
                    result = {'operation': public_operation(prior), 'state': self.state()}
                else:
                    _require(request.get('config_hash') == self.binding['config_hash'], 'binding_mismatch')
                    receipt = self._receipts.get(request['receipt_id'])
                    _require(receipt is not None, 'receipt_unavailable')
                    _require(time.monotonic() <= receipt['expires_mono'], 'receipt_expired')
                    _require(all(request.get(key) == receipt[key] for key in
                                 ('action', 'intent_revision', 'expected_cycle_id')), 'receipt_mismatch')
                    _require(all(request.get(key) == receipt.get(key) for key in
                                 ('parameters_revision', 'parameters_hash')), 'receipt_mismatch')
                    snapshot = self._current()
                    self._admit(request, snapshot)
                    if request['action'] == 'close':
                        _require(all(snapshot.get(key) == receipt['snapshot'].get(key) for key in
                                     ('active_cycle_id', 'entropy_units', 'hedge_units', 'order_watermark')),
                                 'close_evidence_changed')
                    arguments = {key: request[key] for key in ('parameters_revision', 'parameters_hash')
                                 if key in request}
                    operation = self._sync(self._commit, action=request['action'],
                        expected_revision=request['intent_revision'],
                        idempotency_key=request['idempotency_key'], request_fingerprint=fingerprint,
                        expected_cycle_id=request.get('expected_cycle_id'), **arguments)
                    _require(type(operation) is dict, 'invalid_operation')
                    self._sync(self._wake)
                    result = {'operation': public_operation(operation), 'state': self.state()}
            return {'ok': True, 'request_id': request['request_id'], **result}
        except ControlRefusal as exc:
            reason = str(exc)
        except (ValueError, TypeError, OverflowError):
            reason = 'invalid_request'
        except Exception:
            reason = 'owner_operation_failed'
        return {'ok': False, 'protocol_version': PROTOCOL_VERSION,
                'owner_instance_id': self.owner_instance_id,
                'request_id': request.get('request_id') if type(request) is dict else None,
                'reason': reason}

    async def _consume(self):
        while True:
            request, future = await self._queue.get()
            if not future.cancelled():
                future.set_result(self.handle(request))

    async def _client(self, reader, writer):
        task = asyncio.current_task()
        self._clients.add(task)
        try:
            try:
                raw = await asyncio.wait_for(reader.readline(), timeout=5)
                request = _decode(raw)
                future = asyncio.get_running_loop().create_future()
                self._queue.put_nowait((request, future))
                response = await future
                writer.write(_encode(response))
            except (ControlRefusal, ValueError, asyncio.TimeoutError, asyncio.QueueFull):
                writer.write(_encode({'ok': False, 'reason': 'invalid_or_busy_frame'}))
            await writer.drain()
        except ConnectionError:
            pass  # A lost response does not undo a durable accepted command.
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self._clients.discard(task)

    async def start(self, socket_path):
        _require(self._server is None, 'owner_server_already_started')
        path = Path(socket_path).absolute()
        parent = path.parent
        try:
            parent.mkdir(mode=0o700)
        except FileExistsError:
            pass
        info = parent.lstat()
        _require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
                 and stat.S_IMODE(info.st_mode) == 0o700, 'unsafe_socket_directory')
        try:
            existing = path.lstat()
        except FileNotFoundError:
            existing = None
        if existing is not None:
            _require(stat.S_ISSOCK(existing.st_mode) and existing.st_uid == os.getuid(),
                     'unsafe_socket_path')
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path), .5)
            except ConnectionRefusedError:
                current = path.lstat()
                _require((current.st_dev, current.st_ino) == (existing.st_dev, existing.st_ino),
                         'socket_path_changed')
                path.unlink()
            else:
                writer.close()
                await writer.wait_closed()
                raise ControlRefusal('owner_socket_already_active')
        # asyncio's path-based server may unlink a socket that appeared after
        # the check above. Bind ourselves so a racing owner yields EADDRINUSE.
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(path))
            info = path.lstat()
            self._socket_path = path
            self._socket_identity = (info.st_dev, info.st_ino)
            path.chmod(0o600)
            listener.setblocking(False)
            self._server = await asyncio.start_unix_server(self._client, sock=listener,
                                                           limit=MAX_FRAME_BYTES + 1)
        except BaseException:
            listener.close()
            await self.close()
            raise
        self._consumer = asyncio.create_task(self._consume(), name='v3-operator-consumer')

    async def close(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for task in tuple(self._clients):
            task.cancel()
        if self._clients:
            await asyncio.gather(*tuple(self._clients), return_exceptions=True)
        if self._consumer is not None:
            self._consumer.cancel()
            await asyncio.gather(self._consumer, return_exceptions=True)
            self._consumer = None
        if self._socket_path is not None:
            try:
                info = self._socket_path.lstat()
                if (info.st_dev, info.st_ino) == self._socket_identity:
                    self._socket_path.unlink()
            except FileNotFoundError:
                pass
            self._socket_path = None
            self._socket_identity = None


async def request_owner(socket_path, payload, timeout=5):
    """Send one bounded frame to a configured local owner, without shell calls."""
    raw = _encode(payload)
    async def exchange():
        reader, writer = await asyncio.open_unix_connection(socket_path, limit=MAX_FRAME_BYTES + 1)
        try:
            writer.write(raw)
            await writer.drain()
            return _decode(await reader.readline())
        finally:
            writer.close()
            await writer.wait_closed()
    return await asyncio.wait_for(exchange(), timeout=timeout)
