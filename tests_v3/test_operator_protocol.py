"""Offline owner protocol checks: no transport or venue SDK is required."""
import copy
import asyncio
import json
import os
from pathlib import Path
import socket
import stat
import time
import uuid

import pytest

from entropy_arb.v3.operator_control import (ControlRefusal, MAX_FRAME_BYTES,
                                            OperatorControl, _decode, _encode, request_owner)


def uid():
    return str(uuid.uuid4())


class FakeOwner:
    def __init__(self):
        self.binding = dict(account_binding_hash='a' * 64, pair_identity='SNDK:HL/Lighter',
                            deployment='ordinary', batch_id='batch', config_hash='b' * 64)
        self.value = dict(intent='RUNNING', intent_revision=1, phase='MANAGING',
            risk_locks=[], blocked_reason=None, active_cycle_id='cycle', entropy_units=100,
            hedge_units=-100, position_observed_ts=time.time(), position_authoritative=True,
            order_watermark=[['ref', 'TERMINAL', 100]], allowed_actions=['start', 'stop', 'close'])
        self.operations = {}
        self.commits = []
        self.wakes = 0
        self.control = self.new_control()

    def snapshot(self):
        return copy.deepcopy(self.value)

    def operation(self, operation_id=None, *, idempotency_key=None):
        return next((copy.deepcopy(v) for v in self.operations.values()
                     if v['operation_id'] == operation_id or
                     v['idempotency_key'] == idempotency_key), None)

    def commit(self, **arguments):
        assert arguments['expected_revision'] == self.value['intent_revision']
        self.commits.append(arguments)
        operation = dict(operation_id=uid(), state='accepted', **arguments)
        self.operations[operation['operation_id']] = operation
        self.value['intent_revision'] += 1
        if arguments['action'] == 'stop':
            self.value['intent'] = 'STOPPING'
        elif arguments['action'] == 'start':
            self.value['intent'] = 'RUNNING'
        else:
            self.value['phase'] = 'EXITING'
        return copy.deepcopy(operation)

    def wake(self):
        self.wakes += 1

    def new_control(self):
        return OperatorControl(self.binding, snapshot=self.snapshot, commit=self.commit,
                               operation=self.operation, wake=self.wake)

    def request(self, op='state', **values):
        return dict(protocol_version=1, request_id=uid(), op=op,
                    owner_instance_id=self.control.owner_instance_id, **self.binding, **values)

    def preview(self, action='close'):
        return self.control.handle(self.request('preview', action=action,
            intent_revision=self.value['intent_revision'],
            expected_cycle_id='cycle' if action == 'close' else None))

    def confirmation(self, preview, action='close', key=None):
        return self.request('confirm', action=action, receipt_id=preview['receipt_id'],
            intent_revision=preview['intent_revision'], idempotency_key=key or uid(),
            expected_cycle_id=preview['expected_cycle_id'])


def test_handshake_and_preview_are_readonly_and_instance_scoped():
    owner = FakeOwner()
    request = owner.request()
    request.pop('owner_instance_id')
    result = owner.control.handle(request)
    assert result['ok'] and result['intent'] == 'RUNNING'
    assert result['owner_instance_id'] == owner.control.owner_instance_id
    assert all(result[key] == value for key, value in owner.binding.items())
    assert owner.preview()['ok'] and not owner.commits and not owner.wakes


@pytest.mark.parametrize('field', ['account_binding_hash', 'pair_identity', 'deployment',
                                  'batch_id', 'config_hash', 'owner_instance_id'])
def test_scope_or_instance_change_refuses_all_mutating_stages(field):
    owner = FakeOwner()
    preview = owner.preview()
    for request in (owner.request('preview', action='stop', intent_revision=1),
                    owner.confirmation(preview)):
        request[field] = 'different'
        assert not owner.control.handle(request)['ok']
    assert not owner.commits


@pytest.mark.parametrize('field,value', [
    ('active_cycle_id', 'new-cycle'), ('entropy_units', 99), ('hedge_units', -99),
    ('order_watermark', [['ref', 'UNKNOWN', 100]]), ('intent_revision', 2),
    ('position_authoritative', False), ('position_observed_ts', time.time() - 16)])
def test_close_confirm_rechecks_cycle_quantity_watermark_revision_and_fresh_evidence(field, value):
    owner = FakeOwner()
    preview = owner.preview()
    owner.value[field] = value
    assert not owner.control.handle(owner.confirmation(preview))['ok']
    assert not owner.commits and not owner.wakes


@pytest.mark.parametrize('field,value', [('entropy_units', True), ('hedge_units', 1.0),
    ('position_observed_ts', True), ('position_observed_ts', float('nan')),
    ('position_observed_ts', lambda: time.time() + 20), ('order_watermark', None)])
def test_close_preview_missing_or_invalid_authority_refuses(field, value):
    owner = FakeOwner()
    owner.value[field] = value() if callable(value) else value
    assert not owner.preview()['ok'] and not owner.commits


def test_expired_receipt_refuses_uncommitted_action(monkeypatch):
    owner = FakeOwner()
    preview = owner.preview()
    deadline = owner.control._receipts[preview['receipt_id']]['expires_mono']
    monkeypatch.setattr('entropy_arb.v3.operator_control.time.monotonic', lambda: deadline + .1)
    result = owner.control.handle(owner.confirmation(preview))
    assert result['reason'] == 'receipt_expired' and not owner.commits


def test_same_key_returns_durable_progress_after_lost_response_and_restart(monkeypatch):
    owner = FakeOwner()
    request = owner.confirmation(owner.preview())
    first = owner.control.handle(request)
    assert first['ok'] and len(owner.commits) == 1 and owner.wakes == 1
    op_id = first['operation']['operation_id']
    owner.operations[op_id]['state'] = 'partial'
    owner.value['intent'] = 'STOPPING'
    owner.value['intent_revision'] += 1
    owner.control = owner.new_control()
    request['owner_instance_id'] = owner.control.owner_instance_id
    request['request_id'] = uid()
    result = owner.control.handle(request)
    assert result['ok'] and result['operation']['operation_id'] == op_id
    assert result['operation']['state'] == 'partial'
    assert result['state']['intent'] == 'STOPPING'
    assert len(owner.commits) == 1 and owner.wakes == 1
    by_id = owner.control.handle(owner.request('operation', operation_id=op_id))
    by_key = owner.control.handle(owner.request('operation', idempotency_key=request['idempotency_key']))
    assert by_id['operation'] == by_key['operation']


def test_same_key_different_payload_cannot_commit_or_reuse_operation():
    owner = FakeOwner()
    request = owner.confirmation(owner.preview())
    assert owner.control.handle(request)['ok']
    request['action'] = 'stop'
    request.pop('expected_cycle_id')
    result = owner.control.handle(request)
    assert result['reason'] == 'idempotency_conflict' and len(owner.commits) == 1


def install_large_private_operation(owner):
    original = owner.commit
    def commit(**arguments):
        operation = original(**arguments)
        stored = owner.operations[operation['operation_id']]
        stored.update(receipt={'orders': [{'note': 'private-' + 'x' * MAX_FRAME_BYTES}],
                               'full_cycle': {'internal': 'y' * MAX_FRAME_BYTES}},
                      orders=[{'note': 'private internal order'}], note='private internal operation',
                      evidence=dict(entropy_units=0, hedge_units=-10, accounting_final=False,
                          net_pnl_usd=None, fees_complete=False, known_fees_usd='0.001',
                          private_note='must stay internal',
                          pending_orders=[dict(order_ref='h', venue='hedge', state='UNKNOWN',
                                               filled_units=5, reduce_only=True, note='private')],
                          legs=[dict(venue='hedge', initial_units=-100, remaining_units=-10,
                                     note='private', orders=[dict(order_ref='h', state='UNKNOWN',
                                         filled_units=5, reduce_only=True, note='private')])]))
        return copy.deepcopy(stored)
    owner.control._commit = commit


def assert_public_operation_response(response):
    assert response['ok'] and len(_encode(response)) < MAX_FRAME_BYTES
    operation = response['operation']
    assert not {'receipt', 'orders', 'note', 'request_fingerprint'} & operation.keys()
    assert operation['evidence']['net_pnl_usd'] is None
    assert operation['evidence']['hedge_units'] == -10
    assert operation['evidence']['legs'][0]['orders'][0]['state'] == 'UNKNOWN'
    assert 'private' not in json.dumps(response)


def test_large_durable_receipt_is_internal_while_confirm_and_progress_remain_bounded():
    owner = FakeOwner()
    install_large_private_operation(owner)
    request = owner.confirmation(owner.preview())
    confirmation = owner.control.handle(request)
    op_id = confirmation['operation']['operation_id']
    assert len(json.dumps(owner.operations[op_id])) > MAX_FRAME_BYTES
    assert_public_operation_response(confirmation)
    assert_public_operation_response(owner.control.handle(owner.request('operation', operation_id=op_id)))
    assert_public_operation_response(owner.control.handle(request))
    assert len(owner.commits) == 1


def test_older_close_confirmation_cannot_restore_running_after_stop():
    owner = FakeOwner()
    close = owner.confirmation(owner.preview())
    stop = owner.confirmation(owner.preview('stop'), action='stop')
    assert owner.control.handle(stop)['ok']
    assert owner.value['intent'] == 'STOPPING'
    assert owner.control.handle(close)['reason'] == 'intent_revision_changed'
    fresh = owner.confirmation(owner.preview())
    assert owner.control.handle(fresh)['ok']
    assert owner.value['intent'] == 'STOPPING' and owner.value['phase'] == 'EXITING'


@pytest.mark.parametrize('lock', ['MANUAL_STOP', 'REALIZED_LOSS_BUDGET', 'MAX_POSITION_CYCLES'])
def test_start_preserves_and_refuses_risk_locks(lock):
    owner = FakeOwner()
    owner.value['risk_locks'] = [lock]
    assert owner.preview('start')['reason'] == 'risk_locked'
    assert owner.value['risk_locks'] == [lock] and not owner.commits
    assert owner.preview('stop')['ok']


@pytest.mark.parametrize('field', ['path', 'journal', 'account', 'accounts', 'argv', 'env', 'command'])
def test_unknown_fields_and_path_or_launch_inputs_are_refused(field):
    owner = FakeOwner()
    request = owner.request()
    request[field] = '/tmp/ignored'
    assert owner.control.handle(request)['reason'] == 'unknown_field'


@pytest.mark.parametrize('opening', [False, True])
def test_parameter_protocol_preserves_five_or_seven_field_payload(opening):
    owner = FakeOwner()
    calls = []
    owner.control._save_parameters = lambda **values: calls.append(values) or {}
    owner.control._parameter_save = lambda key: {}
    values = dict(entry_band_bps='5', exit_min_gross_profit_bps='2.5',
        entropy_exit_room_bps='12', lighter_exit_room_bps='12', max_notional_usd='200')
    if opening:
        values.update(entropy_open_room_bps='0', lighter_open_room_bps=None)
    request = owner.request('parameters_save', expected_revision=0,
        idempotency_key=uid(), values=values)
    response = owner.control.handle(request)
    assert response['ok'] is True
    assert calls == [dict(expected_revision=0, idempotency_key=request['idempotency_key'], values=values)]
    assert not owner.commits and not owner.wakes


@pytest.mark.parametrize('change', ['missing_required', 'only_one_opening', 'extra'])
def test_parameter_protocol_rejects_incomplete_or_unknown_fields_before_callback(change):
    owner = FakeOwner()
    calls = []
    owner.control._save_parameters = lambda **values: calls.append(values) or {}
    owner.control._parameter_save = lambda key: {}
    values = dict(entry_band_bps='5', exit_min_gross_profit_bps='2.5',
        entropy_exit_room_bps='12', lighter_exit_room_bps='12', max_notional_usd='200')
    if change == 'missing_required':
        values.pop('entry_band_bps')
    elif change == 'only_one_opening':
        values['entropy_open_room_bps'] = None
    else:
        values['unknown'] = '0'
    response = owner.control.handle(owner.request('parameters_save', expected_revision=0,
        idempotency_key=uid(), values=values))
    assert response['ok'] is False and response['reason'] == 'invalid_parameters_fields'
    assert not calls and not owner.commits and not owner.wakes


@pytest.mark.parametrize('patch', [dict(protocol_version=True), dict(protocol_version=2),
    dict(request_id='not-uuid'), dict(op='trade'), dict(intent_revision=True), dict(action='kill')])
def test_strict_protocol_fields(patch):
    owner = FakeOwner()
    request = owner.request('preview', action='stop', intent_revision=1)
    request.update(patch)
    assert not owner.control.handle(request)['ok']


def test_bounded_frames_reject_duplicates_nonfinite_unterminated_and_oversized():
    assert _decode(_encode({'ok': True})) == {'ok': True}
    for raw in (b'{"op":1,"op":2}\n', b'{"x":NaN}\n', b'{}', b'x' * (MAX_FRAME_BYTES + 1)):
        with pytest.raises(ControlRefusal):
            _decode(raw)
    owner = FakeOwner()
    request = owner.request()
    request['extra'] = 'x' * MAX_FRAME_BYTES
    assert owner.control.handle(request)['reason'] == 'frame_too_large'


def test_async_callback_is_rejected_without_starting_or_trading():
    owner = FakeOwner()
    async def commit(**kwargs):
        raise AssertionError('must not await a trading callback')
    with pytest.raises(ControlRefusal, match='callbacks_must_be_sync'):
        OperatorControl(owner.binding, snapshot=owner.snapshot, commit=commit,
                        operation=owner.operation, wake=owner.wake)


def test_independent_consumer_commits_stop_while_trading_owner_is_awaiting():
    owner = FakeOwner()
    async def scenario():
        request = owner.confirmation(owner.preview('stop'), action='stop')
        trading_wait = asyncio.Event()
        trading = asyncio.create_task(trading_wait.wait())
        consumer = asyncio.create_task(owner.control._consume())
        future = asyncio.get_running_loop().create_future()
        owner.control._queue.put_nowait((request, future))
        try:
            response = await asyncio.wait_for(future, .5)
            assert response['ok'] and owner.value['intent'] == 'STOPPING'
            assert not trading.done() and owner.wakes == 1
        finally:
            consumer.cancel()
            trading.cancel()
            await asyncio.gather(consumer, trading, return_exceptions=True)
    asyncio.run(scenario())


@pytest.fixture
def socket_dir():
    root = os.environ.get('ENTROPY_PROTOCOL_TEST_SOCKET_ROOT')
    if not root:
        pytest.skip('requires the isolated runner with an explicit temporary AF_UNIX root')
    path = Path(root) / uuid.uuid4().hex[:8]
    path.mkdir(mode=0o700)
    return path


def test_uds_permissions_and_queue_commit_while_trade_is_awaiting(socket_dir):
    owner = FakeOwner()
    path = socket_dir / 's'
    async def scenario():
        await owner.control.start(path)
        waiting = asyncio.Event()
        trade = asyncio.create_task(waiting.wait())
        try:
            assert stat.S_IMODE(socket_dir.stat().st_mode) == 0o700
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
            state = await request_owner(path, owner.request())
            assert state['ok'] and state['owner_instance_id'] == owner.control.owner_instance_id
            preview = await request_owner(path, owner.request('preview', action='stop', intent_revision=1))
            result = await request_owner(path, owner.confirmation(preview, action='stop'))
            assert result['ok'] and owner.value['intent'] == 'STOPPING'
            assert len(owner.commits) == 1 and not trade.done()
        finally:
            trade.cancel()
            await asyncio.gather(trade, return_exceptions=True)
            await owner.control.close()
        assert not path.exists()
    asyncio.run(scenario())


def test_uds_lost_response_and_restart_replay_return_one_durable_operation(socket_dir):
    owner = FakeOwner()
    path = socket_dir / 's'
    async def scenario():
        await owner.control.start(path)
        request = owner.confirmation(owner.preview())
        try:
            reader, writer = await asyncio.open_unix_connection(path)
            writer.write(_encode(request))
            await writer.drain()
            writer.close()  # Do not observe the confirmation ACK.
            await writer.wait_closed()
            for _ in range(20):
                if owner.commits:
                    break
                await asyncio.sleep(.01)
            assert len(owner.commits) == 1
            operation = owner.operation(idempotency_key=request['idempotency_key'])
            owner.operations[operation['operation_id']]['state'] = 'unknown_recovery'
        finally:
            await owner.control.close()
        owner.control = owner.new_control()
        await owner.control.start(path)
        try:
            state = await request_owner(path, owner.request())
            request['owner_instance_id'] = state['owner_instance_id']
            request['request_id'] = uid()
            replay = await request_owner(path, request)
            assert replay['ok'] and replay['operation']['operation_id'] == operation['operation_id']
            assert replay['operation']['state'] == 'unknown_recovery'
            assert len(owner.commits) == 1 and owner.wakes == 1
        finally:
            await owner.control.close()
    asyncio.run(scenario())


def test_uds_ack_and_operation_lookup_survive_large_internal_receipt(socket_dir):
    owner = FakeOwner()
    install_large_private_operation(owner)
    path = socket_dir / 's'
    async def scenario():
        await owner.control.start(path)
        try:
            preview = await request_owner(path, owner.request('preview', action='close',
                intent_revision=1, expected_cycle_id='cycle'))
            request = owner.confirmation(preview)
            confirmation = await request_owner(path, request)
            assert_public_operation_response(confirmation)
            progress = await request_owner(path, owner.request('operation',
                operation_id=confirmation['operation']['operation_id']))
            assert_public_operation_response(progress)
            assert_public_operation_response(await request_owner(path, request))
            assert len(owner.commits) == 1
        finally:
            await owner.control.close()
    asyncio.run(scenario())


def test_uds_existing_active_owner_is_not_removed_or_replaced(socket_dir):
    owner = FakeOwner()
    peer = owner.new_control()
    path = socket_dir / 's'
    async def scenario():
        await owner.control.start(path)
        identity = path.stat().st_ino
        try:
            with pytest.raises(ControlRefusal, match='owner_socket_already_active'):
                await peer.start(path)
            assert path.stat().st_ino == identity
            assert (await request_owner(path, owner.request()))['ok']
        finally:
            await peer.close()
            await owner.control.close()
    asyncio.run(scenario())


def test_uds_owned_stale_socket_replaced_but_cleanup_preserves_replacement_inode(socket_dir):
    owner = FakeOwner()
    path = socket_dir / 's'
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(path))
    stale.close()
    async def scenario():
        await owner.control.start(path)
        assert (await request_owner(path, owner.request()))['ok']
        path.unlink()
        replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        replacement.bind(str(path))
        replacement.listen(1)
        identity = path.stat().st_ino
        try:
            await owner.control.close()
            assert path.stat().st_ino == identity
        finally:
            replacement.close()
            path.unlink()
    asyncio.run(scenario())


def test_uds_owner_appearing_between_path_check_and_bind_is_never_unlinked(socket_dir, monkeypatch):
    owner = FakeOwner()
    path = socket_dir / 's'
    peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    original_bind = socket.socket.bind
    def racing_bind(listener, address):
        if listener is not peer and address == str(path):
            original_bind(peer, address)
            peer.listen(1)
        return original_bind(listener, address)
    monkeypatch.setattr(socket.socket, 'bind', racing_bind)
    async def scenario():
        try:
            with pytest.raises(OSError):
                await owner.control.start(path)
            identity = path.stat().st_ino
            await owner.control.close()
            assert path.stat().st_ino == identity and owner.control._server is None
        finally:
            peer.close()
            path.unlink()
    asyncio.run(scenario())


@pytest.mark.parametrize('unsafe', ['directory_mode', 'directory_symlink', 'socket_symlink',
                                   'regular_file', 'wrong_owner'])
def test_uds_rejects_unsafe_directory_or_path(socket_dir, unsafe, monkeypatch):
    owner = FakeOwner()
    path = socket_dir / 's'
    if unsafe == 'directory_mode':
        socket_dir.chmod(0o755)
    elif unsafe == 'directory_symlink':
        link = socket_dir.parent / ('l' + uuid.uuid4().hex[:8])
        link.symlink_to(socket_dir, target_is_directory=True)
        path = link / 's'
    elif unsafe == 'socket_symlink':
        target = socket_dir / 'file'
        target.write_text('preserve')
        path.symlink_to(target)
    elif unsafe == 'regular_file':
        path.write_text('preserve')
    else:
        monkeypatch.setattr('entropy_arb.v3.operator_control.os.getuid', lambda: os.stat(socket_dir).st_uid + 1)
    async def scenario():
        with pytest.raises(ControlRefusal, match='unsafe_socket_'):
            await owner.control.start(path)
        await owner.control.close()
        assert not owner.commits
        if unsafe in {'socket_symlink', 'regular_file'}:
            assert path.read_text() == 'preserve'
    asyncio.run(scenario())


@pytest.mark.parametrize('frame', [b'{"op":1,"op":2}\n', b'{}', b'x' * (MAX_FRAME_BYTES + 2) + b'\n'])
def test_uds_rejects_invalid_frame_without_state_or_commit_callback(socket_dir, frame):
    owner = FakeOwner()
    path = socket_dir / 's'
    snapshots = []
    def snapshot():
        snapshots.append(True)
        return owner.snapshot()
    owner.control = OperatorControl(owner.binding, snapshot=snapshot, commit=owner.commit,
                                    operation=owner.operation, wake=owner.wake)
    async def scenario():
        await owner.control.start(path)
        try:
            reader, writer = await asyncio.open_unix_connection(path)
            writer.write(frame)
            await writer.drain()
            if not frame.endswith(b'\n'):
                writer.write_eof()
            result = _decode(await reader.readline())
            assert not result['ok'] and not owner.commits and not snapshots
            writer.close()
            await writer.wait_closed()
        finally:
            await owner.control.close()
    asyncio.run(scenario())
