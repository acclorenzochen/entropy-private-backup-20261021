"""Offline SDK/WS/REST diagnostics; no real transport, account, or order."""
import asyncio
import json
import math
from pathlib import Path
from types import SimpleNamespace

import pytest

import entropy_arb.order_timing as timing_module
import entropy_arb.venue_lighter as lighter_module
from entropy_arb.order_timing import OrderTimingCollector, stamp
from entropy_arb.v3.core import Instrument, Side
from entropy_arb.v3.journal import Intent
from entropy_arb.v3.live_host import VenueAdapterBroker, consume_order_timing
from entropy_arb.v3.market_observer import BufferedJSONLObserver
from entropy_arb.venue_lighter import AccountOrdersFeed, SDK_REQUEST_CONTEXT
from tests_v3.test_lighter_account_ws import account_frame
from tests_v3.test_lighter_request_integration import (
    make_venue, Response, install_signer, send, no_network,
)
from tests_v3.test_runtime import setup_runtime
from decimal import Decimal as D


def frame(status='filled', qty='.01', quote='1', *, coi=101, order_id=777, updated=1700000000):
    return dict(type='update/account_orders', timestamp=1700000000999,
        orders={'139': [dict(client_order_index=coi, order_index=order_id,
            status=status, filled_base_amount=qty, filled_quote_amount=quote,
            timestamp=updated, secret='FAKE_SECRET_NEVER_LOG')]})


def trade(trade_id=1, execution=1700000000123456, *, coi=101, account=42):
    return dict(trade_id_str=str(trade_id), market_id=139, ask_account_id=account,
        bid_account_id=99, ask_client_id_str=str(coi), ask_id_str='777',
        size='.005', price='100', timestamp=1700000000123,
        transaction_time=execution, tx_hash='FAKE_SIGNATURE_NEVER_LOG')


def instrument():
    return Instrument('pair', D('.0001'), D('.01'), D('.01'), D(0), D(0))


def test_trade_units_dedup_coverage_and_order_update_never_fabricate_complete_fill():
    events = []
    collector = OrderTimingCollector(events.append, 'clock-one')
    trace = collector.get('101', create=True)
    feed = AccountOrdersFeed('test', 'unused', 139, 42, None, timing_collector=collector)
    feed._handle_orders(frame())
    assert trace.data['exchange_first_observed_fill_ts'] is None
    assert trace.data['exchange_order_update_block_s'] == 1700000000
    rows = [trade(), trade(), trade(2, 1700000000456789), trade(3, 0), trade(4, None),
            trade(5, account=999), {**trade(6), 'size': 10 ** 400}]
    feed._handle_account({**account_frame(), 'trades': {'139': rows}})
    trace.consumed()
    last = events[-1]
    assert len(last['trades']) == 4 and last['trade_duplicates'] == 1
    assert last['trade_execution_time_missing'] == 2
    assert last['exchange_first_execution_us'] == 1700000000123456
    assert last['exchange_last_execution_us'] == 1700000000456789
    assert last['exchange_first_observed_fill_ts'] == pytest.approx(1700000000.123456)
    assert last['trade_coverage'] == 'observed_subset_not_complete'
    assert 'exchange_fill_ts' not in last and 'exchange_first_fill_ts' not in last
    assert feed.account_snapshot is not None and feed.account_ready.is_set()
    encoded = json.dumps(last, allow_nan=False)
    assert 'FAKE_SECRET' not in encoded and 'FAKE_SIGNATURE' not in encoded


def test_late_32_trades_emit_one_snapshot_per_frame_without_changing_account_or_terminal():
    events = []
    collector = OrderTimingCollector(events.append, 'clock')
    trace = collector.get('101', create=True)
    feed = AccountOrdersFeed('test', 'unused', 139, 42, None, timing_collector=collector)
    feed._handle_orders(frame(qty='1', quote='100'))
    trace.consumed()
    original_terminal = dict(feed.terminal(101))
    feed._handle_account({**account_frame(), 'trades': {'139': [
        {**trade(i + 1, 1700000000123456 + i), 'size': str(1 / 32)} for i in range(32)]}})
    assert len(events) == 2 and len(events[-1]['trades']) == 32
    assert feed.account_ready.is_set() and feed.terminal(101) == original_terminal
    assert events[-1]['version'] == 2


def test_collector_bounded_lists_eviction_generation_and_late_partial_average():
    events = []
    collector = OrderTimingCollector(events.append, 'clock', capacity=2)
    old = collector.get('101', create=True)
    for i in range(12):
        old.request('http_requests', method='POST', endpoint='/api/v1/sendTx')
        old.request('lookups', source='accountOrders_rest')
    for i in range(40):
        old.trade(trade(i + 1), order_id='777', received=stamp(), parsed=stamp(), boundary='handler_call')
    assert len(old.data['http_requests']) == len(old.data['lookups']) == 8
    assert old.data['http_requests_dropped'] == old.data['lookups_dropped'] == 4
    assert len(old.data['trades']) == 32 and old.data['trades_dropped'] == 8
    old.result(status='filled', filled_base=.01, average=100)
    old.result(status='filled', filled_base=.005, average=200)
    assert old.data['cumulative_filled_base'] == .01 and old.data['average_px'] == 100
    collector.get('102', create=True)
    collector.get('103', create=True)
    rebuilt = collector.get('101', create=True)
    assert rebuilt.data['trace_generation'] > old.data['trace_generation']
    rebuilt.consumed()
    assert events[-1]['collector_evicted_total'] == 2
    assert events[-1]['correlation_missing'] == ['cycle_id', 'step_key', 'attempt']
    assert len(collector.orders) == 2


def test_bad_diagnostic_clock_sink_collector_and_aggregate_do_not_break_account_or_terminal(monkeypatch):
    collector = OrderTimingCollector(lambda event: (_ for _ in ()).throw(RuntimeError('sink failure')), 'clock')
    trace = collector.get('101', create=True)
    trace.trade({**trade(), 'size': '1e308'}, order_id=777, received=stamp(), parsed=stamp(), boundary='handler_call')
    trace.trade({**trade(2), 'size': '1e308'}, order_id=777, received=stamp(), parsed=stamp(), boundary='handler_call')
    assert trace.data['trade_quantity_aggregate_invalid'] and math.isfinite(trace.data['observed_trade_base_sum'])
    trace.consumed()
    assert collector.dropped == 1
    monkeypatch.setattr(collector, 'get', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('collector failure')))
    feed = AccountOrdersFeed('test', 'unused', 139, 42, None, timing_collector=collector)
    feed._handle_account({**account_frame(), 'trades': {'139': [trade()]}})
    feed._handle_orders(frame())
    assert feed.account_ready.is_set() and feed.terminal(101)['filled_base'] == .01


def test_ws_recv_timestamp_precedes_parse_and_terminal_with_first_last_and_envelope(monkeypatch):
    events, boundaries = [], []
    collector = OrderTimingCollector(events.append, 'clock')
    trace = collector.get('101', create=True)
    feed = AccountOrdersFeed('test', 'unused', 139, 42,
        SimpleNamespace(create_auth_token_with_expiry=lambda: ('FAKE_AUTH_NEVER_LOG', None)),
        timing_collector=collector)
    stop = asyncio.Event()
    messages = [dict(type='connected'), frame('open', '.005', '.5'), frame()]
    class WS:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return False
        async def recv(self):
            message = messages.pop(0)
            if not messages: stop.set()
            boundaries.append('recv_return')
            return json.dumps(message)
        async def send(self, raw): pass
    real_loads = json.loads
    def parse(raw):
        boundaries.append('parse')
        return real_loads(raw)
    counter = 0
    def clock_stamp():
        nonlocal counter
        counter += 1
        boundaries.append('stamp')
        # Wall time may jump backwards; mono must remain the duration source.
        return dict(wall_ts=1700000000 - counter, mono_ns=counter * 1000)
    monkeypatch.setattr(lighter_module, 'ws_connect', lambda *a, **k: WS())
    monkeypatch.setattr(lighter_module.json, 'loads', parse)
    monkeypatch.setattr(lighter_module, 'stamp', clock_stamp)
    monkeypatch.setattr(timing_module, 'stamp', clock_stamp)
    asyncio.run(feed.run(stop))
    stages = trace.data['stages']
    assert stages['ws_raw_received_first']['mono_ns'] < stages['ws_parsed_first']['mono_ns']
    assert stages['ws_raw_received_first']['mono_ns'] < stages['ws_raw_received_last']['mono_ns']
    assert trace.data['order_updates_seen'] == 2
    assert trace.data['ws_receive_boundary'] == 'ws_recv'
    assert trace.data['ws_envelope_published_ms'] == 1700000000999
    for i, name in enumerate(boundaries):
        if name == 'recv_return': assert boundaries[i + 1:i + 3] == ['stamp', 'parse']


def test_ws_wins_before_sdk_drain_and_late_wrapper_snapshot_has_no_new_requests(monkeypatch):
    async def scenario():
        venue, _, _, _ = make_venue(monkeypatch, lambda *a: Response({}), feed=True)
        events = []
        venue.configure_order_timing(events.append, 'clock')
        gate = asyncio.Event()
        async def delay(method, path):
            if method == 'POST': await gate.wait()
        signer, rest = install_signer(venue, callback=delay,
            before_order=lambda: venue.orders_feed._handle_orders(frame()))
        result = await asyncio.wait_for(send(venue), .2)
        assert result['filled_base'] == .01 and result['confirmation_source'] == 'account_orders_ws'
        trace = venue.order_timing('101')
        trace.consumed()
        assert 'submit_wrapper_done' not in events[-1]['stages']
        assert len(venue._submit_tasks) == 1
        gate.set()
        await venue.wait_for_submission()
        await asyncio.sleep(0)
        assert 'submit_wrapper_done' in events[-1]['stages']
        assert len(rest.calls) == len(signer.orders) == 1
        assert events[-1]['terminal_evidence_source'] == 'account_orders_ws'
    asyncio.run(scenario())


def test_prepared_dispatch_covers_prior_sdk_wait_before_executor_or_submit_creates_trace(monkeypatch):
    async def scenario():
        venue, _, _, _ = make_venue(monkeypatch, lambda *a: Response({}))
        venue.configure_order_timing(lambda event: None, 'clock')
        install_signer(venue)
        gate = asyncio.Event()
        prior = asyncio.create_task(gate.wait())
        venue._submit_tasks.add(prior)
        preparing = asyncio.create_task(venue.prepare_dispatch(order_ref='101'))
        await asyncio.sleep(0)
        trace = venue.order_timing('101')
        assert trace is not None and 'prepare_start' in trace.data['stages']
        assert 'submission_lock_acquired' in trace.data['stages']
        assert 'prior_submission_drained' not in trace.data['stages']
        assert not preparing.done()
        gate.set()
        prepared = await preparing
        assert trace.data['stages']['prepare_start']['mono_ns'] <= trace.data['stages']['prior_submission_drained']['mono_ns']
        result = await venue.send_taker(is_buy=True, qty=.01, limit_px=100,
            order_ref='101', dispatch_token=prepared)
        assert result['status'] == 'sent-unconfirmed'
        assert trace.data['prepare_attempts'] == 1
    asyncio.run(scenario())


def test_concurrent_sdk_requests_share_order_context_but_keep_request_rows_separate(monkeypatch):
    async def scenario():
        venue, _, _, _ = make_venue(monkeypatch, lambda *a: Response({}))
        venue.configure_order_timing(lambda event: None, 'clock')
        async def interleave(method, path): await asyncio.sleep(0)
        _, rest = install_signer(venue, callback=interleave)
        trace = venue.order_timing('101', create=True)
        ticket = venue._request_budget().reserve(2, priority=0)
        context = dict(ticket=ticket, priority=0, order_ref='101', post_started=False, timing_trace=trace)
        token = SDK_REQUEST_CONTEXT.set(context)
        try:
            await asyncio.gather(rest.request('GET', 'https://lighter.invalid/api/v1/nextNonce'),
                                 rest.request('POST', 'https://lighter.invalid/api/v1/sendTx'))
        finally:
            SDK_REQUEST_CONTEXT.reset(token)
        first, second = trace.data['http_requests']
        assert first['method'] == 'GET' and second['method'] == 'POST'
        assert first['sequence'] == 1 and second['sequence'] == 2
        assert all('pool_wrapper_start' in row and 'pool_wrapper_end' in row for row in (first, second))
        assert first is not second and len(rest.calls) == 2
    asyncio.run(scenario())


@pytest.mark.parametrize('timeout', [3, (1, 4), None, 10 ** 400, object()])
def test_sdk_pool_wrapper_records_actual_override_and_never_headers_body_or_credentials(monkeypatch, timeout):
    async def scenario():
        venue, _, _, _ = make_venue(monkeypatch, lambda *a: Response({}))
        events = []
        venue.configure_order_timing(events.append, 'clock')
        _, rest = install_signer(venue)
        trace = venue.order_timing('101', create=True)
        ticket = venue._request_budget().reserve(1, priority=0)
        context = dict(ticket=ticket, priority=0, order_ref='101', post_started=False, timing_trace=trace)
        token = SDK_REQUEST_CONTEXT.set(context)
        try:
            await rest.request('POST', 'https://lighter.invalid/api/v1/sendTx?FAKE_AUTH_NEVER_LOG',
                headers={'Authorization': 'FAKE_AUTH_NEVER_LOG'}, body='FAKE_SIGNATURE_NEVER_LOG',
                _request_timeout=timeout)
        finally:
            SDK_REQUEST_CONTEXT.reset(token)
        trace.consumed()
        row = events[-1]['http_requests'][0]
        unknown = type(timeout) is object or type(timeout) is int and timeout > 10 ** 300
        assert row['request_timeout'] == (None if unknown else list(timeout) if isinstance(timeout, tuple) else timeout)
        assert row['request_timeout_known'] is not unknown
        assert row['start']['mono_ns'] <= row['pool_wrapper_start']['mono_ns'] <= row['pool_wrapper_end']['mono_ns'] <= row['end']['mono_ns']
        assert 'headers_received' not in row and 'body_read_end' not in row
        assert 'FAKE_AUTH' not in json.dumps(events) and 'FAKE_SIGNATURE' not in json.dumps(events)
    asyncio.run(scenario())


def test_sdk_timeout_and_settlement_timeout_have_distinct_sources(monkeypatch):
    async def scenario():
        venue, _, _, _ = make_venue(monkeypatch, lambda *a: Response({}))
        events = []
        venue.configure_order_timing(events.append, 'clock')
        async def timeout(method, path): raise asyncio.TimeoutError('FAKE_SECRET_NEVER_LOG')
        install_signer(venue, callback=timeout)
        result = await send(venue)
        venue.order_timing('101').consumed()
        assert result['status'] == 'send-failed' and result['unresolved']
        assert events[-1]['timeout_reason'] == 'sdk_request_timeout'
        assert events[-1]['http_requests'][0]['outcome'] == 'timeout'
        assert 'FAKE_SECRET' not in json.dumps(events)
        venue2, _, _, _ = make_venue(monkeypatch, lambda *a: Response({}), feed=True)
        events2 = []
        venue2.configure_order_timing(events2.append, 'clock')
        install_signer(venue2)
        result2 = await send(venue2)
        venue2.order_timing('101').consumed()
        assert result2['status'] == 'timeout'
        assert events2[-1]['timeout_reason'] == 'settlement_wait_timeout'
        assert 'settlement_wait_start' in events2[-1]['stages']
    asyncio.run(scenario())


def test_rest_recovery_and_cached_terminal_keep_real_source_without_extra_rest_or_journal_fields(monkeypatch):
    async def scenario():
        order = frame()['orders']['139'][0]
        venue, session, _, _ = make_venue(monkeypatch, lambda *a: Response({'orders': [order]}), feed=True)
        events = []
        venue.configure_order_timing(events.append, 'clock')
        broker = VenueAdapterBroker(venue, venue_key='hedge', venue_kind='lighter', instrument=instrument())
        intent = Intent('101', 'cycle', 'hedge', Side.BUY, 100, D('100'), True)
        result = await broker.resolve(intent, timeout=.02)
        assert result.terminal and result.cumulative_units == 100
        consume_order_timing(broker, intent)
        assert events[-1]['terminal_evidence_source'] == 'accountOrders_rest'
        assert events[-1]['exchange_order_id'] == '777'
        assert 'ws_raw_received_first' not in events[-1]['stages']
        assert events[-1]['normalized_result_confirmation_source'] is None
        assert events[-1]['lookups'][0]['start']['mono_ns'] <= events[-1]['lookups'][0]['end']['mono_ns']
        await broker.resolve(intent, timeout=.02)
        assert len(session.calls) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize('source', [{'Authorization': 'FAKE_SECRET_NEVER_LOG'}, 'FAKE_SECRET_NEVER_LOG'])
def test_normalized_source_is_an_enum_not_a_raw_object_or_arbitrary_secret(monkeypatch, source):
    venue, _, _, _ = make_venue(monkeypatch, lambda *a: Response({}))
    events = []
    venue.configure_order_timing(events.append, 'clock')
    broker = VenueAdapterBroker(venue, venue_key='hedge', venue_kind='lighter', instrument=instrument())
    intent = Intent('101', 'cycle', 'hedge', Side.BUY, 100, D('100'), True)
    result = broker._result(intent, dict(order_ref='101', status='filled',
        filled_base=.01, avg_px=100, unresolved=False, confirmation_source=source))
    assert result.terminal and result.cumulative_units == 100
    consume_order_timing(broker, intent)
    assert events[-1]['normalized_result_confirmation_source'] == 'unknown'
    assert 'FAKE_SECRET' not in json.dumps(events)


def test_runtime_queue_drop_counter_and_existing_optional_broker_compatibility(tmp_path):
    runtime, _, _, journal = setup_runtime(tmp_path)
    try:
        events = []
        runtime.observer = SimpleNamespace(publish=events.append)
        collector = OrderTimingCollector(runtime._queue_order_timing, runtime.clock_session_id)
        trace = collector.get('101', create=True)
        runtime._telemetry_queue = [lambda: None] * 256
        trace.consumed()
        assert runtime.telemetry_dropped == 1 and len(runtime._telemetry_queue) == 256
        runtime._flush_telemetry()
        trace.consumed()
        runtime._flush_telemetry()
        assert events[-1]['runtime_telemetry_dropped_total'] == 1
        consume_order_timing(SimpleNamespace(), Intent('101', 'cycle', 'hedge', Side.BUY, 1, D('100'), True))
    finally:
        journal.close()


def test_recovery_diagnostic_failure_preserves_original_business_exception_order(tmp_path, monkeypatch):
    runtime, _, _, journal = setup_runtime(tmp_path)
    try:
        calls = []
        rows = [dict(order_ref='101', cycle_id='cycle', venue='hedge', side='buy',
            requested_units=1, limit_px='100', reduce_only=True),
            dict(order_ref='102', cycle_id='cycle', venue='hedge', side='invalid',
            requested_units=1, limit_px='100', reduce_only=True)]
        monkeypatch.setattr(journal, 'pending', lambda: rows)
        runtime.brokers = dict(hedge=SimpleNamespace())
        async def original_recovery(timeout):
            for row in rows:
                Side(row['side'])
                calls.append(row['order_ref'])
        runtime.executor = SimpleNamespace(recover_pending=original_recovery)
        with pytest.raises(ValueError, match='invalid'):
            asyncio.run(runtime._resolve_pending())
        assert calls == ['101']
    finally:
        journal.close()


def test_append_only_keeps_existing_history_and_bounded_queue_reports_gaps(tmp_path):
    async def scenario():
        path = tmp_path / 'observer.jsonl'
        old = b'old research bytes\n'
        path.write_bytes(old)
        archive = tmp_path / 'observer.20000101T000000Z-1.jsonl'
        archive.write_bytes(b'preserve archive')
        observer = BufferedJSONLObserver(path, dict(kind='test_session'), queue_size=1,
            batch_size=1, flush_seconds=.001, rotate_seconds=.001,
            max_file_bytes=1, retain_seconds=.001, max_total_bytes=1, append_only=True)
        observer.start()
        for i in range(3): observer.publish(dict(kind='v3_lighter_order_timing', version=i))
        assert observer.dropped_events == 2
        await observer.close()
        assert path.read_bytes().startswith(old)
        assert archive.read_bytes() == b'preserve archive'
        assert len(list(tmp_path.glob('observer.*.jsonl'))) == 1
        assert b'gap' in path.read_bytes()
    asyncio.run(scenario())


def test_shutdown_flush_timeout_is_bounded_and_published_without_erasing_halted_reason(tmp_path):
    runtime, _, _, journal = setup_runtime(tmp_path)
    try:
        runtime.status.write('HALTED', 'original failure')
        class SlowObserver:
            dropped_events = 7
            aborted = False
            async def close(self): await asyncio.Event().wait()
            def abort(self): self.aborted = True
        observer = SlowObserver()
        asyncio.run(runtime._close_order_observer(observer, timeout=.01))
        payload = json.loads(runtime.status.path.read_text())
        assert observer.aborted and payload['observer_flush_incomplete']
        assert payload['observer_dropped_events'] == 7
        assert payload['current_state'] == 'HALTED' and payload['reason'] == 'original failure'
    finally:
        journal.close()
