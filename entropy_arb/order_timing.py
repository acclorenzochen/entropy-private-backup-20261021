"""Bounded, best-effort order diagnostics. No payloads, I/O, or trading state."""
from collections import OrderedDict
from copy import deepcopy
from contextvars import ContextVar
import math
import time
from types import SimpleNamespace

import aiohttp


HTTP_TIMING_CONTEXT = ContextVar('order_http_timing', default=None)


def attach_http_timing(session):
    """Observe the existing pool. Hooks report local boundaries, never wire time."""
    configs = getattr(session, 'trace_configs', None)
    if not isinstance(configs, list) or not isinstance(session, aiohttp.ClientSession):
        return
    if any(getattr(config, '_entropy_order_timing', False) for config in configs):
        return
    config = aiohttp.TraceConfig(trace_config_ctx_factory=lambda **_: SimpleNamespace(
        order_timing=HTTP_TIMING_CONTEXT.get()))
    hooks = {
        'on_request_start': 'request_start', 'on_request_end': 'request_end',
        'on_connection_queued_start': 'connection_queued_start',
        'on_connection_queued_end': 'connection_queued_end',
        'on_connection_create_start': 'connection_create_start',
        'on_connection_create_end': 'connection_create_end',
        'on_connection_reuseconn': 'connection_reuse',
        'on_dns_resolvehost_start': 'dns_resolvehost_start',
        'on_dns_resolvehost_end': 'dns_resolvehost_end',
        'on_dns_cache_hit': 'dns_cache_hit', 'on_dns_cache_miss': 'dns_cache_miss',
        'on_request_headers_sent': 'request_headers_callback',
        'on_request_chunk_sent': 'request_chunk_callback_last',
        'on_response_chunk_received': 'response_body_read_done',
    }
    for hook, stage in hooks.items():
        async def observed(_session, context, _params, stage=stage):
            try:
                bound = context.order_timing
                if bound is not None:
                    trace, row = bound
                    when = stamp()
                    trace.request_mark(row, stage, when)
                    if stage == 'request_chunk_callback_last':
                        trace.request_mark(row, 'request_chunk_callback_first', when, first=True)
            except Exception:
                pass  # Diagnostics cannot alter the request's outcome.
        getattr(config, hook).append(observed)
    config._entropy_order_timing = True
    config.freeze()
    configs.append(config)


def stamp():
    try:
        return {'wall_ts': time.time(), 'mono_ns': time.monotonic_ns()}
    except Exception:
        return {'wall_ts': None, 'mono_ns': None}



def safe_stamp(value):
    """Only local numeric clock fields, never a raw hook/context object."""
    value = value if isinstance(value, dict) else {}
    wall, mono = value.get('wall_ts'), value.get('mono_ns')
    try:
        wall = wall if type(wall) in (int, float) and math.isfinite(wall) else None
    except (OverflowError, ValueError, TypeError):
        wall = None
    mono = mono if type(mono) is int and 0 <= mono < 2 ** 63 else None
    return {'wall_ts': wall, 'mono_ns': mono}


_ORDER_SPANS = {
    'sdk_entry_to_nonce_selection': ('submit_wrapper_start', 'sdk_nonce_select_start', 'Local submission entry to nonce selection; includes preparation and SDK nonce-key lock wait when used'),
    'sdk_nonce_selection': ('sdk_nonce_select_start', 'sdk_nonce_select_end', 'SDK nonce selection wrapper; may include its own HTTP request'),
    'sdk_signing': ('sdk_sign_start', 'sdk_sign_end', 'SDK local signing wrapper'),
    'sdk_submission_wrapper': ('submit_wrapper_start', 'submit_wrapper_done', 'SDK submission wrapper; not exchange processing time'),
    'settlement_wait': ('settlement_wait_start', 'settlement_wait_end', 'Local settlement wait, after SDK return when this path is used'),
    'submit_to_account_ws_receive': ('submit_wrapper_start', 'ws_raw_received_first', 'Local submission entry to account WS recv return'),
    'account_ws_parse': ('ws_raw_received_first', 'ws_parsed_first', 'Local account WS recv return to parsed frame'),
    'account_ws_publish': ('ws_parsed_first', 'terminal_published_first', 'Local parsed frame to terminal publication'),
    'terminal_to_runtime_consumption': ('terminal_published_first', 'runtime_consumed_first', 'Local terminal publication to first business consumption'),
    'account_ws_to_business_progress': ('ws_parsed_first', 'runtime_progress_consumed', 'Local first parsed account WS observation to first business progress consumption'),
}
_HTTP_SPANS = {
    'rest_wrapper': ('start', 'end', 'SDK REST wrapper; not exchange processing time'),
    'pool_wrapper': ('pool_wrapper_start', 'pool_wrapper_end', 'Local session request wrapper'),
    'pool_queue': ('connection_queued_start', 'connection_queued_end', 'aiohttp connector pool queue'),
    'dns_resolution': ('dns_resolvehost_start', 'dns_resolvehost_end', 'aiohttp DNS resolution hook span'),
    'connection_establishment': ('connection_create_start', 'connection_create_end', 'aiohttp connection creation; may include DNS, TCP and TLS'),
    'request_to_headers_parsed': ('request_start', 'request_end', 'Local aiohttp request start to response headers parsed; not server time or first byte'),
    'local_write_callback_window': ('request_headers_callback', 'request_chunk_callback_last', 'Local write-intent callbacks run BEFORE transport write; not physical transmission'),
    'headers_to_body_read_done': ('request_end', 'response_body_read_done', 'Local response headers parsed to complete body read callback; not first byte'),
}
_HTTP_UNKNOWN = {
    'first_response_byte': 'Public aiohttp hooks do not expose first response byte',
    'physical_request_write': 'Public write callbacks precede transport write; no physical-write hook',
    'tcp_connect': 'No separate public TCP completion hook',
    'tls_handshake': 'No separate public TLS completion hook',
}
_REQUEST_MARKS = {stage for start, end, _ in _HTTP_SPANS.values() for stage in (start, end)} | {
    'request_chunk_callback_first', 'connection_reuse', 'dns_cache_hit', 'dns_cache_miss'}
_ORDER_MARKS = {stage for start, end, _ in _ORDER_SPANS.values() for stage in (start, end)} | {
    'prepare_start', 'prepare_start_last', 'prepare_end', 'prepare_end_last',
    'submission_lock_acquired', 'prior_submission_drained', 'send_taker_return',
    'terminal_published', 'runtime_consumed', 'runtime_consumed_first', 'runtime_progress_consumed', 'ws_raw_received_last', 'ws_parsed_last'}
_ORDER_MARKS.add('adapter_normalized')


def timing_span(stages, start, end, boundary):
    a = safe_stamp(stages.get(start))['mono_ns']
    b = safe_stamp(stages.get(end))['mono_ns']
    valid = a is not None and b is not None and b >= a
    return {'status': 'OBSERVED' if valid else 'UNKNOWN',
            'duration_ms': (b - a) / 1_000_000 if valid else None,
            'reason': None if valid else 'missing_boundary' if a is None or b is None else 'non_monotonic_boundaries',
            'boundary': boundary}


def account_ws_observed(event):
    return (event.get('order_observation_source') in {'account_orders_ws', 'order_fills_ws'}
            and event.get('ws_receive_boundary') == 'ws_recv')


def timing_segments(event):
    stages = event['stages']
    spans = {name: timing_span(stages, *spec) for name, spec in _ORDER_SPANS.items()}
    if not account_ws_observed(event):
        for name in ('submit_to_account_ws_receive', 'account_ws_parse', 'account_ws_publish'):
            spans[name].update(status='UNKNOWN', duration_ms=None, reason='account_ws_recv_not_observed')
    for row in event['http_requests']:
        row['timing_segments'] = {name: timing_span(row, *spec) for name, spec in _HTTP_SPANS.items()}
        row['timing_segments'].update({name: {'status': 'UNKNOWN', 'duration_ms': None,
            'reason': 'no_public_hook', 'boundary': description} for name, description in _HTTP_UNKNOWN.items()})
        ws = safe_stamp(stages.get('ws_raw_received_first'))['mono_ns']
        headers = safe_stamp(row.get('request_end'))['mono_ns']
        valid = account_ws_observed(event) and ws is not None and headers is not None
        row['account_ws_vs_http_headers'] = {
            'ordering': ('WS_BEFORE_HTTP_HEADERS' if ws < headers else 'HTTP_HEADERS_BEFORE_WS' if ws > headers else 'SAME_LOCAL_STAMP') if valid else 'UNKNOWN',
            'ws_minus_headers_ms': (ws - headers) / 1_000_000 if valid else None,
            'boundary': 'Signed local clock offset; not an exchange latency or an additive duration'}
    return {'clock': 'local_monotonic', 'overlap_not_additive': True, 'spans': spans}


def integer(value):
    if type(value) is bool:
        return None
    try:
        text = str(value)
        return int(text) if text.isdigit() and len(text) <= 20 else None
    except (TypeError, ValueError):
        return None


def timeout_metadata(value):
    try:
        if value is None:
            return None, 'none', True
        if type(value) in (int, float) and math.isfinite(value):
            return value, 'scalar', True
        if (type(value) in (tuple, list) and len(value) == 2
                and all(type(part) in (int, float) and math.isfinite(part) for part in value)):
            return list(value), 'pair', True
    except (OverflowError, ValueError, TypeError):
        pass
    return None, 'unknown', False


def source_name(value):
    if value is None:
        return None
    return value if type(value) is str and value in {
        'account_orders_ws', 'order_fills_ws', 'accountOrders_rest', 'exchange_http'} else 'unknown'


def diagnostic(fn):
    def checked(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except Exception:
            self.collector.dropped += 1
            return None
    return checked


class OrderTiming:
    def __init__(self, collector, order_ref, generation):
        self.collector = collector
        self.data = dict(kind=('v3_lighter_order_timing' if collector.venue == 'hedge' else 'v3_entropy_order_timing'), schema_version=1,
            venue=collector.venue, order_ref=str(order_ref), client_order_index=str(order_ref),
            clock_session_id=collector.clock_session_id, version=0, stages={},
            trace_generation=generation,
            http_requests=[], http_requests_seen=0, http_requests_dropped=0,
            lookups=[], lookups_seen=0, lookups_dropped=0,
            trades=[], trades_seen=0, trade_duplicates=0, trades_dropped=0,
            cumulative_filled_base=None, exchange_first_observed_fill_ts=None,
            exchange_last_observed_fill_ts=None, exchange_fill_ts_source=None,
            trade_coverage='observed_subset_not_complete', observed_trade_base_sum=0.0,
            trade_execution_time_missing=0)
        self._trade_ids = set()
        self._order_signature = None

    @diagnostic
    def annotate(self, **fields):
        # Explicit allowlist: never merge raw responses, errors, or SDK context.
        for key in ('cycle_id', 'step_key', 'attempt', 'purpose', 'side',
                    'requested_units', 'quantity_step', 'requested_base',
                    'limit_px', 'reduce_only', 'market_id'):
            value = fields.get(key)
            if type(value) in (str, int, float, bool) and (
                    not isinstance(value, float) or math.isfinite(value)):
                self.data[key] = value[:128] if isinstance(value, str) else value

    @diagnostic
    def mark(self, stage, when=None, *, first=False):
        if stage not in _ORDER_MARKS:
            return
        if first and stage in self.data['stages']:
            return
        self.data['stages'][stage] = safe_stamp(stamp() if when is None else when)

    @diagnostic
    def request(self, category, **fields):
        if category not in ('http_requests', 'lookups'):
            return
        self.data[category + '_seen'] += 1
        row = {'sequence': self.data[category + '_seen'], 'start': safe_stamp(stamp())}
        if category == 'http_requests':
            row['method'] = fields.get('method') if fields.get('method') in ('GET', 'POST') else 'other'
            row['endpoint'] = fields.get('endpoint') if fields.get('endpoint') in ('/api/v1/sendTx', '/api/v1/nextNonce', '/exchange') else 'other'
            if 'request_timeout' in fields:
                value, kind, known = timeout_metadata(fields['request_timeout'])
                row['request_timeout'] = value
                row['request_timeout_kind'] = fields.get('request_timeout_kind') if fields.get('request_timeout_kind') in ('none', 'scalar', 'pair', 'unknown') else kind
                row['request_timeout_known'] = fields.get('request_timeout_known') if type(fields.get('request_timeout_known')) is bool else known
        elif 'source' in fields:
            row['source'] = source_name(fields['source'])
        if len(self.data[category]) < 8:
            self.data[category].append(row)
        else:
            self.data[category + '_dropped'] += 1
        return row

    @diagnostic
    def request_mark(self, row, stage, when=None, *, first=False):
        if stage not in _REQUEST_MARKS or not isinstance(row, dict):
            return
        if first and stage in row:
            return
        row[stage] = safe_stamp(stamp() if when is None else when)

    @diagnostic
    def order(self, row, *, received, parsed, boundary, source):
        index = integer(row.get('order_index', row.get('order_id')))
        updated = integer(row.get('timestamp'))
        signature = (str(row.get('status', ''))[:128], str(row.get('filled_base_amount', ''))[:64],
                     str(row.get('filled_quote_amount', ''))[:64], index, updated)
        duplicate = signature == self._order_signature
        self._order_signature = signature
        try:
            quantity = float(row.get('filled_base_amount') or 0)
            quote = float(row.get('filled_quote_amount') or 0)
            self.result(status=row.get('status', ''), filled_base=quantity,
                        average=quote / quantity if quantity > 0 else None)
        except (ValueError, TypeError, OverflowError):
            self.data['order_quantity_observations_invalid'] = self.data.get('order_quantity_observations_invalid', 0) + 1
        self.observe_ws(received=received, parsed=parsed, boundary=boundary, source=source)
        self.data['order_updates_seen'] = self.data.get('order_updates_seen', 0) + 1
        self.data['order_duplicates'] = self.data.get('order_duplicates', 0) + int(duplicate)
        if not duplicate and 'runtime_consumed' in self.data['stages']:
            self.emit()

    @diagnostic
    def observe_ws(self, *, received, parsed, boundary, source):
        self.data['order_observation_source'] = source_name(source)
        self.data['ws_receive_boundary'] = boundary if boundary in {'ws_recv', 'handler_call'} else 'unknown'
        for stage, value in (('ws_raw_received', received), ('ws_parsed', parsed)):
            self.mark(stage + '_first', value, first=True)
            self.mark(stage + '_last', value)

    @diagnostic
    def identity(self, row, source):
        for field in ('order_index', 'order_id', 'order_index_str', 'order_id_str'):
            value = integer(row.get(field))
            if value is not None and value > 0:
                self.data['exchange_order_id'] = str(value)
                self.data['exchange_order_id_source'] = source + ':' + field
                break
        updated = integer(row.get('timestamp'))
        if updated is not None and updated > 0:
            self.data['exchange_order_update_block_s'] = updated
            self.data['exchange_order_update_ts_source'] = 'Order.timestamp:last_update_block_seconds'

    @diagnostic
    def trade(self, row, *, order_id, received, parsed, boundary):
        trade_id = integer(row.get('trade_id_str', row.get('trade_id')))
        if trade_id is None or trade_id <= 0:
            self.data['trades_dropped'] += 1
            return
        if trade_id in self._trade_ids:
            self.data['trade_duplicates'] += 1
            return
        self.data['trades_seen'] += 1
        if len(self._trade_ids) >= 32:
            self.data['trades_dropped'] += 1
            return
        try:
            quantity, price = float(row['size']), float(row['price'])
            if not math.isfinite(quantity) or not math.isfinite(price) or min(quantity, price) <= 0:
                raise ValueError('invalid trade observation')
        except (KeyError, TypeError, ValueError, OverflowError):
            self.data['trades_dropped'] += 1
            return
        self._trade_ids.add(trade_id)
        block_ms, execution_us = integer(row.get('timestamp')), integer(row.get('transaction_time'))
        block_ms = block_ms if block_ms is not None and block_ms > 0 else None
        execution_us = execution_us if execution_us is not None and execution_us > 0 else None
        trade = dict(trade_id=str(trade_id), filled_base=quantity, price=price,
            exchange_block_ms=block_ms, exchange_execution_us=execution_us,
            source='account_all_ws:Trade', received=dict(received), parsed=dict(parsed),
            receive_boundary=boundary)
        actual_order_id = integer(order_id)
        if actual_order_id is not None:
            trade['exchange_order_id'] = str(actual_order_id)
        self.data['trades'].append(trade)
        total = self.data['observed_trade_base_sum'] + quantity
        if math.isfinite(total):
            self.data['observed_trade_base_sum'] = total
        else:
            self.data['trade_quantity_aggregate_invalid'] = True
        self.data['trade_execution_time_missing'] += int(execution_us is None)
        if execution_us is not None:
            old_first = self.data.get('exchange_first_execution_us', execution_us)
            old_last = self.data.get('exchange_last_execution_us', execution_us)
            self.data['exchange_first_execution_us'] = min(old_first, execution_us)
            self.data['exchange_last_execution_us'] = max(old_last, execution_us)
            self.data['exchange_first_observed_fill_ts'] = self.data['exchange_first_execution_us'] / 1_000_000
            self.data['exchange_last_observed_fill_ts'] = self.data['exchange_last_execution_us'] / 1_000_000
            self.data['exchange_fill_ts_source'] = 'Trade.transaction_time:execution_microseconds'

    @diagnostic
    def result(self, *, status, filled_base, average, source=None, timeout_reason=None):
        self.data['result_status'] = str(status)[:128]
        quantity_current = False
        if type(filled_base) in (int, float) and math.isfinite(filled_base):
            previous = self.data['cumulative_filled_base'] or 0
            self.data['cumulative_filled_base'] = max(previous, filled_base)
            quantity_current = filled_base >= previous
            if not quantity_current:
                self.data['out_of_order_quantity_observations'] = self.data.get('out_of_order_quantity_observations', 0) + 1
        if quantity_current and type(average) in (int, float) and math.isfinite(average):
            self.data['average_px'] = average
        if source is not None:
            self.data['normalized_result_confirmation_source'] = source_name(source)
        if timeout_reason is not None:
            self.data['timeout_reason'] = timeout_reason

    @diagnostic
    def consumed(self, when=None):
        self.mark('runtime_consumed_first', when, first=True)
        self.mark('runtime_consumed', when)
        self.data['runtime_consumptions'] = self.data.get('runtime_consumptions', 0) + 1
        self.emit()

    @diagnostic
    def emit(self):
        self.data['version'] += 1
        event = deepcopy(self.data)
        event['timing_segments_schema_version'] = 1
        event['timing_segments'] = timing_segments(event)
        event['collector_evicted_total'] = self.collector.evicted
        event['collector_publish_dropped_total'] = self.collector.dropped
        event['ws_trade_rows_dropped_total'] = self.collector.trade_rows_dropped
        event['correlation_missing'] = [key for key in ('cycle_id', 'step_key', 'attempt') if key not in event]
        event['missing_stages'] = [key for key in ('prepare_start', 'submit_wrapper_start',
            'ws_raw_received_first', 'terminal_published_first', 'runtime_consumed')
            if key not in event['stages']]
        self.collector.sink(event)


class OrderTimingCollector:
    def __init__(self, sink, clock_session_id, *, capacity=512, venue='hedge'):
        self.sink, self.clock_session_id, self.capacity = sink, clock_session_id, capacity
        self.venue = venue
        self.orders = OrderedDict()
        self.evicted = self.dropped = self.trade_rows_dropped = 0
        self.generation = 0

    def get(self, order_ref, *, create=False):
        key = str(order_ref)
        trace = self.orders.get(key)
        if trace is None and create:
            self.generation += 1
            trace = self.orders[key] = OrderTiming(self, key, self.generation)
            while len(self.orders) > self.capacity:
                self.orders.popitem(last=False)
                self.evicted += 1
        return trace
