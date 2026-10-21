import asyncio
from decimal import Decimal as D

import pytest

from entropy_arb.venue_hl import HLVenue
from entropy_arb.v3.core import Instrument, Side
from entropy_arb.v3.journal import Intent
from entropy_arb.v3.live_host import VenueAdapterBroker
from tests_v3.test_runtime import setup_runtime


def test_hl_broker_passes_open_first_fast_resolve_budget_and_timing():
    class Venue:
        supports_open_first_fast_resolve = True

        async def send_taker(self, **kwargs):
            assert kwargs['resolve_timeout'] == 1.0
            kwargs['timing'].update({
                'http_post_start': 10.0,
                'http_result_ts': 10.2,
                'resolve_start_ts': 10.2,
                'resolve_end_ts': 11.0,
            })
            return {
                'status': 'timeout', 'filled_base': 0, 'avg_px': None,
                'unresolved': True, 'order_ref': kwargs['order_ref'],
            }

    instrument = Instrument(
        'pair', D('0.001'), D('0.01'), D('0.01'), D('2'), D('2'))
    broker = VenueAdapterBroker(
        Venue(), venue_key='entropy', venue_kind='hl', instrument=instrument)
    intent = Intent('0x' + '1' * 32, 'cycle', 'entropy', Side.BUY, 85, D('100'))
    timing = {}
    result = asyncio.run(broker.submit(
        intent, resolve_timeout=1.0, timing=timing))
    assert not result.terminal
    assert timing['_fast_resolve_performed'] is True
    assert timing['resolve_end_ts'] - timing['resolve_start_ts'] == pytest.approx(.8)
    assert timing['result_status'] == 'timeout'


def test_open_first_unknown_stops_campaign_and_records_one_attempt(tmp_path):
    runtime, entropy, _hedge, journal = setup_runtime(tmp_path)

    async def unknown_send(**kwargs):
        entropy.sent.append(kwargs)
        return {
            'status': 'timeout', 'filled_base': 0, 'avg_px': None,
            'err': None, 'unresolved': True,
            'order_ref': kwargs['order_ref'],
        }

    async def unknown_resolve(order_ref, timeout=None):
        assert timeout == 1.0
        return {
            'status': 'timeout', 'filled_base': 0, 'avg_px': None,
            'err': None, 'unresolved': True, 'order_ref': order_ref,
        }

    entropy.send_taker = unknown_send
    entropy.resolve_order = unknown_resolve

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))

    asyncio.run(scenario())
    assert len(entropy.sent) == 1
    steps = journal.economics_steps(journal.active_live_cycle()['cycle_id'])
    assert len([step for step in steps if step['purpose'] == 'open_first']) == 1
    assert steps[0]['order_ref'] == entropy.sent[0]['order_ref']
    assert steps[0]['result_status'] == 'timeout'
    assert steps[0]['resolve_start_ts'] is not None
    assert steps[0]['resolve_end_ts'] is not None
    journal.close()


def test_post_exchange_records_network_phases():
    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def text(self):
            return '{"status":"ok"}'

    class Session:
        def post(self, *_args, **_kwargs):
            return Response()

    venue = HLVenue.__new__(HLVenue)
    venue.session = Session()
    venue.api_url = 'https://example.invalid'
    venue._exchange_timings = {}
    body, err, unresolved = asyncio.run(
        venue._post_exchange({'nonce': 7}))
    timing = venue._exchange_timings[7]
    assert body == {'status': 'ok'} and err is None and not unresolved
    assert timing['http_post_start'] <= timing['http_headers_received']
    assert timing['http_headers_received'] <= timing['http_body_received']
    assert timing['http_result_ts'] == timing['http_body_received']


def test_open_first_attempt_row_contains_terminal_timing(tmp_path):
    runtime, entropy, _hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))

    asyncio.run(scenario())
    cycle_id = journal.db.execute(
        'SELECT cycle_id FROM cycles ORDER BY rowid DESC LIMIT 1').fetchone()[0]
    row = next(step for step in journal.economics_steps(cycle_id)
               if step['purpose'] == 'open_first')
    assert row['order_ref'] == entropy.sent[0]['order_ref']
    assert row['submit_start_ts'] is not None
    assert row['terminal_result_ts'] is not None
    assert row['result_status'] in {'filled', 'partial'}
    assert row['result_units'] > 0
    journal.close()
