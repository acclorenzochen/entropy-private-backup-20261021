import asyncio
from contextlib import closing
from decimal import Decimal as D
import hashlib
import json
import threading
from types import SimpleNamespace

import pytest

import main_v3
from entropy_arb.v3.core import Instrument, Settings, Side
from entropy_arb.v3.journal import Intent, Journal
from entropy_arb.v3.live_host import (
    AdapterContractError, AuditedLiveExecutor, BatchAuthorization,
    StatusWriter, VenueAdapterBroker,
)


def broker_fixture():
    intent = Intent('0x' + '1' * 32, 'cycle', 'entropy', Side.BUY, 85, D('100'))
    raw = dict(order_ref=intent.order_ref, status='filled', filled_base='.040',
               avg_px='100', unresolved=False, confirmation_source='exchange_http')

    class Venue:
        sends = 0
        resolves = []

        async def send_taker(self, **kwargs):
            self.sends += 1
            return raw.copy()

        async def resolve_order(self, order_ref, timeout=None):
            self.resolves.append(order_ref)
            return raw.copy()

    broker = VenueAdapterBroker(
        Venue(), venue_key='entropy', venue_kind='hl',
        instrument=Instrument('pair', D('.001'), D('.01'), D('.01'), D('2'), D('2')))
    return intent, raw, broker


@pytest.mark.parametrize('quantity', [None, '', '-.001', '.0005', '.086',
                                      'NaN', 'sNaN', 'Infinity', '-Infinity', True])
def test_bad_receipt_quantity_stays_unknown_same_id(tmp_path, quantity):
    intent, raw, broker = broker_fixture()
    raw['filled_base'] = quantity
    with closing(Journal(tmp_path / 'journal.sqlite', identity='test')) as journal:
        executor = AuditedLiveExecutor(journal, {'entropy': broker, 'hedge': broker})

        async def exercise():
            result = await executor.send(intent)
            assert not result.terminal and result.receipt is None
            assert journal.order(intent.order_ref)['state'] == 'UNKNOWN'
            raw['filled_base'] = '.040'
            result = await executor.resolve(intent)
            assert result.terminal and result.cumulative_units == 40
            assert broker.venue.sends == 1
            assert broker.venue.resolves == [intent.order_ref]

        asyncio.run(exercise())


def test_missing_quantity_and_identity_cannot_create_receipt():
    intent, raw, broker = broker_fixture()
    del raw['filled_base']
    with pytest.raises(AdapterContractError):
        broker._result(intent, raw)
    raw['filled_base'] = '.040'
    del raw['order_ref']
    with pytest.raises(AdapterContractError):
        broker._result(intent, raw)
    with pytest.raises(AdapterContractError):
        broker._result(intent, [])


@pytest.mark.parametrize('field,value', [('avg_px', 'NaN'), ('avg_px', '-1'),
                                       ('fee_usd', 'Infinity')])
def test_bad_numeric_metadata_is_contract_unknown(field, value):
    intent, raw, broker = broker_fixture()
    raw[field] = value
    with pytest.raises(AdapterContractError):
        broker._result(intent, raw)


def test_trusted_zero_requires_explicit_zero_terminal_outcome():
    intent, raw, broker = broker_fixture()
    raw['filled_base'] = '0'
    assert not broker._result(intent, raw).terminal
    raw['status'] = 'iocCancelRejected'
    result = broker._result(intent, raw)
    assert result.terminal and result.cumulative_units == 0 and result.receipt is None


def test_terminal_received_clocks_only_on_validated_terminal(monkeypatch):
    from entropy_arb.v3 import live_host

    intent, raw, broker = broker_fixture()
    monkeypatch.setattr(live_host.time, 'time', lambda: 123.5)
    monkeypatch.setattr(live_host.time, 'monotonic_ns', lambda: 456789)
    timing = {}
    assert broker._result(intent, raw, timing).terminal
    assert timing['terminal_result_ts'] == 123.5
    assert timing['terminal_result_mono_ns'] == 456789
    assert 'runtime_consumed_ts' not in timing
    assert 'clock_session_id' not in timing
    raw.update(status='timeout', unresolved=True, filled_base='0')
    timing = {}
    assert not broker._result(intent, raw, timing).terminal
    assert 'terminal_result_ts' not in timing
    assert 'terminal_result_mono_ns' not in timing
    raw.update(status='filled', unresolved=False, filled_base='.040', avg_px='NaN')
    timing = {}
    with pytest.raises(AdapterContractError):
        broker._result(intent, raw, timing)
    assert 'terminal_result_ts' not in timing
    assert 'terminal_result_mono_ns' not in timing


def writer_at(path):
    return StatusWriter(path, strategy_version='test', config_hash='test', mode='test',
                        background=True)


def test_slow_status_disk_does_not_block_and_coalesces_immutable_snapshot(tmp_path):
    async def exercise():
        writer = writer_at(tmp_path / 'status.json')
        entered, release = threading.Event(), threading.Event()
        owner = threading.get_ident()
        original = writer._write_file
        writes = []

        def slow(payload):
            assert threading.get_ident() != owner
            entered.set()
            assert release.wait(3)
            writes.append(json.loads(payload))
            original(payload)

        writer._write_file = slow
        try:
            writer.write('FIRST', 'test')
            assert await asyncio.to_thread(entered.wait, 1)
            extra = {'units': 85}
            for i in range(30):
                writer.write('LATEST', str(i), position=extra)
            extra['units'] = 999
            # The event loop can run a handoff while disk is still blocked.
            handoff = asyncio.Event()
            asyncio.get_running_loop().call_soon(handoff.set)
            await asyncio.wait_for(handoff.wait(), .5)
            assert writer.dropped_events == 29
            assert writer.dropped == 29
            snapshot = writer.current_snapshot
            assert snapshot['reason'] == '29'
            snapshot['position']['units'] = -1
            assert writer.current_snapshot['position']['units'] == 85
        finally:
            release.set()
            assert await writer.close()
        assert len(writes) == 2
        payload = json.loads(writer.path.read_text())
        assert payload['reason'] == '29'
        assert payload['position']['units'] == 85
        assert payload['status_writer']['dropped_events'] == 29

    asyncio.run(exercise())


def test_status_failure_is_observable_and_next_write_recovers(tmp_path):
    async def exercise():
        writer = writer_at(tmp_path / 'status.json')
        original = writer._write_file

        def fail(payload):
            raise OSError('disk unavailable')

        writer._write_file = fail
        writer.write('FIRST', 'test')
        assert await asyncio.to_thread(writer.flush)
        assert writer.write_errors == writer.dropped_events == 1
        assert writer.errors == writer.dropped == 1
        assert writer.last_error == 'OSError'
        writer._write_file = original
        writer.write('RECOVERED', 'test')
        assert await writer.close()
        payload = json.loads(writer.path.read_text())
        assert payload['current_state'] == 'RECOVERED'
        assert payload['status_writer']['write_errors'] == 1
        assert writer.last_error is None

    asyncio.run(exercise())


def test_status_default_is_synchronous_inside_async_legacy_caller(tmp_path):
    async def exercise():
        writer = StatusWriter(tmp_path / 'status.json', strategy_version='test',
                              config_hash='test', mode='test')
        writer.write('READY', 'legacy')
        assert json.loads(writer.path.read_text())['current_state'] == 'READY'
        assert await writer.close()

    asyncio.run(exercise())


def test_status_close_timeout_is_bounded_and_latest_snapshot_drains(tmp_path):
    async def exercise():
        writer = writer_at(tmp_path / 'status.json')
        entered, release = threading.Event(), threading.Event()
        original = writer._write_file

        def blocked(payload):
            entered.set()
            assert release.wait(3)
            original(payload)

        writer._write_file = blocked
        try:
            writer.write('FIRST', 'test')
            assert await asyncio.to_thread(entered.wait, 1)
            writer.write('FINAL', 'test')
            assert not await writer.close(timeout=.01)
        finally:
            release.set()
            assert await writer.close()
        assert json.loads(writer.path.read_text())['current_state'] == 'FINAL'
        writer.write('TOO_LATE', 'test')
        assert writer.dropped_events == 1

    asyncio.run(exercise())


@pytest.mark.parametrize('opening_hedge_override', [None, 0, 6])
def test_effective_manifest_uses_parsed_bytes_and_resolved_policy(tmp_path, opening_hedge_override):
    source = tmp_path / 'settings.json'
    raw = dict(open_first_rooms_bps=['3', '3', '3'], first_hard_cap_bps='9')
    if opening_hedge_override is not None:
        raw.update(open_hedge_rooms_bps=[str(opening_hedge_override)] * 3,
                   open_hedge_hard_cap_bps=str(opening_hedge_override))
    content = json.dumps(raw).encode()
    source.write_bytes(content)
    provenance = {}
    settings = main_v3.load_settings(source, provenance=provenance)
    source.write_text('{}')  # Later disk edits must not rewrite parsed provenance.
    auth_path = tmp_path / 'auth.json'
    auth_path.write_text(json.dumps(dict(
        batch_id='test', pair_identity='pair', max_position_cycles=2,
        loss_budget_usd='1', max_notional_usd='12', approved_live=True)))
    auth_source = {}
    authorization = BatchAuthorization.load(
        auth_path, expected_pair='pair', configured_max_notional=D('15'),
        provenance=auth_source)
    args = SimpleNamespace(deployment='test', max_new_cycles=1,
                           legacy_reference_sampling=False)
    status = writer_at(tmp_path / 'status.json')
    journal = Journal(tmp_path / 'journal.sqlite', identity='test')
    try:
        asyncio.run(main_v3.publish_effective_manifest(
            args, settings, 'pair', authorization, status, provenance, auth_source,
            journal=journal))
    finally:
        journal.close()
    payload = json.loads((tmp_path / 'status.effective_manifest.json').read_text())
    assert payload['v3_config_source']['sha256'] == hashlib.sha256(content).hexdigest()
    assert payload['v3_config_source']['path'] == str(source.resolve())
    policy = payload['execution_policies']
    assert policy['open']['rooms_bps'] == ['3', '3', '3']
    assert policy['close']['rooms_bps'] == ['3', '5', '8']
    assert policy['close']['sources']['rooms_bps']['origin'] == 'Settings.default'
    assert policy['open']['sources']['hard_cap_bps']['field'] == 'first_hard_cap_bps'
    for suffix in ('rooms_bps', 'hard_cap_bps'):
        expected = ('hedge_' if opening_hedge_override is None else 'open_hedge_') + suffix
        assert policy['hedge']['open']['sources'][suffix]['field'] == expected
        assert policy['hedge']['open']['sources'][suffix]['origin'] == (
            'Settings.default' if opening_hedge_override is None else 'v3_config')
        assert policy['hedge']['close']['sources'][suffix]['field'] == 'hedge_' + suffix
    if opening_hedge_override is not None:
        assert policy['hedge']['open']['rooms_bps'] == [str(opening_hedge_override)] * 3
        assert policy['hedge']['open']['hard_cap_bps'] == str(opening_hedge_override)
    assert policy['hedge']['close']['rooms_bps'] == ['4', '8', '12']
    assert payload['authorization']['max_notional_usd'] == '12'
    assert payload['settings']['max_notional_usd'] == '12'
    assert payload['configured_settings']['max_notional_usd'] == '15'
    assert payload['authorization_source']['evidence'] == 'parsed_bytes'
    assert payload['settings']['exit_fast_path_enabled'] is False
    assert payload['journal_schema']['economics_schema_version']
    assert payload['loaded_modules']['entropy_arb.v3.live_host']['disk_sha256']
    assert 'not_loaded_bytecode_attestation' in payload['source_hash_evidence']
    assert Settings().exit_fast_path_enabled is False


def test_validate_only_entrypoint_flushes_status_and_manifest(tmp_path, monkeypatch):
    source = tmp_path / 'settings.json'
    source.write_text('{}')
    args = main_v3.parser().parse_args([
        '--symbol', 'TEST', '--hedge', 'lighter', '--validate-only',
        '--v3-config', str(source), '--status-json', str(tmp_path / 'status.json')])
    cfg = SimpleNamespace(entropy=SimpleNamespace(kind='hl', symbol='TEST'),
                          hedge=SimpleNamespace(kind='lighter', symbol='TEST'))
    # No connection settings or credentials are needed by this isolated probe.
    monkeypatch.setattr(main_v3, 'load_config', lambda *a, **kw: cfg)
    assert asyncio.run(main_v3.amain(args)) == 0
    status = json.loads((tmp_path / 'status.json').read_text())
    assert status['current_state'] == 'VALIDATED'
    manifest = json.loads((tmp_path / 'status.effective_manifest.json').read_text())
    assert manifest['authorization_source'] is None
    assert manifest['journal_schema'] is None
    assert manifest['settings']['exit_fast_path_enabled'] is False


@pytest.mark.parametrize('disk_failure', [False, True])
def test_record_only_invalid_and_throttled_samples_and_checkpoint_worker(
        tmp_path, monkeypatch, disk_failure):
    import aiohttp
    from entropy_arb.v3 import market_observer, runtime

    frames = [(True, {'valid': True}), (False, {'valid': False}),
              (False, None), (True, None)]
    state = {'index': 0, 'valid': True}
    published, observations, checkpoint_threads = [], [], []
    owner = threading.get_ident()

    class Session:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class Venue:
        def __init__(self):
            self.book = SimpleNamespace(mid=self.mid)

        def mid(self):
            assert state['valid'], 'invalid or throttled-invalid book was consumed'
            return 100

        async def load_market(self):
            pass

        def start_tasks(self, stop, changed, live):
            assert live is False
            state.update(stop=stop, changed=changed)
            changed()
            return []

        async def close(self):
            pass

    class Sampler:
        def __init__(self, *args):
            self.market_valid = False

        def sample(self, **kwargs):
            self.market_valid, snapshot = frames[state['index']]
            state['valid'] = self.market_valid
            state['index'] += 1
            if state['index'] == len(frames):
                state['stop'].set()
            else:
                state['changed']()
            return snapshot

    class Observer:
        dropped_events = 0
        last_error = None

        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            pass

        def publish(self, event):
            published.append(dict(event))

        async def close(self):
            state['observer_closed'] = True

    observe = main_v3.FastReference.observe

    def track_reference(self, *args, **kwargs):
        observations.append(kwargs['valid'])
        return observe(self, *args, **kwargs)

    def checkpoint(path, value):
        checkpoint_threads.append(threading.get_ident())
        assert threading.get_ident() != owner
        if disk_failure:
            raise OSError('checkpoint disk failure')
        from pathlib import Path
        Path(path).write_text(json.dumps(value))

    monkeypatch.setattr(aiohttp, 'TCPConnector', lambda **kwargs: None)
    monkeypatch.setattr(aiohttp, 'ClientSession', Session)
    monkeypatch.setattr(main_v3, '_make_venues', lambda *args: (Venue(), Venue()))
    monkeypatch.setattr(runtime, 'instrument_from_venues', lambda *args: None)
    monkeypatch.setattr(market_observer, 'MarketSampler', Sampler)
    monkeypatch.setattr(market_observer, 'BufferedJSONLObserver', Observer)
    monkeypatch.setattr(main_v3.FastReference, 'observe', track_reference)
    monkeypatch.setattr(main_v3, 'save_checkpoint', checkpoint)
    args = main_v3.parser().parse_args([
        '--symbol', 'TEST', '--hedge', 'lighter',
        '--reference-checkpoint', str(tmp_path / 'checkpoint.json')])
    status = writer_at(tmp_path / 'status.json')

    async def exercise():
        # Keep this probe from installing process signal handlers.
        monkeypatch.setattr(asyncio.get_running_loop(), 'add_signal_handler',
                            lambda *args: None)
        try:
            await main_v3.run_record_only(args, None, Settings(), 'pair', status)
        finally:
            assert await status.close()

    asyncio.run(exercise())
    # Main reference and both shadow references consume each market validity.
    assert observations == [True] * 3 + [False] * 6 + [True] * 3
    invalid = next(event for event in published if event.get('valid') is False)
    assert invalid['reference_usable'] is False
    assert invalid['reference_bps'] is None
    assert invalid['reference_source'] == 'invalid_market'
    assert checkpoint_threads and state['observer_closed']
    result = json.loads(status.path.read_text())
    assert result['checkpoint_writer_errors'] == (len(checkpoint_threads) if disk_failure else 0)
    assert bool(result['checkpoint_writer_last_error']) is disk_failure
