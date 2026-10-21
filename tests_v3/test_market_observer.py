import asyncio
from decimal import Decimal as D
import json
import os
from types import SimpleNamespace
import time

from entropy_arb.book import OrderBook
from entropy_arb.v3.core import Instrument, Settings
from entropy_arb.v3.market_observer import BufferedJSONLObserver, MarketSampler
from entropy_arb.v3.runtime import RuntimeVenues


def test_observer_records_depth_and_both_executable_directions():
    def venue(name, bid, ask):
        book = OrderBook()
        book.apply_hl([[{'px': str(bid), 'sz': '1'}],
                       [{'px': str(ask), 'sz': '1'}]])
        return SimpleNamespace(key=name, book=book, min_base=.01, min_quote=1)

    venues = RuntimeVenues(venue('entropy', 99, 101), venue('hedge', 98, 100))
    instrument = Instrument('pair', D('.01'), D('.01'), D('.01'), D('0'), D('0'))
    sampler = MarketSampler('pair', Settings(), venues, instrument, 'hash')
    first = sampler.sample(mono=1.0)
    assert first['sample_seq'] == 1
    assert first['entropy']['exchange_ts'] is None
    assert first['entropy']['received_wall_ts'] == venues.entropy.book.last_update_ts
    assert first['entropy']['received_mono_ns'] == venues.entropy.book.last_update_mono_ns
    assert first['entropy']['connection_generation'] == 0
    assert first['entropy']['bids'] == [['99.0', 100]]
    assert first['quotes'][0]['buy_entropy_basis_bps'] is not None
    assert first['quotes'][0]['sell_entropy_basis_bps'] is not None
    assert sampler.sample(mono=1.05) is None
    assert sampler.sample(mono=2.05)['sample_seq'] == 2


def test_book_receive_metadata_distinguishes_update_from_reconnect():
    book = OrderBook()
    book.apply_hl([[{'px': '99', 'sz': '1'}],
                   [{'px': '101', 'sz': '1'}]])
    received = book.last_update_ts
    received_mono = book.last_update_mono_ns
    book.touch()
    assert book.last_update_ts == received
    assert book.last_update_mono_ns == received_mono
    book.clear()
    assert book.connection_generation == 1
    assert not book.ready


def _rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_buffered_writer_batches_and_flushes_on_close(tmp_path):
    async def scenario():
        path = tmp_path / 'observer.jsonl'
        writer = BufferedJSONLObserver(
            path, {'kind': 'v3_observer_header', 'session_id': 'session'},
            queue_size=8, batch_size=3, flush_seconds=60)
        batches = []
        original = writer._write_batch

        def capture(batch):
            batches.append(len(batch))
            original(batch)

        writer._write_batch = capture
        writer.start()
        for sequence in range(3):
            writer.publish({'kind': 'sample', 'sequence': sequence})
        await writer.close()
        assert batches == [3]
        rows = _rows(path)
        assert rows[0]['session_id'] == 'session'
        assert [row['sequence'] for row in rows[1:]] == [0, 1, 2]
        assert writer.dropped_events == 0

    asyncio.run(scenario())


def test_buffered_writer_flushes_sparse_events(tmp_path):
    async def scenario():
        path = tmp_path / 'observer.jsonl'
        writer = BufferedJSONLObserver(
            path, {'kind': 'v3_observer_header'},
            batch_size=8, flush_seconds=0.01)
        writer.start()
        writer.publish({'sequence': 1})
        for _ in range(100):
            if path.exists() and len(_rows(path)) > 1:
                break
            await asyncio.sleep(0.01)
        assert _rows(path)[1]['sequence'] == 1
        await writer.close()

    asyncio.run(scenario())


def test_buffered_writer_queue_is_bounded_and_non_blocking(tmp_path):
    async def scenario():
        writer = BufferedJSONLObserver(
            tmp_path / 'observer.jsonl', {'kind': 'v3_observer_header'},
            queue_size=2, batch_size=2, flush_seconds=60)
        writer.start()
        for sequence in range(3):
            assert writer.publish({'sequence': sequence}) is None
        assert writer.dropped_events == 1
        await writer.close()
        rows = _rows(writer.path)
        assert [row['sequence'] for row in rows[1:] if 'sequence' in row] == [0, 1]
        assert rows[-1]['kind'] == 'v3_observer_gap'
        assert rows[-1]['observer_dropped_total'] == 1
        assert rows[1]['observer_dropped_total'] == 0

    asyncio.run(scenario())


def test_buffered_writer_marks_new_session_when_appending(tmp_path):
    async def scenario():
        path = tmp_path / 'observer.jsonl'
        path.write_text('{"kind":"old_session"}\n')
        writer = BufferedJSONLObserver(
            path, {'kind': 'v3_observer_header', 'session_id': 'new_session'},
            batch_size=1)
        writer.start()
        writer.publish({'kind': 'sample', 'session_id': 'new_session'})
        await writer.close()
        assert [row['kind'] for row in _rows(path)] == [
            'old_session', 'v3_observer_header', 'sample']

    asyncio.run(scenario())


def test_buffered_writer_rotates_by_size_and_time(tmp_path):
    async def scenario():
        path = tmp_path / 'observer.jsonl'
        header = {'kind': 'v3_observer_header', 'session_id': 'session'}
        writer = BufferedJSONLObserver(
            path, header, batch_size=1, max_file_bytes=250,
            max_total_bytes=20000)
        writer.start()
        for sequence in range(4):
            writer.publish({'sequence': sequence, 'payload': 'x' * 80})
        await writer.close()
        archives = sorted(tmp_path.glob('observer.*.jsonl'))
        assert archives
        files = [*archives, path]
        assert all(_rows(item)[0]['session_id'] == 'session' for item in files)
        assert sorted(row['sequence'] for item in files for row in _rows(item)[1:]) == [0, 1, 2, 3]

        old = time.time() - 10
        os.utime(path, (old, old))
        next_writer = BufferedJSONLObserver(
            path, header, batch_size=1, rotate_seconds=1,
            max_file_bytes=2000, max_total_bytes=20000)
        next_writer.start()
        next_writer.publish({'sequence': 4})
        await next_writer.close()
        assert _rows(path)[1]['sequence'] == 4
        assert len(list(tmp_path.glob('observer.*.jsonl'))) > len(archives)

    asyncio.run(scenario())


def test_buffered_writer_prunes_only_own_archives(tmp_path):
    async def scenario():
        old = tmp_path / 'observer.20200101T000000Z-1.jsonl'
        recent = tmp_path / 'observer.20260101T000000Z-2.jsonl'
        other = tmp_path / 'journal.sqlite3'
        old.write_bytes(b'x' * 100)
        recent.write_bytes(b'y' * 350)
        other.write_bytes(b'z' * 350)
        now = time.time()
        os.utime(old, (now - 1000, now - 1000))
        writer = BufferedJSONLObserver(
            tmp_path / 'observer.jsonl', {'kind': 'v3_observer_header'},
            retain_seconds=100, max_file_bytes=100, max_total_bytes=200)
        writer.start()
        await writer.close()
        assert not old.exists()
        assert not recent.exists()
        assert other.exists()

    asyncio.run(scenario())


def test_buffered_writer_write_failure_does_not_raise(tmp_path):
    async def scenario():
        path = tmp_path / 'observer.jsonl'
        path.mkdir()
        writer = BufferedJSONLObserver(path, {'kind': 'v3_observer_header'},
                                       batch_size=1)
        writer.start()
        writer.publish({'sequence': 1})
        await writer.close()
        assert writer.dropped_events == 1
        assert 'IsADirectoryError' in writer.last_error

    asyncio.run(scenario())


def test_record_only_snapshot_uses_reference_before_observe(tmp_path, monkeypatch):
    import main_v3
    from entropy_arb.v3 import market_observer, runtime
    from entropy_arb.v3.live_host import StatusWriter

    class FakeReference:
        def __init__(self, *args, **kwargs):
            self.observed = False

        def view(self, now):
            return SimpleNamespace(
                usable=True, center_bps=D('99') if self.observed else D('10'),
                source='test_reference', live_samples=5, live_span_seconds=4.0)

        def observe(self, now, basis, *, valid):
            self.observed = True
            return self.view(now)

        def checkpoint(self):
            return {}

    class FakeVenue:
        def __init__(self):
            self.book = SimpleNamespace(mid=lambda: 100)

        async def load_market(self):
            pass

        def start_tasks(self, stop, changed, *, live):
            changed()
            return []

        async def close(self):
            pass

    class FakeSampler:
        def __init__(self, *args):
            self.session_id = args[-1]
            self.market_valid = True

        def sample(self, *, now):
            return {'kind': 'v3_market_snapshot', 'valid': True}

    monkeypatch.setattr(main_v3, 'FastReference', FakeReference)
    monkeypatch.setattr(main_v3, '_make_venues', lambda cfg, session: (
        FakeVenue(), FakeVenue()))
    monkeypatch.setattr(runtime, 'instrument_from_venues', lambda *args: None)
    monkeypatch.setattr(market_observer, 'MarketSampler', FakeSampler)
    args = SimpleNamespace(
        observer_jsonl=tmp_path / 'observer.jsonl',
        reference_checkpoint=tmp_path / 'checkpoint.json',
        observer_rotate_hours=1, observer_rotate_mib=64,
        observer_retain_hours=72, observer_max_total_mib=1024,
        deployment='observer-test', max_new_cycles=0, legacy_reference_sampling=False,
        validate_only=True)
    status = StatusWriter(tmp_path / 'status.json', strategy_version='v3',
                          config_hash='hash', mode='record-only')

    asyncio.run(main_v3.run_record_only(args, None, Settings(), 'pair', status))

    rows = _rows(args.observer_jsonl)
    snapshot = next(row for row in rows if row['kind'] == 'v3_market_snapshot')
    later = next(row for row in rows if row['kind'] == 'v3_reference')
    assert snapshot['reference_bps'] == '10'
    assert snapshot['reference_usable'] is True
    assert snapshot['reference_source'] == 'test_reference'
    assert snapshot['reference_live_samples'] == 5
    assert later['reference_bps'] == '99'
