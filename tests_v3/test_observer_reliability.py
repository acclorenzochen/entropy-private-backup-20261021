import asyncio
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import threading
from types import SimpleNamespace

from entropy_arb.book import OrderBook
from entropy_arb.v3.core import Direction, Instrument, Settings
from entropy_arb.v3.market_observer import (
    BufferedJSONLObserver, MarketSampler, ObserverFileWriter, quote_snapshot,
    source_identity)
from entropy_arb.v3.runtime import RuntimeVenues


def sampler():
    def venue(name):
        book = OrderBook()
        book.apply_hl([[{'px': '99', 'sz': '1'}], [{'px': '101', 'sz': '1'}]])
        return SimpleNamespace(key=name, book=book, min_base=.01, min_quote=1)
    return MarketSampler('pair', Settings(), RuntimeVenues(venue('entropy'), venue('hedge')),
                         Instrument('pair', D('.01'), D('.01'), D('.01'), D(0), D(0)), 'cfg')


def test_short_invalid_and_reconnect_are_ordered_even_inside_throttle():
    subject = sampler()
    first = subject.sample(mono=1)
    subject.venues.entropy.book.clear()
    invalid = subject.sample(mono=1.01)
    assert not invalid['valid']
    assert invalid['entropy']['connection_generation'] == 1
    subject.venues.entropy.book.apply_hl(
        [[{'px': '99', 'sz': '1'}], [{'px': '101', 'sz': '1'}]])
    recovered = subject.sample(mono=1.02)
    assert recovered['valid']
    assert [row['sample_seq'] for row in (first, invalid, recovered)] == [1, 2, 3]
    assert invalid['continuity_break'] and recovered['continuity_break']
    assert invalid['config_hash'] == 'cfg'
    assert invalid['source_version'].startswith('sha256:')


def test_identical_book_frames_are_heartbeats_not_changes():
    subject = sampler()
    subject.sample(mono=1)
    subject.venues.entropy.book.apply_hl(
        [[{'px': '99', 'sz': '1'}], [{'px': '101', 'sz': '1'}]])
    assert subject.sample(mono=1.2) is None
    assert subject.sample(mono=2)['event_type'] == 'heartbeat'


def test_fixed_quantity_requotes_depth_when_candidate_grid_changes():
    subject = sampler()
    row = subject.sample(mono=1)
    row['quotes'] = [{'units': 84}]
    result = quote_snapshot(row, 85, Direction.LONG_ENTROPY)
    assert result['state'] == 'known_quote'
    assert result['basis_bps'] is not None
    # Existing positions remain measurable beyond a new-entry notional cap.
    row['max_notional_usd'] = '1'
    result = quote_snapshot(row, 85, Direction.LONG_ENTROPY)
    assert result['state'] == 'known_quote'
    assert result['entry_quality'] == 'quantity_not_legal_for_new_entry'


def test_one_shallow_side_does_not_remove_other_direction_candidates():
    subject = sampler()
    subject.venues.entropy.book.apply_hl(
        [[{'px': '99', 'sz': '.01'}], [{'px': '101', 'sz': '1'}]])
    row = subject.sample(mono=1)
    assert max(row['candidate_units']) > 1
    assert quote_snapshot(row, 10, Direction.LONG_ENTROPY)['state'] == 'known_quote'
    assert quote_snapshot(row, 10, Direction.SHORT_ENTROPY)['state'] == 'depth_not_covered'


def test_history_is_detached_time_and_count_bounded():
    subject = sampler()
    row = subject.sample(mono=0)
    row['entropy']['bids'][0][0] = '999'
    history = subject.history()
    assert history[0]['entropy']['bids'][0][0] != '999'
    history[0]['quotes'].clear()
    assert subject.history()[0]['quotes']
    for index in range(1, 600):
        # Frequent real generation boundaries must not bypass the memory cap.
        subject.venues.entropy.book.connection_generation += 1
        subject.sample(mono=index / 100)
    assert len(subject.history()) == 512
    row = subject.sample(mono=40)
    assert len(subject.history()) == 1
    assert row['history_coverage']['evicted_total'] == 600


def test_timestamp_is_preserved_only_when_available():
    subject = sampler()
    subject.venues.entropy.book.exchange_ts = 123456
    row = subject.sample(mono=1)
    assert row['entropy']['exchange_ts'] == 123456
    assert row['lighter']['exchange_ts'] is None


def test_source_identity_hashes_real_dirty_files():
    identity = source_identity()
    root = Path(__file__).resolve().parents[1]
    for name, digest in identity['source_files'].items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest


def test_file_writes_coalesce_are_detached_and_do_not_block_loop():
    async def scenario():
        entered, release = threading.Event(), threading.Event()
        seen = []
        def slow(value):
            entered.set()
            assert release.wait(2)
            seen.append(value)
        writer = ObserverFileWriter({'checkpoint': 60, 'status': 60})
        writer.start()
        data = {'value': 1}
        writer.publish('checkpoint', slow, data)
        data['value'] = 999
        for _ in range(100):
            if entered.is_set():
                break
            await asyncio.sleep(.001)
        assert entered.is_set()
        for index in range(100):
            writer.publish('status', seen.append, index)
        assert len(writer.pending) == 1
        release.set()
        await writer.close()
        assert seen == [{'value': 1}, 99]
        assert writer.coalesced == 99
    asyncio.run(scenario())


def test_file_write_failure_is_visible_and_later_write_survives():
    async def scenario():
        seen = []
        def fail():
            raise OSError('disk unavailable')
        writer = ObserverFileWriter({'checkpoint': .001, 'status': .001})
        writer.start()
        writer.publish('checkpoint', fail)
        writer.publish('status', seen.append, 'ready')
        await writer.close()
        assert seen == ['ready']
        assert writer.errors == 1 and 'disk unavailable' in writer.last_error
    asyncio.run(scenario())


def test_file_writer_throttles_latest_value_until_close():
    async def scenario():
        seen = []
        writer = ObserverFileWriter({'checkpoint': 60})
        writer.start()
        writer.publish('checkpoint', seen.append, 1)
        for _ in range(100):
            if writer.last_write:
                break
            await asyncio.sleep(.001)
        assert seen == [1]
        writer.publish('checkpoint', seen.append, 2)
        await asyncio.sleep(.01)
        assert seen == [1]
        writer.publish('checkpoint', seen.append, 3)
        await writer.close()
        assert seen == [1, 3]
    asyncio.run(scenario())


def test_bootstrap_invalid_and_sampler_events_share_writer_sequence(tmp_path):
    async def scenario():
        writer = BufferedJSONLObserver(tmp_path / 'observer.jsonl',
                                       {'kind': 'v3_observer_header', 'pair_id': 'pair',
                                        'session_id': 's', 'config_hash': 'cfg'})
        writer.start()
        writer.publish({'kind': 'v3_market_snapshot', 'valid': False})
        writer.publish({'kind': 'v3_market_snapshot', 'valid': True, 'sample_seq': 1})
        await writer.close()
        rows = [json.loads(line) for line in writer.path.read_text().splitlines()][1:]
        assert [row['sample_seq'] for row in rows] == [1, 2]
        assert all(row['config_hash'] == 'cfg' for row in rows)
    asyncio.run(scenario())


def test_drop_boundary_precedes_next_accepted_event(tmp_path):
    async def scenario():
        writer = BufferedJSONLObserver(tmp_path / 'observer.jsonl',
                                       {'kind': 'v3_observer_header'}, queue_size=1, batch_size=1)
        writer.start()
        writer.publish({'sample_seq': 1})
        writer.publish({'sample_seq': 2})
        for _ in range(100):
            if writer.queue.empty():
                break
            await asyncio.sleep(.001)
        writer.publish({'sample_seq': 3})
        await writer.close()
        rows = [json.loads(line) for line in writer.path.read_text().splitlines()][1:]
        assert rows[1]['kind'] == 'v3_observer_gap'
        assert rows[2]['sample_seq'] == 3
        seq = [row['observer_event_seq'] for row in rows]
        assert seq == sorted(set(seq))
    asyncio.run(scenario())
