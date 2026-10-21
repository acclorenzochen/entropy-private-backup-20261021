#!/usr/bin/env python3
"""Offline actual Runtime + FakeVenue + temporary Journal FULL benchmark.

No real venue/network/account/production process is used. Results describe
fixture ordering and local wall times, NOT production latency improvement.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import socket
import statistics
import sys
import tempfile
import threading
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests_v3.test_runtime import setup_runtime, seed_close_cycle
from entropy_arb.v3.core import Side
from entropy_arb.v3.economics import _quote_vwap
from entropy_arb.v3.live_host import StatusWriter


async def sample(directory: Path, mode: str, delay: float):
    with patch('entropy_arb.v3.runtime.load_checkpoint', return_value=None):
        runtime, entropy, hedge, journal = setup_runtime(directory)
    runtime.reference_checkpoint_path = directory / 'reference_checkpoint.json'
    runtime.settings = replace(runtime.settings, exit_fast_path_enabled=False)
    stages = []
    origin = time.perf_counter_ns()
    def mark(name, **extra):
        stages.append({'stage': name, 'elapsed_ns': time.perf_counter_ns() - origin, **extra})
    release_status = threading.Event()
    status_entered = threading.Event()
    equity_entered = asyncio.Event()
    release_equity = asyncio.Event()
    equity_task = None
    try:
        await runtime.initialize()
        assert journal.db.execute('PRAGMA synchronous').fetchone()[0] == 2, 'Journal must remain FULL'
        await runtime.status.close()
        runtime.status = StatusWriter(directory / 'background-status.json', strategy_version='offline',
                                      config_hash='fixture', mode='benchmark', background=True)
        cycle = seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85, phase='CLOSE_FIRST')
        entropy.fill_fractions = [30 / 85]
        # Multi-level fixtures ensure partial VWAP uses more than a single level.
        entropy.book.apply_hl([[{'px': '100.10', 'sz': '.002'},
                               {'px': '100.09', 'sz': '.007'},
                               {'px': '100.08', 'sz': '1'}],
                              [{'px': '100.11', 'sz': '2'}]])
        hedge.book.apply_hl([[{'px': '99.99', 'sz': '2'}],
                            [{'px': '100', 'sz': '.002'},
                             {'px': '100.01', 'sz': '.007'},
                             {'px': '100.02', 'sz': '1'}]])
        market = runtime._market()
        prefix = runtime._quote_snapshot(market, entropy_side=Side.SELL, units=85)
        pricing = []
        for name, side in (('entropy', Side.SELL), ('hedge', Side.BUY)):
            book = getattr(market, name)
            assert len(prefix[name]['bids' if side is Side.SELL else 'asks']) == 2
            for quantity in (30, 85):
                actual = _quote_vwap(prefix, name, side, quantity)
                cash, _ = book.quote(side, quantity, runtime.instrument.quantity_step)
                expected = cash / (quantity * runtime.instrument.quantity_step)
                assert actual == expected, 'prefix must price partial and requested quantity exactly'
                pricing.append({'venue': name, 'units': quantity, 'prefix_vwap': str(actual),
                                'full_depth_vwap': str(expected)})
        original_e, original_h = entropy.send_taker, hedge.send_taker
        async def e_send(**kwargs):
            mark('E_submit')
            result = await original_e(**kwargs)
            mark('E_terminal', filled_base=result['filled_base'])
            return result
        async def h_send(**kwargs):
            mark('H_submit', requested_base=kwargs['qty'])
            release_status.set()
            release_equity.set()
            result = await original_h(**kwargs)
            mark('H_terminal', filled_base=result['filled_base'])
            return result
        entropy.send_taker, hedge.send_taker = e_send, h_send
        original_analytics = journal.record_execution_benchmark
        def analytics(*args, **kwargs):
            mark('analytics_start', purpose=kwargs.get('purpose'))
            if mode == 'slow_analytics':
                time.sleep(delay)
            result = original_analytics(*args, **kwargs)
            mark('analytics_end')
            return result
        journal.record_execution_benchmark = analytics
        for venue in (entropy, hedge):
            original = venue.fetch_position
            async def position(original=original, key=venue.key):
                mark('position_read', venue=key)
                return await original()
            venue.fetch_position = position
        if mode == 'slow_status':
            original_write = runtime.status._write_file
            def write(payload):
                mark('status_worker_start')
                status_entered.set()
                time.sleep(delay)
                # A bounded latch proves submission can progress while this
                # file operation is outstanding, without a timing threshold.
                released = release_status.wait(2)
                mark('status_worker_end', handoff_released=released)
                original_write(payload)
            runtime.status._write_file = write
            mark('status_publish')
            runtime.status.write('READY', 'offline_slow_status')
            mark('status_publish_return')
            assert await asyncio.to_thread(status_entered.wait, 2), 'status worker did not start'
        if mode == 'slow_equity':
            original_equity = entropy.fetch_equity
            async def equity():
                mark('equity_start')
                equity_entered.set()
                await asyncio.sleep(delay)
                await asyncio.wait_for(release_equity.wait(), 2)
                value = await original_equity()
                mark('equity_end')
                return value
            entropy.fetch_equity = equity
            equity_task = asyncio.create_task(runtime._equity_loop())
            await asyncio.wait_for(equity_entered.wait(), 2)
        mark('close_start')
        started = time.perf_counter_ns()
        await runtime._close(cycle)
        close_wall_ns = time.perf_counter_ns() - started
        mark('close_end')
        runtime.stop.set()
        release_equity.set()
        if equity_task is not None:
            await asyncio.wait_for(equity_task, 3)
        assert await runtime.status.close(), 'status writer did not drain'
        ordered = sorted(stages, key=lambda row: row['elapsed_ns'])
        names = [row['stage'] for row in ordered]
        assert len(entropy.sent) == len(hedge.sent) == 1
        assert round(entropy.sent[0]['qty'] * 10000) == 85
        assert round(hedge.sent[0]['qty'] * 10000) == 30
        assert all(order['reduce_only'] for order in entropy.sent + hedge.sent)
        active = journal.active_live_cycle()
        assert (active['entropy_units'], active['hedge_units'], active['phase']) == (55, -55, 'OPEN')
        assert round(entropy.position * 10000) == 55 and round(hedge.position * 10000) == -55
        assert names.index('E_terminal') < names.index('H_submit')
        assert 'analytics_start' in names
        assert all(i > names.index('H_submit') for i, name in enumerate(names) if name == 'analytics_start')
        if mode == 'slow_status':
            assert names.index('status_publish_return') < names.index('status_worker_end')
            assert names.index('H_submit') < names.index('status_worker_end')
            assert all(row['handoff_released'] for row in ordered if row['stage'] == 'status_worker_end')
        if mode == 'slow_equity':
            assert names.index('equity_start') < names.index('H_submit') < names.index('equity_end')
        def at(name):
            return next(row['elapsed_ns'] for row in ordered if row['stage'] == name)
        return {'passed': True, 'mode': mode, 'close_wall_ns': close_wall_ns,
                'sample_wall_ns': time.perf_counter_ns() - origin,
                'E_terminal_to_H_submit_ns': at('H_submit') - at('E_terminal'),
                'close_to_E_submit_ns': at('E_submit') - at('close_start'),
                'journal_synchronous': 'FULL', 'fast_path_enabled': False,
                'prefix_pricing': pricing, 'raw_stages': ordered,
                'final_units': [55, -55], 'position_rpc_count': names.count('position_read')}
    except Exception as exc:
        return {'passed': False, 'mode': mode, 'error': f'{type(exc).__name__}: {exc}',
                'raw_stages': sorted(stages, key=lambda row: row['elapsed_ns']),
                'sample_wall_ns': time.perf_counter_ns() - origin}
    finally:
        release_status.set()
        release_equity.set()
        runtime.stop.set()
        if equity_task is not None and not equity_task.done():
            equity_task.cancel()
            await asyncio.gather(equity_task, return_exceptions=True)
        await runtime.status.close()
        journal.close()


def run(samples, delay):
    root = Path(__file__).resolve().parents[1]
    paths = ['tools/v3_edge_benchmark.py', 'tests_v3/test_runtime.py',
             'entropy_arb/v3/runtime.py', 'entropy_arb/v3/journal.py',
             'entropy_arb/v3/live_host.py', 'entropy_arb/v3/core.py',
             'entropy_arb/v3/economics.py', 'entropy_arb/book.py']
    def hashes():
        return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in paths}
    before = hashes()
    started = time.perf_counter_ns()
    rows = []
    def no_network(*args, **kwargs):
        raise RuntimeError('network prohibited in offline benchmark')
    with tempfile.TemporaryDirectory(prefix='entropy-edge-benchmark-') as temporary:
        with patch.object(socket.socket, 'connect', no_network), patch.object(socket.socket, 'connect_ex', no_network), \
                patch.object(socket, 'getaddrinfo', no_network):
            for mode in ('ordinary_close', 'slow_analytics', 'slow_status', 'slow_equity'):
                for index in range(samples):
                    directory = Path(temporary) / f'{mode}-{index}'
                    directory.mkdir()
                    result = asyncio.run(sample(directory, mode, delay))
                    rows.append({**result, 'sample_index': index})
    after = hashes()
    summaries = {}
    for mode in ('ordinary_close', 'slow_analytics', 'slow_status', 'slow_equity'):
        selected = [row for row in rows if row['mode'] == mode and row['passed']]
        summaries[mode] = {'N': samples, 'passed_N': len(selected)}
        for metric in ('close_wall_ns', 'E_terminal_to_H_submit_ns', 'close_to_E_submit_ns'):
            values = sorted(row[metric] / 1e6 for row in selected)
            if values:
                summaries[mode][metric.removesuffix('_ns') + '_ms'] = {
                    'p50': statistics.median(values), 'p95': values[max(0, (95 * len(values) + 99) // 100 - 1)],
                    'min': values[0], 'max': values[-1]}
    return {'passed': all(row['passed'] for row in rows) and before == after,
            'N_per_scenario': samples, 'N_total': len(rows), 'injected_delay_seconds': delay,
            'wall_ns': time.perf_counter_ns() - started, 'source_hashes': before,
            'source_hashes_after': after, 'source_unchanged': before == after,
            'python_executable': sys.executable, 'python_version': sys.version,
            'live_actions': 'NONE', 'network': 'blocked', 'fast_path_enabled': False,
            'limitations': ['Offline fixture wall times are not production latency measurements or improvement evidence.',
                            'Status/equity add bounded handoff latches after the injected delay to test outstanding work.',
                            'Direct actual Runtime._close path; no production main loop or real venue transport.'],
            'summary': summaries, 'samples': rows}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--samples', type=int, default=5, help='N per scenario (1-100)')
    parser.add_argument('--delay', type=float, default=.02, help='injected slow dependency seconds (0.02-1)')
    parser.add_argument('--output', type=Path,
                        default=Path('reports/entropy-edge-v2/execution_path_benchmark.json'))
    args = parser.parse_args(argv)
    if not 1 <= args.samples <= 100 or not .02 <= args.delay <= 1:
        parser.error('samples must be 1-100 and delay 0.02-1 seconds')
    report = run(args.samples, args.delay)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=args.output.parent, delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write('\n')
    try:
        os.replace(temporary, args.output)
    finally:
        temporary.unlink(missing_ok=True)
    print(json.dumps({'passed': report['passed'], 'N': report['N_total'], 'output': str(args.output)}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
