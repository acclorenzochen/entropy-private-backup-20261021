#!/usr/bin/env python3
"""Read-only V3 execution-economics report."""
from __future__ import annotations

import argparse
import csv
from dataclasses import fields
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from entropy_arb.v3.economics import canonical_attempts, summarize, summarize_attempts, summarize_system
from entropy_arb.v3.core import Settings


def read_rows(path: Path) -> tuple[list[dict], list[dict], list[dict], list[dict],
                                    list[dict], list[dict]]:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    db = sqlite3.connect(uri, uri=True, timeout=2.0)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        attempts = canonical_attempts(
            [dict(row) for row in db.execute('SELECT * FROM execution_economics_steps')],
            [dict(row) for row in db.execute('SELECT * FROM live_steps')],
            [dict(row) for row in db.execute('SELECT * FROM orders')])
        canonical = [dict(row) for row in db.execute('SELECT * FROM cycles')]
        canonical_by_id = {row['cycle_id']: row for row in canonical}
        cycles = [dict(row) for row in db.execute('SELECT * FROM live_cycles')]
        for row in cycles:
            row['completed_ts'] = canonical_by_id.get(row['cycle_id'], {}).get('completed_ts')
        return (
            [dict(row) for row in db.execute(
                'SELECT * FROM cycle_economics ORDER BY signal_ts')],
            cycles,
            attempts,
            canonical,
            [dict(row) for row in db.execute('SELECT * FROM request_events')],
            [dict(row) for row in db.execute('SELECT * FROM live_steps')],
        )
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', type=Path, required=True)
    parser.add_argument('--json', dest='json_path', type=Path)
    parser.add_argument('--config', type=Path,
                        help='V3 settings used for this journal, for effective policy display')
    parser.add_argument('--window-start-ts', type=float)
    parser.add_argument('--window-end-ts', type=float)
    parser.add_argument('--valuation', type=Path,
                        help='JSON authoritative positions and executable depth at window end')
    parser.add_argument('--fee-events', type=Path,
                        help='JSON list of {event_ts, fee_usd} with actual fee event times')
    parser.add_argument('--effective-manifest', type=Path,
                        help='Startup manifest reference; report does not verify live authorization')
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args()
    try:
        rows, cycles, attempts, canonical, requests, live_steps = read_rows(args.journal)
        cycle_by_id = {row['cycle_id']: row for row in cycles}
        selected = rows
        if args.window_start_ts is not None or args.window_end_ts is not None:
            selected = []
            for row in rows:
                cycle = cycle_by_id.get(row['cycle_id'], {})
                completed = cycle.get('completed_ts', cycle.get('flat_confirmed_ts'))
                if completed is not None \
                        and (args.window_start_ts is None or completed >= args.window_start_ts) \
                        and (args.window_end_ts is None or completed < args.window_end_ts):
                    selected.append(row)
        report = summarize(selected)
        report['system_economics'] = summarize_system(
            rows, cycles, canonical, requests,
            window_start_ts=args.window_start_ts,
            window_end_ts=args.window_end_ts,
            orders=[row['canonical_order'] for row in attempts if row.get('canonical_order')],
            valuation=json.loads(args.valuation.read_text()) if args.valuation else None,
            fee_events=json.loads(args.fee_events.read_text()) if args.fee_events else ())
        report['execution_attempts'] = summarize_attempts(
            attempts, cycles, rows, live_steps,
            window_start_ts=args.window_start_ts, window_end_ts=args.window_end_ts)
        report['measurement_quality'] = {
            'system': report['system_economics']['measurement_quality'],
            'attempts': report['execution_attempts']['measurement_quality'],
            'missing_economics_step_keys': report['execution_attempts']['missing_economics_step_keys'],
        }
        report['effective_manifest'] = None
        if args.effective_manifest:
            raw_manifest = args.effective_manifest.read_bytes()
            manifest = json.loads(raw_manifest)
            report['effective_manifest'] = {
                'sha256': hashlib.sha256(raw_manifest).hexdigest(),
                'scope': 'provided_startup_manifest_not_independently_verified',
                'manifest': {key: manifest[key] for key in (
                    'config_sha256', 'build_sha256', 'pair', 'deployment',
                    'schema_version', 'pid', 'started_ts', 'module_paths',
                    'effective_max_notional_usd', 'authorization_source',
                    'open_first_rooms_bps', 'open_first_hard_cap_bps',
                    'close_first_rooms_bps', 'close_first_hard_cap_bps',
                    'hedge_rooms_bps', 'hedge_hard_cap_bps',
                    'exit_fast_path_enabled') if key in manifest},
            }
        report['effective_settings'] = None
        if args.config is not None:
            raw = args.config.read_bytes()
            settings = Settings.from_mapping(json.loads(raw))
            open_rooms, open_cap = settings.first_leg_execution_policy('open')
            close_rooms, close_cap = settings.first_leg_execution_policy('close')
            report['effective_settings'] = {
                'config_sha256': hashlib.sha256(raw).hexdigest(),
                'scope': 'configured_only_authorization_unavailable',
                'effective_max_notional_usd': None,
                'configured_settings': {
                    field.name: ([str(item) for item in value]
                                 if isinstance(value := getattr(settings, field.name), tuple)
                                 else str(value) if value is not None and hasattr(value, 'as_tuple')
                                 else value)
                    for field in fields(settings)
                },
                'open_first_rooms_bps': [str(value) for value in open_rooms],
                'open_first_hard_cap_bps': str(open_cap),
                'close_first_rooms_bps': [str(value) for value in close_rooms],
                'close_first_hard_cap_bps': str(close_cap),
                'hedge_rooms_bps': [str(value) for value in settings.hedge_rooms_bps],
                'hedge_hard_cap_bps': str(settings.hedge_hard_cap_bps),
                'exit_fast_path_enabled': settings.exit_fast_path_enabled,
                'entropy_orders_per_min': settings.entropy_orders_per_min,
                'hedge_orders_per_min': settings.hedge_orders_per_min,
            }
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError) as exc:
        print(f'cannot build V3 economics report: {exc}', file=sys.stderr)
        return 2
    encoded = json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False)
    print(encoded)
    if args.json_path is not None:
        args.json_path.parent.mkdir(parents=True, exist_ok=True)
        args.json_path.write_text(encoded + '\n', encoding='utf-8')
    if args.output_dir is not None:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        for filename, value in (
                ('system_economics', report['system_economics']),
                ('execution_attribution', report['execution_attempts']),
                ('measurement_quality', report['measurement_quality']),
                ('effective_manifest', report['effective_manifest'])):
            (args.output_dir / f'{filename}.json').write_text(
                json.dumps(value, indent=2, sort_keys=True) + '\n')
        with (args.output_dir / 'execution_latency.csv').open('w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=[
                'metric', 'count', 'eligible_count', 'missing_count', 'negative_count',
                'p50', 'p90', 'p95'])
            writer.writeheader()
            for name, values in report['execution_attempts']['latency'].items():
                writer.writerow({'metric': name, **{key: values.get(key)
                    for key in writer.fieldnames if key != 'metric'}})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
