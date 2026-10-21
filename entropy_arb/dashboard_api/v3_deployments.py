"""Combine independently collected deployments for GET-only dashboard display."""
from collections import defaultdict
from decimal import Decimal

from .v3_projection import public_status, manual_exit_view, cycle_facts, Window
from .v3_readmodel import number, public_sources


def deployment_info(source_id, snapshot):
    sources = snapshot['sources']
    manifest = sources.get('live_manifest', {}).get('data') or {}
    live = sources.get('live', {}).get('data') or {}
    identity = manifest.get('pair_identity') or live.get('pair_identity') or (snapshot.get('journal') or {}).get('metadata', {}).get('identity')
    mode = 'funding' if (manifest.get('settings') or {}).get('strategy_mode', manifest.get('strategy_mode')) == 'funding_hedge' else 'basis'
    symbol = identity.split(':')[2] if identity and len(identity.split(':')) >= 5 else None
    journal = snapshot.get('journal') or {}
    batch_id = live.get('batch_id') or (manifest.get('authorization') or {}).get('batch_id')
    batch = next((row for row in journal.get('batches', []) if row.get('batch_id') == batch_id), {})
    start = number(live.get('start_ts'))
    current = [row for row in journal.get('live_cycles', []) if start is not None and (number(row.get('started_ts')) or Decimal(0)) >= start]
    session_facts = cycle_facts(snapshot, Window('session', float(start), snapshot['generated_at'] + 1, 'Asia/Taipei')) if start is not None else {}
    batch_view = {key: batch.get(key) for key in ('batch_id', 'position_cycles', 'max_position_cycles', 'loss_used_usd', 'loss_budget_usd', 'closed_for_entries', 'close_reason', 'close_value', 'close_threshold', 'close_cycle_id')}
    for key in ('position_cycles', 'loss_used_usd'):
        if live.get(key) is not None:
            batch_view[key] = live[key]
    batch_view['loss_budget_usd'] = batch.get('loss_budget')
    return {'source_id': source_id, 'mode': mode, 'label': '资金费对冲 / NBIS' if mode == 'funding' else '普通价差对冲',
            'symbols': [symbol] if symbol else [], 'pair_identity': identity,
            'snapshot_id': snapshot['id'], 'generated_at': snapshot['generated_at'],
            'status': public_status(live), 'source_health': public_sources(snapshot),
            'manual_exit': manual_exit_view(snapshot),
            'batches': [{'batch_id': row['batch_id']} for row in journal.get('batches', [])],
            'batch': batch_view,
            'current_session_attempts': len(current),
            'current_session_complete': sum(bool(row.get('paired_round_trip')) for row in session_facts.get('created', [])),
            'current_session_zero_fill': sum(bool(row.get('terminal_zero_fill_campaign')) for row in session_facts.get('created', []))}


def qualify(rows, info):
    return [{**row, 'source_id': info['source_id'], 'strategy_mode': info['mode'],
             'symbol': row.get('symbol') or next(iter(info['symbols']), None)} for row in rows]


def combine_amounts(rows):
    result = {}
    for field in ('realized', 'funding', 'fees'):
        known = sum((number(row.get(f'known_{field}_usd')) or Decimal(0) for row in rows), Decimal(0))
        pending = sum(int(row.get(f'pending_{field}') or 0) for row in rows)
        result[f'known_{field}_usd'] = str(known)
        result[f'pending_{field}'] = pending
        result[f'{field}_usd'] = str(known) if not pending and all(row.get(f'{field}_usd') is not None for row in rows) else None
    result['net_usd'] = (str(Decimal(result['realized_usd']) + Decimal(result['funding_usd']) - Decimal(result['fees_usd']))
                         if all(result[f'{field}_usd'] is not None for field in ('realized', 'funding', 'fees')) else None)
    result['accounting'] = ('pending' if result['net_usd'] is None else 'provisional'
                            if any(row.get('accounting') != 'final' for row in rows) else 'final')
    return result


def merge_profit(profits, *, unavailable=0):
    rows = [row for profit in profits for row in profit.get('cycles', [])]
    totals = combine_amounts([profit['totals'] for profit in profits])
    if unavailable:
        # A missing deployment is unknown coverage, never a zero balance.
        for field in ('realized', 'funding', 'fees'):
            totals[f'{field}_usd'] = None
            totals[f'pending_{field}'] += unavailable
        totals.update(net_usd=None, accounting='pending')
    totals.update(cycle_count=len(rows), complete_cycles=sum(bool(row.get('paired_round_trip')) for row in rows),
                  pending_cycles=sum(row.get('accounting') != 'final' for row in rows))
    symbols, daily = defaultdict(list), defaultdict(list)
    for profit in profits:
        for row in profit.get('symbols', []):
            symbols[row['symbol']].append(row)
        for row in profit.get('daily', []):
            daily[row['day']].append(row)
    holding = [row for profit in profits for row in profit.get('holding', {}).get('cycles', [])]
    floating = sum((number(row.get('unrealized_usd')) or Decimal(0) for row in holding), Decimal(0))
    return {'available': True, 'strategy_mode': 'all', 'symbol': ' / '.join(sorted(symbols)) or '全部币种',
            'totals': totals, 'cycles': sorted(rows, key=lambda row: row.get('first_fill_at') or row.get('started_at') or 0, reverse=True),
            'symbols': [{'symbol': symbol, **combine_amounts(values), 'cycle_count': sum(r['cycle_count'] for r in values),
                         'order_count': sum(r['order_count'] for r in values)} for symbol, values in sorted(symbols.items())],
            'daily': [{'day': day, **combine_amounts(values)} for day, values in sorted(daily.items(), reverse=True)],
            'holding': {'cycle_count': len(holding), 'cycles': holding,
                        'unrealized_usd': str(floating) if not unavailable and all(r.get('unrealized_usd') is not None for r in holding) else None},
            'coverage_missing_sources': unavailable,
            'rules': next((p.get('rules') for p in profits if p.get('rules')), None)}
