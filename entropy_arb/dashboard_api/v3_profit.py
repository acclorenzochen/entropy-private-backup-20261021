"""Read-only profit attribution; never repairs or writes the trading ledger."""
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from .v3_readmodel import number


FIELDS = ('realized', 'funding', 'fees')


def _amounts(events):
    result = {}
    for field in FIELDS:
        field_events = [event for event in events if event['field'] == field]
        values = [event['amount'] for event in field_events]
        known_values = [event.get('known_amount', event['amount']) for event in field_events]
        known = sum((value for value in known_values if value is not None), Decimal(0))
        result[f'{field}_usd'] = str(known) if all(v is not None for v in values) else None
        result[f'known_{field}_usd'] = str(known)
        result[f'pending_{field}'] = sum(v is None for v in values)
    result['net_usd'] = (str(Decimal(result['realized_usd']) + Decimal(result['funding_usd'])
                             - Decimal(result['fees_usd']))
                         if all(result[f'{field}_usd'] is not None for field in FIELDS) else None)
    return result


def _stamp(order):
    return number(order.get('terminal_recorded_ts') or order.get('terminal_received_ts'))


def _inventory(orders, step, cycle):
    """Each order is one cumulative snapshot, not a list of incremental fills."""
    if step is None or step <= 0 or any(o.get('state') != 'TERMINAL' for o in orders):
        return None
    positions = {'entropy': Decimal(0), 'hedge': Decimal(0)}
    cost = dict(positions)
    cash = realized = Decimal(0)
    filled = [o for o in orders if (number(o.get('filled_units')) or 0) > 0]
    if any(_stamp(o) is None for o in filled):
        return None
    for order in sorted(filled, key=lambda o: (_stamp(o), o['order_ref'])):
        venue, side = order.get('venue'), order.get('side')
        units, requested, px = (number(order.get(k)) for k in ('filled_units', 'requested_units', 'avg_px'))
        if (venue not in positions or side not in ('buy', 'sell') or px is None or px <= 0
                or units is None or requested is None or units > requested):
            return None
        signed = units if side == 'buy' else -units
        old = positions[venue]
        cash -= signed * px * step
        if not old or old * signed > 0:
            cost[venue] = (abs(old) * cost[venue] + units * px) / (abs(old) + units)
        else:
            closed = min(abs(old), units)
            realized += closed * (px - cost[venue]) * (1 if old > 0 else -1) * step
            if units > abs(old):
                cost[venue] = px
        positions[venue] += signed
    if any(number(cycle.get(f'{venue}_units')) != units for venue, units in positions.items()):
        return None
    return {'cash': cash, 'realized': realized}


def profit_overview(snapshot, window, facts):
    """All cards, groups and rows are sums of the same attributed events."""
    if not facts.get('available'):
        return {'available': False, 'reason': facts.get('reason'), 'cycles': [], 'daily': [], 'symbols': []}
    journal = snapshot['journal']
    live = snapshot['sources'].get('live', {}).get('data') or {}
    manifest = snapshot['sources'].get('live_manifest', {}).get('data') or {}
    identity = manifest.get('pair_identity') or live.get('pair_identity')
    mode = (manifest.get('settings') or {}).get('strategy_mode') or manifest.get('strategy_mode') or 'basis'
    symbol = identity.split(':')[2] if identity and len(identity.split(':')) >= 5 else '币种待确认'
    canonical = {r['cycle_id']: r for r in journal.get('cycles', [])}
    economics = {r['cycle_id']: r for r in journal.get('cycle_economics', [])}
    orders = defaultdict(dict)
    fee_fills = defaultdict(dict)
    steps = defaultdict(list)
    for row in journal.get('orders', []):
        orders[row['cycle_id']][row['order_ref']] = row
    for row in journal.get('fee_fills', []):
        fee_fills[(row['cycle_id'], row['order_ref'])][(row['venue'], row['exchange_fill_key'])] = row
    for row in journal.get('execution_economics_steps', []):
        steps[row['cycle_id']].append(row)
    # The durable key is venue + event_id, even when an event is returned twice.
    settlements = {}
    conflicts = set()
    for row in journal.get('funding_settlements', []):
        key = (row.get('venue'), row.get('event_id'))
        previous = settlements.get(key)
        if previous and any(previous.get(k) != row.get(k) for k in ('cycle_id', 'amount_usd', 'ts')):
            conflicts.update((previous['cycle_id'], row['cycle_id']))
        settlements[key] = row
    funding_states = {r['cycle_id']: r for r in journal.get('funding_cycles', [])}
    all_events, rows, active = [], [], []
    for cycle in facts['items']:
        if not cycle.get('has_confirmed_fill'):
            continue
        cid = cycle['cycle_id']
        canon, econ = canonical.get(cid, {}), economics.get(cid, {})
        cycle_orders = list(orders[cid].values())
        filled = [o for o in cycle_orders if (number(o.get('filled_units')) or 0) > 0]
        complete = (cycle.get('phase') == 'DONE' and cycle.get('entropy_units') == 0
                    and cycle.get('hedge_units') == 0 and cycle.get('position_evidence_authoritative')
                    and all(o.get('state') == 'TERMINAL' for o in cycle_orders))
        finished = number(canon.get('completed_ts'))
        step_values = {number(s.get('quantity_step')) for s in steps[cid] if s.get('quantity_step') is not None}
        step = next(iter(step_values)) if len(step_values) == 1 else None
        if not step_values and live.get('cycle_id') == cid:
            step = number(live.get('accounting_quantity_step'))
        inventory = _inventory(cycle_orders, step, cycle)
        final_net, final_fees, final_funding = (number(econ.get(k)) for k in
                                               ('finalized_pnl_usd', 'actual_fees_usd', 'actual_funding_usd'))
        final = bool(complete and canon.get('accounting_final') and econ.get('accounting_final')
                     and final_net is not None and final_net == number(canon.get('pnl_usd'))
                     and final_fees is not None and final_funding is not None)
        price = (final_net + final_fees - final_funding if final else
                 inventory['cash'] if complete and inventory is not None else None)
        events = []
        def add(field, amount, stamp, source, known_amount=None):
            events.append({'cycle_id': cid, 'field': field, 'amount': amount,
                           'known_amount': amount if known_amount is None else known_amount,
                           'ts': float(stamp) if stamp is not None else None, 'source': source})
        if complete or cycle.get('phase') == 'DONE':
            add('realized', price, finished, 'cycle_complete')
        elif any(o.get('reduce_only') for o in filled):
            # Partial closes are deferred to this cycle's completion day.
            pass
        fee_values = []
        for order in filled:
            fee = number(order.get('fee_usd'))
            matches = [s for s in steps[cid] if s.get('order_ref') == order['order_ref']
                       and number(s.get('result_units')) == number(order.get('filled_units'))]
            if fee is None and len(matches) == 1:
                fee = number(matches[0].get('actual_fee_usd'))
            known_fee = fee
            if fee is None:
                covered = number(order.get('fee_covered_units')) or Decimal(0)
                history = [r for r in fee_fills[(cid, order['order_ref'])].values()
                           if r['venue'] == order.get('venue') and number(r.get('fee_usd')) is not None]
                history_units = sum((number(r.get('units')) or Decimal(0) for r in history), Decimal(0))
                units = number(order.get('filled_units'))
                # Prefix receipts and fill history cover the same order; select, never add.
                if history and 0 < history_units <= units and history_units >= covered:
                    known_fee = sum((number(r['fee_usd']) for r in history), Decimal(0))
                elif 0 < covered <= units:
                    known_fee = number(order.get('known_fee_usd'))
            fee_values.append((order, fee, known_fee))
        order_fees_complete = all(fee is not None for _, fee, _ in fee_values)
        order_fees = sum((fee for _, fee, _ in fee_values if fee is not None), Decimal(0))
        # Old journals often have final cycle fees but no per-order fee history.
        fee_fallback = final and (not order_fees_complete or order_fees != final_fees
                                  or any(_stamp(o) is None for o, _, _ in fee_values))
        if fee_fallback:
            add('fees', final_fees, finished, 'cycle_accounting')
        else:
            for order, fee, known_fee in fee_values:
                add('fees', fee, _stamp(order), 'order_confirmation', known_fee)
        records = [r for r in settlements.values() if r.get('cycle_id') == cid]
        if cid in conflicts:
            add('funding', None, finished or number(cycle.get('started_ts')), 'conflicting_settlement')
            final = False
        elif records:
            for record in records:
                amount = number(record.get('amount_usd'))
                add('funding', amount, number(record.get('ts')), 'settlement')
            recorded_total = sum((number(r.get('amount_usd')) or Decimal(0) for r in records), Decimal(0))
            if final and recorded_total != final_funding:
                final = False
        elif final_funding is not None:
            add('funding', final_funding, finished, 'cycle_accounting')
        elif cid in funding_states and number(funding_states[cid].get('last_complete_poll_ts')) is not None:
            # This is zero booked to date, never proof of final settlement.
            pass
        else:
            add('funding', None, finished or number(cycle.get('started_ts')), 'funding_unavailable')
        scoped = [event for event in events if window.contains(event['ts'])]
        undated = [event for event in events if event['ts'] is None]
        # Unknown event dates cannot be silently dropped from a selected cohort.
        started = number(cycle.get('started_ts'))
        in_cohort = (window.scope == 'all' or window.contains(started) or window.contains(finished)
                     or (cycle.get('phase') != 'DONE' and started is not None
                         and (window.to_ts is None or started < Decimal(str(window.to_ts)))))
        if in_cohort:
            scoped.extend(undated)
        current = cycle.get('phase') != 'DONE' or any((number(cycle.get(key)) or 0) != 0 for key in ('entropy_units', 'hedge_units'))
        floating = gross = None
        stamp = number(live.get('source_ts'))
        if (current and live.get('cycle_id') == cid and stamp is not None
                and 0 <= snapshot['generated_at'] - float(stamp) <= 10):
            gross = number(live.get('executable_gross_usd'))
            if gross is not None and inventory is not None:
                floating = gross - inventory['realized']
        row = {'cycle_id': cid, 'symbol': symbol, 'pair_identity': identity,
               'side': cycle.get('direction'), 'phase': cycle.get('phase'),
               'started_at': cycle.get('started_ts'), 'completed_at': float(finished) if finished else None,
               'paired_round_trip': cycle.get('paired_round_trip'), 'inherited': cycle.get('inherited'),
               **{key: cycle.get(key) for key in ('first_fill_at', 'flat_confirmed_at', 'hold_seconds', 'duration_state',
                   'first_fill_confirmed_at', 'estimated_hold_seconds', 'confirmation_duration_state',
                   'first_fill_missing_order_refs', 'has_confirmed_fill')},
               'entropy_units': cycle.get('entropy_units'), 'hedge_units': cycle.get('hedge_units'),
               'order_count': len(filled), 'quantity_step': str(step) if step is not None and step > 0 else None,
               'order_fees': [{'order_ref': order['order_ref'], 'fee_usd': str(fee) if fee is not None else None,
                              'known_fee_usd': str(known_fee) if known_fee is not None else None}
                              for order, fee, known_fee in fee_values],
               'accounting': 'final' if final else 'provisional',
               'fee_basis': 'cycle_complete' if fee_fallback else 'order_confirmation',
               'unrealized_usd': str(floating) if floating is not None else None,
               'active_realized_usd': str(inventory['realized']) if current and inventory is not None else None,
               'executable_gross_usd': str(gross) if gross is not None else None,
               **_amounts(scoped)}
        if current:
            active.append(row)
        if scoped or in_cohort:
            rows.append(row)
            all_events.extend(scoped)
    daily_events = defaultdict(list)
    for event in all_events:
        day = (datetime.fromtimestamp(event['ts'], ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d')
               if event['ts'] is not None else '归属时间待确认')
        daily_events[day].append(event)
    totals = _amounts(all_events)
    pending = sum(row['accounting'] != 'final' for row in rows)
    accounting = 'pending' if totals['net_usd'] is None else 'provisional' if pending else 'final'
    row_states = {row['cycle_id']: row['accounting'] for row in rows}
    daily = []
    for day, events in sorted(daily_events.items(), reverse=True):
        amounts = _amounts(events)
        state = ('pending' if amounts['net_usd'] is None else 'provisional'
                 if any(row_states.get(event['cycle_id']) != 'final' for event in events) else 'final')
        daily.append({'day': day, **amounts, 'accounting': state})
    floating = sum((Decimal(row['unrealized_usd']) for row in active if row['unrealized_usd'] is not None), Decimal(0))
    return {'available': True, 'strategy_mode': mode, 'pair_identity': identity, 'symbol': symbol,
            'totals': {**totals, 'accounting': accounting, 'cycle_count': len(rows),
                       'complete_cycles': sum(bool(row['paired_round_trip']) for row in rows),
                       'pending_cycles': pending},
            'symbols': [{'symbol': symbol, **totals, 'accounting': accounting, 'cycle_count': len(rows),
                         'order_count': sum(row['order_count'] for row in rows)}] if rows else [],
            'daily': daily,
            'cycles': sorted(rows, key=lambda row: row['started_at'] or 0, reverse=True),
            'holding': {'cycle_count': len(active), 'cycles': active,
                        'unrealized_usd': str(floating) if all(r['unrealized_usd'] is not None for r in active) else None},
            'rules': '价格盈亏按 cycle 归零完结日；进行中部分平仓待整轮归属。资金费按实际结算日；仅有历史周期汇总时按完结日。订单手续费按成交确认日；仅有已核账周期汇总费用时按完结日。未知归属单列。日汇总固定北京时间（UTC+8）。净结果不含当前未实现盈亏；当前估值用每订单累计 VWAP 按确认顺序计算移动均价成本。'}
