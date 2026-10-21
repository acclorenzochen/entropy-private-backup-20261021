#!/usr/bin/env python3
"""Fixed-argument local worker; preview is read-only, execution uses the exit CLI."""
from __future__ import annotations

import asyncio
from contextlib import closing, redirect_stdout
from dataclasses import asdict, is_dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import sys
import time

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import v3_exit_only as exit_cli
from entropy_arb.process_guard import ProcessGuard, ProcessGuardError


PREVIEW_TTL_SECONDS = 15.0
ACCOUNT_MAX_AGE_SECONDS = 5.0
READ_TIMEOUT_SECONDS = 15.0
QUOTE_TIMEOUT_SECONDS = 3.0


class ControlRefusal(ValueError):
    """Only these fixed codes are safe to return to the local HTTP service."""


def _require(condition, code):
    if not condition:
        raise ControlRefusal(code)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False, default=str).encode()).hexdigest()


def parser():
    p = exit_cli.parser()
    p.description = 'Local V3 exit preview or confirmation worker'
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--preview-only', action='store_true')
    mode.add_argument('--confirmation-json', help='server-owned private receipt file')
    return p


def configuration_hashes(args, cfg, settings, pair):
    files = {'config': args.config, 'env': args.env_file,
             'v3_config': args.v3_config, 'authorization': args.batch_authorization}
    hashes = {name: hashlib.sha256(Path(path).read_bytes()).hexdigest()
              for name, path in files.items()}
    hashes['settings'] = exit_cli.canonical_config_hash(settings, pair_identity=pair)
    # dotenv preserves existing process environment; bind the effective parsed configuration too.
    hashes['effective_config'] = _hash(asdict(cfg) if is_dataclass(cfg) else vars(cfg))
    hashes['locations'] = _hash({name: str(Path(path).resolve()) for name, path in {
        **files, 'journal': args.journal, 'lock': cfg.persistence.lock_file}.items()})
    hashes['exit_tool'] = hashlib.sha256(Path(exit_cli.__file__).read_bytes()).hexdigest()
    return hashes


def _journal_snapshot(args, pair, authorization, account_scope):
    path = Path(args.journal)
    _require(path.is_file(), 'existing_journal_required')
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True,
                                timeout=.5, isolation_level=None)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        metadata = dict(db.execute('SELECT key,value FROM metadata'))
        _require(metadata.get('identity') == f'{pair}:account', 'journal_identity_mismatch')
        _require(metadata.get('version') == '1'
                 and metadata.get('economics_schema_version') == '8', 'journal_schema_unsupported')
        active = [dict(row) for row in db.execute("SELECT * FROM live_cycles WHERE phase!='DONE'")]
        _require(len(active) <= 1, 'multiple_active_cycles')
        _require(bool(active), 'cycle_already_closed')
        cycle = active[0]
        _require(cycle['cycle_id'] == args.expected_cycle_id, 'cycle_changed')
        _require(cycle['batch_id'] == authorization.batch_id, 'batch_mismatch')
        cycle_row = db.execute('SELECT * FROM cycles WHERE cycle_id=?',
                               (args.expected_cycle_id,)).fetchone()
        _require(cycle_row is not None and cycle_row['batch_id'] == authorization.batch_id,
                 'cycle_evidence_missing')
        _require(not db.execute('SELECT 1 FROM funding_cycles WHERE cycle_id=?',
                                (args.expected_cycle_id,)).fetchone(), 'funding_owner_required')
        batch = db.execute('SELECT * FROM batches WHERE batch_id=?',
                           (authorization.batch_id,)).fetchone()
        _require(batch is not None and batch['max_position_cycles'] == authorization.max_position_cycles
                 and Decimal(batch['loss_budget']) == authorization.loss_budget_usd,
                 'batch_authorization_changed')
        prior = metadata.get(f'manual_exit_account:{args.expected_cycle_id}')
        _require(prior is None or prior == account_scope, 'account_binding_changed')
        pending = [dict(row) for row in db.execute("SELECT * FROM orders WHERE state!='TERMINAL'")]
        _require(all(row['cycle_id'] == args.expected_cycle_id for row in pending),
                 'another_cycle_pending')
        _require(all(row['reduce_only'] for row in pending), 'pending_open_requires_reconciliation')
        _require(all(row['state'] != 'SUBMITTING' for row in pending), 'order_inflight_requires_reconciliation')
        orders = [dict(row) for row in db.execute(
            'SELECT rowid AS sequence,* FROM orders WHERE cycle_id=? ORDER BY rowid', (args.expected_cycle_id,))]
        snapshot = {'cycle': cycle, 'cycle_result': dict(cycle_row), 'batch': dict(batch),
                    'account_binding': prior, 'identity': metadata['identity']}
        return {'journal_watermark': _hash(snapshot), 'orders_watermark': _hash(orders),
                'cycle': cycle, 'orders': orders, 'pending': pending}


def _decimal(value, code):
    _require(not isinstance(value, bool) and isinstance(value, (str, int, float, Decimal)), code)
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise ControlRefusal(code) from None
    _require(number.is_finite(), code)
    return number


def _hl_position(raw, coin):
    _require(isinstance(raw, dict) and isinstance(raw.get('assetPositions'), list)
             and isinstance(raw.get('marginSummary'), dict), 'hl_account_shape_invalid')
    _decimal(raw['marginSummary'].get('accountValue'), 'hl_account_shape_invalid')
    positions = []
    for entry in raw['assetPositions']:
        _require(isinstance(entry, dict) and isinstance(entry.get('position'), dict),
                 'hl_position_invalid')
        position = entry['position']
        _require(isinstance(position.get('coin'), str), 'hl_position_invalid')
        quantity = _decimal(position.get('szi'), 'hl_position_invalid')
        if position['coin'] == coin:
            positions.append(quantity)
    _require(len(positions) <= 1, 'hl_duplicate_position')
    return positions[0] if positions else Decimal(0)


def _lighter_position(raw, account_index, market_id):
    _require(isinstance(raw, dict) and raw.get('code') == 200
             and isinstance(raw.get('accounts'), list), 'lighter_account_shape_invalid')
    accounts = raw['accounts']
    _require(len(accounts) == 1 and isinstance(accounts[0], dict), 'lighter_account_identity_mismatch')
    account = accounts[0]
    index = account.get('index', account.get('account_index'))
    _require(type(index) is int and index == account_index, 'lighter_account_identity_mismatch')
    _require(isinstance(account.get('positions'), list), 'lighter_positions_missing')
    _decimal(account.get('total_asset_value'), 'lighter_account_shape_invalid')
    positions = []
    for position in account['positions']:
        _require(isinstance(position, dict) and type(position.get('market_id')) is int,
                 'lighter_position_invalid')
        quantity = _decimal(position.get('position'), 'lighter_position_invalid')
        sign = position.get('sign')
        _require(type(sign) is int and sign in {-1, 1} and quantity >= 0,
                 'lighter_position_invalid')
        if position['market_id'] == market_id:
            positions.append(quantity * sign)
    _require(len(positions) <= 1, 'lighter_duplicate_position')
    return positions[0] if positions else Decimal(0)


def _book_sides(raw, *, coin=None, market_id=None):
    _require(isinstance(raw, dict), 'book_shape_invalid')
    if coin is not None:
        _require(raw.get('coin') == coin and isinstance(raw.get('levels'), list)
                 and len(raw['levels']) == 2, 'book_identity_invalid')
        sides, price_key, quantity_key = raw['levels'], 'px', 'sz'
    else:
        _require(raw.get('code') == 200 and raw.get('market_id', market_id) == market_id,
                 'book_identity_invalid')
        sides, price_key, quantity_key = [raw.get('bids'), raw.get('asks')], 'price', 'remaining_base_amount'
    books = []
    for index, rows in enumerate(sides):
        _require(isinstance(rows, list), 'book_shape_invalid')
        depth = {}
        for row in rows:
            _require(isinstance(row, dict), 'book_level_invalid')
            price = _decimal(row.get(price_key), 'book_level_invalid')
            quantity = _decimal(row.get(quantity_key), 'book_level_invalid')
            _require(price > 0 and quantity >= 0, 'book_level_invalid')
            if quantity:
                depth[price] = depth.get(price, Decimal(0)) + quantity
        _require(bool(depth), 'book_depth_missing')
        books.append(sorted(depth.items(), reverse=index == 0))
    _require(books[0][0][0] <= books[1][0][0], 'book_crossed')
    return books


async def _read_quote_books(entropy, hedge):
    observed = time.time()
    try:
        hl_raw, lighter_raw = await asyncio.wait_for(asyncio.gather(
            entropy._info({'type': 'l2Book', 'coin': entropy.coin}),
            hedge._get('/api/v1/orderBookOrders', params={'market_id': hedge.market_id, 'limit': 250})),
            timeout=QUOTE_TIMEOUT_SECONDS)
        _require(0 <= time.time() - observed < ACCOUNT_MAX_AGE_SECONDS, 'book_evidence_stale')
        return {'books': [_book_sides(hl_raw, coin=entropy.coin),
                          _book_sides(lighter_raw, market_id=hedge.market_id)],
                'observed_at': observed, 'reason': None}
    except Exception as exc:
        return {'books': None, 'observed_at': None,
                'reason': str(exc) if isinstance(exc, ControlRefusal) else 'book_request_unavailable'}


def _gross_estimate(orders, step, units, quote):
    unknown = {'executable_gross_usd': None, 'valuation_ts': None}
    if any(row['state'] != 'TERMINAL' for row in orders):
        return {**unknown, 'valuation_reason': 'pending_orders'}
    if not quote or quote['books'] is None:
        return {**unknown, 'valuation_reason': (quote or {}).get('reason') or 'valuation_unavailable'}
    if not 0 <= time.time() - quote['observed_at'] < ACCOUNT_MAX_AGE_SECONDS:
        return {**unknown, 'valuation_reason': 'book_evidence_stale'}
    cash = Decimal(0)
    try:
        for row in orders:
            _require(type(row['filled_units']) is int and type(row['requested_units']) is int
                     and 0 <= row['filled_units'] <= row['requested_units']
                     and row['requested_units'] > 0 and row['side'] in {'buy', 'sell'}
                     and row['venue'] in {'entropy', 'hedge'}, 'fill_price_missing_or_invalid')
            if row['filled_units']:
                price = _decimal(row['avg_px'], 'fill_price_missing_or_invalid')
                _require(price > 0, 'fill_price_missing_or_invalid')
                cash += row['filled_units'] * step * price * (1 if row['side'] == 'sell' else -1)
        for signed_units, book in zip(units, quote['books']):
            remaining = abs(signed_units) * step
            for price, available in book[0 if signed_units > 0 else 1]:
                take = min(remaining, available)
                cash += take * price * (1 if signed_units > 0 else -1)
                remaining -= take
                if not remaining:
                    break
            _require(remaining == 0, 'insufficient_exit_depth')
    except (ControlRefusal, ArithmeticError) as exc:
        return {**unknown, 'valuation_reason': str(exc) if isinstance(exc, ControlRefusal)
                else 'valuation_arithmetic_invalid'}
    return {'executable_gross_usd': str(cash), 'valuation_ts': quote['observed_at'],
            'valuation_reason': 'full_depth_price_cash_before_fees_and_funding'}


async def _account_quantities(cfg, expected_hl_account, expected_hedge_account_index, *, include_quote=True):
    import aiohttp

    async with aiohttp.ClientSession(trust_env=False) as session:
        entropy, hedge = exit_cli.main_v3._make_venues(cfg, session)
        quote_task = None
        try:
            await asyncio.gather(entropy.load_market(), hedge.load_market())
            _require(entropy._query_address() == expected_hl_account.lower(), 'query_account_mismatch')
            _require(entropy.coin == f'{cfg.entropy.hl_dex}:{cfg.entropy.symbol}'
                     and type(hedge.market_id) is int and hedge.market_id >= 0,
                     'market_identity_mismatch')
            decimals = (entropy.size_decimals, hedge.size_decimals)
            _require(all(type(value) is int and 0 <= value <= 18 for value in decimals),
                     'quantity_precision_invalid')
            step = max(Decimal(1).scaleb(-value) for value in decimals)
            observed = time.time()
            if include_quote:
                quote_task = asyncio.create_task(_read_quote_books(entropy, hedge))
            hl_raw, lighter_raw = await asyncio.gather(
                entropy._info({'type': 'clearinghouseState', 'user': expected_hl_account.lower(),
                               'dex': cfg.entropy.hl_dex}),
                hedge._get('/api/v1/account', params={'by': 'index',
                                                     'value': str(expected_hedge_account_index)}))
            quantities = (_hl_position(hl_raw, entropy.coin),
                          _lighter_position(lighter_raw, expected_hedge_account_index, hedge.market_id))
            _require(0 <= time.time() - observed <= ACCOUNT_MAX_AGE_SECONDS, 'account_evidence_stale')
            units = []
            for quantity in quantities:
                scaled = quantity / step
                _require(scaled == scaled.to_integral_value(), 'position_off_common_grid')
                units.append(int(scaled))
            quote = await quote_task if quote_task is not None else None
            _require(0 <= time.time() - observed <= ACCOUNT_MAX_AGE_SECONDS, 'account_evidence_stale')
            return step, units, observed, quote
        finally:
            if quote_task is not None and not quote_task.done():
                quote_task.cancel()
                await asyncio.gather(quote_task, return_exceptions=True)
            await asyncio.gather(entropy.close(), hedge.close(), return_exceptions=True)


async def _preview_locked(args, cfg, settings, pair, authorization, account_scope, config_hashes, *,
                          include_quote=True):
    _require(configuration_hashes(args, cfg, settings, pair) == config_hashes, 'configuration_changed')
    before = _journal_snapshot(args, pair, authorization, account_scope)
    step, units, observed, quote = await asyncio.wait_for(
        _account_quantities(cfg, args.expected_hl_account, args.expected_hedge_account_index,
                            include_quote=include_quote),
        timeout=READ_TIMEOUT_SECONDS)
    after = _journal_snapshot(args, pair, authorization, account_scope)
    _require((before['journal_watermark'], before['orders_watermark']) ==
             (after['journal_watermark'], after['orders_watermark']), 'journal_changed_during_preview')
    _require(configuration_hashes(args, cfg, settings, pair) == config_hashes, 'configuration_changed')
    pending_venues = {row['venue'] for row in after['pending']}
    for index, venue in enumerate(('entropy', 'hedge')):
        if venue not in pending_venues:
            known = sum((1 if row['side'] == 'buy' else -1) * row['filled_units']
                        for row in after['orders'] if row['venue'] == venue)
            _require(known == units[index], 'account_cycle_quantity_mismatch')
    receipt = {'schema_version': 1, 'pair_identity': pair, 'account_scope_hash': _hash(account_scope),
               'cycle_id': args.expected_cycle_id, 'batch_id': authorization.batch_id,
               'quantity_step': str(step), 'entropy_units': units[0], 'hedge_units': units[1],
               'close_fills_baseline': {venue: {row['order_ref']: row['filled_units']
                   for row in after['orders'] if row['venue'] == venue and row['reduce_only']}
                   for venue in ('entropy', 'hedge')},
               'journal_watermark': after['journal_watermark'], 'orders_watermark': after['orders_watermark'],
               'config_hashes': config_hashes, 'observed_at': observed,
               'expires_at': observed + PREVIEW_TTL_SECONDS}
    preview = {key: receipt[key] for key in ('cycle_id', 'batch_id', 'pair_identity', 'account_scope_hash',
                                            'quantity_step', 'entropy_units', 'hedge_units')}
    preview.update(symbol=cfg.entropy.symbol, position_observed_ts=observed, authoritative=True,
                   fees_usd=None, funding_usd=None, fee_basis='actual_fees_and_funding_pending')
    preview.update(_gross_estimate(after['orders'], step, units, quote))
    return {'preview': preview, 'receipt': receipt}


async def preview_exit(args, cfg, settings, pair, authorization, account_scope, config_hashes):
    with ProcessGuard(cfg.persistence.lock_file):
        return await _preview_locked(args, cfg, settings, pair, authorization, account_scope, config_hashes)


def confirmation_check(receipt, args, cfg, settings, pair, authorization, account_scope, config_hashes):
    def check_expiry():
        _require(isinstance(receipt, dict), 'confirmation_invalid')
        _require(type(receipt.get('schema_version')) is int and receipt['schema_version'] == 1
                 and type(receipt.get('entropy_units')) is int
                 and type(receipt.get('hedge_units')) is int, 'confirmation_invalid')
        observed, expires = receipt.get('observed_at'), receipt.get('expires_at')
        _require(type(observed) in (int, float) and type(expires) in (int, float)
                 and math.isfinite(observed) and math.isfinite(expires)
                 and 0 < expires - observed <= PREVIEW_TTL_SECONDS
                 and observed <= time.time() < expires, 'confirmation_expired')

    async def check(_journal, target):
        check_expiry()
        _require(target == args.expected_cycle_id, 'cycle_changed')
        current = (await _preview_locked(args, cfg, settings, pair, authorization,
                                         account_scope, config_hashes, include_quote=False))['receipt']
        check_expiry()
        expected = {key: value for key, value in receipt.items() if key not in {'observed_at', 'expires_at'}}
        actual = {key: value for key, value in current.items() if key not in {'observed_at', 'expires_at'}}
        _require(expected == actual, 'confirmation_changed')
    return check


def _read_confirmation(path):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), encoding='utf-8') as file:
        info = os.fstat(file.fileno())
        _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                 and not info.st_mode & 0o077 and info.st_size <= 65536, 'confirmation_file_invalid')
        receipt = json.loads(file.read(65537))
    _require(isinstance(receipt, dict), 'confirmation_invalid')
    return receipt


def _safe_outcome(result, expected_cycle_id):
    _require('active_cycle' in result and result.get('cycle_id') == expected_cycle_id,
             'execution_evidence_missing')
    active = result['active_cycle']
    _require(active is None or isinstance(active, dict)
             and active.get('cycle_id') == expected_cycle_id, 'execution_cycle_mismatch')
    safe = {key: result[key] for key in ('success', 'outcome', 'cycle_id', 'remaining',
            'position_observed_ts', 'quantity_step', 'pending', 'entries_remain_closed',
            'accounting_final', 'net_pnl_usd', 'fees_complete', 'funding_complete')}
    safe['active_cycle'] = None if active is None else {'cycle_id': expected_cycle_id}
    return safe


async def amain(args):
    # Parsing may read existing credentials, but no signer or venue is constructed here.
    files = (args.config, args.env_file, args.v3_config, args.batch_authorization)
    before = [hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in files]
    settings = exit_cli.main_v3.load_settings(args.v3_config)
    cfg = exit_cli.load_config(args.config, args.env_file, symbol=args.symbol, hedge_venue=args.hedge)
    account_scope = exit_cli.validate_exit_scope(args, cfg, settings)
    pair, authorization = exit_cli.main_v3.validate_launch(args, cfg, settings)
    _require(before == [hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in files],
             'configuration_changed')
    hashes = configuration_hashes(args, cfg, settings, pair)
    if args.preview_only:
        return await preview_exit(args, cfg, settings, pair, authorization, account_scope, hashes)
    receipt = _read_confirmation(args.confirmation_json)
    result = await exit_cli.run_live_exit(args, cfg, settings, pair, authorization, account_scope,
        pre_arm_check=confirmation_check(receipt, args, cfg, settings, pair, authorization, account_scope, hashes))
    # Do not surface exception text, venue URLs, paths, raw account data or credentials.
    return _safe_outcome(result, args.expected_cycle_id)


def main():
    try:
        args = parser().parse_args()
        # SDK/native diagnostics must never become part of the worker JSON protocol.
        with open(os.devnull, 'w') as sink, redirect_stdout(sink):
            result = asyncio.run(amain(args))
    except Exception as exc:
        code = str(exc) if isinstance(exc, ControlRefusal) else (
            'account_owner_busy' if isinstance(exc, ProcessGuardError) else
            'preview_timeout' if isinstance(exc, asyncio.TimeoutError) else 'exit_worker_refused')
        print(json.dumps({'success': False, 'outcome': 'refused', 'reason': code}))
        raise SystemExit(2)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    raise SystemExit(0 if 'preview' in result or result.get('success') else 3)


if __name__ == '__main__':
    main()
