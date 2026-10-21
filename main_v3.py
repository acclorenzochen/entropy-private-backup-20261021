#!/usr/bin/env python3
"""Standalone Entropy V3 host.

The default mode is a public-data, read-only observer.  This entry point never
imports the legacy Engine.  ``--live`` is fail-closed unless an explicit batch
authorization, shared account lock, durable journal, and account streams are
all available.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
from dataclasses import asdict, replace
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
import math
from pathlib import Path
import signal
import socket
import sys
import time
import uuid

from entropy_arb.config import (
    ConfigError, HEDGE_VENUES, HL_API_URL, HL_WS_URL, LIGHTER_PROFILES, load_config,
)
from entropy_arb.process_guard import ProcessGuard
from entropy_arb.v3.core import Settings, dec
from entropy_arb.v3.live_host import (
    BatchAuthorization,
    LiveAuthorizationError,
    ReadOnlyObserver,
    StatusWriter,
    canonical_config_hash,
)
from entropy_arb.v3.reference import FastReference, load_checkpoint, save_checkpoint


STRATEGY_VERSION = "entropy-v3.0"


def _validate_public_flags(args) -> None:
    if getattr(args, 'operator_control', False) and (
            not args.live or args.validate_only or args.recovery_only
            or args.preflight_only or args.reopen_entries):
        raise ValueError('--operator-control requires the live owner operating mode')
    if not args.public_preflight_only:
        return
    if (args.live or args.record_only or args.recovery_only or args.preflight_only
            or args.reopen_entries or args.batch_authorization
            or args.max_new_cycles is not None or args.validate_only):
        raise ValueError(
            "--public-preflight-only is mutually exclusive with live/private, "
            "authorization, record-only and validate-only flags")


class LaunchParser(argparse.ArgumentParser):
    def parse_args(self, args=None, namespace=None):
        parsed = super().parse_args(args, namespace)
        try:
            _validate_public_flags(parsed)
        except ValueError as exc:
            self.error(str(exc))
        return parsed


def parser() -> argparse.ArgumentParser:
    p = LaunchParser(description="Standalone Entropy V3 host")
    p.add_argument("--symbol", required=True)
    p.add_argument("--hedge", required=True, choices=HEDGE_VENUES)
    p.add_argument("--config", default="config.yaml",
                   help="connection/venue config; legacy strategy fields are not executed")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--v3-config", default="config.v3.json")
    p.add_argument("--deployment", default="default")
    p.add_argument("--status-json", default="logs/v3/status.json")
    p.add_argument("--observer-jsonl", default="logs/v3/observer.jsonl")
    p.add_argument("--observer-rotate-hours", type=float, default=1.0)
    p.add_argument("--observer-rotate-mib", type=int, default=64)
    p.add_argument("--observer-retain-hours", type=float, default=72.0)
    p.add_argument("--observer-max-total-mib", type=int, default=1024)
    p.add_argument("--journal", default="logs/v3/journal.sqlite3")
    p.add_argument("--reference-checkpoint",
                   default="logs/v3/reference_checkpoint.json")
    p.add_argument("--legacy-reference-sampling", action="store_true",
                   help="sample reference in the main loop for a compatible rollback")
    p.add_argument("--batch-authorization")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--record-only", action="store_true",
                      help="public books and V3 reference only (default)")
    mode.add_argument("--live", action="store_true",
                      help="requires an explicit batch authorization")
    mode.add_argument("--public-preflight-only", action="store_true",
                      help="NBIS public metadata/funding/books only; no signer, account, journal or lock")
    p.add_argument("--validate-only", action="store_true",
                   help="validate configuration without network access")
    p.add_argument("--recovery-only", action="store_true",
                   help="with --live, validate/recover durable state but do not open")
    p.add_argument("--preflight-only", action="store_true",
                   help="with --live, connect real adapters and reconcile without opening")
    p.add_argument("--reopen-entries", action="store_true",
                   help="with --live, explicitly unlock a safety latch after authoritative flat check")
    p.add_argument('--operator-control', action='store_true',
                   help='enable local owner start/stop/close protocol; does not unlock risk latches')
    p.add_argument("--max-new-cycles", type=int,
                   help="optional per-process cap, never enlarges batch authorization")
    return p


def load_settings(path: str | Path, *, provenance: dict | None = None) -> Settings:
    try:
        resolved = Path(path).resolve()
        content = resolved.read_bytes()
        raw = json.loads(content)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read V3 config: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("V3 config must contain one JSON object")
    settings = Settings.from_mapping(raw)
    if provenance is not None:
        provenance.update(path=str(resolved), sha256=hashlib.sha256(content).hexdigest(),
                          evidence="parsed_bytes", explicit_fields=sorted(raw))
    return settings


def pair_identity(args: argparse.Namespace, cfg) -> str:
    return f"{args.deployment}:{cfg.entropy.kind}:{cfg.entropy.symbol}:{cfg.hedge.kind}:{cfg.hedge.symbol}"


def validate_launch(args: argparse.Namespace, cfg, settings: Settings,
                    *, authorization_source: dict | None = None):
    _validate_public_flags(args)
    funding_mode = settings.strategy_mode == "funding_hedge"
    if getattr(args, 'operator_control', False) and settings.strategy_mode != 'basis':
        raise ValueError('--operator-control supports ordinary basis only')
    if getattr(args, 'operator_control', False) and settings.opening_experiment_id:
        raise ValueError('operator_control_unsupported_opening_ab')
    if args.public_preflight_only and not funding_mode:
        raise ValueError("--public-preflight-only requires strategy_mode funding_hedge")
    if funding_mode:
        validate_nbis_policy(cfg, settings, symbol=args.symbol, hedge=args.hedge)
    pair = pair_identity(args, cfg)
    authorization = None
    if args.live:
        if not args.batch_authorization:
            raise LiveAuthorizationError("--live requires --batch-authorization")
        authorization = BatchAuthorization.load(
            args.batch_authorization, expected_pair=pair,
            configured_max_notional=settings.max_notional_usd,
            allow_disabled_loss_budget=funding_mode or settings.strategy_mode == 'basis',
            allow_disabled_cycle_limit=settings.strategy_mode == 'basis' and not settings.opening_experiment_id,
            expected_opening_experiment_id=settings.opening_experiment_id or None,
            provenance=authorization_source)
        if funding_mode:
            validate_nbis_policy(cfg, settings, symbol=args.symbol, hedge=args.hedge,
                                 authorization=authorization)
        if getattr(args, 'operator_control', False):
            from entropy_arb.v3.parameter_config import (
                parameter_limits, parameter_values, validate_parameters,
            )
            effective = replace(settings, max_notional_usd=min(
                settings.max_notional_usd, authorization.max_notional_usd))
            validate_parameters(parameter_values(effective),
                                parameter_limits(effective, authorization, cfg))
    elif args.batch_authorization:
        raise LiveAuthorizationError(
            "batch authorization is accepted only together with --live")
    return pair, authorization


def validate_nbis_policy(cfg, settings, *, symbol: str, hedge: str,
                         authorization=None) -> None:
    """Opt-in launch policy, independent of account state and network I/O."""
    if (symbol != "NBIS" or cfg.symbol != "NBIS"
            or cfg.entropy.symbol != "NBIS" or cfg.hedge.symbol != "NBIS"
            or cfg.entropy.kind != "hl" or cfg.entropy.hl_dex != "io"
            or hedge != "lighter" or cfg.hedge_venue != "lighter"
            or cfg.hedge.kind != "lighter"
            or cfg.hedge.lighter_profile != LIGHTER_PROFILES["lighter"]
            or cfg.hl_api_url != HL_API_URL or cfg.hl_ws_url != HL_WS_URL):
        raise ValueError("NBIS funding_hedge requires Entropy hl io:NBIS and Lighter mainnet NBIS")
    if cfg.entropy.leverage != 7 or cfg.hedge.leverage != 7:
        raise ValueError("NBIS funding_hedge requires configured leverage 7 on both legs")
    for cap in (settings.max_notional_usd, cfg.entropy.cap_usd, cfg.hedge.cap_usd):
        if not Decimal('0') < dec(cap) <= Decimal('200'):
            raise ValueError("NBIS per-leg notional caps must be positive and <=200 USD")
    if Path(cfg.persistence.lock_file).resolve() != Path("logs/entropy-arb.lock").resolve():
        raise ValueError("NBIS must retain the shared logs/entropy-arb.lock")
    if authorization is not None:
        if (type(authorization.max_position_cycles) is not int
                or authorization.max_position_cycles < 1):
            raise LiveAuthorizationError("NBIS authorization max_position_cycles must be positive")
        if not Decimal('0') < authorization.max_notional_usd <= Decimal('200'):
            raise LiveAuthorizationError("NBIS authorization cap must be positive and <=200 USD")
        if (authorization.loss_budget_usd is not None
                and settings.funding_round_loss_limit_usd is not None
                and authorization.loss_budget_usd > settings.funding_round_loss_limit_usd):
            raise LiveAuthorizationError("NBIS loss budget must not exceed funding_round_loss_limit_usd")


async def _fetch_public_book(venue) -> dict:
    """Use the adapters' public REST transports, never their account feeds."""
    observed = time.time()
    if venue.conf.kind == 'hl':
        raw = await venue._info({'type': 'l2Book', 'coin': venue.coin})
        if not isinstance(raw, dict) or raw.get('coin') != 'io:NBIS':
            raise ValueError('Entropy public book identity mismatch')
        levels = raw.get('levels')
        if not isinstance(levels, list) or len(levels) != 2:
            raise ValueError('Entropy public book levels missing')
        bids = [(row['px'], row['sz']) for row in levels[0]]
        asks = [(row['px'], row['sz']) for row in levels[1]]
        source = 'hyperliquid:io:l2Book'
    else:
        raw = await venue._get('/api/v1/orderBookOrders',
                               params={'market_id': venue.market_id, 'limit': 250})
        if not isinstance(raw, dict) or raw.get('code') != 200:
            raise ValueError('Lighter public book request failed')
        # SimpleOrder is per-order depth: aggregate equal prices below.
        bids = [(row['price'], row['remaining_base_amount']) for row in raw['bids']]
        asks = [(row['price'], row['remaining_base_amount']) for row in raw['asks']]
        source = 'lighter:mainnet:orderBookOrders'
    sides = []
    for rows, reverse in ((bids, True), (asks, False)):
        depth = {}
        for raw_price, raw_qty in rows:
            price, qty = dec(raw_price), dec(raw_qty)
            if price <= 0 or qty < 0:
                raise ValueError('invalid public book price/quantity')
            if qty:
                depth[price] = depth.get(price, Decimal('0')) + qty
        sides.append(sorted(depth.items(), reverse=reverse))
    if not sides[0] or not sides[1] or sides[0][0][0] > sides[1][0][0]:
        raise ValueError('empty or crossed public book')
    return {'bids': sides[0], 'asks': sides[1], 'observed_at': observed,
            'source': source, 'timestamp_evidence': 'local_request_start'}


def _public_matched_quote(entropy_levels, hedge_levels, *, step, cap) -> dict:
    """Walk both legs on the common grid, budgeting actual depth prices."""
    e_depth = [[price, int((qty / step).to_integral_value(rounding=ROUND_FLOOR))]
               for price, qty in entropy_levels]
    h_depth = [[price, int((qty / step).to_integral_value(rounding=ROUND_FLOOR))]
               for price, qty in hedge_levels]
    i = j = units = 0
    e_cash = h_cash = Decimal('0')
    while i < len(e_depth) and j < len(h_depth):
        ep, eu = e_depth[i]
        hp, hu = h_depth[j]
        if not eu:
            i += 1
            continue
        if not hu:
            j += 1
            continue
        take = min(eu, hu, int((cap - e_cash) / (ep * step)),
                   int((cap - h_cash) / (hp * step)))
        if take <= 0:
            break
        units += take
        e_cash += ep * step * take
        h_cash += hp * step * take
        e_depth[i][1] -= take
        h_depth[j][1] -= take
    return {'matched_units': units, 'matched_qty': units * step,
            'entropy_notional_usd': e_cash, 'lighter_notional_usd': h_cash,
            'per_leg_cap_usd': cap}


def evaluate_nbis_public_preflight(cfg, settings, entropy, hedge, *,
                                   entropy_book, hedge_book, entropy_funding,
                                   hedge_funding, lighter_details, now: float) -> dict:
    """Deterministic public-only policy. No live readiness is inferred."""
    from entropy_arb.v3.funding_hedge import entry_direction, pair_income_usd_per_hour

    validate_nbis_policy(cfg, settings, symbol='NBIS', hedge='lighter')
    checks = []

    def check(name, ok):
        checks.append({'check': name, 'ok': bool(ok)})

    check('entropy_io_NBIS', entropy.coin == 'io:NBIS')
    check('lighter_mainnet_market_189', hedge.market_id == 189)
    e_step = Decimal('1').scaleb(-entropy.size_decimals)
    h_step = Decimal('1').scaleb(-hedge.size_decimals)
    step = max(e_step, h_step)
    check('common_quantity_step_0.01', step == Decimal('0.01'))
    check('entropy_7x_in_market_range', 7 <= entropy.max_leverage)
    if not isinstance(lighter_details, dict) or lighter_details.get('code') != 200:
        raise ValueError('Lighter public market details request failed')
    details = lighter_details.get('order_book_details')
    if not isinstance(details, list):
        raise ValueError('Lighter public market details missing')
    matches = [row for row in details if isinstance(row, dict)
               and row.get('market_id') == 189 and row.get('symbol') == 'NBIS'
               and row.get('market_type') == 'perp' and row.get('status') == 'active']
    if len(matches) != 1:
        raise ValueError('Lighter active mainnet NBIS 189 details unavailable')
    detail = matches[0]
    check('lighter_size_metadata_consistent',
          detail.get('supported_size_decimals') == hedge.size_decimals)
    # Official SDK update_leverage uses int(10_000 / leverage), in bps.
    imf = dec(detail.get('min_initial_margin_fraction'))
    if imf <= 0 or imf > 10000 or imf != imf.to_integral_value():
        raise ValueError('Lighter minimum initial margin fraction invalid')
    lighter_max_leverage = Decimal('10000') / imf
    check('lighter_7x_in_market_range', int(10000 / 7) >= imf)
    for name, book in (('entropy', entropy_book), ('lighter', hedge_book)):
        age = now - book['observed_at']
        check(f'{name}_public_book_fresh', math.isfinite(age)
              and 0 <= age <= settings.max_book_age_seconds)
    check('public_books_synchronized',
          abs(entropy_book['observed_at'] - hedge_book['observed_at'])
          <= settings.max_book_skew_seconds)
    funding = {}
    for name, snapshot, source in (
            ('entropy', entropy_funding, 'hyperliquid:io'),
            ('lighter', hedge_funding, 'lighter:mainnet')):
        valid = (snapshot is not None and snapshot.source == source
                 and snapshot.is_fresh(now, settings.funding_stale_seconds))
        check(f'{name}_public_funding_valid', valid)
        funding[name] = ({'rate_bps_per_hour': str(snapshot.rate_bps_per_hour),
                          'ts': str(snapshot.ts), 'source': snapshot.source}
                         if snapshot is not None else None)
    cap = min(settings.max_notional_usd, dec(cfg.entropy.cap_usd), dec(cfg.hedge.cap_usd))
    valid_funding = all(row['ok'] for row in checks
                        if row['check'].endswith('_public_funding_valid'))
    e_mid = (entropy_book['bids'][0][0] + entropy_book['asks'][0][0]) / 2
    h_mid = (hedge_book['bids'][0][0] + hedge_book['asks'][0][0]) / 2
    direction = (entry_direction(
        e_mid, h_mid, entropy_funding, hedge_funding, now=now,
        stale_seconds=settings.funding_stale_seconds) if valid_funding else None)
    recommended = ({'SHORT_ENTROPY': 'short_entropy_long_lighter',
                    'LONG_ENTROPY': 'long_entropy_short_lighter'}[direction.name]
                   if direction is not None else None)
    quotes = {}
    for quote_direction, e_side, h_side in (
            ('short_entropy_long_lighter', 'bids', 'asks'),
            ('long_entropy_short_lighter', 'asks', 'bids')):
        quote = _public_matched_quote(entropy_book[e_side], hedge_book[h_side],
                                      step=step, cap=cap)
        check(f'{quote_direction}_matched_qty_within_cap',
              quote['matched_qty'] >= max(dec(entropy.min_base), dec(hedge.min_base), step)
              and dec(entropy.min_quote) <= quote['entropy_notional_usd'] <= cap
              and dec(hedge.min_quote) <= quote['lighter_notional_usd'] <= cap)
        qty = quote['matched_qty']
        sign = Decimal('-1') if e_side == 'bids' else Decimal('1')
        quote['estimated_net_funding_usdc_per_hour'] = (
            pair_income_usd_per_hour(
                sign * qty, -sign * qty, quote['entropy_notional_usd'] / qty,
                quote['lighter_notional_usd'] / qty, entropy_funding, hedge_funding,
                now=now, stale_seconds=settings.funding_stale_seconds)
            if valid_funding and qty > 0 else None)
        quotes[quote_direction] = quote
    return {'ok': all(row['ok'] for row in checks), 'checks': checks,
            'common_quantity_step': step,
            'market_leverage_limits': {'entropy_max': entropy.max_leverage,
                                       'lighter_max': lighter_max_leverage,
                                       'lighter_min_initial_margin_fraction_bps': imf},
            'funding': funding, 'books': {'entropy': entropy_book, 'lighter': hedge_book},
            'matched_quotes': quotes, 'funding_direction': recommended,
            'funding_direction_basis': 'public_mid_price_estimate',
            'estimated_net_funding_usdc_per_hour': (
                quotes[recommended]['estimated_net_funding_usdc_per_hour']
                if recommended is not None else None),
            'funding_income_evidence': 'estimated_not_settled_excludes_trading_costs'}


async def _public_preflight_stage(stage, requests):
    """Retry only timed-out public reads, refreshing all peers in the stage."""
    for attempt in range(1, 4):
        values = await asyncio.gather(*(fetch() for _, fetch in requests),
                                      return_exceptions=True)
        failures = [(name, value) for (name, _), value in zip(requests, values)
                    if isinstance(value, BaseException)]
        if not failures:
            return values
        for _, error in failures:
            if not isinstance(error, Exception):
                raise error
        detail = '; '.join(f'{name}: {type(error).__name__}: {error}'
                           for name, error in failures)
        message = f'public stage={stage}, attempt={attempt}/3; {detail}'
        if all(isinstance(error, TimeoutError) for _, error in failures):
            if attempt < 3:
                print(f'{message}; retrying public reads in 1s', file=sys.stderr,
                      flush=True)
                await asyncio.sleep(1)
                continue
            raise TimeoutError(message) from failures[0][1]
        raise RuntimeError(message) from failures[0][1]


async def run_public_preflight_only(args, cfg, settings, pair, status) -> int:
    import aiohttp

    result = {'kind': 'nbis_public_preflight', 'mode': 'public-preflight-only',
              'pair_identity': pair, 'public_only': True, 'live_ready': False,
              'account_verified': False, 'settlements_verified': False,
              'accounting_delay_verified': False}
    try:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            entropy, hedge = _make_venues(cfg, session)
            await _public_preflight_stage('market_metadata', [
                ('entropy.perpDexs/meta', entropy.load_market),
                ('lighter.orderBooks', hedge.load_market)])
            if entropy.coin != 'io:NBIS' or hedge.market_id != 189:
                raise ValueError('public market identity must be io:NBIS / Lighter mainnet 189')
            values = await _public_preflight_stage('funding_and_limits', [
                ('entropy.metaAndAssetCtxs', entropy.fetch_funding),
                ('lighter.funding-rates', hedge.fetch_funding),
                ('lighter.orderBookDetails', lambda: hedge._get(
                    '/api/v1/orderBookDetails', params={'market_id': 189}))])
            # Capture books last so slow funding/metadata cannot age them.
            books = await _public_preflight_stage('books', [
                ('entropy.l2Book', lambda: _fetch_public_book(entropy)),
                ('lighter.orderBookOrders', lambda: _fetch_public_book(hedge))])
            result.update(evaluate_nbis_public_preflight(
                cfg, settings, entropy, hedge, entropy_funding=values[0],
                hedge_funding=values[1], entropy_book=books[0], hedge_book=books[1],
                lighter_details=values[2], now=time.time()))
    except Exception as exc:
        result.update(ok=False, error=f'{type(exc).__name__}: {exc}')
    status.write('PUBLIC_PREFLIGHT_OK' if result['ok'] else 'PUBLIC_PREFLIGHT_REFUSED',
                 'public_checks_only_live_unverified', **result)
    print(json.dumps(result, ensure_ascii=False, default=str, allow_nan=False))
    return 0 if result['ok'] else 2


async def publish_effective_manifest(args, settings, pair, authorization, status,
                                     config_source, authorization_source,
                                     *, journal=None) -> None:
    """Snapshot on the SQLite owner; only frozen JSON reaches the file worker."""
    modules = {}
    for name, module in sorted(tuple(sys.modules.items())):
        if name == __name__ or name.startswith('entropy_arb.'):
            filename = getattr(module, '__file__', None)
            if filename:
                path = Path(filename).resolve()
                try:
                    digest = hashlib.sha256(path.read_bytes()).hexdigest()
                except OSError:
                    digest = None
                modules[name] = {'path': str(path), 'disk_sha256': digest}
    explicit = config_source.get('explicit_fields', [])
    policies = {}
    for purpose in ('open', 'close'):
        rooms, cap = settings.first_leg_execution_policy(purpose)
        sources = {}
        for suffix in ('rooms_bps', 'hard_cap_bps'):
            field = f'{purpose}_first_{suffix}'
            if getattr(settings, field) is None:
                field = f'first_{suffix}'
            sources[suffix] = {'field': field,
                               'origin': ('unknown' if not config_source else
                                          'v3_config' if field in explicit else 'Settings.default')}
        policies[purpose] = {'rooms_bps': rooms, 'hard_cap_bps': cap, 'sources': sources}
    policies['hedge'] = {}
    for purpose in ('open', 'close'):
        rooms, cap = settings.hedge_execution_policy(purpose)
        sources = {}
        for suffix in ('rooms_bps', 'hard_cap_bps'):
            field = f'{purpose}_hedge_{suffix}'
            if getattr(settings, field) is None:
                field = f'hedge_{suffix}'
            sources[suffix] = {'field': field,
                               'origin': ('unknown' if not config_source else
                                          'v3_config' if field in explicit else 'Settings.default')}
        policies['hedge'][purpose] = {'rooms_bps': rooms, 'hard_cap_bps': cap, 'sources': sources}
    schema = None
    if journal is not None:
        schema = dict(journal.db.execute(
            "SELECT key, value FROM metadata WHERE key IN ('version', 'economics_schema_version')"
        ).fetchall())
    effective_settings = (replace(settings, max_notional_usd=min(
        settings.max_notional_usd, authorization.max_notional_usd))
        if authorization is not None else settings)
    manifest = {
        **status.base, 'kind': 'v3_effective_manifest', 'manifest_version': 1,
        'manifest_scope': ('startup_config_snapshot' if getattr(args, 'operator_control', False)
                           else 'effective_configuration'),
        'active_parameter_evidence': ('owner.state.parameters.current / status.active_parameter_manifest'
                                      if getattr(args, 'operator_control', False) else None),
        'pair_identity': pair, 'deployment': args.deployment,
        'settings': asdict(effective_settings), 'configured_settings': asdict(settings),
        'effective_config_hash': canonical_config_hash(effective_settings, pair_identity=pair),
        'execution_policies': policies,
        'notional_cap_source': ('min(v3_settings, batch_authorization)'
                                if authorization is not None else 'v3_settings'),
        'v3_config_source': config_source,
        'authorization': asdict(authorization) if authorization is not None else None,
        'authorization_source': authorization_source or None,
        'session_max_new_cycles': args.max_new_cycles,
        'reference_sampler_enabled': not args.legacy_reference_sampling,
        'operator_control_requested': bool(getattr(args, 'operator_control', False)),
        'journal_schema': schema,
        'loaded_modules': modules,
        'source_hash_evidence': 'disk_files_at_manifest_time_not_loaded_bytecode_attestation',
    }
    manifest['build_hash'] = hashlib.sha256(json.dumps(
        modules, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    path = status.path.with_name(f'{status.path.stem}.effective_manifest.json')
    status.base['effective_manifest_path'] = str(path.resolve())
    status.base['build_hash'] = manifest['build_hash']
    # This happens before the runtime starts, never during an E -> H handoff.
    writer = StatusWriter(path, strategy_version=STRATEGY_VERSION,
                          config_hash=status.base['config_hash'], mode=status.base['mode'],
                          background=True)
    writer.write('STARTUP', 'effective_configuration', **manifest)
    drained = await writer.close()
    status.base['effective_manifest_error'] = (
        writer.last_error if drained else 'writer_shutdown_timeout')


def _make_venues(cfg, session):
    from entropy_arb.venue_hl import HLVenue
    from entropy_arb.venue_lighter import LighterVenue

    def one(conf):
        if conf.kind == "hl":
            return HLVenue(conf, cfg.hl_api_url, cfg.hl_ws_url, session,
                           cfg.settle_timeout_sec)
        if conf.kind == "lighter":
            return LighterVenue(conf, session, cfg.settle_timeout_sec)
        raise RuntimeError(f"unsupported V3 venue kind: {conf.kind}")
    return one(cfg.entropy), one(cfg.hedge)


async def run_record_only(args, cfg, settings, pair: str, status: StatusWriter,
                          *, config_source=None) -> None:
    import aiohttp
    from entropy_arb.v3.runtime import (
        RuntimeHalt, RuntimeVenues, instrument_from_venues)
    from entropy_arb.v3.market_observer import (
        BufferedJSONLObserver, MarketSampler, ObserverFileWriter)

    observer_session_id = uuid.uuid4().hex
    observer = BufferedJSONLObserver(
        args.observer_jsonl,
        {'kind': 'v3_observer_header', 'schema_version': 1,
         'pair_id': pair, 'session_id': observer_session_id,
         'config_hash': status.base['config_hash'],
         'source_version': STRATEGY_VERSION,
         'sampling': {'changed_min_interval_seconds': 0.1,
                      'unchanged_interval_seconds': 1.0},
         'storage': {'rotate_hours': args.observer_rotate_hours,
                     'rotate_mib': args.observer_rotate_mib,
                     'retain_hours': args.observer_retain_hours,
                     'max_total_mib': args.observer_max_total_mib}},
        rotate_seconds=args.observer_rotate_hours * 3600,
        max_file_bytes=args.observer_rotate_mib * 1024 * 1024,
        retain_seconds=args.observer_retain_hours * 3600,
        max_total_bytes=args.observer_max_total_mib * 1024 * 1024)
    reference = FastReference(
        pair, settings, load_checkpoint(args.reference_checkpoint),
        started_at=time.time())
    shadows = {
        half_life: FastReference(
            pair, replace(settings, reference_half_life_seconds=half_life),
            started_at=time.time())
        for half_life in (30.0, 120.0)
    }
    stop = asyncio.Event()
    changed = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    connector = aiohttp.TCPConnector(
        family=socket.AF_INET, keepalive_timeout=75.0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        entropy, hedge = _make_venues(cfg, session)
        await publish_effective_manifest(
            args, settings, pair, None, status, config_source or {}, {})
        await asyncio.gather(entropy.load_market(), hedge.load_market())
        tasks = entropy.start_tasks(stop, changed.set, live=False)
        tasks += hedge.start_tasks(stop, changed.set, live=False)
        sampler = None
        last_invalid_bucket = None
        observer.start()
        file_writer = ObserverFileWriter({'checkpoint': 1.0})
        file_writer.start()
        try:
            status.write("COLLECTING_REFERENCE", "record_only_public_books")
            while not stop.is_set():
                try:
                    await asyncio.wait_for(changed.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                changed.clear()
                now = time.time()
                try:
                    if sampler is None:
                        sampler = MarketSampler(
                            pair, settings, RuntimeVenues(entropy, hedge),
                            instrument_from_venues(pair, entropy, hedge),
                            status.base['config_hash'], observer_session_id)
                    snapshot = sampler.sample(now=now)
                except (RuntimeHalt, ValueError) as exc:
                    snapshot = None
                    market_valid = False
                    if int(now) != last_invalid_bucket:
                        observer.publish({
                            'kind': 'v3_market_snapshot', 'schema_version': 1,
                            'pair_id': pair,
                            'session_id': observer_session_id,
                            'capture_wall_ts': now, 'capture_mono_ns': time.monotonic_ns(),
                            'valid': False, 'quality_reason': str(exc),
                            'reference_bps': None, 'reference_usable': False,
                            'reference_source': 'invalid_market',
                            'entropy': None, 'lighter': None, 'quotes': [],
                        })
                        last_invalid_bucket = int(now)
                else:
                    market_valid = sampler.market_valid
                if snapshot is not None:
                    reference_before = reference.view(now)
                    snapshot['reference_bps'] = (
                        str(reference_before.center_bps)
                        if market_valid and reference_before.usable else None)
                    snapshot['reference_usable'] = market_valid and reference_before.usable
                    snapshot['reference_source'] = (
                        reference_before.source if market_valid else 'invalid_market')
                    snapshot['reference_live_samples'] = reference_before.live_samples
                    snapshot['reference_live_span_seconds'] = (
                        reference_before.live_span_seconds)
                    observer.publish(snapshot)
                if market_valid:
                    basis = (dec(entropy.book.mid()) / dec(hedge.book.mid()) - 1) * dec(10000)
                    view = reference.observe(now, basis, valid=True)
                    for shadow in shadows.values():
                        shadow.observe(now, basis, valid=True)
                    file_writer.publish('checkpoint', save_checkpoint,
                                        args.reference_checkpoint, reference.checkpoint())
                    observer.publish({
                        "kind": "v3_reference", "pair_identity": pair,
                        "session_id": observer_session_id,
                        "basis_bps": str(basis), "reference_bps": (
                            str(view.center_bps) if view.center_bps is not None else None),
                        "usable": view.usable, "live_samples": view.live_samples,
                        "live_span_seconds": view.live_span_seconds,
                        "shadow_reference_bps": {
                            str(half_life): (str(shadow.view(now).center_bps)
                                             if shadow.view(now).center_bps is not None else None)
                            for half_life, shadow in shadows.items()},
                    })
                    status.write("READY_OBSERVING" if view.usable else "COLLECTING_REFERENCE",
                                 view.source, reference_usable=view.usable,
                                 live_samples=view.live_samples,
                                 checkpoint_writer_errors=file_writer.errors,
                                 checkpoint_writer_coalesced=file_writer.coalesced,
                                 checkpoint_writer_last_error=file_writer.last_error,
                                 observer_dropped_events=observer.dropped_events,
                                 observer_last_error=observer.last_error)
                else:
                    reference.observe(now, dec(0), valid=False)
                    for shadow in shadows.values():
                        shadow.observe(now, dec(0), valid=False)
                    status.write("WAITING_FOR_BOOKS", "books_not_fresh_or_synchronized",
                                 checkpoint_writer_errors=file_writer.errors,
                                 checkpoint_writer_coalesced=file_writer.coalesced,
                                 checkpoint_writer_last_error=file_writer.last_error,
                                 observer_dropped_events=observer.dropped_events,
                                 observer_last_error=observer.last_error)
                if args.validate_only:
                    break
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(entropy.close(), hedge.close(), return_exceptions=True)
            try:
                await file_writer.close()
            finally:
                await observer.close()
                final_status = status.current_snapshot
                if final_status is not None:
                    state = final_status.pop('current_state')
                    reason = final_status.pop('reason')
                    final_status.update(
                        checkpoint_writer_errors=file_writer.errors,
                        checkpoint_writer_coalesced=file_writer.coalesced,
                        checkpoint_writer_last_error=file_writer.last_error,
                        observer_dropped_events=observer.dropped_events,
                        observer_last_error=observer.last_error)
                    status.write(state, reason, **final_status)


async def run_live_recovery_only(args, cfg, settings, pair: str,
                                 authorization: BatchAuthorization,
                                 status: StatusWriter, *, config_source=None,
                                 authorization_source=None) -> None:
    """Acquire ownership and validate durable batch state without opening risk.

    The real bridge lives in ``entropy_arb.v3.live_host`` and is integration
    tested.  Autonomous order entry remains deliberately disabled here until
    the project-level fault matrix and an operator-approved live batch are
    complete.  This is a safe production recovery/preflight mode, not paper
    trading disguised as live trading.
    """
    from entropy_arb.v3.journal import Journal

    identity = f"{pair}:account"
    with ProcessGuard(cfg.persistence.lock_file):
        journal = Journal(args.journal, identity=identity)
        try:
            await publish_effective_manifest(
                args, settings, pair, authorization, status, config_source or {},
                authorization_source or {}, journal=journal)
            journal.ensure_batch(
                authorization.batch_id,
                max_position_cycles=authorization.max_position_cycles,
                loss_budget_usd=authorization.loss_budget_usd)
            state = journal.batch_state(authorization.batch_id)
            status.write("LIVE_RECOVERY_ONLY", "new_entries_not_enabled",
                         batch_id=authorization.batch_id, batch_state=state)
        finally:
            journal.close()


async def run_live(args, cfg, settings, pair: str,
                   authorization: BatchAuthorization,
                   status: StatusWriter, *, preflight_only: bool = False,
                   reopen_entries: bool = False, config_source=None,
                   authorization_source=None) -> None:
    import aiohttp
    from entropy_arb.v3.journal import Journal
    from entropy_arb.v3.runtime import LiveRuntime, RuntimeVenues

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    identity = f"{pair}:account"
    with ProcessGuard(cfg.persistence.lock_file):
        journal = Journal(args.journal, identity=identity)
        try:
            connector = aiohttp.TCPConnector(
                family=socket.AF_INET, keepalive_timeout=75.0, ttl_dns_cache=300)
            async with aiohttp.ClientSession(connector=connector) as session:
                entropy, hedge = _make_venues(cfg, session)
                runtime = LiveRuntime(
                    settings=settings, legacy_config=cfg,
                    authorization=authorization, pair_id=pair,
                    deployment=args.deployment,
                    venues=RuntimeVenues(entropy, hedge), journal=journal,
                    status=status, observer=ReadOnlyObserver(args.observer_jsonl),
                    stop=stop, session_max_cycles=args.max_new_cycles,
                    reference_sampler_enabled=not args.legacy_reference_sampling,
                    reference_checkpoint_path=args.reference_checkpoint,
                    operator_socket_path=(Path(args.journal).resolve().parent / 'control' / 'owner.sock'
                        if getattr(args, 'operator_control', False) else None))
                await publish_effective_manifest(
                    args, settings, pair, authorization, status, config_source or {},
                    authorization_source or {}, journal=journal)
                if reopen_entries:
                    result = await runtime.reopen_entries()
                    print(json.dumps(result, ensure_ascii=False, default=str))
                elif preflight_only:
                    result = await runtime.preflight()
                    print(json.dumps(result, ensure_ascii=False, default=str))
                else:
                    await runtime.run()
        finally:
            journal.close()


async def amain(args: argparse.Namespace) -> int:
    _validate_public_flags(args)
    if args.max_new_cycles is not None and args.max_new_cycles < 1:
        raise ValueError("--max-new-cycles must be positive")
    if args.max_new_cycles is not None and not args.live:
        raise ValueError("--max-new-cycles requires --live")
    config_source, authorization_source = {}, {}
    settings = load_settings(args.v3_config, provenance=config_source)
    if settings.strategy_mode == 'funding_hedge':
        if args.deployment == 'default':
            args.deployment = 'nbis-funding'
        status_name = ('public-preflight-status.json' if args.public_preflight_only else
                       'status.json' if args.live else 'record-status.json')
        for field, old, new in (
                ('status_json', 'logs/v3/status.json', f'logs/v3/nbis/{status_name}'),
                ('observer_jsonl', 'logs/v3/observer.jsonl', 'logs/v3/nbis/observer.jsonl'),
                ('journal', 'logs/v3/journal.sqlite3', 'logs/v3/nbis/journal.sqlite3'),
                ('reference_checkpoint', 'logs/v3/reference_checkpoint.json',
                 'logs/v3/nbis/reference_checkpoint.json')):
            if getattr(args, field) == old:
                setattr(args, field, new)
    elif not args.live:
        for field, old, new in (
                ('status_json', 'logs/v3/status.json', 'logs/v3/record-status.json'),
                ('observer_jsonl', 'logs/v3/observer.jsonl', 'logs/v3/record-observer.jsonl'),
                ('reference_checkpoint', 'logs/v3/reference_checkpoint.json',
                 'logs/v3/record-reference_checkpoint.json')):
            if getattr(args, field) == old:
                setattr(args, field, new)
    cfg = load_config(args.config, args.env_file,
                      symbol=args.symbol, hedge_venue=args.hedge)
    pair, authorization = validate_launch(
        args, cfg, settings, authorization_source=authorization_source)
    if args.public_preflight_only:
        status_path = Path(args.status_json).resolve()
        protected = (args.config, args.v3_config, args.env_file, args.journal,
                     args.observer_jsonl, args.reference_checkpoint,
                     cfg.persistence.lock_file, cfg.persistence.sqlite_path,
                     cfg.dashboard_status_json, cfg.dashboard_pnl_state_json,
                     cfg.log_file, cfg.trades_csv, cfg.recorder_csv, cfg.recorder_seconds_csv,
                     'logs/v3/nbis/status.json', 'logs/v3/nbis/record-status.json')
        if (status_path in {Path(path).resolve() for path in protected if path}
                or (status_path.is_relative_to(Path('logs').resolve())
                    and not status_path.is_relative_to(Path('logs/v3/nbis').resolve()))):
            raise ValueError('public preflight status must use an independent NBIS output path')
    mode = ('public-preflight-only' if args.public_preflight_only else
            'live' if args.live else 'record-only')
    status = StatusWriter(
        args.status_json, strategy_version=STRATEGY_VERSION,
        config_hash=canonical_config_hash(settings, pair_identity=pair), mode=mode,
        background=True)
    status.base['configured_leverage'] = {
        'entropy': getattr(cfg.entropy, 'leverage', None),
        'hedge': getattr(cfg.hedge, 'leverage', None)}
    try:
        return await _run_configured(
            args, cfg, settings, pair, authorization, status,
            config_source, authorization_source)
    finally:
        drained = await status.close()
        if not drained or status.last_error:
            print('V3 status publication incomplete: ' +
                  (status.last_error or 'writer_shutdown_timeout'), file=sys.stderr)
            if args.public_preflight_only:
                raise RuntimeError('public preflight status publication failed')


async def _run_configured(args, cfg, settings, pair, authorization, status,
                          config_source, authorization_source) -> int:
    mode = status.base['mode']
    if args.public_preflight_only:
        return await run_public_preflight_only(args, cfg, settings, pair, status)
    if args.validate_only:
        await publish_effective_manifest(
            args, settings, pair, authorization, status, config_source, authorization_source)
        status.write("VALIDATED", "configuration_only", pair_identity=pair)
        print(json.dumps({"ok": True, "mode": mode, "pair_identity": pair},
                         ensure_ascii=False))
        return 0
    if args.live:
        if sum((args.recovery_only, args.preflight_only, args.reopen_entries)) > 1:
            raise LiveAuthorizationError(
                "recovery, preflight and reopen modes are mutually exclusive")
        if args.recovery_only:
            await run_live_recovery_only(
                args, cfg, settings, pair, authorization, status,
                config_source=config_source, authorization_source=authorization_source)
            print("V3 live recovery preflight complete; new entries remain disabled")
        else:
            await run_live(args, cfg, settings, pair, authorization, status,
                           preflight_only=args.preflight_only,
                           reopen_entries=args.reopen_entries, config_source=config_source,
                           authorization_source=authorization_source)
        return 0
    if args.recovery_only or args.preflight_only or args.reopen_entries:
        raise LiveAuthorizationError(
            "--recovery-only/--preflight-only/--reopen-entries require --live")
    await run_record_only(args, cfg, settings, pair, status, config_source=config_source)
    return 0


def main() -> None:
    args = parser().parse_args()
    try:
        raise SystemExit(asyncio.run(amain(args)))
    except (ConfigError, ValueError, LiveAuthorizationError, RuntimeError) as exc:
        print(f"V3 startup refused: {exc}", file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
