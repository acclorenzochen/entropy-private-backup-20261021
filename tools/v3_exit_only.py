#!/usr/bin/env python3
"""Operator-run, single-owner ordinary V3 exit; never admits new exposure."""
from __future__ import annotations

import asyncio
import contextlib
import json
import math
from pathlib import Path
import signal
import socket
import sys
import time

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main_v3
from entropy_arb.config import ConfigError, load_config
from entropy_arb.process_guard import ProcessGuard
from entropy_arb.v3.journal import Journal
from entropy_arb.v3.live_host import LiveAuthorizationError, ReadOnlyObserver, StatusWriter, canonical_config_hash
from entropy_arb.v3.runtime import LiveRuntime, RuntimeHalt, RuntimeVenues


def parser():
    p = main_v3.parser()
    p.description = 'Exit one existing ordinary V3 cycle under the shared account lock'
    p.set_defaults(status_json='logs/v3/manual-exit-status.json')
    p.add_argument('--expected-cycle-id', required=True)
    p.add_argument('--expected-hl-account', required=True,
                   help='explicit public Hyperliquid query account address')
    p.add_argument('--expected-hedge-account-index', required=True, type=int)
    p.add_argument('--timeout-seconds', type=float, default=120.0)
    p.add_argument('--accounting-wait-seconds', type=float, default=10.0)
    return p


def validate_exit_scope(args, cfg, settings):
    if not args.live or any((args.record_only, args.public_preflight_only,
                            args.validate_only, args.recovery_only,
                            args.preflight_only, args.reopen_entries,
                            args.max_new_cycles is not None)):
        raise ValueError('exit-only requires --live and no other operating mode')
    if settings.strategy_mode != 'basis' or getattr(settings, 'opening_experiment_id', ''):
        raise ValueError('exit-only supports ordinary basis V3 only')
    if cfg.entropy.kind != 'hl' or cfg.hedge.kind != 'lighter':
        raise ValueError('exit-only requires Hyperliquid and Lighter')
    if (not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0
            or not math.isfinite(args.accounting_wait_seconds) or args.accounting_wait_seconds < 0):
        raise ValueError('timeout must be positive; accounting wait must be nonnegative')
    address = args.expected_hl_account.lower()
    if (len(address) != 42 or not address.startswith('0x')
            or any(c not in '0123456789abcdef' for c in address[2:])):
        raise ValueError('expected Hyperliquid account must be an explicit public address')
    configured = cfg.entropy.hl_creds.account_address
    if not configured or configured.lower() != address:
        raise ValueError('expected Hyperliquid query account differs from configured account')
    if (args.expected_hedge_account_index < 0
            or cfg.hedge.lighter_creds.account_index != args.expected_hedge_account_index):
        raise ValueError('expected Lighter account differs from configured account')
    if not args.expected_cycle_id.strip():
        raise ValueError('expected cycle ID is required')
    return json.dumps({'hl_query_account': address,
                       'lighter_account_index': args.expected_hedge_account_index,
                       'lighter_profile': cfg.hedge.lighter_profile.name}, sort_keys=True)


def _validate_journal_scope(journal, authorization, expected_cycle_id=None):
    active = journal.active_live_cycle()
    if journal.db.execute("SELECT COUNT(*) FROM live_cycles WHERE phase!='DONE'").fetchone()[0] > 1:
        raise ValueError('multiple active cycles require reconciliation')
    if active and active['batch_id'] != authorization.batch_id:
        raise ValueError('active cycle differs from authorized batch')
    if active and expected_cycle_id and active['cycle_id'] != expected_cycle_id:
        raise ValueError('active cycle differs from expected cycle ID')
    target = expected_cycle_id or (active['cycle_id'] if active else None)
    if target:
        row = journal.db.execute('SELECT * FROM live_cycles WHERE cycle_id=?', (target,)).fetchone()
        if row is None or row['batch_id'] != authorization.batch_id:
            raise ValueError('expected cycle is absent or belongs to another batch')
        if journal.db.execute('SELECT 1 FROM funding_cycles WHERE cycle_id=?', (target,)).fetchone():
            raise ValueError('funding cycle requires its dedicated owner')
    if any(order['cycle_id'] != target for order in journal.pending()):
        raise ValueError('pending order belongs to another cycle')
    batch = journal.batch_state(authorization.batch_id)
    if (batch['max_position_cycles'] != authorization.max_position_cycles
            or batch['loss_budget_usd'] != authorization.loss_budget_usd):
        raise ValueError('existing batch authorization differs; no reset is permitted')
    return target


def arm_manual_exit(journal, authorization, *, expected_cycle_id=None, account_scope=None,
                    clock_session_id=None):
    """Atomically freeze entry and exit intent without fabricating market analytics."""
    target = _validate_journal_scope(journal, authorization, expected_cycle_id)
    active = journal.active_live_cycle()
    if target and account_scope is not None:
        prior = journal.db.execute('SELECT value FROM metadata WHERE key=?',
                                   (f'manual_exit_account:{target}',)).fetchone()
        if prior and prior[0] != account_scope:
            raise ValueError('manual exit account binding changed across restart')
    if not active:
        return target
    now, mono = time.time(), time.monotonic_ns()
    with journal.transaction():
        if account_scope is not None:
            key = f'manual_exit_account:{target}'
            prior = journal.db.execute('SELECT value FROM metadata WHERE key=?', (key,)).fetchone()
            if prior and prior[0] != account_scope:
                raise ValueError('manual exit account binding changed across restart')
            journal.db.execute('INSERT OR IGNORE INTO metadata VALUES (?,?)', (key, account_scope))
        journal._close_entries(authorization.batch_id, 'MANUAL_STOP', cycle_id=target)
        journal.db.execute("""UPDATE live_cycles SET phase='BASIS_EXIT',
            basis_exit_requested_ts=COALESCE(basis_exit_requested_ts,?),updated_ts=?
            WHERE cycle_id=? AND phase!='DONE'""", (now, now, target))
        journal.db.execute('INSERT OR IGNORE INTO metadata VALUES (?,?)',
            (f'manual_exit:{target}', json.dumps({'reason': 'operator_manual_exit', 'requested_ts': now})))
        journal.db.execute("""UPDATE cycle_economics SET
            exit_trigger_ts=COALESCE(exit_trigger_ts,?),
            exit_trigger_reason=COALESCE(exit_trigger_reason,'operator_manual_exit'),
            exit_trigger_mono_ns=COALESCE(exit_trigger_mono_ns,?),
            exit_trigger_clock_session_id=COALESCE(exit_trigger_clock_session_id,?),
            updated_ts=? WHERE cycle_id=?""", (now, mono, clock_session_id, now, target))
    return target


def _outcome(runtime, target, *, outcome, reason):
    evidence = runtime._last_position_evidence
    active = runtime.journal.active_live_cycle()
    pending = [dict(order_ref=row['order_ref'], venue=row['venue'], state=row['state'],
                    reduce_only=bool(row['reduce_only']), filled_units=row['filled_units'])
               for row in runtime.journal.pending()]
    current_proof = bool(evidence and evidence.fully_authoritative
                         and not any(row['state'] == 'SUBMITTING' for row in pending)
                         and (active is None or runtime._position_covers_orders(active, evidence)))
    flat = bool(current_proof
                and (evidence.entropy_units, evidence.hedge_units) == (0, 0)
                and active is None and not pending)
    row = (runtime.journal.db.execute('SELECT * FROM cycles WHERE cycle_id=?', (target,)).fetchone()
           if target else None)
    fees = runtime.journal.fee_state(target) if target else None
    economics = (runtime.journal.db.execute('SELECT * FROM cycle_economics WHERE cycle_id=?',
                                          (target,)).fetchone() if target else None)
    final = bool(row and row['accounting_final'] and fees['fees_complete'] and economics
                 and economics['entropy_funding_usd'] is not None
                 and economics['hedge_funding_usd'] is not None)
    return {'success': flat, 'outcome': outcome, 'reason': reason, 'cycle_id': target,
            'remaining': {'entropy_units': evidence.entropy_units if evidence else None,
                          'hedge_units': evidence.hedge_units if evidence else None,
                          'authoritative': current_proof},
            'position_observed_ts': evidence.observed_ts if evidence else None,
            'quantity_step': str(runtime.instrument.quantity_step) if runtime.instrument else None,
            'active_cycle': active, 'pending': pending, 'entries_remain_closed':
            runtime.journal.batch_state(runtime.authorization.batch_id)['closed_for_entries'],
            'accounting_final': final, 'net_pnl_usd': row['pnl_usd'] if final else None,
            'fees_complete': fees['fees_complete'] if fees else None,
            'funding_complete': bool(economics and economics['entropy_funding_usd'] is not None
                                     and economics['hedge_funding_usd'] is not None)}


async def drive_exit_only(runtime, *, timeout_seconds=120.0, accounting_wait_seconds=10.0,
                          expected_cycle_id=None):
    """Drive an already initialized runtime through existing reduce-only exit waves."""
    if runtime.settings.strategy_mode != 'basis' or getattr(runtime.settings, 'opening_experiment_id', ''):
        raise ValueError('exit-only supports ordinary basis V3 only')
    if (not math.isfinite(timeout_seconds) or timeout_seconds <= 0
            or not math.isfinite(accounting_wait_seconds) or accounting_wait_seconds < 0):
        raise ValueError('timeout must be positive; accounting wait must be nonnegative')
    target = arm_manual_exit(runtime.journal, runtime.authorization,
                             expected_cycle_id=expected_cycle_id,
                             clock_session_id=runtime.clock_session_id)
    runtime.executor.entries_halted = True
    reason = 'position_reconciliation_required'

    async def close_loop():
        nonlocal reason
        while not runtime.stop.is_set():
            if runtime.journal.pending():
                try:
                    await runtime._resolve_pending()  # Original IDs, never resubmit UNKNOWN.
                except Exception as exc:
                    reason = f'order_recovery_{type(exc).__name__}'
            await runtime._audit_positions()
            active = runtime.journal.active_live_cycle()
            evidence = runtime._last_position_evidence
            if active is None:
                if not evidence or not evidence.fully_authoritative:
                    reason = 'authoritative_positions_unavailable'
                elif evidence.entropy_units or evidence.hedge_units:
                    raise RuntimeHalt('positions without an active V3 cycle require reconciliation')
                elif not runtime.journal.pending():
                    return
            elif runtime._basis_events() is None:
                reason = 'external_orders_or_account_stream_not_ready'
            elif evidence is not None and evidence.fully_authoritative:
                pending_venues = {r['venue'] for r in runtime.journal.pending()}
                known = runtime._known_positions(active)
                actual = (evidence.entropy_units, evidence.hedge_units)
                if any(key not in pending_venues and known[i] != actual[i]
                       for i, key in enumerate(('entropy', 'hedge'))):
                    raise RuntimeHalt('authoritative positions differ from canonical cycle fills')
                reason = 'remaining_position_or_unknown_order'
                await runtime._basis_exit(active)
            else:
                reason = 'authoritative_positions_unavailable'
            runtime._flush_telemetry()
            runtime.changed.clear()
            try:
                await asyncio.wait_for(runtime.changed.wait(), timeout=.25)
            except asyncio.TimeoutError:
                pass

    try:
        await asyncio.wait_for(close_loop(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        result = _outcome(runtime, target, outcome='incomplete', reason=reason)
    except RuntimeHalt as exc:
        result = _outcome(runtime, target, outcome='refused', reason=str(exc))
    else:
        result = _outcome(runtime, target, outcome='stopped' if runtime.stop.is_set() else 'flat',
                          reason='operator_signal' if runtime.stop.is_set() else 'authoritative_terminal_flat')
    if result['success'] and target and accounting_wait_seconds > 0 and not runtime.stop.is_set():
        try:
            await asyncio.wait_for(runtime._reconcile_cycle_accounting(target), timeout=accounting_wait_seconds)
        except Exception:
            pass  # Durable evidence remains pending; never substitute zero fees or funding.
        result = _outcome(runtime, target, outcome='flat', reason='authoritative_terminal_flat')
    runtime.status.write('MANUAL_EXIT_FLAT' if result['success'] else 'MANUAL_EXIT_INCOMPLETE',
                         result['reason'], **{key: value for key, value in result.items() if key != 'reason'})
    runtime.observer.publish({'kind': 'v3_manual_exit_result', **result})
    return result


async def run_live_exit(args, cfg, settings, pair, authorization, account_scope, *,
                        pre_arm_check=None):
    """Ownership precedes Journal, adapter construction, signer preparation and HTTP."""
    import aiohttp

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered = []
    tasks = []
    with ProcessGuard(cfg.persistence.lock_file):
        if not Path(args.journal).is_file():
            raise ValueError('existing live journal is required; exit-only never creates an empty ledger')
        journal = Journal(args.journal, identity=f'{pair}:account')
        status = None
        runtime = None
        try:
            target = _validate_journal_scope(journal, authorization, args.expected_cycle_id)
            if pre_arm_check is not None:
                await pre_arm_check(journal, target)
            # Persist before any connection; an initialization failure leaves new entries closed.
            arm_manual_exit(journal, authorization, expected_cycle_id=target, account_scope=account_scope)
            for sig in (signal.SIGINT, signal.SIGTERM):
                with contextlib.suppress(NotImplementedError):
                    loop.add_signal_handler(sig, stop.set)
                    registered.append(sig)
            status = StatusWriter(args.status_json, strategy_version=main_v3.STRATEGY_VERSION,
                config_hash=canonical_config_hash(settings, pair_identity=pair), mode='manual-exit', background=True)
            connector = aiohttp.TCPConnector(family=socket.AF_INET, keepalive_timeout=75.0, ttl_dns_cache=300)
            async with aiohttp.ClientSession(connector=connector) as session:
                entropy, hedge = main_v3._make_venues(cfg, session)
                runtime = LiveRuntime(settings=settings, legacy_config=cfg, authorization=authorization,
                    pair_id=pair, deployment=args.deployment, venues=RuntimeVenues(entropy, hedge),
                    journal=journal, status=status, observer=ReadOnlyObserver(args.observer_jsonl),
                    stop=stop, reference_sampler_enabled=False,
                    reference_checkpoint_path=args.reference_checkpoint)
                try:
                    # Capture tasks even if initialize raises after starting account streams.
                    started = set(asyncio.all_tasks())
                    try:
                        tasks = await asyncio.wait_for(runtime.initialize(apply_leverage=False),
                                                       timeout=args.timeout_seconds)
                    finally:
                        tasks += [t for t in asyncio.all_tasks() - started if t is not asyncio.current_task()
                                  and t not in tasks]
                    if entropy._query_address().lower() != args.expected_hl_account.lower():
                        raise ValueError('initialized query account differs from expected account')
                    return await drive_exit_only(runtime, timeout_seconds=args.timeout_seconds,
                        accounting_wait_seconds=args.accounting_wait_seconds, expected_cycle_id=target)
                finally:
                    stop.set()
                    runtime._release_completion()
                    for task in tasks:
                        task.cancel()
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                    runtime._flush_telemetry()
                    await asyncio.gather(entropy.close(), hedge.close(), return_exceptions=True)
        finally:
            for sig in registered:
                loop.remove_signal_handler(sig)
            if status is not None:
                await status.close()
            journal.close()


async def amain(args):
    settings = main_v3.load_settings(args.v3_config)
    cfg = load_config(args.config, args.env_file, symbol=args.symbol, hedge_venue=args.hedge)
    account_scope = validate_exit_scope(args, cfg, settings)
    pair, authorization = main_v3.validate_launch(args, cfg, settings)
    result = await run_live_exit(args, cfg, settings, pair, authorization, account_scope)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0 if result['success'] else 3


def main():
    try:
        raise SystemExit(asyncio.run(amain(parser().parse_args())))
    except (ConfigError, ValueError, LiveAuthorizationError, RuntimeError) as exc:
        # Exchange exceptions may contain private request details; print the type only.
        if type(exc) in (ValueError, LiveAuthorizationError) or type(exc).__name__ == 'ProcessGuardError':
            print(f'V3 exit-only refused: {exc}', file=sys.stderr)
        else:
            print(f'V3 exit-only incomplete: {type(exc).__name__}; inspect durable local state', file=sys.stderr)
        raise SystemExit(2)


if __name__ == '__main__':
    main()
