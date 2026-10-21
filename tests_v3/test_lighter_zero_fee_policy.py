"""Explicit account/batch fee policy is audit evidence, never exchange fee history."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import json
import socket
from types import SimpleNamespace

import pytest

from entropy_arb.v3.core import Settings
from entropy_arb.v3.journal import Journal
from test_runtime import complete_background_audit, setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('zero-fee policy checks must stay offline')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


async def completed_pending(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    runtime.settings = replace(
        runtime.settings, single_cycle_review_loss_usd=None,
        lighter_zero_fee_account_index=42, lighter_zero_fee_batch_id='batch',
        lighter_zero_fee_policy_source='explicit_user_authorization')
    h.conf.lighter_creds = SimpleNamespace(account_index=42)
    h.profile = SimpleNamespace(name='mainnet')
    h.market_id = 139
    h.fee_policy_order_type = 'market_ioc_no_integrator'
    original_send = h.send_taker

    async def missing_fee(**kwargs):
        result = await original_send(**kwargs)
        result.pop('fee_usd', None)
        for fill in h.fee_fills:
            fill['fee_usd'] = None
        return result

    h.send_taker = missing_fee
    await runtime.initialize()
    await runtime._maybe_open(runtime._market(), D('0'))
    cycle_id = journal.active_live_cycle()['cycle_id']
    e.set_book(99.60, 99.61)
    h.set_book(99.99, 100)
    await runtime._resume_cycle(journal.active_live_cycle())
    await complete_background_audit(runtime)
    assert not journal.active_live_cycle()
    assert journal.batch_state('batch')['pending_accounting'] == 1
    # Simulate identified legacy fills with original NULL fees already committed.
    hedge_orders = {row['order_ref']: row for row in journal.cycle_orders(cycle_id)
                    if row['venue'] == 'hedge'}
    journal.record_fee_fills(cycle_id, 'hedge', [
        {**fill, 'units': hedge_orders[fill['order_ref']]['filled_units']}
        for fill in h.fee_fills])
    return runtime, e, h, journal, cycle_id


def row(journal, table, cycle_id):
    return dict(journal.db.execute(f'SELECT * FROM {table} WHERE cycle_id=?',
                                   (cycle_id,)).fetchone())


def test_old_null_fee_round_finalizes_without_hedge_trades_and_preserves_audit(tmp_path):
    async def scenario():
        runtime, e, h, journal, cycle_id = await completed_pending(tmp_path)
        calls = []
        original_entropy_fills = e.fetch_fee_fills

        async def forbidden_history(*args):
            calls.append('hedge-fees-forbidden')
            raise AssertionError('ordinary policy-covered hedge must not fetch fee history')

        async def entropy_fills(*args):
            calls.append('entropy-fees')
            fills = await original_entropy_fills(*args)
            return [{**fill, 'fee_usd': '.001'} for fill in fills]

        async def funding(key, amount, *args):
            calls.append(f'{key}-funding')
            return amount

        h.fetch_fee_fills = forbidden_history
        e.fetch_fee_fills = entropy_fills
        e.fetch_funding_payments = lambda *args: funding('entropy', D('.002'), *args)
        h.fetch_funding_payments = lambda *args: funding('hedge', D('-.003'), *args)
        try:
            original = [dict(x) for x in journal.db.execute(
                "SELECT * FROM fee_fills WHERE cycle_id=? AND venue='hedge'", (cycle_id,))]
            result = await runtime._reconcile_cycle_accounting(cycle_id)
            assert result is not None
            assert set(calls) == {'entropy-fees', 'entropy-funding', 'hedge-funding'}
            assert row(journal, 'cycles', cycle_id)['accounting_final'] == 1
            economics = row(journal, 'cycle_economics', cycle_id)
            assert economics['accounting_final'] == 1
            assert D(economics['hedge_fees_usd']) == 0
            assert D(economics['entropy_fees_usd']) == D('.002')
            assert D(economics['actual_funding_usd']) == D('-.001')
            assert D(economics['finalized_pnl_usd']) == result
            assert journal.batch_state('batch')['pending_accounting'] == 0
            fees = journal.fee_state(cycle_id)
            assert fees['fees_complete'] and fees['accounting_evidence_complete']
            assert not fees['history_complete']
            assert all(fees['order_fees'][order['order_ref']]['fee_source'] == 'user_policy_zero_fee'
                       for order in journal.cycle_orders(cycle_id) if order['venue'] == 'hedge')
            assert all(order['fee_usd'] is None for order in journal.cycle_orders(cycle_id)
                       if order['venue'] == 'hedge')
            assert original == [dict(x) for x in journal.db.execute(
                "SELECT * FROM fee_fills WHERE cycle_id=? AND venue='hedge'", (cycle_id,))]
            audit = journal.db.execute('SELECT value FROM metadata WHERE key=?',
                                       (f'lighter_zero_fee:{cycle_id}',)).fetchone()[0]
            evidence = json.loads(audit)
            assert evidence['account_index'] == 42 and evidence['source'] == 'explicit_user_authorization'
            assert all(x['fee_usd'] is None for x in evidence['original_fill_fees'])
            assert await runtime._reconcile_cycle_accounting(cycle_id) == result
            assert journal.db.execute('SELECT value FROM metadata WHERE key=?',
                                      (f'lighter_zero_fee:{cycle_id}',)).fetchone()[0] == audit
            assert e.position == h.position == 0 and len(e.sent) == len(h.sent) == 2
            # Durable evidence survives Journal reopen without changing original raw fees.
            path, identity = tmp_path / 'runtime.sqlite', 'pair-account'
            journal.close()
            reopened = Journal(path, identity=identity)
            try:
                assert reopened.fee_state(cycle_id)['fees_complete']
                assert row(reopened, 'cycles', cycle_id)['accounting_final'] == 1
            finally:
                reopened.close()
        finally:
            journal.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('missing', ['entropy-fee', 'entropy-funding', 'hedge-funding'])
def test_fee_policy_does_not_fill_entropy_fee_or_funding_unknown(tmp_path, missing):
    async def scenario():
        runtime, e, h, journal, cycle_id = await completed_pending(tmp_path)
        calls = []

        async def failed(*args):
            raise RuntimeError('offline unavailable')

        async def funding(key, *args):
            calls.append(key)
            if missing == f'{key}-funding':
                raise RuntimeError('offline unavailable')
            return D('.01')

        if missing == 'entropy-fee':
            for fill in e.fee_fills:
                fill['fee_usd'] = None
            e.fetch_fee_fills = failed
            journal.db.execute("UPDATE orders SET fee_usd=NULL,known_fee_usd=NULL,fee_covered_units=NULL "
                               "WHERE cycle_id=? AND venue='entropy'", (cycle_id,))
        h.fetch_fee_fills = failed
        e.fetch_funding_payments = lambda *args: funding('entropy', *args)
        h.fetch_funding_payments = lambda *args: funding('hedge', *args)
        try:
            assert await runtime._reconcile_cycle_accounting(cycle_id) is None
            assert set(calls) == {'entropy', 'hedge'}
            assert row(journal, 'cycles', cycle_id)['accounting_final'] == 0
            assert not row(journal, 'cycle_economics', cycle_id)['accounting_final']
            assert row(journal, 'cycle_economics', cycle_id)['finalized_pnl_usd'] is None
            if missing == 'entropy-fee':
                assert not journal.fee_state(cycle_id)['fees_complete_by_venue']['entropy']
                assert row(journal, 'cycle_economics', cycle_id)['entropy_fees_usd'] is None
                assert D(row(journal, 'cycle_economics', cycle_id)['actual_funding_usd']) == D('.02')
            else:
                assert row(journal, 'cycle_economics', cycle_id)[missing.replace('-', '_') + '_usd'] is None
        finally:
            journal.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('scope', ['account', 'batch', 'market', 'symbol', 'profile',
                                  'mode', 'paid-rate', 'twap', 'integrator', 'no-policy'])
def test_policy_scope_does_not_leak(tmp_path, scope):
    async def scenario():
        runtime, _e, h, journal, cycle_id = await completed_pending(tmp_path)
        if scope == 'account':
            h.conf.lighter_creds.account_index = 43
        elif scope == 'batch':
            runtime.settings = replace(runtime.settings, lighter_zero_fee_batch_id='future-batch')
        elif scope == 'market':
            h.market_id = 189
        elif scope == 'symbol':
            h.conf.symbol = 'NBIS'
        elif scope == 'profile':
            h.profile.name = 'robinhood'
        elif scope == 'mode':
            runtime.settings = replace(runtime.settings, strategy_mode='funding_hedge')
        elif scope == 'paid-rate':
            h.fee_bps = 1
        elif scope in {'twap', 'integrator'}:
            h.fee_policy_order_type = scope
        elif scope == 'no-policy':
            runtime.settings = replace(runtime.settings, lighter_zero_fee_account_index=None,
                                       lighter_zero_fee_batch_id=None, lighter_zero_fee_policy_source=None)
        try:
            assert not runtime._apply_lighter_zero_fee_policy(cycle_id)
            assert not journal.fee_state(cycle_id)['fees_complete']
            assert not journal.db.execute("SELECT 1 FROM metadata WHERE key LIKE 'lighter_zero_fee:%'").fetchone()
        finally:
            journal.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('bad_evidence', ['unknown-order', 'missing-price', 'missing-grid',
                                        'nonflat', 'actual-fee', 'partial-actual-fee'])
def test_invalid_execution_or_actual_fee_cannot_be_overwritten(tmp_path, bad_evidence):
    async def scenario():
        runtime, _e, _h, journal, cycle_id = await completed_pending(tmp_path)
        ref = next(x['order_ref'] for x in journal.cycle_orders(cycle_id) if x['venue'] == 'hedge')
        if bad_evidence == 'unknown-order':
            journal.db.execute("UPDATE orders SET state='UNKNOWN' WHERE order_ref=?", (ref,))
        elif bad_evidence == 'missing-price':
            journal.db.execute('UPDATE orders SET avg_px=NULL WHERE order_ref=?', (ref,))
        elif bad_evidence == 'missing-grid':
            journal.db.execute('UPDATE live_steps SET quantity_step=NULL WHERE order_ref=?', (ref,))
        elif bad_evidence == 'nonflat':
            journal.db.execute('UPDATE live_cycles SET hedge_units=1 WHERE cycle_id=?', (cycle_id,))
        elif bad_evidence == 'actual-fee':
            journal.db.execute('UPDATE fee_fills SET fee_usd=? WHERE order_ref=?', ('.01', ref))
            journal.record_fee_fills(cycle_id, 'hedge', [])
        elif bad_evidence == 'partial-actual-fee':
            journal.db.execute('UPDATE orders SET known_fee_usd=? WHERE order_ref=?', ('.01', ref))
        try:
            assert not runtime._apply_lighter_zero_fee_policy(cycle_id)
            assert not journal.db.execute("SELECT 1 FROM metadata WHERE key LIKE 'lighter_zero_fee:%'").fetchone()
            if bad_evidence == 'actual-fee':
                assert journal.fee_state(cycle_id)['known_fees_by_venue']['hedge'] == D('.01')
        finally:
            journal.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('values', [
    {'lighter_zero_fee_account_index': 42},
    {'lighter_zero_fee_account_index': True, 'lighter_zero_fee_batch_id': 'b', 'lighter_zero_fee_policy_source': 'user'},
    {'lighter_zero_fee_account_index': 42, 'lighter_zero_fee_batch_id': 'b', 'lighter_zero_fee_policy_source': ''},
])
def test_policy_requires_explicit_valid_account_batch_source(values):
    with pytest.raises(ValueError, match='zero-fee policy'):
        Settings.from_mapping(values)


def test_null_policy_default_and_explicit_policy_config_roundtrip():
    assert Settings.from_mapping({'lighter_zero_fee_account_index': None}) == Settings()
    values = dict(lighter_zero_fee_account_index=42, lighter_zero_fee_batch_id='b',
                  lighter_zero_fee_policy_source='user')
    settings = Settings.from_mapping(values)
    assert all(getattr(settings, key) == value for key, value in values.items())


@pytest.mark.parametrize('history', ['absent', 'partial'])
def test_policy_uses_complete_cumulative_price_when_history_is_incomplete(tmp_path, history):
    async def scenario():
        runtime, _e, h, journal, cycle_id = await completed_pending(tmp_path)
        orders = [x for x in journal.cycle_orders(cycle_id) if x['venue'] == 'hedge']
        opening = next(x for x in orders if not x['reduce_only'])
        if history == 'absent':
            journal.db.execute("DELETE FROM fee_fills WHERE cycle_id=? AND venue='hedge'", (cycle_id,))
        else:
            journal.db.execute('UPDATE fee_fills SET units=1,price=? WHERE order_ref=?',
                               ('80', opening['order_ref']))
        original = [dict(x) for x in journal.db.execute(
            "SELECT * FROM fee_fills WHERE cycle_id=? AND venue='hedge'", (cycle_id,))]
        calls = []

        async def forbidden(*args):
            calls.append('fee-history')
            raise AssertionError('policy fee history must not be called')

        h.fetch_fee_fills = forbidden
        try:
            result = await runtime._reconcile_cycle_accounting(cycle_id)
            assert result is not None and calls == []
            economics = row(journal, 'cycle_economics', cycle_id)
            assert economics['accounting_final'] == 1
            assert D(economics['actual_entry_basis_bps']) == (
                D(next(x['avg_px'] for x in journal.cycle_orders(cycle_id)
                       if x['venue'] == 'entropy' and not x['reduce_only']))
                / D(opening['avg_px']) - 1) * 10000
            assert original == [dict(x) for x in journal.db.execute(
                "SELECT * FROM fee_fills WHERE cycle_id=? AND venue='hedge'", (cycle_id,))]
            step = next(x for x in journal.economics_steps(cycle_id)
                        if x['order_ref'] == opening['order_ref'])
            assert step['actual_fill_ts'] is None
            assert D(step['actual_fill_vwap']) == D(opening['avg_px'])
        finally:
            journal.close()
    asyncio.run(scenario())


def test_actual_nonzero_fee_after_policy_keeps_actual_evidence_and_conflict_is_rejected(tmp_path):
    async def scenario():
        runtime, _e, _h, journal, cycle_id = await completed_pending(tmp_path)
        try:
            assert runtime._apply_lighter_zero_fee_policy(cycle_id)
            audit = json.loads(journal.db.execute('SELECT value FROM metadata WHERE key=?',
                                                 (f'lighter_zero_fee:{cycle_id}',)).fetchone()[0])
            policy = {key: audit[key] for key in ('batch_id', 'account_index', 'profile', 'symbol',
                                                'market_id', 'order_type', 'fee_usd', 'source')}
            with pytest.raises(ValueError, match='conflicting'):
                journal.authorize_lighter_zero_fee(cycle_id, policy={**policy, 'source': 'changed'})
            ref = next(x['order_ref'] for x in journal.cycle_orders(cycle_id) if x['venue'] == 'hedge')
            journal.db.execute('UPDATE fee_fills SET fee_usd=? WHERE order_ref=?', ('.01', ref))
            journal.record_fee_fills(cycle_id, 'hedge', [])
            assert runtime._apply_lighter_zero_fee_policy(cycle_id)
            state = journal.fee_state(cycle_id)
            assert state['known_fees_by_venue']['hedge'] == D('.01')
            assert state['order_fees'][ref]['fee_source'] == 'fill_history'
        finally:
            journal.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('source', ['fill-history', 'cumulative'])
@pytest.mark.parametrize('analytics_fees', ['present', 'null'])
def test_actual_fee_after_policy_final_requeues_then_recomputes_pnl(tmp_path, source, analytics_fees):
    async def scenario():
        runtime, _e, h, journal, cycle_id = await completed_pending(tmp_path)
        try:
            first = await runtime._reconcile_cycle_accounting(cycle_id)
            assert first is not None
            if analytics_fees == 'null':
                journal.db.execute('UPDATE cycle_economics SET actual_fees_usd=NULL WHERE cycle_id=?',
                                   (cycle_id,))
            ref = next(x['order_ref'] for x in journal.cycle_orders(cycle_id) if x['venue'] == 'hedge')
            if source == 'fill-history':
                journal.db.execute('UPDATE fee_fills SET fee_usd=? WHERE order_ref=?', ('.01', ref))
                journal.record_fee_fills(cycle_id, 'hedge', [])
            else:
                from entropy_arb.v3.journal import Result
                order = journal.order(ref)
                journal.record(ref, Result(True, order['filled_units'], D(order['avg_px']), D('.01')))
            assert row(journal, 'cycles', cycle_id)['accounting_final'] == 0
            assert D(row(journal, 'cycles', cycle_id)['pnl_usd']) == (
                first - D('.01') if analytics_fees == 'present' else first)
            assert row(journal, 'cycle_economics', cycle_id)['finalized_pnl_usd'] is None
            assert journal.batch_state('batch')['pending_accounting'] == 1
            assert journal.provisional_cycles('batch')[0]['cycle_id'] == cycle_id
            calls = []

            async def forbidden(*args):
                calls.append(True)
                raise AssertionError('existing policy skips fee-only history')

            h.fetch_fee_fills = forbidden
            second = await runtime._reconcile_cycle_accounting(cycle_id)
            assert second == first - D('.01')
            assert row(journal, 'cycles', cycle_id)['accounting_final'] == 1
            assert D(row(journal, 'cycle_economics', cycle_id)['hedge_fees_usd']) == D('.01')
            assert journal.batch_state('batch')['pending_accounting'] == 0
            assert calls == []
        finally:
            journal.close()
    asyncio.run(scenario())


def test_late_fee_funding_reread_failure_preserves_known_loss_and_blocks_reentry(tmp_path):
    async def scenario():
        runtime, e, h, journal, cycle_id = await completed_pending(tmp_path)
        async def funding(*args):
            return D('-2.99')
        h.fetch_funding_payments = funding
        try:
            first = await runtime._reconcile_cycle_accounting(cycle_id)
            assert first is not None and first > D('-3')
            ref = next(x['order_ref'] for x in journal.cycle_orders(cycle_id) if x['venue'] == 'hedge')
            journal.db.execute('UPDATE fee_fills SET fee_usd=? WHERE order_ref=?', ('.1', ref))
            journal.record_fee_fills(cycle_id, 'hedge', [])
            async def unavailable(*args):
                raise RuntimeError('offline funding unavailable')
            e.fetch_funding_payments = h.fetch_funding_payments = unavailable
            assert await runtime._reconcile_cycle_accounting(cycle_id) is None
            assert not runtime._estimate_basis_accounting(cycle_id)
            state = journal.batch_state('batch')
            assert not state['can_open']
            assert state['pending_accounting'] == state['blocking_pending_accounting'] == 1
            assert state['known_loss_used_usd'] == -(first - D('.1')) > 3
            assert row(journal, 'cycle_economics', cycle_id)['accounting_final'] == 0
            assert row(journal, 'cycle_economics', cycle_id)['finalized_pnl_usd'] is None
            assert e.position == h.position == 0 and len(e.sent) == len(h.sent) == 2
        finally:
            journal.close()
    asyncio.run(scenario())
