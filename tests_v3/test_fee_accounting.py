import asyncio
from decimal import Decimal as D
import socket

import pytest

from entropy_arb.funding import FundingSettlement
from entropy_arb.v3.core import Side
from entropy_arb.v3.journal import Intent, Journal, Result
from test_funding_hedge_runtime import carry, fresh_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('offline fee-accounting tests cannot access network')

    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


@pytest.fixture
def fee_journal(tmp_path):
    journal = Journal(tmp_path / 'fees.sqlite', identity='offline-fee-account')
    journal.ensure_batch('fee-batch', max_position_cycles=3, loss_budget_usd=D('3'))
    journal.create_live_cycle(
        batch_id='fee-batch', cycle_id='fee-cycle', direction='long_entropy',
        planned_units=100, reference_bps=D('0'), entry_basis_bps=D('0'),
        started_ts=10000)
    journal.ensure_funding_cycle('fee-cycle', hedged_ts=10000)
    yield journal
    journal.close()


def order(journal, ref, *, venue='entropy', units=20, fee=None, terminal=True,
          filled_units=None):
    journal.prepare(Intent(ref, 'fee-cycle', venue, Side.BUY, units, D('100')))
    journal.record(ref, Result(terminal=terminal,
                               cumulative_units=units if filled_units is None else filled_units,
                               avg_px=D('100'), fee_usd=fee))
    return ref


def fill(ref, key, units, fee, **values):
    return {'order_ref': ref, 'exchange_fill_key': key, 'units': units,
            'price': D('100'), 'ts': 10001, 'fee_usd': fee, 'side': 'buy', **values}


def cycle_result(journal, cycle_id='fee-cycle'):
    return dict(journal.db.execute('SELECT * FROM cycles WHERE cycle_id=?', (cycle_id,)).fetchone())


def finish(journal):
    journal.finish_cycle('fee-cycle', kind='clean', net_pnl_usd=None,
                         positions_verified_flat=True, all_orders_terminal=True,
                         accounting_final=False)
    journal.update_live_cycle('fee-cycle', phase='DONE', updated_ts=10003)


def test_order_growth_invalidates_total_fee_and_retains_known_partial_receipt(fee_journal):
    journal = fee_journal
    ref = order(journal, 'growing', terminal=False, filled_units=10, fee=D('.02'))
    journal.record(ref, Result(terminal=True, cumulative_units=20, avg_px=D('100')))
    row = journal.order(ref)
    assert row['fee_usd'] is None
    assert D(row['known_fee_usd']) == D('.02')
    assert row['fee_covered_units'] == 10
    state = journal.fee_state('fee-cycle')
    assert state['known_fees_usd'] == D('.02')
    assert not state['fees_complete']
    journal.record(ref, Result(terminal=True, cumulative_units=20, avg_px=D('100')))
    assert journal.fee_state('fee-cycle')['known_fees_usd'] == D('.02')


def test_duplicate_history_and_null_enrichment_do_not_double_or_erase_fees(fee_journal):
    journal = fee_journal
    ref = order(journal, 'two-fills')
    first, second = fill(ref, 'f1', 10, D('.02')), fill(ref, 'f2', 10, None)
    journal.record_fee_fills('fee-cycle', 'entropy', [first, second, first])
    state = journal.fee_state('fee-cycle')
    assert state['known_fees_usd'] == D('.02')
    assert state['order_fees'][ref]['known_fee_usd'] == D('.02')
    assert state['order_fees'][ref]['fee_covered_units'] == 10
    assert not state['order_fees'][ref]['history_complete']
    assert journal.db.execute('SELECT SUM(units) FROM fee_fills WHERE order_ref=?',
                              (ref,)).fetchone()[0] == 20
    assert not state['order_fees'][ref]['fee_complete']
    assert journal.order(ref)['fee_usd'] is None
    journal.record_fee_fills('fee-cycle', 'entropy', [{**second, 'fee_usd': D('.03')}, first])
    journal.record_fee_fills('fee-cycle', 'entropy', [second, {**first, 'fee_usd': None}])
    state = journal.fee_state('fee-cycle')
    assert state['known_fees_usd'] == D('.05')
    assert state['fees_complete']
    assert state['history_complete']
    assert D(journal.order(ref)['fee_usd']) == D('.05')
    assert journal.db.execute('SELECT COUNT(*) FROM fee_fills').fetchone()[0] == 2


def test_explicit_zero_and_negative_rebate_are_verified_fee_evidence(fee_journal):
    journal = fee_journal
    ref = order(journal, 'rebate')
    journal.record_fee_fills('fee-cycle', 'entropy',
                            [fill(ref, 'zero', 10, D('0')), fill(ref, 'rebate', 10, D('-.02'))])
    state = journal.fee_state('fee-cycle')
    assert state['fees_complete']
    assert state['known_fees_usd'] == D('-.02')
    assert D(journal.order(ref)['fee_usd']) == D('-.02')


def test_same_fill_key_on_different_venues_is_not_a_duplicate(fee_journal):
    journal = fee_journal
    e_ref, h_ref = order(journal, 'e'), order(journal, 'h', venue='hedge')
    journal.record_fee_fills('fee-cycle', 'entropy', [fill(e_ref, '1', 20, D('.01'))])
    journal.record_fee_fills('fee-cycle', 'hedge', [fill(h_ref, '1', 20, D('.02'))])
    state = journal.fee_state('fee-cycle')
    assert state['known_fees_by_venue'] == {'entropy': D('.01'), 'hedge': D('.02')}
    assert state['fees_complete']
    assert journal.db.execute('SELECT COUNT(*) FROM fee_fills').fetchone()[0] == 2


def test_history_matching_partial_receipt_does_not_add_the_same_fee_twice(fee_journal):
    journal = fee_journal
    ref = order(journal, 'receipt-and-history', terminal=False, filled_units=10, fee=D('.02'))
    journal.record(ref, Result(terminal=True, cumulative_units=20, avg_px=D('100')))
    journal.record_fee_fills('fee-cycle', 'entropy', [fill(ref, 'same-ten', 10, D('.02'))])
    assert journal.fee_state('fee-cycle')['known_fees_usd'] == D('.02')
    journal.record_fee_fills('fee-cycle', 'entropy', [fill(ref, 'remaining-ten', 10, D('.03'))])
    assert journal.fee_state('fee-cycle')['known_fees_usd'] == D('.05')
    assert D(journal.order(ref)['fee_usd']) == D('.05')


def test_smaller_partial_history_cannot_replace_larger_known_receipt(fee_journal):
    journal = fee_journal
    ref = order(journal, 'larger-receipt', terminal=False, filled_units=10, fee=D('.02'))
    journal.record(ref, Result(terminal=True, cumulative_units=20, avg_px=D('100')))
    journal.record_fee_fills('fee-cycle', 'entropy', [fill(ref, 'first-five', 5, D('.01'))])
    state = journal.fee_state('fee-cycle')
    assert state['known_fees_usd'] == D('.02')
    assert state['order_fees'][ref]['fee_covered_units'] == 10
    assert not state['fees_complete']
    journal.record_fee_fills('fee-cycle', 'entropy', [fill(ref, 'next-five', 5, D('.01'))])
    state = journal.fee_state('fee-cycle')
    assert state['known_fees_usd'] == D('.02')
    assert state['order_fees'][ref]['fee_covered_units'] == 10
    assert state['order_fees'][ref]['fee_source'].startswith('fill_history')
    journal.record_fee_fills('fee-cycle', 'entropy', [fill(ref, 'remaining-ten', 10, D('.03'))])
    assert journal.fee_state('fee-cycle')['known_fees_usd'] == D('.05')
    assert journal.fee_state('fee-cycle')['fees_complete']


def test_complete_order_receipt_is_retained_without_complete_fill_history(fee_journal):
    journal = fee_journal
    ref = order(journal, 'actual-receipt', fee=D('.04'))
    journal.record_fee_fills('fee-cycle', 'entropy', [fill(ref, 'missing-fee', 10, None)])
    state = journal.fee_state('fee-cycle')
    assert state['known_fees_usd'] == D('.04')
    assert state['order_fees'][ref]['fee_complete']
    assert not state['order_fees'][ref]['history_complete']
    assert state['order_fees'][ref]['fee_source']
    assert D(journal.order(ref)['fee_usd']) == D('.04')


def test_repeated_terminal_receipt_cannot_replace_complete_fill_fees(fee_journal):
    journal = fee_journal
    ref = order(journal, 'history-over-receipt', fee=D('.04'))
    journal.record_fee_fills('fee-cycle', 'entropy',
                            [fill(ref, 'first-ten', 10, D('.02')),
                             fill(ref, 'remaining-ten', 10, D('.03'))])
    finish(journal)
    journal.refresh_funding_pnl('fee-cycle', trade_gross_cash_usd=D('-.1'))
    assert journal.funding_state('fee-cycle')['known_cash_result_usd'] == D('-.15')
    for _ in range(2):
        journal.record(ref, Result(terminal=True, cumulative_units=20,
                                  avg_px=D('100'), fee_usd=D('.04')))
        journal.refresh_funding_pnl('fee-cycle')
        row = journal.order(ref)
        assert D(row['fee_usd']) == D('.05')
        assert D(row['known_fee_usd']) == D('.05')
        assert row['fee_covered_units'] == 20
        assert row['fee_source'] == 'fill_history'
        assert journal.fee_state('fee-cycle')['known_fees_usd'] == D('.05')
        assert journal.funding_state('fee-cycle')['known_cash_result_usd'] == D('-.15')
        assert D(cycle_result(journal)['pnl_usd']) == D('-.15')
        assert journal.batch_state('fee-batch')['loss_used_usd'] == D('.15')


@pytest.mark.parametrize('fees_complete', [True, False])
def test_full_price_history_survives_conflicting_receipt_with_unknown_fees(fee_journal, fees_complete):
    journal = fee_journal
    ref = order(journal, 'history-price')
    journal.record_fee_fills('fee-cycle', 'entropy',
                            [fill(ref, 'first-ten', 10, D('.02')),
                             fill(ref, 'remaining-ten', 10, D('.03') if fees_complete else None)])
    finish(journal)
    fee_state = journal.fee_state('fee-cycle')
    original_source = journal.order(ref)['fee_source']
    expected = D('-.2') - (D('.05') if fees_complete else D('.02'))
    for _ in range(2):
        journal.record(ref, Result(terminal=True, cumulative_units=20, avg_px=D('101'),
                                  fee_usd=D('.04') if fees_complete else None))
        row = journal.order(ref)
        assert D(row['avg_px']) == D('100')
        assert row['fee_source'] == original_source
        assert journal.fee_state('fee-cycle') == fee_state
        gross = -D(row['avg_px']) * row['filled_units'] * D('.0001')
        journal.refresh_funding_pnl('fee-cycle', trade_gross_cash_usd=gross)
        state = journal.funding_state('fee-cycle')
        assert state['trade_gross_cash_usd'] == D('-.2')
        assert state['known_cash_result_usd'] == expected
        assert state['trade_cash_net_usd'] == (expected if fees_complete else None)
        pnl = cycle_result(journal)['pnl_usd']
        assert (D(pnl) if pnl is not None else None) == (expected if fees_complete else None)
        assert journal.batch_state('fee-batch')['loss_used_usd'] == -expected
        actual_fee = D(row['fee_usd']) if row['fee_usd'] is not None else None
        assert actual_fee == (D('.05') if fees_complete else None)


@pytest.mark.parametrize('conflict', ['order_ref', 'units', 'price', 'fee'])
def test_conflicting_fill_rolls_back_the_whole_page(fee_journal, conflict):
    journal = fee_journal
    ref, other = order(journal, 'original'), order(journal, 'other')
    original = fill(ref, 'existing', 10, D('.02'))
    journal.record_fee_fills('fee-cycle', 'entropy', [original])
    changed = dict(original)
    if conflict == 'order_ref':
        changed['order_ref'] = other
    elif conflict == 'units':
        changed['units'] = 9
    elif conflict == 'price':
        changed['price'] = D('101')
    else:
        changed['fee_usd'] = D('.03')
    before = journal.fee_state('fee-cycle')
    with pytest.raises(ValueError):
        journal.record_fee_fills('fee-cycle', 'entropy', [fill(other, 'new', 10, D('.01')), changed])
    assert journal.fee_state('fee-cycle') == before
    assert journal.db.execute('SELECT COUNT(*) FROM fee_fills').fetchone()[0] == 1


@pytest.mark.parametrize('invalid', ['missing_key', 'oversized', 'cumulative_oversized',
                                    'wrong_venue', 'wrong_side', 'unknown_order', 'zero_units',
                                    'boolean_units', 'nonfinite_fee'])
def test_invalid_fee_fill_cannot_mutate_verified_evidence(fee_journal, invalid):
    journal = fee_journal
    ref = order(journal, 'validated')
    good = fill(ref, 'good', 10, D('.02'))
    journal.record_fee_fills('fee-cycle', 'entropy', [good])
    row, venue = fill(ref, 'bad', 10, D('.03')), 'entropy'
    if invalid == 'missing_key':
        row.pop('exchange_fill_key')
    elif invalid == 'oversized':
        row['units'] = 21
    elif invalid == 'cumulative_oversized':
        row['units'] = 11
    elif invalid == 'wrong_venue':
        venue = 'hedge'
    elif invalid == 'wrong_side':
        row['side'] = 'sell'
    elif invalid == 'unknown_order':
        row['order_ref'] = 'unknown'
    elif invalid == 'zero_units':
        row['units'] = 0
    elif invalid == 'boolean_units':
        row['units'] = True
    else:
        row['fee_usd'] = D('NaN')
    before = journal.fee_state('fee-cycle')
    with pytest.raises((ValueError, KeyError)):
        journal.record_fee_fills('fee-cycle', venue, [row])
    assert journal.fee_state('fee-cycle') == before
    assert journal.db.execute('SELECT COUNT(*) FROM fee_fills').fetchone()[0] == 1


def test_legacy_net_is_not_reinterpreted_as_gross_and_unknown_stays_null(fee_journal):
    journal = fee_journal
    e_ref, h_ref = order(journal, 'e'), order(journal, 'h', venue='hedge')
    finish(journal)
    journal.refresh_funding_pnl('fee-cycle', trade_cash_net_usd=D('-.171'))
    state = journal.funding_state('fee-cycle')
    assert state['trade_gross_cash_usd'] is None
    assert not state['fee_evidence_accounting']
    journal.record_fee_fills('fee-cycle', 'entropy', [fill(e_ref, 'e', 20, D('.035802'))])
    journal.refresh_funding_pnl('fee-cycle', trade_gross_cash_usd=None)
    state = journal.funding_state('fee-cycle')
    assert state['fee_evidence_accounting']
    assert state['trade_gross_cash_usd'] is None
    assert state['trade_cash_net_usd'] is None
    assert state['known_cash_result_usd'] is None
    assert cycle_result(journal)['pnl_usd'] is None
    journal.refresh_funding_pnl('fee-cycle', trade_gross_cash_usd=D('-.1'))
    state = journal.funding_state('fee-cycle')
    assert state['known_cash_result_usd'] == D('-.135802')
    assert state['trade_cash_net_usd'] is None
    assert cycle_result(journal)['pnl_usd'] is None
    journal.record_fee_fills('fee-cycle', 'hedge', [fill(h_ref, 'h', 20, D('.004'))])
    journal.refresh_funding_pnl('fee-cycle')
    assert journal.funding_state('fee-cycle')['trade_cash_net_usd'] == D('-.139802')
    assert D(cycle_result(journal)['pnl_usd']) == D('-.139802')
    assert not cycle_result(journal)['accounting_final']


def test_partial_fees_and_booked_funding_recompute_same_loss_budget_once(fee_journal):
    journal = fee_journal
    e_ref, h_ref = order(journal, 'e'), order(journal, 'h', venue='hedge')
    journal.record_fee_fills('fee-cycle', 'entropy', [fill(e_ref, 'e', 20, D('.035802'))])
    journal.record_fee_fills('fee-cycle', 'hedge', [fill(h_ref, 'h', 20, None)])
    finish(journal)
    funding = [FundingSettlement('funding-e', 10001, D('.001244'))]
    journal.record_funding_settlements('fee-cycle', 'entropy', funding)
    journal.record_funding_settlements('fee-cycle', 'hedge',
                                      [FundingSettlement('funding-h', 10001, D('-.000795'))])
    for _ in range(3):
        journal.record_funding_settlements('fee-cycle', 'entropy', funding)
        journal.refresh_funding_pnl('fee-cycle', trade_gross_cash_usd=D('-.171'))
    state = journal.funding_state('fee-cycle')
    assert state['known_cash_result_usd'] == D('-.206353')
    assert state['trade_cash_net_usd'] is None
    assert cycle_result(journal)['pnl_usd'] is None
    assert journal.batch_state('fee-batch')['loss_used_usd'] == D('.206353')
    journal.record_fee_fills('fee-cycle', 'hedge', [fill(h_ref, 'h', 20, D('.004'))])
    for _ in range(3):
        journal.refresh_funding_pnl('fee-cycle')
    state = journal.funding_state('fee-cycle')
    assert state['trade_cash_net_usd'] == D('-.210802')
    assert state['known_cash_result_usd'] == D('-.210353')
    assert D(cycle_result(journal)['pnl_usd']) == D('-.210353')
    assert journal.batch_state('fee-batch')['loss_used_usd'] == D('.210353')
    assert state['trade_accounting_complete']
    assert not cycle_result(journal)['accounting_final']


def test_old_schema_migration_does_not_invent_fee_evidence_or_gross(fee_journal):
    journal = fee_journal
    ref = order(journal, 'legacy-unknown')
    finish(journal)
    journal.refresh_funding_pnl('fee-cycle', trade_cash_net_usd=D('-.2'))
    path = journal.db.execute('PRAGMA database_list').fetchone()[2]
    journal.db.execute('DROP TABLE fee_fills')
    for name in ('known_fee_usd', 'fee_covered_units', 'fee_source'):
        journal.db.execute(f'ALTER TABLE orders DROP COLUMN {name}')
    for name in ('trade_gross_cash_usd', 'known_cash_result_usd', 'fee_evidence_accounting'):
        journal.db.execute(f'ALTER TABLE funding_cycles DROP COLUMN {name}')
    journal.db.commit()
    journal.close()
    reopened = Journal(path, identity='offline-fee-account')
    try:
        state = reopened.funding_state('fee-cycle')
        assert state['trade_gross_cash_usd'] is None
        assert state['known_cash_result_usd'] is None
        assert not state['fee_evidence_accounting']
        assert state['trade_cash_net_usd'] == D('-.2')
        assert reopened.order(ref)['fee_usd'] is None
        assert not reopened.fee_state('fee-cycle')['fees_complete']
        assert reopened.fee_state('fee-cycle')['known_fees_usd'] == 0
    finally:
        reopened.close()


@pytest.fixture
def fee_runtime(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    for venue in (e, h):
        venue.history_fee = D('.02')
        original = venue.send_taker

        async def send(*args, v=venue, original=original, **kwargs):
            result = await original(*args, **kwargs)
            result['fee_usd'] = None
            if result['filled_base']:
                row = v.fee_fills[-1]
                row['exchange_fill_key'] = f'{v.key}:{len(v.fee_fills)}'
                row['fee_usd'] = v.history_fee
            return result

        venue.send_taker = send
    yield carry


async def close_pair(fee_runtime):
    runtime, e, h, journal, clock, tick, open_pair = fee_runtime
    cycle = journal.active_live_cycle()
    tick(clock[0] + 3)
    runtime._latch_funding_exit(cycle, 'manual_test')
    await runtime._funding_exit(cycle)
    return cycle


def gross_cash(runtime, journal, cycle):
    return sum((D(row['avg_px']) * row['filled_units'] * runtime.instrument.quantity_step
                * (1 if row['side'] == 'sell' else -1)
                for row in journal.cycle_orders(cycle['cycle_id'])), D('0'))


@pytest.mark.parametrize('failed_leg', ['entropy', 'hedge'])
def test_runtime_saves_peer_fees_when_one_history_query_fails(fee_runtime, failed_leg):
    runtime, e, h, journal, clock, tick, open_pair = fee_runtime

    async def scenario():
        cycle = await open_pair()
        failed = e if failed_leg == 'entropy' else h
        good_key = 'hedge' if failed_leg == 'entropy' else 'entropy'

        async def unavailable(*args, **kwargs):
            raise TimeoutError('synthetic-private-account-error')

        failed.fetch_fee_fills = unavailable
        await close_pair(fee_runtime)
        state = journal.fee_state(cycle['cycle_id'])
        assert state['known_fees_by_venue'][good_key] == D('.04')
        assert state['known_fees_by_venue'][failed_leg] == 0
        assert state['fees_complete_by_venue'][good_key]
        assert not state['fees_complete_by_venue'][failed_leg]
        assert not state['fees_complete']
        assert cycle_result(journal, cycle['cycle_id'])['pnl_usd'] is None
        funding = journal.funding_state(cycle['cycle_id'])
        assert funding['trade_cash_net_usd'] is None
        assert funding['known_cash_result_usd'] == gross_cash(runtime, journal, cycle) - D('.04')
        assert journal.batch_state('batch')['loss_used_usd'] == max(-funding['known_cash_result_usd'], D('0'))

    asyncio.run(scenario())


def test_runtime_keeps_same_order_partial_fees_and_total_net_unknown(fee_runtime):
    runtime, e, h, journal, clock, tick, open_pair = fee_runtime

    async def scenario():
        cycle = await open_pair()
        row = h.fee_fills[0]
        h.fee_fills[:] = [{**row, 'exchange_fill_key': 'h-open-a', 'qty': .995, 'fee_usd': D('.01')},
                         {**row, 'exchange_fill_key': 'h-open-b', 'qty': .995, 'fee_usd': None}]
        h.history_fee = None
        await close_pair(fee_runtime)
        state = journal.fee_state(cycle['cycle_id'])
        h_open = state['order_fees'][row['order_ref']]
        assert h_open['known_fee_usd'] == D('.01')
        assert h_open['fee_covered_units'] == 9950
        assert not h_open['history_complete']
        assert journal.db.execute('SELECT SUM(units) FROM fee_fills WHERE order_ref=?',
                                  (row['order_ref'],)).fetchone()[0] == 19900
        assert not h_open['fee_complete']
        assert journal.order(row['order_ref'])['fee_usd'] is None
        assert state['known_fees_usd'] == D('.05')
        assert cycle_result(journal, cycle['cycle_id'])['pnl_usd'] is None
        assert journal.funding_state(cycle['cycle_id'])['known_cash_result_usd'] == (
            gross_cash(runtime, journal, cycle) - D('.05'))

    asyncio.run(scenario())


def test_late_fee_enrichment_restart_and_funding_are_idempotent(fee_runtime):
    runtime, e, h, journal, clock, tick, open_pair = fee_runtime

    async def scenario():
        cycle = await open_pair()
        opening = h.fee_fills[0]
        h.fee_fills[:] = [{**opening, 'exchange_fill_key': 'h-open-a', 'qty': .995, 'fee_usd': D('.01')},
                         {**opening, 'exchange_fill_key': 'h-open-b', 'qty': .995, 'fee_usd': None}]
        h.history_fee = None
        await close_pair(fee_runtime)
        h.fee_fills[1]['fee_usd'] = D('.003')
        h.fee_fills[2]['fee_usd'] = D('.004')
        e.settlements.append(FundingSettlement('late-credit', 10001, D('.001244')))
        h.settlements.append(FundingSettlement('late-debit', 10001, D('-.000795')))
        h.fee_fills[:] = [h.fee_fills[2], h.fee_fills[1], h.fee_fills[0], h.fee_fills[1]]
        tick(10130)
        for _ in range(3):
            assert await runtime._reconcile_cycle_accounting(cycle['cycle_id']) is None
        expected = gross_cash(runtime, journal, cycle) - D('.057') + D('.000449')
        assert D(cycle_result(journal, cycle['cycle_id'])['pnl_usd']) == expected
        assert journal.batch_state('batch')['loss_used_usd'] == max(-expected, D('0'))
        assert journal.fee_state(cycle['cycle_id'])['known_fees_usd'] == D('.057')
        assert journal.fee_state(cycle['cycle_id'])['fees_complete']
        assert journal.funding_state(cycle['cycle_id'])['trade_accounting_complete']
        assert not cycle_result(journal, cycle['cycle_id'])['accounting_final']
        path = journal.db.execute('PRAGMA database_list').fetchone()[2]
        journal.close()
        reopened = Journal(path, identity='pair-account')
        restarted = fresh_runtime(runtime, reopened)
        restarted.instrument = runtime.instrument
        try:
            for _ in range(3):
                assert await restarted._reconcile_cycle_accounting(cycle['cycle_id']) is None
            assert D(cycle_result(reopened, cycle['cycle_id'])['pnl_usd']) == expected
            assert reopened.batch_state('batch')['loss_used_usd'] == max(-expected, D('0'))
            assert reopened.fee_state(cycle['cycle_id'])['known_fees_usd'] == D('.057')
        finally:
            reopened.close()

    asyncio.run(scenario())


def test_zero_configured_fee_does_not_prove_unknown_actual_fee(fee_runtime):
    runtime, e, h, journal, clock, tick, open_pair = fee_runtime
    assert runtime.instrument.entropy_fee_bps == runtime.instrument.hedge_fee_bps == 0
    cash, final = runtime._cash_result('entropy', Side.BUY,
                                     Result(terminal=True, cumulative_units=100,
                                            avg_px=D('100')), D('100'))
    assert cash == D('-1')
    assert not final

    async def scenario():
        cycle = await open_pair()
        for venue in (e, h):
            venue.history_fee = None
            for row in venue.fee_fills:
                row['fee_usd'] = None
        await close_pair(fee_runtime)
        state = journal.fee_state(cycle['cycle_id'])
        assert state['known_fees_usd'] == 0
        assert not state['fees_complete']
        assert cycle_result(journal, cycle['cycle_id'])['pnl_usd'] is None
        assert journal.funding_state(cycle['cycle_id'])['known_cash_result_usd'] == gross_cash(runtime, journal, cycle)

    asyncio.run(scenario())


def test_legacy_missing_unit_grids_cannot_recover_gross_from_net(fee_runtime):
    runtime, e, h, journal, clock, tick, open_pair = fee_runtime

    async def scenario():
        cycle = await open_pair()
        await close_pair(fee_runtime)
        journal.db.execute('UPDATE live_steps SET quantity_step=NULL WHERE cycle_id=?',
                           (cycle['cycle_id'],))
        journal.db.execute('UPDATE execution_economics_steps SET quantity_step=NULL WHERE cycle_id=?',
                           (cycle['cycle_id'],))
        journal.db.execute('UPDATE live_cycles SET cash_net_usd=? WHERE cycle_id=?',
                           ('-7.123', cycle['cycle_id']))
        journal.db.execute('''UPDATE funding_cycles SET trade_gross_cash_usd=NULL,
            known_cash_result_usd=NULL,fee_evidence_accounting=0,trade_cash_net_usd=?
            WHERE cycle_id=?''', ('-7.123', cycle['cycle_id']))
        journal.db.commit()
        assert await runtime._reconcile_cycle_accounting(cycle['cycle_id']) is None
        state = journal.funding_state(cycle['cycle_id'])
        assert state['fee_evidence_accounting']
        assert state['trade_gross_cash_usd'] is None
        assert state['trade_cash_net_usd'] is None
        assert state['known_cash_result_usd'] is None
        assert not state['trade_accounting_complete']
        assert cycle_result(journal, cycle['cycle_id'])['pnl_usd'] is None
        assert not cycle_result(journal, cycle['cycle_id'])['accounting_final']
        assert journal.fee_state(cycle['cycle_id'])['known_fees_usd'] == D('.08')

    asyncio.run(scenario())


@pytest.mark.parametrize('slow_leg', ['entropy', 'hedge'])
def test_fast_venue_fees_are_persistent_before_slow_peer_returns(fee_runtime, slow_leg):
    runtime, e, h, journal, clock, tick, open_pair = fee_runtime

    async def scenario():
        cycle = await open_pair()
        originals = {venue.key: venue.fetch_fee_fills for venue in (e, h)}

        async def no_history(*args, **kwargs):
            return []

        e.fetch_fee_fills = h.fetch_fee_fills = no_history
        await close_pair(fee_runtime)
        assert journal.fee_state(cycle['cycle_id'])['known_fees_usd'] == 0
        entered, release, fetched = asyncio.Event(), asyncio.Event(), asyncio.Event()
        slow, fast = (e, h) if slow_leg == 'entropy' else (h, e)

        async def slow_history(*args, **kwargs):
            entered.set()
            await release.wait()
            return await originals[slow.key](*args, **kwargs)

        async def fast_history(*args, **kwargs):
            rows = await originals[fast.key](*args, **kwargs)
            fetched.set()
            return rows

        slow.fetch_fee_fills, fast.fetch_fee_fills = slow_history, fast_history
        reconciling = asyncio.create_task(runtime._reconcile_cycle_accounting(cycle['cycle_id']))
        try:
            await asyncio.wait_for(entered.wait(), .3)
            await asyncio.wait_for(fetched.wait(), .3)
            assert not release.is_set()
            assert not reconciling.done()
            state = journal.fee_state(cycle['cycle_id'])
            assert state['known_fees_by_venue'][fast.key] == D('.04')
            assert state['known_fees_by_venue'][slow.key] == 0
            assert journal.db.execute('SELECT COUNT(*) FROM fee_fills WHERE cycle_id=?',
                                      (cycle['cycle_id'],)).fetchone()[0] == 2
            assert journal.funding_state(cycle['cycle_id'])['known_cash_result_usd'] == (
                gross_cash(runtime, journal, cycle) - D('.04'))
            assert cycle_result(journal, cycle['cycle_id'])['pnl_usd'] is None
            release.set()
            assert await asyncio.wait_for(reconciling, .3) is None
            assert journal.fee_state(cycle['cycle_id'])['known_fees_usd'] == D('.08')
        finally:
            release.set()
            await asyncio.gather(reconciling, return_exceptions=True)

    asyncio.run(scenario())


@pytest.mark.parametrize('slow_leg', ['entropy', 'hedge'])
def test_late_funding_updates_cash_and_budget_before_slow_fees_return(fee_runtime, slow_leg):
    runtime, e, h, journal, clock, tick, open_pair = fee_runtime

    async def scenario():
        cycle = await open_pair()
        await close_pair(fee_runtime)
        baseline = gross_cash(runtime, journal, cycle) - D('.08')
        assert journal.funding_state(cycle['cycle_id'])['known_cash_result_usd'] == baseline
        e.settlements.append(FundingSettlement('late-before-fees', 10001, D('-.25')))
        entered, release, booked = asyncio.Event(), asyncio.Event(), asyncio.Event()
        slow = e if slow_leg == 'entropy' else h
        original_history, original_poll = slow.fetch_fee_fills, runtime._poll_funding_settlements

        async def slow_history(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original_history(*args, **kwargs)

        async def funding_poll(active):
            state = await original_poll(active)
            booked.set()
            return state

        slow.fetch_fee_fills = slow_history
        runtime._poll_funding_settlements = funding_poll
        reconciling = asyncio.create_task(runtime._reconcile_cycle_accounting(cycle['cycle_id']))
        try:
            await asyncio.wait_for(entered.wait(), .3)
            await asyncio.wait_for(booked.wait(), .3)
            assert not release.is_set()
            assert not reconciling.done()
            state = journal.funding_state(cycle['cycle_id'])
            expected = baseline - D('.25')
            assert state['actual_funding_usd'] == D('-.25')
            assert state['known_cash_result_usd'] == expected
            assert journal.batch_state('batch')['loss_used_usd'] == max(-expected, D('0'))
            assert journal.db.execute('SELECT COUNT(*) FROM funding_settlements WHERE cycle_id=?',
                                      (cycle['cycle_id'],)).fetchone()[0] == 1
            release.set()
            assert await asyncio.wait_for(reconciling, .3) is None
            assert journal.funding_state(cycle['cycle_id'])['known_cash_result_usd'] == expected
            assert journal.batch_state('batch')['loss_used_usd'] == max(-expected, D('0'))
        finally:
            release.set()
            await asyncio.gather(reconciling, return_exceptions=True)

    asyncio.run(scenario())
