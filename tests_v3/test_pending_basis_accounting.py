from decimal import Decimal as D
import socket

import pytest

from entropy_arb.v3.core import Side
from entropy_arb.v3.journal import Intent, Journal, PendingOrder, Result


RATES = {'entropy': D('10'), 'hedge': D('10')}
IDENTITY = 'offline-pending-basis'


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('pending-basis tests must remain offline')

    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


@pytest.fixture
def journal(tmp_path):
    with_journal = Journal(tmp_path / 'basis.sqlite', identity=IDENTITY)
    with_journal.ensure_batch('batch', max_position_cycles=100, loss_budget_usd=D('3'))
    yield with_journal
    with_journal.close()


def complete_round(journal, cycle_id='circle', *, batch_id='batch', start=10000,
                   end=10005, opening_units=(1,), entropy_fee=None, hedge_fee=None):
    journal.create_live_cycle(
        batch_id=batch_id, cycle_id=cycle_id, direction='long_entropy',
        planned_units=sum(opening_units), reference_bps=D('0'),
        entry_basis_bps=D('0'), started_ts=start)
    for venue, opening_side, fee in (
            ('entropy', Side.BUY, entropy_fee), ('hedge', Side.SELL, hedge_fee)):
        executions = [(f'open-{i}', units, opening_side, False)
                      for i, units in enumerate(opening_units)]
        executions.append(('close', sum(opening_units), opening_side.opposite, True))
        for suffix, units, side, reduce_only in executions:
            ref = f'{cycle_id}-{venue}-{suffix}'
            journal.ensure_live_step(
                step_key=ref, cycle_id=cycle_id, order_ref=ref, venue=venue,
                side=side.value, requested_units=units, reduce_only=reduce_only,
                quantity_step=D('1'))
            journal.prepare(Intent(ref, cycle_id, venue, side, units, D('100'),
                                   reduce_only=reduce_only))
            journal.record(ref, Result(True, units, D('100'), fee))
    journal.finish_cycle(
        cycle_id, kind='clean', net_pnl_usd=None, positions_verified_flat=True,
        all_orders_terminal=True, accounting_final=False)
    journal.update_live_cycle(
        cycle_id, phase='DONE', entropy_units=0, hedge_units=0,
        position_evidence_authoritative=True, last_authoritative_ts=end,
        updated_ts=end)
    return cycle_id


def cycle_result(journal, cycle_id='circle'):
    return dict(journal.db.execute('SELECT * FROM cycles WHERE cycle_id=?',
                                   (cycle_id,)).fetchone())


def assert_blocked(journal, cycle_id='circle'):
    assert not journal.estimate_basis_accounting(cycle_id, fee_bps=RATES)
    state = journal.batch_state('batch')
    assert not state['can_open']
    assert state['blocking_pending_accounting'] == 1
    with pytest.raises(PendingOrder):
        journal.begin_cycle('batch', 'next')


def test_missing_fees_estimate_positive_cost_without_inventing_net_pnl(journal):
    complete_round(journal, entropy_fee=D('.02'))
    for _ in range(3):
        assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
        state = journal.batch_state('batch')
        assert state['can_open']
        assert state['known_loss_used_usd'] == D('.04')
        assert state['pending_cost_estimate_usd'] == D('.2')
        assert state['loss_used_usd'] == D('.24')
        assert state['pending_accounting'] == 1
        assert state['blocking_pending_accounting'] == 0
    row = cycle_result(journal)
    assert row['pnl_usd'] is None
    assert row['complete_accounting'] == row['accounting_final'] == 0
    assert all(row['fee_usd'] is None for row in journal.cycle_orders('circle')
               if row['venue'] == 'hedge')
    journal.create_live_cycle(
        batch_id='batch', cycle_id='next', direction='long_entropy',
        planned_units=1, reference_bps=D('0'), entry_basis_bps=D('0'),
        started_ts=10010)


@pytest.mark.parametrize('values', [
    {'entropy_units': 1}, {'hedge_units': -1},
    {'position_evidence_authoritative': False}, {'phase': 'RECOVERY'},
])
def test_unknown_or_nonflat_position_never_allows_pending_reentry(journal, values):
    complete_round(journal)
    journal.update_live_cycle('circle', updated_ts=10006, **values)
    assert_blocked(journal)


def test_unknown_order_never_allows_pending_reentry(journal):
    complete_round(journal)
    journal.db.execute("UPDATE orders SET state='UNKNOWN' WHERE order_ref=?",
                       ('circle-hedge-close',))
    assert_blocked(journal)


@pytest.mark.parametrize('table,column,value', [
    ('live_steps', 'quantity_step', None), ('live_steps', 'quantity_step', '0'),
    ('orders', 'avg_px', None), ('orders', 'avg_px', '0'),
])
def test_missing_or_invalid_persisted_grid_and_price_block(journal, table, column, value):
    complete_round(journal)
    journal.db.execute(f'UPDATE {table} SET {column}=? WHERE order_ref=?',
                       (value, 'circle-hedge-close'))
    assert_blocked(journal)


def test_equal_integer_units_cannot_hide_different_base_quantities(journal):
    complete_round(journal)
    journal.db.execute('UPDATE live_steps SET quantity_step=? WHERE order_ref=?',
                       ('2', 'circle-hedge-close'))
    assert_blocked(journal)


@pytest.mark.parametrize('start,end', [
    (-1, 10005), (10000, None), (10000, 9999),
    (10000, float('nan')), (10000, float('inf')),
])
def test_missing_negative_or_nonfinite_exposure_window_blocks(journal, start, end):
    complete_round(journal)
    journal.db.execute('UPDATE live_cycles SET started_ts=?,last_authoritative_ts=? '
                       'WHERE cycle_id=?', (start, end, 'circle'))
    assert_blocked(journal)


def test_crossing_hour_adds_no_arbitrary_funding_estimate(journal):
    complete_round(journal, start=3599, end=3601, entropy_fee=D('0'), hedge_fee=D('0'))
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    row = cycle_result(journal)
    assert D(row['pending_cost_estimate_usd']) == 0
    assert row['accounting_final'] == 0


def test_configured_estimate_counts_multiple_opening_orders(journal):
    complete_round(journal, opening_units=(1, 2))
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    assert journal.batch_state('batch')['pending_cost_estimate_usd'] == D('1.2')


def test_zero_configured_estimate_preserves_unknown_actual_fee(journal):
    complete_round(journal)
    assert journal.estimate_basis_accounting(
        'circle', fee_bps={'entropy': D('0'), 'hedge': D('0')})
    state = journal.batch_state('batch')
    assert state['can_open'] and state['pending_cost_estimate_usd'] == 0
    assert not cycle_result(journal)['accounting_final']
    assert cycle_result(journal)['pnl_usd'] is None
    assert all(row['fee_usd'] is None for row in journal.cycle_orders('circle'))


def test_higher_configured_fee_rate_is_not_discarded(journal):
    complete_round(journal)
    rates = {'entropy': D('20'), 'hedge': D('0')}
    assert journal.estimate_basis_accounting('circle', fee_bps=rates)
    assert journal.batch_state('batch')['pending_cost_estimate_usd'] == D('.4')


def test_fee_enrichment_replaces_known_cost_and_releases_estimate_once(journal):
    complete_round(journal, entropy_fee=D('.02'))
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    for suffix, estimate, loss in [('open-0', '.1', '.07'), ('close', '0', '.1')]:
        ref = f'circle-hedge-{suffix}'
        for _ in range(2):
            journal.record(ref, Result(True, 1, D('100'), D('.03')))
            assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
            state = journal.batch_state('batch')
            assert state['known_loss_used_usd'] == D(loss)
            assert state['pending_cost_estimate_usd'] == D(estimate)
            assert state['loss_used_usd'] == D(loss) + D(estimate)
            assert cycle_result(journal)['pnl_usd'] is None
            assert not cycle_result(journal)['accounting_final']
    journal.finalize_cycle_accounting('circle', D('-.1'))
    assert journal.batch_state('batch')['loss_used_usd'] == D('.1')


def test_proven_zero_fill_needs_no_fee_or_funding_estimate(journal):
    journal.create_live_cycle(
        batch_id='batch', cycle_id='circle', direction='long_entropy', planned_units=1,
        reference_bps=D('0'), entry_basis_bps=D('0'), started_ts=10000)
    journal.finish_cycle(
        'circle', kind='zero_fill', net_pnl_usd=None, positions_verified_flat=True,
        all_orders_terminal=True, accounting_final=False)
    journal.update_live_cycle(
        'circle', phase='DONE', position_evidence_authoritative=True, updated_ts=10001)
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    state = journal.batch_state('batch')
    assert state['can_open'] and state['position_cycles'] == 0
    assert state['loss_used_usd'] == state['pending_cost_estimate_usd'] == 0
    assert cycle_result(journal)['pnl_usd'] is None


def test_new_batch_cannot_evade_unestimated_accounting(journal):
    complete_round(journal)
    journal.ensure_batch('other', max_position_cycles=10, loss_budget_usd=D('3'))
    with pytest.raises(PendingOrder):
        journal.create_live_cycle(
            batch_id='other', cycle_id='next', direction='long_entropy', planned_units=1,
            reference_bps=D('0'), entry_basis_bps=D('0'), started_ts=10010)
    assert journal.batch_state('other')['attempts'] == 0


@pytest.mark.parametrize('entry', ['begin', 'live'])
def test_new_batch_cannot_evade_known_loss_with_zero_configured_estimate(journal, entry):
    complete_round(journal, entropy_fee=D('1.6'))
    assert journal.estimate_basis_accounting(
        'circle', fee_bps={'entropy': D('0'), 'hedge': D('0')})
    state = journal.batch_state('batch')
    assert state['known_loss_used_usd'] == D('3.2')
    assert state['pending_cost_estimate_usd'] == 0
    assert not state['can_open']
    journal.ensure_batch('other', max_position_cycles=10, loss_budget_usd=D('3'))
    with pytest.raises(PendingOrder):
        if entry == 'begin':
            journal.begin_cycle('other', 'next')
        else:
            journal.create_live_cycle(
                batch_id='other', cycle_id='next', direction='long_entropy', planned_units=1,
                reference_bps=D('0'), entry_basis_bps=D('0'), started_ts=10010)


@pytest.mark.parametrize('entry', ['begin', 'live'])
def test_new_batch_cannot_evade_exhausted_pending_estimates(journal, entry):
    journal.ensure_batch('small', max_position_cycles=10, loss_budget_usd=D('.39'))
    complete_round(journal, batch_id='small')
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    state = journal.batch_state('small')
    assert state['block_reason'] == 'PENDING_ACCOUNTING_ESTIMATE_BUDGET'
    assert not state['closed_for_entries']
    with pytest.raises(PendingOrder):
        if entry == 'begin':
            journal.begin_cycle('batch', 'next')
        else:
            journal.create_live_cycle(
                batch_id='batch', cycle_id='next', direction='long_entropy',
                planned_units=1, reference_bps=D('0'), entry_basis_bps=D('0'),
                started_ts=10010)
    assert journal.batch_state('batch')['attempts'] == 0


def test_final_accounting_replaces_estimate_and_repeated_finalization_is_idempotent(journal):
    complete_round(journal)
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    for _ in range(3):
        journal.finalize_cycle_accounting('circle', D('-.07'))
        state = journal.batch_state('batch')
        assert state['loss_used_usd'] == state['known_loss_used_usd'] == D('.07')
        assert state['pending_cost_estimate_usd'] == 0
        assert state['pending_accounting'] == 0
    row = cycle_result(journal)
    assert row['pending_known_cash_usd'] is row['pending_cost_estimate_usd'] is None
    assert row['accounting_final'] == 1
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    assert journal.batch_state('batch')['loss_used_usd'] == D('.07')


def test_finalizing_one_circle_cannot_latch_other_circle_cost_estimate(journal):
    journal.ensure_batch('small', max_position_cycles=10, loss_budget_usd=D('.7'))
    complete_round(journal, 'first', batch_id='small')
    assert journal.estimate_basis_accounting('first', fee_bps=RATES)
    complete_round(journal, 'second', batch_id='small')
    assert journal.estimate_basis_accounting('second', fee_bps=RATES)
    assert journal.batch_state('small')['pending_cost_estimate_usd'] == D('.8')
    journal.finalize_cycle_accounting('first', D('-.35'))
    state = journal.batch_state('small')
    assert state['loss_used_usd'] == D('.75')
    assert state['block_reason'] == 'PENDING_ACCOUNTING_ESTIMATE_BUDGET'
    assert not state['closed_for_entries']
    journal.finalize_cycle_accounting('second', D('1'))
    state = journal.batch_state('small')
    assert state['can_open']
    assert state['loss_used_usd'] == D('.35')
    assert not state['closed_for_entries']


def test_actual_loss_still_latches_and_late_correction_does_not_rearm(journal):
    complete_round(journal)
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    journal.finalize_cycle_accounting('circle', D('-3'))
    assert journal.batch_state('batch')['close_reason'] == 'REALIZED_LOSS_BUDGET'
    journal.finalize_cycle_accounting('circle', D('1'))
    assert journal.batch_state('batch')['close_reason'] == 'REALIZED_LOSS_BUDGET'
    assert not journal.batch_state('batch')['can_open']


def test_active_drawdown_includes_pending_cost_estimate(journal):
    complete_round(journal)
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    assert journal.batch_state('batch')['pending_cost_estimate_usd'] == D('.4')
    assert journal.observe_open_risk(
        'batch', active_cycle_net_pnl_usd=D('-2.59'), cycle_id='next')
    assert not journal.observe_open_risk(
        'batch', active_cycle_net_pnl_usd=D('-2.6'), cycle_id='next')
    state = journal.batch_state('batch')
    assert state['close_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
    assert D(state['block_threshold']) == D('-2.6')


def test_existing_manual_stop_is_pestimated_after_estimate_and_final_accounting(journal):
    complete_round(journal)
    journal.close_entries('batch', 'MANUAL_STOP')
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    journal.finalize_cycle_accounting('circle', D('1'))
    state = journal.batch_state('batch')
    assert state['close_reason'] == state['block_reason'] == 'MANUAL_STOP'
    assert state['closed_for_entries'] and not state['can_open']


def test_pending_estimates_survive_restart_without_claiming_finality(tmp_path):
    path = tmp_path / 'restart.sqlite'
    journal = Journal(path, identity=IDENTITY)
    journal.ensure_batch('batch', max_position_cycles=100, loss_budget_usd=D('3'))
    complete_round(journal)
    assert journal.estimate_basis_accounting('circle', fee_bps=RATES)
    before = journal.batch_state('batch')
    journal.close()
    restarted = Journal(path, identity=IDENTITY)
    try:
        assert restarted.batch_state('batch') == before
        assert restarted.estimate_basis_accounting('circle', fee_bps=RATES)
        assert restarted.batch_state('batch') == before
        assert cycle_result(restarted)['pnl_usd'] is None
        assert cycle_result(restarted)['accounting_final'] == 0
        restarted.begin_cycle('batch', 'next')
    finally:
        restarted.close()
