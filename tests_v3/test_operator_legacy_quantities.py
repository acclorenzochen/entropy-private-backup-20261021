"""Legacy intents may lack steps; only a durable same-cycle common grid can cover them."""
from contextlib import closing
from decimal import Decimal as D
import socket

import pytest

from entropy_arb.v3.core import Side
from entropy_arb.v3.journal import Intent, Journal, PendingOrder, Result


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('legacy quantity regression is offline')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


@pytest.fixture
def journal(tmp_path):
    with closing(Journal(tmp_path / 'legacy.sqlite', identity='offline-legacy-quantity')) as journal:
        journal.ensure_batch('batch', max_position_cycles=100, loss_budget_usd=D('3'))
        journal.create_live_cycle(batch_id='batch', cycle_id='cycle', direction='long_entropy',
            planned_units=10, reference_bps=D('0'), entry_basis_bps=D('-10'), started_ts=1)
        journal.db.execute("UPDATE live_cycles SET basis_profit_basis_json=NULL WHERE cycle_id='cycle'")
        yield journal


def filled(journal, ref, venue, side, *, legacy=False, units=10, reduce_only=False,
           grid=D('.01'), terminal=True, received_ts=9):
    intent = Intent(ref, 'cycle', venue, side, units, D('100'), reduce_only)
    if legacy:
        journal.prepare(intent)
    else:
        journal.prepare_dispatch(intent, step={'step_key': ref, 'quantity_step': grid})
    journal.record(ref, Result(terminal, units, D('100'), None),
                   timing={'terminal_result_ts': received_ts})


def legacy_round(journal, *, open_legacy=(True, True), close_legacy=False,
                 close_units=10, close_grids=(D('.01'), D('.01'))):
    filled(journal, 'seed-e', 'entropy', Side.BUY, legacy=open_legacy[0])
    filled(journal, 'seed-h', 'hedge', Side.SELL, legacy=open_legacy[1])
    filled(journal, 'close-e', 'entropy', Side.SELL, legacy=close_legacy,
           units=close_units, reduce_only=True, grid=close_grids[0])
    filled(journal, 'close-h', 'hedge', Side.BUY, legacy=close_legacy,
           units=close_units, reduce_only=True, grid=close_grids[1])
    journal.latch_basis_exit('cycle', requested_ts=8)
    journal.update_live_cycle('cycle', entropy_units=0, hedge_units=0,
        position_evidence_authoritative=True, last_authoritative_ts=10, updated_ts=10)


def mark_done(journal, **overrides):
    values = dict(phase='DONE', entropy_units=0, hedge_units=0,
        position_evidence_authoritative=True, last_authoritative_ts=10, updated_ts=11)
    values.update(overrides)
    journal.update_live_cycle('cycle', **values)


@pytest.mark.parametrize('open_legacy', [(True, True), (True, False), (False, True)])
def test_missing_legacy_steps_use_only_unique_same_cycle_filled_close_grid(journal, open_legacy):
    legacy_round(journal, open_legacy=open_legacy, close_grids=(D('.0100'), D('.01')))
    original_steps = journal.live_steps('cycle')
    journal.finish_cycle('cycle', kind='clean', net_pnl_usd=None,
        positions_verified_flat=True, all_orders_terminal=True, accounting_final=False)
    canonical_before = dict(journal.db.execute("SELECT * FROM cycles WHERE cycle_id='cycle'").fetchone())
    mark_done(journal)
    assert journal.active_live_cycle() is None
    assert journal.live_steps('cycle') == original_steps
    assert dict(journal.db.execute("SELECT * FROM cycles WHERE cycle_id='cycle'").fetchone()) == canonical_before
    assert canonical_before['pnl_usd'] is None and not canonical_before['complete_accounting']
    assert not canonical_before['accounting_final']
    assert journal.batch_state('batch')['block_reason'] == 'PENDING_ACCOUNTING'
    assert all(row['fee_usd'] is None for row in journal.cycle_orders('cycle'))
    assert journal.db.execute("SELECT basis_profit_basis_json FROM live_cycles WHERE cycle_id='cycle'").fetchone()[0] is None


def test_apply_last_close_can_finish_legacy_round_without_inventing_opening_steps(journal):
    legacy_round(journal)
    journal.apply_live_step('close-h', result_units=10, cash_delta_usd=D('-10'),
        accounting_final=False, entropy_units=0, hedge_units=0,
        next_phase='DONE', updated_ts=11, position_evidence_authoritative=True, last_authoritative_ts=10)
    assert journal.active_live_cycle() is None
    assert journal.live_step('close-h')['applied'] == 1
    assert {row['order_ref'] for row in journal.live_steps('cycle')} == {'close-e', 'close-h'}
    canonical = journal.db.execute("SELECT * FROM cycles WHERE cycle_id='cycle'").fetchone()
    assert canonical['pnl_usd'] is None and not canonical['accounting_final']


def test_all_missing_grids_cannot_be_supplied_by_current_metadata_or_other_cycle(journal):
    legacy_round(journal, close_legacy=True)
    journal.db.execute("INSERT INTO metadata(key,value) VALUES ('common_quantity_step','.01')")
    journal.db.execute('''INSERT INTO live_steps
        (step_key,cycle_id,order_ref,venue,side,requested_units,reduce_only,quantity_step)
        VALUES ('other-step','other-cycle','other-ref','entropy','sell',10,1,'.01')''')
    with pytest.raises(PendingOrder):
        mark_done(journal)
    assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'


@pytest.mark.parametrize('grid', [None, '0', '-.01', 'NaN', 'Infinity', 'bad'])
def test_existing_step_null_or_invalid_grid_is_never_treated_as_legacy_missing_row(journal, grid):
    legacy_round(journal)
    journal.db.execute("UPDATE live_steps SET quantity_step=? WHERE order_ref='close-e'", (grid,))
    with pytest.raises(PendingOrder):
        mark_done(journal)
    assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'


def test_conflicting_known_grids_cannot_be_hidden_by_balanced_common_unit_counts(journal):
    legacy_round(journal, close_grids=(D('.01'), D('.02')))
    assert sum(Side(row['side']).sign * row['filled_units'] for row in journal.cycle_orders('cycle')) == 0
    with pytest.raises(PendingOrder):
        mark_done(journal)
    assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'


@pytest.mark.parametrize('close_units', [8, 11])
def test_per_venue_conservation_cannot_be_replaced_by_cross_venue_net_zero(journal, close_units):
    legacy_round(journal, close_units=close_units)
    assert sum(Side(row['side']).sign * row['filled_units'] for row in journal.cycle_orders('cycle')) == 0
    with pytest.raises(PendingOrder, match='not conserved'):
        mark_done(journal)
    assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'


@pytest.mark.parametrize('fault', ['nonflat', 'non_authoritative', 'missing_authority', 'stale_authority', 'pending'])
def test_legacy_grid_fallback_does_not_weaken_flat_authority_or_global_pending(journal, fault):
    legacy_round(journal)
    values = {}
    if fault == 'nonflat':
        values['entropy_units'] = 1
    elif fault == 'non_authoritative':
        values['position_evidence_authoritative'] = False
    elif fault == 'missing_authority':
        journal.db.execute("UPDATE live_cycles SET last_authoritative_ts=NULL WHERE cycle_id='cycle'")
        values['last_authoritative_ts'] = None
    elif fault == 'stale_authority':
        values['last_authoritative_ts'] = 8
    elif fault == 'pending':
        journal.prepare(Intent('outside-unknown', 'other-cycle', 'hedge', Side.BUY, 1, D('100')))
    with pytest.raises(PendingOrder):
        mark_done(journal, **values)
    assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'


def test_nonterminal_legacy_order_still_blocks_done(journal):
    legacy_round(journal)
    journal.db.execute("UPDATE orders SET state='UNKNOWN' WHERE order_ref='seed-e'")
    with pytest.raises(PendingOrder):
        mark_done(journal)
    assert journal.active_live_cycle()['phase'] == 'BASIS_EXIT'
