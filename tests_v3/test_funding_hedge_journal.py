from contextlib import closing
from decimal import Decimal as D
import math
import sqlite3
from types import SimpleNamespace

import pytest

from entropy_arb.v3.journal import Journal, PendingOrder


IDENTITY = 'offline-nbis-funding'
HEDGED_TS = 7200.0


def new_journal(path, *, loss_budget_usd=D('10')):
    journal = Journal(path, identity=IDENTITY)
    journal.ensure_batch('batch', max_position_cycles=5, loss_budget_usd=loss_budget_usd)
    create_cycle(journal, 'cycle')
    return journal


def create_cycle(journal, cycle_id):
    journal.create_live_cycle(
        batch_id='batch', cycle_id=cycle_id, direction='long_entropy',
        planned_units=10, reference_bps=D('0'), entry_basis_bps=D('-4'),
        started_ts=3600.0)


def ready_funding_round(journal):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    for venue in ('entropy', 'hedge'):
        journal.record_funding_settlements('cycle', venue, [
            settlement(venue + str(ts), ts) for ts in (10800, 14400)])
    journal.set_funding_exit('cycle', reason='two_settlements_each', completed=True)
    journal.update_live_cycle('cycle', position_evidence_authoritative=True,
                              last_authoritative_ts=14401, updated_ts=14401)
    journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('0'),
                         positions_verified_flat=True, all_orders_terminal=True,
                         accounting_final=False, conservative_pnl_bound=True)
    journal.update_live_cycle('cycle', phase='DONE', updated_ts=14401)
    journal.record_funding_poll('cycle', polled_ts=14401, entropy_ok=True,
                                hedge_ok=True, closed_window=True)
    journal.refresh_funding_pnl('cycle', trade_cash_net_usd=D('0'),
                                trade_accounting_complete=True)


@pytest.fixture
def journal(tmp_path):
    with closing(new_journal(tmp_path / 'funding.sqlite')) as journal:
        yield journal


def settlement(event_id, ts, amount='0', *, position_qty=None, source='history'):
    return SimpleNamespace(event_id=event_id, ts=ts, amount_usd=D(amount),
                           position_qty=position_qty, source=source)


def test_state_requires_live_cycle_and_initialization(journal, tmp_path):
    assert journal.funding_state('cycle') is None
    assert journal.funding_state('missing') is None
    with pytest.raises(KeyError):
        journal.ensure_funding_cycle('missing', hedged_ts=HEDGED_TS)
    with pytest.raises(KeyError):
        journal.record_funding_settlements('cycle', 'entropy', [])
    with pytest.raises(KeyError):
        journal.record_funding_sample('cycle', sample_ts=HEDGED_TS, net_usd_per_hour=None)
    with pytest.raises(KeyError):
        journal.set_funding_exit('cycle', reason='early', completed=False)
    with closing(Journal(tmp_path / 'not-live.sqlite', identity=IDENTITY)) as other:
        other.ensure_batch('batch', max_position_cycles=5, loss_budget_usd=D('10'))
        other.begin_cycle('batch', 'only-in-cycles')
        with pytest.raises(KeyError):
            other.ensure_funding_cycle('only-in-cycles', hedged_ts=HEDGED_TS)


def test_empty_history_and_elapsed_clock_never_create_settlements(journal, monkeypatch):
    state = journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    assert state['hedged_ts'] == HEDGED_TS
    assert state['negative_sample_count'] == 0
    assert state['negative_since_ts'] is state['last_sample_ts'] is None
    assert state['last_net_usd_per_hour'] is state['exit_reason'] is None
    assert state['last_complete_poll_ts'] is None
    assert state['trade_cash_net_usd'] is None
    assert state['completed'] is False
    monkeypatch.setattr('entropy_arb.v3.journal.time.time', lambda: HEDGED_TS + 86400)
    for venue in ('entropy', 'hedge'):
        journal.record_funding_settlements('cycle', venue, [])
    assert journal.funding_state('cycle') == state
    assert state['settlement_counts'] == {'entropy': 0, 'hedge': 0}
    assert state['funding_by_venue'] == {'entropy': D('0'), 'hedge': D('0')}
    assert isinstance(state['actual_funding_usd'], D)
    assert journal.db.execute('SELECT COUNT(*) FROM funding_settlements').fetchone()[0] == 0


def test_provisional_normal_funding_round_allows_reentry_without_finality(journal):
    ready_funding_round(journal)
    state = journal.batch_state('batch')
    assert state['can_open'] and state['position_cycles'] == 1
    assert state['pending_accounting'] == 1 and state['blocking_pending_accounting'] == 0
    assert journal.provisional_cycles('batch')[0]['accounting_final'] == 0
    assert journal.funding_state('cycle')['closed_poll_complete']
    assert journal.funding_state('cycle')['trade_accounting_complete']
    create_cycle(journal, 'next')
    assert journal.batch_state('batch')['position_cycles'] == 2


def test_completion_label_cannot_substitute_for_real_settlement_records(journal):
    ready_funding_round(journal)
    journal.db.execute("DELETE FROM funding_settlements WHERE venue='hedge'")
    assert journal.funding_state('cycle')['completed']
    assert journal.batch_state('batch')['block_reason'] == 'PENDING_ACCOUNTING'
    with pytest.raises(PendingOrder):
        create_cycle(journal, 'next')


def test_legacy_funding_schema_defaults_to_blocked_until_reverified(tmp_path):
    path = tmp_path / 'legacy-funding.sqlite'
    with closing(new_journal(path)) as journal:
        ready_funding_round(journal)
        for column in ('trade_accounting_complete', 'closed_poll_complete'):
            journal.db.execute(f'ALTER TABLE funding_cycles DROP COLUMN {column}')
    with closing(Journal(path, identity=IDENTITY)) as journal:
        state = journal.funding_state('cycle')
        assert not state['trade_accounting_complete'] and not state['closed_poll_complete']
        assert journal.batch_state('batch')['block_reason'] == 'PENDING_ACCOUNTING'
        journal.refresh_funding_pnl('cycle', trade_cash_net_usd=D('0'),
                                    trade_accounting_complete=True)
        assert not journal.batch_state('batch')['can_open']
        journal.record_funding_poll('cycle', polled_ts=14402, entropy_ok=True,
                                    hedge_ok=True, closed_window=True)
        assert journal.batch_state('batch')['can_open']


def test_verified_funding_reentry_evidence_survives_restart_without_resetting_batch(tmp_path):
    path = tmp_path / 'reentry.sqlite'
    with closing(new_journal(path)) as journal:
        ready_funding_round(journal)
    with closing(Journal(path, identity=IDENTITY)) as journal:
        journal.ensure_batch('batch', max_position_cycles=5, loss_budget_usd=D('10'))
        assert journal.batch_state('batch')['can_open']
        assert journal.batch_state('batch')['position_cycles'] == 1
        with pytest.raises(ValueError, match='reset/enlarge'):
            journal.ensure_batch('batch', max_position_cycles=6, loss_budget_usd=D('10'))


def test_basis_provisional_accounting_still_blocks_entry(journal):
    journal.update_live_cycle('cycle', phase='DONE', updated_ts=14401)
    journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('0'),
                         positions_verified_flat=True, all_orders_terminal=True,
                         accounting_final=False, conservative_pnl_bound=True)
    state = journal.batch_state('batch')
    assert state['pending_accounting'] == state['blocking_pending_accounting'] == 1
    assert not state['can_open']
    with pytest.raises(PendingOrder):
        create_cycle(journal, 'next')


def test_zero_real_settlement_counts_but_missing_venue_does_not(journal):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    journal.record_funding_settlements('cycle', 'entropy', [settlement('zero', 10800)])
    state = journal.funding_state('cycle')
    assert state['settlement_counts'] == {'entropy': 1, 'hedge': 0}
    assert state['actual_funding_usd'] == D('0')
    assert journal.db.execute('SELECT COUNT(*) FROM funding_settlements').fetchone()[0] == 1


def test_strict_hedge_cutoff_keeps_all_real_cash_and_raw_details(journal):
    journal.update_live_cycle('cycle', cash_delta_usd=D('9.25'), updated_ts=HEDGED_TS)
    cash_before = journal.active_live_cycle()['cash_net_usd']
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    journal.record_funding_settlements('cycle', 'entropy', [
        settlement('before-hedge', 7199, '-0.25', position_qty=D('2'), source='venue-api'),
        settlement('at-hedge', HEDGED_TS, '0.125'),
        settlement('after-hedge', math.nextafter(HEDGED_TS, math.inf), '0.50'),
    ])
    journal.record_funding_settlements('cycle', 'hedge', [
        settlement('hedge-before', 7198, '-0.1'),
        settlement('hedge-after', 10800, '0.2', position_qty=D('-2')),
    ])
    state = journal.funding_state('cycle')
    assert state['settlement_counts'] == {'entropy': 1, 'hedge': 1}
    assert state['funding_by_venue'] == {'entropy': D('0.375'), 'hedge': D('0.1')}
    assert state['actual_funding_usd'] == D('0.475')
    row = dict(journal.db.execute(
        "SELECT * FROM funding_settlements WHERE event_id='before-hedge'").fetchone())
    assert row == {'venue': 'entropy', 'event_id': 'before-hedge', 'cycle_id': 'cycle',
                   'ts': 7199, 'amount_usd': '-0.25', 'position_qty': '2', 'source': 'venue-api'}
    assert journal.active_live_cycle()['cash_net_usd'] == cash_before
    assert journal.economics_rows()[0]['actual_funding_usd'] is None


def test_same_hour_multiple_entries_and_delayed_history_count_real_periods(journal, monkeypatch):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    monkeypatch.setattr('entropy_arb.v3.journal.time.time', lambda: 1000000)
    journal.record_funding_settlements('cycle', 'entropy', [
        settlement('a', 7201, '1'), settlement('b', 7210, '-0.2'),
        settlement('c', 10799.999, '0'), settlement('next-hour', 10800, '0.3'),
    ])
    state = journal.funding_state('cycle')
    assert state['settlement_counts'] == {'entropy': 2, 'hedge': 0}
    assert state['actual_funding_usd'] == D('1.1')
    journal.record_funding_settlements('cycle', 'hedge', [settlement('late-first', 10800)])
    assert journal.funding_state('cycle')['settlement_counts'] == {'entropy': 2, 'hedge': 1}


def test_duplicate_identity_and_all_state_survive_restart(tmp_path):
    path = tmp_path / 'restart.sqlite'
    event = settlement('same-id', 10800, '0.00125', position_qty=D('10'))
    with closing(new_journal(path)) as journal:
        journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
        journal.record_funding_settlements('cycle', 'entropy', [event, event])
        journal.record_funding_settlements('cycle', 'hedge', [event])
        journal.record_funding_sample('cycle', sample_ts=HEDGED_TS, net_usd_per_hour=D('-1'))
        journal.set_funding_exit('cycle', reason='NEGATIVE_FUNDING', completed=False)
        before = journal.funding_state('cycle')
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS + 99999) == before
        journal.record_funding_settlements('cycle', 'entropy', [event])
        journal.record_funding_settlements('cycle', 'hedge', [event])
        assert journal.funding_state('cycle') == before
        conflict = SimpleNamespace(**(vars(event) | {'amount_usd': D('0.01')}))
        with pytest.raises(ValueError, match='changed'):
            journal.record_funding_settlements('cycle', 'entropy', [conflict])
        assert journal.funding_state('cycle') == before
        assert before['actual_funding_usd'] == D('0.00250')
        assert before['settlement_counts'] == {'entropy': 1, 'hedge': 1}
        assert journal.db.execute('SELECT COUNT(*) FROM funding_settlements').fetchone()[0] == 2


@pytest.mark.parametrize('changes', [
    {'amount_usd': D('2')}, {'ts': 10801}, {'position_qty': D('11')},
    {'position_qty': None}, {'source': 'changed-source'},
])
def test_changed_duplicate_rejected_and_whole_batch_rolled_back(journal, changes):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    event = settlement('immutable', 10800, '1', position_qty=D('10'))
    journal.record_funding_settlements('cycle', 'entropy', [event])
    before = journal.funding_state('cycle')
    conflict = SimpleNamespace(**(vars(event) | changes))
    with pytest.raises(ValueError, match='changed'):
        journal.record_funding_settlements('cycle', 'entropy', [
            settlement('must-rollback', 14400, '5'), conflict])
    assert journal.funding_state('cycle') == before
    assert journal.db.execute('SELECT COUNT(*) FROM funding_settlements').fetchone()[0] == 1
    # Numeric equality permits Decimal representations of the same value.
    equivalent = SimpleNamespace(**(vars(event) | {'amount_usd': D('1.00')}))
    journal.record_funding_settlements('cycle', 'entropy', [equivalent])


def test_event_cannot_be_reassigned_to_another_cycle(journal):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    event = settlement('account-event', 10800, '1')
    journal.record_funding_settlements('cycle', 'entropy', [event])
    journal.update_live_cycle('cycle', phase='DONE', updated_ts=14400)
    journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('1'),
                         positions_verified_flat=True, all_orders_terminal=True)
    create_cycle(journal, 'next-cycle')
    journal.ensure_funding_cycle('next-cycle', hedged_ts=HEDGED_TS)
    with pytest.raises(ValueError, match='changed'):
        journal.record_funding_settlements('next-cycle', 'entropy', [event])
    assert journal.funding_state('cycle')['actual_funding_usd'] == D('1')
    assert journal.funding_state('next-cycle')['settlement_counts']['entropy'] == 0


@pytest.mark.parametrize('venue', ['lighter', 'hl', '', 'Entropy', None])
def test_only_two_leg_venue_keys_are_accepted(journal, venue):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    with pytest.raises(ValueError, match='venue'):
        journal.record_funding_settlements('cycle', venue, [])


@pytest.mark.parametrize('bad', [float('nan'), float('inf'), -float('inf'), -1, True, None, '7200'])
def test_invalid_times_are_rejected_without_changing_state(journal, bad):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    before = journal.funding_state('cycle')
    if bad is not None:
        with pytest.raises(ValueError):
            journal.ensure_funding_cycle('cycle', hedged_ts=bad)
    with pytest.raises(ValueError):
        journal.record_funding_settlements('cycle', 'entropy', [settlement('bad-ts', bad)])
    with pytest.raises(ValueError):
        journal.record_funding_sample('cycle', sample_ts=bad, net_usd_per_hour=D('-1'))
    with pytest.raises(ValueError):
        journal.record_funding_sample('cycle', sample_ts=HEDGED_TS,
                                      net_usd_per_hour=D('-1'), max_gap_seconds=bad)
    with pytest.raises(ValueError):
        journal.record_funding_poll('cycle', polled_ts=bad, entropy_ok=True, hedge_ok=True)
    assert journal.funding_state('cycle') == before


@pytest.mark.parametrize('field', ['amount_usd', 'position_qty'])
@pytest.mark.parametrize('bad', [D('NaN'), D('sNaN'), D('Infinity'), D('-Infinity'), True])
def test_nonfinite_settlement_numbers_rollback_batch(journal, field, bad):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    event = settlement('invalid', 10800)
    setattr(event, field, bad)
    with pytest.raises(ValueError):
        journal.record_funding_settlements('cycle', 'entropy', [
            settlement('must-rollback', 10800, '1'), event])
    assert journal.funding_state('cycle')['settlement_counts'] == {'entropy': 0, 'hedge': 0}
    assert journal.db.execute('SELECT COUNT(*) FROM funding_settlements').fetchone()[0] == 0


def test_ledger_supports_minimal_ducktype_and_rolls_back_baseexception(journal):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    minimal = SimpleNamespace(event_id='minimal', ts=10800, amount_usd=D('0'))
    journal.record_funding_settlements('cycle', 'entropy', [minimal])
    row = journal.db.execute('SELECT position_qty,source FROM funding_settlements').fetchone()
    assert tuple(row) == (None, '')

    def interrupted():
        yield settlement('uncommitted', 14400, '1')
        raise KeyboardInterrupt('offline interruption')

    with pytest.raises(KeyboardInterrupt):
        journal.record_funding_settlements('cycle', 'entropy', interrupted())
    assert not journal.db.in_transaction
    assert journal.funding_state('cycle')['actual_funding_usd'] == D('0')
    assert journal.db.execute('SELECT COUNT(*) FROM funding_settlements').fetchone()[0] == 1


@pytest.mark.parametrize('reset_value', [None, D('0'), D('1')])
def test_invalid_or_nonnegative_sample_resets_only_streak(journal, reset_value):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    journal.record_funding_settlements('cycle', 'entropy', [settlement('retained', 10800, '2')])
    for stamp in (7200, 7230):
        journal.record_funding_sample('cycle', sample_ts=stamp, net_usd_per_hour=D('-1'))
    state = journal.record_funding_sample('cycle', sample_ts=7260, net_usd_per_hour=reset_value)
    assert state['negative_sample_count'] == 0
    assert state['negative_since_ts'] is None
    assert state['last_sample_ts'] == 7260
    assert state['last_net_usd_per_hour'] == reset_value
    assert state['actual_funding_usd'] == D('2')
    state = journal.record_funding_sample('cycle', sample_ts=7290, net_usd_per_hour=D('-0.5'))
    assert state['negative_sample_count'] == 1
    assert state['negative_since_ts'] == 7290


def test_duplicate_old_samples_and_strict_gap_boundary(journal):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    first = journal.record_funding_sample('cycle', sample_ts=7200, net_usd_per_hour=D('-1'))
    for stamp, net in ((7200, None), (7199, D('1')), (7200, D('-2'))):
        assert journal.record_funding_sample('cycle', sample_ts=stamp, net_usd_per_hour=net) == first
    state = journal.record_funding_sample('cycle', sample_ts=7245, net_usd_per_hour=D('-2'))
    assert state['negative_sample_count'] == 2
    assert state['negative_since_ts'] == 7200
    stamp = math.nextafter(7290, math.inf)
    state = journal.record_funding_sample('cycle', sample_ts=stamp, net_usd_per_hour=D('-3'))
    assert state['negative_sample_count'] == 1
    assert state['negative_since_ts'] == stamp
    state = journal.record_funding_sample('cycle', sample_ts=stamp + 60,
                                          net_usd_per_hour=D('-4'), max_gap_seconds=60)
    assert state['negative_sample_count'] == 2
    assert state['negative_since_ts'] == stamp


def test_three_negative_samples_over_sixty_seconds_survive_multiple_restarts(tmp_path):
    path = tmp_path / 'streak.sqlite'
    with closing(new_journal(path)) as journal:
        journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
        journal.record_funding_sample('cycle', sample_ts=7200, net_usd_per_hour=D('-0.1'))
    for stamp, count in ((7230, 2), (7260, 3)):
        with closing(Journal(path, identity=IDENTITY)) as journal:
            before = journal.funding_state('cycle')
            assert journal.ensure_funding_cycle('cycle', hedged_ts=stamp + 100) == before
            assert journal.record_funding_sample(
                'cycle', sample_ts=before['last_sample_ts'], net_usd_per_hour=None) == before
            state = journal.record_funding_sample('cycle', sample_ts=stamp,
                                                  net_usd_per_hour=D('-0.2'))
            assert state['negative_sample_count'] == count
            assert state['negative_since_ts'] == 7200
    with closing(Journal(path, identity=IDENTITY)) as journal:
        state = journal.funding_state('cycle')
        assert state['negative_sample_count'] == 3
        assert state['last_sample_ts'] - state['negative_since_ts'] == 60
        assert state['last_net_usd_per_hour'] == D('-0.2')
        assert isinstance(state['last_net_usd_per_hour'], D)


@pytest.mark.parametrize('completed', [False, True])
def test_exit_outcome_survives_restart_without_cash_or_streak_changes(tmp_path, completed):
    path = tmp_path / 'exit.sqlite'
    reason = 'FUNDING_TWO_SETTLEMENTS' if completed else 'NEGATIVE_FUNDING'
    with closing(new_journal(path)) as journal:
        journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
        for venue in ('entropy', 'hedge'):
            events = [settlement('one', 10800)]
            if completed:
                events.append(settlement('two', 14400))
            journal.record_funding_settlements('cycle', venue, events)
        journal.record_funding_sample('cycle', sample_ts=7200, net_usd_per_hour=D('-1'))
        live_before = journal.active_live_cycle()
        journal.set_funding_exit('cycle', reason=reason, completed=completed)
        assert journal.active_live_cycle() == live_before
        state = journal.funding_state('cycle')
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.ensure_funding_cycle('cycle', hedged_ts=99999) == state
        assert state['completed'] is completed
        assert state['exit_reason'] == reason
        assert state['negative_sample_count'] == 1


def test_schema8_additive_tables_preserve_existing_data_and_single_writer(tmp_path):
    path = tmp_path / 'schema8.sqlite'
    with closing(new_journal(path)) as journal:
        before = journal.active_live_cycle()
        journal.db.execute('DROP TABLE funding_settlements')
        journal.db.execute('DROP TABLE funding_cycles')
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.active_live_cycle() == before
        assert journal.db.execute(
            "SELECT value FROM metadata WHERE key='economics_schema_version'").fetchone()[0] == '8'
        journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
        journal.record_funding_settlements('cycle', 'entropy', [settlement('one', 10800, '1')])
        with pytest.raises(RuntimeError, match='writer'):
            Journal(path, identity=IDENTITY)
        # A read-only consumer sees both newly persisted tables while writer owns the lock.
        with closing(sqlite3.connect(f'{path.as_uri()}?mode=ro', uri=True)) as reader:
            assert reader.execute('SELECT hedged_ts FROM funding_cycles').fetchone()[0] == HEDGED_TS
            assert reader.execute('SELECT amount_usd FROM funding_settlements').fetchone()[0] == '1'
            assert reader.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'


def test_partial_cycle_cash_and_exit_persist_until_hedged_time_set_once(tmp_path):
    path = tmp_path / 'partial.sqlite'
    with closing(new_journal(path)) as journal:
        assert journal.ensure_funding_cycle('cycle')['hedged_ts'] is None
        journal.record_funding_settlements('cycle', 'entropy', [
            settlement('partial-funding', 7199, '-0.2'), settlement('later', 10800, '0.3')])
        journal.record_funding_sample('cycle', sample_ts=7200, net_usd_per_hour=D('-1'))
        journal.record_funding_poll('cycle', polled_ts=14400, entropy_ok=True, hedge_ok=True)
        journal.set_funding_exit('cycle', reason='PARTIAL_RISK_EXIT', completed=False)
        before = journal.funding_state('cycle')
        assert before['settlement_counts'] == {'entropy': 0, 'hedge': 0}
        assert before['actual_funding_usd'] == D('0.1')
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.ensure_funding_cycle('cycle', hedged_ts=None) == before
        hedged = journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
        assert hedged['settlement_counts'] == {'entropy': 1, 'hedge': 0}
        assert hedged['hedged_ts'] == HEDGED_TS
        for field in ('actual_funding_usd', 'negative_sample_count', 'negative_since_ts',
                      'last_sample_ts', 'last_net_usd_per_hour', 'exit_reason',
                      'completed', 'last_complete_poll_ts'):
            assert hedged[field] == before[field]
        assert journal.ensure_funding_cycle('cycle', hedged_ts=99999) == hedged
        assert journal.ensure_funding_cycle('cycle') == hedged
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.funding_state('cycle') == hedged


def test_complete_poll_is_monotonic_diagnostic_only_and_survives_restart(tmp_path):
    path = tmp_path / 'poll.sqlite'
    with closing(new_journal(path)) as journal:
        with pytest.raises(KeyError):
            journal.record_funding_poll('cycle', polled_ts=7200, entropy_ok=True, hedge_ok=True)
        journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
        for outcomes in ((False, False), (True, False), (False, True)):
            journal.record_funding_poll('cycle', polled_ts=10800,
                                        entropy_ok=outcomes[0], hedge_ok=outcomes[1])
            assert journal.funding_state('cycle')['last_complete_poll_ts'] is None
        journal.record_funding_poll('cycle', polled_ts=10800, entropy_ok=True, hedge_ok=True)
        before = journal.funding_state('cycle')
        for stamp, outcomes in ((10799, (True, True)), (10800, (True, True)),
                                (14400, (True, False)), (14400, (False, True))):
            journal.record_funding_poll('cycle', polled_ts=stamp,
                                        entropy_ok=outcomes[0], hedge_ok=outcomes[1])
            assert journal.funding_state('cycle') == before
        assert before['last_complete_poll_ts'] == 10800
        assert before['settlement_counts'] == {'entropy': 0, 'hedge': 0}
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.funding_state('cycle') == before
        journal.record_funding_poll('cycle', polled_ts=86400, entropy_ok=True, hedge_ok=True)
        state = journal.funding_state('cycle')
        assert state['last_complete_poll_ts'] == 86400
        assert state['settlement_counts'] == {'entropy': 0, 'hedge': 0}
        assert state['completed'] is False
        with pytest.raises(ValueError):
            journal.record_funding_poll('cycle', polled_ts=86401, entropy_ok=1, hedge_ok=True)


def test_late_funding_revalues_provisional_pnl_budget_without_finalizing(journal):
    journal.ensure_funding_cycle('cycle', hedged_ts=HEDGED_TS)
    for venue in ('entropy', 'hedge'):
        journal.record_funding_settlements('cycle', venue, [
            settlement('one', 10800, '0.1'), settlement('two', 14400, '0.2')])
    journal.set_funding_exit('cycle', reason='FUNDING_TWO_SETTLEMENTS', completed=True)
    journal.update_live_cycle('cycle', phase='DONE', updated_ts=14401)
    journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('1'),
                         positions_verified_flat=True, all_orders_terminal=True,
                         accounting_final=False, conservative_pnl_bound=True)
    live_before = journal.db.execute('SELECT * FROM live_cycles').fetchone()
    for pnl, loss in ((D('-2'), D('2')), (D('-2'), D('2')), (D('-1'), D('1'))):
        journal.update_funding_pnl('cycle', pnl)
        state = journal.batch_state('batch')
        assert state['loss_used_usd'] == loss
        assert state['pending_accounting'] == 1
        assert state['block_reason'] == 'PENDING_ACCOUNTING'
    journal.record_funding_settlements('cycle', 'entropy', [settlement('late', 14420, '-10')])
    journal.update_funding_pnl('cycle', D('-11'))
    state = journal.batch_state('batch')
    assert state['loss_used_usd'] == D('11')
    assert state['close_reason'] == 'REALIZED_LOSS_BUDGET'
    journal.record_funding_poll('cycle', polled_ts=14401 + 1000, entropy_ok=True, hedge_ok=True)
    row = journal.db.execute('SELECT * FROM cycles WHERE cycle_id=?', ('cycle',)).fetchone()
    assert row['pnl_usd'] == '-11'
    assert row['complete_accounting'] == 1
    assert row['accounting_final'] == 0
    assert journal.funding_state('cycle')['completed'] is True
    assert journal.funding_state('cycle')['actual_funding_usd'] == D('-9.4')
    assert journal.funding_state('cycle')['settlement_counts'] == {'entropy': 2, 'hedge': 2}
    assert journal.provisional_cycles('batch')[0]['cycle_id'] == 'cycle'
    assert tuple(journal.db.execute('SELECT * FROM live_cycles').fetchone()) == tuple(live_before)
    # Existing loss latches remain sticky when a subsequent correction improves PnL.
    journal.update_funding_pnl('cycle', D('3'))
    assert journal.batch_state('batch')['closed_for_entries'] is True
    assert journal.batch_state('batch')['loss_used_usd'] == D('0')


def test_funding_pnl_update_requires_completed_funding_cycle_and_finite_value(journal):
    journal.ensure_funding_cycle('cycle')
    with pytest.raises(PendingOrder, match='completed funding'):
        journal.update_funding_pnl('cycle', D('1'))
    for bad in (D('NaN'), D('Infinity'), True):
        with pytest.raises(ValueError):
            journal.update_funding_pnl('cycle', bad)


def test_late_funding_pnl_and_pending_accounting_survive_restart(tmp_path):
    path = tmp_path / 'late-pnl.sqlite'
    with closing(new_journal(path)) as journal:
        journal.ensure_funding_cycle('cycle')
        journal.set_funding_exit('cycle', reason='PARTIAL_RISK_EXIT', completed=False)
        journal.update_live_cycle('cycle', phase='DONE', updated_ts=8000)
        journal.finish_cycle('cycle', kind='unwound', net_pnl_usd=D('-0.5'),
                             positions_verified_flat=True, all_orders_terminal=True,
                             accounting_final=False, conservative_pnl_bound=True)
    with closing(Journal(path, identity=IDENTITY)) as journal:
        journal.record_funding_settlements('cycle', 'entropy', [settlement('late', 7199, '-0.25')])
        journal.record_funding_poll('cycle', polled_ts=9000, entropy_ok=True, hedge_ok=True)
        journal.update_funding_pnl('cycle', D('-0.75'))
    with closing(Journal(path, identity=IDENTITY)) as journal:
        cycle = journal.provisional_cycles('batch')[0]
        assert cycle['pnl_usd'] == '-0.75'
        assert cycle['accounting_final'] == 0
        assert journal.batch_state('batch')['loss_used_usd'] == D('0.75')
        state = journal.funding_state('cycle')
        assert state['actual_funding_usd'] == D('-0.25')
        assert state['last_complete_poll_ts'] == 9000
        assert state['settlement_counts'] == {'entropy': 0, 'hedge': 0}


def test_pnl_and_budget_updates_rollback_together(journal, monkeypatch):
    journal.ensure_funding_cycle('cycle')
    journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('0'),
                         positions_verified_flat=True, all_orders_terminal=True,
                         accounting_final=False, conservative_pnl_bound=True)
    before = tuple(journal.db.execute('SELECT * FROM cycles').fetchone())

    def fail_budget(*args, **kwargs):
        raise sqlite3.OperationalError('offline injected failure')

    monkeypatch.setattr(journal, '_close_entries', fail_budget)
    with pytest.raises(sqlite3.OperationalError):
        journal.update_funding_pnl('cycle', D('-11'))
    assert tuple(journal.db.execute('SELECT * FROM cycles').fetchone()) == before
    assert journal.batch_state('batch')['loss_used_usd'] == D('0')
    assert not journal.db.in_transaction


def test_disabled_budget_persists_and_cannot_silently_change_authorization(tmp_path, journal):
    path = tmp_path / 'disabled.sqlite'
    with closing(new_journal(path, loss_budget_usd=None)) as disabled:
        state = disabled.batch_state('batch')
        assert state['loss_budget_usd'] is None
        assert disabled.db.execute('SELECT loss_budget FROM batches').fetchone()[0] == 'disabled'
        assert disabled.db.execute(
            "SELECT value FROM metadata WHERE key='economics_schema_version'").fetchone()[0] == '8'
    with closing(Journal(path, identity=IDENTITY)) as disabled:
        disabled.ensure_batch('batch', max_position_cycles=5, loss_budget_usd=None)
        assert disabled.batch_state('batch') == state
        with pytest.raises(ValueError, match='authorization'):
            disabled.ensure_batch('batch', max_position_cycles=5, loss_budget_usd=D('10'))
    with pytest.raises(ValueError, match='authorization'):
        journal.ensure_batch('batch', max_position_cycles=5, loss_budget_usd=None)
    journal.ensure_batch('batch', max_position_cycles=5, loss_budget_usd=D('10.00'))


def test_disabled_budget_finish_finalize_open_risk_and_reopen_do_not_close(tmp_path, monkeypatch):
    with closing(new_journal(tmp_path / 'disabled-risk.sqlite', loss_budget_usd=None)) as journal:
        monkeypatch.setattr('entropy_arb.v3.journal.time.time', lambda: 10000)
        journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('-100000'),
                             positions_verified_flat=True, all_orders_terminal=True)
        journal.update_live_cycle('cycle', phase='DONE', updated_ts=10000)
        journal.finalize_cycle_accounting('cycle', D('-200000'))
        assert journal.observe_open_risk('batch', active_cycle_net_pnl_usd=D('-300000'))
        state = journal.batch_state('batch')
        assert state['loss_used_usd'] == D('200000')
        assert state['pnl_usd'] == D('-200000')
        assert state['loss_budget_usd'] is None
        assert not state['closed_for_entries']
        journal.close_entries('batch', 'MANUAL_STOP')
        assert not journal.observe_open_risk('batch', active_cycle_net_pnl_usd=D('-300000'))
        assert journal.batch_state('batch')['close_reason'] == 'MANUAL_STOP'
        evidence = SimpleNamespace(fully_authoritative=True, entropy_units=0,
                                   hedge_units=0, observed_ts=10000)
        journal.reopen_entries('batch', evidence=evidence)
        assert journal.batch_state('batch')['can_open']
        assert journal.batch_state('batch')['loss_used_usd'] == D('200000')


def test_disabled_budget_funding_pnl_remains_provisional_and_counts_loss_once(tmp_path):
    path = tmp_path / 'disabled-funding.sqlite'
    with closing(new_journal(path, loss_budget_usd=None)) as journal:
        journal.ensure_funding_cycle('cycle')
        journal.record_funding_settlements('cycle', 'entropy', [settlement('partial', 7199, '-1')])
        journal.finish_cycle('cycle', kind='unwound', net_pnl_usd=D('-100000'),
                             positions_verified_flat=True, all_orders_terminal=True,
                             accounting_final=False, conservative_pnl_bound=True)
        journal.update_live_cycle('cycle', phase='DONE', updated_ts=8000)
        for pnl in (D('-200000'), D('-200000'), D('-150000')):
            journal.update_funding_pnl('cycle', pnl)
            state = journal.batch_state('batch')
            assert state['loss_used_usd'] == -pnl
            assert state['pnl_usd'] == pnl
            assert state['loss_budget_usd'] is None
            assert state['block_reason'] == 'PENDING_ACCOUNTING'
            assert not state['closed_for_entries']
        assert journal.funding_state('cycle')['actual_funding_usd'] == D('-1')
        assert journal.db.execute('SELECT cash_net_usd FROM live_cycles').fetchone()[0] == '0'
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.provisional_cycles('batch')[0]['accounting_final'] == 0
        assert journal.batch_state('batch')['loss_used_usd'] == D('150000')
        journal.finalize_cycle_accounting('cycle', D('-150000'))
        assert journal.batch_state('batch')['can_open']


def test_disabled_budget_preserves_pending_accounting_and_cycle_limit(tmp_path, monkeypatch):
    with closing(Journal(tmp_path / 'disabled-limit.sqlite', identity=IDENTITY)) as journal:
        monkeypatch.setattr('entropy_arb.v3.journal.time.time', lambda: 10000)
        journal.ensure_batch('batch', max_position_cycles=1, loss_budget_usd=None)
        journal.begin_cycle('batch', 'cycle')
        assert journal.batch_state('batch')['block_reason'] == 'MAX_POSITION_CYCLES'
        journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('-100000'),
                             positions_verified_flat=True, all_orders_terminal=True)
        assert not journal.batch_state('batch')['closed_for_entries']
        assert journal.batch_state('batch')['block_reason'] == 'MAX_POSITION_CYCLES'
        evidence = SimpleNamespace(fully_authoritative=True, entropy_units=0,
                                   hedge_units=0, observed_ts=10000)
        with pytest.raises(PendingOrder, match='MAX_POSITION_CYCLES'):
            journal.reopen_entries('batch', evidence=evidence)


@pytest.mark.parametrize('budget', [None, D('10')])
def test_refresh_books_late_funding_without_fee_baseline_and_replaces_actual_trade_cash(tmp_path, budget):
    path = tmp_path / 'refresh.sqlite'
    with closing(new_journal(path, loss_budget_usd=budget)) as journal:
        journal.ensure_funding_cycle('cycle')
        journal.update_live_cycle('cycle', cash_delta_usd=D('-1'), updated_ts=7200)
        journal.record_funding_settlements('cycle', 'entropy', [settlement('initial', 7199, '.25')])
        journal.finish_cycle('cycle', kind='unwound', net_pnl_usd=D('-.75'),
                             positions_verified_flat=True, all_orders_terminal=True,
                             accounting_final=False, conservative_pnl_bound=True)
        journal.update_live_cycle('cycle', phase='DONE', updated_ts=8000)
        assert journal.funding_state('cycle')['trade_cash_net_usd'] is None
        # Missing fee history: initialize from trade-only live cash, never cycle PnL.
        assert journal.refresh_funding_pnl('cycle') == D('-.75')
        assert journal.funding_state('cycle')['trade_cash_net_usd'] == D('-1')
    with closing(Journal(path, identity=IDENTITY)) as journal:
        late = settlement('late', 7999, '-.5')
        journal.record_funding_settlements('cycle', 'hedge', [late])
        for _ in range(2):
            journal.record_funding_settlements('cycle', 'hedge', [late])
            assert journal.refresh_funding_pnl('cycle') == D('-1.25')
            assert journal.batch_state('batch')['loss_used_usd'] == D('1.25')
        assert journal.refresh_funding_pnl('cycle', trade_cash_net_usd=D('-.8')) == D('-1.05')
        assert journal.batch_state('batch')['loss_used_usd'] == D('1.05')
        assert journal.db.execute('SELECT cash_net_usd FROM live_cycles').fetchone()[0] == '-1'
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.refresh_funding_pnl('cycle') == D('-1.05')
        state = journal.funding_state('cycle')
        assert state['trade_cash_net_usd'] == D('-.8')
        assert isinstance(state['trade_cash_net_usd'], D)
        assert state['actual_funding_usd'] == D('-.25')
        assert state['settlement_counts'] == {'entropy': 0, 'hedge': 0}
        cycle = journal.provisional_cycles('batch')[0]
        assert D(cycle['pnl_usd']) == D('-1.05')
        assert cycle['complete_accounting'] == 1 and cycle['accounting_final'] == 0
        assert journal.batch_state('batch')['loss_budget_usd'] == budget
        assert not journal.batch_state('batch')['closed_for_entries']


def test_refresh_preserves_explicit_zero_trade_baseline(journal):
    journal.ensure_funding_cycle('cycle')
    journal.update_live_cycle('cycle', cash_delta_usd=D('-1'), updated_ts=7200)
    journal.record_funding_settlements('cycle', 'entropy', [settlement('fee', 7199, '-.25')])
    journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('-1.25'),
                         positions_verified_flat=True, all_orders_terminal=True,
                         accounting_final=False, conservative_pnl_bound=True)
    assert journal.refresh_funding_pnl('cycle', trade_cash_net_usd=D('0')) == D('-.25')
    assert journal.refresh_funding_pnl('cycle') == D('-.25')
    assert journal.funding_state('cycle')['trade_cash_net_usd'] == D('0')
    assert journal.db.execute('SELECT cash_net_usd FROM live_cycles').fetchone()[0] == '-1'


def test_refresh_migrates_existing_funding_table_without_baseline_column(tmp_path):
    path = tmp_path / 'old-funding.sqlite'
    with closing(new_journal(path, loss_budget_usd=None)) as journal:
        journal.ensure_funding_cycle('cycle')
        journal.record_funding_sample('cycle', sample_ts=7200, net_usd_per_hour=D('-1'))
        journal.set_funding_exit('cycle', reason='PARTIAL_RISK_EXIT', completed=False)
        journal.update_live_cycle('cycle', cash_delta_usd=D('-.5'), updated_ts=8000)
        journal.record_funding_settlements('cycle', 'entropy', [settlement('late', 7199, '-.25')])
        journal.finish_cycle('cycle', kind='unwound', net_pnl_usd=D('-.75'),
                             positions_verified_flat=True, all_orders_terminal=True,
                             accounting_final=False, conservative_pnl_bound=True)
        journal.update_live_cycle('cycle', phase='DONE', updated_ts=8000)
        before = journal.funding_state('cycle')
    with sqlite3.connect(path) as db:
        db.execute('ALTER TABLE funding_cycles DROP COLUMN trade_cash_net_usd')
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.funding_state('cycle') == before
        assert journal.refresh_funding_pnl('cycle') == D('-.75')
        assert journal.db.execute(
            "SELECT value FROM metadata WHERE key='economics_schema_version'").fetchone()[0] == '8'
    with closing(Journal(path, identity=IDENTITY)) as journal:
        assert journal.refresh_funding_pnl('cycle') == D('-.75')
        assert journal.funding_state('cycle')['trade_cash_net_usd'] == D('-.5')
        assert journal.db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'


def test_refresh_baseline_pnl_and_numeric_budget_rollback_together(journal, monkeypatch):
    journal.ensure_funding_cycle('cycle')
    journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('0'),
                         positions_verified_flat=True, all_orders_terminal=True,
                         accounting_final=False, conservative_pnl_bound=True)
    before = journal.funding_state('cycle')
    cycle_before = tuple(journal.db.execute('SELECT * FROM cycles').fetchone())
    original_close = journal._close_entries

    def fail_budget(*args, **kwargs):
        raise sqlite3.OperationalError('offline injected failure')

    monkeypatch.setattr(journal, '_close_entries', fail_budget)
    with pytest.raises(sqlite3.OperationalError):
        journal.refresh_funding_pnl('cycle', trade_cash_net_usd=D('-11'))
    assert journal.funding_state('cycle') == before
    assert tuple(journal.db.execute('SELECT * FROM cycles').fetchone()) == cycle_before
    assert not journal.db.in_transaction
    monkeypatch.setattr(journal, '_close_entries', original_close)
    for _ in range(2):
        assert journal.refresh_funding_pnl('cycle', trade_cash_net_usd=D('-11')) == D('-11')
        assert journal.batch_state('batch')['loss_used_usd'] == D('11')
        assert journal.batch_state('batch')['close_reason'] == 'REALIZED_LOSS_BUDGET'
        assert journal.provisional_cycles('batch')[0]['accounting_final'] == 0


@pytest.mark.parametrize('bad', [D('NaN'), D('Infinity'), D('-Infinity'), True])
def test_refresh_rejects_invalid_baseline_without_persisting_it(journal, bad):
    journal.ensure_funding_cycle('cycle')
    journal.finish_cycle('cycle', kind='clean', net_pnl_usd=D('0'),
                         positions_verified_flat=True, all_orders_terminal=True,
                         accounting_final=False, conservative_pnl_bound=True)
    before = journal.funding_state('cycle')
    with pytest.raises(ValueError):
        journal.refresh_funding_pnl('cycle', trade_cash_net_usd=bad)
    assert journal.funding_state('cycle') == before


def test_refresh_requires_completed_funding_cycle(journal):
    with pytest.raises(PendingOrder, match='completed funding'):
        journal.refresh_funding_pnl('cycle')
    journal.ensure_funding_cycle('cycle')
    with pytest.raises(PendingOrder, match='completed funding'):
        journal.refresh_funding_pnl('cycle', trade_cash_net_usd=D('1'))
    assert journal.funding_state('cycle')['trade_cash_net_usd'] is None
