import asyncio
from dataclasses import replace
from decimal import Decimal as D
import json
import socket
import time

import pytest

from entropy_arb.funding import FundingRiskSnapshot, FundingSettlement, FundingSnapshot
from entropy_arb.v3.core import Direction, Settings
from entropy_arb.v3.funding_hedge import (
    entry_direction, liquidation_distance, pair_income_usd_per_hour,
    remaining_cost_usd, reversal_confirmed,
)
from entropy_arb.v3.journal import Journal
from entropy_arb.v3.runtime import RuntimeHalt
from test_runtime import setup_runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('offline funding tests cannot access network')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


@pytest.fixture
def carry(tmp_path, monkeypatch, request):
    clock = [10000.0]
    monkeypatch.setattr(time, 'time', lambda: clock[0])
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, strategy_mode='funding_hedge',
                               max_notional_usd=D('200'))
    runtime.authorization = replace(runtime.authorization, max_position_cycles=getattr(request, 'param', 1),
                                    loss_budget_usd=None, max_notional_usd=D('200'))
    for venue in (entropy, hedge):
        venue.conf.symbol = 'NBIS'
        venue.size_decimals = 2 if venue is entropy else 4
        venue.funding_rate = 1 if venue is entropy else 2
        venue.settlements = []
        venue.liquidation = None
        venue.risk_error = None

        venue.index_price = D('100')

        async def rate(*, include_price=False, v=venue):
            return (None if v.funding_rate is None else
                    FundingSnapshot(v.funding_rate, clock[0], 'fake:hourly', v.index_price))

        async def records(since, until=None, v=venue):
            return list(v.settlements)

        async def risk(v=venue):
            if v.risk_error:
                raise v.risk_error
            qty = D(str(v.position))
            liq = (v.liquidation if v.liquidation is not None else
                   D('95') if qty > 0 else D('105') if qty < 0 else None)
            return FundingRiskSnapshot(qty, D('100'), liq, clock[0], 'fake')

        venue.fetch_funding, venue.fetch_funding_settlements = rate, records
        venue.fetch_funding_risk = risk
        venue.set_book(99.99, 100)
    asyncio.run(runtime.initialize())

    def tick(stamp):
        clock[0] = stamp
        entropy.set_book(99.99, 100)
        hedge.set_book(99.99, 100)

    async def open_pair():
        await runtime._refresh_flat_lease()
        await runtime._maybe_open(runtime._market(), D('0'))
        return journal.active_live_cycle()

    yield runtime, entropy, hedge, journal, clock, tick, open_pair
    journal.close()


def test_signed_pair_income_not_single_leg_sign_and_stale_not_zero():
    rate = lambda v: FundingSnapshot(v, 100, 'hourly')
    net = pair_income_usd_per_hour(D('2'), D('-2'), D('100'), D('100'),
                                   rate(-1), rate(-.5), now=100, stale_seconds=10)
    assert net == D('.01')  # Both negative, but the long receives more than short pays.
    assert entry_direction(D('100'), D('100'), rate(-1), rate(-.5),
                           now=100, stale_seconds=10) is Direction.LONG_ENTROPY
    assert pair_income_usd_per_hour(D('2'), D('-2'), D('100'), D('100'),
                                    None, rate(0), now=100, stale_seconds=10) is None
    assert entry_direction(D('100'), D('100'), rate(0), rate(0),
                           now=100, stale_seconds=10) is None
    assert entry_direction(D('100'), D('100'), rate(1), rate(2),
                           now=111, stale_seconds=10) is None


def test_funding_dashboard_telemetry_reports_rates_and_clears_missing_data(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    asyncio.run(runtime._fetch_funding_rates())
    assert runtime.status.base['funding_rates']['entropy']['rate_bps_per_hour'] == '1'
    assert runtime.status.base['funding_rates']['hedge']['index_price'] == '100'
    assert runtime.status.base['funding_rates']['entropy']['source_ts'] == clock[0]
    e.funding_rate = None
    asyncio.run(runtime._fetch_funding_rates())
    assert runtime.status.base['funding_rates']['entropy'] is None


def test_funding_dashboard_risk_telemetry_uses_actual_snapshot_without_orders(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    before = len(journal.cycle_orders(cycle['cycle_id']))
    tick(10003)
    asyncio.run(runtime._check_funding_liquidation(cycle))
    risk = runtime.status.base['funding_risk']
    assert risk['cycle_id'] == cycle['cycle_id']
    assert risk['legs']['entropy']['mark_price'] == '100'
    assert risk['legs']['entropy']['liquidation_price'] == '95'
    assert risk['legs']['hedge']['distance'] == '0.05'
    assert len(journal.cycle_orders(cycle['cycle_id'])) == before


def test_cost_full_bills_and_liquidation_crossed_boundary():
    assert remaining_cost_usd(D('-.3'), {'entropy': 2, 'hedge': 0}, 2) == D('.6')
    assert remaining_cost_usd(D('.3'), {'entropy': 0, 'hedge': 0}, 2) == 0
    risk = FundingRiskSnapshot(D('-2'), D('100'), D('101'), 100)
    assert liquidation_distance(risk, now=100, stale_seconds=10) == D('.01')
    crossed = replace(risk, mark_price=D('102'))
    assert liquidation_distance(crossed, now=100, stale_seconds=10) < 0
    assert liquidation_distance(risk, now=111, stale_seconds=10) is None


def test_settings_opt_in_and_disabled_round_loss():
    assert Settings().strategy_mode == 'basis'
    assert Settings.from_mapping({'strategy_mode': 'funding_hedge',
                                  'funding_round_loss_limit_usd': None}).funding_round_loss_limit_usd is None
    for bad in ({'funding_reversal_samples': 2}, {'funding_reversal_seconds': 59},
                {'funding_liquidation_distance': '.019'}, {'strategy_mode': 'farming'}):
        with pytest.raises(ValueError):
            Settings.from_mapping(bad)


def test_open_by_funding_matched_step_one_round(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    assert cycle['direction'] == 'long_entropy'
    assert runtime._funding_common_step == D('.01')
    assert runtime.instrument.quantity_step == D('.0001')
    assert e.position == -h.position == 1.99
    assert all(o['qty'] * o['limit_px'] <= 200 for o in e.sent + h.sent)
    assert journal.funding_state(cycle['cycle_id'])['hedged_ts'] == clock[0]
    runtime._latch_funding_exit(cycle, 'manual_test')
    asyncio.run(runtime._funding_exit(cycle))
    before = len(e.sent) + len(h.sent)
    asyncio.run(runtime._maybe_open(runtime._market(), D('0')))
    assert journal.active_live_cycle() is None
    assert len(e.sent) + len(h.sent) == before
    assert all(o['reduce_only'] for o in e.sent[1:] + h.sent[1:])


def test_index_price_not_book_mid_controls_funding_admission(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    e.index_price = D('300')
    cycle = asyncio.run(open_pair())
    assert cycle['direction'] == 'short_entropy'


def test_missing_index_price_blocks_entry_and_resets_negative_streak(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    e.index_price = None
    assert asyncio.run(open_pair()) is None
    assert not e.sent and not h.sent
    e.index_price = D('100')
    tick(10030)
    cycle = asyncio.run(open_pair())
    e.funding_rate, h.funding_rate = 40, 0
    tick(10060)
    asyncio.run(runtime._resume_cycle(cycle))
    e.index_price = None
    tick(10090)
    asyncio.run(runtime._resume_cycle(journal.active_live_cycle()))
    assert journal.funding_state(cycle['cycle_id'])['negative_sample_count'] == 0


def test_zero_real_settlements_delayed_other_leg_then_close_no_rates(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    for venue in (e, h):
        venue.settlements.append(FundingSettlement(venue.key + '1', 10800, D('0')))
    e.settlements.append(FundingSettlement('e2', 14400, D('0')))
    tick(14430)
    asyncio.run(runtime._resume_cycle(cycle))
    assert journal.active_live_cycle() is not None
    assert journal.funding_state(cycle['cycle_id'])['settlement_counts'] == {'entropy': 2, 'hedge': 1}
    h.settlements.append(FundingSettlement('h2', 14400, D('0')))
    e.funding_rate = h.funding_rate = None
    tick(14461)
    asyncio.run(runtime._resume_cycle(journal.active_live_cycle()))
    state = journal.funding_state(cycle['cycle_id'])
    assert state['completed'] and state['exit_reason'] == 'two_settlements_each'
    assert journal.active_live_cycle() is None
    assert e.position == h.position == 0


@pytest.mark.parametrize('net_rate,exit_expected', [(2, False), (40, True)])
def test_reversal_three_samples_sixty_seconds_no_flip(carry, net_rate, exit_expected):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    e.funding_rate, h.funding_rate = net_rate, 0
    for stamp in (10030, 10060):
        tick(stamp)
        asyncio.run(runtime._resume_cycle(journal.active_live_cycle()))
        assert journal.active_live_cycle() is not None
    tick(10090)
    asyncio.run(runtime._resume_cycle(journal.active_live_cycle()))
    state = journal.funding_state(cycle['cycle_id'])
    assert reversal_confirmed(state, samples=3, seconds=60)
    assert bool(state['exit_reason']) is exit_expected
    assert not state['completed']
    if exit_expected:
        assert state['exit_reason'] == 'funding_reversal_cost'
        assert journal.active_live_cycle() is None
        assert all(o['reduce_only'] for o in e.sent[1:] + h.sent[1:])
        assert runtime._funding_completion_fields(cycle['cycle_id'])['funding_completion_label'] == '两周期未完成'
    else:
        assert e.position == -h.position == 1.99


def test_missing_sampling_resets_streak_restart_does_not_reset_hedge(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    e.funding_rate, h.funding_rate = 40, 0
    tick(10030)
    asyncio.run(runtime._resume_cycle(cycle))
    e.funding_rate = None
    tick(10060)
    asyncio.run(runtime._resume_cycle(journal.active_live_cycle()))
    state = journal.funding_state(cycle['cycle_id'])
    assert state['negative_sample_count'] == 0
    path = journal.db.execute('PRAGMA database_list').fetchone()[2]
    journal.close()
    runtime.journal = reopened = Journal(path, identity='pair-account')
    try:
        tick(10090)
        runtime._confirm_funding_hedged(reopened.active_live_cycle())
        assert reopened.funding_state(cycle['cycle_id'])['hedged_ts'] == 10000
    finally:
        reopened.close()
        # Fixture cleanup operates on the original closed connection.


def test_liquidation_hedge_first_before_funding_io(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    h.liquidation = D('101')
    sequence = []
    for venue in (e, h):
        original = venue.send_taker

        async def send(*args, v=venue, original=original, **kwargs):
            sequence.append(v.key)
            return await original(*args, **kwargs)

        async def never(*args, **kwargs):
            raise AssertionError('risk exit must not wait for settlement IO')
        venue.send_taker, venue.fetch_funding_settlements = send, never
    tick(10003)
    asyncio.run(runtime._resume_cycle(cycle))
    assert sequence == ['hedge', 'entropy']
    assert journal.funding_state(cycle['cycle_id'])['exit_reason'] == 'liquidation_hedge'
    assert e.position == h.position == 0


def test_liquidation_exit_closes_fresh_leg_even_other_book_stale(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    h.liquidation = D('101')
    clock[0] = 10003
    h.set_book(99.99, 100)
    asyncio.run(runtime._resume_cycle(cycle))
    assert h.position == 0 and e.position > 0
    assert journal.funding_state(cycle['cycle_id'])['exit_reason'] == 'liquidation_hedge'
    tick(10006)
    asyncio.run(runtime._resume_cycle(journal.active_live_cycle()))
    assert e.position == h.position == 0
    assert all(o['reduce_only'] for o in e.sent[1:] + h.sent[1:])


def test_normal_completion_does_not_wait_on_cross_book_or_rates(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    for venue in (e, h):
        venue.settlements = [FundingSettlement(venue.key + str(ts), ts, D('0'))
                             for ts in (10800, 14400)]
        venue.funding_rate = None
    clock[0] = 14403
    h.set_book(99.99, 100)  # Entropy's book deliberately stale.
    asyncio.run(runtime._resume_cycle(cycle))
    assert journal.funding_state(cycle['cycle_id'])['completed']
    assert h.position == 0 and e.position > 0
    tick(14406)
    asyncio.run(runtime._resume_cycle(journal.active_live_cycle()))
    assert e.position == h.position == 0


@pytest.mark.parametrize('carry', [1, 6], indirect=True)
def test_risk_exit_unknown_and_native_partial_recovered_after_restart(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    h.liquidation = D('101')
    h.fill_fractions = [1.2345 / 1.99]
    h.unknown_once = True
    original_resolve = h.resolve_order
    recovered = [False]

    async def resolve(ref, timeout=None):
        if recovered[0]:
            return await original_resolve(ref, timeout=timeout)
        return {'status': 'timeout', 'filled_base': 0, 'avg_px': None,
                'err': None, 'unresolved': True, 'order_ref': ref}

    h.resolve_order = resolve
    tick(10003)
    asyncio.run(runtime._resume_cycle(cycle))
    assert journal.active_live_cycle() is not None and journal.pending()
    path = journal.db.execute('PRAGMA database_list').fetchone()[2]
    journal.close()
    runtime.journal = reopened = Journal(path, identity='pair-account')
    runtime.executor.journal = reopened
    recovered[0] = True
    try:
        tick(10006)
        asyncio.run(runtime._resume_cycle(reopened.active_live_cycle()))
        assert reopened.active_live_cycle() is None
        assert not reopened.pending()
        assert e.position == h.position == 0
        assert all(o['reduce_only'] for o in e.sent[1:] + h.sent[1:])
    finally:
        reopened.close()


def test_partial_and_unknown_open_retains_recovery(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    e.fill_fractions = [100 / 199]
    e.unknown_once = True
    cycle = asyncio.run(open_pair())
    assert cycle['entropy_units'] == -cycle['hedge_units'] == 10000
    assert not journal.pending()
    assert journal.funding_state(cycle['cycle_id'])['hedged_ts'] is not None


def test_lighter_fine_partial_fills_are_exactly_recovered(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    h.fill_fractions = [1.2345 / 1.99]
    cycle = asyncio.run(open_pair())
    assert cycle['entropy_units'] == -cycle['hedge_units'] == 19900
    assert e.position == -h.position == 1.99
    assert len(h.sent) == 2
    assert h.sent[1]['qty'] == .7555
    assert not journal.pending()


def test_late_cash_changes_same_pnl_once_without_finality(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    tick(10003)
    runtime._latch_funding_exit(cycle, 'manual_test')
    asyncio.run(runtime._funding_exit(cycle))
    before = D(journal.db.execute('SELECT pnl_usd FROM cycles').fetchone()[0])
    e.settlements.append(FundingSettlement('late', 10000.01, D('-.25')))
    tick(10130)
    assert asyncio.run(runtime._reconcile_cycle_accounting(cycle['cycle_id'])) is None
    after = D(journal.db.execute('SELECT pnl_usd FROM cycles').fetchone()[0])
    assert after == before - D('.25')
    asyncio.run(runtime._reconcile_cycle_accounting(cycle['cycle_id']))
    assert D(journal.db.execute('SELECT pnl_usd FROM cycles').fetchone()[0]) == after
    assert not journal.db.execute('SELECT accounting_final FROM cycles').fetchone()[0]
    assert journal.batch_state('batch')['loss_budget_usd'] is None


def test_late_cash_still_updates_pnl_when_fee_history_unavailable(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    tick(10003)
    runtime._latch_funding_exit(cycle, 'manual_test')
    asyncio.run(runtime._funding_exit(cycle))
    before = D(journal.db.execute('SELECT pnl_usd FROM cycles').fetchone()[0])
    e.settlements.append(FundingSettlement('late-without-fees', 10001, D('-.75')))

    async def no_fees(*args, **kwargs):
        return []

    e.fetch_fee_fills = h.fetch_fee_fills = no_fees
    tick(10130)
    assert asyncio.run(runtime._reconcile_cycle_accounting(cycle['cycle_id'])) is None
    after = D(journal.db.execute('SELECT pnl_usd FROM cycles').fetchone()[0])
    assert after == before - D('.75')
    asyncio.run(runtime._reconcile_cycle_accounting(cycle['cycle_id']))
    assert D(journal.db.execute('SELECT pnl_usd FROM cycles').fetchone()[0]) == after
    assert journal.batch_state('batch')['loss_used_usd'] == max(-after, D('0'))


def test_existing_position_refused_before_signer_or_leverage(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, strategy_mode='funding_hedge')
    e.position = .1
    e.init_signer = lambda: pytest.fail('must reject old position before signer')
    try:
        with pytest.raises(RuntimeHalt, match='pre-existing'):
            asyncio.run(runtime.initialize())
        assert not e.sent and not h.sent
    finally:
        journal.close()


@pytest.mark.parametrize('carry', [1, 6], indirect=True)
def test_persistent_unknown_first_leg_never_opens_hedge(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    e.unknown_once = True
    original = e.resolve_order
    resolved = [False]

    async def unknown(ref, timeout=None):
        if resolved[0]:
            return await original(ref, timeout=timeout)
        return {'status': 'timeout', 'filled_base': 0, 'avg_px': None,
                'err': None, 'unresolved': True, 'order_ref': ref}

    e.resolve_order = unknown
    cycle = asyncio.run(open_pair())
    assert e.position == 1.99 and h.position == 0
    assert h.sent == [] and journal.pending()
    assert journal.funding_state(cycle['cycle_id'])['exit_reason'] == 'unknown_order_recovery'
    resolved[0] = True
    tick(10003)
    asyncio.run(runtime._resume_cycle(journal.active_live_cycle()))
    assert e.position == h.position == 0
    assert journal.active_live_cycle() is None
    assert all(o['reduce_only'] for o in e.sent[1:])
    assert h.sent == []


def test_inflight_risk_query_blocks_nonreducing_hedge_until_result(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    tick(10003)

    async def check():
        entered, release = asyncio.Event(), asyncio.Event()

        async def dangerous():
            entered.set()
            await release.wait()
            return FundingRiskSnapshot(D('1.99'), D('100'), D('99'), clock[0], 'fake')

        e.fetch_funding_risk = dangerous
        watcher = asyncio.create_task(runtime._check_funding_liquidation(cycle))
        await entered.wait()
        baseline = len(e.sent) + len(h.sent)
        opening = asyncio.create_task(runtime._campaign(
            cycle, purpose='repair_complete_pair', venue_key='hedge',
            side=Direction(cycle['direction']).entropy_side.opposite,
            units=100, reduce_only=False, rooms=runtime.settings.hedge_rooms_bps,
            hard_cap=runtime.settings.hedge_hard_cap_bps,
            next_phase='RECOVERY', completion=True))
        await asyncio.sleep(0)
        assert not opening.done()
        assert len(e.sent) + len(h.sent) == baseline
        release.set()
        await watcher
        assert await opening == 0
        assert len(e.sent) + len(h.sent) == baseline
        assert journal.funding_state(cycle['cycle_id'])['exit_reason'] == 'liquidation_entropy'

    asyncio.run(check())


def test_funding_economics_never_final_after_handoff_flush(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    events = []
    runtime.observer.publish = events.append
    runtime._latch_funding_exit(cycle, 'manual_test')
    asyncio.run(runtime._resume_cycle(cycle))
    runtime._flush_telemetry()
    row = journal.db.execute('SELECT accounting_final FROM cycle_economics').fetchone()
    assert row['accounting_final'] == 0
    economic_events = [event for event in events if event.get('kind') == 'v3_cycle_economics']
    assert economic_events
    assert all(not event['accounting_final'] for event in economic_events)


def test_zero_fill_funding_cycle_not_falsely_final(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    e.fill_fractions = [0, 0, 0]
    asyncio.run(open_pair())
    cycle = journal.db.execute('SELECT * FROM cycles').fetchone()
    assert cycle['result_kind'] == 'zero_fill'
    assert cycle['accounting_final'] == 0
    assert journal.db.execute('SELECT accounting_final FROM cycle_economics').fetchone()[0] == 0
    assert journal.active_live_cycle() is None
    assert e.position == h.position == 0


def complete_round(carry, cycle):
    runtime, e, h, journal, clock, tick, _ = carry
    first_hour = (int(clock[0] // 3600) + 1) * 3600
    for venue in (e, h):
        venue.settlements.extend(
            FundingSettlement(f'{cycle["cycle_id"]}:{venue.key}:{ts}', ts, D('0'))
            for ts in (first_hour, first_hour + 3600))
    tick(first_hour + 3601)
    asyncio.run(runtime._resume_cycle(cycle))
    assert journal.active_live_cycle() is None
    assert journal.funding_state(cycle['cycle_id'])['completed']
    assert e.position == h.position == 0


@pytest.mark.parametrize('carry', [6], indirect=True)
def test_six_normal_rounds_use_fresh_direction_and_stop_at_authorized_limit(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    for number in range(6):
        e.funding_rate, h.funding_rate = (1, 2) if number % 2 == 0 else (2, 1)
        tick(clock[0] + 2)
        cycle = asyncio.run(open_pair())
        assert cycle is not None
        assert cycle['direction'] == ('long_entropy' if number % 2 == 0 else 'short_entropy')
        state = journal.funding_state(cycle['cycle_id'])
        assert state['settlement_counts'] == {'entropy': 0, 'hedge': 0}
        assert state['negative_sample_count'] == 0 and state['exit_reason'] is None
        complete_round(carry, cycle)
        batch = journal.batch_state('batch')
        assert batch['position_cycles'] == number + 1
        assert batch['pending_accounting'] == number + 1
        assert batch['blocking_pending_accounting'] == 0
        assert batch['can_open'] is (number < 5)
        assert not runtime.stop.is_set()  # Late posting reconciliation stays alive.
    before = len(e.sent) + len(h.sent)
    tick(clock[0] + 2)
    assert asyncio.run(open_pair()) is None
    assert len(e.sent) + len(h.sent) == before
    assert runtime.status.path.read_text().find('authorized_cycle_limit_reached') >= 0
    assert all(not row['accounting_final'] for row in journal.provisional_cycles('batch'))


@pytest.mark.parametrize('carry', [6], indirect=True)
def test_optional_session_limit_stops_entries_but_keeps_late_accounting(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    runtime.session_max_cycles = 2
    for _ in range(2):
        cycle = asyncio.run(open_pair())
        complete_round(carry, cycle)
        tick(clock[0] + 2)
    before = len(e.sent) + len(h.sent)
    assert asyncio.run(open_pair()) is None
    assert journal.batch_state('batch')['can_open']
    assert len(e.sent) + len(h.sent) == before
    assert 'session_cycle_limit_reached' in runtime.status.path.read_text()
    assert not runtime.stop.is_set()


@pytest.mark.parametrize('carry', [2], indirect=True)
@pytest.mark.parametrize('missing', ['fills', 'fee', 'closed_funding_poll'])
def test_next_round_waits_for_verified_closed_accounting(carry, missing):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    original_fills, original_records = e.fetch_fee_fills, e.fetch_funding_settlements

    async def incomplete_fills(*args, **kwargs):
        return [] if missing == 'fills' else [dict(row, fee_usd=None) for row in e.fee_fills]

    async def incomplete_closed_poll(*args, **kwargs):
        if journal.active_live_cycle() is None:
            raise RuntimeError('closed-window funding query unavailable')
        return await original_records(*args, **kwargs)

    if missing == 'closed_funding_poll':
        e.fetch_funding_settlements = incomplete_closed_poll
    else:
        e.fetch_fee_fills = incomplete_fills
    complete_round(carry, cycle)
    assert journal.batch_state('batch')['block_reason'] == 'PENDING_ACCOUNTING'
    before = len(e.sent) + len(h.sent)
    tick(clock[0] + 2)
    assert asyncio.run(open_pair()) is None
    assert len(e.sent) + len(h.sent) == before
    e.fetch_fee_fills, e.fetch_funding_settlements = original_fills, original_records
    asyncio.run(runtime._reconcile_cycle_accounting(cycle['cycle_id']))
    assert journal.batch_state('batch')['can_open']
    assert not journal.db.execute('SELECT accounting_final FROM cycles').fetchone()[0]
    assert asyncio.run(open_pair()) is not None


@pytest.mark.parametrize('carry', [6], indirect=True)
@pytest.mark.parametrize('reason', ['funding_reversal_cost', 'liquidation_entropy',
                                  'risk_data_unavailable', 'unknown_order_recovery'])
def test_early_exit_latches_multi_round_batch_instead_of_reopening(carry, reason):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    runtime._latch_funding_exit(cycle, reason)
    asyncio.run(runtime._funding_exit(cycle))
    assert journal.active_live_cycle() is None
    assert journal.batch_state('batch')['close_reason'] == 'FUNDING_EARLY_EXIT'
    assert not journal.funding_state(cycle['cycle_id'])['completed']
    before = len(e.sent) + len(h.sent)
    tick(clock[0] + 2)
    assert asyncio.run(open_pair()) is None
    assert len(e.sent) + len(h.sent) == before
    assert not runtime.stop.is_set()


@pytest.mark.parametrize('carry', [2], indirect=True)
def test_normal_round_late_cash_stays_in_original_round_during_next_round(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    first = asyncio.run(open_pair())
    complete_round(carry, first)
    before = journal.batch_state('batch')['pnl_usd']
    closed_ts = clock[0]
    tick(closed_ts + 2)
    second = asyncio.run(open_pair())
    e.settlements.append(FundingSettlement('late-first-round', closed_ts - 1, D('-.25')))
    for _ in range(2):
        asyncio.run(runtime._reconcile_cycle_accounting(first['cycle_id']))
        assert journal.batch_state('batch')['pnl_usd'] == before - D('.25')
    asyncio.run(runtime._poll_funding_settlements(second))
    assert journal.funding_state(second['cycle_id'])['actual_funding_usd'] == 0
    assert journal.funding_state(second['cycle_id'])['settlement_counts'] == {'entropy': 0, 'hedge': 0}
    assert journal.batch_state('batch')['position_cycles'] == 2


@pytest.mark.parametrize('carry', [2], indirect=True)
def test_zero_fill_retry_does_not_consume_round_or_claim_finality(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    e.fill_fractions = [0, 0, 0]
    assert asyncio.run(open_pair()) is None
    batch = journal.batch_state('batch')
    assert batch['position_cycles'] == 0 and batch['can_open']
    assert batch['pending_accounting'] == 1 and batch['blocking_pending_accounting'] == 0
    tick(clock[0] + 61)  # Retain the shared send-rate budget.
    assert asyncio.run(open_pair()) is not None
    assert journal.batch_state('batch')['position_cycles'] == 1


@pytest.mark.parametrize('carry', [2], indirect=True)
def test_next_round_after_native_partial_fill_keeps_common_entry_grid(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    h.fill_fractions = [1.2345 / 1.99]
    first = asyncio.run(open_pair())
    assert len(h.sent) == 2 and h.sent[1]['qty'] == .7555
    complete_round(carry, first)
    assert journal.batch_state('batch')['can_open']
    tick(clock[0] + 2)
    second = asyncio.run(open_pair())
    assert second['entropy_units'] == -second['hedge_units'] == 19900
    assert runtime._funding_common_step == D('.01')
    assert runtime.instrument.quantity_step == D('.0001')
    assert not journal.pending()


def fresh_runtime(previous, journal):
    return type(previous)(
        settings=previous.settings, legacy_config=previous.cfg,
        authorization=previous.authorization, pair_id=previous.pair_id,
        deployment=previous.deployment, venues=previous.venues, journal=journal,
        status=previous.status, observer=previous.observer, stop=asyncio.Event(),
        session_max_cycles=previous.session_max_cycles,
        reference_checkpoint_path=previous.reference_checkpoint_path)


@pytest.mark.parametrize('carry', [2], indirect=True)
@pytest.mark.parametrize('active_on_restart', [False, True])
def test_restart_preserves_round_count_and_incomplete_settlement_progress(carry, active_on_restart):
    runtime, e, h, journal, clock, tick, open_pair = carry
    first = asyncio.run(open_pair())
    if active_on_restart:
        for venue in (e, h):
            venue.settlements.append(FundingSettlement(venue.key + ':first-hour', 10800, D('0')))
        tick(10801)
        asyncio.run(runtime._resume_cycle(first))
        assert journal.funding_state(first['cycle_id'])['settlement_counts'] == {'entropy': 1, 'hedge': 1}
    else:
        complete_round(carry, first)
    expected_hedged_ts = journal.funding_state(first['cycle_id'])['hedged_ts']
    path = journal.db.execute('PRAGMA database_list').fetchone()[2]
    journal.close()
    reopened = Journal(path, identity='pair-account')
    restarted = fresh_runtime(runtime, reopened)
    try:
        asyncio.run(restarted.initialize())
        assert reopened.batch_state('batch')['position_cycles'] == 1
        assert reopened.funding_state(first['cycle_id'])['hedged_ts'] == expected_hedged_ts
        new_carry = (restarted, e, h, reopened, clock, tick, None)
        if active_on_restart:
            complete_round(new_carry, reopened.active_live_cycle())
        tick(clock[0] + 2)
        asyncio.run(restarted._refresh_flat_lease())
        asyncio.run(restarted._maybe_open(restarted._market(), D('0')))
        second = reopened.active_live_cycle()
        assert second is not None and second['cycle_id'] != first['cycle_id']
        assert reopened.funding_state(second['cycle_id'])['settlement_counts'] == {'entropy': 0, 'hedge': 0}
        complete_round(new_carry, second)
        before = len(e.sent) + len(h.sent)
        tick(clock[0] + 2)
        asyncio.run(restarted._refresh_flat_lease())
        asyncio.run(restarted._maybe_open(restarted._market(), D('0')))
        assert reopened.active_live_cycle() is None
        assert reopened.batch_state('batch')['position_cycles'] == 2
        assert len(e.sent) + len(h.sent) == before
        assert not restarted.stop.is_set()
    finally:
        reopened.close()


@pytest.mark.parametrize('carry', [6], indirect=True)
def test_restart_does_not_rearm_early_exit_even_with_late_settlements(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    cycle = asyncio.run(open_pair())
    tick(18001)
    runtime._latch_funding_exit(cycle, 'funding_reversal_cost')
    asyncio.run(runtime._funding_exit(cycle))
    path = journal.db.execute('PRAGMA database_list').fetchone()[2]
    journal.close()
    reopened = Journal(path, identity='pair-account')
    restarted = fresh_runtime(runtime, reopened)
    try:
        for venue in (e, h):
            venue.settlements = [FundingSettlement(venue.key + str(ts), ts, D('0'))
                                 for ts in (10800, 14400)]
        tick(18003)
        asyncio.run(restarted.initialize())
        asyncio.run(restarted._reconcile_cycle_accounting(cycle['cycle_id']))
        assert reopened.funding_state(cycle['cycle_id'])['settlement_counts'] == {'entropy': 2, 'hedge': 2}
        assert not reopened.funding_state(cycle['cycle_id'])['completed']
        before = len(e.sent) + len(h.sent)
        asyncio.run(restarted._maybe_open(restarted._market(), D('0')))
        assert len(e.sent) + len(h.sent) == before
        assert reopened.batch_state('batch')['close_reason'] == 'FUNDING_EARLY_EXIT'
    finally:
        reopened.close()


@pytest.mark.parametrize('carry', [2], indirect=True)
def test_new_round_still_requires_fresh_flat_positions(carry):
    runtime, e, h, journal, clock, tick, open_pair = carry
    complete_round(carry, asyncio.run(open_pair()))
    before = len(e.sent) + len(h.sent)
    for unavailable in (RuntimeError('position unavailable'), None):
        e.position_error = unavailable
        e.position = .01 if unavailable is None else 0
        tick(clock[0] + 2)
        assert asyncio.run(open_pair()) is None
        assert len(e.sent) + len(h.sent) == before


@pytest.mark.parametrize('carry', [6], indirect=True)
def test_live_loop_drives_multiple_rounds_without_the_old_one_round_guard(carry):
    runtime, e, h, journal, clock, tick, _ = carry

    async def controller():
        settled = set()
        while not runtime.stop.is_set():
            active = journal.active_live_cycle()
            if active is not None and active['phase'] == 'OPEN' and active['cycle_id'] not in settled:
                first_hour = (int(clock[0] // 3600) + 1) * 3600
                for venue in (e, h):
                    venue.settlements.extend(
                        FundingSettlement(f'{active["cycle_id"]}:{venue.key}:{ts}', ts, D('0'))
                        for ts in (first_hour, first_hour + 3600))
                settled.add(active['cycle_id'])
                tick(first_hour + 3601)
                runtime.changed.set()
            elif active is None:
                if journal.batch_state('batch')['position_cycles'] == 6:
                    if json.loads(runtime.status.path.read_text())['current_state'] == 'BATCH_COMPLETE':
                        runtime.stop.set()  # Test shutdown, not a live order/action.
                        return
                else:
                    await runtime._refresh_flat_lease()
                    runtime.changed.set()
            await asyncio.sleep(.01)

    async def scenario():
        await asyncio.wait_for(asyncio.gather(runtime.run(), controller()), timeout=10)

    asyncio.run(scenario())
    assert journal.batch_state('batch')['position_cycles'] == 6
    assert journal.active_live_cycle() is None and not journal.pending()
    assert e.position == h.position == 0
    assert len(e.sent) == len(h.sent) == 12
