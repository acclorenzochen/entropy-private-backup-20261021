"""Offline A/B rejection and unpaired repair checks through real adapters."""
import asyncio
from decimal import Decimal as D
import json

import pytest

from entropy_arb.v3.core import Direction, plan_open, price_limit
from entropy_arb.v3.runtime import choose_open_plan
from tests_v3.test_ab_adapters import (
    adapter_runtime, isolated_cwd_and_hl_network_guard, no_network, prepare_arm,
)
from tests_v3.test_runtime import complete_background_audit


def books(runtime, clock, *, short, outside_hedge_cap=False):
    e_price = D('103') if short else D('98.50')
    e, h = runtime.venues.entropy, runtime.venues.hedge
    e.book.apply_hl([[{'px': str(e_price), 'sz': '5'}],
                     [{'px': str(e_price + D('.01')), 'sz': '5'}]])
    bids = [{'px': '100', 'sz': '5'}]
    asks = [{'px': '100.01', 'sz': '5'}]
    if outside_hedge_cap:
        if short:
            asks = [{'px': '100.01', 'sz': '.01'}, {'px': '101.01', 'sz': '5'}]
        else:
            bids = [{'px': '100', 'sz': '.01'}, {'px': '99', 'sz': '5'}]
    h.book.apply_hl([bids, asks])
    for venue in (e, h):
        venue.book.last_update = clock.monotonic()
        venue.book.alive_since = clock.monotonic() - 1
    h.orders_feed.last_liveness_ts = clock.time()


def chosen_plan(runtime):
    return choose_open_plan(runtime._market(), runtime.instrument, runtime.settings, D('0'),
        entropy_venue=runtime.venues.entropy, hedge_venue=runtime.venues.hedge)


def assert_hedge_depth_exceeds_cap(runtime, units, *, short, room):
    direction = Direction.SHORT_ENTROPY if short else Direction.LONG_ENTROPY
    market = runtime._market()
    # This is an economically valid same-quantity signal, not a stale or lost signal.
    assert plan_open(market, runtime.instrument, runtime.settings, D('0'), direction, units) is not None
    side = direction.entropy_side.opposite
    _, marginal = market.hedge.quote(side, units, runtime.instrument.quantity_step)
    with pytest.raises(ValueError, match='physical hard cap'):
        price_limit(market.hedge.top(side), marginal, side, D(room), D('12'), runtime.instrument.hedge_tick)


async def drain_flat(runtime):
    for _ in range(6):
        await runtime._resolve_pending()
        await complete_background_audit(runtime)
        active = runtime.journal.active_live_cycle()
        if active is None:
            break
        await runtime._resume_cycle(active)
    assert runtime.journal.active_live_cycle() is None
    assert not runtime.journal.pending() and not runtime.stop.is_set()
    assert json.loads(runtime.status.path.read_text())['current_state'] != 'HALTED'


@pytest.mark.parametrize('short', [False, True])
def test_A_full_hedge_zero_entropy_repairs_flat_without_filled_outcome(tmp_path, monkeypatch, short):
    runtime, transport, journal, clock = adapter_runtime(tmp_path, monkeypatch)
    async def scenario():
        await prepare_arm(runtime, transport, clock, 'A', short=short)
        transport.outcomes['entropy'] = [{'fraction': 0}]
        await runtime._maybe_open(runtime._market(), D('0'))
        await drain_flat(runtime)
        attempt = journal.opening_experiment_state('ab-test')['current_attempt']
        assert attempt['slot'] == 1 and attempt['group_name'] == 'A' and attempt['terminal_flat']
        assert attempt['outcome'] != 'filled'
        cycle_id = attempt['cycle_id']
        opening = [row for row in journal.cycle_orders(cycle_id) if not row['reduce_only']]
        assert len(opening) == 2
        e = next(row for row in opening if row['venue'] == 'entropy')
        h = next(row for row in opening if row['venue'] == 'hedge')
        assert e['filled_units'] == 0 and h['filled_units'] == h['requested_units'] > 0
        assert all(not (0 < row['filled_units'] < row['requested_units']) for row in opening)
        repair = [row for row in transport.records if row['reduce_only']]
        assert len(repair) == 1 and repair[0]['venue'] == 'hedge'
        assert repair[0]['filled'] == runtime.instrument.quantity(h['filled_units'])
        assert repair[0]['is_buy'] is (not short)
        assert transport.positions == {'entropy': D('0'), 'hedge': D('0')}
        cycle = journal.db.execute('SELECT * FROM cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
        assert cycle['result_kind'] == 'repair'
        live = journal.db.execute('SELECT * FROM live_cycles WHERE cycle_id=?', (cycle_id,)).fetchone()
        assert live['phase'] == 'DONE' and live['basis_profit_basis_json'] == journal.BASIS_PROFIT_PENDING
        assert all(row['state'] == 'TERMINAL' for row in journal.cycle_orders(cycle_id))
        assert all(row['applied'] for row in journal.live_steps(cycle_id))
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('short', [False, True])
def test_A_hedge_marginal_outside_cap_rejects_before_slot_or_post(tmp_path, monkeypatch, short):
    runtime, transport, journal, clock = adapter_runtime(tmp_path, monkeypatch)
    async def scenario():
        await prepare_arm(runtime, transport, clock, 'A', short=short)
        books(runtime, clock, short=short, outside_hedge_cap=True)
        plan = chosen_plan(runtime)
        assert plan is not None
        assert_hedge_depth_exceeds_cap(runtime, plan.units, short=short, room='4')
        await runtime._maybe_open(runtime._market(), D('0'))
        state = journal.opening_experiment_state('ab-test')
        assert state['attempts'] == 0 and state['armed'] and state['opportunity_seq'] == 0
        assert state['groups'] == {'A': 0, 'B': 0} and state['current_attempt'] is None
        assert journal.active_live_cycle() is None and not journal.pending()
        assert transport.records == [] and runtime.venues.hedge.signer.orders == []
        assert transport.positions == {'entropy': D('0'), 'hedge': D('0')}
        assert not runtime.stop.is_set()
        assert json.loads(runtime.status.path.read_text())['current_state'] != 'HALTED'
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('short', [False, True])
def test_A_zero_retry_reprice_outside_cap_finishes_original_slot_without_post_or_halt(tmp_path, monkeypatch, short):
    runtime, transport, journal, clock = adapter_runtime(tmp_path, monkeypatch)
    async def scenario():
        await prepare_arm(runtime, transport, clock, 'A', short=short)
        books(runtime, clock, short=short)
        plan = chosen_plan(runtime)
        assert plan is not None
        for key in transport.outcomes:
            transport.outcomes[key] = [{'fraction': 0}]
        transport.gate_opening()
        task = asyncio.create_task(runtime._maybe_open(runtime._market(), D('0')))
        try:
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in transport.entered.values())), 1)
            admitted = journal.opening_experiment_state('ab-test')['current_attempt']
            cycle_id, opportunity = admitted['cycle_id'], admitted['opportunity_id']
            assert admitted['slot'] == 1 and admitted['group_name'] == 'A'
            assert len(transport.records) == 2 and len(journal.pending()) == 2
            assert all(row['qty'] == runtime.instrument.quantity(plan.units) for row in transport.records)
            books(runtime, clock, short=short, outside_hedge_cap=True)
            assert_hedge_depth_exceeds_cap(runtime, plan.units, short=short, room='8')
        finally:
            transport.release.set()
            await task
        await drain_flat(runtime)
        state = journal.opening_experiment_state('ab-test')
        attempt = state['current_attempt']
        assert state['attempts'] == 1 and state['groups'] == {'A': 1, 'B': 0}
        assert attempt['cycle_id'] == cycle_id and attempt['opportunity_id'] == opportunity
        assert attempt['slot'] == 1 and attempt['terminal_flat'] and attempt['outcome'] == 'zero'
        assert not state['armed']
        assert len(transport.records) == 2 and all(row['filled'] == 0 for row in transport.records)
        assert all(row['state'] == 'TERMINAL' for row in journal.cycle_orders(cycle_id))
        assert all(row['applied'] for row in journal.live_steps(cycle_id))
        assert transport.positions == {'entropy': D('0'), 'hedge': D('0')}
    try:
        asyncio.run(scenario())
    finally:
        journal.close()
