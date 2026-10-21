"""Offline admission, alternation and shared-engine A/B opening checks."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import json
import time

import pytest

from entropy_arb.v3.core import Settings, Side
from entropy_arb.v3.journal import Intent, Journal, PendingOrder, Result
from entropy_arb.v3.live_host import BatchAuthorization, LiveAuthorizationError
from entropy_arb.v3.runtime import LiveRuntime
from tests_v3.test_open_handoff import streams
from tests_v3.test_runtime import setup_runtime


def setup_ab(tmp_path, *, rounds=1):
    old, e, h, journal = setup_runtime(tmp_path)
    streams(e, h)
    settings = replace(old.settings, opening_experiment_id='opening-ab-test',
        single_cycle_review_loss_usd=None, entropy_orders_per_min=100,
        first_rooms_bps=(D('3'),) * rounds, hedge_rooms_bps=(D('4'),) * rounds)
    authorization = replace(old.authorization, max_position_cycles=20,
        loss_budget_usd=None, opening_experiment_id=settings.opening_experiment_id)
    runtime = LiveRuntime(settings=settings, legacy_config=old.cfg,
        authorization=authorization, pair_id=old.pair_id, deployment=old.deployment,
        venues=old.venues, journal=journal, status=old.status, observer=old.observer,
        stop=old.stop, reference_checkpoint_path=old.reference_checkpoint_path)
    return runtime, e, h, journal


async def open_once(runtime):
    await runtime._maybe_open(runtime._market(), D('0'))
    return runtime.journal.active_live_cycle()


def test_A_atomic_pair_parallel_equal_then_single_cycle_inventory(tmp_path):
    runtime, e, h, journal = setup_ab(tmp_path)
    async def scenario():
        await runtime.initialize()
        ready = asyncio.Event()
        started = []
        for venue in (e, h):
            original = venue.send_taker
            async def send(*, key=venue.key, original=original, **kwargs):
                row = journal.opening_experiment_state('opening-ab-test')['current_attempt']
                assert row['slot'] == 1 and row['group_name'] == 'A'
                assert len(journal.pending()) == 2
                assert runtime._lighter_increase_guard(kwargs['order_ref']) is None
                started.append((key, kwargs['qty']))
                if len(started) == 2:
                    ready.set()
                await ready.wait()
                return await original(**kwargs)
            venue.send_taker = send
        cycle = await asyncio.wait_for(open_once(runtime), 2)
        assert started[0][1] == started[1][1]
        assert cycle['entropy_units'] == -cycle['hedge_units'] != 0
        assert cycle['phase'] == 'OPEN'
        for step in journal.live_steps(cycle['cycle_id']):
            metric = journal.economics_step(step['step_key'])
            assert metric['execution_path'] == 'parallel'
            assert metric['decision_units'] == step['requested_units']
            assert metric['decision_quote_json'] is not None
            assert metric['attempt_decision_ts'] is not None
            assert 'recovered_original_decision' not in metric['missing_fields_json']
            assert metric['paired_executable_basis_bps'] is not None
        assert runtime._known_positions(cycle) == (cycle['entropy_units'], cycle['hedge_units'])
        state = journal.opening_experiment_state('opening-ab-test')
        assert state['attempts'] == 1 and state['groups'] == {'A': 1, 'B': 0}
        assert not state['armed'] and not state['current_attempt']['terminal_flat']
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


async def finish_zero(runtime):
    await runtime._audit_positions()
    cycle = runtime.journal.active_live_cycle()
    if cycle is not None:
        await runtime._finish_flat(cycle, zero_fill=True)
    assert runtime.journal.active_live_cycle() is None


def test_B_uses_terminal_actual_and_persistent_contingent_pair(tmp_path):
    runtime, e, h, journal = setup_ab(tmp_path)
    async def scenario():
        await runtime.initialize()
        e.fill_fractions = h.fill_fractions = [0]
        await open_once(runtime)
        await finish_zero(runtime)
        # An executable signal continuing through flat is still the same episode.
        await open_once(runtime)
        assert journal.opening_experiment_state('opening-ab-test')['attempts'] == 1
        journal.rearm_opening_opportunity('opening-ab-test')
        e.fill_fractions, h.fill_fractions = [.5], [1]
        release = asyncio.Event()
        entered = asyncio.Event()
        original = e.send_taker
        async def send(**kwargs):
            entered.set()
            await release.wait()
            return await original(**kwargs)
        e.send_taker = send
        before = len(h.sent)
        task = asyncio.create_task(open_once(runtime))
        await entered.wait()
        attempt = journal.opening_experiment_state('opening-ab-test')['current_attempt']
        assert attempt['slot'] == 2 and attempt['group_name'] == 'B'
        assert json.loads(attempt['pair_plan_json'])['hedge_units'] == 'terminal_entropy_unmatched'
        assert attempt['hedge_ref'] is None and len(journal.pending()) == 1
        assert len(h.sent) == before
        release.set()
        cycle = await task
        assert h.sent[-1]['qty'] == pytest.approx(e.sent[-1]['qty'] / 2, abs=.00005)
        assert not h.sent[-1]['reduce_only']
        assert cycle['entropy_units'] == -cycle['hedge_units']
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_twenty_zero_attempts_alternate_and_do_not_stop_recovery_loop(tmp_path):
    runtime, e, h, journal = setup_ab(tmp_path)
    async def scenario():
        await runtime.initialize()
        e.fill_fractions, h.fill_fractions = [0] * 30, [0] * 30
        for slot in range(1, 21):
            journal.rearm_opening_opportunity('opening-ab-test')
            await open_once(runtime)
            current = journal.opening_experiment_state('opening-ab-test')['current_attempt']
            assert current['slot'] == slot and current['group_name'] == ('A' if slot % 2 else 'B')
            await finish_zero(runtime)
        sent = (len(e.sent), len(h.sent))
        journal.rearm_opening_opportunity('opening-ab-test')
        await open_once(runtime)
        assert (len(e.sent), len(h.sent)) == sent
        assert not runtime.stop.is_set()
        state = journal.opening_experiment_state('opening-ab-test')
        assert state['groups'] == {'A': 10, 'B': 10} and state['finished']
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def journal_admission(journal, *, cycle_id='one', direction='long_entropy'):
    cycle = dict(batch_id='ab', cycle_id=cycle_id, direction=direction,
        planned_units=100, reference_bps=D('0'), entry_basis_bps=D('4'),
        started_ts=time.time())
    first = f'{cycle_id}:open_first:0'
    plans = [(Intent(f'{cycle_id}-e', cycle_id, 'entropy', Side.BUY, 100, D('100')),
              dict(step_key=first, execution_group_id=first, quantity_step=D('.01'))),
             (Intent(f'{cycle_id}-h', cycle_id, 'hedge', Side.SELL, 100, D('100')),
              dict(step_key=f'{cycle_id}:open_hedge:0', execution_group_id=first,
                   parent_step_key=first, quantity_step=D('.01')))]
    return dict(cycle=cycle, plans=plans, pair_plan=dict(entropy_units=100, hedge_units=100,
        entropy_side='buy', hedge_side='sell'), request=dict(now=time.time(), limit=100,
        reserve_for_completion=0, completion=False, request_kind='open_first'))


def prepare_journal(tmp_path):
    journal = Journal(tmp_path / 'ab.sqlite', identity='pair')
    journal.ensure_batch('ab', max_position_cycles=20, loss_budget_usd=None)
    journal.ensure_opening_experiment('experiment', batch_id='ab', contract={'pair': 'pair', 'hash': 'hash'})
    return journal


def test_atomic_admission_failure_rolls_back_all_rows_and_quota(tmp_path, monkeypatch):
    journal = prepare_journal(tmp_path)
    original = journal._insert_submitting
    def fail_second(intent):
        if intent.venue == 'hedge':
            raise OSError('durable write failed')
        original(intent)
    monkeypatch.setattr(journal, '_insert_submitting', fail_second)
    try:
        with pytest.raises(OSError):
            journal.admit_opening_attempt('experiment', **journal_admission(journal))
        assert journal.active_live_cycle() is None and journal.pending() == []
        assert journal.opening_experiment_state('experiment')['attempts'] == 0
        assert journal.opening_experiment_state('experiment')['armed']
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 0
    finally:
        journal.close()


def test_crash_after_pair_admission_preserves_same_ids_and_slot(tmp_path):
    journal = prepare_journal(tmp_path)
    journal.admit_opening_attempt('experiment', **journal_admission(journal))
    expected = {o['order_ref'] for o in journal.pending()}
    journal.close()
    journal = Journal(tmp_path / 'ab.sqlite', identity='pair')
    try:
        journal.ensure_opening_experiment('experiment', batch_id='ab', contract={'pair': 'pair', 'hash': 'hash'})
        assert {o['order_ref'] for o in journal.pending()} == expected
        assert journal.opening_experiment_state('experiment')['attempts'] == 1
        assert not journal.opening_experiment_state('experiment')['armed']
        with pytest.raises(PendingOrder):
            journal.admit_opening_attempt('experiment', **journal_admission(journal, cycle_id='two'))
    finally:
        journal.close()


@pytest.mark.parametrize('change', ['batch', 'contract'])
def test_experiment_restart_cannot_drift_or_reset(tmp_path, change):
    journal = prepare_journal(tmp_path)
    try:
        journal.ensure_batch('other', max_position_cycles=20, loss_budget_usd=None)
        with pytest.raises(ValueError):
            journal.ensure_opening_experiment('experiment',
                batch_id='other' if change == 'batch' else 'ab',
                contract={'pair': 'other'} if change == 'contract' else {'pair': 'pair', 'hash': 'hash'})
        with pytest.raises(Exception):
            journal.ensure_opening_experiment('new-id', batch_id='ab', contract={})
        journal.ensure_batch('ordinary', max_position_cycles=100, loss_budget_usd=D('3'))
        with pytest.raises(ValueError):
            journal.ensure_batch('ordinary', max_position_cycles=100, loss_budget_usd=None)
    finally:
        journal.close()


@pytest.mark.parametrize('override', [
    dict(opening_experiment_id='other'), dict(max_position_cycles=100),
    dict(loss_budget_usd='3'), dict(max_notional_usd='201'), dict(approved_live=False)])
def test_only_exact_explicit_experiment_can_disable_budget(tmp_path, override):
    data = dict(batch_id='new-ab', pair_identity='pair', max_position_cycles=20,
        loss_budget_usd=None, max_notional_usd='200', approved_live=True,
        opening_experiment_id='experiment')
    data.update(override)
    path = tmp_path / 'auth.json'; path.write_text(json.dumps(data))
    with pytest.raises(LiveAuthorizationError):
        BatchAuthorization.load(path, expected_pair='pair', configured_max_notional=D('200'),
            expected_opening_experiment_id='experiment')


def test_experiment_disabled_budget_does_not_disable_ordinary_loss_accounting(tmp_path):
    journal = prepare_journal(tmp_path)
    try:
        assert journal.observe_open_risk('ab', active_cycle_net_pnl_usd=D('-99'))
        assert journal.batch_state('ab')['loss_budget_usd'] is None
        journal.ensure_batch('ordinary', max_position_cycles=100, loss_budget_usd=D('3'))
        assert not journal.observe_open_risk('ordinary', active_cycle_net_pnl_usd=D('-3.01'))
        assert journal.batch_state('ordinary')['closed_for_entries']
    finally:
        journal.close()


def test_experiment_cannot_use_funding_or_larger_cap():
    with pytest.raises(ValueError):
        Settings(opening_experiment_id='experiment', strategy_mode='funding_hedge')
    with pytest.raises(ValueError):
        Settings(opening_experiment_id='experiment', max_notional_usd=D('201'))


def test_DONE_crash_before_attempt_finish_is_repaired_once_without_new_slot(tmp_path, monkeypatch):
    runtime, e, h, journal = setup_ab(tmp_path)
    async def scenario():
        await runtime.initialize()
        e.fill_fractions, h.fill_fractions = [0], [0]
        await open_once(runtime)
        await runtime._audit_positions()
        original = journal.finish_opening_attempt
        def crash(*args, **kwargs):
            raise OSError('crash after DONE durable commit')
        monkeypatch.setattr(journal, 'finish_opening_attempt', crash)
        with pytest.raises(OSError):
            await runtime._finish_flat(journal.active_live_cycle(), zero_fill=True)
        state = journal.opening_experiment_state('opening-ab-test')
        assert not state['current_attempt']['terminal_flat']
        assert journal.active_live_cycle() is None
        sent = len(e.sent), len(h.sent)
        monkeypatch.setattr(journal, 'finish_opening_attempt', original)
        runtime._ensure_opening_experiment()
        runtime._ensure_opening_experiment()
        repaired = journal.opening_experiment_state('opening-ab-test')
        assert repaired['attempts'] == 1 and repaired['current_attempt']['terminal_flat']
        assert repaired['current_attempt']['outcome'] == 'zero'
        assert not repaired['armed'] and (len(e.sent), len(h.sent)) == sent
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_parallel_execution_cost_uses_each_legs_own_submission_not_later_E_mark(side):
    from entropy_arb.v3.economics import summarize_attempts
    def quote(e, h):
        return json.dumps(dict(quantity_step='0.1', entropy=dict(asks=[[e, 10]], bids=[[e, 10]]),
            hedge=dict(asks=[[h, 10]], bids=[[h, 10]])))
    first = dict(cycle_id='parallel', execution_group_id='g', step_key='e',
        purpose='open_first', venue='entropy', side=side, attempt=0, requested_units=10,
        result_units=10, actual_fill_vwap='100', quantity_step='0.1',
        decision_quote_json=quote('100', '100'), submit_quote_json=quote('100', '100'),
        fill_mark_quote_json=quote('100', '110'), fill_mark_time_source='runtime_consumed_local',
        terminal_observed_ts=20, execution_path='parallel', order_state='TERMINAL')
    hedge = dict(first, step_key='h', parent_step_key='e', purpose='open_hedge',
        venue='hedge', side='sell' if side == 'buy' else 'buy', terminal_observed_ts=10)
    pair = summarize_attempts([first, hedge], [{'cycle_id':'parallel','direction':'long_entropy'}])['paired_groups'][0]
    assert pair['measurement_quality'] == 'complete'
    assert D(pair['total_execution_loss_usd']) == 0
    assert D(pair['hedge_execution_loss_usd']) == 0
    assert pair['c_basis_bps'] is pair['b_to_c_adverse_bps'] is pair['c_to_d_adverse_bps'] is None
    assert pair['execution_loss_benchmark'] == 'per_leg_submit_quotes_at_actual_filled_units'


def test_source_or_config_fingerprint_cannot_drift_on_resume(tmp_path, monkeypatch):
    runtime, e, h, journal = setup_ab(tmp_path)
    try:
        asyncio.run(runtime.initialize())
        assert runtime.status.base['opening_experiment_contract']['source_build_hash']
        monkeypatch.setattr(runtime, '_opening_source_build_hash', lambda: 'changed-source')
        with pytest.raises(ValueError, match='contract changed'):
            runtime._ensure_opening_experiment()
    finally:
        journal.close()


@pytest.mark.parametrize('e_exchange,h_exchange,expected', [(10, 20, 10), (None, 20, None), (10, None, None)])
def test_parallel_first_observation_and_exchange_time_have_separate_sources(tmp_path, monkeypatch, e_exchange, h_exchange, expected):
    runtime, e, h, journal = setup_ab(tmp_path)
    async def scenario():
        await runtime.initialize()
        cycle = await open_once(runtime)
        rows = journal.economics_steps(cycle['cycle_id'])
        for row in rows:
            row['result_ts'] = 4 if row['venue'] == 'entropy' else 3
            row['actual_fill_ts'] = e_exchange if row['venue'] == 'entropy' else h_exchange
        captured = {}
        monkeypatch.setattr(journal, 'economics_steps', lambda _: rows)
        monkeypatch.setattr(journal, 'capture_entry_economics', lambda _, **kwargs: captured.update(kwargs))
        runtime._capture_entry_economics(cycle['cycle_id'])
        assert captured['first_fill_observed_ts'] == 3
        assert captured['first_fill_exchange_ts'] == expected
        assert captured['first_fill_mark_basis_bps'] is None
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_long_dispatch_admission_wait_requires_new_authoritative_flat_audit(tmp_path, monkeypatch):
    runtime, e, h, journal = setup_ab(tmp_path)
    async def scenario():
        await runtime.initialize()
        original = runtime.brokers['hedge'].prepare_dispatch
        async def wait_and_expire(*args, **kwargs):
            token = await original(*args, **kwargs)
            runtime._last_position_evidence = replace(runtime._last_position_evidence,
                observed_ts=time.time() - runtime.settings.position_audit_seconds - 1)
            return token
        monkeypatch.setattr(runtime.brokers['hedge'], 'prepare_dispatch', wait_and_expire)
        await open_once(runtime)
        assert journal.opening_experiment_state('opening-ab-test')['attempts'] == 0
        assert journal.active_live_cycle() is None and not journal.pending()
        assert not e.sent and not h.sent and runtime._position_audit_requested.is_set()
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_example_config_keeps_edge_policy_but_authorization_is_inactive():
    from pathlib import Path
    from main_v3 import load_settings
    root = Path(__file__).resolve().parents[1]
    settings = load_settings(root / 'ab-opening/config.v3.opening-ab.example.json')
    assert settings.opening_experiment_id and settings.max_notional_usd == D('200')
    assert settings.exit_min_gross_profit_bps == D('2.5')
    assert settings.lighter_zero_fee_batch_id is None
    with pytest.raises(LiveAuthorizationError, match='not approved'):
        BatchAuthorization.load(root / 'ab-opening/batch.v3.opening-ab.authorization.example.json',
            expected_pair='default:hl:SNDK:lighter:SNDK', configured_max_notional=D('200'),
            expected_opening_experiment_id=settings.opening_experiment_id)
