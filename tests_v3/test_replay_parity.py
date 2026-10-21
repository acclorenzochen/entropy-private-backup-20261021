"""Offline synthetic parity: real LiveRuntime, SQLite Journal and fake venues only."""
import asyncio
import json
from decimal import Decimal as D
import time

import pytest

from entropy_arb.v3.core import Settings, Side
from entropy_arb.v3.journal import Journal
from tests_v3.test_opportunities import replay_frame, replay_calibration
from tests_v3.test_runtime import complete_background_audit, setup_runtime
from tools.v3_opportunities import _Budget, _draw, calibrate, replay, load_snapshots


@pytest.fixture(autouse=True)
def isolated_runtime_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize('limit', [3, 4, 6])
def test_request_reserve_and_exact_60_second_boundary_match_journal(tmp_path, limit):
    cfg = Settings(entropy_orders_per_min=limit)
    budget = _Budget(cfg)
    with_journal = Journal(tmp_path / 'budget.sqlite', identity='offline')
    try:
        for ts, completion in [(float(i), False) for i in range(limit + 1)] + [
                (10., True), (11., True), (12., True), (59.999, False),
                (60., False), (61., True), (72., False)]:
            expected = with_journal.reserve_request('entropy', now=ts, limit=limit,
                reserve_for_completion=min(2, max(0, limit - 1)), completion=completion)
            assert budget.use('entropy', ts, completion=completion) == expected
    finally:
        with_journal.close()


@pytest.mark.parametrize('boundary', ['invalid', 'session', 'gap', 'sequence'])
def test_gap_retains_pending_and_does_not_restart_account(boundary):
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9) for i in range(12)]
    if boundary == 'invalid':
        rows.insert(5, {'session_id': 's', 'pair_id': 'p', 'capture_wall_ts': 2.25,
                        'capture_mono_ns': 2250000000, 'valid': False})
    elif boundary == 'session':
        for row in rows[5:]:
            row['session_id'] = 'new-session'
    elif boundary == 'sequence':
        for row in rows[5:]:
            row['sample_seq'] += 1
    else:
        for row in rows[5:]:
            row['capture_wall_ts'] += 3
            row['capture_mono_ns'] += 3000000000
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2),
                    replay_calibration(latency=1.), scenario='neutral', unit_override=10)
    assert result['status'] == 'CENSORED'
    assert result['order_count'] == 1
    unfinished = result['incomplete_cycles'][0]
    assert unfinished['pending_order_ref'] is not None
    assert len(unfinished['request_window']['entropy']) == 1
    assert result['realized_pnl_per_hour_usd'] is None


def test_unknown_resolves_same_identity_without_new_first_leg():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9) for i in range(9)]
    profile = replay_calibration()
    profile['groups']['open:entropy'] = [{'fraction': None, 'latency_seconds': .1,
        'resolve_after_seconds': .1, 'resolved_fraction': 1.}]
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2), profile,
                    scenario='neutral', unit_override=10)
    submitted = [row for row in result['action_trace'] if row['event'] == 'submit']
    resolution = [row for row in result['action_trace'] if row['event'] == 'resolve']
    assert len(submitted) == 2
    assert resolution[0]['order_ref'] == submitted[0]['order_ref']
    assert result['incomplete_cycles'][0]['phase'] == 'OPEN'
    assert result['resolved_unknown_order_count'] == 1


def test_joint_draw_never_combines_unrelated_latency_and_fraction():
    profile = {'groups': {'open:entropy': [
        {'fraction': .25, 'latency_seconds': .2},
        {'fraction': .75, 'latency_seconds': 4.}]}}
    for i in range(20):
        sample = _draw(profile, 'open', 'entropy', str(i), 'neutral')
        assert (sample['fraction'], sample['latency_seconds']) in {(.25, .2), (.75, 4.)}


def test_decision_and_handoff_delays_affect_submits_not_fill_price():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9) for i in range(14)]
    profile = replay_calibration()
    profile['groups']['open:entropy'][0].update(
        decision_to_submit_seconds=1., handoff_seconds=1.)
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2), profile,
                    scenario='neutral', unit_override=10)
    submitted = [row for row in result['action_trace'] if row['event'] == 'submit']
    assert submitted[0]['ts'] == 3.
    assert submitted[1]['ts'] == 4.5
    assert result['incomplete_cycles'][0]['phase'] == 'OPEN'


def test_warmup_is_consumed_without_orders_or_profit():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9) for i in range(10)]
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2),
                    replay_calibration(), scenario='neutral', unit_override=10,
                    trade_start_ts=3., score_start_ts=3.)
    submits = [row for row in result['action_trace'] if row['event'] == 'submit']
    assert submits[0]['ts'] == 3.
    assert all(row['ts'] >= 3. for row in submits)
    assert result['window_hours'] == 1.5 / 3600


def test_calibration_links_parent_handoff_cost_and_actual_fee():
    parent = {'step_key': 'e', 'purpose': 'open_first', 'venue': 'entropy',
        'requested_units': 20, 'result_units': 10, 'side': 'buy',
        'order_state': 'TERMINAL', 'attempt_decision_ts': 1., 'submit_ts': 2.,
        'terminal_result_ts': 3., 'actual_fill_vwap': '100.01',
        'actual_fee_usd': '.01', 'submit_quote_json': {
            'entropy': {'asks': [['100', 20]]}}}
    child = {'parent_step_key': 'e', 'purpose': 'open_hedge', 'venue': 'hedge',
        'submit_ts': 3.5, 'terminal_result_ts': 4., 'requested_units': 10,
        'result_units': 10, 'order_state': 'TERMINAL'}
    profile = calibrate([parent, child], [], 5., D('.01'))
    sample = profile['groups']['open:entropy'][0]
    assert sample['fraction'] == .5
    assert sample['decision_to_submit_seconds'] == sample['latency_seconds'] == 1.
    assert sample['handoff_seconds'] == .5
    assert sample['execution_cost_bps'] == pytest.approx(1.)
    assert sample['fee_bps'] == pytest.approx(.01 / 10.001 * 10000)
    assert sample['execution_ts'] is None


def test_policy_length_not_hardcoded_to_three_attempts():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9) for i in range(9)]
    cfg = Settings(cold_seconds=1., cold_samples=2, open_first_rooms_bps=(D('3'),))
    result = replay(rows, cfg, replay_calibration(fraction=0.),
                    scenario='neutral', unit_override=10)
    assert result['zero_fill_sequence_count'] >= 1
    submits = [row for row in result['action_trace'] if row['event'] == 'submit']
    assert all(row['phase'] == 'OPEN_E' for row in submits)
    assert result['complete_cycle_count'] == 0


def test_no_market_or_funding_data_does_not_claim_zero_profit():
    result = replay([], Settings(), calibrate([], [], 0.), scenario='neutral')
    assert result['realized_pnl_usd'] is None
    assert result['realized_pnl_per_hour_usd'] is None
    assert result['profit_lower_bound'] is None


def test_file_rotation_does_not_reset_pending_or_reference(tmp_path):
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                        99.9 if i < 10 else 100.2) for i in range(13)]
    paths = [tmp_path / 'first.jsonl', tmp_path / 'second.jsonl']
    for path, part in zip(paths, [rows[:5], rows[5:]]):
        path.write_text('\n'.join(json.dumps(row) for row in part))
    cfg = Settings(cold_seconds=1., cold_samples=2)
    assert replay(load_snapshots(paths), cfg, replay_calibration(),
                  scenario='neutral', unit_override=10) == replay(
        rows, cfg, replay_calibration(), scenario='neutral', unit_override=10)


def test_unknown_zero_and_missing_terminal_evidence_are_censored():
    base = {'purpose': 'open_first', 'venue': 'entropy', 'requested_units': 10,
            'result_units': 0, 'submit_ts': 1., 'terminal_result_ts': 2.}
    result = calibrate([dict(base, order_state='UNKNOWN'),
                        dict(base, order_state='TERMINAL')], [], 3.)
    assert result['counts']['open:entropy'] == 1
    assert result['missing']['open:entropy:censored_nonterminal'] == 1
    assert result['groups']['open:entropy'][0]['fraction'] == 0
    assert result['funding_per_notional_hour'] is None


@pytest.mark.parametrize('mode', ['full', 'partial', 'repair', 'unknown', 'close', 'unwind'])
def test_open_action_positions_and_cash_match_fake_live_runtime(tmp_path, mode):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    cfg = Settings(cold_seconds=1., cold_samples=2, max_notional_usd=D('30'))
    runtime.settings = cfg
    # Fixture-only exchange contracts match the recorded instrument exactly.
    for venue in (entropy, hedge):
        venue.size_decimals = 2
        venue.min_base = .1
        venue.min_quote = 1
        venue.fee_bps = .5
    entropy.set_book(99.9, 99.9)
    hedge.set_book(100, 100)
    entropy.fill_fractions = [.5] if mode == 'partial' else []
    hedge.fill_fractions = ([0, 0, 0, 1] if mode == 'repair' else
                            [0] * 6 if mode == 'unwind' else [])
    entropy.unknown_once = mode == 'unknown'
    live_actions = []
    for key, venue in [('entropy', entropy), ('hedge', hedge)]:
        original = venue.send_taker

        async def send(*, _original=original, _venue=venue, _key=key, **kwargs):
            live_actions.append((_key, 'buy' if kwargs['is_buy'] else 'sell',
                                 round(kwargs['qty'] * 100), kwargs['reduce_only']))
            receipt = await _original(**kwargs)
            terminal = _venue.pending.get(kwargs['order_ref'], receipt)
            if terminal['filled_base']:
                price = _venue.book.mid()
                fee = float(D(str(price)) * D(str(terminal['filled_base'])) * D('.00005'))
                terminal.update(avg_px=price, fee_usd=fee)
                _venue.fee_fills[-1].update(price=price, fee_usd=fee)
            return receipt

        venue.send_taker = send

    async def run_live():
        await runtime.initialize()
        journal.create_live_cycle(batch_id='batch', cycle_id='parity',
            direction='long_entropy', planned_units=20, reference_bps=D('0'),
            entry_basis_bps=D('-10'), started_ts=time.time())
        await runtime._open_first(journal.active_live_cycle())
        if journal.pending():
            await runtime._resolve_pending()
            await runtime._audit_positions()
            await runtime._resume_cycle(journal.active_live_cycle())
        if mode == 'close':
            entropy.set_book(100.2, 100.2)
            await runtime._resume_cycle(journal.active_live_cycle())
        await complete_background_audit(runtime)
        return journal.active_live_cycle() or dict(journal.db.execute(
            "SELECT * FROM live_cycles WHERE cycle_id='parity'").fetchone())

    try:
        live = asyncio.run(run_live())
        rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                            100.2 if mode == 'close' and i >= 10 else 99.9)
                for i in range(13 if mode in {'close', 'unwind'} else
                               11 if mode == 'repair' else 9 if mode == 'unknown' else 7)]
        profile = replay_calibration()
        if mode == 'partial':
            profile['groups']['open:entropy'][0]['fraction'] = .5
        if mode in {'repair', 'unwind'}:
            profile['groups']['open:hedge'][0]['fraction'] = 0
        if mode == 'unwind':
            profile['groups']['repair:hedge'][0]['fraction'] = 0
        if mode == 'unknown':
            profile['groups']['open:entropy'] = [{'fraction': None, 'latency_seconds': .1,
                'resolve_after_seconds': .1, 'resolved_fraction': 1.}]
        result = replay(rows, cfg, profile, scenario='neutral', unit_override=20)
        replay_actions = [(row['venue'], row['side'], row['units'], row['reduce_only'])
                          for row in result['action_trace'] if row['event'] == 'submit']
        assert replay_actions == live_actions
        canonical = journal.db.execute(
            "SELECT result_kind FROM cycles WHERE cycle_id='parity'").fetchone()
        if mode in {'close', 'unwind'}:
            end = result['complete_cycles'][0]
            assert live['phase'] == 'DONE'
            assert live['entropy_units'] == live['hedge_units'] == 0
            assert D(end['pnl_usd']) == D(live['cash_net_usd'])
            assert canonical is not None
            assert end['result_kind'] == canonical['result_kind']
            assert canonical['result_kind'] == ('clean' if mode == 'close' else 'repair')
        else:
            # Still OPEN, including a repaired pair: no canonical completion
            # category exists yet, so do not invent or compare a final label.
            assert canonical is None or canonical['result_kind'] is None
            assert result['complete_cycles'] == []
            end = result['incomplete_cycles'][0]
            assert end['phase'] == live['phase'] == 'OPEN'
            assert (end['entropy_units'], end['hedge_units']) == (
                live['entropy_units'], live['hedge_units'])
            assert D(end['known_cash_usd']) == D(live['cash_net_usd'])
            assert end['pending_order_ref'] is None
        assert not journal.pending()
    finally:
        journal.close()


@pytest.mark.parametrize('first_leg', ['open', 'close'])
def test_unknown_known_partial_residual_recovery_matches_live(tmp_path, first_leg):
    """Opening UNKNOWN blocks new exposure; closing UNKNOWN permits peer reduction."""
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    cfg = Settings(cold_seconds=1., cold_samples=2, max_notional_usd=D('30'))
    runtime.settings = cfg
    actions, original_refs = [], []
    allow_terminal = False
    for venue in (entropy, hedge):
        venue.size_decimals = 2
        venue.min_base = .1
        venue.min_quote = 1
        venue.fee_bps = .5
    entropy.set_book(99.9, 99.9)
    hedge.set_book(100, 100)

    for key, venue in [('entropy', entropy), ('hedge', hedge)]:
        original = venue.send_taker

        async def send(*, _original=original, _venue=venue, _key=key, **kwargs):
            actions.append((_key, 'buy' if kwargs['is_buy'] else 'sell',
                            round(kwargs['qty'] * 100), kwargs['reduce_only']))
            receipt = await _original(**kwargs)
            terminal = _venue.pending.get(kwargs['order_ref'], receipt)
            price = _venue.book.mid()
            fee = float(D(str(price)) * D(str(terminal['filled_base'])) * D('.00005'))
            terminal.update(avg_px=price, fee_usd=fee)
            if terminal['filled_base']:
                _venue.fee_fills[-1].update(price=price, fee_usd=fee)
            if receipt['unresolved']:
                original_refs.append(kwargs['order_ref'])
                return {**terminal, 'status': 'timeout', 'unresolved': True}
            return terminal

        venue.send_taker = send

    async def resolve(order_ref, timeout=None):
        terminal = entropy.pending[order_ref]
        return dict(terminal) if allow_terminal else {
            **terminal, 'status': 'timeout', 'unresolved': True}

    entropy.resolve_order = resolve

    async def live_run():
        nonlocal allow_terminal
        await runtime.initialize()
        journal.create_live_cycle(batch_id='batch', cycle_id='known-partial',
            direction='long_entropy', planned_units=20, reference_bps=D('0'),
            entry_basis_bps=D('-10'), started_ts=time.time())
        if first_leg == 'close':
            await runtime._open_first(journal.active_live_cycle())
            entropy.set_book(100.2, 100.2)
            journal.update_live_cycle('known-partial', phase='CLOSE_FIRST', updated_ts=time.time())
        entropy.fill_fractions = [.5]
        entropy.unknown_once = True
        if first_leg == 'open':
            await runtime._open_first(journal.active_live_cycle())
        else:
            # Persisted unresolved _step is a real resume boundary, not a call
            # to a shared replay transition helper.
            result = await runtime._step(journal.active_live_cycle(),
                purpose='close_first_2', attempt=0, venue_key='entropy', side=Side.SELL,
                units=20, reduce_only=True, room_bps=D('3'), hard_cap_bps=D('10'),
                next_phase='CLOSE_HEDGE', completion=True)
            assert not result.terminal and result.cumulative_units == 10
        assert len(journal.pending()) == 1
        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        covered = dict(journal.active_live_cycle())
        assert (covered['entropy_units'], covered['hedge_units']) == (
            10, 0 if first_leg == 'open' else -10)
        if first_leg == 'open':
            assert actions == [('entropy', 'buy', 20, False)]
        assert journal.pending()[0]['order_ref'] == original_refs[0]
        count = len(actions)
        allow_terminal = True
        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        await complete_background_audit(runtime)
        final = journal.active_live_cycle() or dict(journal.db.execute(
            "SELECT * FROM live_cycles WHERE cycle_id='known-partial'").fetchone())
        # The original fill applies once. Only a now-terminal opening can add
        # its exact matching hedge; a covered closing never reduces twice.
        assert len(actions) == count + (first_leg == 'open')
        assert not journal.pending()
        return covered, final

    try:
        covered, final = asyncio.run(live_run())
        rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                            100.2 if first_leg == 'close' and i >= 10 else 99.9)
                for i in range(16 if first_leg == 'close' else 11)]
        profile = replay_calibration()
        profile['groups'][first_leg + ':entropy'] = [{
            'fraction': None, 'latency_seconds': .1, 'known_cumulative_units': 10,
            'resolve_after_seconds': 2., 'resolved_fraction': .5,
            'position_evidence': {'authoritative': True, 'entropy_units': 10,
                                  'hedge_units': -20 if first_leg == 'close' else 0}}]
        result = replay(rows, cfg, profile, scenario='neutral', unit_override=20)
        submits = [row for row in result['action_trace'] if row['event'] == 'submit']
        assert [(row['venue'], row['side'], row['units'], row['reduce_only'])
                for row in submits] == actions
        assert submits[-1]['reduce_only'] is (first_leg == 'close')
        assert submits[-1]['units'] == 10 and submits[-1]['venue'] == 'hedge'
        before_terminal = replay(rows[:-2], cfg, profile, scenario='neutral', unit_override=20)
        pending = before_terminal['incomplete_cycles'][0]
        assert pending['pending_order_ref'] is not None
        assert pending['pending_known_cumulative_units'] == 10
        assert (pending['entropy_units'], pending['hedge_units']) == (
            covered['entropy_units'], covered['hedge_units'])
        assert D(pending['known_cash_usd']) == D(covered['cash_net_usd'])
        state = result['final_account_state']
        assert state['phase'] == final['phase'] or state['phase'] == 'FLAT' and final['phase'] == 'DONE'
        assert (state['entropy_units'], state['hedge_units']) == (final['entropy_units'], final['hedge_units'])
        assert D(state['known_cash_usd']) == D(final['cash_net_usd'])
        assert state['pending_order_ref'] is None
        resolved = [row for row in result['action_trace'] if row['event'] == 'resolve_terminal']
        assert resolved[0]['applied_position_increment'] == 0
        if first_leg == 'open':
            assert result['action_trace'].index(resolved[0]) < result['action_trace'].index(submits[-1])
    finally:
        journal.close()


@pytest.mark.parametrize('settlement', ['pending', 'unavailable', 'conflict'])
def test_unmodeled_settlement_censors_profit_and_retains_risk(settlement):
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else
                        99.9 if i < 10 else 100.2) for i in range(16)]
    profile = replay_calibration()
    profile['groups']['close:entropy'][0]['settlement_status'] = settlement
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2), profile,
                    scenario='neutral', unit_override=10)
    assert result['status'] == 'CENSORED'
    assert result['complete_cycle_count'] == 0
    assert result['realized_pnl_usd'] is None
    assert result['realized_pnl_per_hour_usd'] is None
    assert result['final_account_state']['entropy_units'] == 0
    assert result['final_account_state']['hedge_units'] == -10


def test_unknown_open_resume_blocks_new_exposure(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.unknown_once = True
    entropy.fill_fractions = [.5]
    entropy.min_quote = hedge.min_quote = 1

    async def unresolved(order_ref, timeout=None):
        return {**entropy.pending[order_ref], 'status': 'timeout', 'unresolved': True}

    entropy.resolve_order = unresolved

    async def live_run():
        await runtime.initialize()
        journal.create_live_cycle(batch_id='batch', cycle_id='blocked-open',
            direction='long_entropy', planned_units=1000, reference_bps=D('0'),
            entry_basis_bps=D('-10'), started_ts=time.time())
        result = await runtime._step(journal.active_live_cycle(), purpose='open_first',
            attempt=0, venue_key='entropy', side=Side.BUY, units=1000,
            reduce_only=False, room_bps=D('3'), hard_cap_bps=D('10'),
            next_phase='OPEN_HEDGE', completion=False)
        assert not result.terminal and result.cumulative_units == 500
        original_pending_ref = journal.pending()[0]['order_ref']
        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        assert len(entropy.sent) == 1 and hedge.sent == []
        assert len(journal.pending()) == 1
        assert journal.pending()[0]['order_ref'] == original_pending_ref
        assert journal.active_live_cycle()['entropy_units'] == 500
        assert journal.active_live_cycle()['hedge_units'] == 0

    try:
        asyncio.run(live_run())
        rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9) for i in range(10)]
        profile = replay_calibration()
        profile['groups']['open:entropy'] = [{'fraction': None, 'latency_seconds': .1,
            'known_cumulative_units': 10,
            'position_evidence': {'authoritative': True, 'entropy_units': 10, 'hedge_units': 0}}]
        result = replay(rows, Settings(cold_seconds=1., cold_samples=2,
                                      max_notional_usd=D('30')),
                        profile, scenario='neutral', unit_override=20)
        assert result['status'] == 'CENSORED'
        assert result['path_censoring']['reason'] == 'pending_open_blocks_new_exposure'
        assert result['order_count'] == 1
        assert result['final_account_state']['entropy_units'] == 10
        assert result['final_account_state']['hedge_units'] == 0
        assert result['final_account_state']['pending_order_ref'] is not None
        assert result['realized_pnl_usd'] is None
    finally:
        journal.close()


def test_unknown_partial_without_account_evidence_does_not_invent_recovery():
    rows = [replay_frame(i + 1, i * .5, 100 if i < 4 else 99.9) for i in range(10)]
    profile = replay_calibration()
    profile['groups']['open:entropy'] = [{'fraction': None, 'latency_seconds': .1,
        'known_cumulative_units': 10, 'resolve_after_seconds': 1., 'resolved_fraction': .5}]
    result = replay(rows, Settings(cold_seconds=1., cold_samples=2,
                                  max_notional_usd=D('30')),
                    profile, scenario='neutral', unit_override=20)
    assert result['status'] == 'CENSORED'
    assert result['order_count'] == 1
    assert result['realized_pnl_usd'] is None
    assert result['final_account_state']['entropy_units'] == 10
    assert result['final_account_state']['pending_order_ref'] is not None
