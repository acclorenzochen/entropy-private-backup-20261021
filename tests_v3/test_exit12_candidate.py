"""Offline proof of independent 12bp close policy on real runtime Intent paths."""
import asyncio
from dataclasses import asdict, replace
from decimal import Decimal as D
import json
from pathlib import Path
import socket
from types import SimpleNamespace

import pytest

import main_v3
from entropy_arb.v3.core import Direction, Settings, Side, price_limit
from entropy_arb.v3.live_host import canonical_config_hash
from entropy_arb.v3 import core, runtime as runtime_module
from test_normal_exit import seed_pair
from test_runtime import complete_background_audit, seed_close_cycle, setup_runtime


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'config.v3.edge-next.json'
CLOSE_FIELDS = ('close_first_rooms_bps', 'close_first_hard_cap_bps',
                'close_hedge_rooms_bps', 'close_hedge_hard_cap_bps')


@pytest.fixture(autouse=True)
def isolation_and_no_network(monkeypatch):
    for module in (core, runtime_module, main_v3):
        assert Path(module.__file__).resolve().is_relative_to(ROOT)

    def denied(*args, **kwargs):
        raise AssertionError('candidate tests are offline; no real network')

    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


def configured_runtime(tmp_path):
    runtime, e, h, journal = setup_runtime(tmp_path)
    configured = main_v3.load_settings(CONFIG)
    runtime.settings = replace(runtime.settings, **{
        name: getattr(configured, name) for name in (*CLOSE_FIELDS,
            'open_first_rooms_bps', 'open_first_hard_cap_bps')})
    return runtime, e, h, journal


def capture_intents(runtime):
    sent = []
    original = runtime.executor.send

    async def send(intent, **kwargs):
        sent.append(intent)
        return await original(intent, **kwargs)

    runtime.executor.send = send
    return sent


def test_config_changes_only_requested_entry_and_exit_fields():
    candidate = json.loads(CONFIG.read_text())
    baseline_path = ROOT.parent / 'baseline/config.v3.edge-next.json'
    if baseline_path.is_file():  # The isolated review bundle includes the pre-change config.
        baseline = json.loads(baseline_path.read_text())
        assert {key for key in candidate.keys() | baseline.keys()
                if candidate.get(key) != baseline.get(key)} == set(CLOSE_FIELDS) | {
                    'entry_band_bps', 'exit_min_gross_profit_bps'}
    settings = Settings.from_mapping(candidate)
    assert settings.first_leg_execution_policy('open') == ((D('3'),) * 3, D('10'))
    assert settings.first_rooms_bps == (D('3'), D('5'), D('8'))
    assert settings.first_hard_cap_bps == D('10')
    assert settings.hedge_execution_policy('open') == ((D('4'), D('8'), D('12')), D('12'))
    assert settings.first_leg_execution_policy('close') == ((D('12'),) * 3, D('12'))
    assert settings.hedge_execution_policy('close') == ((D('12'),) * 3, D('12'))
    assert settings.exit_min_gross_profit_bps == D('2.5')
    assert settings.entry_band_bps == D('5')


def test_close_hedge_fallback_null_and_settings_serialization():
    legacy = Settings.from_mapping({'hedge_rooms_bps': ['4', '8', '12']})
    assert legacy.hedge_execution_policy('close') == legacy.hedge_execution_policy('open')
    null = Settings.from_mapping({'close_hedge_rooms_bps': None, 'close_hedge_hard_cap_bps': None})
    assert null.hedge_execution_policy('close') == legacy.hedge_execution_policy('close')
    settings = main_v3.load_settings(CONFIG)
    serialized = json.loads(json.dumps(asdict(settings), default=str))
    assert serialized['close_hedge_rooms_bps'] == ['12', '12', '12']
    assert serialized['close_hedge_hard_cap_bps'] == '12'
    raw = json.loads(CONFIG.read_text())
    assert canonical_config_hash(settings, pair_identity='offline') == canonical_config_hash(
        Settings.from_mapping(raw), pair_identity='offline')
    with pytest.raises(ValueError, match='hedge purpose'):
        settings.hedge_execution_policy('repair')


@pytest.mark.parametrize('raw', [
    {'close_hedge_rooms_bps': []}, {'close_hedge_rooms_bps': '12'},
    {'close_hedge_rooms_bps': ['-1']}, {'close_hedge_rooms_bps': ['13']},
    {'close_hedge_rooms_bps': ['12', '4']}, {'close_hedge_rooms_bps': ['NaN']},
    {'close_hedge_rooms_bps': ['Infinity']}, {'close_hedge_rooms_bps': [True]},
    {'close_hedge_hard_cap_bps': '0'}, {'close_hedge_hard_cap_bps': '-1'},
    {'close_hedge_hard_cap_bps': 'NaN'}, {'close_hedge_hard_cap_bps': 'Infinity'},
    {'close_hedge_rooms_bps': ['12'], 'close_hedge_hard_cap_bps': '10'},
])
def test_close_hedge_validation_rejects_invalid_bounds(raw):
    with pytest.raises((ValueError, ArithmeticError)):
        Settings.from_mapping(raw)


@pytest.mark.parametrize('side,tick,expected', [
    (Side.SELL, '.1', '1598.1'), (Side.BUY, '.1', '1601.9'),
    (Side.SELL, '.01', '1598.08'), (Side.BUY, '.01', '1601.92'),
])
def test_full_12bp_rounds_inward_at_hard_cap(side, tick, expected):
    limit = price_limit(D('1600'), D('1600'), side, D('12'), D('12'), D(tick))
    assert limit == D(expected)
    assert abs(limit / D('1600') - 1) * 10000 <= D('12')
    assert abs(limit / D('1600') - 1) * 10000 > D('10')


@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('e_fractions,h_fractions', [
    ([0, 0, 0, .5, 1], [0, 0, 0, .5, 1]),
    ([0, .5, 1], [1]), ([1], [0, .25, 1]),
])
def test_parallel_runtime_intents_use_12_for_first_zero_partial_and_later_retries(
        tmp_path, direction, e_fractions, h_fractions):
    runtime, e, h, journal = configured_runtime(tmp_path)
    e.price_decimals, h.price_decimals = 1, 2
    e.set_book('1600', '1600')
    h.set_book('1600', '1600')
    e.fill_fractions, h.fill_fractions = list(e_fractions), list(h_fractions)

    async def scenario():
        await runtime.initialize()
        sent = capture_intents(runtime)
        seed_pair(runtime, e, h, journal, direction)
        journal.latch_basis_exit('current', requested_ts=__import__('time').time())
        for _ in range(9):
            cycle = journal.active_live_cycle()
            if cycle is None:
                break
            e.set_book('1600', '1600')
            h.set_book('1600', '1600')
            await runtime._basis_exit(cycle)
            await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert e.position == h.position == 0
        assert len(e.sent) == len(e_fractions)
        assert len(h.sent) == len(h_fractions)
        assert sent and all(intent.reduce_only for intent in sent)
        assert len({intent.order_ref for intent in sent}) == len(sent)
        for intent in sent:
            tick = '.1' if intent.venue == 'entropy' else '.01'
            expected = {'entropy': {Side.SELL: '1598.1', Side.BUY: '1601.9'},
                        'hedge': {Side.SELL: '1598.08', Side.BUY: '1601.92'}}
            assert intent.limit_px == D(expected[intent.venue][intent.side])
            assert intent.limit_px % D(tick) == 0
            assert abs(intent.limit_px / D('1600') - 1) * 10000 <= 12
            assert abs(intent.limit_px / D('1600') - 1) * 10000 > 10
        steps = [step for step in journal.economics_steps('current') if step['purpose'].startswith('close')]
        assert len(steps) == len(sent)
        assert all(D(step['room_bps']) == D(step['hard_cap_bps']) == D('12') for step in steps)
        for key in ('entropy', 'hedge'):
            orders = [order for order in journal.cycle_orders('current') if order['venue'] == key and order['reduce_only']]
            assert sum(order['filled_units'] for order in orders) == 1000
            assert all(order['requested_units'] <= 1000 for order in orders)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_actual_opening_intents_retain_entropy_3_and_lighter_4_8_12(tmp_path):
    runtime, e, h, journal = configured_runtime(tmp_path)
    e.fill_fractions, h.fill_fractions = [0, 0, 1], [0, 0, 1]

    async def scenario():
        await runtime.initialize()
        sent = capture_intents(runtime)
        await runtime._maybe_open(runtime._market(), D('0'))
        assert journal.active_live_cycle()['phase'] == 'OPEN'
        assert len(e.sent) == len(h.sent) == 3
        assert all(not intent.reduce_only for intent in sent)
        steps = journal.economics_steps(journal.active_live_cycle()['cycle_id'])
        assert [D(step['room_bps']) for step in steps if step['venue'] == 'entropy'] == [D('3')] * 3
        assert [D(step['hard_cap_bps']) for step in steps if step['venue'] == 'entropy'] == [D('10')] * 3
        assert [D(step['room_bps']) for step in steps if step['venue'] == 'hedge'] == [D('4'), D('8'), D('12')]
        assert [D(step['hard_cap_bps']) for step in steps if step['venue'] == 'hedge'] == [D('12')] * 3
        by_ref = {step['order_ref']: step for step in steps}
        assert all(intent.limit_px == D(by_ref[intent.order_ref]['limit_px']) for intent in sent)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('venue', ['entropy', 'hedge'])
@pytest.mark.parametrize('purpose,expected_room', [('close_hedge', '12'), ('unknown_cover', '4')])
@pytest.mark.parametrize('latched', [False, True])
def test_residual_close_uses_own_policy_but_unknown_cover_retains_baseline(
        tmp_path, venue, purpose, expected_room, latched):
    runtime, e, h, journal = configured_runtime(tmp_path)
    target = e if venue == 'entropy' else h
    target.fill_fractions = [0, 1]

    async def scenario():
        await runtime.initialize()
        sent = capture_intents(runtime)
        cycle = seed_close_cycle(journal, e, h, e_units=85 if venue == 'entropy' else 0,
                                 h_units=-85 if venue == 'hedge' else 0)
        if latched:
            journal.latch_basis_exit(cycle['cycle_id'], requested_ts=__import__('time').time())
            cycle = journal.active_live_cycle()
        await runtime._reduce_residual(cycle, cycle['entropy_units'], cycle['hedge_units'], purpose=purpose)
        assert len(sent) == 2
        assert all(intent.venue == venue and intent.reduce_only for intent in sent)
        steps = [step for step in journal.economics_steps('close-risk') if step['result_units'] is not None]
        assert D(steps[0]['room_bps']) == D(expected_room)
        if purpose == 'close_hedge':
            assert all(D(step['room_bps']) == D(step['hard_cap_bps']) == D('12') for step in steps)
        else:
            assert [D(step['room_bps']) for step in steps] == [D('4'), D('8')]

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_legacy_close_group_lighter_tail_uses_close12_on_all_attempts(tmp_path):
    runtime, e, h, journal = configured_runtime(tmp_path)
    h.fill_fractions = [0, .5, 1]

    async def scenario():
        await runtime.initialize()
        sent = capture_intents(runtime)
        cycle = seed_close_cycle(journal, e, h, e_units=85, h_units=-85, phase='CLOSE_FIRST')
        e.set_book('104', '104.01')
        await runtime._close(cycle)
        assert len(e.sent) == 1 and len(h.sent) == 3
        assert all(intent.reduce_only for intent in sent)
        steps = journal.economics_steps('close-risk')
        assert all(D(step['room_bps']) == D(step['hard_cap_bps']) == D('12') for step in steps)

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_manifest_reports_each_open_close_policy_and_explicit_source(tmp_path):
    provenance = {}
    settings = main_v3.load_settings(CONFIG, provenance=provenance)
    runtime, _, _, journal = setup_runtime(tmp_path)
    args = SimpleNamespace(deployment='offline-candidate', max_new_cycles=0,
                           legacy_reference_sampling=False)
    try:
        asyncio.run(main_v3.publish_effective_manifest(args, settings, 'offline-pair', None,
                    runtime.status, provenance, {}, journal=journal))
        payload = json.loads((tmp_path / 'status.effective_manifest.json').read_text())
        policies = payload['execution_policies']
        for label, policy, field in [
            ('entropy-open', policies['open'], 'open_first'),
            ('entropy-close', policies['close'], 'close_first'),
            ('hedge-open', policies['hedge']['open'], 'hedge'),
            ('hedge-close', policies['hedge']['close'], 'close_hedge'),
        ]:
            assert policy['sources']['rooms_bps'] == {'field': f'{field}_rooms_bps', 'origin': 'v3_config'}, label
            assert policy['sources']['hard_cap_bps'] == {'field': f'{field}_hard_cap_bps', 'origin': 'v3_config'}, label
        assert policies['open']['rooms_bps'] == ['3'] * 3
        assert policies['open']['hard_cap_bps'] == '10'
        assert policies['hedge']['open']['rooms_bps'] == ['4', '8', '12']
        for policy in (policies['close'], policies['hedge']['close']):
            assert policy['rooms_bps'] == ['12'] * 3 and policy['hard_cap_bps'] == '12'
        assert payload['configured_settings']['close_hedge_rooms_bps'] == ['12'] * 3
    finally:
        journal.close()
