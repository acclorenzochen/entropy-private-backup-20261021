"""Ordinary operator control and explicit A/B opening remain separate modes."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D
import json
import socket
from types import SimpleNamespace

import pytest

import main_v3
from entropy_arb.v3.core import Settings
from entropy_arb.v3.journal import Journal
from entropy_arb.v3.runtime import LiveRuntime, RuntimeHalt
from tests_v3.test_ab_opening import finish_zero, open_once, setup_ab
from tests_v3.test_operator_runtime import command, state
from tests_v3.test_runtime import setup_runtime


REASON = 'operator_control_unsupported_opening_ab'


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('combined mode checks must remain offline')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


def binding(runtime):
    return dict(account_binding_hash='c' * 64, pair_identity=runtime.pair_id,
        deployment=runtime.deployment, batch_id=runtime.authorization.batch_id,
        config_hash=runtime.status.base['config_hash'])


def assert_no_experiment(journal):
    assert journal.db.execute('SELECT COUNT(*) FROM opening_experiments').fetchone()[0] == 0
    assert journal.db.execute('SELECT COUNT(*) FROM opening_attempts').fetchone()[0] == 0


def test_ordinary_launch_authorization_and_runtime_owner_do_not_create_ab_slots(tmp_path):
    args = main_v3.parser().parse_args(['--symbol', 'SNDK', '--hedge', 'lighter',
        '--deployment', 'test', '--live', '--operator-control',
        '--batch-authorization', str(tmp_path / 'ordinary-auth.json')])
    cfg = SimpleNamespace(entropy=SimpleNamespace(kind='hl', symbol='SNDK'),
                          hedge=SimpleNamespace(kind='lighter', symbol='SNDK'))
    pair = main_v3.pair_identity(args, cfg)
    (tmp_path / 'ordinary-auth.json').write_text(json.dumps(dict(batch_id='batch',
        pair_identity=pair, max_position_cycles=2, loss_budget_usd='3',
        max_notional_usd='15', approved_live=True)))
    pair_result, authorization = main_v3.validate_launch(args, cfg, Settings(max_notional_usd=D('15')))
    assert pair_result == pair and authorization.opening_experiment_id is None
    runtime, e, h, journal = setup_runtime(tmp_path)
    assert runtime.settings.opening_experiment_id == ''
    async def scenario():
        await runtime.initialize(apply_leverage=False)
        runtime.enable_operator_control(binding(runtime))
        assert state(runtime)['intent'] == 'RUNNING'
        assert_no_experiment(journal)
        stop, _ = command(runtime, 'stop')
        runtime._update_operator_progress()
        assert state(runtime)['intent'] == 'STOPPED'
        command(runtime, 'start')
        await runtime._maybe_open(runtime._market(), D('0'))
        assert len(e.sent) == len(h.sent) == 1
        assert journal.active_live_cycle()['phase'] == 'OPEN'
        assert journal.active_live_cycle()['entropy_units'] == -journal.active_live_cycle()['hedge_units'] != 0
        assert journal.operator_operation(runtime._operator_scope, stop['operation_id'])['state'] == 'completed'
        assert not journal.batch_state('batch')['closed_for_entries']
        assert_no_experiment(journal)
        assert not runtime.stop.is_set()
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


@pytest.mark.parametrize('entry', ['validate_launch', 'amain'])
def test_ab_operator_launch_rejects_before_authorization_journal_or_network(tmp_path, monkeypatch, entry):
    import aiohttp
    args = main_v3.parser().parse_args(['--symbol', 'SNDK', '--hedge', 'lighter',
        '--live', '--operator-control', '--batch-authorization', str(tmp_path / 'never-read.json'),
        '--env-file', '/dev/null', '--journal', str(tmp_path / 'never-created.sqlite')])
    settings = replace(Settings(), opening_experiment_id='opening-ab-test',
                       single_cycle_review_loss_usd=None)
    calls = []
    def trap(name):
        def forbidden(*args, **kwargs):
            calls.append(name)
            raise AssertionError(f'A/B control reached forbidden {name}')
        return forbidden
    monkeypatch.setattr(main_v3, 'load_settings', lambda *args, **kwargs: settings)
    monkeypatch.setattr(main_v3, 'load_config', lambda *args, **kwargs: None)
    for obj, name in ((Journal, '__init__'), (main_v3.ProcessGuard, '__init__'),
            (main_v3.BatchAuthorization, 'load'), (main_v3, '_make_venues'),
            (main_v3, '_run_configured'), (main_v3, 'StatusWriter'),
            (aiohttp, 'ClientSession'), (aiohttp, 'TCPConnector')):
        monkeypatch.setattr(obj, name, trap(name))
    with pytest.raises(ValueError, match=REASON):
        if entry == 'amain':
            asyncio.run(main_v3.amain(args))
        else:
            main_v3.validate_launch(args, None, settings)
    assert calls == [] and not (tmp_path / 'never-created.sqlite').exists()


@pytest.mark.parametrize('initialized', [False, True])
def test_explicit_ab_runtime_cannot_enable_operator_through_internal_entry(tmp_path, initialized):
    runtime, e, h, journal = setup_ab(tmp_path)
    async def scenario():
        if initialized:
            await runtime.initialize(apply_leverage=False)
        before = [dict(row) for row in journal.db.execute('SELECT * FROM opening_experiments')]
        with pytest.raises(ValueError, match=REASON):
            runtime.enable_operator_control(binding(runtime))
        assert runtime.operator_control is None and runtime._operator_scope is None
        assert journal.db.execute('SELECT COUNT(*) FROM operator_controls').fetchone()[0] == 0
        assert journal.db.execute('SELECT COUNT(*) FROM operator_operations').fetchone()[0] == 0
        assert [dict(row) for row in journal.db.execute('SELECT * FROM opening_experiments')] == before
        assert journal.db.execute('SELECT COUNT(*) FROM opening_attempts').fetchone()[0] == 0
        assert e.sent == h.sent == [] and not journal.pending()
    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_ab_operator_socket_constructor_refuses_before_runtime_initialization(tmp_path):
    runtime, e, h, journal = setup_ab(tmp_path)
    try:
        with pytest.raises(RuntimeHalt, match=REASON):
            LiveRuntime(settings=runtime.settings, legacy_config=runtime.cfg,
                authorization=runtime.authorization, pair_id=runtime.pair_id,
                deployment=runtime.deployment, venues=runtime.venues, journal=journal,
                status=runtime.status, observer=runtime.observer, stop=asyncio.Event(),
                operator_socket_path=tmp_path / 'never-created-owner.sock')
        assert e.sent == h.sent == [] and not journal.pending()
        assert journal.active_live_cycle() is None
        assert journal.db.execute('SELECT COUNT(*) FROM operator_controls').fetchone()[0] == 0
        assert_no_experiment(journal)
        assert not (tmp_path / 'never-created-owner.sock').exists()
    finally:
        journal.close()


@pytest.mark.parametrize('exhausted', [False, True])
def test_existing_ab_batch_cannot_downgrade_to_ordinary_control_or_reset_twenty_slots(tmp_path, exhausted):
    runtime, e, h, journal = setup_ab(tmp_path)
    async def scenario():
        await runtime.initialize(apply_leverage=False)
        if exhausted:
            e.fill_fractions = h.fill_fractions = [0] * 30
            for slot in range(1, 21):
                journal.rearm_opening_opportunity('opening-ab-test')
                await open_once(runtime)
                current = journal.opening_experiment_state('opening-ab-test')['current_attempt']
                assert current['slot'] == slot
                assert current['group_name'] == ('A' if slot % 2 else 'B')
                await finish_zero(runtime)
        before = journal.opening_experiment_state('opening-ab-test')
        sent = (len(e.sent), len(h.sent))
        with pytest.raises(ValueError, match=REASON):
            runtime.enable_operator_control(binding(runtime))
        assert runtime.operator_control is None and runtime._operator_scope is None
        if exhausted:
            journal.rearm_opening_opportunity('opening-ab-test')
            await open_once(runtime)
            assert journal.opening_experiment_state('opening-ab-test')['attempts'] == 20
            assert before['groups'] == {'A': 10, 'B': 10} and before['finished']
        ordinary = LiveRuntime(settings=replace(runtime.settings, opening_experiment_id=''),
            legacy_config=runtime.cfg,
            authorization=replace(runtime.authorization, opening_experiment_id=None),
            pair_id=runtime.pair_id, deployment=runtime.deployment, venues=runtime.venues,
            journal=journal, status=runtime.status, observer=runtime.observer,
            stop=asyncio.Event(), reference_checkpoint_path=runtime.reference_checkpoint_path)
        with pytest.raises(ValueError, match=REASON):
            ordinary.enable_operator_control(binding(ordinary))
        assert ordinary._operator_scope is None
        with pytest.raises(RuntimeHalt, match='experiment batch cannot be launched as ordinary basis'):
            await ordinary.initialize(apply_leverage=False)
        assert ordinary.operator_control is None
        assert (len(e.sent), len(h.sent)) == sent
        after = journal.opening_experiment_state('opening-ab-test')
        assert after['attempts'] == before['attempts'] and after['groups'] == before['groups']
        assert after['current_attempt'] == before['current_attempt']
        assert journal.db.execute('SELECT COUNT(*) FROM operator_controls').fetchone()[0] == 0
        assert not journal.pending() and journal.active_live_cycle() is None
        assert not runtime.stop.is_set() and not ordinary.stop.is_set()
    try:
        asyncio.run(scenario())
    finally:
        journal.close()
