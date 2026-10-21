import asyncio
import json
from decimal import Decimal as D
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import time

import pytest

from entropy_arb.book import OrderBook
from entropy_arb.venue_lighter import LighterVenue
from entropy_arb.v3.core import Settings, Side
from entropy_arb.v3.journal import Intent, Journal, Result
from entropy_arb.v3.live_host import (
    BatchAuthorization, OrderRefFactory, ReadOnlyObserver, StatusWriter,
)
from entropy_arb.v3.runtime import (
    LiveRuntime, RuntimeVenues, choose_open_plan, instrument_from_venues,
    market_from_venues,
)


class FakeVenue:
    def __init__(self, key, kind, bid, ask, *, min_base, fee_bps=0,
                 fill_fractions=None, unknown_once=False):
        self.key = key
        self.kind = kind
        self.conf = SimpleNamespace(symbol="SNDK")
        self.size_decimals = 4
        self.price_decimals = 2
        self.fee_bps = fee_bps
        self.min_base = min_base
        self.min_quote = 10
        self.orders_per_min = 100
        self.position = 0.0
        self.sent = []
        self.fill_fractions = list(fill_fractions or [])
        self.unknown_once = unknown_once
        self.pending = {}
        self.position_error = None
        self.fee_fills = []
        self.book = OrderBook()
        self.set_book(bid, ask)

    def set_book(self, bid, ask):
        self.book.apply_hl([
            [{"px": str(bid), "sz": "2"}],
            [{"px": str(ask), "sz": "2"}],
        ])

    async def load_market(self):
        return None

    def init_signer(self):
        return None

    def start_tasks(self, stop, notify, live):
        return []

    def ready_to_trade(self):
        return True

    async def send_taker(self, *, is_buy, qty, limit_px, reduce_only, order_ref):
        self.sent.append({"is_buy": is_buy, "qty": qty, "limit_px": limit_px,
                          "reduce_only": reduce_only, "order_ref": order_ref})
        fraction = self.fill_fractions.pop(0) if self.fill_fractions else 1
        filled = round(qty * fraction, 4)
        if reduce_only:
            closable = (-self.position if is_buy and self.position < 0
                        else self.position if not is_buy and self.position > 0
                        else 0.0)
            filled = min(filled, round(closable, 4))
        self.position = round(
            self.position + (filled if is_buy else -filled),
            self.size_decimals)
        terminal = {"status": "filled" if filled == qty else (
                        "partial" if filled else "canceled"),
                    "filled_base": filled, "avg_px": limit_px if filled else None,
                    "fee_usd": 0, "err": None, "unresolved": False,
                    "order_ref": order_ref}
        if filled:
            self.fee_fills.append({"order_ref": order_ref, "ts": time.time(),
                                   "exchange_fill_key": f"fake:{self.key}:{order_ref}",
                                   "side": "buy" if is_buy else "sell",
                                   "qty": filled, "price": limit_px,
                                   "fee_usd": 0})
        if self.unknown_once:
            self.unknown_once = False
            self.pending[order_ref] = terminal
            return {"status": "timeout", "filled_base": 0, "avg_px": None,
                    "err": None, "unresolved": True, "order_ref": order_ref}
        return terminal

    async def resolve_order(self, order_ref, timeout=None):
        return self.pending.pop(order_ref)

    async def fetch_position(self):
        if self.position_error is not None:
            raise self.position_error
        return self.position

    async def fetch_equity(self):
        return (100.0, 100.0)

    async def fetch_fee_fills(self, since_ts, until_ts=None):
        return list(self.fee_fills)

    async def fetch_funding_payments(self, since_ts, until_ts=None):
        return 0.0

    async def close(self):
        return None


def setup_runtime(tmp_path):
    entropy = FakeVenue("entropy", "hl", 100.10, 100.11, min_base=.0001)
    hedge = FakeVenue("hedge", "lighter", 99.99, 100.00, min_base=.0035)
    settings = Settings(
        cold_seconds=.01, cold_samples=2, warm_seconds=.01, warm_samples=2,
        entry_band_bps=D("3"), execution_allowance_bps=D("0"),
        sampling_floor_bps=D("-2"), max_notional_usd=D("15"))
    auth = BatchAuthorization("batch", "pair", 2, D("3"), D("15"), True)
    journal = Journal(tmp_path / "runtime.sqlite", identity="pair-account")
    stop = asyncio.Event()
    runtime = LiveRuntime(
        settings=settings,
        legacy_config=SimpleNamespace(),
        authorization=auth, pair_id="pair", deployment="test",
        venues=RuntimeVenues(entropy, hedge), journal=journal,
        status=StatusWriter(tmp_path / "status.json", strategy_version="v3",
                            config_hash="hash", mode="live"),
        observer=ReadOnlyObserver(tmp_path / "observer.jsonl"), stop=stop,
        reference_checkpoint_path=tmp_path / "reference_checkpoint.json")
    return runtime, entropy, hedge, journal


async def complete_background_audit(runtime, *, accounting=False):
    """Advance the independent position/accounting owners explicitly in tests."""
    await runtime._audit_positions()
    cycle = runtime.journal.active_live_cycle()
    if (cycle is not None and not runtime.journal.pending()
            and not cycle['entropy_units'] and not cycle['hedge_units']):
        await runtime._finish_flat(cycle)
    if accounting:
        for row in runtime.journal.provisional_cycles(runtime.authorization.batch_id):
            await runtime._reconcile_cycle_accounting(row['cycle_id'])


def test_initialize_applies_configured_leverage_before_account_streams(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    applied = []
    for venue in (entropy, hedge):
        venue.conf.leverage = 10

        async def set_leverage(value, key=venue.key):
            applied.append((key, value))

        def start_tasks(*args, **kwargs):
            assert applied == [('entropy', 10), ('hedge', 10)]
            return []

        venue.set_leverage, venue.start_tasks = set_leverage, start_tasks
    try:
        asyncio.run(runtime.initialize())
        assert runtime.status.base['applied_leverage'] == {'entropy': 10, 'hedge': 10}
        assert entropy.sent == hedge.sent == []
    finally:
        journal.close()


def test_leverage_failure_halts_live_startup_before_streams_or_orders(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.conf.leverage = 10

    async def fail(value):
        assert value == 10
        raise RuntimeError('leverage update could not be verified')

    def no_streams(*args, **kwargs):
        raise AssertionError('failed leverage must block startup')

    entropy.set_leverage = fail
    entropy.start_tasks = hedge.start_tasks = no_streams
    try:
        with pytest.raises(RuntimeError, match='could not be verified'):
            asyncio.run(runtime.run())
        assert entropy.sent == hedge.sent == []
        assert not journal.pending() and journal.active_live_cycle() is None
        assert json.loads(runtime.status.path.read_text())['current_state'] == 'HALTED'
    finally:
        journal.close()


@pytest.mark.parametrize('mode', ['preflight', 'reopen_entries'])
def test_preflight_and_unlock_do_not_change_exchange_leverage(tmp_path, mode):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    for venue in (entropy, hedge):
        venue.conf.leverage = 10

        async def forbidden(value):
            raise AssertionError('only normal live startup may set leverage')

        venue.set_leverage = forbidden
    try:
        journal.ensure_batch('batch', max_position_cycles=2, loss_budget_usd=D('3'))
        journal.close_entries('batch', 'MANUAL_STOP')
        asyncio.run(getattr(runtime, mode)())
        assert entropy.sent == hedge.sent == []
        assert 'applied_leverage' not in runtime.status.base
    finally:
        journal.close()


@pytest.mark.parametrize('authorized_cap', [D('15'), D('200')])
def test_200_dollar_sizing_still_respects_both_authorization_and_leg_quotes(
        tmp_path, authorized_cap):
    old, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime = LiveRuntime(
        settings=replace(old.settings, max_notional_usd=D('200')),
        legacy_config=old.cfg,
        authorization=replace(old.authorization, max_notional_usd=authorized_cap),
        pair_id=old.pair_id, deployment=old.deployment, venues=old.venues,
        journal=journal, status=old.status, observer=old.observer, stop=old.stop,
        reference_checkpoint_path=old.reference_checkpoint_path)

    async def scenario():
        await runtime.initialize()
        market = runtime._market()
        plan = choose_open_plan(
            market, runtime.instrument, runtime.settings, D('0'),
            entropy_venue=entropy, hedge_venue=hedge)
        assert plan is not None
        assert runtime.settings.max_notional_usd == authorized_cap
        assert authorized_cap - D('1') < plan.notional_usd <= authorized_cap
        side = plan.direction.entropy_side
        for book, order_side in ((market.entropy, side), (market.hedge, side.opposite)):
            cash, _ = book.quote(order_side, plan.units, runtime.instrument.quantity_step)
            assert cash <= authorized_cap

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def seed_close_cycle(journal, entropy, hedge, *, e_units, h_units,
                     phase='CLOSE_HEDGE'):
    journal.create_live_cycle(
        batch_id='batch', cycle_id='close-risk',
        direction='long_entropy' if e_units >= 0 else 'short_entropy',
        planned_units=max(abs(e_units), abs(h_units), 1),
        reference_bps=D('0'), entry_basis_bps=D('-4'),
        started_ts=time.time())
    journal.update_live_cycle(
        'close-risk', phase=phase, entropy_units=e_units, hedge_units=h_units,
        updated_ts=time.time())
    for venue, units in (('entropy', e_units), ('hedge', h_units)):
        if not units:
            continue
        side = Side.BUY if units > 0 else Side.SELL
        intent = Intent(
            f'seed-{venue}', 'close-risk', venue, side, abs(units), D('100'))
        journal.prepare(intent)
        journal.mark_submitting(intent.order_ref)
        journal.record(
            intent.order_ref,
            Result(True, abs(units), D('100'), D('0'), note='seed_position'))
    entropy.position = e_units / 10000
    hedge.position = h_units / 10000
    return journal.active_live_cycle()


def add_unknown_close(journal, *, venue='hedge', side=Side.BUY,
                      units=85, order_ref='123'):
    intent = Intent(order_ref, 'close-risk', venue, side, units, D('100'), True)
    journal.ensure_live_step(
        step_key=f'close-risk:close_hedge:0', cycle_id='close-risk',
        order_ref=order_ref, venue=venue, side=side.value,
        requested_units=units, reduce_only=True)
    journal.prepare(intent)
    journal.mark_submitting(order_ref)
    journal.record(order_ref, Result(False, 0, note='timeout'))


def test_market_conversion_and_plan_respect_common_units_and_cap(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        market = market_from_venues(runtime.venues, runtime.instrument,
                                    now=time.time(),
                                    settings=runtime.settings)
        plan = choose_open_plan(
            market, runtime.instrument, runtime.settings, D("0"),
            entropy_venue=entropy, hedge_venue=hedge)
        assert plan is not None
        assert plan.notional_usd <= D("15")
        assert runtime.instrument.quantity(plan.units) >= D("0.0035")

    asyncio.run(scenario())
    journal.close()


def test_full_open_hedge_close_flat_cycle_uses_actual_integer_units(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        market = runtime._market()
        await runtime._maybe_open(market, D("0"))
        active = journal.active_live_cycle()
        assert active is not None and active["phase"] == "OPEN"
        assert active["entropy_units"] == -active["hedge_units"] != 0
        opened_units = abs(active["entropy_units"])
        assert sum(round(order["qty"] * 10000) for order in entropy.sent[:1]) == opened_units
        assert sum(round(order["qty"] * 10000) for order in hedge.sent[:1]) == opened_units

        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(active)
        assert journal.active_live_cycle() is not None  # Flat audit runs independently.
        await complete_background_audit(runtime, accounting=True)
        assert journal.active_live_cycle() is None
        assert round(entropy.position, 8) == round(hedge.position, 8) == 0
        result = journal.db.execute(
            "SELECT result_kind,complete_accounting FROM cycles").fetchone()
        assert result[0] == "clean" and result[1] == 1
        assert all(order["reduce_only"] for order in entropy.sent[1:] + hedge.sent[1:])

    asyncio.run(scenario())
    journal.close()


def test_open_first_telemetry_waits_until_after_hedge_send(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    original_h_send = hedge.send_taker
    first_key = None
    terminal_basis = None

    async def h_send(**kwargs):
        nonlocal first_key, terminal_basis
        cycle = journal.active_live_cycle()
        first = journal.live_steps(cycle['cycle_id'])[0]
        first_key = first['step_key']
        assert journal.economics_step(first_key) is None
        market = runtime._market()
        units = first['result_units']
        side = Side(first['side'])
        e_cash, _ = market.entropy.quote(side, units,
                                         runtime.instrument.quantity_step)
        h_cash, _ = market.hedge.quote(side.opposite, units,
                                       runtime.instrument.quantity_step)
        terminal_basis = (e_cash / h_cash - 1) * D('10000')
        response = await original_h_send(**kwargs)
        hedge.set_book(200, 200.01)
        return response

    hedge.send_taker = h_send

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        assert first_key is not None
        economics = journal.economics_step(first_key)
        assert economics is not None
        assert D(economics['result_paired_executable_basis_bps']) == terminal_basis
        assert journal.active_live_cycle()['phase'] == 'OPEN'

    asyncio.run(scenario())
    journal.close()


def test_attempt_quotes_preserve_frozen_decision_and_actual_fill_size(
        tmp_path, monkeypatch):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [.5]
    original_create = journal.create_live_cycle

    def move_book_after_decision(**kwargs):
        original_create(**kwargs)
        entropy.set_book(100.12, 100.13)

    monkeypatch.setattr(journal, 'create_live_cycle', move_book_after_decision)

    async def scenario():
        await runtime.initialize()
        market = runtime._market()
        decision = choose_open_plan(
            market, runtime.instrument, runtime.settings, D('0'),
            entropy_venue=entropy, hedge_venue=hedge)
        assert decision is not None
        await runtime._maybe_open(market, D('0'))
        first = journal.economics_steps(journal.active_live_cycle()['cycle_id'])[0]
        assert first['limit_px'] == str(decision.entropy_limit_px)
        assert first['requested_units'] == decision.units
        assert first['result_units'] < decision.units
        a = json.loads(first['decision_quote_json'])
        b = json.loads(first['submit_quote_json'])
        c = json.loads(first['fill_mark_quote_json'])
        assert a['entropy']['book_revision'] == decision.entropy_book_revision
        assert b['entropy']['book_revision'] != a['entropy']['book_revision']
        assert a['units'] == b['units'] == decision.units
        assert c['units'] == first['result_units']
        assert first['clock_session_id'] == runtime.clock_session_id
        assert first['submit_start_mono_ns'] >= first['attempt_decision_mono_ns']
        assert first['terminal_observed_mono_ns'] >= first['submit_start_mono_ns']
        assert first['fill_mark_time_source'] == 'runtime_consumed_local'
        assert first['runtime_consumed_ts'] >= first['terminal_observed_ts']

    asyncio.run(scenario())
    journal.close()


def test_live_step_is_idempotent_across_restart(tmp_path):
    path = tmp_path / "journal.sqlite"
    journal = Journal(path, identity="pair-account")
    journal.ensure_batch("b", max_position_cycles=2, loss_budget_usd=D("3"))
    journal.create_live_cycle(
        batch_id="b", cycle_id="c", direction="long_entropy",
        planned_units=85, reference_bps=D("0"), entry_basis_bps=D("-4"),
        started_ts=1)
    step = journal.ensure_live_step(
        step_key="c:open_first:0", cycle_id="c", order_ref="ref",
        venue="entropy", side="buy", requested_units=85, reduce_only=False)
    assert not step["applied"]
    journal.apply_live_step(
        "c:open_first:0", result_units=85, cash_delta_usd=D("-8.5"),
        accounting_final=True, entropy_units=85, hedge_units=0,
        next_phase="OPEN_HEDGE", updated_ts=2)
    journal.close()

    journal = Journal(path, identity="pair-account")
    journal.apply_live_step(
        "c:open_first:0", result_units=85, cash_delta_usd=D("-8.5"),
        accounting_final=True, entropy_units=85, hedge_units=0,
        next_phase="OPEN_HEDGE", updated_ts=3)
    cycle = journal.active_live_cycle()
    assert cycle["cash_net_usd"] == "-8.5"
    assert cycle["entropy_units"] == 85
    journal.close()


def test_request_budget_survives_restart(tmp_path):
    path = tmp_path / "journal.sqlite"
    journal = Journal(path, identity="pair-account")
    assert journal.reserve_request("entropy", now=100, limit=2,
                                   reserve_for_completion=1, completion=False)
    assert not journal.reserve_request("entropy", now=101, limit=2,
                                       reserve_for_completion=1, completion=False)
    journal.close()
    journal = Journal(path, identity="pair-account")
    assert journal.reserve_request("entropy", now=102, limit=2,
                                   reserve_for_completion=1, completion=True)
    assert not journal.reserve_request("entropy", now=103, limit=2,
                                       reserve_for_completion=1, completion=True)
    assert journal.reserve_request("entropy", now=163, limit=2,
                                   reserve_for_completion=1, completion=False)
    journal.close()


def test_request_budget_retains_attributed_history_without_changing_limit(tmp_path):
    journal = Journal(tmp_path / 'requests.sqlite', identity='pair-account')
    assert journal.reserve_request(
        'entropy', now=100.0, limit=2, reserve_for_completion=0,
        completion=False, request_kind='open_first', cycle_id='c1')
    assert journal.reserve_request(
        'entropy', now=100.0, limit=2, reserve_for_completion=0,
        completion=True, request_kind='close_first', cycle_id='c1')
    assert not journal.reserve_request(
        'entropy', now=100.0, limit=2, reserve_for_completion=0,
        completion=True, request_kind='recovery', cycle_id='c1')
    assert journal.reserve_request(
        'entropy', now=161.0, limit=2, reserve_for_completion=0,
        completion=False, request_kind='open_first', cycle_id='c2')
    rows = [tuple(row) for row in journal.db.execute(
        'SELECT request_kind,cycle_id FROM request_events ORDER BY used_ts')]
    assert rows == [('open_first', 'c1'), ('close_first', 'c1'),
                    ('open_first', 'c2')]
    journal.close()


def test_reference_checkpoint_is_saved_at_most_once_per_second(tmp_path, monkeypatch):
    runtime, _entropy, _hedge, journal = setup_runtime(tmp_path)
    writes = []
    monkeypatch.setattr(
        "entropy_arb.v3.runtime.save_checkpoint",
        lambda path, checkpoint: writes.append((path, checkpoint["ts"])))

    first_ts = int(time.time()) + 1
    runtime.reference.observe(first_ts, D("1"), valid=True)
    runtime.reference.observe(first_ts + 1, D("1"), valid=True)
    runtime._persist_reference(now=10)
    runtime.reference.observe(first_ts + 2, D("2"), valid=True)
    runtime._persist_reference(now=10.5)
    runtime._persist_reference(now=11.1)
    runtime._persist_reference(now=12)

    assert [ts for _, ts in writes] == [first_ts + 1, first_ts + 2]
    journal.close()


def test_legacy_reference_sampling_restores_main_loop_observe(tmp_path):
    runtime, _entropy, _hedge, journal = setup_runtime(tmp_path)
    runtime.reference_sampler_enabled = False
    original_observe = runtime.reference.observe
    original_publish = runtime.observer.publish
    observations = []

    def observe(*args, **kwargs):
        observations.append((args, kwargs))
        return original_observe(*args, **kwargs)

    def publish(event):
        original_publish(event)
        if event['kind'] == 'v3_market':
            runtime.stop.set()

    runtime.reference.observe = observe
    runtime.observer.publish = publish

    async def unexpected_sampler():
        raise AssertionError('independent sampler started in rollback mode')

    runtime._reference_loop = unexpected_sampler
    asyncio.run(runtime.run())
    assert len(observations) == 1
    assert observations[0][1]['valid'] is True
    journal.close()


def test_first_partial_never_expands_open_and_hedges_actual_fill(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [.6]

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D("0"))
        cycle = journal.active_live_cycle()
        assert cycle["phase"] == "OPEN"
        assert len(entropy.sent) == 1
        requested = round(entropy.sent[0]["qty"] * 10000)
        actual = abs(cycle["entropy_units"])
        assert 0 < actual < requested
        assert cycle["hedge_units"] == -cycle["entropy_units"]
        assert round(hedge.sent[0]["qty"] * 10000) == actual

    asyncio.run(scenario())
    journal.close()


def test_timeout_fill_resolves_same_id_without_resend(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.unknown_once = True

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D("0"))
        cycle = journal.active_live_cycle()
        assert cycle["phase"] == "OPEN"
        assert len(entropy.sent) == 1
        assert not journal.pending()
        assert cycle["entropy_units"] == -cycle["hedge_units"]

    asyncio.run(scenario())
    journal.close()


def test_unknown_opening_waits_same_id_terminal_then_hedges_exact_fill(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.unknown_once = True
    original_resolve = entropy.resolve_order

    async def unresolved(order_ref, timeout=None):
        return {"status": "timeout", "filled_base": 0, "avg_px": None,
                "err": None, "unresolved": True, "order_ref": order_ref}
    entropy.resolve_order = unresolved

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D("0"))
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        cycle = journal.active_live_cycle()
        assert cycle['entropy_units'] == round(entropy.position * 10000) != 0
        assert cycle['hedge_units'] == 0
        assert journal.pending()[0]["venue"] == "entropy"
        await runtime._resume_cycle(cycle)
        assert len(entropy.sent) == 1 and hedge.sent == []
        unresolved_ref = journal.pending()[0]['order_ref']

        async def same_id_terminal(order_ref, timeout=None):
            assert order_ref == unresolved_ref
            return await original_resolve(order_ref, timeout)

        entropy.resolve_order = same_id_terminal
        await runtime._resolve_pending()  # The background owner resolves the existing ID.
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        cycle = journal.active_live_cycle()
        assert cycle["entropy_units"] == -cycle["hedge_units"] != 0
        assert len(entropy.sent) == len(hedge.sent) == 1
        assert journal.pending() == []
        assert round(hedge.sent[0]['qty'] * 10000) == abs(cycle['entropy_units'])

    asyncio.run(scenario())
    journal.close()


def test_unknown_hedge_close_waits_same_id_terminal_before_reducing_short_residual(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def unresolved(order_ref, timeout=None):
        return {"status": "timeout", "filled_base": 0, "avg_px": None,
                "err": None, "unresolved": True, "order_ref": order_ref}

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=0, h_units=-85)
        add_unknown_close(journal)
        hedge.resolve_order = unresolved

        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._cover_unknown_exposure(cycle)
        assert entropy.sent == hedge.sent == []
        assert journal.order('123')['state'] == 'UNKNOWN'

        async def terminal(order_ref, timeout=None):
            assert order_ref == '123'
            return {'status': 'canceled', 'filled_base': 0, 'avg_px': None,
                    'fee_usd': 0, 'unresolved': False, 'order_ref': order_ref,
                    'confirmation_source': 'exchange_http'}

        hedge.resolve_order = terminal
        await runtime._resolve_pending()
        await runtime._resume_cycle(journal.active_live_cycle())
        await complete_background_audit(runtime)

        assert len(hedge.sent) == 1
        assert hedge.sent[0]['is_buy']
        assert hedge.sent[0]['reduce_only']
        assert round(hedge.sent[0]['qty'] * 10000) == 85
        assert entropy.sent == []
        assert round(hedge.position * 10000) == 0
        assert journal.pending() == []
        assert journal.active_live_cycle() is None
        old = journal.order('123')
        assert old['state'] == 'TERMINAL'
        assert old['filled_units'] == 0
        assert old['terminal_status'] == 'canceled'
        assert old['terminal_source'] == 'exchange_http'

    asyncio.run(scenario())
    journal.close()


def test_partial_close_repairs_only_exact_lighter_residual(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=45, h_units=-85)

        await runtime._repair_to_balance(cycle, purpose='close_hedge')

        assert len(hedge.sent) == 1
        assert hedge.sent[0]['is_buy']
        assert hedge.sent[0]['reduce_only']
        assert round(hedge.sent[0]['qty'] * 10000) == 40
        assert entropy.sent == []
        active = journal.active_live_cycle()
        assert active['entropy_units'] == 45
        assert active['hedge_units'] == -45
        assert active['phase'] == 'OPEN'

    asyncio.run(scenario())
    journal.close()


def test_flat_positions_with_unknown_close_only_resolve_same_id(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def unresolved(order_ref, timeout=None):
        return {"status": "timeout", "filled_base": 0, "avg_px": None,
                "err": None, "unresolved": True, "order_ref": order_ref}

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=0, h_units=0)
        add_unknown_close(journal)
        hedge.resolve_order = unresolved

        await runtime._cover_unknown_exposure(cycle)

        assert entropy.sent == hedge.sent == []
        assert len(journal.pending()) == 1
        assert journal.active_live_cycle() is not None

    asyncio.run(scenario())
    journal.close()


def test_balanced_partial_close_returns_to_open_without_forced_flatten(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=40, h_units=-40)

        await runtime._repair_to_balance(cycle, purpose='close_hedge')

        assert entropy.sent == hedge.sent == []
        active = journal.active_live_cycle()
        assert active['entropy_units'] == 40
        assert active['hedge_units'] == -40
        assert active['phase'] == 'OPEN'

    asyncio.run(scenario())
    journal.close()


def test_close_first_partial_fill_immediately_hedges_actual_units(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [30 / 85]

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=85, h_units=-85,
            phase='CLOSE_FIRST')
        await runtime._close(cycle)
        assert len(entropy.sent) == 1
        assert round(entropy.sent[0]['qty'] * 10000) == 85
        assert entropy.sent[0]['reduce_only']
        assert len(hedge.sent) == 1
        assert round(hedge.sent[0]['qty'] * 10000) == 30
        assert hedge.sent[0]['is_buy'] and hedge.sent[0]['reduce_only']
        active = journal.active_live_cycle()
        assert active['entropy_units'] == 55
        assert active['hedge_units'] == -55
        assert active['phase'] == 'OPEN'
        first, paired = journal.live_steps('close-risk')
        assert paired['execution_group_id'] == first['step_key']
        await runtime._audit_positions()
        first_measurement = journal.economics_step(first['step_key'])
        hedge_measurement = journal.economics_step(paired['step_key'])
        assert first_measurement['authoritative_reconcile_complete_ts'] is not None
        assert hedge_measurement['authoritative_reconcile_complete_ts'] is not None
        assert (first_measurement['authoritative_reconcile_complete_ts'] ==
                hedge_measurement['authoritative_reconcile_complete_ts'])

    asyncio.run(scenario())
    journal.close()


def test_basis_close_uses_terminal_fill_before_position_and_analytics_reads(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, exit_fast_path_enabled=True)
    entropy.fill_fractions = [30 / 85]
    position_reads = {'entropy': 0, 'hedge': 0}
    original_e_position = entropy.fetch_position
    original_h_position = hedge.fetch_position
    original_e_send = entropy.send_taker
    original_h_send = hedge.send_taker
    after_e_send = None

    async def e_position():
        position_reads['entropy'] += 1
        return await original_e_position()

    async def h_position():
        position_reads['hedge'] += 1
        return await original_h_position()

    async def e_send(**kwargs):
        nonlocal after_e_send
        response = await original_e_send(**kwargs)
        after_e_send = dict(position_reads)
        response['status'] = 'filled'
        response['confirmation_source'] = 'exchange_http'
        return response

    async def h_send(**kwargs):
        assert position_reads == after_e_send
        assert journal.economics_step('close-risk:close_first_0:0') is None
        return await original_h_send(**kwargs)

    entropy.fetch_position = e_position
    hedge.fetch_position = h_position
    entropy.send_taker = e_send
    hedge.send_taker = h_send

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=85, h_units=-85,
            phase='CLOSE_FIRST')
        await runtime._close(cycle, new_exit=True)
        assert len(entropy.sent) == len(hedge.sent) == 1
        assert round(hedge.sent[0]['qty'] * 10000) == 30
        assert journal.economics_step('close-risk:close_first_0:0') is not None
        active = journal.active_live_cycle()
        assert active['phase'] == 'OPEN'
        assert (active['entropy_units'], active['hedge_units']) == (55, -55)

    asyncio.run(scenario())
    journal.close()


def test_basis_close_terminal_without_receipt_does_not_wait_for_rest_before_hedge(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, exit_fast_path_enabled=True)
    entropy.fill_fractions = [30 / 85]
    position_reads = 0
    original_e_position = entropy.fetch_position
    original_e_send = entropy.send_taker
    original_h_send = hedge.send_taker
    after_e_send = None

    async def e_position():
        nonlocal position_reads
        position_reads += 1
        return await original_e_position()

    async def e_send(**kwargs):
        nonlocal after_e_send
        response = await original_e_send(**kwargs)
        after_e_send = position_reads
        return response

    async def h_send(**kwargs):
        assert position_reads == after_e_send
        return await original_h_send(**kwargs)

    entropy.fetch_position = e_position
    entropy.send_taker = e_send
    hedge.send_taker = h_send

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=85, h_units=-85,
            phase='CLOSE_FIRST')
        await runtime._close(cycle, new_exit=True)
        assert len(hedge.sent) == 1
        assert journal.active_live_cycle()['phase'] == 'OPEN'

    asyncio.run(scenario())
    journal.close()


def test_basis_reductions_ignore_old_context_but_final_audit_requires_current_evidence(tmp_path):
    for failure in ('expired', 'generation_changed'):
        runtime, entropy, hedge, journal = setup_runtime(tmp_path / failure)
        runtime.settings = replace(runtime.settings, exit_fast_path_enabled=True)
        original_e_position = entropy.fetch_position
        original_e_send = entropy.send_taker
        original_h_send = hedge.send_taker
        ready = asyncio.Event()
        ready.set()
        hedge.orders_feed = SimpleNamespace(connection_generation=1, ready=ready,
                                            evidence_revision=0)
        e_reads = 0
        after_e_send = None

        async def e_position():
            nonlocal e_reads
            e_reads += 1
            return await original_e_position()

        async def h_snapshot(*, force_rest=False):
            return SimpleNamespace(position=hedge.position,
                                   received_ts=time.time(), authoritative=True,
                                   source='ws', generation=hedge.orders_feed.connection_generation)

        async def e_send(**kwargs):
            nonlocal after_e_send
            response = await original_e_send(**kwargs)
            response['confirmation_source'] = 'exchange_http'
            after_e_send = e_reads
            if failure == 'expired':
                runtime._last_position_evidence = replace(
                    runtime._last_position_evidence, observed_ts=time.time() - 2)
            else:
                hedge.orders_feed.connection_generation = 2
            return response

        async def h_send(**kwargs):
            assert e_reads == after_e_send
            return await original_h_send(**kwargs)

        entropy.fetch_position = e_position
        hedge.fetch_position_snapshot = h_snapshot
        entropy.send_taker = e_send
        hedge.send_taker = h_send

        async def scenario():
            await runtime.initialize()
            cycle = seed_close_cycle(
                journal, entropy, hedge, e_units=85, h_units=-85,
                phase='CLOSE_FIRST')
            await runtime._close(cycle, new_exit=True)
            assert len(hedge.sent) == 1
            assert entropy.sent[0]['reduce_only'] and hedge.sent[0]['reduce_only']
            assert journal.active_live_cycle() is not None
            await complete_background_audit(runtime)
            assert journal.active_live_cycle() is None

        asyncio.run(scenario())
        journal.close()


def test_basis_close_full_fill_waits_for_background_audit_and_links_steps(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings, exit_fast_path_enabled=True)
    original_e_send = entropy.send_taker
    original_e_position = entropy.fetch_position
    original_h_position = hedge.fetch_position
    reads = {'entropy': 0, 'hedge': 0}

    async def e_send(**kwargs):
        response = await original_e_send(**kwargs)
        response['confirmation_source'] = 'exchange_http'
        return response

    async def e_position():
        reads['entropy'] += 1
        return await original_e_position()

    async def h_position():
        reads['hedge'] += 1
        return await original_h_position()

    async def scenario():
        await runtime.initialize()
        entropy.send_taker = e_send
        entropy.fetch_position = e_position
        hedge.fetch_position = h_position
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=85, h_units=-85,
            phase='CLOSE_FIRST')
        await runtime._close(cycle, new_exit=True)
        assert reads == {'entropy': 0, 'hedge': 0}
        assert journal.active_live_cycle() is not None
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert reads == {'entropy': 1, 'hedge': 1}
        first, paired = journal.live_steps('close-risk')
        assert first['execution_group_id'] == first['step_key']
        assert paired['parent_step_key'] == first['step_key']
        assert paired['execution_group_id'] == first['step_key']
        analytics = journal.economics_step(paired['step_key'])
        assert analytics['parent_step_key'] == first['step_key']

    asyncio.run(scenario())
    journal.close()


def test_restart_after_entropy_close_terminal_or_applied_hedges_before_more_entropy(
        tmp_path):
    for applied in (False, True):
        folder = tmp_path / ('applied' if applied else 'terminal')
        runtime, entropy, hedge, journal = setup_runtime(folder)

        async def scenario():
            await runtime.initialize()
            cycle = seed_close_cycle(
                journal, entropy, hedge, e_units=85, h_units=-85,
                phase='CLOSE_FIRST')
            step_key = 'close-risk:close_first_0:0'
            ref = OrderRefFactory.create(
                entropy.kind, deployment='test', cycle_id=cycle['cycle_id'],
                leg='close_first_0:0')
            journal.ensure_live_step(
                step_key=step_key, cycle_id=cycle['cycle_id'], order_ref=ref,
                venue='entropy', side='sell', requested_units=85,
                reduce_only=True, execution_group_id=step_key)
            intent = Intent(ref, cycle['cycle_id'], 'entropy', Side.SELL,
                            85, D('100'), True)
            journal.prepare(intent)
            journal.mark_submitting(ref)
            journal.record(ref, Result(True, 30, D('100'), D('0')))
            entropy.position = 0.0055
            if applied:
                journal.apply_live_step(
                    step_key, result_units=30, cash_delta_usd=D('0.3'),
                    accounting_final=True, entropy_units=55, hedge_units=-85,
                    next_phase='CLOSE_HEDGE', updated_ts=time.time(),
                    position_evidence_authoritative=False)

        asyncio.run(scenario())
        journal.close()
        restarted, e2, h2, journal2 = setup_runtime(folder)
        e2.position, h2.position = 0.0055, -0.0085

        async def resume():
            await restarted.initialize()
            await restarted._resume_cycle(journal2.active_live_cycle())
            assert e2.sent == []
            assert len(h2.sent) == 1
            assert round(h2.sent[0]['qty'] * 10000) == 30
            active = journal2.active_live_cycle()
            assert active['phase'] == 'OPEN'
            assert (active['entropy_units'], active['hedge_units']) == (55, -55)

        asyncio.run(resume())
        journal2.close()


def test_close_restart_boundaries_resolve_same_ids_and_preserve_position(tmp_path):
    for boundary in ('entropy_submitting', 'hedge_submitting',
                     'hedge_terminal', 'flat_before_done'):
        folder = tmp_path / boundary
        runtime, entropy, hedge, journal = setup_runtime(folder)

        async def seed():
            await runtime.initialize()
            full = boundary == 'flat_before_done'
            closed = 85 if full else 30
            cycle = seed_close_cycle(
                journal, entropy, hedge, e_units=85, h_units=-85,
                phase='CLOSE_FIRST')
            first_key = 'close-risk:close_first_0:0'
            first_ref = OrderRefFactory.create(
                entropy.kind, deployment='test', cycle_id=cycle['cycle_id'],
                leg='close_first_0:0')
            journal.ensure_live_step(
                step_key=first_key, cycle_id=cycle['cycle_id'],
                order_ref=first_ref, venue='entropy', side='sell',
                requested_units=85, reduce_only=True,
                execution_group_id=first_key)
            first = Intent(first_ref, cycle['cycle_id'], 'entropy', Side.SELL,
                           85, D('100'), True)
            journal.prepare(first)
            journal.mark_submitting(first_ref)
            if boundary == 'entropy_submitting':
                journal.record(first_ref, Result(False, 0, note='timeout'))
                return first_ref, None, closed
            journal.record(first_ref, Result(True, closed, D('100'), D('0')))
            journal.apply_live_step(
                first_key, result_units=closed,
                cash_delta_usd=D(closed) / 100,
                accounting_final=True, entropy_units=85 - closed,
                hedge_units=-85, next_phase='CLOSE_HEDGE',
                updated_ts=time.time(), position_evidence_authoritative=False)
            hedge_key = 'close-risk:close_hedge_1:0'
            hedge_ref = OrderRefFactory.create(
                hedge.kind, deployment='test', cycle_id=cycle['cycle_id'],
                leg='close_hedge_1:0')
            journal.ensure_live_step(
                step_key=hedge_key, cycle_id=cycle['cycle_id'],
                order_ref=hedge_ref, venue='hedge', side='buy',
                requested_units=closed, reduce_only=True,
                execution_group_id=first_key, parent_step_key=first_key)
            intent = Intent(hedge_ref, cycle['cycle_id'], 'hedge', Side.BUY,
                            closed, D('100'), True)
            journal.prepare(intent)
            journal.mark_submitting(hedge_ref)
            if boundary == 'hedge_submitting':
                journal.record(hedge_ref, Result(False, 0, note='timeout'))
            else:
                journal.record(hedge_ref, Result(True, closed, D('100'), D('0')))
            if full:
                journal.apply_live_step(
                    hedge_key, result_units=closed,
                    cash_delta_usd=-D(closed) / 100,
                    accounting_final=True, entropy_units=0, hedge_units=0,
                    next_phase='CLOSE_HEDGE', updated_ts=time.time(),
                    position_evidence_authoritative=False)
            return first_ref, hedge_ref, closed

        first_ref, hedge_ref, closed = asyncio.run(seed())
        journal.close()
        restarted, e2, h2, journal2 = setup_runtime(folder)
        e2.position = (85 - closed) / 10000
        h2.position = (-85 + (0 if boundary == 'entropy_submitting' else closed)) / 10000
        if boundary == 'entropy_submitting':
            e2.pending[first_ref] = {
                'status': 'filled', 'filled_base': closed / 10000,
                'avg_px': 100, 'fee_usd': 0, 'err': None,
                'unresolved': False, 'order_ref': first_ref}
        if boundary == 'hedge_submitting':
            h2.pending[hedge_ref] = {
                'status': 'filled', 'filled_base': closed / 10000,
                'avg_px': 100, 'fee_usd': 0, 'err': None,
                'unresolved': False, 'order_ref': hedge_ref}
        resolved_refs = []
        pending_refs = {row['order_ref'] for row in journal2.pending()}
        for venue in (e2, h2):
            original_resolve = venue.resolve_order

            async def same_id_resolve(order_ref, timeout=None, original=original_resolve):
                resolved_refs.append(order_ref)
                return await original(order_ref, timeout=timeout)

            venue.resolve_order = same_id_resolve

        async def resume():
            await restarted.initialize()
            assert await restarted._resolve_pending()  # Live run performs startup recovery.
            assert set(resolved_refs) == pending_refs
            assert len(resolved_refs) == len(pending_refs)
            await restarted._audit_positions()
            await restarted._resume_cycle(journal2.active_live_cycle())
            assert e2.sent == []
            if boundary == 'entropy_submitting':
                assert len(h2.sent) == 1
                assert round(h2.sent[0]['qty'] * 10000) == closed
            else:
                assert h2.sent == []
            if boundary == 'flat_before_done':
                await complete_background_audit(restarted)
                assert journal2.active_live_cycle() is None
            else:
                active = journal2.active_live_cycle()
                assert active['phase'] == 'OPEN'
                assert (active['entropy_units'], active['hedge_units']) == (55, -55)
            assert journal2.pending() == []

        asyncio.run(resume())
        journal2.close()


def test_close_first_zero_fills_try_all_rooms_without_hedge(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [0, 0, 0]

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=85, h_units=-85,
            phase='CLOSE_FIRST')
        # This fixture represents a completed opening with a recorded unit grid.
        for order in journal.cycle_orders(cycle['cycle_id']):
            journal.ensure_live_step(
                step_key=f"{cycle['cycle_id']}:seed-{order['venue']}:0",
                cycle_id=cycle['cycle_id'], order_ref=order['order_ref'],
                venue=order['venue'], side=order['side'],
                requested_units=order['requested_units'], reduce_only=False,
                quantity_step=runtime.instrument.quantity_step)
        journal.update_live_cycle(cycle['cycle_id'], phase='OPEN_HEDGE', updated_ts=time.time())
        assert journal.lock_basis_profit_basis(cycle['cycle_id'],
            quantity_step=runtime.instrument.quantity_step, locked_ts=time.time())
        journal.update_live_cycle(cycle['cycle_id'], phase='CLOSE_FIRST', updated_ts=time.time())
        cycle = journal.active_live_cycle()
        entropy.set_book(104.00, 104.01)  # The full pair clears the 2.5 bp floor.
        await runtime._close(cycle)
        assert len(entropy.sent) == 3
        assert hedge.sent == []
        active = journal.active_live_cycle()
        assert active['entropy_units'] == 85
        assert active['hedge_units'] == -85
        assert active['phase'] == 'OPEN'

    asyncio.run(scenario())
    journal.close()


def test_close_first_full_fill_hedges_all_and_finishes_flat(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=85, h_units=-85,
            phase='CLOSE_FIRST')
        await runtime._close(cycle)
        assert len(entropy.sent) == len(hedge.sent) == 1
        assert entropy.sent[0]['reduce_only']
        assert hedge.sent[0]['is_buy'] and hedge.sent[0]['reduce_only']
        assert round(hedge.sent[0]['qty'] * 10000) == 85
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert entropy.position == hedge.position == 0

    asyncio.run(scenario())
    journal.close()


def test_close_first_unknown_resolves_same_id_without_duplicate_send(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [30 / 85]
    entropy.unknown_once = True

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=85, h_units=-85,
            phase='CLOSE_FIRST')
        await runtime._close(cycle)
        assert len(entropy.sent) == 1
        assert len(hedge.sent) == 1
        assert entropy.sent[0]['order_ref'] not in entropy.pending
        active = journal.active_live_cycle()
        assert active['entropy_units'] == 55
        assert active['hedge_units'] == -55
        assert active['phase'] == 'OPEN'
        assert journal.pending() == []

    asyncio.run(scenario())
    journal.close()


def test_unknown_close_does_not_repair_without_authoritative_positions(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def unresolved(order_ref, timeout=None):
        return {"status": "timeout", "filled_base": 0, "avg_px": None,
                "err": None, "unresolved": True, "order_ref": order_ref}

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=0, h_units=-85)
        add_unknown_close(journal)
        hedge.resolve_order = unresolved
        hedge.position_error = RuntimeError('position unavailable')

        assert not await runtime._audit_positions()
        await runtime._cover_unknown_exposure(cycle)

        assert entropy.sent == hedge.sent == []
        assert len(journal.pending()) == 1
        assert runtime._last_position_evidence.cycle_id != cycle['cycle_id']
        assert not runtime._basis_recovered
        assert runtime._position_audit_requested.is_set()

    asyncio.run(scenario())
    journal.close()


def test_unknown_recovery_waits_same_id_terminal_then_reduces_without_reversal(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    resolve_calls = 0

    async def resolve_late(order_ref, timeout=None):
        nonlocal resolve_calls
        resolve_calls += 1
        return {"status": "timeout", "filled_base": 0, "avg_px": None,
                "err": None, "unresolved": True, "order_ref": order_ref}

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=0, h_units=-85)
        add_unknown_close(journal)
        hedge.resolve_order = resolve_late

        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._cover_unknown_exposure(cycle)
        assert hedge.sent == []
        assert journal.order('123')['state'] == 'UNKNOWN'
        assert round(hedge.position * 10000) == -85

        async def terminal_late(order_ref, timeout=None):
            nonlocal resolve_calls
            resolve_calls += 1
            assert order_ref == '123'
            return {'status': 'canceled', 'filled_base': 0, 'avg_px': None,
                    'fee_usd': 0, 'unresolved': False, 'order_ref': order_ref}

        hedge.resolve_order = terminal_late
        await runtime._resolve_pending()
        await runtime._resume_cycle(journal.active_live_cycle())
        await complete_background_audit(runtime)
        assert round(hedge.position * 10000) == 0
        assert journal.pending() == []
        assert journal.active_live_cycle() is None
        assert all(order['reduce_only'] for order in hedge.sent)
        assert resolve_calls == 2
        assert len(hedge.sent) == 1
        assert hedge.sent[0]['is_buy'] and round(hedge.sent[0]['qty'] * 10000) == 85
        old = journal.order('123')
        assert old['state'] == 'TERMINAL'
        assert old['filled_units'] == 0

    asyncio.run(scenario())
    journal.close()


def test_active_cycle_loss_latches_batch_and_review_stop(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings,
                               single_cycle_review_loss_usd=D("0.001"))

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D("0"))
        cycle = journal.active_live_cycle()
        if cycle['entropy_units'] > 0:
            entropy.set_book(90.00, 90.01)
        else:
            entropy.set_book(110.00, 110.01)
        hedge.set_book(99.99, 100.00)
        await runtime._observe_active_risk(cycle)
        state = journal.batch_state("batch")
        assert not state["can_open"]
        assert state['block_reason'] == 'SINGLE_CYCLE_REVIEW_LOSS'
        assert state['block_cycle_id'] == cycle['cycle_id']
        assert D(state['block_value']) <= D('-.001')
        assert D(state['block_threshold']) == D('-.001')

    asyncio.run(scenario())
    journal.close()


def test_unrelated_account_equity_poll_does_not_latch_batch(tmp_path):
    runtime, _entropy, _hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()

        async def equity_drop():
            runtime.stop.set()
            return D('50')

        runtime._account_equity = equity_drop
        runtime._last_equity = D('100')
        await runtime._equity_loop()
        state = journal.batch_state('batch')
        assert not state['closed_for_entries']
        assert state['close_reason'] is None

    asyncio.run(scenario())
    journal.close()


def test_account_equity_change_cannot_latch_v3_strategy_loss(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def unrelated_account_equity():
        raise AssertionError("raw account equity is not V3 strategy PnL")

    entropy.fetch_equity = unrelated_account_equity
    hedge.fetch_equity = unrelated_account_equity

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D("0"))
        cycle = journal.active_live_cycle()
        assert cycle is not None
        runtime._mark_to_flat = lambda _cycle, _market: D('-.01')
        await runtime._observe_active_risk(cycle)
        state = journal.batch_state('batch')
        assert not state['closed_for_entries']
        assert state['close_reason'] is None

    asyncio.run(scenario())
    journal.close()


def test_batch_closed_status_reports_cycle_limit_without_stopping_monitor(tmp_path):
    runtime, _entropy, _hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        for cycle_id in ('one', 'two'):
            journal.begin_cycle('batch', cycle_id)
            journal.finish_cycle(
                cycle_id, kind='clean', net_pnl_usd=D('0'),
                positions_verified_flat=True, all_orders_terminal=True)
        await runtime._maybe_open(runtime._market(), D('0'))

    asyncio.run(scenario())
    status = json.loads((tmp_path / 'status.json').read_text())
    assert status['current_state'] == 'BATCH_CLOSED'
    assert status['reason'] == 'MAX_POSITION_CYCLES'
    assert status['value'] == status['threshold'] == 2
    assert not runtime.stop.is_set()
    journal.close()


def test_zero_fill_hedge_unwinds_entropy_with_reduce_only(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    hedge.fill_fractions = [0, 0, 0, 0, 0, 0]

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D("0"))
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert round(entropy.position, 8) == round(hedge.position, 8) == 0
        assert entropy.sent[-1]["reduce_only"]
        result = journal.db.execute("SELECT result_kind FROM cycles").fetchone()[0]
        assert result == "repair"

    asyncio.run(scenario())
    journal.close()


def test_hedge_repair_completes_pair_before_unwinding_entropy(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    hedge.fill_fractions = [0, 0, 0, 1]

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D("0"))
        cycle = journal.active_live_cycle()
        assert cycle is not None and cycle["phase"] == "OPEN"
        assert cycle["entropy_units"] == -cycle["hedge_units"] != 0
        assert len(hedge.sent) == 4
        assert not entropy.sent[-1]["reduce_only"]

    asyncio.run(scenario())
    journal.close()


def test_registered_but_unsent_step_reuses_same_reference(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        journal.ensure_batch("batch", max_position_cycles=2, loss_budget_usd=D("3"))
        journal.create_live_cycle(
            batch_id="batch", cycle_id="crash-cycle", direction="short_entropy",
            planned_units=85, reference_bps=D("0"), entry_basis_bps=D("5"),
            started_ts=time.time())
        ref = OrderRefFactory.create(
            "hl", deployment="test", cycle_id="crash-cycle", leg="open_first:0")
        journal.ensure_live_step(
            step_key="crash-cycle:open_first:0", cycle_id="crash-cycle",
            order_ref=ref, venue="entropy", side="sell", requested_units=85,
            reduce_only=False, quantity_step=runtime.instrument.quantity_step)
        await runtime._open_first(journal.active_live_cycle())
        assert entropy.sent[0]["order_ref"] == ref
        assert len([s for s in journal.live_steps("crash-cycle")
                    if ":open_first:" in s["step_key"]]) == 1
        opened = journal.active_live_cycle()
        assert opened["entropy_units"] == -85
        assert opened["hedge_units"] == 85
        entropy.set_book(96.00, 96.01)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(opened)
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert [round(o["qty"] * 10000) for o in entropy.sent] == [85, 85]
        assert [round(o["qty"] * 10000) for o in hedge.sent] == [85, 85]

    asyncio.run(scenario())
    journal.close()


def test_partial_hedge_retries_only_remaining_units(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    hedge.fill_fractions = [.5, 1]

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D("0"))
        cycle = journal.active_live_cycle()
        assert cycle["phase"] == "OPEN"
        assert len(hedge.sent) == 2
        first = round(hedge.sent[0]["qty"] * 10000)
        second = round(hedge.sent[1]["qty"] * 10000)
        assert first == abs(cycle["entropy_units"])
        assert 0 < second < first
        assert cycle["entropy_units"] == -cycle["hedge_units"]

    asyncio.run(scenario())
    journal.close()


def test_preflight_connects_and_reconciles_without_orders(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    result = asyncio.run(runtime.preflight())
    assert result["orders_resolved"]
    assert result["entropy_units"] == result["hedge_units"] == 0
    assert entropy.sent == hedge.sent == []
    journal.close()


def test_session_cycle_cap_stops_new_open_while_flat(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.session_max_cycles = 1

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D("0"))
        opened = journal.active_live_cycle()
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(opened)
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        sent = len(entropy.sent) + len(hedge.sent)
        await runtime._maybe_open(runtime._market(), D("0"))
        assert runtime.stop.is_set()
        assert len(entropy.sent) + len(hedge.sent) == sent

    asyncio.run(scenario())
    journal.close()


def test_normal_exit_reduces_known_fills_despite_position_api_failure_then_audits_flat(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        opened = journal.active_live_cycle()
        hedge.position_error = RuntimeError('405 Not Allowed')
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(opened)
        pending_close = journal.active_live_cycle()
        assert pending_close is not None
        assert pending_close['phase'] == 'BASIS_EXIT'
        assert pending_close['entropy_units'] == pending_close['hedge_units'] == 0
        assert len(entropy.sent) == len(hedge.sent) == 2
        assert entropy.sent[-1]['reduce_only'] and hedge.sent[-1]['reduce_only']
        assert entropy.position == hedge.position == 0
        status = json.loads(runtime.status.path.read_text())
        assert status['reason'] == 'background_flat_audit_required'
        assert not pending_close['position_evidence_authoritative']
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is not None
        hedge.position_error = None
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None

    asyncio.run(scenario())
    journal.close()


def test_live_entry_anchor_drives_hold_and_is_reconstructed(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        opened = journal.active_live_cycle()
        assert opened['live_entry_basis_bps'] is not None
        assert D(opened['live_entry_basis_bps']) != D(opened['entry_basis_bps'])
        journal.db.execute('''UPDATE live_cycles SET live_entry_basis_bps=NULL,
            live_entry_basis_source=NULL WHERE cycle_id=?''', (opened['cycle_id'],))
        rebuilt = runtime._live_entry_anchor(journal.active_live_cycle())
        assert rebuilt is not None and rebuilt[1] in {
            'terminal_vwap', 'persisted_step_vwap', 'canonical_order_vwap'}
        journal.update_live_cycle(
            opened['cycle_id'], cash_delta_usd=D('-1'), updated_ts=time.time())
        entropy.set_book(99.98, 99.99)
        hedge.set_book(99.99, 100.00)
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        status = json.loads(runtime.status.path.read_text())
        assert status['current_state'] == 'HOLDING'
        assert D(status['reversion_bps']) < D('2')

    asyncio.run(scenario())
    journal.close()


def test_missing_fill_evidence_cannot_exit_from_cash_net_alone(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        journal.create_live_cycle(
            batch_id='batch', cycle_id='missing-anchor',
            direction='short_entropy', planned_units=85,
            reference_bps=D('0'), entry_basis_bps=D('10'),
            started_ts=time.time())
        journal.update_live_cycle(
            'missing-anchor', phase='OPEN', entropy_units=-85, hedge_units=85,
            cash_delta_usd=D('-0.003'), updated_ts=time.time())
        entropy.position = -0.0085
        hedge.position = 0.0085
        entropy.set_book(100.00, 100.01)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(journal.active_live_cycle())
        status = json.loads(runtime.status.path.read_text())
        assert status['current_state'] == 'HOLDING'
        assert status['reason'] == 'hold_missing_price_evidence'
        assert entropy.sent == hedge.sent == []

        journal.update_live_cycle(
            'missing-anchor', cash_delta_usd=D('1'), updated_ts=time.time())
        await runtime._resume_cycle(journal.active_live_cycle())
        assert entropy.sent == hedge.sent == []
        assert journal.active_live_cycle()['phase'] == 'OPEN'

    asyncio.run(scenario())
    journal.close()


def test_fresh_flat_lease_removes_position_probe_from_signal_path(tmp_path, monkeypatch):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        calls = 0

        async def counted():
            nonlocal calls
            calls += 1
            return 0

        monkeypatch.setattr(runtime.brokers['entropy'], 'position_units', counted)
        monkeypatch.setattr(runtime.brokers['hedge'], 'position_units', counted)

        async def stop_before_send(cycle, decision=None, *, request_reserved=False):
            return None

        monkeypatch.setattr(runtime, '_open_first', stop_before_send)
        await runtime._maybe_open(runtime._market(), D('0'))
        assert journal.active_live_cycle() is not None
        assert calls == 0
        assert runtime._flat_lease is None

    asyncio.run(scenario())
    journal.close()


def test_market_capture_does_not_treat_unchanged_live_book_as_cross_venue_skew(
        tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    now = time.time()
    entropy.set_book(100.00, 100.01)
    hedge.set_book(99.99, 100.00)
    entropy.book.last_update_ts = now - 0.8
    entropy.book.alive_ts = now
    hedge.book.last_update_ts = now
    hedge.book.alive_ts = now
    instrument = instrument_from_venues('pair', entropy, hedge)

    market = market_from_venues(
        RuntimeVenues(entropy, hedge), instrument, now=now,
        settings=runtime.settings)

    assert market.entropy.received_at == market.hedge.received_at == now
    assert market.valid_reason(now, runtime.settings) is None
    assert market.entropy.revision != market.hedge.revision
    journal.close()


def test_stale_flat_lease_blocks_but_final_signal_uses_frozen_first_limit(
        tmp_path, monkeypatch):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        lease = runtime._flat_lease
        assert lease is not None
        runtime._flat_lease = replace(lease, valid_until=0)
        await runtime._maybe_open(runtime._market(), D('0'))
        assert journal.active_live_cycle() is None
        assert entropy.sent == hedge.sent == []

        await runtime._refresh_flat_lease()
        original_market = runtime._market()
        decision = choose_open_plan(
            original_market, runtime.instrument, runtime.settings, D('0'),
            entropy_venue=entropy, hedge_venue=hedge)
        assert decision is not None
        entropy.set_book(100.00, 100.01)
        hedge.set_book(99.99, 100.00)
        await runtime._maybe_open(original_market, D('0'))
        assert entropy.sent[0]['limit_px'] == float(decision.entropy_limit_px)
        assert journal.active_live_cycle() is not None

    asyncio.run(scenario())
    journal.close()


def test_open_first_rate_budget_unavailable_drops_signal_without_position_cycle(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        for _ in range(runtime.settings.entropy_orders_per_min):
            assert journal.reserve_request('entropy', now=time.time(),
                limit=runtime.settings.entropy_orders_per_min,
                reserve_for_completion=0, completion=False)
        await runtime._maybe_open(runtime._market(), D('0'))
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert entropy.sent == hedge.sent == []
        assert journal.batch_state('batch')['position_cycles'] == 0
        assert journal.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0] == 0

    asyncio.run(scenario())
    journal.close()


def test_first_ioc_uses_final_signal_and_retry_reprices_latest_book(
        tmp_path, monkeypatch):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [0, 1]

    async def scenario():
        await runtime.initialize()
        market = runtime._market()
        decision = choose_open_plan(
            market, runtime.instrument, runtime.settings, D('0'),
            entropy_venue=entropy, hedge_venue=hedge)
        assert decision is not None
        original = runtime._execution_benchmark
        calls = []

        def counted(*args, **kwargs):
            if kwargs['venue_key'] == 'entropy':
                calls.append(kwargs['room_bps'])
            return original(*args, **kwargs)

        monkeypatch.setattr(runtime, '_execution_benchmark', counted)
        await runtime._maybe_open(market, D('0'))
        assert entropy.sent[0]['limit_px'] == float(decision.entropy_limit_px)
        assert calls == [runtime.settings.first_rooms_bps[1]]
        assert len(entropy.sent) == 2

    asyncio.run(scenario())
    journal.close()


def test_open_zero_fill_stops_when_original_signal_disappears(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [0]
    send = entropy.send_taker

    async def move_book_after_first(**kwargs):
        result = await send(**kwargs)
        entropy.set_book(100.00, 100.01)
        hedge.set_book(99.99, 100.00)
        return result

    entropy.send_taker = move_book_after_first

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        assert len(entropy.sent) == 1
        assert hedge.sent == []
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None

    asyncio.run(scenario())
    journal.close()


def test_separate_first_leg_rooms_keep_legacy_fallback():
    legacy = Settings.from_mapping({'first_rooms_bps': ['3', '5', '8']})
    assert legacy.first_leg_execution_policy('open') == (
        (D('3'), D('5'), D('8')), D('10'))
    assert legacy.first_leg_execution_policy('close') == legacy.first_leg_execution_policy('open')
    split = Settings.from_mapping({
        'first_rooms_bps': ['3', '5', '8'],
        'open_first_rooms_bps': ['3', '3', '3'],
        'close_first_rooms_bps': ['2', '4'],
        'close_first_hard_cap_bps': '9',
    })
    assert split.first_leg_execution_policy('open') == (
        (D('3'), D('3'), D('3')), D('10'))
    assert split.first_leg_execution_policy('close') == ((D('2'), D('4')), D('9'))


def test_reference_sampler_runs_while_execution_loop_is_idle(tmp_path, monkeypatch):
    runtime, _, _, journal = setup_runtime(tmp_path)
    observed = []
    original = runtime.reference.observe

    def counted(*args, **kwargs):
        observed.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(runtime.reference, 'observe', counted)

    async def scenario():
        await runtime.initialize()
        task = asyncio.create_task(runtime._reference_loop())
        await asyncio.sleep(.55)
        runtime.stop.set()
        await task
        assert len(observed) >= 2

    asyncio.run(scenario())
    journal.close()


def test_close_execution_benchmark_uses_actual_close_sides(tmp_path):
    runtime, _, _, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        market = runtime._market()
        cycle = {'direction': 'long_entropy', 'reference_bps': '0'}
        e_cash, _ = market.entropy.quote(
            Side.SELL, 85, runtime.instrument.quantity_step)
        h_cash, _ = market.hedge.quote(
            Side.BUY, 85, runtime.instrument.quantity_step)
        expected = (e_cash / h_cash - 1) * D('10000')
        benchmark = runtime._execution_benchmark(
            cycle, venue_key='entropy', side=Side.SELL, units=85,
            room_bps=D('3'), hard_cap_bps=D('10'), market=market)
        assert benchmark['paired_basis_bps'] == expected
        assert benchmark['dislocation_bps'] == expected

    asyncio.run(scenario())
    journal.close()


def test_close_zero_fill_stops_when_executable_exit_disappears(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    entropy.fill_fractions = [0]
    send = entropy.send_taker

    async def move_book_after_first(**kwargs):
        result = await send(**kwargs)
        entropy.set_book(98, 98.01)
        return result

    entropy.send_taker = move_book_after_first

    async def scenario():
        await runtime.initialize()
        cycle = seed_close_cycle(
            journal, entropy, hedge, e_units=85, h_units=-85,
            phase='CLOSE_FIRST')
        await runtime._close(cycle)
        assert len(entropy.sent) == 1
        assert hedge.sent == []
        active = journal.active_live_cycle()
        assert active['phase'] == 'OPEN'
        assert active['entropy_units'] == -active['hedge_units'] == 85

    asyncio.run(scenario())
    journal.close()


def test_restart_in_unsent_close_first_rechecks_then_executes_reduce_only_close(tmp_path):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        await runtime._maybe_open(runtime._market(), D('0'))
        cycle = journal.active_live_cycle()
        assert cycle and cycle['phase'] == 'OPEN'
        journal.update_live_cycle(
            cycle['cycle_id'], phase='CLOSE_FIRST', updated_ts=time.time())
        await runtime._resume_cycle(journal.active_live_cycle())
        # No order was sent before the crash. Do not revive a stale Exit signal.
        assert journal.active_live_cycle()['phase'] == 'OPEN'
        assert len(entropy.sent) == len(hedge.sent) == 1
        entropy.set_book(99.60, 99.61)
        hedge.set_book(99.99, 100.00)
        await runtime._resume_cycle(journal.active_live_cycle())
        await complete_background_audit(runtime)
        assert journal.active_live_cycle() is None
        assert entropy.position == hedge.position == 0
        assert entropy.sent[-1]['reduce_only']
        assert hedge.sent[-1]['reduce_only']

    asyncio.run(scenario())
    journal.close()


def test_lighter_account_snapshot_uses_one_cached_rest_read(monkeypatch):
    import lighter

    class AccountApi:
        def __init__(self, api_client):
            raise AssertionError('account reads must use the shared REST path')

    monkeypatch.setattr(lighter, 'AccountApi', AccountApi)
    venue = object.__new__(LighterVenue)
    venue.conf = SimpleNamespace(
        lighter_creds=SimpleNamespace(account_index=42))
    venue.name = 'lighter'
    venue.signer = SimpleNamespace(api_client='shared-client')
    venue.market_id = 7

    calls = 0

    async def raw_account():
        nonlocal calls
        calls += 1
        return {
            'total_asset_value': '123.4', 'available_balance': '100.1',
            'positions': [{'market_id': 7, 'sign': -1, 'position': '0.085'}],
        }

    venue._raw_account = raw_account

    async def scenario():
        snapshot = await venue.fetch_account_snapshot()
        assert snapshot.source == 'raw_rest_fallback'
        assert snapshot.equity == 123.4 and snapshot.available == 100.1
        assert await venue.fetch_position() == -0.085
        assert calls == 1

    asyncio.run(scenario())


def test_lighter_forced_position_snapshot_skips_pre_fill_ws_cache():
    venue = object.__new__(LighterVenue)
    venue.market_id = 7
    venue.orders_feed = SimpleNamespace(
        fresh_account_snapshot=lambda: SimpleNamespace(
            positions=({'market_id': 7, 'sign': -1, 'position': '0.085'},),
            received_ts=1.0, source='ws', generation=4))
    calls = 0

    async def rest_snapshot(*, force_refresh=False):
        nonlocal calls
        assert force_refresh
        calls += 1
        return SimpleNamespace(
            positions=({'market_id': 7, 'sign': 1, 'position': '0'},),
            received_ts=2.0, source='official_sdk')

    venue.fetch_account_snapshot = rest_snapshot

    async def scenario():
        ws = await venue.fetch_position_snapshot()
        assert ws.position == -0.085 and ws.generation == 4
        rest = await venue.fetch_position_snapshot(force_rest=True)
        assert rest.position == 0 and rest.source == 'lighter_account_rest:official_sdk'
        assert calls == 1

    asyncio.run(scenario())
