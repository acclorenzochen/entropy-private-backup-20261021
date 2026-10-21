"""Close-group UNKNOWN settlement regressions through the real Runtime.

All quantities in comments are common integer units (one unit = .0001).
Only venue I/O is fake; no Runtime, broker, or Journal methods are replaced.
"""
import asyncio
import time
from decimal import Decimal as D

from entropy_arb.v3.core import Side
from entropy_arb.v3.journal import Intent, Result
from entropy_arb.v3.live_host import OrderRefFactory
from tests_v3.test_runtime import seed_close_cycle, setup_runtime


FIRST_KEY = 'close-risk:close_first_0:0'


def register_close(journal, venue, *, terminal):
    leg = 'close_first_0' if venue == 'entropy' else 'close_hedge_1'
    kind = 'hl' if venue == 'entropy' else 'lighter'
    side = Side.SELL if venue == 'entropy' else Side.BUY
    ref = OrderRefFactory.create(
        kind, deployment='test', cycle_id='close-risk', leg=f'{leg}:0')
    journal.ensure_live_step(
        step_key=f'close-risk:{leg}:0', cycle_id='close-risk', order_ref=ref,
        venue=venue, side=side.value, requested_units=30, reduce_only=True,
        execution_group_id=FIRST_KEY,
        parent_step_key=None if venue == 'entropy' else FIRST_KEY)
    journal.prepare(Intent(ref, 'close-risk', venue, side, 30, D('100'), True))
    journal.mark_submitting(ref)
    journal.record(ref, Result(True, 30, D('100'), D('0')) if terminal else
                   Result(False, 0, note='timeout_after_actual_fill'))
    return ref


def unknown(ref):
    return {'status': 'unknown', 'filled_base': 0, 'avg_px': None,
            'unresolved': True, 'order_ref': ref}


def terminal_30(ref):
    return {'status': 'filled', 'filled_base': .003, 'avg_px': 100,
            'fee_usd': 0, 'unresolved': False, 'order_ref': ref}


def actual_units(entropy, hedge):
    return round(entropy.position * 10000), round(hedge.position * 10000)


def test_unknown_hedge_mixed_age_snapshots_do_not_reduce_completed_pair(tmp_path):
    """E terminal 30; H UNKNOWN known 0/actual 30; E stale 85/H fresh -55.

    Before the fix, UNKNOWN cover sells another E30, and the original H30
    terminal then causes another H BUY30: actual 55/-55 becomes 25/-25.
    """
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                         phase='CLOSE_HEDGE')
        e_ref = register_close(journal, 'entropy', terminal=True)
        journal.apply_live_step(
            FIRST_KEY, result_units=30, cash_delta_usd=D('.3'),
            accounting_final=True, entropy_units=55, hedge_units=-85,
            next_phase='CLOSE_HEDGE', updated_ts=time.time())
        h_ref = register_close(journal, 'hedge', terminal=False)
        entropy.position, hedge.position = .0055, -.0055
        assert journal.order(e_ref)['filled_units'] == 30
        pending = journal.pending()
        assert [(p['order_ref'], p['requested_units'], p['filled_units'])
                for p in pending] == [(h_ref, 30, 0)]
        resolved_refs = []
        original_fetch = entropy.fetch_position

        async def unresolved_h(ref, timeout=None):
            resolved_refs.append(ref)
            assert ref == h_ref
            return unknown(ref)

        async def stale_entropy():
            return .0085  # Pre-E-fill snapshot; H fetch returns current -.0055.

        hedge.resolve_order = unresolved_h
        entropy.fetch_position = stale_entropy
        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        after_unknown = actual_units(entropy, hedge)
        e_sends_after_unknown = list(entropy.sent)
        assert journal.order(h_ref)['state'] == 'UNKNOWN'
        assert journal.order(h_ref)['filled_units'] == 0

        # Deliver the original H terminal under its persisted ID. Let both
        # account queries catch up, without suppressing downstream recovery.
        entropy.fetch_position = original_fetch

        async def resolved_h(ref, timeout=None):
            resolved_refs.append(ref)
            assert ref == h_ref
            return terminal_30(ref)

        hedge.resolve_order = resolved_h
        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        cycle = journal.active_live_cycle()
        observed = {
            'pending_ref_before_terminal': h_ref,
            'known_before_terminal': {'entropy': 30, 'hedge': 0},
            'actual_after_unknown': after_unknown,
            'extra_entropy_orders': e_sends_after_unknown,
            'extra_hedge_orders': hedge.sent,
            'actual_after_terminal': actual_units(entropy, hedge),
            'durable_after_terminal': (cycle['entropy_units'], cycle['hedge_units']),
        }
        assert after_unknown == (55, -55), observed
        assert entropy.sent == hedge.sent == [], observed
        assert actual_units(entropy, hedge) == (55, -55), observed
        assert (cycle['entropy_units'], cycle['hedge_units']) == (55, -55), observed
        assert journal.order(h_ref)['filled_units'] == 30
        assert not journal.pending()
        assert resolved_refs and set(resolved_refs) == {h_ref}

    try:
        asyncio.run(scenario())
    finally:
        journal.close()


def test_covered_unknown_first_late_terminal_with_failed_snapshot_applies_once(tmp_path):
    """E UNKNOWN known 0/actual SELL30; real cover BUY H30 reaches 55/-55.

    Then E terminal 30 arrives while E position RPC fails. The cycle already
    includes E's fill, so its durable 55 must not be decremented again to 25.
    """
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)

    async def scenario():
        await runtime.initialize()
        seed_close_cycle(journal, entropy, hedge, e_units=85, h_units=-85,
                         phase='CLOSE_FIRST')
        e_ref = register_close(journal, 'entropy', terminal=False)
        entropy.position = .0055
        resolved_refs = []

        async def unresolved_e(ref, timeout=None):
            resolved_refs.append(ref)
            assert ref == e_ref
            return unknown(ref)

        entropy.resolve_order = unresolved_e
        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        cycle = journal.active_live_cycle()
        assert (cycle['entropy_units'], cycle['hedge_units']) == (55, -55)
        assert actual_units(entropy, hedge) == (55, -55)
        assert entropy.sent == [] and len(hedge.sent) == 1
        assert hedge.sent[0]['is_buy'] and hedge.sent[0]['reduce_only']
        assert round(hedge.sent[0]['qty'] * 10000) == 30
        cover_ref = hedge.sent[0]['order_ref']
        assert journal.order(cover_ref)['filled_units'] == 30
        assert [(p['order_ref'], p['filled_units']) for p in journal.pending()] == [(e_ref, 0)]
        assert not journal.live_step(FIRST_KEY)['applied']

        async def resolved_e(ref, timeout=None):
            resolved_refs.append(ref)
            assert ref == e_ref
            return terminal_30(ref)

        entropy.resolve_order = resolved_e
        entropy.position_error = RuntimeError('E account snapshot unavailable')
        await runtime._resolve_pending()
        await runtime._audit_positions()
        await runtime._resume_cycle(journal.active_live_cycle())
        cycle = journal.active_live_cycle()
        observed = {
            'original_pending_ref': e_ref, 'known_before_terminal': 0,
            'cover_ref': cover_ref, 'cover_terminal_units': 30,
            'durable_after_terminal': (cycle['entropy_units'], cycle['hedge_units']),
            'actual_after_terminal': actual_units(entropy, hedge),
            'first_step_applied': journal.live_step(FIRST_KEY)['applied'],
        }
        assert (cycle['entropy_units'], cycle['hedge_units']) == (55, -55), observed
        assert actual_units(entropy, hedge) == (55, -55), observed
        assert entropy.sent == [] and len(hedge.sent) == 1, observed
        assert journal.order(e_ref)['state'] == 'TERMINAL'
        assert journal.live_step(FIRST_KEY)['applied']
        assert not journal.pending()
        assert resolved_refs and set(resolved_refs) == {e_ref}

    try:
        asyncio.run(scenario())
    finally:
        journal.close()
