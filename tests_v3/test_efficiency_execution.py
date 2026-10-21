"""Zero-fill entry retries use current books without blocking account IO."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D

import pytest

from entropy_arb.v3 import runtime as runtime_module
from tests_v3.test_runtime import setup_runtime


@pytest.mark.parametrize('change_books', [False, True])
def test_three_entry_iocs_replan_same_quantity_without_sleep_or_account_reads(
        tmp_path, monkeypatch, change_books):
    runtime, entropy, hedge, journal = setup_runtime(tmp_path)
    runtime.settings = replace(runtime.settings,
        open_first_rooms_bps=(D('3'), D('3'), D('3')),
        open_first_hard_cap_bps=D('3'))
    entropy.fill_fractions = [0, 0, .5]
    checks = []
    planner = runtime_module.plan_open
    send = entropy.send_taker

    def counted(market, instrument, settings, reference, direction, units):
        checks.append((market.entropy.top(direction.entropy_side),
                       market.hedge.top(direction.entropy_side.opposite),
                       reference, direction, units))
        return planner(market, instrument, settings, reference, direction, units)

    async def moving_send(**kwargs):
        result = await send(**kwargs)
        if change_books and len(entropy.sent) < 3:
            offset = D(len(entropy.sent)) / 100
            entropy.set_book(D('100.1') + offset, D('100.11') + offset)
            hedge.set_book(D('99.99') - offset, D('100') - offset)
        return result

    async def forbidden(*args, **kwargs):
        raise AssertionError('entry retry blocked on account IO or fixed sleep')

    async def scenario():
        await runtime.initialize()
        monkeypatch.setattr(runtime_module, 'plan_open', counted)
        monkeypatch.setattr(asyncio, 'sleep', forbidden)
        entropy.fetch_position = hedge.fetch_position = forbidden
        entropy.send_taker = moving_send
        await runtime._maybe_open(runtime._market(), D('0'))
        active = journal.active_live_cycle()
        assert active['phase'] == 'OPEN'
        assert len(entropy.sent) == 3 and len(hedge.sent) == 1
        assert len({order['qty'] for order in entropy.sent}) == 1
        assert len({order['is_buy'] for order in entropy.sent}) == 1
        assert len({order['order_ref'] for order in entropy.sent}) == 3
        assert hedge.sent[0]['qty'] == entropy.sent[0]['qty'] * .5
        assert active['entropy_units'] == -active['hedge_units'] != 0
        assert all(row[2] == D('0') and row[4] == active['planned_units']
                   for row in checks[-2:])
        assert checks[-1][3] == checks[-2][3]
        if change_books:
            assert checks[-1][:2] != checks[-2][:2]
            assert len({order['limit_px'] for order in entropy.sent}) == 3
        else:
            assert checks[-1][:2] == checks[-2][:2]
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 3

    try:
        asyncio.run(scenario())
    finally:
        journal.close()
