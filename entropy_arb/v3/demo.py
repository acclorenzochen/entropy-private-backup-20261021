"""Deterministic end-to-end demonstration. NO network or credentials."""
from __future__ import annotations

import asyncio
from decimal import Decimal as D
from pathlib import Path
import tempfile

from .core import Book, Direction, Instrument, Market, Settings, Side, exit_decision, plan_open, price_limit
from .execution import IntentExecutor, PairEvidence, hedge_units, safe_to_reopen
from .journal import Intent, Journal, Result
from .reference import FastReference


def fixture_market(entropy_bid='99.59', entropy_ask='99.60', now=40.0) -> Market:
    return Market(Book(((D(entropy_bid),1000),), ((D(entropy_ask),1000),), now, f'e-{now}'),
                  Book(((D('100'),1000),), ((D('100.01'),1000),), now, f'h-{now}'))


class FakeBroker:
    is_live = False

    def __init__(self, market: Market, instrument: Instrument) -> None:
        self.market, self.instrument = market, instrument
        self.positions = {'entropy': 0, 'hedge': 0}
        self.cash_net = D('0')
        self.sent: list[Intent] = []
        self.results: dict[str, Result] = {}

    async def submit(self, intent: Intent) -> Result:
        if intent.order_ref in self.results:
            raise AssertionError('duplicate external submit in demo')
        self.sent.append(intent)
        book = getattr(self.market, intent.venue)
        top = book.top(intent.side)
        if (intent.side is Side.BUY and top > intent.limit_px) or \
                (intent.side is Side.SELL and top < intent.limit_px):
            result = Result(True, 0, note='IOC zero fill')
        else:
            if intent.reduce_only:
                old = self.positions[intent.venue]
                if old == 0 or old * intent.side.sign >= 0 or intent.units > abs(old):
                    raise AssertionError('reduce-only would increase/reverse position')
            cash, _ = book.quote(intent.side, intent.units, self.instrument.quantity_step)
            fee_bps = getattr(self.instrument, f'{intent.venue}_fee_bps')
            fee = cash * fee_bps / D('10000')
            self.positions[intent.venue] += intent.side.sign * intent.units
            self.cash_net += -intent.side.sign * cash - fee
            result = Result(True, intent.units,
                            cash / self.instrument.quantity(intent.units), fee, 'fake terminal')
        self.results[intent.order_ref] = result
        return result

    async def resolve(self, intent: Intent) -> Result:
        return self.results.get(intent.order_ref, Result(False,0,note='fake unresolved'))


def make_intent(cid: str, suffix: str, venue: str, side: Side, units: int,
                broker: FakeBroker, cfg: Settings, *, reduce_only=False) -> Intent:
    book = getattr(broker.market, venue)
    _, marginal = book.quote(side, units, broker.instrument.quantity_step)
    room = cfg.first_rooms_bps[0] if venue == 'entropy' else cfg.hedge_rooms_bps[0]
    cap = cfg.first_hard_cap_bps if venue == 'entropy' else cfg.hedge_hard_cap_bps
    tick = getattr(broker.instrument, f'{venue}_tick')
    limit = price_limit(book.top(side), marginal, side, room, cap, tick)
    return Intent(f'{cid}-{suffix}', cid, venue, side, units, limit, reduce_only)


async def run_demo(path: Path) -> dict:
    cfg = Settings()
    inst = Instrument('DEMO:USDC:LINEAR', D('.01'), D('.01'), D('.01'), D('.9'), D('0'))
    ref = FastReference(inst.pair_id,cfg)
    for sec in range(31):
        ref.observe(float(sec),D('0'))
    assert ref.view(30).usable
    journal = Journal(path,identity='fake-accounts:DEMO:USDC:LINEAR')
    journal.ensure_batch('demo',max_position_cycles=2,loss_budget_usd=D('1'))
    broker = FakeBroker(fixture_market(),inst)
    executor = IntentExecutor(journal,broker)
    records=[]
    try:
        for index in range(2):
            # Identical long-side signal can be reused after confirmed FLAT.
            evidence=PairEvidence(**{f'{v}_units':q for v,q in broker.positions.items()},
                                  authoritative=True,all_orders_terminal=True,local_and_exchange_match=True)
            assert safe_to_reopen(evidence,active_execution=False,unresolved_journal_orders=bool(journal.pending()))
            cid=f'cycle-{index}'
            broker.market=fixture_market(now=40+index*10)
            plan=plan_open(broker.market,inst,cfg,ref.view(30).center_bps,
                           Direction.LONG_ENTROPY,10)
            assert plan is not None
            before=broker.cash_net
            journal.begin_cycle('demo',cid)
            opened=await executor.send(make_intent(cid,'open-e','entropy',Side.BUY,plan.units,broker,cfg))
            hedged=await executor.send(make_intent(cid,'open-h','hedge',Side.SELL,hedge_units(opened),broker,cfg))
            assert hedged.cumulative_units==opened.cumulative_units
            assert opened.avg_px is not None and hedged.avg_px is not None
            opening_quantity=inst.quantity(opened.cumulative_units)
            opening_price_cash=opening_quantity*(hedged.avg_px-opened.avg_px)
            opening_matched_notional=opening_quantity*(opened.avg_px+hedged.avg_px)/2
            broker.market=fixture_market('99.99','100.00',now=42+index*10)
            decision=exit_decision(broker.market,inst,cfg,plan.direction,10,plan.executable_basis_bps,
                                   broker.cash_net-before,
                                   accrued_price_cash_usd=opening_price_cash,
                                   profit_entry_notional_usd=opening_matched_notional)
            assert decision.allowed
            closed=await executor.send(make_intent(cid,'close-e','entropy',Side.SELL,10,broker,cfg,reduce_only=True))
            await executor.send(make_intent(cid,'close-h','hedge',Side.BUY,hedge_units(closed),broker,cfg,reduce_only=True))
            assert broker.positions=={'entropy':0,'hedge':0}
            pnl=broker.cash_net-before
            journal.finish_cycle(cid,kind='clean',net_pnl_usd=pnl,
                                 positions_verified_flat=True,all_orders_terminal=True)
            records.append({'cycle_id':cid,'pnl_usd':str(pnl),'orders':4,'result':'clean_fake_cycle'})
        return {'simulation_only':True,'reference_ready_at_seconds':30,
                'cycles':records,'orders':len(broker.sent),'positions':broker.positions,
                'live_orders_sent':0}
    finally:
        journal.close()


def main() -> None:
    import json
    with tempfile.TemporaryDirectory() as tmp:
        print(json.dumps(asyncio.run(run_demo(Path(tmp)/'demo.sqlite3')),indent=2))


if __name__=='__main__':
    main()
