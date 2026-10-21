import asyncio
from decimal import Decimal as D
import pytest
from entropy_arb.v3.core import Side
from entropy_arb.v3.dispatch import RequestBudget,run_ladder
from entropy_arb.v3.execution import IntentExecutor
from entropy_arb.v3.journal import Intent,Journal,Result


def test_budget_reserves_completion_but_obeys_absolute_limit():
    b=RequestBudget(6,2)
    assert all(b.acquire(0) for _ in range(4))
    assert not b.acquire(0)
    assert b.acquire(0,completion=True) and b.acquire(0,completion=True)
    assert not b.acquire(0,completion=True)
    assert b.acquire(60)


def test_backoff_applies_to_both_read_and_send_shared_pool():
    b=RequestBudget(10,2);b.backoff(30)
    assert not b.acquire(29,completion=True)
    assert b.acquire(30,weight=2)


@pytest.mark.parametrize('results,expected_attempts,reason',[
    ([Result(True,0),Result(True,0),Result(True,85,D(100))],3,'hedge_actual_fill'),
    ([Result(False,0)],1,'resolve_same_order'),
    ([Result(True,40,D(100))],1,'hedge_actual_fill'),
    ([Result(True,0)]*3,3,'terminal_zero_fill'),
])
def test_ladder(results,expected_attempts,reason,tmp_path):
    class Fake:
        is_live=False
        count=0
        async def submit(self,i):
            r=results[self.count];self.count+=1;return r
    journal=Journal(tmp_path/'j.db',identity='fake')
    fake=Fake();ex=IntentExecutor(journal,fake);prepares=[]
    async def prepare(k):
        prepares.append(k)
        return Intent(str(k),'cycle','entropy',Side.BUY,85,D(100)+D(k)/10)
    async def no_wait(t): pass
    try:
        out=asyncio.run(run_ladder(ex,prepare,sleep=no_wait))
        assert out.reason==reason and out.attempts==expected_attempts
        assert len(prepares)==fake.count==expected_attempts
    finally:journal.close()


def test_first_fill_never_rechecks_entry_economics(tmp_path):
    # The ladder returns immediately to a hedge obligation after any fill.
    class Fake:
        is_live=False
        async def submit(self,i):return Result(True,1,D(100))
    j=Journal(tmp_path/'j.db',identity='fake')
    async def prepare(k):
        assert k==0
        return Intent('one','c','entropy',Side.BUY,85,D(100))
    try:
        out=asyncio.run(run_ladder(IntentExecutor(j,Fake()),prepare))
        assert out.result.cumulative_units==1 and out.reason=='hedge_actual_fill'
    finally:j.close()


def test_open_drawdown_latches_budget_across_restart(tmp_path):
    p=tmp_path/'j.db';j=Journal(p,identity='x')
    j.ensure_batch('b',max_position_cycles=20,loss_budget_usd=D(1))
    assert not j.observe_open_risk('b',active_cycle_net_pnl_usd=D('-1.1'))
    j.close();j=Journal(p,identity='x')
    try:
        assert not j.batch_state('b')['can_open']
        assert not j.observe_open_risk('b',active_cycle_net_pnl_usd=D(0))
    finally:j.close()
