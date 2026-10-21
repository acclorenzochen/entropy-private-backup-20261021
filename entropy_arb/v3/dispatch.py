"""One short retry ladder and one request-budget primitive. No episode locks."""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
import math
from typing import Awaitable, Callable

from .execution import IntentExecutor
from .journal import Intent, Result


class RequestBudget:
    """Rolling-window budget shared by a real API/account resource pool.

    The host must supply the VERIFIED pool limit and charge *all* requests
    sharing that pool. Reserved capacity protects hedge/reduce/reconcile,
    without allowing risk-reducing requests to bypass a venue's hard ceiling.
    Persisted last-use/backoff should be restored by the live host on restart;
    this small primitive itself does not persist or define a venue's limits.
    """
    def __init__(self, limit: int, reserve_for_completion: int, window_seconds=60.0):
        if type(limit) is not int or type(reserve_for_completion) is not int \
                or limit <= 0 or not 0 <= reserve_for_completion < limit \
                or not math.isfinite(window_seconds) or window_seconds <= 0:
            raise ValueError('invalid request pool limits')
        self.limit=limit
        self.reserve=reserve_for_completion
        self.window=window_seconds
        self.used: deque[tuple[float,int]]=deque()
        self.blocked_until=0.0
        self.last_now: float | None=None

    def acquire(self, now: float, *, weight: int=1, completion: bool=False) -> bool:
        if not math.isfinite(now) or type(weight) is not int or weight <= 0:
            raise ValueError('invalid budget request')
        if self.last_now is not None and now < self.last_now:
            raise ValueError('monotonic clock required')
        self.last_now=now
        while self.used and self.used[0][0] <= now-self.window:
            self.used.popleft()
        if now < self.blocked_until:
            return False
        ceiling=self.limit if completion else self.limit-self.reserve
        if sum(w for _,w in self.used)+weight > ceiling:
            return False
        self.used.append((now,weight))
        return True

    def backoff(self, until: float) -> None:
        if not math.isfinite(until):
            raise ValueError('invalid retry-after')
        self.blocked_until=max(self.blocked_until,until)


@dataclass(frozen=True)
class LadderOutcome:
    attempts: int
    result: Result | None
    reason: str


async def run_ladder(executor: IntentExecutor,
                     prepare_fresh: Callable[[int], Awaitable[Intent | None]],
                     *, max_attempts: int=3, retry_seconds: float=0.1,
                     sleep: Callable[[float], Awaitable[None]]=asyncio.sleep) -> LadderOutcome:
    """prepare_fresh must reread books, apply executable entry checks and rate budget.

    No retry after any fill or unknown. Repeated campaigns may be evaluated
    afresh after terminal zero fills, without a lifetime direction lock.
    """
    if type(max_attempts) is not int or max_attempts < 1 \
            or not math.isfinite(retry_seconds) or retry_seconds < 0:
        raise ValueError('invalid retry settings')
    for index in range(max_attempts):
        order=await prepare_fresh(index)
        if order is None:
            return LadderOutcome(index,None,'fresh_preflight_declined')
        result=await executor.send(order)
        if not result.terminal:
            return LadderOutcome(index+1,result,'resolve_same_order')
        if result.cumulative_units:
            return LadderOutcome(index+1,result,'hedge_actual_fill')
        if index+1 < max_attempts:
            await sleep(retry_seconds)
    return LadderOutcome(max_attempts,result,'terminal_zero_fill')
