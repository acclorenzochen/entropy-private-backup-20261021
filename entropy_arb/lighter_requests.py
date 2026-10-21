"""Conservative, process-local admission and backoff for Lighter HTTP calls."""
from __future__ import annotations

import asyncio
import math
import time
import weakref
from collections import Counter, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Callable, Mapping
from urllib.parse import urlsplit


class RequestDeferred(RuntimeError):
    def __init__(self, reason: str, retry_after: float):
        self.reason = reason
        self.retry_after = max(0.0, float(retry_after))
        super().__init__(f"lighter request deferred: {reason}; retry in {self.retry_after:.3f}s")


@dataclass(eq=False)
class _RequestEvent:
    timestamp: float
    priority: int
    state: str = "reserved"


@dataclass
class RequestTicket:
    budget: "RequestBudget"
    events: list[_RequestEvent]
    owner: object | None = None

    @property
    def remaining(self) -> int:
        return sum(event.state == "reserved" for event in self.events)


class RequestBudget:
    """Count request starts in a rolling minute, independent of account tier.

    Historical I/O is serialized separately so it cannot hold up trading or
    account/risk requests. Only unused reservations can be released; actual
    starts remain counted even if the request subsequently fails.
    """

    def __init__(self, limit: int = 48, reserve: int = 12, history_limit: int = 6,
                 *, clock: Callable[[], float] = time.monotonic,
                 wall_clock: Callable[[], float] = time.time,
                 sleep: Callable = asyncio.sleep):
        if not isinstance(limit, int) or not 1 <= limit <= 60:
            raise ValueError("limit must be an integer between 1 and 60")
        if not isinstance(reserve, int) or not 0 <= reserve < limit:
            raise ValueError("reserve must be an integer below limit")
        if not isinstance(history_limit, int) or not 0 <= history_limit <= limit - reserve:
            raise ValueError("history must be an integer within the ordinary budget")
        self.limit = limit
        self.reserved = reserve
        self.history_limit = history_limit
        self._clock, self._wall_clock, self._sleep = clock, wall_clock, sleep
        self._events: deque[_RequestEvent] = deque()
        self._history_lock = asyncio.Lock()
        self._blocked_until = 0.0
        self._blocked_reason = ""
        self._captcha_failures = 0
        self._rate_failures = 0
        self._queued = self._history_queued = self._history_in_flight = 0
        self._counters: Counter = Counter()
        self._endpoints: Counter = Counter()
        self._defer_reasons: Counter = Counter()

    def _prune(self, now: float) -> None:
        while self._events and self._events[0].timestamp <= now - 60.0:
            self._events.popleft()

    @staticmethod
    def _validate(cost: int, priority: int) -> None:
        if not isinstance(cost, int) or cost < 1:
            raise ValueError("request cost must be a positive integer")
        if priority not in (0, 1, 2):
            raise ValueError("request priority must be 0, 1, or 2")

    def _delay(self, cost: int, priority: int) -> RequestDeferred | None:
        now = self._clock()
        self._prune(now)
        if now < self._blocked_until:
            return RequestDeferred(self._blocked_reason, self._blocked_until - now)
        ceiling = self.limit if priority == 0 else self.limit - self.reserved
        if cost > ceiling or (priority == 2 and cost > self.history_limit):
            raise ValueError("request cost exceeds its admission budget")
        used = len(self._events)
        remaining = used
        budget_delay = 0.0
        if used + cost > ceiling:
            for event in self._events:
                remaining -= 1
                if remaining + cost <= ceiling:
                    budget_delay = event.timestamp + 60.0 - now
                    break
        history_delay = 0.0
        if priority == 2:
            history_used = sum(event.priority == 2 for event in self._events)
            if history_used + cost > self.history_limit:
                for event in self._events:
                    if event.priority != 2:
                        continue
                    history_used -= 1
                    if history_used + cost <= self.history_limit:
                        history_delay = event.timestamp + 60.0 - now
                        break
        if budget_delay or history_delay:
            reason = "history_budget" if history_delay >= budget_delay else "request_budget"
            return RequestDeferred(reason, max(budget_delay, history_delay))
        return None

    def _raise_deferred(self, deferred: RequestDeferred) -> None:
        self._counters["deferred"] += 1
        self._defer_reasons[deferred.reason] += 1
        raise deferred

    def reserve(self, cost: int = 1, priority: int = 0) -> RequestTicket:
        """Synchronously consume admission before starting a transaction."""
        self._validate(cost, priority)
        deferred = self._delay(cost, priority)
        if deferred is not None:
            self._raise_deferred(deferred)
        events = [_RequestEvent(self._clock(), priority) for _ in range(cost)]
        self._events.extend(events)
        self._counters["admitted_units"] += cost
        return RequestTicket(self, events)

    def consume_reserved(self, ticket: RequestTicket, endpoint: str = "unknown") -> None:
        """Move one prepaid unit to its actual start time, without awaiting."""
        if ticket.budget is not self:
            raise ValueError("request ticket belongs to another budget")
        now = self._clock()
        self._prune(now)
        if now < self._blocked_until:
            self._raise_deferred(RequestDeferred(self._blocked_reason, self._blocked_until - now))
        event = next((event for event in ticket.events if event.state == "reserved"), None)
        if event is None:
            raise ValueError("request ticket has no unused units")
        if event in self._events:
            self._events.remove(event)
        else:
            fresh = self.reserve(1, priority=event.priority).events[0]
            self._events.remove(fresh)
        event.timestamp, event.state = now, "started"
        self._events.append(event)
        self.record(endpoint)

    def renew_reserved(self, ticket: RequestTicket, cost: int | None = None) -> None:
        """Atomically recheck prepaid admission before handing ownership to SDK."""
        if ticket.budget is not self:
            raise ValueError("request ticket belongs to another budget")
        pending = [event for event in ticket.events if event.state == 'reserved']
        if not pending:
            raise ValueError("request ticket has no unused units")
        priority = pending[0].priority
        if any(event.priority != priority for event in pending):
            raise ValueError("mixed request ticket priorities")
        cost = len(pending) if cost is None else cost
        self._validate(cost, priority)
        now = self._clock()
        self._prune(now)
        active = [event for event in pending if event in self._events]
        for event in active:
            self._events.remove(event)
        try:
            deferred = self._delay(cost, priority)
            if deferred is not None:
                self._raise_deferred(deferred)
        except BaseException:
            self._events.extend(active)
            self._events = deque(sorted(self._events, key=lambda event: event.timestamp))
            raise
        for event in pending[cost:]:
            event.state = 'released'
        selected = pending[:cost]
        for _ in range(cost - len(selected)):
            event = _RequestEvent(now, priority)
            ticket.events.append(event)
            selected.append(event)
        for event in selected:
            event.timestamp = now
        self._events.extend(selected)
        self._counters['admitted_units'] += max(0, cost - len(active))
        self._counters['released_units'] += max(0, len(active) - cost)

    def release_reserved(self, ticket: RequestTicket) -> None:
        if ticket.budget is not self:
            raise ValueError("request ticket belongs to another budget")
        for event in ticket.events:
            if event.state != "reserved":
                continue
            if event in self._events:
                self._events.remove(event)
                self._counters["released_units"] += 1
            event.state = "released"

    def record(self, endpoint: str, cost: int = 1) -> None:
        """Record actual starts, including calls covered by a prepaid ticket."""
        self._validate(cost, 0)
        path = str(endpoint).split("?", 1)[0].split("#", 1)[0]
        if "://" in path:
            path = urlsplit(path).path
        self._endpoints[path or "unknown"] += cost
        self._counters["started"] += cost

    @asynccontextmanager
    async def request(self, priority: int = 1, cost: int = 1,
                      wait: bool | None = None, endpoint: str = "unknown"):
        self._validate(cost, priority)
        wait = priority == 2 if wait is None else wait
        self._queued += 1
        if priority == 2:
            self._history_queued += 1
        admitted = False
        locked = False
        ticket = None
        try:
            while not admitted:
                if priority == 2:
                    if not wait and self._history_lock.locked():
                        self._counters["deferred"] += 1
                        self._defer_reasons["history_in_flight"] += 1
                        raise RequestDeferred("history_in_flight", 1.0)
                    await self._history_lock.acquire()
                    locked = True
                try:
                    ticket = self.reserve(cost, priority)
                    for _ in range(cost):
                        self.consume_reserved(ticket, endpoint)
                    admitted = True
                except RequestDeferred as exc:
                    if ticket is not None:
                        self.release_reserved(ticket)
                        ticket = None
                    if locked:
                        self._history_lock.release()
                        locked = False
                    if not wait:
                        raise
                    self._counters["waits"] += 1
                    await self._sleep(exc.retry_after)
            self._queued -= 1
            if priority == 2:
                self._history_queued -= 1
                self._history_in_flight += 1
            yield
        finally:
            if ticket is not None:
                self.release_reserved(ticket)
            if admitted:
                if priority == 2:
                    self._history_in_flight -= 1
            else:
                self._queued -= 1
                if priority == 2:
                    self._history_queued -= 1
            if locked:
                self._history_lock.release()

    def defer(self, status: int, headers: Mapping | None = None) -> str | None:
        """Share exchange backoff; no response body or credentials are retained."""
        if status not in (405, 429):
            return None
        lowered = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        captcha = status == 405 and lowered.get("x-amzn-waf-action", "").strip().lower() == "captcha"
        if captcha:
            self._captcha_failures += 1
            delay = (300.0, 900.0, 1800.0)[min(self._captcha_failures - 1, 2)]
            reason = "captcha"
        else:
            self._rate_failures += 1
            delay = min(900.0, 60.0 * (2 ** min(self._rate_failures - 1, 4)))
            reason = f"http_{status}"
        retry_after = lowered.get("retry-after", "")
        try:
            server_delay = float(retry_after)
        except (ValueError, TypeError):
            try:
                server_delay = parsedate_to_datetime(retry_after).timestamp() - self._wall_clock()
            except (ValueError, TypeError, OverflowError):
                server_delay = 0.0
        if math.isfinite(server_delay):
            delay = max(delay, server_delay)
        until = self._clock() + delay
        if until >= self._blocked_until:
            self._blocked_until, self._blocked_reason = until, reason
        self._counters[f"response_{status}"] += 1
        self._counters["captcha" if captcha else "rate_limited"] += 1
        return reason

    def record_success(self) -> None:
        self._counters["successful"] += 1
        if self._clock() >= self._blocked_until:
            self._captcha_failures = self._rate_failures = 0

    def snapshot(self) -> dict:
        now = self._clock()
        self._prune(now)
        used = len(self._events)
        return {
            "limit_per_minute": self.limit,
            "high_priority_reserve": self.reserved,
            "history_limit_per_minute": self.history_limit,
            "used": used,
            "high_priority_remaining": max(0, self.limit - used),
            "ordinary_remaining": max(0, self.limit - self.reserved - used),
            "history_used": sum(event.priority == 2 for event in self._events),
            "reserved_units": sum(event.state == "reserved" for event in self._events),
            "queued": self._queued,
            "history_queued": self._history_queued,
            "history_in_flight": self._history_in_flight,
            "blocked_for_sec": max(0.0, self._blocked_until - now),
            "blocked_reason": self._blocked_reason if now < self._blocked_until else "",
            "counters": dict(self._counters),
            "endpoint_starts": dict(self._endpoints),
            "defer_reasons": dict(self._defer_reasons),
        }


_budgets: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def shared_budget(api_url: str, *, limit: int = 48, reserve: int = 12,
                  history_limit: int = 6) -> RequestBudget:
    """All adapters on one event loop and API host share one conservative quota."""
    loop = asyncio.get_running_loop()
    parsed = urlsplit(api_url.rstrip("/"))
    key = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
    per_loop = _budgets.setdefault(loop, {})
    if key not in per_loop:
        per_loop[key] = RequestBudget(limit, reserve, history_limit)
    elif (limit, reserve, history_limit) != (
            per_loop[key].limit, per_loop[key].reserved, per_loop[key].history_limit):
        raise ValueError("conflicting Lighter request budgets on one API host")
    return per_loop[key]
