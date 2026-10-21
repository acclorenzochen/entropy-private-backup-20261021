"""Small execution primitives; NOT a production live runner.

Live adapter integration intentionally requires a separate audited host. This
package can exercise its order-path logic using a deterministic fake venue,
but cannot accidentally call a real venue merely by changing a config flag.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .journal import Intent, Journal, PendingOrder, Result


class Broker(Protocol):
    is_live: bool

    async def submit(self, intent: Intent) -> Result: ...
    async def resolve(self, intent: Intent) -> Result: ...


class EvidenceRequired(RuntimeError):
    pass


class LiveIntegrationRequired(RuntimeError):
    pass


class IntentExecutor:
    """Prepare -> durable SUBMITTING -> adapter -> durable cumulative result.

    An exception/cancellation must never implicitly turn a possibly transmitted
    request into a zero-fill. A restart resolves SUBMITTING/UNKNOWN by the SAME
    reference. Network retries are the audited host's responsibility.
    """
    def __init__(self, journal: Journal, broker: Broker) -> None:
        self.journal, self.broker = journal, broker
        self.entries_halted = False
        if broker.is_live:
            raise LiveIntegrationRequired('reference core has no audited live safety host; see integration contract')

    async def send(self, intent: Intent) -> Result:
        if self.entries_halted and not intent.reduce_only:
            raise EvidenceRequired('new exposure paused after persistence failure')
        # Failure in either write means NO submit() invocation.
        self.journal.prepare(intent)
        self.journal.mark_submitting(intent.order_ref)
        try:
            result = await self.broker.submit(intent)
        except Exception as exc:
            result = Result(False, 0, note=f'transport outcome unknown: {type(exc).__name__}')
        # CancelledError propagates. SUBMITTING remains durable for restart.
        try:
            self.journal.record(intent.order_ref, result)
        except Exception:
            self.entries_halted = True
            # Do not invent a rollback of the exchange fill. The host must use
            # in-memory evidence + existing risk recovery, never start a new OPEN.
            raise
        return result

    async def resolve(self, intent: Intent) -> Result:
        row = self.journal.order(intent.order_ref)
        if row['state'] == 'PREPARED':
            self.journal.cancel_unsent_prepared(intent.order_ref)
            return Result(True, 0, note='proved_unsent')
        if row['state'] == 'TERMINAL':
            from .core import dec
            return Result(True, row['filled_units'],
                          dec(row['avg_px']) if row['avg_px'] else None,
                          dec(row['fee_usd']) if row['fee_usd'] else None)
        result = await self.broker.resolve(intent)
        self.journal.record(intent.order_ref, result)
        return result


@dataclass(frozen=True)
class PairEvidence:
    entropy_units: int
    hedge_units: int
    authoritative: bool
    all_orders_terminal: bool
    local_and_exchange_match: bool

    @property
    def flat(self) -> bool:
        return self.entropy_units == 0 and self.hedge_units == 0

    @property
    def balanced(self) -> bool:
        return self.entropy_units == -self.hedge_units


def safe_to_reopen(evidence: PairEvidence, *, active_execution: bool,
                   unresolved_journal_orders: bool) -> bool:
    """Historical episode/result/midline-reset is deliberately NOT an input."""
    return bool(evidence.authoritative and evidence.all_orders_terminal
                and evidence.local_and_exchange_match and evidence.flat
                and not active_execution and not unresolved_journal_orders)


def residual_action(evidence: PairEvidence, *, closing: bool) -> tuple[str, int]:
    """Pure risk classification. Quantities remain integer common units.

    Does not infer FLAT from net delta. An unknown pending order requires
    resolution even when a current position snapshot happens to look flat.
    """
    if not evidence.authoritative or not evidence.local_and_exchange_match:
        return 'RECONCILE', 0
    if not evidence.all_orders_terminal:
        return 'RESOLVE_ORDERS', 0
    if evidence.flat:
        return 'FLAT', 0
    if evidence.balanced:
        return ('CONTINUE_REDUCE' if closing else 'HEDGED'), abs(evidence.entropy_units)
    net = evidence.entropy_units + evidence.hedge_units
    return ('REDUCE_RESIDUAL' if closing else 'COMPLETE_OR_UNWIND_EXCESS'), abs(net)


def hedge_units(first_result: Result, already_hedged_units: int = 0) -> int:
    """Only confirmed cumulative fills generate a hedge obligation.

    For the provided sequential kernel terminal first-leg evidence is required.
    Streaming partial-fill hedging is a later optimization, not a hidden fallback.
    """
    if not first_result.terminal:
        raise EvidenceRequired('resolve first order; never hedge requested quantity on timeout')
    if type(already_hedged_units) is not int or already_hedged_units < 0 \
            or already_hedged_units > first_result.cumulative_units:
        raise ValueError('invalid hedged amount')
    return first_result.cumulative_units - already_hedged_units
