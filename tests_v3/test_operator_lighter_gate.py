"""Last-dispatch protection while admission yields; SDK and HTTP are fake."""
import asyncio
from contextlib import asynccontextmanager

import pytest

from tests_v3.test_lighter_request_integration import (
    Response, install_signer, make_venue, no_network,
)


def paused_post_budget(monkeypatch):
    venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
    waiting, resume = asyncio.Event(), asyncio.Event()
    original_request = budget.request

    @asynccontextmanager
    async def request(*args, **kwargs):
        if kwargs.get('endpoint') == '/api/v1/sendTx':
            waiting.set()
            await resume.wait()
        async with original_request(*args, **kwargs):
            yield

    monkeypatch.setattr(budget, 'request', request)
    # The SDK cache can turn cold after local preparation. Its GET consumes the
    # warm one-request ticket, so POST must use the yielding admission branch.
    signer, rest = install_signer(
        venue, before_order=lambda: signer.nonce_manager.nonce.clear())
    return venue, signer, rest, budget, waiting, resume


@pytest.mark.parametrize('reason', ['operator_stop', 'operator_close', 'custom_guard_reason'])
def test_increase_blocked_during_budget_wait_has_no_new_post(monkeypatch, reason):
    async def scenario():
        venue, signer, rest, budget, waiting, resume = paused_post_budget(monkeypatch)
        blocked = False
        checked = []

        def guard(order_ref):
            checked.append(order_ref)
            return reason if blocked else None

        venue.submission_guard = guard
        epoch = venue._account_epoch
        task = asyncio.create_task(venue.send_taker(
            is_buy=True, qty=.01, limit_px=100, order_ref='101'))
        await asyncio.wait_for(waiting.wait(), .5)
        assert checked == ['101']
        assert rest.calls == [('GET', '/api/v1/nextNonce')]
        blocked = True
        resume.set()
        result = await asyncio.wait_for(task, .5)
        assert checked == ['101', '101']
        assert result['deferred'] is True
        assert result['post_started'] is False and result['proved_local_unsent'] is True
        assert result['unresolved'] is False and result['filled_base'] == 0
        assert result['retry_after'] == 0
        assert result['err'] == f'proved_local_unsent: submission_guard:{reason}'
        assert rest.calls == [('GET', '/api/v1/nextNonce')]
        assert signer.nonce_manager.nonce == {7: 10}
        assert venue._account_epoch == epoch
        assert budget.snapshot()['reserved_units'] == 0
        assert not venue._submission_lock.locked()
        await venue.wait_for_submission()
        assert not venue._submit_tasks

    asyncio.run(scenario())


@pytest.mark.parametrize('reduce_only,completion', [(True, False), (True, True), (False, True)])
def test_reduction_and_guard_allowed_completion_still_dispatch(
        monkeypatch, reduce_only, completion):
    async def scenario():
        venue, _, rest, budget, waiting, resume = paused_post_budget(monkeypatch)
        blocked = False
        checked = []

        def guard(order_ref):
            checked.append(order_ref)
            # Reductions bypass this increase guard. The caller may still
            # authorize a specific completion intent after an operator event.
            return 'operator_close' if blocked and reduce_only else None

        venue.submission_guard = guard
        task = asyncio.create_task(venue.send_taker(
            is_buy=True, qty=.01, limit_px=100, order_ref='101',
            reduce_only=reduce_only, completion=completion))
        await asyncio.wait_for(waiting.wait(), .5)
        blocked = True
        resume.set()
        result = await asyncio.wait_for(task, .5)
        assert result['unresolved'] is True and not result.get('deferred')
        assert rest.calls == [('GET', '/api/v1/nextNonce'), ('POST', '/api/v1/sendTx')]
        assert checked == ([] if reduce_only else ['101', '101'])
        assert budget.snapshot()['reserved_units'] == 0
        assert not venue._submission_lock.locked()

    asyncio.run(scenario())


def test_original_guard_rejection_keeps_existing_unsent_proof(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        venue.submission_guard = lambda order_ref: 'caller_specific_denial'
        result = await venue.send_taker(
            is_buy=True, qty=.01, limit_px=100, order_ref='101')
        assert result['deferred'] and result['proved_local_unsent']
        assert result['post_started'] is False and result['unresolved'] is False
        assert result['err'] == 'proved_local_unsent: submission_guard:caller_specific_denial'
        assert rest.calls == [] and signer.nonce_manager.nonce == {7: 10}
        assert budget.snapshot()['reserved_units'] == 0
        assert not venue._submission_lock.locked()

    asyncio.run(scenario())
