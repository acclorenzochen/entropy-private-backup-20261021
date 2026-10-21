"""Offline dispatch ownership, completion quota, and truthful unsent outcomes."""
import asyncio
from types import SimpleNamespace

import pytest

from entropy_arb.lighter_requests import RequestDeferred
from entropy_arb.venue_lighter import AccountOrdersFeed, LighterWafBlocked
from tests_v3.test_lighter_request_integration import (
    Response, account_payload, install_signer, make_venue, no_network,
)
from tests_v3.test_efficiency_intents import executor, intent, journal, prepare
from tests_v3.test_lighter_startup_verification import startup_venue


async def send(venue, ref='101', **kwargs):
    return await venue.send_taker(
        is_buy=True, qty=.01, limit_px=100, order_ref=ref, **kwargs)


@pytest.mark.parametrize('path', ['/api/v1/account', '/api/v1/orderBookDetails'])
def test_ordinary_reads_cannot_consume_completion_reserve(monkeypatch, path):
    async def scenario():
        venue, session, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        budget.reserve(36)
        with pytest.raises(RequestDeferred, match='request_budget'):
            await venue._get(path)
        assert session.calls == []
        async with budget.request(priority=0, endpoint='/api/v1/accountOrders'):
            pass
        assert budget.snapshot()['used'] == 37
    asyncio.run(scenario())


@pytest.mark.parametrize('cold,cost', [(False, 1), (True, 2)])
@pytest.mark.parametrize('completion', [False, True])
def test_submission_counts_only_actual_warm_or_cold_requests(monkeypatch, cold, cost, completion):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        if cold:
            signer.nonce_manager.nonce.clear()
        result = await send(venue, completion=completion)
        assert result['unresolved'] and not result.get('deferred')
        assert len(rest.calls) == cost
        assert budget.snapshot()['used'] == cost
        assert budget.snapshot()['reserved_units'] == 0
        assert budget.snapshot()['counters']['started'] == cost
        assert not venue._submission_lock.locked()
    asyncio.run(scenario())


def test_warm_completion_can_use_last_slot_while_new_exposure_cannot(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        budget.reserve(47)
        denied = await send(venue)
        assert denied['deferred'] and denied['status'] == 'deferred'
        assert denied['post_started'] is False and denied['proved_local_unsent']
        assert denied['unresolved'] is False and denied['filled_base'] == 0
        assert denied['retry_after'] == 60
        assert signer.orders == rest.calls == []
        completed = await send(venue, '102', completion=True)
        assert completed['unresolved'] and len(rest.calls) == 1
        assert budget.snapshot()['used'] == 48
    asyncio.run(scenario())


def test_cold_completion_needs_two_slots_without_starting_sdk(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        signer.nonce_manager.nonce.clear()
        budget.reserve(47)
        denied = await send(venue, reduce_only=True)
        assert denied['deferred'] and denied['retry_after'] == 60
        assert signer.orders == rest.calls == []
        assert budget.snapshot()['used'] == 47
        assert not venue._submission_lock.locked()
    asyncio.run(scenario())


def test_completion_preflight_is_local_and_zero_first_fill_releases(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        ticket = venue.reserve_completion()
        assert ticket.remaining == 1 and budget.snapshot()['reserved_units'] == 1
        assert signer.orders == rest.calls == []
        venue.release_completion(ticket)
        venue.release_completion(ticket)
        assert budget.snapshot()['used'] == 0
    asyncio.run(scenario())


def test_preflight_consumes_reserved_slot_at_48_without_double_count(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        _, rest = install_signer(venue)
        budget.reserve(47)
        ticket = venue.reserve_completion()
        result = await send(venue, completion_ticket=ticket, completion=True)
        assert result['unresolved'] and len(rest.calls) == 1
        assert ticket.remaining == 0 and budget.snapshot()['used'] == 48
        assert budget.snapshot()['counters']['started'] == 1
    asyncio.run(scenario())


def test_estimate_uses_next_sdk_rotation_without_selecting_nonce(monkeypatch):
    async def scenario():
        venue, _, _, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, _ = install_signer(venue)
        signer.nonce_manager.api_keys_list = [7, 8]
        signer.nonce_manager.current = 0
        assert venue._submission_cost() == 2
        assert signer.nonce_manager.current == 0
        signer.nonce_manager.nonce[8] = 20
        assert venue._submission_cost() == 1
        assert signer.nonce_manager.nonce == {7: 10, 8: 20}
    asyncio.run(scenario())


def test_completion_preflight_is_rechecked_after_nonce_becomes_cold(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        ticket = venue.reserve_completion()
        signer.nonce_manager.nonce.clear()
        token = await venue.prepare_dispatch(order_ref='101', completion_ticket=ticket)
        assert token.ticket.remaining == 2 and signer.orders == rest.calls == []
        token.release()
        assert budget.snapshot()['used'] == 0 and not venue._submission_lock.locked()
    asyncio.run(scenario())


def test_nonce_change_cannot_overdraw_completion_or_leak_owner(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        budget.reserve(47)
        ticket = venue.reserve_completion()
        signer.nonce_manager.nonce.clear()
        with pytest.raises(RequestDeferred):
            await venue.prepare_dispatch(order_ref='101', completion_ticket=ticket)
        assert budget.snapshot()['used'] == 47 and ticket.remaining == 0
        assert not venue._submission_lock.locked() and signer.orders == rest.calls == []
    asyncio.run(scenario())


def test_expired_ordinary_ticket_does_not_gain_completion_priority(monkeypatch):
    async def scenario():
        venue, _, budget, clock = make_venue(monkeypatch, lambda *args: Response({}))
        ticket = budget.reserve(1, priority=1)
        clock.advance(60)
        budget.reserve(36)
        with pytest.raises(RequestDeferred):
            budget.consume_reserved(ticket, '/api/v1/sendTx')
        assert budget.snapshot()['used'] == 36
        assert budget.snapshot()['endpoint_starts'] == {}
    asyncio.run(scenario())


def test_renew_failure_is_atomic_and_keeps_original_priority(monkeypatch):
    async def scenario():
        venue, _, budget, clock = make_venue(monkeypatch, lambda *args: Response({}))
        ticket = budget.reserve(1, priority=1)
        budget.reserve(35)
        with pytest.raises(RequestDeferred):
            budget.renew_reserved(ticket, 2)
        assert ticket.remaining == 1 and budget.snapshot()['used'] == 36
        assert all(event.priority == 1 for event in ticket.events)
        clock.advance(60)
        budget.renew_reserved(ticket, 2)
        assert ticket.remaining == 2 and budget.snapshot()['used'] == 2
    asyncio.run(scenario())


def test_expired_completion_ticket_rechecks_physical_window(monkeypatch):
    async def scenario():
        venue, _, budget, clock = make_venue(monkeypatch, lambda *args: Response({}))
        install_signer(venue)
        ticket = venue.reserve_completion()
        clock.advance(60)
        budget.reserve(48)
        with pytest.raises(RequestDeferred):
            await venue.prepare_dispatch(order_ref='101', completion_ticket=ticket)
        assert budget.snapshot()['used'] == 48 and ticket.remaining == 0
        assert not venue._submission_lock.locked()
    asyncio.run(scenario())


def test_unused_preparation_releases_on_logical_budget_denial(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        token = await venue.prepare_dispatch(order_ref='101')
        assert venue._submission_lock.locked() and token.ticket.remaining == 1
        token.release()
        token.release()
        assert not venue._submission_lock.locked() and budget.snapshot()['used'] == 0
        assert signer.orders == rest.calls == []
    asyncio.run(scenario())


def test_cancelled_preparation_waiter_cannot_release_existing_owner(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        install_signer(venue)
        one = await venue.prepare_dispatch(order_ref='101')
        two = asyncio.create_task(venue.prepare_dispatch(order_ref='102'))
        await asyncio.sleep(0)
        two.cancel()
        with pytest.raises(asyncio.CancelledError):
            await two
        assert venue._submission_lock.locked() and budget.snapshot()['used'] == 1
        one.release()
        assert not venue._submission_lock.locked() and budget.snapshot()['used'] == 0
    asyncio.run(scenario())


def test_concurrent_submissions_do_not_overlap_nonce_bookkeeping(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        gate, started = asyncio.Event(), asyncio.Event()
        active = 0
        maximum = 0
        async def http(method, path):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            started.set()
            await gate.wait()
            active -= 1
        signer, rest = install_signer(venue, callback=http)
        first = asyncio.create_task(send(venue))
        await asyncio.wait_for(started.wait(), .2)
        second = asyncio.create_task(send(venue, '102'))
        await asyncio.sleep(0)
        assert len(signer.orders) == 1
        gate.set()
        await asyncio.wait_for(asyncio.gather(first, second), .2)
        assert maximum == 1 and len(rest.calls) == 2
        assert budget.snapshot()['reserved_units'] == 0 and not venue._submission_lock.locked()
    asyncio.run(scenario())


def test_ws_confirmation_does_not_release_sdk_owner_early(monkeypatch):
    async def scenario():
        venue, _, _, _ = make_venue(monkeypatch, lambda *args: Response({}), feed=True)
        gate, started = asyncio.Event(), asyncio.Event()
        async def http(method, path):
            started.set()
            await gate.wait()
        _, rest = install_signer(venue, callback=http)
        token = await venue.prepare_dispatch(order_ref='101')
        first = asyncio.create_task(send(venue, dispatch_token=token))
        await asyncio.wait_for(started.wait(), .2)
        venue.orders_feed._resolve(101, {'status': 'filled', 'filled_base': .01, 'avg_px': 100})
        assert not (await asyncio.wait_for(first, .2))['unresolved']
        token.release()
        assert venue._submission_lock.locked()
        second = asyncio.create_task(venue.prepare_dispatch(order_ref='102'))
        await asyncio.sleep(0)
        assert not second.done() and len(rest.calls) == 1
        gate.set()
        next_token = await asyncio.wait_for(second, .2)
        next_token.release()
        assert not venue._submission_lock.locked()
    asyncio.run(scenario())


def test_no_post_guard_defers_without_forcing_warm_nonce_cold(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response(account_payload()))
        snapshot = await venue.fetch_account_snapshot()
        epoch = venue._account_epoch
        signer, rest = install_signer(venue)
        venue.submission_guard = lambda ref: 'exit_requested'
        result = await send(venue)
        assert result['deferred'] and result['retry_after'] == 0
        assert signer.nonce_manager.nonce == {7: 10} and rest.calls == []
        assert budget.snapshot()['used'] == 1 and not venue._submission_lock.locked()
        assert venue._account_epoch == epoch and (await venue.fetch_account_snapshot()) is snapshot
    asyncio.run(scenario())


def test_new_provider_cooldown_between_preparation_and_post_defers_without_io(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        token = await venue.prepare_dispatch(order_ref='101', completion=True)
        budget.defer(429, {'Retry-After': '180'})
        result = await send(venue, dispatch_token=token, completion=True)
        assert result['deferred'] and result['retry_after'] == 180
        assert rest.calls == [] and signer.nonce_manager.nonce == {7: 10}
        assert budget.snapshot()['reserved_units'] == 0 and not venue._submission_lock.locked()
    asyncio.run(scenario())


def test_provider_cooldown_after_nonce_read_is_unsent_but_not_after_post(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        async def http(method, path):
            budget.defer(429, {'Retry-After': '120'})
        signer, rest = install_signer(venue, callback=http)
        signer.nonce_manager.nonce.clear()
        result = await send(venue)
        assert result['deferred'] and result['retry_after'] == 120
        assert rest.calls == [('GET', '/api/v1/nextNonce')]
        assert signer.nonce_manager.nonce == {}
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        async def post(method, path):
            budget.defer(429, {})
            raise RequestDeferred('http_429', 60)
        _, rest = install_signer(venue, callback=post)
        unknown = await send(venue)
        assert unknown['unresolved'] and not unknown.get('deferred')
        assert rest.calls == [('POST', '/api/v1/sendTx')]
    asyncio.run(scenario())


def test_genuine_captcha_has_one_shared_cooldown_not_a_second_account_breaker(monkeypatch):
    async def scenario():
        venue, session, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        budget.defer(405, {'x-amzn-waf-action': 'captcha'})
        with pytest.raises(LighterWafBlocked, match='shared request circuit'):
            venue._account_rest_allowed()
        with pytest.raises(RequestDeferred, match='captcha'):
            await venue.fetch_account_snapshot(force_refresh=True)
        assert session.calls == [] and budget.snapshot()['blocked_for_sec'] == 300
        assert not hasattr(venue, '_account_rest_blocked_until')
    asyncio.run(scenario())


def test_http_captcha_stops_repeat_account_io_in_shared_budget(monkeypatch):
    async def scenario():
        def captcha(*args):
            response = Response({})
            response.status = 405
            response.headers = {'x-amzn-waf-action': 'captcha'}
            return response
        venue, session, budget, _ = make_venue(monkeypatch, captcha)
        with pytest.raises(LighterWafBlocked):
            await venue.fetch_account_snapshot()
        with pytest.raises(RequestDeferred, match='captcha'):
            await venue.fetch_account_snapshot()
        assert len(session.calls) == 1 and budget.snapshot()['blocked_for_sec'] == 300
        assert budget.snapshot()['counters']['captcha'] == 1
    asyncio.run(scenario())


def test_startup_shared_captcha_after_ack_never_replays_or_reads(monkeypatch):
    async def scenario():
        def blocked_after_ack(venue, budget, clock):
            budget.defer(405, {'x-amzn-waf-action': 'captcha'})
        venue, session, budget, _, signer, rest = startup_venue(
            monkeypatch, lambda *args: Response({}), after_submit=blocked_after_ack)
        with pytest.raises(LighterWafBlocked):
            await venue.set_leverage(10)
        assert len(signer.calls) == 1 and len(rest.calls) == 2 and session.calls == []
        assert budget.snapshot()['blocked_for_sec'] == 300
    asyncio.run(scenario())


def test_claimed_completion_cannot_be_refunded_while_nonce_io_is_running(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        gate, started = asyncio.Event(), asyncio.Event()
        async def nonce_io(method, path):
            if method == 'GET':
                started.set()
                await gate.wait()
        signer, _ = install_signer(venue, callback=nonce_io)
        signer.nonce_manager.nonce.clear()
        ticket = venue.reserve_completion()
        submitting = asyncio.create_task(send(venue, completion_ticket=ticket))
        await asyncio.wait_for(started.wait(), .2)
        venue.release_completion(ticket)
        assert ticket.remaining == 1 and budget.snapshot()['reserved_units'] == 1
        gate.set()
        result = await asyncio.wait_for(submitting, .2)
        assert result['unresolved'] and not result.get('deferred')
        assert budget.snapshot()['used'] == 2 and ticket.remaining == 0
    asyncio.run(scenario())


def test_sdk_cancelled_before_first_turn_still_releases_owner(monkeypatch):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, _ = install_signer(venue)
        async def cancel_before_turn(submission, *args):
            submission.cancel()
            await submission
        venue._await_submission = cancel_before_turn
        with pytest.raises(asyncio.CancelledError):
            await send(venue)
        await asyncio.sleep(0)
        assert signer.orders == []
        assert budget.snapshot()['used'] == 0 and not venue._submission_lock.locked()
    asyncio.run(scenario())


def test_actual_adapter_and_broker_pre_admission_do_not_record_logical_attempt(monkeypatch, journal):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        budget.reserve(36)
        result = await executor(journal, venue).send(
            intent(), persist_dispatch=lambda: prepare(journal, intent()))
        assert result.deferred and not result.terminal and result.retry_after == 60
        assert journal.pending() == [] and journal.live_steps('c') == []
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 0
        assert signer.orders == rest.calls == []
    asyncio.run(scenario())


def test_actual_adapter_and_broker_guard_refund_only_unsubmitted_attempt(monkeypatch, journal):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        signer, rest = install_signer(venue)
        venue.submission_guard = lambda ref: 'exit_requested'
        result = await executor(journal, venue).send(
            intent(), persist_dispatch=lambda: prepare(journal, intent()), completion=True)
        assert result.deferred and not result.terminal and result.receipt is None
        assert journal.pending() == [] and journal.live_steps('c') == []
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 0
        assert signer.nonce_manager.nonce == {7: 10} and rest.calls == []
        assert budget.snapshot()['used'] == 0 and not venue._submission_lock.locked()
    asyncio.run(scenario())


def test_actual_adapter_and_broker_post_exception_keeps_unknown_and_logical_attempt(monkeypatch, journal):
    async def scenario():
        venue, _, budget, _ = make_venue(monkeypatch, lambda *args: Response({}))
        async def failed_post(method, path):
            raise RequestDeferred('http_429', 60)
        _, rest = install_signer(venue, callback=failed_post)
        result = await executor(journal, venue).send(
            intent(), persist_dispatch=lambda: prepare(journal, intent()), completion=True)
        assert not result.deferred and not result.terminal
        assert journal.order(intent().order_ref)['state'] == 'UNKNOWN'
        assert len(journal.live_steps('c')) == 1
        assert journal.db.execute('SELECT COUNT(*) FROM request_events').fetchone()[0] == 1
        assert len(rest.calls) == 1 and budget.snapshot()['used'] == 1
    asyncio.run(scenario())


def test_order_facts_include_external_open_and_unknown_ids_but_are_bounded():
    feed = AccountOrdersFeed('offline', 'unused', 139, 42, None)
    feed._handle_orders({'orders': {'139': [
        {'status': 'open', 'client_order_index': 101, 'filled_base_amount': '0'},
        {'status': 'open'},
    ], '140': [{'status': 'open', 'client_order_index': 102}]}})
    assert feed.order_updates[101]['status'] == 'open' and feed.terminal(101) is None
    assert feed.unidentified_order_revision == 1 and 102 not in feed.order_updates
    feed._handle_orders({'orders': {'139': [{
        'status': 'filled', 'client_order_index': 101,
        'filled_base_amount': '.01', 'filled_quote_amount': '1'}]}})
    assert feed.order_updates[101]['filled_base'] == .01
    assert feed.terminal(101)['avg_px'] == 100
    feed._handle_orders({'orders': {'139': [
        {'status': 'open', 'client_order_index': coi} for coi in range(200, 800)]}})
    assert len(feed.order_updates) == 512
