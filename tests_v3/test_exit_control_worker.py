"""Offline checks for strict preview evidence and the lock-held confirmation boundary."""
import asyncio
from copy import deepcopy
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
from types import SimpleNamespace
import time

import pytest

from entropy_arb.process_guard import ProcessGuard, ProcessGuardError
from entropy_arb.v3.core import Settings, Side
from entropy_arb.v3.journal import Intent, Journal, Result
from entropy_arb.v3.live_host import BatchAuthorization
from test_runtime import FakeVenue
from tools import v3_exit_control as worker
from tools import v3_exit_only as cli


ADDRESS = '0x' + 'a' * 40
PAIR = 'default:hl:SNDK:lighter:SNDK'


class FakeSession:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class ReadVenue(FakeVenue):
    def __init__(self, key, kind, conf):
        super().__init__(key, kind, 100.1, 100.2, min_base=.0001)
        self.conf = conf
        self.coin = 'io:SNDK'
        self.market_id = 139
        self.signer_calls = 0
        self.on_read = None
        self.on_book_read = None
        self.book_raw = None
        self.book_requests = []
        self.raw = ({'marginSummary': {'accountValue': '100'}, 'assetPositions': [
            {'position': {'coin': 'io:SNDK', 'szi': '-0.001'}}]} if kind == 'hl' else
            {'code': 200, 'accounts': [{'index': 42, 'total_asset_value': '100',
              'positions': [{'market_id': 139, 'sign': 1, 'position': '0.001'}]}]})
        self.position = -.001 if kind == 'hl' else .001

    def _query_address(self):
        return ADDRESS

    async def _info(self, payload):
        if payload.get('type') == 'l2Book':
            assert payload == {'type': 'l2Book', 'coin': 'io:SNDK'}
            self.book_requests.append(payload)
            if self.on_book_read:
                self.on_book_read()
            return deepcopy(self.book_raw)
        assert payload == {'type': 'clearinghouseState', 'user': ADDRESS, 'dex': 'io'}
        if self.on_read:
            self.on_read()
        return deepcopy(self.raw)

    async def _get(self, path, params):
        if path == '/api/v1/orderBookOrders':
            assert params == {'market_id': 139, 'limit': 250}
            self.book_requests.append(params)
            if self.on_book_read:
                self.on_book_read()
            return deepcopy(self.book_raw)
        assert path == '/api/v1/account' and params == {'by': 'index', 'value': '42'}
        if self.on_read:
            self.on_read()
        return deepcopy(self.raw)

    def init_signer(self):
        self.signer_calls += 1

    async def prepare_signer(self):
        self.signer_calls += 1


@pytest.fixture
def scope(tmp_path, monkeypatch):
    def deny_network(*args, **kwargs):
        raise AssertionError('real network is forbidden')
    monkeypatch.setattr(socket.socket, 'connect', deny_network)
    monkeypatch.setattr('aiohttp.ClientSession', FakeSession)
    files = {}
    for name in ('config', 'env', 'v3_config', 'authorization'):
        files[name] = tmp_path / name
        files[name].write_text('{}')
    args = worker.parser().parse_args([
        '--symbol', 'SNDK', '--hedge', 'lighter', '--live', '--preview-only',
        '--config', str(files['config']), '--env-file', str(files['env']),
        '--v3-config', str(files['v3_config']), '--batch-authorization', str(files['authorization']),
        '--journal', str(tmp_path / 'journal.sqlite3'), '--status-json', str(tmp_path / 'manual-status.json'),
        '--observer-jsonl', str(tmp_path / 'observer.jsonl'),
        '--reference-checkpoint', str(tmp_path / 'reference.json'),
        '--expected-cycle-id', 'cycle', '--expected-hl-account', ADDRESS,
        '--expected-hedge-account-index', '42', '--accounting-wait-seconds', '0'])
    cfg = SimpleNamespace(
        persistence=SimpleNamespace(lock_file=str(tmp_path / 'account.lock')),
        entropy=SimpleNamespace(kind='hl', symbol='SNDK', hl_dex='io',
                                hl_creds=SimpleNamespace(account_address=ADDRESS)),
        hedge=SimpleNamespace(kind='lighter', symbol='SNDK',
                              lighter_creds=SimpleNamespace(account_index=42),
                              lighter_profile=SimpleNamespace(name='mainnet')))
    settings = Settings()
    auth = BatchAuthorization('batch', PAIR, 2, D('3'), D('15'), True)
    account_scope = cli.validate_exit_scope(args, cfg, settings)
    journal = Journal(args.journal, identity=f'{PAIR}:account')
    journal.ensure_batch('batch', max_position_cycles=2, loss_budget_usd=D('3'))
    journal.create_live_cycle(batch_id='batch', cycle_id='cycle', direction='short_entropy',
        planned_units=10, reference_bps=D(0), entry_basis_bps=D(1), started_ts=time.time())
    for venue, side in (('entropy', Side.SELL), ('hedge', Side.BUY)):
        intent = Intent('entry_' + venue, 'cycle', venue, side, 10, D(100))
        journal.prepare(intent)
        journal.record(intent.order_ref, Result(True, 10, D(100), D(0)))
    journal.update_live_cycle('cycle', phase='OPEN', entropy_units=-10, hedge_units=10,
        position_evidence_authoritative=True, last_authoritative_ts=time.time(), updated_ts=time.time())
    journal.close()
    entropy, hedge = ReadVenue('entropy', 'hl', cfg.entropy), ReadVenue('hedge', 'lighter', cfg.hedge)
    monkeypatch.setattr(cli.main_v3, '_make_venues', lambda *a: (entropy, hedge))
    hashes = worker.configuration_hashes(args, cfg, settings, PAIR)
    return SimpleNamespace(args=args, cfg=cfg, settings=settings, pair=PAIR, authorization=auth,
                           account_scope=account_scope, config_hashes=hashes, entropy=entropy, hedge=hedge)


def arguments(scope):
    return tuple(getattr(scope, name) for name in ('args', 'cfg', 'settings', 'pair',
                 'authorization', 'account_scope', 'config_hashes'))


def preview(scope):
    return asyncio.run(worker.preview_exit(*arguments(scope)))


def assert_unarmed(scope):
    with sqlite3.connect(scope.args.journal) as db:
        assert db.execute('SELECT closed_for_entries FROM batches').fetchone()[0] == 0
        assert db.execute('SELECT phase FROM live_cycles').fetchone()[0] == 'OPEN'
        assert not db.execute("SELECT 1 FROM metadata WHERE key LIKE 'manual_exit:%'").fetchone()
    assert scope.entropy.signer_calls == scope.hedge.signer_calls == 0
    assert scope.entropy.sent == scope.hedge.sent == []
    assert not Path(scope.args.status_json).exists()


def test_preview_leaves_ledger_and_signers_untouched(scope):
    before = hashlib.sha256(Path(scope.args.journal).read_bytes()).hexdigest()
    result = preview(scope)
    assert result['preview']['authoritative']
    assert result['receipt']['entropy_units'] == -10 and result['receipt']['hedge_units'] == 10
    assert result['receipt']['quantity_step'] == '0.0001'
    assert result['preview']['executable_gross_usd'] is None
    assert result['preview']['fees_usd'] is None and result['preview']['funding_usd'] is None
    assert result['receipt']['close_fills_baseline'] == {'entropy': {}, 'hedge': {}}
    assert 'close_fills_baseline' not in result['preview']
    assert ADDRESS not in json.dumps(result)
    assert hashlib.sha256(Path(scope.args.journal).read_bytes()).hexdigest() == before
    assert_unarmed(scope)


def test_busy_owner_refuses_before_any_adapter(scope, monkeypatch):
    monkeypatch.setattr(cli.main_v3, '_make_venues', lambda *a: pytest.fail('adapter constructed'))
    with ProcessGuard(scope.cfg.persistence.lock_file), pytest.raises(ProcessGuardError):
        preview(scope)
    assert_unarmed(scope)


@pytest.mark.parametrize('venue,raw,code', [
    ('entropy', {'marginSummary': {'accountValue': '100'}}, 'hl_account_shape_invalid'),
    ('entropy', {'marginSummary': {'accountValue': '100'}, 'assetPositions': [
        {'position': {'coin': 'io:SNDK'}}]}, 'hl_position_invalid'),
    ('entropy', {'marginSummary': {'accountValue': '100'}, 'assetPositions': [
        {'position': {'coin': 'io:SNDK', 'szi': 'NaN'}}]}, 'hl_position_invalid'),
    ('hedge', {'code': 200, 'accounts': [{'index': 41}]}, 'lighter_account_identity_mismatch'),
    ('hedge', {'code': 200, 'accounts': [{'index': 42, 'total_asset_value': '100'}]}, 'lighter_positions_missing'),
    ('hedge', {'code': 200, 'accounts': [{'index': 42, 'total_asset_value': '100',
        'positions': [{'market_id': 139, 'position': '0.001'}]}]}, 'lighter_position_invalid'),
    ('hedge', {'code': 200, 'accounts': [{'index': 42, 'total_asset_value': '100',
        'positions': [{'market_id': 139, 'position': '0.00001', 'sign': 1}]}]}, 'position_off_common_grid'),
])
def test_malformed_response_never_means_flat(scope, venue, raw, code):
    getattr(scope, venue).raw = raw
    with pytest.raises(worker.ControlRefusal, match=code):
        preview(scope)
    assert_unarmed(scope)


def test_valid_empty_account_is_not_substituted_for_nonzero_cycle(scope):
    scope.entropy.raw['assetPositions'] = []
    with pytest.raises(worker.ControlRefusal, match='account_cycle_quantity_mismatch'):
        preview(scope)
    assert_unarmed(scope)


def test_cycle_identity_checked_before_account_io(scope):
    scope.args.expected_cycle_id = 'another'
    scope.entropy.on_read = lambda: pytest.fail('account read')
    with pytest.raises(worker.ControlRefusal, match='cycle_changed'):
        preview(scope)
    assert_unarmed(scope)


def test_change_during_account_io_invalidates_receipt(scope):
    def change():
        with sqlite3.connect(scope.args.journal) as db:
            db.execute("UPDATE orders SET fee_usd='0.1' WHERE venue='entropy'")
    scope.entropy.on_read = change
    with pytest.raises(worker.ControlRefusal, match='journal_changed_during_preview'):
        preview(scope)
    assert_unarmed(scope)


@pytest.mark.parametrize('mutation', ['expired', 'quantity', 'orders', 'config', 'effective_config', 'binding', 'baseline'])
def test_confirmation_changed_refuses_under_shared_lock_before_arm(scope, mutation):
    receipt = preview(scope)['receipt']
    if mutation == 'expired':
        receipt['observed_at'] -= 30
        receipt['expires_at'] -= 30
    elif mutation == 'quantity':
        scope.entropy.raw['assetPositions'][0]['position']['szi'] = '-0.0009'
    elif mutation == 'orders':
        with sqlite3.connect(scope.args.journal) as db:
            db.execute("UPDATE orders SET fee_usd='0.1' WHERE venue='entropy'")
    elif mutation == 'config':
        Path(scope.args.v3_config).write_text('{"changed":true}')
    elif mutation == 'effective_config':
        scope.cfg.hedge.lighter_creds.api_key_index = 7
    elif mutation == 'baseline':
        receipt['close_fills_baseline']['entropy']['invented'] = 1
    else:
        receipt['account_scope_hash'] = 'different'
    check = worker.confirmation_check(receipt, *arguments(scope))
    with pytest.raises(worker.ControlRefusal):
        asyncio.run(cli.run_live_exit(*arguments(scope)[:-1], pre_arm_check=check))
    assert_unarmed(scope)


def test_cli_hook_runs_while_owned_and_before_latch(scope):
    async def stop_before_arm(journal, target):
        assert target == 'cycle' and not journal.batch_state('batch')['closed_for_entries']
        with pytest.raises(ProcessGuardError):
            with ProcessGuard(scope.cfg.persistence.lock_file):
                pass
        raise worker.ControlRefusal('test_stop')
    with pytest.raises(worker.ControlRefusal, match='test_stop'):
        asyncio.run(cli.run_live_exit(*arguments(scope)[:-1], pre_arm_check=stop_before_arm))
    assert_unarmed(scope)


@pytest.mark.parametrize('unknown_once', [False, True])
def test_confirmed_exit_reuses_real_cli_with_only_fake_reduce_only_orders(scope, unknown_once):
    scope.entropy.unknown_once = unknown_once
    receipt = preview(scope)['receipt']
    check = worker.confirmation_check(receipt, *arguments(scope))
    result = asyncio.run(cli.run_live_exit(*arguments(scope)[:-1], pre_arm_check=check))
    assert result['success'] and result['remaining']['authoritative']
    assert result['remaining']['entropy_units'] == result['remaining']['hedge_units'] == 0
    assert result['entries_remain_closed'] and not result['pending']
    assert scope.entropy.sent and scope.hedge.sent
    assert all(order['reduce_only'] for order in scope.entropy.sent + scope.hedge.sent)
    assert len(scope.entropy.sent) == len(scope.hedge.sent) == 1
    assert worker._safe_outcome(result, 'cycle')['active_cycle'] is None
    result['active_cycle'] = {'cycle_id': 'cycle', 'private_field': 'PRIVATE'}
    safe = worker._safe_outcome(result, 'cycle')
    assert safe['active_cycle'] == {'cycle_id': 'cycle'}
    assert 'PRIVATE' not in json.dumps(safe)
    result['active_cycle'] = {'cycle_id': 'another'}
    with pytest.raises(worker.ControlRefusal, match='execution_cycle_mismatch'):
        worker._safe_outcome(result, 'cycle')
    result.pop('active_cycle')
    with pytest.raises(worker.ControlRefusal, match='execution_evidence_missing'):
        worker._safe_outcome(result, 'cycle')


def test_pending_open_unknown_refuses_without_recovery_or_new_order(scope):
    with sqlite3.connect(scope.args.journal) as db:
        db.execute("UPDATE orders SET state='UNKNOWN' WHERE venue='entropy'")
    with pytest.raises(worker.ControlRefusal, match='pending_open_requires_reconciliation'):
        preview(scope)
    assert_unarmed(scope)


def test_stale_account_response_refuses_without_receipt(scope, monkeypatch):
    clock = SimpleNamespace(now=time.time())
    monkeypatch.setattr(worker, 'time', SimpleNamespace(time=lambda: clock.now))
    scope.entropy.on_read = lambda: setattr(clock, 'now', clock.now + 6)
    with pytest.raises(worker.ControlRefusal, match='account_evidence_stale'):
        preview(scope)
    assert_unarmed(scope)


def test_close_fill_baseline_uses_unique_ids_and_unknown_cumulative_snapshot(scope):
    def record_entropy(units):
        journal = Journal(scope.args.journal, identity=f'{PAIR}:account')
        try:
            journal.record('close_entropy', Result(False, units, D(100), D(0)))
        finally:
            journal.close()

    journal = Journal(scope.args.journal, identity=f'{PAIR}:account')
    try:
        for venue, side in (('entropy', Side.BUY), ('hedge', Side.SELL)):
            close = Intent('close_' + venue, 'cycle', venue, side, 10, D(100), True)
            journal.prepare(close)
            journal.record(close.order_ref, Result(venue == 'hedge', 3 if venue == 'entropy' else 4,
                                                   D(100), D(0)))
    finally:
        journal.close()
    scope.entropy.raw['assetPositions'][0]['position']['szi'] = '-0.0007'
    scope.hedge.raw['accounts'][0]['positions'][0]['position'] = '0.0006'
    record_entropy(3)
    record_entropy(3)
    first = preview(scope)['receipt']
    assert first['close_fills_baseline'] == {
        'entropy': {'close_entropy': 3}, 'hedge': {'close_hedge': 4}}
    record_entropy(5)
    record_entropy(5)
    scope.entropy.raw['assetPositions'][0]['position']['szi'] = '-0.0005'
    second = preview(scope)['receipt']
    assert second['close_fills_baseline'] == {
        'entropy': {'close_entropy': 5}, 'hedge': {'close_hedge': 4}}
    assert second['orders_watermark'] != first['orders_watermark']
    assert len(second['close_fills_baseline']['entropy']) == 1
    assert second['close_fills_baseline']['entropy']['close_entropy'] - first[
        'close_fills_baseline']['entropy']['close_entropy'] == 2


def set_quote_books(scope):
    scope.entropy.book_raw = {'coin': 'io:SNDK', 'levels': [
        [{'px': '103', 'sz': '0.01'}],
        [{'px': '104', 'sz': '0.0004'}, {'px': '106', 'sz': '0.0006'}]]}
    scope.hedge.book_raw = {'code': 200, 'market_id': 139,
        'bids': [{'price': '110', 'remaining_base_amount': '0.0002'},
                 {'price': '108', 'remaining_base_amount': '0.0008'}],
        'asks': [{'price': '111', 'remaining_base_amount': '0.01'}]}


@pytest.mark.parametrize('hedge_units,expected', [(10, D('0.0032')), (5, D('-0.0008'))])
def test_sndk_full_depth_estimate_uses_actual_signed_quantities_and_excludes_fees(scope, hedge_units, expected):
    set_quote_books(scope)
    with sqlite3.connect(scope.args.journal) as db:
        db.execute("UPDATE orders SET fee_usd='1000'")
        db.execute("UPDATE orders SET filled_units=? WHERE venue='hedge'", (hedge_units,))
        db.execute("UPDATE cycle_economics SET entropy_funding_usd='-100',hedge_funding_usd='-100'")
    scope.hedge.raw['accounts'][0]['positions'][0]['position'] = str(D(hedge_units) * D('.0001'))
    result = preview(scope)
    assert D(result['preview']['executable_gross_usd']) == expected
    assert result['preview']['valuation_ts']
    assert result['preview']['hedge_units'] == hedge_units
    assert result['preview']['fees_usd'] is None and result['preview']['funding_usd'] is None
    assert 'net_pnl_usd' not in result['preview']
    assert 'executable_gross_usd' not in result['receipt'] and 'valuation_ts' not in result['receipt']
    assert scope.entropy.book_requests == [{'type': 'l2Book', 'coin': 'io:SNDK'}]
    assert scope.hedge.book_requests == [{'market_id': 139, 'limit': 250}]
    assert_unarmed(scope)


@pytest.mark.parametrize('problem,reason', [
    ('insufficient', 'insufficient_exit_depth'), ('missing_price', 'fill_price_missing_or_invalid'),
    ('unknown', 'pending_orders'), ('wrong_coin', 'book_identity_invalid'),
    ('crossed', 'book_crossed'), ('nan_price', 'book_level_invalid'),
    ('overflow', 'valuation_arithmetic_invalid'),
    ('transport', 'book_request_unavailable'),
])
def test_unknown_or_invalid_valuation_stays_null_and_keeps_account_preview(scope, problem, reason):
    set_quote_books(scope)
    if problem == 'insufficient':
        scope.hedge.book_raw['bids'] = [{'price': '110', 'remaining_base_amount': '0.0001'}]
    elif problem == 'missing_price':
        with sqlite3.connect(scope.args.journal) as db:
            db.execute("UPDATE orders SET avg_px=NULL WHERE venue='entropy'")
    elif problem == 'unknown':
        journal = Journal(scope.args.journal, identity=f'{PAIR}:account')
        try:
            close = Intent('close_unknown', 'cycle', 'entropy', Side.BUY, 10, D(100), True)
            journal.prepare(close)
            journal.record(close.order_ref, Result(False, 0))
        finally:
            journal.close()
    elif problem == 'wrong_coin':
        scope.entropy.book_raw['coin'] = 'io:NBIS'
    elif problem == 'crossed':
        scope.entropy.book_raw['levels'][0][0]['px'] = '107'
    elif problem == 'nan_price':
        scope.entropy.book_raw['levels'][0][0]['px'] = 'NaN'
    elif problem == 'overflow':
        scope.entropy.book_raw['levels'][1][0]['px'] = '1E9999999'
    else:
        def unavailable():
            raise RuntimeError('PRIVATE URL AND KEY')
        scope.entropy.on_book_read = unavailable
    result = preview(scope)
    assert result['preview']['executable_gross_usd'] is None
    assert result['preview']['valuation_ts'] is None
    assert result['preview']['valuation_reason'] == reason
    assert result['preview']['authoritative']
    assert 'PRIVATE' not in json.dumps(result)
    assert_unarmed(scope)


def test_confirmation_rechecks_quantities_and_receipt_without_book_requests(scope):
    set_quote_books(scope)
    receipt = preview(scope)['receipt']
    scope.entropy.on_book_read = scope.hedge.on_book_read = lambda: pytest.fail('confirmation requested quote')
    check = worker.confirmation_check(receipt, *arguments(scope))
    with ProcessGuard(scope.cfg.persistence.lock_file):
        asyncio.run(check(None, 'cycle'))
    assert len(scope.entropy.book_requests) == len(scope.hedge.book_requests) == 1
    assert_unarmed(scope)


def test_quote_timeout_does_not_block_fresh_account_preview(scope, monkeypatch):
    set_quote_books(scope)
    original = scope.entropy._info
    async def stalled_book(payload):
        if payload['type'] == 'l2Book':
            await asyncio.Event().wait()
        return await original(payload)
    scope.entropy._info = stalled_book
    monkeypatch.setattr(worker, 'QUOTE_TIMEOUT_SECONDS', .01)
    result = preview(scope)
    assert result['preview']['authoritative']
    assert result['preview']['executable_gross_usd'] is None
    assert result['preview']['valuation_reason'] == 'book_request_unavailable'
    assert_unarmed(scope)


def test_book_response_older_than_request_window_is_not_estimated(scope, monkeypatch):
    set_quote_books(scope)
    clock = SimpleNamespace(now=time.time())
    monkeypatch.setattr(worker, 'time', SimpleNamespace(time=lambda: clock.now))
    scope.entropy.on_book_read = lambda: setattr(clock, 'now', clock.now + 6)
    quote = asyncio.run(worker._read_quote_books(scope.entropy, scope.hedge))
    assert quote['books'] is None and quote['reason'] == 'book_evidence_stale'


def test_private_receipt_file_rejects_public_file_and_symlink(tmp_path):
    file = tmp_path / 'receipt.json'
    file.write_text('{}')
    file.chmod(0o644)
    with pytest.raises(worker.ControlRefusal, match='confirmation_file_invalid'):
        worker._read_confirmation(file)
    file.chmod(0o600)
    assert worker._read_confirmation(file) == {}
    link = tmp_path / 'link'
    link.symlink_to(file)
    with pytest.raises(OSError):
        worker._read_confirmation(link)


def test_stdout_error_is_fixed_code_and_drops_exception_secret(monkeypatch, capsys):
    monkeypatch.setattr(worker, 'parser', lambda: SimpleNamespace(parse_args=lambda: None))
    async def failure(args):
        print('PRIVATE KEY AND URL')
        raise RuntimeError('PRIVATE KEY AND URL')
    monkeypatch.setattr(worker, 'amain', failure)
    with pytest.raises(SystemExit) as exc:
        worker.main()
    assert exc.value.code == 2
    assert json.loads(capsys.readouterr().out) == {
        'success': False, 'outcome': 'refused', 'reason': 'exit_worker_refused'}
