import asyncio
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal, DecimalException
import json
from pathlib import Path
import time

import pytest

import main_v3
from entropy_arb.config import HL_API_URL, LIGHTER_PROFILES, load_config
from entropy_arb.funding import FundingSnapshot
from entropy_arb.venue_hl import HLVenue
from entropy_arb.venue_lighter import LighterVenue
from entropy_arb.v3.core import Settings
from entropy_arb.v3.journal import Journal
from entropy_arb.v3.live_host import BatchAuthorization, LiveAuthorizationError


ROOT = Path(__file__).resolve().parents[1]
PAIR = 'nbis-funding:hl:NBIS:lighter:NBIS'


def launch_args(*extra, symbol='NBIS', hedge='lighter'):
    return main_v3.parser().parse_args([
        '--symbol', symbol, '--hedge', hedge,
        '--config', str(ROOT / 'config.nbis.yaml'),
        '--v3-config', str(ROOT / 'config.v3.nbis-funding.json'),
        '--env-file', '/dev/null', '--deployment', 'nbis-funding', *extra])


def configuration(args=None):
    args = args or launch_args()
    return load_config(args.config, args.env_file, symbol=args.symbol, hedge_venue=args.hedge)


def settings():
    return main_v3.load_settings(ROOT / 'config.v3.nbis-funding.json')


@pytest.fixture
def public_payloads():
    market = {'symbol': 'NBIS', 'market_id': 189, 'status': 'active',
              'market_type': 'perp', 'supported_price_decimals': 2,
              'supported_size_decimals': 2, 'supported_quote_decimals': 6,
              'min_base_amount': '0.01', 'min_quote_amount': '10'}
    asset = {'name': 'io:NBIS', 'szDecimals': 2, 'maxLeverage': 20}
    return {
        'perpDexs': [None, {'name': 'io'}],
        'meta': {'universe': [asset]},
        'metaAndAssetCtxs': [{'universe': [asset]}, [{'funding': '0.0001'}]],
        'l2Book': {'coin': 'io:NBIS', 'levels': [
            [{'px': '150', 'sz': '10'}], [{'px': '151', 'sz': '10'}]]},
        'orderBooks': {'code': 200, 'order_books': [market]},
        'orderBookDetails': {'code': 200, 'order_book_details': [
            {**market, 'min_initial_margin_fraction': 1000}]},
        'funding-rates': {'code': 200, 'funding_rates': [
            {'market_id': 189, 'symbol': 'NBIS', 'exchange': 'lighter', 'rate': '0.0008'}]},
        'orderBookOrders': {'code': 200,
                            'bids': [{'price': '150', 'remaining_base_amount': '10',
                                      'initial_base_amount': '100'}],
                            'asks': [{'price': '151', 'remaining_base_amount': '10',
                                      'initial_base_amount': '100'}]},
    }


@pytest.fixture
def offline_public(monkeypatch, tmp_path, public_payloads):
    """Run real adapter parsing with public HTTP fakes and private-call traps."""
    monkeypatch.chdir(tmp_path)
    requests = []

    def forbidden(*args, **kwargs):
        requests.append(('FORBIDDEN', 'private'))
        raise AssertionError('public preflight touched a private/live/persistent path')

    for venue_cls in (HLVenue, LighterVenue):
        for name in ('init_signer', 'set_leverage', 'send_taker', 'resolve_order',
                     'start_tasks', 'fetch_equity', 'fetch_position',
                     'fetch_funding_settlements', 'fetch_funding_payments',
                     'fetch_funding_risk'):
            monkeypatch.setattr(venue_cls, name, forbidden)
    monkeypatch.setattr(Journal, '__init__', forbidden)
    monkeypatch.setattr(main_v3.ProcessGuard, '__init__', forbidden)
    for name in ('run_live', 'run_live_recovery_only', 'run_record_only',
                 'publish_effective_manifest', 'ReadOnlyObserver'):
        monkeypatch.setattr(main_v3, name, forbidden)

    class Response:
        def __init__(self, payload):
            self.payload = payload
            self.status = 200
            self.headers = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def raise_for_status(self):
            pass

        async def json(self):
            return deepcopy(self.payload)

    class Session:
        def __init__(self, *, connector):
            assert connector._family == main_v3.socket.AF_INET
            self.connector = connector

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            await self.connector.close()
            return False

        def post(self, url, *, json, **kwargs):
            assert url == HL_API_URL + '/info'
            assert json['type'] in {'perpDexs', 'meta', 'metaAndAssetCtxs', 'l2Book'}
            if json['type'] == 'l2Book':
                assert json['coin'] == 'io:NBIS'
            if 'dex' in json:
                assert json['dex'] == 'io'
            requests.append(('POST', json['type']))
            return Response(public_payloads[json['type']])

        def get(self, url, *, params=None, headers=None, **kwargs):
            base = LIGHTER_PROFILES['lighter'].api_url + '/api/v1/'
            assert url.startswith(base)
            endpoint = url.removeprefix(base)
            assert endpoint in {'orderBooks', 'funding-rates', 'orderBookDetails', 'orderBookOrders'}
            assert not headers  # No auth token or private request is allowed.
            if endpoint == 'orderBookOrders':
                assert params == {'market_id': 189, 'limit': 250}
            if endpoint == 'orderBookDetails':
                assert params == {'market_id': 189}
            requests.append(('GET', endpoint))
            return Response(public_payloads[endpoint])

    import aiohttp
    monkeypatch.setattr(aiohttp, 'ClientSession', Session)
    yield requests
    assert not any(method == 'FORBIDDEN' for method, endpoint in requests)


def test_minimal_templates_load_and_match_real_authorization_format():
    cfg, policy = configuration(), settings()
    args = launch_args('--public-preflight-only')
    pair, authorization = main_v3.validate_launch(args, cfg, policy)
    assert pair == PAIR and authorization is None
    assert cfg.persistence.lock_file == 'logs/entropy-arb.lock'
    assert cfg.entropy.cap_usd == cfg.hedge.cap_usd == 200
    assert cfg.entropy.leverage == cfg.hedge.leverage == 7
    assert policy.strategy_mode == 'funding_hedge'
    assert policy.max_notional_usd == Decimal('200')
    expected = {
        'funding_settlements_required': 2, 'funding_poll_seconds': 30,
        'funding_stale_seconds': 90, 'funding_reversal_samples': 3,
        'funding_reversal_seconds': 60, 'funding_remaining_cost_limit_usd': Decimal('0.5'),
        'funding_round_loss_limit_usd': None, 'funding_liquidation_distance': Decimal('0.02'),
        'funding_risk_poll_seconds': 2, 'funding_risk_stale_seconds': 10,
        'funding_accounting_grace_seconds': 120,
    }
    assert {key: getattr(policy, key) for key in expected} == expected
    raw = json.loads((ROOT / 'nbis-funding-authorization.example.json').read_text())
    assert set(raw) == {
        'batch_id', 'pair_identity', 'max_position_cycles',
        'loss_budget_usd', 'max_notional_usd', 'approved_live',
    }
    assert raw['approved_live'] is False
    assert raw['loss_budget_usd'] is None
    assert raw['max_position_cycles'] == 1 and raw['pair_identity'] == PAIR


@pytest.mark.parametrize('flags', [
    ['--live'], ['--record-only'], ['--preflight-only'], ['--recovery-only'],
    ['--reopen-entries'], ['--validate-only'], ['--max-new-cycles', '1'],
    ['--batch-authorization', 'unused.json'],
])
@pytest.mark.parametrize('public_first', [True, False])
def test_public_mode_rejects_live_and_private_flags(flags, public_first):
    combined = ['--public-preflight-only', *flags] if public_first else [*flags, '--public-preflight-only']
    with pytest.raises(SystemExit) as exc:
        launch_args(*combined)
    assert exc.value.code == 2


def test_public_preflight_has_no_private_calls_and_only_writes_own_status(
        offline_public, tmp_path, capsys, monkeypatch):
    def auth_forbidden(*args, **kwargs):
        raise AssertionError('public preflight loaded authorization')

    monkeypatch.setattr(BatchAuthorization, 'load', auth_forbidden)
    lock = tmp_path / 'logs/entropy-arb.lock'
    lock.parent.mkdir()
    lock.write_text('existing-SNDK-owner')
    production_status = tmp_path / 'logs/v3/status.json'
    production_status.parent.mkdir()
    production_status.write_text('existing-SNDK-status')
    args = launch_args('--public-preflight-only')
    args.deployment = 'default'
    assert asyncio.run(main_v3.amain(args)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['ok'] is True
    assert result['pair_identity'] == PAIR
    assert result['common_quantity_step'] == '0.01'
    assert result['live_ready'] is result['account_verified'] is False
    assert result['settlements_verified'] is result['accounting_delay_verified'] is False
    assert all(row['ok'] for row in result['checks'])
    for quote in result['matched_quotes'].values():
        assert Decimal(quote['matched_qty']) == Decimal('1.32')
        assert Decimal(quote['matched_qty']) % Decimal('0.01') == 0
        assert Decimal(quote['entropy_notional_usd']) <= 200
        assert Decimal(quote['lighter_notional_usd']) <= 200
    status = tmp_path / 'logs/v3/nbis/public-preflight-status.json'
    stored = json.loads(status.read_text())
    assert stored['current_state'] == 'PUBLIC_PREFLIGHT_OK'
    assert stored['mode'] == 'public-preflight-only'
    assert list(status.parent.iterdir()) == [status]
    assert lock.read_text() == 'existing-SNDK-owner'
    assert production_status.read_text() == 'existing-SNDK-status'
    assert set(offline_public) == {
        ('POST', 'perpDexs'), ('POST', 'meta'), ('POST', 'metaAndAssetCtxs'),
        ('POST', 'l2Book'), ('GET', 'orderBooks'), ('GET', 'funding-rates'),
        ('GET', 'orderBookOrders'), ('GET', 'orderBookDetails')}


@pytest.mark.parametrize('endpoint,stage', [
    ('perpDexs', 'market_metadata'), ('meta', 'market_metadata'),
    ('orderBooks', 'market_metadata'),
    ('metaAndAssetCtxs', 'funding_and_limits'),
    ('funding-rates', 'funding_and_limits'),
    ('orderBookDetails', 'funding_and_limits'),
    ('l2Book', 'books'), ('orderBookOrders', 'books'),
])
@pytest.mark.parametrize('timeouts', [1, 2, 3])
def test_public_timeouts_retry_bounded_reads_and_report_stage(
        offline_public, monkeypatch, capsys, endpoint, stage, timeouts):
    clock, attempts, delays = [10000.0], [], []
    monkeypatch.setattr(time, 'time', lambda: clock[0])
    original_info, original_get = HLVenue._info, LighterVenue._get
    original_sleep = asyncio.sleep

    async def backoff(seconds):
        delays.append(seconds)
        clock[0] += seconds
        await original_sleep(0)

    def maybe_timeout(name):
        if name == endpoint:
            attempts.append(name)
            if len(attempts) <= timeouts:
                clock[0] += 10
                raise TimeoutError()

    async def info(self, payload):
        maybe_timeout(payload['type'])
        return await original_info(self, payload)

    async def get(self, path, **kwargs):
        maybe_timeout(path.removeprefix('/api/v1/'))
        return await original_get(self, path, **kwargs)

    monkeypatch.setattr(asyncio, 'sleep', backoff)
    monkeypatch.setattr(HLVenue, '_info', info)
    monkeypatch.setattr(LighterVenue, '_get', get)
    accepted = timeouts < 3
    assert asyncio.run(main_v3.amain(launch_args('--public-preflight-only'))) == (0 if accepted else 2)
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result['ok'] is accepted and result['live_ready'] is False
    assert len(attempts) == min(timeouts + 1, 3)
    assert delays == [1] * min(timeouts, 2)
    assert f'public stage={stage}' in captured.err
    if accepted:
        assert all(row['ok'] for row in result['checks'])
        assert all(book['observed_at'] == clock[0] for book in result['books'].values())
        if stage == 'books':
            # Both legs are fetched again, not reused from before the timeout.
            peer = ('GET', 'orderBookOrders') if endpoint == 'l2Book' else ('POST', 'l2Book')
            assert offline_public.count(peer) == timeouts + 1
    else:
        assert result['error'].startswith('TimeoutError:')
        assert f'public stage={stage}, attempt=3/3' in result['error']
        assert endpoint in result['error']


@pytest.mark.parametrize('error', [ValueError('malformed public response'),
                                  RuntimeError('HTTP access denied')])
def test_public_non_timeout_errors_do_not_retry(offline_public, monkeypatch, capsys, error):
    attempts = []

    async def get(self, path, **kwargs):
        attempts.append(path)
        raise error

    monkeypatch.setattr(LighterVenue, '_get', get)
    assert asyncio.run(main_v3.amain(launch_args('--public-preflight-only'))) == 2
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert attempts == ['/api/v1/orderBooks']
    assert result['ok'] is False and result['live_ready'] is False
    assert 'stage=market_metadata, attempt=1/3' in result['error']
    assert 'lighter.orderBooks' in result['error']
    assert not captured.err


def test_public_retries_do_not_refresh_old_funding_timestamps(
        offline_public, monkeypatch, capsys):
    clock, attempts = [10000.0], []
    monkeypatch.setattr(time, 'time', lambda: clock[0])
    original_info, original_sleep = HLVenue._info, asyncio.sleep

    async def info(self, payload):
        if payload['type'] == 'l2Book':
            attempts.append(payload['type'])
            if len(attempts) < 3:
                clock[0] += 50
                raise TimeoutError()
        return await original_info(self, payload)

    async def backoff(seconds):
        clock[0] += seconds
        await original_sleep(0)

    monkeypatch.setattr(HLVenue, '_info', info)
    monkeypatch.setattr(asyncio, 'sleep', backoff)
    assert asyncio.run(main_v3.amain(launch_args('--public-preflight-only'))) == 2
    result = json.loads(capsys.readouterr().out)
    assert len(attempts) == 3 and result['ok'] is False
    for name in ('entropy', 'lighter'):
        assert result['books'][name]['observed_at'] == clock[0]
        assert result['funding'][name]['ts'] == '10000.0'
        assert {'check': f'{name}_public_funding_valid', 'ok': False} in result['checks']


def test_public_stage_cancellation_is_not_retried():
    attempts = []

    async def cancelled():
        attempts.append('cancelled')
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(main_v3._public_preflight_stage('books', [('entropy.l2Book', cancelled)]))
    assert attempts == ['cancelled']


@pytest.mark.parametrize('output', [
    'logs/status.json', 'logs/v3/record-status.json',
    'logs/entropy-arb.lock', 'logs/v3/nbis/status.json',
    'logs/v3/nbis/record-status.json', 'logs/v3/nbis/journal.sqlite3',
])
def test_public_status_cannot_overwrite_operating_files(offline_public, tmp_path, output):
    args = launch_args('--public-preflight-only', '--status-json', output)
    with pytest.raises(ValueError, match='independent NBIS output'):
        asyncio.run(main_v3.amain(args))
    assert not offline_public
    assert not (tmp_path / 'logs').exists()


@pytest.mark.parametrize('case', [
    'market_id', 'coin', 'step', 'entropy_leverage', 'lighter_leverage',
    'missing_leverage', 'nonfinite_funding', 'missing_funding', 'crossed_book',
    'insufficient_depth', 'wrong_details_symbol', 'duplicate_details',
])
def test_public_preflight_fails_closed_on_bad_public_evidence(
        offline_public, public_payloads, tmp_path, capsys, case):
    if case == 'market_id':
        public_payloads['orderBooks']['order_books'][0]['market_id'] = 190
    elif case == 'coin':
        public_payloads['meta']['universe'][0]['name'] = 'NBIS'
    elif case == 'step':
        public_payloads['meta']['universe'][0]['szDecimals'] = 3
        public_payloads['orderBooks']['order_books'][0]['supported_size_decimals'] = 3
        public_payloads['orderBookDetails']['order_book_details'][0]['supported_size_decimals'] = 3
    elif case == 'entropy_leverage':
        public_payloads['meta']['universe'][0]['maxLeverage'] = 6
    elif case == 'lighter_leverage':
        public_payloads['orderBookDetails']['order_book_details'][0]['min_initial_margin_fraction'] = 2000
    elif case == 'missing_leverage':
        del public_payloads['orderBookDetails']['order_book_details'][0]['min_initial_margin_fraction']
    elif case == 'nonfinite_funding':
        public_payloads['funding-rates']['funding_rates'][0]['rate'] = 'NaN'
    elif case == 'missing_funding':
        public_payloads['funding-rates']['funding_rates'] = []
    elif case == 'crossed_book':
        public_payloads['l2Book']['levels'][0][0]['px'] = '160'
    elif case == 'insufficient_depth':
        for side in ('bids', 'asks'):
            public_payloads['orderBookOrders'][side][0]['remaining_base_amount'] = '0.01'
    elif case == 'wrong_details_symbol':
        public_payloads['orderBookDetails']['order_book_details'][0]['symbol'] = 'SNDK'
    elif case == 'duplicate_details':
        public_payloads['orderBookDetails']['order_book_details'] *= 2
    assert asyncio.run(main_v3.amain(launch_args('--public-preflight-only'))) == 2
    result = json.loads(capsys.readouterr().out)
    assert result['ok'] is False and result['live_ready'] is False
    policy_failures = {
        'step': 'common_quantity_step_0.01',
        'entropy_leverage': 'entropy_7x_in_market_range',
        'lighter_leverage': 'lighter_7x_in_market_range',
        'insufficient_depth': 'short_entropy_long_lighter_matched_qty_within_cap',
    }
    if case in policy_failures:
        assert {'check': policy_failures[case], 'ok': False} in result['checks']
    else:
        assert result['error'].startswith(('ValueError:', 'RuntimeError:', 'InvalidOperation:'))
    stored = json.loads((tmp_path / 'logs/v3/nbis/public-preflight-status.json').read_text())
    assert stored['current_state'] == 'PUBLIC_PREFLIGHT_REFUSED'


@pytest.mark.parametrize('rate,age,source,accepted', [
    (0, 0, 'hyperliquid:io', True), (-1, 0, 'hyperliquid:io', True),
    (1, 91, 'hyperliquid:io', False), (1, -10, 'hyperliquid:io', False),
    (1, 0, 'hyperliquid:xyz', False), (float('nan'), 0, 'hyperliquid:io', False),
])
def test_funding_requires_fresh_finite_correct_source(
        offline_public, monkeypatch, capsys, rate, age, source, accepted):
    async def funding(self):
        return FundingSnapshot(rate, time.time() - age, source)

    monkeypatch.setattr(HLVenue, 'fetch_funding', funding)
    assert asyncio.run(main_v3.amain(launch_args('--public-preflight-only'))) == (0 if accepted else 2)
    assert json.loads(capsys.readouterr().out)['ok'] is accepted


@pytest.mark.parametrize('slow_endpoint', ['metaAndAssetCtxs', 'funding-rates', 'orderBookDetails'])
def test_slow_public_metadata_does_not_age_final_book_snapshots(
        offline_public, monkeypatch, capsys, slow_endpoint):
    clock = [10000.0]
    monkeypatch.setattr(time, 'time', lambda: clock[0])
    original_info, original_get = HLVenue._info, LighterVenue._get

    async def info(self, payload):
        result = await original_info(self, payload)
        if payload['type'] == slow_endpoint:
            await asyncio.sleep(0)
            clock[0] += 2.0
        return result

    async def get(self, path, **kwargs):
        result = await original_get(self, path, **kwargs)
        if path == '/api/v1/' + slow_endpoint:
            await asyncio.sleep(0)
            clock[0] += 2.0
        return result

    monkeypatch.setattr(HLVenue, '_info', info)
    monkeypatch.setattr(LighterVenue, '_get', get)
    assert asyncio.run(main_v3.amain(launch_args('--public-preflight-only'))) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['books']['entropy']['observed_at'] == 10002.0
    assert result['books']['lighter']['observed_at'] == 10002.0
    assert {'check': 'entropy_public_book_fresh', 'ok': True} in result['checks']
    assert {'check': 'lighter_public_book_fresh', 'ok': True} in result['checks']
    assert offline_public[-2:] == [('POST', 'l2Book'), ('GET', 'orderBookOrders')]


@pytest.mark.parametrize('slow_endpoint', ['l2Book', 'orderBookOrders'])
@pytest.mark.parametrize('delay,accepted', [(0.8, True), (1.1, False)])
def test_slow_public_book_keeps_request_start_and_one_second_age_limit(
        offline_public, monkeypatch, capsys, slow_endpoint, delay, accepted):
    clock = [10000.0]
    monkeypatch.setattr(time, 'time', lambda: clock[0])
    original_info, original_get = HLVenue._info, LighterVenue._get

    async def info(self, payload):
        result = await original_info(self, payload)
        if payload['type'] == slow_endpoint:
            await asyncio.sleep(0)
            clock[0] += delay
        return result

    async def get(self, path, **kwargs):
        result = await original_get(self, path, **kwargs)
        if path == '/api/v1/' + slow_endpoint:
            await asyncio.sleep(0)
            clock[0] += delay
        return result

    monkeypatch.setattr(HLVenue, '_info', info)
    monkeypatch.setattr(LighterVenue, '_get', get)
    assert asyncio.run(main_v3.amain(launch_args('--public-preflight-only'))) == (0 if accepted else 2)
    result = json.loads(capsys.readouterr().out)
    assert result['ok'] is accepted and result['live_ready'] is False
    for name in ('entropy', 'lighter'):
        assert result['books'][name]['observed_at'] == 10000.0
        assert {'check': f'{name}_public_book_fresh', 'ok': accepted} in result['checks']
    assert settings().max_book_age_seconds == 1.0


@pytest.mark.parametrize('field,value', [
    ('entropy.hl_dex', 'xyz'), ('entropy.kind', 'lighter'), ('hedge.kind', 'hl'),
    ('hedge.symbol', 'SNDK'), ('entropy.leverage', 6), ('hedge.leverage', None),
    ('entropy.cap_usd', 201), ('hedge.cap_usd', 0),
    ('hl_api_url', 'https://api.hyperliquid-testnet.xyz'),
    ('hl_ws_url', 'wss://api.hyperliquid-testnet.xyz/ws'),
])
def test_nbis_launch_rejects_wrong_scope_and_risk_configuration(field, value):
    args = launch_args('--public-preflight-only')
    cfg = configuration(args)
    target = cfg
    pieces = field.split('.')
    for name in pieces[:-1]:
        target = getattr(target, name)
    setattr(target, pieces[-1], value)
    with pytest.raises(ValueError):
        main_v3.validate_launch(args, cfg, settings())


@pytest.mark.parametrize('symbol,hedge', [('SNDK', 'lighter'), ('NBIS', 'lighter-rh'), ('NBIS', 'tradexyz')])
def test_scope_is_checked_before_null_budget_authorization_load(monkeypatch, symbol, hedge):
    args = launch_args('--live', '--batch-authorization', 'unused.json', symbol=symbol, hedge=hedge)

    def forbidden(*args, **kwargs):
        raise AssertionError('out-of-scope launch reached authorization loading')

    monkeypatch.setattr(BatchAuthorization, 'load', forbidden)
    with pytest.raises(ValueError, match='requires Entropy hl io:NBIS'):
        main_v3.validate_launch(args, configuration(args), settings())


def test_nbis_launch_rejects_enlarged_cap_changed_lock_and_basis_public_mode():
    args, cfg = launch_args('--public-preflight-only'), configuration()
    with pytest.raises(ValueError, match='<=200'):
        main_v3.validate_launch(args, cfg, replace(settings(), max_notional_usd=Decimal('201')))
    cfg.persistence = replace(cfg.persistence, lock_file='logs/v3/nbis/isolated.lock')
    with pytest.raises(ValueError, match='shared'):
        main_v3.validate_launch(args, cfg, settings())
    with pytest.raises(ValueError, match='requires strategy_mode funding_hedge'):
        main_v3.validate_launch(args, cfg, Settings())


def write_authorization(tmp_path, **changes):
    raw = json.loads((ROOT / 'nbis-funding-authorization.example.json').read_text())
    raw.update(approved_live=True, batch_id='offline-test-only')
    raw.update(changes)
    path = tmp_path / 'fake-authorization.json'
    path.write_text(json.dumps(raw))
    return path


def test_explicit_null_loss_budget_is_allowed_for_funding_and_basis_launch(tmp_path):
    path = write_authorization(tmp_path)
    args = launch_args('--live', '--batch-authorization', str(path))
    pair, authorization = main_v3.validate_launch(args, configuration(args), settings())
    assert pair == authorization.pair_identity == PAIR
    assert authorization.loss_budget_usd is None
    assert authorization.max_position_cycles == 1
    assert authorization.max_notional_usd == Decimal('200')
    assert authorization.batch_id == 'offline-test-only' and authorization.approved_live is True
    basis_path = write_authorization(tmp_path, max_notional_usd='10')
    args.batch_authorization = str(basis_path)
    pair, basis_authorization = main_v3.validate_launch(args, configuration(args), Settings())
    assert pair == basis_authorization.pair_identity == PAIR
    assert basis_authorization.loss_budget_usd is None
    assert basis_authorization.max_position_cycles == 1
    assert basis_authorization.max_notional_usd == Decimal('10')
    assert basis_authorization.batch_id == 'offline-test-only' and basis_authorization.approved_live is True


@pytest.mark.parametrize('round_limit,budget,accepted', [
    (None, None, True), (None, '100', True), ('3', None, True),
    ('3', '2', True), ('3', '4', False),
])
def test_loss_budget_comparison_only_when_both_limits_are_enabled(
        tmp_path, round_limit, budget, accepted):
    path = write_authorization(tmp_path, loss_budget_usd=budget)
    args = launch_args('--live', '--batch-authorization', str(path))
    policy = replace(settings(), funding_round_loss_limit_usd=(
        Decimal(round_limit) if round_limit is not None else None))
    if accepted:
        _, authorization = main_v3.validate_launch(args, configuration(args), policy)
        assert authorization.loss_budget_usd == (Decimal(budget) if budget is not None else None)
    else:
        with pytest.raises(LiveAuthorizationError, match='funding_round_loss_limit_usd'):
            main_v3.validate_launch(args, configuration(args), policy)


@pytest.mark.parametrize('changes', [
    {'max_position_cycles': 0}, {'max_position_cycles': None},
    {'max_position_cycles': True}, {'max_notional_usd': '201'},
    {'pair_identity': 'nbis-funding:hl:SNDK:lighter:SNDK'}, {'approved_live': False},
])
def test_nbis_authorization_refusals(tmp_path, changes):
    path = write_authorization(tmp_path, **changes)
    args = launch_args('--live', '--batch-authorization', str(path))
    with pytest.raises(LiveAuthorizationError):
        main_v3.validate_launch(args, configuration(args), settings())


def test_nbis_allows_smaller_authorized_cap(tmp_path):
    path = write_authorization(tmp_path, max_notional_usd='50')
    args = launch_args('--live', '--batch-authorization', str(path))
    cfg = configuration(args)
    cfg.entropy.cap_usd = cfg.hedge.cap_usd = 50
    _, authorization = main_v3.validate_launch(
        args, cfg, replace(settings(), max_notional_usd=Decimal('50')))
    assert authorization.max_notional_usd == Decimal('50')


@pytest.mark.parametrize('limit', [1, 2, 6, 100])
def test_nbis_accepts_explicit_multi_round_authorization_without_five_round_cap(tmp_path, limit):
    path = write_authorization(tmp_path, max_position_cycles=limit)
    args = launch_args('--live', '--batch-authorization', str(path))
    _, authorization = main_v3.validate_launch(args, configuration(args), settings())
    assert authorization.max_position_cycles == limit
    assert args.max_new_cycles is None


def test_matched_depth_quote_budgets_each_leg_on_common_grid():
    quote = main_v3._public_matched_quote(
        [(Decimal('100'), Decimal('0.015')), (Decimal('120'), Decimal('10'))],
        [(Decimal('110'), Decimal('0.025')), (Decimal('140'), Decimal('10'))],
        step=Decimal('0.01'), cap=Decimal('200'))
    assert quote['matched_qty'] == Decimal('1.43')
    assert quote['entropy_notional_usd'] == Decimal('171.40')
    assert quote['lighter_notional_usd'] == Decimal('199.60')


def test_lighter_aggregates_remaining_depth_at_equal_prices(
        offline_public, public_payloads, capsys):
    for side in ('bids', 'asks'):
        row = public_payloads['orderBookOrders'][side][0]
        public_payloads['orderBookOrders'][side] = [
            {**row, 'remaining_base_amount': '0.036'},
            {**row, 'remaining_base_amount': '0.036'}]
    assert asyncio.run(main_v3.amain(launch_args('--public-preflight-only'))) == 0
    result = json.loads(capsys.readouterr().out)
    assert all(quote['matched_qty'] == '0.07' for quote in result['matched_quotes'].values())


@pytest.mark.parametrize('entropy_rate,lighter_rate,direction,income', [
    ('0.0002', '0.0008', 'short_entropy_long_lighter', '0.019668'),
    ('0.0001', '0.0016', 'long_entropy_short_lighter', '0.019668'),
    ('0.0001005', '0.0008', 'short_entropy_long_lighter', '-0.00003300'),
    ('0.0001', '0.0008', None, None),
])
def test_funding_direction_and_estimated_income_use_mid_and_actual_notionals(
        offline_public, public_payloads, capsys, entropy_rate, lighter_rate, direction, income):
    public_payloads['metaAndAssetCtxs'][1][0]['funding'] = entropy_rate
    public_payloads['funding-rates']['funding_rates'][0]['rate'] = lighter_rate
    assert asyncio.run(main_v3.amain(launch_args('--public-preflight-only'))) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['funding_direction'] == direction
    assert result['funding_direction_basis'] == 'public_mid_price_estimate'
    assert result['funding_income_evidence'] == 'estimated_not_settled_excludes_trading_costs'
    assert result['settlements_verified'] is False
    if direction is None:
        assert result['estimated_net_funding_usdc_per_hour'] is None
    else:
        # Existing adapters normalize funding through floats before Decimal policy.
        assert abs(Decimal(result['estimated_net_funding_usdc_per_hour'])
                   - Decimal(income)) <= Decimal('1e-16')
        assert result['estimated_net_funding_usdc_per_hour'] == (
            result['matched_quotes'][direction]['estimated_net_funding_usdc_per_hour'])
    assert all('estimated_net_funding_usdc_per_hour' in quote
               for quote in result['matched_quotes'].values())
