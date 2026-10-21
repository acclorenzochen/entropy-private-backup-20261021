import asyncio
import json
import stat
from types import SimpleNamespace

from entropy_arb.config import WeChatClawBotConfig
from entropy_arb.trade_ledger import TradeLedger
from entropy_arb.wechat_clawbot import (
    WeChatClawBotClient,
    build_hourly_report,
    load_clawbot_credentials,
    save_clawbot_credentials,
    setup_clawbot_qr,
)


def test_send_text_uses_ilink_message_shape(tmp_path):
    config = WeChatClawBotConfig(
        enabled=True, state_file=str(tmp_path / "wechat-state.json"))
    client = WeChatClawBotClient(
        SimpleNamespace(), config, token="secret-token",
        target_user_id="user-1", context_token="context-1")
    calls = []

    async def fake_post(endpoint, payload, *, timeout_sec):
        calls.append((endpoint, payload, timeout_sec))
        return {}

    client._post = fake_post
    assert asyncio.run(client.send_text("hello")) is True
    endpoint, payload, timeout = calls[0]
    assert endpoint == "sendmessage"
    assert timeout == config.request_timeout_sec
    assert payload["base_info"]["channel_version"] == "1.0.3"
    msg = payload["msg"]
    assert msg["to_user_id"] == "user-1"
    assert msg["context_token"] == "context-1"
    assert msg["message_type"] == 2
    assert msg["message_state"] == 2
    assert msg["item_list"][0]["text_item"]["text"] == "hello"


def test_poll_once_learns_and_persists_inbound_context(tmp_path):
    config = WeChatClawBotConfig(
        enabled=True, state_file=str(tmp_path / "wechat-state.json"))
    client = WeChatClawBotClient(
        SimpleNamespace(), config, token="secret-token")

    async def fake_post(endpoint, payload, *, timeout_sec):
        assert endpoint == "getupdates"
        assert payload == {"get_updates_buf": ""}
        return {
            "get_updates_buf": "cursor-2",
            "msgs": [{
                "from_user_id": "user-2",
                "context_token": "context-2",
                "message_type": 1,
            }],
        }

    client._post = fake_post
    messages = asyncio.run(client.poll_once())
    assert len(messages) == 1
    assert client.target_user_id == "user-2"
    assert client.context_token == "context-2"
    assert client.cursor == "cursor-2"
    state = (tmp_path / "wechat-state.json").read_text()
    assert "secret-token" not in state
    assert "context-2" in state


def test_learned_target_rejects_context_from_another_sender(tmp_path):
    config = WeChatClawBotConfig(
        enabled=True, state_file=str(tmp_path / "wechat-state.json"))
    client = WeChatClawBotClient(
        SimpleNamespace(), config, token="secret-token",
        target_user_id="user-1", context_token="context-1")

    async def fake_post(endpoint, payload, *, timeout_sec):
        return {
            "get_updates_buf": "cursor-3",
            "msgs": [{
                "from_user_id": "other-user",
                "context_token": "attacker-context",
                "message_type": 1,
            }],
        }

    client._post = fake_post
    asyncio.run(client.poll_once())
    assert client.target_user_id == "user-1"
    assert client.context_token == "context-1"


def test_qr_credentials_are_loaded_separately_with_private_permissions(tmp_path,
                                                                         monkeypatch):
    monkeypatch.delenv("WECHAT_CLAWBOT_TOKEN", raising=False)
    credentials = tmp_path / "wechat-credentials.json"
    save_clawbot_credentials(
        credentials,
        bot_token="Bearer qr-token",
        base_url="https://redirect.example/",
        bot_id="bot-1",
        user_id="user-1",
    )
    config = WeChatClawBotConfig(
        enabled=True,
        state_file=str(tmp_path / "wechat-state.json"),
        credentials_file=str(credentials),
    )
    client = WeChatClawBotClient(SimpleNamespace(), config)
    assert client.configured
    assert client.token == "qr-token"
    assert client.base_url == "https://redirect.example"
    assert load_clawbot_credentials(credentials)["bot_id"] == "bot-1"
    assert stat.S_IMODE(credentials.stat().st_mode) == 0o600


class _FakeHttpResponse:
    def __init__(self, payload, status=200):
        self.status = status
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return json.dumps(self.payload)


class _FakeQrSession:
    def __init__(self):
        self.posts = []
        self.gets = []
        self.statuses = iter([
            {"status": "wait"},
            {
                "status": "confirmed",
                "bot_token": "Bearer confirmed-token",
                "ilink_bot_id": "bot-2",
                "ilink_user_id": "user-2",
                "baseurl": "https://api-2.example/",
            },
        ])

    def post(self, url, *, data, headers, timeout):
        self.posts.append((url, json.loads(data), headers, timeout))
        return _FakeHttpResponse({
            "ret": 0,
            "qrcode": "qr-key-1",
            "qrcode_img_content": "https://liteapp.weixin.qq.com/q/qr-key-1",
        })

    def get(self, url, *, headers, timeout):
        self.gets.append((url, headers, timeout))
        return _FakeHttpResponse(next(self.statuses))


def test_qr_setup_polls_and_saves_confirmed_session(tmp_path, monkeypatch):
    monkeypatch.delenv("WECHAT_CLAWBOT_TOKEN", raising=False)
    credentials = tmp_path / "wechat-credentials.json"
    config = WeChatClawBotConfig(
        enabled=True,
        base_url="https://ilink.example",
        state_file=str(tmp_path / "wechat-state.json"),
        credentials_file=str(credentials),
    )
    session = _FakeQrSession()
    metadata = asyncio.run(setup_clawbot_qr(
        config,
        open_browser=False,
        timeout_sec=3,
        max_refreshes=1,
        session=session,
    ))
    assert metadata["bot_id"] == "bot-2"
    stored = load_clawbot_credentials(credentials)
    assert stored["bot_token"] == "confirmed-token"
    assert stored["base_url"] == "https://api-2.example"
    assert session.posts[0][1] == {"local_token_list": []}
    assert "qrcode=qr-key-1" in session.gets[0][0]
    assert session.gets[0][1]["iLink-App-ClientVersion"] == "1"


def test_hourly_report_contains_status_pnl_and_entropy_volume(tmp_path):
    ledger = TradeLedger(":memory:")
    try:
        ledger.record_fill(
            fill_key="report-fill", execution_id="report-execution",
            leg="first", venue="entropy", order_ref="e1", ts=1000.0,
            side="sell", qty=.04, price=1500.0)
        ledger.record_trade_attempt({
            "trade_key": "report-execution",
            "execution_id": "report-execution", "ts": 1000.0,
            "action": "open", "direction": "sell_entropy", "qty": .04,
            "buy_fill": 0.0, "sell_fill": .04,
            "buy_venue": "hedge", "sell_venue": "entropy",
            "outcome": "one_leg"})
        engine = SimpleNamespace(
            cfg=SimpleNamespace(symbol="SNDK", dashboard_day_timezone="UTC"),
            ledger=ledger,
            pnl_tracker=SimpleNamespace(snapshot=lambda: {
                "net_pnl_usd": -0.12, "session_pnl_usd": -0.12,
                "day_started_ts": 0.0, "session_started_ts": 0.0,
            }),
            halted=False, record_only=False, strategy_name="legacy",
            start_ts=0.0, venues={
                "entropy": SimpleNamespace(
                    key="entropy", name="ENTROPY", position=-.04,
                    book=SimpleNamespace(mid=lambda: 1500.0)),
                "hedge": SimpleNamespace(
                    key="hedge", name="LIGHTER", position=0.04,
                    book=SimpleNamespace(mid=lambda: 1500.0)),
            },
            active_execution=None,
            reconciliation={"status": "synced"},
            _pair_state=lambda: SimpleNamespace(value="flat"),
            premium_bps=lambda: 0.25,
        )
        report = build_hourly_report(engine, now=2000.0)
        assert "状态：运行中" in report
        assert "账户权益盈亏：$-0.1200" in report
        assert "Entropy 累计成交量" in report
        assert "累计执行笔数：1" in report
        assert "账本历史累计：1" in report
        assert "其中单腿" in report
    finally:
        ledger.close()
