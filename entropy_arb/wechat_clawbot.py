"""WeChat ClawBot/iLink transport, QR login, and hourly trading reports.

The notifier is deliberately read-only: it consumes the engine's durable
SQLite ledger and in-memory status, but it has no venue client and no order
method.  Losing the WeChat connection therefore cannot affect trading.

The bot token is read from ``WECHAT_CLAWBOT_TOKEN`` for backward
compatibility, or from the private credentials file produced by the QR login
flow.  The target user and rotating context token are learned from an inbound
ClawBot message (or can be provided through the optional environment
variables).  The bot token is never included in logs or the git-tracked
configuration; it is stored only in the ignored credentials file with mode
0600 after a successful QR login.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode, urlparse
from zoneinfo import ZoneInfo

import aiohttp

from .config import WeChatClawBotConfig

log = logging.getLogger("wechat-clawbot")

DEFAULT_TOKEN_ENV = "WECHAT_CLAWBOT_TOKEN"
DEFAULT_TARGET_ENV = "WECHAT_CLAWBOT_TARGET_USER_ID"
DEFAULT_CONTEXT_ENV = "WECHAT_CLAWBOT_CONTEXT_TOKEN"
DEFAULT_CREDENTIALS_FILE = "logs/wechat_clawbot_credentials.json"
CHANNEL_VERSION = "1.0.3"
MAX_MESSAGE_CHARS = 3800
QR_BOT_TYPE = "3"
QR_STATUS_TIMEOUT_SEC = 35.0
QR_MAX_REFRESHES = 3
PRIVATE_FILE_MODE = 0o600


def _normalize_token(value: Any) -> str:
    """Normalize the token returned by iLink without ever logging it."""
    token = str(value or "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token


def _valid_http_url(value: Any) -> Optional[str]:
    candidate = str(value or "").strip().rstrip("/")
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return candidate


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def load_clawbot_credentials(path: str | Path) -> dict[str, Any]:
    """Read QR-login credentials, returning an empty mapping when absent.

    The file is deliberately separate from the cursor/context state file.
    Keeping the token out of the regular state file makes it harder to copy
    into diagnostics or a dashboard export.  Existing files are tightened to
    mode 0600 on a best-effort basis.
    """
    target = Path(path).expanduser()
    data = _read_json_object(target)
    if data.get("schema_version") != 1:
        return {}
    token = _normalize_token(data.get("bot_token"))
    if not token:
        return {}
    try:
        os.chmod(target, PRIVATE_FILE_MODE)
    except OSError:
        pass
    result: dict[str, Any] = {"bot_token": token}
    for key in ("base_url", "bot_id", "user_id", "saved_ts"):
        if data.get(key) is not None:
            result[key] = data[key]
    return result


def _write_private_json(path: str | Path, payload: dict[str, Any]) -> None:
    """Atomically write a local secret-bearing JSON file with mode 0600."""
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        dir=target.parent, prefix=f".{target.name}.", suffix=".tmp")
    try:
        os.chmod(temporary, PRIVATE_FILE_MODE)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary, target)
        os.chmod(target, PRIVATE_FILE_MODE)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def save_clawbot_credentials(path: str | Path, *, bot_token: str,
                             base_url: str, bot_id: Optional[str] = None,
                             user_id: Optional[str] = None) -> dict[str, Any]:
    """Persist the result of a confirmed QR login and return safe metadata."""
    token = _normalize_token(bot_token)
    if not token:
        raise WeChatClawBotError("QR login did not return a bot token")
    normalized_url = _valid_http_url(base_url)
    if not normalized_url:
        raise WeChatClawBotError("QR login returned an invalid API base URL")
    payload = {
        "schema_version": 1,
        "bot_token": token,
        "base_url": normalized_url,
        "bot_id": str(bot_id or "").strip() or None,
        "user_id": str(user_id or "").strip() or None,
        "saved_ts": time.time(),
    }
    _write_private_json(path, payload)
    return {key: value for key, value in payload.items() if key != "bot_token"}


class WeChatClawBotError(RuntimeError):
    """A transport or protocol error that must not reach the trading loop."""


class WeChatClawBotClient:
    """Small async client for the public iLink Bot HTTP contract."""

    def __init__(self, session: aiohttp.ClientSession,
                 config: WeChatClawBotConfig, *,
                 token: Optional[str] = None,
                 target_user_id: Optional[str] = None,
                 context_token: Optional[str] = None) -> None:
        self.session = session
        self.config = config
        explicit_token = _normalize_token(token)
        environment_token = _normalize_token(os.getenv(DEFAULT_TOKEN_ENV, ""))
        self.credentials_path = Path(
            getattr(config, "credentials_file", DEFAULT_CREDENTIALS_FILE)
        ).expanduser()
        stored_credentials = load_clawbot_credentials(self.credentials_path)
        stored_token = _normalize_token(stored_credentials.get("bot_token"))
        self.token = explicit_token or environment_token or stored_token
        stored_base_url = (_valid_http_url(stored_credentials.get("base_url"))
                           if stored_token and not (explicit_token or environment_token)
                           else None)
        self.base_url = (stored_base_url or config.base_url).rstrip("/")
        explicit_target = (target_user_id if target_user_id is not None else
                           os.getenv(DEFAULT_TARGET_ENV, "")).strip()
        explicit_context = (context_token if context_token is not None else
                            os.getenv(DEFAULT_CONTEXT_ENV, "")).strip()
        self.target_user_id = explicit_target or None
        self.context_token = explicit_context or None
        self.cursor = ""
        self._state_lock = asyncio.Lock()
        self._explicit_target = bool(explicit_target)
        self._explicit_context = bool(explicit_context)
        self._load_state()

    @property
    def configured(self) -> bool:
        return bool(self.token)

    @property
    def ready(self) -> bool:
        return bool(self.token and self.target_user_id and self.context_token)

    @property
    def state_path(self) -> Path:
        return Path(self.config.state_file).expanduser()

    def _load_state(self) -> None:
        try:
            with self.state_path.open(encoding="utf-8") as fh:
                data = json.load(fh)
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return
        if not isinstance(data, dict) or data.get("schema_version") != 1:
            return
        if not self._explicit_target:
            target = str(data.get("target_user_id") or "").strip()
            self.target_user_id = target or None
        if not self._explicit_context:
            context = str(data.get("context_token") or "").strip()
            self.context_token = context or None
        self.cursor = str(data.get("get_updates_buf") or "")

    def _state_payload(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "target_user_id": self.target_user_id,
            "context_token": self.context_token,
            "get_updates_buf": self.cursor,
            "updated_ts": time.time(),
        }

    def _persist_state(self) -> None:
        target = self.state_path
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(
            dir=target.parent, prefix=f".{target.name}.", suffix=".tmp")
        try:
            os.chmod(temporary, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self._state_payload(), fh, ensure_ascii=False,
                          separators=(",", ":"))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(temporary, target)
        except BaseException:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise

    @staticmethod
    def _uin() -> str:
        value = random.SystemRandom().randrange(0, 2 ** 32)
        return base64.b64encode(value.to_bytes(4, "big")).decode("ascii")

    def _headers(self, body: bytes) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "AuthorizationType": "ilink_bot_token",
            "X-WECHAT-UIN": self._uin(),
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def _post(self, endpoint: str, payload: dict[str, Any], *,
                    timeout_sec: float) -> dict[str, Any]:
        if not self.token:
            raise WeChatClawBotError(
                "ClawBot has no local QR credentials or legacy token; "
                "run --wechat-setup first")
        body = json.dumps(payload, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8")
        url = f"{self.base_url}/ilink/bot/{endpoint.lstrip('/')}"
        try:
            async with self.session.post(
                    url, data=body, headers=self._headers(body),
                    timeout=aiohttp.ClientTimeout(total=timeout_sec)) as response:
                raw = await response.text()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise WeChatClawBotError(
                f"{endpoint} request failed: {type(exc).__name__}") from exc
        if response.status >= 400:
            # Do not include response text: an upstream error can contain
            # request metadata that should not enter the trading log.
            raise WeChatClawBotError(
                f"{endpoint} returned HTTP {response.status}")
        if not raw.strip() or raw.strip() == "{}":
            return {}
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise WeChatClawBotError(
                f"{endpoint} returned invalid JSON") from exc
        if not isinstance(result, dict):
            raise WeChatClawBotError(f"{endpoint} returned a non-object")
        ret = result.get("ret")
        if ret not in (None, 0, "0"):
            raise WeChatClawBotError(f"{endpoint} returned ret={ret!r}")
        return result

    async def send_text(self, text: str) -> bool:
        """Send one text message, returning False until a session is ready."""
        if not self.configured:
            log.warning("ClawBot is enabled but no local QR credentials or "
                        "legacy token is available; report skipped")
            return False
        async with self._state_lock:
            target = self.target_user_id
            context = self.context_token
        if not target or not context:
            log.warning("ClawBot session is not ready; send a message to the "
                        "bot once to bind the target and context")
            return False
        message = str(text)
        if len(message) > MAX_MESSAGE_CHARS:
            message = message[:MAX_MESSAGE_CHARS - 24] + "\n…（报告已截断）"
        await self._post(
            "sendmessage",
            {
                "msg": {
                    "from_user_id": "",
                    "to_user_id": target,
                    "client_id": f"entropy-arb-{uuid.uuid4().hex}",
                    "message_type": 2,
                    "message_state": 2,
                    "context_token": context,
                    "item_list": [{
                        "type": 1,
                        "text_item": {"text": message},
                    }],
                },
                "base_info": {"channel_version": CHANNEL_VERSION},
            },
            timeout_sec=self.config.request_timeout_sec,
        )
        return True

    @staticmethod
    def _message_sender(message: dict[str, Any]) -> Optional[str]:
        nested = message.get("msg")
        nested = nested if isinstance(nested, dict) else {}
        for key in ("from_user_id", "user_id", "sender_id"):
            value = message.get(key) or nested.get(key)
            if value:
                return str(value).strip()
        return None

    @staticmethod
    def _message_context(message: dict[str, Any]) -> Optional[str]:
        nested = message.get("msg")
        nested = nested if isinstance(nested, dict) else {}
        for key in ("context_token", "contextToken"):
            value = message.get(key) or nested.get(key)
            if value:
                return str(value).strip()
        return None

    @staticmethod
    def _is_bot_message(message: dict[str, Any]) -> bool:
        nested = message.get("msg")
        nested = nested if isinstance(nested, dict) else {}
        value = message.get("message_type", nested.get("message_type"))
        return str(value).lower() in {"2", "bot"}

    async def poll_once(self) -> list[dict[str, Any]]:
        """Pull updates and learn the target session from an inbound message."""
        result = await self._post(
            "getupdates", {"get_updates_buf": self.cursor},
            timeout_sec=max(self.config.poll_timeout_sec + 5.0,
                            self.config.request_timeout_sec),
        )
        new_cursor = result.get("get_updates_buf")
        messages = result.get("msgs") or []
        changed = (new_cursor is not None
                   and str(new_cursor) != self.cursor)
        if new_cursor is not None:
            self.cursor = str(new_cursor)
        if not isinstance(messages, list):
            messages = []

        async with self._state_lock:
            for raw_message in messages:
                if not isinstance(raw_message, dict):
                    continue
                if self._is_bot_message(raw_message):
                    continue
                sender = self._message_sender(raw_message)
                if self.target_user_id:
                    # When a target is explicitly configured, never let an
                    # unrelated inbound message replace its context.
                    if not sender or sender != self.target_user_id:
                        continue
                elif sender:
                    self.target_user_id = sender
                    changed = True
                context = self._message_context(raw_message)
                # Once the first sender has been learned, subsequent messages
                # from another account must not rotate the context used for
                # hourly reports.
                if context and sender == self.target_user_id:
                    if context != self.context_token:
                        self.context_token = context
                        changed = True
        if changed:
            self._persist_state()
        return messages

    async def run_listener(self, stop: asyncio.Event) -> None:
        """Keep the iLink cursor/context fresh without handling commands."""
        backoff = 2.0
        while not stop.is_set():
            try:
                await self.poll_once()
                backoff = 2.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("ClawBot receive failed: %s; retry in %.0fs",
                            exc, backoff)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2.0, 60.0)


def _qr_headers() -> dict[str, str]:
    """Build the unauthenticated headers used by the QR login endpoints."""
    # Do not add Content-Length manually.  The HTTP client calculates it and
    # newer undici/aiohttp stacks reject conflicting hand-written values.
    return {
        "Content-Type": "application/json",
        "AuthorizationType": "ilink_bot_token",
        "X-WECHAT-UIN": WeChatClawBotClient._uin(),
        "iLink-App-ClientVersion": "1",
    }


async def _decode_qr_response(response: Any, endpoint: str) -> dict[str, Any]:
    """Decode a QR endpoint response without copying its body into errors."""
    status = int(getattr(response, "status", 200))
    raw = await response.text()
    if status >= 400:
        raise WeChatClawBotError(f"{endpoint} returned HTTP {status}")
    if not raw.strip():
        raise WeChatClawBotError(f"{endpoint} returned an empty response")
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise WeChatClawBotError(
            f"{endpoint} returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise WeChatClawBotError(f"{endpoint} returned a non-object")
    ret = result.get("ret")
    errcode = result.get("errcode")
    if ret not in (None, 0, "0"):
        raise WeChatClawBotError(f"{endpoint} returned ret={ret!r}")
    if errcode not in (None, 0, "0"):
        raise WeChatClawBotError(f"{endpoint} returned errcode={errcode!r}")
    return result


async def fetch_clawbot_qr(session: aiohttp.ClientSession,
                           config: WeChatClawBotConfig) -> dict[str, str]:
    """Request one short-lived ClawBot QR code using the official flow."""
    base_url = config.base_url.rstrip("/")
    stored = load_clawbot_credentials(
        getattr(config, "credentials_file", DEFAULT_CREDENTIALS_FILE))
    existing_token = _normalize_token(
        os.getenv(DEFAULT_TOKEN_ENV, "")) or _normalize_token(
            stored.get("bot_token"))
    # The official client sends local tokens so the server can recognize an
    # already-bound local account.  The token is sent only in this HTTPS body;
    # it is never printed or returned by this function.
    payload = {"local_token_list": [existing_token] if existing_token else []}
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    url = f"{base_url}/ilink/bot/get_bot_qrcode?bot_type={QR_BOT_TYPE}"
    try:
        async with session.post(
                url, data=body, headers=_qr_headers(),
                timeout=aiohttp.ClientTimeout(
                    total=max(10.0, config.request_timeout_sec))) as response:
            result = await _decode_qr_response(response, "get_bot_qrcode")
    except asyncio.CancelledError:
        raise
    except WeChatClawBotError:
        raise
    except Exception as exc:
        raise WeChatClawBotError(
            f"get_bot_qrcode request failed: {type(exc).__name__}") from exc

    qrcode = str(result.get("qrcode") or "").strip()
    image_content = str(result.get("qrcode_img_content") or "").strip()
    if not qrcode or not image_content:
        raise WeChatClawBotError(
            "get_bot_qrcode returned no usable QR code")
    return {"qrcode": qrcode, "qrcode_img_content": image_content}


async def poll_clawbot_qr_status(session: aiohttp.ClientSession,
                                 config: WeChatClawBotConfig, qrcode: str,
                                 *, base_url: Optional[str] = None,
                                 verify_code: Optional[str] = None
                                 ) -> dict[str, Any]:
    """Long-poll the status of one QR code."""
    effective_base = (base_url or config.base_url).rstrip("/")
    query = {"qrcode": qrcode}
    if verify_code:
        query["verify_code"] = verify_code
    url = (f"{effective_base}/ilink/bot/get_qrcode_status?"
           f"{urlencode(query)}")
    try:
        async with session.get(
                url, headers=_qr_headers(),
                timeout=aiohttp.ClientTimeout(
                    total=max(QR_STATUS_TIMEOUT_SEC + 5.0,
                              config.request_timeout_sec))) as response:
            return await _decode_qr_response(response, "get_qrcode_status")
    except asyncio.CancelledError:
        raise
    except WeChatClawBotError:
        raise
    except Exception as exc:
        raise WeChatClawBotError(
            f"get_qrcode_status request failed: {type(exc).__name__}") from exc


def display_clawbot_qr(qrcode_img_content: str, *, open_browser: bool = True
                       ) -> bool:
    """Open the short-lived QR image in the default macOS browser.

    The official endpoint returns ``qrcode_img_content`` as a browser-ready
    URL.  We do not write that URL to a project file.  On non-macOS systems or
    when opening is disabled, it is printed as a fallback for manual opening.
    """
    qr_url = str(qrcode_img_content or "").strip()
    if not qr_url:
        raise WeChatClawBotError("QR response did not contain an image URL")

    print("\n请使用手机微信扫描 ClawBot 二维码并确认授权。")
    if open_browser and sys.platform == "darwin":
        opener = shutil.which("open")
        if opener:
            try:
                subprocess.Popen(
                    [opener, qr_url],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                print("二维码已在默认浏览器打开。")
                return True
            except OSError:
                pass
    print("请在浏览器打开以下临时二维码链接（不要转发）：")
    print(qr_url)
    return False


def _redirect_base_url(value: Any, fallback: str) -> str:
    raw = str(value or "").strip()
    if raw and "://" not in raw:
        raw = f"https://{raw}"
    return _valid_http_url(raw) or fallback.rstrip("/")


async def setup_clawbot_qr(config: WeChatClawBotConfig, *,
                           open_browser: bool = True,
                           timeout_sec: float = 480.0,
                           max_refreshes: int = QR_MAX_REFRESHES,
                           session: Optional[aiohttp.ClientSession] = None
                           ) -> dict[str, Any]:
    """Run QR login, save the resulting token, and return safe metadata.

    This function only talks to the notification service.  It does not load
    venue clients, acquire the trading process lock, or submit an order.
    """
    if timeout_sec <= 0:
        raise WeChatClawBotError("QR setup timeout must be > 0")
    if max_refreshes < 1:
        raise WeChatClawBotError("QR refresh limit must be >= 1")

    owns_session = session is None
    client_session = session or aiohttp.ClientSession()
    deadline = time.monotonic() + timeout_sec
    refresh_count = 0
    try:
        while time.monotonic() < deadline:
            refresh_count += 1
            qr = await fetch_clawbot_qr(client_session, config)
            qrcode = qr["qrcode"]
            display_clawbot_qr(qr["qrcode_img_content"],
                               open_browser=open_browser)
            print("等待扫码确认（二维码短期有效）……")

            poll_base = config.base_url.rstrip("/")
            verify_code: Optional[str] = None
            scanned_notice = False
            while time.monotonic() < deadline:
                try:
                    status_response = await poll_clawbot_qr_status(
                        client_session, config, qrcode,
                        base_url=poll_base, verify_code=verify_code)
                except asyncio.CancelledError:
                    raise
                except WeChatClawBotError as exc:
                    # A transient gateway timeout should not invalidate a QR
                    # code.  Keep polling until the overall setup deadline.
                    print(f"扫码状态暂时不可用（{exc}），继续重试……")
                    await asyncio.sleep(1.0)
                    continue

                status = str(status_response.get("status") or "wait").lower()
                if status == "confirmed":
                    token = _normalize_token(status_response.get("bot_token"))
                    if not token:
                        raise WeChatClawBotError(
                            "扫码已确认，但服务器没有返回 bot token")
                    credential_base = _valid_http_url(
                        status_response.get("baseurl")) or poll_base
                    metadata = save_clawbot_credentials(
                        getattr(config, "credentials_file",
                                DEFAULT_CREDENTIALS_FILE),
                        bot_token=token,
                        base_url=credential_base,
                        bot_id=status_response.get("ilink_bot_id"),
                        user_id=status_response.get("ilink_user_id"),
                    )
                    print("\n✅ 微信 ClawBot 扫码配置成功。")
                    print("凭据已保存到本地安全文件；Token 不会显示。")
                    print("请重启通知脚本，并在微信里给 ClawBot 发一条消息，"
                          "完成接收人和会话绑定。")
                    return metadata

                if status == "binded_redirect":
                    existing = load_clawbot_credentials(
                        getattr(config, "credentials_file",
                                DEFAULT_CREDENTIALS_FILE))
                    if existing:
                        print("\n✅ 此微信已绑定现有 ClawBot 会话，继续使用本地凭据。")
                        return {key: value for key, value in existing.items()
                                if key != "bot_token"}
                    raise WeChatClawBotError(
                        "微信返回已绑定状态，但本地没有可用凭据；请稍后重新扫码")

                if status == "scaned":
                    if not scanned_notice:
                        print("已扫码，等待手机确认授权……")
                        scanned_notice = True
                elif status == "need_verifycode":
                    try:
                        verify_code = (await asyncio.to_thread(
                            input, "请输入微信界面显示的数字验证码：")).strip()
                    except EOFError as exc:
                        raise WeChatClawBotError(
                            "扫码需要验证码，但当前终端无法读取输入") from exc
                    if not verify_code:
                        print("验证码为空，继续等待……")
                elif status == "verify_code_blocked":
                    print("验证码错误次数过多，当前二维码将刷新。")
                    break
                elif status == "scaned_but_redirect":
                    poll_base = _redirect_base_url(
                        status_response.get("redirect_host"), poll_base)
                    print("扫码服务正在切换节点，继续等待……")
                elif status == "expired":
                    print("二维码已过期，准备刷新……")
                    break

                # The official status endpoint is a long poll.  A short sleep
                # protects a mock/edge endpoint that returns immediately.
                await asyncio.sleep(0.2)

            if time.monotonic() >= deadline:
                break
            if refresh_count >= max_refreshes:
                break
            print(f"刷新二维码（{refresh_count + 1}/{max_refreshes}）……")

        raise WeChatClawBotError(
            "二维码登录超时或多次过期，请重新运行 --wechat-setup")
    finally:
        if owns_session:
            await client_session.close()


def _money(value: Any, *, signed: bool = True) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if signed:
        return f"${number:+.4f}"
    return f"${number:.4f}"


def _cost(value: Any) -> str:
    try:
        number = abs(float(value))
    except (TypeError, ValueError):
        return "—"
    return f"-${number:.4f}"


def _qty(value: Any) -> str:
    try:
        return f"{float(value):.6g}"
    except (TypeError, ValueError):
        return "—"


def _duration(seconds: Any) -> str:
    try:
        total = max(0, int(float(seconds)))
    except (TypeError, ValueError):
        return "—"
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}小时{minutes:02d}分"
    return f"{minutes}分{secs:02d}秒"


def _local_day_start(now: float, timezone: str) -> float:
    tz = ZoneInfo(timezone)
    local = datetime.fromtimestamp(now, tz=tz)
    return datetime.combine(local.date(), dt_time.min, tzinfo=tz).timestamp()


def _marks(engine) -> dict[str, Optional[float]]:
    marks: dict[str, Optional[float]] = {}
    for key, venue in getattr(engine, "venues", {}).items():
        book = getattr(venue, "book", None)
        try:
            mark = book.mid() if book is not None else None
        except Exception:
            mark = None
        marks[key] = mark
        venue_key = getattr(venue, "key", key)
        marks[venue_key] = mark
    return marks


def _current_status(engine, now: float) -> tuple[str, str, str, str]:
    halted = bool(getattr(engine, "halted", False))
    record_only = bool(getattr(engine, "record_only", False))
    if halted:
        status = "已暂停（引擎 halted）"
    elif record_only:
        status = "仅采集"
    else:
        status = "运行中"
    try:
        pair_state = engine._pair_state().value
    except Exception:
        pair_state = "unknown"
    reconciliation = (getattr(engine, "reconciliation", {}) or {}).get(
        "status", "unknown")
    active = getattr(engine, "active_execution", None)
    if active is None:
        execution = "无活动执行"
    else:
        execution = (f"{getattr(active, 'phase', 'unknown')} / "
                     f"{getattr(active, 'cycle_id', 'unknown')}")
    return status, pair_state, reconciliation, execution


def build_hourly_report(engine, now: Optional[float] = None) -> str:
    """Build one Chinese report from the same ledger used by the dashboard."""
    now = time.time() if now is None else float(now)
    cfg = engine.cfg
    timezone = cfg.dashboard_day_timezone
    tz = ZoneInfo(timezone)
    hour_start = now - 3600.0
    day_start = _local_day_start(now, timezone)
    ledger = engine.ledger
    hour = ledger.activity_summary(since=hour_start, until=now)
    today = ledger.activity_summary(since=day_start, until=now)
    lifetime = ledger.activity_summary(since=0.0, until=now)
    hour_equity_pnl = ledger.equity_pnl_change(
        since=hour_start, until=now)

    pnl = engine.pnl_tracker.snapshot()
    day_attr = ledger.attribution(
        now=now,
        day_start_ts=pnl.get("day_started_ts") or day_start,
        session_start_ts=pnl.get("session_started_ts"),
        marks=_marks(engine),
        equity_day=pnl.get("net_pnl_usd"),
        equity_session=pnl.get("session_pnl_usd"),
    )["day"]
    status, pair_state, reconciliation, execution = _current_status(engine, now)
    try:
        premium = engine.premium_bps()
    except Exception:
        premium = None
    positions = []
    for venue in getattr(engine, "venues", {}).values():
        positions.append(f"{getattr(venue, 'name', getattr(venue, 'key', '?'))} "
                         f"{float(getattr(venue, 'position', 0.0)):+.6g}")
    net_delta = sum(float(getattr(v, "position", 0.0))
                    for v in getattr(engine, "venues", {}).values())
    strategy_name = str(getattr(engine, "strategy_name", "legacy"))
    local_now = datetime.fromtimestamp(now, tz=tz)
    local_hour_start = datetime.fromtimestamp(hour_start, tz=tz)
    if pnl.get("coverage_complete"):
        today_coverage = "完整本地日"
    elif pnl.get("day_started_ts") is not None:
        baseline = datetime.fromtimestamp(pnl["day_started_ts"], tz=tz)
        today_coverage = f"从 {baseline:%H:%M:%S} 建立权益基线"
    else:
        today_coverage = "权益基线未建立"

    def equity_or_ledger() -> str:
        if hour_equity_pnl is not None:
            return _money(hour_equity_pnl)
        return "—（权益快照不足）"

    return "\n".join([
        f"【Entropy Arb 每小时报告】{local_now:%Y-%m-%d %H:%M:%S} ({timezone})",
        f"策略：{strategy_name} | 品种：{cfg.symbol} | 状态：{status}",
        f"运行时长：{_duration(now - getattr(engine, 'start_ts', now))} | "
        f"配对状态：{pair_state} | 对账：{reconciliation}",
        f"当前持仓：{'；'.join(positions) or '—'} | 净敞口：{net_delta:+.6g}",
        f"当前溢价：{('—' if premium is None else f'{premium:+.3f} bp')} | "
        f"执行：{execution}",
        "",
        f"【过去1小时】{local_hour_start:%H:%M}–{local_now:%H:%M}",
        f"账户权益变动：{equity_or_ledger()} | "
        f"成交账本净额：{_money(hour['realized_net_pnl_usd'])}",
        f"已实现毛利：{_money(hour['gross_realized_pnl_usd'])} | "
        f"手续费：{_cost(hour['fees_usd'])} | 资金费：{_money(hour['funding_pnl_usd'])}",
        f"执行笔数：{hour['trade_count']}（配对 {hour['paired_count']} / "
        f"单腿 {hour['one_leg_count']} / 未成交 {hour['no_fill_count']}）",
        f"Entropy 成交量：{_qty(hour['entropy_qty'])} / "
        f"{_money(hour['entropy_notional_usd'], signed=False)}；"
        f"其中单腿：{_qty(hour['entropy_single_leg_qty'])} / "
        f"{_money(hour['entropy_single_leg_notional_usd'], signed=False)}",
        "",
        "【今日累计】",
        f"账户权益盈亏：{_money(pnl.get('net_pnl_usd'))}（{today_coverage}） | "
        f"账本净盈亏：{_money(day_attr['ledger_net_pnl_usd'])}",
        f"已实现毛利：{_money(day_attr['gross_realized_usd'])} | "
        f"手续费：{_cost(day_attr['fees_usd'])} | "
        f"资金费：{_money(day_attr['funding_pnl_usd'])} | "
        f"未实现：{_money(day_attr['unrealized_usd'])}",
        f"累计执行笔数：{today['trade_count']} | "
        f"账本历史累计：{lifetime['trade_count']} | "
        f"累计成交额：{_money(today['turnover_usd'], signed=False)}",
        f"Entropy 累计成交量：{_qty(today['entropy_qty'])} / "
        f"{_money(today['entropy_notional_usd'], signed=False)}；"
        f"其中单腿：{_qty(today['entropy_single_leg_qty'])} / "
        f"{_money(today['entropy_single_leg_notional_usd'], signed=False)}",
        f"成交覆盖：{today['fill_count']} 个实际 fill，" 
        f"匹配率：{today['matched_rate'] * 100:.1f}%",
    ])


class WeChatHourlyReporter:
    """Run the inbound session listener and one report per configured interval."""

    def __init__(self, engine, config: WeChatClawBotConfig,
                 session: aiohttp.ClientSession, *,
                 client: Optional[WeChatClawBotClient] = None) -> None:
        self.engine = engine
        self.config = config
        self.client = client or WeChatClawBotClient(session, config)
        self.last_report_ts: Optional[float] = None

    async def _send_report(self) -> bool:
        # In --no-dashboard mode this is the regular equity sample that makes
        # the hour-over-hour account PnL measurable after a restart.
        record_snapshot = getattr(self.engine, "_record_pnl_snapshot", None)
        if record_snapshot is not None:
            try:
                record_snapshot()
            except Exception:
                log.exception("failed to record equity snapshot for report")
        report = build_hourly_report(self.engine)
        sent = await self.client.send_text(report)
        if sent:
            self.last_report_ts = time.time()
            log.info("ClawBot hourly report sent")
        return sent

    async def run(self, stop: asyncio.Event) -> None:
        if not self.client.configured:
            log.error("ClawBot reporting enabled but no local QR credentials "
                      "or legacy token is available; run --wechat-setup")
            return
        listener = asyncio.create_task(
            self.client.run_listener(stop), name="wechat-clawbot-listener")
        try:
            if self.config.send_startup:
                try:
                    await self._send_report()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("ClawBot startup report failed")
            next_report = time.monotonic() + self.config.interval_sec
            while not stop.is_set():
                try:
                    await asyncio.wait_for(
                        stop.wait(), timeout=max(0.0, next_report - time.monotonic()))
                except asyncio.TimeoutError:
                    pass
                if stop.is_set():
                    break
                while next_report <= time.monotonic():
                    next_report += self.config.interval_sec
                try:
                    await self._send_report()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A failed notification must never stop the engine or the
                    # listener; the next scheduled report will retry.
                    log.exception("ClawBot hourly report failed")
        finally:
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)
