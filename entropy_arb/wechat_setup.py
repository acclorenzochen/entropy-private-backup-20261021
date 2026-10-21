"""Command-line QR onboarding for the WeChat ClawBot reporter.

This module is intentionally separate from the trading engine.  Running
``python main.py --wechat-setup`` never constructs a venue client, acquires
the trading lock, or starts an execution loop.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import replace
from typing import Sequence

from .config import HEDGE_VENUES, ConfigError, load_config
from .wechat_clawbot import WeChatClawBotError, setup_clawbot_qr


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python main.py --wechat-setup",
        description="Configure WeChat ClawBot by QR code without starting trading.")
    parser.add_argument("--wechat-setup", action="store_true",
                        help=argparse.SUPPRESS)
    parser.add_argument("--config", default="config.yaml",
                        help="strategy config used for the WeChat settings")
    parser.add_argument("--env-file", default=".env",
                        help="optional env file; it is never modified")
    # load_config owns the complete YAML validation contract.  These two
    # values are only validation inputs; setup itself does not trade them.
    parser.add_argument("--symbol", default="SNDK", help=argparse.SUPPRESS)
    parser.add_argument("--hedge", choices=HEDGE_VENUES, default="lighter",
                        help=argparse.SUPPRESS)
    parser.add_argument("--base-url", default=None,
                        help="override the iLink base URL for this setup")
    parser.add_argument("--credentials-file", default=None,
                        help="override the local QR credentials file")
    parser.add_argument("--state-file", default=None,
                        help="override the local recipient/context state file")
    parser.add_argument("--timeout-sec", type=float, default=480.0,
                        help="overall QR setup timeout (default: 480)")
    parser.add_argument("--max-refreshes", type=int, default=3,
                        help="maximum QR displays before failing (default: 3)")
    parser.add_argument("--no-open", action="store_true",
                        help="do not open the QR URL in the default browser")
    return parser


def run_cli(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        cfg = load_config(args.config, args.env_file,
                          symbol=args.symbol, hedge_venue=args.hedge)
        wechat_cfg = cfg.wechat_clawbot
        overrides = {}
        if args.base_url is not None:
            overrides["base_url"] = args.base_url
        if args.credentials_file is not None:
            overrides["credentials_file"] = args.credentials_file
        if args.state_file is not None:
            overrides["state_file"] = args.state_file
        if overrides:
            wechat_cfg = replace(wechat_cfg, **overrides)
        asyncio.run(setup_clawbot_qr(
            wechat_cfg,
            open_browser=not args.no_open,
            timeout_sec=args.timeout_sec,
            max_refreshes=args.max_refreshes,
        ))
    except KeyboardInterrupt:
        print("\n已取消微信扫码配置。", file=sys.stderr)
        return 130
    except (ConfigError, WeChatClawBotError) as exc:
        print(f"微信扫码配置失败：{exc}", file=sys.stderr)
        return 1
    return 0

