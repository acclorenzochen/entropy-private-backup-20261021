#!/usr/bin/env python3
"""entropy-arb entry point.

    # collect minute + one-second data — no strategy or credentials needed
    python3 main.py --record-only --symbol SNDK --hedge lighter-rh

    # Existing taker strategy (backward-compatible default)
    python3 main.py --strategy legacy --symbol SNDK --hedge lighter-rh

    # Maker-hedge strategy from the sibling project
    python3 main.py --strategy maker-hedge --paper

The existing strategy remains the default.  The maker-hedge strategy can be
selected explicitly with --strategy maker-hedge; it has its own config and
runtime state while reusing the existing project's .env by default.

For the legacy strategy, --symbol and --hedge are required on every start.
Add --cn for a Chinese-language dashboard.  The legacy strategy has no paper
mode.  Collect data with --record-only, set your thresholds with
tools/analyze.py, then go live with small position caps.

On a terminal the bot shows a live Rich dashboard (books, signal, positions,
PnL, last executions) and writes log lines to logging.file; use
--no-dashboard for plain console logs (nohup/systemd). Strategy lives in
config.yaml, credentials in .env — see the README (English) /
README.zh-CN.md (中文).
"""
import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys

from entropy_arb.config import HEDGE_VENUES, ConfigError, load_config
from entropy_arb.dashboard_state import mark_dashboard_status
from entropy_arb.engine import Engine
from entropy_arb.process_guard import ProcessGuard
from entropy_arb.strategy_launcher import STRATEGIES, run_maker_hedge, selected_strategy


def setup_logging(level: str, log_file: str = None,
                  extra_handler: logging.Handler = None) -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level, logging.INFO))
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S")
    if log_file:
        d = os.path.dirname(log_file)
        if d:
            os.makedirs(d, exist_ok=True)
        h = logging.FileHandler(log_file)
    else:
        h = logging.StreamHandler()
    h.setFormatter(fmt)
    root.addHandler(h)
    if extra_handler is not None:
        root.addHandler(extra_handler)
    logging.getLogger("websockets").setLevel(logging.WARNING)


async def amain(cfg, record_only: bool, use_dashboard: bool, force_tty: bool,
                log_buffer, lang: str, config_file: str,
                env_file: str, strategy_name: str = "legacy",
                flatten_only: bool = False,
                max_complete_cycles: int | None = None,
                max_run_loss_usd: float | None = None) -> None:
    eng = Engine(cfg, record_only=record_only, config_file=config_file,
                 env_file=env_file, strategy_name=strategy_name,
                 flatten_only=flatten_only,
                 max_complete_cycles=max_complete_cycles,
                 max_run_loss_usd=max_run_loss_usd)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, eng.request_stop)
    dash_task = None
    final_status = "stopped"
    try:
        if use_dashboard:
            from entropy_arb.dashboard import Dashboard
            dash = Dashboard(eng, log_buffer, cfg.log_file,
                             force_terminal=force_tty, lang=lang)
            dash_task = asyncio.create_task(dash.run(), name="dashboard")
        await eng.run()
    except BaseException:
        final_status = "error"
        raise
    finally:
        eng.request_stop()
        if dash_task is not None:
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(dash_task, timeout=5)
            if not dash_task.done():
                dash_task.cancel()
        if cfg.web_dashboard_enabled:
            if final_status == "stopped" and eng.halted:
                final_status = "error"
            mark_dashboard_status(cfg.dashboard_status_json, final_status)


def main() -> None:
    # QR onboarding is an isolated notification setup path.  Check it before
    # strategy dispatch so it does not require trading CLI arguments and can
    # never accidentally start either live strategy.
    if "--wechat-setup" in sys.argv[1:]:
        from entropy_arb.wechat_setup import run_cli
        sys.exit(run_cli(sys.argv[1:]))

    if selected_strategy() == "maker-hedge":
        run_maker_hedge(sys.argv[1:])
        return

    p = argparse.ArgumentParser(
        description="Two-venue LIVE arbitrage: Entropy vs Lighter mainnet / "
                    "Lighter Robinhood / trade.xyz. Without --record-only, "
                    "real orders are sent.")
    p.add_argument("--strategy", choices=STRATEGIES, default="legacy",
                   help="strategy to run; default: legacy. "
                        "Use maker-hedge to select the maker strategy.")
    p.add_argument("--symbol", required=True,
                   help="symbol traded on both venues, e.g. SNDK / "
                        "两个交易所共同交易的品种")
    p.add_argument("--hedge", required=True, choices=HEDGE_VENUES,
                   metavar="VENUE",
                   help=f"hedge venue, one of: {', '.join(HEDGE_VENUES)} / "
                        f"对冲腿，三选一")
    p.add_argument("--config", default="config.yaml",
                   help="strategy config (default: config.yaml)")
    p.add_argument("--env-file", default=".env",
                   help="credentials file (default: .env)")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--record-only", action="store_true",
                      help="only collect market data, run no strategy, send no "
                           "orders (needs no credentials)")
    mode.add_argument("--flatten-only", action="store_true",
                      help="connect and send reduce-only orders until both "
                           "venues are flat; never open a new position")
    p.add_argument("--max-complete-cycles", type=int, default=None,
                   help="stop after this many new strict clean cycles")
    p.add_argument("--max-run-loss-usd", type=float, default=None,
                   help="stop while flat when new completed-cycle net PnL "
                        "reaches this loss budget")
    p.add_argument("--cn", action="store_true",
                   help="display the dashboard in Chinese / 仪表盘使用中文")
    disp = p.add_mutually_exclusive_group()
    disp.add_argument("--dashboard", action="store_true",
                      help="force the Rich dashboard even without a tty")
    disp.add_argument("--no-dashboard", action="store_true",
                      help="plain console logs instead of the dashboard")
    args = p.parse_args()
    if args.max_complete_cycles is not None and args.max_complete_cycles < 1:
        p.error("--max-complete-cycles must be at least 1")
    if args.max_run_loss_usd is not None and args.max_run_loss_usd <= 0:
        p.error("--max-run-loss-usd must be greater than 0")

    try:
        cfg = load_config(args.config, args.env_file,
                          symbol=args.symbol, hedge_venue=args.hedge)
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        sys.exit(2)

    use_dashboard = (cfg.dashboard or args.dashboard) and not args.no_dashboard
    force_tty = args.dashboard
    if use_dashboard and not (sys.stdout.isatty() or force_tty):
        use_dashboard = False

    log_buffer = None
    if use_dashboard:
        try:
            from entropy_arb.dashboard import BufferLogHandler
        except ImportError:
            print("`rich` is not installed — falling back to plain logs "
                  "(pip install -r requirements.txt)", file=sys.stderr)
            use_dashboard = False
    if use_dashboard:
        log_buffer = BufferLogHandler()
        setup_logging(cfg.log_level, log_file=cfg.log_file,
                      extra_handler=log_buffer)
    else:
        # Keep startup failures, reconnects, order errors, and clean shutdown
        # diagnostics when running under nohup/systemd or --no-dashboard.
        setup_logging(cfg.log_level, log_file=cfg.log_file)

    try:
        guard = (ProcessGuard(cfg.persistence.lock_file)
                 if not args.record_only else contextlib.nullcontext())
        with guard:
            asyncio.run(amain(cfg, record_only=args.record_only,
                              use_dashboard=use_dashboard, force_tty=force_tty,
                              log_buffer=log_buffer,
                              lang="zh" if args.cn else "en",
                              config_file=args.config,
                              env_file=args.env_file,
                              strategy_name=args.strategy,
                              flatten_only=args.flatten_only,
                              max_complete_cycles=args.max_complete_cycles,
                              max_run_loss_usd=args.max_run_loss_usd))
    except RuntimeError as e:
        # startup failures (missing credentials, market not found, venue
        # unreachable) — a clean message, not a traceback
        print(f"startup error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
