#!/usr/bin/env python3
"""Import legacy CSV recordings into the durable SQLite ledger."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Keep the command runnable as ``python tools/migrate_legacy.py`` from the
# repository root, just like the existing analysis/replay utilities.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from entropy_arb.trade_ledger import TradeLedger


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="logs/entropy_arb.sqlite3")
    parser.add_argument("--trades", default="logs/trades.csv")
    parser.add_argument("--seconds", default="logs/seconds.csv")
    parser.add_argument("--minutes", default="logs/minutes.csv")
    args = parser.parse_args()
    ledger = TradeLedger(Path(args.db))
    try:
        counts = ledger.migrate_legacy(
            trades_paths=(args.trades, f"{args.trades}.old"),
            seconds_paths=(args.seconds, f"{args.seconds}.old"),
            minutes_paths=(args.minutes, f"{args.minutes}.old"))
        print(f"SQLite migration complete: {counts}")
    finally:
        ledger.close()


if __name__ == "__main__":
    main()
