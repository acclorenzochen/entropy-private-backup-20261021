#!/usr/bin/env python3
"""Export a consistent, read-only V3 Journal snapshot as connector-readable SQL."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import signal
import sqlite3
import tempfile
import threading


def export_once(journal: Path, output: Path) -> int:
    journal = journal.resolve(strict=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with sqlite3.connect(journal.as_uri() + "?mode=ro", uri=True,
                             timeout=5) as database:
            database.execute("BEGIN")
            with tempfile.NamedTemporaryFile(
                    mode="w", encoding="utf-8", dir=output.parent,
                    prefix=f".{output.name}.", suffix=".tmp",
                    delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(
                    f"-- V3 Journal snapshot, UTC "
                    f"{datetime.now(timezone.utc).isoformat()}\n")
                stream.write(f"-- Source: {journal}\n")
                for statement in database.iterdump():
                    stream.write(statement + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            database.rollback()
        os.replace(temporary, output)
        return output.stat().st_size
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path,
                        default=Path("logs/v3/journal.sqlite3"))
    parser.add_argument("--output", type=Path,
                        default=Path("reports/entropy-edge-v1/journal-live.sql"))
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-seconds", type=float, default=60)
    args = parser.parse_args()
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be positive")

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    while True:
        try:
            size = export_once(args.journal, args.output)
            print(f"Journal export updated: {args.output} ({size} bytes)",
                  flush=True)
        except (OSError, sqlite3.Error) as exc:
            if not args.watch:
                raise
            print(f"Journal export failed: {exc}", flush=True)
        if not args.watch or stop.wait(args.interval_seconds):
            break


if __name__ == "__main__":
    main()
