"""Single-process guard for live trading.

The lock is advisory and held by the process for its whole lifetime.  A stale
lock file is harmless: the kernel releases the flock when the owner exits, so
there is no PID-file cleanup race during a crash.
"""
from __future__ import annotations

import fcntl
import json
import os
import time
from pathlib import Path


class ProcessGuardError(RuntimeError):
    pass


class ProcessGuard:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._fh = None

    def acquire(self) -> "ProcessGuard":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            try:
                fh.seek(0)
                owner = fh.read(200).strip()
            finally:
                fh.close()
            raise ProcessGuardError(
                f"another entropy-arb process owns {self.path}"
                + (f" ({owner})" if owner else "")) from exc
        except BaseException:
            fh.close()
            raise
        fh.seek(0)
        fh.truncate()
        json.dump({"pid": os.getpid(), "started_ts": time.time()}, fh)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
        self._fh = fh
        return self

    def release(self) -> None:
        fh, self._fh = self._fh, None
        if fh is None:
            return
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()

    def __enter__(self) -> "ProcessGuard":
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
