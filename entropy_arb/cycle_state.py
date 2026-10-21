"""Small atomic persistence layer for the inventory-cycle context."""
from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path

from .signal import InventoryCycle

log = logging.getLogger("cycle-state")


def load_cycle_state(path: str | os.PathLike[str]) -> InventoryCycle:
    """Load a persisted cycle, failing closed to an unanchored cycle."""
    try:
        with Path(path).open(encoding="utf-8") as fh:
            return InventoryCycle.from_dict(json.load(fh))
    except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError):
        return InventoryCycle()


def write_cycle_state(path: str | os.PathLike[str],
                      cycle: InventoryCycle) -> None:
    """Atomically persist cycle context in the target directory."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        dir=target.parent, prefix=f".{target.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(cycle.to_dict(), fh, ensure_ascii=False,
                      allow_nan=False, separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary, target)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def safe_write_cycle_state(path: str | os.PathLike[str],
                           cycle: InventoryCycle) -> bool:
    try:
        write_cycle_state(path, cycle)
        return True
    except Exception as exc:
        log.warning("cycle state write failed: %r", exc)
        return False
