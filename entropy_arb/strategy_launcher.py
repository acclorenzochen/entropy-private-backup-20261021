"""Startup dispatch for the legacy and maker-hedge strategies.

The two strategies intentionally keep separate configuration and runtime
state.  This module gives them one user-facing entry point without merging
their incompatible execution state machines or allowing one strategy to
reuse the other's checkpoints by accident.
"""
from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path
import sys
from typing import Sequence


STRATEGIES = ("legacy", "maker-hedge")


def selected_strategy(argv: Sequence[str] | None = None) -> str:
    """Read only the common strategy selector, leaving all other args alone."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--strategy", choices=STRATEGIES, default="legacy")
    args, _ = parser.parse_known_args(list(argv) if argv is not None else None)
    return args.strategy


def _without_strategy(argv: Sequence[str]) -> list[str]:
    result: list[str] = []
    args = list(argv)
    index = 0
    while index < len(args):
        item = args[index]
        if item == "--strategy":
            index += 2
            continue
        if item.startswith("--strategy="):
            index += 1
            continue
        result.append(item)
        index += 1
    return result


def _resolve_path_options(argv: Sequence[str], *, cwd: Path) -> list[str]:
    """Make relative config/env paths independent of the temporary chdir."""
    result: list[str] = []
    args = list(argv)
    index = 0
    while index < len(args):
        item = args[index]
        if item in {"--config", "--env-file"} and index + 1 < len(args):
            value = Path(args[index + 1]).expanduser()
            if not value.is_absolute():
                value = cwd / value
            result.extend([item, str(value.resolve())])
            index += 2
            continue
        if item.startswith("--config=") or item.startswith("--env-file="):
            name, raw_value = item.split("=", 1)
            value = Path(raw_value).expanduser()
            if not value.is_absolute():
                value = cwd / value
            result.append(f"{name}={value.resolve()}")
            index += 1
            continue
        result.append(item)
        index += 1
    return result


def build_maker_hedge_argv(
    argv: Sequence[str], *, cwd: Path | None = None,
    maker_root: Path | None = None, project_root: Path | None = None,
) -> list[str]:
    """Translate the unified CLI into the maker-hedge CLI.

    The maker strategy has a different YAML schema from the legacy strategy,
    so its default config and runtime logs stay in the sibling project.  The
    credential file defaults to the existing project's ``.env`` and is never
    copied.
    """
    caller_cwd = (cwd or Path.cwd()).expanduser().resolve()
    legacy_root = (project_root or Path(__file__).resolve().parents[1]).expanduser().resolve()
    root = (maker_root or
            (Path(__file__).resolve().parents[1].parent / "entropy-maker-hedge")
            ).expanduser().resolve()
    args = _resolve_path_options(_without_strategy(argv), cwd=caller_cwd)
    if not any(arg == "--config" or arg.startswith("--config=") for arg in args):
        args.extend(["--config", str(root / "config.yaml")])
    if not any(arg == "--env-file" or arg.startswith("--env-file=") for arg in args):
        args.extend(["--env-file", str(legacy_root / ".env")])
    return args


def run_maker_hedge(argv: Sequence[str]) -> None:
    """Run the sibling maker-hedge entry point as the selected strategy."""
    root = (Path(__file__).resolve().parents[1].parent / "entropy-maker-hedge").resolve()
    entry = root / "main.py"
    if not entry.is_file():
        raise RuntimeError(f"maker-hedge strategy project not found: {root}")
    spec = importlib.util.spec_from_file_location("_entropy_maker_hedge_entry", entry)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load maker-hedge entry point: {entry}")

    module = importlib.util.module_from_spec(spec)
    module_name = spec.name
    old_argv = sys.argv
    old_cwd = Path.cwd()
    old_path = list(sys.path)
    sys.modules[module_name] = module
    sys.path.insert(0, str(root))
    sys.argv = [str(entry), *build_maker_hedge_argv(argv, cwd=old_cwd, maker_root=root)]
    try:
        # The maker strategy's relative logs/config are deliberately scoped to
        # its own project.  The env path is passed explicitly and remains the
        # previous project's file.
        os.chdir(root)
        spec.loader.exec_module(module)
        module.main()
    finally:
        os.chdir(old_cwd)
        sys.path[:] = old_path
        sys.argv = old_argv
        sys.modules.pop(module_name, None)
