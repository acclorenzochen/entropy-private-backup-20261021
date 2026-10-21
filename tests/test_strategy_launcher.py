from __future__ import annotations

from entropy_arb.strategy_launcher import (
    build_maker_hedge_argv,
    selected_strategy,
)


def test_strategy_selector_keeps_legacy_as_backward_compatible_default():
    assert selected_strategy(["--record-only"]) == "legacy"
    assert selected_strategy(["--strategy", "legacy", "--symbol", "SNDK"]) == "legacy"
    assert selected_strategy(["--strategy=maker-hedge", "--paper"]) == "maker-hedge"


def test_maker_hedge_defaults_to_its_config_and_existing_env(tmp_path):
    maker_root = tmp_path / "entropy-maker-hedge"
    args = build_maker_hedge_argv(
        ["--strategy", "maker-hedge", "--paper", "--no-dashboard"],
        cwd=tmp_path,
        maker_root=maker_root,
        project_root=tmp_path,
    )
    assert "--strategy" not in args
    assert args[args.index("--config") + 1] == str(maker_root / "config.yaml")
    assert args[args.index("--env-file") + 1] == str(tmp_path / ".env")


def test_maker_hedge_resolves_explicit_relative_paths_before_chdir(tmp_path):
    args = build_maker_hedge_argv(
        ["--strategy=maker-hedge", "--config", "custom.yaml",
         "--env-file=secrets.env", "--record-only"],
        cwd=tmp_path,
        maker_root=tmp_path / "maker",
    )
    assert args[args.index("--config") + 1] == str((tmp_path / "custom.yaml").resolve())
    assert f"--env-file={(tmp_path / 'secrets.env').resolve()}" in args
