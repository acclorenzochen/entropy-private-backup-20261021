"""Exact shell launch chain, with a capture-only Python stub and no credentials."""
import asyncio
from decimal import Decimal as D
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
import yaml

from entropy_arb import config as config_module
import main_v3
from entropy_arb.v3.parameter_config import parameter_limits


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('connection_config,accepted', [
    (None, False), ('config.sndk-v3-200.yaml', True),
])
def test_user_shell_command_resolves_connection_and_v3_amounts(
        tmp_path, monkeypatch, connection_config, accepted):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    shutil.copy2(ROOT / 'scripts/run_v3_live.sh', scripts)
    for name in ('config.yaml', 'config.sndk-v3-200.yaml', 'config.v3.edge-next.json'):
        shutil.copy2(ROOT / name, tmp_path / name)
    python_stub = tmp_path / '.venv/bin/python'
    python_stub.parent.mkdir(parents=True)
    capture = tmp_path / 'argv.json'
    python_stub.write_text(f'#!{sys.executable}\n'
        'import json,os,sys\nfrom pathlib import Path\n'
        'Path(os.environ["CAPTURE_PATH"]).write_text(json.dumps(sys.argv[1:]))\n')
    python_stub.chmod(0o700)
    authorization = tmp_path / 'logs/v3/batch.authorization.20260923-002.json'
    authorization.parent.mkdir(parents=True)
    authorization.write_text(json.dumps(dict(batch_id='offline-batch',
        pair_identity='default:hl:SNDK:lighter:SNDK', max_position_cycles=100,
        loss_budget_usd='3', max_notional_usd='200', approved_live=True)))
    # Only the script's exec target runs: this stub never loads main_v3 or live SDKs.
    env = dict(PATH=os.environ['PATH'], CAPTURE_PATH=str(capture),
        ENTROPY_SYMBOL='SNDK', ENTROPY_HEDGE='lighter',
        ENTROPY_V3_CONFIG=str(tmp_path / 'config.v3.edge-next.json'),
        ENTROPY_V3_AUTHORIZATION=str(authorization),
        ENTROPY_ENV_FILE=str(tmp_path / '.env'))
    if connection_config:
        env['ENTROPY_CONFIG'] = str(tmp_path / connection_config)
    subprocess.run(['/bin/bash', str(scripts / 'run_v3_live.sh'),
        '--operator-control', '--deployment', 'default'], cwd=tmp_path,
        env=env, check=True, capture_output=True)
    captured = json.loads(capture.read_text())
    assert captured[0] == str(tmp_path / 'main_v3.py')
    args = main_v3.parser().parse_args(captured[1:])
    assert args.live and args.operator_control and not args.reopen_entries
    assert args.config == (str(tmp_path / connection_config) if connection_config else 'config.yaml')
    assert args.env_file == str(tmp_path / '.env')
    assert args.batch_authorization == str(authorization)
    # Keep the real config parser, but prohibit dotenv and credential environment access.
    loaded_env_paths = []
    monkeypatch.setattr(config_module, 'load_dotenv', lambda path: loaded_env_paths.append(path))
    monkeypatch.setattr(config_module, '_env_s', lambda name: None)
    monkeypatch.setattr(config_module, '_env_i', lambda name: None)
    monkeypatch.chdir(tmp_path)
    dispatches = []
    async def after_validation(args, cfg, settings, pair, auth, status, *sources):
        assert settings.max_notional_usd == auth.max_notional_usd == D('200')
        assert cfg.entropy.cap_usd == cfg.hedge.cap_usd == 200
        assert cfg.entropy.leverage == cfg.hedge.leverage == 10
        assert auth.loss_budget_usd == D('3') and auth.max_position_cycles == 100
        assert parameter_limits(settings, auth, cfg)['max_notional_usd'] == '200'
        dispatches.append(pair)
        return 0
    monkeypatch.setattr(main_v3, '_run_configured', after_validation)
    if accepted:
        assert asyncio.run(main_v3.amain(args)) == 0
        assert dispatches == ['default:hl:SNDK:lighter:SNDK']
    else:
        with pytest.raises(ValueError, match='parameter_out_of_bounds_max_notional_usd'):
            asyncio.run(main_v3.amain(args))
        assert dispatches == []
        assert not Path(args.status_json).exists()  # Refusal occurs before account/live initialization.
    assert loaded_env_paths == [args.env_file]
    assert not Path(args.env_file).exists()
    assert not Path(args.journal).exists()


def test_sndk_200_connection_config_preserves_every_other_setting():
    original = yaml.safe_load((ROOT / 'config.yaml').read_text())
    selected = yaml.safe_load((ROOT / 'config.sndk-v3-200.yaml').read_text())
    for leg in ('entropy', 'hedge'):
        assert selected[leg]['max_position_usd'] == 200
        selected[leg]['max_position_usd'] = original[leg]['max_position_usd']
    assert selected == original
