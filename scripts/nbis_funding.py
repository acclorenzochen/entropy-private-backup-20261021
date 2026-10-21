#!/usr/bin/env python3
"""Small manual NBIS launcher; defaults to unsigned public preflight."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid


ROOT = Path(__file__).resolve().parents[1]
AUTHORIZATION = 'nbis-funding-authorization.json'


def positive_cycles(value: str) -> int:
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('轮数必须是正整数') from exc
    if not 1 <= count <= 2**63 - 1:
        raise argparse.ArgumentTypeError('轮数必须是 SQLite 可记录的正整数')
    return count


def command(mode: str) -> list[str]:
    args = [str(ROOT / '.venv/bin/python'), str(ROOT / 'main_v3.py'),
            '--symbol', 'NBIS', '--hedge', 'lighter',
            '--config', 'config.nbis.yaml',
            '--v3-config', 'config.v3.nbis-funding.json',
            '--deployment', 'nbis-funding']
    if mode == 'check':
        return args + ['--env-file', '/dev/null', '--public-preflight-only']
    if mode == 'start':
        return args + ['--env-file', '.env', '--batch-authorization', AUTHORIZATION, '--live']
    raise ValueError('unsupported launcher mode')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='NBIS 手动运行助手；默认仅做无签名公共预检')
    parser.add_argument('mode', nargs='?', default='check',
                        choices=('check', 'prepare', 'start', 'status'))
    parser.add_argument('--cycles', type=positive_cycles,
                        help='仅用于 prepare；不提供则询问轮数，无固定 5 轮上限')
    args = parser.parse_args(argv)
    if args.cycles is not None and args.mode != 'prepare':
        parser.error('--cycles 仅用于 prepare，不能改写已有批次授权')
    try:
        if args.mode == 'prepare':
            path = ROOT / AUTHORIZATION
            if path.exists() or path.is_symlink():
                raise ValueError('授权文件已存在，不覆盖。请继续检查原文件，不要重置已有 batch_id。')
            cycles = args.cycles
            if cycles is None:
                try:
                    cycles = positive_cycles(input('本批次最多运行多少轮？请输入正整数：').strip())
                except (EOFError, argparse.ArgumentTypeError) as exc:
                    raise ValueError('未生成授权文件；请输入正整数，或使用 prepare --cycles N') from exc
            template = json.loads((ROOT / 'nbis-funding-authorization.example.json').read_text())
            template.update(batch_id='nbis-funding-' + uuid.uuid4().hex,
                            max_position_cycles=cycles, approved_live=False)
            with path.open('x', encoding='utf-8') as output:
                output.write(json.dumps(template, ensure_ascii=False, indent=2) + '\n')
            print(f'已生成待确认授权：{path}\n批次上限：{cycles} 轮；approved_live 仍为 false。')
            print('打开文件核对参数，决定实盘时手动将 approved_live 改为 true 并保存。')
            return 0
        if args.mode == 'status':
            path = ROOT / 'logs/v3/nbis/status.json'
            if not path.is_file():
                raise ValueError('没有 NBIS 实盘状态文件；尚未启动，或使用了其他状态路径。')
            state = json.loads(path.read_text())
            if not isinstance(state, dict):
                raise ValueError('NBIS 状态格式不正确')
            fields = ('process_id', 'source_ts', 'mode', 'current_state', 'reason',
                      'configured_leverage', 'applied_leverage', 'position_cycles',
                      'max_position_cycles', 'funding_completion_label',
                      'funding_exit_reason', 'funding_state')
            print('NBIS 最后发布的状态；保留文件不代表进程仍在运行：')
            print(json.dumps({key: state.get(key) for key in fields}, ensure_ascii=False, indent=2))
            return 0
        if args.mode == 'start':
            authorization = json.loads((ROOT / AUTHORIZATION).read_text())
            if not isinstance(authorization, dict) or authorization.get('approved_live') is not True:
                raise ValueError('尚未批准实盘：请手动核对授权文件并将 approved_live 改为 true。')
            print('先运行无签名公共预检；通过后进入实盘，可设置杠杆并发送真实订单。', flush=True)
            result = subprocess.run(command('check'), cwd=ROOT)
            if result.returncode != 0:
                print('公共预检未通过，未进入实盘。不要放宽过期或账户互斥保护。', file=sys.stderr)
                return result.returncode if result.returncode > 0 else 130
        launch = command(args.mode)
        os.chdir(ROOT)
        os.execv(launch[0], launch)
        return 0
    except (OSError, ValueError) as exc:
        print(f'未继续执行：{exc}', file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
