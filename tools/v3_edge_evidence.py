#!/usr/bin/env python3
"""Offline R7 evidence; no sender, credentials, production DB, or process inspection.

Run with the project's Python. Without --run-tests only collect tests; raw run
logs explicitly say NOT_RUN. Name-based mappings are candidates, not acceptance
proof. All generated JSON shares a run_id and binds to before/after hashes.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION = Path('/Users/mingyuchen/Documents/ChatGPT/entropy')
DEFAULT_AUTH = PRODUCTION / 'logs/v3/batch.authorization.20260923-002.json'

# Search only collected node IDs. Explicit test_tNN names are also recognized.
# Empty matches remain UNCOVERED, never silently promoted to PASS.
CASES = [
    ('首次 OPEN 冻结决策', r'frozen|final_signal'),
    ('初始 budget 不足', r'open_first_rate_budget_unavailable'),
    ('terminal zero 后重验', r'original_signal_disappears|retry_reprices'),
    ('partial 正成交不扩大', r'first_partial_never_expands|partial_first_leg_hedges'),
    ('timeout 同 ID', r'timeout_fill_resolves|unknown_resolves_same_identity'),
    ('损坏响应 UNKNOWN', r'malformed_response|untrusted_submitted|corrupt_action'),
    ('数量/身份验证', r'bad_receipt_quantity|invalid_or_oversized|missing_quantity_and_identity'),
    ('zero 与非零敞口矛盾', r'terminal_zero_contradicts'),
    ('registered-only 恢复', r'registered_only_restart'),
    ('持久化意图恢复幂等', r'restart_preserves_intent|prepared_restart|cumulative_fill_updates'),
    ('预算拒绝后重启', r'budget_denied_registered'),
    ('analytics 缺失不 gate', r'analytics.*(missing|absence|fail)|missing.*analytics'),
    ('canonical fee/funding 仍 gate', r'missing_fee|canonical.*(fee|funding)'),
    ('真实入场锚点', r'canonical_entry_anchor|entry_anchor.*reconstruct'),
    ('未知均价不伪造', r'missing_live_entry_anchor'),
    ('fresh CloseContext 复用', r'fresh_close_context'),
    ('context 过期回退', r'stale_context|new_ws_generation'),
    ('迟到 H 快照不重复减仓', r'late_hedge_snapshot'),
    ('partial hedge 精确剩余', r'late_hedge_snapshot|partial_hedge_retries_only_remaining'),
    ('重复终态与 group 幂等', r'cumulative_fill_updates_idempotent|applies_terminal_once'),
    ('UNKNOWN 后迟到终态', r'late_terminal|unknown_close.*same_id'),
    ('快照缺失与实际冲突', r'authoritative_zero|position_api_fails|without_authoritative_positions'),
    ('fast 状态不泄漏', r'(fast|context).*(leak|cleanup|next_open)|next_open.*hedge'),
    ('持久化边界崩溃恢复', r'close_restart_boundaries|restart_after_entropy_close'),
    ('非关键依赖不阻塞 handoff', r'slow_analytics|slow_status|telemetry_waits|equity.*(slow|handoff)'),
    ('A/B/C prefix 同量重报价', r'quote_prefix_reprices|attempt_quotes_preserve'),
    ('writer 故障与 dropped', r'status_failure|drop_boundary|persistence_failure'),
    ('WS tid 去重和乱序', r'snapshot_replay_by_tid|same_price_different_tid'),
    ('active 超 LRU', r'over_capacity'),
    ('WS 完整聚合证据', r'ws_aggregate|without_trade_identity|missing_tid'),
    ('多 watcher 与晚到终态', r'multiple_watchers|resolve_accepts_ws_terminal'),
    ('短 invalid 连续性', r'short_invalid|record_only_invalid'),
    ('seq/session 边界', r'missing_sequence|bootstrap_invalid|cross_restart_mono|reconnect'),
    ('固定数量路径', r'fixed_quantity'),
    ('单侧浅盘口', r'one_shallow_side'),
    ('重复帧 heartbeat', r'identical_book_frames'),
    ('checkpoint/status 慢盘', r'file_write|file_writer|slow_status|record_only_invalid'),
    ('UNKNOWN 与 terminal zero 报表', r'unknown_zero'),
    ('跨进程时钟归属', r'cross_restart_mono|terminal_received_clocks'),
    ('每个退出 group 触发点', r'two_close_groups|safe_group_and_trigger'),
    ('fast/recovery parent 来源', r'links_steps|safe_group_and_trigger'),
    ('zero fill 非完整循环', r'eight_zero_fill'),
    ('任意半开窗口', r'completion_window|window_censors'),
    ('实际现金费用只计一次', r'cash_costs_not_deducted|fee_events|signed_funding|double_counting'),
    ('跨分片保留风险状态', r'gap_retains_pending'),
    ('replay/live 预算一致', r'request_reserve_and_exact'),
    ('replay/fake runtime 差分', r'positions_and_cash_match_fake_live'),
    ('正常循环 clean', r'(clean|completes_one_cycle|full_open_hedge_close)'),
    ('时序证据缺失', r'missing_terminal_evidence|handoff_delays|joint_draw'),
    ('独立 holdout 与 warmup', r'holdout|warmup|chronological_stages|calibration_excludes_future'),
    ('迁移与兼容回滚', r'migration|rollback|newer_schema'),
    ('effective 配置与授权', r'effective_manifest|live_authorization|separate_first_leg_rooms'),
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git(*args: str) -> bytes:
    return subprocess.check_output(['git', '-C', str(ROOT), *args], stderr=subprocess.PIPE)


def fingerprint(path: Path) -> dict:
    try:
        content = path.read_bytes()
        return {'path': str(path.resolve()), 'sha256': digest(content), 'bytes': len(content)}
    except OSError as exc:
        return {'path': str(path), 'sha256': None, 'error': type(exc).__name__}


def source_file(name: str) -> bool:
    path = Path(name)
    if path.parts[0] in {'entropy_arb', 'tools', 'tests', 'tests_v3', 'scripts'}:
        return path.suffix in {'.py', '.sh', '.toml', '.ini', '.cfg'}
    if name.startswith('dashboard/src/'):
        return path.suffix in {'.ts', '.tsx', '.js', '.css'}
    if len(path.parts) == 1:
        return (path.suffix == '.py' or name in {'pyproject.toml', 'pytest.ini', 'setup.cfg'}
                or (name.startswith('requirements') and path.suffix == '.txt')
                or (name.startswith('config.v3') and path.suffix == '.json'))
    return name in {'dashboard/package.json', 'dashboard/package-lock.json'}


def snapshot(config: Path, authorization: Path) -> dict:
    names = set(git('ls-files', '-z', '--cached', '--others', '--exclude-standard')
                .decode().split('\0'))
    # Include the executing script even if a user's ignore rules exclude tools.
    names.add(str(Path(__file__).resolve().relative_to(ROOT)))
    files = {}
    for name in sorted(filter(None, names)):
        if not source_file(name):
            continue
        path = ROOT / name
        if not path.resolve().is_relative_to(ROOT):
            files[name] = {'error': 'external_symlink_not_read', 'sha256': None}
        else:
            files[name] = fingerprint(path)
    inputs = {'v3_config': fingerprint(config), 'authorization': fingerprint(authorization)}
    payload = {'files': files, 'inputs': inputs}
    return {'captured_at': now(), 'source_hash': digest(json.dumps(
        payload, sort_keys=True).encode()), **payload,
        'complete': all(v.get('sha256') for v in [*files.values(), *inputs.values()])}


def command(argv: list[str], log: Path, config: Path, auth: Path) -> dict:
    before = snapshot(config, auth)
    started = now()
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONUNBUFFERED='1')
    # Do not let ambient pytest selection silently shrink the evidence suite.
    env.pop('PYTEST_ADDOPTS', None)
    with log.open('wb') as output:
        try:
            result = subprocess.run(argv, cwd=ROOT, env=env, stdout=output,
                                    stderr=subprocess.STDOUT, check=False)
            code = result.returncode
        except OSError as exc:
            output.write(f'COMMAND_FAILED: {type(exc).__name__}\n'.encode())
            code = 127
        except KeyboardInterrupt:
            output.write(b'\nINTERRUPTED: test execution is incomplete.\n')
            code = 130
    after = snapshot(config, auth)
    return {'argv': argv, 'cwd': str(ROOT), 'started_at': started, 'finished_at': now(),
            'exit_code': code, 'raw_output': fingerprint(log),
            'before': before, 'after': after,
            'source_stable': before['source_hash'] == after['source_hash'],
            'pytest_addopts': 'cleared; -o addopts= used; cacheprovider disabled'}


def offline_manifest(config: Path, auth_path: Path, pair: str) -> dict:
    # Only pure settings/authorization boundaries are imported. Never load_config.
    sys.path.insert(0, str(ROOT))
    from entropy_arb.v3.core import Settings
    from entropy_arb.v3.live_host import BatchAuthorization, canonical_config_hash

    content = config.read_bytes()
    raw = json.loads(content)
    settings = Settings.from_mapping(raw)
    auth_source = {}
    auth = BatchAuthorization.load(auth_path, expected_pair=pair,
                                   configured_max_notional=settings.max_notional_usd,
                                   provenance=auth_source)
    effective = replace(settings, max_notional_usd=min(
        settings.max_notional_usd, auth.max_notional_usd))
    policies = {}
    for purpose in ('open', 'close'):
        rooms, cap = settings.first_leg_execution_policy(purpose)
        sources = {}
        for suffix in ('rooms_bps', 'hard_cap_bps'):
            field = f'{purpose}_first_{suffix}'
            if getattr(settings, field) is None:
                field = f'first_{suffix}'
            sources[suffix] = {'field': field,
                               'origin': 'config' if field in raw else 'Settings.default'}
        policies[purpose] = {'rooms_bps': rooms, 'hard_cap_bps': cap, 'sources': sources}
    return {
        'evidence_kind': 'OFFLINE_RECONSTRUCTION_NOT_OBSERVED_LOADED_PROCESS',
        'pair_identity': pair, 'pair_source': 'CLI expected identity, checked against authorization',
        'deployment': pair.split(':', 1)[0],
        'v3_config_source': {'path': str(config), 'sha256': digest(content),
                             'evidence': 'parsed_bytes'},
        'authorization_source': auth_source, 'authorization': asdict(auth),
        'configured_settings': asdict(settings), 'settings': asdict(effective),
        'config_hash': canonical_config_hash(settings, pair_identity=pair),
        'effective_config_hash': canonical_config_hash(effective, pair_identity=pair),
        'execution_policies': policies,
        'notional_cap_source': 'min(config, validated batch authorization)',
        'observed_process_id': None, 'observed_start_ts': None,
        'observed_loaded_modules': None, 'observed_journal_schema': None,
        'session_max_new_cycles': None, 'observed_launch_environment': None,
        'limitations': ['No running process observed.', 'No production Journal opened.',
                        'No environment overrides or loaded bytecode attested.',
                        'Disk source manifest is separate from process evidence.'],
    }


def junit_results(path: Path, collected: list[str]) -> dict[str, str]:
    index = {}
    for node in collected:
        parts = node.split('::')
        classname = parts[0][:-3].replace('/', '.')
        if len(parts) > 2:
            classname += '.' + '.'.join(parts[1:-1])
        index[(classname, parts[-1])] = node
    results = {}
    severity = {'PASSED': 0, 'SKIPPED': 1, 'FAILED': 2, 'ERROR': 3}
    for case in ET.parse(path).iter('testcase'):
        node = index.get((case.get('classname'), case.get('name')))
        if node is None:
            continue
        outcome = ('ERROR' if case.find('error') is not None else
                   'FAILED' if case.find('failure') is not None else
                   'SKIPPED' if case.find('skipped') is not None else 'PASSED')
        if severity[outcome] >= severity.get(results.get(node), -1):
            results[node] = outcome
    return results


def regression_matrix(nodes: list[str], runs: dict, stable: bool, collection_ok: bool) -> list:
    rows = []
    for number, (description, pattern) in enumerate(CASES, 1):
        candidates = [node for node in nodes if node.startswith('tests_v3/') and
                      (re.search(pattern, node, re.I) or
                       re.search(rf'test_t{number:02d}_', node, re.I))]
        statuses = [outcome for run in runs.values() for node, outcome in
                    run.get('results', {}).items() if node in candidates]
        if not collection_ok:
            status = 'COLLECTION_INCOMPLETE'
        elif not stable:
            status = 'SOURCE_DRIFT_REVIEW_REQUIRED'
        elif not candidates:
            status = 'UNCOVERED'
        elif any(s in {'FAILED', 'ERROR'} for s in statuses):
            status = 'CANDIDATE_TEST_FAILED'
        elif not statuses:
            status = 'MAPPED_NOT_RUN'
        elif all(s == 'PASSED' for s in statuses) and all(
                node in run.get('results', {}) for run in runs.values()
                if run.get('status') == 'EXECUTED' for node in candidates):
            status = 'CANDIDATE_TESTS_PASSED_REVIEW_REQUIRED'
        else:
            status = 'PARTIAL_OR_SKIPPED_REVIEW_REQUIRED'
        rows.append({'id': f'T{number:02d}', 'requirement': description,
                     'status': status, 'acceptance': 'NOT_VERIFIED',
                     'mapping_method': 'collected_name_regex_candidates_only',
                     'pattern': pattern, 'candidate_nodeids': candidates,
                     'executed_outcomes': {key: {node: run.get('results', {}).get(node, 'NOT_RUN')
                                                for node in candidates}
                                           for key, run in runs.items()}})
    return rows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=Path('reports/entropy-edge-v2'))
    parser.add_argument('--v3-config', type=Path, default=ROOT / 'config.v3.edge.json')
    parser.add_argument('--authorization', type=Path, default=DEFAULT_AUTH)
    parser.add_argument('--pair-identity', default='default:hl:SNDK:lighter:SNDK')
    parser.add_argument('--run-tests', action='store_true', help='run V3, then tests + tests_v3')
    args = parser.parse_args(argv)
    sys.dont_write_bytecode = True
    output = args.output_dir
    output = (ROOT / output).resolve() if not output.is_absolute() else output.resolve()
    if ROOT == PRODUCTION.resolve() or output.is_relative_to(PRODUCTION.resolve()):
        parser.error('run from the isolated worktree and write outside the production directory')
    config = (ROOT / args.v3_config).resolve()
    auth = (ROOT / args.authorization).resolve()
    output.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    generated = []

    def write(name: str, value: dict):
        path = output / name
        path.write_text(json.dumps({'run_id': run_id, 'generated_at': now(),
                                    'LIVE_ACTIONS': 'NONE', **value}, indent=2,
                                   ensure_ascii=False, default=str) + '\n', encoding='utf-8')
        generated.append(path)

    initial = snapshot(config, auth)
    manifest_ok = True
    try:
        effective = offline_manifest(config, auth, args.pair_identity)
    except (OSError, ValueError, RuntimeError, ArithmeticError) as exc:
        manifest_ok = False
        # Do not echo arbitrary file contents or validation exception text.
        effective = {'evidence_kind': 'OFFLINE_RECONSTRUCTION_NOT_OBSERVED_LOADED_PROCESS',
                     'status': 'UNAVAILABLE', 'error_type': type(exc).__name__,
                     'config_path': str(config), 'authorization_path': str(auth)}
    write('effective_manifest.json', effective)
    pytest = [sys.executable, '-B', '-m', 'pytest', '-o', 'addopts=',
              '-p', 'no:cacheprovider', '--color=no']
    print('Collecting test names (no test bodies)...', flush=True)
    collection = command([*pytest, '--collect-only', '-q', 'tests', 'tests_v3'],
                         output / 'tests_collected.txt', config, auth)
    generated.append(output / 'tests_collected.txt')
    nodes = sorted(set(line.strip() for line in (output / 'tests_collected.txt')
                       .read_text(errors='replace').splitlines()
                       if re.match(r'^tests(?:_v3)?/[^\s]+\.py::', line.strip())))
    runs = {}
    interrupted = collection['exit_code'] == 130
    for name, targets in [('v3', ['tests_v3']), ('full', ['tests', 'tests_v3'])]:
        log = output / f'tests_final_{name}.txt'
        generated.append(log)
        if not args.run_tests or interrupted:
            reason = 'previous command interrupted' if interrupted else 'pass --run-tests'
            log.write_text(f'NOT_RUN: {reason}. run_id={run_id}\n', encoding='utf-8')
            runs[name] = {'status': 'NOT_RUN', 'exit_code': None, 'results': {},
                          'raw_output': fingerprint(log)}
            continue
        print(f'Running {name} tests; raw output: {log}', flush=True)
        xml = output / f'tests_final_{name}.{run_id}.xml'
        run = command([*pytest, '-vv', '-ra', f'--junitxml={xml}', *targets], log, config, auth)
        interrupted = run['exit_code'] == 130
        run['status'] = 'EXECUTED'
        run['results'] = {}
        if xml.exists():
            generated.append(xml)
            run['junit'] = fingerprint(xml)
            try:
                run['results'] = junit_results(xml, nodes)
            except ET.ParseError:
                run['junit_error'] = 'INVALID_XML'
        else:
            run['junit_error'] = 'MISSING_XML'
        run['outcome_counts'] = dict(Counter(run['results'].values()))
        runs[name] = run
    diff = command(['git', 'diff', '--check', 'HEAD'], output / 'diff_check.txt', config, auth)
    generated.append(output / 'diff_check.txt')
    final = snapshot(config, auth)
    captures = [initial, final, collection['before'], collection['after'],
                diff['before'], diff['after']]
    for run in runs.values():
        if 'before' in run:
            captures.extend([run['before'], run['after']])
    stable = len({capture['source_hash'] for capture in captures}) == 1
    complete = all(capture['complete'] for capture in captures)
    rows = regression_matrix(nodes, runs, stable, collection['exit_code'] == 0)
    write('regression_matrix.json', {'collection': collection, 'cases': rows,
                                    'limitation': 'Names suggest coverage; assertions require review.'})
    monitor_ids = {'T25', 'T27', 'T32', 'T33', 'T34', 'T35', 'T36', 'T37', 'T39'}
    write('monitoring_quality.json', {
        'synthetic_validation': {'evidence': 'isolated tests, not real market observations',
                                 'cases': [row for row in rows if row['id'] in monitor_ids]},
        'real_observation': {'status': 'MISSING', 'coverage_seconds': None,
                             'sample_count': None, 'gap_rate': None,
                             'dropped_events': None, 'latency_p50_ms': None,
                             'latency_p95_ms': None, 'profit_improvement': None},
        'economic_validation': 'PENDING', 'source_stable': stable,
        'limitations': ['No recorder data, production status, or account state read.',
                        'Synthetic success cannot establish live quality or profit.']})
    write('final_source_manifest.json', {
        'workspace': str(ROOT), 'git_head': git('rev-parse', 'HEAD').decode().strip(),
        'python': {'executable': sys.executable, 'version': sys.version},
        'scope': 'Git-visible Python, tests, scripts, V3 config, dependency and frontend source; '
                 'excludes credentials, connection config, logs, reports and generated assets.',
        'initial': initial, 'final': final, 'source_stable': stable,
        'fingerprints_complete': complete, 'effective_manifest_valid': manifest_ok,
        'commands': {'collect': collection, 'v3': runs['v3'], 'full': runs['full'], 'diff_check': diff},
        'diff_check_scope': 'tracked staged + unstaged vs HEAD; untracked files hashed, not diff-checked',
        'artifacts': {path.name: fingerprint(path) for path in generated},
        'acceptance': 'NOT_AUTOMATICALLY_VERIFIED',
    })
    failed = (not stable or not complete or not manifest_ok or collection['exit_code'] != 0
              or diff['exit_code'] != 0 or any(
                  run['exit_code'] != 0 or run.get('junit_error')
                  for run in runs.values() if run['status'] == 'EXECUTED'))
    print(json.dumps({'output_dir': str(output), 'run_id': run_id,
                      'source_stable': stable, 'run_tests': args.run_tests,
                      'status': 'NEEDS_REVIEW' if failed else 'EVIDENCE_GENERATED',
                      'acceptance': 'NOT_AUTOMATICALLY_VERIFIED'}, ensure_ascii=False))
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
