"""webull_paper.yml sparse checkout must hold everything the send job reads.

The job runs ``python3 -m src.webull_seal_gate``, ``python -m src.paper_open``
and ``python3 -m src.workflow_progress`` and lands with
``scripts/safe_git_push.sh``. A file outside the sparse list would be absent
on the runner, so the seal gate would fail closed and nothing would be sent.
These checks are static (imports, path constants, workflow paths) plus one
dynamic trace of the sealed-h1 plan build on a real sealed day.
"""
from __future__ import annotations

import ast
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / '.github' / 'workflows' / 'webull_paper.yml'


def sparse_patterns(text: str) -> list[str]:
    m = re.search(r'sparse-checkout: \|\n((?:[ \t]+/\S+\n)+)', text)
    assert m, 'webull_paper.yml has no sparse-checkout block'
    return [ln.strip() for ln in m.group(1).splitlines() if ln.strip()]


PATTERNS = sparse_patterns(WF.read_text())


def covered(rel: str) -> bool:
    rel = '/' + rel.lstrip('/')
    for p in PATTERNS:
        if p.endswith('/') and (rel.startswith(p) or rel + '/' == p):
            return True
        if rel == p:
            return True
    return False


def _modfile(mod: str) -> Path | None:
    base = ROOT / mod.replace('.', '/')
    for c in (base.with_suffix('.py'), base / '__init__.py'):
        if c.is_file():
            return c
    return None


def import_closure(entries) -> dict[str, Path]:
    seen: dict[str, Path] = {}
    todo = list(entries)
    while todo:
        mod = todo.pop()
        if mod in seen:
            continue
        f = _modfile(mod)
        if f is None:
            continue  # stdlib / site-packages
        seen[mod] = f
        pkg = mod if f.name == '__init__.py' else mod.rpartition('.')[0]
        for n in ast.walk(ast.parse(f.read_text(encoding='utf-8'))):
            if isinstance(n, ast.ImportFrom):
                base = n.module or ''
                if n.level:
                    parts = pkg.split('.')
                    parts = parts[:len(parts) - (n.level - 1)] if n.level > 1 else parts
                    base = '.'.join(parts + ([base] if base else []))
                todo.append(base)
                todo.extend(f'{base}.{a.name}' for a in n.names)
            elif isinstance(n, ast.Import):
                todo.extend(a.name for a in n.names)
    return seen


def entry_modules(text: str) -> list[str]:
    return sorted(set(re.findall(r'python3? -m (src\.[\w.]+)', text)))


def test_sparse_list_is_explicit_and_small():
    assert PATTERNS, 'empty sparse list'
    for p in PATTERNS:
        assert p.startswith('/'), p
        assert p not in ('/', '/data/', '/research/', '/01_daily/'), f'{p} is the full tree'
    text = WF.read_text()
    assert 'sparse-checkout-cone-mode: false' in text


def test_entrypoints_and_imports_inside_sparse():
    mods = entry_modules(WF.read_text())
    assert {'src.webull_seal_gate', 'src.paper_open', 'src.workflow_progress'} <= set(mods)
    closure = import_closure(mods)
    missing = sorted(str(f.relative_to(ROOT)) for f in closure.values()
                     if not covered(str(f.relative_to(ROOT))))
    assert not missing, f'imported by the send job but not in sparse list: {missing}'


def test_workflow_scripts_and_land_paths_inside_sparse():
    text = WF.read_text()
    paths = set(re.findall(r'bash (scripts/\S+)', text))
    for m in re.finditer(r'safe_git_push\.sh "[^"]*" ([^\n]+)', text):
        paths.update(m.group(1).split())
    paths.update(re.findall(r'path: (data/\S+)', text))
    assert paths
    missing = sorted(p for p in paths if not covered(p.rstrip('/') + ('/' if p.endswith('/') else '')))
    assert not missing, missing


def test_path_constants_inside_sparse():
    sys.path.insert(0, str(ROOT))
    from src import (h1_sealed_exec, paper_drift, paper_open, webull_exec,
                     webull_seal_gate, workflow_progress)
    paths = [
        ROOT / webull_seal_gate.H1_LOG,
        h1_sealed_exec.H1_LOG,
        paper_open.FLATTEN_FLAG,
        paper_drift.DRIFT_LOG,
        webull_exec.LAST_JSON,
        *webull_exec.HOT4_STRATEGY_PATHS,
        ROOT / 'data' / 'paper_open' / '2026-10-20_status.json',
        ROOT / 'data' / 'paper_open' / '2026-10-20_submit.json',
        ROOT / 'data' / 'day_board' / '2026-10-20_strategy_tickets.json',
        ROOT / 'data' / 'day_board' / 'today_strategies.json',
        workflow_progress.ROOT / '01_daily' / '_progress' / 'manifest.json',
        ROOT / '01_daily' / '_progress' / '2026-10-20_webull_paper.json',
        ROOT / 'requirements.txt',
    ]
    missing = [str(Path(p).relative_to(ROOT)) for p in paths
               if not covered(str(Path(p).resolve().relative_to(ROOT)))]
    assert not missing, missing


def _latest_sealed_day_with_tickets() -> str | None:
    import json
    log = ROOT / 'research/hot_n4_clean_v4/forward_h1/h1_log.jsonl'
    if not log.is_file():
        return None
    days = []
    for ln in log.read_text().splitlines():
        try:
            row = json.loads(ln)
        except ValueError:
            continue
        if row.get('kind') == 'plan' and row.get('date'):
            days.append(row['date'])
    for d in sorted(set(days), reverse=True):
        if (ROOT / f'data/day_board/{d}_strategy_tickets.json').is_file():
            return d
    return None


def test_sealed_plan_build_reads_only_sparse_files():
    day = _latest_sealed_day_with_tickets()
    if day is None:
        pytest.skip('no sealed h1 day with tickets in this checkout')
    sys.path.insert(0, str(ROOT))
    from src import paper_open as po, webull_seal_gate as gate
    from src.futubull_exec import BrokerSnap

    hits: set[str] = set()
    root = str(ROOT)
    on = [True]

    def rec(p):
        try:
            a = os.path.abspath(os.fsdecode(p))
        except (TypeError, ValueError):
            return
        if a.startswith(root + os.sep) and '__pycache__' not in a and '/.git/' not in a:
            hits.add(os.path.relpath(a, root))

    def hook(ev, args):
        if on[0] and ev in ('open', 'os.listdir', 'os.scandir') and args \
                and isinstance(args[0], (str, bytes, os.PathLike)):
            rec(args[0])

    sys.addaudithook(hook)
    real_stat = os.stat

    def stat(p, *a, **k):
        if on[0] and isinstance(p, (str, bytes, os.PathLike)):
            rec(p)
        return real_stat(p, *a, **k)

    os.stat = stat
    try:
        now = datetime.fromisoformat(f'{day}T08:50:00').replace(
            tzinfo=ZoneInfo('America/New_York'))
        assert gate.sealed_plan_today(day)
        body = po.load_local(day)
        plan = po.make_plan(body, BrokerSnap(env='paper', cash=1e7, positions={}),
                            now, allow_after_bell=True)
        assert plan['card'].get('tickets'), plan['card'].get('look_error')
    finally:
        on[0] = False
        os.stat = real_stat
    missing = sorted(h for h in hits if not covered(h) and not covered(h + '/'))
    assert not missing, f'send path read files outside sparse list: {missing}'
