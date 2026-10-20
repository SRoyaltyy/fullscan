"""openclaw_oauth_guard.yml sparse checkout must hold every repo file the job uses.

Run 39377458959 timed out checking out the full 26.8k-file tree on ECS, and
its status push failed because scripts/safe_git_push.sh was absent.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / '.github' / 'workflows' / 'openclaw_oauth_guard.yml'
TEXT = WF.read_text()


def patterns() -> list[str]:
    m = re.search(r'sparse-checkout: \|\n((?:[ \t]+/\S+\n)+)', TEXT)
    assert m, 'no sparse-checkout block'
    return [ln.strip() for ln in m.group(1).splitlines() if ln.strip()]


def covered(rel: str) -> bool:
    rel = '/' + rel.lstrip('/')
    return any(rel == p or (p.endswith('/') and rel.startswith(p)) for p in patterns())


def test_every_checkout_is_sparse_and_shallow():
    n = TEXT.count('uses: actions/checkout@')
    assert n >= 1
    assert TEXT.count('sparse-checkout: |') == n
    assert TEXT.count('fetch-depth: 1') >= n


def test_files_run_by_workflow_are_listed():
    used = set(re.findall(r'(?<![\w/])((?:scripts|src)/[\w./-]+\.(?:py|sh))', TEXT))
    used |= {m.replace('.', '/') + '.py' for m in re.findall(r'-m (src\.\w+)', TEXT)}
    used |= {'scripts/safe_git_push.sh', '01_daily/_openclaw_oauth_status.json', 'src/__init__.py'}
    for rel in sorted(used):
        assert covered(rel), f'{rel} used by the workflow but not in sparse list'
        if not rel.startswith('01_daily/'):
            assert (ROOT / rel).is_file(), rel


def test_helper_scripts_listed():
    guard = (ROOT / 'scripts' / 'openclaw_oauth_guard.py').read_text()
    for rel in re.findall(r'"scripts"\s*/\s*"([\w.-]+)"', guard):
        assert covered('scripts/' + rel), rel
    inst = (ROOT / 'scripts' / 'install_openclaw_oauth_timer.sh').read_text()
    for rel in re.findall(r'\$SRC/([\w./-]+)', inst):
        assert covered('scripts/' + rel), rel


def test_guard_imports_no_repo_modules():
    for f in ('scripts/openclaw_oauth_guard.py', 'src/workflow_progress.py'):
        src = (ROOT / f).read_text()
        assert not re.search(r'^\s*(from|import) (src|collectors|db)\b', src, re.M), f
