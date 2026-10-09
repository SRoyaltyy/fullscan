"""h1 seal refuses while the general predict is missing.

Missing predict: no plan line, forward.main() is never called, exit 1.
Predict present: forward.main() runs as before and its exit code is kept.
Holdup and non-plan modes never see the check. The sealed h1 log on disk,
including 2026-10-09 (ledger entry 162), is byte-identical after the test.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import h1_seal_gate  # noqa: E402

SESSION = "2026-10-12"
H1_DIR = ROOT / "research/hot_n4_clean_v4/forward_h1"
SEALED = sorted(H1_DIR.glob("*.jsonl")) + sorted((ROOT / "research/hot_n4_clean_v4/forward").glob("*.jsonl"))


def _repo(tmp: Path, with_predict: bool) -> Path:
    repo = tmp / ("with" if with_predict else "without")
    repo.mkdir()

    def git(*args: str) -> None:
        subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)

    git("init", "-q")
    git("config", "user.email", "t@t")
    git("config", "user.name", "t")
    (repo / "README").write_text("x\n")
    if with_predict:
        rel = repo / h1_seal_gate.predict_rel(SESSION)
        rel.parent.mkdir(parents=True)
        rel.write_text("Prediction: UP total score 1.0\n")
    git("add", "-A")
    git("commit", "-q", "-m", "seed")
    return repo


class _Env:
    def __init__(self, **values: str):
        self.values = values
        self.saved: dict[str, str | None] = {}

    def __enter__(self):
        for key, value in self.values.items():
            self.saved[key] = os.environ.get(key)
            os.environ[key] = value
        return self

    def __exit__(self, *exc):
        for key, value in self.saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _fake_forward(log: Path):
    calls = []

    def run() -> int:
        calls.append(1)
        with log.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"date": SESSION, "kind": "plan"}) + "\n")
        return 0

    return run, calls


def _plan_lines(log: Path) -> list[str]:
    if not log.exists():
        return []
    return [line for line in log.read_text().splitlines() if '"kind": "plan"' in line]


def test_missing_predict_writes_no_plan_line():
    with tempfile.TemporaryDirectory() as tmp, _Env(FORWARD_BOOK="h1", HOLDUP_MODE="plan"):
        tmp = Path(tmp)
        repo = _repo(tmp, with_predict=False)
        log = tmp / "h1_log.jsonl"
        run, calls = _fake_forward(log)
        code = h1_seal_gate.main(run=run, target=lambda: SESSION, repo=repo, ref="HEAD")
        assert code == 1, code
        assert calls == [], "forward.main ran with the predict missing"
        assert _plan_lines(log) == []
        assert not log.exists()
        why = h1_seal_gate.predict_missing(SESSION, repo, "HEAD")
        assert why and f"01_daily/general/{SESSION}_predict.md" in why and "stays missing" in why


def test_present_predict_runs_forward_unchanged():
    with tempfile.TemporaryDirectory() as tmp, _Env(FORWARD_BOOK="h1", HOLDUP_MODE="plan"):
        tmp = Path(tmp)
        repo = _repo(tmp, with_predict=True)
        log = tmp / "h1_log.jsonl"
        run, calls = _fake_forward(log)
        assert h1_seal_gate.predict_missing(SESSION, repo, "HEAD") is None
        code = h1_seal_gate.main(run=run, target=lambda: SESSION, repo=repo, ref="HEAD")
        assert code == 0
        assert calls == [1]
        assert len(_plan_lines(log)) == 1
        assert h1_seal_gate.main(run=lambda: 7, target=lambda: SESSION, repo=repo, ref="HEAD") == 7


def test_no_target_and_other_books_delegate():
    with tempfile.TemporaryDirectory() as tmp:
        repo = _repo(Path(tmp), with_predict=False)
        with _Env(FORWARD_BOOK="h1", HOLDUP_MODE="plan"):
            assert h1_seal_gate.main(run=lambda: 5, target=lambda: None, repo=repo, ref="HEAD") == 5
        with _Env(FORWARD_BOOK="holdup", HOLDUP_MODE="plan"):
            assert h1_seal_gate.main(run=lambda: 0, target=lambda: SESSION, repo=repo, ref="HEAD") == 0
        for mode in ("open_fill", "fill", "correct_open", "backfill"):
            with _Env(FORWARD_BOOK="h1", HOLDUP_MODE=mode):
                assert h1_seal_gate.main(run=lambda: 0, target=lambda: SESSION, repo=repo, ref="HEAD") == 0


def test_already_sealed_session_is_left_to_forward():
    # 2026-10-09 is sealed as ledger entry 162. The gate must not resolve it
    # as a seal target, so it never refuses or rewrites a sealed day.
    with _Env(FORWARD_BOOK="h1", HOLDUP_MODE="plan", PLAN_SESSION="2026-10-09"):
        assert h1_seal_gate.seal_target() is None


def test_check_mode_and_workflow_wiring():
    with tempfile.TemporaryDirectory() as tmp, _Env(FORWARD_BOOK="h1", HOLDUP_MODE="plan"):
        tmp = Path(tmp)
        absent = _repo(tmp, with_predict=False)
        present = _repo(tmp, with_predict=True)
        assert h1_seal_gate.check(target=lambda: SESSION, repo=absent, ref="HEAD") == 1
        assert h1_seal_gate.check(target=lambda: SESSION, repo=present, ref="HEAD") == 0
    text = (ROOT / ".github/workflows/h1_forward.yml").read_text(encoding="utf-8")
    gate = "python3 research/hot_n4_clean_v4/forward/h1_seal_gate.py --check"
    seal = "python3 research/hot_n4_clean_v4/forward/forward.py"
    assert text.count(gate) == 1 and text.count(seal) == 1
    assert "set -euo pipefail" in text[text.index("name: Seal the pre-open plan"):text.index(seal)]
    assert text.index(gate) < text.index(seal)


def main() -> None:
    before = {path: path.read_bytes() for path in SEALED}
    try:
        test_missing_predict_writes_no_plan_line()
        test_present_predict_runs_forward_unchanged()
        test_no_target_and_other_books_delegate()
        test_already_sealed_session_is_left_to_forward()
        test_check_mode_and_workflow_wiring()
    finally:
        for path, raw in before.items():
            if path.read_bytes() != raw:
                raise SystemExit(f"sealed file changed {path.relative_to(ROOT)}")
    print("h1 seal gate: missing predict writes no plan; present predict seals as before")


if __name__ == "__main__":
    main()
