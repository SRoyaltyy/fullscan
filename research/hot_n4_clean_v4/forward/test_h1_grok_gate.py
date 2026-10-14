"""h1 seal refuses unless Grok wrote the general predict (pending Cyrus).

Grok-written (grok_ok=true, trace names xai/grok): forward runs.
glm-5.3-flash-written: refuses, even with grok_ok=true (the 10-13 shape).
Missing status / trace / grok_ok=false: refuses. Refusal writes nothing.
Sealed logs on disk are byte-identical after the test.
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

from research.hot_n4_clean_v4.forward import h1_grok_gate as g  # noqa: E402

SESSION = "2026-10-15"
SEALED = sorted((ROOT / "research/hot_n4_clean_v4/forward_h1").glob("*.jsonl")) + sorted(
    (ROOT / "research/hot_n4_clean_v4/forward").glob("*.jsonl")
)
GROK = "**Step 0 — Setup.** Loaded 1 characters. Provider: OpenClaw gateway, backend model `xai/grok-4.6`.\n"
GLM = "**Step 0 — Setup.** Loaded 1 characters. Model: `glm-5.3-flash`. Web search is ENABLED.\n"


def _repo(tmp: Path, name: str, status: dict | None, trace: str | None) -> Path:
    repo = tmp / name
    repo.mkdir()

    def git(*args: str) -> None:
        subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)

    git("init", "-q")
    git("config", "user.email", "t@t")
    git("config", "user.name", "t")
    (repo / "README").write_text("x\n")
    if status is not None:
        p = repo / g.status_rel(SESSION)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(status))
    if trace is not None:
        p = repo / g.trace_rel(SESSION)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(trace)
    git("add", "-A")
    git("commit", "-q", "-m", "seed")
    return repo


class _Env:
    def __init__(self, **values: str):
        self.values, self.saved = values, {}

    def __enter__(self):
        for k, v in self.values.items():
            self.saved[k] = os.environ.get(k)
            os.environ[k] = v
        return self

    def __exit__(self, *exc):
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _check(repo: Path) -> int:
    return g.check(target=lambda: SESSION, repo=repo, ref="HEAD")


def test_grok_written_seals():
    with tempfile.TemporaryDirectory() as tmp, _Env(FORWARD_BOOK="h1", HOLDUP_MODE="plan"):
        repo = _repo(Path(tmp), "grok", {"grok_ok": True}, GROK)
        assert g.not_grok(SESSION, repo, "HEAD") is None
        assert _check(repo) == 0


def test_glm_flash_written_refuses():
    with tempfile.TemporaryDirectory() as tmp, _Env(FORWARD_BOOK="h1", HOLDUP_MODE="plan"):
        tmp = Path(tmp)
        for name, ok in (("glm_ok", True), ("glm_false", False)):
            repo = _repo(tmp, name, {"grok_ok": ok}, GLM)
            assert _check(repo) == 1, name
            why = g.not_grok(SESSION, repo, "HEAD")
            assert why and "stays missing" in why
        assert "glm-5.3-flash" in g.not_grok(SESSION, tmp / "glm_ok", "HEAD")


def test_missing_refuses():
    with tempfile.TemporaryDirectory() as tmp, _Env(FORWARD_BOOK="h1", HOLDUP_MODE="plan"):
        tmp = Path(tmp)
        cases = {
            "nothing": (None, None),
            "no_status": (None, GROK),
            "no_trace": ({"grok_ok": True}, None),
            "grok_false": ({"grok_ok": False}, GROK),
            "no_key": ({}, GROK),
            "no_model_line": ({"grok_ok": True}, "# trace\n"),
        }
        for name, (status, trace) in cases.items():
            assert _check(_repo(tmp, name, status, trace)) == 1, name


def test_other_books_and_modes_and_no_target_pass():
    with tempfile.TemporaryDirectory() as tmp:
        repo = _repo(Path(tmp), "empty", None, None)
        with _Env(FORWARD_BOOK="h1", HOLDUP_MODE="plan"):
            assert g.check(target=lambda: None, repo=repo, ref="HEAD") == 0
        with _Env(FORWARD_BOOK="holdup", HOLDUP_MODE="plan"):
            assert _check(repo) == 0
        for mode in ("open_fill", "fill", "correct_open", "backfill"):
            with _Env(FORWARD_BOOK="h1", HOLDUP_MODE=mode):
                assert _check(repo) == 0


def test_real_traces_parse():
    assert g.trace_model(GROK) == "xai/grok-4.6"
    assert g.trace_model(GLM) == "glm-5.3-flash"


def test_workflow_wiring():
    text = (ROOT / ".github/workflows/h1_forward.yml").read_text(encoding="utf-8")
    pre = "python3 research/hot_n4_clean_v4/forward/h1_fill_missing.py research/hot_n4_clean_v4/forward/"
    gate, grok, seal = pre + "h1_seal_gate.py --check", pre + "h1_grok_gate.py --check", pre + "forward.py"
    assert text.count(grok) == 1
    assert text.index(gate) < text.index(grok) < text.index(seal)
    assert "set -euo pipefail" in text[text.index("name: Seal the pre-open plan"):text.index(grok)]


def main() -> None:
    before = {p: p.read_bytes() for p in SEALED}
    try:
        test_grok_written_seals()
        test_glm_flash_written_refuses()
        test_missing_refuses()
        test_other_books_and_modes_and_no_target_pass()
        test_real_traces_parse()
        test_workflow_wiring()
    finally:
        for p, raw in before.items():
            if p.read_bytes() != raw:
                raise SystemExit(f"sealed file changed {p.relative_to(ROOT)}")
    print("h1 grok gate: grok seals; glm-5.3-flash and missing refuse")


if __name__ == "__main__":
    main()
