"""Refuse the h1 pre-open seal unless Grok wrote the day's general predict.

PENDING CYRUS (2026-10-14). Since #597 a free ``glm-5.3-flash`` predict
can land on main when Grok comes back empty. Until Cyrus rules whether a
flash predict counts for h1, this gate holds it as not sealable.

h1_forward.yml runs ``h1_grok_gate.py --check`` in the plan step, after
``h1_seal_gate.py --check`` (presence) and before forward.py. forward.py and
every file pinned in protocol.ENGINE_SHA256 are unchanged.

For FORWARD_BOOK=h1 and HOLDUP_MODE=plan, the session the run would seal
(h1_seal_gate.seal_target) may seal only when BOTH hold on HOLDUP_GIT_REF:
  * 01_daily/<D>_preopen_status.json has ``grok_ok`` == true, and
  * 01_daily/general/<D>_predict_trace.md names a Grok model in its setup
    line (``Model: `...` `` or ``backend model `...` ``).
Both are needed: 2026-10-13 had grok_ok=true but a glm-5.3-flash trace.
Otherwise it prints the reason and exits 1, writing nothing, exactly like
the missing-predict path: no plan line, no missing line, the day stays
missing and Webull (which needs a sealed plan and a success) does nothing.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import forward  # noqa: E402
from research.hot_n4_clean_v4.forward.book import H1, current_book  # noqa: E402

_MODEL = re.compile(r"(?i)\bmodel:?\s*`([^`]+)`")


def status_rel(session: str) -> str:
    return f"01_daily/{session}_preopen_status.json"


def trace_rel(session: str) -> str:
    return f"01_daily/general/{session}_predict_trace.md"


def _show(repo: Path, ref: str, rel: str) -> str | None:
    out = subprocess.run(["git", "-C", str(repo), "show", f"{ref}:{rel}"], capture_output=True)
    if out.returncode != 0:
        return None
    return out.stdout.decode("utf-8", "replace")


def trace_model(text: str) -> str | None:
    m = _MODEL.search(text)
    return m.group(1).strip() if m else None


def reason_from(session: str, status_raw: str | None, trace_text: str | None,
                where: str = "main", book: str = "h1", tail: str = "") -> str | None:
    """Two-part check on already-read text. None when Grok wrote the predict.

    Shared by the h1 seal (this file) and the Factor Mine seal wrapper
    (src/fm_grok_gate.py) so both books apply the identical rule.
    """
    tail = f" {tail}" if tail else ""
    if status_raw is None:
        return f"{status_rel(session)} is not on {where}; cannot confirm grok_ok.{tail}"
    try:
        ok = json.loads(status_raw).get("grok_ok")
    except (ValueError, AttributeError):
        return f"{status_rel(session)} does not parse; cannot confirm grok_ok.{tail}"
    if ok is not True:
        return f"{status_rel(session)} has grok_ok={ok!r}; the {book} seal needs grok_ok=true.{tail}"
    if trace_text is None:
        return f"{trace_rel(session)} is not on {where}; cannot confirm the predict model.{tail}"
    model = trace_model(trace_text)
    if not model or "grok" not in model.lower():
        return f"{trace_rel(session)} names model {model or 'none'}, not Grok; the {book} seal needs a Grok predict.{tail}"
    return None


def not_grok(session: str, repo: Path = ROOT, ref: str | None = None) -> str | None:
    """None when Grok wrote the predict for ``session`` on ``ref``; else the reason."""
    ref = ref or forward.GIT_REF
    tail = f"No plan line written; {session} stays missing"
    return reason_from(session, _show(repo, ref, status_rel(session)),
                       _show(repo, ref, trace_rel(session)), ref, "h1", tail + ".")


def check(
    target: Callable[[], str | None] | None = None,
    repo: Path = ROOT,
    ref: str | None = None,
) -> int:
    """0 when the plan step may continue; 1 when the h1 seal must refuse."""
    if os.environ.get("HOLDUP_MODE", "plan") != "plan" or current_book() != H1:
        return 0
    if target is None:
        from research.hot_n4_clean_v4.forward.h1_seal_gate import seal_target as target
    session = target()
    if session is None:
        return 0
    why = not_grok(session, repo, ref)
    if why:
        print(f"REFUSING: {why}", file=sys.stderr, flush=True)
        print(f"::error::h1 seal refused for {session}: {why}", flush=True)
        return 1
    print(f"{session} predict was written by Grok (grok_ok=true); sealing h1 as before", flush=True)
    return 0


if __name__ == "__main__":
    if sys.argv[1:] != ["--check"]:
        print("usage: h1_grok_gate.py --check", file=sys.stderr)
        raise SystemExit(2)
    try:
        raise SystemExit(check())
    except RuntimeError as exc:
        forward._fail(str(exc))
        raise SystemExit(1)
