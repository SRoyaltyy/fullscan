"""Refuse the h1 pre-open seal while the general predict is missing.

h1_forward.yml runs ``h1_seal_gate.py --check`` right before forward.py
in the plan step. forward.py is unchanged, so holdup and every other mode
keep their behavior. On 2026-10-09 h1 sealed at 08:10 ET while
01_daily/general/2026-10-09_predict.md was not on main, which left a
sealed plan that could not trade.

For FORWARD_BOOK=h1 and HOLDUP_MODE=plan: when the session this run would
seal has no 01_daily/general/<session>_predict.md on HOLDUP_GIT_REF, print
the reason and exit 1. Nothing is written: no plan line, no missing line,
no skip line, no page. The day stays missing, and a later run inside the
08:00-09:30 ET window can still seal once the predict lands.
``--check`` only decides (exit 0 go, exit 1 refuse). Without it, the file
wraps forward.main() the same way.

The check is presence only. It does not read or score the file. When the
predict is present, forward.main() runs exactly as before. Every case
where forward.py would write nothing anyway (ledger does not load, a plan
is still open, the session is already sealed, too early, no session) also
goes straight to forward.main() so its own message and exit code are kept.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import forward  # noqa: E402
from research.hot_n4_clean_v4.forward.book import H1, current_book  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import kind_on, load, open_plan, plan_on  # noqa: E402


def predict_rel(session: str) -> str:
    return f"01_daily/general/{session}_predict.md"


def predict_missing(session: str, repo: Path = ROOT, ref: str | None = None) -> str | None:
    """None when the general predict for ``session`` is on ``ref``; else the reason."""
    ref = ref or forward.GIT_REF
    rel = predict_rel(session)
    found = subprocess.run(
        ["git", "-C", str(repo), "cat-file", "-e", f"{ref}:{rel}"],
        capture_output=True,
    )
    if found.returncode == 0:
        return None
    return (
        f"{rel} is not on {ref}; the h1 seal refuses while the general predict "
        f"is missing. No plan line written; {session} stays missing"
    )


def seal_target() -> str | None:
    """The session forward.plan_main() would seal now, or None when it would not seal."""
    try:
        records = load()
    except RuntimeError:
        return None
    if open_plan(records) is not None:
        return None
    requested = os.environ.get("PLAN_SESSION", "").strip()
    try:
        target, why = forward.plan_target(records, requested)
    except RuntimeError:
        return None
    if why or target is None:
        return None
    if plan_on(records, target) is not None or kind_on(records, target, "missing") is not None:
        return None
    if forward.plan_too_early(target):
        return None
    return target


def check(
    target: Callable[[], str | None] | None = None,
    repo: Path = ROOT,
    ref: str | None = None,
) -> int:
    """0 when the plan step may run forward.py; 1 when the h1 seal must refuse."""
    target = target or seal_target
    if os.environ.get("HOLDUP_MODE", "plan") != "plan" or current_book() != H1:
        return 0
    session = target()
    if session is None:
        return 0
    why = predict_missing(session, repo, ref)
    if why:
        print(f"REFUSING: {why}", file=sys.stderr, flush=True)
        print(f"::error::h1 seal refused for {session}: {why}", flush=True)
        return 1
    print(f"{predict_rel(session)} is on {ref or forward.GIT_REF}; sealing h1 as before", flush=True)
    return 0


def main(
    run: Callable[[], int] | None = None,
    target: Callable[[], str | None] | None = None,
    repo: Path = ROOT,
    ref: str | None = None,
) -> int:
    run = run or forward.main
    code = check(target, repo, ref)
    if code:
        return code
    return run()


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["--check"]:
            raise SystemExit(check())
        if sys.argv[1:]:
            forward._fail(f"unknown arguments {sys.argv[1:]}")
            raise SystemExit(2)
        raise SystemExit(main())
    except RuntimeError as exc:
        forward._fail(str(exc))
        raise SystemExit(1)
