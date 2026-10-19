"""h1 post-close fill when nothing was sent: an add-only fill_missing note. Wrapper, not engine.

On 2026-10-09, 10-13 and 10-16 no Webull orders went out, so there was no
sent share lock (``data/paper_open/<D>_submit.json``). The post-close fill
then refused ("sent share lock missing ... refusing to size buys from the
open"), failed the job, and left the plan open, which blocked every later
h1 seal until a hand-written note landed (#577).

This wrapper runs ``forward.py`` (HOLDUP_MODE=fill) unchanged except for one
case. When the share lock is missing because nothing was sent, it appends
one ``fill_missing`` note to ``forward_h1/fill_missing_notes.jsonl``, logs
the skip, and exits 0. "Nothing was sent" means all of:

* the h1 book, HOLDUP_MODE=fill, and the open plan has no open fill;
* the refusal is the missing-journal one (a journal that exists but is
  unreadable or disagrees with the sent rows still refuses);
* no submit journal for that date, and the paper status file, when it
  exists, does not say ``submit: true``.

LEDGER.jsonl, h1_log.jsonl and every sealed line stay byte-identical. The
plan stays exactly as sealed, the session stays missing. No backfill, no
Webull. Engine and forward.py are not edited.

Run: ``h1_fill_missing.py h1_not_sent.py`` with FORWARD_BOOK=h1 HOLDUP_MODE=fill.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward import h1_fill_missing, ledger, openfill, share_lock  # noqa: E402
from research.hot_n4_clean_v4.forward.book import H1, current_book  # noqa: E402

ET = ZoneInfo("America/New_York")
DECIDED_BY = "h1_fill auto: nothing sent (h1_not_sent.py)"
_ORIG_DECIDE_FILL = openfill.decide_fill
PENDING: list[dict] = []


def missing_reason(date: str) -> str:
    return f"sent share lock missing for {date}; refusing to size buys from the open"


def status_path(date: str) -> Path:
    return share_lock.journal_path(date).parent / f"{date}_status.json"


def nothing_sent(date: str) -> str | None:
    """Evidence that no order went out on ``date``, or None when it cannot be shown."""
    journal = share_lock.journal_path(date)
    if journal.exists():
        return None
    status = status_path(date)
    if not status.is_file():
        return f"no {journal.name} and no {status.name}"
    try:
        payload = json.loads(status.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or payload.get("submit") is not False:
        return None
    for event in payload.get("events") or []:
        if isinstance(event, dict) and event.get("submit") is True:
            return None
    return f"no {journal.name}; {status.name} status {payload.get('status')!s} submit false"


def build_note(records: list[dict], plan: dict, evidence: str, now: datetime | None = None) -> dict:
    day = plan["date"]
    seq = next(n for n, row in enumerate(records, 1) if row is plan or (row["kind"] == "plan" and row["date"] == day))
    now = now or datetime.now(ET)
    return {
        "date": day,
        "decided_by": DECIDED_BY,
        "decided_on": now.astimezone(ET).date().isoformat(),
        "kind": "fill_missing",
        "no_backfill": True,
        "no_webull": True,
        "plan_seq": seq,
        "plan_sha256": plan["sha256"],
        "reason": (
            f"not sent, fill missing: sealed {day} plan never got its open fill: "
            f"{missing_reason(day)} ({evidence}). The plan stays exactly as sealed; "
            "the session stays missing."
        ),
        "recipe": H1.recipe,
    }


def decide_fill(records, bars, fees, index):
    try:
        return _ORIG_DECIDE_FILL(records, bars, fees, index)
    except share_lock.SentShareLockMissing as exc:
        if current_book() != H1 or os.environ.get("HOLDUP_MODE") != "fill":
            raise
        pending = openfill.open_plan(records)
        if pending is None or ledger.effective_open_fill(records, pending["date"]) is not None:
            raise
        day = pending["date"]
        if str(exc) != missing_reason(day):
            raise
        evidence = nothing_sent(day)
        if evidence is None:
            raise
        PENDING.append(build_note(records, pending, evidence))
        return [], f"not sent, fill missing: {exc}; {evidence}; add-only fill_missing note"


def append_notes(notes: list[dict], path: Path | None = None) -> list[dict]:
    """Append notes not already on file. Never rewrites a byte already there."""
    path = path or h1_fill_missing.notes_path()
    before = path.read_bytes() if path.is_file() else b""
    have = {(n["date"], n["kind"]) for n in h1_fill_missing.load_notes(path)}
    fresh = [n for n in notes if (n["date"], n["kind"]) not in have]
    if not fresh:
        return []
    lines = b"".join(
        json.dumps(n, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
        for n in fresh
    )
    if before and not before.endswith(b"\n"):
        raise RuntimeError(f"{path.name} does not end with a newline")
    with path.open("ab") as fh:
        fh.write(lines)
    if not path.read_bytes().startswith(before):
        raise RuntimeError(f"{path.name} prefix changed")
    return fresh


def install() -> None:
    from research.hot_n4_clean_v4.forward import forward

    # The workflow runs this file through ``h1_fill_missing.py``, which has
    # already installed the notes (as its own __main__ module). Installing
    # again would wrap ledger._note twice, so only install when it is bare.
    if getattr(ledger._note, "__module__", None) == ledger.__name__:
        h1_fill_missing.install()
    if openfill.decide_fill is _ORIG_DECIDE_FILL:
        openfill.decide_fill = decide_fill
    if forward.decide_fill is _ORIG_DECIDE_FILL:
        forward.decide_fill = decide_fill


def uninstall() -> None:
    from research.hot_n4_clean_v4.forward import forward

    if openfill.decide_fill is decide_fill:
        openfill.decide_fill = _ORIG_DECIDE_FILL
    if forward.decide_fill is decide_fill:
        forward.decide_fill = _ORIG_DECIDE_FILL
    PENDING.clear()


def main() -> int:
    from research.hot_n4_clean_v4.forward import forward

    install()
    try:
        code = forward.main()
    except RuntimeError as exc:
        forward._fail(str(exc))
        return 1
    if code != 0 or not PENDING:
        return code
    written = append_notes(list(PENDING))
    for note in written:
        print(f"add-only fill_missing note {note['date']} plan seq {note['plan_seq']}: {note['reason']}", flush=True)
    records = ledger.load()
    h1_fill_missing.check_notes(records, h1_fill_missing.load_notes())
    still = forward.open_plan(records)
    if still is not None:
        forward._fail(f"plan {still['date']} is still open after the note")
        return 1
    print("no open h1 plan; the next session can seal", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
