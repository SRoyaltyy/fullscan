"""ECS 09:12 ET decision for the sealed-h1 Webull paper backstop.

The systemd timer calls this before ``python -m src.paper_open``.
This module does not place orders, does not talk to Webull, and does
not rebuild a HOT4 list. The send list stays the sealed h1 plan inside
``paper_open`` / ``webull_exec`` (fail closed when that list is not the
sealed new-buy set).

A submit journal on ``origin/main``, or a status that already records
an attempt, means this session was tried. ``missed_deadline`` and
``blocked`` are not attempts: the 2026-10-06 manual run wrote
``missed_deadline`` with no ``_submit.json``.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.h1_sealed_exec import SealedH1Error, load_sealed_h1_plan

ET = ZoneInfo("America/New_York")
# Status values that mean a sandbox batch was already attempted.
# missed_deadline / blocked / not_ready / broker_unavailable are not.
ATTEMPTED_STATUSES = frozenset({
    "acknowledged",
    "no_trade",
    "dry_run",
    "failed",
    "releasing",
})


def _et(clock: datetime) -> datetime:
    if clock.tzinfo is None:
        return clock.replace(tzinfo=ET)
    return clock.astimezone(ET)


def is_late(clock: datetime) -> bool:
    """True when ``clock`` is after 09:25 ET (09:25:00.000000 is on time)."""
    current = _et(clock)
    cutoff = current.replace(hour=9, minute=25, second=0, microsecond=0)
    return current > cutoff


def session_date(clock: datetime) -> str:
    return _et(clock).date().isoformat()


def _load_json(path: Path | None) -> tuple[dict | None, str]:
    """Return (object, error). Missing path is (None, '')."""
    if path is None:
        return None, ""
    path = Path(path)
    if not path.is_file():
        return None, ""
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"unreadable {path.name}: {exc}"
    if not isinstance(doc, dict):
        return None, f"{path.name} is not an object"
    return doc, ""


def seal_state(date: str, log_path: Path | None) -> tuple[bool, str]:
    """(sealed, error). A bad log refuses; a missing plan is simply unsealed."""
    if log_path is None:
        return False, "no h1 log"
    try:
        plan = load_sealed_h1_plan(date, log_path=Path(log_path))
    except SealedH1Error as exc:
        return False, str(exc)
    if plan is None:
        return False, ""
    return True, ""


def already_attempted(journal: dict | None, status: dict | None) -> bool:
    """True when main already has a submit journal or an attempt status."""
    if isinstance(journal, dict) and journal.get("date"):
        return True
    if isinstance(status, dict) and status.get("status") in ATTEMPTED_STATUSES:
        return True
    return False


def choose_action(*, sealed: bool, submitted: bool, log_error: str) -> str:
    """What the timer should do. ``send`` is the only broker path."""
    if log_error:
        return "refused"
    if not sealed:
        return "skipped_unsealed"
    if submitted:
        return "skipped_already_submitted"
    return "send"


def build_record(
    *,
    date: str,
    started_at: str,
    late: bool,
    action: str,
    result: str,
    sealed: bool,
    submitted: bool,
    note: str = "",
) -> dict:
    rec = {
        "date": date,
        "started_at": started_at,
        "late": bool(late),
        "action": action,
        "result": result,
        "sealed": bool(sealed),
        "submitted_before": bool(submitted),
        "env": "paper",
        "live": False,
        "owner": "actions",
        "host": "sandbox",
    }
    if note:
        rec["note"] = note
    return rec


def decide(clock: datetime, log_path: Path | None,
           journal_path: Path | None = None,
           status_path: Path | None = None) -> dict:
    """Pure decision from a clock and on-disk copies of origin/main."""
    current = _et(clock)
    date = current.date().isoformat()
    started_at = current.isoformat()
    late = is_late(current)
    sealed, log_error = seal_state(date, log_path)
    journal, journal_error = _load_json(journal_path)
    status, status_error = _load_json(status_path)
    read_error = journal_error or status_error
    if read_error:
        action = "refused"
        submitted = False
        note = read_error
    else:
        submitted = already_attempted(journal, status)
        action = choose_action(
            sealed=sealed, submitted=submitted, log_error=log_error)
        note = log_error
    result = "pending" if action == "send" else action
    if note and action == "refused":
        result = note
    return build_record(
        date=date,
        started_at=started_at,
        late=late,
        action=action,
        result=result,
        sealed=sealed,
        submitted=submitted,
        note=note,
    )


def _parse_clock(text: str) -> datetime:
    raw = datetime.fromisoformat(text)
    return _et(raw)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="ECS paper backstop decision")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("decide")
    d.add_argument("--started-at", required=True)
    d.add_argument("--log", required=True)
    d.add_argument("--journal", default="")
    d.add_argument("--status", default="")

    w = sub.add_parser("record")
    w.add_argument("--out", required=True)
    w.add_argument("--started-at", required=True)
    w.add_argument("--action", required=True)
    w.add_argument("--result", required=True)
    w.add_argument("--late", required=True, choices=("true", "false"))
    w.add_argument("--sealed", required=True, choices=("true", "false"))
    w.add_argument("--submitted", required=True, choices=("true", "false"))
    w.add_argument("--note", default="")

    args = p.parse_args(argv)
    if args.cmd == "decide":
        rec = decide(
            _parse_clock(args.started_at),
            Path(args.log),
            Path(args.journal) if args.journal else None,
            Path(args.status) if args.status else None,
        )
        print(json.dumps(rec))
        return 0
    clock = _parse_clock(args.started_at)
    rec = build_record(
        date=session_date(clock),
        started_at=clock.isoformat(),
        late=args.late == "true",
        action=args.action,
        result=args.result,
        sealed=args.sealed == "true",
        submitted=args.submitted == "true",
        note=args.note,
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rec, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
