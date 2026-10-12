"""09:30 ET lock for the undated (live) strategy-ticket copies.

``data/day_board/<D>_strategy_tickets.json`` already locks at D's
09:30 ET or at the paper-send journal
(``strategy_tickets.dated_tickets_lock_reason``) and is sealed in
``data/past_day_lock/manifest.jsonl``. The undated copies were not
locked. On 2026-10-07 the post-close runs (17:11, 20:24 and 20:32 ET)
rewrote ``data/factor_mine/strategy_tickets.json`` while it still said
``date`` / ``clock_legal_for`` 2026-10-07, from that day's own close
panel (``panel_bake_date`` 2026-10-07). The dated file, the status page
and the sealed ledgers did not move, but a same-dated buy/sell list
changed after its session.

Rule, the same clock as the dated file:

* A live copy that holds session D may take another session-D body
  only while D is open: before 09:30 ET on D and before D's paper-send
  journal. After that the copy keeps its bytes. The evening body goes
  only to ``data/day_board/<D>_strategy_tickets_draft.json``, which is
  marked ``draft: true`` / ``not_for_trading: true``.
* A newer session may replace it. That is the next morning's first
  write (Pre-Open, morning look from #512, 08:07 / 09:07 passes).
* An older session never replaces it.

Fail-closed record: when a write runs after D's lock, the bytes the
canonical copy (``data/factor_mine/strategy_tickets.json``) held are
sealed in ``data/ticket_session_lock/manifest.jsonl`` (append-only).
A later write that finds the sealed copy changed raises
``TicketSessionLockError``. A ``void`` row is the only way a body that
differs from the seal is tolerated on disk, and it does not make that
body valid; it only records it. Rows are never edited or removed.

CI: ``python -m src.ticket_session_lock --check-against <base>`` checks
the manifest is append-only, that the canonical copy matches its seal
(or a void row), and that no live copy changed a same-session body
generated at or after that session's 09:30 ET, or went back a session.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")
MANIFEST_PATH = ROOT / "data" / "ticket_session_lock" / "manifest.jsonl"
CANONICAL = "data/factor_mine/strategy_tickets.json"
# Undated copies that carry one session's full buy/sell list or its slim.
LIVE_COPIES = (
    "data/day_board/strategy_tickets.json",
    "data/factor_mine/strategy_tickets.json",
    "dashboard/factor-mine/strategy_tickets.json",
    "dashboard/factor-mine/today_strategies.json",
    "data/day_board/today_strategies.json",
    "dashboard/today_strategies.json",
)
DRAFT_NOTE = (
    "Post-lock rebuild. Not the ticket list for any session and not for "
    "trading. The session's send-time list is the dated "
    "data/day_board/<date>_strategy_tickets.json and the live copies, "
    "which keep their bytes after 09:30 ET."
)


class TicketSessionLockError(SystemExit):
    """A live ticket copy for a started session would change, or did."""

    def __init__(self, message: str):
        super().__init__(message)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def open_cutoff(date: str) -> datetime:
    year, month, day = (int(part) for part in str(date)[:10].split("-"))
    return datetime(year, month, day, 9, 30, tzinfo=ET)


def _parse(text: str | None) -> dict | None:
    if not text:
        return None
    try:
        doc = json.loads(text)
    except (TypeError, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


def session_of(text: str | None) -> str:
    """Session a ticket body is for. Empty when unreadable."""
    doc = _parse(text)
    if doc is None:
        return ""
    raw = doc.get("clock_legal_for") or doc.get("session_open") or doc.get("date") or ""
    return str(raw)[:10]


def generated_at_of(text: str | None) -> datetime | None:
    doc = _parse(text)
    raw = (doc or {}).get("generated_at")
    if not raw:
        return None
    try:
        stamp = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=ET)
    return stamp


def may_replace(old_text: str | None, new_text: str, new_date: str,
                lock_reason: str | None) -> tuple[bool, str]:
    """Whether a live copy holding ``old_text`` may take ``new_text``.

    ``lock_reason`` is ``strategy_tickets.dated_tickets_lock_reason`` for
    ``new_date`` (09:30 ET passed, or the paper send is journaled).
    """
    if old_text is None:
        return True, "new file"
    if old_text == new_text:
        return True, "unchanged"
    old_date = session_of(old_text)
    new_date = str(new_date)[:10]
    if not old_date:
        return True, "unreadable copy"
    if new_date < old_date:
        return False, f"holds {old_date}; an older session {new_date} never replaces it"
    if new_date > old_date:
        return True, f"next session {new_date} replaces {old_date}"
    if lock_reason:
        return False, f"{old_date} locked ({lock_reason})"
    return True, f"{old_date} still open"


def in_repo(path: Path) -> bool:
    try:
        Path(path).resolve().relative_to(ROOT.resolve())
    except ValueError:
        return False
    return True


def _rel(path: Path) -> str:
    return Path(path).resolve().relative_to(ROOT.resolve()).as_posix()


def load_manifest(path: Path | None = None) -> list[dict]:
    dest = Path(path or MANIFEST_PATH)
    if not dest.is_file():
        return []
    return _rows_from_text(dest.read_text(encoding="utf-8"))


def _rows_from_text(text: str) -> list[dict]:
    rows = []
    for raw in text.splitlines():
        if raw == "":
            raise TicketSessionLockError("ticket-session lock: blank manifest line")
        try:
            obj = json.loads(raw)
        except ValueError as exc:
            raise TicketSessionLockError(
                "ticket-session lock: manifest line is not json") from exc
        if not isinstance(obj, dict):
            raise TicketSessionLockError(
                "ticket-session lock: manifest line is not an object")
        rows.append(obj)
    return rows


def manifest_line(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _append(row: dict, path: Path | None = None) -> None:
    dest = Path(path or MANIFEST_PATH)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("a", encoding="utf-8") as handle:
        handle.write(manifest_line(row) + "\n")


def _rows_for(rows: list[dict], kind: str, date: str, rel: str) -> list[dict]:
    return [
        r for r in rows
        if r.get("kind") == kind and str(r.get("date") or "") == date
        and str(r.get("path") or "") == rel
    ]


def allowed_shas(rows: list[dict], date: str, rel: str) -> tuple[str | None, set[str]]:
    """Seal sha for (date, path), plus every sha that row set tolerates."""
    seals = _rows_for(rows, "seal", date, rel)
    if not seals:
        return None, set()
    seal = str(seals[0].get("sha256") or "")
    ok = {seal}
    ok |= {str(r.get("sha256") or "") for r in _rows_for(rows, "void", date, rel)}
    return seal, ok


def assert_sealed(path: Path, *, manifest: Path | None = None,
                  rel: str | None = None) -> None:
    """Fail when the copy on disk no longer matches its session's seal."""
    path = Path(path)
    if not path.is_file():
        return
    if rel is None:
        if manifest is None and not in_repo(path):
            return
        rel = _rel(path) if in_repo(path) else path.as_posix()
    text = path.read_text(encoding="utf-8")
    date = session_of(text)
    if not date:
        return
    seal, ok = allowed_shas(load_manifest(manifest), date, rel)
    if seal is None:
        return
    digest = sha256_text(text)
    if digest not in ok:
        raise TicketSessionLockError(
            f"ticket-session lock: {rel} for {date} changed after its seal "
            f"(sha256 {seal} -> {digest}). Not rewriting a started session."
        )


def seal_locked(path: Path, date: str, *, lock_reason: str | None,
                manifest: Path | None = None, rel: str | None = None,
                now: datetime | None = None) -> dict | None:
    """Seal the canonical copy's bytes once ``date`` is locked. Add-only."""
    path = Path(path)
    if not lock_reason or not path.is_file():
        return None
    if rel is None:
        if manifest is None and not in_repo(path):
            return None
        rel = _rel(path) if in_repo(path) else path.as_posix()
    text = path.read_text(encoding="utf-8")
    day = str(date)[:10]
    if session_of(text) != day:
        return None
    rows = load_manifest(manifest)
    if _rows_for(rows, "seal", day, rel):
        return None
    clock = (now or datetime.now(ET)).astimezone(ET)
    row = {
        "kind": "seal",
        "date": day,
        "path": rel,
        "sha256": sha256_text(text),
        "generated_at": str((_parse(text) or {}).get("generated_at") or ""),
        "sealed_at": clock.isoformat(timespec="seconds"),
        "basis": f"bytes held at the lock ({lock_reason})",
    }
    _append(row, manifest)
    return row


def draft_body(payload: dict, *, date: str, reason: str, dated_name: str) -> str:
    """The evening body, marked so nothing reads it as a session list."""
    marked = dict(payload)
    marked["draft"] = True
    marked["not_for_trading"] = True
    marked["draft_reason"] = reason
    marked["draft_of"] = dated_name
    marked["draft_note"] = DRAFT_NOTE.replace("<date>", str(date)[:10])
    return json.dumps(marked, indent=2)


# --- CI check ---------------------------------------------------------------

def _git_text(rev: str, rel: str, root: Path | None = None) -> str | None:
    proc = subprocess.run(
        ["git", "show", f"{rev}:{rel}"],
        cwd=root or ROOT, check=False, capture_output=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout.decode("utf-8", errors="replace")


def assert_manifest_prefix(base_text: str, head_text: str) -> None:
    base = [line for line in base_text.splitlines()]
    head = [line for line in head_text.splitlines()]
    if any(line == "" for line in base + head):
        raise TicketSessionLockError("ticket-session lock: blank manifest line")
    if head[: len(base)] != base:
        if len(head) < len(base):
            raise TicketSessionLockError("ticket-session lock: manifest line removed")
        raise TicketSessionLockError("ticket-session lock: manifest line changed")


def assert_copy_change(rel: str, base_text: str | None, head_text: str | None) -> None:
    """One live copy, base -> head. Same rule as ``may_replace``.

    A same-session change is legal only for a body generated before
    that session's 09:30 ET. Going back a session is never legal.
    """
    if base_text is None or head_text is None or base_text == head_text:
        return
    old_date = session_of(base_text)
    new_date = session_of(head_text)
    if not old_date or not new_date:
        return
    if new_date < old_date:
        raise TicketSessionLockError(
            f"ticket-session lock: {rel} went back from {old_date} to {new_date}."
        )
    if new_date > old_date:
        return
    stamp = generated_at_of(head_text)
    if stamp is None or stamp >= open_cutoff(new_date):
        when = stamp.isoformat() if stamp else "unknown time"
        raise TicketSessionLockError(
            f"ticket-session lock: {rel} for {new_date} was rewritten with a "
            f"body generated at {when}, at or after 09:30 ET. Use the draft."
        )


def check_manifest(*, root: Path | None = None, manifest: Path | None = None) -> None:
    base = root or ROOT
    rows = load_manifest(manifest or (base / MANIFEST_PATH.relative_to(ROOT)))
    for row in rows:
        kind = row.get("kind")
        if kind not in ("seal", "void", "note", "seal_reference"):
            raise TicketSessionLockError(f"ticket-session lock: unknown row kind {kind!r}")
        if kind in ("seal", "void") and not (row.get("date") and row.get("path") and row.get("sha256")):
            raise TicketSessionLockError("ticket-session lock: seal/void row missing date, path or sha256")
    seen: set[tuple[str, str]] = set()
    for row in rows:
        if row.get("kind") != "seal":
            continue
        key = (str(row["date"]), str(row["path"]))
        if key in seen:
            raise TicketSessionLockError(
                f"ticket-session lock: second seal for {key[1]} {key[0]}")
        seen.add(key)
    path = base / CANONICAL
    if not path.is_file():
        return
    text = path.read_text(encoding="utf-8")
    date = session_of(text)
    seal, ok = allowed_shas(rows, date, CANONICAL)
    if seal is None:
        return
    digest = sha256_text(text)
    if digest not in ok:
        raise TicketSessionLockError(
            f"ticket-session lock: {CANONICAL} for {date} does not match its "
            f"seal {seal} or any void row (on disk {digest})."
        )


def check_against(rev: str, *, root: Path | None = None) -> None:
    base_root = root or ROOT
    rel_manifest = MANIFEST_PATH.relative_to(ROOT).as_posix()
    head_manifest = base_root / rel_manifest
    assert_manifest_prefix(
        _git_text(rev, rel_manifest, base_root) or "",
        head_manifest.read_text(encoding="utf-8") if head_manifest.is_file() else "",
    )
    for rel in LIVE_COPIES:
        head_path = base_root / rel
        head = head_path.read_text(encoding="utf-8") if head_path.is_file() else None
        assert_copy_change(rel, _git_text(rev, rel, base_root), head)
    check_manifest(root=base_root)


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Check the live ticket 09:30 lock")
    parser.add_argument("--check-against", dest="rev", default="")
    args = parser.parse_args(argv)
    if args.rev:
        check_against(args.rev)
        print(f"ticket-session lock ok against {args.rev}")
        return 0
    check_manifest()
    print("ticket-session lock ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
