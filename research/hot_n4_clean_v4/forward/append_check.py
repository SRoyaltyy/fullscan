"""Fail when an existing forward log line or rendered day is edited or deleted.

Covers the holdup book and the h1 book, including price ledgers. A file that
is not on the base ref is new and is not a rewrite. A file that is on the
base and is missing, shorter, or different in any earlier byte fails.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.book import BOOKS  # noqa: E402

RECORD_NAMES = (
    "LEDGER.jsonl",
    "LOG_META.json",
    "PRICE_LEDGER.jsonl",
    "price_revisions.jsonl",
    "prices.jsonl",
    "skips.jsonl",
)


def git_bytes(ref: str, path: Path) -> bytes | None:
    rel = path.relative_to(ROOT).as_posix()
    proc = subprocess.run(
        ["git", "show", f"{ref}:{rel}"],
        cwd=ROOT,
        capture_output=True,
    )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace")
        if "does not exist" in err or "not in" in err or "exists on disk, but not in" in err:
            return None
        raise SystemExit(f"git show {ref}:{rel} failed: {err.strip()}")
    return proc.stdout


def _prefix(old: bytes, new: bytes, label: str) -> None:
    if old and not old.endswith(b"\n"):
        raise SystemExit(f"{label} base does not end on a line")
    if not new.startswith(old):
        raise SystemExit(f"append-only violated: {label} was modified or deleted")
    if len(new) > len(old) and not new[len(old):].endswith(b"\n"):
        raise SystemExit(f"{label} suffix is not complete lines")


def _check_page(ref: str, path: Path) -> None:
    old_page = git_bytes(ref, path)
    label = path.relative_to(ROOT).as_posix()
    if old_page is None:
        return
    if not path.is_file():
        raise SystemExit(f"append-only violated: {label} was deleted")
    old_rows = json.loads(old_page)
    new_rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(old_rows, list) or not isinstance(new_rows, list):
        raise SystemExit(f"{label} is not a list")
    if len(new_rows) < len(old_rows) or new_rows[:len(old_rows)] != old_rows:
        raise SystemExit(f"append-only violated: {label} rewrote a past day")


def check_against(ref: str) -> None:
    for book in BOOKS:
        names = (book.log_name, *RECORD_NAMES)
        for name in names:
            path = book.folder / name
            old = git_bytes(ref, path)
            if old is None:
                continue
            if not path.is_file():
                raise SystemExit(f"append-only violated: {path.relative_to(ROOT)} was deleted")
            _prefix(old, path.read_bytes(), path.relative_to(ROOT).as_posix())
        _check_page(ref, book.page / "log.json")


def main() -> None:
    ref = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
    check_against(ref)
    print("append-only prefix ok", ref)


if __name__ == "__main__":
    main()
