"""Stop a Finviz session whose export is a byte copy of the previous one.

The morning collector's Elite login has been returning HTTP 403 since
2026-10-13. It then read ``data/exports/finviz_latest.csv`` and archived
those bytes as ``finviz_<today>.csv``. That latest file is still the
2026-10-12 tape, so every later session file has the same sha256.

A copied tape is not a fresh session. Callers exit 1 and do not publish it.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR_NAME = Path("data") / "exports"


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def sha256_file(path: Path) -> str | None:
    try:
        return sha256_bytes(path.read_bytes())
    except OSError:
        return None


def export_path(date: str, root: Path | None = None) -> Path:
    day = str(date or "")[:10]
    return Path(root or ROOT) / EXPORT_DIR_NAME / f"finviz_{day}.csv"


def previous_session(date: str) -> str:
    from .skip_if_good import _prev_weekday

    return _prev_weekday(str(date)[:10])


def copied_from(date: str, raw: bytes | None = None, *,
                root: Path | None = None) -> tuple[str, str] | None:
    """``(previous session, sha256)`` when this tape matches that file.

    ``raw`` is the body about to be written. With ``raw`` omitted, the
    check reads today's file on disk. A missing file is not a copy.
    """
    day = str(date or "")[:10]
    prev = previous_session(day)
    prev_path = export_path(prev, root)
    prev_sha = sha256_file(prev_path)
    if not prev_sha:
        return None
    if raw is None:
        have = sha256_file(export_path(day, root))
    else:
        have = sha256_bytes(raw)
    if have and have == prev_sha:
        return prev, have
    return None


def refuse_copied_export(date: str, raw: bytes | None = None, *,
                         root: Path | None = None) -> None:
    """Exit 1 when ``date`` is byte-identical to the previous session."""
    hit = copied_from(date, raw, root=root)
    if not hit:
        return
    prev, digest = hit
    raise SystemExit(
        f"[finviz] FAIL {str(date)[:10]} export sha256={digest} "
        f"is byte-identical to {prev}. Stopping. "
        f"A copied tape is not a fresh session."
    )


def publish_bytes(date: str, raw: bytes, dest: Path, *,
                  root: Path | None = None) -> None:
    """Write ``dest`` only when ``raw`` is not the previous session's file."""
    if len(raw) < 200:
        raise SystemExit(
            f"[finviz] FAIL {str(date)[:10]} export is too small "
            f"({len(raw)} bytes). Stopping."
        )
    refuse_copied_export(date, raw, root=root)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(raw)
