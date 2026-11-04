"""Morning Finviz tape: own fetch, else the exact Theme Radar prior raw file.

Morning D uses fullscan's own export when that body is present and is not
a byte copy of the previous session. Otherwise it reads theme-radar
``data/snapshots/<D-1>.raw.csv`` and no earlier file. The fallback rows
are tagged ``source=theme_radar_snapshot`` with that file's sha256.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .finviz_export_guard import (
    copied_from,
    export_path,
    previous_session,
    publish_bytes,
    sha256_bytes,
    sha256_file,
)
from .theme_radar_raw import SOURCE, require_prior_raw

ROOT = Path(__file__).resolve().parent.parent


def _latest_path(root: Path | None) -> Path:
    return Path(root or ROOT) / "data" / "exports" / "finviz_latest.csv"


def _own_is_fresh(day: str, raw: bytes | None, *, root: Path | None) -> bool:
    if not raw or len(raw) < 200:
        return False
    return copied_from(day, raw, root=root) is None


def _stamp(frame: pd.DataFrame, loaded: dict) -> pd.DataFrame:
    out = frame.copy()
    out["source"] = loaded["source"]
    out["source_file"] = loaded["source_file"]
    out["source_sha256"] = loaded["source_sha256"]
    return out


def _sidecar(dest: Path, loaded: dict) -> Path:
    path = dest.with_name(dest.stem + ".source.json")
    path.write_text(json.dumps({
        "source": loaded["source"],
        "source_file": loaded["source_file"],
        "source_sha256": loaded["source_sha256"],
        "source_session": loaded["source_session"],
        "source_scrape_ts_utc": loaded["source_scrape_ts_utc"],
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def ensure_morning_export(date: str, *, own_bytes: bytes | None = None,
                          fetch=None, manifest: dict | None = None,
                          root: Path | None = None) -> dict:
    """Publish morning ``date``'s export.

    ``own_bytes`` is fullscan's fetch. Omit it to read the dated file
    already on disk. A missing body or a body whose sha256 matches the
    previous session is replaced by theme-radar's exact prior raw file.
    """
    day = str(date or "")[:10]
    dest = export_path(day, root)
    latest = _latest_path(root)
    if own_bytes is None and dest.is_file():
        own_bytes = dest.read_bytes()
    if _own_is_fresh(day, own_bytes, root=root):
        if sha256_file(dest) != sha256_bytes(own_bytes):
            publish_bytes(day, own_bytes, dest, root=root)
            publish_bytes(day, own_bytes, latest, root=root)
        return {
            "source": "finviz_export",
            "fallback": False,
            "path": str(dest),
            "raw": own_bytes,
        }
    prior = previous_session(day)
    why = "missing" if not own_bytes else "byte-identical to the previous session"
    print(
        f"[finviz] {day} own export is {why}. "
        f"Falling back to theme-radar data/snapshots/{prior}.raw.csv only.",
        flush=True,
    )
    loaded = require_prior_raw(day, fetch=fetch, manifest=manifest)
    if loaded["source"] != SOURCE or loaded["source_session"] != prior:
        message = (
            f"[finviz] FAIL {day} fallback did not stay on "
            f"data/snapshots/{prior}.raw.csv. Stopping."
        )
        print(message, flush=True)
        raise SystemExit(message)
    stamped = _stamp(loaded["frame"], loaded)
    raw = stamped.to_csv(index=False).encode("utf-8")
    publish_bytes(day, raw, dest, root=root)
    publish_bytes(day, raw, latest, root=root)
    side = _sidecar(dest, loaded)
    return {
        "source": SOURCE,
        "source_file": loaded["source_file"],
        "source_sha256": loaded["source_sha256"],
        "source_session": loaded["source_session"],
        "source_scrape_ts_utc": loaded["source_scrape_ts_utc"],
        "fallback": True,
        "path": str(dest),
        "sidecar": str(side),
        "frame": stamped,
        "raw": raw,
    }
