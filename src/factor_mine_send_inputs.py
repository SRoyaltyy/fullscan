"""One frozen input set per session for live tickets and the Factor Mine record.

From 2026-09-28 the ticket writer saves the files, candidate rows, and
sizing prices it actually read to ``data/factor_mine/send_inputs/<date>.json``.
The nightly lock builds HOT4, holdup, and any other live recipe's picks
from that file. Fills stay on the 09:30 Yahoo open. A day with no send
file still uses the 09:30 snapshot and is labeled ``source=snapshot``.

Days on or before 2026-09-25 are not rewritten.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIR = ROOT / "data" / "factor_mine" / "send_inputs"
PANEL_REL = "data/factor_mine/panel.json"
SEND_INPUT_FROM = "2026-09-28"
LIVE_RECIPES = ("union_hot_n4_h1", "union_hot_n4_holdup")


def applies(date: str) -> bool:
    """True for sessions that share one frozen input set."""
    return str(date or "")[:10] >= SEND_INPUT_FROM


def path_for(date: str) -> Path:
    return DIR / f"{str(date)[:10]}.json"


def is_live_recipe(name: str, doc: dict | None = None) -> bool:
    """HOT4, holdup, and any recipe the send file recorded a pick list for."""
    if name in LIVE_RECIPES:
        return True
    return name in ((doc or {}).get("picks") or {})


def rows_for_record(date: str, doc: dict | None) -> list[dict]:
    """Same-day candidate rows only. A later or earlier date is not read."""
    day = str(date or "")[:10]
    out: list[dict] = []
    for row in (doc or {}).get("rows") or []:
        if not isinstance(row, dict):
            continue
        row_date = str(row.get("date") or day)[:10]
        if row_date != day:
            continue
        out.append(row)
    return out


def overlay_session(panel: dict, date: str, rows: list[dict]) -> dict:
    """Copy of ``panel`` whose ``date`` rows are the send set.

    Earlier sessions stay on the panel. The Yahoo fill bars are not
    touched here.
    """
    day = str(date)[:10]
    stamped = []
    for row in rows:
        item = dict(row)
        item["date"] = day
        stamped.append(item)
    out = dict(panel)
    by = dict(panel.get("by_date") or {})
    by[day] = stamped
    out["by_date"] = by
    kept = [
        r for r in (panel.get("rows") or [])
        if str((r or {}).get("date") or "")[:10] != day
    ]
    out["rows"] = kept + stamped
    return out


def content_sha(doc: dict) -> str:
    body = {k: v for k, v in doc.items() if k != "sha256"}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _file_sha(path: Path) -> dict:
    rel = PANEL_REL
    try:
        rel = str(path.resolve().relative_to(ROOT))
    except ValueError:
        rel = str(path)
    if not path.is_file():
        return {"path": rel, "sha256": None, "bytes": 0}
    raw = path.read_bytes()
    return {
        "path": rel,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }


def input_files(payload: dict) -> list[dict]:
    """Panel bytes plus the decision-readiness hashes the writer already took."""
    files = [_file_sha(ROOT / PANEL_REL)]
    seen = {files[0]["path"]}
    inputs = ((payload or {}).get("decision_readiness") or {}).get("inputs") or {}
    if isinstance(inputs, dict):
        for rel, digest in sorted(inputs.items()):
            rel = str(rel)
            if rel in seen:
                continue
            seen.add(rel)
            files.append({"path": rel, "sha256": digest})
    return files


def sizing_prices(payload: dict) -> list[dict]:
    """Elite / open prices stamped on live buy and sell rows."""
    out: list[dict] = []
    seen: set[str] = set()
    strategies = (payload or {}).get("strategies") or {}
    names = list(LIVE_RECIPES)
    for extra in (payload or {}).get("order") or []:
        if extra not in names and is_live_recipe(str(extra), payload):
            names.append(str(extra))
    for name in names:
        rec = strategies.get(name) or {}
        if not isinstance(rec, dict):
            continue
        for row in list(rec.get("buy") or []) + list(rec.get("sell") or []):
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "").upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            item = {"ticker": ticker}
            for key in ("px", "open_px", "px_src", "open_src"):
                if row.get(key) is not None:
                    item[key] = row.get(key)
            out.append(item)
    return out


def build_document(date: str, payload: dict, session: dict) -> dict:
    """Canonical send set. ``sha256`` covers every field except itself."""
    day = str(date)[:10]
    source = str((session or {}).get("source") or "")
    rows = rows_for_record(day, {"rows": (session or {}).get("rows") or []})
    if source != "panel":
        rows = []
        source = "no_same_day_panel"
    picks = {}
    raw_picks = (session or {}).get("picks") or {}
    for name in LIVE_RECIPES:
        picks[name] = [str(t) for t in (raw_picks.get(name) or []) if t]
    doc = {
        "date": day,
        "source": "panel" if rows else "no_same_day_panel",
        "reason": str((session or {}).get("error") or ""),
        "files": input_files(payload),
        "rows": rows,
        "prices": sizing_prices(payload),
        "picks": picks,
    }
    doc["sha256"] = content_sha(doc)
    return doc


def load(date: str) -> dict | None:
    path = path_for(date)
    if not path.is_file():
        return None
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return doc if isinstance(doc, dict) else None


def _lock_reason(date: str, now: datetime | None) -> str | None:
    from . import strategy_tickets as st
    return st.dated_tickets_lock_reason(date, now)


def store(date: str, payload: dict, session: dict,
          now: datetime | None = None) -> Path | None:
    """Write the send set once. A locked file with a new body stays put."""
    if not applies(date):
        return None
    path = path_for(date)
    doc = build_document(date, payload, session)
    text = json.dumps(doc, indent=2, sort_keys=True)
    if path.is_file() and _lock_reason(date, now):
        have = path.read_text(encoding="utf-8")
        if have != text:
            reason = _lock_reason(date, now)
            print(
                f"[strategy-tickets] WARN: send_inputs {path.name} locked "
                f"({reason}). File unchanged.",
                flush=True,
            )
            return path
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    print(
        f"[strategy-tickets] send_inputs {doc['date']} "
        f"source={doc['source']} rows={len(doc['rows'])} "
        f"sha={doc['sha256'][:12]}",
        flush=True,
    )
    return path


def picks_from_send_inputs(date: str, rec: dict, doc: dict | None = None) -> list[str] | None:
    """``pick_day`` on the frozen rows. None when this day has no send file.

    This is the list before hard-red, cash, and min-hold skips. It does
    not open a snapshot or any later session.
    """
    if not applies(date):
        return None
    if doc is None:
        doc = load(date)
    if not doc:
        return None
    from . import factor_mine as fm
    rows = []
    for row in rows_for_record(date, doc):
        item = dict(row)
        item.setdefault("date", str(date)[:10])
        rows.append(item)
    return [
        str(row.get("ticker"))
        for row in fm.pick_day(rows, rec)
        if row.get("ticker")
    ]
