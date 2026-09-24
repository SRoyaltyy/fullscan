"""Sessions that research grading and mining must not learn from.

Live buying does not read this list. Factor-mine recipe math is unchanged;
callers drop these dates from the evidence calendar before they score.

File: data/quarantine_sessions.json
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PATH = ROOT / "data" / "quarantine_sessions.json"


def load(path: Path | None = None) -> list[dict]:
    p = path or PATH
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    rows = data.get("sessions") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows:
        if isinstance(row, str) and len(row) >= 10:
            out.append({"date": row[:10], "reason": "", "detail": ""})
        elif isinstance(row, dict) and row.get("date"):
            out.append({
                "date": str(row["date"])[:10],
                "reason": str(row.get("reason") or ""),
                "detail": str(row.get("detail") or ""),
            })
    return out


def dates(path: Path | None = None) -> set[str]:
    return {row["date"] for row in load(path) if row.get("date")}


def lane_runs(path: Path | None = None) -> list[dict]:
    """Lane Actions runs whose harvest read a stale news window.

    Research loaders that walk the published board or one-shot rows should
    skip these artifacts. Session dates those runs read are also in ``dates``.
    """
    p = path or PATH
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    rows = data.get("lane_runs") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("run_id"):
            continue
        out.append({
            "run_id": str(row["run_id"]),
            "url": str(row.get("url") or ""),
            "reason": str(row.get("reason") or ""),
            "detail": str(row.get("detail") or ""),
            "artifacts": [str(a) for a in (row.get("artifacts") or [])],
            "stale_sessions_read": [
                str(d)[:10] for d in (row.get("stale_sessions_read") or [])
            ],
        })
    return out


def lane_artifact_blocked(path: str, file: Path | None = None) -> bool:
    """True when ``path`` is an artifact of a quarantined Lane run."""
    text = str(path).replace("\\", "/")
    for run in lane_runs(file):
        for art in run["artifacts"]:
            if text == art or text.startswith(art.rstrip("/") + "/"):
                return True
    return False


def is_quarantined(session: str, path: Path | None = None) -> bool:
    return str(session)[:10] in dates(path)


def reason(session: str, path: Path | None = None) -> str:
    want = str(session)[:10]
    for row in load(path):
        if row["date"] == want:
            return row.get("reason") or row.get("detail") or "quarantined"
    return ""


def filter_dates(sessions: list[str], path: Path | None = None) -> list[str]:
    bad = dates(path)
    return [d for d in sessions if str(d)[:10] not in bad]


def drop_sessions(panel: dict, path: Path | None = None) -> tuple[dict, list[str]]:
    """Copy ``panel`` without quarantined session rows. Scoring math is untouched."""
    if not isinstance(panel, dict):
        return panel, []
    bad = dates(path)
    cal_in = [str(d) for d in (panel.get("session_dates") or [])]
    dropped = [d for d in cal_in if d[:10] in bad]
    if not dropped:
        return panel, []
    drop = {d[:10] for d in dropped}
    cal = [d for d in cal_in if d[:10] not in drop]
    rows = [r for r in (panel.get("rows") or [])
            if str(r.get("date") or "")[:10] not in drop]
    out = dict(panel)
    out["session_dates"] = cal
    out["rows"] = rows
    out["n_sessions"] = len(cal)
    out["n_rows"] = len(rows)
    by_date = panel.get("by_date")
    if isinstance(by_date, dict):
        out["by_date"] = {
            d: rows_ for d, rows_ in by_date.items()
            if str(d)[:10] not in drop
        }
    if cal:
        out["from_date"] = cal[0]
        out["to_date"] = cal[-1]
    out["quarantine_excluded"] = dropped
    return out, dropped
