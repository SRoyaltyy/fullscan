"""Freeze the earliest committed morning board. This writes no return.

Cameras, list rank, and the name list come from the first commit on
origin/main whose panel.json contains that session. Earnings polarity
is stamped with the board's morning-export rule. Morning S is the
Factor Mine red-day input, snapshotted from the files on this commit's
tree. Later panel rewrites are not read.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_score_search_v1.protocol import INPUTS, SESSIONS  # noqa: E402
from src.factor_mine_book import HARD_RED, load_regime, morning_s  # noqa: E402
from src.factor_mine_probe import attach_erd_polarity  # noqa: E402

KEEP = ("ticker", "src_rank", "cond_good", "cond_bad", "prior_date", "e_pol")


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True)


def _earliest() -> dict[str, tuple[str, list[dict]]]:
    commits = _git("log", "--reverse", "--format=%H", "origin/main", "--", "data/factor_mine/panel.json").split()
    found: dict[str, tuple[str, list[dict]]] = {}
    for sha in commits:
        raw = subprocess.check_output(["git", "show", f"{sha}:data/factor_mine/panel.json"], cwd=ROOT)
        panel = json.loads(raw)
        buckets: dict[str, list[dict]] = {}
        for row in panel.get("rows") or []:
            session = str(row.get("date") or "")[:10]
            buckets.setdefault(session, []).append(row)
        for session, rows in buckets.items():
            if session not in found:
                found[session] = (sha, rows)
    return found


def _slim(rows: list[dict]) -> list[dict]:
    panel = {"rows": rows}
    attach_erd_polarity(panel)
    out = []
    seen = set()
    ordered = sorted(panel["rows"], key=lambda row: (
        99 if row.get("src_rank") is None else int(row["src_rank"]),
        str(row.get("ticker") or ""),
    ))
    for row in ordered:
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        out.append({
            "cond_bad": int(row.get("cond_bad") or 0),
            "cond_good": int(row.get("cond_good") or 0),
            "e_pol": str(row.get("e_pol") or "missing"),
            "prior_date": (str(row.get("prior_date") or "")[:10] or None),
            "src_rank": None if row.get("src_rank") is None else int(row["src_rank"]),
            "ticker": ticker,
        })
    return out


def main() -> None:
    if HARD_RED != -3.0:
        raise SystemExit("HARD_RED moved")
    found = _earliest()
    regime = load_regime()
    dates = {}
    for session in SESSIONS:
        if session not in found:
            raise SystemExit(f"{session} has no committed board")
        sha, rows = found[session]
        score = morning_s(regime, session)
        dates[session] = {
            "commit": sha,
            "n": None,
            "rows": _slim(rows),
            "s": None if score is None else round(float(score), 4),
        }
        dates[session]["n"] = len(dates[session]["rows"])
        print(f"{session} {sha[:12]} n={dates[session]['n']} s={dates[session]['s']}", flush=True)
    payload = {
        "dates": dates,
        "hard_red": -3.0,
        "main": _git("rev-parse", "origin/main").strip(),
        "rule": "earliest origin/main commit whose panel.json contains the session",
    }
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    INPUTS.write_text(text, encoding="utf-8")
    print(f"wrote {INPUTS}", flush=True)


if __name__ == "__main__":
    main()
