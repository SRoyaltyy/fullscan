"""Freeze the earliest labelled board. Writes no return."""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_recipe_search_v4.protocol import (  # noqa: E402
    HARD_RED,
    INPUTS,
    MANIFEST,
    SESSIONS,
)
from src.factor_mine import matches  # noqa: E402
from src.factor_mine_book import HARD_RED as BOOK_RED  # noqa: E402

KEEP_BOOL = (
    "alarm", "blue", "zero_red", "last_green", "last_red", "candle_capture", "ohlc_break_10",
)
KEEP_NUM = (
    "ohlc_ret_5", "ohlc_rvol", "ohlc_hot_score", "candle_score", "cond_good", "cond_bad",
    "erd_flag_E", "erd_flag_R", "erd_days_since_E", "erd_days_since_R",
)
SCORE_RE = re.compile(
    r"Prediction:\s*(UP|DOWN|FLAT).*?total score\s*(-?[\d.]+)",
    re.S,
)


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True)


def _num(value):
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _earliest_panels() -> dict[str, tuple[str, str, list[dict]]]:
    commits = _git(
        "log", "--reverse", "--format=%H", "origin/main", "--", "data/factor_mine/panel.json",
    ).split()
    found: dict[str, tuple[str, str, list[dict]]] = {}
    for sha in commits:
        blob = _git("rev-parse", f"{sha}:data/factor_mine/panel.json").strip()
        raw = subprocess.check_output(
            ["git", "show", f"{sha}:data/factor_mine/panel.json"], cwd=ROOT,
        )
        panel = json.loads(raw)
        buckets: dict[str, list[dict]] = {}
        for row in panel.get("rows") or []:
            session = str(row.get("date") or "")[:10]
            buckets.setdefault(session, []).append(row)
        for session, rows in buckets.items():
            if session not in found:
                found[session] = (sha, blob, rows)
    return found


def _slim(row: dict) -> dict:
    boxes = {str(key): str(value).lower() for key, value in (row.get("boxes") or {}).items()}
    out = {
        "boxes": boxes,
        "news_box": None if row.get("news_box") is None else str(row.get("news_box")).lower(),
        "news_prior": None if row.get("news_prior") is None else str(row.get("news_prior")).lower(),
        "sources": sorted({str(item) for item in (row.get("sources") or []) if item}),
        "src_rank": None if row.get("src_rank") is None else int(row["src_rank"]),
        "ticker": str(row.get("ticker") or "").strip().upper(),
        "erd_earn_react": bool(row.get("erd_earn_react")),
    }
    for key in KEEP_BOOL:
        out[key] = bool(row.get(key))
    for key in KEEP_NUM:
        number = _num(row.get(key))
        if key in {"cond_good", "cond_bad", "erd_flag_E", "erd_flag_R", "erd_days_since_E", "erd_days_since_R"}:
            out[key] = None if number is None else int(number)
        else:
            out[key] = number
    return out


def _rows(raw_rows: list[dict]) -> list[dict]:
    ordered = sorted(raw_rows, key=lambda row: (
        99 if row.get("src_rank") is None else int(row["src_rank"]),
        str(row.get("ticker") or ""),
    ))
    out = []
    seen = set()
    for row in ordered:
        slim = _slim(row)
        if not slim["ticker"] or slim["ticker"] in seen:
            continue
        seen.add(slim["ticker"])
        out.append(slim)
    return out


def _first_text(path: str) -> tuple[str, str] | None:
    commits = _git("log", "--reverse", "--format=%H", "origin/main", "--", path).split()
    if not commits:
        return None
    sha = commits[0]
    try:
        text = subprocess.check_output(["git", "show", f"{sha}:{path}"], cwd=ROOT, text=True)
    except subprocess.CalledProcessError:
        return None
    return sha, text


def _morning_s(session: str) -> dict:
    predict = _first_text(f"01_daily/general/{session}_predict.md")
    if predict is not None:
        sha, text = predict
        match = SCORE_RE.search(text)
        if match:
            return {"s": round(float(match.group(2)), 4), "s_commit": sha, "s_source": "predict"}
    weather = _first_text(f"01_daily/weather/{session}_weather.json")
    if weather is not None:
        sha, text = weather
        try:
            payload = json.loads(text)
            value = (payload.get("signals") or {}).get("general_score")
            if value is not None:
                return {
                    "s": round(float(value), 4),
                    "s_commit": sha,
                    "s_source": "weather",
                }
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    return {"s": None, "s_commit": None, "s_source": None}


def main() -> None:
    if BOOK_RED != HARD_RED:
        raise SystemExit("HARD_RED moved")
    found = _earliest_panels()
    seen: dict[str, int] = {}
    dates = {}
    manifest_sessions = {}
    for session in sorted(found):
        _sha, _blob, raw_rows = found[session]
        slim = _rows(raw_rows)
        prior = None
        for older in sorted(k for k in found if k < session):
            prior = older
        counts = {}
        if prior is not None:
            counts = seen
        stamped = []
        for row in slim:
            days = counts.get(row["ticker"], 0) + 1
            stamped.append(dict(row, days_on_list=days))
        seen = {row["ticker"]: row["days_on_list"] for row in stamped}
        if session not in SESSIONS:
            continue
        gate = {
            "name": "parity", "universe": "union", "hold": 1, "side": "long",
            "top_n": 8, "require": {}, "forbid": {"alarm": True}, "rank": "hot_score",
        }
        raw_by = {str(row.get("ticker") or "").upper(): row for row in raw_rows}
        for row in stamped:
            if matches(raw_by[row["ticker"]], gate) != matches(row, gate):
                raise SystemExit(f"match drift {session} {row['ticker']}")
        score = _morning_s(session)
        dates[session] = {
            "blob": _blob,
            "commit": _sha,
            "n": len(stamped),
            "path": "data/factor_mine/panel.json",
            "rows": stamped,
            **score,
        }
        manifest_sessions[session] = {
            "blob": _blob,
            "commit": _sha,
            "n": len(stamped),
            "path": "data/factor_mine/panel.json",
            "s": score["s"],
            "s_commit": score["s_commit"],
            "s_source": score["s_source"],
        }
        print(
            f"{session} {_sha[:12]} n={len(stamped)} s={score['s']} {score['s_source']}",
            flush=True,
        )
    missing = [session for session in SESSIONS if session not in dates]
    if missing:
        raise SystemExit(f"no earliest board for {missing}")
    payload = {
        "dates": {session: dates[session] for session in SESSIONS},
        "hard_red": HARD_RED,
        "main": _git("rev-parse", "origin/main").strip(),
        "rule": (
            "earliest origin/main commit whose panel.json contains the session; "
            "morning S is the earliest predict.md score else the earliest weather "
            "general_score; a later rebuild is not used"
        ),
    }
    INPUTS.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    MANIFEST.write_text(json.dumps({
        "main": payload["main"],
        "rule": payload["rule"],
        "sessions": manifest_sessions,
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("wrote inputs", flush=True)


if __name__ == "__main__":
    main()
