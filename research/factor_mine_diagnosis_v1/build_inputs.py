"""Freeze the earliest morning board and the subject recipes. Writes no return."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_diagnosis_v1.protocol import (  # noqa: E402
    DROP_PATH,
    HOLDUP_NAME,
    INPUTS,
    JUMPS_SHA256,
    MATCHED_SPLITS,
    NAMED,
    RANKED_15,
    RECIPES,
    SESSIONS,
    SUBJECTS,
)
from src.factor_mine import make_recipe, matches, rank_key, recipe_created_on  # noqa: E402
from src.factor_mine_book import HARD_RED, load_regime, morning_s  # noqa: E402
from src.lever_search_proof import build_group3_recipes  # noqa: E402

KEEP_BOOL = (
    "alarm", "blue", "zero_red", "last_green", "last_red", "candle_capture", "ohlc_break_10",
)
KEEP_NUM = (
    "ohlc_ret_5", "ohlc_rvol", "ohlc_hot_score", "candle_score", "cond_good", "cond_bad",
    "erd_flag_E", "erd_flag_R", "erd_days_since_E", "erd_days_since_R",
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


def _recipe_blob(recipe: dict) -> dict:
    return {
        "created_on": recipe_created_on(recipe["name"], recipe),
        "exit_when": dict(recipe.get("exit_when") or {}),
        "forbid": dict(recipe.get("forbid") or {}),
        "hold": int(recipe.get("hold") or 1),
        "name": recipe["name"],
        "rank": recipe.get("rank"),
        "require": dict(recipe.get("require") or {}),
        "s_boost": recipe.get("s_boost") or "none",
        "sell": recipe.get("sell") or "list",
        "side": recipe.get("side") or "long",
        "top_n": int(recipe.get("top_n") or 8),
        "universe": recipe.get("universe") or "union",
    }


def _subjects() -> list[dict]:
    group = {recipe["name"]: recipe for recipe in build_group3_recipes()}
    out = []
    for name in NAMED + RANKED_15:
        if name not in group:
            raise SystemExit(f"missing {name}")
        out.append(_recipe_blob(group[name]))
    holdup = make_recipe(
        HOLDUP_NAME, universe="union", hold=1, top_n=4, rank="hot_score",
        s_boost="holdup", forbid={"alarm": True},
    )
    out.append(_recipe_blob(holdup))
    if [row["name"] for row in out] != list(SUBJECTS):
        raise SystemExit("subject order")
    return out


def _drop() -> dict:
    jumps = (ROOT / "research/breadth_rank_v1c/JUMPS.md").read_text(encoding="utf-8")
    import hashlib
    if hashlib.sha256(jumps.encode("utf-8")).hexdigest() != JUMPS_SHA256:
        raise SystemExit("JUMPS sha")
    dropped_path = ROOT / "research/breadth_rank_v1c/bars/DROPPED.json"
    if hashlib.sha256(dropped_path.read_bytes()).hexdigest() != (
        "4da67a52e469be8ccd1430b5b7992a1d70b7542bcca67851d7d6182c267ce6ad"
    ):
        raise SystemExit("DROPPED.json sha")
    tickers = []
    for line in jumps.splitlines():
        if not line.startswith("| ") or line.startswith("| ---") or line.startswith("| ticker"):
            continue
        tickers.append(line.split("|")[1].strip())
    names = sorted(set(tickers) - {"YAAS"})
    if len(names) != 76:
        raise SystemExit(f"76 tickers, got {len(names)}")
    return {
        "dropped": names,
        "dropped_file_sha256": "4da67a52e469be8ccd1430b5b7992a1d70b7542bcca67851d7d6182c267ce6ad",
        "jumps_sha256": JUMPS_SHA256,
        "matched_splits": list(MATCHED_SPLITS),
        "note": "76 jump tickers. YAAS is in DROPPED.json and absent from the cleaned bars, and is not part of this 76.",
        "n_dropped": 76,
    }


def _parity(raw_rows: list[dict], slim_rows: list[dict], recipes: list[dict]) -> None:
    raw = {str(row.get("ticker") or "").upper(): row for row in raw_rows}
    for recipe in recipes:
        if recipe["name"] == HOLDUP_NAME:
            continue
        for slim in slim_rows:
            left = matches(raw[slim["ticker"]], recipe)
            right = matches(slim, recipe)
            if left != right:
                raise SystemExit(f"match drift {recipe['name']} {slim['ticker']}")
            if left and rank_key(raw[slim["ticker"]], recipe) != rank_key(slim, recipe):
                raise SystemExit(f"rank drift {recipe['name']} {slim['ticker']}")


def main() -> None:
    if HARD_RED != -3.0:
        raise SystemExit("HARD_RED moved")
    found = _earliest()
    regime = load_regime()
    recipes = _subjects()
    dates = {}
    for session in SESSIONS:
        if session not in found:
            raise SystemExit(f"{session} has no committed board")
        sha, raw_rows = found[session]
        score = morning_s(regime, session)
        rows = _rows(raw_rows)
        _parity(raw_rows, rows, recipes)
        dates[session] = {
            "commit": sha,
            "n": len(rows),
            "rows": rows,
            "s": None if score is None else round(float(score), 4),
        }
        print(f"{session} {sha[:12]} n={len(rows)} s={dates[session]['s']}", flush=True)
    payload = {
        "dates": dates,
        "hard_red": -3.0,
        "main": _git("rev-parse", "origin/main").strip(),
        "rule": "earliest origin/main commit whose panel.json contains the session; fills are not the panel open or close",
    }
    INPUTS.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    RECIPES.write_text(json.dumps(recipes, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    DROP_PATH.write_text(json.dumps(_drop(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("wrote inputs", flush=True)


if __name__ == "__main__":
    main()
