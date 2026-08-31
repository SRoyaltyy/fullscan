"""As-of method and input contract for historical stock-book days.

The live diagnostic requires every file the *current* pipeline produces.
Books from 2026-08-13 onward were written with a thinner packet and a
simpler ranker.  Running today's contract on those dates looks like a
hard refusal (missing digest / Judge / AB / peers / map-heat).

This module answers three questions for a session date D:

  1. Which ranker actually wrote D's book (or would have, if we rebuild)?
  2. Which files were part of the live contract on D?
  3. How did the paper sleeves do versus SPY from the dashboard start
     through D?

Hard-coded feature starts are the first session each artifact was a
daily input, taken from the files on disk.  A missing file dated before
its start is "not in era", not a ranker blocker.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)

DASHBOARD_START = "2026-08-13"

# First session each feature was a daily pipeline input.
FEATURE_START = {
    "dashboard": "2026-08-13",
    "stock_book": "2026-08-13",
    "join": "2026-08-13",
    "weather": "2026-08-13",
    "news_actions": "2026-08-13",
    "sector_board": "2026-08-13",
    "sector_predict": "2026-08-13",
    "general_predict": "2026-08-13",
    "news_parse": "2026-08-10",
    "peer_rs": "2026-08-18",
    "ab_enriched": "2026-08-19",
    "news_judge": "2026-08-19",
    "finviz_digest": "2026-08-20",
    "events": "2026-08-20",
    "preopen_qc": "2026-08-25",
    "grok_review": "2026-08-25",
    "map_heat": "2026-08-26",
    "map_heat_research": "2026-08-26",
    "green_pile": "2026-08-29",
    "decision_lattice": "2026-08-31",
    "catalyst": "2026-08-31",
}

# Diagnostic file key → feature. sector_* is handled in feature_for_key.
KEY_FEATURE = {
    "baseline_json": "map_heat",
    "baseline_md": "map_heat",
    "heat_json": "map_heat",
    "heat_md": "map_heat",
    "digest_json": "finviz_digest",
    "digest_md": "finviz_digest",
    "in_digest": "finviz_digest",
    "in_baseline": "map_heat",
    "in_heat": "map_heat",
    "parsed": "news_parse",
    "events": "events",
    "judge": "news_judge",
    "in_judge": "news_judge",
    "research_json": "map_heat_research",
    "research_md": "map_heat_research",
    "actions": "news_actions",
    "in_actions": "news_actions",
    "general": "general_predict",
    "in_general": "general_predict",
    "board": "sector_board",
    "in_board": "sector_board",
    "qc": "preopen_qc",
    "status": "preopen_qc",
    "review": "grok_review",
    "weather": "weather",
    "in_weather": "weather",
    "ab_raw": "ab_enriched",
    "ab_enriched": "ab_enriched",
    "in_ab": "ab_enriched",
    "dossiers": "catalyst",
    "join": "join",
    "peers": "peer_rs",
    "book_json": "stock_book",
    "book_md": "stock_book",
    "in_book": "stock_book",
    "green": "green_pile",
    "paper": "dashboard",
    "dashboard": "dashboard",
    "dash_html": "dashboard",
    "pages": "dashboard",
}

METHOD_ORDER = (
    ("decision_lattice", "2026-08-31"),
    ("green_pile", "2026-08-29"),
    ("weighted", "2026-08-13"),
)


def today_et() -> str:
    return datetime.now(ET).date().isoformat()


def live(date: str, feature: str) -> bool:
    start = FEATURE_START.get(feature)
    if not start:
        return True
    return str(date) >= start


def feature_for_key(key: str) -> str | None:
    if str(key).startswith("sector_"):
        return "sector_predict"
    return KEY_FEATURE.get(key)


def apply_era(specs: list[dict], date: str) -> list[dict]:
    """Demote files that were not in the live contract on `date` to role=era."""
    for spec in specs:
        for row in spec.get("files") or []:
            feature = feature_for_key(str(row.get("key") or ""))
            if not feature or live(date, feature):
                continue
            row["role"] = "era"
            row["era_feature"] = feature
            row["era_start"] = FEATURE_START.get(feature, "")
    return specs


def method_from_meta(meta: dict | None) -> str:
    meta = meta if isinstance(meta, dict) else {}
    ranker = str(meta.get("ranker") or "").strip()
    if ranker:
        return ranker
    if meta.get("pile_used"):
        return "green_pile"
    weights = meta.get("weights") or {}
    sample = next(iter(weights.values()), None) if isinstance(weights, dict) else None
    if isinstance(sample, (list, tuple)) and len(sample) >= 6:
        return "weighted"
    if isinstance(sample, (list, tuple)) and len(sample) == 4:
        return "weighted_4"
    return "weighted"


def method_for(date: str, meta: dict | None = None) -> str:
    """Ranker that wrote this book, else the method that was live on `date`."""
    if meta:
        return method_from_meta(meta)
    for name, start in METHOD_ORDER:
        if str(date) >= start:
            return name
    return "weighted"


def family_names(method: str) -> tuple[str, ...]:
    if method == "weighted_4":
        return ("join", "sector", "ab", "news")
    return ("join", "sector", "general", "news", "ab", "peer")


def describe(date: str, meta: dict | None = None) -> dict:
    """Human + machine record of the as-of method and available families."""
    meta = meta if isinstance(meta, dict) else {}
    method = method_for(date, meta or None)
    families = family_names(method)
    weights = meta.get("weights") or {}
    w1d = weights.get("1d") if isinstance(weights, dict) else None
    present = []
    absent = list(meta.get("absent_families") or [])
    inputs = meta.get("inputs") if isinstance(meta.get("inputs"), dict) else {}
    if not absent and inputs:
        absent = [k for k, row in inputs.items()
                  if isinstance(row, dict) and not row.get("found")]
    health = meta.get("input_health") if isinstance(meta.get("input_health"), dict) else {}
    fam_status = health.get("family_status") if isinstance(health.get("family_status"), dict) else {}
    if fam_status:
        present = [k for k, st in fam_status.items() if st in ("ok", "degraded")]
        if not absent:
            absent = [k for k, st in fam_status.items() if st in ("missing", "stale")]
    if not present:
        present = [f for f in families if f not in absent]

    if method == "decision_lattice":
        process = (
            "1d gate → route → rank (market / parent / child / company / "
            "setup / flow). Scores rank only inside a granted lane."
        )
    elif method == "green_pile":
        process = (
            "Green pile: buy the liquid all-green names when the pile "
            "clears the minimum; otherwise weighted fallback."
        )
    elif method == "weighted_4":
        process = (
            "Legacy 4-family weighted sum: join (labels × weather) + "
            "sector essay + news actions + AB (0 when the file was absent)."
        )
    else:
        process = (
            "Weighted sum over the families present that day "
            "(join / sector / general / news / AB / peer), renormalized "
            "when a family is missing."
        )

    return {
        "date": date,
        "method": method,
        "process": process,
        "families": list(families),
        "families_present": present,
        "families_absent": absent,
        "weights_1d": list(w1d) if isinstance(w1d, (list, tuple)) else None,
        "general_bias": meta.get("general_bias"),
        "general_direction": meta.get("general_direction") or "",
        "n_ab": meta.get("n_ab"),
        "n_peer": meta.get("n_peer"),
        "same_day_general": meta.get("same_day_general"),
        "same_day_sectors": meta.get("same_day_sectors"),
        "pile_used": meta.get("pile_used"),
        "n_pile": meta.get("n_pile"),
        "dashboard_start": DASHBOARD_START,
        "lattice_live": live(date, "decision_lattice"),
        "green_pile_live": live(date, "green_pile"),
    }


def how_steps(era: dict) -> list[str]:
    method = str(era.get("method") or "weighted")
    date = era.get("date") or "?"
    steps = [
        f"As-of **{date}** the ranker was **{method}** — not today's lattice "
        f"unless that method is decision_lattice.",
        era.get("process") or "",
    ]
    absent = era.get("families_absent") or []
    if absent:
        steps.append(
            "Families scored 0 / renormalized away that day: "
            + ", ".join(str(x) for x in absent) + "."
        )
    present = era.get("families_present") or []
    if present:
        steps.append(
            "Families that actually moved the rank: "
            + ", ".join(str(x) for x in present) + "."
        )
    if method == "decision_lattice":
        steps += [
            "1d uses gate → route → rank: market permission; parent/child "
            "group; direct company evidence; setup/flow.",
            "The weighted score ranks only inside an eligible lane.",
            "SELL/AVOID uses the bear lattice. Paper does not short.",
        ]
    elif method == "green_pile":
        steps += [
            "BUY walks the liquid green pile when it is large enough; "
            "SELL still ranks on the core weighted score.",
            "Paper does not short.",
        ]
    else:
        steps += [
            "BUY is the top-N of that weighted score after liquidity gates. "
            "There is no market HARD_RED lane and no six-domain permission row.",
            "The `_size` paper sleeves take the top 3 names in each "
            "large / mid / small-micro bucket — that is the book that "
            "printed the ~11% while SPY fell.",
            "Paper does not short. Fill = that session's close.",
        ]
    steps += [
        "Stock Book ALL calls `python -m src.stock_book` after the "
        "upstream files that existed that morning land.",
        "paper_trade writes dashboard/index.html; stock_book_all.yml "
        "force-pushes gh-pages → https://sroyaltyy.github.io/fullscan/dashboard/",
    ]
    return [s for s in steps if s]


def paper_context(date: str) -> dict:
    """Sleeve equity vs SPY from the dashboard start through `date`."""
    path = ROOT / "data" / "paper" / "equity_curve.csv"
    out = {
        "start": DASHBOARD_START,
        "through": date,
        "spy_return": None,
        "sleeves": [],
        "best": None,
        "note": "",
    }
    if not path.exists():
        out["note"] = "equity_curve.csv missing"
        return out
    by_date: dict[str, dict[str, float]] = {}
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                d = str(row.get("date") or "")
                sleeve = str(row.get("sleeve") or "")
                if not d or not sleeve or d < DASHBOARD_START or d > date:
                    continue
                try:
                    equity = float(row.get("equity") or 0)
                except (TypeError, ValueError):
                    continue
                by_date.setdefault(d, {})[sleeve] = equity
    except OSError:
        out["note"] = "equity_curve.csv unreadable"
        return out
    if not by_date:
        out["note"] = f"no paper rows between {DASHBOARD_START} and {date}"
        return out
    last = max(by_date)
    first = min(by_date)
    out["through"] = last
    start_row = by_date.get(first) or {}
    end_row = by_date.get(last) or {}
    sleeves = []
    for name, end in sorted(end_row.items()):
        start = start_row.get(name)
        if name.startswith("SPY"):
            if start:
                out["spy_return"] = end / start - 1.0
            else:
                out["spy_return"] = end / 10000.0 - 1.0
            continue
        if not start:
            start = 10000.0
        sleeves.append({
            "sleeve": name,
            "equity": end,
            "ret": end / start - 1.0,
        })
    sleeves.sort(key=lambda r: r["ret"], reverse=True)
    out["sleeves"] = sleeves
    out["best"] = sleeves[0] if sleeves else None
    spy = out["spy_return"]
    best = out["best"]
    if best and spy is not None:
        out["note"] = (
            f"{best['sleeve']} {best['ret']:+.1%} vs SPY {spy:+.1%} "
            f"from {first} through {last}. The 1d_top sleeve is the "
            f"names this Action prints; the ~11% prints sat on "
            f"_size and longer-horizon sleeves of the same weighted books."
        )
    return out


def load_book_meta(date: str) -> dict:
    path = ROOT / "data" / "stock_book" / f"{date}_stock_book.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    meta = data.get("meta") if isinstance(data, dict) else None
    return meta if isinstance(meta, dict) else {}


def join_exists(date: str) -> bool:
    return (ROOT / "data" / "join" / f"{date}_ranked.csv").exists()


def book_exists(date: str) -> bool:
    return (ROOT / "data" / "stock_book" / f"{date}_stock_book.json").exists()
