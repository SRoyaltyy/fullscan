"""Frozen rules for factor_mine_avg_v1.

This module has no scores. The universe is computed from the #357 recipe
freeze and the #336 book names. It is not a list picked after seeing returns.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from src.lever_search_bars import BAR_BLOB_SHA, BAR_COMMIT, BAR_SHA256
from src.lever_search_proof import build_group3_recipes

ROOT = Path(__file__).resolve().parents[2]
STUDY = Path(__file__).resolve().parent
PREREG = STUDY / "PREREG.md"
DROP_PATH = STUDY / "DROP_LIST.json"
RETURNS = STUDY / "returns"
REPORT = RETURNS / "REPORT.md"
STATE_DIR = ROOT / "data" / "factor_mine" / "state"
MARKER = "<!-- BEGIN COVERED -->\n"
CAPITAL = 10_000.0

BAR_PATH = "data/prices/ohlc.parquet"
CLEAN_COMMIT = "ad10f862ad51df88f4dd03ff3dbcdf628f7e2685"
GROUP3_COMMIT = "8e8c36a7117e60040d04cfa598e503b82fd7c6ea"
PROOF_COMMIT = "1bf94d7dc3ce00c29667d0fae6fdb6bb8effb73c"

BEFORE = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11",
)
AFTER = (
    "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
    "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25",
)
SESSIONS = BEFORE + AFTER
WINDOWS = (("before_0914", BEFORE), ("from_0914", AFTER))

RANDOM4_SEED = 20260813
RANDOM4_DRAWS = 1000
RANDOM4_N = 4
ALLOW_PREFIXES = (
    "research/factor_mine_avg_v1/",
    ".github/workflows/factor_mine_avg_v1.yml",
)


def prereg_fingerprint(text: str | None = None) -> str:
    """SHA-256 of the UTF-8 bytes after the covered marker, including its newline."""
    raw = PREREG.read_text(encoding="utf-8") if text is None else text
    if MARKER not in raw:
        raise SystemExit("prereg marker missing")
    body = raw.split(MARKER, 1)[1]
    if not body.endswith("\n"):
        raise SystemExit("prereg body must end in a newline")
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def load_drop() -> dict:
    payload = json.loads(DROP_PATH.read_text(encoding="utf-8"))
    dropped = list(payload["dropped"])
    if dropped != sorted(dropped) or len(dropped) != len(set(dropped)):
        raise SystemExit("drop list is not a sorted unique list")
    if len(dropped) != 77:
        raise SystemExit("drop list length")
    matched = list(payload["matched_splits"])
    if matched != ["ALP", "NFE", "TNMG", "WCT"]:
        raise SystemExit("matched splits")
    if any(name not in dropped for name in matched):
        raise SystemExit("matched split missing from the drop list")
    if payload["explained_split_still_dropped"] != ["YAAS"] or "YAAS" not in dropped:
        raise SystemExit("YAAS flag")
    open52 = list(payload["open_prev_close_52"])
    if len(open52) != 52 or open52 != sorted(open52):
        raise SystemExit("52-name list")
    if "YAAS" in open52 or any(name not in dropped for name in open52):
        raise SystemExit("52-name list is not inside the latest drop list")
    if payload["source_commit"] != CLEAN_COMMIT:
        raise SystemExit("drop list source commit")
    if payload["cleaned_parquet_sha256"] != "5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2":
        raise SystemExit("cleaned parquet sha")
    return payload


def universe() -> list[dict]:
    """Long top-N ranked buys in both the #357 110 and the #336 book.

    A recipe qualifies when build_group3_recipes() gives it side long and a
    rank key, and data/factor_mine/state has that name. The rank key is what
    makes the buy "sort, then keep top_n". List-order recipes, shorts, and
    sleeves added after that 110 stay out.
    """
    state = {path.name for path in STATE_DIR.iterdir() if path.is_dir()}
    chosen = []
    for recipe in build_group3_recipes():
        if recipe.get("side") != "long" or not recipe.get("rank"):
            continue
        if recipe["name"] not in state:
            raise SystemExit(f"{recipe['name']} is not in the #336 book")
        chosen.append(recipe)
    chosen.sort(key=lambda recipe: recipe["name"])
    if len(chosen) != 15:
        raise SystemExit(f"universe count {len(chosen)}")
    return chosen


def compound(returns: list[float]) -> float:
    acc = 1.0
    for value in returns:
        acc *= 1.0 + float(value)
    return acc - 1.0


def median(values: list[float]) -> float | None:
    clean = sorted(float(value) for value in values)
    if not clean:
        return None
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2.0


def day_counts(returns: list[float]) -> tuple[int, int, int]:
    up = down = flat = 0
    for value in returns:
        if value > 0:
            up += 1
        elif value < 0:
            down += 1
        else:
            flat += 1
    return up, down, flat


def best_ticker(totals: dict[str, float], first: dict[str, str]) -> str | None:
    if not totals:
        return None
    return min(totals, key=lambda ticker: (-totals[ticker], first.get(ticker, "9999-99-99"), ticker))


def ex_best_compound(
    sessions: list[str],
    check: list[str],
    returns: list[float],
    pnl_by_day: dict[str, dict[str, float]],
    best: str | None,
) -> float | None:
    """Check-session compound after the best ticker's dollars are removed.

    Equity starts at 10,000 and follows every walked session. Only check
    sessions enter the product.
    """
    if not check or best is None or len(returns) != len(sessions):
        return None
    equity = CAPITAL
    by_session = dict(zip(sessions, returns))
    out: list[float] = []
    check_set = set(check)
    for session in sessions:
        start = equity
        end = start * (1.0 + float(by_session[session]))
        if session in check_set and start != 0.0:
            removed = float(pnl_by_day.get(session, {}).get(best) or 0.0)
            out.append((end - removed) / start - 1.0)
        equity = end
    if not out:
        return None
    return compound(out)


def random4_rows(session: str, names: list[str]) -> list[dict]:
    """Picks the union walk keeps on a stock_book day and drops on a sit day."""
    return [
        {"date": session, "ticker": ticker, "sources": ["stock_book"], "src_rank": i}
        for i, ticker in enumerate(names)
    ]


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


# Re-export the pin so the prereg and the scorer share one constant.
BAR_PIN = {
    "blob_sha": BAR_BLOB_SHA,
    "commit": BAR_COMMIT,
    "path": BAR_PATH,
    "sha256": BAR_SHA256,
}
