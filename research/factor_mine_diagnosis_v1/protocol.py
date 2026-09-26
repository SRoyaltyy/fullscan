"""Locked measurement rules for factor_mine_diagnosis_v1.

This file has no window score. The study describes books. It does not
choose a recipe or a combination for trading.
"""
from __future__ import annotations

import hashlib
import itertools
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STUDY = Path(__file__).resolve().parent
PREREG = STUDY / "PREREG.md"
INPUTS = STUDY / "INPUTS.json"
RECIPES = STUDY / "RECIPES.json"
DROP_PATH = STUDY / "DROP_LIST.json"
RETURNS = STUDY / "returns"
REPORT = RETURNS / "REPORT.md"
MARKER = "<!-- BEGIN COVERED -->\n"

CAPITAL = 10_000.0
HARD_RED = -3.0
HOLDUP_S = 0.0
HOLDUP_SESS = 2
RANDOM_SEED = 20260813
RANDOM_DRAWS = 1000
RANDOM_N = 4
N_GRID = (4, 8, 12)
MIN_TRADES = 30
TREE_DEPTHS = (2, 3)
COMBO_TABLE_CAP = 20
SORT_SEED = 20260813

TUNE = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11",
)
FORWARD = (
    "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
    "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25",
)
SESSIONS = TUNE + FORWARD

CLEAN_COMMIT = "ad10f862ad51df88f4dd03ff3dbcdf628f7e2685"
CLEAN_BLOB = "e7bbac335cfa87243f9d0ddf8c331a0739dd0df8"
CLEAN_SHA256 = "5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2"
YAHOO_PATH = "data/prices/ohlc.parquet"
YAHOO_SHA256 = "559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55"
YAHOO_BLOB = "3456f7f489a6fa7033e8ae5cc942d8279f0113e3"
JUMPS_SHA256 = "8080267a532fff2ea4c9e225fdf006f38f4ddeb5ca8e30900b16a320e58ce05a"
DROPPED_FILE_SHA256 = "4da67a52e469be8ccd1430b5b7992a1d70b7542bcca67851d7d6182c267ce6ad"
FEE_SHA256 = "019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3"
MATCHED_SPLITS = ("ALP", "NFE", "TNMG", "WCT")

# The #363 fifteen, name order. Plus the three #357 names and holdup.
RANKED_15 = (
    "union_candle_score_h1",
    "union_candle_score_h3",
    "union_cond_h1",
    "union_cond_h3",
    "union_cond_n4_h3",
    "union_hot_n12_h1",
    "union_hot_n4_h1",
    "union_hot_score_h1",
    "union_hot_score_h3",
    "union_ret_5_h1",
    "union_ret_5_h3",
    "union_w_hot_candle_h1",
    "union_w_hot_candle_h3",
    "union_w_hot_cond_h1",
    "union_w_hot_cond_h3",
)
NAMED = ("probable_h3", "probable_h5", "short_extended_h3")
HOLDUP_NAME = "union_hot_n4_holdup"
SUBJECTS = NAMED + RANKED_15 + (HOLDUP_NAME,)

# Drop-one toggles. Pairs are every combination of two.
DROP_TOGGLES = ("must_have", "must_not", "sort", "weather", "hold_rule")
BUILD_PARTS = ("list_source", "must_have", "must_not", "sort", "weather", "hold_rule")
BIN_FEATURES = (
    "price_band", "ret5_band", "cond_band", "heat", "candle", "news",
    "earn", "days_on_list", "weather", "alarm", "blue", "zero_red", "vol",
)

ALLOW_PREFIXES = (
    "research/factor_mine_diagnosis_v1/",
    ".github/workflows/factor_mine_diagnosis_v1.yml",
)

INPUTS_SHA256 = "c44d31254f524c136b54e066a1405ede68cc7c2492afd19e6c9ac38ef81ce368"
RECIPES_SHA256 = "4c4f0eefdcfd8c3c1cb45a044e8f881a867c23fc4d40a8da473fc19b1620ca8f"
DROP_SHA256 = "0b45c8a3d5b196845deba4178f2b1206f3eb9529171026d9e6bcf319a7c317d2"


class SameDayLeak(Exception):
    """A feature bar is dated on or after the session."""


def prereg_fingerprint(text: str | None = None) -> str:
    raw = PREREG.read_text(encoding="utf-8") if text is None else text
    if MARKER not in raw:
        raise SystemExit("prereg marker missing")
    body = raw.split(MARKER, 1)[1]
    if not body.endswith("\n"):
        raise SystemExit("prereg body must end in a newline")
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compound(returns: list[float]) -> float:
    acc = 1.0
    for value in returns:
        acc *= 1.0 + float(value)
    return acc - 1.0


def mean(values: list[float]) -> float | None:
    clean = [float(value) for value in values]
    if not clean:
        return None
    return sum(clean) / len(clean)


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


def prior_feature_dates(dates: list[str], session: str) -> list[str]:
    """Feature bars must be strictly before the session. The fill is not a feature."""
    out = []
    for date in dates:
        if date >= session:
            raise SameDayLeak(date)
        out.append(date)
    return out


def hard_red(score) -> bool:
    """Engine rule: a missing S does not sit. S <= -3 does."""
    if score is None:
        return False
    return float(score) <= HARD_RED


def active_parts(recipe: dict) -> list[str]:
    """Parts the full recipe adds onto the bare mixed list. Stable order."""
    sell = recipe.get("sell") or "list"
    boost = recipe.get("s_boost") or "none"
    exit_when = recipe.get("exit_when") or {}
    parts = []
    if (recipe.get("universe") or "union") != "union":
        parts.append("list_source")
    if recipe.get("require"):
        parts.append("must_have")
    if recipe.get("forbid"):
        parts.append("must_not")
    if recipe.get("rank"):
        parts.append("sort")
    parts.append("weather")
    bare_hold = int(recipe.get("hold") or 1) == 1 and sell == "time" and boost == "none" and not exit_when
    if not bare_hold:
        parts.append("hold_rule")
    return parts


def pair_drops() -> list[tuple[str, str]]:
    return list(itertools.combinations(DROP_TOGGLES, 2))


def build_orders(parts: list[str]) -> list[tuple[str, ...]]:
    return list(itertools.permutations(parts))


def interaction_gain(base: float, only_i: float, only_j: float, both: float) -> float:
    """Combined drop effect minus the two single drop effects.

    effect(part) = baseline - book with that part removed.
    combined = baseline - book with both removed.
    gain = combined - effect(i) - effect(j) = only_i + only_j - both - baseline.
    """
    return float(only_i) + float(only_j) - float(both) - float(base)


def price_band(px: float | None) -> str:
    if px is None:
        return "missing"
    if px < 3:
        return "lt3"
    if px < 10:
        return "m3_10"
    return "ge10"


def ret5_band(value) -> str:
    if value is None:
        return "missing"
    number = float(value)
    if number < 0:
        return "neg"
    if number <= 10:
        return "mid"
    return "hot"


def cond_band(good, bad) -> str:
    if good is None and bad is None:
        return "missing"
    net = int(good or 0) - int(bad or 0)
    if net <= 0:
        return "le0"
    if net <= 3:
        return "m1_3"
    return "ge4"


def days_band(days: int) -> str:
    if days <= 1:
        return "1"
    if days <= 5:
        return "2_5"
    return "ge6"


def weather_band(score) -> str:
    if score is None:
        return "missing"
    number = float(score)
    if number <= HARD_RED:
        return "hard_red"
    if number <= 0:
        return "down"
    if number < 5:
        return "up"
    return "strong"


def tone(boxes: dict | None, key: str) -> str:
    return str((boxes or {}).get(key) or "missing").lower()


def feature_bins(row: dict, *, entry_px: float | None, days_on_list: int, score) -> dict[str, str]:
    boxes = row.get("boxes") or {}
    news = str(row.get("news_box") or tone(boxes, "news")).lower()
    if news not in {"good", "bad", "neutral", "missing"}:
        news = "other"
    return {
        "alarm": "yes" if row.get("alarm") else "no",
        "blue": "yes" if row.get("blue") else "no",
        "candle": "yes" if row.get("candle_capture") else "no",
        "cond_band": cond_band(row.get("cond_good"), row.get("cond_bad")),
        "days_on_list": days_band(int(days_on_list)),
        "earn": "react" if row.get("erd_earn_react") else "no",
        "heat": tone(boxes, "heat"),
        "news": news,
        "price_band": price_band(entry_px),
        "ret5_band": ret5_band(row.get("ohlc_ret_5")),
        "vol": tone(boxes, "vol"),
        "weather": weather_band(score),
        "zero_red": "yes" if row.get("zero_red") else "no",
    }


def _win_rate(rows: list[dict]) -> float | None:
    if not rows:
        return None
    return sum(1 for row in rows if row["win"]) / len(rows)


def grow_tree(rows: list[dict], depth: int, min_n: int = MIN_TRADES) -> list[dict]:
    """One-vs-rest splits. Both sides need min_n. Ties take the feature, then the value.

    The split uses the win label, so it is a description of trades already
    taken. It is not a rule for a later book. Depth is 2 or 3.
    """
    leaves: list[dict] = []

    def walk(node: list[dict], conds: tuple[tuple[str, str, str], ...], left: int) -> None:
        if left <= 0 or len(node) < min_n * 2:
            leaves.append(_leaf(node, conds))
            return
        best = None
        for feature in BIN_FEATURES:
            values = sorted({row["bins"][feature] for row in node})
            for value in values:
                left_rows = [row for row in node if row["bins"][feature] == value]
                right_rows = [row for row in node if row["bins"][feature] != value]
                if len(left_rows) < min_n or len(right_rows) < min_n:
                    continue
                gap = abs((_win_rate(left_rows) or 0.0) - (_win_rate(right_rows) or 0.0))
                key = (-gap, feature, value)
                if best is None or key < best[0]:
                    best = (key, feature, value, left_rows, right_rows)
        if best is None:
            leaves.append(_leaf(node, conds))
            return
        _key, feature, value, left_rows, right_rows = best
        walk(left_rows, conds + ((feature, "==", value),), left - 1)
        walk(right_rows, conds + ((feature, "!=", value),), left - 1)

    walk(list(rows), (), depth)
    return leaves


def _leaf(rows: list[dict], conds: tuple[tuple[str, str, str], ...]) -> dict:
    rets = [float(row["ret"]) for row in rows]
    wins = sum(1 for row in rows if row["win"])
    n = len(rows)
    return {
        "conds": [{"feature": feature, "op": op, "value": value} for feature, op, value in conds],
        "flag_lt_30": n < MIN_TRADES,
        "mean_ret": mean(rets),
        "n": n,
        "win_rate": (wins / n) if n else None,
    }


def top_leaves(leaves: list[dict], cap: int = COMBO_TABLE_CAP) -> list[dict]:
    """Table order only. A larger gap from 50% is listed first. Nothing is selected."""
    eligible = [leaf for leaf in leaves if leaf["n"] >= MIN_TRADES and leaf["win_rate"] is not None]
    eligible.sort(key=lambda leaf: (
        -abs(float(leaf["win_rate"]) - 0.5),
        -int(leaf["n"]),
        json.dumps(leaf["conds"], sort_keys=True),
    ))
    return eligible[:cap]


def leaf_matches(bins: dict[str, str], conds: list[dict]) -> bool:
    for cond in conds:
        got = bins[cond["feature"]]
        if cond["op"] == "==" and got != cond["value"]:
            return False
        if cond["op"] == "!=" and got == cond["value"]:
            return False
    return True


def load_drop() -> dict:
    payload = json.loads(DROP_PATH.read_text(encoding="utf-8"))
    dropped = list(payload["dropped"])
    if dropped != sorted(set(dropped)) or len(dropped) != 76:
        raise SystemExit("drop list")
    if payload["matched_splits"] != list(MATCHED_SPLITS):
        raise SystemExit("matched splits")
    if "YAAS" in dropped:
        raise SystemExit("YAAS is not one of the 76")
    return payload


def load_recipes() -> list[dict]:
    if file_sha256(RECIPES) != RECIPES_SHA256:
        raise SystemExit("RECIPES.json sha mismatch")
    payload = json.loads(RECIPES.read_text(encoding="utf-8"))
    names = [row["name"] for row in payload]
    if names != list(SUBJECTS):
        raise SystemExit("subject order")
    return payload


def load_inputs() -> dict:
    if file_sha256(INPUTS) != INPUTS_SHA256:
        raise SystemExit("INPUTS.json sha mismatch")
    payload = json.loads(INPUTS.read_text(encoding="utf-8"))
    if list(payload["dates"]) != list(SESSIONS):
        raise SystemExit("input dates")
    return payload
