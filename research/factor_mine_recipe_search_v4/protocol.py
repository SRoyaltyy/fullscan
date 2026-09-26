"""Locked rules for factor_mine_recipe_search_v4. No score is imported here."""
from __future__ import annotations

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STUDY = "factor_mine_recipe_search_v4"
HERE = ROOT / "research" / STUDY
PREREG = HERE / "PREREG.md"
INPUTS = HERE / "INPUTS.json"
MANIFEST = HERE / "MANIFEST.json"
RETURNS = HERE / "returns"
FREEZE = HERE / "freeze" / "FREEZE.json"
MARKER = "<!-- BEGIN COVERED -->\n"

CAPITAL = 10_000.0
HARD_RED = -3.0
HOLDUP_S = 0.0
HOLDUP_SESS = 2
RANDOM_SEED = 20260813
RANDOM_DRAWS = 1000
RANDOM_N = 4
MIN_TRADES = 30
REJECT_JOINT = 0.5
CREATION = "2026-09-26"

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
# Monday sessions inside 2026-08-17..2026-09-08. 2026-09-07 is not a session.
STARTS = ("2026-08-17", "2026-08-24", "2026-08-31")

CLEAN_PATH = "research/breadth_rank_v1c/bars/ohlc.parquet"
CLEAN_COMMIT = "9a0e12ca2f7a3e87f965190e8766053a37ffaabc"
CLEAN_BLOB = "e7bbac335cfa87243f9d0ddf8c331a0739dd0df8"
CLEAN_SHA256 = "5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2"
SPLIT_SHA256 = "24f342feb5559faf0ddaa091257ea45d9b7424c9c3e5da83886b134022fa28fc"
FEES_SHA256 = "019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3"

# Cells actually walked, not a later study's cumulative headline.
TRIES_364 = 34 * 3 * 1
TRIES_366 = 34 * 3 * 13
TRIES_367 = 34 * 3 * 13
TRIES_368 = 9500
TRIES_THEME = 9132

BASE_NAME = "union_hot_score_h3"
PICKED_NAME = "union_hot_n4_h1"
DROP_NAMES = ("CYPH", "GLND", "INDP")

_ALARM = {"alarm": True}
_NONEWS = {"alarm": True, "news": "bad"}
_COIL = {"ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_min": 0.7, "rvol_max": 2.2}
_GREEN = {"last_green": True}
_VOL = {"vol": "good"}


def _part(name, *, rank="hot_score", top_n=8, hold=3, sell="list",
          s_boost="none", require=None, forbid=None, exit_when=None,
          earn_news=False, skip_first=False, base=False, already_picked=False):
    return {
        "already_picked": already_picked,
        "base": base,
        "earn_news": earn_news,
        "exit_when": dict(exit_when or {}),
        "forbid": dict(forbid or _ALARM),
        "hold": hold,
        "name": name,
        "rank": rank,
        "require": dict(require or {}),
        "s_boost": s_boost,
        "sell": sell,
        "side": "long",
        "skip_first": skip_first,
        "top_n": top_n,
        "universe": "union",
    }


# 23 part rows keep the must/must-not grid. The two #365 patterns are base-shaped
# rows only. Crossing them on and off through every other part would be 184
# candidates, past the 50 cap, so they are not a second multiplier.
PARTS = (
    _part(BASE_NAME, base=True),
    _part(PICKED_NAME, top_n=4, hold=1, already_picked=True),
    _part("union_hot_n4_holdup", top_n=4, hold=1, s_boost="holdup"),
    _part("union_candle_score_h3", rank="candle_score"),
    _part("union_cond_h3", rank="cond"),
    _part("union_ret_5_h3", rank="ret_5"),
    _part("union_w_hot_cond_h3", rank="w_hot_cond"),
    _part("union_w_hot_candle_h3", rank="w_hot_candle"),
    _part("union_hot_n12_h1", top_n=12, hold=1),
    _part("union_hot_n4_h3", top_n=4, hold=3),
    _part("union_hot_score_h1", hold=1),
    _part("union_hot_n4_h5", top_n=4, hold=5),
    _part("union_hot_score_h5", hold=5),
    _part("union_hot_score_h3_time", sell="time"),
    _part("union_hot_n4_h1_time", top_n=4, hold=1, sell="time"),
    _part("union_hot_score_h3_exitalarm", exit_when={"alarm": True}),
    _part("union_hot_score_h3_holdup", s_boost="holdup"),
    _part("union_hot_score_h3_green", require=_GREEN),
    _part("union_hot_n4_h1_green", top_n=4, hold=1, require=_GREEN),
    _part("union_hot_score_h3_coil", require=_COIL),
    _part("union_hot_score_h3_nonews", forbid=_NONEWS),
    _part("union_hot_n4_h1_nonews", top_n=4, hold=1, forbid=_NONEWS),
    _part("union_hot_score_h3_vol", require=_VOL),
    _part("union_hot_score_h3_earnnews", earn_news=True),
    _part("union_hot_score_h3_skipfirst", skip_first=True),
)


def candidate_id(name: str, weather: bool) -> str:
    return f"{name}__w{1 if weather else 0}"


def candidates() -> tuple[dict, ...]:
    out = []
    for part in PARTS:
        for weather in (True, False):
            row = dict(part)
            row["weather"] = weather
            row["id"] = candidate_id(part["name"], weather)
            out.append(row)
    return tuple(out)


BASE_ID = candidate_id(BASE_NAME, True)
PICKED_ID = candidate_id(PICKED_NAME, True)
N_PARTS = len(PARTS)
N_CANDIDATES = N_PARTS * 2
V4_TRIES = N_PARTS * len(STARTS) * 2
LUCK_N = V4_TRIES + TRIES_368 + TRIES_THEME + TRIES_364 + TRIES_366 + TRIES_367


def covered_bytes(text: str) -> bytes:
    idx = text.find(MARKER)
    if idx < 0:
        raise RuntimeError("covered marker missing")
    return text[idx + len(MARKER):].encode("utf-8")


def fingerprint_sha256(text: str) -> str:
    return hashlib.sha256(covered_bytes(text)).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def hard_red(score) -> bool:
    if score is None:
        return False
    return float(score) <= HARD_RED
