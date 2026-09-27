"""Start-day sweep rules. This module does not walk a tape and does not pick a recipe."""
from __future__ import annotations

import statistics

from research.concentration_cap_v3.protocol import LUCK_N as LUCK_N
from research.concentration_cap_v3.protocol import candidates as cap_candidates
from research.concentration_screen_v1.protocol import g3_recipe
from research.factor_mine_recipe_search_v4.protocol import (
    CAPITAL,
    FORWARD,
    SESSIONS,
    STARTS,
    TUNE,
    candidates as v4_candidates,
)
from src.lever_search_proof import build_group3_recipes

STUDY = "start_day_sweep_v1"
# Same luck total as concentration_cap_v3. This report selects nothing, so it adds no tries.
NEW_TRIES = 0
EPS = 1e-12
END_SESSION = "2026-09-25"
FIRST_START = "2026-08-13"
LAST_START = "2026-09-24"
P2 = FORWARD
P2_V4_ANCHOR = "2026-08-17"
P2_OTHER_ANCHOR = "2026-08-13"
SCREEN_STARTS = STARTS

V4_IDS = (
    "union_hot_n4_holdup__w0",
    "union_hot_n4_holdup__w1",
    "union_hot_n4_h1__w0",
    "union_hot_n4_h1__w1",
    "union_hot_n4_h1_nonews__w0",
    "union_hot_n4_h1_nonews__w1",
    "union_hot_n4_h1_time__w0",
    "union_hot_score_h3__w0",
    "union_hot_score_h3__w1",
)
G3_IDS = (
    "union_ret_5_h3",
    "union_vol_ab_h1",
)
CAP_IDS = (
    "union_hot_n4_holdup__w0__n4__c20",
    "union_hot_n4_h1_nonews__w0__n4__c20",
)

START_DAYS = tuple(day for day in SESSIONS if FIRST_START <= day <= LAST_START)


def _v4_recipe(ident: str) -> dict:
    found = {row["id"]: row for row in v4_candidates()}
    recipe = dict(found[ident])
    recipe["exit_when"] = dict(recipe.get("exit_when") or {})
    recipe["forbid"] = dict(recipe.get("forbid") or {})
    recipe["require"] = dict(recipe.get("require") or {})
    return recipe


def _g3_recipe(ident: str) -> dict:
    found = {row["name"]: row for row in build_group3_recipes()}
    recipe = g3_recipe(found[ident])
    recipe = dict(recipe)
    recipe["id"] = ident
    return recipe


def _cap_recipe(ident: str) -> dict:
    found = {row["id"]: row for row in cap_candidates()}
    recipe = dict(found[ident])
    recipe["exit_when"] = dict(recipe.get("exit_when") or {})
    recipe["forbid"] = dict(recipe.get("forbid") or {})
    recipe["require"] = dict(recipe.get("require") or {})
    return recipe


def recipes() -> tuple[dict, ...]:
    """The thirteen books, in the report order. v4 uses the v4 walker. The rest use cap v3 or Group 3."""
    rows = []
    for ident in V4_IDS:
        rows.append({
            "anchor": P2_V4_ANCHOR,
            "engine": "v4",
            "family": "v4",
            "id": ident,
            "recipe": _v4_recipe(ident),
        })
    for ident in G3_IDS:
        rows.append({
            "anchor": P2_OTHER_ANCHOR,
            "engine": "v4",
            "family": "g3",
            "id": ident,
            "recipe": _g3_recipe(ident),
        })
    for ident in CAP_IDS:
        rows.append({
            "anchor": P2_OTHER_ANCHOR,
            "engine": "cap",
            "family": "cap_v3",
            "id": ident,
            "recipe": _cap_recipe(ident),
        })
    return tuple(rows)


def assert_recipe_grid() -> None:
    rows = recipes()
    ids = [row["id"] for row in rows]
    if ids != list(V4_IDS) + list(G3_IDS) + list(CAP_IDS):
        raise RuntimeError("recipe order")
    if len(ids) != len(set(ids)):
        raise RuntimeError("duplicate recipe")
    if len(START_DAYS) != 30 or START_DAYS[0] != FIRST_START or START_DAYS[-1] != LAST_START:
        raise RuntimeError("start days")
    if END_SESSION not in SESSIONS or END_SESSION in START_DAYS:
        raise RuntimeError("end session")
    if P2[0] != "2026-09-14" or P2[-1] != END_SESSION or len(P2) != 10:
        raise RuntimeError("p2 window")
    if LUCK_N != 22_009 or NEW_TRIES != 0:
        raise RuntimeError("luck")
    by_id = {row["id"]: row for row in rows}
    ret5 = by_id["union_ret_5_h3"]["recipe"]
    if ret5["top_n"] != 8 or ret5["hold"] != 3 or ret5["rank"] != "ret_5":
        raise RuntimeError("union_ret_5_h3")
    if ret5.get("weather") or ret5["forbid"] != {"alarm": True}:
        raise RuntimeError("union_ret_5_h3 gates")
    vol = by_id["union_vol_ab_h1"]["recipe"]
    if vol["top_n"] != 8 or vol["hold"] != 1:
        raise RuntimeError("union_vol_ab_h1")
    if vol["require"] != {"vol": "good", "ab": "good"}:
        raise RuntimeError("union_vol_ab_h1 require")
    if vol["forbid"] != {"alarm": True, "news": "bad"} or vol.get("weather"):
        raise RuntimeError("union_vol_ab_h1 gates")
    for ident in CAP_IDS:
        recipe = by_id[ident]["recipe"]
        if recipe["top_n"] != 4 or recipe.get("weight_cap") != 0.2:
            raise RuntimeError(ident)
        if recipe.get("cap_cash") != "sit":
            raise RuntimeError("cap cash")
    if by_id["union_hot_n4_holdup__w0"]["recipe"]["weather"] is not False:
        raise RuntimeError("w0 weather")
    if by_id["union_hot_n4_holdup__w1"]["recipe"]["weather"] is not True:
        raise RuntimeError("w1 weather")


def day_kind(ret: float) -> str:
    """Up, down, or flat/sat-out. A move inside 1e-12 of zero is flat."""
    value = float(ret)
    if value > EPS:
        return "up"
    if value < -EPS:
        return "down"
    return "flat_sat"


def order_trades(day: dict) -> int:
    """Buy, sell, and trim orders at the open. A held name that is left alone is not a trade."""
    return (
        len(day.get("bought") or [])
        + len(day.get("sold") or [])
        + len(day.get("trimmed") or [])
    )


def ending_return(equity: float) -> float:
    return float(equity) / float(CAPITAL) - 1.0


def summarize_starts(starts: list[dict]) -> dict:
    rets = [float(row["ret"]) for row in starts]
    equities = [float(row["end_equity"]) for row in starts]
    equities_15 = [float(row["end_equity_15"]) for row in starts]
    return {
        "median_ret": statistics.median(rets),
        "n_starts": len(starts),
        "positive": sum(1 for equity in equities if equity > CAPITAL),
        "positive_15": sum(1 for equity in equities_15 if equity > CAPITAL),
        "worst_ret": min(rets),
    }


def summarize_p2(daily: list[dict], closed: list[dict]) -> dict:
    wanted = set(P2)
    days = [day for day in daily if day["session"] in wanted]
    if [day["session"] for day in days] != list(P2):
        raise RuntimeError("p2 sessions")
    sessions = []
    for day in days:
        trades = order_trades(day)
        kind = day_kind(day["ret"])
        closed_n = sum(1 for trade in closed if trade["exit"] == day["session"])
        sessions.append({
            "bought": len(day.get("bought") or []),
            "closed": closed_n,
            "kind": kind,
            "ret": float(day["ret"]),
            "ret_15": float(day["ret_15"]),
            "sat_out": bool(kind == "flat_sat" and trades == 0),
            "session": day["session"],
            "sold": len(day.get("sold") or []),
            "trades": trades,
            "trimmed": len(day.get("trimmed") or []),
        })
    return {
        "down": sum(1 for row in sessions if row["kind"] == "down"),
        "flat_sat": sum(1 for row in sessions if row["kind"] == "flat_sat"),
        "sat_out": sum(1 for row in sessions if row["sat_out"]),
        "sessions": sessions,
        "up": sum(1 for row in sessions if row["kind"] == "up"),
    }


def positive_phrase(count: int, total: int) -> str:
    return f"positive from {int(count)} of {int(total)} start days"
