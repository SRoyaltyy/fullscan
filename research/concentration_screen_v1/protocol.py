"""Report-only concentration screen. No score is a pick and nothing is frozen."""
from __future__ import annotations

from research.factor_mine_recipe_search_v4.protocol import (
    FORWARD,
    STARTS,
    TUNE,
    V4_TRIES,
    candidates,
)
from src.lever_search_proof import build_group3_recipes

P1_ALL = TUNE
P2 = FORWARD
LUCK_PRIOR = 21536
N_G3 = 110
N_V4 = 150
N_ROWS = N_G3 + N_V4
LUCK_N = LUCK_PRIOR + N_ROWS
MIN_TRADES = 30
NAMED = "union_vol_ab_h1"
# A return this close to zero is flat, not a positive screen pass.
POSITIVE_EPS = 1e-12


def g3_recipe(raw: dict) -> dict:
    """Group 3 recipe on the keep-held book. Weather is off; these 110 have no weather gate."""
    return {
        "earn_news": False,
        "exit_when": dict(raw.get("exit_when") or {}),
        "forbid": dict(raw.get("forbid") or {}),
        "hold": int(raw.get("hold") or 1),
        "id": f"g3:{raw['name']}",
        "name": raw["name"],
        "rank": raw.get("rank"),
        "require": dict(raw.get("require") or {}),
        "s_boost": "none",
        "sell": "list",
        "side": raw.get("side") or "long",
        "skip_first": False,
        "top_n": int(raw.get("top_n") or 8),
        "universe": raw.get("universe") or "union",
        "weather": False,
    }


def screen_specs() -> tuple[dict, ...]:
    """110 Group 3 recipes, then v4's 150 cells (part × weather × Monday start)."""
    rows = []
    for raw in build_group3_recipes():
        recipe = g3_recipe(raw)
        rows.append({
            "family": "g368",
            "id": recipe["id"],
            "p1": P1_ALL,
            "recipe": recipe,
            "start": P1_ALL[0],
        })
    for spec in candidates():
        for start in STARTS:
            recipe = dict(spec)
            recipe["id"] = f"v4:{spec['id']}@{start}"
            p1 = tuple(day for day in P1_ALL if day >= start)
            rows.append({
                "family": "v4",
                "id": recipe["id"],
                "p1": p1,
                "recipe": recipe,
                "start": start,
            })
    return tuple(rows)


def assert_grid() -> None:
    specs = screen_specs()
    if len(build_group3_recipes()) != N_G3:
        raise RuntimeError("group3 count")
    if V4_TRIES != N_V4 or len(candidates()) * len(STARTS) != N_V4:
        raise RuntimeError("v4 cell count")
    if len(specs) != N_ROWS or LUCK_N != 21796:
        raise RuntimeError("screen rows")
    if not any(row["id"] == f"g3:{NAMED}" for row in specs):
        raise RuntimeError("named recipe missing")
    ids = [row["id"] for row in specs]
    if len(ids) != len(set(ids)):
        raise RuntimeError("duplicate id")
