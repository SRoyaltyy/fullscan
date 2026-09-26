"""Frozen Group 3 input days from the pinned FULLSCAN_FILE_PROOF.csv.

The CSV is commit 1bf94d7 on PR #354. Later commits on that branch are not
this pin. Dates below are the days that pin marks usable.
"""
from __future__ import annotations

PROOF_COMMIT = "1bf94d7dc3ce00c29667d0fae6fdb6bb8effb73c"
PROOF_BLOB_SHA = "832eaa4fdc19d368d87a4dad5c3edd3c200b0a54"
PROOF_SHA256 = "1bfafd9d4f9a512b9bc3ac33d6d087349028f57ec4d0fedffe76ee0998e48f75"
PROOF_PATH = "research/audit/FULLSCAN_FILE_PROOF.csv"

HEADLINE_INPUTS = frozenset({
    "actions",
    "baseline",
    "catalyst",
    "digest",
    "events",
    "judge",
    "map_heat",
    "market_digest",
    "parsed",
    "research",
})

# Run window and check calendar already locked for this study.
RUN_WINDOW: tuple[str, ...] = (
    "2026-08-07", "2026-08-10", "2026-08-11", "2026-08-12",
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11",
)
CHECK_CALENDAR: tuple[str, ...] = tuple(
    day for day in RUN_WINDOW if day >= "2026-08-20"
)

PROVEN_DATES: dict[str, tuple[str, ...]] = {
    "stock_book": ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-27", "2026-08-31", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24"),
    "S": ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-26", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25"),
    "predict": ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-26", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25"),
    "hard_red": ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-26", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25"),
    "sector_predict": ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24"),
    "ab_checklist": ("2026-08-24", "2026-08-25", "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-21", "2026-09-22", "2026-09-24"),
    "ab_enriched": ("2026-08-24", "2026-08-25", "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-21", "2026-09-22", "2026-09-25"),
    "actions": ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-26", "2026-08-27", "2026-08-28", "2026-09-25"),
    "judge": ("2026-08-21", "2026-08-24", "2026-08-26", "2026-08-27", "2026-08-28", "2026-09-25"),
    "map_heat": ("2026-08-26", "2026-08-27", "2026-08-28", "2026-09-25"),
    "catalyst": ("2026-08-28", "2026-09-25"),
    "join": (),
    "digest": ("2026-08-20", "2026-08-21", "2026-08-24", "2026-08-26", "2026-08-27", "2026-08-28", "2026-09-25"),
    "parsed": ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-26", "2026-08-27", "2026-08-28", "2026-09-25"),
    "export": ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-24", "2026-08-25", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-21", "2026-09-22"),
    "weather": ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-24", "2026-08-25", "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25"),
    "peers": (),
}

_CARD = frozenset({"blue", "zero_red", "alarm"})
_EXPORT = frozenset({
    "earn_react", "days_since_E_max", "flag_E_min", "days_since_R_max", "flag_R",
})
_PRICE_RANKS = frozenset({"hot_score", "candle_score", "ret_5"})


def row_is_usable(*, proven: str, stale_content: str, headline: bool) -> bool:
    """proven=yes counts, including PROVEN_BUT_CHANGED. Headlines also need stale_content=no."""
    if proven != "yes":
        return False
    if headline and stale_content != "no":
        return False
    return True


def input_day_is_usable(rows: list[dict], *, headline: bool) -> bool:
    """A day counts only when every file of that input is usable. A directory placeholder does not."""
    if not rows:
        return False
    if any(str(row.get("path") or "").endswith("/") for row in rows):
        return False
    return all(
        row_is_usable(
            proven=str(row.get("proven") or ""),
            stale_content=str(row.get("stale_content") or ""),
            headline=headline,
        )
        for row in rows
    )


def _usable(day: str, role: str) -> bool:
    if role == "ab":
        return day in set(PROVEN_DATES["ab_checklist"]) and day in set(PROVEN_DATES["ab_enriched"])
    return day in set(PROVEN_DATES[role])


def build_group3_recipes() -> list[dict]:
    """The 110 recipes from commit 8e8c36a. Not today's expanded build_recipes()."""
    recs: list[dict] = []

    def add(**kw):
        recs.append({
            "name": kw["name"],
            "universe": kw.get("universe", "union"),
            "hold": int(kw.get("hold", 1)),
            "side": kw.get("side", "long"),
            "top_n": int(kw.get("top_n", 8)),
            "require": dict(kw.get("require") or {}),
            "forbid": dict(kw.get("forbid") or {}),
            "rank": kw.get("rank"),
            "exit_when": dict(kw.get("exit_when") or {}),
            "note": kw.get("note", ""),
            "trades_at_open": True,
        })

    for uni in ("union", "flatten", "probable", "yday_gainer", "ohlc_hot"):
        for hold in (1, 3, 5):
            add(name=f"{uni}_h{hold}", universe=uni, hold=hold, note="baseline list, no extra gate")
    gates = [
        ("vol_g", {"vol": "good"}),
        ("vol_missing", {"vol": "missing"}),
        ("ab_g", {"ab": "good"}),
        ("join_g", {"join": "good"}),
        ("join_present", {"join_present": True}),
        ("news_g", {"news": "good"}),
        ("news_present", {"news_present": True}),
        ("news_missing", {"news": "missing"}),
        ("catal_present", {"catal_present": True}),
        ("blue", {"blue": True}),
        ("white", {"zero_red": True}),
        ("last_green", {"last_green": True}),
        ("last_red", {"last_red": True}),
        ("candle", {"candle_capture": True}),
        ("coil_off", {"ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_min": 0.7, "rvol_max": 2.2}),
        ("earn_react", {"earn_react": True}),
        ("e_fresh", {"days_since_E_max": 1, "flag_E_min": 0}),
        ("r_up", {"days_since_R_max": 5, "flag_R": 1}),
        ("break10", {"break_10": True}),
    ]
    for gname, req in gates:
        for hold in (1, 3):
            add(name=f"union_{gname}_h{hold}", universe="union", hold=hold, require=req, forbid={"alarm": True})
    for gname in ("vol_g", "coil_off", "last_green", "news_g", "white"):
        req = next(item for name, item in gates if name == gname)
        add(name=f"union_{gname}_h5", universe="union", hold=5, require=req, forbid={"alarm": True})
    combos = [
        ("vol_ab", {"vol": "good", "ab": "good"}),
        ("blue_vol", {"vol": "good", "blue": True}),
        ("news_vol", {"news": "good", "vol": "good"}),
        ("e_green", {"earn_react": True, "last_green": True}),
        ("probable_ok", {"last_green": True, "ret_5_max": 10.0}),
        ("vol_green", {"vol": "good", "last_green": True}),
        ("coil_green", {"last_green": True, "ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_min": 0.7, "rvol_max": 2.2}),
        ("blue_coil", {"blue": True, "ret_5_max": 10.0}),
        ("join_vol_green", {"join": "good", "vol": "good", "last_green": True}),
        ("white_coil", {"zero_red": True, "ret_5_max": 10.0, "rvol_max": 2.2}),
    ]
    for gname, req in combos:
        uni = "probable" if gname.startswith("probable") else "union"
        for hold in (1, 3):
            add(name=f"{uni}_{gname}_h{hold}", universe=uni, hold=hold, require=req, forbid={"alarm": True, "news": "bad"})
    add(name="flatten_vol_g_h3", universe="flatten", hold=3, require={"vol": "good"}, forbid={"alarm": True})
    add(name="ohlc_hot_coil_h1", universe="ohlc_hot", hold=1, require={"ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_max": 2.2}, forbid={"alarm": True})
    for rank in ("hot_score", "candle_score", "ret_5", "cond", "w_hot_cond", "w_hot_candle"):
        add(name=f"union_{rank}_h1", universe="union", hold=1, rank=rank, forbid={"alarm": True})
        add(name=f"union_{rank}_h3", universe="union", hold=3, rank=rank, forbid={"alarm": True})
    add(name="union_hot_n4_h1", universe="union", hold=1, top_n=4, rank="hot_score", forbid={"alarm": True})
    add(name="union_hot_n12_h1", universe="union", hold=1, top_n=12, rank="hot_score", forbid={"alarm": True})
    add(name="union_cond_n4_h3", universe="union", hold=3, top_n=4, rank="cond", forbid={"alarm": True})
    add(name="union_h3_exit_alarm", universe="union", hold=3, forbid={"alarm": True}, exit_when={"alarm": True})
    add(name="union_h5_exit_alarm", universe="union", hold=5, forbid={"alarm": True}, exit_when={"alarm": True})
    add(name="union_h3_exit_red", universe="union", hold=3, require={"last_green": True}, forbid={"alarm": True}, exit_when={"last_red": True})
    add(name="union_h3_exit_news_r", universe="union", hold=3, forbid={"alarm": True, "news": "bad"}, exit_when={"news": "bad"})
    add(name="coil_h3_exit_alarm", universe="union", hold=3, require={"ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_max": 2.2}, forbid={"alarm": True}, exit_when={"alarm": True})
    for name, req in (
        ("short_alarm", {"alarm": True}),
        ("short_news_r", {"news": "bad"}),
        ("short_r_down", {"flag_R": -1, "days_since_R_max": 5}),
        ("short_extended", {"ret_5_min": 15.0}),
        ("short_last_red", {"last_red": True}),
    ):
        for hold in (1, 3):
            add(name=f"{name}_h{hold}", universe="union", hold=hold, side="short", require=req)
    return recs


def required_roles(recipe: dict) -> frozenset[str]:
    """Fullscan roles the recipe must have that day. Price features are not roles."""
    keys: set[str] = set()
    for block in (recipe.get("require"), recipe.get("forbid"), recipe.get("exit_when")):
        keys |= set(block or {})
    roles: set[str] = set()
    if "ab" in keys:
        roles.add("ab")
    if keys & {"join", "join_present"}:
        roles.add("join")
    if any("news" in key for key in keys):
        roles.add("actions")
    if "catal_present" in keys:
        roles.add("catalyst")
    if keys & _CARD:
        roles.add("stock_book")
    if keys & _EXPORT:
        roles.add("export")
    if recipe.get("rank") not in (None, *_PRICE_RANKS):
        roles.add("stock_book")
    if recipe.get("universe") == "flatten":
        roles.add("stock_book")
    return frozenset(roles)


def search_days(recipe: dict) -> tuple[str, ...]:
    """Run-window sessions where every required input is usable."""
    need = required_roles(recipe)
    return tuple(day for day in RUN_WINDOW if all(_usable(day, role) for role in need))


def check_days(recipe: dict) -> tuple[str, ...]:
    """Search days that also sit on the 16-day check calendar. A sit-out is not a check day."""
    return tuple(day for day in search_days(recipe) if day in CHECK_CALENDAR)


def _median(values: list[int]) -> float:
    ordered = sorted(values)
    count = len(ordered)
    mid = count // 2
    if count % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2


def group_day_report() -> list[dict]:
    """One row per input-requirement group. Every recipe in a group shares the counts."""
    recipes = build_group3_recipes()
    buckets: dict[frozenset[str], list[dict]] = {}
    for recipe in recipes:
        buckets.setdefault(required_roles(recipe), []).append(recipe)
    rows = []
    for roles, group in buckets.items():
        searches = [len(search_days(recipe)) for recipe in group]
        checks = [len(check_days(recipe)) for recipe in group]
        rows.append({
            "roles": tuple(sorted(roles)),
            "n_recipes": len(group),
            "names": tuple(recipe["name"] for recipe in group),
            "search_min": min(searches),
            "search_median": _median(searches),
            "search_max": max(searches),
            "check_min": min(checks),
            "check_median": _median(checks),
            "check_max": max(checks),
        })
    rows.sort(key=lambda row: (-row["n_recipes"], row["roles"]))
    return rows


def overall_day_report() -> dict:
    recipes = build_group3_recipes()
    searches = [len(search_days(recipe)) for recipe in recipes]
    checks = [len(check_days(recipe)) for recipe in recipes]
    return {
        "n_recipes": len(recipes),
        "search_min": min(searches),
        "search_median": _median(searches),
        "search_max": max(searches),
        "check_min": min(checks),
        "check_median": _median(checks),
        "check_max": max(checks),
    }
