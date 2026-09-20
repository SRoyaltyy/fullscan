"""Blind ≤2026-09-09 factor-mine FORMATION remine.

A researcher on the close of 2026-09-09 would have had the recipe
builders at ``cb7f09ae`` (combo books #175) and the leak-free panel
through that session. They would not have had:

  * Clock-B catalogue pins (2026-09-19)
  * ``union_hot_n4_holdup`` / holdup s_boost (2026-09-19)
  * overnight_mega sleeves (2026-09-19)
  * fee-KEEP / WORKABLE_ALWAYS Pages pins (2026-09-19/20)
  * FOCUS / LONG_LED_PIN live-board seeds

This harness freezes that 9/9 recipe + combo-spec menu, scores it with
the current cash-book engine on 2026-08-13 → 2026-09-09 only, then
**forms additional combos** from the IS winners using the same shared /
50-50 / 70-30 / 333 primitives. Featured names are chosen from IS
stats only (Cyrus: Starts YES + Book%). OOS 9/10–9/18 never ranks.

Not a KEEP-selection cut of today's live menu (that was PR #285).

CLI: python -m src.factor_mine --from-date 2026-08-13 --to-date 2026-09-09 \\
       --write --auto-tweak --holdout --blind-0909 \\
       --out-root 03_scoreboard/factor_mine_blind_0909 \\
       --dash-dir dashboard/factor-mine-blind-0909 \\
       --asof-md 03_scoreboard/FACTOR_MINE_BLIND_0909.md
"""
from __future__ import annotations

import json
from datetime import datetime
from itertools import combinations
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_asof as fma
from . import factor_mine_book as fmb
from . import factor_mine_combo as fmc
from . import ticker_lookback as tl

CUTOFF = "2026-09-09"
OOS_START = "2026-09-10"
FROM_DATE = "2026-08-13"
RECIPE_FREEZE_SHA = "cb7f09ae"
RECIPE_FREEZE_NOTE = (
    "Recipe + combo-spec definitions frozen to commit cb7f09ae "
    "(2026-09-09, Factor-mine combo books #175). Current cash-book / "
    "mark / fee engine is used to score those frozen definitions. "
    "Clock-B, holdup, overnight_mega, fee-KEEP, and WORKABLE_ALWAYS "
    "were not in the 9/9 builders."
)

# Live-board pins that must not be injected as seeds / ALWAYS extras.
LIVE_FEATURE_PINS = (
    "union_hot_n4_h1",
    "union_hot_n4_holdup",
    "combo_sh_5050_shared",
    "combo_sh_macd_5050_shared",
    "combo_sh_3070_shared",
    "combo_sh_7030_shared",
    "combo_oh_5050_shared",
    "flatten_h5",
    "overnight_mega_h1",
    "overnight_mega_h2",
    "overnight_h1",
)
POST_0909_NAMES = (
    "union_hot_n4_holdup",
    "overnight_mega_h1",
    "overnight_mega_h2",
    "overnight_mega_green_h1",
    "overnight_h1",
    "combo_oh_5050_shared",
    "combo_sh_macd_5050_shared",
    "short_news_r_macd_h3",
)

# Cyrus would-have-featured: start-day win + Book% / asymmetric wins.
CYRUS_MIN_TRADES = 30
CYRUS_MIN_BOOK = 0.0
CYRUS_START_YES = 17
CYRUS_START_N_REF = 19
CYRUS_START_RATE = 0.85
FORM_MIN_TRADES = 20
FORM_MIN_START = 0.50
FORM_MAX_EXTRA = 18

DEFAULT_OUT_ROOT = "03_scoreboard/factor_mine_blind_0909"
DEFAULT_DASH = "dashboard/factor-mine-blind-0909"
DEFAULT_MD = "03_scoreboard/FACTOR_MINE_BLIND_0909.md"


def build_recipes_asof_0909() -> list[dict]:
    """Recipe menu as it existed at end of 2026-09-09 (cb7f09ae)."""
    recs: list[dict] = []

    def add(**kw):
        recs.append(fm.make_recipe(**kw))

    universes = ("union", "flatten", "probable", "yday_gainer", "ohlc_hot")
    for uni in universes:
        for hold in (1, 3, 5):
            add(name=f"{uni}_h{hold}", universe=uni, hold=hold,
                note="baseline list, no extra gate")

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
        ("coil_off", {"ret_5_min": 0.0, "ret_5_max": 10.0,
                      "rvol_min": 0.7, "rvol_max": 2.2}),
        ("earn_react", {"earn_react": True}),
        ("e_fresh", {"days_since_E_max": 1, "flag_E_min": 0}),
        ("r_up", {"days_since_R_max": 5, "flag_R": 1}),
        ("break10", {"break_10": True}),
    ]
    for gname, req in gates:
        for hold in (1, 3):
            add(name=f"union_{gname}_h{hold}", universe="union",
                hold=hold, require=req, forbid={"alarm": True},
                note=f"union ∩ {gname}, no 🚨")

    for gname in ("vol_g", "coil_off", "last_green", "news_g", "white"):
        req = next(r for n, r in gates if n == gname)
        add(name=f"union_{gname}_h5", universe="union", hold=5,
            require=req, forbid={"alarm": True},
            note=f"union ∩ {gname} hold 5, no 🚨")

    combos = [
        ("vol_ab", {"vol": "good", "ab": "good"}),
        ("blue_vol", {"vol": "good", "blue": True}),
        ("news_vol", {"news": "good", "vol": "good"}),
        ("e_green", {"earn_react": True, "last_green": True}),
        ("probable_ok", {"last_green": True, "ret_5_max": 10.0}),
        ("vol_green", {"vol": "good", "last_green": True}),
        ("coil_green", {"last_green": True, "ret_5_min": 0.0, "ret_5_max": 10.0,
                        "rvol_min": 0.7, "rvol_max": 2.2}),
        ("blue_coil", {"blue": True, "ret_5_max": 10.0}),
        ("join_vol_green", {"join": "good", "vol": "good", "last_green": True}),
        ("white_coil", {"zero_red": True, "ret_5_max": 10.0, "rvol_max": 2.2}),
    ]
    for gname, req in combos:
        uni = "probable" if gname.startswith("probable") else "union"
        for hold in (1, 3):
            add(name=f"{uni}_{gname}_h{hold}", universe=uni, hold=hold,
                require=req, forbid={"alarm": True, "news": "bad"},
                note="combo gate")

    add(name="flatten_vol_g_h3", universe="flatten", hold=3,
        require={"vol": "good"}, forbid={"alarm": True},
        note="flatten wish-list ∩ vol🟢")
    for hold in (1, 3, 5):
        add(name=f"flatten_live_h{hold}", universe="flatten", hold=hold,
            require={"live_entry": True},
            note="09:30 tickets only when flatten_robust gate fires (mover)")
    add(name="ohlc_hot_coil_h1", universe="ohlc_hot", hold=1,
        require={"ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_max": 2.2},
        forbid={"alarm": True}, note="hot list ∩ not exploded")

    for rank in ("hot_score", "candle_score", "ret_5", "cond",
                 "w_hot_cond", "w_hot_candle"):
        add(name=f"union_{rank}_h1", universe="union", hold=1, rank=rank,
            forbid={"alarm": True}, note=f"rank by {rank}")
        add(name=f"union_{rank}_h3", universe="union", hold=3, rank=rank,
            forbid={"alarm": True}, note=f"rank by {rank}")

    add(name="union_hot_n4_h1", universe="union", hold=1, top_n=4,
        rank="hot_score", forbid={"alarm": True}, note="top 4 by hot")
    add(name="union_hot_n12_h1", universe="union", hold=1, top_n=12,
        rank="hot_score", forbid={"alarm": True}, note="top 12 by hot")
    add(name="union_cond_n4_h3", universe="union", hold=3, top_n=4,
        rank="cond", forbid={"alarm": True}, note="top 4 by cond")

    add(name="union_h3_exit_alarm", universe="union", hold=3,
        forbid={"alarm": True}, exit_when={"alarm": True},
        note="hold 3d, sell next 09:30 if 🚨")
    add(name="union_h5_exit_alarm", universe="union", hold=5,
        forbid={"alarm": True}, exit_when={"alarm": True},
        note="hold 5d, sell next 09:30 if 🚨")
    add(name="union_h3_exit_red", universe="union", hold=3,
        require={"last_green": True}, forbid={"alarm": True},
        exit_when={"last_red": True},
        note="buy last-green, sell next 09:30 if last bar flipped red")
    add(name="union_h3_exit_news_r", universe="union", hold=3,
        forbid={"alarm": True, "news": "bad"},
        exit_when={"news": "bad"},
        note="hold 3d, sell next 09:30 if news🔴")
    add(name="coil_h3_exit_alarm", universe="union", hold=3,
        require={"ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_max": 2.2},
        forbid={"alarm": True}, exit_when={"alarm": True},
        note="coil, exit on 🚨")

    shorts = [
        ("short_alarm", {"alarm": True}, "alarm"),
        ("short_news_r", {"news": "bad"}, "news🔴"),
        ("short_r_down", {"flag_R": -1, "days_since_R_max": 5}, "downgrade ≤5d"),
        ("short_extended", {"ret_5_min": 15.0}, "ret_5>15"),
        ("short_last_red", {"last_red": True}, "last bar red"),
    ]
    for name, req, note in shorts:
        for hold in (1, 3):
            add(name=f"{name}_h{hold}", universe="union", hold=hold,
                side="short", require=req, note=note)

    bases = [
        dict(name="flatten_h5", universe="flatten", hold=5),
        dict(name="flatten_h3", universe="flatten", hold=3),
        dict(name="flatten_live_h1", universe="flatten", hold=1,
             require={"live_entry": True}),
        dict(name="union_h5", universe="union", hold=5),
        dict(name="union_h3", universe="union", hold=3),
        dict(name="union_h1", universe="union", hold=1),
    ]
    tweaks = [
        ("rankw", dict(size="rank_w", note="rank-weighted leftover")),
        ("topheavy", dict(size="topheavy", note="40% to #1, rest split")),
        ("half", dict(size="half", note="deploy half leftover")),
        ("time", dict(sell="time", note="sell at min-hold even if still listed")),
        ("cut", dict(sell="cut_loser", note="after min-hold, cut −3% losers")),
        ("trail", dict(sell="trail", note="after min-hold, trail 5% off peak")),
        ("sboost", dict(s_boost="both", note="S≥+5: sizeup + more names")),
        ("sizeup", dict(s_boost="sizeup", note="S≥+5: 1.35× leftover")),
    ]
    have = {r["name"] for r in recs}
    for base in bases:
        for suffix, kw in tweaks:
            nm = f"{base['name']}_{suffix}"
            if nm in have:
                continue
            add(name=nm, universe=base["universe"], hold=base["hold"],
                require=base.get("require"),
                size=kw.get("size", "leftover"),
                sell=kw.get("sell", "list"),
                s_boost=kw.get("s_boost", "none"),
                note=kw["note"])
            have.add(nm)
    return recs


def combo_specs_asof_0909() -> list[dict]:
    """Combo engine menu as of 2026-09-09 (no holdup / overnight / macd overlay)."""
    S, E, H = "short_news_r_h3", "union_e_fresh_h3", "union_hot_n4_h1"
    N, J, F = "union_news_g_h1", "union_join_vol_green_h1", "flatten_h5"
    ER, E1 = "union_earn_react_h3", "union_e_fresh_h1"
    elite = (S, E, H, N, J, F, E1, ER)
    abbr = {
        S: "s", E: "e", H: "h", N: "n", J: "j", F: "f",
        E1: "e1", ER: "er",
    }
    specs = []
    seen: set[str] = set()

    def add(name, members, weights, *, net="priority", pool="shared"):
        if name in seen:
            return
        seen.add(name)
        specs.append({
            "name": name,
            "members": list(members),
            "weights": [float(w) for w in weights],
            "net": net,
            "pool": pool,
            "formed": False,
        })

    add("combo_seh_333_shared", [S, E, H], [1, 1, 1])
    add("combo_seh_333_split", [S, E, H], [1, 1, 1], pool="split")
    add("combo_seh_502525_shared", [S, E, H], [50, 25, 25])
    add("combo_seh_502525_split", [S, E, H], [50, 25, 25], pool="split")
    add("combo_seh_404020_shared", [S, E, H], [40, 40, 20])
    add("combo_seh_403525_shared", [S, E, H], [40, 35, 25])
    add("combo_seh_451540_shared", [S, E, H], [45, 15, 40])
    add("combo_seh_601525_shared", [S, E, H], [60, 15, 25])
    add("combo_se_5050_shared", [S, E], [1, 1])
    add("combo_se_7030_shared", [S, E], [70, 30])
    add("combo_eh_5050_shared", [E, H], [1, 1])
    add("combo_sh_5050_shared", [S, H], [1, 1])
    add("combo_se_5050_skip", [S, E], [1, 1], net="skip")
    add("combo_seh_333_skip", [S, E, H], [1, 1, 1], net="skip")
    add("combo_seh_333_weather", [S, E, H], [1, 1, 1], net="weather")
    add("combo_se_5050_weather", [S, E], [1, 1], net="weather")
    add("combo_se_5050_split", [S, E], [1, 1], pool="split")
    add("combo_es_8020_shared", [E, S], [80, 20])
    add("combo_es_9010_shared", [E, S], [90, 10])
    add("combo_ehs_702010_shared", [E, H, S], [70, 20, 10])
    add("combo_ehs_601525_shared", [E, H, S], [60, 15, 25])
    add("combo_sn_5050_shared", [S, N], [1, 1])
    add("combo_sj_5050_shared", [S, J], [1, 1])
    add("combo_sf_5050_shared", [S, F], [1, 1])
    add("combo_snj_333_shared", [S, N, J], [1, 1, 1])
    add("combo_nse_333_shared", [N, S, E], [1, 1, 1])
    add("combo_jse_333_shared", [J, S, E], [1, 1, 1])
    add("combo_fse_333_shared", [F, S, E], [1, 1, 1])
    add("combo_e1s_7030_shared", [E1, S], [70, 30])
    add("combo_ers_7030_shared", [ER, S], [70, 30])
    add("combo_eh_7030_shared", [E, H], [70, 30])
    add("combo_fh_7030_shared", [F, H], [70, 30])
    add("combo_fe_5050_shared", [F, E], [1, 1])
    add("combo_fes_403030_shared", [F, E, S], [40, 30, 30])
    for a, b in combinations(elite, 2):
        add(f"combo_{abbr[a]}{abbr[b]}_5050_shared", [a, b], [1, 1])
    for a, b in ((S, E), (S, H), (E, H), (S, N), (S, J), (S, F),
                 (E, N), (E, F), (H, N), (E, E1)):
        tag = f"{abbr[a]}{abbr[b]}"
        add(f"combo_{tag}_7030_shared", [a, b], [70, 30])
        add(f"combo_{tag}_3070_shared", [a, b], [30, 70])
    return specs


def blind_recipes(*, universe="auto", hold="auto", gate="auto",
                  rank="auto", side="auto", top_n="auto", exit="auto",
                  entry="auto", size="auto", sell="auto", s_boost="auto",
                  auto_tweak=True) -> list[dict]:
    """Standing auto + auto-tweak sweep over the frozen 9/9 menu only."""
    recs = fmb.recipes_from_action(
        universe=universe, hold=hold, gate=gate, rank=rank, side=side,
        top_n=top_n, exit=exit, entry=entry, size=size, sell=sell,
        s_boost=s_boost, auto_tweak=auto_tweak,
        base=build_recipes_asof_0909(),
    )
    banned = set(POST_0909_NAMES)
    out = []
    for r in recs:
        name = r.get("name") or ""
        if name in banned:
            continue
        if name.startswith("union_clk_") or name.startswith("short_clk_"):
            continue
        if "oppset" in name or name.startswith("oppset_"):
            continue
        if (r.get("s_boost") or "none") == "holdup":
            continue
        if (r.get("universe") or "") in ("overnight_mega", "overnight"):
            continue
        out.append(r)
    return out


def _ntr(s: dict) -> int:
    return int(fm._finite_stat(s, "book_n_trades", "n_trades", "n_graded"))


def _book(s: dict) -> float:
    return float(fm._finite_stat(s, "total_ret_pct"))


def _start_rate(s: dict) -> float:
    n = int(s.get("start_n") or 0)
    g = int(s.get("start_green") or 0)
    if n <= 0:
        return float(fm._finite_stat(s, "start_rate"))
    return g / n


def is_cyrus_featured(s: dict) -> bool:
    """High Starts YES + Book% > 0 + enough trades. IS only."""
    if s.get("audit_ok") is False:
        return False
    if _ntr(s) < CYRUS_MIN_TRADES:
        return False
    if _book(s) <= CYRUS_MIN_BOOK:
        return False
    n = int(s.get("start_n") or 0)
    g = int(s.get("start_green") or 0)
    if n >= CYRUS_START_N_REF:
        return g >= CYRUS_START_YES
    return _start_rate(s) >= CYRUS_START_RATE


def is_form_member(s: dict) -> bool:
    """Loose IS bar for building extra combos (not featuring)."""
    if s.get("audit_ok") is False:
        return False
    if str(s.get("name") or "").startswith("combo_"):
        return False
    if _ntr(s) < FORM_MIN_TRADES:
        return False
    if _book(s) <= 0:
        return False
    return _start_rate(s) >= FORM_MIN_START


def recipe_signature(rec: dict | None) -> tuple:
    rec = rec or {}
    req = tuple(sorted((rec.get("require") or {}).items(), key=str))
    forb = tuple(sorted((rec.get("forbid") or {}).items(), key=str))
    return (
        rec.get("universe"),
        int(rec.get("hold") or 0),
        int(rec.get("top_n") or 0),
        rec.get("rank"),
        rec.get("side") or "long",
        req,
        forb,
        rec.get("s_boost") or "none",
    )


HOT4_SIG = (
    "union", 1, 4, "hot_score", "long",
    (), (("alarm", True),), "none",
)
HOLDUP_SIG = (
    "union", 1, 4, "hot_score", "long",
    (), (("alarm", True),), "holdup",
)


def twins_of(payload: dict, target: tuple) -> list[str]:
    hits = []
    for rec in payload.get("recipes") or []:
        if recipe_signature(rec) == target:
            hits.append(rec["name"])
    return hits


def _abbr(name: str) -> str:
    bits = [b for b in str(name).replace("union_", "").replace("short_", "s_").split("_")
            if b]
    return "".join(b[:2] for b in bits[:4])[:10] or "x"


def form_combos_from_is(stats: list[dict], *,
                        existing: list[dict] | None = None,
                        max_extra: int = FORM_MAX_EXTRA) -> list[dict]:
    """Invent extra mixes from IS singles using combo-engine primitives."""
    existing = list(existing or [])
    seen_names = {s["name"] for s in existing}
    seen_members = {tuple(s.get("members") or []) for s in existing}
    cands = [s for s in stats if is_form_member(s)]
    longs = sorted(
        [s for s in cands if (s.get("side") or "long") != "short"],
        key=lambda s: (-_start_rate(s), -_book(s), s["name"]),
    )[:4]
    shorts = sorted(
        [s for s in cands if s.get("side") == "short"],
        key=lambda s: (-_start_rate(s), -_book(s), s["name"]),
    )[:2]
    extras = []

    def add(name, members, weights, *, net="priority", pool="shared"):
        if len(extras) >= max_extra:
            return
        if name in seen_names:
            return
        key = tuple(members)
        if key in seen_members and weights == [1, 1]:
            return
        seen_names.add(name)
        seen_members.add(key)
        extras.append({
            "name": name,
            "members": list(members),
            "weights": [float(w) for w in weights],
            "net": net,
            "pool": pool,
            "formed": True,
        })

    for lg in longs:
        for sh in shorts:
            tag = f"{_abbr(lg['name'])}{_abbr(sh['name'])}"
            add(f"combo_form_{tag}_5050_shared",
                [lg["name"], sh["name"]], [1, 1])
    if longs and shorts:
        lg, sh = longs[0], shorts[0]
        tag = f"{_abbr(lg['name'])}{_abbr(sh['name'])}"
        add(f"combo_form_{tag}_7030_shared",
            [lg["name"], sh["name"]], [70, 30])
        add(f"combo_form_{tag}_3070_shared",
            [lg["name"], sh["name"]], [30, 70])
    if len(longs) >= 2 and shorts:
        a, b, sh = longs[0], longs[1], shorts[0]
        tag = f"{_abbr(a['name'])}{_abbr(b['name'])}{_abbr(sh['name'])}"
        add(f"combo_form_{tag}_333_shared",
            [a["name"], b["name"], sh["name"]], [1, 1, 1])
    if len(longs) >= 2:
        a, b = longs[0], longs[1]
        if (a.get("side") or "long") != "short" and (b.get("side") or "long") != "short":
            tag = f"{_abbr(a['name'])}{_abbr(b['name'])}"
            add(f"combo_form_{tag}_5050_shared",
                [a["name"], b["name"]], [1, 1])
    return extras


def cyrus_names(stats: list[dict]) -> list[str]:
    rows = [s for s in stats if s.get("name") and is_cyrus_featured(s)]
    rows.sort(key=lambda s: (-_start_rate(s), -_book(s), s["name"]))
    return [s["name"] for s in rows]


def formal_keep_names(stats: list[dict]) -> list[str]:
    rows = [s for s in stats if s.get("name") and fm.is_workable_stat(s)]
    rows.sort(key=lambda s: (-_book(s), s["name"]))
    return [s["name"] for s in rows]


def freeze_names(payload: dict) -> list[str]:
    """IS-only freeze set: Cyrus featured ∪ formal KEEP. No ALWAYS/FOCUS."""
    stats = list(payload.get("stats") or [])
    names = []
    seen: set[str] = set()
    for n in cyrus_names(stats) + formal_keep_names(stats):
        if n not in seen:
            names.append(n)
            seen.add(n)
    # hot4 was on the 9/9 menu — report OOS even if it missed featuring
    # so the remine can answer "was it rediscovered / does it still work".
    if any(s.get("name") == "union_hot_n4_h1" for s in stats):
        if "union_hot_n4_h1" not in seen:
            names.append("union_hot_n4_h1")
    return names


def apply_blind_featured(payload: dict) -> dict:
    featured = cyrus_names(payload.get("stats") or [])
    payload["featured"] = featured
    payload["blind"] = {
        "cutoff": CUTOFF,
        "recipe_freeze": RECIPE_FREEZE_SHA,
        "recipe_freeze_note": RECIPE_FREEZE_NOTE,
        "excluded_seeds": list(POST_0909_NAMES) + [
            "Clock-B catalogue (union_clk_* / short_clk_* / oppset)",
            "WORKABLE_ALWAYS",
            "FOCUS / LONG_LED_PIN live featured pins",
        ],
        "cyrus_featured": featured,
        "formal_keep": formal_keep_names(payload.get("stats") or []),
        "cyrus_rule": (
            f"Starts YES ≥{CYRUS_START_YES}/{CYRUS_START_N_REF} "
            f"(or ≥{CYRUS_START_RATE:.0%} when start_n < {CYRUS_START_N_REF}), "
            f"Book% > {CYRUS_MIN_BOOK:g}, n ≥ {CYRUS_MIN_TRADES}. "
            "No ALWAYS / FOCUS pins."
        ),
        "hot4_twins": twins_of(payload, HOT4_SIG),
        "holdup_twins": twins_of(payload, HOLDUP_SIG),
    }
    return payload


def merge_formed_combos(payload: dict, combo_stats: list[dict],
                        combo_books: dict) -> dict:
    fmc.merge_into_payload(payload, combo_stats, combo_books)
    return apply_blind_featured(payload)


def _md_pct(v) -> str:
    return "—" if v is None else f"{100 * float(v):.0f}%"


def _md_pp(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


def render_blind_md(payload: dict, rows: list[dict], *,
                    cutoff: str, oos_start: str, oos_end: str) -> str:
    blind = payload.get("blind") or {}
    n_mined = int(payload.get("n_mined") or payload.get("n_recipes") or 0)
    n_singles = int(payload.get("n_singles") or 0)
    n_combos = int((payload.get("combos") or {}).get("n") or 0)
    n_formed = int(payload.get("n_formed_combos") or 0)
    bar = fm.WORKABLE_BAR
    cyrus = set(blind.get("cyrus_featured") or [])
    formal = set(blind.get("formal_keep") or [])
    hot_twins = blind.get("hot4_twins") or []
    hold_twins = blind.get("holdup_twins") or []
    by = {r["name"]: r for r in rows}

    lines = [
        f"# Factor mine blind formation — as-of {cutoff}",
        "",
        "This is a **blind formation remine**, not a KEEP-selection cut "
        "of the already-known live menu (that was [PR #285](https://github.com/SRoyaltyy/fullscan/pull/285)).",
        "",
        f"In-sample formation: **{payload.get('from_date')} → {cutoff}** "
        f"({payload.get('n_sessions')} sessions, {payload.get('n_rows')} rows). "
        f"Out-of-sample (frozen discoveries only): **{oos_start} → {oos_end}**.",
        "",
        "## What was frozen",
        "",
        f"- **Panel cutoff:** `{FROM_DATE}` → `{cutoff}` only. No 9/10–{oos_end[-5:] if oos_end else 'end'} row entered ranking, tweaking, or featuring.",
        f"- **Recipe-definition freeze:** yes. {RECIPE_FREEZE_NOTE}",
        "- **Excluded from the seed set:** Clock-B catalogue pins, "
        "`union_hot_n4_holdup`, overnight_mega / overnight_h1, "
        "`combo_oh_5050_shared`, `combo_sh_macd_5050_shared`, "
        "`short_news_r_macd_h3`, WORKABLE_ALWAYS extras, FOCUS / "
        "LONG_LED_PIN live featured pins.",
        "- **Kept:** generic 9/9 auto-grid primitives (universe / hold / "
        "gate / rank / side / top_n / exit / size / sell / S-boost as of 9/9). "
        "`union_hot_n4_h1` is in that 9/9 menu (added 2026-09-05) — it is "
        "not injected as a live FOCUS seed.",
        "- **Holdup primitive:** `s_boost=holdup` did not exist on 9/9 "
        "(landed 2026-09-19). It was not swept and no holdup twin was invented.",
        "",
        "## Method (same as the current board, knowledge-capped)",
        "",
        "1. Leak-free 09:30 panel / $10k cash books (current engine).",
        "2. Default **auto** slice + **auto-tweak** neighbor sweep on the "
        f"frozen 9/9 recipe menu (**{n_singles or '—'}** singles).",
        "3. Combo construction: 9/9 combo-engine specs "
        f"(**{n_combos - n_formed if n_combos else '—'}** mixes) plus extra "
        f"shared 50/50 / 70/30 / 333 mixes **formed from IS singles** "
        f"(**{n_formed}** extras). Total scored: **{n_mined}**.",
        "4. Rank / KEEP from IS only. Cyrus would-have-featured: "
        f"{blind.get('cyrus_rule') or 'Starts YES + Book% > 0 + n ≥ 30'} "
        "Formal WORKABLE_BAR is reported beside it and does **not** pin "
        "FOCUS / WORKABLE_ALWAYS / hot4-holdup.",
        "5. Frozen discoveries replayed OOS continued + fresh $10k. "
        "OOS never added a name.",
        "",
        "Live `dashboard/factor-mine/` and `03_scoreboard/factor_mine.json` "
        "were not written.",
        "",
        f"Formal WORKABLE_BAR (reported, not a live-board pin): "
        f"min_trades {bar['min_trades']}, min_win {bar['min_win']}, "
        f"min_book_pct {bar['min_book_pct']}, min_start {bar['min_start']}, "
        f"min_dollar_days {bar['min_dollar_days']}.",
        "",
        "## Blindly formed keepers (IS) + OOS book",
        "",
        "| Strategy | Side | Cyrus | Formal bar | "
        "IS start | IS book% | IS win% | IS n | "
        "OOS cont book% | Fresh $10k | Members / note |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    rec_by = {r.get("name"): r for r in (payload.get("recipes") or []) if r.get("name")}
    formed_notes = []
    for r in rows:
        name = r["name"]
        rec = rec_by.get(name) or {}
        members = list(rec.get("members") or [])
        note = ""
        if str(name).startswith("combo_form_"):
            note = "formed · " + " + ".join(f"`{m}`" for m in members)
            formed_notes.append((name, members))
        elif members:
            note = " + ".join(f"`{m}`" for m in members)
        elif name == "union_hot_n4_h1":
            note = "9/9 grid point (union / h1 / n4 / hot_score); not featured"
        elif name == "short_news_r_h3":
            note = "9/9 short grid (news🔴 hold 3)"
        cys = "YES" if name in cyrus else "no"
        frm = "YES" if name in formal else "no"
        start = "—"
        if r.get("is_start_n"):
            start = f"{r.get('is_start_green') or 0}/{r.get('is_start_n')}"
        lines.append(
            f"| `{name}` | {r.get('side') or '—'} | {cys} | {frm} | "
            f"{start} | {_md_pp(r.get('is_book_pct'))} | "
            f"{_md_pct(r.get('is_win_rate'))} | {r.get('is_n_trades') or 0} | "
            f"{_md_pp(r.get('oos_book_pct_continued'))} | "
            f"{_md_pp(r.get('fresh_book_pct'))} | {note} |"
        )

    if formed_notes:
        lines += [
            "",
            "Formed extras (IS singles mixed with combo-engine primitives; "
            "not live FOCUS pins):",
            "",
        ]
        for name, members in formed_notes:
            lines.append(
                f"- `{name}` = " + " + ".join(f"`{m}`" for m in members)
            )

    hot = by.get("union_hot_n4_h1") or {}
    hold = by.get("union_hot_n4_holdup")
    cyrus_surv = [
        r for r in rows
        if r["name"] in cyrus
        and r.get("oos_book_pct_continued") is not None
        and float(r["oos_book_pct_continued"]) > 0
    ]
    cyrus_fade = [
        r for r in rows
        if r["name"] in cyrus
        and (r.get("oos_book_pct_continued") is None
             or float(r["oos_book_pct_continued"]) <= 0)
    ]

    def _names(xs):
        return ", ".join(f"`{r['name']}`" for r in xs) or "none"

    hot_cyrus = "union_hot_n4_h1" in cyrus
    hot_formal = "union_hot_n4_h1" in formal
    if hot_cyrus:
        hot_ans = (
            "Yes — a 9/9 researcher running this method would have **featured** "
            f"`union_hot_n4_h1` (Cyrus Starts YES {hot.get('is_start_green') or 0}/"
            f"{hot.get('is_start_n') or 0}, IS book {_md_pp(hot.get('is_book_pct'))}%). "
            "It is in the 9/9 auto grid (union / hold=1 / top_n=4 / hot_score / no 🚨), "
            "not a live FOCUS pin."
        )
    elif hot and hot_formal:
        hot_ans = (
            "Partly — `union_hot_n4_h1` was in the 9/9 menu and passes the "
            "formal WORKABLE_BAR, but it **misses Cyrus featuring** "
            f"(Starts {hot.get('is_start_green') or 0}/{hot.get('is_start_n') or 0}, "
            f"win {_md_pct(hot.get('is_win_rate'))}). A 9/9 researcher using "
            "today's Cyrus bar would not have put it on the featured strip."
        )
    elif hot:
        hot_ans = (
            "No — `union_hot_n4_h1` was scored because it is a 9/9 grid point "
            f"(union / h1 / n4 / hot_score), but it failed featuring "
            f"(Starts {hot.get('is_start_green') or 0}/{hot.get('is_start_n') or 0}, "
            f"win {_md_pct(hot.get('is_win_rate'))}, book "
            f"{_md_pp(hot.get('is_book_pct'))}%). Twins: "
            + (", ".join(f"`{n}`" for n in hot_twins) or "none") + "."
        )
    else:
        hot_ans = (
            "No — `union_hot_n4_h1` was not in the scored payload."
        )

    if hold_twins:
        hold_ans = (
            "A holdup twin appeared independently: "
            + ", ".join(f"`{n}`" for n in hold_twins) + "."
        )
    else:
        hold_ans = (
            "No — `union_hot_n4_holdup` was **not** in the 9/9 recipe menu "
            "(added 2026-09-19 with the holdup s_boost primitive). The remine "
            "did not seed it and did not invent a holdup twin. A 9/9 researcher "
            "using this method would not have found holdup."
        )

    oos_hot = ""
    if hot:
        oos_hot = (
            f" Hot4 OOS continued {_md_pp(hot.get('oos_book_pct_continued'))}% · "
            f"fresh $10k {_md_pp(hot.get('fresh_book_pct'))}%."
        )

    lines += [
        "",
        "## Verdict",
        "",
        "### Formation vs #285 selection-cut",
        "",
        "- **#285** scored today's live recipe menu (including Clock-B, "
        "holdup, overnight_mega) and applied WORKABLE_BAR + ALWAYS / FOCUS "
        "pins. That is a selection cut among already-known names.",
        "- **This remine** freezes recipe *definitions* to 2026-09-09, "
        "strips post-9/9 catalogue pins from the seed set, forms extra "
        "combos from IS winners, and features with Cyrus Starts YES + "
        "Book%. Live FOCUS / ALWAYS never enter the keep set.",
        "",
        "### Would a 9/9 researcher have found hot4 / holdup?",
        "",
        f"- **hot4 / `union_hot_n4_h1`:** {hot_ans}{oos_hot}",
        f"- **holdup / `union_hot_n4_holdup`:** {hold_ans}",
        "",
        f"Cyrus featured that still print a positive continued OOS book: "
        f"{_names(cyrus_surv)}.",
        "",
        f"Cyrus featured that fade after the cut: {_names(cyrus_fade)}.",
        "",
        f"Formal-bar KEEP count: **{len(formal)}**. "
        f"Cyrus featured count: **{len(cyrus)}**.",
        "",
        "OOS **continued** walks the 9/9 cash book forward. "
        f"OOS **fresh $10k** wakes the frozen recipe on {oos_start} "
        "with empty lots.",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_blind_report(payload: dict, full_panel: dict, *,
                       cutoff: str = CUTOFF,
                       oos_start: str = OOS_START,
                       out_root: Path | None = None,
                       asof_md: Path | None = None,
                       bars=None, fees=None, regime=None) -> dict:
    full_panel = fm.rehydrate_panel(full_panel)
    cal = list(full_panel.get("session_dates") or [])
    oos_end = cal[-1] if cal else oos_start
    names = freeze_names(payload)
    rows = fma.score_holdout(
        payload, full_panel, cutoff=cutoff, oos_start=oos_start,
        names=names, bars=bars, fees=fees, regime=regime)
    md = render_blind_md(
        payload, rows, cutoff=cutoff, oos_start=oos_start, oos_end=oos_end)
    blob = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "kind": "blind_formation_0909",
        "cutoff": cutoff,
        "oos_start": oos_start,
        "oos_end": oos_end,
        "bar": dict(fm.WORKABLE_BAR),
        "always": [],
        "cyrus_featured": (payload.get("blind") or {}).get("cyrus_featured") or [],
        "formal_keep": (payload.get("blind") or {}).get("formal_keep") or [],
        "n_report": len(rows),
        "rows": rows,
        "recipe_freeze": RECIPE_FREEZE_SHA,
    }
    if out_root:
        out_root = Path(out_root)
        out_root.mkdir(parents=True, exist_ok=True)
        (out_root / "holdout.json").write_text(
            json.dumps(blob, indent=2), encoding="utf-8")
        (out_root / "FACTOR_MINE_BLIND.md").write_text(md, encoding="utf-8")
    dest_md = Path(asof_md) if asof_md else None
    if dest_md:
        dest_md.parent.mkdir(parents=True, exist_ok=True)
        dest_md.write_text(md, encoding="utf-8")
        print(f"[blind] wrote {dest_md}", flush=True)
    return blob


def run_blind(*, from_date: str = FROM_DATE, to_date: str = CUTOFF,
              write: bool = False, auto_tweak: bool = True,
              paths: dict | None = None, panel: dict | None = None,
              full_panel: dict | None = None,
              holdout: bool = False, asof_md: Path | None = None,
              bars=None, universe="auto", hold="auto", gate="auto",
              rank="auto", side="auto", top_n="auto", exit="auto",
              entry="auto", size="auto", sell="auto", s_boost="auto",
              rebuild_panel: bool = False) -> dict:
    recipes = blind_recipes(
        universe=universe, hold=hold, gate=gate, rank=rank, side=side,
        top_n=top_n, exit=exit, entry=entry, size=size, sell=sell,
        s_boost=s_boost, auto_tweak=auto_tweak)
    print(f"[blind] frozen 9/9 singles={len(recipes)} "
          f"(Clock-B / holdup / overnight_mega / ALWAYS stripped)",
          flush=True)
    persist = bool(write) and bool((paths or {}).get("persist_panel"))
    payload = fm.run(
        from_date, to_date, write=False, recipes=recipes, panel=panel,
        rebuild_panel=rebuild_panel, persist_panel=persist, book=True,
        bars=bars, combos=False, paths=paths)
    payload["n_singles"] = len(recipes)
    frozen_specs = combo_specs_asof_0909()
    formed = form_combos_from_is(
        payload.get("stats") or [], existing=frozen_specs)
    specs = frozen_specs + formed
    print(f"[blind] combo specs frozen={len(frozen_specs)} "
          f"formed={len(formed)}", flush=True)
    fees = fm.pt_fees()
    regime = fmb.load_regime()
    use_panel = panel if panel is not None else fm.rehydrate_panel(
        fm.load_or_build_panel(from_date, to_date))
    combo_stats, combo_books = fmc.run_combos(
        use_panel, recipes, bars=bars, fees=fees, regime=regime,
        member_stat_by={s["name"]: s for s in (payload.get("stats") or [])},
        specs=specs)
    if write:
        fmc.write_combo_sidecar(
            combo_stats, dest=(paths or {}).get("combo"))
    payload = merge_formed_combos(payload, combo_stats, combo_books)
    payload["n_mined"] = len(payload.get("stats") or [])
    payload["n_formed_combos"] = len(formed)
    combo_meta = dict(payload.get("combos") or {})
    combo_meta["frozen_n"] = len(frozen_specs)
    combo_meta["formed_n"] = len(formed)
    combo_meta["formed"] = [s["name"] for s in formed]
    payload["combos"] = combo_meta
    payload = apply_blind_featured(payload)
    keep = set(freeze_names(payload))
    if write:
        fm.write_outputs(
            payload, payload.get("stats"),
            books=combo_books if (paths or {}).get("write_actions") else None,
            paths=paths, always=(), keep_names=keep, pin_long_led=False)
    if holdout:
        write_blind_report(
            payload,
            full_panel or fm.load_or_build_panel(from_date, None),
            cutoff=to_date or CUTOFF, oos_start=OOS_START,
            out_root=(paths or {}).get("json") and Path(paths["json"]).parent,
            asof_md=asof_md, bars=bars, fees=fees, regime=regime)
    return payload


def run_cli(args, paths: dict) -> int:
    from_date = args.from_date or FROM_DATE
    to_date = args.to_date or CUTOFF
    full_panel = fm.load_or_build_panel(from_date, None,
                                       rebuild=args.rebuild_panel)
    full_panel = fm.rehydrate_panel(full_panel)
    fm.attach_tape_flow(full_panel)
    panel = fm.slice_panel(full_panel, from_date, to_date)
    cal = list(panel.get("session_dates") or [])
    if from_date and from_date not in cal:
        raise SystemExit(
            f"panel missing --from-date {from_date}; "
            f"have {cal[:3]}…{cal[-3:] if len(cal) >= 3 else cal}")
    if to_date and to_date not in cal:
        raise SystemExit(
            f"panel missing --to-date {to_date}; "
            f"last session is {cal[-1] if cal else 'none'}")
    print(f"[blind] out-root {paths['json'].parent} "
          f"slice {panel.get('from_date')}→{panel.get('to_date')} "
          f"rows={panel.get('n_rows')} (live panel untouched)",
          flush=True)
    asof_md = Path(args.asof_md) if getattr(args, "asof_md", "") else None
    if asof_md and not asof_md.is_absolute():
        asof_md = fm.ROOT / asof_md
    if asof_md is None and args.write:
        asof_md = fm.ROOT / DEFAULT_MD
    payload = run_blind(
        from_date=from_date, to_date=to_date, write=args.write,
        auto_tweak=args.auto_tweak, paths=paths if args.write else None,
        panel=panel, full_panel=full_panel,
        holdout=bool(getattr(args, "holdout", False)),
        asof_md=asof_md,
        universe=args.universe, hold=args.hold, gate=args.gate,
        rank=args.rank, side=args.side, top_n=args.top_n, exit=args.exit,
        entry=args.entry, size=args.size, sell=args.sell,
        s_boost=args.s_boost, rebuild_panel=False)
    print(f"[blind] recipes={payload.get('n_recipes')} "
          f"singles={payload.get('n_singles')} "
          f"formed={payload.get('n_formed_combos')} "
          f"cyrus={len((payload.get('blind') or {}).get('cyrus_featured') or [])} "
          f"to={payload.get('to_date')}")
    for s in (payload.get("stats") or [])[:8]:
        print(f"  {s['name']:32s}  win={fm._pct(s.get('win_rate'))}  "
              f"starts={s.get('start_green')}/{s.get('start_n')}  "
              f"tot={fm._n(s.get('total_ret_pct'))}%")
    return 0


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default=FROM_DATE)
    ap.add_argument("--to-date", default=CUTOFF)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--holdout", action="store_true")
    ap.add_argument("--out-root", default=DEFAULT_OUT_ROOT)
    ap.add_argument("--dash-dir", default=DEFAULT_DASH)
    ap.add_argument("--asof-md", default=DEFAULT_MD)
    ap.add_argument("--auto-tweak", dest="auto_tweak", action="store_true",
                    default=True)
    ap.add_argument("--no-auto-tweak", dest="auto_tweak", action="store_false")
    ap.add_argument("--rebuild-panel", action="store_true")
    ap.add_argument("--universe", default="auto")
    ap.add_argument("--hold", default="auto")
    ap.add_argument("--gate", default="auto")
    ap.add_argument("--rank", default="auto")
    ap.add_argument("--side", default="auto")
    ap.add_argument("--top-n", default="auto")
    ap.add_argument("--exit", default="auto")
    ap.add_argument("--entry", default="auto")
    ap.add_argument("--size", default="auto")
    ap.add_argument("--sell", default="auto")
    ap.add_argument("--s-boost", dest="s_boost", default="auto")
    args = ap.parse_args(argv)
    paths = fm.publish_paths(args.out_root, args.dash_dir)
    return run_cli(args, paths)


if __name__ == "__main__":
    raise SystemExit(main())
