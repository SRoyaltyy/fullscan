"""Factor-mine long fee KEEP — cash leaders + leftover Clock-B longs.

Research only. Live flatten_robust / cash book are not imported or written.

Scores name-day after-fee H (not cash Book%) for:

  A  top cash leaders: union_e_fresh_h3, combo_se_5050_skip,
     combo_ej_5050_shared
  B  remaining Clock-B longs with leftover Book% not in #281:
     union_clk_mom_break_peer_h1, union_clk_hold_vs_sector_h1,
     union_clk_nr7_mom_opp_h1 (thin-n if prove n cannot reach 30)

Recipe atoms are 09:30-knowable (join_morning + finviz_asof T−1 /
panel prior tape). Same-day Gap / minute Performance are never features.
Cash-leader gates are restated here so the miner module is not loaded.

Scoring is the CATALOGUE_COMBO_KEEP / FACTOR_CLK_LONG_KEEP /
excel_factor_mine fee path: time-split holdout, after-fee H, ≥30 prove
fires, WR strictly > 55%. Futubull FEE_RT=0.0015. Lift-only is never
KEEP. Thin n is thin-n (not KEEP).

Aisle = restored multi-src morning panel ∪ Theme Radar Clock-B
oppset. Oppset aisle membership is required only where the recipe
requires opp. combo_se_5050_skip is mixed (short+long, skip fights);
a mix KEEP is not a long KEEP for goal (b).

  python3 excel_bot/engine/factor_long_fee_keep.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
sys.path.insert(0, HERE)
sys.path.insert(0, REPO)

from excel_clock_gate import gate_payload  # noqa: E402
from excel_factor_mine import (  # noqa: E402
    cutoff_from_dates,
    keep_verdict,
    score_hits,
    split_rows,
    walk_folds,
)
from join_post_813 import FEE_RT  # noqa: E402
from j_winrate import FEE_CAVEAT, MIN_FIRES, WIN_BAR  # noqa: E402
from catalogue_combo_keep import (  # noqa: E402
    FORBIDDEN_FEATURE_FIELDS,
    HOLD_FRAC,
    OPPSET_PATH,
    OPPSET_SOURCE,
    PANEL_PATH,
    SCOREBOARD,
    aisle_rows,
    leak_check,
    list_finviz_dates,
    load_finviz_labels,
    load_oppset_flagged,
    load_panel,
    prior_finviz_date,
)
from src import clock_b_tells as cbt  # noqa: E402

BOARD_MD = os.path.join(SCOREBOARD, "FACTOR_LONG_FEE_KEEP.md")
BOARD_JSON = os.path.join(SCOREBOARD, "factor_long_fee_keep.json")
REMINE = "35458265920"
PRIOR_PR = 281

# Feature keys recipe evaluators may read. H/I/Gap/Change/RelVol
# and minute Performance stay labels / leaks, never flags.
FEATURE_KEYS = frozenset({
    "ohlc_ret_1", "ohlc_ret_5", "ohlc_ret_10", "ohlc_rvol",
    "ohlc_hot_score", "ohlc_nr7", "ohlc_break_10",
    "last_green", "last_red", "candle_capture",
    "macd_up", "macd_down", "rsi_ob", "rsi_os", "rsi",
    "flow_in", "alarm", "boxes", "rs_week",
    "news_box", "news_prior", "erd_earn_react",
    "erd_flag_E", "erd_days_since_E", "erd_flag_R", "erd_days_since_R",
    "e_pol", "fv_inst", "ins_buy", "form4_buy",
    "oppset", "sources",
})

# Claim order when two combo members want the same name (factor_mine_combo).
CLAIM_RANK = {
    "union_e_fresh_h3": 0,
    "union_join_vol_green_h1": 4,
    "short_news_r_h3": 10,
}

# Cash Book% on FACTOR_MINE_ACTION — context only. Not a KEEP input.
RECIPES = (
    {
        "id": 1,
        "group": "A",
        "name": "union_e_fresh_h3",
        "kind": "cash",
        "clk": None,
        "need_opp": False,
        "side": "long",
        "hold": 3,
        "members": (),
        "net": None,
        "title": "union ∩ e_fresh, no 🚨",
        "thesis": "knowable earnings printed within 1 session",
        "have": "erd_days_since_E ≤ 1 and erd_flag_E ≥ 0; forbid alarm",
        "cash_book_pct": 38.46,
        "cash_starts": "25/26",
        "note": "Cash Book% +38.46% on FACTOR_MINE_ACTION is not KEEP. "
                "Hold 3 is a cash-book timer; name-day H is same-session.",
    },
    {
        "id": 2,
        "group": "A",
        "name": "combo_se_5050_skip",
        "kind": "combo",
        "clk": None,
        "need_opp": False,
        "side": "mix",
        "hold": None,
        "members": ("short_news_r_h3", "union_e_fresh_h3"),
        "net": "skip",
        "title": "50/50 short_news_r + e_fresh, skip opposite-side fights",
        "thesis": "long-led cash mix; name-day prove scores both sides",
        "have": "short: boxes.news=bad; long: e_fresh (days_since_E≤1, "
                "flag_E≥0, no alarm). Skip when both claim one ticker.",
        "cash_book_pct": 38.42,
        "cash_starts": "26/26",
        "note": "Mixed recipe. A mix KEEP is not a long KEEP for goal (b). "
                "Shorts pay FEE_RT (they do not collect it).",
    },
    {
        "id": 3,
        "group": "A",
        "name": "combo_ej_5050_shared",
        "kind": "combo",
        "clk": None,
        "need_opp": False,
        "side": "long",
        "hold": None,
        "members": ("union_e_fresh_h3", "union_join_vol_green_h1"),
        "net": "priority",
        "title": "50/50 e_fresh + join_vol_green, shared pile",
        "thesis": "two long rifles, one name-day lot via claim order",
        "have": "e_fresh as #1; join🟢 + vol🟢 + last_green, forbid alarm "
                "and news🔴. Same-side ties go to e_fresh.",
        "cash_book_pct": 36.17,
        "cash_starts": "26/26",
        "note": "Both members are longs. Cash Book% +36.17% is not KEEP.",
    },
    {
        "id": 4,
        "group": "B",
        "name": "union_clk_mom_break_peer_h1",
        "kind": "clock_b",
        "clk": "clk_mom_break_peer",
        "need_opp": False,
        "side": "long",
        "hold": 1,
        "members": (),
        "net": None,
        "title": "Clock-B #1 mom+breakout+peer/sector (no opp filter)",
        "thesis": "continuation without Theme Radar membership",
        "have": "panel ohlc_ret_5 / last_green / macd_up / ohlc_break_10 / "
                "candle_capture / boxes.peer|sector / rs_week",
        "cash_book_pct": 3.05,
        "cash_starts": "26/26",
        "note": "#281 proved the opp splice (FAIL 50.0% n=54). This is the "
                "plain union recipe. Cash Book% +3.05% is not KEEP.",
    },
    {
        "id": 5,
        "group": "B",
        "name": "union_clk_hold_vs_sector_h1",
        "kind": "clock_b",
        "clk": "clk_hold_vs_sector",
        "need_opp": False,
        "side": "long",
        "hold": 1,
        "members": (),
        "net": None,
        "title": "Clock-B #6 stock holds while sector camera is red",
        "thesis": "stock-specific resilience, no opp filter",
        "have": "boxes.sector bad + ohlc_ret_1 / last_green",
        "cash_book_pct": 4.71,
        "cash_starts": "18/26",
        "note": "#281 proved the opp splice (FAIL 49.0% n=102). This is the "
                "plain union recipe. Cash Book% +4.71% is not KEEP.",
    },
    {
        "id": 6,
        "group": "B",
        "name": "union_clk_nr7_mom_opp_h1",
        "kind": "clock_b",
        "clk": "clk_nr7_mom",
        "need_opp": True,
        "side": "long",
        "hold": 1,
        "members": (),
        "net": None,
        "title": "Clock-B #10 NR7 ∩ Theme Radar T−1 oppset",
        "thesis": "coil then continuation on flagged oppset names",
        "have": "panel ohlc_nr7 / ohlc_ret_5 / last_green / macd_up; Theme "
                "Radar T−1 oppset membership",
        "cash_book_pct": -0.74,
        "cash_starts": "1/26",
        "note": "#281 union_clk_nr7_mom_h1 (no opp) was thin-n n=15 WR 60%. "
                "Opp splice is thinner. Mark thin-n if prove n < 30. Cash "
                "Book% −0.74% (1/26 starts) is not KEEP.",
    },
)


def _tick(v):
    return str(v or "").strip().upper()


def _finite(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float("inf"), float("-inf")):
        return None
    return v


def _tone(boxes, key):
    return str((boxes or {}).get(key) or "missing").strip().lower()


def _int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def on_oppset(row):
    """Theme Radar T−1 flagged membership. Stamp or aisle source."""
    if row.get("oppset"):
        return True
    srcs = {str(s).strip().lower() for s in (row.get("sources") or [])}
    return bool(srcs & {"oppset", "oppset_clock_b"})


def feature_row(row):
    """Open-knowable inputs only. Drop labels and same-day tape leaks."""
    out = {}
    for k, v in (row or {}).items():
        if k in FORBIDDEN_FEATURE_FIELDS or k in cbt.LEAK_FIELDS:
            continue
        if k in ("H", "I", "net", "open", "close", "high", "low"):
            continue
        if k.startswith("clk_") or k in ("_clock_b", "_tape_filled"):
            continue
        out[k] = v
    leak = set(out) & FORBIDDEN_FEATURE_FIELDS
    if leak:
        raise ValueError(f"LEAK abort: feature row stored {sorted(leak)}")
    leak2 = set(out) & cbt.LEAK_FIELDS
    if leak2:
        raise ValueError(f"LEAK abort: feature row stored Clock-B leaks {sorted(leak2)}")
    return out


def assert_atoms_legal():
    leak = FEATURE_KEYS & FORBIDDEN_FEATURE_FIELDS
    if leak:
        raise ValueError(f"LEAK abort: feature keys include {sorted(leak)}")
    leak2 = FEATURE_KEYS & cbt.LEAK_FIELDS
    if leak2:
        raise ValueError(f"LEAK abort: feature keys include Clock-B leaks {sorted(leak2)}")
    return True


def recipe_defs_from_wire():
    """Documented require/forbid. Clock-B from #279; cash gates restated."""
    veto = {"clk_ext_veto": True, "alarm": True}

    def rec(name, **kw):
        return {
            "name": name,
            "universe": "union",
            "hold": kw.get("hold"),
            "side": kw.get("side") or "long",
            "require": dict(kw.get("require") or {}),
            "forbid": dict(kw.get("forbid") or {}),
            "members": list(kw.get("members") or []),
            "net": kw.get("net"),
        }

    return {
        "union_e_fresh_h3": rec(
            "union_e_fresh_h3", hold=3, side="long",
            require={"days_since_E_max": 1, "flag_E_min": 0},
            forbid={"alarm": True},
        ),
        "combo_se_5050_skip": rec(
            "combo_se_5050_skip", side="mix",
            members=["short_news_r_h3", "union_e_fresh_h3"],
            net="skip",
        ),
        "combo_ej_5050_shared": rec(
            "combo_ej_5050_shared", side="long",
            members=["union_e_fresh_h3", "union_join_vol_green_h1"],
            net="priority",
        ),
        "union_clk_mom_break_peer_h1": rec(
            "union_clk_mom_break_peer_h1", hold=1, side="long",
            require={"clk_mom_break_peer": True}, forbid=dict(veto),
        ),
        "union_clk_hold_vs_sector_h1": rec(
            "union_clk_hold_vs_sector_h1", hold=1, side="long",
            require={"clk_hold_vs_sector": True}, forbid=dict(veto),
        ),
        "union_clk_nr7_mom_opp_h1": rec(
            "union_clk_nr7_mom_opp_h1", hold=1, side="long",
            require={"clk_nr7_mom": True, "oppset": True}, forbid=dict(veto),
        ),
    }


def fire_e_fresh(row):
    """union_e_fresh_h3: days_since_E≤1, flag_E≥0, no alarm."""
    feats = row.get("feats") or {}
    if feats.get("alarm") or row.get("alarm"):
        return False
    dse = _int(feats.get("erd_days_since_E"))
    fe = _int(feats.get("erd_flag_E"))
    if dse is None or fe is None:
        return False
    return dse <= 1 and fe >= 0


def fire_join_vol_green(row):
    """union_join_vol_green_h1: join🟢 vol🟢 last_green; forbid alarm + news🔴."""
    feats = row.get("feats") or {}
    if feats.get("alarm") or row.get("alarm"):
        return False
    boxes = feats.get("boxes") or {}
    if _tone(boxes, "join") != "good":
        return False
    if _tone(boxes, "vol") != "good":
        return False
    if not feats.get("last_green"):
        return False
    if _tone(boxes, "news") == "bad":
        return False
    return True


def fire_short_news_r(row):
    """short_news_r_h3: news camera red. Shorts have no alarm forbid."""
    feats = row.get("feats") or {}
    boxes = feats.get("boxes") or {}
    return _tone(boxes, "news") == "bad"


def fire_clock_b(row, spec):
    """#279 long gate: Clock-B combo ∧ (oppset if required) ∧ not veto/alarm."""
    feats = row.get("feats") or {}
    if spec.get("need_opp") and not row.get("oppset"):
        return False
    if feats.get("alarm") or row.get("alarm"):
        return False
    if cbt.combo_true(feats, "clk_ext_veto"):
        return False
    return bool(cbt.combo_true(feats, spec["clk"]))


MEMBER_FIRE = {
    "union_e_fresh_h3": ("long", fire_e_fresh),
    "union_join_vol_green_h1": ("long", fire_join_vol_green),
    "short_news_r_h3": ("short", fire_short_news_r),
}


def combo_claims(row, spec):
    """Winning member after skip / claim-order. Empty if no fire."""
    claims = []
    for name in spec.get("members") or ():
        side, fn = MEMBER_FIRE[name]
        if fn(row):
            claims.append((name, side))
    if not claims:
        return []
    sides = {s for _, s in claims}
    if spec.get("net") == "skip" and len(sides) > 1:
        return []
    claims.sort(key=lambda x: CLAIM_RANK.get(x[0], 99))
    return [claims[0]]


def recipe_fires(row, spec):
    """True when the recipe would take this name-day."""
    kind = spec.get("kind")
    if kind == "clock_b":
        return fire_clock_b(row, spec)
    if kind == "cash":
        return fire_e_fresh(row)
    if kind == "combo":
        return bool(combo_claims(row, spec))
    return False


def board_verdict(n, wr):
    """KEEP / FAIL / thin-n. Thin n is never KEEP."""
    v, why = keep_verdict(n, wr)
    if v == "KEEP":
        return "KEEP", why
    if not n:
        return "FAIL", why
    if int(n) < MIN_FIRES:
        return "thin-n", why
    return "FAIL", why


def name_days_from_aisle(panel, export_dir=None, oppset_by_date=None):
    """Open-knowable aisle name-days. H is a label from same-day OHLC."""
    raw, aisle_dates, skipped, stats = aisle_rows(panel, oppset_by_date)
    fv_dates = list_finviz_dates(export_dir)
    label_cache = {}

    def _labels(iso):
        if iso not in label_cache:
            label_cache[iso] = load_finviz_labels(iso, export_dir)
        return label_cache[iso]

    rows = []
    n_label_fv = 0
    for r in raw:
        iso = str(r.get("date") or "")[:10]
        tk = _tick(r.get("ticker"))
        o, c = _finite(r.get("open")), _finite(r.get("close"))
        if (not o or not c or o <= 0) and iso and tk:
            lab = _labels(iso).get(tk) or {}
            o = o or lab.get("open")
            c = c or lab.get("close")
            if lab.get("open") and lab.get("close"):
                n_label_fv += 1
        if not o or not c or o <= 0:
            continue
        stamped = str(
            r.get("finviz_asof") or r.get("news_export_date")
            or r.get("prior_date") or ""
        )[:10]
        if stamped == iso:
            raise ValueError(f"LEAK abort: Finviz date {stamped} is same-row as {iso}")
        t1 = prior_finviz_date(iso, fv_dates, r)
        if t1 == iso:
            raise ValueError(f"LEAK abort: Finviz date {t1} is same-row as {iso}")
        feats = feature_row(r)
        H = (c - o) / o
        rows.append({
            "date": iso,
            "ticker": tk,
            "H": H,
            "net": H - FEE_RT,
            "net_short": -H - FEE_RT,
            "feats": feats,
            "oppset": on_oppset(r),
            "alarm": bool(r.get("alarm")),
            "sources": list(r.get("sources") or []),
        })
    stats["n_label_finviz_t"] = n_label_fv
    stats["n_scored"] = len(rows)
    return rows, aisle_dates, skipped, stats


def _hit_net(row, side):
    if side == "short":
        return row.get("net_short")
    return row.get("net")


def recipe_hits(rows, spec):
    hits = []
    kind = spec.get("kind")
    for r in rows:
        if kind == "combo":
            claims = combo_claims(r, spec)
            if not claims:
                continue
            name, side = claims[0]
            net = _hit_net(r, side)
            if net is None:
                continue
            hits.append({
                "net": net, "date": r.get("date"),
                "ticker": r.get("ticker"), "side": side, "member": name,
            })
            continue
        if r.get("net") is None:
            continue
        if recipe_fires(r, spec):
            hits.append({
                "net": r["net"],
                "date": r.get("date"),
                "ticker": r.get("ticker"),
                "side": "long",
            })
    return hits


def score_recipe(rows, spec):
    hits = recipe_hits(rows, spec)
    scored = score_hits(hits)
    v, why = board_verdict(scored.get("n") or 0, scored.get("wr"))
    scored["verdict"] = v
    scored["why"] = why
    n_long = sum(1 for h in hits if h.get("side") != "short")
    n_short = sum(1 for h in hits if h.get("side") == "short")
    scored["n_long"] = n_long
    scored["n_short"] = n_short
    return scored


def score_space(disc, hold):
    folds = walk_folds([r["date"] for r in disc])
    fold_sets = []
    for lo, hi, chunk in folds:
        s = set(chunk)
        fold_sets.append((lo, hi, [r for r in disc if r["date"] in s]))
    out = []
    for spec in RECIPES:
        d = score_recipe(disc, spec)
        h = score_recipe(hold, spec)
        rec = {
            "id": spec["id"],
            "group": spec["group"],
            "name": spec["name"],
            "kind": spec["kind"],
            "clk": spec["clk"],
            "need_opp": spec["need_opp"],
            "title": spec["title"],
            "thesis": spec["thesis"],
            "have": spec["have"],
            "note": spec["note"],
            "side": spec["side"],
            "min_hold": spec["hold"],
            "members": list(spec.get("members") or ()),
            "net": spec.get("net"),
            "cash_book_pct": spec["cash_book_pct"],
            "cash_starts": spec["cash_starts"],
            "disc": d,
            "hold": h,
            "verdict": h["verdict"],
            "why": h["why"],
            "goal_b_eligible": spec["side"] == "long",
        }
        walk = []
        n_ok = 0
        for lo, hi, frows in fold_sets:
            fs = score_recipe(frows, spec)
            walk.append({
                "lo": lo, "hi": hi, "n": fs["n"], "wr": fs["wr"],
                "verdict": fs["verdict"],
            })
            if fs["n"] and fs["wr"] is not None and fs["wr"] > 0.50:
                n_ok += 1
        rec["walk"] = walk
        rec["walk_folds_wr_gt_50"] = n_ok
        rec["n_walk_folds"] = len(fold_sets)
        out.append(rec)
    return out


def _pct(wr):
    if wr is None:
        return "—"
    return f"{100 * wr:.1f}%"


def _mean_net(rec):
    v = (rec or {}).get("mean_net")
    if v is None:
        return "—"
    return f"{v:+.4f}"


def _book(pct):
    if pct is None:
        return "—"
    return f"{float(pct):+.2f}%"


def long_keeps(scored):
    return [r for r in scored
            if r.get("verdict") == "KEEP" and r.get("side") == "long"]


def headline_from(scored, cutoff, baseline=None):
    keeps = [r for r in scored if r.get("verdict") == "KEEP"]
    long_k = long_keeps(scored)
    thins = [r for r in scored if r.get("verdict") == "thin-n"]
    fails = [r for r in scored if r.get("verdict") == "FAIL"]
    n_long = sum(1 for r in scored if r.get("side") == "long")
    if long_k:
        bits = [f"`{r['name']}` {r['why']}" for r in long_k]
        extra = ""
        mix_k = [r for r in keeps if r.get("side") != "long"]
        if mix_k:
            extra = (
                " Mix KEEP (not goal b): "
                + "; ".join(f"`{r['name']}` {r['why']}" for r in mix_k)
                + "."
            )
        return {
            "verdict": "KEEP",
            "n_keep": len(long_k),
            "n_keep_all": len(keeps),
            "text": (
                f"KEEP vs goal (b). {len(long_k)} of {n_long} longs cleared "
                f"≥{MIN_FIRES} prove fires and >{100 * WIN_BAR:.0f}% "
                f"after-fee H (cutoff {cutoff}): " + "; ".join(bits) + ". "
                "Cash Book% is not KEEP." + extra
            ),
        }
    material = [r for r in scored
                if r.get("side") == "long"
                and (r.get("hold") or {}).get("n", 0) >= MIN_FIRES]
    ranked = material or [
        r for r in scored
        if r.get("side") == "long" and (r.get("hold") or {}).get("n")
    ]
    best = None
    if ranked:
        best = max(ranked, key=lambda r: (
            ((r.get("hold") or {}).get("wr") or 0),
            (r.get("hold") or {}).get("n") or 0,
        ))
    extra = ""
    if best:
        h = best["hold"]
        extra = (
            f" Best near-miss: `{best['name']}` prove n={h.get('n')} "
            f"after-fee WR {_pct(h.get('wr'))}."
        )
    if baseline and baseline.get("n"):
        extra += (
            f" Aisle baseline prove n={baseline['n']} after-fee WR "
            f"{_pct(baseline.get('wr'))}."
        )
    mix_k = [r for r in keeps if r.get("side") != "long"]
    if mix_k:
        extra += (
            " Mix KEEP (not a long / not goal b): "
            + "; ".join(f"`{r['name']}` {r['why']}" for r in mix_k) + "."
        )
    text = (
        f"FAIL vs goal (b). 0 of {n_long} longs cleared the Cyrus fee-KEEP "
        f"bar on holdout (cutoff {cutoff}). {len(fails)} FAIL, "
        f"{len(thins)} thin-n.{extra} Cash Book% is not KEEP. Lift-only "
        f"is not KEEP. Prior #{PRIOR_PR} Clock-B opp longs were 0 KEEP. "
        "Touch-rate already improved +3.3pp on Taskforce (goal a partial)."
    )
    return {
        "verdict": "FAIL",
        "n_keep": 0,
        "n_keep_all": len(keeps),
        "text": text,
        "best": None if not best else {
            "name": best["name"],
            "n": (best.get("hold") or {}).get("n"),
            "wr": (best.get("hold") or {}).get("wr"),
        },
    }


def write_board(payload, path=None):
    path = path or BOARD_MD
    gp = payload["gate"]
    hl = payload["headline"]
    scored = payload["scored"]
    base = payload.get("baseline_hold") or {}
    lines = [
        "# Factor-mine long fee KEEP — cash leaders + leftover Clock-B",
        "",
        f"status={payload.get('status', 'DONE')} verdict=**{hl['verdict']}** "
        f"TIME-SPLIT cutoff={payload.get('cutoff')} "
        f"aisle_days={payload.get('n_aisle_dates')} "
        f"name-days={payload.get('n_rows')} "
        f"KEEP={hl.get('n_keep', 0)} thin-n={payload.get('n_thin', 0)} "
        f"FAIL={payload.get('n_fail', 0)}",
        "",
        "Research only. Live `flatten_robust` is not imported and is not written.",
        "Cash Book% on the remine / FACTOR_MINE_ACTION board is **not** KEEP.",
        f"Prior #{PRIOR_PR} Clock-B opp longs: 0 KEEP.",
        "",
        "## Headline",
        "",
        hl["text"],
        "",
        "## KEEP bar",
        "",
        f"Cyrus KEEP: **≥{MIN_FIRES} prove fires** and **after-fee H win rate "
        f"> {100 * WIN_BAR:.0f}%**. After-fee H = open-to-close minus "
        f"{FEE_RT * 10000:.0f} bp Futubull (`FEE_RT={FEE_RT}`). Shorts pay "
        "the same 15 bp (they do not collect it). A fire is an aisle "
        "name-day (multi-src panel ∪ Clock-B oppset) where the recipe gate "
        "is true at the 09:30 open. **Lift-only is never KEEP.** Thin n "
        "that prints >55% is **thin-n** (not KEEP). Discovery cannot KEEP. "
        "Cash Book% cannot KEEP. Goal (b) needs a **long** KEEP — a mix "
        f"KEEP on `combo_se_5050_skip` does not count. {FEE_CAVEAT}",
        "",
        "## Goal",
        "",
        "- (a) Touch-rate: already improved +3.3pp on Taskforce — partial, "
        "out of scope here.",
        "- (b) Tangible factor-mine selection improvement: ≥1 **long** with "
        f"prove n≥{MIN_FIRES} and after-fee H WR >{100 * WIN_BAR:.0f}%. "
        "A long that clears this bar is the call; 0 long KEEP is FAIL.",
        "",
        "## Clock lock",
        "",
        f"- Gate: `{gp['gate']}`",
        f"- Same-row leak abort: `{', '.join(gp['same_row_leak_abort'])}`",
        f"- Leak check: **{payload['leak']}**",
        "- Features: cash-leader ERD / camera atoms and `#279` "
        "`clock_b_tells` on `join_morning` (T) + `finviz_asof` (T−1) / "
        "panel prior tape. Open is the fill, not a feature. Same-day Gap / "
        "Change / RelVol / minute Performance* are never Clock B and never "
        "flags. H/I are labels only.",
        f"- Split: TIME-SPLIT last {HOLD_FRAC:.0%} of aisle "
        f"session dates (cutoff `{payload.get('cutoff')}`). Discovery "
        "feature date is strictly before cutoff.",
        f"- Aisle: restored multi-src morning panel "
        f"(`lookback={payload.get('lookback')}`) ∪ Theme Radar Clock-B "
        f"flagged oppset (`{payload.get('oppset_source') or OPPSET_SOURCE}`). "
        "Oppset membership is required **only** where the recipe requires "
        "opp (`union_clk_nr7_mom_opp_h1`). Oppset gap/RelVol flags are T−1 "
        "membership only (VOL/CROWD aisle, not direction atoms). "
        "Flatten-only / starved days stay out unless the oppset covers "
        "that morning.",
        "- Live: `flatten_robust` not imported, not written. The miner "
        "module is not loaded (avoids live lookback).",
        f"- Remine input: GitHub Actions `{REMINE}` "
        f"(FULLSCAN_OPPSET_UNION=1, from=2026-08-13, rebuild_panel=true).",
        f"- Prior prove: #{PRIOR_PR} FACTOR_CLK_LONG_KEEP (0 KEEP).",
        "",
        "## Aisle",
        "",
        f"Panel `{payload.get('panel_to')}` n_rows={payload.get('panel_n')} "
        f"lookback=`{payload.get('lookback')}`. "
        f"Oppset `{payload.get('oppset_source') or OPPSET_SOURCE}` "
        f"flagged={payload.get('n_oppset_flagged', '—')}. "
        f"Aisle mix: panel={payload.get('n_aisle_panel', '—')} "
        f"overlap={payload.get('n_aisle_overlap', '—')} "
        f"oppset_only={payload.get('n_aisle_oppset_only', '—')} "
        f"scored={payload.get('n_rows')}. "
        f"Oppset-only labels use same-day Finviz Open→Price "
        f"(n={payload.get('n_label_finviz_t', 0)}); not features. "
        f"Aisle days: {', '.join(payload.get('aisle_dates') or []) or '—'}.",
        "",
    ]
    skipped = payload.get("skipped_days") or {}
    if skipped:
        lines.append("Excluded days (not the restored aisle):")
        lines.append("")
        for iso, rec in skipped.items():
            lines.append(
                f"- `{iso}` n={rec.get('n')} sources={rec.get('sources')} "
                f"— {rec.get('why')}"
            )
        lines.append("")
    lines += [
        f"Holdout baseline (every aisle name-day, long): n={base.get('n', 0)} "
        f"after-fee WR {_pct(base.get('wr'))} mean_net="
        f"{_mean_net(base)}.",
        "",
        "## Cash Book% (not KEEP)",
        "",
        "These printed on FACTOR_MINE_ACTION / remine cash books. They are "
        "leftover top-8 books with hard-red sit — a different metric. They "
        "cannot KEEP a name-day fee prove.",
        "",
        "| # | recipe | remine Book% | starts | side | fee-KEEP |",
        "|--:|---|---:|---|---|---|",
    ]
    for r in scored:
        lines.append(
            f"| {r['id']} | `{r['name']}` | {_book(r.get('cash_book_pct'))} | "
            f"{r.get('cash_starts')} | {r.get('side')} | "
            f"**{r.get('verdict')}** |"
        )
    lines += [
        "",
        "## Per-recipe prove",
        "",
        "| # | recipe | group | side | opp | prove n | after-fee WR | verdict | notes |",
        "|--:|---|---|---|---|---:|---:|---|---|",
    ]
    for r in scored:
        h = r.get("hold") or {}
        lines.append(
            f"| {r['id']} | `{r['name']}` | {r.get('group')} | "
            f"{r.get('side')} | "
            f"{'yes' if r.get('need_opp') else 'no'} | "
            f"{h.get('n', 0)} | {_pct(h.get('wr'))} | "
            f"**{r.get('verdict')}** | {r.get('why')} |"
        )
    lines += [
        "",
        "## Cards",
        "",
    ]
    for r in scored:
        h, d = r.get("hold") or {}, r.get("disc") or {}
        gate = []
        if r.get("clk"):
            gate.append(f"`{r['clk']}=True`")
        if r.get("need_opp"):
            gate.append("`oppset=True`")
        if r.get("kind") == "cash":
            gate.append("`days_since_E≤1` + `flag_E≥0`")
        if r.get("members"):
            gate.append("members " + ", ".join(f"`{m}`" for m in r["members"]))
            if r.get("net"):
                gate.append(f"net=`{r['net']}`")
        forbid = ""
        if r.get("kind") in ("clock_b", "cash") or (
                r.get("kind") == "combo" and r.get("side") == "long"):
            if r.get("kind") == "clock_b":
                forbid = "; forbid `clk_ext_veto` + `alarm`"
            elif r.get("kind") == "cash":
                forbid = "; forbid `alarm`"
        lines += [
            f"### {r['id']}. `{r['name']}`",
            "",
            f"**{r.get('verdict')}** — {r.get('why')}",
            "",
            f"- Thesis: {r['thesis']}. Side `{r.get('side')}`. "
            f"Hold {r.get('min_hold') if r.get('min_hold') is not None else 'mix'}.",
            f"- Gate: {', '.join(gate) or '—'}{forbid}.",
            f"- HAVE / calculable: {r.get('have') or '—'}",
            f"- FACTOR_MINE_ACTION cash Book% {_book(r.get('cash_book_pct'))} "
            f"(starts {r.get('cash_starts')}) — not KEEP.",
            f"- Discovery n={d.get('n', 0)} after-fee WR {_pct(d.get('wr'))} "
            f"mean_net={_mean_net(d)} (not KEEP).",
            f"- Prove n={h.get('n', 0)} after-fee WR {_pct(h.get('wr'))} "
            f"mean_net={_mean_net(h)} n_pos={h.get('n_pos', 0)}"
            + (
                f" (long {h.get('n_long', 0)} / short {h.get('n_short', 0)})"
                if r.get("side") == "mix" else ""
            )
            + ".",
            f"- Goal (b) eligible: "
            f"{'yes' if r.get('goal_b_eligible') else 'no (mix)'}.",
        ]
        if r.get("note"):
            lines.append(f"- {r['note']}")
        lines.append("")
    lines += [
        "## Discovery (not KEEP)",
        "",
        "Discovery ranks honesty only. A discovery >55% print is not a call.",
        "",
        "| recipe | disc n | disc after-fee WR |",
        "|---|---:|---:|",
    ]
    for r in scored:
        d = r.get("disc") or {}
        lines.append(f"| `{r['name']}` | {d.get('n', 0)} | {_pct(d.get('wr'))} |")
    lines += [
        "",
        "## Walk-forward (discovery folds, not KEEP)",
        "",
        "| recipe | fold1 n / WR | fold2 n / WR | fold3 n / WR |",
        "|---|---|---|---|",
    ]
    for r in scored:
        bits = []
        for w in r.get("walk") or []:
            bits.append(f"{w['n']} / {_pct(w['wr'])}")
        while len(bits) < 3:
            bits.append("—")
        lines.append(f"| `{r['name']}` | {bits[0]} | {bits[1]} | {bits[2]} |")
    lines += [
        "",
        "## Explicit verdict",
        "",
    ]
    keep_l = [r for r in scored if r.get("verdict") == "KEEP"]
    fail_l = [r for r in scored if r.get("verdict") == "FAIL"]
    thin_l = [r for r in scored if r.get("verdict") == "thin-n"]
    lines.append(
        "**KEEP:** " + (", ".join(f"`{r['name']}`" for r in keep_l) or "none")
    )
    lines.append(
        "**FAIL:** " + (", ".join(f"`{r['name']}`" for r in fail_l) or "none")
    )
    lines.append(
        "**thin-n:** " + (", ".join(f"`{r['name']}`" for r in thin_l) or "none")
    )
    if not long_keeps(scored):
        lines += [
            "",
            "**Goal (b):** FAIL. No long cleared ≥30 prove fires and "
            ">55% after-fee H.",
        ]
    else:
        names = ", ".join(f"`{r['name']}`" for r in long_keeps(scored))
        lines += [
            "",
            f"**Goal (b):** KEEP. Long(s) that cleared the fee bar: {names}.",
        ]
    lines += [
        "",
        "## Explicitly not live",
        "",
        "No recipe is wired into `flatten_robust` or cash/paper. A KEEP here "
        "would be a research card, not a ship. Do not train ML on "
        "flatten-only starved days.",
        "",
        "## Source",
        "",
        "`src/clock_b_tells.py` (#279 recipe wire) · cash-leader gates "
        "restated (e_fresh / join_vol_green / short_news_r; miner not "
        "imported) · `excel_clock_gate.py` / `CLOCK_MAP.md` · `j_winrate.py` "
        f"(`WIN_BAR`, `MIN_FIRES`, `FEE_RT={FEE_RT}`) · "
        f"Clock-B oppset `{OPPSET_SOURCE}` · "
        f"oppset-union remine `{REMINE}` · prior #{PRIOR_PR} · "
        "restored `data/factor_mine/panel.json`. Research only.",
        "",
    ]
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path, "w", encoding="utf-8").write("\n".join(lines))
    return path


def slim(r):
    def sl(s):
        if not s:
            return s
        return {
            "n": s.get("n"), "n_pos": s.get("n_pos"), "wr": s.get("wr"),
            "mean_net": s.get("mean_net"), "verdict": s.get("verdict"),
            "why": s.get("why"),
            "n_long": s.get("n_long"), "n_short": s.get("n_short"),
        }
    return {
        "id": r.get("id"), "group": r.get("group"),
        "name": r.get("name"), "kind": r.get("kind"),
        "clk": r.get("clk"), "need_opp": r.get("need_opp"),
        "title": r.get("title"), "thesis": r.get("thesis"),
        "have": r.get("have"), "note": r.get("note"),
        "side": r.get("side"), "min_hold": r.get("min_hold"),
        "members": r.get("members"), "net": r.get("net"),
        "cash_book_pct": r.get("cash_book_pct"),
        "cash_starts": r.get("cash_starts"),
        "disc": sl(r.get("disc")), "hold": sl(r.get("hold")),
        "verdict": r.get("verdict"), "why": r.get("why"),
        "goal_b_eligible": r.get("goal_b_eligible"),
        "walk": r.get("walk"),
        "walk_folds_wr_gt_50": r.get("walk_folds_wr_gt_50"),
        "n_walk_folds": r.get("n_walk_folds"),
    }


def run(panel_path=None, export_dir=None, oppset_path=None,
        out_md=None, out_json=None):
    assert_atoms_legal()
    clocks, leak = leak_check()
    panel = load_panel(panel_path)
    oppset = load_oppset_flagged(oppset_path or OPPSET_PATH)
    rows, aisle_dates, skipped, aisle_stats = name_days_from_aisle(
        panel, export_dir, oppset,
    )
    dates = sorted({r["date"] for r in rows})
    cutoff = cutoff_from_dates(dates, hold_frac=HOLD_FRAC, locked=None)
    disc, hold = split_rows(rows, cutoff)
    baseline_hold = score_hits(hold)
    baseline_disc = score_hits(disc)
    scored = score_space(disc, hold)
    hl = headline_from(scored, cutoff, baseline_hold)
    n_thin = sum(1 for r in scored if r.get("verdict") == "thin-n")
    n_fail = sum(1 for r in scored if r.get("verdict") == "FAIL")
    payload = {
        "status": "DONE",
        "generated": str(date.today()),
        "leak": leak,
        "live_untouched": "flatten_robust",
        "gate": gate_payload(),
        "fire_bar": {
            "win": f"after-fee H > 0 on prove name-days, strictly > {WIN_BAR}",
            "min_fires": MIN_FIRES,
            "fee_rt": FEE_RT,
            "fee_caveat": FEE_CAVEAT,
            "lift_never_keep": True,
            "cash_book_never_keep": True,
            "mix_keep_not_goal_b": True,
        },
        "goal": {
            "a": "touch-rate +3.3pp on Taskforce (partial, out of scope)",
            "b": "tangible factor-mine selection improvement via fee-KEEP long",
            "b_verdict": hl["verdict"] if hl.get("n_keep") else "FAIL",
        },
        "cutoff": cutoff,
        "split_kind": "time",
        "hold_frac": HOLD_FRAC,
        "lookback": panel.get("lookback"),
        "panel_to": panel.get("to_date"),
        "panel_n": panel.get("n_rows"),
        "aisle_dates": aisle_dates,
        "n_aisle_dates": len(aisle_dates),
        "skipped_days": skipped,
        "oppset_source": (aisle_stats or {}).get("oppset_source") or OPPSET_SOURCE,
        "n_oppset_flagged": sum(len(v) for v in oppset.values()),
        "n_aisle_panel": (aisle_stats or {}).get("n_panel"),
        "n_aisle_overlap": (aisle_stats or {}).get("n_overlap"),
        "n_aisle_oppset_only": (aisle_stats or {}).get("n_oppset_only"),
        "n_label_finviz_t": (aisle_stats or {}).get("n_label_finviz_t"),
        "n_tickers": len({r["ticker"] for r in rows}),
        "n_rows": len(rows),
        "n_disc": len(disc),
        "n_hold": len(hold),
        "n_keep": hl["n_keep"],
        "n_thin": n_thin,
        "n_fail": n_fail,
        "headline": hl,
        "remine": REMINE,
        "prior_pr": PRIOR_PR,
        "recipes_wire": "src/clock_b_tells.py#clock_b_recipes + restated cash gates",
        "baseline_disc": {
            "n": baseline_disc.get("n"), "wr": baseline_disc.get("wr"),
            "mean_net": baseline_disc.get("mean_net"),
        },
        "baseline_hold": {
            "n": baseline_hold.get("n"), "wr": baseline_hold.get("wr"),
            "mean_net": baseline_hold.get("mean_net"),
            "verdict": baseline_hold.get("verdict"),
            "why": baseline_hold.get("why"),
        },
        "scored": [slim(r) for r in scored],
        "clocks_generated": clocks.get("generated"),
        "feature_keys": sorted(FEATURE_KEYS),
        "forbidden": sorted(FORBIDDEN_FEATURE_FIELDS),
    }
    md = write_board(payload, out_md or BOARD_MD)
    js = out_json or BOARD_JSON
    os.makedirs(os.path.dirname(js) or ".", exist_ok=True)
    json.dump(payload, open(js, "w"), indent=2, default=str)
    print("wrote", md, js, flush=True)
    print("headline", hl["verdict"], hl["text"], flush=True)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", default="")
    ap.add_argument("--exports", default="")
    ap.add_argument("--oppset", default="")
    ap.add_argument("--out-md", default=BOARD_MD)
    ap.add_argument("--out-json", default=BOARD_JSON)
    args = ap.parse_args()
    run(panel_path=args.panel or None,
        export_dir=args.exports or None,
        oppset_path=args.oppset or None,
        out_md=args.out_md, out_json=args.out_json)


if __name__ == "__main__":
    main()
