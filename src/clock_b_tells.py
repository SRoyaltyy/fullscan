"""Clock-B (09:30-knowable) catalogue tells already supported by Fullscan.

Cyrus Stock Direction Tell Catalogue: 24 families / 240 entries —
candidate predictors, not proven edges. James order: map
have / calculable / need-source, then wire Fullscan-supported Clock-B
tells into the restored factor-mine panel / recipe path.

This module does **not** scrape new vendors, does **not** read VWAP /
intraday / same-day Gap / RelVol, and does **not** touch Webull or
``flatten_robust``. Excel fee-KEEP prove of the 10 priority combos is
a separate path. Theme Radar T−1 gap+RelVol is an optional opportunity-set
feed (``src/oppset_clock_b.py``) — not a same-day leak.

Clock-B proof: every atom below is knowable at 09:30 ET on session D
from prior tape, the morning packet, or a prior Finviz export.
"""
from __future__ import annotations

import math
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FORM4_PANEL = ROOT / "data" / "insider" / "history" / "monthly_panel.csv"

# Same-day tape / open-fill leaks. Never a Clock-B input.
LEAK_FIELDS = frozenset({
    "change", "change_pct", "Change", "Gap", "RelVol", "relvol",
    "vwap", "VWAP", "premarket", "pre_mkt", "intraday",
})

# ---------------------------------------------------------------------------
# 24-family catalogue map (have / calculable / need-source)
# ---------------------------------------------------------------------------
# status: have = already a panel / camera column
#         calculable = derived from those columns (no new vendor)
#         need-source = not wired; documented only
FAMILIES: tuple[dict, ...] = (
    {"id": "mom", "name": "Moderate / tape momentum",
     "status": "have",
     "cols": ("ohlc_ret_5", "macd_up", "last_green", "ohlc_hot_score")},
    {"id": "breakout", "name": "Completed breakout",
     "status": "have",
     "cols": ("ohlc_break_10", "candle_capture")},
    {"id": "peer", "name": "Peer relative strength",
     "status": "have",
     "cols": ("boxes.peer", "rs_week")},
    {"id": "sector", "name": "Sector relative strength",
     "status": "have",
     "cols": ("boxes.sector",)},
    {"id": "pos_cat", "name": "Fresh material positive catalyst",
     "status": "have",
     "cols": ("boxes.catal", "e_pol", "news_box", "news_prior",
              "erd_earn_react")},
    {"id": "extension", "name": "Limited / extreme prior extension",
     "status": "have",
     "cols": ("ohlc_ret_5", "ohlc_rvol", "rsi_ob")},
    {"id": "earnings", "name": "Earnings improvement",
     "status": "have",
     "cols": ("erd_flag_E", "e_pol", "erd_days_since_E")},
    {"id": "guidance", "name": "Raised guidance",
     "status": "calculable",
     "cols": ("news_prior", "news_box"),
     "note": "headline proxy (NEWS_POS includes 'raises'); no structured event"},
    {"id": "earn_react", "name": "Favorable earnings reaction",
     "status": "have",
     "cols": ("erd_earn_react", "e_pol")},
    {"id": "neg_cat", "name": "Negative catalyst",
     "status": "have",
     "cols": ("news_box", "news_prior", "e_pol")},
    {"id": "rel_weak", "name": "Relative weakness",
     "status": "have",
     "cols": ("boxes.peer", "boxes.sector", "ohlc_ret_5")},
    {"id": "fail_rec", "name": "Failed recovery",
     "status": "have",
     "cols": ("last_red", "macd_down")},
    {"id": "diminish", "name": "Diminishing progress",
     "status": "calculable",
     "cols": ("ohlc_ret_5", "ohlc_ret_10"),
     "note": "ret_10 stretched and ret_5 < half of ret_10"},
    {"id": "fail_bo", "name": "Failed breakout",
     "status": "calculable",
     "cols": ("ohlc_break_10", "last_red")},
    {"id": "hold_vs_sec", "name": "Stock holds while sector weakens",
     "status": "calculable",
     "cols": ("ohlc_ret_1", "last_green", "boxes.sector")},
    {"id": "insider", "name": "Insider buying",
     "status": "calculable",
     "cols": ("ins_buy", "form4_buy", "news_prior"),
     "note": "headline heuristic + Form-4 monthly panel when present"},
    {"id": "cash_econ", "name": "Improving cash economics",
     "status": "calculable",
     "cols": ("fv_inst",),
     "note": "prior-export institutional transactions > 0; not true FCF"},
    {"id": "stabilize", "name": "Stabilization / coil",
     "status": "have",
     "cols": ("flow_in", "rsi_os", "last_green", "ohlc_ret_5")},
    {"id": "volume", "name": "Prior relative volume",
     "status": "have",
     "cols": ("ohlc_rvol", "fv_rvol")},
    {"id": "revision", "name": "Analyst revision",
     "status": "have",
     "cols": ("erd_flag_R", "erd_days_since_R")},
    {"id": "flow", "name": "Tape flow / accumulation",
     "status": "have",
     "cols": ("flow_in", "macd_up")},
    {"id": "compress", "name": "Compression / NR7",
     "status": "have",
     "cols": ("ohlc_nr7",)},
    {"id": "weather", "name": "Morning weather S",
     "status": "have",
     "cols": ("morning_s",),
     "note": "book-level sit (S≤−3); already combo_se_5050_weather"},
    {"id": "news", "name": "News tone / headline",
     "status": "have",
     "cols": ("news_box", "news_prior")},
)

NEED_SOURCE = (
    "VWAP / intraday tape for open fills (James: no)",
    "Same-day Gap / RelVol from snapshot T (leak; Theme Radar T−1 oppset is the Clock-B feed)",
    "Webull live path (frozen)",
    "Structured raised-guidance event (headline proxy only)",
    "True FCF / cash-flow statement improvement (no vendor)",
    "Same-day earnings surprise stamped after 09:30 (finviz_events already drops it)",
)

# 10 priority combos → Clock-B recipe keys.
PRIORITY = (
    {"n": 1, "key": "clk_mom_break_peer",
     "title": "Moderate momentum + completed breakout + peer/sector strength",
     "families": ("mom", "breakout", "peer", "sector"),
     "side": "long"},
    {"n": 2, "key": "clk_fresh_cat_coil",
     "title": "Fresh material positive catalyst + limited prior extension",
     "families": ("pos_cat", "extension"),
     "side": "long"},
    {"n": 3, "key": "clk_earn_guide_react",
     "title": "Earnings improvement + raised guidance + favorable reaction",
     "families": ("earnings", "guidance", "earn_react"),
     "side": "long",
     "note": "only if pre-09:30 knowable (asof E/R + prior headline)"},
    {"n": 4, "key": "clk_neg_weak_fail",
     "title": "Negative catalyst + relative weakness + failed recovery",
     "families": ("neg_cat", "rel_weak", "fail_rec"),
     "side": "short"},
    {"n": 5, "key": "clk_ext_veto",
     "title": "Extreme extension + diminishing progress + failed breakout",
     "families": ("extension", "diminish", "fail_bo"),
     "side": "veto",
     "note": "long veto; also a short sleeve"},
    {"n": 6, "key": "clk_hold_vs_sector",
     "title": "Stock holds while sector weakens",
     "families": ("hold_vs_sec",),
     "side": "long"},
    {"n": 7, "key": "clk_insider_cash_stab",
     "title": "Insider buying + improving cash economics + stabilization",
     "families": ("insider", "cash_econ", "stabilize"),
     "side": "long",
     "note": "fires only when insider or Form-4 data is present"},
    {"n": 8, "key": "clk_flow_coil",
     "title": "Flow-in + last green + limited extension",
     "families": ("flow", "stabilize", "extension"),
     "side": "long"},
    {"n": 9, "key": "clk_r_up_coil",
     "title": "Analyst upgrade + last green + limited extension",
     "families": ("revision", "mom", "extension"),
     "side": "long"},
    {"n": 10, "key": "clk_nr7_mom",
     "title": "NR7 compression + moderate momentum",
     "families": ("compress", "mom"),
     "side": "long"},
)

COMBO_KEYS = tuple(p["key"] for p in PRIORITY)
COMBO_KID = {
    "clk_mom_break_peer": (
        "Clock-B #1: moderate prior momentum, a completed 10-session "
        "breakout (or candle capture), and peer or sector camera green"
    ),
    "clk_fresh_cat_coil": (
        "Clock-B #2: a fresh good catalyst (catal / EPS beat / packet or "
        "headline green) and the prior tape is not already exploded"
    ),
    "clk_earn_guide_react": (
        "Clock-B #3: knowable earnings improvement, a raised-guidance "
        "headline proxy, and a favorable reaction window — all public "
        "by 09:30 (same-day E after the open does not count)"
    ),
    "clk_neg_weak_fail": (
        "Clock-B #4: a red catalyst, peer/sector (or tape) weakness, "
        "and a failed recovery (last bar red or MACD down)"
    ),
    "clk_ext_veto": (
        "Clock-B #5 long veto: extreme prior extension plus diminishing "
        "progress or a failed breakout"
    ),
    "clk_hold_vs_sector": (
        "Clock-B #6: the stock held up (yesterday up or last bar green) "
        "while the sector camera is red"
    ),
    "clk_insider_cash_stab": (
        "Clock-B #7: insider / Form-4 buying is present, institutional "
        "transactions are not deteriorating, and the tape is stabilizing"
    ),
    "clk_flow_coil": (
        "Clock-B #8: prior flow-in, last bar green, not already extended"
    ),
    "clk_r_up_coil": (
        "Clock-B #9: a knowable analyst upgrade, last bar green, "
        "not already extended"
    ),
    "clk_nr7_mom": (
        "Clock-B #10: prior NR7 compression plus moderate momentum"
    ),
}

# Recipe names spliced onto the board (research only; not KEEP).
CLOCK_B_RECIPES = (
    "union_clk_mom_break_peer_h1",
    "union_clk_fresh_cat_coil_h1",
    "union_clk_earn_guide_react_h1",
    "short_clk_neg_weak_fail_h3",
    "short_clk_ext_veto_h3",
    "union_clk_hold_vs_sector_h1",
    "union_clk_insider_cash_stab_h3",
    "union_clk_flow_coil_h1",
    "union_clk_r_up_coil_h1",
    "union_clk_nr7_mom_h1",
)
# James: ship 1/2/4/5/6/10 + Theme Radar oppset hook. Do not wait on Excel.
CLOCK_B_CORE = (
    "union_clk_mom_break_peer_h1",
    "union_clk_fresh_cat_coil_h1",
    "short_clk_neg_weak_fail_h3",
    "short_clk_ext_veto_h3",
    "union_clk_hold_vs_sector_h1",
    "union_clk_nr7_mom_h1",
)
CLOCK_B_OPPSET_RECIPES = (
    "union_clk_mom_break_peer_opp_h1",
    "union_clk_fresh_cat_coil_opp_h1",
    "short_clk_neg_weak_fail_opp_h3",
    "short_clk_ext_veto_opp_h3",
    "union_clk_hold_vs_sector_opp_h1",
    "union_clk_nr7_mom_opp_h1",
    "union_oppset_h1",
    "oppset_h1",
)
CLOCK_B_PIN = CLOCK_B_CORE + CLOCK_B_OPPSET_RECIPES


def _finite(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _tone(boxes, cam: str) -> str:
    return str((boxes or {}).get(cam) or "missing").strip().lower()


def _yday_up(row: dict) -> bool:
    v = _finite(row.get("ohlc_ret_1"))
    if v is not None:
        return v > 0
    return bool(row.get("last_green"))


def _news_green(row: dict) -> bool:
    return (
        str(row.get("news_box") or "").lower() == "good"
        or str(row.get("news_prior") or "").lower() == "good"
    )


def _news_red(row: dict) -> bool:
    return (
        str(row.get("news_box") or "").lower() == "bad"
        or str(row.get("news_prior") or "").lower() == "bad"
    )


def _major_catalyst(row: dict) -> bool:
    boxes = row.get("boxes") or {}
    if _tone(boxes, "catal") == "good":
        return True
    if str(row.get("e_pol") or "").strip().lower() == "good":
        return True
    earn = bool(row.get("erd_earn_react") or row.get("earn_react"))
    if earn and str(row.get("e_pol") or "").strip().lower() != "bad":
        return True
    return False


def _is_burst(row: dict) -> bool:
    ret = _finite(row.get("ohlc_ret_5"))
    rvol = _finite(row.get("ohlc_rvol"))
    if ret is None or ret < 12.0:
        return False
    if not row.get("last_green"):
        return False
    return bool(row.get("ohlc_break_10")) or (rvol is not None and rvol >= 2.0)


def limited_ext(row: dict) -> bool:
    """Prior tape not exploded. Clock-B: ret_5 / rvol from date < D."""
    ret = _finite(row.get("ohlc_ret_5"))
    if ret is None or ret > 10.0:
        return False
    rvol = _finite(row.get("ohlc_rvol"))
    if rvol is not None and rvol > 2.2:
        return False
    return True


def moderate_mom(row: dict) -> bool:
    ret = _finite(row.get("ohlc_ret_5"))
    if ret is None or ret < 0.0 or ret > 10.0:
        return False
    if not row.get("last_green") and not row.get("macd_up"):
        return False
    return True


def completed_breakout(row: dict) -> bool:
    return bool(row.get("ohlc_break_10") or row.get("candle_capture"))


def peer_or_sector_good(row: dict) -> bool:
    boxes = row.get("boxes") or {}
    if _tone(boxes, "peer") == "good" or _tone(boxes, "sector") == "good":
        return True
    rs = _finite(row.get("rs_week"))
    return rs is not None and rs > 0


def extreme_ext(row: dict) -> bool:
    ret = _finite(row.get("ohlc_ret_5"))
    if ret is not None and ret >= 15.0:
        return True
    if row.get("rsi_ob"):
        return True
    return _is_burst(row)


def diminishing(row: dict) -> bool:
    r5 = _finite(row.get("ohlc_ret_5"))
    r10 = _finite(row.get("ohlc_ret_10"))
    if r5 is None or r10 is None:
        return False
    return r10 >= 8.0 and r5 < 0.5 * r10


def failed_breakout(row: dict) -> bool:
    return bool(row.get("ohlc_break_10") and row.get("last_red"))


def earn_improve(row: dict) -> bool:
    """Knowable EPS improvement. Same-day E after 09:30 is already dropped."""
    days = row.get("erd_days_since_E")
    if days is None or int(days) > 5:
        return False
    if str(row.get("e_pol") or "").strip().lower() == "bad":
        return False
    flag = row.get("erd_flag_E")
    if flag is not None and int(flag) >= 1:
        return True
    return str(row.get("e_pol") or "").strip().lower() == "good"


def guidance_proxy(row: dict) -> bool:
    """Raised-guidance proxy: prior/packet headline already painted good.

    NEWS_POS includes 'raises'. Structured guidance is need-source.
    """
    return _news_green(row)


def fav_react(row: dict) -> bool:
    if not row.get("erd_earn_react"):
        return False
    return str(row.get("e_pol") or "").strip().lower() != "bad"


def rel_weak(row: dict) -> bool:
    boxes = row.get("boxes") or {}
    if _tone(boxes, "peer") == "bad" or _tone(boxes, "sector") == "bad":
        return True
    ret = _finite(row.get("ohlc_ret_5"))
    return ret is not None and ret < 0


def fail_rec(row: dict) -> bool:
    return bool(row.get("last_red") or row.get("macd_down"))


def stabilize(row: dict) -> bool:
    if row.get("flow_in") or row.get("rsi_os"):
        return True
    return bool(row.get("last_green") and limited_ext(row))


def cash_econ(row: dict) -> bool:
    """Prior-export institutional transactions > 0. Not a P&L claim."""
    v = _finite(row.get("fv_inst"))
    return v is not None and v > 0


def insider_present(row: dict) -> bool:
    if row.get("ins_buy") or row.get("form4_buy"):
        return True
    return False


# --- priority combo evaluators --------------------------------------------

def clk_mom_break_peer(row: dict) -> bool:
    return moderate_mom(row) and completed_breakout(row) and peer_or_sector_good(row)


def clk_fresh_cat_coil(row: dict) -> bool:
    cat = _major_catalyst(row) or _news_green(row)
    return bool(cat and limited_ext(row))


def clk_earn_guide_react(row: dict) -> bool:
    return earn_improve(row) and guidance_proxy(row) and fav_react(row)


def clk_neg_weak_fail(row: dict) -> bool:
    neg = _news_red(row) or str(row.get("e_pol") or "").strip().lower() == "bad"
    return bool(neg and rel_weak(row) and fail_rec(row))


def clk_ext_veto(row: dict) -> bool:
    if not extreme_ext(row):
        return False
    return diminishing(row) or failed_breakout(row)


def clk_hold_vs_sector(row: dict) -> bool:
    boxes = row.get("boxes") or {}
    if _tone(boxes, "sector") != "bad":
        return False
    return _yday_up(row) or bool(row.get("last_green"))


def clk_insider_cash_stab(row: dict) -> bool:
    if not insider_present(row):
        return False
    inst = _finite(row.get("fv_inst"))
    if inst is not None and inst < 0:
        return False
    return stabilize(row)


def clk_flow_coil(row: dict) -> bool:
    return bool(row.get("flow_in") and row.get("last_green") and limited_ext(row))


def clk_r_up_coil(row: dict) -> bool:
    if int(row.get("erd_flag_R") or 0) != 1:
        return False
    days = row.get("erd_days_since_R")
    if days is None or int(days) > 5:
        return False
    return bool(row.get("last_green") and limited_ext(row))


def clk_nr7_mom(row: dict) -> bool:
    return bool(row.get("ohlc_nr7") and moderate_mom(row))


EVALS = {
    "clk_mom_break_peer": clk_mom_break_peer,
    "clk_fresh_cat_coil": clk_fresh_cat_coil,
    "clk_earn_guide_react": clk_earn_guide_react,
    "clk_neg_weak_fail": clk_neg_weak_fail,
    "clk_ext_veto": clk_ext_veto,
    "clk_hold_vs_sector": clk_hold_vs_sector,
    "clk_insider_cash_stab": clk_insider_cash_stab,
    "clk_flow_coil": clk_flow_coil,
    "clk_r_up_coil": clk_r_up_coil,
    "clk_nr7_mom": clk_nr7_mom,
}


def combo_true(row: dict, key: str) -> bool:
    fn = EVALS.get(key)
    if fn is None:
        return bool(row.get(key))
    return bool(fn(row))


def gate_row(row: dict, req: dict, forb: dict | None = None) -> bool:
    """Apply Clock-B require / forbid keys. Unknown keys are ignored."""
    req = req or {}
    forb = forb or {}
    for key in COMBO_KEYS:
        if req.get(key) and not combo_true(row, key):
            return False
        if forb.get(key) and combo_true(row, key):
            return False
    return True


def recipe_needs_clock_b(rec: dict | None) -> bool:
    req = (rec or {}).get("require") or {}
    forb = (rec or {}).get("forbid") or {}
    return any(k in COMBO_KEYS for k in list(req) + list(forb))


def recipe_needs_epol(rec: dict | None) -> bool:
    req = (rec or {}).get("require") or {}
    forb = (rec or {}).get("forbid") or {}
    keys = set(req) | set(forb)
    return bool(keys & {
        "clk_fresh_cat_coil", "clk_earn_guide_react", "clk_neg_weak_fail",
        "major_catalyst", "yday_or_catalyst", "yday_and_catalyst",
    })


# --- Form-4 (optional; completed month < session) -------------------------

@lru_cache(maxsize=1)
def _form4_index() -> dict[str, list[tuple[str, float, float]]]:
    """ticker → [(month, n_buys, net_value), ...] sorted by month."""
    path = FORM4_PANEL
    if not path.is_file():
        return {}
    out: dict[str, list[tuple[str, float, float]]] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    lines = text.splitlines()
    if len(lines) < 2:
        return {}
    hdr = [h.strip() for h in lines[0].split(",")]
    try:
        i_t = hdr.index("ticker")
        i_m = hdr.index("month")
        i_b = hdr.index("n_buys")
        i_n = hdr.index("net_value")
    except ValueError:
        return {}
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) <= max(i_t, i_m, i_b, i_n):
            continue
        t = parts[i_t].strip().upper()
        month = parts[i_m].strip()
        if not t or len(month) < 7:
            continue
        try:
            n_buys = float(parts[i_b] or 0)
            net = float(parts[i_n] or 0)
        except ValueError:
            continue
        out.setdefault(t, []).append((month, n_buys, net))
    for t in out:
        out[t].sort()
    return out


def form4_buy_asof(ticker: str, date: str) -> bool:
    """True when the last completed Form-4 month before ``date`` shows buys.

    Month 2026-08 is knowable on 2026-09-16 09:30. Missing panel → False.
    """
    t = str(ticker or "").strip().upper()
    d = str(date or "")[:10]
    if not t or len(d) < 7:
        return False
    cutoff = d[:7]
    rows = _form4_index().get(t) or []
    hit = None
    for month, n_buys, net in rows:
        if month < cutoff:
            hit = (month, n_buys, net)
        else:
            break
    if hit is None:
        return False
    return hit[1] > 0 and hit[2] >= 0


def stamp_row(row: dict) -> dict:
    """Stamp Clock-B combo flags onto a panel row. Mutates ``row``."""
    if not isinstance(row, dict):
        return row
    if row.get("form4_buy") is None:
        row["form4_buy"] = form4_buy_asof(row.get("ticker") or "",
                                          row.get("date") or "")
    for key, fn in EVALS.items():
        row[key] = bool(fn(row))
    row["_clock_b"] = True
    return row


def attach_panel(panel: dict) -> dict:
    """Stamp Clock-B flags on every row. Cheap; idempotent."""
    if not isinstance(panel, dict):
        return panel
    for r in panel.get("rows") or []:
        stamp_row(r)
    by_date = panel.get("by_date") or {}
    if isinstance(by_date, dict):
        for rows in by_date.values():
            for r in rows or []:
                if not r.get("_clock_b"):
                    stamp_row(r)
    panel["_clock_b"] = True
    return panel


def clock_b_recipes(make_recipe) -> list[dict]:
    """Named sleeves for the 10 priority combos. Research only — not KEEP."""
    recs = []

    def add(**kw):
        recs.append(make_recipe(**kw))

    # Longs carry the #5 extreme-extension veto.
    veto = {"clk_ext_veto": True, "alarm": True}
    add(name="union_clk_mom_break_peer_h1", universe="union", hold=1,
        require={"clk_mom_break_peer": True}, forbid=dict(veto),
        rank="hot_score",
        note="Clock-B #1 mom+breakout+peer/sector (research; not KEEP)")
    add(name="union_clk_fresh_cat_coil_h1", universe="union", hold=1,
        require={"clk_fresh_cat_coil": True}, forbid=dict(veto),
        rank="cond",
        note="Clock-B #2 fresh catalyst + limited extension (research; not KEEP)")
    add(name="union_clk_earn_guide_react_h1", universe="union", hold=1,
        require={"clk_earn_guide_react": True}, forbid=dict(veto),
        rank="cond",
        note="Clock-B #3 knowable E + guidance proxy + react (research; not KEEP)")
    add(name="short_clk_neg_weak_fail_h3", universe="union", hold=3,
        side="short", require={"clk_neg_weak_fail": True},
        note="Clock-B #4 neg catalyst + weakness + failed recovery")
    add(name="short_clk_ext_veto_h3", universe="union", hold=3,
        side="short", require={"clk_ext_veto": True},
        note="Clock-B #5 extreme ext + fade/fail-break as a short")
    add(name="union_clk_hold_vs_sector_h1", universe="union", hold=1,
        require={"clk_hold_vs_sector": True}, forbid=dict(veto),
        rank="cond",
        note="Clock-B #6 stock holds while sector camera is red")
    add(name="union_clk_insider_cash_stab_h3", universe="union", hold=3,
        require={"clk_insider_cash_stab": True}, forbid=dict(veto),
        rank="cond",
        note="Clock-B #7 insider/Form-4 + inst Tx + stabilize (if data)")
    add(name="union_clk_flow_coil_h1", universe="union", hold=1,
        require={"clk_flow_coil": True}, forbid=dict(veto),
        rank="cond",
        note="Clock-B #8 flow-in + green + not extended")
    add(name="union_clk_r_up_coil_h1", universe="union", hold=1,
        require={"clk_r_up_coil": True}, forbid=dict(veto),
        rank="list",
        note="Clock-B #9 knowable upgrade + green + not extended")
    add(name="union_clk_nr7_mom_h1", universe="union", hold=1,
        require={"clk_nr7_mom": True}, forbid=dict(veto),
        rank="hot_score",
        note="Clock-B #10 NR7 + moderate momentum")
    # Theme Radar T−1 gap+RelVol oppset: rank/filter on the existing union,
    # plus a dedicated universe. Optional — empty if the CSV was not pulled.
    add(name="union_oppset_h1", universe="union", hold=1,
        require={"oppset": True}, forbid={"alarm": True},
        rank="opp_rvol",
        note="Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP)")
    add(name="oppset_h1", universe="oppset", hold=1,
        require={"oppset": True},
        rank="opp_rvol",
        note="Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP)")
    add(name="union_clk_mom_break_peer_opp_h1", universe="union", hold=1,
        require={"clk_mom_break_peer": True, "oppset": True}, forbid=dict(veto),
        rank="opp_rvol",
        note="Clock-B #1 ∩ Theme Radar T−1 oppset")
    add(name="union_clk_fresh_cat_coil_opp_h1", universe="union", hold=1,
        require={"clk_fresh_cat_coil": True, "oppset": True}, forbid=dict(veto),
        rank="opp_rvol",
        note="Clock-B #2 ∩ Theme Radar T−1 oppset")
    add(name="short_clk_neg_weak_fail_opp_h3", universe="union", hold=3,
        side="short", require={"clk_neg_weak_fail": True, "oppset": True},
        rank="opp_rvol",
        note="Clock-B #4 ∩ Theme Radar T−1 oppset")
    add(name="short_clk_ext_veto_opp_h3", universe="union", hold=3,
        side="short", require={"clk_ext_veto": True, "oppset": True},
        rank="opp_rvol",
        note="Clock-B #5 ∩ Theme Radar T−1 oppset")
    add(name="union_clk_hold_vs_sector_opp_h1", universe="union", hold=1,
        require={"clk_hold_vs_sector": True, "oppset": True}, forbid=dict(veto),
        rank="opp_rvol",
        note="Clock-B #6 ∩ Theme Radar T−1 oppset")
    add(name="union_clk_nr7_mom_opp_h1", universe="union", hold=1,
        require={"clk_nr7_mom": True, "oppset": True}, forbid=dict(veto),
        rank="opp_rvol",
        note="Clock-B #10 ∩ Theme Radar T−1 oppset")
    return recs


def leak_fields_used(row: dict) -> list[str]:
    """Any leak key present *and* consulted as a truthy Clock-B input."""
    return [k for k in LEAK_FIELDS if k in row and row.get(k) not in (None, "")]


def panel_fire_counts(panel: dict, dates: list[str] | None = None) -> dict:
    """How many union rows fire each Clock-B combo. Not a KEEP score."""
    want = set(dates or [])
    out = {k: {} for k in COMBO_KEYS}
    n_by: dict[str, int] = {}
    src_by: dict[str, dict[str, int]] = {}
    for r in panel.get("rows") or []:
        d = str(r.get("date") or "")[:10]
        if want and d not in want:
            continue
        n_by[d] = n_by.get(d, 0) + 1
        bag = src_by.setdefault(d, {})
        for s in r.get("sources") or []:
            bag[s] = bag.get(s, 0) + 1
        for key in COMBO_KEYS:
            day = out[key]
            day[d] = day.get(d, 0) + int(combo_true(r, key))
    return {"n": n_by, "sources": src_by, "fires": out}


def family_status_table() -> list[dict]:
    return [dict(f) for f in FAMILIES]
