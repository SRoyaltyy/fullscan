"""Join Excel open-gate features × fullscan morning dumps, post-8-13 holdout.

Leak-free: same-row Excel atoms are only the locked 44 / open fills.
H/I same-row are labels only. Same-day Finviz Change / High / Low / Close
are never features. Same-day Open / Gap / J are 09:30-fair.

Fullscan side uses standing dumps (join ranked, prior-day 1d book, morning
feature_asof). Same-day stock-book JSON is afternoon — not an open feature.

Live flatten_robust is not imported or written.

  python3 excel_bot/engine/join_post_813.py
"""
from __future__ import annotations

import csv
import json
import math
import os
import sys
from collections import defaultdict
from datetime import date
from glob import glob

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    assert_excel_clock_gate, assert_feature_legal, gate_payload,
)

RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
JOIN_DIR = os.path.join(REPO, "data", "join")
BOOK_DIR = os.path.join(REPO, "data", "stock_book")
ASOF_DIR = os.path.join(REPO, "data", "feature_asof")
EXPORTS = os.path.join(REPO, "data", "exports")
SLEEVE = os.path.join(REPO, "data", "sleeve_merge", "trades.csv")
FEES_PATH = os.path.join(REPO, "00_grounding", "futubull_fees.json")

HOLD_CUT = "2026-08-13"  # discovery KEEP was strictly after this date
PANEL_START = "2026-08-13"  # first join+Finviz date on disk
FEE_RT = 0.0015  # Futubull ~15 bp round-trip (Excel ship bar)
BEAT_PP = 0.20  # must beat fullscan-alone by ≥20 bp after fees
TOP_N = 8
WIDE_N = 80
ELEV_BAND = 80
MIN_TAPE = 8  # window-adapted; Excel usual 40 is larger than this tape
MIN_MONTH = 12
J_FRESH_DAYS = 5  # prior session Open must be this close (skip April gap)

# Time holdout. Discovery = first half of the published post-8-13 tape.
# Prove = later sessions. Both are weekday US sessions only.
DISCOVERY = ("2026-08-14", "2026-08-25")
PROVE = ("2026-08-26", "2026-09-07")
ORIG = ("2026-08-14", "2026-09-07")
KEEP_RECIPES = ("avoid_J_ge0", "elev_cap2_J_le-1")

# Pre-specified Excel atoms (prior mines / CLOCK_MAP). Not searched on holdout.
EXCEL_ATOMS = (
    ("J", 0, "value"),
    ("AH", 0, "value"),
    ("ER", 0, "value"),
    ("FQ", 0, "value"),
    ("JB", 0, "value"),
    ("JC", 0, "value"),
    ("H", 1, "value"),
    ("I", 1, "value"),
    ("J", 1, "value"),
    ("G", 1, "value"),
)


def _num(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in ("-", "nan", "None"):
        return None
    s = s.replace(",", "").replace("%", "")
    if s.endswith(("K", "M", "B")):
        mult = {"K": 1e3, "M": 1e6, "B": 1e9}[s[-1]]
        s = s[:-1]
        try:
            return float(s) * mult
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def _pct(v):
    """Finviz percent cell → fraction (0.01 = 1%)."""
    if v is None:
        return None
    raw = str(v).strip()
    if not raw or raw in ("-", "nan"):
        return None
    had = "%" in raw
    x = _num(raw)
    if x is None:
        return None
    if had or abs(x) > 1.5:
        return x / 100.0
    # already a fraction in some dumps
    return x


def load_fees():
    if not os.path.isfile(FEES_PATH):
        return {}
    return json.loads(open(FEES_PATH, encoding="utf-8").read())


def order_fees(shares, price, side, f):
    if not f or shares <= 0 or price <= 0:
        return 0.0
    amount = shares * price
    comm = min(max(f["commission_per_share"] * shares, f["commission_min_per_order"]),
               f["commission_max_pct_of_amount"] * amount)
    plat = min(max(f["platform_per_share"] * shares, f["platform_min_per_order"]),
               f["platform_max_pct_of_amount"] * amount)
    settle = f["settlement_per_share"] * shares
    total = comm + plat + settle
    if side == "sell":
        reg = max(f["regulatory_pct_of_amount_sell_only"] * amount,
                  f["regulatory_min_per_order"])
        taf = min(max(f["taf_per_share_sell_only"] * shares, f["taf_min_per_order"]),
                  f["taf_max_per_order"])
        total += reg + taf
    return round(total, 4)


def load_finviz():
    """date -> ticker -> {open, prev, h, i, vol, gap, spy later}."""
    by = {}
    for path in sorted(glob(os.path.join(EXPORTS, "finviz_????-??-??.csv"))):
        iso = os.path.basename(path)[7:17]
        mp = {}
        with open(path, encoding="utf-8", errors="replace") as fh:
            for rec in csv.DictReader(fh):
                t = (rec.get("Ticker") or "").strip().upper()
                if not t:
                    continue
                mp[t] = {
                    "open": _num(rec.get("Open")),
                    "prev": _num(rec.get("Prev Close")),
                    "close": _num(rec.get("Price")),
                    "high": _num(rec.get("High")),
                    "low": _num(rec.get("Low")),
                    "vol": _num(rec.get("Volume")),
                    "h": _pct(rec.get("Change from Open")),
                    "i": _pct(rec.get("Change")),
                    "gap": _pct(rec.get("Gap")),
                    "relvol": _num(rec.get("Relative Volume")),
                }
        if mp:
            by[iso] = mp
    return by


def history_index(fz):
    """ticker -> sorted list of (date, rec) for prior-row walks."""
    hist = defaultdict(list)
    for iso in sorted(fz):
        for t, rec in fz[iso].items():
            hist[t].append((iso, rec))
    return hist


def weekday(iso):
    return date.fromisoformat(iso).weekday()  # 0=Mon … 6=Sun


def is_session(iso):
    """US equity session = Mon–Fri. Sunday join dumps are not a 1d clock."""
    return weekday(iso) < 5


def prior_bars(hist, ticker, iso, sessions_only=True):
    """Prior Finviz rows. Default skips Sat/Sun dumps (not session Opens)."""
    rows = hist.get(ticker) or []
    out = []
    for d, r in rows:
        if d >= iso:
            continue
        if sessions_only and not is_session(d):
            continue
        out.append((d, r))
    return out


def j_fresh(prior_iso, iso, max_days=J_FRESH_DAYS):
    if not prior_iso:
        return False
    return (date.fromisoformat(iso) - date.fromisoformat(prior_iso)).days <= max_days


def j_from_opens(today_open, prior_open):
    """Excel J value: (C[t] − C[t−1]) / C[t−1]. Opens only."""
    if not today_open or not prior_open:
        return None
    return (today_open - prior_open) / prior_open


# Prove-window case studies (real name-days). Avoid + elevate.
CASE_AVOID = {
    "recipe": "avoid_J_ge0",
    "date": "2026-08-27",
    "dropped": "FIGR",
    "added": "EMBJ",
}
CASE_ELEV = {
    "recipe": "elev_cap2_J_le-1",
    "date": "2026-09-04",
    "dropped": "HRMY",
    "added": "AVAH",
}


def excel_features(hist, ticker, iso, today_open):
    """Open-knowable Excel atoms for name-day. Never same-row H/I.

    J uses the prior *session* Open (weekday Finviz only). A Sunday or
    April dump is not yesterday's session.
    """
    prior = prior_bars(hist, ticker, iso)
    out = {
        "J": None, "AH": None, "ER": None, "FQ": None, "JB": None, "JC": None,
        "H_l1": None, "I_l1": None, "J_l1": None, "G_l1": None,
        "n_prior": len(prior),
        "prior_open_date": prior[-1][0] if prior else None,
        "J_fresh": False,
    }
    if today_open and prior and prior[-1][1].get("open"):
        po = prior[-1][1]["open"]
        if po:
            out["J"] = j_from_opens(today_open, po)
            out["J_fresh"] = j_fresh(out["prior_open_date"], iso)
    if len(prior) >= 2:
        o0 = prior[-1][1].get("open")
        o1 = prior[-2][1].get("open")
        if o0 and o1:
            out["J_l1"] = (o0 - o1) / o1
        v0 = prior[-1][1].get("vol")
        v1 = prior[-2][1].get("vol")
        if v0 and v1:
            out["G_l1"] = v0 / v1
    if prior:
        out["H_l1"] = prior[-1][1].get("h")
        out["I_l1"] = prior[-1][1].get("i")
        h1, i1 = out["H_l1"], out["I_l1"]
        er = 0
        if (h1 is not None and h1 >= 0.05) or (i1 is not None and i1 >= 0.05):
            er = 1
        elif (h1 is not None and h1 <= -0.03) or (i1 is not None and i1 <= -0.03):
            er = -1
        out["ER"] = er
        out["FQ"] = 1 if (h1 is not None and h1 > 0.03) else 0
    hs = [r.get("h") for _, r in prior if r.get("h") is not None]
    last6 = hs[-6:]
    last8 = hs[-8:]
    if last6:
        out["AH"] = sum(1 for x in last6 if x <= -0.05)
    if last8:
        out["JB"] = 1 if sum(1 for x in last8 if abs(x) > 0.03) >= 7 else 0
        out["JC"] = 1 if all(x >= -0.03 for x in last8) else 0
    return out


def load_join_days():
    """date -> list of join rows in rank order."""
    by = {}
    for path in sorted(glob(os.path.join(JOIN_DIR, "????-??-??_ranked.csv"))):
        iso = os.path.basename(path)[:10]
        rows = []
        with open(path, encoding="utf-8", errors="replace") as fh:
            for i, rec in enumerate(csv.DictReader(fh), 1):
                t = (rec.get("Ticker") or rec.get("ticker") or "").strip().upper()
                if not t:
                    continue
                fam = rec.get("families_known")
                try:
                    fam_n = int(float(fam)) if fam not in (None, "") else 0
                except ValueError:
                    fam_n = 0
                rows.append({
                    "ticker": t,
                    "rank": i,
                    "total_score": _num(rec.get("total_score")),
                    "families_known": fam_n,
                    "incomplete": fam_n < 13,
                    "sector": (rec.get("sector") or "").strip(),
                    "size": (rec.get("size") or "").strip(),
                    "vol": (rec.get("vol") or "").strip(),
                    "veto": str(rec.get("veto") or "").lower() in ("true", "1", "yes"),
                })
        if rows:
            by[iso] = rows
    return by


def load_book_1d():
    """date -> [{ticker, score}] 1d buy list (afternoon stamp — PIT = prior)."""
    by = {}
    for path in sorted(glob(os.path.join(BOOK_DIR, "????-??-??_stock_book.json"))):
        iso = os.path.basename(path)[:10]
        try:
            raw = json.load(open(path, encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        buy = ((raw.get("books") or {}).get("1d") or {}).get("buy") or []
        names = []
        for rec in buy:
            if isinstance(rec, dict):
                t = (rec.get("ticker") or rec.get("Ticker") or "").strip().upper()
                if t:
                    names.append({"ticker": t, "score": rec.get("score")})
        if names:
            by[iso] = names
    return by


def prior_date(keys, iso):
    prev = [k for k in keys if k < iso]
    return prev[-1] if prev else None


def load_asof():
    """date -> ticker -> row (morning features + forward rets)."""
    by = {}
    for path in sorted(glob(os.path.join(ASOF_DIR, "????-??-??_feature_asof.csv"))):
        iso = os.path.basename(path)[:10]
        mp = {}
        with open(path, encoding="utf-8", errors="replace") as fh:
            for rec in csv.DictReader(fh):
                t = (rec.get("Ticker") or rec.get("ticker") or "").strip().upper()
                if t:
                    mp[t] = rec
        if mp:
            by[iso] = mp
    return by


def load_flatten():
    rows = []
    if not os.path.isfile(SLEEVE):
        return rows
    with open(SLEEVE, encoding="utf-8", errors="replace") as fh:
        for rec in csv.DictReader(fh):
            if (rec.get("side") or "").upper() != "BUY":
                continue
            iso = (rec.get("entry_date") or "")[:10]
            t = (rec.get("ticker") or "").strip().upper()
            if iso and t:
                rec["_date"] = iso
                rec["_ticker"] = t
                rec["_ret"] = _num(rec.get("ret_pct"))
                rec["_pnl"] = _num(rec.get("pnl"))
                rows.append(rec)
    return rows


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def tstat(xs):
    xs = [x for x in xs if x is not None]
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    if var <= 0:
        return None
    return m / math.sqrt(var / n)


def ghost(trades):
    """name / month / day lottery on after-fee nets."""
    if not trades:
        return {"name": "FAIL", "month": "FAIL", "day": "FAIL",
                "reasons": ["empty"], "pass": False}
    nets = [tr["net"] for tr in trades]
    wins = [x for x in nets if x > 0]
    gross = sum(wins) or 0.0
    by_t = defaultdict(list)
    by_m = defaultdict(list)
    by_d = defaultdict(list)
    for tr in trades:
        by_t[tr["ticker"]].append(tr["net"])
        by_m[tr["date"][:7]].append(tr["net"])
        by_d[tr["date"]].append(tr["net"])
    reasons = []
    n = len(trades)
    top_name_n = max(len(v) for v in by_t.values())
    top_name = max(by_t, key=lambda k: len(by_t[k]))
    name_ok = (top_name_n / n) <= 0.15 and len(by_t) >= 5
    if not name_ok:
        reasons.append(f"name:{top_name}={top_name_n}/{n}")
    if wins and gross > 0:
        top_name_w = max(sum(x for x in v if x > 0) for v in by_t.values())
        if top_name_w / gross > 0.25:
            name_ok = False
            reasons.append("name_win_share")
    month_ok = True
    months = sorted(by_m)
    if len(months) >= 2:
        scored = []
        for m in months:
            if len(by_m[m]) >= MIN_MONTH:
                scored.append((m, mean(by_m[m])))
        if len(scored) >= 2:
            signs = [1 if v > 0 else -1 if v < 0 else 0 for _, v in scored]
            if len(set(signs)) > 1:
                month_ok = False
                reasons.append("month_split")
        else:
            month_ok = True  # too short to split — not a fail
    day_ok = True
    if wins and gross > 0:
        top_day = max(sum(x for x in v if x > 0) for v in by_d.values())
        if top_day / gross > 0.25:
            day_ok = False
            reasons.append("lottery_day")
    ok = name_ok and month_ok and day_ok
    return {
        "name": "PASS" if name_ok else "FAIL",
        "month": "PASS" if month_ok else "FAIL",
        "day": "PASS" if day_ok else "FAIL",
        "reasons": reasons,
        "pass": ok,
        "n_tickers": len(by_t),
        "n_dates": len(by_d),
        "top_name": top_name,
        "top_name_n": top_name_n,
    }


def tapes(trades, spy):
    up = [tr["net"] for tr in trades if spy.get(tr["date"]) == 1]
    dn = [tr["net"] for tr in trades if spy.get(tr["date"]) == -1]
    return {
        "up": {"n": len(up), "mean": mean(up)},
        "dn": {"n": len(dn), "mean": mean(dn)},
        "enough": len(up) >= MIN_TAPE and len(dn) >= MIN_TAPE,
    }


def dollar_book(day_picks, fees):
    """$10k / 8 names, whole shares, Futubull both sides, open→close."""
    day_rets = []
    fee_sum = 0.0
    for iso, picks in sorted(day_picks.items()):
        if not picks:
            continue
        slot = 10000.0 / max(len(picks), 1)
        nets = []
        for tr in picks:
            o, c = tr.get("open"), tr.get("close")
            if not o or not c or o <= 0:
                continue
            sh = int(slot // o)
            if sh <= 0:
                continue
            fin = order_fees(sh, o, "buy", fees)
            fout = order_fees(sh, c, "sell", fees)
            pnl = sh * (c - o) - fin - fout
            fee_sum += fin + fout
            nets.append(pnl / (sh * o))
        if nets:
            day_rets.append(mean(nets))
    return {"n_days": len(day_rets), "mean": mean(day_rets), "fee_sum": fee_sum}


def score_book(name, kind, trades, baseline, spy, notes=""):
    nets = [tr["net"] for tr in trades]
    h = mean(nets)
    base = baseline["holdout_mean"] if baseline else None
    vs = None if (h is None or base is None) else (h - base) * 100
    g = ghost(trades)
    tp = tapes(trades, spy)
    reasons = []
    if not trades:
        reasons.append("empty")
    if h is not None and h <= 0:
        reasons.append("hold_sign")
    if vs is not None and vs < BEAT_PP:
        reasons.append("no_edge_vs_fullscan")
    if not g["pass"]:
        reasons.append("ghost:" + ",".join(g["reasons"] or ["fail"]))
    if tp["enough"]:
        if (tp["up"]["mean"] is not None and tp["up"]["mean"] <= 0) or (
                tp["dn"]["mean"] is not None and tp["dn"]["mean"] <= 0):
            reasons.append("spy_tape")
    else:
        reasons.append("tape_thin")
    asof_nets = [tr["asof_net"] for tr in trades if tr.get("asof_net") is not None]
    quality = [r for r in reasons if r != "tape_thin"]
    if not trades:
        verdict = "THIN"
    elif not quality:
        verdict = "KEEP"
    elif set(quality) <= {"tape_thin"}:
        verdict = "KEEP"
    else:
        verdict = "KILL"
    # window-adapted: tape_thin alone is not KILL (user: both tapes if enough n)
    if reasons == ["tape_thin"] and vs is not None and vs >= BEAT_PP and g["pass"] and h and h > 0:
        verdict = "KEEP"
        reasons = []
    if "tape_thin" in reasons and verdict == "KILL":
        reasons = [r for r in reasons if r != "tape_thin"] or reasons
    return {
        "name": name, "kind": kind, "verdict": verdict, "fail_reasons": reasons,
        "n": len(trades), "n_tickers": len({tr["ticker"] for tr in trades}),
        "n_dates": len({tr["date"] for tr in trades}),
        "holdout_mean": h, "t": tstat(nets),
        "vs_fullscan_pp": vs,
        "win": (sum(1 for x in nets if x > 0) / len(nets)) if nets else None,
        "ghost": g, "tapes": tp,
        "asof_1d_mean": mean(asof_nets), "asof_1d_n": len(asof_nets),
        "notes": notes, "live_untouched": "flatten_robust",
    }


def pick_book(ranked, flags, mode, avoid_key=None, elev_key=None, n=TOP_N,
              cap=2):
    """mode: raw | avoid_refill | elev | elev_cap | intersect.

    elev_cap: keep the fullscan eight; swap at most `cap` names that trip
    avoid_key (or the tail) for elev_key hits in ranks n+1..80.
    """
    if mode == "raw":
        return [r for r in ranked[:n]]
    if mode == "avoid_refill":
        out = []
        for r in ranked:
            if avoid_key and flags.get(r["ticker"], {}).get(avoid_key):
                continue
            out.append(r)
            if len(out) >= n:
                break
        return out
    if mode == "elev":
        # replace-the-book: first n elev_key hits in ranks 1..80
        out = []
        for r in ranked:
            if r["rank"] > ELEV_BAND:
                break
            if elev_key and flags.get(r["ticker"], {}).get(elev_key):
                out.append(r)
            if len(out) >= n:
                break
        return out or list(ranked[:n])
    if mode == "elev_cap":
        core = list(ranked[:n])
        extras = []
        for r in ranked:
            if r["rank"] <= n or r["rank"] > ELEV_BAND:
                continue
            if elev_key and flags.get(r["ticker"], {}).get(elev_key):
                extras.append(r)
        if not extras:
            return core
        # drop from the eight: prefer names that trip avoid_key, else tail
        drop_i = []
        for i, r in enumerate(core):
            if avoid_key and flags.get(r["ticker"], {}).get(avoid_key):
                drop_i.append(i)
        if len(drop_i) < cap:
            for i in range(len(core) - 1, -1, -1):
                if i not in drop_i:
                    drop_i.append(i)
                if len(drop_i) >= cap:
                    break
        drop_i = drop_i[:cap]
        keep = [r for i, r in enumerate(core) if i not in drop_i]
        return (keep + extras)[:n]
    if mode == "intersect":
        out = []
        for r in ranked[:n]:
            if elev_key and flags.get(r["ticker"], {}).get(elev_key):
                out.append(r)
        return out
    return ranked[:n]


def build_panel(joins, fz, hist, books, asof, spy, start=HOLD_CUT,
                sessions_only=True):
    """Name-days with a Finviz H label.

    start is exclusive (same as the original HOLD_CUT filter) unless
    start is None, in which case every join+Finviz date is kept.
    sessions_only drops Sat/Sun join dumps — those are not a 1d clock.
    """
    book_dates = sorted(books)
    rows = []
    flags_by_day = {}
    for iso in sorted(joins):
        if start is not None and iso <= start:
            continue
        if sessions_only and not is_session(iso):
            continue
        if iso not in fz:
            continue
        spy_ret = None
        if "SPY" in fz[iso] and fz[iso]["SPY"].get("i") is not None:
            spy_ret = fz[iso]["SPY"]["i"]
            spy[iso] = 1 if spy_ret > 0.001 else -1 if spy_ret < -0.001 else 0
        prev_b = prior_date(book_dates, iso)
        prior_buy = {x["ticker"] for x in books.get(prev_b, [])} if prev_b else set()
        day_flags = {}
        asof_day = asof.get(iso) or {}
        for rec in joins[iso]:
            t = rec["ticker"]
            fz_t = fz[iso].get(t)
            if not fz_t or fz_t.get("h") is None or not fz_t.get("open"):
                continue
            xl = excel_features(hist, t, iso, fz_t["open"])
            a = asof_day.get(t) or {}
            j = xl["J"] if xl.get("J_fresh") else None
            flags = {
                "J_fresh": bool(xl.get("J_fresh")),
                "J_le-1": j is not None and j <= -0.01,
                "J_lt0": j is not None and j < 0,
                "J_ge0": j is not None and j >= 0,
                "J_ge1": j is not None and j >= 0.01,
                "AH_ge1": (xl["AH"] or 0) >= 1,
                "AH_ge2": (xl["AH"] or 0) >= 2,
                "ER_m1": xl["ER"] == -1,
                "ER_p1": xl["ER"] == 1,
                "FQ": xl["FQ"] == 1,
                "JB": xl["JB"] == 1,
                "JC": xl["JC"] == 1,
                "H_l1_lt0": xl["H_l1"] is not None and xl["H_l1"] < 0,
                "I_l1_lt0": xl["I_l1"] is not None and xl["I_l1"] < 0,
                "G_l1_ge3": xl["G_l1"] is not None and xl["G_l1"] >= 3,
                "incomplete": rec["incomplete"],
                "prior_book": t in prior_buy,
                "asof_join_good": str(a.get("join_good") or "").lower() == "true",
                "asof_blue": str(a.get("blue") or "").lower() == "true",
                "asof_ab_good": str(a.get("ab_good") or "").lower() == "true",
            }
            day_flags[t] = flags
            asof_ret = _num(a.get("ret_1d"))
            asof_net = None if asof_ret is None else asof_ret / 100.0 - FEE_RT
            i = fz_t.get("i")
            rows.append({
                **rec, "date": iso, "xl": xl, "flags": flags,
                "h": fz_t["h"], "i": i,
                "open": fz_t["open"], "close": fz_t.get("close"),
                "net": fz_t["h"] - FEE_RT,
                "i_net": None if i is None else i - FEE_RT,
                "asof_net": asof_net,
                "prior_book_date": prev_b,
            })
        flags_by_day[iso] = day_flags
    return rows, flags_by_day


def in_window(iso, lo, hi):
    if lo and iso < lo:
        return False
    if hi and iso > hi:
        return False
    return True


def materialize(joins, flags_by_day, panel_index, spy, mode, avoid=None,
                elev=None, n=TOP_N, lo=None, hi=None, special=None):
    trades = []
    day_picks = {}
    for iso, ranked in sorted(joins.items()):
        if iso not in flags_by_day:
            continue
        if not in_window(iso, lo, hi):
            continue
        fl = flags_by_day.get(iso) or {}
        ranked = [r for r in ranked if r["ticker"] in fl]
        if special == "avoid_J_ge0":
            picks = []
            for r in ranked:
                if not fl.get(r["ticker"], {}).get("J_lt0"):
                    continue
                picks.append(r)
                if len(picks) >= n:
                    break
        elif special == "avoid_incomplete_or_Jge1":
            picks = []
            for r in ranked:
                f = fl.get(r["ticker"]) or {}
                if f.get("incomplete") or f.get("J_ge1"):
                    continue
                picks.append(r)
                if len(picks) >= n:
                    break
        else:
            picks = pick_book(ranked, fl, mode, avoid, elev, n=n, cap=2)
        day_picks[iso] = []
        for r in picks:
            key = (iso, r["ticker"])
            tr = panel_index.get(key)
            if tr:
                trades.append(tr)
                day_picks[iso].append(tr)
    return trades, day_picks


def flatten_overlay(flat, hist, fz, start=HOLD_CUT):
    """Sleeve tickets with clock-clean J + same-day H.

    start exclusive (post-8-13) matches the published flatten n=30.
    Pass start=None to include the 08-13 io_core tickets (J is stale).
    """
    out = []
    for rec in flat:
        iso, t = rec["_date"], rec["_ticker"]
        if start is not None and iso <= start:
            continue
        fz_t = (fz.get(iso) or {}).get(t) or {}
        xl = excel_features(hist, t, iso, fz_t.get("open"))
        j = xl["J"] if xl.get("J_fresh") else None
        flags = {
            "J_fresh": bool(xl.get("J_fresh")),
            "J_ge1": j is not None and j >= 0.01,
            "J_ge0": j is not None and j >= 0,
            "J_lt0": j is not None and j < 0,
            "JB": xl["JB"] == 1,
            "FQ": xl["FQ"] == 1,
            "J_le-1": j is not None and j <= -0.01,
            "ER_m1": xl["ER"] == -1,
        }
        ret = rec["_ret"]
        net = None if ret is None else ret / 100.0  # already fee-native
        h_net = None if fz_t.get("h") is None else fz_t["h"] - FEE_RT
        out.append({
            "date": iso, "ticker": t, "flags": flags, "xl": xl,
            "net": net, "h_net": h_net, "pnl": rec["_pnl"],
            "sleeve": rec.get("sleeve"), "ret_pct": ret,
            "session": is_session(iso),
        })
    return out


def audit_j_clock(fz, hist, joins, flags_by_day):
    """Absolute time-leak re-audit of the J value used by avoid/elevate.

    PASS only if every scored J is (today Finviz Open − prior weekday Open)
    / prior Open, and H/I/Close/High/Low never enter that number.
    """
    reasons = []
    n_checked = 0
    n_mismatch = 0
    sample_ok = None
    for iso, fl in sorted(flags_by_day.items()):
        if iso not in joins or iso not in fz:
            continue
        for rec in joins[iso][:80]:
            t = rec["ticker"]
            if t not in fl or not fl[t].get("J_fresh"):
                continue
            fz_t = fz[iso].get(t) or {}
            today_open = fz_t.get("open")
            prior = prior_bars(hist, t, iso)
            if not today_open or not prior or not prior[-1][1].get("open"):
                continue
            recon = j_from_opens(today_open, prior[-1][1]["open"])
            xl = excel_features(hist, t, iso, today_open)
            n_checked += 1
            if xl["J"] is None or recon is None or abs(xl["J"] - recon) > 1e-12:
                n_mismatch += 1
            # Flat zeros (J=H=I=0) are coincidence, not a peek.
            if xl["J"] is not None and abs(xl["J"]) > 1e-12:
                if fz_t.get("h") is not None and abs(xl["J"] - fz_t["h"]) < 1e-12:
                    reasons.append(f"nonzero J equals same-row H for {t} {iso}")
                if fz_t.get("i") is not None and abs(xl["J"] - fz_t["i"]) < 1e-12:
                    reasons.append(f"nonzero J equals same-row I for {t} {iso}")
            if sample_ok is None:
                sample_ok = {
                    "ticker": t, "date": iso,
                    "today_open": today_open,
                    "prior_open_date": prior[-1][0],
                    "prior_open": prior[-1][1]["open"],
                    "J": xl["J"],
                }
    if n_checked < 50:
        reasons.append(f"too few J checks ({n_checked})")
    if n_mismatch:
        reasons.append(f"J reconstruct mismatch n={n_mismatch}")
    # Gate: J value is in the locked 44; H/I/M/core_score are not features.
    try:
        assert_feature_legal("value", "J", 0)
    except ValueError as e:
        reasons.append(f"J lag0 not legal: {e}")
    for col in ("H", "I", "M", "core_score"):
        try:
            assert_feature_legal("value", col, 0)
        except ValueError:
            pass
        else:
            reasons.append(f"same-row {col} was accepted as a feature")
    verdict = "PASS" if not reasons else "FAIL"
    return {
        "verdict": verdict,
        "n_checked": n_checked,
        "n_mismatch": n_mismatch,
        "reasons": reasons,
        "formula": "J = (Finviz Open[t] − Finviz Open[prior weekday]) / Open[prior weekday]",
        "excel_map": "CLOCK_MAP J = C[t] vs C[t−1] (value-open). C = Open / IT.",
        "inputs": "Finviz Open only (09:30 print). Prior bar skips Sat/Sun dumps.",
        "not_inputs": [
            "same-row H (Change from Open) — label only",
            "same-row I (Change) — label only",
            "High / Low / Close / Price",
            "M number / H paint / core_score",
        ],
        "file_clock": (
            "Finviz CSVs are EOD dumps; the Open column is still the 09:30 "
            "print (Excel C). Using it at the open is not a close peek."
        ),
        "holes": [
            "2026-08-26 has join but no Finviz — 08-27 J uses 08-25 Open (missing bar, not future).",
            "2026-08-13 J vs 2026-04-26 is stale and is not used.",
        ],
        "sample": sample_ok,
    }


def _case_leg(iso, ticker, joins, fz, hist, flags_by_day, role):
    ranked = joins.get(iso) or []
    rec = next((r for r in ranked if r["ticker"] == ticker), None)
    fz_t = (fz.get(iso) or {}).get(ticker) or {}
    prior = prior_bars(hist, ticker, iso)
    pdate = prior[-1][0] if prior else None
    po = prior[-1][1].get("open") if prior else None
    to = fz_t.get("open")
    xl = excel_features(hist, ticker, iso, to)
    h, i = fz_t.get("h"), fz_t.get("i")
    return {
        "role": role,
        "ticker": ticker,
        "date": iso,
        "rank": rec["rank"] if rec else None,
        "total_score": rec.get("total_score") if rec else None,
        "J": xl.get("J"),
        "J_fresh": xl.get("J_fresh"),
        "today_open": to,
        "prior_open_date": pdate,
        "prior_open": po,
        "h": h,
        "h_fee": None if h is None else h - FEE_RT,
        "i": i,
        "i_fee": None if i is None else i - FEE_RT,
        "in_top8": bool(rec and rec["rank"] <= TOP_N),
    }


def prove_case_studies(joins, fz, hist, flags_by_day):
    """One avoid + one elevate name-day from the prove window."""
    out = []
    for spec, without, with_ in (
        (CASE_AVOID,
         "join top-8 keeps FIGR (rank 3)",
         "avoid_J_ge0 drops FIGR (J≥0) and refills EMBJ (J<0, rank 9)"),
        (CASE_ELEV,
         "join top-8 keeps HRMY (rank 1)",
         "elev_cap2 swaps HRMY (J≥0) for AVAH (J≤−1%, rank 13)"),
    ):
        iso = spec["date"]
        dropped = _case_leg(iso, spec["dropped"], joins, fz, hist, flags_by_day,
                            "dropped")
        added = _case_leg(iso, spec["added"], joins, fz, hist, flags_by_day,
                          "added")
        out.append({
            **spec,
            "without_rule": without,
            "with_rule": with_,
            "dropped": dropped,
            "added": added,
        })
    return out


def _pct_s(x, n=None):
    if x is None:
        return "—"
    s = f"{x * 100:+.2f}%"
    if n is not None:
        s += f" (n={n})"
    return s


def _ghost_s(g):
    return f"{g['name']}/{g['month']}/{g['day']}"


def _pp_s(x):
    return "—" if x is None else f"{x:+.2f} pp"


def score_window(name, joins, flags_by_day, panel_index, spy, lo, hi, n=TOP_N):
    """Baseline + the two standing KEEP recipes on one window / book size."""
    raw, _ = materialize(joins, flags_by_day, panel_index, spy, "raw",
                         n=n, lo=lo, hi=hi)
    base = score_book(f"join_top{n}", "baseline", raw, None, spy,
                      f"fullscan-alone top-{n}")
    base["holdout_mean"] = mean([tr["net"] for tr in raw])
    avoid, _ = materialize(joins, flags_by_day, panel_index, spy, "raw",
                           n=n, lo=lo, hi=hi, special="avoid_J_ge0")
    elev, _ = materialize(joins, flags_by_day, panel_index, spy, "elev_cap",
                          "J_ge0", "J_le-1", n=n, lo=lo, hi=hi)
    a = score_book("avoid_J_ge0", "avoid", avoid, base, spy,
                   "Excel: drop J≥0, refill from J<0")
    e = score_book("elev_cap2_J_le-1", "elevate", elev, base, spy,
                   "Excel: swap ≤2 J≥0 in the book for J≤−1% from ranks n+1–80")
    from j_winrate import pack_rule_clock
    a["winrate"] = pack_rule_clock(raw, avoid)
    e["winrate"] = pack_rule_clock(raw, elev)
    return {
        "name": name, "lo": lo, "hi": hi, "n_book": n,
        "days": sorted({tr["date"] for tr in raw}),
        "baseline": base, "avoid_J_ge0": a, "elev_cap2_J_le-1": e,
    }


def flatten_keep_score(flat_rows):
    """KEEP recipes on flatten tickets: sleeve-native P&L vs same-day H."""
    if not flat_rows:
        return {}

    def pack(name, rows, vs_rows=None):
        sl = [r["net"] for r in rows if r.get("net") is not None]
        hh = [r["h_net"] for r in rows if r.get("h_net") is not None]
        sl_m, h_m = mean(sl), mean(hh)
        vs_sl = vs_h = None
        if vs_rows is not None:
            vsl = mean([r["net"] for r in vs_rows if r.get("net") is not None])
            vh = mean([r["h_net"] for r in vs_rows if r.get("h_net") is not None])
            if sl_m is not None and vsl is not None:
                vs_sl = (sl_m - vsl) * 100
            if h_m is not None and vh is not None:
                vs_h = (h_m - vh) * 100
        return {
            "name": name, "n": len(sl),
            "sleeve_mean": sl_m, "h_mean": h_m,
            "vs_sleeve_pp": vs_sl, "vs_h_pp": vs_h,
        }

    all_rows = flat_rows
    avoid = [r for r in all_rows if r["flags"].get("J_lt0")]
    # elev_cap2 analogue: cannot swap names that were not ticketed.
    # Drop up to 2 J≥0 tickets per day (prefer largest J).
    by_d = defaultdict(list)
    for r in all_rows:
        by_d[r["date"]].append(r)
    elev = []
    for _iso, rows in by_d.items():
        jpos = sorted([r for r in rows if r["flags"].get("J_ge0")],
                      key=lambda r: (r["xl"].get("J") or 0), reverse=True)
        jneg = [r for r in rows if not r["flags"].get("J_ge0")]
        elev.extend(jneg + jpos[2:])
    io = [r for r in all_rows if r.get("sleeve") == "io_core"]
    mv = [r for r in all_rows if r.get("sleeve") == "mover_long"]
    io_avoid = [r for r in io if r["flags"].get("J_lt0")]
    fair = io_avoid + mv
    stale_j = [r for r in all_rows if not r["flags"].get("J_fresh")]
    return {
        "n_all": len(all_rows),
        "cuts": [
            pack("all tickets", all_rows),
            pack("avoid_J_ge0 (keep J<0)", avoid, all_rows),
            pack("elev_cap2 analogue (drop ≤2 J≥0 / day)", elev, all_rows),
            pack("io_core only", io, all_rows),
            pack("mover_long only", mv, all_rows),
            pack("io_core avoid_J_ge0", io_avoid, io),
            pack("fair: J-avoid on io only, movers untouched", fair, all_rows),
        ],
        "n_stale_j": len(stale_j),
        "note": (
            "KEEP is a join top-8 1d H overlay. Flatten is io_core 3d + "
            "mover_long 1d sleeve-native P&L. Blanket J-avoid fights the "
            "mover thesis (buy gap-up). Fair test applies J-avoid to io only."
        ),
    }


def _win_row(label, rec):
    gh = rec["ghost"]
    vs = rec.get("vs_fullscan_pp")
    return (
        f"| {label} | {rec['n']} | {rec.get('n_dates', '—')} | "
        f"{_pct_s(rec['holdout_mean'])} | {_pp_s(vs)} | "
        f"{(rec['win'] or 0)*100:.1f}% | {_ghost_s(gh)} | "
        f"**{rec.get('verdict', '—')}** |"
    )


def render(payload):
    g = payload["gate"]
    recs = payload["recipes"]
    flat = payload["flatten"]
    L = [
        "# JOIN Excel open-gate × fullscan — prove (post-8-13 KEEP)",
        "",
        f"_Generated {payload['generated']} · live `flatten_robust` frozen · "
        "research only · no live push._",
        "",
        "## Plain English",
        "",
        payload["plain"],
        "",
        f"**Family verdict: {payload['family']}**",
        "",
        "Standing recipes (research only, not live): `avoid_J_ge0` and "
        "`elev_cap2_J_le-1` on morning join top-8. Open Excel **J only** "
        "(clock-clean prior-session Open). Live stays frozen.",
        "",
        "### J clock / leak re-audit",
        "",
    ]
    leak = payload.get("leak") or {}
    L += [
        f"**Clock verdict: {leak.get('verdict', '—')}** "
        f"({leak.get('n_checked', 0)} name-days reconstructed).",
        "",
        f"- Formula: `{leak.get('formula', '')}`",
        f"- Excel map: {leak.get('excel_map', '')}",
        f"- Inputs: {leak.get('inputs', '')}",
        "- Not inputs: " + "; ".join(leak.get("not_inputs") or []),
        f"- File clock: {leak.get('file_clock', '')}",
    ]
    for hole in leak.get("holes") or []:
        L.append(f"- Hole (not a future peek): {hole}")
    if leak.get("reasons"):
        L.append("- FAIL reasons: " + "; ".join(leak["reasons"]))
    L += [
        "",
        "Same-row H/I, M number, H paint, and `core_score` are not features. "
        "pick_book reads only J flags (`J_ge0` / `J_lt0` / `J_le-1`) plus join rank.",
        "",
        "### Case studies (prove window)",
        "",
    ]
    for cs in payload.get("cases") or []:
        d, a = cs["dropped"], cs["added"]
        L += [
            f"**{cs['recipe']}** — {cs['date']} `{d['ticker']}` → `{a['ticker']}`",
            "",
            f"- Without J: {cs['without_rule']}.",
            f"- With J: {cs['with_rule']}.",
            f"- `{d['ticker']}` at open: J {_pct_s(d['J'])} "
            f"(Open {d['today_open']} vs {d['prior_open_date']} Open {d['prior_open']}), "
            f"join rank {d['rank']}. After fees: H {_pct_s(d['h_fee'])} "
            f"(raw {_pct_s(d['h'])}), I {_pct_s(d['i_fee'])} (raw {_pct_s(d['i'])}).",
            f"- `{a['ticker']}` at open: J {_pct_s(a['J'])} "
            f"(Open {a['today_open']} vs {a['prior_open_date']} Open {a['prior_open']}), "
            f"join rank {a['rank']}. After fees: H {_pct_s(a['h_fee'])} "
            f"(raw {_pct_s(a['h'])}), I {_pct_s(a['i_fee'])} (raw {_pct_s(a['i'])}).",
            "",
        ]
    L += [
        "### Prove (time holdout, weekday sessions)",
        "",
        "Post-8-13 was discovery. Prove = later weekday sessions "
        f"({PROVE[0]} → {PROVE[1]}), J vs prior weekday Open. "
        "Sunday join dumps (2026-08-30, 2026-09-06) are **not** a 1d clock "
        "and are held out. 2026-08-13 J is stale (only prior Open is 2026-04-26) "
        "and is not used.",
        "",
        "| window | book | recipe | n | days | after-fee H | vs fullscan | win | ghost | bar |",
        "|---|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for wname, w in payload["windows"].items():
        for rec_name in ("baseline", "avoid_J_ge0", "elev_cap2_J_le-1"):
            rec = w[rec_name]
            vs = "—" if rec.get("vs_fullscan_pp") is None else _pp_s(rec["vs_fullscan_pp"])
            bar = rec.get("verdict", "—") if rec_name != "baseline" else "—"
            L.append(
                f"| {wname} | top-{w['n_book']} | `{rec['name']}` | {rec['n']} | "
                f"{rec['n_dates']} | {_pct_s(rec['holdout_mean'])} | {vs} | "
                f"{(rec['win'] or 0)*100:.1f}% | {_ghost_s(rec['ghost'])} | "
                f"**{bar}** |"
            )
    L += [
        "",
        "### Wider book (more name-days on the same dumps)",
        "",
        "Join ranked files on disk stop at 2026-08-12 / 2026-09-07. No older "
        "weekday join+Finviz pair exists (04-26 has Finviz+membership, no weather, "
        "no ranked file). Wider book = ranks 1–80 on the same sessions.",
        "",
        "| window | recipe | n | days | after-fee H | vs top-80 | win | ghost | bar |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for wname, w in payload["wide"].items():
        for rec_name in ("baseline", "avoid_J_ge0", "elev_cap2_J_le-1"):
            rec = w[rec_name]
            vs = "—" if rec.get("vs_fullscan_pp") is None else _pp_s(rec["vs_fullscan_pp"])
            bar = rec.get("verdict", "—") if rec_name != "baseline" else "—"
            L.append(
                f"| {wname} | `{rec['name']}` | {rec['n']} | {rec['n_dates']} | "
                f"{_pct_s(rec['holdout_mean'])} | {vs} | "
                f"{(rec['win'] or 0)*100:.1f}% | {_ghost_s(rec['ghost'])} | "
                f"**{bar}** |"
            )
    L += [
        "",
        "cap=2 on an 80-name book is a 2.5% swap — elev_cap2 is a top-8 recipe "
        "and is not expected to move the wide book.",
        "",
        "### Flatten / sleeve confirm (KEEP recipes, not J≥+1%)",
        "",
        payload.get("flatten_plain") or "",
        "",
    ]
    if not flat:
        L.append("No flatten tickets after 2026-08-13.")
    else:
        L.append(
            f"Live tickets after {HOLD_CUT}: **{flat['n_all']}**. "
            f"{flat.get('note', '')} Avoid/elevate here does not change live."
        )
        L.append("")
        L.append("| cut | n | sleeve P&L | vs sleeve | same-day H | vs H |")
        L.append("|---|---:|---:|---:|---:|---:|")
        for row in flat.get("cuts") or []:
            L.append(
                f"| {row['name']} | {row['n']} | {_pct_s(row.get('sleeve_mean'))} | "
                f"{_pp_s(row.get('vs_sleeve_pp'))} | {_pct_s(row.get('h_mean'))} | "
                f"{_pp_s(row.get('vs_h_pp'))} |"
            )
    L += [
        "",
        "### Universes beyond join top-8",
        "",
        "Same open J. Each universe beats **its own** fullscan-alone book "
        "(not join top-8). Yahoo OHLC (`data/prices/ohlc.parquet`) covers "
        "2024-03-04 → 2026-08-21. Join ranked files start 2026-08-12. "
        "Fresh Finviz J starts 2026-08-14.",
        "",
        "| universe | clock | holdout slice | avoid n | avoid vs | ghost | filter J≤−1 vs | family |",
        "|---|---|---|---:|---:|---|---:|---|",
    ]
    for uname, u in (payload.get("universes") or {}).items():
        sl = u.get("slices") or {}
        hold = sl.get("prove") or sl.get("pre813") or sl.get("long")
        if not hold:
            continue
        a = hold["avoid_J_ge0"]
        f = hold.get("filter_J_le-1") or {}
        hold_name = "prove" if "prove" in sl else ("pre813" if "pre813" in sl else "long")
        L.append(
            f"| `{uname}` | {u.get('clock','')} | {hold_name} | {a.get('n')} | "
            f"{_pp_s(a.get('vs_fullscan_pp'))} | {a.get('ghost') if isinstance(a.get('ghost'), str) else _ghost_s(a.get('ghost') or {})} | "
            f"{_pp_s(f.get('vs_fullscan_pp'))} | **{u.get('verdict')}** |"
        )
    L += [
        "",
        payload.get("universe_plain") or "",
        "",
        "### Dashboard sleeves (open J vs sleeve-alone)",
        "",
        "Same leak bar: open J only. Each sleeve is scored against **its own** "
        "morning picks after Futubull 15 bp — not against join top-8. "
        "List books (green / weighted / unweighted) refill from the ranked leftover. "
        "Ticket books (flatten fills, factor-mine BUYs, paper) filter the names "
        "they actually took; elev is drop ≤2 J≥0 / day. Shorts and excel/`strategies/` "
        "are null. Live stays frozen.",
        "",
        payload.get("sleeve_plain") or "",
        "",
    ]
    from j_sleeve_prove import render_winrate_md
    wr_md, _, _ = render_winrate_md(payload, payload.get("sleeves") or [])
    L += wr_md
    L += [
        "#### Featured",
        "",
    ]
    from j_sleeve_prove import render_sleeve_tables
    L += render_sleeve_tables(payload.get("sleeves") or [], featured_only=True)
    L += [
        "",
        "#### All dashboard / STRATEGY_BOARD sleeves",
        "",
    ]
    L += render_sleeve_tables(payload.get("sleeves") or [], featured_only=False)
    L += [
        "",
        "### What was joined",
        "",
        "**Excel (clock gate, open-only):**",
        "",
        "- Same-row numbers from the locked 44: **J** is the standing recipe "
        "(AH / ER / FQ / JB / JC still asserted legal, not in the KEEP pair). "
        "J = (today Open − prior *weekday session* Open) / prior Open.",
        "- Lags (any letter from rows above): **H[t−1], I[t−1], J[t−1], G[t−1]** "
        "from weekday bars only.",
        "- Fills A B C G J K L M O IR IS IT are legal but **not on these dumps**. "
        "Not invented.",
        "- OUT: same-row H/I, M number, B/G/K/O numbers, D/E/F, `core_score`, "
        "H paint.",
        "",
        "**Fullscan (standing dumps, open or earliest fair clock):**",
        "",
        "- `data/join/YYYY-MM-DD_ranked.csv` — morning rank. Sunday files "
        "(08-30, 09-06) exist but are held out of prove.",
        "- `data/stock_book/` **1d buy, prior date only**.",
        "- `data/feature_asof/` morning tags when present. `ret_*` labels only.",
        "- `data/sleeve_merge/trades.csv` — flatten_robust tickets (overlay).",
        "- `03_scoreboard/factor_mine/*.md` — 09:30 BUY fills (flatten_h5 / live / unions).",
        "- `data/stock_book/*_green.json` / `*_unweighted.json` / 1d buy — prior-day PIT.",
        "- `data/paper/roundtrips.csv`, `data/sleeve_combine/bt_trades.csv`, "
        "book/mover paper fills.",
        "",
        "### Labels",
        "",
        "- **Primary:** same-day Excel **H** = Finviz Change from Open, minus "
        f"{FEE_RT*100:.2f} pp Futubull. Join / stock-book 1d clock.",
        "- **Secondary:** flatten ticket `ret_pct` (already fee-native; io 3d / "
        "mover 1d). feature_asof `ret_1d` when present.",
        "",
        "### Window / dumps",
        "",
        f"- Discovery (in-sample peek): weekday sessions {DISCOVERY[0]} → {DISCOVERY[1]}.",
        f"- Prove (time holdout): weekday sessions {PROVE[0]} → {PROVE[1]} "
        "(08-26 has join, no Finviz H — skipped).",
        f"- Published 18-day tape included Sundays; session-clean pooled tape is "
        f"**{payload.get('n_session_days')}** weekdays "
        f"({payload.get('first_day')} → {payload.get('last_day')}).",
        f"- Panel name-days with H (weekday): **{payload['n_panel']}**.",
        f"- Finviz history dates: **{payload['n_fz_dates']}** "
        f"({payload['fz_first']} → {payload['fz_last']}); Sat/Sun dumps are "
        "not used as prior Open.",
        "- Recipes were pre-specified. They were not re-searched on prove.",
        "",
        "### Ship bar",
        "",
        f"Beat same-window fullscan-alone (join top-{TOP_N}) by ≥{int(BEAT_PP*100)} bp "
        "after Futubull 15 bp, ghost (name/month/day), leak-free. "
        "KEEP holds only if the **prove** window clears that bar. "
        "Pooled leftover that still includes discovery is not a holdout.",
        "",
        "### Discovery-pool recipes vs join top-8 (session-clean, not a holdout)",
        "",
        "| recipe | kind | H | vs fullscan | win | n | ghost | tapes | asof 1d | verdict | why |",
        "|---|---|---:|---:|---:|---:|---|---|---:|---|---|",
    ]
    for r in recs:
        vs = "—" if r["vs_fullscan_pp"] is None else f"{r['vs_fullscan_pp']:+.2f} pp"
        win = "—" if r["win"] is None else f"{r['win']*100:.1f}%"
        tp = r["tapes"]
        tape_s = "thin"
        if tp["enough"]:
            tape_s = (
                f"↑{_pct_s(tp['up']['mean'])}/↓{_pct_s(tp['dn']['mean'])}"
            )
        asof_s = _pct_s(r["asof_1d_mean"], r["asof_1d_n"]) if r["asof_1d_n"] else "—"
        L.append(
            f"| `{r['name']}` | {r['kind']} | {_pct_s(r['holdout_mean'])} | {vs} | "
            f"{win} | {r['n']} | {_ghost_s(r['ghost'])} | {tape_s} | "
            f"{asof_s} | **{r['verdict']}** | "
            f"{','.join(r['fail_reasons']) or '—'} |"
        )
    L += [
        "",
        "### What this does not do",
        "",
        "- Does not wire live `flatten_robust` / `LIVE_POLICY` / `join_rules.json`.",
        "- Does not use same-row H/I, H paint, or M’s number.",
        "- Does not treat afternoon stock-book prints as 09:30 features.",
        "- Does not score Sunday join dumps as a 1d session.",
        "- Does not claim CE/CD (need 43 sessions of High/Low).",
        "- Does not re-open shade hex / light+O (fills not in these dumps).",
        "",
        f"Gate: `{g['gate']}`. Fills open: {', '.join(g['fill_open'])}. "
        "Live frozen.",
        "",
        f"Tip `{payload.get('tip_sha') or 'local'}` · family **{payload['family']}**.",
        "",
        "Research only.",
        "",
    ]
    return "\n".join(L)


def scoreboard_line(payload):
    tip = payload.get("tip_sha") or "local"
    prove = payload["windows"]["prove"]
    a = prove["avoid_J_ge0"]
    e = prove["elev_cap2_J_le-1"]
    return (
        f"## Join Excel open-gate × fullscan (post-8-13)\n\n"
        f"_Generated {payload['generated']} · live `flatten_robust` frozen. "
        f"Family **{payload['family']}** (join top-8 only; DEMOTE as a "
        f"general rule). "
        f"Prove weekday top-8: `avoid_J_ge0` {_pp_s(a.get('vs_fullscan_pp'))} "
        f"n={a['n']} ghost {_ghost_s(a['ghost'])}; "
        f"`elev_cap2_J_le-1` {_pp_s(e.get('vs_fullscan_pp'))} "
        f"n={e['n']} ghost {_ghost_s(e['ghost'])}. "
        f"Sleeves: {(payload.get('sleeve_plain') or '')[:180]} "
        f"Fire bar: {(payload.get('plain') or '')[:160]} "
        f"Tip `{tip}`. See `excel_bot/research/JOIN_POST_813.md`._\n"
    )


def main():
    clocks = assert_excel_clock_gate()
    for col, lag, kind in EXCEL_ATOMS:
        assert_feature_legal(kind, col, lag)
    print("clock gate ok", flush=True)

    fz = load_finviz()
    hist = history_index(fz)
    joins = load_join_days()
    books = load_book_1d()
    asof = load_asof()
    spy = {}
    # Session-clean panel: weekdays after HOLD_CUT, J vs prior weekday Open.
    panel, flags_by_day = build_panel(joins, fz, hist, books, asof, spy,
                                      start=HOLD_CUT, sessions_only=True)
    panel_index = {(r["date"], r["ticker"]): r for r in panel}
    print(f"panel {len(panel)} session_days={len(flags_by_day)} fz={len(fz)}",
          flush=True)

    windows = {
        "discovery": score_window("discovery", joins, flags_by_day, panel_index,
                                  spy, DISCOVERY[0], DISCOVERY[1], TOP_N),
        "prove": score_window("prove", joins, flags_by_day, panel_index,
                              spy, PROVE[0], PROVE[1], TOP_N),
        "pooled_sessions": score_window("pooled_sessions", joins, flags_by_day,
                                        panel_index, spy, ORIG[0], ORIG[1], TOP_N),
    }
    wide = {
        "discovery": score_window("discovery80", joins, flags_by_day, panel_index,
                                  spy, DISCOVERY[0], DISCOVERY[1], WIDE_N),
        "prove": score_window("prove80", joins, flags_by_day, panel_index,
                              spy, PROVE[0], PROVE[1], WIDE_N),
        "pooled_sessions": score_window("pooled80", joins, flags_by_day,
                                        panel_index, spy, ORIG[0], ORIG[1], WIDE_N),
    }

    # Full recipe card on the session-clean pooled tape (not a holdout).
    baseline = windows["pooled_sessions"]["baseline"]
    recipes_spec = [
        ("avoid_J_ge1", "avoid", "avoid_refill", "J_ge1", None,
         "Excel: drop join buys with same-row J≥+1% (44), refill"),
        ("avoid_J_ge0", "avoid", "avoid_refill", None, None,
         "Excel: drop join buys with J≥0, refill from J<0"),
        ("avoid_JB", "avoid", "avoid_refill", "JB", None,
         "Excel: drop join buys with JB=1 (prior-8 |H|>3%), refill"),
        ("avoid_FQ", "avoid", "avoid_refill", "FQ", None,
         "Excel: drop join buys with FQ=1 (yesterday H>+3%), refill"),
        ("avoid_ER_p1", "avoid", "avoid_refill", "ER_p1", None,
         "Excel: drop join buys with ER=+1 (yesterday +5% H/I), refill"),
        ("avoid_incomplete", "control", "avoid_refill", "incomplete", None,
         "fullscan-only control: drop incomplete cards, refill"),
        ("avoid_incomplete_or_Jge1", "control", "avoid_refill",
         None, None, "control: incomplete OR J≥+1% — Excel must beat incomplete alone"),
        ("elev_cap2_J_le-1", "elevate", "elev_cap", "J_ge0", "J_le-1",
         "Excel: swap ≤2 J≥0 names in top-8 for J≤−1% from ranks 9–80"),
        ("elev_cap2_J_lt0", "elevate", "elev_cap", "J_ge0", "J_lt0",
         "Excel: swap ≤2 J≥0 names in top-8 for J<0 from ranks 9–80"),
        ("elev_cap2_ER_m1", "elevate", "elev_cap", None, "ER_m1",
         "Excel: swap ≤2 tail names for ER=−1 from ranks 9–80"),
        ("elev_cap2_AH_ge1", "elevate", "elev_cap", None, "AH_ge1",
         "Excel: swap ≤2 tail names for AH≥1 from ranks 9–80"),
        ("replace_J_lt0", "replace", "elev", None, "J_lt0",
         "not a join: replace the eight with J<0 from ranks 1–80"),
        ("top8_and_J_lt0", "intersect", "intersect", None, "J_lt0",
         "keep join top-8 only when J<0 (smaller book)"),
        ("top8_and_J_le-1", "intersect", "intersect", None, "J_le-1",
         "keep join top-8 only when J≤−1% (smaller book)"),
        ("top8_and_ER_m1", "intersect", "intersect", None, "ER_m1",
         "keep join top-8 only when ER=−1"),
    ]
    scored = []
    lo, hi = ORIG
    for name, kind, mode, avoid, elev, note in recipes_spec:
        special = name if name in ("avoid_J_ge0", "avoid_incomplete_or_Jge1") else None
        trades, _ = materialize(joins, flags_by_day, panel_index, spy,
                                mode, avoid, elev, n=TOP_N, lo=lo, hi=hi,
                                special=special)
        scored.append(score_book(name, kind, trades, baseline, spy, note))

    inc = next((r for r in scored if r["name"] == "avoid_incomplete"), None)
    mix = next((r for r in scored if r["name"] == "avoid_incomplete_or_Jge1"), None)
    if mix and inc and mix.get("holdout_mean") is not None and inc.get("holdout_mean") is not None:
        if mix["holdout_mean"] < inc["holdout_mean"] + 0.002:
            mix["verdict"] = "KILL"
            mix["fail_reasons"] = list(dict.fromkeys(
                (mix.get("fail_reasons") or []) + ["no_edge_vs_incomplete_control"]))

    flat_rows = flatten_overlay(load_flatten(), hist, fz, start=HOLD_CUT)
    flat_payload = flatten_keep_score(flat_rows)

    prove_a = windows["prove"]["avoid_J_ge0"]
    prove_e = windows["prove"]["elev_cap2_J_le-1"]
    prove_clears = (
        prove_a.get("verdict") == "KEEP" or prove_e.get("verdict") == "KEEP"
    )
    # Family: KEEP holds only if the time-holdout clears the ship bar.
    # Pooled leftover that still includes discovery is not a holdout.
    if prove_clears:
        family = "KEEP holds"
    else:
        family = "CONDITIONAL"

    pa, pe = prove_a, prove_e
    disc_a = windows["discovery"]["avoid_J_ge0"]
    pool_a = windows["pooled_sessions"]["avoid_J_ge0"]
    pool_e = windows["pooled_sessions"]["elev_cap2_J_le-1"]
    wide_p = wide["prove"]["avoid_J_ge0"]
    avoid_sl = next((c for c in (flat_payload.get("cuts") or [])
                     if c["name"].startswith("avoid_J_ge0")), None)
    fair_sl = next((c for c in (flat_payload.get("cuts") or [])
                    if c["name"].startswith("fair:")), None)
    flatten_plain = (
        "First flatten cut used J≥+1%, not the KEEP recipes. Correct recipes: "
        f"`avoid_J_ge0` on all post-8-13 tickets is n={avoid_sl['n'] if avoid_sl else 0}, "
        f"sleeve {_pct_s(avoid_sl['sleeve_mean'] if avoid_sl else None)} "
        f"({_pp_s(avoid_sl['vs_sleeve_pp'] if avoid_sl else None)} vs all) and "
        f"same-day H {_pct_s(avoid_sl['h_mean'] if avoid_sl else None)} "
        f"({_pp_s(avoid_sl['vs_h_pp'] if avoid_sl else None)}). "
        "Movers on 08-20/21 gapped up (J>0) and paid sleeve; blanket J-avoid "
        "removes those winners. Fair clock — J-avoid on io_core only, movers "
        f"untouched — is n={fair_sl['n'] if fair_sl else 0}, sleeve "
        f"{_pct_s(fair_sl['sleeve_mean'] if fair_sl else None)} "
        f"({_pp_s(fair_sl['vs_sleeve_pp'] if fair_sl else None)} vs all). "
        "H and sleeve disagree on names like CYPH (sleeve +25%, H −3%). "
        "Flatten does not confirm a blanket KEEP. Do not wire."
    )
    plain = (
        f"Prove (weekday {PROVE[0]}→{PROVE[1]}) does **not** re-clear the ship bar. "
        f"`avoid_J_ge0` n={pa['n']} H {_pct_s(pa['holdout_mean'])} "
        f"({_pp_s(pa.get('vs_fullscan_pp'))} vs same-window top-8, "
        f"ghost {_ghost_s(pa['ghost'])}). "
        f"`elev_cap2_J_le-1` n={pe['n']} H {_pct_s(pe['holdout_mean'])} "
        f"({_pp_s(pe.get('vs_fullscan_pp'))}, ghost {_ghost_s(pe['ghost'])}). "
        f"Discovery half still prints (`avoid_J_ge0` {_pp_s(disc_a.get('vs_fullscan_pp'))} "
        f"n={disc_a['n']}) — that is the peek, not prove. "
        f"Pooled weekday leftover is `avoid_J_ge0` {_pp_s(pool_a.get('vs_fullscan_pp'))} "
        f"n={pool_a['n']} / `elev_cap2_J_le-1` {_pp_s(pool_e.get('vs_fullscan_pp'))} "
        f"n={pool_e['n']} (includes discovery; not a holdout). "
        f"Wider book (top-80) prove `avoid_J_ge0` {_pp_s(wide_p.get('vs_fullscan_pp'))} "
        f"n={wide_p['n']}. Join dumps do not add sessions before 8-13 with a "
        "fresh J (08-13 prior Open is 04-26). "
        "Family is **CONDITIONAL**: not KEEP holds, not a full KILL of the "
        "discovery print. Live flatten_robust stays frozen. Do not wire."
    )

    leak = audit_j_clock(fz, hist, joins, flags_by_day)
    cases = prove_case_studies(joins, fz, hist, flags_by_day)
    from j_universe_prove import compact as _uni_compact, run as run_universes
    print("scoring universes …", flush=True)
    uni_raw = run_universes(panel=panel, flags_by_day=flags_by_day, joins=joins,
                            spy=spy, fz=fz, hist=hist)
    uni = _uni_compact(uni_raw)
    def _from_window(wname, rec_a, rec_e, clock="Finviz Open J"):
        def slim(r):
            g = r.get("ghost") or {}
            return {
                "name": r.get("name"), "n": r.get("n"), "n_dates": r.get("n_dates"),
                "holdout_mean": r.get("holdout_mean"),
                "vs_fullscan_pp": r.get("vs_fullscan_pp"),
                "ghost": f"{g.get('name')}/{g.get('month')}/{g.get('day')}",
                "verdict": r.get("verdict"),
            }
        return {
            "verdict": "CONDITIONAL" if wname == "join_top8" else (
                "DEMOTE" if (rec_a.get("verdict") != "KEEP") else "KEEP"
            ),
            "clock": clock,
            "note": wname,
            "slices": {
                "prove": {
                    "lo": PROVE[0], "hi": PROVE[1],
                    "baseline": slim(windows["prove"]["baseline"]),
                    "avoid_J_ge0": slim(windows["prove"]["avoid_J_ge0"]),
                    "filter_J_le-1": slim(windows["prove"]["elev_cap2_J_le-1"]),
                },
                "pooled": {
                    "lo": ORIG[0], "hi": ORIG[1],
                    "baseline": slim(windows["pooled_sessions"]["baseline"]),
                    "avoid_J_ge0": slim(windows["pooled_sessions"]["avoid_J_ge0"]),
                    "filter_J_le-1": slim(windows["pooled_sessions"]["elev_cap2_J_le-1"]),
                },
            },
        }
    uni.setdefault("universes", {})
    uni["universes"]["join_top8"] = _from_window(
        "join_top8", windows["prove"]["avoid_J_ge0"],
        windows["prove"]["elev_cap2_J_le-1"])
    uni["universes"]["join_top80"] = {
        "verdict": "DEMOTE",
        "clock": "Finviz Open J",
        "note": "morning join ranks 1–80",
        "slices": {
            "prove": {
                "lo": PROVE[0], "hi": PROVE[1],
                "baseline": {
                    "n": wide["prove"]["baseline"]["n"],
                    "holdout_mean": wide["prove"]["baseline"]["holdout_mean"],
                    "vs_fullscan_pp": None,
                    "ghost": _ghost_s(wide["prove"]["baseline"]["ghost"]),
                    "verdict": wide["prove"]["baseline"].get("verdict"),
                },
                "avoid_J_ge0": {
                    "n": wide["prove"]["avoid_J_ge0"]["n"],
                    "holdout_mean": wide["prove"]["avoid_J_ge0"]["holdout_mean"],
                    "vs_fullscan_pp": wide["prove"]["avoid_J_ge0"]["vs_fullscan_pp"],
                    "ghost": _ghost_s(wide["prove"]["avoid_J_ge0"]["ghost"]),
                    "verdict": wide["prove"]["avoid_J_ge0"]["verdict"],
                },
                "filter_J_le-1": {
                    "n": wide["prove"]["elev_cap2_J_le-1"]["n"],
                    "vs_fullscan_pp": wide["prove"]["elev_cap2_J_le-1"]["vs_fullscan_pp"],
                    "ghost": _ghost_s(wide["prove"]["elev_cap2_J_le-1"]["ghost"]),
                    "verdict": wide["prove"]["elev_cap2_J_le-1"]["verdict"],
                },
            }
        },
    }
    uni_plain = (
        "Expanded prove: the J overlay does **not** hold as a general rule. "
        "Yahoo liquid names (prior-session volume ≥ 1M, 2024–2026) are about "
        "flat (+2 to +5 bp, ghost month fail). The all-name Yahoo tape’s "
        "large mean is a microcap lottery (ghost FAIL). Join-full / membership "
        "on the Finviz window is +5 to +13 bp (under 20 bp). membership_liq "
        "is flat to negative. Join top-8 stays **CONDITIONAL** (discovery only)."
    )
    family = "CONDITIONAL"
    from j_sleeve_prove import compact as _sl_compact, run as run_sleeves
    print("scoring dashboard sleeves …", flush=True)
    sl_raw = run_sleeves(panel=panel, flags_by_day=flags_by_day, spy=spy,
                         fz=fz, hist=hist, panel_index=panel_index)
    sl = _sl_compact(sl_raw)
    sleeve_plain = sl_raw.get("plain") or ""
    from j_sleeve_prove import render_winrate_md as _wr_md
    _tmp = {
        "windows": windows,
    }
    _, wr_clears, wr_prints = _wr_md(_tmp, sl_raw.get("sleeves") or [])
    if wr_clears:
        win_plain = (
            "Cyrus fire bar (>55% of days the rule changes the book vs "
            "the same-day no-rule book): **CLEAR** — "
            + "; ".join(wr_clears) + "."
        )
    else:
        win_plain = (
            "Cyrus fire bar (>55% of days the rule changes the book vs "
            "the same-day no-rule book): **no CLEAR** "
            "(≥8 fires required). "
            + (("Thin prints: " + "; ".join(wr_prints) + ".") if wr_prints
               else "No circumstance printed >55%.")
        )
    plain = (
        f"J clock leak **{leak['verdict']}**. " + win_plain + " "
        + plain + " " + uni_plain + " " + sleeve_plain
    )

    fz_dates = sorted(fz)
    join_eval = sorted(flags_by_day)
    payload = {
        "generated": str(date.today()),
        "family": family,
        "plain": plain,
        "flatten_plain": flatten_plain,
        "gate": gate_payload(),
        "hold_cut": HOLD_CUT,
        "discovery": list(DISCOVERY),
        "prove": list(PROVE),
        "session_only": True,
        "j_prior": "prior weekday session Open",
        "leak": leak,
        "cases": cases,
        "universes": uni.get("universes"),
        "universe_plain": uni_plain,
        "sleeves": sl.get("sleeves"),
        "sleeve_plain": sleeve_plain,
        "sleeve_counts": sl.get("counts"),
        "ohlc": uni.get("ohlc"),
        "primary_label": "same-day H (Finviz Change from Open) − 15 bp Futubull",
        "secondary_label": "flatten ret_pct (io 3d / mover 1d); feature_asof ret_1d",
        "fullscan_features": [
            "join rank / total_score / families_known (weekday ranked.csv)",
            "prior-day stock_book 1d buy (PIT)",
            "feature_asof join_good / blue / ab_good (morning, when present)",
            "flatten_robust tickets (overlay only)",
        ],
        "excel_features": [
            "J (44, same-row open; prior weekday session Open; fresh ≤5d)",
            "AH, ER, FQ, JB, JC (legal, not in standing KEEP pair)",
            "H[t-1], I[t-1], J[t-1], G[t-1] (weekday lags)",
        ],
        "excel_not_joined": [
            "open fills A B C G J K L M O IR IS IT (no grid on disk)",
            "CE/CD (need 43d High/Low)",
            "M #95CA82 / O green / five-cell light",
            "Sunday join dumps 2026-08-30 / 2026-09-06 (not a 1d session)",
            "2026-08-13 J vs 2026-04-26 Open (stale)",
        ],
        "n_panel": len(panel),
        "n_join_days": len(join_eval),
        "n_session_days": len(join_eval),
        "first_day": join_eval[0] if join_eval else None,
        "last_day": join_eval[-1] if join_eval else None,
        "n_fz_dates": len(fz_dates),
        "fz_first": fz_dates[0] if fz_dates else None,
        "fz_last": fz_dates[-1] if fz_dates else None,
        "windows": windows,
        "wide": wide,
        "baseline": baseline,
        "recipes": scored,
        "flatten": flat_payload,
        "clocks_groups": {
            "value_mine_open": clocks["groups"]["value_mine_open"],
            "fill_mine_open": clocks["groups"]["fill_mine_open"],
        },
        "live_untouched": "flatten_robust",
        "tip_sha": os.popen("git rev-parse --short HEAD").read().strip(),
    }

    os.makedirs(RESEARCH, exist_ok=True)
    os.makedirs(SCOREBOARD, exist_ok=True)
    md = render(payload)
    open(os.path.join(RESEARCH, "JOIN_POST_813.md"), "w", encoding="utf-8").write(md)
    json.dump(payload, open(os.path.join(RESEARCH, "join_post_813.json"), "w"),
              indent=2, default=str)
    json.dump(uni, open(os.path.join(RESEARCH, "j_universe_prove.json"), "w"),
              indent=2, default=str)
    json.dump(sl, open(os.path.join(RESEARCH, "j_sleeve_prove.json"), "w"),
              indent=2, default=str)
    from j_sleeve_prove import write_keep_cards
    write_keep_cards(payload, sl)
    sb = scoreboard_line(payload)
    open(os.path.join(SCOREBOARD, "JOIN_POST_813.md"), "w", encoding="utf-8").write(sb)
    excel_sb = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
    if os.path.isfile(excel_sb):
        old = open(excel_sb, encoding="utf-8").read()
        marker = "## Join Excel open-gate × fullscan (post-8-13)"
        if marker in old:
            old = old.split(marker, 1)[0].rstrip() + "\n\n" + sb
        else:
            old = old.rstrip() + "\n\n" + sb
        open(excel_sb, "w", encoding="utf-8").write(old)
    else:
        open(excel_sb, "w", encoding="utf-8").write(
            "# Excel bot mine scoreboard\n\nResearch only. Live `flatten_robust` frozen.\n\n" + sb
        )
    print(f"family={family} leak={leak['verdict']} n_j={leak['n_checked']}",
          flush=True)
    print(f"family={family} pooled_n={baseline['n']} session_days={len(join_eval)}",
          flush=True)
    print(
        f"prove avoid vs={prove_a.get('vs_fullscan_pp')} "
        f"elev vs={prove_e.get('vs_fullscan_pp')} "
        f"ghost { _ghost_s(prove_a['ghost']) } / { _ghost_s(prove_e['ghost']) }",
        flush=True,
    )
    for wname, w in windows.items():
        print(f"  [{wname} top-8]", flush=True)
        for key in ("baseline", "avoid_J_ge0", "elev_cap2_J_le-1"):
            r = w[key]
            print(f"    {r.get('verdict','—'):12} {r['name']:22} n={r['n']:4} "
                  f"{_pct_s(r['holdout_mean'])} vs={r.get('vs_fullscan_pp')}",
                  flush=True)
    print("dashboard sleeves:", payload.get("sleeve_plain"), flush=True)
    for s in (payload.get("sleeves") or []):
        if s.get("name") in (
            "flatten_robust", "flatten_h5", "flatten_live_h5",
            "green_pile_prior", "green_book_prior",
            "weighted_book_1d_prior", "unweighted_book_prior",
        ) or s.get("verdict") == "KEEP":
            print(f"  {s.get('verdict','?'):12} {s.get('name')}", flush=True)


if __name__ == "__main__":
    main()
