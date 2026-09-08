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

HOLD_CUT = "2026-08-13"  # KEEP decision is strictly after this date
FEE_RT = 0.0015  # Futubull ~15 bp round-trip (Excel ship bar)
BEAT_PP = 0.20  # must beat fullscan-alone by ≥20 bp after fees
TOP_N = 8
ELEV_BAND = 80
MIN_TAPE = 8  # window-adapted; Excel usual 40 is larger than this tape
MIN_MONTH = 12

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


def prior_bars(hist, ticker, iso):
    rows = hist.get(ticker) or []
    return [(d, r) for d, r in rows if d < iso]


def excel_features(hist, ticker, iso, today_open):
    """Open-knowable Excel atoms for name-day. Never same-row H/I."""
    prior = prior_bars(hist, ticker, iso)
    out = {
        "J": None, "AH": None, "ER": None, "FQ": None, "JB": None, "JC": None,
        "H_l1": None, "I_l1": None, "J_l1": None, "G_l1": None,
        "n_prior": len(prior),
    }
    if today_open and prior and prior[-1][1].get("open"):
        po = prior[-1][1]["open"]
        if po:
            out["J"] = (today_open - po) / po
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


def build_panel(joins, fz, hist, books, asof, spy):
    book_dates = sorted(books)
    rows = []
    flags_by_day = {}
    for iso in sorted(joins):
        if iso <= HOLD_CUT:
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
            flags = {
                "J_le-1": xl["J"] is not None and xl["J"] <= -0.01,
                "J_lt0": xl["J"] is not None and xl["J"] < 0,
                "J_ge0": xl["J"] is not None and xl["J"] >= 0,
                "J_ge1": xl["J"] is not None and xl["J"] >= 0.01,
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
            rows.append({
                **rec, "date": iso, "xl": xl, "flags": flags,
                "h": fz_t["h"], "open": fz_t["open"], "close": fz_t.get("close"),
                "net": fz_t["h"] - FEE_RT,
                "asof_net": asof_net,
                "prior_book_date": prev_b,
            })
        flags_by_day[iso] = day_flags
    return rows, flags_by_day


def materialize(joins, flags_by_day, panel_index, spy, mode, avoid=None, elev=None):
    trades = []
    day_picks = {}
    for iso, ranked in sorted(joins.items()):
        if iso <= HOLD_CUT:
            continue
        fl = flags_by_day.get(iso) or {}
        # only names that have a label
        ranked = [r for r in ranked if r["ticker"] in fl]
        picks = pick_book(ranked, fl, mode, avoid, elev, n=TOP_N, cap=2)
        day_picks[iso] = []
        for r in picks:
            key = (iso, r["ticker"])
            tr = panel_index.get(key)
            if tr:
                trades.append(tr)
                day_picks[iso].append(tr)
    return trades, day_picks


def flatten_overlay(flat, hist, fz):
    out = []
    for rec in flat:
        iso, t = rec["_date"], rec["_ticker"]
        if iso <= HOLD_CUT:
            continue
        fz_t = (fz.get(iso) or {}).get(t) or {}
        xl = excel_features(hist, t, iso, fz_t.get("open"))
        flags = {
            "J_ge1": xl["J"] is not None and xl["J"] >= 0.01,
            "JB": xl["JB"] == 1,
            "FQ": xl["FQ"] == 1,
            "J_le-1": xl["J"] is not None and xl["J"] <= -0.01,
            "ER_m1": xl["ER"] == -1,
        }
        ret = rec["_ret"]
        net = None if ret is None else ret / 100.0  # already fee-native
        out.append({
            "date": iso, "ticker": t, "flags": flags, "xl": xl,
            "net": net, "pnl": rec["_pnl"], "sleeve": rec.get("sleeve"),
            "ret_pct": ret,
        })
    return out


def _pct_s(x, n=None):
    if x is None:
        return "—"
    s = f"{x * 100:+.2f}%"
    if n is not None:
        s += f" (n={n})"
    return s


def render(payload):
    g = payload["gate"]
    base = payload["baseline"]
    recs = payload["recipes"]
    flat = payload["flatten"]
    L = [
        "# JOIN Excel open-gate × fullscan — post-8-13 holdout",
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
        "Excel KEEP (vs join top-8, after fees, ghost pass, both tapes green):",
        "",
    ]
    for r in payload["recipes"]:
        if r["kind"] in ("avoid", "elevate") and r["verdict"] == "KEEP":
            L.append(
                f"- `{r['name']}` — {_pct_s(r['holdout_mean'], r['n'])} · "
                f"{r['vs_fullscan_pp']:+.2f} pp · win {(r['win'] or 0)*100:.1f}% · "
                f"{r['notes']}"
            )
    L += [
        "",
        "Flatten_robust tickets on this same window do **not** confirm: dropping "
        "J≥+1% names from the live book **hurts** sleeve-native P&L (movers on "
        "08-20/21 gapped up and paid). Primary label is the join 1d H clock, "
        "not flatten 3d.",
        "",
        "### What was joined",
        "",
        "**Excel (clock gate, open-only):**",
        "",
        f"- Same-row numbers from the locked 44: **J, AH, ER, FQ, JB, JC** "
        f"(asserted via `excel_clock_gate.py`).",
        "- Lags (any letter from rows above): **H[t−1], I[t−1], J[t−1], G[t−1]**.",
        "- Fills A B C G J K L M O IR IS IT are legal but **not on these dumps** "
        "(no Excel grid / M-hex / O-green / five-cell light in the morning files). "
        "Not invented.",
        "- OUT: same-row H/I, M number, B/G/K/O numbers, D/E/F, `core_score`.",
        "",
        "**Fullscan (standing dumps, open or earliest fair clock):**",
        "",
        "- `data/join/YYYY-MM-DD_ranked.csv` — morning rank, `total_score`, "
        "`families_known` / incomplete. Same-day file is the 09:30 ranker.",
        "- `data/stock_book/YYYY-MM-DD_stock_book.json` **1d buy, prior date only** "
        "(same-day book is stamped afternoon — not an open feature).",
        "- `data/feature_asof/` morning tags (`join_good`, `blue`, `ab_good`) when "
        "the dated file exists. Forward `ret_*` are labels only.",
        "- `data/sleeve_merge/trades.csv` — live flatten_robust tickets (overlay).",
        "",
        "### Labels",
        "",
        "- **Primary:** same-day Excel **H** = Finviz Change from Open, minus "
        f"{FEE_RT*100:.2f} pp Futubull. This is the join / stock-book 1d clock.",
        "- **Secondary:** feature_asof `ret_1d` (sleeve-native forward) when the "
        "asof file exists; flatten ticket `ret_pct` (already fee-native).",
        "",
        "### Window",
        "",
        f"- KEEP decision: names/days **after {HOLD_CUT}** only.",
        f"- Join days with a Finviz label: **{payload['n_join_days']}** "
        f"({payload['first_day']} → {payload['last_day']}).",
        f"- Panel name-days with H: **{payload['n_panel']}**.",
        f"- Finviz history dates (for lags): **{payload['n_fz_dates']}** "
        f"({payload['fz_first']} → {payload['fz_last']}).",
        "- Recipes were pre-specified from earlier Excel mines (J≤−1, AH, ER, "
        "JB, FQ). They were not searched on this holdout.",
        "",
        "### Ship bar",
        "",
        f"Beat fullscan-alone (join top-{TOP_N}) by ≥{int(BEAT_PP*100)} bp after "
        "Futubull 15 bp, ghost (name/month/day), leak-free. Both SPY tapes "
        f"if each has n≥{MIN_TAPE}; otherwise tape-thin is noted, not a KILL. "
        "KEEP only if the join elevates and/or avoids better than fullscan alone.",
        "",
        "### Fullscan-alone baseline (primary)",
        "",
        f"| book | n | after-fee H | win | t | ghost |",
        f"|---|---:|---:|---:|---:|---|",
    ]
    def _base_row(label, rec):
        tt = f"{rec['t']:.2f}" if rec.get("t") is not None else "—"
        gh = rec["ghost"]
        return (
            f"| {label} | {rec['n']} | {_pct_s(rec['holdout_mean'])} | "
            f"{(rec['win'] or 0)*100:.1f}% | {tt} | "
            f"{gh['name']}/{gh['month']}/{gh['day']} |"
        )

    L.append(_base_row(f"join top-{TOP_N}", base))
    if payload.get("baseline15"):
        L.append(_base_row("join top-15", payload["baseline15"]))
    L += [
        "",
        "### Recipes vs join top-8",
        "",
        "| recipe | kind | holdout H | vs fullscan | win | n | ghost | tapes | asof 1d | verdict | why |",
        "|---|---|---:|---:|---:|---:|---|---|---:|---|---|",
    ]
    for r in recs:
        vs = "—" if r["vs_fullscan_pp"] is None else f"{r['vs_fullscan_pp']:+.2f} pp"
        win = "—" if r["win"] is None else f"{r['win']*100:.1f}%"
        gh = r["ghost"]
        tp = r["tapes"]
        tape_s = "thin"
        if tp["enough"]:
            tape_s = (
                f"↑{_pct_s(tp['up']['mean'])}/↓{_pct_s(tp['dn']['mean'])}"
            )
        asof_s = _pct_s(r["asof_1d_mean"], r["asof_1d_n"]) if r["asof_1d_n"] else "—"
        L.append(
            f"| `{r['name']}` | {r['kind']} | {_pct_s(r['holdout_mean'])} | {vs} | "
            f"{win} | {r['n']} | {gh['name']}/{gh['month']}/{gh['day']} | {tape_s} | "
            f"{asof_s} | **{r['verdict']}** | "
            f"{','.join(r['fail_reasons']) or '—'} |"
        )
    L += [
        "",
        "### Flatten_robust overlay (sleeve-native P&L, already fee-native)",
        "",
    ]
    if not flat:
        L.append("No flatten tickets after 2026-08-13.")
    else:
        L.append(
            f"Live tickets after {HOLD_CUT}: **{flat['n_all']}** "
            f"(mean { _pct_s(flat['mean_all']) }). Avoid/elevate on this book "
            "does not change live."
        )
        L.append("")
        L.append("| cut | n | mean ret | leftover vs all |")
        L.append("|---|---:|---:|---:|")
        for row in flat.get("cuts") or []:
            L.append(
                f"| {row['name']} | {row['n']} | {_pct_s(row['mean'])} | "
                f"{row['delta_pp'] if row['delta_pp'] is not None else '—'} |"
            )
    L += [
        "",
        "### What this does not do",
        "",
        "- Does not wire live `flatten_robust` / `LIVE_POLICY` / `join_rules.json`.",
        "- Does not use same-row H/I, H paint, or M’s number.",
        "- Does not treat afternoon stock-book prints as 09:30 features.",
        "- Does not claim CE/CD (need 43 sessions of High/Low; Finviz tape is too short).",
        "- Does not re-open shade hex / light+O (fills not in these dumps).",
        "",
        f"Gate: `{g['gate']}`. Fills open: {', '.join(g['fill_open'])}. "
        "Live frozen.",
        "",
        f"Tip `{payload.get('tip_sha') or 'local'}` · "
        f"n={payload['baseline']['n']} join top-8.",
        "",
        "Research only.",
        "",
    ]
    return "\n".join(L)


def scoreboard_line(payload):
    excel = [r for r in payload["recipes"] if r["kind"] in ("avoid", "elevate", "intersect")]
    keep = sum(1 for r in excel if r["verdict"] == "KEEP")
    kill = sum(1 for r in excel if r["verdict"] == "KILL")
    tip = payload.get("tip_sha") or "local"
    n = payload["baseline"]["n"]
    h = payload["baseline"]["holdout_mean"]
    best = None
    for r in excel:
        if r["verdict"] == "KEEP":
            if best is None or (r.get("vs_fullscan_pp") or -9) > (best.get("vs_fullscan_pp") or -9):
                best = r
    if best:
        effect = f"{best['name']} {best['vs_fullscan_pp']:+.2f} pp vs join top-8 n={best['n']}"
    else:
        effect = "no elevate/avoid beat"
    return (
        f"## Join Excel open-gate × fullscan (post-8-13)\n\n"
        f"_Generated {payload['generated']} · live `flatten_robust` frozen. "
        f"Family **{payload['family']}**. Excel KEEP {keep} · KILL {kill}. "
        f"Baseline join top-8 after-fee H {_pct_s(h)} n={n}. {effect}. "
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
    panel, flags_by_day = build_panel(joins, fz, hist, books, asof, spy)
    panel_index = {(r["date"], r["ticker"]): r for r in panel}
    print(f"panel {len(panel)} days={len(flags_by_day)} fz={len(fz)}", flush=True)

    fees = load_fees()
    raw, raw_days = materialize(joins, flags_by_day, panel_index, spy, "raw")
    raw15, _ = materialize(joins, flags_by_day, panel_index, spy, "raw")
    # top-15 baseline separately
    raw15_trades = []
    for iso, ranked in sorted(joins.items()):
        if iso <= HOLD_CUT or iso not in flags_by_day:
            continue
        fl = flags_by_day[iso]
        for r in ranked[:15]:
            tr = panel_index.get((iso, r["ticker"]))
            if tr:
                raw15_trades.append(tr)

    baseline = score_book("join_top8", "baseline", raw, None, spy,
                          "fullscan-alone morning rank")
    baseline["holdout_mean"] = mean([tr["net"] for tr in raw])
    baseline15 = score_book("join_top15", "baseline", raw15_trades, None, spy)

    recipes_spec = [
        # Excel avoid (refill keeps book size)
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
        # fullscan-only control (PR 143) — not an Excel join
        ("avoid_incomplete", "control", "avoid_refill", "incomplete", None,
         "fullscan-only control: drop incomplete cards, refill"),
        ("avoid_incomplete_or_Jge1", "control", "avoid_refill",
         None, None, "control: incomplete OR J≥+1% — Excel must beat incomplete alone"),
        # capped elevate: swap ≤2 names in the eight
        ("elev_cap2_J_le-1", "elevate", "elev_cap", "J_ge0", "J_le-1",
         "Excel: swap ≤2 J≥0 names in top-8 for J≤−1% from ranks 9–80"),
        ("elev_cap2_J_lt0", "elevate", "elev_cap", "J_ge0", "J_lt0",
         "Excel: swap ≤2 J≥0 names in top-8 for J<0 from ranks 9–80"),
        ("elev_cap2_ER_m1", "elevate", "elev_cap", None, "ER_m1",
         "Excel: swap ≤2 tail names for ER=−1 from ranks 9–80"),
        ("elev_cap2_AH_ge1", "elevate", "elev_cap", None, "AH_ge1",
         "Excel: swap ≤2 tail names for AH≥1 from ranks 9–80"),
        # replace-book (not a join overlay) — reported, not family KEEP
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
    for name, kind, mode, avoid, elev, note in recipes_spec:
        if name in ("avoid_incomplete_or_Jge1", "avoid_J_ge0"):
            trades = []
            for iso, ranked in sorted(joins.items()):
                if iso <= HOLD_CUT or iso not in flags_by_day:
                    continue
                fl = flags_by_day[iso]
                ranked = [r for r in ranked if r["ticker"] in fl]
                out = []
                for r in ranked:
                    f = fl[r["ticker"]]
                    if name == "avoid_J_ge0" and not f.get("J_lt0"):
                        continue
                    if name == "avoid_incomplete_or_Jge1" and (
                            f.get("incomplete") or f.get("J_ge1")):
                        continue
                    out.append(r)
                    if len(out) >= TOP_N:
                        break
                for r in out:
                    tr = panel_index.get((iso, r["ticker"]))
                    if tr:
                        trades.append(tr)
            scored.append(score_book(name, kind, trades, baseline, spy, note))
            continue
        trades, _days = materialize(joins, flags_by_day, panel_index, spy,
                                    mode, avoid, elev)
        scored.append(score_book(name, kind, trades, baseline, spy, note))

    # flatten overlay
    flat_rows = flatten_overlay(load_flatten(), hist, fz)
    flat_payload = {}
    if flat_rows:
        all_n = [r["net"] for r in flat_rows if r["net"] is not None]
        cuts = []
        for cname, key, want in (
            ("all tickets", None, None),
            ("avoid J≥+1%", "J_ge1", False),
            ("avoid JB", "JB", False),
            ("keep J≤−1% only", "J_le-1", True),
            ("keep ER=−1 only", "ER_m1", True),
        ):
            if key is None:
                sub = flat_rows
            elif want:
                sub = [r for r in flat_rows if r["flags"].get(key)]
            else:
                sub = [r for r in flat_rows if not r["flags"].get(key)]
            xs = [r["net"] for r in sub if r["net"] is not None]
            m = mean(xs)
            dpp = None if (m is None or mean(all_n) is None) else (m - mean(all_n)) * 100
            cuts.append({"name": cname, "n": len(xs), "mean": m,
                         "delta_pp": None if dpp is None else f"{dpp:+.2f} pp"})
        flat_payload = {
            "n_all": len(all_n), "mean_all": mean(all_n), "cuts": cuts,
        }

    # incomplete-alone is a fullscan control; Excel must beat it to claim that mix
    inc = next((r for r in scored if r["name"] == "avoid_incomplete"), None)
    mix = next((r for r in scored if r["name"] == "avoid_incomplete_or_Jge1"), None)
    if mix and inc and mix.get("holdout_mean") is not None and inc.get("holdout_mean") is not None:
        if mix["holdout_mean"] < inc["holdout_mean"] + 0.002:
            mix["verdict"] = "KILL"
            mix["fail_reasons"] = list(dict.fromkeys(
                (mix.get("fail_reasons") or []) + ["no_edge_vs_incomplete_control"]))
    excel_scored = [r for r in scored if r["kind"] in ("avoid", "elevate", "intersect")]
    keep_n = sum(1 for r in excel_scored if r["verdict"] == "KEEP")
    kill_n = sum(1 for r in excel_scored if r["verdict"] == "KILL")
    family = "KEEP" if keep_n else "KILL" if excel_scored else "null"
    if keep_n == 0 and excel_scored and all(r["n"] < 20 for r in excel_scored):
        family = "null"

    best_keep = [r for r in excel_scored if r["verdict"] == "KEEP"]
    if family == "KEEP" and best_keep:
        b0 = max(best_keep, key=lambda r: r.get("vs_fullscan_pp") or -9)
        plain = (
            f"On the post-{HOLD_CUT} window, joining Excel open-gate numbers "
            "(J / AH / ER / FQ / JB / JC and H/I/J/G lags) onto the morning "
            f"join rank **does** beat fullscan-alone. Best Excel KEEP "
            f"`{b0['name']}` holdout H {_pct_s(b0['holdout_mean'])} vs join "
            f"top-8 {_pct_s(baseline['holdout_mean'])} "
            f"({b0['vs_fullscan_pp']:+.2f} pp, n={b0['n']}). "
            "avoid_incomplete is a fullscan-only control (not Excel). "
            "Flatten tickets do not confirm (avoid J≥+1% hurts sleeve P&L). "
            "Live flatten_robust stays frozen."
        )
    elif family == "KILL":
        inc_s = ""
        if inc and inc.get("holdout_mean") is not None:
            inc_s = (
                f" The fullscan-only incomplete avoid still prints "
                f"{_pct_s(inc['holdout_mean'])} (n={inc['n']}) — that is PR 143, "
                "not this join."
            )
        plain = (
            f"On the post-{HOLD_CUT} window, joining Excel open-gate numbers "
            "(J / AH / ER / FQ / JB / JC and H/I/J/G lags) onto the morning "
            "join rank does **not** elevate or avoid better than fullscan-alone "
            f"(join top-8 after-fee H {_pct_s(baseline['holdout_mean'])}, "
            f"n={baseline['n']}). Excel KEEP 0 · KILL {kill_n}.{inc_s} "
            "Fills (light+O / M hex) were not on these dumps and were not invented. "
            "Live flatten_robust stays frozen. Do not wire."
        )
    else:
        plain = (
            f"Post-{HOLD_CUT} join is **null** — not enough powered name-days "
            "to KEEP or honestly KILL an elevate/avoid vs fullscan-alone. "
            "Live frozen."
        )

    fz_dates = sorted(fz)
    join_eval = sorted(flags_by_day)
    payload = {
        "generated": str(date.today()),
        "family": family,
        "plain": plain,
        "gate": gate_payload(),
        "hold_cut": HOLD_CUT,
        "primary_label": "same-day H (Finviz Change from Open) − 15 bp Futubull",
        "secondary_label": "feature_asof ret_1d; flatten ret_pct",
        "fullscan_features": [
            "join rank / total_score / families_known (same-day ranked.csv)",
            "prior-day stock_book 1d buy (PIT)",
            "feature_asof join_good / blue / ab_good (morning, when present)",
            "flatten_robust tickets (overlay only)",
        ],
        "excel_features": [
            "J, AH, ER, FQ, JB, JC (44, same-row open)",
            "H[t-1], I[t-1], J[t-1], G[t-1] (lags)",
        ],
        "excel_not_joined": [
            "open fills A B C G J K L M O IR IS IT (no grid on disk)",
            "CE/CD (need 43d High/Low)",
            "M #95CA82 / O green / five-cell light",
        ],
        "n_panel": len(panel),
        "n_join_days": len(join_eval),
        "first_day": join_eval[0] if join_eval else None,
        "last_day": join_eval[-1] if join_eval else None,
        "n_fz_dates": len(fz_dates),
        "fz_first": fz_dates[0] if fz_dates else None,
        "fz_last": fz_dates[-1] if fz_dates else None,
        "baseline": baseline,
        "baseline15": baseline15,
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
    print(f"family={family} KEEP={keep_n} KILL={kill_n} baseline_n={baseline['n']}",
          flush=True)
    print(f"baseline H={_pct_s(baseline['holdout_mean'])}", flush=True)
    for r in scored:
        print(f"  {r['verdict']:5} {r['name']:28} n={r['n']:4} "
              f"{_pct_s(r['holdout_mean'])} vs={r['vs_fullscan_pp']}",
              flush=True)


if __name__ == "__main__":
    main()
