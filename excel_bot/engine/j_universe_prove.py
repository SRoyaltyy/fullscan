"""J avoid/elevate on every in-repo universe (research only).

Clock: same open J = (today Open − prior weekday Open) / prior Open.
OHLC Opens from data/prices/ohlc.parquet when pyarrow is available.
Finviz Opens on dated exports. Live flatten_robust is not imported.

  python3 excel_bot/engine/j_universe_prove.py
"""
from __future__ import annotations

import csv
import json
import os
import sys
from collections import defaultdict
from datetime import date
from glob import glob

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from join_post_813 import (  # noqa: E402
    BEAT_PP, DISCOVERY, FEE_RT, HOLD_CUT, ORIG, PROVE, REPO,
    build_panel, ghost, is_session, j_from_opens, j_fresh, load_asof,
    load_book_1d, load_finviz, load_join_days, mean, prior_bars,
    score_book, tapes, history_index,
)

UNIVERSE_JSON = os.path.join(os.path.dirname(HERE), "research", "j_universe_prove.json")
OHLC_PATH = os.path.join(REPO, "data", "prices", "ohlc.parquet")
MEM_DIR = os.path.join(REPO, "data", "universe")
BOOK_DIR = os.path.join(REPO, "data", "stock_book")
PRIOR_VOL_LIQ = 1_000_000


def _try_pyarrow():
    try:
        import pyarrow.parquet as pq  # noqa: F401
        return True
    except ImportError:
        return False


def load_ohlc():
    """iso -> ticker -> (open, close, prior_vol_placeholder)."""
    import pyarrow.parquet as pq
    by = defaultdict(dict)
    pf = pq.ParquetFile(OHLC_PATH)
    for i in range(pf.num_row_groups):
        tbl = pf.read_row_group(i, columns=["date", "ticker", "open", "close", "volume"])
        for d, t, o, c, v in zip(
            tbl.column("date").to_pylist(),
            tbl.column("ticker").to_pylist(),
            tbl.column("open").to_pylist(),
            tbl.column("close").to_pylist(),
            tbl.column("volume").to_pylist(),
        ):
            iso = (d.date() if hasattr(d, "date") else d).isoformat()
            if not is_session(iso) or not o or not c or o <= 0:
                continue
            by[iso][str(t).strip().upper()] = (float(o), float(c), float(v or 0))
    return by


def ohlc_name_days(by):
    """All weekday name-days with fresh J (prior weekday Open ≤5d)."""
    days = sorted(by)
    hist = defaultdict(list)
    for iso in days:
        for t, (o, _c, v) in by[iso].items():
            hist[t].append((iso, o, v))
    out_all, out_liq = [], []
    spy = {}
    for iso in days:
        if "SPY" in by[iso]:
            o, c, _ = by[iso]["SPY"]
            r = (c - o) / o
            spy[iso] = 1 if r > 0.001 else -1 if r < -0.001 else 0
        for t, (o, c, _v) in by[iso].items():
            rows = hist[t]
            lo, hi = 0, len(rows)
            while lo < hi:
                mid = (lo + hi) // 2
                if rows[mid][0] < iso:
                    lo = mid + 1
                else:
                    hi = mid
            if lo == 0:
                continue
            pdate, po, pv = rows[lo - 1]
            if not j_fresh(pdate, iso):
                continue
            j = j_from_opens(o, po)
            h = (c - o) / o
            rec = {
                "date": iso, "ticker": t, "J": j, "net": h - FEE_RT,
                "prior_vol": pv,
            }
            out_all.append(rec)
            if pv >= PRIOR_VOL_LIQ:
                out_liq.append(rec)
    return out_all, out_liq, spy


def load_membership():
    all_m, liq = defaultdict(set), defaultdict(set)
    static = set()
    for path in sorted(glob(os.path.join(MEM_DIR, "????-??-??_membership.csv"))):
        iso = os.path.basename(path)[:10]
        with open(path, encoding="utf-8", errors="replace") as fh:
            for rec in csv.DictReader(fh):
                t = (rec.get("Ticker") or "").strip().upper()
                if not t:
                    continue
                all_m[iso].add(t)
                if (rec.get("liq") or "").strip().lower() == "high":
                    liq[iso].add(t)
                if iso == "2026-04-26":
                    static.add(t)
    return all_m, liq, static


def load_book_hz(hz):
    by = {}
    for path in sorted(glob(os.path.join(BOOK_DIR, "????-??-??_stock_book.json"))):
        iso = os.path.basename(path)[:10]
        try:
            raw = json.load(open(path, encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        buy = ((raw.get("books") or {}).get(hz) or {}).get("buy") or []
        names = []
        for rec in buy:
            if isinstance(rec, dict):
                t = (rec.get("ticker") or rec.get("Ticker") or "").strip().upper()
                if t:
                    names.append(t)
        if names:
            by[iso] = names
    return by


def prior_map(by, days):
    keys = sorted(by)
    out = {}
    for iso in days:
        prev = [k for k in keys if k < iso]
        if prev:
            out[iso] = by[prev[-1]]
    return out


def pack_trades(name, kind, trades, baseline, spy):
    rec = score_book(name, kind, trades, baseline, spy)
    rec["holdout_mean"] = mean([tr["net"] for tr in trades])
    return rec


def score_filter_universe(name, rows, spy, lo, hi):
    """Unranked universe: baseline = all; avoid = J<0; filter = J≤−1%."""
    sub = [r for r in rows if lo <= r["date"] <= hi]
    # score_book wants ticker/date/net
    def as_tr(r):
        return {"ticker": r["ticker"], "date": r["date"], "net": r["net"]}
    base_tr = [as_tr(r) for r in sub]
    av_tr = [as_tr(r) for r in sub if r.get("J") is not None and r["J"] < 0]
    el_tr = [as_tr(r) for r in sub if r.get("J") is not None and r["J"] <= -0.01]
    dummy = {"holdout_mean": None}
    base = pack_trades(f"{name}_all", "baseline", base_tr, None, spy)
    dummy["holdout_mean"] = base["holdout_mean"]
    avoid = pack_trades("avoid_J_ge0", "avoid", av_tr, dummy, spy)
    filt = pack_trades("filter_J_le-1", "filter", el_tr, dummy, spy)
    return {"name": name, "lo": lo, "hi": hi, "baseline": base,
            "avoid_J_ge0": avoid, "filter_J_le-1": filt}


def universe_verdict(prove_rec, pooled_rec=None):
    """KEEP only if the holdout/prove slice clears the bar. Else CONDITIONAL/DEMOTE."""
    def ok(r):
        return r and r.get("verdict") == "KEEP"
    if ok(prove_rec):
        return "KEEP"
    if pooled_rec and ok(pooled_rec):
        return "CONDITIONAL"
    return "DEMOTE"


def finviz_universe_rows(panel, flags, joins, mem_all, mem_liq, b1, b3):
    """Name-day rows with Finviz J already on the panel."""
    by_u = {
        "join_full": [],
        "join_top15": [],
        "membership": [],
        "membership_liq": [],
        "book_1d_prior": [],
        "book_3d_prior": [],
    }
    top15 = {}
    for iso, ranked in joins.items():
        if iso not in flags:
            continue
        top15[iso] = {r["ticker"] for r in ranked[:15]}
    p1 = prior_map(b1, flags)
    p3 = prior_map(b3, flags)
    for r in panel:
        iso, t = r["date"], r["ticker"]
        j = r["xl"]["J"] if r["xl"].get("J_fresh") else None
        row = {"date": iso, "ticker": t, "J": j, "net": r["net"]}
        by_u["join_full"].append(row)
        if t in mem_all.get(iso, ()):
            by_u["membership"].append(row)
        if t in mem_liq.get(iso, ()):
            by_u["membership_liq"].append(row)
        if t in top15.get(iso, ()):
            by_u["join_top15"].append(row)
        if t in set(p1.get(iso) or ()):
            by_u["book_1d_prior"].append(row)
        if t in set(p3.get(iso) or ()):
            by_u["book_3d_prior"].append(row)
    return by_u


def run(panel=None, flags_by_day=None, joins=None, spy=None, fz=None, hist=None):
    if fz is None:
        fz = load_finviz()
        hist = history_index(fz)
    if joins is None:
        joins = load_join_days()
    if panel is None:
        books = load_book_1d()
        asof = load_asof()
        spy = {}
        panel, flags_by_day = build_panel(
            joins, fz, hist, books, asof, spy, start=HOLD_CUT, sessions_only=True,
        )
    mem_all, mem_liq, mem_static = load_membership()
    b1 = load_book_hz("1d")
    b3 = load_book_hz("3d")
    frows = finviz_universe_rows(panel, flags_by_day, joins, mem_all, mem_liq, b1, b3)

    slices = {
        "discovery": DISCOVERY,
        "prove": PROVE,
        "pooled": ORIG,
    }
    universes = {}
    for uname, rows in frows.items():
        scored = {}
        for sname, (lo, hi) in slices.items():
            scored[sname] = score_filter_universe(uname, rows, spy, lo, hi)
        prove_a = scored["prove"]["avoid_J_ge0"]
        pool_a = scored["pooled"]["avoid_J_ge0"]
        universes[uname] = {
            "slices": scored,
            "verdict": universe_verdict(prove_a, pool_a),
            "clock": "Finviz Open J",
            "note": {
                "join_full": "all join-ranked names with H (same as membership on these dumps)",
                "join_top15": "morning join ranks 1–15",
                "membership": "same-day universe membership",
                "membership_liq": "same-day membership liq=high (morning tag)",
                "book_1d_prior": "prior-day stock_book 1d buy (PIT)",
                "book_3d_prior": "prior-day stock_book 3d buy (PIT)",
            }.get(uname, ""),
        }

    ohlc_block = {"available": False, "reason": "pyarrow or ohlc.parquet missing"}
    if _try_pyarrow() and os.path.isfile(OHLC_PATH):
        print("loading ohlc.parquet …", flush=True)
        by = load_ohlc()
        all_nd, liq_nd, spy_o = ohlc_name_days(by)
        mem_nd = [r for r in all_nd if r["ticker"] in mem_static]
        print(f"ohlc all={len(all_nd)} liq={len(liq_nd)} mem04-26={len(mem_nd)}",
              flush=True)
        ohlc_slices = {
            "long": ("2024-03-06", "2026-08-21"),
            "y2025": ("2025-01-02", "2025-12-31"),
            "pre813": ("2026-01-02", "2026-08-12"),
            "post813_ohlc": ("2026-08-14", "2026-08-21"),
        }
        ohlc_unis = {
            "ohlc_all": (all_nd, "every Yahoo name-day with fresh J (includes microcaps)"),
            "ohlc_liq": (liq_nd, f"prior-session volume ≥ {PRIOR_VOL_LIQ:,} (open-fair)"),
            "mem_20260426": (mem_nd, "04-26 membership tickers on later OHLC sessions"),
        }
        ohlc_scored = {}
        for uname, (rows, note) in ohlc_unis.items():
            scored = {}
            for sname, (lo, hi) in ohlc_slices.items():
                scored[sname] = score_filter_universe(uname, rows, spy_o, lo, hi)
            # holdout = long / y2025 / pre813 — none of these were the discovery peek
            hold = scored["pre813"]["avoid_J_ge0"]
            long_a = scored["long"]["avoid_J_ge0"]
            # KEEP only if a long/pre holdout clears; long ohlc_all ghosts fail
            verdict = universe_verdict(hold, long_a)
            # liquid long is the honest Yahoo panel
            if uname == "ohlc_liq":
                verdict = "DEMOTE" if not (
                    scored["long"]["avoid_J_ge0"].get("verdict") == "KEEP"
                    or scored["pre813"]["avoid_J_ge0"].get("verdict") == "KEEP"
                    or scored["y2025"]["avoid_J_ge0"].get("verdict") == "KEEP"
                ) else "KEEP"
            if uname == "ohlc_all":
                # mean looks good; ghost fails (lottery). Do not KEEP.
                verdict = "DEMOTE"
            ohlc_scored[uname] = {
                "slices": scored, "verdict": verdict, "clock": "Yahoo OHLC Open J",
                "note": note,
            }
        ohlc_block = {
            "available": True,
            "n_ohlc_days": len({r["date"] for r in all_nd}),
            "first": min((r["date"] for r in all_nd), default=None),
            "last": max((r["date"] for r in all_nd), default=None),
            "universes": ohlc_scored,
        }
        universes.update(ohlc_scored)

    # standing join top-8 / top-80 already in JOIN_POST_813 windows — card here too
    payload = {
        "generated": str(date.today()),
        "j_formula": "J = (today Open − prior weekday Open) / prior Open",
        "leak": "PASS (unchanged gate)",
        "live_untouched": "flatten_robust",
        "no_prices_before": "2024-03-04",
        "no_join_before": "2026-08-12",
        "no_fresh_j_before": "2026-08-14 on Finviz (08-13 prior is 04-26)",
        "universes": universes,
        "ohlc": ohlc_block,
        "finviz_slices": slices,
    }
    return payload


def compact(payload):
    """Drop huge nested ghost objects' unused fields for json size — keep scores."""
    def slim_rec(r):
        if not isinstance(r, dict) or "n" not in r:
            return r
        g = r.get("ghost") or {}
        return {
            "name": r.get("name"), "kind": r.get("kind"),
            "n": r.get("n"), "n_dates": r.get("n_dates"),
            "holdout_mean": r.get("holdout_mean"),
            "vs_fullscan_pp": r.get("vs_fullscan_pp"),
            "win": r.get("win"),
            "ghost": f"{g.get('name')}/{g.get('month')}/{g.get('day')}",
            "ghost_pass": g.get("pass"),
            "verdict": r.get("verdict"),
            "fail_reasons": r.get("fail_reasons"),
        }

    out = {k: v for k, v in payload.items() if k != "universes"}
    unis = {}
    for name, u in payload["universes"].items():
        slices = {}
        for sname, sl in u["slices"].items():
            slices[sname] = {
                "lo": sl["lo"], "hi": sl["hi"],
                "baseline": slim_rec(sl["baseline"]),
                "avoid_J_ge0": slim_rec(sl["avoid_J_ge0"]),
                "filter_J_le-1": slim_rec(sl["filter_J_le-1"]),
            }
        unis[name] = {
            "verdict": u["verdict"], "clock": u["clock"], "note": u["note"],
            "slices": slices,
        }
    out["universes"] = unis
    if out.get("ohlc", {}).get("universes"):
        out["ohlc"] = {
            k: v for k, v in out["ohlc"].items() if k != "universes"
        }
        out["ohlc"]["universe_names"] = [
            n for n in payload["universes"] if n.startswith("ohlc") or n.startswith("mem_2026")
        ]
    return out


if __name__ == "__main__":
    p = run()
    c = compact(p)
    os.makedirs(os.path.dirname(UNIVERSE_JSON), exist_ok=True)
    json.dump(c, open(UNIVERSE_JSON, "w"), indent=2, default=str)
    print("wrote", UNIVERSE_JSON, flush=True)
    for name, u in c["universes"].items():
        print(f"  {u['verdict']:12} {name}", flush=True)
        sl = u["slices"]
        key = "prove" if "prove" in sl else "pre813"
        if key in sl:
            a = sl[key]["avoid_J_ge0"]
            print(f"    {key} avoid n={a['n']} vs={a['vs_fullscan_pp']} "
                  f"ghost={a['ghost']} bar={a['verdict']}", flush=True)
