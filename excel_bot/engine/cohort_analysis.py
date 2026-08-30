"""Cohort analysis: does cluster behavior depend on stock type?

Joins finviz fundamentals to the grids and reruns the key metrics per cohort:
  - real-time cluster-following trades (green long / red short)
  - green-cluster-end forward returns (the exit signal)
  - the lag decomposition: how much of a green cluster's hindsight return
    happens BEFORE the cluster is confirmable (uncapturable) vs after
"""
import csv
import glob
import json
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sweep import days_features, detect_def
from signals import classify_fill

FINVIZ = "data/finviz_with_descriptions.csv"


def load_finviz():
    out = {}
    with open(FINVIZ, encoding="utf-8", errors="replace") as fh:
        for rec in csv.DictReader(fh):
            t = rec.get("Ticker", "").strip()
            if not t:
                continue
            def num(k):
                v = (rec.get(k) or "").replace("%", "").replace(",", "").strip()
                mult = 1.0
                if v.endswith(("K", "M", "B")):          # finviz 2.5M-style
                    mult = {"K": 1e3, "M": 1e6, "B": 1e9}[v[-1]]
                    v = v[:-1]
                try:
                    return float(v) * mult
                except ValueError:
                    return None
            price = num("Price")
            avgvol = num("Average Volume")              # THOUSANDS of shares/day
            avgvol = avgvol * 1e3 if avgvol is not None else None
            out[t] = {
                "mcap": num("Market Cap"),          # $ millions
                "beta": num("Beta"),
                "pm": num("Profit Margin"),
                "short_float": num("Short Float"),
                "sector": rec.get("Sector") or "",
                "inst_own": num("Institutional Ownership"),
                "volm": num("Volatility (Month)"),  # % per day-ish
                "dollar_vol": (avgvol * price) if (avgvol and price) else None,
                "index": rec.get("Index") or "",
                "optionable": rec.get("Optionable") or "",
                "gross_margin": num("Gross Margin"),
                "roe": num("Return on Equity"),
                "insider_own": num("Insider Ownership"),
            }
    return out


def cohorts_of(fz):
    """Assign cohort labels to one finviz record."""
    out = []
    mc, b, pm = fz["mcap"], fz["beta"], fz["pm"]
    sf = fz["short_float"]
    if mc is not None:
        out.append("mcap:micro(<300M)" if mc < 300 else
                   "mcap:small(300M-2B)" if mc < 2000 else
                   "mcap:mid(2-10B)" if mc < 10000 else "mcap:large(>10B)")
    if b is not None:
        out.append("beta:low(<0.8)" if b < 0.8 else
                   "beta:mid(0.8-1.5)" if b <= 1.5 else "beta:high(>1.5)")
    if pm is not None:
        out.append("prof:yes" if pm > 0 else "prof:no")
    if sf is not None:
        out.append("short:high(>10%)" if sf > 10 else "short:low")
    if b is not None and pm is not None:
        if b > 1.5 and pm < 0:
            out.append("style:HYPE")
        elif pm > 10 and b < 1.2:
            out.append("style:SOLID")
    if mc is not None and 1000 <= mc < 10000:   # the "sweet spot" middle
        out.append("mid(1-10B):all")
        if b is not None:
            out.append("mid:beta>1.5" if b > 1.5 else "mid:beta<=1.5")
        if pm is not None:
            out.append("mid:unprofitable" if pm < 0 else "mid:profitable")
        if b is not None and pm is not None and b > 1.5 and pm < 0:
            out.append("mid:BBAI-like(hi-beta,unprof)")
    # ---- extended differentiators (added 2026-07-27 for full-universe run) --
    if fz.get("sector"):
        out.append(f"sector:{fz['sector']}")
    if fz.get("index") and fz["index"] not in ("-", ""):
        out.append(f"index:{fz['index']}")
    vm = fz.get("volm")
    if vm is not None:
        out.append("volM:high(>8%)" if vm > 8 else
                   "volM:low(<3%)" if vm < 3 else "volM:mid")
    dv = fz.get("dollar_vol")
    if dv is not None:
        out.append("liq:ultra(>$500M/d)" if dv > 500e6 else
                   "liq:high($50-500M/d)" if dv > 50e6 else
                   "liq:low(<$5M/d)" if dv < 5e6 else "liq:mid($5-50M/d)")
    io = fz.get("inst_own")
    if io is not None:
        out.append("inst:high(>70%)" if io > 70 else
                   "inst:low(<25%)" if io < 25 else "inst:mid")
    gm = fz.get("gross_margin")
    if gm is not None:
        out.append("gm:high(>50%)" if gm > 50 else
                   "gm:low(<20%)" if gm < 20 else "gm:mid")
    roe = fz.get("roe")
    if roe is not None:
        out.append("roe:neg" if roe < 0 else
                   "roe:high(>20%)" if roe > 20 else "roe:mid")
    ins = fz.get("insider_own")
    if ins is not None:
        out.append("insider:high(>20%)" if ins > 20 else "insider:low")
    if fz.get("optionable"):
        out.append(f"opt:{fz['optionable']}")
    return out


def tstat(rets):
    n = len(rets)
    if n < 10:
        return None
    avg = sum(rets) / n
    sd = math.sqrt(sum((r - avg) ** 2 for r in rets) / (n - 1))
    return avg, avg / (sd / math.sqrt(n)) if sd > 0 else 0, n


def main():
    fz = load_finviz()
    print(f"finviz: {len(fz)} tickers")
    files = sorted(f for f in glob.glob("grids/*.json")
                   if not os.path.basename(f).startswith("_"))
    tickers = {}
    for f in files:
        g = json.load(open(f))
        t = g["ticker"]
        if t in fz and len(g["days"]) >= 40:
            tickers[t] = days_features(g["days"])
    print(f"grids joined to finviz: {len(tickers)}")

    DEF = {"name": "tol2_core_score_ml3", "kind": "tolerant", "key": "core_score",
           "thresh": 0.5, "tol": 2, "min_len": 3}
    DEF2 = {"name": "strict_A_ml2", "kind": "strict", "key": "a", "min_len": 2}

    rt = defaultdict(list)        # cohort -> [rt trade returns]
    flip5 = defaultdict(list)     # cohort -> [green-flip +5d returns]
    flip10 = defaultdict(list)
    lag_pre, lag_post = [], []    # lag decomposition (green clusters)
    for t, days in tickers.items():
        closes = [d["close"] for d in days]
        for cohort in cohorts_of(fz[t]):
            clusters = detect_def(days, DEF)
            for c in clusters:
                i, j, side = c["start"], c["end"] - 1, c["side"]
                ei, xi = c.get("entry_idx"), c.get("exit_idx")
                if ei is not None and xi is not None and closes[ei] and closes[xi]:
                    rt[cohort].append((closes[xi] - closes[ei]) / closes[ei] * side)
                if xi is not None and side == 1 and xi + 10 < len(closes):
                    flip5[cohort].append((closes[xi + 5] - closes[xi]) / closes[xi])
                    flip10[cohort].append((closes[xi + 10] - closes[xi]) / closes[xi])
        # lag decomposition on green clusters of DEF2 (all tickers pooled)
        for c in detect_def(days, DEF2):
            if c["side"] != 1:
                continue
            i, j, ei = c["start"], c["end"] - 1, c.get("entry_idx")
            if ei is None or ei >= j:
                continue
            o = days[i]["open"]
            if not o or not closes[ei] or not closes[j]:
                continue
            lag_pre.append((closes[ei] - o) / o)
            lag_post.append((closes[j] - closes[ei]) / closes[ei])

    s = tstat(lag_pre)
    print(f"\nLAG DECOMPOSITION (green clusters, strict_A_ml2, all stocks):")
    print(f"  start-open -> confirmation close: avg={s[0]:+.2%} t={s[1]:+.1f} n={s[2]}")
    s = tstat(lag_post)
    print(f"  confirmation  -> cluster end:     avg={s[0]:+.2%} t={s[1]:+.1f} n={s[2]}")

    print(f"\n{'cohort':22s} {'RT trades':>9s} {'RT avg':>8s} {'RT t':>6s} "
          f"{'flip+5d':>9s} {'t':>6s} {'flip+10d':>9s} {'t':>6s}")
    order = sorted(set(list(rt) + list(flip5)))
    for cohort in order:
        a = tstat(rt.get(cohort, []))
        f5 = tstat(flip5.get(cohort, []))
        f10 = tstat(flip10.get(cohort, []))
        print(f"{cohort:22s} "
              f"{(a[2] if a else 0):9d} {(a[0] if a else 0):+8.2%} {(a[1] if a else 0):+6.2f} "
              f"{(f5[0] if f5 else 0):+9.2%} {(f5[1] if f5 else 0):+6.2f} "
              f"{(f10[0] if f10 else 0):+9.2%} {(f10[1] if f10 else 0):+6.2f}")


if __name__ == "__main__":
    main()
