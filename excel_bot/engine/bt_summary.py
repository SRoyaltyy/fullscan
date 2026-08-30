"""Summarize backtest/trades.csv into a human-readable effectiveness report.

Tables:
  - per strategy: n, win%, avg, median, t-stat, profit factor, exit mix
  - per strategy x year (regime check: 2023/2024/2025/2026)
  - per strategy x mcap cohort
  - validation: overlap of historical signals with live suggestions.csv

Usage: python engine/bt_summary.py
"""
import csv
import math
import os
import sys
from collections import defaultdict
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

BT_DIR = "backtest"
TRADES_CSV = os.path.join(BT_DIR, "trades.csv")
SUGG_CSV = "suggestions/suggestions.csv"


def pct(s):
    try:
        return float(str(s).replace("%", "")) / 100
    except (ValueError, TypeError):
        return None


def stats(rets):
    rets = [r for r in rets if r is not None]
    n = len(rets)
    if n < 2:
        return None
    avg = sum(rets) / n
    sd = math.sqrt(sum((r - avg) ** 2 for r in rets) / (n - 1))
    t = avg / (sd / math.sqrt(n)) if sd > 0 else 0
    wins = sum(1 for r in rets if r > 0)
    gw = sum(r for r in rets if r > 0)
    gl = -sum(r for r in rets if r < 0)
    med = sorted(rets)[n // 2] if n % 2 else \
        (sorted(rets)[n // 2 - 1] + sorted(rets)[n // 2]) / 2
    return {"n": n, "win": wins / n, "avg": avg, "med": med, "t": t,
            "pf": gw / gl if gl > 0 else float("inf")}


def row_md(cells):
    return "| " + " | ".join(str(c) for c in cells) + " |"


def table(headers, rows):
    out = [row_md(headers), "|" + "---|" * len(headers)]
    out += [row_md(r) for r in rows]
    return "\n".join(out)


def fmt_stats(name, s):
    if not s:
        return [name, 0, "-", "-", "-", "-", "-"]
    return [name, s["n"], f"{s['win']:.1%}", f"{s['avg']:+.2%}",
            f"{s['med']:+.2%}", f"{s['t']:+.2f}",
            f"{s['pf']:.2f}" if s["pf"] != float("inf") else "inf"]


def main():
    trades = []
    with open(TRADES_CSV, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            r["_ret"] = pct(r["ret_close_entry"])
            r["_year"] = (r["signal_date"] or "")[:4]
            mc = next((c for c in r["cohorts"].split("|")
                       if c.startswith("mcap:")), "mcap:?")
            r["_mcap"] = mc
            trades.append(r)
    print(f"{len(trades)} trades loaded")

    by_strat = defaultdict(list)
    by_strat_year = defaultdict(list)
    by_strat_mcap = defaultdict(list)
    exit_mix = defaultdict(lambda: defaultdict(int))
    for r in trades:
        by_strat[r["strategy"]].append(r["_ret"])
        by_strat_year[(r["strategy"], r["_year"])].append(r["_ret"])
        by_strat_mcap[(r["strategy"], r["_mcap"])].append(r["_ret"])
        exit_mix[r["strategy"]][r["exit_reason"]] += 1

    L = []
    L.append(f"# Excel-bot historical backtest — generated {date.today()}")
    L.append("")
    L.append("Every historical day was run through the exact Excel-replica "
             "engine; a trade is recorded when a cluster confirmed that day "
             "(same logic as the live bot). Entry = confirmation-day close "
             "(per strategy cards), exit per each strategy's exit rule. "
             "Cohort tags use the current finviz snapshot (see caveats).")
    L.append("")
    L.append("## Per strategy (all years)")
    L.append("")
    hdr = ["strategy", "n", "win%", "avg", "median", "t-stat", "PF"]
    L.append(table(hdr, [fmt_stats(s, stats(by_strat[s]))
                         for s in sorted(by_strat)]))
    L.append("")
    L.append("## Per strategy × year (regime consistency)")
    L.append("")
    years = sorted({r["_year"] for r in trades})
    hdr2 = ["strategy"] + years
    rows2 = []
    for s in sorted(by_strat):
        row = [s]
        for y in years:
            st = stats(by_strat_year.get((s, y), []))
            row.append(f"{st['win']:.0%}/{st['avg']:+.1%} (n={st['n']})"
                       if st else "-")
        rows2.append(row)
    L.append(table(hdr2, rows2))
    L.append("")
    L.append("## Per strategy × market-cap cohort")
    L.append("")
    mcaps = sorted({r["_mcap"] for r in trades})
    hdr3 = ["strategy"] + [m.replace("mcap:", "") for m in mcaps]
    rows3 = []
    for s in sorted(by_strat):
        row = [s]
        for m in mcaps:
            st = stats(by_strat_mcap.get((s, m), []))
            row.append(f"{st['win']:.0%}/{st['avg']:+.1%} (n={st['n']})"
                       if st else "-")
        rows3.append(row)
    L.append(table(hdr3, rows3))
    L.append("")
    L.append("## Exit-reason mix")
    L.append("")
    reasons = sorted({rn for v in exit_mix.values() for rn in v})
    L.append(table(["strategy"] + reasons,
                   [[s] + [exit_mix[s].get(rn, 0) for rn in reasons]
                    for s in sorted(exit_mix)]))
    L.append("")

    # ---- validation: overlap with live suggestions ------------------------
    if os.path.exists(SUGG_CSV):
        with open(SUGG_CSV, newline="", encoding="utf-8") as fh:
            sugg = {(r["signal_date"], r["ticker"], r["strategy"])
                    for r in csv.DictReader(fh)}
        got = {(r["signal_date"], r["ticker"], r["strategy"]) for r in trades}
        hit = len(sugg & got)
        L.append("## Validation vs live bot")
        L.append("")
        L.append(f"- live suggestions on record: {len(sugg)}")
        L.append(f"- reproduced by historical engine: {hit} "
                 f"({hit/len(sugg):.1%})" if sugg else "- none")
        missing = sorted(sugg - got)[:10]
        if missing:
            L.append(f"- first missing: {', '.join('/'.join(m) for m in missing)}")
        L.append("")
    L.append("## Caveats")
    L.append("")
    L.append("- Cohort membership (mcap/beta/profitability/...) is the CURRENT "
             "finviz snapshot, not historical — a mid-cap today may have been "
             "a small-cap in 2023.")
    L.append("- Returns exclude fees/slippage; shorts assume borrow available.")
    L.append("- Clusters still open at the data edge exit at the last close "
             "(exit_reason *_end).")

    out = os.path.join(BT_DIR, f"summary_{date.today()}.md")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
