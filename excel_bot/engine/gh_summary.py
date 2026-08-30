"""Write a human-readable daily summary for the Excel-replica bot.

Run AFTER daily_run.py, from the excel_bot/ directory:
    python engine/gh_summary.py

Reads suggestions/suggestions.csv, writes daily/{today}_excel_bot.md.
Zero network, zero tokens — pure stdlib + the suggestions file.
"""
import csv
import os
from datetime import date

SUGG = "suggestions/suggestions.csv"
OUT_DIR = "daily"


def pct(v):
    try:
        return float(str(v).replace("%", "").replace("+", ""))
    except (TypeError, ValueError):
        return None


def main():
    today = date.today().isoformat()
    with open(SUGG, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    new = [r for r in rows if r["run_date"] == today]
    tracked = [r for r in rows if pct(r.get("ret_vs_open")) is not None
               and r["run_date"] != today]

    L = [f"# Excel-bot daily — {today}", ""]
    L.append(f"**{len(new)} new suggestions** today "
             f"(all-time: {len(rows)}; tracked with live returns: {len(tracked)})")
    L.append("")

    # ---- today's new signals
    L += ["## New suggestions", ""]
    if not new:
        L.append("_None — no cluster confirmed on the latest trading day._")
    else:
        L.append("| ticker | side | strategy | exit | ref close | signal colors |")
        L.append("|---|---|---|---|---|---|")
        for r in sorted(new, key=lambda r: (r["strategy"], r["ticker"])):
            L.append(f"| {r['ticker']} | {r['side']} | {r['strategy']} "
                     f"| {r['exit_rule']} | {r['ref_close']} | {r['signal_colors']} |")
    L.append("")

    # ---- strategy leaderboard (all tracked)
    L += ["## Live strategy scoreboard (all tracked suggestions, ret vs entry open)", ""]
    L.append("| strategy | n | mean | median | win% |")
    L.append("|---|---|---|---|---|")
    by_strat = {}
    for r in tracked:
        by_strat.setdefault(r["strategy"], []).append(pct(r["ret_vs_open"]))
    for s, vals in sorted(by_strat.items()):
        vals = sorted(vals)
        n = len(vals)
        mean = sum(vals) / n
        med = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
        win = sum(1 for v in vals if v > 0) / n * 100
        L.append(f"| {s} | {n} | {mean:+.2f}% | {med:+.2f}% | {win:.1f}% |")
    L.append("")

    # ---- movers among tracked
    def key(r):
        return pct(r["ret_vs_open"])
    top = sorted(tracked, key=key, reverse=True)[:10]
    bot = sorted(tracked, key=key)[:10]
    L += ["## Best open suggestions (ret vs entry open)", "",
          "| signal date | ticker | strategy | entry open | current | ret | days held |",
          "|---|---|---|---|---|---|---|"]
    for r in top:
        L.append(f"| {r['signal_date']} | {r['ticker']} | {r['strategy']} "
                 f"| {r['first_open']} | {r['current_price']} "
                 f"| {r['ret_vs_open']} | {r['days_held']} |")
    L += ["", "## Worst open suggestions", "",
          "| signal date | ticker | strategy | entry open | current | ret | days held |",
          "|---|---|---|---|---|---|---|"]
    for r in bot:
        L.append(f"| {r['signal_date']} | {r['ticker']} | {r['strategy']} "
                 f"| {r['first_open']} | {r['current_price']} "
                 f"| {r['ret_vs_open']} | {r['days_held']} |")
    L.append("")

    os.makedirs(OUT_DIR, exist_ok=True)
    out = os.path.join(OUT_DIR, f"{today}_excel_bot.md")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    print(f"[summary] wrote {out}", flush=True)


if __name__ == "__main__":
    main()
