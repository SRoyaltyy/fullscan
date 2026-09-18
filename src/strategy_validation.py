"""Re-evaluate published recipes with one stateful, risk-limited cash engine.

CLI: python -m src.strategy_validation [--names A,B] [--starts all|featured]
Historical reconstructed inputs remain exploratory, regardless of performance.
No network calls, model calls, orders, or live-policy changes.
"""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import html
import gzip
import json
from pathlib import Path
from statistics import mean

from . import factor_mine as fm, factor_mine_book as fmb, factor_mine_combo as fmc
from .execution_clock import fixed_bps
from .research_validation import block_interval, clock_errors, digest, start_diagnostics

ROOT = Path(__file__).resolve().parent.parent


def inputs(path):
    data = json.loads(Path(path).read_text())
    sim = data["sim"]
    rows = sim["rows"]
    by_date = defaultdict(list)
    for r in rows:
        by_date[r["date"]].append(r)
    panel = {"rows": rows, "by_date": dict(by_date), "session_dates": sim["dates"],
             "_tape_filled": True, "_ohlc_filled": True}
    bars = {(t, d): {"open": px[0], "close": px[1]} for t, days in sim["tape"].items()
            for d, px in days.items()}
    # Missing regime is conservative sit, explicitly disclosed in the report.
    regime = {d: {"predict_score": sim["s"].get(d, -3)} for d in sim["dates"]}
    return data, panel, bars, regime


def clipped(panel, dates):
    keep = set(dates)
    return {**panel, "session_dates": list(dates),
            "rows": [r for r in panel["rows"] if r["date"] in keep],
            "by_date": {d: panel["by_date"].get(d, []) for d in dates}}


def replay(name, panel, recipes, bars, regime, fees, bps=10, risk=None, start=None):
    rec = recipes[name]
    kw = dict(bars=bars, fees=fees, regime=regime, start=start, exec_fill=fixed_bps(bps), risk=risk)
    if rec.get("universe") != "combo":
        return fmb.simulate_book(panel, rec, **kw)
    members = [recipes[m] for m in rec["members"]]
    if rec.get("pool") == "split":
        # Split accounts keep their own capital, rather than sharing financing.
        return fmc.simulate_split(panel, members, rec["weights"], name=name, **kw)
    return fmc.simulate_shared(panel, members, rec["weights"], name=name,
                               net=rec.get("net", "priority"), **kw)


def summarize(book):
    eq = book.get("equity") or [10000.]
    peak, dd = eq[0], 0.
    for x in eq:
        peak = max(peak, x)
        dd = max(dd, 100 * (peak - x) / peak if peak else 0)
    pnl = defaultdict(float)
    turnover = 0.
    for t in book.get("trades", []):
        if t.get("pnl") is not None:
            pnl[t["ticker"]] += t["pnl"]
        if t.get("side") in ("BUY", "SELL", "SHORT", "COVER"):
            turnover += t["shares"] * t["price"]
    daily = [r["mean"] for r in book.get("daily", []) if r.get("mean") is not None]
    return {"book_pct": book["total_ret_pct"], "max_drawdown_pct": round(dd, 3),
            "n_orders": book.get("n_trades"), "n_sessions": len(daily),
            "mean_daily_interval": block_interval(daily),
            "turnover_dollars": round(turnover, 2),
            "top_realized_contributors": sorted(pnl.items(), key=lambda x: -x[1])[:5],
            "max_gross_ratio": max((d.get("exposure", {}).get("gross_ratio") or 0
                                     for d in book.get("daily", [])), default=0),
            "audit": book.get("audit"),
            "unavailable_borrow_entries": sum(s.get("kind") == "borrow_unavailable"
                                                for s in book.get("skips", [])),
            "status": "EXPLORATORY — no prospective execution proof"}


def build(path, *, names=None, starts="featured"):
    data, panel, bars, regime = inputs(path)
    recipes = {r["name"]: r for r in data["recipes"]}
    names = names or sorted(recipes)
    fees = data["sim"]["fees"]
    dates = panel["session_dates"]
    invalid = sum(bool(clock_errors(r)) for r in panel["rows"])
    report = {"generated_at": datetime.now(timezone.utc).isoformat(),
              "input_hash": digest(data["sim"]), "accounting_version": 2,
              "window": [dates[0], dates[-1]], "n_recipes": len(names),
              "provenance_missing_or_late_rows": invalid,
              "unknown_regime_dates_sit": [d for d in dates if d not in data["sim"]["s"]],
              "execution": "fixed adverse bps on both entry and exit; sensitivity, not observed fills",
              "shorts": "unavailable without dated locates; separate assumed-borrow sensitivity",
              "certified_strategies": [], "results": {}, "selection_walkforward": []}
    featured = set(data.get("featured", [])) | {"union_hot_n4_h1", "short_news_r_h3", "combo_sh_5050_shared"}
    for i, name in enumerate(names):
        scenarios = {}
        for bps in (0, 10, 50):
            book = replay(name, panel, recipes, bars, regime, fees, bps)
            scenarios[f"strict_{bps}bps"] = summarize(book)
        assumed = replay(name, panel, recipes, bars, regime, fees, 10,
                         risk={"require_locate": False})
        scenarios["assumed_borrow_10bps"] = summarize(assumed)
        start_rows = []
        if starts == "all" or name in featured:
            for date in dates:
                b = replay(name, panel, recipes, bars, regime, fees, 10, start=date)
                start_rows.append({"date": date, "total_ret_pct": b["total_ret_pct"]})
        report["results"][name] = {"scenarios": scenarios, "starts": start_rows,
                                   "start_diagnostics": start_diagnostics(start_rows)}
        if (i + 1) % 25 == 0:
            print(f"[validation] {i+1}/{len(names)} recipes", flush=True)
    # Each fold chooses only on its training path; reset accounts are explicit.
    # Grid itself is retrospective, so this is not a prospective certification.
    for cut in range(8, len(dates) - 3, 4):
        train, test = clipped(panel, dates[:cut]), clipped(panel, dates[cut:cut+4])
        scores = []
        for name in names:
            b = replay(name, train, recipes, bars, regime, fees, 10)
            scores.append((b["total_ret_pct"], name))
        chosen = sorted(scores, key=lambda x: (-x[0], x[1]))[0][1]
        result = replay(chosen, test, recipes, bars, regime, fees, 10)
        report["selection_walkforward"].append({"fit_through": dates[cut-1],
            "test_dates": test["session_dates"], "selected": chosen,
            "test": summarize(result), "fresh_account": True})
    report["research_requests"] = [
        {"need": "pre-decision feature snapshots", "reason": f"{invalid} rows lack valid immutable availability evidence"},
        {"need": "timestamped bid/ask, displayed size and order acknowledgments", "reason": "daily OHLC cannot calibrate submission latency or queue fills"},
        {"need": "dated short inventory and borrow rates", "reason": "historical short availability is unknown"},
        {"need": "new untouched sessions", "reason": "current recipe grid has already seen this historical window"}]
    return report


def write_report(report, out):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "strategy_validation.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    (out / "strategy_validation.json.gz").write_bytes(gzip.compress(
        (out / "strategy_validation.json").read_bytes(), mtime=0))
    lines = ["# Strategy validation", "", "**No strategy is certified by this historical replay.**",
             "", f"Window: {report['window']}. Recipes: {report['n_recipes']}.",
             "", "Strict columns exclude shorts without locates; assumed borrow is a separate research sensitivity.",
             "Costs apply to actual stateful entry and exit quantities. Book-to-end starts overlap.", "",
             "| Strategy | Strict 0bp | Strict 10bp | Strict 50bp | Assumed borrow 10bp | Positive starts |",
             "|---|---:|---:|---:|---:|---:|"]
    for name, r in sorted(report["results"].items(), key=lambda x: -x[1]["scenarios"]["strict_10bps"]["book_pct"]):
        s = r["scenarios"]
        rate = r["start_diagnostics"]["positive_fraction"]
        lines.append(f"| {name} | {s['strict_0bps']['book_pct']:.2f}% | {s['strict_10bps']['book_pct']:.2f}% | "
                     f"{s['strict_50bps']['book_pct']:.2f}% | {s['assumed_borrow_10bps']['book_pct']:.2f}% | "
                     + (f"{rate:.1%}" if rate is not None else "not run") + " |")
    lines += ["", "## Historical selection walk-forward", "", "Fresh account per fold; retrospectively defined grid.", ""]
    for f in report["selection_walkforward"]:
        lines.append(f"- Fit through {f['fit_through']}: {f['selected']}; next window {f['test']['book_pct']:+.2f}%.")
    lines += ["", "## Prerequisite research", ""]
    lines.extend(f"- {r['need']}: {r['reason']}" for r in report["research_requests"])
    text = "\n".join(lines) + "\n"
    (out / "STRATEGY_VALIDATION.md").write_text(text)
    write_dashboard(report)
    return text


def write_dashboard(report):
    page = ROOT / "dashboard/validation/index.html"
    page.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for name, r in sorted(report["results"].items(), key=lambda x: -x[1]["scenarios"]["strict_10bps"]["book_pct"]):
        v = r["scenarios"]
        rows.append("<tr><td>" + html.escape(name) + "</td>" + "".join(
            f"<td>{v[k]['book_pct']:+.2f}%</td>" for k in
            ("strict_0bps", "strict_10bps", "strict_50bps", "assumed_borrow_10bps")) + "</tr>")
    page.write_text("""<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Strategy validation</title>
<style>body{font:16px system-ui;max-width:1100px;margin:40px auto;padding:0 20px;background:#101827;color:#e5eaf3}table{border-collapse:collapse;width:100%;font-size:14px}td,th{padding:9px;border-bottom:1px solid #394458;text-align:right}td:first-child,th:first-child{text-align:left}.scroll{overflow:auto}aside{padding:20px;background:#483817}</style>
<h1>Strategy validation</h1><aside><strong>Research only: zero certified strategies.</strong>
Historical availability is unverified. Positive returns do not establish reliable live profits.</aside>
<p>Same stateful portfolio engine, fees, collateral limits and calendar-day borrow costs.
Strict scenarios exclude shorts without dated locates. Assumed borrow is an unverified sensitivity.
Basis points apply adversely on entry and exit; they are not measured latency.</p>
<p>Cash-start windows overlap. A decreasing book-to-end curve does not prove profitability in all markets.
Stops are morning checks, not intraday protective orders. Combo results are precomputed.</p>
<p>""" + html.escape(str(report["window"])) + """</p>
<div class="scroll"><table><thead><tr><th>Strategy</th><th>Strict 0bp</th><th>Strict 10bp</th><th>Strict 50bp</th><th>Assumed borrow 10bp</th></tr></thead><tbody>""" + "".join(rows) + "</tbody></table></div></html>")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(ROOT / "03_scoreboard/factor_mine.json"))
    ap.add_argument("--out", default=str(ROOT / "03_scoreboard/validation"))
    ap.add_argument("--names", default="")
    ap.add_argument("--starts", choices=("all", "featured"), default="featured")
    args = ap.parse_args()
    report = build(args.input, names=args.names.split(",") if args.names else None, starts=args.starts)
    write_report(report, args.out)
    print(f"[validation] {len(report['results'])} recipes; certified=0; {args.out}")


if __name__ == "__main__":
    main()
