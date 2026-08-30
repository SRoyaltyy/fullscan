"""Run: python -m src.ticker_lookback_run --tickers TEM,ELF,AAPL"""
from __future__ import annotations

import argparse
import json
from datetime import datetime

from . import ticker_lookback as tl
from . import ticker_lookback_cli as scan


def _icon(kind):
    return tl.BOX_ICON.get(kind, tl.BOX_ICON["missing"])


def scan_ticker(ticker):
    idx = tl.build_index()
    t = tl._tick(ticker)
    days, recommended, green_days = [], [], []
    for sess in idx["sessions"]:
        card = scan._scan_session(sess, t)
        if card is None:
            days.append({
                "date": sess["date"], "ticker": t, "class": "no_data",
                "sources": [], "boxes": {k: "missing" for k, _ in tl.BOX_COLS},
                "artifacts_that_day": sess["has"],
            })
            continue
        days.append(card)
        if card.get("buy_ranks"):
            recommended.append({
                "date": card["date"],
                "horizons": list(card["buy_ranks"].keys()),
                "ranks": card["buy_ranks"],
            })
        if card.get("independent_green", {}).get("green") or card.get("in_green_buy"):
            green_days.append(card["date"])
    hits = [d for d in days if d.get("class") != "no_data"]
    return {
        "ticker": t, "n_sessions": len(idx["sessions"]), "n_with_print": len(hits),
        "recommended_days": recommended, "green_days": green_days,
        "paper": idx["paper"].get(t) or [], "days": days,
    }


def scan_tickers(tickers):
    names = [tl._tick(t) for t in tickers if tl._tick(t)]
    idx = tl.build_index()
    return {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "sessions": [
            {"date": s["date"], "has": s["has"], "n_book": s["n_book"],
             "n_join": s["n_join"], "n_finviz": s["n_finviz"],
             "n_ab": s["n_ab"], "n_peer": s["n_peer"]}
            for s in idx["sessions"]
        ],
        "names": [scan_ticker(t) for t in names],
    }


def render_md(payload):
    L = [
        "# Ticker lookback — any name, every session",
        "", f"_Generated {payload['generated_at']}_", "",
        "Same boxes as `01_daily/2026-08-20_lookback.md`:",
        "good / present-flat / against / missing.", "",
        "A name does not have to be in the printed book.",
        "Independent green = join/gen/AB/peer all >= +0.05, sector/news not red, relvol not dead (<0.7).",
        "", "## Sessions on disk", "",
        "| Date | book | join | finviz | AB | peer |",
        "|------|-----:|-----:|-------:|---:|-----:|",
    ]
    for s in payload["sessions"]:
        L.append(
            f"| {s['date']} | {s['n_book'] or '—'} | {s['n_join'] or '—'} | "
            f"{s['n_finviz'] or '—'} | {s['n_ab'] or '—'} | {s['n_peer'] or '—'} |"
        )
    for rec in payload["names"]:
        buys = ", ".join(
            x["date"] + " (" + ",".join(x["horizons"]) + ")"
            for x in rec["recommended_days"]
        ) or "—"
        L += ["", f"## {rec['ticker']}", "",
              f"Prints on **{rec['n_with_print']}/{rec['n_sessions']}** sessions. Buy-book days: {buys}.", "",
              f"Independent-green days: {', '.join(rec['green_days']) or '—'}.", ""]
        if rec["paper"]:
            L.append("Paper fills:")
            for p in rec["paper"]:
                L.append(f"- {p['date']} {p.get('side')} {p.get('sleeve')} @ {p.get('price')} — {p.get('reason')}")
            L.append("")
        L += [
            "| Date | class | join | sect | gen | news | dig | jdg | AB | peer | heat | vol | cat | buy | sources |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for d in rec["days"]:
            boxes = d.get("boxes") or {}
            cells = " | ".join(_icon(boxes.get(k, "missing")) for k, _ in tl.BOX_COLS)
            src = ",".join(d.get("sources") or []) or "—"
            L.append(f"| {d['date']} | {d.get('class')} | {cells} | {src} |")
        L.append("")
        for d in rec["days"]:
            if d.get("class") == "no_data":
                continue
            s = d.get("signals") or {}
            L += [
                f"### {d['date']} · {d.get('size') or '?'} · {d.get('sector') or '?'}", "",
                f"**class: `{d.get('class')}`** · sources: {', '.join(d.get('sources') or []) or '—'}", "",
                " ".join(f"{_icon((d.get('boxes') or {}).get(k, 'missing'))}{lab}" for k, lab in tl.BOX_COLS), "",
            ]
            gate = d.get("independent_green") or {}
            L.append(f"Independent green: **{'YES' if gate.get('green') else 'no'}** — {gate.get('why')}")
            L.append("")
            if d.get("buy_ranks"):
                L.append("Buy-book ranks: " + ", ".join(f"{h} #{v['rank']}" for h, v in d["buy_ranks"].items()))
                L.append("")
            if d.get("reasons"):
                L.append(f"Ranker reasons: `{d['reasons']}`")
                L.append("")
    L += ["## Files", "", "- `data/stock_book/ticker_lookback.json`", "- `01_daily/ticker_lookback.md`", ""]
    return "\n".join(L) + "\n"


def run(tickers):
    payload = scan_tickers(tickers)
    tl.BOOK_DIR.mkdir(parents=True, exist_ok=True)
    tl.DAILY.mkdir(parents=True, exist_ok=True)
    tl.SCORE.mkdir(parents=True, exist_ok=True)
    js = tl.BOOK_DIR / "ticker_lookback.json"
    js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md = render_md(payload)
    (tl.DAILY / "ticker_lookback.md").write_text(md, encoding="utf-8")
    (tl.SCORE / "TICKER_LOOKBACK.md").write_text(md, encoding="utf-8")
    print(md[:12000])
    print(f"[ticker-lookback] wrote {js}")
    print(f"[ticker-lookback] wrote {tl.DAILY / 'ticker_lookback.md'}")
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", required=True, help="comma-separated, any listed name")
    args = ap.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        raise SystemExit("pass --tickers TEM,ELF")
    run(tickers)


if __name__ == "__main__":
    main()
