"""Run: python -m src.ticker_lookback_run --tickers TEM,ELF,AAPL"""
from __future__ import annotations

import argparse
import html
import json
from datetime import datetime

from . import ticker_lookback as tl
from . import ticker_lookback_cli as scan


def _icon(kind):
    kind = {"green": "good", "red": "bad", "yellow": "neutral"}.get(
        str(kind or "").lower(), kind)
    return tl.BOX_ICON.get(kind, tl.BOX_ICON["missing"])


def scan_ticker(ticker, sessions=None, idx=None):
    idx = idx or tl.build_index()
    t = tl._tick(ticker)
    days, recommended, green_days = [], [], []
    sessions = sessions if sessions is not None else idx["sessions"]
    for sess in sessions:
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
        "ticker": t, "n_sessions": len(sessions), "n_with_print": len(hits),
        "recommended_days": recommended, "green_days": green_days,
        "paper": idx["paper"].get(t) or [], "days": days,
    }


def scan_tickers(tickers, from_date=None, to_date=None):
    names = [tl._tick(t) for t in tickers if tl._tick(t)]
    idx = tl.build_index()
    sessions = [
        s for s in idx["sessions"]
        if (not from_date or s["date"] >= from_date)
        and (not to_date or s["date"] <= to_date)
    ]
    return {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "from_date": from_date, "to_date": to_date,
        "sessions": [
            {"date": s["date"], "has": s["has"], "n_book": s["n_book"],
             "n_join": s["n_join"], "n_finviz": s["n_finviz"],
             "n_ab": s["n_ab"], "n_peer": s["n_peer"]}
            for s in sessions
        ],
        "names": [scan_ticker(t, sessions=sessions, idx=idx) for t in names],
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
            fwd = d.get("forward_returns") or {}
            L.append(
                "Forward return from signal close: "
                + " · ".join(
                    f"**{h} {v:+.2f}%**" if v is not None else f"{h} —"
                    for h, v in fwd.items()
                )
            )
            L.append("")
            gate = d.get("independent_green") or {}
            L.append(f"Independent green: **{'YES' if gate.get('green') else 'no'}** — {gate.get('why')}")
            L.append("")
            if d.get("buy_ranks"):
                L.append("Buy-book ranks: " + ", ".join(f"{h} #{v['rank']}" for h, v in d["buy_ranks"].items()))
                L.append("")
            if d.get("reasons"):
                L.append(f"Ranker reasons: `{d['reasons']}`")
                L.append("")
            jf = d.get("join_families") or {}
            if jf:
                L.append("Join factors: " + " · ".join(
                    f"{_icon(v.get('tone', 'missing'))}{k}={v.get('value')}"
                    for k, v in jf.items()
                ))
                L.append("")
            for title, key in (
                ("Finviz full-export factors", "finviz_factors"),
                ("Finviz quote-color fields", "quote_color_fields"),
                ("AB checklist factors", "ab_factors"),
            ):
                factors = d.get(key) or {}
                if not factors:
                    continue
                L += [f"**{title}:**", ""]
                chunks = []
                for name, val in factors.items():
                    tone = val.get("tone") or val.get("color") or "neutral"
                    chunks.append(
                        f"{_icon(tone)} {name}=`{val.get('value')}`"
                    )
                L.append(" · ".join(chunks))
                L.append("")
    L += ["## Files", "", "- `data/stock_book/ticker_lookback.json`", "- `01_daily/ticker_lookback.md`", ""]
    return "\n".join(L) + "\n"


def _slug(tickers):
    return "-".join(tl._tick(t) for t in tickers if tl._tick(t)).lower()


def _factor_chips(day):
    chips = []
    for key, label in tl.BOX_COLS:
        tone = (day.get("boxes") or {}).get(key, "missing")
        chips.append(
            f'<span class="chip {html.escape(tone)}">{_icon(tone)} '
            f'{html.escape(label)}</span>'
        )
    return "".join(chips)


def _detail_rows(day):
    blocks = []
    for title, key in (
        ("Join factors", "join_families"),
        ("Finviz full-market factors", "finviz_factors"),
        ("Finviz quote colors", "quote_color_fields"),
        ("AB checklist", "ab_factors"),
    ):
        vals = day.get(key) or {}
        if not vals:
            continue
        chips = []
        for name, rec in vals.items():
            tone = str(rec.get("tone") or rec.get("color") or "neutral")
            css = {"green": "good", "red": "bad"}.get(tone, tone)
            chips.append(
                f'<span class="factor {html.escape(css)}">{_icon(tone)} '
                f'<b>{html.escape(str(name))}</b> '
                f'{html.escape(str(rec.get("value")))}</span>'
            )
        blocks.append(
            f"<h4>{html.escape(title)}</h4><div class='factors'>"
            + "".join(chips) + "</div>"
        )
    return "".join(blocks)


def render_html(payload):
    sections = []
    for rec in payload["names"]:
        cards = []
        for day in reversed(rec["days"]):
            no_data = day.get("class") == "no_data"
            fwd = day.get("forward_returns") or {}
            returns = " · ".join(
                f"{h} {v:+.2f}%" if v is not None else f"{h} —"
                for h, v in fwd.items()
            )
            cards.append(f"""
<article class="day {'nodata' if no_data else ''}" data-class="{html.escape(day.get('class',''))}">
  <div class="dayhead"><h3>{html.escape(day['date'])}</h3>
  <span class="class">{html.escape(day.get('class') or 'no_data')}</span></div>
  <div class="meta">{html.escape(str(day.get('sector') or '—'))} ·
    {html.escape(str(day.get('industry') or '—'))} · {html.escape(', '.join(day.get('sources') or []) or 'no source')}</div>
  <div class="chips">{_factor_chips(day)}</div>
  <div class="returns"><b>After signal close:</b> {html.escape(returns)}</div>
  <div class="gate"><b>Independent green:</b> {'YES' if (day.get('independent_green') or {}).get('green') else 'no'} —
    {html.escape(str((day.get('independent_green') or {}).get('why') or ''))}</div>
  {_detail_rows(day)}
</article>""")
        sections.append(f"""
<section class="ticker" id="{html.escape(rec['ticker'])}">
 <h2>{html.escape(rec['ticker'])}</h2>
 <p>{rec['n_with_print']}/{rec['n_sessions']} sessions with data ·
 independent-green: {html.escape(', '.join(rec['green_days']) or 'none')}</p>
 {''.join(cards)}
</section>""")
    nav = "".join(
        f'<a href="#{html.escape(r["ticker"])}">{html.escape(r["ticker"])}</a>'
        for r in payload["names"]
    )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ticker lookback</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui}}
main{{max-width:1000px;margin:auto;padding:16px}}h1,h2,h3,h4{{margin:.35em 0}}
nav{{display:flex;gap:8px;overflow:auto;position:sticky;top:0;background:#0b1020ee;padding:10px 0;z-index:2}}
nav a,.class{{padding:8px 12px;border:1px solid var(--line);border-radius:999px;color:var(--text);text-decoration:none;white-space:nowrap}}
.day{{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px;margin:12px 0}}
.day.nodata{{opacity:.55}}.dayhead{{display:flex;justify-content:space-between;align-items:center}}
.meta,.returns,.gate{{color:var(--muted);margin:8px 0}}.chips,.factors{{display:flex;flex-wrap:wrap;gap:7px}}
.chip,.factor{{border:1px solid var(--line);border-radius:9px;padding:7px 9px;min-height:38px}}
.good{{background:#123d2c}}.bad{{background:#4b2028}}.neutral{{background:#473e1d}}.missing{{background:#23283a}}
h4{{font-size:13px;color:var(--muted);margin-top:13px}}
@media(max-width:600px){{main{{padding:10px}}.day{{padding:11px}}.factor{{width:100%}}}}
</style></head><body><main>
<h1>Ticker lookback</h1>
<p>Any ticker, every dated full-market artifact on disk. Generated {html.escape(payload['generated_at'])}.</p>
<nav>{nav}</nav>{''.join(sections)}
</main></body></html>"""


def run(tickers, from_date=None, to_date=None):
    payload = scan_tickers(tickers, from_date=from_date, to_date=to_date)
    tl.BOOK_DIR.mkdir(parents=True, exist_ok=True)
    tl.DAILY.mkdir(parents=True, exist_ok=True)
    tl.SCORE.mkdir(parents=True, exist_ok=True)
    slug = _slug(tickers)
    if not slug:
        raise SystemExit("no valid tickers")
    js = tl.BOOK_DIR / "ticker_lookback.json"
    js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    json_dir = tl.BOOK_DIR / "ticker_lookback"
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / f"{slug}.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md = render_md(payload)
    (tl.DAILY / "ticker_lookback.md").write_text(md, encoding="utf-8")
    (tl.SCORE / "TICKER_LOOKBACK.md").write_text(md, encoding="utf-8")
    md_dir = tl.SCORE / "ticker_lookback"
    md_dir.mkdir(parents=True, exist_ok=True)
    (md_dir / f"{slug}.md").write_text(md, encoding="utf-8")
    web_dir = tl.ROOT / "dashboard" / "ticker-lookback"
    web_dir.mkdir(parents=True, exist_ok=True)
    page = render_html(payload)
    (web_dir / f"{slug}.html").write_text(page, encoding="utf-8")
    (web_dir / "index.html").write_text(page, encoding="utf-8")
    print(md[:12000])
    print(f"[ticker-lookback] wrote {js}")
    print(f"[ticker-lookback] wrote {tl.DAILY / 'ticker_lookback.md'}")
    print(f"[ticker-lookback] phone page dashboard/ticker-lookback/{slug}.html")
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", required=True, help="comma-separated, any listed name")
    ap.add_argument("--from-date", default="", help="optional YYYY-MM-DD")
    ap.add_argument("--to-date", default="", help="optional YYYY-MM-DD")
    args = ap.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        raise SystemExit("pass --tickers TEM,ELF")
    run(tickers, from_date=args.from_date or None, to_date=args.to_date or None)


if __name__ == "__main__":
    main()
