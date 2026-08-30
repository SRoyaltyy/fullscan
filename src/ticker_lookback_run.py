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
        card["price_changes"] = tl.trailing_returns(
            t, sess["date"], sessions=sessions,
            current_finviz=(sess.get("finviz") or {}).get(t),
        )
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
    L = ["# Ticker lookback", "", f"_Generated {payload['generated_at']}_", ""]
    cols = " | ".join(label for _, label in tl.BOX_COLS)
    bars = "|".join(["---"] * (6 + len(tl.BOX_COLS)))
    for rec in payload["names"]:
        L += [f"## {rec['ticker']}", "",
              f"| Date | Price | 1d | 3d | 1w | Class | {cols} |",
              f"|{bars}|"]
        for d in rec["days"]:
            pc = d.get("price_changes") or {}
            def pval(key):
                v = pc.get(key)
                if v is None:
                    return "—"
                return f"{v:+.2f}%" if key != "price" else f"${v:,.2f}"
            boxes = d.get("boxes") or {}
            cells = " | ".join(
                _icon(boxes.get(k, "missing")) for k, _ in tl.BOX_COLS)
            L.append(
                f"| {d['date']} | {pval('price')} | {pval('1d')} | "
                f"{pval('3d')} | {pval('1w')} | {d.get('class')} | {cells} |"
            )
        L.append("")
    return "\n".join(L) + "\n"


def _slug(tickers):
    return "-".join(tl._tick(t) for t in tickers if tl._tick(t)).lower()


def render_html(payload):
    sections = []
    for rec in payload["names"]:
        rows = []
        for day in rec["days"]:
            pc = day.get("price_changes") or {}
            def val(key):
                v = pc.get(key)
                if v is None:
                    return "—"
                return f"{v:+.2f}%" if key != "price" else f"${v:,.2f}"
            cells = "".join(
                f'<td class="{html.escape((day.get("boxes") or {}).get(k, "missing"))}">'
                f'{_icon((day.get("boxes") or {}).get(k, "missing"))}</td>'
                for k, _ in tl.BOX_COLS
            )
            rows.append(
                f"<tr><th>{html.escape(day['date'])}</th>"
                f"<td>{val('price')}</td><td>{val('1d')}</td>"
                f"<td>{val('3d')}</td><td>{val('1w')}</td>{cells}</tr>"
            )
        factor_headers = "".join(
            f"<th>{html.escape(label)}</th>" for _, label in tl.BOX_COLS)
        sections.append(f"""
<section class="ticker" id="{html.escape(rec['ticker'])}">
 <h2>{html.escape(rec['ticker'])}</h2>
 <div class="sheet"><table>
 <thead><tr><th>Date</th><th>Price</th><th>1d</th><th>3d</th><th>1w</th>{factor_headers}</tr></thead>
 <tbody>{''.join(rows)}</tbody></table></div>
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
.sheet{{overflow-x:auto;border:1px solid var(--line);border-radius:12px;margin-bottom:22px}}
table{{border-collapse:separate;border-spacing:0;min-width:900px;width:100%;background:var(--card)}}
th,td{{padding:10px 9px;text-align:center;border-bottom:1px solid var(--line);white-space:nowrap}}
thead th{{position:sticky;top:0;background:#17213a}}tbody th{{position:sticky;left:0;background:#17213a;text-align:left}}
td.good{{background:#123d2c}}td.bad{{background:#4b2028}}td.neutral{{background:#473e1d}}td.missing{{background:#23283a}}
@media(max-width:600px){{main{{padding:8px}}th,td{{padding:9px 7px;font-size:13px}}}}
</style></head><body><main>
<h1>Ticker lookback</h1>
<p>🟢 positive · 🟡 neutral · 🔴 negative · ⬛ missing</p>
<nav>{nav}</nav>{''.join(sections)}
</main></body></html>"""


def write_xlsx(payload, path):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill

    fills = {
        "good": PatternFill("solid", fgColor="63BE7B"),
        "neutral": PatternFill("solid", fgColor="FFEB84"),
        "bad": PatternFill("solid", fgColor="F8696B"),
        "missing": PatternFill("solid", fgColor="808080"),
    }
    wb = Workbook()
    wb.remove(wb.active)
    headers = ["Date", "Price", "1d", "3d", "1w"] + [
        label for _, label in tl.BOX_COLS]
    for rec in payload["names"]:
        ws = wb.create_sheet(rec["ticker"][:31])
        ws.freeze_panes = "B2"
        ws.append(headers)
        for cell in ws[1]:
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="1F4E78")
            cell.alignment = Alignment(horizontal="center")
        for day in rec["days"]:
            pc = day.get("price_changes") or {}
            ws.append([
                day["date"], pc.get("price"), pc.get("1d"),
                pc.get("3d"), pc.get("1w"),
            ] + [
                _icon((day.get("boxes") or {}).get(k, "missing"))
                for k, _ in tl.BOX_COLS
            ])
            row = ws.max_row
            for offset, (key, _label) in enumerate(tl.BOX_COLS, start=6):
                tone = (day.get("boxes") or {}).get(key, "missing")
                ws.cell(row, offset).fill = fills.get(tone, fills["missing"])
                ws.cell(row, offset).alignment = Alignment(horizontal="center")
            for col in (3, 4, 5):
                ws.cell(row, col).number_format = '0.00"%"'
        ws.column_dimensions["A"].width = 13
        ws.column_dimensions["B"].width = 12
        for col in range(3, len(headers) + 1):
            ws.column_dimensions[chr(64 + col)].width = 9
        ws.auto_filter.ref = ws.dimensions
    wb.save(path)


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
    xlsx_dir = tl.SCORE / "ticker_lookback"
    xlsx_path = xlsx_dir / f"{slug}.xlsx"
    write_xlsx(payload, xlsx_path)
    print(md[:12000])
    print(f"[ticker-lookback] wrote {js}")
    print(f"[ticker-lookback] wrote {tl.DAILY / 'ticker_lookback.md'}")
    print(f"[ticker-lookback] phone page dashboard/ticker-lookback/{slug}.html")
    print(f"[ticker-lookback] spreadsheet {xlsx_path}")
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
