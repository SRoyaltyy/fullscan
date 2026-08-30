"""Run: python -m src.ticker_lookback_run --tickers TEM,ELF,AAPL"""
from __future__ import annotations

import argparse
import html
import json
import os
from datetime import datetime

from . import ticker_lookback as tl
from . import ticker_lookback_cli as scan


def _icon(kind):
    kind = {"green": "good", "red": "bad", "yellow": "neutral"}.get(
        str(kind or "").lower(), kind)
    return tl.BOX_ICON.get(kind, tl.BOX_ICON["missing"])


def _price_tones(pc):
    pc = pc or {}
    return {k: tl.price_tone(pc.get(k)) for k in ("1d", "3d", "1w")}


def _attach_day_extras(card, ticker, sess, sessions):
    fv = (sess.get("finviz") or {}).get(ticker)
    card["price_changes"] = tl.forward_price_changes(
        ticker, sess["date"], sessions=sessions, current_finviz=fv)
    card["forward_returns"] = tl.forward_returns(
        ticker, sess["date"], sessions=sessions, current_finviz=fv)
    card["price_tones"] = _price_tones(card["price_changes"])
    return card


def scan_ticker(ticker, sessions=None, idx=None):
    idx = idx or tl.build_index()
    t = tl._tick(ticker)
    days, recommended, green_days = [], [], []
    sessions = sessions if sessions is not None else idx["sessions"]
    for sess in sessions:
        card = scan._scan_session(sess, t)
        if card is None:
            card = {
                "date": sess["date"], "ticker": t, "class": "no_data",
                "sources": [], "boxes": {k: "missing" for k, _ in tl.BOX_COLS},
                "artifacts_that_day": sess["has"],
            }
        _attach_day_extras(card, t, sess, sessions)
        days.append(card)
        if card.get("buy_ranks"):
            recommended.append({
                "date": card["date"],
                "horizons": list(card["buy_ranks"].keys()),
                "ranks": card["buy_ranks"],
            })
        if card.get("independent_green", {}).get("green") or card.get("in_green_buy"):
            green_days.append(card["date"])
    tl.annotate_signal_improved(days)
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
        "asof": "09:30_et",
        "from_date": from_date, "to_date": to_date,
        "sessions": [
            {"date": s["date"], "has": s["has"], "n_book": s["n_book"],
             "n_join": s["n_join"], "n_finviz": s["n_finviz"],
             "n_ab": s["n_ab"], "n_peer": s["n_peer"]}
            for s in sessions
        ],
        "names": [scan_ticker(t, sessions=sessions, idx=idx) for t in names],
    }


def _fmt_price(pc, key):
    v = (pc or {}).get(key)
    if v is None:
        return "—"
    return f"{v:+.2f}%" if key != "price" else f"${v:,.2f}"


def _fmt_price_md(pc, tones, key):
    text = _fmt_price(pc, key)
    if key == "price" or text == "—":
        return text
    return f"{_icon((tones or {}).get(key) or tl.price_tone((pc or {}).get(key)))} {text}"


def _date_marks(day):
    marks = []
    if day.get("signal_improved"):
        marks.append("🔵")
    if day.get("signal_alarm"):
        marks.append("🚨")
    if day.get("zero_red"):
        marks.append("⚪")
    return "".join(marks)


def _date_label(day):
    marks = _date_marks(day)
    return f"{marks} {day['date']}".strip() if marks else day["date"]


def _date_classes(day):
    cls = []
    if day.get("signal_improved"):
        cls.append("better")
    if day.get("signal_alarm"):
        cls.append("alarm")
    if day.get("zero_red"):
        cls.append("clean")
    tone = (day.get("region") or {}).get("tone")
    if tone == "good":
        cls.append("reg-good")
    elif tone == "bad":
        cls.append("reg-bad")
    return " ".join(cls)


def _condition(day):
    cond = day.get("condition")
    if not cond:
        cond = tl.general_condition(day.get("boxes") or {})
    return cond


def _condition_text(cond):
    if not cond or cond.get("tone") == "missing" or not cond.get("n"):
        return "—"
    return f"{cond['good']}/{cond['neutral']}/{cond['bad']}"


def _condition_md(cond):
    text = _condition_text(cond)
    if text == "—":
        return text
    return f"{_icon(cond.get('tone'))} {text}"


def _region(day):
    reg = day.get("region")
    if not reg:
        reg = tl.color_region(day.get("boxes") or {})
    return reg


def _region_text(reg):
    tone = (reg or {}).get("tone")
    if tone in (None, "missing", "thin"):
        return "—"
    g, r = (reg or {}).get("good", 0), (reg or {}).get("bad", 0)
    return f"{g}-{r}"


def _region_md(reg):
    text = _region_text(reg)
    if text == "—":
        return text
    return f"{_icon((reg or {}).get('tone'))} {text}"


def render_md(payload):
    L = ["# Ticker lookback", "", f"_Generated {payload['generated_at']}_",
         "", "_As of 09:30 ET: overnight tape + that morning's pre-open packet. "
         "Same-day stock book and post-close Finviz do not color the factor boxes. "
         "Price / +1d / +3d / +1w are session-close outcomes._", ""]
    if payload.get("random"):
        L += [f"_Random {len(payload['names'])} names, "
              f"mcap > $100M, avg vol > 500K_", ""]
    L += ["_🔵 = vs prior session: no cell worse and at least one better, "
          "or factor points jumped by ≥3 (red=1, yellow=2, green=3)_",
          "_🚨 = purely worse vs prior session (no cell better, at least one worse)_",
          "_⚪ = no red factor cells that day_",
          "_Cond = G/Y/R tally; green or red when that color is the majority_",
          "_Reg = green vs red cell mass (yellows ignored). Tags change meaning "
          "on a mismatch: 🔵 on a red row has been the useful turn; 🔵 on a "
          "green row has been late. 🚨 on a still-green row has been the "
          "first-crack 1d drop; 🚨 on an already-red row meant the bounce "
          "did not show up yet._", ""]
    cols = " | ".join(label for _, label in tl.BOX_COLS)
    bars = "|".join(["---"] * (8 + len(tl.BOX_COLS)))
    for rec in payload["names"]:
        L += [f"## {rec['ticker']}", "",
              f"| Date | Price | +1d | +3d | +1w | Class | Cond | Reg | {cols} |",
              f"|{bars}|"]
        for d in rec["days"]:
            pc = d.get("price_changes") or {}
            tones = d.get("price_tones") or _price_tones(pc)
            boxes = d.get("boxes") or {}
            cells = " | ".join(
                _icon(boxes.get(k, "missing")) for k, _ in tl.BOX_COLS)
            date = _date_label(d)
            L.append(
                f"| {date} | {_fmt_price(pc, 'price')} | "
                f"{_fmt_price_md(pc, tones, '1d')} | "
                f"{_fmt_price_md(pc, tones, '3d')} | "
                f"{_fmt_price_md(pc, tones, '1w')} | {d.get('class')} | "
                f"{_condition_md(_condition(d))} | "
                f"{_region_md(_region(d))} | {cells} |"
            )
        L.append("")
    return "\n".join(L) + "\n"


def _slug(tickers, random_pick=False):
    body = "-".join(tl._tick(t) for t in tickers if tl._tick(t)).lower()
    if random_pick:
        return f"random-{body}" if body else "random"
    return body


def render_html(payload):
    sections = []
    for rec in payload["names"]:
        rows = []
        for day in rec["days"]:
            pc = day.get("price_changes") or {}
            tones = day.get("price_tones") or _price_tones(pc)
            cells = "".join(
                f'<td class="{html.escape((day.get("boxes") or {}).get(k, "missing"))}">'
                f'{_icon((day.get("boxes") or {}).get(k, "missing"))}</td>'
                for k, _ in tl.BOX_COLS
            )
            date_cls = _date_classes(day)
            cond = _condition(day)
            reg = _region(day)
            price_tds = "".join(
                f'<td class="{html.escape(tones.get(key, "missing"))}">'
                f'{_fmt_price(pc, key)}</td>'
                for key in ("1d", "3d", "1w")
            )
            rows.append(
                f'<tr><th class="{html.escape(date_cls)}">'
                f'{html.escape(_date_label(day))}</th>'
                f"<td>{_fmt_price(pc, 'price')}</td>{price_tds}"
                f'<td class="{html.escape(cond.get("tone", "missing"))}">'
                f'{html.escape(_condition_text(cond))}</td>'
                f'<td class="{html.escape(reg.get("tone", "missing"))}">'
                f'{html.escape(_region_text(reg))}</td>{cells}</tr>'
            )
        factor_headers = "".join(
            f"<th>{html.escape(label)}</th>" for _, label in tl.BOX_COLS)
        sections.append(f"""
<section class="ticker" id="{html.escape(rec['ticker'])}">
 <h2>{html.escape(rec['ticker'])}</h2>
 <div class="sheet"><table>
 <thead><tr><th>Date</th><th>Price</th><th>+1d</th><th>+3d</th><th>+1w</th><th>Cond</th><th>Reg</th>{factor_headers}</tr></thead>
 <tbody>{''.join(rows)}</tbody></table></div>
</section>""")
    nav = "".join(
        f'<a href="#{html.escape(r["ticker"])}">{html.escape(r["ticker"])}</a>'
        for r in payload["names"]
    )
    picked = ", ".join(html.escape(r["ticker"]) for r in payload["names"])
    random_note = (
        f'<p class="muted">Random draw: {picked} · mcap &gt; $100M · avg vol &gt; 500K</p>'
        if payload.get("random") else ""
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
tbody th.better{{background:#1d4ed8;color:#edf2ff}}
tbody th.alarm{{box-shadow:inset 3px 0 0 #f97316}}
tbody th.alarm:not(.better):not(.clean){{background:#4b2028;color:#edf2ff}}
tbody th.clean{{box-shadow:inset 3px 0 0 #f8fafc}}
tbody th.clean:not(.better){{background:#e8eef7;color:#0b1020}}
tbody th.reg-good{{box-shadow:inset 0 -3px 0 #22c55e}}
tbody th.reg-bad{{box-shadow:inset 0 -3px 0 #ef4444}}
.muted{{color:var(--muted)}}
@media(max-width:600px){{main{{padding:8px}}th,td{{padding:9px 7px;font-size:13px}}}}
</style></head><body><main>
<h1>Ticker lookback</h1>
<p>Factor colors = knowable by 09:30 ET (overnight tape + pre-open packet). Price / +1d / +3d / +1w are session-close outcomes.</p>
<p>🟢 up / positive · 🟡 flat · 🔴 down / negative · ⬛ missing · 🔵 improved or +≥3 pts · 🚨 purely worse · ⚪ no red · Cond = G/Y/R majority · Reg = green vs red mass</p>
<p class="muted">Tags change meaning by region: 🔵 on a red row has been the useful turn; 🔵 on a green row has been late. 🚨 on a still-green row has been the first-crack 1d drop; 🚨 on an already-red row meant the bounce did not show up yet.</p>
{random_note}<nav>{nav}</nav>{''.join(sections)}
</main></body></html>"""


def write_xlsx(payload, path):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill

    fills = {
        "good": PatternFill("solid", fgColor="63BE7B"),
        "neutral": PatternFill("solid", fgColor="FFEB84"),
        "bad": PatternFill("solid", fgColor="F8696B"),
        "missing": PatternFill("solid", fgColor="808080"),
        "better": PatternFill("solid", fgColor="5B9BD5"),
        "clean": PatternFill("solid", fgColor="FFFFFF"),
    }
    wb = Workbook()
    wb.remove(wb.active)
    headers = ["Date", "Price", "+1d", "+3d", "+1w", "Cond", "Reg"] + [
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
            tones = day.get("price_tones") or _price_tones(pc)
            cond = _condition(day)
            reg = _region(day)
            ws.append([
                _date_label(day), pc.get("price"), pc.get("1d"),
                pc.get("3d"), pc.get("1w"), _condition_text(cond),
                _region_text(reg),
            ] + [
                _icon((day.get("boxes") or {}).get(k, "missing"))
                for k, _ in tl.BOX_COLS
            ])
            row = ws.max_row
            date_cell = ws.cell(row, 1)
            if day.get("signal_improved"):
                date_cell.fill = fills["better"]
                date_cell.font = Font(bold=True, color="FFFFFF")
            elif day.get("signal_alarm"):
                date_cell.fill = fills["bad"]
                date_cell.font = Font(bold=True, color="FFFFFF")
            elif day.get("zero_red"):
                date_cell.fill = fills["clean"]
                date_cell.font = Font(bold=True, color="1F4E78")
            for col, key in ((3, "1d"), (4, "3d"), (5, "1w")):
                cell = ws.cell(row, col)
                cell.number_format = '0.00"%"'
                cell.fill = fills.get(tones.get(key, "missing"), fills["missing"])
                cell.alignment = Alignment(horizontal="center")
            cond_cell = ws.cell(row, 6)
            cond_cell.fill = fills.get(cond.get("tone", "missing"), fills["missing"])
            cond_cell.alignment = Alignment(horizontal="center")
            reg_cell = ws.cell(row, 7)
            reg_tone = reg.get("tone", "missing")
            if reg_tone == "thin":
                reg_tone = "missing"
            reg_cell.fill = fills.get(reg_tone, fills["missing"])
            reg_cell.alignment = Alignment(horizontal="center")
            for offset, (key, _label) in enumerate(tl.BOX_COLS, start=8):
                tone = (day.get("boxes") or {}).get(key, "missing")
                ws.cell(row, offset).fill = fills.get(tone, fills["missing"])
                ws.cell(row, offset).alignment = Alignment(horizontal="center")
        ws.column_dimensions["A"].width = 18
        ws.column_dimensions["B"].width = 12
        for col in range(3, len(headers) + 1):
            ws.column_dimensions[chr(64 + col)].width = 9
        ws.auto_filter.ref = ws.dimensions
    wb.save(path)


def resolve_tickers(raw, random_pick=False, n=tl.RANDOM_N, asof=None, seed=None):
    tokens = [t.strip() for t in (raw or "").split(",") if t.strip()]
    named = [t for t in tokens if t.lower() != "random"]
    want_random = bool(random_pick) or any(t.lower() == "random" for t in tokens)
    if want_random:
        return tl.pick_random_tickers(n=n, asof=asof, seed=seed), True
    return [tl._tick(t) for t in named if tl._tick(t)], False


def _emit_github_env(slug, tickers, random_pick=False):
    path = os.environ.get("GITHUB_ENV")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(f"SLUG={slug}\n")
        fh.write(f"LOOKBACK_TICKERS={','.join(tickers)}\n")
        fh.write(f"LOOKBACK_RANDOM={'true' if random_pick else 'false'}\n")


def run(tickers, from_date=None, to_date=None, random_pick=False):
    payload = scan_tickers(tickers, from_date=from_date, to_date=to_date)
    payload["random"] = bool(random_pick)
    tl.BOOK_DIR.mkdir(parents=True, exist_ok=True)
    tl.DAILY.mkdir(parents=True, exist_ok=True)
    tl.SCORE.mkdir(parents=True, exist_ok=True)
    slug = _slug(tickers, random_pick=random_pick)
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
    _emit_github_env(slug, tickers, random_pick=random_pick)
    print(md[:12000])
    print(f"[ticker-lookback] slug {slug}")
    print(f"[ticker-lookback] names {','.join(tickers)}")
    print(f"[ticker-lookback] wrote {js}")
    print(f"[ticker-lookback] wrote {tl.DAILY / 'ticker_lookback.md'}")
    print(f"[ticker-lookback] phone page dashboard/ticker-lookback/{slug}.html")
    print(f"[ticker-lookback] spreadsheet {xlsx_path}")
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default="",
                    help="comma-separated names, or 'random'")
    ap.add_argument("--random", action="store_true",
                    help="pick 10 stocks with mcap>$100M and avg vol>500K")
    ap.add_argument("--random-n", type=int, default=tl.RANDOM_N)
    ap.add_argument("--seed", default="", help="optional RNG seed for --random")
    ap.add_argument("--from-date", default="", help="optional YYYY-MM-DD")
    ap.add_argument("--to-date", default="", help="optional YYYY-MM-DD")
    args = ap.parse_args()
    seed = args.seed if args.seed else None
    tickers, random_pick = resolve_tickers(
        args.tickers, random_pick=args.random, n=args.random_n,
        asof=args.to_date or None, seed=seed)
    if not tickers:
        raise SystemExit("pass --tickers TEM,ELF or --random")
    run(tickers, from_date=args.from_date or None,
        to_date=args.to_date or None, random_pick=random_pick)


if __name__ == "__main__":
    main()
