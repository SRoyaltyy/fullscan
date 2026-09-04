"""Comparison board for every paper / combine book we have shipped.

Reads existing artifacts (no full sweep). Hard-red flatten is the live
combine. Stitch books and Excel stay labeled so a 17% curve-stitch is
not sat next to a 12% fill-level sleeve as if they were the same account.

CLI: python -m src.strategy_board [--write]
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCOREBOARD = ROOT / "03_scoreboard"
DASH_DIR = ROOT / "dashboard" / "strategy-board"
OUT_DIR = ROOT / "data" / "strategy_board"
PAPER_EQ = ROOT / "data" / "paper" / "equity_curve.csv"
PAPER_MD = SCOREBOARD / "PAPER_TRADING.md"
MERGE_SWEEP = ROOT / "data" / "sleeve_merge" / "sweep.json"
MERGE_STATE = ROOT / "data" / "sleeve_merge" / "state.json"
MERGE_CURVE = ROOT / "data" / "sleeve_merge" / "equity_curve.csv"
COMBINE_BT = ROOT / "data" / "sleeve_combine" / "bt.json"
MOVER_STATE = ROOT / "data" / "mover_paper" / "state.json"
MOVER_CURVE = ROOT / "data" / "mover_paper" / "equity_curve.csv"
BOOK_STATE = ROOT / "data" / "book_paper" / "state.json"
BOOK_CURVE = ROOT / "data" / "book_paper" / "equity_curve.csv"
EXCEL_SUG = ROOT / "excel_bot" / "suggestions" / "suggestions.csv"

WINDOW = ("2026-08-13", "2026-09-03")

# PR-shipped books that are not the current file on disk (superseded
# stitches / rejected fallbacks). Numbers are the published headlines.
SHIPPED = [
    {
        "id": "stitch_empty_gap",
        "name": "Empty BUY list + skip → live 2w_size",
        "family": "mover stitch",
        "pr": 67,
        "integrity": "stitch",
        "ret_pct": 17.5,
        "max_dd_pct": 0.63,
        "capital": 100_000,
        "trades": 40,
        "hit": 0.50,
        "href": "../mover-paper/",
        "note": "Daily 2w_size marks on empty non-neg mornings and S<+1. Not one fill clock.",
        "live": False,
    },
    {
        "id": "stitch_skip_io",
        "name": "Skip-day → live 2w_size",
        "family": "mover stitch",
        "pr": 64,
        "integrity": "stitch",
        "ret_pct": 13.89,
        "max_dd_pct": None,
        "capital": 100_000,
        "trades": None,
        "hit": None,
        "href": "../mover-paper/",
        "note": "S<+1 takes already-on 2w_size. Superseded by PR #67 empty-gap add.",
        "live": False,
    },
    {
        "id": "soft_red_1d_fallback",
        "name": "1d mover × soft-red 1d .io",
        "family": "mover stitch",
        "pr": 62,
        "integrity": "fill",
        "ret_pct": 1.55,
        "max_dd_pct": 0.66,
        "capital": 100_000,
        "trades": 30,
        "hit": 0.433,
        "href": "../sleeve-combine/",
        "note": "Rejected: 09-03 .io green was already-on 2w, not a new 1d ticket.",
        "live": False,
    },
    {
        "id": "mover_paper_leaky",
        "name": "Mover paper v2 (old sim)",
        "family": "mover",
        "pr": 55,
        "integrity": "leak",
        "ret_pct": 9.3,
        "max_dd_pct": 0.12,
        "capital": 100_000,
        "trades": 29,
        "hit": 0.621,
        "href": "../mover-paper/",
        "note": "Same-day close→open recycle leak. Fill-level gated mover is +1.55%.",
        "live": False,
    },
]


def _num(v):
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace("%", "").replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return None


def _row(**kw) -> dict:
    base = {
        "id": "", "name": "", "family": "", "pr": None, "integrity": "",
        "ret_pct": None, "max_dd_pct": None, "capital": None,
        "trades": None, "hit": None, "final_equity": None,
        "href": "", "note": "", "live": False, "curve": [],
    }
    base.update(kw)
    return base


def _curve_from_csv(path: Path, date_key="date", eq_key="equity",
                    sleeve=None, sleeve_key="sleeve",
                    start=10_000.0) -> list[dict]:
    if not path.is_file():
        return []
    out = []
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if sleeve and row.get(sleeve_key) != sleeve:
                continue
            d = (row.get(date_key) or "")[:10]
            eq = _num(row.get(eq_key))
            if not d or eq is None:
                continue
            out.append({"date": d, "equity": eq,
                        "idx": round(100.0 * eq / start, 4)})
    return out


def io_sleeves() -> list[dict]:
    last: dict[str, float] = {}
    first: dict[str, float] = {}
    if PAPER_EQ.is_file():
        with PAPER_EQ.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                s = row.get("sleeve") or ""
                eq = _num(row.get("equity"))
                if not s or eq is None:
                    continue
                first.setdefault(s, eq)
                last[s] = eq
    rows = []
    for sleeve, eq in sorted(last.items()):
        start = 10_000.0
        ret = round(100.0 * (eq / start - 1.0), 2)
        rows.append(_row(
            id=f"io_{sleeve}",
            name=f".io {sleeve}",
            family=".io paper",
            pr=None,
            integrity="follow_book",
            ret_pct=ret,
            capital=10_000,
            final_equity=eq,
            href="../",
            note="Close fill, follow-the-book, $10k/sleeve, Futubull fees.",
            live=(sleeve == "2w_size"),
            curve=_curve_from_csv(PAPER_EQ, sleeve=sleeve, start=start),
        ))
    return rows


def mover_books() -> list[dict]:
    rows = [dict(r) for r in SHIPPED]
    if MOVER_STATE.is_file():
        st = json.loads(MOVER_STATE.read_text(encoding="utf-8"))
        rows.append(_row(
            id="mover_paper_live",
            name="Mover paper (current stitch)",
            family="mover stitch",
            pr=67,
            integrity="stitch",
            ret_pct=st.get("total_ret_pct"),
            max_dd_pct=st.get("max_dd_pct"),
            capital=st.get("params", {}).get("capital") or 100_000,
            trades=st.get("n_trades"),
            hit=st.get("hit"),
            final_equity=st.get("final_equity"),
            href="../mover-paper/",
            note="Current mover-paper page: empty-gap + skip-day 2w marks.",
            live=False,
            curve=_curve_from_csv(MOVER_CURVE, start=100_000),
        ))
    return rows


def combine_bt() -> list[dict]:
    if not COMBINE_BT.is_file():
        return []
    doc = json.loads(COMBINE_BT.read_text(encoding="utf-8"))
    rows = []
    for r in doc.get("results") or []:
        hold, mode = r.get("hold"), r.get("mode")
        live = hold == "3d" and mode == "io_boost"
        rows.append(_row(
            id=f"combine_{hold}_{mode}",
            name=f"Combine {hold} {mode}",
            family="sleeve combine",
            pr=None,
            integrity="fill",
            ret_pct=r.get("total_ret_pct"),
            max_dd_pct=r.get("max_dd_pct"),
            capital=doc.get("capital") or 100_000,
            trades=r.get("n_trades"),
            hit=r.get("hit"),
            final_equity=r.get("final_equity"),
            href="../sleeve-combine/",
            note="Matched hold, 09:30 vs 16:00 cash clock, Futubull.",
            live=live,
        ))
    return rows


def merge_books() -> list[dict]:
    rows = []
    live_name = "flatten_robust"
    sweep = []
    if MERGE_SWEEP.is_file():
        doc = json.loads(MERGE_SWEEP.read_text(encoding="utf-8"))
        live_name = doc.get("live") or live_name
        sweep = doc.get("rows") or []
    elif MERGE_STATE.is_file():
        st = json.loads(MERGE_STATE.read_text(encoding="utf-8"))
        sweep = [{"name": (st.get("policy") or {}).get("name"),
                  **(st.get("stats") or {})}]
    curve = _curve_from_csv(MERGE_CURVE, start=100_000)
    for r in sweep:
        name = r.get("name") or ""
        is_live = bool(r.get("live") or name == live_name)
        rows.append(_row(
            id=f"merge_{name}",
            name=f"Flatten {name}" if not name.startswith("flatten") else name,
            family="sleeve merge",
            pr=72 if name == "flatten_hard_red" else (66 if name == "flatten_switch_recycle" else None),
            integrity="fill",
            ret_pct=r.get("total_ret_pct"),
            max_dd_pct=r.get("max_dd_pct"),
            capital=100_000,
            trades=r.get("n_trades"),
            hit=r.get("hit"),
            final_equity=r.get("final_equity"),
            href="../sleeve-merge/",
            note=("LIVE: 3d robust size book + flatten clock; S≤−3 no new buys."
                  if is_live else
                  "One-account flatten-switch, Futubull, leftover cash."),
            live=is_live,
            curve=curve if is_live else [],
        ))
    return rows


def book_paper() -> list[dict]:
    if not BOOK_STATE.is_file():
        return []
    st = json.loads(BOOK_STATE.read_text(encoding="utf-8"))
    return [_row(
        id="book_paper_1w",
        name="Book paper 1w (gated close)",
        family="book paper",
        pr=57,
        integrity="fill",
        ret_pct=st.get("total_ret_pct"),
        max_dd_pct=st.get("max_dd_pct"),
        capital=st.get("params", {}).get("capital") or 100_000,
        trades=st.get("n_trades"),
        hit=st.get("hit"),
        final_equity=st.get("final_equity"),
        href="../book-paper/",
        note="1d book BUY list, close entry, 1w hold, S≥+1 gate.",
        curve=_curve_from_csv(BOOK_CURVE, start=100_000),
    )]


def excel_books() -> list[dict]:
    if not EXCEL_SUG.is_file():
        return []
    from src.sleeve_combine import _parse_ret
    by: dict[str, list[float]] = defaultdict(list)
    with EXCEL_SUG.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            strat = row.get("strategy") or "excel"
            r = _parse_ret(row.get("ret_vs_open"))
            if r is None:
                continue
            by[strat].append(r)
    rows = []
    all_rets = [x for xs in by.values() for x in xs]
    if all_rets:
        win = sum(1 for x in all_rets if x > 0) / len(all_rets)
        mean = 100.0 * sum(all_rets) / len(all_rets)
        rows.append(_row(
            id="excel_all",
            name="Excel live ledger (all cards)",
            family="excel",
            integrity="confirm",
            ret_pct=round(mean, 2),
            trades=len(all_rets),
            hit=round(win, 3),
            href="../",
            note="Confirm-only. Not a capital book. Mean vs entry open.",
        ))
    for strat, rets in sorted(by.items(), key=lambda kv: -len(kv[1])):
        win = sum(1 for x in rets if x > 0) / len(rets)
        mean = 100.0 * sum(rets) / len(rets)
        rows.append(_row(
            id=f"excel_{strat}",
            name=f"Excel {strat}",
            family="excel",
            integrity="confirm",
            ret_pct=round(mean, 2),
            trades=len(rets),
            hit=round(win, 3),
            note="Confirm-only searchlight. Median trade ~0.",
        ))
    return rows


def collect() -> list[dict]:
    rows = []
    rows += merge_books()
    rows += mover_books()
    rows += combine_bt()
    rows += io_sleeves()
    rows += book_paper()
    rows += excel_books()
    # de-dupe by id, keep first (merge/live first)
    seen = set()
    out = []
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        out.append(r)
    out.sort(key=lambda r: (
        0 if r.get("live") and r.get("family") == "sleeve merge" else 1,
        0 if r.get("integrity") == "fill" else 1,
        -(r.get("ret_pct") if isinstance(r.get("ret_pct"), (int, float)) else -999),
    ))
    return out


def _esc(s) -> str:
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def render(rows: list[dict]) -> str:
    live = next((r for r in rows if r.get("live")
                 and r.get("family") == "sleeve merge"), None)
    live_ret = f"{live['ret_pct']:+.2f}%" if live and live.get("ret_pct") is not None else "—"
    families = sorted({r["family"] for r in rows if r.get("family")})
    table = []
    for r in rows:
        ret = r.get("ret_pct")
        rcls = "good" if isinstance(ret, (int, float)) and ret > 0 else (
            "bad" if isinstance(ret, (int, float)) and ret < 0 else "")
        live_tag = " LIVE" if r.get("live") else ""
        href = r.get("href") or ""
        name = _esc(r.get("name"))
        if href:
            name = f"<a href='{_esc(href)}'>{name}</a>"
        pr = f"#{r['pr']}" if r.get("pr") else "—"
        hit = f"{100 * r['hit']:.1f}%" if isinstance(r.get("hit"), (int, float)) else "—"
        dd = f"{r['max_dd_pct']:.2f}%" if isinstance(r.get("max_dd_pct"), (int, float)) else "—"
        trades = r.get("trades") if r.get("trades") is not None else "—"
        cap = (f"${r['capital']:,.0f}" if r.get("capital") else "—")
        table.append(
            f"<tr data-family='{_esc(r.get('family'))}' "
            f"data-integrity='{_esc(r.get('integrity'))}'>"
            f"<td class='name'>{name}<span class='live'>{live_tag}</span></td>"
            f"<td>{_esc(r.get('family'))}</td>"
            f"<td>{pr}</td>"
            f"<td class='tag {_esc(r.get('integrity'))}'>{_esc(r.get('integrity'))}</td>"
            f"<td class='{rcls}'>{ret:+.2f}%</td>"
            f"<td>{dd}</td>"
            f"<td>{trades}</td>"
            f"<td>{hit}</td>"
            f"<td>{cap}</td>"
            f"<td class='why'>{_esc(r.get('note'))}</td></tr>"
            if isinstance(ret, (int, float)) else
            f"<tr data-family='{_esc(r.get('family'))}' "
            f"data-integrity='{_esc(r.get('integrity'))}'>"
            f"<td class='name'>{name}</td>"
            f"<td>{_esc(r.get('family'))}</td><td>{pr}</td>"
            f"<td class='tag {_esc(r.get('integrity'))}'>{_esc(r.get('integrity'))}</td>"
            f"<td>—</td><td>{dd}</td><td>{trades}</td><td>{hit}</td>"
            f"<td>{cap}</td><td class='why'>{_esc(r.get('note'))}</td></tr>"
        )
    chips = "".join(
        f"<button type='button' data-fam='{_esc(f)}'>{_esc(f)}</button>"
        for f in families)
    # overlay curves: live merge + io 2w + mover stitch + book
    want = {"merge_flatten_hard_red", "io_2w_size", "mover_paper_live",
            "book_paper_1w", "merge_flatten_switch_recycle"}
    series = []
    colors = ["#4ade80", "#60a5fa", "#fbbf24", "#f472b6", "#a78bfa"]
    for r in rows:
        if r["id"] not in want or not r.get("curve"):
            continue
        series.append({
            "id": r["id"], "name": r["name"],
            "color": colors[len(series) % len(colors)],
            "pts": r["curve"],
        })
    payload = json.dumps({"series": series}, separators=(",", ":"))
    return f"""<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Strategy board — every shipped book</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui}}
main{{max-width:1280px;margin:auto;padding:16px}}h1,h2{{margin:.4em 0}}
.muted{{color:var(--muted)}}a{{color:#93c5fd}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin:14px 0}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px}}
.card b{{display:block;font-size:22px;margin-top:4px}}
.chips{{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0}}
.chips button{{background:#17213a;color:var(--text);border:1px solid var(--line);border-radius:999px;padding:6px 12px;cursor:pointer}}
.chips button.on,.chips button:hover{{border-color:#93c5fd}}
.sheet{{overflow-x:auto;border:1px solid var(--line);border-radius:12px;margin:14px 0}}
table{{border-collapse:separate;border-spacing:0;width:100%;background:var(--card)}}
th,td{{padding:7px 8px;text-align:center;border-bottom:1px solid var(--line);white-space:nowrap}}
thead th{{position:sticky;top:0;background:#17213a;cursor:pointer}}
td.name{{text-align:left}}td.why{{text-align:left;white-space:normal;max-width:280px;font-size:12px;color:var(--muted)}}
td.good{{color:#4ade80}}td.bad{{color:#f87171}}
.tag{{font-size:11px;text-transform:uppercase}}
.tag.fill{{color:#4ade80}}.tag.stitch{{color:#60a5fa}}.tag.follow_book{{color:#fbbf24}}
.tag.confirm{{color:#c084fc}}.tag.leak{{color:#f87171}}
.live{{color:#4ade80;font-weight:700}}
svg text{{fill:var(--muted)}}
</style></head><body><main>
<h1>Strategy board</h1>
<p class="muted">
<a href="../">.io paper</a> ·
<a href="../sleeve-merge/">live combine</a> ·
<a href="../mover-paper/">mover paper</a> ·
<a href="../sleeve-combine/">sleeve combine</a> ·
<a href="../book-paper/">book paper</a>
</p>
<p class="muted">Window {WINDOW[0]} → {WINDOW[1]}. Live production book is
<b>flatten_robust</b> (3d size book + flatten clock; S ≤ −3: no new buys). Returns are
not interchangeable: <b>fill</b> is one Futubull cash account,
<b>stitch</b> is a daily-mark overlay, <b>follow_book</b> is the $10k .io
sleeves, <b>confirm</b> is Excel (not capital), <b>leak</b> is a known
same-day recycle.</p>
<div class="cards">
<div class="card">Live method<b>flatten_robust</b></div>
<div class="card">Live return<b>{live_ret}</b></div>
<div class="card">Books on this page<b>{len(rows)}</b></div>
<div class="card">Families<b>{len(families)}</b></div>
</div>
<div class="chips" id="chips">
<button type="button" data-fam="" class="on">all</button>
{chips}
<button type="button" data-int="fill">fill only</button>
</div>
<div id="chart"></div>
<h2>Every shipped book</h2>
<div class="sheet"><table id="T">
<thead><tr>
<th data-k="name">Book</th><th data-k="family">Family</th><th data-k="pr">PR</th>
<th data-k="integrity">Integrity</th><th data-k="ret">Return</th>
<th data-k="dd">Max DD</th><th data-k="trades">Trades</th><th data-k="hit">Win</th>
<th data-k="cap">Capital</th><th>Note</th>
</tr></thead>
<tbody>{''.join(table)}</tbody>
</table></div>
<script>
const D = {payload};
const chips = document.getElementById('chips');
const rows = [...document.querySelectorAll('#T tbody tr')];
chips.addEventListener('click', e => {{
  const b = e.target.closest('button'); if (!b) return;
  chips.querySelectorAll('button').forEach(x => x.classList.remove('on'));
  b.classList.add('on');
  const fam = b.dataset.fam, integ = b.dataset.int;
  rows.forEach(tr => {{
    const ok = integ ? tr.dataset.integrity === integ
      : (!fam || tr.dataset.family === fam);
    tr.style.display = ok ? '' : 'none';
  }});
}});
function draw() {{
  const host = document.getElementById('chart');
  const series = D.series || [];
  if (!series.length) return;
  const dates = [...new Set(series.flatMap(s => s.pts.map(p => p.date)))].sort();
  const W=960,H=280,P=40;
  const vals = series.flatMap(s => s.pts.map(p => p.idx));
  const lo = Math.min(100, ...vals), hi = Math.max(100, ...vals);
  const rng = (hi-lo)||1;
  const X = i => P + (W-2*P)*i/Math.max(dates.length-1,1);
  const Y = v => H-P - (H-2*P)*(v-lo)/rng;
  const ix = Object.fromEntries(dates.map((d,i)=>[d,i]));
  let polylines = '';
  series.forEach(s => {{
    const pts = s.pts.filter(p => p.date in ix)
      .map(p => X(ix[p.date]).toFixed(1)+','+Y(p.idx).toFixed(1)).join(' ');
    polylines += `<polyline points="${{pts}}" fill="none" stroke="${{s.color}}" stroke-width="2"/>`;
  }});
  const legend = series.map(s =>
    `<span style="color:${{s.color}};margin-right:12px">${{s.name}}</span>`).join('');
  host.innerHTML = `<svg viewBox="0 0 ${{W}} ${{H}}" width="100%" height="${{H}}">
    <line x1="${{P}}" y1="${{Y(100)}}" x2="${{W-P}}" y2="${{Y(100)}}" stroke="#5b6b8c" stroke-dasharray="4 4"/>
    ${{polylines}}
    <text x="${{P}}" y="16">indexed 100 at start</text>
    <text x="${{P}}" y="${{H-8}}">${{dates[0]||''}}</text>
    <text x="${{W-P}}" y="${{H-8}}" text-anchor="end">${{dates[dates.length-1]||''}}</text>
  </svg><p class="muted">${{legend}}</p>`;
}}
draw();
</script>
<p class="muted">Generated {datetime.now().isoformat(timespec='seconds')} ·
machine data/strategy_board/catalog.json · write-up 03_scoreboard/STRATEGY_BOARD.md</p>
</main></body></html>
"""


def write_md(rows: list[dict]) -> str:
    live = next((r for r in rows if r.get("live")
                 and r.get("family") == "sleeve merge"), None)
    lines = [
        "# Strategy board — every shipped book",
        "",
        f"_Generated {datetime.now().isoformat(timespec='seconds')} — "
        f"{WINDOW[0]} → {WINDOW[1]}_",
        "",
        "Live production method is **`flatten_robust`**: 3d size-book "
        "selection (not raw 2w_size), 3-session recycle, same flatten-switch "
        "clock, plus S ≤ −3 blocks new buys. Working lots and due 1d exits "
        "stay on.",
        "",
        f"Live headline: **{(live or {}).get('ret_pct', '—')}%** "
        f"({(live or {}).get('name')}).",
        "",
        "Integrity: **fill** = one Futubull cash account. **stitch** = daily "
        "mark overlay (not one fill clock). **follow_book** = $10k .io paper "
        "sleeves. **confirm** = Excel, not capital. **leak** = known "
        "same-day recycle.",
        "",
        "| Live | Book | Family | PR | Integrity | Return | Max DD | Trades | Win | Cap |",
        "|---|---|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        ret = r.get("ret_pct")
        rs = f"{ret:+.2f}%" if isinstance(ret, (int, float)) else "—"
        dd = (f"{r['max_dd_pct']:.2f}%"
              if isinstance(r.get("max_dd_pct"), (int, float)) else "—")
        hit = (f"{100 * r['hit']:.1f}%"
               if isinstance(r.get("hit"), (int, float)) else "—")
        lines.append(
            f"| {'YES' if r.get('live') else ''} | {r.get('name')} | "
            f"{r.get('family')} | {('#' + str(r['pr'])) if r.get('pr') else '—'} | "
            f"{r.get('integrity')} | {rs} | {dd} | {r.get('trades') or '—'} | "
            f"{hit} | {r.get('capital') or '—'} |")
    lines += ["", "Dashboard: `dashboard/strategy-board/index.html`.", ""]
    return "\n".join(lines)


def write(rows: list[dict]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    slim = [{k: v for k, v in r.items() if k != "curve"} for r in rows]
    (OUT_DIR / "catalog.json").write_text(
        json.dumps({"generated": datetime.now().isoformat(timespec="seconds"),
                    "live": "flatten_hard_red", "window": list(WINDOW),
                    "n": len(slim), "rows": slim}, indent=2),
        encoding="utf-8")
    (DASH_DIR / "index.html").write_text(render(rows), encoding="utf-8")
    (SCOREBOARD / "STRATEGY_BOARD.md").write_text(write_md(rows), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    rows = collect()
    live = next((r for r in rows if r.get("live")
                 and r.get("family") == "sleeve merge"), None)
    print(f"[strategy-board] n={len(rows)} live="
          f"{(live or {}).get('name')} ret={(live or {}).get('ret_pct')}")
    if args.write:
        write(rows)
        print(f"[strategy-board] wrote {DASH_DIR / 'index.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
