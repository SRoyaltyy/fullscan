"""Sequential Factor Mine dashboard pack.

The page is built from ``daily_returns.csv`` and the saved per-day state
files. Designed-after sessions stay out of the real total. A recipe with
no fills is left blank. This module does not rescore and does not write
snapshots, ledgers, state files, or the fee file.
"""
from __future__ import annotations

import csv
import html
import json
from pathlib import Path

from .factor_mine import ROOT, recipe_created_on
from .factor_mine_rules import compound_pct

CSV_PATH = ROOT / "data" / "factor_mine" / "daily_returns.csv"
STATE_DIR = ROOT / "data" / "factor_mine" / "state"
REPORT_JSON = ROOT / "data" / "factor_mine" / "retro_report.json"
CHANGELOG_MD = ROOT / "03_scoreboard" / "FACTOR_MINE_CHANGELOG.md"
DASH_DIR = ROOT / "dashboard" / "factor-mine"
CAPITAL = 10000.0


def _num(cell) -> float | None:
    text = str(cell or "").strip()
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if number != number:
        return None
    return number


def _fires(cell) -> int:
    number = _num(cell)
    if number is None:
        return 0
    return int(number)


def _flag(cell) -> bool:
    return str(cell or "").strip().lower() == "true"


def _chain(rows: list[dict], key: str) -> float | None:
    values = [row[key] for row in rows if row.get(key) is not None]
    if not values:
        return None
    if sum(row["fires"] for row in rows) == 0:
        return None
    return compound_pct(values)


def _window(rows: list[dict]) -> dict:
    fires = sum(row["fires"] for row in rows)
    quiet = not rows or fires == 0
    return {
        "n": len(rows),
        "fires": fires,
        "futubull": None if quiet else _chain(rows, "futu"),
        "flat_15bp": None if quiet else _chain(rows, "flat"),
    }


def load_state_equity(state_dir: Path | None = None) -> dict[str, dict]:
    """Last saved equity for each recipe. Read-only."""
    root = Path(state_dir or STATE_DIR)
    out: dict[str, dict] = {}
    if not root.is_dir():
        return out
    for rec_dir in root.iterdir():
        if not rec_dir.is_dir():
            continue
        files = sorted(
            p for p in rec_dir.glob("*.json")
            if len(p.stem) >= 10 and p.stem[0].isdigit()
        )
        if not files:
            continue
        try:
            doc = json.loads(files[-1].read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(doc, dict):
            continue
        out[rec_dir.name] = {
            "date": str(doc.get("date") or files[-1].stem)[:10],
            "equity": doc.get("equity"),
            "mean": doc.get("mean"),
        }
    return out


def load_baselines(path: Path | None = None) -> dict:
    src = Path(path or REPORT_JSON)
    if not src.is_file():
        return {}
    try:
        doc = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    baselines = doc.get("baselines") if isinstance(doc, dict) else None
    return baselines if isinstance(baselines, dict) else {}


def pack_from_rows(rows: list[dict], *,
                   created_on=None,
                   state_equity: dict | None = None,
                   baselines: dict | None = None) -> dict:
    """One scoreboard row per recipe. Real totals omit designed_after days."""
    created_on = created_on or (lambda name, rec=None: recipe_created_on(name, rec or {}))
    by_name: dict[str, list[dict]] = {}
    for row in rows:
        name = str(row.get("recipe") or "").strip()
        if not name:
            continue
        by_name.setdefault(name, []).append(row)

    recipes = []
    for name in sorted(by_name):
        series = by_name[name]
        starts = [
            str(r.get("start_date") or "")[:10]
            for r in series if r.get("start_date")
        ]
        if not starts:
            continue
        start = min(starts)
        book = [r for r in series if str(r.get("start_date") or "")[:10] == start]
        book.sort(key=lambda r: str(r.get("D") or ""))
        created = str(created_on(name, {}) or "")[:10]
        days = []
        for row in book:
            date = str(row.get("D") or "")[:10]
            if not date:
                continue
            label = "designed_after" if date < created else "real"
            days.append({
                "date": date,
                "label": label,
                "timing_clean": _flag(row.get("timing_clean")),
                "futu": _num(row.get("net_ret_futubull")),
                "flat": _num(row.get("net_ret_15bp")),
                "fires": _fires(row.get("fires")),
                "status": str(row.get("day_status") or ""),
            })
        real = [d for d in days if d["label"] == "real"]
        designed = [d for d in days if d["label"] == "designed_after"]
        timing_real = [d for d in real if d["timing_clean"]]
        timing_designed = [d for d in designed if d["timing_clean"]]
        whole = _window(days)
        untestable = whole["fires"] == 0
        state = (state_equity or {}).get(name) or {}
        equity = state.get("equity")
        book_pct = None
        if not untestable and equity not in (None, ""):
            try:
                book_pct = round(100.0 * (float(equity) / CAPITAL - 1.0), 3)
            except (TypeError, ValueError, ZeroDivisionError):
                book_pct = None
        recipes.append({
            "name": name,
            "created_on": created,
            "start": start,
            "untestable": untestable,
            "n_real": len(real),
            "n_designed_after": len(designed),
            "real": _window(real),
            "designed_after": _window(designed),
            "timing_real": _window(timing_real),
            "timing_designed_after": _window(timing_designed),
            "book_pct": None if untestable else book_pct,
            "state_date": None if untestable else state.get("date"),
            "state_equity": None if untestable else equity,
            "days": [
                {
                    "date": d["date"],
                    "label": d["label"],
                    "timing_clean": d["timing_clean"],
                    "futu": None if untestable else d["futu"],
                    "flat": None if untestable else d["flat"],
                    "fires": d["fires"],
                    "status": d["status"],
                }
                for d in days
            ],
        })
    return {
        "source": "daily_returns.csv + state files",
        "recipes": recipes,
        "baselines": baselines or {},
        "n_recipes": len(recipes),
        "n_untestable": sum(1 for r in recipes if r["untestable"]),
    }


def build_pack(*, csv_path: Path | None = None,
               state_dir: Path | None = None,
               report_json: Path | None = None) -> dict:
    path = Path(csv_path or CSV_PATH)
    rows = []
    with path.open(newline="", encoding="utf-8") as handle:
        rows.extend(csv.DictReader(handle))
    return pack_from_rows(
        rows,
        state_equity=load_state_equity(state_dir),
        baselines=load_baselines(report_json),
    )


def _pct(value) -> str:
    if value is None:
        return ""
    return f"{float(value):.3f}"


def _baseline_rows(baselines: dict) -> list[dict]:
    out = []
    for key in ("random4", "iwm"):
        block = baselines.get(key) or {}
        for row in block.get("rows") or []:
            if isinstance(row, dict):
                out.append(row)
    if out:
        return out
    for block in baselines.values():
        if isinstance(block, dict):
            for row in block.get("rows") or []:
                if isinstance(row, dict):
                    out.append(row)
    return out


def render_sequential_html(pack: dict) -> str:
    payload = json.dumps(pack, separators=(",", ":")).replace("<", "\\u003c")
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Factor Mine sequential rebuild</title>
<style>
 :root {{
  --bg:#0f1420; --card:#171e2e; --line:#262f45; --fg:#dfe6f2;
  --mut:#8b96ab; --pos:#4ade80; --neg:#f87171; --gold:#fbbf24;
 }}
 * {{ box-sizing:border-box }}
 body {{ margin:0; background:var(--bg); color:var(--fg);
  font:13px/1.45 -apple-system,Segoe UI,Roboto,sans-serif }}
 .wrap {{ padding:12px 12px 40px }}
 h1 {{ font-size:18px; margin:0 0 6px }}
 a {{ color:#93c5fd }}
 .mut {{ color:var(--mut) }}
 .note {{ color:var(--mut); max-width:920px; margin:0 0 12px }}
 .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
  gap:8px; margin:0 0 14px }}
 .card {{ background:var(--card); border:1px solid var(--line); border-radius:10px;
  padding:8px 10px }}
 .card b {{ display:block; font-size:16px; margin-top:2px }}
 table {{ width:100%; border-collapse:collapse; font-size:12px }}
 th,td {{ padding:4px 6px; border-bottom:1px solid var(--line); text-align:right;
  font-variant-numeric:tabular-nums }}
 th {{ color:var(--mut); font-weight:600 }}
 td:first-child, th:first-child {{ text-align:left }}
 tr.pick {{ cursor:pointer }}
 tr.pick:hover, tr.on {{ background:#121826 }}
 .pos {{ color:var(--pos) }} .neg {{ color:var(--neg) }}
 input {{ background:var(--card); color:var(--fg); border:1px solid var(--line);
  border-radius:8px; padding:8px 10px; width:min(420px,100%); margin:0 0 8px }}
 .tbl {{ overflow-x:auto }}
 #days {{ margin-top:12px }}
</style></head><body><div class="wrap">
<h1>Factor Mine sequential rebuild</h1>
<p class="note">Session percents come from <code>data/factor_mine/daily_returns.csv</code>.
Book equity is the last file under <code>data/factor_mine/state/&lt;recipe&gt;/</code>.
A day before <code>created_on</code> is <b>designed_after</b> and is not in the real total.
<code>timing_clean</code> is the pit_rebuilt flag. The chain multiplies the published
session percents. The calendar is not rebuilt. A recipe with zero fills is
untestable: its return cells stay blank.</p>
<p class="mut"><a href="index.html">mine</a> · <a href="changelog.html">change log</a></p>
<div class="cards" id="cards"></div>
<h2>RANDOM4 and IWM</h2>
<p class="note">Baselines already scored on the frozen sessions. This page does not redraw them.</p>
<div class="tbl"><table id="base"></table></div>
<h2>Recipes</h2>
<input id="q" placeholder="Filter recipe" aria-label="Filter recipe">
<div class="tbl"><table id="stats"></table></div>
<div id="days"></div>
<script id="pack" type="application/json">{payload}</script>
<script>
const PACK = JSON.parse(document.getElementById("pack").textContent);
function pct(v) {{
  if (v === null || v === undefined || v === "") return "";
  const n = Number(v);
  if (!Number.isFinite(n)) return "";
  return n.toFixed(3);
}}
function cls(v) {{
  if (v === null || v === undefined || v === "") return "";
  return Number(v) < 0 ? "neg" : "pos";
}}
function esc(s) {{
  return String(s ?? "").replace(/[&<>"]/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}}[c]));
}}
function card(title, lines) {{
  return `<div class="card"><div class="mut">${{esc(title)}}</div>` +
    lines.map(line => `<b class="${{cls(line.value)}}">${{esc(line.text)}}</b><div class="mut">${{esc(line.note)}}</div>`).join("") +
    `</div>`;
}}
const byName = Object.fromEntries((PACK.recipes || []).map(r => [r.name, r]));
const hot = byName.union_hot_n4_h1 || {{}};
const hold = byName.union_hot_n4_holdup || {{}};
const bases = [];
for (const key of ["random4", "iwm"]) {{
  const rows = ((PACK.baselines || {{}})[key] || {{}}).rows || [];
  for (const row of rows) bases.push(row);
}}
function baseMean(name, fee, start) {{
  return bases.find(r => r.name === name && r.fee === fee && r.start === start && (name === "iwm" || r.universe === "with GLND"));
}}
const cards = [];
if (hot.name) {{
  cards.push(card("HOT4 sequential book", [
    {{value: hot.book_pct, text: pct(hot.book_pct), note: "state equity " + (hot.state_equity ?? "")}},
    {{value: hot.timing_real && hot.timing_real.futubull, text: pct(hot.timing_real.futubull), note: "timing_clean real futubull"}},
  ]));
}}
if (hold.name) {{
  cards.push(card("holdup real (from created_on)", [
    {{value: hold.real && hold.real.futubull, text: pct(hold.real.futubull), note: "real futubull, designed_after excluded"}},
    {{value: hold.designed_after && hold.designed_after.futubull, text: pct(hold.designed_after.futubull), note: "designed_after futubull"}},
  ]));
}}
const r4 = baseMean("random4", "futubull", "2026-08-13");
const iwm = baseMean("iwm", "futubull", "2026-08-13");
if (r4) cards.push(card("RANDOM4 futubull from 08-13", [
  {{value: r4.mean, text: pct(r4.mean), note: "mean, with GLND"}},
]));
if (iwm) cards.push(card("IWM futubull from 08-13", [
  {{value: iwm.mean, text: pct(iwm.mean), note: "buy-and-hold"}},
]));
document.getElementById("cards").innerHTML = cards.join("");
const baseBody = bases.map(r => `<tr>
  <td>${{esc(r.name)}}</td><td>${{esc(r.fee)}}</td><td>${{esc(r.universe)}}</td>
  <td>${{esc(r.start)}}</td><td class="${{cls(r.mean)}}">${{pct(r.mean)}}</td>
  <td>${{pct(r.p5)}}</td><td>${{pct(r.p50)}}</td><td>${{pct(r.p95)}}</td>
  <td>${{r.trades ?? ""}}</td></tr>`).join("");
document.getElementById("base").innerHTML =
  `<thead><tr><th>baseline</th><th>fee</th><th>universe</th><th>window</th>
   <th>mean %</th><th>p5</th><th>p50</th><th>p95</th><th>trades</th></tr></thead><tbody>${{baseBody}}</tbody>`;
function rowHtml(r) {{
  const real = r.real || {{}};
  const des = r.designed_after || {{}};
  const tim = r.timing_real || {{}};
  return `<tr class="pick" data-name="${{esc(r.name)}}">
    <td>${{esc(r.name)}}</td>
    <td>${{esc(r.created_on)}}</td>
    <td>${{r.untestable ? "true" : ""}}</td>
    <td class="${{cls(r.book_pct)}}">${{pct(r.book_pct)}}</td>
    <td class="${{cls(real.futubull)}}">${{pct(real.futubull)}}</td>
    <td class="${{cls(real.flat_15bp)}}">${{pct(real.flat_15bp)}}</td>
    <td class="${{cls(des.futubull)}}">${{pct(des.futubull)}}</td>
    <td class="${{cls(des.flat_15bp)}}">${{pct(des.flat_15bp)}}</td>
    <td class="${{cls(tim.futubull)}}">${{pct(tim.futubull)}}</td>
    <td class="${{cls(tim.flat_15bp)}}">${{pct(tim.flat_15bp)}}</td>
    <td>${{real.fires ?? ""}}</td>
    <td>${{r.n_designed_after || ""}}</td>
  </tr>`;
}}
function paint(q) {{
  const needle = (q || "").trim().toLowerCase();
  const rows = (PACK.recipes || []).filter(r => !needle || r.name.toLowerCase().includes(needle));
  document.getElementById("stats").innerHTML =
    `<thead><tr>
      <th>recipe</th><th>created_on</th><th>untestable</th><th>book %</th>
      <th>real futubull</th><th>real 15bp</th>
      <th>designed_after futubull</th><th>designed_after 15bp</th>
      <th>timing_clean real futubull</th><th>timing_clean real 15bp</th>
      <th>real fires</th><th>designed_after days</th>
    </tr></thead><tbody>${{rows.map(rowHtml).join("")}}</tbody>`;
}}
function showDays(name) {{
  const rec = byName[name];
  if (!rec) return;
  const body = (rec.days || []).map(d => `<tr>
    <td>${{esc(d.date)}}</td><td>${{esc(d.label)}}</td>
    <td>${{d.timing_clean ? "true" : "false"}}</td>
    <td class="${{cls(d.futu)}}">${{pct(d.futu)}}</td>
    <td class="${{cls(d.flat)}}">${{pct(d.flat)}}</td>
    <td>${{d.fires}}</td><td>${{esc(d.status)}}</td></tr>`).join("");
  document.getElementById("days").innerHTML =
    `<h2>${{esc(name)}}</h2><div class="tbl"><table>
     <thead><tr><th>date</th><th>label</th><th>timing_clean</th>
     <th>futubull</th><th>15bp</th><th>fires</th><th>status</th></tr></thead>
     <tbody>${{body}}</tbody></table></div>`;
}}
document.getElementById("q").addEventListener("input", e => paint(e.target.value));
document.getElementById("stats").addEventListener("click", e => {{
  const tr = e.target.closest("tr.pick");
  if (!tr) return;
  showDays(tr.getAttribute("data-name"));
}});
paint("");
</script>
</div></body></html>
"""


def render_changelog_html(markdown: str) -> str:
    body = html.escape(markdown or "")
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Factor Mine change log</title>
<style>
 body {{ margin:0; background:#0f1420; color:#dfe6f2;
  font:13px/1.45 -apple-system,Segoe UI,Roboto,sans-serif }}
 .wrap {{ padding:12px 12px 40px }}
 a {{ color:#93c5fd }}
 pre {{ white-space:pre-wrap; overflow-wrap:anywhere;
  font:12px/1.4 ui-monospace,Menlo,Consolas,monospace }}
</style></head><body><div class="wrap">
<h1>Factor Mine change log</h1>
<p><a href="index.html">mine</a> · <a href="sequential.html">sequential rebuild</a></p>
<pre>{body}</pre>
</div></body></html>
"""


def write_pack(dash_dir: Path | None = None, *,
               pack: dict | None = None,
               changelog: str | None = None) -> dict[str, Path]:
    """Write the sequential page and the changelog page. Nothing else."""
    dest = Path(dash_dir or DASH_DIR)
    dest.mkdir(parents=True, exist_ok=True)
    built = pack if pack is not None else build_pack()
    text = changelog
    if text is None:
        text = CHANGELOG_MD.read_text(encoding="utf-8") if CHANGELOG_MD.is_file() else ""
    sequential = dest / "sequential.html"
    change = dest / "changelog.html"
    sequential.write_text(render_sequential_html(built), encoding="utf-8")
    change.write_text(render_changelog_html(text), encoding="utf-8")
    return {"sequential": sequential, "changelog": change}


def main() -> None:
    paths = write_pack()
    print(f"[factor-mine] sequential pack {paths['sequential']}", flush=True)
    print(f"[factor-mine] changelog page {paths['changelog']}", flush=True)


if __name__ == "__main__":
    main()
