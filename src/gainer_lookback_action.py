"""Liquid Finviz top-gainers × ticker lookback × 09:30 BUY/SELL catch.

For every session since the dashboard start (2026-08-13):

  * take that day's liquid Finviz gainers (mcap ≥ $100M, adv ≥ 500k)
  * run the readiness lookback paint on those names
  * stamp an authoritative BUY / SELL / NO BUY / HOLD from 09:30-only
    cameras, featured mine setups, and hall-pass lane
  * grade the call on forward +1d / +3d / +1w (never same-day Change%)

Same-day Change% only picks the universe ("who ripped"). The call does
not see it.

CLI: python -m src.gainer_lookback_action --write
"""
from __future__ import annotations

import argparse
import html
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from . import gainer_asof as ga
from . import lookback_action as act
from . import ticker_lookback as tl
from . import ticker_lookback_run as run
from . import ticker_lookback_setups as setups

ROOT = Path(__file__).resolve().parent.parent
OUT_MD = ROOT / "03_scoreboard" / "GAINER_LOOKBACK_ACTION.md"
OUT_JSON = ROOT / "03_scoreboard" / "gainer_lookback_action.json"
OUT_HTML = ROOT / "dashboard" / "gainer-lookback" / "index.html"
DAILY_MD = ROOT / "01_daily" / "gainer_lookback_action.md"

START = ga.START
TOP_N = 25
MIN_CHANGE = 2.0
HORIZONS = act.HORIZONS


def collect_gainers(from_date: str = START, to_date: str | None = None,
                    top_n: int = TOP_N, min_change: float = MIN_CHANGE,
                    liquid: bool = True) -> dict:
    idx = tl.build_index()
    sessions = [
        s for s in idx["sessions"]
        if s["date"] >= from_date and (not to_date or s["date"] <= to_date)
    ]
    by_date: dict[str, list[dict]] = {}
    keys: set[tuple[str, str]] = set()
    names: set[str] = set()
    for sess in sessions:
        date = sess["date"]
        rows = ga.liquid_gainers(
            ga.load_finviz(date), top_n=top_n, min_change=min_change,
            liquid=liquid,
        )
        by_date[date] = rows
        for row in rows:
            keys.add((date, row["ticker"]))
            names.add(row["ticker"])
    return {
        "from_date": from_date,
        "to_date": to_date or (sessions[-1]["date"] if sessions else from_date),
        "top_n": top_n,
        "min_change": min_change,
        "liquid": liquid,
        "n_sessions": len(sessions),
        "n_gainer_days": len(keys),
        "n_tickers": len(names),
        "by_date": by_date,
        "keys": keys,
        "tickers": sorted(names),
        "session_dates": [s["date"] for s in sessions],
    }


def _fwd(day: dict) -> dict:
    return day.get("price_changes") or day.get("forward_returns") or {}


def _score_rows(rows: list[dict], params: dict) -> dict:
    counts = Counter()
    hits = {h: Counter() for h in HORIZONS}
    rets = {h: [] for h in HORIZONS}
    aligned = Counter()
    for row in rows:
        packed = act.action_call(row, params=params)
        action = packed["action"]
        counts[action] += 1
        counts["n"] += 1
        grade = act.grade_call(action, _fwd(row))
        row_hits = {}
        for h in HORIZONS:
            g = grade[h]
            row_hits[h] = g
            if g is None:
                continue
            hits[h]["n"] += 1
            hits[h]["hit"] += int(g)
            val = (_fwd(row) or {}).get(h)
            try:
                rets[h].append(float(val))
            except (TypeError, ValueError):
                pass
        if action in ("BUY", "SELL"):
            a, b = row_hits.get("1d"), row_hits.get("3d")
            if a is not None and b is not None:
                aligned["n"] += 1
                aligned["hit"] += int(bool(a and b))
        row["action_call"] = action
        row["action_reason"] = packed["reason"]
        row["hits"] = row_hits
    def rate(c: Counter) -> float | None:
        n = c.get("n") or 0
        if not n:
            return None
        return round(c.get("hit", 0) / n, 3)

    mean = {}
    for h in HORIZONS:
        xs = rets[h]
        mean[h] = None if not xs else round(sum(xs) / len(xs), 3)
    return {
        "n": counts["n"],
        "n_buy": counts.get("BUY", 0),
        "n_sell": counts.get("SELL", 0),
        "n_no_buy": counts.get("NO BUY", 0),
        "n_hold": counts.get("HOLD", 0),
        "catch": {h: rate(hits[h]) for h in HORIZONS},
        "catch_n": {h: hits[h].get("n", 0) for h in HORIZONS},
        "catch_hit": {h: hits[h].get("hit", 0) for h in HORIZONS},
        "aligned_1d_3d": rate(aligned),
        "aligned_n": aligned.get("n", 0),
        "mean_ret": mean,
        "params": {k: v for k, v in params.items() if k != "label"},
        "label": params.get("label") or "",
    }


def paint_gainers(meta: dict, from_date: str, to_date: str | None) -> dict:
    names = meta["tickers"]
    if not names:
        return {"generated_at": datetime.now(tl.ET).isoformat(), "names": []}
    payload = run.scan_tickers(names, from_date=from_date, to_date=to_date)
    setups.attach_setups(payload)
    return payload


def _day_index(payload: dict) -> dict[tuple[str, str], dict]:
    out = {}
    for rec in payload.get("names") or []:
        t = rec.get("ticker")
        for day in rec.get("days") or []:
            out[(day.get("date"), t)] = day
    return out


def walk(from_date: str = START, to_date: str | None = None,
         top_n: int = TOP_N, min_change: float = MIN_CHANGE,
         liquid: bool = True, preset: str | None = None) -> dict:
    meta = collect_gainers(
        from_date=from_date, to_date=to_date, top_n=top_n,
        min_change=min_change, liquid=liquid,
    )
    payload = paint_gainers(meta, meta["from_date"], meta["to_date"])
    by_card = _day_index(payload)
    gainer_rows = []
    for date, rows in meta["by_date"].items():
        for raw in rows:
            card = by_card.get((date, raw["ticker"]))
            if not card:
                continue
            rec = dict(card)
            rec["gainer_change"] = raw.get("change_pct")
            rec["gainer_rank"] = rows.index(raw) + 1
            rec["sector"] = raw.get("sector") or rec.get("sector")
            gainer_rows.append(rec)
    history_rows = [
        dict(day)
        for rec in payload.get("names") or []
        for day in rec.get("days") or []
        if day.get("class") != "no_data"
    ]

    default_name = preset or act.default_preset_name()
    sweeps = {}
    for name in act.PRESETS:
        params = act.preset_params(name)
        # Score copies so later presets don't inherit stamped action.
        sweeps[name] = {
            "gainer_days": _score_rows([dict(r) for r in gainer_rows], params),
            "history": _score_rows([dict(r) for r in history_rows], params),
        }

    chosen = act.preset_params(default_name)
    act.attach_actions(payload, params=chosen)
    for row in gainer_rows:
        packed = act.action_call(row, params=chosen)
        row["action_call"] = packed["action"]
        row["action_reason"] = packed["reason"]
        row["hits"] = act.grade_call(packed["action"], _fwd(row))

    recall_buy = sum(1 for r in gainer_rows if r.get("action_call") == "BUY")
    return {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "method": "gainer_lookback_action",
        "from_date": meta["from_date"],
        "to_date": meta["to_date"],
        "top_n": top_n,
        "min_change": min_change,
        "liquid": liquid,
        "n_sessions": meta["n_sessions"],
        "n_gainer_days": len(gainer_rows),
        "n_tickers": meta["n_tickers"],
        "preset": default_name,
        "recall_buy": recall_buy,
        "recall_buy_rate": (
            None if not gainer_rows
            else round(recall_buy / len(gainer_rows), 3)
        ),
        "sweeps": sweeps,
        "chosen": sweeps.get(default_name) or {},
        "gainer_rows": gainer_rows,
        "lookback": {
            "generated_at": payload.get("generated_at"),
            "n_names": len(payload.get("names") or []),
        },
        "session_dates": meta["session_dates"],
        "by_date": {
            d: [r["ticker"] for r in rows] for d, rows in meta["by_date"].items()
        },
    }


def _pct(v) -> str:
    if v is None:
        return "—"
    return f"{100 * float(v):.1f}%"


def _ret(v) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):+.2f}%"
    except (TypeError, ValueError):
        return "—"


def _hit_icon(v) -> str:
    if v is True:
        return "✅"
    if v is False:
        return "❌"
    return "—"


def render_markdown(payload: dict) -> str:
    sweeps = payload.get("sweeps") or {}
    chosen = payload.get("preset") or "featured"
    L = [
        "# Gainer lookback action",
        "",
        f"_Generated {payload.get('generated_at')}_",
        "",
        f"Universe: liquid Finviz top **{payload.get('top_n')}** "
        f"gainers (Change% ≥ {payload.get('min_change'):.0f}%, "
        f"mcap ≥ $100M, adv ≥ 500k) from **{payload.get('from_date')}** "
        f"to **{payload.get('to_date')}**. "
        f"{payload.get('n_gainer_days')} gainer-days · "
        f"{payload.get('n_tickers')} names · "
        f"{payload.get('n_sessions')} sessions.",
        "",
        "Each name is painted with ticker lookback (09:30 cameras + "
        "coaches + featured mine setups). The **Action** column is the "
        "authoritative BUY / SELL / NO BUY / HOLD. Same-day Change% "
        "only selects the universe — it does not enter the call. "
        "Catch = BUY and the forward move is up, or SELL and it is down.",
        "",
        f"Default preset **`{chosen}`**. "
        f"BUY on the gainer morning (recall): "
        f"**{_pct(payload.get('recall_buy_rate'))}** "
        f"({payload.get('recall_buy')}/{payload.get('n_gainer_days')}).",
        "",
        "## Preset sweep",
        "",
        "Paint once, score each rule set. **Gainer-days** = the morning "
        "of the rip. **History** = every printed lookback day of those "
        "names (the full sheet).",
        "",
        "| Preset | Slice | n | BUY | SELL | NO BUY | HOLD | "
        "catch 1d | catch 3d | catch 1w | 1d+3d aligned | mean +1d |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, block in sweeps.items():
        for slice_name, key in (("gainer-days", "gainer_days"), ("history", "history")):
            s = block.get(key) or {}
            mark = " ←" if name == chosen and key == "gainer_days" else ""
            L.append(
                f"| `{name}`{mark} | {slice_name} | {s.get('n') or 0} | "
                f"{s.get('n_buy') or 0} | {s.get('n_sell') or 0} | "
                f"{s.get('n_no_buy') or 0} | {s.get('n_hold') or 0} | "
                f"{_pct((s.get('catch') or {}).get('1d'))} "
                f"({(s.get('catch_hit') or {}).get('1d') or 0}/"
                f"{(s.get('catch_n') or {}).get('1d') or 0}) | "
                f"{_pct((s.get('catch') or {}).get('3d'))} | "
                f"{_pct((s.get('catch') or {}).get('1w'))} | "
                f"{_pct(s.get('aligned_1d_3d'))} | "
                f"{_ret((s.get('mean_ret') or {}).get('1d'))} |"
            )
    L += [
        "",
        "## How the default call is made",
        "",
        "1. A featured **fade** setup (`first crack`, `🚨+heat🔴`) → **SELL**.",
        "2. Hall pass **blocked** → **NO BUY**.",
        "3. Hall pass standard / group leader / catalyst / probable → **BUY**.",
        "4. Else a featured **long** setup with 1d edge ≥ the preset cut "
        "(vol+AB, vol+gen🔴, vol+join🔴, 🔵+heat) → **BUY**.",
        "5. Else **HOLD**. Pre-lattice days (before 2026-08-31) have no "
        "hall pass, so setups carry the call.",
        "",
        "Judge-yellow and 🔵-stretch stay out of the default cut "
        "(edge too soft / too common). Flip to `loose` to include them.",
        "",
        "## Gainer mornings (default preset)",
        "",
        "| Date | # | Ticker | Δ that day | Action | Why | Lane | Setups | "
        "+1d | +3d | +1w | 1d | 3d | 1w |",
        "|---|---:|---|---:|---|---|---|---|---:|---:|---:|---|---|---|",
    ]
    rows = sorted(
        payload.get("gainer_rows") or [],
        key=lambda r: (str(r.get("date") or ""), int(r.get("gainer_rank") or 0)),
    )
    for r in rows:
        hits = r.get("hits") or {}
        L.append(
            f"| {r.get('date')} | {r.get('gainer_rank')} | "
            f"`{r.get('ticker')}` | {_ret(r.get('gainer_change'))} | "
            f"**{r.get('action_call') or '—'}** | "
            f"{str(r.get('action_reason') or '—').replace('|', '/')} | "
            f"{r.get('lane_label') or r.get('lane') or '—'} | "
            f"{setups.setup_labels(r) or '—'} | "
            f"{_ret((_fwd(r) or {}).get('1d'))} | "
            f"{_ret((_fwd(r) or {}).get('3d'))} | "
            f"{_ret((_fwd(r) or {}).get('1w'))} | "
            f"{_hit_icon(hits.get('1d'))} | "
            f"{_hit_icon(hits.get('3d'))} | "
            f"{_hit_icon(hits.get('1w'))} |"
        )
    L += [
        "",
        "_Δ that day is the Finviz Change% that put the name on the "
        "gainer list (outcome, not an input). +1d / +3d / +1w are the "
        "next-session closes from the lookback price panel._",
        "",
    ]
    return "\n".join(L) + "\n"


def render_html(payload: dict) -> str:
    chosen = payload.get("preset") or "featured"
    sweeps = payload.get("sweeps") or {}
    sweep_rows = []
    for name, block in sweeps.items():
        s = (block or {}).get("gainer_days") or {}
        cls = "chosen" if name == chosen else ""
        sweep_rows.append(
            f"<tr class='{cls}'><td><code>{html.escape(name)}</code></td>"
            f"<td>{s.get('n') or 0}</td><td>{s.get('n_buy') or 0}</td>"
            f"<td>{s.get('n_sell') or 0}</td><td>{s.get('n_hold') or 0}</td>"
            f"<td>{html.escape(_pct((s.get('catch') or {}).get('1d')))}</td>"
            f"<td>{html.escape(_pct((s.get('catch') or {}).get('3d')))}</td>"
            f"<td>{html.escape(_pct((s.get('catch') or {}).get('1w')))}</td>"
            f"<td>{html.escape(_ret((s.get('mean_ret') or {}).get('1d')))}</td></tr>"
        )
    body = []
    for r in sorted(
        payload.get("gainer_rows") or [],
        key=lambda x: (str(x.get("date") or ""), int(x.get("gainer_rank") or 0)),
    ):
        tone = act.action_tone(r.get("action_call") or "")
        hits = r.get("hits") or {}
        body.append(
            f"<tr><th>{html.escape(str(r.get('date') or ''))}</th>"
            f"<td>{r.get('gainer_rank') or ''}</td>"
            f"<td>{html.escape(str(r.get('ticker') or ''))}</td>"
            f"<td>{html.escape(_ret(r.get('gainer_change')))}</td>"
            f"<td class='{tone}'>{html.escape(str(r.get('action_call') or '—'))}</td>"
            f"<td class='why'>{html.escape(str(r.get('action_reason') or '—'))}</td>"
            f"<td>{html.escape(setups.setup_labels(r) or '—')}</td>"
            f"<td>{html.escape(_ret((_fwd(r) or {}).get('1d')))}</td>"
            f"<td>{html.escape(_ret((_fwd(r) or {}).get('3d')))}</td>"
            f"<td>{html.escape(_ret((_fwd(r) or {}).get('1w')))}</td>"
            f"<td>{_hit_icon(hits.get('1d'))}</td>"
            f"<td>{_hit_icon(hits.get('3d'))}</td>"
            f"<td>{_hit_icon(hits.get('1w'))}</td></tr>"
        )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Gainer lookback action</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui}}
main{{max-width:1100px;margin:auto;padding:16px}}h1,h2{{margin:.4em 0}}
.muted{{color:var(--muted)}}
.sheet{{overflow-x:auto;border:1px solid var(--line);border-radius:12px;margin:16px 0}}
table{{border-collapse:separate;border-spacing:0;min-width:880px;width:100%;background:var(--card)}}
th,td{{padding:8px 7px;text-align:center;border-bottom:1px solid var(--line);white-space:nowrap}}
thead th{{position:sticky;top:0;background:#17213a}}tbody th{{position:sticky;left:0;background:#17213a;text-align:left}}
td.good{{background:#123d2c}}td.bad{{background:#4b2028}}td.neutral{{background:#473e1d}}
td.why{{text-align:left;white-space:normal;max-width:280px;font-size:12px}}
tr.chosen td{{outline:1px solid #eab308}}
@media(max-width:600px){{main{{padding:8px}}th,td{{padding:8px 6px;font-size:13px}}}}
</style></head><body><main>
<h1>Gainer lookback action</h1>
<p>Liquid Finviz top {html.escape(str(payload.get('top_n')))} gainers
({html.escape(str(payload.get('from_date')))} → {html.escape(str(payload.get('to_date')))}).
Action is 09:30-only. Catch = BUY and the forward move is up, or SELL and it is down.</p>
<p class="muted">BUY on the gainer morning (recall):
<b>{html.escape(_pct(payload.get('recall_buy_rate')))}</b>
· default preset <code>{html.escape(str(chosen))}</code></p>
<h2>Preset sweep (gainer mornings)</h2>
<div class="sheet"><table>
<thead><tr><th>Preset</th><th>n</th><th>BUY</th><th>SELL</th><th>HOLD</th><th>catch 1d</th><th>catch 3d</th><th>catch 1w</th><th>mean +1d</th></tr></thead>
<tbody>{''.join(sweep_rows)}</tbody></table></div>
<h2>Each gainer morning</h2>
<div class="sheet"><table>
<thead><tr><th>Date</th><th>#</th><th>Ticker</th><th>Δ day</th><th>Action</th><th>Why</th><th>Setups</th><th>+1d</th><th>+3d</th><th>+1w</th><th>1d</th><th>3d</th><th>1w</th></tr></thead>
<tbody>{''.join(body)}</tbody></table></div>
</main></body></html>"""


def write(payload: dict) -> dict:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    DAILY_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    slim = {k: v for k, v in payload.items() if k != "gainer_rows"}
    slim["gainer_rows"] = [
        {
            "date": r.get("date"),
            "ticker": r.get("ticker"),
            "gainer_rank": r.get("gainer_rank"),
            "gainer_change": r.get("gainer_change"),
            "action_call": r.get("action_call"),
            "action_reason": r.get("action_reason"),
            "lane": r.get("lane"),
            "lane_label": r.get("lane_label"),
            "setups": [
                {"id": s.get("id"), "short": s.get("short"), "verdict": s.get("verdict")}
                for s in (r.get("setups") or [])
            ],
            "price_changes": r.get("price_changes") or r.get("forward_returns"),
            "hits": r.get("hits"),
        }
        for r in payload.get("gainer_rows") or []
    ]
    md = render_markdown(payload)
    OUT_MD.write_text(md, encoding="utf-8")
    DAILY_MD.write_text(md, encoding="utf-8")
    OUT_JSON.write_text(json.dumps(slim, indent=2, default=str), encoding="utf-8")
    OUT_HTML.write_text(render_html(payload), encoding="utf-8")
    print(f"[gainer-lookback-action] wrote {OUT_MD}")
    print(f"[gainer-lookback-action] wrote {OUT_JSON}")
    print(f"[gainer-lookback-action] phone {OUT_HTML}")
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default=START)
    ap.add_argument("--to-date", default="")
    ap.add_argument("--top-n", type=int, default=TOP_N)
    ap.add_argument("--min-change", type=float, default=MIN_CHANGE)
    ap.add_argument("--preset", default="", help="featured|strict|setups|lane|loose")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    payload = walk(
        from_date=args.from_date,
        to_date=args.to_date or None,
        top_n=args.top_n,
        min_change=args.min_change,
        preset=args.preset or None,
    )
    if args.write:
        write(payload)
    print(render_markdown(payload)[:8000])


if __name__ == "__main__":
    main()
