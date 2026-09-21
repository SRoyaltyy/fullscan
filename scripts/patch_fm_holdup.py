#!/usr/bin/env python3
"""Give holdup / other score-only sleeves a cash-start replay path."""
from __future__ import annotations

from pathlib import Path
import sys


def patch_html(html: str) -> str:
    old = "function startRows(){\n  return (D.starts||{})[pickedName()] || [];\n}"
    new = """function startRows(){
  const name=pickedName();
  const rows=(D.starts||{})[name];
  if(rows && rows.length) return rows;
  const ds=D.dates||[];
  if(!name || !ds.length) return [];
  const known=(D.stats||[]).some(function(s){return s.name===name;})
    || (D.recipes||[]).some(function(r){return r.name===name;});
  if(!known) return [];
  return ds.map(function(d){return {start:d, pending:false, prelim:false};});
}"""
    if old in html:
        html = html.replace(old, new, 1)

    old = "  return (D.daily||{})[name] || (((D.books||{})[name]||{}).daily) || [];"
    new = """  const baked=(D.daily||{})[name] || (((D.books||{})[name]||{}).daily) || [];
  if(baked && baked.length) return baked;
  const replay=simBook();
  if(replay && (replay.daily||[]).length) return replay.daily;
  return [];"""
    if old in html:
        html = html.replace(old, new, 1)

    old = "        const [doSell, kind] = lotShouldSell(lot, held, minHold, early, dropped, sellMode, p, side, rec.take_pct, rec.stop_pct);"
    new = "        const lotMin = Number(lot.min_hold != null ? lot.min_hold : minHold);\n        const [doSell, kind] = lotShouldSell(lot, held, lotMin, early, dropped, sellMode, p, side, rec.take_pct, rec.stop_pct);"
    if old in html:
        html = html.replace(old, new, 1)

    old = '          if (dropped && held < minHold) {\n            skips.push({ date, ticker: t, kind: "min_hold", reason: "dropped but min-hold " + held + "/" + minHold + " sess — no sell" });'
    new = '          if (dropped && held < lotMin) {\n            skips.push({ date, ticker: t, kind: "min_hold", reason: "dropped but min-hold " + held + "/" + lotMin + " sess — no sell" });'
    if old in html:
        html = html.replace(old, new, 1)

    old = "            lot = { ticker: t, shares, entry_px: p, entry_date: date, cost, fee_in: fee, notional: shares * p, last_px: p, peak_px: p, reason };"
    new = '            const holdup = sBoost === "holdup" && side === "long" && s != null && Number(s) > 0 && !hardRed;\n            lot = { ticker: t, shares, entry_px: p, entry_date: date, cost, fee_in: fee, notional: shares * p, last_px: p, peak_px: p, reason, min_hold: holdup ? Math.max(minHold, 2) : minHold };'
    if old in html:
        html = html.replace(old, new, 1)
    return html


def patch_file(path: Path | str) -> Path:
    p = Path(path)
    html = p.read_text(encoding="utf-8", errors="replace")
    new = patch_html(html)
    if new != html:
        p.write_text(new, encoding="utf-8")
        print("patched holdup cash-start", p, "bytes", p.stat().st_size)
    else:
        print("no holdup anchors", p)
    return p


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    target = Path(args[0] if args else "dashboard/factor-mine/index.html")
    patch_file(target)


if __name__ == "__main__":
    main()
