"""Hard-red polarity flip — research only.

On S≤−3 mornings, take the sleeve's leak-free pick list and fire the
*opposite* side at the official 09:30 open. Intended long → short.
Intended short → long. No scoop. Hold H = close of the H-th session
including entry.

Stupid on purpose. KEEP still needs ≥30 fires and >55% after Futubull
fees. Live sit is unchanged.

CLI: python -m src.hard_red_flip --write
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from src import book_era
from src import combo_broker as cb
from src import factor_mine as fm
from src import factor_mine_book as fmb
from src import factor_mine_combo as fmc
from src import hard_red_hold_x as hx
from src import hard_red_sit_research as hrs
from src import paper_trade as pt
from src import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "03_scoreboard" / "hard_red_sit"
OUT_JSON = OUT_DIR / "hard_red_flip.json"
OUT_MD = ROOT / "03_scoreboard" / "HARD_RED_SIT.md"
DASH_FM = ROOT / "dashboard" / "factor-mine" / "hard_red_flip.json"
DASH_SB = ROOT / "dashboard" / "strategy-board" / "hard_red_flip.json"
DASH_HR = ROOT / "dashboard" / "hard-red-sit" / "hard_red_flip.json"
DASH_PAGE = ROOT / "dashboard" / "hard-red-sit" / "index.html"
MARK_B = "<!-- FLIP_BEGIN -->"
MARK_E = "<!-- FLIP_END -->"

HOLD_GRID = hx.HOLD_GRID
KEEP_MIN = hx.KEEP_MIN
KEEP_WIN = hx.KEEP_WIN
FLIP_COMBOS = (
    "combo_sh_5050_shared",
    cb.PAPER_COMBO,
)
EXTRA_NAMES = (
    "union_news_pack_net2_h1",
    "short_news_or_h3",
)


def flip_side(side: str | None) -> str:
    return "short" if (side or "long") == "long" else "long"


def open_day_fire(ticker: str, date: str, cal: list[str], *,
                  bars, fees, side: str, hold: int) -> dict | None:
    """Fill at the official open. Close only grades."""
    bar = hrs.clock_bar(ticker, date, bars)
    o = bar.get("open")
    if o is None:
        return None
    entry = float(o)
    exit_px, exit_d, how = hrs.horizon_exit(cal, date, hold, ticker, bars)
    pnl = None
    if exit_px is not None:
        pnl = round(hrs.after_fee_pnl(
            1, entry, float(exit_px), side=side, fees=fees), 4)
    return {
        "date": date, "ticker": ticker, "side": side, "shares": 1,
        "entry": round(entry, 4),
        "exit": None if exit_px is None else round(float(exit_px), 4),
        "exit_date": exit_d, "exit_how": how,
        "pnl": pnl, "win": None if pnl is None else bool(pnl > 0),
        "kind": "open_flip", "open": o, "low": bar.get("low"),
        "intended": flip_side(side), "hold": hold,
    }


def _picks_by_day(rec: dict, panel: dict, red: list[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for date in red:
        try:
            picks = fm.pick_day(hx._rows_on(panel, date), rec)
        except Exception:
            picks = []
        out[date] = [
            str(r.get("ticker") or "").upper()
            for r in picks if r.get("ticker")
        ]
    return out


def sweep_recipe(rec: dict, panel: dict, red: list[str], cal: list[str],
                 *, bars, fees) -> dict:
    intended = rec.get("side") or "long"
    fired = flip_side(intended)
    by_day = _picks_by_day(rec, panel, red)
    cells = []
    for hold in HOLD_GRID:
        fires = []
        for date, names in by_day.items():
            for t in names:
                f = open_day_fire(
                    t, date, cal, bars=bars, fees=fees,
                    side=fired, hold=hold)
                if f:
                    fires.append(f)
        cells.append(hx._cell("polarity_flip", hold, None, fires))
    return {
        "name": rec.get("name") or "?",
        "side": intended,
        "fired_side": fired,
        "family": rec.get("family") or rec.get("universe") or "",
        "n_named_days": sum(1 for v in by_day.values() if v),
        "picked": hx.pick_cell(cells),
        "grid": [{k: c.get(k) for k in (
            "mode", "hold", "dip_pct", "n_fires", "win_rate", "pnl",
            "verdict")} for c in cells],
    }


def sweep_combo(name: str, panel: dict, red: list[str], cal: list[str],
                *, bars, fees) -> dict:
    spec = cb.combo_spec(name)
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    recs = fmc.recs_for_spec(spec, rec_by)
    kids = [sweep_recipe(r, panel, red, cal, bars=bars, fees=fees)
            for r in recs]
    by_rec = {r["name"]: _picks_by_day(r, panel, red) for r in recs}
    cells = []
    for hold in HOLD_GRID:
        fires = []
        for rec in recs:
            fired = flip_side(rec.get("side") or "long")
            for date, names in by_rec[rec["name"]].items():
                for t in names:
                    f = open_day_fire(
                        t, date, cal, bars=bars, fees=fees,
                        side=fired, hold=hold)
                    if f:
                        fires.append(f)
        cells.append(hx._cell("polarity_flip", hold, None, fires))
    return {
        "name": name,
        "side": "mixed",
        "fired_side": "flipped",
        "family": "combo",
        "kids": [{k: kid.get(k) for k in (
            "name", "side", "fired_side", "picked")} for kid in kids],
        "picked": hx.pick_cell(cells),
        "grid": [{k: c.get(k) for k in (
            "mode", "hold", "dip_pct", "n_fires", "win_rate", "pnl",
            "verdict")} for c in cells],
    }


def sweep_flatten(red: list[str], cal: list[str], *, bars, fees) -> dict:
    try:
        woulds = hrs.flatten_woulds(cal)
    except Exception:
        woulds = []
    by_day = {d["date"]: list(d.get("tickers") or [])
              for d in woulds if d.get("date") in set(red)}
    cells = []
    for hold in HOLD_GRID:
        fires = []
        for date, names in by_day.items():
            for t in names:
                f = open_day_fire(
                    t, date, cal, bars=bars, fees=fees,
                    side="short", hold=hold)
                if f:
                    fires.append(f)
        cells.append(hx._cell("polarity_flip", hold, None, fires))
    return {
        "name": "flatten_io",
        "side": "long",
        "fired_side": "short",
        "family": "flatten",
        "n_named_days": sum(1 for v in by_day.values() if v),
        "picked": hx.pick_cell(cells),
        "grid": [{k: c.get(k) for k in (
            "mode", "hold", "dip_pct", "n_fires", "win_rate", "pnl",
            "verdict")} for c in cells],
    }


def default_recipes() -> list[dict]:
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    names = list(hx.SWEEP_NAMES) + [n for n in EXTRA_NAMES if n not in hx.SWEEP_NAMES]
    return [rec_by[n] for n in names if n in rec_by]


def run(*, from_date: str = book_era.DASHBOARD_START,
        to_date: str | None = None, write: bool = False,
        panel: dict | None = None, bars=None, fees=None,
        regime=None, recipes: list[dict] | None = None) -> dict:
    if panel is None:
        panel = fm.load_or_build_panel(from_date, None)
    cal = [d for d in (panel.get("session_dates") or [])
           if d >= from_date and (not to_date or d <= to_date)]
    if regime is None:
        try:
            regime = fmb.load_regime()
        except Exception:
            regime = {}
    red = hx._red_dates(cal, regime)
    fees = fees if fees is not None else pt.load_fees()
    if bars is None:
        try:
            from src import walkforward_factor_mine as wf
            bars = wf.preload_bars(panel)
        except Exception:
            bars = None
    recs = recipes if recipes is not None else default_recipes()
    sleeves = [sweep_recipe(rec, panel, red, cal, bars=bars, fees=fees)
               for rec in recs]
    combos = []
    for name in FLIP_COMBOS:
        try:
            combos.append(sweep_combo(
                name, panel, red, cal, bars=bars, fees=fees))
        except Exception:
            continue
    flatten = sweep_flatten(red, cal, bars=bars, fees=fees)
    board = combos + [flatten] + sleeves
    keeps = [s for s in board if (s.get("picked") or {}).get("verdict") == "KEEP"]
    watches = [s for s in board
               if (s.get("picked") or {}).get("verdict") == "WATCH"]
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "tag": "RESEARCH",
        "idea": "polarity_flip",
        "live_sit": True,
        "live_untouched": ["flatten_robust", cb.PAPER_COMBO],
        "from_date": cal[0] if cal else from_date,
        "to_date": cal[-1] if cal else to_date,
        "n_sessions": len(cal),
        "hard_red_n": len(red),
        "hard_red_days": red,
        "hold_grid": list(HOLD_GRID),
        "keep_bar": {"min_fires": KEEP_MIN, "win": KEEP_WIN},
        "n_strategies": len(board),
        "n_keep": len(keeps),
        "n_watch": len(watches),
        "keeps": [_slim(s) for s in keeps],
        "watches": [_slim(s) for s in watches],
        "strategies": [_slim(s) for s in board],
        "note": (
            "Paper counterfactual. On hard-red, flip the intended side "
            "at the 09:30 open (long→short, short→long). No scoop. "
            "Live sit unchanged. X is not used."
        ),
    }
    if write:
        write_payload(payload)
    return payload


def _slim(s: dict) -> dict:
    return {
        "name": s.get("name"),
        "side": s.get("side"),
        "fired_side": s.get("fired_side"),
        "family": s.get("family"),
        "n_named_days": s.get("n_named_days"),
        "picked": s.get("picked") or None,
        "grid": s.get("grid") or [],
        "kids": s.get("kids"),
    }


def render_md(payload: dict) -> str:
    lines = [
        MARK_B,
        "",
        "## Polarity flip (research, not a wire)",
        "",
        str(payload.get("note") or ""),
        "",
        f"Window `{payload.get('from_date')}` → `{payload.get('to_date')}` · "
        f"{payload.get('hard_red_n')} hard-red sessions · "
        f"hold grid {list(payload.get('hold_grid') or HOLD_GRID)}.",
        "",
        f"**KEEP** `{payload.get('n_keep')}` · **WATCH** "
        f"`{payload.get('n_watch')}` · strategies "
        f"`{payload.get('n_strategies')}`. Live sit stays.",
        "",
        "| Sleeve | Intended | Fired | Hold | n | Win | $ | Verdict |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    show = [s for s in (payload.get("strategies") or []) if s.get("picked")]
    for s in show:
        p = s.get("picked") or {}
        lines.append(
            f"| `{s.get('name')}` | {s.get('side') or ''} | "
            f"{s.get('fired_side') or ''} | "
            f"{p.get('hold') or '—'} | "
            f"{p.get('n_fires') or 0} | "
            f"{hrs._pct(p.get('win_rate'))} | "
            f"{hrs._n(p.get('pnl'))} | "
            f"{p.get('verdict') or '—'} |"
        )
    lines += [
        "",
        "KEEP needs ≥30 fires and >55% after Futubull fees. "
        "This is the opposite-side open fill, not a scoop.",
        "",
        MARK_E,
        "",
    ]
    return "\n".join(lines)


def write_payload(payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2)
    OUT_JSON.write_text(text, encoding="utf-8")
    DASH_FM.parent.mkdir(parents=True, exist_ok=True)
    DASH_SB.parent.mkdir(parents=True, exist_ok=True)
    DASH_FM.write_text(text, encoding="utf-8")
    DASH_SB.write_text(text, encoding="utf-8")
    DASH_HR.parent.mkdir(parents=True, exist_ok=True)
    DASH_HR.write_text(text, encoding="utf-8")
    section = render_md(payload)
    if OUT_MD.is_file():
        raw = OUT_MD.read_text(encoding="utf-8")
        if MARK_B in raw and MARK_E in raw:
            pre = raw.split(MARK_B, 1)[0]
            post = raw.split(MARK_E, 1)[1]
            OUT_MD.write_text(pre.rstrip() + "\n\n" + section + post.lstrip(),
                              encoding="utf-8")
        else:
            OUT_MD.write_text(raw.rstrip() + "\n\n" + section, encoding="utf-8")
    else:
        OUT_MD.write_text("# Hard-red sit experiment\n\n" + section,
                          encoding="utf-8")
    _splice_dash(payload)


def _splice_dash(payload: dict) -> None:
    """Keep the hold×X page and append a flip table before </body>."""
    DASH_PAGE.parent.mkdir(parents=True, exist_ok=True)
    extra = json.dumps(payload)
    marker = "<!-- FLIP_BOARD -->"
    block = f"""{marker}
<div class="wrap">
<h2>Polarity flip — intended side reversed at the open</h2>
<p class="mut" id="flipNote"></p>
<table id="flip"><thead><tr>
<th>Sleeve</th><th>Intended</th><th>Fired</th><th>Hold</th>
<th>n</th><th>Win</th><th>$</th><th>Verdict</th></tr></thead>
<tbody></tbody></table>
</div>
<script>
const F = {extra};
(function(){{
  const n = document.getElementById('flipNote');
  if(n) n.textContent = (F.note||'') + '  KEEP ' + (F.n_keep||0) +
    ' · WATCH ' + (F.n_watch||0);
  const tb = document.querySelector('#flip tbody');
  if(!tb) return;
  function pct(v){{ return v==null ? '—' : (100*v).toFixed(1)+'%'; }}
  function usd(v){{ return v==null ? '—' : (v>=0?'+':'')+Number(v).toFixed(2); }}
  const rows = (F.strategies||[]).filter(function(s){{ return s && s.picked; }});
  rows.forEach(function(s){{
    const p = s.picked||{{}};
    const tr = document.createElement('tr');
    const v = p.verdict||'';
    tr.innerHTML = '<td>'+s.name+'</td><td>'+(s.side||'')+'</td><td>'+
      (s.fired_side||'')+'</td><td>'+(p.hold||'—')+'</td><td>'+(p.n_fires||0)+
      '</td><td>'+pct(p.win_rate)+'</td><td>'+usd(p.pnl)+
      '</td><td class="'+v.toLowerCase()+'">'+v+'</td>';
    tb.appendChild(tr);
  }});
}})();
</script>
"""
    if DASH_PAGE.is_file():
        raw = DASH_PAGE.read_text(encoding="utf-8")
        if marker in raw:
            raw = raw.split(marker, 1)[0]
        if "</body>" in raw:
            raw = raw.replace("</body>", block + "</body>", 1)
        else:
            raw = raw.rstrip() + "\n" + block
        DASH_PAGE.write_text(raw, encoding="utf-8")
    else:
        DASH_PAGE.write_text(
            "<!DOCTYPE html><html><body>" + block + "</body></html>",
            encoding="utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default=book_era.DASHBOARD_START)
    ap.add_argument("--to-date", default="")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    payload = run(
        from_date=args.from_date,
        to_date=args.to_date or None,
        write=args.write,
    )
    print(
        f"[flip] hard-red={payload.get('hard_red_n')} "
        f"strats={payload.get('n_strategies')} "
        f"KEEP={payload.get('n_keep')} WATCH={payload.get('n_watch')}",
        flush=True,
    )
    for s in (payload.get("keeps") or []) + (payload.get("watches") or []):
        p = s.get("picked") or {}
        print(
            f"  {p.get('verdict')} {s.get('name')} intended={s.get('side')} "
            f"fired={s.get('fired_side')} H={p.get('hold')} "
            f"n={p.get('n_fires')} win={p.get('win_rate')} pnl={p.get('pnl')}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
