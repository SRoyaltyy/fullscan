"""Hard-red short + open−X% scoop: pick X and hold from a sweep.

Research only. Live flatten_robust and Webull sit stay full sit.

(A) short-only — pre-open. Fire the short kid at the 09:30 open.
(B) long dip-scoop — after 09:30. First touch of open−X% (session low).
Hold H = close of the H-th session including entry (1 = same day).

KEEP: ≥30 fires and >55% after Futubull fees. Else WATCH (≥10) or KILL.

CLI: python -m src.hard_red_hold_x --write
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
from src import hard_red_sit_research as hrs
from src import paper_trade as pt
from src import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "03_scoreboard" / "hard_red_sit"
OUT_JSON = OUT_DIR / "hard_red_hold_x.json"
OUT_MD = ROOT / "03_scoreboard" / "HARD_RED_SIT.md"
DASH_FM = ROOT / "dashboard" / "factor-mine" / "hard_red_hold_x.json"
DASH_SB = ROOT / "dashboard" / "strategy-board" / "hard_red_hold_x.json"
DASH_PAGE = ROOT / "dashboard" / "hard-red-sit" / "index.html"
MARK_B = "<!-- HOLD_X_BEGIN -->"
MARK_E = "<!-- HOLD_X_END -->"

HOLD_GRID = (1, 2, 3, 5)
DIP_GRID = fmc.DIP_GRID
KEEP_MIN = hrs.KEEP_MIN_FIRES
KEEP_WIN = hrs.KEEP_WIN
LIVE_COMBO = cb.PAPER_COMBO

# Distinct live / research sleeves — not the full factor-mine cartesian.
# Combo kids first, then short families, then long families used on the board.
SWEEP_NAMES = (
    "short_news_r_macd_h3",
    "union_hot_n4_h1",
    "short_news_r_h3",
    "short_news_or_h3",
    "short_alarm_h3",
    "short_rsi_ob_h3",
    "short_macd_dn_h3",
    "short_last_red_h3",
    "short_extended_h3",
    "short_r_down_h3",
    "union_e_fresh_h3",
    "union_h3",
    "flatten_h3",
    "flatten_h5",
    "flatten_live_h1",
    "ohlc_hot_h3",
    "yday_gainer_h3",
    "union_rsi_os_h3",
    "union_macd_up_h3",
    "union_flow_in_h3",
    "union_news_pack_h1",
    "union_join_vol_green_h1",
    "union_earn_react_h3",
)


def default_recipes() -> list[dict]:
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    out = []
    for name in SWEEP_NAMES:
        rec = rec_by.get(name)
        if rec:
            out.append(rec)
    return out


def _cell(mode: str, hold: int, dip_pct, fires: list[dict]) -> dict:
    st = hrs.fire_stats(fires)
    n = int(st.get("n_fires") or 0)
    wr = st.get("win_rate")
    if n >= KEEP_MIN and wr is not None and float(wr) > KEEP_WIN:
        verdict = "KEEP"
    elif n >= 10 and wr is not None and float(wr) > 0.5:
        verdict = "WATCH"
    else:
        verdict = "KILL"
    return {
        "mode": mode,
        "hold": int(hold),
        "dip_pct": dip_pct,
        "n_fires": n,
        "n_graded": st.get("n_graded"),
        "n_wins": st.get("n_wins"),
        "win_rate": wr,
        "pnl": st.get("pnl"),
        "verdict": verdict,
    }


def pick_cell(cells: list[dict]) -> dict | None:
    """KEEP first, else best win% among n≥10, else best n. Testing picks X/hold."""
    ranked = [c for c in cells if (c.get("n_fires") or 0) > 0]
    if not ranked:
        return None
    keepers = [c for c in ranked if c.get("verdict") == "KEEP"]
    watch = [c for c in ranked if c.get("verdict") == "WATCH"]
    pool = keepers or watch or ranked
    pool = sorted(
        pool,
        key=lambda c: (
            float(c.get("win_rate") or -1),
            int(c.get("n_fires") or 0),
            float(c.get("pnl") or 0),
            -float(c.get("dip_pct") or 0),
            -int(c.get("hold") or 0),
        ),
        reverse=True,
    )
    best = dict(pool[0])
    best["picked_by"] = (
        "KEEP bar" if keepers else
        "best WATCH (n≥10, win>50%)" if watch else
        "best available (below bar)"
    )
    return best


def name_day_fire(ticker: str, date: str, cal: list[str], *,
                  bars, fees, side: str, hold: int,
                  dip_pct: float | None = None) -> dict | None:
    bar = hrs.clock_bar(ticker, date, bars)
    o, low = bar.get("open"), bar.get("low")
    if o is None:
        return None
    if side == "long":
        if dip_pct is None:
            return None
        fill, kind = fmc.dip_limit_px(o, low, dip_pct)
        if fill is None:
            return None
        entry = float(fill)
    else:
        entry = float(o)
        kind = "open_short"
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
        "kind": kind, "open": o, "low": low,
        "dip_pct": dip_pct, "hold": hold,
    }


def _red_dates(cal: list[str], regime) -> list[str]:
    return [r["date"] for r in hrs.hard_red_dates(cal, regime)]


def _rows_on(panel: dict, date: str) -> list[dict]:
    by = panel.get("by_date") or {}
    rows = by.get(date)
    if rows:
        return list(rows)
    return [r for r in (panel.get("rows") or [])
            if str(r.get("date") or "") == date]


def sweep_recipe(rec: dict, panel: dict, red: list[str], cal: list[str],
                 *, bars, fees) -> dict:
    side = rec.get("side") or "long"
    name = rec.get("name") or "?"
    by_day: dict[str, list[str]] = {}
    for date in red:
        picks = []
        try:
            picks = fm.pick_day(_rows_on(panel, date), rec)
        except Exception:
            picks = []
        by_day[date] = [
            str(r.get("ticker") or "").upper()
            for r in picks if r.get("ticker")
        ]
    cells = []
    if side == "short":
        for hold in HOLD_GRID:
            fires = []
            for date, names in by_day.items():
                for t in names:
                    f = name_day_fire(
                        t, date, cal, bars=bars, fees=fees,
                        side="short", hold=hold)
                    if f:
                        fires.append(f)
            cells.append(_cell("short_only", hold, None, fires))
    else:
        for hold in HOLD_GRID:
            for x in DIP_GRID:
                fires = []
                for date, names in by_day.items():
                    for t in names:
                        f = name_day_fire(
                            t, date, cal, bars=bars, fees=fees,
                            side="long", hold=hold, dip_pct=x)
                        if f:
                            fires.append(f)
                cells.append(_cell("dip_scoop", hold, x, fires))
    picked = pick_cell(cells)
    return {
        "name": name,
        "side": side,
        "family": rec.get("family") or rec.get("universe") or "",
        "n_named_days": sum(1 for v in by_day.values() if v),
        "picked": picked,
        "grid": [{k: c.get(k) for k in (
            "mode", "hold", "dip_pct", "n_fires", "win_rate", "pnl",
            "verdict")} for c in cells],
    }


def sweep_combo(panel: dict, red: list[str], cal: list[str],
                *, bars, fees) -> dict:
    spec = cb.combo_spec(LIVE_COMBO)
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    recs = fmc.recs_for_spec(spec, rec_by)
    kids = [sweep_recipe(r, panel, red, cal, bars=bars, fees=fees)
            for r in recs]
    by_rec: dict[str, dict[str, list[str]]] = {}
    for rec in recs:
        day: dict[str, list[str]] = {}
        for date in red:
            try:
                picks = fm.pick_day(_rows_on(panel, date), rec)
            except Exception:
                picks = []
            day[date] = [
                str(r.get("ticker") or "").upper()
                for r in picks if r.get("ticker")
            ]
        by_rec[rec["name"]] = day
    both = []
    for hold in HOLD_GRID:
        for x in DIP_GRID:
            fires = []
            for rec in recs:
                side = rec.get("side") or "long"
                for date, names in by_rec[rec["name"]].items():
                    for t in names:
                        if side == "short":
                            f = name_day_fire(
                                t, date, cal, bars=bars, fees=fees,
                                side="short", hold=hold)
                        else:
                            f = name_day_fire(
                                t, date, cal, bars=bars, fees=fees,
                                side="long", hold=hold, dip_pct=x)
                        if f:
                            fires.append(f)
            both.append(_cell("short_and_scoop", hold, x, fires))
    picked = pick_cell(both)
    return {
        "name": LIVE_COMBO,
        "side": "mixed",
        "family": "combo",
        "kids": [{k: kid.get(k) for k in ("name", "side", "picked")}
                 for kid in kids],
        "picked": picked,
        "grid": [{k: c.get(k) for k in (
            "mode", "hold", "dip_pct", "n_fires", "win_rate", "pnl",
            "verdict")} for c in both],
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
        for x in DIP_GRID:
            fires = []
            for date, names in by_day.items():
                for t in names:
                    f = name_day_fire(
                        t, date, cal, bars=bars, fees=fees,
                        side="long", hold=hold, dip_pct=x)
                    if f:
                        fires.append(f)
            cells.append(_cell("dip_scoop", hold, x, fires))
    return {
        "name": "flatten_io",
        "side": "long",
        "family": "flatten",
        "n_named_days": sum(1 for v in by_day.values() if v),
        "picked": pick_cell(cells),
        "grid": [{k: c.get(k) for k in (
            "mode", "hold", "dip_pct", "n_fires", "win_rate", "pnl",
            "verdict")} for c in cells],
    }


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
    red = _red_dates(cal, regime)
    fees = fees if fees is not None else pt.load_fees()
    if bars is None:
        try:
            from src import walkforward_factor_mine as wf
            bars = wf.preload_bars(panel)
        except Exception:
            bars = None
    recs = recipes if recipes is not None else default_recipes()
    sleeves = []
    for rec in recs:
        sleeves.append(sweep_recipe(
            rec, panel, red, cal, bars=bars, fees=fees))
    combo = sweep_combo(panel, red, cal, bars=bars, fees=fees)
    flatten = sweep_flatten(red, cal, bars=bars, fees=fees)
    board = [combo, flatten] + sleeves
    keeps = [s for s in board if (s.get("picked") or {}).get("verdict") == "KEEP"]
    watches = [s for s in board
               if (s.get("picked") or {}).get("verdict") == "WATCH"]
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "tag": "RESEARCH",
        "live_sit": True,
        "live_untouched": ["flatten_robust", LIVE_COMBO],
        "from_date": cal[0] if cal else from_date,
        "to_date": cal[-1] if cal else to_date,
        "n_sessions": len(cal),
        "hard_red_n": len(red),
        "hard_red_days": red,
        "hold_grid": list(HOLD_GRID),
        "dip_grid": list(DIP_GRID),
        "keep_bar": {"min_fires": KEEP_MIN, "win": KEEP_WIN},
        "n_strategies": len(board),
        "n_keep": len(keeps),
        "n_watch": len(watches),
        "keeps": [_slim_pick(s) for s in keeps],
        "watches": [_slim_pick(s) for s in watches],
        "strategies": [_slim_pick(s) for s in board],
        "note": (
            "Paper counterfactual. Live sit unchanged. "
            "(A) short-only at the 09:30 open. "
            "(B) long scoop at first touch of open−X%. "
            "Hold H = close of the H-th session including entry. "
            "X and H are the tested winners on this window, not a guess."
        ),
    }
    if write:
        write_payload(payload)
    return payload


def _slim_pick(s: dict) -> dict:
    p = s.get("picked") or {}
    return {
        "name": s.get("name"),
        "side": s.get("side"),
        "family": s.get("family"),
        "n_named_days": s.get("n_named_days"),
        "picked": p or None,
        "grid": s.get("grid") or [],
        "kids": s.get("kids"),
    }


def render_md(payload: dict) -> str:
    lines = [
        MARK_B,
        "",
        "## Hold × X sweep (research, not a wire)",
        "",
        str(payload.get("note") or ""),
        "",
        f"Window `{payload.get('from_date')}` → `{payload.get('to_date')}` · "
        f"{payload.get('hard_red_n')} hard-red sessions · "
        f"hold grid {list(payload.get('hold_grid') or HOLD_GRID)} · "
        f"X grid {list(payload.get('dip_grid') or DIP_GRID)}.",
        "",
        f"**KEEP** `{payload.get('n_keep')}` · **WATCH** "
        f"`{payload.get('n_watch')}` · strategies "
        f"`{payload.get('n_strategies')}`. Live sit stays.",
        "",
        "| Sleeve | Side | Mode | X% | Hold | n | Win | $ | Verdict |",
        "|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    show = list(payload.get("keeps") or []) + list(payload.get("watches") or [])
    if not show:
        show = list(payload.get("strategies") or [])[:24]
    for s in show:
        p = s.get("picked") or {}
        x = p.get("dip_pct")
        lines.append(
            f"| `{s.get('name')}` | {s.get('side') or ''} | "
            f"{p.get('mode') or '—'} | "
            f"{'—' if x is None else f'{x:g}'} | "
            f"{p.get('hold') or '—'} | "
            f"{p.get('n_fires') or 0} | "
            f"{hrs._pct(p.get('win_rate'))} | "
            f"{hrs._n(p.get('pnl'))} | "
            f"{p.get('verdict') or '—'} |"
        )
    lines += [
        "",
        "KEEP needs ≥30 fires and >55% after Futubull fees. "
        "WATCH is n≥10 and win>50% — not a live wire. "
        "Close grades; it does not trigger the scoop.",
        "",
        MARK_E,
        "",
    ]
    return "\n".join(lines)


def _dash_html(payload: dict) -> str:
    body = json.dumps(payload)
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hard-red hold × X — research</title>
<style>
 body{{background:#0f1420;color:#dfe6f2;font:13px/1.45 -apple-system,Segoe UI,sans-serif;margin:0}}
 .wrap{{max-width:1100px;margin:0 auto;padding:16px}}
 h1{{font-size:20px;margin:0 0 6px}}
 .mut{{color:#8b96ab}}
 .pos{{color:#4ade80}} .neg{{color:#f87171}} .gold{{color:#fbbf24}}
 table{{width:100%;border-collapse:collapse;margin:10px 0;font-size:12px}}
 th,td{{padding:5px 6px;border-bottom:1px solid #262f45;text-align:right}}
 th:first-child,td:first-child{{text-align:left}}
 .keep{{color:#4ade80}} .watch{{color:#fbbf24}} .kill{{color:#f87171}}
</style></head><body><div class="wrap">
<h1>Hard-red short + scoop — hold × X</h1>
<p class="mut" id="note"></p>
<p class="mut">Research only. Live <code>flatten_robust</code> and Webull sit unchanged.</p>
<h2>Picked X and hold</h2>
<table id="tbl"><thead><tr>
<th>Sleeve</th><th>Side</th><th>Mode</th><th>X%</th><th>Hold</th>
<th>n</th><th>Win</th><th>$</th><th>Verdict</th></tr></thead>
<tbody></tbody></table>
<p class="mut">Full grid is in <code>hard_red_hold_x.json</code>.</p>
</div>
<script>
const D = {body};
document.getElementById('note').textContent =
  (D.note||'') + '  ' + (D.from_date||'') + ' → ' + (D.to_date||'') +
  ' · hard-red ' + (D.hard_red_n||0) +
  ' · KEEP ' + (D.n_keep||0) + ' · WATCH ' + (D.n_watch||0);
const rows = (D.keeps||[]).concat(D.watches||[]);
const tb = document.querySelector('#tbl tbody');
function pct(v){{ return v==null ? '—' : (100*v).toFixed(1)+'%'; }}
function usd(v){{ return v==null ? '—' : (v>=0?'+':'')+Number(v).toFixed(2); }}
(rows.length?rows:(D.strategies||[]).slice(0,30)).forEach(function(s){{
  const p = s.picked||{{}};
  const tr = document.createElement('tr');
  const v = p.verdict||'';
  tr.innerHTML = '<td>'+s.name+'</td><td>'+(s.side||'')+'</td><td>'+(p.mode||'—')+
    '</td><td>'+(p.dip_pct==null?'—':p.dip_pct)+'</td><td>'+(p.hold||'—')+
    '</td><td>'+(p.n_fires||0)+'</td><td>'+pct(p.win_rate)+'</td><td>'+usd(p.pnl)+
    '</td><td class="'+v.toLowerCase()+'">'+v+'</td>';
  tb.appendChild(tr);
}});
</script></body></html>
"""


def write_payload(payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2)
    OUT_JSON.write_text(text, encoding="utf-8")
    DASH_FM.parent.mkdir(parents=True, exist_ok=True)
    DASH_SB.parent.mkdir(parents=True, exist_ok=True)
    DASH_PAGE.parent.mkdir(parents=True, exist_ok=True)
    DASH_FM.write_text(text, encoding="utf-8")
    DASH_SB.write_text(text, encoding="utf-8")
    DASH_PAGE.write_text(_dash_html(payload), encoding="utf-8")
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
        f"[hold-x] hard-red={payload.get('hard_red_n')} "
        f"strats={payload.get('n_strategies')} "
        f"KEEP={payload.get('n_keep')} WATCH={payload.get('n_watch')}",
        flush=True,
    )
    for s in (payload.get("keeps") or [])[:12]:
        p = s.get("picked") or {}
        print(
            f"  KEEP {s.get('name')} {p.get('mode')} "
            f"X={p.get('dip_pct')} H={p.get('hold')} "
            f"n={p.get('n_fires')} win={p.get('win_rate')}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
