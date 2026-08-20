#!/usr/bin/env python3
"""Add Finviz-chart E (earnings) / R (analyst) PIT columns to ab_backfill.py."""
from pathlib import Path

p = Path("src/ab_backfill.py")
t = p.read_text(encoding="utf-8")
if "event_markers as em" in t and "flag_E" in t and "_er_chip" in t:
    print("E/R already patched")
    raise SystemExit(0)

if "from . import ab_context_daily as ctx" in t and "event_markers" not in t:
    t = t.replace(
        "from . import ab_context_daily as ctx\n",
        "from . import ab_context_daily as ctx\nfrom . import event_markers as em\n",
        1,
    )

if "events_by_ticker" not in t:
    t = t.replace(
        'print(f"[backfill] corr map tickers={len(corr_map):,}")',
        'print(f"[backfill] corr map tickers={len(corr_map):,}")\n'
        "    events_by_ticker: dict[str, pd.DataFrame] = {}",
        1,
    )

if "Prefetch E/R" not in t:
    # after tickers list is ready — look for sessions print
    needle = 'print(f"[backfill] sessions={len(days)}'
    i = t.find(needle)
    if i < 0:
        raise SystemExit("sessions print not found")
    # insert after next newline following peers= line if present
    j = t.find("\n", i)
    # try to land after the sector/peers debug line
    k = t.find('print(f"[backfill] {ticker}:',
              i)
    if k > 0:
        j = t.find("\n", k)
    block = '''
    # Prefetch E/R event markers (yfinance) for requested names
    for _t in tickers:
        try:
            ev = em.fetch(_t)
            events_by_ticker[_t] = ev
            if len(ev):
                em.save(_t, ev)
            nE = int((ev["kind"] == "E").sum()) if len(ev) else 0
            nR = int((ev["kind"] == "R").sum()) if len(ev) else 0
            print(f"[backfill] events {_t}: E={nE} R={nR}")
        except Exception as e:
            print(f"[backfill] events {_t} skip: {e}")
            events_by_ticker[_t] = pd.DataFrame()
'''
    t = t[: j + 1] + block + t[j + 1 :]

if "esnap = em.asof_snapshot" not in t:
    old = '            secc = ctx.sector_context_asof(sec_n, asof, boards)\n\n            p01, p02, p03, p04 = peer["P01"], peer["P02"], indc["P03"], secc["P04"]'
    new = '            secc = ctx.sector_context_asof(sec_n, asof, boards)\n            esnap = em.asof_snapshot(events_by_ticker.get(t, pd.DataFrame()), asof)\n\n            p01, p02, p03, p04 = peer["P01"], peer["P02"], indc["P03"], secc["P04"]'
    if old not in t:
        raise SystemExit("secc block not found")
    t = t.replace(old, new, 1)

if '"flag_E"' not in t:
    t = t.replace(
        '"P04": p04,\n',
        '"P04": p04,\n'
        '                "last_E_date": esnap.get("last_E_date"),\n'
        '                "last_E_color": esnap.get("last_E_color"),\n'
        '                "last_E_label": esnap.get("last_E_label"),\n'
        '                "last_E_surprise": esnap.get("last_E_surprise"),\n'
        '                "days_since_E": esnap.get("days_since_E"),\n'
        '                "flag_E": esnap.get("flag_E", 0),\n'
        '                "last_R_date": esnap.get("last_R_date"),\n'
        '                "last_R_color": esnap.get("last_R_color"),\n'
        '                "last_R_label": esnap.get("last_R_label"),\n'
        '                "days_since_R": esnap.get("days_since_R"),\n'
        '                "flag_R": esnap.get("flag_R", 0),\n',
        1,
    )

if "_er_chip" not in t:
    helper = '''
def _er_chip(kind, color, label, surprise=None, days=None) -> str:
    chip = {"green": "🟢", "red": "🔴", "white": "⚪"}.get(str(color or ""), "⚪")
    if not label and not color:
        return "—"
    extra = ""
    if kind == "E" and surprise is not None and np.isfinite(surprise):
        extra = f"{surprise:+.0f}%"
    elif days is not None and np.isfinite(days):
        extra = f"d{int(days)}"
    return f"{chip}{label or kind}{(' ' + extra) if extra else ''}"


'''
    t = t.replace("def _score_cell(score) -> str:", helper + "def _score_cell(score) -> str:", 1)

# Trail header/row — only if colored MD path exists
if "| context | rs5d |" in t and "| E | R |" not in t:
    t = t.replace(
        "| date | score | ctx | enr | context | rs5d | sec | board_date | 1d | 3d | 1w | 2m |",
        "| date | score | ctx | enr | context | E | R | rs5d | sec | board_date | 1d | 3d | 1w | 2m |",
        1,
    )
    t = t.replace(
        "|------|-------|----:|-----|---------|------|-----|------------|:--:|:--:|:--:|:--:|",
        "|------|-------|----:|-----|---------|---|---|------|-----|------------|:--:|:--:|:--:|:--:|",
        1,
    )

if "_er_chip('E'" not in t and "_label_colored(r)" in t:
    # inject E/R after context chip in trail rows
    t = t.replace(
        "f\"{_score_cell(r['score_enriched'])} | {_label_colored(r)} | \"\n",
        "f\"{_score_cell(r['score_enriched'])} | {_label_colored(r)} | \"\n"
        "                f\"{_er_chip('E', r.get('last_E_color'), r.get('last_E_label'), r.get('last_E_surprise'), r.get('days_since_E'))} | \"\n"
        "                f\"{_er_chip('R', r.get('last_R_color'), r.get('last_R_label'), None, r.get('days_since_R'))} | \"\n",
        1,
    )

if "Finviz chart E / R" not in t and "Legend — P01" in t:
    t = t.replace(
        '"Label chips:',
        '"## Legend — Finviz chart E / R markers",\n'
        '        "",\n'
        '        "| Marker | Meaning | Green | Red | Source |",\n'
        '        "|--------|---------|-------|-----|--------|",\n'
        '        "| **E** | Earnings (chart E) | EPS beat | EPS miss | yfinance earnings dates, PIT <= asof |",\n'
        '        "| **R** | Analyst action (chart R) | Upgrade | Downgrade | yfinance recommendations |",\n'
        '        "",\n'
        '        "Trail shows the most recent E and R on or before that day.",\n'
        '        "",\n'
        '        "Label chips:',
        1,
    )

p.write_text(t, encoding="utf-8")
print("patched E/R", p.stat().st_size)
assert "event_markers as em" in t
assert "flag_E" in t
