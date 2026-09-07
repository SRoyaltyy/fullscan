"""Write today's BUY/SELL tickets for every registered strategy.

Heal path: if Daily inputs are missing, run ``src.run_daily`` until the
ranker can print names. Future strategy PRs must append a row to
``STRATEGIES`` and a collector here — otherwise the new book never
lands on the 09:30 generate board.

CLI: python3 -m src.run_generate [--date YYYY-MM-DD] [--force] [--no-heal]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, skip_if_good

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
OUT_DIR = ROOT / "data" / "generate"
DASH_DIR = ROOT / "dashboard" / "generate"
FACTOR_JSON = ROOT / "03_scoreboard" / "factor_mine.json"
EXCEL_SUG = ROOT / "excel_bot" / "suggestions" / "suggestions.csv"
TODAY_JSON = ROOT / "data" / "sleeve_merge" / "today.json"

# Keepers shown on the generate board. Research only — do not remine.
FACTOR_KEEPERS = (
    "union_e_green_h3",
    "union_e_fresh_h3",
    "flatten_h5",
)

# id must stay stable. add=True means Generate writes it; False = collect only.
STRATEGIES = [
    {"id": "stock_book", "name": "Stock book (1d/3d/1w/2w/1m)",
     "family": "book", "live": False, "href": "../"},
    {"id": "flatten_robust", "name": "flatten_robust live card",
     "family": "sleeve merge", "live": True, "href": "../sleeve-merge/"},
    {"id": "paper_io", "name": ".io paper sleeves",
     "family": ".io paper", "live": False, "href": "../"},
    {"id": "sleeve_combine", "name": "Sleeve combine 3d io_boost",
     "family": "sleeve combine", "live": False, "href": "../sleeve-combine/"},
    {"id": "factor_mine", "name": "Factor-mine keepers (research)",
     "family": "factor mine", "live": False, "href": "../factor-mine/"},
    {"id": "excel", "name": "Excel bot cluster signals",
     "family": "excel", "live": False, "href": "../"},
    {"id": "strategy_board", "name": "Strategy board catalog",
     "family": "board", "live": False, "href": "../strategy-board/"},
]


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _run(cmd: list[str], timeout_s: int | None = None) -> int:
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    try:
        r = subprocess.run(
            cmd, cwd=str(ROOT), env=os.environ.copy(), timeout=timeout_s)
    except subprocess.TimeoutExpired:
        print(f"[generate] WARN: timed out after {timeout_s}s: {' '.join(cmd)}",
              flush=True)
        return 124
    return r.returncode


def _tickers(rows) -> list[str]:
    out = []
    for r in rows or []:
        if isinstance(r, str) and r.strip():
            out.append(r.strip().upper())
        elif isinstance(r, dict):
            t = str(r.get("ticker") or "").strip().upper()
            if t:
                out.append(t)
    return out


def collect_stock_book(date: str) -> dict:
    js = ROOT / "data" / "stock_book" / f"{date}_stock_book.json"
    if not js.is_file():
        return {"ok": False, "reason": "book json missing", "horizons": {}}
    try:
        payload = json.loads(js.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return {"ok": False, "reason": str(e), "horizons": {}}
    horizons = {}
    for h, book in (payload.get("books") or {}).items():
        if not isinstance(book, dict):
            continue
        horizons[h] = {
            "buy": _tickers(book.get("buy")),
            "sell": _tickers(book.get("sell")),
        }
    ok = bool(horizons)
    return {
        "ok": ok,
        "reason": "" if ok else "empty books",
        "horizons": horizons,
        "buy": (horizons.get("1d") or {}).get("buy") or [],
        "sell": (horizons.get("1d") or {}).get("sell") or [],
    }


def collect_flatten(date: str) -> dict:
    if not TODAY_JSON.is_file():
        return {"ok": False, "reason": "today.json missing", "buy": [],
                "sell": []}
    try:
        card = json.loads(TODAY_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return {"ok": False, "reason": str(e), "buy": [], "sell": []}
    tickets = card.get("tickets") or []
    buy = [t.get("ticker") for t in tickets
           if isinstance(t, dict) and t.get("side") == "BUY"]
    sell = [t.get("ticker") for t in tickets
            if isinstance(t, dict) and t.get("side") == "SELL"]
    would = (card.get("would_buy") or {}).get("rows") or []
    return {
        "ok": True,
        "reason": card.get("why") or "",
        "policy": card.get("policy"),
        "route": card.get("route"),
        "hard_red": card.get("hard_red"),
        "buy": [str(t).upper() for t in buy if t],
        "sell": [str(t).upper() for t in sell if t],
        "would_buy": _tickers(would),
        "n_holds": card.get("n_holds_open") or 0,
    }


def collect_paper(date: str) -> dict:
    from . import paper_trade
    js = ROOT / "data" / "stock_book" / f"{date}_stock_book.json"
    if not js.is_file():
        return {"ok": False, "reason": "book missing", "sleeves": {}}
    try:
        book = json.loads(js.read_text(encoding="utf-8"))
        picks = paper_trade.picks_from_book(book, 10)
    except (OSError, json.JSONDecodeError, TypeError) as e:
        return {"ok": False, "reason": str(e), "sleeves": {}}
    return {"ok": True, "reason": "", "sleeves": picks,
            "buy": picks.get("1d_top") or [], "sell": []}


def collect_factor_mine(date: str) -> dict:
    """Today's blotter from the already-mined books. Does not remine."""
    if not FACTOR_JSON.is_file():
        return {"ok": False, "reason": "factor_mine.json missing",
                "keepers": {}}
    try:
        doc = json.loads(FACTOR_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return {"ok": False, "reason": str(e), "keepers": {}}
    daily = doc.get("daily") or {}
    keepers = {}
    for name in FACTOR_KEEPERS:
        rows = daily.get(name) or []
        hit = next((r for r in rows if (r.get("date") or "")[:10] == date),
                   None)
        if not hit:
            keepers[name] = {
                "buy": [], "sell": [],
                "reason": f"no {date} row (mine window "
                          f"{doc.get('from_date')}→{doc.get('to_date')})",
            }
            continue
        keepers[name] = {
            "buy": [str(t).upper() for t in (hit.get("bought") or []) if t],
            "sell": [str(t).upper() for t in (hit.get("sold") or []) if t],
            "held": [str(t).upper() for t in (hit.get("held") or []) if t],
            "reason": "",
        }
    any_day = any(k.get("buy") or k.get("sell") or k.get("held")
                  for k in keepers.values())
    return {
        "ok": True,
        "reason": "" if any_day else f"{date} not in mined window",
        "keepers": keepers,
        "window": [doc.get("from_date"), doc.get("to_date")],
    }


def collect_excel(date: str) -> dict:
    if not EXCEL_SUG.is_file():
        return {"ok": False, "reason": "suggestions.csv missing", "rows": []}
    rows = []
    try:
        with EXCEL_SUG.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sig = (row.get("signal_date") or "")[:10]
                run = (row.get("run_date") or "")[:10]
                if sig != date and run != date:
                    continue
                rows.append({
                    "ticker": (row.get("ticker") or "").upper(),
                    "side": (row.get("side") or "").upper(),
                    "strategy": row.get("strategy") or "",
                })
    except OSError as e:
        return {"ok": False, "reason": str(e), "rows": []}
    buy = [r["ticker"] for r in rows if r["side"] == "LONG" and r["ticker"]]
    sell = [r["ticker"] for r in rows
            if r["side"] in ("SHORT", "SELL") and r["ticker"]]
    return {"ok": True, "reason": "" if rows else f"no excel rows for {date}",
            "buy": buy, "sell": sell, "n": len(rows)}


def heal(date: str, force: bool = False) -> None:
    from . import run_daily
    if force or not skip_if_good.check_finviz_scrape(date):
        print("[generate] heal scrape")
        run_daily.scrape(date, force=force)
    if force or not skip_if_good.check_stock_book_all(date):
        print("[generate] heal morning ranker (skip LLM)")
        run_daily.morning(date, force=force, skip_llm=True)


def write_flatten_card(date: str) -> int:
    return _run(
        [sys.executable, "-m", "src.sleeve_merge", "--card",
         "--date", date, "--write-card"],
        timeout_s=180,
    )


def write_paper(date: str) -> int:
    return _run(
        [sys.executable, "-m", "src.paper_trade", "--date", date, "--top", "10"],
        timeout_s=600,
    )


def write_sleeve_combine() -> int:
    return _run(
        [sys.executable, "-m", "src.sleeve_combine_bt",
         "--mode", "io_boost", "--hold", "3d"],
        timeout_s=600,
    )


def write_strategy_board() -> int:
    return _run(
        [sys.executable, "-m", "src.strategy_board", "--write"],
        timeout_s=120,
    )


def write_diag(date: str) -> int:
    return _run(
        [sys.executable, "-m", "src.stock_book_diag", "--date", date,
         "--write", "--no-gh"],
        timeout_s=180,
    )


def _esc(s) -> str:
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;"))


def render_board(doc: dict) -> str:
    rows = []
    for s in doc.get("strategies") or []:
        buy = ", ".join(s.get("buy") or []) or "—"
        sell = ", ".join(s.get("sell") or []) or "—"
        live = " LIVE" if s.get("live") else ""
        href = s.get("href") or ""
        name = _esc(s.get("name"))
        if href:
            name = f"<a href='{_esc(href)}'>{name}</a>"
        rows.append(
            f"<tr><td class='name'>{name}<span class='live'>{live}</span></td>"
            f"<td>{_esc(s.get('family'))}</td>"
            f"<td class='ok'>{'ok' if s.get('ok') else 'miss'}</td>"
            f"<td>{_esc(buy)}</td><td>{_esc(sell)}</td>"
            f"<td class='muted'>{_esc(s.get('reason'))}</td></tr>"
        )
    return f"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Generate {doc.get('date')} — 09:30 tickets</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9;--good:#4ade80}}
body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 ui-sans-serif,system-ui}}
.wrap{{max-width:1100px;margin:0 auto;padding:18px 16px 48px}}
a{{color:#93c5fd}}
.muted{{color:var(--muted);font-size:13px}}
.live{{color:var(--good);font-size:12px}}
table{{border-collapse:collapse;width:100%;background:var(--card)}}
th,td{{border-top:1px solid var(--line);padding:8px 10px;text-align:left;vertical-align:top}}
th{{color:var(--muted);font-size:12px}}
.ok{{text-transform:uppercase;font-size:12px}}
</style></head><body><div class="wrap">
<h1>09:30 tickets — { _esc(doc.get('date')) }</h1>
<p class="muted">Generated { _esc(doc.get('generated')) }. Live production is
<b>flatten_robust</b>. Other rows are would-be books for the same morning.
<a href="../">paper</a> · <a href="../sleeve-merge/">flatten card</a> ·
<a href="../strategy-board/">strategy board</a></p>
<table><thead><tr><th>Strategy</th><th>Family</th><th>Status</th>
<th>BUY</th><th>SELL</th><th>Note</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
</div></body></html>
"""


def write_outputs(doc: dict) -> None:
    date = doc["date"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    tickets = OUT_DIR / f"{date}_tickets.json"
    tickets.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    lines = [
        f"# Generate — {date}",
        "",
        f"Live: **flatten_robust**. Board: `dashboard/generate/`.",
        "",
        "| Strategy | Family | Status | BUY | SELL |",
        "|---|---|---|---|---|",
    ]
    for s in doc.get("strategies") or []:
        buy = ", ".join(s.get("buy") or []) or "—"
        sell = ", ".join(s.get("sell") or []) or "—"
        lines.append(
            f"| {s.get('name')} | {s.get('family')} | "
            f"{'ok' if s.get('ok') else 'miss'} | {buy} | {sell} |"
        )
    (ROOT / "01_daily" / f"{date}_generate.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8")
    (DASH_DIR / "index.html").write_text(render_board(doc), encoding="utf-8")
    print(f"[generate] wrote {tickets}")
    print(f"[generate] wrote dashboard/generate/index.html")


def _row(meta: dict, collected: dict) -> dict:
    out = dict(meta)
    out.update({
        "ok": bool(collected.get("ok")),
        "reason": collected.get("reason") or "",
        "buy": collected.get("buy") or [],
        "sell": collected.get("sell") or [],
    })
    for k in ("horizons", "sleeves", "keepers", "route", "would_buy",
              "hard_red", "n", "window", "policy"):
        if k in collected:
            out[k] = collected[k]
    return out


def run(date: str | None = None, force: bool = False,
        heal_daily: bool = True) -> dict:
    date = date or _today()
    print(f"[generate] {date} force={force} heal={heal_daily}")
    session = skip_if_good._session_date(
        datetime.strptime(date, "%Y-%m-%d").date())
    if heal_daily and (session or force):
        heal(date, force=force)
    elif not session:
        print(f"[generate] {date} is not an NYSE session — collect only")

    write_flatten_card(date)
    # Paper / combine refresh the dashboards the 09:30 tickets must land on.
    write_paper(date)
    write_sleeve_combine()
    write_strategy_board()
    write_diag(date)

    by_id = {s["id"]: s for s in STRATEGIES}
    rows = [
        _row(by_id["stock_book"], collect_stock_book(date)),
        _row(by_id["flatten_robust"], collect_flatten(date)),
        _row(by_id["paper_io"], collect_paper(date)),
        _row(by_id["sleeve_combine"], {
            "ok": (ROOT / "dashboard" / "sleeve-combine" / "index.html").is_file(),
            "reason": "",
            "buy": collect_stock_book(date).get("buy") or [],
            "sell": [],
        }),
        _row(by_id["factor_mine"], collect_factor_mine(date)),
        _row(by_id["excel"], collect_excel(date)),
        _row(by_id["strategy_board"], {
            "ok": (ROOT / "dashboard" / "strategy-board" / "index.html").is_file(),
            "reason": "",
            "buy": [],
            "sell": [],
        }),
    ]
    doc = {
        "date": date,
        "generated": datetime.now(ET).isoformat(timespec="seconds"),
        "live": "flatten_robust",
        "strategies": rows,
    }
    write_outputs(doc)
    return doc


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--no-heal", action="store_true")
    args = ap.parse_args()
    run(date=args.date, force=args.force, heal_daily=not args.no_heal)


if __name__ == "__main__":
    main()
