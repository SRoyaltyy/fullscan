"""One-shot orchestrator: ensure THIS trading day's prereqs → stock book → backtest.

All "already done?" checks are for the target trading day only.
Yesterday's files do not count as done for today.

At start, prints a clear status table by workflow *name* (not yml file).

CLI:
  python -m src.run_stock_book_all [--date YYYY-MM-DD] [--force] [--skip-llm] [--top 25]
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _run(cmd: list[str], check: bool = True) -> int:
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, cwd=str(ROOT), env=os.environ.copy())
    if check and r.returncode != 0:
        raise SystemExit(f"step failed ({r.returncode}): {' '.join(cmd)}")
    return r.returncode


def _p(*parts: str) -> Path:
    return ROOT.joinpath(*parts)


def _exists(*parts: str) -> bool:
    return _p(*parts).exists()


def _status_for_day(date: str) -> list[dict]:
    """One row per logical workflow. done = artifact for THIS date exists."""
    sector_dir = _p("01_daily", "sectors", date)
    sector_n = 0
    if sector_dir.is_dir():
        sector_n = len(list(sector_dir.glob("*_predict.md")))
    sector_done = sector_n >= 11
    sector_partial = sector_n > 0 and not sector_done

    rows = [
        {
            "name": "Finviz universe export",
            "key": "finviz",
            "done": _exists("data", "exports", f"finviz_{date}.csv"),
            "artifact": f"data/exports/finviz_{date}.csv",
            "required": False,
        },
        {
            "name": "Stock labeling (segments)",
            "key": "segments",
            "done": _exists("data", "universe", f"{date}_membership.csv"),
            "artifact": f"data/universe/{date}_membership.csv",
            "required": True,
        },
        {
            "name": "Weather / regime",
            "key": "weather",
            "done": _exists("01_daily", "weather", f"{date}_weather.json"),
            "artifact": f"01_daily/weather/{date}_weather.json",
            "required": True,
        },
        {
            "name": "Join / match rank",
            "key": "join",
            "done": _exists("data", "join", f"{date}_ranked.csv"),
            "artifact": f"data/join/{date}_ranked.csv",
            "required": True,
        },
        {
            "name": "News parse",
            "key": "news_parse",
            "done": _exists("01_daily", "news", f"{date}_parsed.json")
            or _exists("01_daily", "news", f"{date}_parsed.md"),
            "artifact": f"01_daily/news/{date}_parsed.*",
            "required": False,
        },
        {
            "name": "News actions (ticker edges)",
            "key": "news_actions",
            "done": _exists("01_daily", "news", f"{date}_actions.json"),
            "artifact": f"01_daily/news/{date}_actions.json",
            "required": False,
        },
        {
            "name": "General market predict",
            "key": "general_predict",
            "done": _exists("01_daily", "general", f"{date}_predict.md"),
            "artifact": f"01_daily/general/{date}_predict.md",
            "required": False,
        },
        {
            "name": "Per-sector predict (all 11)",
            "key": "sector_predict",
            "done": sector_done,
            "partial": sector_partial,
            "detail": f"{sector_n}/11 sector predict files",
            "artifact": f"01_daily/sectors/{date}/*_predict.md",
            "required": False,
        },
        {
            "name": "Stock book (suggestions)",
            "key": "stock_book",
            "done": _exists("data", "stock_book", f"{date}_stock_book.json"),
            "artifact": f"data/stock_book/{date}_stock_book.json",
            "required": True,
        },
        {
            "name": "Stock book backtest",
            "key": "backtest",
            "done": _exists("03_scoreboard", "STOCK_BOOK_BACKTEST.md"),
            "artifact": "03_scoreboard/STOCK_BOOK_BACKTEST.md (repo-level, re-run daily)",
            "required": True,
        },
        {
            "name": "Paper trading dashboard",
            "key": "paper",
            "done": _exists("03_scoreboard", "PAPER_TRADING.md"),
            "artifact": "dashboard/index.html + 03_scoreboard/PAPER_TRADING.md (repo-level, re-run daily)",
            "required": True,
        },
    ]
    return rows


def _print_status(date: str, rows: list[dict]) -> None:
    print("")
    print("=" * 72)
    print(f"  TRADING DAY STATUS — {date} (America/New_York)")
    print("  Only artifacts dated this day count as DONE.")
    print("=" * 72)
    print(f"{'Workflow':<36} {'Status':<14} Artifact")
    print("-" * 72)
    for r in rows:
        if r.get("partial"):
            st = f"PARTIAL ({r.get('detail', '')})"
        elif r["done"]:
            st = "DONE"
        else:
            st = "NOT RUN"
        print(f"{r['name']:<36} {st:<14} {r['artifact']}")
    print("-" * 72)
    done_n = sum(1 for r in rows if r["done"] and not r.get("partial"))
    print(f"  {done_n}/{len(rows)} workflows complete for {date}")
    print("=" * 72)
    print("")


def run(
    date: str | None = None,
    force: bool = False,
    skip_llm: bool = False,
    top: int = 25,
    force_sectors: bool = False,
) -> None:
    date = date or _today()
    rows = _status_for_day(date)
    by_key = {r["key"]: r for r in rows}
    _print_status(date, rows)

    print(f"[all] plan force={force} skip_llm={skip_llm} force_sectors={force_sectors}")

    def need(key: str) -> bool:
        if force:
            return True
        r = by_key[key]
        if r.get("partial"):
            return True
        return not r["done"]

    # ---- Finviz + labels ----
    if need("finviz") or need("segments"):
        if need("finviz"):
            print("[all] → Finviz universe export (this trading day)")
            code = _run([sys.executable, "-m", "collectors.finviz_financials"], check=False)
            if code != 0:
                print("[all] WARN: Finviz export failed — labeling may fail without today's file")
        else:
            print("[all] skip Finviz universe export (DONE for this day)")

        if need("segments"):
            print("[all] → Stock labeling / segments")
            _run([sys.executable, "-m", "src.segments", "--date", date], check=False)
            if not _exists("data", "universe", f"{date}_membership.csv"):
                raise SystemExit(
                    f"[all] FATAL: no membership for {date}. "
                    "Cannot use yesterday's labels as today's."
                )
        else:
            print("[all] skip Stock labeling (DONE for this day)")
    else:
        print("[all] skip Finviz + labeling (DONE for this day)")

    # ---- Weather (MUST pass --date) ----
    if need("weather"):
        print("[all] → Weather / regime")
        _run([sys.executable, "-m", "src.weather", "--date", date], check=False)
        if not _exists("01_daily", "weather", f"{date}_weather.json"):
            raise SystemExit(
                f"[all] FATAL: weather did not write 01_daily/weather/{date}_weather.json. "
                f"Cannot join without this trading day's weather."
            )
    else:
        print("[all] skip Weather / regime (DONE for this day)")

    # ---- Join (strict for this date) ----
    if need("join"):
        print("[all] → Join / match rank")
        _run([sys.executable, "-m", "src.join", "--date", date], check=False)
        if not _exists("data", "join", f"{date}_ranked.csv"):
            raise SystemExit(
                f"[all] FATAL: no join ranked file for {date}. "
                "Yesterday's rank cannot substitute."
            )
    else:
        print("[all] skip Join / match rank (DONE for this day)")

    # ---- News ----
    if need("news_parse") or need("news_actions"):
        if need("news_parse"):
            print("[all] → News parse")
            _run(
                [sys.executable, "-m", "src.news_parse", "--hours", "48", "--limit", "300", "--date", date],
                check=False,
            )
        else:
            print("[all] skip News parse (DONE for this day)")
        if need("news_actions"):
            print("[all] → News actions (ticker edges)")
            _run(
                [sys.executable, "-m", "src.news_actions", "--hours", "48", "--limit", "300", "--date", date],
                check=False,
            )
            if not _exists("01_daily", "news", f"{date}_actions.json"):
                print("[all] WARN: news actions missing for", date, "— 1d book weaker")
        else:
            print("[all] skip News actions (DONE for this day)")
    else:
        print("[all] skip News parse + actions (DONE for this day)")

    # ---- General predict ----
    if not skip_llm and need("general_predict"):
        if config.DEEPSEEK_API_KEY:
            print("[all] → General market predict")
            _run([sys.executable, "-m", "src.run_predict", "--date", date], check=False)
        else:
            print("[all] skip General market predict (no DEEPSEEK_API_KEY)")
    elif skip_llm:
        print("[all] skip General market predict (--skip-llm)")
    else:
        print("[all] skip General market predict (DONE for this day)")

    # ---- Sector predicts ----
    if not skip_llm and (force_sectors or need("sector_predict")):
        if config.DEEPSEEK_API_KEY:
            print("[all] → Per-sector predict (all 11 for this trading day)")
            _run([sys.executable, "-m", "src.run_sector_predict", "--date", date], check=False)
        else:
            print("[all] skip Per-sector predict (no DEEPSEEK_API_KEY)")
    elif skip_llm:
        print("[all] skip Per-sector predict (--skip-llm)")
    else:
        print("[all] skip Per-sector predict (DONE for this day — 11/11)")

    # ---- Stock book always ----
    print("[all] → Stock book (1d / 3d / 1w / 2w / 1m)")
    _run([sys.executable, "-m", "src.stock_book", "--date", date, "--top", str(top)], check=True)

    # ---- Backtest always ----
    print("[all] → Stock book backtest")
    _run(
        [sys.executable, "-m", "src.stock_book_backtest", "--top", str(top), "--max-books", "30"],
        check=False,
    )

    # ---- Paper trading always (rebuilds from all books; idempotent) ----
    print("[all] → Paper trading (Futubull-fee simulation + dashboard)")
    _run(
        [sys.executable, "-m", "src.paper_trade", "--date", date, "--top", "10"],
        check=False,
    )

    print("\n[all] FINAL STATUS after run:")
    _print_status(date, _status_for_day(date))
    print(f"[all] book → 01_daily/{date}_stock_book.md")
    print("[all] backtest → 03_scoreboard/STOCK_BOOK_BACKTEST.md")
    print("[all] paper trading → dashboard/index.html + 03_scoreboard/PAPER_TRADING.md")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="Trading day YYYY-MM-DD (default today ET)")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--skip-llm", action="store_true")
    ap.add_argument("--force-sectors", action="store_true")
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()
    run(
        date=args.date,
        force=args.force,
        skip_llm=args.skip_llm,
        top=args.top,
        force_sectors=args.force_sectors,
    )


if __name__ == "__main__":
    main()
