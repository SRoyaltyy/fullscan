"""One-shot orchestrator: ensure prereqs → stock book → backtest.

Checks artifacts for the target date (default: today America/New_York).
Runs only what is missing (unless --force).

Order
-----
  1. Finviz Elite export (if no membership/export)
  2. segments (labels)
  3. weather
  4. join (match rank)
  5. news_parse + news_actions (if no actions json)
  6. general predict (if no predict md and DEEPSEEK set)
  7. sector predicts (if no recent sector bias and DEEPSEEK set)
  8. stock_book (always)
  9. stock_book_backtest (always)

CLI:
  python -m src.run_stock_book_all [--date YYYY-MM-DD] [--force] [--skip-llm] [--top 25]
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, scoreboard

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _run(cmd: list[str], env: dict | None = None, check: bool = True) -> int:
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    e = os.environ.copy()
    if env:
        e.update(env)
    r = subprocess.run(cmd, cwd=str(ROOT), env=e)
    if check and r.returncode != 0:
        raise SystemExit(f"step failed ({r.returncode}): {' '.join(cmd)}")
    return r.returncode


def _exists(*parts: str) -> bool:
    return (ROOT.joinpath(*parts)).exists()


def _any_glob(*parts_and_pattern: str) -> bool:
    parent = ROOT.joinpath(*parts_and_pattern[:-1])
    pat = parts_and_pattern[-1]
    if not parent.exists():
        return False
    return any(parent.glob(pat))


def _has_membership(date: str) -> bool:
    return (
        _exists("data", "universe", f"{date}_membership.csv")
        or _any_glob("data", "universe", "*_membership.csv")
    )


def _has_weather(date: str) -> bool:
    return (
        _exists("01_daily", "weather", f"{date}_weather.json")
        or _exists("01_daily", "weather", "latest.json")
    )


def _has_join(date: str) -> bool:
    return (
        _exists("data", "join", f"{date}_ranked.csv")
        or _any_glob("data", "join", "*_ranked.csv")
    )


def _has_news_actions(date: str) -> bool:
    return (
        _exists("01_daily", "news", f"{date}_actions.json")
        or _any_glob("01_daily", "news", "*_actions.json")
    )


def _has_general_predict(date: str) -> bool:
    return _exists("01_daily", "general", f"{date}_predict.md")


def _has_recent_sector_bias(max_age_days: int = 5) -> bool:
    board = scoreboard.load()
    cutoff = (datetime.now(ET).date() - timedelta(days=max_age_days)).isoformat()
    for r in board.get("runs", []):
        t = r.get("topic") or ""
        if t.startswith("sector:") and r.get("predicted_direction") and r.get("date", "") >= cutoff:
            return True
    # also check files
    sec_root = ROOT / "01_daily" / "sectors"
    if sec_root.exists():
        for d in sec_root.iterdir():
            if d.is_dir() and d.name >= cutoff:
                if any(d.glob("*_predict.md")):
                    return True
    return False


def _has_stock_book(date: str) -> bool:
    return _exists("data", "stock_book", f"{date}_stock_book.json")


def run(
    date: str | None = None,
    force: bool = False,
    skip_llm: bool = False,
    top: int = 25,
    force_sectors: bool = False,
) -> None:
    date = date or _today()
    print(f"[all] target date={date} force={force} skip_llm={skip_llm}")

    # ---- 1–2 labels ----
    if force or not _has_membership(date):
        # try finviz export (needs secrets in env); non-fatal if fails and old membership exists
        if force or not _any_glob("data", "universe", "*_membership.csv"):
            code = _run([sys.executable, "-m", "collectors.finviz_financials"], check=False)
            if code != 0:
                print("[all] finviz export failed/skipped — will use existing membership if any")
        _run([sys.executable, "-m", "src.segments", "--date", date], check=False)
        if not _has_membership(date) and not _any_glob("data", "universe", "*_membership.csv"):
            raise SystemExit("[all] no membership csv — cannot continue")
    else:
        print("[all] membership OK")

    # ---- 3 weather ----
    if force or not _has_weather(date):
        _run([sys.executable, "-m", "src.weather"], check=False)
        if not _has_weather(date):
            print("[all] WARN: weather missing — join may degrade")
    else:
        print("[all] weather OK")

    # ---- 4 join ----
    if force or not _has_join(date):
        _run([sys.executable, "-m", "src.join", "--date", date], check=False)
        if not _has_join(date) and not _any_glob("data", "join", "*_ranked.csv"):
            raise SystemExit("[all] no join ranked csv — cannot build stock book")
    else:
        print("[all] join OK")

    # ---- 5 news ----
    if force or not _has_news_actions(date):
        _run(
            [sys.executable, "-m", "src.news_parse", "--hours", "48", "--limit", "300", "--date", date],
            check=False,
        )
        _run(
            [sys.executable, "-m", "src.news_actions", "--hours", "48", "--limit", "300", "--date", date],
            check=False,
        )
        if not _has_news_actions(date):
            print("[all] WARN: news actions missing — 1d book will be join/sector only")
    else:
        print("[all] news actions OK")

    # ---- 6 general LLM ----
    if not skip_llm and (force or not _has_general_predict(date)):
        if config.DEEPSEEK_API_KEY:
            _run([sys.executable, "-m", "src.run_predict", "--date", date], check=False)
        else:
            print("[all] skip general predict — no DEEPSEEK_API_KEY")
    else:
        if _has_general_predict(date):
            print("[all] general predict OK")
        else:
            print("[all] general predict skipped")

    # ---- 7 sectors LLM (expensive) ----
    need_sectors = force_sectors or force or not _has_recent_sector_bias(5)
    if not skip_llm and need_sectors:
        if config.DEEPSEEK_API_KEY:
            print("[all] running ALL sector predicts (can take a long time)…")
            _run([sys.executable, "-m", "src.run_sector_predict", "--date", date], check=False)
        else:
            print("[all] skip sector predict — no DEEPSEEK_API_KEY")
    else:
        print("[all] sector bias OK (recent) — not re-running 11 sector LLMs")

    # ---- 8 stock book (always) ----
    _run([sys.executable, "-m", "src.stock_book", "--date", date, "--top", str(top)], check=True)

    # ---- 9 backtest (always) ----
    _run(
        [sys.executable, "-m", "src.stock_book_backtest", "--top", str(top), "--max-books", "30"],
        check=False,
    )

    print("\n[all] DONE")
    print(f"  book:     01_daily/{date}_stock_book.md")
    print("  backtest: 03_scoreboard/STOCK_BOOK_BACKTEST.md")


def main() -> None:
    ap = argparse.ArgumentParser(description="One workflow: prereqs + stock book + backtest")
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true", help="Re-run all steps even if artifacts exist")
    ap.add_argument("--skip-llm", action="store_true", help="Skip general/sector DeepSeek calls")
    ap.add_argument("--force-sectors", action="store_true", help="Always re-run all 11 sector predicts")
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
