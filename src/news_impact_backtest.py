"""Specialised news-impact backtest.

Classifies the Grok/pipeline parse book, optionally hops the $0 Lane
stack, grades named tickers / theme ETFs against the tape, and writes a
human markdown table:

  article · published · retrieved · LLM(s) · reasoning · up/down · actual

Does not touch flatten / Webull / factor-mine.

CLI: python -m src.news_impact_backtest --date all
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.news_impact.backtest import markdown, run_backtest
from src.news_impact.grade import grade_results, performance_rollup

NEWS_DIR = Path("01_daily/news")
SCOREBOARD = Path("03_scoreboard/NEWS_IMPACT_BACKTEST.md")


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def run(
    date: str = "all",
    limit: int = 0,
    use_lane: bool = False,
    use_search: bool = False,
    prices: bool = True,
    persist: bool = False,
) -> dict:
    report = run_backtest(
        date,
        limit=limit,
        persist=persist,
        use_lane=use_lane,
        use_search=use_search,
        keep_results=True,
    )
    rows = report.get("results") or []
    if prices:
        rows = grade_results(rows, fetch=True)
        report["results"] = rows
        report["tape"] = performance_rollup(rows)
    label = "all" if str(date).lower() in {"all", "*", "history"} else date
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    out_json = NEWS_DIR / f"{label}_news_impact_backtest.json"
    out_md = NEWS_DIR / f"{label}_news_impact_backtest.md"
    md = markdown(report)
    slim = dict(report)
    _write(out_json, json.dumps(slim, indent=2, ensure_ascii=False))
    _write(out_md, md)
    _write(SCOREBOARD, md)
    tape = report.get("tape") or {}
    print(
        "news_impact_backtest", label,
        "old", (report.get("old") or {}).get("usable_ratio"),
        "new", (report.get("new") or {}).get("usable_ratio"),
        "rescued", report.get("rescued_n"),
        "killed", report.get("killed_n"),
        "hit_1d", tape.get("hit_rate_1d"),
        "hit_20d", tape.get("hit_rate_20d"),
        "→", out_md,
    )
    return report


def main() -> None:
    ap = argparse.ArgumentParser(
        description="News-impact backtest → human markdown table + tape grades",
    )
    ap.add_argument("--date", default="all", help="YYYY-MM-DD or all")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--lane", action="store_true", help="use $0 Lane hops when keys exist")
    ap.add_argument("--search", action="store_true", help="Google AI Overview + web parse")
    ap.add_argument(
        "--prices", dest="prices", action="store_true", default=True,
        help="grade named tickers / theme ETFs against the tape (default)",
    )
    ap.add_argument("--no-prices", dest="prices", action="store_false")
    ap.add_argument("--persist", action="store_true", help="write scratch_impact rows")
    args = ap.parse_args()
    run(
        args.date,
        limit=args.limit,
        use_lane=args.lane,
        use_search=args.search,
        prices=args.prices,
        persist=args.persist,
    )


if __name__ == "__main__":
    main()
