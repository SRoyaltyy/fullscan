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


def _weather_has_sectors(date: str) -> bool:
    p = _p("01_daily", "weather", f"{date}_weather.json")
    if not p.exists():
        return False
    try:
        import json
        d = json.loads(p.read_text(encoding="utf-8"))
        secs = (d.get("signals") or {}).get("sectors") or {}
        return len(secs) >= 5
    except Exception:
        return False


def _events_n(date: str) -> int:
    import json
    for name in (f"{date}_events.json", "latest.json"):
        p = _p("01_daily", "events", name)
        if not p.exists():
            continue
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if name == "latest.json":
            sd = str(d.get("scan_date") or "")
            if sd and sd != date:
                continue
        n = len(d.get("events") or [])
        if n:
            return n
    return 0


def _events_quality_ok(date: str) -> bool:
    """QC verdict, not existence: carry-forwards and stubs are NOT done."""
    from . import output_qc
    return output_qc.qc_events_date(date).ok


def _judge_quality_ok(date: str) -> bool:
    from . import output_qc
    return output_qc.qc_news_judge(
        _p("01_daily", "news", f"{date}_judge.md")).ok


def _general_predict_quality_ok(date: str) -> bool:
    from . import output_qc
    return output_qc.qc_general_predict(
        _p("01_daily", "general", f"{date}_predict.md")).ok


def _sector_quality_n(date: str) -> int:
    import re as _re

    from . import output_qc
    from .sector_taxonomy import FINVIZ_SECTORS

    def slug(s: str) -> str:
        return _re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")

    d = _p("01_daily", "sectors", date)
    return sum(1 for s in FINVIZ_SECTORS
               if output_qc.qc_sector_predict(d / f"{slug(s)}_predict.md").ok)


def _catalyst_quality_ok(date: str) -> bool:
    """Two usable dossiers for this date. Skip-if-good uses the same bar."""
    from . import catalyst_daily
    return catalyst_daily.already_good(date)


def _ab_raw(date: str) -> bool:
    return _exists("data", "ab_checklist", f"{date}_ab_checklist.csv")


def _ab_enriched(date: str) -> bool:
    return _exists("data", "ab_checklist", f"{date}_ab_checklist_enriched.csv")


def _status_for_day(date: str) -> list[dict]:
    """One row per logical workflow. done = artifact for THIS date exists."""
    sector_n = _sector_quality_n(date)
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
            "done": _weather_has_sectors(date),
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
            "name": "Peer relative strength",
            "key": "peer_rs",
            "done": _exists("data", "peers", f"{date}_peer_rs.csv"),
            "artifact": f"data/peers/{date}_peer_rs.csv",
            "required": False,
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
            "name": "Finviz daily digest",
            "key": "finviz_digest",
            "done": _exists("01_daily", "news", f"{date}_finviz_digest.json")
            or _exists("01_daily", "news", f"{date}_finviz_digest.md"),
            "artifact": f"01_daily/news/{date}_finviz_digest.*",
            "required": False,
        },
        {
            "name": "News judge (LLM rank)",
            "key": "news_judge",
            "done": _judge_quality_ok(date),
            "artifact": f"01_daily/news/{date}_judge.md",
            "required": True,
        },
        {
            "name": "News actions (ticker edges)",
            "key": "news_actions",
            "done": _exists("01_daily", "news", f"{date}_actions.json"),
            "artifact": f"01_daily/news/{date}_actions.json",
            "required": False,
        },
        {
            "name": "Catalyst daily (dossiers)",
            "key": "catalyst",
            "done": _catalyst_quality_ok(date),
            "artifact": f"01_daily/catalyst/{date}_dossiers.json",
            "required": False,
        },
        {
            "name": "Event scanner",
            "key": "events",
            "done": _events_quality_ok(date),
            "artifact": f"01_daily/events/{date}_events.json",
            "required": False,
        },
        {
            "name": "General market predict",
            "key": "general_predict",
            "done": _general_predict_quality_ok(date),
            "artifact": f"01_daily/general/{date}_predict.md",
            "required": False,
        },
        {
            "name": "Per-sector predict (all 11)",
            "key": "sector_predict",
            "done": sector_done,
            "partial": sector_partial,
            "detail": f"{sector_n}/11 quality sector predicts",
            "artifact": f"01_daily/sectors/{date}/*_predict.md",
            "required": False,
        },
        {
            "name": "Ticker checklist (rebound)",
            "key": "ticker_checklist",
            "done": _exists("data", "checklist", f"{date}_checklist.csv"),
            "artifact": f"data/checklist/{date}_checklist.csv",
            "required": False,
        },
        {
            "name": "AB checklist + peer enrich",
            "key": "ab",
            "done": _ab_enriched(date),
            "partial": _ab_raw(date) and not _ab_enriched(date),
            "detail": "raw only, enrich missing" if (_ab_raw(date) and not _ab_enriched(date)) else "",
            "artifact": f"data/ab_checklist/{date}_ab_checklist_enriched.csv",
            "required": True,
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

    if skip_llm:
        print("[all] skip Event scanner (--skip-llm)")
    elif need("events"):
        print("[all] → Event scanner (primary)")
        _run([sys.executable, "-m", "src.run_events", "--date", date], check=False)
        print("[all] → Event catcher (gap hunt / replacement if primary empty)")
        _run([sys.executable, "-m", "src.run_events_catcher", "--date", date], check=False)
        if _events_n(date) == 0:
            print("[all] → Events carry-forward fallback (both passes empty)")
            _run([sys.executable, "-m", "src.events_fallback", "--date", date], check=False)
        if _events_n(date) == 0:
            print("[all] WARN: events still empty for", date, "— weather/book sector tilt missing")
    else:
        print("[all] skip Event scanner (DONE for this day)")

    if need("news_parse"):
        print("[all] → News parse")
        _run(
            [sys.executable, "-m", "src.news_parse", "--hours", "48", "--limit", "400", "--date", date],
            check=False,
        )
    else:
        print("[all] skip News parse (DONE for this day)")
    if need("finviz_digest"):
        print("[all] → Finviz daily digest")
        _run([sys.executable, "-m", "src.finviz_digest", "--date", date], check=False)
    else:
        print("[all] skip Finviz daily digest (DONE for this day)")
    if need("news_judge") and not skip_llm and config.has_llm():
        print("[all] → News judge")
        _run([sys.executable, "-m", "src.run_news_judge", "--date", date], check=False)
        if not _exists("01_daily", "news", f"{date}_judge.md"):
            print("[all] WARN: news judge missing for", date, "— s_news will lack LLM tilts")
    elif skip_llm:
        print("[all] skip News judge (--skip-llm)")
    elif not config.has_llm():
        print("[all] skip News judge (no LLM configured)")
    else:
        print("[all] skip News judge (DONE for this day)")
    if need("news_actions"):
        print("[all] → News actions (ticker edges)")
        _run(
            [sys.executable, "-m", "src.news_actions", "--hours", "48", "--limit", "400", "--date", date],
            check=False,
        )
        if not _exists("01_daily", "news", f"{date}_actions.json"):
            print("[all] WARN: news actions missing for", date, "— 1d book weaker")
    else:
        print("[all] skip News actions (DONE for this day)")

    # Catalyst dossiers from OVERRIDE captains, map-heat opportunities,
    # mega-cap earnings, and action conflicts. Merges net_signal into
    # today's news actions so the .io paper book sees them.
    if skip_llm:
        print("[all] skip Catalyst daily (--skip-llm)")
    elif config.has_llm():
        print("[all] → Catalyst daily (skip-if-good + merge into actions)")
        _run([sys.executable, "-m", "src.catalyst_daily", "--date", date], check=False)
        if not _exists("01_daily", "catalyst", f"{date}_dossiers.json"):
            print("[all] WARN: catalyst dossiers missing for", date,
                  "— book ranks without catalyst merge")
    else:
        print("[all] skip Catalyst daily (no LLM configured)")

    if not skip_llm and need("general_predict"):
        if config.has_llm():
            print("[all] → General market predict")
            _run([sys.executable, "-m", "src.run_predict", "--date", date], check=False)
        else:
            print("[all] skip General market predict (no LLM configured)")
    elif skip_llm:
        print("[all] skip General market predict (--skip-llm)")
    else:
        print("[all] skip General market predict (DONE for this day)")

    if not skip_llm and (force_sectors or need("sector_predict")):
        if config.has_llm():
            print("[all] → Per-sector predict (all 11 for this trading day)")
            _run([sys.executable, "-m", "src.run_sector_predict", "--date", date], check=False)
        else:
            print("[all] skip Per-sector predict (no LLM configured)")
    elif skip_llm:
        print("[all] skip Per-sector predict (--skip-llm)")
    else:
        print("[all] skip Per-sector predict (DONE for this day — 11/11)")

    print("[all] → Weather / regime (after same-day predicts)")
    _run([sys.executable, "-m", "src.weather", "--date", date], check=False)
    if not _exists("01_daily", "weather", f"{date}_weather.json"):
        raise SystemExit(
            f"[all] FATAL: weather did not write 01_daily/weather/{date}_weather.json."
        )

    print("[all] → Join / match rank")
    _run([sys.executable, "-m", "src.join", "--date", date], check=False)
    if not _exists("data", "join", f"{date}_ranked.csv"):
        raise SystemExit(f"[all] FATAL: no join ranked file for {date}.")

    if need("peer_rs"):
        print("[all] → Peer relative strength")
        _run([sys.executable, "-m", "src.peer_rs", "--date", date], check=False)
        if not _exists("data", "peers", f"{date}_peer_rs.csv"):
            print("[all] WARN: peer_rs missing for", date)
    else:
        print("[all] skip Peer relative strength (DONE for this day)")

    if need("ticker_checklist"):
        print("[all] → Ticker checklist")
        _run([sys.executable, "-m", "src.ticker_checklist", "--date", date], check=False)
    else:
        print("[all] skip Ticker checklist (DONE for this day)")

    if need("ab"):
        if not _ab_raw(date):
            print("[all] → AB checklist (one day, liquid universe)")
            _run([sys.executable, "-m", "src.ab_checklist", "--date", date], check=False)
        else:
            print("[all] skip AB checklist raw (DONE) — will enrich")
        print("[all] → AB enrich (peers + industry + sector)")
        _run([sys.executable, "-m", "src.ab_enrich", "--date", date], check=False)
        if not _ab_enriched(date) and not _ab_raw(date):
            print("[all] WARN: AB missing — book ranks without s_ab (goldmine unused)")
        elif not _ab_enriched(date):
            print("[all] WARN: AB enrich missing — book will use raw checklist score")
    else:
        print("[all] skip AB checklist + enrich (DONE for this day)")

    print("[all] → Input health preflight")
    _run([sys.executable, "-m", "src.input_health", "--date", date], check=False)

    print("[all] → Stock book (1d / 3d / 1w / 2w / 1m)")
    _run([sys.executable, "-m", "src.stock_book", "--date", date, "--top", str(top)], check=True)

    print("[all] → Stock book backtest")
    _run(
        [sys.executable, "-m", "src.stock_book_backtest", "--top", str(top), "--max-books", "30"],
        check=False,
    )

    print("[all] → Paper trading (Futubull-fee simulation + dashboard)")
    _run(
        [sys.executable, "-m", "src.paper_trade", "--date", date, "--top", "10"],
        check=False,
    )

    print("[all] → Book learn (weight tuner from realized forward returns)")
    _run(
        [sys.executable, "-m", "src.book_learn", "--date", date, "--update-prices"],
        check=False,
    )

    print("[all] → Book reflect (gap scan + missing-input hypotheses)")
    reflect_cmd = [sys.executable, "-m", "src.book_reflect", "--date", date]
    if skip_llm:
        reflect_cmd.append("--skip-llm")
    _run(reflect_cmd, check=False)

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
