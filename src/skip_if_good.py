"""Cheap skip-if-good for Actions. File + JSON checks only. No LLM.

Exit 0  = today's expected outputs are already on disk → workflow must stop.
Exit 1  = missing or thin → workflow may run its real job.

Used as:
  python -m src.skip_if_good --job preopen_all [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from . import output_qc

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def last_closed_session(now: datetime | None = None) -> str:
    """Most recent session that has already printed a close.

    Before 16:00 ET a weekday is still live, so yesterday (rolling
    back across the weekend) is the closed source date. Post-close
    research keys off this, not calendar 'today'.
    """
    now = now or datetime.now(ET)
    d = now.date()
    if now.hour < 16:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def _next_weekday(date_s: str) -> str:
    d = datetime.strptime(date_s, "%Y-%m-%d").date()
    d += timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.isoformat()


def _exists_gt(path: Path, min_bytes: int) -> bool:
    try:
        return path.is_file() and path.stat().st_size >= min_bytes
    except OSError:
        return False


def _log(ok: bool, job: str, date: str, detail: str) -> bool:
    flag = "SKIP" if ok else "RUN"
    print(f"[skip_if_good] {flag} job={job} date={date} {detail}", flush=True)
    return ok


def check_finviz_scrape(date: str) -> bool:
    digest = ROOT / "01_daily" / "news" / f"{date}_finviz_digest.json"
    heat = ROOT / "01_daily" / "map_heat" / f"{date}_map_heat.json"
    if not output_qc.qc_finviz_digest(digest).ok:
        return _log(False, "finviz_preopen_scrape", date, "digest missing/thin")
    if not output_qc.qc_map_heat(heat).ok:
        return _log(False, "finviz_preopen_scrape", date, "map_heat missing/thin")
    try:
        payload = json.loads(heat.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _log(False, "finviz_preopen_scrape", date, "map_heat unreadable")
    overlay = str(payload.get("overlay_at") or "")
    tape = payload.get("tape") or []
    ok = overlay.startswith(date) and bool(tape)
    return _log(ok, "finviz_preopen_scrape", date,
                f"overlay_at={overlay!r} tape={len(tape) if isinstance(tape, list) else 0}")


def check_preopen_all(date: str) -> bool:
    """Morning packet only. Night captain research is NOT required."""
    pred = ROOT / "01_daily" / "general" / f"{date}_predict.md"
    if not output_qc.qc_general_predict(pred).ok:
        return _log(False, "preopen_all", date, "general predict missing/thin")
    if not output_qc.qc_events_date(date).ok:
        return _log(False, "preopen_all", date, "events missing/carried")
    judge = ROOT / "01_daily" / "news" / f"{date}_judge.md"
    if not output_qc.qc_news_judge(judge).ok:
        return _log(False, "preopen_all", date, "judge missing/thin")
    parsed = ROOT / "01_daily" / "news" / f"{date}_parsed.json"
    if not output_qc.qc_news_parse(parsed).ok:
        return _log(False, "preopen_all", date, "parse missing")
    sector_dir = ROOT / "01_daily" / "sectors" / date
    n_ok = 0
    if sector_dir.is_dir():
        for p in sector_dir.glob("*_predict.md"):
            if output_qc.qc_sector_predict(p).ok:
                n_ok += 1
    ok = n_ok >= 8
    return _log(ok, "preopen_all", date, f"sector_predict_ok={n_ok}/11")


def check_map_heat_postclose(date: str) -> bool:
    """date = completed (source) session. Artifact lives on the NEXT session."""
    target = _next_weekday(date)
    heat = ROOT / "01_daily" / "map_heat"
    baseline = heat / f"{target}_research_baseline.json"
    md_base = heat / f"{target}_research_baseline.md"
    md_short = heat / f"{target}_research.md"
    if not output_qc.qc_map_heat_baseline(baseline).ok:
        return _log(False, "map_heat_postclose", date,
                    f"target={target} baseline missing/thin")
    if not (_exists_gt(md_base, 400) or _exists_gt(md_short, 400)):
        return _log(False, "map_heat_postclose", date,
                    f"target={target} baseline.md missing")
    return _log(True, "map_heat_postclose", date,
                f"target={target} baseline present")


def check_label_weather(date: str) -> bool:
    """Weather JSON with enough sector stances to rank."""
    p = ROOT / "01_daily" / "weather" / f"{date}_weather.json"
    if not _exists_gt(p, 200):
        return _log(False, "label_weather", date, "weather json missing/thin")
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _log(False, "label_weather", date, "weather unreadable")
    secs = (payload.get("signals") or {}).get("sectors") or {}
    n = len(secs) if isinstance(secs, dict) else 0
    ok = n >= 5
    return _log(ok, "label_weather", date, f"weather_sectors={n}")


def check_stock_book_all(date: str) -> bool:
    """Book + green pile + the ranker inputs BUY/SELL need."""
    js = ROOT / "data" / "stock_book" / f"{date}_stock_book.json"
    md = ROOT / "01_daily" / f"{date}_stock_book.md"
    if not (_exists_gt(js, 200) or _exists_gt(md, 400)):
        return _log(False, "stock_book_all", date, "book json/md missing")
    green = ROOT / "data" / "stock_book" / f"{date}_green.json"
    if not _exists_gt(green, 40):
        return _log(False, "stock_book_all", date, "green.json missing — pile not graded")
    if not check_label_weather(date):
        return _log(False, "stock_book_all", date, "weather incomplete")
    if not check_ab_checklist(date):
        return _log(False, "stock_book_all", date, "AB missing")
    ranked = ROOT / "data" / "join" / f"{date}_ranked.csv"
    if not _exists_gt(ranked, 200):
        return _log(False, "stock_book_all", date, "join ranked missing")
    return _log(True, "stock_book_all", date,
                "book + green + weather + join + AB")


def check_ab_checklist(date: str) -> bool:
    enriched = ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist_enriched.csv"
    raw = ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist.csv"
    ok = _exists_gt(enriched, 200) or _exists_gt(raw, 10_000)
    return _log(ok, "ab_checklist", date, f"enriched={enriched.exists()} raw={raw.exists()}")


def check_daily_pipeline_outcome(date: str) -> bool:
    outcome = ROOT / "01_daily" / "general" / f"{date}_outcome.md"
    ok = _exists_gt(outcome, 400)
    return _log(ok, "daily_pipeline", date, f"outcome={outcome.exists()} size={outcome.stat().st_size if outcome.exists() else 0}")


def check_learn_cycle(date: str) -> bool:
    """Dated session file only. Stale LEARNINGS.md must not skip today."""
    daily = ROOT / "01_daily" / f"{date}_learnings.md"
    ok = _exists_gt(daily, 200)
    return _log(ok, "learn_cycle", date, f"daily_md={daily.exists()}")


def check_preopen_full(date: str) -> bool:
    """Morning packet AND today's stock book — one-click pre-open done."""
    if not check_preopen_all(date):
        return _log(False, "preopen_full", date, "packet incomplete")
    if not check_stock_book_all(date):
        return _log(False, "preopen_full", date, "stock book / ranker inputs missing")
    return _log(True, "preopen_full", date, "packet + book + green + ranker inputs")


def check_sector_outcomes(date: str) -> bool:
    d = ROOT / "01_daily" / "sectors" / date
    n = len(list(d.glob("*_outcome.md"))) if d.is_dir() else 0
    ok = n >= 8
    return _log(ok, "sector_outcomes", date, f"outcome_md={n}/11")


def check_postclose_all(date: str) -> bool:
    """Closed session graded + next-session captain baseline + learnings."""
    if not check_daily_pipeline_outcome(date):
        return _log(False, "postclose_all", date, "outcome missing")
    if not check_map_heat_postclose(date):
        return _log(False, "postclose_all", date, "next-session baseline missing")
    if not check_learn_cycle(date):
        return _log(False, "postclose_all", date, "learnings missing")
    return _log(True, "postclose_all", date, "outcome + baseline + learnings")


JOBS = {
    "finviz_preopen_scrape": check_finviz_scrape,
    "preopen_all": check_preopen_all,
    "preopen_full": check_preopen_full,
    "map_heat_postclose": check_map_heat_postclose,
    "stock_book_all": check_stock_book_all,
    "label_weather": check_label_weather,
    "ab_checklist": check_ab_checklist,
    "daily_pipeline": check_daily_pipeline_outcome,
    "learn_cycle": check_learn_cycle,
    "sector_outcomes": check_sector_outcomes,
    "postclose_all": check_postclose_all,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, choices=sorted(JOBS))
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    if args.date:
        date = args.date
    elif args.job in ("map_heat_postclose", "postclose_all"):
        date = last_closed_session()
    else:
        date = _today()
    os.chdir(ROOT)
    ok = JOBS[args.job](date)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
