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


def check_stock_book_all(date: str) -> bool:
    js = ROOT / "data" / "stock_book" / f"{date}_stock_book.json"
    md = ROOT / "01_daily" / f"{date}_stock_book.md"
    ok = _exists_gt(js, 200) or _exists_gt(md, 400)
    return _log(ok, "stock_book_all", date,
                f"json={js.exists()} md={md.exists()}")


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
    daily = ROOT / "01_daily" / f"{date}_learnings.md"
    board = ROOT / "03_scoreboard" / "LEARNINGS.md"
    ok = _exists_gt(daily, 200) or _exists_gt(board, 400)
    return _log(ok, "learn_cycle", date, f"daily_md={daily.exists()}")


JOBS = {
    "finviz_preopen_scrape": check_finviz_scrape,
    "preopen_all": check_preopen_all,
    "map_heat_postclose": check_map_heat_postclose,
    "stock_book_all": check_stock_book_all,
    "ab_checklist": check_ab_checklist,
    "daily_pipeline": check_daily_pipeline_outcome,
    "learn_cycle": check_learn_cycle,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, choices=sorted(JOBS))
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    if args.date:
        date = args.date
    elif args.job == "map_heat_postclose":
        date = last_closed_session()
    else:
        date = _today()
    os.chdir(ROOT)
    ok = JOBS[args.job](date)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
