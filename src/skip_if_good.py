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
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from . import output_qc

# Keep these here so Actions can skip-if-good before pip install pandas
# (green_pile imports pandas). Must match src/green_pile.py.
EPS = 0.05
RELVOL_DEAD = 0.7

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent

# DeepSeek sometimes writes the DSML tool-call XML into `content`
# instead of the tool_calls field. Those files are 600–900 B so a
# byte-size gate treats them as a finished essay and skip-if-good
# never rewrites them (live 2026-09-03 XLB/XLE/XLV).
_TOOL_DUMP_MARKERS = (
    "tool_calls>",
    'invoke name="web_search"',
    "DSML",
)


def is_tool_dump(text: str) -> bool:
    """True when `text` is a leaked tool-call blob, not an analysis."""
    if not text:
        return False
    return any(m in text for m in _TOOL_DUMP_MARKERS)


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """n>0 = nth weekday of month (Mon=0). n=-1 = last weekday of month."""
    if n > 0:
        d = date(year, month, 1)
        d += timedelta(days=(weekday - d.weekday()) % 7)
        return d + timedelta(weeks=n - 1)
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    d -= timedelta(days=(d.weekday() - weekday) % 7)
    return d


def _easter_gregorian(year: int) -> date:
    """Anonymous Gregorian Easter (Western)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    el = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * el) // 451
    month, day = divmod(h + el - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _observed_nyse(d: date) -> date:
    """Saturday holiday → Friday; Sunday holiday → Monday."""
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def is_nyse_holiday(d: date) -> bool:
    """Full-day NYSE closures. No pandas — skip-if-good runs before pip."""
    y = d.year
    holidays = {
        _observed_nyse(date(y, 1, 1)),
        _nth_weekday(y, 1, 0, 3),
        _nth_weekday(y, 2, 0, 3),
        _easter_gregorian(y) - timedelta(days=2),
        _nth_weekday(y, 5, 0, -1),
        _observed_nyse(date(y, 6, 19)),
        _observed_nyse(date(y, 7, 4)),
        _nth_weekday(y, 9, 0, 1),
        _nth_weekday(y, 11, 3, 4),
        _observed_nyse(date(y, 12, 25)),
    }
    return d in holidays


def _session_date(d: date) -> bool:
    return d.weekday() < 5 and not is_nyse_holiday(d)


def last_closed_session(now: datetime | None = None) -> str:
    """Most recent session that has already printed a close.

    Before 16:00 ET a weekday is still live, so yesterday (rolling
    back across the weekend and NYSE holidays) is the closed source
    date. Post-close research keys off this, not calendar 'today'.
    Labor Day 2026-09-07 is not a session — Tuesday 09-08 morning
    must still close Friday 09-04.
    """
    now = now or datetime.now(ET)
    d = now.date()
    if now.hour < 16:
        d -= timedelta(days=1)
    while not _session_date(d):
        d -= timedelta(days=1)
    return d.isoformat()


def _next_weekday(date_s: str) -> str:
    """Next NYSE session (weekends + full-day holidays).

    pandas_market_calendars on the runner maps Fri 2026-09-04 → Tue
    2026-09-08 (Labor Day). A weekend-only walk looked for Monday
    09-07 and left check_postclose_all red after captains wrote 09-08.
    """
    d = datetime.strptime(date_s, "%Y-%m-%d").date()
    d += timedelta(days=1)
    while not _session_date(d):
        d += timedelta(days=1)
    return d.isoformat()


def _prev_weekday(date_s: str) -> str:
    d = datetime.strptime(date_s, "%Y-%m-%d").date()
    d -= timedelta(days=1)
    while not _session_date(d):
        d -= timedelta(days=1)
    return d.isoformat()


def night_pack_dates(now: datetime | None = None) -> list[str]:
    """Last-closed, plus the prior weekday when last-closed is still missing.

    After 16:00 ET last_closed flips to today; if yesterday's pack never
    landed, prepend it. Do not walk further back (live 09-03 complete
    must not spend a pre-bell fire on 09-02's missing sectors).
    """
    closed = last_closed_session(now)
    dates = [closed]
    if not check_postclose_all(closed):
        prior = _prev_weekday(closed)
        if not check_postclose_all(prior):
            dates.insert(0, prior)
    return dates


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
    if not (overlay.startswith(date) and bool(tape)):
        return _log(False, "finviz_preopen_scrape", date,
                    f"overlay_at={overlay!r} tape={len(tape) if isinstance(tape, list) else 0}")
    export = ROOT / "data" / "exports" / f"finviz_{date}.csv"
    # Full universe is megabytes. A header-only stub must not skip the book.
    if not _exists_gt(export, 50_000):
        return _log(False, "finviz_preopen_scrape", date, "elite export missing/thin")
    return _log(True, "finviz_preopen_scrape", date,
                f"overlay_at={overlay!r} tape={len(tape) if isinstance(tape, list) else 0} export_ok")


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


def book_files_are_degraded(js: Path, green: Path) -> bool:
    """True when the crash-fallback book landed instead of a real rank."""
    try:
        if js.is_file():
            payload = json.loads(js.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return True
            if (payload.get("meta") or {}).get("degraded"):
                return True
            books = payload.get("books")
            # A real rank writes 1d/3d/1w/2w/1m. Empty books = crash stub.
            if isinstance(books, dict) and not books:
                return True
    except (OSError, json.JSONDecodeError, TypeError):
        return True
    try:
        if green.is_file():
            g = json.loads(green.read_text(encoding="utf-8"))
            if isinstance(g, dict) and g.get("degraded"):
                return True
    except (OSError, json.JSONDecodeError, TypeError):
        return True
    return False


def book_missing_same_day_essays(js: Path) -> bool:
    """True when the book ranked without today's general + most sector essays.

    An early 06:10 ubuntu book (s_general=0, pile unused) must not skip the
    09:15 heal that applies the morning packet.
    """
    try:
        payload = json.loads(js.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return True
    meta = payload.get("meta") if isinstance(payload, dict) else None
    if not isinstance(meta, dict):
        return True
    if not meta.get("same_day_general"):
        return True
    try:
        n_sec = int(meta.get("same_day_sectors") or 0)
    except (TypeError, ValueError):
        n_sec = 0
    return n_sec < 8


def book_1d_has_dead_relvol(js: Path) -> bool:
    """True when 1d BUY lists a name Finviz printed in (0, 0.7) relvol."""
    try:
        payload = json.loads(js.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return False
    buys = ((payload.get("books") or {}).get("1d") or {}).get("buy") or []
    if not isinstance(buys, list):
        return False
    dead = []
    for row in buys:
        if not isinstance(row, dict):
            continue
        try:
            rv = row.get("relvol")
            if rv is None:
                continue
            rv = float(rv)
        except (TypeError, ValueError):
            continue
        if 0 < rv < RELVOL_DEAD:
            dead.append(str(row.get("ticker") or "?"))
    if dead:
        print(f"[skip_if_good] 1d BUY dead relvol: {', '.join(dead[:8])}",
              flush=True)
        return True
    return False


def book_1d_breaks_all_green(js: Path) -> bool:
    """True when 1d BUY lists a name that fails the all-green contract.

    Live 2026-09-04 kept HTFL (s_peer=0) and CNH (s_sector=−0.45) on 1d
    after the pile of 117 liquid greens had already landed.
    """
    try:
        payload = json.loads(js.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return False
    buys = ((payload.get("books") or {}).get("1d") or {}).get("buy") or []
    if not isinstance(buys, list):
        return False
    bad = []
    eps = EPS
    for row in buys:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "?")
        if row.get("green") is False:
            bad.append(ticker)
            continue
        failed = False
        for col in ("s_join", "s_general", "s_ab", "s_peer"):
            if col not in row or row.get(col) is None:
                continue
            try:
                if float(row[col]) < eps:
                    failed = True
                    break
            except (TypeError, ValueError):
                continue
        if not failed:
            for col in ("s_sector", "s_news"):
                if row.get(col) is None:
                    continue
                try:
                    if float(row[col]) <= -eps:
                        failed = True
                        break
                except (TypeError, ValueError):
                    continue
        if not failed:
            try:
                rv = row.get("relvol")
                if rv is not None:
                    rv = float(rv)
                    if 0 < rv < RELVOL_DEAD:
                        failed = True
            except (TypeError, ValueError):
                pass
        if failed:
            bad.append(ticker)
    if bad:
        print(f"[skip_if_good] 1d BUY not all-green: {', '.join(bad[:8])}",
              flush=True)
        return True
    return False


def check_stock_book_all(date: str) -> bool:
    """Book + green pile + the ranker inputs BUY/SELL need."""
    js = ROOT / "data" / "stock_book" / f"{date}_stock_book.json"
    md = ROOT / "01_daily" / f"{date}_stock_book.md"
    if not (_exists_gt(js, 200) or _exists_gt(md, 400)):
        return _log(False, "stock_book_all", date, "book json/md missing")
    green = ROOT / "data" / "stock_book" / f"{date}_green.json"
    if not _exists_gt(green, 40):
        return _log(False, "stock_book_all", date, "green.json missing — pile not graded")
    if book_files_are_degraded(js, green):
        return _log(False, "stock_book_all", date, "degraded book — re-rank required")
    if js.is_file() and book_missing_same_day_essays(js):
        return _log(False, "stock_book_all", date,
                    "book ranked without same-day essays — re-rank required")
    if js.is_file() and book_1d_has_dead_relvol(js):
        return _log(False, "stock_book_all", date,
                    "1d BUY has printed dead relvol — re-rank required")
    if js.is_file() and book_1d_breaks_all_green(js):
        return _log(False, "stock_book_all", date,
                    "1d BUY is not all-green — re-rank required")
    if not check_label_weather(date):
        return _log(False, "stock_book_all", date, "weather incomplete")
    if not check_ab_checklist(date):
        return _log(False, "stock_book_all", date, "AB missing")
    ranked = ROOT / "data" / "join" / f"{date}_ranked.csv"
    # Full universe is megabytes. A header-only stub must not skip rebuild.
    if not _exists_gt(ranked, 5_000):
        return _log(False, "stock_book_all", date, "join ranked missing")
    return _log(True, "stock_book_all", date,
                "book + green + weather + join + AB")


def check_ab_checklist(date: str) -> bool:
    enriched = ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist_enriched.csv"
    raw = ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist.csv"
    ok = _exists_gt(enriched, 5_000) or _exists_gt(raw, 10_000)
    return _log(ok, "ab_checklist", date, f"enriched={enriched.exists()} raw={raw.exists()}")


def check_daily_pipeline_outcome(date: str) -> bool:
    outcome = ROOT / "01_daily" / "general" / f"{date}_outcome.md"
    ok = _exists_gt(outcome, 400)
    if ok:
        try:
            ok = not is_tool_dump(outcome.read_text(encoding="utf-8"))
        except OSError:
            ok = False
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


def check_general_reflect(date: str) -> bool:
    p = ROOT / "01_daily" / "general" / f"{date}_reflect.md"
    ok = _exists_gt(p, 200)
    if ok:
        try:
            ok = not is_tool_dump(p.read_text(encoding="utf-8"))
        except OSError:
            ok = False
    return _log(ok, "general_reflect", date, f"reflect={p.exists()}")


def _sector_md_is_essay(path: Path, min_bytes: int = 200) -> bool:
    if not _exists_gt(path, min_bytes):
        return False
    try:
        return not is_tool_dump(path.read_text(encoding="utf-8"))
    except OSError:
        return False


def _count_sector_md(date: str, suffix: str, min_bytes: int = 200) -> int:
    d = ROOT / "01_daily" / "sectors" / date
    if not d.is_dir():
        return 0
    n = 0
    for p in d.glob(f"*{suffix}"):
        if _sector_md_is_essay(p, min_bytes):
            n += 1
    return n


def _count_sector_dumps(date: str, suffix: str) -> int:
    d = ROOT / "01_daily" / "sectors" / date
    if not d.is_dir():
        return 0
    n = 0
    for p in d.glob(f"*{suffix}"):
        try:
            if is_tool_dump(p.read_text(encoding="utf-8")):
                n += 1
        except OSError:
            continue
    return n


def check_sector_outcomes(date: str) -> bool:
    n = _count_sector_md(date, "_outcome.md")
    stubs = _count_sector_dumps(date, "_outcome.md")
    # ≥8 essays is not enough when leaked tool-call XML is sitting in
    # the other slots — skip would freeze those stubs forever.
    ok = n >= 8 and stubs == 0
    return _log(ok, "sector_outcomes", date,
                f"outcome_md={n}/11 dumps={stubs}")


def check_sector_reflects(date: str) -> bool:
    """Sector reflect is part of the intended pack, not an optional extra.

    Outcomes-only would let skip-if-good SKIP after 8 stubs and never write
    the 11 diagnostics the night pack is supposed to land.
    """
    n = _count_sector_md(date, "_reflect.md")
    stubs = _count_sector_dumps(date, "_reflect.md")
    ok = n >= 8 and stubs == 0
    return _log(ok, "sector_reflects", date,
                f"reflect_md={n}/11 dumps={stubs}")


def check_postclose_all(date: str) -> bool:
    """Closed session fully graded — not just a dated learnings file.

    2026-09-03 has a general outcome and next-session baseline, but zero
    sector outcomes and no reflect. Learn writing _learnings.md from
    lookback must not skip those layers on the 22:00 retry.
    """
    if not check_daily_pipeline_outcome(date):
        return _log(False, "postclose_all", date, "outcome missing")
    if not check_general_reflect(date):
        return _log(False, "postclose_all", date, "reflect missing")
    if not check_sector_outcomes(date):
        return _log(False, "postclose_all", date, "sector outcomes missing")
    if not check_sector_reflects(date):
        return _log(False, "postclose_all", date, "sector reflects missing")
    if not check_map_heat_postclose(date):
        return _log(False, "postclose_all", date, "next-session baseline missing")
    if not check_learn_cycle(date):
        return _log(False, "postclose_all", date, "learnings missing")
    return _log(True, "postclose_all", date,
                "outcome + reflect + sectors + sector-reflects + baseline + learnings")


# postclose_all.yml skip-if-good must yield when the last-closed sidecar
# is already writing. Do not put this in check_postclose_all() — that
# function is the pack-complete predicate (night_pack_dates, orch).
# Only the Post-Close ALL workflow itself should skip; the sidecar
# uses the same --job and must not skip itself.
POSTCLOSE_ALL_WORKFLOW_NAME = "Post-Close ALL (grade + learn + next captains)"
_SIDECAR_ACTIVE = ("in_progress", "queued", "waiting", "pending")


def last_closed_sidecar_running() -> bool:
    """True when postclose_last_closed.yml is already writing or queued."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY") or ""
    if not token or "/" not in repo:
        return False
    url = (
        f"https://api.github.com/repos/{repo}/actions/workflows/"
        "postclose_last_closed.yml/runs?per_page=10"
    )
    import urllib.error
    import urllib.request
    try:
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "User-Agent": "fullscan-skip-if-good",
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (OSError, ValueError, urllib.error.URLError):
        return False
    for run in payload.get("workflow_runs") or []:
        if run.get("status") in _SIDECAR_ACTIVE:
            return True
    return False


def postclose_all_should_yield_to_sidecar() -> bool:
    """Schedule/push Post-Close ALL must not race a live sidecar persist."""
    if os.environ.get("GITHUB_WORKFLOW") != POSTCLOSE_ALL_WORKFLOW_NAME:
        return False
    return last_closed_sidecar_running()


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
    "sector_reflects": check_sector_reflects,
    "postclose_all": check_postclose_all,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, choices=sorted(JOBS))
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    os.chdir(ROOT)
    if args.job == "postclose_all" and postclose_all_should_yield_to_sidecar():
        print("[skip_if_good] SKIP job=postclose_all "
              "sidecar already writing last-closed")
        raise SystemExit(0)
    if args.date:
        ok = JOBS[args.job](args.date)
    elif args.job == "postclose_all":
        # Last-closed plus a missing prior weekday (2026-09-03 learnings).
        ok = True
        for d in night_pack_dates():
            if not check_postclose_all(d):
                ok = False
                break
    elif args.job == "map_heat_postclose":
        ok = JOBS[args.job](last_closed_session())
    else:
        ok = JOBS[args.job](_today())
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
