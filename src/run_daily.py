"""Daily spine: post-close, scrape, morning pack, diagnostics.

This is the only scheduled precursor. Generate writes the 09:30 tickets.
Do not add a fifth clock — put new morning/night work in a phase here
and register the tickets in ``src.run_generate``.

CLI:
  python3 -m src.run_daily --print-clock
  python3 -m src.run_daily --phase auto|scrape|morning|night|diagnose
"""
from __future__ import annotations

import argparse
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
PHASES = ("auto", "scrape", "morning", "night", "diagnose")
# github.event.schedule → which Daily job this cron owns
SCHEDULE_JOB = {
    "40 9 * * 1-5": "scrape",
    "10 10 * * 1-5": "generate",
    "15 13 * * 1-5": "generate",
    "20 13 * * 1-5": "generate",
    "10 20 * * 1-5": "night",
    "15 21 * * 1-5": "night",
    "30 3 * * 2-6": "night",
}


def _now(now: datetime | None = None) -> datetime:
    return now or datetime.now(ET)


def et_hm(now: datetime | None = None) -> int:
    return int(_now(now).strftime("%H%M"))


def today_et(now: datetime | None = None) -> str:
    return _now(now).date().isoformat()


def is_session(date: str) -> bool:
    d = datetime.strptime(date, "%Y-%m-%d").date()
    return skip_if_good._session_date(d)


def resolve_lane(phase: str, now: datetime | None = None) -> str:
    """morning | night — which Daily jobs should fire."""
    if phase in ("scrape", "morning", "diagnose"):
        return "morning"
    if phase == "night":
        return "night"
    return "night" if et_hm(now) >= 1600 else "morning"


def schedule_job(schedule: str, phase: str, now: datetime | None = None) -> str:
    """Which Daily job this fire should run."""
    if phase and phase != "auto":
        return phase
    mapped = SCHEDULE_JOB.get((schedule or "").strip())
    if mapped:
        return mapped
    return resolve_lane("auto", now)


def clock(phase: str = "auto", date: str | None = None,
          now: datetime | None = None, force: bool = False,
          schedule: str | None = None) -> dict:
    now = _now(now)
    date = date or today_et(now)
    lane = resolve_lane(phase, now)
    job = schedule_job(schedule if schedule is not None
                       else os.environ.get("SCHEDULE", ""), phase, now)
    session = is_session(date)
    return {
        "phase": phase if phase != "auto" else lane,
        "lane": lane,
        "job": job,
        "date": date,
        "session": session,
        "et_hm": et_hm(now),
        "force": force,
        "skip_session": (not session) and (not force),
    }


def _run(cmd: list[str], timeout_s: int | None = None,
         env: dict | None = None) -> int:
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    try:
        r = subprocess.run(
            cmd, cwd=str(ROOT), env=env or os.environ.copy(),
            timeout=timeout_s)
    except subprocess.TimeoutExpired:
        print(f"[daily] WARN: timed out after {timeout_s}s: {' '.join(cmd)}",
              flush=True)
        return 124
    return r.returncode


def scrape(date: str, force: bool = False) -> int:
    """Elite digest + overlay + universe CSV. Ubuntu only (ECS 403s)."""
    if (not force) and skip_if_good.check_finviz_scrape(date):
        print(f"[daily] scrape {date}: already good")
        return 0
    py = sys.executable
    env = os.environ.copy()
    env["FULLSCAN_DB_OPTIONAL"] = "1"
    env.pop("FINVIZ_SKIP_LIVE", None)
    code = _run([py, "-m", "src.finviz_digest", "--date", date],
                timeout_s=180, env=env)
    args = [py, "-m", "src.map_heat", "--date", date, "--overlay"]
    if force:
        args.append("--force")
    _run(args, timeout_s=180, env=env)
    _run([py, "-m", "collectors.finviz_financials"], timeout_s=180, env=env)
    ok = skip_if_good.check_finviz_scrape(date)
    print(f"[daily] scrape {date}: {'PASS' if ok else 'DEGRADED'}")
    return 0 if ok or code == 0 else code


def morning(date: str, force: bool = False, skip_llm: bool = True) -> int:
    """Weather / join / AB / book. ECS packet is ``skip_llm=False``."""
    py = sys.executable
    if skip_llm:
        args = [py, "-m", "src.run_stock_book_all", "--date", date,
                "--skip-llm", "--skip-extras", "--refresh-ranker", "--top", "25"]
        if force:
            args.append("--force")
        return _run(args, timeout_s=2400)
    args = [py, "-m", "src.run_preopen_all", "--date", date]
    if force:
        args.append("--force")
    backend = os.environ.get("LLM_BACKEND")
    if backend:
        args += ["--llm-backend", backend]
    return _run(args, timeout_s=10800)


def night(date: str | None = None, force: bool = False) -> int:
    """Grade the closed session and write next-session captains."""
    py = sys.executable
    args = [py, "-m", "src.run_postclose_all"]
    if date:
        args += ["--date", date]
    if force:
        args.append("--force")
    backend = os.environ.get("LLM_BACKEND") or "deepseek"
    args += ["--llm-backend", backend]
    return _run(args, timeout_s=21600)


def diagnose(date: str) -> int:
    py = sys.executable
    report = {
        "date": date,
        "scrape": skip_if_good.check_finviz_scrape(date),
        "preopen_full": skip_if_good.check_preopen_full(date),
        "stock_book_all": skip_if_good.check_stock_book_all(date),
        "postclose_last": skip_if_good.check_postclose_all(
            skip_if_good.last_closed_session()),
        "generate": skip_if_good.check_generate(date),
    }
    out = ROOT / "01_daily" / f"{date}_daily_status.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[daily] diagnose {date}: {report}")
    _run([py, "-m", "src.stock_book_diag", "--date", date, "--write",
          "--no-gh", "--rebuild-if-missing"], timeout_s=180)
    return 0


def run(phase: str = "auto", date: str | None = None, force: bool = False,
        skip_llm: bool | None = None, llm_backend: str | None = None,
        now: datetime | None = None) -> int:
    if llm_backend:
        os.environ["LLM_BACKEND"] = llm_backend
        config.apply_llm_backend(llm_backend)
    clk = clock(phase, date, now=now, force=force)
    date = clk["date"]
    phase = clk["phase"]
    print(f"[daily] phase={phase} lane={clk['lane']} date={date} "
          f"session={clk['session']} et_hm={clk['et_hm']} force={force}")
    if clk["skip_session"] and phase in ("scrape", "morning"):
        print(f"[daily] {date} is not an NYSE session — skip {phase}")
        return 0
    if skip_llm is None:
        skip_llm = True
    code = 0
    if phase == "scrape":
        code = scrape(date, force=force)
    elif phase == "morning":
        if not skip_if_good.check_finviz_scrape(date):
            scrape(date, force=force)
        code = morning(date, force=force, skip_llm=skip_llm)
    elif phase == "night":
        code = night(None if not date else date, force=force)
    elif phase == "diagnose":
        code = diagnose(date)
    else:
        raise SystemExit(f"unknown phase {phase}")
    if phase != "diagnose":
        diagnose(date)
    return code


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="auto", choices=PHASES)
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--skip-llm", action="store_true", default=None)
    ap.add_argument("--no-skip-llm", dest="skip_llm", action="store_false")
    ap.add_argument("--llm-backend", default=None,
                    choices=["auto", "grok", "deepseek"])
    ap.add_argument("--print-clock", action="store_true")
    args = ap.parse_args()
    clk = clock(args.phase, args.date, force=args.force)
    if args.print_clock:
        print(f"PHASE={clk['phase']}")
        print(f"LANE={clk['lane']}")
        print(f"JOB={clk['job']}")
        print(f"DATE={clk['date']}")
        print(f"SESSION={'yes' if clk['session'] else 'no'}")
        print(f"ET_HM={clk['et_hm']}")
        print(f"SKIP_SESSION={'yes' if clk['skip_session'] else 'no'}")
        return
    raise SystemExit(run(
        phase=args.phase, date=args.date, force=args.force,
        skip_llm=args.skip_llm, llm_backend=args.llm_backend,
    ))


if __name__ == "__main__":
    main()
