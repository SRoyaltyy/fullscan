"""One-button POST-CLOSE ALL: grade today, learn, write tomorrow's captains.

Skip-if-good on every step. Safe to fire from the 16:10 ET GitHub cron
AND the 22:00 ET ECS timer — the second run spends no LLM if files exist.

  → general outcome + horizon grade + reflect
  → sector outcomes + sector reflect + sector board
  → news-actions grader (no LLM)
  → HIT board (no LLM)
  → learn cycle
  → map-heat captain research for the NEXT session

Does NOT scrape Finviz (ECS 403). Clones today's heat onto the next
session date, then researches. Does NOT rewrite the morning packet
or the stock book.

CLI:
  python -m src.run_postclose_all [--date YYYY-MM-DD] [--force]
                                  [--llm-backend auto]
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, skip_if_good
from .map_heat_postclose import next_weekday
from .skip_if_good import night_pack_dates

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)


def _run(cmd: list[str], timeout_s: int | None = None) -> int:
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    try:
        r = subprocess.run(
            cmd, cwd=str(ROOT), env=os.environ.copy(), timeout=timeout_s)
    except subprocess.TimeoutExpired:
        print(f"[postclose-all] WARN: timed out after {timeout_s}s: "
              f"{' '.join(cmd)}", flush=True)
        return 124
    return r.returncode


def _llm_http_timeout(default: int = 900) -> int:
    raw = os.environ.get("POSTCLOSE_LLM_TIMEOUT", str(default))
    try:
        return max(60, int(raw))
    except ValueError:
        return default


def _push_pack(date: str) -> None:
    """Land this session before the next date (or a job-timeout kill)."""
    script = ROOT / "scripts" / "safe_git_push.sh"
    if not script.is_file():
        print("[postclose-all] WARN: safe_git_push.sh missing — skip persist")
        return
    msg = f"auto: post-close ALL [{date}]"
    paths = [
        "01_daily/general/", "01_daily/sectors/", "01_daily/map_heat/",
        "01_daily/news/", "01_daily/_transcripts/", "01_daily/_channel1/",
        f"01_daily/{date}_learnings.md",
        "02_lessons/", "03_scoreboard/",
        "00_grounding/mutable_policy.md",
        "00_grounding/book_policy.json",
        "00_grounding/weather_rules_proposals.json",
    ]
    print(f"[postclose-all] persist {date} before the next session / timeout")
    code = _run(["bash", str(script), msg, *paths], timeout_s=180)
    if code != 0:
        print(f"[postclose-all] WARN: persist {date} exited {code}")


def _exists_gt(rel: str, n: int) -> bool:
    p = ROOT / rel
    try:
        return p.is_file() and p.stat().st_size >= n
    except OSError:
        return False


def run(date: str | None = None, force: bool = False,
        llm_backend: str | None = None) -> None:
    config.apply_llm_backend(llm_backend)
    if date:
        dates = [date]
    else:
        dates = night_pack_dates()
    for one in dates:
        _run_one(one, force=force)


def _run_one(date: str, force: bool = False) -> None:
    target = next_weekday(date)
    py = sys.executable
    print("")
    print("=" * 72)
    print(f"  POST-CLOSE ALL — closed={date}  next={target}")
    print("  Grade → learn → tomorrow's captain notebook.")
    print("  Each step skips when its file is already quality-ok.")
    print("=" * 72)

    if (not force) and skip_if_good.check_postclose_all(date):
        print(f"[postclose-all] {date}: outcome + next baseline + learnings "
              f"already on disk — nothing to do")
        return

    def step(title: str, cmd: list[str], done: bool,
             timeout_s: int | None = None,
             llm_timeout_s: int | None = None) -> int:
        if done and not force:
            print(f"[postclose-all] skip {title} (already on disk)")
            return 0
        print(f"\n[postclose-all] → {title}")
        prev = None
        if llm_timeout_s is not None:
            prev = os.environ.get("OPENCLAW_TIMEOUT")
            os.environ["OPENCLAW_TIMEOUT"] = str(llm_timeout_s)
            print(f"[postclose-all] LLM HTTP timeout {llm_timeout_s}s "
                  "(hung Grok fails over; 10800s ate the morning packet)")
            if timeout_s is None:
                timeout_s = llm_timeout_s + 60
        try:
            code = _run(cmd, timeout_s=timeout_s)
        finally:
            if llm_timeout_s is not None:
                if prev is None:
                    os.environ.pop("OPENCLAW_TIMEOUT", None)
                else:
                    os.environ["OPENCLAW_TIMEOUT"] = prev
        if code != 0:
            print(f"[postclose-all] WARN: {title} exited {code}")
        return code

    llm_to = _llm_http_timeout()
    try:
        captain_to = max(60, int(os.environ.get("POSTCLOSE_CAPTAIN_TIMEOUT", "1200")))
    except ValueError:
        captain_to = 1200

    step("General outcome",
         [py, "-m", "src.run_outcome", "--date", date],
         skip_if_good.check_daily_pipeline_outcome(date),
         llm_timeout_s=llm_to)
    # Land outcome before reflect/sectors so a hang still heals the gate.
    _push_pack(date)
    # Cheap / no-LLM — always refresh so a yesterday file cannot skip today.
    step("Horizon grade",
         [py, "-m", "src.horizon_grade", "--date", date],
         False, timeout_s=60)
    step("General reflect",
         [py, "-m", "src.run_reflect", "--date", date],
         _exists_gt(f"01_daily/general/{date}_reflect.md", 200),
         llm_timeout_s=llm_to)
    # 09-03 already had a 6k-char transcript but never wrote *_reflect.md.
    # Land it before the 11-sector loop so a kill still heals the gate.
    _push_pack(date)

    # 11 sectors × (HTTP cap + slack). A 5400s wall died mid-pack when
    # several Grok calls each burned the 900s hang budget.
    sector_wall = max(5400, 11 * (llm_to + 90))
    step("Sector outcomes",
         [py, "-m", "src.run_sector_outcome", "--date", date],
         skip_if_good.check_sector_outcomes(date),
         timeout_s=sector_wall, llm_timeout_s=llm_to)
    # 11 Grok calls. Land whatever graded before reflect/learn or a kill.
    _push_pack(date)
    n_reflect = 0
    sec = ROOT / "01_daily" / "sectors" / date
    if sec.is_dir():
        n_reflect = len(list(sec.glob("*_reflect.md")))
    step("Sector reflect",
         [py, "-m", "src.run_sector_reflect", "--date", date],
         n_reflect >= 8,
         timeout_s=sector_wall, llm_timeout_s=llm_to)
    _push_pack(date)
    step("Sector board",
         [py, "-m", "src.sector_board", "--date", date],
         False, timeout_s=60)

    # yfinance can hang the night pack before learn/captains. Bound it.
    step("News actions grader",
         [py, "-m", "src.news_grade"],
         False, timeout_s=300)
    step("HIT board",
         [py, "-m", "src.hit_board"],
         False, timeout_s=60)

    # Always rewrite. A thin dated file written before sector grades must
    # not skip the digest that includes tonight's 11 HIT% rows.
    step("Learn cycle",
         [py, "-m", "src.learn_cycle", "--date", date],
         False, timeout_s=180)
    # Captains can burn 2h. Dated learnings must already be on main.
    _push_pack(date)

    heat_src = ROOT / "01_daily" / "map_heat" / f"{date}_map_heat.json"
    heat_dst = ROOT / "01_daily" / "map_heat" / f"{target}_map_heat.json"
    if heat_src.is_file() and not heat_dst.is_file():
        print(f"[postclose-all] clone map_heat {date} → {target}")
        try:
            heat_dst.write_bytes(heat_src.read_bytes())
            md_src = heat_src.with_suffix(".md")
            md_dst = heat_dst.with_suffix(".md")
            if md_src.is_file():
                md_dst.write_bytes(md_src.read_bytes())
        except OSError as e:
            print(f"[postclose-all] clone failed: {e}")

    os.environ.setdefault("FINVIZ_SKIP_LIVE", "1")
    step("Captain research → next session",
         [py, "-m", "src.map_heat_postclose",
          "--source-date", date, "--target-date", target],
         skip_if_good.check_map_heat_postclose(date),
         timeout_s=7200, llm_timeout_s=captain_to)

    if not skip_if_good.check_postclose_all(date):
        print(f"[postclose-all] DEGRADED {date}: outcome/reflect/sectors/"
              "learn/next-session still missing — wrote whatever landed; "
              "persist then continue")
        _push_pack(date)
        return
    print(f"[postclose-all] PASS {date} — grades/learn/next-session research done")
    _push_pack(date)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None,
                    help="Closed session YYYY-MM-DD (default last closed ET)")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--llm-backend", default=None,
                    choices=["auto", "grok", "deepseek"])
    args = ap.parse_args()
    run(date=args.date, force=args.force, llm_backend=args.llm_backend)


if __name__ == "__main__":
    main()
