"""Pre-open time gate for predictive jobs.

US cash open is 09:30 America/New_York. Predictive writes (predict, sectors,
events, news judge/parse/actions, finviz digest) must land BEFORE that.
Hard cutoff is 09:25 ET so a late-queued job cannot overwrite a pre-open
artifact after (or into) the bell.

Outcome, reflect, scoreboard grading, weather, AB, stock-book are NOT
gated here.

Late salvage (fix #4): --bypass-cutoff / PREOPEN_BYPASS_CUTOFF=1 ignores
the 09:25 gate but still honors skip-if-good. --force ignores both.
Do not leave --force on every main push.

CLI: python -m src.preopen   # prints now + whether we are inside the window
"""
from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config

# 09:25 ET — last moment a predictive write is allowed to start/land.
PREDICT_CUTOFF_HM = 925
PREDICT_WINDOW_START_HM = 600  # 06:00 ET
# Push late-heal window in preopen_all.yml (09:25–12:00 ET).
LATE_HEAL_END_HM = 1200


def et_now() -> datetime:
    return datetime.now(ZoneInfo(config.TZ))


def et_hm() -> int:
    return int(et_now().strftime("%H%M"))


def past_predict_cutoff() -> bool:
    return et_hm() >= PREDICT_CUTOFF_HM


def in_predict_window() -> bool:
    hm = et_hm()
    return PREDICT_WINDOW_START_HM <= hm < PREDICT_CUTOFF_HM


def bypass_cutoff() -> bool:
    """True when a late heal asked to run essays after 09:25 without --force."""
    v = (os.environ.get("PREOPEN_BYPASS_CUTOFF") or "").strip().lower()
    return v in ("1", "true", "yes")


def refuse_if_late(stage: str, force: bool = False) -> None:
    """Abort a predictive write that would land after 09:25 ET.

    Called immediately BEFORE an LLM call / file write, not at process
    start — skip-if-good still works on a late orchestrator dispatch
    (good files stay, missing ones are refused rather than backfilled late).
    """
    now = et_now()
    if force:
        print(f"[preopen] {stage}: --force, ignoring 09:25 ET cutoff "
              f"(now {now.strftime('%H:%M %Z')})")
        return
    if bypass_cutoff():
        print(f"[preopen] {stage}: --bypass-cutoff, ignoring 09:25 ET "
              f"(skip-if-good still on; now {now.strftime('%H:%M %Z')})")
        return
    if past_predict_cutoff():
        raise SystemExit(
            f"[preopen] refusing {stage}: now {now.strftime('%H:%M %Z')} "
            f"is past 09:25 ET cutoff. Predictive writes after this corrupt "
            f"the session. Re-run with --bypass-cutoff (late heal) or "
            f"--force (full rewrite) only as an emergency."
        )


def main() -> None:
    now = et_now()
    print(f"now: {now.isoformat()}  hm={et_hm()}  "
          f"in_window={in_predict_window()}  "
          f"past_cutoff={past_predict_cutoff()}")


if __name__ == "__main__":
    main()
