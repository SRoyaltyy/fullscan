"""Pre-open time gate for predictive jobs.

US cash open is 09:30 America/New_York. Predictive writes (predict, sectors,
events, news judge/parse/actions, finviz digest) must land BEFORE that.
Hard cutoff is 09:25 ET so a late-queued job cannot overwrite a pre-open
artifact after (or into) the bell.

Outcome, reflect, scoreboard grading, weather, AB, stock-book are NOT
gated here.

CLI: python -m src.preopen   # prints now + whether we are inside the window
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from . import config

# 09:25 ET — last moment a predictive write is allowed to start/land.
PREDICT_CUTOFF_HM = 925
PREDICT_WINDOW_START_HM = 600  # 06:00 ET


def et_now() -> datetime:
    return datetime.now(ZoneInfo(config.TZ))


def et_hm() -> int:
    return int(et_now().strftime("%H%M"))


def past_predict_cutoff() -> bool:
    return et_hm() >= PREDICT_CUTOFF_HM


def in_predict_window() -> bool:
    hm = et_hm()
    return PREDICT_WINDOW_START_HM <= hm < PREDICT_CUTOFF_HM


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
    if past_predict_cutoff():
        raise SystemExit(
            f"[preopen] refusing {stage}: now {now.strftime('%H:%M %Z')} "
            f"is past 09:25 ET cutoff. Predictive writes after this corrupt "
            f"the session. Re-run with --force only as an emergency."
        )


def main() -> None:
    now = et_now()
    print(f"now: {now.isoformat()}  hm={et_hm()}  "
          f"in_window={in_predict_window()}  "
          f"past_cutoff={past_predict_cutoff()}")


if __name__ == "__main__":
    main()
