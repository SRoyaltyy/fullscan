"""Stage OUTCOME (5:00 PM ET): fetch actual close, DeepSeek reviews the day
with cited sources, verify citations, write <date>_outcome.md, grade the
morning prediction in the scoreboard.

CLI: python -m src.run_outcome [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import (compute_scores, config, deepseek_client, fetch_channel1,
               scoreboard, verifier)


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return "(missing)"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    if not config.DEEPSEEK_API_KEY:
        raise SystemExit("DEEPSEEK_API_KEY not set")

    # 1. Actual close (deterministic)
    ch1 = fetch_channel1.build("outcome")
    fetch_channel1.save(ch1, date_str, "outcome")
    actual = ch1["actual_close"]
    spx_pct = actual.get("SPX", {}).get("pct_change")

    # 2. LLM review
    predict_md = _read(os.path.join(config.DAILY_GENERAL,
                                    f"{date_str}_predict.md"))
    with open(os.path.join(config.GROUNDING, "outcome_prompt.md"),
              encoding="utf-8") as fh:
        prompt = fh.read()
    user_msg = (f"TODAY: {date_str}\n\n"
                f"=== MORNING PREDICTION ===\n{predict_md}\n\n"
                f"{fetch_channel1.to_markdown(ch1)}\n\n"
                "Execute the post-market review now. Every factual claim MUST "
                "use the CLAIM/URL/PUBLISHED/QUOTE/SUMMARY format.")
    text = deepseek_client.chat(
        [{"role": "system", "content": prompt},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_OUTCOME, tools=True, max_tokens=8000,
        transcript_path=os.path.join("01_daily/_transcripts",
                                     f"{date_str}_outcome.json"))

    # 3. Verify citations
    claims, verify_md = verifier.verify_outcome(text)

    # 4. Write outcome file
    path = os.path.join(config.DAILY_GENERAL, f"{date_str}_outcome.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"# Post-Market Outcome — {date_str}\n\n")
        fh.write(text)
        fh.write(verify_md + "\n")

    # 5. Grade prediction in scoreboard
    if spx_pct is not None:
        board = scoreboard.load()
        entry = scoreboard.get_or_create(board, date_str, config.TOPIC)
        grade = compute_scores.grade(entry.get("predicted_direction") or "flat",
                                     entry.get("predicted_magnitude_band")
                                     or "flat", spx_pct)
        entry.update({
            "actual_open": actual["SPX"].get("open"),
            "actual_close": actual["SPX"].get("close"),
            "actual_pct_change": spx_pct,
            "actual_direction": grade["actual_direction"],
            "actual_magnitude_band": grade["actual_magnitude_band"],
            "direction_hit": grade["direction_hit"],
            "magnitude_hit": grade["magnitude_hit"],
            "per_factor_breakdown": compute_scores.per_factor_breakdown(
                entry.get("components") or {}, spx_pct),
            "sources_used": [{"url": c["url"], "date_accessed": date_str,
                              "summary": c["summary"][:200],
                              "verification": c["status"]}
                             for c in claims],
        })
        scoreboard.save(board)
        print(f"[outcome] {date_str}: SPX {spx_pct}% | "
              f"dir_hit={grade['direction_hit']} mag_hit={grade['magnitude_hit']}")
    else:
        print(f"[outcome] {date_str}: SPX actuals unavailable — "
              "scoreboard not graded")


if __name__ == "__main__":
    main()
