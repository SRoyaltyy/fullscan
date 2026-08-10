"""Stage OUTCOME: grade morning prediction. Missing predict = ops_fail, not a miss.

CLI: python -m src.run_outcome [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from . import (compute_scores, config, deepseek_client, fetch_channel1,
               scoreboard, snapshot, verifier)


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return "(missing)"


def _actual_band(spx_pct: float) -> str:
    a = abs(spx_pct)
    if a >= 2.0:
        return "severe"
    if a >= 1.0:
        return "notable"
    if a >= 0.3:
        return "mild"
    return "flat"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    if not config.DEEPSEEK_API_KEY:
        raise SystemExit("DEEPSEEK_API_KEY not set")

    ch1 = fetch_channel1.build("outcome", date_str)
    fetch_channel1.save(ch1, date_str, "outcome")
    actual = ch1["actual_close"]
    spx_pct = actual.get("SPX", {}).get("pct_change")

    morning_md = "(archived morning Channel 1 not found)"
    try:
        import json
        with open(os.path.join(config.CHANNEL1_DIR, f"{date_str}_predict.json"),
                  encoding="utf-8") as fh:
            morning_md = fetch_channel1.to_markdown(json.load(fh))
    except (OSError, ValueError) as e:
        print(f"[outcome] morning channel1 load failed: {e}")

    predict_path = os.path.join(config.DAILY_GENERAL, f"{date_str}_predict.md")
    predict_md = _read(predict_path)
    predict_missing = predict_md.strip() in ("", "(missing)") or not os.path.isfile(predict_path)

    with open(os.path.join(config.GROUNDING, "outcome_prompt.md"), encoding="utf-8") as fh:
        prompt = fh.read()
    user_msg = (
        f"TODAY: {date_str}\n\n"
        f"=== MORNING PREDICTION ===\n{predict_md}\n\n"
        f"=== MORNING CHANNEL 1 ===\n{morning_md}\n\n"
        f"{fetch_channel1.to_markdown(ch1)}\n\n"
        "Review the day. Cite sources in URL/PUBLISHED/QUOTE/SUMMARY format."
    )
    text = deepseek_client.chat(
        [{"role": "system", "content": prompt},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_OUTCOME, tools=True, max_tokens=12000,
        transcript_path=os.path.join("01_daily/_transcripts", f"{date_str}_outcome.json"),
        trace_path=os.path.join(config.DAILY_GENERAL, f"{date_str}_outcome_trace.md"),
        stage_label=f"OUTCOME {date_str}",
    )

    claims, verify_md = verifier.verify_outcome(text)

    board = scoreboard.load()
    entry = scoreboard.get_or_create(board, date_str, config.TOPIC)
    pred_dir = entry.get("predicted_direction")
    pred_mag = entry.get("predicted_magnitude_band")
    ops_fail = predict_missing or pred_dir is None

    grade = None
    if spx_pct is not None and not ops_fail:
        grade = compute_scores.grade(pred_dir, pred_mag or "flat", spx_pct)
    elif spx_pct is not None and ops_fail:
        ad = "up" if spx_pct > 0.05 else ("down" if spx_pct < -0.05 else "flat")
        grade = {
            "actual_direction": ad,
            "actual_magnitude_band": _actual_band(spx_pct),
            "direction_hit": None,
            "magnitude_hit": None,
        }

    ob = snapshot.parse_kv_block(text, "OUTCOME_BEGIN", "OUTCOME_END")
    snap_entry = dict(entry)
    if grade is not None:
        snap_entry.update({
            "actual_pct_change": spx_pct,
            "actual_direction": grade["actual_direction"],
            "actual_magnitude_band": grade["actual_magnitude_band"],
            "direction_hit": grade["direction_hit"],
            "magnitude_hit": grade["magnitude_hit"],
            "ops_fail": ops_fail,
        })
    snap_entry["path_shape"] = (actual.get("SPX", {}).get("path", {}) or {}).get("shape")

    path = os.path.join(config.DAILY_GENERAL, f"{date_str}_outcome.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"# Post-Market Outcome — {date_str}\n\n")
        if ops_fail:
            fh.write("**OPS_FAIL:** morning predict missing/null — not a market miss.\n\n")
        fh.write(snapshot.outcome_snapshot(snap_entry, ob, claims))
        fh.write(text)
        fh.write(verify_md + "\n")

    if spx_pct is not None:
        entry.update({
            "actual_open": actual["SPX"].get("open"),
            "actual_close": actual["SPX"].get("close"),
            "actual_pct_change": spx_pct,
            "actual_direction": grade["actual_direction"] if grade else None,
            "actual_magnitude_band": grade["actual_magnitude_band"] if grade else None,
            "direction_hit": None if ops_fail else (grade["direction_hit"] if grade else None),
            "magnitude_hit": None if ops_fail else (grade["magnitude_hit"] if grade else None),
            "ops_fail": ops_fail,
            "path_shape": (actual.get("SPX", {}).get("path", {}) or {}).get("shape"),
            "primary_driver": ob.get("PRIMARY_DRIVER") or ob.get("DOMINANT_DRIVER"),
            "key_interaction": ob.get("KEY_INTERACTION"),
            "knowable_at_9am": ob.get("KNOWABLE_AT_9AM"),
            "attribution_contested": ob.get("ATTRIBUTION_CONTESTED"),
            "outlier_watch": ob.get("OUTLIER_WATCH"),
            "per_factor_breakdown": (
                [] if ops_fail else
                compute_scores.per_factor_breakdown(entry.get("components") or {}, spx_pct)
            ),
            "sources_used": [{
                "url": c["url"], "date_accessed": date_str,
                "summary": c["summary"][:200], "verification": c["status"],
            } for c in claims],
        })
        scoreboard.save(board)
        print(f"[outcome] {date_str}: SPX {spx_pct}% ops_fail={ops_fail} "
              f"dir_hit={entry.get('direction_hit')} mag_hit={entry.get('magnitude_hit')}")
    else:
        print(f"[outcome] {date_str}: SPX actuals unavailable")


if __name__ == "__main__":
    main()
