"""Stage PREDICT (9:00 AM ET): assemble prompt, call DeepSeek with web_search,
write 01_daily/general/<date>_predict.md, parse scores, compute decision
deterministically, update scoreboard.

CLI: python -m src.run_predict [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from . import (compute_scores, config, deepseek_client, event_context,
               fetch_channel1, memory, scoreboard, snapshot)
from .run_news_judge import inject_block as news_judge_block
try:
    from .finviz_digest import inject_block as finviz_digest_block
except Exception:
    def finviz_digest_block(*_a, **_k):
        return ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    config.require_llm()

    # 1. Channel 1 (deterministic) — archived for auditability
    ch1 = fetch_channel1.build("predict")
    fetch_channel1.save(ch1, date_str, "predict")
    ch1_md = fetch_channel1.to_markdown(ch1)

    # 1b. Ranked news judge (LLM layer on mechanical parse) — preferred B1 input
    nj = news_judge_block(date_str)
    if not nj:
        nj = news_judge_block()  # fall back to latest_judge.md

    # 1c. Finviz Daily Digest (export + index narratives) — elevated themes
    fv = finviz_digest_block(date_str) or finviz_digest_block()

    # 2. Assemble prompt: rubric + event scan + news judge + finviz digest + memory + channel 1
    with open(os.path.join(config.GROUNDING, "master_rubric.md"),
              encoding="utf-8") as fh:
        rubric = fh.read()
    user_msg = (f"TODAY: {date_str} (America/New_York)\n\n"
                f"{event_context.block()}\n\n"
                f"{nj}"
                f"{fv}"
                f"{memory.prediction_context()}\n\n{ch1_md}\n\n"
                "Execute the full rubric now. Remember: use web_search for "
                "ALL six Channel 2 categories before scoring.\n"
                "When NEWS JUDGE is present, treat its ranked MACRO/SECTOR "
                "lines as the primary B1 catalyst input; raw Channel 1 news "
                "is secondary corroboration only.\n"
                "When FINVIZ DAILY DIGEST is present, treat its index narratives "
                "and high-signal ticker digests as pre-validated elevated themes "
                "— use them to reinforce or correct thin/noisy mechanical parses.\n"
                "In the SCORES block, also include these three lines:\n"
                "GOOD_NEWS: <semicolon-separated list, max 5, short phrases>\n"
                "BAD_NEWS: <semicolon-separated list, max 5, short phrases>\n"
                "UNCERTAINTY_LEVEL: <low|moderate|elevated|high>\n"
                "And these four multi-timeframe outlook lines (your call for "
                "SPX over each horizon, format dir:band:confidence):\n"
                "HORIZON_3D: <up|down|flat>:<flat|mild|notable|severe>:<0-1>\n"
                "HORIZON_1W: <up|down|flat>:<flat|mild|notable|severe>:<0-1>\n"
                "HORIZON_2W: <up|down|flat>:<flat|mild|notable|severe>:<0-1>\n"
                "HORIZON_1M: <up|down|flat>:<flat|mild|notable|severe>:<0-1>")

    # 3. LLM with tool loop (full transcript + readable trace saved)
    text = deepseek_client.chat(
        [{"role": "system", "content": rubric},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_PREDICT, tools=True, max_tokens=8000,
        transcript_path=os.path.join("01_daily/_transcripts",
                                     f"{date_str}_predict.json"),
        trace_path=os.path.join(config.DAILY_GENERAL,
                                f"{date_str}_predict_trace.md"),
        stage_label=f"PREDICT {date_str}")

    # 4. Deterministic scoring
    scores = compute_scores.parse_scores(text)
    decision = compute_scores.compute(scores)
    horizon_calls = compute_scores.parse_horizon_calls(scores)

    # 5. Write prediction file (human-readable snapshot first)
    os.makedirs(config.DAILY_GENERAL, exist_ok=True)
    path = os.path.join(config.DAILY_GENERAL, f"{date_str}_predict.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"# Premarket Prediction — {date_str}\n\n")
        fh.write(snapshot.predict_snapshot(decision, scores, ch1))
        fh.write(text)
        fh.write("\n\n---\n## Pipeline-computed decision (deterministic)\n\n")
        fh.write(f"- total_score: **{decision['total_score']}** "
                 f"(multiplier {decision['multiplier']})\n")
        fh.write(f"- leading_sum: {decision['leading_sum']}\n")
        fh.write(f"- divergence_flagged: **{decision['divergence_flagged']}**\n")
        fh.write(f"- predicted_direction: **{decision['predicted_direction']}**\n")
        fh.write(f"- predicted_magnitude_band: "
                 f"**{decision['predicted_magnitude_band']}**\n")
        fh.write(f"- confidence_score: {decision['confidence_score']}\n")
        if horizon_calls:
            fh.write("\n### Multi-timeframe outlook (LLM call, graded at "
                     "T+h by src.horizon_grade)\n\n")
            for key, hc in horizon_calls.items():
                fh.write(f"- {key}: **{hc['direction']}** / "
                         f"{hc['magnitude_band']} (conf {hc['confidence']})\n")

    # 6. Scoreboard
    board = scoreboard.load()
    entry = scoreboard.get_or_create(board, date_str, config.TOPIC)
    entry.update({
        "predicted_direction": decision["predicted_direction"],
        "predicted_magnitude_band": decision["predicted_magnitude_band"],
        "confidence_score": decision["confidence_score"],
        "total_score": decision["total_score"],
        "multiplier": decision["multiplier"],
        "components": decision["components"],
        "leading_sum": decision["leading_sum"],
        "divergence_flagged": decision["divergence_flagged"],
        "horizon_calls": horizon_calls,
        "news_judge_present": bool(nj),
    })
    scoreboard.save(board)
    print(f"[predict] {date_str}: {decision['predicted_direction']}/"
          f"{decision['predicted_magnitude_band']} "
          f"(total {decision['total_score']}, div={decision['divergence_flagged']}, "
          f"horizons={len(horizon_calls)}, news_judge={'yes' if nj else 'no'})"
          f" -> {path}")


if __name__ == "__main__":
    main()
