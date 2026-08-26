"""Stage PREDICT (9:00 AM ET): assemble prompt, call DeepSeek with web_search,
write 01_daily/general/<date>_predict.md, parse scores, compute decision
deterministically, update scoreboard.

CLI: python -m src.run_predict [--date YYYY-MM-DD] [--force] [--retries 1]
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from . import (compute_scores, config, deepseek_client, event_context,
               fetch_channel1, memory, output_qc, preopen, scoreboard,
               snapshot)
from .run_news_judge import inject_block as news_judge_block
try:
    from .finviz_digest import inject_block as finviz_digest_block
except Exception:
    def finviz_digest_block(*_a, **_k):
        return ""
try:
    from .map_heat_research import (
        decision_gate as map_heat_decision_gate,
        inject_block as map_heat_research_block,
    )
except Exception:
    def map_heat_research_block(*_a, **_k):
        return ""
    def map_heat_decision_gate(_date, decision, **_kwargs):
        return decision


def _write(path: str, date_str: str, text: str, decision: dict, scores: dict,
           ch1, horizon_calls: dict) -> None:
    os.makedirs(config.DAILY_GENERAL, exist_ok=True)
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


def _update_scoreboard(date_str: str, decision: dict, horizon_calls: dict,
                       nj: str) -> None:
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--retries", type=int, default=1)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    config.require_llm()
    path = os.path.join(config.DAILY_GENERAL, f"{date_str}_predict.md")

    if not args.force:
        existing = output_qc.qc_general_predict(path)
        if existing.ok:
            print(f"[predict] {date_str}: skip, quality-ok already on disk "
                  f"({existing.size} chars) — not rewriting a pre-open copy")
            return
        if os.path.exists(path):
            print(f"[predict] {date_str}: existing file rejected "
                  f"({existing.reason}) — throwing out and rerunning")
            output_qc.reject(path)

    preopen.refuse_if_late("general-predict", force=args.force)

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
    mh = map_heat_research_block(date_str)

    # 2. Assemble prompt: rubric + event scan + news judge + finviz digest + memory + channel 1
    with open(os.path.join(config.GROUNDING, "master_rubric.md"),
              encoding="utf-8") as fh:
        rubric = fh.read()
    user_msg = (f"TODAY: {date_str} (America/New_York)\n\n"
                f"{event_context.block()}\n\n"
                f"{nj}"
                f"{fv}"
                f"{mh}"
                f"{memory.prediction_context()}\n\n{ch1_md}\n\n"
                "Execute the full rubric now. Remember: use web_search for "
                "ALL six Channel 2 categories before scoring.\n"
                "When NEWS JUDGE is present, treat its ranked MACRO/SECTOR "
                "lines as the primary B1 catalyst input; raw Channel 1 news "
                "is secondary corroboration only.\n"
                "When FINVIZ DAILY DIGEST is present, treat its index narratives "
                "and high-signal ticker digests as pre-validated elevated themes "
                "— use them to reinforce or correct thin/noisy mechanical parses.\n"
                "When MAP HEAT RESEARCH is present, honor nested OVERRIDE/SPLIT "
                "(do not average Uranium into Energy) and the size_gate "
                "(do not promote XLK/SPX notable UP into a mega-cap print).\n"
                "First line MUST be MEMORY_CONFIRM. Then analysis. Then a "
                "SCORES_BEGIN..SCORES_END block. HIT_GRID_BEGIN is required "
                "when the rubric asks for it.\n"
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

    last_qc = None
    for attempt in range(args.retries + 1):
        if attempt and preopen.past_predict_cutoff() and not args.force:
            print("[predict] past 09:25 ET, not retrying")
            break
        text = deepseek_client.chat(
            [{"role": "system", "content": rubric},
             {"role": "user", "content": user_msg}],
            model=config.MODEL_PREDICT, tools=True, max_tokens=8000,
            transcript_path=os.path.join("01_daily/_transcripts",
                                         f"{date_str}_predict.json"),
            trace_path=os.path.join(config.DAILY_GENERAL,
                                    f"{date_str}_predict_trace.md"),
            stage_label=f"PREDICT {date_str}"
                        + (f" retry{attempt}" if attempt else ""))

        raw_qc = output_qc.qc_text_general_predict(text or "", path)
        if not raw_qc.ok:
            print(f"[predict] attempt {attempt + 1} raw QC FAIL "
                  f"({raw_qc.reason}) — not writing a stub")
            last_qc = raw_qc
            continue

        scores = compute_scores.parse_scores(text)
        decision = compute_scores.compute(scores)
        decision = map_heat_decision_gate(date_str, decision)
        horizon_calls = compute_scores.parse_horizon_calls(scores)
        _write(path, date_str, text, decision, scores, ch1, horizon_calls)

        file_qc = output_qc.qc_general_predict(path)
        last_qc = file_qc
        if not file_qc.ok:
            print(f"[predict] attempt {attempt + 1} file QC FAIL "
                  f"({file_qc.reason}) — throwing out")
            output_qc.reject(path)
            continue

        _update_scoreboard(date_str, decision, horizon_calls, nj)
        print(f"[predict] {date_str}: {decision['predicted_direction']}/"
              f"{decision['predicted_magnitude_band']} "
              f"(total {decision['total_score']}, div={decision['divergence_flagged']}, "
              f"horizons={len(horizon_calls)}, news_judge={'yes' if nj else 'no'})"
              f" -> {path}")
        return

    print(f"[predict] FAIL-CLOSED after {args.retries + 1} attempt(s) "
          f"({getattr(last_qc, 'reason', 'unknown')}) — no file, no scoreboard")
    output_qc.reject(path)
    raise SystemExit("general predict produced no quality-ok essay")


if __name__ == "__main__":
    main()
