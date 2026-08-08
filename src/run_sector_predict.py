"""Sector PREDICT — one sector = one independent LLM run (same method as general).

CLI:
  python -m src.run_sector_predict [--date YYYY-MM-DD] [--sectors Technology,Energy]
  python -m src.run_sector_predict --date 2026-08-11          # all 11, sequential
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import compute_sector_scores, config, deepseek_client, scoreboard
from .sector_engine import etf_relative_snapshot, search_query_bundle
from .sector_memory import prediction_context, topic_for
from .sector_taxonomy import FINVIZ_SECTORS, SECTOR_ETFS, validate


def _slug(sector: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")


def run_one(sector: str, date_str: str) -> dict:
    if not config.DEEPSEEK_API_KEY:
        raise SystemExit("DEEPSEEK_API_KEY not set")

    with open(os.path.join(config.GROUNDING, "sector_rubric.md"),
              encoding="utf-8") as fh:
        rubric = fh.read()

    etf_ctx = etf_relative_snapshot(sector)
    seeds = search_query_bundle(sector, limit=14)
    user_msg = (
        f"TODAY: {date_str} (America/New_York)\n"
        f"SECTOR UNDER ANALYSIS (ONLY THIS ONE): {sector}\n"
        f"ETF: {SECTOR_ETFS.get(sector)}\n\n"
        f"{prediction_context(sector)}\n\n"
        f"=== CHANNEL 1 ETF CONTEXT ===\n{etf_ctx or '(unavailable)'}\n\n"
        "Suggested web_search seeds (use tools; expand as needed):\n"
        + "\n".join(f"- {q}" for q in seeds)
        + "\n\nExecute the full sector rubric now. "
          "Research thoroughly. Take as long as needed. "
          "Output MEMORY_CONFIRM first, then analysis, then SECTOR_SCORES block."
    )

    slug = _slug(sector)
    out_dir = os.path.join(config.DAILY_SECTORS, date_str)
    os.makedirs(out_dir, exist_ok=True)

    text = deepseek_client.chat(
        [{"role": "system", "content": rubric},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_PREDICT,
        tools=True,
        max_tokens=8000,
        transcript_path=os.path.join(
            "01_daily/_transcripts", f"{date_str}_sector_{slug}_predict.json"),
        trace_path=os.path.join(out_dir, f"{slug}_predict_trace.md"),
        stage_label=f"SECTOR PREDICT {sector} {date_str}",
    )

    scores = compute_sector_scores.parse_scores(text)
    decision = compute_sector_scores.compute(scores)

    path = os.path.join(out_dir, f"{slug}_predict.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"# Sector Prediction — {sector} — {date_str}\n\n")
        fh.write(f"- ETF: **{SECTOR_ETFS.get(sector)}**\n")
        fh.write(f"- predicted_direction: **{decision['predicted_direction']}**\n")
        fh.write(f"- predicted_magnitude_band: **{decision['predicted_magnitude_band']}**\n")
        fh.write(f"- total_score: **{decision['total_score']}** "
                 f"(mult {decision['multiplier']})\n")
        fh.write(f"- regime: {decision.get('regime')}\n")
        fh.write(f"- divergence_flagged: **{decision['divergence_flagged']}**\n\n")
        fh.write("## Channel 1 ETF context\n\n```\n" + (etf_ctx or "") + "\n```\n\n")
        fh.write(text)
        fh.write("\n\n---\n## Pipeline-computed decision (deterministic)\n\n")
        fh.write(f"```json\n{decision}\n```\n")

    topic = topic_for(sector)
    board = scoreboard.load()
    entry = scoreboard.get_or_create(board, date_str, topic)
    entry.update({
        "predicted_direction": decision["predicted_direction"],
        "predicted_magnitude_band": decision["predicted_magnitude_band"],
        "confidence_score": decision["confidence_score"],
        "total_score": decision["total_score"],
        "multiplier": decision["multiplier"],
        "components": decision["components"],
        "leading_sum": decision["leading_sum"],
        "divergence_flagged": decision["divergence_flagged"],
        "sector": sector,
        "etf": SECTOR_ETFS.get(sector),
    })
    scoreboard.save(board)
    print(f"[sector-predict] {sector}: {decision['predicted_direction']}/"
          f"{decision['predicted_magnitude_band']} total={decision['total_score']} -> {path}")
    return decision


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--sectors", default=None,
                    help="Comma-separated; default all 11 sequential")
    args = ap.parse_args()

    errs = validate()
    if errs:
        raise SystemExit(f"taxonomy invalid: {errs}")

    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    if args.sectors:
        sectors = [s.strip() for s in args.sectors.split(",") if s.strip()]
        for s in sectors:
            if s not in FINVIZ_SECTORS:
                raise SystemExit(f"unknown sector {s!r}")
    else:
        sectors = list(FINVIZ_SECTORS)

    for sector in sectors:
        print(f"\n======== SECTOR PREDICT: {sector} ========\n")
        run_one(sector, date_str)


if __name__ == "__main__":
    main()
