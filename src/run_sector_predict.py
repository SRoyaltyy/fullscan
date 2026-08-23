"""Sector PREDICT — general method + per-sector specialized rubric.

System prompt = sector_method.md (shared machine) + 00_grounding/sectors/<slug>.md
User prompt   = sector memory + full Channel 1 (same as general) + ETF tape + search seeds

CLI:
  python -m src.run_sector_predict [--date YYYY-MM-DD] [--sectors Technology,Energy]
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import compute_scores, compute_sector_scores, config, deepseek_client, fetch_channel1, scoreboard
from .sector_engine import etf_relative_snapshot, search_query_bundle
from .sector_memory import prediction_context, topic_for
from .sector_taxonomy import FINVIZ_SECTORS, SECTOR_ETFS, amp_damp_table, taxonomy_list, validate
from .run_news_judge import inject_block as news_judge_block
try:
    from .finviz_digest import inject_block as finviz_digest_block
except Exception:
    def finviz_digest_block(*_a, **_k):
        return ""


def _slug(sector: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")


def _load_system_prompt(sector: str) -> str:
    method_path = os.path.join(config.GROUNDING, "sector_method.md")
    sector_path = os.path.join(config.GROUNDING, "sectors", f"{_slug(sector)}.md")
    with open(method_path, encoding="utf-8") as fh:
        method = fh.read()
    if not os.path.exists(sector_path):
        raise SystemExit(f"missing sector rubric: {sector_path}")
    with open(sector_path, encoding="utf-8") as fh:
        specialized = fh.read()
    # Machine taxonomy appendix (labels stay stable for lessons)
    labs = taxonomy_list(sector)
    checklist = "\n".join(f"  - {x}" for x in labs)
    appendix = (
        "\n\n=== FULL TAXONOMY LABEL LIST (use exact strings in HIT_GRID) ===\n"
        f"{checklist}\n\n"
        f"=== AMP/DAMP ONE-LINERS ===\n{amp_damp_table(sector)}\n"
    )
    return method + "\n\n" + specialized + appendix


def run_one(sector: str, date_str: str, ch1_md: str) -> dict:
    config.require_llm(config.MODEL_PREDICT)

    rubric = _load_system_prompt(sector)
    etf_ctx = etf_relative_snapshot(sector)
    seeds = search_query_bundle(sector, limit=16)
    nj = news_judge_block(date_str) or news_judge_block()
    fv = finviz_digest_block(date_str) or finviz_digest_block()

    user_msg = (
        f"TODAY: {date_str} (America/New_York)\n"
        f"SECTOR UNDER ANALYSIS (ONLY THIS ONE): {sector}\n"
        f"ETF TO GRADE LATER: {SECTOR_ETFS.get(sector)}\n\n"
        f"{prediction_context(sector)}\n\n"
        f"{nj}"
        f"{fv}"
        f"{ch1_md}\n\n"
        f"=== CHANNEL 1 SECTOR ETF TAPE (also pre-fetched) ===\n"
        f"{etf_ctx or '(unavailable)'}\n\n"
        "Suggested web_search seeds (expand; cover all Channel 2 categories):\n"
        + "\n".join(f"- {q}" for q in seeds)
        + "\n\nWhen NEWS JUDGE / FINVIZ DIGEST is present, treat ranked "
          "MACRO/SECTOR lines that mention THIS sector as primary S1 input.\n"
          "Execute the shared method + THIS sector layer now. "
          "Specialize S1 to the spine factors. "
          "MEMORY_CONFIRM first, then analysis, then SECTOR_SCORES block. "
          "In the SECTOR_SCORES block, also include these four "
          "multi-timeframe outlook lines for THIS sector's ETF "
          "(format dir:band:confidence):\n"
          "HORIZON_3D: <up|down|flat>:<flat|mild|notable|severe>:<0-1>\n"
          "HORIZON_1W: <up|down|flat>:<flat|mild|notable|severe>:<0-1>\n"
          "HORIZON_2W: <up|down|flat>:<flat|mild|notable|severe>:<0-1>\n"
          "HORIZON_1M: <up|down|flat>:<flat|mild|notable|severe>:<0-1>"
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
    horizon_calls = compute_scores.parse_horizon_calls(scores)

    path = os.path.join(out_dir, f"{slug}_predict.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"# Sector Prediction — {sector} — {date_str}\n\n")
        fh.write(f"- ETF: **{SECTOR_ETFS.get(sector)}**\n")
        fh.write(f"- rubric: `00_grounding/sectors/{slug}.md`\n")
        fh.write(f"- predicted_direction: **{decision['predicted_direction']}**\n")
        fh.write(f"- predicted_magnitude_band: **{decision['predicted_magnitude_band']}**\n")
        fh.write(f"- total_score: **{decision['total_score']}** "
                 f"(mult {decision['multiplier']})\n")
        fh.write(f"- regime: {decision.get('regime')}\n")
        fh.write(f"- divergence_flagged: **{decision['divergence_flagged']}**\n\n")
        fh.write("## Channel 1 sector ETF tape\n\n```\n"
                 + (etf_ctx or "") + "\n```\n\n")
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
        "horizon_calls": horizon_calls,
        "sector": sector,
        "etf": SECTOR_ETFS.get(sector),
        "rubric": f"00_grounding/sectors/{slug}.md",
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

    # Same Channel 1 build as general predict — once per batch
    try:
        ch1 = fetch_channel1.build("predict")
        fetch_channel1.save(ch1, date_str, "sector_predict")
        ch1_md = fetch_channel1.to_markdown(ch1)
    except Exception as e:  # noqa: BLE001
        print(f"[sector-predict] Channel 1 build failed ({e}); continuing with stub")
        ch1_md = ("=== CHANNEL 1: PRE-FETCHED DATA ===\n"
                  "(unavailable this run — do not invent precise levels; "
                  "use web_search cautiously for macro and state uncertainty)\n")

    for sector in sectors:
        print(f"\n======== SECTOR PREDICT: {sector} ========\n")
        run_one(sector, date_str, ch1_md)


if __name__ == "__main__":
    main()
