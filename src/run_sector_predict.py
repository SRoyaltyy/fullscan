"""Sector PREDICT — general method + per-sector specialized rubric.

System prompt = sector_method.md (shared machine) + 00_grounding/sectors/<slug>.md
User prompt   = sector memory + full Channel 1 (same as general) + ETF tape + search seeds

CLI:
  python -m src.run_sector_predict [--date YYYY-MM-DD] [--sectors Technology,Energy]
                                   [--force] [--retries 1]
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import (compute_scores, compute_sector_scores, config, deepseek_client,
               fetch_channel1, output_qc, preopen, scoreboard)
from .sector_engine import etf_relative_snapshot, search_query_bundle
from .sector_memory import prediction_context, topic_for
from .sector_taxonomy import FINVIZ_SECTORS, SECTOR_ETFS, amp_damp_table, taxonomy_list, validate
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


def _write_essay(path: str, sector: str, date_str: str, slug: str,
                 etf_ctx: str, text: str, decision: dict) -> None:
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


def _update_scoreboard(sector: str, date_str: str, slug: str,
                       decision: dict, horizon_calls: dict) -> None:
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


def run_one(sector: str, date_str: str, ch1_md: str,
            retries: int = 1, force: bool = False) -> dict:
    config.require_llm()

    slug = _slug(sector)
    out_dir = os.path.join(config.DAILY_SECTORS, date_str)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{slug}_predict.md")

    # Skip-if-good: a previous pass (or a same-day retry) already produced
    # a quality essay. Do not burn another 6–15 min on OpenClaw, and do
    # not overwrite a pre-open copy.
    if not force:
        existing = output_qc.qc_sector_predict(path)
        if existing.ok:
            print(f"[sector-predict] {sector}: skip, quality-ok already on disk "
                  f"({existing.size} chars)")
            return {"skipped": True, "quality": "ok", "path": path}
        if os.path.exists(path):
            print(f"[sector-predict] {sector}: existing file rejected "
                  f"({existing.reason}) — throwing out and rerunning")
            output_qc.reject(path)

    # Late dispatch: refuse NEW writes (skip-if-good already returned).
    try:
        preopen.refuse_if_late(f"sector-predict {sector}", force=force)
    except SystemExit as e:
        print(str(e))
        return {"skipped": True, "reason": "past_cutoff", "path": path}

    rubric = _load_system_prompt(sector)
    etf_ctx = etf_relative_snapshot(sector)
    seeds = search_query_bundle(sector, limit=16)
    nj = news_judge_block(date_str) or news_judge_block()
    fv = finviz_digest_block(date_str) or finviz_digest_block()
    mh = map_heat_research_block(date_str, sector=sector)

    user_msg = (
        f"TODAY: {date_str} (America/New_York)\n"
        f"SECTOR UNDER ANALYSIS (ONLY THIS ONE): {sector}\n"
        f"ETF TO GRADE LATER: {SECTOR_ETFS.get(sector)}\n\n"
        f"{prediction_context(sector)}\n\n"
        f"{nj}"
        f"{fv}"
        f"{mh}"
        f"{ch1_md}\n\n"
        f"=== CHANNEL 1 SECTOR ETF TAPE (also pre-fetched) ===\n"
        f"{etf_ctx or '(unavailable)'}\n\n"
        "Suggested web_search seeds (expand; cover all Channel 2 categories):\n"
        + "\n".join(f"- {q}" for q in seeds)
        + "\n\nWhen NEWS JUDGE / FINVIZ DIGEST is present, treat ranked "
          "MACRO/SECTOR lines that mention THIS sector as primary S1 input.\n"
          "When MAP HEAT RESEARCH is present for THIS sector, nested "
          "OVERRIDE/SPLIT beats the parent ETF: do not bury a hot child "
          "(e.g. Uranium) inside a weak parent (Energy), and honor size_gate.\n"
          "Execute the shared method + THIS sector layer now. "
          "Specialize S1 to the spine factors. "
          "MEMORY_CONFIRM first, then analysis, then SECTOR_SCORES block. "
          "HIT_GRID_BEGIN is required. "
          "In the SECTOR_SCORES block, also include these four "
          "multi-timeframe outlook lines for THIS sector's ETF "
          "(format dir:band:confidence):\n"
          "HORIZON_3D: <up|down|flat>:<flat|mild|notable|severe>:<0-1>\n"
          "HORIZON_1W: <up|down|flat>:<flat|mild|notable|severe>:<0-1>\n"
          "HORIZON_2W: <up|down|flat>:<flat|mild|notable|severe>:<0-1>\n"
          "HORIZON_1M: <up|down|flat>:<flat|mild|notable|severe>:<0-1>"
    )

    last_qc = None
    retry_extra = ""
    for attempt in range(retries + 1):
        if attempt and preopen.past_predict_cutoff() and not force:
            print(f"[sector-predict] {sector}: past 09:25 ET, not retrying")
            break
        text = deepseek_client.chat(
            [{"role": "system", "content": rubric},
             {"role": "user", "content": user_msg + retry_extra}],
            model=config.MODEL_PREDICT,
            tools=True,
            max_tokens=8000,
            transcript_path=os.path.join(
                "01_daily/_transcripts",
                f"{date_str}_sector_{slug}_predict.json"),
            trace_path=os.path.join(out_dir, f"{slug}_predict_trace.md"),
            stage_label=f"SECTOR PREDICT {sector} {date_str}"
                        + (f" retry{attempt}" if attempt else ""),
        )

        # Fail-closed on timeout/empty BEFORE parse_scores can emit 0/flat.
        raw_qc = output_qc.qc_text_sector_predict(text or "", path)
        if not raw_qc.ok:
            print(f"[sector-predict] {sector}: attempt {attempt + 1} raw QC "
                  f"FAIL ({raw_qc.reason}) — not writing a stub")
            last_qc = raw_qc
            retry_extra = (
                "\n\nRETRY CONSTRAINT: previous attempt failed QC "
                f"({raw_qc.reason}). The essay MUST contain MEMORY_CONFIRM, "
                "HIT_GRID_BEGIN, and SECTOR_SCORES_BEGIN ... SECTOR_SCORES_END "
                "with at least three S0_/S1_/S2_ numeric lines. "
                "A long analysis without those tokens is trash.\n"
            )
            continue

        scores = compute_sector_scores.parse_scores(text)
        decision = compute_sector_scores.compute(scores)
        decision = map_heat_decision_gate(date_str, decision, sector=sector)
        horizon_calls = compute_scores.parse_horizon_calls(scores)
        _write_essay(path, sector, date_str, slug, etf_ctx or "", text, decision)

        # Self-read the file we just wrote.
        file_qc = output_qc.qc_sector_predict(path)
        last_qc = file_qc
        if not file_qc.ok:
            print(f"[sector-predict] {sector}: attempt {attempt + 1} file QC "
                  f"FAIL ({file_qc.reason}) — throwing out")
            output_qc.reject(path)
            continue

        _update_scoreboard(sector, date_str, slug, decision, horizon_calls)
        print(f"[sector-predict] {sector}: {decision['predicted_direction']}/"
              f"{decision['predicted_magnitude_band']} "
              f"total={decision['total_score']} -> {path}")
        return decision

    print(f"[sector-predict] {sector}: FAIL-CLOSED after {retries + 1} "
          f"attempt(s) ({getattr(last_qc, 'reason', 'unknown')}) — "
          f"no 0/flat stub, scoreboard not updated")
    output_qc.reject(path)
    return {"skipped": True, "quality": "fail",
            "reason": getattr(last_qc, "reason", "qc_failed"), "path": path}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--sectors", default=None,
                    help="Comma-separated; default all 11 sequential")
    ap.add_argument("--force", action="store_true",
                    help="Ignore skip-if-good and 09:25 ET cutoff")
    ap.add_argument("--retries", type=int, default=1,
                    help="Per-sector QC retries after the first attempt")
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

    n_ok = n_skip = n_fail = 0
    for sector in sectors:
        print(f"\n======== SECTOR PREDICT: {sector} ========\n")
        result = run_one(sector, date_str, ch1_md,
                         retries=args.retries, force=args.force)
        if result.get("quality") == "ok" or result.get("skipped") and result.get("quality") == "ok":
            n_skip += 1
            n_ok += 1
        elif result.get("quality") == "fail" or result.get("reason") == "past_cutoff":
            if result.get("reason") == "past_cutoff":
                n_skip += 1
            else:
                n_fail += 1
        elif result.get("skipped"):
            n_skip += 1
        else:
            n_ok += 1

    # Re-count from disk so skip-if-good and this run agree.
    report = output_qc.preopen_report(date_str)
    n_ok = report["sector_n_ok"]
    output_qc.write_preopen_report(date_str)
    print(f"\n[sector-predict] QC {n_ok}/{len(FINVIZ_SECTORS)} quality-ok "
          f"(this-run skip={n_skip} fail={n_fail})")
    if n_ok == 0:
        raise SystemExit("no quality-ok sector essays on disk")


if __name__ == "__main__":
    main()
