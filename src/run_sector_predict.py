"""Per-sector environment scan (11 Finviz sectors).

Uses sector_taxonomy (HIT labels/weights/searches/amp-damp) + DeepSeek web_search
to fill HIT/MISS grids. Deterministic ETF relative performance is injected as
Channel-1 context. Scoring is parallel to catalyst net → Lead/Lag.

Outputs (full set, every sector):
  01_daily/sectors/<date>/<sector_slug>_predict.md
  01_daily/sectors/<date>/<sector_slug>_grid.json
  01_daily/sectors/<date>/_summary.md

CLI:
  python -m src.run_sector_predict [--date YYYY-MM-DD] [--sectors Technology,Energy]
  python -m src.run_sector_predict --date 2026-08-11 --no-llm   # ETF channel only
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, deepseek_client
from .sector_engine import (build_sector_prompt, etf_relative_snapshot,
                            format_sector_markdown, merge_llm_grid, score_grid,
                            search_query_bundle)
from .sector_taxonomy import FINVIZ_SECTORS, SECTOR_ETFS, validate


def _slug(sector: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")


def _parse_json(raw: str) -> dict | None:
    if not raw:
        return None
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, re.S)
        if not m:
            return None
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            return None


def run_one(sector: str, date_str: str, out_dir: str, use_llm: bool) -> dict:
    etf_ctx = etf_relative_snapshot(sector)
    grid = merge_llm_grid(sector, None)
    horizons = {}
    stack = ""
    key_assumption = ""
    raw_text = ""

    if use_llm:
        if not config.DEEPSEEK_API_KEY:
            raise SystemExit("DEEPSEEK_API_KEY not set")
        system = build_sector_prompt(sector, date_str, etf_ctx)
        # Seed a few high-value searches in the user message so the model
        # does not burn the whole tool budget inventing queries.
        seeds = search_query_bundle(sector, limit=12)
        user = (
            f"Analyze {sector} for {date_str}.\n"
            f"Suggested searches (use web_search on the most relevant):\n"
            + "\n".join(f"- {q}" for q in seeds)
            + "\n\nReturn ONLY the JSON object specified in the system prompt."
        )
        slug = _slug(sector)
        raw_text = deepseek_client.chat(
            [{"role": "system", "content": system},
             {"role": "user", "content": user}],
            model=config.MODEL_PREDICT,
            tools=True,
            max_tokens=6000,
            transcript_path=os.path.join(
                "01_daily/_transcripts", f"{date_str}_sector_{slug}.json"),
            trace_path=os.path.join(out_dir, f"{slug}_trace.md"),
            stage_label=f"SECTOR {sector} {date_str}",
        )
        parsed = _parse_json(raw_text)
        if parsed:
            grid = merge_llm_grid(sector, parsed.get("catalyst_grid"))
            horizons = parsed.get("horizons") or {}
            stack = parsed.get("sector_stack") or ""
            key_assumption = parsed.get("key_assumption") or ""
        else:
            stack = "(LLM JSON parse failed — ETF channel only; see raw below)"
            key_assumption = "parse_failure"

    scored = score_grid(grid)
    # If LLM did not supply horizon labels, mirror net label on all horizons
    if not horizons:
        for h in ("1d", "3d", "1w", "1m"):
            horizons[h] = {
                "label": scored["label"],
                "conviction": scored["conviction"],
                "note": "mirrored from Sector_Net" if use_llm else "ETF-only / empty grid",
            }

    md = format_sector_markdown(
        sector, scored, grid, horizons, stack, etf_ctx, key_assumption)
    if raw_text and key_assumption == "parse_failure":
        md += "\n## Raw model output\n\n```\n" + raw_text[:4000] + "\n```\n"

    slug = _slug(sector)
    md_path = os.path.join(out_dir, f"{slug}_predict.md")
    js_path = os.path.join(out_dir, f"{slug}_grid.json")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write(md)
    payload = {
        "sector": sector,
        "etf": SECTOR_ETFS.get(sector),
        "date": date_str,
        "scored": scored,
        "horizons": horizons,
        "grid": grid,
        "etf_context": etf_ctx,
        "key_assumption": key_assumption,
    }
    with open(js_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False, default=str)
    print(f"[sector] {sector}: {scored['label']} net={scored['net']:+.1f} "
          f"hits=+{scored['n_pos_hits']}/-{scored['n_neg_hits']} -> {md_path}")
    return payload


def write_summary(date_str: str, out_dir: str, results: list[dict]) -> None:
    results = sorted(results, key=lambda r: -r["scored"]["net"])
    L = [f"# Sector Lead/Lag summary — {date_str}", "",
         "Macro environment only (not stock picks). "
         "Net from sector factor HIT grid (catalyst-style).", "",
         "| Sector | ETF | Net | Label | +HITs | −HITs | 1d | 1w |",
         "|---|---|---|---|---|---|---|---|"]
    for r in results:
        h = r.get("horizons") or {}
        L.append(
            f"| {r['sector']} | {r.get('etf')} | {r['scored']['net']:+.1f} | "
            f"{r['scored']['label']} | {r['scored']['n_pos_hits']} | "
            f"{r['scored']['n_neg_hits']} | "
            f"{(h.get('1d') or {}).get('label', '?')} | "
            f"{(h.get('1w') or {}).get('label', '?')} |"
        )
    L.append("")
    path = os.path.join(out_dir, "_summary.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L))
    print(f"[sector] summary -> {path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--sectors", default=None,
                    help="Comma-separated subset (default: all 11)")
    ap.add_argument("--no-llm", action="store_true",
                    help="Skip DeepSeek; write ETF context + empty grids only")
    args = ap.parse_args()

    errs = validate()
    if errs:
        raise SystemExit(f"taxonomy invalid: {errs}")

    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    if args.sectors:
        wanted = [s.strip() for s in args.sectors.split(",") if s.strip()]
        for s in wanted:
            if s not in FINVIZ_SECTORS:
                raise SystemExit(f"unknown sector {s!r}; choose from {FINVIZ_SECTORS}")
        sectors = wanted
    else:
        sectors = list(FINVIZ_SECTORS)

    out_dir = os.path.join(config.DAILY_SECTORS, date_str)
    os.makedirs(out_dir, exist_ok=True)

    results = []
    for sector in sectors:
        results.append(run_one(sector, date_str, out_dir, use_llm=not args.no_llm))
    write_summary(date_str, out_dir, results)


if __name__ == "__main__":
    main()
