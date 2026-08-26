"""Fast pre-open delta refresh over the post-close captain baseline.

One Grok call updates only the industries that are hot/cold/overrides now,
plus captains on today's earnings calendar. It merges those cards into the
exhaustive post-close baseline and applies the fresh futures/calendar gate.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, deepseek_client, output_qc, preopen
from .map_heat_evidence import opportunity_tickers_valid, validate_cards
from .map_heat_postclose import all_targets
from .map_heat_research import extract_json, render

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "01_daily" / "map_heat"
PROMPT = ROOT / "00_grounding" / "map_heat_research_prompt.md"
ET = ZoneInfo(config.TZ)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _refresh_targets(heat: dict, baseline: dict, limit: int = 28) -> list[dict]:
    current = {t["industry"]: t for t in all_targets(heat)}
    names = []
    for row in (heat.get("overrides") or []) + (heat.get("hot") or []) + (heat.get("cold") or []):
        name = str(row.get("industry") or "")
        if name and name not in names:
            names.append(name)
    earning = {str(e.get("ticker") or "").upper() for e in heat.get("earnings") or []}
    for card in baseline.get("cards") or []:
        if any(str(c.get("ticker") or "").upper() in earning
               for c in card.get("captains") or []):
            name = str(card.get("industry") or "")
            if name and name not in names:
                names.append(name)
    return [current[n] for n in names if n in current][:limit]


def _chat(system: str, user: str, date: str) -> str:
    return deepseek_client.chat(
        [{"role": "system", "content": system},
         {"role": "user", "content": user}],
        model=config.MODEL_PREDICT, tools=True, max_tokens=24000,
        transcript_path=str(ROOT / "01_daily" / "_transcripts"
                            / f"{date}_map_heat_refresh.json"),
        trace_path=str(OUT / f"{date}_refresh_trace.md"),
        stage_label=f"MAP HEAT MORNING REFRESH {date}",
    )


def run(date: str, force: bool = False) -> dict:
    preopen.refuse_if_late("map_heat_refresh", force=force)
    heat_path = OUT / f"{date}_map_heat.json"
    base_path = OUT / f"{date}_research_baseline.json"
    final_path = OUT / f"{date}_research.json"
    final_md = OUT / f"{date}_research.md"
    if not force and final_path.exists() and final_md.exists():
        qc = output_qc.qc_map_heat_research(final_md)
        if qc.ok:
            print(f"[map-refresh] quality-present: {final_path}")
            return _load(final_path)
    if not heat_path.exists():
        raise SystemExit(f"morning map heat missing: {heat_path}")
    if not base_path.exists():
        raise SystemExit(
            f"post-close baseline missing: {base_path}; research is optional "
            "today, do not run the old exhaustive pass pre-open"
        )
    config.require_llm()
    heat, baseline = _load(heat_path), _load(base_path)
    targets = _refresh_targets(heat, baseline)
    if len(targets) < 3:
        raise SystemExit(f"too few morning refresh targets: {len(targets)}")
    base_by = {str(c.get("industry") or ""): c for c in baseline.get("cards") or []}
    ticker_set = {
        str(c.get("ticker") or "").upper()
        for t in targets for k in ("spx_leaders", "rut_leaders")
        for c in t.get(k) or []
    }
    news = [
        n for n in heat.get("ticker_news") or []
        if ticker_set.intersection({str(x).upper() for x in n.get("tickers") or []})
    ][:100]
    system = PROMPT.read_text(encoding="utf-8")
    user = (
        f"TODAY: {date} America/New_York\n"
        "MORNING DELTA REFRESH — ONE call. Update EVERY supplied target using "
        "overnight ticker-tagged news plus native web/X. Return captain cards "
        "with strict evidence URLs/timestamps and a fresh opportunity synthesis. "
        "Do not repeat old sentiment without checking it. size_gate must equal "
        "the supplied mechanical calendar gate.\n\n"
        f"SIZE_GATE={heat.get('size_gate')}\n"
        f"ECON={json.dumps(heat.get('econ') or [], default=str)[:9000]}\n"
        f"EARNINGS={json.dumps(heat.get('earnings') or [], default=str)[:7000]}\n"
        f"EVENT_OPTIONS_VOL_ONLY={json.dumps(heat.get('event_options') or [], default=str)[:7000]}\n"
        f"FUTURES={json.dumps(heat.get('tape') or [], default=str)[:7000]}\n"
        f"THEME_ETF_TAPE={json.dumps((heat.get('theme_tape') or [])[:31], default=str)[:10000]}\n"
        f"THEME_GICS_JOIN={json.dumps(heat.get('themes') or [], default=str)[:10000]}\n"
        f"TARGETS={json.dumps(targets, default=str)[:30000]}\n"
        f"FINVIZ_V3_NEWS={json.dumps(news, default=str)[:18000]}\n"
        f"FINVIZ_MAJOR_NEWS_TICKERS={json.dumps(heat.get('major_news_tickers') or [])}\n"
        f"POSTCLOSE_CARDS={json.dumps([base_by.get(t['industry']) for t in targets], default=str)[:22000]}"
    )
    obj = extract_json(_chat(system, user, date)) or {}
    clean, errors = validate_cards(
        obj.get("cards") or [], targets, min_coverage=0.8,
        require_x_record=True)
    if errors or len(clean) < max(3, int(0.8 * len(targets))):
        raise SystemExit(f"morning card validation failed: {errors[:12]}")
    merged = dict(base_by)
    for card in clean:
        merged[card["industry"]] = card
    refreshed_names = {c["industry"] for c in clean}
    cards = clean + [
        c for name, c in merged.items() if name not in refreshed_names
    ]
    opp_errors = opportunity_tickers_valid(obj, cards)
    if opp_errors:
        raise SystemExit("; ".join(opp_errors[:10]))
    payload = {
        "date": date,
        "phase": "morning_refresh",
        "source_baseline_generated_at": baseline.get("generated_at"),
        "generated_at": datetime.now(ET).isoformat(),
        "n_targets": baseline.get("n_targets"),
        "n_cards": len(cards),
        "n_refreshed": len(clean),
        "evidence_errors": [],
        "cards": cards,
        "size_gate": bool(heat.get("size_gate")),
        "size_gate_reason": obj.get("size_gate_reason") or (
            "high-impact Finviz calendar / mega-cap earnings"
            if heat.get("size_gate") else ""),
        "calendar_entry_scale": 0.5 if heat.get("size_gate") else 1.0,
        "parent_splits": obj.get("parent_splits") or baseline.get("parent_splits") or [],
        "opportunities": obj.get("opportunities") or baseline.get("opportunities") or [],
        "vetoes": obj.get("vetoes") or baseline.get("vetoes") or [],
        "one_paragraph": obj.get("one_paragraph") or baseline.get("one_paragraph") or "",
        "futures": heat.get("tape") or [],
        "econ": heat.get("econ") or [],
        "earnings": heat.get("earnings") or [],
    }
    final_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    final_md.write_text(render(payload), encoding="utf-8")
    (OUT / "latest_research.md").write_text(
        final_md.read_text(encoding="utf-8"), encoding="utf-8")
    qc = output_qc.qc_map_heat_research(final_md)
    if not qc.ok:
        output_qc.reject(final_path, final_md)
        raise SystemExit(f"morning research QC failed: {qc.reason}")
    print(f"[map-refresh] wrote {final_path}; {len(clean)} refreshed, {len(cards)} total")
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    date = args.date or datetime.now(ET).date().isoformat()
    run(date, force=args.force)


if __name__ == "__main__":
    main()
