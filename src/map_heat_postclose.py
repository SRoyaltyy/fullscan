"""Post-close exhaustive captain research for the NEXT trading session.

Heavy work is deliberately outside the 05:55 pre-open critical path:
one Grok batch per GICS sector researches every SPX/RUT industry captain.
The morning refresh later updates only hot/changed cards with overnight
news, X, futures, and the fresh calendar.

CLI:
  python -m src.map_heat_postclose [--source-date YYYY-MM-DD]
                                  [--target-date YYYY-MM-DD] [--force]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, deepseek_client
from .map_heat_evidence import opportunity_tickers_valid, validate_cards
from .map_heat_research import extract_json, render
from .run_reflect import last_assistant

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "01_daily" / "map_heat"
PROMPT = ROOT / "00_grounding" / "map_heat_research_prompt.md"
ET = ZoneInfo(config.TZ)
REACTION_TICKERS = [
    "SPY", "QQQ", "IWM", "XLB", "XLC", "XLY", "XLP", "XLE", "XLF",
    "XLV", "XLI", "XLRE", "XLK", "XLU",
]


def next_weekday(date: str) -> str:
    d = datetime.fromisoformat(date).date()
    try:
        import pandas_market_calendars as mcal
        valid = mcal.get_calendar("NYSE").valid_days(
            start_date=d + timedelta(days=1),
            end_date=d + timedelta(days=10),
        )
        if len(valid):
            return valid[0].date().isoformat()
    except Exception:
        pass
    from .skip_if_good import _next_weekday
    return _next_weekday(date)


def load_heat(target_date: str) -> dict:
    p = OUT / f"{target_date}_map_heat.json"
    if not p.exists():
        raise SystemExit(f"target map heat missing: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def all_targets(heat: dict) -> list[dict]:
    out = []
    overrides = {
        str(x.get("industry") or ""): str(x.get("action") or "HEAT")
        for x in heat.get("overrides") or []
    }
    hotcold = {
        str(x.get("industry") or "")
        for x in (heat.get("hot") or []) + (heat.get("cold") or [])
    }
    for row in heat.get("industries") or []:
        if not (row.get("spx_leaders") or row.get("rut_leaders")):
            continue
        industry = str(row.get("industry") or "")
        out.append({
            "industry": industry,
            "sector": row.get("sector") or "",
            "action": overrides.get(industry, "HEAT"),
            "flagged": industry in overrides or industry in hotcold,
            "w1": row.get("w1"),
            "d1": row.get("d1"),
            "vs_parent_w1": row.get("vs_parent_w1"),
            "breadth": row.get("breadth"),
            "spx_leaders": row.get("spx_leaders") or [],
            "rut_leaders": row.get("rut_leaders") or [],
        })
    return out


def _ticker_news(heat: dict, tickers: set[str]) -> list[dict]:
    return [
        n for n in heat.get("ticker_news") or []
        if tickers.intersection({str(t).upper() for t in n.get("tickers") or []})
    ][:80]


def _sector_prompt(target_date: str, sector: str, targets: list[dict],
                   heat: dict) -> str:
    tickers = {
        str(c.get("ticker") or "").upper()
        for t in targets
        for key in ("spx_leaders", "rut_leaders")
        for c in t.get(key) or []
    }
    return (
        f"TARGET SESSION: {target_date} America/New_York\n"
        f"POST-CLOSE BASELINE BATCH: {sector}\n\n"
        "Research EVERY supplied industry and ONLY the supplied captain tickers. "
        "Use current ticker-tagged Finviz news plus live web/X research. "
        "Return one card per industry. Each captain MUST include evidence[] "
        "with source,url,published_at,fact. If no current evidence exists, set "
        "sent=none and include search_note describing queries attempted. "
        "Never infer sentiment from price alone.\n\n"
        f"TARGETS:\n{json.dumps(targets, indent=1)[:30000]}\n\n"
        f"FINVIZ_TICKER_NEWS:\n"
        f"{json.dumps(_ticker_news(heat, tickers), indent=1)[:14000]}\n"
    )


def _align_openclaw_token() -> None:
    """GitHub OPENCLAW_TOKEN is often the 64-char secret that 401s."""
    config.align_openclaw_token(force=True)


def _chat(system: str, user: str, target_date: str, stage: str,
          max_tokens: int = 24000, tools: bool = True) -> str:
    try:
        return deepseek_client.chat(
            [{"role": "system", "content": system},
             {"role": "user", "content": user}],
            model=config.MODEL_PREDICT, tools=tools, max_tokens=max_tokens,
            transcript_path=str(ROOT / "01_daily" / "_transcripts"
                                / f"{target_date}_map_postclose_{stage}.json"),
            trace_path=str(OUT / f"{target_date}_postclose_{stage}_trace.md"),
            stage_label=f"MAP POSTCLOSE {stage} {target_date}",
        )
    except Exception as e:  # noqa: BLE001 — one sector must not kill the night
        print(f"[map-postclose] {stage} chat failed: {e}")
        return ""


def market_reaction(source_date: str) -> dict:
    """Close-to-close reaction context for completed calendar surprises."""
    try:
        import yfinance as yf
        source_day = datetime.fromisoformat(source_date).date()
        import socket
        prev_to = socket.getdefaulttimeout()
        socket.setdefaulttimeout(30)
        try:
            raw = yf.download(
                REACTION_TICKERS,
                start=(source_day - timedelta(days=5)).isoformat(),
                end=(source_day + timedelta(days=1)).isoformat(),
                auto_adjust=True, progress=False, group_by="column",
                threads=False,
            )
        finally:
            socket.setdefaulttimeout(prev_to)
        close = raw["Close"] if "Close" in raw else raw
        out = {}
        for t in REACTION_TICKERS:
            s = close[t].dropna() if t in close else None
            if s is not None and len(s) >= 2:
                out[t] = round(100 * (float(s.iloc[-1]) / float(s.iloc[-2]) - 1), 3)
        return out
    except Exception as e:  # noqa: BLE001
        print(f"[map-postclose] market reaction skipped: {e}")
        return {}


def run(source_date: str, target_date: str, force: bool = False) -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    js = OUT / f"{target_date}_research_baseline.json"
    md = OUT / f"{target_date}_research_baseline.md"
    if not force and js.exists() and md.exists():
        try:
            old = json.loads(js.read_text(encoding="utf-8"))
            if len(old.get("cards") or []) >= 50:
                print(f"[map-postclose] baseline quality-present: {js}")
                return old
        except (OSError, json.JSONDecodeError):
            pass

    print(f"[map-postclose] start source research → {target_date}", flush=True)
    _align_openclaw_token()
    config.require_llm()
    heat = load_heat(target_date)
    n_ind = len(heat.get("industries") or [])
    print(f"[map-postclose] heat industries={n_ind}", flush=True)
    # Re-fetch the completed session in memory (do not overwrite its morning
    # artifact) so actual-vs-consensus becomes tomorrow's learning context.
    try:
        from .map_heat import build as build_heat
        source_close = build_heat(source_date)
        source_results = [
            e for e in source_close.get("econ") or []
            if e.get("actual") is not None
        ]
    except Exception as e:  # noqa: BLE001
        print(f"[map-postclose] completed-calendar refresh skipped: {e}")
        source_results = []
    source_reaction = market_reaction(source_date)
    targets = all_targets(heat)
    if len(targets) < 20:
        raise SystemExit(f"too few captain industries: {len(targets)}")
    rubric = PROMPT.read_text(encoding="utf-8")
    cards: list[dict] = []
    errors: list[str] = []
    for sector in sorted({str(t["sector"]) for t in targets}):
        batch = [t for t in targets if t["sector"] == sector]
        stage = f"captains_{sector.lower().replace(' ', '_')}"
        trans = ROOT / "01_daily" / "_transcripts" / f"{target_date}_map_postclose_{stage}.json"
        if not force and trans.exists():
            try:
                # last_assistant skips DSML dumps so a leaked tool-call
                # cannot burn the 7200s captain budget on a re-call.
                asst = last_assistant(str(trans))
                obj = extract_json(asst) or {}
                clean, errs = validate_cards(
                    obj.get("cards") or [], batch, min_coverage=0.75)
                if len(clean) >= int(len(batch) * 0.75 + 0.999):
                    cards.extend(clean)
                    errors.extend(f"{sector}:{e}" for e in errs)
                    print(
                        f"[map-postclose] reuse {sector}: {len(clean)}/{len(batch)} "
                        f"from {trans.name}",
                        flush=True,
                    )
                    continue
            except Exception as e:  # noqa: BLE001
                print(f"[map-postclose] reuse miss {sector}: {e}", flush=True)
        print(f"[map-postclose] Grok {sector} n={len(batch)} t0", flush=True)
        raw = _chat(rubric, _sector_prompt(target_date, sector, batch, heat),
                    target_date, stage)
        obj = extract_json(raw) or {}
        clean, errs = validate_cards(
            obj.get("cards") or [], batch, min_coverage=0.75)
        done = {c["industry"] for c in clean}
        missing = [t for t in batch if t["industry"] not in done]
        if missing:
            retry_raw = _chat(
                rubric, _sector_prompt(target_date, sector, missing, heat),
                target_date, f"captains_{sector.lower().replace(' ', '_')}_retry",
            )
            retry_obj = extract_json(retry_raw) or {}
            retry_clean, retry_errs = validate_cards(
                retry_obj.get("cards") or [], missing, min_coverage=0.75)
            clean.extend(retry_clean)
            errs.extend(f"retry:{e}" for e in retry_errs)
        # Sidecar 33914939384: Industrials closed with "I'll research…"
        # (0/24) and Consumer Cyclical cards lacked evidence URLs (0/23).
        # A no-tool JSON close recovers those sectors so coverage can
        # reach 90% and the baseline file actually gets written.
        need = int(len(batch) * 0.75 + 0.999)
        if len(clean) < need:
            still = [t for t in batch
                     if t["industry"] not in {c["industry"] for c in clean}]
            print(f"[map-postclose] {sector}: JSON-only close "
                  f"{len(clean)}/{len(batch)} still={len(still)}", flush=True)
            close_raw = _chat(
                rubric,
                _sector_prompt(target_date, sector, still, heat)
                + "\n\nReturn ONLY the JSON object with cards. "
                  "Every non-none sentiment needs evidence url+ts+fact. "
                  "No searches. No preamble.",
                target_date,
                f"captains_{sector.lower().replace(' ', '_')}_json",
                tools=False,
            )
            close_obj = extract_json(close_raw) or {}
            close_clean, close_errs = validate_cards(
                close_obj.get("cards") or [], still, min_coverage=0.75)
            clean.extend(close_clean)
            errs.extend(f"json:{e}" for e in close_errs)
        cards.extend(clean)
        errors.extend(f"{sector}:{e}" for e in errs)
        print(f"[map-postclose] {sector}: {len(clean)}/{len(batch)} valid cards", flush=True)

    required = int(len(targets) * 0.90 + 0.999)
    if len(cards) < required:
        raise SystemExit(
            f"captain baseline coverage {len(cards)}/{len(targets)} < {required}; "
            f"errors={errors[:12]}"
        )

    flagged = {
        t["industry"] for t in targets if t.get("flagged")
    }
    synth_cards = [c for c in cards if c.get("industry") in flagged]
    if len(synth_cards) < 8:
        synth_cards = cards[:30]
    user = (
        f"TARGET SESSION: {target_date}\n"
        "POST-CLOSE OPPORTUNITY BASELINE. Synthesize where sector ETFs hide "
        "nested opportunities. Every opportunity ticker must be a captain in "
        "the cards. Include no morning size_gate conclusion yet: overnight "
        "futures/calendar are refreshed pre-open.\n\n"
        f"FLAGGED_CARDS:\n{json.dumps(synth_cards, indent=1)[:30000]}\n\n"
        f"COMPLETED_CALENDAR_ACTUAL_VS_CONSENSUS:\n"
        f"{json.dumps(source_results, indent=1)[:12000]}\n\n"
        f"COMPLETED_SESSION_CLOSE_TO_CLOSE_REACTION_PCT:\n"
        f"{json.dumps(source_reaction, indent=1)}\n\n"
        f"SECTORS/THEMES:\n{json.dumps({'sectors': heat.get('sectors'), 'themes': heat.get('themes'), 'theme_tape': (heat.get('theme_tape') or [])[:30]}, indent=1)[:18000]}"
    )
    syn = extract_json(_chat(rubric, user, target_date, "synthesis")) or {}
    opp_errors = opportunity_tickers_valid(syn, cards)
    if opp_errors:
        raise SystemExit("; ".join(opp_errors[:10]))

    payload = {
        "date": target_date,
        "source_heat_date": source_date,
        "phase": "postclose_baseline",
        "generated_at": datetime.now(ET).isoformat(),
        "n_targets": len(targets),
        "n_cards": len(cards),
        "coverage": round(len(cards) / len(targets), 3),
        "validation_errors": errors,
        "source_calendar_results": source_results,
        "source_market_reaction_pct": source_reaction,
        "cards": cards,
        "size_gate": False,
        "size_gate_reason": "set by pre-open refresh",
        "parent_splits": syn.get("parent_splits") or [],
        "opportunities": syn.get("opportunities") or [],
        "vetoes": syn.get("vetoes") or [],
        "one_paragraph": syn.get("one_paragraph") or "",
    }
    js.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md.write_text(render(payload), encoding="utf-8")
    print(f"[map-postclose] wrote {js} ({len(cards)}/{len(targets)} cards)", flush=True)
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-date", default=None)
    ap.add_argument("--target-date", default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    if args.source_date:
        source = args.source_date
    else:
        from .skip_if_good import last_closed_session
        source = last_closed_session()
    target = args.target_date or next_weekday(source)
    run(source, target, force=args.force)


if __name__ == "__main__":
    main()
