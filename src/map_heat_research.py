"""Captain research + opportunity synthesis on top of map_heat tables.

Job 1: Grok researches top-2 SPX/RUT captains per flagged industry.
Job 2: one synthesis pass — where 11 ETFs miss the money.

Outputs:
  01_daily/map_heat/<date>_research.json
  01_daily/map_heat/<date>_research.md
  01_daily/map_heat/latest_research.md

CLI:
  python -m src.map_heat_research [--date YYYY-MM-DD] [--force]
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, deepseek_client, output_qc, preopen

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
OUT_DIR = ROOT / "01_daily" / "map_heat"
PROMPT_PATH = ROOT / "00_grounding" / "map_heat_research_prompt.md"
HEAT_DIR = OUT_DIR
MAX_INDUSTRIES = 12
MAX_TOKENS = 12000


def _heat_paths(date: str) -> tuple[Path, Path]:
    return (
        HEAT_DIR / f"{date}_map_heat.json",
        HEAT_DIR / f"{date}_research.json",
    )


def load_heat(date: str) -> dict:
    p, _ = _heat_paths(date)
    if not p.exists():
        raise SystemExit(f"map_heat missing: {p} — run src.map_heat first")
    return json.loads(p.read_text(encoding="utf-8"))


def _captains_ok(row: dict) -> bool:
    return bool(row.get("spx_leaders") or row.get("rut_leaders"))


def select_targets(heat: dict) -> list[dict]:
    """OVERRIDE/SPLIT first, then hot/cold. Need at least one captain."""
    by_name: dict[str, dict] = {}
    for row in heat.get("industries") or []:
        by_name[row.get("industry") or ""] = row

    picked: list[dict] = []
    seen: set[str] = set()

    def add(name: str, action: str) -> None:
        if not name or name in seen or len(picked) >= MAX_INDUSTRIES:
            return
        src = by_name.get(name) or {}
        leaders_spx = src.get("spx_leaders") or []
        leaders_rut = src.get("rut_leaders") or []
        # fall back to hot/cold payload captains
        if not leaders_spx and not leaders_rut:
            return
        seen.add(name)
        picked.append({
            "industry": name,
            "sector": src.get("sector") or "",
            "action": action,
            "w1": src.get("w1"),
            "d1": src.get("d1"),
            "vs_parent_w1": src.get("vs_parent_w1"),
            "spx_leaders": leaders_spx,
            "rut_leaders": leaders_rut,
        })

    for o in heat.get("overrides") or []:
        add(o.get("industry") or "", o.get("action") or "OVERRIDE")
    for row in (heat.get("hot") or []) + (heat.get("cold") or []):
        add(row.get("industry") or "", "HEAT")
        # hot/cold already carry captains
        if row.get("industry") in seen:
            continue
        if not _captains_ok(row):
            continue
        if len(picked) >= MAX_INDUSTRIES:
            break
        seen.add(row["industry"])
        picked.append({
            "industry": row["industry"],
            "sector": row.get("sector") or "",
            "action": "HEAT",
            "w1": row.get("w1"),
            "d1": row.get("d1"),
            "vs_parent_w1": row.get("vs_parent_w1"),
            "spx_leaders": row.get("spx_leaders") or [],
            "rut_leaders": row.get("rut_leaders") or [],
        })
    return picked


def extract_json(text: str) -> dict | None:
    if not text or not str(text).strip():
        return None
    candidates: list[str] = []
    for m in re.finditer(r"```json\s*(.*?)```", text, re.S):
        candidates.append(m.group(1))
    m_open = re.search(r"```json\s*(.*)$", text, re.S)
    if m_open:
        candidates.append(m_open.group(1))
    i, j = text.find("{"), text.rfind("}")
    if i != -1 and j > i:
        candidates.append(text[i:j + 1])
    for blob in candidates:
        raw = re.sub(r",\s*([}\]])", r"\1", blob.strip())
        try:
            obj = json.loads(raw)
        except ValueError:
            try:
                obj, _ = json.JSONDecoder().raw_decode(raw)
            except ValueError:
                continue
        if isinstance(obj, dict) and (obj.get("cards") or obj.get("opportunities")
                                      or obj.get("parent_splits")):
            return obj
    return None


def _compact_board(heat: dict, targets: list[dict]) -> str:
    tape = heat.get("tape") or []
    tape_s = ", ".join(
        f"{t.get('label')} {t.get('change')}" for t in tape[:12]
    )
    econ = heat.get("econ") or []
    earns = heat.get("earnings") or []
    lines = [
        f"DATE {heat.get('date')}",
        f"SIZE_GATE {heat.get('size_gate')}",
        f"TAPE {tape_s}",
        "ECON " + "; ".join(
            f"{e.get('event')} cons {e.get('forecast')}" for e in econ[:8]
        ),
        "EARN " + "; ".join(
            f"{e.get('session')} {e.get('ticker')}" for e in earns[:8]
        ),
        "SECTOR_RS " + "; ".join(
            f"{s.get('sector')} w1={s.get('w1')}" for s in (heat.get("sectors") or [])
        ),
        "",
        "TARGETS (research ONLY these captains):",
    ]
    for t in targets:
        def fmt(caps, idx):
            bits = []
            for c in caps or []:
                bits.append(
                    f"{c.get('ticker')} d1={c.get('d1')} w1={c.get('w1')} "
                    f"kw={c.get('sent')}"
                )
            return f"{idx}[" + (", ".join(bits) or "—") + "]"
        lines.append(
            f"- {t['action']} {t['industry']} ({t['sector']}) "
            f"w1={t.get('w1')} vs_parent={t.get('vs_parent_w1')} "
            f"{fmt(t.get('spx_leaders'), 'SPX')} "
            f"{fmt(t.get('rut_leaders'), 'RUT')}"
        )
    themes = heat.get("themes") or []
    if themes:
        lines.append("THEME_JOIN:")
        for th in themes:
            for st in th.get("subthemes") or []:
                flag = "AGREE" if st.get("agree") else "DIVERGE"
                lines.append(
                    f"- {th.get('theme')} / {st.get('label')}: "
                    f"w1={st.get('w1')} vs_parent={st.get('parent_w1')} {flag}"
                )
    return "\n".join(lines)


def _chat(system: str, user: str, date: str, stage: str) -> str:
    return deepseek_client.chat(
        [{"role": "system", "content": system},
         {"role": "user", "content": user}],
        model=config.MODEL_PREDICT,
        tools=True,
        max_tokens=MAX_TOKENS,
        transcript_path=os.path.join(
            "01_daily/_transcripts", f"{date}_map_heat_{stage}.json"),
        trace_path=str(OUT_DIR / f"{date}_{stage}_trace.md"),
        stage_label=f"MAP HEAT {stage} {date}",
    )


def run_jobs(heat: dict, targets: list[dict], date: str) -> dict:
    rubric = PROMPT_PATH.read_text(encoding="utf-8")
    board = _compact_board(heat, targets)
    user1 = (
        f"TODAY: {date} America/New_York\n\n"
        "JOB 1 — captain cards. JSON first. Research the listed tickers.\n\n"
        f"{board}\n"
    )
    raw1 = _chat(rubric, user1, date, "captains")
    cards_obj = extract_json(raw1) or {}
    cards = cards_obj.get("cards") or []
    if len(cards) < 3:
        raise SystemExit(
            f"captain research too thin ({len(cards)} cards) — not writing a stub"
        )

    user2 = (
        f"TODAY: {date} America/New_York\n\n"
        "JOB 2 — opportunity synthesis. JSON first. "
        "Use the board AND the captain cards. Do not repeat the card dump.\n\n"
        f"{board}\n\n"
        "CAPTAIN_CARDS:\n"
        f"{json.dumps(cards, indent=2)[:8000]}\n"
    )
    raw2 = _chat(rubric, user2, date, "opportunity")
    syn = extract_json(raw2) or {}
    if not (syn.get("opportunities") or syn.get("parent_splits") or syn.get("vetoes")):
        raise SystemExit("opportunity synthesis empty — not writing a stub")

    return {
        "date": date,
        "generated_at": datetime.now(ET).isoformat(),
        "n_targets": len(targets),
        "n_cards": len(cards),
        "cards": cards,
        "size_gate": bool(syn.get("size_gate", heat.get("size_gate"))),
        "size_gate_reason": syn.get("size_gate_reason") or "",
        "parent_splits": syn.get("parent_splits") or [],
        "opportunities": syn.get("opportunities") or [],
        "vetoes": syn.get("vetoes") or [],
        "one_paragraph": syn.get("one_paragraph") or "",
    }


def render(payload: dict) -> str:
    lines = [
        f"# MAP HEAT RESEARCH — {payload['date']}",
        "",
        f"{payload.get('n_cards')} captain cards · "
        f"size_gate={payload.get('size_gate')} "
        f"{payload.get('size_gate_reason') or ''}",
        "",
        "## CAPTAIN CARDS",
    ]
    for c in payload.get("cards") or []:
        caps = c.get("captains") or []
        cap_s = ", ".join(
            f"{x.get('ticker')} {x.get('index')} {x.get('sent')}" for x in caps
        ) or "—"
        lines.append(
            f"- **{c.get('action')} {c.get('industry')}** ({c.get('sector')}) "
            f"dir={c.get('subsector_dir')} conv={c.get('conviction')}"
        )
        lines.append(f"  captains: {cap_s}")
        if c.get("one_line"):
            lines.append(f"  {c['one_line']}")
        if c.get("do_not"):
            lines.append(f"  do_not: {c['do_not']}")
    lines += ["", "## PARENT SPLITS"]
    for s in payload.get("parent_splits") or []:
        lines.append(
            f"- **{s.get('sector')}** long {s.get('long')} · "
            f"avoid {s.get('avoid')} — {s.get('why')}"
        )
    if not payload.get("parent_splits"):
        lines.append("_none_")
    lines += ["", "## OPPORTUNITIES"]
    for o in payload.get("opportunities") or []:
        ticks = ",".join(o.get("tickers") or [])
        lines.append(
            f"- **{o.get('side')} {o.get('id')}** [{ticks}] "
            f"{o.get('horizon')}: {o.get('why')}"
        )
    lines += ["", "## VETOES"]
    for v in payload.get("vetoes") or []:
        lines.append(f"- {v.get('what')} — {v.get('why')}")
    if payload.get("one_paragraph"):
        lines += ["", "## SYNTHESIS", payload["one_paragraph"]]
    lines += ["", "CAPTAIN_CARDS_OK", "OPPORTUNITY_OK", ""]
    return "\n".join(lines)


def write(payload: dict) -> tuple[Path, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    date = payload["date"]
    js = OUT_DIR / f"{date}_research.json"
    md = OUT_DIR / f"{date}_research.md"
    js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md.write_text(render(payload), encoding="utf-8")
    (OUT_DIR / "latest_research.md").write_text(
        md.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"[map_heat_research] wrote {md}")
    print(f"[map_heat_research] wrote {js}")
    return md, js


def already_good(date: str) -> bool:
    md = OUT_DIR / f"{date}_research.md"
    js = OUT_DIR / f"{date}_research.json"
    if not md.exists() or not js.exists():
        return False
    text = md.read_text(encoding="utf-8")
    if "CAPTAIN_CARDS_OK" not in text or "OPPORTUNITY_OK" not in text:
        return False
    try:
        data = json.loads(js.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return len(data.get("cards") or []) >= 3


def inject_block(date_str: str | None = None, sector: str | None = None,
                 max_chars: int = 3500) -> str:
    """Predictor inject. If sector is set, keep that sector's cards + global splits."""
    date_str = date_str or datetime.now(ET).date().isoformat()
    js = OUT_DIR / f"{date_str}_research.json"
    if not js.exists():
        return ""
    try:
        data = json.loads(js.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    if data.get("phase") != "morning_refresh":
        return ""
    cards = data.get("cards") or []
    if sector:
        cards = [c for c in cards if (c.get("sector") or "") == sector]
        if not cards and not data.get("size_gate"):
            return ""
    lines = [
        "=== MAP HEAT RESEARCH (captains + nested overrides; do not average into the parent ETF) ===",
        f"size_gate={data.get('size_gate')} {data.get('size_gate_reason') or ''}",
    ]
    for c in cards[:10]:
        caps = ", ".join(
            f"{x.get('ticker')}:{x.get('sent')}" for x in (c.get("captains") or [])
        )
        lines.append(
            f"- {c.get('action')} {c.get('industry')} dir={c.get('subsector_dir')} "
            f"conv={c.get('conviction')} captains={caps} | {c.get('one_line') or ''}"
        )
    for s in data.get("parent_splits") or []:
        if sector and s.get("sector") != sector:
            continue
        lines.append(
            f"- SPLIT {s.get('sector')}: long {s.get('long')} avoid {s.get('avoid')} "
            f"({s.get('why')})"
        )
    for v in data.get("vetoes") or []:
        lines.append(f"- VETO {v.get('what')}: {v.get('why')}")
    if data.get("one_paragraph") and not sector:
        lines.append(data["one_paragraph"][:500])
    lines.append("=== END MAP HEAT RESEARCH ===")
    body = "\n".join(lines)
    if len(body) > max_chars:
        body = body[:max_chars] + "\n...(truncated)"
    return body + "\n"


def ticker_boosts(date: str) -> tuple[dict[str, float], dict[str, float]]:
    """(ticker → score, industry → score) for stock_book s_heat."""
    js = OUT_DIR / f"{date}_research.json"
    if not js.exists():
        return {}, {}
    try:
        data = json.loads(js.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}, {}
    if (data.get("phase") != "morning_refresh"
            or len(data.get("cards") or []) < 20
            or data.get("evidence_errors")):
        print("[map-heat] research not strict morning_refresh — no book boosts")
        return {}, {}
    tboost: dict[str, float] = {}
    iboost: dict[str, float] = {}
    for c in data.get("cards") or []:
        direction = str(c.get("subsector_dir") or "").lower()
        action = str(c.get("action") or "")
        conv = str(c.get("conviction") or "low")
        scale = {"high": 1.0, "medium": 0.7, "low": 0.4}.get(conv, 0.5)
        sign = 1.0 if direction == "up" else -1.0 if direction == "down" else 0.0
        if not sign:
            continue
        # Nested overrides move the whole (liquid) industry a bit; splits
        # only move captains — otherwise gold clones every miner.
        if action == "OVERRIDE":
            iboost[c.get("industry") or ""] = round(0.12 * sign * scale, 3)
        for cap in c.get("captains") or []:
            sent = str(cap.get("sent") or "none")
            if sent == "neg" and sign > 0:
                continue
            if sent == "pos" and sign < 0:
                continue
            mag = 0.28 if sent in ("pos", "neg") else 0.16
            tboost[str(cap.get("ticker") or "").upper()] = round(mag * sign * scale, 3)
    for o in data.get("opportunities") or []:
        side = str(o.get("side") or "").lower()
        sign = 1.0 if "long" in side else -1.0 if "short" in side or "avoid" in side else 0.0
        if not sign:
            continue
        for t in o.get("tickers") or []:
            tboost.setdefault(str(t).upper(), round(0.20 * sign, 3))
    tboost.pop("", None)
    iboost.pop("", None)
    return tboost, iboost


def decision_gate(date: str, decision: dict, sector: str | None = None) -> dict:
    """Deterministically enforce the calendar gate after LLM parsing.

    Prompt advice is not enforcement. High-impact macro caps broad general
    and sector magnitude; mega-cap earnings additionally caps Technology.
    """
    js = OUT_DIR / f"{date}_research.json"
    try:
        data = json.loads(js.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return decision
    if data.get("phase") != "morning_refresh":
        return decision
    out = dict(decision)

    # Deterministic sector-RS confirmation/veto. A sector directional call
    # cannot remain directional when BOTH Finviz 1d and 1w tape disagree.
    if sector:
        heat_path = OUT_DIR / f"{date}_map_heat.json"
        try:
            heat = json.loads(heat_path.read_text(encoding="utf-8"))
            row = next(
                (x for x in heat.get("sectors") or []
                 if x.get("sector") == sector), None)
        except (OSError, json.JSONDecodeError):
            row = None
        if row:
            d1, w1 = row.get("d1"), row.get("w1")
            direction = str(out.get("predicted_direction") or "")
            disagrees = (
                direction == "up" and d1 is not None and w1 is not None
                and d1 < 0 and w1 < 0
            ) or (
                direction == "down" and d1 is not None and w1 is not None
                and d1 > 0 and w1 > 0
            )
            if disagrees:
                out["predicted_direction"] = "flat"
                out["predicted_magnitude_band"] = "flat"
                out["confidence_score"] = min(
                    float(out.get("confidence_score") or 0.5), 0.55)
                out["sector_rs_veto_applied"] = True
                out["sector_rs_tape"] = {"d1": d1, "w1": w1}

    if not data.get("macro_gate") and not data.get("size_gate"):
        return out
    econ = data.get("econ") or []
    earnings = data.get("earnings") or []
    macro_gate = bool(data.get("macro_gate")) or any(
        int(e.get("importance") or 0) >= 2 for e in econ)
    tech_gate = any(str(e.get("ticker") or "").upper() in {
        "AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "GOOGL", "META", "TSLA",
        "AVGO", "CRM", "ORCL",
    } for e in earnings)
    # Mega-cap earnings cap Technology (and general). They do NOT half the
    # whole book — that's calendar_entry_scale / macro_gate.
    applies = macro_gate or sector is None or (sector == "Technology" and tech_gate)
    if not applies:
        return out
    if str(out.get("predicted_magnitude_band")) in ("notable", "severe"):
        out["predicted_magnitude_band"] = "mild"
    out["confidence_score"] = min(float(out.get("confidence_score") or 0.5), 0.65)
    out["calendar_size_gate_applied"] = True
    out["calendar_size_gate_reason"] = data.get("size_gate_reason") or "high-impact calendar"
    return out


def calendar_entry_scale(date: str) -> float:
    """Broad new-entry cash scale. 0.5 only on high-impact MACRO prints."""
    js = OUT_DIR / f"{date}_research.json"
    try:
        data = json.loads(js.read_text(encoding="utf-8"))
        if data.get("phase") != "morning_refresh":
            return 1.0
        if data.get("macro_gate"):
            return 0.5
        # Ignore legacy calendar_entry_scale=0.5 that mixed in AMC earnings.
        return 1.0
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return 1.0


def earnings_entry_tickers(date: str) -> list[str]:
    """Mega-cap names reporting today — half those names only, not the book."""
    js = OUT_DIR / f"{date}_research.json"
    try:
        data = json.loads(js.read_text(encoding="utf-8"))
        if data.get("phase") != "morning_refresh":
            return []
        raw = data.get("earnings_entry_tickers") or [
            str(e.get("ticker") or "").upper()
            for e in (data.get("earnings") or []) if e.get("ticker")
        ]
        return [t for t in (str(x).upper() for x in raw) if t]
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    date = args.date or datetime.now(ET).date().isoformat()
    preopen.refuse_if_late("map_heat_research", force=args.force)
    if already_good(date) and not args.force:
        print(f"[map_heat_research] skip-if-good {date}")
        return
    config.require_llm()
    heat = load_heat(date)
    targets = select_targets(heat)
    if len(targets) < 3:
        raise SystemExit(f"not enough captain targets ({len(targets)})")
    print(f"[map_heat_research] {len(targets)} industries → Grok")
    payload = run_jobs(heat, targets, date)
    md, _js = write(payload)
    qc = output_qc.qc_map_heat_research(md)
    if not qc.ok:
        output_qc.reject(str(md), str(OUT_DIR / f"{date}_research.json"))
        raise SystemExit(f"research QC fail: {qc.reason}")
    print(render(payload))


if __name__ == "__main__":
    main()
