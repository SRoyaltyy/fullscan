"""Sector factor engine — HIT/MISS grid scoring parallel to catalyst net.

Does NOT invent HITs. Callers supply a grid of {label, status, confidence}
from LLM/search or deterministic rules. This module:
  - applies base weights + optional multipliers
  - computes Sector_Net and Lead/Lag label
  - builds prompt blocks for LLM sector analysis
  - optional deterministic ETF relative performance channel
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from .sector_taxonomy import (FINVIZ_SECTORS, HORIZONS, SECTOR_ETFS,
                              all_labels, amp_damp_table, make_search_templates,
                              net_to_label, polarity_map, taxonomy_list,
                              weights_map)


def empty_grid(sector: str) -> list[dict]:
    """Full checklist with status N/A — ready for HIT/MISS fill."""
    pol = polarity_map(sector)
    wts = weights_map(sector)
    rows = []
    for lab in taxonomy_list(sector):
        rows.append({
            "taxonomy": lab,
            "type": "positive" if pol.get(lab) == "+" else "negative",
            "status": "N/A",
            "base_weight": wts.get(lab, 5),
            "adjusted_weight": wts.get(lab, 5),
            "confidence": 0,
            "event_date": None,
            "evidence_excerpt": "",
            "source_urls": [],
        })
    return rows


def score_grid(grid: list[dict]) -> dict:
    """Sector_Net = Σ adj_w×conf/100 for +HITs − same for −HITs."""
    pos = neg = 0.0
    pos_hits = neg_hits = 0
    for c in grid:
        if c.get("status") != "HIT":
            continue
        w = float(c.get("adjusted_weight") or c.get("base_weight") or 0)
        conf = float(c.get("confidence") or 50) / 100.0
        contrib = w * conf
        if c.get("type") == "positive":
            pos += contrib
            pos_hits += 1
        else:
            neg += contrib
            neg_hits += 1
    net = pos - neg
    return {
        "positive_score": round(pos, 2),
        "negative_score": round(neg, 2),
        "net": round(net, 2),
        "label": net_to_label(net),
        "n_pos_hits": pos_hits,
        "n_neg_hits": neg_hits,
        "conviction": min(100, int(abs(net) * 2)),
    }


def build_sector_prompt(sector: str, date_str: str, etf_context: str = "") -> str:
    """System-style instructions for one sector analysis."""
    labels = taxonomy_list(sector)
    wts = weights_map(sector)
    pol = polarity_map(sector)
    checklist = "\n".join(
        f"- [{pol[l]}] w={wts[l]} | {l}" for l in labels
    )
    amp = amp_damp_table(sector)
    etf = SECTOR_ETFS.get(sector, "?")
    return f"""You are a SECTOR MACRO analyst for US equities.

SECTOR: {sector} (ETF proxy: {etf})
ANALYSIS DATE: {date_str} (America/New_York)

GOAL: Produce a HIT/MISS/N/A grid over the FIXED taxonomy below and a net
Lead/Lag call. This is MACRO sector environment — NOT a stock picker.

RULES:
- Use web_search. Prefer dated primary sources (last 14–30 days for 1d/3d;
  last 60–90 days ok for 1w/1m context).
- status HIT only with dated evidence + URL. Otherwise MISS or N/A.
- confidence 0–100. Single-name anecdotes must not dominate Healthcare/biotech
  sector HITs unless breadth is visible.
- Apply amp/damp notes when adjusting weight (adjusted_weight 0–10).
- Output Lead/Lag for horizons 1d, 3d, 1w, 1m (same grid; different conviction
  if evidence is short-lived vs structural).

TAXONOMY (exact labels — do not invent new labels):
{checklist}

AMP/DAMP:
{amp}

DETERMINISTIC ETF CONTEXT (if provided):
{etf_context or '(none)'}

OUTPUT: Return ONLY JSON:
{{
  "sector": "{sector}",
  "etf": "{etf}",
  "analysis_date": "{date_str}",
  "catalyst_grid": [
    {{
      "taxonomy": "<exact label>",
      "type": "positive|negative",
      "status": "HIT|MISS|N/A",
      "base_weight": 0,
      "adjusted_weight": 0,
      "confidence": 0,
      "event_date": "YYYY-MM-DD or null",
      "evidence_excerpt": "",
      "source_urls": []
    }}
  ],
  "horizons": {{
    "1d": {{"label": "Lead|Lag|...", "conviction": 0, "note": ""}},
    "3d": {{"label": "...", "conviction": 0, "note": ""}},
    "1w": {{"label": "...", "conviction": 0, "note": ""}},
    "1m": {{"label": "...", "conviction": 0, "note": ""}}
  }},
  "sector_stack": "3-5 sentences, dated",
  "key_assumption": "one sentence",
  "kill_switches": ["optional list"]
}}
Include EVERY taxonomy label in catalyst_grid.
"""


def etf_relative_snapshot(sector: str) -> str:
    """Deterministic channel: sector ETF vs SPY recent returns."""
    etf = SECTOR_ETFS.get(sector)
    if not etf:
        return ""
    try:
        import yfinance as yf
        end = datetime.utcnow()
        start = end - timedelta(days=40)
        data = yf.download([etf, "SPY"], start=start.date().isoformat(),
                           progress=False, threads=False)
        if data is None or data.empty:
            return f"{etf}: no yfinance data"
        # handle multi-index close
        if hasattr(data.columns, "levels"):
            px = data["Close"]
        else:
            px = data
        if etf not in px.columns or "SPY" not in px.columns:
            # single ticker path
            return f"{etf}: columns={list(px.columns)}"
        s = px[etf].dropna()
        spy = px["SPY"].dropna()
        common = s.index.intersection(spy.index)
        if len(common) < 5:
            return f"{etf}: insufficient history"
        s, spy = s.loc[common], spy.loc[common]

        def ret(series, n):
            if len(series) <= n:
                return None
            return float(series.iloc[-1] / series.iloc[-1 - n] - 1)

        lines = [f"ETF {etf} vs SPY (yfinance, through {common[-1].date()}):"]
        for n, name in ((1, "1d"), (3, "3d"), (5, "1w"), (21, "1m")):
            re = ret(s, n)
            rs = ret(spy, n)
            if re is None or rs is None:
                continue
            lines.append(
                f"  {name}: {etf} {re*100:+.2f}% | SPY {rs*100:+.2f}% | "
                f"rel { (re-rs)*100:+.2f}%"
            )
        return "\n".join(lines)
    except Exception as e:  # noqa: BLE001
        return f"{etf}: yfinance error {e}"


def merge_llm_grid(sector: str, llm_grid: list[dict] | None) -> list[dict]:
    """Force full taxonomy coverage; fill missing as MISS."""
    base = {r["taxonomy"]: r for r in empty_grid(sector)}
    if llm_grid:
        for row in llm_grid:
            lab = row.get("taxonomy")
            if lab in base:
                base[lab].update({k: row[k] for k in row if k in base[lab] or k in (
                    "status", "confidence", "adjusted_weight", "base_weight",
                    "event_date", "evidence_excerpt", "source_urls", "type")})
    # ensure types/weights
    pol = polarity_map(sector)
    wts = weights_map(sector)
    out = []
    for lab, row in base.items():
        row["type"] = "positive" if pol.get(lab) == "+" else "negative"
        row["base_weight"] = int(row.get("base_weight") or wts.get(lab, 5))
        if row.get("adjusted_weight") is None:
            row["adjusted_weight"] = row["base_weight"]
        if row.get("status") not in ("HIT", "MISS", "N/A"):
            row["status"] = "MISS"
        out.append(row)
    return out


def format_sector_markdown(sector: str, scored: dict, grid: list[dict],
                           horizons: dict | None, stack: str,
                           etf_ctx: str, key_assumption: str = "") -> str:
    etf = SECTOR_ETFS.get(sector, "")
    L = [f"# Sector environment — {sector} ({etf})", "",
         f"**Net:** {scored['net']:+.1f} → **{scored['label']}** "
         f"(conviction {scored['conviction']})", "",
         f"Positive HITs: {scored['n_pos_hits']} (score {scored['positive_score']}) | "
         f"Negative HITs: {scored['n_neg_hits']} (score {scored['negative_score']})", ""]
    if horizons:
        L.append("## Horizons")
        L.append("")
        for h in HORIZONS:
            hv = horizons.get(h) or {}
            L.append(f"- **{h}:** {hv.get('label', '?')} "
                     f"(conv {hv.get('conviction', '?')}) — {hv.get('note', '')}")
        L.append("")
    if stack:
        L.append("## Stack")
        L.append(stack)
        L.append("")
    if key_assumption:
        L.append(f"**Key assumption:** {key_assumption}")
        L.append("")
    if etf_ctx:
        L.append("## ETF channel")
        L.append("```")
        L.append(etf_ctx)
        L.append("```")
        L.append("")
    hits = [g for g in grid if g.get("status") == "HIT"]
    L.append(f"## HITs ({len(hits)})")
    L.append("")
    for g in sorted(hits, key=lambda x: -float(x.get("adjusted_weight") or 0)):
        L.append(f"- **{'+' if g.get('type')=='positive' else '-'}** "
                 f"{g['taxonomy']} | w={g.get('adjusted_weight')} "
                 f"conf={g.get('confidence')} | {g.get('event_date')} | "
                 f"{str(g.get('evidence_excerpt',''))[:120]}")
    L.append("")
    return "\n".join(L)


def search_query_bundle(sector: str, limit: int = 24) -> list[str]:
    qs = make_search_templates(sector)
    return qs[:limit]
