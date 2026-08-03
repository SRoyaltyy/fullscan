"""Human-readable snapshot renderers. Every stage file (predict, outcome,
reflect) gets a plain-English SNAPSHOT section at the top, rendered
deterministically by the pipeline — never invented by the LLM.
"""
from __future__ import annotations

import re

from . import compute_scores

FACTOR_NAMES = {
    "B0_ASIA": "Asia overnight session",
    "B0_EUROPE": "Europe session",
    "B1_CATALYSTS": "Overnight news & catalysts",
    "B2_BONDS": "Bond yields",
    "B3_FEDPATH": "Fed policy path",
    "B4_VIX": "Volatility (VIX)",
    "B5_SENTIMENT": "Sentiment (Fear & Greed)",
    "B6_FUTURES": "US index futures",
    "B7_OIL_DOLLAR": "Oil & dollar",
}

DIRECTION_WORD = {"up": "UP 📈", "down": "DOWN 📉", "flat": "FLAT ➖"}


def _split_items(s: str | None, max_items: int = 6) -> list[str]:
    if not s:
        return []
    parts = [p.strip(" -•\t") for p in re.split(r"[;\n]", str(s))]
    return [p for p in parts if p][:max_items]


def parse_kv_block(text: str, begin: str, end: str) -> dict:
    """Parse a BEGIN..END block of 'KEY: value' lines (values may be text)."""
    m = re.search(rf"{begin}(.*?){end}", text, re.S)
    block = m.group(1) if m else ""
    out = {}
    for line in block.splitlines():
        m2 = re.match(r"\s*([A-Z0-9_]+):\s*(.+?)\s*$", line)
        if m2:
            out[m2.group(1)] = m2.group(2)
    return out


def _data_quality(ch1: dict) -> list[str]:
    bad = []
    fred = ch1.get("fred", {})
    missing = [s for s in ("DGS30", "DGS10", "DFII10", "BAMLH0A0HYM2",
                           "RRPONTSYD", "USEPUINDXD")
               if not fred.get(s, {}).get("available")]
    if missing:
        bad.append(f"FRED series missing: {', '.join(missing)}")
    v = ch1.get("vix", {})
    if not v.get("vix"):
        bad.append("VIX unavailable")
    elif v.get("ratio") is None and v.get("ratio_stale"):
        bad.append("VIX/VIX3M ratio stale (suppressed)")
    if not ch1.get("fear_greed", {}).get("available"):
        bad.append("Fear & Greed unavailable (CNN blocked + no DB fallback)")
    elif "supabase" in str(ch1["fear_greed"].get("note", "")).lower():
        bad.append("Fear & Greed from Supabase fallback (CNN direct blocked)")
    for sym in ("ES=F", "NQ=F"):
        if not ch1.get("futures", {}).get(sym, {}).get("available"):
            bad.append(f"{sym} futures unavailable")
    gs = ch1.get("global_sessions", {})
    for region in ("asia", "europe"):
        if gs.get(region, {}).get("composite_avg") is None:
            bad.append(f"{region.capitalize()} composite incomplete")
    if not ch1.get("news_24h", {}).get("available"):
        bad.append("Supabase news unavailable")
    return bad


def predict_snapshot(decision: dict, scores: dict, ch1: dict) -> str:
    """Plain-English snapshot for the top of the predict file."""
    comps = decision["components"]
    weights = compute_scores.WEIGHTS
    pos = sum(comps[k] * w for k, w in weights.items() if comps[k] * w > 0)
    neg = sum(comps[k] * w for k, w in weights.items() if comps[k] * w < 0)

    good = _split_items(scores.get("GOOD_NEWS"))
    bad_news = _split_items(scores.get("BAD_NEWS"))
    unc = (scores.get("UNCERTAINTY_LEVEL")
           or ("elevated" if decision["multiplier"] < 1.0 else "normal"))

    lines = ["> ## 📋 SNAPSHOT (human-readable)", ">", ]
    d = decision["predicted_direction"]
    lines.append(
        f"> **Prediction: {DIRECTION_WORD.get(d, d.upper())} — "
        f"{decision['predicted_magnitude_band'].upper()}** | "
        f"total score {decision['total_score']} "
        f"(uncertainty multiplier ×{decision['multiplier']}) | "
        f"confidence {decision['confidence_score']}")
    lines.append(">")
    lines.append(f"> **Good news vs bad news:** good +{round(pos, 2)} pts "
                 f"vs bad {round(neg, 2)} pts → "
                 + ("bad news outweighs good" if abs(neg) > pos
                    else "good news outweighs bad" if pos > abs(neg)
                    else "roughly balanced"))
    if good:
        lines.append(">")
        lines.append("> **Good news driving this:**")
        lines += [f"> - {g}" for g in good]
    if bad_news:
        lines.append(">")
        lines.append("> **Bad news driving this:**")
        lines += [f"> - {b}" for b in bad_news]
    v = ch1.get("vix", {}).get("vix", {})
    epu = ch1.get("fred", {}).get("USEPUINDXD", {})
    unc_bits = [f"market uncertainty: **{unc}**"]
    if v:
        unc_bits.append(f"VIX {v.get('current')}")
    if epu.get("available"):
        unc_bits.append(f"policy-uncertainty index {epu.get('current')} "
                        f"(1d {epu.get('delta_1d'):+})")
    lines.append(">")
    lines.append("> **" + " | ".join(unc_bits) + "**")
    lines.append(">")
    lines.append("> **Factor scoreboard** (score × weight = contribution):")
    for k in weights:
        s, w = comps[k], weights[k]
        c = round(s * w, 2)
        arrow = "🟢" if c > 0 else ("🔴" if c < 0 else "⚪")
        lines.append(f"> - {arrow} {FACTOR_NAMES.get(k, k)}: {s:+} × {w} = {c:+}")
    dq = _data_quality(ch1)
    lines.append(">")
    if dq:
        lines.append("> **⚠️ Data gaps this run:** " + "; ".join(dq))
    else:
        lines.append("> **✅ All key data sources fetched successfully.**")
    lines.append("")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def outcome_snapshot(entry: dict, ob: dict, claims: list[dict]) -> str:
    """Plain-English snapshot for the top of the outcome file."""
    pred_d = entry.get("predicted_direction", "?")
    pred_b = entry.get("predicted_magnitude_band", "?")
    act_d = entry.get("actual_direction", ob.get("ACTUAL_DIRECTION", "?"))
    act_b = entry.get("actual_magnitude_band", ob.get("ACTUAL_MAGNITUDE", "?"))
    pct = entry.get("actual_pct_change")
    dh, mh = entry.get("direction_hit"), entry.get("magnitude_hit")
    verified = sum(1 for c in claims if c.get("status") == "verified")

    lines = ["> ## 📋 SNAPSHOT (human-readable)", ">"]
    lines.append(
        f"> **Predicted {DIRECTION_WORD.get(pred_d, pred_d)} ({pred_b}) → "
        f"actual {DIRECTION_WORD.get(act_d, act_d)} ({act_b})"
        + (f", SPX {pct:+.2f}%" if pct is not None else "") + "**")
    lines.append(">")
    lines.append(f"> - Direction call: {'✅ CORRECT' if dh else '❌ WRONG'}")
    lines.append(f"> - Magnitude call: {'✅ CORRECT' if mh else '❌ WRONG'}")
    if ob.get("DOMINANT_DRIVER"):
        lines.append(f"> - What actually drove the day: {ob['DOMINANT_DRIVER']}")
    if entry.get("path_shape") or ob.get("PATH_SHAPE"):
        lines.append(f"> - Intraday path: "
                     f"{entry.get('path_shape') or ob.get('PATH_SHAPE')}")
    if ob.get("KEY_INTERACTION"):
        lines.append(f"> - Key interaction: {ob['KEY_INTERACTION']}")
    if ob.get("KNOWABLE_AT_9AM"):
        know = ob["KNOWABLE_AT_9AM"].lower()
        tag = ("⚠️ not foreseeable premarket" if know.startswith("no")
               else "partly foreseeable" if know.startswith("part")
               else "foreseeable premarket")
        lines.append(f"> - 9AM foreseeability: {tag}")
    if (ob.get("ATTRIBUTION_CONTESTED") or "").lower().startswith("yes"):
        lines.append("> - ⚠️ Attribution contested: major outlets disagree "
                     "on what drove the day — treat the driver story as uncertain")
    if ob.get("OUTLIER_WATCH"):
        lines.append(f"> - What didn't fit: {ob['OUTLIER_WATCH']}")
    if ob.get("MORNING_READ_VERDICT"):
        lines.append(f"> - Verdict on the morning read: "
                     f"{ob['MORNING_READ_VERDICT']}")
    lines.append(">")
    lines.append(f"> **Fact-check:** {verified}/{len(claims)} cited sources "
                 f"verified by direct fetch (paywalled sources often cannot "
                 f"be verified — flagged, not deleted).")
    lines.append("")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


CATEGORY_PLAIN = {
    "A": "Missing evidence — the data needed was not fetched or not found",
    "B": "Misweighted evidence — the facts were seen but given the wrong weight",
    "C": "Miscalibrated confidence — scores were right, uncertainty multiplier was off",
    "D": "Upstream data/tool failure — reasoning was fine, inputs were missing/stale/wrong",
    "NONE": "No error — the call was essentially right",
}


def reflect_snapshot(lb: dict, entry: dict) -> str:
    cat = lb.get("ERROR_CATEGORY", "NONE")
    lines = ["> ## 📋 SNAPSHOT (human-readable)", ">"]
    lines.append(f"> **Direction call:** {'✅ correct' if entry.get('direction_hit') else '❌ wrong'} | "
                 f"**Magnitude:** {'✅ correct' if entry.get('magnitude_hit') else '❌ wrong'}")
    lines.append(">")
    lines.append(f"> **Error category {cat}:** "
                 f"{CATEGORY_PLAIN.get(cat, cat)}")
    if lb.get("TRIGGER_PATTERN"):
        lines.append(">")
        lines.append(f"> **When this happens again:** {lb['TRIGGER_PATTERN']}")
    if lb.get("CORRECTED_BEHAVIOR"):
        lines.append(">")
        lines.append(f"> **What the engine should do instead:** "
                     f"{lb['CORRECTED_BEHAVIOR']}")
    if lb.get("FALSIFIER"):
        lines.append(">")
        lines.append(f"> **This lesson is wrong if:** {lb['FALSIFIER']}")
    if lb.get("BACKWARD_CHECK"):
        lines.append(">")
        lines.append(f"> **Would have helped on recent similar days?** "
                     f"{lb['BACKWARD_CHECK']}")
    if lb.get("LESSON_MATCH_CHECK") and \
            not lb["LESSON_MATCH_CHECK"].lower().startswith("no match"):
        lines.append(">")
        lines.append(f"> **⚠️ Lesson retrieval check:** {lb['LESSON_MATCH_CHECK']}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)
