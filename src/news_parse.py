"""Dedicated news parsing over Supabase `news` table — v2.

v1 failure mode (observed): 200 Yahoo/Cramer/single-name earnings rows,
~192 neutral, "earnings" macro inflated, geopolitics false positives.
Useless for general B1 or sector S1.

v2 goals:
  1. DROP noise (Cramer, clickbait, pure "Q2 Earnings Call Highlights")
  2. Classify: noise | single_name | sector_relevant | macro_relevant
  3. Only macro/sector_relevant enter "usable" sets for predictors
  4. single_name kept in a side bucket (earnings calendar), not sector spine
  5. Polarity is secondary; presence of a real macro event matters more

CLI:
  python -m src.news_parse [--hours 48] [--limit 300] [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, db, output_qc, preopen

NEWS_DIR = "01_daily/news"

# ── noise: drop entirely from usable sets ─────────────────────────────
NOISE_TITLE = re.compile(
    r"(?i)("
    r"jim cramer|cramer (believes|reveals|shares|says|didn)|"
    r"earnings call highlights$|"
    r"here'?s how much the stock could move|"
    r"turn a \$\d+ profit trading|"
    r"is it the smarter .{0,40} to buy|"
    r"what comes next\.?$|"
    r"here'?s what to know$|"
    r"stock of the day|top stocks to|stocks to watch|"
    r"should you buy|is it too late to buy"
    r")"
)

NOISE_SOURCE = re.compile(
    r"(?i)(seeking.?alpha|benzinga|motley|fool\.com|tipranks|zacks)"
)

# Single-name / ticker-ish patterns (not automatically noise — side bucket)
TICKER_HINT = re.compile(
    r"\b([A-Z]{1,5})\b(?:\s+Stock)?\s+(?:Q[1-4]|Earnings|EPS|Revenue)|"
    r"\b(?:NASDAQ|NYSE|NYSEARCA):[A-Z]{1,5}\b|"
    r"\b(Nvidia|Apple|Microsoft|Tesla|Meta Platforms|Alphabet|Google|"
    r"Amazon|Broadcom|AMD|Palantir|Walmart|Home Depot|Vertex)\b",
    re.I,
)

SINGLE_EARNINGS = re.compile(
    r"(?i)(\bQ[1-4]\b.{0,30}\bearnings\b|\bearnings\b.{0,20}\b(call|report|beat|miss)\b)"
)

# ── source tiers ──────────────────────────────────────────────────────
SOURCE_WEIGHTS = [
    (r"bloomberg|reuters|wsj|ft\.com|financial times", 1.0),
    (r"cnbc|marketwatch|barron|economist|aljazeera", 0.9),
    (r"ap news|afp|nikkei|scmp|fed\b|ecb", 0.8),
    (r"google_macro|google_business|newsapi", 0.65),
    (r"yahoo", 0.35),  # demoted — mostly retail single-name
    (r".*", 0.4),
]

# Macro themes — tight patterns (no bare "earnings", no bare "war")
MACRO_RULES: list[tuple[str, str]] = [
    (r"(?i)\b(fomc|federal reserve|powell|fed chair|fed officials?|"
     r"rate (cut|hike|hold)|fed funds|dot plot|fedwatch)\b", "fed_path"),
    (r"(?i)\b(cpi|pce|core inflation|inflation (data|print|report))\b", "inflation"),
    (r"(?i)\b(non-?farm|payrolls|nfp|jobless claims|unemployment rate|"
     r"jobs report)\b", "labor"),
    (r"(?i)\b(10-?year (yield|treasury)|real yield|tips yield|"
     r"treasury yields?)\b", "rates"),
    (r"(?i)\b(dxy|us dollar index|dollar (index|retreats|surges|weakens))\b", "usd"),
    (r"(?i)\b(china pmi|pboc|china (property|stimulus|exports?))\b", "china"),
    (r"(?i)\b(strait of hormuz|opec\+?|sanctions on|missile|"
     r"geopolitical|military strike)\b", "geopolitics"),
    (r"(?i)\b(vix\b|risk-?off|risk-?on|flight to safety)\b", "risk_regime"),
    (r"(?i)\b(ism manufacturing|ism services|durable goods orders)\b", "activity"),
]

# Sector — require substance, not just a ticker name when possible
SECTOR_RULES: list[tuple[str, str]] = [
    (r"(?i)\b(crude oil|wti\b|brent\b|opec\+?|oil inventories|eia crude|"
     r"natural gas price|henry hub)\b", "Energy"),
    (r"(?i)\b(semiconductor(?!s?:)|foundry|tsmc|hbm\b|hyperscaler capex|"
     r"chip export control|ai chip demand)\b", "Technology"),
    (r"(?i)\b(yield curve|net interest margin|\bnim\b|credit spreads?|"
     r"regional banks?|bank (earnings|lending)|commercial real estate bank)\b",
     "Financial"),
    (r"(?i)\b(copper price|lme copper|iron ore|aluminum price|gold price|"
     r"silver price|critical minerals)\b", "Basic Materials"),
    (r"(?i)\b(medicare advantage|cms rate|drug pricing|ira drug|"
     r"biotech etf|xbi\b|fda (approval|panel|crl))\b", "Healthcare"),
    (r"(?i)\b(ism manufacturing|durable goods|freight rates|truck tonnage|"
     r"defense (budget|orders)|grid (capex|transformer))\b", "Industrials"),
    (r"(?i)\b(utility (sector|stocks)|rate case|data center (power|electricity)|"
     r"power demand utilities)\b", "Utilities"),
    (r"(?i)\b(reit (sector|index)|office vacancy|cap rates?|"
     r"data center reit)\b", "Real Estate"),
    (r"(?i)\b(retail sales|consumer discretionary|card spend|revpar|"
     r"auto saar)\b", "Consumer Cyclical"),
    (r"(?i)\b(consumer staples|defensive (sector|rotation)|staples sector)\b",
     "Consumer Defensive"),
    (r"(?i)\b(digital ad spend|ad revenue|app store (fee|antitrust)|"
     r"search monopoly)\b", "Communication Services"),
]

# Mega-cap names that can still be sector_relevant if + sector language
MEGA = re.compile(
    r"(?i)\b(nvidia|microsoft|apple|amazon|meta|alphabet|google|"
    r"jpmorgan|jpmorgan chase|exxon|chevron)\b"
)

BULLISH = re.compile(
    r"(?i)\b(surge|soar|jump|rally|record high|beats? estimates|"
    r"raises? guidance|rate cut|easing|inventory draw|ceasefire)\b"
)
BEARISH = re.compile(
    r"(?i)\b(plunge|crash|slump|selloff|misses? estimates|cuts? guidance|"
    r"rate hike|tightening|inventory build|recession|default|layoff)\b"
)


def _norm_title(t: str) -> str:
    t = (t or "").lower()
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()[:120]


def _source_weight(source: str) -> float:
    s = (source or "").lower()
    for pat, w in SOURCE_WEIGHTS:
        if re.search(pat, s):
            return w
    return 0.4


def _tag_macro(title: str) -> list[str]:
    return [theme for pat, theme in MACRO_RULES if re.search(pat, title or "")]


def _tag_sectors(title: str) -> list[str]:
    return [sec for pat, sec in SECTOR_RULES if re.search(pat, title or "")]


def _polarity(title: str) -> str:
    b = len(BULLISH.findall(title or ""))
    e = len(BEARISH.findall(title or ""))
    if b > e and b:
        return "+"
    if e > b and e:
        return "-"
    if b and e:
        return "mixed"
    return "neutral"


def _is_noise(title: str, source: str) -> bool:
    if NOISE_TITLE.search(title or ""):
        return True
    if NOISE_SOURCE.search(source or ""):
        return True
    return False


def classify(title: str, source: str) -> str:
    """noise | single_name | sector_relevant | macro_relevant"""
    if _is_noise(title, source):
        return "noise"
    macros = _tag_macro(title)
    sectors = _tag_sectors(title)
    if macros:
        return "macro_relevant"
    if sectors:
        return "sector_relevant"
    # pure single-name earnings / ticker story
    if SINGLE_EARNINGS.search(title or "") or TICKER_HINT.search(title or ""):
        return "single_name"
    # yahoo with no tags → usually noise
    if re.search(r"(?i)yahoo", source or "") and not macros and not sectors:
        return "noise"
    return "noise"


def parse_rows(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for r in rows:
        title = (r.get("title") or "").strip()
        if not title:
            continue
        key = _norm_title(title)
        if key in seen:
            continue
        seen.add(key)
        source = r.get("source") or ""
        kind = classify(title, source)
        macros = _tag_macro(title)
        sectors = _tag_sectors(title)
        # mega-cap + no sector rule: still single_name unless macro
        if kind == "noise" and MEGA.search(title) and macros:
            kind = "macro_relevant"
        pol = _polarity(title)
        sw = _source_weight(source)
        usable = kind in ("macro_relevant", "sector_relevant")
        rank = sw
        if usable:
            rank += 0.4
        rank += 0.15 * len(macros) + 0.1 * len(sectors)
        if pol in ("+", "-"):
            rank += 0.05
        if kind == "single_name":
            rank -= 0.2
        if kind == "noise":
            rank -= 0.5
        out.append({
            "source": source,
            "title": title,
            "url": r.get("url"),
            "published_at": r.get("published_at"),
            "class": kind,
            "usable": usable,
            "sectors": sectors,
            "macro_themes": macros,
            "polarity": pol,
            "source_weight": sw,
            "rank_score": round(rank, 3),
        })
    out.sort(key=lambda x: -x["rank_score"])
    return out


def _bucket(items: list[dict], key: str) -> dict[str, list[dict]]:
    b: dict[str, list[dict]] = defaultdict(list)
    for it in items:
        vals = it.get(key) or []
        if not vals:
            b["_none"].append(it)
            continue
        for v in vals:
            b[v].append(it)
    return dict(b)


def polarity_counts(items: list[dict]) -> dict[str, int]:
    c = {"+": 0, "-": 0, "mixed": 0, "neutral": 0}
    for x in items:
        c[x.get("polarity", "neutral")] = c.get(x.get("polarity", "neutral"), 0) + 1
    return c


def build_report(hours: int = 48, limit: int = 300) -> dict:
    rows = db.recent_news(hours=hours, limit=limit)
    if not rows:
        rows = db.recent_news(hours=24 * 7, limit=limit)
    parsed = parse_rows(rows)
    usable = [p for p in parsed if p["usable"]]
    single = [p for p in parsed if p["class"] == "single_name"]
    noise = [p for p in parsed if p["class"] == "noise"]
    by_sector = _bucket(usable, "sectors")
    by_macro = _bucket(usable, "macro_themes")

    def pack(bucket: dict[str, list[dict]]) -> dict:
        return {
            k: polarity_counts(v) | {"n": len(v), "top": v[:8]}
            for k, v in bucket.items() if k != "_none"
        }

    return {
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "hours": hours,
        "raw_count": len(rows),
        "parsed_count": len(parsed),
        "usable_count": len(usable),
        "single_name_count": len(single),
        "noise_count": len(noise),
        "polarity_usable": polarity_counts(usable),
        "by_macro_usable": pack(by_macro),
        "by_sector_usable": pack(by_sector),
        "usable_top": usable[:30],
        "single_name_top": single[:15],
        "noise_sample": noise[:10],
        "all_items": parsed,
    }


def to_markdown(report: dict) -> str:
    lines = [
        f"# News Parse v2 — {report.get('generated_at', '')}",
        "",
        f"Window≈{report.get('hours')}h | raw={report.get('raw_count')} | "
        f"usable={report.get('usable_count')} | "
        f"single_name={report.get('single_name_count')} | "
        f"noise_dropped={report.get('noise_count')}",
        f"usable polarity={report.get('polarity_usable')}",
        "",
        "## USABLE — macro themes (for general B1)",
    ]
    for theme, blob in sorted(
            report.get("by_macro_usable", {}).items(),
            key=lambda kv: -kv[1].get("n", 0)):
        lines.append(f"### {theme} (n={blob.get('n')})")
        for it in blob.get("top", [])[:6]:
            lines.append(
                f"- [{it.get('polarity')}] {it.get('title')} "
                f"({it.get('source')})")
        lines.append("")
    lines.append("## USABLE — sectors (for sector S1)")
    for sec, blob in sorted(
            report.get("by_sector_usable", {}).items(),
            key=lambda kv: -kv[1].get("n", 0)):
        lines.append(f"### {sec} (n={blob.get('n')})")
        for it in blob.get("top", [])[:6]:
            lines.append(
                f"- [{it.get('polarity')}] {it.get('title')} "
                f"({it.get('source')})")
        lines.append("")
    lines.append("## Single-name side bucket (not sector spine)")
    for it in report.get("single_name_top", [])[:12]:
        lines.append(f"- {it.get('title')} ({it.get('source')})")
    lines.append("")
    lines.append("## Noise sample (dropped)")
    for it in report.get("noise_sample", [])[:8]:
        lines.append(f"- {it.get('title')} ({it.get('source')})")
    return "\n".join(lines) + "\n"

def to_markdown_for_channel1(report: dict) -> str:
    """Only usable macro + sector lines — safe for predict inject."""
    lines = [
        f"[PARSED NEWS v2 usable={report.get('usable_count')} "
        f"noise_dropped={report.get('noise_count')} "
        f"single_name={report.get('single_name_count')}]",
    ]
    mac = report.get("by_macro_usable") or {}
    if not mac:
        lines.append("  macro: (none tagged in window)")
    for theme, blob in sorted(mac.items(), key=lambda kv: -kv[1].get("n", 0)):
        lines.append(f"  MACRO {theme} n={blob.get('n')}")
        for it in blob.get("top", [])[:3]:
            lines.append(f"    [{it.get('polarity')}] {it.get('title')[:130]}")
    sec = report.get("by_sector_usable") or {}
    for name, blob in sorted(sec.items(), key=lambda kv: -kv[1].get("n", 0))[:8]:
        lines.append(f"  SECTOR {name} n={blob.get('n')}")
        for it in blob.get("top", [])[:2]:
            lines.append(f"    [{it.get('polarity')}] {it.get('title')[:130]}")
    return "\n".join(lines)


def save_report(report: dict, date_str: str) -> tuple[str, str]:
    os.makedirs(NEWS_DIR, exist_ok=True)
    # strip all_items from md-facing size if huge — keep full in json
    jp = os.path.join(NEWS_DIR, f"{date_str}_parsed.json")
    mp = os.path.join(NEWS_DIR, f"{date_str}_parsed.md")
    with open(jp, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False, default=str)
    with open(mp, "w", encoding="utf-8") as fh:
        fh.write(to_markdown(report))
    return jp, mp


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    jp = os.path.join(NEWS_DIR, f"{date_str}_parsed.json")
    existing = output_qc.qc_news_parse(jp)
    if existing.ok and not args.force:
        print(f"[news_parse] {date_str}: skip, quality-ok already on disk")
        return
    if preopen.past_predict_cutoff() and not args.force:
        if existing.ok:
            print(f"[news_parse] {date_str}: past 09:25 ET, keeping quality-ok parse")
            return
        print(f"[news_parse] {date_str}: past 09:25 ET — not writing a late parse")
        return
    if not config.DATABASE_URL:
        print("[news_parse] DATABASE_URL not set — writing from files only")
    report = build_report(hours=args.hours, limit=args.limit)
    jp, mp = save_report(report, date_str)
    qc = output_qc.qc_news_parse(jp)
    if not qc.ok:
        print(f"[news_parse] QC FAIL ({qc.reason}) — throwing out")
        output_qc.reject(jp, mp)
        raise SystemExit("news parse produced no quality-ok file")
    print(
        f"[news_parse v2] raw={report['raw_count']} "
        f"usable={report['usable_count']} "
        f"single={report['single_name_count']} "
        f"noise={report['noise_count']}"
    )
    print(f"[news_parse] {jp}")
    print(f"[news_parse] {mp}")
    print("--- channel1 preview ---")
    print(to_markdown_for_channel1(report))


if __name__ == "__main__":
    main()
