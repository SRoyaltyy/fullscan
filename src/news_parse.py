"""Dedicated news parsing over Supabase `news` table.

Goal: turn the piled RSS/NewsAPI rows into structured market objects that
general + sector predictors can consume — not another free-form web search.

Pipeline (deterministic first; optional LLM enrichment later):
  1. Pull last N hours from db.recent_news (or wider window)
  2. Dedupe by normalized title
  3. Tag: sector(s), macro theme, polarity hint, catalyst family
  4. Cluster near-duplicate stories
  5. Rank by market-source weight × recency × tag richness
  6. Write 01_daily/news/<date>_parsed.json + .md
  7. Expose to_markdown_for_channel1() for inject into predict

CLI:
  python -m src.news_parse [--hours 24] [--limit 200] [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, db

NEWS_DIR = "01_daily/news"

# Source reliability weights (substring match on lower(source))
SOURCE_WEIGHTS = [
    (r"bloomberg|reuters|wsj|ft\.com|financial times", 1.0),
    (r"cnbc|marketwatch|barron|economist", 0.85),
    (r"ap news|afp|nikkei|scmp", 0.75),
    (r"yahoo|investing\.com|seeking alpha|benzinga", 0.55),
    (r".*", 0.35),
]

# Sector keyword → Finviz sector name
SECTOR_RULES: list[tuple[str, str]] = [
    (r"\b(crude|wti|brent|opec|oil inventory|natural gas|xle|refiner)\b", "Energy"),
    (r"\b(semiconductor|foundry|tsmc|hbm|nvidia|hyperscaler|capex|xlk|chip)\b", "Technology"),
    (r"\b(bank|nim|yield curve|credit spread|jpmorgan|goldman|xlf|regional bank)\b", "Financial"),
    (r"\b(copper|aluminum|iron ore|lme|gold price|silver price|xlb|mining)\b", "Basic Materials"),
    (r"\b(fda|biotech|medicare|cms|drug pricing|xlv|pharma|trial)\b", "Healthcare"),
    (r"\b(ism|manufacturing|freight|defense order|ge vernova|xli|industrial)\b", "Industrials"),
    (r"\b(utility|utilities|xlu|rate case|data.?center power|grid capex)\b", "Utilities"),
    (r"\b(reit|xlre|office vacancy|cap rate|data.?center reit)\b", "Real Estate"),
    (r"\b(retail sales|consumer discretionary|xly|revpar|auto saar)\b", "Consumer Cyclical"),
    (r"\b(staples|xlp|defensive rotation|walmart|procter)\b", "Consumer Defensive"),
    (r"\b(advertising|meta platforms|alphabet|antitrust|xlc|ad spend)\b", "Communication Services"),
]

MACRO_RULES: list[tuple[str, str]] = [
    (r"\b(fed|fomc|powell|rate cut|rate hike|fedwatch)\b", "fed_path"),
    (r"\b(cpi|pce|inflation|core inflation)\b", "inflation"),
    (r"\b(payroll|nfp|jobless claims|unemployment|jobs report)\b", "labor"),
    (r"\b(vix|risk.?off|risk.?on|selloff|rally)\b", "risk_regime"),
    (r"\b(treasury|yields|10-year|10 year|real yield|tips)\b", "rates"),
    (r"\b(dollar|dxy|usd)\b", "usd"),
    (r"\b(china|pmi|pboc)\b", "china"),
    (r"\b(geopolit|sanction|strait|conflict|war)\b", "geopolitics"),
    (r"\b(earnings|guidance|eps)\b", "earnings"),
]

BULLISH_HINTS = re.compile(
    r"\b(surge|soar|jump|beat|upside|record high|raises guidance|cut rates|"
    r"easing|drawdown in inventory|inventory draw|ceasefire)\b",
    re.I,
)
BEARISH_HINTS = re.compile(
    r"\b(plunge|crash|miss|downside|layoff|hike|tightening|inventory build|"
    r"default|recession|war premium|selloff|slump)\b",
    re.I,
)


def _norm_title(t: str) -> str:
    t = (t or "").lower()
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:120]


def _source_weight(source: str) -> float:
    s = (source or "").lower()
    for pat, w in SOURCE_WEIGHTS:
        if re.search(pat, s):
            return w
    return 0.35


def _tag_sectors(title: str) -> list[str]:
    t = title or ""
    out = []
    for pat, sec in SECTOR_RULES:
        if re.search(pat, t, re.I):
            out.append(sec)
    return out


def _tag_macro(title: str) -> list[str]:
    t = title or ""
    out = []
    for pat, theme in MACRO_RULES:
        if re.search(pat, t, re.I):
            out.append(theme)
    return out


def _polarity(title: str) -> str:
    b = len(BULLISH_HINTS.findall(title or ""))
    e = len(BEARISH_HINTS.findall(title or ""))
    if b > e and b > 0:
        return "+"
    if e > b and e > 0:
        return "-"
    if b and e:
        return "mixed"
    return "neutral"


def parse_rows(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    parsed: list[dict] = []
    for r in rows:
        title = (r.get("title") or "").strip()
        if not title:
            continue
        key = _norm_title(title)
        if key in seen:
            continue
        seen.add(key)
        sectors = _tag_sectors(title)
        macros = _tag_macro(title)
        pol = _polarity(title)
        sw = _source_weight(r.get("source") or "")
        richness = 0.15 * len(sectors) + 0.1 * len(macros)
        score = sw + richness
        if pol in ("+", "-"):
            score += 0.1
        parsed.append({
            "source": r.get("source"),
            "title": title,
            "url": r.get("url"),
            "published_at": r.get("published_at"),
            "sectors": sectors,
            "macro_themes": macros,
            "polarity": pol,
            "source_weight": sw,
            "rank_score": round(score, 3),
        })
    parsed.sort(key=lambda x: -x["rank_score"])
    return parsed


def cluster_by_sector(parsed: list[dict]) -> dict[str, list[dict]]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for p in parsed:
        if not p["sectors"]:
            buckets["_untagged"].append(p)
            continue
        for s in p["sectors"]:
            buckets[s].append(p)
    return dict(buckets)


def cluster_by_macro(parsed: list[dict]) -> dict[str, list[dict]]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for p in parsed:
        if not p["macro_themes"]:
            buckets["_untagged"].append(p)
            continue
        for m in p["macro_themes"]:
            buckets[m].append(p)
    return dict(buckets)


def polarity_counts(items: list[dict]) -> dict[str, int]:
    c = {"+": 0, "-": 0, "mixed": 0, "neutral": 0}
    for x in items:
        c[x.get("polarity", "neutral")] = c.get(x.get("polarity", "neutral"), 0) + 1
    return c


def build_report(hours: int = 24, limit: int = 200) -> dict:
    rows = db.recent_news(hours=hours, limit=limit)
    # If hours filter empty, try unlimited recent
    if not rows:
        rows = db.recent_news(hours=24 * 7, limit=limit)
    parsed = parse_rows(rows)
    by_sector = cluster_by_sector(parsed)
    by_macro = cluster_by_macro(parsed)
    return {
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "hours": hours,
        "raw_count": len(rows),
        "parsed_count": len(parsed),
        "polarity_total": polarity_counts(parsed),
        "by_sector": {k: polarity_counts(v) | {"n": len(v),
                      "top": v[:8]} for k, v in by_sector.items()},
        "by_macro": {k: polarity_counts(v) | {"n": len(v),
                     "top": v[:8]} for k, v in by_macro.items()},
        "top_items": parsed[:40],
        "all_items": parsed,
    }


def to_markdown(report: dict) -> str:
    lines = [
        f"# News Parse — {report.get('generated_at', '')}",
        "",
        f"Window≈{report.get('hours')}h | raw={report.get('raw_count')} | "
        f"deduped={report.get('parsed_count')} | "
        f"polarity={report.get('polarity_total')}",
        "",
        "## By sector",
    ]
    for sec, blob in sorted(
            ((k, v) for k, v in report.get("by_sector", {}).items()
             if k != "_untagged"),
            key=lambda kv: -kv[1].get("n", 0)):
        lines.append(f"### {sec} (n={blob.get('n')}, +{blob.get('+')} "
                     f"/-{blob.get('-')} /neu={blob.get('neutral')})")
        for it in blob.get("top", [])[:6]:
            lines.append(
                f"- [{it.get('polarity')}] {it.get('title')} "
                f"({it.get('source')})")
        lines.append("")
    lines.append("## By macro theme")
    for theme, blob in sorted(
            ((k, v) for k, v in report.get("by_macro", {}).items()
             if k != "_untagged"),
            key=lambda kv: -kv[1].get("n", 0)):
        lines.append(f"### {theme} (n={blob.get('n')})")
        for it in blob.get("top", [])[:5]:
            lines.append(
                f"- [{it.get('polarity')}] {it.get('title')} "
                f"({it.get('source')})")
        lines.append("")
    lines.append("## Top ranked items")
    for it in report.get("top_items", [])[:25]:
        lines.append(
            f"- ({it.get('rank_score')}) [{it.get('polarity')}] "
            f"{it.get('title')} | {it.get('source')} | "
            f"sectors={it.get('sectors')} macros={it.get('macro_themes')}")
    return "\n".join(lines) + "\n"

def to_markdown_for_channel1(report: dict, max_per_sector: int = 4) -> str:
    """Compact block safe to inject into general/sector Channel 1."""
    lines = [
        f"[PARSED NEWS from Supabase — deduped={report.get('parsed_count')} "
        f"polarity={report.get('polarity_total')}]",
    ]
    for sec, blob in sorted(
            ((k, v) for k, v in report.get("by_sector", {}).items()
             if k != "_untagged"),
            key=lambda kv: -kv[1].get("n", 0))[:11]:
        tops = blob.get("top", [])[:max_per_sector]
        if not tops:
            continue
        lines.append(f"  {sec}: +{blob.get('+')}/-{blob.get('-')} n={blob.get('n')}")
        for it in tops:
            lines.append(f"    [{it.get('polarity')}] {it.get('title')[:120]}")
    return "\n".join(lines)


def save_report(report: dict, date_str: str) -> tuple[str, str]:
    os.makedirs(NEWS_DIR, exist_ok=True)
    jp = os.path.join(NEWS_DIR, f"{date_str}_parsed.json")
    mp = os.path.join(NEWS_DIR, f"{date_str}_parsed.md")
    with open(jp, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False, default=str)
    with open(mp, "w", encoding="utf-8") as fh:
        fh.write(to_markdown(report))
    return jp, mp


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    if not config.DATABASE_URL:
        raise SystemExit("DATABASE_URL not set — cannot reach Supabase news")

    report = build_report(hours=args.hours, limit=args.limit)
    jp, mp = save_report(report, date_str)
    print(f"[news_parse] raw={report['raw_count']} parsed={report['parsed_count']}")
    print(f"[news_parse] wrote {jp}")
    print(f"[news_parse] wrote {mp}")
    # sector coverage summary
    for sec, blob in sorted(report.get("by_sector", {}).items(),
                            key=lambda kv: -kv[1].get("n", 0)):
        if sec == "_untagged":
            continue
        print(f"  {sec}: n={blob.get('n')} +{blob.get('+')} -{blob.get('-')}")


if __name__ == "__main__":
    main()
