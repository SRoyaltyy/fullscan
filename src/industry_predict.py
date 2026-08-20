"""Industry-level direction from *local* resources only (no web search).

Sources:
  1) Supabase `news` table (RSS / NewsAPI collectors) — titles in a date window
  2) Finviz export — membership, optional per-ticker News Title / Time / URL
     and (preferred) Daily Digest AI catalyst summary
  3) yfinance peer relative strength when available

CLI:
  python -m src.industry_predict --industry Semiconductors [--as-of YYYY-MM-DD] [--lookback-days 7]
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config, db
from .finviz_universe import members, load_export

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)


def _news_for_industry(industry: str, as_of: str | None, lookback_days: int) -> list[dict]:
    """Titles from Supabase news table whose tickers map into the industry."""
    m = members(industry, as_of)
    if m.empty:
        return []
    tickers = set(m["Ticker"].astype(str).str.upper())
    end = as_of or datetime.now(ET).date().isoformat()
    start = (datetime.fromisoformat(end) - timedelta(days=lookback_days)).date().isoformat()
    try:
        rows = db.query(
            """
            SELECT ticker, title, source, published_at, url
            FROM news
            WHERE published_at::date BETWEEN %s AND %s
              AND upper(ticker) = ANY(%s)
            ORDER BY published_at DESC
            LIMIT 400
            """,
            (start, end, list(tickers)),
        )
    except Exception as e:
        print(f"[industry_predict] news query failed: {e}")
        return []
    out = []
    for r in rows or []:
        title = (r.get("title") or "").strip()
        if not title:
            continue
        out.append({
            "source": r.get("source") or "supabase",
            "title": title,
            "url": r.get("url") or "",
            "published_at": str(r.get("published_at") or ""),
            "origin": "supabase_news",
            "ticker": (r.get("ticker") or "").upper(),
        })
    return out


def _finviz_news_for_industry(industry: str, as_of: str | None) -> list[dict]:
    """Prefer Daily Digest (AI catalyst summary) over raw News Title."""
    m = members(industry, as_of)
    if m.empty:
        return []
    rows = []
    for _, r in m.iterrows():
        digest = r.get("Daily Digest")
        title = r.get("News Title")
        text = None
        origin_field = "news_title"
        if digest is not None and not (isinstance(digest, float) and np.isnan(digest)):
            d = str(digest).strip()
            if d and d.lower() not in ("nan", "none"):
                text = d
                origin_field = "daily_digest"
        if text is None:
            if title is None or (isinstance(title, float) and np.isnan(title)):
                continue
            text = str(title).strip()
            if not text or text.lower() in ("nan", "none"):
                continue
        rows.append({
            "source": f"finviz_export:{r.get('_export', '')}",
            "title": text,
            "url": str(r.get("News URL") or "") if pd.notna(r.get("News URL")) else "",
            "published_at": str(r.get("News Time") or "") if pd.notna(r.get("News Time")) else "",
            "origin": "finviz_export",
            "origin_field": origin_field,
            "ticker": r["Ticker"],
        })
    return rows


def _peer_rs_snippet(industry: str, as_of: str | None) -> str:
    try:
        from .peer_rs import relative_strength_block
        return relative_strength_block(industry, as_of) or ""
    except Exception as e:
        return f"(peer RS unavailable: {e})"


def build_context(industry: str, as_of: str | None, lookback_days: int = 7) -> dict:
    as_of_str = as_of or datetime.now(ET).date().isoformat()
    mem = members(industry, as_of_str)
    news = _news_for_industry(industry, as_of_str, lookback_days)
    fv = _finviz_news_for_industry(industry, as_of_str)
    # Prefer Daily Digest rows when present
    digests = [x for x in fv if x.get("origin_field") == "daily_digest"]
    titles = [x for x in fv if x.get("origin_field") != "daily_digest"]
    combined = digests + news + titles
    # de-dupe by title key
    seen = set()
    unique = []
    for item in combined:
        key = (item.get("title") or "")[:120].lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return {
        "industry": industry,
        "as_of": as_of_str,
        "member_count": len(mem),
        "members_sample": mem["Ticker"].astype(str).head(25).tolist() if not mem.empty else [],
        "news_items": unique[:80],
        "digest_count": len(digests),
        "peer_rs": _peer_rs_snippet(industry, as_of_str),
    }


def to_prompt(ctx: dict) -> str:
    lines = [
        f"INDUSTRY PREDICT — {ctx['industry']} (as-of {ctx['as_of']})",
        f"members≈{ctx['member_count']} sample={', '.join(ctx['members_sample'][:12])}",
        f"Daily-Digest elevated rows: {ctx.get('digest_count', 0)}",
        "",
        "=== PEER / RS ===",
        ctx.get("peer_rs") or "(none)",
        "",
        "=== NEWS / DIGEST (prefer Daily Digest over raw titles) ===",
    ]
    for it in ctx.get("news_items") or []:
        of = it.get("origin_field") or it.get("origin") or ""
        lines.append(f"- [{it.get('ticker','')}] ({of}) {it.get('title','')[:220]}")
    lines += [
        "",
        "Task: give a short directional call for the industry over the next 1–5 sessions.",
        "Format:",
        "DIRECTION: up|down|flat",
        "BAND: flat|mild|notable|severe",
        "CONFIDENCE: 0.0-1.0",
        "CATALYSTS: short semicolon list",
        "RISKS: short semicolon list",
        "SUMMARY: 2-4 sentences",
    ]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--industry", required=True)
    ap.add_argument("--as-of", default=None)
    ap.add_argument("--lookback-days", type=int, default=7)
    args = ap.parse_args()
    ctx = build_context(args.industry, args.as_of, args.lookback_days)
    prompt = to_prompt(ctx)
    print(prompt)
    out_dir = ROOT / "01_daily" / "industry"
    out_dir.mkdir(parents=True, exist_ok=True)
    safe = args.industry.replace(" ", "_").replace("/", "-")[:60]
    path = out_dir / f"{ctx['as_of']}_{safe}_predict.md"
    path.write_text(prompt + "\n", encoding="utf-8")
    print(f"[industry_predict] wrote {path}  digests={ctx.get('digest_count')} items={len(ctx.get('news_items') or [])}")


if __name__ == "__main__":
    main()
