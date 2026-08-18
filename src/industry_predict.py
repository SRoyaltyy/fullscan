"""Industry-level direction from *local* resources only (no web search).

Sources:
  1) Supabase `news` table (RSS / NewsAPI collectors) — titles in a date window
  2) Finviz export — membership, optional per-ticker News Title / Time / URL

Live:
  python -m src.industry_predict --industry "Semiconductors"
  python -m src.industry_predict --industry "Oil & Gas E&P" --lookback-days 7

As-of / backtest (only news published before as-of; grade next week if prices exist):
  python -m src.industry_predict --industry "Semiconductors" --as-of 2026-08-01 --backtest

List industries:
  python -m src.industry_predict --list
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config, db
from .industry_map import (
    list_industries,
    match_patterns,
    members,
    resolve_industry,
    title_matches,
)

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "01_daily" / "industry"
ET = ZoneInfo(config.TZ)

BULLISH = re.compile(
    r"(?i)\b(surge|soar|jump|rally|record high|beats? estimates|"
    r"raises? guidance|upgrade|approve[sd]?|win(s|ning)? contract|"
    r"rate cut|easing|inventory draw|ceasefire|shortage|sold out|"
    r"breakthrough|bullish)\b"
)
BEARISH = re.compile(
    r"(?i)\b(plunge|crash|slump|selloff|misses? estimates|cuts? guidance|"
    r"downgrade|reject|ban[sn]?|tariff|shortage of demand|"
    r"rate hike|tightening|inventory build|recession|default|layoff|"
    r"investigation|fraud|bearish|glut)\b"
)


def _polarity(title: str) -> str:
    b = len(BULLISH.findall(title or ""))
    e = len(BEARISH.findall(title or ""))
    if b > e:
        return "up"
    if e > b:
        return "down"
    return "neutral"


def _normalize_date_str(s: str) -> str:
    """Accept YYYY-MM-DD; fix common mangling (GitHub form: 2026+06-01)."""
    s = str(s).strip()
    if not s:
        raise ValueError("empty date")
    s = s.replace("+", "-").replace("/", "-").replace(".", "-")
    s = re.sub(r"\s+", "", s)
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if not m:
        raise ValueError(
            f"invalid as-of date {s!r}; use YYYY-MM-DD (e.g. 2026-06-01)"
        )
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y:04d}-{mo:02d}-{d:02d}"


def _parse_day(s: str) -> pd.Timestamp:
    return pd.Timestamp(_normalize_date_str(s)).normalize()


def _window(as_of: pd.Timestamp, lookback_days: int) -> tuple[str, str]:
    end = as_of
    start = as_of - pd.Timedelta(days=lookback_days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _finviz_news_for_industry(industry: str, as_of: str | None) -> list[dict]:
    m = members(industry, as_of)
    if m.empty:
        return []
    rows = []
    for _, r in m.iterrows():
        title = r.get("News Title")
        if title is None or (isinstance(title, float) and np.isnan(title)):
            continue
        title = str(title).strip()
        if not title or title.lower() in ("nan", "none"):
            continue
        rows.append({
            "source": f"finviz_export:{r.get('_export', '')}",
            "title": title,
            "url": str(r.get("News URL") or "") if pd.notna(r.get("News URL")) else "",
            "published_at": str(r.get("News Time") or "") if pd.notna(r.get("News Time")) else "",
            "origin": "finviz_export",
            "ticker": r["Ticker"],
        })
    return rows


def _supabase_news(start: str, end: str, limit: int) -> list[dict]:
    rows = db.news_between(start, end, limit=limit)
    for r in rows:
        r["origin"] = "supabase_news"
    return rows


def _filter_industry(rows: list[dict], industry: str, mem: pd.DataFrame) -> list[dict]:
    pats = match_patterns(industry, mem)
    out = []
    for r in rows:
        title = r.get("title") or ""
        if r.get("origin") == "finviz_export":
            r = dict(r)
            r["polarity"] = _polarity(title)
            out.append(r)
            continue
        if title_matches(title, pats):
            r = dict(r)
            r["polarity"] = _polarity(title)
            out.append(r)
    seen = set()
    deduped = []
    for r in out:
        k = re.sub(r"\s+", " ", (r.get("title") or "").lower()).strip()[:160]
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    return deduped


def _direction_from_hits(hits: list[dict]) -> tuple[str, float, dict]:
    if not hits:
        return "neutral", 0.0, {"up": 0, "down": 0, "neutral": 0}
    counts = {"up": 0, "down": 0, "neutral": 0}
    for h in hits:
        counts[h.get("polarity", "neutral")] = counts.get(h.get("polarity", "neutral"), 0) + 1
    up, down = counts["up"], counts["down"]
    total = up + down
    if total == 0:
        return "neutral", 0.15, counts
    score = (up - down) / total
    if score >= 0.25:
        direction = "up"
    elif score <= -0.25:
        direction = "down"
    else:
        direction = "neutral"
    conf = min(0.85, 0.35 + 0.1 * total + 0.2 * abs(score))
    return direction, round(conf, 2), counts


def _industry_forward_return(industry: str, as_of: str, horizon_days: int = 5) -> dict | None:
    try:
        from . import price_store as ps
    except Exception:
        return None
    store = ps._load_store()
    if store is None or not len(store):
        return None
    store = store.copy()
    store["date"] = pd.to_datetime(store["date"]).dt.normalize()
    mem = members(industry, as_of)
    tickers = set(mem["Ticker"].astype(str).str.upper())
    if not tickers:
        return None
    sub = store[store["ticker"].astype(str).str.upper().isin(tickers)].copy()
    if sub.empty:
        return None
    as_of_ts = pd.Timestamp(as_of)
    rets = []
    used = 0
    for t, g in sub.groupby("ticker"):
        g = g.sort_values("date")
        g = g[g["date"] <= as_of_ts]
        if g.empty:
            continue
        entry_px = float(g.iloc[-1]["close"])
        entry_dt = g.iloc[-1]["date"]
        fut = store[(store["ticker"] == t) & (store["date"] > entry_dt)].sort_values("date")
        if len(fut) < 1:
            continue
        exit_row = fut.iloc[min(horizon_days, len(fut)) - 1]
        exit_px = float(exit_row["close"])
        if entry_px > 0:
            rets.append(exit_px / entry_px - 1.0)
            used += 1
    if not rets:
        return None
    arr = np.array(rets, dtype=float)
    return {
        "horizon_sessions": horizon_days,
        "n_tickers": int(used),
        "median_return": float(np.median(arr)),
        "mean_return": float(np.mean(arr)),
        "pct_up": float((arr > 0).mean()),
        "pct_ge_4pct": float((arr >= 0.04).mean()),
    }


def analyze(
    industry_query: str,
    as_of: str | None = None,
    lookback_days: int = 7,
    news_limit: int = 2500,
    backtest: bool = False,
    horizon_days: int = 5,
) -> dict:
    as_of_ts = _parse_day(as_of) if as_of else pd.Timestamp(datetime.now(ET).date())
    as_of_str = as_of_ts.strftime("%Y-%m-%d")
    industry = resolve_industry(industry_query, as_of_str)
    mem = members(industry, as_of_str)
    start, end = _window(as_of_ts, lookback_days)

    sb = _supabase_news(start, end, limit=news_limit)
    fv = _finviz_news_for_industry(industry, as_of_str)
    fv_kept = []
    for r in fv:
        pub = (r.get("published_at") or "").strip()
        if not pub:
            fv_kept.append(r)
            continue
        d = str(pub)[:10]
        if re.match(r"\d{4}-\d{2}-\d{2}", d):
            if d < end:
                fv_kept.append(r)
        else:
            fv_kept.append(r)

    hits = _filter_industry(sb + fv_kept, industry, mem)
    direction, conf, counts = _direction_from_hits(hits)

    report = {
        "industry": industry,
        "as_of": as_of_str,
        "lookback_days": lookback_days,
        "window": {"start": start, "end_exclusive": end},
        "data_policy": "supabase_news + finviz_export only (no web search)",
        "members_n": int(len(mem)),
        "members_sample": mem["Ticker"].head(12).tolist(),
        "export_used": str(mem["_export"].iloc[0]) if len(mem) and "_export" in mem.columns else None,
        "supabase_rows_in_window": len(sb),
        "finviz_news_rows": len(fv_kept),
        "hits_n": len(hits),
        "polarity_counts": counts,
        "direction": direction,
        "confidence": conf,
        "sources": [
            {
                "title": h.get("title"),
                "source": h.get("source"),
                "published_at": h.get("published_at"),
                "url": h.get("url"),
                "origin": h.get("origin"),
                "polarity": h.get("polarity"),
                "ticker": h.get("ticker"),
            }
            for h in hits[:80]
        ],
        "sufficiency": (
            "ok" if len(hits) >= 5
            else "thin" if len(hits) >= 1
            else "insufficient — no matched headlines in window"
        ),
        "generated_at": datetime.now(ET).isoformat(),
    }

    if backtest:
        fwd = _industry_forward_return(industry, as_of_str, horizon_days=horizon_days)
        report["backtest"] = {
            "label": f"forward {horizon_days} sessions after {as_of_str}",
            "result": fwd,
            "hit_direction": None,
        }
        if fwd and direction in ("up", "down"):
            med = fwd["median_return"]
            report["backtest"]["hit_direction"] = bool(med > 0) if direction == "up" else bool(med < 0)

    return report


def render_md(report: dict) -> str:
    lines = [
        f"# Industry predict — **{report['industry']}**",
        "",
        f"- **As-of:** {report['as_of']} (news window `{report['window']['start']}` → `{report['window']['end_exclusive']}` exclusive)",
        f"- **Lookback:** {report['lookback_days']} days",
        f"- **Data policy:** {report['data_policy']}",
        f"- **Members:** {report['members_n']} (sample: {', '.join(report.get('members_sample') or [])})",
        f"- **Export:** {report.get('export_used')}",
        f"- **Supabase rows in window:** {report['supabase_rows_in_window']} · Finviz news fields: {report['finviz_news_rows']}",
        f"- **Matched headlines:** {report['hits_n']} · sufficiency: **{report['sufficiency']}**",
        "",
        "## Direction",
        "",
        f"- **Prediction:** **{report['direction'].upper()}** (confidence {report['confidence']})",
        f"- Polarity counts: {report['polarity_counts']}",
        "",
        "## Sources (matched)",
        "",
        "| Published | Origin | Source | Polarity | Title |",
        "|-----------|--------|--------|----------|-------|",
    ]
    for s in report.get("sources") or []:
        pub = (s.get("published_at") or "")[:19]
        title = (s.get("title") or "").replace("|", "/")[:120]
        src = (s.get("source") or "")[:40]
        lines.append(f"| {pub} | {s.get('origin')} | {src} | {s.get('polarity')} | {title} |")
    if not report.get("sources"):
        lines.append("| — | — | — | — | *(none)* |")

    bt = report.get("backtest")
    if bt:
        lines += ["", "## Backtest (forward industry returns)", ""]
        lines.append(f"- {bt.get('label')}")
        res = bt.get("result")
        if not res:
            lines.append("- **No price store data** to grade forward returns.")
        else:
            lines.append(
                f"- n={res['n_tickers']} · median={res['median_return']:.2%} · "
                f"mean={res['mean_return']:.2%} · pct_up={res['pct_up']:.1%} · "
                f"pct≥4%={res['pct_ge_4pct']:.1%}"
            )
            lines.append(f"- direction hit vs median: **{bt.get('hit_direction')}**")

    lines += [
        "",
        "## Note",
        "",
        "Resource sufficiency probe: keyword match on titles only, no web search. "
        "Thin/insufficient means Supabase+Finviz do not support a confident industry call for this window.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Industry predict from Supabase+Finviz only")
    ap.add_argument("--industry", default=None, help="Finviz industry name or substring")
    ap.add_argument("--list", action="store_true", help="List Finviz industries and exit")
    ap.add_argument("--as-of", default=None, help="YYYY-MM-DD — only news published before this date")
    ap.add_argument("--lookback-days", type=int, default=7)
    ap.add_argument("--limit", type=int, default=2500, help="Max Supabase rows in window")
    ap.add_argument("--backtest", action="store_true", help="Grade next-week industry median return")
    ap.add_argument("--horizon-days", type=int, default=5)
    args = ap.parse_args()

    if args.list:
        inds = list_industries(args.as_of)
        print(f"n={len(inds)}")
        for i in inds:
            print(i)
        return

    if not args.industry:
        raise SystemExit("pass --industry 'Semiconductors' (or --list)")

    report = analyze(
        args.industry,
        as_of=(args.as_of.strip() if args.as_of else None),
        lookback_days=args.lookback_days,
        news_limit=args.limit,
        backtest=args.backtest,
        horizon_days=args.horizon_days,
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^\w\-]+", "_", report["industry"])[:60]
    stem = f"{report['as_of']}_{safe}"
    json_path = OUT_DIR / f"{stem}.json"
    md_path = OUT_DIR / f"{stem}.md"
    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_md(report), encoding="utf-8")
    print(render_md(report))
    print(f"\n[industry] wrote {md_path}")
    print(f"[industry] wrote {json_path}")


if __name__ == "__main__":
    main()
