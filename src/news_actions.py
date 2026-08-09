"""News → event family → edges → compact buy/sell book (v2).

v1 failure: 2 Hormuz headlines × full E&P industry = 85 identical oil scores;
Fed matched a gold blog and applied hike-default sides while text was dovish.

v2:
  - one score per event family (headline evidence listed, not double-counted)
  - preferred liquid tickers (not entire industry)
  - Fed polarity: dovish inverts duration sides; hawkish keeps defaults
  - report is EVENT-FIRST, then a short ticker book
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, db
from .event_edges import EVENT_FAMILIES, EventFamily
from .finviz_universe import load_universe, tickers_for_bucket
from .news_parse import _is_noise, _norm_title

OUT_DIR = "01_daily/news"

_COMPILED = [
    (fam, [re.compile(p, re.I) for p in fam.patterns]) for fam in EVENT_FAMILIES
]

DOVISH = re.compile(
    r"(?i)(rate\s+cut|cuts?\s+rates?|cooling\s+(fed\s+)?rate\s+hike|"
    r"hike\s+expectations?\s+(cool|fall|ease)|dovish|"
    r"odds\s+of\s+(a\s+)?(rate\s+)?hike\s+(fall|drop|cut)|"
    r"fewer\s+hikes|no\s+hike|pause)"
)
HAWKISH = re.compile(
    r"(?i)(rate\s+hike|hikes?\s+rates?|hawkish|hotter\s+inflation|"
    r"higher\s+for\s+longer|reaccelerate)"
)


def match_families(title: str) -> list[EventFamily]:
    return [fam for fam, regs in _COMPILED if any(r.search(title or "") for r in regs)]


def fed_polarity(title: str) -> str:
    """dovish | hawkish | unknown"""
    d = bool(DOVISH.search(title or ""))
    h = bool(HAWKISH.search(title or ""))
    if d and not h:
        return "dovish"
    if h and not d:
        return "hawkish"
    if d and h:
        return "unknown"
    return "unknown"


def _adjust_side(event_key: str, side: str, title: str) -> str:
    if event_key != "fed_rate_path":
        return side
    pol = fed_polarity(title)
    if pol == "unknown":
        return side  # keep template default but caller may down-weight
    if pol == "dovish":
        return "buy" if side == "sell" else "sell"
    return side  # hawkish: template assumes hike pressure on duration


def build_from_db(hours: int = 48, limit: int = 300) -> dict:
    rows = db.recent_news(hours=hours, limit=limit)
    if not rows:
        rows = db.recent_news(hours=24 * 7, limit=limit)
    universe = load_universe()

    # title dedupe
    seen_titles: set[str] = set()
    # event_key -> best evidence (first / highest source weight later)
    event_hits: dict[str, dict] = {}

    for r in rows:
        title = (r.get("title") or "").strip()
        source = r.get("source") or ""
        if not title or _is_noise(title, source):
            continue
        tkey = _norm_title(title)
        if tkey in seen_titles:
            continue
        seen_titles.add(tkey)
        for fam in match_families(title):
            rec = event_hits.get(fam.key)
            evidence = {
                "title": title,
                "source": source,
                "url": r.get("url") or "",
                "published_at": str(r.get("published_at") or ""),
            }
            if rec is None:
                event_hits[fam.key] = {
                    "family": fam,
                    "evidence": [evidence],
                    "headline_count": 1,
                }
            else:
                rec["headline_count"] += 1
                if len(rec["evidence"]) < 5:
                    rec["evidence"].append(evidence)

    edge_actions: list[dict] = []
    for key, blob in event_hits.items():
        fam: EventFamily = blob["family"]
        # representative title = first evidence
        rep_title = blob["evidence"][0]["title"]
        pol = fed_polarity(rep_title) if key == "fed_rate_path" else None
        # unknown Fed polarity → skip directional book (still note the hit)
        skip_book = key == "fed_rate_path" and pol == "unknown"

        for edge in list(fam.primary) + list(fam.substitute):
            role = "primary" if edge in fam.primary else "substitute"
            side = _adjust_side(key, edge.side, rep_title)
            weight = fam.base_weight * edge.weight
            if key == "fed_rate_path" and pol == "unknown":
                weight *= 0.25
            tickers = [] if skip_book else tickers_for_bucket(
                edge.bucket, universe=universe, max_names=6, min_mcap=1000,
            )
            edge_actions.append({
                "event": key,
                "taxonomy": list(fam.taxonomy_labels),
                "role": role,
                "bucket": edge.bucket,
                "side": side,
                "weight": round(weight, 2),
                "tickers": tickers,
                "note": edge.note,
                "amp_damp": fam.amp_damp,
                "mean_revert": fam.mean_revert,
                "fed_polarity": pol,
                "headline_count": blob["headline_count"],
                "evidence": blob["evidence"],
            })

    # ticker book from deduped edges only
    book: dict[str, dict] = {}
    for a in edge_actions:
        for t in a.get("tickers") or []:
            rec = book.setdefault(t, {
                "ticker": t, "buy_score": 0.0, "sell_score": 0.0, "events": [],
            })
            if a["side"] == "buy":
                rec["buy_score"] += a["weight"]
            else:
                rec["sell_score"] += a["weight"]
            rec["events"].append({
                "event": a["event"], "role": a["role"], "side": a["side"],
                "weight": a["weight"], "bucket": a["bucket"],
            })

    ranked = []
    for t, rec in book.items():
        net = rec["buy_score"] - rec["sell_score"]
        ranked.append({
            **rec,
            "net": round(net, 2),
            "side": "buy" if net > 0 else ("sell" if net < 0 else "flat"),
            "buy_score": round(rec["buy_score"], 2),
            "sell_score": round(rec["sell_score"], 2),
        })
    ranked.sort(key=lambda x: -abs(x["net"]))

    events_summary = []
    for key, blob in event_hits.items():
        events_summary.append({
            "event": key,
            "headline_count": blob["headline_count"],
            "evidence": blob["evidence"],
            "mean_revert": blob["family"].mean_revert,
            "amp_damp": blob["family"].amp_damp,
        })

    return {
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "hours": hours,
        "raw_headlines": len(rows),
        "deduped_titles": len(seen_titles),
        "unique_events": len(event_hits),
        "universe_rows": int(len(universe)),
        "events": events_summary,
        "edge_actions": edge_actions,
        "ticker_actions": ranked,
    }


def to_markdown(report: dict) -> str:
    lines = [
        f"# News → Actions v2 — {report.get('generated_at', '')}",
        "",
        f"raw={report.get('raw_headlines')} unique_events={report.get('unique_events')} "
        f"universe={report.get('universe_rows')} "
        f"tickers_in_book={len(report.get('ticker_actions') or [])}",
        "",
        "## Events (deduped — score once even if many headlines)",
    ]
    for ev in report.get("events") or []:
        lines.append(f"### `{ev['event']}` (headlines×{ev['headline_count']})")
        for e in ev.get("evidence") or []:
            lines.append(f"- {e.get('title')} _{e.get('source')}_")
        if ev.get("mean_revert"):
            lines.append(f"  - mean-revert: {ev['mean_revert']}")
        lines.append("")

    lines.append("## Edges (bucket → side → liquid tickers)")
    for a in report.get("edge_actions") or []:
        tshow = ",".join(a.get("tickers") or []) or "(none)"
        extra = f" fed_polarity={a['fed_polarity']}" if a.get("fed_polarity") else ""
        lines.append(
            f"- **{a['event']}** {a['role']} `{a['bucket']}` → **{a['side']}** "
            f"w={a['weight']}{extra}"
        )
        lines.append(f"  tickers: {tshow}")
        lines.append(f"  note: {a.get('note')}")
    lines.append("")
    lines.append("## Compact ticker book")
    for rec in (report.get("ticker_actions") or [])[:25]:
        evs = ",".join(sorted({e["event"] for e in rec.get("events") or []}))
        lines.append(
            f"- **{rec['ticker']}** {rec['side']} net={rec['net']} "
            f"({evs})"
        )
    return "\n".join(lines) + "\n"

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--date", default=None)
    ap.add_argument("--finviz", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    if args.finviz:
        os.environ["FINVIZ_CSV"] = args.finviz
    if not config.DATABASE_URL:
        raise SystemExit("DATABASE_URL not set")

    report = build_from_db(hours=args.hours, limit=args.limit)
    os.makedirs(OUT_DIR, exist_ok=True)
    jp = os.path.join(OUT_DIR, f"{date_str}_actions.json")
    mp = os.path.join(OUT_DIR, f"{date_str}_actions.md")
    with open(jp, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False, default=str)
    with open(mp, "w", encoding="utf-8") as fh:
        fh.write(to_markdown(report))
    print(f"[news_actions v2] unique_events={report['unique_events']} "
          f"tickers={len(report['ticker_actions'])}")
    print(to_markdown(report)[:2500])


if __name__ == "__main__":
    main()
