"""News → event family → primary/substitute edges → buy/sell tickers.

Borrows catalyst_analysis ideas (taxonomy labels, weights, amp/damp) but is
event-first and cheap enough for hundreds of headlines/day.

CLI:
  python -m src.news_actions [--hours 48] [--limit 300] [--date YYYY-MM-DD]
  FINVIZ_CSV=/path/to/finviz_with_descriptions.csv python -m src.news_actions
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
from .news_parse import classify as coarse_classify  # noise filter reuse
from .news_parse import _is_noise, _norm_title

OUT_DIR = "01_daily/news"


def _compile_family(fam: EventFamily):
    return [re.compile(p, re.I) for p in fam.patterns]


_COMPILED = [(fam, _compile_family(fam)) for fam in EVENT_FAMILIES]


def match_families(title: str) -> list[EventFamily]:
    hits = []
    for fam, regs in _COMPILED:
        if any(r.search(title or "") for r in regs):
            hits.append(fam)
    return hits


def relevance_gate(title: str, source: str) -> bool:
    """Drop obvious non-US-equity noise before edge matching."""
    if _is_noise(title, source):
        return False
    # pure foreign diplomacy without market channel keywords → drop later if no family
    return True


def actions_for_headline(
    title: str,
    source: str = "",
    url: str = "",
    published_at: str = "",
    universe=None,
    max_per_edge: int = 15,
) -> list[dict]:
    if not relevance_gate(title, source):
        return []
    families = match_families(title)
    if not families:
        return []
    rows = []
    for fam in families:
        for edge in list(fam.primary) + list(fam.substitute):
            role = "primary" if edge in fam.primary else "substitute"
            tickers = tickers_for_bucket(
                edge.bucket, universe=universe, max_names=max_per_edge,
                min_mcap=200,  # skip micro junk by default
            )
            if not tickers:
                # still emit bucket-level action for debugging
                rows.append({
                    "event": fam.key,
                    "taxonomy": list(fam.taxonomy_labels),
                    "role": role,
                    "bucket": edge.bucket,
                    "side": edge.side,
                    "weight": round(fam.base_weight * edge.weight, 2),
                    "tickers": [],
                    "note": edge.note,
                    "amp_damp": fam.amp_damp,
                    "mean_revert": fam.mean_revert,
                    "title": title,
                    "source": source,
                    "url": url,
                    "published_at": published_at,
                })
                continue
            rows.append({
                "event": fam.key,
                "taxonomy": list(fam.taxonomy_labels),
                "role": role,
                "bucket": edge.bucket,
                "side": edge.side,
                "weight": round(fam.base_weight * edge.weight, 2),
                "tickers": tickers,
                "note": edge.note,
                "amp_damp": fam.amp_damp,
                "mean_revert": fam.mean_revert,
                "title": title,
                "source": source,
                "url": url,
                "published_at": published_at,
            })
    return rows


def build_from_db(hours: int = 48, limit: int = 300) -> dict:
    rows = db.recent_news(hours=hours, limit=limit)
    if not rows:
        rows = db.recent_news(hours=24 * 7, limit=limit)
    universe = load_universe()
    seen = set()
    all_actions: list[dict] = []
    matched_headlines = 0
    for r in rows:
        title = (r.get("title") or "").strip()
        key = _norm_title(title)
        if not title or key in seen:
            continue
        seen.add(key)
        acts = actions_for_headline(
            title,
            source=r.get("source") or "",
            url=r.get("url") or "",
            published_at=str(r.get("published_at") or ""),
            universe=universe,
        )
        if acts:
            matched_headlines += 1
            all_actions.extend(acts)

    # Aggregate ticker-level book
    book: dict[str, dict] = {}
    for a in all_actions:
        for t in a.get("tickers") or []:
            rec = book.setdefault(t, {
                "ticker": t,
                "buy_score": 0.0,
                "sell_score": 0.0,
                "events": [],
            })
            if a["side"] == "buy":
                rec["buy_score"] += a["weight"]
            else:
                rec["sell_score"] += a["weight"]
            rec["events"].append({
                "event": a["event"],
                "role": a["role"],
                "side": a["side"],
                "weight": a["weight"],
                "title": a["title"][:160],
            })

    ranked = []
    for t, rec in book.items():
        net = rec["buy_score"] - rec["sell_score"]
        side = "buy" if net > 0 else ("sell" if net < 0 else "flat")
        ranked.append({
            **rec,
            "net": round(net, 2),
            "side": side,
            "buy_score": round(rec["buy_score"], 2),
            "sell_score": round(rec["sell_score"], 2),
        })
    ranked.sort(key=lambda x: -abs(x["net"]))

    return {
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "hours": hours,
        "raw_headlines": len(rows),
        "deduped": len(seen),
        "matched_headlines": matched_headlines,
        "universe_rows": int(len(universe)),
        "action_rows": len(all_actions),
        "ticker_actions": ranked,
        "edge_actions": all_actions,
    }


def to_markdown(report: dict) -> str:
    lines = [
        f"# News → Actions — {report.get('generated_at', '')}",
        "",
        f"raw={report.get('raw_headlines')} matched_headlines={report.get('matched_headlines')} "
        f"universe={report.get('universe_rows')} ticker_rows={len(report.get('ticker_actions') or [])}",
        "",
        "## Ticker book (net = buy_score − sell_score)",
    ]
    for rec in (report.get("ticker_actions") or [])[:40]:
        lines.append(
            f"- **{rec['ticker']}** {rec['side']} net={rec['net']} "
            f"(buy={rec['buy_score']} sell={rec['sell_score']})"
        )
        for e in rec.get("events", [])[:3]:
            lines.append(
                f"  - [{e['side']}/{e['role']}] {e['event']} w={e['weight']}: "
                f"{e['title']}"
            )
    lines.append("")
    lines.append("## Edge-level detail")
    for a in (report.get("edge_actions") or [])[:30]:
        tshow = ",".join((a.get("tickers") or [])[:8]) or "(no ticker map)"
        lines.append(
            f"- **{a['event']}** {a['role']} `{a['bucket']}` → **{a['side']}** "
            f"w={a['weight']} | {tshow}"
        )
        lines.append(f"  _{a.get('title', '')[:140]}_")
    return "\n".join(lines) + "\n"

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--date", default=None)
    ap.add_argument("--finviz", default=None, help="Path to finviz_with_descriptions.csv")
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
    print(f"[news_actions] matched_headlines={report['matched_headlines']} "
          f"tickers={len(report['ticker_actions'])} universe={report['universe_rows']}")
    print(f"[news_actions] wrote {mp}")
    for rec in report["ticker_actions"][:15]:
        print(f"  {rec['ticker']:6} {rec['side']:4} net={rec['net']}")


if __name__ == "__main__":
    main()
