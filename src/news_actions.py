"""News → events → reasoned edges → compact ticker book (v3).

Adds:
  - broader event catalog (Fed/labor/SaaS/AI power/chips…)
  - bridge from news_parse theme tags when regex alone is thin
  - explicit mechanism / channel / horizon / amp_damp / mean_revert per event
  - why-these-tickers line (preferred liquid list + bucket role)
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
from .news_parse import _is_noise, _norm_title, _tag_macro, build_report as parse_build

OUT_DIR = "01_daily/news"

_COMPILED = [
    (fam, [re.compile(p, re.I) for p in fam.patterns]) for fam in EVENT_FAMILIES
]

DOVISH = re.compile(
    r"(?i)(rate\s+cut|cuts?\s+rates?|cooling\s+(fed\s+)?rate\s+hike|"
    r"hike\s+expectations?\s+(cool|fall|ease)|dovish|"
    r"odds\s+of\s+(a\s+)?(rate\s+)?hike\s+(fall|drop|cut)|"
    r"cut\s+chances\s+of\s+.{0,20}hike|fewer\s+hikes|no\s+hike|pause)"
)
HAWKISH = re.compile(
    r"(?i)(rate\s+hike|hikes?\s+rates?|hawkish|hotter\s+inflation|"
    r"higher\s+for\s+longer|reaccelerate)"
)


def match_families(title: str) -> list[EventFamily]:
    return [fam for fam, regs in _COMPILED if any(r.search(title or "") for r in regs)]


def fed_polarity(title: str) -> str:
    d, h = bool(DOVISH.search(title or "")), bool(HAWKISH.search(title or ""))
    if d and not h:
        return "dovish"
    if h and not d:
        return "hawkish"
    return "unknown"


def _adjust_side(event_key: str, side: str, title: str) -> str:
    if event_key != "fed_rate_path":
        return side
    pol = fed_polarity(title)
    if pol == "dovish":
        return "buy" if side == "sell" else "sell"
    return side


def _reason_block(fam: EventFamily, evidence: list[dict], pol: str | None) -> str:
    lines = [
        f"**Channel:** {fam.channel or 'n/a'} | **Horizon:** {fam.horizon}",
        f"**Mechanism:** {fam.mechanism or fam.amp_damp or 'n/a'}",
    ]
    if pol:
        lines.append(f"**Fed polarity inferred:** {pol} (duration sides adjusted if dovish/hawkish)")
    if fam.amp_damp:
        lines.append(f"**Amp/damp:** {fam.amp_damp}")
    if fam.mean_revert:
        lines.append(f"**Mean-revert when:** {fam.mean_revert}")
    if fam.taxonomy_labels:
        lines.append("**Taxonomy:** " + "; ".join(fam.taxonomy_labels[:3]))
    lines.append("**Evidence:**")
    for e in evidence[:4]:
        lines.append(f"  - {e.get('title')} ({e.get('source')})")
    return "\n".join(lines)


def build_from_db(hours: int = 48, limit: int = 500) -> dict:
    rows = db.recent_news(hours=hours, limit=limit)
    if not rows:
        rows = db.recent_news(hours=24 * 7, limit=limit)
    universe = load_universe()

    seen_titles: set[str] = set()
    event_hits: dict[str, dict] = {}

    def _add(fam: EventFamily, title: str, source: str, url: str, published: str):
        evidence = {
            "title": title, "source": source, "url": url, "published_at": published,
        }
        if fam.key not in event_hits:
            event_hits[fam.key] = {
                "family": fam, "evidence": [evidence], "headline_count": 1,
            }
        else:
            event_hits[fam.key]["headline_count"] += 1
            if len(event_hits[fam.key]["evidence"]) < 6:
                event_hits[fam.key]["evidence"].append(evidence)

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
            _add(fam, title, source, r.get("url") or "",
                 str(r.get("published_at") or ""))

    # Bridge: if parse themes fire but regex event missing, attach representative headlines
    # by re-scanning titles for theme keywords already captured in parse_themes linkage.
    theme_to_fams: dict[str, list[EventFamily]] = {}
    for fam in EVENT_FAMILIES:
        for th in fam.parse_themes:
            theme_to_fams.setdefault(th, []).append(fam)

    for r in rows:
        title = (r.get("title") or "").strip()
        source = r.get("source") or ""
        if not title or _is_noise(title, source):
            continue
        macros = _tag_macro(title)
        for th in macros:
            for fam in theme_to_fams.get(th, []):
                if fam.key in event_hits:
                    # already have event; still add evidence if new
                    if not any(e["title"] == title for e in event_hits[fam.key]["evidence"]):
                        _add(fam, title, source, r.get("url") or "",
                             str(r.get("published_at") or ""))
                else:
                    # only auto-bridge if title still matches family patterns OR theme is strong
                    if match_families(title) or th in ("fed_path", "labor", "geopolitics"):
                        _add(fam, title, source, r.get("url") or "",
                             str(r.get("published_at") or ""))

    edge_actions: list[dict] = []
    reasoned_events: list[dict] = []

    for key, blob in event_hits.items():
        fam: EventFamily = blob["family"]
        rep_title = blob["evidence"][0]["title"]
        pol = fed_polarity(rep_title) if key in ("fed_rate_path", "weak_labor_print") else None
        # For weak_labor, treat as dovish-leaning growth scare
        if key == "weak_labor_print":
            pol = pol or "dovish"

        skip_book = key == "fed_rate_path" and fed_polarity(rep_title) == "unknown"

        reasoned_events.append({
            "event": key,
            "headline_count": blob["headline_count"],
            "channel": fam.channel,
            "horizon": fam.horizon,
            "mechanism": fam.mechanism,
            "amp_damp": fam.amp_damp,
            "mean_revert": fam.mean_revert,
            "taxonomy": list(fam.taxonomy_labels),
            "fed_polarity": pol,
            "reasoning_md": _reason_block(fam, blob["evidence"], pol),
            "evidence": blob["evidence"],
        })

        for edge in list(fam.primary) + list(fam.substitute):
            role = "primary" if edge in fam.primary else "substitute"
            side = edge.side
            if key == "fed_rate_path":
                side = _adjust_side(key, edge.side, rep_title)
            weight = fam.base_weight * edge.weight
            if key == "fed_rate_path" and fed_polarity(rep_title) == "unknown":
                weight *= 0.2
            tickers = [] if skip_book else tickers_for_bucket(
                edge.bucket, universe=universe, max_names=6, min_mcap=1000,
            )
            why = (
                f"{role} edge on `{edge.bucket}` because: {edge.note}. "
                f"Tickers = liquid preferred set for that bucket (not full industry dump)."
            )
            edge_actions.append({
                "event": key,
                "role": role,
                "bucket": edge.bucket,
                "side": side,
                "weight": round(weight, 2),
                "tickers": tickers,
                "note": edge.note,
                "why_tickers": why,
                "channel": fam.channel,
                "horizon": fam.horizon,
                "fed_polarity": pol,
            })

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
                "event": a["event"], "side": a["side"], "weight": a["weight"],
                "bucket": a["bucket"], "channel": a["channel"],
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

    return {
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "hours": hours,
        "raw_headlines": len(rows),
        "deduped_titles": len(seen_titles),
        "unique_events": len(event_hits),
        "universe_rows": int(len(universe)),
        "reasoned_events": reasoned_events,
        "edge_actions": edge_actions,
        "ticker_actions": ranked,
    }


def to_markdown(report: dict) -> str:
    lines = [
        f"# News → Actions v3 — {report.get('generated_at', '')}",
        "",
        f"raw={report.get('raw_headlines')} unique_events={report.get('unique_events')} "
        f"universe={report.get('universe_rows')} "
        f"tickers={len(report.get('ticker_actions') or [])}",
        "",
        "## Reasoned events (framework)",
    ]
    for ev in report.get("reasoned_events") or []:
        lines.append(f"### `{ev['event']}` ×{ev['headline_count']}")
        lines.append(ev.get("reasoning_md") or "")
        lines.append("")

    lines.append("## Edges → tickers (with why)")
    for a in report.get("edge_actions") or []:
        tshow = ",".join(a.get("tickers") or []) or "(none)"
        lines.append(
            f"- **{a['event']}** | {a['role']} | `{a['bucket']}` → **{a['side']}** "
            f"w={a['weight']} | channel={a.get('channel')} horizon={a.get('horizon')}"
        )
        lines.append(f"  tickers: {tshow}")
        lines.append(f"  {a.get('why_tickers')}")
    lines.append("")
    lines.append("## Compact ticker book")
    for rec in (report.get("ticker_actions") or [])[:30]:
        evs = ",".join(sorted({e["event"] for e in rec.get("events") or []}))
        lines.append(f"- **{rec['ticker']}** {rec['side']} net={rec['net']} ({evs})")
    return "\n".join(lines) + "\n"

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--limit", type=int, default=500)
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
    print(f"[news_actions v3] unique_events={report['unique_events']} "
          f"tickers={len(report['ticker_actions'])}")
    print(to_markdown(report)[:3500])


if __name__ == "__main__":
    main()
