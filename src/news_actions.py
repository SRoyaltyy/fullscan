"""News → framework score → edges → tickers (v4).

Every event runs through news_framework.score_event (keep/channel/geography/
severity/horizon/object/polarity/confidence) before any ticker book is built.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, db, output_qc, preopen
from .event_edges import EVENT_FAMILIES, EventFamily
from .finviz_universe import load_universe, tickers_for_bucket
from .news_framework import apply_interactions, score_event
from .news_parse import _is_noise, _norm_title, _tag_macro

OUT_DIR = "01_daily/news"

_COMPILED = [
    (fam, [re.compile(p, re.I) for p in fam.patterns]) for fam in EVENT_FAMILIES
]
_TITLE_TICKER = re.compile(r"\(([A-Z]{1,5})\)")


def _load_size_map() -> dict[str, str]:
    """Ticker -> size bucket from the newest universe membership file
    (Step A labels). Empty dict when no membership exists yet."""
    import glob as _glob

    paths = sorted(_glob.glob("data/universe/????-??-??_membership.csv"))
    if not paths:
        return {}
    try:
        import pandas as pd

        df = pd.read_csv(paths[-1], usecols=["Ticker", "size"])
        return dict(zip(df["Ticker"].astype(str), df["size"].astype(str)))
    except Exception:
        return {}


def match_families(title: str) -> list[EventFamily]:
    return [fam for fam, regs in _COMPILED if any(r.search(title or "") for r in regs)]


def _tickers_in_titles(titles: list[str], universe) -> list[str]:
    """Company tickers written as (EOG) / (ADBE) in the headline, if in universe."""
    have = set()
    if universe is not None and getattr(universe, "empty", True) is False:
        col = "ticker" if "ticker" in universe.columns else "Ticker"
        have = set(universe[col].astype(str).str.upper())
    out: list[str] = []
    for title in titles:
        for m in _TITLE_TICKER.findall(title or ""):
            t = m.upper()
            if have and t not in have:
                continue
            if t not in out:
                out.append(t)
    return out


def _fed_side(side: str, polarity: str) -> str:
    if polarity == "dovish":
        return "buy" if side == "sell" else "sell"
    return side


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
            if len(event_hits[fam.key]["evidence"]) < 8:
                if not any(e["title"] == title for e in event_hits[fam.key]["evidence"]):
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

    theme_to_fams: dict[str, list[EventFamily]] = {}
    for fam in EVENT_FAMILIES:
        for th in fam.parse_themes:
            theme_to_fams.setdefault(th, []).append(fam)
    for r in rows:
        title = (r.get("title") or "").strip()
        source = r.get("source") or ""
        if not title or _is_noise(title, source):
            continue
        for th in _tag_macro(title):
            for fam in theme_to_fams.get(th, []):
                _add(fam, title, source, r.get("url") or "",
                     str(r.get("published_at") or ""))

    reasoned: list[dict] = []
    edge_actions: list[dict] = []

    for key, blob in event_hits.items():
        fam: EventFamily = blob["family"]
        titles = [e["title"] for e in blob["evidence"]]
        fw = score_event(
            event_key=key,
            channel=fam.channel,
            horizon_default=fam.horizon,
            titles=titles,
            headline_count=blob["headline_count"],
            mechanism=fam.mechanism,
        )
        fw_d = fw.to_dict()

        reasoned.append({
            "event": key,
            "headline_count": blob["headline_count"],
            "framework": fw_d,
            "mechanism": fam.mechanism,
            "amp_damp": fam.amp_damp,
            "mean_revert": fam.mean_revert,
            "taxonomy": list(fam.taxonomy_labels),
            "evidence": blob["evidence"],
        })

        if fw.keep == "drop":
            continue

        rates_pol = fw.polarity if key in ("fed_rate_path", "weak_labor_print") else ""
        if key == "weak_labor_print" and rates_pol not in ("dovish", "hawkish"):
            rates_pol = "dovish"
        if key == "fed_rate_path" and rates_pol not in ("dovish", "hawkish"):
            # try evidence blob already scored; if still neutral, soft-book
            rates_pol = rates_pol if rates_pol in ("dovish", "hawkish") else "unknown"

        soft = fw.confidence < 0.35 or (
            key == "fed_rate_path" and rates_pol == "unknown"
        )

        for edge in list(fam.primary) + list(fam.substitute):
            role = "primary" if edge in fam.primary else "substitute"
            side = edge.side
            if key == "fed_rate_path" and rates_pol in ("dovish", "hawkish"):
                side = _fed_side(edge.side, rates_pol)

            weight = fam.base_weight * edge.weight * max(fw.confidence, 0.2)

            if (key == "weak_labor_print" and edge.bucket == "software_app"
                    and side == "buy" and "saas_multiple_compression" in event_hits):
                continue
            if key == "offshore_wind_cancel" and edge.bucket in (
                    "utilities_power", "utilities_renewable"):
                if "ai_power_demand" in event_hits:
                    weight *= 0.35

            tickers = []
            if not soft and fw.keep in ("keep", "conditional"):
                tickers = tickers_for_bucket(
                    edge.bucket, universe=universe, max_names=8, min_mcap=300,
                )
                for t in _tickers_in_titles(titles, universe):
                    if t not in tickers:
                        tickers.append(t)
                tickers = tickers[:10]

            edge_actions.append({
                "event": key,
                "role": role,
                "bucket": edge.bucket,
                "side": side,
                "weight": round(weight, 2),
                "tickers": tickers,
                "note": edge.note,
                "why": (
                    f"Framework keep={fw.keep} channel={fw.channel} "
                    f"object={fw.action_object}:{fw.action_object_detail} "
                    f"horizon={fw.horizon} severity={fw.severity} conf={fw.confidence}. "
                    f"Edge={role} {side} `{edge.bucket}` because {edge.note}."
                ),
                "framework_polarity": fw.polarity,
            })

    interactions = apply_interactions(reasoned)

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
                "bucket": a["bucket"],
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

    # Annotate with size labels from the universe membership (Step A labels)
    # so the book shows which suggestions are small caps vs mega caps.
    sizes = _load_size_map()
    for rec in ranked:
        rec["size"] = sizes.get(rec["ticker"], "?")

    return {
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "hours": hours,
        "raw_headlines": len(rows),
        "unique_events": len(event_hits),
        "universe_rows": int(len(universe)),
        "interactions": interactions,
        "reasoned_events": reasoned,
        "edge_actions": edge_actions,
        "ticker_actions": ranked,
    }


def merge_company_news(report: dict, date_str: str) -> dict:
    """Add Elite-export company headlines so the news box is not RSS-only."""
    try:
        from .finviz_news import actions_from_company_news, load_company_news
        extra = actions_from_company_news(load_company_news(date_str, today=date_str))
    except Exception as e:
        print(f"[news_actions] finviz company news skipped: {e}")
        return report
    if not extra:
        return report
    book = {r["ticker"]: r for r in report.get("ticker_actions") or []}
    added = 0
    for t, rec in extra.items():
        net_add = float(rec.get("net") or 0)
        if t in book:
            existing = book[t]
            if net_add > 0:
                existing["buy_score"] = float(existing.get("buy_score") or 0) + net_add
            else:
                existing["sell_score"] = float(existing.get("sell_score") or 0) + abs(net_add)
            existing["net"] = round(
                float(existing.get("buy_score") or 0)
                - float(existing.get("sell_score") or 0), 2)
            existing["side"] = (
                "buy" if existing["net"] > 0
                else ("sell" if existing["net"] < 0 else "flat")
            )
            existing.setdefault("events", []).extend(rec.get("events") or [])
        else:
            book[t] = {
                "ticker": t,
                "buy_score": net_add if net_add > 0 else 0.0,
                "sell_score": abs(net_add) if net_add < 0 else 0.0,
                "net": net_add,
                "side": rec.get("side") or "flat",
                "events": rec.get("events") or [],
            }
            added += 1
    report["ticker_actions"] = sorted(
        book.values(), key=lambda x: -abs(float(x.get("net") or 0)))
    report["finviz_company_tickers"] = len(extra)
    print(f"[news_actions] finviz company news +{added} new / {len(extra)} directional")
    return report


def to_markdown(report: dict) -> str:
    lines = [
        f"# News → Actions v4 (full framework) — {report.get('generated_at', '')}",
        "",
        f"raw={report.get('raw_headlines')} unique_events={report.get('unique_events')} "
        f"tickers={len(report.get('ticker_actions') or [])}",
        "",
    ]
    if report.get("interactions"):
        lines.append("## Cross-event interactions (catalyst-style)")
        for m in report["interactions"]:
            lines.append(f"- {m}")
        lines.append("")

    lines.append("## Events scored on full framework")
    for ev in report.get("reasoned_events") or []:
        fw = ev.get("framework") or {}
        lines.append(
            f"### `{ev['event']}` ×{ev['headline_count']} — **{str(fw.get('keep', '?')).upper()}**"
        )
        lines.append(
            f"- **US relevance:** {fw.get('us_relevance')} — {fw.get('us_relevance_why')}"
        )
        lines.append(
            f"- **Channel:** {fw.get('channel')} | **Geography:** {fw.get('geography')} | "
            f"**Severity:** {fw.get('severity')} | **Horizon:** {fw.get('horizon')}"
        )
        lines.append(
            f"- **Action object:** {fw.get('action_object')} — {fw.get('action_object_detail')}"
        )
        lines.append(
            f"- **Polarity:** {fw.get('polarity')} ({fw.get('polarity_why')}) | "
            f"**Confidence:** {fw.get('confidence')}"
        )
        if fw.get("notes"):
            lines.append(f"- **Notes:** {'; '.join(fw['notes'])}")
        if ev.get("mechanism"):
            lines.append(f"- **Mechanism:** {ev['mechanism']}")
        if ev.get("mean_revert"):
            lines.append(f"- **Mean-revert:** {ev['mean_revert']}")
        lines.append("- **Evidence:**")
        for e in (ev.get("evidence") or [])[:4]:
            lines.append(f"  - {e.get('title')}")
        lines.append("")

    lines.append("## Edges (only keep/conditional with confidence)")
    for a in report.get("edge_actions") or []:
        if not a.get("tickers") and a.get("weight", 0) < 1:
            continue
        tshow = ",".join(a.get("tickers") or []) or "(no tickers — soft/unknown)"
        lines.append(
            f"- **{a['event']}** {a['role']} `{a['bucket']}` → **{a['side']}** "
            f"w={a['weight']}"
        )
        lines.append(f"  {tshow}")
        lines.append(f"  _{a.get('why')}_")
    lines.append("")
    lines.append("## Compact ticker book")
    for rec in (report.get("ticker_actions") or [])[:35]:
        evs = ",".join(sorted({e["event"] for e in rec.get("events") or []}))
        lines.append(f"- **{rec['ticker']}** ({rec.get('size', '?')}) "
                     f"{rec['side']} net={rec['net']} ({evs})")
    return "\n".join(lines) + "\n"

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--date", default=None)
    ap.add_argument("--finviz", default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    jp = os.path.join(OUT_DIR, f"{date_str}_actions.json")
    existing = output_qc.qc_news_actions(jp)
    if existing.ok and not args.force:
        print(f"[news_actions] {date_str}: skip, quality-ok already on disk")
        return
    if preopen.past_predict_cutoff() and not args.force:
        if existing.ok:
            print(f"[news_actions] {date_str}: past 09:25 ET, keeping quality-ok")
            return
        print(f"[news_actions] {date_str}: past 09:25 ET — not writing a late copy")
        return
    if args.finviz:
        os.environ["FINVIZ_CSV"] = args.finviz
    if not config.DATABASE_URL:
        print("[news_actions] DATABASE_URL not set — using files / empty news")

    report = build_from_db(hours=args.hours, limit=args.limit)
    report = merge_company_news(report, date_str)
    try:
        from .judge_apply import load_or_parse
        j = load_or_parse(date_str)
        tilts = j.get("tickers") or {}
        if tilts:
            book = {r["ticker"]: r for r in report.get("ticker_actions") or []}
            for t, net_add in tilts.items():
                rec = book.setdefault(t, {
                    "ticker": t, "buy_score": 0.0, "sell_score": 0.0,
                    "events": [], "net": 0.0, "side": "flat",
                })
                if net_add > 0:
                    rec["buy_score"] = rec.get("buy_score", 0) + net_add
                else:
                    rec["sell_score"] = rec.get("sell_score", 0) + abs(net_add)
                rec["net"] = round(rec["buy_score"] - rec["sell_score"], 2)
                rec["side"] = "buy" if rec["net"] > 0 else ("sell" if rec["net"] < 0 else "flat")
                rec.setdefault("events", []).append({
                    "event": "news_judge", "side": rec["side"],
                    "weight": round(abs(net_add), 2), "bucket": "judge",
                })
            report["ticker_actions"] = sorted(
                book.values(), key=lambda x: -abs(x.get("net") or 0)
            )
            report["judge_tickers"] = tilts
            print(f"[news_actions] elevated {len(tilts)} judge tickers")
    except Exception as e:
        print(f"[news_actions] judge apply skipped: {e}")
    os.makedirs(OUT_DIR, exist_ok=True)
    jp = os.path.join(OUT_DIR, f"{date_str}_actions.json")
    mp = os.path.join(OUT_DIR, f"{date_str}_actions.md")
    with open(jp, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False, default=str)
    with open(mp, "w", encoding="utf-8") as fh:
        fh.write(to_markdown(report))
    print(f"[news_actions v4] events={report['unique_events']} "
          f"tickers={len(report['ticker_actions'])}")
    print(to_markdown(report)[:4000])
    qc = output_qc.qc_news_actions(jp)
    if not qc.ok:
        print(f"[news_actions] QC FAIL ({qc.reason}) — throwing out")
        output_qc.reject(jp, mp)
        raise SystemExit("news actions produced no quality-ok file")


if __name__ == "__main__":
    main()
