"""Harvest every on-disk news article and ask Lane: sector bull / bear.

Smoke test for free intensive inference. One article → one hop.
Each row is watermarked with lane (API family) + model id.

Does not touch flatten / Webull / factor-mine live books.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import time
from collections import Counter
from pathlib import Path

from . import lane_route as lane

NEWS_DIR = Path("01_daily/news")
EVENTS_DIR = Path("01_daily/events")
EXPORTS_DIR = Path("data/exports")
GROK_DIR = Path("data/grok_automations")
OUTBOX_DIR = Path("02_lessons/lane/outbox_sectors")

FINVIZ_SECTORS = (
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities",
)

SYSTEM = (
    "You map one news article to US equity market sectors. "
    "Reply with one JSON object only. No markdown."
)

PROMPT = """One news article. Which Finviz market sectors does it make bullish or bearish?

Rules:
- Use ONLY these sector names: {sectors}
- A sector may appear in bullish or bearish, not both.
- Skip sectors the article does not actually move.
- If the article is noise / single-name earnings / tabloid, return empty lists and say so in notes.
- No tickers. No second-order essays.

Article source_file: {source_file}
known_at: {known_at}
Title: {title}
Body: {body}

STRICT JSON:
{{\"bullish\":[],\"bearish\":[],\"notes\":\"\"}}
"""

_NOISE_TITLE = re.compile(
    r"(?i)(jim cramer|earnings call highlights$|stock of the day|"
    r"should you buy|is it too late to buy|top stocks to)"
)


def _norm(title: str) -> str:
    t = re.sub(r"[^a-z0-9\s]", " ", (title or "").lower())
    return re.sub(r"\s+", " ", t).strip()[:160]


def _add(bag: dict[str, dict], title: str, body: str, source_file: str, known_at: str = "") -> None:
    title = (title or "").strip()
    body = (body or "").strip()
    if not title and not body:
        return
    key = _norm(title or body)
    if not key or key in bag:
        return
    if _NOISE_TITLE.search(title):
        return
    bag[key] = {
        "title": title[:300],
        "body": (body or title)[:1200],
        "source_file": source_file,
        "known_at": known_at,
    }


def _load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def harvest(date: str) -> list[dict]:
    bag: dict[str, dict] = {}
    export = EXPORTS_DIR / f"finviz_{date}.csv"
    if export.is_file():
        with export.open(newline="", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                t = (row.get("News Title") or "").strip()
                d = (row.get("Daily Digest") or "").strip()
                known = (row.get("News Time") or "").strip()
                if t:
                    _add(bag, t, d, str(export), known)
                if d and _norm(d) != _norm(t):
                    _add(bag, d, t, str(export) + "#digest", known)
    parsed = _load_json(NEWS_DIR / f"{date}_parsed.json")
    if isinstance(parsed, dict):
        for it in parsed.get("all_items") or []:
            if isinstance(it, dict):
                _add(bag, str(it.get("title") or ""), str(it.get("source") or ""),
                     f"01_daily/news/{date}_parsed.json", str(it.get("published_at") or ""))
    ev = _load_json(EVENTS_DIR / f"{date}_events.json")
    if isinstance(ev, dict):
        for it in ev.get("events") or []:
            if isinstance(it, dict):
                _add(bag, str(it.get("title") or it.get("event") or it.get("name") or ""),
                     str(it.get("summary") or it.get("note") or it.get("category") or ""),
                     f"01_daily/events/{date}_events.json",
                     str(it.get("when") or it.get("timing") or ""))
    digest = _load_json(NEWS_DIR / f"{date}_finviz_digest.json")
    if isinstance(digest, dict):
        for it in digest.get("top_signal") or []:
            if isinstance(it, dict):
                _add(bag, str(it.get("news_title") or ""), str(it.get("digest") or ""),
                     f"01_daily/news/{date}_finviz_digest.json", "")
        for it in digest.get("index_digests") or []:
            if isinstance(it, dict):
                _add(bag, str(it.get("digest") or it.get("title") or ""),
                     str(it.get("source") or "index"),
                     f"01_daily/news/{date}_finviz_digest.json#index", "")
    actions = _load_json(NEWS_DIR / f"{date}_actions.json")
    if isinstance(actions, dict):
        for it in actions.get("events") or actions.get("items") or []:
            if isinstance(it, dict):
                _add(bag, str(it.get("title") or it.get("headline") or ""),
                     str(it.get("reason") or it.get("note") or ""),
                     f"01_daily/news/{date}_actions.json", "")
    if GROK_DIR.is_dir():
        for path in sorted(GROK_DIR.glob(f"{date}_*.json")):
            blob = _load_json(path)
            rows = blob if isinstance(blob, list) else (blob or {}).get("results") or [blob]
            for it in rows:
                if not isinstance(it, dict):
                    continue
                _add(bag, str(it.get("title") or ""),
                     str(it.get("prompt") or it.get("body") or "")[:800],
                     str(path), str(it.get("createTime") or ""))
    return list(bag.values())


def _prompt(art: dict) -> str:
    return PROMPT.format(
        sectors=", ".join(FINVIZ_SECTORS),
        source_file=art.get("source_file") or "",
        known_at=art.get("known_at") or "",
        title=art.get("title") or "(untitled)",
        body=art.get("body") or "",
    )


def _infer_one(art: dict, ctx: dict) -> dict:
    prompt = _prompt(art)
    tmpl = "news_to_tickers"
    for hop in lane.lanes_for(tmpl):
        parsed, model = lane.ask_lane(
            hop, prompt, ctx, max_tokens=400, system=SYSTEM, tmpl=tmpl,
        )
        if parsed is not None:
            bull = [s for s in (parsed.get("bullish") or []) if s in FINVIZ_SECTORS]
            bear = [s for s in (parsed.get("bearish") or []) if s in FINVIZ_SECTORS]
            return {
                "ok": True,
                "title": art.get("title"),
                "source_file": art.get("source_file"),
                "known_at": art.get("known_at"),
                "bullish": bull,
                "bearish": bear,
                "notes": str(parsed.get("notes") or "")[:240],
                "lane": hop,
                "model": model,
                "inference_source": hop,
                "via": "direct",
            }
    return {
        "ok": False,
        "title": art.get("title"),
        "source_file": art.get("source_file"),
        "known_at": art.get("known_at"),
        "error": "all direct lanes failed",
        "lane": "",
        "model": "",
        "inference_source": "",
        "via": "direct",
    }


def _rollup(rows: list[dict]) -> dict:
    bull, bear, lanes, models = Counter(), Counter(), Counter(), Counter()
    for r in rows:
        if not r.get("ok"):
            continue
        for s in r.get("bullish") or []:
            bull[s] += 1
        for s in r.get("bearish") or []:
            bear[s] += 1
        if r.get("lane"):
            lanes[f"{r.get('lane')}::{r.get('model')}"] += 1
            models[str(r.get("model"))] += 1
    return {
        "bullish_mentions": dict(bull.most_common()),
        "bearish_mentions": dict(bear.most_common()),
        "hopper_watermark": dict(lanes.most_common()),
        "models": dict(models.most_common()),
    }


def _markdown(date: str, rows: list[dict], roll: dict) -> str:
    lines = [
        f"# Lane sector scan — {date}", "",
        f"articles={len(rows)} ok={sum(1 for r in rows if r.get('ok'))}", "",
        "## Hopper watermark (lane::model)",
    ]
    for k, n in (roll.get("hopper_watermark") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Bullish sector mentions", ""]
    for k, n in (roll.get("bullish_mentions") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Bearish sector mentions", ""]
    for k, n in (roll.get("bearish_mentions") or {}).items():
        lines.append(f"- {k}: {n}")
    lines += ["", "## Sample rows", ""]
    for r in rows[:12]:
        mark = f"{r.get('lane')}/{r.get('model')}" if r.get("ok") else r.get("error")
        lines.append(
            f"- [{mark}] {r.get('title','')[:120]} "
            f"bull={r.get('bullish')} bear={r.get('bearish')}"
        )
    return "\n".join(lines) + "\n"


def run(date: str, limit: int = 0, dry_harvest: bool = False) -> dict:
    arts = harvest(date)
    if limit and limit > 0:
        arts = arts[:limit]
    report = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "date": date,
        "harvested": len(arts),
        "limit": limit,
        "results": [],
        "rollup": {},
    }
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    harvest_path = NEWS_DIR / f"{date}_lane_sectors_harvest.json"
    harvest_path.write_text(json.dumps(arts, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[lane_news_scan] harvested {len(arts)} unique articles → {harvest_path}")
    if dry_harvest:
        return report
    keys, ollama_url, gh_direct = lane.load_keys()
    if not keys and not ollama_url and not gh_direct:
        print("[lane_news_scan] no free-lane secrets — harvest only")
        report["error"] = "no free-lane secrets"
        return report
    ctx = {"keys": keys, "ollama_url": ollama_url, "gh_direct": gh_direct}
    lane._SKIP.clear()
    rows = []
    for i, art in enumerate(arts):
        print(f"[lane_news_scan] {i + 1}/{len(arts)} {art.get('title','')[:80]}")
        rows.append(_infer_one(art, ctx))
        time.sleep(getattr(lane, "PACE", 2.1))
    roll = _rollup(rows)
    report["results"] = rows
    report["rollup"] = roll
    report["ok"] = sum(1 for r in rows if r.get("ok"))
    out_json = NEWS_DIR / f"{date}_lane_sectors.json"
    out_md = NEWS_DIR / f"{date}_lane_sectors.md"
    out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    out_md.write_text(_markdown(date, rows, roll), encoding="utf-8")
    OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
    (OUTBOX_DIR / f"{date}.json").write_text(out_json.read_text(encoding="utf-8"), encoding="utf-8")
    print("wrote", out_json, "ok=", report["ok"], "/", len(rows))
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--limit", type=int, default=0, help="0 = every harvested article")
    ap.add_argument("--harvest-only", action="store_true")
    args = ap.parse_args()
    run(args.date, limit=args.limit, dry_harvest=args.harvest_only)


if __name__ == "__main__":
    main()
