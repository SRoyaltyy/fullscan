"""Slim article index for dashboard/news-dir.

Parse date is the file date. Published date is the article clock.
A title that is not in the index was not written by a news pipe.

  PYTHONPATH=. python3 -m src.news_dir
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "dashboard" / "news-dir" / "index.json"
TICK = re.compile(r"\b[A-Z]{1,5}\b")


def _rows(path: Path, parse_date: str, stage: str) -> list[dict]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    items = []
    if isinstance(doc, dict):
        items = doc.get("all_items") or doc.get("items") or doc.get("events") or doc.get("actions") or []
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        title = str(it.get("title") or it.get("event") or it.get("name") or "").strip()
        if not title:
            continue
        published = str(it.get("published_at") or it.get("known_at") or "")[:10]
        tick = str(it.get("ticker") or "")
        extra = " ".join(TICK.findall(title)[:8])
        out.append({
            "parse": parse_date,
            "published": published,
            "stage": stage,
            "title": title[:240],
            "source": str(it.get("source") or "")[:80],
            "url": str(it.get("url") or "")[:300],
            "tick": (tick + " " + extra).strip(),
        })
    return out


def build() -> dict:
    rows = []
    news = ROOT / "01_daily" / "news"
    events = ROOT / "01_daily" / "events"
    for path in sorted(news.glob("*_parsed.json")):
        rows.extend(_rows(path, path.name[:10], "parsed"))
    for path in sorted(news.glob("*_actions.json")):
        rows.extend(_rows(path, path.name[:10], "published"))
    for path in sorted(events.glob("*_events.json")):
        rows.extend(_rows(path, path.name[:10], "parsed"))
    return {"n": len(rows), "rows": rows}


def main() -> None:
    doc = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    print(f"NEWS_DIR_N={doc['n']}")


if __name__ == "__main__":
    main()
