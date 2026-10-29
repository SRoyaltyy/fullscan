"""Slim article index for dashboard/news-dir.

Parse date is the file date. Published date is the article clock.
A title that is not in the index was not written by a news pipe.

  PYTHONPATH=. python3 -m src.news_dir
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "dashboard" / "news-dir"
INDEX = DIR / "index.json"
DAYS = DIR / "days.json"
TICK = re.compile(r"\b[A-Z]{1,5}\b")
ISO = re.compile(r"(20\d{2}-\d{2}-\d{2})")
RFC = re.compile(r"(\d{1,2})\s+([A-Za-z]{3})\s+(20\d{2})")
MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def published_day(raw: str) -> str:
    text = str(raw or "")
    iso = ISO.search(text)
    if iso:
        return iso.group(1)
    rfc = RFC.search(text)
    if not rfc:
        return ""
    month = MONTHS.get(rfc.group(2).lower(), "")
    if not month:
        return ""
    return f"{rfc.group(3)}-{month}-{int(rfc.group(1)):02d}"


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
        tick = str(it.get("ticker") or "")
        extra = " ".join(TICK.findall(title)[:8])
        out.append({
            "parse": parse_date,
            "published": published_day(
                str(it.get("published_at") or it.get("known_at") or it.get("date") or "")
            ),
            "stage": stage,
            "title": title[:240],
            "source": str(it.get("source") or "")[:80],
            "url": str(it.get("url") or "")[:300],
            "tick": (tick + " " + extra).strip(),
        })
    return out


def collect_rows(root: Path | None = None) -> list[dict]:
    root = root or ROOT
    rows = []
    news = root / "01_daily" / "news"
    events = root / "01_daily" / "events"
    if news.is_dir():
        for path in sorted(news.glob("*_parsed.json")):
            rows.extend(_rows(path, path.name[:10], "parsed"))
        for path in sorted(news.glob("*_actions.json")):
            rows.extend(_rows(path, path.name[:10], "published"))
    if events.is_dir():
        for path in sorted(events.glob("*_events.json")):
            rows.extend(_rows(path, path.name[:10], "parsed"))
    return rows


def dedupe(rows: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for r in rows:
        key = (r["parse"], r["stage"], r["title"].lower(), r["url"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def days_doc(rows: list[dict]) -> dict:
    counts = Counter(r["parse"] for r in rows if r.get("parse"))
    return {
        "n": len(rows),
        "days": [{"date": day, "n": counts[day]} for day in sorted(counts)],
    }


def build(root: Path | None = None) -> dict:
    rows = dedupe(collect_rows(root))
    return {"n": len(rows), "rows": rows, **{"days": days_doc(rows)["days"]}}


def write(root: Path | None = None) -> dict:
    """Write per-parse-day row shards plus a small index (GitHub caps files at 100MB)."""
    root = root or ROOT
    rows = dedupe(collect_rows(root))
    directory = root / "dashboard" / "news-dir"
    shard_dir = directory / "rows"
    shard_dir.mkdir(parents=True, exist_ok=True)
    by_day: dict[str, list[dict]] = {}
    for r in rows:
        by_day.setdefault(r.get("parse") or "undated", []).append(r)
    shards = []
    for day in sorted(by_day):
        part = by_day[day]
        (shard_dir / f"{day}.json").write_text(
            json.dumps({"date": day, "n": len(part), "rows": part}, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        shards.append({"date": day, "n": len(part), "path": f"rows/{day}.json"})
    index = {"n": len(rows), "sharded": True, "shards": shards}
    days = days_doc(rows)
    (directory / "index.json").write_text(
        json.dumps(index, ensure_ascii=False), encoding="utf-8"
    )
    (directory / "days.json").write_text(
        json.dumps(days, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return days


def main() -> None:
    days = write()
    print(f"NEWS_DIR_N={days['n']}")
    print(f"NEWS_DIR_DAYS={len(days['days'])}")


if __name__ == "__main__":
    main()
