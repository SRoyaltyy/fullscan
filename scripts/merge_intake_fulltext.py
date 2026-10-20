"""Merge fetched full article bodies back into the intake documents.

Produces documents.full.json: identical schema to documents.json, but the
`body` of every article we successfully fetched is replaced by the full
text, and a `full_text: true` marker is added so downstream exports can
badge coverage honestly.

Usage:
    python -m scripts.merge_intake_fulltext
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NEWS = ROOT / "data" / "news_intake" / "2026-10-17" / "documents.json"
FULL = ROOT / "data" / "news_intake" / "2026-10-17" / "fulltext.jsonl"
OUT = ROOT / "data" / "news_intake" / "2026-10-17" / "documents.full.json"


def main() -> int:
    bodies: dict[str, str] = {}
    with FULL.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("text"):
                bodies[str(row["id"])] = row["text"]

    articles = json.loads(NEWS.read_text(encoding="utf-8"))
    merged = 0
    for article in articles:
        text = bodies.get(str(article.get("id")))
        if text:
            article["body"] = text
            article["full_text"] = True
            merged += 1
        else:
            article["full_text"] = False

    OUT.write_text(json.dumps(articles, ensure_ascii=False), encoding="utf-8")
    print(f"merged {merged}/{len(articles)} full bodies -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
