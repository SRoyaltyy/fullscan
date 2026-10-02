"""One date -> which news pipes parsed, which published.

Reads 00_grounding/news_pipes/catalog.json and the day files on disk.
Does not fetch feeds. Does not call JEV or Lane.

  PYTHONPATH=. python3 -m src.news_day_index --date 2026-10-01
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "00_grounding" / "news_pipes" / "catalog.json"


def load() -> dict:
    return json.loads(CATALOG.read_text(encoding="utf-8"))


def day(date: str) -> dict:
    cat = load()
    parsed, published, missing = [], [], []
    for stage, bag in (("parsed", parsed), ("published", published)):
        for pattern in cat["day_globs"][stage]:
            rel = pattern.format(date=date)
            path = ROOT / rel
            hit = path.is_file()
            row = {"stage": stage, "path": rel, "present": hit}
            (bag if hit else missing).append(row)
    # dated siblings the catalog does not name, so a new writer cannot hide
    extra = []
    known = {r["path"] for r in parsed + published + missing}
    for folder in ("01_daily/news", "01_daily/events"):
        base = ROOT / folder
        if not base.is_dir():
            continue
        for path in sorted(base.glob(f"{date}*")):
            rel = str(path.relative_to(ROOT))
            if rel not in known:
                extra.append(rel)
    return {
        "date": date,
        "parsed": parsed,
        "published": published,
        "missing": missing,
        "unlisted": extra,
        "pipes": cat["pipes"],
    }


def render(row: dict) -> str:
    lines = [f"# {row['date']}", "", "## parsed"]
    for item in row["parsed"]:
        lines.append(f"- {item['path']}")
    if not row["parsed"]:
        lines.append("- none")
    lines += ["", "## published"]
    for item in row["published"]:
        lines.append(f"- {item['path']}")
    if not row["published"]:
        lines.append("- none")
    lines += ["", "## missing"]
    for item in row["missing"]:
        lines.append(f"- {item['stage']}: {item['path']}")
    if row["unlisted"]:
        lines += ["", "## unlisted day files"]
        lines += [f"- {p}" for p in row["unlisted"]]
    lines += ["", "## pipes"]
    for pipe in row["pipes"]:
        flag = "live" if pipe.get("live") else "off"
        lines.append(
            f"- {pipe['id']} [{pipe['stage']}/{flag}] {pipe['writes']}"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    row = day(args.date)
    print(json.dumps(row, indent=2) if args.json else render(row))


if __name__ == "__main__":
    main()
