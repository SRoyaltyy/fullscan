#!/usr/bin/env python3
"""Ingest Grok Automations dumps into data/grok_automations/{date}_{slug}.json.

GH Actions tokens cannot call the Automations API. Refresh is bot/Cursor:

  1. Gmail from noreply@x.ai Automation mails, OR
  2. automation_get_results when an X/Grok connector exists
  3. Write the dated JSON and commit it. Harvest reads the tree next restamp.

Usage:
  python3 -m scripts.ingest_grok_automations --date 2026-09-21 --slug hype-factor \\
      --file mail.txt
  python3 -m scripts.ingest_grok_automations --date 2026-09-21 --connector results.json
  cat body.txt | python3 -m scripts.ingest_grok_automations --date 2026-09-21 --stdin

Does not live-trade. Does not call flatten / Webull / factor-mine.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow `python3 -m scripts.ingest_grok_automations` from repo root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.news_impact.grok_automations import (  # noqa: E402
    GROK_DIR,
    parse_connector_results,
    parse_gmail_plaintext,
    write_dump,
)


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default: UTC today)")
    ap.add_argument("--slug", default="", help="hype-factor | google-news-prompt | 13-questions")
    ap.add_argument("--task-id", default="", dest="task_id")
    ap.add_argument("--subject", default="", help="Gmail Subject: line")
    ap.add_argument("--file", default="", help="plaintext mail body or connector JSON")
    ap.add_argument("--connector", default="", help="automation_get_results JSON path")
    ap.add_argument("--stdin", action="store_true", help="read body from stdin")
    ap.add_argument("--out-dir", default="", help="override data/grok_automations")
    args = ap.parse_args()
    date = args.date or _today()
    retrieved = datetime.now(timezone.utc).isoformat()
    raw = ""
    if args.file:
        raw = Path(args.file).read_text(encoding="utf-8")
    elif args.connector:
        raw = Path(args.connector).read_text(encoding="utf-8")
    elif args.stdin or not sys.stdin.isatty():
        raw = sys.stdin.read()
    if not raw.strip():
        print("no input — writing empty dump so the harvest path stays ready")
        dump = {
            "task_id": args.task_id,
            "slug": args.slug or "unknown",
            "kind": args.slug or "unknown",
            "retrieved": retrieved,
            "items": [],
        }
    else:
        try:
            blob = json.loads(raw)
        except json.JSONDecodeError:
            blob = None
        if blob is not None and (args.connector or isinstance(blob, (dict, list))):
            dump = parse_connector_results(blob)
        else:
            dump = parse_gmail_plaintext(
                raw,
                subject=args.subject,
                retrieved=retrieved,
                task_id=args.task_id,
                slug=args.slug,
            )
        if args.slug:
            dump["slug"] = args.slug
        if args.task_id:
            dump["task_id"] = args.task_id
        if not dump.get("retrieved"):
            dump["retrieved"] = retrieved
    root = Path(args.out_dir) if args.out_dir else GROK_DIR
    path = write_dump(dump, date=date, root=root)
    print(f"wrote {path} items={len(dump.get('items') or [])} slug={dump.get('slug')}")


if __name__ == "__main__":
    main()
