"""Write a Lane batch for Deepseek web, and apply the reply to tickers.

The model names the event and the vertical. This script attaches tickers
from the Finviz industry book. It does not call a model.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.news_impact.lane_batch import apply_reply, job_for, render_batch
from src.news_impact.ticker_verticals import load_book


def _rows(path: Path) -> list[dict]:
    blob = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(blob, dict):
        return list(blob.get("items") or blob.get("documents") or [])
    return list(blob)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--prompt", type=Path)
    parser.add_argument("--reply", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--book", type=Path)
    args = parser.parse_args()
    book = load_book(args.book) if args.book else load_book()
    jobs = [job_for(row, book) for row in _rows(args.inputs)]
    kept = [job for job in jobs if job["keep"]]
    if args.prompt:
        args.prompt.parent.mkdir(parents=True, exist_ok=True)
        args.prompt.write_text(render_batch(jobs), encoding="utf-8")
    linked = []
    if args.reply:
        linked = apply_reply(kept, args.reply.read_text(encoding="utf-8"), book)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(linked, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "rows": len(jobs),
        "kept": len(kept),
        "prompt": str(args.prompt or ""),
        "applied": len(linked),
        "complete": sum(1 for row in linked if row.get("complete")),
    }, indent=2))


if __name__ == "__main__":
    main()
