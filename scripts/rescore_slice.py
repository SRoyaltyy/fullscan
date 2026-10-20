"""Re-score a slice of articles in-process (no process pool).

Useful for small re-runs where spinning up a ProcessPool (and a board per
worker) costs more than the scoring itself.

Usage:
    python -m scripts.rescore_slice --news data/news_intake/2026-10-17/documents.full.json --only-enriched
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from src.correlation import link_articles
from src.link_correlation import ROOT, load_articles
from src.scanner_backtest import ENGINE, open_program, present

OUT_DIR = ROOT / "dashboard" / "scanner-backtest"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--news", type=Path, required=True)
    parser.add_argument("--only-enriched", action="store_true",
                        help="only score articles flagged full_text in the news file")
    parser.add_argument("--chunk", type=int, default=25)
    args = parser.parse_args(argv)

    descriptions = ROOT / "excel_bot" / "data" / "finviz_with_descriptions.csv"
    sectors = ROOT / "data" / "finviz" / "latest.csv"
    if not sectors.is_file():
        sectors = None

    started = time.time()
    board, names = open_program(descriptions, sectors, None)
    print(f"board ready ({time.time() - started:.0f}s)", flush=True)

    articles = load_articles(args.news, 0)
    if args.only_enriched:
        articles = [a for a in articles if a.get("full_text")]
    results_path = OUT_DIR / "results.jsonl"
    done = set()
    if results_path.is_file():
        with results_path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    done.add(str(json.loads(line).get("id")))
                except json.JSONDecodeError:
                    continue
    todo = [a for a in articles if str(a.get("id")) not in done]
    print(f"todo {len(todo)}", flush=True)

    scored = 0
    with results_path.open("a", encoding="utf-8") as out:
        for i in range(0, len(todo), args.chunk):
            chunk = todo[i:i + args.chunk]
            linked = link_articles(board, chunk, names=names, engine=ENGINE)
            for article, item in zip(chunk, linked):
                row = present(item)
                row["id"] = article.get("id")
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
            scored += len(chunk)
            out.flush()
            print(f"scored {scored}/{len(todo)} ({time.time() - started:.0f}s)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
