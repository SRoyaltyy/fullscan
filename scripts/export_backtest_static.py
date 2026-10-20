"""Export static backtest artifacts for the GitHub Pages scanner-backtest page.

GitHub Pages cannot run the evidence engine, so this script scores the whole
news intake offline through the same production path the local server page
uses — ``link_articles(..., engine="evidence")`` — and writes two committed
artifacts:

- ``dashboard/scanner-backtest/results.jsonl`` — one line per article with the
  full match result. This is the permanent trial-and-error log: every match
  anyone sees is a reproducible record tied to an engine version.
- ``dashboard/scanner-backtest/articles.json`` — display-sized article records
  (text truncated to ``--text-limit`` chars). The match ran on full text
  offline; the page only needs enough text to read the story.

Batching note: ``link_articles`` has a fixed per-call preparation cost, so each
worker scores its whole shard through ONE call. Same function, same engine,
same decision path — just amortized overhead.

Runs are resumable: existing article ids in ``results.jsonl`` are skipped, so
a long export can be chunked across invocations (``--offset``/``--limit``)
or interrupted and restarted.

Usage:
    python -m scripts.export_backtest_static                 # full intake
    python -m scripts.export_backtest_static --limit 500     # smoke test
    python -m scripts.export_backtest_static --workers 8     # parallelism
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from src.correlation import link_articles
from src.link_correlation import ROOT, load_articles
from src.scanner_backtest import ENGINE, open_program, present

OUT_DIR = ROOT / "dashboard" / "scanner-backtest"
RESULTS = OUT_DIR / "results.jsonl"
ARTICLES = OUT_DIR / "articles.json"

_BOARD = None
_NAMES = None


def _init_worker():
    """Build the board once per worker process; it is read-only while scoring."""
    global _BOARD, _NAMES
    descriptions = ROOT / "excel_bot" / "data" / "finviz_with_descriptions.csv"
    sectors = ROOT / "data" / "finviz" / "latest.csv"
    if not sectors.is_file():
        sectors = None
    _BOARD, _NAMES = open_program(descriptions, sectors, None)


def _score_shard(shard: list[dict]) -> list[dict]:
    """Score a shard in one link_articles call; results come back in order."""
    linked = link_articles(_BOARD, shard, names=_NAMES, engine=ENGINE)
    results = []
    for article, item in zip(shard, linked):
        row = present(item)
        row["id"] = article.get("id")
        results.append(row)
    return results


def export_articles(articles: list[dict], text_limit: int) -> list[dict]:
    """Display-sized article records, keyed for the static page."""
    records = []
    for article in articles:
        text = str(article.get("text") or "")
        records.append({
            "id": article.get("id"),
            "title": article.get("title") or "",
            "url": article.get("url") or "",
            "source": article.get("source") or "",
            "published_at": str(article.get("published_at") or ""),
            "text": text[:text_limit],
            "truncated": len(text) > text_limit,
        })
    return records


def load_done_ids(path: Path) -> set[str]:
    done: set[str] = set()
    if path.is_file():
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    done.add(str(json.loads(line).get("id")))
                except json.JSONDecodeError:
                    continue
    return done


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--news", type=Path, default=ROOT / "data" / "news_intake" / "2026-10-17" / "documents.json")
    parser.add_argument("--limit", type=int, default=0, help="score at most N not-yet-done articles")
    parser.add_argument("--offset", type=int, default=0, help="skip the first N intake articles")
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 2))
    parser.add_argument("--shard-size", type=int, default=400, help="articles per link_articles batch call")
    parser.add_argument("--text-limit", type=int, default=3000)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args(argv)

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    results_path = out_dir / RESULTS.name
    articles_path = out_dir / ARTICLES.name

    started = time.time()
    articles = load_articles(args.news, 0)
    if args.offset:
        articles = articles[args.offset:]
    print(f"intake: {len(articles)} articles (engine={ENGINE})")

    done = load_done_ids(results_path)
    todo = [a for a in articles if str(a.get("id")) not in done]
    if args.limit:
        todo = todo[: args.limit]
    print(f"done: {len(done)}  todo: {len(todo)}  workers: {args.workers}")

    if todo:
        # Warm the article display artifact for the scored slice; subsequent
        # runs merge by id so chunked exports accumulate.
        display = export_articles(todo, args.text_limit)
        if articles_path.is_file():
            existing = {str(r.get("id")): r for r in json.loads(articles_path.read_text(encoding="utf-8"))}
        else:
            existing = {}
        for record in display:
            existing[str(record["id"])] = record
        articles_path.write_text(json.dumps(list(existing.values()), ensure_ascii=False), encoding="utf-8")

        shards = [todo[i:i + args.shard_size] for i in range(0, len(todo), args.shard_size)]
        import concurrent.futures as futures

        scored = 0
        with results_path.open("a", encoding="utf-8") as out, \
                futures.ProcessPoolExecutor(max_workers=args.workers, initializer=_init_worker) as pool:
            for shard_results in pool.map(_score_shard, shards):
                for row in shard_results:
                    out.write(json.dumps(row, ensure_ascii=False) + "\n")
                scored += len(shard_results)
                out.flush()
                rate = scored / (time.time() - started)
                remaining = (len(todo) - scored) / max(rate, 0.01)
                print(f"scored {scored}/{len(todo)}  {rate:.1f}/s  eta {remaining/60:.1f} min", flush=True)

    total = len(load_done_ids(results_path))
    with_passes = 0
    with results_path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                if json.loads(line).get("pass_count"):
                    with_passes += 1
            except json.JSONDecodeError:
                continue
    meta = {
        "engine": ENGINE,
        "intake": args.news.parent.name,
        "articles": total,
        "articles_with_passes": with_passes,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    print(f"results.jsonl: {total} articles ({with_passes} with passes)  ({time.time() - started:.0f}s total)")
    if articles_path.is_file():
        print(f"articles.json: {len(json.loads(articles_path.read_text(encoding='utf-8')))} records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
