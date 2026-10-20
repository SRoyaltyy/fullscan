"""Fetch full article bodies for the news intake (resumable, deterministic).

Most intake articles are feed_text (headline + snippet) with Google News
redirect-stub URLs. Google News stubs are decoded through the r.jina.ai
reader proxy, which resolves the stub and returns the article body as
markdown. Direct URLs are fetched normally.

Writes fulltext.jsonl with one row per article: {id, url, status, chars, text}.
Rows that already have text are never refetched; failed rows (429s, timeouts)
are retried on the next run, so the job can be chunked across invocations
with --seconds budget caps.

Usage:
    python -m scripts.fetch_intake_fulltext                 # all missing, no time cap
    python -m scripts.fetch_intake_fulltext --seconds 270   # bounded chunk
    python -m scripts.fetch_intake_fulltext --ids-file ids.txt  # priority order
"""
from __future__ import annotations

import argparse
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NEWS = ROOT / "data" / "news_intake" / "2026-10-17" / "documents.json"
OUT = ROOT / "data" / "news_intake" / "2026-10-17" / "fulltext.jsonl"

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"}
GOOGLE_HOST = "news.google.com"
_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


def extract_text(html: str) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer", "aside", "form", "noscript"]):
        tag.decompose()
    for node in soup.select("article, main, [role=main], .article-body, .post-body, .entry-content"):
        text = node.get_text("\n")
        if len(text) > 800:
            return clean(text)
    return clean(soup.get_text("\n"))


def clean(text: str) -> str:
    lines = [re.sub(r"\s+", " ", l).strip() for l in text.splitlines()]
    lines = [l for l in lines if len(l) > 40]
    return "\n".join(lines)[:40000]


def jina_body(markdown: str) -> str:
    """Pull the 'Markdown Content:' section out of an r.jina.ai response."""
    idx = markdown.find("Markdown Content:")
    if idx >= 0:
        markdown = markdown[idx + len("Markdown Content:"):]
    # r.jina.ai link-card spam lines are noise; drop them
    lines = [l for l in markdown.splitlines() if not l.strip().startswith(("![", "[!["))]
    return clean("\n".join(lines))


def fetch_via_jina(url: str) -> tuple[str, str]:
    import requests
    resp = requests.get("https://r.jina.ai/" + url, timeout=45)
    if resp.status_code == 200:
        text = jina_body(resp.text)
        if len(text) > 400:
            return "ok", text
        return "short", ""
    return str(resp.status_code), ""


def fetch_direct(url: str) -> tuple[str, str]:
    import requests
    resp = requests.get(url, headers=UA, timeout=15, allow_redirects=True)
    if resp.status_code != 200 or len(resp.text) < 800:
        return str(resp.status_code), ""
    text = extract_text(resp.text)
    if len(text) <= 400:
        return "short", ""
    return "ok", text


def fetch_one(article: dict) -> dict:
    url = str(article.get("url") or "")
    row = {"id": article.get("id"), "url": url[:300]}
    if not url:
        row.update(status="nourl", chars=0, text="")
        return row
    try:
        if GOOGLE_HOST in url:
            status, text = fetch_via_jina(url)
            if status == "429":
                time.sleep(8)  # one patient retry inside the worker
                status, text = fetch_via_jina(url)
        else:
            status, text = fetch_direct(url)
        row.update(status=status, chars=len(text), text=text)
        return row
    except Exception as exc:
        row.update(status=type(exc).__name__, chars=0, text="")
        return row


def load_existing(out_path: Path) -> tuple[set[str], dict[str, dict]]:
    """Return (ids with text, latest failure row per id)."""
    done_text: set[str] = set()
    failures: dict[str, dict] = {}
    if not out_path.is_file():
        return done_text, failures
    with out_path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            rid = str(row.get("id"))
            if row.get("text"):
                done_text.add(rid)
            else:
                failures[rid] = row
    return done_text, failures


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--news", type=Path, default=NEWS)
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--seconds", type=float, default=0.0, help="stop submitting new fetches after this many seconds")
    parser.add_argument("--ids-file", type=Path, default=None,
                        help="file with one article id per line; those first, in order")
    parser.add_argument("--direct-only", action="store_true",
                        help="skip Google News stubs (decode route unavailable)")
    args = parser.parse_args(argv)

    started = time.time()
    stop_submitting_at = started + args.seconds if args.seconds else float("inf")
    articles = json.loads(args.news.read_text(encoding="utf-8"))
    if args.direct_only:
        articles = [a for a in articles if GOOGLE_HOST not in str(a.get("url") or "")]
    by_id = {str(a.get("id")): a for a in articles}

    done_text, failures = load_existing(args.out)

    order: list[str] = []
    if args.ids_file and args.ids_file.is_file():
        for line in args.ids_file.read_text(encoding="utf-8").splitlines():
            rid = line.strip()
            if rid and rid in by_id and rid not in done_text:
                order.append(rid)
    order += [str(a.get("id")) for a in articles
              if str(a.get("id")) not in done_text and str(a.get("id")) not in order]
    if args.limit:
        order = order[: args.limit]
    todo = [by_id[rid] for rid in order]
    log(f"total {len(articles)} with-text {len(done_text)} todo {len(todo)} workers {args.workers}")

    # survivors: finished-text rows + failure rows NOT being retried this run
    retry_ids = {str(a.get("id")) for a in todo}
    survivors = [row for rid, row in failures.items() if rid not in retry_ids]
    # also recover finished-text rows (need full rows, not just ids)
    if args.out.is_file():
        with args.out.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("text"):
                    survivors.append(row)

    got = ok = 0
    with args.out.open("w", encoding="utf-8") as out:
        for row in survivors:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
        out.flush()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = []
            for article in todo:
                if time.time() >= stop_submitting_at:
                    break
                futures.append(pool.submit(fetch_one, article))
            for fut in futures:
                row = fut.result()
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                got += 1
                if row.get("text"):
                    ok += 1
                if got % 100 == 0:
                    out.flush()
                    rate = got / max(time.time() - started, 0.01)
                    log(f"fetched {got}/{len(futures)} ok {ok} {rate:.1f}/s "
                        f"elapsed {time.time() - started:.0f}s")
        out.flush()

    total_text = 0
    with args.out.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                if json.loads(line).get("text"):
                    total_text += 1
            except json.JSONDecodeError:
                continue
    log(f"done: +{ok} bodies this run, {total_text} total with body ({time.time() - started:.0f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
