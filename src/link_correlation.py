"""Link news-scanner articles to stocks through the correlation board.

Reads Finviz descriptions, the granular industry on each row, optional sector
labels, and PR642 annual business prose when an object store or a JSON cache
is present. Writes the passes next to the news directory.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

from .correlation import build_board, link_articles
from .filing_business import load_business_texts

ROOT = Path(__file__).resolve().parents[1]


def _rows(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        return list(csv.DictReader(handle))


def load_stocks(descriptions: Path, universe: Path | None, sectors: Path | None, filings: dict[str, str]) -> list[dict]:
    desc_rows = _rows(descriptions)
    wanted: set[str] | None = None
    if universe and universe.is_file():
        wanted = set()
        for row in _rows(universe):
            for ticker in str(row.get("tickers") or row.get("ticker") or "").split():
                ticker = ticker.strip().upper()
                if ticker:
                    wanted.add(ticker)
    sector_of: dict[str, str] = {}
    if sectors and sectors.is_file():
        for row in _rows(sectors):
            ticker = str(row.get("Ticker") or row.get("ticker") or "").upper().strip()
            sector_of[ticker] = str(row.get("Sector") or row.get("sector") or "")
    stocks = []
    seen = set()
    for row in desc_rows:
        ticker = str(row.get("Ticker") or row.get("ticker") or "").upper().strip()
        if not ticker or ticker in seen:
            continue
        if wanted is not None and ticker not in wanted and ticker not in filings:
            continue
        seen.add(ticker)
        stocks.append({
            "ticker": ticker,
            "name": row.get("Company") or row.get("name") or "",
            "industry": row.get("Industry") or row.get("industry") or "",
            "sector": sector_of.get(ticker, row.get("Sector") or ""),
            "description": row.get("Finviz_Description") or row.get("description") or "",
            "filing": filings.get(ticker, ""),
        })
    for ticker, text in filings.items():
        if ticker in seen:
            continue
        stocks.append({
            "ticker": ticker,
            "name": ticker,
            "industry": "",
            "sector": sector_of.get(ticker, ""),
            "description": "",
            "filing": text,
        })
    return stocks


def load_articles(path: Path, limit: int) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload if isinstance(payload, list) else payload.get("documents") or payload.get("rows") or []
    articles = []
    for row in rows:
        title = str(row.get("title") or "")
        body = str(row.get("body") or row.get("text") or "")
        text = (title + "\n" + body).strip()
        if len(text) < 40:
            continue
        articles.append({
            "id": row.get("id") or row.get("url") or title[:80],
            "title": title[:240],
            "url": str(row.get("url") or "")[:300],
            "source": str(row.get("source") or "")[:80],
            "text": text[:20000],
        })
        if limit and len(articles) >= limit:
            break
    return articles


def summarize(linked: list[dict]) -> dict:
    status = Counter(item["status"] for item in linked)
    tickers = Counter()
    industries = Counter()
    passes = 0
    for item in linked:
        hit = False
        for row in item.get("stocks") or []:
            if row.get("pass"):
                tickers[row["ticker"]] += 1
                industries[row["industry"]] += 1
                hit = True
        passes += int(hit)
    return {
        "articles": len(linked),
        "unknown": status.get("unknown", 0),
        "scored": status.get("scored", 0),
        "articles_with_a_pass": passes,
        "top_tickers": tickers.most_common(25),
        "top_industries": industries.most_common(15),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Link news articles to stocks by small-circle overlap")
    parser.add_argument("--descriptions", type=Path, default=ROOT / "excel_bot/data/finviz_with_descriptions.csv")
    parser.add_argument("--universe", type=Path, default=None, help="PR642 dates CSV (tickers column)")
    parser.add_argument("--sectors", type=Path, default=ROOT / "data/finviz/latest.csv")
    parser.add_argument("--filings", type=Path, default=None, help="PR642 objects root, or a ticker->text JSON cache")
    parser.add_argument("--news", type=Path, default=ROOT / "data/news_intake/2026-10-17/documents.json")
    parser.add_argument("--fixture", type=Path, default=None, help="Extra article text scored against the same board")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--out", type=Path, default=ROOT / "dashboard/news-dir/correlation/latest.json")
    args = parser.parse_args()

    filings: dict[str, str] = {}
    if args.filings and args.filings.is_file() and args.filings.suffix == ".json":
        filings = {str(k).upper(): v for k, v in json.loads(args.filings.read_text()).items()}
    elif args.filings and args.filings.is_dir():
        print("reading annual business prose from", args.filings)
        filings = load_business_texts(args.filings)
        print(f"FILING_PROFILES={len(filings)}")

    stocks = load_stocks(args.descriptions, args.universe, args.sectors, filings)
    print(f"STOCKS={len(stocks)} WITH_FILING={sum(1 for row in stocks if row.get('filing'))}")
    board = build_board(stocks)
    print(f"BOARD={board.n} INDUSTRY_TERMS={len(board.industry_of_term)}")

    articles = load_articles(args.news, args.limit)
    if args.fixture and args.fixture.is_file():
        articles.insert(0, {
            "id": "fixture",
            "title": args.fixture.stem,
            "url": "",
            "source": "fixture",
            "text": args.fixture.read_text(encoding="utf-8"),
        })
    print(f"ARTICLES={len(articles)}")
    linked = link_articles(board, articles)
    summary = summarize(linked)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    payload = {"summary": summary, "links": linked}
    args.out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"WROTE={args.out}")


if __name__ == "__main__":
    main()
