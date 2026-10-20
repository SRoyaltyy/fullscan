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

from .aliases import profiles_from_csv
from .correlation import build_board, link_articles
from .filing_business import load_business_texts
from .names import NameIndex, catalog_from_csv
from .filing_evidence import load_filing_sections, load_section_cache

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
        if wanted is not None and ticker not in wanted:
            continue
        seen.add(ticker)
        stocks.append({
            "ticker": ticker,
            "name": row.get("Company") or row.get("name") or "",
            "industry": row.get("Industry") or row.get("industry") or "",
            "sector": sector_of.get(ticker, row.get("Sector") or ""),
            "description": row.get("Finviz_Description") or row.get("description") or "",
            "filing": filings.get(ticker, "") if isinstance(filings.get(ticker, ""), str) else "",
            "filing_sections": filings.get(ticker, []) if isinstance(filings.get(ticker, ""), list) else [],
        })
    for ticker, text in filings.items():
        if ticker in seen:
            continue
        if wanted is not None and ticker not in wanted:
            continue
        stocks.append({
            "ticker": ticker,
            "name": ticker,
            "industry": "",
            "sector": sector_of.get(ticker, ""),
            "description": "",
            "filing": text if isinstance(text,str) else "",
            "filing_sections": text if isinstance(text,list) else [],
        })
    return stocks


def load_articles(path: Path, limit: int, *, headline_only: bool = False) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload if isinstance(payload, list) else payload.get("documents") or payload.get("rows") or []
    articles = []
    for row in rows:
        title = str(row.get("title") or "")
        body = '' if headline_only else str(row.get("body") or row.get("text") or "")
        text = (title + "\n" + body).strip()
        if not text:
            continue
        articles.append({
            "id": row.get("id") or row.get("url") or title[:80],
            "title": title[:240],
            "url": str(row.get("url") or "")[:300],
            "source": str(row.get("source") or "")[:80],
            "text": text,
            "published_at": row.get("published_at") or row.get("published") or "",
            "extraction_status": row.get("extraction_status") or "unknown",
            "full_text": bool(row.get("full_text")),
        })
        if limit and len(articles) >= limit:
            break
    return articles


def summarize(linked: list[dict]) -> dict:
    status = Counter(item["status"] for item in linked)
    tickers = Counter()
    industries = Counter()
    passes = 0
    tiers = Counter()
    for item in linked:
        hit = False
        for row in item.get("stocks") or []:
            if row.get("pass"):
                tiers[str(row.get("tier", "legacy"))] += 1
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
        "passes_by_tier": dict(tiers),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Link news articles to stocks by small-circle overlap")
    parser.add_argument("--descriptions", type=Path, default=ROOT / "excel_bot/data/finviz_with_descriptions.csv")
    parser.add_argument("--universe", type=Path, default=None, help="PR642 dates CSV (tickers column)")
    parser.add_argument("--sectors", type=Path, default=ROOT / "data/finviz/latest.csv")
    parser.add_argument("--filings", type=Path, default=None, help="PR642 objects root, or a ticker->text JSON cache")
    parser.add_argument("--section-cache", type=Path, default=None, help="Explicit fallback to an incomplete legacy SQLite section cache")
    parser.add_argument("--identity-filings",type=Path,default=None,help="Verified full-text objects for explicit glossary/legal identities only; does not establish full exposure coverage")
    parser.add_argument("--as-of", default=None, help="Only consume filings published on/before YYYY-MM-DD")
    parser.add_argument("--require-complete", action="store_true", help="Fail if the full archive reader reports missing text")
    parser.add_argument("--engine", choices=("evidence", "legacy"), default="legacy")
    parser.add_argument("--profile-cache", type=Path, help="Precompiled stock-evidence JSONL gzip; skips repeated profile parsing")
    parser.add_argument("--news", type=Path, default=ROOT / "data/news_intake/2026-10-17/documents.json")
    parser.add_argument("--headline-only", action="store_true", help="Score primary headlines; exclude body and related-headline bundles")
    parser.add_argument("--fixture", type=Path, default=None, help="Extra article text scored against the same board")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--out", type=Path, default=ROOT / "dashboard/news-dir/correlation/latest.json")
    args = parser.parse_args()
    if args.identity_filings and (args.engine!='evidence' or args.profile_cache):
        raise SystemExit('--identity-filings requires uncached evidence compilation')
    if args.require_complete and not args.universe:
        raise SystemExit('--require-complete also needs an explicit --universe to verify company coverage')
    if args.require_complete and (args.section_cache or (args.filings and args.filings.is_file())):
        raise SystemExit('--require-complete requires full PR642 objects, not a legacy cache')
    if args.profile_cache:
        if args.engine != "evidence":
            raise SystemExit('--profile-cache is for the evidence engine')
        from .profile_cache import load_profiles
        wanted=None
        if args.universe:
            wanted={t.upper() for row in _rows(args.universe) for t in str(row.get('tickers') or row.get('ticker') or '').split()}
        evidence, meta = load_profiles(args.profile_cache,as_of=args.as_of,tickers=wanted)
        if args.require_complete and not meta.get('coverage',{}).get('complete'):
            raise SystemExit('profile cache does not establish full source coverage')
        stocks = [{"ticker":p.ticker,"name":p.name,"industry":p.industry,"sector":p.sector} for p in evidence.profiles.values()]
        board = build_board(stocks,engine="evidence")
        board.evidence_board = evidence
        articles = load_articles(args.news,args.limit,headline_only=args.headline_only)
        if args.fixture and args.fixture.is_file():
            articles.insert(0,{"id":"fixture","title":args.fixture.stem,"text":args.fixture.read_text(encoding="utf-8")})
        names=NameIndex(catalog_from_csv(args.descriptions)) if args.descriptions and args.descriptions.is_file() else None
        linked = link_articles(board,articles,engine='evidence',names=names)
        summary = summarize(linked)
        args.out.parent.mkdir(parents=True,exist_ok=True)
        args.out.write_text(json.dumps({"summary":summary,"source_coverage":meta,"links":linked},ensure_ascii=False),encoding="utf-8")
        print(json.dumps(summary,indent=2));print(f"WROTE={args.out}")
        return

    filings: dict[str, str] = {}
    if args.filings and args.filings.is_file() and args.filings.suffix == ".json":
        filings = {str(k).upper(): v for k, v in json.loads(args.filings.read_text()).items()}
    elif args.filings and args.filings.is_dir():
        print("reading annual business prose from", args.filings)
        filings, coverage = load_filing_sections(args.filings, as_of=args.as_of, strict=args.require_complete)
        print("FILING_COVERAGE=" + json.dumps(coverage))
        print(f"FILING_PROFILES={len(filings)}")
        if args.require_complete:
            wanted={t.upper() for row in _rows(args.universe) for t in str(row.get('tickers') or row.get('ticker') or '').split()}
            missing={t for t in wanted if not filings.get(t)}
            if missing:raise SystemExit('full archive is missing requested companies: '+','.join(sorted(missing)[:20]))

    stocks = load_stocks(args.descriptions, args.universe, args.sectors, filings)
    if args.identity_filings:
        identities,identity_coverage=load_filing_sections(args.identity_filings,as_of=args.as_of,strict=True)
        print('IDENTITY_SOURCE_COVERAGE='+json.dumps(identity_coverage))
        for row in stocks:
            row.setdefault('filing_sections',[]).extend({**chunk,'kind':'identity_source'} for chunk in identities.get(row['ticker'],[]))
    if args.section_cache:
        cached, coverage = load_section_cache(args.section_cache, as_of=args.as_of)
        print("FILING_COVERAGE=" + json.dumps(coverage))
        sections = {r["ticker"]:r["filing_sections"] for r in cached}
        for row in stocks:
            row["filing_sections"] = sections.get(row["ticker"], [])
    print(f"STOCKS={len(stocks)} WITH_FILING={sum(1 for row in stocks if row.get('filing'))}")
    board = build_board(stocks, engine=args.engine)
    names = NameIndex(catalog_from_csv(args.descriptions))
    profiles = profiles_from_csv(args.descriptions)
    print(f"BOARD={board.n} INDUSTRY_TERMS={len(board.industry_of_term)}")

    articles = load_articles(args.news, args.limit, headline_only=args.headline_only)
    if args.fixture and args.fixture.is_file():
        articles.insert(0, {
            "id": "fixture",
            "title": args.fixture.stem,
            "url": "",
            "source": "fixture",
            "text": args.fixture.read_text(encoding="utf-8"),
        })
    print(f"ARTICLES={len(articles)}")
    linked = link_articles(board, articles, names=names, profiles=profiles, engine=args.engine)
    summary = summarize(linked)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    payload = {"summary": summary, "links": linked}
    args.out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"WROTE={args.out}")


if __name__ == "__main__":
    main()

