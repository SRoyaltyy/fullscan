"""One backtest gate for the stock scanner.

The page draws a random article from the news intake. Test runs that article
through ``link_articles`` with the evidence engine. That call is the production
decision path, so a scanner update shows up here when it changes that path.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .correlation import build_board, link_articles
from .link_correlation import ROOT, load_articles, load_stocks
from .names import NameIndex, catalog_from_csv
from .stock_evidence import COMPILER_VERSION, compile_board
from .filing_evidence import load_filing_sections

ENGINE = "evidence"
PAGE = Path(__file__).resolve().parents[1] / "dashboard" / "scanner-backtest" / "index.html"


def draw_article(articles: list[dict], rng: random.Random | None = None) -> dict:
    if not articles:
        raise ValueError("article pool is empty")
    rng = rng or random.Random()
    return dict(articles[rng.randrange(len(articles))])


def public_article(article: dict) -> dict:
    text = str(article.get("text") or "")
    return {
        "id": article.get("id"),
        "title": article.get("title") or "",
        "url": article.get("url") or "",
        "source": article.get("source") or "",
        "published_at": str(article.get("published_at") or ""),
        "text": text[:12000],
        "truncated": len(text) > 12000,
    }


def open_program(descriptions: Path, sectors: Path | None, universe: Path | None,
                 *, identity_filings: Path | None = None, as_of: str | None = None,
                 filing_profiles: Path | None = None):
    """Load the same description board the evidence CLI scores, then compile it."""
    stocks = load_stocks(descriptions, universe, sectors, {})
    coverage = {"scope": "description_only", "covered_tickers": [],
                "catalog_tickers": len(stocks), "full_business_coverage": False}
    if identity_filings is not None:
        if not as_of:
            raise ValueError("--identity-filings requires an explicit --as-of date")
        from datetime import date
        date.fromisoformat(as_of)
        sources, report = load_filing_sections(identity_filings, as_of=as_of, strict=True)
        covered = []
        enhanced = []
        for row in stocks:
            # Keep XML/XSD/JSON available in the source archive, not as prose
            # identity assertions. This overlay is not full business extraction.
            sections = [s for s in sources.get(row['ticker'], []) if s['kind'] != 'machine_data']
            if sections:
                covered.append(row['ticker'])
            enhanced.append({**row, 'filing_sections': [
                *list(row.get('filing_sections') or []),
                *[{**s, 'kind': 'identity_source'} for s in sections],
            ]})
        stocks = enhanced
        coverage = {**report, 'scope': 'dated_identity_overlay',
                    'covered_tickers': sorted(covered), 'catalog_tickers': len(stocks),
                    'full_business_coverage': False}
    board = build_board(stocks, engine=ENGINE)
    board.evidence_board = compile_board(board.source_rows)
    if filing_profiles is not None:
        if not as_of:
            raise ValueError('--filing-profiles requires an explicit --as-of date')
        from .stock_keyword_store import attach_keyword_profiles
        from .stock_evidence import Catalog
        board.evidence_board, coverage = attach_keyword_profiles(board.evidence_board, filing_profiles,
                                                       as_of=as_of, catalog=Catalog(board.source_rows))
    board.filing_coverage = coverage
    names = NameIndex(catalog_from_csv(descriptions)) if descriptions.is_file() else None
    return board, names


def select_input(article: dict, input_mode: str = 'headline') -> dict:
    if input_mode not in ('headline', 'full-text'):
        raise ValueError('invalid input mode')
    selected = dict(article)
    if input_mode == 'headline':
        selected['text'] = str(article.get('primary_headline') or article.get('title') or '')
    selected['input_mode'] = input_mode
    return selected


def load_test_articles(path: Path) -> list[dict]:
    """Preserve the uncapped primary headline independently of display titles."""
    articles = load_articles(path, 0)
    payload = json.loads(path.read_text(encoding='utf-8'))
    rows = payload if isinstance(payload, list) else payload.get('documents') or payload.get('rows') or []
    titles = {str(r.get('id') or r.get('url') or str(r.get('title') or '')[:80]): str(r.get('title') or '') for r in rows}
    for article in articles:
        article['primary_headline'] = titles[str(article['id'])]
    return articles


def snapshot_metadata(board, input_mode: str = 'headline') -> dict:
    """Bind cached results to source code, lexical resources and compiled inputs."""
    root = Path(__file__).resolve().parents[1]
    source_names = ('correlation', 'evidence_matcher', 'stock_evidence', 'filing_evidence', 'deep_profile_store', 'filing_relationships',
                    'stock_keywords', 'stock_keyword_store', 'names', 'aliases', 'linguistics', 'semantic_lookup', 'synonyms',
                    'places', 'spelling', 'profile_cache', 'link_correlation', 'scanner_backtest')
    paths = [root / 'src' / (name + '.py') for name in source_names]
    paths += [root / 'scripts/export_backtest_static.py']
    paths += [root / 'requirements-filing-parser.txt']
    data = root / 'src/data'
    paths += [data / name for name in ('english_words.txt', 'lemma_lookup.json.gz', 'territories.json')]
    paths += sorted((data / 'senses').glob('*.json*'))
    hashes = {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    evidence = getattr(board, 'evidence_board', None)
    # The older profile fingerprint hashes passage text only. Also bind dates,
    # accessions, issuer labels and industry assignments used by this board.
    profile_inputs = [{"ticker": ticker, "name": p.name, "industry": p.industry,
                       "sector": p.sector, "sources": p.sources}
                      for ticker, p in sorted(evidence.profiles.items())] if evidence else []
    meta = {'engine': ENGINE, 'compiler_version': COMPILER_VERSION,
            'input_mode': input_mode, 'source_hashes': hashes,
            'profile_fingerprint': evidence.source_fingerprint if evidence else '',
            'profile_inputs_sha256': hashlib.sha256(json.dumps(profile_inputs, sort_keys=True).encode()).hexdigest(),
            'filing_coverage': getattr(board, 'filing_coverage', {'scope': 'description_only', 'full_business_coverage': False})}
    meta['snapshot_id'] = hashlib.sha256(json.dumps(meta, sort_keys=True).encode()).hexdigest()
    return meta


def profile_view(profile) -> dict:
    """Display only the actual board used by this run, never a second EDGAR fetch."""
    facts = {}
    for fact in profile.facts:
        entries = facts.setdefault(fact.kind, [])
        entries.append({**fact.record(), 'term': fact.text})
    return {'name': profile.name, 'industry': profile.industry, 'sector': profile.sector,
            'source': 'scoring_board', 'enriched': any(s.get('filing_date') for s in profile.sources),
            'warnings': list(profile.warnings), 'facts': facts, 'sources': profile.sources,
            'deep_profile': getattr(profile, 'deep_profile', None),
            'keyword_schema': 'stock-keywords-1.0',
            'keywords': getattr(profile, 'keyword_records', []),
            'keyword_evidence': getattr(profile, 'keyword_evidence', {}),
            'keyword_coverage': getattr(profile, 'keyword_coverage', {}),
            'display_limits': {'facts_per_kind': None, 'passage_characters': None},
            'used_for_scoring': True}


def present(item: dict) -> dict:
    passes = []
    for row in item.get("stocks") or []:
        if not row.get("pass"):
            continue
        evidence = []
        for ev in (row.get("evidence") or [])[:4]:
            evidence.append({
                "channel": ev.get("channel") or "",
                "passage": str(ev.get("article_passage") or "")[:500],
                "terms": ev.get("matched_terms") or [],
                "stock_evidence": ev.get("stock_evidence") or {},
            })
        passes.append({
            "ticker": row.get("ticker") or "",
            "name": row.get("name") or "",
            "industry": row.get("industry") or "",
            "sector": row.get("sector") or "",
            "tier": row.get("tier"),
            "reasons": list(row.get("reasons") or []),
            "terms": list(row.get("terms") or []),
            "evidence": evidence,
        })
    return {
        "id": item.get("id"),
        "title": item.get("title") or "",
        "decision": item.get("decision") or "",
        "engine": item.get("engine") or ENGINE,
        "status": item.get("status") or "",
        "passes": passes,
        "pass_count": len(passes),
        "industries": [
            {"industry": row.get("industry") or "", "tier": row.get("tier")}
            for row in (item.get("industries") or [])
            if row.get("pass")
        ],
    }


def run_article(board, article: dict, *, names=None, input_mode='headline', snapshot=None) -> dict:
    """Score one article. Scanner updates have to come through this call."""
    selected = select_input(article, input_mode)
    linked = link_articles(board, [selected], names=names, engine=ENGINE)
    result = present(linked[0])
    from .stock_keywords import compare_article
    result['keyword_comparison'] = compare_article(board,selected['text'],article_date=str(selected.get('published_at') or '')[:10])
    result.update({'input_mode': input_mode,
                   'input_sha256': hashlib.sha256(str(selected.get('text') or '').encode()).hexdigest(),
                   'snapshot_id': (snapshot or {}).get('snapshot_id', '')})
    return result


class Gate:
    def __init__(self, articles: list[dict], *, input_mode='headline', identity_filings=None, as_of=None, filing_profiles=None):
        self.articles = list(articles)
        self.by_id: dict[str, dict] = {}
        for article in self.articles:
            key = str(article.get("id") or "")
            if key and key not in self.by_id:
                self.by_id[key] = article
        self.board = None
        self.names = None
        self.error: str | None = None
        self.ready = False
        self.input_mode = input_mode
        self.identity_filings = identity_filings
        self.filing_profiles = filing_profiles
        self.as_of = as_of
        self.snapshot = {}
        self._rng = random.Random()
        self._lock = threading.Lock()

    def draw(self) -> dict:
        with self._lock:
            return draw_article(self.articles, self._rng)

    def status(self) -> dict:
        stocks = 0
        evidence = getattr(self.board, "evidence_board", None)
        if evidence is not None:
            stocks = len(evidence.profiles)
        return {
            "ready": self.ready,
            "error": self.error,
            "articles": len(self.articles),
            "stocks": stocks,
            "engine": ENGINE,
            "snapshot": self.snapshot,
            "input_mode": self.input_mode,
        }

    def test(self, article_id: str) -> dict:
        if self.error:
            raise RuntimeError(self.error)
        if not self.ready or self.board is None:
            raise RuntimeError("stock board is still loading")
        article = self.by_id.get(str(article_id))
        if article is None:
            raise KeyError(article_id)
        with self._lock:
            return run_article(self.board, article, names=self.names,
                               input_mode=self.input_mode, snapshot=self.snapshot)

    def load(self, descriptions: Path, sectors: Path | None, universe: Path | None) -> None:
        try:
            board, names = open_program(descriptions, sectors, universe,
                                        identity_filings=self.identity_filings, as_of=self.as_of, filing_profiles=self.filing_profiles)
            snapshot = snapshot_metadata(board, self.input_mode)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            return
        self.board = board
        self.names = names
        self.snapshot = snapshot
        self.ready = True

    def load_async(self, descriptions: Path, sectors: Path | None, universe: Path | None) -> None:
        threading.Thread(
            target=self.load,
            args=(descriptions, sectors, universe),
            daemon=True,
        ).start()


def dispatch(gate: Gate, method: str, path: str, body: bytes | None = None):
    from urllib.parse import parse_qs
    query = parse_qs(urlparse(path).query)
    path = urlparse(path).path
    if method == "GET" and path in ("/", "/index.html"):
        return 200, PAGE.read_text(encoding="utf-8"), "text/html; charset=utf-8"
    if method == "GET" and path == "/api/status":
        return 200, gate.status(), "application/json"
    if method == "GET" and path == "/api/profile":
        if not gate.ready or gate.board is None:
            return 503, {'error': gate.error or 'stock board is still loading'}, 'application/json'
        ticker = query.get('ticker', [''])[0].strip().upper()
        profile = gate.board.evidence_board.profiles.get(ticker)
        if profile is None:
            return 404, {'error': 'ticker absent from scoring board'}, 'application/json'
        return 200, profile_view(profile), 'application/json'
    if method == "GET" and path == "/api/article":
        try:
            article = gate.draw()
        except ValueError as exc:
            return 404, {"error": str(exc)}, "application/json"
        article = select_input(article, gate.input_mode)
        return 200, {**public_article(article), 'input_mode': gate.input_mode,
                     'full_text': gate.input_mode == 'full-text'}, "application/json"
    if method == "POST" and path == "/api/test":
        try:
            payload = json.loads((body or b"{}").decode() or "{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            return 400, {"error": "invalid json"}, "application/json"
        if not isinstance(payload, dict):
            return 400, {'error': 'expected a JSON object'}, 'application/json'
        try:
            result = gate.test(str(payload.get("id") or ""))
        except KeyError:
            return 404, {"error": "unknown article"}, "application/json"
        except RuntimeError as exc:
            return 503, {"error": str(exc)}, "application/json"
        return 200, result, "application/json"
    if method == 'POST' and path == '/api/keywords':
        if not gate.ready or gate.board is None:
            return 503, {'error':'stock board is still loading'}, 'application/json'
        try:
            payload = json.loads((body or b'{}').decode())
            if not isinstance(payload,dict) or not isinstance(payload.get('text'),str) or not payload['text'].strip():
                raise ValueError('nonempty article text is required')
            date = payload.get('article_date','')
            if not isinstance(date,str) or (date and not re.fullmatch(r'\d{4}-\d{2}-\d{2}',date)):
                raise ValueError('article_date must be YYYY-MM-DD')
        except (ValueError,UnicodeError) as exc:
            return 400, {'error':str(exc)}, 'application/json'
        from .stock_keywords import compare_article
        return 200, compare_article(gate.board,payload['text'],article_date=date), 'application/json'
    return 404, {"error": "not found"}, "application/json"


class Handler(BaseHTTPRequestHandler):
    gate: Gate

    def log_message(self, fmt: str, *args) -> None:
        return

    def _send(self, status: int, payload, content_type: str) -> None:
        if isinstance(payload, str):
            raw = payload.encode()
        elif isinstance(payload, bytes):
            raw = payload
        else:
            raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        status, payload, content_type = dispatch(self.gate, "GET", self.path)
        self._send(status, payload, content_type)

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        status, payload, content_type = dispatch(self.gate, "POST", self.path, body)
        self._send(status, payload, content_type)


def serve(gate: Gate, host: str, port: int) -> ThreadingHTTPServer:
    Handler.gate = gate
    httpd = ThreadingHTTPServer((host, port), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Backtest the current stock scanner on a random article")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--descriptions", type=Path, default=ROOT / "excel_bot/data/finviz_with_descriptions.csv")
    parser.add_argument("--sectors", type=Path, default=ROOT / "data/finviz/latest.csv")
    parser.add_argument("--universe", type=Path, default=None)
    parser.add_argument("--news", type=Path, default=ROOT / "data/news_intake/2026-10-17/documents.json")
    parser.add_argument('--input-mode', choices=('headline', 'full-text'), default='headline')
    parser.add_argument('--identity-filings', type=Path, help='materialized PR642 objects directory')
    parser.add_argument('--filing-profiles', type=Path, help='complete precompiled deep filing profile directory')
    parser.add_argument('--as-of', help='required ISO date cutoff for filing identities')
    args = parser.parse_args(argv)
    if (args.identity_filings or args.filing_profiles) and not args.as_of:
        parser.error('--identity-filings/--filing-profiles requires --as-of')
    articles = load_test_articles(args.news)
    gate = Gate(articles, input_mode=args.input_mode,
                identity_filings=args.identity_filings, as_of=args.as_of, filing_profiles=args.filing_profiles)
    sectors = args.sectors if args.sectors.is_file() else None
    gate.load_async(args.descriptions, sectors, args.universe if args.universe else None)
    httpd = serve(gate, args.host, args.port)
    host, port = httpd.server_address[:2]
    print(f"scanner backtest http://{host}:{port} articles={len(articles)} engine={ENGINE}")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()



