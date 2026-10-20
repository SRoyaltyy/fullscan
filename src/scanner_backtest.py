"""One backtest gate for the stock scanner.

The page draws a random article from the news intake. Test runs that article
through ``link_articles`` with the evidence engine. That call is the production
decision path, so a scanner update shows up here when it changes that path.
"""
from __future__ import annotations

import argparse
import json
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .correlation import build_board, link_articles
from .link_correlation import ROOT, load_articles, load_stocks
from .names import NameIndex, catalog_from_csv
from .stock_evidence import compile_board

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


def open_program(descriptions: Path, sectors: Path | None, universe: Path | None):
    """Load the same description board the evidence CLI scores, then compile it."""
    stocks = load_stocks(descriptions, universe, sectors, {})
    board = build_board(stocks, engine=ENGINE)
    board.evidence_board = compile_board(board.source_rows)
    names = NameIndex(catalog_from_csv(descriptions)) if descriptions.is_file() else None
    return board, names


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


def run_article(board, article: dict, *, names=None) -> dict:
    """Score one article. Scanner updates have to come through this call."""
    linked = link_articles(board, [article], names=names, engine=ENGINE)
    return present(linked[0])


class Gate:
    def __init__(self, articles: list[dict]):
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
            return run_article(self.board, article, names=self.names)

    def load(self, descriptions: Path, sectors: Path | None, universe: Path | None) -> None:
        try:
            board, names = open_program(descriptions, sectors, universe)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            return
        self.board = board
        self.names = names
        self.ready = True

    def load_async(self, descriptions: Path, sectors: Path | None, universe: Path | None) -> None:
        threading.Thread(
            target=self.load,
            args=(descriptions, sectors, universe),
            daemon=True,
        ).start()


def dispatch(gate: Gate, method: str, path: str, body: bytes | None = None):
    path = urlparse(path).path
    if method == "GET" and path in ("/", "/index.html"):
        return 200, PAGE.read_text(encoding="utf-8"), "text/html; charset=utf-8"
    if method == "GET" and path == "/api/status":
        return 200, gate.status(), "application/json"
    if method == "GET" and path == "/api/article":
        try:
            article = gate.draw()
        except ValueError as exc:
            return 404, {"error": str(exc)}, "application/json"
        return 200, public_article(article), "application/json"
    if method == "POST" and path == "/api/test":
        try:
            payload = json.loads((body or b"{}").decode() or "{}")
        except json.JSONDecodeError:
            return 400, {"error": "invalid json"}, "application/json"
        try:
            result = gate.test(str(payload.get("id") or ""))
        except KeyError:
            return 404, {"error": "unknown article"}, "application/json"
        except RuntimeError as exc:
            return 503, {"error": str(exc)}, "application/json"
        return 200, result, "application/json"
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
    args = parser.parse_args(argv)
    articles = load_articles(args.news, 0)
    gate = Gate(articles)
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
