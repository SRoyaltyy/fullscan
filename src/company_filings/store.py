"""SQLite store for section text and source-point offsets.

The section is stored once, gzipped. A point is a start/end into that text.
Rebuilding a company replaces every row for its CIK in one transaction.
"""
from __future__ import annotations

import gzip
import hashlib
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    ticker TEXT PRIMARY KEY,
    cik TEXT,
    name TEXT,
    sector TEXT,
    industry TEXT,
    market_cap_m REAL,
    status TEXT,
    error TEXT,
    fingerprint TEXT,
    n_docs INTEGER,
    n_points INTEGER,
    stored_bytes INTEGER,
    parsed_at TEXT
);
CREATE TABLE IF NOT EXISTS documents (
    accession TEXT PRIMARY KEY,
    cik TEXT,
    form TEXT,
    filing_date TEXT,
    report_date TEXT,
    items TEXT,
    url TEXT,
    sha256 TEXT,
    stored_bytes INTEGER,
    split_status TEXT
);
CREATE TABLE IF NOT EXISTS sections (
    accession TEXT,
    item TEXT,
    text_gz BLOB,
    n_chars INTEGER,
    n_points INTEGER,
    PRIMARY KEY (accession, item)
);
CREATE TABLE IF NOT EXISTS points (
    point_id TEXT PRIMARY KEY,
    accession TEXT,
    item TEXT,
    ordinal INTEGER,
    start INTEGER,
    end INTEGER
);
CREATE TABLE IF NOT EXISTS exhibits (
    accession TEXT,
    filename TEXT,
    exhibit_type TEXT,
    description TEXT,
    url TEXT,
    size_bytes INTEGER,
    stored INTEGER,
    skip_reason TEXT,
    PRIMARY KEY (accession, filename)
);
CREATE INDEX IF NOT EXISTS documents_cik ON documents(cik);
CREATE INDEX IF NOT EXISTS points_accession ON points(accession);
"""


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(companies)")}
        if "extractor" not in columns:
            self.conn.execute("ALTER TABLE companies ADD COLUMN extractor TEXT")

    def close(self) -> None:
        self.conn.close()

    def fingerprint(self, ticker: str) -> str | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT fingerprint, status FROM companies WHERE ticker = ?",
                (ticker,),
            ).fetchone()
        if row and row[1] == "ok":
            return row[0]
        return None

    def known_cik(self, ticker: str) -> str:
        """CIK from an earlier parse. The SEC ticker file sometimes drops a symbol for a day."""
        with self._lock:
            row = self.conn.execute(
                "SELECT cik FROM companies WHERE ticker = ? AND cik != ''",
                (ticker,),
            ).fetchone()
        return row[0] if row else ""

    def extractor(self, ticker: str) -> str:
        with self._lock:
            row = self.conn.execute(
                "SELECT extractor FROM companies WHERE ticker = ?",
                (ticker,),
            ).fetchone()
        return row[0] or "" if row else ""

    def base_is_thin(self, ticker: str, forms: tuple[str, ...], minimum_chars: int = 500) -> bool:
        """True when the annual report's longest section is a contents line.

        An amendment or an 8-K can be long while the 10-K extract is still the
        contents page, so the annual report is checked on its own.
        """
        annual = self._longest_section(ticker, ("10-K", "20-F", "40-F"))
        if annual is None:
            annual = self._longest_section(ticker, forms)
        if annual is None:
            return False
        return annual < minimum_chars

    def _longest_section(self, ticker: str, forms: tuple[str, ...]) -> int | None:
        if not forms:
            return None
        marks = ",".join("?" for _ in forms)
        with self._lock:
            row = self.conn.execute(
                f"""SELECT MAX(s.n_chars)
                    FROM sections s
                    JOIN documents d ON d.accession = s.accession
                    JOIN companies c ON c.cik = d.cik
                    WHERE c.ticker = ? AND d.form IN ({marks})""",
                (ticker, *forms),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    def replace_company(self, company: dict, docs: list[dict]) -> None:
        """docs: accession, form, filing_date, report_date, items, url, sha256,
        split_status, sections {item: text}, points {item: [(ordinal, start, end)]},
        exhibits [dict].
        """
        cik = company["cik"]
        tickers = company.get("tickers") or [company["ticker"]]
        with self._lock:
            try:
                self.conn.execute("BEGIN")
                old = [row[0] for row in self.conn.execute("SELECT accession FROM documents WHERE cik = ?", (cik,))]
                if old:
                    marks = ",".join("?" for _ in old)
                    self.conn.execute(f"DELETE FROM points WHERE accession IN ({marks})", old)
                    self.conn.execute(f"DELETE FROM sections WHERE accession IN ({marks})", old)
                    self.conn.execute(f"DELETE FROM exhibits WHERE accession IN ({marks})", old)
                    self.conn.execute("DELETE FROM documents WHERE cik = ?", (cik,))
                stored = 0
                n_points = 0
                for doc in docs:
                    doc_bytes = 0
                    for item, text in doc["sections"].items():
                        blob = gzip.compress(text.encode("utf-8"), compresslevel=6)
                        doc_bytes += len(blob)
                        item_points = doc["points"].get(item) or []
                        self.conn.execute(
                            "INSERT INTO sections (accession, item, text_gz, n_chars, n_points) VALUES (?, ?, ?, ?, ?)",
                            (doc["accession"], item, blob, len(text), len(item_points)),
                        )
                        for ordinal, start, end in item_points:
                            point_id = f"{doc['accession']}:{item}:{ordinal}"
                            self.conn.execute(
                                "INSERT INTO points (point_id, accession, item, ordinal, start, end) VALUES (?, ?, ?, ?, ?, ?)",
                                (point_id, doc["accession"], item, ordinal, start, end),
                            )
                            n_points += 1
                    stored += doc_bytes
                    self.conn.execute(
                        """INSERT INTO documents
                           (accession, cik, form, filing_date, report_date, items, url, sha256, stored_bytes, split_status)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            doc["accession"], cik, doc["form"], doc.get("filing_date") or "",
                            doc.get("report_date") or "", doc.get("items") or "", doc.get("url") or "",
                            doc.get("sha256") or "", doc_bytes, doc.get("split_status") or "",
                        ),
                    )
                    for exhibit in doc.get("exhibits") or []:
                        self.conn.execute(
                            """INSERT OR REPLACE INTO exhibits
                               (accession, filename, exhibit_type, description, url, size_bytes, stored, skip_reason)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                doc["accession"], exhibit.get("filename") or "", exhibit.get("type") or "",
                                exhibit.get("description") or "", exhibit.get("url") or "",
                                int(exhibit.get("size") or 0), 1 if exhibit.get("stored") else 0,
                                exhibit.get("skip_reason") or "",
                            ),
                        )
                for ticker in tickers:
                    self.conn.execute(
                        """INSERT INTO companies
                           (ticker, cik, name, sector, industry, market_cap_m, status, error, fingerprint,
                            n_docs, n_points, stored_bytes, parsed_at, extractor)
                           VALUES (?, ?, ?, ?, ?, ?, 'ok', '', ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(ticker) DO UPDATE SET
                             cik=excluded.cik, name=excluded.name, sector=excluded.sector,
                             industry=excluded.industry, market_cap_m=excluded.market_cap_m,
                             status='ok', error='', fingerprint=excluded.fingerprint,
                             n_docs=excluded.n_docs, n_points=excluded.n_points,
                             stored_bytes=excluded.stored_bytes, parsed_at=excluded.parsed_at,
                             extractor=excluded.extractor""",
                        (
                            ticker, cik, company.get("name") or "", company.get("sector") or "",
                            company.get("industry") or "", company.get("market_cap_m") or 0,
                            company.get("fingerprint") or "", len(docs), n_points, stored, _now(),
                            company.get("extractor") or "",
                        ),
                    )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def mark(self, company: dict, status: str, error: str = "") -> None:
        tickers = company.get("tickers") or [company["ticker"]]
        with self._lock:
            for ticker in tickers:
                self.conn.execute(
                    """INSERT INTO companies
                       (ticker, cik, name, sector, industry, market_cap_m, status, error, fingerprint,
                        n_docs, n_points, stored_bytes, parsed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', 0, 0, 0, ?)
                       ON CONFLICT(ticker) DO UPDATE SET
                         status=excluded.status, error=excluded.error, parsed_at=excluded.parsed_at""",
                    (
                        ticker, company.get("cik") or "", company.get("name") or "",
                        company.get("sector") or "", company.get("industry") or "",
                        company.get("market_cap_m") or 0, status, error[:500], _now(),
                    ),
                )
            self.conn.commit()

    def point_text(self, point_id: str) -> str:
        with self._lock:
            row = self.conn.execute(
                """SELECT s.text_gz, p.start, p.end
                   FROM points p JOIN sections s ON s.accession = p.accession AND s.item = p.item
                   WHERE p.point_id = ?""",
                (point_id,),
            ).fetchone()
        if row is None:
            raise KeyError(point_id)
        text = gzip.decompress(row[0]).decode("utf-8")
        return text[row[1]:row[2]]

    def summary(self) -> dict:
        with self._lock:
            companies = self.conn.execute("SELECT status, COUNT(*) FROM companies GROUP BY status").fetchall()
            docs = self.conn.execute("SELECT COUNT(*), COALESCE(SUM(stored_bytes), 0) FROM documents").fetchone()
            points = self.conn.execute("SELECT COUNT(*) FROM points").fetchone()[0]
            forms = self.conn.execute("SELECT form, COUNT(*) FROM documents GROUP BY form ORDER BY 2 DESC").fetchall()
        return {
            "companies": {status: count for status, count in companies},
            "documents": docs[0],
            "stored_bytes": docs[1],
            "points": points,
            "forms": {form: count for form, count in forms},
            "db_bytes": self.path.stat().st_size if self.path.exists() else 0,
        }


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()
