"""EDGAR reads for the company-filing parser.

The Commission asks for a contact in the User-Agent and a rate at or below
ten requests a second. This client stays under that and retries a throttle.
"""
from __future__ import annotations

import os
import threading
import time
import urllib.parse

import requests

SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
SUBMISSIONS_FILE = "https://data.sec.gov/submissions/{name}"
TICKERS = "https://www.sec.gov/files/company_tickers.json"
ARCHIVES = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{name}"

DEFAULT_IDENTITY = "Fullscan filing parser research@sroyaltyy.github.io"
MAX_DOCUMENT_BYTES = 25_000_000


class EdgarError(RuntimeError):
    pass


class EdgarBlocked(EdgarError):
    """SEC accepted the connection and refused automated access. Do not retry in a loop."""


class RateLimiter:
    def __init__(self, per_sec: float = 8.0):
        self.gap = 1.0 / per_sec
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                delay = self._next - now
                self._next += self.gap
            else:
                delay = 0.0
                self._next = now + self.gap
        if delay:
            time.sleep(delay)


def relay_target(relay_url: str, url: str) -> str:
    """Fetch a .gov document through the official-source worker when one is configured.

    The worker allowlist is the same host policy as the primary-action scanner.
    A non-.gov URL is left alone so the relay cannot be pointed at other sites.
    """
    relay_url = (relay_url or "").strip().rstrip("/")
    if not relay_url:
        return url
    host = (urllib.parse.urlsplit(url).hostname or "").lower()
    if not host.endswith(".gov"):
        return url
    return relay_url + "/?url=" + urllib.parse.quote(url, safe="")


class Client:
    def __init__(self, identity: str | None = None, per_sec: float = 8.0, relay_url: str | None = None):
        self.identity = identity or os.environ.get("EDGAR_IDENTITY") or DEFAULT_IDENTITY
        self.relay_url = (relay_url if relay_url is not None else os.environ.get("PRIMARY_ACTION_RELAY_URL") or "").strip().rstrip("/")
        self.limiter = RateLimiter(per_sec)
        self._local = threading.local()
        self._cool_lock = threading.Lock()
        self._cool_until = 0.0

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "User-Agent": self.identity,
                "Accept-Encoding": "gzip, deflate",
            })
            self._local.session = session
        return session

    def _get(self, url: str) -> requests.Response:
        last: Exception | None = None
        target = relay_target(self.relay_url, url)
        for attempt in range(4):
            self.limiter.wait()
            try:
                response = self._session().get(target, timeout=120)
            except requests.RequestException as exc:
                last = exc
                time.sleep(1.5 * (attempt + 1))
                continue
            if response.status_code == 403:
                reason = blocked_reason(response.text)
                if reason and "rate threshold" in reason.lower() and attempt < 3:
                    last = EdgarBlocked(reason)
                    self._cool(60 * (attempt + 1))
                    continue
                if reason:
                    raise EdgarBlocked(reason)
            if response.status_code in {429, 500, 502, 503, 504}:
                last = EdgarError(f"{response.status_code} {url}")
                time.sleep(1.5 * (attempt + 1))
                continue
            if response.status_code == 404:
                raise EdgarError(f"404 {url}")
            response.raise_for_status()
            return response
        if isinstance(last, EdgarBlocked):
            raise last
        raise EdgarError(f"failed {url}: {last}")

    def _cool(self, seconds: float) -> None:
        """One shared pause when SEC asks us to slow down, so workers do not stampede."""
        with self._cool_lock:
            until = time.monotonic() + seconds
            self._cool_until = max(self._cool_until, until)
            delay = self._cool_until - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def ticker_map(self) -> dict[str, str]:
        payload = self._get(TICKERS).json()
        out: dict[str, str] = {}
        for entry in payload.values():
            ticker = str(entry.get("ticker") or "").upper()
            if ticker:
                out[ticker] = str(entry.get("cik_str") or "")
        return out

    def submissions(self, cik: str) -> dict:
        return self._get(SUBMISSIONS.format(cik=str(cik).zfill(10))).json()

    def submissions_file(self, name: str) -> dict:
        return self._get(SUBMISSIONS_FILE.format(name=name)).json()

    def document(self, cik: str, accession: str, filename: str) -> bytes:
        url = archive_url(cik, accession, filename)
        response = self._get(url)
        # The business description is at the front. A 20-F with inline exhibits can
        # be far larger than this; keep the front instead of dropping the company.
        raw = response.content
        if len(raw) > MAX_DOCUMENT_BYTES:
            raw = raw[:MAX_DOCUMENT_BYTES]
        return raw

    def index(self, cik: str, accession: str) -> dict:
        url = archive_url(cik, accession, "index.json")
        try:
            return self._get(url).json()
        except EdgarBlocked:
            raise
        except EdgarError:
            return {}


def blocked_reason(body: str) -> str:
    """SEC's Akamai page. Empty when the response is an ordinary document."""
    if "Undeclared Automated Tool" in body:
        return (
            "SEC rejected this network as an undeclared automated tool. "
            "Set EDGAR_IDENTITY to a contact string like 'Name email@domain' "
            "and run the parser from a network SEC accepts. Companies already stored are skipped on the next run."
        )
    if "Request Rate Threshold Exceeded" in body:
        return (
            "SEC rate threshold exceeded. Wait, then rerun the same command. "
            "Companies already stored are skipped."
        )
    return ""


def archive_url(cik: str, accession: str, filename: str) -> str:
    return ARCHIVES.format(
        cik=str(int(cik)),
        accession=accession.replace("-", ""),
        name=filename,
    )


def exhibit_rows(index: dict, cik: str, accession: str) -> list[dict]:
    directory = (index or {}).get("directory") or {}
    items = directory.get("item") or []
    if isinstance(items, dict):
        items = [items]
    rows = []
    for item in items:
        name = item.get("name") or ""
        if not name or name.endswith("index.html") or name.endswith("index.json"):
            continue
        exhibit_type = (item.get("type") or "").upper()
        if not exhibit_type.startswith("EX-"):
            continue
        try:
            size = int(item.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        rows.append({
            "filename": name,
            "type": exhibit_type,
            "description": item.get("description") or "",
            "url": archive_url(cik, accession, name),
            "size": size,
            "stored": False,
            "skip_reason": "",
        })
    return rows
