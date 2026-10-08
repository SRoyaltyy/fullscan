"""Durable, zero-API-spend news intake and inspectable first-pass parsing.

Run: python -m src.news_intake --date 2026-10-02 --force
No paid clients, DB or LLM dependencies. All sources contribute to one ledger.
Search evidence is a candidate, never proof of a final Lane classification.
"""
from __future__ import annotations

import argparse
import ast
import concurrent.futures as futures
import csv
import hashlib
import html
import json
import re
import time
import threading
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

from .news_search_catalog import PHRASES, search_specs
from .news_impact.classify import classify_text
from .news_impact.prompts import PROMPT_VERSION
from .news_impact.schema import family_of

VERSION = "free-intake-v1"
ROOT = Path(__file__).resolve().parents[1]
UA = "FullscanNewsIntake/1.0 (+https://github.com/SRoyaltyy/fullscan)"
FALLBACK_QUERIES = {
    'rss_cnbc_economy': 'site:cnbc.com (economy OR inflation OR jobs)',
    'rss_cnbc_finance': 'site:cnbc.com (business OR earnings OR markets)',
    'rss_ap_topnews': 'site:apnews.com (government OR technology OR economy)',
    'rss_ap_business': 'site:apnews.com (business OR companies OR markets)',
    'company_aws_blog': 'site:aws.amazon.com/blogs/aws (launch OR announcing OR available)',
    'official_commerce': 'site:commerce.gov/news/press-releases',
    'official_federal_register': 'site:federalregister.gov (rule OR executive order)',
    'official_bls': 'site:bls.gov (release OR employment OR inflation)',
    'official_ftc': 'site:ftc.gov (merger OR order OR complaint)',
    'sec_8-K': '"8-K" (filed OR filing OR announces)',
    'sec_6-K': '"6-K" (filed OR filing OR announces)',
    'sec_4': '"Form 4" (insider OR filed OR filing)',
    'sec_SC_13D': '"13D" (stake OR filed OR activist)',
    'sec_SCHEDULE_13D': '"Schedule 13D" (stake OR filed OR activist)',
    'sec_S-1': '"S-1" (IPO OR registration OR filed)',
    'sec_S-3': '"S-3" (registration OR offering OR filed)',
    'sec_SC_TO-T': '"tender offer" (SEC OR filed OR filing)',
}
SEC_LOCK = threading.Lock()
SEC_LAST_REQUEST = 0.0
OFFICIAL_FEEDS = {
    "official_fed_releases": "https://www.federalreserve.gov/feeds/press_all.xml",
    "official_fed_speeches": "https://www.federalreserve.gov/feeds/speeches.xml",
    "official_bls": "https://www.bls.gov/feed/bls_latest.rss",
    "official_bea": "https://apps.bea.gov/rss/rss.xml",
    "official_ecb": "https://www.ecb.europa.eu/rss/press.html",
    "official_boe": "https://www.bankofengland.co.uk/rss/news",
    "official_ftc": "https://www.ftc.gov/feeds/press-release.xml",
    "official_doj": "https://www.justice.gov/news/rss",
    "company_nvidia_blog": "https://blogs.nvidia.com/feed/",
    "company_microsoft_blog": "https://blogs.microsoft.com/feed/",
    "company_google_blog": "https://blog.google/rss/",
    "company_openai_blog": "https://openai.com/news/rss.xml",
    "company_aws_blog": "https://aws.amazon.com/blogs/aws/feed/",
}
OFFICIAL_PAGES = {
    "official_fda_releases": ("https://www.fda.gov/news-events/fda-newsroom/press-announcements", r"/news-events/press-announcements/"),
    "official_treasury": ("https://home.treasury.gov/news/press-releases", r"/news/press-releases/[a-z]+\d+"),
    "official_commerce": ("https://www.commerce.gov/news/press-releases", r"/news/press-releases/\d{4}/"),
    "official_scotus": ("https://www.supremecourt.gov/opinions/slipopinion/25", r"/opinions/.*\.pdf"),
}
THEMES = {
    'Central banks, inflation and rates': r'\b(fed|fomc|powell|kashkari|central bank|inflation|cpi|pce|interest rate)\b',
    'AI models, products and infrastructure': r'\b(ai|artificial intelligence|openai|anthropic|coreweave|data cent(?:er|re))\b',
    'Semiconductors and hardware': r'\b(semiconductor|chip|nvidia|asml|micron|tsmc|foundry)\b',
    'Drug approvals and clinical trials': r'\b(fda|clinical trial|phase [23]|primary endpoint|drug approval)\b',
    'Acquisitions and business restructurings': r'\b(acquires|acquisition|merger|spin.?off|definitive agreement|strategic review)\b',
    'Capital returns and financing': r'\b(buyback|repurchase|dividend|offering|credit facility|refinancing)\b',
    'Contracts, orders and capacity': r'\b(contract|supply agreement|order backlog|factory|capacity|manufacturing)\b',
    'Personnel and governance': r'\b(board|ceo|cfo|resigns|steps down|activist|proxy fight)\b',
    'Government, legislation and trade': r'\b(tariff|sanction|legislation|signed into law|executive order|treasury|tax bill)\b',
    'Courts, regulation and investigations': r'\b(court|injunction|judgment|verdict|antitrust|investigation|regulatory)\b',
    'Energy, commodities and shipping': r'\b(oil|lng|opec|gas|copper|gold|shipping|hormuz)\b',
    'Operational, cyber and labour disruptions': r'\b(outage|shutdown|ransomware|breach|strike|union|recall)\b',
    'Earnings and guidance': r'\b(earnings|quarterly results|guidance|profit warning|preliminary results)\b',
}


def stamp(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()


def parse_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        out = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            out = parsedate_to_datetime(value)
        except (ValueError, TypeError, OverflowError):
            return None
    # Date-only/local timestamps do not prove a publication instant.
    if out.tzinfo is None:
        return None
    return out.astimezone(timezone.utc)


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", text.casefold())).strip()


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts, self.links = [], []
        self.hidden = 0
        self.anchor = None
        self.publication_times = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'meta' and (attrs.get('property') or attrs.get('name') or '').lower() in {'article:published_time', 'datepublished', 'date', 'dc.date.issued'}:
            self.publication_times.append(attrs.get('content', ''))
        if tag == 'time' and attrs.get('datetime'):
            self.publication_times.append(attrs['datetime'])
        if tag in {"script", "style", "noscript"}:
            self.hidden += 1
        if tag == "a":
            self.anchor = [attrs.get("href", ""), []]

    def handle_endtag(self, tag):
        if tag in {"script", "style", "noscript"}:
            self.hidden = max(0, self.hidden - 1)
        if tag == "a" and self.anchor:
            self.links.append((self.anchor[0], " ".join(self.anchor[1]).strip()))
            self.anchor = None

    def handle_data(self, data):
        if not self.hidden and data.strip():
            self.parts.append(data.strip())
            if self.anchor:
                self.anchor[1].append(data.strip())


def clean(text: str) -> str:
    parser = TextExtractor()
    parser.feed(text or "")
    return " ".join(parser.parts)


def get(url: str) -> tuple[bytes, str]:
    global SEC_LAST_REQUEST
    if urllib.parse.urlsplit(url).hostname in {"www.sec.gov", "data.sec.gov"}:
        with SEC_LOCK:
            wait = 0.25 - (time.monotonic() - SEC_LAST_REQUEST)
            if wait > 0:
                time.sleep(wait)
            SEC_LAST_REQUEST = time.monotonic()
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/rss+xml, application/atom+xml, application/json, text/html, */*"})
    # Bounded retries; a failing host never blocks other feeds.
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=12) as response:
                data = response.read(8_000_001)
                if len(data) > 8_000_000:
                    raise ValueError("response exceeds 8MB; needs source-specific paging")
                return data, response.headers.get("Content-Type", "")
        except urllib.error.HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt:
                raise
        except (OSError, TimeoutError):
            if attempt:
                raise
        time.sleep(1)
    raise RuntimeError("fetch exhausted")


def feed_rows(data: bytes) -> list[dict]:
    root = ET.fromstring(data)
    if root.tag.split("}")[-1] not in {"rss", "feed", "RDF"}:
        raise ValueError("not an RSS/Atom feed")
    rows = []
    for item in root.iter():
        if item.tag.split("}")[-1] not in {"item", "entry"}:
            continue
        fields = defaultdict(list)
        for child in item:
            fields[child.tag.split("}")[-1]].append(child)
        def val(*names):
            for name in names:
                for elem in fields.get(name, []):
                    value = "".join(elem.itertext()).strip()
                    if value:
                        return value
            return ""
        link = val("link")
        for elem in fields.get("link", []):
            if elem.get("href") and elem.get("rel", "alternate") == "alternate":
                link = elem.get("href")
                break
        title = clean(val("title"))
        if not title:
            continue
        raw_date = val("pubDate", "published", "updated", "date")
        dt = parse_time(raw_date)
        rows.append({"title": title, "url": link, "publisher": val("source"),
                     "body": clean(val("encoded", "content", "description", "summary")),
                     "published_at": stamp(dt) if dt else "", "published_raw": raw_date})
    return rows


def registry(root: Path = ROOT) -> list[dict]:
    # Reuse the existing feed list without importing its Postgres dependencies.
    feeds = {}
    tree = ast.parse((root / "collectors/rss_news.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "FEEDS" for t in node.targets):
            feeds = ast.literal_eval(node.value)
    specs, urls = [], set()
    for key, url in {**feeds, **OFFICIAL_FEEDS}.items():
        if "feeds.reuters.com" in url:
            # Defunct wire endpoints replaced by publisher-specific discovery.
            continue
        if url in urls:
            continue
        urls.add(url)
        specs.append({"id": key, "url": url, "kind": "feed", "interval_minutes": 15,
                      "primary": key.startswith(("official_", "company_"))})
    specs.extend(search_specs())
    for name in ("Reuters", "Associated Press", "Bloomberg"):
        specs.append({"id": f"publisher_{norm(name).replace(' ', '_')}", "kind": "search",
                      "query": f'"{name}" (business OR markets OR technology)', "interval_minutes": 30})
    for key, (url, pattern) in OFFICIAL_PAGES.items():
        specs.append({"id": key, "kind": "page", "url": url, "link_pattern": pattern,
                      "interval_minutes": 30, "primary": True})
    for form in ("8-K", "6-K", "4", "SC 13D", "SCHEDULE 13D", "S-1", "S-3", "SC TO-T"):
        specs.append({"id": "sec_" + form.replace(" ", "_"), "kind": "sec", "form": form,
                      "interval_minutes": 15, "primary": True})
    custom = root / "config/news_intake_sources.json"
    if custom.exists():
        specs.extend(json.loads(custom.read_text()).get("sources", []))
    ids = [s["id"] for s in specs]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate source ids")
    return specs


def google_url(query: str, start: datetime, end: datetime) -> str:
    # Google date bounds are deliberately wider than the exact UTC window.
    query += f" after:{(start - timedelta(days=1)).date()} before:{(end + timedelta(days=1)).date()}"
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode({"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"})


def collect_source(spec: dict, start: datetime, end: datetime, fetch=get) -> tuple[list[dict], dict]:
    begin = time.monotonic()
    health = {"source": spec["id"], "kind": spec["kind"], "checked_at": stamp(),
              "status": "ok", "fetched": 0, "in_window": 0, "undated": 0,
              "saturated": False, "requests": 0, "classes": spec.get("classes", [])}
    rows = []
    try:
        def read_feed(url):
            health["requests"] += 1
            data, _ = fetch(url)
            return feed_rows(data)
        if spec["kind"] == "search":
            def search(a, b, depth=0):
                found = read_feed(google_url(spec["query"], a, b))
                if len(found) >= 100:
                    # RSS has no reliable cursor. Split dates, then flag any
                    # still-saturated daily leaf rather than pretending complete.
                    if (b - a).total_seconds() > 86400 and depth < 3:
                        mid = a + (b - a) / 2
                        return search(a, mid, depth+1) + search(mid, b, depth+1)
                    health["saturated"] = True
                return found
            if spec.get('quick'):
                rows = read_feed(google_url(spec['query'], start, end))
                health['saturated'] = len(rows) >= 100
            else:
                rows = search(start, end)
            if not spec.get('quick') and health['saturated'] and len(spec.get('terms', [])) > 1:
                health['expanded_terms'] = []
                unresolved = False
                for term in spec['terms']:
                    found = read_feed(google_url(f'"{term}"', start, end))
                    rows.extend(found)
                    health['expanded_terms'].append(term)
                    if len(found) >= 100:
                        unresolved = True
                health['saturated'] = unresolved
        elif spec["kind"] == "sec":
            pages = 1 if spec.get('quick') else 20
            for page in range(pages):
                query = urllib.parse.urlencode({"action": "getcurrent", "type": spec["form"], "owner": "include", "count": 100, "start": page*100, "output": "atom"})
                batch = read_feed("https://www.sec.gov/cgi-bin/browse-edgar?" + query)
                rows.extend(batch)
                oldest = min((parse_time(r["published_at"]) for r in batch if r["published_at"]), default=None)
                if len(batch) < 100 or (oldest and oldest < start):
                    break
                if page == pages - 1:
                    health["saturated"] = True
        elif spec["kind"] == "page":
            health["requests"] += 1
            data, _ = fetch(spec["url"])
            parser = TextExtractor()
            parser.feed(data.decode("utf-8", "replace"))
            for href, title in parser.links:
                url = urllib.parse.urljoin(spec["url"], href)
                if url.rstrip('/') != spec['url'].rstrip('/') and len(title) >= 20 and re.search(spec["link_pattern"], url):
                    rows.append({"title": title, "url": url, "body": "", "published_at": "", "published_raw": ""})
        else:
            rows = read_feed(spec["url"])
        health["fetched"] = len(rows)
        accepted = []
        for row in rows:
            dt = parse_time(row.get("published_at", ""))
            if dt and not start <= dt <= end:
                continue
            if not dt:
                health["undated"] += 1
            accepted.append(row | {"source": spec["id"], "primary": spec.get("primary", False),
                                   "query_classes": spec.get("classes", []), "query": spec.get("query", "")})
        rows = accepted
        health["in_window"] = len(rows)
        if not health["fetched"]:
            health["status"] = "empty_unverified"
        if health["saturated"]:
            health["status"] = "partial_saturated"
    except Exception as exc:
        health["status"] = "failed"
        health["error"] = f"{type(exc).__name__}: {exc}"[:400]
        rows = []
        fallback = FALLBACK_QUERIES.get(spec['id'])
        if fallback:
            try:
                found = read_feed(google_url(fallback, start, end))
                for row in found:
                    dt = parse_time(row.get('published_at', ''))
                    if dt and start <= dt <= end:
                        rows.append(row | {'source': spec['id'] + ':google_fallback', 'primary': False,
                                           'query': fallback, 'query_classes': spec.get('classes', [])})
                health['fallback_query'] = fallback
                health['fallback_count'] = len(rows)
                health['status'] = 'fallback_search' if rows else 'failed'
                health['saturated'] = len(found) >= 100
                health['in_window'] = len(rows)
            except Exception as fallback_exc:
                health['fallback_error'] = str(fallback_exc)[:200]
    health["duration_seconds"] = round(time.monotonic()-begin, 2)
    return rows, health


def document_id(row: dict) -> str:
    url = row.get("url", "").strip()
    if url:
        bits = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qsl(bits.query)
        query = [(k,v) for k,v in query if not k.startswith("utm_") and k not in {"oc", "fbclid", "gclid"}]
        key = urllib.parse.urlunsplit((bits.scheme, bits.netloc.lower(), bits.path, urllib.parse.urlencode(query), ""))
    else:
        key = norm(row["title"]) + "|" + row.get("published_at", "")
    return hashlib.sha256(key.encode()).hexdigest()[:24]


def merge_documents(previous: list[dict], incoming: list[dict], now: str) -> list[dict]:
    docs = {d["id"]: d for d in previous}
    for row in incoming:
        key = document_id(row)
        if key not in docs:
            docs[key] = row | {"id": key, "first_seen": now, "observations": [],
                               "extraction_status": "feed_text" if row.get("body") else "headline_only"}
        doc = docs[key]
        # Preserve the longest evidence and the first discovery; never replace
        # full text with a short RSS snippet on the next poll.
        if len(row.get("body", "")) > len(doc.get("body", "")):
            doc["body"] = row["body"]
        if not doc.get("published_at") and row.get("published_at"):
            doc["published_at"] = row["published_at"]
        doc["primary"] = bool(doc.get("primary") or row.get("primary"))
        obs = {"source": row.get("source", ""), "query": row.get("query", ""), "classes": row.get("query_classes", [])}
        if obs not in doc["observations"]:
            doc["observations"].append(obs)
        doc["last_seen"] = now
    return sorted(docs.values(), key=lambda d: (d.get("published_at") or d["first_seen"], d["id"]), reverse=True)


def local_rows(root: Path, date: str) -> tuple[list[dict], list[dict]]:
    rows, health = [], []
    paths = list((root / "01_daily/news").glob(f"{date}*.json"))
    paths += list((root / "01_daily/events").glob(f"{date}*events.json"))
    paths += list((root / "data/grok_automations").glob(f"**/*{date}*.json"))
    def walk(blob):
        if isinstance(blob, dict):
            title = blob.get("title") or blob.get("headline")
            if title and isinstance(title, str):
                yield blob
            for value in blob.values():
                if isinstance(value, (dict, list)):
                    yield from walk(value)
        elif isinstance(blob, list):
            for value in blob:
                yield from walk(value)
    for path in sorted(paths):
        if "_intake" in path.name or "_parsed" in path.name and path.name.endswith("_intake_parsed.json"):
            continue
        try:
            count = 0
            for item in walk(json.loads(path.read_text())):
                raw = str(item.get("published_at") or item.get("published") or "")
                dt = parse_time(raw)
                source_urls = item.get("sources") or []
                url = str(item.get("url") or item.get("link") or (source_urls[0] if source_urls and isinstance(source_urls[0], str) else ""))
                rows.append({"title": item.get("title") or item.get("headline"), "url": url,
                             "body": str(item.get("body") or item.get("summary") or item.get("why_it_matters") or ""),
               "source": str(path.relative_to(root)), "published_at": stamp(dt) if dt else "",
                             "published_raw": raw, "primary": False, "query_classes": [],
                             "source_file": str(path.relative_to(root)), "legacy_generated": "events" in str(path) or "judge" in path.name})
                count += 1
            health.append({"source": str(path.relative_to(root)), "kind": "local", "status": "ok", "fetched": count})
        except (OSError, ValueError) as exc:
            health.append({"source": str(path.relative_to(root)), "kind": "local", "status": "failed", "error": str(exc)[:200]})
    path = root / f"data/exports/finviz_{date}.csv"
    if path.exists():
        with path.open(newline="", encoding="utf-8") as handle:
            for item in csv.DictReader(handle):
                if item.get("News Title"):
                    dt = parse_time(item.get("News Time", ""))
                    rows.append({"title": item["News Title"], "body": item.get("Daily Digest", ""),
                                 "url": item.get("News URL", ""), "source": "finviz_export",
                                 "published_at": stamp(dt) if dt else "", "published_raw": item.get("News Time", ""),
                                 "ticker_hint": item.get("Ticker", ""), "primary": False, "query_classes": []})
    return rows, health


def first_pass(doc: dict, now: datetime) -> dict:
    title = doc["title"]
    # Short feed bodies only; legacy LLM essays must not invent title matches.
    text = title + " " + (doc.get("body", "")[:1000] if doc.get('extraction_status') == 'feed_text' and not doc.get("legacy_generated") else "")
    hits = {}
    for cls, phrases in PHRASES.items():
        matched = [p for p in phrases if re.search(r"(?<!\w)" + re.escape(p) + r"(?!\w)", text, re.I)]
        if matched:
            hits[cls] = matched
    packaging = bool(re.search(r"\b(should you buy|stocks to buy|price target|how to play|best stocks|stock picks)\b", title, re.I))
    concrete = set(hits) - {"regime_state", "statement_public", "rumor", "peer_spill"}
    dt = parse_time(doc.get("published_at", ""))
    date_status = "dated" if dt else "publication_unknown"
    if dt and dt < now - timedelta(hours=48):
        date_status = "outside_window"
    if dt and dt > now:
        date_status = "future_timestamp"
    # No irreversible drop: every row goes to candidate or review queue.
    state = "candidate" if (concrete or doc.get("primary")) and date_status == "dated" else "review"
    reasons = []
    if packaging:
        reasons.append("packaging; retain embedded event for review")
    if "rumor" in hits:
        reasons.append("rumor wording; verification required")
        state = "review"
    if date_status != "dated":
        reasons.append(date_status)
    if doc.get("legacy_generated"):
        state = "review"
        reasons.append("legacy generated event; verify underlying sources")
    if not hits:
        reasons.append("no lexical match; open-set review")
    classification = classify_text(title, doc.get("body", "")[:1000] if doc.get("extraction_status") == "feed_text" and not doc.get("legacy_generated") else "").to_dict()
    route_class = classification['event_class']
    route_basis = 'existing_lane_rules'
    if route_class == 'discard' and concrete:
        route_class = next(cls for cls in hits if cls in concrete)
        route_basis = 'coverage_hint_requires_confirmation'
        reasons.append('Lane rule router disagrees with event wording; retain for contextual review')
    return {"document_id": doc["id"], "title": title, "url": doc.get("url", ""),
            "source": doc.get("source", ""), "published_at": doc.get("published_at", ""),
            "first_seen": doc["first_seen"], "last_seen": doc.get("last_seen", ""),
            "parsed_at": stamp(now), "publisher": doc.get("publisher", ""),
            "publisher_domain": urllib.parse.urlsplit(doc.get("url", "")).hostname or "",
            "published_raw": doc.get("published_raw", ""),
            "discovery_paths": doc.get("observations", []),
            "state": state, "candidate_classes": list(hits),
            "matched_phrases": hits, "date_status": date_status, "review_reasons": reasons,
            "parser": VERSION, "classifier": "deterministic_coverage_hints", "final_lane_class": None,
            "lane_rule_classification": classification, "analyst_family": family_of(route_class),
            "routing_class": route_class, "routing_basis": route_basis,
            "analyst_prompt_version": PROMPT_VERSION, "analysis_status": "pending_contextual_analysis",
            "themes": [name for name, pattern in THEMES.items() if re.search(pattern, text, re.I)],
            "extraction_status": doc.get("extraction_status", "headline_only")}


def extract_documents(docs: list[dict], limit: int, fetch=get) -> dict:
    now = datetime.now(timezone.utc)
    def ready(d):
        attempted = parse_time(d.get('extraction_attempted_at', ''))
        return d.get('extraction_attempts', 0) < 3 and (not attempted or (now-attempted).total_seconds() >= 1800)
    pending = [d for d in docs if d.get("url", "").startswith("https://")
               and "news.google.com/" not in d["url"]
               and d.get("extraction_status") != "page_text" and ready(d)]
    pending.sort(key=lambda d: (not d.get("primary"), d.get('source') == 'sec_4', -len(d.get("body", ""))))
    counts = Counter()
    def one(doc):
        try:
            data, mime = fetch(doc["url"])
            if doc.get('source', '').startswith('sec_') and re.search(r'-index\.html?$', doc['url']):
                listing = TextExtractor()
                listing.feed(data.decode('utf-8', 'replace'))
                filings = []
                for href, label in listing.links:
                    url = urllib.parse.urljoin(doc['url'], href)
                    if '/Archives/edgar/data/' in url and re.search(r'\.(?:htm|html|xml)$', url) and '-index.' not in url:
                        filings.append((url, label))
                if filings:
                    # Press-release exhibits before the filing wrapper; keep
                    # the original accession URL as immutable provenance.
                    chosen = next((url for url,label in filings if re.search(r'ex(?:hibit)?[-_]?99|press|release', url, re.I)), filings[0][0])
                    data, mime = fetch(chosen)
                    doc['extracted_url'] = chosen
            if 'xml' in mime and doc.get('source', '').startswith('sec_'):
                root = ET.fromstring(data)
                body = '\n'.join(f"{e.tag.split('}')[-1]}: {e.text.strip()}" for e in root.iter() if e.text and e.text.strip())
                return doc, 'page_text', body
            if "html" not in mime and "text/plain" not in mime:
                return doc, "unsupported_format", ""
            body = clean(data.decode("utf-8", "replace"))
            if len(body) < 200:
                return doc, "thin_text", ""
            parser = TextExtractor()
            parser.feed(data.decode('utf-8', 'replace'))
            if not doc.get('published_at'):
                for raw in parser.publication_times:
                    dt = parse_time(raw)
                    if dt:
                        doc['published_at'] = stamp(dt)
                        doc['publication_time_source'] = 'page_metadata'
                        break
            return doc, "page_text", body
        except Exception as exc:
            return doc, "fetch_failed", f"{type(exc).__name__}: {exc}"[:300]
    with futures.ThreadPoolExecutor(max_workers=6) as pool:
        for doc, state, body in pool.map(one, pending[:limit]):
            doc["extraction_status"] = state
            doc["extraction_attempted_at"] = stamp()
            doc['extraction_attempts'] = doc.get('extraction_attempts', 0) + 1
            if state == "page_text":
                doc["body"] = body  # preserve all extracted text, no 800-char loss
            else:
                doc["extraction_error"] = body
            counts[state] += 1
    counts["google_link_unresolved"] = sum("news.google.com/" in d.get("url", "") for d in docs)
    counts["pending"] = max(0, len(pending)-limit)
    counts['extraction_dead_letter'] = sum(d.get('extraction_attempts',0) >= 3 and d.get('extraction_status') != 'page_text' for d in docs)
    return dict(counts)


def write_json(path: Path, blob) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(blob, ensure_ascii=False, indent=2) + "\n")
    temp.replace(path)


def render(report: dict, rows: list[dict], health: list[dict]) -> str:
    lines = [f"# Free news intake — {report['date']}", "", f"Generated: {report['generated_at']}", "",
             "This is a free deterministic first pass. Candidate classes are search/lexical hints, not JEV decisions or verified Lane analysis.", "",
             f"Documents: **{len(rows)}**; dated candidates: **{report['candidate_count']}**; review: **{report['review_count']}**.",
             f"Coverage status: **{report['coverage_status']}**. No claim of all-news recall. Failed, saturated and undated sources remain visible.", "",
             "## Themes", "", "Counts are distinct normalized headlines, not verified events.", ""]
    for theme, count in report.get('theme_counts', {}).items():
        lines.append(f"- **{theme}:** {count}")
    lines += ["", "## Candidate event classes", "", "Counts are documents, not independently verified unique events.", ""]
    for cls, count in report["class_counts"].items():
        lines += [f"### {cls} — {count}", ""]
        sample = [r for r in rows if cls in r["candidate_classes"]]
        sample.sort(key=lambda r: (r["state"] != "candidate", not bool(r["published_at"])))
        seen = set()
        for row in sample:
            key = norm(row["title"].rsplit(" - ", 1)[0])
            if key in seen:
                continue
            seen.add(key)
            title = row["title"].replace("\n", " ").replace("[", "(").replace("]", ")")
            label = f"[{title}]({row['url']})" if row["url"] else title
            lines.append(f"- {label} — {row['state']}; {row['published_at'] or 'publication unknown'}; {row['source']}")
            if len(seen) == 8:
                break
        lines.append("")
    lines += ["## Source health", "", "| Source | State | Fetched | In window | Issue |", "|---|---|---:|---:|---|"]
    for item in health:
        issue = item.get("error", "") or ("result cap reached" if item.get("saturated") else "")
        lines.append(f"| {item['source']} | {item['status']} | {item.get('fetched',0)} | {item.get('in_window','')} | {issue.replace('|','/')} |")
    return "\n".join(lines) + "\n"


def run(root: Path, date: str, force: bool = False, workers: int = 12, extract: int = 40,
        hours: int = 48, now: datetime | None = None, specs: list[dict] | None = None,
        parse_only: bool = False, quick: bool = False) -> dict:
    now = now or datetime.now(timezone.utc)
    now_text = stamp(now)
    day = datetime.fromisoformat(date).replace(tzinfo=ZoneInfo("Asia/Hong_Kong"))
    end = min(now, day + timedelta(days=1))
    start = end - timedelta(hours=hours)
    folder = root / "data/news_intake" / date
    ledger_path = folder / "documents.json"
    previous = json.loads(ledger_path.read_text()) if ledger_path.exists() else []
    state_path = root / "data/news_intake/source_state.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    sources = specs if specs is not None else registry(root)
    due, health = [], []
    for spec in sources:
        prior = state.get(spec["id"], {})
        last = parse_time(prior.get("checked_at", ""))
        if not parse_only and (force or not last or (now-last).total_seconds() >= spec.get("interval_minutes",15)*60):
            due.append(spec | {'quick': quick})
        else:
            health.append(prior | {"source": spec['id'], "kind": spec['kind'],
                                  "status": prior.get('status', 'not_polled'),
                                  "scheduled_status": "parse_only" if parse_only else "not_due"})
    incoming = []
    with futures.ThreadPoolExecutor(max_workers=workers) as pool:
        tasks = {pool.submit(collect_source, s, start, end): s for s in due}
        for task in futures.as_completed(tasks):
            rows, item = task.result()
            incoming.extend(rows)
            health.append(item)
            state[item["source"]] = item
            print(f"[intake] {item['source']} {item['status']} {len(rows)}", flush=True)
    local, local_health = local_rows(root, date)
    incoming.extend(local)
    health.extend(local_health)
    docs = merge_documents(previous, incoming, now_text)
    # Preserve first discovery across daily ledgers. Only today's selected
    # documents carry forward; prior-day material is not silently reharvested.
    known = {}
    for previous_day in (day - timedelta(days=1), day - timedelta(days=2)):
        old_path = root / 'data/news_intake' / previous_day.date().isoformat() / 'documents.json'
        if old_path.exists():
            for old in json.loads(old_path.read_text()):
                known[old['id']] = min(known.get(old['id'], old['first_seen']), old['first_seen'])
    for doc in docs:
        if doc['id'] in known:
            doc['first_seen'] = min(doc['first_seen'], known[doc['id']])
    # Raw evidence lands before potentially slow extraction or parsing.
    write_json(ledger_path, docs)
    write_json(state_path, state)
    extraction = extract_documents(docs, extract) if extract and not parse_only else {"not_attempted": len(docs)}
    parsed = [first_pass(doc, now) for doc in docs]
    headline_groups = defaultdict(list)
    for doc in docs:
        headline_groups[norm(doc['title'].rsplit(' - ', 1)[0])].append(doc['id'])
    counts = Counter(cls for row in parsed for cls in row["candidate_classes"])
    theme_groups = defaultdict(set)
    for row in parsed:
        for theme in row['themes']:
            theme_groups[theme].add(norm(row['title'].rsplit(' - ', 1)[0]))
    missing = [s["source"] for s in health if s["status"] != "ok"]
    report = {"version": VERSION, "date": date, "generated_at": stamp(), "window_start": stamp(start), "window_end": stamp(end),
              "collection_profile": 'parse_only' if parse_only else ('quick' if quick else 'deep'),
              "document_count": len(docs), "incoming_observations": len(incoming),
              "candidate_count": sum(r["state"] == "candidate" for r in parsed),
              "review_count": sum(r["state"] == "review" for r in parsed),
              "dated_count": sum(r["date_status"] == "dated" for r in parsed),
              "today_hk_count": sum(bool(d.get("published_at")) and parse_time(d["published_at"]).astimezone(ZoneInfo("Asia/Hong_Kong")).date().isoformat() == date for d in docs),
              "class_counts": dict(counts.most_common()), "extraction": extraction,
              "source_count": len(sources), "polled_sources": len(due), "coverage_status": "degraded" if missing else "sources_polled_not_recall_verified",
              "unique_headline_count": len(headline_groups),
              "theme_counts": dict(sorted(((k,len(v)) for k,v in theme_groups.items()), key=lambda kv:-kv[1])),
              "source_issues": missing, "paid_api_calls": 0, "llm_calls": 0,
              "final_classification": False, "event_clustering": "exact normalized headline only; semantic clustering pending",
              "sources": sorted(health, key=lambda h:h["source"]), "all_items": parsed}
    write_json(ledger_path, docs)
    write_json(state_path, state)
    write_json(folder / "parsed.json", report)
    write_json(folder / "headline_groups.json", dict(headline_groups))
    write_json(folder / "review_queue.json", [{k:r[k] for k in ('document_id','state','candidate_classes','review_reasons')}
                                              for r in parsed if r["state"] == "review" or r['review_reasons']])
    # Every article, not only candidates, is available to stronger downstream analysis.
    write_json(folder / "lane_queue.json", [{"document_id": d["id"], "title": d["title"],
               "evidence_file": 'documents.json', "parse_file": 'parsed.json',
               "url": d.get("url", ""), "published_at": d.get("published_at", ""),
               "known_at": d.get("published_at") or d["first_seen"], "candidate_classes": p["candidate_classes"],
               "state": p["state"], "status": "pending_analysis",
               "analyst_family": p['analyst_family'],
               "routing_class": p['routing_class'], "routing_basis": p['routing_basis'],
               "prompt_version": PROMPT_VERSION} for d,p in zip(docs,parsed)])
    news = root / "01_daily/news"
    compact = [{k:r[k] for k in ('document_id','title','url','source','published_at','published_raw','first_seen','last_seen','parsed_at','publisher','publisher_domain','discovery_paths','state','candidate_classes','lane_rule_classification','routing_class','routing_basis','final_lane_class','themes','review_reasons','extraction_status')} for r in parsed]
    write_json(news / f"{date}_intake_parsed.json", report | {'all_items': compact})
    (news / f"{date}_intake.md").write_text(render(report, parsed, health))
    dash = root / "dashboard/news-intake"
    write_json(dash / "latest.json", {k:v for k,v in report.items() if k != "all_items"})
    write_json(dash / "articles.json", compact)
    run_report = {k:v for k,v in report.items() if k != 'all_items'}
    write_json(folder / 'runs' / (datetime.now(timezone.utc).strftime('%H%M%S') + '.json'), run_report)
    return report


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=datetime.now(ZoneInfo("Asia/Hong_Kong")).date().isoformat())
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--parse-only", action="store_true", help="Reparse saved evidence; no remote requests")
    ap.add_argument("--quick", action="store_true", help="Publish first results before slow overflow/backfill sweeps")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--extract", type=int, default=40)
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--context-vault", type=Path, help="CompanyResearch checkout; run exposure/Lane research after intake")
    ap.add_argument("--context-model", help="Optional installed local Ollama model; rules otherwise")
    ap.add_argument("--context-limit", type=int, help="Explicit cap for a bounded context check")
    args = ap.parse_args()
    if args.context_model and not args.context_vault:
        ap.error("--context-model requires --context-vault")
    if args.context_limit is not None and args.context_limit < 1:
        ap.error("--context-limit must be positive")
    datetime.fromisoformat(args.date)
    if not 1 <= args.workers <= 24 or not 1 <= args.hours <= 168 or args.extract < 0:
        ap.error("workers 1..24, hours 1..168, extract >=0")
    if args.parse_only:
        args.extract = 0
    report = run(ROOT, args.date, args.force, args.workers, args.extract, args.hours, parse_only=args.parse_only, quick=args.quick)
    from .news_intake_review_export import export_review_request
    report['review_export'] = export_review_request(ROOT)
    if args.context_vault:
        from news_context_check import run_context
        report["news_context"] = run_context(ROOT, args.date, args.context_vault,
                                              args.context_model, args.context_limit)
        write_json(ROOT / "data/news_intake" / args.date / "parsed.json", report)
        write_json(ROOT / "dashboard/news-intake/latest.json",
                   {k:v for k,v in report.items() if k != "all_items"})
    print(json.dumps({k:v for k,v in report.items() if k not in {"all_items", "sources"}}, indent=2))
    if not report["document_count"]:
        raise SystemExit("No documents collected")


if __name__ == "__main__":
    main()
