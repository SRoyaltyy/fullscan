"""Who an article names, kept separate from what it is about.

Three tiers, applied to the raw text before it is lowercased:

1. ``$POST`` or ``NYSE:POST`` is that ticker, even when the ticker is an English word.
2. A multi-word legal name (``Lockheed Martin``) is that company in any case.
3. A one-word name that is also an English word (``post``, ``booking``,
   ``news``) counts only in CamelCase, never from headline title case
   (``IPO News``) and never inside a publisher name (``Washington Post``).
   A one-word name that is not an English word (``DraftKings``) counts in
   any case. A bare all-caps ticker counts even when the ticker is an
   English word (``HOOD``, ``COIN``), unless a publisher name already
   covers that token (``WASHINGTON POST`` does not become ``POST``).
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_WORDS = Path(__file__).resolve().parent / "data" / "english_words.txt"

# Publication names that contain a real ticker or a real company word.
# Longest first so "new york post" is masked before "york post" could matter.
PUBLISHERS = (
    "wall street journal",
    "new york times",
    "washington post",
    "new york post",
    "financial times",
    "renaissance capital",
    "business insider",
    "associated press",
    "yahoo finance",
    "abc news",
    "barron's",
    "barrons",
    "reuters",
    "bloomberg",
    "ap news",
    "associated press",
)

# Bare all-caps abbreviations of government bodies. $IRS still matches.
# ICE is left out: it is also Intercontinental Exchange.
AGENCIES = frozenset("""
irs dea fbi cia hhs fda cdc nih doj dol hud sba ssa usda atf dhs
sec va oig uspis usps usda hud ftc fcc epa
""".split())

# MD before a person's name is a title. $MD still matches.
TITLES = frozenset("md ceo cfo coo cto cao vp".split())

_SOURCE_TAIL = re.compile(r"\s+[-|\u2013|\u2014]\s+\S.{0,80}$")

_TICKER_SYNTAX = re.compile(
    r"(?:\$|(?:NYSE|NASDAQ|NASD|AMEX|NYSEAMERICAN)\s*:\s*)([A-Za-z]{1,5})\b"
)
_CAPS_TICKER = re.compile(r"\b([A-Z]{2,5})\b")
_TOKEN = re.compile(r"[A-Za-z][A-Za-z0-9'&.-]*")


@lru_cache(maxsize=1)
def english_words() -> frozenset[str]:
    if not _WORDS.is_file():
        return frozenset()
    return frozenset(
        line.strip().lower()
        for line in _WORDS.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )


@dataclass(frozen=True)
class Name:
    ticker: str
    name: str
    alias: str
    industry: str = ""
    sector: str = ""


class NameIndex:
    """Lookup built once per board. Mention scans are token walks, not one regex per company."""

    def __init__(self, records: list[Name]):
        self.by_ticker: dict[str, Name] = {}
        self.by_word: dict[str, list[Name]] = {}
        self.by_first: dict[str, list[tuple[tuple[str, ...], Name]]] = {}
        for record in records:
            ticker = record.ticker.upper().strip()
            if not ticker or ticker in self.by_ticker:
                continue
            kept = Name(ticker, record.name, record.alias, record.industry, record.sector)
            self.by_ticker[ticker] = kept
            alias = (record.alias or "").strip().lower()
            if not alias:
                continue
            parts = tuple(alias.split())
            if len(parts) == 1:
                self.by_word.setdefault(parts[0], []).append(kept)
            else:
                self.by_first.setdefault(parts[0], []).append((parts, kept))

    def mentioned(self, text: str) -> list[Name]:
        if not text:
            return []
        words = english_words()
        masked = _masked_token_starts(text)
        found: dict[str, Name] = {}

        for match in _TICKER_SYNTAX.finditer(text):
            record = self.by_ticker.get(match.group(1).upper())
            if record:
                found[record.ticker] = record

        tokens = [(match.group(0), match.start()) for match in _TOKEN.finditer(text)]
        lowered = [_possessive(token).lower() for token, _start in tokens]
        for match in _CAPS_TICKER.finditer(text):
            if match.start() in masked:
                continue
            ticker = match.group(1)
            if ticker.lower() in AGENCIES:
                continue
            # IRS-CI is one code, not the tickers IRS and CI.
            before = text[match.start() - 1] if match.start() else ""
            after = text[match.end():match.end() + 1]
            if before == "-" or after == "-":
                continue
            if ticker.lower() in TITLES:
                # "MD Peter" is a title. "BA, LMT" is a ticker list.
                nxt = _next_word(text, match.end())
                if nxt and nxt[0].isupper() and not nxt.isupper():
                    continue
            # All caps is how a headline writes a ticker, including word-tickers
            # (HOOD, COIN). The publisher mask is what keeps WASHINGTON POST
            # from becoming the cereal company.
            record = self.by_ticker.get(ticker)
            if record:
                found[record.ticker] = record

        for index, (surface, start) in enumerate(tokens):
            if start in masked:
                continue
            surface = _possessive(surface)
            word = lowered[index]
            for record in self.by_word.get(word, ()):
                if record.ticker in found:
                    continue
                # A dictionary word needs a distinctive shape. "booking platform"
                # is not Booking Holdings, and headline title case ("IPO News")
                # is not News Corp. CamelCase (DraftKings) still counts, as
                # does the full multi-word legal name and a $TICKER.
                if word in words and not _camel(surface):
                    continue
                found[record.ticker] = record
            for parts, record in self.by_first.get(word, ()):
                if record.ticker in found:
                    continue
                width = len(parts)
                if lowered[index:index + width] == list(parts):
                    found[record.ticker] = record
        return list(found.values())


def catalog_from_csv(path: Path) -> list[Name]:
    from .correlation import _alias

    records = []
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        for row in csv.DictReader(handle):
            ticker = str(row.get("Ticker") or row.get("ticker") or "").upper().strip()
            name = str(row.get("Company") or row.get("name") or "")
            if not ticker:
                continue
            records.append(Name(
                ticker=ticker,
                name=name,
                alias=_alias(name),
                industry=str(row.get("Industry") or row.get("industry") or ""),
                sector=str(row.get("Sector") or row.get("sector") or ""),
            ))
    return records


def _camel(surface: str) -> bool:
    """True for DraftKings or eBay, false for News, Post, and APPLE."""
    if len(surface) < 2:
        return False
    return any(char.isupper() for char in surface[1:]) and any(char.islower() for char in surface)


def _possessive(token: str) -> str:
    """Moderna's and Moderna’s are Moderna."""
    for mark in ("'s", "\u2019s"):
        if len(token) > len(mark) + 1 and token.endswith(mark):
            return token[: -len(mark)]
    return token


def _next_word(text: str, end: int) -> str:
    match = _TOKEN.search(text, end)
    return match.group(0) if match else ""


@lru_cache(maxsize=1)
def _place_phrases() -> frozenset[str]:
    from .places import place_names
    return frozenset(place.lower() for place in place_names())


def _masked_token_starts(text: str) -> set[int]:
    """Offsets of tokens inside a publisher, a trailing source, a place, or a dateline."""
    lowered = text.lower()
    spans: list[tuple[int, int]] = []
    for phrase in PUBLISHERS:
        start = 0
        while True:
            found = lowered.find(phrase, start)
            if found < 0:
                break
            spans.append((found, found + len(phrase)))
            start = found + len(phrase)
    offset = 0
    for line in text.splitlines(keepends=True):
        match = _SOURCE_TAIL.search(line.rstrip("\r\n"))
        if match:
            spans.append((offset + match.start(), offset + match.end()))
        offset += len(line)
    # SAN JUAN, Puerto Rico is a dateline. BA, LMT is a ticker list: one word, then a comma.
    for match in re.finditer(r"\b(?:[A-Z]{2,}\s+){1,3}[A-Z]{2,},", text):
        # Uppercase SEC legal names are not location datelines.
        if re.match(r'\s*(?:INC|CORP|CORPORATION|CO|LLC|LTD|LIMITED)\b',text[match.end():],re.I):
            continue
        spans.append((match.start(), match.end()))
    places = _place_phrases()
    tokens = list(_TOKEN.finditer(text))
    words = [match.group(0).lower() for match in tokens]
    for index, word in enumerate(words):
        for width in (3, 2):
            if index + width > len(words):
                continue
            phrase = " ".join(words[index:index + width])
            if phrase in places:
                spans.append((tokens[index].start(), tokens[index + width - 1].end()))
                break
    if not spans:
        return set()
    masked = set()
    for match in tokens:
        begin = match.start()
        if any(span_start <= begin < span_end for span_start, span_end in spans):
            masked.add(begin)
    return masked

