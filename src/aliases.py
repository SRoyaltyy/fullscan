"""Typed aliases and jurisdictions taken from a company profile.

An alias hit is closer inspection, never a pass by itself. A pass still
needs description overlap or an industry vote that is not just the alias.
A place name scopes the companies whose profile mentions that place. A
place by itself never passes anyone.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass

from .names import english_words
from .places import CITIES, STATES

_ACRONYM_DENY = frozenset("""
us usa sec fda irs ceo cfo coo cto gapp gaap ai api gpu cpu etf ipo
nyse nasdaq eu uk un pr doj fbi cia dea hhs cdc nih va nc tx ca
""".split())

_FOUNDED = re.compile(r"founded by (.+?)(?:\s+on\b|\s+in\b)", re.I)
_PERSON = re.compile(r"[A-Z][a-z]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][a-z]+){1,3}")
_CAMEL = re.compile(r"\b([A-Z][a-z0-9]*[A-Z][A-Za-z0-9]{2,})\b")
_ACRONYM = re.compile(r"\b([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,5})\s+\(\"?([A-Z][A-Z0-9]{1,7})\"?\)")
_DBA = re.compile(r"doing business as ([A-Z][A-Za-z0-9&.'’ -]{2,40})")
_COUNTRIES = (
    "canada", "united kingdom", "mexico", "china", "japan", "germany",
    "france", "australia", "brazil", "india", "denmark",
)
_CAROLINAS = ("north carolina", "south carolina")


@dataclass(frozen=True)
class Alias:
    ticker: str
    kind: str
    text: str


def extract_aliases(ticker: str, name: str, profile: str) -> list[Alias]:
    """Names, products, people, and filing acronyms from one profile."""
    ticker = ticker.upper()
    found: list[Alias] = []
    seen: set[tuple[str, str]] = set()

    def add(kind: str, text: str) -> None:
        cleaned = re.sub(r"\s+", " ", text or "").strip(" .,;")
        key = (kind, cleaned.lower())
        if len(cleaned) < 3 or key in seen:
            return
        if kind not in {"name", "person"} and cleaned.lower() in english_words() and " " not in cleaned:
            return
        seen.add(key)
        found.append(Alias(ticker, kind, cleaned))

    if name:
        add("name", name)
    for match in _DBA.finditer(profile or ""):
        add("name", match.group(1))
    for match in _FOUNDED.finditer(profile or ""):
        for person in _PERSON.findall(match.group(1)):
            add("person", person)
            # The surname is the alias people actually write. "Musk" is also
            # an English word; it still counts because the profile named the person.
            surname = person.split()[-1]
            if len(surname) >= 4:
                add("person", surname)
    for match in _CAMEL.finditer(profile or ""):
        add("product", match.group(1))
    for match in _ACRONYM.finditer(profile or ""):
        acronym = match.group(2)
        if acronym.lower() in _ACRONYM_DENY:
            continue
        add("acronym", acronym)
    return found


def extract_jurisdictions(profile: str) -> set[str]:
    """States, countries, and a few region names a profile actually uses."""
    low = profile or ""
    low = low.lower()
    found: set[str] = set()
    for state in STATES:
        if re.search(rf"\b{re.escape(state)}\b", low):
            found.add(state)
    for city in CITIES:
        if re.search(rf"\b{re.escape(city)}\b", low):
            found.add(city)
    for country in _COUNTRIES:
        if re.search(rf"\b{re.escape(country)}\b", low):
            found.add(country)
    if re.search(r"\bcarolinas\b", low):
        found.update(_CAROLINAS)
        found.add("carolinas")
    # "Charlotte, NC" is a jurisdiction even though the state is abbreviated.
    _ABBREV = {
        "nc": "north carolina", "sc": "south carolina", "tx": "texas", "ny": "new york",
        "ca": "california", "fl": "florida", "oh": "ohio", "ky": "kentucky",
        "ga": "georgia", "va": "virginia", "tn": "tennessee", "wa": "washington",
    }
    for abbrev, state in _ABBREV.items():
        if re.search(rf",\s*{abbrev}\b", low):
            found.add(state)
    return found


def profiles_from_csv(path) -> list[dict]:
    """Every Finviz description, so off-board companies can still be scoped or aliased."""
    import csv
    from pathlib import Path
    profiles = []
    with Path(path).open(newline="", encoding="utf-8", errors="replace") as handle:
        for row in csv.DictReader(handle):
            ticker = str(row.get("Ticker") or row.get("ticker") or "").upper().strip()
            if not ticker:
                continue
            profiles.append({
                "ticker": ticker,
                "name": str(row.get("Company") or row.get("name") or ""),
                "industry": str(row.get("Industry") or row.get("industry") or ""),
                "sector": str(row.get("Sector") or row.get("sector") or ""),
                "description": str(row.get("Finviz_Description") or row.get("description") or ""),
            })
    return profiles


class AliasIndex:
    """Mention scan over the alias graph. Tickers come back with a kind."""

    def __init__(self, aliases: list[Alias]):
        self.by_word: dict[str, list[Alias]] = defaultdict(list)
        self.by_first: dict[str, list[tuple[tuple[str, ...], Alias]]] = defaultdict(list)
        for alias in aliases:
            parts = tuple(alias.text.lower().split())
            if not parts:
                continue
            if len(parts) == 1:
                self.by_word[parts[0]].append(alias)
            else:
                self.by_first[parts[0]].append((parts, alias))

    def mentioned(self, text: str) -> list[Alias]:
        if not text:
            return []
        tokens = [match.group(0).lower() for match in re.finditer(r"[A-Za-z][A-Za-z0-9'&.-]*", text)]
        tokens = [token[:-2] if token.endswith("'s") and len(token) > 3 else token for token in tokens]
        found: dict[tuple[str, str], Alias] = {}
        for index, token in enumerate(tokens):
            for alias in self.by_word.get(token, ()):
                found[(alias.ticker, alias.text.lower())] = alias
            for parts, alias in self.by_first.get(token, ()):
                if tuple(tokens[index:index + len(parts)]) == parts:
                    found[(alias.ticker, alias.text.lower())] = alias
        return list(found.values())


def alias_terms(aliases: list[Alias]) -> set[str]:
    """Tokens that are the alias itself. Description credit from only these is not corroboration."""
    terms: set[str] = set()
    for alias in aliases:
        if alias.kind == "name":
            continue
        for part in alias.text.lower().split():
            if len(part) >= 3:
                terms.add(part)
        terms.add(alias.text.lower())
    return terms
