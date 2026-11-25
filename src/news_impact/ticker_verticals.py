"""Link a stated activity to listed tickers in that vertical.

The book is the Finviz export: Sector, Industry, Country, Market Cap.
A vertical is one Industry, not the whole sector ETF. Integrated oil
companies stay in their own row because they produce and refine.
Country exposure is not in this book, so a sanction with no named
activity does not become Exxon.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

from .finviz_linker import latest_finviz_csv

# Activity the title states -> (chain, role). First match wins.
CUES: list[tuple[re.Pattern[str], str, str]] = [
    (re.compile(r"(?i)license to drill|\bdrilling\b"), "crude", "driller"),
    (re.compile(r"(?i)oil producers?|upstream|\be\s*&\s*p\b"), "crude", "producer"),
    (re.compile(r"(?i)refin(?:er|ing|ery)"), "crude", "refiner"),
    (re.compile(r"(?i)midstream|pipeline transportation|oil transporter"), "crude", "midstream"),
    (re.compile(r"(?i)oilfield service|equipment and services"), "crude", "services"),
    (re.compile(r"(?i)dissolving pulp|paper products"), "pulp", "producer"),
]

# Role -> Finviz Industry names. Order is the preference inside the role.
CHAINS: dict[str, dict[str, list[str]]] = {
    "crude": {
        "producer": ["Oil & Gas E&P"],
        "integrated": ["Oil & Gas Integrated"],
        "refiner": ["Oil & Gas Refining & Marketing"],
        "midstream": ["Oil & Gas Midstream"],
        "services": ["Oil & Gas Equipment & Services"],
        "driller": ["Oil & Gas Drilling"],
    },
    "pulp": {
        "producer": ["Paper & Paper Products"],
        "packaging": ["Packaging & Containers"],
    },
    "banks": {
        "diversified": ["Banks - Diversified"],
        "regional": ["Banks - Regional"],
    },
    "semis": {
        "designer": ["Semiconductors"],
        "equipment": ["Semiconductor Equipment & Materials"],
    },
    "pharma": {
        "large_cap": ["Drug Manufacturers - General"],
        "specialty": ["Drug Manufacturers - Specialty & Generic"],
        "biotech": ["Biotechnology"],
    },
    "insurance": {
        "property": ["Insurance - Property & Casualty"],
        "life": ["Insurance - Life"],
        "broker": ["Insurance Brokers"],
    },
    "defense": {
        "prime": ["Aerospace & Defense"],
    },
}

# A producer call also surfaces integrated majors, marked as mixed.
ROLE_ALSO: dict[tuple[str, str], list[str]] = {
    ("crude", "producer"): ["integrated"],
    ("crude", "driller"): ["producer", "integrated"],
}

_SUFFIX = frozenset(
    "inc incorporated corp corporation ltd limited plc sa nv ag co company "
    "holdings holding group class common stock trust lp llc".split()
)
_GAP = frozenset("of and the for".split())
_GENERIC = frozenset(
    "energy financial technology services capital securities markets health "
    "american united national first general international global power water "
    "medical science digital data bank banks oil gas group resources partners "
    "therapeutics pharmaceuticals biotechnology software systems solutions "
    "bullish bearish rally surge jump producer producers refiner refiners "
    "driller drillers people republic china products wood steel paper pulp "
    "pasta".split()
)


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", (text or "").lower())


def _cap(value: str) -> float:
    try:
        return float(str(value or "").replace(",", ""))
    except ValueError:
        return 0.0


def load_book(path: Path | None = None, root: Path | None = None) -> list[dict]:
    csv_path = path or latest_finviz_csv(root)
    if csv_path is None or not Path(csv_path).is_file():
        return []
    rows = []
    with Path(csv_path).open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            ticker = str(raw.get("Ticker") or "").strip().upper()
            if not ticker or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", ticker):
                continue
            rows.append({
                "ticker": ticker,
                "company": str(raw.get("Company") or "").strip(),
                "sector": str(raw.get("Sector") or "").strip(),
                "industry": str(raw.get("Industry") or "").strip(),
                "country": str(raw.get("Country") or "").strip(),
                "market_cap": _cap(raw.get("Market Cap") or ""),
            })
    return rows


def leaders(book: list[dict], industries: list[str], n: int = 4, usa_only: bool = True) -> list[dict]:
    """Largest names in these industries. USA first when usa_only is set."""
    wanted = set(industries)
    pool = [r for r in book if r["industry"] in wanted and r["market_cap"] > 0]
    if usa_only:
        us = [r for r in pool if r["country"] == "USA"]
        if us:
            pool = us
    pool.sort(key=lambda r: r["market_cap"], reverse=True)
    seen: set[str] = set()
    out = []
    for row in pool:
        if row["ticker"] in seen:
            continue
        seen.add(row["ticker"])
        out.append(row)
        if len(out) >= n:
            break
    return out


def roles_for_text(text: str) -> list[tuple[str, str]]:
    """Roles the title itself states. Empty when the vertical is not in the title."""
    for pattern, chain, role in CUES:
        if pattern.search(text or ""):
            roles = [(chain, role)]
            for extra in ROLE_ALSO.get((chain, role), []):
                roles.append((chain, extra))
            return roles
    return []


def tickers_for_roles(book: list[dict], roles: list[tuple[str, str]], n: int = 4) -> list[dict]:
    out = []
    for chain, role in roles:
        industries = (CHAINS.get(chain) or {}).get(role) or []
        for row in leaders(book, industries, n=n):
            out.append({
                "ticker": row["ticker"],
                "company": row["company"],
                "industry": row["industry"],
                "sector": row["sector"],
                "role": role,
                "chain": chain,
                "basis": "finviz_industry_leaders",
            })
    return out


def _significant(company: str) -> list[str]:
    return [t for t in _tokens(company) if t not in _SUFFIX]


def named_companies(title: str, book: list[dict]) -> list[dict]:
    """Companies whose name is actually in the title. Longest match wins."""
    words = _tokens(title)
    if not words:
        return []
    folded = re.sub(r"[^a-z0-9]", "", (title or "").lower())
    single_owners: dict[str, int] = {}
    for row in book:
        tokens = _significant(row["company"])
        if len(tokens) == 1:
            single_owners[tokens[0]] = single_owners.get(tokens[0], 0) + 1
    hits = []
    for row in book:
        tokens = _significant(row["company"])
        if not tokens:
            continue
        if len(tokens) == 1:
            token = tokens[0]
            if len(token) < 5 or token in _GENERIC or single_owners.get(token, 0) != 1:
                continue
            if _person_name(title, token):
                continue
            if token in words or (len(token) >= 8 and token in folded):
                hits.append((len(token), row))
            continue
        # Multiword names must appear in order. Gaps may only be of/and/the.
        if _phrase(words, tokens):
            hits.append((sum(len(t) for t in tokens), row))
    hits.sort(key=lambda item: item[0], reverse=True)
    seen: set[str] = set()
    out = []
    for _, row in hits:
        if row["ticker"] in seen:
            continue
        seen.add(row["ticker"])
        out.append({
            "ticker": row["ticker"],
            "company": row["company"],
            "industry": row["industry"],
            "sector": row["sector"],
            "role": "named",
            "chain": "",
            "basis": "company_name_in_title",
        })
        if len(out) >= 3:
            break
    return out


def _person_name(title: str, token: str) -> bool:
    """A surname after a given name is a person, not the company that shares it."""
    shown = token[:1].upper() + token[1:]
    return re.search(
        rf"\b[A-Z][a-z]+\s+(?:[A-Z]\.\s+)?{re.escape(shown)}\b",
        title or "",
    ) is not None


def _phrase(words: list[str], tokens: list[str]) -> bool:
    if len(tokens) > len(words):
        return False
    for start in range(len(words)):
        i = start
        matched = 0
        while i < len(words) and matched < len(tokens):
            if words[i] == tokens[matched]:
                matched += 1
                i += 1
                continue
            if matched and words[i] in _GAP:
                i += 1
                continue
            break
        if matched == len(tokens):
            return True
    return False


def link_title(title: str, book: list[dict] | None = None, n: int = 4) -> dict:
    """Named companies first. Otherwise the vertical the title states."""
    book = book if book is not None else load_book()
    named = named_companies(title, book)
    if named:
        return {"tickers": named, "reason": "company named in the title"}
    roles = roles_for_text(title)
    if not roles:
        return {
            "tickers": [],
            "reason": "no named company and no vertical in the title; exposure is not in the ticker book",
        }
    return {
        "tickers": tickers_for_roles(book, roles, n=n),
        "reason": "title states " + ", ".join(f"{chain}:{role}" for chain, role in roles),
    }
