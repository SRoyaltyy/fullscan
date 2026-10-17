"""Choose which EDGAR filings belong in the company record.

The annual report is the base. Quarterly reports and business-changing 8-Ks
are kept only when they were filed after that annual report. A registration
statement is kept only when the issuer has no annual report yet.
"""
from __future__ import annotations

import hashlib
import re

ANNUAL_FORMS = frozenset({"10-K", "20-F", "40-F"})
ANNUAL_AMENDMENTS = frozenset({"10-K/A", "20-F/A", "40-F/A"})
QUARTERLY_FORMS = frozenset({"10-Q", "10-Q/A"})
REGISTRATION_FORMS = frozenset({"S-1", "F-1"})
REGISTRATION_AMENDMENTS = frozenset({"S-1/A", "F-1/A"})
CURRENT_FORMS = frozenset({"8-K", "8-K/A"})

# These items change what the company is, or publish the earnings release
# that arrives before the 10-Q.
KEEP_8K_ITEMS = frozenset({"1.01", "2.01", "2.02", "2.05"})
# Included only when the filing's own description says it discloses a
# contract, a place, or a product. A bare regulation-FD item is not kept.
CONDITIONAL_8K_ITEMS = frozenset({"7.01", "8.01"})

_DISCLOSURE = re.compile(
    r"\b(agreement|contract|award|customer|facility|plant|site|acquisition|"
    r"acquir|product|launch|lease|licen[cs]e|manufactur)\b",
    re.I,
)
_ITEM_SPLIT = re.compile(r"\s*,\s*|\s*;\s*")


def parse_items(raw: str) -> set[str]:
    return {part.strip() for part in _ITEM_SPLIT.split(raw or "") if part.strip()}


def keep_8k(filing: dict) -> bool:
    items = parse_items(filing.get("items") or "")
    if items & KEEP_8K_ITEMS:
        return True
    if items & CONDITIONAL_8K_ITEMS and _DISCLOSURE.search(filing.get("description") or ""):
        return True
    return False


def _annual(filing: dict) -> bool:
    return filing.get("form") in ANNUAL_FORMS


def priority(filing: dict) -> tuple:
    """Lower sorts first. The annual report is never the document we drop."""
    form = filing.get("form") or ""
    items = parse_items(filing.get("items") or "")
    if form in ANNUAL_FORMS:
        rank = 0
    elif form in ANNUAL_AMENDMENTS:
        rank = 1
    elif form in QUARTERLY_FORMS:
        rank = 2
    elif form in REGISTRATION_FORMS:
        rank = 3
    elif form in REGISTRATION_AMENDMENTS:
        rank = 4
    elif items & {"1.01", "2.01", "2.05"}:
        rank = 5
    elif "2.02" in items:
        rank = 6
    else:
        rank = 7
    # Newest first inside a rank, so a byte budget keeps the latest quarter.
    return (rank, _date_key(filing.get("filing_date") or ""), filing.get("accession") or "")


def _date_key(value: str) -> tuple:
    parts = []
    for part in (value or "").split("-"):
        if part.isdigit():
            parts.append(-int(part))
    return tuple(parts) or (0,)


def select_filings(rows: list[dict]) -> list[dict]:
    """Return the filings that are stored for one company."""
    ordered = sorted(rows, key=lambda row: (row.get("filing_date") or "", row.get("accession") or ""), reverse=True)
    annual = next((row for row in ordered if _annual(row)), None)
    chosen: list[dict] = []
    if annual is not None:
        chosen.append(annual)
        annual_date = annual.get("filing_date") or ""
        annual_period = annual.get("report_date") or ""
        for row in ordered:
            form = row.get("form")
            filed = row.get("filing_date") or ""
            if form in ANNUAL_AMENDMENTS and filed >= annual_date and (row.get("report_date") or "") == annual_period:
                chosen.append(row)
            elif form in QUARTERLY_FORMS and filed > annual_date:
                chosen.append(row)
            elif form in CURRENT_FORMS and filed > annual_date and keep_8k(row):
                chosen.append(row)
        return _unique(chosen)
    registration = next((row for row in ordered if row.get("form") in REGISTRATION_FORMS), None)
    if registration is None:
        return []
    chosen.append(registration)
    filed_after = registration.get("filing_date") or ""
    for row in ordered:
        if row.get("form") in REGISTRATION_AMENDMENTS and (row.get("filing_date") or "") >= filed_after:
            chosen.append(row)
    return _unique(chosen)


def _unique(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for row in rows:
        key = row.get("accession") or ""
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def fingerprint(rows: list[dict]) -> str:
    body = "\n".join(sorted(f"{row.get('accession')}|{row.get('form')}" for row in rows))
    return hashlib.sha256(body.encode()).hexdigest()[:20]


def needs_exhibits(filing: dict) -> bool:
    """Earnings releases and material agreements live in exhibits, not the 8-K shell."""
    if filing.get("form") not in CURRENT_FORMS:
        return False
    items = parse_items(filing.get("items") or "")
    return bool(items & {"1.01", "2.01", "2.02", "2.05", "7.01", "8.01"})


def filings_from_columnar(block: dict) -> list[dict]:
    """EDGAR submissions store filings as parallel arrays."""
    if not block:
        return []
    forms = block.get("form") or []
    count = len(forms)
    def col(name: str) -> list:
        values = block.get(name) or []
        if len(values) < count:
            return list(values) + [""] * (count - len(values))
        return values
    accessions = col("accessionNumber")
    dates = col("filingDate")
    periods = col("reportDate")
    documents = col("primaryDocument")
    descriptions = col("primaryDocDescription")
    items = col("items")
    out = []
    for i in range(count):
        out.append({
            "form": forms[i] or "",
            "accession": accessions[i] or "",
            "filing_date": dates[i] or "",
            "report_date": periods[i] or "",
            "primary": documents[i] or "",
            "description": descriptions[i] or "",
            "items": items[i] or "",
        })
    return out
