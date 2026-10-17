"""Choose which EDGAR filings belong in the company record.

The annual report is the base. Quarterly reports and business-changing 8-Ks
are kept only when they were filed after that annual report. A registration
statement is kept when the issuer has no annual report yet. A fund's N-CSR
report, a Canadian F-10, a spinoff 10-12B, and a lone 10-Q fill that same
role for issuers that have not filed a 10-K, 20-F, or 40-F.
"""
from __future__ import annotations

import hashlib
import re

ANNUAL_FORMS = frozenset({"10-K", "20-F", "40-F"})
ANNUAL_AMENDMENTS = frozenset({"10-K/A", "20-F/A", "40-F/A"})
QUARTERLY_FORMS = frozenset({"10-Q", "10-Q/A"})
# S-1 and F-1 are the US and foreign IPO filings. F-10 is the Canadian
# long-form prospectus. 10-12B is the spinoff registration. N-2 is the
# closed-end fund registration.
REGISTRATION_FORMS = frozenset({"S-1", "F-1", "F-10", "10-12B", "N-2"})
REGISTRATION_AMENDMENTS = frozenset({"S-1/A", "F-1/A", "F-10/A", "10-12B/A", "N-2/A"})
# Shareholder reports stand in for the annual report of a registered fund.
FUND_ANNUAL_FORMS = frozenset({"N-CSR", "N-CSRS"})
FUND_ANNUAL_AMENDMENTS = frozenset({"N-CSR/A", "N-CSRS/A"})
CURRENT_FORMS = frozenset({"8-K", "8-K/A"})

_CANONICAL_BASE = {
    "10-K": "10-K",
    "10-K405": "10-K",
    "10K405": "10-K",
    "20-F": "20-F",
    "20F": "20-F",
    "40-F": "40-F",
    "40F": "40-F",
    "10-Q": "10-Q",
    "F-10": "F-10",
    "10-12B": "10-12B",
    "10-12G": "10-12B",
    "N-2": "N-2",
    "N-CSR": "N-CSR",
    "N-CSRS": "N-CSRS",
    "S-1": "S-1",
    "F-1": "F-1",
    "8-K": "8-K",
}

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


def canonical_form(form: str) -> str:
    """Map EDGAR's registration suffixes onto the form the selector understands.

    A Canadian 40-F registration is filed as 40FR12B, not 40-F. 20FR12B is the
    same idea for a 20-F. 10-12G is the 12(g) sibling of the 10-12B spinoff
    registration.
    """
    text = (form or "").strip().upper()
    amendment = text.endswith("/A")
    base = text[:-2] if amendment else text
    mapped = _CANONICAL_BASE.get(base)
    if mapped is None:
        if base.startswith("40FR"):
            mapped = "40-F"
        elif base.startswith("20FR"):
            mapped = "20-F"
        elif base.startswith("10-KT"):
            mapped = "10-K"
        elif base.startswith("10-QT"):
            mapped = "10-Q"
        else:
            return text
    if amendment:
        return f"{mapped}/A"
    return mapped


def is_annual_form(form: str) -> bool:
    return canonical_form(form) in ANNUAL_FORMS


def parse_items(raw: str) -> set[str]:
    return {part.strip() for part in _ITEM_SPLIT.split(raw or "") if part.strip()}


def keep_8k(filing: dict) -> bool:
    items = parse_items(filing.get("items") or "")
    if items & KEEP_8K_ITEMS:
        return True
    if items & CONDITIONAL_8K_ITEMS and _DISCLOSURE.search(filing.get("description") or ""):
        return True
    return False


def priority(filing: dict) -> tuple:
    """Lower sorts first. The annual report is never the document we drop."""
    form = filing.get("form") or ""
    items = parse_items(filing.get("items") or "")
    if form in ANNUAL_FORMS or form in FUND_ANNUAL_FORMS:
        rank = 0
    elif form in ANNUAL_AMENDMENTS or form in FUND_ANNUAL_AMENDMENTS:
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


def _normalize(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        copy = dict(row)
        copy["form"] = canonical_form(row.get("form") or "")
        out.append(copy)
    return out


def _latest(ordered: list[dict], forms: frozenset) -> dict | None:
    return next((row for row in ordered if row.get("form") in forms), None)


def _with_later_reports(ordered: list[dict], chosen: list[dict], anchor_date: str, anchor_period: str, amendments: frozenset) -> list[dict]:
    for row in ordered:
        form = row.get("form")
        filed = row.get("filing_date") or ""
        if form in amendments and filed >= anchor_date and (row.get("report_date") or "") == anchor_period:
            chosen.append(row)
        elif form in QUARTERLY_FORMS and filed > anchor_date:
            chosen.append(row)
        elif form in CURRENT_FORMS and filed > anchor_date and keep_8k(row):
            chosen.append(row)
    return _unique(chosen)


def select_filings(rows: list[dict]) -> list[dict]:
    """Return the filings that are stored for one company."""
    ordered = sorted(
        _normalize(rows),
        key=lambda row: (row.get("filing_date") or "", row.get("accession") or ""),
        reverse=True,
    )
    annual = _latest(ordered, ANNUAL_FORMS) or _latest(ordered, ANNUAL_AMENDMENTS)
    if annual is not None:
        return _with_later_reports(
            ordered, [annual], annual.get("filing_date") or "", annual.get("report_date") or "", ANNUAL_AMENDMENTS,
        )
    fund = _latest(ordered, FUND_ANNUAL_FORMS) or _latest(ordered, FUND_ANNUAL_AMENDMENTS)
    if fund is not None:
        return _with_later_reports(
            ordered, [fund], fund.get("filing_date") or "", fund.get("report_date") or "", FUND_ANNUAL_AMENDMENTS,
        )
    return _registration_or_quarters(ordered)


def _registration_or_quarters(ordered: list[dict]) -> list[dict]:
    """Issuers with no annual report yet: the registration, else the 10-Qs."""
    chosen: list[dict] = []
    registration = _latest(ordered, REGISTRATION_FORMS)
    family = [row for row in ordered if row.get("form") in REGISTRATION_FORMS | REGISTRATION_AMENDMENTS]
    anchor = ""
    if registration is not None:
        chosen.append(registration)
        filed_after = registration.get("filing_date") or ""
        for row in family:
            if row.get("form") in REGISTRATION_AMENDMENTS and (row.get("filing_date") or "") >= filed_after:
                chosen.append(row)
        dates = [row.get("filing_date") or "" for row in family if row.get("filing_date")]
        anchor = min(dates) if dates else filed_after
    elif family:
        chosen.extend(family)
        dates = [row.get("filing_date") or "" for row in family if row.get("filing_date")]
        anchor = min(dates) if dates else ""
    quarters = [row for row in ordered if row.get("form") in QUARTERLY_FORMS and (row.get("filing_date") or "") >= anchor]
    if not chosen and not quarters:
        return []
    chosen.extend(quarters)
    for row in ordered:
        filed = row.get("filing_date") or ""
        if row.get("form") in CURRENT_FORMS and filed >= anchor and keep_8k(row):
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
