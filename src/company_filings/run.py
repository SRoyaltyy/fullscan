"""Fetch the selected filings for one company and write the section store.

The annual report is always stored. Later documents are stored while the
company's non-annual text stays under the byte budget. A second copy of a
dual-class CIK is not fetched.
"""
from __future__ import annotations

import gzip
from pathlib import Path

from .edgar import Client, EdgarBlocked, EdgarError, archive_url, exhibit_rows
from .extract import EXTRACTOR_VERSION, EXHIBIT_CAP, _cap, html_to_text, sections_for, source_points
from .select import (
    ANNUAL_AMENDMENTS,
    ANNUAL_FORMS,
    FOREIGN_FORMS,
    FUND_ANNUAL_AMENDMENTS,
    FUND_ANNUAL_FORMS,
    QUARTERLY_FORMS,
    REGISTRATION_AMENDMENTS,
    REGISTRATION_FORMS,
    filings_from_columnar,
    fingerprint,
    is_annual_form,
    needs_exhibits,
    priority,
    select_filings,
)
from .store import Store, sha256

# Gzipped prose for 10-Qs, 8-Ks, and the one exhibit we keep. The annual
# report is extra and is always stored. ~2,000 companies at this cap is
# about 1 GB plus the annual reports, which is the storage budget.
STRONG_FORMS = (
    ANNUAL_FORMS | ANNUAL_AMENDMENTS
    | REGISTRATION_FORMS | REGISTRATION_AMENDMENTS
    | FUND_ANNUAL_FORMS | FUND_ANNUAL_AMENDMENTS
)
OTHER_BUDGET_BYTES = 500_000
EX99_MAX_BYTES = 1_500_000
EX10_MAX_BYTES = 1_000_000
DB_STOP_BYTES = 8_000_000_000


def _columnar_blocks(payload: dict) -> list[dict]:
    filings = payload.get("filings") or {}
    recent = filings.get("recent")
    if recent:
        return [recent]
    if payload.get("form"):
        return [payload]
    return []


def _load_rows(client: Client, payload: dict) -> list[dict]:
    rows: list[dict] = []
    for block in _columnar_blocks(payload):
        rows.extend(filings_from_columnar(block))
    if any(is_annual_form(row["form"]) for row in rows):
        return rows
    for extra in (payload.get("filings") or {}).get("files") or []:
        name = extra.get("name") or ""
        if not name:
            continue
        try:
            more = client.submissions_file(name)
        except EdgarError:
            continue
        rows.extend(filings_from_columnar(more if more.get("form") else (more.get("filings") or {}).get("recent") or {}))
        if any(is_annual_form(row["form"]) for row in rows):
            break
    return rows


def _gzip_len(text: str) -> int:
    return len(gzip.compress(text.encode("utf-8"), compresslevel=6))


def _attach_exhibits(client: Client, cik: str, filing: dict, sections: dict[str, str]) -> list[dict]:
    if not needs_exhibits(filing):
        return []
    try:
        index = client.index(cik, filing["accession"])
    except EdgarBlocked:
        raise
    except EdgarError:
        return []
    rows = exhibit_rows(index, cik, filing["accession"])
    want_99 = True
    want_10 = True
    for row in rows:
        kind = row["type"]
        take = None
        if want_99 and kind.startswith("EX-99") and row["size"] <= EX99_MAX_BYTES and not row["filename"].lower().endswith(".pdf"):
            take = "ex-99"
            want_99 = False
        elif want_10 and kind.startswith("EX-10") and row["size"] <= EX10_MAX_BYTES and not row["filename"].lower().endswith(".pdf"):
            take = "ex-10"
            want_10 = False
        elif kind.startswith("EX-99") or kind.startswith("EX-10"):
            row["skip_reason"] = "pdf" if row["filename"].lower().endswith(".pdf") else "over_size_or_extra"
        else:
            row["skip_reason"] = "not_kept"
        if take is None:
            continue
        try:
            raw = client.document(cik, filing["accession"], row["filename"])
        except EdgarBlocked:
            raise
        except EdgarError as exc:
            row["skip_reason"] = str(exc)[:180]
            continue
        text = html_to_text(raw)
        if text.strip():
            sections[take] = _cap(text, EXHIBIT_CAP)
            row["stored"] = True
            row["skip_reason"] = ""
    return rows


def parse_company(company: dict, client: Client, store: Store, force: bool = False) -> str:
    """Parse one CIK. Returns ok, skip, no_filing, or error."""
    cik = company["cik"]
    ticker = company["ticker"]
    try:
        payload = client.submissions(cik)
    except EdgarBlocked:
        raise
    except EdgarError as exc:
        store.mark(company, "error", str(exc))
        return "error"
    name = payload.get("name") or company.get("name") or ""
    company = {**company, "name": name}
    rows = _load_rows(client, payload)
    chosen = select_filings(rows)
    if not chosen:
        store.mark(company, "no_filing", "no annual report, fund report, registration, or 10-Q")
        return "no_filing"
    digest = fingerprint(chosen)
    if not force and store.fingerprint(ticker) == digest:
        # A contents-page extract is stored under the same accession list. Read it
        # again once after the splitter learns the "Part I. Item 1" header.
        narrative_forms = tuple(STRONG_FORMS | QUARTERLY_FORMS | FOREIGN_FORMS)
        already = store.extractor(ticker) == EXTRACTOR_VERSION
        if already or not store.base_is_thin(ticker, narrative_forms):
            return "skip"
    chosen.sort(key=priority)
    selected_forms = {filing["form"] for filing in chosen}
    if selected_forms & STRONG_FORMS:
        required = STRONG_FORMS
    elif selected_forms & QUARTERLY_FORMS:
        required = QUARTERLY_FORMS
    else:
        required = FOREIGN_FORMS
    docs = []
    other_bytes = 0
    for filing in chosen:
        if not filing.get("primary") or not filing.get("accession"):
            continue
        form = filing["form"]
        protected = form in required
        try:
            raw = client.document(cik, filing["accession"], filing["primary"])
        except EdgarBlocked:
            raise
        except EdgarError:
            continue
        text = html_to_text(raw)
        sections, status = sections_for(form, text)
        exhibits = _attach_exhibits(client, cik, filing, sections)
        if not sections:
            continue
        size = sum(_gzip_len(value) for value in sections.values())
        if not protected and other_bytes + size > OTHER_BUDGET_BYTES:
            continue
        points = {item: [(point.ordinal, point.start, point.end) for point in source_points(value)] for item, value in sections.items()}
        docs.append({
            "accession": filing["accession"],
            "form": form,
            "filing_date": filing.get("filing_date") or "",
            "report_date": filing.get("report_date") or "",
            "items": filing.get("items") or "",
            "url": archive_url(cik, filing["accession"], filing["primary"]),
            "sha256": sha256(raw),
            "split_status": status,
            "sections": sections,
            "points": points,
            "exhibits": exhibits,
        })
        if not protected:
            other_bytes += size
    kept_forms = {doc["form"] for doc in docs}
    if not docs or not (kept_forms & required):
        store.mark(company, "error", "filings selected but the annual report could not be read")
        return "error"
    company = {**company, "fingerprint": digest, "extractor": EXTRACTOR_VERSION}
    store.replace_company(company, docs)
    return "ok"


def db_over_budget(path: Path) -> bool:
    return path.exists() and path.stat().st_size > DB_STOP_BYTES
