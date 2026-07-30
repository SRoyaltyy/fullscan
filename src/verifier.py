"""Citation verifier for the outcome stage. Every claim must carry
URL / PUBLISHED / QUOTE / SUMMARY. Each URL is fetched (best effort) and the
quote checked for containment. Non-blocking: results are appended as a
verification table; unverified claims are flagged, not deleted.
"""
from __future__ import annotations

import re

import requests

CLAIM_RE = re.compile(
    r"CLAIM:\s*(?P<claim>.+?)\s*\n\s*URL:\s*(?P<url>\S+)\s*\n\s*PUBLISHED:\s*"
    r"(?P<pub>.+?)\s*\n\s*QUOTE:\s*(?P<quote>.+?)\s*\n\s*SUMMARY:\s*"
    r"(?P<summary>.+?)(?=\n\s*(?:CLAIM:|OUTCOME_BEGIN|$))", re.S)


def extract_claims(text: str) -> list[dict]:
    return [m.groupdict() for m in CLAIM_RE.finditer(text)]


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().lower()


def verify_claim(claim: dict, timeout: int = 8) -> str:
    url = claim.get("url", "")
    if not url.startswith("http"):
        return "invalid_url"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"},
                         timeout=timeout)
        if r.status_code >= 400:
            return f"http_{r.status_code}"
        body = _norm(re.sub(r"<[^>]+>", " ", r.text[:400_000]))
        quote = _norm(claim.get("quote", ""))
        if quote and quote[:200] in body:
            return "verified"
        return "quote_not_found"
    except requests.RequestException:
        return "unreachable"


def verify_outcome(text: str) -> tuple[list[dict], str]:
    """Return (claims with status, markdown verification table)."""
    claims = extract_claims(text)
    for c in claims:
        c["status"] = verify_claim(c)
    lines = ["", "---", "## Citation verification (pipeline)",
             "", "| # | claim | url | status |", "|---|---|---|---|"]
    for i, c in enumerate(claims, 1):
        lines.append(f"| {i} | {c['claim'][:80]} | {c['url'][:60]} | {c['status']} |")
    if not claims:
        lines.append("| - | NO STRUCTURED CLAIMS FOUND | - | FAILED |")
    return claims, "\n".join(lines)
