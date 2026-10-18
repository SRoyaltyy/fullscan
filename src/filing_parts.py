"""Split a filing business section into the core description and labeled partners.

Item 1 mixes what the company sells with names of customers, suppliers, and
collaborators. Those names stay available, but only as an explicit role.
They do not enter the text that is compared with an article.
"""
from __future__ import annotations

import re

_DROP = re.compile(
    r"(competition|corporate history|our history|recent developments|"
    r"human capital|properties|intellectual property|table of contents|"
    r"research and development|backlog)",
    re.I,
)
_ROLE = (
    ("supplier", re.compile(r"\b(suppliers?|subcontractors?|sole source)\b", re.I)),
    ("customer", re.compile(r"\b(customers?|clients?|third[- ]parties|third[- ]party)\b", re.I)),
    ("collaborator", re.compile(r"\b(strategic partners?|partners?|collaborat\w+|licens\w+)\b", re.I)),
)
_LIST = re.compile(
    r"\b(?:including|such as)\s+("
    r"[A-Z][\w&.'’\-]*"
    r"(?:\s+[A-Z][\w&.'’\-]*){0,5}"
    r"(?:\s*,\s*(?:and\s+)?|\s+and\s+)"
    r"[A-Z][\w&.'’\-]*"
    r"(?:(?:\s+[A-Z][\w&.'’\-]*){0,5}"
    r"(?:\s*,\s*(?:and\s+)?|\s+and\s+)){0,8}"
    r"[A-Z][\w&.'’\-]*"
    r"(?:\s+[A-Z][\w&.'’\-]*){0,5}"
    r")"
)
_ORG_SUFFIX = {
    "Inc", "Corp", "Corporation", "Holdings", "Group", "LLC", "Ltd", "SA", "AG", "Co", "LP",
}
_NOT_NAMES = {
    "united states", "u.s.", "table of contents", "item", "part", "company", "the company",
}


def _heading(line: str) -> bool:
    text = line.strip()
    words = text.split()
    if not words or len(words) > 8 or len(text) > 70:
        return False
    if text.isdigit() or not any(ch.isalpha() for ch in text):
        return False
    if text.endswith("."):
        return False
    return text[0].isupper()


def _sections(text: str):
    heading = "Preamble"
    buf: list[str] = []
    for line in (text or "").splitlines():
        if _heading(line):
            yield heading, "\n".join(buf)
            heading = line.strip()
            buf = []
        else:
            buf.append(line)
    yield heading, "\n".join(buf)


def _role(sentence: str) -> str | None:
    # Supplier and customer cues are more specific than the word partner.
    for name, pattern in _ROLE:
        if pattern.search(sentence):
            return name
    return None


def _good_name(name: str) -> bool:
    name = re.sub(r"\s+", " ", name).strip(" .;,")
    if len(name) < 3 or name.lower() in _NOT_NAMES:
        return False
    words = [word for word in re.split(r"\s+", name) if word]
    if not words or any(word.lower() in {"and", "or", "the", "our", "its", "including"} for word in words):
        return False
    if any(word in _ORG_SUFFIX for word in words):
        return True
    if re.fullmatch(r"[A-Z]{3,6}", name) and name not in {"SEC", "CEO", "CFO", "IPO"}:
        return True
    if re.search(r"[a-z][A-Z]", name):
        return True
    if len(words) >= 2 and all(word[:1].isupper() for word in words):
        return True
    return False


def _names(chunk: str) -> list[str]:
    found = []
    for piece in re.split(r"\s*,\s*|\s+and\s+", chunk):
        piece = re.sub(r"\s*\([^)]*\)", "", piece).strip(" .;")
        if _good_name(piece):
            found.append(piece)
    # Keep order, drop duplicates.
    seen = set()
    unique = []
    for name in found:
        key = name.lower()
        if key not in seen:
            seen.add(key)
            unique.append(name)
    return unique


def divide_profile(text: str) -> tuple[str, list[dict]]:
    """Return core business text and partner records with a role."""
    core_parts: list[str] = []
    partners: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for heading, body in _sections(text):
        if _DROP.search(heading):
            continue
        kept_sentences = []
        for sentence in re.split(r"(?<=[.!?])\s+", body):
            role = _role(sentence)
            match = _LIST.search(sentence) if role else None
            if role and match:
                for name in _names(match.group(1)):
                    key = (role, name.lower())
                    if key in seen:
                        continue
                    seen.add(key)
                    partners.append({"role": role, "name": name, "section": heading})
                sentence = (sentence[:match.start()] + sentence[match.end():]).strip()
            if sentence:
                kept_sentences.append(sentence)
        if kept_sentences:
            core_parts.append(" ".join(kept_sentences))
    core = "\n".join(part for part in core_parts if part.strip())
    return core, partners
