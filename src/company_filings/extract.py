"""Turn a filing document into section text and source-point offsets.

Financial-statement tables are dropped. Item headers in the table of contents
are dropped. What remains is the prose a later step can cite: business,
risk factors, properties, legal proceedings, MD&A, and the financial-statement
notes.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from lxml import html as lhtml

ANNUAL_ITEMS = frozenset({"1", "1a", "2", "3", "7", "7a"})
# Part-qualified 10-Q items. Part I item 2 is MD&A. Part II item 1 is legal.
QUARTER_ITEMS = frozenset({"1-2", "2-1", "2-1a", "2-5"})
CURRENT_ITEMS = frozenset({"1.01", "2.01", "2.02", "2.05", "7.01", "8.01"})

SECTION_CAP = 350_000
EXHIBIT_CAP = 60_000
BODY_CAP = 180_000
MIN_POINT_CHARS = 80
# Bump when section splitting changes, so a stored contents page is read again.
EXTRACTOR_VERSION = "2"

_BLOCK = frozenset({
    "p", "div", "tr", "h1", "h2", "h3", "h4", "li", "br", "section",
    "table", "caption", "dt", "dd", "blockquote",
})
_DROP_LOCAL = frozenset({"script", "style", "noscript", "svg"})
# "Part I. Item 1. Business" is the body header in a lot of filings. The
# contents page is a bare "Item 1." on its own line.
_ITEM_RE = re.compile(
    r"(?im)^[ \t]*(?:part[ \t]+(?:i{1,3}|[1-4])[ \t]*[.\-—:]*[ \t]*)?"
    r"item[ \t]+(\d{1,2}\.\d{2}|\d{1,2}[a-c]?)[ \t]*[.\-—:]*[ \t]*([^\n]{0,140})"
)
_PART_RE = re.compile(r"(?im)^[ \t]*part[ \t]+(i{1,3}|1|2)\b")
_NOTES_RE = re.compile(
    r"(?i)notes to (?:the )?(?:consolidated |condensed |combined |unaudited ){0,4}financial statements"
)
_TITLE_RULES = (
    ("business", re.compile(r"\bbusiness\b|information on the company", re.I)),
    ("risk", re.compile(r"risk factors|key information", re.I)),
    ("properties", re.compile(r"\bpropert", re.I)),
    ("legal", re.compile(r"legal proceedings", re.I)),
    ("mda", re.compile(r"management.?s discussion|operating and financial review", re.I)),
)


@dataclass(frozen=True)
class Point:
    ordinal: int
    start: int
    end: int


def html_to_text(raw: bytes) -> str:
    if not raw:
        return ""
    sample = raw[:800].lstrip().lower()
    looks_html = any(token in sample for token in (b"<html", b"<!doctype", b"<div", b"<p", b"<body", b"<table", b"<ix:"))
    if not looks_html:
        return _collapse(raw.decode("utf-8", "replace"))
    parser = lhtml.HTMLParser(recover=True, huge_tree=True)
    try:
        doc = lhtml.fromstring(raw, parser=parser)
    except (lhtml.ParserError, ValueError):
        return _collapse(raw.decode("utf-8", "replace"))
    _drop_noise(doc)
    parts: list[str] = []
    _walk(doc, parts)
    return _collapse("".join(parts))


def sections_for(form: str, text: str) -> tuple[dict[str, str], str]:
    """Return item -> text, and a split status of 'items' or 'unsplit'."""
    kind = (form or "").upper()
    if kind in {"8-K", "8-K/A"}:
        found = _current_sections(text)
    elif kind in {"10-Q", "10-Q/A"}:
        found = _quarter_sections(text)
    elif kind in {"10-K", "10-K/A"}:
        found = _annual_sections(text)
    else:
        found = _title_sections(text)
    if not found:
        body = _cap(text, BODY_CAP)
        if not body:
            return {}, "empty"
        return {"body": body}, "unsplit"
    return {key: _cap(value, SECTION_CAP if not key.startswith("ex-") else EXHIBIT_CAP) for key, value in found.items()}, "items"


def source_points(text: str) -> list[Point]:
    """Paragraph offsets into the stored section. Short lines and number tables are skipped.

    A paragraph that runs past 1,200 characters is cut on sentence boundaries so one
    block of text does not become a single unusable point.
    """
    spans: list[tuple[int, int]] = []
    for start, end in _paragraph_spans(text):
        if end - start > 1200:
            for inner_start, inner_end in _sentence_windows(text[start:end], width=900):
                spans.append((start + inner_start, start + inner_end))
        else:
            spans.append((start, end))
    if len(spans) <= 1 and len(text) > 900:
        spans = _sentence_windows(text)
    points: list[Point] = []
    for start, end in spans:
        chunk = text[start:end].strip()
        if len(chunk) < MIN_POINT_CHARS:
            continue
        digits = sum(ch.isdigit() for ch in chunk)
        if digits / max(len(chunk), 1) > 0.35:
            continue
        lead = len(text[start:end]) - len(text[start:end].lstrip())
        points.append(Point(len(points), start + lead, start + lead + len(chunk)))
    return points


def _annual_sections(text: str) -> dict[str, str]:
    headers = _body_headers(text)
    sections: dict[str, str] = {}
    for header, nxt in _spans(headers):
        item = header["item"].lower()
        if item in ANNUAL_ITEMS:
            _keep_longest(sections, item, text[header["content"]:nxt])
        elif item == "8":
            notes = _notes(text[header["content"]:nxt])
            if notes:
                _keep_longest(sections, "notes", notes)
    if sections:
        return sections
    return _title_sections(text)


def _quarter_sections(text: str) -> dict[str, str]:
    headers = _body_headers(text)
    sections: dict[str, str] = {}
    for header, nxt in _spans(headers):
        key = f"{header['part']}-{header['item'].lower()}"
        body = text[header["content"]:nxt]
        if key in QUARTER_ITEMS:
            _keep_longest(sections, key, body)
        elif key == "1-1":
            notes = _notes(body)
            if notes:
                _keep_longest(sections, "notes", notes)
    if sections:
        return sections
    return _title_sections(text)


def _current_sections(text: str) -> dict[str, str]:
    headers = [header for header in _item_headers(text) if _is_header_line(text, header["start"], header["end"])]
    headers = [header for header in headers if re.fullmatch(r"\d{1,2}\.\d{2}", header["item"])]
    if not headers:
        return {"body": text} if text.strip() else {}
    sections: dict[str, str] = {}
    for header, nxt in _spans(headers):
        if header["item"] in CURRENT_ITEMS:
            _keep_longest(sections, header["item"], text[header["content"]:nxt])
    return sections


def _title_sections(text: str) -> dict[str, str]:
    headers = _body_headers(text)
    sections: dict[str, str] = {}
    for header, nxt in _spans(headers):
        title = header.get("title") or ""
        body = text[header["content"]:nxt]
        # The rest of a wrapped heading ("Information" / "on the Company") is the
        # start of the body. Match the rule against both.
        heading = re.sub(r"\s+", " ", f"{title} {body[:120]}")
        for name, pattern in _TITLE_RULES:
            if pattern.search(heading) and len(body) > 80:
                _keep_longest(sections, name, body)
                break
    return sections


def _body_headers(text: str) -> list[dict]:
    headers = _item_headers(text)
    toc = _toc_spans(text, headers)
    headers = [header for header in headers if not _inside(header["start"], toc)]
    parts = [part for part in _part_headers(text) if not _inside(part["start"], toc)]
    for header in headers:
        header["part"] = _part_before(header["start"], parts)
    return headers


def _item_headers(text: str) -> list[dict]:
    found = []
    for match in _ITEM_RE.finditer(text):
        if not _is_header_line(text, match.start(), match.end()):
            continue
        title = match.group(2).strip(" .\t")
        found.append({
            "item": match.group(1),
            "title": title,
            "start": match.start(),
            "content": match.end(),
            "end": match.end(),
        })
    return found


def _part_headers(text: str) -> list[dict]:
    found = []
    for match in _PART_RE.finditer(text):
        if not _is_header_line(text, match.start(), match.end()):
            continue
        raw = match.group(1).lower()
        part = {"i": 1, "ii": 2, "iii": 3, "1": 1, "2": 2}.get(raw, 1)
        found.append({"part": part, "start": match.start()})
    return found


def _toc_spans(text: str, headers: list[dict]) -> list[tuple[int, int]]:
    """A run of item headers with no prose between them is the table of contents.

    The first body header often sits on the next line, so when it repeats the
    item that opened the run it is left in the body.
    """
    if not headers:
        return []
    spans: list[tuple[int, int]] = []

    def close(cluster: list[dict]) -> None:
        if len(cluster) < 4:
            return
        last = cluster[-1]
        if last["item"].lower() == cluster[0]["item"].lower() and last is not cluster[0]:
            cluster = cluster[:-1]
        if len(cluster) >= 4:
            spans.append((cluster[0]["start"], cluster[-1]["end"]))

    def packed(prev: dict, header: dict) -> bool:
        between = text[prev["end"]:header["start"]]
        return len(between) < 80 and not any(ch.isalpha() for ch in between)

    cluster = [headers[0]]
    for header in headers[1:]:
        if packed(cluster[-1], header):
            cluster.append(header)
        else:
            close(cluster)
            cluster = [header]
    close(cluster)
    return spans


def _keep_longest(sections: dict[str, str], key: str, body: str) -> None:
    """The contents page and the body both carry the same item number. Keep the body."""
    if len(body) > len(sections.get(key, "")):
        sections[key] = body


def _spans(headers: list[dict]) -> list[tuple[dict, int]]:
    pairs = []
    for index, header in enumerate(headers):
        nxt = headers[index + 1]["start"] if index + 1 < len(headers) else 10**12
        pairs.append((header, nxt))
    return pairs


def _notes(text: str) -> str:
    matches = list(_NOTES_RE.finditer(text))
    for match in reversed(matches):
        if len(text) - match.start() > 80:
            return text[match.start():]
    return ""


def _part_before(pos: int, parts: list[dict]) -> int:
    current = 1
    for part in parts:
        if part["start"] < pos:
            current = part["part"]
        else:
            break
    return current


def _inside(pos: int, spans: list[tuple[int, int]]) -> bool:
    return any(start <= pos <= end for start, end in spans)


def _is_header_line(text: str, start: int, end: int) -> bool:
    line_end = text.find("\n", start)
    if line_end < 0:
        line_end = len(text)
    return (line_end - start) <= 180 and (end - start) <= 180


def _paragraph_spans(text: str) -> list[tuple[int, int]]:
    spans = []
    cursor = 0
    for part in re.split(r"\n\s*\n", text):
        idx = text.find(part, cursor)
        if idx < 0:
            idx = cursor
        cursor = idx + len(part)
        if part.strip():
            spans.append((idx, idx + len(part)))
    return spans


def _sentence_windows(text: str, width: int = 700) -> list[tuple[int, int]]:
    spans = []
    start = 0
    while start < len(text):
        end = min(len(text), start + width)
        if end < len(text):
            cut = text.rfind(". ", start + 200, end)
            if cut > start:
                end = cut + 1
        spans.append((start, end))
        start = end
    return spans


def _cap(text: str, limit: int) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    head = limit * 2 // 3
    tail = limit - head - 20
    return text[:head].rstrip() + "\n\n[...truncated...]\n\n" + text[-tail:].lstrip()


def _collapse(text: str) -> str:
    text = text.replace("\xa0", " ").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _numeric_ratio(text: str) -> float:
    stripped = re.sub(r"\s+", "", text)
    if len(stripped) < 12:
        return 0.0
    numeric = sum(ch.isdigit() or ch in "$,.%()-" for ch in stripped)
    return numeric / len(stripped)


def _drop_noise(doc) -> None:
    for element in list(doc.iter()):
        tag = element.tag
        if not isinstance(tag, str):
            continue
        namespace = tag[1:].split("}", 1)[0].lower() if tag.startswith("{") else ""
        local = tag.rsplit("}", 1)[-1].lower()
        if local in _DROP_LOCAL or ("xbrl" in namespace and local in {"header", "hidden", "references"}):
            parent = element.getparent()
            if parent is not None:
                parent.remove(element)


def _walk(element, parts: list[str]) -> None:
    tag = element.tag if isinstance(element.tag, str) else ""
    local = tag.rsplit("}", 1)[-1].lower()
    if local == "table":
        try:
            content = element.text_content() or ""
        except (ValueError, AttributeError):
            content = ""
        if _numeric_ratio(content) >= 0.45:
            parts.append("\n")
            return
    if local in _BLOCK:
        parts.append("\n")
    if element.text:
        parts.append(element.text)
    for child in element:
        _walk(child, parts)
        if child.tail:
            parts.append(child.tail)
    if local in _BLOCK:
        parts.append("\n")
