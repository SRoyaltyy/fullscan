"""Pick WHICH active lessons a predict prompt gets to see.

Until now every predict (general and all 11 sectors) received every file in
02_lessons/active — ~200 prose rules, most about other sectors or the stock
book. That is noise the LLM cannot act on and it buries the handful of
rules that matter. This module returns only the lessons attributed to the
topic being predicted (plus ops rules, which apply everywhere), ranked by
efficacy verdict and recency, capped at MAX_INJECTED.

Ranking: improved > insufficient/unjudged > flat. Lessons judged WORSE are
never injected (lesson_retire moves them out of active/ anyway).
"""
from __future__ import annotations

import glob
import json
import os
import re
from pathlib import Path

from . import config
from .lesson_efficacy import OUT_JSON as EFFICACY_JSON, _frontmatter, _topic_of

MAX_INJECTED = 20
MAX_CHARS = 1200      # per lesson; the body repeats the front matter, so drop it
_RANK = {"improved": 0, "insufficient": 1, "no graded runs": 1,
         "not market-graded": 1, None: 1, "flat": 2, "WORSE": 9}


def _verdicts() -> dict[str, str]:
    try:
        data = json.loads(Path(EFFICACY_JSON).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {r.get("lesson"): r.get("verdict") for r in data.get("lessons", [])}


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def compact(text: str, limit: int = MAX_CHARS) -> str:
    """Body only (RULE / WHEN / WRONG IF / EVIDENCE), trimmed."""
    m = re.match(r"^---\n.*?\n---\s*", text, re.S)
    body = text[m.end():] if m else text
    body = body.strip()
    return body if len(body) <= limit else body[:limit].rstrip() + " …"


def _relevant(fm: dict, topic: str) -> bool:
    scope = (fm.get("scope") or "").strip().lower()
    if scope in ("book", "news"):
        return False
    lesson_topic = _topic_of(fm)
    if scope == "ops" or fm.get("error_category") == "D":
        # ops rules: everywhere, unless pinned to a different sector
        return lesson_topic is None or lesson_topic == topic or lesson_topic == "general"
    return lesson_topic == topic


def select_active(topic: str, limit: int = MAX_INJECTED) -> list[tuple[str, str]]:
    """[(file name, compact text)] for `topic` ("general" or "sector:<Name>")."""
    verdicts = _verdicts()
    rows = []
    for p in sorted(glob.glob(os.path.join(config.LESSONS_ACTIVE, "*.md"))):
        name = os.path.basename(p)
        if name.startswith("."):
            continue
        text = _read(p).strip()
        if not text:
            continue
        fm = _frontmatter(text)
        if not _relevant(fm, topic):
            continue
        verdict = verdicts.get(name)
        if verdict == "WORSE":
            continue
        since = fm.get("promoted_on") or fm.get("date") or ""
        rows.append((_RANK.get(verdict, 1), since, name, compact(text)))
    # best verdict first, then newest (two stable sorts)
    rows.sort(key=lambda r: r[1], reverse=True)
    rows.sort(key=lambda r: r[0])
    return [(name, text) for _, _, name, text in rows[:limit]]


def count_active(topic: str) -> tuple[int, int]:
    """(injected, total relevant) so the prompt can say what was left out."""
    all_rel = select_active(topic, limit=10 ** 6)
    return min(len(all_rel), MAX_INJECTED), len(all_rel)
