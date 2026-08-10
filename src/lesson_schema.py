"""Strict lesson schema for candidate + active rules.

Required fields (market or ops):
  when / trigger_pattern
  do_instead / corrected_behavior
  wrong_if / falsifier
  error_category (A|B|C|D|E|NONE)
  scope (general|ops|sector:*)
"""
from __future__ import annotations

import re
from typing import Any

REQUIRED = ("when", "do_instead", "wrong_if", "error_category")

# Map LESSON_BEGIN keys / frontmatter aliases → canonical
ALIASES = {
    "when": ("when", "TRIGGER_PATTERN", "trigger_pattern"),
    "do_instead": ("do_instead", "CORRECTED_BEHAVIOR", "corrected_behavior"),
    "wrong_if": ("wrong_if", "FALSIFIER", "falsifier"),
    "error_category": ("error_category", "ERROR_CATEGORY"),
    "current_behavior": ("current_behavior", "CURRENT_BEHAVIOR"),
    "evidence": ("evidence", "EVIDENCE", "evidence_cited"),
    "scope": ("scope", "SCOPE"),
}


def _pick(raw: dict[str, Any], canonical: str) -> str:
    for k in ALIASES.get(canonical, (canonical,)):
        v = raw.get(k)
        if v is None:
            continue
        s = str(v).strip().strip('"')
        if s and s.lower() not in ("none", "n/a", "na", "(not recorded)"):
            return s
    return ""


def normalize(raw: dict[str, Any], date_str: str = "") -> dict[str, str]:
    cat = _pick(raw, "error_category").upper() or "NONE"
    if len(cat) > 1:
        # allow "B — Misweighted" → B
        m = re.match(r"([A-E]|NONE)", cat)
        cat = m.group(1) if m else "NONE"
    scope = _pick(raw, "scope") or ("ops" if cat == "D" else "general")
    return {
        "date": date_str or _pick(raw, "date") or "",
        "scope": scope,
        "error_category": cat,
        "when": _pick(raw, "when"),
        "do_instead": _pick(raw, "do_instead"),
        "wrong_if": _pick(raw, "wrong_if"),
        "current_behavior": _pick(raw, "current_behavior"),
        "evidence": _pick(raw, "evidence"),
        "status": _pick(raw, "status") or "candidate",
    }


def is_complete(n: dict[str, str]) -> bool:
    if n.get("error_category") in ("NONE", ""):
        # no lesson claimed — complete as empty lesson
        return True
    return bool(n.get("when") and n.get("do_instead") and n.get("wrong_if"))


def validation_errors(n: dict[str, str]) -> list[str]:
    if n.get("error_category") in ("NONE", ""):
        return []
    errs = []
    if not n.get("when"):
        errs.append("missing when/trigger_pattern")
    if not n.get("do_instead"):
        errs.append("missing do_instead/corrected_behavior")
    if not n.get("wrong_if"):
        errs.append("missing wrong_if/falsifier")
    # do_instead should be operational for market lessons
    di = (n.get("do_instead") or "").lower()
    if n.get("error_category") in ("A", "B", "C") and di:
        if not re.search(
            r"\bb[0-7]\b|score|direction|futures|gate|search|weight|cap |forbid",
            di,
        ):
            errs.append(
                "do_instead must name a B-score, direction rule, futures, weight, or gate"
            )
    return errs


def frontmatter(n: dict[str, str], extra: dict | None = None) -> str:
    lines = ["---"]
    fields = {
        "trigger_pattern": n.get("when", ""),
        "corrected_behavior": n.get("do_instead", ""),
        "falsifier": n.get("wrong_if", ""),
        "current_behavior": n.get("current_behavior", ""),
        "evidence_cited": n.get("evidence", ""),
        "error_category": n.get("error_category", "NONE"),
        "scope": n.get("scope", "general"),
        "date": n.get("date", ""),
        "status": n.get("status", "candidate"),
        "schema_ok": str(is_complete(n) and not validation_errors(n)).lower(),
    }
    if extra:
        fields.update({k: str(v) for k, v in extra.items()})
    for k, v in fields.items():
        v = (v or "").replace('"', "'").replace("\n", " ")
        lines.append(f'{k}: "{v}"')
    lines.append("---")
    return "\n".join(lines) + "\n"


def active_rule_markdown(n: dict[str, str], extra_body: str = "") -> str:
    """Canonical standing-rule body injected into predict."""
    return (
        f"{frontmatter({**n, 'status': 'active'})}\n"
        f"## RULE\n{n.get('do_instead', '').strip()}\n\n"
        f"## WHEN IT FIRES\n{n.get('when', '').strip()}\n\n"
        f"## WRONG IF\n{n.get('wrong_if', '').strip()}\n\n"
        f"## EVIDENCE\n{n.get('evidence', '').strip()}\n\n"
        f"{extra_body}\n"
    )


def standing_rules_block(active_texts: list[str]) -> str:
    if not active_texts:
        return (
            "[STANDING ACTIVE LESSONS]\n"
            "(none — no promoted rules yet)\n"
        )
    body = "\n\n---\n\n".join(active_texts)
    return (
        "[STANDING ACTIVE LESSONS — MUST FOLLOW WHEN TRIGGER MATCHES]\n"
        "For each rule: if WHEN matches today's tape, apply RULE. "
        "In your write-up list RULES_APPLIED: <ids or none>.\n\n"
        f"{body}\n"
    )
