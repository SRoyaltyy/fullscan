"""Shared two-part Grok predict rule (pending Cyrus).

Used by the h1 seal (research/hot_n4_clean_v4/forward/h1_grok_gate.py) and
the Factor Mine seal wrapper (src/fm_grok_gate.py). Standard library only so
it imports in every CI checkout. Session D passes only when
01_daily/<D>_preopen_status.json has grok_ok == true AND
01_daily/general/<D>_predict_trace.md names a Grok model.
"""
from __future__ import annotations

import json
import re

_MODEL = re.compile(r"(?i)\bmodel:?\s*`([^`]+)`")


def status_rel(session: str) -> str:
    return f"01_daily/{session}_preopen_status.json"


def trace_rel(session: str) -> str:
    return f"01_daily/general/{session}_predict_trace.md"


def trace_model(text: str) -> str | None:
    m = _MODEL.search(text)
    return m.group(1).strip() if m else None


def reason_from(session: str, status_raw: str | None, trace_text: str | None,
                where: str = "main", book: str = "h1", tail: str = "") -> str | None:
    """Two-part check on already-read text. None when Grok wrote the predict.

    Shared by the h1 seal and the Factor Mine seal wrapper
    (src/fm_grok_gate.py) so both books apply the identical rule.
    """
    tail = f" {tail}" if tail else ""
    if status_raw is None:
        return f"{status_rel(session)} is not on {where}; cannot confirm grok_ok.{tail}"
    try:
        ok = json.loads(status_raw).get("grok_ok")
    except (ValueError, AttributeError):
        return f"{status_rel(session)} does not parse; cannot confirm grok_ok.{tail}"
    if ok is not True:
        return f"{status_rel(session)} has grok_ok={ok!r}; the {book} seal needs grok_ok=true.{tail}"
    if trace_text is None:
        return f"{trace_rel(session)} is not on {where}; cannot confirm the predict model.{tail}"
    model = trace_model(trace_text)
    if not model or "grok" not in model.lower():
        return f"{trace_rel(session)} names model {model or 'none'}, not Grok; the {book} seal needs a Grok predict.{tail}"
    return None


