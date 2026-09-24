"""Scan many headlines in two SuperGrok batches before any single-article hop.

Order:
  Gate 0 (code) → usability batch (Fast) → classify batch (Floor)
    → only KEEP+classed rows go to meta / pack / analyst.
"""
from __future__ import annotations

from typing import Any

from src.news_impact.hygiene import is_reaction_title
from src.news_impact.one_shot_stack import gate0
from src.openclaw_models import DEFAULT_NEWS_MODEL, model_for_hop

USABILITY_BATCH = 40
CLASSIFY_BATCH = 20


def _chunks(rows: list, n: int):
    for i in range(0, len(rows), n):
        yield rows[i:i + n]


def gate0_many(arts: list[dict]) -> tuple[list[dict], list[dict]]:
    """Code screen. Returns (survivors, killed)."""
    kept, killed = [], []
    for art in arts:
        title = str(art.get("title") or "")
        body = str(art.get("body") or "")
        why = gate0(title, body)
        if not why and is_reaction_title(title):
            why = "tape"
        if why:
            killed.append({**art, "verdict": "kill", "reason": why, "stage": "gate0"})
        else:
            kept.append(art)
    return kept, killed


def _parse_rows(parsed: dict | None, n: int) -> list[dict]:
    if not isinstance(parsed, dict):
        return []
    raw = parsed.get("rows")
    if not isinstance(raw, list):
        return []
    out = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        try:
            i = int(row.get("i"))
        except (TypeError, ValueError):
            continue
        if 0 <= i < n:
            out.append(row)
    return out


def usability_pass(arts: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Fast-model batch. Returns (keep_or_unsure, killed, hop_logs)."""
    from src.news_impact.openclaw_hop import hop_batch
    keep, kill, logs = [], [], []
    for chunk in _chunks(arts, USABILITY_BATCH):
        parsed, lane, model, log = hop_batch("news_usability_batch", chunk)
        logs.extend(log)
        by_i = {int(r["i"]): r for r in _parse_rows(parsed, len(chunk)) if "i" in r}
        for i, art in enumerate(chunk):
            row = by_i.get(i) or {}
            verdict = str(row.get("verdict") or "unsure").lower()
            reason = str(row.get("reason") or "")
            stamped = {
                **art,
                "verdict": verdict,
                "reason": reason,
                "stage": "usability_batch",
                "screen_lane": lane,
                "screen_model": model,
            }
            if verdict == "kill":
                kill.append(stamped)
            else:
                keep.append(stamped)
    return keep, kill, logs


def classify_pass(arts: list[dict]) -> tuple[list[dict], list[dict]]:
    """Floor-model batch. Attaches q5 + event_class."""
    from src.news_impact.openclaw_hop import hop_batch
    out, logs = [], []
    for chunk in _chunks(arts, CLASSIFY_BATCH):
        parsed, lane, model, log = hop_batch("news_classify_batch", chunk)
        logs.extend(log)
        by_i = {int(r["i"]): r for r in _parse_rows(parsed, len(chunk)) if "i" in r}
        for i, art in enumerate(chunk):
            row = by_i.get(i) or {}
            out.append({
                **art,
                "q5": row.get("q5") or "",
                "event_class": row.get("event_class") or "",
                "sign": row.get("sign"),
                "constraint": row.get("constraint") or "",
                "split": bool(row.get("split")),
                "classify_lane": lane,
                "classify_model": model,
                "stage": "classify_batch",
            })
    return out, logs


def screen_then_classify(arts: list[dict]) -> dict[str, Any]:
    """Full intake. SuperGrok is only called on post-Gate-0 rows, in batches."""
    survivors, g0_kill = gate0_many(arts)
    usable, u_kill, u_logs = usability_pass(survivors) if survivors else ([], [], [])
    classified, c_logs = classify_pass(usable) if usable else ([], [])
    deep = [
        r for r in classified
        if r.get("event_class") and r.get("event_class") != "discard"
        and r.get("q5") != "regime"
    ]
    return {
        "n_in": len(arts),
        "n_gate0_kill": len(g0_kill),
        "n_usability_kill": len(u_kill),
        "n_usable": len(usable),
        "n_classified": len(classified),
        "n_deep": len(deep),
        "screen_model": model_for_hop("news_usability_batch"),
        "classify_model": model_for_hop("news_classify_batch") or DEFAULT_NEWS_MODEL,
        "killed_gate0": g0_kill,
        "killed_usability": u_kill,
        "classified": classified,
        "deep": deep,
        "hop_logs": u_logs + c_logs,
    }
