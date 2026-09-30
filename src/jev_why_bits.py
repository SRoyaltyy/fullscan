"""Why-bits column for the Jev trainer page.

Live six: tape soft tip done print spoke.
Falls back to mapping a code reason (k_print, v_tape, ...) onto those six.
"""
from __future__ import annotations

BIT_ORDER = (
    "tape", "soft", "tip", "done", "print", "spoke",
)
BIT_NOUL = 0.60

_REASON_MAP = {
    "k_print": "print",
    "code_print": "print",
    "k_done": "done",
    "k_earn": "done",
    "k_rating": "done",
    "k_policy": "done",
    "k_choke": "done",
    "k_dollar": "done",
    "v_tape": "tape",
    "v_week": "soft",
    "v_odds": "soft",
    "v_tipsheet": "tip",
    "v_quote": "tip",
    "v_fluff": "tip",
    "v_ask": "tip",
    "source": "tip",
}


def why_bits(title: str, decision: dict | None) -> list[str]:
    decision = decision or {}
    reason = str(decision.get("reason") or "")
    noul = decision.get("noul") or {}
    fired: list[str] = []

    def lit(name: str) -> bool:
        raw = noul.get(name) if isinstance(noul, dict) else None
        if isinstance(raw, dict):
            raw = raw.get("noul", 0)
        try:
            return float(raw or 0.0) >= BIT_NOUL
        except (TypeError, ValueError):
            return False

    for name in BIT_ORDER:
        if lit(name) or reason == name:
            fired.append(name)
    if fired:
        return fired
    mapped = _REASON_MAP.get(reason)
    if mapped:
        return [mapped]
    if reason and reason not in {"no_keep_bit", "low_material", "geo_other"}:
        return [reason]
    return []


def patch_train() -> None:
    import sys
    mod = sys.modules.get("src.jev_train")
    if mod is None:
        return
    mod.BIT_ORDER = BIT_ORDER
    mod.why_bits = why_bits
