"""Trainer why-bits for the live hop-0 rubric.

Keep iff (done OR print OR spoke) AND NOT tip.
tape/soft cannot veto a title that already has a keep bit.

BIT_ORDER is the six live labels. Old mapped tags (signed/lever/earnings/
blast/opinion/actor/geo) are not written onto draw rows.
"""
from __future__ import annotations

BIT_ORDER = ("tape", "soft", "tip", "done", "print", "spoke")
BIT_NOUL = 0.60

_REASON_MAP = {
    "tape": "tape",
    "soft": "soft",
    "tip": "tip",
    "done": "done",
    "print": "print",
    "spoke": "spoke",
    "code_print": "print",
    "code_lever": "done",
    "code_instrument": "done",
    "code_ops": "done",
    "state_head_action": "done",
    "other_powerful": "done",
    "earnings": "done",
}


def why_bits(title: str, decision: dict | None) -> list[str]:
    """Fired six-bit labels plus the decide() winner when it is one of them."""
    del title
    decision = decision or {}
    fired: set[str] = set()
    noul = decision.get("noul") or {}
    if isinstance(noul, dict):
        for name in BIT_ORDER:
            try:
                if float(noul.get(name) or 0.0) >= BIT_NOUL:
                    fired.add(name)
            except (TypeError, ValueError):
                continue
    reason = str(decision.get("reason") or "")
    mapped = _REASON_MAP.get(reason)
    if mapped:
        fired.add(mapped)
    return [name for name in BIT_ORDER if name in fired]


def patch_train() -> None:
    from src import jev_train

    jev_train.BIT_ORDER = BIT_ORDER
    jev_train.why_bits = why_bits
