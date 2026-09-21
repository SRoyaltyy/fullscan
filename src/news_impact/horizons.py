"""Horizon-by-class defaults. Tested windows, not a new taxonomy.

0-1d: inventory print, hard CPI/NFP surprise, tariff imposed, same-day blast_ops.
1-4w: guidance cut/raise (not reaffirm), print_vs_priced leftover, blast_cyber.
1-6m: gate / capacity / CHIPS / trial_readout.

A 6m event is never a 0-1d miss. Window grid is printed per class when tape exists.
"""
from __future__ import annotations

import re
from typing import Any

# Explicit class → natural window. Unlisted classes keep the family assignment.
CLASS_HORIZON = {
    "inventory_print": "0-1d",
    "blast_ops": "0-1d",
    "factor_impulse": "0-1d",  # hard print / tariff; weather already q5=regime
    "guidance": "1-4w",
    "print_vs_priced": "1-4w",
    "blast_cyber": "1-4w",
    "gate": "1-6m",
    "capacity": "1-6m",
    "trial_readout": "1-6m",
}

_HARD_MACRO = re.compile(
    r"(?i)(\bcpi\b|\bnfp\b|payrolls|non-?farm|"
    r"(imposes?|announces?|levies?).{0,24}tariff)"
)
_CHIPS = re.compile(r"(?i)\bchips\b.{0,24}(act|award)")

HORIZON_OFFSET = {"0-1d": 0, "1-4w": 20, "1-6m": 63}


def default_horizon(
    event_class: str,
    title: str = "",
    sign: str | None = None,
) -> str | None:
    """Natural window for this class, or None to keep the family assignment."""
    ev = str(event_class or "")
    # Reaffirm stays ungraded via sign/direction; the class window is still 1-4w.
    if ev == "gate" or _CHIPS.search(str(title or "")):
        return "1-6m"
    if ev == "factor_impulse" and not _HARD_MACRO.search(str(title or "")):
        return "0-1d"
    return CLASS_HORIZON.get(ev)


def apply_class_horizons(entities: list, event_class: str,
                         title: str = "", sign: str | None = None) -> list:
    hz = default_horizon(event_class, title, sign)
    if not hz:
        return entities
    for e in entities:
        if hasattr(e, "horizon"):
            e.horizon = hz
        elif isinstance(e, dict):
            e["horizon"] = hz
    return entities


def skips_short_window(event_class: str, title: str = "",
                       sign: str | None = None, horizon: str = "") -> bool:
    """Do not grade 0-1d as a miss when the *class* natural window is longer.

    Family-assigned horizons (e.g. input_cost 1-4w) do not skip 0-1d unless
    the class is in CLASS_HORIZON / the A list (gate, capacity, blast_cyber).
    """
    hz = default_horizon(event_class, title, sign)
    if hz:
        return hz in {"1-4w", "1-6m", "6m+"}
    return False


def window_grid(results: list[dict]) -> dict[str, dict[str, Any]]:
    """Per-class hit grid at 0-1d / 1-4w / 1-6m when tape exists.

    Long-horizon classes are omitted from the 0-1d column (not counted as misses).
    """
    from .grade import is_gradeable, ungraded_reason

    bag: dict[str, dict[str, Any]] = {}

    def _slot(ev: str) -> dict[str, Any]:
        if ev not in bag:
            bag[ev] = {
                "horizon": CLASS_HORIZON.get(ev) or default_horizon(ev) or "?",
                "n_1d": 0, "hit_1d": 0, "hit_rate_1d": None,
                "n_20d": 0, "hit_20d": 0, "hit_rate_20d": None,
                "n_63d": 0, "hit_63d": 0, "hit_rate_63d": None,
            }
        return bag[ev]

    def _rate(h: int, n: int) -> float | None:
        return round(h / n, 4) if n else None

    for r in results:
        if not r.get("usable"):
            continue
        ev = str((r.get("classification") or {}).get("event_class") or "")
        title = str(r.get("title") or "")
        sign = (r.get("classification") or {}).get("sign")
        skip_01d = skips_short_window(ev, title, sign)
        for g in r.get("performance") or []:
            if not isinstance(g, dict):
                continue
            if ungraded_reason(g, r) is not None or not is_gradeable(g, r):
                continue
            slot = _slot(ev or g.get("event_class") or "?")
            if g.get("ret_1d") is not None and not skip_01d:
                slot["n_1d"] += 1
                slot["hit_1d"] += int(g.get("agree_1d") is True)
            if g.get("ret_20d") is not None:
                slot["n_20d"] += 1
                slot["hit_20d"] += int(g.get("agree_20d") is True)
            ret63 = g.get("ret_63d")
            if ret63 is not None:
                slot["n_63d"] += 1
                slot["hit_63d"] += int(g.get("agree_63d") is True)
    for slot in bag.values():
        slot["hit_rate_1d"] = _rate(slot["hit_1d"], slot["n_1d"])
        slot["hit_rate_20d"] = _rate(slot["hit_20d"], slot["n_20d"])
        slot["hit_rate_63d"] = _rate(slot["hit_63d"], slot["n_63d"])
    return bag


def format_window_grid(grid: dict[str, dict[str, Any]]) -> list[str]:
    lines = [
        "| class | natural | 0-1d hits | 0-1d n | 0-1d rate | 1-4w hits | 1-4w n | 1-4w rate | 1-6m hits | 1-6m n | 1-6m rate |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for ev, slot in sorted(grid.items()):
        def cell(k, fallback="—"):
            v = slot.get(k)
            return fallback if v is None else v
        lines.append(
            f"| {ev} | {slot.get('horizon') or '?'} | "
            f"{cell('hit_1d', 0)} | {cell('n_1d', 0)} | {cell('hit_rate_1d')} | "
            f"{cell('hit_20d', 0)} | {cell('n_20d', 0)} | {cell('hit_rate_20d')} | "
            f"{cell('hit_63d', 0)} | {cell('n_63d', 0)} | {cell('hit_rate_63d')} |"
        )
    return lines
