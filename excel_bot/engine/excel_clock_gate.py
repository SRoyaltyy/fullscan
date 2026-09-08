"""Excel clock gate — source of truth for open-entry features.

Locks against OPEN_SAME_ROW_LABELS.md + CLOCK_MAP.md / clock_map.json.
Refuse to invent clocks. Live flatten_robust is not imported or written.

Same-row OPEN fills (timing-proven): A B C G J K L M O IR IS IT
Same-row OPEN numbers/text: the 44 value_mine_open cols (never same-row H/I)
Lags: any letter from rows above is fair
OUT same-row: M/B/G/K/O numbers, D/E/F/H/I, L/N/AA values, core_score
"""
from __future__ import annotations

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RESEARCH = os.path.join(ROOT, "research")
CLOCK_JSON = os.path.join(RESEARCH, "clock_map.json")
CLOCK_MD = os.path.join(RESEARCH, "CLOCK_MAP.md")
LABELS_MD = os.path.join(RESEARCH, "OPEN_SAME_ROW_LABELS.md")

# Excel-locked. Do not invent. Must match clock_map groups.
VALUE_OPEN_44 = (
    "A", "C", "J", "Q", "Z", "AC", "AH", "BT", "BV", "CG", "CH", "DC", "DE",
    "EB", "EK", "EN", "EP", "EQ", "ER", "ES", "ET", "EU", "EV", "FQ", "FR",
    "FS", "FU", "GD", "GE", "GF", "HF", "HG", "HW", "II", "IR", "IT", "IY",
    "IZ", "JB", "JC", "JD", "JE", "JF", "JL",
)
FILL_OPEN = ("A", "B", "C", "G", "J", "K", "L", "M", "O", "IR", "IS", "IT")
# User OUT + CLOCK_MAP value-close landmines. Never same-row features.
VALUE_OUT_SAME_ROW = (
    "B", "G", "K", "M", "O", "L", "D", "E", "F", "H", "I", "N", "AA",
)
LANDMINE_VALUE = VALUE_OUT_SAME_ROW
CORE_SCORE_OUT = "core_score"
# Date serials — in the 44 as open values, not useful as thresholds.
SKIP_VALUE_OPS = {"A", "IR"}


def load_clocks():
    return json.load(open(CLOCK_JSON, encoding="utf-8"))


def assert_excel_clock_gate(clocks=None):
    """Abort if the locked 12-fill / 44-value lists drifted from Excel."""
    clocks = clocks if clocks is not None else load_clocks()
    vo = tuple(clocks["groups"]["value_mine_open"])
    fo = tuple(clocks["groups"]["fill_mine_open"])
    if vo != VALUE_OPEN_44:
        raise ValueError(f"value_mine_open drifted: {vo} != locked 44")
    if fo != FILL_OPEN:
        raise ValueError(f"fill_mine_open drifted: {fo} != locked fills")
    if len(VALUE_OPEN_44) != 44:
        raise ValueError("locked value-open list is not 44")
    by = {r["col"]: r for r in clocks["columns"]}
    for col in VALUE_OUT_SAME_ROW:
        rec = by.get(col)
        if rec is None:
            continue
        if rec.get("value_mine") == "open":
            raise ValueError(f"landmine {col} is value-open — do not invent")
    if by["O"]["fill_mine"] != "open" or by["O"]["value_mine"] != "close":
        raise ValueError("O fill/value lock broken")
    if by["M"]["fill_mine"] != "open" or by["M"]["value_mine"] != "close":
        raise ValueError("M fill is open; M number is close — lock broken")
    if by["B"]["value_mine"] != "close" or by["G"]["value_mine"] != "close":
        raise ValueError("B/G number lock broken")
    if by["K"]["value_mine"] != "close":
        raise ValueError("K number lock broken")
    for col in ("D", "E", "F", "H", "I"):
        if by[col]["value_mine"] != "close" or by[col]["fill_mine"] != "close":
            raise ValueError(f"{col} same-row must stay close")
    if clocks.get("core_score_entry") not in (None, "close"):
        # classify_clocks stores this at top level when present
        if clocks.get("core_score_entry") == "open":
            raise ValueError("core_score must stay close-entry")
    labels = open(LABELS_MD, encoding="utf-8").read()
    clock_md = open(CLOCK_MD, encoding="utf-8").read()
    if "value_mine_open" not in labels or "44" not in labels:
        raise ValueError("OPEN_SAME_ROW_LABELS.md is not the 44-col gate")
    for letter in FILL_OPEN:
        if letter not in labels.split("Same-row CLOSE", 1)[0]:
            raise ValueError(f"OPEN_SAME_ROW_LABELS.md missing fill {letter}")
    if "Fill OPEN" not in clock_md or "Value OPEN" not in clock_md:
        raise ValueError("CLOCK_MAP.md missing Fill/Value OPEN groups")
    if "core_score" not in clock_md.lower() and "CLOSE" not in clock_md:
        raise ValueError("CLOCK_MAP.md must keep core_score close")
    return clocks


def assert_feature_legal(kind, col, lag):
    """Refuse a close-knowable same-row atom. Lags of any letter are fair."""
    if col == CORE_SCORE_OUT or (isinstance(col, str) and "core_score" in col):
        raise ValueError("core_score is close-entry only")
    if lag and lag >= 1:
        return
    if kind == "fill":
        if col not in FILL_OPEN:
            raise ValueError(f"same-row fill {col} is not in fill_mine_open")
        return
    if kind in ("num", "text", "value"):
        if col in VALUE_OUT_SAME_ROW:
            raise ValueError(f"same-row value {col} is OUT (close/landmine)")
        if col not in VALUE_OPEN_44:
            raise ValueError(f"same-row value {col} is not in the locked 44")
        if col in ("H", "I"):
            raise ValueError("same-row H/I are labels only")
        return
    raise ValueError(f"unknown feature kind {kind}")


def gate_payload():
    return {
        "gate": "OPEN_SAME_ROW_LABELS + CLOCK_MAP",
        "fill_open": list(FILL_OPEN),
        "value_mine_open": list(VALUE_OPEN_44),
        "value_out_same_row": list(VALUE_OUT_SAME_ROW),
        "core_score": "close",
        "m_number": "close",
        "lags": "any letter from rows above",
        "live_untouched": "flatten_robust",
    }
