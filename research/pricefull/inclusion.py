"""Read REBUILD_MATCH marks for the price-only fullscan.

An id is exact only when INPUT_PROVENANCE_336.md contains that id and the
same table row or the same bullet sets REBUILD_MATCH to the string exact.
A missing id, a different spelling, a different value, or a mark on a
nearby row is not exact. Marks in any other file are ignored.
"""
from __future__ import annotations

import re
from pathlib import Path

AUDIT_NAME = "research/audit/INPUT_PROVENANCE_336.md"

# Section 4 ids, in protocol order. The strings are the match keys.
PRICE_IDS = (
    "hot_score",
    "ret_1",
    "ret_5",
    "ret_10",
    "rvol",
    "break_10",
    "last_green",
    "candle_score",
    "rsi",
    "macd",
    "overnight_gap",
)
SOURCE_IDS = ("yday_gainer", "yday_mover", "ohlc_hot", "overnight_move")
AB_IDS = (
    "A01_rsi_value",
    "A02_rsi_cross_30",
    "A03_rsi_cross_50",
    "A04_rsi_cross_70",
    "A05_body_red_green_2day",
    "A06_volume_red_green_2day",
    "A07_rvol",
    "A08_bollinger_position",
    "A09_above_sma50",
    "A10_sma20_50_80_stack",
    "A11_three_section_lows",
    "A12_green_body_vs_wick_2day",
    "A13_red_body_vs_wick_2day",
    "A14_profitable_oversold_setup",
    "A15_tape_recovery_setup",
)
AB_USABLE = tuple(i for i in AB_IDS if i != "A14_profitable_oversold_setup")
MARKET_IDS = ("peer_rs", "vix", "dgs10", "dgs30", "dfii10", "sofr")
SECTION4_IDS = PRICE_IDS + SOURCE_IDS + AB_IDS + MARKET_IDS
HOT_PARTS = ("ret_5", "ret_10", "rvol", "break_10", "last_green")

_ASSIGN = re.compile(r"REBUILD_MATCH\s*[=:]\s*`?([A-Za-z0-9_-]+)`?")


def _ident_re(ident: str) -> re.Pattern[str]:
    return re.compile(rf"(?<![A-Za-z0-9_]){re.escape(ident)}(?![A-Za-z0-9_])")


_ID_RES = {ident: _ident_re(ident) for ident in SECTION4_IDS}


def _cells(line: str) -> list[str] | None:
    text = line.strip()
    if not text.startswith("|"):
        return None
    return [cell.strip() for cell in text.strip("|").split("|")]


def _separator(cells: list[str]) -> bool:
    if not cells:
        return False
    return all(cell != "" and set(cell) <= set("-: ") for cell in cells)


def rebuild_rows(text: str) -> list[dict]:
    """Data rows from tables that have a REBUILD_MATCH column."""
    lines = text.splitlines()
    rows: list[dict] = []
    i = 0
    while i < len(lines):
        cells = _cells(lines[i])
        if not cells or "REBUILD_MATCH" not in cells:
            i += 1
            continue
        col = cells.index("REBUILD_MATCH")
        i += 1
        if i < len(lines):
            sep = _cells(lines[i])
            if sep is not None and _separator(sep):
                i += 1
        while i < len(lines):
            row = _cells(lines[i])
            if row is None:
                break
            if _separator(row):
                i += 1
                continue
            mark = row[col].strip() if col < len(row) else ""
            rows.append({
                "line": i + 1,
                "kind": "table",
                "text": lines[i],
                "mark": mark,
            })
            i += 1
    return rows


def rebuild_bullets(text: str) -> list[dict]:
    """Bullets that assign REBUILD_MATCH. Other bullets are not marks."""
    out: list[dict] = []
    for n, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not (stripped.startswith("- ") or stripped.startswith("* ")):
            continue
        found = _ASSIGN.search(stripped)
        if not found:
            continue
        out.append({
            "line": n,
            "kind": "bullet",
            "text": line,
            "mark": found.group(1),
        })
    return out


def _contains(ident: str, text: str) -> bool:
    return _ID_RES[ident].search(text) is not None


def read_marks(text: str) -> dict:
    """Which section 4 ids the audit file marks exact."""
    if text is None:
        text = ""
    file_present = True
    rows = rebuild_rows(text) + rebuild_bullets(text)
    found = []
    exact = []
    for ident in SECTION4_IDS:
        hits = [row for row in rows if _contains(ident, row["text"])]
        mark_values = [row["mark"] for row in hits]
        is_exact = any(mark == "exact" for mark in mark_values)
        if is_exact:
            exact.append(ident)
        if hits or is_exact:
            found.append({
                "id": ident,
                "exact": is_exact,
                "marks": mark_values,
                "lines": [row["line"] for row in hits],
            })
        else:
            found.append({
                "id": ident,
                "exact": False,
                "marks": [],
                "lines": [],
            })
    return {
        "file": AUDIT_NAME,
        "file_present": file_present,
        "exact_ids": exact,
        "ids": found,
    }


def hot_score_exact(exact_ids: set[str]) -> bool:
    """hot_score is exact only with its own mark and every formula part."""
    if "hot_score" not in exact_ids:
        return False
    return all(part in exact_ids for part in HOT_PARTS)


def decide(marks: dict | None) -> dict:
    """Variant gates after section 3 exclusions and the hot_score rule.

    A missing audit is the same as every id not exact: both variants buy
    nobody.
    """
    if not marks or not marks.get("file_present"):
        exact: set[str] = set()
        ids = [
            {"id": ident, "exact": False, "marks": [], "lines": [], "reason": "audit file absent"}
            for ident in SECTION4_IDS
        ]
    else:
        exact = set(marks.get("exact_ids") or [])
        ids = []
        for row in marks["ids"]:
            reason = "exact" if row["exact"] else (
                "id not on a REBUILD_MATCH row" if not row["marks"]
                else "REBUILD_MATCH is not exact"
            )
            ids.append({**row, "reason": reason})

    # Section 3 drops A14 even when the audit marks it exact.
    usable_ab = [ident for ident in AB_USABLE if ident in exact]
    a14_exact = "A14_profitable_oversold_setup" in exact
    peer_exact = "peer_rs" in exact
    hot_ok = hot_score_exact(exact)
    source_exact = [ident for ident in SOURCE_IDS if ident in exact]
    # ohlc_hot and overnight_move have extra requirements. A dropped source
    # is not replaced. The extra requirement only removes that source.
    usable_sources = []
    for ident in source_exact:
        if ident == "ohlc_hot" and not hot_ok:
            continue
        if ident == "overnight_move" and "overnight_gap" not in exact:
            continue
        usable_sources.append(ident)

    ab_survives = len(usable_ab) > 0
    peer_survives = peer_exact
    w1d_buys = ab_survives or peer_survives
    hot4_buys = hot_ok and len(usable_sources) > 0
    return {
        "exact_ids": sorted(exact),
        "dropped_ids": [ident for ident in SECTION4_IDS if ident not in exact],
        "ids": ids,
        "a14_audit_exact": a14_exact,
        "a14_dropped_by_section_3": True,
        "usable_ab": usable_ab,
        "ab_survives": ab_survives,
        "peer_survives": peer_survives,
        "hot_score_usable": hot_ok,
        "usable_sources": usable_sources,
        "variants": {
            "pricefull_w1d": {
                "buys": w1d_buys,
                "reason": (
                    "ab or peer survives"
                    if w1d_buys
                    else "neither ab nor peer survives, so this variant buys nobody"
                ),
            },
            "pricefull_hot4": {
                "buys": hot4_buys,
                "reason": (
                    "hot_score and at least one source are exact"
                    if hot4_buys
                    else "hot_score is not exact or no section 4.2 source is exact, so this variant buys nobody"
                ),
            },
        },
    }


def load_audit(path: Path) -> dict:
    if not path.is_file():
        decision = decide(None)
        decision["file"] = str(path)
        decision["file_present"] = False
        return decision
    text = path.read_text(encoding="utf-8")
    marks = read_marks(text)
    decision = decide(marks)
    decision["file"] = AUDIT_NAME
    decision["file_present"] = True
    return decision
