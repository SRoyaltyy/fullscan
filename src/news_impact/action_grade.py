"""Grade a frozen ACTION against the tape.

Uses news_impact.grade entry clock (next regular session; at/after 09:30 ET
rolls to the next session; the next bar is Monday when that session is a
weekend). Does not fetch prices during classify or pack. Call only after
the ACTION string is frozen.
"""
from __future__ import annotations

import re
from datetime import timedelta

from src.news_impact.grade import grade_one, load_book, parse_when
from src.news_impact.hygiene import skips_01d_horizon

_ACTION_BIT = re.compile(
    r"\b(BUY|SELL|AVOID_ADD)\s+([A-Z][A-Z0-9./]{0,80}?),\s*"
    r"([A-Za-z0-9+\-]+),\s*because\b"
)
_WINDOWS = (
    ("0-1d", "ret_1d"),
    ("2d", "ret_2d"),
    ("3d", "ret_3d"),
    ("4d", "ret_4d"),
    ("5d", "ret_5d"),
    ("1-4w", "ret_20d"),
)
_NATIVE_FIELD = {
    "0-1d": "ret_1d",
    "2d": "ret_2d",
    "3d": "ret_3d",
    "4d": "ret_4d",
    "5d": "ret_5d",
    "1-4w": "ret_20d",
    "1-6m": "ret_63d",
    "6m+": "ret_horizon",
}
GOLD_FOUR = ("tsa", "buist", "tsv", "amrx")


def parse_action(action: str) -> list[dict]:
    """Tickers and verbs from a frozen ACTION line. Empty if there is none."""
    out = []
    for match in _ACTION_BIT.finditer(action or ""):
        verb = match.group(1).upper()
        horizon = match.group(3)
        direction = {"BUY": "up", "SELL": "down"}.get(verb, "avoid")
        for tick in re.findall(r"[A-Z]{1,5}", match.group(2)):
            out.append({
                "verb": verb,
                "ticker": tick,
                "horizon": horizon,
                "direction": direction,
            })
    return out


def _reason(grade: dict, ret: float | None) -> str:
    note = str(grade.get("note") or "")
    if ret is not None:
        return ""
    if "no session within" in note:
        return "halt"
    if note.startswith("no tape") or note.startswith("no published") or note.startswith("no listed"):
        return "no_price"
    if "bad entry" in note:
        return "no_price"
    return "too_new"


def _cell(horizon: str, ret, verdict: str) -> dict:
    if ret is None:
        text = f"n/a {verdict}"
    else:
        text = f"{float(ret):+.2f}% {verdict}"
    return {"horizon": horizon, "ret_pct": ret, "verdict": verdict, "text": text}


def grade_row_action(row: dict, book: dict[str, list[dict]]) -> dict:
    """One kept row. Prices come from `book` bars dated on/after the entry session."""
    clock = str(row.get("clock") or "")
    reprint = str((row.get("history") or {}).get("history_state") or "") == "reprint"
    flag = "reprint — not a first_print test" if reprint else ""
    when = parse_when(row.get("known_at"))
    legs = []
    for leg in parse_action(str(row.get("action") or "")):
        native = str(leg["horizon"] or "0-1d")
        target = {
            "ticker": leg["ticker"],
            "name": leg["ticker"],
            "direction": "not_determined" if leg["verb"] == "AVOID_ADD" else leg["direction"],
            "horizon": native if native in {"0-1d", "1-4w", "1-6m", "6m+"} else "0-1d",
            "event_class": row.get("event_class") or "",
            "q5": "impulse",
            "tradeable_expression": "direct",
            "kind": "ticker",
        }
        graded = grade_one(target, book.get(leg["ticker"]) or [], when)
        suppress_01d = (
            clock in {"monday_open", "not_0_1d"}
            or native != "0-1d"
            or skips_01d_horizon(
                {"event_class": row.get("event_class") or "", "horizon": native},
                row,
            )
        )
        cells = []
        for name, field in _WINDOWS:
            ret = graded.get(field)
            if ret is None:
                verdict = _reason(graded, None) or "too_new"
            elif name == "0-1d" and suppress_01d:
                verdict = "not_scored"
            elif leg["verb"] == "AVOID_ADD":
                verdict = "did it fall" if float(ret) < 0 else "did not fall"
            else:
                up = leg["direction"] == "up"
                verdict = "agree" if ((up and float(ret) > 0) or (not up and float(ret) < 0)) else "disagree"
            cells.append(_cell(name, ret, verdict))
        native_field = _NATIVE_FIELD.get(native)
        native_ret = graded.get(native_field) if native_field else None
        if native not in {name for name, _ in _WINDOWS} and native_field:
            if native_ret is None:
                native_verdict = _reason(graded, None) or "too_new"
            elif leg["verb"] == "AVOID_ADD":
                native_verdict = "did it fall" if float(native_ret) < 0 else "did not fall"
            else:
                up = leg["direction"] == "up"
                native_verdict = (
                    "agree" if ((up and float(native_ret) > 0) or (not up and float(native_ret) < 0))
                    else "disagree"
                )
            cells.append(_cell(native, native_ret, native_verdict))
        scored = False
        hit = None
        unscored = ""
        if reprint:
            unscored = "reprint"
        elif leg["verb"] == "AVOID_ADD":
            unscored = "avoid_add"
        elif native_ret is None:
            unscored = _reason(graded, None) or "too_new"
        elif native == "0-1d" and suppress_01d:
            unscored = "too_new"
        else:
            scored = True
            up = leg["direction"] == "up"
            hit = (up and float(native_ret) > 0) or ((not up) and float(native_ret) < 0)
        legs.append({
            "ticker": leg["ticker"],
            "verb": leg["verb"],
            "native_window": native,
            "native_clock": clock,
            "entry_date": graded.get("entry_date"),
            "scored": scored,
            "hit": hit,
            "unscored_reason": unscored,
            "reprint": reprint,
            "cells": cells,
        })
    return {
        "native_clock": clock,
        "flag": flag,
        "legs": legs,
    }


def _empty_bucket() -> dict:
    return {"hits": 0, "n_scored": 0, "n_unscored": 0, "no_price": 0, "halt": 0, "too_new": 0}


def summarize_tape(rows: list[dict]) -> dict:
    """Hit rates for the gold four, separately from every other kept row."""
    gold = _empty_bucket()
    rest = _empty_bucket()
    out = {
        "gold_four": gold,
        "rest": rest,
        "n_scored": 0,
        "n_unscored": 0,
        "no_price": 0,
        "halt": 0,
        "too_new": 0,
        "n_reprint": 0,
        "n_avoid": 0,
    }
    for row in rows or []:
        bucket = gold if (row.get("gold_id") or "") in GOLD_FOUR else rest
        for leg in (row.get("tape") or {}).get("legs") or []:
            if leg.get("reprint"):
                out["n_reprint"] += 1
                continue
            if leg.get("verb") == "AVOID_ADD":
                out["n_avoid"] += 1
                continue
            if leg.get("scored"):
                bucket["n_scored"] += 1
                out["n_scored"] += 1
                if leg.get("hit"):
                    bucket["hits"] += 1
                continue
            reason = str(leg.get("unscored_reason") or "too_new")
            if reason == "reprint":
                out["n_reprint"] += 1
                continue
            if reason not in {"no_price", "halt", "too_new"}:
                reason = "too_new"
            bucket["n_unscored"] += 1
            bucket[reason] += 1
            out["n_unscored"] += 1
            out[reason] += 1
    return out


def grade_action_rows(
    rows: list[dict],
    *,
    book: dict[str, list[dict]] | None = None,
    fetch: bool = False,
) -> dict:
    """Attach tape grades. `fetch` uses the existing load_book yfinance tail."""
    tickers: list[str] = []
    whens = []
    for row in rows or []:
        for leg in parse_action(str(row.get("action") or "")):
            tickers.append(leg["ticker"])
        dt = parse_when(row.get("known_at"))
        if dt is not None:
            whens.append(dt)
    if book is None:
        start = "2020-01-01"
        if whens:
            start = (min(whens) - timedelta(days=7)).date().isoformat()
        book = load_book(tickers, start, fetch=fetch) if tickers else {}
    for row in rows or []:
        row["tape"] = grade_row_action(row, book)
    return summarize_tape(rows)
