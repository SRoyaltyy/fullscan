"""Sandbox account drift versus one day's sealed h1 tickets.

The sealed book decides the tickets. The sandbox account does not.
A sell the account does not hold is ``drift-skipped`` and is not sent.
This module does not add a catch-up buy or sell, does not reuse another
day's tickets, and does not fail the rest of the day's sealed list.

``data/paper_open/drift_log.jsonl`` is append-only. Callers add a line.
They do not rewrite earlier lines, including the 2026-10-06 gap.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DRIFT_LOG = ROOT / "data" / "paper_open" / "drift_log.jsonl"
DRIFT_SKIPPED = "drift-skipped"


def _shares(positions, ticker: str) -> int:
    raw = (positions or {}).get(ticker)
    if raw is None:
        raw = (positions or {}).get(str(ticker).upper())
    if isinstance(raw, dict):
        try:
            n = int(float(raw.get("shares") or 0))
        except (TypeError, ValueError):
            return 0
        return n if n > 0 else 0
    try:
        n = int(float(raw or 0))
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def append_drift(record: dict, path: Path | None = None) -> None:
    """Add one JSON line. Never reads the file back to rewrite it."""
    path = Path(path) if path is not None else DRIFT_LOG
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(
        record, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    )
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def partition_tickets(tickets, positions, date: str, *, positions_known=True):
    """Split today's sealed tickets into sendable and drift-skipped.

    A ticket dated for another session is not sent. A sealed sell the
    sandbox account does not hold is not sent. Buys are unchanged.
    Holdings that are not on today's list are named on the warning and
    are not turned into orders. When the snapshot could not be read,
    ``positions_known`` is false: sells are not skipped and the day is
    not blocked.
    """
    sendable = []
    skipped = []
    foreign = []
    on_plan = set()
    for ticket in tickets or []:
        if not isinstance(ticket, dict):
            continue
        ticker = str(ticket.get("ticker") or "").upper().strip()
        side = str(ticket.get("side") or "").upper()
        ticket_date = str(ticket.get("date") or date)
        if ticket_date != date:
            foreign.append({
                "ticker": ticker,
                "side": side,
                "shares": ticket.get("shares"),
                "date": ticket_date,
                "status": DRIFT_SKIPPED,
                "reason": f"refusing ticket dated {ticket_date}; session is {date}",
            })
            continue
        on_plan.add(ticker)
        if positions_known and side == "SELL" and _shares(positions, ticker) < 1:
            skipped.append({
                "ticker": ticker,
                "side": "SELL",
                "shares": ticket.get("shares"),
                "date": date,
                "status": DRIFT_SKIPPED,
                "ok": False,
                "reason": (
                    f"drift-skipped: sealed h1 sell {ticker} "
                    f"{ticket.get('shares')} but the sandbox account does not hold it"
                ),
            })
            continue
        sendable.append(ticket)
    held_not_on_plan = []
    for ticker in sorted(positions or {}) if positions_known else []:
        name = str(ticker).upper()
        shares = _shares(positions, name)
        if shares < 1 or name in on_plan:
            continue
        held_not_on_plan.append({
            "ticker": name,
            "shares": shares,
            "note": "held on the sandbox account; not on today's sealed tickets; no order",
        })
    warning = None
    if skipped or foreign or held_not_on_plan:
        warning = {
            "kind": "warning",
            "date": date,
            "drift_skipped": skipped + foreign,
            "held_not_on_plan": held_not_on_plan,
            "note": (
                "account drift is a warning; no catch-up order and the "
                "sealed ticket check is not blocked"
            ),
        }
    return sendable, skipped, foreign, warning
