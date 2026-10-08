"""Share counts locked at send time for the h1 book.

The morning wire sizes new buys from Finviz ``fv_price`` and writes those
counts on the paper submit journal (``data/paper_open/{date}_submit.json``,
card tickets with ``sealed_shares``). That journal is the lock. The
afternoon open fill keeps those counts. It may still price, cost, and fee
the fill at the session open. It must not size the shares from that open.

The lock starts 2026-10-09. 2026-10-08 and every earlier open fill stay as
written. That day the wire was NAUT 1,250 and PENG 33, and the sealed open
fill is NAUT 1,234 at 2.00 and PENG 34 at 72.50. The journal for that day
is not a reason to recompute it. A missing lock on a later h1 session
refuses the fill instead of sizing from the open. A session that already
has an open fill is not rewritten.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

# First session whose new buys keep the sent count. Earlier sessions,
# including the sealed 2026-10-08 open fill, keep the open-price size.
LOCK_FROM = "2026-10-09"

# Passed when the caller did not choose a lock. ``None`` means the old
# open-price size, which is what every sealed day before LOCK_FROM uses.
AUTO = object()


class SentShareLockMissing(RuntimeError):
    """An h1 session on or after LOCK_FROM has no sent share count."""


def _root() -> Path:
    raw = os.environ.get("SENT_SHARE_LOCK_ROOT")
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[3]


def journal_path(date: str) -> Path:
    """Submit journal the morning wire already writes at send time."""
    return _root() / "data" / "paper_open" / f"{date}_submit.json"


def buys_from_journal(payload: dict, date: str) -> dict[str, int]:
    """Sealed BUY share counts on a paper submit journal.

    The card is the count computed at send time. An acknowledged ``sent``
    row that disagrees fails closed. Nothing here picks a new size.
    """
    if not isinstance(payload, dict):
        raise SentShareLockMissing(f"sent share lock for {date} is not an object")
    stamped = payload.get("date")
    if stamped not in (None, date):
        raise SentShareLockMissing(
            f"sent share lock is for {stamped}, not {date}"
        )
    card = payload.get("card") if isinstance(payload.get("card"), dict) else {}
    locked: dict[str, int] = {}
    for ticket in card.get("tickets") or []:
        if not isinstance(ticket, dict):
            continue
        if str(ticket.get("side") or "").upper() != "BUY":
            continue
        if not ticket.get("sealed_shares"):
            continue
        ticker = str(ticket.get("ticker") or "").upper().strip()
        if not ticker or ticker in locked:
            raise SentShareLockMissing(
                f"sent share lock for {date} buy {ticker or '?'} is unusable"
            )
        try:
            shares = int(ticket.get("shares"))
        except (TypeError, ValueError) as exc:
            raise SentShareLockMissing(
                f"sent share lock for {date} {ticker} has no share count"
            ) from exc
        if shares < 1:
            raise SentShareLockMissing(
                f"sent share lock for {date} {ticker} is {shares}"
            )
        locked[ticker] = shares
    for row in payload.get("sent") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("side") or "").upper() != "BUY":
            continue
        if row.get("skipped") or row.get("status") in ("skipped_cash",):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if ticker not in locked or row.get("shares") is None:
            continue
        try:
            sent = int(row.get("shares"))
        except (TypeError, ValueError) as exc:
            raise SentShareLockMissing(
                f"sent row for {date} {ticker} has no share count"
            ) from exc
        if sent != locked[ticker]:
            raise SentShareLockMissing(
                f"sent share count for {ticker} is {sent} but the locked "
                f"count is {locked[ticker]}; refusing to pick one"
            )
    return locked


def _h1_book() -> bool:
    from research.hot_n4_clean_v4.forward.book import current_book

    return current_book().folder_name == "forward_h1"


def resolve_share_lock(date: str, share_lock=AUTO):
    """The lock to apply, or None when this fill still sizes from the open.

    An explicit dict is that lock. ``None`` forces the open-price size.
    ``AUTO`` reads the submit journal only for the h1 book on or after
    ``LOCK_FROM``. A missing journal there raises. Earlier dates, and the
    holdup book, do not read the journal.
    """
    if share_lock is not AUTO:
        return share_lock
    if date < LOCK_FROM or not _h1_book():
        return None
    path = journal_path(date)
    if not path.is_file():
        raise SentShareLockMissing(
            f"sent share lock missing for {date}; refusing to size buys from the open"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SentShareLockMissing(
            f"sent share lock for {date} cannot be read; refusing to size buys from the open"
        ) from exc
    return buys_from_journal(payload, date)
