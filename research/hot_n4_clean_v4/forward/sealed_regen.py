"""Refuse to regenerate a sealed h1 (or holdup) day's buy/sell list.

Factor Mine may recompute a past day and overwrite that day's buys and
sells. This book must not. ``regenerate_day`` is the regenerate path: a
day that already has a buy/sell line fails, and nothing is written. A
new day is appended by the forward runner, not by this function.
"""
from __future__ import annotations

from pathlib import Path

from research.hot_n4_clean_v4.forward.book import BOOKS, reset_book, use_book
from research.hot_n4_clean_v4.forward.ledger import ledger_path, load, log_path


class SealedDayRegen(RuntimeError):
    """A sealed day's buy/sell list was not rewritten."""


def _leg(row: dict) -> tuple:
    fill = row.get("fill")
    return (
        str(row.get("ticker") or ""),
        int(row.get("shares") or 0),
        None if fill is None else float(fill),
    )


def legs(rows: list | None) -> tuple:
    return tuple(sorted(_leg(row) for row in rows or []))


def buy_sell_record(records: list[dict], day: str) -> dict | None:
    """The sealed buy/sell line for ``day``.

    A session, an open fill, or a fill carries that day's buys and sells.
    A later mark does not replace that line.
    """
    found = None
    for row in records:
        if row.get("date") != day:
            continue
        if row.get("kind") in ("session", "open_fill", "fill"):
            found = row
    return found


def same_buy_sell(record: dict, buys: list, sells: list) -> bool:
    return legs(record.get("buys")) == legs(buys) and legs(record.get("sells")) == legs(sells)


def replay_buy_sell(records: list[dict], day: str) -> dict:
    """The buy/sell list already sealed for ``day``.

    Reading the sealed line again is the same list. This does not write.
    """
    record = buy_sell_record(records, day)
    if record is None:
        raise SealedDayRegen(f"{day} has no sealed buy/sell line")
    return {
        "buys": list(record.get("buys") or []),
        "date": day,
        "sells": list(record.get("sells") or []),
    }


def regenerate_day(folder: Path, day: str, buys: list, sells: list) -> None:
    """Regenerate ``day``. A sealed day fails and the old line stays.

    The log and the ledger are read and then compared. This function does
    not open either file for write. The same list and a conflicting list
    both fail: a sealed line is not rewritten to itself.
    """
    folder = Path(folder)
    book = next((item for item in BOOKS if item.folder.resolve() == folder.resolve()), None)
    token = use_book(book) if book is not None else None
    try:
        log = log_path(folder)
        led = ledger_path(folder)
        before_log = log.read_bytes() if log.is_file() else b""
        before_led = led.read_bytes() if led.is_file() else b""
        records = load(folder)
        sealed = buy_sell_record(records, day)
        if log.is_file() and log.read_bytes() != before_log:
            raise RuntimeError("regenerate rewrote the log")
        if led.is_file() and led.read_bytes() != before_led:
            raise RuntimeError("regenerate rewrote the ledger")
    finally:
        if token is not None:
            reset_book(token)
    if sealed is None:
        raise SealedDayRegen(
            f"{day} has no sealed buy/sell line; append the next day through the forward book"
        )
    if same_buy_sell(sealed, buys, sells):
        raise SealedDayRegen(f"{day} is sealed; same buy/sell list is not rewritten")
    raise SealedDayRegen(f"{day} is sealed; conflicting buy/sell list was refused")
