"""Add-only notes for a forward close mark. The sealed log is not an input.

A note lives beside the book log in ``mark_notes.jsonl``. The dashboard
reads a rendered copy. Appending a note does not rewrite a sealed row,
a buy, a sell, or a past-day lock line.

The guard runs after a close mark is already sealed. It compares the
close used for that mark's equity, and the price stored on each holding,
to a fresh Yahoo split-adjusted close. A gap wider than 50% is logged
and recorded. It is not corrected in the sealed record, and a failed
compare does not stop the seal.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from research.hot_n4_clean_v4.forward.book import current_book
from research.hot_n4_clean_v4.run_study import mark_px

NOTES_NAME = "mark_notes.jsonl"
PAGE_NAME = "mark_notes.json"
OFF_FRACTION = 0.50
WARNING_KIND = "close_mark_warning"


def notes_path(folder: Path | None = None) -> Path:
    return (folder or current_book().folder) / NOTES_NAME


def page_notes_path(page: Path | None = None) -> Path:
    return (page or current_book().page) / PAGE_NAME


def load_notes(path: Path | None = None) -> list[dict]:
    dest = path or notes_path()
    if not dest.is_file():
        return []
    rows = []
    for line in dest.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def assert_notes_append_only(base_text: str, head_text: str) -> None:
    """Existing note lines are a prefix. A run may only append."""
    base = [line for line in base_text.splitlines() if line.strip()]
    head = [line for line in head_text.splitlines() if line.strip()]
    if head[: len(base)] != base:
        raise ValueError(
            "mark notes are add-only: an existing line was edited, removed, or reordered"
        )


def _dumps(row: dict) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def append_note(row: dict, path: Path | None = None) -> None:
    """Append one JSON line. Bytes already in the file stay as they are."""
    dest = path or notes_path()
    dest.parent.mkdir(parents=True, exist_ok=True)
    prior = dest.read_text(encoding="utf-8") if dest.is_file() else ""
    if prior and not prior.endswith("\n"):
        raise ValueError("mark notes are add-only: the file has no trailing newline")
    head = prior + _dumps(row) + "\n"
    assert_notes_append_only(prior, head)
    dest.write_text(head, encoding="utf-8")


def publish_notes(folder: Path | None = None, page: Path | None = None) -> Path:
    """Write the dashboard copy. This file is a render, not the sealed log."""
    rows = load_notes(notes_path(folder))
    dest = page_notes_path(page)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    return dest


def px_text(value: float) -> str:
    text = f"{float(value):.6f}".rstrip("0").rstrip(".")
    return text or "0"


def equity_text(value: float) -> str:
    return json.dumps(float(value))


def relative_gap(price: float, yahoo: float) -> float:
    return abs(float(price) - float(yahoo)) / float(yahoo)


def is_off(price: float, yahoo: float) -> bool:
    """True when ``price`` is more than 50% away from the Yahoo close."""
    if yahoo is None:
        return False
    try:
        yahoo_px = float(yahoo)
        mark_px_value = float(price)
    except (TypeError, ValueError):
        return False
    if yahoo_px <= 0:
        return False
    return relative_gap(mark_px_value, yahoo_px) > OFF_FRACTION


def valuation_close(stored: dict, lot: dict, session: str) -> float | None:
    """The close that entered equity for this lot. Same rule as the fill."""
    px = mark_px(stored, str(lot.get("ticker") or ""), session)
    if px is None:
        raw = lot.get("last_px")
        if raw is None:
            raw = lot.get("entry_px")
        if raw is None:
            return None
        px = raw
    try:
        value = float(px)
    except (TypeError, ValueError):
        return None
    return value


def implied_equity(mark: dict, closes: dict[str, float]) -> float:
    """Cash plus each lot at ``closes``. A missing name uses no substitute."""
    stock = 0.0
    for lot in mark.get("holdings") or []:
        ticker = str(lot["ticker"])
        if ticker not in closes:
            raise KeyError(ticker)
        stock += int(lot["shares"]) * float(closes[ticker])
    return float(mark["cash_primary"]) + stock


def fetch_session_closes(tickers: list[str], session: str, fetch=None) -> dict[str, float]:
    """Yahoo split-adjusted close for ``session``. ``end`` is exclusive.

    This is the same ``auto_adjust=False`` download the price store locks.
    A name with no bar for that session is omitted.
    """
    if fetch is None:
        from research.hot_n4_clean_v4.forward.prices import fetch_yahoo

        fetch = fetch_yahoo
    if not tickers:
        return {}
    end = (date.fromisoformat(session) + timedelta(days=1)).isoformat()
    payload = fetch(list(tickers), session, end)
    if not isinstance(payload, dict):
        raise RuntimeError("Yahoo close compare returned no payload")
    if payload.get("error"):
        raise RuntimeError(str(payload["error"]))
    out: dict[str, float] = {}
    for bar in payload.get("bars") or []:
        if str(bar.get("date") or "")[:10] != session:
            continue
        ticker = str(bar.get("ticker") or "").upper()
        try:
            close = float(bar["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if close > 0 and ticker:
            out[ticker] = close
    return out


def _holding_price(lot: dict) -> float | None:
    raw = lot.get("last_px")
    if raw is None:
        raw = lot.get("entry_px")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def book_label(folder: Path | None = None) -> str:
    name = (folder or current_book().folder).name
    if name == "forward_h1":
        return "h1"
    if name == "forward":
        return "holdup"
    return name


def _warning_note(
    mark: dict,
    flagged: list[dict],
    yahoo: dict[str, float],
    stored: dict,
    folder: Path | None,
) -> dict:
    session = str(mark["date"])
    closes: dict[str, float] = {}
    for lot in mark.get("holdings") or []:
        ticker = str(lot["ticker"])
        if ticker in yahoo:
            closes[ticker] = float(yahoo[ticker])
            continue
        value = valuation_close(stored, lot, session)
        if value is None:
            raise KeyError(ticker)
        closes[ticker] = value
    implied = implied_equity(mark, closes)
    sealed = float(mark["equity_primary"])
    bits = []
    for row in flagged:
        yahoo_px = float(row["yahoo_close"])
        described = []
        if is_off(row["valuation_close"], yahoo_px):
            described.append(f"valuation close {px_text(row['valuation_close'])}")
        if is_off(row["sealed_mark"], yahoo_px) and float(row["sealed_mark"]) != float(row["valuation_close"]):
            described.append(f"holding price {px_text(row['sealed_mark'])}")
        if not described:
            described.append(f"holding price {px_text(row['sealed_mark'])}")
        gap = max(
            relative_gap(price, yahoo_px)
            for price, flag in (
                (row["valuation_close"], is_off(row["valuation_close"], yahoo_px)),
                (row["sealed_mark"], is_off(row["sealed_mark"], yahoo_px)),
            )
            if flag
        )
        bits.append(
            f"{row['ticker']} {' and '.join(described)} is {gap * 100:.1f}% "
            f"from the Yahoo split-adjusted close {px_text(yahoo_px)}"
        )
    implied_s = equity_text(implied)
    sealed_s = equity_text(sealed)
    note = (
        f"Close-mark warning {session}: {'; '.join(bits)}. "
        f"Implied equity at Yahoo closes is {implied_s}. "
        f"Sealed equity {sealed_s} is unchanged."
    )
    return {
        "book": book_label(folder),
        "close_source": f"Yahoo split-adjusted {session} daily close",
        "date": session,
        "implied_equity": implied_s,
        "kind": WARNING_KIND,
        "note": note,
        "sealed_equity": sealed_s,
        "tickers": flagged,
    }


def guard_close_mark(
    mark: dict,
    bars: dict,
    *,
    fetch=None,
    folder: Path | None = None,
    page: Path | None = None,
    log=print,
) -> list[dict]:
    """Log and record a >50% Yahoo gap. Does not change ``mark`` or raise.

    ``fetch`` defaults to the locked Yahoo download. Tests pass their own.
    A download error, a missing close, or a note-file error is logged and
    swallowed so the sealed mark stays the record.
    """
    try:
        return _guard_close_mark(mark, bars, fetch=fetch, folder=folder, page=page, log=log)
    except Exception as exc:  # noqa: BLE001 — the warning is not allowed to block the seal
        log(f"close-mark guard did not block the seal ({exc})")
        return []


def _guard_close_mark(mark, bars, *, fetch, folder, page, log) -> list[dict]:
    if mark.get("kind") not in ("mark", "fill"):
        return []
    if "equity_primary" not in mark:
        return []
    session = str(mark.get("date") or "")
    holdings = list(mark.get("holdings") or [])
    if not session or not holdings:
        return []
    stored = (bars or {}).get("stored") or {}
    names = [str(lot["ticker"]) for lot in holdings if lot.get("ticker")]
    try:
        yahoo = fetch_session_closes(names, session, fetch)
    except Exception as exc:  # noqa: BLE001 — no Yahoo print means no flag, not a failed seal
        log(f"close-mark guard: Yahoo compare skipped for {session} ({exc})")
        return []
    flagged = []
    for lot in holdings:
        ticker = str(lot.get("ticker") or "")
        if not ticker:
            continue
        yahoo_px = yahoo.get(ticker)
        if yahoo_px is None:
            log(f"close-mark guard: no Yahoo close for {ticker} {session}")
            continue
        value = valuation_close(stored, lot, session)
        held = _holding_price(lot)
        if value is None or held is None:
            log(f"close-mark guard: no sealed close for {ticker} {session}")
            continue
        if not is_off(value, yahoo_px) and not is_off(held, yahoo_px):
            continue
        row = {
            "sealed_mark": held,
            "ticker": ticker,
            "valuation_close": value,
            "yahoo_close": float(yahoo_px),
        }
        flagged.append(row)
        log(
            f"close-mark warning {session} {ticker} "
            f"valuation {px_text(value)} holding {px_text(held)} "
            f"yahoo {px_text(yahoo_px)}"
        )
    if not flagged:
        return []
    note = _warning_note(mark, flagged, yahoo, stored, folder)
    append_note(note, notes_path(folder))
    if page is not None or folder is None:
        publish_notes(folder, page)
    return [note]


def warn_sealed_close(body: dict, bars: dict) -> None:
    """After the seal. A guard failure does not change the caller's result."""
    if body.get("kind") not in ("mark", "fill") or "equity_primary" not in body:
        return
    book = current_book()
    guard_close_mark(body, bars, folder=book.folder, page=book.page)
