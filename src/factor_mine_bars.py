"""Session-date guard for Factor Mine bars.

A bar is the session only when its calendar date in America/New_York is
that session. A date-only Yahoo label is already the session date. A
timezone-aware timestamp is converted first, so a UTC instant that is
still the previous New York date is not this session.

A mismatch is not filled from the previous session. Callers drop the
name (reason ``stale_bar``) or refuse the day, the same way a missing
Yahoo bar is dropped. Locked days already store a bar dated the session,
so the check does not rewrite them.
"""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
STALE_BAR = "stale_bar"
MISSING = "missing"


def ny_bar_date(value) -> str | None:
    """Calendar date of a Yahoo bar in America/New_York.

    ``YYYY-MM-DD`` is already the session date. A timezone-aware timestamp
    is converted before the calendar date is read.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        try:
            if value.tzinfo is not None:
                value = value.astimezone(ET)
            return value.date().isoformat()
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text or text.lower() in {"nat", "nan", "none"}:
        return None
    if len(text) >= 19 and text[4] == "-" and text[7] == "-" and text[10] in " T":
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(ET)
            return parsed.date().isoformat()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return None


def accept_session_bar(bar: dict | None, session: str) -> dict | None:
    """``bar`` when ``bar["date"]`` is ``session`` in America/New_York.

    None when the bar is missing or dated any other session. The returned
    object is the same dict; prices are not copied from another day.
    """
    if not isinstance(bar, dict):
        return None
    day = str(session or "")[:10]
    if ny_bar_date(bar.get("date")) != day:
        return None
    return bar


def usable_session_print(bar: dict | None, session: str) -> bool:
    """True when ``bar`` is not dated a different session.

    A print already looked up by ``session`` often has no date field.
    That print is kept. A date that is not the session in
    America/New_York is refused. Another day's prices are not returned.
    """
    if not isinstance(bar, dict):
        return False
    raw = bar.get("date")
    if raw in (None, ""):
        return True
    return ny_bar_date(raw) == str(session or "")[:10]


def classify_rows(rows: list[dict] | None, session: str) -> tuple[dict | None, str]:
    """The session row, or ``(None, stale_bar|missing)``.

    The last row is not the session row when its date is another session.
    """
    day = str(session or "")[:10]
    found = None
    other = False
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        when = ny_bar_date(row.get("date"))
        if when == day:
            found = row
        elif when:
            other = True
    if found is not None:
        return found, ""
    if other:
        return None, STALE_BAR
    return None, MISSING


def status_for_dates(dates: list[str] | None, session: str, *,
                     has_session_print: bool) -> str:
    """``ok``, ``stale_bar``, or ``missing``.

    ``has_session_print`` is an open or close already stored on ``session``.
    Any other stored date, with no session print, is a stale bar. An empty
    history is missing.
    """
    if has_session_print:
        return "ok"
    day = str(session or "")[:10]
    others = [d for d in (dates or []) if d and d != day]
    if others:
        return STALE_BAR
    return MISSING
