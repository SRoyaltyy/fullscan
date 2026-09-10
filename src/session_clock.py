"""When a US session is actually closed — not when a pre-open file exists.

Start-on chips grade from that morning's 09:30 through the last finished
16:00 print. A 09:10 ET book for *today* must not flatten yesterday to 0%.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def session_has_closed(date: str, now: datetime | None = None) -> bool:
    """True after that session's 16:00 ET print is knowable."""
    now = now or datetime.now(ET)
    if now.tzinfo is None:
        now = now.replace(tzinfo=ET)
    else:
        now = now.astimezone(ET)
    today = now.strftime("%Y-%m-%d")
    if not date:
        return False
    if date < today:
        return True
    if date > today:
        return False
    close = now.replace(hour=16, minute=2, second=0, microsecond=0)
    return now >= close


def last_closed_session(from_date: str, to_date: str | None = None,
                        cal: list[str] | None = None) -> str | None:
    """Last calendar date whose regular session has actually finished."""
    dates = [
        d for d in (cal or [])
        if d >= from_date and (not to_date or d <= to_date)
        and session_has_closed(d)
    ]
    return dates[-1] if dates else None
