"""Price features for the Group 3 run.

Features read Yahoo split-adjusted daily bars dated strictly before the
session. The session open is the 09:30 fill, and the gap against the prior
close, only when the frozen recipe trades at the open.
"""
from __future__ import annotations

BAR_PATH = "data/prices/ohlc.parquet"
BAR_COMMIT = "ff996f535e1343dd739cc801780ae224018bd96c"
BAR_BLOB_SHA = "3456f7f489a6fa7033e8ae5cc942d8279f0113e3"
BAR_SHA256 = "559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55"
# Yahoo download with auto_adjust=False. Splits are in the print.
# Dividends are not. auto_adjust=True is refused by the price store.
BAR_ADJUSTMENT = "split-adjusted, dividends not applied"


class SameDayBarError(Exception):
    """A feature read saw a bar dated the session or later."""


def _date(bar: dict) -> str:
    return str(bar["date"])


def feature_bars(bars: list[dict], session: str) -> list[dict]:
    """Return bars the feature engine may read. Same-day or later bars raise."""
    chosen: list[dict] = []
    for bar in bars:
        if _date(bar) >= session:
            raise SameDayBarError(
                f"same-day-or-later bar {_date(bar)} for session {session}"
            )
        chosen.append(bar)
    return chosen


def fill_open(session_bar: dict, *, trades_at_open: bool) -> float:
    """Session open for a 09:30 fill. High, low, and close of that bar are refused.

    Overnight and gap use this open only when the recipe trades at the open,
    so the gap versus the prior close is known at 09:30. A recipe that does
    not trade at the open does not read the open.
    """
    if not trades_at_open:
        raise SameDayBarError("session open")
    extra = set(session_bar) - {"date", "open"}
    if extra:
        raise SameDayBarError(
            "same-day field " + ",".join(sorted(extra))
        )
    return float(session_bar["open"])
