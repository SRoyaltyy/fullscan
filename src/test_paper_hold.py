"""Min-hold is trading sessions, not calendar days.

Run: python -m src.test_paper_hold
"""
from __future__ import annotations

import pandas as pd

from src.paper_trade import (
    calendar_days_held,
    match_roundtrips,
    session_index,
    sessions_held,
    trading_calendar,
)


def test_friday_to_monday_is_one_session() -> None:
    ix = session_index(["2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19"])
    assert sessions_held("2026-08-14", "2026-08-17", ix) == 1
    assert sessions_held("2026-08-14", "2026-08-18", ix) == 2
    assert sessions_held("2026-08-14", "2026-08-14", ix) == 0
    assert calendar_days_held("2026-08-14", "2026-08-17") == 3


def test_one_week_is_five_sessions() -> None:
    days = [
        "2026-08-10", "2026-08-11", "2026-08-12", "2026-08-13", "2026-08-14",
        "2026-08-17",
    ]
    ix = session_index(days)
    assert sessions_held("2026-08-10", "2026-08-17", ix) == 5
    assert calendar_days_held("2026-08-10", "2026-08-17") == 7


def test_roundtrip_uses_sessions_not_calendar() -> None:
    idx = pd.to_datetime(["2026-08-14", "2026-08-17", "2026-08-18"])
    prices = pd.DataFrame({"NRG": [100.0, 99.0, 98.0]}, index=idx)
    trades = [
        {"date": "2026-08-14", "sleeve": "1d_top", "ticker": "NRG",
         "side": "buy", "shares": 8, "price": 126.24, "fees": 2.0,
         "amount": 1011.92},
        {"date": "2026-08-17", "sleeve": "1d_top", "ticker": "NRG",
         "side": "sell", "shares": 8, "price": 122.37, "fees": 2.0,
         "amount": 976.96},
    ]
    ix = session_index(trading_calendar(prices))
    rows = match_roundtrips(trades, prices, session_ix=ix, asof="2026-08-17")
    closed = [r for r in rows if r["status"] == "closed"]
    assert len(closed) == 1, closed
    assert closed[0]["held_sessions"] == 1, closed[0]
    assert closed[0]["held_cal_days"] == 3, closed[0]


def main() -> None:
    test_friday_to_monday_is_one_session()
    test_one_week_is_five_sessions()
    test_roundtrip_uses_sessions_not_calendar()
    print("test_paper_hold: 3 ok")


if __name__ == "__main__":
    main()
