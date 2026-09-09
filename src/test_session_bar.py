"""Official 09:30 / 16:00 marks — not Finviz last-trade, not holidays."""
from __future__ import annotations

import pandas as pd

from src import factor_mine_book as fmb
from src import sleeve_merge as sm
from src import ticker_lookback as tl


def _official(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["date"] = df["date"].astype(str)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    return df.set_index(["date", "ticker"])


def test_labor_day_is_not_a_session() -> None:
    assert tl.is_trading_date("2026-09-04") is True
    assert tl.is_trading_date("2026-09-07") is False
    assert tl.is_trading_date("2026-09-08") is True
    assert "2026-09-07" not in tl.session_dates()
    hol = tl.session_bar("CABA", "2026-09-07")
    assert hol["open"] is None and hol["close"] is None


def test_session_calendar_drops_labor_day() -> None:
    payload = {"session_dates": ["2026-09-04", "2026-09-07", "2026-09-08"]}
    books = [("2026-09-07", None)]
    assert sm.session_calendar(payload, books) == ["2026-09-04", "2026-09-08"]


def test_same_day_finviz_last_trade_is_not_the_close() -> None:
    """finviz_2026-09-08.csv prints CABA 3.56 → 3.89 (live). Official was 3.43 → 3.27."""
    tl.reset_price_caches()
    tl._OHLC_BARS = pd.DataFrame()
    bar = tl.session_bar("CABA", "2026-09-08")
    assert bar["close"] != 3.89
    assert bar["open"] != 3.56
    assert bar["high"] != 3.89
    tl.reset_price_caches()


def test_official_store_wins_over_finviz() -> None:
    tl.reset_price_caches()
    tl._OHLC_BARS = _official([
        {"date": "2026-09-08", "ticker": "CABA",
         "open": 3.43, "high": 3.48, "low": 3.26, "close": 3.27, "volume": 1},
    ])
    bar = tl.session_bar("CABA", "2026-09-08")
    assert abs(bar["open"] - 3.43) < 1e-9
    assert abs(bar["close"] - 3.27) < 1e-9
    assert bar["close"] < 3.50
    tl.reset_price_caches()


def test_later_weekend_finviz_recovers_friday_tape() -> None:
    """finviz_2026-09-05.csv is Saturday's dump of Friday 09-04's official bar."""
    tl.reset_price_caches()
    tl._OHLC_BARS = pd.DataFrame()
    bar = tl.session_bar("CABA", "2026-09-04")
    assert bar["open"] is not None and bar["close"] is not None
    assert abs(bar["open"] - 3.46) < 0.02
    assert abs(bar["close"] - 3.47) < 0.02
    assert bar["open"] != 3.63  # that print is Thursday 09-03
    tl.reset_price_caches()


def test_px_does_not_substitute_close_for_open() -> None:
    bars = {("CABA", "2026-09-08"): {"close": 3.27}}
    assert fmb._px("CABA", "2026-09-08", "open", bars) is None
    assert fmb._px("CABA", "2026-09-08", "close", bars) == 3.27


def test_caba_regular_session_when_store_has_the_bar() -> None:
    """After the store is filled, marks must match Yahoo regular-session prints."""
    tl.reset_price_caches()
    d4 = tl.session_bar("CABA", "2026-09-04")
    d8 = tl.session_bar("CABA", "2026-09-08")
    # Store may still be stale in a checkout that has not run price_store
    # update. Recovery / official row must never reprint the 3.89 live print
    # or Thursday's 3.63 open as Friday.
    if d4["open"] is not None:
        assert abs(d4["open"] - 3.46) < 0.02
        assert abs(d4["close"] - 3.47) < 0.02
    if d8["open"] is not None:
        assert abs(d8["open"] - 3.43) < 0.02
        assert abs(d8["close"] - 3.27) < 0.02
        assert d8["high"] is None or d8["high"] <= 3.50
    assert d8["close"] != 3.89
    tl.reset_price_caches()


if __name__ == "__main__":
    test_labor_day_is_not_a_session()
    test_session_calendar_drops_labor_day()
    test_same_day_finviz_last_trade_is_not_the_close()
    test_official_store_wins_over_finviz()
    test_later_weekend_finviz_recovers_friday_tape()
    test_px_does_not_substitute_close_for_open()
    test_caba_regular_session_when_store_has_the_bar()
    print("test_session_bar: 7 ok")
