"""Raw OHLC stays raw. Indicators use split/dividend events known before D."""
from __future__ import annotations

import pandas as pd

from src import price_store as ps


def _use(tmp):
    old = (ps.PRICE_DIR, ps.STORE_PATH, ps.ACTIONS_PATH, ps.META_PATH, list(ps._PENDING_ACTIONS))
    ps.PRICE_DIR = tmp
    ps.STORE_PATH = tmp / "ohlc.parquet"
    ps.ACTIONS_PATH = tmp / "actions.parquet"
    ps.META_PATH = tmp / "meta.json"
    ps._PENDING_ACTIONS.clear()
    ps.reset_action_cache()
    return old


def _restore(old) -> None:
    ps.PRICE_DIR, ps.STORE_PATH, ps.ACTIONS_PATH, ps.META_PATH, pending = old
    ps._PENDING_ACTIONS[:] = pending
    ps.reset_action_cache()


def test_event_on_asof_does_not_adjust_bars_used_for_that_session() -> None:
    bars = [
        {"date": "2026-09-22", "open": 20.0, "high": 21.0, "low": 19.0, "close": 20.0, "volume": 100.0},
        {"date": "2026-09-23", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 200.0},
    ]
    events = pd.DataFrame([{
        "date": "2026-09-24", "ticker": "AAA",
        "dividend": 0.0, "split": 2.0, "close": 11.0,
    }])
    ps.reset_action_cache()
    # asof is the session being scored. The split ex-date is that session,
    # so prior bars stay on the raw print.
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            ps._save_actions(events)
            adjusted = ps.adjust_bars_asof(bars, "2026-09-24", ticker="AAA")
            assert [b["close"] for b in adjusted] == [20.0, 10.0]
            assert bars[0]["close"] == 20.0
        finally:
            _restore(old)


def test_split_and_dividend_before_asof_scale_older_bars_only() -> None:
    bars = [
        {"date": "2026-09-22", "open": 40.0, "high": 42.0, "low": 38.0, "close": 40.0, "volume": 100.0},
        {"date": "2026-09-23", "open": 20.0, "high": 21.0, "low": 19.0, "close": 20.0, "volume": 200.0},
        {"date": "2026-09-24", "open": 19.0, "high": 19.5, "low": 18.0, "close": 19.0, "volume": 150.0},
    ]
    events = pd.DataFrame([
        {
            "date": "2026-09-23", "ticker": "AAA",
            "dividend": 0.0, "split": 2.0, "close": 20.0,
        },
        {
            "date": "2026-09-24", "ticker": "AAA",
            "dividend": 1.0, "split": 0.0, "close": 19.0,
        },
    ])
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            ps._save_actions(events)
            adjusted = ps.adjust_bars_asof(bars, "2026-09-25", ticker="AAA")
            # 09-24 is the dividend ex-date: raw print, then older bars
            # are scaled by (19-1)/19. 09-23 is the 2-for-1 ex-date: raw
            # print, then 09-22 is divided by 2 as well.
            div = (19.0 - 1.0) / 19.0
            assert adjusted[2]["close"] == 19.0
            assert abs(adjusted[1]["close"] - 20.0 * div) < 1e-9
            assert abs(adjusted[0]["close"] - 40.0 / 2.0 * div) < 1e-9
            assert abs(adjusted[0]["volume"] - 100.0 * 2.0) < 1e-9
            assert adjusted[1]["volume"] == 200.0
        finally:
            _restore(old)


def test_missing_actions_file_leaves_bars_raw() -> None:
    bars = [{"date": "2026-09-22", "open": 5.0, "high": 5.0, "low": 5.0, "close": 5.0, "volume": 1.0}]
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            adjusted = ps.adjust_bars_asof(bars, "2026-09-25", ticker="AAA")
            assert adjusted[0]["close"] == 5.0
            assert not ps.ACTIONS_PATH.exists()
            assert not ps.STORE_PATH.exists()
        finally:
            _restore(old)


def test_actions_keep_the_first_factor() -> None:
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as d:
        old = _use(Path(d))
        try:
            first = pd.DataFrame([{
                "date": "2026-09-23", "ticker": "AAA",
                "dividend": 0.0, "split": 2.0, "close": 20.0,
            }])
            ps._save_actions(first)
            second = pd.DataFrame([{
                "date": "2026-09-23", "ticker": "AAA",
                "dividend": 0.0, "split": 4.0, "close": 10.0,
            }, {
                "date": "2026-09-24", "ticker": "AAA",
                "dividend": 0.5, "split": 0.0, "close": 19.0,
            }])
            ps._save_actions(second)
            stored = ps._load_actions()
            row = stored[stored["date"] == pd.Timestamp("2026-09-23")].iloc[0]
            assert float(row["split"]) == 2.0
            assert (stored["date"] == pd.Timestamp("2026-09-24")).any()
        finally:
            _restore(old)


def test_flatten_actions_keeps_real_events_only() -> None:
    idx = pd.to_datetime(["2026-09-23", "2026-09-24"])
    frame = pd.DataFrame({
        "Open": [20.0, 10.0],
        "High": [21.0, 11.0],
        "Low": [19.0, 9.0],
        "Close": [20.0, 10.0],
        "Volume": [100, 200],
        "Dividends": [0.0, 0.25],
        "Stock Splits": [2.0, 0.0],
    }, index=idx)
    raw = pd.concat({"AAA": frame}, axis=1)
    out = ps._flatten_actions(raw, ["AAA"])
    assert len(out) == 2
    assert set(out["ticker"]) == {"AAA"}
    splits = out.set_index(out["date"].dt.strftime("%Y-%m-%d"))["split"].to_dict()
    divs = out.set_index(out["date"].dt.strftime("%Y-%m-%d"))["dividend"].to_dict()
    assert float(splits["2026-09-23"]) == 2.0
    assert float(divs["2026-09-24"]) == 0.25
    ohlc = ps._flatten_yf(raw, ["AAA"])
    assert list(ohlc.columns) == ["date", "ticker", "open", "high", "low", "close", "volume"]
    assert float(ohlc.iloc[0]["close"]) == 20.0


if __name__ == "__main__":
    test_event_on_asof_does_not_adjust_bars_used_for_that_session()
    test_split_and_dividend_before_asof_scale_older_bars_only()
    test_missing_actions_file_leaves_bars_raw()
    test_actions_keep_the_first_factor()
    test_flatten_actions_keeps_real_events_only()
    print("price action tests passed")
