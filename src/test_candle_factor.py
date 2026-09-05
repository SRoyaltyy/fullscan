"""Leak-free red:green / volume / candlestick factor."""
from __future__ import annotations

from src import candle_factor as cf
from src import flatten_lookback_action as fla


def test_prior_bars_never_include_asof() -> None:
    bars = cf.prior_bars("TLN", "2026-08-14")
    assert bars, "need OHLC history for TLN"
    assert all(b["date"] < "2026-08-14" for b in bars)
    assert bars[-1]["date"] <= "2026-08-13"


def test_features_are_prior_only_and_have_rg() -> None:
    feat = cf.features("TLN", "2026-08-14")
    assert feat["ok"] is True
    assert feat["asof"] == "2026-08-14"
    assert feat["n"] >= 2
    assert feat["body_rg"] is not None
    assert feat["vol_rg"] is not None
    assert "engulf_bull" in feat
    assert "hammer" in feat
    assert "morning_star" in feat


def test_synthetic_engulf_and_hammer() -> None:
    engulf = cf._from_bars([
        {"date": "2026-08-12", "open": 10, "high": 10.2, "low": 9.4,
         "close": 9.5, "volume": 100},
        {"date": "2026-08-13", "open": 9.4, "high": 10.4, "low": 9.3,
         "close": 10.3, "volume": 180},
    ])
    assert engulf["engulf_bull"] is True
    assert engulf["last_green"] is True
    assert engulf["vol_rg"] > 1
    hammer = cf._from_bars([
        {"date": "2026-08-12", "open": 10, "high": 10.1, "low": 9.8,
         "close": 9.9, "volume": 80},
        {"date": "2026-08-13", "open": 10.0, "high": 10.1, "low": 9.0,
         "close": 9.9, "volume": 120},
    ])
    assert hammer["hammer"] is True
    shoot = cf._from_bars([
        {"date": "2026-08-12", "open": 10, "high": 10.2, "low": 9.8,
         "close": 10.1, "volume": 80},
        {"date": "2026-08-13", "open": 10.2, "high": 11.0, "low": 10.15,
         "close": 10.3, "volume": 90},
    ])
    assert shoot["shooting_star"] is True
    assert cf.keep(shoot, 0.0, "drop_bear") is False
    assert cf.keep(engulf, 0.0, "need_bull") is True


def test_missing_bars_do_not_veto() -> None:
    empty = cf._empty()
    assert cf.keep(empty, -5.0, "combo") is True
    assert cf.capture(empty) is False


def test_flatten_814_still_tln_with_candle_labels() -> None:
    plan = fla.flatten_day_targets("2026-08-14")
    names = [t.upper() for t in plan["tickers"]]
    assert names[:3] == ["TLN", "VST", "NRG"]
    feat = cf.features("TLN", "2026-08-14")
    assert feat["ok"]
    assert all(b["date"] < "2026-08-14" for b in cf.prior_bars("TLN", "2026-08-14"))


if __name__ == "__main__":
    test_prior_bars_never_include_asof()
    test_features_are_prior_only_and_have_rg()
    test_synthetic_engulf_and_hammer()
    test_missing_bars_do_not_veto()
    test_flatten_814_still_tln_with_candle_labels()
    print("5 candle-factor tests passed")
