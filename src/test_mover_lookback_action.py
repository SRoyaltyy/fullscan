"""Mover-universe Action — 09:30 list, not same-day gainers."""
from __future__ import annotations

from src import lookback_action as act
from src import mover_lookback_action as mla
from src import ticker_lookback as tl


def test_collect_movers_uses_prior_tape_not_gainers() -> None:
    meta = mla.collect_movers(from_date="2026-09-02", to_date="2026-09-02")
    assert meta["n_sessions"] == 1
    assert meta["tape_of"]["2026-09-02"] == "2026-09-01"
    names = set(meta["by_date"]["2026-09-02"])
    assert len(names) >= 1000
    assert "CVS" not in names
    assert "HIVE" in names
    assert "TSLA" in names
    assert meta["min_atr_pct"] == tl.MIN_ATR_PCT
    assert meta["min_mcap_m"] == 100.0
    assert meta["min_avg_vol_k"] == 500.0


def test_first_session_falls_back_to_same_tape() -> None:
    meta = mla.collect_movers(from_date="2026-08-13", to_date="2026-08-13")
    assert meta["tape_of"]["2026-08-13"] == "2026-08-13"
    assert len(meta["by_date"]["2026-08-13"]) >= 1000


def test_collect_movers_keeps_cvs_when_it_still_moved() -> None:
    meta = mla.collect_movers(from_date="2026-08-17", to_date="2026-08-17")
    tape = meta["tape_of"]["2026-08-17"]
    assert tape < "2026-08-17" or tape == "2026-08-17"
    names = set(meta["by_date"]["2026-08-17"])
    assert "CVS" in names


def test_mover_table_is_not_gainer_list() -> None:
    page = mla.render_html({
        "generated_at": "t",
        "preset": "featured",
        "from_date": "2026-09-02",
        "to_date": "2026-09-02",
        "min_mcap_m": 100,
        "min_avg_vol_k": 500,
        "min_atr_pct": 2.5,
        "n_mover_days": 2,
        "n_called": 1,
        "recall_buy_rate": 0.5,
        "recall_buy": 1,
        "sweeps": {
            "featured": {"mover_days": {
                "n": 2, "n_buy": 1, "n_sell": 0, "n_no_buy": 0, "n_hold": 1,
                "catch": {"1d": 1.0, "3d": None},
                "buy_catch": {"1d": 1.0},
                "sell_catch": {"1d": None},
                "catch_hit": {"1d": 1},
                "catch_n": {"1d": 1},
                "mean_pnl": {"1d": 1.2},
            }},
        },
        "daily": [{
            "date": "2026-09-02", "tape": "2026-09-01", "n": 2,
            "n_buy": 1, "n_sell": 0, "n_no_buy": 0, "n_hold": 1,
            "catch_1d": 1.0, "buy_1d": 1.0, "sell_1d": None, "pnl_1d": 1.2,
        }],
        "called_rows": [{
            "date": "2026-09-02",
            "ticker": "HIVE",
            "action_call": "BUY",
            "action_label": "BUY · 2026-09-02 09:30 ET",
            "action_reason": "lane=probable",
            "cond_tally": "3/1/0",
            "day_change": 2.4,
            "session_bar": {"open": 3.0, "close": 3.1, "close_open_pct": 3.333},
            "horizon_dates": {"1d": "2026-09-03"},
            "price_changes": {"1d": 1.2},
            "hits": {"1d": True, "3d": None, "1w": None},
            "setups": [],
        }],
    })
    assert "Not top gainers" in page
    assert "ATR%" in page
    assert "BUY · 2026-09-02 09:30 ET" in page
    assert "HIVE" in page
    assert "Hits 1d/3d/1w" in page
    assert "✅/—/—" in page
    md = mla.render_markdown({
        "generated_at": "t",
        "preset": "featured",
        "from_date": "2026-09-02",
        "to_date": "2026-09-02",
        "min_mcap_m": 100,
        "min_avg_vol_k": 500,
        "min_atr_pct": 2.5,
        "n_mover_days": 2,
        "n_called": 1,
        "recall_buy_rate": 0.5,
        "recall_buy": 1,
        "sweeps": {},
        "daily": [],
        "called_rows": [],
    })
    assert "Not top gainers" in md
    assert act.OPEN_CLOCK in md


if __name__ == "__main__":
    test_collect_movers_uses_prior_tape_not_gainers()
    test_first_session_falls_back_to_same_tape()
    test_collect_movers_keeps_cvs_when_it_still_moved()
    test_mover_table_is_not_gainer_list()
    print("3 mover-lookback-action tests passed")
