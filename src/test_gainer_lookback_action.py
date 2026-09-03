"""Gainer universe + catch scoring, no full-history walk."""
from __future__ import annotations

from src import gainer_lookback_action as gla
from src import lookback_action as act


def test_collect_gainers_is_liquid_top() -> None:
    meta = gla.collect_gainers(
        from_date="2026-08-13", to_date="2026-08-13", top_n=15, min_change=2.0)
    assert meta["n_sessions"] == 1
    assert 0 < meta["n_tickers"] <= 15
    rows = meta["by_date"]["2026-08-13"]
    assert rows
    assert rows[0]["change_pct"] >= rows[-1]["change_pct"]
    assert all(r["change_pct"] >= 2.0 for r in rows)


def test_score_rows_buy_catch() -> None:
    rows = [
        {
            "class": "asof_0930",
            "boxes": {"join": "good", "ab": "good", "vol": "good",
                      "peer": "good", "gen": "neutral"},
            "lattice_live": False,
            "lane": None,
            "setups": [{
                "id": "pair:vol=good|ab=good", "verdict": "long",
                "short": "vol+AB", "edge_1d": 2.76, "label": "vol+AB",
            }],
            "price_changes": {"1d": 2.0, "3d": 3.0, "1w": 1.0},
        },
        {
            "class": "asof_0930",
            "boxes": {"join": "good", "ab": "good", "vol": "good",
                      "peer": "good", "gen": "neutral"},
            "lattice_live": True,
            "lane": "blocked",
            "setups": [],
            "price_changes": {"1d": 4.0, "3d": 5.0, "1w": 6.0},
        },
    ]
    scored = gla._score_rows([dict(r) for r in rows], act.preset_params("featured"))
    assert scored["n_buy"] == 1
    assert scored["n_no_buy"] == 1
    assert scored["catch"]["1d"] == 1.0
    assert scored["catch_n"]["1d"] == 1
    assert scored["buy_catch"]["1d"] == 1.0
    assert scored["mean_pnl"]["1d"] == 2.0


def test_gainer_table_stamps_clocks_prices_and_cond() -> None:
    page = gla.render_html({
        "generated_at": "t",
        "preset": "featured",
        "top_n": 25,
        "from_date": "2026-08-17",
        "to_date": "2026-08-17",
        "recall_buy_rate": 0.0,
        "sweeps": {},
        "gainer_rows": [{
            "date": "2026-08-17",
            "ticker": "CBRS",
            "gainer_rank": 5,
            "gainer_change": 14.55,
            "action_call": "SELL",
            "action_label": "SELL · 2026-08-17 09:30 ET",
            "action_reason": "fade: first crack",
            "cond_tally": "2/3/1",
            "session_bar": {
                "open": 3.72, "close": 3.82, "close_open_pct": 2.688,
            },
            "horizon_dates": {
                "1d": "2026-08-18", "3d": "2026-08-20", "1w": "2026-08-24",
            },
            "price_changes": {"1d": -12.69, "3d": -16.72, "1w": -25.70},
            "hits": {"1d": True, "3d": True, "1w": True},
            "setups": [{"id": "tag_context:first_crack", "short": "first crack",
                        "verdict": "fade"}],
        }],
    })
    assert "Date 09:30 ET" in page
    assert "Close 16:00 ET" in page
    assert "Open 09:30 ET" in page
    assert "o→c 09:30→16:00" in page
    assert "Action 09:30 ET" in page
    assert "SELL · 2026-08-17 09:30 ET" in page
    assert "$3.82 · 2026-08-17 16:00 ET" in page
    assert "$3.72 · 2026-08-17 09:30 ET" in page
    assert "+14.55% · 2026-08-17 16:00 ET" in page
    assert "+2.69% · 2026-08-17 09:30→16:00 ET" in page
    assert "2/3/1" in page
    assert "-12.69% · 2026-08-18 16:00 ET" in page
    assert "known at the open" in page
    assert "Not an end-of-day call" in page


if __name__ == "__main__":
    test_collect_gainers_is_liquid_top()
    test_score_rows_buy_catch()
    test_gainer_table_stamps_clocks_prices_and_cond()
    print("3 gainer-lookback-action tests passed")
