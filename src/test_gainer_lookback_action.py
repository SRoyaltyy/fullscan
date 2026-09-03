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


if __name__ == "__main__":
    test_collect_gainers_is_liquid_top()
    test_score_rows_buy_catch()
    print("2 gainer-lookback-action tests passed")
