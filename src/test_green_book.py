"""Live green-pile ranker: mask + _book_side fill from the pile."""
from __future__ import annotations

import pandas as pd

from src.green_pile import GREEN_MIN, attach_ranks, green_mask
from src.stock_book import _book_side, _row_dict


def _row(ticker, join, gen, ab, peer, sector=0.0, news=0.0, relvol=1.2,
         size="mid", mcap=800, score=0.4, sector_name="Technology"):
    return {
        "Ticker": ticker,
        "s_join": join,
        "s_general": gen,
        "s_ab": ab,
        "s_peer": peer,
        "s_sector": sector,
        "s_news": news,
        "relvol": relvol,
        "size": size,
        "market_cap_m": mcap,
        "industry": "Software",
        "sector": sector_name,
        "score_1d": score,
        "core_1d": score - 0.05,
        "rebound": False,
        "reasons": "",
        "avg_vol_k": 2000,
    }


def test_mask_requires_four_greens():
    df = pd.DataFrame([
        _row("OK", 0.2, 0.2, 0.2, 0.2),
        _row("NOAB", 0.2, 0.2, 0.0, 0.2),
        _row("REDNEWS", 0.2, 0.2, 0.2, 0.2, news=-0.2),
        _row("DEAD", 0.2, 0.2, 0.2, 0.2, relvol=0.4),
    ])
    m = green_mask(df)
    assert list(df.loc[m, "Ticker"]) == ["OK"]


def test_yellow_news_is_not_a_veto():
    df = pd.DataFrame([_row("FLAT", 0.2, 0.2, 0.2, 0.2, news=0.0)])
    assert bool(green_mask(df).iloc[0]) is True


def test_book_fills_from_pile_when_thick():
    rows = []
    sectors = [
        "Technology", "Healthcare", "Financial", "Energy", "Industrials",
        "Consumer Cyclical", "Utilities", "Real Estate", "Basic Materials",
        "Communication Services", "Consumer Defensive",
    ]
    for i in range(20):
        rows.append(_row(
            f"G{i:02d}", 0.2, 0.2, 0.2, 0.2,
            score=1.0 - i * 0.01,
            sector_name=sectors[i % len(sectors)],
        ))
        rows[-1]["industry"] = f"ind{i}"
    for i in range(10):
        rows.append(_row(
            f"W{i:02d}", 0.6, 0.6, 0.0, 0.0,
            score=2.0 - i * 0.01,
            sector_name="Energy",
        ))
        rows[-1]["industry"] = f"w{i}"
    df = pd.DataFrame(rows)
    df = attach_ranks(df)
    df["green"] = green_mask(df)
    assert int(df["green"].sum()) >= GREEN_MIN
    buys, sells = _book_side(df, "1d", 15, buy_mask=df["green"], buy_sort="green_rank")
    assert len(buys) == 15
    assert set(buys["Ticker"]).issubset(set(df.loc[df["green"], "Ticker"]))
    assert not any(str(t).startswith("W") for t in buys["Ticker"])
    rec = _row_dict(buys.iloc[0], "1d", "buy")
    assert rec.get("green") is True or rec.get("in_pile") is True


def test_thin_pile_caller_keeps_weights():
    df = pd.DataFrame([
        _row("G1", 0.2, 0.2, 0.2, 0.2, score=0.3),
        _row("W1", 0.5, 0.5, 0.0, 0.0, score=0.9),
    ])
    df["green"] = green_mask(df)
    assert int(df["green"].sum()) < GREEN_MIN
    buys, _ = _book_side(df, "1d", 15, buy_mask=None)
    assert list(buys["Ticker"])[0] == "W1"


def test_buy_sort_ranks_by_green_rank_not_score():
    """High score_1d / low cores must not beat high green_rank on the pile."""
    sectors = [
        "Technology", "Healthcare", "Financial", "Energy", "Industrials",
        "Consumer Cyclical", "Utilities", "Real Estate",
    ]
    rows = []
    for i, sec in enumerate(sectors):
        rows.append(_row(
            f"CORE{i}", 0.9, 0.9, 0.9, 0.9,
            score=0.10, sector_name=sec,
        ))
        rows[-1]["industry"] = f"core{i}"
    rows.append(_row(
        "SCOREY", 0.2, 0.2, 0.2, 0.2,
        score=9.0, sector_name="Basic Materials",
    ))
    rows[-1]["industry"] = "scorey"
    df = attach_ranks(pd.DataFrame(rows))
    df["green"] = green_mask(df)
    assert int(df["green"].sum()) >= GREEN_MIN
    buys, _ = _book_side(df, "1d", 8, buy_mask=df["green"], buy_sort="green_rank")
    assert list(buys["Ticker"])[0].startswith("CORE")
    assert "SCOREY" not in set(buys["Ticker"][:4])


def test_load_frame_attaches_relvol_so_vol_red_fires():
    from src.book_learn import load_frame
    df = load_frame("2026-08-20")
    assert df is not None and not df.empty
    assert "relvol" in df.columns
    assert int(pd.to_numeric(df["relvol"], errors="coerce").notna().sum()) > 0
    n_dead = int((
        pd.to_numeric(df["relvol"], errors="coerce").gt(0)
        & pd.to_numeric(df["relvol"], errors="coerce").lt(0.7)
    ).sum())
    assert n_dead > 0
    n_green = int(green_mask(df).sum())
    assert 8 <= n_green < 200  # vol-red shrinks the raw 357 pile


if __name__ == "__main__":
    test_mask_requires_four_greens()
    test_yellow_news_is_not_a_veto()
    test_book_fills_from_pile_when_thick()
    test_thin_pile_caller_keeps_weights()
    test_buy_sort_ranks_by_green_rank_not_score()
    test_load_frame_attaches_relvol_so_vol_red_fires()
    print("ok")
