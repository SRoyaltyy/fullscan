"""BUY quality gates: event-tilt clip, hard sector-red, LAG+peer, pile sort.

Run: python -m src.test_stock_book_gates
"""
from __future__ import annotations

import pandas as pd

from src.green_pile import attach_ranks, green_mask
from src.stock_book import (
    HARD_SECTOR_RED,
    MAX_EVENT_SECTOR_TILT,
    OPP_CAP,
    _book_side,
    _buy_veto_mask,
    _clip_event_tilt,
)


def _row(ticker, join=0.9, gen=-0.07, ab=0.9, peer=0.5, sector=0.2, news=0.0,
         relvol=1.2, size="mid", mcap=2000, score=1.0, sector_name="Technology",
         context="", reasons=""):
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
        "industry": f"ind-{ticker}",
        "sector": sector_name,
        "score_1d": score,
        "core_1d": score - 0.2,
        "green_rank": (join + ab + peer) / 3.0,
        "rebound": False,
        "reasons": reasons or context,
        "context_label": context,
        "avg_vol_k": 2000,
    }


def test_event_tilt_cannot_invert_essay() -> None:
    raw = {"Energy": -0.56, "Technology": 1.04, "Healthcare": 0.0}
    clipped = _clip_event_tilt(raw)
    assert clipped["Energy"] == -MAX_EVENT_SECTOR_TILT
    assert clipped["Technology"] == MAX_EVENT_SECTOR_TILT
    assert clipped["Healthcare"] == 0.0
    # 2026-08-31 shape: Energy essay stays green, Tech essay stays not-green
    energy = 0.4675 + clipped["Energy"]
    tech = -0.275 + clipped["Technology"]
    assert energy > 0.05
    assert tech < 0.05


def test_hard_sector_red_is_buy_veto() -> None:
    df = pd.DataFrame([
        _row("WAY", sector=-0.50, sector_name="Healthcare", score=1.05,
             context="LEAD,peers↓,ind↓,sec↓"),
        _row("NCNO", sector=0.20, sector_name="Technology", score=0.80,
             context="LEAD,peers↑"),
    ])
    veto = _buy_veto_mask(df)
    assert bool(veto[df.Ticker == "WAY"].iloc[0]) is True
    assert bool(veto[df.Ticker == "NCNO"].iloc[0]) is False
    buys, _ = _book_side(df, "1d", 10)
    assert "WAY" not in set(buys["Ticker"])
    assert "NCNO" in set(buys["Ticker"])


def test_lag_and_peer_red_is_buy_veto() -> None:
    df = pd.DataFrame([
        _row("NFG", peer=-0.09, sector=-0.09, sector_name="Energy", score=1.05,
             context="LAG,peers↑,ind↓,sec↑"),
        _row("SM", peer=0.39, sector=0.27, sector_name="Energy", score=1.01,
             context="LEAD,peers↓,ind↓,sec↑"),
    ])
    veto = _buy_veto_mask(df)
    assert bool(veto[df.Ticker == "NFG"].iloc[0]) is True
    assert bool(veto[df.Ticker == "SM"].iloc[0]) is False


def test_dead_relvol_is_buy_veto() -> None:
    df = pd.DataFrame([
        _row("DEAD", relvol=0.4, score=1.2),
        _row("LIVE", relvol=1.1, score=0.4),
    ])
    veto = _buy_veto_mask(df)
    assert bool(veto.iloc[0]) is True
    assert bool(veto.iloc[1]) is False


def test_pile_sorts_by_green_rank_not_opp_score() -> None:
    rows = []
    sectors = [
        "Technology", "Healthcare", "Financial", "Energy", "Industrials",
        "Consumer Cyclical", "Utilities", "Real Estate",
    ]
    for i, sec in enumerate(sectors):
        rows.append(_row(
            f"G{i:02d}", join=0.9, ab=0.9, peer=0.8, sector=0.2,
            score=0.3, sector_name=sec,
        ))
        rows[-1]["industry"] = f"g{i}"
    # High weighted score, weak tape — must not beat the pile
    rows.append(_row(
        "OPP", join=0.2, ab=0.2, peer=0.2, sector=0.2,
        score=2.0, sector_name="Communication Services",
    ))
    rows[-1]["industry"] = "opp"
    rows.append(_row(
        "DOG", join=-0.6, ab=-0.4, peer=-0.5, sector=-0.4, gen=-0.2,
        score=-1.0, sector_name="Utilities",
    ))
    rows[-1]["industry"] = "dog"
    df = pd.DataFrame(rows)
    df = attach_ranks(df)
    df["green"] = green_mask(df)
    buys, sells = _book_side(
        df, "1d", 8, buy_mask=df["green"], buy_sort="green_rank",
    )
    assert "OPP" not in set(buys["Ticker"])
    assert list(buys["Ticker"])[0].startswith("G")
    # pile used → do not short a green name
    assert not set(sells["Ticker"]).intersection(set(df.loc[df["green"], "Ticker"]))


def test_stand_down_empties_buy() -> None:
    from src.stock_book import _book_side, _stand_down_status
    st = _stand_down_status("2026-08-31", {
        "same_day_general": True,
        "general_direction": "down",
        "weather_risk": "off",
        "general_bias": -0.47,
    })
    assert st["stand_down"] is True
    assert st["n_usable_catalysts"] == 0
    df = pd.DataFrame([
        _row("SM", sector=0.27, sector_name="Energy", score=1.0),
    ])
    buys, sells = _book_side(
        df, "1d", 10,
        buy_mask=pd.Series(False, index=df.index),
        allow_empty=True,
    )
    assert len(buys) == 0
    assert len(sells) >= 1
    # live policy has sell_excludes_addons=False — still rank SELL on score
    df2 = df.drop(columns=["core_1d"])
    buys2, sells2 = _book_side(
        df2, "1d", 10,
        buy_mask=pd.Series(False, index=df2.index),
        allow_empty=True,
        sell_core=False,
    )
    assert len(buys2) == 0
    assert len(sells2) >= 1
    assert "SM" in set(sells2["Ticker"])


def test_finviz_board_reads_theme_tape() -> None:
    import json
    from src.stock_book import ROOT, _finviz_board_md

    heat_dir = ROOT / "01_daily" / "map_heat"
    heat_dir.mkdir(parents=True, exist_ok=True)
    p = heat_dir / "2099-01-02_map_heat.json"
    p.write_text(json.dumps({
        "sectors": [{"sector": "Energy", "d1": 0.1, "w1": -2.1}],
        "hot": [], "cold": [], "overrides": [],
        "themes": [{"theme": "Energy Traditional", "subthemes": [
            {"label": "Oil Services", "w1": 1.6, "parent_w1": -2.1, "agree": False}
        ]}],
        "theme_tape": [{"theme": "Space Exploration & Technology",
                        "d1": -2.3, "w1": -5.0, "n_etfs": 13,
                        "leaders": [{"ticker": "NASA"}]}],
    }), encoding="utf-8")
    try:
        md = "\n".join(_finviz_board_md("2099-01-02", {
            "heat_source": "finviz_tape",
            "n_heat_captains": 1,
            "n_heat_industries": 1,
            "sector_bias": {"Energy": 0.47},
        }))
        assert "essay UP, tape DOWN" in md
        assert "Oil Services" in md and "DIVERGE" in md
        assert "Theme ETF tape" in md
        assert "NASA" in md
    finally:
        p.unlink(missing_ok=True)


def test_opp_cap_constant() -> None:
    assert OPP_CAP == 0.20
    assert HARD_SECTOR_RED == -0.25


def test_20260831_sleeve_drops_broken_names() -> None:
    from pathlib import Path
    import json
    p = Path("data/stock_book/2026-08-31_stock_book.json")
    if not p.exists():
        return
    book = json.loads(p.read_text(encoding="utf-8"))
    buys = [r["ticker"] for r in (book.get("books") or {}).get("1d", {}).get("buy") or []]
    if (book.get("meta") or {}).get("ranker") == "decision_lattice":
        lattice = (book.get("meta") or {}).get("decision_lattice") or {}
        assert (lattice.get("market") or {}).get("state") == "hard_red"
        assert not buys
        assert len(
            (book.get("books") or {}).get("1d", {}).get("sell") or []
        ) >= 10
        watches = [r.get("ticker") for r in lattice.get("bull_watch") or []]
        assert "AMGN" in watches[:5], watches[:5]
        assert lattice.get("n_bull_eligible") == 0
        return
    if not buys:
        return
    for bad in ("WAY", "PBH", "NFG"):
        assert bad not in buys[:10], f"{bad} still in 1d sleeve: {buys[:10]}"
    for row in (book.get("books") or {}).get("1d", {}).get("buy") or []:
        if row.get("ticker") in buys[:10]:
            assert not row.get("lb_alarm"), f"{row.get('ticker')} is 🚨 in the sleeve"
            assert not row.get("lb_fade"), f"{row.get('ticker')} is a fade setup in the sleeve"
            assert row.get("lb_cond") != "bad"
            assert row.get("lb_region") != "bad"


def main() -> None:
    tests = [
        test_event_tilt_cannot_invert_essay,
        test_hard_sector_red_is_buy_veto,
        test_lag_and_peer_red_is_buy_veto,
        test_dead_relvol_is_buy_veto,
        test_pile_sorts_by_green_rank_not_opp_score,
        test_stand_down_empties_buy,
        test_finviz_board_reads_theme_tape,
        test_opp_cap_constant,
        test_20260831_sleeve_drops_broken_names,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
