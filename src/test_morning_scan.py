"""Morning ticket scan: oppset_union aisle + Clock-B gates.

Run: PYTHONPATH=. python3 -m src.test_morning_scan
"""
from __future__ import annotations

from src import clock_b_tells as cbt
from src import factor_mine as fm
from src import morning_scan as ms
from src import strategy_tickets as st


def _idx(*rows):
    return {(r["join_morning"], r["ticker"]): r for r in rows}


def test_discover_finds_vendored_keep_aisle() -> None:
    from src import oppset_clock_b as opp
    path = opp.discover_csv()
    assert path is not None
    assert path.name == "oppset_flagged.csv"
    # Vendored Theme Radar KEEP aisle is in-repo; tickets must see it
    # without FULLSCAN_OPPSET_UNION remine.
    if "theme_radar" in str(path) or path.is_file():
        n = opp.flagged_n("2026-09-18")
        assert n >= 200, n


def test_aisle_unions_oppset_and_stamps_clock_b() -> None:
    idx = _idx({
        "join_morning": "2026-09-18",
        "ticker": "SDGR",
        "rvol": 7.3,
        "gap_pct": 0.67,
        "change_pct": 26.37,
        "any_opp": True,
        "finviz_asof": "2026-09-17",
    })
    panel = [{
        "date": "2026-09-18", "ticker": "ALMU", "sources": ["union"],
        "boxes": {"peer": "good"}, "alarm": False, "last_green": True,
        "ohlc_ret_5": 4.0, "ohlc_break_10": True, "macd_up": True,
        "rs_week": 1.2, "erd_days_since_E": 1, "erd_flag_E": 1,
    }]
    aisle = ms.aisle_rows("2026-09-18", panel, index=idx)
    names = {r["ticker"] for r in aisle}
    assert names == {"ALMU", "SDGR"}
    almu = next(r for r in aisle if r["ticker"] == "ALMU")
    sdgr = next(r for r in aisle if r["ticker"] == "SDGR")
    assert almu["oppset"] is False
    assert almu.get("_clock_b") is True
    assert almu.get("clk_mom_break_peer") is True
    assert sdgr["oppset"] is True
    assert sdgr["sources"] == ["oppset"]
    assert sdgr["opp_rvol"] == 7.3
    assert sdgr["opp_finviz_asof"] == "2026-09-17"
    assert "Gap" not in sdgr and "RelVol" not in sdgr


def test_clock_b_veto_and_keep_rank_change_picks() -> None:
    """Scanner change: same recipe, different names vs raw pick_day."""
    rec = fm.make_recipe(
        "union_h1", universe="union", hold=1, forbid={"alarm": True},
        rank="list",
    )
    veto = {
        "date": "2026-09-18", "ticker": "BURST", "sources": ["union"],
        "src_rank": 0, "boxes": {}, "alarm": False,
        "ohlc_ret_5": 20.0, "ohlc_ret_10": 22.0, "last_red": True,
        "ohlc_break_10": True, "rsi_ob": True,
    }
    keep = {
        "date": "2026-09-18", "ticker": "FRESH", "sources": ["union"],
        "src_rank": 9, "boxes": {}, "alarm": False,
        "erd_days_since_E": 1, "erd_flag_E": 1, "last_green": True,
        "ohlc_ret_5": 3.0,
    }
    plain = {
        "date": "2026-09-18", "ticker": "PLAIN", "sources": ["union"],
        "src_rank": 1, "boxes": {}, "alarm": False, "ohlc_ret_5": 1.0,
    }
    cbt.stamp_row(veto)
    cbt.stamp_row(keep)
    cbt.stamp_row(plain)
    assert cbt.clk_ext_veto(veto) is True
    assert ms.e_fresh_atom(keep) is True
    rows = [veto, keep, plain]
    baseline = [r["ticker"] for r in fm.pick_day(rows, rec)]
    scanned = [r["ticker"] for r in ms.pick_morning(rows, rec)]
    assert "BURST" in baseline
    assert "BURST" not in scanned
    assert scanned[0] == "FRESH"
    assert scanned != baseline


def test_short_keep_clock_b4_ranks_first() -> None:
    rec = fm.make_recipe(
        "short_news_r_h3", universe="union", hold=3, side="short",
        require={"news_or_red": True},
    )
    c4 = {
        "date": "2026-09-18", "ticker": "DOWN", "sources": ["union"],
        "src_rank": 8, "boxes": {"peer": "bad"}, "alarm": False,
        "news_box": "bad", "news_prior": "bad", "last_red": True,
        "ohlc_ret_5": -3.0, "macd_down": True,
    }
    other = {
        "date": "2026-09-18", "ticker": "MEH", "sources": ["union"],
        "src_rank": 0, "boxes": {}, "alarm": False,
        "news_box": "bad", "last_green": True, "ohlc_ret_5": 2.0,
    }
    cbt.stamp_row(c4)
    cbt.stamp_row(other)
    assert ms.keep_short_hit(c4) is True
    assert ms.keep_short_hit(other) is False
    scanned = [r["ticker"] for r in ms.pick_morning([other, c4], rec)]
    assert scanned[0] == "DOWN"


def test_short_ext_veto_recipe_not_dropped() -> None:
    rec = fm.make_recipe(
        "short_clk_ext_veto_h3", universe="union", hold=3, side="short",
        require={"clk_ext_veto": True},
    )
    row = {
        "date": "2026-09-18", "ticker": "EXT", "sources": ["union"],
        "boxes": {}, "ohlc_ret_5": 20.0, "ohlc_ret_10": 22.0,
        "last_red": True, "ohlc_break_10": True, "rsi_ob": True,
    }
    cbt.stamp_row(row)
    assert ms.long_veto(row, rec) is False
    assert [r["ticker"] for r in ms.pick_morning([row], rec)] == ["EXT"]


def test_tickets_use_morning_scan_not_pin() -> None:
    src = open(st.__file__, encoding="utf-8").read()
    assert "morning_scan" in src
    assert "pick_morning" in src
    assert "aisle_rows" in src
    assert "PAPER_FEE_KEEP" not in src
    assert "attach_paper_dual_run" not in src
    flat = open(st.__file__, encoding="utf-8").read()
    # flatten_robust card stays the sleeve-merge path.
    assert "def flatten_strat" in flat
    assert "live flatten_robust card" in flat
    from src import webull_exec
    wsrc = open(webull_exec.__file__, encoding="utf-8").read()
    assert "morning_scan" not in wsrc
    assert "pick_day" in wsrc


def test_scan_meta_does_not_claim_live() -> None:
    meta = ms.scan_meta(look={"source": "panel"}, rows=[
        {"ticker": "A", "oppset": True, "_clock_b": True},
        {"ticker": "B", "oppset": False, "_clock_b": True},
    ])
    assert meta["methodology"] == "oppset_union+clock_b"
    assert meta["live_untouched"] == "flatten_robust"
    assert "not a pinned" in meta["note"].lower()
    assert "LIVE money" in meta["note"]
    assert meta["n_oppset"] == 1


def test_recipe_strats_look_reports_methodology() -> None:
    look: dict = {}
    rows = [{
        "date": "2026-09-18", "ticker": "ALMU", "sources": ["union"],
        "boxes": {}, "alarm": False, "last_green": True,
        "erd_days_since_E": 1, "erd_flag_E": 1,
        "ohlc_ret_5": 3.0, "ohlc_break_10": False,
    }]
    panel = {"by_date": {"2026-09-18": rows}, "to_date": "2026-09-18"}
    from unittest import mock
    with mock.patch.object(st, "_load_json", return_value=panel):
        with mock.patch("src.factor_mine.rehydrate_panel", return_value=panel):
            out = st.recipe_strats("2026-09-18", look_out=look)
    assert look.get("methodology") == "oppset_union+clock_b"
    assert look.get("aisle")
    names = {r["name"] for r in out}
    assert "union_e_fresh_h3" in names
    assert "short_clk_neg_weak_fail_h3" in names
    e = next(r for r in out if r["name"] == "union_e_fresh_h3")
    assert e["family"] == "factor_mine"
    assert e.get("live") is None or e.get("live") is False


def main() -> None:
    test_discover_finds_vendored_keep_aisle()
    test_aisle_unions_oppset_and_stamps_clock_b()
    test_clock_b_veto_and_keep_rank_change_picks()
    test_short_keep_clock_b4_ranks_first()
    test_short_ext_veto_recipe_not_dropped()
    test_tickets_use_morning_scan_not_pin()
    test_scan_meta_does_not_claim_live()
    test_recipe_strats_look_reports_methodology()
    print("ok")


if __name__ == "__main__":
    main()
