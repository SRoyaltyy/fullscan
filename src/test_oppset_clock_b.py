"""Theme Radar Clock-B oppset hook — no clone, T−1 only, research feed."""
from __future__ import annotations

from pathlib import Path

from src import clock_b_tells as cbt
from src import factor_mine as fm
from src import factor_mine_book as fmb
from src import oppset_clock_b as opp


FIXTURE = """join_morning,finviz_asof,ticker,sector,price,avg_vol,mcap,rvol,change_pct,chg_open_pct,gap_pct,ah_change_pct,pweek,any_opp
2026-09-16,2026-09-15,TRMD,Energy,20,800,400,10.68,5.0,1.0,0.2,,8.0,1
2026-09-17,2026-09-16,BWIN,Financial,30,900,500,10.04,-2.0,0.0,1.1,,4.0,1
2026-09-18,2026-09-17,SDGR,Healthcare,25,700,800,7.30,26.37,2.0,0.67,,12.0,1
2026-09-18,2026-09-17,GNRC,Industrials,140,600,9000,7.64,18.34,4.0,31.06,,9.0,1
2026-09-18,2026-09-18,LEAK,Tech,10,600,300,9.0,20.0,1.0,5.0,,1.0,1
"""


def test_parse_drops_same_day_asof() -> None:
    rows = opp.parse_rows(FIXTURE)
    tickers = {(r["join_morning"], r["ticker"]) for r in rows}
    assert ("2026-09-18", "SDGR") in tickers
    assert ("2026-09-18", "GNRC") in tickers
    assert ("2026-09-18", "LEAK") not in tickers
    sdgr = next(r for r in rows if r["ticker"] == "SDGR")
    assert sdgr["finviz_asof"] == "2026-09-17"
    assert sdgr["finviz_asof"] < sdgr["join_morning"]
    assert sdgr["rvol"] == 7.3


def test_stamp_and_filter_recipes() -> None:
    idx = {(r["join_morning"], r["ticker"]): r for r in opp.parse_rows(FIXTURE)}
    row = {
        "date": "2026-09-18", "ticker": "SDGR", "sources": ["yday_gainer"],
        "boxes": {}, "alarm": False, "last_green": True,
        "ohlc_ret_5": 4.0, "ohlc_break_10": True, "macd_up": True,
        "ohlc_rvol": 1.1,
    }
    opp.stamp_row(row, idx)
    assert row["oppset"] is True
    assert row["opp_rvol"] == 7.3
    assert row["opp_finviz_asof"] == "2026-09-17"
    recs = {r["name"]: r for r in fm.build_recipes()}
    assert fm.matches(row, recs["union_oppset_h1"]) is True
    assert fm.matches(row, recs["oppset_h1"]) is True
    miss = dict(row, ticker="ZZZZ", oppset=False, opp_rvol=None)
    opp.stamp_row(miss, idx)
    assert miss["oppset"] is False
    assert fm.matches(miss, recs["union_oppset_h1"]) is False
    assert fm.matches(miss, recs["oppset_h1"]) is False
    # Same-day RelVol must not be the gate.
    assert "RelVol" not in fm.INPUT_FIELDS
    assert "opp_rvol" in fm.INPUT_FIELDS
    assert "oppset" in fmb.UNIVERSES
    assert "opp_rvol" in fmb.RANKS
    assert fm.rank_key(row, recs["union_oppset_h1"])[0] == -7.3


def test_core_oppset_recipes_and_pull_url() -> None:
    names = {r["name"] for r in fm.build_recipes()}
    for n in fm.CLOCK_B_SPLICE:
        assert n in names, n
    assert set(fm.CLOCK_B_SPLICE) == set(cbt.CLOCK_B_CORE + cbt.CLOCK_B_OPPSET_RECIPES)
    for skip in ("union_clk_earn_guide_react_h1", "union_clk_insider_cash_stab_h3",
                 "union_clk_flow_coil_h1", "union_clk_r_up_coil_h1"):
        assert skip not in fm.CLOCK_B_SPLICE
    url = opp.raw_url()
    assert "raw.githubusercontent.com/SRoyaltyy/theme-radar/a782cc2b/" in url
    assert url.endswith("research/oppset_clock_b/oppset_flagged.csv")
    assert "git clone" not in (opp.pull.__doc__ or "")
    assert "curl -fsSL" in (opp.pull.__doc__ or "")
    js = fm.SIM_JS.read_text(encoding="utf-8")
    assert "oppset" in js
    assert "opp_rvol" in js
    why = fm.match_why(
        {"date": "2026-09-18", "ticker": "SDGR", "sources": ["union"],
         "oppset": True, "boxes": {}},
        fm.make_recipe("t", require={"oppset": True}),
    )
    assert why["ok"] is True
    gated = fmb.recipes_from_action(gate="oppset", auto_tweak=False)
    gated_names = {r["name"] for r in gated}
    assert "union_oppset_h1" in gated_names
    assert "oppset_h1" in gated_names


def test_union_opt_in_default_off() -> None:
    import inspect
    import os
    old = os.environ.pop("FULLSCAN_OPPSET_UNION", None)
    try:
        assert opp.union_enabled() is False
        os.environ["FULLSCAN_OPPSET_UNION"] = "1"
        assert opp.union_enabled() is True
    finally:
        if old is None:
            os.environ.pop("FULLSCAN_OPPSET_UNION", None)
        else:
            os.environ["FULLSCAN_OPPSET_UNION"] = old
    src = inspect.getsource(fm._candidates)
    assert "buckets = {" in src
    assert 'buckets["oppset"]' in src
    assert src.index("buckets = {") < src.index("return buckets")


def test_live_flagged_proof_if_csv_present() -> None:
    """theme-radar a782cc2b: 09-16=242, 09-17=261, 09-18=451; SDGR/GNRC asof 09-17."""
    path = opp.discover_csv()
    if path is None:
        return
    idx = opp.load_index(path)
    assert opp.flagged_n("2026-09-16", idx) == 242
    assert opp.flagged_n("2026-09-17", idx) == 261
    assert opp.flagged_n("2026-09-18", idx) == 451
    sdgr = opp.lookup("2026-09-18", "SDGR", idx)
    gnrc = opp.lookup("2026-09-18", "GNRC", idx)
    assert sdgr and sdgr["finviz_asof"] == "2026-09-17"
    assert gnrc and gnrc["finviz_asof"] == "2026-09-17"
    assert sdgr["finviz_asof"] < "2026-09-18"
    panel_path = Path("data/factor_mine/panel.json")
    if not panel_path.is_file():
        return
    import json
    panel = json.loads(panel_path.read_text(encoding="utf-8"))
    opp.attach_panel(panel, idx)
    by = {}
    for r in panel["rows"]:
        if r.get("date") in ("2026-09-16", "2026-09-17", "2026-09-18") and r.get("oppset"):
            by.setdefault(r["date"], []).append(r["ticker"])
    assert len(by.get("2026-09-16") or []) >= 20
    assert "SDGR" in (by.get("2026-09-18") or [])
    assert "GNRC" in (by.get("2026-09-18") or [])


def main() -> None:
    test_parse_drops_same_day_asof()
    test_stamp_and_filter_recipes()
    test_core_oppset_recipes_and_pull_url()
    test_union_opt_in_default_off()
    test_live_flagged_proof_if_csv_present()
    print("5 oppset clock-b tests passed")


if __name__ == "__main__":
    main()
