"""Fee-aware OOS board for the Clock-B/oppset remine — unit tests."""
from __future__ import annotations

from src import factor_mine_blind_oos as fmoos
from src import factor_mine_blind_oppset as fmbopp
from src import factor_mine_blind_oppset_oos as fmoos2


def test_should_replay_includes_clock_b_catalogue() -> None:
    assert fmoos2.should_replay("union_clk_mom_break_peer_h1", set()) is True
    assert fmoos2.should_replay("oppset_h1", set()) is True
    assert fmoos2.should_replay("union_hot_n4_h1", set()) is True
    assert fmoos2.should_replay("combo_sh_5050_shared", {"combo_sh_5050_shared"}) is True
    assert fmoos2.should_replay("flatten_h5", set()) is False


def test_contrast_286_and_oos_md() -> None:
    holdout = {
        "recipe_freeze": fmbopp.RECIPE_FREEZE_SHA,
        "cyrus_featured": ["combo_ecearnguid_5050_shared", "combo_sh_5050_shared"],
        "formal_keep": [],
        "rows": [{
            "name": "combo_ecearnguid_5050_shared",
            "members": ["union_e_fresh_h3", "union_clk_earn_guide_react_h1"],
        }],
    }
    no_oppset = {"cyrus_featured": ["combo_sh_5050_shared", "combo_sj_5050_shared"]}
    vs = fmoos2.contrast_286(holdout, no_oppset)
    assert "combo_ecearnguid_5050_shared" in vs["new_vs_286"]
    assert "combo_ecearnguid_5050_shared" in vs["clock_b_in_cyrus"]
    assert "combo_sj_5050_shared" in vs["dropped_vs_286"]
    assert "combo_sh_5050_shared" in vs["shared"]
    rows = [{
        "name": "combo_ecearnguid_5050_shared",
        "side": "mix",
        "cyrus_is": True,
        "members": ["union_e_fresh_h3", "union_clk_earn_guide_react_h1"],
        "is_start_green": 17,
        "is_start_n": 19,
        "is_book_pct": 12.0,
        "is_win_rate": 0.55,
        "oos_start_green": 2,
        "oos_start_n": 7,
        "oos_book_pct_continued": 0.4,
        "fresh_book_pct": 0.1,
        "oos_fee_h_wr": 0.4,
        "verdict": "FAIL",
        "why": "OOS starts 2/7",
        "book_only": True,
        "wr_only": False,
    }]
    md = fmoos2.render_oos_md(
        holdout, rows, contamination={
            "hot4_live_featured": True,
            "holdup_live_featured": True,
            "post_0909_live_pins": ["union_hot_n4_holdup"],
            "clock_b_live_pins": ["union_clk_mom_break_peer_h1"],
        }, contrast=vs)
    assert "Clock-B/oppset" in md or "Clock-B / oppset" in md
    assert "#286" in md
    assert "combo_ecearnguid_5050_shared" in md
    assert "union_hot_n4_holdup" in md


if __name__ == "__main__":
    test_should_replay_includes_clock_b_catalogue()
    test_contrast_286_and_oos_md()
    print("blind-oppset-oos unit tests passed")
