"""Fee-aware OOS board for the Clock-B/oppset remine — unit tests."""
from __future__ import annotations

from src import factor_mine as fm
from src import factor_mine_blind as fmbld
from src import factor_mine_blind_oos as fmoos
from src import factor_mine_blind_oppset as fmbopp
from src import factor_mine_blind_oppset_oos as fmoos2


ZERO_FEES = {
    "commission_per_share": 0.0,
    "commission_min_per_order": 0.0,
    "commission_max_pct_of_amount": 1.0,
    "platform_per_share": 0.0,
    "platform_min_per_order": 0.0,
    "platform_max_pct_of_amount": 1.0,
    "settlement_per_share": 0.0,
    "regulatory_pct_of_amount_sell_only": 0.0,
    "regulatory_min_per_order": 0.0,
    "taf_per_share_sell_only": 0.0,
    "taf_min_per_order": 0.0,
    "taf_max_per_order": 0.0,
}


def _holdout():
    return {
        "recipe_freeze": fmbopp.RECIPE_FREEZE_SHA,
        "cyrus_featured": [
            "combo_ecearnguid_5050_shared",
            "combo_sh_7030_shared",
        ],
        "formal_keep": ["combo_jse_333_shared", "combo_ecearnguid_5050_shared"],
        "rows": [
            {
                "name": "combo_ecearnguid_5050_shared",
                "side": "mix",
                "members": ["union_e_fresh_h3", "union_clk_earn_guide_react_h1"],
                "is_book_pct": 30.341,
                "is_win_rate": 0.551,
                "is_start_green": 17,
                "is_start_n": 19,
                "is_n_trades": 106,
                "oos_book_pct_continued": 0.002,
                "fresh_book_pct": 0.609,
            },
            {
                "name": "combo_sh_7030_shared",
                "side": "mix",
                "is_book_pct": 19.77,
                "is_win_rate": 0.67,
                "is_start_green": 17,
                "is_start_n": 19,
                "oos_book_pct_continued": 0.469,
                "fresh_book_pct": -0.21,
            },
            {
                "name": "combo_jse_333_shared",
                "side": "mix",
                "is_book_pct": 31.18,
                "is_win_rate": 0.59,
                "is_start_green": 16,
                "is_start_n": 19,
                "oos_book_pct_continued": -0.976,
                "fresh_book_pct": -1.19,
            },
            {
                "name": "union_hot_n4_h1",
                "side": "long",
                "is_book_pct": 19.52,
                "is_win_rate": 0.55,
                "is_start_green": 8,
                "is_start_n": 19,
                "oos_book_pct_continued": 2.221,
                "fresh_book_pct": 2.23,
            },
            {
                "name": "oppset_h1",
                "side": "long",
                "is_book_pct": -2.89,
                "is_win_rate": 0.51,
                "is_start_green": 5,
                "is_start_n": 19,
                "oos_book_pct_continued": -0.221,
                "fresh_book_pct": -0.23,
            },
        ],
    }


def test_fee_rt_is_excel_futubull() -> None:
    assert fmoos2.FEE_RT == 0.0015
    assert fmoos2.FEE_RT == fmoos.FEE_RT


def test_should_replay_all_frozen_not_live() -> None:
    frozen = {
        "combo_ecearnguid_5050_shared",
        "combo_jse_333_shared",
        "union_clk_mom_break_peer_h1",
        "oppset_h1",
        "union_hot_n4_h1",
    }
    assert fmoos2.should_replay("combo_jse_333_shared", set(), frozen=frozen) is True
    assert fmoos2.should_replay("oppset_h1", set(), frozen=frozen) is True
    assert fmoos2.should_replay("union_hot_n4_h1", set(), frozen=frozen) is True
    assert fmoos2.should_replay("combo_ecearnguid_5050_shared", set(), frozen=frozen) is True
    assert fmoos2.should_replay("flatten_h5", set(), frozen=frozen) is False
    assert fmoos2.should_replay("union_hot_n4_holdup", set(), frozen=frozen) is False
    assert fmoos2.should_replay("overnight_mega_h1", set(), frozen=frozen) is False
    assert fmoos2.should_replay("ghost_rip", set(), frozen=frozen) is False


def test_frozen_list_file_is_is_only() -> None:
    names = fmoos2.load_frozen_list()
    assert names, "FROZEN_RECIPES.txt missing"
    assert names[0] == "combo_ecearnguid_5050_shared"
    assert "combo_scextvetooh_5050_shared" in names
    assert "combo_form_jovogrh1sclexve_5050_shared" in names
    assert "oppset_h1" in names
    assert "union_hot_n4_holdup" not in names
    assert "overnight_mega_h1" not in names
    assert "flatten_h5" not in names
    holdout = _holdout()
    listed = fmoos2.frozen_names(holdout)
    assert listed == names


def test_taskforce_seven_are_frozen_cyrus_names() -> None:
    names = set(fmoos2.load_frozen_list())
    for n in fmoos2.TASKFORCE_BOOK_SURVIVORS:
        assert n in names
    assert len(fmoos2.TASKFORCE_BOOK_SURVIVORS) == 7
    assert "combo_e1s_7030_shared" in fmoos2.TASKFORCE_BOOK_SURVIVORS
    assert "combo_form_efrh1snerh3_7030_shared" in fmoos2.TASKFORCE_BOOK_SURVIVORS


def test_cyrus_oos_keep_needs_starts_and_book_not_wr() -> None:
    keep = {
        "cyrus_is": True,
        "oos_book_pct_continued": 0.45,
        "oos_start_green": 6,
        "oos_start_n": 7,
        "oos_fee_h_wr": 0.40,
        "fresh_win_rate": 0.40,
    }
    assert fmoos.is_cyrus_oos_keep(keep) is True
    assert fmoos.oos_start_ok(6, 7) is True
    assert fmoos.oos_start_ok(5, 7) is False

    wr_only = dict(keep, oos_start_green=2, oos_fee_h_wr=0.70)
    assert fmoos.is_cyrus_oos_keep(wr_only) is False
    assert fmoos.wr_only_would_pass(wr_only) is True

    faded = dict(keep, oos_book_pct_continued=0.0)
    assert fmoos.is_cyrus_oos_keep(faded) is False

    solo = dict(keep, cyrus_is=False)
    assert fmoos.is_cyrus_oos_keep(solo) is False


def test_contrast_286_and_oos_md() -> None:
    holdout = _holdout()
    no_oppset = {"cyrus_featured": ["combo_sh_7030_shared", "combo_sj_5050_shared"]}
    vs = fmoos2.contrast_286(holdout, no_oppset)
    assert "combo_ecearnguid_5050_shared" in vs["new_vs_286"]
    assert "combo_ecearnguid_5050_shared" in vs["clock_b_in_cyrus"]
    assert "combo_sj_5050_shared" in vs["dropped_vs_286"]
    assert "combo_sh_7030_shared" in vs["shared"]
    rows = [{
        "name": "combo_ecearnguid_5050_shared",
        "side": "mix",
        "cyrus_is": True,
        "formal_is": True,
        "members": ["union_e_fresh_h3", "union_clk_earn_guide_react_h1"],
        "is_start_green": 17,
        "is_start_n": 19,
        "is_book_pct": 30.34,
        "is_win_rate": 0.55,
        "oos_start_green": 2,
        "oos_start_n": 7,
        "oos_book_pct_continued": 0.002,
        "fresh_book_pct": 0.609,
        "oos_fee_h_wr": 0.625,
        "oos_fee_h_n": 8,
        "verdict": "FAIL",
        "why": "OOS starts 2/7 < Cyrus ≥85% (7 sessions → ≥6/7)",
        "book_only": True,
        "wr_only": True,
        "oos_starts": [
            {"start": "2026-09-10", "return_pct": 0.609, "made_money": True},
            {"start": "2026-09-11", "return_pct": 0.609, "made_money": True},
            {"start": "2026-09-14", "return_pct": 0.0, "made_money": False},
            {"start": "2026-09-15", "return_pct": 0.0, "made_money": False},
            {"start": "2026-09-16", "return_pct": 0.0, "made_money": False},
            {"start": "2026-09-17", "return_pct": 0.0, "made_money": False},
            {"start": "2026-09-18", "return_pct": 0.0, "made_money": False},
        ],
        "cont_match": True,
        "fresh_match": True,
    }, {
        "name": "combo_jse_333_shared",
        "side": "mix",
        "cyrus_is": False,
        "formal_is": True,
        "is_start_green": 16,
        "is_start_n": 19,
        "is_book_pct": 31.18,
        "is_win_rate": 0.59,
        "oos_start_green": 0,
        "oos_start_n": 7,
        "oos_book_pct_continued": -0.98,
        "fresh_book_pct": -1.19,
        "verdict": "FAIL",
        "why": "not Cyrus-featured on the 9/9 IS freeze (cannot KEEP from OOS)",
        "book_only": False,
        "wr_only": False,
        "oos_starts": [],
    }, {
        "name": "oppset_h1",
        "side": "long",
        "cyrus_is": False,
        "formal_is": False,
        "is_start_green": 5,
        "is_start_n": 19,
        "is_book_pct": -2.89,
        "oos_start_green": 0,
        "oos_start_n": 7,
        "oos_book_pct_continued": -0.22,
        "fresh_book_pct": -0.23,
        "oos_fee_h_wr": 0.50,
        "verdict": "FAIL",
        "book_only": False,
        "wr_only": False,
        "oos_starts": [],
    }]
    md = fmoos2.render_oos_md(
        holdout, rows, contamination={
            "hot4_live_featured": False,
            "holdup_live_featured": True,
            "post_0909_live_pins": ["union_hot_n4_holdup"],
            "clock_b_live_pins": [],
            "hot4_is_starts": "8/19",
            "hot4_is_book": 19.52,
            "taskforce_in_live_featured": [],
        }, contrast=vs)
    assert "Excel lane" in md
    assert "7 sessions → ≥6/7" in md
    assert "Win% > 55% is not enough" in md
    assert "Taskforce 7" in md
    assert "combo_ecearnguid_5050_shared" in md
    assert "combo_jse_333_shared" in md
    assert "aisle is a mixer" in md
    assert "union_hot_n4_holdup" in md
    assert "#286" in md
    assert "09-10" in md
    assert "Formal-bar" in md


def test_score_frozen_does_not_repick(tmp_path) -> None:
    holdout = _holdout()
    payload = {
        "recipes": [
            fm.make_recipe("combo_ecearnguid_5050_shared"),
            fm.make_recipe("union_hot_n4_h1", top_n=4, rank="hot_score"),
        ],
        "stats": [],
    }
    frozen = tmp_path / "FROZEN_RECIPES.txt"
    frozen.write_text(
        "\n".join(r["name"] for r in holdout["rows"]) + "\n", encoding="utf-8")
    names = fmoos2.frozen_names(holdout, frozen)
    orig_should = fmoos.should_replay
    orig_frozen = fmoos.frozen_names
    fmoos.should_replay = lambda n, c: fmoos2.should_replay(n, c, frozen=set(names))
    fmoos.frozen_names = lambda h: names
    try:
        rows = fmoos.score_frozen(
            holdout, payload, {}, replay=False, fees=ZERO_FEES, regime={})
    finally:
        fmoos.should_replay = orig_should
        fmoos.frozen_names = orig_frozen
    rows = fmoos2.mark_taskforce(rows)
    by = {r["name"]: r for r in rows}
    assert set(by) == {
        "combo_ecearnguid_5050_shared", "combo_sh_7030_shared",
        "combo_jse_333_shared", "union_hot_n4_h1", "oppset_h1",
    }
    assert by["combo_ecearnguid_5050_shared"]["cyrus_is"] is True
    assert by["combo_ecearnguid_5050_shared"]["book_only"] is True
    assert by["combo_ecearnguid_5050_shared"]["verdict"] == "FAIL"
    assert by["combo_ecearnguid_5050_shared"]["taskforce_book"] is True
    assert by["combo_jse_333_shared"]["cyrus_is"] is False
    assert by["combo_jse_333_shared"]["verdict"] == "FAIL"
    assert by["union_hot_n4_h1"]["cyrus_is"] is False
    assert by["oppset_h1"]["verdict"] == "FAIL"
    assert "union_hot_n4_holdup" not in by


def test_write_board_refuses_live_and_flags_contamination(tmp_path) -> None:
    holdout = _holdout()
    payload = {"recipes": [], "stats": []}
    rows = fmoos.score_frozen(
        holdout, payload, {}, replay=False, fees=ZERO_FEES, regime={})
    for r in rows:
        if r["name"] == "combo_ecearnguid_5050_shared":
            r["oos_start_green"] = 6
            r["oos_start_n"] = 7
            r["oos_starts"] = [
                {"start": f"2026-09-{d}", "return_pct": 0.2, "made_money": True}
                for d in (10, 11, 14, 15, 16, 17, 18)
            ]
    rows = [fmoos.attach_verdict(r) for r in rows]
    live = {
        "from_date": "2026-08-13",
        "to_date": "2026-09-18",
        "featured": [
            "union_hot_n4_holdup", "overnight_mega_h1",
            "combo_sh_macd_5050_shared",
        ],
        "stats": [{
            "name": "union_hot_n4_h1",
            "total_ret_pct": 31.97,
            "start_green": 26,
            "start_n": 26,
        }],
    }
    contam = fmoos2.live_contamination(live, holdout)
    assert contam["holdup_live_featured"] is True
    assert contam["hot4_live_featured"] is False
    assert "union_hot_n4_holdup" in contam["post_0909_live_pins"]
    assert contam["taskforce_in_live_featured"] == []
    vs = fmoos2.contrast_286(holdout, {"cyrus_featured": ["combo_sh_7030_shared"]})
    md = tmp_path / "FACTOR_MINE_BLIND_0909_OPPSET_OOS.md"
    js = tmp_path / "factor_mine_blind_0909_oppset_oos.json"
    blob = fmoos2.write_board(
        holdout, rows, contamination=contam, contrast=vs,
        dest_md=md, dest_json=js)
    text = md.read_text(encoding="utf-8")
    assert "KEEP" in text and "FAIL" in text
    assert "7 sessions → ≥6/7" in text
    assert "union_hot_n4_holdup" in text
    assert "aisle is a mixer" in text
    assert blob["n_keep"] == 1
    assert "flatten_robust" in blob["live_untouched"]
    assert blob["taskforce_book_survivors"][0] == "combo_ecearnguid_5050_shared"


def test_cli_refuses_live_board_paths() -> None:
    try:
        fmoos2.main(["--md", str(fm.OUT_MD), "--json", "/tmp/x.json",
                     "--no-replay"])
    except SystemExit as e:
        assert "live" in str(e).lower()
    else:
        raise AssertionError("expected SystemExit")


def test_cyrus_is_bar_still_ignores_win() -> None:
    yes = {
        "name": "good", "start_green": 17, "start_n": 19,
        "total_ret_pct": 4.0, "win_rate": 0.52, "book_n_trades": 40,
        "audit_ok": True,
    }
    assert fmbld.is_cyrus_featured(yes) is True
    assert fm.is_workable_stat(yes) is False


if __name__ == "__main__":
    import tempfile
    from pathlib import Path

    test_fee_rt_is_excel_futubull()
    test_should_replay_all_frozen_not_live()
    test_frozen_list_file_is_is_only()
    test_taskforce_seven_are_frozen_cyrus_names()
    test_cyrus_oos_keep_needs_starts_and_book_not_wr()
    test_contrast_286_and_oos_md()
    with tempfile.TemporaryDirectory() as td:
        test_score_frozen_does_not_repick(Path(td))
        test_write_board_refuses_live_and_flags_contamination(Path(td))
    test_cyrus_is_bar_still_ignores_win()
    test_cli_refuses_live_board_paths()
    print("blind-oppset-oos unit tests passed")
