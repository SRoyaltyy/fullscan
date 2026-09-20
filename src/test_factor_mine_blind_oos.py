"""Fee-aware OOS board — unit tests, no full panel walk."""
from __future__ import annotations

from src import factor_mine as fm
from src import factor_mine_blind as fmbld
from src import factor_mine_blind_oos as fmoos


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
        "recipe_freeze": "cb7f09ae",
        "cyrus_featured": [
            "combo_sh_5050_shared",
            "combo_sj_5050_shared",
        ],
        "formal_keep": ["combo_se_5050_skip"],
        "rows": [
            {
                "name": "combo_sh_5050_shared",
                "side": "mix",
                "is_book_pct": 33.456,
                "is_win_rate": 0.6164,
                "is_start_green": 17,
                "is_start_n": 19,
                "is_n_trades": 148,
                "oos_book_pct_continued": 1.106,
                "fresh_book_pct": 0.435,
                "fresh_n_trades": 8,
            },
            {
                "name": "combo_sj_5050_shared",
                "side": "mix",
                "is_book_pct": 21.254,
                "is_win_rate": 0.5652,
                "is_start_green": 17,
                "is_start_n": 19,
                "is_n_trades": 186,
                "oos_book_pct_continued": -0.773,
                "fresh_book_pct": -1.46,
                "fresh_n_trades": 12,
            },
            {
                "name": "combo_se_5050_skip",
                "side": "mix",
                "is_book_pct": 26.065,
                "is_win_rate": 0.6232,
                "is_start_green": 14,
                "is_start_n": 19,
                "is_n_trades": 148,
                "oos_book_pct_continued": -0.17,
                "fresh_book_pct": -0.563,
            },
            {
                "name": "union_hot_n4_h1",
                "side": "long",
                "is_book_pct": 18.839,
                "is_win_rate": 0.5263,
                "is_start_green": 11,
                "is_start_n": 19,
                "is_n_trades": 80,
                "oos_book_pct_continued": 2.219,
                "fresh_book_pct": 2.225,
            },
        ],
    }


def test_fee_rt_is_excel_futubull() -> None:
    assert fmoos.FEE_RT == 0.0015


def test_after_fee_h_long_and_short_pay_fee() -> None:
    long_net = fmoos.after_fee_h(10.0, 10.20, "long")
    assert abs(long_net - (0.02 - 0.0015)) < 1e-9
    short_net = fmoos.after_fee_h(10.0, 9.80, "short")
    assert abs(short_net - (0.02 - 0.0015)) < 1e-9
    short_pay = fmoos.after_fee_h(10.0, 10.20, "short")
    assert short_pay < 0
    assert fmoos.after_fee_h(0, 10, "long") is None


def test_oos_start_bar_is_cyrus_85pct() -> None:
    assert fmoos.oos_start_ok(6, 7) is True
    assert fmoos.oos_start_ok(5, 7) is False
    assert fmoos.oos_start_ok(17, 19) is True
    assert fmoos.oos_start_ok(16, 19) is False
    assert fmoos.oos_start_ok(0, 0) is False


def test_cyrus_oos_keep_needs_starts_and_book_not_wr() -> None:
    keep = {
        "cyrus_is": True,
        "oos_book_pct_continued": 1.1,
        "oos_start_green": 6,
        "oos_start_n": 7,
        "oos_fee_h_wr": 0.40,
        "fresh_win_rate": 0.40,
    }
    assert fmoos.is_cyrus_oos_keep(keep) is True
    assert fmoos.wr_only_would_pass(keep) is False

    wr_only = {
        "cyrus_is": True,
        "oos_book_pct_continued": 1.1,
        "oos_start_green": 2,
        "oos_start_n": 7,
        "oos_fee_h_wr": 0.70,
        "fresh_win_rate": 0.62,
    }
    assert fmoos.is_cyrus_oos_keep(wr_only) is False
    assert fmoos.wr_only_would_pass(wr_only) is True

    faded = dict(keep, oos_book_pct_continued=-0.5)
    assert fmoos.is_cyrus_oos_keep(faded) is False

    hot4 = dict(keep, cyrus_is=False, oos_book_pct_continued=2.2)
    assert fmoos.is_cyrus_oos_keep(hot4) is False


def test_should_replay_cyrus_and_hot4_only() -> None:
    cyrus = {"combo_sh_5050_shared"}
    assert fmoos.should_replay("combo_sh_5050_shared", cyrus) is True
    assert fmoos.should_replay("union_hot_n4_h1", cyrus) is True
    assert fmoos.should_replay("combo_se_5050_skip", cyrus) is False
    assert fmoos.should_replay("union_hot_n4_holdup", cyrus) is False


def test_frozen_names_never_add_oos_or_holdup() -> None:
    h = _holdout()
    names = fmoos.frozen_names(h)
    assert names[0] == "combo_sh_5050_shared"
    assert "combo_sj_5050_shared" in names
    assert "combo_se_5050_skip" in names
    assert "union_hot_n4_h1" in names
    assert "union_hot_n4_holdup" not in names
    assert "overnight_mega_h1" not in names
    # An OOS rip that was not frozen stays out.
    assert "ghost_rip" not in names


def test_attach_verdict_wr_trap() -> None:
    row = fmoos.attach_verdict({
        "name": "combo_sh_5050_shared",
        "cyrus_is": True,
        "oos_book_pct_continued": 1.106,
        "oos_start_green": 2,
        "oos_start_n": 7,
        "oos_fee_h_wr": 0.62,
        "fresh_win_rate": 0.50,
    })
    assert row["verdict"] == "FAIL"
    assert row["book_only"] is True
    assert row["wr_only"] is True
    assert "not enough" in row["why"]


def test_score_frozen_does_not_repick(tmp_path) -> None:
    holdout = _holdout()
    payload = {
        "recipes": [
            fm.make_recipe("combo_sh_5050_shared"),
            fm.make_recipe("union_hot_n4_h1", top_n=4, rank="hot_score"),
        ],
        "stats": [],
    }
    rows = fmoos.score_frozen(
        holdout, payload, {}, replay=False, fees=ZERO_FEES, regime={})
    by = {r["name"]: r for r in rows}
    assert set(by) == {
        "combo_sh_5050_shared", "combo_sj_5050_shared",
        "combo_se_5050_skip", "union_hot_n4_h1",
    }
    assert by["combo_sh_5050_shared"]["cyrus_is"] is True
    assert by["union_hot_n4_h1"]["cyrus_is"] is False
    assert by["union_hot_n4_h1"]["verdict"] == "FAIL"
    assert by["combo_sj_5050_shared"]["verdict"] == "FAIL"
    assert by["combo_se_5050_skip"]["verdict"] == "FAIL"
    # Book-only still true for the Taskforce survivor even without starts.
    assert by["combo_sh_5050_shared"]["book_only"] is True
    assert by["combo_sh_5050_shared"]["verdict"] == "FAIL"


def test_render_and_write_refuse_live_and_flag_contamination(tmp_path) -> None:
    holdout = _holdout()
    payload = {"recipes": [], "stats": []}
    rows = fmoos.score_frozen(
        holdout, payload, {}, replay=False, fees=ZERO_FEES, regime={})
    # Pretend the Taskforce name actually cleared 6/7 so KEEP appears.
    for r in rows:
        if r["name"] == "combo_sh_5050_shared":
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
    contam = fmoos.live_contamination(live, holdout)
    assert contam["holdup_live_featured"] is True
    assert contam["hot4_live_featured"] is False
    assert "union_hot_n4_holdup" in contam["post_0909_live_pins"]
    assert contam["taskforce_in_live_featured"] == []

    md = tmp_path / "FACTOR_MINE_BLIND_0909_OOS.md"
    js = tmp_path / "factor_mine_blind_0909_oos.json"
    blob = fmoos.write_board(holdout, rows, contamination=contam,
                             dest_md=md, dest_json=js)
    text = md.read_text(encoding="utf-8")
    assert "KEEP" in text and "FAIL" in text
    assert "Win% > 55% is not enough" in text or "not enough by itself" in text
    assert "union_hot_n4_holdup" in text
    assert "would not have featured" in text
    assert "combo_sh_5050_shared" in text
    assert blob["n_keep"] == 1
    assert "flatten_robust" in blob["live_untouched"]


def test_cli_refuses_live_board_paths() -> None:
    try:
        fmoos.main(["--md", str(fm.OUT_MD), "--json", "/tmp/x.json",
                    "--no-replay"])
    except SystemExit as e:
        assert "live" in str(e).lower()
    else:
        raise AssertionError("expected SystemExit")


def test_cyrus_is_bar_still_ignores_win() -> None:
    # Guardrail: IS featuring is still starts + book (existing module).
    yes = {
        "name": "good", "start_green": 17, "start_n": 19,
        "total_ret_pct": 4.0, "win_rate": 0.52, "book_n_trades": 40,
        "audit_ok": True,
    }
    assert fmbld.is_cyrus_featured(yes) is True
    assert fm.is_workable_stat(yes) is False


if __name__ == "__main__":
    test_fee_rt_is_excel_futubull()
    test_after_fee_h_long_and_short_pay_fee()
    test_oos_start_bar_is_cyrus_85pct()
    test_cyrus_oos_keep_needs_starts_and_book_not_wr()
    test_frozen_names_never_add_oos_or_holdup()
    test_attach_verdict_wr_trap()
    test_score_frozen_does_not_repick(None)
    test_cyrus_is_bar_still_ignores_win()
    test_cli_refuses_live_board_paths()
    print("blind oos unit tests passed")
