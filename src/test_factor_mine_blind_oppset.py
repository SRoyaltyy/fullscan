"""Blind ≤2026-09-09 + Clock-B/oppset remine — unit tests, no full panel walk."""
from __future__ import annotations

from src import clock_b_tells as cbt
from src import factor_mine as fm
from src import factor_mine_blind as fmbld
from src import factor_mine_blind_oppset as fmbopp
from src import oppset_clock_b as opp


def _stat(name, *, start_green=17, start_n=19, book=5.0, win=0.60,
          n=40, side="long", dollar=0.50, audit=True):
    return {
        "name": name,
        "side": side,
        "start_green": start_green,
        "start_n": start_n,
        "start_rate": start_green / start_n if start_n else 0,
        "total_ret_pct": book,
        "win_rate": win,
        "book_n_trades": n,
        "n_trades": n,
        "profitable_day_rate": dollar,
        "audit_ok": audit,
    }


def test_menu_has_clock_b_and_oppset_not_holdup() -> None:
    recs = fmbopp.build_recipes_asof_0909_oppset()
    names = {r["name"] for r in recs}
    assert "union_hot_n4_h1" in names
    assert "union_hot_n4_holdup" not in names
    assert "overnight_mega_h1" not in names
    for n in cbt.CLOCK_B_CORE + cbt.CLOCK_B_OPPSET_RECIPES:
        assert n in names, n
    assert "oppset_h1" in names
    assert "union_oppset_h1" in names
    assert not any((r.get("s_boost") or "none") == "holdup" for r in recs)


def test_blind_recipes_keep_clock_b_strip_holdup() -> None:
    recs = fmbopp.blind_recipes()
    names = {r["name"] for r in recs}
    assert "union_clk_mom_break_peer_h1" in names
    assert "oppset_h1" in names
    assert "union_hot_n4_h1" in names
    for n in fmbopp.POST_0909_CONTAM:
        assert n not in names
    assert not any((r.get("s_boost") or "none") == "holdup" for r in recs)


def test_combo_overlay_mixes_clock_b_with_9_9_primitives() -> None:
    specs = fmbopp.combo_specs_asof_0909_oppset()
    names = {s["name"] for s in specs}
    assert "combo_sh_5050_shared" in names
    assert "combo_oh_5050_shared" not in names
    members = {m for s in specs for m in s["members"]}
    assert "union_clk_mom_break_peer_h1" in members
    assert "union_oppset_h1" in members
    assert "oppset_h1" in members
    assert "union_hot_n4_holdup" not in members
    assert any(s.get("clock_b") for s in specs)
    assert len(names) == len(specs)


def test_freeze_includes_clock_b_report_not_always() -> None:
    payload = {
        "stats": [
            _stat("union_clk_fresh_cat_coil_h1", start_green=18, start_n=19,
                  book=6.0),
            _stat("union_hot_n4_holdup", start_green=12, start_n=19,
                  book=8.0, win=0.51),
            _stat("flatten_h5", start_green=8, start_n=19, book=2.0,
                  win=0.40, n=12, dollar=0.20),
            _stat("oppset_h1", start_green=10, start_n=19, book=1.0,
                  win=0.40, n=20, dollar=0.20),
        ],
        "recipes": fmbopp.build_recipes_asof_0909_oppset(),
    }
    names = fmbopp.freeze_names(payload)
    assert "union_clk_fresh_cat_coil_h1" in names  # Cyrus
    assert "oppset_h1" in names  # contamination check even if not featured
    assert "union_hot_n4_holdup" not in names
    assert "flatten_h5" not in names
    featured = fmbopp.apply_blind_featured(payload)
    assert "union_clk_fresh_cat_coil_h1" in featured["featured"]
    assert "union_hot_n4_holdup" not in featured["featured"]
    assert featured["blind"]["holdup_twins"] == []
    assert "union_clk_mom_break_peer_h1" in featured["blind"]["clock_b_twins"]["core"]


def test_filter_index_drops_oos_mornings() -> None:
    idx = {
        ("2026-09-09", "AAA"): {"join_morning": "2026-09-09",
                                "finviz_asof": "2026-09-08", "ticker": "AAA"},
        ("2026-09-10", "BBB"): {"join_morning": "2026-09-10",
                                "finviz_asof": "2026-09-09", "ticker": "BBB"},
    }
    is_only = fmbopp.filter_index(idx, join_morning_max="2026-09-09")
    assert ("2026-09-09", "AAA") in is_only
    assert ("2026-09-10", "BBB") not in is_only


def test_t1_proof_caps_is_asof() -> None:
    idx = {
        ("2026-09-09", "AAA"): {"finviz_asof": "2026-09-08"},
        ("2026-09-08", "BBB"): {"finviz_asof": "2026-09-04"},
        ("2026-09-10", "CCC"): {"finviz_asof": "2026-09-09"},
    }
    proof = fmbopp.t1_proof(idx, is_max="2026-09-09")
    assert proof["t1_clean"] is True
    assert proof["is_asof_max"] == "2026-09-08"
    assert proof["is_asof_ok"] is True
    assert proof["is_n"] == 2
    leak = {
        ("2026-09-09", "ZZZ"): {"finviz_asof": "2026-09-09"},
    }
    bad = fmbopp.t1_proof(leak, is_max="2026-09-09")
    assert bad["t1_clean"] is False


def test_union_does_not_persist_and_stamps() -> None:
    panel = {
        "from_date": "2026-09-09",
        "to_date": "2026-09-09",
        "session_dates": ["2026-09-09"],
        "rows": [{
            "date": "2026-09-09", "ticker": "ALMU",
            "sources": ["flatten"], "boxes": {},
        }],
        "by_date": None,
        "n_rows": 1,
    }
    idx = {
        ("2026-09-09", "ALMU"): {
            "join_morning": "2026-09-09", "ticker": "ALMU",
            "finviz_asof": "2026-09-08", "rvol": 4.2, "any_opp": True,
        },
        ("2026-09-09", "NEW1"): {
            "join_morning": "2026-09-09", "ticker": "NEW1",
            "finviz_asof": "2026-09-08", "rvol": 9.0, "any_opp": True,
        },
        ("2026-09-10", "OOS"): {
            "join_morning": "2026-09-10", "ticker": "OOS",
            "finviz_asof": "2026-09-09", "rvol": 99.0, "any_opp": True,
        },
    }
    out = fmbopp.union_oppset_on_panel(panel, top_n=5, index=idx, persist=False)
    assert out.get("_oppset") is True
    assert out.get("_clock_b") is True
    almu = next(r for r in out["rows"] if r["ticker"] == "ALMU")
    assert almu.get("oppset") is True
    assert almu.get("opp_finviz_asof") == "2026-09-08"
    added = [r for r in out["rows"] if r["ticker"] == "NEW1"]
    assert added
    assert "oppset" in (added[0].get("sources") or [])
    assert not any(r.get("ticker") == "OOS" for r in out["rows"])
    try:
        fmbopp.union_oppset_on_panel(panel, index=idx, persist=True)
    except RuntimeError as e:
        assert "persist" in str(e).lower()
    else:
        raise AssertionError("expected persist refuse")


def test_render_contrasts_286_and_lists_clock_b() -> None:
    payload = {
        "from_date": "2026-08-13",
        "to_date": "2026-09-09",
        "n_sessions": 19,
        "n_rows": 1800,
        "n_recipes": 4,
        "n_singles": 3,
        "n_formed_combos": 1,
        "combos": {"n": 2, "clock_b_n": 1},
        "stats": [_stat("union_clk_nr7_mom_h1")],
        "recipes": fmbopp.build_recipes_asof_0909_oppset(),
        "blind": {
            "cyrus_featured": ["union_clk_nr7_mom_h1"],
            "formal_keep": ["union_clk_nr7_mom_h1"],
            "cyrus_rule": "Starts YES + Book%",
            "hot4_twins": ["union_hot_n4_h1"],
            "holdup_twins": [],
            "clock_b_twins": {"core": ["union_clk_nr7_mom_h1"], "oppset": []},
            "aisle": {
                "t1_proof": {
                    "leaks_asof_ge_morning": 0, "t1_clean": True,
                    "is_n": 10, "is_asof_min": "2026-08-13",
                    "is_asof_max": "2026-09-08", "is_asof_cap": "2026-09-08",
                    "is_asof_ok": True,
                },
                "union": {"tagged": 20, "added": 5, "slim": 0, "top_n": 30},
                "is_n_rows": 1800,
            },
        },
    }
    rows = [{
        "name": "union_clk_nr7_mom_h1",
        "side": "long",
        "is_start_green": 17,
        "is_start_n": 19,
        "is_book_pct": 4.0,
        "is_win_rate": 0.56,
        "is_n_trades": 40,
        "oos_book_pct_continued": 1.1,
        "fresh_book_pct": 0.4,
    }]
    md = fmbopp.render_blind_md(
        payload, rows, cutoff="2026-09-09",
        oos_start="2026-09-10", oos_end="2026-09-18")
    assert "Clock-B / oppset" in md
    assert "PR #286" in md
    assert "cb7f09ae" in md
    assert "e3aabf24" in md
    assert "union_clk_nr7_mom_h1" in md
    assert "holdup" in md.lower()
    assert "Excel fee-aware" in md


def test_cli_rejects_without_out_root() -> None:
    try:
        fm.main(["--blind-0909-oppset", "--write"])
    except SystemExit as e:
        assert "out-root" in str(e) or "blind-0909-oppset" in str(e)
    else:
        raise AssertionError("expected SystemExit")


def test_cached_oppset_is_t1_clean_if_present() -> None:
    path = opp.discover_csv()
    if path is None:
        return
    idx = opp.load_index(path)
    proof = fmbopp.t1_proof(idx, is_max="2026-09-09")
    assert proof["t1_clean"] is True
    assert proof["is_asof_ok"] is True
    assert proof["is_asof_max"] <= "2026-09-08"


if __name__ == "__main__":
    test_menu_has_clock_b_and_oppset_not_holdup()
    test_blind_recipes_keep_clock_b_strip_holdup()
    test_combo_overlay_mixes_clock_b_with_9_9_primitives()
    test_freeze_includes_clock_b_report_not_always()
    test_filter_index_drops_oos_mornings()
    test_t1_proof_caps_is_asof()
    test_union_does_not_persist_and_stamps()
    test_render_contrasts_286_and_lists_clock_b()
    test_cli_rejects_without_out_root()
    test_cached_oppset_is_t1_clean_if_present()
    print("blind-oppset unit tests passed")
