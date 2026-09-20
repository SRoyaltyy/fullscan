"""Blind ≤2026-09-09 formation remine — unit tests, no full panel walk."""
from __future__ import annotations

from src import factor_mine as fm
from src import factor_mine_blind as fmbld
from src import factor_mine_combo as fmc


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


def test_frozen_menu_has_hot4_not_holdup_or_clockb() -> None:
    recs = fmbld.build_recipes_asof_0909()
    names = {r["name"] for r in recs}
    assert "union_hot_n4_h1" in names
    assert "union_hot_score_h1" in names
    assert "union_hot_n4_holdup" not in names
    assert "overnight_mega_h1" not in names
    assert not any(n.startswith("union_clk_") for n in names)
    assert not any(n.startswith("short_clk_") for n in names)
    assert "oppset_h1" not in names
    assert not any((r.get("s_boost") or "none") == "holdup" for r in recs)
    assert len(recs) >= 100
    hot = next(r for r in recs if r["name"] == "union_hot_n4_h1")
    assert fmbld.recipe_signature(hot) == fmbld.HOT4_SIG


def test_blind_recipes_strips_post_0909_even_if_smuggled() -> None:
    recs = fmbld.blind_recipes()
    names = {r["name"] for r in recs}
    for n in fmbld.POST_0909_NAMES:
        assert n not in names
    assert "union_hot_n4_h1" in names
    assert "flatten_h5" in names


def test_combo_specs_asof_0909_has_sh_not_oh_or_macd() -> None:
    specs = fmbld.combo_specs_asof_0909()
    names = {s["name"] for s in specs}
    assert "combo_sh_5050_shared" in names
    assert "combo_sh_3070_shared" in names
    assert "combo_oh_5050_shared" not in names
    assert "combo_sh_macd_5050_shared" not in names
    members = {m for s in specs for m in s["members"]}
    assert "union_hot_n4_h1" in members
    assert "union_hot_n4_holdup" not in members
    assert "overnight_mega_h1" not in members


def test_cyrus_featured_prioritizes_starts_and_book() -> None:
    yes = _stat("good", start_green=17, start_n=19, book=4.0, win=0.52)
    assert fmbld.is_cyrus_featured(yes) is True
    # Formal bar needs win 0.55 — Cyrus still keeps the start-day winner.
    assert fm.is_workable_stat(yes) is False
    no_starts = _stat("flat", start_green=10, start_n=19, book=20.0, win=0.70)
    assert fmbld.is_cyrus_featured(no_starts) is False
    no_book = _stat("red", start_green=18, start_n=19, book=-1.0)
    assert fmbld.is_cyrus_featured(no_book) is False
    thin = _stat("thin", start_green=3, start_n=3, book=8.0, n=10)
    assert fmbld.is_cyrus_featured(thin) is False


def test_freeze_names_never_injects_always_or_holdup() -> None:
    payload = {
        "stats": [
            _stat("union_news_g_h1", start_green=18, start_n=19, book=6.0),
            _stat("flatten_h5", start_green=8, start_n=19, book=2.0,
                  win=0.40, n=12, dollar=0.20),
            _stat("union_hot_n4_holdup", start_green=12, start_n=19,
                  book=8.0, win=0.51),
        ],
        "recipes": [],
    }
    names = fmbld.freeze_names(payload)
    assert "union_news_g_h1" in names
    assert "flatten_h5" not in names
    assert "union_hot_n4_holdup" not in names
    assert fm.WORKABLE_ALWAYS
    for n in fm.WORKABLE_ALWAYS:
        if n not in {s["name"] for s in payload["stats"]
                     if fmbld.is_cyrus_featured(s) or fm.is_workable_stat(s)}:
            assert n not in names


def test_form_combos_from_is_uses_engine_primitives() -> None:
    stats = [
        _stat("union_e_fresh_h3", start_green=18, start_n=19, book=12.0),
        _stat("union_hot_n4_h1", start_green=16, start_n=19, book=8.0),
        _stat("short_news_r_h3", start_green=17, start_n=19, book=10.0,
              side="short"),
        _stat("union_coil_off_h1", start_green=15, start_n=19, book=3.0),
    ]
    existing = fmbld.combo_specs_asof_0909()
    extras = fmbld.form_combos_from_is(stats, existing=existing)
    assert extras
    assert all(s.get("formed") for s in extras)
    assert all(s["pool"] == "shared" for s in extras)
    assert any(s["weights"] == [1, 1] for s in extras)
    assert any(s["weights"] == [70, 30] for s in extras)
    names = {s["name"] for s in extras}
    assert "combo_sh_5050_shared" not in names
    members = [tuple(s["members"]) for s in extras]
    assert any("union_coil_off_h1" in m for m in members) or extras


def test_apply_blind_featured_ignores_live_pins() -> None:
    payload = {
        "stats": [
            _stat("union_e_fresh_h3", start_green=18, start_n=19, book=9.0),
            _stat("union_hot_n4_h1", start_green=12, start_n=19, book=4.0,
                  win=0.51),
        ],
        "recipes": fmbld.build_recipes_asof_0909(),
        "featured": list(fmc.LONG_LED_PIN),
    }
    out = fmbld.apply_blind_featured(payload)
    assert out["featured"] == ["union_e_fresh_h3"]
    assert "union_hot_n4_holdup" not in out["featured"]
    assert "overnight_mega_h1" not in out["featured"]
    assert "union_hot_n4_h1" in out["blind"]["hot4_twins"]
    assert out["blind"]["holdup_twins"] == []


def test_render_distinguishes_formation_from_selection() -> None:
    payload = {
        "from_date": "2026-08-13",
        "to_date": "2026-09-09",
        "n_sessions": 19,
        "n_rows": 1668,
        "n_recipes": 3,
        "n_singles": 2,
        "n_formed_combos": 1,
        "combos": {"n": 2},
        "stats": [_stat("union_e_fresh_h3")],
        "recipes": [],
        "blind": {
            "cyrus_featured": ["union_e_fresh_h3"],
            "formal_keep": ["union_e_fresh_h3"],
            "cyrus_rule": "Starts YES + Book%",
            "hot4_twins": ["union_hot_n4_h1"],
            "holdup_twins": [],
        },
    }
    rows = [{
        "name": "union_e_fresh_h3",
        "side": "long",
        "is_start_green": 18,
        "is_start_n": 19,
        "is_book_pct": 9.0,
        "is_win_rate": 0.6,
        "is_n_trades": 40,
        "oos_book_pct_continued": 2.2,
        "fresh_book_pct": 1.8,
    }]
    md = fmbld.render_blind_md(
        payload, rows, cutoff="2026-09-09",
        oos_start="2026-09-10", oos_end="2026-09-18")
    assert "blind formation" in md.lower()
    assert "PR #285" in md
    assert "KEEP-selection" in md or "selection-cut" in md
    assert "would not have found holdup" in md
    assert "union_e_fresh_h3" in md


def test_cli_rejects_blind_without_out_root() -> None:
    try:
        fm.main(["--blind-0909", "--write"])
    except SystemExit as e:
        assert "--blind-0909" in str(e)
    else:
        raise AssertionError("expected SystemExit")


if __name__ == "__main__":
    test_frozen_menu_has_hot4_not_holdup_or_clockb()
    test_blind_recipes_strips_post_0909_even_if_smuggled()
    test_combo_specs_asof_0909_has_sh_not_oh_or_macd()
    test_cyrus_featured_prioritizes_starts_and_book()
    test_freeze_names_never_injects_always_or_holdup()
    test_form_combos_from_is_uses_engine_primitives()
    test_apply_blind_featured_ignores_live_pins()
    test_render_distinguishes_formation_from_selection()
    test_cli_rejects_blind_without_out_root()
    print("blind unit tests passed")
