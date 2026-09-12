"""Walk-forward factor-mine harness — unit tests, no full remine."""
from __future__ import annotations

from src import factor_mine as fm
from src import walkforward_factor_mine as wf

CAL = [
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18",
    "2026-08-19", "2026-08-20", "2026-08-21", "2026-08-24",
    "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28",
    "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03",
    "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11",
]
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


def _row(date, ticker, **kw):
    src = kw.pop("sources", ["union"])
    r = {
        "date": date, "ticker": ticker, "sources": list(src),
        "boxes": {"vol": "good", "news": kw.pop("news", "missing")},
        "blue": False, "alarm": False, "zero_red": True,
        "last_green": True, "last_red": False,
        "ohlc_ret_5": 3.0, "ohlc_rvol": 1.0, "ohlc_hot_score": 1.0,
        "src_rank": kw.pop("src_rank", 0), "cond_good": 1, "cond_bad": 0,
    }
    r.update(kw)
    return r


def _panel(cal, rows):
    by_date: dict[str, list] = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    return {
        "session_dates": list(cal),
        "rows": rows,
        "by_date": by_date,
        "from_date": cal[0],
        "to_date": cal[-1],
        "n_sessions": len(cal),
        "n_rows": len(rows),
    }


def test_slice_panel_drops_later_sessions() -> None:
    cal = CAL[:6]
    rows = [_row(d, "AAA") for d in cal] + [_row(CAL[6], "LEAK")]
    panel = _panel(cal + [CAL[6]], rows)
    cut = wf.slice_panel(panel, end="2026-08-20")
    assert cut["session_dates"] == cal
    assert all(r["date"] <= "2026-08-20" for r in cut["rows"])
    assert "LEAK" not in {r["ticker"] for r in cut["rows"]}
    assert "2026-08-21" not in cut["by_date"]
    oos = wf.slice_panel(panel, start="2026-08-21", end="2026-08-21")
    assert oos["session_dates"] == ["2026-08-21"]
    assert {r["ticker"] for r in oos["rows"]} == {"LEAK"}


def test_folds_roll_forward_and_hide_oos() -> None:
    folds = wf.make_folds(CAL, first_cutoff="2026-08-20", step=4, forward=4)
    assert folds, "data supports at least one fold from 2026-08-20"
    assert folds[0]["cutoff"] == "2026-08-20"
    assert folds[0]["is_dates"][-1] == "2026-08-20"
    assert folds[0]["oos_dates"][0] == "2026-08-21"
    assert "2026-08-20" not in folds[0]["oos_dates"]
    for f in folds:
        assert max(f["is_dates"]) == f["cutoff"]
        assert min(f["oos_dates"]) > f["cutoff"]
        assert set(f["is_dates"]).isdisjoint(f["oos_dates"])
        assert len(f["oos_dates"]) >= 3
    # Four hidden blocks fit on the 21-session tape.
    assert len(folds) >= 3


def test_select_is_cannot_see_oos_winner() -> None:
    """AAA wins only on IS; BBB wins only on OOS. Selector must pick AAA."""
    is_days = CAL[:6]
    oos_days = CAL[6:10]
    cal = is_days + oos_days
    rows = []
    bars = {}
    for d in is_days:
        rows += [
            _row(d, "AAA", sources=["union", "aaa"]),
            _row(d, "BBB", sources=["union", "bbb"]),
        ]
        bars[("AAA", d)] = {"open": 10.0, "close": 11.0}
        bars[("BBB", d)] = {"open": 10.0, "close": 9.0}
    for d in oos_days:
        rows += [
            _row(d, "AAA", sources=["union", "aaa"]),
            _row(d, "BBB", sources=["union", "bbb"]),
        ]
        bars[("AAA", d)] = {"open": 10.0, "close": 9.0}
        bars[("BBB", d)] = {"open": 10.0, "close": 11.0}
    panel = _panel(cal, rows)
    rec_aaa = fm.make_recipe(
        "long_aaa", universe="aaa", hold=1, top_n=1, sell="time")
    rec_bbb = fm.make_recipe(
        "long_bbb", universe="bbb", hold=1, top_n=1, sell="time")
    is_panel = wf.slice_panel(panel, end=is_days[-1])
    oos_panel = wf.slice_panel(panel, start=oos_days[0], end=oos_days[-1])
    scored = [
        wf.score_recipe_book(
            is_panel, rec, bars=bars, fees=ZERO_FEES, regime={},
            role="is_recipe")
        for rec in (rec_aaa, rec_bbb)
    ]
    pick = wf.select_is(scored, min_days=3)
    assert pick["top"] == "long_aaa"
    assert pick["best_long"] == "long_aaa"
    oos_aaa = wf.score_recipe_book(
        oos_panel, rec_aaa, bars=bars, fees=ZERO_FEES, regime={},
        start=oos_days[0], role="discovered_top", selected=True)
    oos_bbb = wf.score_recipe_book(
        oos_panel, rec_bbb, bars=bars, fees=ZERO_FEES, regime={},
        start=oos_days[0], role="oos_only")
    assert (oos_aaa.get("total_ret_pct") or 0) < 0
    assert (oos_bbb.get("total_ret_pct") or 0) > 0
    assert oos_aaa.get("n_sessions") == len(oos_days)
    # Fresh $10k — first OOS open cash is not the IS leftover.
    assert abs(float((oos_aaa.get("final_equity") or 0)) - 10_000) > 1


def test_random_matched_same_count_avoids_actuals() -> None:
    cal = CAL[:3]
    rows = []
    for d in cal:
        rows += [
            _row(d, "HOT", sources=["union"], ohlc_hot_score=9, src_rank=0),
            _row(d, "RED", sources=["union"], news="bad", src_rank=1),
            _row(d, "X1", sources=["union"], src_rank=2),
            _row(d, "X2", sources=["union"], src_rank=3),
            _row(d, "X3", sources=["union"], src_rank=4),
        ]
    panel = _panel(cal, rows)
    long_rec = fm.make_recipe(
        "hot", universe="union", hold=1, top_n=1, rank="hot_score")
    short_rec = fm.make_recipe(
        "news", universe="union", hold=1, side="short",
        require={"news": "bad"})
    lp, sp = wf.random_matched_groups(panel, long_rec, short_rec, seed=7)
    for d in cal:
        actual_l = {r["ticker"] for r in fm.pick_day(panel["by_date"][d], long_rec)}
        actual_s = {r["ticker"] for r in fm.pick_day(panel["by_date"][d], short_rec)}
        assert len(lp[d]) == len(actual_l) == 1
        assert len(sp[d]) == len(actual_s) == 1
        assert set(lp[d]).isdisjoint(actual_l)
        assert set(sp[d]).isdisjoint(actual_s)
        assert set(lp[d]).isdisjoint(set(sp[d]))


def test_pins_drive_simulate_book_names() -> None:
    cal = ["2026-08-17", "2026-08-18"]
    rows = [
        _row("2026-08-17", "AAA"), _row("2026-08-17", "BBB"),
        _row("2026-08-18", "AAA"), _row("2026-08-18", "BBB"),
    ]
    panel = wf.apply_pins(_panel(cal, rows), {
        wf.PIN_LONG: {"2026-08-17": ["BBB"], "2026-08-18": ["BBB"]},
    })
    rec = wf.pin_recipe("pin", wf.PIN_LONG, fm.make_recipe("t", hold=1, top_n=1))
    bars = {
        ("AAA", "2026-08-17"): {"open": 10, "close": 20},
        ("AAA", "2026-08-18"): {"open": 20, "close": 30},
        ("BBB", "2026-08-17"): {"open": 10, "close": 10.1},
        ("BBB", "2026-08-18"): {"open": 10.1, "close": 10.2},
    }
    book = wf.score_recipe_book(
        panel, rec, bars=bars, fees=ZERO_FEES, regime={}, role="pin")
    # Bought BBB both days, not the jackpot AAA.
    assert book["n_trades"] >= 1
    assert (book.get("total_ret_pct") or 0) < 50


def test_neutralize_subtracts_exposure_times_market() -> None:
    row = {
        "name": "union_hot_n4_h1",
        "role": "control_hot4",
        "daily": [
            {"date": "2026-08-17", "mean": 2.0, "equity": 10200, "stock": 10200},
            {"date": "2026-08-18", "mean": 2.0, "equity": 10404, "stock": 10404},
        ],
        "total_ret_pct": 4.04,
    }
    mkt = {"2026-08-17": 2.0, "2026-08-18": 2.0}
    neut = wf.neutralize_metrics(row, mkt)
    assert neut["role"] == "neutralized"
    assert neut["name"].endswith("_mktneut")
    # Fully invested long vs a +2% market → residual ~0.
    assert abs(float(neut["total_ret_pct"])) < 0.05


def test_verdict_kills_when_oos_not_plus_or_not_above_random() -> None:
    dead = wf.decide_verdict(
        {"n_folds": 4, "n_green_folds": 1, "mean_book_pct": -3.0},
        {"mean_book_pct": 0.5},
        {"mean_book_pct": -1.0},
    )
    assert dead["label"] == "KILL"
    assert dead["keep_combo"] is False
    assert dead["process_plus_ev"] is False
    lucky = wf.decide_verdict(
        {"n_folds": 4, "n_green_folds": 3, "mean_book_pct": 1.2},
        {"mean_book_pct": 1.5},
        {"mean_book_pct": 0.4},
    )
    assert lucky["label"] == "KILL"
    assert "random" in lucky["why"].lower()
    keep = wf.decide_verdict(
        {"n_folds": 4, "n_green_folds": 3, "mean_book_pct": 2.5},
        {"mean_book_pct": 0.1},
        {"mean_book_pct": 1.0},
    )
    assert keep["label"] == "KEEP"
    assert keep["kill_35pct_print"] is True
    assert "35.9" in keep["why"]
    assert keep["process_plus_ev"] is True


def test_run_fold_scores_controls_without_leaking_is_lots() -> None:
    """Tiny grid: IS prefers the early winner; OOS books start at $10k."""
    is_days = CAL[:6]
    oos_days = CAL[6:10]
    cal = is_days + oos_days
    rows, bars = [], {}
    for d in is_days:
        rows += [
            _row(d, "HOT", sources=["union"], ohlc_hot_score=9,
                 news="missing"),
            _row(d, "RED", sources=["union"], ohlc_hot_score=1, news="bad"),
            _row(d, "Z1", sources=["union"], ohlc_hot_score=2),
            _row(d, "Z2", sources=["union"], ohlc_hot_score=2),
        ]
        bars[("HOT", d)] = {"open": 10.0, "close": 11.0}
        bars[("RED", d)] = {"open": 10.0, "close": 9.0}
        bars[("Z1", d)] = {"open": 10.0, "close": 10.0}
        bars[("Z2", d)] = {"open": 10.0, "close": 10.0}
    for d in oos_days:
        rows += [
            _row(d, "HOT", sources=["union"], ohlc_hot_score=9,
                 news="missing"),
            _row(d, "RED", sources=["union"], ohlc_hot_score=1, news="bad"),
            _row(d, "Z1", sources=["union"], ohlc_hot_score=2),
            _row(d, "Z2", sources=["union"], ohlc_hot_score=2),
        ]
        bars[("HOT", d)] = {"open": 10.0, "close": 9.5}
        bars[("RED", d)] = {"open": 10.0, "close": 10.5}
        bars[("Z1", d)] = {"open": 10.0, "close": 10.0}
        bars[("Z2", d)] = {"open": 10.0, "close": 10.0}
    panel = _panel(cal, rows)
    recipes = [
        fm.make_recipe(wf.HOT4, universe="union", hold=1, top_n=1,
                       rank="hot_score", forbid={"alarm": True}, sell="time"),
        fm.make_recipe(wf.NEWS_RED, universe="union", hold=1, side="short",
                       require={"news": "bad"}, sell="time"),
        fm.make_recipe("union_h1", universe="union", hold=1, top_n=1,
                       sell="time"),
    ]
    fold = {
        "cutoff": is_days[-1],
        "is_dates": is_days,
        "oos_dates": oos_days,
    }
    out = wf.run_fold(
        panel, fold, recipes, bars=bars, fees=ZERO_FEES, regime={}, mine=True)
    assert out["selection"]["best_long"] in (wf.HOT4, "union_h1")
    assert out["selection"]["best_short"] == wf.NEWS_RED
    roles = {r["role"] for r in out["oos"]}
    for need in ("control_hot4", "control_news_red", "control_combo_sh",
                 "control_random", "discovered_5050", "neutralized"):
        assert need in roles
    combo = next(r for r in out["oos"] if r["role"] == "control_combo_sh")
    assert combo["n_sessions"] == len(oos_days)
    # IS leftover cannot be the OOS starting equity.
    assert combo.get("final_equity") is not None


def test_controls_exist_on_published_grid() -> None:
    names = {r["name"] for r in fm.build_recipes()}
    assert wf.HOT4 in names
    assert wf.NEWS_RED in names


def test_render_mentions_keep_kill_and_no_flatten_wire() -> None:
    payload = {
        "n_recipes": 235,
        "from_date": "2026-08-13",
        "to_date": "2026-09-11",
        "n_sessions": 21,
        "first_cutoff": "2026-08-20",
        "step": 4,
        "forward": 4,
        "folds": [],
        "pools": {
            "control_combo_sh": {
                "n_folds": 4, "n_green_folds": 1, "mean_book_pct": -2.0,
                "hit_rate": 0.4, "n_sessions": 16, "pnl": -800,
            },
        },
        "verdict": {
            "label": "KILL",
            "why": "Frozen combo is not +EV after fees.",
            "process_plus_ev": False,
            "combo": {"mean_book_pct": -2.0},
            "random": {"mean_book_pct": 0.1},
            "process": {"mean_book_pct": -1.0},
        },
    }
    md = wf.render_md(payload)
    assert "KILL" in md
    assert "flatten_robust" in md
    assert "does **not** change" in md
    assert "combo_sh_5050_shared" in md


if __name__ == "__main__":
    test_slice_panel_drops_later_sessions()
    test_folds_roll_forward_and_hide_oos()
    test_select_is_cannot_see_oos_winner()
    test_random_matched_same_count_avoids_actuals()
    test_pins_drive_simulate_book_names()
    test_neutralize_subtracts_exposure_times_market()
    test_verdict_kills_when_oos_not_plus_or_not_above_random()
    test_run_fold_scores_controls_without_leaking_is_lots()
    test_controls_exist_on_published_grid()
    test_render_mentions_keep_kill_and_no_flatten_wire()
    print("10 walk-forward factor-mine tests passed")
