"""Leak-free factor-mine combination books — unit tests, no full remine."""
from __future__ import annotations

from src import factor_mine as fm
from src import factor_mine_book as fmb
from src import factor_mine_combo as fmc

DATES = [
    "2026-08-13", "2026-08-14", "2026-08-17",
    "2026-08-18", "2026-08-19",
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


def _boxes(news="missing"):
    return {k: "missing" for k in fm.CAMERAS} | {"news": news}


def _row(date, ticker, **kw):
    news = kw.pop("news", "missing")
    boxes = kw.pop("boxes", None) or _boxes(news)
    return {
        "date": date,
        "ticker": ticker,
        "sources": kw.pop("sources", ["union"]),
        "boxes": boxes,
        "blue": False,
        "alarm": kw.pop("alarm", False),
        "zero_red": True,
        "last_green": kw.pop("last_green", False),
        "last_red": False,
        "src_rank": kw.pop("src_rank", 1),
        "erd_days_since_E": kw.pop("erd_days_since_E", None),
        "erd_flag_E": kw.pop("erd_flag_E", None),
        **kw,
    }


def _panel(rows, dates=DATES):
    by = {}
    for r in rows:
        by.setdefault(r["date"], []).append(r)
    return {
        "session_dates": list(dates),
        "from_date": dates[0],
        "to_date": dates[-1],
        "n_sessions": len(dates),
        "n_rows": len(rows),
        "rows": rows,
        "by_date": by,
    }


def _rec(name: str) -> dict:
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    rec = rec_by.get(name)
    if rec is None:
        raise AssertionError(f"missing recipe {name}")
    return rec


def test_combo_specs_unique_and_members_known() -> None:
    specs = fmc.combo_specs()
    names = [s["name"] for s in specs]
    assert len(names) == len(set(names))
    assert len(specs) >= 40
    known = {r["name"] for r in fm.build_recipes()} | set(fmc.MEMBER_POOL)
    for spec in specs:
        assert spec["members"]
        assert len(spec["members"]) == len(spec["weights"])
        assert spec["pool"] in ("shared", "split")
        assert spec["net"] in ("priority", "skip", "weather", "none") or spec["pool"] == "split"
        for n in spec["members"]:
            assert n in known, n
    assert any(s["name"] == "combo_seh_403525_shared" for s in specs)
    assert any(s["pool"] == "split" for s in specs)


def test_choose_intents_priority_one_side() -> None:
    e = {"ticker": "INO", "side": "long", "rank": fmc._claim_rank("union_e_fresh_h3"),
         "rec": {"name": "union_e_fresh_h3"}}
    h = {"ticker": "INO", "side": "long", "rank": fmc._claim_rank("union_hot_n4_h1"),
         "rec": {"name": "union_hot_n4_h1"}}
    s = {"ticker": "INO", "side": "short", "rank": fmc._claim_rank("short_news_r_h3"),
         "rec": {"name": "short_news_r_h3"}}
    kept = fmc._choose_intents([e, h, s], net="priority", s=1.0)
    assert len(kept) == 1
    assert kept[0]["rec"]["name"] == "union_e_fresh_h3"
    assert kept[0]["side"] == "long"


def test_choose_intents_skip_and_weather() -> None:
    e = {"ticker": "EU", "side": "long", "rank": 0, "rec": {"name": "union_e_fresh_h3"}}
    s = {"ticker": "EU", "side": "short", "rank": 9, "rec": {"name": "short_news_r_h3"}}
    assert fmc._choose_intents([e, s], net="skip", s=1.0) == []
    red = fmc._choose_intents([e, s], net="weather", s=-1.0)
    assert len(red) == 1 and red[0]["side"] == "short"
    green = fmc._choose_intents([e, s], net="weather", s=2.0)
    assert len(green) == 1 and green[0]["side"] == "long"


def test_shared_no_double_ticker_or_opposite_side() -> None:
    dates = DATES
    rows = []
    bars = {}
    for d in dates:
        rows.append(_row(d, "INO", news="bad", erd_days_since_E=0, erd_flag_E=1))
        rows.append(_row(d, "ZZZ", news="missing"))
        bars[("INO", d)] = {"open": 2.0, "close": 2.2}
        bars[("ZZZ", d)] = {"open": 5.0, "close": 5.0}
    panel = _panel(rows, dates)
    recs = [_rec("union_e_fresh_h3"), _rec("short_news_r_h3")]
    book = fmc.simulate_shared(
        panel, recs, [1, 1], bars=bars, fees=ZERO_FEES, regime={},
        net="priority", name="t_net")
    fills = [t for t in book["trades"] if t.get("side") in ("BUY", "SHORT")]
    by_day = {}
    for t in fills:
        by_day.setdefault((t["date"], t["ticker"]), set()).add(t["side"])
    assert all(len(v) == 1 for v in by_day.values())
    ino_sides = {t["side"] for t in fills if t["ticker"] == "INO"}
    assert ino_sides == {"BUY"}
    assert book["audit"]["ok"] is True


def test_shared_hard_red_sits_new_buys() -> None:
    dates = DATES[:3]
    rows = [_row(d, "INO", erd_days_since_E=0, erd_flag_E=1) for d in dates]
    bars = {("INO", d): {"open": 1.0, "close": 1.1} for d in dates}
    panel = _panel(rows, dates)
    recs = [_rec("union_e_fresh_h3")]
    regime = {dates[0]: {"predict_score": -5.2}}
    book = fmc.simulate_shared(
        panel, recs, [1], bars=bars, fees=ZERO_FEES, regime=regime,
        name="t_red")
    day0 = [t for t in book["trades"]
            if t["date"] == dates[0] and t.get("side") in ("BUY", "SHORT")]
    assert day0 == []
    assert any(k.get("kind") == "hard_red" for k in book["skips"])
    later = [t for t in book["trades"]
             if t["date"] != dates[0] and t.get("side") == "BUY"]
    assert later, "later non-red mornings should still buy"


def test_shared_owner_min_hold() -> None:
    dates = DATES
    rows = []
    bars = {}
    for i, d in enumerate(dates):
        if i == 0:
            rows.append(_row(d, "INO", erd_days_since_E=0, erd_flag_E=1))
        else:
            rows.append(_row(d, "ZZZ"))
        bars[("INO", d)] = {"open": 1.0 + i * 0.05, "close": 1.02 + i * 0.05}
        bars[("ZZZ", d)] = {"open": 4.0, "close": 4.0}
    panel = _panel(rows, dates)
    recs = [_rec("union_e_fresh_h3")]  # hold 3, list-drop
    book = fmc.simulate_shared(
        panel, recs, [1], bars=bars, fees=ZERO_FEES, regime={}, name="t_hold")
    sells = [t for t in book["trades"] if t.get("side") == "SELL" and t["ticker"] == "INO"]
    assert sells, book["trades"]
    assert sells[0]["date"] == "2026-08-18"
    assert sells[0]["held"] >= 3
    assert any(k.get("kind") == "min_hold" for k in book["skips"])


def test_missing_open_is_not_replaced_by_close() -> None:
    dates = DATES[:2]
    rows = [_row(d, "GAP", erd_days_since_E=0, erd_flag_E=1) for d in dates]
    bars = {
        ("GAP", dates[0]): {"open": None, "close": 50.0},
        ("GAP", dates[1]): {"open": 10.0, "close": 10.5},
    }
    panel = _panel(rows, dates)
    recs = [_rec("union_e_fresh_h3")]
    book = fmc.simulate_shared(
        panel, recs, [1], bars=bars, fees=ZERO_FEES, regime={}, name="t_px")
    day0 = [t for t in book["trades"]
            if t["date"] == dates[0] and t.get("side") == "BUY"]
    assert day0 == []
    assert any(k.get("kind") == "no_price" and k["ticker"] == "GAP"
               for k in book["skips"])
    day1 = [t for t in book["trades"]
            if t["date"] == dates[1] and t.get("side") == "BUY"]
    assert day1 and abs(float(day1[0]["price"]) - 10.0) < 1e-9


def test_split_survives_unclosed_panel_session() -> None:
    """Pre-Open adds today to the panel; member books stop at last close.

    Factor strategy mine 34587345171 died here on 2026-09-11:
    ``simulate_split`` indexed ``daily[i]`` against the raw panel
    calendar (including the still-open session) and IndexError'd.
    """
    open_day = "2099-01-15"
    dates = DATES + [open_day]
    rows = [_row(d, "WIN") for d in dates]
    bars = {("WIN", d): {"open": 10.0, "close": 11.0} for d in dates}
    panel = _panel(rows, dates)
    rec = fm.make_recipe("union_h1", universe="union", hold=1, top_n=1)
    rec["name"] = "union_h1"
    book = fmc.simulate_split(
        panel, [rec, rec], [1, 1], bars=bars, fees=ZERO_FEES, regime={},
        name="t_open")
    assert {d["date"] for d in book["daily"]} == set(DATES)
    assert open_day not in {d["date"] for d in book["daily"]}
    shared = fmc.simulate_shared(
        panel, [rec], [1], bars=bars, fees=ZERO_FEES, regime={},
        name="t_open_shared")
    assert {d["date"] for d in shared["daily"]} == set(DATES)


def test_split_scales_capital_and_keeps_member_audits() -> None:
    dates = DATES[:3]
    rows = [_row(d, "WIN") for d in dates]
    bars = {("WIN", d): {"open": 10.0, "close": 11.0} for d in dates}
    panel = _panel(rows, dates)
    rec = fm.make_recipe("union_h1", universe="union", hold=1, top_n=1)
    rec["name"] = "union_h1"
    book = fmc.simulate_split(
        panel, [rec, rec], [1, 1], bars=bars, fees=ZERO_FEES, regime={},
        name="t_split")
    assert book["pool"] == "split"
    assert book["audit"]["ok"] is True
    assert len(book["parts"]) == 2
    assert abs(book["parts"][0]["capital"] - 5000.0) < 1e-6
    # Two accounts may hold the same name.
    assert book["collisions"]["double_long"]


def test_scorecard_beats_all_book() -> None:
    combo = {
        "total_ret_pct": 20.0, "max_dd_pct": 3.0, "start_rate": 0.8,
        "both_halves": True, "audit_ok": True, "worst_day_pct": -1.0,
        "effectiveness": 10.0,
    }
    members = [
        {"total_ret_pct": 10.0, "max_dd_pct": 4.0, "start_rate": 0.5,
         "worst_day_pct": -2.0, "effectiveness": 40.0},
        {"total_ret_pct": 12.0, "max_dd_pct": 5.0, "start_rate": 0.6,
         "worst_day_pct": -3.0, "effectiveness": 41.0},
    ]
    sc = fmc.scorecard(combo, members)
    assert sc["beats_all_book"] is True
    assert sc["outperforms"] is True


def test_scorecard_best_of_both_and_rejects_eff_only() -> None:
    combo = {
        "total_ret_pct": 21.0, "max_dd_pct": 1.0, "start_rate": 0.94,
        "both_halves": True, "audit_ok": True, "worst_day_pct": -0.6,
        "effectiveness": 12.0,
    }
    members = [
        {"name": "e", "total_ret_pct": 22.8, "max_dd_pct": 7.2, "start_rate": 0.67,
         "worst_day_pct": -5.2, "effectiveness": 8.0},
        {"name": "s", "total_ret_pct": 14.7, "max_dd_pct": 1.5, "start_rate": 0.94,
         "worst_day_pct": -0.8, "effectiveness": 20.0},
        {"name": "h", "total_ret_pct": 17.4, "max_dd_pct": 7.9, "start_rate": 0.56,
         "worst_day_pct": -6.7, "effectiveness": 9.0},
    ]
    sc = fmc.scorecard(combo, members)
    assert sc["beats_all_book"] is False
    assert sc["best_of_both"] is True
    assert sc["outperforms"] is True

    eff_only = dict(combo, total_ret_pct=5.0, max_dd_pct=8.0,
                    effectiveness=99.0, both_halves=False)
    sc2 = fmc.scorecard(eff_only, members)
    assert sc2["outperforms"] is False


def test_explain_recipe_combo_does_not_int_mix() -> None:
    spec = next(s for s in fmc.combo_specs() if s["name"] == "combo_seh_333_shared")
    rec = fmc.combo_recipe(spec)
    assert rec["hold"] == 5
    assert rec["universe"] == "combo"
    ex = fm.explain_recipe(rec)
    blob = " ".join([ex.get("kid") or ""] + list(ex.get("inputs") or []))
    assert "09:30" in blob
    assert "Change%" in blob
    assert "leftover" in (ex.get("kid") or "").lower()
    assert ex.get("size") == "leftover"
    assert ex.get("sell_rule") == "list"
    assert "mashed" in (ex.get("kid") or "").lower()


def test_run_skips_combos_when_members_absent() -> None:
    dates = ["2026-08-17"]
    panel = _panel([], dates)
    recs = [fm.make_recipe("demo_h1", hold=1)]
    payload = fm.run(
        "2026-08-17", "2026-08-17", write=False, recipes=recs, panel=panel,
        book=True, bars={}, combos=True,
    )
    names = {s["name"] for s in payload["stats"]}
    assert names == {"demo_h1"}
    assert (payload.get("combos") or {}).get("n") == 0


if __name__ == "__main__":
    test_combo_specs_unique_and_members_known()
    test_choose_intents_priority_one_side()
    test_choose_intents_skip_and_weather()
    test_shared_no_double_ticker_or_opposite_side()
    test_shared_hard_red_sits_new_buys()
    test_shared_owner_min_hold()
    test_missing_open_is_not_replaced_by_close()
    test_split_survives_unclosed_panel_session()
    test_split_scales_capital_and_keeps_member_audits()
    test_scorecard_beats_all_book()
    test_scorecard_best_of_both_and_rejects_eff_only()
    test_explain_recipe_combo_does_not_int_mix()
    test_run_skips_combos_when_members_absent()
    print("13 factor-mine combo tests passed")
