"""Matched-hold combine backtest integrity. No network.

Run: python -m src.test_sleeve_combine_bt
"""
from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from src.sleeve_combine_bt import (
    MismatchError,
    _dual_gap,
    analyze_io_size_attrs,
    assert_matched_hold,
    build_bar_fn,
    dashboard_payload,
    enrich_io_with_mover,
    exit_date,
    fills_from_trades,
    io_keep,
    load_calendar,
    load_fees,
    order_fees,
    parse_reasons,
    render,
    run_bt,
    run_dual,
    write_dashboard,
)


CAL = [
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18",
    "2026-08-19", "2026-08-20", "2026-08-21",
]


def _bars(px: dict):
    """px[(ticker, date)] = (open, close). Missing → {}."""
    def fn(ticker, date):
        hit = px.get((ticker, date))
        if not hit:
            return {}
        o, c = hit
        return {"open": o, "close": c}
    return fn


def _fees():
    return load_fees()


def test_refuse_2w_combine() -> None:
    try:
        assert_matched_hold("combine", "2w")
    except MismatchError as e:
        assert "2w" in str(e)
    else:
        raise AssertionError("2w combine must be refused")
    assert_matched_hold("io_only", "2w")  # reference book is allowed
    assert_matched_hold("combine", "1d")


def test_exit_is_sessions_not_calendar() -> None:
    # Fri 08-14 → Mon 08-17 is ONE session for 1d
    assert exit_date(CAL, "2026-08-14", "1d") == "2026-08-17"
    assert exit_date(CAL, "2026-08-13", "3d") == "2026-08-18"
    assert exit_date(CAL, "2026-08-13", "1w") == "2026-08-20"
    assert exit_date(CAL, "2026-08-21", "1d") is None


def test_open_cannot_spend_same_day_close_cash() -> None:
    """1d hold: Monday buy spends the account; Tuesday 09:30 cannot reuse
    the Tuesday 16:00 exit. With $10k and 100% sizing, day-2 open must skip."""
    px = {}
    for d in CAL:
        px[("AAA", d)] = (100.0, 100.0)
        px[("BBB", d)] = (100.0, 100.0)
    mover = {
        "2026-08-13": [{"ticker": "AAA", "conviction": 1}],
        "2026-08-14": [{"ticker": "BBB", "conviction": 1}],
    }
    sim = run_bt(
        calendar=CAL, scores={"2026-08-13": 5.0, "2026-08-14": 5.0},
        mover_calls=mover, io_picks={}, bars=_bars(px),
        hold="1d", mode="mover_only", capital=12_000, top_n=1, pct=0.90,
        fees=_fees(),
    )
    d13 = next(c for c in sim["curve"] if c["date"] == "2026-08-13")
    am14 = next(c for c in sim["curve"] if c["date"] == "2026-08-14")
    assert d13["filled_am"] == 1, d13
    assert am14["filled_am"] == 0, am14
    assert any("insufficient cash" in (s.get("reason") or "")
               for s in sim["skipped"] if s["date"] == "2026-08-14")
    # Monday's exit still happens Tuesday close
    assert am14["exits"] == 1


def test_close_can_recycle_exit_cash_for_io() -> None:
    """After the 16:00 exit, an .io fill the same afternoon may reuse cash."""
    px = {}
    for d in CAL:
        px[("AAA", d)] = (100.0, 110.0)
        px[("CCC", d)] = (50.0, 50.0)
    mover = {"2026-08-13": [{"ticker": "AAA", "conviction": 1}]}
    io = {"2026-08-14": [{"ticker": "CCC", "score": 1}]}
    # 08-14 score in the messy band → io at close, after AAA exits
    scores = {"2026-08-13": 5.0, "2026-08-14": 0.0}
    sim = run_bt(
        calendar=CAL, scores=scores, mover_calls=mover, io_picks=io,
        bars=_bars(px),         hold="1d", mode="combine",
        capital=12_000, top_n=1, pct=0.90, fees=_fees(),
    )
    d13 = next(c for c in sim["curve"] if c["date"] == "2026-08-13")
    d14 = next(c for c in sim["curve"] if c["date"] == "2026-08-14")
    assert d13["filled_am"] == 1
    assert d14["exits"] == 1
    assert d14["filled_pm"] == 1, d14
    assert d14["route"] == "io"


def test_missing_book_is_a_gap() -> None:
    scores = {"2026-08-13": 0.0}  # io day
    sim = run_bt(
        calendar=CAL[:3], scores=scores, mover_calls={}, io_picks={},
        bars=_bars({}), hold="1d", mode="combine",
        capital=10_000, top_n=1, pct=0.1, fees=_fees(),
    )
    d13 = next(c for c in sim["curve"] if c["date"] == "2026-08-13")
    assert d13["filled_pm"] == 0
    assert "io source missing" in d13["gap"]
    assert sim["n_gap_days"] >= 1


def test_missing_mover_calls_on_green_day_is_a_gap() -> None:
    """08-13 / 08-14 style: score is +5 but mover called nothing."""
    scores = {"2026-08-13": 5.5}
    sim = run_bt(
        calendar=CAL[:3], scores=scores, mover_calls={}, io_picks={},
        bars=_bars({}), hold="1d", mode="combine",
        capital=10_000, top_n=1, pct=0.1, fees=_fees(),
    )
    d13 = next(c for c in sim["curve"] if c["date"] == "2026-08-13")
    assert d13["route"] == "mover"
    assert d13["filled_am"] == 0
    assert "mover source empty" in d13["gap"]


def test_missing_bar_skips() -> None:
    mover = {"2026-08-13": [{"ticker": "GHOST", "conviction": 1}]}
    sim = run_bt(
        calendar=CAL[:3], scores={"2026-08-13": 5.0},
        mover_calls=mover, io_picks={}, bars=_bars({}),
        hold="1d", mode="mover_only", capital=10_000, top_n=1, pct=0.1,
        fees=_fees(),
    )
    assert sim["n_trades"] == 0
    assert any("missing open bar" in (s.get("reason") or "")
               for s in sim["skipped"])


def test_1w_hold_locks_cash_against_later_mover() -> None:
    px = {}
    for d in CAL:
        px[("AAA", d)] = (100.0, 100.0)
        px[("BBB", d)] = (100.0, 100.0)
    io = {"2026-08-13": [{"ticker": "AAA", "score": 1}]}
    mover = {"2026-08-14": [{"ticker": "BBB", "conviction": 1}]}
    scores = {"2026-08-13": 0.0, "2026-08-14": 5.0}
    sim = run_bt(
        calendar=CAL, scores=scores, mover_calls=mover, io_picks=io,
        bars=_bars(px),         hold="1w", mode="combine",
        capital=12_000, top_n=1, pct=0.90, fees=_fees(),
    )
    d13 = next(c for c in sim["curve"] if c["date"] == "2026-08-13")
    d14 = next(c for c in sim["curve"] if c["date"] == "2026-08-14")
    assert d13["filled_pm"] == 1
    assert d14["route"] == "mover"
    assert d14["filled_am"] == 0, "1w .io hold must starve Tuesday mover"
    assert d14["open"] == 1


def test_hard_red_does_not_flatten() -> None:
    px = {("AAA", d): (100.0, 100.0) for d in CAL}
    io = {"2026-08-13": [{"ticker": "AAA", "score": 1}]}
    scores = {"2026-08-13": 0.0, "2026-08-14": -6.0}
    sim = run_bt(
        calendar=CAL, scores=scores, mover_calls={}, io_picks=io,
        bars=_bars(px),         hold="1w", mode="combine",
        capital=12_000, top_n=1, pct=0.90, fees=_fees(),
    )
    d14 = next(c for c in sim["curve"] if c["date"] == "2026-08-14")
    assert d14["route"] == "cash"
    assert d14["open"] == 1
    assert d14["exits"] == 0
    assert "no new entries" in d14["gap"]


def test_fees_and_whole_shares() -> None:
    px = {("AAA", d): (33.0, 33.0) for d in CAL}
    mover = {"2026-08-13": [{"ticker": "AAA", "conviction": 1}]}
    sim = run_bt(
        calendar=CAL[:4], scores={"2026-08-13": 5.0},
        mover_calls=mover, io_picks={}, bars=_bars(px),
        hold="1d", mode="mover_only", capital=10_000, top_n=1, pct=0.10,
        fees=_fees(),
    )
    assert sim["n_trades"] == 1
    t = sim["trades"][0]
    assert t["shares"] == int((10_000 * 0.10) // 33.0)
    assert t["fee_in"] > 0 and t["fee_out"] > 0
    expect_in = order_fees(t["shares"], 33.0, "buy", _fees())
    assert abs(t["fee_in"] - expect_in) < 0.02


def test_bar_fn_does_not_invent_open() -> None:
    payload = {"called_rows": [
        {"date": "2026-08-13", "ticker": "AAA",
         "session_bar": {"open": 10.0, "close": 11.0}},
    ]}
    fn = build_bar_fn(payload)
    assert fn("AAA", "2026-08-13")["open"] == 10.0
    # unknown name / day → empty, never a fabricated open
    ghost = fn("GHOST", "2026-08-13")
    assert not ghost.get("open")


def test_report_says_combine_lost_when_it_did() -> None:
    md = render({
        "generated_at": "t", "window": ["2026-08-13", "2026-09-03"],
        "capital": 100000, "top_n": 10, "pct": 0.1,
        "n_sessions": 17, "n_mover_call_days": 14, "n_book_days": 13,
        "results": [
            {"hold": "1d", "mode": "combine", "total_ret_pct": 0.59,
             "max_dd_pct": 1.6, "hit": 0.38, "n_trades": 29,
             "n_gap_days": 9, "by_source": {"mover": {"pnl": 1}, "io": {"pnl": -1}}},
            {"hold": "1d", "mode": "mover_only", "total_ret_pct": 1.55,
             "max_dd_pct": 0.7, "hit": 0.43, "n_trades": 30,
             "n_gap_days": 11, "by_source": {"mover": {"pnl": 1}, "io": {"pnl": 0}}},
            {"hold": "1d", "mode": "io_only", "total_ret_pct": 6.5,
             "max_dd_pct": 2.5, "hit": 0.48, "n_trades": 85,
             "n_gap_days": 4, "by_source": {"mover": {"pnl": 0}, "io": {"pnl": 6}}},
            {"hold": "1d", "mode": "dual", "total_ret_pct": 4.0,
             "max_dd_pct": 1.5, "hit": 0.5, "n_trades": 50,
             "n_gap_days": 5, "by_source": {"mover": {"pnl": 1}, "io": {"pnl": 3}}},
        ],
    })
    assert "Finding" in md
    assert "worse than" in md
    assert "dual" in md
    assert "cannot combine mover with .io hold" not in md
    assert "2w / 1m are not combined" in md
    md_attr = render({
        "generated_at": "t", "window": ["2026-08-13", "2026-09-03"],
        "capital": 100000, "top_n": 10, "pct": 0.1,
        "n_sessions": 17, "n_mover_call_days": 14, "n_book_days": 13,
        "results": [],
        "io_attrs": {
            "n_prints": 10, "n_down": 6, "n_green": 4,
            "cuts": {"down_large+": {"n": 3, "mean": 0.4, "hit": 0.67}},
        },
        "io_attr_books": [
            {"filter": "all", "total_ret_pct": 6.5, "max_dd_pct": 2.5,
             "hit": 0.48, "n_trades": 85},
        ],
    })
    assert "inside the size book" in md_attr
    assert "`all`" in md_attr
    assert "stay in the size book" in md_attr
    assert "How to backtest every session" in md_attr
    assert "sleeve-combine" in md_attr
    assert "beats the raw size book" in md_attr


def test_dual_keeps_io_on_down_days() -> None:
    """Mover wallet sits out S<1; .io wallet still fills at the close."""
    px = {("AAA", d): (100.0, 100.0) for d in CAL}
    px.update({("CCC", d): (50.0, 50.0) for d in CAL})
    sim = run_dual(
        calendar=CAL,
        scores={"2026-08-13": 5.0, "2026-08-14": -6.0},
        mover_calls={"2026-08-13": [{"ticker": "AAA", "conviction": 1}]},
        io_picks={"2026-08-14": [{"ticker": "CCC", "score": 1}]},
        bars=_bars(px), hold="1d", io_select="top",
        capital=24_000, top_n=1, pct=0.90, fees=_fees(),
    )
    assert sim["mode"] == "dual"
    d13 = next(c for c in sim["curve"] if c["date"] == "2026-08-13")
    d14 = next(c for c in sim["curve"] if c["date"] == "2026-08-14")
    assert d13["filled_am"] == 1
    assert d14["filled_pm"] == 1, "io wallet must still buy the down day"
    assert d14["route"] == "dual"
    assert "route cash" not in (d14.get("gap") or ""), d14
    srcs = {t["source"] for t in sim["trades"]}
    assert srcs == {"mover", "io"}


def test_dual_gap_drops_expected_mover_cash() -> None:
    assert _dual_gap("route cash — no new entries", "") == ""
    assert _dual_gap(
        "route cash — no new entries",
        "io source missing (no stock_book file)",
    ) == "io source missing (no stock_book file)"
    assert _dual_gap("mover source empty (no BUY calls)", "") == (
        "mover source empty (no BUY calls)")


def test_parse_reasons_and_keeps() -> None:
    blob = ("join=+0.32; sector=+0.65; gen=-0.08; news=+0.83; "
            "ev={'event': 'hormuz_energy_risk', 'side': 'buy'}")
    r = parse_reasons(blob)
    assert r["join"] == 0.32
    assert r["sector"] == 0.65
    assert r["has_event"] is True
    assert parse_reasons("join=+0.39; rebound_floor")["rebound_floor"] is True
    pick = {"bucket": "large+", "sector": "Energy", "rebound": False,
            "reasons": blob}
    assert io_keep("large+")(pick)
    assert io_keep("energy")(pick)
    assert io_keep("event")(pick)
    assert not io_keep("rebound")(pick)
    assert io_keep("sector_good")(pick)


def test_io_attr_cut_splits_down_vs_green() -> None:
    cal = ["2026-08-13", "2026-08-14", "2026-08-17"]
    scores = {"2026-08-13": 5.0, "2026-08-14": -6.0}
    io = {
        "2026-08-13": [{"ticker": "AAA", "bucket": "large+",
                        "sector": "Tech", "reasons": "join=+0.2; sector=+0.1"}],
        "2026-08-14": [{"ticker": "BBB", "bucket": "mid",
                        "sector": "Energy", "reasons": "join=-0.1; sector=+0.6; "
                        "ev={'event': 'x'}"}],
    }
    px = {
        ("AAA", "2026-08-13"): (10.0, 10.0),
        ("AAA", "2026-08-14"): (10.0, 11.0),
        ("BBB", "2026-08-14"): (20.0, 20.0),
        ("BBB", "2026-08-17"): (20.0, 18.0),
    }
    doc = analyze_io_size_attrs(cal, scores, io, _bars(px), hold="1d")
    assert doc["n_prints"] == 2
    assert doc["n_green"] == 1
    assert doc["n_down"] == 1
    assert doc["cuts"]["down"]["n"] == 1
    assert doc["cuts"]["down"]["mean"] == -10.0
    assert doc["cuts"]["green"]["mean"] == 10.0
    assert doc["cuts"]["down_energy"]["n"] == 1


def test_run_bt_rejects_bad_mode() -> None:
    try:
        run_bt(calendar=CAL, scores={}, mover_calls={}, io_picks={},
               bars=_bars({}), mode="average_the_lists")
    except ValueError:
        return
    raise AssertionError("bad mode must raise")


def test_fallback_io_on_soft_red_mover_otherwise() -> None:
    px = {("AAA", d): (100.0, 100.0) for d in CAL}
    px.update({("CCC", d): (50.0, 50.0) for d in CAL})
    sim = run_bt(
        calendar=CAL,
        scores={"2026-08-13": 2.25, "2026-08-14": -0.9, "2026-08-17": -6.0},
        mover_calls={"2026-08-13": [{"ticker": "AAA", "conviction": 1}]},
        io_picks={"2026-08-14": [{"ticker": "CCC", "score": 1}]},
        bars=_bars(px), hold="1d", mode="fallback",
        capital=24_000, top_n=1, pct=0.90, fees=_fees(),
    )
    d13 = next(c for c in sim["curve"] if c["date"] == "2026-08-13")
    d14 = next(c for c in sim["curve"] if c["date"] == "2026-08-14")
    d17 = next(c for c in sim["curve"] if c["date"] == "2026-08-17")
    assert d13["route"] == "mover" and d13["filled_am"] == 1
    assert d14["route"] == "io" and d14["filled_pm"] == 1
    assert d17["route"] == "io" and d17["filled_am"] == 0 and d17["filled_pm"] == 0
    assert "hard-red" in (d17.get("gap") or "")


def test_fallback_last_day_io_is_a_gap() -> None:
    px = {("CCC", "2026-08-17"): (50.0, 50.0)}
    sim = run_bt(
        calendar=["2026-08-17"],
        scores={"2026-08-17": -0.9},
        mover_calls={},
        io_picks={"2026-08-17": [{"ticker": "CCC", "score": 1}]},
        bars=_bars(px), hold="1d", mode="fallback",
        capital=24_000, top_n=1, pct=0.90, fees=_fees(),
    )
    d = sim["curve"][0]
    assert d["route"] == "io" and d["filled_pm"] == 0
    assert "end of calendar" in (d.get("gap") or "")


def test_overlay_keeps_io_on_red_and_caps_mover() -> None:
    """Full .io book stays on; mover satellite is 1 name, even with 100% cash."""
    px = {("AAA", d): (100.0, 100.0) for d in CAL}
    px.update({("CCC", d): (50.0, 50.0) for d in CAL})
    px.update({("DDD", d): (25.0, 25.0) for d in CAL})
    sim = run_bt(
        calendar=CAL,
        scores={"2026-08-13": 5.0, "2026-08-14": -6.0},
        mover_calls={
            "2026-08-13": [{"ticker": "AAA", "conviction": 2},
                           {"ticker": "DDD", "conviction": 1}],
        },
        io_picks={"2026-08-13": [{"ticker": "CCC", "score": 1}],
                  "2026-08-14": [{"ticker": "CCC", "score": 1}]},
        bars=_bars(px), hold="1d", mode="overlay",
        capital=24_000, top_n=10, pct=0.10, sat_n=1, sat_pct=0.10,
        fees=_fees(),
    )
    d13 = next(c for c in sim["curve"] if c["date"] == "2026-08-13")
    d14 = next(c for c in sim["curve"] if c["date"] == "2026-08-14")
    assert d13["filled_am"] == 1, "satellite capped at sat_n"
    assert d13["filled_pm"] == 1, "io still fills after satellite"
    assert d14["filled_pm"] == 1, "io still fills the red afternoon"
    assert d14["filled_am"] == 0, "hard-red morning does not add mover"
    srcs = {t["source"] for t in sim["trades"]}
    assert "io" in srcs and "mover" in srcs


def test_enrich_sizes_up_overlap_and_adds_book_extra() -> None:
    io = {"2026-08-13": [
        {"ticker": "AAA", "score": 1, "bucket": "large+"},
        {"ticker": "BBB", "score": 0.5, "bucket": "mid"},
    ]}
    mover = {"2026-08-13": [{"ticker": "AAA"}, {"ticker": "ZZZ"}]}
    buys = {"2026-08-13": [{"ticker": "ZZZ", "score": 0.4}]}
    out = enrich_io_with_mover(io, mover, buys, boost_pct=0.15)
    tickers = [p["ticker"] for p in out["2026-08-13"]]
    assert tickers[0] == "AAA"
    assert "ZZZ" in tickers
    aaa = next(p for p in out["2026-08-13"] if p["ticker"] == "AAA")
    assert aaa["_pct"] == 0.15


def test_fills_are_buy_then_sell() -> None:
    trades = [{
        "entry_dt": "2026-08-13 09:30 ET", "date": "2026-08-13",
        "ticker": "AAA", "source": "mover", "shares": 10,
        "entry_px": 10.0, "exit_dt": "2026-08-14 16:00 ET",
        "exit_px": 11.0, "fee_in": 0.5, "fee_out": 0.4, "pnl": 9.1,
        "hold": "1d",
    }]
    fills = fills_from_trades(trades)
    assert [f["side"] for f in fills] == ["BUY", "SELL"]
    assert fills[0]["clock"] == "09:30 ET"
    assert fills[1]["side"] == "SELL" and fills[1]["pnl"] == 9.1


def test_dashboard_lists_every_session_fill() -> None:
    sim = run_dual(
        calendar=CAL,
        scores={"2026-08-13": 5.0, "2026-08-14": -6.0},
        mover_calls={"2026-08-13": [{"ticker": "AAA", "conviction": 1}]},
        io_picks={"2026-08-14": [{"ticker": "CCC", "score": 1}]},
        bars=_bars({
            **{("AAA", d): (100.0, 100.0) for d in CAL},
            **{("CCC", d): (50.0, 50.0) for d in CAL},
        }),
        hold="1d", io_select="top",
        capital=24_000, top_n=1, pct=0.90, fees=_fees(),
    )
    doc = {"generated_at": "t", "window": [CAL[0], CAL[-1]],
           "capital": 24000, "results": []}
    payload = dashboard_payload(doc, sim)
    assert payload["fills"]
    sides = {f["side"] for f in payload["fills"]}
    assert sides == {"BUY", "SELL"}
    dates = {d["date"] for d in payload["days"]}
    assert "2026-08-13" in dates and "2026-08-14" in dates
    with TemporaryDirectory() as tmp:
        html = write_dashboard(doc, sim, path=Path(tmp) / "index.html")
        text = html.read_text(encoding="utf-8")
    assert "Buys and sells" in text
    assert "AAA" in text and "CCC" in text
    assert '"side": "BUY"' in text


def test_calendar_from_to_clips() -> None:
    cal = load_calendar(
        {"session_dates": ["2026-08-13", "2026-08-14", "2026-08-17",
                           "2026-08-18"]},
        from_date="2026-08-14", to_date="2026-08-17",
    )
    assert cal[0] >= "2026-08-14"
    assert cal[-1] <= "2026-08-17"


def test_run_bt_rejects_dual_inline() -> None:
    try:
        run_bt(calendar=CAL, scores={}, mover_calls={}, io_picks={},
               bars=_bars({}), mode="dual")
    except ValueError as e:
        assert "run_dual" in str(e)
        return
    raise AssertionError("dual must go through run_dual")


if __name__ == "__main__":
    test_refuse_2w_combine()
    test_exit_is_sessions_not_calendar()
    test_open_cannot_spend_same_day_close_cash()
    test_close_can_recycle_exit_cash_for_io()
    test_missing_book_is_a_gap()
    test_missing_mover_calls_on_green_day_is_a_gap()
    test_missing_bar_skips()
    test_1w_hold_locks_cash_against_later_mover()
    test_hard_red_does_not_flatten()
    test_fees_and_whole_shares()
    test_bar_fn_does_not_invent_open()
    test_report_says_combine_lost_when_it_did()
    test_dual_keeps_io_on_down_days()
    test_dual_gap_drops_expected_mover_cash()
    test_parse_reasons_and_keeps()
    test_io_attr_cut_splits_down_vs_green()
    test_run_bt_rejects_bad_mode()
    test_fallback_io_on_soft_red_mover_otherwise()
    test_fallback_last_day_io_is_a_gap()
    test_overlay_keeps_io_on_red_and_caps_mover()
    test_enrich_sizes_up_overlap_and_adds_book_extra()
    test_fills_are_buy_then_sell()
    test_dashboard_lists_every_session_fill()
    test_calendar_from_to_clips()
    test_run_bt_rejects_dual_inline()
    print("ok")
