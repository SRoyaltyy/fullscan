"""$10k butterfly fill realities — unit tests, no parquet required.

Run: PYTHONPATH=. python3 -m src.test_book_fill_reality
"""
from __future__ import annotations

from src import book_fill_reality as bfr
from src import factor_mine as fm
from src import factor_mine_book as fmb
from src import open_bell_slip as obs

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

CAL = ["2026-08-13", "2026-08-14", "2026-08-17"]
BAR = {"open": 10.0, "high": 11.0, "low": 9.5, "close": 10.4}
SLIP = {"alpha_buy": 0.35, "alpha_sell": 0.35, "delay_frac": 0.35}


def _row(date, ticker, **kw):
    r = {
        "date": date, "ticker": ticker, "sources": ["union"],
        "boxes": {"vol": "good"}, "blue": False, "alarm": False,
        "zero_red": True, "last_green": True, "last_red": False,
        "ohlc_ret_5": 3.0, "ohlc_rvol": 1.0, "ohlc_hot_score": 1.0,
        "src_rank": 0, "cond_good": 1, "cond_bad": 0,
    }
    r.update(kw)
    return r


def _panel(rows, dates=None):
    dates = list(dates or CAL)
    by = {}
    for r in rows:
        by.setdefault(r["date"], []).append(r)
    return {
        "session_dates": dates, "rows": rows, "by_date": by,
        "from_date": dates[0], "to_date": dates[-1],
        "n_sessions": len(dates),
    }


def test_trade_through_limit_fill_or_miss() -> None:
    ran = {"open": 10.0, "high": 11.0, "low": 10.0, "close": 10.8}
    back = {"open": 10.0, "high": 10.4, "low": 9.7, "close": 10.1}
    px, how = bfr.limit_through(ran, "long", 10.0)
    assert px is None and how == "limit_miss"
    px, how = bfr.limit_through(back, "long", 10.0)
    assert how == "limit_fill" and px == 10.0
    px, how = bfr.limit_through(ran, "short", 10.0)
    assert how == "limit_fill" and px == 10.0
    dumped = {"open": 10.0, "high": 10.0, "low": 9.2, "close": 9.4}
    px, how = bfr.limit_through(dumped, "short", 10.0)
    assert px is None and how == "limit_miss"
    # Prior-close limit: gap-and-go never comes back = miss.
    gapped = {"open": 12.0, "high": 12.5, "low": 11.8, "close": 12.2}
    got = bfr.resolve_reality("limit_prior", gapped, "long", prior_px=10.0)
    assert got["miss"] and got["how"] == "limit_miss"
    came = {"open": 12.0, "high": 12.2, "low": 9.8, "close": 11.0}
    got = bfr.resolve_reality("limit_prior", came, "long", prior_px=10.0)
    assert not got["miss"] and got["px"] == 10.0


def test_missing_bar_is_miss_never_close() -> None:
    got = bfr.resolve_reality("ideal", None, "long")
    assert got["miss"] and got["how"] == "no_open"
    got = bfr.resolve_reality("ideal", {"close": 99.0}, "long")
    assert got["miss"] and got["how"] == "no_open"
    got = bfr.resolve_reality("market_adverse", {"open": 10.0, "close": 11.0}, "long")
    assert got["miss"] and got["how"] == "no_extreme"
    got = bfr.resolve_reality("market_favorable", {"open": 10.0, "high": 11.0}, "long")
    assert got["miss"]  # no low — cannot invent the favorable print


def test_market_extremes_stay_inside_bar() -> None:
    buy_bad, how = bfr.market_extreme(BAR, "long", favorable=False)
    assert how == "adverse_high" and buy_bad == 11.0
    buy_good, _ = bfr.market_extreme(BAR, "long", favorable=True)
    assert buy_good == 9.5
    short_bad, _ = bfr.market_extreme(BAR, "short", favorable=False)
    assert short_bad == 9.5
    short_good, _ = bfr.market_extreme(BAR, "short", favorable=True)
    assert short_good == 11.0
    mid, _ = obs.market_fill(BAR, "long", SLIP)
    assert mid is not None and 10.0 < mid <= 11.0
    clipped = bfr.clip_fill(99.0, BAR)
    assert clipped == 11.0
    clipped = bfr.clip_fill(1.0, BAR)
    assert clipped == 9.5


def test_partial_50_and_gap_miss() -> None:
    assert bfr.partial_shares(1) == (1, False)  # all-or-none
    assert bfr.partial_shares(2) == (1, True)
    assert bfr.partial_shares(5) == (2, True)
    got = bfr.resolve_reality(
        "partial_50", BAR, "long", slip=SLIP, intended_shares=10)
    assert not got["miss"] and got["shares"] == 5 and got["partial"]
    got = bfr.resolve_reality(
        "partial_50", BAR, "long", slip=SLIP, intended_shares=1)
    assert not got["miss"] and got["shares"] == 1 and not got["partial"]
    # Buy gaps up 10% → miss. Short gaps down 10% → miss.
    up = {"open": 11.0, "high": 11.4, "low": 10.8, "close": 11.1}
    got = bfr.resolve_reality("gap_miss", up, "long", prior_px=10.0)
    assert got["miss"] and got["how"] == "gap_miss"
    got = bfr.resolve_reality("gap_miss", up, "short", prior_px=10.0)
    assert not got["miss"]  # gap with the short is fine
    down = {"open": 9.0, "high": 9.2, "low": 8.8, "close": 9.1}
    got = bfr.resolve_reality("gap_miss", down, "short", prior_px=10.0)
    assert got["miss"]
    got = bfr.resolve_reality("gap_miss", down, "long", prior_px=10.0)
    assert not got["miss"] and got["px"] == 9.0


def test_miss_leaves_leftover_and_never_spends_past_cash() -> None:
    dates = ["2026-08-13", "2026-08-14"]
    rows = [_row("2026-08-13", "AAA"), _row("2026-08-14", "BBB")]
    panel = _panel(rows, dates)
    bars = {
        ("AAA", "2026-08-13"): {"open": 10, "high": 12, "low": 10, "close": 11},
        ("AAA", "2026-08-14"): {"open": 11, "high": 11.5, "low": 10.5, "close": 11},
        ("BBB", "2026-08-13"): {"open": 10, "high": 10.2, "low": 9.8, "close": 10},
        ("BBB", "2026-08-14"): {"open": 10, "high": 10.2, "low": 9.8, "close": 10},
    }
    rec = fm.make_recipe("union_h1", hold=1, top_n=4)
    tape = bfr.Tape(bars=bars)
    # limit_open buy misses when low == open (ran away).
    miss = bfr.make_exec_fill("limit_open", tape)
    book = fmb.simulate_book(
        panel, rec, bars=bars, fees=ZERO_FEES, regime={}, exec_fill=miss)
    buys = [t for t in book["trades"] if t["side"] == "BUY"]
    assert not any(t["date"] == "2026-08-13" and t["ticker"] == "AAA" for t in buys)
    assert any(k["kind"] in ("fill_miss", "no_price") and k["ticker"] == "AAA"
               for k in book["skips"])
    assert book["daily"][0]["cash"] == 10000.0  # leftover stayed
    # Worse fill price cannot spend past leftover.
    def worse(**kw):
        intended = int(kw.get("intended_shares") or 0)
        return {"px": 50.0, "how": "worse", "shares": intended,
                "miss": False, "partial": False}
    book = fmb.simulate_book(
        panel, rec, bars=bars, fees=ZERO_FEES, regime={}, exec_fill=worse)
    aud = bfr.audit_reality_book(book)
    assert aud["ok"], aud["fails"]
    for t in book["trades"]:
        if t["side"] == "BUY":
            assert t["shares"] * t["price"] + t["fees"] <= 10000.0 + 0.05


def test_partial_leftover_and_never_sell_unheld() -> None:
    dates = ["2026-08-13", "2026-08-14"]
    rows = [_row("2026-08-13", "AAA"), _row("2026-08-14", "BBB")]
    panel = _panel(rows, dates)
    bars = {
        ("AAA", d): {"open": 10, "high": 10.4, "low": 9.6, "close": 10}
        for d in dates
    }
    bars.update({("BBB", d): {"open": 10, "high": 10.2, "low": 9.8, "close": 10}
                 for d in dates})
    rec = fm.make_recipe("union_h1", hold=1, top_n=1)
    tape = bfr.Tape(bars=bars)
    book = fmb.simulate_book(
        panel, rec, bars=bars, fees=ZERO_FEES, regime={},
        exec_fill=bfr.make_exec_fill("partial_50", tape))
    buy = next(t for t in book["trades"] if t["side"] == "BUY" and t["ticker"] == "AAA")
    ideal = fmb.simulate_book(panel, rec, bars=bars, fees=ZERO_FEES, regime={})
    ideal_buy = next(t for t in ideal["trades"]
                     if t["side"] == "BUY" and t["ticker"] == "AAA")
    assert buy["shares"] == max(1, ideal_buy["shares"] // 2)
    assert buy["shares"] < ideal_buy["shares"] or ideal_buy["shares"] == 1
    assert book["daily"][0]["cash"] > ideal["daily"][0]["cash"]  # leftover stayed
    aud = bfr.audit_reality_book(book)
    assert aud["ok"], aud["fails"]
    # Forced oversell is caught.
    fake = {
        "cash": 10000, "open": [],
        "trades": [{"date": "2026-08-13", "ticker": "ZZZ", "side": "SELL",
                    "shares": 3, "price": 10, "fees": 0, "cash_after": 10030}],
    }
    bad = bfr.audit_reality_book(fake)
    assert not bad["ok"]
    assert any("unheld" in f for f in bad["fails"])


def test_hard_red_sit_still_sits() -> None:
    dates = ["2026-08-13", "2026-08-14"]
    rows = [_row(d, "AAA") for d in dates]
    panel = _panel(rows, dates)
    bars = {("AAA", d): dict(BAR) for d in dates}
    rec = fm.make_recipe("union_h1", hold=1, top_n=1)
    tape = bfr.Tape(bars=bars)
    red = {"2026-08-13": {"predict_score": -6.2}}
    book = fmb.simulate_book(
        panel, rec, bars=bars, fees=ZERO_FEES, regime=red,
        exec_fill=bfr.make_exec_fill("market_favorable", tape))
    assert all(t["date"] != "2026-08-13" or t["side"] != "BUY"
               for t in book["trades"])
    assert any(k["kind"] == "hard_red" for k in book["skips"])


def test_ideal_hook_matches_default_book() -> None:
    dates = ["2026-08-13", "2026-08-14"]
    rows = [_row("2026-08-13", "AAA"), _row("2026-08-14", "AAA")]
    panel = _panel(rows, dates)
    bars = {("AAA", d): {"open": 10, "high": 10.5, "low": 9.5, "close": 10.2}
            for d in dates}
    rec = fm.make_recipe("union_h1", hold=1, top_n=1)
    a = fmb.simulate_book(panel, rec, bars=bars, fees=ZERO_FEES, regime={})
    tape = bfr.Tape(bars=bars)
    b = fmb.simulate_book(
        panel, rec, bars=bars, fees=ZERO_FEES, regime={},
        exec_fill=bfr.make_exec_fill("ideal", tape))
    assert a["total_ret_pct"] == b["total_ret_pct"]
    assert a["final_equity"] == b["final_equity"]
    assert [t["shares"] for t in a["trades"] if t["side"] == "BUY"] == [
        t["shares"] for t in b["trades"] if t["side"] == "BUY"]


def test_source_policy_research_only() -> None:
    src = (bfr.ROOT / "src" / "book_fill_reality.py").read_text(encoding="utf-8")
    assert "import yfinance" not in src
    assert "flatten_robust" in src
    assert "hard_red_sit" in src or "hard-red sit" in src
    assert "webull_paper" in src
    assert "research only" in src.lower() or "Research only" in src
    md = bfr.render_md({
        "headline": "test", "note": "n",
        "misread": "do not write 21/23 as a daily win rate",
        "proxy_note": "proxy", "ideal_note": "ideal",
        "favorable_note": "fav", "adverse_note": "adv",
        "from_date": "2026-08-13", "to_date": "2026-08-14",
        "n_sessions": 2, "sleeves": [], "reports": {},
        "realities": list(bfr.REALITIES), "not_yet_run": ["union_h9"],
        "elapsed_sec": 0, "starts_policy": "featured",
    })
    assert "flatten_robust" in md
    assert "21/23 as a daily win rate" in md or "not a daily win rate" in md
    html = bfr.render_html({
        "headline": "h", "note": "n", "misread": "Starts YES is not a daily win rate",
        "from_date": "2026-08-13", "to_date": "2026-08-14",
        "n_sessions": 2, "generated_at": "x", "sleeves": [],
        "reports": {}, "realities": list(bfr.REALITIES),
        "not_yet_run": [], "proxy_note": "p", "elapsed_sec": 0,
    })
    assert "flatten_robust" in html
    assert "Research only" in html


def test_empty_build_does_not_crash() -> None:
    payload = bfr.build(
        panel={"session_dates": [], "rows": [], "by_date": {}},
        store={}, bars={}, fees=ZERO_FEES, regime={},
        names=[], starts="none", time_budget=1)
    assert payload["live_wire"] is False
    assert "flatten_robust" in payload["live_untouched"]
    assert payload["reports"] == {}


def test_action_blotter_parse_has_must_run() -> None:
    names = bfr.blotter_names_from_action()
    if not names["all"]:
        return
    for n in ("combo_sh_5050_shared", "union_hot_n4_h1", "flatten_h3"):
        assert n in names["all"], n
    # Featured table must not silently treat Starts YES as Win%.
    text = bfr.ACTION_MD.read_text(encoding="utf-8") if bfr.ACTION_MD.exists() else ""
    if "combo_sh_5050_shared" in text:
        assert "Starts" in text or "starts" in text  # counts change with the data window
    md = bfr.render_md({
        "headline": "combo_sh_5050_shared",
        "misread": (
            "combo_sh_5050_shared Starts YES is wake-$10k-on-each-date "
            "replay, not a daily win rate."
        ),
        "note": "", "proxy_note": "", "ideal_note": "",
        "favorable_note": "", "adverse_note": "",
        "from_date": "2026-08-13", "to_date": "2026-09-15",
        "n_sessions": 23, "sleeves": ["combo_sh_5050_shared"],
        "reports": {"combo_sh_5050_shared": {"columns": {
            "ideal": {"book_pct": 41.54, "start_green": 21, "start_n": 23,
                      "win_session_pct": 61.0, "audit_ok": True,
                      "equity_daily": []},
            "market_mid": {"book_pct": 0, "equity_daily": []},
            "market_adverse": {"book_pct": -1, "equity_daily": []},
            "market_favorable": {"book_pct": 2, "equity_daily": []},
        }}},
        "realities": list(bfr.REALITIES), "not_yet_run": [],
        "elapsed_sec": 0, "starts_policy": "featured",
    })
    assert "not a daily win rate" in md.lower() or "not a daily" in md


def main() -> int:
    test_trade_through_limit_fill_or_miss()
    test_missing_bar_is_miss_never_close()
    test_market_extremes_stay_inside_bar()
    test_partial_50_and_gap_miss()
    test_miss_leaves_leftover_and_never_spends_past_cash()
    test_partial_leftover_and_never_sell_unheld()
    test_hard_red_sit_still_sits()
    test_ideal_hook_matches_default_book()
    test_source_policy_research_only()
    test_empty_build_does_not_crash()
    test_action_blotter_parse_has_must_run()
    print("test_book_fill_reality: 11 ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
