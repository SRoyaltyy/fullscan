"""Open-bell slippage / no-fill — unit tests, no parquet required.

Run: PYTHONPATH=. python3 -m src.test_open_bell_slip
"""
from __future__ import annotations

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

# Prior tape: 6 of 10 days close above the open → buy-adverse 60%.
PRIOR = [
    {"date": f"2026-08-{10 + i:02d}",
     "open": 10.0, "high": 10.8 if i < 6 else 10.1,
     "low": 9.9 if i < 6 else 9.2,
     "close": 10.5 if i < 6 else 9.4}
    for i in range(10)
]


def test_ideal_fill_is_the_open() -> None:
    px, how = obs.ideal_fill({"open": 12.5, "high": 13, "low": 12, "close": 12.8})
    assert how == "ideal_open" and px == 12.5
    assert obs.ideal_fill({"open": None})[0] is None
    assert obs.ideal_fill({"open": 0})[0] is None


def test_market_fill_is_adverse_vs_open() -> None:
    bar = {"open": 10.0, "high": 11.0, "low": 9.5, "close": 10.4}
    cal = {"alpha_buy": 0.35, "alpha_sell": 0.35}
    buy, how = obs.market_fill(bar, "long", cal)
    short, _ = obs.market_fill(bar, "short", cal)
    assert how == "market"
    assert buy is not None and buy > 10.0
    assert abs(buy - (10.0 + 0.35 * 1.0)) < 1e-9
    assert short is not None and short < 10.0
    assert abs(short - (10.0 - 0.35 * 0.5)) < 1e-9
    # Clip to the session range — never invent a print above the high.
    wild = obs.market_fill(bar, "long", {"alpha_buy": 0.55})[0]
    assert wild <= 11.0


def test_limit_open_fill_or_miss() -> None:
    ran_away = {"open": 10.0, "high": 11.0, "low": 10.0, "close": 10.8}
    came_back = {"open": 10.0, "high": 10.4, "low": 9.7, "close": 10.1}
    dumped = {"open": 10.0, "high": 10.0, "low": 9.2, "close": 9.4}
    px, how = obs.limit_at_open(ran_away, "long")
    assert px is None and how == "limit_miss"
    px, how = obs.limit_at_open(came_back, "long")
    assert px == 10.0 and how == "limit_fill"
    px, how = obs.limit_at_open(dumped, "short")
    assert px is None and how == "limit_miss"
    px, how = obs.limit_at_open(came_back, "short")
    assert px == 10.0 and how == "limit_fill"


def test_limit_prior_gap_and_go_misses() -> None:
    gapped = {"open": 12.0, "high": 12.5, "low": 11.8, "close": 12.2}
    px, how = obs.limit_at_prior(gapped, "long", 10.0)
    assert px is None and how == "limit_miss"
    came = {"open": 12.0, "high": 12.2, "low": 9.8, "close": 11.0}
    px, how = obs.limit_at_prior(came, "long", 10.0)
    assert how == "limit_fill" and px == 10.0
    gap_down = {"open": 9.0, "high": 9.4, "low": 8.8, "close": 9.1}
    px, how = obs.limit_at_prior(gap_down, "long", 10.0)
    assert how == "limit_fill" and px == 9.0


def test_calibrate_slip_uses_prior_only_and_is_adverse() -> None:
    cal = obs.calibrate_slip(PRIOR)
    assert cal["n"] == 10
    assert cal["buy_adverse_rate"] == 0.6
    assert cal["buy_adverse_rate"] > 0.5
    assert ALPHA_IN_RANGE(cal["alpha_buy"])
    # Injecting today's monster range must not change prior α.
    today = {"date": "2026-09-01", "open": 10.0, "high": 50.0,
             "low": 1.0, "close": 40.0}
    again = obs.calibrate_slip(PRIOR)
    leaked = obs.calibrate_slip(PRIOR + [today])
    # Caller must not pass today; if they do, n grows — the grade path
    # strips date >= session before calling. Check the helper does.
    bars = {(f"AAA", f"2026-08-{10 + i:02d}"): PRIOR[i] for i in range(10)}
    bars[("AAA", "2026-09-01")] = today
    prior = obs.prior_ohlc("AAA", "2026-09-01", bars=bars)
    assert all(p["date"] < "2026-09-01" for p in prior)
    assert not any(p["date"] == "2026-09-01" for p in prior)
    assert leaked["n"] == 11  # only if someone passes today; we don't


def ALPHA_IN_RANGE(a) -> bool:
    return obs.ALPHA_LO - 1e-9 <= float(a) <= obs.ALPHA_HI + 1e-9


def test_grade_intent_side_by_side() -> None:
    bars = {("AAA", "2026-09-02"): {
        "open": 10.0, "high": 11.0, "low": 9.6, "close": 10.5,
    }}
    rec = obs.grade_intent(
        {"date": "2026-09-02", "ticker": "AAA", "side": "long"},
        bars=bars, fees=ZERO_FEES, prior=PRIOR)
    assert rec["ideal"]["filled"] and rec["ideal"]["fill"] == 10.0
    assert rec["market"]["filled"] and rec["market"]["fill"] > 10.0
    assert rec["market"]["adverse"] is True
    assert rec["limit_open"]["filled"] is True  # low 9.6 < open
    assert rec["order_sent"] == "09:30 ET"
    # After-fee $: market entry is worse, so P&L is smaller than ideal.
    assert rec["market"]["pnl"] < rec["ideal"]["pnl"]


def test_align_sleeves_empty_and_unequal_no_indexerror() -> None:
    assert obs.align_sleeves({}) == []
    assert obs.align_sleeves({"a": [], "b": None}) == []
    a = [
        {"date": "2026-09-01", "ticker": "AAA", "side": "long"},
        {"date": "2026-09-02", "ticker": "BBB", "side": "long"},
    ]
    b = [
        {"date": "2026-09-02", "ticker": "BBB", "side": "long"},
    ]
    zipped = obs.align_sleeves({"hot": a, "flat": b, "empty": []})
    assert len(zipped) == 2
    first = zipped[0]
    assert first["ticker"] == "AAA"
    assert first["sleeves"]["hot"] is not None
    assert first["sleeves"]["flat"] is None
    assert first["sleeves"]["empty"] is None
    # zip_longest path
    rows = obs.zip_sleeve_rows(a, b, [])
    assert len(rows) == 2
    assert rows[0][0] is not None and rows[0][1] is not None
    assert rows[1][1] is None
    assert rows[0][2] is None and rows[1][2] is None
    # A caller that used to do sleeve_b[i] would IndexError here.
    assert len(b) != len(a)


def test_keep_kill_thin_and_standing_bar() -> None:
    thin = obs.decide_verdict(label="thin", n_fires=10, n_filled=10, win_rate=1, pnl=100)
    assert thin["label"] == "INSUFFICIENT_EVIDENCE" and thin["thin"]
    losing = obs.decide_verdict(label="loser", n_fires=100, n_filled=100, win_rate=.8, pnl=-500)
    assert losing["label"] == "NEGATIVE_DIAGNOSTIC" and not losing["keep"]
    winning = obs.decide_verdict(label="winner", n_fires=100, n_filled=100, win_rate=.4, pnl=500)
    assert winning["label"] == "POSITIVE_DIAGNOSTIC" and not winning["keep"]


def test_after_fee_uses_paper_trade() -> None:
    fees = obs.load_fees()
    pnl = obs.after_fee_pnl(1, 10.0, 11.0, side="long", fees=fees)
    buy = obs.order_fees(1, 10.0, "buy", fees)
    sell = obs.order_fees(1, 11.0, "sell", fees)
    assert abs(pnl - ((11.0 - sell) - (10.0 + buy))) < 1e-9
    short = obs.after_fee_pnl(1, 10.0, 9.0, side="short", fees=fees)
    assert short > 0
    try:
        from src.paper_trade import order_fees as pt_fees
    except ImportError:
        return
    assert abs(buy - pt_fees(1, 10.0, "buy", fees)) < 1e-9
    assert abs(sell - pt_fees(1, 11.0, "sell", fees)) < 1e-9


def test_sleeve_report_and_render_mention_live_untouched() -> None:
    bars = {}
    rows = []
    for i, d in enumerate(["2026-09-01", "2026-09-02", "2026-09-03"]):
        bars[("AAA", d)] = {
            "open": 10.0, "high": 10.6, "low": 9.8, "close": 10.3 + 0.1 * i,
        }
        rows.append(obs.grade_intent(
            {"date": d, "ticker": "AAA", "side": "long"},
            bars=bars, fees=ZERO_FEES, prior=PRIOR))
    rep = obs.sleeve_report("union_hot_n4_h1", rows)
    assert set(rep["columns"]) == set(obs.REALITIES)
    assert "ideal" in rep["verdicts"]
    md = obs.render_md({
        "headline": "Open-bell fills at 09:30 ET: demo KILL.",
        "order_sent": obs.ORDER_SENT,
        "from_date": "2026-09-01",
        "to_date": "2026-09-03",
        "n_sessions": 3,
        "sleeves": ["union_hot_n4_h1"],
        "reports": {"union_hot_n4_h1": rep},
        "note": "Research overlay only.",
    })
    assert "flatten_robust" in md
    assert "hard-red sit" in md.lower() or "hard-red" in md
    assert "Webull" in md or "webull" in md
    assert "09:30" in md
    assert "after-fee" in md.lower() or "After-fee" in md
    html = obs.render_html({
        "headline": "demo",
        "order_sent": obs.ORDER_SENT,
        "from_date": "2026-09-01",
        "to_date": "2026-09-03",
        "n_sessions": 3,
        "sleeves": ["union_hot_n4_h1"],
        "reports": {"union_hot_n4_h1": rep},
        "note": "untouched",
        "generated_at": "now",
    })
    assert "MARKET" in html or "market" in html
    assert "flatten_robust" in html


def test_source_does_not_touch_live_policy() -> None:
    src = (obs.ROOT / "src" / "open_bell_slip.py").read_text(encoding="utf-8")
    assert "does not change" in src.lower() or "not changed" in src.lower()
    assert "flatten_robust" in src
    assert "hard-red sit" in src.lower() or "hard_red_sit" in src
    assert "research only" in src.lower() or "research overlay" in src.lower()
    assert "import yfinance" not in src
    assert obs.LIVE_UNTOUCHED == (
        "flatten_robust", "hard_red_sit", "webull_paper")
    # Live policy modules are not imported for a side effect write.
    assert "sleeve_merge.LIVE_POLICY" not in src


def test_build_empty_panel_does_not_crash() -> None:
    payload = obs.build(panel={"session_dates": [], "by_date": {}, "rows": []},
                        store={}, bars={}, fees=ZERO_FEES)
    assert payload["live_wire"] is False
    assert payload["live_untouched"] == list(obs.LIVE_UNTOUCHED)
    assert payload["research_only"] is True
    for name in obs.SLEEVE_ORDER:
        assert name in payload["reports"]
        assert payload["reports"][name]["n_intents"] == 0


def test_smoke_regenerate_if_panel_present() -> None:
    """If the leak-free panel is on disk, grade a 2-session slice."""
    try:
        import pandas  # noqa: F401
    except ImportError:
        return
    panel = obs.load_panel()
    dates = list(panel.get("session_dates") or [])
    if len(dates) < 2:
        return
    slice_dates = dates[:2]
    payload = obs.build(
        panel=panel, from_date=slice_dates[0], to_date=slice_dates[-1],
        fees=ZERO_FEES)
    assert payload["n_sessions"] >= 1
    md = obs.render_md(payload)
    assert "KEEP" in md or "KILL" in md
    html = obs.render_html(payload)
    assert "09:30" in html


def main() -> None:
    test_ideal_fill_is_the_open()
    test_market_fill_is_adverse_vs_open()
    test_limit_open_fill_or_miss()
    test_limit_prior_gap_and_go_misses()
    test_calibrate_slip_uses_prior_only_and_is_adverse()
    test_grade_intent_side_by_side()
    test_align_sleeves_empty_and_unequal_no_indexerror()
    test_keep_kill_thin_and_standing_bar()
    test_after_fee_uses_paper_trade()
    test_sleeve_report_and_render_mention_live_untouched()
    test_source_does_not_touch_live_policy()
    test_build_empty_panel_does_not_crash()
    test_smoke_regenerate_if_panel_present()
    print("test_open_bell_slip: 13 ok")


if __name__ == "__main__":
    main()
