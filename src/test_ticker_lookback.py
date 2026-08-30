"""Ticker-first lookback regression tests against committed artifacts."""
from __future__ import annotations

import tempfile
from pathlib import Path

from openpyxl import load_workbook

from src import ticker_lookback as tl
from src import ticker_lookback_run as run


def test_enriched_ab_dates_are_indexed() -> None:
    assert "2026-08-24" in tl.session_dates()
    idx = tl.build_index()
    sess = next(x for x in idx["sessions"] if x["date"] == "2026-08-24")
    assert sess["n_ab"] > 1000


def test_session_dates_skip_weekends() -> None:
    dates = tl.session_dates()
    assert dates
    assert "2026-08-29" not in dates  # Saturday dump
    assert "2026-08-30" not in dates  # Sunday dump
    assert "2026-04-26" not in dates  # Sunday dump
    from datetime import datetime
    for d in dates:
        assert datetime.strptime(d, "%Y-%m-%d").weekday() < 5


def test_any_finviz_name_gets_cards_without_book() -> None:
    payload = run.scan_tickers(
        ["AAPL"], from_date="2026-08-24", to_date="2026-08-25")
    rec = payload["names"][0]
    assert rec["n_sessions"] == 2
    assert rec["n_with_print"] == 2
    assert all("finviz" in d["sources"] for d in rec["days"])
    assert all(len(d.get("finviz_factors") or {}) >= 10 for d in rec["days"])
    assert all(len(d.get("ab_factors") or {}) >= 10 for d in rec["days"])


def test_phone_html_and_returns() -> None:
    payload = run.scan_tickers(
        ["AAPL"], from_date="2026-08-19", to_date="2026-08-20")
    page = run.render_html(payload)
    assert 'name="viewport"' in page
    assert "🟢 up / positive" in page
    assert "<th>+1d</th><th>+3d</th><th>+1w</th><th>Cond</th>" in page
    assert "AAPL" in page
    day0 = payload["names"][0]["days"][0]
    assert day0["forward_returns"]["1d"] is not None
    changes = day0["price_changes"]
    assert changes["price"] is not None
    assert changes["1d"] is not None
    assert changes["1d"] == day0["forward_returns"]["1d"]
    panel = tl._price_panel()
    t = panel["AAPL"]
    i = panel.index.searchsorted(__import__("pandas").Timestamp("2026-08-19"))
    expected = round(100 * (float(t.iloc[i + 1]) / float(t.iloc[i]) - 1), 3)
    assert changes["1d"] == expected
    tones = payload["names"][0]["days"][0]["price_tones"]
    assert tones["1d"] in {"good", "neutral", "bad"}
    assert 'td class="' in page
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "lookback.xlsx"
        run.write_xlsx(payload, p)
        wb = load_workbook(p)
        assert "AAPL" in wb.sheetnames
        assert wb["AAPL"]["C2"].value is not None
        assert wb["AAPL"]["C2"].fill.fgColor.rgb[-6:] in {
            "63BE7B", "FFEB84", "F8696B", "808080"}


def test_random_universe_gates() -> None:
    uni = tl.liquid_universe()
    assert len(uni) >= 10
    path = tl.latest_finviz_path()
    import pandas as pd
    df = pd.read_csv(path, usecols=["Ticker", "Market Cap", "Average Volume"])
    row = df[df["Ticker"].astype(str).str.upper() == uni[0]].iloc[0]
    assert float(row["Market Cap"]) > 100
    assert float(row["Average Volume"]) > 500
    a = tl.pick_random_tickers(n=10, seed=7)
    b = tl.pick_random_tickers(n=10, seed=7)
    c = tl.pick_random_tickers(n=10, seed=8)
    assert a == b
    assert len(a) == 10
    assert len(set(a)) == 10
    assert a != c
    names, flag = run.resolve_tickers("random", seed=7)
    assert flag is True
    assert names == a


def test_price_tones() -> None:
    assert tl.price_tone(1.2) == "good"
    assert tl.price_tone(-1.2) == "bad"
    assert tl.price_tone(0.1) == "neutral"
    assert tl.price_tone(None) == "missing"


def test_signal_improved_is_strict() -> None:
    worse = {"join": "neutral", "ab": "good"}
    next_worse = {"join": "bad", "ab": "good"}
    assert tl.objectively_better(worse, next_worse) is False

    same = {"join": "neutral", "ab": "good"}
    assert tl.objectively_better(same, same) is False

    better = {"join": "good", "ab": "good"}
    assert tl.objectively_better(worse, better) is True

    # missing on either side is ignored, not treated as a downgrade
    assert tl.objectively_better(
        {"join": "neutral", "ab": "missing"},
        {"join": "good", "ab": "missing"},
    ) is True

    days = [
        {"date": "2026-08-19", "boxes": {"join": "neutral", "ab": "neutral"}},
        {"date": "2026-08-20", "boxes": {"join": "good", "ab": "neutral"}},
        {"date": "2026-08-21", "boxes": {"join": "good", "ab": "bad"}},
    ]
    tl.annotate_signal_improved(days)
    assert days[0]["signal_improved"] is False
    assert days[0]["zero_red"] is True
    assert days[1]["signal_improved"] is True
    assert days[1]["zero_red"] is True
    assert days[2]["signal_improved"] is False
    assert days[2]["signal_alarm"] is True
    assert days[2]["zero_red"] is False
    assert days[2]["condition"]["tone"] in {"good", "neutral", "bad"}

    payload = {
        "generated_at": "t",
        "names": [{"ticker": "TEST", "days": days}],
    }
    page = run.render_html(payload)
    assert 'th class="better clean">🔵⚪ 2026-08-20</th>' in page
    assert "th class=\"clean\">⚪ 2026-08-19</th>" in page
    assert "🚨 2026-08-21" in page
    assert "+≥3 pts" in page
    assert "purely worse" in page
    md = run.render_md(payload)
    assert "🔵⚪ 2026-08-20" in md
    assert "⚪ 2026-08-19" in md
    assert "🚨 2026-08-21" in md
    assert "| Cond |" in md
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "lookback.xlsx"
        run.write_xlsx(payload, p)
        wb = load_workbook(p)
        assert wb["TEST"]["A3"].fill.fgColor.rgb[-6:] == "5B9BD5"
        assert wb["TEST"]["A2"].fill.fgColor.rgb[-6:] == "FFFFFF"


def test_blue_on_point_jump_and_zero_red() -> None:
    assert tl.box_points({"join": "bad", "ab": "neutral", "gen": "good"}) == 6
    assert tl.zero_red({"join": "neutral", "ab": "good"}) is True
    assert tl.zero_red({"join": "bad", "ab": "good"}) is False
    assert tl.zero_red({"join": "missing"}) is False

    # One cell worse (ab yellow→red) but net points +4 → still blue.
    days = [
        {"date": "2026-08-19", "boxes": {
            "join": "bad", "sector": "bad", "gen": "bad", "ab": "neutral"}},
        {"date": "2026-08-20", "boxes": {
            "join": "good", "sector": "good", "gen": "neutral", "ab": "bad"}},
    ]
    assert tl.objectively_better(days[0]["boxes"], days[1]["boxes"]) is False
    assert tl.point_delta(days[0]["boxes"], days[1]["boxes"]) >= 3
    tl.annotate_signal_improved(days)
    assert days[1]["signal_improved"] is True
    assert days[1]["signal_alarm"] is False
    assert days[1]["zero_red"] is False


def test_alarm_and_condition_majority() -> None:
    assert tl.purely_worse(
        {"join": "good", "ab": "neutral"},
        {"join": "neutral", "ab": "bad"},
    ) is True
    assert tl.purely_worse(
        {"join": "good", "ab": "neutral"},
        {"join": "good", "ab": "good"},
    ) is False
    # Mixed: one better, one worse — not purely worse.
    assert tl.purely_worse(
        {"join": "neutral", "ab": "good"},
        {"join": "good", "ab": "bad"},
    ) is False

    green_major = {k: "good" for k, _ in tl.BOX_COLS}
    red_major = {k: "bad" for k, _ in tl.BOX_COLS}
    mixed = {k: "neutral" for k, _ in tl.BOX_COLS}
    mixed["join"] = mixed["ab"] = mixed["peer"] = "good"
    mixed["news"] = mixed["vol"] = "bad"
    assert tl.general_condition(green_major)["tone"] == "good"
    assert tl.general_condition(red_major)["tone"] == "bad"
    assert tl.general_condition(mixed)["tone"] == "neutral"
    assert tl.general_condition({})["tone"] == "missing"

    days = [
        {"date": "2026-08-19", "boxes": {"join": "good", "ab": "good", "peer": "neutral"}},
        {"date": "2026-08-20", "boxes": {"join": "neutral", "ab": "bad", "peer": "neutral"}},
    ]
    tl.annotate_signal_improved(days)
    assert days[1]["signal_alarm"] is True
    assert days[1]["signal_improved"] is False
    payload = {
        "generated_at": "t",
        "names": [{"ticker": "TEST", "days": days}],
    }
    page = run.render_html(payload)
    assert "🚨 2026-08-20" in page
    assert ">0/2/1<" in page


if __name__ == "__main__":
    test_enriched_ab_dates_are_indexed()
    test_session_dates_skip_weekends()
    test_any_finviz_name_gets_cards_without_book()
    test_phone_html_and_returns()
    test_random_universe_gates()
    test_price_tones()
    test_signal_improved_is_strict()
    test_blue_on_point_jump_and_zero_red()
    test_alarm_and_condition_majority()
    print("9 tests passed")
