"""Elite live marks for open-pack tickets. No network.

Run: PYTHONPATH=. python3 -m src.test_elite_live_px
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from unittest import mock

from src import elite_live_px as elp
from src import strategy_tickets as st


def test_parse_elite_overview_prices() -> None:
    text = "Ticker,Company,Price\nINDP,Indaptus,2.89\nBKV,BKV,24.31\n"
    got = elp.parse_elite_price_csv(text)
    assert got["INDP"] == 2.89
    assert got["BKV"] == 24.31


def test_theme_radar_close_is_never_live() -> None:
    assert elp._is_theme_radar(Path("01_daily/news/2026-09-14_finviz_market_digest_close.json"))
    assert elp._is_theme_radar(Path("data/exports/theme_radar_close.csv"))
    assert not elp._is_theme_radar(Path("data/exports/finviz_2026-09-14.csv"))


def test_after_open_clock() -> None:
    et = ZoneInfo("America/New_York")
    assert elp.after_open(datetime(2026, 9, 14, 9, 29, tzinfo=et)) is False
    assert elp.after_open(datetime(2026, 9, 14, 9, 30, tzinfo=et)) is True
    assert elp.after_open(datetime(2026, 9, 14, 16, 30, tzinfo=et)) is True


def test_quote_is_elite_live() -> None:
    assert elp.quote_is_elite_live(
        {"src": "elite_live", "after_open": True}) is True
    assert elp.quote_is_elite_live(
        {"src": "elite_live_file", "after_open": True}) is True
    assert elp.quote_is_elite_live({
        "src": "session_export+finviz_session: No module named 'requests'",
        "after_open": True,
    }) is False
    assert elp.quote_is_elite_live(
        {"src": "elite_live", "after_open": False}) is False
    assert elp.quote_is_elite_live({}) is False


def test_stamp_rows_keeps_open_and_live() -> None:
    book = {
        "prices": {"INDP": 2.89},
        "src": "elite_live",
        "at": "2026-09-14T09:35:00-04:00",
    }
    rows = elp.stamp_rows(
        [{"ticker": "INDP", "kid_side": "long"}],
        book, opens={"INDP": 2.80},
    )
    assert rows[0]["px"] == 2.89
    assert rows[0]["px_src"] == "elite_live"
    assert rows[0]["open_px"] == 2.80


def test_quote_book_offline_uses_fallback_not_theme_radar() -> None:
    import tempfile
    from unittest import mock
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        exports = tmp / "exports"
        exports.mkdir()
        (exports / "finviz_2026-09-14.csv").write_text(
            "Ticker,Price\nINDP,2.80\n", encoding="utf-8")
        with mock.patch.object(elp, "EXPORTS", exports), \
                mock.patch.object(elp, "FINVIZ", tmp / "finviz"), \
                mock.patch.object(elp, "LIVE_CSV", exports / "finviz_live.csv"):
            book = elp.quote_book("2026-09-14", pull_live=False)
        assert book["prices"]["INDP"] == 2.80
        assert book["src"] == "session_export"


def test_after_bell_dated_export_is_elite_live_file() -> None:
    import os
    import tempfile
    et = ZoneInfo("America/New_York")
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        exports = tmp / "exports"
        exports.mkdir()
        path = exports / "finviz_2026-09-15.csv"
        path.write_text("Ticker,Price\nAVAH,15.10\n", encoding="utf-8")
        os.utime(path, (datetime(2026, 9, 15, 9, 49, tzinfo=et).timestamp(),) * 2)
        with mock.patch.object(elp, "EXPORTS", exports), \
                mock.patch.object(elp, "FINVIZ", tmp / "finviz"), \
                mock.patch.object(elp, "LIVE_CSV", exports / "finviz_live.csv"):
            book = elp.quote_book("2026-09-15", pull_live=False)
        assert book["src"] == "elite_live_file"
        assert book["prices"]["AVAH"] == 15.10


def test_excel_clock_is_session_open_not_run_date() -> None:
    import tempfile
    from unittest import mock
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        sug = tmp / "excel_bot" / "suggestions"
        sug.mkdir(parents=True)
        (sug / "suggestions.csv").write_text(
            "run_date,signal_date,ticker,side,strategy\n"
            "2026-07-28,2026-09-14,AEP,LONG,L1_long_green_tp8_lowvol\n"
            "2026-07-28,2026-09-11,ALLE,LONG,L1_long_green_tp8_lowvol\n",
            encoding="utf-8",
        )
        with mock.patch.object(st, "ROOT", tmp):
            rows = st.excel_strats("2026-09-14")
        by = {r["name"]: r for r in rows}
        assert by["excel_all"]["clock_legal_for"] == "2026-09-14"
        assert by["excel_all"]["session_open"] == "2026-09-14"
        assert by["excel_L1_long_green_tp8_lowvol"]["clock_legal_for"] == "2026-09-14"
        names = {x["ticker"] for x in by["excel_all"]["buy"]}
        assert names == {"AEP"}
        assert "ALLE" not in names
        assert by["excel_all"]["signal_date"] == "2026-09-14"


def test_slim_board_keeps_live_px() -> None:
    rows = st._board_quote_rows([
        {"ticker": "CVE", "px": 16.2, "open_px": 16.0, "px_src": "elite_live",
         "side": "long", "predict": "UP"},
        "TNDM",
    ])
    assert rows[0]["ticker"] == "CVE"
    assert rows[0]["px"] == 16.2
    assert rows[0]["open_px"] == 16.0
    assert rows[0]["px_src"] == "elite_live"
    assert rows[1] == {"ticker": "TNDM"}


def test_hard_red_research_is_tagged_not_a_wire() -> None:
    payload = {
        "quote": {"src": "elite_live"},
        "strategies": {
            "combo_sh_macd_5050_shared": {
                "sit": True, "hard_red": True,
                "buy": [
                    {"ticker": "INDP", "kid_side": "long", "px": 2.89, "open_px": 2.80},
                    {"ticker": "BKV", "kid_side": "short", "px": 24.31, "open_px": 24.26},
                ],
                "sell": [],
            }
        },
    }
    with mock.patch("src.hard_red_sit_research.clock_bar", side_effect=lambda t, d, bars=None: {
        "INDP": {"open": 2.80, "low": 2.77, "close": 2.89},
        "BKV": {"open": 24.26, "low": 24.08, "close": 24.31},
    }.get(t, {})):
        out = st.attach_hard_red_research(payload, "2026-09-14")
    rec = out["strategies"]["combo_sh_macd_5050_shared"]
    res = rec["research"]
    assert res["tag"] == "RESEARCH"
    assert res["live_sit"] is True
    assert res["keep_bar_unchanged"] is True
    assert any(x["ticker"] == "BKV" for x in res["short_only"])
    assert any(x["ticker"] == "INDP" for x in res["dip_scoop"])
    assert "not a wire" in res["note"]
    indp = next(x for x in res["dip_scoop"] if x["ticker"] == "INDP")
    assert (indp["scoops"]["0.5"]["kind"] == "scoop")
    assert (indp["scoops"]["3.0"]["kind"] == "no_dip")


def test_research_uses_scoreboard_open_when_parquet_missing() -> None:
    payload = {
        "quote": {"src": "session_export"},
        "strategies": {
            "combo_sh_macd_5050_shared": {
                "sit": True, "hard_red": True,
                "buy": [{"ticker": "INDP", "kid_side": "long", "px": 2.89}],
                "sell": [],
            }
        },
    }
    with mock.patch("src.hard_red_sit_research.clock_bar", return_value={}), \
            mock.patch("src.hard_red_sit_research.scoreboard_bars", return_value={
                "INDP": {"open": 2.80, "low": 2.77},
            }):
        out = st.attach_hard_red_research(payload, "2026-09-14")
    indp = out["strategies"]["combo_sh_macd_5050_shared"]["research"]["dip_scoop"][0]
    assert indp["open"] == 2.80
    assert indp["scoops"]["0.5"]["kind"] == "scoop"
    assert indp["scoops"]["3.0"]["kind"] == "no_dip"


def test_research_scoop_ignores_close_and_stale_last() -> None:
    payload = {
        "quote": {"src": "session_export"},
        "strategies": {
            "union_hot_n4_h1": {
                "sit": True, "hard_red": True, "side": "long",
                "buy": [{"ticker": "HOT1", "kid_side": "long",
                         "px": 90.0, "open_px": 100.0,
                         "px_src": "session_export"}],
                "sell": [{"ticker": "SH1", "side": "short",
                          "px": 8.0, "open_px": 8.0}],
            }
        },
    }
    with mock.patch("src.hard_red_sit_research.clock_bar", side_effect=lambda t, d, bars=None: {
        "HOT1": {"open": 100.0, "low": 99.2, "close": 90.0},
        "SH1": {"open": 8.0, "low": 7.9, "close": 7.5},
    }.get(t, {})):
        out = st.attach_hard_red_research(payload, "2026-09-14")
    res = out["strategies"]["union_hot_n4_h1"]["research"]
    hot = next(x for x in res["dip_scoop"] if x["ticker"] == "HOT1")
    assert hot["scoops"]["0.5"]["kind"] == "scoop"
    assert hot["scoops"]["1.0"]["kind"] == "no_dip"
    assert any(x["ticker"] == "SH1" for x in res["short_only"])


def main() -> None:
    test_parse_elite_overview_prices()
    test_theme_radar_close_is_never_live()
    test_after_open_clock()
    test_quote_is_elite_live()
    test_stamp_rows_keeps_open_and_live()
    test_quote_book_offline_uses_fallback_not_theme_radar()
    test_after_bell_dated_export_is_elite_live_file()
    test_excel_clock_is_session_open_not_run_date()
    test_slim_board_keeps_live_px()
    test_hard_red_research_is_tagged_not_a_wire()
    test_research_uses_scoreboard_open_when_parquet_missing()
    test_research_scoop_ignores_close_and_stale_last()
    print("ok")


if __name__ == "__main__":
    main()
