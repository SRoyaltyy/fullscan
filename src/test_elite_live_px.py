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
    with mock.patch.object(st, "attach_hard_red_research", wraps=st.attach_hard_red_research):
        out = st.attach_hard_red_research(payload, "2026-09-14")
    rec = out["strategies"]["combo_sh_macd_5050_shared"]
    res = rec["research"]
    assert res["tag"] == "RESEARCH"
    assert res["live_sit"] is True
    assert any(x["ticker"] == "BKV" for x in res["short_only"])
    assert any(x["ticker"] == "INDP" for x in res["dip_scoop"])
    assert "not a wire" in res["note"]


def main() -> None:
    test_parse_elite_overview_prices()
    test_theme_radar_close_is_never_live()
    test_after_open_clock()
    test_stamp_rows_keeps_open_and_live()
    test_quote_book_offline_uses_fallback_not_theme_radar()
    test_excel_clock_is_session_open_not_run_date()
    test_slim_board_keeps_live_px()
    test_hard_red_research_is_tagged_not_a_wire()
    print("ok")


if __name__ == "__main__":
    main()
