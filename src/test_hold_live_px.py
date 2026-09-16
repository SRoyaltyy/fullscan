"""Open-lot Elite marks. No network.

Run: PYTHONPATH=. python3 -m src.test_hold_live_px
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

from src import elite_live_px as elp
from src import hold_live_px as hlp


PACK = {
    "featured": ["combo_sh_5050_shared", "union_hot_n4_h1"],
    "recipes": [
        {"name": "combo_sh_5050_shared", "side": "mixed"},
        {"name": "union_hot_n4_h1", "side": "long"},
    ],
    "books": {
        "combo_sh_5050_shared": {
            "open": [
                {"ticker": "INDP", "shares": 633, "entry_px": 2.7,
                 "entry_date": "2026-09-11", "side": "long"},
                {"ticker": "QRVO", "shares": 15, "entry_px": 112.835,
                 "entry_date": "2026-09-11", "side": "short"},
            ],
        },
        "union_hot_n4_h1": {
            "open": [
                {"ticker": "INDP", "shares": 1087, "entry_px": 2.7,
                 "entry_date": "2026-09-11"},
            ],
        },
    },
    "daily": {
        "combo_sh_5050_shared": [
            {"date": "2026-09-15", "hard_red": True,
             "lots": [
                 {"ticker": "INDP", "shares": 633, "entry_px": 2.7},
                 {"ticker": "QRVO", "shares": 15, "entry_px": 112.835},
                 {"ticker": "GONE", "shares": 10, "entry_px": 1.0},
             ],
             "held": ["INDP", "QRVO", "GONE"]},
        ],
    },
}


def test_open_lots_are_book_open_not_looker_or_closed() -> None:
    lots = hlp.open_lots_for(PACK, "combo_sh_5050_shared")
    ticks = [x["ticker"] for x in lots]
    assert ticks == ["INDP", "QRVO"]
    assert "GONE" not in ticks  # closed / not in open book
    assert "BBNX" not in ticks  # looker name never held
    by = {x["ticker"]: x for x in lots}
    assert by["INDP"]["side"] == "long"
    assert by["INDP"]["shares"] == 633
    assert by["INDP"]["entry"] == 2.7
    assert by["QRVO"]["side"] == "short"


def test_featured_pins_combo_sh() -> None:
    names = hlp.featured_names(PACK)
    assert names[0] == "combo_sh_5050_shared"
    assert "union_hot_n4_h1" in names


def test_stamp_lot_pnl_long_and_short() -> None:
    book = {
        "prices": {"INDP": 3.7, "QRVO": 119.06},
        "src": "session_export",
        "at": "2026-09-16T12:31:00-04:00",
    }
    opens = {"INDP": 3.71, "QRVO": 118.0}
    long = hlp.stamp_lot(
        {"ticker": "INDP", "side": "long", "shares": 633, "entry": 2.7},
        book, opens)
    short = hlp.stamp_lot(
        {"ticker": "QRVO", "side": "short", "shares": 15, "entry": 112.835},
        book, opens)
    assert long["px"] == 3.7
    assert long["open_px"] == 3.71
    assert long["px_src"] == "session_export"
    assert long["pnl"] == round((3.7 - 2.7) * 633, 2)
    assert short["pnl"] == round((112.835 - 119.06) * 15, 2)
    assert short["vs_entry_pct"] < 0  # short and price up = loss


def test_banner_never_says_live_on_session_export() -> None:
    et = ZoneInfo("America/New_York")
    book = {
        "src": "session_export+no_elite_auth",
        "at": datetime(2026, 9, 16, 12, 31, tzinfo=et).isoformat(),
    }
    text = hlp.banner_text(book)
    assert "Elite export as of 12:31 ET (not live)" == text
    assert "Overview" not in text
    live = hlp.banner_text({
        "src": "elite_live",
        "at": datetime(2026, 9, 16, 10, 5, tzinfo=et).isoformat(),
    })
    assert live == "Elite Overview as of 10:05 ET"
    assert hlp.is_live_src("session_export") is False
    assert hlp.is_live_src("preopen_export") is False
    assert hlp.is_live_src("theme_radar_close") is False
    assert hlp.is_live_src("elite_live") is True


def test_build_uses_quote_book_and_skips_looker() -> None:
    qbook = {
        "prices": {"INDP": 3.7, "QRVO": 119.06, "BBNX": 18.2},
        "src": "session_export",
        "at": "2026-09-16T12:31:00-04:00",
        "after_open": True,
        "n": 3,
        "error": "no_elite_auth",
        "clock_rule": "px is Finviz Elite Price",
    }
    with mock.patch.object(hlp.elp, "quote_book", return_value=qbook), \
            mock.patch.object(hlp.elp, "fallback_opens",
                              return_value={"INDP": 3.71, "QRVO": 118.0}), \
            mock.patch.object(hlp.elp, "official_opens", return_value={}):
        out = hlp.build("2026-09-16", pack=PACK, pull_live=False)
    assert out["looker"] is False
    assert out["quote"]["live"] is False
    assert "not live" in out["banner"]
    lots = out["sleeves"]["combo_sh_5050_shared"]["lots"]
    ticks = [x["ticker"] for x in lots]
    assert ticks == ["INDP", "QRVO"]
    assert "BBNX" not in ticks
    indp = lots[0]
    assert indp["px"] == 3.7
    assert indp["open_px"] == 3.71
    assert indp["shares"] == 633


def test_parse_elite_open_column_not_price() -> None:
    text = "Ticker,Price,Open,Gap\nINDP,3.70,3.71,2.5\n"
    opens = elp.parse_elite_open_csv(text)
    assert opens["INDP"] == 3.71
    prices = elp.parse_elite_price_csv(text)
    assert prices["INDP"] == 3.70


def test_theme_radar_still_refused() -> None:
    assert elp._is_theme_radar(Path("data/exports/theme_radar_close.csv"))


def main() -> None:
    test_open_lots_are_book_open_not_looker_or_closed()
    test_featured_pins_combo_sh()
    test_stamp_lot_pnl_long_and_short()
    test_banner_never_says_live_on_session_export()
    test_build_uses_quote_book_and_skips_looker()
    test_parse_elite_open_column_not_price()
    test_theme_radar_still_refused()
    print("ok")


if __name__ == "__main__":
    main()
