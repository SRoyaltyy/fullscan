"""Fail-closed integration for post-close research → stock book.

Run: python -m src.test_map_heat_integration
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import map_heat as mh
from src import map_heat_research as research
from src import output_qc
from src import stock_book


def test_macro_gate_halves_book_earnings_does_not() -> None:
    econ = [{"event": "PCE", "importance": 3, "actual": None, "forecast": None}]
    earns = [{"ticker": "NVDA", "mcap": 4_000_000}]
    g = mh._calendar_fields(econ, earns)
    assert g["macro_gate"] is True
    assert g["earnings_gate"] is True
    assert g["size_gate"] is True
    assert g["calendar_entry_scale"] == 0.5
    assert "NVDA" in g["earnings_entry_tickers"]

    g2 = mh._calendar_fields([], earns)
    assert g2["macro_gate"] is False
    assert g2["earnings_gate"] is True
    assert g2["size_gate"] is False
    assert g2["calendar_entry_scale"] == 1.0
    assert g2["earnings_entry_tickers"] == ["NVDA"]


def test_missing_research_is_visible_but_bootstrap_is_safe() -> None:
    missing = output_qc.qc_map_heat_research(
        "01_daily/map_heat/1999-01-01_research.md")
    assert not missing.ok
    assert missing.kind == "map_heat_research"
    base = output_qc.qc_map_heat_baseline(
        "01_daily/map_heat/1999-01-01_research_baseline.json")
    assert not base.ok
    assert "postclose_baseline_missing" in base.reason
    heat = output_qc.qc_map_heat(
        "01_daily/map_heat/1999-01-01_map_heat.json")
    assert not heat.ok
    with tempfile.TemporaryDirectory() as d:
        md = Path(d) / "2099-01-01_research.md"
        js = Path(d) / "2099-01-01_research.json"
        md.write_text(
            "# MAP HEAT RESEARCH — 2099-01-01 (BOOTSTRAP)\n\n"
            "No post-close baseline; no heat applied.\n\n"
            "CAPTAIN_CARDS_OK\nOPPORTUNITY_OK\n"
        )
        js.write_text(json.dumps({
            "date": "2099-01-01", "phase": "morning_bootstrap",
            "cards": [], "calendar_entry_scale": 1.0,
        }))
        safe = output_qc.qc_map_heat_research(md)
        assert safe.ok


def test_calendar_entry_scale_ignores_legacy_earnings_mix() -> None:
    orig = research.OUT_DIR
    with tempfile.TemporaryDirectory() as d:
        research.OUT_DIR = Path(d)
        js = Path(d) / "2099-01-01_research.json"
        js.write_text(json.dumps({
            "phase": "morning_refresh",
            "macro_gate": False,
            "calendar_entry_scale": 0.5,
            "earnings_entry_tickers": ["NVDA"],
        }))
        assert research.calendar_entry_scale("2099-01-01") == 1.0
        assert research.earnings_entry_tickers("2099-01-01") == ["NVDA"]
        js.write_text(json.dumps({
            "phase": "morning_refresh",
            "macro_gate": True,
            "calendar_entry_scale": 0.5,
        }))
        assert research.calendar_entry_scale("2099-01-01") == 0.5
        js.write_text(json.dumps({
            "phase": "postclose_baseline", "macro_gate": True,
        }))
        assert research.calendar_entry_scale("2099-01-01") == 1.0
        assert research.earnings_entry_tickers("2099-01-01") == []
    research.OUT_DIR = orig


def test_empty_tape_is_not_overlay_good() -> None:
    empty = {
        "date": "2099-01-01",
        "phase": "morning_overlay",
        "overlay_at": "2099-01-01T06:32:00-04:00",
        "tape": [],
        "industries": [{}] * 60,
    }
    assert mh.overlay_is_good(empty, "2099-01-01") is False
    filled = dict(empty)
    filled["tape"] = [{"ticker": "ES", "last": 6500, "change": 0.1}]
    assert mh.overlay_is_good(filled, "2099-01-01") is True
    assert mh.overlay_is_good(filled, "2099-01-02") is False
    assert mh.overlay_is_good(None, "2099-01-01") is False


def test_heat_scale_default_is_incubate() -> None:
    _, meta = stock_book.load_policy()
    assert float(meta["heat_scale"]) <= 0.50


def test_weekend_upcoming_econ_does_not_trip_size_gate() -> None:
    econ = [
        {"event": "NFP", "importance": 3, "datetime": "2026-09-04T08:30:00"},
        {"event": "ISM", "importance": 3, "datetime": "2026-09-01T10:00:00"},
    ]
    earns = [{"ticker": "AVGO", "mcap": 1_700_000, "datetime": "2026-09-02T16:30:00"}]
    g = mh._calendar_fields(econ, earns, "2026-08-29")
    assert g["macro_gate"] is False
    assert g["size_gate"] is False
    assert g["calendar_entry_scale"] == 1.0
    assert g["earnings_gate"] is False
    assert g["econ"][0]["event"] == "NFP" or g["econ"][0]["event"] == "ISM"
    assert len(g["econ"]) == 2
    assert g["earnings_entry_tickers"] == []

    g_today = mh._calendar_fields(econ, earns, "2026-09-04")
    assert g_today["macro_gate"] is True
    assert g_today["calendar_entry_scale"] == 0.5


def test_parse_econ_route_init_keeps_upcoming() -> None:
    html = """
    <script id="route-init-data" type="application/json">
    {"data":{"initialDateFrom":"2026-08-31","entries":[
      {"event":"Non Farm Payrolls","date":"2026-09-04T08:30:00",
       "actual":null,"previous":"-23K","forecast":"45K","importance":3,"category":"Employment"},
      {"event":"Old print","date":"2026-08-21T08:30:00",
       "actual":"1","previous":"1","forecast":"1","importance":3,"category":"X"}
    ]}}
    </script>
    """
    rows = mh._parse_econ_html(html, "2026-08-29")
    names = [r["event"] for r in rows]
    assert "Non Farm Payrolls" in names
    assert "Old print" not in names
    assert rows[0]["importance"] == 3
    assert rows[0]["forecast"] == "45K"


def test_parse_earnings_preview_window() -> None:
    html = """
    <script id="route-init-data" type="application/json">
    {"data":{"entries":[
      {"date":"2026-08-26","ticker":"NVDA","company":"NVIDIA Corp",
       "marketCap":5242955.07,"earningsDate":"2026-08-26T16:30:00","epsEstimate":2.09},
      {"date":"2026-09-02","ticker":"AVGO","company":"Broadcom Inc",
       "marketCap":1754548,"earningsDate":"2026-09-02T16:30:00","epsEstimate":1.1},
      {"date":"2026-08-20","ticker":"OLD","company":"Gone",
       "marketCap":900000,"earningsDate":"2026-08-20T16:30:00","epsEstimate":1}
    ]}}
    </script>
    """
    rows = mh._parse_earnings_html(html, "2026-08-29")
    tickers = [r["ticker"] for r in rows]
    assert "AVGO" in tickers
    assert "NVDA" not in tickers
    assert "OLD" not in tickers
    assert rows[0]["session"] == "AMC"


def test_tape_keeps_all_tiles_not_just_whitelist() -> None:
    tiles = {
        "ES": {"label": "S&P 500", "last": 7700, "change": -0.2},
        "QA": {"label": "Crude Oil Brent", "last": 88.2, "change": -0.4},
        "ZC": {"label": "Corn", "last": 536, "change": 0.5},
        "KC": {"label": "Coffee", "last": 312, "change": 1.0},
        "BTC": {"label": "Bitcoin", "last": 78000, "change": 0.1},
        "6A": {"label": "AUD", "last": 0.71, "change": -0.5},
    }
    tape = mh._tape_from_futures(tiles)
    keys = [t["ticker"] for t in tape]
    for need in ("ES", "QA", "ZC", "KC", "BTC", "6A"):
        assert need in keys
    assert keys.index("ES") < keys.index("6A")
    # alias BZ (legacy keep name) resolves to QA tiles
    aliased = mh._tape_from_futures({"BZ": {"label": "Brent", "last": 1, "change": 0}})
    assert any(t["ticker"] == "QA" or t["ticker"] == "BZ" for t in aliased)


if __name__ == "__main__":
    test_macro_gate_halves_book_earnings_does_not()
    test_missing_research_is_visible_but_bootstrap_is_safe()
    test_calendar_entry_scale_ignores_legacy_earnings_mix()
    test_empty_tape_is_not_overlay_good()
    test_heat_scale_default_is_incubate()
    test_weekend_upcoming_econ_does_not_trip_size_gate()
    test_parse_econ_route_init_keeps_upcoming()
    test_parse_earnings_preview_window()
    test_tape_keeps_all_tiles_not_just_whitelist()
    print("9 tests passed")
