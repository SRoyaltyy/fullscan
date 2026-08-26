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


def test_heat_scale_default_is_incubate() -> None:
    _, meta = stock_book.load_policy()
    assert float(meta["heat_scale"]) <= 0.50


if __name__ == "__main__":
    test_macro_gate_halves_book_earnings_does_not()
    test_missing_research_is_visible_but_bootstrap_is_safe()
    test_calendar_entry_scale_ignores_legacy_earnings_mix()
    test_heat_scale_default_is_incubate()
    print("4 tests passed")
