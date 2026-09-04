"""Dashboard shell: overview first, then per-day blotter.

Run: python -m src.test_paper_dash
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent
SHELL = ROOT / "paper_dash.html"


def test_overview_sits_above_day_blotter() -> None:
    html = SHELL.read_text(encoding="utf-8")
    assert 'id="cards"' in html
    assert 'id="chart"' in html
    assert 'id="stats"' in html
    assert 'id="dayHost"' in html
    assert 'id="dayPick"' in html
    chart = html.index('id="chart"')
    day = html.index('id="daySection"')
    tape = html.index('id="tapeSection"')
    assert chart < day < tape, "curve/stats must sit above the day blotter, tape last"


def test_template_still_has_data_slot() -> None:
    html = SHELL.read_text(encoding="utf-8")
    assert "__DATA__" in html
    assert "flatten-action/" in html


if __name__ == "__main__":
    test_overview_sits_above_day_blotter()
    test_template_still_has_data_slot()
    print("ok")
