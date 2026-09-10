"""Live book strip: sidecar shape + dashboards poll raw main.

Run: PYTHONPATH=. python3 -m src.test_book_suggestions
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src import book_suggestions, land_file, stock_book_diag_signals as signals


def test_suggestions_from_book_lists_1d_names() -> None:
    book = {
        "date": "2026-09-09",
        "books": {
            "1d": {
                "buy": [{"ticker": "CIG", "score": 0.7}, {"ticker": "ATRO", "score": 0.5}],
                "sell": [{"ticker": "METC", "score": -0.5}],
            }
        },
    }
    payload = book_suggestions.suggestions_from_book(book)
    assert payload["date"] == "2026-09-09"
    assert payload["buy_1d"] == ["CIG", "ATRO"]
    assert payload["sell_1d"] == ["METC"]
    assert payload["horizons"]["1d"]["buy"][0]["ticker"] == "CIG"


def test_write_skips_degraded_book() -> None:
    assert book_suggestions.write({"meta": {"degraded": True, "date": "2026-09-09"}}) is None


def test_land_stock_book_includes_suggestions() -> None:
    paths = {p.name for p in land_file.step_paths("2026-09-09", "stock_book")}
    assert "2026-09-09_suggestions.json" in paths
    assert "latest_suggestions.json" in paths
    live = {p.name for p in land_file.step_paths("2026-09-09", "live_boards")}
    assert "today.json" in live


def test_preview_suggestions_lists_names() -> None:
    text = land_file._preview_json({
        "date": "2026-09-09",
        "buy_1d": ["CIG", "ATRO"],
        "sell_1d": [{"ticker": "METC"}],
    }, "2026-09-09_suggestions.json")
    assert "CIG" in text and "ATRO" in text and "METC" in text


def test_paper_template_polls_raw_main() -> None:
    root = Path(__file__).resolve().parent.parent
    html = (root / "src" / "paper_dash.html").read_text(encoding="utf-8")
    assert book_suggestions.POLLER_MARK in html
    assert book_suggestions.TODAY_URL in html
    assert book_suggestions.SUG_URL in html
    assert "id=\"liveBook\"" in html
    baked = (root / "dashboard" / "index.html").read_text(encoding="utf-8")
    assert book_suggestions.POLLER_MARK in baked
    assert book_suggestions.TODAY_URL in baked
    assert book_suggestions.SUG_URL in baked


def test_day_board_falls_back_to_suggestions() -> None:
    html = (Path(__file__).resolve().parent.parent
            / "dashboard" / "day-board" / "index.html").read_text(encoding="utf-8")
    assert "data/stock_book/latest_suggestions.json" in html


def test_preopen_and_book_publish_strip_without_paper() -> None:
    root = Path(__file__).resolve().parent.parent
    pre = (root / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    book = (root / "src" / "run_stock_book_all.py").read_text(encoding="utf-8")
    sb = (root / "src" / "stock_book.py").read_text(encoding="utf-8")
    assert "src.publish_live_boards" in pre
    assert "--no-extras" in pre
    assert "live_boards" in pre
    live_idx = pre.index("src.publish_live_boards")
    paper_idx = pre.index("src.paper_trade")
    assert live_idx < paper_idx
    assert "src.publish_live_boards" in book
    assert "skip extras (catalyst/backtest/paper/sleeve)" in book
    assert "book_suggestions.write" in sb
    assert "ensure_dashboard_poller" in sb


def test_inspect_html_ok_when_poller_and_sidecar() -> None:
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        html = tmp / "index.html"
        html.write_text(
            '<html><body><script>const D = {"dates":["2026-09-08"]}</script>'
            '<script id="live-book-poller"></script></body></html>',
            encoding="utf-8",
        )
        sug_dir = tmp / "data" / "stock_book"
        sug_dir.mkdir(parents=True)
        (sug_dir / "2026-09-09_suggestions.json").write_text(json.dumps({
            "date": "2026-09-09",
            "buy_1d": ["CIG"],
            "sell_1d": ["METC"],
        }), encoding="utf-8")
        orig_root = signals.ROOT
        signals.ROOT = tmp
        try:
            status, reason, _ = signals.inspect_dashboard_html(html, "2026-09-09")
        finally:
            signals.ROOT = orig_root
        assert status == "OK"
        assert "2026-09-09_suggestions.json" in reason


def test_ensure_poller_is_idempotent() -> None:
    with tempfile.TemporaryDirectory() as d:
        html = Path(d) / "index.html"
        html.write_text(
            '<html><head><style>body{}</style></head>'
            '<body><div class="wrap"><h1>x</h1></div></body></html>',
            encoding="utf-8",
        )
        assert book_suggestions.ensure_dashboard_poller(html) is True
        text = html.read_text(encoding="utf-8")
        assert book_suggestions.POLLER_MARK in text
        assert book_suggestions.TODAY_URL in text
        assert book_suggestions.ensure_dashboard_poller(html) is False


def main() -> None:
    test_suggestions_from_book_lists_1d_names()
    test_write_skips_degraded_book()
    test_land_stock_book_includes_suggestions()
    test_preview_suggestions_lists_names()
    test_paper_template_polls_raw_main()
    test_day_board_falls_back_to_suggestions()
    test_preopen_and_book_publish_strip_without_paper()
    test_inspect_html_ok_when_poller_and_sidecar()
    test_ensure_poller_is_idempotent()
    print("ok")


if __name__ == "__main__":
    main()
