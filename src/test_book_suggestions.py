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
    assert "today_strategies.json" in live
    assert "strategy_tickets.json" in live
    assert "2026-09-09_strategy_tickets.json" in live
    live_rel = {str(p.relative_to(land_file.ROOT))
                for p in land_file.step_paths("2026-09-09", "live_boards")}
    assert "dashboard/today_strategies.json" in live_rel
    assert "dashboard/factor-mine/strategy_tickets.json" in live_rel
    assert "dashboard/factor-mine/today_strategies.json" in live_rel


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
    assert book_suggestions.STRAT_URL in html
    assert "every strategy" in html
    assert "id=\"liveBook\"" in html
    assert html.index("<h1>Paper Trading") < html.index('id="liveBook"')
    baked = (root / "dashboard" / "index.html").read_text(encoding="utf-8")
    assert book_suggestions.POLLER_MARK in baked
    assert book_suggestions.TODAY_URL in baked
    assert book_suggestions.SUG_URL in baked
    assert book_suggestions.STRAT_URL in baked
    assert "every strategy" in baked
    assert baked.index("<h1>Paper Trading") < baked.index('id="liveBook"')
    sleeve = (root / "dashboard" / "sleeve-merge" / "index.html").read_text(
        encoding="utf-8")
    strat = (root / "dashboard" / "strategy-board" / "index.html").read_text(
        encoding="utf-8")
    assert book_suggestions.POLLER_MARK in sleeve
    assert book_suggestions.STRAT_URL in sleeve
    assert book_suggestions.POLLER_MARK in strat
    assert book_suggestions.STRAT_URL in strat


def test_factor_mine_template_paints_every_strategy() -> None:
    html = (Path(__file__).resolve().parent.parent
            / "src" / "factor_mine_dash.html").read_text(encoding="utf-8")
    assert "today_strategies.json" in html
    assert "every strategy" in html
    assert "open_px" in html
    assert "elite_live" in html
    assert "RESEARCH" in html
    assert "liveResearch" in html
    assert "(A) short-only" in html
    assert "(B) dip-scoop" in html
    assert "keep_bar" in html or "KEEP bar unchanged" in html
    assert "SIT would-have" in html
    assert "liveStartRow" in html
    assert "pending — not 0%" in html
    assert "attachLiveSessionDate" in html
    assert "loadLiveDay" in html
    assert "today_strategies.json" in html
    assert "function firstOk" in html
    assert "after 09:30 live" in html
    assert "LIVE</span>" in html
    assert "setInterval(loadLiveDay, afterBell()?20000:60000)" in html
    assert html.index("<h1>Factor strategy mine") < html.index('id="liveDay"')
    assert 'id="bracketOn"' in html
    assert 'id="takePct"' in html
    assert 'id="stopPct"' in html
    assert 'id="fmSellOn"' in html
    assert "function ensureLiveBooks" in html
    assert "const sp=document.getElementById('stopPct')" not in html
    assert "stopEl" in html
    assert "sell% all strats" in html
    assert "Long-led" in html
    assert "function legsLine" in html
    assert "kindFilter==='longled'" in html
    assert "<th>Δ</th>" in html
    assert "kindFilter='single'" in html


def test_day_board_falls_back_to_suggestions() -> None:
    html = (Path(__file__).resolve().parent.parent
            / "dashboard" / "day-board" / "index.html").read_text(encoding="utf-8")
    assert "data/stock_book/latest_suggestions.json" in html
    assert "today_strategies.json" in html
    assert "Every strategy" in html


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
    assert "timeout_s=180" in book
    assert "book_suggestions.write" in sb
    assert "ensure_dashboard_poller" in sb


def test_open_pack_wired_when_skip_if_good() -> None:
    """A quality-ok book must still restamp every-sleeve tickets after 09:30."""
    root = Path(__file__).resolve().parent.parent
    wf = root / ".github" / "workflows"
    pre = (wf / "preopen_all.yml").read_text(encoding="utf-8")
    book = (wf / "stock_book_all.yml").read_text(encoding="utf-8")
    orch = (wf / "daily_orchestrator.yml").read_text(encoding="utf-8")
    pub = (wf / "publish_strategy_tickets.yml").read_text(encoding="utf-8")
    dep = (wf / "deploy-dashboard.yml").read_text(encoding="utf-8")
    script = (root / "scripts" / "publish_open_pack.sh").read_text(encoding="utf-8")
    assert "scripts/publish_open_pack.sh" in pre
    assert "scripts/publish_open_pack.sh" in book
    assert 'env.skip == \'yes\'' in book
    assert "skip_python == 'yes'" in pre
    assert "src.strategy_tickets" in script
    assert "set -euo pipefail" in script
    assert "clock_legal_for" in script
    assert "src.land_file" in script
    assert "publish_dashboard.sh" in script
    assert "src.sleeve_merge" not in script
    assert "src.webull_exec" not in script
    assert "write_per_sleeve" in (root / "src" / "strategy_tickets.py").read_text(encoding="utf-8")
    assert "unset FINVIZ_SKIP_LIVE" in script
    assert "FINVIZ_SKIP_LIVE: \"\"" in pre
    assert "FINVIZ_SKIP_LIVE: \"\"" in book
    assert "quoteLabel" in book_suggestions._POLLER_JS
    assert "elite_live" in book_suggestions._POLLER_JS
    assert "px_src" in book_suggestions._POLLER_JS or "open_px" in book_suggestions._POLLER_JS
    assert "publish_strategy_tickets.yml" in orch
    assert "open_0930.yml" in orch
    assert "past 09:30 ET" in orch
    assert "Publish strategy tickets" in dep
    assert "Open 09:30 pack" in dep
    assert "gh workflow run deploy-dashboard.yml" in pub
    assert "dashboard/today_strategies.json" in pub
    assert "scripts/publish_dashboard.sh" in pub
    assert "FINVIZ_EMAIL" in pub
    assert "open_px" in book_suggestions._POLLER_JS
    assert "RESEARCH" in book_suggestions._POLLER_JS
    assert "(A) short-only" in book_suggestions._POLLER_JS
    assert "(B) dip-scoop" in book_suggestions._POLLER_JS
    assert "not a wire" in book_suggestions._POLLER_JS
    assert "SIT would-have" in book_suggestions._POLLER_JS
    assert "Theme Radar" in book_suggestions._POLLER_JS
    assert "factor-mine/today_strategies.json" in book_suggestions._POLLER_JS
    assert "afterBell() ? 20000 : 60000" in book_suggestions._POLLER_JS
    assert "function firstOk" in book_suggestions._POLLER_JS
    assert 'cron: "32 13 * * 1-5"' in orch
    assert 'cron: "35 14 * * 1-5"' in orch


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


def test_ensure_poller_injects_into_main_sleeve_page() -> None:
    with tempfile.TemporaryDirectory() as d:
        html = Path(d) / "index.html"
        html.write_text(
            "<html><head><style>body{}</style></head>"
            "<body><main><h1>Combined sleeve</h1></main></body></html>",
            encoding="utf-8",
        )
        assert book_suggestions.ensure_dashboard_poller(html) is True
        text = html.read_text(encoding="utf-8")
        assert book_suggestions.POLLER_MARK in text
        assert book_suggestions.STRAT_URL in text
        assert "<main>" in text
        assert text.index("<main>") < text.index('id="liveBook"')


def test_ensure_poller_refreshes_old_uniform_strip() -> None:
    """A baked 'BUY 1d / SELL 1d only' poller must be replaced."""
    with tempfile.TemporaryDirectory() as d:
        html = Path(d) / "index.html"
        html.write_text(
            '<html><head><style>body{}</style></head><body>'
            '<script id="live-book-poller">\n'
            '(function(){ var URLS=["https://example/today.json"]; '
            'function paint(d){ el.innerHTML = "BUY 1d " + d.buy_1d; }'
            '})();\n</script></body></html>',
            encoding="utf-8",
        )
        assert book_suggestions.ensure_dashboard_poller(html) is True
        text = html.read_text(encoding="utf-8")
        assert book_suggestions.STRAT_URL in text
        assert "every strategy" in text
        assert "https://example/today.json" not in text


def main() -> None:
    test_suggestions_from_book_lists_1d_names()
    test_write_skips_degraded_book()
    test_land_stock_book_includes_suggestions()
    test_preview_suggestions_lists_names()
    test_paper_template_polls_raw_main()
    test_factor_mine_template_paints_every_strategy()
    test_day_board_falls_back_to_suggestions()
    test_preopen_and_book_publish_strip_without_paper()
    test_open_pack_wired_when_skip_if_good()
    test_inspect_html_ok_when_poller_and_sidecar()
    test_ensure_poller_is_idempotent()
    test_ensure_poller_injects_into_main_sleeve_page()
    test_ensure_poller_refreshes_old_uniform_strip()
    print("ok")


if __name__ == "__main__":
    main()
