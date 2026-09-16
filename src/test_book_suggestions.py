"""Live book strip: sidecar shape + dashboards poll raw main.

Run: PYTHONPATH=. python3 -m src.test_book_suggestions
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src import (
    book_suggestions,
    land_file,
    publish_live_boards,
    stock_book_diag_signals as signals,
    strategy_tickets as st,
)


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
    assert "stock_book_1d" in html
    assert "strat.buy_1d && strat.buy_1d.length" in html
    baked = (root / "dashboard" / "index.html").read_text(encoding="utf-8")
    assert book_suggestions.POLLER_MARK in baked
    assert book_suggestions.TODAY_URL in baked
    assert book_suggestions.SUG_URL in baked
    assert book_suggestions.STRAT_URL in baked
    assert "every strategy" in baked
    assert baked.index("<h1>Paper Trading") < baked.index('id="liveBook"')
    assert "stock_book_1d" in baked
    sleeve = (root / "dashboard" / "sleeve-merge" / "index.html").read_text(
        encoding="utf-8")
    strat = (root / "dashboard" / "strategy-board" / "index.html").read_text(
        encoding="utf-8")
    assert book_suggestions.POLLER_MARK in sleeve
    assert book_suggestions.STRAT_URL in sleeve
    assert book_suggestions.POLLER_MARK in strat
    assert book_suggestions.STRAT_URL in strat
    assert "stock_book_1d" in strat
    assert (root / "src" / "paper_dash.html") in book_suggestions.LIVE_BOARD_HTML


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
    assert "let d=liveTicketsDate();" in html
    assert "h>9 || (h===9 && m>=30)" in html
    assert "D._closedDate" in html
    assert "loadLiveDay" in html
    assert "today_strategies.json" in html
    assert "function firstOk" in html
    assert "ok ? {} : last" in html
    assert "function ticketsLookLive" in html
    assert "function stripLooksLive" in html
    assert "function elitePx" in html
    assert "elitePx(st)&&ticketsToday" in html
    assert "function sessionDate" in html
    assert "d!==today" in html or "d !== today" in html
    assert "function ticketRows" in html
    assert "function liveTicketsAreLive" in html
    assert "d!==today" in html
    assert "liveDate===date && liveTicketsAreLive()" in html
    assert "after 09:30 live" in html
    assert "LIVE</span>" in html
    assert "setInterval(loadLiveDay, afterBell()?20000:60000)" in html
    assert html.index("<h1>Factor strategy mine") < html.index('id="liveDay"')
    assert 'id="live-day-loader"' in html
    assert html.index('id="liveDay"') < html.index('id="cards"')
    fm_main = html.split('id="fm-main"')[1].split('id="live-day-loader"')[0]
    assert "function loadLiveDay" not in fm_main
    assert "function loadLiveDay" in html.split('id="live-day-loader"')[1]
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
    assert "stock_book_1d" in html
    assert "function firstOk" in html
    assert "ok ? {} : last" in html
    assert "function ticketsLookLive" in html
    assert "function stripLooksLive" in html
    assert "function sessionDate" in html
    assert "afterBell() ? ticketsLookLive" in html
    assert "afterBell() ? stripLooksLive" in html
    assert '"today.json"' in html
    assert "viewingToday && afterBell() && !liveBoard" in html
    assert "afterBell() && dates.indexOf(today) < 0" in html
    assert "today && afterBell() && dates.includes(today)" in html
    assert "earlyBuys" in html
    assert "function elitePx" in html
    assert "elitePx(d) && ticketsToday" in html
    assert "takeTicket1d && (earlyBuys.length || earlySells.length)" in html
    assert "!buys && !sells && !afterBell()" in html


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
    assert "timeout_s=420" in pre
    assert "src.publish_live_boards" in book
    pub_py = (root / "src" / "publish_live_boards.py").read_text(encoding="utf-8")
    assert "rewrite_today_strip" in pub_py
    assert "ticket_1d_rows" in pub_py
    assert "load_live_ticket_payload" in pub_py
    assert '{date}_open_0930.json' in pub_py
    assert "keep 09:30 Elite 1d" in pub_py
    assert "tickets not elite_live after rebuild" in pub_py
    assert "skip extras (catalyst/backtest/paper/sleeve)" in book
    assert "timeout_s=420" in book
    assert book.count("timeout_s=420") >= 2
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
    assert 'python3 -c "import requests"' in script
    assert "open-pack live px miss" in script
    assert "openpyxl requests" in pre
    assert "openpyxl requests" in book
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
    assert "Overlayed dashboard/index.html (paper) from main" in dep
    assert "data/day_board/**" in dep
    assert "Overlayed live strip" in dep
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
    assert "ok ? {} : last" in book_suggestions._POLLER_JS
    assert "afterBell() ? Object.assign({}, today)" in book_suggestions._POLLER_JS
    assert "if(!afterBell())" in book_suggestions._POLLER_JS
    assert "function ticketsLookLive" in book_suggestions._POLLER_JS
    assert "function stripLooksLive" in book_suggestions._POLLER_JS
    assert "function elitePx" in book_suggestions._POLLER_JS
    assert "elitePx(strat) && ticketsToday" in book_suggestions._POLLER_JS
    assert "function sessionDate" in book_suggestions._POLLER_JS
    assert "src.overlay_live_strip" in dep
    assert "stock_book_1d" in book_suggestions._POLLER_JS
    assert "strat.buy_1d && strat.buy_1d.length" in book_suggestions._POLLER_JS
    assert 'cron: "32 13 * * 1-5"' in orch
    assert 'cron: "35 14 * * 1-5"' in orch
    assert 'cron: "15 14-19 * * 1-5"' in orch


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


def test_ticket_1d_rows_fall_back_to_stock_book() -> None:
    buys, sells, quote = publish_live_boards.ticket_1d_rows({
        "quote": {"src": "elite_live", "after_open": True},
        "strategies": {
            "stock_book_1d": {
                "buy": [{"ticker": "AVAH", "px": 14.24, "px_src": "elite_live"}],
                "sell": [{"ticker": "METC", "px": 9.98, "px_src": "elite_live"}],
            }
        },
    })
    assert [x["ticker"] for x in buys] == ["AVAH"]
    assert [x["ticker"] for x in sells] == ["METC"]
    assert quote["src"] == "elite_live"


def test_rewrite_today_strip_overwrites_reranked_names() -> None:
    """A noon live_boards land must not leave today.json on MTCH without px."""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        day = root / "data" / "day_board"
        fm = root / "dashboard" / "factor-mine"
        day.mkdir(parents=True)
        fm.mkdir(parents=True)
        (day / "today.json").write_text(json.dumps({
            "date": "2026-09-15",
            "buy_1d": [{"ticker": "MTCH", "score": 0.3}],
        }), encoding="utf-8")
        old_day = publish_live_boards.DAY_BOARD
        old_fm = publish_live_boards.FM_TODAY
        old_root = publish_live_boards.ROOT
        publish_live_boards.ROOT = root
        publish_live_boards.DAY_BOARD = day
        publish_live_boards.FM_TODAY = fm / "today.json"
        try:
            wrote = publish_live_boards.rewrite_today_strip(
                "2026-09-15",
                [{"ticker": "AVAH", "px": 14.24, "px_src": "elite_live"}],
                [{"ticker": "METC", "px": 9.98, "px_src": "elite_live"}],
                {"src": "elite_live", "after_open": True},
                generated_at="t",
            )
        finally:
            publish_live_boards.ROOT = old_root
            publish_live_boards.DAY_BOARD = old_day
            publish_live_boards.FM_TODAY = old_fm
        assert any(p.endswith("today.json") for p in wrote)
        strip = json.loads((day / "today.json").read_text(encoding="utf-8"))
        assert strip["buy_1d"][0]["ticker"] == "AVAH"
        assert strip["buy_1d"][0]["px"] == 14.24
        assert strip["quote"]["src"] == "elite_live"
        fm_strip = json.loads((fm / "today.json").read_text(encoding="utf-8"))
        assert fm_strip["buy_1d"][0]["ticker"] == "AVAH"


def test_load_live_ticket_payload_requires_elite_live() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        day = root / "data" / "day_board"
        day.mkdir(parents=True)
        (day / "today_strategies.json").write_text(json.dumps({
            "date": "2026-09-15",
            "clock_legal_for": "2026-09-15",
            "quote": {"after_open": True, "src": "session_export"},
            "buy_1d": [{"ticker": "AVAH", "px": 14.24}],
        }), encoding="utf-8")
        old_root = publish_live_boards.ROOT
        old_day = publish_live_boards.DAY_BOARD
        publish_live_boards.ROOT = root
        publish_live_boards.DAY_BOARD = day
        try:
            assert publish_live_boards.load_live_ticket_payload("2026-09-15") == {}
            payload = json.loads((day / "today_strategies.json").read_text())
            payload["quote"]["src"] = "elite_live"
            (day / "today_strategies.json").write_text(
                json.dumps(payload), encoding="utf-8")
            got = publish_live_boards.load_live_ticket_payload("2026-09-15")
            assert got["buy_1d"][0]["ticker"] == "AVAH"
        finally:
            publish_live_boards.ROOT = old_root
            publish_live_boards.DAY_BOARD = old_day


def test_load_live_ticket_payload_prefers_open_0930_lock() -> None:
    """A later elite DBX today_strategies.json must not beat the 09:30 lock."""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        day = root / "data" / "day_board"
        day.mkdir(parents=True)
        (day / "2026-09-15_open_0930.json").write_text(json.dumps({
            "date": "2026-09-15",
            "clock_legal_for": "2026-09-15",
            "quote": {"after_open": True, "src": "elite_live"},
            "buy_1d": [{"ticker": "MTCH", "px": 43.04, "px_src": "elite_live"}],
        }), encoding="utf-8")
        (day / "today_strategies.json").write_text(json.dumps({
            "date": "2026-09-15",
            "clock_legal_for": "2026-09-15",
            "quote": {"after_open": True, "src": "elite_live"},
            "buy_1d": [{"ticker": "DBX", "px": 38.07, "px_src": "elite_live"}],
        }), encoding="utf-8")
        old_root = publish_live_boards.ROOT
        old_day = publish_live_boards.DAY_BOARD
        publish_live_boards.ROOT = root
        publish_live_boards.DAY_BOARD = day
        try:
            got = publish_live_boards.load_live_ticket_payload("2026-09-15")
        finally:
            publish_live_boards.ROOT = old_root
            publish_live_boards.DAY_BOARD = old_day
        assert got["buy_1d"][0]["ticker"] == "MTCH"
        assert got["buy_1d"][0]["px"] == 43.04


def test_publish_keeps_elite_1d_when_tickets_rebuild_fails() -> None:
    """Noon skip-if-good land must not leave MTCH on today.json."""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        day = root / "data" / "day_board"
        fm = root / "dashboard" / "factor-mine"
        book = root / "data" / "stock_book"
        day.mkdir(parents=True)
        fm.mkdir(parents=True)
        book.mkdir(parents=True)
        (book / "2026-09-15_stock_book.json").write_text("{}", encoding="utf-8")
        (day / "today_strategies.json").write_text(json.dumps({
            "date": "2026-09-15",
            "clock_legal_for": "2026-09-15",
            "generated_at": "t",
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "AVAH", "px": 14.24, "px_src": "elite_live"}],
            "sell_1d": [{"ticker": "METC", "px": 9.98, "px_src": "elite_live"}],
            "strategies": {"stock_book_1d": {
                "buy": [{"ticker": "AVAH", "px": 14.24, "px_src": "elite_live"}],
                "sell": [{"ticker": "METC", "px": 9.98, "px_src": "elite_live"}],
            }},
        }), encoding="utf-8")
        board = {
            "date": "2026-09-15",
            "generated_at": "noon",
            "overall": "ok",
            "ranker_ready": True,
            "counts": {},
            "selections": {"buy_1d": [{"ticker": "MTCH", "score": 0.3}]},
            "lands": [],
        }
        old = {
            "ROOT": publish_live_boards.ROOT,
            "DAY_BOARD": publish_live_boards.DAY_BOARD,
            "FM_TODAY": publish_live_boards.FM_TODAY,
            "BOOK": publish_live_boards.BOOK,
        }
        publish_live_boards.ROOT = root
        publish_live_boards.DAY_BOARD = day
        publish_live_boards.FM_TODAY = fm / "today.json"
        publish_live_boards.BOOK = book
        from unittest import mock
        from src import day_board
        try:
            with mock.patch.object(day_board, "BOARD_DIR", day), \
                    mock.patch.object(day_board, "build", return_value=board), \
                    mock.patch.object(day_board, "write_html",
                                     side_effect=RuntimeError("skip html")), \
                    mock.patch("src.strategy_tickets.build",
                               side_effect=RuntimeError("timeout")):
                out = publish_live_boards.publish(
                    "2026-09-15", write=True, extras=False)
        finally:
            for k, v in old.items():
                setattr(publish_live_boards, k, v)
        strip = json.loads((day / "today.json").read_text(encoding="utf-8"))
        assert strip["buy_1d"][0]["ticker"] == "AVAH"
        assert strip["buy_1d"][0]["px"] == 14.24
        assert out["buy_1d"] == ["AVAH"]


def test_publish_does_not_clobber_last_closed_fm_today_before_bell() -> None:
    """Pre-open live_boards must not empty factor-mine today.json."""
    from datetime import datetime
    from unittest import mock
    from zoneinfo import ZoneInfo
    from src import day_board

    et = ZoneInfo("America/New_York")
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        day = root / "data" / "day_board"
        fm = root / "dashboard" / "factor-mine"
        book = root / "data" / "stock_book"
        day.mkdir(parents=True)
        fm.mkdir(parents=True)
        book.mkdir(parents=True)
        lock = {
            "date": "2026-09-15",
            "clock_legal_for": "2026-09-15",
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "MTCH", "px": 43.04, "px_src": "elite_live"}],
            "sell_1d": [{"ticker": "OKLO", "px": 36.14, "px_src": "elite_live"}],
        }
        (day / "2026-09-15_open_0930.json").write_text(
            json.dumps(lock), encoding="utf-8")
        (fm / "today.json").write_text(json.dumps(lock), encoding="utf-8")
        (book / "2026-09-16_stock_book.json").write_text("{}", encoding="utf-8")
        board = {
            "date": "2026-09-16",
            "generated_at": "preopen",
            "overall": "—",
            "ranker_ready": False,
            "counts": {},
            "selections": {"buy_1d": [], "sell_1d": []},
            "lands": [{"key": "news_parse"}],
        }
        old = {
            "ROOT": publish_live_boards.ROOT,
            "DAY_BOARD": publish_live_boards.DAY_BOARD,
            "FM_TODAY": publish_live_boards.FM_TODAY,
            "BOOK": publish_live_boards.BOOK,
        }
        publish_live_boards.ROOT = root
        publish_live_boards.DAY_BOARD = day
        publish_live_boards.FM_TODAY = fm / "today.json"
        publish_live_boards.BOOK = book
        before = datetime(2026, 9, 16, 4, 16, tzinfo=et)
        try:
            with mock.patch.object(day_board, "ROOT", root), \
                    mock.patch.object(day_board, "BOARD_DIR", day), \
                    mock.patch.object(day_board, "build", return_value=board), \
                    mock.patch.object(day_board, "write_html",
                                     side_effect=RuntimeError("skip html")), \
                    mock.patch("src.strategy_tickets.build",
                               side_effect=RuntimeError("skip tickets")):
                publish_live_boards.publish(
                    "2026-09-16", write=True, extras=False, when=before)
        finally:
            for k, v in old.items():
                setattr(publish_live_boards, k, v)
        today = json.loads((day / "today.json").read_text(encoding="utf-8"))
        assert today["date"] == "2026-09-15"
        assert today["buy_1d"][0]["ticker"] == "MTCH"
        assert today["buy_1d"][0]["px"] == 43.04
        fm_today = json.loads((fm / "today.json").read_text(encoding="utf-8"))
        assert fm_today["date"] == "2026-09-15"
        assert fm_today["buy_1d"][0]["ticker"] == "MTCH"
        assert fm_today["buy_1d"][0]["px"] == 43.04


def test_day_board_write_json_keeps_elite_when_news_parse_lands() -> None:
    """note_land / write_json must not drop Elite px for a ranker restamp."""
    import tempfile
    from src import day_board
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        board_dir = root / "data" / "day_board"
        board_dir.mkdir(parents=True)
        (board_dir / "today_strategies.json").write_text(json.dumps({
            "date": "2026-09-15",
            "clock_legal_for": "2026-09-15",
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "MTCH", "px": 43.04, "px_src": "elite_live"}],
            "sell_1d": [{"ticker": "METC", "px": 9.98, "px_src": "elite_live"}],
        }), encoding="utf-8")
        old = day_board.BOARD_DIR
        day_board.BOARD_DIR = board_dir
        try:
            day_board.write_json({
                "date": "2026-09-15",
                "generated_at": "news_parse",
                "overall": "ok",
                "ranker_ready": True,
                "counts": {},
                "selections": {"buy_1d": [{"ticker": "MTCH", "score": 0.34}]},
                "lands": [{"key": "news_parse"}],
            })
        finally:
            day_board.BOARD_DIR = old
        strip = json.loads((board_dir / "today.json").read_text(encoding="utf-8"))
        assert strip["buy_1d"][0]["ticker"] == "MTCH"
        assert strip["buy_1d"][0]["px"] == 43.04
        assert strip["quote"]["src"] == "elite_live"
        fm = root / "dashboard" / "factor-mine" / "today.json"
        fm_strip = json.loads(fm.read_text(encoding="utf-8"))
        assert fm_strip["buy_1d"][0]["px"] == 43.04
        assert fm_strip["quote"]["src"] == "elite_live"


def test_publish_keeps_elite_when_rebuild_is_session_export() -> None:
    """A failed Elite restamp must not replace AVAH elite_live with MTCH."""
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        day = root / "data" / "day_board"
        fm = root / "dashboard" / "factor-mine"
        book = root / "data" / "stock_book"
        fmd = root / "data" / "factor_mine"
        day.mkdir(parents=True)
        fm.mkdir(parents=True)
        book.mkdir(parents=True)
        fmd.mkdir(parents=True)
        (book / "2026-09-15_stock_book.json").write_text("{}", encoding="utf-8")
        elite = {
            "date": "2026-09-15",
            "clock_legal_for": "2026-09-15",
            "generated_at": "0930",
            "n": 1,
            "n_ok": 1,
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "AVAH", "px": 14.24, "px_src": "elite_live"}],
            "sell_1d": [{"ticker": "METC", "px": 9.98, "px_src": "elite_live"}],
            "strategies": {"stock_book_1d": {
                "buy": [{"ticker": "AVAH", "px": 14.24, "px_src": "elite_live"}],
                "sell": [{"ticker": "METC", "px": 9.98, "px_src": "elite_live"}],
            }},
        }
        (day / "today_strategies.json").write_text(
            json.dumps(elite), encoding="utf-8")
        (day / "2026-09-15_strategy_tickets.json").write_text(
            json.dumps(elite), encoding="utf-8")
        board = {
            "date": "2026-09-15",
            "generated_at": "noon",
            "overall": "ok",
            "ranker_ready": True,
            "counts": {},
            "selections": {"buy_1d": [{"ticker": "MTCH", "score": 0.3}]},
            "lands": [],
        }
        bad = {
            "date": "2026-09-15",
            "clock_legal_for": "2026-09-15",
            "clock_use": "session_open",
            "generated_at": "1226",
            "n": 1,
            "n_ok": 1,
            "look": {"source": "look"},
            "quote": {
                "src": "session_export+finviz_session: No module named 'requests'",
                "after_open": True,
            },
            "buy_1d": [{"ticker": "MTCH", "px": 42.69,
                        "px_src": "session_export"}],
            "strategies": {"stock_book_1d": {
                "buy": [{"ticker": "MTCH", "px": 42.69,
                         "px_src": "session_export"}],
                "sell": [],
            }},
        }
        old_pub = {
            "ROOT": publish_live_boards.ROOT,
            "DAY_BOARD": publish_live_boards.DAY_BOARD,
            "FM_TODAY": publish_live_boards.FM_TODAY,
            "BOOK": publish_live_boards.BOOK,
        }
        old_st = {
            "ROOT": st.ROOT,
            "DAY": st.DAY,
            "DASH_FM": st.DASH_FM,
            "FM_DIR": st.FM_DIR,
        }
        publish_live_boards.ROOT = root
        publish_live_boards.DAY_BOARD = day
        publish_live_boards.FM_TODAY = fm / "today.json"
        publish_live_boards.BOOK = book
        st.ROOT = root
        st.DAY = day
        st.DASH_FM = fm
        st.FM_DIR = fmd
        from unittest import mock
        from src import day_board
        from src import elite_live_px as elp
        try:
            with mock.patch.object(day_board, "BOARD_DIR", day), \
                    mock.patch.object(day_board, "build", return_value=board), \
                    mock.patch.object(day_board, "write_html",
                                     side_effect=RuntimeError("skip html")), \
                    mock.patch.object(elp, "after_open", return_value=True), \
                    mock.patch("src.strategy_tickets.build", return_value=bad), \
                    mock.patch("src.hard_red_sit_research.write_per_sleeve"), \
                    mock.patch("src.book_suggestions.write", return_value=None), \
                    mock.patch("src.book_suggestions.ensure_dashboard_poller",
                               return_value=False), \
                    mock.patch("src.book_suggestions.ensure_live_board_pollers",
                               return_value=[]):
                out = publish_live_boards.publish(
                    "2026-09-15", write=True, extras=False)
        finally:
            for k, v in old_pub.items():
                setattr(publish_live_boards, k, v)
            for k, v in old_st.items():
                setattr(st, k, v)
        tickets = json.loads((day / "today_strategies.json").read_text())
        assert tickets["buy_1d"][0]["ticker"] == "AVAH"
        assert tickets["quote"]["src"].startswith("elite_live")
        strip = json.loads((day / "today.json").read_text(encoding="utf-8"))
        assert strip["buy_1d"][0]["ticker"] == "AVAH"
        assert strip["buy_1d"][0]["px"] == 14.24
        assert out["buy_1d"] == ["AVAH"]


def test_overlay_prefers_quoted_tickets_and_restamps_score_only_strip() -> None:
    """Pages must not publish stripped factor-mine tickets over the slim."""
    from src import overlay_live_strip as ols

    with tempfile.TemporaryDirectory() as d:
        repo = Path(d)
        dest = repo / "pages_out"
        day = repo / "data" / "day_board"
        fm = repo / "dashboard" / "factor-mine"
        day.mkdir(parents=True)
        fm.mkdir(parents=True)
        (day / "today_strategies.json").write_text(json.dumps({
            "date": "2026-09-15",
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "MTCH", "px": 43.04, "px_src": "elite_live"}],
            "sell_1d": [{"ticker": "OKLO", "px": 36.14, "px_src": "elite_live"}],
            "strategies": {"stock_book_1d": {
                "buy": [{"ticker": "MTCH", "px": 43.04, "px_src": "elite_live"}],
                "sell": [{"ticker": "OKLO", "px": 36.14, "px_src": "elite_live"}],
            }},
        }), encoding="utf-8")
        (fm / "today_strategies.json").write_text(json.dumps({
            "date": "2026-09-15",
            "n": 327,
            "families": ["excel"],
            "strategies": {"stock_book_1d": {
                "buy": [{"ticker": "MTCH", "px": 43.04, "px_src": "elite_live"}],
            }},
        }), encoding="utf-8")
        (day / "today.json").write_text(json.dumps({
            "date": "2026-09-15",
            "buy_1d": [{"ticker": "MTCH", "score": 0.3}],
        }), encoding="utf-8")
        (fm / "today.json").write_text(json.dumps({
            "date": "2026-09-15",
            "buy_1d": [{"ticker": "MPC", "score": 0.9}],
        }), encoding="utf-8")
        wrote = ols.overlay(
            dest, repo=repo, date="2026-09-15",
            when=datetime(2026, 9, 15, 15, 5, tzinfo=ZoneInfo("America/New_York")),
        )
        assert wrote
        for rel in ("", "factor-mine", "strategy-board", "day-board"):
            base = dest / rel if rel else dest
            tickets = json.loads((base / "today_strategies.json").read_text())
            strip = json.loads((base / "today.json").read_text())
            assert tickets["quote"]["src"] == "elite_live"
            assert tickets["buy_1d"][0]["px"] == 43.04
            assert "families" not in tickets
            assert strip["buy_1d"][0]["ticker"] == "MTCH"
            assert strip["buy_1d"][0]["px"] == 43.04
            assert strip["quote"]["src"] == "elite_live"


def test_overlay_restamps_empty_today_from_open_0930_lock() -> None:
    """14:44 ET today.json had buy_1d=[] while tickets were elite DBX."""
    from src import overlay_live_strip as ols

    with tempfile.TemporaryDirectory() as d:
        repo = Path(d)
        dest = repo / "pages_out"
        day = repo / "data" / "day_board"
        day.mkdir(parents=True)
        (day / "2026-09-15_open_0930.json").write_text(json.dumps({
            "date": "2026-09-15",
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "MTCH", "px": 43.04, "px_src": "elite_live"}],
        }), encoding="utf-8")
        (day / "today_strategies.json").write_text(json.dumps({
            "date": "2026-09-15",
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "DBX", "px": 38.07, "px_src": "elite_live"}],
        }), encoding="utf-8")
        (day / "today.json").write_text(json.dumps({
            "date": "2026-09-15",
            "buy_1d": [],
        }), encoding="utf-8")
        ols.overlay(
            dest, repo=repo, date="2026-09-15",
            when=datetime(2026, 9, 15, 15, 5, tzinfo=ZoneInfo("America/New_York")),
        )
        tickets = json.loads((dest / "today_strategies.json").read_text())
        strip = json.loads((dest / "today.json").read_text())
        assert tickets["buy_1d"][0]["ticker"] == "MTCH"
        assert tickets["buy_1d"][0]["px"] == 43.04
        assert strip["buy_1d"][0]["ticker"] == "MTCH"
        assert strip["buy_1d"][0]["px"] == 43.04
        assert strip["quote"]["src"] == "elite_live"


def test_overlay_slims_fat_same_origin_tickets() -> None:
    """deploy-dashboard must not copy the ~900K book onto Pages firstOk."""
    from src import overlay_live_strip as ols

    fat_strats = {
        "stock_book_1d": {
            "buy": [{"ticker": "MPC", "px": 411.65, "px_src": "session_export"}],
            "sell": [{"ticker": "OKLO", "px": 35.98, "px_src": "session_export"}],
        },
        **{f"family_{i}": {"family": "factor_mine", "rows": [0] * 20}
           for i in range(80)},
    }
    with tempfile.TemporaryDirectory() as d:
        repo = Path(d)
        dest = repo / "pages_out"
        day = repo / "data" / "day_board"
        day.mkdir(parents=True)
        (day / "today_strategies.json").write_text(json.dumps({
            "date": "2026-09-16",
            "clock_legal_for": "2026-09-16",
            "quote": {"src": "session_export", "after_open": False},
            "buy_1d": [{"ticker": "MPC", "px": 411.65,
                        "px_src": "session_export"}],
            "sell_1d": [{"ticker": "OKLO", "px": 35.98,
                         "px_src": "session_export"}],
            "families": ["excel", "factor_mine"],
            "strategies": fat_strats,
        }), encoding="utf-8")
        (day / "today.json").write_text(json.dumps({
            "date": "2026-09-15",
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "MTCH", "px": 43.04, "px_src": "elite_live"}],
        }), encoding="utf-8")
        ols.overlay(
            dest, repo=repo, date="2026-09-16",
            when=datetime(2026, 9, 16, 5, 30, tzinfo=ZoneInfo("America/New_York")),
        )
        for rel in ("day-board", "strategy-board", "factor-mine", ""):
            base = dest / rel if rel else dest
            raw = (base / "today_strategies.json").read_text()
            tickets = json.loads(raw)
            assert tickets["buy_1d"][0]["ticker"] == "MPC"
            assert tickets["quote"]["src"] == "session_export"
            assert "families" not in tickets
            assert set((tickets.get("strategies") or {})) == {"stock_book_1d"}
            assert len(raw) < 20_000


def test_overlay_ignores_yesterdays_open_0930_lock() -> None:
    """A 09-15 lock must not keep Pages on MTCH after the 09-16 bell."""
    from src import overlay_live_strip as ols

    with tempfile.TemporaryDirectory() as d:
        repo = Path(d)
        dest = repo / "pages_out"
        day = repo / "data" / "day_board"
        day.mkdir(parents=True)
        (day / "2026-09-15_open_0930.json").write_text(json.dumps({
            "date": "2026-09-15",
            "clock_legal_for": "2026-09-15",
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "MTCH", "px": 43.04, "px_src": "elite_live"}],
        }), encoding="utf-8")
        (day / "today_strategies.json").write_text(json.dumps({
            "date": "2026-09-16",
            "clock_legal_for": "2026-09-16",
            "quote": {"src": "elite_live", "after_open": True},
            "buy_1d": [{"ticker": "AAPL", "px": 221.1, "px_src": "elite_live"}],
        }), encoding="utf-8")
        (day / "today.json").write_text(json.dumps({
            "date": "2026-09-16",
            "buy_1d": [{"ticker": "AAPL", "score": 0.4}],
        }), encoding="utf-8")
        ols.overlay(
            dest, repo=repo, date="2026-09-16",
            when=datetime(2026, 9, 16, 9, 35, tzinfo=ZoneInfo("America/New_York")),
        )
        tickets = json.loads((dest / "today_strategies.json").read_text())
        strip = json.loads((dest / "today.json").read_text())
        assert tickets["buy_1d"][0]["ticker"] == "AAPL"
        assert tickets["buy_1d"][0]["px"] == 221.1
        assert strip["buy_1d"][0]["ticker"] == "AAPL"
        assert strip["buy_1d"][0]["px"] == 221.1


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
    test_ticket_1d_rows_fall_back_to_stock_book()
    test_rewrite_today_strip_overwrites_reranked_names()
    test_load_live_ticket_payload_requires_elite_live()
    test_load_live_ticket_payload_prefers_open_0930_lock()
    test_publish_keeps_elite_1d_when_tickets_rebuild_fails()
    test_publish_keeps_elite_when_rebuild_is_session_export()
    test_publish_does_not_clobber_last_closed_fm_today_before_bell()
    test_day_board_write_json_keeps_elite_when_news_parse_lands()
    test_overlay_prefers_quoted_tickets_and_restamps_score_only_strip()
    test_overlay_restamps_empty_today_from_open_0930_lock()
    test_overlay_slims_fat_same_origin_tickets()
    test_overlay_ignores_yesterdays_open_0930_lock()
    print("ok")


if __name__ == "__main__":
    main()
