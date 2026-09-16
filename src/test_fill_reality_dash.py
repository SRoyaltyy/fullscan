"""Fill-reality dashboards keep the strategy-table format.

Run: PYTHONPATH=. python3 -m src.test_fill_reality_dash
"""
from __future__ import annotations

from pathlib import Path

from src import book_fill_reality as bfr
from src import strategy_board as sb

ROOT = Path(__file__).resolve().parent.parent
JS = ROOT / "dashboard" / "factor-mine" / "fill-scenarios.js"
TPL = ROOT / "src" / "factor_mine_dash.html"


def test_js_has_market_and_limit_scenarios() -> None:
    text = JS.read_text(encoding="utf-8")
    assert "Market-buy messiness" in text
    assert "Limit-buy messiness" in text
    for key in (
        "market_mid", "market_adverse", "market_favorable",
        "partial_50", "gap_miss", "limit_open", "limit_prior",
    ):
        assert key in text, key
    assert "wrong price" in text
    assert "partial 50%" in text
    assert "missed fill" in text
    for col in ("Strategy", "Side", "Win%", "$ days", "Starts", "Book%", "Signal%", "Audit"):
        assert col in text, col


def test_factor_mine_template_loads_js() -> None:
    text = TPL.read_text(encoding="utf-8")
    assert "fill-scenarios.js" in text
    assert "Market-buy / limit-buy messiness" in text


def test_strategy_board_has_sleeve_and_script() -> None:
    html = sb.render(sb.collect())
    assert "fill-scenarios.js" in html
    assert "data-sleeve=" in html
    assert "Market buy" in html
    assert "Limit buy" in html
    md = sb.write_md(sb.collect())
    assert "Market buy" in md or "BOOK_FILL_REALITY" in md


def test_standalone_strategy_format_has_both_modes() -> None:
    payload = {
        "headline": "h", "note": "n",
        "misread": "Starts YES is not a daily win rate",
        "from_date": "2026-08-13", "to_date": "2026-08-14",
        "n_sessions": 2, "generated_at": "x",
        "sleeves": ["combo_sh_5050_shared"],
        "reports": {"combo_sh_5050_shared": {"columns": {
            "ideal": {"book_pct": 10.0, "win_session_pct": 60.0,
                      "n_sessions": 2, "n_sessions_green": 1,
                      "start_green": 2, "start_n": 2, "audit_ok": True,
                      "n_fills": 4, "n_miss": 0, "n_partial": 0},
            "market_mid": {"book_pct": -2.0, "win_session_pct": 40.0,
                           "n_sessions": 2, "n_sessions_green": 0,
                           "audit_ok": True, "n_fills": 3, "n_miss": 1,
                           "n_partial": 0},
            "limit_open": {"book_pct": 5.0, "win_session_pct": 50.0,
                           "n_sessions": 2, "n_sessions_green": 1,
                           "audit_ok": True, "n_fills": 2, "n_miss": 2,
                           "n_partial": 0},
        }}},
        "realities": list(bfr.REALITIES),
        "not_yet_run": [], "proxy_note": "p", "elapsed_sec": 0,
    }
    html = bfr.render_html(payload)
    assert "fillScenarioRoot" in html
    assert "Market buy" in html
    assert "Limit buy" in html
    assert "Win%" in html and "$ days" in html and "Book%" in html
    assert "market · wrong price" in html
    assert "limit @ open" in html
    assert "fill-scenarios.js" in html
    assert "flatten_robust" in html
    rows = bfr.strategy_format_rows(payload, bfr.MARKET_KEYS, bfr.MARKET_LABEL)
    assert "combo_sh_5050_shared" in rows
    assert "wrong price" in rows


def main() -> int:
    test_js_has_market_and_limit_scenarios()
    test_factor_mine_template_loads_js()
    test_strategy_board_has_sleeve_and_script()
    test_standalone_strategy_format_has_both_modes()
    print("test_fill_reality_dash: 4 ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
