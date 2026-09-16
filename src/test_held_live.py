"""OPEN held lots + live-session cash-start. No network.

Run: PYTHONPATH=. python3 -m src.test_held_live
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

from src import elite_live_px as elp
from src import factor_mine as fm
from src import held_live as hl


def test_open_held_lots_skips_closed_and_lookers() -> None:
    payload = {
        "books": {
            "combo_sh_5050_shared": {
                "open": [
                    {"ticker": "INDP", "side": "long", "shares": 40, "entry_px": 2.80},
                ],
                "trades": [
                    {"date": "2026-09-14", "ticker": "LOOK", "side": "BUY",
                     "shares": 10, "price": 5},
                    {"date": "2026-09-15", "ticker": "LOOK", "side": "SELL",
                     "shares": 10, "price": 6},
                ],
            }
        }
    }
    lots = hl.open_held_lots(payload, "combo_sh_5050_shared")
    assert [x["ticker"] for x in lots] == ["INDP"]
    assert lots[0]["shares"] == 40
    assert lots[0]["entry"] == 2.80
    replay = hl.replay_open_lots(payload["books"]["combo_sh_5050_shared"]["trades"])
    assert replay == []


def test_open_held_lots_from_last_daily_marks() -> None:
    payload = {
        "books": {"butterfly": {"trades": []}},
        "daily": {
            "butterfly": [{
                "date": "2026-09-15",
                "marks": [
                    {"ticker": "AAA", "shares_close": 12, "entry_px": 10, "side": "long"},
                    {"ticker": "BBB", "shares_close": 0, "entry_px": 8},
                ],
            }]
        },
    }
    lots = hl.open_held_lots(payload, "butterfly")
    assert [x["ticker"] for x in lots] == ["AAA"]
    assert lots[0]["shares"] == 12


def test_stamp_payload_holds_soft_fails_without_auth() -> None:
    payload = {
        "featured": ["combo_sh_5050_shared"],
        "live_session": "2026-09-16",
        "books": {
            "combo_sh_5050_shared": {
                "open": [{"ticker": "INDP", "side": "long", "shares": 10, "entry_px": 2.8}],
            }
        },
    }
    book = {
        "date": "2026-09-16",
        "prices": {"INDP": 3.18},
        "src": "session_export+no_elite_auth",
        "at": "2026-09-16T12:22:00-04:00",
        "n": 1,
        "error": "no_elite_auth",
    }
    with mock.patch.object(elp, "quote_book", return_value=book), \
            mock.patch.object(elp, "official_opens", return_value={"INDP": 3.0}), \
            mock.patch.object(fm, "live_session_date", return_value="2026-09-16"):
        hl.stamp_payload_holds(payload, pull_live=False)
    rec = payload["held_live"]
    assert rec["src"] != "elite_live"
    assert "not live" in rec["banner"] or "session export" in rec["banner"]
    lot = rec["sleeves"]["combo_sh_5050_shared"][0]
    assert lot["px"] == 3.18
    assert lot["pnl"] == 3.80
    assert payload["quote"]["src"] == rec["src"]


def test_live_px_workflow_soft_fails() -> None:
    yml = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "live_px.yml"
    text = yml.read_text(encoding="utf-8")
    assert "src.held_live" in text
    assert "export.ashx" not in text  # Overview pull lives in elite_live_px
    assert "|| true" in text or "exit 0" in text
    assert "flatten_robust" not in text.split("name:")[1][:80] or "does not" in text
    assert "webull" not in text.lower() or "does not" in text
    assert "FINVIZ_EMAIL" in text
    assert "FINVIZ_AUTH" in text


def main() -> None:
    test_open_held_lots_skips_closed_and_lookers()
    test_open_held_lots_from_last_daily_marks()
    test_stamp_payload_holds_soft_fails_without_auth()
    test_live_px_workflow_soft_fails()
    print("ok")


if __name__ == "__main__":
    main()
