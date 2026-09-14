"""combo_sh_macd_5050 paper tickets: MACD short filter + shared leftover.

Run: PYTHONPATH=. python3 -m src.test_combo_broker
"""
from __future__ import annotations

from unittest import mock

from src import combo_broker as cb
from src import factor_mine as fm
from src.futubull_exec import BrokerSnap


def _recs():
    rec_by = {r["name"]: r for r in fm.build_recipes()}
    return [rec_by["short_news_r_macd_h3"], rec_by["union_hot_n4_h1"]]


def _rows():
    return [
        {"ticker": "HOT1", "sources": ["ohlc_hot"], "boxes": {"news": "missing"},
         "alarm": False, "macd_up": False, "ohlc_hot_score": 90},
        {"ticker": "HOT2", "sources": ["ohlc_hot"], "boxes": {"news": "missing"},
         "alarm": False, "macd_up": False, "ohlc_hot_score": 80},
        {"ticker": "SH1", "sources": ["yday_gainer"], "boxes": {"news": "bad"},
         "alarm": False, "macd_up": True, "ohlc_hot_score": 1},
        {"ticker": "SH2", "sources": ["yday_gainer"], "boxes": {"news": "bad"},
         "alarm": False, "macd_up": True, "ohlc_hot_score": 1},
        {"ticker": "NOMACD", "sources": ["yday_gainer"], "boxes": {"news": "bad"},
         "alarm": False, "macd_up": False, "ohlc_hot_score": 1},
    ]


def test_macd_short_drops_news_red_without_macd() -> None:
    rec = next(r for r in _recs() if r["name"] == "short_news_r_macd_h3")
    names = [r["ticker"] for r in fm.pick_day(_rows(), rec)]
    assert "SH1" in names and "SH2" in names
    assert "NOMACD" not in names
    assert "HOT1" not in names


def test_shared_5050_sizes_longs_and_short_sells() -> None:
    recs = _recs()

    def _pick(rows, rec):
        if rec["name"] == "union_hot_n4_h1":
            return [r for r in rows if str(r["ticker"]).startswith("HOT")]
        return [r for r in rows if str(r["ticker"]).startswith("SH")]

    with mock.patch.object(cb, "quote_px", side_effect=lambda t, d, **k: 10.0), \
            mock.patch.object(fm, "pick_day", side_effect=_pick):
        tickets, skips = cb.size_combo_tickets(
            _rows(), recs, [1, 1], cash=10_000, held=set(),
            date="2026-09-14", s=1.0, combo=cb.PAPER_COMBO,
        )
    by_t = {t["ticker"]: t for t in tickets}
    assert by_t["HOT1"]["side"] == "BUY"
    assert by_t["HOT2"]["side"] == "BUY"
    assert by_t["SH1"]["side"] == "SELL"
    assert by_t["SH2"]["side"] == "SELL"
    assert "NOMACD" not in by_t
    # Heat claims first at 50% of $10k = $5k → 250 sh each @ $10.
    assert by_t["HOT1"]["shares"] == 250
    assert by_t["HOT2"]["shares"] == 250
    # Short room is leftover $5k, capped at 50% leftover = $2.5k → 125 sh.
    assert by_t["SH1"]["shares"] == 125
    assert by_t["SH2"]["shares"] == 125
    assert all(t["combo"] == cb.PAPER_COMBO for t in tickets)
    assert not any(s["kind"] == "hard_red" for s in skips)


def test_skip_held_and_hard_red_sit() -> None:
    recs = _recs()

    def _pick(rows, rec):
        if rec["name"] == "union_hot_n4_h1":
            return [r for r in rows if str(r["ticker"]).startswith("HOT")]
        return [r for r in rows if str(r["ticker"]).startswith("SH")]

    with mock.patch.object(cb, "quote_px", side_effect=lambda t, d, **k: 10.0), \
            mock.patch.object(fm, "pick_day", side_effect=_pick):
        tickets, skips = cb.size_combo_tickets(
            _rows(), recs, [1, 1], cash=10_000, held={"HOT1"},
            date="2026-09-14", s=1.0,
        )
    assert "HOT1" not in {t["ticker"] for t in tickets}
    assert any(s["ticker"] == "HOT1" and s["kind"] == "held" for s in skips)
    tickets, skips = cb.size_combo_tickets(
        _rows(), recs, [1, 1], cash=10_000, held=set(),
        date="2026-09-14", s=-3.5,
    )
    assert tickets == []
    assert any(s["kind"] == "hard_red" for s in skips)


def test_plan_marks_stale_when_look_is_asof() -> None:
    panel = {
        "to_date": "2026-09-11",
        "session_dates": ["2026-09-11"],
        "by_date": {"2026-09-11": _rows()},
    }
    snap = BrokerSnap(env="paper", cash=10_000, positions={})
    with mock.patch.object(cb, "build_look_rows", return_value=[]), \
            mock.patch.object(cb, "quote_px", side_effect=lambda t, d, **k: 10.0), \
            mock.patch.object(cb.fmb, "morning_s", return_value=1.0), \
            mock.patch.object(cb.fmb, "load_regime", return_value={}):
        card = cb.plan_combo_for_broker("2026-09-14", snap, panel=panel)
    assert card["stale"] is True
    assert card["combo"] == cb.PAPER_COMBO
    assert card["tickets"]
    assert "STALE" in card["why"]


def main() -> None:
    test_macd_short_drops_news_red_without_macd()
    test_shared_5050_sizes_longs_and_short_sells()
    test_skip_held_and_hard_red_sit()
    test_plan_marks_stale_when_look_is_asof()
    print("test_combo_broker: 4 ok")


if __name__ == "__main__":
    main()
