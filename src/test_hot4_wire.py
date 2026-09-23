"""Webull HOT4 tickets must match Factor Mine cash-start, not Clock-B.

Run: PYTHONPATH=. python3 -m src.test_hot4_wire
"""
from __future__ import annotations

import gzip
import json
from datetime import datetime, timedelta

from src import factor_mine as fm
from src import morning_scan as ms
from src import strategy_tickets as st

ROOT = st.ROOT
CASH_START = {
    "2026-09-21": ["FEAM", "TJGC", "LVWR", "SECZ"],
    "2026-09-22": ["SECZ", "GRAL", "NUAI", "INDP"],
}
CLOCK_B_PREPARE = ["DELL", "GME", "UMC", "VSTS"]


def _cash_start_bought() -> dict[str, list[str]]:
    path = ROOT / "03_scoreboard" / "factor_mine" / "shards" / "starts.json.gz"
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        starts = json.load(handle)
    out: dict[str, list[str]] = {}
    for row in starts[st.HOT4_WIRE]:
        out[str(row.get("start") or "")[:10]] = list(row.get("bought") or [])
    return out


def test_published_hot4_matches_cash_start() -> None:
    """09-21 / 09-22 ticket emission equals cash-start bought, not Clock-B."""
    bought = _cash_start_bought()
    raw = json.loads((st.PANEL).read_text(encoding="utf-8"))
    panel = fm.rehydrate_panel(raw)
    rec = next(r for r in fm.build_recipes() if r["name"] == st.HOT4_WIRE)
    for date, want in CASH_START.items():
        assert bought[date] == want, bought[date]
        emitted = st.recipe_strats(date)
        hot = next(r for r in emitted if r["name"] == st.HOT4_WIRE)
        got = [b["ticker"] for b in hot["buy"]]
        assert got == want, (date, got)
        assert hot.get("methodology") == st.HOT4_WIRE_METHODOLOGY
        assert hot.get("picker") == "pick_day"
        assert hot.get("status") == "ok"
        clock_b = [
            r["ticker"] for r in ms.pick_morning(ms.aisle_rows(date, panel["by_date"][date]), rec)
        ]
        assert clock_b != want
        assert got != clock_b
        st.assert_hot4_wire(date, hot["buy"], panel=panel)


def test_gate_refuses_clock_b_prepare_list() -> None:
    with_raise = False
    try:
        st.assert_hot4_wire(
            "2026-09-21",
            [{"ticker": t, "side": "long"} for t in CLOCK_B_PREPARE],
        )
    except ValueError as exc:
        with_raise = True
        text = str(exc)
        assert "diverge" in text
        assert "DELL" in text
        assert "FEAM" in text
    assert with_raise, "expected HOT4 wire refuse"


def test_gate_accepts_cash_start_list() -> None:
    for date, want in CASH_START.items():
        got = st.assert_hot4_wire(
            date, [{"ticker": t, "side": "long"} for t in want])
        assert got == want


def test_paper_open_and_webull_refuse_divergent_submit() -> None:
    from src.futubull_exec import BrokerSnap
    from src import paper_open as po
    from src import webull_exec as we

    date = "2026-09-21"
    clock = datetime.fromisoformat(date + "T09:29:00-04:00")
    payload = {
        "date": date,
        "decision_readiness": {
            "ready": True,
            "fingerprint": "hot4-wire",
            "completed_at": (clock - timedelta(minutes=2)).isoformat(),
        },
        "look": {"stale": False, "source": "panel"},
        "strategies": {
            we.HOT4: {
                "date": date,
                "status": "ok",
                "s": 12.87,
                "buy": [
                    {"ticker": t, "side": "long", "px": 10}
                    for t in CLOCK_B_PREPARE
                ],
            }
        },
    }
    snap = BrokerSnap(env="paper", cash=10_000, positions={}, connected=True)
    try:
        po.make_plan(payload, snap, clock)
    except ValueError as exc:
        assert "diverge" in str(exc)
    else:
        raise AssertionError("paper_open accepted a Clock-B HOT4 list")

    class Alive:
        env = "paper"
        host = we.PAPER_HOST
        err = None

        def connect(self):
            return True

        def snapshot(self):
            return snap

        def place(self, *args, **kwargs):
            raise AssertionError("divergent HOT4 must not place")

        def place_batch(self, *args, **kwargs):
            raise AssertionError("divergent HOT4 must not place")

    from unittest import mock
    card = {
        "date": date,
        "stale": False,
        "policy": we.HOT4,
        "tickets": [{
            "side": "BUY", "ticker": "DELL", "shares": 1, "px": 10.0,
            "date": date, "status": "plan",
        }],
        "would_buy": {"rows": [
            {"ticker": t, "side": "long"} for t in CLOCK_B_PREPARE
        ]},
        "hard_red": False,
    }
    with mock.patch.object(we, "PaperAPI", return_value=Alive()), \
            mock.patch.object(we, "_plan", return_value=card), \
            mock.patch.object(we, "write_last") as wrote:
        rc = we.run(date, submit=True, write=True, source="hot4")
    assert rc == 2
    last = wrote.call_args[0][0]
    assert last["submit"] is False
    assert last["sent"] == []
    assert "diverge" in (last.get("error") or "")


def _cash_book_days() -> dict[str, dict]:
    path = ROOT / "03_scoreboard" / "factor_mine" / "shards" / "daily.json.gz"
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        daily = json.load(handle)
    return {d["date"]: d for d in daily[st.HOT4_WIRE]}


def test_published_sells_match_cash_book() -> None:
    """Every continuous-book exit, including 09-22 FEAM/TJGC/LVWR, is the wire."""
    book = _cash_book_days()
    raw = json.loads((st.PANEL).read_text(encoding="utf-8"))
    panel = fm.rehydrate_panel(raw)
    for date, day in book.items():
        want = list(day.get("sold") or [])
        assert st.hot4_recipe_sells(date, panel=panel) == want, (date, want)
    assert book["2026-09-21"]["bought"] == ["FEAM", "TJGC", "LVWR", "SECZ"]
    assert book["2026-09-22"]["sold"] == ["FEAM", "TJGC", "LVWR"]
    assert book["2026-08-18"]["hard_red"] is True
    assert book["2026-08-18"]["bought"] == []
    emitted = {}
    for date in ("2026-09-22", "2026-08-18"):
        hot = next(r for r in st.recipe_strats(date) if r["name"] == st.HOT4_WIRE)
        emitted[date] = hot
        got = [s["ticker"] for s in hot["sell"]]
        assert got == list(book[date]["sold"]), (date, got)
        assert all(s.get("side") == "long" for s in hot["sell"])
        assert all(s.get("src") == "list-drop" for s in hot["sell"])
        st.assert_hot4_wire(date, hot["buy"], sells=hot["sell"], panel=panel)
    sit = emitted["2026-08-18"]
    assert sit.get("sit") is True
    assert sit.get("status") == "sit"
    assert [s["ticker"] for s in sit["sell"]] == ["XHG", "STDN", "HTFL"]


def test_min_hold_blocks_list_drop_and_never_sells_unheld() -> None:
    """Inside min-hold a drop is not a sell. A name never bought is never sold."""
    d1, d2, d3 = "2026-09-01", "2026-09-02", "2026-09-03"

    def row(date, ticker, score):
        return {
            "date": date, "ticker": ticker,
            "sources": ["yday_gainer", "yday_mover"],
            "ohlc_hot_score": score, "alarm": False, "boxes": {},
        }

    by_date = {
        d1: [row(d1, "AAA", 10), row(d1, "BBB", 9)],
        d2: [row(d2, "BBB", 9), row(d2, "EEE", 8)],
        d3: [row(d3, "DDD", 10)],
    }
    panel = {
        "to_date": d3,
        "session_dates": [d1, d2, d3],
        "by_date": by_date,
        "rows": by_date[d1] + by_date[d2] + by_date[d3],
    }
    rec = next(r for r in fm.build_recipes() if r["name"] == st.HOT4_WIRE)
    scores = {d1: 1.0, d2: 1.0, d3: 1.0}
    hold1 = dict(rec, hold=1)
    assert st.hot4_recipe_sells(d1, panel=panel, rec=hold1, scores=scores) == []
    assert st.hot4_recipe_sells(d2, panel=panel, rec=hold1, scores=scores) == ["AAA"]
    assert "EEE" not in st.hot4_recipe_sells(d2, panel=panel, rec=hold1, scores=scores)
    assert "ZZZ" not in st.hot4_recipe_sells(d3, panel=panel, rec=hold1, scores=scores)
    hold2 = dict(rec, hold=2)
    assert st.hot4_recipe_sells(d2, panel=panel, rec=hold2, scores=scores) == []
    assert st.hot4_recipe_sells(d3, panel=panel, rec=hold2, scores=scores) == ["AAA", "BBB"]


def test_gate_refuses_divergent_sells() -> None:
    with_raise = False
    try:
        st.assert_hot4_wire(
            "2026-09-22",
            [{"ticker": t, "side": "long"} for t in CASH_START["2026-09-22"]],
            sells=[{"ticker": t, "side": "long"} for t in ("DELL", "GME", "UMC")],
        )
    except ValueError as exc:
        with_raise = True
        text = str(exc)
        assert "sells" in text and "diverge" in text
        assert "DELL" in text and "FEAM" in text
    assert with_raise, "expected HOT4 sell refuse"
    accepted = ["FEAM", "TJGC", "LVWR"]
    st.assert_hot4_wire(
        "2026-09-22",
        [{"ticker": t, "side": "long"} for t in CASH_START["2026-09-22"]],
        sells=[{"ticker": t, "side": "long"} for t in accepted],
    )


def test_clock_b_aisle_cannot_change_hot4_sells() -> None:
    """Oppset ∪ Clock-B may rank DELL first. HOT4 still sells the cash book."""
    from unittest import mock
    real = ms.aisle_rows

    def widen(date, panel_rows, index=None):
        return real(date, panel_rows, index=index) + [{
            "date": date, "ticker": "DELL", "sources": ["oppset"],
            "oppset": True, "ohlc_hot_score": 999, "alarm": False,
            "boxes": {}, "erd_days_since_E": 1, "erd_flag_E": 1,
            "last_green": True,
        }]

    with mock.patch.object(ms, "aisle_rows", side_effect=widen):
        emitted = st.recipe_strats("2026-09-22")
    hot = next(r for r in emitted if r["name"] == st.HOT4_WIRE)
    assert [s["ticker"] for s in hot["sell"]] == ["FEAM", "TJGC", "LVWR"]
    assert "DELL" not in [b["ticker"] for b in hot["buy"]]
    assert "DELL" not in [s["ticker"] for s in hot["sell"]]


def test_in_memory_recipe_is_hot_score_top_n() -> None:
    rows = [
        {"date": "2026-09-21", "ticker": "FEAM", "sources": ["yday_gainer", "yday_mover"],
         "ohlc_hot_score": 10.9, "alarm": False, "boxes": {}},
        {"date": "2026-09-21", "ticker": "TJGC", "sources": ["yday_gainer"],
         "ohlc_hot_score": 8.8, "alarm": False, "boxes": {}},
        {"date": "2026-09-21", "ticker": "LVWR", "sources": ["yday_mover"],
         "ohlc_hot_score": 7.8, "alarm": False, "boxes": {}},
        {"date": "2026-09-21", "ticker": "SECZ", "sources": ["yday_gainer"],
         "ohlc_hot_score": 7.3, "alarm": False, "boxes": {}},
        {"date": "2026-09-21", "ticker": "NOPE", "sources": ["ohlc_hot"],
         "ohlc_hot_score": 1.0, "alarm": False, "boxes": {}},
    ]
    panel = {"to_date": "2026-09-21", "by_date": {"2026-09-21": rows}}
    got = st.hot4_recipe_tickers("2026-09-21", panel=panel)
    assert got == ["FEAM", "TJGC", "LVWR", "SECZ"]


def test_oppset_aisle_cannot_widen_hot4_emission() -> None:
    """Clock-B aisle may append a name; the HOT4 wire still uses the panel."""
    from unittest import mock
    rows = [
        {"date": "2026-09-21", "ticker": "FEAM", "sources": ["yday_gainer", "yday_mover"],
         "ohlc_hot_score": 10.9, "alarm": False, "boxes": {}},
        {"date": "2026-09-21", "ticker": "TJGC", "sources": ["yday_gainer"],
         "ohlc_hot_score": 8.8, "alarm": False, "boxes": {}},
        {"date": "2026-09-21", "ticker": "LVWR", "sources": ["yday_mover"],
         "ohlc_hot_score": 7.8, "alarm": False, "boxes": {}},
        {"date": "2026-09-21", "ticker": "SECZ", "sources": ["yday_gainer"],
         "ohlc_hot_score": 7.3, "alarm": False, "boxes": {}},
    ]
    panel = {
        "to_date": "2026-09-21",
        "by_date": {"2026-09-21": rows},
        "rows": rows,
    }
    real_aisle = ms.aisle_rows

    def widen(date, panel_rows, index=None):
        return real_aisle(date, panel_rows, index=index) + [{
            "date": date, "ticker": "DELL", "sources": ["oppset"],
            "oppset": True, "ohlc_hot_score": 999, "alarm": False,
            "boxes": {}, "erd_days_since_E": 1, "erd_flag_E": 1,
            "last_green": True,
        }]

    with mock.patch.object(st, "_load_json", return_value=panel), \
            mock.patch.object(ms, "aisle_rows", side_effect=widen):
        emitted = st.recipe_strats("2026-09-21")
    hot = next(r for r in emitted if r["name"] == st.HOT4_WIRE)
    assert [b["ticker"] for b in hot["buy"]] == ["FEAM", "TJGC", "LVWR", "SECZ"]
    assert [s["ticker"] for s in hot["sell"]] == []
    assert "DELL" not in [s["ticker"] for s in hot["sell"]]
    assert hot.get("methodology") == "factor_mine_recipe"


def main() -> None:
    test_in_memory_recipe_is_hot_score_top_n()
    test_min_hold_blocks_list_drop_and_never_sells_unheld()
    test_oppset_aisle_cannot_widen_hot4_emission()
    test_gate_refuses_clock_b_prepare_list()
    test_gate_accepts_cash_start_list()
    test_gate_refuses_divergent_sells()
    test_published_hot4_matches_cash_start()
    test_published_sells_match_cash_book()
    test_clock_b_aisle_cannot_change_hot4_sells()
    test_paper_open_and_webull_refuse_divergent_submit()
    print("test_hot4_wire: ok")


if __name__ == "__main__":
    main()
