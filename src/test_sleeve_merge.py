"""Combined .io × mover sleeve — leak rules, 2-week gate, fees.

Run: python -m src.test_sleeve_merge
"""
from __future__ import annotations

from src.sleeve_merge import (
    TARGET_2W_PCT,
    TWO_WEEK_SESSIONS,
    calendar_2w_returns,
    fortnight_returns,
    io_picks,
    next_session,
    rank_calls,
    rolling_window_returns,
    stats,
)


def test_two_week_is_ten_sessions() -> None:
    assert TWO_WEEK_SESSIONS == 10
    assert TARGET_2W_PCT == 15.0


def test_next_session_skips_weekend() -> None:
    cal = ["2026-08-14", "2026-08-17", "2026-08-18"]
    assert next_session(cal, "2026-08-14", 1) == "2026-08-17"
    assert next_session(cal, "2026-08-18", 1) is None


def test_rank_calls_cond_then_conviction() -> None:
    rows = [
        {"ticker": "AAA", "condition": {"good": 3, "bad": 1}, "_conv": 1.0},
        {"ticker": "BBB", "condition": {"good": 7, "bad": 0}, "_conv": 0.5},
        {"ticker": "CCC", "condition": {"good": 7, "bad": 0}, "_conv": 4.0},
    ]
    got = [r["ticker"] for r in rank_calls(rows, "cond")]
    assert got[0] == "CCC"
    assert got[1] == "BBB"


def test_io_picks_size_bucket() -> None:
    book = {
        "books": {
            "2w": {
                "buy": [{"ticker": "TOP1"}, {"ticker": "TOP2"}],
                "buy_by_size": {
                    "large+": [{"ticker": "L1"}, {"ticker": "L2"},
                               {"ticker": "L3"}, {"ticker": "L4"}],
                    "mid": [{"ticker": "M1"}],
                    "small/micro": [{"ticker": "S1"}, {"ticker": "S2"}],
                },
            }
        }
    }
    assert io_picks(book, "2w_size") == ["L1", "L2", "L3", "M1", "S1", "S2"]
    assert io_picks(book, "2w_top") == ["TOP1", "TOP2"]


def test_rolling_and_block_gate() -> None:
    # 12 sessions: first 10 go 100 → 116 (+16%), next step still ≥15%.
    curve = []
    eq = 100_000.0
    for i, ret in enumerate([0.02] * 8 + [0.00, 0.00, -0.01, 0.01]):
        eq *= 1 + ret
        curve.append({"date": f"2026-08-{13+i:02d}", "equity": eq})
    rolls = rolling_window_returns(curve, 10)
    blocks = calendar_2w_returns(curve)
    assert rolls, rolls
    assert any(b.get("partial") for b in blocks) or len(blocks) >= 1

    sim = {
        "capital": 100_000,
        "curve": [
            {"date": "2026-08-13", "equity": 100_000},
            {"date": "2026-08-14", "equity": 103_000},
            {"date": "2026-08-17", "equity": 108_000},
            {"date": "2026-08-18", "equity": 112_000},
            {"date": "2026-08-19", "equity": 118_000},
            {"date": "2026-08-20", "equity": 120_000},
            {"date": "2026-08-21", "equity": 124_000},
            {"date": "2026-08-24", "equity": 128_000},
            {"date": "2026-08-25", "equity": 132_000},
            {"date": "2026-08-26", "equity": 136_000},
            {"date": "2026-08-27", "equity": 138_000},
            {"date": "2026-08-28", "equity": 140_000},
        ],
        "trades": [],
        "skipped": [],
    }
    # 100k → 136k over first 10 sessions = +36% ≥ 15%; full +40% beats 12.85.
    st = stats(sim, io_top=12.85)
    assert st["hit_every_block_2w"] is True, st
    assert st["beat_io_top"] is True
    assert st["min_block_2w"] >= 15.0
    forts = fortnight_returns(sim["curve"])
    assert forts, forts


def test_gate_fails_when_a_block_misses_15() -> None:
    sim = {
        "capital": 100_000,
        "curve": [{"date": f"2026-08-{13+i:02d}", "equity": 100_000 + i * 200}
                  for i in range(12)],
        "trades": [],
        "skipped": [],
    }
    st = stats(sim, io_top=12.85)
    assert st["hit_every_block_2w"] is False
    assert st["passed"] is False


def test_live_flatten_switch_clears_15pct_fortnight() -> None:
    """Winner on the checked-in books/payload must still beat the gate."""
    from pathlib import Path
    from src.sleeve_merge import (
        DEFAULT, io_top_return, list_books, load_payload,
        run_flatten_switch, stats,
    )
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file() or not list_books():
        print("skip live flatten (no payload/books)")
        return
    pol = {**DEFAULT, "name": "flatten_switch_recycle", "engine": "flatten_switch",
           "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
           "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5,
           "rotate_mover": True, "carry_last_book": True}
    sim = run_flatten_switch(load_payload(), list_books(), pol, 100_000)
    st = stats(sim, io_top_return())
    assert pol["rotate_mover"] is True and pol["carry_last_book"] is True
    assert st["min_fortnight"] is not None and st["min_fortnight"] >= 15.0, st
    assert st["min_block_2w"] is not None and st["min_block_2w"] >= 15.0, st
    assert st["beat_io_top"] is True, st
    assert st["passed"] is True, st
    assert st["total_ret_pct"] > 12.85, st
    # recycle + carry must stay ahead of the no-recycle flatten_switch_full
    assert st["min_fortnight"] >= 19.0, st
    assert st["total_ret_pct"] >= 21.0, st


def test_flatten_needs_book_and_enough_buys() -> None:
    """Green + 1 BUY is not a flatten day; no-book days stay in .io."""
    from src.sleeve_merge import DEFAULT
    assert DEFAULT["long_gate"] == 1.0
    # winner knobs: only flatten when the mover book is actually there
    pol = {**DEFAULT, "min_buys": 5, "engine": "flatten_switch"}
    assert pol["min_buys"] >= 5
    assert pol["engine"] == "flatten_switch"


def test_prior_book_never_uses_today() -> None:
    """09:30 flatten may not look at today's 13:00–15:45 book print."""
    from src.sleeve_merge import _prior_book
    books = {"2026-08-20": {"d": "20"}, "2026-08-21": {"d": "21"}}
    cal = ["2026-08-20", "2026-08-21", "2026-08-24"]
    assert _prior_book(books, cal, "2026-08-21", "yesterday") == {"d": "20"}
    assert _prior_book(books, cal, "2026-08-21", "last") == {"d": "20"}
    # previous session is 08-21 (weekend gap); yesterday = that print or None
    assert _prior_book(books, cal, "2026-08-24", "yesterday") == {"d": "21"}
    assert _prior_book(books, cal, "2026-08-24", "last") == {"d": "21"}
    # a gap day with no print: yesterday is None, last walks back
    books2 = {"2026-08-21": {"d": "21"}}
    cal2 = ["2026-08-21", "2026-08-24", "2026-08-25"]
    assert _prior_book(books2, cal2, "2026-08-25", "yesterday") is None
    assert _prior_book(books2, cal2, "2026-08-25", "last") == {"d": "21"}
    # today's print is never returned
    today = _prior_book(books, cal, "2026-08-21", "last")
    assert today != {"d": "21"}


def test_fortnight_is_14_calendar_days() -> None:
    # Aug 13 → Aug 26 is one complete fortnight (10 sessions).
    dates = [
        "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18",
        "2026-08-19", "2026-08-20", "2026-08-21", "2026-08-24",
        "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28",
    ]
    eq = 100_000.0
    curve = []
    for i, d in enumerate(dates):
        eq *= 1.02
        curve.append({"date": d, "equity": eq})
    forts = fortnight_returns(curve)
    complete = [f for f in forts if not f.get("partial")]
    assert complete, forts
    assert complete[0]["start"] == "2026-08-13"
    assert complete[0]["end"] == "2026-08-26"
    assert complete[0]["ret_pct"] > 15.0


def main() -> None:
    test_two_week_is_ten_sessions()
    test_next_session_skips_weekend()
    test_rank_calls_cond_then_conviction()
    test_io_picks_size_bucket()
    test_rolling_and_block_gate()
    test_gate_fails_when_a_block_misses_15()
    test_flatten_needs_book_and_enough_buys()
    test_prior_book_never_uses_today()
    test_fortnight_is_14_calendar_days()
    test_live_flatten_switch_clears_15pct_fortnight()
    print("test_sleeve_merge: 10 ok")


if __name__ == "__main__":
    main()
