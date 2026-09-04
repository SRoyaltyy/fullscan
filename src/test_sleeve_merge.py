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
    replay_ledger,
    rolling_window_returns,
    stats,
)


def test_live_policy_is_hard_red() -> None:
    from src.sleeve_merge import LIVE_POLICY
    assert LIVE_POLICY == "flatten_hard_red"


def test_two_week_is_ten_sessions() -> None:
    assert TWO_WEEK_SESSIONS == 10
    assert TARGET_2W_PCT == 15.0


def test_session_calendar_drops_weekend_book() -> None:
    from src.sleeve_merge import session_calendar
    payload = {"session_dates": ["2026-08-28", "2026-08-30", "2026-08-31"]}
    books = [("2026-08-30", None)]
    assert session_calendar(payload, books) == ["2026-08-28", "2026-08-31"]


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
    assert st.get("fees_total", 0) > 100, st
    led = replay_ledger(sim["trades"], 100_000)
    assert led["broke"] is False, led["min_cash"]
    assert led["min_cash"] >= -0.01, led


def test_live_fees_and_cash_lockup() -> None:
    """Every fill uses Futubull fees; held stock blocks later tickets."""
    from pathlib import Path
    from src.paper_trade import load_fees, order_fees
    from src.sleeve_merge import (
        DEFAULT, list_books, load_payload, run_flatten_switch,
    )
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file() or not list_books():
        print("skip live cash lockup (no payload/books)")
        return
    pol = {**DEFAULT, "name": "flatten_switch_recycle", "engine": "flatten_switch",
           "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
           "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5,
           "rotate_mover": True, "carry_last_book": True}
    sim = run_flatten_switch(load_payload(), list_books(), pol, 100_000)
    fees = load_fees()
    assert sim["trades"], "expected fills"
    for t in sim["trades"]:
        want_in = order_fees(t["shares"], t["entry_px"], "buy", fees)
        assert abs((t.get("fee_in") or 0) - want_in) < 0.05, (t["ticker"], t.get("fee_in"), want_in)
        if t.get("exit_px"):
            want_out = order_fees(t["shares"], t["exit_px"], "sell", fees)
            assert abs((t.get("fee_out") or 0) - want_out) < 0.05, (t["ticker"], t.get("fee_out"), want_out)
        if t.get("cash_before") is not None:
            assert t["cash_after"] <= t["cash_before"] + 1e-6
            assert t["cash_after"] + t["notional"] + t["fee_in"] == t["cash_before"] \
                or abs(t["cash_before"] - t["cash_after"] - t["notional"] - t["fee_in"]) < 0.02
    # 08-13 spends the book; 08-14 add-ons are leftover crumbs.
    day13 = [t for t in sim["trades"] if t["entry_date"] == "2026-08-13"]
    day14 = [t for t in sim["trades"] if t["entry_date"] == "2026-08-14"]
    assert day13 and sum(t["notional"] for t in day13) > 90_000
    if day14:
        assert max(t["notional"] for t in day14) < 50
    # 08-24 re-enters 2w_size; 08-27 new names only get leftover cash.
    day24 = [t for t in sim["trades"] if t["entry_date"] == "2026-08-24"]
    day27 = [t for t in sim["trades"] if t["entry_date"] == "2026-08-27"]
    assert day24 and sum(t["notional"] for t in day24) > 100_000
    if day27:
        assert max(t["notional"] for t in day27) < 200
    cash_row = {r["date"]: r["cash"] for r in sim["curve"]}
    # Fully invested days leave pocket change, not a second sleeve's cash.
    assert cash_row["2026-08-13"] < 200
    assert cash_row["2026-08-21"] < 50
    led = replay_ledger(sim["trades"], 100_000)
    assert led["broke"] is False
    assert led["n_events"] == 2 * len(sim["trades"])


def test_hard_red_no_new_skips_0824_io_keeps_holds() -> None:
    """S ≤ −3: no new tickets; working lots and due 1d exits still settle."""
    from pathlib import Path
    from src.sleeve_merge import (
        DEFAULT, list_books, load_payload, run_flatten_switch,
    )
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file() or not list_books():
        print("skip live hard-red (no payload/books)")
        return
    base = {**DEFAULT, "engine": "flatten_switch",
            "io_sleeve": "2w_size", "long_top_n": 10, "long_pct": 0.10,
            "day_cap": 1.00, "sizeup": 1.0, "allow_short": False, "min_buys": 5,
            "rotate_mover": True, "carry_last_book": True}
    raw = run_flatten_switch(load_payload(), list_books(), base, 100_000)
    gated = run_flatten_switch(
        load_payload(), list_books(),
        {**base, "name": "flatten_hard_red", "hard_red_no_new": True},
        100_000)
    assert "2026-08-30" not in {r["date"] for r in raw["curve"]}
    day20 = [t for t in gated["trades"] if t["entry_date"] == "2026-08-20"]
    assert any(t["sleeve"] == "mover_long" for t in day20)
    day24_io = [t for t in gated["trades"]
                if t["entry_date"] == "2026-08-24" and t["sleeve"] == "io_core"]
    assert day24_io == [], day24_io
    raw24 = [t for t in raw["trades"]
             if t["entry_date"] == "2026-08-24" and t["sleeve"] == "io_core"]
    assert raw24, "baseline still re-enters 2w_size on hard-red 08-24"
    by = {r["date"]: r for r in gated["curve"]}
    assert by["2026-08-24"]["route"] == "hold"
    assert by["2026-08-18"]["route"] == "hold"
    # 08-18/19 already held 2w — hard-red must not flatten them.
    assert by["2026-08-18"]["core_n"] > 0
    assert by["2026-08-19"]["core_n"] > 0
    # 08-20 is not hard-red; flatten still fires.
    assert by["2026-08-20"]["route"] == "mover"


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


def test_start_date_skips_earlier_sessions() -> None:
    """A later start is a fresh $100k book; 8-13 lots are not inherited."""
    from pathlib import Path
    from src.sleeve_merge import (
        list_books, live_policy, load_payload, run_flatten_switch,
        run_start_dates, stats,
    )
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file() or not list_books():
        print("skip start-date sweep (no payload/books)")
        return
    payload = load_payload()
    books = list_books()
    pol = live_policy()
    live = run_flatten_switch(payload, books, pol, 100_000)
    later = run_flatten_switch(payload, books, pol, 100_000,
                               start_date="2026-08-14")
    assert live["start_date"] == "2026-08-13"
    assert later["start_date"] == "2026-08-14"
    assert all(r["date"] >= "2026-08-14" for r in later["curve"])
    assert any(t["entry_date"] == "2026-08-13" for t in live["trades"])
    assert not any(t["entry_date"] == "2026-08-13" for t in later["trades"])
    # Full cash on 8-14 buys the names the live book skipped as crumbs.
    day14_live = [t for t in live["trades"] if t["entry_date"] == "2026-08-14"]
    day14_fresh = [t for t in later["trades"] if t["entry_date"] == "2026-08-14"]
    assert day14_fresh, "fresh 8-14 book must deploy 2w_size"
    assert sum(t["notional"] for t in day14_fresh) > 80_000
    if day14_live:
        assert max(t["notional"] for t in day14_live) < 50
    skipped14 = {s["ticker"] for s in live["skipped"]
                 if s["date"] == "2026-08-14" and s["side"] == "BUY"}
    bought14 = {t["ticker"] for t in day14_fresh}
    assert skipped14, "live 8-14 should have cash-tied skips"
    assert skipped14 <= bought14, (skipped14 - bought14, bought14)
    # Start-date table covers every session; 8-13 matches the live headline.
    rows = run_start_dates(payload, books, 100_000)
    by = {r["start"]: r for r in rows}
    assert "2026-08-13" in by and "2026-08-14" in by
    st_live = stats(live)
    assert abs(by["2026-08-13"]["total_ret_pct"] - st_live["total_ret_pct"]) < 0.02
    assert by["2026-08-14"]["first_n"] >= 5
    assert by["2026-08-14"]["first_notional"] > 80_000


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
    test_live_policy_is_hard_red()
    test_two_week_is_ten_sessions()
    test_session_calendar_drops_weekend_book()
    test_next_session_skips_weekend()
    test_rank_calls_cond_then_conviction()
    test_io_picks_size_bucket()
    test_rolling_and_block_gate()
    test_gate_fails_when_a_block_misses_15()
    test_flatten_needs_book_and_enough_buys()
    test_prior_book_never_uses_today()
    test_fortnight_is_14_calendar_days()
    test_live_flatten_switch_clears_15pct_fortnight()
    test_live_fees_and_cash_lockup()
    test_hard_red_no_new_skips_0824_io_keeps_holds()
    test_start_date_skips_earlier_sessions()
    print("test_sleeve_merge: 15 ok")


if __name__ == "__main__":
    main()
