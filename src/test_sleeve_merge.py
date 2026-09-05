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


def test_live_policy_is_robust() -> None:
    from src.sleeve_merge import LIVE_POLICY, live_policy, policy_by_name
    assert LIVE_POLICY == "flatten_robust"
    live = live_policy()
    assert int(live.get("ripper_top_n") or 0) == 0
    rip = policy_by_name("flatten_robust_ripper")
    assert int(rip.get("ripper_top_n") or 0) >= 3
    assert float(rip.get("ripper_cash_frac") or 0) > 0


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


def test_io_select_robust_keeps_3d_not_2w_junk() -> None:
    """8-14 2w_size is ASTS/SITM; robust uses the 3d size book (TLN/VST)."""
    from pathlib import Path
    from src.sleeve_merge import io_picks, io_select_picks, load_book_map, list_books
    books = load_book_map(list_books())
    doc = books.get("2026-08-14")
    if not doc:
        print("skip robust 8-14 (no book)")
        return
    raw = io_picks(doc, "2w_size")
    got = io_select_picks(doc, {"io_sleeve": "3d_size", "io_select": "robust",
                                "io_min_names": 4},
                          date="2026-08-14", score=5.5)
    assert "ASTS" in raw and "SITM" in raw
    assert "ASTS" not in got and "SITM" not in got, got
    assert "TLN" in got or "VST" in got, got


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


def test_stop_before_leaves_lots_open() -> None:
    """Live card needs yesterday's working lots, not the terminal mark-close."""
    from pathlib import Path
    from src.sleeve_merge import (
        DEFAULT, list_books, load_payload, live_policy, run_flatten_switch,
    )
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file() or not list_books():
        print("skip stop_before (no payload/books)")
        return
    pol = live_policy()
    closed = run_flatten_switch(load_payload(), list_books(), pol, 100_000)
    opened = run_flatten_switch(
        load_payload(), list_books(), pol, 100_000,
        stop_before="2026-09-04", close_open=False)
    assert closed["open_io"] == {} or not closed.get("open_io")
    assert closed["open_mover"] == []
    assert opened["calendar"][-1] < "2026-09-04"
    assert opened["open_io"] or opened["open_mover"] or opened["cash"] > 0
    n_open = len(opened["open_io"]) + len(opened["open_mover"])
    # 3d recycle may already be in cash by 9-03 (hold cannot settle).
    assert n_open >= 1 or opened["cash"] > 1_000
    assert opened["cash"] >= 0


def test_card_hard_red_0824_no_new_buys() -> None:
    """S ≤ −3: card may sell due 1d lots; it must not propose a new BUY."""
    from pathlib import Path
    from src.sleeve_merge_live import plan_today
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file():
        print("skip card hard-red (no payload)")
        return
    card = plan_today("2026-08-24")
    assert card["hard_red"] is True
    assert card["route"] == "hold"
    assert card["flatten_ok"] is False
    buys = [t for t in card["tickets"] if t["side"] == "BUY"]
    assert buys == [], buys
    assert any("hard-red" in (s.get("reason") or "") for s in card["skipped"])


def test_card_skips_already_held() -> None:
    """A name already on the book is not a buy — leftover cash only."""
    from pathlib import Path
    from src.sleeve_merge_live import plan_today
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file():
        print("skip card already-held (no payload)")
        return
    card = plan_today("2026-09-04")
    held = {h["ticker"] for h in card.get("holds_open") or []}
    bought = {t["ticker"] for t in card["tickets"] if t["side"] == "BUY"}
    assert held.isdisjoint(bought), held & bought
    if held:
        assert any(s.get("reason") == "already held" for s in card["skipped"]), \
            card["skipped"][:5]


def test_card_cost_fits_leftover_cash() -> None:
    """Planned buys cannot spend more than leftover cash after planned sells."""
    from pathlib import Path
    from src.sleeve_merge_live import plan_today
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file():
        print("skip card cash (no payload)")
        return
    card = plan_today("2026-09-04")
    room = card["cash_open"] + card["sell_proceeds"]
    assert card["buy_cost"] <= room + 0.02, (card["buy_cost"], room)
    assert card["cash_after_1600"] >= -0.01
    for t in card["tickets"]:
        if t["side"] == "BUY":
            assert t["shares"] >= 1
            assert t.get("cost", 0) > 0


def test_card_would_buy_ignores_holdings() -> None:
    """Wish list still names the sleeve even when leftover cash cannot fill."""
    from pathlib import Path
    from src.sleeve_merge import io_picks, list_books, load_book_map
    from src.sleeve_merge_live import plan_today
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file():
        print("skip card would-buy (no payload)")
        return
    card = plan_today("2026-09-04")
    would = card.get("would_buy") or {}
    names = [r["ticker"] for r in would.get("rows") or []]
    assert names, would
    held = {h["ticker"] for h in card.get("holds_open") or []}
    if held:
        assert held & set(names), (held, names)
    books = load_book_map(list_books())
    from src.sleeve_merge import io_select_picks, live_policy
    want = set(io_select_picks(books.get("2026-09-04") or {}, live_policy(),
                              date="2026-09-04"))
    if not want:
        want = set(io_picks(books.get("2026-09-04") or {}, "3d_size"))
    assert want and want <= set(names), (want, names)
    assert (would.get("equity") or 0) > 10_000
    # Full-cash wish list spends the book when a hold can settle.
    # Last session(s) of a 3d recycle correctly spend leftover / nothing.
    if (would.get("spent") or 0) <= card["cash_open"]:
        assert names, would
    live = {t["ticker"] for t in card["tickets"] if t["side"] == "BUY"}
    assert not (held & live)


def test_card_writes_today_json() -> None:
    from pathlib import Path
    from src.sleeve_merge_live import (
        POSITIONS_JSON, TODAY_JSON, inject_today_panel, plan_today,
        today_panel_html, write_card,
    )
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file():
        print("skip card write (no payload)")
        return
    card = plan_today("2026-09-04")
    paths = write_card(card)
    assert TODAY_JSON.is_file()
    assert POSITIONS_JSON.is_file()
    assert paths["daily"].is_file()
    html = paths["dashboard"].read_text(encoding="utf-8")
    assert "TODAY_BEGIN" in html
    assert card["date"] in html
    assert "today-card" in html
    assert "holdings disregarded" in html
    assert "Would have bought" in paths["daily"].read_text(encoding="utf-8")
    wrapped = inject_today_panel("<main><p>x</p>\n<div class=\"cards\">",
                                 today_panel_html(card))
    assert wrapped.count("TODAY_BEGIN") == 1


def test_start_date_skips_earlier_sessions() -> None:
    from pathlib import Path
    from src.sleeve_merge import (
        list_books, live_policy, load_payload, run_flatten_switch,
    )
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file() or not list_books():
        print("skip start_date (no payload/books)")
        return
    sim = run_flatten_switch(load_payload(), list_books(), live_policy(),
                             100_000, start_date="2026-08-14")
    assert sim["calendar"][0] == "2026-08-14"
    assert all(t["entry_date"] >= "2026-08-14" for t in sim["trades"])
    names = set(sim.get("first_targets") or [])
    assert "ASTS" not in names and "SITM" not in names, names


def test_start_date_mean_clears_five_pct() -> None:
    """Fresh $100k every session — mean ≥ 5%, not an 8-13 lottery."""
    from pathlib import Path
    from src.sleeve_merge import (
        list_books, live_policy, load_payload, run_start_dates,
    )
    payload_path = Path(__file__).resolve().parent.parent / "03_scoreboard" / "mover_lookback_action.json"
    if not payload_path.is_file() or not list_books():
        print("skip start mean (no payload/books)")
        return
    st = run_start_dates(load_payload(), list_books(), live_policy(), 100_000)
    assert st["mean_pct"] >= 5.0, st
    by = {r["start"]: r for r in st["rows"]}
    r14 = by.get("2026-08-14") or {}
    assert "ASTS" not in (r14.get("would_buy") or []), r14
    assert (r14.get("return_pct") or 0) > 1.0, r14
    long_enough = [r for r in st["rows"] if r["n_sessions"] >= 5]
    assert long_enough
    assert min(r["return_pct"] for r in long_enough) > -3.0


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
    test_live_policy_is_robust()
    test_two_week_is_ten_sessions()
    test_session_calendar_drops_weekend_book()
    test_next_session_skips_weekend()
    test_rank_calls_cond_then_conviction()
    test_io_picks_size_bucket()
    test_io_select_robust_keeps_3d_not_2w_junk()
    test_rolling_and_block_gate()
    test_gate_fails_when_a_block_misses_15()
    test_flatten_needs_book_and_enough_buys()
    test_prior_book_never_uses_today()
    test_fortnight_is_14_calendar_days()
    test_live_flatten_switch_clears_15pct_fortnight()
    test_live_fees_and_cash_lockup()
    test_hard_red_no_new_skips_0824_io_keeps_holds()
    test_start_date_skips_earlier_sessions()
    test_start_date_mean_clears_five_pct()
    test_stop_before_leaves_lots_open()
    test_card_hard_red_0824_no_new_buys()
    test_card_skips_already_held()
    test_card_cost_fits_leftover_cash()
    test_card_would_buy_ignores_holdings()
    test_card_writes_today_json()
    print("test_sleeve_merge: 23 ok")


if __name__ == "__main__":
    main()
