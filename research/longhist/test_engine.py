"""Small checks for the long-history book. No market score."""
from __future__ import annotations

import numpy as np

from research.longhist.engine import (
    Book, Closed, Rule, Tape, build_panel, cluster_test, fee_model,
    holm, look_list, simulate, ymd,
)
from research.longhist.membership import proxy_members


def _tape(closes, volumes, opens=None, highs=None, lows=None, start=20190102):
    n = len(closes)
    dates = np.arange(start, start + n, dtype=np.int32)
    # Dates above are not calendar dates. Tests pass an explicit session list.
    o = np.array(opens if opens is not None else closes, dtype=float)
    h = np.array(highs if highs is not None else np.array(closes) * 1.01, dtype=float)
    low = np.array(lows if lows is not None else np.array(closes) * 0.99, dtype=float)
    return Tape(dates, o, h, low, np.array(closes, float), np.array(volumes, float))


def test_holm_is_monotone():
    adj = holm([0.01, 0.04, 0.03])
    assert abs(adj[0] - 0.03) < 1e-12
    assert abs(adj[2] - 0.06) < 1e-12
    assert abs(adj[1] - 0.06) < 1e-12


def test_flat_positive_cluster_is_zero_p():
    trades = [
        Closed("A", "2019-01-02", "2019-01-03", 1, 10, 11, 0, 0, 1, 0.1),
        Closed("B", "2019-01-04", "2019-01-07", 1, 10, 11, 0, 0, 1, 0.1),
    ]
    got = cluster_test(trades)
    assert got["p"] == 0.0
    assert got["mean"] == 0.1


def test_panel_matches_membership_and_ignores_today_close():
    closes = [10.0] * 25
    volumes = [200_000.0] * 23 + [500_000.0, 100_000_000.0]
    opens = [10.0] * 25
    # Session D is the last bar. Open gaps up. Close and volume are poisoned.
    opens[-1] = 11.0
    closes[-1] = 500.0
    dates = [f"2019-01-{d:02d}" for d in range(1, 26)]
    # ymd of those dates must match the tape dates below.
    tape_dates = np.array([ymd(day) for day in dates], dtype=np.int32)
    base = _tape(closes, volumes, opens)
    base.dates = tape_dates
    other = _tape([80.0] * 25, [200_000.0] * 25, [80.0] * 25)
    other.dates = tape_dates.copy()
    tapes = {"KEEP": base, "DROP": other}
    session = dates[-1]
    panel = build_panel(tapes, [session])
    bars = {}
    for ticker, tape in tapes.items():
        bars[ticker] = [{
            "date": dates[j],
            "open": float(tape.o[j]),
            "high": float(tape.h[j]),
            "low": float(tape.l[j]),
            "close": float(tape.c[j]),
            "volume": float(tape.v[j]),
        } for j in range(len(dates))]
    assert [row.ticker for row in panel[0]] == proxy_members(bars, session)
    assert panel[0][0].ticker == "KEEP"
    # Today's close is 500 and today's volume is huge. Membership still holds.
    assert proxy_members(bars, session) == ["KEEP"]


def test_book_pnl_matches_fee_formula():
    fees = fee_model()
    dates = [f"2019-01-{d:02d}" for d in range(1, 24)]
    tape_dates = np.array([ymd(day) for day in dates], dtype=np.int32)
    closes = [10.0] * 22 + [12.0]
    volumes = [200_000.0] * 21 + [500_000.0, 200_000.0]
    opens = [10.0] * 21 + [11.0, 12.0]
    tape = _tape(closes, volumes, opens)
    tape.dates = tape_dates
    # Two sessions: entry day (index 21) and exit day (index 22), hold 1.
    sessions = dates[-2:]
    # Rebuild a one-day panel by hand so the test does not depend on rvol.
    from research.longhist.engine import Row
    row = Row("AAA", 0.1, 2.0, True, True, True, 1.0, 1.0, 1.0, 1.0, 11.0)
    rule = Rule("longhist_break10_h1", 1, "break10", "hot_score", 4)
    book = simulate([[row], []], {"AAA": tape}, sessions, rule, fees)
    assert len(book.trades) == 1
    trade = book.trades[0]
    pnl = trade.shares * (trade.exit_px - trade.entry_px) - trade.buy_fee - trade.sell_fee
    assert abs(trade.pnl - pnl) < 1e-9
    assert trade.entry_px == 11.0
    assert trade.exit_px == 12.0
    assert book.buy_dates == [(sessions[0], "AAA")]


def test_banned_name_does_not_change_the_original_list():
    from research.longhist.engine import Row
    rows = [
        Row("BBB", 0.2, 2.0, True, True, False, 1, 1, 3, 0, 10),
        Row("AAA", 0.2, 2.0, True, True, False, 1, 1, 5, 0, 10),
    ]
    rule = Rule("r", 1, "break10", "hot_score", 1)
    assert look_list(rows, rule, None)[0].ticker == "AAA"
    assert look_list(rows, rule, "AAA")[0].ticker == "BBB"


if __name__ == "__main__":
    test_holm_is_monotone()
    test_flat_positive_cluster_is_zero_p()
    test_panel_matches_membership_and_ignores_today_close()
    test_book_pnl_matches_fee_formula()
    test_banned_name_does_not_change_the_original_list()
    print("engine tests ok")
