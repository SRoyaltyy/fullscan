"""Fingerprint, grid size, and breadth math for breadth_mine_v1.

Run: PYTHONHASHSEED=0 python3 -m src.test_breadth_mine_v1
"""
from __future__ import annotations

import json

from src.breadth_mine_v1_grid import LUCK_DENOMINATOR, N, grid_counts, iter_rules
from src.breadth_mine_v1_metrics import ex_best_compound, median, raw_luck_p, ticker_shares
from src.breadth_mine_v1_protocol import (
    CHECK_CALENDAR,
    FINGERPRINT,
    MANIFEST_SHA256,
    PREREG,
    RETURNS,
    STUDY_LABEL,
    assert_prereg,
    fingerprint_sha256,
    header_fingerprint,
)
from src.breadth_mine_v1_score import Book, advance_book, assert_feature_dates, prior_end, want_exit
from src.lever_search_bars import SameDayBarError
from src.paper_trade import load_fees


def test_fingerprint_matches_covered_bytes() -> None:
    assert_prereg()
    text = PREREG.read_text(encoding="utf-8")
    assert header_fingerprint(text) == fingerprint_sha256(text) == FINGERPRINT
    assert MANIFEST_SHA256 in text
    assert STUDY_LABEL in text
    assert "57600" in text.replace(",", "") or "57,600" in text
    assert "66,990" in text


def test_grid_n_is_exact() -> None:
    counts = grid_counts()
    assert counts["atoms"] == 15
    assert counts["pairs"] == 105
    assert counts["signals"] == 120
    assert counts["cross"] == 480
    assert counts["n"] == 57600 == N
    assert counts["luck"] == 9390 + 57600 == LUCK_DENOMINATOR == 66990
    ids = [item[1] for item in iter_rules()]
    assert len(ids) == N
    assert len(set(ids)) == N
    assert ids[0].startswith("f:a09_sma50|long|time|h1m1|n1")
    assert ids[-1].startswith("p:s_gt_0+sector_up|short|trail|h5m5|n12")


def test_check_calendar_starts_20_august() -> None:
    assert CHECK_CALENDAR[0] == "2026-08-20"
    assert CHECK_CALENDAR[-1] == "2026-09-11"
    assert "2026-09-07" not in CHECK_CALENDAR
    assert len(CHECK_CALENDAR) == 16


def test_same_day_bar_is_refused() -> None:
    dates = ["2026-08-13", "2026-08-14", "2026-08-17"]
    end = prior_end(dates, "2026-08-14")
    assert end == 1
    assert all(date < "2026-08-14" for date in dates[:end])
    try:
        assert_feature_dates(["2026-08-13", "2026-08-14"], "2026-08-14")
    except SameDayBarError:
        return
    raise AssertionError("same-day bar was accepted as a feature")


def test_top1_share_rejects_above_half() -> None:
    top1, top3, best = ticker_shares({"AAA": 60.0, "BBB": 30.0, "CCC": 10.0})
    assert best == "AAA"
    assert abs(top1 - 0.60) < 1e-12
    assert abs(top3 - 1.0) < 1e-12
    assert top1 > 0.50
    top1_ok, _top3, _best = ticker_shares({"AAA": 40.0, "BBB": 35.0, "CCC": 25.0})
    assert top1_ok <= 0.50
    missing, _top3, name = ticker_shares({"AAA": 5.0, "BBB": -8.0})
    assert missing is None
    assert name == "AAA"


def test_ex_best_removes_the_runner() -> None:
    sessions = ["2026-08-20", "2026-08-21"]
    # Day 1: +10% of which AAA is the whole dollar gain. Day 2: flat.
    # Start 10000, end day1 11000, AAA pnl 1000. Removing AAA leaves 10000, return 0.
    returns = [0.10, 0.0]
    pnl = {"2026-08-20": {"AAA": 1000.0}, "2026-08-21": {"AAA": 0.0}}
    removed = ex_best_compound(sessions, sessions, returns, pnl, "AAA")
    assert removed is not None
    assert abs(removed) < 1e-9


def test_hold_one_long_is_positive_when_price_rises() -> None:
    fees = load_fees()
    book = Book()
    open1 = {"AAA": 10.0}
    close1 = {"AAA": 10.0}
    ret1, _r15, fills1, _u, entries, _pnl, _closed = advance_book(
        book, side="long", exit_name="time", hold=1, min_hold=1,
        pick_names=["AAA"], open_of=open1, close_of=close1,
        universe={"AAA"}, day_i=0, fees=fees,
    )
    assert entries == 1 and fills1 == 1
    assert "AAA" in book.pos
    open2 = {"AAA": 11.0}
    close2 = {"AAA": 11.0}
    ret2, _r15, fills2, _u, _e, pnl, closed = advance_book(
        book, side="long", exit_name="time", hold=1, min_hold=1,
        pick_names=[], open_of=open2, close_of=close2,
        universe={"AAA"}, day_i=1, fees=fees,
    )
    assert fills2 == 1
    assert "AAA" not in book.pos
    assert closed and closed[0] > 0
    assert ret2 > 0 or ret1 + ret2 > 0
    assert pnl["AAA"] > 0


def test_list_exit_waits_for_min_hold() -> None:
    lot = {"entry_px": 10.0, "extreme": 10.0}
    assert want_exit(0, 3, 2, "list", "long", 10.0, lot, False) is False
    assert want_exit(2, 3, 2, "list", "long", 10.0, lot, False) is True
    assert want_exit(2, 3, 2, "list", "long", 10.0, lot, True) is False
    assert want_exit(3, 3, 2, "time", "long", 10.0, lot, True) is True


def test_cut_loser_uses_three_percent() -> None:
    lot = {"entry_px": 10.0, "extreme": 10.0}
    assert want_exit(1, 5, 1, "cut_loser", "long", 9.70, lot, True) is True
    assert want_exit(1, 5, 1, "cut_loser", "long", 9.71, lot, True) is False
    assert want_exit(1, 5, 1, "cut_loser", "short", 10.30, lot, True) is True


def test_median_and_raw_p() -> None:
    assert median([0.02, -0.01, 0.03]) == 0.02
    assert raw_luck_p([0.01]) == 1.0
    assert raw_luck_p([0.01, 0.02, 0.015]) < 0.05


def test_scored_record_when_present() -> None:
    report = RETURNS / "REPORT.md"
    manifest = RETURNS / "manifest.jsonl"
    if not report.exists() and not manifest.exists():
        return
    text = report.read_text(encoding="utf-8")
    assert STUDY_LABEL in text
    assert "Verdict:" in text
    lines = manifest.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 31
    for line in lines:
        row = json.loads(line)
        blob = (RETURNS / row["file"]).read_bytes()
        assert __import__("hashlib").sha256(blob).hexdigest() == row["sha256"]
        day = json.loads(blob)
        assert day["n"] == N
        assert len(day["ret_futubull"]) == N


def main() -> None:
    tests = [
        test_fingerprint_matches_covered_bytes,
        test_grid_n_is_exact,
        test_check_calendar_starts_20_august,
        test_same_day_bar_is_refused,
        test_top1_share_rejects_above_half,
        test_ex_best_removes_the_runner,
        test_hold_one_long_is_positive_when_price_rises,
        test_list_exit_waits_for_min_hold,
        test_cut_loser_uses_three_percent,
        test_median_and_raw_p,
        test_scored_record_when_present,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        raise SystemExit(f"{failed} test(s) failed")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
