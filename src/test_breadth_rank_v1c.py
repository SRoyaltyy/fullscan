"""Checks for the fingerprinted breadth_rank_v1c protocol and the book rules."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from src.breadth_rank_v1c_bars import (
    SCHEMA,
    assert_split_consistent,
    load_bars,
    session_jumps,
    split_explains,
    unexplained,
)
from src.breadth_rank_v1c_grid import (
    LUCK_DENOMINATOR,
    MIN_CLOSED,
    N,
    RANDOM4_DRAWS,
    RANDOM4_N,
    RANDOM4_SEED,
    WIN_MIN,
    iter_rules,
)
from src.breadth_rank_v1c_protocol import (
    FIT_CUTOFF,
    FINGERPRINT,
    OOS_START,
    RANK_END,
    fingerprint_sha256,
)
from src.breadth_rank_v1c_score import (
    BarView,
    Book,
    advance_book,
    percentile_ranks,
    red_day_sit,
)
from src.factor_mine_book import HARD_RED
from src.paper_trade import load_fees


def test_fingerprint_matches() -> None:
    assert fingerprint_sha256() == FINGERPRINT


def test_counts() -> None:
    assert N == 40
    assert N <= 50
    assert len(list(iter_rules())) == 40
    assert LUCK_DENOMINATOR == 239910
    assert MIN_CLOSED == 30
    assert WIN_MIN == 0.55
    assert RANDOM4_SEED == 20260813
    assert RANDOM4_DRAWS == 1000
    assert RANDOM4_N == 4
    assert RANK_END == "2026-09-11"
    assert FIT_CUTOFF == "2026-09-13"
    assert OOS_START == "2026-09-14"


def test_long_compound_outranks_a_richer_short() -> None:
    from src.breadth_rank_v1c_score import _rank_rows, freeze_payload

    ordered = _rank_rows([
        {"id": "b|long|time|h1m1|n4", "compound": 0.05, "ex_best": 0.90},
        {"id": "a|long|time|h1m1|n4", "compound": 0.10, "ex_best": 0.01},
    ])
    assert [row["id"] for row in ordered] == [
        "a|long|time|h1m1|n4",
        "b|long|time|h1m1|n4",
    ]
    rules = list(iter_rules())
    n = len(rules)
    ret_f = [[0.0] for _ in range(n)]
    for index, rid, _basket, _atoms, side, _top in rules:
        if rid.endswith("|long|time|h1m1|n4") and rid.startswith("sma|"):
            ret_f[index] = [0.20]
        if side == "short":
            ret_f[index] = [0.90]
    series = {
        "entries": [[1] for _ in range(n)],
        "first": [{} for _ in range(n)],
        "pnl": [[{} for _ in range(n)]],
        "ret_15": [[0.0] for _ in range(n)],
        "ret_f": ret_f,
        "sessions": ["2026-08-13"],
        "trades": [[[0.01] for _ in range(n)]],
    }
    # pnl is stored per day as a list of rule maps. _slice indexes [rule][day].
    series["pnl"] = [[{}] for _ in range(n)]
    series["trades"] = [[[0.01]] for _ in range(n)]
    frozen = freeze_payload(series)
    assert frozen["rank1"] == "sma|long|time|h1m1|n4"
    assert frozen["rank1"].split("|")[1] == "long"
    assert frozen["shorts"][0]["compound"] == 0.9
    assert frozen["rows"][0]["compound"] == 0.2


def test_percentile_ties_share_the_average_rank() -> None:
    ranks = percentile_ranks(np.array([1.0, 1.0, 3.0]))
    assert abs(ranks[0] - 0.5) < 1e-12
    assert abs(ranks[1] - 0.5) < 1e-12
    assert abs(ranks[2] - 1.0) < 1e-12


def test_red_day_uses_factor_mine_guard() -> None:
    assert red_day_sit(float(HARD_RED), "long") is True
    assert red_day_sit(float(HARD_RED), "short") is True
    assert red_day_sit(float(HARD_RED) + 0.01, "long") is False
    assert red_day_sit(None, "short") is False


def test_missing_bar_closes_and_does_not_carry() -> None:
    fees = load_fees()
    book = Book()
    first = advance_book(
        book, side="long", pick_names=["AAA"],
        open_of={"AAA": 10.0}, close_of={"AAA": 11.0},
        day_i=0, fees=fees, final_session=False,
    )
    assert first["n_entries"] == 1
    assert "AAA" in book.pos
    second = advance_book(
        book, side="long", pick_names=["AAA"],
        open_of={}, close_of={},
        day_i=1, fees=fees, final_session=False,
    )
    assert book.pos == {}
    assert second["exits"][0]["reason"] == "missing_bar"
    assert second["exits"][0]["ticker"] == "AAA"
    third = advance_book(
        book, side="long", pick_names=[],
        open_of={}, close_of={},
        day_i=2, fees=fees, final_session=False,
    )
    assert third["ret_f"] == 0.0
    assert third["pnl"] == {}
    assert book.pos == {}


def test_window_end_closes_a_same_day_lot() -> None:
    fees = load_fees()
    book = Book()
    result = advance_book(
        book, side="long", pick_names=["AAA"],
        open_of={"AAA": 10.0}, close_of={"AAA": 12.0},
        day_i=0, fees=fees, final_session=True,
    )
    assert book.pos == {}
    assert result["exits"][0]["reason"] == "window_end"
    assert result["trade_rets"][0] > 0


def test_time_exit_sells_at_the_next_open() -> None:
    fees = load_fees()
    book = Book()
    advance_book(
        book, side="long", pick_names=["AAA"],
        open_of={"AAA": 10.0}, close_of={"AAA": 10.5},
        day_i=0, fees=fees, final_session=False,
    )
    result = advance_book(
        book, side="long", pick_names=[],
        open_of={"AAA": 13.0}, close_of={"AAA": 13.0},
        day_i=1, fees=fees, final_session=False,
    )
    assert result["exits"][0]["reason"] == "time"
    assert book.pos == {}


def test_red_day_buys_nothing() -> None:
    fees = load_fees()
    book = Book()
    assert red_day_sit(-4.0, "long") is True
    result = advance_book(
        book, side="long", pick_names=[],
        open_of={"AAA": 10.0}, close_of={"AAA": 11.0},
        day_i=0, fees=fees, final_session=False,
    )
    assert result["n_entries"] == 0
    assert book.pos == {}


def test_prior_bars_exclude_the_session() -> None:
    view = BarView({
        "AAA": [
            {"date": "2026-09-10", "open": 1, "high": 9, "low": 1, "close": 1, "volume": 1},
            {"date": "2026-09-11", "open": 2, "high": 50, "low": 1, "close": 3, "volume": 1},
        ]
    })
    prior = view.prior_bars("AAA", "2026-09-11")
    assert [bar["date"] for bar in prior] == ["2026-09-10"]
    opx, cpx = view.session_open_close("AAA", "2026-09-11")
    assert opx == 2
    assert cpx == 3


def test_loader_drops_bars_after_the_fit_cutoff(tmp_path: Path | None = None) -> None:
    folder = tmp_path or Path("/tmp/breadth_rank_v1c_bars")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "ohlc.parquet"
    rows = [
        ("2026-09-11", "AAA", 1.0),
        ("2026-09-14", "AAA", 9.0),
        ("2026-09-25", "AAA", 8.0),
    ]
    table = pa.table(
        {
            "date": [row[0] for row in rows],
            "ticker": [row[1] for row in rows],
            "open": [row[2] for row in rows],
            "high": [row[2] for row in rows],
            "low": [row[2] for row in rows],
            "close": [row[2] for row in rows],
            "volume": [1.0 for _ in rows],
        },
        schema=SCHEMA,
    )
    pq.write_table(table, path)
    clipped = load_bars(path, max_date=FIT_CUTOFF)
    assert [bar["date"] for bar in clipped["AAA"]] == ["2026-09-11"]
    full = load_bars(path)
    assert "2026-09-14" in [bar["date"] for bar in full["AAA"]]


def test_split_explanation_and_real_snapshot() -> None:
    assert split_explains(0.5, 2.0) is True
    assert split_explains(4.0, 2.0) is False
    flags = session_jumps(
        {"AAA": [
            {"date": "2026-09-10", "open": 10.0, "close": 10.0},
            {"date": "2026-09-11", "open": 2.5, "close": 2.5},
        ]},
        {("AAA", "2026-09-11"): 4.0},
    )
    assert flags[0]["explained"] is True
    assert unexplained(flags) == []
    bare = session_jumps(
        {"BBB": [
            {"date": "2026-09-10", "open": 10.0, "close": 10.0},
            {"date": "2026-09-11", "open": 50.0, "close": 50.0},
        ]},
        {},
    )
    assert len(unexplained(bare)) == 1
    intraday = session_jumps(
        {"CCC": [{"date": "2026-09-11", "open": 10.0, "close": 40.0}]},
        {},
    )
    assert intraday[0]["leg"] == "close_over_open"
    assert len(unexplained(intraday)) == 1
    assert_split_consistent()


def main() -> None:
    tests = [
        test_fingerprint_matches,
        test_counts,
        test_long_compound_outranks_a_richer_short,
        test_percentile_ties_share_the_average_rank,
        test_red_day_uses_factor_mine_guard,
        test_missing_bar_closes_and_does_not_carry,
        test_window_end_closes_a_same_day_lot,
        test_time_exit_sells_at_the_next_open,
        test_red_day_buys_nothing,
        test_prior_bars_exclude_the_session,
        test_loader_drops_bars_after_the_fit_cutoff,
        test_split_explanation_and_real_snapshot,
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
