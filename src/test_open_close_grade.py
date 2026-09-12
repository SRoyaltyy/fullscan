"""Open-vs-close entry grading — unit tests, no network, no parquet."""
from __future__ import annotations

from src import open_close_grade as ocg
from src.paper_trade import load_fees, order_fees


CAL = [
    "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04",
]


def test_next_session_skips_nothing_and_stops() -> None:
    assert ocg.next_session(CAL, "2026-09-02") == "2026-09-03"
    assert ocg.next_session(CAL, "2026-09-04") is None
    assert ocg.next_session(CAL, "2026-01-01") is None


def test_land_bucket_preopen_session_after_close() -> None:
    assert ocg.land_bucket("2026-09-11", "2026-09-11T07:25:56-04:00") == "preopen"
    assert ocg.land_bucket("2026-09-02", "2026-09-02T15:43:57-04:00") == "session"
    assert ocg.land_bucket("2026-09-01", "2026-09-01T16:49:27-04:00") == "after_close"
    assert ocg.land_bucket("2026-08-21", "2026-08-22T03:38:49-04:00") == "after_close"
    assert ocg.land_bucket("2026-09-04", "2026-09-04T08:54:27-04:00") == "preopen"


def test_clock_return_gap_down_then_green_day() -> None:
    """9/2-shaped tape: gapped down into a green session, faded next close.

    prior close 100, open 97, close 101, next close 99.
    c2c = −1% (book convention says loss)
    o2c = +4.12% (open-entry same day says win)
    """
    bar0 = {"open": 97.0, "close": 101.0}
    bar1 = {"open": 100.0, "close": 99.0}
    c2c = ocg.clock_return(bar0, bar1, "c2c")
    o2c = ocg.clock_return(bar0, bar1, "o2c")
    o2nc = ocg.clock_return(bar0, bar1, "o2nc")
    o2o = ocg.clock_return(bar0, bar1, "o2o")
    assert c2c is not None and c2c < 0
    assert o2c is not None and o2c > 0
    assert abs(c2c - (99 / 101 - 1)) < 1e-12
    assert abs(o2c - (101 / 97 - 1)) < 1e-12
    assert abs(o2nc - (99 / 97 - 1)) < 1e-12
    assert abs(o2o - (100 / 97 - 1)) < 1e-12


def test_clock_return_needs_next_bar() -> None:
    bar0 = {"open": 10.0, "close": 11.0}
    assert ocg.clock_return(bar0, None, "c2c") is None
    assert abs(ocg.clock_return(bar0, None, "o2c") - 0.1) < 1e-12
    assert ocg.clock_return({"open": None, "close": 11.0}, None, "o2c") is None


def test_pick_hot4_drops_alarm_and_takes_top4() -> None:
    rows = [
        {"ticker": "AAA", "ohlc_hot_score": 9, "alarm": True, "sources": ["union"]},
        {"ticker": "BBB", "ohlc_hot_score": 8, "alarm": False, "sources": ["yday_gainer"]},
        {"ticker": "CCC", "ohlc_hot_score": 7, "alarm": False, "sources": ["flatten"]},
        {"ticker": "DDD", "ohlc_hot_score": 6, "alarm": False, "sources": ["ohlc_hot"]},
        {"ticker": "EEE", "ohlc_hot_score": 5, "alarm": False, "sources": ["union"]},
        {"ticker": "FFF", "ohlc_hot_score": 4, "alarm": False, "sources": ["union"]},
    ]
    got = [r["ticker"] for r in ocg.pick_hot4(rows)]
    assert got == ["BBB", "CCC", "DDD", "EEE"]
    assert "AAA" not in got


def test_uses_paper_trade_fee_helper() -> None:
    fees = load_fees()
    # $2,500 / $10 = 250 shares. Helper must match paper_trade.order_fees.
    drag = ocg.fee_drag_order(10.0, 10.1, fees, slice_usd=2500.0)
    assert drag is not None and drag > 0
    buy = order_fees(250, 10.0, "buy", fees)
    sell = order_fees(250, 10.1, "sell", fees)
    assert abs(drag - (buy + sell) / 2500.0) < 1e-9
    assert ocg.FEE_RT == 0.0015


def test_eight_of_ten_o2c_win_c2c_loss() -> None:
    """Mechanical 8/10: open-entry wins, close-to-close says losses."""
    store = {}
    tickers = [f"T{i:02d}" for i in range(10)]
    for i, t in enumerate(tickers):
        if i < 8:
            store[("2026-09-02", t)] = {"open": 97.0, "close": 101.0}
            store[("2026-09-03", t)] = {"open": 100.0, "close": 99.0}
        else:
            store[("2026-09-02", t)] = {"open": 100.0, "close": 98.0}
            store[("2026-09-03", t)] = {"open": 97.0, "close": 102.0}
    days = [{"date": "2026-09-02", "tickers": tickers, "land": "session"}]
    fees = load_fees()
    graded = ocg.grade_picks(days, CAL, store, fees)
    assert graded["o2c"]["raw"]["hits"] == 8
    assert graded["c2c"]["raw"]["hits"] == 2
    assert graded["o2c"]["raw"]["hit_rate"] == 0.8
    assert graded["c2c"]["raw"]["hit_rate"] == 0.2


def test_flip_board_yes_and_no() -> None:
    def pack(book_c2c, book_o2c, hot_c2c, hot_o2c):
        def side(c2c, o2c):
            return {
                "c2c": {"raw": {"n": 10, "hits": int(10 * c2c), "hit_rate": c2c, "mean": 0.0}},
                "o2c": {"raw": {"n": 10, "hits": int(10 * o2c), "hit_rate": o2c, "mean": 0.0}},
                "o2nc": {"raw": {"n": 10, "hits": 5, "hit_rate": 0.5, "mean": 0.0}},
                "o2o": {"raw": {"n": 10, "hits": 5, "hit_rate": 0.5, "mean": 0.0}},
            }
        return ocg.flip_board(side(book_c2c, book_o2c), side(hot_c2c, hot_o2c))

    yes = pack(0.43, 0.40, 0.40, 0.55)
    assert yes["ranking_flips"] is True
    assert yes["verdict"].startswith("YES")
    assert yes["cells"]["c2c"]["mean_winner"] == "tie"

    no = pack(0.43, 0.80, 0.50, 0.85)
    assert no["ranking_flips"] is False
    assert no["verdict"].startswith("NO")
    assert no["book_crosses_50"] is True


def test_recommend_book_stays_c2c_hot4_o2c() -> None:
    days = [
        {"date": "2026-09-02", "land": "session"},
        {"date": "2026-09-11", "land": "preopen"},
        {"date": "2026-09-01", "land": "after_close"},
    ]
    flip = {"verdict": "NO — placeholder"}
    rec = ocg.recommend(days, flip)
    assert rec["book_1d"]["default"] == "c2c"
    assert rec["hot4"]["default"] == "o2c"
    assert rec["hot4"]["alt_fill"] == "o2o"
    assert rec["book_1d"]["land_counts"]["session"] == 1


def test_no_yfinance_import_in_module_source() -> None:
    src = (ocg.ROOT / "src" / "open_close_grade.py").read_text(encoding="utf-8")
    assert "import yfinance" not in src
    assert "yf.download" not in src
    assert "no live wire" in (ocg.__doc__ or "").lower()


if __name__ == "__main__":
    test_next_session_skips_nothing_and_stops()
    test_land_bucket_preopen_session_after_close()
    test_clock_return_gap_down_then_green_day()
    test_clock_return_needs_next_bar()
    test_pick_hot4_drops_alarm_and_takes_top4()
    test_uses_paper_trade_fee_helper()
    test_eight_of_ten_o2c_win_c2c_loss()
    test_flip_board_yes_and_no()
    test_recommend_book_stays_c2c_hot4_o2c()
    test_no_yfinance_import_in_module_source()
    print("10 open-close-grade tests passed")
