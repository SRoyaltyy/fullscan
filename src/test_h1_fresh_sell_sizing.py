"""10-20 case: stale 10-08 sell marks oversized the BYRN buy (503 sh vs $51 book cash).

Fresh sell prices here are illustrative pre-open quotes below the stale marks.
"""
import json
import pytest

from src import h1_sealed_exec as h

FEES = h._load_fees()
HELD = {
    "KOD": {"ticker": "KOD", "shares": 27, "last_px": 90.01},
    "PENG": {"ticker": "PENG", "shares": 34, "last_px": 72.5},
    "SDEV": {"ticker": "SDEV", "shares": 1222, "last_px": 2.47},
}
PLAN = {
    "cash_before": 18.41070278877669,
    "planned_sells": [
        {"ticker": "KOD", "shares": 27, "reason": "hold-expired"},
        {"ticker": "PENG", "shares": 34, "reason": "hold-expired"},
        {"ticker": "SDEV", "shares": 1222, "reason": "hold-expired"},
    ],
}
PICKS = [
    {"ticker": "SLN", "rank": 1, "fv_price": 19.58, "fv_avg_volume": 1201.72},
    {"ticker": "ARQT", "rank": 3, "fv_price": 31.89, "fv_avg_volume": 2278.84},
    {"ticker": "BYRN", "rank": 4, "fv_price": 5.17, "fv_avg_volume": 609.04},
]
FRESH = {"KOD": 70.0, "PENG": 60.0, "SDEV": 1.9}


def _cost(rows):
    return sum(r["shares"] * r["px"] * (1 + h.SLIP_PRIMARY)
               + h._order_fees(r["shares"], r["px"], "buy", FEES) for r in rows)


def test_stale_marks_reproduce_the_1020_oversize():
    _, stale_cash, _ = h._plan_sells(PLAN, HELD, FEES, "2026-10-20")
    stale = h._size_new_buys(PICKS, stale_cash, FEES, "2026-10-20")
    assert {r["ticker"]: r["shares"] for r in stale}["BYRN"] == 503
    _, real_cash, _ = h._plan_sells(PLAN, HELD, FEES, "2026-10-21", FRESH)
    with pytest.raises(h.SealedH1Error, match="refusing"):
        h.assert_buys_within_cash(stale, real_cash, FEES, "2026-10-21")


def test_fresh_prices_size_buys_within_real_cash():
    _, cash, _ = h._plan_sells(PLAN, HELD, FEES, "2026-10-21", FRESH)
    sized = h._size_new_buys(PICKS, cash, FEES, "2026-10-21")
    assert _cost(sized) <= cash + 1e-6
    assert {r["ticker"]: r["shares"] for r in sized}["BYRN"] < 503
    h.assert_buys_within_cash(sized, cash, FEES, "2026-10-21")


def test_fresh_above_stale_is_capped_at_stale():
    hi = {"KOD": 999.0, "PENG": 999.0, "SDEV": 99.0}
    _, a, _ = h._plan_sells(PLAN, HELD, FEES, "2026-10-21", hi)
    _, b, _ = h._plan_sells(PLAN, HELD, FEES, "2026-10-20")
    assert a == pytest.approx(b)


def test_missing_fresh_price_fails_closed():
    with pytest.raises(h.SealedH1Error, match="no fresh pre-open price for sell SDEV"):
        h._plan_sells(PLAN, HELD, FEES, "2026-10-21", {"KOD": 70.0, "PENG": 60.0})


def test_preopen_price_file(tmp_path):
    (tmp_path / "2026-10-21_preopen_px.json").write_text(
        json.dumps({"date": "2026-10-21", "prices": {"kod": 70, "SDEV": "bad"}}))
    assert h.load_preopen_prices("2026-10-21", root=tmp_path) == {"KOD": 70.0}
    assert h.load_preopen_prices("2026-10-22", root=tmp_path) is None
    (tmp_path / "2026-10-23_preopen_px.json").write_text(json.dumps({"date": "2026-10-20"}))
    with pytest.raises(h.SealedH1Error):
        h.load_preopen_prices("2026-10-23", root=tmp_path)
