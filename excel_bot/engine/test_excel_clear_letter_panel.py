"""Clock lock + leak checks for the Theme Radar CLEAR letter panel."""
from __future__ import annotations

import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import SAME_ROW_LEAK_ABORT, assert_feature_legal  # noqa: E402
from excel_clear_letter_panel import (  # noqa: E402
    CSV_PATH, HEADER, HOLIDAYS, OPEN_TALLIES, PANEL_DATES,
    expected_prior_session, leak_check, prior_feature_bars, prior_is_fresh,
    row_from_prior,
)
from excel_open_features import df_pattern, open_features  # noqa: E402
from join_post_813 import is_session  # noqa: E402


def test_leak_check_pass():
    assert leak_check() == "PASS"
    for col in SAME_ROW_LEAK_ABORT:
        try:
            assert_feature_legal("value", col, 0)
        except ValueError:
            pass
        else:
            raise AssertionError(f"same-row {col} must abort")
        assert_feature_legal("value", col, 1)
    for col in OPEN_TALLIES:
        assert_feature_legal("value", col, 0)
    assert_feature_legal("value", "DF", 1)


def test_panel_dates_skip_labor_day_and_listed_weekends_are_mornings():
    assert "2026-09-07" in HOLIDAYS
    assert "2026-09-07" not in PANEL_DATES
    assert "2026-08-30" in PANEL_DATES  # Sunday Theme Radar morning
    assert "2026-09-06" in PANEL_DATES
    assert is_session("2026-08-30") is False
    assert is_session("2026-09-07") is True  # weekday calendar; holiday via HOLIDAYS


def test_df_lag1_is_prior_bar_not_same_row():
    prior = [
        {"o": 10.0, "h": 10.4, "l": 9.7, "c": 10.1, "v": 2e6, "date": "2026-08-12"},
        {"o": 10.1, "h": 10.25, "l": 9.0, "c": 10.2, "v": 2e6, "date": "2026-08-13"},
        {"o": 10.2, "h": 10.3, "l": 10.0, "c": 10.05, "v": 2e6, "date": "2026-08-14"},
        {"o": 10.0, "h": 10.1, "l": 9.8, "c": 9.9, "v": 2e6, "date": "2026-08-17"},
        {"o": 9.9, "h": 10.0, "l": 9.7, "c": 9.85, "v": 2e6, "date": "2026-08-18"},
        {"o": 10.0, "h": 10.25, "l": 9.0, "c": 10.2, "v": 3e6, "date": "2026-08-19"},
    ]
    rec = row_from_prior("2026-08-20", "AAA", prior)
    assert rec is not None
    assert rec["DF_lag1"] == df_pattern(prior[-1])
    assert rec["DF_lag1"] == "Bullish Hammer"
    assert rec["_prior_date"] == "2026-08-19"
    # today's fake H/L/C must not change letters
    xl = open_features(
        [{k: b[k] for k in ("o", "h", "l", "c", "v")} for b in prior],
        99.0,
    )
    assert xl["df"] == rec["DF_lag1"]
    assert xl["FQ"] == rec["FQ"]
    assert xl["same_row_df"] is False


def test_same_row_bar_in_prior_aborts():
    prior = [
        {"o": 10.0, "h": 10.4, "l": 9.7, "c": 10.1, "v": 2e6, "date": "2026-08-12"},
        {"o": 10.1, "h": 11.0, "l": 10.0, "c": 10.8, "v": 2e6, "date": "2026-08-13"},
        {"o": 10.2, "h": 10.3, "l": 10.0, "c": 10.05, "v": 2e6, "date": "2026-08-14"},
        {"o": 10.0, "h": 10.1, "l": 9.8, "c": 9.9, "v": 2e6, "date": "2026-08-17"},
        {"o": 9.9, "h": 10.0, "l": 9.7, "c": 9.85, "v": 2e6, "date": "2026-08-18"},
        {"o": 9.8, "h": 10.0, "l": 9.5, "c": 9.6, "v": 3e6, "date": "2026-08-20"},
    ]
    try:
        row_from_prior("2026-08-20", "AAA", prior)
    except ValueError as e:
        assert "LEAK" in str(e)
    else:
        raise AssertionError("same-row bar must abort")


def test_prior_feature_bars_cut_same_row():
    hist = {
        "AAA": [
            ("2026-08-19", {"o": 1, "h": 2, "l": 1, "c": 1.5, "v": 1e6}),
            ("2026-08-20", {"o": 9, "h": 99, "l": 1, "c": 50, "v": 9e9}),
        ]
    }
    prior = prior_feature_bars(hist, "AAA", "2026-08-20")
    assert [b["date"] for b in prior] == ["2026-08-19"]
    assert prior[0]["h"] == 2


def test_expected_prior_skips_weekend_and_labor_day():
    assert expected_prior_session("2026-08-17") == "2026-08-14"
    assert expected_prior_session("2026-08-30") == "2026-08-28"
    assert expected_prior_session("2026-08-31") == "2026-08-28"
    assert expected_prior_session("2026-09-08") == "2026-09-04"
    assert expected_prior_session("2026-09-09") == "2026-09-08"
    assert prior_is_fresh("2026-08-27", "2026-08-26")
    assert prior_is_fresh("2026-08-27", "2026-08-25")  # documented fallback
    assert not prior_is_fresh("2026-09-09", "2026-09-04")


def test_stale_prior_is_not_emitted_as_lag1():
    prior = [
        {"o": 10.0, "h": 10.4, "l": 9.7, "c": 10.1, "v": 2e6, "date": "2026-08-28"},
        {"o": 10.1, "h": 10.2, "l": 9.9, "c": 10.0, "v": 2e6, "date": "2026-08-31"},
        {"o": 10.0, "h": 10.1, "l": 9.8, "c": 9.9, "v": 2e6, "date": "2026-09-01"},
        {"o": 9.9, "h": 10.0, "l": 9.7, "c": 9.85, "v": 2e6, "date": "2026-09-02"},
        {"o": 9.8, "h": 9.9, "l": 9.6, "c": 9.7, "v": 2e6, "date": "2026-09-03"},
        {"o": 9.7, "h": 9.8, "l": 9.5, "c": 9.6, "v": 2e6, "date": "2026-09-04"},
    ]
    assert row_from_prior("2026-09-08", "AAA", prior) is not None  # 09-04 is correct
    assert row_from_prior("2026-09-09", "AAA", prior) is None  # missing 09-08


def test_no_flatten_import():
    path = os.path.join(HERE, "excel_clear_letter_panel.py")
    text = open(path, encoding="utf-8").read()
    assert "flatten_robust" in text
    assert "from flatten" not in text
    assert "import flatten" not in text


def test_csv_header_if_present():
    if not os.path.isfile(CSV_PATH):
        return
    with open(CSV_PATH, encoding="utf-8") as fh:
        recs = csv.DictReader(fh)
        assert tuple(recs.fieldnames) == HEADER
        n = 0
        dates = set()
        for rec in recs:
            n += 1
            dates.add(rec["date"])
            assert rec["ticker"]
            # DF_lag1 is text; must not look like a same-row numeric leak col
            assert "DF" not in rec or rec.get("DF") is None
            assert "BB" not in rec
            assert "BQ" not in rec
            assert "core_score" not in rec
            if n >= 200:
                break
        assert n >= 1
        assert dates <= set(PANEL_DATES)


if __name__ == "__main__":
    test_leak_check_pass()
    test_panel_dates_skip_labor_day_and_listed_weekends_are_mornings()
    test_df_lag1_is_prior_bar_not_same_row()
    test_same_row_bar_in_prior_aborts()
    test_prior_feature_bars_cut_same_row()
    test_expected_prior_skips_weekend_and_labor_day()
    test_stale_prior_is_not_emitted_as_lag1()
    test_no_flatten_import()
    test_csv_header_if_present()
    print("ok")
