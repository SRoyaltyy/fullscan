"""No-lookahead and determinism tests for the Excel ML lever.

Run: python research/lever_search/excel_ml_lever/test_excel_ml_lever.py
"""
from __future__ import annotations

import importlib.util
import json
import tempfile
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent


def load_mod():
    path = HERE / "excel_ml_lever.py"
    spec = importlib.util.spec_from_file_location("excel_ml_lever", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


M = load_mod()

SESSIONS = [
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18",
    "2026-08-19", "2026-08-20", "2026-08-21", "2026-08-24",
    "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28",
]


def _fail(msg: str) -> None:
    raise AssertionError(msg)


def test_training_clock_is_n_minus_2() -> None:
    day = SESSIONS[10]  # 2026-08-27, index 10
    entries = M.training_entry_dates(SESSIONS, day)
    if entries[-1] != SESSIONS[8]:
        _fail(f"last training entry {entries[-1]} is not N-2 {SESSIONS[8]}")
    if SESSIONS[9] in entries:
        _fail("N-1 is in the training set")
    if day in entries:
        _fail("decision day is in the training set")
    for entry in entries:
        exit_day = M.next_session(SESSIONS, entry)
        if exit_day is None or exit_day >= day:
            _fail(f"exit {exit_day} of {entry} is not known before {day}")
    M.assert_no_lookahead(SESSIONS, day, entries)
    if M.training_entry_dates(SESSIONS, SESSIONS[0]) != []:
        _fail("first session must not train on a negative slice")
    if M.training_entry_dates(SESSIONS, SESSIONS[1]) != []:
        _fail("second session has no completed exit yet")


def test_rank_is_symmetric_and_centers_missing() -> None:
    ranked = M.rank_center([1.0, None, 3.0, 3.0])
    if ranked[1] != 0.0:
        _fail(f"missing rank {ranked[1]} is not the center")
    if ranked[2] != ranked[3]:
        _fail("ties did not share a rank")
    pair = M.rank_center([1.0, 2.0])
    if abs(pair[0] + pair[1]) > 1e-12:
        _fail(f"two-name ranks are not symmetric: {pair}")
    if M.rank_center([5.0]) != [0.0]:
        _fail("a lone name must sit at 0")


def test_price_features_ignore_today_and_the_future() -> None:
    bars = [
        {"date": "2026-08-13", "open": 10, "close": 11, "volume": 100},
        {"date": "2026-08-14", "open": 12, "close": 13, "volume": 200},
        {"date": "2026-08-17", "open": 99, "close": 1, "volume": 9_999},
        {"date": "2026-09-14", "open": 1, "close": 1, "volume": 1},
    ]
    feat = M.price_features_from_bars(bars, "2026-08-17")
    # Prior bars are 08-13 and 08-14 only. Today's 99 open must not enter.
    if abs(feat["px_ret1"] - (13 / 11 - 1)) > 1e-12:
        _fail(f"ret1 used a future or same-day close: {feat}")
    if abs(feat["px_gap_prior"] - (12 / 11 - 1)) > 1e-12:
        _fail(f"gap used today's open: {feat}")
    if M.open_on(bars, "2026-09-14") is not None:
        _fail("cutoff open was readable")
    if M.open_on(bars, "2026-08-17") != 99:
        _fail("same-day open is the fill and should still be readable")


def test_suggestion_clock() -> None:
    suggestions = {
        "AAA": [
            {"signal_date": "2026-08-14", "side": "long"},
            {"signal_date": "2026-08-17", "side": "long"},
            {"signal_date": "2026-09-25", "side": "short"},
        ]
    }
    # Morning 08-17 may see the 08-14 confirm (prior session) only.
    today = M.suggestion_features("AAA", "2026-08-14", suggestions)
    if today["excel_sugg_long"] != 1.0 or today["excel_sugg_n"] != 1.0:
        _fail(f"prior confirm missing: {today}")
    same_day = M.suggestion_features("AAA", "2026-08-16", suggestions)
    if same_day["excel_sugg_long"] != 0.0 or same_day["excel_sugg_n"] != 0.0:
        _fail(f"a non-prior signal leaked: {same_day}")
    # signal_date == decision day is not the prior session, so it is absent.
    leaked = M.suggestion_features("AAA", None, suggestions)
    if leaked["excel_sugg_long"] is not None:
        _fail("first session should not invent a suggestion")


def test_excluded_columns_are_not_features() -> None:
    banned = {
        "open", "open_0930", "close", "headline", "current_price",
        "ret_vs_close", "ret_vs_open", "run_date", "signal_colors",
    }
    found = banned & set(M.FEATURES)
    if found:
        _fail(f"excluded columns are features: {found}")
    if "box_ab" not in M.FEATURES:
        _fail("AB camera is missing")
    if "px_ret20" not in M.FEATURES or "px_gap_prior" not in M.FEATURES:
        _fail("price features missing")
    if M.THEME_RADAR_ENABLED:
        _fail("theme radar hook must stay off")
    if any(name.startswith("theme_") for name in M.FEATURES):
        _fail("theme radar column is on")


def _synthetic():
    sessions = [f"2026-01-{day:02d}" for day in range(5, 21)]
    # 16 weekdays-looking dates. Enough for the minimum of 8 resolved sessions.
    panel = {}
    bars = {"AAA": [], "BBB": [], "CCC": [], "DDD": [], "EEE": []}
    names = list(bars)
    for index, day in enumerate(sessions):
        rows = []
        for offset, ticker in enumerate(names):
            # A rising hot score for AAA so the fit has a slope.
            hot = float(offset + index)
            rows.append({
                "date": day,
                "ticker": ticker,
                "src_rank": offset,
                "ohlc_hot_score": hot,
                "boxes": {"ab": "good" if ticker == "AAA" else "bad"},
                "alarm": False,
                "last_green": True,
            })
            # Opens climb. Exit is the next session's open.
            bars[ticker].append({
                "date": day,
                "open": 10.0 + index + offset * 0.1,
                "close": 10.0 + index + offset * 0.1,
                "volume": 1000 + offset,
            })
            # History before the first session so ret1 exists.
        panel[day] = rows
    for ticker in names:
        bars[ticker].insert(0, {
            "date": "2026-01-02", "open": 9.0, "close": 9.5, "volume": 800,
        })
    # Plant a future bar and a future panel row. They must not move the series.
    bars["AAA"].append({
        "date": "2026-09-14", "open": 1.0, "close": 100.0, "volume": 1,
    })
    return sessions, panel, bars


def test_walk_does_not_train_on_unknown_exits_and_is_deterministic() -> None:
    sessions, panel, bars = _synthetic()
    # Shrink the minimum so this short synthetic calendar can pick.
    old_sessions = M.MIN_RESOLVED_SESSIONS
    old_rows = M.MIN_TRAIN_ROWS
    M.MIN_RESOLVED_SESSIONS = 4
    M.MIN_TRAIN_ROWS = 8
    try:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            first = M.walk(sessions, panel, bars, fees=_fees(), state_dir=state)
            second = M.walk(sessions, panel, bars, fees=_fees(), state_dir=state)
        if first["daily"] != second["daily"]:
            _fail("two walks disagreed")
        # Locked files were reused on the second walk (no mismatch raise).
        picked = [row for row in first["daily"] if row["n"]]
        if not picked:
            _fail("synthetic book never picked")
        for row in first["daily"]:
            entries = M.training_entry_dates(sessions, row["date"])
            M.assert_no_lookahead(sessions, row["date"], entries)
            if row["date"] >= M.CUTOFF:
                _fail("cutoff day was scored")
        # A name's training target for the decision day must not use that day's exit.
        day = picked[-1]["date"]
        items = M.training_items(panel, sessions, day, letters={}, suggestions={}, bars_by_ticker=bars)
        for item in items:
            if item["exit"] >= day:
                _fail(f"training exit {item['exit']} is not before {day}")
            if item["entry"] not in M.training_entry_dates(sessions, day):
                _fail("training item outside the allowed entries")
    finally:
        M.MIN_RESOLVED_SESSIONS = old_sessions
        M.MIN_TRAIN_ROWS = old_rows


def test_future_panel_row_is_dropped() -> None:
    doc = {
        "session_dates": ["2026-08-13", "2026-09-11", "2026-09-14"],
        "rows": [
            {"date": "2026-08-13", "ticker": "AAA"},
            {"date": "2026-09-14", "ticker": "FUTURE"},
            {"date": "2026-09-11", "ticker": "BBB"},
        ],
    }
    sessions, by_day = M.filter_panel(doc)
    if "2026-09-14" in sessions:
        _fail("cutoff session kept")
    names = {row["ticker"] for rows in by_day.values() for row in rows}
    if "FUTURE" in names:
        _fail("future row kept")
    if names != {"AAA", "BBB"}:
        _fail(f"panel filter changed the window: {names}")


def test_cutoff_range_is_refused() -> None:
    try:
        M.walk(["2026-09-14"], {}, {}, fees=_fees(), end="2026-09-14")
    except RuntimeError:
        pass
    else:
        _fail("scoring the cutoff must fail")


def test_tie_break_is_ticker_ascending() -> None:
    scored = [
        {"ticker": "ZZZ", "score": 1.0},
        {"ticker": "MMM", "score": 1.0},
        {"ticker": "AAA", "score": 1.0},
    ]
    bars = {
        "ZZZ": [{"date": "2026-08-13", "open": 10, "close": 10, "volume": 1}],
        "MMM": [{"date": "2026-08-13", "open": 10, "close": 10, "volume": 1}],
        "AAA": [{"date": "2026-08-13", "open": 10, "close": 10, "volume": 1}],
    }
    picks = M.choose_picks(scored, bars, "2026-08-13", set())
    if [row["ticker"] for row in picks] != ["AAA", "MMM", "ZZZ"]:
        _fail(f"tie break {picks}")


def test_missing_open_is_skipped() -> None:
    scored = [
        {"ticker": "AAA", "score": 3.0},
        {"ticker": "BBB", "score": 2.0},
    ]
    bars = {
        "BBB": [{"date": "2026-08-13", "open": 10, "close": 10, "volume": 1}],
    }
    picks = M.choose_picks(scored, bars, "2026-08-13", set())
    if [row["ticker"] for row in picks] != ["BBB"]:
        _fail(f"missing bar was bought: {picks}")


def test_fees_match_the_schedule_and_borrow_is_separate() -> None:
    fees = _fees()
    # 100 shares at $20. Commission min 0.99 binds (0.49 < 0.99), cap is $10.
    # Platform min $1 binds (0.50 < 1). Settlement 0.30.
    buy = M.order_fees(100, 20.0, "buy", fees)
    if abs(buy - (0.99 + 1.0 + 0.30)) > 1e-9:
        _fail(f"buy fee {buy}")
    sell = M.order_fees(100, 20.0, "sell", fees)
    # reg max(0.000008*2000, 0.01) = 0.016, taf max(0.0166, 0.01) = 0.0166
    expect = 0.99 + 1.0 + 0.30 + 0.016 + 0.0166
    if abs(sell - round(expect, 4)) > 1e-9:
        _fail(f"sell fee {sell} != {expect}")
    if M.borrow_fee(1000) != 3.0:
        _fail("borrow is not 0.3%")
    if M.HOLD != 1:
        _fail("hold 2 was added without a spec change")


def test_ridge_is_deterministic() -> None:
    rng = np.random.default_rng(M.SEED)
    ranks = rng.normal(size=(40, 5))
    target = ranks @ np.array([0.2, -0.1, 0.0, 0.05, 0.0]) + 0.01
    items = [{"rank": list(row), "y": float(y)} for row, y in zip(ranks, target)]
    a = M.fit_model(items)
    b = M.fit_model(items)
    if not np.allclose(a["coef"], b["coef"]):
        _fail("ridge coefs moved")
    scores_a = M.predict(a, items)
    scores_b = M.predict(b, items)
    if scores_a != scores_b:
        _fail("scores moved")


def test_published_window_stops_at_09_11() -> None:
    path = HERE / "outputs" / "daily_returns.json"
    if not path.is_file():
        _fail("luck-test daily_returns.json is missing")
    doc = json.loads(path.read_text(encoding="utf-8"))
    dates = [row["date"] for row in doc["daily"]]
    if dates[0] != "2026-08-13" or dates[-1] != "2026-09-11":
        _fail(f"window is {dates[0]}..{dates[-1]}")
    if any(day >= "2026-09-14" for day in dates):
        _fail("published series scores the cutoff")
    if doc["n_days"] != 21 or doc["n_days_picked"] != 12:
        _fail(f"day counts drifted: {doc['n_days']} {doc['n_days_picked']}")
    if doc["id"] != "excel_ml_lever_h1":
        _fail("lever id drifted")


def test_fingerprints() -> None:
    module_hash = M.sha256_file(HERE / "excel_ml_lever.py")
    spec_path = HERE / "SPEC.md"
    frozen_path = HERE / "frozen_spec.json"
    if not spec_path.is_file() or not frozen_path.is_file():
        _fail("SPEC.md and frozen_spec.json must be present")
    spec = spec_path.read_text(encoding="utf-8")
    if module_hash not in spec:
        _fail("SPEC.md does not record the module sha256")
    frozen = json.loads(frozen_path.read_text(encoding="utf-8"))
    spec_hash = M.sha256_file(spec_path)
    if frozen.get("spec_sha256") != spec_hash:
        _fail("frozen_spec.json spec_sha256 does not match SPEC.md")
    if frozen.get("module_sha256", {}).get("excel_ml_lever.py") != module_hash:
        _fail("frozen module hash disagrees with the file")
    if frozen.get("hold") != 1 or frozen.get("cutoff") != "2026-09-14":
        _fail("frozen settings drifted")


def _fees() -> dict:
    return {
        "commission_per_share": 0.0049,
        "commission_min_per_order": 0.99,
        "commission_max_pct_of_amount": 0.005,
        "platform_per_share": 0.005,
        "platform_min_per_order": 1.00,
        "platform_max_pct_of_amount": 0.005,
        "settlement_per_share": 0.003,
        "regulatory_pct_of_amount_sell_only": 0.000008,
        "regulatory_min_per_order": 0.01,
        "taf_per_share_sell_only": 0.000166,
        "taf_min_per_order": 0.01,
        "taf_max_per_order": 8.30,
    }


def main() -> None:
    tests = [
        test_training_clock_is_n_minus_2,
        test_rank_is_symmetric_and_centers_missing,
        test_price_features_ignore_today_and_the_future,
        test_suggestion_clock,
        test_excluded_columns_are_not_features,
        test_walk_does_not_train_on_unknown_exits_and_is_deterministic,
        test_future_panel_row_is_dropped,
        test_cutoff_range_is_refused,
        test_tie_break_is_ticker_ascending,
        test_missing_open_is_skipped,
        test_fees_match_the_schedule_and_borrow_is_separate,
        test_ridge_is_deterministic,
        test_published_window_stops_at_09_11,
        test_fingerprints,
    ]
    for test in tests:
        test()
        print(f"ok {test.__name__}", flush=True)
    print(f"ok {len(tests)} tests", flush=True)


if __name__ == "__main__":
    main()
