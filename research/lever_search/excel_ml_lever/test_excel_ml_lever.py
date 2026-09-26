"""No-lookahead and determinism tests for the Excel ML lever.

Run: python research/lever_search/excel_ml_lever/test_excel_ml_lever.py
"""
from __future__ import annotations

import datetime as dt
import gzip
import importlib.util
import io
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


def _when(stamp: str) -> dt.datetime:
    text = stamp[:-1] + "+00:00" if stamp.endswith("Z") else stamp
    return dt.datetime.fromisoformat(text)


_NOTE = """# note

## New suggestions

| ticker | side | strategy | exit | ref close | signal colors |
|---|---|---|---|---|---|
| AAPL | LONG | L1_long_green_tp8_lowvol | tp8 | 10 | red|white |
| E | SHORT | L9_short | tp3 | 5 | white |

## Live strategy scoreboard (all tracked suggestions, ret vs entry open)

| strategy | n | mean | median | win% |
|---|---|---|---|---|
| L1_long_green_tp8_lowvol | 290 | -0.08% | -0.97% | 42.4% |

## Best open suggestions (ret vs entry open)

| signal date | ticker | strategy | entry open | current | ret | days held |
|---|---|---|---|---|---|---|
| 2026-07-27 | HHS | L1_long_green_tp8_lowvol | 2.36 | 4.33 | +83.47% | 34 |
"""

_EMPTY = "None — no cluster confirmed today.\n"


def test_suggestion_clock() -> None:
    """A note dated D is visible only on the next panel session, from the last commit before that 09:30."""
    sessions = [
        "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-03",
        "2026-09-04", "2026-09-08", "2026-09-11",
    ]
    early = "a" * 40
    late = "b" * 40
    notes = [{
        "date": "2026-08-30",
        "commits": [
            (_when("2026-08-30T15:45:42Z"), early, "early"),
            (_when("2026-08-30T16:22:19Z"), late, "late"),
        ],
        "blobs": {early: _EMPTY, late: _NOTE},
    }]
    pinned = M.assign_pinned_signals(sessions, notes)
    morning = pinned["2026-08-31"]
    if [row["ticker"] for row in morning] != ["AAPL", "E"]:
        _fail(f"later blob was not the pin: {morning}")
    if any(row["ticker"] == "HHS" for row in morning):
        _fail("scoreboard ticker leaked into the signal")
    flags = M.suggestion_features("AAPL", morning)
    if flags["excel_sugg_long"] != 1.0 or flags["excel_sugg_n"] != 1.0:
        _fail(f"long flag missing: {flags}")
    short = M.suggestion_features("E", morning)
    if short["excel_sugg_short"] != 1.0 or short["excel_sugg_long"] != 0.0:
        _fail(f"short flag missing: {short}")
    if pinned["2026-09-01"]:
        _fail("a note must not be reused on a later morning")
    if pinned["2026-08-28"]:
        _fail("a note must not be a feature on a morning before it exists")
    # A commit at the open, and a commit after it, are both too late.
    at_open = M.excel_open_cutoff("2026-08-31")
    after = _when("2026-08-31T18:00:00Z")
    late_only = M.assign_pinned_signals(sessions, [{
        "date": "2026-08-30",
        "commits": [(at_open, late, "at"), (after, early, "after")],
        "blobs": {late: _NOTE, early: _NOTE},
    }])
    if late_only["2026-08-31"]:
        _fail("a commit at 09:30 was treated as knowable")
    # The earlier blob is the pin when the second write misses the open.
    mixed = M.assign_pinned_signals(sessions, [{
        "date": "2026-08-30",
        "commits": [
            (_when("2026-08-30T15:45:42Z"), early, "early"),
            (after, late, "late"),
        ],
        "blobs": {early: _EMPTY, late: _NOTE},
    }])
    if mixed["2026-08-31"]:
        _fail("the after-open rewrite was used, or the empty blob invented rows")
    # Two notes share the next session (Friday and Saturday both open Monday).
    friday = "c" * 40
    saturday = "d" * 40
    both = M.assign_pinned_signals(sessions, [
        {
            "date": "2026-09-04",
            "commits": [(_when("2026-09-04T20:00:00Z"), friday, "fri")],
            "blobs": {friday: _NOTE},
        },
        {
            "date": "2026-09-05",
            "commits": [(_when("2026-09-05T20:00:00Z"), saturday, "sat")],
            "blobs": {saturday: _NOTE},
        },
    ])
    if len(both["2026-09-08"]) != 4:
        _fail(f"Friday and Saturday notes did not union onto Monday: {both['2026-09-08']}")
    if both["2026-09-11"]:
        _fail("weekend notes leaked onto a later morning")
    # A note whose next session is the cutoff is not loaded.
    past = M.assign_pinned_signals(sessions, [{
        "date": "2026-09-11",
        "commits": [(_when("2026-09-11T20:00:00Z"), late, "late")],
        "blobs": {late: _NOTE},
    }])
    if any(past.values()):
        _fail(f"a note aimed at the cutoff was loaded: {past}")
    absent = M.suggestion_features("AAA", [])
    if absent["excel_sugg_long"] != 0.0 or absent["excel_sugg_n"] != 0.0:
        _fail(f"a morning with no note should be zeros: {absent}")


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
    letters = {
        "excel_FQ", "excel_ER", "excel_EP", "excel_AH", "excel_FR",
        "excel_prior_hammer",
    }
    leaked = letters & set(M.FEATURES)
    if leaked:
        _fail(f"clear-letter columns are features: {leaked}")
    if set(M.EXCEL_FEATURES) != {"excel_sugg_long", "excel_sugg_short", "excel_sugg_n"}:
        _fail(f"excel features drifted: {M.EXCEL_FEATURES}")


def test_llm_packet_is_blank_before_the_panel() -> None:
    row = {
        "date": "2026-08-12",
        "ticker": "AAA",
        "ohlc_hot_score": 1.5,
        "boxes": {"ab": "good", "sector": "bad", "peer": "good", "vol": "neutral"},
        "news_prior": "good",
        "clk_mom_break_peer": True,
        "last_green": True,
    }
    feat = M.row_features(row, price_features={}, excel_rows=[])
    if feat["box_ab"] is not None or feat["box_sector"] is not None:
        _fail(f"LLM boxes were rebuilt before the panel: {feat['box_ab']}")
    if feat["cat_news_prior"] is not None or feat["clk_mom_break_peer"] is not None:
        _fail("news or clock-b was rebuilt before the panel")
    if feat["box_peer"] != 1.0 or feat["ohlc_hot_score"] != 1.5 or feat["last_green"] != 1.0:
        _fail("price boxes were blanked with the LLM packet")
    kept = M.row_features(
        {"date": "2026-08-13", "ticker": "AAA", "boxes": {"ab": "good"}},
        price_features={}, excel_rows=[],
    )
    if kept["box_ab"] != 1.0:
        _fail("AB on a real morning session was dropped")


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
        items = M.training_items(panel, sessions, day, excel_by_day={}, bars_by_ticker=bars)
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
    if doc["daily"]:
        _fail("a morning without a pre-open list was scored")
    if doc["n_days"] != 0 or doc["n_days_picked"] != 0 or doc["n_train_sessions"] != 0:
        _fail(f"day counts drifted: {doc['n_days']} {doc['n_days_picked']} {doc.get('n_train_sessions')}")
    if doc["after_fees_return"] != 0 or doc["after_fees_return_15bp"] != 0:
        _fail("an empty book has a return")
    dropped = [row["date"] for row in doc.get("dropped") or []]
    if dropped[0] != "2026-08-13" or dropped[-1] != "2026-09-11" or len(dropped) != 21:
        _fail(f"dropped days drifted: {dropped[:2]}..{dropped[-1:]} n={len(dropped)}")
    if any(day >= "2026-09-14" for day in dropped):
        _fail("published series scores the cutoff")
    if doc["id"] != "excel_ml_lever_h1":
        _fail("lever id drifted")


def test_preopen_inputs_are_not_todays_panel() -> None:
    try:
        M.load_panel()
    except RuntimeError:
        pass
    else:
        _fail("today's panel.json was readable")
    if M.server_before_open("2026-09-09T13:30:00Z", "2026-09-09"):
        _fail("a run at 09:30 ET was early enough")
    if not M.server_before_open("2026-09-09T13:29:07Z", "2026-09-09"):
        _fail("13:29Z should be before 09:30 ET")
    if M.panel_has_morning(
        {"session_dates": ["2026-08-13", "2026-09-08"], "rows": [{"date": "2026-09-08", "ticker": "AAA"}]},
        "2026-09-09",
    ):
        _fail("a blob that ends 09-08 was treated as the 09-09 morning list")
    manifest = json.loads((HERE / "input_manifest.json").read_text(encoding="utf-8"))
    called = []

    def read_blob(commit, path):
        called.append((commit, path))
        raise AssertionError("a dropped day must not load a blob")

    report = M.series_from_manifest(manifest, read_blob)
    if called:
        _fail(f"manifest read a file: {called[:2]}")
    if report["n_days"] != 0 or len(report["dropped"]) != 21:
        _fail("manifest series is not the 21 dropped mornings")
    for day, slot in manifest["days"].items():
        if not M.server_before_open(slot["server_time"], day):
            _fail(f"{day} server time {slot['server_time']} is not before the open")
        if slot["status"] == "loaded":
            _fail(f"{day} was marked loaded")
        latest = slot.get("latest_session_in_panel")
        if latest is not None and latest >= day:
            _fail(f"{day} panel already contains that morning")

    class SpyList(list):
        def __init__(self, values):
            super().__init__(values)
            self.touched = []

        def __getitem__(self, index):
            self.touched.append(index)
            return list.__getitem__(self, index)

    header = [
        "run_date", "signal_date", "ticker", "side", "strategy",
        "current_price", "ret_vs_close", "ret_vs_open",
    ]
    leak = header.index("current_price")
    cells = SpyList([
        "2026-09-08", "2026-09-08", "AAA", "LONG", "L1", "99", "1", "1",
    ])
    rows = M.signal_rows_from_header(header, [cells], morning="2026-09-09")
    if leak in cells.touched:
        _fail("current_price was read")
    if rows != [{
        "ticker": "AAA", "side": "long", "strategy": "L1", "signal_date": "2026-09-08",
    }]:
        _fail(f"signal row drifted: {rows}")
    same_day = SpyList([
        "2026-09-09", "2026-09-09", "BBB", "LONG", "L1", "99", "1", "1",
    ])
    if M.signal_rows_from_header(header, [same_day], morning="2026-09-09"):
        _fail("signal_date equal to the morning was kept")


def test_theme_reader_rejects_non_whitelist_and_stays_off() -> None:
    if M.THEME_RADAR_ENABLED:
        _fail("theme radar is on in the declared lever")
    if M.feature_list() != M.FEATURES:
        _fail("declared features include the optional theme block")
    if any(name.startswith("tr_") for name in M.FEATURES):
        _fail("a theme column is in the declared feature list")
    if not str(M.THEME_RADAR_COMMIT).startswith("3973e13"):
        _fail(f"theme commit pin drifted: {M.THEME_RADAR_COMMIT}")
    hashes = {item["path"]: item["sha256"] for item in M.THEME_RADAR_FILES}
    august = "research/lever_panel/finviz_panel_asof0930_2026-08.csv.gz"
    september = "research/lever_panel/finviz_panel_asof0930_2026-09.csv.gz"
    if hashes.get(august) != "c8977b8eea8e74115899e9d4cc04d5b4ea67490376d972905781eb8e1aeb6459":
        _fail("august theme sha256 drifted")
    if hashes.get(september) != "cbf35da9e1587703059abd9ff77525a1047c67a91edc3276ca93db4cd8669c16":
        _fail("september theme sha256 drifted")
    banned = (
        "fwd_ret", "label", "outcome", "future_ret", "hit",
        "trf_true_ret", "trf_true_ret_dir", "Open", "y_true",
    )
    for name in banned:
        if name in M.THEME_RADAR_READ:
            _fail(f"{name} is in the theme read list")
        if not M._theme_banned(name):
            _fail(f"{name} is not treated as banned")
    try:
        M.assert_theme_read_list(["Price", "fwd_ret"])
    except RuntimeError:
        pass
    else:
        _fail("a non-whitelisted column was accepted")

    class SpyList(list):
        def __init__(self, values):
            super().__init__(values)
            self.touched = []

        def __getitem__(self, index):
            self.touched.append(index)
            return list.__getitem__(self, index)

    header = [
        "trade_date", "Ticker", "scrape_ts_utc", "Price",
        "fwd_ret", "hit", "label", "trf_true_ret",
    ]
    leak_at = header.index("fwd_ret")
    cells = SpyList([
        "2026-09-11", "aaa", "2026-09-11T12:00:00Z", "3.5",
        "999", "1", "up", "0.2",
    ])
    picked = M.fields_from_cells(header, cells)
    if leak_at in cells.touched or "999" in set(picked.values()):
        _fail(f"a non-whitelisted cell was read: {cells.touched}")
    if set(picked) - set(M.THEME_RADAR_READ):
        _fail(f"picked keys left the allow-list: {set(picked)}")
    late = ["2026-09-14", "BBB", "2026-09-14T12:00:00Z", "4", "999", "1", "up", "0.2"]
    at_open = ["2026-09-11", "CCC", "2026-09-11T13:30:00Z", "4", "999", "1", "up", "0.2"]
    joined = M.load_theme_rows([
        picked,
        M.fields_from_cells(header, late),
        M.fields_from_cells(header, at_open),
    ])
    if set(joined) != {("2026-09-11", "AAA")}:
        _fail(f"theme join kept a row after 2026-09-11 or at the open: {set(joined)}")
    if joined[("2026-09-11", "AAA")].get("tr_price") != 3.5:
        _fail("whitelist price did not join")
    try:
        M.load_theme_rows([picked, picked])
    except RuntimeError:
        pass
    else:
        _fail("a duplicate theme key was accepted")
    blob = io.BytesIO()
    with gzip.GzipFile(fileobj=blob, mode="wb", mtime=0) as gz:
        gz.write(
            b"trade_date,Ticker,scrape_ts_utc,Price,fwd_ret\n"
            b"2026-09-11,AAA,2026-09-11T12:00:00Z,3.5,999\n"
            b"2026-09-14,BBB,2026-09-14T12:00:00Z,8,999\n"
        )
    blob.seek(0)
    with gzip.GzipFile(fileobj=blob, mode="rb") as gz:
        text = io.TextIOWrapper(gz, encoding="utf-8", newline="")
        streamed = M.load_theme_stream(text)
    if set(streamed) != {("2026-09-11", "AAA")}:
        _fail(f"gzip reader kept a late row: {set(streamed)}")
    old = M.THEME_RADAR_ENABLED
    M.THEME_RADAR_ENABLED = True
    try:
        try:
            M.walk(["2026-08-13"], {}, {}, fees=_fees(), end="2026-08-13")
        except RuntimeError:
            pass
        else:
            _fail("the hook ran without the pinned files")
    finally:
        M.THEME_RADAR_ENABLED = old


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
        test_llm_packet_is_blank_before_the_panel,
        test_walk_does_not_train_on_unknown_exits_and_is_deterministic,
        test_future_panel_row_is_dropped,
        test_cutoff_range_is_refused,
        test_tie_break_is_ticker_ascending,
        test_missing_open_is_skipped,
        test_fees_match_the_schedule_and_borrow_is_separate,
        test_ridge_is_deterministic,
        test_published_window_stops_at_09_11,
        test_preopen_inputs_are_not_todays_panel,
        test_theme_reader_rejects_non_whitelist_and_stays_off,
        test_fingerprints,
    ]
    for test in tests:
        test()
        print(f"ok {test.__name__}", flush=True)
    print(f"ok {len(tests)} tests", flush=True)


if __name__ == "__main__":
    main()
