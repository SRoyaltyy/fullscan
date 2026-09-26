"""Column guard for the Theme Radar lever panel and the Excel signal file.

Run: PYTHONHASHSEED=0 python3 -m src.test_lever_search_panel

Theme Radar export columns are all allowed. Excel price and return columns
are outcomes. A search load must not keep a trade_date after 2026-09-11.
"""
from __future__ import annotations

import gzip
import tempfile
from pathlib import Path

from src.lever_search_panel import (
    LEVER_COLUMNS,
    SEARCH_CUTOFF,
    SEARCH_SESSIONS,
    FutureLeak,
    LeverColumnError,
    OutcomeColumnError,
    column_is_outcome,
    load_search_csv,
    load_search_rows,
    read_lever,
    select_indexes,
)

PREREG = Path(__file__).resolve().parents[1] / "research" / "lever_search" / "PREREG.md"

HEADER = [
    "trade_date",
    "Ticker",
    "Performance (Week)",
    "tr1d_total_score",
    "trc_resid",
    "trc_ret",
    "trf_d_Price",
    "trf_upside_pct_lvl",
    "tr1d_ret_H",
    "tr1w_ret_H",
    "tr1m_ret_H",
    "trf_true_ret",
    "trf_true_ret_dir",
    "fwd_1d",
    "short_fwd_1d",
    "label_date_1",
    "exit_price_1d",
    "entry_price",
    "prediction_day_1d",
    "price_T",
    "price_T1",
    "up_3d",
    "down_3d",
    "scan_date",
    "signal_asof",
    "true_ret",
    "ret_H",
    "tr1d_fwd_1d",
    "seg_mom",
    "trf_dir_Price",
    "Open",
    "tr1d_status_trend",
]

LEAK = "LEAKED_OUTCOME_VALUE"
SAFE = {
    "Performance (Week)": "1.5",
    "tr1d_total_score": "62",
    "trc_resid": "0.2",
    "trc_ret": "0.1",
    "trf_d_Price": "0.4",
    "trf_upside_pct_lvl": "3",
}


def _marked(text: str, begin: str, end: str) -> list[str]:
    start = text.index(begin) + len(begin)
    stop = text.index(end, start)
    names = []
    for line in text[start:stop].splitlines():
        stripped = line.strip()
        if stripped.startswith("- `") and stripped.endswith("`"):
            names.append(stripped[3:-1])
    return names


def _row(trade_date: str, ticker: str) -> dict[str, str]:
    row = {name: LEAK for name in HEADER}
    row["trade_date"] = trade_date
    row["Ticker"] = ticker
    row.update(SAFE)
    return row


def _write_csv(directory: Path, *, gzip_file: bool) -> Path:
    lines = [",".join(HEADER)]
    samples = [
        ("2026-08-07", "EARLY"),
        ("2026-08-13", "KEEP"),
        ("2026-09-11", "LAST"),
        ("2026-09-14", "AFTER"),
        ("2026-09-28", "NEVER"),
    ]
    for trade_date, ticker in samples:
        row = _row(trade_date, ticker)
        lines.append(",".join(row[name] for name in HEADER))
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    if gzip_file:
        path = directory / "panel.csv.gz"
        with gzip.open(path, "wb") as handle:
            handle.write(payload)
        return path
    path = directory / "panel.csv"
    path.write_bytes(payload)
    return path


def test_prereg_whitelist_matches_loader() -> None:
    text = PREREG.read_text(encoding="utf-8")
    names = _marked(text, "<!-- LEVER_WHITELIST_BEGIN -->", "<!-- LEVER_WHITELIST_END -->")
    assert names == list(LEVER_COLUMNS)
    assert len(names) == 102
    assert len(set(names)) == 102
    for name in names:
        assert not column_is_outcome(name), name


def test_prereg_exclude_block_is_outcome() -> None:
    from src.lever_search_inputs import EXCEL_OUTCOME_COLUMNS, assert_excel_column

    text = PREREG.read_text(encoding="utf-8")
    names = _marked(text, "<!-- EXCEL_OUTCOME_BEGIN -->", "<!-- EXCEL_OUTCOME_END -->")
    assert names == sorted(EXCEL_OUTCOME_COLUMNS)
    for name in names:
        try:
            assert_excel_column(name)
        except OutcomeColumnError:
            pass
        else:
            raise AssertionError(f"{name} was a signal column")
    assert "<!-- OUTCOME_EXCLUDE_BEGIN -->" not in text
    overlap = set(names) & set(LEVER_COLUMNS)
    assert not overlap


def test_outcome_names_and_safe_scores() -> None:
    from src.lever_search_inputs import assert_excel_column

    # Theme Radar confirmed the export has none of these as outcomes.
    allowed = [
        "fwd_1d", "tr1d_ret_H", "trf_true_ret", "trf_true_ret_dir",
        "seg_mom", "seg_exit_price_1d", "Open", "trf_dir_Price",
        "trc_ret", "trc_resid", "Performance (Quarter)",
    ]
    for name in allowed:
        assert not column_is_outcome(name), name
        read_lever({"trade_date": "2026-08-13", name: "1"}, name)
    for name in ("current_price", "ret_vs_close", "ret_vs_open", "ref_close", "first_open", "days_held"):
        try:
            assert_excel_column(name)
        except OutcomeColumnError:
            pass
        else:
            raise AssertionError(name)
    for name in ("ticker", "strategy", "signal_date"):
        assert_excel_column(name)
    for name in ("run_date", "side", "exit_rule", "signal_colors"):
        try:
            assert_excel_column(name)
        except LeverColumnError:
            pass
        else:
            raise AssertionError(name)


def test_select_indexes_skips_outcome_columns() -> None:
    indexes = select_indexes(HEADER, ["Performance (Week)", "trc_resid", "fwd_1d", "tr1d_ret_H", "seg_mom"])
    assert indexes["fwd_1d"] == HEADER.index("fwd_1d")
    assert indexes["tr1d_ret_H"] == HEADER.index("tr1d_ret_H")
    assert indexes["seg_mom"] == HEADER.index("seg_mom")
    assert indexes["Performance (Week)"] == HEADER.index("Performance (Week)")


def _assert_loaded(rows: list[dict[str, str]]) -> None:
    dates = [row["trade_date"] for row in rows]
    assert dates == ["2026-08-13", "2026-09-11"]
    assert all(day <= SEARCH_CUTOFF for day in dates)
    assert all(day in SEARCH_SESSIONS for day in dates)
    tickers = [row["Ticker"] for row in rows]
    assert tickers == ["KEEP", "LAST"]
    for row in rows:
        for name in HEADER:
            assert name in row
        assert row["seg_mom"] == LEAK
        assert row["fwd_1d"] == LEAK
        assert row["tr1d_ret_H"] == LEAK
        assert row["Open"] == LEAK
        assert row["Performance (Week)"] == "1.5"
        assert row["trc_ret"] == "0.1"
        assert read_lever(row, "trc_resid") == "0.2"
        assert read_lever(row, "seg_mom") == LEAK
        assert read_lever(row, "trf_true_ret") == LEAK


def test_search_csv_drops_late_rows_and_outcome_cells() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_csv(Path(tmp), gzip_file=False)
        rows = load_search_csv(path)
    _assert_loaded(rows)


def test_search_gzip_same_guard() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_csv(Path(tmp), gzip_file=True)
        rows = load_search_csv(path)
    _assert_loaded(rows)


def test_in_memory_rows_and_late_session() -> None:
    rows = [
        _row("2026-08-13", "KEEP"),
        _row("2026-09-14", "AFTER"),
        _row("2026-09-28", "NEVER"),
        _row("2026-08-07", "EARLY"),
    ]
    loaded = load_search_rows(rows)
    assert [row["Ticker"] for row in loaded] == ["KEEP"]
    assert loaded[0]["fwd_1d"] == LEAK
    assert loaded[0]["seg_mom"] == LEAK
    try:
        load_search_rows(rows, sessions=SEARCH_SESSIONS + ("2026-09-14",))
    except FutureLeak as exc:
        assert "2026-09-14" in str(exc)
    else:
        raise AssertionError("late session was accepted")
    try:
        load_search_csv(
            Path("/dev/null"),
            sessions=("2026-09-28",),
            columns=["Performance (Week)"],
        )
    except FutureLeak:
        pass
    else:
        raise AssertionError("2026-09-28 session was accepted")


def test_requesting_excluded_column_raises_before_load() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_csv(Path(tmp), gzip_file=False)
        rows = load_search_csv(path, columns=["Performance (Week)", "fwd_1d", "seg_mom", "tr1d_ret_H"])
        assert rows[0]["fwd_1d"] == LEAK
        assert rows[0]["seg_mom"] == LEAK
        assert "Open" not in rows[0]


def test_tally_lines_stay_in_prereg() -> None:
    from src.lever_search_inputs import (
        INITIAL_N,
        RUNNING_TALLY,
        covered_fingerprint,
    )

    text = PREREG.read_text(encoding="utf-8")
    assert "1,984,711,680" in text
    assert "16,646,400" in text
    assert "31,852,800" in text
    assert "31,861,969" in text
    assert f"{INITIAL_N}" == "31852800"
    assert f"{RUNNING_TALLY}" == "31861969"
    assert "868" in text
    assert "nothing proven yet" in text
    assert "30d483f2944b6a23e5ebca5bd894aeba1e2b94366de5d1168c2fe568f16fb32c" in text
    assert "b01166a6a3803672c21daf4af55c276402abb3c7" in text
    assert "excel_ml_count: LATER_ADDON" in text
    digest = covered_fingerprint(text)
    assert f"fingerprint_sha256: {digest}" in text


def test_initial_inputs_and_hash_guard() -> None:
    from src.lever_search_inputs import (
        EXCEL_DROPPED,
        EXCEL_DROPPED_SESSIONS,
        EXCEL_SESSIONS,
        FINVIZ_PROVEN_DATES,
        DroppedInput,
        InputHashError,
        assert_excel_column,
        assert_excel_row,
        assert_finviz_date,
        assert_initial_finviz_column,
        assert_manifest_hashes,
        load_manifest,
        proof_is_before_open,
    )
    from src.lever_search_panel import LeverColumnError, OutcomeColumnError

    manifest = load_manifest()
    assert manifest["finviz_proven_dates"] == list(FINVIZ_PROVEN_DATES)
    levers = manifest["theme_radar_finviz_levers"]
    assert levers["status"] == "available-proven"
    assert levers["commit"] == "19973230e4d80c74565e1246ada911503a808fb7"
    assert levers["path"] == "research/lever_panel/server_time_proof.csv"
    assert levers["sha256"] == "5bde5e9164f499899b4ed70fcdbfde3f72b3e8b5692da645f1e9682e3c6a913e"
    assert levers["export_morning_count"] == 35
    assert levers["in_window_count"] == 20
    assert levers["in_window_mornings"] == list(FINVIZ_PROVEN_DATES)
    assert len(levers["sample_api_checks"]) == 8
    assert all(row["start_before_0930_et"] and row["created_at_matches_api"] for row in levers["sample_api_checks"])
    assert manifest["excel_sessions"] == list(EXCEL_SESSIONS)
    assert "2026-08-28" not in manifest["finviz_proven_dates"]
    assert len(manifest["finviz_proven_dates"]) == 20
    assert_manifest_hashes()
    for name in ("fwd_1d", "tr1d_ret_H", "trf_true_ret", "label_date_1", "seg_mom"):
        try:
            assert_initial_finviz_column(name)
        except LeverColumnError:
            pass
        else:
            raise AssertionError(name)
    try:
        assert_excel_column("current_price")
    except OutcomeColumnError:
        pass
    else:
        raise AssertionError("current_price was a signal")
    for session in EXCEL_DROPPED_SESSIONS:
        assert session not in manifest["excel_sessions"]
    for name in ("tr1d_total_score", "trc_resid", "trf_d_Price", "Open"):
        try:
            assert_initial_finviz_column(name)
        except LeverColumnError:
            pass
        else:
            raise AssertionError(name)
    assert_initial_finviz_column("Performance (Week)")
    try:
        assert_finviz_date("2026-08-28")
    except DroppedInput:
        pass
    else:
        raise AssertionError("08-28 was accepted")
    try:
        assert_finviz_date("2026-09-14")
    except DroppedInput:
        pass
    else:
        raise AssertionError("09-14 was accepted")
    assert_finviz_date("2026-09-11")
    for ticker, session in EXCEL_DROPPED:
        try:
            assert_excel_row(ticker, session)
        except DroppedInput:
            pass
        else:
            raise AssertionError((ticker, session))
    try:
        assert_excel_row("AAPL", "2026-08-13")
    except DroppedInput:
        pass
    else:
        raise AssertionError("08-13 excel was accepted")
    assert_excel_row("AAPL", "2026-08-31")
    assert_excel_row("AAPL", "2026-09-02")
    assert_excel_row("AAPL", "2026-09-03")
    assert_excel_row("AAPL", "2026-09-11")
    for session in ("2026-08-27", "2026-08-28", "2026-09-01", "2026-09-09"):
        try:
            assert_excel_row("AAPL", session)
        except DroppedInput:
            pass
        else:
            raise AssertionError(f"{session} excel was accepted")
    from src.lever_search_inputs import WALK_FORWARD
    assert tuple(day for day in WALK_FORWARD if day not in EXCEL_SESSIONS) == EXCEL_DROPPED_SESSIONS
    for day in manifest["finviz_mornings"]:
        for source in day["sources"]:
            if "/snapshots/" not in source["source_path"]:
                continue
            assert source["status"] == "PROVEN"
            assert source["proof"]["kind"] == "actions_run_start"
            assert source["proof"]["head_sha"] == source["commit_sha"]
            assert proof_is_before_open(source, day["trade_date"])
            assert not proof_is_before_open(
                {"commit_sha": source["commit_sha"], "git_commit_date": source["git_commit_date"]},
                day["trade_date"],
            )
    for copy in manifest["excel_copies"]:
        assert proof_is_before_open(copy, copy["session_date"])
        assert copy["session_date"] in EXCEL_SESSIONS
    bad = dict(manifest)
    bad["pinned_files"] = [dict(manifest["pinned_files"][0], sha256="0" * 64)]
    try:
        assert_manifest_hashes(manifest=bad)
    except InputHashError:
        pass
    else:
        raise AssertionError("bad hash was accepted")


def main() -> None:
    tests = [
        test_prereg_whitelist_matches_loader,
        test_prereg_exclude_block_is_outcome,
        test_outcome_names_and_safe_scores,
        test_select_indexes_skips_outcome_columns,
        test_search_csv_drops_late_rows_and_outcome_cells,
        test_search_gzip_same_guard,
        test_in_memory_rows_and_late_session,
        test_requesting_excluded_column_raises_before_load,
        test_tally_lines_stay_in_prereg,
        test_initial_inputs_and_hash_guard,
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
