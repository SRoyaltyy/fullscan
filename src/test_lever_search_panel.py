"""Column guard for the Theme Radar lever panel.

Run: PYTHONHASHSEED=0 python3 -m src.test_lever_search_panel

Fails when an excluded outcome column is requested or returned, and when a
search load keeps a trade_date after 2026-09-11.
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
    text = PREREG.read_text(encoding="utf-8")
    names = _marked(text, "<!-- OUTCOME_EXCLUDE_BEGIN -->", "<!-- OUTCOME_EXCLUDE_END -->")
    assert names
    for name in names:
        assert column_is_outcome(name), name
        try:
            read_lever({"trade_date": "2026-08-13"}, name)
        except OutcomeColumnError:
            pass
        else:
            raise AssertionError(f"{name} was readable")
    overlap = set(names) & set(LEVER_COLUMNS)
    assert not overlap


def test_outcome_names_and_safe_scores() -> None:
    excluded = [
        "fwd_1d", "fwd_2d", "fwd_3d",
        "short_fwd_1d", "short_fwd_2d", "short_fwd_3d",
        "label_date_1", "label_date_2", "label_date_3",
        "exit_price_1d", "exit_price_2d", "exit_price_3d",
        "prediction_day_1d", "prediction_day_2d", "prediction_day_3d",
        "price_T", "price_T1", "price_T2", "price_T3",
        "up_3d", "down_3d", "scan_date", "signal_asof", "entry_price",
        "ret_H", "true_ret", "true_ret_dir",
        "tr1d_ret_H", "tr1w_ret_H", "tr1m_ret_H",
        "trf_true_ret", "trf_true_ret_dir",
        "tr1d_fwd_1d", "tr1w_short_fwd_2d", "trf_label_date_1",
        "seg_exit_price_1d", "trc_price_T1", "tr1m_true_ret",
    ]
    for name in excluded:
        assert column_is_outcome(name), name
    kept = [
        "trc_ret", "trc_resid", "trf_d_Forward P/E", "trf_upside_pct_lvl",
        "Performance (Quarter)", "tr1d_total_score", "Open", "seg_mom",
        "trf_dir_Price",
    ]
    for name in kept:
        assert not column_is_outcome(name), name


def test_select_indexes_skips_outcome_columns() -> None:
    indexes = select_indexes(HEADER, ["Performance (Week)", "trc_resid"])
    assert "fwd_1d" not in indexes
    assert "tr1d_ret_H" not in indexes
    assert "trf_true_ret" not in indexes
    assert indexes["Performance (Week)"] == HEADER.index("Performance (Week)")
    try:
        select_indexes(HEADER, ["fwd_1d"])
    except OutcomeColumnError as exc:
        assert "fwd_1d" in str(exc)
    else:
        raise AssertionError("fwd_1d index was built")
    try:
        select_indexes(HEADER, ["tr1d_ret_H"])
    except OutcomeColumnError:
        pass
    else:
        raise AssertionError("tr1d_ret_H index was built")


def _assert_loaded(rows: list[dict[str, str]]) -> None:
    dates = [row["trade_date"] for row in rows]
    assert dates == ["2026-08-13", "2026-09-11"]
    assert all(day <= SEARCH_CUTOFF for day in dates)
    assert all(day in SEARCH_SESSIONS for day in dates)
    tickers = [row["Ticker"] for row in rows]
    assert tickers == ["KEEP", "LAST"]
    for row in rows:
        blob = " ".join(row.values())
        assert LEAK not in blob
        for name in HEADER:
            if column_is_outcome(name):
                assert name not in row
        assert "seg_mom" not in row
        assert "trf_dir_Price" not in row
        assert "Open" not in row
        assert "tr1d_status_trend" not in row
        assert row["Performance (Week)"] == "1.5"
        assert row["trc_ret"] == "0.1"
        assert read_lever(row, "trc_resid") == "0.2"
        for bad in ("fwd_1d", "tr1d_ret_H", "trf_true_ret", "label_date_1", "exit_price_1d"):
            try:
                read_lever(row, bad)
            except OutcomeColumnError:
                pass
            else:
                raise AssertionError(f"read {bad}")
        for blocked in ("seg_mom", "Open", "trf_dir_Price", "tr1d_status_trend"):
            try:
                read_lever(row, blocked)
            except LeverColumnError:
                pass
            else:
                raise AssertionError(f"read {blocked}")


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
    assert LEAK not in " ".join(loaded[0].values())
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
        for name in ("fwd_1d", "tr1d_ret_H", "trf_true_ret_dir", "short_fwd_1d"):
            try:
                load_search_csv(path, columns=["Performance (Week)", name])
            except OutcomeColumnError as exc:
                assert name in str(exc)
            else:
                raise AssertionError(f"loaded {name}")


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
        FINVIZ_PROVEN_DATES,
        DroppedInput,
        InputHashError,
        assert_excel_row,
        assert_finviz_date,
        assert_initial_finviz_column,
        assert_manifest_hashes,
        load_manifest,
    )
    from src.lever_search_panel import LeverColumnError, OutcomeColumnError

    manifest = load_manifest()
    assert manifest["finviz_proven_dates"] == list(FINVIZ_PROVEN_DATES)
    assert "2026-08-28" not in manifest["finviz_proven_dates"]
    assert len(manifest["finviz_proven_dates"]) == 20
    assert_manifest_hashes()
    for name in ("fwd_1d", "tr1d_ret_H", "trf_true_ret", "label_date_1"):
        try:
            assert_initial_finviz_column(name)
        except OutcomeColumnError:
            pass
        else:
            raise AssertionError(name)
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
    assert_excel_row("AAPL", "2026-09-02")
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
