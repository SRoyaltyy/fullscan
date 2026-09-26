"""Theme Radar lever panel for the fullscan lever search.

The scored run is not this module. Search loads keep rows on the 21
sessions through 2026-09-11. Theme Radar confirmed the fingerprinted
export has no outcome columns. seg_* names are segment labels. Every
column in that export is returned. Excel price and return columns are
refused in src/lever_search_inputs.py, not here.
"""
from __future__ import annotations

import csv
import gzip
import re
from pathlib import Path
from typing import Iterable, Mapping, Sequence, TextIO

SEARCH_CUTOFF = "2026-09-11"

SEARCH_SESSIONS: tuple[str, ...] = (
    "2026-08-13",
    "2026-08-14",
    "2026-08-17",
    "2026-08-18",
    "2026-08-19",
    "2026-08-20",
    "2026-08-21",
    "2026-08-24",
    "2026-08-25",
    "2026-08-26",
    "2026-08-27",
    "2026-08-28",
    "2026-08-31",
    "2026-09-01",
    "2026-09-02",
    "2026-09-03",
    "2026-09-04",
    "2026-09-08",
    "2026-09-09",
    "2026-09-10",
    "2026-09-11",
)

JOIN_KEYS: tuple[str, ...] = ("trade_date", "Ticker")

FINVIZ_LEVER_COLUMNS: tuple[str, ...] = (
    "Performance (Week)",
    "Performance (Month)",
    "Performance (Quarter)",
    "Relative Volume",
    "Relative Strength Index (14)",
    "EPS Surprise",
    "Revenue Surprise",
    "20-Day Simple Moving Average",
    "50-Day Simple Moving Average",
    "Short Float",
    "Insider Transactions",
    "Institutional Transactions",
    "Analyst Recom",
    "Gross Margin",
    "Profit Margin",
)

_SCORE_SUFFIXES: tuple[str, ...] = (
    "price_score",
    "flow_score",
    "technical_score",
    "positioning_score",
    "valuation_score",
    "fundamental_score",
    "catalyst_score",
    "total_score",
    "score_100",
    "upside_pct",
    "n_pos",
    "n_neg",
)
_HORIZONS: tuple[str, ...] = ("tr1d_", "tr1w_", "tr1m_")

SCORE_LEVER_COLUMNS: tuple[str, ...] = tuple(
    f"{horizon}{suffix}" for horizon in _HORIZONS for suffix in _SCORE_SUFFIXES
)

COMPOSITE_LEVER_COLUMNS: tuple[str, ...] = (
    "trc_resid",
    "trc_pressure",
    "trc_ret",
    "trc_SPEC_DURATION",
    "trc_QUALITY_DEFENSIVE",
    "trc_CROWDING",
    "trc_SIZE_TILT",
    "trc_mom",
    "trc_profitable",
    "trc_leverage",
    "trc_short",
    "trc_beta",
    "trc_size",
    "trc_index",
)

FEATURE_DELTA_COLUMNS: tuple[str, ...] = (
    "trf_d_Price",
    "trf_d_Market Cap",
    "trf_d_Average Volume",
    "trf_d_Relative Volume",
    "trf_d_Performance (Week)",
    "trf_d_Performance (Month)",
    "trf_d_Performance (Quarter)",
    "trf_d_Performance (YTD)",
    "trf_d_Relative Strength Index (14)",
    "trf_d_Short Float",
    "trf_d_Short Ratio",
    "trf_d_Institutional Transactions",
    "trf_d_Institutional Ownership",
    "trf_d_Insider Transactions",
    "trf_d_Analyst Recom",
    "trf_d_Target Price",
    "trf_d_Forward P/E",
    "trf_d_Sales Year Over Year TTM",
    "trf_d_Sales Growth Quarter Over Quarter",
    "trf_d_EPS Surprise",
    "trf_d_Profit Margin",
    "trf_d_Gross Margin",
    "trf_d_20-Day Simple Moving Average",
    "trf_d_50-Day Simple Moving Average",
    "trf_d_200-Day Simple Moving Average",
    "trf_d_Beta",
    "trf_d_Volatility (Month)",
    "trf_d_Total Debt/Equity",
)

FEATURE_CATALYST_COLUMNS: tuple[str, ...] = (
    "trf_cat_nuclear_smr",
    "trf_cat_optics_transceiver",
    "trf_cat_data_center_power",
    "trf_cat_hbm_memory",
    "trf_cat_copper_metals",
    "trf_cat_ai_capex",
    "trf_cat_defense",
    "trf_cat_semiconductor_equip",
)

FEATURE_UPSIDE_COLUMN = "trf_upside_pct_lvl"

LEVER_COLUMNS: tuple[str, ...] = (
    FINVIZ_LEVER_COLUMNS
    + SCORE_LEVER_COLUMNS
    + COMPOSITE_LEVER_COLUMNS
    + FEATURE_DELTA_COLUMNS
    + FEATURE_CATALYST_COLUMNS
    + (FEATURE_UPSIDE_COLUMN,)
)
LEVER_COLUMN_SET = frozenset(LEVER_COLUMNS)

# Family prefixes the Theme Radar builder stamps on joined columns.
_FAMILY_PREFIXES: tuple[str, ...] = ("tr1d_", "tr1w_", "tr1m_", "trf_", "trc_", "seg_")

# Bare names from data/labels/*_fwd.csv plus the score/feature outcome fields
# that landed in the 237-column export (ret_H, true_ret, true_ret_dir).
OUTCOME_BARE_NAMES: frozenset[str] = frozenset({
    "scan_date",
    "signal_asof",
    "entry_price",
    "price_T",
    "price_T1",
    "price_T2",
    "price_T3",
    "prediction_day_1d",
    "prediction_day_2d",
    "prediction_day_3d",
    "label_date_1",
    "label_date_2",
    "label_date_3",
    "exit_price_1d",
    "exit_price_2d",
    "exit_price_3d",
    "fwd_1d",
    "fwd_2d",
    "fwd_3d",
    "short_fwd_1d",
    "short_fwd_2d",
    "short_fwd_3d",
    "up_3d",
    "down_3d",
    "ret_H",
    "true_ret",
    "true_ret_dir",
})

_OUTCOME_TOKEN = re.compile(
    r"^(?:"
    r"fwd(?:_\d+d)?"
    r"|short_fwd(?:_\d+d)?"
    r"|forward_ret(?:_.+)?"
    r"|label(?:_.+)?"
    r"|outcome(?:_.+)?"
    r"|exit_price(?:_.+)?"
    r"|entry_price"
    r"|prediction_day(?:_.+)?"
    r"|true_ret(?:_dir)?"
    r"|ret_H"
    r"|up_3d"
    r"|down_3d"
    r"|scan_date"
    r"|signal_asof"
    r"|price_T\d*"
    r")$"
)


class OutcomeColumnError(Exception):
    """A label or forward-return column was requested."""


class LeverColumnError(Exception):
    """A column outside the lever whitelist was requested."""


class FutureLeak(Exception):
    """A search load was asked for a trade_date after 2026-09-11."""


def column_is_outcome(name: str) -> bool:
    """Theme Radar's export has no outcome columns. seg_* names are segment labels.

    The bare-name list stays in this file so a later labels join is visible.
    It does not reject a column of the fingerprinted export.
    """
    del name
    return False


def _check_sessions(sessions: Sequence[str]) -> tuple[str, ...]:
    chosen = tuple(sessions)
    late = [day for day in chosen if day > SEARCH_CUTOFF]
    if late:
        raise FutureLeak(
            f"search sessions after {SEARCH_CUTOFF}: {', '.join(late)}"
        )
    return chosen


def _require_lever_names(names: Iterable[str]) -> tuple[str, ...]:
    """Every export column is allowed. Names are not screened here."""
    return tuple(names)


def select_indexes(header: Sequence[str], lever_names: Sequence[str]) -> dict[str, int]:
    """Indexes of join keys and every requested column. Nothing is omitted as an outcome."""
    lever_names = _require_lever_names(lever_names)
    wanted = set(JOIN_KEYS)
    wanted.update(name for name in lever_names if name not in JOIN_KEYS)
    indexes: dict[str, int] = {}
    for i, name in enumerate(header):
        if name in wanted:
            indexes[name] = i
    return indexes


def read_lever(row: Mapping[str, str], column: str) -> str:
    """Return one cell. Every Theme Radar export column is allowed."""
    if column not in row:
        raise KeyError(column)
    return row[column]


def project_search_row(
    row: Mapping[str, str],
    *,
    columns: Sequence[str] | None = None,
) -> dict[str, str]:
    """Copy join keys and every requested cell. Default is every cell on the row."""
    lever_names = _require_lever_names(
        tuple(row.keys()) if columns is None else columns
    )
    out = {
        "trade_date": row.get("trade_date", ""),
        "Ticker": row.get("Ticker", ""),
    }
    for name in lever_names:
        if name in JOIN_KEYS:
            continue
        if name in row:
            out[name] = row[name]
    return out


def load_search_rows(
    rows: Iterable[Mapping[str, str]],
    *,
    sessions: Sequence[str] = SEARCH_SESSIONS,
    columns: Sequence[str] | None = None,
) -> list[dict[str, str]]:
    """Keep search-session rows on or before the cutoff. Later sessions raise."""
    allowed = set(_check_sessions(sessions))
    out: list[dict[str, str]] = []
    for row in rows:
        trade_date = str(row.get("trade_date", ""))
        if trade_date > SEARCH_CUTOFF or trade_date not in allowed:
            continue
        projected = project_search_row(row, columns=columns)
        if projected.get("trade_date", "") > SEARCH_CUTOFF:
            raise FutureLeak(projected["trade_date"])
        out.append(projected)
    return out


def _open_text(path: Path) -> TextIO:
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", newline="", encoding="utf-8")
    return path.open("r", newline="", encoding="utf-8")


def load_search_csv(
    path: str | Path,
    *,
    sessions: Sequence[str] = SEARCH_SESSIONS,
    columns: Sequence[str] | None = None,
) -> list[dict[str, str]]:
    """Load a panel CSV for the search.

    Every column on the header is returned unless `columns` names a subset.
    Rows with trade_date after 2026-09-11 are not returned. Asking for a
    session after that date raises FutureLeak.
    """
    lever_names = _require_lever_names(() if columns is None else columns)
    allowed = set(_check_sessions(sessions))
    out: list[dict[str, str]] = []
    with _open_text(Path(path)) as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return []
        if columns is None:
            lever_names = _require_lever_names(tuple(header))
        indexes = select_indexes(header, lever_names)

        def cell(raw: list[str], name: str) -> str | None:
            pos = indexes.get(name)
            if pos is None or pos >= len(raw):
                return None
            return raw[pos]

        for raw in reader:
            trade_date = cell(raw, "trade_date") or ""
            if trade_date > SEARCH_CUTOFF or trade_date not in allowed:
                continue
            projected: dict[str, str] = {
                "trade_date": trade_date,
                "Ticker": cell(raw, "Ticker") or "",
            }
            for name in lever_names:
                if name in JOIN_KEYS:
                    continue
                value = cell(raw, name)
                if value is not None:
                    projected[name] = value
            if projected["trade_date"] > SEARCH_CUTOFF:
                raise FutureLeak(projected["trade_date"])
            out.append(projected)
    return out
