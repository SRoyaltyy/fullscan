"""Excel's fullscan lever: walk-forward ridge on the morning panel.

One entry, hold 1. Each session N is fit and traded on its own, then the
next session starts from that close (cash and holdings). Research only.
Days in the 2026-08-13..2026-09-11 window are designed_after. No session
on or after 2026-09-14 is scored.

Hold 2 is not a variant.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import re
import subprocess
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]

LEVER_ID = "excel_ml_lever_h1"
AUTHOR = "excel"
FAMILY = "lever"
CREATED_ON = "2026-09-26"
SEED = 7
HOLD = 1
TOP_N = 4
CAPITAL = 10_000.0
RIDGE_ALPHA = 10.0
MIN_RESOLVED_SESSIONS = 8
MIN_TRAIN_ROWS = 30
FLAT_RT = 0.0015
FLAT_SIDE = FLAT_RT / 2.0
BORROW_FEE_RATE = 0.003
CUTOFF = "2026-09-14"
LUCK_START = "2026-08-13"
LUCK_END = "2026-09-11"
PRICE_FLOOR = "2026-05-01"
# AB, S, hard-red, sector essays, news judgments, and Grok review exist
# only on the 31 morning sessions that start this day. A row dated earlier
# must not be filled by rebuilding those columns.
LLM_START = "2026-08-13"
ET = ZoneInfo("America/New_York")
_DAILY_NOTE = re.compile(r"^excel_bot/daily/(\d{4}-\d{2}-\d{2})_excel_bot\.md$")

# Theme Radar frozen 09:30 export. Off. The declared feature list does not
# include it: 2026-08-28 has no snapshot, and the files are not in this repo.
# Turning it on reads only THEME_RADAR_WHITELIST from the pinned blobs.
THEME_RADAR_ENABLED = False
THEME_RADAR_COMMIT = "3973e13cd953e5705d08d8d9f78a5b1b9dd1a1d0"
THEME_RADAR_REPO = "SRoyaltyy/theme-radar"
THEME_RADAR_FILES = (
    {
        "path": "research/lever_panel/finviz_panel_asof0930_2026-08.csv.gz",
        "sha256": "c8977b8eea8e74115899e9d4cc04d5b4ea67490376d972905781eb8e1aeb6459",
    },
    {
        "path": "research/lever_panel/finviz_panel_asof0930_2026-09.csv.gz",
        "sha256": "cbf35da9e1587703059abd9ff77525a1047c67a91edc3276ca93db4cd8669c16",
    },
)
# Join and clock columns are read to attach a row. They are not model features.
THEME_RADAR_JOIN = ("trade_date", "Ticker")
THEME_RADAR_CLOCK = ("scrape_ts_utc",)
# Explicit score and snapshot columns. Text, buckets, kill lists, the
# snapshot Open, and realized-return names are not in this tuple.
THEME_RADAR_WHITELIST = (
    "Price",
    "Market Cap",
    "Average Volume",
    "Relative Volume",
    "Short Float",
    "Short Ratio",
    "Institutional Ownership",
    "Insider Ownership",
    "Analyst Recom",
    "Performance (Week)",
    "Performance (Month)",
    "Relative Strength Index (14)",
    "EPS Surprise",
    "Beta",
    "tr1d_total_score",
    "tr1d_score_100",
    "tr1d_confidence",
    "tr1d_n_pos",
    "tr1d_n_neg",
    "tr1w_total_score",
    "tr1w_score_100",
    "tr1w_confidence",
    "tr1m_total_score",
    "tr1m_score_100",
    "tr1m_confidence",
    "trc_pressure",
    "trc_resid",
    "seg_n_themes",
)
THEME_RADAR_READ = THEME_RADAR_JOIN + THEME_RADAR_CLOCK + THEME_RADAR_WHITELIST

PANEL_NUMERIC = (
    "src_rank", "cond_good", "cond_bad",
    "ohlc_ret_1", "ohlc_ret_5", "ohlc_ret_10", "ohlc_rvol", "ohlc_hot_score",
    "candle_score", "candle_body_rg",
    "erd_days_since_E", "erd_days_since_R", "erd_days_since_D",
    "rsi", "fv_rsi", "macd", "macd_sig", "macd_hist",
    "close_loc", "fv_rvol", "fv_sma20", "fv_sma50", "fv_inst", "rs_week",
    "opp_rvol", "opp_gap_pct", "opp_change_pct",
    "n_neg",
)
PANEL_BOOL = (
    "blue", "alarm", "zero_red",
    "ohlc_nr7", "ohlc_break_10", "last_green", "last_red",
    "candle_capture", "erd_earn_react", "overnight_sched",
    "erd_flag_E", "erd_flag_R",
    "macd_cross_up", "macd_cross_down",
    "rsi_os", "rsi_ob", "macd_up", "macd_down", "flow_in",
    "ins_buy", "form4_buy", "oppset", "opp_any", "burst",
    "clk_mom_break_peer", "clk_fresh_cat_coil", "clk_earn_guide_react",
    "clk_neg_weak_fail", "clk_ext_veto", "clk_hold_vs_sector",
    "clk_insider_cash_stab", "clk_flow_coil", "clk_r_up_coil", "clk_nr7_mom",
)
BOXES = (
    "join", "sector", "gen", "news", "digest", "judge",
    "ab", "peer", "heat", "vol", "catal", "buy",
)
CATS = (
    ("e_pol", "cat_e_pol"),
    ("r_pol", "cat_r_pol"),
    ("news_prior", "cat_news_prior"),
    ("news_box", "cat_news_box"),
    ("headline_tone", "cat_headline_tone"),
)
PRICE_FEATURES = (
    "px_ret1", "px_ret5", "px_ret20", "px_gap_prior", "px_rvol_prior",
)
EXCEL_FEATURES = (
    "excel_sugg_long", "excel_sugg_short", "excel_sugg_n",
)
# Camera boxes that are an LLM or vendor-essay packet. Price boxes stay.
LLM_BOXES = (
    "join", "sector", "gen", "news", "digest", "judge", "ab", "heat", "catal",
)
LLM_CATS = {"news_prior", "news_box", "headline_tone"}
FEATURES = (
    PANEL_NUMERIC
    + tuple(f"box_{name}" for name in BOXES)
    + PANEL_BOOL
    + tuple(dest for _, dest in CATS)
    + PRICE_FEATURES
    + EXCEL_FEATURES
)

# Columns seen on the morning panel or the Excel files that this lever
# does not feed to the model. Reasons are the spec.
EXCLUDED = (
    ("open", "same-day Yahoo open; the buy fill, not a feature"),
    ("open_0930", "same print as the buy fill"),
    ("close", "same-day close is not known at 09:30 ET"),
    ("headline", "free text, not a fixed score"),
    ("e_label", "free text; polarity is cat_e_pol"),
    ("r_label", "free text; polarity is cat_r_pol"),
    ("sources", "open list of list names; src_rank is the numeric rank"),
    ("news_export_date", "a date, not a score; used only as a leak guard"),
    ("prior_date", "a date, not a score"),
    ("opp_finviz_asof", "a date; T-1 opp numbers are the features"),
    ("heat_vintage", "source stamp, same for every name that morning"),
    ("_clock_b", "pipeline flag, not a name score"),
    ("current_price", "Excel tracking mark refreshed after the signal"),
    ("ret_vs_close", "Excel tracking mark, not the morning signal"),
    ("ret_vs_open", "Excel tracking mark, not the morning signal"),
    ("days_held", "Excel tracking mark"),
    ("ref_close", "signal-day close; not used as a feature"),
    ("first_open", "the next open is the fill, not a feature"),
    ("run_date", "not a clock; the suggestions file bakes stale run dates"),
    ("signal_colors", "open color vocabulary; side and count are the features"),
    ("strategy name", "open strategy vocabulary; side and row count are the features"),
    ("signal_date == N", "confirm day uses that session's close; not known at 09:30 N"),
    ("excel clear-letter panel", "not a morning input in INPUT_HISTORY; generated 2026-09-12"),
    ("suggestions.csv live file", "rewritten through later dates and carries tracking marks"),
    ("daily note scoreboard", "live returns in the same markdown file; not the signal"),
    ("daily note commit at or after the next 09:30", "too late for that open, and not reused later"),
    ("theme_radar frozen export", "optional and off; 2026-08-28 does not join; files are not in this repo"),
    ("numeric s_ab", "not a panel column; the morning AB gate is box_ab"),
    ("morning S / hard-red", "day-level sit, not a per-name panel column; pick rule is top 4"),
    ("LLM packet before 2026-08-13", "AB, sector, news, and Grok exist only on the 31 sessions"),
)

TONE = {"good": 1.0, "neutral": 0.0, "bad": -1.0}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def _finite(value):
    if value is None or value == "":
        return None
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if text.endswith("%"):
            text = text[:-1]
        if text.lower() in {"none", "nan", "null", "nat"}:
            return None
        value = text
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _tick(value) -> str:
    return str(value or "").strip().upper()


def _as_day(value) -> str:
    if value is None:
        return ""
    text = str(value)
    return text[:10]


def training_entry_dates(sessions: list[str], day: str) -> list[str]:
    """Entry dates whose hold-1 exit open is already known before 09:30 on day.

    Hold 1 buys the open of entry D and sells the open of the next session
    E. That exit print happens at 09:30 on E. On morning N it is known only
    when E is strictly before N, which is entries at index <= N-2
    (E = N-1 at the latest). The entry on N-1 exits at today's open, and
    that open is not known before 09:30 on N.
    """
    if day not in sessions:
        raise KeyError(day)
    index = sessions.index(day)
    # index 0 and 1 have no entry whose exit session is already over.
    # sessions[:k] with a negative k would keep almost every day.
    if index < 2:
        return []
    return list(sessions[: index - 1])


def next_session(sessions: list[str], day: str) -> str | None:
    if day not in sessions:
        return None
    index = sessions.index(day)
    if index + 1 >= len(sessions):
        return None
    return sessions[index + 1]


def prior_session(sessions: list[str], day: str) -> str | None:
    if day not in sessions:
        return None
    index = sessions.index(day)
    if index == 0:
        return None
    return sessions[index - 1]


def assert_no_lookahead(sessions: list[str], day: str, entry_dates: list[str]) -> None:
    """Fail if a training entry's exit is not strictly before this morning."""
    allowed = training_entry_dates(sessions, day)
    if list(entry_dates) != [d for d in entry_dates if d in allowed]:
        raise RuntimeError(f"{day}: training entries are not a subset of days <= N-2")
    for entry in entry_dates:
        if entry >= day:
            raise RuntimeError(f"{day}: training entry {entry} is not before the decision")
        exit_day = next_session(sessions, entry)
        if exit_day is None or exit_day >= day:
            raise RuntimeError(
                f"{day}: exit of {entry} is {exit_day}, not known before 09:30"
            )


def rank_center(values: list) -> list[float]:
    """Within-day rank mapped to about [-0.5, 0.5]. Missing values sit at 0.

    Ties share the average rank. The map is (average_rank - 0.5) / n - 0.5
    with average_rank 1-based, so a lone name is 0 and a two-name day is
    symmetric.
    """
    out = [0.0] * len(values)
    usable = []
    for index, value in enumerate(values):
        number = _finite(value)
        if number is not None:
            usable.append((number, index))
    count = len(usable)
    if count == 0:
        return out
    usable.sort(key=lambda item: (item[0], item[1]))
    cursor = 0
    while cursor < count:
        end = cursor
        while end + 1 < count and usable[end + 1][0] == usable[cursor][0]:
            end += 1
        average = 0.5 * ((cursor + 1) + (end + 1))
        mapped = (average - 0.5) / count - 0.5
        for offset in range(cursor, end + 1):
            out[usable[offset][1]] = mapped
        cursor = end + 1
    return out


def _tone(value) -> float | None:
    if value is None or value == "":
        return None
    return TONE.get(str(value).strip().lower())


def _bool(value) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, str) and value.strip().lower() in {"none", "nan", "null"}:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = _finite(value)
        if number is None:
            return None
        return 1.0 if number != 0 else 0.0
    return 1.0 if bool(value) else 0.0


def excel_open_cutoff(open_date: str) -> dt.datetime:
    """09:30 ET on the session that can see the prior note. A commit at 09:30 is late."""
    day = str(open_date or "")[:10]
    return dt.datetime.fromisoformat(f"{day}T09:30:00").replace(tzinfo=ET)


def _aware(when: dt.datetime) -> dt.datetime:
    if when.tzinfo is None:
        return when.replace(tzinfo=ET)
    return when


def last_commit_before(rows: list, cutoff: dt.datetime) -> tuple | None:
    """Last row strictly before cutoff. Rows are (time, sha, ...). Sorted here."""
    ordered = sorted(rows or [], key=lambda item: _aware(item[0]))
    hit = None
    for item in ordered:
        if _aware(item[0]) < cutoff:
            hit = item
        else:
            break
    return hit


def next_visible_session(file_date: str, sessions: list[str]) -> str | None:
    """The next panel session after the note's date, if that session is before the cutoff.

    The note is a feature on that morning only. A later morning does not
    pick the note up, and a next session on or after the cutoff is unused.
    """
    day = _as_day(file_date)
    for session in sessions:
        if session > day and session < CUTOFF:
            return session
    return None


def parse_new_suggestions(text: str) -> list[dict]:
    """Ticker, side, and strategy from the New suggestions table only.

    The scoreboard and the best/worst sections sit under later headings.
    They carry live returns and are not read. A note with no table is empty.
    """
    lines = str(text or "").splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip().lower() == "## new suggestions":
            start = index + 1
            break
    if start is None:
        return []
    header = None
    rows = []
    for line in lines[start:]:
        stripped = line.strip()
        if stripped.startswith("## "):
            break
        if not stripped.startswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if header is None:
            header = [cell.lower() for cell in cells]
            continue
        if cells and all(set(cell) <= set("-: ") and cell for cell in cells):
            continue
        data = {header[i]: cells[i] for i in range(min(len(header), len(cells)))}
        ticker = _tick(data.get("ticker"))
        side = str(data.get("side") or "").strip().lower()
        if not ticker or side not in {"long", "short"}:
            continue
        rows.append({
            "ticker": ticker,
            "side": side,
            "strategy": str(data.get("strategy") or "").strip(),
        })
    return rows


def assign_pinned_signals(sessions: list[str], notes: list[dict]) -> dict[str, list]:
    """Map each daily note onto the one morning that may see it.

    ``notes`` items are ``{date, commits, blobs}``. ``commits`` are
    ``(time, sha, ...)``. ``blobs`` maps sha to the file text. The legal
    blob is the last commit strictly before the next panel session's
    09:30 ET. No such commit means the note is unused.
    """
    out: dict[str, list] = {day: [] for day in sessions if day < CUTOFF}
    for note in notes:
        file_date = _as_day(note.get("date"))
        if not file_date or file_date >= CUTOFF:
            continue
        visible = next_visible_session(file_date, sessions)
        if visible is None:
            continue
        hit = last_commit_before(note.get("commits") or [], excel_open_cutoff(visible))
        if hit is None:
            continue
        text = (note.get("blobs") or {}).get(hit[1], "")
        for row in parse_new_suggestions(text):
            kept = dict(row)
            kept["signal_date"] = file_date
            out.setdefault(visible, []).append(kept)
    return out


def _blank_llm(out: dict) -> None:
    for name in LLM_BOXES:
        out[f"box_{name}"] = None
    for source, dest in CATS:
        if source in LLM_CATS:
            out[dest] = None
    for name in PANEL_BOOL:
        if name.startswith("clk_"):
            out[name] = None


def _theme_banned(name: str) -> bool:
    """Label, outcome, future-return, hit, and realized-return names."""
    text = str(name or "").lower().replace(" ", "_")
    if text in {"trf_true_ret", "trf_true_ret_dir", "open"}:
        return True
    return re.search(
        r"(label|outcome|future_ret|fwd_ret|forward_ret|(^|_)hit($|_)|y_true|true_ret)",
        text,
    ) is not None


def assert_theme_read_list(columns) -> None:
    """Fail if a read column is outside the allow-list or is a banned name."""
    allowed = set(THEME_RADAR_READ)
    bad = [column for column in columns if column not in allowed or _theme_banned(column)]
    if bad:
        raise RuntimeError(f"non-whitelisted theme column read: {bad}")


assert_theme_read_list(THEME_RADAR_READ)


def theme_feature_name(column: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(column).lower()).strip("_")
    return "tr_" + slug


def _stamp_before_open(stamp: str, trade_date: str) -> bool:
    text = str(stamp or "").strip()
    if not text:
        return False
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        when = dt.datetime.fromisoformat(text)
    except ValueError:
        return False
    return _aware(when) < excel_open_cutoff(trade_date)


def theme_column_index(header) -> list[tuple[str, int]]:
    """Positions of allow-list columns. Extra header names are not indexed."""
    assert_theme_read_list(THEME_RADAR_READ)
    positions = []
    for name in THEME_RADAR_READ:
        if name in header:
            positions.append((name, list(header).index(name)))
    return positions


def fields_from_cells(header, cells) -> dict:
    """Build a row from allow-list cells only. Other cells are not subscripted."""
    picked = {name: None for name in THEME_RADAR_READ}
    for name, index in theme_column_index(header):
        if index < len(cells):
            picked[name] = cells[index]
    return picked


def load_theme_stream(handle, *, max_date: str = LUCK_END) -> dict:
    reader = csv.reader(handle)
    try:
        header = next(reader)
    except StopIteration:
        return {}
    packed = []
    for cells in reader:
        if not cells:
            continue
        packed.append(fields_from_cells(header, cells))
    return load_theme_rows(packed, max_date=max_date)


def take_theme_fields(raw) -> dict:
    """Copy only the allow-list. Other keys in raw are not read."""
    assert_theme_read_list(THEME_RADAR_READ)
    picked = {}
    for key in THEME_RADAR_READ:
        if key in raw:
            picked[key] = raw[key]
        else:
            picked[key] = None
    return picked


def project_theme_row(raw) -> dict:
    """Whitelist values only. A banned or extra column raises."""
    assert_theme_read_list(THEME_RADAR_WHITELIST)
    out = {}
    for key in THEME_RADAR_WHITELIST:
        if key in raw:
            raw_value = raw[key]
        else:
            raw_value = None
        out[theme_feature_name(key)] = _finite(raw_value)
    return out


def load_theme_rows(rows, *, max_date: str = LUCK_END) -> dict:
    """(trade_date, ticker) -> whitelist features, through max_date and before the cutoff.

    A second row for the same pair raises. The join is then not clean.
    A scrape stamp that is missing or not strictly before 09:30 ET is dropped.
    """
    if max_date >= CUTOFF:
        raise RuntimeError(f"theme load max_date {max_date} is on or after {CUTOFF}")
    out = {}
    for raw in rows:
        picked = take_theme_fields(raw)
        day = _as_day(picked.get("trade_date"))
        if not day or day > max_date or day >= CUTOFF:
            continue
        if not _stamp_before_open(picked.get("scrape_ts_utc"), day):
            continue
        ticker = _tick(picked.get("Ticker"))
        if not ticker:
            continue
        key = (day, ticker)
        if key in out:
            raise RuntimeError(f"theme radar join is not clean: duplicate {day} {ticker}")
        out[key] = project_theme_row(picked)
    return out


def load_pinned_theme_radar(root: Path | None = None, *, max_date: str = LUCK_END) -> dict:
    """Read the pinned gzip blobs if the flag is on. Sha256 must match the commit."""
    import gzip

    base = Path(root or ROOT)
    rows = []
    for item in THEME_RADAR_FILES:
        path = base / "data" / "theme_radar" / Path(item["path"]).name
        if not path.is_file():
            raise RuntimeError(
                f"theme radar file missing: {path.name} from {THEME_RADAR_COMMIT}"
            )
        digest = sha256_file(path)
        if digest != item["sha256"]:
            raise RuntimeError(f"theme radar sha256 mismatch for {path.name}")
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            rows.append(load_theme_stream(handle, max_date=max_date))
    merged = {}
    for part in rows:
        for key, value in part.items():
            if key in merged:
                raise RuntimeError(f"theme radar join is not clean: duplicate {key[0]} {key[1]}")
            merged[key] = value
    return merged


def row_features(row: dict, *, price_features: dict | None,
                 excel_rows: list | None, theme_row: dict | None = None) -> dict:
    """One name's raw features. None means missing (rank-fills to the center)."""
    day = _as_day(row.get("date"))
    ticker = _tick(row.get("ticker"))
    out: dict = {}
    for name in PANEL_NUMERIC:
        out[name] = _finite(row.get(name))
    boxes = row.get("boxes") or {}
    if not isinstance(boxes, dict):
        boxes = {}
    for name in BOXES:
        out[f"box_{name}"] = _tone(boxes.get(name))
    for name in PANEL_BOOL:
        out[name] = _bool(row.get(name))
    asof = _as_day(row.get("opp_finviz_asof"))
    if asof and asof >= day:
        for name in ("opp_rvol", "opp_gap_pct", "opp_change_pct", "oppset", "opp_any"):
            out[name] = None
    export_day = _as_day(row.get("news_export_date"))
    news_leak = bool(export_day and export_day >= day)
    for source, dest in CATS:
        if news_leak and source in {"news_prior", "news_box", "headline_tone"}:
            out[dest] = None
        else:
            out[dest] = _tone(row.get(source))
    px = (price_features or {}).get((day, ticker)) or {}
    for name in PRICE_FEATURES:
        out[name] = _finite(px.get(name))
    out.update(suggestion_features(ticker, excel_rows))
    if THEME_RADAR_ENABLED:
        projected = theme_row or {}
        for column in THEME_RADAR_WHITELIST:
            name = theme_feature_name(column)
            out[name] = projected.get(name)
    if day and day < LLM_START:
        _blank_llm(out)
    return out


def suggestion_features(ticker: str, rows: list | None) -> dict:
    """Long/short flags and row count for this ticker on this morning's pinned notes.

    Absence is 0, not missing: the morning either had a legal note or it did not.
    Strategy names are not a feature. Same-day signal letters are not a feature.
    """
    matched = [
        row for row in (rows or [])
        if _tick(row.get("ticker")) == _tick(ticker)
        and _as_day(row.get("signal_date") or "0000-01-01") < CUTOFF
    ]
    longs = sum(1 for row in matched if str(row.get("side") or "").lower() == "long")
    shorts = sum(1 for row in matched if str(row.get("side") or "").lower() == "short")
    return {
        "excel_sugg_long": 1.0 if longs else 0.0,
        "excel_sugg_short": 1.0 if shorts else 0.0,
        "excel_sugg_n": float(len(matched)),
    }


def feature_list() -> tuple:
    """Declared features. Theme columns are added only when the hook is on."""
    if not THEME_RADAR_ENABLED:
        return FEATURES
    extra = tuple(theme_feature_name(column) for column in THEME_RADAR_WHITELIST)
    return FEATURES + extra


def rank_rows(raws: list[dict]) -> list[dict]:
    """Attach a rank vector in feature_list order. raws items need an 'x' dict."""
    names = feature_list()
    columns = []
    for name in names:
        columns.append(rank_center([item["x"].get(name) for item in raws]))
    ranked = []
    for index, item in enumerate(raws):
        vector = [columns[col][index] for col in range(len(names))]
        nxt = dict(item)
        nxt["rank"] = vector
        ranked.append(nxt)
    return ranked


def ridge_fit(x_matrix: np.ndarray, y: np.ndarray, alpha: float) -> np.ndarray:
    """Ridge with an unpenalized intercept. Returns coef of length p+1.

    The last entry is the intercept. Deterministic. No CV.
    """
    n_rows, n_cols = x_matrix.shape
    if n_rows < 1 or n_cols < 1:
        raise RuntimeError("ridge needs rows and columns")
    design = np.zeros((n_cols + 1, n_cols + 1), dtype=np.float64)
    gram = x_matrix.T @ x_matrix
    column_sum = x_matrix.sum(axis=0)
    design[:n_cols, :n_cols] = gram
    design[:n_cols, n_cols] = column_sum
    design[n_cols, :n_cols] = column_sum
    design[n_cols, n_cols] = float(n_rows)
    penalty = np.eye(n_cols + 1, dtype=np.float64) * float(alpha)
    penalty[n_cols, n_cols] = 0.0
    design = design + penalty
    target = np.zeros(n_cols + 1, dtype=np.float64)
    target[:n_cols] = x_matrix.T @ y
    target[n_cols] = float(y.sum())
    try:
        coef = np.linalg.solve(design, target)
    except np.linalg.LinAlgError:
        coef = np.linalg.lstsq(design, target, rcond=None)[0]
    return coef


def standardize_fit(ranks: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    mean = ranks.mean(axis=0)
    std = ranks.std(axis=0)
    flat = std < 1e-12
    safe = np.where(flat, 1.0, std)
    return mean, safe, flat


def apply_standard(ranks: np.ndarray, mean, safe, flat) -> np.ndarray:
    scaled = (ranks - mean) / safe
    if np.any(flat):
        scaled[:, flat] = 0.0
    return scaled


def fit_model(train_items: list[dict], alpha: float = RIDGE_ALPHA) -> dict:
    ranks = np.array([item["rank"] for item in train_items], dtype=np.float64)
    target = np.array([item["y"] for item in train_items], dtype=np.float64)
    mean, safe, flat = standardize_fit(ranks)
    scaled = apply_standard(ranks, mean, safe, flat)
    coef = ridge_fit(scaled, target, alpha)
    return {"mu": mean, "sd": safe, "flat": flat, "coef": coef, "alpha": float(alpha)}


def predict(model: dict, items: list[dict]) -> list[float]:
    if not items:
        return []
    ranks = np.array([item["rank"] for item in items], dtype=np.float64)
    scaled = apply_standard(ranks, model["mu"], model["sd"], model["flat"])
    beta = model["coef"][:-1]
    intercept = float(model["coef"][-1])
    scores = scaled @ beta + intercept
    return [float(value) for value in scores]


def load_fees(path: Path | None = None) -> dict:
    src = path or (ROOT / "00_grounding" / "futubull_fees.json")
    raw = json.loads(src.read_text(encoding="utf-8"))
    out = {}
    for key, value in raw.items():
        if str(key).startswith("_") or key in {"currency", "paper_account"}:
            continue
        if isinstance(value, (int, float)):
            out[key] = float(value)
    return out


def order_fees(shares: int, price: float, side: str, fees: dict) -> float:
    """Futubull US order fees. Same schedule as paper_trade.order_fees."""
    if shares <= 0 or price <= 0:
        return 0.0
    amount = shares * price
    comm = min(
        max(fees["commission_per_share"] * shares, fees["commission_min_per_order"]),
        fees["commission_max_pct_of_amount"] * amount,
    )
    plat = min(
        max(fees["platform_per_share"] * shares, fees["platform_min_per_order"]),
        fees["platform_max_pct_of_amount"] * amount,
    )
    settle = fees["settlement_per_share"] * shares
    total = comm + plat + settle
    if side == "sell":
        reg = max(
            fees["regulatory_pct_of_amount_sell_only"] * amount,
            fees["regulatory_min_per_order"],
        )
        taf = min(
            max(fees["taf_per_share_sell_only"] * shares, fees["taf_min_per_order"]),
            fees["taf_max_per_order"],
        )
        total += reg + taf
    return round(total, 4)


def flat_fee(shares: int, price: float) -> float:
    if shares <= 0 or price <= 0:
        return 0.0
    return round(shares * price * FLAT_SIDE, 4)


def borrow_fee(notional: float) -> float:
    """0.3% of short notional. This lever is long-only, so the book never pays it."""
    return round(abs(float(notional)) * BORROW_FEE_RATE, 4)


def shares_for_budget(price: float, budget: float, fees: dict) -> int:
    if price <= 0 or budget <= 0:
        return 0
    shares = int(budget / price)
    while shares > 0:
        fee = order_fees(shares, price, "buy", fees)
        if shares * price + fee <= budget + 1e-8:
            return shares
        shares -= 1
    return 0


def price_features_from_bars(bars: list[dict], day: str) -> dict:
    """Prior-bar features. bars are {date, open, close, volume}, any order.

    Only bars with date < day and date < CUTOFF are read. Today's open is
    not a feature. ret20 needs 21 prior bars. rvol matches ohlc_ripper:
    last prior volume over the mean of the last 20 prior volumes (those
    20 include the last bar) when at least 8 prior bars exist.
    """
    prior = [
        bar for bar in bars
        if _as_day(bar.get("date")) < day and _as_day(bar.get("date")) < CUTOFF
    ]
    prior.sort(key=lambda bar: _as_day(bar.get("date")))
    out = {name: None for name in PRICE_FEATURES}
    if len(prior) < 2:
        return out
    closes = [_finite(bar.get("close")) for bar in prior]
    opens = [_finite(bar.get("open")) for bar in prior]
    volumes = [_finite(bar.get("volume")) or 0.0 for bar in prior]
    c1, c0 = closes[-1], closes[-2]
    o1 = opens[-1]
    if c1 and c0 and c0 != 0:
        out["px_ret1"] = c1 / c0 - 1.0
    if o1 and c0 and c0 != 0:
        out["px_gap_prior"] = o1 / c0 - 1.0
    if len(prior) >= 6 and closes[-1] and closes[-6]:
        out["px_ret5"] = closes[-1] / closes[-6] - 1.0
    if len(prior) >= 21 and closes[-1] and closes[-21]:
        out["px_ret20"] = closes[-1] / closes[-21] - 1.0
    if len(prior) >= 8:
        window = volumes[-20:]
        mean = float(sum(window) / len(window)) if window else 0.0
        if mean > 0:
            out["px_rvol_prior"] = volumes[-1] / mean
    return out


def open_on(bars: list[dict], day: str) -> float | None:
    for bar in bars:
        if _as_day(bar.get("date")) != day:
            continue
        if _as_day(bar.get("date")) >= CUTOFF:
            return None
        px = _finite(bar.get("open"))
        if px is not None and px > 0:
            return px
    return None


def hold_target(entry_open: float | None, exit_open: float | None) -> float | None:
    """Open of D to open of the next session, minus 15bp. None if either is missing."""
    if entry_open is None or exit_open is None:
        return None
    if entry_open <= 0 or exit_open <= 0:
        return None
    return exit_open / entry_open - 1.0 - FLAT_RT


def build_day_items(rows: list[dict], day: str, sessions: list[str], *,
                    excel_by_day, bars_by_ticker, theme_by_key=None) -> list[dict]:
    morning_rows = (excel_by_day or {}).get(day) or []
    raws = []
    seen = set()
    ordered = sorted(rows, key=lambda row: (_tick(row.get("ticker")), int(row.get("src_rank") or 0)))
    for row in ordered:
        if _as_day(row.get("date")) != day:
            continue
        ticker = _tick(row.get("ticker"))
        if not ticker or ticker in seen:
            continue
        if day >= CUTOFF:
            continue
        seen.add(ticker)
        px = price_features_from_bars(bars_by_ticker.get(ticker) or [], day)
        theme_row = (theme_by_key or {}).get((day, ticker)) if THEME_RADAR_ENABLED else None
        raws.append({
            "ticker": ticker,
            "date": day,
            "x": row_features(
                row, price_features={(day, ticker): px}, excel_rows=morning_rows,
                theme_row=theme_row,
            ),
        })
    return rank_rows(raws)


def training_items(panel_by_day: dict, sessions: list[str], day: str, *,
                   excel_by_day, bars_by_ticker, theme_by_key=None) -> list[dict]:
    entries = training_entry_dates(sessions, day)
    assert_no_lookahead(sessions, day, entries)
    items = []
    for entry in entries:
        exit_day = next_session(sessions, entry)
        if exit_day is None or exit_day >= CUTOFF or exit_day >= day:
            continue
        for item in build_day_items(
            panel_by_day.get(entry) or [], entry, sessions,
            excel_by_day=excel_by_day, bars_by_ticker=bars_by_ticker,
            theme_by_key=theme_by_key,
        ):
            ticker = item["ticker"]
            bars = bars_by_ticker.get(ticker) or []
            target = hold_target(open_on(bars, entry), open_on(bars, exit_day))
            if target is None:
                continue
            kept = dict(item)
            kept["y"] = target
            kept["entry"] = entry
            kept["exit"] = exit_day
            items.append(kept)
    return items


def choose_picks(scored: list[dict], bars_by_ticker: dict, day: str,
                 held: set[str]) -> list[dict]:
    """Top TOP_N by score, ticker A-Z on ties, among names with a positive open."""
    ranked = sorted(scored, key=lambda item: (-float(item["score"]), item["ticker"]))
    picks = []
    for item in ranked:
        if item["ticker"] in held:
            continue
        px = open_on(bars_by_ticker.get(item["ticker"]) or [], day)
        if px is None:
            continue
        picks.append({
            "ticker": item["ticker"],
            "score": round(float(item["score"]), 6),
            "price": px,
        })
        if len(picks) >= TOP_N:
            break
    return picks


def _mark(positions: dict) -> float:
    total = 0.0
    for lot in positions.values():
        total += int(lot["shares"]) * float(lot["last_px"])
    return total


def settle_day(day: str, sessions: list[str], picks: list[dict], *,
               cash: float, cash_flat: float, positions: dict,
               fees: dict) -> dict:
    """Sell hold-1 lots at today's open, then buy today's picks. Long only."""
    index = {date: pos for pos, date in enumerate(sessions)}
    sells = []
    for ticker in sorted(list(positions)):
        lot = positions[ticker]
        held = index[day] - index.get(lot["entry_date"], index[day])
        if held < HOLD:
            continue
        px = lot.get("exit_px")
        if px is None or px <= 0:
            lot["unpriced"] = True
            continue
        shares = int(lot["shares"])
        fee = order_fees(shares, px, "sell", fees)
        fee_flat = flat_fee(shares, px)
        proceeds = shares * px - fee
        cash = cash + proceeds
        cash_flat = cash_flat + shares * px - fee_flat
        pnl = proceeds - float(lot["cost"])
        sells.append({
            "ticker": ticker,
            "side": "SELL",
            "shares": shares,
            "price": round(float(px), 4),
            "fees": fee,
            "fees_15bp": fee_flat,
            "pnl": round(pnl, 4),
        })
        del positions[ticker]
    buys = []
    n_names = len(picks)
    budget = cash / n_names if n_names else 0.0
    planned = []
    for pick in picks:
        if pick["ticker"] in positions:
            continue
        shares = shares_for_budget(float(pick["price"]), budget, fees)
        if shares < 1:
            continue
        planned.append((pick, shares))
    for pick, shares in planned:
        px = float(pick["price"])
        fee = order_fees(shares, px, "buy", fees)
        fee_flat = flat_fee(shares, px)
        cost = shares * px + fee
        if cost > cash + 1e-6:
            continue
        cash -= cost
        cash_flat -= shares * px + fee_flat
        positions[pick["ticker"]] = {
            "shares": shares,
            "entry_px": px,
            "last_px": px,
            "entry_date": day,
            "cost": shares * px + fee,
        }
        buys.append({
            "ticker": pick["ticker"],
            "side": "BUY",
            "shares": shares,
            "price": round(px, 4),
            "fees": fee,
            "fees_15bp": fee_flat,
            "score": pick.get("score"),
        })
    equity = round(cash + _mark(positions), 2)
    # Flat ledger marks the same shares at the same prices. Rebuild the
    # mark from positions (identical) and use cash_flat.
    equity_flat = round(cash_flat + _mark(positions), 2)
    return {
        "cash": cash,
        "cash_flat": cash_flat,
        "positions": positions,
        "equity": equity,
        "equity_flat": equity_flat,
        "buys": buys,
        "sells": sells,
    }


def _canonical(doc: dict) -> str:
    return json.dumps(doc, indent=2, sort_keys=True) + "\n"


def _state_path(state_dir: Path, day: str) -> Path:
    return state_dir / f"{day}.json"


def _read_state(state_dir: Path | None, day: str) -> dict | None:
    if state_dir is None:
        return None
    path = _state_path(state_dir, day)
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_state(state_dir: Path | None, day: str, body: dict) -> None:
    if state_dir is None:
        return
    state_dir.mkdir(parents=True, exist_ok=True)
    path = _state_path(state_dir, day)
    text = _canonical(body)
    digest = sha256_bytes(text.encode("utf-8"))
    payload = dict(body)
    payload["sha256"] = digest
    path.write_text(_canonical(payload), encoding="utf-8")


def _locked_matches(saved: dict, fresh: dict) -> bool:
    left = {key: value for key, value in saved.items() if key != "sha256"}
    return _canonical(left) == _canonical(fresh)


def walk(sessions: list[str], panel_by_day: dict, bars_by_ticker: dict, *,
         excel_by_day=None, theme_by_key=None, fees: dict | None = None,
         state_dir: Path | None = None, start: str | None = None,
         end: str | None = None) -> dict:
    """Fit and trade one session at a time. Refuses the cutoff and after."""
    np.random.seed(SEED)
    if end is not None and end >= CUTOFF:
        raise RuntimeError(f"refusing to score {end}; cutoff is {CUTOFF}")
    sessions = [day for day in sessions if day < CUTOFF]
    clock = [day for day in sessions if day < CUTOFF]
    if start is not None:
        clock = [day for day in clock if day >= start]
    if end is not None:
        clock = [day for day in clock if day <= end]
    if any(day >= CUTOFF for day in clock):
        raise RuntimeError("clock contains a cutoff date")
    if THEME_RADAR_ENABLED and theme_by_key is None:
        theme_by_key = load_pinned_theme_radar()
    fees = fees if fees is not None else load_fees()
    cash = CAPITAL
    cash_flat = CAPITAL
    positions: dict = {}
    prev_equity = CAPITAL
    prev_equity_flat = CAPITAL
    daily = []
    for day in clock:
        saved = _read_state(state_dir, day)
        items = training_items(
            panel_by_day, sessions, day,
            excel_by_day=excel_by_day, bars_by_ticker=bars_by_ticker,
            theme_by_key=theme_by_key,
        )
        # Attach today's exit opens onto lots before the sell, from the tape.
        still_held = set()
        day_index = sessions.index(day)
        for ticker, lot in positions.items():
            px = open_on(bars_by_ticker.get(ticker) or [], day)
            lot["exit_px"] = px
            held_for = day_index - sessions.index(lot["entry_date"])
            if held_for < HOLD or px is None:
                still_held.add(ticker)
        picked = []
        n_train = len(items)
        n_sessions = len(training_entry_dates(sessions, day))
        sat = n_sessions < MIN_RESOLVED_SESSIONS or n_train < MIN_TRAIN_ROWS
        if not sat:
            model = fit_model(items, RIDGE_ALPHA)
            today = build_day_items(
                panel_by_day.get(day) or [], day, sessions,
                excel_by_day=excel_by_day, bars_by_ticker=bars_by_ticker,
                theme_by_key=theme_by_key,
            )
            scores = predict(model, today)
            scored = []
            for item, score in zip(today, scores):
                scored.append({"ticker": item["ticker"], "score": score})
            picked = choose_picks(scored, bars_by_ticker, day, still_held)
        settled = settle_day(
            day, sessions, picked, cash=cash, cash_flat=cash_flat,
            positions=positions, fees=fees,
        )
        cash = settled["cash"]
        cash_flat = settled["cash_flat"]
        positions = settled["positions"]
        equity = settled["equity"]
        equity_flat = settled["equity_flat"]
        ret = 0.0 if prev_equity == 0 else round(100.0 * (equity / prev_equity - 1.0), 4)
        ret_flat = 0.0 if prev_equity_flat == 0 else round(
            100.0 * (equity_flat / prev_equity_flat - 1.0), 4
        )
        body = {
            "buys": settled["buys"],
            "cash": round(cash, 4),
            "cash_flat": round(cash_flat, 4),
            "date": day,
            "designed_after": True,
            "equity": equity,
            "equity_15bp": equity_flat,
            "holdings": sorted(positions),
            "n": len(settled["buys"]),
            "n_train_rows": n_train,
            "n_train_sessions": n_sessions,
            "picks": [row["ticker"] for row in settled["buys"]],
            "ret_pct": ret,
            "ret_pct_flat_15bp": ret_flat,
            "sat": sat,
            "scores": [
                {"score": row.get("score"), "ticker": row["ticker"]}
                for row in settled["buys"]
            ],
            "sells": settled["sells"],
        }
        if saved is not None and not _locked_matches(saved, body):
            raise RuntimeError(f"locked day {day} does not match a fresh walk")
        if saved is None:
            _write_state(state_dir, day, body)
        daily.append({
            "date": day,
            "ret_pct": ret,
            "ret_pct_flat_15bp": ret_flat,
            "picks": body["picks"],
            "n": body["n"],
            "equity": equity,
            "equity_15bp": equity_flat,
            "sat": sat,
            "designed_after": True,
            "buys": settled["buys"],
            "sells": settled["sells"],
        })
        prev_equity = equity
        prev_equity_flat = equity_flat
        print(
            f"[excel_ml_lever] {day} sat={sat} picks={body['picks']} "
            f"ret={ret} flat={ret_flat} train_sessions={n_sessions}",
            flush=True,
        )
    return report_from_daily(daily, clock)


def report_from_daily(daily: list[dict], clock: list[str]) -> dict:
    equity = CAPITAL if not daily else float(daily[-1]["equity"])
    equity_flat = CAPITAL if not daily else float(daily[-1]["equity_15bp"])
    picked_days = [row for row in daily if row.get("n")]
    return {
        "id": LEVER_ID,
        "author": AUTHOR,
        "family": FAMILY,
        "created_on": CREATED_ON,
        "hold": HOLD,
        "side": "long",
        "top_n": TOP_N,
        "seed": SEED,
        "ridge_alpha": RIDGE_ALPHA,
        "min_resolved_sessions": MIN_RESOLVED_SESSIONS,
        "min_train_rows": MIN_TRAIN_ROWS,
        "cutoff": CUTOFF,
        "designed_after": True,
        "note": (
            "Luck-test window only. Every session is before the lever's "
            "creation date, so the series is designed_after and is not a "
            "real record. No session on or after 2026-09-14 is scored. "
            "The last entry's open-to-next-open move is not in this window "
            "because that exit is the next session, which is not scored."
        ),
        "window": {
            "start": clock[0] if clock else None,
            "end": clock[-1] if clock else None,
        },
        "daily": [
            {
                "date": row["date"],
                "ret_pct": row["ret_pct"],
                "ret_pct_flat_15bp": row["ret_pct_flat_15bp"],
                "picks": list(row.get("picks") or []),
                "n": row.get("n") or 0,
                "sat": bool(row.get("sat")),
                "designed_after": True,
            }
            for row in daily
        ],
        "fills": [
            {
                "date": row["date"],
                "buys": row.get("buys") or [],
                "sells": row.get("sells") or [],
            }
            for row in daily
        ],
        "after_fees_return": round(100.0 * (equity / CAPITAL - 1.0), 4),
        "after_fees_return_15bp": round(100.0 * (equity_flat / CAPITAL - 1.0), 4),
        "final_equity": equity,
        "final_equity_15bp": equity_flat,
        "n_days": len(daily),
        "n_days_picked": len(picked_days),
        "fee": "futubull",
        "flat_fee": "15bp",
        "borrow_if_short": BORROW_FEE_RATE,
    }


def filter_panel(doc: dict, *, cutoff: str = CUTOFF) -> tuple[list[str], dict]:
    """Drop every row and session on or after the cutoff before any fit."""
    sessions = [day for day in (doc.get("session_dates") or []) if day < cutoff]
    by_day: dict[str, list] = {day: [] for day in sessions}
    for row in doc.get("rows") or []:
        day = _as_day(row.get("date"))
        if not day or day >= cutoff or day not in by_day:
            continue
        by_day[day].append(row)
    return sessions, by_day


MANIFEST_NAME = "input_manifest.json"
SIGNAL_COLUMNS = ("signal_date", "ticker", "side", "strategy")


def load_panel(path: Path | None = None) -> tuple[list[str], dict]:
    """The worktree panel is today's file. It is not a pre-open copy of a past morning."""
    if path is None or path == ROOT / "data" / "factor_mine" / "panel.json":
        raise RuntimeError(
            "refusing to read today's panel.json; past mornings come from the pre-open manifest"
        )
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    return filter_panel(doc)


def load_input_manifest(path: Path | None = None) -> dict:
    src = path or (HERE / MANIFEST_NAME)
    return json.loads(src.read_text(encoding="utf-8"))


def server_before_open(stamp: str, day: str) -> bool:
    """True when a GitHub Actions run_started_at is strictly before 09:30 ET."""
    text = str(stamp or "").strip()
    if not text:
        return False
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        when = dt.datetime.fromisoformat(text)
    except ValueError:
        return False
    return _aware(when) < excel_open_cutoff(day)


def panel_has_morning(doc: dict, day: str) -> bool:
    """The pinned blob covers day only when that morning's rows are already in it."""
    if not day or day >= CUTOFF:
        return False
    sessions = [_as_day(item) for item in (doc.get("session_dates") or [])]
    if day not in sessions:
        return False
    return any(_as_day(row.get("date")) == day for row in (doc.get("rows") or []))


def signal_rows_from_header(header, cell_rows, *, morning: str) -> list[dict]:
    """Signal columns only. Tracking-price cells are not subscripted.

    A row whose signal_date is this morning or later is the confirm that
    uses this session's close, so it is not a feature.
    """
    positions = []
    for name in SIGNAL_COLUMNS:
        if name not in list(header):
            raise RuntimeError(f"suggestions file is missing {name}")
        positions.append((name, list(header).index(name)))
    out = []
    for cells in cell_rows:
        picked = {}
        for name, index in positions:
            picked[name] = cells[index] if index < len(cells) else ""
        sig = _as_day(picked.get("signal_date"))
        if not sig or sig >= morning or sig >= CUTOFF:
            continue
        ticker = _tick(picked.get("ticker"))
        side = str(picked.get("side") or "").strip().lower()
        if not ticker or side not in {"long", "short"}:
            continue
        out.append({
            "ticker": ticker,
            "side": side,
            "strategy": str(picked.get("strategy") or "").strip(),
            "signal_date": sig,
        })
    return out


def series_from_manifest(manifest: dict, read_blob) -> dict:
    """Score only mornings whose own list is in the pre-open tree.

    ``read_blob(commit, path)`` is the only way a file is loaded. The
    worktree ``panel.json`` is not opened. A day whose pinned blob has no
    row for that morning is dropped, not filled from a later commit.
    """
    dropped = []
    for day in sorted((manifest.get("days") or {})):
        if day >= CUTOFF:
            raise RuntimeError(f"manifest contains {day}")
        slot = manifest["days"][day]
        if not server_before_open(slot.get("server_time"), day):
            raise RuntimeError(f"{day} snapshot {slot.get('server_time')} is not before 09:30 ET")
        if slot.get("status") != "loaded":
            dropped.append({
                "date": day,
                "reason": slot.get("reason"),
                "commit": slot.get("commit"),
                "server_time": slot.get("server_time"),
            })
            continue
        raw = read_blob(slot["commit"], "data/factor_mine/panel.json")
        doc = json.loads(raw)
        if panel_has_morning(doc, day):
            raise RuntimeError(
                f"{day} has a pre-open morning list; the blob walk is required and the worktree file is refused"
            )
        dropped.append({
            "date": day,
            "reason": "pinned blob has no row for this morning",
            "commit": slot.get("commit"),
            "server_time": slot.get("server_time"),
        })
    return {
        "id": LEVER_ID,
        "author": AUTHOR,
        "family": FAMILY,
        "created_on": CREATED_ON,
        "hold": HOLD,
        "side": "long",
        "top_n": TOP_N,
        "seed": SEED,
        "ridge_alpha": RIDGE_ALPHA,
        "min_resolved_sessions": MIN_RESOLVED_SESSIONS,
        "min_train_rows": MIN_TRAIN_ROWS,
        "cutoff": CUTOFF,
        "designed_after": True,
        "note": (
            "Every morning in 2026-08-13..2026-09-11 lacks a pre-open copy of "
            "its own candidate list. Those days are dropped. Today's panel.json "
            "is not substituted. Training sessions: 0. Picks: 0."
        ),
        "window": {"start": None, "end": None},
        "daily": [],
        "fills": [],
        "dropped": dropped,
        "after_fees_return": 0.0,
        "after_fees_return_15bp": 0.0,
        "final_equity": CAPITAL,
        "final_equity_15bp": CAPITAL,
        "n_days": 0,
        "n_days_picked": 0,
        "n_train_sessions": 0,
        "fee": "futubull",
        "flat_fee": "15bp",
        "borrow_if_short": BORROW_FEE_RATE,
    }


def git_commit_rows(path: str, repo: Path | None = None) -> list[tuple]:
    """Oldest-first (time, sha, stamp) for one path. Empty if git has no history."""
    root = Path(repo or ROOT)
    out = subprocess.run(
        ["git", "log", "--pretty=format:%cI %H", "--", path],
        cwd=root, capture_output=True, text=True, check=False,
    )
    rows = []
    for line in (out.stdout or "").splitlines():
        line = line.strip()
        if not line or " " not in line:
            continue
        stamp, sha = line.split(" ", 1)
        text = stamp.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            when = dt.datetime.fromisoformat(text)
        except ValueError:
            continue
        rows.append((_aware(when), sha.strip(), stamp.strip()))
    rows.sort(key=lambda item: item[0])
    return rows


def git_show(sha: str, path: str, repo: Path | None = None) -> str:
    root = Path(repo or ROOT)
    out = subprocess.run(
        ["git", "show", f"{sha}:{path}"],
        cwd=root, capture_output=True, text=True, check=False,
    )
    if out.returncode != 0:
        return ""
    return out.stdout


def list_daily_notes(repo: Path | None = None) -> list[tuple[str, str]]:
    """(file date, repo-relative path) for final daily notes. Drafts are not signal files."""
    root = Path(repo or ROOT)
    out = subprocess.run(
        ["git", "ls-files", "excel_bot/daily"],
        cwd=root, capture_output=True, text=True, check=False,
    )
    found = []
    for line in (out.stdout or "").splitlines():
        rel = line.strip().replace("\\", "/")
        match = _DAILY_NOTE.match(rel)
        if match:
            found.append((match.group(1), rel))
    return sorted(found)


def load_pinned_excel(sessions: list[str], repo: Path | None = None) -> dict[str, list]:
    """Pinned New-suggestions rows, keyed by the morning that may trade them.

    For a note dated D, the blob is the last commit strictly before the next
    panel session's 09:30 ET. That blob is attached only to that next session.
    A note whose next session is on or after the cutoff is not loaded.
    """
    root = Path(repo or ROOT)
    notes = []
    for file_date, rel in list_daily_notes(root):
        if file_date >= CUTOFF:
            continue
        visible = next_visible_session(file_date, sessions)
        if visible is None:
            continue
        commits = git_commit_rows(rel, root)
        hit = last_commit_before(commits, excel_open_cutoff(visible))
        if hit is None:
            continue
        notes.append({
            "date": file_date,
            "commits": commits,
            "blobs": {hit[1]: git_show(hit[1], rel, root)},
        })
    return assign_pinned_signals(sessions, notes)


def load_bars(tickers: set[str], *, price_path: Path | None = None,
              min_date: str = PRICE_FLOOR, max_date: str = LUCK_END) -> dict:
    """Yahoo split-adjusted bars (auto_adjust false) through max_date.

    The store file also holds later sessions. Those rows are filtered out
    before the dict is returned. A remaining bar on or after the cutoff
    fails the run.
    """
    import datetime as dt
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    if max_date >= CUTOFF:
        raise RuntimeError(f"price load max_date {max_date} is on or after {CUTOFF}")
    src = price_path or (ROOT / "data" / "prices" / "ohlc.parquet")
    # Timestamp filter. The column is timestamp[ms].
    start = pa.scalar(dt.datetime.fromisoformat(min_date), type=pa.timestamp("ms"))
    stop = pa.scalar(dt.datetime.fromisoformat(max_date), type=pa.timestamp("ms"))
    table = pq.read_table(
        src,
        columns=["date", "ticker", "open", "close", "volume"],
        filters=[
            ("date", ">=", start),
            ("date", "<=", stop),
        ],
    )
    if tickers:
        mask = pc.is_in(table["ticker"], value_set=pa.array(sorted(tickers)))
        table = table.filter(mask)
    dates = table.column("date").to_pylist()
    names = table.column("ticker").to_pylist()
    opens = table.column("open").to_pylist()
    closes = table.column("close").to_pylist()
    volumes = table.column("volume").to_pylist()
    by: dict[str, list] = {}
    for day, ticker, opened, closed, volume in zip(dates, names, opens, closes, volumes):
        text = _as_day(day)
        if text >= CUTOFF or text > max_date:
            raise RuntimeError(f"price row {ticker} {text} is past the load cap")
        by.setdefault(_tick(ticker), []).append({
            "date": text,
            "open": opened,
            "close": closed,
            "volume": volume,
        })
    for rows in by.values():
        rows.sort(key=lambda bar: bar["date"])
    return by


def panel_tickers(panel_by_day: dict) -> set[str]:
    names = set()
    for rows in panel_by_day.values():
        for row in rows:
            ticker = _tick(row.get("ticker"))
            if ticker:
                names.add(ticker)
    return names


def write_outputs(report: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "daily_returns.json").write_text(_canonical(report), encoding="utf-8")
    csv_path = out_dir / "daily_returns.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow([
            "recipe", "recipe_created_date", "start_date", "D",
            "net_ret_futubull", "net_ret_15bp", "day_status",
            "designed_after", "n_picks", "picks",
        ])
        start = (report.get("window") or {}).get("start") or ""
        for row in report.get("daily") or []:
            status = "sat" if row.get("sat") else "picked"
            writer.writerow([
                report["id"], CREATED_ON, start, row["date"],
                f"{float(row['ret_pct']):.4f}",
                f"{float(row['ret_pct_flat_15bp']):.4f}",
                status, "true", row.get("n") or 0,
                " ".join(row.get("picks") or []),
            ])
    fills = {"id": report["id"], "fills": report.get("fills") or []}
    (out_dir / "fills.json").write_text(_canonical(fills), encoding="utf-8")


def run_luck_test(*, state_dir: Path | None = None, out_dir: Path | None = None) -> dict:
    """Luck-test window. Days without a pre-open morning list are dropped.

    Nothing is read from the worktree panel, the worktree price file, or
    the worktree suggestions file. No session on or after 2026-09-14 is scored.
    """
    manifest = load_input_manifest()

    def read_blob(commit: str, path: str) -> bytes:
        if path == "data/factor_mine/panel.json" and commit in {"", "WORKTREE"}:
            raise RuntimeError("refusing to read today's panel.json")
        import subprocess
        return subprocess.check_output(["git", "show", f"{commit}:{path}"])

    report = series_from_manifest(manifest, read_blob)
    # Dropped days have no locked state. A previous series must not remain.
    if state_dir is not None and state_dir.is_dir():
        for child in state_dir.glob("*.json"):
            child.unlink()
    write_outputs(report, out_dir if out_dir is not None else HERE / "outputs")
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Walk the Excel ML lever one day at a time")
    parser.add_argument("--start", default=LUCK_START)
    parser.add_argument("--end", default=LUCK_END)
    parser.add_argument("--out", default=str(HERE / "outputs"))
    parser.add_argument("--state", default=str(HERE / "state"))
    args = parser.parse_args(argv)
    if args.end >= CUTOFF or args.start >= CUTOFF:
        raise SystemExit(f"refusing range {args.start}..{args.end}")
    if args.start != LUCK_START or args.end != LUCK_END:
        raise SystemExit("refusing to rebuild from today's panel.json")
    report = run_luck_test(state_dir=Path(args.state), out_dir=Path(args.out))
    print(
        f"[excel_ml_lever] days={report['n_days']} picked={report['n_days_picked']} "
        f"futubull={report['after_fees_return']} flat15={report['after_fees_return_15bp']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
