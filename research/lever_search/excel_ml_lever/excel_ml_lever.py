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
import hashlib
import json
import math
from pathlib import Path

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

# Theme Radar frozen export (SRoyaltyy/theme-radar, due 17:00 HKT).
# Not in this repo when the spec was frozen. Off. Turning it on is a new
# strategy under IRONCLAD rule 4, not a rerun of this lever.
THEME_RADAR_ENABLED = False
THEME_RADAR_EXPORT = "data/theme_radar/frozen_export.json"

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
    "excel_FQ", "excel_ER", "excel_EP", "excel_AH", "excel_FR",
    "excel_prior_hammer",
    "excel_sugg_long", "excel_sugg_short", "excel_sugg_n",
)
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
    ("signal_date == N", "confirm day uses that session's close; not known at 09:30 N"),
    ("theme_radar frozen export", "file not in the repo; hook is off"),
    ("numeric s_ab", "not a panel column; the morning AB gate is box_ab"),
    ("morning S / hard-red", "day-level sit, not a per-name panel column; pick rule is top 4"),
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


def prior_hammer(text) -> float | None:
    raw = str(text or "").strip()
    if raw.lower() in {"", "none", "nan", "null"}:
        return None
    if "Hammer" in raw and "Inverted" not in raw:
        return 1.0
    return 0.0


def row_features(row: dict, *, letters: dict | None, suggestions: dict | None,
                 price_features: dict | None, prior: str | None) -> dict:
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
    letter = (letters or {}).get((day, ticker)) or {}
    for name in ("excel_FQ", "excel_ER", "excel_EP", "excel_AH", "excel_FR"):
        out[name] = _finite(letter.get(name))
    if letter:
        out["excel_prior_hammer"] = prior_hammer(letter.get("DF_lag1"))
    else:
        out["excel_prior_hammer"] = None
    sugg = suggestion_features(ticker, prior, suggestions)
    out.update(sugg)
    if THEME_RADAR_ENABLED:
        raise RuntimeError("theme radar hook is off in the frozen lever")
    return out


def suggestion_features(ticker: str, prior: str | None,
                        suggestions: dict | None) -> dict:
    """Prior-session Excel suggestions only.

    signal_date is the confirm day (that session's completed colors). The
    job writes after the US close, and the fill is the next open. A row is
    a morning feature on N only when signal_date is the prior session.
    signal_date == N is the same day's close and is excluded. run_date is
    not a clock.
    """
    empty = {
        "excel_sugg_long": 0.0 if prior else None,
        "excel_sugg_short": 0.0 if prior else None,
        "excel_sugg_n": 0.0 if prior else None,
    }
    if not prior:
        return empty
    rows = (suggestions or {}).get(ticker) or []
    matched = [
        row for row in rows
        if _as_day(row.get("signal_date")) == prior
        and _as_day(row.get("signal_date")) < CUTOFF
    ]
    longs = sum(1 for row in matched if str(row.get("side") or "").lower() == "long")
    shorts = sum(1 for row in matched if str(row.get("side") or "").lower() == "short")
    return {
        "excel_sugg_long": 1.0 if longs else 0.0,
        "excel_sugg_short": 1.0 if shorts else 0.0,
        "excel_sugg_n": float(len(matched)),
    }


def rank_rows(raws: list[dict]) -> list[dict]:
    """Attach a rank vector in FEATURES order. raws items need an 'x' dict."""
    columns = []
    for name in FEATURES:
        columns.append(rank_center([item["x"].get(name) for item in raws]))
    ranked = []
    for index, item in enumerate(raws):
        vector = [columns[col][index] for col in range(len(FEATURES))]
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
                    letters, suggestions, bars_by_ticker) -> list[dict]:
    prior = prior_session(sessions, day)
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
        raws.append({
            "ticker": ticker,
            "date": day,
            "x": row_features(
                row, letters=letters, suggestions=suggestions,
                price_features={(day, ticker): px}, prior=prior,
            ),
        })
    return rank_rows(raws)


def training_items(panel_by_day: dict, sessions: list[str], day: str, *,
                   letters, suggestions, bars_by_ticker) -> list[dict]:
    entries = training_entry_dates(sessions, day)
    assert_no_lookahead(sessions, day, entries)
    items = []
    for entry in entries:
        exit_day = next_session(sessions, entry)
        if exit_day is None or exit_day >= CUTOFF or exit_day >= day:
            continue
        for item in build_day_items(
            panel_by_day.get(entry) or [], entry, sessions,
            letters=letters, suggestions=suggestions, bars_by_ticker=bars_by_ticker,
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
         letters=None, suggestions=None, fees: dict | None = None,
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
            letters=letters, suggestions=suggestions, bars_by_ticker=bars_by_ticker,
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
                letters=letters, suggestions=suggestions, bars_by_ticker=bars_by_ticker,
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


def load_panel(path: Path | None = None) -> tuple[list[str], dict]:
    src = path or (ROOT / "data" / "factor_mine" / "panel.json")
    doc = json.loads(src.read_text(encoding="utf-8"))
    return filter_panel(doc)


def load_letters(path: Path | None = None, sessions: list[str] | None = None,
                 tickers: set[str] | None = None) -> dict:
    src = path or (ROOT / "excel_bot" / "research" / "excel_clear_letter_panel.csv")
    keep_days = set(sessions or [])
    table = {}
    if not src.is_file():
        return table
    with src.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            day = _as_day(row.get("date"))
            if day >= CUTOFF:
                continue
            if keep_days and day not in keep_days:
                continue
            ticker = _tick(row.get("ticker"))
            if tickers and ticker not in tickers:
                continue
            table[(day, ticker)] = {
                "excel_FQ": row.get("FQ"),
                "excel_ER": row.get("ER"),
                "excel_EP": row.get("EP"),
                "excel_AH": row.get("AH"),
                "excel_FR": row.get("FR"),
                "DF_lag1": row.get("DF_lag1"),
            }
    return table


def load_suggestions(path: Path | None = None) -> dict:
    """Ticker -> suggestion rows. Tracking price columns are not kept.

    signal_date on or after the cutoff is dropped so a later row cannot
    enter the luck-test window.
    """
    src = path or (ROOT / "excel_bot" / "suggestions" / "suggestions.csv")
    by: dict[str, list] = {}
    if not src.is_file():
        return by
    with src.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            day = _as_day(row.get("signal_date"))
            if not day or day >= CUTOFF:
                continue
            ticker = _tick(row.get("ticker"))
            if not ticker:
                continue
            side = str(row.get("side") or "").strip().lower()
            if side not in {"long", "short"}:
                continue
            by.setdefault(ticker, []).append({
                "signal_date": day,
                "side": side,
            })
    return by


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
    """Sequential luck-test walk for 2026-08-13..2026-09-11. Nothing later."""
    sessions, panel_by_day = load_panel()
    window = [day for day in sessions if LUCK_START <= day <= LUCK_END]
    if not window or window[-1] >= CUTOFF:
        raise RuntimeError("luck-test calendar is empty or crosses the cutoff")
    names = panel_tickers({day: panel_by_day[day] for day in window})
    bars = load_bars(names, max_date=LUCK_END)
    letters = load_letters(sessions=window, tickers=names)
    suggestions = load_suggestions()
    # Full session list, still cut before the cutoff, so N+1 inside the
    # window is the next fullscan session and 2026-09-14 is not on the clock.
    clock = [day for day in sessions if day <= LUCK_END and day >= LUCK_START]
    report = walk(
        clock, panel_by_day, bars, letters=letters, suggestions=suggestions,
        state_dir=state_dir if state_dir is not None else HERE / "state",
        start=LUCK_START, end=LUCK_END,
    )
    write_outputs(report, out_dir if out_dir is not None else HERE / "outputs")
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Walk the Excel ML lever one day at a time")
    parser.add_argument("--start", default=LUCK_START)
    parser.add_argument("--end", default=LUCK_END)
    parser.add_argument("--out", default=str(HERE / "outputs"))
    parser.add_argument("--state", default=str(HERE / "state"))
    args = parser.parse_args(argv)
    if args.start != LUCK_START or args.end != LUCK_END:
        # The frozen lever's published series is the luck-test window.
        # Other ranges on or after the cutoff are refused. An earlier
        # one-day rebuild still goes through walk(), which locks past days.
        if args.end >= CUTOFF or args.start >= CUTOFF:
            raise SystemExit(f"refusing range {args.start}..{args.end}")
    if args.start == LUCK_START and args.end == LUCK_END:
        report = run_luck_test(state_dir=Path(args.state), out_dir=Path(args.out))
    else:
        sessions, panel_by_day = load_panel()
        clock = [day for day in sessions if args.start <= day <= args.end and day < CUTOFF]
        names = panel_tickers({day: panel_by_day[day] for day in clock})
        bars = load_bars(names, max_date=args.end)
        letters = load_letters(sessions=clock, tickers=names)
        suggestions = load_suggestions()
        report = walk(
            [day for day in sessions if day < CUTOFF and day <= args.end],
            panel_by_day, bars, letters=letters, suggestions=suggestions,
            state_dir=Path(args.state), start=args.start, end=args.end,
        )
        write_outputs(report, Path(args.out))
    print(
        f"[excel_ml_lever] days={report['n_days']} picked={report['n_days_picked']} "
        f"futubull={report['after_fees_return']} flat15={report['after_fees_return_15bp']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
