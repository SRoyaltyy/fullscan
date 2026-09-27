"""Sequential day cards and the locked score for hot_n4_clean_v4.

Calls the pinned ``pick_day``, ``should_exit``, ``lot_should_sell``,
``split_budgets``, ``from_bars``, ``too_extended``, and ``order_fees``.
Does not edit a gate, a rank, a hold, a fee, or the luck N.
A halt on a held name or on IWM writes nothing for that session and stops.
"""
from __future__ import annotations

import bisect
import csv
import json
import math
import random
import re
import subprocess
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.protocol import (  # noqa: E402
    ADV_SHARE_SCALE,
    AUG_SHA256,
    BP_SIDE,
    CAPITAL,
    DAYS,
    ENGINE_SHA256,
    EXCLUSIONS_PLAN,
    FEES_PATH,
    FEES_SHA256,
    FORWARD,
    GAP_DAY,
    HOLD,
    HOLDUP_S,
    HOLDUP_SESS,
    LIQ_CAP_FRAC,
    MIN_TRADES,
    MISSING_BARS_CSV,
    MISSING_COUNTS,
    OHLC_PATH,
    OHLC_SHA256,
    RANDOM_DRAWS,
    RANDOM_N,
    RANDOM_SEED,
    RATIO_HI,
    RATIO_LO,
    REAL_MOVE_LEGS,
    SEP_SHA256,
    REJECT_JOINT,
    SESSIONS,
    SLIP_LINES,
    SLIP_PRIMARY,
    SPLITS,
    SPLIT_TOL,
    S_SOURCE,
    STARTS,
    TUNE,
    TUNE_END,
    UNEXPLAINED_REASON,
    VARIANTS,
    WILSON_Z,
    commit_day,
    file_sha256,
    price_factor,
    s_path,
    sources_on,
    unexplained_legs_on,
    verify_ledger,
)
from src.factor_mine import pick_day, should_exit  # noqa: E402
from src.factor_mine_book import lot_should_sell, split_budgets  # noqa: E402
from src.ohlc_ripper import from_bars, too_extended  # noqa: E402
from src.paper_trade import order_fees  # noqa: E402
from src.skip_if_good import is_nyse_holiday  # noqa: E402

PANEL_DIR = Path("/tmp/theme-radar/research/lever_panel")
AUG_NAME = "finviz_panel_asof0930_2026-08.csv.gz"
SEP_NAME = "finviz_panel_asof0930_2026-09.csv.gz"
PREDICT_RE = re.compile(
    r"Prediction:\s*(UP|DOWN|FLAT).*?total score\s*(-?[\d.]+)",
)
HALT_LEGS = frozenset({"open_over_prev_close", "close_over_open"})
LOOKBACK = 60
HOT_N = 30
GAINER_N = 25
MOVER_N = 20
PROBABLE_CONSIDER = 60
PROBABLE_N = 8
PROBABLE_RET5_MAX = 10.0
RETURNS = ROOT / "research" / "hot_n4_clean_v4" / "returns"


class Halt(RuntimeError):
    """An unexplained >3x leg on a held name or on IWM. Do not write the day."""


def nyse_sessions(start: str, end: str) -> list[str]:
    d = date.fromisoformat(start)
    last = date.fromisoformat(end)
    out = []
    while d <= last:
        if d.weekday() < 5 and not is_nyse_holiday(d):
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


CAL = nyse_sessions("2025-01-01", "2026-12-31")
CAL_IX = {day: i for i, day in enumerate(CAL)}


def prev_session(day: str) -> str:
    i = CAL_IX[day]
    if i < 1:
        raise RuntimeError(f"no previous session {day}")
    return CAL[i - 1]


def plus_sessions(day: str, n: int) -> str:
    i = CAL_IX.get(day)
    if i is None:
        raise RuntimeError(f"not a session {day}")
    j = i + n
    if j >= len(CAL):
        raise RuntimeError(f"calendar short {day} + {n}")
    return CAL[j]


def _f(value) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def check_pins() -> None:
    for path, digest in ENGINE_SHA256.items():
        got = file_sha256(ROOT / path)
        if got != digest:
            raise SystemExit(f"engine bytes changed {path}")
    for path, digest in (
        (OHLC_PATH, OHLC_SHA256),
        (FEES_PATH, FEES_SHA256),
    ):
        got = file_sha256(ROOT / path)
        if got != digest:
            raise SystemExit(f"pin changed {path}")
    if list(nyse_sessions("2026-08-13", "2026-09-25")) != list(SESSIONS):
        raise SystemExit("calendar")


def load_fees() -> dict:
    return json.loads((ROOT / FEES_PATH).read_text(encoding="utf-8"))


def load_bars() -> dict[str, dict]:
    import pandas as pd

    frame = pd.read_parquet(ROOT / OHLC_PATH)
    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    frame = frame.sort_values(["ticker", "date"])
    frame = frame.drop_duplicates(["ticker", "date"], keep="last")
    store: dict[str, dict] = {}
    for ticker, group in frame.groupby("ticker", sort=False):
        store[str(ticker)] = {
            "date": group["date"].tolist(),
            "open": group["open"].to_numpy(dtype=float, copy=True),
            "high": group["high"].to_numpy(dtype=float, copy=True),
            "low": group["low"].to_numpy(dtype=float, copy=True),
            "close": group["close"].to_numpy(dtype=float, copy=True),
            "volume": group["volume"].to_numpy(dtype=float, copy=True),
        }
    factors = {}
    for _ex, ticker, share, boundary in SPLITS:
        factors[ticker] = (float(share), price_factor(float(share)), boundary)
    for ticker, (share, px, boundary) in factors.items():
        blob = store.get(ticker)
        if blob is None:
            continue
        dates = blob["date"]
        for key in ("open", "high", "low", "close"):
            arr = blob[key]
            for i, day in enumerate(dates):
                if day < boundary:
                    arr[i] = arr[i] * px
        vol = blob["volume"]
        for i, day in enumerate(dates):
            if day < boundary:
                vol[i] = vol[i] * share
        blob["adjusted"] = True
    # Jump checks need the stored prints. Keep a second copy of stored
    # OHLC for split names only; everyone else is unadjusted.
    stored_path = ROOT / OHLC_PATH
    raw = pd.read_parquet(stored_path, columns=["date", "ticker", "open", "close"])
    raw["date"] = raw["date"].dt.strftime("%Y-%m-%d")
    raw = raw.sort_values(["ticker", "date"]).drop_duplicates(["ticker", "date"], keep="last")
    stored: dict[str, dict] = {}
    for ticker, group in raw.groupby("ticker", sort=False):
        stored[str(ticker)] = {
            "date": group["date"].tolist(),
            "open": group["open"].to_numpy(dtype=float, copy=True),
            "close": group["close"].to_numpy(dtype=float, copy=True),
        }
    return {"feat": store, "stored": stored}


def _slice(blob: dict | None, end_exclusive: str, n: int | None) -> list[dict]:
    if not blob:
        return []
    dates = blob["date"]
    i = bisect.bisect_left(dates, end_exclusive)
    start = 0 if n is None else max(0, i - n)
    out = []
    for j in range(start, i):
        out.append({
            "date": dates[j],
            "open": float(blob["open"][j]),
            "high": float(blob["high"][j]) if "high" in blob else float(blob["open"][j]),
            "low": float(blob["low"][j]) if "low" in blob else float(blob["open"][j]),
            "close": float(blob["close"][j]),
            "volume": float(blob["volume"][j]) if "volume" in blob else 0.0,
        })
    return out


def bar_on(blob: dict | None, day: str) -> dict | None:
    if not blob:
        return None
    dates = blob["date"]
    i = bisect.bisect_left(dates, day)
    if i >= len(dates) or dates[i] != day:
        return None
    return {
        "date": day,
        "open": float(blob["open"][i]),
        "close": float(blob["close"][i]),
    }


def last_close_before(blob: dict | None, day: str) -> float | None:
    if not blob:
        return None
    dates = blob["date"]
    i = bisect.bisect_left(dates, day) - 1
    if i < 0:
        return None
    close = float(blob["close"][i])
    if not math.isfinite(close) or close <= 0:
        return None
    return close


def first_bar_date(blob: dict | None) -> str | None:
    if not blob or not blob["date"]:
        return None
    return blob["date"][0]


def has_bar_on_or_before(blob: dict | None, day: str) -> bool:
    if not blob or not blob["date"]:
        return False
    return blob["date"][0] <= day


def has_bar_on_or_after(blob: dict | None, day: str) -> bool:
    if not blob or not blob["date"]:
        return False
    return blob["date"][-1] >= day


def load_panels():
    import pandas as pd

    cols = [
        "trade_date", "snapshot_date", "Ticker", "Industry",
        "Market Cap", "Average Volume", "Volume", "Price",
    ]
    frames = []
    for name in (AUG_NAME, SEP_NAME):
        path = PANEL_DIR / name
        if not path.is_file():
            raise SystemExit(f"missing panel {path}")
        frames.append(pd.read_csv(path, usecols=cols, compression="gzip"))
    frame = pd.concat(frames, ignore_index=True)
    frame["trade_date"] = frame["trade_date"].astype(str).str[:10]
    frame["snapshot_date"] = frame["snapshot_date"].astype(str).str[:10]
    frame["Ticker"] = frame["Ticker"].astype(str).str.strip().str.upper()
    for col in ("Market Cap", "Average Volume", "Volume", "Price"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    by = {}
    for day, group in frame.groupby("trade_date", sort=False):
        by[str(day)] = group
    return by


def load_scores() -> dict[str, dict]:
    proof = {}
    with (ROOT / "research/audit/FULLSCAN_FILE_PROOF.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["input"] not in ("predict", "weather"):
                continue
            if row["proven"] != "yes" or row["before_0930"] != "yes":
                continue
            proof[(row["date"], row["input"], row["server_time_utc"])] = row
    out = {}
    for session, kind, status, stamp in S_SOURCE:
        row = proof.get((session, kind, stamp))
        if row is None:
            raise SystemExit(f"no proof row {session} {kind} {stamp}")
        if row["status"] != status:
            raise SystemExit(f"status drift {session}")
        blob = row["blob_sha"]
        text = subprocess.check_output(
            ["git", "-C", str(ROOT), "cat-file", "-p", blob],
        )
        score = None
        if kind == "predict":
            match = PREDICT_RE.search(text.decode("utf-8", errors="replace"))
            if match:
                score = float(match.group(2))
        else:
            doc = json.loads(text)
            raw = (doc.get("signals") or {}).get("general_score")
            score = _f(raw)
        out[session] = {
            "blob_sha": blob,
            "kind": kind,
            "morning_s": score,
            "path": s_path(session, kind),
            "server_time_utc": stamp,
            "status": status,
        }
    return out


def split_row(ticker: str):
    for ex_date, name, share, _boundary in SPLITS:
        if name == ticker:
            return ex_date, float(share)
    return None


def explains_split(ticker: str, ratio: float, prev_date: str, jump_date: str) -> bool:
    found = split_row(ticker)
    if found is None:
        return False
    ex_date, share = found
    if jump_date not in CAL_IX:
        return False
    if not (prev_date < ex_date <= plus_sessions(jump_date, 5)):
        return False
    px = price_factor(share)
    for candidate in (px, share):
        if candidate <= 0:
            continue
        if abs(ratio - candidate) / candidate <= SPLIT_TOL:
            return True
    return False


REAL_KEYS = {(row[0], row[1], row[2]) for row in REAL_MOVE_LEGS}


def scan_legs(stored: dict, ticker: str, session: str) -> list[dict]:
    """>3x legs on the last 60 stored bars before D plus D's own bar."""
    blob = stored.get(ticker)
    if not blob:
        return []
    dates = blob["date"]
    i = bisect.bisect_left(dates, session)
    start = max(0, i - LOOKBACK)
    # Include the bar just before the window so the first loaded bar has
    # its previous stored close. That prior bar is not itself a loaded bar.
    prev_i = start - 1
    loaded_hi = i
    if i < len(dates) and dates[i] == session:
        loaded_hi = i + 1
    legs = []
    prev = prev_i if prev_i >= 0 else None
    for j in range(start, loaded_hi):
        day = dates[j]
        op = float(blob["open"][j])
        cl = float(blob["close"][j])
        prev_close = None
        prev_date = None
        if prev is not None:
            prev_close = float(blob["close"][prev])
            prev_date = dates[prev]
        ratios = []
        if prev_close is not None and prev_close > 0 and op > 0:
            ratios.append(("open_over_prev_close", op / prev_close, prev_date))
        if prev_close is not None and prev_close > 0 and cl > 0:
            ratios.append(("close_over_prev_close", cl / prev_close, prev_date))
        if op > 0 and cl > 0:
            # Same-bar leg. The date test still uses the prior stored bar
            # when one exists; otherwise the bar's own date.
            ratios.append(("close_over_open", cl / op, prev_date or day))
        for leg, ratio, anchor in ratios:
            if ratio > RATIO_HI or ratio < RATIO_LO:
                legs.append({
                    "anchor": anchor,
                    "bar_date": day,
                    "leg": leg,
                    "ratio": ratio,
                    "ticker": ticker,
                })
        prev = j
    return legs


def classify(leg: dict) -> str:
    if (leg["ticker"], leg["bar_date"], leg["leg"]) in REAL_KEYS:
        return "real"
    if explains_split(leg["ticker"], leg["ratio"], leg["anchor"], leg["bar_date"]):
        return "split"
    return "unexplained"


def removal_legs(stored: dict, ticker: str, session: str) -> list[dict]:
    loaded = _loaded_dates(stored, ticker, session)
    found = []
    seen = set()
    for leg in scan_legs(stored, ticker, session):
        if classify(leg) != "unexplained":
            continue
        key = (leg["ticker"], leg["bar_date"], leg["leg"])
        if key in seen:
            continue
        seen.add(key)
        found.append({
            "bar_date": leg["bar_date"],
            "leg": leg["leg"],
            "reason": UNEXPLAINED_REASON.get(ticker, "not a verified split and not a filing-verified real move"),
            "ticker": ticker,
        })
    for row in unexplained_legs_on(ticker, loaded):
        key = (row["ticker"], row["bar_date"], row["leg"])
        if key in seen:
            continue
        seen.add(key)
        found.append(dict(row))
    found.sort(key=lambda row: (row["ticker"], row["bar_date"], row["leg"]))
    return found


def _loaded_dates(stored: dict, ticker: str, session: str) -> list[str]:
    blob = stored.get(ticker)
    if not blob:
        return []
    dates = blob["date"]
    i = bisect.bisect_left(dates, session)
    start = max(0, i - LOOKBACK)
    hi = i + 1 if i < len(dates) and dates[i] == session else i
    return dates[start:hi]


def halt_legs(stored: dict, ticker: str, session: str) -> list[dict]:
    out = []
    for leg in removal_legs(stored, ticker, session):
        if leg["leg"] in HALT_LEGS:
            out.append(leg)
    return out


def features_of(feat_store: dict, ticker: str, session: str) -> dict:
    blob = feat_store.get(ticker)
    bars = _slice(blob, session, LOOKBACK) if blob else []
    # from_bars needs high/low/volume. Adjusted store has them.
    if blob and bars:
        # _slice on feat store includes high/low/volume.
        pass
    feat = from_bars(bars)
    return {
        "break_10": bool(feat.get("break_10")),
        "hot_score": float(hot_num(feat)),
        "last_green": bool(feat.get("last_green")),
        "ok": bool(feat.get("ok")),
        "ret_5": float(feat.get("ret_5") or 0.0),
        "ret_10": float(feat.get("ret_10") or 0.0),
        "rvol": float(feat.get("rvol") or 0.0),
    }


def hot_num(feat: dict) -> float:
    # from_bars already stores hot_score from the pinned function.
    return float(feat.get("hot_score") or 0.0)


def prior_return(feat_store: dict, ticker: str, snapshot: str) -> float | None:
    blob = feat_store.get(ticker)
    if not blob:
        return None
    older = prev_session(snapshot)
    b1 = bar_on(blob, older)
    b2 = bar_on(blob, snapshot)
    if not b1 or not b2:
        return None
    if b1["close"] <= 0 or b2["close"] <= 0:
        return None
    return b2["close"] / b1["close"] - 1.0


def liquid_universe(frame) -> list[dict]:
    industry = frame["Industry"].astype(str)
    keep = (
        frame["Ticker"].astype(bool)
        & ~industry.eq("Exchange Traded Fund")
        & frame["Market Cap"].ge(100)
        & frame["Average Volume"].ge(500)
        & frame["Volume"].gt(0)
    )
    rows = frame.loc[keep, ["Ticker", "Price", "Average Volume"]]
    out = []
    seen = set()
    for ticker, price, adv in rows.itertuples(index=False, name=None):
        name = str(ticker)
        if name in seen:
            continue
        seen.add(name)
        px = _f(price)
        av = _f(adv)
        out.append({
            "fv_avg_volume": None if av is None or av <= 0 else av,
            "fv_price": None if px is None or px <= 0 else px,
            "ticker": name,
        })
    out.sort(key=lambda row: row["ticker"])
    return out


def all_tickers(frame) -> list[str]:
    return sorted({str(t) for t in frame["Ticker"].tolist() if str(t)})


def build_candidates(feat_store: dict, stored: dict, frame, session: str,
                     held: set[str] | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    """Candidates after the unexplained-leg removal. A held name stays."""
    held = held or set()
    universe = liquid_universe(frame)
    by_ticker = {row["ticker"]: row for row in universe}
    snapshot = prev_session(session)
    prior = {}
    feat = {}
    for row in universe:
        ticker = row["ticker"]
        prior[ticker] = prior_return(feat_store, ticker, snapshot)
        feat[ticker] = features_of(feat_store, ticker, session)

    ranked_ret = [
        (ticker, ret) for ticker, ret in prior.items() if ret is not None
    ]
    ranked_ret.sort(key=lambda item: (-item[1], item[0]))
    gainers = [ticker for ticker, _ret in ranked_ret[:GAINER_N]]
    movers = sorted(ranked_ret, key=lambda item: (-abs(item[1]), item[0]))[:MOVER_N]
    mover_names = [ticker for ticker, _ret in movers]

    hot = []
    for ticker, row_feat in feat.items():
        if not row_feat["ok"] or too_extended(row_feat):
            continue
        hot.append((row_feat["hot_score"], ticker))
    hot.sort(key=lambda item: (-item[0], item[1]))
    hot_names = [ticker for _score, ticker in hot[:HOT_N]]

    probable = []
    for ticker, _ret in ranked_ret[:PROBABLE_CONSIDER]:
        row_feat = feat[ticker]
        if not row_feat["ok"] or too_extended(row_feat):
            continue
        if row_feat["ret_5"] > PROBABLE_RET5_MAX:
            continue
        probable.append(ticker)
        if len(probable) >= PROBABLE_N:
            break

    sources = defaultdict(list)
    for ticker in hot_names:
        sources[ticker].append("ohlc_hot")
    for ticker in gainers:
        sources[ticker].append("yday_gainer")
    for ticker in mover_names:
        sources[ticker].append("yday_mover")
    for ticker in probable:
        sources[ticker].append("probable")

    no_fill = []
    for ticker in all_tickers(frame):
        blob = stored.get(ticker)
        if has_bar_on_or_before(blob, session):
            continue
        reason = "no_store" if blob is None else "first_bar_after"
        liquid = ticker in by_ticker
        no_fill.append({
            "liquid_gate": liquid,
            "reason": reason,
            "ticker": ticker,
        })
    no_fill.sort(key=lambda row: row["ticker"])

    candidates = []
    excluded = []
    for ticker in sorted(sources):
        legs = removal_legs(stored, ticker, session)
        if legs and ticker not in held:
            excluded.extend(legs)
            continue
        row_feat = feat[ticker]
        fin = by_ticker[ticker]
        candidates.append({
            "break_10": row_feat["break_10"],
            "fv_avg_volume": fin["fv_avg_volume"],
            "fv_price": fin["fv_price"],
            "hot_score": row_feat["hot_score"],
            "last_green": row_feat["last_green"],
            "ohlc_hot_score": row_feat["hot_score"],
            "prior_ret": prior[ticker],
            "ret_10": row_feat["ret_10"],
            "ret_5": row_feat["ret_5"],
            "rvol": row_feat["rvol"],
            "sources": sorted(sources[ticker]),
            "ticker": ticker,
        })
    excluded.sort(key=lambda row: (row["ticker"], row["bar_date"], row["leg"]))
    return candidates, excluded, no_fill


def missing_flags(stored: dict, frame, session: str) -> list[dict]:
    out = []
    for ticker in all_tickers(frame):
        blob = stored.get(ticker)
        if has_bar_on_or_before(blob, session):
            continue
        reason = "no_store" if blob is None else "first_bar_after"
        out.append({"reason": reason, "ticker": ticker})
    return out


def assert_snapshot(frame, session: str) -> None:
    expect = prev_session(session)
    bad = frame.loc[frame["snapshot_date"] != expect]
    if len(bad):
        raise SystemExit(
            f"{session} snapshot_date is not {expect} on {len(bad)} rows"
        )


def build_payload(bars, panels, scores, session: str, held: set[str]) -> dict:
    score = scores[session]
    stored = bars["stored"]
    # IWM is checked even when nobody is held.
    iwm = scan_legs(stored, "IWM", session)
    iwm_bad = [leg for leg in iwm if classify(leg) != "split"]
    if iwm_bad:
        raise Halt(
            f"IWM {session} unexplained leg {iwm_bad[0]['leg']} on {iwm_bad[0]['bar_date']}"
        )
    for ticker in sorted(held):
        bad = halt_legs(stored, ticker, session)
        if bad:
            raise Halt(
                f"held {ticker} {session} unexplained {bad[0]['leg']} on {bad[0]['bar_date']}"
            )
    if session == GAP_DAY:
        candidates, excluded, no_fill = [], [], []
        if session in panels:
            raise SystemExit("2026-08-28 has a frozen row")
        panel_file = None
        panel_sha = None
    else:
        if session not in panels:
            raise SystemExit(f"no frozen row {session}")
        frame = panels[session]
        assert_snapshot(frame, session)
        candidates, excluded, no_fill = build_candidates(
            bars["feat"], stored, frame, session, held,
        )
        panel_file = AUG_NAME if session < "2026-09-01" else SEP_NAME
        panel_sha = AUG_SHA256 if session < "2026-09-01" else SEP_SHA256
    names = sorted({row["ticker"] for row in excluded})
    return {
        "bar_cutoff": prev_session(session),
        "candidates": candidates,
        "excluded_unexplained_legs": excluded,
        "morning_s": score["morning_s"],
        "n_excluded": len(names),
        "no_fill": [
            {"reason": row["reason"], "ticker": row["ticker"]}
            for row in no_fill if row["liquid_gate"]
        ],
        "panel_file": panel_file,
        "panel_sha256": panel_sha,
        "price_sha256": OHLC_SHA256,
        "s_source": {
            "blob_sha": score["blob_sha"],
            "kind": score["kind"],
            "path": score["path"],
            "server_time_utc": score["server_time_utc"],
            "status": score["status"],
        },
        "session": session,
        "sources": sources_on(session),
    }


def open_px(stored: dict, ticker: str, session: str) -> float | None:
    bar = bar_on(stored.get(ticker), session)
    if not bar:
        return None
    op = bar["open"]
    if not math.isfinite(op) or op <= 0:
        return None
    return op


def mark_px(stored: dict, ticker: str, session: str) -> float | None:
    bar = bar_on(stored.get(ticker), session)
    if bar and bar["open"] > 0 and bar["close"] > 0:
        return bar["close"]
    return last_close_before(stored.get(ticker), session)


def buy_shares(budget: float, cash: float, op: float, fv_price, fv_adv, fees: dict):
    if fv_price is None or fv_adv is None or fv_price <= 0 or fv_adv <= 0:
        return 0, "missing Finviz Price or Average Volume"
    cap_dollars = LIQ_CAP_FRAC * float(fv_price) * float(fv_adv) * ADV_SHARE_SCALE
    cap_shares = int(math.floor(cap_dollars / op + 1e-12))
    if cap_shares < 1:
        return 0, "cap below one share"
    room = min(float(budget), float(cash))
    gross = op * (1.0 + SLIP_PRIMARY)
    shares = int(math.floor(room / gross + 1e-12))
    shares = min(shares, cap_shares)
    while shares >= 1:
        fee = order_fees(shares, op, "buy", fees)
        cost = shares * gross + fee
        if cost <= room + 1e-6:
            return shares, None
        shares -= 1
    return 0, "cannot buy one share"


def schedule_buy_cost(shares: int, op: float, fees: dict) -> dict:
    fee = order_fees(shares, op, "buy", fees)
    fee15 = round(BP_SIDE * shares * op, 4)
    out = {}
    for slip in SLIP_LINES:
        out[slip] = shares * op * (1.0 + slip) + fee
    out["15"] = shares * op + fee15
    return out


def schedule_sell_proceeds(shares: int, op: float, fees: dict) -> dict:
    fee = order_fees(shares, op, "sell", fees)
    fee15 = round(BP_SIDE * shares * op, 4)
    out = {}
    for slip in SLIP_LINES:
        out[slip] = shares * op * (1.0 - slip) - fee
    out["15"] = shares * op - fee15
    return out


def skip_reason_buy(stored: dict, ticker: str, session: str) -> str | None:
    blob = stored.get(ticker)
    if not has_bar_on_or_before(blob, session):
        return "frozen-record ticker with no pinned bar on or before D"
    if open_px(stored, ticker, session) is None:
        return "no open"
    return None


def skip_reason_sell(stored: dict, ticker: str, session: str) -> str:
    blob = stored.get(ticker)
    if not has_bar_on_or_after(blob, session):
        return "no bar left to fill"
    return "no open"


def simulate(payloads: list[dict], variant: dict, bars, fees: dict, banned: set[str] | None = None) -> dict:
    banned = banned or set()
    stored = bars["stored"]
    cash = {slip: float(CAPITAL) for slip in list(SLIP_LINES) + ["15"]}
    pos: dict[str, dict] = {}
    trades = []
    skips = []
    daily = []
    prev_eq = {slip: float(CAPITAL) for slip in cash}
    ix = {day: i for i, day in enumerate(SESSIONS)}
    for payload in payloads:
        session = payload["session"]
        rows = [row for row in payload["candidates"] if row["ticker"] not in banned]
        chosen = pick_day(rows, variant)
        chosen_names = {row["ticker"] for row in chosen}
        row_by = {row["ticker"]: row for row in rows}
        s = payload["morning_s"]
        holdup = (
            variant["s_boost"] == "holdup"
            and s is not None
            and float(s) > HOLDUP_S
        )
        bought = 0
        sold = 0
        # Sells first, ticker order for a stable cash path.
        for ticker in sorted(pos):
            lot = pos[ticker]
            op = open_px(stored, ticker, session)
            row = row_by.get(ticker) or {}
            early = should_exit(row, variant.get("exit_when") or {})
            held_n = ix[session] - ix[lot["entry_date"]]
            # List-drop does not need a price. A missing open is an unfilled
            # sell only when the lot would have sold. A min-hold keep is not.
            do_sell, _kind = lot_should_sell(
                lot, held=held_n, min_hold=int(lot["min_hold"]), early=early,
                dropped=ticker not in chosen_names, sell_mode=variant["sell"],
                px=op, side="long", take_pct=None, stop_pct=None,
            )
            if op is None:
                if do_sell:
                    skips.append({
                        "date": session,
                        "reason": skip_reason_sell(stored, ticker, session),
                        "side": "sell",
                        "ticker": ticker,
                    })
                continue
            if not do_sell:
                lot["last_px"] = op
                lot["peak_px"] = max(float(lot.get("peak_px") or op), op)
                continue
            proceeds = schedule_sell_proceeds(lot["shares"], op, fees)
            pnl = {}
            for key, cash_key in ((slip, slip) for slip in list(SLIP_LINES) + ["15"]):
                cash[cash_key] += proceeds[key]
                pnl[key] = proceeds[key] - lot["cost"][key]
            trades.append({
                "date": session,
                "entry_date": lot["entry_date"],
                "entry_px": lot["entry_px"],
                "open": op,
                "pnl": pnl,
                "shares": lot["shares"],
                "side": "sell",
                "ticker": ticker,
            })
            del pos[ticker]
            sold += 1
        new = [row for row in chosen if row["ticker"] not in pos]
        if session == GAP_DAY:
            new = []
        budgets = split_budgets(new, cash[SLIP_PRIMARY], "leftover") if new else []
        for row, budget in zip(new, budgets):
            ticker = row["ticker"]
            blocked = skip_reason_buy(stored, ticker, session)
            if blocked:
                skips.append({
                    "date": session, "reason": blocked, "side": "buy", "ticker": ticker,
                })
                continue
            op = open_px(stored, ticker, session)
            shares, why = buy_shares(
                budget, cash[SLIP_PRIMARY], op, row.get("fv_price"), row.get("fv_avg_volume"), fees,
            )
            if why:
                skips.append({
                    "date": session, "reason": why, "side": "buy", "ticker": ticker,
                })
                continue
            cost = schedule_buy_cost(shares, op, fees)
            for key in cost:
                cash[key] -= cost[key]
            pos[ticker] = {
                "cost": cost,
                "entry_date": session,
                "entry_px": op,
                "last_px": op,
                "min_hold": max(HOLD, HOLDUP_SESS) if holdup else HOLD,
                "peak_px": op,
                "shares": shares,
                "ticker": ticker,
            }
            trades.append({
                "date": session,
                "open": op,
                "shares": shares,
                "side": "buy",
                "ticker": ticker,
            })
            bought += 1
        row_daily = {"bought": bought, "session": session, "sold": sold}
        for key in cash:
            stock = 0.0
            for ticker, lot in pos.items():
                px = mark_px(stored, ticker, session)
                if px is None:
                    px = float(lot.get("last_px") or lot["entry_px"])
                stock += lot["shares"] * px
            equity = cash[key] + stock
            base = prev_eq[key]
            row_daily[key] = {
                "equity": equity,
                "ret": (equity / base - 1.0) if base else 0.0,
            }
            prev_eq[key] = equity
        daily.append(row_daily)
    return {"cash": cash, "daily": daily, "pos": pos, "skips": skips, "trades": trades}


def closed_trades(book: dict, sessions: list[str] | None = None) -> list[dict]:
    keep = set(sessions) if sessions is not None else None
    out = []
    for trade in book["trades"]:
        if trade["side"] != "sell":
            continue
        if keep is not None and trade["date"] not in keep:
            continue
        out.append(trade)
    return out


def ticker_pnl(trades: list[dict], key=SLIP_PRIMARY) -> list[tuple[str, float]]:
    totals: dict[str, float] = defaultdict(float)
    for trade in trades:
        totals[trade["ticker"]] += float(trade["pnl"][key])
    ranked = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    return ranked


def window_return(book: dict, sessions: list[str], key) -> float:
    """Equity at the last session of the slice over equity at the prior close."""
    if not sessions:
        return 0.0
    by = {row["session"]: row for row in book["daily"]}
    end = by[sessions[-1]][key]["equity"]
    first = sessions[0]
    i = SESSIONS.index(first)
    if i == 0:
        start = float(CAPITAL)
    else:
        start = by[SESSIONS[i - 1]][key]["equity"]
    if start == 0:
        return 0.0
    return end / start - 1.0


def wilson(k: int, n: int) -> tuple[float, float, float] | None:
    if n <= 0:
        return None
    p = k / n
    z = WILSON_Z
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return p, center - half, center + half


def active_days(book: dict, sessions: list[str], key=SLIP_PRIMARY):
    by = {row["session"]: row for row in book["daily"]}
    rows = []
    for session in sessions:
        row = by[session]
        if row["bought"] or row["sold"]:
            rows.append(row)
    return rows


def summarize_window(book: dict, sessions: list[str]) -> dict:
    closed = closed_trades(book, sessions)
    wins = sum(1 for trade in closed if trade["pnl"][SLIP_PRIMARY] > 0)
    n = len(closed)
    interval = wilson(wins, n)
    active = active_days(book, sessions)
    up = sum(1 for row in active if row[SLIP_PRIMARY]["ret"] > 0)
    up_share = (up / len(active)) if active else None
    win_rate = interval[0] if interval else None
    joint = None
    if win_rate is not None and up_share is not None:
        joint = min(win_rate, up_share)
    ranked = ticker_pnl(closed)
    book_pnl = sum(amount for _ticker, amount in ranked)
    best = ranked[0][0] if ranked else None
    if best is None or book_pnl == 0:
        share = None
    else:
        share = ranked[0][1] / book_pnl
    buys = [
        trade for trade in book["trades"]
        if trade["side"] == "buy" and trade["date"] in set(sessions)
    ]
    skips = [row for row in book["skips"] if row["date"] in set(sessions)]
    returns = {}
    for key in list(SLIP_LINES) + ["15"]:
        returns[str(key)] = window_return(book, sessions, key)
    return {
        "best": best,
        "best_pnl": None if not ranked else ranked[0][1],
        "best_share": share,
        "book_pnl": book_pnl,
        "buys": len(buys),
        "closed": n,
        "joint": joint,
        "ranked": ranked,
        "rejects": bool(n >= MIN_TRADES and joint is not None and joint < REJECT_JOINT),
        "returns": returns,
        "rule21": bool(n >= MIN_TRADES and win_rate is not None and win_rate > 0.55),
        "skips": skips,
        "trades_per_session": (n / len(sessions)) if sessions else None,
        "unfilled": len(skips),
        "up": up,
        "up_days_denom": len(active),
        "up_share": up_share,
        "wilson": None if interval is None else {
            "high": interval[2], "low": interval[1], "p": interval[0],
        },
        "wins": wins,
    }


def positive_starts(book: dict) -> dict:
    out = {}
    for start in STARTS:
        total = 0.0
        for trade in closed_trades(book):
            if start <= trade["date"] <= TUNE_END:
                total += float(trade["pnl"][SLIP_PRIMARY])
        out[start] = {"pnl": total, "positive": total > 0}
    x = sum(1 for row in out.values() if row["positive"])
    return {"of": len(STARTS), "positive": x, "starts": out, "text": f"{x} of {len(STARTS)}"}


def iwm_path(bars, fees: dict, sessions: list[str]) -> dict:
    """Buy IWM at the window's first open. A session with no pinned bar is cash.

    The mark stays at the last stored close. No second price file is read.
    A missing entry open does not fill, so that window stays in cash.
    """
    stored = bars["stored"]
    first = sessions[0]
    op = open_px(stored, "IWM", first)
    missing = [
        session for session in sessions
        if bar_on(stored.get("IWM"), session) is None
    ]
    if op is None:
        return {
            "filled": False,
            "last_mark_date": None,
            "missing_sessions": missing,
            "reason": "no open",
            "return": 0.0,
            "return_15": 0.0,
            "shares": 0,
        }
    fee = order_fees(1, op, "buy", fees)
    shares = int((CAPITAL - fee) // op)
    while shares > 0 and shares * op + order_fees(shares, op, "buy", fees) > CAPITAL + 1e-6:
        shares -= 1
    if shares < 1:
        return {
            "filled": False,
            "last_mark_date": None,
            "missing_sessions": missing,
            "reason": "cannot buy one share",
            "return": 0.0,
            "return_15": 0.0,
            "shares": 0,
        }
    fee = order_fees(shares, op, "buy", fees)
    fee15 = round(BP_SIDE * shares * op, 4)
    cash = CAPITAL - shares * op - fee
    cash15 = CAPITAL - shares * op - fee15
    mark = None
    mark_date = None
    for session in sessions:
        bar = bar_on(stored.get("IWM"), session)
        if bar and bar["open"] > 0 and math.isfinite(bar["close"]) and bar["close"] > 0:
            mark = bar["close"]
            mark_date = session
    equity = cash + shares * mark
    equity15 = cash15 + shares * mark
    return {
        "filled": True,
        "last_mark_date": mark_date,
        "missing_sessions": missing,
        "reason": None,
        "return": equity / CAPITAL - 1.0,
        "return_15": equity15 / CAPITAL - 1.0,
        "shares": shares,
    }


def random4(payloads: list[dict], variant: dict, bars, fees: dict, sessions: list[str]) -> dict:
    compounds = []
    compounds_15 = []
    want = set(sessions)
    for draw in range(RANDOM_DRAWS):
        rng = random.Random(RANDOM_SEED + draw)
        sampled = []
        for payload in payloads:
            pool = list(payload["candidates"])
            pool.sort(key=lambda row: row["ticker"])
            k = min(RANDOM_N, len(pool))
            picked = rng.sample(pool, k) if k else []
            card = dict(payload)
            card["candidates"] = picked
            sampled.append(card)
        # pick_day would re-sort. RANDOM4 does not use the hot-score sort.
        # simulate() calls pick_day. Feed it a list that is already the
        # sample, and a rank that keeps that sample: top_n is 4 and the
        # sample has at most 4, so pick_day returns all of them. The order
        # does not change who is held. Hold and holdup still apply.
        book = simulate(sampled, variant, bars, fees, banned=None)
        compounds.append(window_return(book, sessions, SLIP_PRIMARY))
        compounds_15.append(window_return(book, sessions, "15"))
    def mean(xs):
        return sum(xs) / len(xs) if xs else None
    return {"mean": mean(compounds), "mean_15": mean(compounds_15), "n": len(compounds)}


def rerun_without(payloads, variant, bars, fees, names: list[str], sessions: list[str]) -> dict:
    book = simulate(payloads, variant, bars, fees, banned=set(names))
    return {
        "names": names,
        "return": window_return(book, sessions, SLIP_PRIMARY),
        "return_15": window_return(book, sessions, "15"),
        "return_0": window_return(book, sessions, 0.0),
        "return_1": window_return(book, sessions, 0.01),
    }


def exclusion_diff(payloads: list[dict]) -> list[str]:
    with EXCLUSIONS_PLAN.open(encoding="utf-8", newline="") as handle:
        plan = list(csv.DictReader(handle))
    got = []
    for payload in payloads:
        legs = payload["excluded_unexplained_legs"]
        names = sorted({row["ticker"] for row in legs})
        n = len(names)
        if not legs:
            got.append((payload["session"], "0", "", "", "", ""))
            continue
        for row in legs:
            got.append((
                payload["session"], str(n), row["ticker"], row["bar_date"], row["leg"], row["reason"],
            ))
    want = []
    for row in plan:
        want.append((
            row["session"], row["n_excluded"], row["ticker"], row["bar_date"], row["leg"], row["reason"],
        ))
    diffs = []
    if got != want:
        gs, ws = set(got), set(want)
        for row in sorted(ws - gs):
            diffs.append("plan only " + "|".join(row))
        for row in sorted(gs - ws):
            diffs.append("run only " + "|".join(row))
    return diffs


def missing_diff(bars, panels) -> list[str]:
    stored = bars["stored"]
    path = ROOT / MISSING_BARS_CSV
    with path.open(encoding="utf-8", newline="") as handle:
        plan = list(csv.DictReader(handle))
    got = []
    for session in SESSIONS:
        if session == GAP_DAY:
            continue
        frame = panels[session]
        liquid = {row["ticker"] for row in liquid_universe(frame)}
        for ticker in all_tickers(frame):
            blob = stored.get(ticker)
            if has_bar_on_or_before(blob, session):
                continue
            reason = "no_store" if blob is None else "first_bar_after"
            got.append((session, ticker, "yes" if ticker in liquid else "no", reason))
    want = [
        (row["trade_date"], row["ticker"], row["liquid_gate"], row["reason"])
        for row in plan
    ]
    if got == want:
        return []
    diffs = []
    gs, ws = set(got), set(want)
    diffs.append(f"missing bars run {len(got)} plan {len(want)}")
    for row in sorted(ws - gs)[:12]:
        diffs.append("plan only " + "|".join(row))
    for row in sorted(gs - ws)[:12]:
        diffs.append("run only " + "|".join(row))
    # per-day counts vs the lock
    by = defaultdict(lambda: [0, 0])
    for session, _ticker, liquid, _reason in got:
        by[session][0] += 1
        if liquid == "yes":
            by[session][1] += 1
    for session, n, n_liq in MISSING_COUNTS:
        got_n, got_l = by[session]
        if got_n != n or got_l != n_liq:
            diffs.append(f"count {session} got {got_n}/{got_l} lock {n}/{n_liq}")
    return diffs


def load_published() -> dict:
    v4 = json.loads(
        (ROOT / "research/factor_mine_recipe_search_v4/returns/RESULTS.json").read_text(encoding="utf-8")
    )
    forward = json.loads(
        (ROOT / "research/factor_mine_recipe_search_v4/returns/FORWARD.json").read_text(encoding="utf-8")
    )
    by_id = {row["id"]: row for row in v4["ranked"]}
    fwd = {row["id"]: row for row in forward["results"]}
    return {"forward": fwd, "iwm": v4["iwm"], "tune": by_id}


def pct(value) -> str:
    if value is None:
        return "undefined"
    return f"{100.0 * float(value):.2f}%"


def dollars(value) -> str:
    if value is None:
        return "undefined"
    return f"${float(value):,.2f}"


def published_cell(value) -> str:
    if value is None:
        return "not published"
    return pct(value)


def write_reports(payloads, books, extras, randoms, iwms, diffs) -> None:
    published = load_published()
    windows = {
        "before": list(TUNE),
        "after": list(FORWARD),
        "overall": list(SESSIONS),
    }
    summary = {}
    for variant in VARIANTS:
        vid = variant["id"]
        book = books[vid]
        summary[vid] = {
            "positive_from": positive_starts(book),
            "windows": {},
        }
        for name, sessions in windows.items():
            stat = summarize_window(book, sessions)
            stat["ex"] = {}
            ranked = stat["ranked"]
            for k in (1, 3, 5):
                names = [ticker for ticker, _pnl in ranked[:k]]
                stat["ex"][str(k)] = rerun_without(
                    payloads, variant, extras["bars"], extras["fees"], names, sessions,
                )
            summary[vid]["windows"][name] = stat
        summary[vid]["random4"] = {
            name: randoms[vid][name] for name in windows
        }
        summary[vid]["iwm"] = {name: iwms[name] for name in windows}

    RETURNS.mkdir(parents=True, exist_ok=True)
    # Machine record. Day cards stay untouched.
    blob = {
        "diffs": diffs,
        "excluded_by_day": [
            {
                "date": payload["session"],
                "n_excluded": payload["n_excluded"],
                "tickers": sorted({row["ticker"] for row in payload["excluded_unexplained_legs"]}),
            }
            for payload in payloads
        ],
        "recipes": {},
    }
    for vid, stat in summary.items():
        blob["recipes"][vid] = _public_recipe(stat)
    (RETURNS / "RESULTS.json").write_text(
        json.dumps(blob, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    (RETURNS / "REPORT.md").write_text(
        render_report(summary, published, payloads, diffs), encoding="utf-8",
    )
    (ROOT / "research/hot_n4_clean_v4/RESULTS.md").write_text(
        render_plain(summary, published, payloads, diffs), encoding="utf-8",
    )


def _public_recipe(stat: dict) -> dict:
    out = {"positive_from": stat["positive_from"]["text"], "windows": {}}
    for name, window in stat["windows"].items():
        out["windows"][name] = {
            "best": window["best"],
            "best_share": window["best_share"],
            "book_pnl": window["book_pnl"],
            "buys": window["buys"],
            "closed": window["closed"],
            "ex": window["ex"],
            "joint": window["joint"],
            "rejects": window["rejects"],
            "returns": window["returns"],
            "rule21": window["rule21"],
            "trades_per_session": window["trades_per_session"],
            "unfilled": window["unfilled"],
            "up": window["up"],
            "up_days_denom": window["up_days_denom"],
            "up_share": window["up_share"],
            "wilson": window["wilson"],
            "wins": window["wins"],
            "skips": window["skips"],
        }
    out["iwm"] = stat["iwm"]
    out["random4"] = stat["random4"]
    return out


def render_report(summary, published, payloads, diffs) -> str:
    lines = [
        "# hot_n4_clean_v4 scores",
        "",
        "Primary path: Futubull fees plus 0.5% per side, keep-held, 1% dollar-volume cap, fills at the open.",
        "0% and 1% reprice those shares. Flat 15bp is 7.5bp per side on the actual open and no slip.",
        "The day ledger is not rewritten by this file.",
        "",
        "## Returns",
        "",
        "| recipe | window | 0% slip | 0.5% slip | 1% slip | flat 15bp | trades | buys | win rate | Wilson low | Wilson high | unfilled |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for vid, stat in summary.items():
        for name in ("before", "after", "overall"):
            window = stat["windows"][name]
            w = window["wilson"]
            lines.append(
                f"| `{vid}` | {name} | {pct(window['returns']['0.0'])} | {pct(window['returns']['0.005'])} | "
                f"{pct(window['returns']['0.01'])} | {pct(window['returns']['15'])} | {window['closed']} | "
                f"{window['buys']} | {pct(window['wilson']['p']) if w else 'undefined'} | "
                f"{pct(w['low']) if w else 'undefined'} | {pct(w['high']) if w else 'undefined'} | {window['unfilled']} |"
            )
    lines += [
        "",
        "## Without the largest closed trades",
        "",
        "The rerun makes those tickers ineligible from the first session. The original day cards stay as written.",
        "",
        "| recipe | window | removed | primary | flat 15bp |",
        "| --- | --- | --- | ---: | ---: |",
    ]
    for vid, stat in summary.items():
        for name in ("before", "after", "overall"):
            window = stat["windows"][name]
            for k in ("1", "3", "5"):
                ex = window["ex"][k]
                label = ", ".join(ex["names"]) if ex["names"] else "nobody"
                lines.append(
                    f"| `{vid}` | {name} | top {k}: {label} | {pct(ex['return'])} | {pct(ex['return_15'])} |"
                )
    lines += [
        "",
        "## Best-stock share, up days, joint",
        "",
        "Only the after window can reject. A before or overall joint under 0.5 is not a rejection.",
        "",
        "| recipe | window | best | share | up days | winning-day share | trades per session | joint | after reject | rule 21 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for vid, stat in summary.items():
        for name in ("before", "after", "overall"):
            window = stat["windows"][name]
            lines.append(
                f"| `{vid}` | {name} | {window['best'] or 'nobody'} | "
                f"{pct(window['best_share']) if window['best_share'] is not None else 'undefined'} | "
                f"{window['up']} of {window['up_days_denom']} | {pct(window['up_share']) if window['up_share'] is not None else 'undefined'} | "
                f"{window['trades_per_session']:.3f} | "
                f"{pct(window['joint']) if window['joint'] is not None else 'undefined'} | "
                f"{('yes' if window['rejects'] else 'no') if name == 'after' else 'n/a'} | "
                f"{'met' if window['rule21'] else 'not met'} |"
            )
    lines += ["", "## Positive from X of Y", ""]
    for vid, stat in summary.items():
        lines.append(f"- `{vid}`: {stat['positive_from']['text']} (Mondays through 2026-09-11, closed-trade P&L).")
    lines += [
        "",
        "## IWM and RANDOM4",
        "",
        "IWM is bought at the first open of that window after the Futubull entry fee only, with no 0.5% slip and no dollar cap. A session with no pinned IWM bar leaves the mark at the last stored close. The after-window figure is a fresh buy, not the continuous book. RANDOM4 is 1,000 draws, seed 20260813 plus the draw index, 4 names from that morning's candidate list, on the continuous book.",
        "",
        "| recipe | window | IWM | IWM 15bp | RANDOM4 | RANDOM4 15bp |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for vid, stat in summary.items():
        for name in ("before", "after", "overall"):
            lines.append(
                f"| `{vid}` | {name} | {pct(stat['iwm'][name]['return'])} | {pct(stat['iwm'][name]['return_15'])} | "
                f"{pct(stat['random4'][name]['mean'])} | {pct(stat['random4'][name]['mean_15'])} |"
            )
    lines += ["", "## Names removed before ranking", ""]
    lines.append("| session | n_excluded | tickers |")
    lines.append("| --- | ---: | --- |")
    for payload in payloads:
        tickers = sorted({row["ticker"] for row in payload["excluded_unexplained_legs"]})
        lines.append(f"| {payload['session']} | {payload['n_excluded']} | {', '.join(tickers)} |")
    if diffs:
        lines += ["", "## Exclusion mismatches", ""]
        for row in diffs:
            lines.append(f"- {row}")
    else:
        lines += ["", "Exclusions match `EXCLUSIONS_PLAN.csv`.", ""]
    lines += ["", "## Published v4, same recipe id", ""]
    lines.append("Copied from `research/factor_mine_recipe_search_v4/returns/RESULTS.json` and `FORWARD.json`. A blank in those files is `not published` here. Not recomputed.")
    lines.append("")
    lines.append("| recipe | window | v4 compound | v4 flat 15bp | v4 ex-best | v4 win rate | v4 up share | v4 trades | v4 RANDOM4 | v4 IWM | v4 joint | v4 status |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    iwm_tune = published["iwm"].get("continuous")
    for vid in summary:
        tune = published["tune"].get(vid) or {}
        cont = tune.get("continuous") or {}
        fwd = published["forward"].get(vid) or {}
        lines.append(
            f"| `{vid}` | before | {published_cell(cont.get('compound'))} | {published_cell(cont.get('compound_15'))} | "
            f"{published_cell(cont.get('ex_best'))} | {published_cell(cont.get('win_rate'))} | {published_cell(cont.get('up_share'))} | "
            f"{cont.get('n', 'not published')} | {published_cell(tune.get('random4'))} | {published_cell(iwm_tune)} | "
            f"{published_cell(cont.get('joint'))} | not published |"
        )
        lines.append(
            f"| `{vid}` | after | {published_cell(fwd.get('compound'))} | {published_cell(fwd.get('compound_15'))} | "
            f"{published_cell(fwd.get('ex_best'))} | {published_cell(fwd.get('win_rate'))} | {published_cell(fwd.get('up_share'))} | "
            f"{fwd.get('n', 'not published')} | not published | not published | {published_cell(fwd.get('joint'))} | {fwd.get('status', 'not published')} |"
        )
        lines.append(
            f"| `{vid}` | slip 0% / 1%, Wilson, dollar-volume cap, skipped count, positive-from-X-of-Y, ex top 3, ex top 5 | not published | not published | not published | not published | not published | not published | not published | not published | not published | not published |"
        )
    lines += ["", "## Cap studies", ""]
    lines.append(cap_table())
    lines.append("")
    return "\n".join(lines)


def _md_cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def cap_table() -> str:
    """Copy published width-4 cap-none rows. The exact recipe id is absent."""
    specs = (
        ("concentration_cap_v1", "research/concentration_cap_v1/returns/REPORT.md"),
        ("concentration_cap_v2", "research/concentration_cap_v2/returns/REPORT.md"),
        ("concentration_cap_v3", "research/concentration_cap_v3/returns/REPORT.md"),
    )
    needles = {
        "union_hot_n4_h1__w0__n4__cnone",
        "union_hot_n4_holdup__w0__n4__cnone",
    }
    lines = [
        "The cap studies do not publish `union_hot_n4_h1__w0` or `union_hot_n4_holdup__w0`.",
        "The rows below are the published width-4 cap-none ids, copied with that file's own column names.",
        "",
    ]
    for study, path in specs:
        text = (ROOT / path).read_text(encoding="utf-8")
        section = ""
        header = None
        grouped: dict[str, list[list[str]]] = {}
        order: list[str] = []
        for line in text.splitlines():
            if line.startswith("## "):
                section = line[3:].strip()
            if line.startswith("| id |"):
                header = _md_cells(line)
            if not line.startswith("| `"):
                continue
            cells = _md_cells(line)
            if not cells or cells[0].strip("`") not in needles or header is None:
                continue
            key = section or "published"
            if key not in grouped:
                grouped[key] = []
                order.append(key)
                grouped[key].append(header)
            grouped[key].append(cells)
        lines.append(f"### {study}")
        lines.append("")
        if not grouped:
            lines.append("not published")
            lines.append("")
            continue
        for key in order:
            rows = grouped[key]
            lines.append(f"**{key}**")
            lines.append("")
            head = rows[0]
            lines.append("| " + " | ".join(head) + " |")
            lines.append("| " + " | ".join("---" if i == 0 else "---:" for i in range(len(head))) + " |")
            for cells in rows[1:]:
                padded = cells + [""] * (len(head) - len(cells))
                lines.append("| " + " | ".join(padded[:len(head)]) + " |")
            lines.append("")
    lines.append("Those cells are not this study's result.")
    return "\n".join(lines)


def render_plain(summary, published, payloads, diffs) -> str:
    lines = [
        "# hot_n4_clean_v4 results",
        "",
        "Both recipes were scored after the 31 day cards were written and the ledger check passed.",
        "The primary figure is Futubull fees plus 0.5% slippage per side. The same shares are also shown at 0% slippage, at 1% slippage, and on the flat 15bp schedule.",
        "Every session in this window is before the study date 2026-09-27, so each one is designed_after.",
        "",
    ]
    if diffs:
        lines.append("The exclusion log does not match `EXCLUSIONS_PLAN.csv`:")
        for row in diffs:
            lines.append(f"- {row}")
        lines.append("")
    else:
        lines.append("The exclusion log matches `EXCLUSIONS_PLAN.csv` on every session.")
        lines.append("")
    for vid, stat in summary.items():
        lines.append(f"## {vid}")
        lines.append("")
        for name, label in (
            ("before", "Before window, 2026-08-13 through 2026-09-11"),
            ("after", "After window, 2026-09-14 through 2026-09-25"),
            ("overall", "Overall, 2026-08-13 through 2026-09-25"),
        ):
            window = stat["windows"][name]
            w = window["wilson"]
            lines.append(f"### {label}")
            lines.append("")
            lines.append(
                f"Total return: {pct(window['returns']['0.005'])} at 0.5% slippage, "
                f"{pct(window['returns']['0.0'])} at 0%, {pct(window['returns']['0.01'])} at 1%, "
                f"and {pct(window['returns']['15'])} on the flat 15bp schedule."
            )
            lines.append(
                f"Closed trades: {window['closed']}. Buys: {window['buys']}. "
                f"Trades per session: {window['trades_per_session']:.3f}."
            )
            if w is None:
                lines.append("Win rate: undefined. There were no closed trades. Wilson interval: undefined.")
            else:
                lines.append(
                    f"Win rate: {pct(w['p'])} ({window['wins']} of {window['closed']}). "
                    f"95% Wilson interval: {pct(w['low'])} to {pct(w['high'])}. "
                    f"Unfilled or skipped orders, counted beside the rate and not inside it: {window['unfilled']}."
                )
            ex1 = window["ex"]["1"]
            ex3 = window["ex"]["3"]
            ex5 = window["ex"]["5"]
            lines.append(
                f"Return without the best stock ({', '.join(ex1['names']) or 'nobody'}): {pct(ex1['return'])}. "
                f"Without the best 3 ({', '.join(ex3['names']) or 'nobody'}): {pct(ex3['return'])}. "
                f"Without the best 5 ({', '.join(ex5['names']) or 'nobody'}): {pct(ex5['return'])}."
            )
            share = window["best_share"]
            lines.append(
                "Best stock's share of closed-trade P&L: "
                + (
                    "undefined, because closed-trade P&L is 0."
                    if share is None else
                    f"{pct(share)} ({window['best']}). "
                    f"That ticker's closed P&L is {dollars(window['best_pnl'])} and the book's closed P&L is {dollars(window['book_pnl'])}."
                )
            )
            lines.append(
                f"Up days: {window['up']} of {window['up_days_denom']} sessions that had a buy or a sell. "
                f"Winning-day share: {pct(window['up_share']) if window['up_share'] is not None else 'undefined'}."
            )
            iwm = stat["iwm"][name]
            if not iwm.get("filled", True):
                iwm_line = (
                    f"IWM buy-and-hold: {pct(iwm['return'])} after the entry fee, "
                    f"{pct(iwm['return_15'])} on the flat 15bp print. "
                    f"The buy did not fill ({iwm.get('reason')}), so the sleeve stayed cash."
                )
            else:
                missed = ", ".join(iwm.get("missing_sessions") or []) or "none"
                iwm_line = (
                    f"IWM buy-and-hold: {pct(iwm['return'])} after the entry fee, "
                    f"{pct(iwm['return_15'])} on the flat 15bp print. "
                    f"Last stored mark: {iwm.get('last_mark_date')}. "
                    f"Sessions with no pinned IWM bar, marked unchanged: {missed}."
                )
            lines.append(iwm_line)
            lines.append(
                f"RANDOM4 mean: {pct(stat['random4'][name]['mean'])} primary, "
                f"{pct(stat['random4'][name]['mean_15'])} flat 15bp."
            )
            lines.append(
                f"Rule 21 (≥30 closed trades and win rate above 55%): {'met' if window['rule21'] else 'not met'}."
            )
            if name == "after":
                joint_txt = pct(window["joint"]) if window["joint"] is not None else "undefined"
                if window["rejects"]:
                    verdict = "rejected, because there are at least 30 closed trades and the joint is under 0.5."
                elif window["closed"] < MIN_TRADES:
                    verdict = (
                        f"not rejected. Closed trades are {window['closed']}, which is under 30. "
                        f"The joint is {joint_txt}. Fewer than 30 closed trades cannot reject the variant and cannot prove it."
                    )
                else:
                    verdict = (
                        f"not rejected. The joint is {joint_txt}. "
                        "A joint at or above 0.5 does not prove the variant."
                    )
                lines.append("After-window reject rule: " + verdict)
            lines.append("")
        lines.append(
            f"Positive from X of Y, Y=3, Mondays 2026-08-17, 2026-08-24, and 2026-08-31 through 2026-09-11: {stat['positive_from']['text']}."
        )
        lines.append("")
    lines += [
        "## Rule 23",
        "",
        "Rule 23 asks for about 20 locked sessions that beat RANDOM4 and IWM after fees, including without the best stock, before any real money.",
        "This study has 0 such locked sessions. Every session here is designed_after, because the study date is 2026-09-27 and the window ends 2026-09-25.",
        "The hindsight comparison is in the tables above. It does not meet the rule 23 threshold.",
        "",
        "## Skipped sessions",
        "",
        "- 2026-09-07 is Labor Day. It is not a session and it is not in the 31.",
        "- 2026-08-28 has no frozen Theme Radar row. No new buys. A lot already held still follows the list-drop rule at the open. The session stays in the 31.",
        "- No session was halted. No held name loaded an unexplained open-over-previous-close or close-over-open leg, and IWM did not either.",
        "",
        "## Unfilled orders",
        "",
    ]
    any_skip = False
    for vid, stat in summary.items():
        skips = stat["windows"]["overall"]["skips"]
        if not skips:
            lines.append(f"`{vid}`: none.")
            continue
        any_skip = True
        lines.append(f"`{vid}`: {len(skips)}.")
        for row in skips:
            lines.append(f"- {row['date']} {row['ticker']} {row['side']}: {row['reason']}")
    if not any_skip:
        pass
    lines += [
        "",
        "## Morning score on weather days",
        "",
        "Holdup reads `general_score` from the proven weather file on 2026-08-25, 2026-08-27, 2026-09-08, and 2026-09-09.",
        "Those pre-open blobs do not contain `general_score`, so S is null and holdup does not lengthen the minimum hold on those four mornings.",
        "The other 27 mornings use the proven predict total, and holdup applies when that total is above 0.",
        "",
    ]
    return "\n".join(lines) + "\n"


def held_names_from(books: dict[str, dict]) -> set[str]:
    out = set()
    for book in books.values():
        out.update(book["pos"])
    return out


def walk(bars, panels, scores, fees) -> list[dict]:
    """Build every card. Commit only after the exclusion check matches.

    A halt raises before any commit when it fires on the first disagreement,
    and after the earlier cards when it fires later. This function commits
    only when the full window is clean, because a halt is reported instead
    of a partial ledger the prereg did not expect. If a halt fires, nothing
    is written.
    """
    payloads = []
    # Positions entering each session, per recipe. Both books are stepped
    # so a held-name halt is visible before the card is committed.
    positions = {variant["id"]: {} for variant in VARIANTS}
    for session in SESSIONS:
        held = set()
        for pos in positions.values():
            held.update(pos)
        payload = build_payload(bars, panels, scores, session, held)
        payloads.append(payload)
        # Step both books on this card so the next session knows holdings.
        # Banned is empty. This is the same simulator the score uses.
        one = [payload]
        # simulate() expects a full calendar index. Stepping one day with
        # a fresh book would reset cash. Replay the prefix instead.
        for variant in VARIANTS:
            book = simulate(payloads, variant, bars, fees)
            positions[variant["id"]] = book["pos"]
        print(
            f"{session} candidates {len(payload['candidates'])} "
            f"excluded {payload['n_excluded']} held {sorted(held)}",
            flush=True,
        )
    diffs = exclusion_diff(payloads)
    miss = missing_diff(bars, panels)
    if miss:
        print("MISSING BARS MISMATCH", flush=True)
        for row in miss:
            print(row, flush=True)
        raise SystemExit("missing bars do not match the lock")
    if diffs:
        print("EXCLUSION MISMATCH", flush=True)
        for row in diffs:
            print(row, flush=True)
        raise SystemExit("exclusions do not match EXCLUSIONS_PLAN.csv")
    for payload in payloads:
        commit_day(payload["session"], payload)
    verify_ledger()
    return payloads


def score_written(bars, fees) -> None:
    """Score the day cards already on disk. Does not write a day or a ledger line."""
    verify_ledger()
    payloads = []
    for session in SESSIONS:
        payloads.append(json.loads((DAYS / f"{session}.json").read_text(encoding="utf-8")))
    books = {}
    for variant in VARIANTS:
        books[variant["id"]] = simulate(payloads, variant, bars, fees)
        print(variant["id"], "trades", sum(1 for t in books[variant["id"]]["trades"] if t["side"] == "sell"), flush=True)
    windows = {"before": list(TUNE), "after": list(FORWARD), "overall": list(SESSIONS)}
    randoms = {}
    for variant in VARIANTS:
        randoms[variant["id"]] = {}
        for name, sessions in windows.items():
            print("random4", variant["id"], name, flush=True)
            randoms[variant["id"]][name] = random4(payloads, variant, bars, fees, sessions)
    iwms = {name: iwm_path(bars, fees, sessions) for name, sessions in windows.items()}
    write_reports(payloads, books, {"bars": bars, "fees": fees}, randoms, iwms, diffs=[])
    print("wrote returns and RESULTS.md", flush=True)


def main() -> None:
    check_pins()
    fees = load_fees()
    print("loading bars", flush=True)
    bars = load_bars()
    print("loading panels", flush=True)
    panels = load_panels()
    print("loading scores", flush=True)
    scores = load_scores()
    print("walking", flush=True)
    walk(bars, panels, scores, fees)
    print("ledger ok, scoring", flush=True)
    score_written(bars, fees)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--score-only":
        check_pins()
        print("loading bars", flush=True)
        bars = load_bars()
        print("ledger ok, scoring", flush=True)
        score_written(bars, load_fees())
    else:
        main()
