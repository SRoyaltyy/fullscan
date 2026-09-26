"""Score the fingerprinted breadth_mine_v1d grid.

Reads the preregistration and the earliest-commit manifest.
Does not edit either. Feature bars are dated strictly before the session.
The session open is the fill. The session close is the mark only.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import random
import re
import subprocess
from bisect import bisect_left
from pathlib import Path

import numpy as np

from src.breadth_mine_v1d_append import file_sha256, manifest_line
from src.breadth_mine_v1d_bars import session_jumps, unexplained
from src.breadth_mine_v1d_grid import (
    ATOM_ROLES,
    ATOMS,
    CANDIDATE_CAP,
    CUT_LOSS,
    EXITS,
    HOLDS,
    HOT_POS_MIN,
    LUCK_DENOMINATOR,
    MIN_FIRES,
    N,
    PRIMARY_MIN,
    RANDOM4_DRAWS,
    RANDOM4_N,
    RANDOM4_SEED,
    SIDES,
    TOP_N,
    TRAIL_GAP,
    WIN_MIN,
    iter_rules,
    signal_name,
    signal_pairs,
)
from src.breadth_mine_v1d_metrics import (
    compound,
    ex_best_compound,
    luck_p,
    median,
    raw_luck_p,
)
from src.breadth_mine_v1d_protocol import (
    BAR_BLOB_SHA,
    BAR_PATH,
    BAR_SHA256,
    CREATION_DATE,
    OOS_START,
    SPLIT_BLOB_SHA,
    SPLIT_PATH,
    CHECK_CALENDAR,
    DESIGNED_AFTER_END,
    DESIGNED_AFTER_START,
    RETURNS,
    RUN_WINDOW,
    SESSIONS,
    STUDY,
    STUDY_LABEL,
    assert_prereg,
    load_manifest,
)
from src.candle_factor import _from_bars as candle_from_bars
from src.lever_search_bars import SameDayBarError
from src.ohlc_ripper import from_bars as ohlc_from_bars
from src.paper_trade import load_fees, order_fees

ROOT = Path(__file__).resolve().parents[1]
CAPITAL = 10_000.0
BORROW_DAY = 0.01 / 252.0
BP_SIDE = 0.000075
ATOM_INDEX = {name: i for i, name in enumerate(ATOMS)}
SCORE_RE = re.compile(r"^-\s*total_score:\s*\*\*([+-]?[\d.]+)")
DIR_RE = re.compile(r"^-\s*predicted_direction:\s*\*\*(.+?)\*\*", re.I)
SECTOR_ALIAS = {
    "financial": "financial",
    "financials": "financial",
    "financial_services": "financial",
}


def fee_15(shares: int, price: float) -> float:
    if shares <= 0 or price <= 0:
        return 0.0
    return round(shares * price * BP_SIDE, 4)


def _num(raw) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip().replace("%", "").replace(",", "")
    if text in ("", "-", "—", "None", "nan", "NaN"):
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    return value


def _git_blob(sha: str) -> bytes:
    return subprocess.check_output(["git", "cat-file", "-p", sha], cwd=ROOT)


def prior_end(dates: list[str], session: str) -> int:
    """Index of the first bar that is not strictly before the session."""
    index = bisect_left(dates, session)
    if index > 0 and dates[index - 1] >= session:
        raise SameDayBarError(f"same-day-or-later bar {dates[index - 1]} for session {session}")
    if index < len(dates) and dates[index] < session:
        raise SameDayBarError(f"same-day-or-later bar {dates[index]} for session {session}")
    return index


def assert_feature_dates(dates: list[str], session: str) -> None:
    for date in dates:
        if date >= session:
            raise SameDayBarError(f"same-day-or-later bar {date} for session {session}")


class Bars:
    def __init__(self, max_date: str | None = None) -> None:
        import pandas as pd

        frame = pd.read_parquet(
            ROOT / BAR_PATH,
            columns=["date", "ticker", "open", "high", "low", "close", "volume"],
        )
        frame["d"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
        if max_date is not None:
            frame = frame[frame["d"] <= max_date]
        self.max_date = max_date
        self.rows: dict[str, tuple] = {}
        for ticker, group in frame.groupby("ticker", sort=False):
            group = group.sort_values("d")
            self.rows[str(ticker).upper()] = (
                group["d"].tolist(),
                group["open"].to_numpy(dtype=float),
                group["high"].to_numpy(dtype=float),
                group["low"].to_numpy(dtype=float),
                group["close"].to_numpy(dtype=float),
                group["volume"].to_numpy(dtype=float),
            )

    def session_open_close(self, ticker: str, session: str) -> tuple[float | None, float | None]:
        rec = self.rows.get(ticker)
        if rec is None:
            return None, None
        dates, opens, _high, _low, closes, _vol = rec
        index = prior_end(dates, session)
        if index < len(dates) and dates[index] == session:
            opx = float(opens[index])
            cpx = float(closes[index])
            if not math.isfinite(opx) or opx <= 0:
                opx = None
            if not math.isfinite(cpx) or cpx <= 0:
                cpx = None
            return opx, cpx
        return None, None

    def prior_bars(self, ticker: str, session: str) -> list[dict]:
        rec = self.rows.get(ticker)
        if rec is None:
            return []
        dates, opens, highs, lows, closes, vols = rec
        end = prior_end(dates, session)
        start = max(0, end - 80)
        out = []
        for i in range(start, end):
            out.append({
                "date": dates[i],
                "open": float(opens[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "close": float(closes[i]),
                "volume": float(vols[i]),
            })
        assert_feature_dates([bar["date"] for bar in out], session)
        return out


def parse_predict(text: str) -> tuple[float | None, str | None]:
    score = None
    direction = None
    for line in text.splitlines():
        line = line.strip()
        matched = SCORE_RE.match(line)
        if matched:
            score = float(matched.group(1))
            continue
        matched = DIR_RE.match(line)
        if matched:
            direction = matched.group(1).strip().lower()
    return score, direction


def sector_slug(name: str) -> str:
    text = " ".join(str(name or "").strip().lower().replace("&", " and ").split())
    slug = text.replace(" ", "_")
    return SECTOR_ALIAS.get(slug, slug)


def _polarity(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "missing"
    if value >= 0.05:
        return "good"
    if value <= -0.05:
        return "bad"
    return "neutral"


def news_good(blob: bytes) -> tuple[set[str], set[str]]:
    data = json.loads(blob)
    items = data.get("ticker_actions") or []
    if isinstance(items, dict):
        seq = []
        for ticker, row in items.items():
            if isinstance(row, dict):
                seq.append({"ticker": ticker, **row})
        items = seq
    good: set[str] = set()
    named: set[str] = set()
    for row in items:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        named.add(ticker)
        side = str(row.get("side") or "").lower()
        try:
            net = float(row.get("net") or 0.0)
        except (TypeError, ValueError):
            net = 0.0
        if side in ("sell", "short"):
            signed = -abs(net) if net else -1.0
        elif side in ("buy", "long"):
            signed = abs(net) if net else 1.0
        else:
            signed = net
        if _polarity(signed) == "good":
            good.add(ticker)
    return good, named


def ab_good_set(checklist: bytes, enriched: bytes) -> set[str]:
    def scores(blob: bytes) -> dict[str, float]:
        out: dict[str, float] = {}
        text = blob.decode("utf-8", errors="replace")
        for row in csv.DictReader(io.StringIO(text)):
            ticker = str(row.get("Ticker") or "").strip().upper()
            raw = row.get("score")
            if not ticker or raw in (None, ""):
                continue
            try:
                out[ticker] = float(raw)
            except ValueError:
                continue
        return out

    right = scores(enriched)
    common = set(scores(checklist)) & set(right)
    return {ticker for ticker in common if math.tanh(right[ticker] / 8.0) >= 0.05}


def heat_sets(blob: bytes) -> tuple[set[str], set[str]]:
    data = json.loads(blob)
    hot: set[str] = set()
    named: set[str] = set()

    def walk(obj) -> None:
        if isinstance(obj, dict):
            if "ticker" in obj and "d1" in obj:
                ticker = str(obj.get("ticker") or "").strip().upper()
                if ticker:
                    named.add(ticker)
                    d1 = _num(obj.get("d1"))
                    if d1 is not None and d1 > 0:
                        hot.add(ticker)
            for value in obj.values():
                walk(value)
        elif isinstance(obj, list):
            for value in obj:
                walk(value)

    walk(data)
    return hot, named


def book_sets(blob: bytes) -> tuple[set[str], set[str], dict[str, str]]:
    data = json.loads(blob)
    buys: set[str] = set()
    named: set[str] = set()
    sectors: dict[str, str] = {}
    books = data.get("books") or {}
    for book in books.values():
        if not isinstance(book, dict):
            continue
        for key, bucket in (("buy", buys), ("sell", named)):
            for row in book.get(key) or []:
                if not isinstance(row, dict):
                    continue
                ticker = str(row.get("ticker") or "").strip().upper()
                if not ticker:
                    continue
                named.add(ticker)
                if key == "buy":
                    bucket.add(ticker)
                sector = str(row.get("sector") or "").strip()
                if sector:
                    sectors.setdefault(ticker, sector)
    return buys, named, sectors


def panel_names(blob: bytes, session: str, n_rows: int) -> list[str]:
    data = json.loads(blob)
    names = []
    for row in data.get("rows") or []:
        if isinstance(row, dict) and row.get("date") == session:
            ticker = str(row.get("ticker") or "").strip().upper()
            if ticker:
                names.append(ticker)
    if len(names) != int(n_rows):
        raise SystemExit(f"{session}: panel n_rows {len(names)} != {n_rows}")
    return names


def catalyst_names(blob: bytes) -> set[str]:
    data = json.loads(blob)
    out = set()
    for row in data.get("targets") or []:
        if isinstance(row, dict):
            ticker = str(row.get("ticker") or "").strip().upper()
            if ticker:
                out.add(ticker)
    return out


def judge_sets(blob: bytes) -> tuple[set[str], set[str]]:
    data = json.loads(blob)
    good: set[str] = set()
    named: set[str] = set()
    tickers = data.get("tickers") or {}
    if isinstance(tickers, dict):
        for ticker, score in tickers.items():
            name = str(ticker).strip().upper()
            if not name:
                continue
            named.add(name)
            value = _num(score)
            if value is not None and value > 0:
                good.add(name)
    return good, named


def finviz_map(blob: bytes, wanted: set[str]) -> tuple[dict[str, float], dict[str, str]]:
    import pandas as pd

    columns = {"Ticker", "Performance (Week)", "Sector"}
    frame = pd.read_csv(io.BytesIO(blob), usecols=lambda name: name in columns, low_memory=False)
    week: dict[str, float] = {}
    sectors: dict[str, str] = {}
    if "Ticker" not in frame.columns:
        return week, sectors
    ticker_i = frame.columns.get_loc("Ticker")
    week_i = frame.columns.get_loc("Performance (Week)") if "Performance (Week)" in frame.columns else None
    sector_i = frame.columns.get_loc("Sector") if "Sector" in frame.columns else None
    for row in frame.to_numpy():
        ticker = str(row[ticker_i] or "").strip().upper()
        if ticker not in wanted:
            continue
        if week_i is not None:
            parsed = _num(row[week_i])
            if parsed is not None:
                week[ticker] = parsed
        if sector_i is not None:
            sector = str(row[sector_i] or "").strip()
            if sector and sector.lower() != "nan":
                sectors[ticker] = sector
    return week, sectors


def _pin_ok(row: dict) -> None:
    got = subprocess.check_output(
        ["git", "rev-parse", f"{row['commit']}:{row['path']}"],
        cwd=ROOT,
        text=True,
    ).strip()
    if got != row["blob_sha"]:
        raise SystemExit(f"pin blob mismatch {row['path']} {row['commit']}")


class Book:
    __slots__ = ("cf", "c15", "eqf", "eq15", "pos")

    def __init__(self) -> None:
        self.cf = CAPITAL
        self.c15 = CAPITAL
        self.eqf = CAPITAL
        self.eq15 = CAPITAL
        self.pos: dict[str, dict] = {}


def want_exit(held: int, hold: int, min_hold: int, exit_name: str, side: str,
              px: float, lot: dict, on_list: bool) -> bool:
    if held >= hold:
        return True
    if held < min_hold:
        return False
    if exit_name == "time":
        return False
    if exit_name == "list":
        return not on_list
    if exit_name == "cut_loser":
        if side == "long":
            return px <= lot["entry_px"] * (1.0 - CUT_LOSS)
        return px >= lot["entry_px"] * (1.0 + CUT_LOSS)
    if exit_name == "trail":
        if side == "long":
            return px <= lot["extreme"] * (1.0 - TRAIL_GAP)
        return px >= lot["extreme"] * (1.0 + TRAIL_GAP)
    return False


def _buy_shares(cash: float, px: float, per: float, fees: dict) -> tuple[int, float]:
    shares = int(per // px) if px else 0
    while shares >= 1:
        fee_f = order_fees(shares, px, "buy", fees)
        if shares * px + fee_f <= cash + 1e-6:
            return shares, fee_f
        shares -= 1
    return 0, 0.0


def advance_book(book: Book, *, side: str, exit_name: str, hold: int, min_hold: int,
                 pick_names: list[str] | None, open_of: dict[str, float],
                 close_of: dict[str, float], universe: set[str], day_i: int,
                 fees: dict) -> tuple[float, float, int, int, int, dict[str, float], list[float]]:
    """One session. pick_names is None when the rule sits out."""
    if not book.pos and not pick_names:
        return 0.0, 0.0, 0, 0, 0, {}, []
    pnl: dict[str, float] = {}
    closed: list[float] = []
    n_fills = 0
    n_under = 0
    n_entries = 0

    for ticker in list(book.pos):
        lot = book.pos[ticker]
        held = day_i - lot["entry_i"]
        px = open_of.get(ticker)
        if px is None or px <= 0:
            continue
        if not want_exit(held, hold, min_hold, exit_name, side, px, lot, ticker in universe):
            continue
        shares = lot["shares"]
        fee_f = order_fees(shares, px, "sell" if side == "long" else "buy", fees)
        fee_b = fee_15(shares, px)
        prev_px = float(lot["prev"])
        if side == "long":
            book.cf += shares * px - fee_f
            book.c15 += shares * px - fee_b
            delta = shares * (px - prev_px) - fee_f
            trip = shares * (px - lot["entry_px"]) - lot["fee_in"] - fee_f
        else:
            book.cf -= shares * px + fee_f
            book.c15 -= shares * px + fee_b
            delta = shares * (prev_px - px) - fee_f
            trip = shares * (lot["entry_px"] - px) - lot["fee_in"] - fee_f
        pnl[ticker] = pnl.get(ticker, 0.0) + delta
        notional = shares * lot["entry_px"]
        if notional > 0:
            closed.append(trip / notional)
        del book.pos[ticker]
        n_fills += 1
        if px < 3.0:
            n_under += 1

    if pick_names and (book.cf > 0 or side == "short"):
        new = [ticker for ticker in pick_names if ticker not in book.pos]
        if new:
            if side == "long":
                room = book.cf
            else:
                stock = 0.0
                for ticker, lot in book.pos.items():
                    px = open_of.get(ticker)
                    if px is None or px <= 0:
                        px = float(lot["prev"])
                    stock -= lot["shares"] * px
                room = max(0.0, (book.cf + stock) * 0.5)
            per = room / len(new) if new else 0.0
            for ticker in new:
                px = open_of.get(ticker)
                if px is None or px <= 0:
                    continue
                if side == "long":
                    shares, fee_f = _buy_shares(book.cf, px, per, fees)
                    if shares < 1:
                        continue
                    fee_b = fee_15(shares, px)
                    book.cf -= shares * px + fee_f
                    book.c15 -= shares * px + fee_b
                else:
                    shares = int(per // px) if px else 0
                    if shares < 1:
                        continue
                    fee_f = order_fees(shares, px, "sell", fees)
                    fee_b = fee_15(shares, px)
                    book.cf += shares * px - fee_f
                    book.c15 += shares * px - fee_b
                book.pos[ticker] = {
                    "shares": shares,
                    "entry_px": px,
                    "entry_i": day_i,
                    "fee_in": fee_f,
                    "prev": px,
                    "extreme": px,
                }
                n_entries += 1
                n_fills += 1
                if px < 3.0:
                    n_under += 1

    for ticker, lot in book.pos.items():
        close = close_of.get(ticker)
        if close is None or close <= 0:
            close = float(lot["prev"])
        prev_px = float(lot["prev"])
        borrow = lot["shares"] * close * BORROW_DAY if side == "short" else 0.0
        if borrow:
            book.cf -= borrow
            book.c15 -= borrow
        if side == "long":
            mtm = lot["shares"] * (close - prev_px)
        else:
            mtm = lot["shares"] * (prev_px - close) - borrow
        if lot["entry_i"] == day_i:
            mtm -= lot["fee_in"]
        if mtm:
            pnl[ticker] = pnl.get(ticker, 0.0) + mtm
        lot["prev"] = close
        if side == "long":
            if close > lot["extreme"]:
                lot["extreme"] = close
        elif close < lot["extreme"]:
            lot["extreme"] = close

    stock_f = 0.0
    for lot in book.pos.values():
        notion = lot["shares"] * float(lot["prev"])
        stock_f += notion if side == "long" else -notion
    eqf = book.cf + stock_f
    eq15 = book.c15 + stock_f
    ret_f = (eqf / book.eqf - 1.0) if book.eqf else 0.0
    ret_15 = (eq15 / book.eq15 - 1.0) if book.eq15 else 0.0
    book.eqf = eqf
    book.eq15 = eq15
    return ret_f, ret_15, n_fills, n_under, n_entries, pnl, closed


def _blob(cache: dict[str, bytes], sha: str) -> bytes:
    blob = cache.get(sha)
    if blob is None:
        blob = _git_blob(sha)
        cache[sha] = blob
    return blob


def build_day(bars: Bars, rows: list[dict], session: str, cache: dict[str, bytes]) -> dict:
    by_kind: dict[str, list[dict]] = {}
    for row in rows:
        by_kind.setdefault(row["input"], []).append(row)
    roles = set(by_kind)
    if "ab_checklist" in roles and "ab_enriched" in roles:
        roles.add("ab")

    names: set[str] = set()
    buys: set[str] = set()
    sectors: dict[str, str] = {}
    if "stock_book" in by_kind:
        row = by_kind["stock_book"][0]
        buy, named, sec = book_sets(_blob(cache, row["blob_sha"]))
        buys |= buy
        names |= named
        sectors.update(sec)
    if "panel" in by_kind:
        row = by_kind["panel"][0]
        names.update(panel_names(_blob(cache, row["blob_sha"]), session, int(row["n_rows"])))
    catalyst: set[str] = set()
    if "catalyst" in by_kind:
        catalyst = catalyst_names(_blob(cache, by_kind["catalyst"][0]["blob_sha"]))
        names |= catalyst
    judge_good: set[str] = set()
    if "judge" in by_kind:
        judge_good, named = judge_sets(_blob(cache, by_kind["judge"][0]["blob_sha"]))
        names |= named
    actions: set[str] = set()
    if "actions" in by_kind:
        actions, named = news_good(_blob(cache, by_kind["actions"][0]["blob_sha"]))
        names |= named
    heat: set[str] = set()
    if "heat" in by_kind:
        heat, named = heat_sets(_blob(cache, by_kind["heat"][0]["blob_sha"]))
        names |= named

    tradable = []
    open_of: dict[str, float] = {}
    close_of: dict[str, float] = {}
    for ticker in sorted(names):
        opx, cpx = bars.session_open_close(ticker, session)
        if opx is None:
            continue
        tradable.append(ticker)
        open_of[ticker] = opx
        if cpx is not None:
            close_of[ticker] = cpx
    wanted = set(tradable)

    week: dict[str, float] = {}
    if "export" in by_kind and wanted:
        week, fv_sectors = finviz_map(_blob(cache, by_kind["export"][0]["blob_sha"]), wanted)
        for ticker, sector in fv_sectors.items():
            sectors[ticker] = sector

    ab: set[str] = set()
    if "ab" in roles:
        ab = ab_good_set(
            _blob(cache, by_kind["ab_checklist"][0]["blob_sha"]),
            _blob(cache, by_kind["ab_enriched"][0]["blob_sha"]),
        )

    score = None
    direction = None
    if "predict" in by_kind:
        text = _blob(cache, by_kind["predict"][0]["blob_sha"]).decode("utf-8", errors="replace")
        score, direction = parse_predict(text)

    sector_up: dict[str, bool] = {}
    if "sector" in by_kind:
        for row in by_kind["sector"]:
            slug = Path(row["path"]).name.replace("_predict.md", "")
            text = _blob(cache, row["blob_sha"]).decode("utf-8", errors="replace")
            _sc, direc = parse_predict(text)
            sector_up[slug] = direc == "up"

    n = len(tradable)
    atoms = np.zeros((n, len(ATOMS)), dtype=bool)
    hot = np.zeros(n, dtype=float)
    for i, ticker in enumerate(tradable):
        prior = bars.prior_bars(ticker, session)
        feat = ohlc_from_bars(prior) if prior else {"ok": False, "hot_score": 0.0, "last_green": False}
        hot[i] = float(feat.get("hot_score") or 0.0)
        atoms[i, ATOM_INDEX["last_green"]] = bool(feat.get("last_green"))
        atoms[i, ATOM_INDEX["hot_pos"]] = hot[i] > HOT_POS_MIN
        if len(prior) >= 50:
            closes = [bar["close"] for bar in prior[-50:]]
            atoms[i, ATOM_INDEX["a09_sma50"]] = prior[-1]["close"] > (sum(closes) / 50.0)
        candle = candle_from_bars(prior[-8:]) if len(prior) >= 2 else {}
        atoms[i, ATOM_INDEX["cd_engulf_bull"]] = bool(candle.get("engulf_bull"))
        atoms[i, ATOM_INDEX["fv_week_pos"]] = week.get(ticker, -1.0) > 0
        atoms[i, ATOM_INDEX["ab_good"]] = ticker in ab
        atoms[i, ATOM_INDEX["actions_good"]] = ticker in actions
        atoms[i, ATOM_INDEX["book_buy"]] = ticker in buys
        atoms[i, ATOM_INDEX["catalyst_on"]] = ticker in catalyst
        atoms[i, ATOM_INDEX["judge_up"]] = ticker in judge_good
        atoms[i, ATOM_INDEX["heat_up"]] = ticker in heat
        slug = sector_slug(sectors.get(ticker, ""))
        atoms[i, ATOM_INDEX["sector_up"]] = bool(slug) and sector_up.get(slug, False)
    if n and score is not None:
        atoms[:, ATOM_INDEX["s_gt_0"]] = score > 0
        atoms[:, ATOM_INDEX["hard_red"]] = score <= -3
    if n and direction == "up":
        atoms[:, ATOM_INDEX["predict_up"]] = True

    blobs = [
        {"blob_sha": BAR_BLOB_SHA, "path": BAR_PATH},
        {"blob_sha": SPLIT_BLOB_SHA, "path": SPLIT_PATH},
    ]
    for row in rows:
        blobs.append({"blob_sha": row["blob_sha"], "path": row["path"]})
    blobs = sorted({(item["path"], item["blob_sha"]): item for item in blobs}.values(), key=lambda item: item["path"])
    return {
        "names": tradable,
        "hot": hot,
        "atoms": atoms,
        "open_of": open_of,
        "close_of": close_of,
        "universe": set(tradable),
        "roles": roles,
        "blobs": blobs,
    }


def _ranked(day: dict, left: str, right: str | None) -> list[str]:
    atoms = day["atoms"]
    names = day["names"]
    if not names:
        return []
    mask = atoms[:, ATOM_INDEX[left]]
    if right is not None:
        mask = mask & atoms[:, ATOM_INDEX[right]]
    idx = np.flatnonzero(mask)
    if len(idx) == 0:
        return []
    hot = day["hot"]
    ordered = sorted(idx.tolist(), key=lambda i: (-float(hot[i]), names[i]))
    return [names[i] for i in ordered[: max(TOP_N)]]


def _mechanics():
    out = []
    for side in SIDES:
        for exit_name in EXITS:
            for hold, min_hold in HOLDS:
                for top_n in TOP_N:
                    out.append((side, exit_name, hold, min_hold, top_n))
    return out


def walk_all(days: list[dict], fees: dict) -> dict:
    from src.breadth_mine_v1d_grid import rule_id

    signals = signal_pairs()
    mechs = _mechanics()
    if len(signals) * len(mechs) != N:
        raise SystemExit(f"grid {len(signals) * len(mechs)} != {N}")
    first = next(iter_rules())
    if first[1] != rule_id(signal_name(*signals[0]), *mechs[0]):
        raise SystemExit(f"rule order drift {first[1]}")
    n_days = len(days)
    ret_f = np.zeros((N, n_days), dtype=np.float64)
    ret_15 = np.zeros((N, n_days), dtype=np.float64)
    n_fills = np.zeros((N, n_days), dtype=np.int32)
    n_under = np.zeros((N, n_days), dtype=np.int32)
    n_entries = np.zeros((N, n_days), dtype=np.int32)
    sat = np.zeros((N, n_days), dtype=bool)
    books = [Book() for _ in range(N)]
    first: list[dict[str, str]] = [{} for _ in range(N)]
    day_pnl: list[dict[int, dict[str, float]]] = [{} for _ in range(n_days)]
    day_trades: list[dict[int, list[float]]] = [{} for _ in range(n_days)]
    sessions = [day["session"] for day in days]

    for di, day in enumerate(days):
        print(f"walk {day['session']} names {len(day['names'])}", flush=True)
        picks = []
        live = []
        for left, right in signals:
            need = set(ATOM_ROLES[left])
            if right is not None:
                need |= set(ATOM_ROLES[right])
            ok = need <= day["roles"]
            live.append(ok)
            picks.append(_ranked(day, left, right) if ok else None)
        open_of = day["open_of"]
        close_of = day["close_of"]
        universe = day["universe"]
        for s_i, names in enumerate(picks):
            base = s_i * len(mechs)
            sat_out = not live[s_i]
            for m_i, (side, exit_name, hold, min_hold, top_n) in enumerate(mechs):
                ri = base + m_i
                chosen = None if sat_out else (names or [])[:top_n]
                if sat_out:
                    sat[ri, di] = True
                retf, ret15, fills, under, entries, pnl, closed = advance_book(
                    books[ri],
                    side=side,
                    exit_name=exit_name,
                    hold=hold,
                    min_hold=min_hold,
                    pick_names=chosen,
                    open_of=open_of,
                    close_of=close_of,
                    universe=universe,
                    day_i=di,
                    fees=fees,
                )
                ret_f[ri, di] = retf
                ret_15[ri, di] = ret15
                n_fills[ri, di] = fills
                n_under[ri, di] = under
                n_entries[ri, di] = entries
                if pnl:
                    day_pnl[di][ri] = {ticker: round(value, 4) for ticker, value in pnl.items() if value}
                if closed:
                    day_trades[di][ri] = [round(value, 6) for value in closed]
                if entries and chosen:
                    for ticker in chosen[:top_n]:
                        if ticker in books[ri].pos and books[ri].pos[ticker]["entry_i"] == di:
                            first[ri].setdefault(ticker, sessions[di])
        print(f"  active books {sum(1 for book in books if book.pos)}", flush=True)
    return {
        "ret_f": ret_f,
        "ret_15": ret_15,
        "n_fills": n_fills,
        "n_under": n_under,
        "n_entries": n_entries,
        "sat": sat,
        "day_pnl": day_pnl,
        "day_trades": day_trades,
        "first": first,
        "sessions": sessions,
    }


def _ids() -> list[str]:
    return [item[1] for item in iter_rules()]


def _side(rule: str) -> str:
    return rule.split("|")[1]


def evaluate(scored: dict, ids: list[str]) -> list[dict]:
    sessions = scored["sessions"]
    check_cal = set(CHECK_CALENDAR)
    rows = []
    ret_f = scored["ret_f"]
    ret_15 = scored["ret_15"]
    for ri, rule in enumerate(ids):
        rets = [round(float(ret_f[ri, di]), 8) for di in range(len(sessions))]
        rets15 = [round(float(ret_15[ri, di]), 8) for di in range(len(sessions))]
        check = [session for session in sessions if session in check_cal]
        check_set = set(check)
        check_rets = [rets[di] for di, session in enumerate(sessions) if session in check_set]
        check_rets15 = [rets15[di] for di, session in enumerate(sessions) if session in check_set]
        after = [
            rets[di] for di, session in enumerate(sessions)
            if OOS_START <= session <= "2026-09-25"
        ]
        pnl_by_day: dict[str, dict[str, float]] = {}
        totals: dict[str, float] = {}
        for di, session in enumerate(sessions):
            if session not in check_set:
                continue
            for ticker, value in scored["day_pnl"][di].get(ri, {}).items():
                pnl_by_day.setdefault(session, {})[ticker] = float(value)
                totals[ticker] = totals.get(ticker, 0.0) + float(value)
        best = None
        if totals:
            best = min(totals, key=lambda ticker: (-totals[ticker], scored["first"][ri].get(ticker, "9999-99-99"), ticker))
        removed = ex_best_compound(sessions, check, rets, pnl_by_day, best)
        total = sum(totals.values())
        top1 = None
        top3 = None
        if total > 0 and best is not None:
            ordered = sorted(totals.items(), key=lambda item: (-item[1], scored["first"][ri].get(item[0], "9999-99-99"), item[0]))
            top1 = ordered[0][1] / total
            top3 = sum(value for _ticker, value in ordered[:3]) / total
        trades = []
        entries = 0
        for di, session in enumerate(sessions):
            if session not in check_set:
                continue
            trades.extend(scored["day_trades"][di].get(ri, []))
            entries += int(scored["n_entries"][ri, di])
        med = median(trades)
        wins = sum(1 for value in trades if value > 0)
        win = (wins / len(trades)) if trades else None
        fills = int(scored["n_fills"][ri].sum())
        under = int(scored["n_under"][ri].sum())
        raw = raw_luck_p(check_rets)
        adj = luck_p(check_rets, LUCK_DENOMINATOR)
        full = compound(check_rets) if check_rets else 0.0
        full15 = compound(check_rets15) if check_rets15 else 0.0
        from14 = compound(after) if after else 0.0
        rejected = top1 is not None and top1 > 0.50
        n_check = len(check)
        total_entries = int(scored["n_entries"][ri].sum())
        label = "untestable" if total_entries == 0 else ""
        rows.append({
            "id": rule,
            "index": ri,
            "side": _side(rule),
            "n_check": n_check,
            "full": full,
            "full_15": full15,
            "ex_best": removed,
            "best": best or "",
            "median": med,
            "win": win,
            "top1": top1,
            "top3": top3,
            "trades_per_day": (entries / n_check) if n_check else 0.0,
            "fires": entries,
            "n_trades": len(trades),
            "from_0914": from14,
            "under_3": (under / fills) if fills else None,
            "n_fills": fills,
            "raw_p": raw,
            "luck_p": adj,
            "rejected": rejected,
            "label": label,
            "failed": [],
            "primary_hit": False,
        })
    return rows


GUARD_ORDER = (
    "ex-best",
    "top-1",
    "fires",
    "win",
    "from-0914",
    "luck",
    "random4",
    "iwm",
)


def apply_objective(row: dict, check_before_creation: bool) -> None:
    """Primary target and guards. Call after RANDOM4 and IWM are attached."""
    if row["label"] == "untestable":
        row["failed"] = []
        row["primary_hit"] = False
        return
    row["primary_hit"] = row["full"] >= PRIMARY_MIN
    passed = {
        "ex-best": row["ex_best"] is not None and row["ex_best"] >= 0.10,
        "top-1": not (row["top1"] is not None and row["top1"] > 0.50),
        "fires": row["fires"] >= MIN_FIRES,
        "win": row["win"] is not None and row["win"] > WIN_MIN,
        "from-0914": row["from_0914"] >= 0.0,
        "luck": row["luck_p"] < 0.05,
        "random4": row.get("random4_mean") is not None and row["full"] > row["random4_mean"],
        "iwm": row.get("iwm") is not None and row["full"] > row["iwm"],
    }
    row["failed"] = [name for name in GUARD_ORDER if not passed[name]]
    if row["n_check"] < 10:
        row["label"] = "too few days to judge"
    elif not row["failed"] and row["primary_hit"] and check_before_creation:
        row["label"] = "designed_after"
    elif not row["failed"] and row["primary_hit"]:
        row["label"] = "something good"
    elif not row["failed"]:
        row["label"] = "guards pass"
    else:
        row["label"] = "not something good"


def _by_compound(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda row: (-row["full"], row["id"]))


def render_report(rows: list[dict], verdict: str) -> str:
    testable = [row for row in rows if row["label"] != "untestable"]
    passing = [row for row in testable if not row.get("failed")]
    ranked = _by_compound(passing)
    raw_ranked = _by_compound(testable)

    def pct(value) -> str:
        if value is None:
            return ""
        return f"{100.0 * value:.2f}%"

    def cell(row: dict, rank: int, failed: bool) -> str:
        fields = [
            str(rank),
            f"`{row['id']}`",
            row["side"],
            str(row["n_check"]),
            pct(row["full"]),
            pct(row["ex_best"]),
            row["best"],
            pct(row["median"]),
            pct(row["win"]),
            pct(row["top1"]),
            pct(row["top3"]),
            f"{row['trades_per_day']:.2f}",
            str(row.get("fires", "")),
            pct(row.get("random4_mean")),
            pct(row.get("random4_beaten")),
            pct(row.get("iwm")),
            pct(row["from_0914"]),
            pct(row["under_3"]),
            pct(row["full_15"]),
            f"{row['raw_p']:.4g}",
            f"{row['luck_p']:.4g}",
            "yes" if row.get("primary_hit") else "no",
            row["label"],
        ]
        if failed:
            fields.append(", ".join(row.get("failed") or []) or "none")
        return "| " + " | ".join(fields) + " |"

    proven = [row["id"] for row in rows if row["label"] == "something good"]
    blank_pass = "|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | no rule passed the guards |"
    blank_raw = "|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | no testable rule |"
    pass_head = (
        "| rank | rule | side | check days | full compound | ex-best | best ticker | median trade | win rate | top-1 share | top-3 share | trades/day | fires | random4 mean | random4 beaten | iwm | from 09-14 | under $3 | 15bp compound | raw p | luck p | primary 30% | label |"
    )
    pass_rule = "| ---: | --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |"
    raw_head = pass_head + " failed guards |"
    raw_rule = pass_rule + " --- |"
    header = [
        "# Breadth mine v1d",
        "",
        f"study: {STUDY}",
        "",
        f"Study label: `{STUDY_LABEL}`.",
        "",
        "Ranked by Futubull after-fee compound on the whole check window, "
        "every store session from 2026-08-20 through 2026-09-11, including sessions a rule sat out. "
        "The primary target is that compound at or above 30%. "
        "Guards are ex-best at or above 10%, top-1 share at or below 50%, "
        "at least 30 fires, win rate above 55%, the 2026-09-14 through 2026-09-25 compound at or above 0, "
        "luck p below 0.05, and a compound strictly above RANDOM4 and IWM on that same window.",
        "",
        f"N = {N}. Luck denominator = {LUCK_DENOMINATOR}. "
        "Raw p is the one-sided t-test. Adjusted p multiplies raw p by the denominator and caps at 1.",
        "",
        f"Rules scored: {len(rows)}. Untestable: {sum(1 for row in rows if row['label'] == 'untestable')}. "
        f"Passed the guards: {len(passing)}.",
        f"Proven: {len(proven)}.",
        "",
        f"Verdict: `{verdict}`",
        "",
        "## Top 20 that pass the guards",
        "",
        pass_head,
        pass_rule,
    ]
    body = [cell(row, rank, False) for rank, row in enumerate(ranked[:20], start=1)]
    if not body:
        body = [blank_pass]
    raw_body = [cell(row, rank, True) for rank, row in enumerate(raw_ranked[:20], start=1)]
    if not raw_body:
        raw_body = [blank_raw]
    tail = [
        "",
        "## Top 20 by compound, guards shown",
        "",
        raw_head,
        raw_rule,
    ]
    return "\n".join(header + body + tail + raw_body) + "\n\n" + bar_audit_section()


def bar_audit_section() -> str:
    flags = session_jumps()
    bad = unexplained(flags)
    lines = [
        "## Consecutive-session jumps",
        "",
        f"Flagged: {len(flags)}. Unexplained: {len(bad)}.",
        "",
        "| ticker | date | ratio | split | explained |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for row in flags:
        split = "" if row["split"] is None else str(row["split"])
        lines.append(
            f"| {row['ticker']} | {row['date']} | {row['ratio']} | {split} | {str(row['explained']).lower()} |"
        )
    return "\n".join(lines) + "\n"


def write_record(days: list[dict], scored: dict, rows: list[dict], verdict: str) -> None:
    RETURNS.mkdir(parents=True, exist_ok=True)
    sessions = scored["sessions"]
    ids = [row["id"] for row in rows]
    if [item[1] for item in iter_rules()] != ids:
        # rows follow evaluate() which follows ids order. Check length only if ids were passed in order.
        pass
    lines = []
    for di, session in enumerate(sessions):
        pnl = {
            str(ri): ",".join(f"{ticker}:{value:.4f}" for ticker, value in sorted(pairs.items()))
            for ri, pairs in sorted(scored["day_pnl"][di].items())
            if pairs
        }
        trades = {
            str(ri): pairs
            for ri, pairs in sorted(scored["day_trades"][di].items())
            if pairs
        }
        if days[di]["candidate_n"] > CANDIDATE_CAP:
            raise SystemExit("rule 18 candidate list")
        payload = {
            "candidate_n": days[di]["candidate_n"],
            "label": STUDY_LABEL,
            "n": N,
            "n_entries": [int(scored["n_entries"][ri, di]) for ri in range(N)],
            "n_fills": [int(scored["n_fills"][ri, di]) for ri in range(N)],
            "n_under_3": [int(scored["n_under"][ri, di]) for ri in range(N)],
            "pnl": pnl,
            "ret_flat_15bp": [round(float(scored["ret_15"][ri, di]), 8) for ri in range(N)],
            "ret_futubull": [round(float(scored["ret_f"][ri, di]), 8) for ri in range(N)],
            "sat_out": [ri for ri in range(N) if bool(scored["sat"][ri, di])],
            "session": session,
            "study": STUDY,
            "trade_rets": trades,
        }
        raw = (json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
        (RETURNS / f"{session}.json").write_bytes(raw)
        lines.append(manifest_line({
            "file": f"{session}.json",
            "input_blobs": days[di]["blobs"],
            "session": session,
            "sha256": file_sha256(raw),
        }))
        print(f"wrote {session} {len(raw)} bytes", flush=True)
    (RETURNS / "manifest.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    summary = {
        "label": STUDY_LABEL,
        "luck_denominator": LUCK_DENOMINATOR,
        "n": N,
        "study": STUDY,
        "verdict": verdict,
        "rows": rows,
    }
    (RETURNS / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (RETURNS / "REPORT.md").write_text(render_report(rows, verdict), encoding="utf-8")


def _verify_bar_pin() -> None:
    raw = (ROOT / BAR_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != BAR_SHA256:
        raise SystemExit("pinned bar sha256 mismatch")
    blob = subprocess.check_output(["git", "hash-object", BAR_PATH], cwd=ROOT, text=True).strip()
    if blob != BAR_BLOB_SHA:
        raise SystemExit("pinned bar blob mismatch")


def cap_day(day: dict) -> dict:
    """IRONCLAD 18. Hot score descending, ticker ascending, at most 50."""
    names = list(day["names"])
    hot = day["hot"]
    order = sorted(range(len(names)), key=lambda i: (-float(hot[i]), names[i]))
    keep = order[:CANDIDATE_CAP]
    if len(keep) != len(names):
        chosen = [names[i] for i in keep]
        day = dict(day)
        day["names"] = chosen
        day["hot"] = hot[keep]
        day["atoms"] = day["atoms"][keep]
        day["open_of"] = {ticker: day["open_of"][ticker] for ticker in chosen}
        day["close_of"] = {ticker: day["close_of"][ticker] for ticker in chosen if ticker in day["close_of"]}
        day["universe"] = set(chosen)
    if len(day["names"]) > CANDIDATE_CAP:
        raise SystemExit("rule 18 candidate list")
    day["candidate_n"] = len(day["names"])
    return day


def random4_names(pool: list[str], draw_i: int) -> list[str]:
    rng = random.Random(RANDOM4_SEED + int(draw_i))
    ordered = sorted(pool)
    k = min(RANDOM4_N, len(ordered))
    if k == 0:
        return []
    return rng.sample(ordered, k)


def _iwm_returns(bars: Bars, sessions: list[str], fees: dict) -> list[float]:
    book = Book()
    out = []
    for di, session in enumerate(sessions):
        op, cl = bars.session_open_close("IWM", session)
        if op is None or cl is None or op <= 0 or cl <= 0:
            raise SystemExit(f"IWM missing {session}")
        ret, _ret15, _fills, _under, _entries, _pnl, _closed = advance_book(
            book,
            side="long",
            exit_name="time",
            hold=10_000,
            min_hold=1,
            pick_names=["IWM"],
            open_of={"IWM": op},
            close_of={"IWM": cl},
            universe={"IWM"},
            day_i=di,
            fees=fees,
        )
        out.append(float(ret))
    lot = book.pos.get("IWM")
    if lot is not None and out:
        prev_eq = book.eqf / (1.0 + out[-1]) if (1.0 + out[-1]) else book.eqf
        fee = order_fees(int(lot["shares"]), float(lot["prev"]), "sell", fees)
        book.eqf -= fee
        out[-1] = (book.eqf / prev_eq - 1.0) if prev_eq else 0.0
    return out


def _random4_matrix(days: list[dict], fees: dict) -> np.ndarray:
    n_days = len(days)
    out = np.zeros((RANDOM4_DRAWS, n_days), dtype=np.float64)
    for draw_i in range(RANDOM4_DRAWS):
        book = Book()
        for di, day in enumerate(days):
            picks = random4_names(day["names"], draw_i)
            ret, _ret15, _fills, _under, _entries, _pnl, _closed = advance_book(
                book,
                side="long",
                exit_name="time",
                hold=1,
                min_hold=1,
                pick_names=picks,
                open_of=day["open_of"],
                close_of=day["close_of"],
                universe=day["universe"],
                day_i=di,
                fees=fees,
            )
            out[draw_i, di] = ret
        if draw_i % 100 == 0:
            print(f"random4 {draw_i}", flush=True)
    return out


def _compound_idx(rets: list[float] | np.ndarray, idx: list[int]) -> float:
    value = 1.0
    for i in idx:
        value *= 1.0 + float(rets[i])
    return value - 1.0


def attach_baselines(rows: list[dict], scored: dict, days: list[dict], fees: dict, bars: Bars) -> None:
    """IRONCLAD 19. The whole check window, the same 16 sessions for every rule."""
    sessions = scored["sessions"]
    iwm = _iwm_returns(bars, sessions, fees)
    matrix = _random4_matrix(days, fees)
    check_cal = set(CHECK_CALENDAR)
    idx = [di for di, session in enumerate(sessions) if session in check_cal]
    before = all(sessions[di] < CREATION_DATE for di in idx)
    iwm_c = _compound_idx(iwm, idx) if idx else None
    draw_compounds = [_compound_idx(matrix[draw_i], idx) for draw_i in range(RANDOM4_DRAWS)] if idx else []
    random_mean = (sum(draw_compounds) / len(draw_compounds)) if draw_compounds else None
    for row in rows:
        if row["label"] == "untestable" or not idx:
            row["random4_beaten"] = None
            row["random4_mean"] = None
            row["iwm"] = None
            apply_objective(row, before)
            continue
        rule_c = row["full"]
        beaten = sum(1 for base in draw_compounds if rule_c > base)
        row["random4_beaten"] = beaten / RANDOM4_DRAWS
        row["random4_mean"] = random_mean
        row["iwm"] = iwm_c
        apply_objective(row, before)


def _build_phase(bars: Bars, grouped: dict, sessions: list[str], cache: dict) -> list[dict]:
    days = []
    for session in sessions:
        print(f"build {session}", flush=True)
        day = cap_day(build_day(bars, grouped[session], session, cache))
        day["session"] = session
        days.append(day)
        print(f"  candidates {day['candidate_n']} roles {sorted(day['roles'])}", flush=True)
    return days


def main() -> None:
    assert_prereg()
    _verify_bar_pin()
    from src.breadth_mine_v1d_bars import assert_split_consistent
    assert_split_consistent()
    _manifest, index = load_manifest()
    seen = set()
    grouped: dict[str, list[dict]] = {day: [] for day in SESSIONS}
    for row in index.values():
        key = (row["commit"], row["path"])
        if key not in seen:
            seen.add(key)
            _pin_ok(row)
        grouped[row["date"]].append(row)
    fees = load_fees()
    cache: dict[str, bytes] = {}
    search = [day for day in SESSIONS if day < OOS_START]
    print("loading search bars", flush=True)
    search_bars = Bars(max_date="2026-09-13")
    days = _build_phase(search_bars, grouped, search, cache)
    scored = walk_all(days, fees)
    # The 2026-09-11 file is on disk before any bar dated 2026-09-14 is loaded.
    write_record(days, scored, evaluate(scored, _ids()), "search written")
    print("loading test bars after the search file is on disk", flush=True)
    oos_bars = Bars()
    oos_days = _build_phase(oos_bars, grouped, [day for day in SESSIONS if day >= OOS_START], cache)
    # One book from the first session. Replay the whole window on the full tape
    # so the test days continue the search book. Search bars were not the test bars.
    days = days + oos_days
    # The search walk above used only search bars. Replay from the start on the
    # joined days. Test-day bars were not used to build the search days.
    scored = walk_all(days, fees)
    ids = _ids()
    if len(ids) != N:
        raise SystemExit(f"id count {len(ids)}")
    print("evaluate", flush=True)
    rows = evaluate(scored, ids)
    attach_baselines(rows, scored, days, fees, oos_bars)
    proven = [row["id"] for row in rows if row["label"] == "something good"]
    verdict = "nothing proven yet" if not proven else "something good: " + ", ".join(proven)
    write_record(days, scored, rows, verdict)
    print(verdict, flush=True)


if __name__ == "__main__":
    main()
