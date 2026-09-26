"""Price tapes and the cash book. The preregistration is not edited."""
from __future__ import annotations

import hashlib
import io
import random
import subprocess
import sys
from bisect import bisect_left
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.factor_mine_score_search_v3.protocol import (  # noqa: E402
    BY_ID,
    CLEAN_BLOB,
    CLEAN_SHA256,
    RANDOM4_DRAWS,
    RANDOM4_N,
    RANDOM4_SEED,
    SESSIONS,
    TUNE,
    YAHOO_PATH,
    YAHOO_SHA256,
    hard_red,
    simulate,
    top_names,
    unexplained_jumps,
    window_stats,
)
from src.lever_search_score import fee_15  # noqa: E402
from src.ohlc_ripper import from_bars, too_extended  # noqa: E402
from src.paper_trade import load_fees, order_fees  # noqa: E402

PRIOR_BARS = 60
GAINERS_N = 25
MOVERS_N = 20
HOT_N = 30


class Tape:
    def __init__(self, frame) -> None:
        self.rows: dict[str, dict] = {}
        for ticker, group in frame.groupby("ticker", sort=False):
            group = group.sort_values("date")
            self.rows[str(ticker)] = {
                "close": group["close"].to_numpy(dtype=float),
                "date": group["date"].tolist(),
                "high": group["high"].to_numpy(dtype=float),
                "low": group["low"].to_numpy(dtype=float),
                "open": group["open"].to_numpy(dtype=float),
                "volume": group["volume"].to_numpy(dtype=float),
            }

    def _idx(self, ticker: str, session: str) -> int | None:
        tape = self.rows.get(ticker)
        if not tape:
            return None
        idx = bisect_left(tape["date"], session)
        if idx >= len(tape["date"]) or tape["date"][idx] != session:
            return None
        return idx

    def price(self, ticker: str, session: str, which: str) -> float | None:
        tape = self.rows.get(ticker)
        idx = self._idx(ticker, session)
        if tape is None or idx is None:
            return None
        value = float(tape[which][idx])
        if value != value or value <= 0:
            return None
        return value

    def prior(self, ticker: str, session: str) -> list[dict]:
        tape = self.rows.get(ticker)
        if not tape:
            return []
        end = bisect_left(tape["date"], session)
        start = max(0, end - PRIOR_BARS)
        out = []
        for idx in range(start, end):
            out.append({
                "close": float(tape["close"][idx]),
                "date": tape["date"][idx],
                "high": float(tape["high"][idx]),
                "low": float(tape["low"][idx]),
                "open": float(tape["open"][idx]),
                "volume": float(tape["volume"][idx]),
            })
        return out

    def calendar(self) -> list[str]:
        if not hasattr(self, "_calendar"):
            dates = set()
            for tape in self.rows.values():
                dates.update(tape["date"])
            self._calendar = sorted(dates)
        return self._calendar

    def prior_date(self, session: str) -> str | None:
        dates = self.calendar()
        idx = bisect_left(dates, session)
        if idx == 0:
            return None
        return dates[idx - 1]


def _frame(kind: str, *, through: str | None):
    import pandas as pd

    if kind == "clean":
        raw = subprocess.check_output(["git", "cat-file", "blob", CLEAN_BLOB], cwd=ROOT)
        if hashlib.sha256(raw).hexdigest() != CLEAN_SHA256:
            raise SystemExit("clean bar sha")
        frame = pd.read_parquet(io.BytesIO(raw))
    elif kind == "yahoo":
        path = ROOT / YAHOO_PATH
        if hashlib.sha256(path.read_bytes()).hexdigest() != YAHOO_SHA256:
            raise SystemExit("yahoo bar sha")
        frame = pd.read_parquet(path, columns=["date", "ticker", "open", "high", "low", "close", "volume"])
    else:
        raise SystemExit(kind)
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["ticker"] = frame["ticker"].astype(str)
    if through is not None:
        if frame["date"].max() > through and kind == "clean" and through < "2026-09-14":
            pass
        frame = frame.loc[frame["date"] <= through]
        if not frame.empty and frame["date"].max() > through:
            raise SystemExit("bar filter failed")
    return frame


def load_tape(kind: str, *, through: str | None) -> Tape:
    return Tape(_frame(kind, through=through))


def _prior_return(tape: Tape, ticker: str, prior: str) -> float | None:
    idx = tape._idx(ticker, prior)
    if idx is None or idx < 1:
        return None
    series = tape.rows[ticker]
    prev = float(series["close"][idx - 1])
    close = float(series["close"][idx])
    if prev <= 0 or close <= 0 or prev != prev or close != close:
        return None
    return 100.0 * (close / prev - 1.0)


_PRICE_ONLY: dict[tuple[int, str], list[str]] = {}


def price_only_names(tape: Tape, session: str) -> list[str]:
    """Prior-day gainers, movers, and the hot list. Overnight is not in the bars."""
    key = (id(tape), session)
    if key in _PRICE_ONLY:
        return list(_PRICE_ONLY[key])
    prior = tape.prior_date(session)
    if prior is None:
        return []
    gainers = []
    movers = []
    hot = []
    for ticker in tape.rows:
        if tape._idx(ticker, prior) is None:
            continue
        ret = _prior_return(tape, ticker, prior)
        if ret is not None:
            if ret > 0:
                gainers.append((ret, ticker))
            movers.append((abs(ret), ticker))
        feat = from_bars(tape.prior(ticker, session))
        if feat.get("ok") and not too_extended(feat):
            hot.append((float(feat.get("hot_score") or 0.0), ticker))
    gainers.sort(key=lambda item: (-item[0], item[1]))
    movers.sort(key=lambda item: (-item[0], item[1]))
    hot.sort(key=lambda item: (-item[0], item[1]))
    names = []
    seen = set()
    for _score, ticker in gainers[:GAINERS_N] + movers[:MOVERS_N] + hot[:HOT_N]:
        if ticker in seen:
            continue
        seen.add(ticker)
        names.append(ticker)
    _PRICE_ONLY[key] = names
    return list(names)


def _variant_block(block: dict, variant: str) -> tuple[str, list[str] | None, float | None, dict, dict | None]:
    if variant == "primary":
        source = block["primary_source"]
        names = list(block["stock_book_primary"]["names"]) if source == "stock_book" else None
        return source, names, block.get("s_primary"), block["earn_primary"], block.get("cam")
    if variant != "strict":
        raise SystemExit(variant)
    source = block["strict_source"]
    names = list(block["stock_book_strict"]["names"]) if source == "stock_book" else None
    return source, names, block.get("s_strict"), block["earn_strict"], None


def prepare_days(inputs: dict, tape: Tape, sessions: tuple[str, ...], variant: str = "primary") -> list[dict]:
    days = []
    for session in sessions:
        block = inputs["dates"][session]
        source, names, score, earn, cam = _variant_block(block, variant)
        if source == "price_only":
            names = price_only_names(tape, session)
        cam_rows = {}
        if cam:
            cam_rows = cam.get("rows") or {}
        pol = earn.get("pol") if earn.get("on") else None
        rows = []
        for index, ticker in enumerate(names or []):
            feat = from_bars(tape.prior(ticker, session))
            ok = bool(feat.get("ok"))
            pair = cam_rows.get(ticker) if cam else None
            if pair is None:
                cond_good = cond_bad = None
            else:
                cond_good, cond_bad = pair
            if pol is None:
                e_pol = None
            else:
                e_pol = pol.get(ticker, "missing")
            rows.append({
                "close": tape.price(ticker, session, "close"),
                "cond_bad": cond_bad,
                "cond_good": cond_good,
                "e_pol": e_pol,
                "flow": (bool(feat.get("flow_in")) if ok else None),
                "macd": (float(feat["macd_hist"]) if ok and feat.get("macd_hist") is not None else None),
                "open": tape.price(ticker, session, "open"),
                "rsi": (float(feat["rsi"]) if ok and feat.get("rsi") is not None else None),
                "src_rank": index + 1,
                "ticker": ticker,
                "yday": (float(feat["ret_1"]) if ok else None),
            })
        days.append({
            "hard_red": hard_red(score),
            "rows": rows,
            "s": score,
            "session": session,
            "source": source,
        })
    return days


def fee_fns():
    fees = load_fees()

    def futu(shares: int, price: float, side: str) -> float:
        return order_fees(shares, price, side, fees)

    def flat(shares: int, price: float, side: str) -> float:
        return fee_15(shares, price)

    return futu, flat


def run_formula(days: list[dict], formula: dict, top_n: int, fee_fn) -> dict:
    picks = {}
    for day in days:
        if day["hard_red"]:
            picks[day["session"]] = []
        else:
            picks[day["session"]] = top_names(day["rows"], formula, top_n)
    return simulate(days, picks, fee_fn)


def run_random4(days: list[dict], fee_fn) -> list[dict]:
    books = []
    for draw_i in range(RANDOM4_DRAWS):
        rng = random.Random(RANDOM4_SEED + draw_i)
        picks = {}
        for day in days:
            if day["hard_red"]:
                picks[day["session"]] = []
                continue
            pool = sorted(row["ticker"] for row in day["rows"] if row.get("open") and float(row["open"]) > 0)
            k = min(RANDOM4_N, len(pool))
            picks[day["session"]] = rng.sample(pool, k) if k else []
        books.append(simulate(days, picks, fee_fn))
    return books


def run_iwm(tape: Tape, sessions: tuple[str, ...], fee_fn) -> dict:
    from research.factor_mine_score_search_v3.protocol import CAPITAL

    cash = CAPITAL
    shares = 0
    prev = CAPITAL
    rets = {}
    missing = []
    for session in sessions:
        opx = tape.price("IWM", session, "open")
        cpx = tape.price("IWM", session, "close")
        if shares == 0 and opx is not None:
            trial = int(cash // opx)
            while trial >= 1:
                fee = float(fee_fn(trial, opx, "buy"))
                if trial * opx + fee <= cash + 1e-6:
                    cash -= trial * opx + fee
                    shares = trial
                    break
                trial -= 1
        if shares == 0 or cpx is None:
            missing.append(session)
            continue
        equity = cash + shares * cpx
        rets[session] = equity / prev - 1.0 if prev else 0.0
        prev = equity
    return {"missing": missing, "returns": rets, "shares": shares}


def formula_rows(days: list[dict], fee_fn, sessions: tuple[str, ...], window: tuple[str, ...], ids: list[str]) -> dict:
    out = {}
    for fid in ids:
        formula = BY_ID[fid]
        by_x = {}
        for top_n in (2, 4, 8):
            book = run_formula(days, formula, top_n, fee_fn)
            by_x[str(top_n)] = window_stats(book, list(sessions), window)
        out[fid] = by_x
    return out


def assert_tune_bars(kind: str = "clean") -> None:
    frame = _frame(kind, through=TUNE[-1])
    if frame.empty:
        raise SystemExit("no tune bars")
    if frame["date"].max() > TUNE[-1]:
        raise SystemExit("tune bars include a forward session")
    if "2026-09-14" in set(frame["date"]):
        raise SystemExit("forward session in the tune tape")


def load_splits() -> dict[tuple[str, str], float]:
    import json
    payload = json.loads((ROOT / "research/breadth_rank_v1c/bars/splits.json").read_text(encoding="utf-8"))
    rows = payload["splits"] if isinstance(payload, dict) else payload
    return {(str(row["ticker"]), str(row["date"])): float(row["split"]) for row in rows}


def board_tickers(inputs: dict, tape: Tape, through: str) -> set[str]:
    """Names the primary or strict book can hold, plus IWM. Price-only uses this tape."""
    names = {"IWM"}
    for session, block in inputs["dates"].items():
        if session > through:
            continue
        for variant in ("primary", "strict"):
            source, listed, _score, _earn, _cam = _variant_block(block, variant)
            if source == "stock_book":
                names.update(listed or [])
            else:
                names.update(price_only_names(tape, session))
    return names


def jump_flags(tape: Tape, tickers: set[str], through: str, dropped: set[str]) -> list[dict]:
    """Unexplained 3x legs on kept names. Names in ``dropped`` are the known list."""
    bars: dict[str, list[dict]] = {}
    for ticker in tickers:
        if ticker in dropped:
            continue
        series = tape.rows.get(ticker)
        if not series:
            continue
        rows = []
        for idx, date in enumerate(series["date"]):
            if date > through:
                break
            opx = float(series["open"][idx])
            cpx = float(series["close"][idx])
            if opx != opx or cpx != cpx:
                continue
            rows.append({"close": cpx, "date": date, "open": opx})
        if rows:
            bars[ticker] = rows
    return unexplained_jumps(bars, load_splits())
