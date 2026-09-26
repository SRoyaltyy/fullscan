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

from research.factor_mine_score_search_v2.protocol import (  # noqa: E402
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
from src.gainer_asof import load_finviz  # noqa: E402
from src.lever_search_score import fee_15  # noqa: E402
from src.ohlc_ripper import from_bars  # noqa: E402
from src.paper_trade import load_fees, order_fees  # noqa: E402

PRIOR_BARS = 60
FV_RSI = "Relative Strength Index (14)"


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


def _num(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text.lower() in {"nan", "none", "—", "-"}:
        return None
    try:
        out = float(text)
    except ValueError:
        return None
    if out != out:
        return None
    return out


def _fv_rsi(dates: set[str]) -> dict[tuple[str, str], float]:
    out = {}
    for session in sorted(dates):
        if not session:
            continue
        frame = load_finviz(session)
        if frame is None or getattr(frame, "empty", True) or "Ticker" not in getattr(frame, "columns", []):
            continue
        if FV_RSI not in frame.columns:
            continue
        for _, row in frame.iterrows():
            ticker = str(row.get("Ticker") or "").strip().upper()
            value = _num(row.get(FV_RSI))
            if ticker and value is not None:
                out[(session, ticker)] = value
    return out


def prepare_days(inputs: dict, tape: Tape, sessions: tuple[str, ...]) -> list[dict]:
    priors = set()
    for session in sessions:
        for row in inputs["dates"][session]["rows"]:
            prior = row.get("prior_date")
            if prior:
                priors.add(prior)
    rsi_map = _fv_rsi(priors)
    days = []
    for session in sessions:
        block = inputs["dates"][session]
        rows = []
        for src in block["rows"]:
            ticker = src["ticker"]
            feat = from_bars(tape.prior(ticker, session))
            ok = bool(feat.get("ok"))
            prior = src.get("prior_date")
            rsi = rsi_map.get((prior, ticker)) if prior else None
            if rsi is None and ok and feat.get("rsi") is not None:
                rsi = float(feat["rsi"])
            rows.append({
                "close": tape.price(ticker, session, "close"),
                "cond_bad": src["cond_bad"],
                "cond_good": src["cond_good"],
                "e_pol": src["e_pol"],
                "flow": (bool(feat.get("flow_in")) if ok else None),
                "macd": (float(feat["macd_hist"]) if ok and feat.get("macd_hist") is not None else None),
                "open": tape.price(ticker, session, "open"),
                "rsi": rsi,
                "src_rank": src["src_rank"],
                "ticker": ticker,
                "yday": (float(feat["ret_1"]) if ok else None),
            })
        days.append({
            "hard_red": hard_red(block.get("s")),
            "rows": rows,
            "s": block.get("s"),
            "session": session,
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
    from research.factor_mine_score_search_v2.protocol import CAPITAL

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


def board_tickers(inputs: dict, through: str) -> set[str]:
    names = {"IWM"}
    for session, block in inputs["dates"].items():
        if session > through:
            continue
        for row in block["rows"]:
            names.add(row["ticker"])
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
