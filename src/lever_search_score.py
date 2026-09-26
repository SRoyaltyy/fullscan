"""Initial Group 3 cash-book score.

Reads the pinned proof blobs and the pinned Yahoo bar file. Does not
rewrite the preregistration or any locked ledger. Price features use
bars dated strictly before the session. The session open is the fill.
The session close is the mark only.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import subprocess
from bisect import bisect_left
from pathlib import Path

from src.candle_factor import _from_bars as candle_from_bars
from src.candle_factor import capture as candle_capture
from src.factor_mine import matches, pick_day, should_exit
from src.finviz_events import asof_snapshot, events_from_export_fields
from src.lever_search_append import manifest_line
from src.lever_search_bars import (
    BAR_BLOB_SHA,
    BAR_PATH,
    BAR_SHA256,
    feature_bars,
    fill_open,
)
from src.lever_search_proof import (
    CHECK_CALENDAR,
    HEADLINE_INPUTS,
    PROOF_BLOB_SHA,
    PROOF_COMMIT,
    PROOF_PATH,
    RUN_WINDOW,
    _usable,
    build_group3_recipes,
    check_days,
    input_day_is_usable,
    required_roles,
    search_days,
)
from src.ohlc_ripper import from_bars as ohlc_from_bars
from src.ohlc_ripper import too_extended
from src.paper_trade import load_fees, order_fees

ROOT = Path(__file__).resolve().parents[1]
RETURNS = ROOT / "research" / "lever_search" / "returns" / "factor_mine_seq"
CAPITAL = 10_000.0
BORROW_ANNUAL = 0.01
BORROW_DAY = BORROW_ANNUAL / 252.0
BP_SIDE = 0.000075
MIN_AVG_VOL = 500_000.0
RELVOL_GOOD = 1.5
RELVOL_DEAD = 0.7
LUCK_N = 9280
DESIGNED_AFTER_START = "2026-09-14"
DESIGNED_AFTER_END = "2026-09-25"
PRICE_SOURCES = frozenset({
    "probable", "yday_gainer", "yday_mover", "ohlc_hot", "overnight",
})


def fee_15(shares: int, price: float) -> float:
    """Flat 7.5 bp of notional. One side of the 15 bp round trip."""
    if shares <= 0 or price <= 0:
        return 0.0
    return round(shares * price * BP_SIDE, 4)


def compound(returns: list[float]) -> float:
    acc = 1.0
    for value in returns:
        acc *= 1.0 + float(value)
    return acc - 1.0


def _log_gamma(value: float) -> float:
    return math.lgamma(value)


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta function."""
    max_iter = 200
    eps = 3e-12
    am = 1.0
    a0 = 1.0
    b0 = 1.0
    a1 = 1.0
    b1 = 1.0 - (a + b) * x / (a + 1.0)
    if abs(b1) < 1e-30:
        b1 = 1e-30
    frac = a1 / b1
    for m in range(1, max_iter + 1):
        em = float(m)
        tem = em + em
        d = em * (b - em) * x / ((a + tem - 1.0) * (a + tem))
        ap = a1 + d * a0
        bp = b1 + d * b0
        d = -(a + em) * (a + b + em) * x / ((a + tem) * (a + tem + 1.0))
        app = ap + d * a1
        bpp = bp + d * b1
        a0, b0, a1, b1 = ap, bp, app, bpp
        if abs(bpp) < 1e-30:
            bpp = 1e-30
        frac_next = app / bpp
        if abs(frac_next - frac) < eps * abs(frac_next):
            return frac_next
        frac = frac_next
    return frac


def regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    log_beta = _log_gamma(a) + _log_gamma(b) - _log_gamma(a + b)
    front = math.exp(a * math.log(x) + b * math.log(1.0 - x) - log_beta) / a
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x)
    return 1.0 - math.exp(
        b * math.log(1.0 - x) + a * math.log(x) - log_beta
    ) / b * _betacf(b, a, 1.0 - x)


def student_t_sf(stat: float, df: int) -> float:
    """One-sided P(T >= stat) for a Student t with df degrees of freedom."""
    if df < 1:
        return 1.0
    if stat == 0.0:
        return 0.5
    x = df / (df + stat * stat)
    prob = 0.5 * regularized_incomplete_beta(df / 2.0, 0.5, x)
    if stat > 0:
        return prob
    return 1.0 - prob


def luck_p(returns: list[float], n_tries: int = LUCK_N) -> float:
    """Bonferroni one-sided t-test that the mean check-day return exceeds 0.

    The denominator is tonight's luck-test N. Fewer than two check days
    cannot reject the null.
    """
    clean = [float(value) for value in returns if math.isfinite(value)]
    n = len(clean)
    if n < 2:
        return 1.0
    mean = sum(clean) / n
    var = sum((value - mean) ** 2 for value in clean) / (n - 1)
    if var <= 0.0:
        raw = 0.0 if mean > 0.0 else 1.0
    else:
        stat = mean / math.sqrt(var / n)
        raw = student_t_sf(stat, n - 1)
    return min(1.0, raw * n_tries)


def _git_blob(sha: str) -> bytes:
    proc = subprocess.run(
        ["git", "cat-file", "-p", sha],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return proc.stdout


def load_proof_rows() -> dict[tuple[str, str], list[dict]]:
    raw = subprocess.check_output(
        ["git", "show", f"{PROOF_COMMIT}:{PROOF_PATH}"],
        cwd=ROOT,
    )
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in csv.DictReader(io.StringIO(raw.decode("utf-8"))):
        grouped.setdefault((row["date"], row["input"]), []).append(row)
    return grouped


def role_usable(proof: dict, day: str, role: str) -> bool:
    """Same usable-day rule as the frozen date table, checked on the pin."""
    if role == "ab":
        return role_usable(proof, day, "ab_checklist") and role_usable(proof, day, "ab_enriched")
    rows = proof.get((day, role)) or []
    return input_day_is_usable(rows, headline=role in HEADLINE_INPUTS)


def _first_blob(proof: dict, day: str, role: str) -> dict | None:
    rows = [
        row for row in (proof.get((day, role)) or [])
        if row.get("proven") == "yes" and row.get("blob_sha")
        and not str(row.get("path") or "").endswith("/")
    ]
    if not rows:
        return None
    return rows[0]


class Store:
    """Pinned split-adjusted bars. Feature reads go through feature_bars."""

    def __init__(self) -> None:
        import pandas as pd

        frame = pd.read_parquet(ROOT / BAR_PATH, columns=["date", "ticker", "open", "high", "low", "close", "volume"])
        frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
        digest = hashlib.sha256((ROOT / BAR_PATH).read_bytes()).hexdigest()
        if digest != BAR_SHA256:
            raise SystemExit(f"ohlc.parquet sha256 {digest} != pin {BAR_SHA256}")
        self.dates = tuple(sorted(frame["date"].unique()))
        self.tapes: dict[str, dict] = {}
        for ticker, group in frame.groupby("ticker", sort=False):
            group = group.sort_values("date")
            self.tapes[str(ticker)] = {
                "date": group["date"].tolist(),
                "open": group["open"].to_numpy(dtype=float),
                "high": group["high"].to_numpy(dtype=float),
                "low": group["low"].to_numpy(dtype=float),
                "close": group["close"].to_numpy(dtype=float),
                "volume": group["volume"].to_numpy(dtype=float),
            }
        self._feat: dict[tuple[str, str], dict] = {}

    def prior_index(self, ticker: str, session: str) -> int:
        tape = self.tapes.get(ticker)
        if not tape:
            return -1
        return bisect_left(tape["date"], session) - 1

    def session_open(self, ticker: str, session: str) -> float | None:
        tape = self.tapes.get(ticker)
        if not tape:
            return None
        dates = tape["date"]
        idx = bisect_left(dates, session)
        if idx >= len(dates) or dates[idx] != session:
            return None
        return fill_open({"date": session, "open": float(tape["open"][idx])}, trades_at_open=True)

    def session_close(self, ticker: str, session: str) -> float | None:
        """Mark only. Not a feature and not a fill."""
        tape = self.tapes.get(ticker)
        if not tape:
            return None
        dates = tape["date"]
        idx = bisect_left(dates, session)
        if idx >= len(dates) or dates[idx] != session:
            return None
        close = float(tape["close"][idx])
        if not math.isfinite(close) or close <= 0:
            return None
        return close

    def feature(self, ticker: str, session: str) -> dict:
        key = (ticker, session)
        hit = self._feat.get(key)
        if hit is not None:
            return hit
        tape = self.tapes.get(ticker)
        empty = {"ok": False}
        if not tape:
            self._feat[key] = empty
            return empty
        end = bisect_left(tape["date"], session)
        start = max(0, end - 60)
        bars = []
        for idx in range(start, end):
            bars.append({
                "date": tape["date"][idx],
                "open": float(tape["open"][idx]),
                "high": float(tape["high"][idx]),
                "low": float(tape["low"][idx]),
                "close": float(tape["close"][idx]),
                "volume": float(tape["volume"][idx]),
            })
        bars = feature_bars(bars, session)
        oh = ohlc_from_bars(bars)
        cd = candle_from_bars(bars)
        oh["candle_score"] = cd.get("score")
        oh["candle_capture"] = bool(candle_capture(cd))
        oh["last_green"] = bool(oh.get("last_green") or cd.get("last_green"))
        oh["last_red"] = bool(oh.get("last_red") or cd.get("last_red"))
        self._feat[key] = oh
        return oh


def _vol_tone(feat: dict) -> str:
    if not feat.get("ok"):
        return "missing"
    rvol = feat.get("rvol")
    if rvol is None:
        return "missing"
    if float(rvol) >= RELVOL_GOOD:
        return "good"
    if float(rvol) < RELVOL_DEAD:
        return "bad"
    return "neutral"


def _polarity(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "missing"
    if value >= 0.05:
        return "good"
    if value <= -0.05:
        return "bad"
    return "neutral"


def _ab_map(blob: bytes) -> dict[str, float]:
    text = blob.decode("utf-8", errors="replace")
    out: dict[str, float] = {}
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


def _news_map(blob: bytes) -> dict[str, str]:
    data = json.loads(blob)
    items = data.get("ticker_actions") or []
    out: dict[str, str] = {}
    if isinstance(items, dict):
        seq = []
        for ticker, row in items.items():
            if isinstance(row, dict):
                seq.append({"ticker": ticker, **row})
        items = seq
    for row in items:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
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
        out[ticker] = _polarity(signed)
    return out


def _export_index(blob: bytes) -> dict[str, list[dict]]:
    import pandas as pd

    frame = pd.read_csv(io.BytesIO(blob), low_memory=False)
    if "Ticker" not in frame.columns:
        return {}
    earn = "Earnings Date" if "Earnings Date" in frame.columns else None
    div = "Dividend Ex Date" if "Dividend Ex Date" in frame.columns else None
    out: dict[str, list[dict]] = {}
    for rec in frame.to_dict("records"):
        ticker = str(rec.get("Ticker") or "").strip().upper()
        if not ticker:
            continue
        rows = events_from_export_fields(
            ticker,
            rec.get(earn) if earn else None,
            rec.get(div) if div else None,
        )
        if rows:
            out[ticker] = rows
    return out


def _stock_lists(blob: bytes) -> tuple[list[str], list[str]]:
    obj = json.loads(blob)
    books = obj.get("books") or {}
    ordered: list[str] = []
    seen: set[str] = set()
    flatten: list[str] = []
    day = (books.get("1d") or {}).get("buy") or []
    for row in day:
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        flatten.append(ticker)
        if ticker not in seen:
            seen.add(ticker)
            ordered.append(ticker)
    for horizon in ("3d", "1w", "2w", "1m"):
        for row in (books.get(horizon) or {}).get("buy") or []:
            ticker = str(row.get("ticker") or "").strip().upper()
            if ticker and ticker not in seen:
                seen.add(ticker)
                ordered.append(ticker)
    return ordered, flatten


def _liquid_prior(store: Store, ticker: str, prior: str, session: str) -> tuple[float, dict] | None:
    tape = store.tapes.get(ticker)
    if not tape:
        return None
    idx = bisect_left(tape["date"], prior)
    if idx >= len(tape["date"]) or tape["date"][idx] != prior or idx < 1:
        return None
    if idx < 7:
        return None
    window = tape["volume"][max(0, idx - 19): idx + 1]
    if float(window.mean()) < MIN_AVG_VOL:
        return None
    prev = float(tape["close"][idx - 1])
    last = float(tape["close"][idx])
    if prev <= 0 or last <= 0:
        return None
    feat = store.feature(ticker, session)
    if not feat.get("ok"):
        return None
    return ((last / prev - 1.0) * 100.0, feat)


def price_lists(store: Store, session: str, calendar: list[str]) -> dict[str, list[str]]:
    """Prior-day gainers, movers, hot list, continuation, and the open gap.

    Gainers, movers, hot, and continuation use bars dated strictly before
    the session. The overnight list ranks the absolute gap of this
    session's open versus the prior close. Every frozen recipe trades at
    the 09:30 open, so that gap is known then. Liquidity is the frozen
    500k-share average-volume floor. Market cap is not on the bar file.
    """
    pos = bisect_left(calendar, session)
    if pos <= 0:
        return {name: [] for name in PRICE_SOURCES}
    prior = calendar[pos - 1]
    ranked: list[tuple[float, str, dict]] = []
    for ticker in store.tapes:
        hit = _liquid_prior(store, ticker, prior, session)
        if hit is None:
            continue
        ret, feat = hit
        ranked.append((ret, ticker, feat))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    gainers = [ticker for _, ticker, _ in ranked[:25]]
    movers = sorted(ranked, key=lambda item: (-abs(item[0]), item[1]))
    mover_names = [ticker for _, ticker, _ in movers[:20]]
    hot = []
    for _, ticker, feat in sorted(ranked, key=lambda item: (-(float(item[2].get("hot_score") or 0.0)), item[1])):
        if too_extended(feat):
            continue
        hot.append(ticker)
        if len(hot) >= 30:
            break
    probable: list[str] = []
    for _, ticker, feat in ranked[:60]:
        if too_extended(feat):
            continue
        if float(feat.get("ret_5") or 0.0) > 10.0:
            continue
        probable.append(ticker)
        if len(probable) >= 8:
            break
    gaps: list[tuple[float, str]] = []
    for ret, ticker, _feat in ranked:
        tape = store.tapes[ticker]
        idx = bisect_left(tape["date"], prior)
        prev_close = float(tape["close"][idx])
        opened = store.session_open(ticker, session)
        if opened is None or prev_close <= 0:
            continue
        gaps.append((abs(opened / prev_close - 1.0), ticker))
    gaps.sort(key=lambda item: (-item[0], item[1]))
    overnight = [ticker for _, ticker in gaps[:20]]
    return {
        "yday_gainer": gainers,
        "yday_mover": mover_names,
        "ohlc_hot": hot,
        "probable": probable,
        "overnight": overnight,
    }


def _row(store: Store, session: str, ticker: str, sources: list[str], src_rank: int,
         news: dict[str, str], ab: dict[str, str], export: dict[str, list[dict]] | None,
         spy_ret5: float | None) -> dict:
    feat = store.feature(ticker, session)
    boxes = {name: "missing" for name in (
        "join", "sector", "gen", "news", "digest", "judge", "ab", "peer", "heat", "vol", "catal", "buy",
    )}
    boxes["vol"] = _vol_tone(feat)
    if ticker in news:
        boxes["news"] = news[ticker]
    if ticker in ab:
        boxes["ab"] = ab[ticker]
    snap = asof_snapshot((export or {}).get(ticker) or [], session) if export is not None else {}
    ret5 = feat.get("ret_5")
    rs = None
    if spy_ret5 is not None and ret5 is not None:
        rs = float(ret5) - spy_ret5
    return {
        "date": session,
        "ticker": ticker,
        "sources": sources,
        "src_rank": src_rank,
        "boxes": boxes,
        "blue": False,
        "alarm": False,
        "zero_red": False,
        "cond_good": 0,
        "cond_bad": 0,
        "ohlc_ret_1": feat.get("ret_1"),
        "ohlc_ret_5": ret5,
        "ohlc_ret_10": feat.get("ret_10"),
        "ohlc_rvol": feat.get("rvol"),
        "ohlc_hot_score": feat.get("hot_score"),
        "ohlc_break_10": bool(feat.get("break_10")),
        "last_green": bool(feat.get("last_green")),
        "last_red": bool(feat.get("last_red")),
        "candle_score": feat.get("candle_score") or 0.0,
        "candle_capture": bool(feat.get("candle_capture")),
        "erd_earn_react": bool(snap.get("earn_react")),
        "erd_days_since_E": snap.get("days_since_E"),
        "erd_days_since_R": snap.get("days_since_R"),
        "erd_flag_E": snap.get("flag_E"),
        "erd_flag_R": snap.get("flag_R"),
        "rs_week": rs,
    }


def build_day(store: Store, proof: dict, session: str, calendar: list[str],
              cache: dict) -> tuple[list[dict], list[dict]]:
    """Return candidate rows and the input blobs read for this session."""
    blobs = [{"path": BAR_PATH, "blob_sha": BAR_BLOB_SHA},
             {"path": PROOF_PATH, "blob_sha": PROOF_BLOB_SHA}]
    lists = price_lists(store, session, calendar)
    stock_names: list[str] = []
    flatten: list[str] = []
    if role_usable(proof, session, "stock_book"):
        meta = _first_blob(proof, session, "stock_book")
        if meta:
            stock_names, flatten = _stock_lists(_git_blob(meta["blob_sha"]))
            blobs.append({"path": meta["path"], "blob_sha": meta["blob_sha"]})
    news: dict[str, str] = {}
    if role_usable(proof, session, "actions"):
        meta = _first_blob(proof, session, "actions")
        if meta:
            news = _news_map(_git_blob(meta["blob_sha"]))
            blobs.append({"path": meta["path"], "blob_sha": meta["blob_sha"]})
    ab: dict[str, str] = {}
    if role_usable(proof, session, "ab"):
        left = _first_blob(proof, session, "ab_checklist")
        right = _first_blob(proof, session, "ab_enriched")
        if left and right:
            common = set(_ab_map(_git_blob(left["blob_sha"]))) & set(_ab_map(_git_blob(right["blob_sha"])))
            scores = _ab_map(_git_blob(right["blob_sha"]))
            ab = {
                ticker: _polarity(math.tanh(scores[ticker] / 8.0))
                for ticker in common
                if ticker in scores
            }
            blobs.append({"path": left["path"], "blob_sha": left["blob_sha"]})
            blobs.append({"path": right["path"], "blob_sha": right["blob_sha"]})
    export = None
    if role_usable(proof, session, "export"):
        meta = _first_blob(proof, session, "export")
        if meta:
            export = cache.setdefault(meta["blob_sha"], _export_index(_git_blob(meta["blob_sha"])))
            blobs.append({"path": meta["path"], "blob_sha": meta["blob_sha"]})
    spy = store.feature("SPY", session)
    spy_ret5 = float(spy["ret_5"]) if spy.get("ok") and spy.get("ret_5") is not None else None
    buckets: dict[str, list[str]] = {
        "stock_book": stock_names,
        "flatten": flatten,
        **lists,
    }
    order: list[str] = []
    sources: dict[str, list[str]] = {}
    for name in ("flatten", "probable", "yday_gainer", "yday_mover", "ohlc_hot", "overnight", "stock_book"):
        for ticker in buckets.get(name) or []:
            sources.setdefault(ticker, [])
            if name not in sources[ticker]:
                sources[ticker].append(name)
            if ticker not in order:
                order.append(ticker)
    rows = []
    for rank, ticker in enumerate(order):
        rows.append(_row(store, session, ticker, sources[ticker], rank, news, ab, export, spy_ret5))
    blobs = sorted({(item["path"], item["blob_sha"]): item for item in blobs}.values(), key=lambda item: item["path"])
    return rows, blobs


def _candidate_rows(rows: list[dict], recipe: dict, stock_book_day: bool) -> list[dict]:
    uni = recipe.get("universe") or "union"
    if uni != "union":
        return rows
    if stock_book_day:
        return [row for row in rows if "stock_book" in row["sources"]]
    return [row for row in rows if set(row["sources"]) & PRICE_SOURCES]


def _mark(cash: float, pos: dict, session: str, store: Store, side: str) -> float:
    stock = 0.0
    for lot in pos.values():
        px = store.session_close(lot["ticker"], session)
        if px is None:
            px = lot.get("last_px")
        if px is None:
            continue
        notional = lot["shares"] * float(px)
        stock += notional if side == "long" else -notional
    return cash + stock


def walk_recipe(recipe: dict, sessions: list[str], rows_by_date: dict[str, list[dict]],
                store: Store, search_ok: dict[str, bool], fees: dict) -> dict:
    """One $10k book. Share counts follow Futubull fees. 15 bp reprices those fills."""
    cash_f = CAPITAL
    cash_15 = CAPITAL
    pos: dict[str, dict] = {}
    side = recipe.get("side") or "long"
    hold = int(recipe["hold"])
    days = []
    ticker_pnl: dict[str, dict[str, float]] = {}
    first_entry: dict[str, str] = {}
    prev_eq_f = CAPITAL
    prev_eq_15 = CAPITAL
    under = 0
    fills_n = 0
    for session in sessions:
        sat = not search_ok.get(session, False)
        rows = [] if sat else _candidate_rows(rows_by_date.get(session) or [], recipe, _usable(session, "stock_book"))
        chosen = [] if sat else pick_day(rows, recipe)
        # matches() is the frozen gate. pick_day already applied it.
        assert chosen == [] or all(matches(row, recipe) for row in chosen)
        by_ticker = {row["ticker"]: row for row in (rows_by_date.get(session) or [])}
        sold = []
        bought = []
        for ticker in list(pos):
            lot = pos[ticker]
            held = sessions.index(session) - sessions.index(lot["entry"])
            row = by_ticker.get(ticker) or {}
            early = should_exit(row, recipe.get("exit_when"))
            do_sell = early or held >= hold
            if not do_sell:
                continue
            try:
                px = store.session_open(ticker, session)
            except Exception:
                px = None
            if px is None:
                continue
            shares = lot["shares"]
            fee_f = order_fees(shares, px, "sell" if side == "long" else "buy", fees)
            fee_b = fee_15(shares, px)
            prev_px = float(lot.get("prev_mark", lot["entry_px"]))
            if side == "long":
                cash_f += shares * px - fee_f
                cash_15 += shares * px - fee_b
                pnl = shares * (px - prev_px) - fee_f
            else:
                cash_f -= shares * px + fee_f
                cash_15 -= shares * px + fee_b
                pnl = shares * (prev_px - px) - fee_f
            ticker_pnl.setdefault(ticker, {})[session] = ticker_pnl.get(ticker, {}).get(session, 0.0) + pnl
            pos.pop(ticker)
            price = round(float(px), 4)
            fill = {"ticker": ticker, "side": "SELL" if side == "long" else "COVER",
                    "shares": shares, "price": price}
            sold.append(fill)
            if price < 3.0:
                under += 1
            fills_n += 1
        new = [row for row in chosen if row["ticker"] not in pos]
        if new and (cash_f > 0 or side == "short"):
            room = cash_f if side == "long" else max(0.0, (cash_f + _open_stock(pos, session, store, side)) * 0.5)
            per = room / len(new) if new else 0.0
            for row in new:
                ticker = row["ticker"]
                try:
                    px = store.session_open(ticker, session)
                except Exception:
                    px = None
                if px is None:
                    continue
                shares = int(per // px) if side == "long" else int(per // px)
                if shares < 1:
                    continue
                fee_f = order_fees(shares, px, "buy" if side == "long" else "sell", fees)
                fee_b = fee_15(shares, px)
                if side == "long":
                    cost = shares * px + fee_f
                    if cost > cash_f + 1e-6:
                        shares = int((cash_f - fee_f) // px) if px else 0
                        if shares < 1:
                            continue
                        fee_f = order_fees(shares, px, "buy", fees)
                        fee_b = fee_15(shares, px)
                        cost = shares * px + fee_f
                    cash_f -= cost
                    cash_15 -= shares * px + fee_b
                    lot = {
                        "ticker": ticker, "shares": shares, "entry_px": px, "entry": session,
                        "fee_in_f": fee_f, "notional": shares * px, "last_px": px,
                    }
                else:
                    notional = shares * px
                    fee_f = order_fees(shares, px, "sell", fees)
                    cash_f += notional - fee_f
                    cash_15 += notional - fee_b
                    lot = {
                        "ticker": ticker, "shares": shares, "entry_px": px, "entry": session,
                        "fee_in_f": fee_f, "notional": notional, "last_px": px,
                    }
                pos[ticker] = lot
                first_entry.setdefault(ticker, session)
                price = round(float(px), 4)
                bought.append({
                    "ticker": ticker, "side": "BUY" if side == "long" else "SHORT",
                    "shares": shares, "price": price,
                })
                if price < 3.0:
                    under += 1
                fills_n += 1
        for lot in list(pos.values()):
            close = store.session_close(lot["ticker"], session)
            if close is not None:
                lot["last_px"] = close
            prev_px = float(lot.get("prev_mark", lot["entry_px"]))
            mark_px = float(lot.get("last_px") or prev_px)
            borrow = lot["shares"] * mark_px * BORROW_DAY if side == "short" else 0.0
            if borrow:
                cash_f -= borrow
                cash_15 -= borrow
            if side == "long":
                mtm = lot["shares"] * (mark_px - prev_px)
            else:
                mtm = lot["shares"] * (prev_px - mark_px) - borrow
            if lot["entry"] == session:
                mtm -= lot["fee_in_f"]
            ticker_pnl.setdefault(lot["ticker"], {})
            ticker_pnl[lot["ticker"]][session] = ticker_pnl[lot["ticker"]].get(session, 0.0) + mtm
            lot["prev_mark"] = mark_px
        eq_f = _mark(cash_f, pos, session, store, side)
        eq_15 = _mark(cash_15, pos, session, store, side)
        ret_f = (eq_f / prev_eq_f - 1.0) if prev_eq_f else 0.0
        ret_15 = (eq_15 / prev_eq_15 - 1.0) if prev_eq_15 else 0.0
        days.append({
            "session": session,
            "sat_out": sat,
            "picks": [row["ticker"] for row in chosen],
            "fills": sold + bought,
            "ret_futubull": ret_f,
            "ret_flat_15bp": ret_15,
            "equity_futubull": eq_f,
            "equity_flat_15bp": eq_15,
            "under_3_share": (under / fills_n) if fills_n else None,
        })
        # under_3_share on the day is that day's fills, not the running total.
        day_fills = sold + bought
        day_under = sum(1 for fill in day_fills if fill["price"] < 3.0)
        days[-1]["under_3_share"] = (day_under / len(day_fills)) if day_fills else None
        days[-1]["n_fills"] = len(day_fills)
        prev_eq_f = eq_f
        prev_eq_15 = eq_15
    return {
        "days": days,
        "ticker_pnl": ticker_pnl,
        "first_entry": first_entry,
        "n_fills": fills_n,
        "n_under_3": under,
    }


def _open_stock(pos: dict, session: str, store: Store, side: str) -> float:
    stock = 0.0
    for lot in pos.values():
        try:
            px = store.session_open(lot["ticker"], session)
        except Exception:
            px = None
        if px is None:
            px = lot.get("last_px")
        if px is None:
            continue
        notional = lot["shares"] * float(px)
        stock += notional if side == "long" else -notional
    return stock


def best_removed(check: list[str], ticker_pnl: dict[str, dict[str, float]],
                 first_entry: dict[str, str], equity: dict[str, float]) -> tuple[str | None, float | None]:
    """Drop the check-day ticker with the highest attributed P&L. Ties take the earlier ticker."""
    totals = []
    for ticker, by_day in ticker_pnl.items():
        total = sum(float(by_day.get(day) or 0.0) for day in check)
        entry = first_entry.get(ticker) or "9999-99-99"
        totals.append((total, entry, ticker))
    if not totals or not check:
        return None, None
    totals.sort(key=lambda item: (-item[0], item[1], item[2]))
    ticker = totals[0][2]
    rets = []
    for day in check:
        start = equity.get(day)
        if start is None or start == 0:
            continue
        # equity map stores the equity at the start of the day.
        end_key = day + "|end"
        end = equity.get(end_key)
        if end is None:
            continue
        removed = float(ticker_pnl.get(ticker, {}).get(day) or 0.0)
        rets.append((end - removed) / start - 1.0)
    if not rets:
        return ticker, None
    return ticker, compound(rets)


def score_all() -> dict:
    print("loading bars", flush=True)
    store = Store()
    print(f"tickers {len(store.tapes)} dates {len(store.dates)}", flush=True)
    proof = load_proof_rows()
    # The frozen date table and the pin must agree on the headline counts.
    for role, n in (("stock_book", 19), ("actions", 12), ("export", 20)):
        got = sum(1 for day in {d for d, _role in proof if _role == role} if role_usable(proof, day, role))
        if got != n:
            raise SystemExit(f"{role} usable days {got} != {n}")
    calendar = list(store.dates)
    after = [day for day in calendar if DESIGNED_AFTER_START <= day <= DESIGNED_AFTER_END]
    sessions = list(RUN_WINDOW) + after
    recipes = build_group3_recipes()
    print(f"sessions {len(sessions)} recipes {len(recipes)}", flush=True)
    rows_by_date: dict[str, list[dict]] = {}
    blobs_by_date: dict[str, list[dict]] = {}
    export_cache: dict = {}
    for session in sessions:
        print(f"build {session}", flush=True)
        rows, blobs = build_day(store, proof, session, calendar, export_cache)
        rows_by_date[session] = rows
        blobs_by_date[session] = blobs
        print(f"  rows {len(rows)} blobs {len(blobs)}", flush=True)
    fees = load_fees()
    books = []
    for recipe in recipes:
        ok = {day: all(_usable(day, role) for role in required_roles(recipe)) for day in sessions}
        # Run-window search days stay the frozen helper. Later days use the same role rule.
        frozen = set(search_days(recipe))
        for day in RUN_WINDOW:
            if ok[day] != (day in frozen):
                raise SystemExit(f"search day drift {recipe['name']} {day}")
        print(f"walk {recipe['name']}", flush=True)
        book = walk_recipe(recipe, sessions, rows_by_date, store, ok, fees)
        books.append((recipe, book, ok))
    return {
        "sessions": sessions,
        "blobs_by_date": blobs_by_date,
        "books": books,
        "store_sha": BAR_SHA256,
    }


def _round(value, digits: int):
    if value is None:
        return None
    return round(float(value), digits)


def write_record(scored: dict) -> dict:
    RETURNS.mkdir(parents=True, exist_ok=True)
    sessions = scored["sessions"]
    books = scored["books"]
    by_session: dict[str, dict] = {day: {"session": day, "study": "initial", "recipes": {}} for day in sessions}
    summaries = []
    for recipe, book, ok in books:
        name = recipe["name"]
        check = list(check_days(recipe))
        rets_f = []
        rets_15 = []
        after_f = []
        after_15 = []
        equity_start = {}
        prev_f = CAPITAL
        for day in book["days"]:
            session = day["session"]
            equity_start[session] = prev_f
            equity_start[session + "|end"] = day["equity_futubull"]
            prev_f = day["equity_futubull"]
            payload = {
                "sat_out": day["sat_out"],
                "picks": day["picks"],
                "fills": day["fills"],
                "ret_futubull": _round(day["ret_futubull"], 8),
                "ret_flat_15bp": _round(day["ret_flat_15bp"], 8),
                "under_3_share": None if day["under_3_share"] is None else _round(day["under_3_share"], 6),
            }
            by_session[session]["recipes"][name] = payload
            if session in check:
                rets_f.append(day["ret_futubull"])
                rets_15.append(day["ret_flat_15bp"])
            if DESIGNED_AFTER_START <= session <= DESIGNED_AFTER_END:
                after_f.append(day["ret_futubull"])
                after_15.append(day["ret_flat_15bp"])
        ticker, removed = best_removed(check, book["ticker_pnl"], book["first_entry"], equity_start)
        cum_f = compound(rets_f) if rets_f else 0.0
        cum_15 = compound(rets_15) if rets_15 else 0.0
        mean_f = sum(rets_f) / len(rets_f) if rets_f else 0.0
        mean_15 = sum(rets_15) / len(rets_15) if rets_15 else 0.0
        p_f = luck_p(rets_f)
        p_15 = luck_p(rets_15)
        line_a = cum_f >= 0.20
        line_b = removed is not None and removed > 0.0
        line_c = compound(after_f) >= 0.0 if after_f else False
        line_d = p_f < 0.05
        n_check = len(check)
        struck = True  # price-store rebuild_match is null; every fill reads it
        if struck:
            label = "struck"
        elif line_a and line_b and line_c and line_d and n_check < 10:
            label = "too few days to judge"
        elif line_a and line_b and line_c and line_d:
            label = "something good"
        else:
            label = "not something good"
        summaries.append({
            "name": name,
            "n_check_days": n_check,
            "n_search_days": sum(1 for day in RUN_WINDOW if ok.get(day)),
            "cum_futubull": cum_f,
            "cum_flat_15bp": cum_15,
            "mean_futubull": mean_f,
            "mean_flat_15bp": mean_15,
            "best_ticker": ticker,
            "best_removed": removed,
            "from_0914_futubull": compound(after_f) if after_f else None,
            "from_0914_flat_15bp": compound(after_15) if after_15 else None,
            "under_3_share": (book["n_under_3"] / book["n_fills"]) if book["n_fills"] else None,
            "n_fills": book["n_fills"],
            "luck_p_futubull": p_f,
            "luck_p_flat_15bp": p_15,
            "line_a": line_a,
            "line_b": line_b,
            "line_c": line_c,
            "line_d": line_d,
            "label": label,
            "full_span_futubull": compound([day["ret_futubull"] for day in book["days"]]),
        })
    lines = []
    for session in sessions:
        payload = by_session[session]
        # Stable recipe order.
        payload["recipes"] = {name: payload["recipes"][name] for name in sorted(payload["recipes"])}
        raw = (json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
        path = RETURNS / f"{session}.json"
        path.write_bytes(raw)
        lines.append(manifest_line({
            "file": f"{session}.json",
            "input_blobs": scored["blobs_by_date"][session],
            "session": session,
            "sha256": hashlib.sha256(raw).hexdigest(),
        }))
    manifest = "\n".join(lines) + "\n"
    (RETURNS / "manifest.jsonl").write_text(manifest, encoding="utf-8")
    verdict = "nothing proven yet"
    report = render_report(summaries, sessions)
    (RETURNS / "REPORT.md").write_text(report, encoding="utf-8")
    return {"summaries": summaries, "verdict": verdict, "sessions": sessions}


def render_report(summaries: list[dict], sessions: list[str]) -> str:
    ranked = sorted(summaries, key=lambda row: (-row["cum_futubull"], -row["full_span_futubull"], row["name"]))
    def n_pass(key: str) -> int:
        return sum(1 for row in summaries if row[key])

    def pct(value) -> str:
        if value is None:
            return ""
        return f"{100.0 * value:.2f}%"

    lines = [
        "# Group 3 initial score",
        "",
        "study: initial",
        "",
        "This is the initial Group 3 score of the 110 Factor Mine recipes. "
        "The preregistration was not edited. No locked ledger was rewritten.",
        "",
        "Price features are the frozen engine on `data/prices/ohlc.parquet`, "
        f"sha256 `{BAR_SHA256}`, blob `{BAR_BLOB_SHA}`. "
        "Feature bars are dated strictly before the session. "
        "Every recipe trades at the 09:30 open, so the session open is the fill and the gap. "
        "The session close is the mark only.",
        "",
        "The pinned stock_book blob is the candidate list on a usable stock_book day. "
        "That blob has buy lists and does not carry blue, alarm, zero_red, or camera counts. "
        "Those flags are not recomputed, so they stay false. "
        "AB Part A is not recomputed. AB tone is the enriched checklist score on days both AB files are usable.",
        "",
        "On days without usable stock_book, the union list is prior-day gainers (top 25), "
        "prior-day movers (top 20), the hot list (top 30), continuation (top 8), and the overnight gap list (top 20). "
        "Liquidity is a 20-session average volume of at least 500,000 shares. "
        "Market cap is not on the bar file.",
        "",
        "Every recipe reads the price store. `rebuild_match` on that store is null, so every recipe is `struck`. "
        "A struck recipe is not something good.",
        "",
        f"Sessions written: {len(sessions)} ({sessions[0]} through {sessions[-1]}).",
        f"Luck-test denominator: {LUCK_N}. "
        "p is a one-sided t-test that the mean check-day return is above zero, multiplied by 9,280.",
        "",
        f"Line (a) cumulative Futubull check-day return >= 20%: {n_pass('line_a')}",
        f"Line (b) best stock removed still positive: {n_pass('line_b')}",
        f"Line (c) Futubull compound 2026-09-14 through 2026-09-25 >= 0: {n_pass('line_c')}",
        f"Line (d) Futubull luck p < 0.05 on 9,280: {n_pass('line_d')}",
        f"All four lines: {sum(1 for row in summaries if row['line_a'] and row['line_b'] and row['line_c'] and row['line_d'])}",
        "",
        "Verdict: `nothing proven yet`",
        "",
        "## Top 10",
        "",
        "| rank | recipe | label | check days | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | from 09-14 | under $3 | luck p |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for rank, row in enumerate(ranked[:10], start=1):
        lines.append(
            "| {rank} | `{name}` | {label} | {n} | {cum} | {mean} | {c15} | {m15} | {removed} | {after} | {under} | {p} |".format(
                rank=rank,
                name=row["name"],
                label=row["label"],
                n=row["n_check_days"],
                cum=pct(row["cum_futubull"]),
                mean=pct(row["mean_futubull"]),
                c15=pct(row["cum_flat_15bp"]),
                m15=pct(row["mean_flat_15bp"]),
                removed=pct(row["best_removed"]),
                after=pct(row["from_0914_futubull"]),
                under="" if row["under_3_share"] is None else f"{100.0 * row['under_3_share']:.1f}%",
                p=f"{row['luck_p_futubull']:.4g}",
            )
        )
    lines += [
        "",
        "## All 110",
        "",
        "| recipe | label | search | check | a | b | c | d | Futubull cum | Futubull mean | 15bp cum | 15bp mean | from 09-14 | under $3 |",
        "| --- | --- | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in ranked:
        lines.append(
            "| `{name}` | {label} | {search} | {check} | {a} | {b} | {c} | {d} | {cum} | {mean} | {c15} | {m15} | {after} | {under} |".format(
                name=row["name"],
                label=row["label"],
                search=row["n_search_days"],
                check=row["n_check_days"],
                a="yes" if row["line_a"] else "no",
                b="yes" if row["line_b"] else "no",
                c="yes" if row["line_c"] else "no",
                d="yes" if row["line_d"] else "no",
                cum=pct(row["cum_futubull"]),
                mean=pct(row["mean_futubull"]),
                c15=pct(row["cum_flat_15bp"]),
                m15=pct(row["mean_flat_15bp"]),
                after=pct(row["from_0914_futubull"]),
                under="" if row["under_3_share"] is None else f"{100.0 * row['under_3_share']:.1f}%",
            )
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    scored = score_all()
    result = write_record(scored)
    print(result["verdict"])
    print("wrote", RETURNS)


if __name__ == "__main__":
    main()
