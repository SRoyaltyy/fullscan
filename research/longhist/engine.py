"""Long-history books. Price-only. No live path.

Membership, features, fees, and the pass rules follow PREREG.md and
ADDENDUM_1.md. This module does not write a score. The runner does.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import pyarrow.dataset as ds
from scipy import stats

from src.candle_factor import _from_bars as candle_from_bars
from src.ohlc_ripper import from_bars, hot_score
from src.paper_trade import load_fees, order_fees

from research.longhist.membership import proxy_members

BARS_DIR = "research/longhist/bars/ohlcv"
FEE_SHA = "3fc9b5829758722bd968e84baf0923f5efd8006d3b780a18e4d417552a9e80a7"
CAPITAL = 10000.0
BP15 = 0.0015

GATES = ("break10", "rvol_lg", "zero_px", "ret5_up", "pullback")
HOLDS = (1, 2)
RANKS = ("hot_score", "candle_score")
TOPS = (2, 4)


def fee_model() -> dict:
    raw = load_fees()
    priced = {k: v for k, v in raw.items() if not str(k).startswith("_")}
    body = json.dumps(priced, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(body).hexdigest()
    if digest != FEE_SHA:
        raise RuntimeError(f"fee hash {digest} != {FEE_SHA}")
    return raw


def ymd(day: str) -> int:
    return int(day.replace("-", ""))


def iso(day: int) -> str:
    text = f"{int(day):08d}"
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


@dataclass
class Tape:
    dates: np.ndarray
    o: np.ndarray
    h: np.ndarray
    l: np.ndarray
    c: np.ndarray
    v: np.ndarray

    def index(self, day: int) -> int | None:
        i = int(np.searchsorted(self.dates, day))
        if i >= len(self.dates) or int(self.dates[i]) != int(day):
            return None
        return i

    def px(self, day: int, which: str) -> float | None:
        i = self.index(day)
        if i is None:
            return None
        val = float(self.o[i] if which == "open" else self.c[i])
        if not math.isfinite(val) or val <= 0:
            return None
        return val


@dataclass
class Row:
    ticker: str
    gap: float
    rvol: float
    break_10: bool
    last_green: bool
    zero_red: bool
    ret_5: float
    ret_10: float
    hot: float
    candle: float
    open: float


@dataclass
class Rule:
    name: str
    hold: int
    gate: str
    rank: str
    top_n: int


@dataclass
class Closed:
    ticker: str
    entry_date: str
    exit_date: str
    shares: int
    entry_px: float
    exit_px: float
    buy_fee: float
    sell_fee: float
    pnl: float
    ret: float


@dataclass
class Book:
    rule: str
    trades: list[Closed] = field(default_factory=list)
    buy_dates: list[tuple[str, str]] = field(default_factory=list)
    daily: list[tuple[str, float, float, float, int]] = field(default_factory=list)
    final_equity: float = CAPITAL


def part1_rules() -> list[Rule]:
    return [
        Rule("longhist_break10_h2", 2, "break10", "hot_score", 4),
        Rule("longhist_rvol_lg_h1", 1, "rvol_lg", "hot_score", 4),
        Rule("longhist_break10_h1", 1, "break10", "hot_score", 4),
        Rule("longhist_zero_candle_h2", 2, "zero_px", "candle_score", 4),
    ]


def part2_rules() -> list[Rule]:
    rules = []
    for gate in GATES:
        for hold in HOLDS:
            for rank in RANKS:
                for top_n in TOPS:
                    rules.append(Rule(
                        f"longhist2_{gate}_h{hold}_n{top_n}_{rank}",
                        hold, gate, rank, top_n,
                    ))
    if len(rules) != 40:
        raise RuntimeError(f"grid is {len(rules)}, not 40")
    return rules


def rule_body(rule: Rule) -> dict:
    return {
        "alarm": False,
        "capital": 10000,
        "created_on": "2026-09-26",
        "day_cap": 1,
        "exit_when": [],
        "gate": rule.gate,
        "hold": rule.hold,
        "name": rule.name,
        "rank": rule.rank,
        "s_boost": "none",
        "sell": "time",
        "side": "long",
        "size": "leftover",
        "stop_pct": None,
        "take_pct": None,
        "top_n": rule.top_n,
        "universe": "proxy60",
    }


def sessions_between(start: str, end: str) -> list[str]:
    import exchange_calendars as ec
    cal = ec.get_calendar("XNYS")
    days = cal.sessions_in_range(start, end)
    return [d.strftime("%Y-%m-%d") for d in days]


def load_tapes(max_date: str | None) -> dict[str, Tape]:
    """Load quote bars. ``max_date`` drops every later session.

    The train search passes 2023-12-31 and then checks the newest bar.
    """
    dataset = ds.dataset(BARS_DIR, format="parquet")
    columns = ["date", "ticker", "open", "high", "low", "close", "volume"]
    filt = ds.field("date") <= max_date if max_date else None
    table = dataset.to_table(columns=columns, filter=filt)
    if table.num_rows == 0:
        raise RuntimeError("bar cache is empty")
    newest = max(table.column("date").to_pylist())
    if max_date and newest > max_date:
        raise RuntimeError(f"loader leaked {newest} past {max_date}")
    frame = table.to_pandas()
    frame.sort_values(["ticker", "date"], inplace=True, kind="mergesort")
    tapes: dict[str, Tape] = {}
    for ticker, grp in frame.groupby("ticker", sort=False):
        dates = np.array([ymd(str(d)[:10]) for d in grp["date"]], dtype=np.int32)
        tapes[str(ticker)] = Tape(
            dates=dates,
            o=grp["open"].to_numpy(np.float64),
            h=grp["high"].to_numpy(np.float64),
            l=grp["low"].to_numpy(np.float64),
            c=grp["close"].to_numpy(np.float64),
            v=grp["volume"].to_numpy(np.float64),
        )
    return tapes


def _passes_prior(tape: Tape, i: int) -> tuple[bool, float]:
    """Price, dollar volume, and gap. Prior bars are ``[:i]``."""
    pc = float(tape.c[i - 1])
    if not (1.0 <= pc <= 30.0):
        return False, 0.0
    total = 0.0
    c, v = tape.c, tape.v
    for j in range(i - 20, i):
        total += float(c[j]) * float(v[j])
    mean = total / 20.0
    if mean < 1_000_000.0 or mean > 40_000_000.0:
        return False, 0.0
    opened = float(tape.o[i])
    if opened <= 0 or pc == 0:
        return False, 0.0
    gap = abs(opened / pc - 1.0)
    if gap < 0.04:
        return False, 0.0
    return True, gap


def _zero_red(tape: Tape, i: int) -> bool:
    greens = reds = 0
    o, c = tape.o, tape.c
    for j in range(i - 8, i):
        if c[j] > o[j]:
            greens += 1
        elif c[j] < o[j]:
            reds += 1
    return greens >= 1 and reds == 0


def _prior_dicts(tape: Tape, i: int, n: int) -> list[dict]:
    start = max(0, i - n)
    out = []
    o, h, low, c, v = tape.o, tape.h, tape.l, tape.c, tape.v
    for j in range(start, i):
        out.append({
            "open": float(o[j]),
            "high": float(h[j]),
            "low": float(low[j]),
            "close": float(c[j]),
            "volume": float(v[j]),
        })
    return out


def _gate(row: Row, gate: str) -> bool:
    if gate == "break10":
        return row.break_10
    if gate == "rvol_lg":
        return row.last_green and row.rvol >= 1.5
    if gate == "zero_px":
        return row.zero_red and row.last_green
    if gate == "ret5_up":
        return row.ret_5 >= 0
    if gate == "pullback":
        return row.last_green and row.ret_5 <= -3
    raise KeyError(gate)


def _rank_key(row: Row, how: str) -> tuple:
    score = row.hot if how == "hot_score" else row.candle
    if not math.isfinite(score):
        score = 0.0
    return (-score, row.ticker)


def build_panel(tapes: dict[str, Tape], sessions: list[str]) -> list[list[Row]]:
    """One capped proxy list per session, in gap / rvol / ticker order."""
    session_ints = [ymd(day) for day in sessions]
    session_arr = np.array(session_ints, dtype=np.int32)
    hits: dict[int, list[Row]] = {day: [] for day in session_ints}
    checked = 0
    for n_ticker, (ticker, tape) in enumerate(tapes.items(), start=1):
        if ticker == "IWM":
            continue
        n = len(tape.dates)
        if n < 21:
            continue
        idx = np.flatnonzero(np.isin(tape.dates, session_arr))
        idx = idx[idx >= 20]
        if len(idx) == 0:
            if n_ticker % 1000 == 0:
                print(f"panel tickers {n_ticker}/{len(tapes)}", flush=True)
            continue
        pc = tape.c[idx - 1]
        opened = tape.o[idx]
        with np.errstate(divide="ignore", invalid="ignore"):
            gap = np.abs(opened / pc - 1.0)
        keep = (pc >= 1.0) & (pc <= 30.0) & (opened > 0) & np.isfinite(gap) & (gap >= 0.04)
        for i in idx[keep]:
            i = int(i)
            ok, gap_i = _passes_prior(tape, i)
            if not ok:
                continue
            checked += 1
            feat = from_bars(_prior_dicts(tape, i, 60))
            rvol = float(feat.get("rvol") or 0.0)
            if not feat.get("ok") or rvol < 1.5:
                continue
            hits[int(tape.dates[i])].append(Row(
                ticker=ticker,
                gap=gap_i,
                rvol=rvol,
                break_10=bool(feat.get("break_10")),
                last_green=bool(feat.get("last_green")),
                zero_red=_zero_red(tape, i),
                ret_5=float(feat.get("ret_5") or 0.0),
                ret_10=float(feat.get("ret_10") or 0.0),
                hot=float(hot_score(feat)),
                candle=0.0,
                open=float(tape.o[i]),
            ))
        if n_ticker % 1000 == 0:
            print(f"panel tickers {n_ticker}/{len(tapes)}", flush=True)
    panel: list[list[Row]] = []
    for day in session_ints:
        pool = hits[day]
        pool.sort(key=lambda row: (-row.gap, -row.rvol, row.ticker))
        chosen = pool[:60]
        tape_for = tapes
        for row in chosen:
            tape = tape_for[row.ticker]
            i = tape.index(day)
            feat = candle_from_bars(_prior_dicts(tape, i, 8))
            row.candle = float(feat.get("score") or 0.0)
        panel.append(chosen)
    print(f"panel prior-checks {checked}", flush=True)
    return panel


def audit_panel(tapes: dict[str, Tape], panel: list[list[Row]], sessions: list[str]) -> None:
    """Three sessions must match membership.proxy_members."""
    picks = [min(100, len(sessions) - 1), len(sessions) // 2, len(sessions) - 1]
    for si in picks:
        day = sessions[si]
        day_i = ymd(day)
        bars = {}
        for ticker, tape in tapes.items():
            if ticker == "IWM":
                continue
            i = tape.index(day_i)
            if i is None:
                continue
            start = max(0, i - 80)
            chunk = []
            for j in range(start, i + 1):
                chunk.append({
                    "date": iso(int(tape.dates[j])),
                    "open": float(tape.o[j]),
                    "high": float(tape.h[j]),
                    "low": float(tape.l[j]),
                    "close": float(tape.c[j]),
                    "volume": float(tape.v[j]),
                })
            bars[ticker] = chunk
        got = [row.ticker for row in panel[si]]
        exp = proxy_members(bars, day)
        if got != exp:
            raise RuntimeError(f"panel mismatch on {day}: {got[:8]} vs {exp[:8]}")
    print("panel audit ok", flush=True)


def look_list(rows: list[Row], rule: Rule, banned: str | None) -> list[Row]:
    pool = [row for row in rows if row.ticker != banned and _gate(row, rule.gate)]
    pool.sort(key=lambda row: _rank_key(row, rule.rank))
    return pool[: rule.top_n]


def simulate(panel: list[list[Row]], tapes: dict[str, Tape], sessions: list[str],
             rule: Rule, fees: dict, banned: str | None = None,
             picks: list[list[Row]] | None = None) -> Book:
    """Leftover book. Sell at the open of session i+hold, then buy."""
    cash = CAPITAL
    pos: dict[str, dict] = {}
    book = Book(rule=rule.name)
    prev = CAPITAL
    for si, day in enumerate(sessions):
        day_i = ymd(day)
        still = {}
        for ticker, lot in pos.items():
            px = tapes[ticker].px(day_i, "open") if ticker in tapes else None
            held = si - lot["entry_si"]
            if px is None or held < rule.hold:
                if px is not None:
                    lot["last_px"] = px
                still[ticker] = lot
                continue
            fee = order_fees(lot["shares"], px, "sell", fees)
            pnl = lot["shares"] * (px - lot["entry_px"]) - lot["buy_fee"] - fee
            ret = pnl / (lot["shares"] * lot["entry_px"])
            cash += lot["shares"] * px - fee
            book.trades.append(Closed(
                ticker=ticker,
                entry_date=lot["entry_date"],
                exit_date=day,
                shares=lot["shares"],
                entry_px=lot["entry_px"],
                exit_px=px,
                buy_fee=lot["buy_fee"],
                sell_fee=fee,
                pnl=pnl,
                ret=ret,
            ))
        pos = still
        chosen = picks[si] if picks is not None else look_list(panel[si], rule, banned)
        new = [row for row in chosen if row.ticker not in pos]
        if new and cash > 0:
            each = cash / len(new)
            for row in new:
                px = row.open
                if px is None or px <= 0:
                    continue
                shares = int(each // px)
                if shares < 1:
                    continue
                fee = order_fees(shares, px, "buy", fees)
                cost = shares * px + fee
                while shares >= 1 and cost > cash + 1e-6:
                    shares = int((cash - fee) // px) if px else 0
                    if shares < 1:
                        break
                    fee = order_fees(shares, px, "buy", fees)
                    cost = shares * px + fee
                if shares < 1 or cost > cash + 1e-6:
                    continue
                cash -= cost
                pos[row.ticker] = {
                    "shares": shares,
                    "entry_px": px,
                    "entry_date": day,
                    "entry_si": si,
                    "buy_fee": fee,
                    "last_px": px,
                }
                book.buy_dates.append((day, row.ticker))
        equity = cash
        for ticker, lot in pos.items():
            px = tapes[ticker].px(day_i, "close") if ticker in tapes else None
            if px is None:
                px = lot["last_px"]
            else:
                lot["last_px"] = px
            equity += lot["shares"] * px
        daily_ret = equity / prev - 1.0 if prev else 0.0
        book.daily.append((day, equity, daily_ret, cash, len(pos)))
        prev = equity
    book.final_equity = prev
    return book


def one_share_return(tapes: dict[str, Tape], sessions: list[str], si: int,
                     ticker: str, hold: int, fees: dict) -> float:
    entry_i = ymd(sessions[si])
    exit_at = si + hold
    if exit_at >= len(sessions) or ticker not in tapes:
        return float("nan")
    entry = tapes[ticker].px(entry_i, "open")
    exit_px = tapes[ticker].px(ymd(sessions[exit_at]), "open")
    if entry is None or exit_px is None:
        return float("nan")
    buy = order_fees(1, entry, "buy", fees)
    sell = order_fees(1, exit_px, "sell", fees)
    return (1.0 * (exit_px - entry) - buy - sell) / entry


def cluster_test(trades: list[Closed]) -> dict:
    by_day: dict[str, list[float]] = defaultdict(list)
    for trade in trades:
        by_day[trade.entry_date].append(trade.ret)
    days = sorted(by_day)
    series = np.array([float(np.mean(by_day[day])) for day in days], dtype=float)
    n = int(series.size)
    mean = float(series.mean()) if n else 0.0
    if n < 2:
        return {"n": n, "mean": mean, "t": 0.0, "p": 1.0, "std": 0.0}
    std = float(series.std(ddof=1))
    if std == 0.0 and mean > 0.0:
        return {"n": n, "mean": mean, "t": math.inf, "p": 0.0, "std": 0.0}
    if std == 0.0:
        return {"n": n, "mean": mean, "t": 0.0, "p": 1.0, "std": 0.0}
    result = stats.ttest_1samp(series, 0.0, alternative="greater")
    p = float(result.pvalue)
    tstat = float(result.statistic)
    if not math.isfinite(p):
        p = 1.0
    return {"n": n, "mean": mean, "t": tstat, "p": p, "std": std}


def holm(pvals: list[float]) -> list[float]:
    m = len(pvals)
    order = sorted(range(m), key=lambda i: (pvals[i], i))
    adj = [1.0] * m
    prev = 0.0
    for rank, i in enumerate(order, start=1):
        raw = min(1.0, (m - rank + 1) * pvals[i])
        prev = max(raw, prev)
        adj[i] = prev
    return adj


def _year_pnl(trades: list[Closed]) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for trade in trades:
        out[trade.exit_date[:4]] += trade.pnl
    return dict(out)


def _period_pnl(trades: list[Closed], start: str, end: str) -> float:
    return float(sum(t.pnl for t in trades if start <= t.exit_date <= end))


def best_ticker(trades: list[Closed]) -> str | None:
    totals: dict[str, float] = defaultdict(float)
    for trade in trades:
        totals[trade.ticker] += trade.pnl
    if not totals:
        return None
    return sorted(totals.items(), key=lambda item: (-item[1], item[0]))[0][0]


def pnl15(trades: list[Closed]) -> float:
    return float(sum(
        t.shares * (t.exit_px - t.entry_px) - BP15 * t.shares * t.entry_px
        for t in trades
    ))


def buy_counts(book: Book) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for day, _ticker in book.buy_dates:
        counts[day[:4]] += 1
    return dict(counts)


def score_books(books: list[Book], reruns: list[Book], n_sessions: int,
                family: str) -> list[dict]:
    tests = [cluster_test(book.trades) for book in books]
    adjusted = holm([item["p"] for item in tests])
    rows = []
    for book, rerun, test, adj in zip(books, reruns, tests, adjusted):
        closed = float(sum(t.pnl for t in book.trades))
        wins = sum(1 for t in book.trades if t.pnl > 0)
        n_closed = len(book.trades)
        win_rate = wins / n_closed if n_closed else 0.0
        rate = len(book.buy_dates) / (n_sessions / 252.0) if n_sessions else 0.0
        years = _year_pnl(book.trades)
        six = [years.get(str(y), 0.0) > 0 for y in range(2019, 2025)]
        train_years = [years.get(str(y), 0.0) > 0 for y in range(2019, 2024)]
        y2024 = _period_pnl(book.trades, "2024-01-01", "2024-12-31")
        y2025 = _period_pnl(book.trades, "2025-01-01", "2026-08-12")
        pass_mean = test["mean"] > 0 and adj < 0.05
        pass_drop = float(sum(t.pnl for t in rerun.trades)) > 0
        pass_rate = rate >= 30
        if family == "part1":
            pass_years = sum(six) >= 4
            study_pass = pass_mean and pass_years and pass_drop and pass_rate
        elif family == "part2_train":
            pass_years = sum(train_years) >= 4
            study_pass = pass_mean and pass_years and pass_drop and pass_rate
        elif family == "part2_test":
            pass_years = y2024 > 0 and y2025 > 0
            study_pass = pass_mean and pass_years and pass_drop and pass_rate
        else:
            raise KeyError(family)
        keep = len(book.buy_dates) >= 30 and win_rate > 0.55
        rows.append({
            "rule": book.rule,
            "family": family,
            "pass_6_1_mean": pass_mean,
            "pass_years": pass_years,
            "pass_6_3_best_removed": pass_drop,
            "pass_6_4_rate": pass_rate,
            "study_pass": study_pass,
            "ironclad_keep_bar": keep,
            "label": (
                "study_pass" if study_pass and keep
                else "study_pass_ironclad_keep_bar_fail" if study_pass
                else "fail"
            ),
            "cluster_n": test["n"],
            "cluster_mean": test["mean"],
            "t": test["t"],
            "p": test["p"],
            "p_holm": adj,
            "closed_trades": n_closed,
            "buy_fills": len(book.buy_dates),
            "buy_fills_by_year": buy_counts(book),
            "fires_per_year": rate,
            "closed_pnl": closed,
            "return_on_10000": book.final_equity / CAPITAL - 1.0,
            "ending_equity": book.final_equity,
            "pnl_15bp_same_shares": pnl15(book.trades),
            "best_ticker": best_ticker(book.trades),
            "removed_closed_pnl": float(sum(t.pnl for t in rerun.trades)),
            "removed_return_on_10000": rerun.final_equity / CAPITAL - 1.0,
            "removed_ending_equity": rerun.final_equity,
            "year_pnl": {str(y): years.get(str(y), 0.0) for y in range(2019, 2027)},
            "pnl_2024": y2024,
            "pnl_2025_through_2026_08_12": y2025,
            "win_rate": win_rate,
            "positive_years_2019_2024": int(sum(six)),
            "positive_train_years": int(sum(train_years)),
        })
    return rows


def random4(panel: list[list[Row]], tapes: dict[str, Tape], sessions: list[str],
            fees: dict) -> list[float]:
    rng = np.random.Generator(np.random.PCG64(20260813))
    rule = Rule("RANDOM4", 1, "break10", "hot_score", 4)
    equities = []
    for _draw in range(1000):
        picks: list[list[Row]] = []
        for rows in panel:
            if not rows:
                picks.append([])
                continue
            k = min(4, len(rows))
            chosen = rng.choice(len(rows), size=k, replace=False)
            picks.append([rows[int(i)] for i in chosen])
        book = simulate(panel, tapes, sessions, rule, fees, picks=picks)
        equities.append(book.final_equity)
    return equities


def iwm_book(tapes: dict[str, Tape], sessions: list[str], fees: dict) -> dict:
    tape = tapes.get("IWM")
    if tape is None:
        return {"ok": False, "reason": "IWM missing"}
    entry_day = entry = None
    for day in sessions:
        entry = tape.px(ymd(day), "open")
        if entry is not None:
            entry_day = day
            break
    exit_px = tape.px(ymd(sessions[-1]), "close") if sessions else None
    if entry is None or exit_px is None or entry_day is None:
        return {"ok": False, "reason": "IWM missing open or final close"}
    shares = int(CAPITAL // entry)
    fee = order_fees(shares, entry, "buy", fees)
    cost = shares * entry + fee
    if cost > CAPITAL + 1e-6:
        shares = int((CAPITAL - fee) // entry)
        fee = order_fees(shares, entry, "buy", fees)
        cost = shares * entry + fee
    sell_fee = order_fees(shares, exit_px, "sell", fees)
    pnl = shares * (exit_px - entry) - fee - sell_fee
    pnl_15 = shares * (exit_px - entry) - BP15 * shares * entry
    return {
        "ok": True,
        "entry_date": entry_day,
        "exit_date": sessions[-1],
        "shares": shares,
        "entry": entry,
        "exit": exit_px,
        "pnl": pnl,
        "return_on_10000": pnl / CAPITAL,
        "pnl_15bp": pnl_15,
        "return_15bp": pnl_15 / CAPITAL,
    }


def attach_baselines(rows: list[dict], equities: list[float], iwm: dict) -> None:
    mean_ret = float(np.mean(np.array(equities) / CAPITAL - 1.0))
    for row in rows:
        beats = sum(1 for eq in equities if row["ending_equity"] > eq)
        row["random4_mean_return"] = mean_ret
        row["random4_draws_beaten"] = beats / len(equities)
        row["iwm"] = iwm


def luck_test(books: list[Book], panel: list[list[Row]], tapes: dict[str, Tape],
              sessions: list[str], fees: dict) -> dict:
    """10,000 within-day reshuffles. One permutation per session per draw."""
    n_rules = len(books)
    n_sess = len(sessions)
    ret1 = []
    ret2 = []
    slots = [[ [] for _ in range(n_sess) ] for _ in range(n_rules)]
    for si, rows in enumerate(panel):
        ordered = sorted(rows, key=lambda row: row.ticker)
        index = {row.ticker: i for i, row in enumerate(ordered)}
        r1 = np.array([
            one_share_return(tapes, sessions, si, row.ticker, 1, fees) for row in ordered
        ], dtype=float)
        r2 = np.array([
            one_share_return(tapes, sessions, si, row.ticker, 2, fees) for row in ordered
        ], dtype=float)
        ret1.append(r1)
        ret2.append(r2)
    holds = []
    for book in books:
        if "_h1" in book.rule:
            holds.append(1)
        elif "_h2" in book.rule:
            holds.append(2)
        else:
            raise RuntimeError(f"no hold in {book.rule}")
    for ri, book in enumerate(books):
        by_day = defaultdict(list)
        for day, ticker in book.buy_dates:
            by_day[day].append(ticker)
        for si, day in enumerate(sessions):
            ordered = sorted(panel[si], key=lambda row: row.ticker)
            index = {row.ticker: i for i, row in enumerate(ordered)}
            slots[ri][si] = [index[t] for t in by_day.get(day, []) if t in index]

    real = np.zeros(n_rules)
    real_n = np.zeros(n_rules)
    for ri, hold in enumerate(holds):
        for si in range(n_sess):
            series = ret1[si] if hold == 1 else ret2[si]
            for slot in slots[ri][si]:
                val = float(series[slot]) if len(series) else float("nan")
                if math.isfinite(val):
                    real[ri] += val
                    real_n[ri] += 1
    real_mean = np.divide(real, real_n, out=np.full(n_rules, np.nan), where=real_n > 0)

    hold1 = np.array([1.0 if h == 1 else 0.0 for h in holds])
    hold2 = 1.0 - hold1
    masks = []
    for si in range(n_sess):
        n = len(ret1[si])
        mask = np.zeros((max(n, 1), n_rules), dtype=np.float64)
        for ri in range(n_rules):
            for slot in slots[ri][si]:
                mask[slot, ri] = 1.0
        masks.append(mask)
    rng = np.random.Generator(np.random.PCG64(20260926))
    null_sum = np.zeros((10000, n_rules))
    null_n = np.zeros((10000, n_rules))
    for k in range(10000):
        if k and k % 1000 == 0:
            print(f"luck {k}", flush=True)
        for si in range(n_sess):
            n = len(ret1[si])
            perm = rng.permutation(n)
            if n == 0:
                continue
            p1 = ret1[si][perm]
            p2 = ret2[si][perm]
            c1 = np.where(np.isfinite(p1), p1, 0.0)
            c2 = np.where(np.isfinite(p2), p2, 0.0)
            mask = masks[si]
            null_sum[k] += (c1 @ mask) * hold1 + (c2 @ mask) * hold2
            f1 = np.isfinite(p1).astype(np.float64)
            f2 = np.isfinite(p2).astype(np.float64)
            null_n[k] += (f1 @ mask) * hold1 + (f2 @ mask) * hold2
    null_mean = np.divide(null_sum, null_n, out=np.full_like(null_sum, np.nan), where=null_n > 0)
    out_rules = []
    eligible = []
    for ri, book in enumerate(books):
        real_ok = math.isfinite(real_mean[ri])
        if real_ok:
            ge = int(np.sum(null_mean[:, ri] >= real_mean[ri]))
            p = (1 + ge) / 10001
            eligible.append(ri)
        else:
            p = 1.0
        out_rules.append({
            "rule": book.rule,
            "real_1share_mean": None if not real_ok else float(real_mean[ri]),
            "pooled_picks": int(real_n[ri]),
            "p_rule": p,
        })
    if eligible:
        real_best = float(np.nanmax(real_mean[eligible]))
        best_null = np.nanmax(null_mean[:, eligible], axis=1)
        ge_best = int(np.sum(best_null >= real_best))
        p_best = (1 + ge_best) / 10001
    else:
        real_best = None
        p_best = 1.0
    return {
        "seed": 20260926,
        "reshuffles": 10000,
        "permutation_calls": 10000 * n_sess,
        "p_best": p_best,
        "real_best_1share_mean": real_best,
        "rules": out_rules,
    }


def freeze_hash(bodies: list[dict]) -> str:
    body = json.dumps(bodies, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(body).hexdigest()
