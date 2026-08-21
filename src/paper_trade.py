"""Paper-trading engine + dashboard for the daily stock book.

Simulates 10 sleeves (5 horizons x 2 selection sets), each with its own
capital, following the daily stock book:

    {1d,3d,1w,2w,1m}_top   — top N overall BUY names from the book
    {1d,3d,1w,2w,1m}_size  — top 3 per size bucket (large+ / mid / small-micro)

Rules
- Rebuild from scratch every run: replay all books chronologically (idempotent).
- Entry/exit at the signal day's closing price (yfinance, auto-adjusted).
- Follow-the-book: hold a name while it stays in the sleeve's pick list;
  sell when it drops out (only after the horizon min-hold: 1d=1, 3d=3,
  1w=5, 2w=10, 1m=21 sessions). New names split leftover cash equally
  (whole shares). Horizon chooses WHICH book to follow and the hold floor.
- Every order is charged the Futubull US-stock fee schedule
  (00_grounding/futubull_fees.json).
- SPY tracked as benchmark with the same starting capital.

Outputs
- data/paper/equity_curve.csv   — daily equity per sleeve + SPY
- data/paper/trades.csv         — every simulated order
- data/paper/state.json         — positions / cash per sleeve (latest)
- 03_scoreboard/PAPER_TRADING.md — summary table
- dashboard/index.html          — self-contained equity dashboard

CLI: python -m src.paper_trade [--date YYYY-MM-DD] [--top 10] [--capital 10000]
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
PAPER_DIR = ROOT / "data" / "paper"
SCOREBOARD = ROOT / "03_scoreboard"
DASH_DIR = ROOT / "dashboard"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"
PRICE_CACHE = PAPER_DIR / "prices_cache.csv"

HOLD_DAYS = {"1d": 1, "3d": 3, "1w": 5, "2w": 10, "1m": 21}
HORIZONS = list(HOLD_DAYS.keys())


# ---------------------------------------------------------------- fees ----

def load_fees() -> dict:
    return json.loads(FEES_PATH.read_text(encoding="utf-8"))


def order_fees(shares: int, price: float, side: str, f: dict) -> float:
    """Futubull US-stock order fees for one order."""
    if shares <= 0 or price <= 0:
        return 0.0
    amount = shares * price
    comm = min(max(f["commission_per_share"] * shares, f["commission_min_per_order"]),
               f["commission_max_pct_of_amount"] * amount)
    plat = min(max(f["platform_per_share"] * shares, f["platform_min_per_order"]),
               f["platform_max_pct_of_amount"] * amount)
    settle = f["settlement_per_share"] * shares
    total = comm + plat + settle
    if side == "sell":
        reg = max(f["regulatory_pct_of_amount_sell_only"] * amount,
                  f["regulatory_min_per_order"])
        taf = min(max(f["taf_per_share_sell_only"] * shares, f["taf_min_per_order"]),
                  f["taf_max_per_order"])
        total += reg + taf
    return round(total, 4)


# -------------------------------------------------------------- prices ----

def get_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Date-indexed close prices; incremental cache in data/paper/.

    Fetches a padded window (books are generated pre-market, so the signal
    day's own close may not exist yet — fills then use the last available
    close on/before the signal date, resolved in run_sim)."""
    cache = pd.DataFrame()
    if PRICE_CACHE.exists():
        cache = pd.read_csv(PRICE_CACHE, index_col=0, parse_dates=True)
    missing_cols = [t for t in tickers if t not in cache.columns]
    need_refresh = len(cache) == 0 or cache.index.max() < pd.Timestamp(end)
    if missing_cols or need_refresh:
        try:
            import yfinance as yf
        except ImportError:
            if cache.empty:
                raise SystemExit("[paper] yfinance missing and no prices_cache.csv")
            print("[paper] yfinance not installed — using prices_cache.csv")
            return cache
        fetch = sorted(set(tickers) | set(cache.columns))
        # yfinance `end` is EXCLUSIVE — pad so the signal day is included once
        # its close exists; start padded back for a pre-book price baseline.
        start_pad = (pd.Timestamp(start) - pd.Timedelta(days=10)).date().isoformat()
        end_excl = (pd.Timestamp(end) + pd.Timedelta(days=6)).date().isoformat()
        raw = yf.download(fetch, start=start_pad, end=end_excl, auto_adjust=True,
                          group_by="ticker", progress=False, threads=True)
        frames = {}
        for t in fetch:
            try:
                s = raw[(t, "Close")] if len(fetch) > 1 else raw["Close"]
            except KeyError:
                continue
            frames[t] = s.dropna()
        if not frames:
            raise SystemExit("[paper] yfinance returned no prices at all")
        new = pd.DataFrame(frames)
        new.index = pd.to_datetime(new.index)
        # new data wins on overlap; keep older cached rows outside the window
        combined = new.combine_first(cache) if not cache.empty else new
        cache = combined.dropna(how="all").sort_index()
        PAPER_DIR.mkdir(parents=True, exist_ok=True)
        cache.to_csv(PRICE_CACHE)
    return cache


# -------------------------------------------------------------- books -----

def list_books() -> list[tuple[str, Path]]:
    out = []
    for p in sorted(BOOK_DIR.glob("*_stock_book.json")):
        out.append((p.name.replace("_stock_book.json", ""), p))
    return out


def picks_from_book(book: dict, top_n: int) -> dict[str, list[str]]:
    """sleeve -> ordered pick list for one book."""
    books = book.get("books", {})
    picks: dict[str, list[str]] = {}
    for h in HORIZONS:
        hb = books.get(h) or {}
        top = [r["ticker"] for r in (hb.get("buy") or [])[:top_n]]
        picks[f"{h}_top"] = top
        sized: list[str] = []
        by_size = hb.get("buy_by_size") or {}
        for bucket in ("large+", "mid", "small/micro"):
            sized += [r["ticker"] for r in (by_size.get(bucket) or [])[:3]]
        picks[f"{h}_size"] = sized
    return picks


# ------------------------------------------------------------- engine -----

def run_sim(books: list[tuple[str, Path]], prices: pd.DataFrame,
            capital: float, top_n: int, fees: dict):
    sleeves = [f"{h}_{k}" for h in HORIZONS for k in ("top", "size")]
    st = {
        s: {"cash": capital, "pos": {}, "realized": 0.0, "fees": 0.0,
            "trades": 0, "wins": 0, "closed": 0}
        for s in sleeves
    }
    curve_rows: list[dict] = []
    trade_rows: list[dict] = []
    spy0 = None
    date_ix = {d: i for i, (d, _) in enumerate(books)}

    for date, path in books:
        day_px = prices.loc[:date]
        if day_px.empty:
            continue
        px = day_px.iloc[-1]  # close on (or last close before) signal date

        def price_of(t: str) -> float | None:
            v = px.get(t)
            if v is None or (isinstance(v, float) and (math.isnan(v) or v <= 0)):
                return None
            return float(v)

        book = json.loads(path.read_text(encoding="utf-8"))
        picks = picks_from_book(book, top_n)

        for sleeve, targets in picks.items():
            S = st[sleeve]
            tset = set(targets)
            horizon = sleeve.split("_")[0]
            min_hold = HOLD_DAYS[horizon]

            # exits: dropped off the book AND min-hold has elapsed
            for t in list(S["pos"]):
                if t in tset:
                    continue
                pos = S["pos"][t]
                held = date_ix[date] - date_ix.get(pos["entry_date"], date_ix[date])
                if held < min_hold:
                    continue  # still locked
                p = price_of(t)
                if p is None:
                    continue  # can't price -> carry position
                pos = S["pos"].pop(t)
                fee = order_fees(pos["shares"], p, "sell", fees)
                proceeds = pos["shares"] * p - fee
                S["cash"] += proceeds
                pnl = proceeds - pos["cost"]
                S["realized"] += pnl
                S["fees"] += fee
                S["trades"] += 1
                S["closed"] += 1
                S["wins"] += 1 if pnl > 0 else 0
                trade_rows.append({"date": date, "sleeve": sleeve, "ticker": t,
                                   "side": "sell", "shares": pos["shares"],
                                   "price": round(p, 4), "fees": fee,
                                   "amount": round(proceeds, 2),
                                   "realized_pnl": round(pnl, 2),
                                   "reason": f"dropped from {sleeve} after {held}d (min {min_hold}d)"})

            # entries: new names split available cash equally
            new = [t for t in targets if t not in S["pos"]]
            if new:
                per = S["cash"] / len(new)
                for t in new:
                    p = price_of(t)
                    if p is None or per <= 0:
                        continue
                    shares = int(per // p)
                    if shares < 1:
                        continue
                    fee = order_fees(shares, p, "buy", fees)
                    cost = shares * p + fee
                    if cost > S["cash"]:
                        shares = int((S["cash"] - fee) // p)
                        if shares < 1:
                            continue
                        fee = order_fees(shares, p, "buy", fees)
                        cost = shares * p + fee
                    S["cash"] -= cost
                    S["pos"][t] = {"shares": shares, "entry_date": date,
                                   "entry_px": p, "cost": cost}
                    S["fees"] += fee
                    S["trades"] += 1
                    trade_rows.append({"date": date, "sleeve": sleeve,
                                       "ticker": t, "side": "buy",
                                       "shares": shares, "price": round(p, 4),
                                       "fees": fee, "amount": round(cost, 2),
                                       "realized_pnl": "",
                                       "reason": f"entered {sleeve} book"})

        # mark-to-market
        spy = price_of("SPY")
        if spy and spy0 is None:
            spy0 = spy
        for sleeve in sleeves:
            S = st[sleeve]
            invested = 0.0
            for t, pos in S["pos"].items():
                p = price_of(t)
                invested += pos["shares"] * (p if p else pos["entry_px"])
            curve_rows.append({"date": date, "sleeve": sleeve,
                               "equity": round(S["cash"] + invested, 2),
                               "cash": round(S["cash"], 2),
                               "invested": round(invested, 2),
                               "fees_cum": round(S["fees"], 2),
                               "realized_cum": round(S["realized"], 2)})
        if spy and spy0:
            curve_rows.append({"date": date, "sleeve": "SPY (benchmark)",
                               "equity": round(capital * spy / spy0, 2),
                               "cash": "", "invested": "", "fees_cum": "",
                               "realized_cum": ""})
    return st, curve_rows, trade_rows


def match_roundtrips(trade_rows: list[dict], prices: pd.DataFrame) -> list[dict]:
    """FIFO: pair each buy lot with later sells of the same ticker in the same sleeve.

    One row per closed round-trip (bought then sold) and one row per leftover
    open lot. This is what the dashboard shows as 'closed' vs 'open'.
    """
    lots: dict[tuple[str, str], deque] = defaultdict(deque)
    closed: list[dict] = []

    def _held(buy: str, sell: str) -> int:
        try:
            return max(0, (pd.Timestamp(sell) - pd.Timestamp(buy)).days)
        except Exception:
            return 0

    for r in trade_rows:
        key = (r["sleeve"], str(r["ticker"]).upper())
        if r["side"] == "buy":
            lots[key].append({
                "buy_date": r["date"],
                "buy_px": float(r["price"]),
                "buy_fees": float(r.get("fees") or 0),
                "shares": int(r["shares"]),
                "buy_amount": float(r.get("amount") or 0),
            })
            continue
        if r["side"] != "sell":
            continue
        remaining = int(r["shares"])
        sold_total = remaining or 1
        sell_px = float(r["price"])
        sell_fees = float(r.get("fees") or 0)
        sell_amount = float(r.get("amount") or 0)
        sell_date = r["date"]
        while remaining > 0 and lots[key]:
            lot = lots[key][0]
            take = min(lot["shares"], remaining)
            frac_sell = take / sold_total
            frac_lot = take / lot["shares"] if lot["shares"] else 1.0
            buy_cost = lot["buy_amount"] * frac_lot
            sell_net = sell_amount * frac_sell
            pnl = sell_net - buy_cost
            closed.append({
                "status": "closed",
                "sleeve": r["sleeve"],
                "ticker": key[1],
                "shares": take,
                "buy_date": lot["buy_date"],
                "buy_px": round(lot["buy_px"], 4),
                "sell_date": sell_date,
                "sell_px": round(sell_px, 4),
                "last": None,
                "held_cal_days": _held(lot["buy_date"], sell_date),
                "realized_pnl": round(pnl, 2),
                "unrealized_pnl": None,
                "buy_fees": round(lot["buy_fees"] * frac_lot, 4),
                "sell_fees": round(sell_fees * frac_sell, 4),
            })
            lot["shares"] -= take
            lot["buy_amount"] -= buy_cost
            lot["buy_fees"] = lot["buy_fees"] * (1.0 - frac_lot)
            remaining -= take
            if lot["shares"] <= 0:
                lots[key].popleft()

    last_px = prices.iloc[-1] if len(prices) else pd.Series(dtype=float)
    open_rows: list[dict] = []
    for (sleeve, ticker), q in lots.items():
        for lot in q:
            if lot["shares"] <= 0:
                continue
            cur = last_px.get(ticker)
            last = float(cur) if cur == cur and cur else lot["buy_px"]
            mtm = lot["shares"] * last - lot["buy_amount"]
            open_rows.append({
                "status": "open",
                "sleeve": sleeve,
                "ticker": ticker,
                "shares": lot["shares"],
                "buy_date": lot["buy_date"],
                "buy_px": round(lot["buy_px"], 4),
                "sell_date": None,
                "sell_px": None,
                "last": round(last, 4),
                "held_cal_days": None,
                "realized_pnl": None,
                "unrealized_pnl": round(mtm, 2),
                "buy_fees": round(lot["buy_fees"], 4),
                "sell_fees": 0.0,
            })
    return closed + open_rows


# ------------------------------------------------------------ outputs -----

def sleeve_stats(sleeve: str, S: dict, prices: pd.DataFrame, capital: float) -> dict:
    px = prices.iloc[-1] if len(prices) else pd.Series(dtype=float)
    invested = 0.0
    unrealized = 0.0
    open_wins = 0
    for t, pos in S["pos"].items():
        v = px.get(t)
        last = float(v) if v == v and v else pos["entry_px"]
        invested += pos["shares"] * last
        mtm = pos["shares"] * last - pos["cost"]
        unrealized += mtm
        if mtm > 0:
            open_wins += 1
    equity = S["cash"] + invested
    closed = S["closed"]
    opened = len(S["pos"])
    return {"sleeve": sleeve, "equity": round(equity, 2),
            "return_pct": round(100 * (equity / capital - 1), 2),
            "cash": round(S["cash"], 2), "open": opened,
            "trades": S["trades"], "fees": round(S["fees"], 2),
            "realized": round(S["realized"], 2),
            "unrealized": round(unrealized, 2),
            "closed": closed,
            "closed_wins": S["wins"],
            "open_wins": open_wins,
            "win_rate": round(100 * S["wins"] / closed, 1) if closed else None,
            "open_win_rate": round(100 * open_wins / opened, 1) if opened else None}


def write_dashboard(curve: pd.DataFrame, stats: list[dict], st: dict,
                    prices: pd.DataFrame, date: str, capital: float,
                    fees: dict, trade_rows: list[dict] | None = None,
                    last_picks: dict[str, list[str]] | None = None,
                    book_dates: list[str] | None = None,
                    roundtrips: list[dict] | None = None) -> None:
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    curve = curve.copy()
    curve["date"] = pd.to_datetime(curve["date"])
    pivot = curve.pivot_table(index="date", columns="sleeve", values="equity",
                              aggfunc="last").sort_index()
    series = {c: [None if (v != v) else v for v in pivot[c].tolist()]
              for c in pivot.columns}
    payload = {
        "generated": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "dates": [str(d.date()) for d in pivot.index],
        "series": series,
        "stats": stats,
        "capital": capital,
        "fees": {k: fees[k] for k in fees if not k.startswith("_")},
        "rules": {
            "hold_days": HOLD_DAYS,
            "hold_applied": True,
            "fill": "signal-day close",
            "top": "top-N overall BUY names on that horizon's book",
            "size": "top 3 per size bucket (large+ / mid / small-micro)",
        },
    }
    positions = []
    px = prices.iloc[-1] if len(prices) else pd.Series(dtype=float)
    date_ix = {d: i for i, d in enumerate(book_dates or [])}
    last_picks = last_picks or {}
    for sleeve, S in st.items():
        horizon = sleeve.split("_")[0]
        min_hold = HOLD_DAYS.get(horizon, 1)
        on_list = set(last_picks.get(sleeve) or [])
        for t, pos in S["pos"].items():
            cur = px.get(t)
            cur = float(cur) if cur == cur and cur else pos["entry_px"]
            held = date_ix.get(date, 0) - date_ix.get(pos["entry_date"], date_ix.get(date, 0))
            positions.append({
                "sleeve": sleeve, "ticker": t, "shares": pos["shares"],
                "entry_date": pos["entry_date"], "entry_px": round(pos["entry_px"], 2),
                "last": round(cur, 2),
                "unrealized": round(pos["shares"] * cur - pos["cost"], 2),
                "on_book": t in on_list,
                "held_sessions": held,
                "min_hold": min_hold,
            })
    payload["positions"] = positions
    fills = []
    for r in trade_rows or []:
        pnl = r.get("realized_pnl")
        if pnl == "" or pnl is None:
            pnl = None
        else:
            try:
                pnl = round(float(pnl), 2)
            except (TypeError, ValueError):
                pnl = None
        fills.append({
            "date": r["date"], "sleeve": r["sleeve"], "ticker": r["ticker"],
            "side": r["side"], "shares": int(r["shares"]),
            "price": float(r["price"]), "fees": float(r.get("fees") or 0),
            "amount": float(r.get("amount") or 0), "realized_pnl": pnl,
            "reason": r.get("reason") or "",
        })
    payload["trades"] = fills
    payload["roundtrips"] = roundtrips or []

    shell = Path(__file__).with_name("paper_dash.html").read_text(encoding="utf-8")
    html = shell.replace("__DATA__", json.dumps(payload))
    (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def write_report(stats: list[dict], date: str, capital: float) -> None:
    SCOREBOARD.mkdir(parents=True, exist_ok=True)
    L = [
        "# Paper trading — Futubull-fee simulation",
        "",
        f"As of **{date}** · ${capital:,.0f} starting capital per sleeve · "
        "fees per `00_grounding/futubull_fees.json`",
        "",
        "Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 "
        "per size bucket. Fill at signal-day close. Sell only after min-hold "
        "(1d=1, 3d=3, 1w=5, 2w=10, 1m=21 sessions) AND the name has left the book.",
        "",
        "| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |",
        "|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|",
    ]
    for s in stats:
        wr = f"{s['win_rate']}%" if s["win_rate"] is not None else "—"
        ow = f"{s.get('open_win_rate')}%" if s.get("open_win_rate") is not None else "—"
        L.append(f"| {s['sleeve']} | ${s['equity']:,.2f} | {s['return_pct']:+.2f}% | "
                 f"${s['cash']:,.2f} | {s['open']} | {s['trades']} | "
                 f"${s['fees']:,.2f} | ${s['realized']:+,.2f} | "
                 f"${s.get('unrealized', 0):+,.2f} | {wr} | {ow} |")
    L += ["", "Equity curves + positions: `dashboard/index.html`", ""]
    (SCOREBOARD / "PAPER_TRADING.md").write_text("\n".join(L), encoding="utf-8")


# ------------------------------------------------------------ driver ------

def run(date: str | None = None, top_n: int = 10, capital: float | None = None) -> None:
    fees = load_fees()
    capital = capital or float(fees["paper_account"]["starting_capital_per_sleeve"])
    books = list_books()
    if date:
        books = [b for b in books if b[0] <= date]
    if not books:
        raise SystemExit("[paper] no stock books found — run stock_book first")

    # collect every ticker we may need to price
    tickers = {"SPY"}
    for _, p in books:
        bk = json.loads(p.read_text(encoding="utf-8"))
        for picks in picks_from_book(bk, top_n).values():
            tickers.update(picks)
    start, end = books[0][0], books[-1][0]
    prices = get_prices(sorted(tickers), start, end)

    st, curve_rows, trade_rows = run_sim(books, prices, capital, top_n, fees)
    if not curve_rows:
        raise SystemExit(
            f"[paper] no price data on/before {books[0][0]} — cannot simulate. "
            "Check yfinance connectivity.")

    trips = match_roundtrips(trade_rows, prices)
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    curve = pd.DataFrame(curve_rows)
    curve.to_csv(PAPER_DIR / "equity_curve.csv", index=False)
    pd.DataFrame(trade_rows).to_csv(PAPER_DIR / "trades.csv", index=False)
    if trips:
        pd.DataFrame(trips).to_csv(PAPER_DIR / "roundtrips.csv", index=False)
    (PAPER_DIR / "state.json").write_text(json.dumps(st, indent=2, default=str),
                                          encoding="utf-8")

    stats = [sleeve_stats(s, st[s], prices, capital) for s in st]
    last = books[-1][0]
    last_picks = picks_from_book(json.loads(books[-1][1].read_text(encoding="utf-8")), top_n)
    write_report(stats, last, capital)
    write_dashboard(curve, stats, st, prices, last, capital, fees, trade_rows,
                    last_picks=last_picks, book_dates=[d for d, _ in books],
                    roundtrips=trips)
    n_closed = sum(1 for t in trips if t["status"] == "closed")
    n_open = sum(1 for t in trips if t["status"] == "open")
    print(f"[paper] {len(books)} book(s), {len(trade_rows)} trades "
          f"({n_closed} closed pairs, {n_open} open lots), "
          f"curves → dashboard/index.html, summary → 03_scoreboard/PAPER_TRADING.md")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--capital", type=float, default=None)
    args = ap.parse_args()
    run(date=args.date, top_n=args.top, capital=args.capital)




if __name__ == "__main__":
    main()
