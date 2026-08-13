"""Daily backtest of unified stock-book suggestions.

For each saved data/stock_book/<signal_date>_stock_book.json:
  entry  = next trading session after signal (approx: signal_date close via yfinance,
           then forward N trading days)
  horizons: 1d=1, 3d=3, 1w=5, 2w=10, 1m=21 trading days
  buy  side: +forward return
  sell side: −forward return (profit if price falls)

Writes:
  03_scoreboard/STOCK_BOOK_BACKTEST.md
  03_scoreboard/stock_book_backtest.json
  01_daily/<today>_stock_book_backtest.md

CLI: python -m src.stock_book_backtest [--max-books 30] [--top 25]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from . import config

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
OUT_MD = ROOT / "03_scoreboard" / "STOCK_BOOK_BACKTEST.md"
OUT_JSON = ROOT / "03_scoreboard" / "stock_book_backtest.json"
DAILY = ROOT / "01_daily"

HORIZON_DAYS = {"1d": 1, "3d": 3, "1w": 5, "2w": 10, "1m": 21}


def _list_books() -> list[Path]:
    return sorted(BOOK_DIR.glob("*_stock_book.json"))


def _fwd_returns(tickers: list[str], start: str, n_td: int) -> dict[str, float | None]:
    """Close-to-close style forward return over ~n trading days using yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        print("[bt] yfinance not installed")
        return {t: None for t in tickers}

    out: dict[str, float | None] = {t: None for t in tickers}
    if not tickers:
        return out

    start_dt = datetime.fromisoformat(start).date()
    # pad calendar days
    end_dt = start_dt + timedelta(days=int(n_td * 2 + 7))
    try:
        data = yf.download(
            tickers=tickers,
            start=start_dt.isoformat(),
            end=(end_dt + timedelta(days=1)).isoformat(),
            group_by="ticker",
            auto_adjust=True,
            threads=True,
            progress=False,
        )
    except Exception as e:
        print(f"[bt] yfinance download failed: {e}")
        return out

    if data is None or data.empty:
        return out

    # yfinance multi-ticker columns are MultiIndex (Ticker, OHLCV) or single
    for t in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if t not in data.columns.get_level_values(0):
                    continue
                close = data[t]["Close"].dropna()
            else:
                close = data["Close"].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
            if close.empty or len(close) < 2:
                continue
            # entry = first close on/after start; exit = entry_idx + n_td
            entry_px = float(close.iloc[0])
            if len(close) > n_td:
                exit_px = float(close.iloc[n_td])
            else:
                exit_px = float(close.iloc[-1])
            if entry_px and entry_px == entry_px and entry_px != 0:
                out[t] = (exit_px / entry_px) - 1.0
        except Exception:
            continue
    return out


def grade_book(path: Path, top_n: int = 25) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    meta = data.get("meta") or {}
    books = data.get("books") or {}
    signal_date = meta.get("date") or path.name.replace("_stock_book.json", "")

    result = {
        "signal_date": signal_date,
        "horizons": {},
        "generated_at": meta.get("generated_at"),
    }

    for h, n_td in HORIZON_DAYS.items():
        block = books.get(h) or {}
        buys = (block.get("buy") or [])[:top_n]
        sells = (block.get("sell") or [])[:top_n]
        tickers = list({
            *(str(x.get("ticker") or "").upper() for x in buys),
            *(str(x.get("ticker") or "").upper() for x in sells),
        })
        tickers = [t for t in tickers if t]

        rets = _fwd_returns(tickers, signal_date, n_td)

        def side_stats(rows, side: str):
            hits = 0
            graded = 0
            pnls = []
            detail = []
            for r in rows:
                t = str(r.get("ticker") or "").upper()
                fr = rets.get(t)
                if fr is None:
                    detail.append({"ticker": t, "side": side, "fwd": None, "hit": None})
                    continue
                pnl = fr if side == "buy" else -fr
                hit = pnl > 0
                graded += 1
                hits += int(hit)
                pnls.append(pnl)
                detail.append({
                    "ticker": t,
                    "side": side,
                    "fwd": round(fr * 100, 3),
                    "pnl": round(pnl * 100, 3),
                    "hit": hit,
                })
            avg = sum(pnls) / len(pnls) if pnls else None
            return {
                "n": graded,
                "hits": hits,
                "hit_rate": round(hits / graded, 3) if graded else None,
                "avg_pnl_pct": round(avg * 100, 3) if avg is not None else None,
                "rows": detail,
            }

        bstat = side_stats(buys, "buy")
        sstat = side_stats(sells, "sell")
        # combined long-short style: average of buy pnl and sell pnl
        comb = []
        if bstat["avg_pnl_pct"] is not None:
            comb.append(bstat["avg_pnl_pct"])
        if sstat["avg_pnl_pct"] is not None:
            comb.append(sstat["avg_pnl_pct"])
        result["horizons"][h] = {
            "trading_days": n_td,
            "buy": {k: bstat[k] for k in ("n", "hits", "hit_rate", "avg_pnl_pct")},
            "sell": {k: sstat[k] for k in ("n", "hits", "hit_rate", "avg_pnl_pct")},
            "avg_pnl_pct_both": round(sum(comb) / len(comb), 3) if comb else None,
            "detail_buy": bstat["rows"][:15],
            "detail_sell": sstat["rows"][:15],
        }
    return result


def run(max_books: int = 30, top_n: int = 25) -> None:
    paths = _list_books()[-max_books:]
    if not paths:
        print("[bt] no stock_book json files — run stock_book first")
        return

    grades = []
    for p in paths:
        print(f"[bt] grading {p.name}")
        try:
            grades.append(grade_book(p, top_n=top_n))
        except Exception as e:
            print(f"[bt] fail {p.name}: {e}")

    # aggregate by horizon
    agg = {h: {"n_books": 0, "buy_hits": 0, "buy_n": 0, "sell_hits": 0, "sell_n": 0,
               "pnl_sum": 0.0, "pnl_n": 0} for h in HORIZON_DAYS}
    for g in grades:
        for h, block in (g.get("horizons") or {}).items():
            a = agg[h]
            a["n_books"] += 1
            b, s = block.get("buy") or {}, block.get("sell") or {}
            a["buy_hits"] += b.get("hits") or 0
            a["buy_n"] += b.get("n") or 0
            a["sell_hits"] += s.get("hits") or 0
            a["sell_n"] += s.get("n") or 0
            if block.get("avg_pnl_pct_both") is not None:
                a["pnl_sum"] += block["avg_pnl_pct_both"]
                a["pnl_n"] += 1

    today = datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    now = datetime.now(ZoneInfo(config.TZ)).isoformat()

    L = [
        f"# Stock book backtest — {today}",
        "",
        f"Generated: **{now}**",
        f"Books graded: **{len(grades)}** (top {top_n} buy + sell per horizon)",
        "",
        "Entry proxy: first yfinance close on/after signal date; exit ≈ N trading days later.",
        "Buy PnL = price return; Sell PnL = −price return.",
        "",
        "## Aggregate by horizon",
        "",
        "| Horizon | books | buy hit% | sell hit% | avg book pnl% |",
        "|---------|-------|----------|-----------|---------------|",
    ]
    for h, a in agg.items():
        bhr = f"{100*a['buy_hits']/a['buy_n']:.1f}%" if a["buy_n"] else "n/a"
        shr = f"{100*a['sell_hits']/a['sell_n']:.1f}%" if a["sell_n"] else "n/a"
        ap = f"{a['pnl_sum']/a['pnl_n']:+.2f}" if a["pnl_n"] else "n/a"
        L.append(f"| {h} | {a['n_books']} | {bhr} | {shr} | {ap} |")

    L += ["", "## Per signal date", ""]
    for g in grades:
        L.append(f"### Signal **{g['signal_date']}**")
        L.append("")
        L.append("| Horizon | buy hit | sell hit | avg pnl% |")
        L.append("|---------|---------|----------|----------|")
        for h, block in (g.get("horizons") or {}).items():
            b, s = block.get("buy") or {}, block.get("sell") or {}
            bh = f"{b.get('hits')}/{b.get('n')} ({b.get('hit_rate')})" if b.get("n") else "n/a"
            sh = f"{s.get('hits')}/{s.get('n')} ({s.get('hit_rate')})" if s.get("n") else "n/a"
            L.append(f"| {h} | {bh} | {sh} | {block.get('avg_pnl_pct_both')} |")
        L.append("")

    text = "\n".join(L)
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(text, encoding="utf-8")
    OUT_JSON.write_text(
        json.dumps({"generated_at": now, "aggregate": agg, "grades": grades}, indent=2),
        encoding="utf-8",
    )
    DAILY.mkdir(parents=True, exist_ok=True)
    (DAILY / f"{today}_stock_book_backtest.md").write_text(text, encoding="utf-8")
    print(f"[bt] wrote {OUT_MD}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-books", type=int, default=30)
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()
    run(max_books=args.max_books, top_n=args.top)


if __name__ == "__main__":
    main()
