"""NEWS ACTIONS GRADER — backtests every buy/sell suggestion the
news_actions stage has ever emitted.

For every 01_daily/news/<date>_actions.json file:
  * entry timing comes from the `generated_at` INSIDE the JSON (the workflow
    runs twice daily and the second run overwrites the file — the embedded
    timestamp is the only trustworthy signal time):
      - generated before 09:30 ET  -> entry = THAT trading day's open
      - generated at/after 09:30 ET -> entry = NEXT trading day's open
  * the suggestion is then tracked for up to 14 trading days (or the latest
    available close, whichever comes first).
  * buy:  profitable if any High > entry within the window;
          horizon return = close[k] / entry - 1
    sell: profitable if any Low  < entry within the window;
          horizon return = entry / close[k] - 1   (short P&L)
  * also records max favorable excursion (MFE) and the day it happened.

Outputs:
  01_daily/news/grades/<date>_grades.json   per-signal-date detail
  03_scoreboard/news_actions_scoreboard.json cumulative machine-readable board
  03_scoreboard/news_actions_report.md      human report; every section states
                                            explicitly WHICH day predictions
                                            were made, the entry day, and the
                                            grading window end.

CLI: python -m src.news_grade [--max-days N]   (N = only grade the newest N
signal dates; default grades everything)
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
NEWS_DIR = "01_daily/news"
GRADES_DIR = os.path.join(NEWS_DIR, "grades")
SCOREBOARD_DIR = "03_scoreboard"
HORIZONS = [1, 3, 5, 10, 14]          # trading days from entry
MAX_WINDOW = 14                        # trading days tracked
MARKET_OPEN = dtime(9, 30)


def _signal_dates(max_days: int | None) -> list[str]:
    files = sorted(glob.glob(os.path.join(NEWS_DIR, "*_actions.json")))
    dates = []
    for p in files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})_actions\.json$", p)
        if m:
            dates.append(m.group(1))
    if max_days:
        dates = dates[-max_days:]
    return dates


def _load_actions(date_str: str) -> dict | None:
    path = os.path.join(NEWS_DIR, f"{date_str}_actions.json")
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError) as e:
        print(f"[grade] {date_str}: cannot read actions json: {e}")
        return None


def _fetch_bars(tickers: list[str], start: str) -> dict[str, list[dict]]:
    """{ticker: [{date, open, high, low, close}, ...]} ascending."""
    import yfinance as yf
    out: dict[str, list[dict]] = {t: [] for t in tickers}
    if not tickers:
        return out
    try:
        data = yf.download(tickers, start=start, interval="1d",
                           group_by="ticker", auto_adjust=False,
                           progress=False, threads=True)
    except Exception as e:  # noqa: BLE001
        print(f"[grade] yfinance batch download failed: {e}")
        return out
    if data.empty:
        return out
    single = len(tickers) == 1
    for t in tickers:
        try:
            df = data[t] if not single else data
        except KeyError:
            continue
        rows = []
        for idx, r in df.iterrows():
            try:
                o, h, l, c = float(r["Open"]), float(r["High"]), \
                    float(r["Low"]), float(r["Close"])
            except (TypeError, ValueError, KeyError):
                continue
            if any(v != v for v in (o, h, l, c)):  # NaN guard
                continue
            rows.append({"date": idx.date().isoformat(),
                         "open": o, "high": h, "low": l, "close": c})
        out[t] = rows
    return out


def _grade_one(gen_dt: datetime, side: str, bars: list[dict]) -> dict | None:
    """Grade a single suggestion against that ticker's daily bars."""
    gen_date = gen_dt.date().isoformat()
    premarket = gen_dt.time() < MARKET_OPEN
    entry_idx = None
    for i, b in enumerate(bars):
        if (b["date"] >= gen_date) if premarket else (b["date"] > gen_date):
            entry_idx = i
            break
    if entry_idx is None:
        return None  # no trading day on/after signal yet
    entry = bars[entry_idx]
    window = bars[entry_idx:entry_idx + MAX_WINDOW + 1]
    entry_px = entry["open"]
    if entry_px <= 0:
        return None

    is_buy = side == "buy"
    def ret(close_px: float) -> float:
        return (close_px / entry_px - 1) if is_buy else (entry_px / close_px - 1)

    returns = {}
    for h in HORIZONS:
        if len(window) > h:
            returns[f"{h}d"] = round(ret(window[h]["close"]) * 100, 2)

    mfe = 0.0
    mfe_day = None
    ever_profitable = False
    first_prof_day = None
    for b in window:
        fav = ((b["high"] / entry_px - 1) if is_buy
               else (entry_px / b["low"] - 1))
        if fav > mfe:
            mfe, mfe_day = fav, b["date"]
        if fav > 0 and not ever_profitable:
            ever_profitable = True
            first_prof_day = b["date"]

    last = window[-1]
    return {
        "entry_date": entry["date"],
        "entry_open": round(entry_px, 4),
        "window_through": last["date"],
        "trading_days_elapsed": len(window) - 1,
        "complete": len(window) > MAX_WINDOW,
        "latest_close": round(last["close"], 4),
        "current_return_pct": round(ret(last["close"]) * 100, 2),
        "horizon_returns_pct": returns,
        "mfe_pct": round(mfe * 100, 2),
        "mfe_day": mfe_day,
        "ever_profitable": ever_profitable,
        "first_profitable_day": first_prof_day,
    }


def _summarize(graded: list[dict]) -> dict:
    def wins(key_fn, rows):
        rows = [r for r in rows if key_fn(r) is not None]
        if not rows:
            return None
        w = sum(1 for r in rows if key_fn(r) > 0)
        return {"n": len(rows), "wins": w,
                "win_rate": round(w / len(rows) * 100, 1),
                "avg": round(sum(key_fn(r) for r in rows) / len(rows), 2)}

    out = {"n_suggestions": len(graded),
           "ever_profitable": wins(lambda r: r["mfe_pct"],
                                   [r for r in graded
                                    if r.get("ever_profitable") is not None]),
           }
    # ever_profitable as its own rate
    ep = [r for r in graded if r.get("ever_profitable") is not None]
    if ep:
        w = sum(1 for r in ep if r["ever_profitable"])
        out["ever_profitable"] = {"n": len(ep), "wins": w,
                                  "win_rate": round(w / len(ep) * 100, 1)}
    for h in HORIZONS:
        out[f"close_{h}d"] = wins(
            lambda r, h=h: r["horizon_returns_pct"].get(f"{h}d"), graded)
    for side in ("buy", "sell"):
        rows = [r for r in graded if r["side"] == side]
        out[f"side_{side}"] = {
            "n": len(rows),
            "ever_profitable": wins(
                lambda r: r["mfe_pct"] if r["ever_profitable"] else None,
                rows),
            "close_5d": wins(lambda r: r["horizon_returns_pct"].get("5d"),
                             rows),
        }
    return out


def _report(all_rows: list[dict], by_date: dict[str, list[dict]]) -> str:
    L = ["# News Actions — Backtest Report", "",
         f"_Generated {datetime.now(ET).strftime('%Y-%m-%d %H:%M %Z')}_", "",
         "How to read this: each section is one prediction day. "
         "**Entry** is the open of the first trading day at/after the "
         "signal timestamp (same day if the signal ran pre-market, next day "
         "otherwise). A buy is `ever ✓` if price traded ABOVE the entry open "
         "at any point within the tracked window (up to 14 trading days); "
         "a sell/short is `ever ✓` if price traded BELOW it. "
         "`now%` is the return at the latest close in the window.", ""]
    total = _summarize(all_rows)
    L += ["## Overall", "",
          f"- suggestions graded: **{total['n_suggestions']}**",
          f"- ever profitable within window: "
          f"**{total['ever_profitable']['win_rate']}%** "
          f"({total['ever_profitable']['wins']}/{total['ever_profitable']['n']})"
          if total.get("ever_profitable") else "- no gradable suggestions yet",
          ""]
    for h in HORIZONS:
        s = total.get(f"close_{h}d")
        if s:
            L.append(f"- close @ {h} trading days: win rate "
                     f"**{s['win_rate']}%** ({s['wins']}/{s['n']}), "
                     f"avg {s['avg']}%")
    for side in ("buy", "sell"):
        s = total.get(f"side_{side}")
        if s and s["n"]:
            ep = s["ever_profitable"] or {}
            c5 = s["close_5d"] or {}
            L.append(f"- **{side}**: n={s['n']}, ever-profitable "
                     f"{ep.get('win_rate', '?')}%, "
                     f"5d close win rate {c5.get('win_rate', '?')}%")
    L.append("")

    for date_str in sorted(by_date, reverse=True):
        rows = by_date[date_str]
        if not rows:
            continue
        gen = rows[0]["generated_at"]
        entry_dates = sorted({r["entry_date"] for r in rows})
        through = max(r["window_through"] for r in rows)
        s = _summarize(rows)
        L += [f"## Predictions made {date_str} "
              f"(signal {gen} ET)", "",
              f"Entry: **{entry_dates[0]}** open"
              + (f" (some {entry_dates[-1]})" if len(entry_dates) > 1 else "")
              + f" — window through **{through}**"
              + ("" if all(r["complete"] for r in rows) else
                 " (still open)"),
              f"- {s['n_suggestions']} suggestions; ever-profitable "
              f"**{s['ever_profitable']['win_rate']}%**",
              ""]
        L.append("| ticker | side | net | entry | entry date | now% | "
                 "MFE% (day) | ever |")
        L.append("|---|---|---|---|---|---|---|---|")
        for r in sorted(rows, key=lambda r: -abs(r["net"])):
            L.append(
                f"| {r['ticker']} | {r['side']} | {r['net']:+.1f} | "
                f"{r['entry_open']} | {r['entry_date']} | "
                f"{r['current_return_pct']:+.2f} | "
                f"{r['mfe_pct']:+.2f} ({r['mfe_day']}) | "
                f"{'✓' if r['ever_profitable'] else '✗'} |")
        L.append("")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-days", type=int, default=None)
    args = ap.parse_args()

    dates = _signal_dates(args.max_days)
    if not dates:
        print("[grade] no actions files found")
        return
    print(f"[grade] signal dates: {', '.join(dates)}")

    # collect every suggestion first so we can batch-download prices
    pending = []  # (date_str, generated_at_dt, action dict)
    tickers = set()
    for d in dates:
        payload = _load_actions(d)
        if not payload:
            continue
        gen_raw = payload.get("generated_at")
        try:
            gen_dt = datetime.fromisoformat(gen_raw).astimezone(ET)
        except (TypeError, ValueError):
            print(f"[grade] {d}: bad generated_at {gen_raw!r} — skipped")
            continue
        for a in payload.get("ticker_actions") or []:
            if a.get("side") not in ("buy", "sell"):
                continue
            pending.append((d, gen_dt, a))
            tickers.add(a["ticker"])

    if not pending:
        print("[grade] nothing to grade")
        return

    start = min(g.date().isoformat() for _, g, _ in pending)
    print(f"[grade] fetching prices for {len(tickers)} tickers since {start}")
    bars = _fetch_bars(sorted(tickers), start)

    by_date: dict[str, list[dict]] = {}
    all_rows: list[dict] = []
    for d, gen_dt, a in pending:
        g = _grade_one(gen_dt, a["side"], bars.get(a["ticker"], []))
        if g is None:
            print(f"[grade] {d} {a['ticker']}: no entry bar yet")
            continue
        row = {"signal_date": d,
               "generated_at": gen_dt.strftime("%Y-%m-%d %H:%M"),
               "ticker": a["ticker"], "side": a["side"],
               "net": a.get("net", 0),
               "events": [e.get("event") for e in a.get("events", [])],
               **g}
        by_date.setdefault(d, []).append(row)
        all_rows.append(row)

    os.makedirs(GRADES_DIR, exist_ok=True)
    os.makedirs(SCOREBOARD_DIR, exist_ok=True)
    for d, rows in by_date.items():
        with open(os.path.join(GRADES_DIR, f"{d}_grades.json"), "w",
                  encoding="utf-8") as fh:
            json.dump({"signal_date": d, "suggestions": rows,
                       "summary": _summarize(rows)}, fh, indent=2)

    board = {"updated_at": datetime.now(ET).isoformat(),
             "grading_rule": {
                 "entry": "open of first trading day at/after generated_at "
                          "(same day if signal < 09:30 ET, else next day)",
                 "window_trading_days": MAX_WINDOW,
                 "buy_profitable_if": "any High > entry open within window",
                 "sell_profitable_if": "any Low < entry open within window"},
             "suggestions": all_rows,
             "summary": _summarize(all_rows)}
    with open(os.path.join(SCOREBOARD_DIR, "news_actions_scoreboard.json"),
              "w", encoding="utf-8") as fh:
        json.dump(board, fh, indent=2)

    report = _report(all_rows, by_date)
    with open(os.path.join(SCOREBOARD_DIR, "news_actions_report.md"), "w",
              encoding="utf-8") as fh:
        fh.write(report + "\n")
    print(f"[grade] graded {len(all_rows)} suggestions across "
          f"{len(by_date)} signal dates -> "
          f"{SCOREBOARD_DIR}/news_actions_report.md")


if __name__ == "__main__":
    main()
