"""Historical backfill: run the EXACT daily-signal logic over past days.

For every ticker:
  1. FETCH  deep history via Yahoo v8 (same datasource the daily bot uses;
            NOT persisted -- re-fetched per run, ~0.3s/ticker)
  2. ENGINE rebuild grids over the FULL span (anchor tiling, STEP=120, same
            equations/dependencies/colors as Excel; colors for a day use
            only data available that day -- see NOTES.md timing verdict)
  3. SIGNALS for each validated strategy, a trade signal exists on every
            historical day D where a cluster's confirmation day == D
            (entry_idx == i), identical to daily_run.find_signals but for
            every day index instead of only the latest
  4. EVAL   entry at confirmation-day CLOSE (strategy card entry rule);
            also records next-day OPEN for reference. Exit per the
            strategy's exit_rule (flip / holdN / tpX / trailY), shorts
            side-adjusted. Outcome uses future prices -- that IS the
            backtest measurement; the signal decision itself is causal.

Resume: done tickers are tracked in backtest/state/done.json; re-running
skips them. Trades append to backtest/trades.csv (one row per trade).

Usage:
  python engine/backfill.py --years 5 --limit 20          # smoke test
  python engine/backfill.py --years 5 --workers 4 --budget-min 300
"""
import argparse
import csv
import glob
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fastfetch import fetch_daily_fast
from backtest import build_ticker, pick_anchors
from sweep import days_features, detect_def
from cohort_analysis import load_finviz, cohorts_of
from cards import color_name, s2d
from daily_run import DEFS, load_strategies, parse_date

BT_DIR = "backtest"
TRADES_CSV = os.path.join(BT_DIR, "trades.csv")
DONE_JSON = os.path.join(BT_DIR, "state", "done.json")
DONE_GRIDS_JSON = os.path.join(BT_DIR, "state", "done_grids.json")
GRIDS_DEEP_DIR = "grids_deep"

TRADE_FIELDS = [
    "ticker", "strategy", "side", "signal_date", "cluster_start",
    "cluster_len_days", "entry_close", "next_open", "exit_date",
    "exit_price", "exit_reason", "ret_close_entry", "ret_open_entry",
    "hold_days", "cohorts", "signal_colors",
]


# ---------------------------------------------------------------- exit sim --
def eval_exit(days, ei, side, rule):
    """Exit outcome for a signal confirmed at day index ei.
    Returns (exit_idx, exit_price, reason) or None if unevaluable."""
    n = len(days)
    entry = days[ei]["close"]
    if not entry:
        return None
    closes = [d["close"] for d in days]

    if rule == "flip":
        xi = None
        for k in range(ei + 1, n):   # first opposite-family confirmation is
            break                    # handled by caller via cluster exit_idx
        return None                  # 'flip' needs the cluster's exit_idx

    if rule.startswith("hold"):
        hold_n = int(rule[4:])
        k = min(ei + hold_n, n - 1)
        return k, closes[k], "hold" if ei + hold_n < n else "hold_end"

    if rule.startswith("tp"):
        x = int(rule[2:]) / 100.0
        tgt = entry * (1 + x * side)          # long: above; short: below
        for k in range(ei + 1, n):
            px = days[k]["high"] if side == 1 else days[k]["low"]
            if px is None:
                continue
            if (side == 1 and px >= tgt) or (side == -1 and px <= tgt):
                return k, tgt, "tp_hit"
        return n - 1, closes[n - 1], "tp_miss_end"

    if rule.startswith("trail"):
        y = int(rule[5:]) / 100.0
        peak = entry
        for k in range(ei + 1, n):
            c = closes[k]
            if not c:
                continue
            peak = max(peak, c) if side == 1 else min(peak, c)
            if side == 1 and c <= peak * (1 - y):
                return k, c, "trail"
            if side == -1 and c >= peak * (1 + y):
                return k, c, "trail"
        return n - 1, closes[n - 1], "trail_end"

    raise ValueError(f"unknown exit rule {rule}")


def ticker_trades(ticker, rows, strategies, fz_rec):
    """Build full-span grid, emit every historical signal for `strategies`."""
    earliest = rows[0]["date"] + timedelta(days=600)   # weekly lookback warm-up
    anchors = pick_anchors(rows, earliest, rows[-1]["date"])
    if not anchors:
        return [], "no anchors"
    days = build_ticker(ticker, rows, anchors)
    if len(days) < 30:
        return [], "thin grid"
    feats = days_features(days)
    closes = [d["close"] for d in feats]
    opens = [d["open"] for d in feats]
    cohorts = {"ALL"} | set(cohorts_of(fz_rec) if fz_rec else [])
    n = len(feats)
    out = []
    for st in strategies:
        if st["cohort"] not in cohorts:
            continue
        for c in detect_def(feats, st["definition"]):
            if c["side"] != st["side"]:
                continue
            ei = c.get("entry_idx")
            if ei is None or ei >= n or not closes[ei]:
                continue
            rule = st["exit_rule"]
            if rule == "flip":
                xi = c.get("exit_idx")
                if xi is None or xi >= n or not closes[xi]:
                    continue
                ex_i, ex_px, reason = xi, closes[xi], "flip"
            else:
                # hold/tp/trail are also bounded by the cluster flip if known
                res = eval_exit(feats, ei, st["side"], rule)
                if res is None:
                    continue
                ex_i, ex_px, reason = res
                xi = c.get("exit_idx")
                if xi is not None and xi < ex_i and closes[xi]:
                    ex_i, ex_px, reason = xi, closes[xi], "flip_first"
            if not ex_px:
                continue
            entry = closes[ei]
            ret_c = (ex_px - entry) / entry * st["side"]
            no = opens[ei + 1] if ei + 1 < n else None
            ret_o = ((ex_px - no) / no * st["side"]
                     if no and ex_i > ei else None)
            d_e = feats[ei]
            out.append({
                "ticker": ticker,
                "strategy": st["name"],
                "side": "LONG" if st["side"] == 1 else "SHORT",
                "signal_date": str(s2d(d_e["date"])),
                "cluster_start": str(s2d(feats[c["start"]]["date"])),
                "cluster_len_days": c["end"] - c["start"],
                "entry_close": f"{entry:.4f}",
                "next_open": f"{no:.4f}" if no else "",
                "exit_date": str(s2d(feats[ex_i]["date"])),
                "exit_price": f"{ex_px:.4f}",
                "exit_reason": reason,
                "ret_close_entry": f"{ret_c*100:+.2f}%",
                "ret_open_entry": (f"{ret_o*100:+.2f}%"
                                   if ret_o is not None else ""),
                "hold_days": (s2d(feats[ex_i]["date"]) -
                              s2d(d_e["date"])).days,
                "cohorts": "|".join(sorted(cohorts - {"ALL"})),
                "signal_colors": "|".join(color_name(f) for f in d_e["fills"]),
            })
    return out, None


def grid_for(ticker, rows):
    """Full-span color grid for a ticker's deep rows."""
    earliest = rows[0]["date"] + timedelta(days=600)   # weekly lookback warm-up
    anchors = pick_anchors(rows, earliest, rows[-1]["date"])
    if not anchors:
        return None
    days = build_ticker(ticker, rows, anchors)
    return days or None


def _work_grids(job):
    """Grids-only mode: fetch deep, build full-span grid, save it. No trades."""
    ticker, years = job
    try:
        rows = fetch_daily_fast(ticker, date.today() - timedelta(days=years * 365),
                                date.today())
        if len(rows) < 200:
            return ticker, "insufficient history"
        days = grid_for(ticker, rows)
        if not days or len(days) < 30:
            return ticker, "thin grid"
        os.makedirs(GRIDS_DEEP_DIR, exist_ok=True)
        json.dump({"ticker": ticker, "days": days},
                  open(os.path.join(GRIDS_DEEP_DIR, f"{ticker}.json"), "w"))
        return ticker, None
    except Exception as e:  # noqa: BLE001
        return ticker, f"{type(e).__name__}: {e}"[:150]


def main_grids_only(args):
    """Build + persist deep grids for the whole universe (resumable)."""
    t0 = time.time()
    tickers = sorted({os.path.basename(p)[:-5]
                      for p in glob.glob("data/rows/*.json")}
                     | {os.path.basename(p)[:-5]
                        for p in glob.glob("grids/*.json")
                        if not os.path.basename(p).startswith("_")})
    if args.limit:
        tickers = tickers[:args.limit]
    done = {} if args.no_resume else (
        json.load(open(DONE_GRIDS_JSON)) if os.path.exists(DONE_GRIDS_JSON)
        else {})
    todo = [t for t in tickers if t not in done]
    print(f"[grids-only] {len(done)} done, {len(todo)} to go", flush=True)
    if not todo:
        print("REMAINING=0", flush=True)
        return
    from multiprocessing import Pool
    budget_s = args.budget_min * 60
    with Pool(args.workers) as pool:
        for tk, err in pool.imap_unordered(
                _work_grids, [(t, args.years) for t in todo], chunksize=4):
            done[tk] = f"ERR {err}" if err else "ok"
            if len(done) % 100 == 0:
                os.makedirs(os.path.dirname(DONE_GRIDS_JSON), exist_ok=True)
                json.dump(done, open(DONE_GRIDS_JSON, "w"))
                print(f"  ... {len(done)}/{len(tickers)} "
                      f"({(time.time()-t0)/60:.1f}m)", flush=True)
            if time.time() - t0 > budget_s:
                pool.terminate()
                print("[budget] stopping", flush=True)
                break
    os.makedirs(os.path.dirname(DONE_GRIDS_JSON), exist_ok=True)
    json.dump(done, open(DONE_GRIDS_JSON, "w"))
    remaining = len([t for t in tickers if t not in done])
    print(f"[grids-only done] {remaining} remaining, "
          f"{(time.time()-t0)/60:.1f}m", flush=True)
    print(f"REMAINING={remaining}", flush=True)



def load_done():
    if os.path.exists(DONE_JSON):
        return json.load(open(DONE_JSON))
    return {}


def save_done(done):
    os.makedirs(os.path.dirname(DONE_JSON), exist_ok=True)
    json.dump(done, open(DONE_JSON, "w"))


def _work(job):
    ticker, years = job
    try:
        rows = fetch_daily_fast(ticker, date.today() - timedelta(days=years * 365),
                                date.today())
        if len(rows) < 200:
            return ticker, [], "insufficient history"
        fz = _work._fz
        strategies = _work._strategies
        trades, err = ticker_trades(ticker, rows, strategies, fz.get(ticker))
        return ticker, trades, err
    except Exception as e:  # noqa: BLE001
        return ticker, [], f"{type(e).__name__}: {e}"[:150]


def _init_child(years, strategies):
    _work._fz = load_finviz()
    _work._strategies = strategies


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=5,
                    help="deep fetch window; colors start ~600d after this")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    ap.add_argument("--budget-min", type=float, default=1e9,
                    help="stop launching new tickers after this many minutes")
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--grids-only", action="store_true",
                    help="build+persist deep grids only, no trades")
    args = ap.parse_args()
    if args.grids_only:
        main_grids_only(args)
        return

    t0 = time.time()
    strategies = load_strategies()
    print(f"[setup] {len(strategies)} strategies, {args.years}y window",
          flush=True)

    tickers = sorted({os.path.basename(p)[:-5]
                      for p in glob.glob("data/rows/*.json")}
                     | {os.path.basename(p)[:-5]
                        for p in glob.glob("grids/*.json") if
                        not os.path.basename(p).startswith("_")})
    if args.limit:
        tickers = tickers[:args.limit]
    done = {} if args.no_resume else load_done()
    todo = [t for t in tickers if t not in done]
    print(f"[resume] {len(done)} done, {len(todo)} to go", flush=True)
    if not todo:
        print("COMPLETE")
        return

    os.makedirs(BT_DIR, exist_ok=True)
    new_trades = 0
    write_header = not os.path.exists(TRADES_CSV) or args.no_resume
    fh = open(TRADES_CSV, "a", newline="", encoding="utf-8")
    w = csv.DictWriter(fh, fieldnames=TRADE_FIELDS)
    if write_header:
        w.writeheader()

    from multiprocessing import Pool
    budget_s = args.budget_min * 60
    with Pool(args.workers, initializer=_init_child,
              initargs=(args.years, strategies)) as pool:
        for tk, trades, err in pool.imap_unordered(
                _work, [(t, args.years) for t in todo], chunksize=4):
            if err:
                done[tk] = f"ERR {err}"
            else:
                done[tk] = f"ok trades={len(trades)}"
                for tr in trades:
                    w.writerow(tr)
                new_trades += len(trades)
            nd = len(done)
            if nd % 50 == 0:
                fh.flush()
                save_done(done)
                print(f"  ... {nd}/{len(tickers)} "
                      f"({(time.time()-t0)/60:.1f}m, +{new_trades} trades)",
                      flush=True)
            if time.time() - t0 > budget_s:
                pool.terminate()
                print(f"[budget] stopping at {nd}/{len(tickers)}", flush=True)
                break
    fh.close()
    save_done(done)
    remaining = len([t for t in tickers if t not in done])
    print(f"[done] +{new_trades} trades, {remaining} tickers remaining, "
          f"{(time.time()-t0)/60:.1f}m", flush=True)
    # machine-readable line for the workflow's self-continuation
    print(f"REMAINING={remaining}", flush=True)


if __name__ == "__main__":
    main()
