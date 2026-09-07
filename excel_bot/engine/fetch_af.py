"""Fetch Yahoo OHLCV into excel_bot/data/rows — the A–F / STOCKHISTORY seed.

Never uses run.py --from-cache (Excel frozen snapshot).

  python engine/fetch_af.py --refresh          # last 14d merge, all cached tickers
  python engine/fetch_af.py --spy              # SPY tape for regimes
  python engine/fetch_af.py --extend-days 500  # stitch older history
  python engine/fetch_af.py --limit 50         # smoke
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fastfetch import fetch_daily_fast  # noqa: E402

ROWS = Path(__file__).resolve().parent.parent / "data" / "rows"
FETCH_BACK = 14


def _ser(rows):
    return [{**r, "date": r["date"].isoformat()} for r in rows]


def _deser(rows):
    out = []
    for r in rows:
        d = r["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d[:10])
        out.append({**r, "date": d})
    return out


def load_rows(ticker):
    p = ROWS / f"{ticker}.json"
    if not p.exists():
        return []
    return _deser(json.loads(p.read_text()))


def save_rows(ticker, rows):
    ROWS.mkdir(parents=True, exist_ok=True)
    by = {}
    for r in rows:
        by[r["date"]] = r
    ordered = [by[k] for k in sorted(by)]
    (ROWS / f"{ticker}.json").write_text(json.dumps(_ser(ordered)))
    return len(ordered)


def merge_fetch(ticker, start, end):
    existing = load_rows(ticker)
    by = {r["date"]: r for r in existing}
    fetched = fetch_daily_fast(ticker, start, end)
    for r in fetched:
        by[r["date"]] = r
    n = save_rows(ticker, list(by.values()))
    return ticker, n, None


def _job(ticker, start, end):
    try:
        return merge_fetch(ticker, start, end)
    except Exception as e:  # noqa: BLE001
        return ticker, 0, f"{type(e).__name__}: {e}"[:160]


def tickers_on_disk():
    return sorted(p.stem for p in ROWS.glob("*.json") if p.stem != "_failed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--spy", action="store_true")
    ap.add_argument("--extend-days", type=int, default=0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--tickers", nargs="*", default=[])
    args = ap.parse_args()

    today = date.today()
    names = args.tickers or tickers_on_disk()
    if args.spy and "SPY" not in names:
        names = ["SPY"] + names
    if args.limit:
        names = names[: args.limit]
    if not names:
        raise SystemExit("no tickers — restore excel-state rows first")

    jobs = []
    if args.refresh or not args.extend_days:
        start = today - timedelta(days=FETCH_BACK)
        if args.extend_days:
            start = today - timedelta(days=args.extend_days)
        end = today
    else:
        start = today - timedelta(days=args.extend_days)
        end = today

    # When only --spy, still allow a lone SPY job.
    if args.spy and not args.refresh and not args.extend_days and not args.tickers:
        names = ["SPY"]
        start = today - timedelta(days=max(args.extend_days, 750))
        end = today

    t0 = time.time()
    ok = err = 0
    print(f"fetch A–F Yahoo {start} → {end}  n={len(names)} workers={args.workers}",
          flush=True)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = [pool.submit(_job, t, start, end) for t in names]
        for i, fut in enumerate(as_completed(futs), 1):
            t, n, e = fut.result()
            if e:
                err += 1
                if err <= 12:
                    print(f"  FAIL {t}: {e}", flush=True)
            else:
                ok += 1
            if i % 200 == 0 or i == len(futs):
                print(f"  {i}/{len(futs)} ok={ok} err={err}  {t} days={n}",
                      flush=True)
    print(f"done ok={ok} err={err} in {time.time()-t0:.1f}s  dir={ROWS}")


if __name__ == "__main__":
    main()
