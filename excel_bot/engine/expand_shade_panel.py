"""Build the max Yahoo / excel_bot rows panel and paint real column-M fills.

Prior shade KEEP on ~4.4k fires used a 1000-grid cut and indexed
OPEN_FILL into the A–O fill array, so letter M read column H. This
rebuilds ALL split/universe tickers × ALL dump history, paints M from
the locked CF (IZ=1 → #95CA82), and writes rows_cache grids the shade
miner can score.

  python3 engine/expand_shade_panel.py
  python3 engine/expand_shade_panel.py --excel-validate 8
  python3 engine/expand_shade_panel.py --yahoo-fill

Does not fetch live flatten_robust. Does not write strategies/.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from multiprocessing import Pool

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest import pick_anchors  # noqa: E402
from fastfetch import fetch_daily_fast  # noqa: E402
from stockhistory import serial  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
ROWS = os.environ.get("ROWS_DIR", os.path.join(ROOT, "data", "rows"))
GRIDS = os.environ.get("GRIDS_DIR", os.path.join(ROOT, "grids"))
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
MODEL = os.path.join(HERE, "model.json")
PARQUET = os.path.join(REPO, "data", "prices", "ohlc.parquet")
SPY_PATH = os.path.join(ROOT, "research", "spy_tape.json")
STATS = os.path.join(ROOT, "research", "shade_panel_stats.json")

M_IDX = 12
HEX_MID = "95CA82"
HEX_ORANGE = "F2AA84"


def load_iy():
    ext = json.load(open(MODEL))["external"]
    out = {}
    for k, v in ext.items():
        if k.startswith("VIX!IQ"):
            try:
                out[int(k[6:])] = int(v)
            except (TypeError, ValueError):
                continue
    return out


def deser_rows(raw):
    out = []
    for r in raw:
        d = r["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d[:10])
        elif hasattr(d, "date"):
            d = d.date() if not isinstance(d, date) else d
        o, h, l, c = r.get("open"), r.get("high"), r.get("low"), r.get("close")
        if o is None or c is None:
            continue
        out.append({
            "date": d,
            "open": float(o),
            "high": float(h) if h is not None else float(c),
            "low": float(l) if l is not None else float(c),
            "close": float(c),
            "volume": float(r.get("volume") or 0),
        })
    out.sort(key=lambda x: x["date"])
    # last write wins on duplicate dates
    by = {x["date"]: x for x in out}
    return [by[k] for k in sorted(by)]


def merge_rows(*lists):
    by = {}
    for rows in lists:
        for r in rows:
            by[r["date"]] = r
    return [by[k] for k in sorted(by)]


def write_rows(ticker, rows):
    os.makedirs(ROWS, exist_ok=True)
    payload = [{
        "date": r["date"].isoformat(),
        "open": r["open"],
        "close": r["close"],
        "high": r["high"],
        "low": r["low"],
        "volume": r["volume"],
    } for r in rows]
    json.dump(payload, open(os.path.join(ROWS, f"{ticker}.json"), "w"))
    return len(payload)


def load_ticker_rows(ticker):
    p = os.path.join(ROWS, f"{ticker}.json")
    if not os.path.exists(p):
        return []
    return deser_rows(json.load(open(p)))


def parquet_by_ticker(want):
    """Return {ticker: [rows]} for tickers in want from ohlc.parquet."""
    import pandas as pd
    if not os.path.exists(PARQUET):
        return {}
    df = pd.read_parquet(PARQUET)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    want = {t.upper() for t in want}
    df = df[df["ticker"].isin(want)]
    out = defaultdict(list)
    for rec in df.itertuples(index=False):
        try:
            out[str(rec.ticker).upper()].append({
                "date": date.fromisoformat(str(rec.date)[:10]),
                "open": float(rec.open),
                "high": float(rec.high),
                "low": float(rec.low),
                "close": float(rec.close),
                "volume": float(rec.volume or 0),
            })
        except (TypeError, ValueError):
            continue
    return {t: deser_rows(rows) for t, rows in out.items()}


def yahoo_one(job):
    ticker, start, end = job
    try:
        rows = fetch_daily_fast(ticker, start, end)
        return ticker, deser_rows(rows), None
    except Exception as e:  # noqa: BLE001
        return ticker, [], f"{type(e).__name__}: {e}"[:160]


def paint_m(rows, iy):
    """Excel-faithful M fill: IZ_r from H_{r-1} and IY_{r-1}.

    STOCKHISTORY window is [anchor-200d, anchor], oldest first, row 2 =
    first day. CF M10:M142. Same tiling as backtest.build_ticker.
    """
    if len(rows) < 40:
        return []
    anchors = pick_anchors(rows, rows[0]["date"], rows[-1]["date"])
    if not anchors:
        return []
    by_date = {}
    for k, anchor in enumerate(anchors):
        cutoff = anchors[k - 1] if k > 0 else None
        start_d = anchor - timedelta(days=200)
        window = [r for r in rows if start_d <= r["date"] <= anchor]
        for i, r in enumerate(window):
            excel_row = i + 2  # row 2 = window[0]
            if excel_row > 145:
                break
            if cutoff is not None and r["date"] <= cutoff:
                continue
            fills = [None] * 15
            if excel_row >= 10 and i >= 1:
                prev = window[i - 1]
                prev_h = ((prev["close"] - prev["open"]) / prev["open"]
                          if prev["open"] else None)
                prev_iy = iy.get(excel_row - 1, 0)
                iz = 0
                if prev_h is not None:
                    if prev_iy != 1 and prev_h < -0.03:
                        iz = -1
                    elif prev_h > 0 and prev_iy == 1:
                        iz = 1
                if iz == 1:
                    fills[M_IDX] = HEX_MID
                elif iz == -1:
                    fills[M_IDX] = HEX_ORANGE
            by_date[r["date"]] = {
                "date": serial(r["date"]),
                "open": r["open"],
                "close": r["close"],
                "high": r["high"],
                "low": r["low"],
                "volume": r["volume"],
                "fills": fills,
            }
    return [by_date[d] for d in sorted(by_date)]


def write_grid(ticker, days):
    os.makedirs(GRIDS, exist_ok=True)
    json.dump(
        {"ticker": ticker, "days": days, "source": "rows_cache",
         "seed": "yahoo_rows_cache", "m_paint": "iz_cf"},
        open(os.path.join(GRIDS, f"{ticker}.json"), "w"),
    )
    return len(days)


def _paint_job(job):
    ticker, iy = job
    rows = load_ticker_rows(ticker)
    days = paint_m(rows, iy)
    if not days:
        return ticker, 0, "no_days"
    return ticker, write_grid(ticker, days), None


def extend_spy(rows):
    """Rewrite spy_tape.json from SPY rows (close series)."""
    days = [{"date": r["date"].isoformat(), "close": r["close"]} for r in rows]
    days.sort(key=lambda x: x["date"])
    json.dump(days, open(SPY_PATH, "w"))
    return len(days), days[0]["date"] if days else None, days[-1]["date"] if days else None


def panel_stats(tickers):
    n = 0
    d0 = d1 = None
    lens = []
    for t in tickers:
        rows = load_ticker_rows(t)
        if not rows:
            continue
        n += 1
        lens.append(len(rows))
        a, b = rows[0]["date"], rows[-1]["date"]
        d0 = a if d0 is None or a < d0 else d0
        d1 = b if d1 is None or b > d1 else d1
    return {
        "n_tickers_with_rows": n,
        "n_requested": len(tickers),
        "date_min": d0.isoformat() if d0 else None,
        "date_max": d1.isoformat() if d1 else None,
        "median_sessions": sorted(lens)[len(lens) // 2] if lens else 0,
        "mean_sessions": (sum(lens) / len(lens)) if lens else 0,
        "min_sessions": min(lens) if lens else 0,
        "max_sessions": max(lens) if lens else 0,
        "name_days": sum(lens),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--yahoo-fill", action="store_true",
                    help="Yahoo-fetch missing split tickers + extend SPY")
    ap.add_argument("--yahoo-max-years", type=int, default=8)
    ap.add_argument("--excel-validate", type=int, default=0)
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    ap.add_argument("--paint-only", action="store_true")
    args = ap.parse_args()

    split = json.load(open(SPLIT_PATH))
    want = sorted(set(split["discovery"]) | set(split["holdout"]) | {"SPY"})
    existing = {fn[:-5] for fn in os.listdir(ROWS) if fn.endswith(".json")}
    print(f"split+SPY {len(want)} already_on_disk {len(existing & set(want))}",
          flush=True)

    if not args.paint_only:
        print("merge parquet…", flush=True)
        pq = parquet_by_ticker(want)
        print(f"  parquet tickers {len(pq)}", flush=True)
        n_merge = 0
        for t in want:
            old = load_ticker_rows(t)
            extra = pq.get(t) or []
            if not old and not extra:
                continue
            merged = merge_rows(extra, old)  # excel-state / disk wins last
            if len(merged) != len(old) or (old and merged[-1]["date"] != old[-1]["date"]):
                write_rows(t, merged)
                n_merge += 1
            elif not old and extra:
                write_rows(t, extra)
                n_merge += 1
        print(f"  wrote/extended {n_merge}", flush=True)

        if args.yahoo_fill:
            end = date.today()
            start = end - timedelta(days=365 * args.yahoo_max_years)
            # Every split name + SPY — dumps first, Yahoo fills holes and
            # extends past parquet / excel-state.
            jobs = [(t, start, end) for t in want]
            print(f"yahoo {len(jobs)} tickers max {args.yahoo_max_years}y "
                  f"{start}→{end}", flush=True)
            ok = err = 0
            with Pool(args.workers) as pool:
                for i, (t, rows, e) in enumerate(
                        pool.imap_unordered(yahoo_one, jobs, chunksize=8), 1):
                    if e or len(rows) < 20:
                        err += 1
                    else:
                        write_rows(t, merge_rows(load_ticker_rows(t), rows))
                        ok += 1
                    if i % 100 == 0:
                        print(f"  yahoo … {i}/{len(jobs)} ok={ok} err={err}",
                              flush=True)
            print(f"  yahoo done ok={ok} err={err}", flush=True)

        spy = load_ticker_rows("SPY")
        if len(spy) >= 40:
            n, a, b = extend_spy(spy)
            print(f"spy tape {n} {a} → {b}", flush=True)

    iy = load_iy()
    have = sorted(t for t in want if t != "SPY" and len(load_ticker_rows(t)) >= 40)
    print(f"paint M on {len(have)} tickers workers={args.workers}", flush=True)
    jobs = [(t, iy) for t in have]
    ok = err = 0
    n_days = 0
    if args.workers > 1:
        with Pool(args.workers) as pool:
            for i, (t, n, e) in enumerate(
                    pool.imap_unordered(_paint_job, jobs, chunksize=8), 1):
                if e:
                    err += 1
                else:
                    ok += 1
                    n_days += n
                if i % 400 == 0:
                    print(f"  paint … {i}/{len(jobs)} ok={ok} err={err}",
                          flush=True)
    else:
        for t, iy_ in jobs:
            _, n, e = _paint_job((t, iy_))
            if e:
                err += 1
            else:
                ok += 1
                n_days += n
    print(f"painted ok={ok} err={err} grid_days={n_days}", flush=True)

    stats = panel_stats(have)
    stats.update({
        "generated": date.today().isoformat(),
        "live_untouched": "flatten_robust",
        "n_grids_painted": ok,
        "n_grid_days": n_days,
        "iy_ones": sum(1 for v in iy.values() if v == 1),
        "iy_rows": len(iy),
        "m_cf": "IZ=1 → #95CA82 (prior-row H>0 and IY=1)",
        "index_fix": "FILL_IDX uses VISIBLE (M=12), not OPEN_FILL order",
    })
    json.dump(stats, open(STATS, "w"), indent=2)
    print(json.dumps(stats, indent=2), flush=True)

    if args.excel_validate:
        sample = have[: args.excel_validate]
        print(f"excel-validate {sample}", flush=True)
        # rebuild into a side dir then compare M
        side = os.path.join(ROOT, "grids_excel_check")
        os.makedirs(side, exist_ok=True)
        old_g, old_r = os.environ.get("GRIDS_DIR"), os.environ.get("ROWS_DIR")
        os.environ["GRIDS_DIR"] = side
        os.environ["ROWS_DIR"] = ROWS
        agree = total = 0
        for t in sample:
            # call rebuild_one against default paths — patch by copying
            src = os.path.join(ROWS, f"{t}.json")
            # rebuild_one reads ROWS_DIR env at import time; do inline
            from backtest import build_ticker, pick_anchors as pa
            rows = deser_rows(json.load(open(src)))
            anchors = pa(rows, rows[0]["date"], rows[-1]["date"])
            days = build_ticker(t, rows, anchors)
            fast = {d["date"]: (d.get("fills") or [None] * 15)[M_IDX]
                    for d in json.load(open(os.path.join(GRIDS, f"{t}.json")))["days"]}
            for d in days:
                total += 1
                excel_m = None
                fills = d.get("fills") or []
                if len(fills) > M_IDX:
                    excel_m = fills[M_IDX]
                    if excel_m:
                        excel_m = str(excel_m).upper().replace("#", "")
                        if len(excel_m) == 8 and excel_m.startswith("FF"):
                            excel_m = excel_m[2:]
                if fast.get(d["date"]) == excel_m or (
                        not fast.get(d["date"]) and not excel_m):
                    agree += 1
        os.environ.pop("GRIDS_DIR", None)
        if old_g:
            os.environ["GRIDS_DIR"] = old_g
        print(f"excel vs fast M match {agree}/{total}", flush=True)
        stats["excel_validate"] = {"agree": agree, "total": total,
                                   "sample": sample}
        json.dump(stats, open(STATS, "w"), indent=2)


if __name__ == "__main__":
    main()
