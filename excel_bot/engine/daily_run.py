"""Daily local pipeline — ZERO tokens, runs under Windows Task Scheduler.

Every run (scheduled 06:30 local, after the US close):
  1. FETCH   last ~14 days of OHLCV per ticker (same Yahoo datasource method
             as engine/backtest.py), merged into the persistent cache
             data/rows/<ticker>.json
  2. ENGINE  rebuild grids/<ticker>.json through the exact Excel-replica
             engine (same equations, same dependencies, same colors)
  3. SIGNALS evaluate every validated strategy in strategies/*/card.json;
             a suggestion is a cluster whose CONFIRMATION day is the latest
             trading day -> buy at next open (long) / short at next open
  4. STORE   On a final run only (16:00 ET or later, or a weekend /
             holiday for the previous completed session): append to ONE
             file, suggestions/suggestions.csv (never per-day files).
             Old rows get current_price / returns refreshed. From
             2026-10-06, signal_freeze locks each closed day's picks
             before the write. A mismatch leaves the file untouched.
             A draft run (before 16:00 ET on a session day) does not
             append rows and does not write freeze_manifest.json.
  5. TRACK   effectiveness: first_open (filled next run) vs current price

Usage:
  python engine/daily_run.py              # full run (all cached tickers)
  python engine/daily_run.py --limit 60   # test on 60 tickers
  python engine/daily_run.py --signals-only   # skip fetch, re-signal from grids
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
from stockhistory import fetch_daily
from fastfetch import fetch_daily_fast
from sweep import days_features, detect_def, definition_matrix
from cohort_analysis import load_finviz, cohorts_of
from cards import color_name, s2d

ROWS_DIR = "data/rows"
GRIDS_DIR = "grids"
SUGG_CSV = "suggestions/suggestions.csv"
SPAN_DAYS = 130
FETCH_BACK_DAYS = 14

DEFS = {d["name"]: d for d in definition_matrix()}


# ----------------------------------------------------------------- rows cache
def load_rows(ticker):
    p = os.path.join(ROWS_DIR, f"{ticker}.json")
    if os.path.exists(p):
        return json.load(open(p))
    return None


def save_rows(ticker, rows):
    os.makedirs(ROWS_DIR, exist_ok=True)
    json.dump(rows, open(os.path.join(ROWS_DIR, f"{ticker}.json"), "w"))


def deser(rows):
    return [{**r, "date": date.fromisoformat(r["date"])} for r in rows]


def ser(rows):
    return [{**r, "date": r["date"].isoformat()} for r in rows]


def init_cache_from_grids(ticker):
    """First run: seed the rows cache from the existing grid (no refetch)."""
    p = os.path.join(GRIDS_DIR, f"{ticker}.json")
    if not os.path.exists(p):
        return None
    out = []
    for d in json.load(open(p))["days"]:
        if any(d[k] is None for k in ("open", "close", "high", "low", "volume")):
            continue
        out.append({
            "date": (datetime(1899, 12, 30) + timedelta(days=d["date"])).date(),
            "open": d["open"], "close": d["close"], "high": d["high"],
            "low": d["low"], "volume": d["volume"],
        })
    return out or None


def update_ticker(ticker):
    """Incremental fetch + merge + rebuild grid. Returns (ticker, n_days, err)."""
    # Imported here so a store-path test can load this module without openpyxl.
    from backtest import build_ticker, pick_anchors
    try:
        cached = load_rows(ticker)
        if cached is None:
            rows = init_cache_from_grids(ticker)
            if rows is None:
                return (ticker, 0, "no cache and no grid")
        else:
            rows = deser(cached)
        start = rows[-1]["date"] - timedelta(days=FETCH_BACK_DAYS)
        by_date = {r["date"]: r for r in rows}
        try:
            fetched = fetch_daily_fast(ticker, start, date.today())
        except Exception as fe:
            if "permanent" in str(fe):
                raise  # dead ticker -- skip slow plugin fallback too
            # fall back to the plugin-tool path (slower, but authoritative)
            fetched = fetch_daily(ticker, start, date.today())
        for r in fetched:
            by_date[r["date"]] = r
        rows = [by_date[k] for k in sorted(by_date)]
        save_rows(ticker, ser(rows))
        anchors = pick_anchors(rows, max(date.today() - timedelta(days=SPAN_DAYS),
                                         rows[0]["date"]), rows[-1]["date"])
        days = build_ticker(ticker, rows, anchors)
        if days:
            os.makedirs(GRIDS_DIR, exist_ok=True)
            json.dump({"ticker": ticker, "days": days},
                      open(os.path.join(GRIDS_DIR, f"{ticker}.json"), "w"))
        return (ticker, len(days), None)
    except Exception as e:  # noqa: BLE001
        return (ticker, 0, f"{type(e).__name__}: {e}"[:150])


# ----------------------------------------------------------------- strategies
def load_strategies():
    out = []
    for cj in sorted(glob.glob("strategies/*/card.json")):
        card = json.load(open(cj))
        if card["name"] == "smoke_test":
            continue
        spec = card["spec"]
        # prefer the card's own definition dict (mined strategies may use
        # definitions outside the built-in matrix); fall back to DEFS by name
        d = spec["cluster_definition"]
        out.append({
            "name": card["name"],
            "definition": DEFS.get(d["name"], d),
            "side": 1 if spec["side"] == "long_green" else -1,
            "exit_rule": spec["exit_rule"],
            "cohort": spec["cohort_filter"],
        })
    return out


def find_signals(strategies):
    """Scan all grids; a signal = cluster confirmed on the LAST trading day,
    and the ticker must belong to the strategy's cohort."""
    fz = load_finviz()
    sigs = []
    for p in sorted(glob.glob(os.path.join(GRIDS_DIR, "*.json"))):
        t = os.path.basename(p)[:-5]
        if t.startswith("_"):
            continue
        try:
            days = days_features(json.load(open(p))["days"])
        except Exception:
            continue
        if not days:
            continue
        rec = fz.get(t)
        cohorts = {"ALL"} | set(cohorts_of(rec) if rec else [])
        last_i = len(days) - 1
        for st in strategies:
            if st["cohort"] not in cohorts:
                continue
            for c in detect_def(days, st["definition"]):
                if c["side"] != st["side"]:
                    continue
                if c.get("entry_idx") == last_i:   # confirmed TODAY
                    d = days[last_i]
                    sigs.append({
                        "ticker": t,
                        "strategy": st["name"],
                        "side": "LONG" if st["side"] == 1 else "SHORT",
                        "exit_rule": st["exit_rule"],
                        "ref_close": round(d["close"], 4),
                        "signal_date": str(s2d(d["date"])),
                        "signal_colors": "|".join(color_name(f)
                                                  for f in d["fills"]),
                    })
    return sigs


# ----------------------------------------------------------------- store/track
FIELDS = ["run_date", "signal_date", "ticker", "side", "strategy",
          "exit_rule", "ref_close", "first_open", "current_price",
          "ret_vs_close", "ret_vs_open", "days_held", "signal_colors"]


def parse_date(s):
    """Accept ISO (2026-07-28) or Excel-mangled locale dates (28/7/2026);
    return a date. Excel re-saves the CSV in locale format, so be liberal."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        pass
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def load_suggestions(path=None):
    """Load CSV and NORMALIZE all date fields back to ISO, so an Excel
    re-save can never break dedupe keys or tracking math."""
    path = path or SUGG_CSV
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    for r in rows:
        for fld in ("run_date", "signal_date"):
            d = parse_date(r.get(fld))
            if d:
                r[fld] = d.isoformat()
    return [r for r in rows if r.get("signal_date") and r.get("ticker")
            and r.get("strategy")]


def latest_close(ticker):
    p = os.path.join(GRIDS_DIR, f"{ticker}.json")
    try:
        days = json.load(open(p))["days"]
        return days[-1]["close"] if days and days[-1]["close"] else None
    except Exception:
        return None


def open_after(ticker, after_iso):
    """First trading-day OPEN strictly after after_iso (from rows cache)."""
    cached = load_rows(ticker)
    if cached is None:
        return None
    for r in deser(cached):
        if r["date"].isoformat() > after_iso and r["open"]:
            return r["open"]
    return None


def store_signals(sigs, *, now=None, sugg_csv=None, manifest_path=None):
    """Persist picks after the close. A pre-close run writes neither file.

    `now` is the clock for gh_summary.resolve_session, the same ET rule
    the summary uses. Before 16:00 ET on a session day this verifies
    locks already on disk and returns. suggestions.csv and
    freeze_manifest.json stay byte-identical. Only a final run appends
    rows and manifest entries. A new row is appended only when its
    signal_date is that resolved session. A weekend or holiday stamp
    is already the previous completed session, so a confirmation on
    that day is still appended. Any other date is logged and skipped:
    a failed fetch leaves the grid's last bar on an older day, and
    appending that bar would add a pick to a day that may already be
    locked. Rows already stored keep verifying. A signal_date whose
    session has not closed is not locked (signal_freeze refuses that too).
    """
    import gh_summary
    import signal_freeze

    sugg_csv = SUGG_CSV if sugg_csv is None else sugg_csv
    stamp = gh_summary.resolve_session(now)
    run_date = stamp.session.isoformat()
    if not stamp.write_final:
        manifest = manifest_path or signal_freeze.MANIFEST_PATH
        if os.path.exists(sugg_csv) and os.path.exists(manifest):
            # Read-only. verify_store raises if a locked day already
            # disagrees with the file; it does not append a lock.
            signal_freeze.verify_store(sugg_csv, manifest)
        print(
            f"[store] draft {run_date} before 16:00 ET: "
            "suggestions.csv and freeze_manifest.json were not written",
            flush=True,
        )
        return 0

    # -- update tracking on old rows, append new ones (final run only)
    os.makedirs(os.path.dirname(os.path.abspath(sugg_csv)), exist_ok=True)
    rows_old = load_suggestions(sugg_csv)
    for r in rows_old:
        tk = r["ticker"]
        if not r["first_open"]:
            fo = open_after(tk, r["signal_date"])
            if fo:
                r["first_open"] = f"{fo:.4f}"
        cur = latest_close(tk)
        if cur:
            r["current_price"] = f"{cur:.4f}"
            ref = float(r["ref_close"])
            r["ret_vs_close"] = f"{(cur/ref-1)*100:+.2f}%"
            if r["first_open"]:
                fo = float(r["first_open"])
                r["ret_vs_open"] = f"{(cur/fo-1)*100:+.2f}%"
        r["days_held"] = str((date.fromisoformat(run_date) -
                              parse_date(r["signal_date"])).days)

    # Same (signal_date, ticker, strategy) is not appended twice. If that
    # day is already locked and the engine's locked fields disagree, put
    # the disagreement on the in-memory row so plan_rows fails closed
    # before the file is replaced. Pre-lock rows stay on the old dedupe.
    manifest_on_disk = manifest_path or signal_freeze.MANIFEST_PATH
    locked_days = set()
    if os.path.exists(manifest_on_disk):
        locked_days = {
            entry["signal_date"]
            for entry in signal_freeze.load_manifest(manifest_on_disk)["entries"]
            if entry.get("kind") == "lock"
        }
    by_key = {(r["signal_date"], r["ticker"], r["strategy"]): r for r in rows_old}
    new = []
    for s in sigs:
        key_date = s["signal_date"]          # actual grid date of confirmation
        fresh = {
            "run_date": run_date, "signal_date": key_date,
            "ticker": s["ticker"], "side": s["side"],
            "strategy": s["strategy"], "exit_rule": s["exit_rule"],
            "ref_close": f"{s['ref_close']:.4f}", "first_open": "",
            "current_price": f"{s['ref_close']:.4f}",
            "ret_vs_close": "+0.00%", "ret_vs_open": "", "days_held": "0",
            "signal_colors": s["signal_colors"],
        }
        key = (key_date, s["ticker"], s["strategy"])
        if key_date != run_date:
            # Not this session. Do not append it, and do not copy it
            # onto a stored row: fresh["run_date"] is today's session,
            # so that copy would change a locked field and fail the run.
            print(
                f"[store] skip {s['ticker']} {s['strategy']} "
                f"signal_date {key_date}: not the {run_date} session",
                flush=True,
            )
            continue
        if key in by_key:
            stored = by_key[key]
            if key_date in locked_days:
                for field in signal_freeze.LOCKED_FIELDS:
                    if stored.get(field) != fresh[field]:
                        stored[field] = fresh[field]
            continue
        new.append(fresh)

    # Lock days on or after 2026-10-06 before the file is replaced.
    # A mismatch must not write suggestions.csv and must not commit.
    # plan_rows also refuses a signal_date whose session has not closed.
    payload = rows_old + new
    pending = signal_freeze.plan_rows(payload, manifest_path, now=now)

    # -- crash-proof write: retry if the file is open in Excel; never lose data
    written = False
    for attempt in range(6):
        try:
            tmp = sugg_csv + ".tmp"
            with open(tmp, "w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=FIELDS)
                w.writeheader()
                w.writerows(payload)
            os.replace(tmp, sugg_csv)
            written = True
            break
        except PermissionError:
            print(f"[store] WARNING: {sugg_csv} is locked (open in Excel?). "
                  f"Retry {attempt+1}/6 in 20s -- close it now!", flush=True)
            time.sleep(20)
    if not written:
        fb = os.path.join(os.path.dirname(sugg_csv),
                          f"suggestions_fallback_{run_date}.csv")
        with open(fb, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(payload)
        print(f"[store] ERROR: main file stayed locked. FULL results saved to "
              f"{fb} -- close Excel and rerun: "
              f"python engine/daily_run.py --signals-only", flush=True)
        sys.exit(2)
    added = signal_freeze.append_entries(pending, manifest_path, now=now)
    if added:
        print(f"[freeze] appended {len(added)} manifest "
              f"{'entry' if len(added) == 1 else 'entries'}", flush=True)
    else:
        print("[freeze] locked signal dates verified", flush=True)
    print(f"[store] {len(new)} new suggestions appended -> {sugg_csv} "
          f"(total {len(payload)})", flush=True)
    return len(new)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int,
                    default=min(8, (os.cpu_count() or 4)))
    ap.add_argument("--signals-only", action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    # GitHub's clock is UTC. A job that starts after midnight UTC can
    # still be the previous evening in New York. run_date (a locked
    # field) is that NYSE session, never the UTC date.
    import gh_summary
    stamp = gh_summary.resolve_session()
    run_date = stamp.session.isoformat()
    print(
        f"[session] {run_date} America/New_York "
        f"({'final' if stamp.write_final else 'draft'})",
        flush=True,
    )
    # Universe = grids (if present) UNION rows cache. On GitHub runners the
    # grids dir starts empty (only rows are restored from the state branch),
    # so grids-only enumeration silently processed ZERO tickers.
    tickers = sorted({os.path.basename(p)[:-5]
                      for p in glob.glob(os.path.join(GRIDS_DIR, "*.json"))}
                     | {os.path.basename(p)[:-5]
                        for p in glob.glob(os.path.join(ROWS_DIR, "*.json"))}
                     - {"_failed"})
    if args.limit:
        tickers = tickers[:args.limit]

    if not args.signals_only:
        # Processes, not threads: the engine rebuild is CPU-bound pure Python,
        # so threads would serialize on the GIL. Fetch is only ~0.15s inside.
        from multiprocessing import Pool
        ok = err = 0
        with Pool(args.workers) as pool:
            for tk, n, e in pool.imap_unordered(update_ticker, tickers,
                                                chunksize=32):
                ok += 0 if e else 1
                err += 1 if e else 0
                if e and err <= 5:
                    print(f"  ERR {tk}: {e}", flush=True)
                if (ok + err) % 200 == 0:
                    print(f"  ... {ok+err}/{len(tickers)} "
                          f"({time.time()-t0:.0f}s)", flush=True)
        print(f"[fetch+engine] {ok} ok, {err} err, "
              f"{time.time()-t0:.0f}s", flush=True)

    strategies = load_strategies()
    sigs = find_signals(strategies)
    print(f"[signals] {len(sigs)} new cluster confirmations", flush=True)

    import signal_freeze
    try:
        store_signals(sigs)
    except signal_freeze.SignalFreezeError as exc:
        print(exc, flush=True)
        sys.exit(3)
    print(f"[done] {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
