#!/usr/bin/env python3
"""REBUILD_MATCH for each deterministic input on job-log proven days.

A day from 2026-09-09 on is tested only when an Actions log pushed the
reference file at or before 13:30 UTC. Committer time is not a proof.
Price features and candidate lists have no pre-open output file; those
rows are compared with the #336 snapshot and stay not rebuildable for
earlier sessions. AI text is not regenerated. Nothing here writes a
ledger, snapshot, panel, or state file.
"""
from __future__ import annotations

import csv
import io
import json
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "research" / "audit"))

from build_fullscan_file_proof import (  # noqa: E402
    cutoff_for,
    git_text,
    load_history,
    load_indexes,
    load_parents,
    load_proofs,
)

OUT_MD = Path(__file__).resolve().parent / "REBUILD_MATCH_336.md"
OUT_JSON = Path(__file__).resolve().parent / "rebuild_match_336.json"
PROOF_CSV = Path(__file__).resolve().parent / "FULLSCAN_FILE_PROOF.csv"
SNAP = ROOT / "data" / "factor_mine" / "snapshots"

SESSIONS = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
    "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24",
    "2026-09-25",
)
ERA = "2026-09-09"
PRICE_SOURCES = {
    "yday_gainer", "yday_mover", "ohlc_hot", "overnight",
    "overnight_mega", "probable",
}
A_FLAGS = (
    "A01_rsi_value", "A02_rsi_cross_30", "A03_rsi_cross_50",
    "A04_rsi_cross_70", "A05_body_red_green_2day",
    "A06_volume_red_green_2day", "A07_rvol", "A08_bollinger_position",
    "A09_above_sma50", "A10_sma20_50_80_stack", "A11_three_section_lows",
    "A12_green_body_vs_wick_2day", "A13_red_body_vs_wick_2day",
    "A15_tape_recovery_setup",
)


def close_num(saved, fresh) -> bool:
    try:
        a = float(saved)
        b = float(fresh)
    except (TypeError, ValueError):
        return saved == fresh
    if a != a and b != b:
        return True
    return abs(a - b) <= 1e-3 * max(abs(a), abs(b), 1.0) or abs(a - b) <= 1e-4


def as_bool(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def latest_before(history, proofs, path: str, cutoff: datetime):
    chosen = None
    for item in history.get(path) or []:
        proof = proofs.get(item["commit"])
        if not proof or proof["time"] > cutoff:
            continue
        chosen = (item, proof)
    return chosen


def git_blob(blob: str) -> bytes:
    proc_text = git_text(["cat-file", "-p", blob])
    return proc_text.encode("utf-8", "surrogateescape")


def verdict(days: list[dict]) -> str:
    tested = [row for row in days if row.get("tested")]
    if not tested:
        return "not-rebuildable (no proven pre-open copy)"
    bad = [row for row in tested if not row.get("match")]
    if not bad:
        return "exact"
    return f"mismatch ({len(bad)}/{len(tested)}, first {bad[0]['day']})"


def yahoo_daily(symbol: str) -> list[tuple[str, float]]:
    start = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp())
    end = int(datetime(2026, 9, 27, tzinfo=timezone.utc).timestamp())
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{symbol}?interval=1d&period1={start}&period2={end}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = json.load(resp)
    result = payload["chart"]["result"][0]
    closes = result["indicators"]["quote"][0]["close"]
    out = []
    for stamp, close in zip(result["timestamp"], closes):
        if close is None:
            continue
        day = datetime.fromtimestamp(stamp, timezone.utc).date().isoformat()
        out.append((day, float(close)))
    return out


def prior_close(bars: list[tuple[str, float]], day: str) -> float | None:
    prev = [close for bar_day, close in bars if bar_day < day]
    return prev[-1] if prev else None


def fred_dgs10() -> list[tuple[str, float]]:
    url = (
        "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10"
        "&cosd=2026-07-01&coed=2026-09-25"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        text = resp.read().decode("utf-8", "replace")
    out = []
    for line in text.splitlines()[1:]:
        parts = line.split(",")
        if len(parts) < 2 or parts[1] in {"", "."}:
            continue
        out.append((parts[0], float(parts[1])))
    return out


def snapshot_rows(day: str) -> list[dict]:
    path = SNAP / f"{day}.json"
    if not path.is_file():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [row for row in (payload.get("rows") or []) if isinstance(row, dict)]


def snapshot_sources(rows: list[dict]) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        for src in row.get("sources") or []:
            if ticker not in buckets[src]:
                buckets[src].append(ticker)
    return buckets


def load_groups():
    import pandas as pd
    store = pd.read_parquet(ROOT / "data" / "prices" / "ohlc.parquet")
    store["date"] = pd.to_datetime(store["date"])
    return {
        ticker: frame.drop(columns=["ticker"]).set_index("date").sort_index()
        for ticker, frame in store.groupby("ticker")
    }


def compare_ab(history, proofs, groups) -> dict:
    import pandas as pd
    from src import ab_checklist as ab

    per_flag = {name: [] for name in A_FLAGS}
    tested_days = []
    for day in SESSIONS:
        if day < ERA:
            continue
        cutoff = cutoff_for(day)
        chosen = None
        for suffix in ("_ab_checklist.csv", "_ab_checklist_enriched.csv", "_ab_slim.csv"):
            path = f"data/ab_checklist/{day}{suffix}"
            hit = latest_before(history, proofs, path, cutoff)
            if not hit:
                continue
            blob = git_blob(hit[0]["blob"])
            if b"flag_A01_rsi_value" not in blob.splitlines()[0]:
                continue
            chosen = (path, hit, blob)
            break
        if not chosen:
            for name in A_FLAGS:
                per_flag[name].append({"day": day, "tested": False, "match": False})
            continue
        path, hit, blob = chosen
        frame = pd.read_csv(io.BytesIO(blob))
        asof = pd.Timestamp(day)
        bad = {name: 0 for name in A_FLAGS}
        present = {name: f"flag_{name}" in frame.columns for name in A_FLAGS}
        rows = 0
        for rec in frame.itertuples(index=False):
            ticker = str(getattr(rec, "Ticker", "") or "").strip().upper()
            bars = groups.get(ticker)
            fresh = {name: 0 for name in A_FLAGS}
            if bars is not None:
                passed = ab._pass_a(ab._part_a(bars[bars.index < asof]))
                fresh.update(passed)
            rows += 1
            for name in A_FLAGS:
                if not present[name]:
                    continue
                try:
                    saved = int(float(getattr(rec, f"flag_{name}")))
                except (TypeError, ValueError):
                    saved = None
                if saved != int(fresh.get(name, 0)):
                    bad[name] += 1
        tested_days.append(day)
        for name in A_FLAGS:
            if not present[name]:
                per_flag[name].append({
                    "day": day, "tested": False, "match": False,
                    "reason": "column absent",
                })
                continue
            per_flag[name].append({
                "day": day, "tested": True, "match": bad[name] == 0,
                "bad_rows": bad[name], "rows": rows, "path": path,
                "server": hit[1]["time"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
        print(f"[rebuild] ab {day} rows={rows}", flush=True)
    return {"days": tested_days, "flags": per_flag}


def compare_features(days_rows: dict[str, list[dict]]) -> dict:
    from src import candle_factor as cf
    from src import ohlc_ripper as ohlc

    keys = (
        "hot_score", "returns", "rvol", "break_10", "candles", "rsi_ohlc", "macd",
    )
    out = {key: [] for key in keys}
    fv_rows = 0
    fv_equal = 0
    for day in SESSIONS:
        if day < ERA:
            continue
        rows = days_rows[day]
        if not rows:
            for key in keys:
                out[key].append({"day": day, "tested": False, "match": False})
            continue
        bad = {key: 0 for key in keys}
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            oh = ohlc.features(ticker, day) if ticker else {}
            cd = cf.features(ticker, day) if ticker else {}
            if not close_num(row.get("ohlc_hot_score"), oh.get("hot_score")):
                bad["hot_score"] += 1
            rets_ok = all(close_num(row.get(saved), oh.get(src)) for saved, src in (
                ("ohlc_ret_1", "ret_1"), ("ohlc_ret_5", "ret_5"), ("ohlc_ret_10", "ret_10"),
            ))
            if not rets_ok:
                bad["returns"] += 1
            if not close_num(row.get("ohlc_rvol"), oh.get("rvol")):
                bad["rvol"] += 1
            if as_bool(row.get("ohlc_break_10")) != as_bool(oh.get("break_10")):
                bad["break_10"] += 1
            candles_ok = (
                close_num(row.get("candle_score"), cd.get("score"))
                and as_bool(row.get("last_green")) == as_bool(cd.get("last_green"))
                and as_bool(row.get("last_red")) == as_bool(cd.get("last_red"))
                and close_num(row.get("candle_body_rg"), cd.get("body_rg"))
                and as_bool(row.get("candle_capture")) == as_bool(cf.capture(cd))
            )
            if not candles_ok:
                bad["candles"] += 1
            if not close_num(row.get("rsi"), oh.get("rsi")):
                bad["rsi_ohlc"] += 1
            macd_ok = all(close_num(row.get(saved), oh.get(src)) for saved, src in (
                ("macd", "macd"), ("macd_sig", "macd_sig"), ("macd_hist", "macd_hist"),
            )) and as_bool(row.get("macd_up")) == as_bool(oh.get("macd_up"))
            if not macd_ok:
                bad["macd"] += 1
            fv = row.get("fv_rsi")
            if fv is not None:
                fv_rows += 1
                if close_num(row.get("rsi"), fv):
                    fv_equal += 1
        for key in keys:
            out[key].append({
                "day": day, "tested": True, "match": bad[key] == 0,
                "bad_rows": bad[key], "rows": len(rows),
            })
        print(
            f"[rebuild] px {day} "
            + " ".join(f"{key}={bad[key]}" for key in keys),
            flush=True,
        )
    out["fv_rsi_rows"] = fv_rows
    out["fv_rsi_equals_stored"] = fv_equal
    return out


def compare_lists(history, proofs, days_rows) -> dict:
    from src import gainer_asof as ga
    from src import gainer_capture as gc
    from src import ohlc_ripper as ohlc

    keys = ("yday_gainer", "yday_mover", "ohlc_hot", "overnight", "probable", "earn_react")
    out = {key: [] for key in keys}
    look = list(SESSIONS)
    state = {"day": None, "frames": {}}
    original = ga.load_finviz

    def load_finviz(date: str):
        import pandas as pd
        key = (state["day"], date)
        if key in state["frames"]:
            return state["frames"][key]
        hit = latest_before(
            history, proofs, f"data/exports/finviz_{date}.csv", cutoff_for(state["day"]),
        )
        frame = pd.DataFrame()
        if hit:
            frame = pd.read_csv(io.BytesIO(git_blob(hit[0]["blob"])))
        state["frames"][key] = frame
        return frame

    ga.load_finviz = load_finviz
    try:
        for day in SESSIONS:
            if day < ERA:
                continue
            rows = days_rows[day]
            prior = None
            if rows:
                priors = [str(row.get("prior_date") or "")[:10] for row in rows]
                priors = [item for item in priors if item]
                prior = max(set(priors), key=priors.count) if priors else None
            if not prior:
                prior = gc.knowable_export_date(look, day)
            hit = latest_before(
                history, proofs, f"data/exports/finviz_{prior}.csv", cutoff_for(day),
            ) if prior else None
            if not hit or not rows:
                for key in keys:
                    out[key].append({
                        "day": day, "tested": False, "match": False,
                        "reason": "prior export not log-proven before 13:30" if not hit else "no snapshot rows",
                    })
                continue
            state["day"] = day
            nxt = gc.next_session(look, day)
            built = {
                "yday_gainer": gc.yesterday_gainers(prior, top_n=25),
                "yday_mover": gc.yesterday_movers(prior, top_n=20),
                "ohlc_hot": ohlc.liquid_hot(prior, day, top_n=30),
                "overnight": gc.overnight_scheduled(prior, day, nxt),
                "probable": ohlc.continuation(prior, day, top_n=ohlc.CONT_TOP_N),
                "earn_react": gc.earnings_reaction(prior, day),
            }
            saved = snapshot_sources(rows)
            for key in keys:
                fresh = [ticker for ticker in built[key] if ticker]
                old = saved.get(key) or []
                out[key].append({
                    "day": day, "tested": True, "match": fresh == old,
                    "set_match": set(fresh) == set(old),
                    "fresh": fresh, "saved": old,
                    "only_fresh": [ticker for ticker in fresh if ticker not in set(old)],
                    "only_saved": [ticker for ticker in old if ticker not in set(fresh)],
                    "prior": prior,
                    "server": hit[1]["time"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                })
            print(f"[rebuild] lists {day} prior={prior}", flush=True)
    finally:
        ga.load_finviz = original
    return out


def compare_peer(history, proofs) -> list[dict]:
    import numpy as np
    import pandas as pd
    from src import peer_rs

    corr = peer_rs._load_correlations()
    days = []
    for day in SESSIONS:
        if day < ERA:
            continue
        cutoff = cutoff_for(day)
        export = latest_before(history, proofs, f"data/exports/finviz_{day}.csv", cutoff)
        peer = latest_before(history, proofs, f"data/peers/{day}_peer_rs.csv", cutoff)
        if not export or not peer:
            days.append({"day": day, "tested": False, "match": False})
            continue
        raw = pd.read_csv(io.BytesIO(git_blob(export[0]["blob"])), low_memory=False)
        saved = pd.read_csv(io.BytesIO(git_blob(peer[0]["blob"])))
        tcol = "Ticker" if "Ticker" in raw.columns else raw.columns[0]
        raw[tcol] = raw[tcol].astype(str).str.strip().str.upper()
        raw = raw.drop_duplicates(subset=[tcol], keep="first")
        week = raw.set_index(tcol)[peer_rs.PERF_WEEK].map(peer_rs._pct)
        fresh = {}
        for ticker, peers in corr.items():
            if ticker not in week.index:
                continue
            own = float(week.get(ticker, np.nan))
            present = [name for name in peers if name in week.index]
            peer_w = week.reindex(present).dropna()
            med = float(peer_w.median()) if len(peer_w) else np.nan
            if own == own and med == med:
                fresh[ticker] = own - med
        saved["Ticker"] = saved["Ticker"].astype(str).str.strip().str.upper()
        bad = 0
        both = 0
        for rec in saved.itertuples(index=False):
            ticker = rec.Ticker
            if ticker not in fresh:
                continue
            both += 1
            if not close_num(getattr(rec, "rs_week", None), fresh[ticker]):
                bad += 1
        days.append({
            "day": day, "tested": True, "match": bad == 0 and both > 0,
            "bad": bad, "compared": both, "saved_rows": len(saved),
        })
        print(f"[rebuild] peer {day} bad={bad}/{both}", flush=True)
    return days


def compare_vix_rates(history, proofs) -> dict:
    vix = yahoo_daily("%5EVIX")
    vix3m = yahoo_daily("%5EVIX3M")
    try:
        dgs = fred_dgs10()
        rate_note = "FRED DGS10"
    except Exception as exc:
        dgs = []
        rate_note = f"FRED DGS10 unavailable ({type(exc).__name__})"
    vix_days = []
    rate_days = []
    for day in SESSIONS:
        if day < ERA:
            continue
        hit = latest_before(history, proofs, f"01_daily/weather/{day}_weather.json", cutoff_for(day))
        if not hit:
            vix_days.append({"day": day, "tested": False, "match": False})
            rate_days.append({"day": day, "tested": False, "match": False})
            continue
        weather = json.loads(git_blob(hit[0]["blob"]).decode("utf-8", "replace"))
        sig = weather.get("signals") or {}
        spot = prior_close(vix, day)
        far = prior_close(vix3m, day)
        ratio = (spot / far) if spot and far else None
        vix_ok = close_num(sig.get("vix_spot"), spot) and close_num(sig.get("vix_ratio"), ratio)
        vix_days.append({
            "day": day, "tested": True, "match": vix_ok,
            "saved_spot": sig.get("vix_spot"), "prior_close": None if spot is None else round(spot, 2),
            "saved_ratio": sig.get("vix_ratio"),
            "prior_ratio": None if ratio is None else round(ratio, 3),
        })
        prints = [value for bar_day, value in dgs if bar_day < day]
        latest = prints[-1] if prints else None
        rate_ok = bool(dgs) and close_num(sig.get("dgs10_current"), latest)
        rate_days.append({
            "day": day, "tested": bool(dgs), "match": rate_ok,
            "saved": sig.get("dgs10_current"),
            "prior_print": latest,
            "saved_date_field": sig.get("yields_source"),
        })
    return {"vix": vix_days, "rates": rate_days, "rate_note": rate_note}


def panel_miss(days_rows) -> list[dict]:
    out = []
    for day in SESSIONS:
        rows = days_rows[day]
        names = []
        missed = []
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            sources = [str(src) for src in (row.get("sources") or [])]
            if not ticker:
                continue
            names.append(ticker)
            if not (set(sources) & PRICE_SOURCES):
                missed.append(f"{ticker} ({'+'.join(sources) or 'no source'})")
        out.append({
            "day": day,
            "n": len(names),
            "missed": missed,
            "share": (len(missed) / len(names)) if names else None,
        })
    return out


def proven_counts() -> dict[str, int]:
    """Days from 09-09 on where every file of that input is log-proven."""
    by_day: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    with PROOF_CSV.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["date"] < ERA:
                continue
            by_day[row["input"]][row["date"]].append(row["proven"])
    counts = {}
    for role, days in by_day.items():
        counts[role] = sum(
            1 for vals in days.values() if vals and all(val == "yes" for val in vals)
        )
    return counts


def md_table(rows: list[tuple]) -> list[str]:
    lines = [
        "| Input | Proven days (09-09 on) | REBUILD_MATCH | Rebuildable for earlier days |",
        "| --- | ---: | --- | --- |",
    ]
    for name, days, match, rebuild in rows:
        lines.append(f"| {name} | {days} | {match} | {rebuild} |")
    return lines


def main() -> None:
    print("[rebuild] proofs", flush=True)
    parents = load_parents()
    by7, by8 = load_indexes(parents)
    proofs, stats = load_proofs(parents, by7, by8)
    history = load_history()
    print(f"[rebuild] proofs {stats['commits']} paths {len(history)}", flush=True)
    days_rows = {day: snapshot_rows(day) for day in SESSIONS}
    features = compare_features(days_rows)
    lists = compare_lists(history, proofs, days_rows)
    print("[rebuild] ab", flush=True)
    groups = load_groups()
    ab = compare_ab(history, proofs, groups)
    peer = compare_peer(history, proofs)
    market = compare_vix_rates(history, proofs)
    missed = panel_miss(days_rows)
    counts = proven_counts()
    cache = ROOT / "src" / "_ab_checklist_cached.py"
    if cache.exists():
        cache.unlink()

    fv_share = 0.0
    if features["fv_rsi_rows"]:
        fv_share = features["fv_rsi_equals_stored"] / features["fv_rsi_rows"]
    rsi_label = verdict(features["rsi_ohlc"])
    if fv_share >= 0.5:
        rsi_match = "not-rebuildable (Finviz snapshot)"
    else:
        rsi_match = rsi_label

    rows = []
    feature_labels = {
        "hot_score": "hot score",
        "returns": "returns (1d, 5d, 10d)",
        "rvol": "rvol",
        "break_10": "10d breakout",
        "candles": "candles",
        "macd": "MACD",
    }
    for key, label in feature_labels.items():
        tested = sum(1 for row in features[key] if row.get("tested"))
        rows.append((label, f"0 pre-open; {tested} snapshot", verdict(features[key]), "no"))
    tested_rsi = sum(1 for row in features["rsi_ohlc"] if row.get("tested"))
    rows.append(("RSI", f"0 pre-open; {tested_rsi} snapshot", rsi_match, "no"))

    list_labels = {
        "yday_gainer": "prior-day gainers",
        "yday_mover": "prior-day movers",
        "ohlc_hot": "hot list",
        "overnight": "overnight moves",
        "probable": "probable continuation",
        "earn_react": "earnings reaction",
    }
    for key, label in list_labels.items():
        tested = sum(1 for row in lists[key] if row.get("tested"))
        tested_rows = [row for row in lists[key] if row.get("tested")]
        set_bad = [row for row in tested_rows if not row.get("set_match")]
        order_only = [row for row in tested_rows if row.get("set_match") and not row.get("match")]
        if key == "earn_react":
            match = "not-rebuildable (Finviz snapshot)"
        elif not tested_rows:
            match = "not-rebuildable (no proven pre-open copy)"
        elif not set_bad and not order_only:
            match = "exact"
        elif set_bad and order_only:
            match = (
                f"mismatch ({len(set_bad)}/{len(tested_rows)} name sets, "
                f"first {set_bad[0]['day']}; order-only {len(order_only)}, "
                f"first {order_only[0]['day']})"
            )
        elif set_bad:
            match = f"mismatch ({len(set_bad)}/{len(tested_rows)}, first {set_bad[0]['day']})"
        else:
            match = (
                f"mismatch (order-only {len(order_only)}/{len(tested_rows)}, "
                f"first {order_only[0]['day']})"
            )
        rows.append((label, f"0 pre-open output; {tested} with proven prior export", match, "no"))

    for name in A_FLAGS:
        tested = sum(1 for row in ab["flags"][name] if row.get("tested"))
        rows.append((
            name, tested, verdict(ab["flags"][name]),
            "yes" if verdict(ab["flags"][name]) == "exact" and tested else "no",
        ))
    rows.append((
        "A14_profitable_oversold_setup", 0,
        "not-rebuildable (no proven pre-open copy)", "no",
    ))
    peer_tested = sum(1 for row in peer if row.get("tested"))
    peer_match = verdict(peer)
    rows.append((
        "peer RS", peer_tested, peer_match,
        "yes" if peer_match == "exact" and peer_tested else "no",
    ))
    vix_tested = sum(1 for row in market["vix"] if row.get("tested"))
    rate_tested = sum(1 for row in market["rates"] if row.get("tested"))
    rows.append(("VIX", vix_tested, verdict(market["vix"]), "no"))
    rate_match = verdict(market["rates"]) if rate_tested else f"not-rebuildable ({market['rate_note']})"
    rows.append(("rates (DGS10)", rate_tested, rate_match, "no"))

    ai = (
        ("predict", "predict"), ("actions", "actions"), ("judge", "judge"),
        ("digest", "digest"), ("map_heat", "map_heat"), ("catalyst", "catalyst"),
        ("research", "research"), ("baseline", "baseline"), ("events", "events"),
        ("sector_predict", "sector predict"),
    )
    for key, label in ai:
        rows.append((label, counts.get(key, 0), "not-rebuildable (AI)", "no"))
    rows.append(("Finviz export", counts.get("export", 0), "not-rebuildable (Finviz snapshot)", "no"))

    lines = [
        "# REBUILD_MATCH",
        "",
        "Each deterministic input is tested on its own. A session from 2026-09-09 on counts only when an Actions job log pushed the reference file at or before 13:30 UTC. `exact` means every tested day matches. `mismatch (k/n, first day)` is the number of tested days that differ and the earliest of them. AI text is not regenerated. A Finviz print is not a price-store formula.",
        "",
        "Price features and the candidate lists have no pre-open file of their own. The number under Proven days is how many snapshot sessions were compared. A match there does not make the input rebuildable for days before 09-09. The snapshot commit is `5f13a4415ea0`, which is outside every harvested push range.",
        "",
        *md_table(rows),
        "",
        f"Panel RSI prefers the prior Finviz print. On the snapshot rows, stored `rsi` equals `fv_rsi` on {features['fv_rsi_equals_stored']} of {features['fv_rsi_rows']} rows that have a Finviz RSI. Wilder RSI from the price store versus that stored field is `{rsi_label}`.",
        "",
        f"Rates source: {market['rate_note']}. VIX is the prior Yahoo close (bars dated before the session), compared with `vix_spot` and `vix_ratio` on the log-proven pre-open `weather.json`.",
        "",
        "A14 is the backfill setup flag in `src/ab_backfill.py`. The proven checklist files have A01–A13 and A15, not A14.",
        "",
        "Peer RS is `Performance (Week)` minus the median of the correlation peers, scored from the log-proven pre-open Finviz export against the log-proven pre-open `peer_rs.csv`. The checklist coverage rule does not reject this file here: the formula does not need a session date inside the body.",
        "",
        "## Names the price-list functions select",
        "",
    ]
    for key, label in list_labels.items():
        lines.append(f"### {label}")
        lines.append("")
        any_row = False
        for row in lists[key]:
            if not row.get("tested"):
                continue
            any_row = True
            if row["match"]:
                mark = "exact"
                tail = ""
            elif row.get("set_match"):
                mark = "same names, different order"
                tail = ""
            else:
                mark = "mismatch"
                tail = (
                    f" Only in rebuild: {', '.join(row['only_fresh']) or '—'}. "
                    f"Only in snapshot: {', '.join(row['only_saved']) or '—'}."
                )
            lines.append(
                f"- {row['day']} {mark}. Rebuilt {len(row['fresh'])}, "
                f"snapshot {len(row['saved'])}.{tail}"
            )
        if not any_row:
            lines.append("- No day had a log-proven prior export and snapshot rows.")
        lines.append("")

    lines += [
        "## AB rules, rows that differ",
        "",
        "| Rule | Days tested | Days that differ | First differing day | Worst day (bad rows) |",
        "| --- | ---: | ---: | --- | --- |",
    ]
    for name in A_FLAGS:
        tested = [row for row in ab["flags"][name] if row.get("tested")]
        bad = [row for row in tested if not row["match"]]
        worst = max(bad, key=lambda row: row["bad_rows"]) if bad else None
        worst_txt = f"{worst['day']} {worst['bad_rows']}/{worst['rows']}" if worst else "—"
        lines.append(
            f"| {name} | {len(tested)} | {len(bad)} | "
            f"{bad[0]['day'] if bad else '—'} | {worst_txt} |"
        )
    lines += ["", "## Panel names a price-only rebuild misses", ""]
    lines.append(
        "A name is missed when none of its snapshot `sources` are "
        "prior-day gainers, prior-day movers, the hot list, overnight, "
        "overnight mega, or probable. Flatten, mover-buy, and earnings-reaction "
        "names with no price source are the miss. Share is missed names / panel names."
    )
    lines.append("")
    lines.append("| Day | Panel names | Missed | Share | Names (source) |")
    lines.append("| --- | ---: | ---: | ---: | --- |")
    for row in missed:
        if not row["n"]:
            lines.append(f"| {row['day'][5:]} | 0 | 0 | — | — |")
            continue
        share = f"{row['share']:.0%}"
        shown = ", ".join(row["missed"][:12])
        if len(row["missed"]) > 12:
            shown += f", +{len(row['missed']) - 12} more"
        lines.append(
            f"| {row['day'][5:]} | {row['n']} | {len(row['missed'])} | {share} | {shown or '—'} |"
        )
    lines.append("")
    text = "\n".join(lines) + "\n"
    OUT_MD.write_text(text, encoding="utf-8")
    OUT_JSON.write_text(json.dumps({
        "rows": rows,
        "features": {key: features[key] for key in feature_labels},
        "rsi_ohlc": features["rsi_ohlc"],
        "fv_rsi_rows": features["fv_rsi_rows"],
        "fv_rsi_equals_stored": features["fv_rsi_equals_stored"],
        "lists": {
            key: [
                {k: v for k, v in row.items() if k not in {"fresh", "saved"}}
                for row in lists[key]
            ]
            for key in list_labels
        },
        "peer": peer,
        "vix": market["vix"],
        "rates": market["rates"],
        "rate_note": market["rate_note"],
        "panel_miss": missed,
        "ab_days": ab["days"],
    }, indent=2), encoding="utf-8")
    print(f"[rebuild] wrote {OUT_MD}", flush=True)


if __name__ == "__main__":
    main()
