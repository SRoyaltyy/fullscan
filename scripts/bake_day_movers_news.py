#!/usr/bin/env python3
"""Bake dashboard/day-movers/n/{A-Z}.json from data/exports/finviz_YYYY-MM-DD.csv.

Keeps News Title / Daily Digest / News URL for every ticker that appears on
the day-movers board, deduped across sessions. No 400-name digest cap.
"""
from __future__ import annotations
import argparse, csv, collections, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXPORT = ROOT / "data" / "exports"
DAYS = ROOT / "dashboard" / "day-movers" / "days.json"
OUT = ROOT / "dashboard" / "day-movers" / "n"


def movers(days_path: Path) -> set[str]:
    d = json.loads(days_path.read_text())
    tickers = set()
    for day in d.get("days") or []:
        for clock in ("intraday", "interday"):
            pack = day.get(clock) or {}
            for side in ("gainers", "losers"):
                for r in pack.get(side) or []:
                    if r.get("t"):
                        tickers.add(str(r["t"]).upper())
    return tickers


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", default=str(DAYS))
    args = ap.parse_args()
    days = json.loads(Path(args.days).read_text())
    tickers = movers(Path(args.days))
    by = {t: [] for t in tickers}
    for date in days.get("dates") or []:
        path = EXPORT / f"finviz_{date}.csv"
        if not path.exists():
            print(f"skip {date}: no export")
            continue
        with path.open(newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                t = (row.get("Ticker") or "").strip().upper()
                if t not in tickers:
                    continue
                title = (row.get("News Title") or "").strip()
                digest = (row.get("Daily Digest") or "").strip()
                url = (row.get("News URL") or "").strip()
                ntime = (row.get("News Time") or "").strip()
                if title.lower() in ("", "nan", "none", "-") and digest.lower() in ("", "nan", "none", "-"):
                    continue
                if title.lower() in ("nan", "none", "-"):
                    title = ""
                if digest.lower() in ("nan", "none", "-"):
                    digest = ""
                if url.lower() in ("nan", "none", "-"):
                    url = ""
                by[t].append({"date": date, "time": ntime, "title": title, "digest": digest, "url": url, "src": "finviz_export"})
        print(f"baked {date}")
    shards = collections.defaultdict(dict)
    n_unique = 0
    for t, arts in by.items():
        groups = collections.OrderedDict()
        for a in arts:
            key = (a.get("title") or "").strip() + "||" + (a.get("digest") or "").strip()
            if key == "||":
                continue
            g = groups.get(key)
            if not g:
                g = {"title": a.get("title") or "", "digest": a.get("digest") or "", "url": a.get("url") or "",
                     "src": "finviz_export", "first": a["date"], "last": a["date"], "time": a.get("time") or "", "dates": []}
                groups[key] = g
            if a["date"] not in g["dates"]:
                g["dates"].append(a["date"])
            g["last"] = a["date"]
            if a.get("time"):
                g["time"] = a["time"]
            if a.get("url") and not g["url"]:
                g["url"] = a["url"]
        packed = list(groups.values())
        if not packed:
            continue
        n_unique += len(packed)
        letter = t[0].upper() if t and t[0].isalpha() else "0"
        shards[letter][t] = packed
    OUT.mkdir(parents=True, exist_ok=True)
    index = {"letters": {}, "n_tickers": sum(len(v) for v in shards.values()), "n_unique": n_unique,
             "src": "data/exports/finviz_YYYY-MM-DD.csv"}
    for letter, pack in sorted(shards.items()):
        p = OUT / f"{letter}.json"
        p.write_text(json.dumps(pack, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
        index["letters"][letter] = {"n": len(pack), "bytes": p.stat().st_size}
    (OUT / "index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")
    print(f"wrote {OUT} tickers={index['n_tickers']} unique={n_unique}")


if __name__ == "__main__":
    main()
