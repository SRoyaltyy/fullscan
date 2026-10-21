"""Pre-open fresh sell prices for the h1 send (#663).

Writes ``data/paper_open/{date}_preopen_px.json`` before the sealed h1
send so ``h1_sealed_exec.load_preopen_prices`` can size buys on cash the
book will really have. Shape::

    {"date": D, "written_at": ISO, "prices": {"KOD": 61.2},
     "sources": {"KOD": "yahoo_premarket" | "yahoo_prior_close"}}

Free source only: Yahoo's public chart endpoint. Pre-market last trade
first, else the last prior regular close. A ticker with neither is left
out, so the send fails closed (#663) instead of guessing. The file is
add-only: an existing file for the date is never rewritten.
Run: python3 -m src.preopen_px --date 2026-10-21
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "paper_open"
CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{t}"
UA = {"User-Agent": "Mozilla/5.0 (fullscan preopen_px)"}


def _ok(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 and math.isfinite(f) else None


def parse_chart(obj: dict) -> tuple[float | None, str | None]:
    """(price, source) from a Yahoo chart JSON with includePrePost."""
    try:
        res = obj["chart"]["result"][0]
    except (KeyError, IndexError, TypeError):
        return None, None
    meta = res.get("meta") or {}
    pre = (meta.get("currentTradingPeriod") or {}).get("pre") or {}
    ts = res.get("timestamp") or []
    closes = (((res.get("indicators") or {}).get("quote") or [{}])[0] or {}).get("close") or []
    start, end = pre.get("start"), pre.get("end")
    if start and end:
        for t, c in sorted(zip(ts, closes), reverse=True):
            if start <= t < end and _ok(c):
                return _ok(c), "yahoo_premarket"
    for k in ("chartPreviousClose", "previousClose", "regularMarketPrice"):
        if _ok(meta.get(k)):
            return _ok(meta.get(k)), "yahoo_prior_close"
    return None, None


def fetch_yahoo(ticker: str, session=None) -> tuple[float | None, str | None]:
    import requests
    s = session or requests
    try:
        r = s.get(CHART.format(t=ticker), headers=UA, timeout=15,
                  params={"interval": "1m", "range": "1d", "includePrePost": "true"})
        r.raise_for_status()
        return parse_chart(r.json())
    except Exception:
        return None, None


def sell_tickers(date: str, *, log_path: Path | None = None) -> list[str]:
    from src.h1_sealed_exec import load_sealed_h1_plan
    plan = load_sealed_h1_plan(date, log_path=log_path) or {}
    out = []
    for row in plan.get("planned_sells") or []:
        t = str((row or {}).get("ticker") or "").upper().strip()
        if t and t not in out:
            out.append(t)
    return out


def write_preopen(date: str, tickers, *, fetch=fetch_yahoo, out_dir: Path | None = None) -> Path:
    path = Path(out_dir or OUT_DIR) / f"{date}_preopen_px.json"
    if path.exists():
        return path  # add-only: never rewrite a day's file
    prices, sources, missing = {}, {}, []
    for t in tickers:
        px, src = fetch(t)
        if px is None:
            missing.append(t)
            continue
        prices[t], sources[t] = round(px, 4), src
    path.parent.mkdir(parents=True, exist_ok=True)
    obj = {"date": date, "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "prices": prices, "sources": sources, "missing": missing}
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, sort_keys=True, indent=1) + "\n", encoding="utf-8")
    tmp.replace(path)
    return path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    a = ap.parse_args(argv)
    try:
        tickers = sell_tickers(a.date)
    except Exception as exc:  # no sealed plan: the send itself fails closed
        print(f"preopen_px: no sealed h1 plan for {a.date} ({exc}); nothing written")
        return 0
    p = write_preopen(a.date, tickers)
    print(f"preopen_px: {p} ({len(tickers)} sells)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
