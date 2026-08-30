"""Fast in-process Yahoo daily OHLCV fetch (v8 chart API, plain HTTP).

Same upstream datasource as the yahoo_finance plugin tool (Yahoo Finance),
but called directly over HTTPS from this machine -- no subprocess, no
gateway round-trip. ~0.3s/ticker instead of ~12s.

Data-equivalence is enforced by verify_equivalence(): compare against rows
previously fetched through the plugin tool before trusting this path.
"""
import json
import time
import urllib.error
import urllib.request
from datetime import date, datetime, time as dtime, timedelta, timezone

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _ts(d: date) -> int:
    return int(datetime.combine(d, dtime.min, tzinfo=timezone.utc).timestamp())


def fetch_daily_fast(ticker, start: date, end: date, retries: int = 3) -> list:
    """Return [{date, open, high, low, close, volume}] ascending, or raise."""
    p1, p2 = _ts(start), _ts(end + timedelta(days=1))
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
           f"{urllib.request.quote(ticker)}"
           f"?period1={p1}&period2={p2}&interval=1d&events=div,split")
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            res = payload["chart"]["result"][0]
            ts = res["timestamp"]
            q = res["indicators"]["quote"][0]
            rows = []
            for i, t in enumerate(ts):
                o, h, l, c, v = (q["open"][i], q["high"][i], q["low"][i],
                                 q["close"][i], q["volume"][i])
                if c is None or o is None:
                    continue
                rows.append({
                    "date": datetime.fromtimestamp(t, tz=timezone.utc).date(),
                    "open": float(o), "high": float(h), "low": float(l),
                    "close": float(c), "volume": float(v or 0),
                })
            rows.sort(key=lambda r: r["date"])
            return [r for r in rows if start <= r["date"] <= end]
        except urllib.error.HTTPError as e:
            # 404/422/500 = dead/delisted ticker -- permanent, do not retry
            if e.code in (400, 404, 422, 500):
                raise RuntimeError(f"fast fetch failed for {ticker}: "
                                   f"HTTP {e.code} (permanent)") from e
            last = e
            time.sleep(1.5 * (attempt + 1))
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"fast fetch failed for {ticker}: {last}")


def verify_equivalence(ticker, start: date, end: date, tol=1e-6) -> bool:
    """Compare fast path vs plugin-tool path; True if identical within tol."""
    from stockhistory import fetch_daily
    a = fetch_daily(ticker, start, end)
    b = fetch_daily_fast(ticker, start, end)
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if ra["date"] != rb["date"]:
            return False
        for k in ("open", "high", "low", "close", "volume"):
            if abs(ra[k] - rb[k]) > tol * max(1.0, abs(ra[k])):
                return False
    return True
