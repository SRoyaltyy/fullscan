"""Local OHLC store: bootstrap once, append daily, analyze offline.

Layout
------
data/prices/
  ohlc.parquet     long table: date, ticker, open, high, low, close, volume
  meta.json        {last_date, n_rows, n_tickers, updated}

CLI
---
  python -m src.price_store bootstrap --days 400
  python -m src.price_store update
  python -m src.price_store status
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
PRICE_DIR = ROOT / "data" / "prices"
STORE_PATH = PRICE_DIR / "ohlc.parquet"
META_PATH = PRICE_DIR / "meta.json"
ET = ZoneInfo(config.TZ)

CHUNK = 80


def _universe_tickers() -> list[str]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        raise SystemExit("[price_store] no finviz exports under data/exports/")
    df = pd.read_csv(files[-1], low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    out: set[str] = set()
    for x in df[tcol].tolist():
        if x is None:
            continue
        try:
            if isinstance(x, float) and np.isnan(x):
                continue
        except Exception:
            pass
        if isinstance(x, float) and pd.isna(x):
            continue
        s = str(x).strip().upper()
        if not s or s in {"NAN", "NONE", "NAT", "NULL", "-", "—"}:
            continue
        out.add(s)
    names = sorted(out)
    print(f"[price_store] universe={len(names)} from {files[-1].name}")
    return names


def _load_store() -> pd.DataFrame:
    if not STORE_PATH.exists():
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])
    df = pd.read_parquet(STORE_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    return df


def _save_store(df: pd.DataFrame) -> None:
    PRICE_DIR.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df.drop_duplicates(subset=["date", "ticker"], keep="last")
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    df.to_parquet(STORE_PATH, index=False)
    meta = {
        "last_date": str(df["date"].max().date()) if len(df) else None,
        "first_date": str(df["date"].min().date()) if len(df) else None,
        "n_rows": int(len(df)),
        "n_tickers": int(df["ticker"].nunique()) if len(df) else 0,
        "updated": datetime.now(ET).isoformat(),
    }
    META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(
        f"[price_store] saved {meta['n_rows']:,} rows / {meta['n_tickers']:,} tickers "
        f"[{meta['first_date']} → {meta['last_date']}]")


def status() -> None:
    if META_PATH.exists():
        print(META_PATH.read_text())
    else:
        print("[price_store] empty — run bootstrap")
    if STORE_PATH.exists():
        print(f"file: {STORE_PATH} ({STORE_PATH.stat().st_size/1e6:.1f} MB)")


class PriceStoreUnavailable(RuntimeError):
    """yfinance is not importable in this environment.

    A plain ``Exception`` on purpose: factor-mine's ``price ensure`` is
    best-effort and guards with ``except Exception``. A ``SystemExit`` here
    slipped through that guard and killed the Factor strategy mine job
    (2026-09-10 13:17Z) with no traceback.
    """


def _yf_download(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError as e:
        raise PriceStoreUnavailable(f"[price_store] yfinance required: {e}") from e
    if not tickers:
        return pd.DataFrame()
    try:
        # Printed regular-session Open/High/Low/Close — not split-adjusted
        # history and not a live last-trade. Factor-mine 09:30 / 16:00
        # marks must match the tape the user can look up.
        raw = yf.download(
            tickers=tickers, start=start, end=end, group_by="ticker",
            auto_adjust=False, actions=False, threads=True, progress=False,
        )
    except Exception as e:
        print(f"[price_store] download failed ({len(tickers)}): {e}")
        return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()
    return _flatten_yf(raw, tickers)


def _flatten_yf(raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Normalize yfinance 0.2 / 1.x column layouts to a long OHLC table."""
    def _one(df: pd.DataFrame, sym: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        part = df.dropna(how="all").copy()
        if part.empty:
            return pd.DataFrame()
        if isinstance(part.columns, pd.MultiIndex):
            part.columns = [
                str(c[-1] if c[-1] not in ("", None) else c[0]) for c in part.columns
            ]
        cols = {str(c): c for c in part.columns}
        lower = {str(c).strip().lower(): c for c in part.columns}
        need = {"open": None, "high": None, "low": None, "close": None, "volume": None}
        for key in list(need):
            for cand in (key, key.title(), key.capitalize()):
                if cand in cols:
                    need[key] = cols[cand]
                    break
                if cand in lower:
                    need[key] = lower[cand]
                    break
        if need["close"] is None:
            return pd.DataFrame()
        part = part.reset_index()
        date_col = "Date" if "Date" in part.columns else part.columns[0]
        out = pd.DataFrame({
            "date": pd.to_datetime(part[date_col], errors="coerce"),
            "ticker": sym,
            "open": part[need["open"]] if need["open"] is not None else None,
            "high": part[need["high"]] if need["high"] is not None else None,
            "low": part[need["low"]] if need["low"] is not None else None,
            "close": part[need["close"]],
            "volume": part[need["volume"]] if need["volume"] is not None else None,
        })
        return out.dropna(subset=["close"])

    if not isinstance(raw.columns, pd.MultiIndex):
        if len(tickers) == 1:
            return _one(raw, tickers[0])
        return pd.DataFrame()
    levels0 = set(raw.columns.get_level_values(0))
    levels1 = set(raw.columns.get_level_values(1)) if raw.columns.nlevels > 1 else set()
    rows = []
    ticker_set = {str(t).upper() for t in tickers}
    if ticker_set & {str(x).upper() for x in levels0}:
        for sym in tickers:
            if sym not in levels0:
                continue
            try:
                rows.append(_one(raw[sym], sym))
            except Exception:
                continue
    elif ticker_set & {str(x).upper() for x in levels1}:
        for sym in tickers:
            try:
                sub = raw.xs(sym, axis=1, level=1, drop_level=True)
            except Exception:
                continue
            rows.append(_one(sub, sym))
    else:
        if len(tickers) == 1:
            rows.append(_one(raw, tickers[0]))
    rows = [r for r in rows if r is not None and len(r)]
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def bootstrap(days: int = 400, tickers: list[str] | None = None, resume: bool = True) -> None:
    names = tickers or _universe_tickers()
    end = datetime.now(ET).date() + timedelta(days=1)
    start = end - timedelta(days=days)
    existing = _load_store() if resume else pd.DataFrame()
    have = set(existing["ticker"].unique()) if len(existing) else set()
    if len(existing):
        counts = existing.groupby("ticker")["date"].nunique()
        min_bars = max(30, days // 4)
        short = set(counts[counts < min_bars].index)
        need = [t for t in names if t not in have or t in short]
    else:
        need = list(names)
    print(f"[price_store] bootstrap days={days} universe={len(names)} need={len(need)}")
    frames = [existing] if len(existing) else []
    for i in range(0, len(need), CHUNK):
        batch = need[i : i + CHUNK]
        print(f"[price_store] chunk {i//CHUNK+1}/{(len(need)-1)//CHUNK+1} ({batch[0]}…{batch[-1]})")
        part = _yf_download(batch, start.isoformat(), end.isoformat())
        if len(part):
            frames.append(part)
            if (i // CHUNK) % 5 == 4:
                _save_store(pd.concat(frames, ignore_index=True))
    if not frames:
        raise SystemExit("[price_store] nothing downloaded")
    _save_store(pd.concat(frames, ignore_index=True))


def update(lookback_days: int = 7) -> None:
    names = _universe_tickers()
    existing = _load_store()
    end = datetime.now(ET).date() + timedelta(days=1)
    if len(existing):
        last = existing["date"].max().date()
        start = last - timedelta(days=2)
    else:
        start = end - timedelta(days=max(lookback_days, 30))
        print("[price_store] empty store — short window; run bootstrap for full history")
    print(f"[price_store] update {start} → {end} for {len(names)} tickers")
    import time
    frames = [existing] if len(existing) else []
    n_chunks = max(1, (len(names) - 1) // CHUNK + 1)
    for i in range(0, len(names), CHUNK):
        batch = names[i : i + CHUNK]
        print(f"[price_store] update chunk {i//CHUNK+1}/{n_chunks}")
        part = _yf_download(batch, start.isoformat(), end.isoformat())
        if not len(part):
            time.sleep(4)
            part = _yf_download(batch, start.isoformat(), end.isoformat())
        if len(part):
            frames.append(part)
        time.sleep(1.5)
    if not frames:
        raise SystemExit("[price_store] update got nothing")
    _save_store(pd.concat(frames, ignore_index=True))


def fill_range(start: str, end: str, tickers: list[str] | None = None) -> None:
    """Download official regular-session bars for every universe name in [start, end].

    ``update()`` keys off the store's max date, so a handful of seeded
    tickers through Friday would skip everyone else. Factor-mine marks
    every name; this fills the hole.
    """
    import time
    names = tickers or _universe_tickers()
    existing = _load_store()
    print(f"[price_store] fill_range {start} → {end} for {len(names)} tickers")
    frames = [existing] if len(existing) else []
    n_chunks = max(1, (len(names) - 1) // CHUNK + 1)
    for i in range(0, len(names), CHUNK):
        batch = names[i : i + CHUNK]
        print(f"[price_store] fill chunk {i//CHUNK+1}/{n_chunks} ({batch[0]}…{batch[-1]})")
        try:
            part = _yf_download(batch, start, end)
            if not len(part):
                time.sleep(6)
                part = _yf_download(batch, start, end)
        except PriceStoreUnavailable:
            raise  # no yfinance — every chunk would fail; let the caller skip
        except Exception as e:
            print(f"[price_store] fill chunk failed: {e}")
            part = pd.DataFrame()
        if len(part):
            frames.append(part)
            if (i // CHUNK) % 8 == 7:
                _save_store(pd.concat(frames, ignore_index=True))
                frames = [_load_store()]
        time.sleep(1.5)
    if frames:
        _save_store(pd.concat(frames, ignore_index=True))


def ensure_through(end: str | None = None,
                   tickers: list[str] | None = None) -> None:
    """Fill official regular-session bars through the last closed session.

    Prefer the names we actually mark: a full-universe yahoo walk is slow
    and rate-limited, and leftover lots are what lose their overnight /
    session status when the new session never lands.
    """
    from .skip_if_good import last_closed_session

    existing = _load_store()
    closed = last_closed_session()
    target_s = str(end or closed)[:10]
    if target_s > closed:
        target_s = closed
    want = sorted({str(t).upper() for t in (tickers or []) if t})
    n_on = 0
    have = set()
    if len(existing):
        last = existing["date"].max().date().isoformat()
        on = existing[existing["date"] == pd.Timestamp(target_s)]
        n_on = int(on["ticker"].nunique()) if len(on) else 0
        if len(on):
            have = {str(t).upper() for t in on["ticker"].tolist()}
        if want:
            missing = [t for t in want if t not in have]
            if not missing and last >= target_s:
                print(f"[price_store] ensure_through {target_s} "
                      f"have {len(want)} requested tickers")
                return
            want = missing or want
        elif last >= target_s and n_on >= 8000:
            print(f"[price_store] ensure_through {target_s} have {n_on} tickers")
            return
    print(f"[price_store] ensure_through → {target_s} "
          f"(have {n_on} bars; fetch {len(want) if want else 'universe'})")
    start = (datetime.strptime(target_s, "%Y-%m-%d") - timedelta(days=21)).date().isoformat()
    # Date-only: yfinance parses "YYYY-MM-DD" and rejects a datetime
    # isoformat ("T00:00:00" → ValueError on every ticker, so the new
    # session never landed and leftover lots kept $0 marks).
    stop = (datetime.strptime(target_s, "%Y-%m-%d") + timedelta(days=1)).date().isoformat()
    fill_range(start, stop, want or None)


def candle_bias(ohlc: pd.DataFrame, lookback: int = 10) -> dict:
    empty = {
        "pass": None, "detail": "no OHLC", "bull": 0,
        "green": np.nan, "red": np.nan,
        "asof": None, "window_start": None, "window_end": None, "n": 0, "sessions": "",
    }
    if ohlc is None or ohlc.empty:
        return empty
    cols = {c.lower(): c for c in ohlc.columns}
    if "open" not in cols or "close" not in cols:
        return empty
    ohlc = ohlc.rename(columns={cols["open"]: "open", cols["close"]: "close"})
    df = ohlc.dropna(subset=["open", "close"]).copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df = df.sort_index().tail(lookback)
    if len(df) < 3:
        empty["detail"] = f"only {len(df)} bars"
        empty["n"] = len(df)
        return empty
    parts, green, red = [], 0.0, 0.0
    for dt, row in df.iterrows():
        o, c = float(row["open"]), float(row["close"])
        body = c - o
        flag = "G" if body > 0 else ("R" if body < 0 else "F")
        if body > 0:
            green += body
        elif body < 0:
            red += -body
        parts.append(f"{dt.date()}:{o:.4f}->{c:.4f}:{body:+.4f}:{flag}")
    bias = green - red
    asof = df.index[-1].date().isoformat()
    return {
        "pass": bias > 0,
        "detail": (
            f"asof={asof} window={df.index[0].date()}→{df.index[-1].date()} "
            f"n={len(df)} green={green:.4f} red={red:.4f} bias={bias:.4f}"
        ),
        "bull": 1 if bias > 0 else -1,
        "green": green, "red": red, "asof": asof,
        "window_start": df.index[0].date().isoformat(),
        "window_end": df.index[-1].date().isoformat(),
        "n": int(len(df)), "sessions": "|".join(parts),
    }


def consecutive_down(closes: pd.Series) -> dict:
    s = closes.dropna().sort_index()
    if not isinstance(s.index, pd.DatetimeIndex):
        s.index = pd.to_datetime(s.index)
    if len(s) < 2:
        return {"pass": False, "detail": "short history", "bull": 0, "n": 0, "asof": None, "steps": ""}
    steps, n = [], 0
    for i in range(len(s) - 1, 0, -1):
        c0, c1 = float(s.iloc[i - 1]), float(s.iloc[i])
        d0, d1 = s.index[i - 1].date().isoformat(), s.index[i].date().isoformat()
        if c1 < c0:
            n += 1
            steps.append(f"{d0}:{c0:.4f}->{d1}:{c1:.4f}")
        else:
            break
    steps.reverse()
    asof = s.index[-1].date().isoformat()
    ok = n >= 3
    return {
        "pass": ok,
        "detail": f"asof={asof} consecutive_down={n} (need≥3) steps={';'.join(steps) if steps else 'none'}",
        "bull": 1 if ok else 0, "n": n, "asof": asof, "steps": ";".join(steps),
    }


def _rel_line(close: pd.Series):
    s = close.dropna().sort_index()
    if not isinstance(s.index, pd.DatetimeIndex):
        s.index = pd.to_datetime(s.index)
    if s.empty:
        return s, np.nan, None
    t_last = s.index[-1]
    t0 = t_last - pd.Timedelta(days=365)
    base_c = s[s.index <= t0]
    if len(base_c):
        base = float(base_c.iloc[-1])
        base_dt = base_c.index[-1].date().isoformat()
    else:
        base = float(s.iloc[0])
        base_dt = s.index[0].date().isoformat()
    if not base:
        return s * np.nan, np.nan, base_dt
    return s / base - 1.0, base, base_dt


def peer_compare_7d(ticker: str, peers: list[str], close_panel: pd.DataFrame, horizon: int = 7) -> dict:
    empty = {
        "pass_outperform": None, "pass_breadth": None, "detail": "no price panel / ticker missing",
        "bull_outperform": 0, "bull_breadth": 0, "rs_7d": np.nan, "overtake_7d": False,
        "leadership_7d": False, "peer_breadth_7d": np.nan, "peers_used": "", "ret_7d": np.nan,
        "asof": None, "d0": None, "d1": None, "px_d0": np.nan, "px_d1": np.nan,
        "baseline_date": None, "baseline_px": np.nan, "rel_d0": np.nan, "rel_d1": np.nan,
        "peer_med_rel_d0": np.nan, "peer_med_rel_d1": np.nan, "peer_med_ret_7d": np.nan, "peer_rets": "",
    }
    if close_panel is None or close_panel.empty or ticker not in close_panel.columns:
        return empty
    stock = close_panel[ticker].dropna().sort_index()
    if not isinstance(stock.index, pd.DatetimeIndex):
        stock.index = pd.to_datetime(stock.index)
    if len(stock) < horizon + 2:
        empty["detail"] = "short stock history"
        return empty
    d1, d0 = stock.index[-1], stock.index[-(horizon + 1)]
    px_d1, px_d0 = float(stock.iloc[-1]), float(stock.iloc[-(horizon + 1)])
    ret_7 = px_d1 / px_d0 - 1.0 if px_d0 else np.nan
    used, peer_ret7, peer_bits = [], [], []
    for p in peers:
        if p == ticker or p not in close_panel.columns:
            continue
        series = close_panel[p].dropna().sort_index()
        if not isinstance(series.index, pd.DatetimeIndex):
            series.index = pd.to_datetime(series.index)
        if len(series) < horizon + 2:
            continue
        p_d1, p_d0 = float(series.iloc[-1]), float(series.iloc[-(horizon + 1)])
        p_ret = p_d1 / p_d0 - 1.0 if p_d0 else np.nan
        used.append(p)
        peer_ret7.append(p_ret)
        peer_bits.append(
            f"{p}:{series.index[-(horizon+1)].date()}->{series.index[-1].date()}:"
            f"{p_d0:.4f}->{p_d1:.4f}:{p_ret:+.2%}"
        )
    if not used:
        return {
            **empty, "detail": "no peers with prices", "ret_7d": ret_7,
            "asof": d1.date().isoformat(), "d0": d0.date().isoformat(), "d1": d1.date().isoformat(),
            "px_d0": px_d0, "px_d1": px_d1,
        }
    stock_rel, base_px, base_dt = _rel_line(close_panel[ticker])
    peer_rel_map = {p: _rel_line(close_panel[p])[0] for p in used}
    win = stock_rel.dropna().index[-(horizon + 1):]
    s_rel = stock_rel.reindex(win).ffill()
    peer_mat = pd.DataFrame({p: peer_rel_map[p].reindex(win).ffill() for p in used})
    med = peer_mat.median(axis=1)
    s0, s1 = float(s_rel.iloc[0]), float(s_rel.iloc[-1])
    m0, m1 = float(med.iloc[0]), float(med.iloc[-1])
    rs_7d = (s1 - s0) - (m1 - m0)
    overtake = (s0 <= m0) and (s1 > m1)
    leadership = (s1 - s0) > (m1 - m0)
    breadth = float(np.mean([1.0 if (r == r and r > 0) else 0.0 for r in peer_ret7]))
    med_peer_ret = float(np.nanmedian(peer_ret7))
    bull_o = 2 if overtake else (1 if leadership or rs_7d > 0 else (-1 if rs_7d < -0.02 else 0))
    bull_b = 1 if breadth >= 0.6 else (-1 if breadth <= 0.3 else 0)
    detail = (
        f"asof={d1.date()} d0={d0.date()} d1={d1.date()} "
        f"px {px_d0:.4f}->{px_d1:.4f} ret7={ret_7:+.2%} | "
        f"baseline@{base_dt}={base_px:.4f} rel {s0:+.2%}->{s1:+.2%} "
        f"peerMedRel {m0:+.2%}->{m1:+.2%} rs7={rs_7d:+.2%} | "
        f"breadth={breadth:.0%} overtake={overtake} lead={leadership}"
    )
    return {
        "pass_outperform": bool(overtake or leadership or rs_7d > 0),
        "pass_breadth": breadth >= 0.5, "detail": detail,
        "bull_outperform": bull_o, "bull_breadth": bull_b,
        "rs_7d": rs_7d, "overtake_7d": bool(overtake), "leadership_7d": bool(leadership),
        "peer_breadth_7d": breadth, "peers_used": "|".join(used), "ret_7d": ret_7,
        "asof": d1.date().isoformat(), "d0": d0.date().isoformat(), "d1": d1.date().isoformat(),
        "px_d0": px_d0, "px_d1": px_d1, "baseline_date": base_dt, "baseline_px": base_px,
        "rel_d0": s0, "rel_d1": s1, "peer_med_rel_d0": m0, "peer_med_rel_d1": m1,
        "peer_med_ret_7d": med_peer_ret, "peer_rets": "|".join(peer_bits),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bootstrap")
    b.add_argument("--days", type=int, default=400)
    b.add_argument("--tickers", default=None)
    b.add_argument("--no-resume", action="store_true")
    u = sub.add_parser("update")
    u.add_argument("--lookback-days", type=int, default=7)
    f = sub.add_parser("fill-range")
    f.add_argument("--start", required=True)
    f.add_argument("--end", required=True)
    f.add_argument("--tickers", default=None)
    sub.add_parser("status")
    args = ap.parse_args()
    if args.cmd == "status":
        status()
    elif args.cmd == "bootstrap":
        tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
        bootstrap(days=args.days, tickers=tickers, resume=not args.no_resume)
    elif args.cmd == "update":
        update(lookback_days=args.lookback_days)
    elif args.cmd == "fill-range":
        tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
        fill_range(args.start, args.end, tickers=tickers)


if __name__ == "__main__":
    try:
        main()
    except PriceStoreUnavailable as e:
        raise SystemExit(str(e)) from e
