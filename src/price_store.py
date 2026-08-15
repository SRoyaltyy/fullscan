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
    return sorted(df[tcol].astype(str).str.strip().str.upper().unique())


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


def _yf_download(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError as e:
        raise SystemExit(f"[price_store] yfinance required: {e}") from e

    if not tickers:
        return pd.DataFrame()

    try:
        raw = yf.download(
            tickers=tickers,
            start=start,
            end=end,
            group_by="ticker",
            auto_adjust=True,
            threads=True,
            progress=False,
        )
    except Exception as e:
        print(f"[price_store] download failed ({len(tickers)}): {e}")
        return pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame()

    rows = []
    if len(tickers) == 1:
        sym = tickers[0]
        if not isinstance(raw, pd.DataFrame) or "Close" not in raw.columns:
            return pd.DataFrame()
        part = raw.copy().reset_index()
        date_col = "Date" if "Date" in part.columns else part.columns[0]
        part["ticker"] = sym
        part = part.rename(columns={
            date_col: "date", "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        keep = [c for c in ["date", "ticker", "open", "high", "low", "close", "volume"] if c in part.columns]
        return part[keep].dropna(subset=["close"])

    if not isinstance(raw.columns, pd.MultiIndex):
        return pd.DataFrame()

    level0 = set(raw.columns.get_level_values(0))
    for sym in tickers:
        if sym not in level0:
            continue
        try:
            sub = raw[sym].dropna(how="all").copy().reset_index()
        except Exception:
            continue
        if sub.empty or "Close" not in sub.columns:
            continue
        date_col = "Date" if "Date" in sub.columns else sub.columns[0]
        sub["ticker"] = sym
        sub = sub.rename(columns={
            date_col: "date", "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        keep = [c for c in ["date", "ticker", "open", "high", "low", "close", "volume"] if c in sub.columns]
        rows.append(sub[keep])
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True).dropna(subset=["close"])


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
    frames = [existing] if len(existing) else []
    for i in range(0, len(names), CHUNK):
        batch = names[i : i + CHUNK]
        if (i // CHUNK) % 10 == 0:
            print(f"[price_store] update chunk {i//CHUNK+1}/{(len(names)-1)//CHUNK+1}")
        part = _yf_download(batch, start.isoformat(), end.isoformat())
        if len(part):
            frames.append(part)
    if not frames:
        raise SystemExit("[price_store] update got nothing")
    _save_store(pd.concat(frames, ignore_index=True))


def candle_bias(ohlc: pd.DataFrame, lookback: int = 10) -> dict:
    if ohlc is None or ohlc.empty or not {"open", "close"}.issubset(
            {c.lower() for c in ohlc.columns} | set(ohlc.columns)):
        # tolerate either case
        cols = {c.lower(): c for c in (ohlc.columns if ohlc is not None else [])}
        if ohlc is None or ohlc.empty or "open" not in cols or "close" not in cols:
            return {"pass": None, "detail": "no OHLC", "bull": 0, "green": np.nan, "red": np.nan}
        ohlc = ohlc.rename(columns={cols["open"]: "open", cols["close"]: "close"})
    df = ohlc.dropna(subset=["open", "close"]).tail(lookback)
    if len(df) < 3:
        return {"pass": None, "detail": f"only {len(df)} bars", "bull": 0, "green": np.nan, "red": np.nan}
    body = df["close"] - df["open"]
    green = float(body[body > 0].sum())
    red = float((-body[body < 0]).sum())
    bias = green - red
    return {
        "pass": bias > 0,
        "detail": f"green={green:.4f} red={red:.4f} bias={bias:.4f} n={len(df)}",
        "bull": 1 if bias > 0 else -1,
        "green": green,
        "red": red,
    }


def consecutive_down(closes: pd.Series) -> dict:
    s = closes.dropna()
    if len(s) < 2:
        return {"pass": False, "detail": "short history", "bull": 0, "n": 0}
    n = 0
    for i in range(len(s) - 1, 0, -1):
        if float(s.iloc[i]) < float(s.iloc[i - 1]):
            n += 1
        else:
            break
    ok = n >= 3
    return {"pass": ok, "detail": f"{n} consecutive down sessions", "bull": 1 if ok else 0, "n": n}


def _rel_line(close: pd.Series) -> pd.Series:
    s = close.dropna()
    if s.empty:
        return s
    t0 = s.index[-1] - pd.Timedelta(days=365)
    base_c = s[s.index <= t0]
    base = float(base_c.iloc[-1]) if len(base_c) else float(s.iloc[0])
    if not base:
        return s * np.nan
    return s / base - 1.0


def peer_compare_7d(ticker: str, peers: list[str], close_panel: pd.DataFrame, horizon: int = 7) -> dict:
    empty = {
        "pass_outperform": None, "pass_breadth": None, "detail": "no price panel / ticker missing",
        "bull_outperform": 0, "bull_breadth": 0, "rs_7d": np.nan, "overtake_7d": False,
        "leadership_7d": False, "peer_breadth_7d": np.nan, "peers_used": "", "ret_7d": np.nan,
    }
    if close_panel is None or close_panel.empty or ticker not in close_panel.columns:
        return empty
    stock = close_panel[ticker].dropna()
    if len(stock) < horizon + 2:
        empty["detail"] = "short stock history"
        return empty

    used, peer_ret7 = [], []
    for p in peers:
        if p == ticker or p not in close_panel.columns:
            continue
        series = close_panel[p].dropna()
        if len(series) < horizon + 2:
            continue
        used.append(p)
        p_now, p_7 = float(series.iloc[-1]), float(series.iloc[-(horizon + 1)])
        peer_ret7.append(p_now / p_7 - 1.0 if p_7 else np.nan)

    px_now, px_7 = float(stock.iloc[-1]), float(stock.iloc[-(horizon + 1)])
    ret_7 = px_now / px_7 - 1.0 if px_7 else np.nan
    if not used:
        return {
            **empty, "detail": "no peers with prices", "ret_7d": ret_7,
            "pass_outperform": None, "pass_breadth": None,
        }

    stock_rel = _rel_line(close_panel[ticker])
    peer_rels = [_rel_line(close_panel[p]) for p in used]
    idx = stock_rel.dropna().index[-(horizon + 1):]
    s_rel = stock_rel.reindex(idx).ffill()
    peer_mat = pd.DataFrame({p: peer_rels[i].reindex(idx).ffill() for i, p in enumerate(used)})
    med = peer_mat.median(axis=1)
    s0, s1 = float(s_rel.iloc[0]), float(s_rel.iloc[-1])
    m0, m1 = float(med.iloc[0]), float(med.iloc[-1])
    rs_7d = (s1 - s0) - (m1 - m0)
    overtake = (s0 <= m0) and (s1 > m1)
    leadership = (s1 - s0) > (m1 - m0)
    breadth = float(np.mean([1.0 if (r == r and r > 0) else 0.0 for r in peer_ret7]))
    bull_o = 2 if overtake else (1 if leadership or rs_7d > 0 else (-1 if rs_7d < -0.02 else 0))
    bull_b = 1 if breadth >= 0.6 else (-1 if breadth <= 0.3 else 0)
    detail = (
        f"ret7={ret_7:+.1%} breadth={breadth:.0%} rs7={rs_7d:+.2%} "
        f"overtake={overtake} lead={leadership} peers={len(used)}"
    )
    return {
        "pass_outperform": bool(overtake or leadership or rs_7d > 0),
        "pass_breadth": breadth >= 0.5,
        "detail": detail,
        "bull_outperform": bull_o,
        "bull_breadth": bull_b,
        "rs_7d": rs_7d,
        "overtake_7d": bool(overtake),
        "leadership_7d": bool(leadership),
        "peer_breadth_7d": breadth,
        "peers_used": "|".join(used),
        "ret_7d": ret_7,
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
    sub.add_parser("status")
    args = ap.parse_args()
    if args.cmd == "status":
        status()
    elif args.cmd == "bootstrap":
        tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
        bootstrap(days=args.days, tickers=tickers, resume=not args.no_resume)
    elif args.cmd == "update":
        update(lookback_days=args.lookback_days)


if __name__ == "__main__":
    main()
