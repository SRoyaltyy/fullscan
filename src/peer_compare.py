"""Finviz Compare-style peer relative lines (1Y baseline) + 7-day leadership.

Finviz Compare plots, for each name:
    rel(t) = price(t) / price(t0) - 1
where t0 is ~1 year before the as-of date (or first available bar).

Leadership / overtaking uses the path of rel(t) over the **7 trading days**
before as-of (not a single Finviz Performance(Week) snapshot).

CLI:
  python -m src.peer_compare --date 2026-08-14 --tickers XPON,AAPL,NVDA
  python -m src.peer_compare --date 2026-08-14 --from-book --top 40
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config
from .peer_rs import _load_correlations

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
BOOK_DIR = ROOT / "data" / "stock_book"
OUT_DIR = ROOT / "data" / "peers"
DAILY = ROOT / "01_daily"
ET = ZoneInfo(config.TZ)

LOOKBACK_CAL_DAYS = 400
HORIZON_SESSIONS = 7


def _asof(date: str | None) -> str:
    if date:
        return date
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if files:
        return files[-1].stem.replace("finviz_", "")
    return datetime.now(ET).date().isoformat()


def _book_tickers(date: str, top: int) -> list[str]:
    p = BOOK_DIR / f"{date}_stock_book.csv"
    if not p.exists():
        files = sorted(BOOK_DIR.glob("*_stock_book.csv"))
        if not files:
            return []
        p = files[-1]
    df = pd.read_csv(p)
    if "Ticker" not in df.columns:
        return []
    for col in ("score_1d", "score_1w", "checklist_score"):
        if col in df.columns:
            df = df.sort_values(col, ascending=False)
            break
    return list(df["Ticker"].astype(str).str.upper().head(top))


def _download_closes(symbols: list[str], end_date: str) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError as e:
        raise SystemExit(f"[peer_compare] yfinance required: {e}") from e

    end = datetime.fromisoformat(end_date).date() + timedelta(days=1)
    start = end - timedelta(days=LOOKBACK_CAL_DAYS)
    syms = sorted(set(s for s in symbols if s))
    if not syms:
        return pd.DataFrame()

    frames = []
    chunk = 40
    for i in range(0, len(syms), chunk):
        batch = syms[i : i + chunk]
        try:
            raw = yf.download(
                tickers=batch,
                start=start.isoformat(),
                end=end.isoformat(),
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
            )
        except Exception as e:
            print(f"[peer_compare] download chunk failed: {e}")
            continue
        if raw is None or raw.empty:
            continue
        if len(batch) == 1:
            sym = batch[0]
            if "Close" in raw.columns:
                frames.append(raw[["Close"]].rename(columns={"Close": sym}))
            continue
        if isinstance(raw.columns, pd.MultiIndex):
            closes = {}
            level0 = raw.columns.get_level_values(0)
            for sym in batch:
                if sym not in level0:
                    continue
                try:
                    closes[sym] = raw[sym]["Close"]
                except Exception:
                    continue
            if closes:
                frames.append(pd.DataFrame(closes))

    if not frames:
        return pd.DataFrame()
    panel = pd.concat(frames, axis=1)
    panel = panel.loc[:, ~panel.columns.duplicated()]
    panel.index = pd.to_datetime(panel.index).tz_localize(None).normalize()
    panel = panel.sort_index()
    panel = panel[panel.index.date <= datetime.fromisoformat(end_date).date()]
    return panel.dropna(how="all")


def _rel_line(close: pd.Series) -> pd.Series:
    """rel(t) = price(t)/price(~1Y ago) - 1."""
    s = close.dropna()
    if s.empty:
        return s
    t0 = s.index[-1] - pd.Timedelta(days=365)
    base_candidates = s[s.index <= t0]
    base = float(base_candidates.iloc[-1]) if len(base_candidates) else float(s.iloc[0])
    if base == 0 or base != base:
        return s * np.nan
    return s / base - 1.0


def _analyze_one(ticker: str, peers: list[str], panel: pd.DataFrame, horizon: int = HORIZON_SESSIONS) -> dict:
    if ticker not in panel.columns:
        return {"Ticker": ticker, "ok": False, "detail": "no price history"}
    stock_px = panel[ticker].dropna()
    if len(stock_px) < horizon + 2:
        return {"Ticker": ticker, "ok": False, "detail": "short history"}

    px_now = float(stock_px.iloc[-1])
    px_7 = float(stock_px.iloc[-(horizon + 1)])
    ret_7 = px_now / px_7 - 1.0 if px_7 else np.nan

    stock_rel = _rel_line(panel[ticker])
    peer_rels, peer_ret7, used = [], [], []
    for p in peers:
        if p not in panel.columns or p == ticker:
            continue
        series = panel[p].dropna()
        if len(series) < horizon + 2:
            continue
        used.append(p)
        peer_rels.append(_rel_line(panel[p]))
        p_now = float(series.iloc[-1])
        p_7 = float(series.iloc[-(horizon + 1)])
        peer_ret7.append(p_now / p_7 - 1.0 if p_7 else np.nan)

    if not used:
        return {
            "Ticker": ticker, "ok": True, "n_peers": 0, "ret_7d": ret_7,
            "detail": "no peer prices", "peer_breadth_7d": np.nan, "rs_7d": np.nan,
            "overtake_7d": False, "leadership_7d": False, "peers_used": "",
        }

    idx = stock_rel.dropna().index[-(horizon + 1):]
    s_rel = stock_rel.reindex(idx).ffill()
    peer_mat = pd.DataFrame({p: peer_rels[i].reindex(idx).ffill() for i, p in enumerate(used)})
    med = peer_mat.median(axis=1)

    s0, s1 = float(s_rel.iloc[0]), float(s_rel.iloc[-1])
    m0, m1 = float(med.iloc[0]), float(med.iloc[-1])
    stock_rel_chg = s1 - s0
    med_rel_chg = m1 - m0
    rs_7d = stock_rel_chg - med_rel_chg
    overtake = (s0 <= m0) and (s1 > m1)
    leadership = stock_rel_chg > med_rel_chg
    breadth = float(np.mean([1.0 if (r == r and r > 0) else 0.0 for r in peer_ret7]))
    med_peer_ret = float(np.nanmedian(peer_ret7))

    return {
        "Ticker": ticker,
        "ok": True,
        "n_peers": len(used),
        "peers_used": "|".join(used),
        "ret_7d": ret_7,
        "peer_med_ret_7d": med_peer_ret,
        "peer_breadth_7d": breadth,
        "stock_rel_now": s1,
        "peer_med_rel_now": m1,
        "stock_rel_chg_7d": stock_rel_chg,
        "peer_med_rel_chg_7d": med_rel_chg,
        "rs_7d": rs_7d,
        "overtake_7d": bool(overtake),
        "leadership_7d": bool(leadership),
        "detail": (
            f"ret7={ret_7:+.1%} peerMed7={med_peer_ret:+.1%} breadth={breadth:.0%} "
            f"rs7={rs_7d:+.2%} overtake={overtake} lead={leadership}"
        ),
    }


def run(date=None, tickers=None, from_book=False, top=40) -> pd.DataFrame:
    date = _asof(date)
    corr = _load_correlations()

    if tickers:
        names = [t.upper() for t in tickers]
    elif from_book:
        names = _book_tickers(date, top)
    else:
        files = sorted(EXPORT_DIR.glob(f"finviz_{date}.csv")) or sorted(EXPORT_DIR.glob("finviz_*.csv"))
        if not files:
            raise SystemExit("[peer_compare] no finviz export")
        exp = pd.read_csv(files[-1], low_memory=False)
        tcol = "Ticker" if "Ticker" in exp.columns else exp.columns[0]
        names = list(exp[tcol].astype(str).str.upper().head(top))

    names = [n for n in names if n in corr and corr[n]]
    if not names:
        raise SystemExit("[peer_compare] no tickers with peer lists — add Correlations.xlsx")

    need = set(names)
    for n in names:
        need.update(corr.get(n, [])[:10])
    print(f"[peer_compare] {date}: {len(names)} names, {len(need)} unique symbols")

    panel = _download_closes(sorted(need), date)
    if panel.empty:
        raise SystemExit("[peer_compare] no price panel from yfinance")

    rows = [_analyze_one(t, corr.get(t, []), panel) for t in names]
    out = pd.DataFrame(rows)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DAILY.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"{date}_peer_compare_7d.csv"
    out.to_csv(path, index=False)

    L = [
        f"# Peer Compare 7d — {date}",
        "",
        "Finviz Compare lines = `price(t)/price(~1Y ago) - 1`.",
        f"Leadership judged on the **last {HORIZON_SESSIONS} trading sessions** before as-of.",
        "",
        f"- Names: **{len(out)}**",
        "",
        "| Ticker | ret_7d | peer_med_7d | breadth | rs_7d | overtake | lead | peers |",
        "|---|---|---|---|---|---|---|---|",
    ]
    show = out.copy()
    if "rs_7d" in show.columns:
        show = show.sort_values("rs_7d", ascending=False, key=lambda s: s.fillna(-999))
    for _, r in show.head(30).iterrows():
        if not r.get("ok", True):
            continue
        L.append(
            f"| {r['Ticker']} | {r.get('ret_7d', float('nan')):+.1%} | "
            f"{r.get('peer_med_ret_7d', float('nan')):+.1%} | "
            f"{r.get('peer_breadth_7d', float('nan')):.0%} | "
            f"{r.get('rs_7d', float('nan')):+.2%} | "
            f"{'Y' if r.get('overtake_7d') else 'N'} | "
            f"{'Y' if r.get('leadership_7d') else 'N'} | "
            f"{str(r.get('peers_used', ''))[:36]} |"
        )
    md = DAILY / f"{date}_peer_compare_7d.md"
    md.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"[peer_compare] -> {path.name}, {md.name}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--tickers", default=None)
    ap.add_argument("--from-book", action="store_true")
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
    run(date=args.date, tickers=tickers, from_book=args.from_book, top=args.top)


if __name__ == "__main__":
    main()
