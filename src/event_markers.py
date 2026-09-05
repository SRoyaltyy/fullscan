"""Finviz-chart-style event markers: E (earnings), R (ratings), D (dividends).

Authoritative source is the quote-page ``chartEvents`` overlay — the same
E / R / D chips Finviz paints on the daily chart. See ``src.finviz_events``.

yfinance is an opt-in fallback only (``--yf``). Its earnings dates often
do not match the Finviz report stamp.

CLI:
  python -m src.event_markers AAPL
  python -m src.event_markers AAPL --asof 2026-09-04
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config
from . import finviz_events as fe

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "events"
COLORS_DIR = ROOT / "data" / "quote_colors"
ET = ZoneInfo(config.TZ)


def _yf_earnings(ticker: str, limit: int = 24) -> pd.DataFrame:
    """Return rows: event_date, eps_est, eps_act, surprise_pct, color (green/red/white)."""
    try:
        import yfinance as yf
    except ImportError as e:
        raise SystemExit("yfinance required: pip install yfinance") from e

    t = yf.Ticker(ticker.upper())
    df = None
    try:
        df = t.get_earnings_dates(limit=limit)
    except Exception:
        df = None
    if df is None or (hasattr(df, "empty") and df.empty):
        try:
            df = t.earnings_dates
        except Exception:
            df = None
    if df is None or (hasattr(df, "empty") and df.empty):
        return pd.DataFrame(
            columns=["event_date", "eps_est", "eps_act", "surprise_pct", "color", "label"]
        )

    out = df.copy()
    # index is usually DatetimeIndex of earnings date
    if not isinstance(out.index, pd.DatetimeIndex):
        if "Earnings Date" in out.columns:
            out.index = pd.to_datetime(out["Earnings Date"], errors="coerce")
        else:
            out.index = pd.to_datetime(out.index, errors="coerce")

    # normalize column names
    colmap = {c.lower().strip(): c for c in out.columns}

    def col(*names):
        for n in names:
            if n in colmap:
                return colmap[n]
            for k, orig in colmap.items():
                if n in k:
                    return orig
        return None

    c_est = col("eps estimate", "estimate")
    c_act = col("reported eps", "actual", "eps actual")
    c_sur = col("surprise", "surprise(%)")

    rows = []
    for dt, r in out.iterrows():
        if pd.isna(dt):
            continue
        ed = pd.Timestamp(dt).tz_localize(None).date().isoformat()
        est = float(r[c_est]) if c_est and pd.notna(r.get(c_est)) else np.nan
        act = float(r[c_act]) if c_act and pd.notna(r.get(c_act)) else np.nan
        sur = float(r[c_sur]) if c_sur and pd.notna(r.get(c_sur)) else np.nan
        if not np.isfinite(sur) and np.isfinite(est) and np.isfinite(act) and est != 0:
            sur = (act - est) / abs(est) * 100.0

        if np.isfinite(sur):
            if sur > 0:
                color, label = "green", "E_BEAT"
            elif sur < 0:
                color, label = "red", "E_MISS"
            else:
                color, label = "white", "E_INLINE"
        elif np.isfinite(act) and np.isfinite(est):
            color, label = ("green", "E_BEAT") if act >= est else ("red", "E_MISS")
        else:
            # date known, result unknown (future or missing) → white E
            color, label = "white", "E"

        rows.append(
            {
                "ticker": ticker.upper(),
                "kind": "E",
                "event_date": ed,
                "eps_est": est,
                "eps_act": act,
                "surprise_pct": sur,
                "color": color,
                "label": label,
            }
        )
    return pd.DataFrame(rows)


def _yf_recommendations(ticker: str) -> pd.DataFrame:
    """Analyst grade changes → R markers.

    yfinance recommendations columns vary; we map firm grade changes to up/down.
    """
    try:
        import yfinance as yf
    except ImportError:
        return pd.DataFrame()

    t = yf.Ticker(ticker.upper())
    try:
        rec = t.recommendations
    except Exception:
        rec = None
    if rec is None or (hasattr(rec, "empty") and rec.empty):
        try:
            rec = t.recommendations_summary
        except Exception:
            rec = None
    if rec is None or (hasattr(rec, "empty") and rec.empty):
        return pd.DataFrame(
            columns=["ticker", "kind", "event_date", "color", "label", "detail"]
        )

    out = rec.copy()
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, errors="coerce")

    # common cols: Firm, To Grade, From Grade, Action
    cols = {c.lower(): c for c in out.columns}
    c_to = cols.get("to grade") or cols.get("to_grade")
    c_from = cols.get("from grade") or cols.get("from_grade")
    c_act = cols.get("action")

    rank = {
        "strong buy": 5,
        "buy": 4,
        "outperform": 4,
        "overweight": 4,
        "hold": 3,
        "neutral": 3,
        "market perform": 3,
        "equal-weight": 3,
        "underperform": 2,
        "underweight": 2,
        "sell": 1,
        "strong sell": 0,
    }

    def grade_num(x):
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return np.nan
        s = str(x).strip().lower()
        return rank.get(s, np.nan)

    rows = []
    for dt, r in out.iterrows():
        if pd.isna(dt):
            continue
        ed = pd.Timestamp(dt).tz_localize(None).date().isoformat()
        action = str(r[c_act]).strip().lower() if c_act and pd.notna(r.get(c_act)) else ""
        to_g = r[c_to] if c_to else None
        from_g = r[c_from] if c_from else None
        to_n, from_n = grade_num(to_g), grade_num(from_g)

        color, label = "white", "R"
        if "upgrade" in action or (np.isfinite(to_n) and np.isfinite(from_n) and to_n > from_n):
            color, label = "green", "R_UP"
        elif "downgrade" in action or (
            np.isfinite(to_n) and np.isfinite(from_n) and to_n < from_n
        ):
            color, label = "red", "R_DOWN"
        elif "init" in action or "assume" in action:
            color, label = "white", "R_INIT"

        rows.append(
            {
                "ticker": ticker.upper(),
                "kind": "R",
                "event_date": ed,
                "color": color,
                "label": label,
                "detail": f"{action}|{from_g}->{to_g}".strip("|"),
                "eps_est": np.nan,
                "eps_act": np.nan,
                "surprise_pct": np.nan,
            }
        )
    return pd.DataFrame(rows)


def _quote_analyst_fallback(ticker: str) -> pd.DataFrame:
    """Use latest quote_colors detail JSON if yfinance recommendations empty."""
    if not COLORS_DIR.exists():
        return pd.DataFrame()
    files = sorted(COLORS_DIR.glob("*_quote_colors_detail.json"))
    if not files:
        return pd.DataFrame()
    try:
        data = json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:
        return pd.DataFrame()
    # structure varies — look for ticker key or list of analyst actions
    rows = []
    blob = data.get(ticker.upper()) or data.get("tickers", {}).get(ticker.upper()) or {}
    actions = blob.get("analyst_actions") or blob.get("analyst") or []
    if isinstance(actions, dict):
        actions = actions.get("last2") or actions.get("items") or []
    for a in actions if isinstance(actions, list) else []:
        if not isinstance(a, dict):
            continue
        ed = str(a.get("date") or a.get("event_date") or "")[:10]
        kind = str(a.get("action") or a.get("type") or "").lower()
        color, label = "white", "R"
        if "up" in kind:
            color, label = "green", "R_UP"
        elif "down" in kind:
            color, label = "red", "R_DOWN"
        rows.append(
            {
                "ticker": ticker.upper(),
                "kind": "R",
                "event_date": ed,
                "color": color,
                "label": label,
                "detail": str(a),
                "eps_est": np.nan,
                "eps_act": np.nan,
                "surprise_pct": np.nan,
            }
        )
    return pd.DataFrame(rows)


def fetch(ticker: str, limit_earnings: int = 24, *,
          html: str | None = None, live: bool = False,
          yf: bool = False) -> pd.DataFrame:
    rows = fe.fetch(ticker, html=html, live=live)
    if not rows and yf:
        e = _yf_earnings(ticker, limit=limit_earnings)
        r = _yf_recommendations(ticker)
        if r.empty:
            r = _quote_analyst_fallback(ticker)
        parts = [x for x in (e, r) if x is not None and len(x)]
        if parts:
            out = pd.concat(parts, ignore_index=True, sort=False)
            out["event_date"] = out["event_date"].astype(str).str[:10]
            return out.sort_values(["event_date", "kind"]).reset_index(drop=True)
    if not rows:
        return pd.DataFrame(
            columns=[
                "ticker", "kind", "event_date", "color", "label",
                "eps_est", "eps_act", "surprise_pct", "detail",
            ]
        )
    return pd.DataFrame(rows)


def asof_snapshot(events: pd.DataFrame, asof: str) -> dict:
    if events is not None and not getattr(events, "empty", True):
        recs = events.to_dict("records")
        snap = fe.asof_snapshot(recs, asof)
        # Keep the historical key set plus D / cell from Finviz.
        return snap
    return fe.asof_snapshot([], asof)


def save(ticker: str, events: pd.DataFrame) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"{ticker.upper()}_events.csv"
    events.to_csv(path, index=False)
    md = OUT_DIR / f"{ticker.upper()}_events.md"
    lines = [
        f"# Event markers — {ticker.upper()}",
        "",
        "Finviz-chart style: **E** = earnings, **R** = analyst action, **D** = dividend.",
        "Color: green = beat/upgrade, red = miss/downgrade, blue = ex-div, white = unknown/init.",
        "",
        "| date | kind | color | label | surprise% | detail |",
        "|------|------|-------|-------|----------:|--------|",
    ]
    for _, r in events.sort_values("event_date", ascending=False).head(40).iterrows():
        sur = r.get("surprise_pct")
        sur_s = f"{sur:+.1f}" if sur is not None and np.isfinite(sur) else "—"
        chip = {"green": "🟢", "red": "🔴", "blue": "🔵", "white": "⚪"}.get(str(r.get("color")), "⚪")
        lines.append(
            f"| {r.get('event_date')} | {r.get('kind')} | {chip} | {r.get('label')} | "
            f"{sur_s} | {str(r.get('detail') or '')[:40]} |"
        )
    md.write_text("\n".join(lines), encoding="utf-8")
    print(f"[events] {ticker}: {len(events)} rows → {path.name}, {md.name}")
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker")
    ap.add_argument("--asof", default=None, help="If set, print PIT snapshot only")
    ap.add_argument("--limit", type=int, default=24)
    ap.add_argument("--html", default="", help="Finviz quote-page HTML")
    ap.add_argument("--live", action="store_true", help="Fetch Elite quote page")
    ap.add_argument("--yf", action="store_true", help="yfinance fallback (dates often disagree)")
    args = ap.parse_args()
    html = Path(args.html).read_text(encoding="utf-8") if args.html else None
    ev = fetch(args.ticker, limit_earnings=args.limit,
               html=html, live=args.live, yf=args.yf)
    if not ev.empty:
        recs = ev.to_dict("records")
        fe.save_ticker_events(args.ticker, recs)
    save(args.ticker, ev)
    if args.asof:
        snap = asof_snapshot(ev, args.asof)
        print(json.dumps(snap, indent=2, default=str))


if __name__ == "__main__":
    main()
