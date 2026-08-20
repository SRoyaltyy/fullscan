"""Interactive price chart with catalyst overlays.

For each catalyst HIT with an event_date, draws:
  - a vertical line on that date
  - a marker at the session close (nearest trading day)

Hover shows taxonomy, headline, weight, confidence, type.

Data
----
  Price : data/prices/ohlc.parquet (price_store) or yfinance fallback
  Events: data/catalyst/{TICKER}_*.json  (written by catalyst_analysis)
          OR --events path/to/file.json
          OR include Finviz-style E/R markers via --earnings

CLI
---
  python -m src.catalyst_chart BB
  python -m src.catalyst_chart BB --days 400
  python -m src.catalyst_chart BB --events data/catalyst/BB_2026-08-19.json
  python -m src.catalyst_chart BB --earnings --days 500

Output
------
  data/charts/{TICKER}_catalyst_chart.html   (open in any browser)
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config

ROOT = Path(__file__).resolve().parent.parent
PRICE_DIR = ROOT / "data" / "prices"
STORE_PATH = PRICE_DIR / "ohlc.parquet"
CATALYST_DIR = ROOT / "data" / "catalyst"
CHART_DIR = ROOT / "data" / "charts"
ET = ZoneInfo(config.TZ)


def _load_ohlc(ticker: str, days: int = 400) -> pd.DataFrame:
    """Return date-indexed OHLCV for ticker."""
    t = ticker.upper()
    end = datetime.now(ET).date() + timedelta(days=1)
    start = end - timedelta(days=days)

    if STORE_PATH.exists():
        try:
            df = pd.read_parquet(STORE_PATH)
            df["ticker"] = df["ticker"].astype(str).str.upper()
            df["date"] = pd.to_datetime(df["date"]).dt.normalize()
            sub = df[df["ticker"] == t].copy()
            if len(sub):
                sub = sub[(sub["date"] >= pd.Timestamp(start)) & (sub["date"] <= pd.Timestamp(end))]
                sub = sub.sort_values("date").drop_duplicates("date")
                if len(sub) >= 10:
                    print(f"[chart] price_store rows={len(sub)} {sub['date'].min().date()}→{sub['date'].max().date()}")
                    return sub.set_index("date")[["open", "high", "low", "close", "volume"]]
        except Exception as e:
            print(f"[chart] price_store read failed: {e}")

    print(f"[chart] falling back to yfinance for {t}")
    try:
        import yfinance as yf
    except ImportError as e:
        raise SystemExit(f"yfinance required: {e}") from e
    raw = yf.download(t, start=start.isoformat(), end=end.isoformat(), auto_adjust=True, progress=False)
    if raw is None or raw.empty:
        raise SystemExit(f"[chart] no price data for {t}")
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0].lower() if isinstance(c, tuple) else str(c).lower() for c in raw.columns]
    else:
        raw.columns = [str(c).lower() for c in raw.columns]
    raw = raw.rename(columns={"adj close": "close"})
    keep = [c for c in ["open", "high", "low", "close", "volume"] if c in raw.columns]
    out = raw[keep].copy()
    out.index = pd.to_datetime(out.index).tz_localize(None).normalize()
    out = out.dropna(subset=["close"])
    print(f"[chart] yfinance rows={len(out)}")
    return out


def _nearest_close(ohlc: pd.DataFrame, event_date: str) -> tuple[pd.Timestamp | None, float | None]:
    """Map an event calendar date to the nearest trading-day close on/after, else before."""
    try:
        ed = pd.Timestamp(event_date).normalize()
    except Exception:
        return None, None
    if ohlc.empty:
        return None, None
    idx = ohlc.index
    # on or after
    after = idx[idx >= ed]
    if len(after):
        dt = after[0]
        return dt, float(ohlc.loc[dt, "close"])
    before = idx[idx <= ed]
    if len(before):
        dt = before[-1]
        return dt, float(ohlc.loc[dt, "close"])
    return None, None


def _parse_events_from_catalyst_json(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    grid = data.get("catalyst_grid") or data.get("events") or []
    if isinstance(data, list):
        grid = data
    out = []
    for e in grid:
        if not isinstance(e, dict):
            continue
        status = str(e.get("status") or "HIT").upper()
        if status not in ("HIT", "TRUE", "1", "YES"):
            # allow records without status field
            if e.get("status") is not None and status == "MISS":
                continue
        ed = e.get("event_date") or e.get("date")
        if not ed or str(ed) in ("?", "None", "null", ""):
            continue
        # normalize date
        m = re.match(r"(\d{4}-\d{2}-\d{2})", str(ed))
        if not m:
            continue
        ed = m.group(1)
        typ = str(e.get("type") or "positive").lower()
        if typ not in ("positive", "negative"):
            typ = "positive" if float(e.get("adjusted_weight") or e.get("base_weight") or 0) >= 0 else "negative"
        out.append(
            {
                "event_date": ed,
                "type": typ,
                "taxonomy": e.get("taxonomy") or e.get("label") or "catalyst",
                "headline": (e.get("headline") or e.get("description") or e.get("evidence_excerpt") or "")[:200],
                "confidence": e.get("confidence"),
                "weight": e.get("adjusted_weight", e.get("base_weight")),
                "source": "catalyst",
                "url": (e.get("source_urls") or [None])[0] if isinstance(e.get("source_urls"), list) else e.get("url"),
            }
        )
    return out


def _latest_catalyst_json(ticker: str) -> Path | None:
    if not CATALYST_DIR.exists():
        return None
    t = ticker.upper()
    cands = sorted(CATALYST_DIR.glob(f"{t}_*.json")) + sorted(CATALYST_DIR.glob(f"{t.lower()}_*.json"))
    # also any file containing ticker
    if not cands:
        cands = sorted(CATALYST_DIR.glob("*.json"))
        cands = [p for p in cands if t in p.stem.upper()]
    return cands[-1] if cands else None


def _load_events(ticker: str, events_path: str | None) -> list[dict]:
    if events_path:
        p = Path(events_path)
        if not p.exists():
            raise SystemExit(f"[chart] events file not found: {p}")
        print(f"[chart] events from {p}")
        return _parse_events_from_catalyst_json(p)
    latest = _latest_catalyst_json(ticker)
    if latest:
        print(f"[chart] events from {latest.relative_to(ROOT)}")
        return _parse_events_from_catalyst_json(latest)
    print("[chart] WARN: no catalyst JSON found under data/catalyst/ — chart will be price-only")
    return []


def _load_earnings_markers(ticker: str) -> list[dict]:
    """Optional E/R markers from src.event_markers."""
    try:
        from . import event_markers as em
    except Exception as e:
        print(f"[chart] event_markers unavailable: {e}")
        return []
    try:
        df = em.fetch(ticker)
    except Exception as e:
        print(f"[chart] event_markers fetch failed: {e}")
        return []
    out = []
    for _, r in df.iterrows():
        ed = str(r.get("event_date") or "")
        if not re.match(r"\d{4}-\d{2}-\d{2}", ed):
            continue
        color = str(r.get("color") or "white").lower()
        label = str(r.get("label") or "E")
        typ = "positive" if color == "green" else ("negative" if color == "red" else "positive")
        out.append(
            {
                "event_date": ed,
                "type": typ,
                "taxonomy": label,
                "headline": f"{label} ({color})",
                "confidence": None,
                "weight": None,
                "source": "earnings" if label.startswith("E") else "analyst",
                "url": None,
            }
        )
    print(f"[chart] earnings/analyst markers={len(out)}")
    return out


def build_chart(
    ticker: str,
    days: int = 400,
    events_path: str | None = None,
    include_earnings: bool = False,
    title_extra: str = "",
) -> Path:
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError as e:
        raise SystemExit(
            "[chart] plotly required: pip install plotly\n"
            f"underlying: {e}"
        ) from e

    t = ticker.upper()
    ohlc = _load_ohlc(t, days=days)
    events = _load_events(t, events_path)
    if include_earnings:
        events = events + _load_earnings_markers(t)

    # de-dupe by (date, taxonomy, type)
    seen = set()
    uniq = []
    for e in events:
        key = (e["event_date"], e["taxonomy"], e["type"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)
    events = sorted(uniq, key=lambda x: x["event_date"])

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.78, 0.22],
    )

    # Candles
    fig.add_trace(
        go.Candlestick(
            x=ohlc.index,
            open=ohlc["open"],
            high=ohlc["high"],
            low=ohlc["low"],
            close=ohlc["close"],
            name="OHLC",
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ),
        row=1,
        col=1,
    )

    # Volume
    if "volume" in ohlc.columns:
        colors = np.where(ohlc["close"] >= ohlc["open"], "rgba(38,166,154,0.45)", "rgba(239,83,80,0.45)")
        fig.add_trace(
            go.Bar(x=ohlc.index, y=ohlc["volume"], name="Volume", marker_color=colors, showlegend=False),
            row=2,
            col=1,
        )

    # Catalyst overlays
    pos_x, pos_y, pos_text = [], [], []
    neg_x, neg_y, neg_text = [], [], []
    shapes = []
    y_lo = float(ohlc["low"].min())
    y_hi = float(ohlc["high"].max())
    pad = (y_hi - y_lo) * 0.02 if y_hi > y_lo else 1.0

    for e in events:
        dt, px = _nearest_close(ohlc, e["event_date"])
        if dt is None or px is None:
            continue
        # vertical line at event date (use trading day we mapped to)
        color = "rgba(38,166,154,0.55)" if e["type"] == "positive" else "rgba(239,83,80,0.55)"
        shapes.append(
            dict(
                type="line",
                xref="x",
                yref="y",
                x0=dt,
                x1=dt,
                y0=y_lo - pad,
                y1=y_hi + pad,
                line=dict(color=color, width=1, dash="dot"),
                layer="below",
            )
        )
        hover = (
            f"<b>{e['taxonomy']}</b><br>"
            f"{e['type'].upper()} · {e['event_date']}<br>"
            f"px≈{px:.2f}<br>"
            f"{e['headline']}<br>"
            f"conf={e.get('confidence')} wt={e.get('weight')}<br>"
            f"source={e.get('source')}"
        )
        if e["type"] == "positive":
            pos_x.append(dt)
            pos_y.append(px)
            pos_text.append(hover)
        else:
            neg_x.append(dt)
            neg_y.append(px)
            neg_text.append(hover)

    if pos_x:
        fig.add_trace(
            go.Scatter(
                x=pos_x,
                y=pos_y,
                mode="markers",
                name="Catalyst +",
                marker=dict(symbol="triangle-up", size=11, color="#26a69a", line=dict(width=1, color="#0b3d38")),
                text=pos_text,
                hoverinfo="text",
            ),
            row=1,
            col=1,
        )
    if neg_x:
        fig.add_trace(
            go.Scatter(
                x=neg_x,
                y=neg_y,
                mode="markers",
                name="Catalyst −",
                marker=dict(symbol="triangle-down", size=11, color="#ef5350", line=dict(width=1, color="#5c1410")),
                text=neg_text,
                hoverinfo="text",
            ),
            row=1,
            col=1,
        )

    net = ""
    # try to surface net signal from json if present
    src = Path(events_path) if events_path else _latest_catalyst_json(t)
    if src and src.exists():
        try:
            meta = json.loads(src.read_text(encoding="utf-8"))
            if isinstance(meta, dict):
                ns = meta.get("net_signal")
                cv = meta.get("conviction")
                if ns:
                    net = f" · signal={ns}" + (f" ({cv})" if cv is not None else "")
        except Exception:
            pass

    fig.update_layout(
        title=dict(
            text=f"{t} — price + catalysts{net}{(' · ' + title_extra) if title_extra else ''}",
            x=0.01,
        ),
        shapes=shapes,
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=780,
        margin=dict(l=50, r=20, t=60, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        hovermode="x unified",
    )
    fig.update_xaxes(
        rangebreaks=[dict(bounds=["sat", "mon"])],  # hide weekends
        rangeselector=dict(
            buttons=[
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all", label="All"),
            ],
            bgcolor="#222",
            activecolor="#444",
        ),
        row=1,
        col=1,
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Vol", row=2, col=1)

    CHART_DIR.mkdir(parents=True, exist_ok=True)
    out = CHART_DIR / f"{t}_catalyst_chart.html"
    fig.write_html(str(out), include_plotlyjs="cdn", full_html=True)
    print(f"[chart] wrote {out.relative_to(ROOT)}  events_plotted={len(pos_x)+len(neg_x)}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Interactive catalyst overlay chart")
    ap.add_argument("ticker", help="Ticker symbol")
    ap.add_argument("--days", type=int, default=400, help="Calendar days of price history")
    ap.add_argument("--events", default=None, help="Path to catalyst JSON (default: latest under data/catalyst/)")
    ap.add_argument("--earnings", action="store_true", help="Also overlay E/R markers from event_markers")
    args = ap.parse_args()
    build_chart(
        ticker=args.ticker,
        days=args.days,
        events_path=args.events,
        include_earnings=args.earnings,
    )


if __name__ == "__main__":
    main()
