"""Daily per-ticker checklist — confirm direction with hard evidence.

Checks (v1):
  1. candle_bias     — recent green body sum > red body sum (yfinance OHLC, optional)
  2. rsi_oversold_ok — RSI < 30 AND business profitable (margin or EPS > 0)
  3. consecutive_dn  — 3+ consecutive down sessions (multi-day Finviz Change)
  4. peer_outperform — rs_week > 0 vs correlation peers (peer_rs / Compare-style)
  5. peer_breadth    — majority of peers have positive week performance
  6. revenue_trend   — Sales YoY / QoQ from Finviz snapshot
  7. analyst         — recom / target upside from Finviz (stale targets ignored)
  8. rvol            — relative volume elevated

Full universe: Finviz + peer_rs. Optional --ohlc for yfinance candle bias on a subset.

CLI:
  python -m src.ticker_checklist [--date YYYY-MM-DD] [--ohlc] [--top 50]
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

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
PEER_DIR = ROOT / "data" / "peers"
BOOK_DIR = ROOT / "data" / "stock_book"
OUT_DIR = ROOT / "data" / "checklist"
DAILY = ROOT / "01_daily"
ET = ZoneInfo(config.TZ)

RSI_COL = "Relative Strength Index (14)"
RVOL_COL = "Relative Volume"
PERF_W = "Performance (Week)"
CHG = "Change"
PRICE = "Price"
PM = "Profit Margin"
EPS = "EPS (ttm)"
SALES_YOY = "Sales Year Over Year TTM"
SALES_QOQ = "Sales Growth Quarter Over Quarter"
RECOM = "Analyst Recom"
TARGET = "Target Price"
EARN = "Earnings Date"


def _pct(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    s = str(x).replace("%", "").replace(",", "").strip()
    if s in ("", "-", "nan", "None", "—"):
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def _num(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    try:
        return float(str(x).replace(",", "").strip())
    except ValueError:
        return np.nan


def _dated_exports() -> list[tuple[str, Path]]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    out = []
    for p in files:
        d = p.stem.replace("finviz_", "")
        if len(d) == 10 and d[4] == "-":
            out.append((d, p))
    return out


def _load_export(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df[tcol] = df[tcol].astype(str).str.strip().str.upper()
    return df.drop_duplicates(subset=[tcol], keep="first").set_index(tcol)


def _load_peer_rs(date: str) -> pd.DataFrame:
    exact = PEER_DIR / f"{date}_peer_rs.csv"
    if exact.exists():
        df = pd.read_csv(exact)
    else:
        files = sorted(PEER_DIR.glob("????-??-??_peer_rs.csv"))
        files = [f for f in files if f.name[:10] <= date]
        if not files:
            return pd.DataFrame()
        df = pd.read_csv(files[-1])
    if df.empty:
        return df
    df["Ticker"] = df["Ticker"].astype(str).str.upper()
    return df.drop_duplicates("Ticker").set_index("Ticker")


def _consecutive_down(dates: list[str], frames: dict[str, pd.DataFrame], ticker: str) -> int:
    n = 0
    for d in reversed(dates):
        df = frames.get(d)
        if df is None or ticker not in df.index:
            break
        ch = _pct(df.at[ticker, CHG]) if CHG in df.columns else np.nan
        if ch == ch and ch < 0:
            n += 1
        else:
            break
    return n


def _candle_bias_yf(tickers: list[str], lookback: int = 10) -> dict[str, dict]:
    if not tickers:
        return {}
    try:
        import yfinance as yf
    except ImportError:
        print("[checklist] yfinance not installed — skip candle_bias")
        return {}
    out: dict[str, dict] = {}
    try:
        data = yf.download(
            tickers=tickers, period="1mo", group_by="ticker",
            auto_adjust=True, threads=True, progress=False,
        )
    except Exception as e:
        print(f"[checklist] yfinance download failed: {e}")
        return {}

    def one(sym: str, ohlc: pd.DataFrame) -> dict | None:
        if ohlc is None or ohlc.empty or len(ohlc) < 3:
            return None
        df = ohlc.tail(lookback).copy()
        if not {"Open", "Close"}.issubset(df.columns):
            return None
        body = df["Close"] - df["Open"]
        green = float(body[body > 0].sum())
        red = float((-body[body < 0]).sum())
        bias = green - red
        return {
            "green_body": round(green, 4), "red_body": round(red, 4),
            "bias": round(bias, 4), "pass": bias > 0, "n_bars": int(len(df)),
        }

    if len(tickers) == 1:
        r = one(tickers[0], data if isinstance(data, pd.DataFrame) else None)
        if r:
            out[tickers[0]] = r
        return out

    for sym in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if sym not in data.columns.get_level_values(0):
                    continue
                ohlc = data[sym].dropna(how="all")
            else:
                ohlc = data
            r = one(sym, ohlc)
            if r:
                out[sym] = r
        except Exception:
            continue
    return out


def _check_row(ticker, cur, dates, frames, peer, candle):
    rsi = _num(cur.get(RSI_COL))
    rvol = _num(cur.get(RVOL_COL))
    pm = _pct(cur.get(PM))
    eps = _num(cur.get(EPS))
    sales_yoy = _pct(cur.get(SALES_YOY)) if SALES_YOY in cur.index else np.nan
    sales_qoq = _pct(cur.get(SALES_QOQ)) if SALES_QOQ in cur.index else np.nan
    recom = _num(cur.get(RECOM))
    price = _num(cur.get(PRICE))
    target = _num(cur.get(TARGET))
    chg = _pct(cur.get(CHG))
    earn = cur.get(EARN)
    profitable = (pm == pm and pm > 0) or (eps == eps and eps > 0)
    checks = {}

    if candle:
        checks["candle_bias"] = {
            "pass": bool(candle.get("pass")),
            "detail": f"green={candle.get('green_body')} red={candle.get('red_body')} bias={candle.get('bias')}",
            "bull": 1 if candle.get("pass") else -1,
        }
    else:
        checks["candle_bias"] = {"pass": None, "detail": "no OHLC", "bull": 0}

    if rsi == rsi and rsi < 30 and profitable:
        checks["rsi_oversold_quality"] = {
            "pass": True,
            "detail": f"RSI={rsi:.1f}<30 and profitable (pm={pm}, eps={eps})",
            "bull": 2,
        }
    elif rsi == rsi and rsi < 30 and not profitable:
        checks["rsi_oversold_quality"] = {
            "pass": False,
            "detail": f"RSI={rsi:.1f}<30 but NOT profitable (pm={pm}, eps={eps}) — trap risk",
            "bull": -1,
        }
    elif rsi == rsi and rsi > 70:
        checks["rsi_oversold_quality"] = {
            "pass": False, "detail": f"RSI={rsi:.1f}>70 overbought", "bull": -1,
        }
    else:
        checks["rsi_oversold_quality"] = {
            "pass": None,
            "detail": f"RSI={rsi if rsi == rsi else 'n/a'} (no oversold setup)",
            "bull": 0,
        }

    cdn = _consecutive_down(dates, frames, ticker)
    if cdn >= 3:
        checks["consecutive_down"] = {
            "pass": True,
            "detail": f"{cdn} consecutive down sessions — washout / bounce setup",
            "bull": 1 if profitable else 0,
        }
    else:
        checks["consecutive_down"] = {
            "pass": False, "detail": f"{cdn} consecutive down (need 3+)", "bull": 0,
        }

    rs_w = peer_med = beat = np.nan
    peers_used = ""
    if peer is not None and not peer.empty and ticker in peer.index:
        pr = peer.loc[ticker]
        rs_w = _num(pr.get("rs_week"))
        peer_med = _num(pr.get("peer_med_week"))
        peers_used = str(pr.get("peers_used") or "")
        beat = _num(pr.get("beat_week_pct"))
    if rs_w == rs_w:
        checks["peer_outperform"] = {
            "pass": rs_w > 0,
            "detail": f"rs_week={rs_w:+.1f} (peer med {peer_med:+.1f}) peers={peers_used[:40]}",
            "bull": 1 if rs_w > 0 else (-1 if rs_w < -3 else 0),
        }
    else:
        checks["peer_outperform"] = {"pass": None, "detail": "no peer_rs", "bull": 0}

    if beat == beat:
        checks["peer_breadth"] = {
            "pass": beat >= 0.5,
            "detail": f"beat_week_pct={beat:.0%} of peers",
            "bull": 1 if beat >= 0.6 else (-1 if beat <= 0.3 else 0),
        }
    else:
        checks["peer_breadth"] = {"pass": None, "detail": "no peer breadth", "bull": 0}

    bits, bull_rev = [], 0
    if sales_yoy == sales_yoy:
        bits.append(f"SalesYoY={sales_yoy:+.1f}%")
        bull_rev += 1 if sales_yoy > 0 else -1
    if sales_qoq == sales_qoq:
        bits.append(f"SalesQoQ={sales_qoq:+.1f}%")
        bull_rev += 1 if sales_qoq > 0 else -1
    if earn and str(earn) not in ("nan", "None", ""):
        bits.append(f"EarningsDate={earn}")
    checks["revenue_trend"] = {
        "pass": bull_rev > 0 if bits else None,
        "detail": "; ".join(bits) if bits else "no sales fields",
        "bull": max(-1, min(1, bull_rev)),
    }

    upside = np.nan
    if price and price == price and price > 0 and target == target:
        upside = (target / price - 1.0) * 100
    if recom == recom or upside == upside:
        detail = f"recom={recom if recom == recom else 'n/a'}"
        if upside == upside:
            detail += f" upside={upside:+.1f}%"
        bull_a = 0
        if recom == recom:
            if recom <= 2.0:
                bull_a += 1
            elif recom >= 4.0:
                bull_a -= 1
        if upside == upside:
            if upside >= 500:
                detail += " [target likely stale]"
            elif upside >= 20:
                bull_a += 1
            elif upside <= -10:
                bull_a -= 1
        checks["analyst"] = {
            "pass": bull_a > 0, "detail": detail, "bull": max(-1, min(1, bull_a)),
        }
    else:
        checks["analyst"] = {"pass": None, "detail": "no analyst data", "bull": 0}

    if rvol == rvol:
        checks["rvol"] = {
            "pass": rvol >= 1.5,
            "detail": f"RVol={rvol:.2f}",
            "bull": 1 if rvol >= 1.5 else (0 if rvol >= 0.7 else -1),
        }
    else:
        checks["rvol"] = {"pass": None, "detail": "no rvol", "bull": 0}

    score = int(sum(c.get("bull", 0) for c in checks.values()))
    n_pass = sum(1 for c in checks.values() if c.get("pass") is True)
    n_fail = sum(1 for c in checks.values() if c.get("pass") is False)
    pos = [f"{k}: {c['detail']}" for k, c in checks.items() if c.get("bull", 0) > 0]
    neg = [f"{k}: {c['detail']}" for k, c in checks.items() if c.get("bull", 0) < 0]

    return {
        "Ticker": ticker,
        "sector": cur.get("Sector", ""),
        "industry": cur.get("Industry", ""),
        "price": price,
        "change": chg,
        "rsi": rsi,
        "rvol": rvol,
        "profit_margin": pm,
        "checklist_score": score,
        "n_pass": n_pass,
        "n_fail": n_fail,
        "profitable": bool(profitable),
        "consecutive_down": cdn,
        "rs_week": rs_w,
        "checks": checks,
        "bull_reasons": pos,
        "bear_reasons": neg,
    }


def run(date=None, ohlc=False, top=0, tickers=None):
    exports = _dated_exports()
    if not exports:
        raise SystemExit("[checklist] no data/exports/finviz_YYYY-MM-DD.csv")
    if date is None:
        date = exports[-1][0]
    exports = [(d, p) for d, p in exports if d <= date]
    if not exports:
        raise SystemExit(f"[checklist] no exports on/before {date}")
    date, _cur_path = exports[-1]
    dates = [d for d, _ in exports]
    frames = {d: _load_export(p) for d, p in exports}
    cur_df = frames[date]
    peer = _load_peer_rs(date)

    if tickers:
        univ = [t.upper() for t in tickers if t.upper() in cur_df.index]
    else:
        univ = list(cur_df.index)

    candle_map = {}
    if ohlc:
        ohlc_list = univ[: top or 120]
        bp = BOOK_DIR / f"{date}_stock_book.csv"
        if bp.exists():
            bdf = pd.read_csv(bp)
            if "Ticker" in bdf.columns:
                ohlc_list = list(dict.fromkeys(
                    list(bdf["Ticker"].astype(str).str.upper().head(80)) + ohlc_list
                ))[:120]
        print(f"[checklist] OHLC candle bias for {len(ohlc_list)} tickers…")
        candle_map = _candle_bias_yf(ohlc_list)

    rows = []
    for t in univ:
        try:
            rows.append(_check_row(t, cur_df.loc[t], dates, frames, peer, candle_map.get(t)))
        except Exception:
            continue
    out = pd.DataFrame(rows)
    if out.empty:
        raise SystemExit("[checklist] no rows")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DAILY.mkdir(parents=True, exist_ok=True)
    flat = out.drop(columns=["checks", "bull_reasons", "bear_reasons"], errors="ignore")
    csv_path = OUT_DIR / f"{date}_checklist.csv"
    flat.to_csv(csv_path, index=False)

    ranked = out.sort_values("checklist_score", ascending=False)
    payload = {
        "date": date,
        "generated": datetime.now(ET).isoformat(),
        "n": len(out),
        "exports_used": dates[-5:],
        "top": ranked.head(30)[
            ["Ticker", "checklist_score", "n_pass", "sector", "rsi", "rs_week"]
        ].to_dict("records"),
    }
    (OUT_DIR / f"{date}_checklist.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )

    L = [
        f"# Ticker checklist — {date}",
        "",
        "Hard evidence: price path, RSI+quality, peer Compare RS, sales, analyst, RVol.",
        "",
        f"- Universe: **{len(out):,}** | exports: `{', '.join(dates[-5:])}`",
        f"- Peer RS: `{'yes' if not peer.empty else 'no'}` | OHLC names: **{len(candle_map)}**",
        "",
        "## Rubric",
        "",
        "| Check | Bull when |",
        "|---|---|",
        "| candle_bias | green body sum > red (yfinance, optional) |",
        "| rsi_oversold_quality | RSI<30 **and** profitable |",
        "| consecutive_down | ≥3 down sessions |",
        "| peer_outperform | rs_week > 0 |",
        "| peer_breadth | ≥50% peers up on week |",
        "| revenue_trend | Sales YoY/QoQ positive |",
        "| analyst | recom≤2 or upside 20–500% |",
        "| rvol | RVol ≥ 1.5 |",
        "",
        "## Top 25",
        "",
        "| Ticker | Score | Pass | RSI | rs_week | Sector | Bull notes |",
        "|---|---|---|---|---|---|---|",
    ]
    for _, r in ranked.head(25).iterrows():
        notes = "; ".join((r.get("bull_reasons") or [])[:2])
        L.append(
            f"| {r['Ticker']} | {r['checklist_score']:+d} | {r['n_pass']} | "
            f"{r['rsi'] if r['rsi'] == r['rsi'] else '—'} | "
            f"{r['rs_week'] if r['rs_week'] == r['rs_week'] else '—'} | "
            f"{r.get('sector', '')} | {notes[:80]} |"
        )
    L += ["", "## Bottom 15", "",
          "| Ticker | Score | RSI | rs_week | Bear notes |", "|---|---|---|---|---|"]
    for _, r in ranked.tail(15).iloc[::-1].iterrows():
        notes = "; ".join((r.get("bear_reasons") or [])[:2])
        L.append(
            f"| {r['Ticker']} | {r['checklist_score']:+d} | "
            f"{r['rsi'] if r['rsi'] == r['rsi'] else '—'} | "
            f"{r['rs_week'] if r['rs_week'] == r['rs_week'] else '—'} | {notes[:80]} |"
        )

    if "XPON" in set(out["Ticker"]):
        xr = out[out["Ticker"] == "XPON"].iloc[0]
        L += ["", "## Sample deep — XPON", ""]
        for k, c in (xr.get("checks") or {}).items():
            flag = "PASS" if c.get("pass") is True else ("FAIL" if c.get("pass") is False else "n/a")
            L.append(f"- **{k}** [{flag}] (bull={c.get('bull')}): {c.get('detail')}")

    md = DAILY / f"{date}_checklist.md"
    md.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"[checklist] {date}: {len(out):,} -> {csv_path.name}, {md.name}")
    tops = ", ".join(
        f"{r.Ticker}({int(r.checklist_score):+d})" for _, r in ranked.head(5).iterrows()
    )
    print(f"[checklist] top: {tops}")
    return csv_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--ohlc", action="store_true")
    ap.add_argument("--top", type=int, default=0)
    ap.add_argument("--tickers", default=None)
    args = ap.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
    run(date=args.date, ohlc=args.ohlc, top=args.top, tickers=tickers)


if __name__ == "__main__":
    main()
