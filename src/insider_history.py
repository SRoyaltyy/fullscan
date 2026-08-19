"""Multi-month insider history from SEC Form 4 (for backtests).

Finviz only shows a short recent window. For every-month nets + deltas you need
Form 4 history from EDGAR. This module pulls Form 4 via edgartools, keeps open-
market buys/sells (codes P/S by default), and writes a monthly panel.

CLI:
  python -m src.insider_history --tickers AAPL,AMLX,CORT --months 18
  python -m src.insider_history --liquid --months 12 --max-tickers 200
  python -m src.insider_history --liquid --months 12 --resume

Outputs under data/insider/history/:
  trades_YYYY-MM-DD.parquet   # every parsed Form 4 leg
  monthly_panel.csv           # ticker × month nets (append/merge)
  monthly_panel.parquet
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
OUT_DIR = ROOT / "data" / "insider" / "history"
ET = ZoneInfo(config.TZ)

MCAP_MIN = 80_000_000.0
ADV_MIN = 500_000.0

# Open-market-ish codes (default). Awards/exercises/tax withheld excluded.
DEFAULT_CODES = {"P", "S"}  # Purchase, Sale


def _num(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip().replace(",", "").replace("$", "")
    if not s or s in {"-", "—", "N/A"}:
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def _liquid_tickers() -> list[str]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        return []
    df = pd.read_csv(files[-1], low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    mcap = pd.to_numeric(df.get("Market Cap"), errors="coerce") * 1e6
    adv = pd.to_numeric(df.get("Average Volume"), errors="coerce") * 1e3
    ok = df[(mcap > MCAP_MIN) & (adv > ADV_MIN)]
    return sorted(ok["Ticker"].dropna().unique().tolist())


def _setup_edgar():
    try:
        from edgar import set_identity
    except ImportError as e:
        raise SystemExit(
            "[insider_history] edgartools required: pip install edgartools"
        ) from e
    # SEC requires a descriptive User-Agent / identity
    identity = (
        __import__("os").environ.get("SEC_IDENTITY")
        or __import__("os").environ.get("EDGAR_IDENTITY")
        or "fullscan-insider-bot contact@example.com"
    )
    set_identity(identity)


def _fetch_ticker_trades(
    ticker: str,
    since: pd.Timestamp,
    codes: set[str],
) -> pd.DataFrame:
    from edgar import Company

    rows = []
    try:
        company = Company(ticker)
    except Exception as e:
        print(f"[insider_history] {ticker}: company lookup fail {e}")
        return pd.DataFrame()

    try:
        filings = company.get_filings(form="4")
    except Exception as e:
        print(f"[insider_history] {ticker}: filings fail {e}")
        return pd.DataFrame()

    count = 0
    for f in filings:
        try:
            fdate = pd.Timestamp(getattr(f, "filing_date", None))
        except Exception:
            fdate = pd.NaT
        if pd.notna(fdate) and fdate < since:
            # filings are newest-first; can stop once older than window
            break
        try:
            obj = f.obj()
            df = obj.to_dataframe() if hasattr(obj, "to_dataframe") else None
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for _, r in df.iterrows():
            code = str(r.get("Code", "") or "").strip().upper()
            txn = str(r.get("Transaction Type", "") or "").strip()
            if codes and code not in codes:
                # also allow matching by type name if code missing
                tl = txn.lower()
                if code:
                    continue
                if not (("purch" in tl or tl == "buy") or ("sale" in tl or tl == "sell")):
                    continue
            val = _num(r.get("Value"))
            shares = _num(r.get("Shares"))
            price = _num(r.get("Price"))
            if not np.isfinite(val) and np.isfinite(shares) and np.isfinite(price):
                val = shares * price
            d = r.get("Date")
            try:
                trade_date = pd.Timestamp(d).date().isoformat() if d is not None else None
            except Exception:
                trade_date = None
            if not trade_date:
                continue
            if pd.Timestamp(trade_date) < since:
                continue

            side = "other"
            signed = np.nan
            tl = txn.lower()
            if code == "P" or "purch" in tl or tl == "buy":
                side = "buy"
                signed = val if np.isfinite(val) else np.nan
            elif code == "S" or "sale" in tl or tl == "sell":
                side = "sell"
                signed = -val if np.isfinite(val) else np.nan

            rows.append({
                "ticker": ticker.upper(),
                "trade_date": trade_date,
                "filing_date": str(getattr(f, "filing_date", "")),
                "accession": str(getattr(f, "accession_no", "")),
                "code": code,
                "transaction_type": txn,
                "side": side,
                "shares": shares,
                "price": price,
                "value": val,
                "value_signed": signed,
                "insider": r.get("Insider"),
                "position": r.get("Position"),
            })
        count += 1
        if count >= 400:  # safety cap per ticker
            break

    return pd.DataFrame(rows)


def monthly_panel(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    df = trades.dropna(subset=["trade_date", "ticker"]).copy()
    df["month"] = pd.to_datetime(df["trade_date"]).dt.to_period("M").astype(str)
    df["buy_val"] = np.where(df["side"] == "buy", df["value"].fillna(0.0), 0.0)
    df["sell_val"] = np.where(df["side"] == "sell", df["value"].fillna(0.0), 0.0)
    g = (
        df.groupby(["ticker", "month"], as_index=False)
        .agg(
            buy_value=("buy_val", "sum"),
            sell_value=("sell_val", "sum"),
            n_buys=("side", lambda s: int((s == "buy").sum())),
            n_sells=("side", lambda s: int((s == "sell").sum())),
            n_trades=("side", "count"),
        )
    )
    g["net_value"] = g["buy_value"] - g["sell_value"]
    return g.sort_values(["ticker", "month"]).reset_index(drop=True)


def add_deltas(panel: pd.DataFrame) -> pd.DataFrame:
    """For each ticker, month-over-month Δ net and prior month net."""
    if panel.empty:
        return panel
    parts = []
    for t, sub in panel.groupby("ticker"):
        s = sub.sort_values("month").copy()
        s["net_prev"] = s["net_value"].shift(1)
        s["net_delta"] = s["net_value"] - s["net_prev"]
        s["buy_prev"] = s["buy_value"].shift(1)
        s["sell_prev"] = s["sell_value"].shift(1)
        parts.append(s)
    return pd.concat(parts, ignore_index=True)


def _done_tickers(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
        return set(df["ticker"].astype(str).str.upper().unique())
    except Exception:
        return set()


def run(
    tickers: list[str],
    months: int = 18,
    codes: set[str] | None = None,
    resume: bool = False,
    sleep: float = 0.25,
) -> pd.DataFrame:
    _setup_edgar()
    codes = codes or set(DEFAULT_CODES)
    since = pd.Timestamp(datetime.now(ET).date()) - pd.DateOffset(months=months)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    asof = datetime.now(ET).date().isoformat()
    trades_path = OUT_DIR / f"trades_{asof}.parquet"
    panel_path = OUT_DIR / "monthly_panel.parquet"
    panel_csv = OUT_DIR / "monthly_panel.csv"

    existing_trades = []
    done = set()
    if resume and trades_path.exists():
        old = pd.read_parquet(trades_path)
        existing_trades.append(old)
        done = set(old["ticker"].astype(str).str.upper().unique())
        print(f"[insider_history] resume: {len(done)} tickers already in {trades_path.name}")
    elif resume and panel_path.exists():
        done = _done_tickers(panel_path)
        print(f"[insider_history] resume from panel: {len(done)} tickers")

    todo = [t for t in tickers if t.upper() not in done]
    print(f"[insider_history] months={months} since={since.date()} codes={sorted(codes)} todo={len(todo)}")

    frames = list(existing_trades)
    for i, t in enumerate(todo, 1):
        t = t.upper()
        print(f"[insider_history] {t} ({i}/{len(todo)}) …")
        df = _fetch_ticker_trades(t, since=since, codes=codes)
        if len(df):
            frames.append(df)
            print(f"  rows={len(df)} months={df['trade_date'].str[:7].nunique()}")
        else:
            print("  no open-market P/S in window")
        # checkpoint every 10 tickers
        if frames and i % 10 == 0:
            tmp = pd.concat(frames, ignore_index=True)
            tmp.to_parquet(trades_path, index=False)
            print(f"  checkpoint trades rows={len(tmp)}")
        time.sleep(sleep)

    if not frames:
        print("[insider_history] no trades collected")
        return pd.DataFrame()

    trades = pd.concat(frames, ignore_index=True)
    trades = trades.drop_duplicates(
        subset=["ticker", "trade_date", "accession", "code", "shares", "value", "insider"],
        keep="first",
    )
    trades.to_parquet(trades_path, index=False)
    trades.to_csv(OUT_DIR / f"trades_{asof}.csv", index=False)

    panel = add_deltas(monthly_panel(trades))

    # merge with any prior panel so history accumulates across runs
    if panel_path.exists():
        try:
            old_p = pd.read_parquet(panel_path)
            panel = (
                pd.concat([old_p, panel], ignore_index=True)
                .drop_duplicates(subset=["ticker", "month"], keep="last")
                .sort_values(["ticker", "month"])
            )
        except Exception as e:
            print(f"[insider_history] prior panel merge skip: {e}")

    panel.to_parquet(panel_path, index=False)
    panel.to_csv(panel_csv, index=False)

    # snapshot MoM for latest complete month per ticker
    latest_month = (pd.Timestamp(asof).to_period("M") - 1).strftime("%Y-%m")  # prior full month
    snap = panel[panel["month"] == latest_month].copy() if len(panel) else pd.DataFrame()
    if len(snap):
        snap.to_csv(OUT_DIR / f"{asof}_month_{latest_month}.csv", index=False)

    meta = {
        "asof": asof,
        "months_window": months,
        "since": str(since.date()),
        "codes": sorted(codes),
        "n_tickers_requested": len(tickers),
        "n_trade_rows": int(len(trades)),
        "n_panel_rows": int(len(panel)),
        "panel_months": sorted(panel["month"].unique().tolist()) if len(panel) else [],
        "trades_path": str(trades_path.relative_to(ROOT)),
        "panel_path": str(panel_path.relative_to(ROOT)),
        "generated": datetime.now(ET).isoformat(),
    }
    (OUT_DIR / f"meta_{asof}.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # short MD
    lines = [
        f"# Insider history (SEC Form 4) — {asof}",
        "",
        f"- Window: last **{months}** months (since {since.date()})",
        f"- Codes kept: **{sorted(codes)}** (open-market purchase/sale)",
        f"- Trade rows: **{len(trades):,}**",
        f"- Panel rows (ticker×month): **{len(panel):,}**",
        f"- Months present: {meta['panel_months'][:6]} … {meta['panel_months'][-3:] if meta['panel_months'] else []}",
        "",
        "## Sample (latest rows of panel)",
        "",
        "| ticker | month | buy | sell | net | net_prev | net_delta |",
        "|--------|-------|----:|-----:|----:|---------:|----------:|",
    ]
    for _, r in panel.tail(20).iterrows():
        lines.append(
            f"| {r['ticker']} | {r['month']} | {r['buy_value']:,.0f} | {r['sell_value']:,.0f} | "
            f"{r['net_value']:,.0f} | {r.get('net_prev', float('nan'))} | {r.get('net_delta', float('nan'))} |"
        )
    lines += [
        "",
        f"- `{panel_csv}` — full monthly panel (use this for backtests)",
        f"- `{trades_path.name}` — underlying Form 4 legs",
        "",
        "Backtest usage: for signal date D, use months strictly before D;",
        "e.g. net_delta on month M-1 as feature available at start of month M.",
    ]
    (OUT_DIR / f"history_{asof}.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"[insider_history] panel → {panel_path} rows={len(panel)}")
    print(f"[insider_history] months={panel['month'].nunique() if len(panel) else 0}")
    return panel


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default=None, help="Comma list")
    ap.add_argument("--liquid", action="store_true", help="Use liquid Finviz universe")
    ap.add_argument("--months", type=int, default=18)
    ap.add_argument("--max-tickers", type=int, default=300)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument(
        "--codes",
        default="P,S",
        help="Form 4 codes to keep (default P,S open market). Use ALL for every code.",
    )
    args = ap.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.liquid:
        tickers = _liquid_tickers()[: args.max_tickers]
    else:
        raise SystemExit("Pass --tickers AAPL,AMLX or --liquid")

    if args.codes.strip().upper() == "ALL":
        codes = set()  # empty = keep all in fetch filter path
    else:
        codes = {c.strip().upper() for c in args.codes.split(",") if c.strip()}

    run(tickers=tickers, months=args.months, codes=codes or DEFAULT_CODES, resume=args.resume, sleep=args.sleep)


if __name__ == "__main__":
    main()
