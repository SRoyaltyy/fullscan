"""Finviz insider trading fetch (B2).

Sources (no paid third-party API):
  1. Market-wide: https://finviz.com/insidertrading.ashx  (latest / buys / sales / top week)
  2. Per-ticker:  https://finviz.com/quote.ashx?t=TICKER   (same table as Elite UI)

CLI:
  python -m src.insider_fetch                  # market-wide only
  python -m src.insider_fetch --quotes liquid  # + quote pages for liquid universe
  python -m src.insider_fetch --quotes AMLX,AAPL --sleep 0.4
  python -m src.insider_fetch --date 2026-08-18
"""
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from . import config

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
OUT_DIR = ROOT / "data" / "insider"
ET = ZoneInfo(config.TZ)

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

MARKET_URLS = {
    "latest": "https://finviz.com/insidertrading.ashx",
    "buys": "https://finviz.com/insidertrading.ashx?tc=1",
    "sales": "https://finviz.com/insidertrading.ashx?tc=2",
    "top_week": "https://finviz.com/insidertrading.ashx?or=-10&tv=100000&tc=7&o=-transactionValue",
    "top_week_buys": "https://finviz.com/insidertrading.ashx?or=-10&tv=100000&tc=1&o=-transactionValue",
}

QUOTE_URL = "https://finviz.com/quote.ashx?t={ticker}"

MCAP_MIN = 80_000_000.0
ADV_MIN = 500_000.0


def _num(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip().replace(",", "").replace("$", "").replace("%", "")
    if not s or s in {"-", "—", "N/A", "NA", "None", "nan", ""}:
        return np.nan
    m = re.match(r"^([+-]?\d*\.?\d+)\s*([KMBTkmbt])?$", s)
    if not m:
        try:
            return float(s)
        except ValueError:
            return np.nan
    v = float(m.group(1))
    suf = (m.group(2) or "").upper()
    return v * {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[suf]


def _parse_finviz_date(s: str) -> str | None:
    """'Aug 17 \'26' or 'Aug 17 26' → 2026-08-17."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace("'", " ")
    for fmt in ("%b %d %y", "%b %d %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    # Optional Elite cookie / auth if present
    token = (
        __import__("os").environ.get("FINVIZ_AUTH")
        or __import__("os").environ.get("AUTH_TOKEN_FINVIZ")
        or ""
    )
    if token:
        s.cookies.set("auth", token, domain=".finviz.com")
    return s


def _table_to_rows(soup: BeautifulSoup, source: str, ticker_hint: str | None = None) -> list[dict]:
    rows_out = []
    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if len(trs) < 2:
            continue
        heads = [c.get_text(strip=True) for c in trs[0].find_all(["th", "td"])]
        # market page has Ticker; quote page has Insider Trading
        has_mkt = "Ticker" in heads and "Transaction" in heads
        has_q = "Insider Trading" in heads and "Transaction" in heads
        if not (has_mkt or has_q):
            continue
        for tr in trs[1:]:
            cols = [c.get_text(strip=True) for c in tr.find_all("td")]
            if len(cols) < 7:
                continue
            if has_mkt:
                # Ticker, Owner, Relationship, Date, Transaction, Cost, #Shares, Value ($), #Shares Total, SEC Form 4
                ticker = cols[0].upper()
                owner, rel, date_s, txn = cols[1], cols[2], cols[3], cols[4]
                cost, shares, value, shares_tot = cols[5], cols[6], cols[7], cols[8] if len(cols) > 8 else ""
                form4 = cols[9] if len(cols) > 9 else ""
            else:
                ticker = (ticker_hint or "").upper()
                owner, rel, date_s, txn = cols[0], cols[1], cols[2], cols[3]
                cost, shares, value = cols[4], cols[5], cols[6]
                shares_tot = cols[7] if len(cols) > 7 else ""
                form4 = cols[8] if len(cols) > 8 else ""
            # SEC link from anchor if present
            link = ""
            for a in tr.find_all("a", href=True):
                if "sec.gov" in a["href"] or "Archives" in a["href"]:
                    link = a["href"]
                    break
            trade_date = _parse_finviz_date(date_s)
            val = _num(value)
            # sign: Buy / Purchase positive; Sale / Sell / Proposed Sale negative for net
            txn_l = txn.lower()
            if any(k in txn_l for k in ("buy", "purchase", "exercise")):
                signed = val if np.isfinite(val) else np.nan
                side = "buy" if "buy" in txn_l or "purchase" in txn_l else "other_in"
            elif any(k in txn_l for k in ("sale", "sell")):
                signed = -val if np.isfinite(val) else np.nan
                side = "sell"
            else:
                signed = np.nan
                side = "other"
            rows_out.append({
                "ticker": ticker,
                "owner": owner,
                "relationship": rel,
                "date_raw": date_s,
                "trade_date": trade_date,
                "transaction": txn,
                "side": side,
                "cost": _num(cost),
                "shares": _num(shares),
                "value": val,
                "value_signed": signed,
                "shares_total": _num(shares_tot),
                "form4_stamp": form4,
                "form4_url": link,
                "source": source,
            })
        break
    return rows_out


def fetch_market(sess: requests.Session | None = None) -> pd.DataFrame:
    sess = sess or _session()
    all_rows = []
    for name, url in MARKET_URLS.items():
        try:
            r = sess.get(url, timeout=45)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            part = _table_to_rows(soup, source=f"market:{name}")
            print(f"[insider] market:{name} rows={len(part)}")
            all_rows.extend(part)
            time.sleep(0.6)
        except Exception as e:
            print(f"[insider] market:{name} FAIL {e}")
    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    # dedupe on ticker+owner+date+txn+shares+value
    df = df.drop_duplicates(
        subset=["ticker", "owner", "trade_date", "transaction", "shares", "value"],
        keep="first",
    )
    return df.reset_index(drop=True)


def fetch_quote_insiders(ticker: str, sess: requests.Session | None = None) -> pd.DataFrame:
    sess = sess or _session()
    t = ticker.upper().strip()
    r = sess.get(QUOTE_URL.format(ticker=t), timeout=45)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    rows = _table_to_rows(soup, source=f"quote:{t}", ticker_hint=t)
    return pd.DataFrame(rows)


def _liquid_tickers() -> list[str]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        return []
    df = pd.read_csv(files[-1], low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    mcap = df["Market Cap"].map(_num) * 1e6 if "Market Cap" in df.columns else np.nan
    adv = df["Average Volume"].map(_num) * 1e3 if "Average Volume" in df.columns else np.nan
    df["_mcap"] = mcap
    df["_adv"] = adv
    ok = df[(df["_mcap"] > MCAP_MIN) & (df["_adv"] > ADV_MIN)]
    return sorted(ok["Ticker"].unique().tolist())


def monthly_net(trades: pd.DataFrame) -> pd.DataFrame:
    """Per ticker × calendar month: buy $, sell $, net $, counts."""
    if trades.empty:
        return pd.DataFrame()
    df = trades.dropna(subset=["trade_date", "ticker"]).copy()
    df["month"] = pd.to_datetime(df["trade_date"]).dt.to_period("M").astype(str)
    df["buy_val"] = np.where(df["side"] == "buy", df["value"].fillna(0), 0.0)
    df["sell_val"] = np.where(df["side"] == "sell", df["value"].fillna(0), 0.0)
    g = df.groupby(["ticker", "month"], as_index=False).agg(
        buy_value=("buy_val", "sum"),
        sell_value=("sell_val", "sum"),
        n_buys=("side", lambda s: int((s == "buy").sum())),
        n_sells=("side", lambda s: int((s == "sell").sum())),
        n_trades=("side", "count"),
    )
    g["net_value"] = g["buy_value"] - g["sell_value"]
    return g.sort_values(["ticker", "month"])


def month_over_month(monthly: pd.DataFrame, asof: str) -> pd.DataFrame:
    """For each ticker: current month net vs previous month net."""
    if monthly.empty:
        return pd.DataFrame()
    asof_m = pd.Timestamp(asof).to_period("M")
    cur = str(asof_m)
    prev = str(asof_m - 1)
    rows = []
    for t, sub in monthly.groupby("ticker"):
        m = sub.set_index("month")
        cur_net = float(m.loc[cur, "net_value"]) if cur in m.index else 0.0
        prev_net = float(m.loc[prev, "net_value"]) if prev in m.index else 0.0
        cur_buy = float(m.loc[cur, "buy_value"]) if cur in m.index else 0.0
        cur_sell = float(m.loc[cur, "sell_value"]) if cur in m.index else 0.0
        n_b = int(m.loc[cur, "n_buys"]) if cur in m.index else 0
        n_s = int(m.loc[cur, "n_sells"]) if cur in m.index else 0
        delta = cur_net - prev_net
        if cur_net > 0 and delta > 0:
            flag = 1
        elif cur_net < 0 and delta < 0:
            flag = -1
        elif cur_net > 0:
            flag = 1
        elif cur_net < 0:
            flag = -1
        else:
            flag = 0
        rows.append({
            "ticker": t,
            "asof_date": asof,
            "month_cur": cur,
            "month_prev": prev,
            "net_cur": cur_net,
            "net_prev": prev_net,
            "net_delta": delta,
            "buy_cur": cur_buy,
            "sell_cur": cur_sell,
            "n_buys_cur": n_b,
            "n_sells_cur": n_s,
            "flag_insider_net": flag,  # +1 accumulation, -1 distribution
            "status": "GOOD" if flag > 0 else ("BAD" if flag < 0 else "NEUTRAL"),
        })
    return pd.DataFrame(rows).sort_values("net_cur", ascending=False)


def run(
    asof: str | None = None,
    quotes: str | None = None,
    sleep: float = 0.45,
    max_quotes: int = 400,
) -> dict:
    asof = asof or datetime.now(ET).date().isoformat()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sess = _session()

    market = fetch_market(sess)
    print(f"[insider] market deduped rows={len(market)}")

    quote_parts = []
    tickers: list[str] = []
    if quotes:
        if quotes.strip().lower() == "liquid":
            tickers = _liquid_tickers()
            print(f"[insider] liquid universe for quote scrape: {len(tickers)}")
            tickers = tickers[:max_quotes]
        else:
            tickers = [t.strip().upper() for t in quotes.split(",") if t.strip()]

    for i, t in enumerate(tickers, 1):
        try:
            q = fetch_quote_insiders(t, sess)
            if len(q):
                quote_parts.append(q)
                print(f"[insider] quote {t} rows={len(q)} ({i}/{len(tickers)})")
            else:
                print(f"[insider] quote {t} empty ({i}/{len(tickers)})")
        except Exception as e:
            print(f"[insider] quote {t} FAIL {e}")
        time.sleep(sleep)

    frames = [f for f in [market, *quote_parts] if f is not None and len(f)]
    trades = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if len(trades):
        trades = trades.drop_duplicates(
            subset=["ticker", "owner", "trade_date", "transaction", "shares", "value"],
            keep="first",
        )

    trades_path = OUT_DIR / f"{asof}_insider_trades.csv"
    trades.to_csv(trades_path, index=False)

    monthly = monthly_net(trades)
    monthly_path = OUT_DIR / f"{asof}_insider_monthly.csv"
    monthly.to_csv(monthly_path, index=False)

    mom = month_over_month(monthly, asof)
    mom_path = OUT_DIR / f"{asof}_insider_mom.csv"
    mom.to_csv(mom_path, index=False)

    # human summary
    lines = [
        f"# Insider fetch — {asof}",
        "",
        f"- Market-wide Finviz rows (deduped with quotes): **{len(trades)}**",
        f"- Quote pages scraped: **{len(tickers)}**",
        f"- Tickers with monthly aggregates: **{monthly['ticker'].nunique() if len(monthly) else 0}**",
        "",
        "## Month-over-month net (top accumulation)",
        "",
        "| Ticker | month | net_cur | net_prev | Δ | buys | sells | status |",
        "|--------|-------|--------:|---------:|--:|-----:|------:|:------:|",
    ]
    for _, r in mom.head(25).iterrows():
        lines.append(
            f"| {r['ticker']} | {r['month_cur']} | {r['net_cur']:,.0f} | {r['net_prev']:,.0f} | "
            f"{r['net_delta']:,.0f} | {int(r['n_buys_cur'])} | {int(r['n_sells_cur'])} | **{r['status']}** |"
        )
    lines += ["", "## Top distribution (most negative net_cur)", ""]
    for _, r in mom.nsmallest(15, "net_cur").iterrows():
        lines.append(
            f"| {r['ticker']} | {r['net_cur']:,.0f} | buys={int(r['n_buys_cur'])} sells={int(r['n_sells_cur'])} | **{r['status']}** |"
        )
    lines += [
        "",
        f"- trades: `data/insider/{asof}_insider_trades.csv`",
        f"- monthly: `data/insider/{asof}_insider_monthly.csv`",
        f"- MoM flags: `data/insider/{asof}_insider_mom.csv`",
        "",
        "Sign convention: Buy/Purchase → +value; Sale/Sell/Proposed Sale → −value for net.",
        "flag_insider_net: +1 accumulation, −1 distribution, 0 flat/unknown.",
    ]
    md_path = OUT_DIR / f"{asof}_insider.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    meta = {
        "asof": asof,
        "n_trades": int(len(trades)),
        "n_quote_tickers": len(tickers),
        "n_mom": int(len(mom)),
        "generated": datetime.now(ET).isoformat(),
    }
    (OUT_DIR / f"{asof}_insider.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"[insider] wrote {trades_path}")
    print(f"[insider] wrote {monthly_path}")
    print(f"[insider] wrote {mom_path}")
    print(f"[insider] wrote {md_path}")
    if len(mom):
        print("Top net_cur:", mom.head(5)[["ticker", "net_cur", "status"]].to_string(index=False))
    return meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="asof YYYY-MM-DD")
    ap.add_argument(
        "--quotes",
        default=None,
        help="'liquid' or comma list of tickers for quote.ashx scrape",
    )
    ap.add_argument("--sleep", type=float, default=0.45)
    ap.add_argument("--max-quotes", type=int, default=400)
    args = ap.parse_args()
    run(asof=args.date, quotes=args.quotes, sleep=args.sleep, max_quotes=args.max_quotes)


if __name__ == "__main__":
    main()
