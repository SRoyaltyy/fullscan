"""Finviz quote-page snapshot colors (green / red / neutral).

Each of the ~84 snapshot fields on quote.ashx is marked in HTML as:
  color-text is-positive  → green
  color-text is-negative  → red
  (else)                  → neutral

CLI:
  python -m src.quote_colors --tickers AAPL,AMLX,CORT
  python -m src.quote_colors --liquid --max-tickers 200
  python -m src.quote_colors --from-ab --top 100   # top of latest ab_checklist
"""
from __future__ import annotations

import argparse
import json
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
AB_DIR = ROOT / "data" / "ab_checklist"
OUT_DIR = ROOT / "data" / "quote_colors"
ET = ZoneInfo(config.TZ)

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
QUOTE_URL = "https://finviz.com/quote.ashx?t={ticker}"

MCAP_MIN = 80_000_000.0
ADV_MIN = 500_000.0

# Core fields we always surface as columns (subset of the 84)
KEY_FIELDS = [
    "EPS Y/Y TTM",
    "EPS Q/Q",
    "Sales Y/Y TTM",
    "Sales Q/Q",
    "EPS/Sales Surpr.",
    "ROA",
    "ROE",
    "ROIC",
    "Gross Margin",
    "Oper. Margin",
    "Profit Margin",
    "Insider Trans",
    "Inst Trans",
    "SMA20",
    "SMA50",
    "SMA200",
    "Perf Week",
    "Perf Month",
    "Perf Quarter",
    "RSI (14)",
    "Target Price",
    "Change %",
    "Debt/Eq",
    "PEG",
    "P/E",
    "Forward P/E",
]


def _num(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip().replace(",", "")
    if not s or s in {"-", "—"}:
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    token = (
        __import__("os").environ.get("FINVIZ_AUTH")
        or __import__("os").environ.get("AUTH_TOKEN_FINVIZ")
        or ""
    )
    if token:
        s.cookies.set("auth", token, domain=".finviz.com")
    return s


def parse_snapshot_colors(html: str) -> dict:
    """Return {label: {value, color}} for snapshot-td2 pairs."""
    soup = BeautifulSoup(html, "html.parser")
    out = {}
    for lab_td in soup.select("td.snapshot-td2.cursor-pointer"):
        lab = lab_td.get_text(strip=True)
        val_td = lab_td.find_next_sibling("td")
        if not val_td or not lab:
            continue
        val = val_td.get_text(strip=True)
        color = "neutral"
        for el in [val_td] + list(val_td.find_all(True)):
            cls = " ".join(el.get("class") or [])
            if "is-positive" in cls:
                color = "green"
                break
            if "is-negative" in cls:
                color = "red"
                break
        out[lab] = {"value": val, "color": color}
    return out


def fetch_ticker(ticker: str, sess: requests.Session | None = None) -> dict:
    sess = sess or _session()
    r = sess.get(QUOTE_URL.format(ticker=ticker.upper()), timeout=45)
    r.raise_for_status()
    fields = parse_snapshot_colors(r.text)
    n_green = sum(1 for f in fields.values() if f["color"] == "green")
    n_red = sum(1 for f in fields.values() if f["color"] == "red")
    n_neu = sum(1 for f in fields.values() if f["color"] == "neutral")
    n = max(n_green + n_red + n_neu, 1)
    return {
        "ticker": ticker.upper(),
        "n_fields": len(fields),
        "n_green": n_green,
        "n_red": n_red,
        "n_neutral": n_neu,
        "green_minus_red": n_green - n_red,
        "green_pct": n_green / n,
        "red_pct": n_red / n,
        "fields": fields,
    }


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


def _from_ab_top(n: int) -> list[str]:
    files = sorted(AB_DIR.glob("????-??-??_ab_checklist.csv"))
    if not files:
        return []
    df = pd.read_csv(files[-1])
    if "score" in df.columns:
        df = df.sort_values("score", ascending=False)
    return df["Ticker"].astype(str).str.upper().head(n).tolist()


def run(
    tickers: list[str],
    asof: str | None = None,
    sleep: float = 0.45,
) -> pd.DataFrame:
    asof = asof or datetime.now(ET).date().isoformat()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sess = _session()

    rows = []
    detail = {}
    for i, t in enumerate(tickers, 1):
        t = t.upper().strip()
        try:
            data = fetch_ticker(t, sess)
            fields = data.pop("fields")
            detail[t] = fields
            rec = {"asof_date": asof, **data}
            # expand key fields
            for k in KEY_FIELDS:
                if k in fields:
                    rec[f"val_{k}"] = fields[k]["value"]
                    rec[f"color_{k}"] = fields[k]["color"]
                else:
                    rec[f"val_{k}"] = None
                    rec[f"color_{k}"] = None
            # score: +1 green, -1 red on key fields only (cleaner signal)
            key_score = 0
            for k in KEY_FIELDS:
                c = rec.get(f"color_{k}")
                if c == "green":
                    key_score += 1
                elif c == "red":
                    key_score -= 1
            rec["key_color_score"] = key_score
            # overall bias flag
            gm = data["green_minus_red"]
            if gm >= 5:
                rec["flag_colors"] = 1
                rec["status_colors"] = "GOOD"
            elif gm <= -5:
                rec["flag_colors"] = -1
                rec["status_colors"] = "BAD"
            else:
                rec["flag_colors"] = 0
                rec["status_colors"] = "NEUTRAL"
            rows.append(rec)
            print(
                f"[colors] {t} green={data['n_green']} red={data['n_red']} "
                f"Δ={gm:+d} key_score={key_score:+d} ({i}/{len(tickers)})"
            )
        except Exception as e:
            print(f"[colors] {t} FAIL {e}")
            rows.append({
                "asof_date": asof,
                "ticker": t,
                "n_fields": 0,
                "n_green": 0,
                "n_red": 0,
                "n_neutral": 0,
                "green_minus_red": 0,
                "flag_colors": 0,
                "status_colors": "NEUTRAL",
                "error": str(e),
            })
        time.sleep(sleep)

    df = pd.DataFrame(rows)
    if "green_minus_red" in df.columns:
        df = df.sort_values("green_minus_red", ascending=False)

    csv_path = OUT_DIR / f"{asof}_quote_colors.csv"
    df.to_csv(csv_path, index=False)
    (OUT_DIR / f"{asof}_quote_colors_detail.json").write_text(
        json.dumps(detail, indent=2), encoding="utf-8"
    )

    lines = [
        f"# Finviz snapshot colors — {asof}",
        "",
        f"- Tickers: **{len(df)}**",
        "- Source: `quote.ashx` snapshot-td2 (`is-positive` / `is-negative` / neutral)",
        "- **green_minus_red** = n_green − n_red across all ~84 fields",
        "- **key_color_score** = same idea restricted to KEY_FIELDS (margins, growth, SMA, perf…)",
        "",
        "| Ticker | green | red | Δ | key_score | status |",
        "|--------|------:|----:|--:|----------:|:------:|",
    ]
    for _, r in df.head(40).iterrows():
        lines.append(
            f"| {r.get('ticker')} | {int(r.get('n_green') or 0)} | {int(r.get('n_red') or 0)} | "
            f"{int(r.get('green_minus_red') or 0):+d} | {int(r.get('key_color_score') or 0):+d} | "
            f"**{r.get('status_colors', '')}** |"
        )
    lines += ["", f"CSV: `{csv_path.relative_to(ROOT)}`"]
    md_path = OUT_DIR / f"{asof}_quote_colors.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"[colors] wrote {csv_path}")
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default=None)
    ap.add_argument("--liquid", action="store_true")
    ap.add_argument("--from-ab", action="store_true")
    ap.add_argument("--top", type=int, default=100)
    ap.add_argument("--max-tickers", type=int, default=200)
    ap.add_argument("--date", default=None)
    ap.add_argument("--sleep", type=float, default=0.45)
    args = ap.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.from_ab:
        tickers = _from_ab_top(args.top)
    elif args.liquid:
        tickers = _liquid_tickers()[: args.max_tickers]
    else:
        raise SystemExit("Pass --tickers, --liquid, or --from-ab")

    if not tickers:
        raise SystemExit("[colors] empty ticker list")
    run(tickers=tickers, asof=args.date, sleep=args.sleep)


if __name__ == "__main__":
    main()
