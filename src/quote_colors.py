"""Finviz quote-page snapshot colors (green / red / neutral) + last-2 analyst actions.

Color rules:
  * `is-positive` → green, `is-negative` → red, else neutral
  * **Performance timeframe fields are EXCLUDED** from green/red counts
    (Perf Week/Month/Quarter/Half/YTD/Year/3Y/5Y/10Y, Change %, etc.)

CLI:
  python -m src.quote_colors --tickers AAPL,AMLX
  python -m src.quote_colors --from-ab --top 80
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

from . import config, finviz_session

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
QUOTE_URL = "https://elite.finviz.com/quote.ashx?t={ticker}"

MCAP_MIN = 80_000_000.0
ADV_MIN = 500_000.0

# Fields that must NOT affect color score (performance / tape path)
PERF_EXCLUDE = {
    "Perf Week", "Perf Month", "Perf Quarter", "Perf Half Y", "Perf YTD",
    "Perf Year", "Perf 3Y", "Perf 5Y", "Perf 10Y",
    "Change %", "Change", "Change from Open", "Gap",
    "Prev Close", "Price", "Volume", "Avg Volume", "Rel Volume",
    "Volatility", "ATR (14)", "Trades",
}

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
    "RSI (14)",
    "Target Price",
    "Debt/Eq",
    "PEG",
    "P/E",
    "Forward P/E",
]


def _session() -> requests.Session:
    return finviz_session.session()


def parse_snapshot_colors(html: str) -> dict:
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


def parse_analyst_last2(html: str) -> list[dict]:
    """Extract last 2 analyst actions (Upgrade/Downgrade/Initiated/…)."""
    # Dates like Aug-17-26 glued to Action
    pat = re.compile(
        r"(?P<date>[A-Z][a-z]{2}-\d{1,2}-\d{2})"
        r"(?P<action>Upgrade|Downgrade|Initiated|Reiterated|Resumed)"
        r"(?P<rest>.{0,120}?)"
        r"(?=(?:[A-Z][a-z]{2}-\d{1,2}-\d{2})(?:Upgrade|Downgrade|Initiated|Reiterated|Resumed)|Show Previous|$)",
        re.DOTALL,
    )
    # Prefer text from body without scripts
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("", strip=True)
    hits = []
    for m in pat.finditer(text):
        date_s = m.group("date")
        action = m.group("action")
        rest = re.sub(r"\s+", " ", m.group("rest"))[:100]
        polarity = 0
        al = action.lower()
        if al == "upgrade":
            polarity = 1
        elif al == "downgrade":
            polarity = -1
        elif al == "initiated":
            # Buy-ish words in rest → +1, Sell/Underperform → -1
            rl = rest.lower()
            if any(k in rl for k in ("buy", "outperform", "overweight", "positive")):
                polarity = 1
            elif any(k in rl for k in ("sell", "underperform", "underweight", "reduce")):
                polarity = -1
        hits.append({
            "date_raw": date_s,
            "action": action,
            "detail": rest,
            "polarity": polarity,
            "status": "GOOD" if polarity > 0 else ("BAD" if polarity < 0 else "NEUTRAL"),
        })
        if len(hits) >= 2:
            break
    return hits


def fetch_ticker(ticker: str, sess: requests.Session | None = None) -> dict:
    sess = sess or _session()
    r = finviz_session.get(sess, [f"/quote.ashx?t={ticker.upper()}"], timeout=45)
    if r is None:
        raise RuntimeError(f"Elite quote page unavailable for {ticker}")
    fields = parse_snapshot_colors(r.text)
    analysts = parse_analyst_last2(r.text)

    # counts EXCLUDING performance timeframe labels
    n_green = n_red = n_neu = 0
    for lab, f in fields.items():
        if lab in PERF_EXCLUDE or lab.startswith("Perf "):
            continue
        if f["color"] == "green":
            n_green += 1
        elif f["color"] == "red":
            n_red += 1
        else:
            n_neu += 1

    n = max(n_green + n_red + n_neu, 1)
    key_score = 0
    for k in KEY_FIELDS:
        if k in PERF_EXCLUDE:
            continue
        c = (fields.get(k) or {}).get("color")
        if c == "green":
            key_score += 1
        elif c == "red":
            key_score -= 1

    # analyst last-2 aggregate
    a_pol = [x["polarity"] for x in analysts]
    while len(a_pol) < 2:
        a_pol.append(0)
    if a_pol[0] > 0 and a_pol[1] > 0:
        a_flag = 1
    elif a_pol[0] < 0 and a_pol[1] < 0:
        a_flag = -1
    elif a_pol[0] > 0:
        a_flag = 1
    elif a_pol[0] < 0:
        a_flag = -1
    else:
        a_flag = 0

    return {
        "ticker": ticker.upper(),
        "n_fields_scored": n_green + n_red + n_neu,
        "n_green": n_green,
        "n_red": n_red,
        "n_neutral": n_neu,
        "green_minus_red": n_green - n_red,
        "green_pct": n_green / n,
        "red_pct": n_red / n,
        "key_color_score": key_score,
        "analyst_last2": analysts,
        "analyst_1": analysts[0] if analysts else None,
        "analyst_2": analysts[1] if len(analysts) > 1 else None,
        "flag_analyst_last2": a_flag,
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
            analysts = data.pop("analyst_last2")
            a1 = data.pop("analyst_1")
            a2 = data.pop("analyst_2")
            detail[t] = {"fields": fields, "analyst_last2": analysts}
            rec = {"asof_date": asof, **data}
            for k in KEY_FIELDS:
                if k in fields:
                    rec[f"val_{k}"] = fields[k]["value"]
                    rec[f"color_{k}"] = fields[k]["color"]
                else:
                    rec[f"val_{k}"] = None
                    rec[f"color_{k}"] = None

            # human-readable last-2 analyst
            def fmt(a):
                if not a:
                    return "none"
                return f"{a['date_raw']} {a['action']} [{a['status']}] {a['detail'][:60]}"

            rec["val_B19_analyst_last2"] = f"#1 {fmt(a1)} || #2 {fmt(a2)}"
            rec["flag_B19_analyst_last2"] = data["flag_analyst_last2"]
            rec["status_B19_analyst_last2"] = (
                "GOOD" if data["flag_analyst_last2"] > 0 else (
                    "BAD" if data["flag_analyst_last2"] < 0 else "NEUTRAL"
                )
            )

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
                f"Δ={gm:+d} key={data['key_color_score']:+d} "
                f"analyst={rec['status_B19_analyst_last2']} ({i}/{len(tickers)})"
            )
        except Exception as e:
            print(f"[colors] {t} FAIL {e}")
            rows.append({
                "asof_date": asof,
                "ticker": t,
                "n_green": 0,
                "n_red": 0,
                "green_minus_red": 0,
                "flag_colors": 0,
                "status_colors": "NEUTRAL",
                "flag_B19_analyst_last2": 0,
                "status_B19_analyst_last2": "NEUTRAL",
                "val_B19_analyst_last2": f"error: {e}",
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
        "- **Perf* / Change % fields excluded** from green/red counts",
        "- **B19** = last 2 analyst actions (Upgrade=GOOD, Downgrade=BAD)",
        "",
        "| Ticker | green | red | Δ | key | analyst | status |",
        "|--------|------:|----:|--:|----:|---------|:------:|",
    ]
    for _, r in df.head(40).iterrows():
        lines.append(
            f"| {r.get('ticker')} | {int(r.get('n_green') or 0)} | {int(r.get('n_red') or 0)} | "
            f"{int(r.get('green_minus_red') or 0):+d} | {int(r.get('key_color_score') or 0):+d} | "
            f"{r.get('status_B19_analyst_last2')} | **{r.get('status_colors', '')}** |"
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
