"""Channel 1: deterministic data fetch. Exact numbers only — no LLM.

Sources: FRED API (fallback: macro_indicators DB table), yfinance,
Supabase `news` table (last 24h). Output: dict + Markdown block, archived to
01_daily/_channel1/<date>_<stage>.json so every prediction is auditable.

CLI: python -m src.fetch_channel1 --stage predict|outcome [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from . import config, db

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_SERIES = ["DGS30", "DGS10", "DFII10", "BAMLH0A0HYM2", "SOFR", "IORB",
               "RRPONTSYD", "USEPUINDXD"]


# ------------------------------------------------------------------ helpers
def _yf_history(symbol: str, days: int = 45) -> list[dict]:
    """[(date, open, close)] ascending via yfinance; [] on failure."""
    try:
        import yfinance as yf
        hist = yf.Ticker(symbol).history(period=f"{days}d", interval="1d")
        out = [{"date": str(idx.date()), "open": float(row["Open"]),
                "close": float(row["Close"])}
               for idx, row in hist.iterrows()]
        return out
    except Exception as e:  # noqa: BLE001
        print(f"[ch1] yfinance {symbol} failed: {e}")
        return []


def _pct(a: float, b: float) -> float | None:
    return round((a / b - 1) * 100, 2) if b else None


def _fred(series_id: str) -> list[tuple[str, float]]:
    """[(date, value)] ascending. FRED API first, DB fallback."""
    if config.FRED_API_KEY:
        try:
            r = requests.get(FRED_URL, params={
                "series_id": series_id, "api_key": config.FRED_API_KEY,
                "file_type": "json", "sort_order": "desc", "limit": 45},
                timeout=20)
            r.raise_for_status()
            rows = [(o["date"], float(o["value"]))
                    for o in r.json().get("observations", [])
                    if o.get("value") not in (".", None)]
            if rows:
                return sorted(rows)
        except Exception as e:  # noqa: BLE001
            print(f"[ch1] FRED {series_id} API failed: {e} (trying DB)")
    return db.macro_series(series_id)


def _delta_block(rows: list[tuple[str, float]]) -> dict:
    """current + 1d/1w/1m deltas from an ascending (date, value) series."""
    if not rows:
        return {"available": False}
    cur_d, cur = rows[-1]
    def ago(n):
        return rows[-1 - n][1] if len(rows) > n else None
    d1, d5, d21 = ago(1), ago(5), ago(21)
    return {
        "available": True, "date": cur_d, "current": round(cur, 4),
        "delta_1d": round(cur - d1, 4) if d1 is not None else None,
        "delta_1w": round(cur - d5, 4) if d5 is not None else None,
        "delta_1m": round(cur - d21, 4) if d21 is not None else None,
    }


# ------------------------------------------------------------------ fetchers
def fetch_fred_block() -> dict:
    return {s: _delta_block(_fred(s)) for s in FRED_SERIES}


def fetch_vix() -> dict:
    vix, vix3m = _yf_history("^VIX"), _yf_history("^VIX3M")
    out = {"vix": {}, "vix3m": {}, "ratio": None}
    if vix:
        out["vix"] = {"date": vix[-1]["date"], "current": round(vix[-1]["close"], 2),
                      "delta_1d": round(vix[-1]["close"] - vix[-2]["close"], 2) if len(vix) > 1 else None,
                      "delta_1w": round(vix[-1]["close"] - vix[-6]["close"], 2) if len(vix) > 5 else None}
    if vix3m:
        out["vix3m"] = {"date": vix3m[-1]["date"], "current": round(vix3m[-1]["close"], 2)}
    if vix and vix3m and vix3m[-1]["close"]:
        out["ratio"] = round(vix[-1]["close"] / vix3m[-1]["close"], 3)
    return out


def _pct_block(symbol: str) -> dict:
    h = _yf_history(symbol, days=10)
    if len(h) < 2:
        return {"available": False}
    return {"available": True, "date": h[-1]["date"],
            "last": round(h[-1]["close"], 2),
            "pct_1d": _pct(h[-1]["close"], h[-2]["close"])}


def fetch_commodities_fx() -> dict:
    dxy = _yf_history("DX-Y.NYB", days=45)
    return {
        "CL=F": _pct_block("CL=F"), "BZ=F": _pct_block("BZ=F"),
        "DXY": ({"available": True, "date": dxy[-1]["date"],
                 "pct_1d": _pct(dxy[-1]["close"], dxy[-2]["close"]),
                 "pct_1m": _pct(dxy[-1]["close"], dxy[-22]["close"])}
                if len(dxy) >= 22 else {"available": False}),
    }


def fetch_futures() -> dict:
    return {"ES=F": _pct_block("ES=F"), "NQ=F": _pct_block("NQ=F")}


ASIA = {"^N225": "Nikkei", "^HSI": "Hang Seng", "000001.SS": "Shanghai",
        "^KS11": "Kospi", "^AXJO": "ASX200"}
EUROPE = {"^FTSE": "FTSE", "^GDAXI": "DAX", "^FCHI": "CAC",
          "^STOXX50E": "EuroStoxx50"}


def _composite(symbols: dict) -> dict:
    per, vals = {}, []
    for sym, name in symbols.items():
        b = _pct_block(sym)
        per[name] = b
        if b.get("available") and b.get("pct_1d") is not None:
            vals.append(b["pct_1d"])
    return {"per_index": per,
            "composite_avg": round(sum(vals) / len(vals), 2) if vals else None}


def fetch_global_sessions() -> dict:
    return {"asia": _composite(ASIA), "europe": _composite(EUROPE)}


def fetch_yield_spx_corr() -> dict:
    """5-day rolling correlation of daily CHANGES: 10Y yield (^TNX) vs SPX."""
    tnx, spx = _yf_history("^TNX", 15), _yf_history("^GSPC", 15)
    if len(tnx) < 7 or len(spx) < 7:
        return {"available": False}
    sy = {r["date"]: r["close"] for r in tnx}
    ss = {r["date"]: r["close"] for r in spx}
    common = sorted(set(sy) & set(ss))[-6:]
    if len(common) < 6:
        return {"available": False}
    dy = [sy[common[i]] - sy[common[i - 1]] for i in range(1, len(common))]
    ds = [(ss[common[i]] / ss[common[i - 1]] - 1) for i in range(1, len(common))]
    my, ms = sum(dy) / len(dy), sum(ds) / len(ds)
    cov = sum((a - my) * (b - ms) for a, b in zip(dy, ds))
    vy = sum((a - my) ** 2 for a in dy) ** 0.5
    vs = sum((b - ms) ** 2 for b in ds) ** 0.5
    corr = round(cov / (vy * vs), 3) if vy and vs else None
    return {"available": corr is not None, "corr_5d": corr,
            "window": f"{common[0]}..{common[-1]}"}


def fetch_fear_greed() -> dict:
    """CNN Fear & Greed — always flagged as prior-close snapshot."""
    try:
        r = requests.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        fg = r.json().get("fear_and_greed", {})
        return {"available": True, "value": round(fg.get("score", 0), 1),
                "label": fg.get("rating"),
                "note": "prior-close snapshot, not live"}
    except Exception as e:  # noqa: BLE001
        return {"available": False, "note": f"fetch failed: {e}"}


def fetch_actual_close() -> dict:
    """Outcome stage: actual index results for today."""
    out = {}
    for sym, name in (("^GSPC", "SPX"), ("^DJI", "DOW"), ("^IXIC", "NDX")):
        h = _yf_history(sym, days=5)
        if len(h) >= 2:
            out[name] = {"date": h[-1]["date"], "open": round(h[-1]["open"], 2),
                         "close": round(h[-1]["close"], 2),
                         "pct_change": _pct(h[-1]["close"], h[-2]["close"]),
                         "prev_close": round(h[-2]["close"], 2)}
        else:
            out[name] = {"available": False}
    return out


def fetch_news_block() -> dict:
    rows = db.recent_news(hours=24, limit=30)
    return {"available": bool(rows), "count": len(rows), "items": rows}


# ------------------------------------------------------------------ assembly
def build(stage: str) -> dict:
    data = {
        "stage": stage,
        "fetched_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "fred": fetch_fred_block(),
        "vix": fetch_vix(),
        "commodities_fx": fetch_commodities_fx(),
        "futures": fetch_futures(),
        "global_sessions": fetch_global_sessions(),
        "yield_spx_corr": fetch_yield_spx_corr(),
        "fear_greed": fetch_fear_greed(),
        "fedwatch": {"available": False,
                     "note": "CME FedWatch not directly scrapable; "
                             "CONFIDENCE low — derive via Channel 2 search"},
        "news_24h": fetch_news_block(),
    }
    if stage == "outcome":
        data["actual_close"] = fetch_actual_close()
    return data


def _fmt_delta(label: str, b: dict, unit: str = "") -> str:
    if not b.get("available"):
        return f"[{label}: UNAVAILABLE]"
    return (f"[{label}: {b['current']}{unit} as of {b['date']} | "
            f"1d {b['delta_1d']:+} | 1w {b['delta_1w']:+} | 1m {b['delta_1m']:+}]"
            if b.get("delta_1w") is not None else
            f"[{label}: {b['current']}{unit} as of {b['date']}]")


def to_markdown(data: dict) -> str:
    f = data["fred"]
    lines = ["=== CHANNEL 1: PRE-FETCHED DATA (exact, do not alter) ==="]
    v = data["vix"]
    if v.get("vix"):
        vv = v["vix"]
        lines.append(f"[VIX: {vv['current']} as of {vv['date']} | 1d {vv['delta_1d']:+} | 1w {vv['delta_1w']:+}]"
                     .replace("None", "n/a"))
    if v.get("vix3m"):
        lines.append(f"[VIX3M: {v['vix3m']['current']} | ratio VIX/VIX3M: {v['ratio']}"
                     f"{' — BACKWARDATION (>1.0)' if (v['ratio'] or 0) > 1.0 else ''}]")
    for s in ("DGS30", "DGS10", "DFII10", "BAMLH0A0HYM2", "RRPONTSYD", "USEPUINDXD"):
        lines.append(_fmt_delta(s, f.get(s, {})))
    sofr, iorb = f.get("SOFR", {}), f.get("IORB", {})
    if sofr.get("available") and iorb.get("available"):
        spread = round(sofr["current"] - iorb["current"], 4)
        lines.append(f"[SOFR-IORB spread: {spread:+} (SOFR {sofr['current']}, IORB {iorb['current']})]")
    else:
        lines.append("[SOFR-IORB spread: UNAVAILABLE]")
    cf = data["commodities_fx"]
    for sym in ("CL=F", "BZ=F"):
        b = cf.get(sym, {})
        lines.append(f"[{sym}: {b.get('pct_1d')}% 1d]" if b.get("available")
                     else f"[{sym}: UNAVAILABLE]")
    dxy = cf.get("DXY", {})
    lines.append(f"[DXY: 1d {dxy.get('pct_1d')}% | 1m {dxy.get('pct_1m')}%]"
                 if dxy.get("available") else "[DXY: UNAVAILABLE]")
    for sym in ("ES=F", "NQ=F"):
        b = data["futures"].get(sym, {})
        lines.append(f"[{sym} premarket: {b.get('pct_1d')}% vs prev close]"
                     if b.get("available") else f"[{sym}: UNAVAILABLE]")
    gs = data["global_sessions"]
    for region in ("asia", "europe"):
        r = gs[region]
        det = ", ".join(f"{k} {v.get('pct_1d')}%" for k, v in r["per_index"].items())
        tag = "final" if region == "asia" else "in progress (partial)"
        lines.append(f"[{region.upper()} ({tag}): {det} — composite avg {r['composite_avg']}%]")
    lines.append(f"[CME FedWatch: {data['fedwatch']['note']}]")
    fgx = data["fear_greed"]
    lines.append(f"[Fear & Greed: {fgx.get('value')} ({fgx.get('label')}) — {fgx.get('note')}]"
                 if fgx.get("available") else "[Fear & Greed: UNAVAILABLE]")
    c = data["yield_spx_corr"]
    lines.append(f"[5-day corr 10Y yield vs SPX: {c.get('corr_5d')} ({c.get('window')})]"
                 if c.get("available") else "[5-day corr 10Y/SPX: UNAVAILABLE]")
    nb = data["news_24h"]
    if nb["available"]:
        lines.append(f"[NEWS last 24h ({nb['count']} items from Supabase):")
        for it in nb["items"]:
            lines.append(f"  - ({it['source']}) {it['title']}")
    else:
        lines.append("[NEWS last 24h: DB unavailable or empty]")
    if "actual_close" in data:
        lines.append("\n=== ACTUAL CLOSE DATA ===")
        for name, b in data["actual_close"].items():
            lines.append(f"[{name}: open {b.get('open')}, close {b.get('close')}, "
                         f"{b.get('pct_change')}% vs prev close {b.get('prev_close')}]"
                         if b.get("open") else f"[{name}: UNAVAILABLE]")
    return "\n".join(lines)


def save(data: dict, date_str: str, stage: str) -> str:
    os.makedirs(config.CHANNEL1_DIR, exist_ok=True)
    path = os.path.join(config.CHANNEL1_DIR, f"{date_str}_{stage}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=["predict", "outcome"], required=True)
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    data = build(args.stage)
    path = save(data, date_str, args.stage)
    print(to_markdown(data))
    print(f"\n[saved] {path}")


if __name__ == "__main__":
    main()
