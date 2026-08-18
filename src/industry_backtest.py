"""Separate backtest for industry predicts — does NOT re-run analysis.

Uses only:
  - prior report in 01_daily/industry/ (optional, for predicted direction)
  - price_store member basket (primary industry return)
  - optional industry ETF proxy when mapped

CLI:
  python -m src.industry_backtest --industry "Semiconductors" --as-of 2026-08-01
  python -m src.industry_backtest --industry "Semiconductors" --as-of 2026-08-01 --horizon-days 5
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config
from .industry_etf import etf_for
from .industry_map import members, resolve_industry

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "01_daily" / "industry" / "backtests"
PRED_DIR = ROOT / "01_daily" / "industry"
ET = ZoneInfo(config.TZ)


def _load_store() -> pd.DataFrame | None:
    try:
        from . import price_store as ps
        store = ps._load_store()
    except Exception as e:  # noqa: BLE001
        print(f"[industry_backtest] price store unavailable: {e}")
        return None
    if store is None or not len(store):
        return None
    store = store.copy()
    store["date"] = pd.to_datetime(store["date"]).dt.normalize()
    store["ticker"] = store["ticker"].astype(str).str.upper()
    return store


def _session_return(store: pd.DataFrame, ticker: str, as_of: pd.Timestamp, horizon: int) -> float | None:
    g = store[store["ticker"] == ticker].sort_values("date")
    past = g[g["date"] <= as_of]
    if past.empty:
        return None
    entry_px = float(past.iloc[-1]["close"])
    entry_dt = past.iloc[-1]["date"]
    fut = g[g["date"] > entry_dt]
    if fut.empty or entry_px <= 0:
        return None
    exit_px = float(fut.iloc[min(horizon, len(fut)) - 1]["close"])
    return exit_px / entry_px - 1.0


def basket_stats(industry: str, as_of: str, horizon: int) -> dict | None:
    store = _load_store()
    if store is None:
        return None
    mem = members(industry, as_of)
    tickers = mem["Ticker"].astype(str).str.upper().tolist()
    as_of_ts = pd.Timestamp(as_of)
    rets = []
    for t in tickers:
        r = _session_return(store, t, as_of_ts, horizon)
        if r is not None:
            rets.append(r)
    if not rets:
        return None
    arr = np.array(rets, dtype=float)
    return {
        "method": "equal_weight_members",
        "n_tickers": int(len(arr)),
        "median_return": float(np.median(arr)),
        "mean_return": float(np.mean(arr)),
        "pct_up": float((arr > 0).mean()),
        "pct_ge_4pct": float((arr >= 0.04).mean()),
        "pct_le_m4pct": float((arr <= -0.04).mean()),
    }


def etf_stats(industry: str, as_of: str, horizon: int) -> dict | None:
    proxy = etf_for(industry)
    if not proxy:
        return {"method": "etf_proxy", "ticker": None, "note": "no mapped ETF for this industry"}
    store = _load_store()
    if store is None:
        return {"method": "etf_proxy", "ticker": proxy, "note": "no price store"}
    r = _session_return(store, proxy.upper(), pd.Timestamp(as_of), horizon)
    if r is None:
        return {
            "method": "etf_proxy",
            "ticker": proxy,
            "note": "proxy not in price store — run price_store update including this ETF",
        }
    return {
        "method": "etf_proxy",
        "ticker": proxy,
        "return": float(r),
        "up": bool(r > 0),
        "ge_4pct": bool(r >= 0.04),
    }


def load_prior_prediction(industry: str, as_of: str) -> dict | None:
    safe = re.sub(r"[^\w\-]+", "_", industry)[:60]
    path = PRED_DIR / f"{as_of}_{safe}.json"
    if not path.exists():
        cands = list(PRED_DIR.glob(f"{as_of}_*.json"))
        for c in cands:
            try:
                d = json.loads(c.read_text())
                if d.get("industry") == industry:
                    return d
            except Exception:
                continue
        return None
    return json.loads(path.read_text())


def run(industry_query: str, as_of: str, horizon_days: int = 5) -> dict:
    industry = resolve_industry(industry_query, as_of)
    prior = load_prior_prediction(industry, as_of)
    basket = basket_stats(industry, as_of, horizon_days)
    etf = etf_stats(industry, as_of, horizon_days)

    pred_dir = (prior or {}).get("direction")
    hit = None
    if pred_dir in ("up", "down") and basket:
        med = basket["median_return"]
        hit = bool(med > 0) if pred_dir == "up" else bool(med < 0)

    report = {
        "mode": "backtest_only",
        "industry": industry,
        "as_of": as_of,
        "horizon_sessions": horizon_days,
        "prior_prediction": {
            "found": prior is not None,
            "direction": pred_dir,
            "confidence": (prior or {}).get("confidence"),
            "hits_n": (prior or {}).get("hits_n"),
            "path": f"01_daily/industry/{as_of}_*.json",
        },
        "member_basket": basket,
        "etf_proxy": etf,
        "direction_hit_vs_basket_median": hit,
        "how_industry_is_measured": (
            "Primary: equal-weight (median/mean) of Finviz industry members with "
            "prices in the store. Secondary: mapped industry ETF if present "
            "(not all 149 have a clean ETF). Sector ETFs are coarser and not used as the grade."
        ),
        "generated_at": datetime.now(ET).isoformat(),
    }
    return report


def render_md(report: dict) -> str:
    lines = [
        f"# Industry backtest — **{report['industry']}**",
        "",
        f"- **As-of (signal date):** {report['as_of']}",
        f"- **Horizon:** {report['horizon_sessions']} sessions after as-of",
        f"- **Mode:** backtest only (does not re-score news)",
        "",
        "## Prior prediction",
        "",
    ]
    pp = report["prior_prediction"]
    if not pp["found"]:
        lines.append("- No saved industry predict JSON for this as-of/industry.")
    else:
        lines.append(
            f"- Direction **{pp.get('direction')}** · conf {pp.get('confidence')} · "
            f"matched headlines {pp.get('hits_n')}"
        )

    lines += ["", "## Member basket (primary industry return)", ""]
    b = report.get("member_basket")
    if not b:
        lines.append("- No basket returns (price store empty or no members with history).")
    else:
        lines.append(
            f"- n={b['n_tickers']} · median={b['median_return']:.2%} · mean={b['mean_return']:.2%} · "
            f"pct_up={b['pct_up']:.1%} · pct≥4%={b['pct_ge_4pct']:.1%} · pct≤-4%={b['pct_le_m4pct']:.1%}"
        )

    lines += ["", "## ETF proxy (secondary)", ""]
    e = report.get("etf_proxy") or {}
    if e.get("ticker") and "return" in e:
        lines.append(
            f"- **{e['ticker']}** return={e['return']:.2%} · up={e.get('up')} · ≥4%={e.get('ge_4pct')}"
        )
    else:
        lines.append(f"- {e.get('note') or e}")

    lines += [
        "",
        f"## Direction hit (pred vs basket median): **{report.get('direction_hit_vs_basket_median')}**",
        "",
        "## Measurement note",
        "",
        report.get("how_industry_is_measured", ""),
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Industry backtest only (no news re-analysis)")
    ap.add_argument("--industry", required=True)
    ap.add_argument("--as-of", required=True, help="Signal date YYYY-MM-DD")
    ap.add_argument("--horizon-days", type=int, default=5)
    args = ap.parse_args()

    report = run(args.industry, as_of=args.as_of, horizon_days=args.horizon_days)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^\w\-]+", "_", report["industry"])[:60]
    stem = f"{report['as_of']}_{safe}_bt{report['horizon_sessions']}d"
    (OUT_DIR / f"{stem}.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    md = render_md(report)
    (OUT_DIR / f"{stem}.md").write_text(md, encoding="utf-8")
    print(md)
    print(f"[industry_backtest] wrote 01_daily/industry/backtests/{stem}.md")


if __name__ == "__main__":
    main()
