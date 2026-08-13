"""Unified multi-horizon stock suggestion book.

Merges existing fullscan layers into one ranked BUY / SELL list per horizon.
Does not replace upstream engines — it only composes their outputs.

Layers
------
  L1  Join rank (labels × weather)     structural fit for today's regime
  L2  Sector regime (scoreboard)       sleeve bias from sector predicts
  L3  General regime (scoreboard)      risk-on/off tilt for high-beta names
  L4  News actions                     event-driven force buy/sell
  L5  Policy soft gates                weak-sector caution from learnings (optional text)

Horizons & emphasis
-------------------
  1d   news heavy + join; sector light
  3d   news + join + sector
  1w   join + sector dominant; news fades
  1m   join structural + sector; news almost off

CLI
---
  python -m src.stock_book [--date YYYY-MM-DD] [--top 25]

Writes
------
  01_daily/<date>_stock_book.md
  data/stock_book/<date>_stock_book.csv   (all tickers, all horizon scores)
  data/stock_book/<date>_stock_book.json  (top books machine-readable)
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config, scoreboard

ROOT = Path(__file__).resolve().parent.parent
JOIN_DIR = ROOT / "data" / "join"
UNIVERSE_DIR = ROOT / "data" / "universe"
WEATHER_DIR = ROOT / "01_daily" / "weather"
NEWS_DIR = ROOT / "01_daily" / "news"
OUT_DIR = ROOT / "data" / "stock_book"
DAILY = ROOT / "01_daily"

HORIZONS = ("1d", "3d", "1w", "1m")

# Weights must sum ~1 per horizon (news can overshoot via abs, then we clip)
WEIGHTS = {
    #           join  sector  general  news
    "1d":      (0.35, 0.15,   0.10,    0.40),
    "3d":      (0.40, 0.25,   0.10,    0.25),
    "1w":      (0.45, 0.35,   0.10,    0.10),
    "1m":      (0.50, 0.40,   0.10,    0.00),
}


def _latest_file(dirpath: Path, pattern: str) -> Path | None:
    files = sorted(dirpath.glob(pattern))
    return files[-1] if files else None


def _load_join(date: str | None) -> tuple[pd.DataFrame, str]:
    if date:
        p = JOIN_DIR / f"{date}_ranked.csv"
        if not p.exists():
            raise SystemExit(f"missing join ranked: {p}")
        return pd.read_csv(p, low_memory=False), date
    p = _latest_file(JOIN_DIR, "*_ranked.csv")
    if not p:
        raise SystemExit("no data/join/*_ranked.csv — run join first")
    d = p.name.replace("_ranked.csv", "")
    return pd.read_csv(p, low_memory=False), d


def _load_membership(date: str) -> pd.DataFrame | None:
    p = UNIVERSE_DIR / f"{date}_membership.csv"
    if not p.exists():
        p2 = _latest_file(UNIVERSE_DIR, "*_membership.csv")
        if not p2:
            return None
        p = p2
    return pd.read_csv(p, low_memory=False)


def _load_weather(date: str) -> dict:
    p = WEATHER_DIR / f"{date}_weather.json"
    if not p.exists():
        p = WEATHER_DIR / "latest.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _sector_bias_map() -> dict[str, float]:
    """Map Finviz sector name -> bias in [-1, 1] from latest sector predict."""
    board = scoreboard.load()
    latest: dict[str, dict] = {}
    for r in board.get("runs", []):
        t = r.get("topic") or ""
        if not t.startswith("sector:"):
            continue
        if not r.get("predicted_direction"):
            continue
        sec = t.split(":", 1)[1]
        prev = latest.get(sec)
        if prev is None or r.get("date", "") >= prev.get("date", ""):
            latest[sec] = r

    out = {}
    for sec, r in latest.items():
        d = str(r.get("predicted_direction", "")).lower()
        conf = float(r.get("confidence_score") or 0.5)
        conf = max(0.3, min(1.0, conf))
        if d == "up":
            out[sec] = conf
        elif d == "down":
            out[sec] = -conf
        else:
            out[sec] = 0.0
    return out


def _general_bias() -> float:
    board = scoreboard.load()
    gens = [
        r for r in board.get("runs", [])
        if r.get("topic") == "general" and r.get("predicted_direction")
    ]
    if not gens:
        return 0.0
    r = sorted(gens, key=lambda x: x.get("date", ""))[-1]
    d = str(r.get("predicted_direction", "")).lower()
    conf = float(r.get("confidence_score") or 0.5)
    conf = max(0.3, min(1.0, conf))
    if d == "up":
        return conf
    if d == "down":
        return -conf
    return 0.0


def _load_news_actions(date: str) -> dict[str, dict]:
    """ticker -> {side, net, events, reason}."""
    # prefer same date, else latest
    candidates = [
        NEWS_DIR / f"{date}_actions.json",
    ]
    latest = _latest_file(NEWS_DIR, "*_actions.json")
    if latest:
        candidates.append(latest)
    data = None
    for p in candidates:
        if p and p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                break
            except (OSError, json.JSONDecodeError):
                continue
    if not data:
        return {}

    out: dict[str, dict] = {}
    # preferred structure: ticker_actions list or dict
    ta = data.get("ticker_actions")
    if isinstance(ta, dict):
        items = [{"ticker": k, **(v if isinstance(v, dict) else {"raw": v})} for k, v in ta.items()]
    elif isinstance(ta, list):
        items = ta
    else:
        items = data.get("edge_actions") or data.get("actions") or []
        if isinstance(items, dict):
            items = list(items.values())

    for row in items:
        if not isinstance(row, dict):
            continue
        t = str(row.get("ticker") or row.get("Ticker") or "").strip().upper()
        if not t:
            continue
        side = str(row.get("side") or row.get("action") or "").lower()
        net = row.get("net") or row.get("weight") or row.get("score") or 0
        try:
            net = float(net)
        except (TypeError, ValueError):
            net = 0.0
        if side in ("sell", "short"):
            signed = -abs(net) if net else -1.0
        elif side in ("buy", "long"):
            signed = abs(net) if net else 1.0
        else:
            # infer from net
            signed = float(net)
        events = row.get("events") or row.get("event") or []
        if isinstance(events, str):
            events = [events]
        out[t] = {
            "side": "buy" if signed > 0 else "sell",
            "net": signed,
            "events": events,
            "reason": row.get("reason") or row.get("note") or "",
        }
    return out


def _z(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    mu = s.mean()
    sd = s.std()
    if sd is None or sd == 0 or (isinstance(sd, float) and np.isnan(sd)):
        return pd.Series(0.0, index=s.index)
    return (s - mu) / sd


def build(date: str | None = None, top_n: int = 25) -> tuple[pd.DataFrame, dict]:
    join, date = _load_join(date)
    join = join.drop_duplicates(subset=["Ticker"], keep="first").copy()
    join["Ticker"] = join["Ticker"].astype(str).str.strip().str.upper()

    memb = _load_membership(date)
    if memb is not None and "Ticker" in memb.columns:
        memb = memb.drop_duplicates(subset=["Ticker"], keep="first")
        memb["Ticker"] = memb["Ticker"].astype(str).str.strip().str.upper()
        extra = [c for c in ("beta", "short", "mom", "profit", "size") if c in memb.columns and c not in join.columns]
        if extra:
            join = join.merge(memb[["Ticker"] + extra], on="Ticker", how="left")

    weather = _load_weather(date)
    sector_bias = _sector_bias_map()
    gen_bias = _general_bias()
    news = _load_news_actions(date)

    # L1 join score normalized
    if "score_norm" in join.columns:
        join["s_join"] = pd.to_numeric(join["score_norm"], errors="coerce").fillna(0.0)
    else:
        join["s_join"] = _z(join.get("total_score", pd.Series(0, index=join.index))).fillna(0.0)
    # squash to roughly [-1,1]
    join["s_join"] = np.tanh(join["s_join"].astype(float))

    # L2 sector
    join["s_sector"] = join["sector"].map(lambda s: float(sector_bias.get(s, 0.0))).fillna(0.0)

    # L3 general × beta sensitivity
    def beta_load(v):
        s = str(v).lower() if v == v else ""
        if s == "high":
            return 1.0
        if s == "mid":
            return 0.5
        if s == "low":
            return 0.15
        return 0.4

    if "beta" in join.columns:
        bl = join["beta"].map(beta_load)
    else:
        bl = pd.Series(0.4, index=join.index)
    join["s_general"] = float(gen_bias) * bl

    # L4 news
    def news_score(t):
        row = news.get(str(t).upper())
        if not row:
            return 0.0
        # normalize nets roughly into [-1,1]
        return float(np.tanh(row["net"] / 5.0))

    join["s_news"] = join["Ticker"].map(news_score).astype(float)

    # veto soft-downrank
    if "veto" in join.columns:
        veto = join["veto"].astype(str).str.lower().isin(["true", "1", "yes"])
        join.loc[veto, "s_join"] = join.loc[veto, "s_join"] * 0.2

    meta = {
        "date": date,
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "general_bias": gen_bias,
        "sector_bias": sector_bias,
        "n_news_tickers": len(news),
        "weather_risk": (weather.get("signals") or {}).get("risk")
        if isinstance(weather.get("signals"), dict)
        else weather.get("risk"),
        "n_universe": int(len(join)),
        "weights": WEIGHTS,
    }

    for h in HORIZONS:
        wj, ws, wg, wn = WEIGHTS[h]
        join[f"score_{h}"] = (
            wj * join["s_join"]
            + ws * join["s_sector"]
            + wg * join["s_general"]
            + wn * join["s_news"]
        )

    # reason codes
    def reasons(row):
        bits = []
        if abs(row["s_join"]) > 0.15:
            bits.append(f"join={row['s_join']:+.2f}")
        if abs(row["s_sector"]) > 0.1:
            bits.append(f"sector={row['s_sector']:+.2f}")
        if abs(row["s_general"]) > 0.05:
            bits.append(f"gen={row['s_general']:+.2f}")
        if abs(row["s_news"]) > 0.05:
            bits.append(f"news={row['s_news']:+.2f}")
            ev = news.get(str(row["Ticker"]).upper(), {}).get("events") or []
            if ev:
                bits.append("ev=" + ",".join(map(str, ev[:2])))
        return "; ".join(bits)

    join["reasons"] = join.apply(reasons, axis=1)

    meta["top_n"] = top_n
    return join, meta


def _book_side(df: pd.DataFrame, horizon: str, top_n: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    col = f"score_{horizon}"
    ranked = df.sort_values(col, ascending=False)
    buys = ranked.head(top_n)
    sells = ranked.tail(top_n).iloc[::-1]
    return buys, sells


def write_report(df: pd.DataFrame, meta: dict, top_n: int) -> None:
    date = meta["date"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DAILY.mkdir(parents=True, exist_ok=True)

    cols_keep = [
        "Ticker", "sector", "industry", "size",
        "s_join", "s_sector", "s_general", "s_news",
        "score_1d", "score_3d", "score_1w", "score_1m",
        "reasons", "bulls", "bears", "flags",
    ]
    cols_keep = [c for c in cols_keep if c in df.columns]
    csv_path = OUT_DIR / f"{date}_stock_book.csv"
    df[cols_keep].to_csv(csv_path, index=False)

    books = {}
    for h in HORIZONS:
        b, s = _book_side(df, h, top_n)
        books[h] = {
            "buy": b[["Ticker", f"score_{h}", "sector", "reasons"]].assign(
                side="buy"
            ).to_dict(orient="records"),
            "sell": s[["Ticker", f"score_{h}", "sector", "reasons"]].assign(
                side="sell"
            ).to_dict(orient="records"),
        }

    json_path = OUT_DIR / f"{date}_stock_book.json"
    json_path.write_text(
        json.dumps({"meta": meta, "books": books}, indent=2, default=str),
        encoding="utf-8",
    )

    L = [
        f"# Stock book — **{date}** (unified multi-horizon)",
        "",
        f"Generated: {meta['generated_at']}",
        "",
        "Merges **join (labels×weather)** + **sector predict bias** + **general regime** + **news actions**.",
        "Not a replacement for upstream engines — a single suggestion layer on top.",
        "",
        "## Regime snapshot",
        "",
        f"- **General bias:** {meta['general_bias']:+.2f} (−1 down … +1 up)",
        f"- **Weather risk signal:** {meta.get('weather_risk')}",
        f"- **News tickers mapped:** {meta['n_news_tickers']}",
        f"- **Universe scored:** {meta['n_universe']}",
        "",
        "### Sector bias (from latest sector predicts)",
        "",
        "| Sector | bias |",
        "|--------|------|",
    ]
    for sec, b in sorted(meta["sector_bias"].items(), key=lambda x: -abs(x[1])):
        L.append(f"| {sec} | {b:+.2f} |")

    L += [
        "",
        "## Horizon weights",
        "",
        "| Horizon | join | sector | general | news |",
        "|---------|------|--------|---------|------|",
    ]
    for h in HORIZONS:
        w = WEIGHTS[h]
        L.append(f"| {h} | {w[0]:.2f} | {w[1]:.2f} | {w[2]:.2f} | {w[3]:.2f} |")

    for h in HORIZONS:
        buys, sells = _book_side(df, h, top_n)
        L += [
            "",
            f"## Horizon **{h}** — TOP {top_n} BUY",
            "",
            "| Ticker | Score | Sector | Reasons |",
            "|--------|-------|--------|---------|",
        ]
        for _, r in buys.iterrows():
            L.append(
                f"| {r['Ticker']} | {r[f'score_{h}']:+.3f} | {r.get('sector','')} | {r.get('reasons','')} |"
            )
        L += [
            "",
            f"## Horizon **{h}** — TOP {top_n} SELL / avoid",
            "",
            "| Ticker | Score | Sector | Reasons |",
            "|--------|-------|--------|---------|",
        ]
        for _, r in sells.iterrows():
            L.append(
                f"| {r['Ticker']} | {r[f'score_{h}']:+.3f} | {r.get('sector','')} | {r.get('reasons','')} |"
            )

    L += [
        "",
        "## How to read",
        "",
        "- **BUY** = highest combined score for that horizon (not financial advice).",
        "- **SELL/avoid** = lowest scores (underperform / hostile labels / news sells).",
        "- **1d** leans on news; **1m** leans on structural join + sector sleeves.",
        "- Cross-check weak sectors in learnings before sizing.",
        "",
        f"CSV: `data/stock_book/{date}_stock_book.csv`",
        f"JSON: `data/stock_book/{date}_stock_book.json`",
        "",
    ]

    md_path = DAILY / f"{date}_stock_book.md"
    md_path.write_text("\n".join(L), encoding="utf-8")
    print(f"[stock-book] {md_path}")
    print(f"[stock-book] {csv_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()
    df, meta = build(args.date, top_n=args.top)
    write_report(df, meta, top_n=args.top)


if __name__ == "__main__":
    main()
