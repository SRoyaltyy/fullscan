"""Unified multi-horizon stock suggestion book.

Horizons: 1d, 3d, 1w, 2w, 1m
Layers: join (labels×weather) + sector bias + general regime + news actions

CLI: python -m src.stock_book [--date YYYY-MM-DD] [--top 25]
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

HORIZONS = ("1d", "3d", "1w", "2w", "1m")

WEIGHTS = {
    #           join  sector  general  news
    "1d":      (0.35, 0.15,   0.10,    0.40),
    "3d":      (0.40, 0.25,   0.10,    0.25),
    "1w":      (0.45, 0.30,   0.10,    0.15),
    "2w":      (0.48, 0.35,   0.10,    0.07),
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


# Map book horizon -> multi-timeframe call key stored on scoreboard runs.
# 1d uses the run's headline predicted_direction; longer horizons prefer the
# LLM's explicit HORIZON_* call and fall back to the 1d direction when absent.
HORIZON_CALL_KEY = {
    "1d": None,
    "3d": "HORIZON_3D",
    "1w": "HORIZON_1W",
    "2w": "HORIZON_2W",
    "1m": "HORIZON_1M",
}


def _dir_conf_to_bias(direction: str, conf: float) -> float:
    conf = max(0.3, min(1.0, float(conf or 0.5)))
    d = str(direction or "").lower()
    if d == "up":
        return conf
    if d == "down":
        return -conf
    return 0.0


def _latest_runs_by_topic() -> dict[str, dict]:
    board = scoreboard.load()
    latest: dict[str, dict] = {}
    for r in board.get("runs", []):
        t = r.get("topic") or ""
        if not r.get("predicted_direction"):
            continue
        prev = latest.get(t)
        if prev is None or r.get("date", "") >= prev.get("date", ""):
            latest[t] = r
    return latest


def _bias_for(run: dict | None, horizon: str) -> float:
    """Signed bias for one topic at one horizon (uses horizon_calls when stored)."""
    if not run:
        return 0.0
    key = HORIZON_CALL_KEY.get(horizon)
    if key:
        hc = (run.get("horizon_calls") or {}).get(key)
        if hc and hc.get("direction"):
            return _dir_conf_to_bias(hc["direction"], hc.get("confidence"))
    return _dir_conf_to_bias(run.get("predicted_direction"), run.get("confidence_score"))


def _accuracy_gates() -> tuple[dict[str, float], dict[str, dict]]:
    """Learning gate: graded outcomes scale how much a topic's bias is trusted.

    Reads direction_hit over all graded scoreboard runs per topic.
    n>=3 and hit rate <45%  -> bias halved (the loop was losing here)
    n>=3 and hit rate <55%  -> bias trimmed to 0.85
    otherwise                 -> full weight
    Returns (gates, stats) where stats[topic] = {hit_rate, n, gate}."""
    board = scoreboard.load()
    hits: dict[str, list[float]] = {}
    for r in board.get("runs", []):
        t = r.get("topic") or ""
        h = r.get("direction_hit")
        if h is None:
            continue
        hits.setdefault(t, []).append(1.0 if h else 0.0)
    gates: dict[str, float] = {}
    stats: dict[str, dict] = {}
    for t, arr in hits.items():
        n = len(arr)
        hr = sum(arr) / n
        if n >= 3 and hr < 0.45:
            g = 0.5
        elif n >= 3 and hr < 0.55:
            g = 0.85
        else:
            g = 1.0
        gates[t] = g
        stats[t] = {"hit_rate": round(hr, 3), "n": n, "gate": g}
    return gates, stats


# Size buckets for guaranteed small/mid representation in the book.
SIZE_BUCKETS = {
    "large+": {"mega", "large"},
    "mid": {"mid"},
    "small/micro": {"small", "micro"},
}


def _load_news_actions(date: str) -> dict[str, dict]:
    candidates = [NEWS_DIR / f"{date}_actions.json"]
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
    news = _load_news_actions(date)
    latest_runs = _latest_runs_by_topic()
    gates, gate_stats = _accuracy_gates()

    if "score_norm" in join.columns:
        join["s_join"] = pd.to_numeric(join["score_norm"], errors="coerce").fillna(0.0)
    else:
        s = pd.to_numeric(join.get("total_score", 0), errors="coerce")
        sd = s.std()
        join["s_join"] = ((s - s.mean()) / sd) if sd and sd == sd and sd != 0 else 0.0
    join["s_join"] = np.tanh(join["s_join"].astype(float))

    def beta_load(v):
        s = str(v).lower() if v == v else ""
        if s == "high":
            return 1.0
        if s == "mid":
            return 0.5
        if s == "low":
            return 0.15
        return 0.4

    bl = join["beta"].map(beta_load) if "beta" in join.columns else pd.Series(0.4, index=join.index)

    def news_score(t):
        row = news.get(str(t).upper())
        if not row:
            return 0.0
        return float(np.tanh(row["net"] / 5.0))

    join["s_news"] = join["Ticker"].map(news_score).astype(float)

    if "veto" in join.columns:
        veto = join["veto"].astype(str).str.lower().isin(["true", "1", "yes"])
        join.loc[veto, "s_join"] = join.loc[veto, "s_join"] * 0.2

    # Per-horizon biases: longer horizons use the predictors' explicit
    # HORIZON_* calls (when the LLM stored them), gated by graded accuracy.
    gen_run = latest_runs.get("general")
    gen_gate = gates.get("general", 1.0)
    sector_runs = {t.split(":", 1)[1]: r for t, r in latest_runs.items() if t.startswith("sector:")}
    sectors_present = [s for s in join["sector"].dropna().unique()] if "sector" in join.columns else []

    for h in HORIZONS:
        sec_bias_h = {
            sec: _bias_for(sector_runs.get(sec), h) * gates.get(f"sector:{sec}", 1.0)
            for sec in sectors_present
        }
        join[f"s_sector_{h}"] = join["sector"].map(sec_bias_h).fillna(0.0) if "sector" in join.columns else 0.0
        join[f"s_general_{h}"] = float(_bias_for(gen_run, h) * gen_gate) * bl

    # Representative 1d columns kept for CSV readability / downstream tools.
    join["s_sector"] = join["s_sector_1d"]
    join["s_general"] = join["s_general_1d"]
    gen_bias = float(_bias_for(gen_run, "1d") * gen_gate)
    sector_bias = {sec: float(_bias_for(r, "1d") * gates.get(f"sector:{sec}", 1.0))
                   for sec, r in sector_runs.items()}

    meta = {
        "date": date,
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "general_bias": gen_bias,
        "sector_bias": sector_bias,
        "accuracy_gates": gate_stats,
        "n_news_tickers": len(news),
        "weather_risk": (weather.get("signals") or {}).get("risk")
        if isinstance(weather.get("signals"), dict)
        else weather.get("risk"),
        "n_universe": int(len(join)),
        "weights": WEIGHTS,
        "horizons": list(HORIZONS),
        "top_n": top_n,
    }

    for h in HORIZONS:
        wj, ws, wg, wn = WEIGHTS[h]
        join[f"score_{h}"] = (
            wj * join["s_join"]
            + ws * join[f"s_sector_{h}"]
            + wg * join[f"s_general_{h}"]
            + wn * join["s_news"]
        )

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
    return join, meta


def _book_side(df: pd.DataFrame, horizon: str, top_n: int):
    col = f"score_{horizon}"
    ranked = df.sort_values(col, ascending=False)
    buys = ranked.head(top_n)
    sells = ranked.tail(top_n).iloc[::-1]
    return buys, sells


def _bucket_side(df: pd.DataFrame, horizon: str, bucket: str, n: int = 8):
    """Top/bottom n within one size bucket — guarantees small/mid visibility."""
    if "size" not in df.columns:
        return None, None
    sub = df[df["size"].astype(str).str.lower().isin(SIZE_BUCKETS[bucket])]
    if sub.empty:
        return None, None
    return _book_side(sub, horizon, min(n, max(1, len(sub) // 2)))


def _row_dict(r: pd.Series, horizon: str, side: str) -> dict:
    return {
        "ticker": r["Ticker"],
        "score": float(r[f"score_{horizon}"]),
        "sector": r.get("sector"),
        "size": r.get("size"),
        "side": side,
        "reasons": r.get("reasons"),
    }


def write_report(df: pd.DataFrame, meta: dict, top_n: int) -> None:
    date = meta["date"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DAILY.mkdir(parents=True, exist_ok=True)

    cols_keep = [
        "Ticker", "sector", "industry", "size",
        "s_join", "s_sector", "s_general", "s_news",
        "score_1d", "score_3d", "score_1w", "score_2w", "score_1m",
        "reasons", "bulls", "bears", "flags",
    ]
    cols_keep = [c for c in cols_keep if c in df.columns]
    csv_path = OUT_DIR / f"{date}_stock_book.csv"
    df[cols_keep].to_csv(csv_path, index=False)

    books = {}
    for h in HORIZONS:
        b, s = _book_side(df, h, top_n)
        entry = {
            "buy": [_row_dict(r, h, "buy") for _, r in b.iterrows()],
            "sell": [_row_dict(r, h, "sell") for _, r in s.iterrows()],
            "buy_by_size": {},
            "sell_by_size": {},
        }
        for bucket in SIZE_BUCKETS:
            bb, ss = _bucket_side(df, h, bucket)
            if bb is not None:
                entry["buy_by_size"][bucket] = [_row_dict(r, h, "buy") for _, r in bb.iterrows()]
                entry["sell_by_size"][bucket] = [_row_dict(r, h, "sell") for _, r in ss.iterrows()]
        books[h] = entry

    json_path = OUT_DIR / f"{date}_stock_book.json"
    json_path.write_text(
        json.dumps({"meta": meta, "books": books}, indent=2, default=str),
        encoding="utf-8",
    )

    L = [
        f"# Stock book — **{date}** (1d / 3d / 1w / 2w / 1m)",
        "",
        f"Generated: {meta['generated_at']}",
        "",
        "Layers: join (labels×weather) + sector predict + general regime + news actions.",
        "",
        "## Regime snapshot",
        "",
        f"- **General bias:** {meta['general_bias']:+.2f}",
        f"- **Weather risk:** {meta.get('weather_risk')}",
        f"- **News tickers:** {meta['n_news_tickers']}",
        f"- **Universe:** {meta['n_universe']}",
        "",
        "### Sector bias",
        "",
        "| Sector | bias |",
        "|--------|------|",
    ]
    for sec, b in sorted(meta["sector_bias"].items(), key=lambda x: -abs(x[1])):
        L.append(f"| {sec} | {b:+.2f} |")

    gates = meta.get("accuracy_gates") or {}
    if gates:
        L += [
            "",
            "### Learning gate (graded accuracy → how much each predictor is trusted)",
            "",
            "Bias from a topic with a weak graded track record is scaled down before it can move scores.",
            "",
            "| Topic | hit rate | graded runs | weight applied |",
            "|-------|----------|-------------|----------------|",
        ]
        for t, st in sorted(gates.items()):
            L.append(f"| {t} | {st['hit_rate']:.0%} | {st['n']} | ×{st['gate']:.2f} |")

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
            f"## {h} — BUY (top {top_n})",
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
            f"## {h} — SELL / avoid (bottom {top_n})",
            "",
            "| Ticker | Score | Sector | Reasons |",
            "|--------|-------|--------|---------|",
        ]
        for _, r in sells.iterrows():
            L.append(
                f"| {r['Ticker']} | {r[f'score_{h}']:+.3f} | {r.get('sector','')} | {r.get('reasons','')} |"
            )

        # Guaranteed small/mid-cap visibility: best buys inside each size bucket.
        L += [
            "",
            f"### {h} — BUY by size bucket",
            "",
        ]
        for bucket in SIZE_BUCKETS:
            bb, _ss = _bucket_side(df, h, bucket)
            L += ["", f"**{bucket}**", "",
                  "| Ticker | Score | Sector | Reasons |",
                  "|--------|-------|--------|---------|"]
            if bb is None:
                L.append("| — | — | — | no labelled names in bucket |")
            else:
                for _, r in bb.iterrows():
                    L.append(
                        f"| {r['Ticker']} | {r[f'score_{h}']:+.3f} | {r.get('sector','')} | {r.get('reasons','')} |"
                    )

    L += [
        "",
        "## Read",
        "",
        "- **1d** news-heavy; **1m** structural join + sector.",
        "- Longer horizons use the predictors' explicit 3d/1w/2w/1m calls when stored, else fall back to the 1d call.",
        "- Predictor bias is scaled by its graded hit rate (learning gate) — weak topics move scores less.",
        "- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).",
        "",
        f"CSV: `data/stock_book/{date}_stock_book.csv`",
        f"JSON: `data/stock_book/{date}_stock_book.json`",
        "",
    ]
    md_path = DAILY / f"{date}_stock_book.md"
    md_path.write_text("\n".join(L), encoding="utf-8")
    print(f"[stock-book] {md_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()
    df, meta = build(args.date, top_n=args.top)
    write_report(df, meta, top_n=args.top)


if __name__ == "__main__":
    main()
