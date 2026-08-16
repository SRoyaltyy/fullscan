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

# Tradeable universe gates (Finviz export units)
# Market Cap column is in *millions* USD → 80 == $80M
# Average Volume in export is in *thousands* of shares → 500 == 500k shares
MIN_MARKET_CAP_M = 80.0
MIN_AVG_VOL_K = 500.0
REBOUND_WINDOW = 40
REBOUND_BOOST = 0.15  # soft add to each horizon score when floor+tape fires


def _load_finviz_liquidity(date: str) -> pd.DataFrame:
    """Ticker → market_cap_m, avg_vol_k from dated (or latest) Finviz export."""
    export_dir = ROOT / "data" / "exports"
    path = export_dir / f"finviz_{date}.csv"
    if not path.exists():
        files = sorted(export_dir.glob("finviz_????-??-??.csv"))
        if not files:
            return pd.DataFrame(columns=["Ticker", "market_cap_m", "avg_vol_k"])
        path = files[-1]
    df = pd.read_csv(path, low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    out = pd.DataFrame()
    out["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    out["market_cap_m"] = pd.to_numeric(df.get("Market Cap"), errors="coerce")
    out["avg_vol_k"] = pd.to_numeric(df.get("Average Volume"), errors="coerce")
    return out.drop_duplicates("Ticker", keep="first")


def _rebound_flags(date: str) -> pd.DataFrame:
    """Stock-specific checklist score floor + tape_ok (sparse mean-reversion tag).

    Prefer checklist_history.parquet; else latest daily checklist with c3 proxy.
    Returns DataFrame[Ticker, rebound, at_low, tape_ok].
    """
    empty = pd.DataFrame(columns=["Ticker", "rebound", "at_low", "tape_ok"])
    hist = ROOT / "data" / "checklist" / "checklist_history.parquet"
    daily = ROOT / "data" / "checklist" / f"{date}_checklist.csv"
    if not daily.exists():
        files = sorted((ROOT / "data" / "checklist").glob("*_checklist.csv"))
        daily = files[-1] if files else None

    h = None
    if hist.exists():
        try:
            h = pd.read_parquet(hist)
        except Exception:
            h = None

    if h is not None and len(h):
        h["asof_date"] = pd.to_datetime(h["asof_date"])
        h["Ticker"] = h["Ticker"].astype(str).str.upper()
        h["checklist_score"] = pd.to_numeric(h["checklist_score"], errors="coerce")
        h = h[h["asof_date"] <= pd.Timestamp(date)].sort_values(["Ticker", "asof_date"])
        if "c1_candle_pass" in h.columns:
            h["tape_ok"] = h["c1_candle_pass"].astype(str).str.lower().isin(["true", "1"])
        else:
            h["tape_ok"] = False
        g = h.groupby("Ticker", group_keys=False)
        h["roll_min"] = g["checklist_score"].transform(
            lambda s: s.rolling(REBOUND_WINDOW, min_periods=max(10, REBOUND_WINDOW // 2)).min()
        )
        h["at_low"] = h["checklist_score"] <= h["roll_min"]
        last = h.groupby("Ticker", as_index=False).tail(1)
        last["rebound"] = last["at_low"] & last["tape_ok"]
        return last[["Ticker", "rebound", "at_low", "tape_ok"]]

    if daily is not None and Path(daily).exists():
        d = pd.read_csv(daily, low_memory=False)
        d["Ticker"] = d["Ticker"].astype(str).str.upper()
        if "c1_candle_pass" in d.columns:
            tape = d["c1_candle_pass"].astype(str).str.lower().isin(["true", "1"])
        else:
            tape = False
        if "c3_down_n" in d.columns:
            at_low = pd.to_numeric(d["c3_down_n"], errors="coerce").fillna(0) >= 3
        else:
            at_low = False
        out = pd.DataFrame({"Ticker": d["Ticker"], "at_low": at_low, "tape_ok": tape})
        out["rebound"] = out["at_low"] & out["tape_ok"]
        return out.drop_duplicates("Ticker")
    return empty


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
    if not run:
        return 0.0
    key = HORIZON_CALL_KEY.get(horizon)
    if key:
        hc = (run.get("horizon_calls") or {}).get(key)
        if hc and hc.get("direction"):
            return _dir_conf_to_bias(hc["direction"], hc.get("confidence"))
    return _dir_conf_to_bias(run.get("predicted_direction"), run.get("confidence_score"))


def _accuracy_gates() -> tuple[dict[str, float], dict[str, dict]]:
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

    # Liquidity / size gates (user: avg vol >500k shares, mcap >$80M)
    liq = _load_finviz_liquidity(date)
    if len(liq):
        join = join.merge(liq, on="Ticker", how="left")
    else:
        join["market_cap_m"] = np.nan
        join["avg_vol_k"] = np.nan
    n_before = len(join)
    liquid = (
        (join["market_cap_m"].fillna(0) >= MIN_MARKET_CAP_M)
        & (join["avg_vol_k"].fillna(0) >= MIN_AVG_VOL_K)
    )
    join["liquid"] = liquid
    join = join.loc[liquid].copy()
    print(
        f"[stock-book] liquidity filter mcap>={MIN_MARKET_CAP_M}M vol>={MIN_AVG_VOL_K}k: "
        f"{n_before} → {len(join)}"
    )

    reb = _rebound_flags(date)
    join["rebound"] = False
    join["at_low"] = False
    if len(reb):
        join = join.drop(columns=[c for c in ("rebound", "at_low", "tape_ok") if c in join.columns], errors="ignore")
        join = join.merge(reb, on="Ticker", how="left")
        join["rebound"] = join["rebound"].fillna(False).astype(bool)
        if "at_low" in join.columns:
            join["at_low"] = join["at_low"].fillna(False).astype(bool)
        print(f"[stock-book] rebound flags: {int(join['rebound'].sum())} names")

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
        if "rebound" in join.columns:
            join.loc[join["rebound"], f"score_{h}"] = (
                join.loc[join["rebound"], f"score_{h}"] + REBOUND_BOOST
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
        if row.get("rebound"):
            bits.append("rebound_floor")
        return "; ".join(bits)

    join["reasons"] = join.apply(reasons, axis=1)
    meta["n_after_liquidity"] = int(len(join))
    meta["min_market_cap_m"] = MIN_MARKET_CAP_M
    meta["min_avg_vol_k"] = MIN_AVG_VOL_K
    meta["n_rebound"] = int(join["rebound"].sum()) if "rebound" in join.columns else 0
    return join, meta


def _book_side(df: pd.DataFrame, horizon: str, top_n: int):
    col = f"score_{horizon}"
    ranked = df.sort_values(col, ascending=False)
    buys = ranked.head(top_n)
    sells = ranked.tail(top_n).iloc[::-1]
    return buys, sells


def _bucket_side(df: pd.DataFrame, horizon: str, bucket: str, n: int = 8):
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
        "rebound": bool(r.get("rebound", False)),
        "market_cap_m": r.get("market_cap_m"),
        "avg_vol_k": r.get("avg_vol_k"),
    }


def write_report(df: pd.DataFrame, meta: dict, top_n: int) -> None:
    date = meta["date"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DAILY.mkdir(parents=True, exist_ok=True)

    cols_keep = [
        "Ticker", "sector", "industry", "size",
        "market_cap_m", "avg_vol_k", "liquid", "rebound", "at_low",
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
        f"- **Universe (after liquidity):** {meta['n_universe']}",
        f"- **Gates:** mcap ≥ ${meta.get('min_market_cap_m', 80)}M, avg vol ≥ {meta.get('min_avg_vol_k', 500)}k",
        f"- **Rebound floor tags:** {meta.get('n_rebound', 0)}",
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

        L += ["", f"### {h} — BUY by size bucket", ""]
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
        "- Universe gated: Market Cap ≥ $80M and Average Volume ≥ 500k shares (Finviz units).",
        "- `rebound_floor` = checklist own-history score low + green-body bias (sparse; soft boost only).",
        "- Raw checklist total score is NOT used as a buy rank (failed forward IC).",
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
