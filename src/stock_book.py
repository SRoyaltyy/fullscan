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
    #           join  sector  general  news    ab     peer
    "1d":      (0.12, 0.10,   0.08,    0.25,   0.25,  0.20),
    "3d":      (0.16, 0.14,   0.08,    0.16,   0.26,  0.20),
    "1w":      (0.18, 0.16,   0.08,    0.10,   0.28,  0.20),
    "2w":      (0.20, 0.18,   0.08,    0.06,   0.28,  0.20),
    "1m":      (0.22, 0.20,   0.08,    0.00,   0.30,  0.20),
}

# Tradeable universe gates (Finviz export units)
# Market Cap column is in *millions* USD → 80 == $80M
# Average Volume in export is in *thousands* of shares → 500 == 500k shares
MIN_MARKET_CAP_M = 80.0
MIN_AVG_VOL_K = 500.0
REBOUND_WINDOW = 40
REBOUND_BOOST = 0.08  # smaller than AB/peer so it cannot clone a sector
MAX_PER_INDUSTRY = 3
MAX_PER_SECTOR = 4
PERSIST_PENALTY = 0.10  # already-on-yesterday-book without fresh evidence


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
        print(f"[stock-book] WARN: no weather for {date} — join already baked stances")
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
    conf = max(0.15, min(1.0, float(conf or 0.5)))
    d = str(direction or "").lower()
    if d == "up":
        return conf
    if d == "down":
        return -conf
    return 0.0


def _runs_for_date(asof: str) -> dict[str, dict]:
    """Same-calendar-day scoreboard runs only. Stale sector/general = unused."""
    board = scoreboard.load()
    latest: dict[str, dict] = {}
    for r in board.get("runs", []):
        if r.get("date") != asof:
            continue
        t = r.get("topic") or ""
        if not r.get("predicted_direction"):
            continue
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


def _load_ab_enriched(date: str) -> pd.DataFrame:
    paths = [
        ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist_enriched.csv",
        ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist.csv",
    ]
    for p in paths:
        if p.exists():
            df = pd.read_csv(p, low_memory=False)
            tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
            df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
            score_col = next(
                (c for c in ("score_enriched", "score", "checklist_score") if c in df.columns),
                None,
            )
            if not score_col:
                continue
            out = df[["Ticker"]].copy()
            out["ab_raw"] = pd.to_numeric(df[score_col], errors="coerce")
            for c in ("score_context", "P01_peer_lead_week", "P02_peers_advancing",
                      "P03_industry_advancing", "P04_sector_supportive", "context_label"):
                if c in df.columns:
                    out[c] = df[c]
            print(f"[stock-book] AB loaded {p.name} rows={len(out):,} col={score_col}")
            return out
    print(f"[stock-book] WARN: no AB checklist for {date}")
    return pd.DataFrame(columns=["Ticker", "ab_raw"])


def _load_peer_rs(date: str) -> pd.DataFrame:
    p = ROOT / "data" / "peers" / f"{date}_peer_rs.csv"
    if not p.exists():
        print(f"[stock-book] WARN: no peer_rs for {date}")
        return pd.DataFrame(columns=["Ticker", "rs_week", "beat_week_pct"])
    df = pd.read_csv(p, low_memory=False)
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    keep = [c for c in ("Ticker", "rs_week", "rs_month", "beat_week_pct") if c in df.columns]
    return df[keep].drop_duplicates("Ticker")


def _prev_book_buys(date: str) -> dict[str, set[str]]:
    """Yesterday's (or previous file's) buy lists, per horizon — used to break clones."""
    files = sorted((ROOT / "data" / "stock_book").glob("????-??-??_stock_book.json"))
    prev = None
    for p in files:
        d = p.name[:10]
        if d < date:
            prev = p
    if prev is None:
        return {}
    try:
        data = json.loads(prev.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out: dict[str, set[str]] = {}
    for h, entry in (data.get("books") or {}).items():
        out[h] = {str(r.get("ticker", "")).upper() for r in (entry.get("buy") or [])}
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
        extra = [c for c in ("beta", "short", "mom", "profit", "size", "industry")
                 if c in memb.columns and c not in join.columns]
        if extra:
            join = join.merge(memb[["Ticker"] + extra], on="Ticker", how="left")

    weather = _load_weather(date)
    news = _load_news_actions(date)
    try:
        from .judge_apply import load_or_parse
        jt = (load_or_parse(date).get("tickers") or {})
        for t, net in jt.items():
            rec = news.setdefault(t.upper(), {"net": 0.0, "events": []})
            rec["net"] = float(rec.get("net") or 0) + float(net)
            rec.setdefault("events", []).append({"event": "news_judge", "weight": net})
        if jt:
            print(f"[stock-book] elevated {len(jt)} news-judge tickers into s_news")
    except Exception as e:
        print(f"[stock-book] judge overlay skipped: {e}")
    ab = _load_ab_enriched(date)
    peer = _load_peer_rs(date)
    same_day_runs = _runs_for_date(date)
    gates, gate_stats = _accuracy_gates()
    prev_buys = _prev_book_buys(date)

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

    if len(ab):
        join = join.merge(ab, on="Ticker", how="left")
        raw = pd.to_numeric(join.get("ab_raw"), errors="coerce").fillna(0.0)
        join["s_ab"] = np.tanh(raw / 8.0)
    else:
        join["s_ab"] = 0.0

    if len(peer):
        join = join.merge(peer, on="Ticker", how="left")
        rs = pd.to_numeric(join.get("rs_week"), errors="coerce").fillna(0.0)
        join["s_peer"] = np.tanh(rs / 8.0)
    else:
        join["s_peer"] = 0.0

    if "veto" in join.columns:
        veto = join["veto"].astype(str).str.lower().isin(["true", "1", "yes"])
        join.loc[veto, "s_join"] = join.loc[veto, "s_join"] * 0.2

    gen_run = same_day_runs.get("general")
    if not gen_run:
        print(f"[stock-book] WARN: no same-day general predict for {date} — s_general=0")
    gen_gate = gates.get("general", 1.0)
    sector_runs = {t.split(":", 1)[1]: r for t, r in same_day_runs.items() if t.startswith("sector:")}
    if not sector_runs:
        print(f"[stock-book] WARN: no same-day sector predicts for {date} — s_sector=0")
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
        "n_ab": int(join["s_ab"].ne(0).sum()) if "s_ab" in join.columns else 0,
        "n_peer": int(join["s_peer"].ne(0).sum()) if "s_peer" in join.columns else 0,
        "same_day_general": bool(gen_run),
        "same_day_sectors": len(sector_runs),
        "weather_risk": (weather.get("signals") or {}).get("risk")
        if isinstance(weather.get("signals"), dict)
        else weather.get("risk"),
        "n_universe": int(len(join)),
        "weights": WEIGHTS,
        "horizons": list(HORIZONS),
        "top_n": top_n,
    }

    fresh = (join["s_news"].abs() > 0.15) | (join["s_ab"] > 0.20) | (join["s_peer"] > 0.20)
    for h in HORIZONS:
        wj, ws, wg, wn, wa, wp = WEIGHTS[h]
        join[f"score_{h}"] = (
            wj * join["s_join"]
            + ws * join[f"s_sector_{h}"]
            + wg * join[f"s_general_{h}"]
            + wn * join["s_news"]
            + wa * join["s_ab"]
            + wp * join["s_peer"]
        )
        if "rebound" in join.columns:
            join.loc[join["rebound"], f"score_{h}"] = (
                join.loc[join["rebound"], f"score_{h}"] + REBOUND_BOOST
            )
        held = prev_buys.get(h, set())
        if held:
            mask = join["Ticker"].isin(held) & ~fresh
            join.loc[mask, f"score_{h}"] = join.loc[mask, f"score_{h}"] - PERSIST_PENALTY

    def reasons(row):
        bits = []
        if abs(row["s_join"]) > 0.12:
            bits.append(f"join={row['s_join']:+.2f}")
        if abs(row.get("s_sector", 0) or 0) > 0.1:
            bits.append(f"sector1d={row['s_sector']:+.2f}")
        if abs(row.get("s_general", 0) or 0) > 0.05:
            bits.append(f"gen1d={row['s_general']:+.2f}")
        if abs(row["s_news"]) > 0.05:
            bits.append(f"news={row['s_news']:+.2f}")
            ev = news.get(str(row["Ticker"]).upper(), {}).get("events") or []
            keys = []
            for e in ev[:2]:
                if isinstance(e, dict):
                    keys.append(str(e.get("event") or ""))
                else:
                    keys.append(str(e))
            if keys:
                bits.append("ev=" + ",".join(k for k in keys if k))
        if abs(row.get("s_ab", 0) or 0) > 0.05:
            bits.append(f"ab={row['s_ab']:+.2f}")
            lab = row.get("context_label")
            if isinstance(lab, str) and lab and lab != "—":
                bits.append(lab)
        if abs(row.get("s_peer", 0) or 0) > 0.05:
            bits.append(f"peer={row['s_peer']:+.2f}")
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
    picks = []
    sec_n: dict[str, int] = {}
    ind_n: dict[str, int] = {}
    for _, r in ranked.iterrows():
        sec = str(r.get("sector") or "")
        ind = str(r.get("industry") or "")
        if sec and sec_n.get(sec, 0) >= MAX_PER_SECTOR:
            continue
        if ind and ind not in ("", "nan", "None") and ind_n.get(ind, 0) >= MAX_PER_INDUSTRY:
            continue
        picks.append(r)
        if sec:
            sec_n[sec] = sec_n.get(sec, 0) + 1
        if ind and ind not in ("", "nan", "None"):
            ind_n[ind] = ind_n.get(ind, 0) + 1
        if len(picks) >= top_n:
            break
    buys = pd.DataFrame(picks) if picks else ranked.head(0)
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
        "s_join", "s_sector", "s_general", "s_news", "s_ab", "s_peer",
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
        "Layers: join (labels×weather) + same-day sector/general + news "
        "+ AB checklist + peer RS. Max 4 names/sector, 3/industry. "
        "Names already on yesterday's book are penalized unless AB/peer/news is fresh.",
        "",
        "## Regime snapshot",
        "",
        f"- **General bias (same-day):** {meta['general_bias']:+.2f} "
        f"({'yes' if meta.get('same_day_general') else 'MISSING — treated as 0'})",
        f"- **Sector predicts this date:** {meta.get('same_day_sectors', 0)}/11",
        f"- **Weather risk:** {meta.get('weather_risk')}",
        f"- **News tickers:** {meta['n_news_tickers']}",
        f"- **AB names:** {meta.get('n_ab', 0)} · **peer RS names:** {meta.get('n_peer', 0)}",
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
        "| Horizon | join | sector | general | news | AB | peer |",
        "|---------|------|--------|---------|------|----|------|",
    ]
    for h in HORIZONS:
        w = WEIGHTS[h]
        L.append(f"| {h} | {w[0]:.2f} | {w[1]:.2f} | {w[2]:.2f} | {w[3]:.2f} | {w[4]:.2f} | {w[5]:.2f} |")

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
        "- **1d** news + AB + peer; **1m** AB + peer + join + same-day sector.",
        "- Universe gated: Market Cap ≥ $80M and Average Volume ≥ 500k shares (Finviz units).",
        "- AB score (checklist + P01–P04 peer/industry/sector context) is a first-class rank, not a footnote.",
        "- Peer RS (`rs_week` vs correlated basket) breaks ties inside a sector so the book is not 8 clones of XLE.",
        "- Diversify: max 4 names per sector, 3 per industry. Persistence penalty if already on yesterday's list without fresh evidence.",
        "- Same-day sector/general only — stale Monday calls are not reused on Wednesday.",
        "- `rebound_floor` is a small boost from today's ticker checklist (tape at own-history low).",
        "- Predictor bias is scaled by graded 1d hit rate. Weak topics move scores less.",
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
