"""Unified multi-horizon stock suggestion book.

Horizons: 1d, 3d, 1w, 2w, 1m
Layers: join (labels×weather) + sector bias + general regime + news actions

CLI: python -m src.stock_book [--date YYYY-MM-DD] [--top 25] [--as-of]
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

from . import config, green_pile, scoreboard
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
JOIN_DIR = ROOT / "data" / "join"
UNIVERSE_DIR = ROOT / "data" / "universe"
WEATHER_DIR = ROOT / "01_daily" / "weather"
NEWS_DIR = ROOT / "01_daily" / "news"
OUT_DIR = ROOT / "data" / "stock_book"
DAILY = ROOT / "01_daily"

HORIZONS = ("1d", "3d", "1w", "2w", "1m")

# Code defaults. The live run prefers 00_grounding/book_policy.json, which
# book_learn tunes from realized forward returns (bounded to ±MAX_POLICY_DRIFT
# of these values). These constants stay the anchor and the fallback.
WEIGHTS = {
    #           join  sector  general  news    ab     peer
    "1d":      (0.12, 0.10,   0.08,    0.25,   0.25,  0.20),
    "3d":      (0.16, 0.14,   0.08,    0.16,   0.26,  0.20),
    "1w":      (0.18, 0.16,   0.08,    0.10,   0.28,  0.20),
    "2w":      (0.20, 0.18,   0.08,    0.06,   0.28,  0.20),
    "1m":      (0.22, 0.20,   0.08,    0.00,   0.30,  0.20),
}
SIGNAL_FAMILIES = ("join", "sector", "general", "news", "ab", "peer")
POLICY_PATH = ROOT / "00_grounding" / "book_policy.json"
MAX_POLICY_DRIFT = 0.12   # per-weight bound vs code default
MAX_POLICY_WEIGHT = 0.50


def load_policy() -> tuple[dict[str, tuple[float, ...]], dict]:
    """Learned weights from book_policy.json, validated against defaults.

    Any malformed/out-of-bounds policy falls back to code defaults — the
    learner can only ever nudge weights inside a bounded box, never break
    the ranker.
    """
    meta = {"weights_source": "defaults", "policy_version": None,
            "sell_excludes_addons": True, "heat_scale": 0.25}
    if not POLICY_PATH.exists():
        return dict(WEIGHTS), meta
    try:
        pol = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"[stock-book] WARN: unreadable book_policy.json ({e}) — defaults")
        return dict(WEIGHTS), meta
    weights: dict[str, tuple[float, ...]] = {}
    pw = pol.get("weights") or {}
    for h in HORIZONS:
        cand = pw.get(h)
        base = WEIGHTS[h]
        if not isinstance(cand, (list, tuple)) or len(cand) != len(base):
            print(f"[stock-book] WARN: policy weights malformed for {h} — defaults")
            return dict(WEIGHTS), meta
        try:
            cand = tuple(float(x) for x in cand)
        except (TypeError, ValueError):
            print(f"[stock-book] WARN: policy weights non-numeric for {h} — defaults")
            return dict(WEIGHTS), meta
        for c, b in zip(cand, base):
            if c < 0 or c > MAX_POLICY_WEIGHT or abs(c - b) > MAX_POLICY_DRIFT + 1e-9:
                print(f"[stock-book] WARN: policy weight out of bounds for {h} — defaults")
                return dict(WEIGHTS), meta
        if not 0.85 * sum(base) <= sum(cand) <= 1.15 * sum(base):
            print(f"[stock-book] WARN: policy weight sum off for {h} — defaults")
            return dict(WEIGHTS), meta
        weights[h] = cand
    meta["weights_source"] = "book_policy.json"
    meta["policy_version"] = pol.get("version")
    meta["sell_excludes_addons"] = bool(pol.get("sell_excludes_addons", True))
    try:
        meta["heat_scale"] = min(
            1.5, max(0.0, float(pol.get("heat_scale", 0.25)))
        )
    except (TypeError, ValueError):
        meta["heat_scale"] = 0.25
    return weights, meta


def effective_weights(
    weights: dict[str, tuple[float, ...]], present: dict[str, bool]
) -> tuple[dict[str, tuple[float, ...]], list[str]]:
    """Renormalize each horizon's weights over the signal families that are
    actually present today.

    Previously a missing input (e.g. no AB file) silently scored 0 while
    keeping its 0.25–0.30 weight, compressing every total until the additive
    opportunity tilt dominated the rank. Now absent families give their
    weight back to the present ones, proportionally, and the shift is
    recorded in the meta.
    """
    absent = [f for f in SIGNAL_FAMILIES if not present.get(f, True)]
    if not absent:
        return dict(weights), []
    out: dict[str, tuple[float, ...]] = {}
    for h, w in weights.items():
        total = sum(w)
        kept = [x if present.get(f, True) else 0.0 for f, x in zip(SIGNAL_FAMILIES, w)]
        kept_sum = sum(kept)
        if kept_sum <= 0:
            out[h] = tuple(w)
            continue
        out[h] = tuple(round(x * total / kept_sum, 4) for x in kept)
    return out, absent

# Tradeable universe gates (Finviz export units)
# Market Cap column is in *millions* USD → 80 == $80M
# Average Volume in export is in *thousands* of shares → 500 == 500k shares
MIN_MARKET_CAP_M = 80.0
MIN_AVG_VOL_K = 500.0
MIN_ATR_PCT = tl.MIN_ATR_PCT
REBOUND_WINDOW = 40
REBOUND_BOOST = 0.08  # smaller than AB/peer so it cannot clone a sector
MAX_PER_INDUSTRY = 3
MAX_PER_SECTOR = 4
MAX_LARGE_MEGA = 4          # rest of the book is small/mid
MIN_OPP_MCAP_M = 400.0      # skip sub-$400M "lottery" micros in BUY
MAX_OPP_MCAP_M = 20000.0    # above this is large/mega for the cap
PERSIST_PENALTY = 0.10
# mid_opp used to add +0.60–0.68 and buy Healthcare into a −0.50 sector call.
OPP_CAP = 0.20
HARD_SECTOR_RED = -0.25     # essay-red; not the ±0.09 event-tilt noise
MAX_EVENT_SECTOR_TILT = 0.20
# Liquid mid/small with room to run (BB-class). Additive, not a 7th weight.
SIZE_OPP = {"micro": 0.00, "small": 0.16, "mid": 0.32, "large": -0.05, "mega": -0.22}
RANGE_OPP = {
    "deep_low": 0.16, "low": 0.12, "mid": 0.08,
    "high": 0.0, "top": -0.12, "breakout": -0.06,
}


def _load_finviz_liquidity(date: str) -> pd.DataFrame:
    """Ticker liquidity + live price confirmation from the dated Finviz export."""
    export_dir = ROOT / "data" / "exports"
    path = export_dir / f"finviz_{date}.csv"
    if not path.exists():
        files = sorted(export_dir.glob("finviz_????-??-??.csv"))
        if not files:
            return pd.DataFrame(columns=[
                "Ticker", "market_cap_m", "avg_vol_k", "relvol",
                "change_pct", "gap_pct", "news_time", "atr_pct",
            ])
        path = files[-1]
    df = pd.read_csv(path, low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    out = pd.DataFrame()
    out["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    out["market_cap_m"] = pd.to_numeric(df.get("Market Cap"), errors="coerce")
    out["avg_vol_k"] = pd.to_numeric(df.get("Average Volume"), errors="coerce")
    rel_col = next(
        (c for c in ("Relative Volume", "Rel Volume", "Rel Vol", "RelVol", "relvol")
         if c in df.columns),
        None,
    )
    out["relvol"] = pd.to_numeric(df[rel_col], errors="coerce") if rel_col else np.nan
    for source, target in (("Change", "change_pct"), ("Gap", "gap_pct")):
        if source in df.columns:
            raw = df[source].astype(str).str.replace("%", "", regex=False)
            out[target] = pd.to_numeric(raw, errors="coerce")
        else:
            out[target] = np.nan
    out["news_time"] = (
        df["News Time"].astype(str)
        if "News Time" in df.columns else ""
    )
    atr = pd.to_numeric(df.get("Average True Range"), errors="coerce")
    px = pd.to_numeric(df.get("Price"), errors="coerce")
    out["atr_pct"] = np.where(px > 0, 100.0 * atr / px, np.nan)
    return out.drop_duplicates("Ticker", keep="first")


def _keep_liquid(join: pd.DataFrame) -> pd.DataFrame:
    """Drop illiquid names. If the filter wipes everyone, keep the universe.

    Missing Finviz mcap/vol/ATR used to become 0 and empty the book, so
    green.json never wrote.
    """
    if join is None or join.empty:
        return join
    n_before = len(join)
    if "atr_pct" not in join.columns:
        join = join.copy()
        join["atr_pct"] = np.nan
    liquid = (
        (join["market_cap_m"].fillna(0) >= MIN_MARKET_CAP_M)
        & (join["avg_vol_k"].fillna(0) >= MIN_AVG_VOL_K)
        & (join["atr_pct"].fillna(0) >= MIN_ATR_PCT)
    )
    join = join.copy()
    join["liquid"] = liquid
    kept = join.loc[liquid].copy()
    print(
        f"[stock-book] liquidity filter mcap>={MIN_MARKET_CAP_M}M "
        f"vol>={MIN_AVG_VOL_K}k ATR%>={MIN_ATR_PCT}: "
        f"{n_before} → {len(kept)}"
    )
    if n_before and not len(kept):
        print("[stock-book] WARN: liquidity filter emptied the universe — "
              "ranking unfiltered so green.json can still land")
        return join
    return kept


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


def _digest_polarity(text: str) -> float:
    t = (text or "").lower()
    pos = len(re.findall(
        r"\b(beat|beats|upgrade|upgrades|raises|surge|surges|record|climbs|guides? above|buyback)\b", t))
    neg = len(re.findall(
        r"\b(miss|misses|downgrade|downgrades|fall|falls|plunge|selloff|cut|cuts|lowers|bankruptcy)\b", t))
    if pos == neg:
        return 0.0
    return 1.6 if pos > neg else -1.6


def _load_finviz_digest(date: str) -> dict[str, dict]:
    """Per-ticker Daily Digest from Elite export — company news the judge may have skipped."""
    path = NEWS_DIR / f"{date}_finviz_digest.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out: dict[str, dict] = {}
    rows = []
    for key in ("top_signal", "all_ticker_digests_sample"):
        rows.extend(data.get(key) or [])
    for sec_rows in (data.get("by_sector") or {}).values():
        rows.extend(sec_rows or [])
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        t = str(row.get("ticker") or "").strip().upper()
        if not t or t in seen or t in ("SPY", "QQQ", "DIA", "IWM"):
            continue
        if row.get("is_dividend"):
            continue
        digest = row.get("digest") or row.get("news_title") or ""
        pol = _digest_polarity(digest)
        if not pol:
            continue
        seen.add(t)
        out[t] = {
            "net": pol,
            "events": [{"event": "finviz_digest", "digest": str(digest)[:160]}],
            "source": "digest",
        }
    if out:
        print(f"[stock-book] finviz digest elevated {len(out)} tickers")
    return out


def _merge_news(*books: dict[str, dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for book in books:
        for t, rec in (book or {}).items():
            cur = out.setdefault(t, {"ticker": t, "net": 0.0, "events": []})
            cur["net"] = float(cur.get("net") or 0) + float(rec.get("net") or 0)
            for e in rec.get("events") or []:
                cur.setdefault("events", []).append(e)
    return out


def _load_events_sector_tilt(date: str) -> dict[str, float]:
    path = ROOT / "01_daily" / "events" / "latest.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    sd = data.get("scan_date")
    if sd:
        try:
            delta = abs((datetime.fromisoformat(sd) - datetime.fromisoformat(date)).days)
        except ValueError:
            delta = 99
        if delta > 3:
            return {}
    tilt: dict[str, float] = {}
    for e in data.get("events") or []:
        impact = float(e.get("impact") or 0)
        if impact < 3:
            continue
        direc = str(e.get("expected_direction") or "").lower()
        sign = 1.0 if direc.startswith(("bull", "pos")) else -1.0 if direc.startswith(("bear", "neg")) else 0.0
        if not sign:
            continue
        for sec in e.get("sectors") or []:
            if str(sec).upper() in ("BROAD", "SPX", "ALL"):
                continue
            tilt[str(sec)] = tilt.get(str(sec), 0.0) + sign * min(impact, 5) * 0.08
    return _clip_event_tilt(tilt)


def _stand_down_status(date: str, meta: dict) -> dict:
    """Empty the BUY book on a clearly no-win open.

    Fires when the same-day general is down, weather is risk-off, the
    signed bias is ≤ −0.25, and catalyst daily produced zero usable
    company dossiers. If a dossier *did* land, BUY is only those tickers.
    """
    direction = str(meta.get("general_direction") or "").lower()
    risk = str(meta.get("weather_risk") or "")
    try:
        bias = float(meta.get("general_bias") or 0)
    except (TypeError, ValueError):
        bias = 0.0
    cat_tickers: list[str] = []
    n_cat = 0
    try:
        from .catalyst_daily import load_dossiers, usable_dossier
        for row in load_dossiers(date):
            if not usable_dossier(row):
                continue
            n_cat += 1
            t = str(row.get("ticker") or "").strip().upper()
            if t:
                cat_tickers.append(t)
    except Exception:
        pass
    fire = (
        bool(meta.get("same_day_general"))
        and direction == "down"
        and risk == "off"
        and bias <= -0.25
        and n_cat == 0
    )
    if fire:
        reason = (
            f"general {direction} bias={bias:+.2f} risk={risk} "
            f"and 0 usable company dossiers — no BUY"
        )
    elif direction == "down" and n_cat:
        reason = (
            f"general {direction} but {n_cat} usable dossiers — "
            "BUY is those names only"
        )
        fire = False
        restrict = True
    else:
        reason = "open is tradeable"
        restrict = False
    return {
        "stand_down": fire,
        "restrict_to_catalysts": (not fire) and direction == "down" and n_cat > 0,
        "reason": reason,
        "catalyst_tickers": cat_tickers,
        "n_usable_catalysts": n_cat,
        "general_direction": direction,
        "weather_risk": risk,
        "general_bias": bias,
    }


def _clip_event_tilt(
    tilt: dict[str, float], cap: float = MAX_EVENT_SECTOR_TILT
) -> dict[str, float]:
    """Keep the event overlay from inverting a same-day sector essay.

    2026-08-31: Energy essay +0.47 + event −0.56 → −0.09 (pile veto);
    Technology essay −0.28 + event +1.04 → +0.77 (fake cluster).
    """
    out: dict[str, float] = {}
    for sec, raw in (tilt or {}).items():
        try:
            v = float(raw)
        except (TypeError, ValueError):
            continue
        out[str(sec)] = max(-cap, min(cap, v))
    return out


def _inputs_status(date: str) -> list[dict]:
    def exists(*parts):
        return ROOT.joinpath(*parts).exists()

    rows = [
        ("Finviz Elite export", exists("data", "exports", f"finviz_{date}.csv"), "liquidity + labels + AB proxy + digest"),
        ("Labels / membership", exists("data", "universe", f"{date}_membership.csv"), "join + mid_opp + earnings/range"),
        ("Weather (tape + FRED/DXY/VIX)", exists("01_daily", "weather", f"{date}_weather.json"), "join × weather"),
        ("Channel 1 raw", exists("01_daily", "_channel1", f"{date}_predict.json"), "via weather"),
        ("Join ranked universe", exists("data", "join", f"{date}_ranked.csv"), "s_join"),
        ("News parse + actions", exists("01_daily", "news", f"{date}_actions.json"), "s_news"),
        ("News judge", exists("01_daily", "news", f"{date}_judge.json") or exists("01_daily", "news", f"{date}_judge.md"), "s_news ticker tilts"),
        ("Finviz daily digest", exists("01_daily", "news", f"{date}_finviz_digest.json"), "s_news company headlines"),
        ("General predict", exists("01_daily", "general", f"{date}_predict.md"), "s_general × beta"),
        ("Sector LLM essays", exists("01_daily", "sectors", date, "_board.json"), "s_sector (0 if essays missing)"),
        ("AB checklist + P01–P04", exists("data", "ab_checklist", f"{date}_ab_checklist_enriched.csv"), "s_ab"),
        ("Peer RS", exists("data", "peers", f"{date}_peer_rs.csv"), "s_peer"),
        ("Ticker checklist (rebound)",
         exists("data", "checklist", f"{date}_checklist.csv")
         or any((ROOT / "data" / "checklist").glob("*_checklist.csv")),
         "rebound_floor (dated file, else latest — can be stale)"),
        ("Event scanner", exists("01_daily", "events", "latest.json"), "sector tilt + weather"),
        ("Finviz map heat (industry RS / themes)",
         exists("01_daily", "map_heat", f"{date}_map_heat.json"),
         "industry residual + theme tape → s_heat when research is gone"),
        ("Map heat captain research", exists("01_daily", "map_heat", f"{date}_research.json"),
         "Grok captain essays (strict morning_refresh; else Finviz tape)"),
        ("Catalyst overlays", False, "not in ranker — separate chart workflow"),
        ("Insider / politician flow", False, "no daily file in repo"),
        ("Industry predict", exists("01_daily", "industry"), "not scored (ad-hoc only)"),
        ("Learnings / mutable policy", exists("01_daily", f"{date}_learnings.md"), "next predict prompt, not a ticker score"),
    ]
    return [{"name": n, "found": bool(f), "used_as": u} for n, f, u in rows]


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
            # The lattice uses score_base as intrinsic setup evidence.  P01-P04
            # remain visible in s_ab, but no longer get counted again as peer /
            # group permission.
            if "score_base" in df.columns:
                out["ab_base_raw"] = pd.to_numeric(
                    df["score_base"], errors="coerce"
                )
            else:
                out["ab_base_raw"] = out["ab_raw"]
            for c in ("score_context", "P01_peer_lead_week", "P02_peers_advancing",
                      "P03_industry_advancing", "P04_sector_supportive", "context_label"):
                if c in df.columns:
                    out[c] = df[c]
            # duplicate tickers here would explode rows in the later merge
            n_dup = int(out["Ticker"].duplicated().sum())
            if n_dup:
                print(f"[stock-book] WARN: AB file had {n_dup} duplicate tickers — deduped")
                out = out.drop_duplicates("Ticker", keep="first")
            print(f"[stock-book] AB loaded {p.name} rows={len(out):,} col={score_col}")
            return out
    print(f"[stock-book] WARN: no AB checklist for {date}")
    return pd.DataFrame(columns=["Ticker", "ab_raw", "ab_base_raw"])


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


def build(date: str | None = None, top_n: int = 25,
          as_of: bool = False) -> tuple[pd.DataFrame, dict]:
    join, date = _load_join(date)

    # Pre-flight integrity check — never fatal, but recorded and used to
    # renormalize weights when whole signal families are absent.
    health = None
    try:
        from . import input_health
        # Always re-scan. A same-day snapshot from a earlier failed
        # attempt (AB/weather missing) must not hide layers that just landed.
        health = input_health.check(date)
        print(input_health.render(health))
    except Exception as e:  # noqa: BLE001 — health must never kill the book
        print(f"[stock-book] WARN: input health check failed: {e}")
    join = join.drop_duplicates(subset=["Ticker"], keep="first").copy()
    join["Ticker"] = join["Ticker"].astype(str).str.strip().str.upper()

    # Liquidity / size gates (user: avg vol >500k shares, mcap >$80M)
    liq = _load_finviz_liquidity(date)
    if len(liq):
        join = join.merge(liq, on="Ticker", how="left")
    else:
        join["market_cap_m"] = np.nan
        join["avg_vol_k"] = np.nan
        join["relvol"] = np.nan
    if "relvol" not in join.columns:
        join["relvol"] = np.nan
    join = _keep_liquid(join)

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
    news = _merge_news(
        _load_news_actions(date),
        _load_finviz_digest(date),
    )
    # news_actions already persists News Judge events.  Apply the judge here
    # only for older packets that predate that integration; otherwise the
    # same adjudicated headline was counted twice.
    try:
        from .judge_apply import load_or_parse
        judge_already_applied = any(
            isinstance(event, dict) and event.get("event") == "news_judge"
            for rec in news.values()
            for event in (rec.get("events") or [])
        )
        jt = (load_or_parse(date).get("tickers") or {})
        if not judge_already_applied:
            for t, net in jt.items():
                rec = news.setdefault(t.upper(), {"net": 0.0, "events": []})
                rec["net"] = float(rec.get("net") or 0) + float(net)
                rec.setdefault("events", []).append(
                    {"event": "news_judge", "weight": net}
                )
            if jt:
                print(
                    f"[stock-book] applied {len(jt)} News Judge tickers once"
                )
        elif jt:
            print(
                f"[stock-book] News Judge already in actions "
                f"({len(jt)} tickers) — not double-counted"
            )
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
        base_raw = pd.to_numeric(
            join.get("ab_base_raw"), errors="coerce"
        ).fillna(raw)
        join["s_ab_intrinsic"] = np.tanh(base_raw / 8.0)
    else:
        join["s_ab"] = 0.0
        join["s_ab_intrinsic"] = 0.0

    if len(peer):
        join = join.merge(peer, on="Ticker", how="left")
        rs = pd.to_numeric(join.get("rs_week"), errors="coerce").fillna(0.0)
        join["s_peer"] = np.tanh(rs / 8.0)
    else:
        join["s_peer"] = 0.0

    # Load policy before optional add-ons: heat has its own learned scalar,
    # while the original six-family tuple stays backward-compatible.
    base_weights, policy_meta = load_policy()

    # Nested industry heat: captains + OVERRIDE child vs parent. Additive.
    tboost, iboost = {}, {}
    try:
        from .map_heat_research import ticker_boosts
        tboost, iboost = ticker_boosts(date)
    except Exception as e:  # noqa: BLE001
        print(f"[stock-book] heat research skipped: {e}")
    if tboost or iboost:
        def _heat_row(r):
            t = str(r.get("Ticker") or "").upper()
            if t in tboost:
                return tboost[t]
            ind = str(r.get("industry") or "")
            return float(iboost.get(ind) or 0.0)
        join["s_heat_raw"] = join.apply(_heat_row, axis=1).astype(float)
        heat_scale = float(policy_meta.get("heat_scale", 1.0))
        join["s_heat"] = join["s_heat_raw"] * heat_scale
        print(f"[stock-book] s_heat on {int(join['s_heat'].ne(0).sum())} names "
              f"({len(tboost)} captains, {len(iboost)} industries; "
              f"learned scale={heat_scale:.2f})")
    else:
        join["s_heat_raw"] = 0.0
        join["s_heat"] = 0.0

    heat_source = "none"
    if tboost or iboost:
        rjs = ROOT / "01_daily" / "map_heat" / f"{date}_research.json"
        used_research = False
        if rjs.exists():
            try:
                rd = json.loads(rjs.read_text(encoding="utf-8"))
                used_research = (
                    rd.get("phase") == "morning_refresh"
                    and len(rd.get("cards") or []) >= 20
                    and not rd.get("evidence_errors")
                )
            except (OSError, json.JSONDecodeError, TypeError):
                used_research = False
        heat_source = "captain_research" if used_research else "finviz_tape"

    # --- opportunity: liquid mid/small with room to run (BB-class) ---
    sz = join["size"].astype(str).str.lower() if "size" in join.columns else pd.Series("", index=join.index)
    rng = join["range"].astype(str).str.lower() if "range" in join.columns else pd.Series("", index=join.index)
    ext = join["ext"].astype(str).str.lower() if "ext" in join.columns else pd.Series("", index=join.index)
    surp = join["earnsurp"].astype(str).str.lower() if "earnsurp" in join.columns else pd.Series("", index=join.index)
    join["s_opp"] = (
        sz.map(SIZE_OPP).fillna(0.0) + rng.map(RANGE_OPP).fillna(0.0)
    )
    midish = sz.isin(["small", "mid"])
    join.loc[midish & ext.isin(["washed", "neutral", ""]), "s_opp"] += 0.08
    join.loc[midish & surp.isin(["beat", "big_beat"]), "s_opp"] += 0.12
    # A sector-weather stamp (e.g. ADBE → all Tech hostile) must not bury a
    # mid-cap that is beating peers / just beat earnings.
    spec = midish & (
        (join["s_ab"] > 0.10) | (join["s_peer"] > 0.20) | surp.isin(["beat", "big_beat"])
    )
    clipped = spec & (join["s_join"] < -0.15)
    if clipped.any():
        print(f"[stock-book] clipped sector-join nuke on {int(clipped.sum())} mid/small names with stock-specific evidence")
        join.loc[clipped, "s_join"] = -0.15

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
    # Preserve the essay-only verdict before event-scanner context is added.
    # The parent evaluator can then show a real tape-vs-essay conflict.
    join["s_sector_essay"] = join["s_sector_1d"]

    ev_tilt = _load_events_sector_tilt(date)
    if ev_tilt and "sector" in join.columns:
        extra = join["sector"].map(ev_tilt).fillna(0.0)
        for h in HORIZONS:
            join[f"s_sector_{h}"] = join[f"s_sector_{h}"] + extra
        join["s_sector"] = join["s_sector_1d"]
        print(f"[stock-book] event-scanner sector tilt on {len(ev_tilt)} sectors "
              f"(clipped ±{MAX_EVENT_SECTOR_TILT:.2f})")
    gen_bias = float(_bias_for(gen_run, "1d") * gen_gate)
    sector_bias = {sec: float(_bias_for(r, "1d") * gates.get(f"sector:{sec}", 1.0))
                   for sec, r in sector_runs.items()}

    # Build independent decision domains before the color/lookback layer:
    # market permission, parent sector, child industry/theme, company event,
    # intrinsic setup, and flow.  These colors do not alter the legacy core;
    # they decide which lane is allowed to use it.
    # Historical --as-of replays skip the lattice until the session it
    # actually shipped (2026-08-31).
    lattice_context = None
    use_lattice = True
    if as_of:
        from . import book_era
        use_lattice = book_era.live(date, "decision_lattice")
        if not use_lattice:
            print(f"[stock-book] as-of {date}: lattice not live — "
                  f"using {book_era.method_for(date)}")
    if use_lattice:
        try:
            from . import decision_lattice
            lattice_context = decision_lattice.build_context(
                date, general_run=gen_run, weather=weather,
            )
            join = decision_lattice.attach_domains(join, lattice_context)
            market = lattice_context.get("market") or {}
            print(f"[stock-book] decision lattice market: {market.get('rationale')}")
        except Exception as e:  # noqa: BLE001 — legacy ranker remains a fallback
            print(f"[stock-book] decision lattice evidence skipped: {e}")
            lattice_context = None

    # Opportunity is a BUY-side nudge, not a license to ignore a red sector.
    if "s_opp" in join.columns:
        join["s_opp_raw"] = join["s_opp"]
        hard_sec = pd.to_numeric(join["s_sector"], errors="coerce").fillna(0.0) <= HARD_SECTOR_RED
        n_zero = int(hard_sec.sum())
        join.loc[hard_sec, "s_opp"] = 0.0
        join["s_opp"] = pd.to_numeric(join["s_opp"], errors="coerce").fillna(0.0).clip(upper=OPP_CAP)
        print(f"[stock-book] s_opp capped at {OPP_CAP:.2f}; "
              f"zeroed on {n_zero} hard sector-red names")

    join = green_pile.attach_ranks(join)
    join["green"] = green_pile.green_mask(join)
    try:
        gp = green_pile.pile_status(join)
    except Exception as e:  # noqa: BLE001 — still write the book + a stub green.json
        print(f"[stock-book] WARN: pile_status failed: {e}")
        gp = {
            "n_pile": 0, "used": False, "buy_mode": "weighted_fallback",
            "sell_mode": "core_weights", "reason": f"pile_status error: {e}",
        }
    if as_of:
        from . import book_era
        if not book_era.live(date, "green_pile"):
            gp = dict(gp)
            gp["used"] = False
            gp["buy_mode"] = "weighted_as_of"
            gp["reason"] = (
                f"as-of {date}: green pile not live — weighted walk"
            )
    print(f"[stock-book] green-pile {gp['reason']}")

    # ---- resolve weights: learned policy (bounded) → renorm over present families ----
    present = {
        "join": True,  # build aborts earlier without a join file
        "sector": bool(sector_runs),
        "general": bool(gen_run),
        "news": bool(news),
        "ab": bool(len(ab)) and bool(join["s_ab"].ne(0).any()),
        "peer": bool(len(peer)) and bool(join["s_peer"].ne(0).any()),
    }
    weights_h, absent_families = effective_weights(base_weights, present)
    if absent_families:
        print(f"[stock-book] renormalized weights — absent families: {absent_families}")
    print(f"[stock-book] weights source: {policy_meta['weights_source']}"
          + (f" v{policy_meta['policy_version']}" if policy_meta.get("policy_version") else ""))

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
        "weights": {h: list(w) for h, w in weights_h.items()},
        "weights_source": policy_meta["weights_source"],
        "policy_version": policy_meta.get("policy_version"),
        "heat_scale": policy_meta.get("heat_scale", 1.0),
        "heat_source": heat_source,
        "n_heat_captains": len(tboost),
        "n_heat_industries": len(iboost),
        "sell_excludes_addons": policy_meta.get("sell_excludes_addons", True),
        "absent_families": absent_families,
        "input_health": {
            "worst": health.get("worst"),
            "family_status": health.get("family_status"),
            "learn_grade": health.get("learn_grade"),
        } if health else None,
        "horizons": list(HORIZONS),
        "top_n": top_n,
        "inputs": _inputs_status(date),
        "n_news_after_digest": int((join["s_news"].abs() > 0).sum()) if "s_news" in join.columns else 0,
        "event_sector_tilt": ev_tilt,
        "green_pile": gp,
        "n_pile": gp.get("n_pile"),
        "n_pile_liquid": gp.get("n_pile_liquid"),
        "pile_used": gp.get("used"),
        "green_min": gp.get("min", 8),
        "ranker": "green_pile" if gp.get("used") else "weighted",
        "as_of": bool(as_of),
        "general_direction": str((gen_run or {}).get("predicted_direction") or ""),
        "market_decision": (
            (lattice_context or {}).get("market")
            if lattice_context else None
        ),
    }
    # Final stand-down is known only after direct-company exceptions have
    # been tested against setup/flow and the lookback marks.
    meta["stand_down"] = {
        "stand_down": False,
        "restrict_to_catalysts": False,
        "reason": "decision lattice pending",
        "n_usable_catalysts": 0,
    }
    try:
        from .map_heat_research import calendar_entry_scale, earnings_entry_tickers
        meta["calendar_entry_scale"] = calendar_entry_scale(date)
        meta["earnings_entry_tickers"] = earnings_entry_tickers(date)
    except Exception:
        meta["calendar_entry_scale"] = 1.0
        meta["earnings_entry_tickers"] = []

    fresh = (join["s_news"].abs() > 0.15) | (join["s_ab"] > 0.20) | (join["s_peer"] > 0.20)
    for h in HORIZONS:
        wj, ws, wg, wn, wa, wp = weights_h[h]
        # core = the six weighted signals only. The SELL side ranks on this:
        # the opportunity add-on is a BUY-side tilt, and letting its negative
        # leg (mega-cap −0.22, 52w-top −0.12) leak into the sell rank filled
        # the sell book with structural shorts of strong mega-caps.
        join[f"core_{h}"] = (
            wj * join["s_join"]
            + ws * join[f"s_sector_{h}"]
            + wg * join[f"s_general_{h}"]
            + wn * join["s_news"]
            + wa * join["s_ab"]
            + wp * join["s_peer"]
            + join["s_heat"]
        )
        join[f"score_{h}"] = join[f"core_{h}"] + join["s_opp"]
        if "rebound" in join.columns:
            join.loc[join["rebound"], f"score_{h}"] = (
                join.loc[join["rebound"], f"score_{h}"] + REBOUND_BOOST
            )
        held = prev_buys.get(h, set())
        if held:
            mask = join["Ticker"].isin(held) & ~fresh
            join.loc[mask, f"score_{h}"] = join.loc[mask, f"score_{h}"] - PERSIST_PENALTY

    try:
        from . import book_marks
        join = book_marks.attach(join, date)
        join = book_marks.apply_blue_boost(join, HORIZONS)
        meta["lookback_marks"] = {
            "n_alarm": int(join["lb_alarm"].sum()) if "lb_alarm" in join.columns else 0,
            "n_fade": int(join["lb_fade"].sum()) if "lb_fade" in join.columns else 0,
            "n_blue": int(join["lb_blue"].sum()) if "lb_blue" in join.columns else 0,
            "n_white": int(join["lb_zero_red"].sum()) if "lb_zero_red" in join.columns else 0,
            "white_is_gate": False,
            "vetoes": ["alarm", "fade_setup", "cond=bad", "region=bad"],
        }
    except Exception as e:  # noqa: BLE001 — marks must never kill the book
        print(f"[stock-book] lookback marks skipped: {e}")

    if lattice_context is not None:
        try:
            from . import decision_lattice
            join = decision_lattice.finalize_decisions(
                join, date, lattice_context,
            )
            lattice_summary = decision_lattice.summarize(
                join, lattice_context, top_n=max(15, top_n),
            )
            meta["decision_lattice"] = lattice_summary
            meta["stand_down"] = lattice_summary.get("stand_down") or {}
            meta["ranker"] = "decision_lattice"
            if meta["stand_down"].get("stand_down"):
                print(
                    f"[stock-book] STAND DOWN — "
                    f"{meta['stand_down'].get('reason')}"
                )
            else:
                print(
                    f"[stock-book] decision lattice: "
                    f"{lattice_summary.get('n_bull_eligible', 0)} bull / "
                    f"{lattice_summary.get('n_bear_eligible', 0)} bear eligible"
                )
        except Exception as e:  # noqa: BLE001
            print(f"[stock-book] decision lattice routing skipped: {e}")
            meta["stand_down"] = _stand_down_status(date, meta)
    else:
        meta["stand_down"] = _stand_down_status(date, meta)
        if meta["stand_down"].get("stand_down"):
            meta["ranker"] = "stand_down"

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
        heat = float(row.get("s_heat") or 0)
        if abs(heat) > 0.04:
            bits.append(f"heat={heat:+.2f}")
        opp = float(row.get("s_opp") or 0)
        if opp > 0.05:
            bits.append(f"mid_opp={opp:+.2f}")
        if row.get("rebound"):
            bits.append("rebound_floor")
        if row.get("lb_alarm"):
            bits.append("🚨")
        if row.get("lb_blue"):
            bits.append("🔵")
        if row.get("lb_zero_red"):
            bits.append("⚪")
        cond = row.get("lb_cond")
        if isinstance(cond, str) and cond and cond not in ("missing", "nan"):
            bits.append(f"cond={cond}")
        tags = row.get("lb_tags")
        if isinstance(tags, str) and tags:
            bits.append(tags)
        lane = row.get("decision_lane")
        if isinstance(lane, str) and lane:
            bits.append(f"lane={lane}")
        blockers = row.get("decision_blockers")
        if isinstance(blockers, str) and blockers:
            bits.append(f"blocked={blockers}")
        return "; ".join(bits)

    join["reasons"] = join.apply(reasons, axis=1)
    meta["n_after_liquidity"] = int(len(join))
    meta["min_market_cap_m"] = MIN_MARKET_CAP_M
    meta["min_avg_vol_k"] = MIN_AVG_VOL_K
    meta["min_atr_pct"] = MIN_ATR_PCT
    meta["n_rebound"] = int(join["rebound"].sum()) if "rebound" in join.columns else 0
    return join, meta


def _buy_veto_mask(df: pd.DataFrame) -> pd.Series:
    """Hard quality gates on the BUY walk (pile + weighted fallback).

    Drops: essay-red sectors, LAG names that also lost their peer basket,
    and printed dead relative volume. Soft yellow (sector −0.09 event noise,
    modest red general) is not a veto here — the pile / weights handle that.
    """
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    veto = pd.Series(False, index=df.index)
    # Printed dead relvol is a tape fact. Lattice eligibility must not
    # put WAY/TXG-style (0, 0.7) names on 1d BUY (2026-09-04 live book).
    rel = None
    for c in ("relvol", "rel_vol", "Relative Volume"):
        if c in df.columns:
            rel = pd.to_numeric(df[c], errors="coerce")
            break
    if rel is not None:
        printed = rel.notna() & (rel > 0)
        veto |= printed & (rel < green_pile.RELVOL_DEAD)
    # In lattice mode permission has already been decided from all domains,
    # including lookback alarm/blue/white.  Do not re-apply the legacy sector
    # veto and accidentally kill a valid direct-company exception.
    if "bull_eligible" in df.columns:
        veto |= ~df["bull_eligible"].astype(bool)
        return veto
    if "s_sector" in df.columns:
        veto |= pd.to_numeric(df["s_sector"], errors="coerce").fillna(0.0) <= HARD_SECTOR_RED
    peer = (
        pd.to_numeric(df["s_peer"], errors="coerce").fillna(0.0)
        if "s_peer" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    lab = df["context_label"].astype(str) if "context_label" in df.columns else None
    reasons = df["reasons"].astype(str) if "reasons" in df.columns else None
    is_lag = pd.Series(False, index=df.index)
    if lab is not None:
        is_lag |= lab.str.contains(r"\bLAG\b", regex=True, na=False)
    if reasons is not None:
        is_lag |= reasons.str.contains(r"\bLAG\b", regex=True, na=False)
    veto |= is_lag & (peer <= 0)
    try:
        from . import book_marks
        veto |= book_marks.veto_mask(df)
    except Exception:
        pass
    return veto


def _rank_sells(df: pd.DataFrame, horizon: str, top_n: int,
                sell_core: bool = True, sell_mask=None,
                sell_sort: str | None = None) -> pd.DataFrame:
    """Worst names on the SELL score. Never empty just because BUY stood down.

    Policy may turn sell_excludes_addons off (live book_policy.json does).
    In that case rank on score_{h}; otherwise prefer core_{h}.
    """
    if df is None or not len(df):
        return df.head(0) if df is not None else pd.DataFrame()
    pool = df
    if sell_mask is not None:
        try:
            masked = df.loc[sell_mask]
            if masked is not None and len(masked):
                pool = masked
            else:
                return df.head(0)
        except Exception:
            pool = df
    if sell_sort and sell_sort in pool.columns:
        return pool.sort_values(sell_sort, ascending=False).head(top_n)
    core_col = f"core_{horizon}"
    score_col = f"score_{horizon}"
    if sell_core and core_col in pool.columns:
        return pool.sort_values(core_col, ascending=True).head(top_n)
    if score_col in pool.columns:
        return pool.sort_values(score_col, ascending=True).head(top_n)
    return pool.head(0)


def _book_side(df: pd.DataFrame, horizon: str, top_n: int, sell_core: bool = True,
              buy_mask=None, buy_sort=None, allow_empty=False,
              sell_mask=None, sell_sort=None, respect_mask=False):
    """Prefer liquid mid/small. Cap large+mega. Skip sub-$400M micros on the BUY side.

    BUY fills from buy_mask (green pile) when it is thick enough; otherwise the
    full weighted walk. buy_sort (e.g. green_rank) overrides score_{h} when the
    pile is used. SELL always ranks on the core weighted score (no pile,
    no buy-side add-ons) when sell_core is set and the column exists, and
    never shorts a name that is in the buy_mask. allow_empty empties BUY
    (stand-down / catalyst-only miss) but still ranks SELL.
    """
    col = f"score_{horizon}"
    pool = df
    if buy_mask is not None:
        try:
            masked = df.loc[buy_mask]
        except Exception:
            masked = df
        if masked is not None and len(masked):
            pool = masked
            if respect_mask:
                sort_col = (
                    buy_sort if buy_sort and buy_sort in pool.columns
                    else f"score_{horizon}"
                )
                try:
                    pool = pool.loc[~_buy_veto_mask(pool)]
                except Exception:
                    pass
                buys = pool.sort_values(sort_col, ascending=False)
                return buys, _rank_sells(
                    df, horizon, top_n, sell_core,
                    sell_mask=sell_mask, sell_sort=sell_sort,
                )
        elif allow_empty:
            return df.head(0), _rank_sells(
                df, horizon, top_n, sell_core,
                sell_mask=sell_mask, sell_sort=sell_sort,
            )
    if pool is not None and len(pool):
        try:
            pool = pool.loc[~_buy_veto_mask(pool)]
        except Exception:
            pass
    sort_col = col
    if buy_sort and pool is not None and buy_sort in getattr(pool, "columns", []):
        sort_col = buy_sort
    ranked = pool.sort_values(sort_col, ascending=False)
    picks = []
    sec_n: dict[str, int] = {}
    ind_n: dict[str, int] = {}
    large_n = 0
    for _, r in ranked.iterrows():
        sec = str(r.get("sector") or "")
        ind = str(r.get("industry") or "")
        size = str(r.get("size") or "").lower()
        mcap = r.get("market_cap_m")
        try:
            mcap_f = float(mcap) if mcap == mcap else 0.0
        except (TypeError, ValueError):
            mcap_f = 0.0
        if size == "micro" or mcap_f < MIN_OPP_MCAP_M:
            continue
        if size in ("large", "mega") or mcap_f > MAX_OPP_MCAP_M:
            if large_n >= MAX_LARGE_MEGA:
                continue
        if sec and sec_n.get(sec, 0) >= MAX_PER_SECTOR:
            continue
        if ind and ind not in ("", "nan", "None") and ind_n.get(ind, 0) >= MAX_PER_INDUSTRY:
            continue
        picks.append(r)
        if size in ("large", "mega") or mcap_f > MAX_OPP_MCAP_M:
            large_n += 1
        if sec:
            sec_n[sec] = sec_n.get(sec, 0) + 1
        if ind and ind not in ("", "nan", "None"):
            ind_n[ind] = ind_n.get(ind, 0) + 1
        if len(picks) >= top_n:
            break
    buys = pd.DataFrame(picks) if picks else ranked.head(0)
    sell_df = df
    pile_sell = False
    if buy_mask is not None:
        try:
            rest = df.loc[~buy_mask]
            if rest is not None and len(rest):
                sell_df = rest
                pile_sell = True
            else:
                return buys, df.head(0)
        except Exception:
            sell_df = df
    local_sell_mask = sell_mask
    if sell_mask is not None and sell_df is not df:
        try:
            local_sell_mask = sell_mask.reindex(sell_df.index).fillna(False)
        except Exception:
            local_sell_mask = None
    sells = _rank_sells(
        sell_df, horizon, top_n, sell_core,
        sell_mask=local_sell_mask, sell_sort=sell_sort,
    )
    if pile_sell and buy_mask is not None:
        try:
            sells = sells.loc[~buy_mask.reindex(sells.index).fillna(False)]
        except Exception:
            pass
    return buys, sells


def _bucket_side(df: pd.DataFrame, horizon: str, bucket: str, n: int = 8,
                 sell_core: bool = True, buy_mask=None, buy_sort=None,
                 allow_empty=False, sell_mask=None, sell_sort=None,
                 respect_mask=False):
    if "size" not in df.columns:
        return None, None
    sub = df[df["size"].astype(str).str.lower().isin(SIZE_BUCKETS[bucket])]
    if sub.empty:
        return None, None
    sub_mask = None
    sub_sell_mask = None
    if buy_mask is not None:
        try:
            sub_mask = buy_mask.reindex(sub.index).fillna(False)
        except Exception:
            sub_mask = None
    if sell_mask is not None:
        try:
            sub_sell_mask = sell_mask.reindex(sub.index).fillna(False)
        except Exception:
            sub_sell_mask = None
    return _book_side(sub, horizon, min(n, max(1, len(sub) // 2)),
                      sell_core=sell_core, buy_mask=sub_mask, buy_sort=buy_sort,
                      allow_empty=allow_empty, sell_mask=sub_sell_mask,
                      sell_sort=sell_sort, respect_mask=respect_mask)


def _row_dict(r: pd.Series, horizon: str, side: str) -> dict:
    def _f(key):
        try:
            return float(r.get(key) or 0)
        except (TypeError, ValueError):
            return 0.0
    return {
        "ticker": r["Ticker"],
        "score": float(r[f"score_{horizon}"]),
        "sector": r.get("sector"),
        "industry": r.get("industry"),
        "size": r.get("size"),
        "side": side,
        "reasons": r.get("reasons"),
        "rebound": bool(r.get("rebound", False)),
        "market_cap_m": r.get("market_cap_m"),
        "avg_vol_k": r.get("avg_vol_k"),
        "atr_pct": r.get("atr_pct"),
        "s_join": _f("s_join"),
        "s_general": _f("s_general"),
        "s_ab": _f("s_ab"),
        "s_peer": _f("s_peer"),
        "s_sector": _f("s_sector"),
        "s_sector_essay": _f("s_sector_essay"),
        "s_news": _f("s_news"),
        "s_heat": _f("s_heat"),
        "s_ab_intrinsic": _f("s_ab_intrinsic"),
        "relvol": _f("relvol"),
        "change_pct": _f("change_pct"),
        "gap_pct": _f("gap_pct"),
        "news_time": r.get("news_time") or "",
        "green": bool(r.get("green", False)),
        "lb_cond": r.get("lb_cond"),
        "lb_region": r.get("lb_region"),
        "lb_alarm": bool(r.get("lb_alarm", False)),
        "lb_blue": bool(r.get("lb_blue", False)),
        "lb_zero_red": bool(r.get("lb_zero_red", False)),
        "lb_fade": bool(r.get("lb_fade", False)),
        "lb_tags": r.get("lb_tags") or "",
        "lb_setups": r.get("lb_setups") or "",
        "decision_lane": r.get("decision_lane") or "",
        "bull_eligible": bool(r.get("bull_eligible", False)),
        "bear_eligible": bool(r.get("bear_eligible", False)),
        "bull_rank": _f("bull_rank"),
        "bear_rank": _f("bear_rank"),
        "bull_decision": r.get("bull_decision") or "",
        "bear_decision": r.get("bear_decision") or "",
        "decision_blockers": r.get("decision_blockers") or "",
        "domain_boxes": {
            key: r.get(f"d_{key}") or "missing"
            for key in (
                "market", "parent", "child", "company", "setup", "flow"
            )
        },
        "source_boxes": {
            key: r.get(f"src_{key}_tone") or "missing"
            for key in (
                "join", "sector", "gen", "news", "digest", "judge",
                "ab", "peer", "heat", "vol", "catal",
            )
        },
        "domain_cond": r.get("domain_cond") or "",
        "domain_region": r.get("domain_region") or "",
        "domain_white": bool(r.get("domain_white", False)),
        "domain_name_white": bool(r.get("domain_name_white", False)),
        "domain_blue": bool(r.get("domain_blue", False)),
        "domain_alarm": bool(r.get("domain_alarm", False)),
        "parent_conflict": bool(r.get("parent_conflict", False)),
        "child_abs_tone": r.get("child_abs_tone") or "",
        "child_rel_tone": r.get("child_rel_tone") or "",
        "child_d1": _f("child_d1"),
        "child_w1": _f("child_w1"),
        "child_residual": _f("child_residual"),
        "group_label": r.get("group_label") or "",
        "group_themes": r.get("group_themes") or "",
        "company_strength": _f("company_strength"),
        "company_direct": bool(r.get("company_direct", False)),
        "company_materiality": r.get("company_materiality") or "",
        "company_fresh": bool(r.get("company_fresh", False)),
        "company_price_confirmed": bool(
            r.get("company_price_confirmed", False)
        ),
        "company_summary": r.get("company_summary") or "",
        "company_sources": r.get("company_sources") or "",
    }


def _usd(mcap) -> str:
    try:
        m = float(mcap)
        if m != m:
            return "?"
        if m >= 1000:
            return f"${m/1000:.1f}B"
        return f"${m:.0f}M"
    except (TypeError, ValueError):
        return "?"


def _label_plain(lab: str) -> str:
    lab = str(lab or "")
    if not lab or lab in ("—", "nan", "None"):
        return ""
    bits = []
    if "LEAD" in lab:
        bits.append("this name **beat most of its own correlated peers** this week")
    elif "LAG" in lab:
        bits.append("this name **lagged its own correlated peers** this week")
    if "peers↑" in lab:
        bits.append("the peer basket itself was **up**")
    elif "peers↓" in lab:
        bits.append("the peer basket itself was **down** (name-specific, not a sector tide)")
    if "ind↑" in lab:
        bits.append("the Finviz industry was **advancing**")
    elif "ind↓" in lab:
        bits.append("the Finviz industry was **down**")
    return "; ".join(bits)


def _why(row) -> str:
    """One readable paragraph: why this name is on the list today."""
    t = row.get("Ticker")
    size = str(row.get("size") or "?").lower()
    sec = row.get("sector") or "?"
    ind = row.get("industry") or "?"
    parts = [
        f"**{t}** is a liquid **{size}-cap** {sec} name ({ind}) at "
        f"{_usd(row.get('market_cap_m'))}, ADV ~{float(row.get('avg_vol_k') or 0):.0f}k shares/day."
    ]
    rng = str(row.get("range") or "")
    mom = str(row.get("mom") or "")
    ext = str(row.get("ext") or "")
    setup = []
    if rng in ("deep_low", "low", "mid"):
        setup.append(f"still in the **{rng.replace('_', ' ')}** of its 52-week range (room left)")
    elif rng in ("top", "breakout"):
        setup.append(f"already at the **{rng}** of the 52-week range (less upside left)")
    if mom in ("uptrend", "mixed", "downtrend"):
        setup.append(f"tape is **{mom}** (50/200DMA)")
    if ext in ("washed", "neutral", "extended", "extreme"):
        setup.append(f"extension **{ext}**")
    if setup:
        parts.append("Setup: " + ", ".join(setup) + ".")
    surp = str(row.get("earnsurp") or "")
    if surp in ("beat", "big_beat", "miss", "big_miss"):
        parts.append(f"Last earnings were a **{surp.replace('_', ' ')}**.")
    lab = _label_plain(row.get("context_label"))
    if lab:
        parts.append("AB/peer context: " + lab + ".")
    news = float(row.get("s_news") or 0)
    if news > 0.15:
        parts.append("Today's **news/judge** is a tailwind for this ticker.")
    elif news < -0.15:
        parts.append("Today's **news/judge** is a headwind for this ticker.")
    sj = float(row.get("s_join") or 0)
    if sj > 0.4:
        parts.append("Labels × today's weather **fit** this environment.")
    elif sj < -0.2:
        parts.append("Labels × today's weather are a **headwind** (sector stamp or hostile tape).")
    if row.get("rebound"):
        parts.append("Checklist marks it as a **rebound-from-own-lows** candidate.")
    opp = float(row.get("s_opp") or 0)
    if opp > 0.2:
        parts.append("Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).")
    return " ".join(parts)


def _layer_lines(row, horizon: str, weights: dict | None = None) -> list[str]:
    w_h = (weights or {}).get(horizon) or WEIGHTS[horizon]
    wj, ws, wg, wn, wa, wp = w_h
    rows = [
        ("join × weather", wj, float(row.get("s_join") or 0),
         "does this *kind* of stock fit today's regime?"),
        ("sector predict", ws, float(row.get(f"s_sector_{horizon}", row.get("s_sector") or 0) or 0),
         "same-day sector LLM, 0 if that file is missing"),
        ("general predict", wg, float(row.get(f"s_general_{horizon}", row.get("s_general") or 0) or 0),
         "same-day SPX call × this stock's beta"),
        ("news / judge", wn, float(row.get("s_news") or 0),
         "headlines + news-judge ticker tilts"),
        ("AB checklist", wa, float(row.get("s_ab") or 0),
         "structure + P01–P04 peer/industry/sector"),
        ("peer RS", wp, float(row.get("s_peer") or 0),
         "this week vs its correlated basket"),
        ("map heat / captains", 1.0, float(row.get("s_heat") or 0),
         "nested OVERRIDE + captain research (additive)"),
    ]
    out = [
        "| Layer | Weight | Signal | Contribution | Means |",
        "|-------|-------:|-------:|-------------:|-------|",
    ]
    for name, w, s, mean in rows:
        out.append(f"| {name} | {w:.2f} | {s:+.2f} | {w*s:+.3f} | {mean} |")
    opp = float(row.get("s_opp") or 0)
    reb = REBOUND_BOOST if row.get("rebound") else 0.0
    out.append(f"| mid-cap opportunity | add | {opp:+.2f} | {opp:+.3f} | liquid small/mid, room to run |")
    if reb:
        out.append(f"| rebound floor | add | {reb:+.2f} | {reb:+.3f} | tape at own-history low |")
    out.append(f"| **{horizon} total** | | | **{float(row.get(f'score_{horizon}') or 0):+.3f}** | |")
    return out


def _green_pile_md(meta: dict) -> list[str]:
    gp = meta.get("green_pile") or {}
    sd = meta.get("stand_down") or {}
    if sd.get("stand_down"):
        return [
            "## All-green BUY / SELL",
            "",
            f"- Stand-down: **no BUY.** {sd.get('reason', '')}",
            f"- Pile still computed ({gp.get('n_pile', 0)} liquid green of "
            f"{gp.get('n_universe', meta.get('n_universe'))}) but is not used "
            "to force names into a no-win open.",
            f"- SELL still ranks on {'core' if meta.get('sell_excludes_addons', True) else 'full'} weights.",
            "",
        ]
    if not gp:
        return [
            "## All-green BUY / SELL",
            "",
            "- green-pile meta missing — this book used the weighted walk.",
            "",
        ]
    fired = gp.get("core_fired") or {}
    bits = ", ".join(f"{k}={'yes' if v else 'NO'}" for k, v in fired.items()) or "unscored"
    return [
        "## All-green BUY / SELL",
        "",
        f"- Mode: **{gp.get('buy_mode', 'weighted_fallback')}** · SELL **{gp.get('sell_mode', 'core_weights')}**",
        f"- Pile: **{gp.get('n_pile', 0)}** liquid all-green names (need ≥ {gp.get('min', 8)}) of {gp.get('n_universe', meta.get('n_universe'))}",
        f"- Core fired: {bits}",
        f"- {gp.get('reason', '')}",
        "",
    ]


def _load_map_heat(date: str) -> dict:
    p = ROOT / "01_daily" / "map_heat" / f"{date}_map_heat.json"
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _pp(v, digits=1) -> str:
    try:
        return f"{float(v):+.{digits}f}%"
    except (TypeError, ValueError):
        return "—"


def _finviz_board_md(date: str, meta: dict) -> list[str]:
    """Human read of the Finviz outperform board (industry/theme, not 11-sector essays)."""
    heat = _load_map_heat(date)
    src = meta.get("heat_source") or "none"
    n_cap = meta.get("n_heat_captains") or 0
    n_ind = meta.get("n_heat_industries") or 0
    lines = [
        "## Finviz outperform board (industry + theme)",
        "",
        "This is the live Finviz groups tape — child industry vs parent sector, "
        "plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.",
        "",
        f"- Heat into the ranker today: **{src}**"
        f" ({n_cap} captains, {n_ind} industries → s_heat).",
        f"- Board file: `01_daily/map_heat/{date}_map_heat.json`"
        + (f" · generated {heat.get('generated_at')}" if heat.get("generated_at") else ""),
        "",
    ]
    if not heat:
        lines += ["_map_heat.json missing — no industry/theme tape today._", ""]
        return lines

    essay = meta.get("sector_bias") or {}
    lines += [
        "### Sector RS vs same-day LLM essay",
        "",
        "| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |",
        "|--------|----------:|----------:|-------:|---------------|",
    ]
    for row in heat.get("sectors") or []:
        sec = row.get("sector") or ""
        d1 = row.get("d1")
        w1 = row.get("w1")
        bias = essay.get(sec)
        note = ""
        try:
            w1f = float(w1)
            if bias is not None:
                bf = float(bias)
                if bf > 0.15 and w1f <= -1.0:
                    note = "essay UP, tape DOWN"
                elif bf < -0.15 and w1f >= 1.0:
                    note = "essay DOWN, tape UP"
                elif abs(bf) <= 0.15 and abs(w1f) >= 1.5:
                    note = "essay flat, tape moving"
        except (TypeError, ValueError):
            note = ""
        bias_s = f"{float(bias):+.2f}" if bias is not None else "—"
        lines.append(
            f"| {sec} | {_pp(d1)} | {_pp(w1)} | {bias_s} | {note} |"
        )

    hot = heat.get("hot") or []
    cold = heat.get("cold") or []
    lines += [
        "",
        "### Industry heat (1w vs parent)",
        "",
        "**HOT**",
        "",
    ]
    for row in hot[:8]:
        caps = []
        for c in (row.get("spx_leaders") or []) + (row.get("rut_leaders") or []):
            if isinstance(c, dict) and c.get("ticker"):
                caps.append(str(c["ticker"]))
            elif isinstance(c, str) and c:
                caps.append(c)
        cap_s = ", ".join(caps[:4]) if caps else "—"
        lines.append(
            f"- **{row.get('industry')}** ({row.get('sector')}) "
            f"{_pp(row.get('d1'))} 1d · {_pp(row.get('w1'))} 1w · "
            f"vs parent {_pp(row.get('vs_parent_w1'))} · {cap_s}"
        )
    lines += ["", "**COLD**", ""]
    for row in cold[:8]:
        caps = []
        for c in (row.get("spx_leaders") or []) + (row.get("rut_leaders") or []):
            if isinstance(c, dict) and c.get("ticker"):
                caps.append(str(c["ticker"]))
            elif isinstance(c, str) and c:
                caps.append(c)
        cap_s = ", ".join(caps[:4]) if caps else "—"
        lines.append(
            f"- **{row.get('industry')}** ({row.get('sector')}) "
            f"{_pp(row.get('d1'))} 1d · {_pp(row.get('w1'))} 1w · "
            f"vs parent {_pp(row.get('vs_parent_w1'))} · {cap_s}"
        )

    ov = heat.get("overrides") or []
    if ov:
        lines += [
            "",
            "### Overrides (child 1w residual ≥ 3pp)",
            "",
            "| Action | Industry | 1w | Parent 1w | Gap | Captains |",
            "|--------|----------|---:|----------:|----:|----------|",
        ]
        for o in ov[:16]:
            pretty = []
            for c in (o.get("spx_leaders") or []) + (o.get("rut_leaders") or []):
                if isinstance(c, dict) and c.get("ticker"):
                    pretty.append(str(c["ticker"]))
                elif isinstance(c, str) and c:
                    pretty.append(c)
            lines.append(
                f"| {o.get('action') or '—'} | {o.get('industry')} | "
                f"{_pp(o.get('w1'))} | {_pp(o.get('parent_w1'))} | "
                f"{_pp(o.get('vs_parent_w1'))} | {', '.join(pretty[:4]) or '—'} |"
            )

    # Theme join lives on `themes`; the Finviz ETF basket tape is `theme_tape`.
    join_themes = [t for t in (heat.get("themes") or []) if t.get("subthemes")]
    etf_themes = list(heat.get("theme_tape") or [])
    if not etf_themes:
        etf_themes = [t for t in (heat.get("themes") or []) if t.get("n_etfs")]
    if join_themes:
        lines += [
            "",
            "### Theme join (sub-sector vs GICS parent)",
            "",
        ]
        for th in join_themes:
            bits = []
            for st in th.get("subthemes") or []:
                flag = "AGREE" if st.get("agree") else "**DIVERGE**"
                bits.append(
                    f"{st.get('label')}: {_pp(st.get('w1'))} 1w vs parent "
                    f"{_pp(st.get('parent_w1'))} → {flag}"
                )
            lines.append(f"- **{th.get('theme')}** — " + "; ".join(bits))
    if etf_themes:
        ranked = sorted(
            etf_themes,
            key=lambda t: abs(float(t.get("w1") or 0)),
            reverse=True,
        )
        lines += [
            "",
            "### Theme ETF tape (biggest |1w| moves)",
            "",
            "| Theme | 1d | 1w | Leaders |",
            "|-------|---:|---:|---------|",
        ]
        for t in ranked[:12]:
            leads = []
            for L in t.get("leaders") or []:
                if isinstance(L, dict) and L.get("ticker"):
                    leads.append(str(L["ticker"]))
            lines.append(
                f"| {t.get('theme')} | {_pp(t.get('d1'))} | {_pp(t.get('w1'))} | "
                f"{', '.join(leads[:3]) or '—'} |"
            )
    lines.append("")
    return lines


def _lattice_md(meta: dict) -> list[str]:
    """Trace the independent market/group/company/setup decision."""
    lattice = meta.get("decision_lattice") or {}
    market = lattice.get("market") or meta.get("market_decision") or {}
    if not lattice and not market:
        return []
    icon = {"good": "🟢", "neutral": "🟡", "bad": "🔴", "missing": "⬛"}
    lines = [
        "## Decision lattice — gate → route → rank",
        "",
        "The weighted score is now a tie-breaker inside an eligible lane. "
        "It cannot average away a market, group, company, or setup veto.",
        "",
        f"### MARKET: {icon.get(market.get('tone'), '⬛')} "
        f"{str(market.get('state') or 'unknown').upper()}",
        "",
        f"- {market.get('rationale') or 'no market evaluation'}",
        f"- Allowed long lanes: "
        f"**{', '.join(market.get('allowed_lanes') or []) or 'none'}** · "
        f"max slots {market.get('max_long_slots', 0)} · "
        f"size ×{_num_or_zero(market.get('position_scale')):.2f}",
    ]
    if market.get("bull_reasons"):
        lines.append("- Bull evidence: " + "; ".join(market["bull_reasons"]))
    if market.get("bear_reasons"):
        lines.append("- Bear evidence: " + "; ".join(market["bear_reasons"]))
    lines += [
        "",
        "Decision domains: **MKT · parent · child · company · setup · flow**. "
        "Measured parent/child tape is kept separate from the LLM essay; "
        "direct company events must be price-confirmed on a hard-red day.",
        "",
        "### Bull decisions (eligible or closest blocked cases)",
        "",
        "| # | Ticker | Domains | Lane | Company / group | Decision |",
        "|---:|--------|---------|------|-----------------|----------|",
    ]
    for i, row in enumerate((lattice.get("bull_watch") or [])[:15], 1):
        domains = "".join(
            icon.get((row.get("domains") or {}).get(k), "⬛")
            for k in ("market", "parent", "child", "company", "setup", "flow")
        )
        company = str(row.get("company") or "—").replace("|", "/")
        group = str(row.get("group") or "—").replace("|", "/")
        decision = str(row.get("bull_decision") or "").replace("|", "/")
        lines.append(
            f"| {i} | **{row.get('ticker')}** | {domains} | "
            f"{row.get('lane')} | {company}; {group} "
            f"{_pp(row.get('child_d1'))} d1 / {_pp(row.get('child_w1'))} 1w "
            f"/ {_pp(row.get('child_residual'))} vs parent | {decision} |"
        )
    lines += [
        "",
        "### Bear decisions",
        "",
        "| # | Ticker | Domains | Industry | Decision |",
        "|---:|--------|---------|----------|----------|",
    ]
    for i, row in enumerate((lattice.get("bear_watch") or [])[:15], 1):
        domains = "".join(
            icon.get((row.get("domains") or {}).get(k), "⬛")
            for k in ("market", "parent", "child", "company", "setup", "flow")
        )
        decision = str(row.get("bear_decision") or "").replace("|", "/")
        lines.append(
            f"| {i} | **{row.get('ticker')}** | {domains} | "
            f"{str(row.get('group') or '—').replace('|', '/')} | {decision} |"
        )
    lines.append("")
    return lines


def _num_or_zero(value) -> float:
    try:
        v = float(value)
        return 0.0 if v != v else v
    except (TypeError, ValueError):
        return 0.0


def write_report(df: pd.DataFrame, meta: dict, top_n: int) -> None:

    date = meta["date"]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DAILY.mkdir(parents=True, exist_ok=True)

    cols_keep = [
        "Ticker", "sector", "industry", "size",
        "market_cap_m", "avg_vol_k", "atr_pct", "relvol", "change_pct", "gap_pct",
        "news_time",
        "liquid", "rebound", "at_low",
        "s_join", "s_sector", "s_sector_essay", "s_general", "s_news", "s_ab",
        "s_ab_intrinsic", "s_peer", "s_opp",
        "s_opp_raw", "s_heat_raw", "s_heat", "green", "green_rank", "relvol",
        "lb_cond", "lb_region", "lb_zero_red", "lb_blue", "lb_alarm",
        "lb_fade", "lb_tags", "lb_setups", "lb_points",
        *[f"d_{k}" for k in (
            "market", "parent", "child", "company", "setup", "flow",
        )],
        *[f"src_{k}_tone" for k in (
            "join", "sector", "gen", "news", "digest", "judge",
            "ab", "peer", "heat", "vol", "catal",
        )],
        "domain_cond", "domain_region", "domain_white", "domain_name_white",
        "domain_blue", "domain_alarm", "parent_conflict",
        "child_abs_tone", "child_rel_tone", "child_d1", "child_w1",
        "child_residual", "group_label", "group_themes", "group_strength",
        "company_strength", "company_direct", "company_materiality",
        "company_fresh", "company_price_confirmed", "company_summary",
        "company_sources",
        "setup_strength", "flow_strength",
        "decision_lane", "bull_eligible", "bear_eligible",
        "bull_rank", "bear_rank", "bull_decision", "bear_decision",
        "decision_blockers", "parent_trace", "child_trace", "company_trace",
        "setup_trace", "flow_trace",
        # per-horizon LLM components + core scores: this CSV is the learning
        # snapshot book_learn re-scores under candidate weights
        *[f"s_sector_{h}" for h in HORIZONS],
        *[f"s_general_{h}" for h in HORIZONS],
        *[f"core_{h}" for h in HORIZONS],
        "score_1d", "score_3d", "score_1w", "score_2w", "score_1m",
        "reasons", "bulls", "bears", "flags",
    ]
    cols_keep = [c for c in cols_keep if c in df.columns]
    csv_path = OUT_DIR / f"{date}_stock_book.csv"
    df[cols_keep].to_csv(csv_path, index=False)

    sell_core = bool(meta.get("sell_excludes_addons", True))
    gp = meta.get("green_pile") or {}
    sd = meta.get("stand_down") or {}

    def selection(horizon: str):
        """1d uses the lattice; longer horizons remain shadow-compatible."""
        if (
            horizon == "1d"
            and "bull_eligible" in df.columns
            and "bear_eligible" in df.columns
        ):
            market = (
                (meta.get("decision_lattice") or {}).get("market")
                or meta.get("market_decision")
                or {}
            )
            slots = max(0, int(market.get("max_long_slots", top_n)))
            eligible = df["bull_eligible"].astype(bool)
            allowed_idx = (
                df.loc[eligible].sort_values(
                    "bull_rank", ascending=False
                ).head(slots).index
            )
            return {
                "buy_mask": pd.Series(df.index.isin(allowed_idx), index=df.index),
                "buy_sort": "bull_rank",
                "allow_empty": True,
                "respect_mask": True,
                "sell_mask": df["bear_eligible"].astype(bool),
                "sell_sort": "bear_rank",
                "ranker": "decision_lattice",
                "top_n": slots or top_n,
            }
        if horizon == "1d" and sd.get("stand_down"):
            return {
                "buy_mask": pd.Series(False, index=df.index),
                "buy_sort": None,
                "allow_empty": True,
                "sell_mask": None,
                "sell_sort": None,
                "ranker": "stand_down",
                "top_n": top_n,
            }
        if (
            horizon == "1d"
            and sd.get("restrict_to_catalysts")
            and sd.get("catalyst_tickers")
        ):
            tickers = {str(t).upper() for t in sd["catalyst_tickers"]}
            return {
                "buy_mask": df["Ticker"].isin(tickers),
                "buy_sort": None,
                "allow_empty": True,
                "sell_mask": None,
                "sell_sort": None,
                "ranker": "catalyst_only",
                "top_n": top_n,
            }
        mask = None
        sort = None
        if gp.get("used") and "green" in df.columns:
            mask = df["green"] == True  # noqa: E712
            sort = "green_rank" if "green_rank" in df.columns else None
        return {
            "buy_mask": mask,
            "buy_sort": sort,
            "allow_empty": False,
            "sell_mask": None,
            "sell_sort": None,
            "ranker": "green_pile" if gp.get("used") else "weighted",
            "top_n": top_n,
        }

    books = {}
    for h in HORIZONS:
        pick = selection(h)
        b, s = _book_side(df, h, pick["top_n"], sell_core=sell_core,
                          buy_mask=pick["buy_mask"],
                          buy_sort=pick["buy_sort"],
                          allow_empty=pick["allow_empty"],
                          sell_mask=pick["sell_mask"],
                          sell_sort=pick["sell_sort"],
                          respect_mask=bool(pick.get("respect_mask")))
        entry = {
            "buy": [_row_dict(r, h, "buy") for _, r in b.iterrows()],
            "sell": [_row_dict(r, h, "sell") for _, r in s.iterrows()],
            "buy_by_size": {},
            "sell_by_size": {},
            "ranker": pick["ranker"],
        }
        for bucket in SIZE_BUCKETS:
            bmask = (
                pick["buy_mask"].reindex(df.index).fillna(False)
                if pick["buy_mask"] is not None else None
            )
            smask = (
                pick["sell_mask"].reindex(df.index).fillna(False)
                if pick["sell_mask"] is not None else None
            )
            bb, ss = _bucket_side(df, h, bucket, sell_core=sell_core,
                                  buy_mask=bmask, buy_sort=pick["buy_sort"],
                                  allow_empty=pick["allow_empty"],
                                  sell_mask=smask,
                                  sell_sort=pick["sell_sort"],
                                  respect_mask=bool(pick.get("respect_mask")))
            if bb is not None:
                entry["buy_by_size"][bucket] = [_row_dict(r, h, "buy") for _, r in bb.iterrows()]
                entry["sell_by_size"][bucket] = [_row_dict(r, h, "sell") for _, r in ss.iterrows()]
        books[h] = entry

    json_path = OUT_DIR / f"{date}_stock_book.json"
    json_path.write_text(
        json.dumps({"meta": meta, "books": books}, indent=2, default=str),
        encoding="utf-8",
    )
    green_path = OUT_DIR / f"{date}_green.json"
    try:
        tickers: list[str] = []
        if "green" in df.columns and "Ticker" in df.columns:
            tickers = [str(t) for t in df.loc[df["green"] == True, "Ticker"].tolist()]  # noqa: E712
        green_path.write_text(
            json.dumps({**gp, "tickers": tickers}, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
    except Exception as e:  # noqa: BLE001 — book json already landed
        print(f"[stock-book] WARN: green.json write failed: {e}")

    wr = meta.get("weather_risk")
    L = [
        f"# Stock book — {date}",
        "",
        f"_Generated {meta['generated_at']}_",
        "",
        "This file is the **human read** of one run. CSV/JSON next to it are the machine files.",
        "",
        "## How today's action is built",
        "",
        "**1d uses a decision lattice.** Evidence is evaluated on its own "
        "merits before any numeric rank:",
        "",
        "1. **Market gate** — raw general factor scoreboard + risk state "
        "sets exposure. An extreme confirmed red day closes ordinary longs.",
        "2. **Parent / child route** — sector tape/essay and independent "
        "industry/theme absolute + relative strength decide where.",
        "3. **Company route** — News Judge adjudicates; actions, Finviz digest "
        "and dossiers form one deduplicated direct-event decision.",
        "4. **Setup / flow gate** — intrinsic AB + join structure, peer RS, "
        "price/gap and time-aware relative volume decide whether now.",
        "5. **Rank inside the lane** — standard, group-leader or catalyst. "
        "mid_opp cannot grant permission.",
        "",
        "The existing red/yellow/green source graph remains visible. "
        "Its digest, judge and catalyst cells are now populated before "
        "selection. 🔵 / 🚨 / ⚪, Cond, region and featured fades remain gates. "
        "A second six-domain row prevents duplicate headlines from voting "
        "three times. Longer horizons remain on the legacy weighted rank "
        "while the 1d lattice is validated.",
        "",
        "## Today's regime",
        "",
        f"- Weather risk: **{wr}**",
        f"- General predict (same-day): {meta['general_bias']:+.2f} "
        f"{meta.get('general_direction') or ''} "
        f"({'present' if meta.get('same_day_general') else 'MISSING → 0'})",
        f"- Stand-down: **{'YES — no BUY' if (meta.get('stand_down') or {}).get('stand_down') else 'no'}**"
        f" — {(meta.get('stand_down') or {}).get('reason', '')}",
        f"- Sector predicts this date: {meta.get('same_day_sectors', 0)}/11 "
        f"({'ok' if meta.get('same_day_sectors') else 'missing → sector layer is 0; Finviz week tape still sits in join'})",
        f"- News tickers in play: {meta['n_news_tickers']}",
        f"- AB coverage: {meta.get('n_ab', 0)} names · peer RS: {meta.get('n_peer', 0)}",
        f"- Universe after liquidity: {meta['n_universe']}",
        f"- BUY window: ${meta.get('min_market_cap_m', 80):.0f}M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega",
        f"- News names after digest+judge: {meta.get('n_news_after_digest', meta.get('n_news_tickers'))}",
        "",
        *_green_pile_md(meta),
        *_lattice_md(meta),
        *_finviz_board_md(date, meta),
        "## Inputs this run — every resource",
        "",
        "If a row says **missing**, that layer scored 0 today. If it says **found**, it moved the rank.",
        "",
        "| Resource | This run | Where it lands in the score |",
        "|----------|----------|-----------------------------|",
    ]
    for row in meta.get("inputs") or []:
        mark = "found" if row.get("found") else "missing / not in ranker"
        L.append(f"| {row['name']} | **{mark}** | {row['used_as']} |")
    L += [
        "",
        "### Sector LLM bias (1d) — 0 / empty means that essay was not run today",
        "",
        "| Sector | bias |",
        "|--------|------|",
    ]
    if meta.get("sector_bias"):
        for sec, b in sorted(meta["sector_bias"].items(), key=lambda x: -abs(x[1])):
            L.append(f"| {sec} | {b:+.2f} |")
    else:
        L.append("| — | none today |")

    gates = meta.get("accuracy_gates") or {}
    if gates:
        L += [
            "",
            "### How much each predictor is trusted (graded hit rate)",
            "",
            "| Topic | hit rate | n | weight |",
            "|-------|----------|---|--------|",
        ]
        for t, st in sorted(gates.items()):
            L.append(f"| {t} | {st['hit_rate']:.0%} | {st['n']} | ×{st['gate']:.2f} |")

    src_note = meta.get("weights_source", "defaults")
    if meta.get("policy_version"):
        src_note += f" v{meta['policy_version']}"
    if meta.get("absent_families"):
        src_note += f" · renormalized (absent: {', '.join(meta['absent_families'])})"
    L += [
        "",
        f"## Horizon weights — {src_note}",
        "",
        "| Horizon | join | sector | general | news | AB | peer | + opportunity |",
        "|---------|------|--------|---------|------|----|------|----------------|",
    ]
    w_used = meta.get("weights") or WEIGHTS
    for h in HORIZONS:
        w = w_used.get(h, WEIGHTS[h])
        L.append(
            f"| {h} | {w[0]:.2f} | {w[1]:.2f} | {w[2]:.2f} | {w[3]:.2f} | {w[4]:.2f} | {w[5]:.2f} | additive |"
        )

    # Full rationale for 1d and 1m (the two sleeves that matter). Other horizons: compact table.
    detail_h = ("1d", "1m")
    for h in HORIZONS:
        pick = selection(h)
        buys, sells = _book_side(df, h, pick["top_n"], sell_core=sell_core,
                                buy_mask=pick["buy_mask"],
                                buy_sort=pick["buy_sort"],
                                allow_empty=pick["allow_empty"],
                                sell_mask=pick["sell_mask"],
                                sell_sort=pick["sell_sort"],
                                respect_mask=bool(pick.get("respect_mask")))
        empty_why = (
            (meta.get("stand_down") or {}).get("reason")
            if (meta.get("stand_down") or {}).get("stand_down")
            else "no names passed the BUY mask"
        )
        if h in detail_h:
            L += ["", f"## {h} BUY — why these names", ""]
            if buys is None or not len(buys):
                L += [f"_{empty_why}_", ""]
            else:
                for i, (_, r) in enumerate(buys.iterrows(), 1):
                    L += [
                        f"### {i}. {r['Ticker']} · {_usd(r.get('market_cap_m'))} {r.get('size')} · {r.get('sector')}",
                        "",
                        f"**{h} score {float(r[f'score_{h}']):+.3f}**",
                        "",
                        _why(r),
                        "",
                        *_layer_lines(r, h, meta.get("weights")),
                        "",
                    ]
            L += ["", f"## {h} AVOID — bottom of the same rank", ""]
            if sells is None or not len(sells):
                L += ["_no SELL rank today_", ""]
            else:
                for _, r in sells.iterrows():
                    if h == "1d" and r.get("bear_decision"):
                        lab = str(r.get("bear_decision"))
                    else:
                        lab = (
                            _label_plain(r.get("context_label"))
                            or str(r.get("reasons") or "")
                        )
                    L.append(
                        f"- **{r['Ticker']}** ({r.get('size')}, {r.get('sector')}, {_usd(r.get('market_cap_m'))}) "
                        f"score {float(r[f'score_{h}']):+.3f}. {lab}"
                    )
        else:
            L += [
                "",
                f"## {h} BUY (compact — same names, different weights)",
                "",
            ]
            if buys is None or not len(buys):
                L += [f"_{empty_why}_", ""]
            else:
                L += [
                    "| # | Ticker | Score | Size | Sector | Why in short |",
                    "|---|--------|------:|------|--------|--------------|",
                ]
                for i, (_, r) in enumerate(buys.iterrows(), 1):
                    short = _label_plain(r.get("context_label")) or str(r.get("reasons") or "")[:80]
                    L.append(
                        f"| {i} | {r['Ticker']} | {float(r[f'score_{h}']):+.3f} | {r.get('size')} | "
                        f"{r.get('sector')} | {short} |"
                    )

    L += [
        "",
        "## Files for this run",
        "",
        f"- This rationale: `01_daily/{date}_stock_book.md`",
        f"- Machine table: `data/stock_book/{date}_stock_book.csv`",
        f"- Machine book: `data/stock_book/{date}_stock_book.json`",
        f"- Join rank: `data/join/{date}_ranked.csv`",
        f"- Weather: `01_daily/weather/{date}_weather.md`",
        f"- AB enrich: `data/ab_checklist/{date}_ab_checklist_enriched.md`",
        f"- Peer RS: `01_daily/{date}_peer_rs.md`",
        f"- Finviz map heat: `01_daily/map_heat/{date}_map_heat.md`",
        "",
    ]
    md_path = DAILY / f"{date}_stock_book.md"
    md_path.write_text("\n".join(L), encoding="utf-8")
    copy = OUT_DIR / f"{date}_stock_book.md"
    copy.write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"[stock-book] {md_path}")
    print(f"[stock-book] {copy}")


def _write_degraded(date: str, reason: str) -> None:
    """Last-ditch marker files. skip-if-good rejects meta.degraded so heal re-ranks."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DAILY.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {"date": date, "degraded": True, "reason": reason},
        "books": {},
    }
    (OUT_DIR / f"{date}_stock_book.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    green = {
        "n_pile": 0, "used": False, "buy_mode": "weighted_fallback",
        "sell_mode": "core_weights", "tickers": [], "degraded": True,
        "reason": reason,
    }
    (OUT_DIR / f"{date}_green.json").write_text(
        json.dumps(green, indent=2) + "\n", encoding="utf-8")
    (DAILY / f"{date}_stock_book.md").write_text(
        f"# Stock book — {date}\n\n_Degraded: {reason}_\n\n"
        "Ranker crashed before BUY/SELL. Heal via Stock Book ALL.\n",
        encoding="utf-8",
    )
    print(f"[stock-book] WARN: wrote degraded book + green.json ({reason})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--as-of", dest="as_of", action="store_true",
                    help="Use the ranker that was live on --date "
                         "(weighted / green-pile / lattice)")
    args = ap.parse_args()
    date = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    try:
        df, meta = build(args.date, top_n=args.top, as_of=args.as_of)
        write_report(df, meta, top_n=args.top)
    except (Exception, SystemExit) as e:  # noqa: BLE001 — still land green.json
        print(f"[stock-book] WARN: ranker crashed: {e}")
        try:
            _write_degraded(date, str(e)[:300])
        except Exception as e2:  # noqa: BLE001
            print(f"[stock-book] WARN: degraded write failed: {e2}")


if __name__ == "__main__":
    main()
