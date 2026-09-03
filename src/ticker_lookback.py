"""Ticker-first lookback — any name, every session we have artifacts for.

Each dated row is the 09:30 ET information set, not the same-day close.

  Packet recipe (what the ranker is supposed to know by the open):
    join — D's ranked file when D's weather was built from the morning
           predict (labels × pre-open weather). Else last prior join.
    vol / Finviz cells / AB / peer / overnight book — last completed
           tape dated before D (walk back if the prior session file
           is missing). Same-day Finviz / book still never color D.
    sector / gen — D's morning predict, else the last prior predict.
    news / digest / judge / catal — D's pre-open files (ticker row,
           else sector tilt / sector digest when the file printed one).
    heat — D morning captains, else the map-heat industry/sector board
           knowable by 09:30 (D morning_overlay or last night's board).

  +1d / +3d / +1w stay forward outcomes from D's close.

CLI:
  python -m src.ticker_lookback_run --tickers TEM,ELF,AAPL
"""
from __future__ import annotations

import argparse
import json
import math
import random
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
JOIN_DIR = ROOT / "data" / "join"
EXPORT_DIR = ROOT / "data" / "exports"
AB_DIR = ROOT / "data" / "ab_checklist"
PEER_DIR = ROOT / "data" / "peers"
UNIVERSE_DIR = ROOT / "data" / "universe"
QUOTE_DIR = ROOT / "data" / "quote_colors"
CATALYST_DIR = ROOT / "01_daily" / "catalyst"
NEWS_DIR = ROOT / "01_daily" / "news"
GENERAL_DIR = ROOT / "01_daily" / "general"
MAP_HEAT_DIR = ROOT / "01_daily" / "map_heat"
WEATHER_DIR = ROOT / "01_daily" / "weather"
PAPER_DIR = ROOT / "data" / "paper"
PRICE_STORE = ROOT / "data" / "prices" / "ohlc.parquet"
DAILY = ROOT / "01_daily"
SCORE = ROOT / "03_scoreboard"
ET = ZoneInfo("America/New_York")

EPS = 0.05
PRICE_EPS = 0.5  # ±0.5% prints yellow on forward 1d/3d/1w
RELVOL_SPIKE = 1.5
RELVOL_DEAD = 0.7
CORE = ("s_join", "s_general", "s_ab", "s_peer")
BOX_ICON = {"good": "\U0001f7e2", "bad": "\U0001f534", "neutral": "\U0001f7e1", "missing": "\u2b1b"}
TONE_RANK = {"bad": 0, "neutral": 1, "good": 2}
# User-facing points for day-over-day jumps: red=1, yellow=2, green=3.
TONE_POINTS = {"bad": 1, "neutral": 2, "good": 3}
BLUE_POINT_JUMP = 3
# Row region: ignore yellows. Need this many printed cells and this G−R gap.
REGION_MIN_PRINT = 3
REGION_GAP = 2
STRETCH_WINDOW = 3
STRETCH_EDGE = 0.15
# Walk at most this many prior sessions for last-known tape / predict.
LAST_KNOWN_STEPS = 8
RANDOM_N = 50
# Finviz export units: Market Cap = $ millions, Average Volume = thousands of shares.
RANDOM_MIN_MCAP_M = 100.0
RANDOM_MIN_AVG_VOL_K = 500.0
BOX_COLS = (
    ("join", "join"), ("sector", "sect"), ("gen", "gen"), ("news", "news"),
    ("digest", "dig"), ("judge", "jdg"), ("ab", "AB"), ("peer", "peer"),
    ("heat", "heat"), ("vol", "vol"), ("catal", "cat"), ("buy", "buy"),
)
JOIN_FAMILIES = (
    "mom", "rsi", "sma20", "profit", "earn", "earnsurp", "rvol",
    "themes", "analyst", "peg", "q_mom", "range", "ext", "roe",
)
_INDEX = None
_PRICE_PANEL = None
_OHLC_BARS = None
_FINVIZ_BARS = {}


def _tick(s):
    return str(s or "").strip().upper()


def _num(x, default=None):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return default
    try:
        raw = str(x).strip().replace(",", "")
        if raw.endswith("%"):
            raw = raw[:-1]
        v = float(raw)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(v) else v


def _polarity(x, eps=EPS):
    v = _num(x)
    if v is None:
        return "missing"
    if v >= eps:
        return "good"
    if v <= -eps:
        return "bad"
    return "neutral"


def price_tone(x, eps=PRICE_EPS):
    """Red / yellow / green for a forward percent change."""
    return _polarity(x, eps=eps)


def is_trading_date(date_str) -> bool:
    """True for Mon–Fri. Weekend artifact dumps are not market sessions."""
    try:
        d = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
    except (TypeError, ValueError):
        return False
    return d.weekday() < 5


def _tone_rank(tone):
    return TONE_RANK.get(str(tone or "").lower())


def objectively_better(prev_boxes, next_boxes) -> bool:
    """True when next day's factor colors improved with no cell worse.

    Comparable cells only (red / yellow / green on both days). Missing is
    ignored. Rank: red < yellow < green.
    """
    improved = False
    for key, _ in BOX_COLS:
        a = _tone_rank((prev_boxes or {}).get(key))
        b = _tone_rank((next_boxes or {}).get(key))
        if a is None or b is None:
            continue
        if b < a:
            return False
        if b > a:
            improved = True
    return improved


def purely_worse(prev_boxes, next_boxes) -> bool:
    """True when no comparable cell improved and at least one got worse."""
    worsened = False
    for key, _ in BOX_COLS:
        a = _tone_rank((prev_boxes or {}).get(key))
        b = _tone_rank((next_boxes or {}).get(key))
        if a is None or b is None:
            continue
        if b > a:
            return False
        if b < a:
            worsened = True
    return worsened


def tone_tally(boxes) -> dict:
    counts = {"good": 0, "neutral": 0, "bad": 0}
    for key, _ in BOX_COLS:
        tone = str((boxes or {}).get(key) or "").lower()
        if tone in counts:
            counts[tone] += 1
    return counts


def general_condition(boxes) -> dict:
    """Majority tone of printed factors plus G/Y/R counts.

    Green or red only when that color strictly outcounts the other two.
    Ties and yellow-led days are yellow. All-missing is missing.
    """
    c = tone_tally(boxes)
    g, y, r = c["good"], c["neutral"], c["bad"]
    n = g + y + r
    if n == 0:
        tone = "missing"
    elif g > r and g > y:
        tone = "good"
    elif r > g and r > y:
        tone = "bad"
    else:
        tone = "neutral"
    return {"tone": tone, "good": g, "neutral": y, "bad": r, "n": n}


def box_points(boxes) -> int:
    """Sum red=1 / yellow=2 / green=3. Missing is 0."""
    total = 0
    for key, _ in BOX_COLS:
        total += TONE_POINTS.get(str((boxes or {}).get(key) or "").lower(), 0)
    return total


def point_delta(prev_boxes, next_boxes) -> int:
    return box_points(next_boxes) - box_points(prev_boxes)


def zero_red(boxes) -> bool:
    """True when at least one printed factor exists and none of them is red."""
    printed = False
    for key, _ in BOX_COLS:
        tone = str((boxes or {}).get(key) or "").lower()
        if tone not in TONE_POINTS:
            continue
        printed = True
        if tone == "bad":
            return False
    return printed


def color_region(boxes, min_print=REGION_MIN_PRINT, gap=REGION_GAP) -> dict:
    """Green vs red cell mass, yellows ignored.

    Cond treats yellow as a color, so a 5/4/1 row is yellow. The sheet's
    visual 'sea of green' / 'sea of red' is G−R. Thin = not enough prints
    to call a region.
    """
    c = tone_tally(boxes)
    g, r = c["good"], c["bad"]
    n = g + c["neutral"] + r
    if n == 0:
        tone, bal = "missing", None
    elif n < min_print:
        tone, bal = "thin", (g - r) / n
    elif g - r >= gap:
        tone, bal = "good", (g - r) / n
    elif r - g >= gap:
        tone, bal = "bad", (g - r) / n
    else:
        tone, bal = "neutral", (g - r) / n
    return {"tone": tone, "good": g, "neutral": c["neutral"], "bad": r,
            "n": n, "balance": None if bal is None else round(bal, 3)}


def tag_context(day) -> list[str]:
    """How the day's tags sit on the row's green/red mass.

    Labels only — the market-wide mine decides which ones pay.
    first_crack (alarm on a still-green row) is the clean fade.
    turn (blue on a red row) did not clear the bar market-wide.
    """
    reg = str((day.get("region") or {}).get("tone") or "")
    out = []
    if day.get("signal_improved"):
        if reg == "bad":
            out.append("turn")
        elif reg == "good":
            out.append("late")
    if day.get("signal_alarm"):
        if reg == "good":
            out.append("first_crack")
        elif reg == "bad":
            out.append("continuation")
    if day.get("zero_red"):
        if reg == "neutral":
            out.append("clean_chop")
        elif reg == "good":
            out.append("crowded")
    return out


def annotate_regions(days, window=STRETCH_WINDOW, edge=STRETCH_EDGE):
    """Attach row region, trailing stretch, and tag-in-region context."""
    bals = []
    for day in days or []:
        region = color_region(day.get("boxes") or {})
        day["region"] = region
        bals.append(region.get("balance"))
    for i, day in enumerate(days or []):
        window_vals = [b for b in bals[max(0, i - window + 1): i + 1]
                       if b is not None]
        if not window_vals:
            stretch_tone, stretch_bal = "missing", None
        else:
            stretch_bal = sum(window_vals) / len(window_vals)
            if stretch_bal >= edge:
                stretch_tone = "good"
            elif stretch_bal <= -edge:
                stretch_tone = "bad"
            else:
                stretch_tone = "neutral"
        day["stretch"] = {
            "tone": stretch_tone,
            "n": len(window_vals),
            "balance": None if stretch_bal is None else round(stretch_bal, 3),
        }
        day["tag_context"] = tag_context(day)
    return days


def annotate_signal_improved(days):
    """Mark blue / alarm / white and attach the general-condition tally."""
    for i, day in enumerate(days or []):
        boxes = day.get("boxes") or {}
        day["zero_red"] = zero_red(boxes)
        day["box_points"] = box_points(boxes)
        day["condition"] = general_condition(boxes)
        day["point_delta"] = None
        day["signal_improved"] = False
        day["signal_alarm"] = False
        if i == 0:
            continue
        prev = days[i - 1].get("boxes") or {}
        delta = point_delta(prev, boxes)
        day["point_delta"] = delta
        day["signal_improved"] = (
            objectively_better(prev, boxes) or delta >= BLUE_POINT_JUMP)
        day["signal_alarm"] = purely_worse(prev, boxes)
    annotate_regions(days)
    return days


def latest_finviz_path(asof=None):
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if asof:
        files = [p for p in files if p.stem.split("_", 1)[-1] <= asof]
    return files[-1] if files else None


def liquid_universe(asof=None, min_mcap_m=RANDOM_MIN_MCAP_M,
                    min_avg_vol_k=RANDOM_MIN_AVG_VOL_K):
    """Tickers with market cap > $100M and average volume > 500K shares."""
    path = latest_finviz_path(asof)
    if path is None:
        return []
    df = _csv(path)
    if df.empty:
        return []
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    seen, out = set(), []
    for rec in df.to_dict(orient="records"):
        t = _tick(rec.get(tcol))
        if not t or t in seen:
            continue
        mcap = _num(rec.get("Market Cap"))
        adv = _num(rec.get("Average Volume") or rec.get("Avg Volume")
                   or rec.get("Avg Vol"))
        if mcap is None or adv is None:
            continue
        if mcap > min_mcap_m and adv > min_avg_vol_k:
            seen.add(t)
            out.append(t)
    return out


def pick_random_tickers(n=RANDOM_N, asof=None, seed=None):
    names = liquid_universe(asof)
    if not names:
        raise SystemExit(
            "no liquid names (mcap>$100M, avg vol>500K) in Finviz exports")
    rng = random.Random(seed)
    if len(names) <= n:
        return names
    return rng.sample(names, n)


def _csv(path):
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _jload(path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _dates_from(folder, pattern):
    out = []
    for p in folder.glob(pattern):
        m = re.search(r"(20\d{2}-\d{2}-\d{2})", p.name)
        if m:
            out.append(m.group(1))
    return sorted(set(out))


def session_dates():
    dates = set()
    dates.update(_dates_from(BOOK_DIR, "????-??-??_stock_book.csv"))
    dates.update(_dates_from(JOIN_DIR, "????-??-??_ranked.csv"))
    dates.update(_dates_from(EXPORT_DIR, "finviz_????-??-??.csv"))
    dates.update(_dates_from(AB_DIR, "????-??-??_ab_slim.csv"))
    dates.update(_dates_from(AB_DIR, "????-??-??_ab_checklist_enriched.csv"))
    dates.update(_dates_from(AB_DIR, "????-??-??_ab_checklist.csv"))
    dates.update(_dates_from(PEER_DIR, "????-??-??_peer_rs.csv"))
    dates.update(_dates_from(QUOTE_DIR, "????-??-??_quote_colors_detail.json"))
    dates.update(_dates_from(NEWS_DIR, "????-??-??_actions.json"))
    dates.update(_dates_from(NEWS_DIR, "????-??-??_finviz_digest.json"))
    dates.update(_dates_from(GENERAL_DIR, "????-??-??_predict.md"))
    return sorted(d for d in dates if is_trading_date(d))


def _by_ticker(df):
    if df is None or df.empty:
        return {}
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    out = {}
    for rec in df.to_dict(orient="records"):
        t = _tick(rec.get(tcol))
        if t and t not in out:
            out[t] = rec
    return out


def build_index():
    global _INDEX
    if _INDEX is not None:
        return _INDEX
    dates = session_dates()
    sessions = []
    for d in dates:
        book_json = _jload(BOOK_DIR / f"{d}_stock_book.json") or {}
        green = _jload(BOOK_DIR / f"{d}_green.json") or {}
        book_map = _by_ticker(_csv(BOOK_DIR / f"{d}_stock_book.csv"))
        join_map = _by_ticker(_csv(JOIN_DIR / f"{d}_ranked.csv"))
        fv_map = _by_ticker(_csv(EXPORT_DIR / f"finviz_{d}.csv"))
        ab_path = AB_DIR / f"{d}_ab_slim.csv"
        if not ab_path.exists():
            ab_path = AB_DIR / f"{d}_ab_checklist_enriched.csv"
        if not ab_path.exists():
            ab_path = AB_DIR / f"{d}_ab_checklist.csv"
        ab_map = _by_ticker(_csv(ab_path))
        peer_map = _by_ticker(_csv(PEER_DIR / f"{d}_peer_rs.csv"))
        univ_map = _by_ticker(_csv(UNIVERSE_DIR / f"{d}_membership.csv"))
        quote_map = _jload(QUOTE_DIR / f"{d}_quote_colors_detail.json") or {}
        catalyst_payload = _jload(CATALYST_DIR / f"{d}_dossiers.json") or {}
        catalyst_map = {
            _tick(r.get("ticker")): r
            for r in (catalyst_payload.get("dossiers") or [])
            if isinstance(r, dict) and _tick(r.get("ticker"))
        }
        buys, sells = {}, {}
        for h, entry in (book_json.get("books") or {}).items():
            for i, r in enumerate(entry.get("buy") or [], 1):
                t = _tick(r.get("ticker"))
                buys.setdefault(t, {})[h] = {"rank": i, "score": r.get("score"), "reasons": r.get("reasons")}
            for i, r in enumerate(entry.get("sell") or [], 1):
                t = _tick(r.get("ticker"))
                sells.setdefault(t, {})[h] = {"rank": i, "score": r.get("score")}
        green_buy = {_tick(x.get("ticker") if isinstance(x, dict) else x) for x in (green.get("green_buy") or [])}
        live_buy = {_tick(x.get("ticker") if isinstance(x, dict) else x) for x in (green.get("live_buy") or [])}
        sessions.append({
            "date": d,
            "has": {"book": bool(book_map), "join": bool(join_map), "finviz": bool(fv_map),
                     "ab": bool(ab_map), "peer": bool(peer_map), "universe": bool(univ_map),
                     "quote_colors": bool(quote_map), "catalyst": bool(catalyst_map),
                     "green": bool(green)},
            "n_book": len(book_map), "n_join": len(join_map), "n_finviz": len(fv_map),
            "n_ab": len(ab_map), "n_peer": len(peer_map),
            "book": book_map, "join": join_map, "finviz": fv_map, "ab": ab_map,
            "peer": peer_map, "universe": univ_map, "quote_colors": quote_map,
            "catalyst": catalyst_map, "buys": buys, "sells": sells,
            "green_buy": green_buy, "live_buy": live_buy,
            "green_meta": {"n_pile": green.get("n_pile"), "n_universe": green.get("n_universe"), "pile_used": green.get("pile_used")},
        })
    for i, sess in enumerate(sessions):
        sess["prior"] = sessions[i - 1] if i else None
        sess["prior_date"] = sessions[i - 1]["date"] if i else None
    paper_hits = {}
    trades = _csv(PAPER_DIR / "trades.csv")
    if not trades.empty and "ticker" in trades.columns:
        for rec in trades.to_dict(orient="records"):
            t = _tick(rec.get("ticker"))
            paper_hits.setdefault(t, []).append({
                "date": str(rec.get("date") or "")[:10], "side": rec.get("side"),
                "sleeve": rec.get("sleeve"), "price": _num(rec.get("price")),
                "reason": str(rec.get("reason") or "")[:180],
            })
    _INDEX = {"sessions": sessions, "paper": paper_hits, "dates": dates,
              "preopen": {}}
    return _INDEX


def _beta_load(v):
    s = str(v).lower() if v == v else ""
    if s == "high":
        return 1.0
    if s == "mid":
        return 0.5
    if s == "low":
        return 0.15
    return 0.4


def walk_prior(sess, pred, max_steps=LAST_KNOWN_STEPS):
    """Last prior session that satisfies pred. Never returns sess itself."""
    cur = (sess or {}).get("prior")
    n = 0
    while cur is not None and n < max_steps:
        try:
            if pred(cur):
                return cur
        except Exception:
            pass
        cur = cur.get("prior")
        n += 1
    return None


def last_predict_date(date):
    """D's morning predict, else the last prior session that has one."""
    if (GENERAL_DIR / f"{date}_predict.md").exists():
        return date
    idx = _INDEX or build_index()
    sess = next((s for s in idx["sessions"] if s["date"] == date), None)
    hit = walk_prior(
        sess, lambda s: (GENERAL_DIR / f"{s['date']}_predict.md").exists())
    return hit["date"] if hit else None


# Judge sector_tilts use short aliases, not always Finviz sector names.
_JUDGE_SECTOR = {
    "technology": "technology",
    "semis/xlk": "technology",
    "semis/ai": "technology",
    "software": "technology",
    "energy": "energy",
    "healthcare": "healthcare",
    "basic materials": "basic materials",
    "basic-materials": "basic materials",
    "materials_gold": "basic materials",
    "gold/miners": "basic materials",
    "precious": "basic materials",
}


def _norm_sector(name):
    return str(name or "").lower().replace("_", " ").replace("-", " ").strip()


def judge_sector_tone(tilts, sector):
    """Map judge sector_tilts onto a Finviz sector. None if no match."""
    if not tilts or not sector:
        return None
    target = _JUDGE_SECTOR.get(_norm_sector(sector), _norm_sector(sector))
    if not target:
        return None
    for key, val in (tilts.items() if isinstance(tilts, dict) else []):
        mapped = _JUDGE_SECTOR.get(_norm_sector(key), _norm_sector(key))
        if not mapped or mapped != target:
            continue
        v = str(val or "").lower()
        if "bull" in v or v in {"up", "pos", "positive"}:
            return "good"
        if "bear" in v or v in {"down", "neg", "negative"}:
            return "bad"
        if "mix" in v or "neutral" in v or "flat" in v:
            return "neutral"
    return None


def _digest_tones(date):
    """Ticker → tone from D's pre-open Finviz digest file only."""
    data = _jload(NEWS_DIR / f"{date}_finviz_digest.json") or {}
    out = {}
    rows = list(data.get("top_signal") or []) + list(
        data.get("all_ticker_digests_sample") or [])
    for sec_rows in (data.get("by_sector") or {}).values():
        rows.extend(sec_rows or [])
    try:
        from .stock_book import _digest_polarity
    except Exception:
        _digest_polarity = lambda _t: 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        t = _tick(row.get("ticker"))
        if not t or t in out or t in {"SPY", "QQQ", "DIA", "IWM"}:
            continue
        text = str(row.get("digest") or row.get("news_title") or "")
        if not text.strip():
            continue
        out[t] = _polarity(_digest_polarity(text))
    return out


def _digest_sector_tones(date):
    """Sector → tone from D's digest by_sector bucket (mean polarity)."""
    data = _jload(NEWS_DIR / f"{date}_finviz_digest.json") or {}
    try:
        from .stock_book import _digest_polarity
    except Exception:
        return {}
    out = {}
    for sec, rows in (data.get("by_sector") or {}).items():
        scores = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            text = str(row.get("digest") or row.get("news_title") or "")
            if not text.strip():
                continue
            scores.append(_digest_polarity(text))
        if scores:
            out[str(sec)] = _polarity(sum(scores) / len(scores))
    return out


def _map_heat_board(date, prior_date=None):
    """Map-heat industry/sector board knowable at 09:30.

    D's file is used only when it is a morning_overlay (prior export).
    Same-day postclose boards leak D's tape. Otherwise last prior board.
    """
    data = _jload(MAP_HEAT_DIR / f"{date}_map_heat.json") or {}
    if data.get("phase") == "morning_overlay":
        return data, date
    idx = _INDEX or build_index()
    sess = next((s for s in idx["sessions"] if s["date"] == date), None)

    def has_board(s):
        return (MAP_HEAT_DIR / f"{s['date']}_map_heat.json").exists()

    hit = walk_prior(sess, has_board)
    if hit:
        return _jload(MAP_HEAT_DIR / f"{hit['date']}_map_heat.json") or {}, hit["date"]
    if prior_date:
        fallback = _jload(MAP_HEAT_DIR / f"{prior_date}_map_heat.json") or {}
        if fallback:
            return fallback, prior_date
    return {}, None


def _heat_from_board(data):
    """Industry / sector d1 from a map_heat.json board."""
    ind, sec = {}, {}
    for row in data.get("industries") or []:
        if not isinstance(row, dict):
            continue
        name, d1 = row.get("industry"), _num(row.get("d1"))
        if name and d1 is not None:
            ind[name] = float(d1)
    for row in data.get("sectors") or []:
        if not isinstance(row, dict):
            continue
        name, d1 = row.get("sector"), _num(row.get("d1"))
        if name and d1 is not None:
            sec[name] = float(d1)
    return ind, sec


def _heat_asof(date, prior_date=None):
    """Heat knowable by 09:30: captains, then the map-heat board.

    Same-day research.json is used only when map_heat_research.ticker_boosts
    accepts it (phase=morning_refresh, ≥20 cards). A rejected same-day file
    is not re-read loosely — that would smuggle a post-close stub onto D.
    """
    tboost, iboost, vintage = {}, {}, None
    try:
        from .map_heat_research import ticker_boosts
        tb, ib = ticker_boosts(date)
        if tb:
            tboost, iboost, vintage = tb, ib, date
    except Exception:
        pass
    if not tboost:
        for name in (
            MAP_HEAT_DIR / f"{prior_date}_research_baseline.json" if prior_date else None,
            MAP_HEAT_DIR / f"{prior_date}_research.json" if prior_date else None,
        ):
            if name is None or not name.exists():
                continue
            data = _jload(name) or {}
            m = re.search(r"(20\d{2}-\d{2}-\d{2})", name.name)
            file_v = m.group(1) if m else prior_date
            for card in data.get("cards") or []:
                direction = str(card.get("subsector_dir") or "").lower()
                sign = 1.0 if direction == "up" else -1.0 if direction == "down" else 0.0
                for cap in card.get("captains") or []:
                    if not isinstance(cap, dict):
                        continue
                    t = _tick(cap.get("ticker"))
                    sent = str(cap.get("sent") or "")
                    if not t:
                        continue
                    if sent == "pos":
                        tboost[t] = 0.2 * (sign or 1.0)
                    elif sent == "neg":
                        tboost[t] = -0.2 * (abs(sign) or 1.0)
                ind = card.get("industry")
                if ind and sign and card.get("action") == "OVERRIDE":
                    iboost[ind] = 0.12 * sign
            if tboost:
                vintage = file_v
                break
    board, board_v = _map_heat_board(date, prior_date)
    board_ind, board_sec = _heat_from_board(board)
    for k, v in board_ind.items():
        iboost.setdefault(k, v)
    if board_v and vintage is None:
        vintage = board_v
    return tboost, iboost, vintage, board_sec, board_v


def _digest_book(date):
    """Finviz digest → news-book rows, without stock_book's print side effect."""
    try:
        from .stock_book import _digest_polarity
    except Exception:
        return {}
    data = _jload(NEWS_DIR / f"{date}_finviz_digest.json") or {}
    out = {}
    rows = list(data.get("top_signal") or []) + list(
        data.get("all_ticker_digests_sample") or [])
    for sec_rows in (data.get("by_sector") or {}).values():
        rows.extend(sec_rows or [])
    for row in rows:
        if not isinstance(row, dict):
            continue
        t = _tick(row.get("ticker"))
        if not t or t in out or t in {"SPY", "QQQ", "DIA", "IWM"}:
            continue
        if row.get("is_dividend"):
            continue
        text = str(row.get("digest") or row.get("news_title") or "")
        pol = _digest_polarity(text)
        if not pol:
            continue
        out[t] = {
            "net": pol,
            "events": [{"event": "finviz_digest", "digest": text[:160]}],
            "source": "digest",
        }
    return out


def _accuracy_gates_asof(date, prior_date=None):
    """Hit-rate gates from scoreboard runs whose 1d outcome closed before D.

    A predict on D−1 is graded off D's close, so it is not knowable at
    09:30 on D. Cutoff is prior_date (exclusive). The full committed
    scoreboard would leak later outcomes into historical magnitudes.
    """
    try:
        from . import scoreboard
        board = scoreboard.load()
    except Exception:
        return {}
    cutoff = prior_date or date
    hits = {}
    for r in board.get("runs") or []:
        if str(r.get("date") or "") >= str(cutoff):
            continue
        h = r.get("direction_hit")
        if h is None:
            continue
        topic = r.get("topic") or ""
        hits.setdefault(topic, []).append(1.0 if h else 0.0)
    gates = {}
    for topic, arr in hits.items():
        n = len(arr)
        hr = sum(arr) / n
        if n >= 3 and hr < 0.45:
            gates[topic] = 0.5
        elif n >= 3 and hr < 0.55:
            gates[topic] = 0.85
        else:
            gates[topic] = 1.0
    return gates


def _events_sector_tilt(date):
    """D's dated events file only — never events/latest.json."""
    data = _jload(ROOT / "01_daily" / "events" / f"{date}_events.json") or {}
    tilt = {}
    for e in data.get("events") or []:
        try:
            impact = float(e.get("impact") or 0)
        except (TypeError, ValueError):
            continue
        if impact < 3:
            continue
        direc = str(e.get("expected_direction") or "").lower()
        sign = (1.0 if direc.startswith(("bull", "pos"))
                else -1.0 if direc.startswith(("bear", "neg")) else 0.0)
        if not sign:
            continue
        for sec in e.get("sectors") or []:
            if str(sec).upper() in ("BROAD", "SPX", "ALL"):
                continue
            tilt[str(sec)] = tilt.get(str(sec), 0.0) + sign * min(impact, 5) * 0.08
    return tilt


def join_packet_ok(date) -> bool:
    """True when D's ranked join is the two-packet recipe.

    Join is labels × weather. Weather's primary input is D's general
    predict (05:55 ET). If that predict is missing or never made it
    into the weather file, fall back to the prior session's join.
    """
    if not (JOIN_DIR / f"{date}_ranked.csv").exists():
        return False
    if not (GENERAL_DIR / f"{date}_predict.md").exists():
        return False
    weather = _jload(WEATHER_DIR / f"{date}_weather.json") or {}
    if not weather:
        return False
    sig = weather.get("signals") or {}
    if sig.get("general_score") is None and str(sig.get("risk") or "") in {"", "unknown"}:
        return False
    return True


def preopen_packet(date, prior_date=None):
    """D's 05:40–05:55 ET packet. Cached on the index. No same-day tape.

    Gen/sector may come from the last prior morning predict when D's
    file is missing — that call was still knowable at 09:30 on D.
    """
    idx = _INDEX or build_index()
    cache = idx.setdefault("preopen", {})
    if date in cache:
        return cache[date]
    news, judge_map, judge_tilts = {}, {}, {}
    gen_bias, sector_bias = 0.0, {}
    pred_date = last_predict_date(date)
    try:
        from . import stock_book as sb
        news = sb._merge_news(sb._load_news_actions(date), _digest_book(date))
        try:
            from .judge_apply import load_or_parse
            judge_payload = load_or_parse(date) or {}
            judge_map = judge_payload.get("tickers") or {}
            judge_tilts = judge_payload.get("sector_tilts") or {}
            for t, net in judge_map.items():
                rec = news.setdefault(str(t).upper(), {"net": 0.0, "events": []})
                rec["net"] = float(rec.get("net") or 0) + float(net)
        except Exception:
            judge_map, judge_tilts = {}, {}
            raw_judge = _jload(NEWS_DIR / f"{date}_judge.json") or {}
            judge_tilts = raw_judge.get("sector_tilts") or {}
        runs = sb._runs_for_date(pred_date or date)
        gates = _accuracy_gates_asof(date, prior_date=prior_date)
        gen_bias = float(
            sb._bias_for(runs.get("general"), "1d") * gates.get("general", 1.0))
        for topic, run in runs.items():
            if not str(topic).startswith("sector:"):
                continue
            sec = topic.split(":", 1)[1]
            sector_bias[sec] = float(
                sb._bias_for(run, "1d") * gates.get(topic, 1.0))
        for sec, tilt in _events_sector_tilt(date).items():
            sector_bias[sec] = sector_bias.get(sec, 0.0) + float(tilt)
    except Exception:
        pass
    heat, heat_ind, heat_v, heat_sec, heat_board_v = _heat_asof(date, prior_date)
    digest_tones = _digest_tones(date)
    digest_sector = _digest_sector_tones(date)
    if not judge_tilts:
        judge_tilts = (_jload(NEWS_DIR / f"{date}_judge.json") or {}).get(
            "sector_tilts") or {}
    catalyst = {
        _tick(r.get("ticker")): r
        for r in ((_jload(CATALYST_DIR / f"{date}_dossiers.json") or {}).get("dossiers") or [])
        if isinstance(r, dict) and _tick(r.get("ticker"))
    }
    packet = {
        "asof": "09:30_et",
        "news": news,
        "judge": {str(k).upper(): float(v) for k, v in (judge_map or {}).items()
                  if v is not None},
        "judge_tilts": judge_tilts,
        "digest_tones": digest_tones,
        "digest_sector": digest_sector,
        "gen_bias": gen_bias,
        "sector_bias": sector_bias,
        "heat": heat,
        "heat_ind": heat_ind,
        "heat_sec": heat_sec,
        "heat_vintage": heat_v or heat_board_v,
        "catalyst": catalyst,
        "predict_vintage": pred_date,
        "has_actions": (NEWS_DIR / f"{date}_actions.json").exists(),
        "has_digest": (NEWS_DIR / f"{date}_finviz_digest.json").exists(),
        "has_judge": (NEWS_DIR / f"{date}_judge.json").exists()
                     or (NEWS_DIR / f"{date}_judge.md").exists(),
        "has_predict": bool(pred_date),
    }
    cache[date] = packet
    return packet


def session_after(date: str, n: int, dates: list[str] | None = None) -> str | None:
    """Nth trading session after `date`, or None if the tape ends."""
    dates = dates if dates is not None else session_dates()
    key = str(date or "")[:10]
    try:
        i = dates.index(key)
    except ValueError:
        return None
    j = i + int(n)
    if 0 <= j < len(dates):
        return dates[j]
    return None


def horizon_dates(date: str, dates: list[str] | None = None) -> dict[str, str | None]:
    dates = dates if dates is not None else session_dates()
    return {
        "1d": session_after(date, 1, dates),
        "3d": session_after(date, 3, dates),
        "1w": session_after(date, 5, dates),
    }


def _ohlc_bars():
    """Full daily OHLC indexed by (date, ticker). Close-only panel stays separate."""
    global _OHLC_BARS
    if _OHLC_BARS is not None:
        return _OHLC_BARS
    if not PRICE_STORE.exists():
        _OHLC_BARS = pd.DataFrame()
        return _OHLC_BARS
    try:
        df = pd.read_parquet(PRICE_STORE)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["ticker"] = df["ticker"].astype(str).str.upper()
        _OHLC_BARS = (
            df.drop_duplicates(["date", "ticker"], keep="last")
            .set_index(["date", "ticker"])
            .sort_index()
        )
    except Exception:
        _OHLC_BARS = pd.DataFrame()
    return _OHLC_BARS


def _finviz_bar(ticker: str, date: str) -> dict:
    """Same-day Finviz Open / Price when the OHLC store has no bar yet."""
    d = str(date or "")[:10]
    empty = {"open": None, "close": None, "close_open_pct": None}
    if not d:
        return empty
    if d not in _FINVIZ_BARS:
        df = _csv(EXPORT_DIR / f"finviz_{d}.csv")
        out: dict[str, dict] = {}
        if not df.empty and "Ticker" in df.columns:
            for rec in df.to_dict(orient="records"):
                name = _tick(rec.get("Ticker"))
                if not name:
                    continue
                o = _num(rec.get("Open"))
                c = _num(rec.get("Price"))
                oc = _num(rec.get("Change from Open"))
                if oc is None and o and c:
                    oc = round(100.0 * (c / o - 1.0), 3)
                elif oc is not None:
                    oc = round(float(oc), 3)
                out[name] = {
                    "open": None if o is None else round(float(o), 4),
                    "close": None if c is None else round(float(c), 4),
                    "close_open_pct": oc,
                }
        _FINVIZ_BARS[d] = out
    return _FINVIZ_BARS[d].get(_tick(ticker)) or empty


def session_bar(ticker: str, date: str) -> dict:
    """Regular-session OHLC on `date` (open ≈ 09:30 ET, close ≈ 16:00 ET)."""
    t = _tick(ticker)
    empty = {
        "open": None, "high": None, "low": None, "close": None,
        "close_open_pct": None, "open_clock": "09:30 ET",
        "close_clock": "16:00 ET",
    }
    if not t or not date:
        return empty
    bars = _ohlc_bars()
    key = (str(date)[:10], t)
    o = h = low = c = oc = None
    if not bars.empty:
        try:
            row = bars.loc[key]
        except KeyError:
            row = None
        if row is not None:
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            o = _num(row.get("open") if hasattr(row, "get") else row["open"])
            h = _num(row.get("high") if hasattr(row, "get") else row["high"])
            low = _num(row.get("low") if hasattr(row, "get") else row["low"])
            c = _num(row.get("close") if hasattr(row, "get") else row["close"])
            oc = None if not o or not c else round(100.0 * (c / o - 1.0), 3)
    if o is None or c is None:
        fv = _finviz_bar(t, date)
        if o is None:
            o = fv.get("open")
        if c is None:
            c = fv.get("close")
        if oc is None:
            oc = fv.get("close_open_pct")
            if oc is None and o and c:
                oc = round(100.0 * (c / o - 1.0), 3)
    return {
        "open": None if o is None else round(float(o), 4),
        "high": None if h is None else round(float(h), 4),
        "low": None if low is None else round(float(low), 4),
        "close": None if c is None else round(float(c), 4),
        "close_open_pct": oc,
        "open_clock": "09:30 ET",
        "close_clock": "16:00 ET",
    }


def _price_panel():
    global _PRICE_PANEL
    if _PRICE_PANEL is not None:
        return _PRICE_PANEL
    if not PRICE_STORE.exists():
        _PRICE_PANEL = pd.DataFrame()
        return _PRICE_PANEL
    try:
        df = pd.read_parquet(PRICE_STORE)
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        df["ticker"] = df["ticker"].astype(str).str.upper()
        _PRICE_PANEL = df.drop_duplicates(
            ["date", "ticker"], keep="last"
        ).pivot(index="date", columns="ticker", values="close").sort_index()
    except Exception:
        _PRICE_PANEL = pd.DataFrame()
    return _PRICE_PANEL


def _asof_close(ticker: str, date: str, current_finviz: dict | None = None):
    """Session close on `date`: OHLC panel first, then that day's Finviz Price."""
    t = _tick(ticker)
    panel = _price_panel()
    if not panel.empty and t in panel.columns:
        idx = panel.index.searchsorted(pd.Timestamp(date))
        if idx < len(panel.index) and panel.index[idx].date().isoformat() == date:
            px = _num(panel[t].iloc[idx])
            if px:
                return px
    return _num((current_finviz or {}).get("Price"))


def _fwd_from_panel(ticker: str, date: str) -> dict[str, float | None]:
    panel = _price_panel()
    t = _tick(ticker)
    horizons = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}
    out = {h: None for h in horizons}
    if panel.empty or t not in panel.columns:
        return out
    idx = panel.index.searchsorted(pd.Timestamp(date))
    if idx >= len(panel.index) or panel.index[idx].date().isoformat() != date:
        return out
    entry = _num(panel[t].iloc[idx])
    if not entry:
        return out
    for h, n in horizons.items():
        if idx + n < len(panel.index):
            exitp = _num(panel[t].iloc[idx + n])
            if exitp:
                out[h] = round(100 * (exitp / entry - 1), 3)
    return out


def _fwd_from_sessions(ticker: str, date: str, sessions: list[dict] | None,
                       entry: float | None) -> dict[str, float | None]:
    """Fill forward % from later trading-session Finviz closes."""
    horizons = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}
    out = {h: None for h in horizons}
    if not entry or not sessions:
        return out
    t = _tick(ticker)
    future = [s for s in sessions
              if s.get("date") and s["date"] > date and is_trading_date(s["date"])]
    for h, n in horizons.items():
        if len(future) < n:
            continue
        row = (future[n - 1].get("finviz") or {}).get(t) or {}
        exitp = _num(row.get("Price"))
        if exitp:
            out[h] = round(100 * (exitp / entry - 1), 3)
    return out


def forward_returns(ticker: str, date: str,
                    sessions: list[dict] | None = None,
                    current_finviz: dict | None = None) -> dict[str, float | None]:
    """Close on `date` → close 1 / 2 / 3 / 5 trading sessions later."""
    out = _fwd_from_panel(ticker, date)
    if all(v is not None for v in out.values()):
        return out
    entry = _asof_close(ticker, date, current_finviz=current_finviz)
    fallback = _fwd_from_sessions(ticker, date, sessions, entry)
    for h, v in fallback.items():
        if out.get(h) is None:
            out[h] = v
    return out


def forward_price_changes(ticker: str, date: str,
                          sessions: list[dict] | None = None,
                          current_finviz: dict | None = None) -> dict:
    """As-of close plus forward 1d / 3d / 1w percent changes.

    1d = next trading session, 3d = three sessions later, 1w = five sessions later.
    """
    t = _tick(ticker)
    price = _asof_close(t, date, current_finviz=current_finviz)
    fwd = forward_returns(
        t, date, sessions=sessions, current_finviz=current_finviz)
    return {
        "price": None if price is None else round(price, 4),
        "1d": fwd.get("1d"),
        "3d": fwd.get("3d"),
        "1w": fwd.get("1w"),
    }


def _join_family_tone(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "missing"
    s = str(val).strip().lower()
    if not s or s in {"nan", "none", "neutral", "mid", "past", "flat"}:
        return "neutral"
    good = {"uptrend", "above", "yes", "good", "cheap", "strong_buy", "buy", "big_beat", "beat", "fast", "up", "high", "spike", "hot", "lead", "bull", "positive"}
    bad = {"downtrend", "below", "no", "poor", "rich", "expensive", "sell", "strong_sell", "miss", "big_miss", "slow", "down", "low", "quiet", "dead", "lag", "veto", "bear", "negative", "overbought"}
    if s in good:
        return "good"
    if s in bad:
        return "bad"
    if "beat" in s or "buy" in s or "up" in s:
        return "good"
    if "miss" in s or "sell" in s or "down" in s:
        return "bad"
    return "neutral"


def _s_from_join(j):
    if not j:
        return None
    v = _num(j.get("score_norm"))
    if v is not None:
        return max(-1.0, min(1.0, v / 2.0))
    v = _num(j.get("total_score"))
    if v is not None:
        return max(-1.0, min(1.0, v / 3.0))
    return None


def _s_from_ab(ab):
    if not ab:
        return None
    v = None
    for key in ("ab_raw", "score_enriched", "score_merged", "score"):
        v = _num(ab.get(key))
        if v is not None:
            break
    return None if v is None else max(-1.0, min(1.0, v / 12.0))


def _s_from_peer(p):
    if not p:
        return None
    v = _num(p.get("rs_week"))
    if v is None:
        v = _num(p.get("rs_month"))
    return None if v is None else max(-1.0, min(1.0, v / 8.0))


def _fv_relvol(fv):
    if not fv:
        return None
    for k in ("Relative Volume", "Rel Volume", "Rel Vol", "RelVol", "relvol"):
        v = _num(fv.get(k))
        if v is not None:
            return v
    vol = _num(fv.get("Volume"))
    adv = _num(fv.get("Average Volume") or fv.get("Avg Volume") or fv.get("Avg Vol"))
    if vol and adv and adv > 0:
        adv_shares = adv * 1000 if adv < vol else adv
        return vol / adv_shares if adv_shares else None
    return None
