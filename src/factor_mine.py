"""Leak-free 09:30 factor strategy miner.

Systematically tweaks pre-open cameras, marks, E/R/D, prior news,
OHLC, candles, universe lists, hold length, rank weights, and
condition exits — the same comparison shape as the paper-trading
sleeve board.

Every *input* is knowable at 09:30 ET on session D:

  * cameras / 🔵 / 🚨 from the morning packet + last completed tape
  * news tone from the morning news box, else the **prior** Finviz export
  * 20-bar OHLC and 8-bar candles with date < D
  * E/R/D via finviz_events.asof_snapshot (same-day R off; same-day E
    only if stamped ≤ 09:30)

Same-day Change%, Gap, RelVol, printed book, and headlines from a
later export are outcomes or leaks, never gates. A pick on 2026-08-17
with hold=3 is graded on 8-17 / 8-18 / 8-19.

Fills: 09:30 open → horizon close. Early exit (🚨 / last red / news🔴)
fills at that later session's 09:30 open — the first price we can act.

This is a research miner. It does not change flatten_robust live.

CLI: python -m src.factor_mine --write
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path

from . import book_era
from . import candle_factor as cf
from . import finviz_events as fe
from . import flatten_lookback_action as fla
from . import gainer_asof as ga
from . import gainer_capture as gc
from . import ohlc_ripper as ohlc
from . import sleeve_merge as sm
from . import ticker_lookback as tl
from . import ticker_lookback_cli as scan

ROOT = Path(__file__).resolve().parent.parent
OUT_JSON = ROOT / "03_scoreboard" / "factor_mine.json"
OUT_MD = ROOT / "03_scoreboard" / "FACTOR_MINE.md"
OUT_START = ROOT / "data" / "factor_mine" / "start_dates.json"
PANEL_PATH = ROOT / "data" / "factor_mine" / "panel.json"
DASH_DIR = ROOT / "dashboard" / "factor-mine"
TEMPLATE = Path(__file__).with_name("factor_mine_dash.html")
START = book_era.DASHBOARD_START
LOSER_CUT = -1.5
TOP_N_DEFAULT = 8
CAPITAL = 10_000.0
NEWS_POS = (
    "beat", "upgrade", "approv", "record high", "surge", "wins ",
    "raises", "buyback", "phase 3", "fda", "breakthrough",
)
NEWS_NEG = (
    "miss", "downgrade", "lawsuit", "probe", "dilut", "offering",
    "cuts ", "delay", "recall", "bankrupt", "fraud", "warning",
)
CAMERAS = [k for k, _ in tl.BOX_COLS]
# Feature keys matches() may read. Same-day Change%/Gap/RelVol are absent.
INPUT_FIELDS = frozenset({
    "sources", "src_rank", "boxes", "blue", "alarm", "zero_red",
    "cond_good", "cond_bad", "news_prior", "news_box", "news_export_date",
    "ohlc_ret_5", "ohlc_ret_10", "ohlc_rvol", "ohlc_hot_score",
    "ohlc_nr7", "ohlc_break_10", "last_green", "last_red",
    "candle_score", "candle_capture", "candle_body_rg",
    "erd_earn_react", "erd_days_since_E", "erd_days_since_R",
    "erd_days_since_D", "erd_flag_E", "erd_flag_R",
})
_SCAN_CACHE: dict[tuple[str, str], dict | None] = {}
_OHLC_CACHE: dict[tuple[str, str], dict] = {}
_CANDLE_CACHE: dict[tuple[str, str], dict] = {}
_EXPORT_CACHE: dict[str, dict] = {}


def _tick(v) -> str:
    return str(v or "").strip().upper()


def _finite(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def hold_window(cal: list[str], date: str, hold: int) -> list[str]:
    """Sessions covered by a 09:30 entry on ``date`` held ``hold`` sessions.

    Buy 2026-08-17 09:30 with hold=3 → 8-17, 8-18, 8-19.
    """
    if date not in cal or hold < 1:
        return []
    i = cal.index(date)
    return cal[i:i + int(hold)]


def feature_export_date(cal: list[str], date: str) -> str | None:
    """Finviz export allowed as a 09:30 *input* on ``date``.

    Always the prior session. Same-day export is tape/outcome only.
    """
    return gc.prior_session(cal, date)


def prior_news_tone(title: str | None) -> str:
    """RYG from a prior-export headline. Empty → missing."""
    text = str(title or "").strip().lower()
    if not text:
        return "missing"
    hit_pos = any(w in text for w in NEWS_POS)
    hit_neg = any(w in text for w in NEWS_NEG)
    if hit_pos and not hit_neg:
        return "good"
    if hit_neg and not hit_pos:
        return "bad"
    return "neutral"


def input_news_tone(news_box: str | None, prior_title: str | None) -> str:
    """Morning packet box wins; else prior-export headline. Never D's tape."""
    box = str(news_box or "missing").lower()
    if box != "missing":
        return box
    return prior_news_tone(prior_title)


def _news_title(df, ticker: str) -> str:
    if df is None or getattr(df, "empty", True) or "Ticker" not in df.columns:
        return ""
    if "News Title" not in df.columns:
        return ""
    hit = df.loc[df["Ticker"].astype(str).str.upper() == ticker]
    if hit.empty:
        return ""
    return str(hit.iloc[0].get("News Title") or "")


def make_recipe(name: str, *, universe: str = "union", hold: int = 1,
                side: str = "long", top_n: int = TOP_N_DEFAULT,
                require: dict | None = None, forbid: dict | None = None,
                rank: str | None = None, exit_when: dict | None = None,
                note: str = "") -> dict:
    return {
        "name": name,
        "universe": universe,
        "hold": int(hold),
        "side": side,
        "top_n": int(top_n),
        "require": dict(require or {}),
        "forbid": dict(forbid or {}),
        "rank": rank,
        "exit_when": dict(exit_when or {}),
        "note": note,
    }


def build_recipes() -> list[dict]:
    """Systematic grid: universe × hold × present/RYG/weights/exits/shorts."""
    recs: list[dict] = []

    def add(**kw):
        recs.append(make_recipe(**kw))

    universes = ("union", "flatten", "probable", "yday_gainer", "ohlc_hot")
    for uni in universes:
        for hold in (1, 3, 5):
            add(name=f"{uni}_h{hold}", universe=uni, hold=hold,
                note="baseline list, no extra gate")

    # Presence / absence and RYG on stock-ish cameras.
    gates = [
        ("vol_g", {"vol": "good"}),
        ("vol_missing", {"vol": "missing"}),
        ("ab_g", {"ab": "good"}),
        ("join_g", {"join": "good"}),
        ("join_present", {"join_present": True}),
        ("news_g", {"news": "good"}),
        ("news_present", {"news_present": True}),
        ("news_missing", {"news": "missing"}),
        ("catal_present", {"catal_present": True}),
        ("blue", {"blue": True}),
        ("white", {"zero_red": True}),
        ("last_green", {"last_green": True}),
        ("last_red", {"last_red": True}),
        ("candle", {"candle_capture": True}),
        ("coil_off", {"ret_5_min": 0.0, "ret_5_max": 10.0,
                      "rvol_min": 0.7, "rvol_max": 2.2}),
        ("earn_react", {"earn_react": True}),
        ("e_fresh", {"days_since_E_max": 1, "flag_E_min": 0}),
        ("r_up", {"days_since_R_max": 5, "flag_R": 1}),
        ("break10", {"break_10": True}),
    ]
    for gname, req in gates:
        for hold in (1, 3):
            add(name=f"union_{gname}_h{hold}", universe="union",
                hold=hold, require=req, forbid={"alarm": True},
                note=f"union ∩ {gname}, no 🚨")

    for gname in ("vol_g", "coil_off", "last_green", "news_g", "white"):
        req = next(r for n, r in gates if n == gname)
        add(name=f"union_{gname}_h5", universe="union", hold=5,
            require=req, forbid={"alarm": True},
            note=f"union ∩ {gname} hold 5, no 🚨")

    combos = [
        ("vol_ab", {"vol": "good", "ab": "good"}),
        ("blue_vol", {"vol": "good", "blue": True}),
        ("news_vol", {"news": "good", "vol": "good"}),
        ("e_green", {"earn_react": True, "last_green": True}),
        ("probable_ok", {"last_green": True, "ret_5_max": 10.0}),
        ("vol_green", {"vol": "good", "last_green": True}),
        ("coil_green", {"last_green": True, "ret_5_min": 0.0, "ret_5_max": 10.0,
                        "rvol_min": 0.7, "rvol_max": 2.2}),
        ("blue_coil", {"blue": True, "ret_5_max": 10.0}),
        ("join_vol_green", {"join": "good", "vol": "good", "last_green": True}),
        ("white_coil", {"zero_red": True, "ret_5_max": 10.0, "rvol_max": 2.2}),
    ]
    for gname, req in combos:
        uni = "probable" if gname.startswith("probable") else "union"
        for hold in (1, 3):
            add(name=f"{uni}_{gname}_h{hold}", universe=uni, hold=hold,
                require=req, forbid={"alarm": True, "news": "bad"},
                note="combo gate")

    add(name="flatten_vol_g_h3", universe="flatten", hold=3,
        require={"vol": "good"}, forbid={"alarm": True},
        note="flatten book ∩ vol🟢")
    add(name="ohlc_hot_coil_h1", universe="ohlc_hot", hold=1,
        require={"ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_max": 2.2},
        forbid={"alarm": True}, note="hot list ∩ not exploded")

    for rank in ("hot_score", "candle_score", "ret_5", "cond",
                 "w_hot_cond", "w_hot_candle"):
        add(name=f"union_{rank}_h1", universe="union", hold=1, rank=rank,
            forbid={"alarm": True}, note=f"rank by {rank}")
        add(name=f"union_{rank}_h3", universe="union", hold=3, rank=rank,
            forbid={"alarm": True}, note=f"rank by {rank}")

    add(name="union_hot_n4_h1", universe="union", hold=1, top_n=4,
        rank="hot_score", forbid={"alarm": True}, note="top 4 by hot")
    add(name="union_hot_n12_h1", universe="union", hold=1, top_n=12,
        rank="hot_score", forbid={"alarm": True}, note="top 12 by hot")
    add(name="union_cond_n4_h3", universe="union", hold=3, top_n=4,
        rank="cond", forbid={"alarm": True}, note="top 4 by cond")

    add(name="union_h3_exit_alarm", universe="union", hold=3,
        forbid={"alarm": True}, exit_when={"alarm": True},
        note="hold 3d, sell next 09:30 if 🚨")
    add(name="union_h5_exit_alarm", universe="union", hold=5,
        forbid={"alarm": True}, exit_when={"alarm": True},
        note="hold 5d, sell next 09:30 if 🚨")
    add(name="union_h3_exit_red", universe="union", hold=3,
        require={"last_green": True}, forbid={"alarm": True},
        exit_when={"last_red": True},
        note="buy last-green, sell next 09:30 if last bar flipped red")
    add(name="union_h3_exit_news_r", universe="union", hold=3,
        forbid={"alarm": True, "news": "bad"},
        exit_when={"news": "bad"},
        note="hold 3d, sell next 09:30 if news🔴")
    add(name="coil_h3_exit_alarm", universe="union", hold=3,
        require={"ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_max": 2.2},
        forbid={"alarm": True}, exit_when={"alarm": True},
        note="coil, exit on 🚨")

    shorts = [
        ("short_alarm", {"alarm": True}, "alarm"),
        ("short_news_r", {"news": "bad"}, "news🔴"),
        ("short_r_down", {"flag_R": -1, "days_since_R_max": 5}, "downgrade ≤5d"),
        ("short_extended", {"ret_5_min": 15.0}, "ret_5>15"),
        ("short_last_red", {"last_red": True}, "last bar red"),
    ]
    for name, req, note in shorts:
        for hold in (1, 3):
            add(name=f"{name}_h{hold}", universe="union", hold=hold,
                side="short", require=req, note=note)

    return recs


def _tone(boxes: dict | None, key: str) -> str:
    return str((boxes or {}).get(key) or "missing").lower()


def _cam_ok(got: str, want) -> bool:
    if want is None:
        return True
    w = str(want).lower()
    if w == "present":
        return got in ("good", "neutral", "bad")
    if w == "missing":
        return got == "missing"
    return got == w


def matches(row: dict, rec: dict) -> bool:
    uni = rec.get("universe") or "union"
    srcs = set(row.get("sources") or [])
    if uni != "union" and uni not in srcs:
        return False
    req = rec.get("require") or {}
    forb = rec.get("forbid") or {}
    boxes = row.get("boxes") or {}
    for cam in CAMERAS:
        want = req.get(cam)
        if want and not _cam_ok(_tone(boxes, cam), want):
            return False
        ban = forb.get(cam)
        if ban and _cam_ok(_tone(boxes, cam), ban):
            return False
    if req.get("blue") and not row.get("blue"):
        return False
    if req.get("zero_red") and not row.get("zero_red"):
        return False
    if forb.get("alarm") and row.get("alarm"):
        return False
    if req.get("alarm") and not row.get("alarm"):
        return False
    if req.get("last_green") and not row.get("last_green"):
        return False
    if req.get("last_red") and not row.get("last_red"):
        return False
    if req.get("candle_capture") and not row.get("candle_capture"):
        return False
    if req.get("break_10") and not row.get("ohlc_break_10"):
        return False
    if req.get("earn_react") and not row.get("erd_earn_react"):
        return False
    if req.get("news_present") and _tone(boxes, "news") == "missing":
        return False
    if req.get("join_present") and _tone(boxes, "join") == "missing":
        return False
    if req.get("catal_present") and _tone(boxes, "catal") == "missing":
        return False
    if "ret_5_min" in req:
        v = _finite(row.get("ohlc_ret_5"))
        if v is None or v < float(req["ret_5_min"]):
            return False
    if "ret_5_max" in req:
        v = _finite(row.get("ohlc_ret_5"))
        if v is None or v > float(req["ret_5_max"]):
            return False
    if "rvol_min" in req:
        v = _finite(row.get("ohlc_rvol"))
        if v is None or v < float(req["rvol_min"]):
            return False
    if "rvol_max" in req:
        v = _finite(row.get("ohlc_rvol"))
        if v is None or v > float(req["rvol_max"]):
            return False
    if "days_since_E_max" in req:
        v = row.get("erd_days_since_E")
        if v is None or int(v) > int(req["days_since_E_max"]):
            return False
    if "flag_E_min" in req:
        v = row.get("erd_flag_E")
        if v is None or int(v) < int(req["flag_E_min"]):
            return False
    if "days_since_R_max" in req:
        v = row.get("erd_days_since_R")
        if v is None or int(v) > int(req["days_since_R_max"]):
            return False
    if "flag_R" in req:
        if int(row.get("erd_flag_R") or 0) != int(req["flag_R"]):
            return False
    return True


def rank_key(row: dict, rec: dict) -> tuple:
    how = rec.get("rank")
    hot = _finite(row.get("ohlc_hot_score")) or 0.0
    candle = _finite(row.get("candle_score")) or 0.0
    cond = int(row.get("cond_good") or 0) - int(row.get("cond_bad") or 0)
    if how == "hot_score":
        return (-hot, row["ticker"])
    if how == "candle_score":
        return (-candle, row["ticker"])
    if how == "ret_5":
        return (-(_finite(row.get("ohlc_ret_5")) or 0.0), row["ticker"])
    if how == "cond":
        return (-int(row.get("cond_good") or 0), int(row.get("cond_bad") or 0),
                row["ticker"])
    if how == "w_hot_cond":
        return (-(0.6 * hot + 0.4 * max(cond, 0)), row["ticker"])
    if how == "w_hot_candle":
        return (-(0.6 * hot + 0.4 * candle), row["ticker"])
    return (int(row.get("src_rank") or 99), row["ticker"])


def pick_day(rows: list[dict], rec: dict) -> list[dict]:
    kept = [r for r in rows if matches(r, rec)]
    kept.sort(key=lambda r: rank_key(r, rec))
    return kept[: int(rec.get("top_n") or TOP_N_DEFAULT)]


def _candidates(date: str, cal: list[str], flatten_plan: dict,
                mover_by_date: dict) -> dict[str, list[str]]:
    prior = gc.prior_session(cal, date)
    return {
        "flatten": [_tick(t) for t in (flatten_plan.get("tickers") or [])],
        "probable": ohlc.continuation(prior, date, top_n=ohlc.CONT_TOP_N),
        "yday_gainer": gc.yesterday_gainers(prior, top_n=25),
        "yday_mover": gc.yesterday_movers(prior, top_n=20),
        "ohlc_hot": ohlc.liquid_hot(prior, date, top_n=30),
        "earn_react": gc.earnings_reaction(prior, date),
        "mover_buy": [_tick(t) for t in (mover_by_date.get(date) or [])][:15],
    }


def _session_map(from_date: str, to_date: str | None):
    idx = tl.build_index()
    sessions = [
        s for s in idx["sessions"]
        if s["date"] >= from_date and (not to_date or s["date"] <= to_date)
    ]
    return {s["date"]: s for s in sessions}, idx["sessions"]


def _cached_scan(sess, ticker: str):
    if sess is None:
        return None
    key = (sess["date"], ticker)
    if key not in _SCAN_CACHE:
        _SCAN_CACHE[key] = scan._scan_session(sess, ticker)
    return _SCAN_CACHE[key]


def _cached_ohlc(ticker: str, date: str) -> dict:
    key = (ticker, date)
    if key not in _OHLC_CACHE:
        _OHLC_CACHE[key] = ohlc.features(ticker, date)
    return _OHLC_CACHE[key]


def _cached_candle(ticker: str, date: str) -> dict:
    key = (ticker, date)
    if key not in _CANDLE_CACHE:
        _CANDLE_CACHE[key] = cf.features(ticker, date)
    return _CANDLE_CACHE[key]


def _export_index(date: str) -> dict:
    if date not in _EXPORT_CACHE:
        _EXPORT_CACHE[date] = fe.load_export_events(date) or {}
    return _EXPORT_CACHE[date]


def _attach_row(date: str, ticker: str, sources: list[str], src_rank: int,
                sess, prev_sess, prior_date: str | None, prior_df) -> dict:
    card = _cached_scan(sess, ticker) or {
        "date": date, "ticker": ticker,
        "boxes": {k: "missing" for k in CAMERAS},
    }
    prev = _cached_scan(prev_sess, ticker) if prev_sess else None
    days = [d for d in (prev, card) if d]
    if len(days) >= 1:
        tl.annotate_signal_improved(days)
        card = days[-1]
    boxes = {k: _tone(card.get("boxes"), k) for k in CAMERAS}
    n_red = sum(1 for k in CAMERAS if boxes[k] == "bad")
    n_good = sum(1 for k in CAMERAS if boxes[k] == "good")
    oh = _cached_ohlc(ticker, date)
    cd = _cached_candle(ticker, date)
    snap = fe.asof_snapshot(
        fe.events_for(ticker, asof=date, export_index=_export_index(date)),
        date,
    )
    news_box = boxes.get("news") or "missing"
    prior_title = _news_title(prior_df, ticker)
    prior_tone = prior_news_tone(prior_title)
    news = input_news_tone(news_box, prior_title)
    boxes["news"] = news
    bar = tl.session_bar(ticker, date)
    return {
        "date": date,
        "ticker": ticker,
        "sources": sources,
        "src_rank": src_rank,
        "boxes": boxes,
        "blue": bool(card.get("signal_improved")),
        "alarm": bool(card.get("signal_alarm")),
        "zero_red": n_red == 0 and n_good >= 1,
        "cond_good": n_good,
        "cond_bad": n_red,
        "news_prior": prior_tone,
        "news_box": news_box,
        "news_export_date": prior_date,
        "ohlc_ret_5": oh.get("ret_5"),
        "ohlc_ret_10": oh.get("ret_10"),
        "ohlc_rvol": oh.get("rvol"),
        "ohlc_hot_score": oh.get("hot_score"),
        "ohlc_nr7": bool(oh.get("nr7")),
        "ohlc_break_10": bool(oh.get("break_10")),
        "last_green": bool(oh.get("last_green") or cd.get("last_green")),
        "last_red": bool(oh.get("last_red") or cd.get("last_red")),
        "candle_score": cd.get("score"),
        "candle_capture": bool(cf.capture(cd)),
        "candle_body_rg": cd.get("body_rg"),
        "erd_earn_react": bool(snap.get("earn_react")),
        "erd_days_since_E": snap.get("days_since_E"),
        "erd_days_since_R": snap.get("days_since_R"),
        "erd_days_since_D": snap.get("days_since_D"),
        "erd_flag_E": snap.get("flag_E"),
        "erd_flag_R": snap.get("flag_R"),
        "open": (bar or {}).get("open"),
        "close": (bar or {}).get("close"),
        "prior_date": prior_date,
    }


def build_panel(from_date: str = START, to_date: str | None = None) -> dict:
    """Leak-free candidate rows for every session in the window."""
    _SCAN_CACHE.clear()
    payload = sm.load_payload()
    books = sm.list_books()
    cal = [d for d in sm.session_calendar(payload, books)
           if d >= from_date and (not to_date or d <= to_date)]
    end = to_date or (cal[-1] if cal else from_date)
    sess_map, _all_sessions = _session_map(from_date, end)
    movers = (fla.collect_mover_buys(payload, cal[0], cal[-1], top_n=15)
              if cal else {"by_date": {}})
    rows: list[dict] = []
    by_date: dict[str, list[dict]] = {}
    for date in cal:
        prior = feature_export_date(cal, date)
        # Prior export only. Same-day Finviz is never a feature.
        prior_df = ga.load_finviz(prior) if prior else None
        plan = fla.flatten_day_targets(date)
        buckets = _candidates(date, cal, plan, movers.get("by_date") or {})
        reasons: dict[str, list[str]] = {}
        order: list[str] = []
        for key, names in buckets.items():
            for t in names:
                if not t:
                    continue
                reasons.setdefault(t, [])
                if key not in reasons[t]:
                    reasons[t].append(key)
                if t not in order:
                    order.append(t)
        sess = sess_map.get(date)
        prev_sess = sess_map.get(prior) if prior else None
        day_rows = []
        for i, t in enumerate(order):
            if sess is None:
                continue
            rec = _attach_row(
                date, t, reasons[t], i, sess, prev_sess, prior, prior_df,
            )
            day_rows.append(rec)
        by_date[date] = day_rows
        rows.extend(day_rows)
        print(f"[factor-mine] panel {date} names={len(day_rows)} "
              f"total={len(rows)}", flush=True)
    return {
        "from_date": from_date,
        "to_date": end,
        "session_dates": cal,
        "n_rows": len(rows),
        "n_sessions": len(cal),
        "asof": "09:30_et",
        "leak": "prior tape + pre-open packet; news from prior export or morning box",
        "rows": rows,
        "by_date": by_date,
    }


def rehydrate_panel(raw: dict) -> dict:
    """Rebuild by_date from persisted rows if needed."""
    by_date = raw.get("by_date")
    rows = raw.get("rows") or []
    if not by_date:
        by_date = {}
        for r in rows:
            by_date.setdefault(r["date"], []).append(r)
        raw = dict(raw)
        raw["by_date"] = by_date
    return raw


def load_or_build_panel(from_date: str = START, to_date: str | None = None,
                        rebuild: bool = False) -> dict:
    if not rebuild and PANEL_PATH.exists():
        raw = json.loads(PANEL_PATH.read_text(encoding="utf-8"))
        if (raw.get("from_date") == from_date
                and raw.get("session_dates")
                and raw.get("rows")
                and (not to_date or raw.get("to_date") == to_date
                     or raw.get("to_date") >= to_date)):
            print(f"[factor-mine] loaded panel {PANEL_PATH} "
                  f"rows={raw.get('n_rows')}", flush=True)
            return rehydrate_panel(raw)
    return build_panel(from_date, to_date)


def _tapes(cal: list[str]) -> dict:
    """Same-day Change% used as *outcome* only (gainer / loser hits)."""
    gainers, losers, chg = {}, {}, {}
    for d in cal:
        df = ga.load_finviz(d)
        gainers[d] = {_tick(r["ticker"]) for r in ga.liquid_gainers(df, top_n=25)}
        chmap = ga._finviz_change_map(df)
        chg[d] = {_tick(k): float(v) for k, v in (chmap or {}).items()
                  if _tick(k) and _finite(v) is not None}
        losers[d] = {t for t, v in chg[d].items() if v < LOSER_CUT}
    return {"gainers": gainers, "losers": losers, "chg": chg}


def should_exit(row: dict, exit_when: dict | None) -> bool:
    if not exit_when:
        return False
    if exit_when.get("alarm") and row.get("alarm"):
        return True
    if exit_when.get("last_red") and row.get("last_red"):
        return True
    if exit_when.get("news") == "bad" and _tone(row.get("boxes"), "news") == "bad":
        return True
    return False


def _bar(ticker: str, date: str, bars: dict | None) -> dict:
    if bars is not None:
        return bars.get((ticker, date)) or bars.get((date, ticker)) or {}
    return tl.session_bar(ticker, date) or {}


def hold_return(ticker: str, date: str, hold: int, cal: list[str],
                side: str, exit_when: dict | None,
                row_index: dict, bars: dict | None = None) -> float | None:
    win = hold_window(cal, date, hold)
    if len(win) < 1:
        return None
    entry_bar = _bar(ticker, date, bars)
    entry = _finite(entry_bar.get("open")) or _finite(entry_bar.get("close"))
    if entry is None or entry == 0:
        return None
    exit_date = win[-1]
    early = False
    if exit_when:
        for later in win[1:]:
            nxt = row_index.get((later, ticker))
            if nxt and should_exit(nxt, exit_when):
                exit_date = later
                early = True
                break
    exit_bar = _bar(ticker, exit_date, bars)
    if early:
        px = _finite(exit_bar.get("open")) or _finite(exit_bar.get("close"))
    else:
        px = _finite(exit_bar.get("close"))
    if px is None or px == 0:
        return None
    ret = 100.0 * (px / entry - 1.0)
    if side == "short":
        ret = -ret
    return round(ret, 4)


def window_hits(ticker: str, date: str, hold: int, cal: list[str],
                tapes: dict) -> tuple[bool, bool]:
    win = hold_window(cal, date, hold)
    g = any(ticker in (tapes["gainers"].get(d) or set()) for d in win)
    lose = any(ticker in (tapes["losers"].get(d) or set()) for d in win)
    return g, lose


def score_recipe(panel: dict, rec: dict, tapes: dict,
                 bars: dict | None = None) -> dict:
    cal = list(panel.get("session_dates") or [])
    by_date = panel.get("by_date") or {}
    row_index = {(r["date"], r["ticker"]): r for r in (panel.get("rows") or [])}
    picks: list[dict] = []
    daily: list[dict] = []
    for date in cal:
        chosen = pick_day(by_date.get(date) or [], rec)
        rets = []
        for row in chosen:
            ret = hold_return(
                row["ticker"], date, rec["hold"], cal, rec["side"],
                rec.get("exit_when"), row_index, bars=bars,
            )
            g, lose = window_hits(row["ticker"], date, rec["hold"], cal, tapes)
            picks.append({
                "date": date, "ticker": row["ticker"], "ret": ret,
                "gainer": g, "loser": lose,
            })
            if ret is not None:
                rets.append(ret)
        day_mean = None if not rets else sum(rets) / len(rets)
        daily.append({
            "date": date,
            "n": len(chosen),
            "mean": None if day_mean is None else round(day_mean, 4),
            "made_money": bool(day_mean is not None and day_mean > 0),
            "tickers": [r["ticker"] for r in chosen],
        })
    graded = [p for p in picks if p["ret"] is not None]
    wins = [p for p in graded if p["ret"] > 0]
    losses = [p for p in graded if p["ret"] < 0]
    flats = [p for p in graded if p["ret"] == 0]
    win_rate = None if not graded else round(len(wins) / len(graded), 4)
    avg_win = None if not wins else round(sum(p["ret"] for p in wins) / len(wins), 3)
    avg_loss = None if not losses else round(sum(p["ret"] for p in losses) / len(losses), 3)
    days_scored = [d for d in daily if d["mean"] is not None]
    profitable_days = None if not days_scored else round(
        sum(1 for d in days_scored if d["made_money"]) / len(days_scored), 4)
    starts = []
    for i, start in enumerate(cal):
        seq = [d["mean"] for d in daily[i:] if d["mean"] is not None]
        if not seq:
            continue
        eq = 1.0
        for m in seq:
            eq *= (1.0 + float(m) / 100.0)
        ret = round(100.0 * (eq - 1.0), 3)
        starts.append({
            "start": start,
            "return_pct": ret,
            "made_money": ret > 0,
            "n_sessions": len(seq),
        })
    n_green = sum(1 for s in starts if s["made_money"])
    start_rate = None if not starts else round(n_green / len(starts), 4)
    gainer_hits = sum(1 for p in picks if p["gainer"])
    loser_hits = sum(1 for p in picks if p["loser"])
    n_picks = len(picks)
    equity = [CAPITAL]
    for d in daily:
        if d["mean"] is None:
            equity.append(equity[-1])
        else:
            equity.append(round(equity[-1] * (1.0 + d["mean"] / 100.0), 2))
    total_ret = round(100.0 * (equity[-1] / CAPITAL - 1.0), 3) if equity else 0.0
    payoff = None
    if avg_win and avg_loss:
        payoff = round(abs(avg_win / avg_loss), 3)
    effectiveness = _effectiveness(
        win_rate, profitable_days, start_rate,
        (gainer_hits / n_picks) if n_picks else None,
        (loser_hits / n_picks) if n_picks else None,
        payoff, total_ret,
    )
    return {
        "name": rec["name"],
        "universe": rec["universe"],
        "hold": rec["hold"],
        "side": rec["side"],
        "top_n": rec["top_n"],
        "rank": rec.get("rank"),
        "require": rec.get("require") or {},
        "forbid": rec.get("forbid") or {},
        "exit_when": rec.get("exit_when") or {},
        "note": rec.get("note") or "",
        "n_picks": n_picks,
        "n_graded": len(graded),
        "n_days": len(days_scored),
        "win_rate": win_rate,
        "n_wins": len(wins),
        "n_losses": len(losses),
        "n_flats": len(flats),
        "profitable_day_rate": profitable_days,
        "avg_win_pct": avg_win,
        "avg_loss_pct": avg_loss,
        "payoff": payoff,
        "gainer_hits": gainer_hits,
        "gainer_rate": None if not n_picks else round(gainer_hits / n_picks, 4),
        "loser_hits": loser_hits,
        "loser_rate": None if not n_picks else round(loser_hits / n_picks, 4),
        "start_n": len(starts),
        "start_green": n_green,
        "start_rate": start_rate,
        "total_ret_pct": total_ret,
        "final_equity": equity[-1] if equity else CAPITAL,
        "effectiveness": effectiveness,
        "daily": daily,
        "equity": equity,
        "starts": starts,
    }


def _effectiveness(win_rate, day_rate, start_rate, gainer_rate,
                   loser_rate, payoff, total_ret) -> float:
    def n(v, default=0.0):
        return default if v is None else float(v)
    score = (
        40 * n(win_rate)
        + 20 * n(day_rate)
        + 25 * n(start_rate)
        + 15 * n(gainer_rate)
        - 20 * n(loser_rate)
        + 5 * min(n(payoff, 1.0), 3.0)
        + 0.15 * n(total_ret)
    )
    return round(score, 3)


def run(from_date: str = START, to_date: str | None = None,
        write: bool = False, recipes: list[dict] | None = None,
        panel: dict | None = None, rebuild_panel: bool = False,
        persist_panel: bool = False) -> dict:
    recipes = list(recipes or build_recipes())
    panel = (panel if panel is not None
             else load_or_build_panel(from_date, to_date, rebuild=rebuild_panel))
    if persist_panel or write:
        PANEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        slim = {k: v for k, v in panel.items() if k != "by_date"}
        slim["by_date"] = None
        PANEL_PATH.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    cal = list(panel.get("session_dates") or [])
    tapes = _tapes(cal)
    stats = [score_recipe(panel, rec, tapes) for rec in recipes]
    stats.sort(key=lambda r: (-(r.get("effectiveness") or -999), r["name"]))
    dates = cal
    series = {}
    for s in stats:
        eq = s["equity"]
        series[s["name"]] = eq[1:] if len(eq) == len(dates) + 1 else eq
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "fill": "09:30 open → horizon close; early exit at next 09:30 open",
        "from_date": panel.get("from_date"),
        "to_date": panel.get("to_date"),
        "n_sessions": panel.get("n_sessions"),
        "n_rows": panel.get("n_rows"),
        "n_recipes": len(stats),
        "capital": CAPITAL,
        "loser_cut": LOSER_CUT,
        "leak": "prior tape + pre-open packet only; news from prior export or morning box",
        "live_untouched": "flatten_robust",
        "dates": dates,
        "stats": [{k: v for k, v in s.items()
                   if k not in ("daily", "equity", "starts")} for s in stats],
        "series": series,
        "daily": {s["name"]: s["daily"] for s in stats},
        "starts": {s["name"]: s["starts"] for s in stats},
        "recipes": recipes,
        "panel_n": panel.get("n_rows"),
    }
    if write:
        write_outputs(payload, stats)
    return payload


def write_outputs(payload: dict, stats: list[dict] | None = None) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_START.parent.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    starts = {
        "generated_at": payload.get("generated_at"),
        "rows": [
            {
                "name": s["name"],
                "start_green": s.get("start_green"),
                "start_n": s.get("start_n"),
                "start_rate": s.get("start_rate"),
            }
            for s in (stats or [])
        ],
    }
    OUT_START.write_text(json.dumps(starts, indent=2), encoding="utf-8")
    lines = [
        f"# Factor strategy mine — {payload.get('from_date')} → {payload.get('to_date')}",
        "",
        f"Leak-free 09:30 recipes: **{payload.get('n_recipes')}** · "
        f"candidate rows **{payload.get('n_rows')}** · "
        f"fill `{payload.get('fill')}`.",
        "",
        "Research only — does not change live `flatten_robust`.",
        "",
        "| Strategy | Side | H | Win% | $ days | Starts YES | "
        "Top-g | Losers | Avg win | Avg loss | Tot% | Eff |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for s in (stats or []):
        lines.append(
            f"| `{s['name']}` | {s['side']} | {s['hold']} | "
            f"{_pct(s.get('win_rate'))} | {_pct(s.get('profitable_day_rate'))} | "
            f"{s.get('start_green') or 0}/{s.get('start_n') or 0} | "
            f"{s.get('gainer_hits') or 0} | {s.get('loser_hits') or 0} | "
            f"{_n(s.get('avg_win_pct'))} | {_n(s.get('avg_loss_pct'))} | "
            f"{_n(s.get('total_ret_pct'))} | {s.get('effectiveness')} |"
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if TEMPLATE.is_file():
        html = TEMPLATE.read_text(encoding="utf-8").replace(
            "__DATA__", json.dumps(payload, separators=(",", ":")))
        (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def _pct(v) -> str:
    return "—" if v is None else f"{100 * float(v):.0f}%"


def _n(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default=START)
    ap.add_argument("--to-date", default="")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--rebuild-panel", action="store_true")
    args = ap.parse_args(argv)
    payload = run(
        args.from_date, args.to_date or None, write=args.write,
        rebuild_panel=args.rebuild_panel, persist_panel=args.write,
    )
    print(f"[factor-mine] recipes={payload['n_recipes']} "
          f"rows={payload['n_rows']} sessions={payload['n_sessions']}")
    for s in (payload.get("stats") or [])[:8]:
        print(f"  {s['name']:32s}  win={_pct(s.get('win_rate'))}  "
              f"starts={s.get('start_green')}/{s.get('start_n')}  "
              f"tot={_n(s.get('total_ret_pct'))}%  eff={s.get('effectiveness')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
