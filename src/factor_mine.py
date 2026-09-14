"""Leak-free 09:30 factor strategy miner.

Systematically tweaks pre-open cameras, marks, E/R/D, prior news,
OHLC, candles, universe lists, hold length, rank weights, and
condition exits — the same comparison shape as the paper-trading
sleeve board.

Every *input* is knowable at 09:30 ET on session D:

  * cameras / 🔵 / 🚨 from the morning packet + last completed tape
  * news tone from the morning news box, else the **prior** Finviz export
  * 20-bar OHLC and 8-bar candles with date < D
  * RSI / MACD / flow-in from prior bars (60-bar lookback) plus prior-export
    Finviz RSI(14) / Rel Volume / SMA% / institutional transactions
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
import base64
import gzip
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
SIM_JS = Path(__file__).with_name("factor_mine_sim.js")
START = book_era.DASHBOARD_START
LOSER_CUT = -1.5
TOP_N_DEFAULT = 8
CAPITAL = 10_000.0
MIN_GRADED = 20
MIN_STARTS = 8
MIN_DAYS = 5
POTHOLE_CUT = 30.0  # one session's mean % that dominates the path
NEWS_POS = (
    "beat", "upgrade", "approv", "record high", "surge", "wins ",
    "raises", "buyback", "phase 3", "fda", "breakthrough",
    "director buys", "insider buy", "buys shares", "buys stock",
    "purchases shares", "form 4",
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
    "ohlc_ret_1", "ohlc_ret_5", "ohlc_ret_10", "ohlc_rvol", "ohlc_hot_score",
    "ohlc_nr7", "ohlc_break_10", "last_green", "last_red",
    "candle_score", "candle_capture", "candle_body_rg",
    "erd_earn_react", "erd_days_since_E", "erd_days_since_R",
    "erd_days_since_D", "erd_flag_E", "erd_flag_R",
    "e_pol", "e_label",
    "rsi", "fv_rsi", "macd", "macd_sig", "macd_hist",
    "macd_cross_up", "macd_cross_down", "rsi_os", "rsi_ob",
    "macd_up", "macd_down", "flow_in", "close_loc",
    "fv_rvol", "fv_sma20", "fv_sma50", "fv_inst",
})
_SCAN_CACHE: dict[tuple[str, str], dict | None] = {}
_OHLC_CACHE: dict[tuple[str, str], dict] = {}
_CANDLE_CACHE: dict[tuple[str, str], dict] = {}
_EXPORT_CACHE: dict[str, dict] = {}
_PLAN: dict | None = None


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


def _headline_insider_buy(text: str) -> bool:
    """Director / insider open-market buy — not 'buyback' and not a downgrade."""
    if any(w in text for w in ("downgrade", "lawsuit", "dilut", "offering")):
        return False
    people = any(w in text for w in ("director", "insider", "form 4", "form-4",
                                     "ceo ", "cfo ", "board"))
    bought = any(w in text for w in ("buy", "purchase", "acquire"))
    return people and bought


def prior_news_tone(title: str | None) -> str:
    """RYG from a prior-export headline. Empty → missing."""
    text = str(title or "").strip().lower()
    if not text:
        return "missing"
    hit_pos = any(w in text for w in NEWS_POS) or _headline_insider_buy(text)
    hit_neg = any(w in text for w in NEWS_NEG)
    if hit_pos and not hit_neg:
        return "good"
    if hit_neg and not hit_pos:
        return "bad"
    return "neutral"


def input_news_tone(news_box: str | None, prior_title: str | None) -> str:
    """Packet red stays red. A clearly green headline can lift a yellow box."""
    box = str(news_box or "missing").lower()
    prior = prior_news_tone(prior_title)
    if box == "bad":
        return "bad"
    if box == "good" or prior == "good":
        return "good"
    if box == "neutral":
        return "neutral"
    return prior


def _news_title(df, ticker: str) -> str:
    if df is None or getattr(df, "empty", True) or "Ticker" not in df.columns:
        return ""
    if "News Title" not in df.columns:
        return ""
    hit = df.loc[df["Ticker"].astype(str).str.upper() == ticker]
    if hit.empty:
        return ""
    return str(hit.iloc[0].get("News Title") or "")


FV_RSI = "Relative Strength Index (14)"
FV_RVOL = "Relative Volume"
FV_SMA20 = "20-Day Simple Moving Average"
FV_SMA50 = "50-Day Simple Moving Average"
FV_INST = "Institutional Transactions"


def _fv_num(v):
    """Parse a Finviz number or percent string. Same rules as ticker_lookback."""
    return tl._num(v)


def _finviz_snap(df, ticker: str) -> dict:
    """Prior-export tape snapshot. Empty dict fields stay None — never same-day."""
    empty = {
        "fv_rsi": None, "fv_rvol": None, "fv_sma20": None,
        "fv_sma50": None, "fv_inst": None,
    }
    if df is None or getattr(df, "empty", True):
        return empty
    cols = getattr(df, "columns", [])
    if "Ticker" not in cols:
        return empty
    hit = df.loc[df["Ticker"].astype(str).str.upper() == _tick(ticker)]
    if hit.empty:
        return empty
    rec = hit.iloc[0]

    def col(name: str):
        return rec[name] if name in rec.index else None

    return {
        "fv_rsi": _fv_num(col(FV_RSI)),
        "fv_rvol": _fv_num(col(FV_RVOL)),
        "fv_sma20": _fv_num(col(FV_SMA20)),
        "fv_sma50": _fv_num(col(FV_SMA50)),
        "fv_inst": _fv_num(col(FV_INST)),
    }


def apply_tape_fields(row: dict, oh: dict | None = None,
                      fv: dict | None = None) -> dict:
    """Stamp leak-free RSI / MACD / flow onto a panel row. Mutates ``row``.

    RSI prefers the prior Finviz Elite print when present, else Wilder
    RSI on bars with date < D. MACD is computed (not in the export).
    flow_in = prior rel-vol high while |1-day %| is small.
    """
    oh = oh or {}
    fv = fv or {}
    fv_rsi = _finite(fv.get("fv_rsi"))
    rsi = fv_rsi if fv_rsi is not None else _finite(oh.get("rsi"))
    if rsi is None:
        rsi = _finite(row.get("rsi"))
    macd = _finite(oh.get("macd"))
    if macd is None:
        macd = _finite(row.get("macd"))
    macd_sig = _finite(oh.get("macd_sig"))
    if macd_sig is None:
        macd_sig = _finite(row.get("macd_sig"))
    macd_hist = _finite(oh.get("macd_hist"))
    if macd_hist is None:
        macd_hist = _finite(row.get("macd_hist"))
    rvol = _finite(oh.get("rvol"))
    if rvol is None:
        rvol = _finite(row.get("ohlc_rvol"))
    if rvol is None:
        rvol = _finite(fv.get("fv_rvol"))
    ret1 = _finite(oh.get("ret_1"))
    if ret1 is None:
        ret1 = _finite(row.get("ohlc_ret_1"))
    xup = oh.get("macd_cross_up")
    if xup is None:
        xup = row.get("macd_cross_up")
    xdn = oh.get("macd_cross_down")
    if xdn is None:
        xdn = row.get("macd_cross_down")
    row["rsi"] = None if rsi is None else round(float(rsi), 2)
    row["fv_rsi"] = None if fv_rsi is None else round(float(fv_rsi), 2)
    row["macd"] = macd
    row["macd_sig"] = macd_sig
    row["macd_hist"] = macd_hist
    row["macd_cross_up"] = bool(xup)
    row["macd_cross_down"] = bool(xdn)
    row["rsi_os"] = bool(rsi is not None and rsi <= ohlc.RSI_OS)
    row["rsi_ob"] = bool(rsi is not None and rsi >= ohlc.RSI_OB)
    row["macd_up"] = bool(macd_hist is not None and macd_hist > 0)
    row["macd_down"] = bool(macd_hist is not None and macd_hist < 0)
    row["flow_in"] = bool(
        rvol is not None and ret1 is not None
        and float(rvol) >= ohlc.FLOW_RVOL
        and abs(float(ret1)) <= ohlc.FLOW_RET_MAX
    )
    row["close_loc"] = _finite(oh.get("close_loc"))
    if row["close_loc"] is None:
        row["close_loc"] = _finite(row.get("close_loc"))
    row["fv_rvol"] = _finite(fv.get("fv_rvol"))
    row["fv_sma20"] = _finite(fv.get("fv_sma20"))
    row["fv_sma50"] = _finite(fv.get("fv_sma50"))
    row["fv_inst"] = _finite(fv.get("fv_inst"))
    return row


def attach_tape_flow(panel: dict) -> dict:
    """Fill RSI / MACD / flow on an already-built panel. Prior tape only."""
    if not isinstance(panel, dict):
        return panel
    panel = rehydrate_panel(panel)
    rows = panel.get("rows") or []
    cal = list(panel.get("session_dates") or [])
    fv_by_date: dict[str, object] = {}
    for r in rows:
        if ("rsi" in r and "flow_in" in r and "macd_hist" in r
                and r.get("rsi_os") is not None):
            continue
        t = _tick(r.get("ticker"))
        d = str(r.get("date") or "")[:10]
        if not t or not d:
            continue
        oh = _cached_ohlc(t, d)
        prior = (r.get("prior_date") or r.get("news_export_date")
                 or feature_export_date(cal, d))
        if prior and prior not in fv_by_date:
            fv_by_date[prior] = ga.load_finviz(prior)
        fv = _finviz_snap(fv_by_date.get(prior) if prior else None, t)
        apply_tape_fields(r, oh, fv)
        if r.get("ohlc_ret_1") is None and _finite(oh.get("ret_1")) is not None:
            r["ohlc_ret_1"] = oh.get("ret_1")
        if r.get("ohlc_rvol") is None and _finite(oh.get("rvol")) is not None:
            r["ohlc_rvol"] = oh.get("rvol")
    panel["_tape_filled"] = True
    return panel


def make_recipe(name: str, *, universe: str = "union", hold: int = 1,
                side: str = "long", top_n: int = TOP_N_DEFAULT,
                require: dict | None = None, forbid: dict | None = None,
                rank: str | None = None, exit_when: dict | None = None,
                size: str = "leftover", sell: str = "list",
                s_boost: str = "none", day_cap: float = 1.0,
                take_pct: float | None = None, stop_pct: float | None = None,
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
        "size": size or "leftover",
        "sell": sell or "list",
        "s_boost": s_boost or "none",
        "day_cap": float(day_cap),
        "take_pct": take_pct,
        "stop_pct": stop_pct,
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
        ("rsi_os", {"rsi_os": True}),
        ("macd_up", {"macd_up": True}),
        ("macd_xup", {"macd_cross_up": True}),
        ("flow_in", {"flow_in": True}),
    ]
    for gname, req in gates:
        for hold in (1, 3):
            add(name=f"union_{gname}_h{hold}", universe="union",
                hold=hold, require=req, forbid={"alarm": True},
                note=f"union ∩ {gname}, no 🚨")

    for gname in ("vol_g", "coil_off", "last_green", "news_g", "white",
                  "rsi_os", "flow_in"):
        req = next(r for n, r in gates if n == gname)
        add(name=f"union_{gname}_h5", universe="union", hold=5,
            require=req, forbid={"alarm": True},
            note=f"union ∩ {gname} hold 5, no 🚨")

    # News packet / headline as first-class gates. Green is OR
    # (packet🟢 or headline🟢). Camera filter is a positive-enough
    # +G −R net — +9 −1 was only an example, not the gate.
    for hold in (1, 3):
        add(name=f"union_news_pack_h{hold}", universe="union", hold=hold,
            require={"news_box": "good"}, forbid={"alarm": True},
            rank="cond", note="morning packet news🟢 only (not the merged box)")
        add(name=f"union_news_head_h{hold}", universe="union", hold=hold,
            require={"headline": "good"}, forbid={"alarm": True},
            rank="cond", note="prior-export headline🟢 only")
        add(name=f"union_news_or_h{hold}", universe="union", hold=hold,
            require={"news_or_headline": True}, forbid={"alarm": True},
            rank="cond", note="packet🟢 OR headline🟢")
        add(name=f"union_news_both_h{hold}", universe="union", hold=hold,
            require={"news_and_headline": True}, forbid={"alarm": True},
            rank="cond", note="packet🟢 AND headline🟢 (thin; kept as a KILL)")
        add(name=f"union_news_g_cond_h{hold}", universe="union", hold=hold,
            require={"news": "good"}, forbid={"alarm": True},
            rank="cond", note="merged news🟢, rank +G−R")
        add(name=f"union_news_g_cam71_h{hold}", universe="union", hold=hold,
            require={"news": "good", "n_pos_min": 7, "cam_bad_max": 1},
            forbid={"alarm": True}, rank="cond",
            note="merged news🟢 and cameras +7 −≤1")
        add(name=f"union_news_g_cam61_h{hold}", universe="union", hold=hold,
            require={"news": "good", "n_pos_min": 6, "cam_bad_max": 1},
            forbid={"alarm": True}, rank="cond",
            note="merged news🟢 and cameras +6 −≤1")
        for net in (2, 3, 4, 5):
            add(name=f"union_news_or_net{net}_h{hold}", universe="union",
                hold=hold, require={"news_or_headline": True, "cam_net_min": net},
                forbid={"alarm": True}, rank="cond",
                note=f"packet🟢 OR headline🟢 and camera net ≥ {net}")
        add(name=f"union_news_pack_net3_h{hold}", universe="union", hold=hold,
            require={"news_box": "good", "cam_net_min": 3},
            forbid={"alarm": True}, rank="cond",
            note="packet🟢 and camera net ≥ 3")
        add(name=f"union_news_pack_net2_h{hold}", universe="union", hold=hold,
            require={"news_box": "good", "cam_net_min": 2},
            forbid={"alarm": True}, rank="cond",
            note="packet🟢 and camera net ≥ 2")
    add(name="union_news_or_net4_rw_h1", universe="union", hold=1, top_n=4,
        require={"news_or_headline": True, "cam_net_min": 4},
        forbid={"alarm": True}, rank="cond", size="rank_w",
        note="OR news + net≥4; leftover weighted by camera rank")
    add(name="union_news_or_net4_conv_h1", universe="union", hold=1, top_n=4,
        require={"news_or_headline": True, "cam_net_min": 4},
        forbid={"alarm": True}, rank="cond", size="conviction",
        note="OR news + net≥4; 70% leftover if #1 net ≥ 5")
    add(name="union_news_g_cam91_n1_h1", universe="union", hold=1, top_n=1,
        require={"news": "good", "n_pos_min": 9, "cam_bad_max": 1},
        forbid={"alarm": True}, rank="cond",
        note="all leftover on the rare news🟢 +9 −≤1 name (KILL example)")
    add(name="union_news_g_cam71_n2_h1", universe="union", hold=1, top_n=2,
        require={"news": "good", "n_pos_min": 7, "cam_bad_max": 1},
        forbid={"alarm": True}, rank="cond", size="topheavy",
        note="topheavy leftover on news🟢 +7 −≤1")
    add(name="union_news_g_conv_h1", universe="union", hold=1, top_n=4,
        require={"news": "good"}, forbid={"alarm": True},
        rank="cond", size="conviction",
        note="merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5")
    add(name="union_news_g_conv_h3", universe="union", hold=3, top_n=4,
        require={"news": "good"}, forbid={"alarm": True},
        rank="cond", size="conviction",
        note="merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5")
    add(name="union_news_g_cam71_conv_h1", universe="union", hold=1, top_n=4,
        require={"news": "good", "n_pos_min": 7, "cam_bad_max": 1},
        forbid={"alarm": True}, rank="cond", size="conviction",
        note="news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5")
    add(name="short_news_pack_h3", universe="union", hold=3, side="short",
        require={"news_box": "bad"}, note="short morning packet news🔴")
    add(name="short_news_head_h3", universe="union", hold=3, side="short",
        require={"headline": "bad"}, note="short prior-export headline🔴")
    add(name="short_news_or_h3", universe="union", hold=3, side="short",
        require={"news_or_red": True}, note="short packet🔴 OR headline🔴")

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
        ("rsi_os_macd", {"rsi_os": True, "macd_up": True}),
        ("flow_in_white", {"flow_in": True, "zero_red": True}),
    ]
    for gname, req in combos:
        uni = "probable" if gname.startswith("probable") else "union"
        for hold in (1, 3):
            add(name=f"{uni}_{gname}_h{hold}", universe=uni, hold=hold,
                require=req, forbid={"alarm": True, "news": "bad"},
                note="combo gate")

    add(name="flatten_vol_g_h3", universe="flatten", hold=3,
        require={"vol": "good"}, forbid={"alarm": True},
        note="flatten wish-list ∩ vol🟢")
    for hold in (1, 3, 5):
        add(name=f"flatten_live_h{hold}", universe="flatten", hold=hold,
            require={"live_entry": True},
            note="09:30 tickets only when flatten_robust gate fires (mover)")
    add(name="ohlc_hot_coil_h1", universe="ohlc_hot", hold=1,
        require={"ret_5_min": 0.0, "ret_5_max": 10.0, "rvol_max": 2.2},
        forbid={"alarm": True}, note="hot list ∩ not exploded")

    for rank in ("hot_score", "candle_score", "ret_5", "cond",
                 "w_hot_cond", "w_hot_candle", "rsi", "macd_hist"):
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
        ("short_rsi_ob", {"rsi_ob": True}, "RSI overbought"),
        ("short_macd_dn", {"macd_down": True}, "MACD histogram < 0"),
    ]
    for name, req, note in shorts:
        for hold in (1, 3):
            add(name=f"{name}_h{hold}", universe="union", hold=hold,
                side="short", require=req, note=note)

    # Cash-state tweaks on a few proven bases — not a full cartesian.
    bases = [
        dict(name="flatten_h5", universe="flatten", hold=5),
        dict(name="flatten_h3", universe="flatten", hold=3),
        dict(name="flatten_live_h1", universe="flatten", hold=1,
             require={"live_entry": True}),
        dict(name="union_h5", universe="union", hold=5),
        dict(name="union_h3", universe="union", hold=3),
        dict(name="union_h1", universe="union", hold=1),
    ]
    tweaks = [
        ("rankw", dict(size="rank_w", note="rank-weighted leftover")),
        ("topheavy", dict(size="topheavy", note="40% to #1, rest split")),
        ("half", dict(size="half", note="deploy half leftover")),
        ("time", dict(sell="time", note="sell at min-hold even if still listed")),
        ("cut", dict(sell="cut_loser", note="after min-hold, cut −3% losers")),
        ("trail", dict(sell="trail", note="after min-hold, trail 5% off peak")),
        ("sboost", dict(s_boost="both", note="S≥+5: sizeup + more names")),
        ("sizeup", dict(s_boost="sizeup", note="S≥+5: 1.35× leftover")),
    ]
    have = {r["name"] for r in recs}
    for base in bases:
        for suffix, kw in tweaks:
            nm = f"{base['name']}_{suffix}"
            if nm in have:
                continue
            add(name=nm, universe=base["universe"], hold=base["hold"],
                require=base.get("require"),
                size=kw.get("size", "leftover"),
                sell=kw.get("sell", "list"),
                s_boost=kw.get("s_boost", "none"),
                note=kw["note"])
            have.add(nm)

    # Same looker list as the union / flatten books; pick is 0 red cameras
    # plus yesterday's session up, ranked by green−red (aggregate score).
    add(name="union_white_yday_h1", universe="union", hold=1, rank="cond",
        require={"zero_red": True, "yday_up": True},
        forbid={"alarm": True},
        note="union looker: 0 red cameras + yesterday up, rank +G−R")
    add(name="union_white_yday_h3", universe="union", hold=3, rank="cond",
        require={"zero_red": True, "yday_up": True},
        forbid={"alarm": True},
        note="union looker hold 3: 0 red + yesterday up")
    add(name="flatten_white_yday_h5", universe="flatten", hold=5, rank="cond",
        require={"zero_red": True, "yday_up": True},
        forbid={"alarm": True},
        note="flatten looker: 0 red + yesterday up")

    # Pool first: −0 red cameras + (yesterday up OR major catalyst).
    # Rank by morning-board Score (100 − list rank) only after that pool.
    for hold in (1, 2, 3, 5):
        add(name=f"union_white_any_h{hold}", universe="union", hold=hold,
            rank="list",
            require={"cam_bad_max": 0, "yday_or_catalyst": True},
            forbid={"alarm": True},
            note="−0 red + (yday up or major catalyst), then Score")
    # Same Score-after-pool, but the name must have yday-up AND a catalyst,
    # then take only the top 4. That is the cash-book winner on 08-13→09-11.
    for hold in (1, 2, 3, 5):
        add(name=f"union_white_both_n4_h{hold}", universe="union", hold=hold,
            top_n=4, rank="list",
            require={"cam_bad_max": 0, "yday_and_catalyst": True},
            forbid={"alarm": True},
            note="−0 red + yday up AND catalyst, top 4 by Score")
    # Stop-only brackets that beat the plain hold on 08-13→09-11.
    # Take-profit cut the runners; do not register those.
    add(name="union_white_both_n4_h5_s12", universe="union", hold=5,
        top_n=4, rank="list", stop_pct=0.12,
        require={"cam_bad_max": 0, "yday_and_catalyst": True},
        forbid={"alarm": True},
        note="both+top4 hold5, stop −12% at 09:30 even inside hold")
    add(name="flatten_h5_s8", universe="flatten", hold=5, stop_pct=0.08,
        note="flatten hold 5, stop −8% at 09:30 even inside hold")

    return recs


# Plain-language labels for the 09:30 cameras and gates. These are the
# *inputs* a recipe may read — never same-day Change% / Gap / RelVol.
_UNI_KID = {
    "union": "the mixed morning shopping list (every name that showed up on any 09:30 list that day)",
    "flatten": "the flatten wish-list (names the flatten board wanted that morning)",
    "probable": "yesterday's 'likely to keep moving' list",
    "yday_gainer": "yesterday's top liquid winners",
    "ohlc_hot": "names that looked hot on the prior price/volume tape",
    "combo": "several existing sleeves sharing one $10k book (each kid still uses its own 09:30 list)",
}
_CAM_KID = {
    "vol": "the volume camera (is this name unusually active?)",
    "news": "the news camera (does the morning packet like the headline?)",
    "ab": "the A/B camera (does our A/B score like this name?)",
    "join": "the join camera (do several factors agree?)",
    "catal": "the catalyst camera (is there a known event?)",
    "buy": "the overnight-buy camera",
    "peer": "the peer camera (are cousins doing the same thing?)",
    "digest": "the digest camera",
    "judge": "the judge camera",
    "sector": "the sector camera",
    "gen": "the general-condition camera",
    "heat": "the heat camera",
}
_RANK_KID = {
    "hot_score": "how hot the prior tape looked",
    "candle_score": "how clean the prior candles looked",
    "ret_5": "the prior 5-session return (bigger first)",
    "cond": "how many morning cameras are green vs red",
    "w_hot_cond": "a mix of tape-heat and green cameras",
    "w_hot_candle": "a mix of tape-heat and prior candles",
    "rsi": "how oversold the prior RSI is (lower first)",
    "macd_hist": "how positive the prior MACD histogram is",
    "list": "the morning-board Score (100 minus list rank) — only after the pool is chosen",
    "score": "the morning-board Score (100 minus list rank) — only after the pool is chosen",
}


def _gate_kid(key: str, val) -> str:
    if key == "live_entry":
        return "the live flatten gate must say GO (green morning S, enough priced BUYs, prior book) — io/HOLD mornings sit"
    if key == "blue":
        return "the name is painted 🔵 (a turn higher on a still-red row)"
    if key == "zero_red":
        return "no morning camera is red (the 'white' / all-clear row)"
    if key == "alarm":
        return "the 🚨 alarm is on (cameras got worse overnight)"
    if key == "last_green":
        return "the last finished bar was green (closed up)"
    if key == "last_red":
        return "the last finished bar was red (closed down)"
    if key == "candle_capture":
        return "the prior-candle capture flag is on"
    if key == "break_10":
        return "the name broke its prior 10-session range"
    if key == "earn_react":
        return "the name is in an earnings-reaction window (just reported, we are trading the reaction — not today's print)"
    if key == "news_present":
        return "the news camera printed something (any color, not blank)"
    if key == "join_present":
        return "the join camera printed something (any color, not blank)"
    if key == "catal_present":
        return "the catalyst camera printed something (any color, not blank)"
    if key == "yday_up":
        return "yesterday's session was up (prior close-to-close Change% > 0, or last finished bar green if the % is missing)"
    if key == "cam_bad_max":
        return (
            f"at most {int(val)} red cameras (the −R half of +G −R; "
            "🚨 is not counted here)"
        )
    if key == "yday_or_catalyst":
        return (
            "yesterday's session was up, or a major good catalyst "
            "(EPS beat / catal green / earnings-react that is not a miss)"
        )
    if key == "yday_and_catalyst":
        return "yesterday's session was up AND a major good catalyst"
    if key == "major_catalyst":
        return (
            "a major good catalyst (EPS beat, catal green, or "
            "earnings-react that is not a miss)"
        )
    if key == "n_neg_max":
        return f"at most {int(val)} red cameras (the −N next to the name)"
    if key == "n_neg_min":
        return f"at least {int(val)} red cameras"
    if key == "n_pos_min":
        return f"at least {int(val)} green cameras (the +G half of +G −R)"
    if key == "news_box":
        tone = {"good": "green", "bad": "red"}.get(val, str(val))
        return f"the morning news packet box is {tone}"
    if key == "headline":
        tone = {"good": "green", "bad": "red"}.get(val, str(val))
        return f"the prior-export headline is {tone}"
    if key == "news_and_headline":
        return "the morning news packet AND the prior-export headline are both green"
    if key == "news_or_headline":
        return "the morning news packet OR the prior-export headline is green"
    if key == "news_or_red":
        return "the morning news packet OR the prior-export headline is red"
    if key == "cam_net_min":
        return f"camera net (+G −R) is at least {int(val)}"
    if key == "rsi_os":
        return "prior RSI is oversold (≤30) — Finviz prior export, else computed on prior bars"
    if key == "rsi_ob":
        return "prior RSI is overbought (≥70)"
    if key == "macd_up":
        return "prior MACD histogram is above zero (momentum still up)"
    if key == "macd_down":
        return "prior MACD histogram is below zero (momentum still down)"
    if key == "macd_cross_up":
        return "MACD histogram just crossed from ≤0 to >0 on the last finished bar"
    if key == "flow_in":
        return "money came in (prior rel vol ≥ 1.5) but price barely moved (|1-day| ≤ 1.2%)"
    if key == "rsi_min":
        return f"prior RSI is at least {float(val):g}"
    if key == "rsi_max":
        return f"prior RSI is at most {float(val):g} (not already stretched)"
    if key == "burst":
        return "a parabolic / high-intensity prior tape (ret5≥12, last green, and a 10-session break or rvol≥2)"
    if key == "ret_5_min":
        return f"prior 5-session return is at least {float(val):g}%"
    if key == "ret_5_max":
        return f"prior 5-session return is at most {float(val):g}% (not already exploded)"
    if key == "rvol_min":
        return f"prior relative volume is at least {float(val):g}"
    if key == "rvol_max":
        return f"prior relative volume is at most {float(val):g} (not a blow-off)"
    if key == "days_since_E_max":
        return f"earnings (E) printed within the last {int(val)} session(s)"
    if key == "flag_E_min":
        return "the earnings flag is on"
    if key == "days_since_R_max":
        return f"an analyst revision (R) printed within the last {int(val)} session(s)"
    if key == "flag_R":
        if int(val) == 1:
            return "the latest revision flag is an upgrade"
        if int(val) == -1:
            return "the latest revision flag is a downgrade"
        return f"the revision flag equals {val}"
    if key in _CAM_KID:
        tone = {True: "green", False: "off", "good": "green", "bad": "red",
                "neutral": "yellow", "missing": "blank"}.get(val, str(val))
        return f"{_CAM_KID[key]} is {tone}"
    return f"{key} = {val}"


def explain_recipe(rec: dict) -> dict:
    """Kid-plain rules for one sleeve: inputs, buy, sell. No black box."""
    rec = rec or {}
    if rec.get("universe") == "combo" or rec.get("members"):
        from . import factor_mine_combo as fmc
        spec = {
            "name": rec.get("name"),
            "members": list(rec.get("members") or []),
            "weights": list(rec.get("weights") or [1]),
            "net": rec.get("net") or "priority",
            "pool": rec.get("pool") or "shared",
        }
        if spec["members"]:
            return fmc.explain_combo(spec)
    uni = rec.get("universe") or "union"
    hold = int(rec.get("hold") or 1)
    side = rec.get("side") or "long"
    top_n = int(rec.get("top_n") or TOP_N_DEFAULT)
    req = {k: v for k, v in (rec.get("require") or {}).items()
           if k != "live_entry" or v}
    forb = dict(rec.get("forbid") or {})
    live = bool((rec.get("require") or {}).get("live_entry"))
    rank = rec.get("rank")
    size = rec.get("size") or "leftover"
    sell_mode = rec.get("sell") or "list"
    boost = rec.get("s_boost") or "none"
    exit_when = rec.get("exit_when") or {}
    short = side == "short"

    inputs = [
        f"Shopping list: {_UNI_KID.get(uni, uni)}.",
        "Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.",
        "News, if used, is the morning packet box or yesterday's headline — never a later scrape.",
        "Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.",
        "Fill price: the 09:30 open, whole shares, Futubull fees.",
        "Morning weather S: if S ≤ −3 the sleeve sits (no new buys).",
    ]
    if live:
        inputs.append(
            "Live flatten gate: new buys only when flatten_robust would actually send 09:30 tickets."
        )
    if rank:
        inputs.append(f"Sort: {_RANK_KID.get(rank, rank)}.")
    for k, v in req.items():
        if k == "live_entry":
            continue
        inputs.append(f"Must-have: {_gate_kid(k, v)}.")
    for k, v in forb.items():
        inputs.append(f"Must-not: {_gate_kid(k, v)}.")

    buy = [
        f"At 09:30, take names on {_UNI_KID.get(uni, uni)} that pass the must-haves.",
    ]
    if live:
        buy.append(
            "If the live flatten gate is HOLD / io that morning, buy nobody new."
        )
    buy.append("If morning S ≤ −3, buy nobody new (hard-red sit).")
    if req:
        buy.append("A name is allowed only when every must-have is true.")
    if forb:
        buy.append("A name is thrown out if any must-not is true.")
    if rank:
        buy.append(
            f"Sort the keepers by {_RANK_KID.get(rank, rank)} and keep the top {top_n}."
        )
    else:
        buy.append(f"Keep the first {top_n} names in list order.")
    if size == "rank_w":
        buy.append("Split leftover cash by rank (first name gets the biggest slice).")
    elif size == "topheavy":
        buy.append("Give about 40% of leftover cash to the first name; split the rest.")
    elif size == "half":
        buy.append("Only spend half of leftover cash; the rest stays cash.")
    else:
        buy.append("Split leftover cash equally across *new* names (not ones we already hold).")
    buy.append("Skip a name if the slice cannot buy 1 share after fees.")
    if boost == "sizeup":
        buy.append("On a strong morning (S ≥ +5), spend 1.35× leftover — still capped by cash.")
    elif boost == "more_names":
        buy.append(f"On a strong morning (S ≥ +5), raise the name cap by 4 (still cash-capped).")
    elif boost == "both":
        buy.append(
            "On a strong morning (S ≥ +5), spend 1.35× leftover and add 4 extra names — still cash-capped."
        )
    if short:
        buy.append(
            "This is a SHORT sleeve: it borrows the name and profits if the price falls. "
            "Equity treats the short as a liability (must keep enough to cover)."
        )
    else:
        buy.append("This is a LONG sleeve: it buys shares and wants the price to go up.")

    sell_bits = [
        "Sell first, then buy. Never sell a ticker we do not hold.",
        f"Minimum hold is {hold} session(s) — the buy morning counts as 1.",
    ]
    if exit_when.get("alarm"):
        sell_bits.append("Early exit: sell at the next 09:30 if 🚨 prints, even inside the minimum hold.")
    if exit_when.get("last_red"):
        sell_bits.append("Early exit: sell at the next 09:30 if the last bar flipped red, even inside the floor.")
    if exit_when.get("news") == "bad":
        sell_bits.append("Early exit: sell at the next 09:30 if the news camera turns red, even inside the floor.")
    take = _finite(rec.get("take_pct"))
    stop = _finite(rec.get("stop_pct"))
    if take and take > 0:
        sell_bits.append(
            f"Take-profit: sell at the next 09:30 if that open is {100 * take:g}% "
            "better than our fill, even inside the minimum hold. "
            "This is open vs our entry — not today's Change%."
        )
    if stop and stop > 0:
        sell_bits.append(
            f"Stop-loss: sell at the next 09:30 if that open is {100 * stop:g}% "
            "worse than our fill, even inside the minimum hold."
        )
    if not exit_when and not (take and take > 0) and not (stop and stop > 0):
        sell_bits.append("No extra panic button — only the hold timer and the sell rule below.")
    elif not exit_when:
        sell_bits.append("The hold timer still applies if take-profit and stop-loss do not fire.")
    if sell_mode == "time":
        sell_bits.append(
            f"Time-stop: once {hold} session(s) are up, sell at 09:30 even if the name is still on the list."
        )
    elif sell_mode == "cut_loser":
        sell_bits.append(
            f"After {hold} session(s), sell if the 09:30 open is 3% worse than entry. "
            "Otherwise sell when the name drops off the list."
        )
    elif sell_mode == "trail":
        sell_bits.append(
            f"After {hold} session(s), sell if the 09:30 open is 5% off the best price since entry. "
            "Otherwise sell when the name drops off the list."
        )
    else:
        sell_bits.append(
            f"List-drop: after {hold} session(s), sell at the 09:30 open if the name is no longer on today's list. "
            "If it fell off earlier, we still wait out the minimum hold."
        )
    sell_bits.append("Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.")

    verb = "short" if short else "buy"
    kid = (
        f"Imagine a kid with $10,000 at the 09:30 school bell. "
        f"They look at {_UNI_KID.get(uni, uni)} and only {verb} names that pass "
        f"{'every must-have on the checklist' if req else 'the list as written'}"
        f"{' and skip anything on the must-not list' if forb else ''}. "
        f"They take up to {top_n} names, spend leftover cash on whole shares, "
        f"and hold at least {hold} morning(s). "
        f"{'They sell when the timer rings.' if sell_mode == 'time' else 'They sell when the name falls off the list (after the timer).'} "
        f"They never peek at today's report card (Change%) to pick. "
        f"{'This sleeve bets the price will fall.' if short else 'This sleeve bets the price will rise.'}"
    )
    return {
        "kid": kid,
        "inputs": inputs,
        "buy": buy,
        "sell": sell_bits,
        "universe": uni,
        "hold": hold,
        "side": side,
        "top_n": top_n,
        "size": size,
        "sell_rule": sell_mode,
        "s_boost": boost,
    }


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


def flatten_plan(date: str) -> dict:
    """Live flatten_robust would-buy / route for one session.

    Cached. Used so ``live_entry`` recipes only buy when the 09:30
    flatten gate actually fires (green S, ≥5 priced BUYs, prior book).
    Wish-list HOLD mornings are not tickets.
    """
    global _PLAN
    if not date:
        return {}
    try:
        if _PLAN is None:
            payload = sm.load_payload()
            books = sm.list_books()
            _PLAN = {
                "payload": payload,
                "books": books,
                "book_map": sm.load_book_map(books),
                "cal": sm.session_calendar(payload, books),
                "pol": sm.live_policy(),
                "by_date": {},
            }
        hit = _PLAN["by_date"].get(date)
        if hit is None:
            hit = fla.flatten_day_targets(
                date, payload=_PLAN["payload"], books=_PLAN["books"],
                pol=_PLAN["pol"], book_map=_PLAN["book_map"], cal=_PLAN["cal"],
            )
            _PLAN["by_date"][date] = hit
        return hit
    except Exception:
        return {}


def matches(row: dict, rec: dict) -> bool:
    uni = rec.get("universe") or "union"
    srcs = set(row.get("sources") or [])
    if uni != "union" and uni not in srcs:
        return False
    req = rec.get("require") or {}
    forb = rec.get("forbid") or {}
    if req.get("live_entry"):
        ok = row.get("flatten_ok")
        if ok is None:
            ok = flatten_plan(row.get("date") or "").get("flatten_ok")
        if not ok:
            return False
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
    if "n_neg_max" in req and n_neg(row) > int(req["n_neg_max"]):
        return False
    if "n_neg_min" in req and n_neg(row) < int(req["n_neg_min"]):
        return False
    if "n_pos_min" in req and n_pos(row) < int(req["n_pos_min"]):
        return False
    if req.get("news_box") and not _cam_ok(
            str(row.get("news_box") or "missing").lower(), req["news_box"]):
        return False
    if req.get("headline") and not _cam_ok(
            str(row.get("news_prior") or "missing").lower(), req["headline"]):
        return False
    if req.get("news_and_headline") and not (
            str(row.get("news_box") or "").lower() == "good"
            and str(row.get("news_prior") or "").lower() == "good"):
        return False
    if req.get("news_or_headline") and not news_or_green(row):
        return False
    if req.get("news_or_red") and not news_or_red(row):
        return False
    if "cam_net_min" in req and cam_net(row) < int(req["cam_net_min"]):
        return False
    if req.get("burst") and not is_burst(row):
        return False
    if req.get("yday_up") and not yday_up(row):
        return False
    if "cam_bad_max" in req and cam_bad(row) > int(req["cam_bad_max"]):
        return False
    if req.get("major_catalyst") and not major_catalyst(row):
        return False
    if req.get("yday_or_catalyst") and not (
            yday_up(row) or major_catalyst(row)):
        return False
    if req.get("yday_and_catalyst") and not (
            yday_up(row) and major_catalyst(row)):
        return False
    if req.get("rsi_os") and not row.get("rsi_os"):
        return False
    if req.get("rsi_ob") and not row.get("rsi_ob"):
        return False
    if req.get("macd_up") and not row.get("macd_up"):
        return False
    if req.get("macd_down") and not row.get("macd_down"):
        return False
    if req.get("macd_cross_up") and not row.get("macd_cross_up"):
        return False
    if req.get("flow_in") and not row.get("flow_in"):
        return False
    if "rsi_min" in req:
        v = _finite(row.get("rsi"))
        if v is None or v < float(req["rsi_min"]):
            return False
    if "rsi_max" in req:
        v = _finite(row.get("rsi"))
        if v is None or v > float(req["rsi_max"]):
            return False
    return True


def n_pos(row: dict) -> int:
    """Green-camera count — the +N on the morning board."""
    if row.get("cond_good") is not None:
        return int(row.get("cond_good") or 0)
    boxes = row.get("boxes") or {}
    return sum(1 for k, v in boxes.items() if k != "yday" and v == "good")


def n_neg(row: dict) -> int:
    """Red-camera count plus 🚨 — the −N on the morning board."""
    boxes = row.get("boxes") or {}
    n = sum(1 for k, v in boxes.items() if k != "yday" and v == "bad")
    if row.get("alarm"):
        n += 1
    return n


def cam_bad(row: dict) -> int:
    """Red cameras only (no 🚨). The −M half of +G −R."""
    if row.get("cond_bad") is not None:
        return int(row.get("cond_bad") or 0)
    boxes = row.get("boxes") or {}
    return sum(1 for k, v in boxes.items() if k != "yday" and v == "bad")


def cam_net(row: dict) -> int:
    """+G −R. Positive enough is the long filter; +9 −1 is not required."""
    return n_pos(row) - cam_bad(row)


def news_or_green(row: dict) -> bool:
    """Packet green OR prior-export headline green. Not both."""
    return (
        str(row.get("news_box") or "").lower() == "good"
        or str(row.get("news_prior") or "").lower() == "good"
    )


def news_or_red(row: dict) -> bool:
    """Packet red OR prior-export headline red."""
    return (
        str(row.get("news_box") or "").lower() == "bad"
        or str(row.get("news_prior") or "").lower() == "bad"
    )


def yday_ret(row: dict, *, date: str | None = None, bars=None,
             cal: list | None = None) -> float | None:
    """Prior-session close-to-close %. Leak-free at this 09:30."""
    v = _finite(row.get("ohlc_ret_1"))
    if v is not None:
        return round(float(v), 2)
    t = _tick(row.get("ticker"))
    d = str(date or row.get("date") or "")[:10]
    if not t or not d or not cal or d not in cal:
        return None
    i = list(cal).index(d)
    if i < 2:
        return None
    c1 = _finite((_bar(t, cal[i - 1], bars) or {}).get("close"))
    c0 = _finite((_bar(t, cal[i - 2], bars) or {}).get("close"))
    if c1 is None or c0 is None or c0 == 0:
        return None
    return round(100.0 * (float(c1) / float(c0) - 1.0), 2)


def yday_up(row: dict, *, date: str | None = None, bars=None,
            cal: list | None = None) -> bool:
    """Yesterday positive: prior close-to-close > 0, else last-green bar."""
    v = yday_ret(row, date=date, bars=bars, cal=cal)
    if v is not None:
        return v > 0
    return bool(row.get("last_green"))


def major_catalyst(row: dict) -> bool:
    """Public good catalyst: catal green, last EPS beat, or react-not-miss.

    Leak-free: ``e_pol`` is morning-export surprise already public at 09:30.
    ``catal`` is the pre-open camera. Earnings-react without a miss still
    counts — a date-only E is not painted green, but it is a known event.
    """
    boxes = row.get("boxes") or {}
    if str(boxes.get("catal") or "").strip().lower() == "good":
        return True
    if str(row.get("catal") or "").strip().lower() == "good":
        return True
    ep = str(row.get("e_pol") or "").strip().lower()
    if ep == "good":
        return True
    earn = bool(row.get("erd_earn_react") or row.get("earn_react"))
    if earn and ep != "bad":
        return True
    return False


def _recipe_needs_catalyst(rec: dict | None) -> bool:
    req = (rec or {}).get("require") or {}
    return bool(req.get("yday_or_catalyst") or req.get("yday_and_catalyst")
                or req.get("major_catalyst"))


def ensure_sim_fields(panel: dict, rec: dict | None = None) -> dict:
    """Fill leak-free ``ohlc_ret_1`` and, when the recipe needs it, ``e_pol``."""
    if not isinstance(panel, dict):
        return panel
    rows = panel.get("rows") or []
    cal = list(panel.get("session_dates") or [])
    if not panel.get("_ohlc_filled"):
        for r in rows:
            if r.get("ohlc_ret_1") is None:
                yr = yday_ret(r, date=r.get("date"), cal=cal or None)
                if yr is not None:
                    r["ohlc_ret_1"] = yr
        panel["_ohlc_filled"] = True
    if rec is None or _recipe_needs_catalyst(rec):
        if rows and "e_pol" not in rows[0]:
            from . import factor_mine_probe as fmp
            fmp.attach_erd_polarity(panel)
    if not panel.get("_tape_filled"):
        if rows and "rsi" not in rows[0]:
            attach_tape_flow(panel)
        else:
            panel["_tape_filled"] = True
    return panel


def white_horizon_overlay(rec: dict) -> dict:
    """Same looker list; −0 red + (yday up or catalyst); then Score."""
    base = rec or {}
    name = str(base.get("name") or "looker")
    tag = name if name.endswith("_white_any") else f"{name}_white_any"
    out = make_recipe(
        name=tag,
        universe=base.get("universe") or "union",
        hold=int(base.get("hold") or 1),
        side=base.get("side") or "long",
        top_n=int(base.get("top_n") or TOP_N_DEFAULT),
        require={"cam_bad_max": 0, "yday_or_catalyst": True},
        forbid={"alarm": True},
        rank="list",
        size=base.get("size") or "leftover",
        sell=base.get("sell") or "list",
        s_boost=base.get("s_boost") or "none",
        take_pct=base.get("take_pct"),
        stop_pct=base.get("stop_pct"),
        note="same looker list; −0 red + (yday up or major catalyst); then Score",
    )
    out["looker"] = name
    return out


def white_yday_overlay(rec: dict) -> dict:
    """Same 09:30 looker list; buy 0 red cameras + yesterday up; rank +G−R."""
    base = rec or {}
    name = str(base.get("name") or "looker")
    tag = name if name.endswith("_white_yday") else f"{name}_white_yday"
    out = make_recipe(
        name=tag,
        universe=base.get("universe") or "union",
        hold=int(base.get("hold") or 1),
        side=base.get("side") or "long",
        top_n=int(base.get("top_n") or TOP_N_DEFAULT),
        require={"zero_red": True, "yday_up": True},
        forbid={"alarm": True},
        rank="cond",
        size=base.get("size") or "leftover",
        sell=base.get("sell") or "list",
        s_boost=base.get("s_boost") or "none",
        take_pct=base.get("take_pct"),
        stop_pct=base.get("stop_pct"),
        note="same looker list; buy 0 red cameras + prior-session up; rank +G−R",
    )
    out["looker"] = name
    return out


def is_burst(row: dict) -> bool:
    """Parabolic prior tape. Leak-free: ret_5 / rvol / break_10 from date < D."""
    ret = _finite(row.get("ohlc_ret_5"))
    rvol = _finite(row.get("ohlc_rvol"))
    if ret is None or ret < 12.0:
        return False
    if not row.get("last_green"):
        return False
    return bool(row.get("ohlc_break_10")) or (rvol is not None and rvol >= 2.0)


def match_why(row: dict, rec: dict) -> dict:
    """Kid-plain pass/fail for one name vs one recipe. No hard-red, no cash."""
    failed: list[str] = []
    passed: list[str] = []

    def need(ok: bool, msg: str) -> None:
        (passed if ok else failed).append(msg)

    uni = rec.get("universe") or "union"
    srcs = set(row.get("sources") or [])
    if uni != "union":
        need(uni in srcs, f"on the {uni} 09:30 list")
    req = rec.get("require") or {}
    forb = rec.get("forbid") or {}
    if req.get("live_entry"):
        ok = row.get("flatten_ok")
        if ok is None:
            ok = flatten_plan(row.get("date") or "").get("flatten_ok")
        need(bool(ok), "live flatten gate says GO")
    boxes = row.get("boxes") or {}
    for cam in CAMERAS:
        want = req.get(cam)
        if want:
            need(_cam_ok(_tone(boxes, cam), want), _gate_kid(cam, want))
        ban = forb.get(cam)
        if ban:
            need(not _cam_ok(_tone(boxes, cam), ban),
                 f"not {_gate_kid(cam, ban)}")
    if req.get("blue"):
        need(bool(row.get("blue")), _gate_kid("blue", True))
    if req.get("zero_red"):
        need(bool(row.get("zero_red")), _gate_kid("zero_red", True))
    if forb.get("alarm"):
        need(not row.get("alarm"), "no 🚨 overnight alarm")
    if req.get("alarm"):
        need(bool(row.get("alarm")), "🚨 alarm is on")
    if req.get("last_green"):
        need(bool(row.get("last_green")), _gate_kid("last_green", True))
    if req.get("last_red"):
        need(bool(row.get("last_red")), _gate_kid("last_red", True))
    if req.get("candle_capture"):
        need(bool(row.get("candle_capture")), _gate_kid("candle_capture", True))
    if req.get("break_10"):
        need(bool(row.get("ohlc_break_10")), _gate_kid("break_10", True))
    if req.get("earn_react"):
        need(bool(row.get("erd_earn_react")), _gate_kid("earn_react", True))
    if req.get("news_present"):
        need(_tone(boxes, "news") != "missing", _gate_kid("news_present", True))
    if req.get("join_present"):
        need(_tone(boxes, "join") != "missing", _gate_kid("join_present", True))
    if req.get("catal_present"):
        need(_tone(boxes, "catal") != "missing", _gate_kid("catal_present", True))
    if "ret_5_min" in req:
        v = _finite(row.get("ohlc_ret_5"))
        need(v is not None and v >= float(req["ret_5_min"]),
             _gate_kid("ret_5_min", req["ret_5_min"]))
    if "ret_5_max" in req:
        v = _finite(row.get("ohlc_ret_5"))
        need(v is not None and v <= float(req["ret_5_max"]),
             _gate_kid("ret_5_max", req["ret_5_max"]))
    if "rvol_min" in req:
        v = _finite(row.get("ohlc_rvol"))
        need(v is not None and v >= float(req["rvol_min"]),
             _gate_kid("rvol_min", req["rvol_min"]))
    if "rvol_max" in req:
        v = _finite(row.get("ohlc_rvol"))
        need(v is not None and v <= float(req["rvol_max"]),
             _gate_kid("rvol_max", req["rvol_max"]))
    if "days_since_E_max" in req:
        v = row.get("erd_days_since_E")
        need(v is not None and int(v) <= int(req["days_since_E_max"]),
             _gate_kid("days_since_E_max", req["days_since_E_max"]))
    if "flag_E_min" in req:
        v = row.get("erd_flag_E")
        need(v is not None and int(v) >= int(req["flag_E_min"]),
             _gate_kid("flag_E_min", req["flag_E_min"]))
    if "days_since_R_max" in req:
        v = row.get("erd_days_since_R")
        need(v is not None and int(v) <= int(req["days_since_R_max"]),
             _gate_kid("days_since_R_max", req["days_since_R_max"]))
    if "flag_R" in req:
        need(int(row.get("erd_flag_R") or 0) == int(req["flag_R"]),
             _gate_kid("flag_R", req["flag_R"]))
    if "n_neg_max" in req:
        need(n_neg(row) <= int(req["n_neg_max"]),
             _gate_kid("n_neg_max", req["n_neg_max"]))
    if "n_neg_min" in req:
        need(n_neg(row) >= int(req["n_neg_min"]),
             _gate_kid("n_neg_min", req["n_neg_min"]))
    if "n_pos_min" in req:
        need(n_pos(row) >= int(req["n_pos_min"]),
             _gate_kid("n_pos_min", req["n_pos_min"]))
    if req.get("news_box"):
        need(_cam_ok(str(row.get("news_box") or "missing").lower(), req["news_box"]),
             _gate_kid("news_box", req["news_box"]))
    if req.get("headline"):
        need(_cam_ok(str(row.get("news_prior") or "missing").lower(), req["headline"]),
             _gate_kid("headline", req["headline"]))
    if req.get("news_and_headline"):
        need(str(row.get("news_box") or "").lower() == "good"
             and str(row.get("news_prior") or "").lower() == "good",
             _gate_kid("news_and_headline", True))
    if req.get("news_or_headline"):
        need(news_or_green(row), _gate_kid("news_or_headline", True))
    if req.get("news_or_red"):
        need(news_or_red(row), _gate_kid("news_or_red", True))
    if "cam_net_min" in req:
        need(cam_net(row) >= int(req["cam_net_min"]),
             _gate_kid("cam_net_min", req["cam_net_min"]))
    if req.get("burst"):
        need(is_burst(row), _gate_kid("burst", True))
    if req.get("yday_up"):
        need(yday_up(row), _gate_kid("yday_up", True))
    if "cam_bad_max" in req:
        need(cam_bad(row) <= int(req["cam_bad_max"]),
             _gate_kid("cam_bad_max", req["cam_bad_max"]))
    if req.get("major_catalyst"):
        need(major_catalyst(row), _gate_kid("major_catalyst", True))
    if req.get("yday_or_catalyst"):
        need(yday_up(row) or major_catalyst(row),
             _gate_kid("yday_or_catalyst", True))
    if req.get("yday_and_catalyst"):
        need(yday_up(row) and major_catalyst(row),
             _gate_kid("yday_and_catalyst", True))
    if req.get("rsi_os"):
        need(bool(row.get("rsi_os")), _gate_kid("rsi_os", True))
    if req.get("rsi_ob"):
        need(bool(row.get("rsi_ob")), _gate_kid("rsi_ob", True))
    if req.get("macd_up"):
        need(bool(row.get("macd_up")), _gate_kid("macd_up", True))
    if req.get("macd_down"):
        need(bool(row.get("macd_down")), _gate_kid("macd_down", True))
    if req.get("macd_cross_up"):
        need(bool(row.get("macd_cross_up")), _gate_kid("macd_cross_up", True))
    if req.get("flow_in"):
        need(bool(row.get("flow_in")), _gate_kid("flow_in", True))
    if "rsi_min" in req:
        v = _finite(row.get("rsi"))
        need(v is not None and v >= float(req["rsi_min"]),
             _gate_kid("rsi_min", req["rsi_min"]))
    if "rsi_max" in req:
        v = _finite(row.get("rsi"))
        need(v is not None and v <= float(req["rsi_max"]),
             _gate_kid("rsi_max", req["rsi_max"]))
    return {"ok": not failed, "failed": failed, "passed": passed}


def decision_why(rec: dict, *, hard: bool = False, s=None,
                 look: dict | None = None, why: dict | None = None) -> dict:
    """BUY / SIT / NO in sentences, from gates + rank + morning S."""
    rec = rec or {}
    why = why or {}
    failed = list(why.get("failed") or [])
    passed = list(why.get("passed") or [])
    top_n = int(rec.get("top_n") or TOP_N_DEFAULT)
    lines: list[str] = []
    if hard:
        s_txt = "—" if s is None else f"{float(s):.2f}"
        lines.append(f"Morning weather S is {s_txt} (hard-red ≤ −3).")
        lines.append(
            "The sleeve sits — no new lots today, even if this name "
            "would pass the buy gates."
        )
        if passed:
            lines.append("Gates that already pass: " + "; ".join(passed) + ".")
        if failed:
            lines.append(
                "It would still fail these gates if S were above −3: "
                + "; ".join(failed) + "."
            )
        return {"take": "sit", "lines": lines}
    if look and look.get("buy"):
        lines.append(
            "Would buy: on this recipe's shopping list and inside the cash cut."
        )
        if passed:
            lines.append("Gates that fired: " + "; ".join(passed) + ".")
        rank = look.get("rank")
        if rank is not None:
            lines.append(
                f"Ranked #{int(rank)} of top {top_n} by "
                f"{rec.get('rank') or 'list order'}."
            )
        return {"take": "buy", "lines": lines}
    if look and look.get("pass"):
        lines.append(
            f"Would not buy: passed the gates but ranked #{look.get('rank')} "
            f"— only the top {top_n} get leftover cash."
        )
        return {"take": "no", "lines": lines}
    lines.append("Would not buy.")
    if failed:
        lines.append("Failed: " + "; ".join(failed) + ".")
    else:
        lines.append("Not on this recipe's 09:30 universe.")
    return {"take": "no", "lines": lines}


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
    if how == "rsi":
        v = _finite(row.get("rsi"))
        return (999.0 if v is None else v, row["ticker"])
    if how == "macd_hist":
        return (-(_finite(row.get("macd_hist")) or 0.0), row["ticker"])
    if how in ("list", "score", "src_rank"):
        src = row.get("src_rank")
        src_i = 99 if src is None else int(src)
        return (src_i, row["ticker"])
    src = row.get("src_rank")
    src_i = 99 if src is None else int(src)
    return (src_i, row["ticker"])


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
    rec = {
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
        "ohlc_ret_1": oh.get("ret_1"),
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
    apply_tape_fields(rec, oh, _finviz_snap(prior_df, ticker))
    return rec


def build_panel(from_date: str = START, to_date: str | None = None) -> dict:
    """Leak-free candidate rows for every *closed* session in the window.

    An empty ``to_date`` used to walk the whole stock-book calendar,
    including today's pre-open stub. Member books then stopped at
    ``last_closed`` and combo split crashed on the extra day.
    """
    _SCAN_CACHE.clear()
    payload = sm.load_payload()
    books = sm.list_books()
    end = live_panel_end(from_date, to_date)
    cal = [d for d in sm.session_calendar(payload, books)
           if d >= from_date and (not end or d <= end)]
    end = end or (cal[-1] if cal else from_date)
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


def refresh_panel_marks(panel: dict) -> dict:
    """Fill row open/close from the official tape after a late price fetch.

    ``build_panel`` used to run *before* ``ensure_through``, so a new
    session's 09:30 / 16:00 stayed blank on investigator cards even
    after Yahoo landed the bars.
    """
    panel = rehydrate_panel(panel)
    for r in panel.get("rows") or []:
        if r.get("open") is not None and r.get("close") is not None:
            continue
        t = _tick(r.get("ticker"))
        d = r.get("date")
        if not t or not d:
            continue
        bar = tl.session_bar(t, d) or {}
        if r.get("open") is None and _finite(bar.get("open")) is not None:
            r["open"] = round(float(bar["open"]), 4)
        if r.get("close") is None and _finite(bar.get("close")) is not None:
            r["close"] = round(float(bar["close"]), 4)
    return panel


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


def session_has_closed(date: str, now=None) -> bool:
    """True after that session's 16:00 ET print is knowable.

    A 09:10 ET pre-open file for *today* does not close yesterday's
    start-path, and it does not give today a 0% mark. Yesterday already
    has a full open\u2192close; grade that. Today waits until the close.
    """
    from zoneinfo import ZoneInfo
    now = now or datetime.now(ZoneInfo("America/New_York"))
    today = now.strftime("%Y-%m-%d")
    if not date:
        return False
    if date < today:
        return True
    if date > today:
        return False
    close = now.replace(hour=16, minute=2, second=0, microsecond=0)
    return now >= close


def last_closed_session(from_date: str, to_date: str | None = None,
                        cal: list[str] | None = None) -> str | None:
    """Last calendar date whose regular session has actually finished."""
    if cal is None:
        payload = sm.load_payload()
        books = sm.list_books()
        cal = list(sm.session_calendar(payload, books))
    dates = [d for d in cal
             if d >= from_date and (not to_date or d <= to_date)
             and session_has_closed(d)]
    return dates[-1] if dates else None


def live_panel_end(from_date: str, to_date: str | None = None) -> str | None:
    """Last NYSE session we can mark — not a pre-open stub for tomorrow."""
    if to_date and session_has_closed(to_date):
        return to_date
    return last_closed_session(from_date, to_date)


def panel_is_current(raw: dict, from_date: str,
                     to_date: str | None = None) -> bool:
    """Cached panel is stale once a later session exists — even with no fills.

    A panel that already includes an *open* session (pre-open stub / midday
    stock book) is also stale — that day is not markable yet.
    """
    if not raw or raw.get("from_date") != from_date:
        return False
    if not raw.get("session_dates"):
        return False
    cached = raw.get("to_date")
    want = live_panel_end(from_date, to_date)
    if want and str(cached or "") != str(want):
        return False
    if want and want not in (raw.get("session_dates") or []):
        return False
    if to_date and cached and str(cached) < str(to_date) and session_has_closed(to_date):
        return False
    return True


def load_or_build_panel(from_date: str = START, to_date: str | None = None,
                        rebuild: bool = False) -> dict:
    if not rebuild and PANEL_PATH.exists():
        raw = json.loads(PANEL_PATH.read_text(encoding="utf-8"))
        if panel_is_current(raw, from_date, to_date):
            print(f"[factor-mine] loaded panel {PANEL_PATH} "
                  f"rows={raw.get('n_rows')} → {raw.get('to_date')}", flush=True)
            return rehydrate_panel(raw)
        print(f"[factor-mine] panel stale "
              f"{raw.get('to_date')} != live {live_panel_end(from_date, to_date)} "
              f"— rebuilding so leftover lots get a mark", flush=True)
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
    panel = ensure_sim_fields(panel, rec)
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
    start_rets = [s["return_pct"] for s in starts]
    median_start = None
    if start_rets:
        mid = sorted(start_rets)[len(start_rets) // 2]
        median_start = round(float(mid), 3)
    scored_means = [d["mean"] for d in daily if d["mean"] is not None]
    pothole_pct = max(scored_means) if scored_means else None
    pothole_date = None
    if scored_means:
        pothole_date = next(d["date"] for d in daily if d["mean"] == pothole_pct)
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
    reliable = (
        len(graded) >= MIN_GRADED
        and len(starts) >= MIN_STARTS
        and len(days_scored) >= MIN_DAYS
    )
    effectiveness = _effectiveness(
        win_rate, profitable_days, start_rate,
        (gainer_hits / n_picks) if n_picks else None,
        (loser_hits / n_picks) if n_picks else None,
        payoff, total_ret, median_start, pothole_pct, reliable,
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
        "size": rec.get("size") or "leftover",
        "sell": rec.get("sell") or "list",
        "s_boost": rec.get("s_boost") or "none",
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
        "median_start_pct": median_start,
        "pothole_date": pothole_date,
        "pothole_pct": None if pothole_pct is None else round(float(pothole_pct), 3),
        "reliable": reliable,
        "total_ret_pct": total_ret,
        "final_equity": equity[-1] if equity else CAPITAL,
        "effectiveness": effectiveness,
        "daily": daily,
        "equity": equity,
        "starts": starts,
    }


def _effectiveness(win_rate, day_rate, start_rate, gainer_rate,
                   loser_rate, payoff, total_ret, median_start=None,
                   pothole_pct=None, reliable=True) -> float:
    def n(v, default=0.0):
        return default if v is None else float(v)
    # Cap the one-day jackpot so an 8-13 / 8-19 rip cannot dominate.
    capped_tot = min(max(n(total_ret), -40.0), 40.0)
    pothole_pen = 0.0
    if pothole_pct is not None and float(pothole_pct) >= POTHOLE_CUT:
        pothole_pen = min(25.0, float(pothole_pct) / 8.0)
    score = (
        40 * n(win_rate)
        + 20 * n(day_rate)
        + 25 * n(start_rate)
        + 10 * (1.0 if n(median_start) > 0 else 0.0)
        + 15 * n(gainer_rate)
        - 20 * n(loser_rate)
        + 5 * min(n(payoff, 1.0), 3.0)
        + 0.15 * capped_tot
        - pothole_pen
    )
    if not reliable:
        score -= 20.0
    return round(score, 3)


def run(from_date: str = START, to_date: str | None = None,
        write: bool = False, recipes: list[dict] | None = None,
        panel: dict | None = None, rebuild_panel: bool = False,
        persist_panel: bool = False, book: bool = True,
        bars: dict | None = None, combos: bool = True) -> dict:
    from . import factor_mine_book as fmb
    recipes = list(recipes or build_recipes())
    end = to_date or live_panel_end(from_date, to_date)
    if write or persist_panel or rebuild_panel:
        try:
            from . import price_store as ps
            held = _held_tickers_from_disk()
            # Official bars *before* the panel walk so 09:30 / 16:00
            # exist on the new session. Held lots first — a full-universe
            # yahoo walk used to die on junk tickers.
            ps.ensure_through(end, tickers=sorted(held) or None)
            tl.reset_price_caches()
        except Exception as e:
            print(f"[factor-mine] price ensure skipped: {e}", flush=True)
    panel = (panel if panel is not None
             else load_or_build_panel(from_date, to_date, rebuild=rebuild_panel))
    attach_tape_flow(panel)
    if write or persist_panel or rebuild_panel:
        try:
            from . import price_store as ps
            names = {str(r.get("ticker") or "").upper()
                     for r in (panel.get("rows") or []) if r.get("ticker")}
            held = _held_tickers_from_disk()
            need = sorted(names | held)
            if need:
                ps.ensure_through(end or panel.get("to_date"), tickers=need)
                tl.reset_price_caches()
            panel = refresh_panel_marks(panel)
        except Exception as e:
            print(f"[factor-mine] price ensure skipped: {e}", flush=True)
    if persist_panel or write:
        PANEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        slim = {k: v for k, v in panel.items() if k != "by_date"}
        slim["by_date"] = None
        PANEL_PATH.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    cal = list(panel.get("session_dates") or [])
    tapes = _tapes(cal)
    regime = fmb.load_regime() if book else {}
    fees = pt_fees() if book else None
    stats = []
    books = {}
    for rec in recipes:
        st = score_recipe(panel, rec, tapes, bars=bars)
        if book:
            bk = fmb.simulate_book(
                panel, rec, bars=bars, fees=fees, regime=regime)
            starts = fmb.replay_starts(
                panel, rec, bars=bars, fees=fees, regime=regime)
            st = fmb.attach_book(st, bk, starts)
            books[rec["name"]] = bk
        stats.append(st)
    combo_meta = {"n": 0, "outperform": [], "rule": ""}
    if book and combos:
        from . import factor_mine_combo as fmc
        member_stat_by = {s["name"]: s for s in stats}
        combo_stats, combo_books = fmc.run_combos(
            panel, recipes, bars=bars, fees=fees, regime=regime,
            member_stat_by=member_stat_by)
        for st in combo_stats:
            stats.append(st)
            books[st["name"]] = combo_books[st["name"]]
            recipes.append(fmc.combo_recipe({
                "name": st["name"],
                "members": st.get("members") or [],
                "weights": st.get("weights") or [],
                "net": st.get("net") or "priority",
                "pool": st.get("pool") or "shared",
            }))
        if write:
            fmc.write_combo_sidecar(combo_stats)
        combo_meta = {
            "n": len(combo_stats),
            "outperform": [s["name"] for s in combo_stats if s.get("outperforms")],
            "rule": fmc.OUTPERFORM_RULE,
        }
    stats.sort(key=lambda r: (
        0 if r.get("outperforms") else 1,
        0 if r.get("reliable") else 1,
        -(r.get("effectiveness") or -999),
        r["name"],
    ))
    dates = cal
    series = {}
    for s in stats:
        eq = s["equity"]
        series[s["name"]] = eq[1:] if len(eq) == len(dates) + 1 else eq
    extra = [n for n in (
        "flatten_live_h1", "flatten_live_h3", "flatten_live_h5",
        "union_e_fresh_h3", "union_news_g_h5", "union_white_coil_h1",
        "union_news_pack_h1", "union_news_pack_net2_h1",
        "union_news_or_h1",
        "union_news_or_net3_h1", "union_news_or_net4_h1",
        "union_news_or_net4_rw_h1", "union_news_or_net4_conv_h1",
        "short_news_head_h3",
        "combo_ps_5050_shared", "combo_ps_7030_shared",
        "combo_p2s_5050_shared",
        "union_news_g_cam91_n1_h1", "union_news_both_h1",
        "union_news_g_cam71_h1", "union_news_g_conv_h1",
        "union_e_green_h3",
        "union_white_any_h1", "union_white_any_h2",
        "union_white_any_h3", "union_white_any_h5",
        "union_white_both_n4_h1", "union_white_both_n4_h2",
        "union_white_both_n4_h5", "union_white_both_n4_h5_s12",
        "flatten_h5_s8",
        "flatten_h5", "flatten_h5_rankw", "flatten_h5_time", "flatten_h5_sboost",
        "union_h5_sboost", "flatten_live_h1_sizeup",
        "union_h3_cut", "union_h1_topheavy",
    ) if any(s["name"] == n for s in stats)]
    by_ret = [s["name"] for s in sorted(
        [s for s in stats if s.get("reliable") and s.get("total_ret_pct") is not None],
        key=lambda s: -float(s["total_ret_pct"]),
    )[:8]]
    featured = []
    winners = list(combo_meta.get("outperform") or [])
    for n in (winners + by_ret
              + [s["name"] for s in stats if s.get("reliable")][:8] + extra):
        if n not in featured:
            featured.append(n)
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "fill": "09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit",
        "from_date": panel.get("from_date"),
        "to_date": panel.get("to_date"),
        "n_sessions": panel.get("n_sessions"),
        "n_rows": panel.get("n_rows"),
        "n_recipes": len(stats),
        "capital": CAPITAL,
        "loser_cut": LOSER_CUT,
        "leak": "prior tape + pre-open packet only; news from prior export or morning box",
        "live_untouched": "flatten_robust",
        "book_rules": dict(fmb.BOOK_RULES),
        "dates": dates,
        "featured": featured,
        "stats": [{k: v for k, v in s.items()
                   if k not in ("daily", "equity", "starts")} for s in stats],
        "series": series,
        "daily": {s["name"]: _slim_dash_daily(s["daily"]) for s in stats},
        "starts": {s["name"]: s["starts"] for s in stats},
        "books": {n: _slim_dash_book(bk) for n, bk in books.items()},
        "md_names": [s["name"] for s in stats if s.get("book_n_trades")],
        "recipes": recipes,
        "panel_n": panel.get("n_rows"),
        "combos": combo_meta,
    }
    stamp_explains(payload)
    from . import factor_mine_probe as fmp
    from . import factor_mine_sim as fms
    bought = _bought_tickers(books, {s["name"]: s.get("starts") for s in stats})
    payload["probe"] = fmp.slim_probe(fmp.build_probe(panel), bought)
    payload["mornings"] = fmp.build_mornings()
    payload.update(fmp.probe_meta())
    payload["sim"] = fms.build_sim_pack(panel)
    if write:
        write_outputs(payload, stats, books=books)
    return payload


def _held_tickers_from_disk() -> set[str]:
    """Names still on a mined book — they need a mark even if today's list is empty."""
    out: set[str] = set()
    if not OUT_JSON.is_file():
        return out
    try:
        doc = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return out
    for bk in (doc.get("books") or {}).values():
        for t in (bk or {}).get("trades") or []:
            if t.get("ticker"):
                out.add(_tick(t["ticker"]))
            for n in t.get("overnight") or t.get("open_held") or []:
                name = n.get("ticker") if isinstance(n, dict) else str(n).split("×")[0]
                if name:
                    out.add(_tick(name))
        for d in (bk or {}).get("daily") or []:
            for m in (d.get("marks") or d.get("overnight") or []):
                if isinstance(m, dict) and m.get("ticker"):
                    out.add(_tick(m["ticker"]))
    return {t for t in out if t}


def _bought_tickers(books: dict | None, starts: dict | None = None) -> set[str]:
    out: set[str] = set()
    for bk in (books or {}).values():
        for t in (bk or {}).get("trades") or []:
            if t.get("side") in ("BUY", "SHORT") and t.get("ticker"):
                out.add(_tick(t["ticker"]))
    for paths in (starts or {}).values():
        for p in paths or []:
            for b in (p.get("buys") or []):
                if b.get("ticker"):
                    out.add(_tick(b["ticker"]))
            for name in p.get("bought") or []:
                if name:
                    out.add(_tick(name))
    return out


def stamp_starts_and_probe(payload: dict, panel: dict | None = None,
                           bars=None) -> dict:
    """Replay cash-start paths + investigator cards. Does not remine books."""
    from . import factor_mine_book as fmb
    from . import factor_mine_probe as fmp
    panel = panel if panel is not None else load_or_build_panel(
        payload.get("from_date") or START,
        payload.get("to_date"),
    )
    panel = rehydrate_panel(panel)
    regime = fmb.load_regime()
    fees = pt_fees()
    recs = list(payload.get("recipes") or [])
    starts = dict(payload.get("starts") or {})
    for i, rec in enumerate(recs, 1):
        if rec.get("universe") == "combo" or rec.get("members"):
            continue
        starts[rec["name"]] = fmb.replay_starts(
            panel, rec, bars=bars, fees=fees, regime=regime)
        if i == 1 or i == len(recs) or i % 20 == 0:
            print(f"[factor-mine] cash-start {i}/{len(recs)} {rec['name']}",
                  flush=True)
    payload["starts"] = starts
    bought = _bought_tickers(payload.get("books"), starts)
    payload["probe"] = fmp.slim_probe(fmp.build_probe(panel), bought)
    payload["mornings"] = fmp.build_mornings()
    payload.update(fmp.probe_meta())
    from . import factor_mine_sim as fms
    payload["sim"] = fms.build_sim_pack(panel)
    return payload


def stamp_sim(payload: dict, panel: dict | None = None) -> dict:
    """Attach the compact replay pack. Does not remine books or starts."""
    from . import factor_mine_sim as fms
    panel = panel if panel is not None else load_or_build_panel(
        payload.get("from_date") or START,
        payload.get("to_date"),
    )
    payload["sim"] = fms.build_sim_pack(rehydrate_panel(panel))
    return payload


def stamp_probe_and_sim(payload: dict, panel: dict | None = None) -> dict:
    """Refresh investigator cards + replay pack. Does not remine books or starts."""
    from . import factor_mine_probe as fmp
    from . import factor_mine_sim as fms
    panel = panel if panel is not None else load_or_build_panel(
        payload.get("from_date") or START,
        payload.get("to_date"),
    )
    panel = rehydrate_panel(panel)
    fmp.attach_erd_polarity(panel)
    bought = _bought_tickers(payload.get("books"), payload.get("starts"))
    payload["probe"] = fmp.slim_probe(fmp.build_probe(panel), bought)
    payload["mornings"] = fmp.build_mornings()
    payload.update(fmp.probe_meta())
    payload["sim"] = fms.build_sim_pack(panel)
    from . import factor_mine_burst as fmburst
    if fmburst.OUT_JSON.is_file():
        try:
            raw = json.loads(fmburst.OUT_JSON.read_text(encoding="utf-8"))
            raw["keepers"] = [
                k for k in (raw.get("keepers") or [])
                if any((r.get("name") == k and (r.get("book_pct") or 0) >= 23.84)
                       for r in (raw.get("rows") or []))
            ]
            payload["burst"] = fmburst.slim_for_dash(raw)
        except (OSError, json.JSONDecodeError):
            pass
    return payload


def stamp_explains(payload: dict) -> dict:
    """Attach kid-plain inputs / buy / sell to every recipe and stat.

    Safe to run on an already-mined payload — does not resimulate books.
    """
    recs = list(payload.get("recipes") or [])
    rec_by = {r.get("name"): r for r in recs}
    for rec in recs:
        rec["explain"] = explain_recipe(rec)
    for s in payload.get("stats") or []:
        s["explain"] = explain_recipe(rec_by.get(s.get("name")) or s)
    payload["recipes"] = recs
    return payload


def _slim_mark(m: dict) -> dict:
    keep = (
        "ticker", "shares", "shares_open", "shares_close",
        "yday_px", "open_px", "close_px", "overnight", "session", "day",
        "held", "vs_entry_open", "vs_entry_close", "entry_px", "delta",
    )
    return {k: m.get(k) for k in keep if m.get(k) is not None}


def _slim_dash_daily(days: list | None) -> list:
    """Phone page: structured marks, no duplicated why-paragraphs."""
    keep = (
        "date", "s", "hard_red", "open_cash", "open_held", "open_equity",
        "yday_equity", "overnight_delta", "session_delta", "cash", "stock",
        "equity", "bought", "sold", "held", "lots", "made_money", "mean",
    )
    out = []
    for d in days or []:
        row = {k: d.get(k) for k in keep}
        row["marks"] = [_slim_mark(m) for m in (d.get("marks") or [])]
        out.append(row)
    return out


def _slim_dash_book(bk: dict) -> dict:
    """Fills for the phone page. Daily state lives on payload['daily']."""
    keep_t = (
        "date", "ticker", "side", "shares", "price", "fees", "pnl",
        "cash_after", "equity_after", "equity_delta", "stock_after", "reason",
        "yday_equity", "open_held", "overnight", "overnight_delta",
        "equity_before", "sell_eq_chg", "vs_yday",
        "session_delta", "intraday", "close_held", "open_equity",
        "owner",
    )
    keep_k = ("date", "ticker", "kind", "reason")

    def slim_t(t: dict) -> dict:
        row = {k: t.get(k) for k in keep_t}
        if t.get("overnight"):
            row["overnight"] = [_slim_mark(n) for n in t["overnight"]]
        if t.get("intraday"):
            row["intraday"] = [_slim_mark(n) for n in t["intraday"]]
        if t.get("side") in ("OPEN", "CLOSE"):
            row.pop("reason", None)
        return row

    return {
        "trades": [slim_t(t) for t in (bk.get("trades") or [])],
        "skips": [{k: x.get(k) for k in keep_k} for x in (bk.get("skips") or [])],
        "open": bk.get("open"),
        "n_trades": bk.get("n_trades"),
        "n_skips": bk.get("n_skips"),
        "realized": bk.get("realized"),
        "cash": bk.get("cash"),
        "total_ret_pct": bk.get("total_ret_pct"),
        "audit": bk.get("audit"),
        "size": bk.get("size"),
        "sell": bk.get("sell"),
        "s_boost": bk.get("s_boost"),
    }


def pt_fees():
    from . import paper_trade as pt
    return pt.load_fees()


def write_outputs(payload: dict, stats: list[dict] | None = None,
                  books: dict | None = None) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_START.parent.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
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
        "Cash book: $10k, whole shares, Futubull fees, leftover split, "
        "sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts "
        "marked as a liability. Each session starts from leftover cash "
        "and lots actually held (butterfly). Cash-start buttons "
        "wake a sleeve on date X with $10k and no lots (same rules). "
        "Stock investigator quotes 09:30 cameras / coaches / news from "
        "repo files. Size / sell / S-boost tweaks sit on the same "
        "ledger. Signal-only % is the old equal-weight path (not a "
        "fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). "
        "`flatten_live_*` = only when the live flatten gate fires. "
        "Research only — does not change live `flatten_robust`.",
        "",
    ]
    combos = payload.get("combos") or {}
    if combos.get("n"):
        wins = combos.get("outperform") or []
        lines += [
            f"Combination books: **{combos['n']}** mixes on the same $10k "
            f"cash ledger (shared leftover or split sleeves, official 09:30 / "
            f"16:00, hard-red sit, owner min-hold). "
            f"Outperformers: "
            + (", ".join(f"`{n}`" for n in wins) if wins else "none this window")
            + ".",
            "",
            str(combos.get("rule") or ""),
            "",
        ]
    lines += [
        "Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).",
        "",
        "| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | "
        "Starts YES | Med start | Top-g | Losers | AvgW | AvgL | "
        "Book% | Signal% | Audit | Eff |",
        "|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|",
    ]
    for s in (stats or []):
        aud = "PASS" if s.get("audit_ok", True) else f"FAIL×{s.get('audit_n_fail') or '?'}"
        lines.append(
            f"| `{s['name']}`{' *(thin)*' if not s.get('reliable') else ''} | "
            f"{s['side']} | {s['hold']} | "
            f"{s.get('size') or 'leftover'} | {s.get('sell') or 'list'} | "
            f"{s.get('s_boost') or 'none'} | "
            f"{_pct(s.get('win_rate'))} | {_pct(s.get('profitable_day_rate'))} | "
            f"{s.get('start_green') or 0}/{s.get('start_n') or 0} | "
            f"{_n(s.get('median_start_pct'))} | "
            f"{s.get('gainer_hits') or 0} | {s.get('loser_hits') or 0} | "
            f"{_n(s.get('avg_win_pct'))} | {_n(s.get('avg_loss_pct'))} | "
            f"{_n(s.get('total_ret_pct'))} | {_n(s.get('signal_ret_pct'))} | "
            f"{aud} | {s.get('effectiveness')} |"
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    write_dash_html(payload)
    if books:
        from . import factor_mine_book as fmb
        featured = payload.get("featured") or [
            s["name"] for s in (stats or []) if s.get("reliable")][:8]
        fmb.write_action_mds(payload, stats or [], books, featured)


def write_dash_html(payload: dict) -> Path:
    """Bake the current template + sim.js + payload into Pages HTML."""
    from . import factor_mine_combo as fmc
    payload = fmc.enrich_payload_legs(payload)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    dest = DASH_DIR / "index.html"
    if not TEMPLATE.is_file():
        return dest
    html = TEMPLATE.read_text(encoding="utf-8")
    if SIM_JS.is_file() and "__SIM_JS__" in html:
        html = html.replace("__SIM_JS__", SIM_JS.read_text(encoding="utf-8"))
    html = html.replace("__DATA__", encode_payload(payload))
    dest.write_text(html, encoding="utf-8")
    return dest


def load_dash_payload() -> dict:
    """Prefer the baked .io payload so a restamp does not remine."""
    html_path = DASH_DIR / "index.html"
    if html_path.is_file():
        text = html_path.read_text(encoding="utf-8")
        marker = 'const B64 = "'
        start = text.find(marker)
        if start >= 0:
            start += len(marker)
            end = text.find('"', start)
            if end > start:
                return decode_payload(text[start:end])
    if OUT_JSON.is_file():
        return json.loads(OUT_JSON.read_text(encoding="utf-8"))
    raise FileNotFoundError("no baked factor-mine dashboard payload")


def restamp_dash() -> dict:
    """Rewrite Pages HTML from the current template; keep the last payload.

    Rebuilds the sim pack from the panel so new look-list columns
    (RSI / MACD / Flow) land on already-mined sleeves without a remine.
    """
    payload = load_dash_payload()
    panel = load_or_build_panel(
        payload.get("from_date") or START,
        payload.get("to_date"),
        rebuild=False,
    )
    attach_tape_flow(panel)
    from . import factor_mine_probe as fmp
    from . import factor_mine_sim as fms
    fmp.attach_erd_polarity(panel)
    bought = _bought_tickers(payload.get("books"), payload.get("starts"))
    payload["probe"] = fmp.slim_probe(fmp.build_probe(panel), bought)
    payload["sim"] = fms.build_sim_pack(panel)
    payload["generated_at"] = datetime.now(tl.ET).isoformat()
    dest = write_dash_html(payload)
    print(f"[factor-mine] restamp-dash → {dest} "
          f"to={payload.get('to_date')} recipes={payload.get('n_recipes')}",
          flush=True)
    return payload


def encode_payload(payload: dict) -> str:
    """base64(gzip(compact JSON)) for the dashboard template's __DATA__.

    2026-09-10: plain JSON was 76MB inside index.html (GitHub rejects files
    over 100MB and the .io page took a minute to parse). Gzip lands ~8.5MB
    (~11.5MB as base64) and the browser inflates it with DecompressionStream.
    """
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(gzip.compress(raw, compresslevel=9)).decode("ascii")


def decode_payload(b64: str) -> dict:
    """Inverse of encode_payload (tests, offline readers)."""
    return json.loads(gzip.decompress(base64.b64decode(b64)).decode("utf-8"))


def _pct(v) -> str:
    return "—" if v is None else f"{100 * float(v):.0f}%"


def _n(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


NEWS_CAM_SPLICE = (
    "union_news_pack_h1",
    "union_news_pack_net2_h1",
    "union_news_pack_net3_h1",
    "union_news_or_h1",
    "union_news_or_net2_h1",
    "union_news_or_net4_h1",
    "union_news_or_net4_conv_h1",
    "union_news_g_cam91_n1_h1",
    "union_news_both_h1",
    "short_news_or_h3",
    "short_news_head_h3",
)
NEWS_CAM_COMBOS = (
    {
        "name": "combo_ps_5050_shared",
        "members": ["union_news_pack_h1", "short_news_r_h3"],
        "weights": [1.0, 1.0],
        "net": "priority",
        "pool": "shared",
    },
    {
        "name": "combo_ps_7030_shared",
        "members": ["union_news_pack_h1", "short_news_r_h3"],
        "weights": [70.0, 30.0],
        "net": "priority",
        "pool": "shared",
    },
    {
        "name": "combo_p2s_5050_shared",
        "members": ["union_news_pack_net2_h1", "short_news_r_h3"],
        "weights": [1.0, 1.0],
        "net": "priority",
        "pool": "shared",
    },
)
NEWS_CAM_PIN = (
    "union_news_pack_net2_h1",
    "combo_p2s_5050_shared",
    "union_news_pack_h1",
    "combo_ps_5050_shared",
    "union_news_or_h1",
    "union_news_or_net4_h1",
    "union_news_g_cam91_n1_h1",
    "short_news_head_h3",
)
TAPE_FLOW_SPLICE = (
    "union_rsi_os_h1",
    "union_rsi_os_h3",
    "union_macd_up_h1",
    "union_macd_up_h3",
    "union_macd_xup_h1",
    "union_flow_in_h1",
    "union_flow_in_h3",
    "union_rsi_os_macd_h1",
    "union_flow_in_white_h1",
    "union_rsi_h1",
    "union_macd_hist_h1",
    "short_rsi_ob_h1",
    "short_rsi_ob_h3",
    "short_macd_dn_h3",
)
TAPE_FLOW_PIN = (
    "union_rsi_os_h1",
    "union_flow_in_h1",
    "union_macd_xup_h1",
    "union_rsi_os_macd_h1",
    "short_rsi_ob_h3",
)


def merge_stats_into_payload(
    payload: dict,
    stats: list[dict],
    books: dict | None = None,
    recipes: list[dict] | None = None,
    *,
    pin: list[str] | tuple[str, ...] | None = None,
) -> dict:
    """Insert or replace named recipe rows without remine."""
    books = books or {}
    names = {s["name"] for s in stats if s.get("name")}
    dates = list(payload.get("dates") or [])

    def keep(name: str) -> bool:
        return name not in names

    payload["stats"] = [
        s for s in (payload.get("stats") or []) if keep(s.get("name"))]
    if recipes:
        payload["recipes"] = [
            r for r in (payload.get("recipes") or []) if keep(r.get("name"))]
        payload["recipes"].extend(recipes)
    for key in ("series", "daily", "starts", "books"):
        blob = payload.get(key) or {}
        payload[key] = {k: v for k, v in blob.items() if keep(k)}

    for s in stats:
        name = s["name"]
        slim = {k: v for k, v in s.items()
                if k not in ("daily", "equity", "starts")}
        payload["stats"].append(slim)
        eq = s.get("equity") or []
        payload["series"][name] = (
            eq[1:] if dates and len(eq) == len(dates) + 1 else eq)
        payload["daily"][name] = _slim_dash_daily(s.get("daily"))
        payload["starts"][name] = s.get("starts")
        if name in books:
            payload["books"][name] = _slim_dash_book(books[name])
    payload["n_recipes"] = len(payload["stats"])
    if pin:
        seen: set[str] = set()
        feat: list[str] = []
        have = {s.get("name") for s in payload["stats"]}
        for n in list(pin) + list(payload.get("featured") or []):
            if n and n not in seen and n in have:
                feat.append(n)
                seen.add(n)
        payload["featured"] = feat
    stamp_explains(payload)
    from . import factor_mine_combo as fmc
    for s in payload["stats"]:
        if not str(s.get("name") or "").startswith("combo_"):
            continue
        spec = {
            "name": s["name"],
            "members": s.get("members") or [],
            "weights": s.get("weights") or [1],
            "net": s.get("net") or "priority",
            "pool": s.get("pool") or "shared",
        }
        if spec["members"]:
            s["explain"] = fmc.explain_combo(spec)
    return payload


def splice_news_cam(*, write: bool = True) -> dict:
    """Cash-book the news-packet / headline / camera recipes onto the board."""
    from . import factor_mine_book as fmb
    from . import factor_mine_combo as fmc
    from . import factor_mine_sim as fms

    panel = load_or_build_panel(START, None, rebuild=False)
    payload = load_dash_payload()
    rec_by = {r["name"]: r for r in build_recipes()}
    recipes = [rec_by[n] for n in NEWS_CAM_SPLICE if n in rec_by]
    tapes = _tapes(list(panel.get("session_dates") or []))
    regime = fmb.load_regime()
    fees = pt_fees()
    stats: list[dict] = []
    books: dict = {}
    for rec in recipes:
        print(f"[splice] {rec['name']}", flush=True)
        st = score_recipe(panel, rec, tapes)
        bk = fmb.simulate_book(panel, rec, fees=fees, regime=regime)
        starts = fmb.replay_starts(panel, rec, fees=fees, regime=regime)
        st = fmb.attach_book(st, bk, starts)
        stats.append(st)
        books[rec["name"]] = bk
    member_stat_by = {s["name"]: s for s in stats}
    for s in payload.get("stats") or []:
        if s.get("name") and s["name"] not in member_stat_by:
            member_stat_by[s["name"]] = s
    combo_stats, combo_books = fmc.run_combos(
        panel, list(rec_by.values()), fees=fees, regime=regime,
        specs=list(NEWS_CAM_COMBOS), member_stat_by=member_stat_by)
    stats.extend(combo_stats)
    books.update(combo_books)
    recipes = recipes + [fmc.combo_recipe(sp) for sp in NEWS_CAM_COMBOS]
    merge_stats_into_payload(
        payload, stats, books, recipes, pin=NEWS_CAM_PIN)
    payload["sim"] = fms.build_sim_pack(panel)
    payload["generated_at"] = datetime.now(tl.ET).isoformat()
    payload["n_recipes"] = len(payload["stats"])
    if write:
        write_outputs(payload, payload["stats"], books=None)
    print("[splice] news-cam books", flush=True)
    print(f"{'name':32s} {'book%':>8} {'starts':>8} {'fills':>5}", flush=True)
    for s in stats:
        print(
            f"{s['name']:32s} {_n(s.get('total_ret_pct')):>8} "
            f"{s.get('start_green') or 0:>2}/{s.get('start_n') or 0:<4} "
            f"{s.get('book_n_trades') or 0:>5}",
            flush=True,
        )
    return payload


def _persist_panel(panel: dict) -> None:
    PANEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    slim = {k: v for k, v in panel.items() if k != "by_date"}
    slim["by_date"] = None
    PANEL_PATH.write_text(json.dumps(slim, indent=2), encoding="utf-8")


def splice_tape_flow(*, write: bool = True) -> dict:
    """Cash-book RSI / MACD / flow-in recipes onto the last board."""
    from . import factor_mine_book as fmb
    from . import factor_mine_sim as fms

    panel = load_or_build_panel(START, None, rebuild=False)
    attach_tape_flow(panel)
    payload = load_dash_payload()
    rec_by = {r["name"]: r for r in build_recipes()}
    recipes = [rec_by[n] for n in TAPE_FLOW_SPLICE if n in rec_by]
    tapes = _tapes(list(panel.get("session_dates") or []))
    regime = fmb.load_regime()
    fees = pt_fees()
    stats: list[dict] = []
    books: dict = {}
    for rec in recipes:
        print(f"[splice] {rec['name']}", flush=True)
        st = score_recipe(panel, rec, tapes)
        bk = fmb.simulate_book(panel, rec, fees=fees, regime=regime)
        starts = fmb.replay_starts(panel, rec, fees=fees, regime=regime)
        st = fmb.attach_book(st, bk, starts)
        stats.append(st)
        books[rec["name"]] = bk
    merge_stats_into_payload(
        payload, stats, books, recipes, pin=TAPE_FLOW_PIN)
    payload["sim"] = fms.build_sim_pack(panel)
    payload["generated_at"] = datetime.now(tl.ET).isoformat()
    payload["n_recipes"] = len(payload["stats"])
    if write:
        _persist_panel(panel)
        write_outputs(payload, payload["stats"], books=None)
    print("[splice] tape-flow books", flush=True)
    print(f"{'name':32s} {'book%':>8} {'starts':>8} {'fills':>5}", flush=True)
    for s in stats:
        print(
            f"{s['name']:32s} {_n(s.get('total_ret_pct')):>8} "
            f"{s.get('start_green') or 0:>2}/{s.get('start_n') or 0:<4} "
            f"{s.get('book_n_trades') or 0:>5}",
            flush=True,
        )
    return payload


def existing_single_recipes(payload: dict | None = None) -> list[dict]:
    """Recipes already on the published board — no combo rows, no remine grid."""
    doc = payload
    if doc is None and OUT_JSON.is_file():
        try:
            doc = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            doc = {}
    recs = []
    for rec in (doc or {}).get("recipes") or []:
        if rec.get("universe") == "combo" or rec.get("members"):
            continue
        if rec.get("name"):
            recs.append(rec)
    return recs


def payload_covers_session(payload: dict | None, date: str) -> bool:
    """True when cash-start / books / mornings already include ``date``."""
    if not payload or not date:
        return False
    if date not in (payload.get("dates") or []):
        return False
    if str(payload.get("to_date") or "") < date:
        return False
    daily = payload.get("daily") or {}
    has_day = any(
        any(row.get("date") == date for row in (days or []))
        for days in daily.values()
    )
    if not has_day:
        return False
    mornings = payload.get("mornings") or {}
    sim_s = ((payload.get("sim") or {}).get("s") or {})
    if date not in mornings and date not in sim_s:
        return False
    return True


def land_closed(from_date: str = START, write: bool = False,
                rebuild_panel: bool = False) -> dict:
    """Roll the existing recipe set through the last closed session.

    Does not rediscover the cartesian grid. Morning Pre-Open / Stock Book
    triggers become a no-op once yesterday is already on the board;
    post-close / 16:25 ET schedule lands today.
    """
    closed = last_closed_session(from_date)
    if not closed:
        print("[factor-mine] land-closed: no closed session yet", flush=True)
        return {}
    payload = {}
    if OUT_JSON.is_file():
        try:
            payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
    if not rebuild_panel and payload_covers_session(payload, closed):
        print(f"[factor-mine] land-closed: {closed} already on the board — skip",
              flush=True)
        return payload
    recs = existing_single_recipes(payload)
    if not recs:
        from . import factor_mine_book as fmb
        recs = fmb.recipes_from_action(auto_tweak=False)
    print(f"[factor-mine] land-closed → {closed} recipes={len(recs)}",
          flush=True)
    return run(
        from_date, closed, write=write, recipes=recs,
        rebuild_panel=rebuild_panel, persist_panel=write,
        book=True, combos=True,
    )


def sweep_white_horizon(panel: dict, *, bars=None, fees=None,
                        regime=None) -> list[dict]:
    """Cash-book grid: pool first, then rank, across holds / top_n / ranks."""
    from . import factor_mine_book as fmb
    from . import paper_trade as pt
    panel = ensure_sim_fields(rehydrate_panel(panel))
    fees = fees if fees is not None else pt.load_fees()
    pools = (
        ("yday", {"cam_bad_max": 0, "yday_up": True}),
        ("any", {"cam_bad_max": 0, "yday_or_catalyst": True}),
        ("both", {"cam_bad_max": 0, "yday_and_catalyst": True}),
        ("cat", {"cam_bad_max": 0, "major_catalyst": True}),
    )
    ranks = ("list", "cond", "hot_score")
    holds = (1, 2, 3, 5)
    tops = (4, 8, 12)
    out: list[dict] = []
    for pool, req in pools:
        for hold in holds:
            for top_n in tops:
                for rank in ranks:
                    rec = make_recipe(
                        name=f"sweep_{pool}_h{hold}_n{top_n}_{rank}",
                        universe="union", hold=hold, top_n=top_n,
                        rank=rank, require=req, forbid={"alarm": True},
                    )
                    bk = fmb.simulate_book(
                        panel, rec, bars=bars, fees=fees, regime=regime)
                    fills = sum(1 for d in (bk.get("daily") or [])
                                if d.get("bought"))
                    days = len(bk.get("daily") or [])
                    out.append({
                        "name": rec["name"],
                        "pool": pool,
                        "hold": hold,
                        "top_n": top_n,
                        "rank": rank,
                        "book_pct": bk.get("total_ret_pct"),
                        "win_rate": bk.get("win_rate"),
                        "n_trades": bk.get("n_trades"),
                        "fill_mornings": fills,
                        "n_days": days,
                    })
    out.sort(key=lambda r: (
        -(r.get("book_pct") if r.get("book_pct") is not None else -999),
        r["name"],
    ))
    return out


def sweep_bracket(panel: dict, *, bars=None, fees=None,
                  regime=None) -> list[dict]:
    """Take-profit / stop-loss on top of hold, on a few proven bases."""
    from . import factor_mine_book as fmb
    from . import paper_trade as pt
    panel = ensure_sim_fields(rehydrate_panel(panel))
    fees = fees if fees is not None else pt.load_fees()
    named = {r["name"]: r for r in build_recipes()}
    bases = []
    for name in (
        "union_white_both_n4_h5",
        "union_white_both_n4_h2",
        "union_white_any_h5",
        "union_e_fresh_h3",
        "flatten_h5",
    ):
        if name in named:
            bases.append(named[name])
    takes = (None, 0.03, 0.05, 0.08, 0.12, 0.20)
    stops = (None, 0.03, 0.05, 0.08, 0.12)
    out: list[dict] = []
    for base in bases:
        for take in takes:
            for stop in stops:
                if take is None and stop is None:
                    tag = f"{base['name']}_plain"
                else:
                    t = "x" if take is None else f"{int(round(100 * take))}"
                    s = "x" if stop is None else f"{int(round(100 * stop))}"
                    tag = f"{base['name']}_t{t}s{s}"
                rec = dict(base)
                rec["name"] = tag
                rec["take_pct"] = take
                rec["stop_pct"] = stop
                bk = fmb.simulate_book(
                    panel, rec, bars=bars, fees=fees, regime=regime)
                fills = sum(1 for d in (bk.get("daily") or [])
                            if d.get("bought"))
                kinds = {}
                for t in bk.get("trades") or []:
                    reason = str(t.get("reason") or "")
                    if t.get("side") not in ("SELL", "COVER"):
                        continue
                    if "take-profit" in reason:
                        kinds["take"] = kinds.get("take", 0) + 1
                    elif "stop-loss" in reason:
                        kinds["stop"] = kinds.get("stop", 0) + 1
                out.append({
                    "name": tag,
                    "base": base["name"],
                    "take_pct": take,
                    "stop_pct": stop,
                    "book_pct": bk.get("total_ret_pct"),
                    "win_rate": bk.get("win_rate"),
                    "n_trades": bk.get("n_trades"),
                    "fill_mornings": fills,
                    "n_days": len(bk.get("daily") or []),
                    "n_take": kinds.get("take", 0),
                    "n_stop": kinds.get("stop", 0),
                })
    out.sort(key=lambda r: (
        -(r.get("book_pct") if r.get("book_pct") is not None else -999),
        r["name"],
    ))
    return out


def main(argv=None) -> int:
    from . import factor_mine_book as fmb
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default=START)
    ap.add_argument("--to-date", default="")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--restamp-dash", action="store_true",
                    help="rewrite dashboard HTML from the current template; no remine")
    ap.add_argument("--splice-news-cam", action="store_true",
                    help="cash-book news packet / headline / camera recipes onto the last board")
    ap.add_argument("--splice-tape-flow", action="store_true",
                    help="cash-book RSI / MACD / flow-in recipes onto the last board")
    ap.add_argument("--sweep-white", action="store_true",
                    help="cash-book sweep: −0 red + (yday/catalyst) × hold × rank")
    ap.add_argument("--sweep-bracket", action="store_true",
                    help="cash-book sweep: take-profit / stop-loss on top of hold")
    ap.add_argument("--rebuild-panel", action="store_true")
    ap.add_argument("--land-closed", action="store_true",
                    help="reuse existing recipes; mine through last closed session")
    ap.add_argument("--universe", default="auto", choices=fmb.UNIVERSES)
    ap.add_argument("--hold", default="auto", choices=fmb.HOLDS)
    ap.add_argument("--gate", default="auto", choices=fmb.GATES)
    ap.add_argument("--rank", default="auto", choices=fmb.RANKS)
    ap.add_argument("--side", default="auto", choices=fmb.SIDES)
    ap.add_argument("--top-n", default="auto", choices=fmb.TOP_NS)
    ap.add_argument("--exit", default="auto", choices=fmb.EXITS)
    ap.add_argument("--entry", default="auto", choices=fmb.ENTRIES)
    ap.add_argument("--size", default="auto", choices=fmb.SIZES)
    ap.add_argument("--sell", default="auto", choices=fmb.SELLS)
    ap.add_argument("--s-boost", dest="s_boost", default="auto",
                    choices=fmb.S_BOOSTS)
    ap.add_argument("--auto-tweak", dest="auto_tweak", action="store_true",
                    default=True)
    ap.add_argument("--no-auto-tweak", dest="auto_tweak", action="store_false")
    ap.add_argument("--no-book", action="store_true",
                    help="signal-only (do not use; cash book is the default)")
    ap.add_argument("--no-combo", action="store_true",
                    help="skip combination books (single-recipe mine only)")
    args = ap.parse_args(argv)
    if args.restamp_dash:
        payload = restamp_dash()
        print(f"[factor-mine] recipes={payload.get('n_recipes')} "
              f"to={payload.get('to_date')}")
        return 0
    if args.splice_news_cam:
        payload = splice_news_cam(write=args.write)
        print(f"[factor-mine] splice-news-cam recipes={payload.get('n_recipes')} "
              f"to={payload.get('to_date')}")
        return 0
    if args.splice_tape_flow:
        payload = splice_tape_flow(write=args.write)
        print(f"[factor-mine] splice-tape-flow recipes={payload.get('n_recipes')} "
              f"to={payload.get('to_date')}")
        return 0
    if args.sweep_white:
        panel = load_or_build_panel(
            args.from_date, args.to_date or None,
            rebuild=args.rebuild_panel)
        rows = sweep_white_horizon(panel)
        print(f"[factor-mine] white-horizon sweep n={len(rows)} "
              f"to={panel.get('to_date')}", flush=True)
        print(f"{'name':40s} {'book%':>8} {'win':>6} {'trd':>5} "
              f"{'fill':>7} pool hold n rank")
        for r in rows:
            print(f"{r['name']:40s} {_n(r.get('book_pct')):>8} "
                  f"{_pct(r.get('win_rate')):>6} {r.get('n_trades') or 0:>5} "
                  f"{r.get('fill_mornings') or 0:>3}/{r.get('n_days') or 0:<3} "
                  f"{r['pool']:4s} h{r['hold']} n{r['top_n']} {r['rank']}")
        return 0
    if args.sweep_bracket:
        panel = load_or_build_panel(
            args.from_date, args.to_date or None,
            rebuild=args.rebuild_panel)
        rows = sweep_bracket(panel)
        print(f"[factor-mine] bracket sweep n={len(rows)} "
              f"to={panel.get('to_date')}", flush=True)
        print(f"{'name':42s} {'book%':>8} {'win':>6} {'trd':>5} "
              f"{'take':>4} {'stop':>4} base")
        for r in rows:
            print(f"{r['name']:42s} {_n(r.get('book_pct')):>8} "
                  f"{_pct(r.get('win_rate')):>6} {r.get('n_trades') or 0:>5} "
                  f"{r.get('n_take') or 0:>4} {r.get('n_stop') or 0:>4} "
                  f"{r['base']}")
        return 0
    if args.land_closed:
        payload = land_closed(
            args.from_date, write=args.write,
            rebuild_panel=args.rebuild_panel,
        )
    else:
        recipes = fmb.recipes_from_action(
            universe=args.universe, hold=args.hold, gate=args.gate,
            rank=args.rank, side=args.side, top_n=args.top_n, exit=args.exit,
            entry=args.entry, size=args.size, sell=args.sell,
            s_boost=args.s_boost, auto_tweak=args.auto_tweak,
        )
        payload = run(
            args.from_date, args.to_date or None, write=args.write,
            recipes=recipes, rebuild_panel=args.rebuild_panel,
            persist_panel=args.write, book=not args.no_book,
            combos=not args.no_combo,
        )
    print(f"[factor-mine] recipes={payload.get('n_recipes')} "
          f"rows={payload.get('n_rows')} sessions={payload.get('n_sessions')} "
          f"to={payload.get('to_date')}")
    for s in (payload.get("stats") or [])[:8]:
        print(f"  {s['name']:32s}  win={_pct(s.get('win_rate'))}  "
              f"starts={s.get('start_green')}/{s.get('start_n')}  "
              f"tot={_n(s.get('total_ret_pct'))}%  eff={s.get('effectiveness')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
