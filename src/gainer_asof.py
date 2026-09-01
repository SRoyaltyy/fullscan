"""Top-gainer as-of walk — what the golden inputs said before the open.

For every session since the dashboard start (2026-08-13):

  * realized liquid gainers at 2% and 5% (same-day Finviz Change%)
  * realized liquid losers at −2% and −5%
  * the names the 1d BUY and 1d SELL lists printed that morning
  * the 12 lookback boxes plus yΔ (yesterday's Change%, prior tape)
  * hall-pass lane (standard / group leader / catalyst / …)

Boxes come from the 09:30 ET packet. Same-day RelVol and same-day
stock book never color a cell. Change% / sleeve realized Δ are outcomes.
Missing-as-of (era-skip) prints grey.

Writes:
  03_scoreboard/TOP_GAINER_ASOF.md
  03_scoreboard/top_gainer_asof.json

CLI: python -m src.gainer_asof --floors 2,5 --all --buys --sells --losers --write
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import book_era, ticker_lookback as tl
from . import ticker_lookback_cli as scan

ROOT = Path(__file__).resolve().parent.parent
OUT_MD = ROOT / "03_scoreboard" / "TOP_GAINER_ASOF.md"
OUT_JSON = ROOT / "03_scoreboard" / "top_gainer_asof.json"
EXPORT_DIR = ROOT / "data" / "exports"
BOOK_DIR = ROOT / "data" / "stock_book"

START = book_era.DASHBOARD_START
TOP_N = 15
MIN_CHANGE = 5.0
FLOORS = (2.0, 5.0)
MIN_MCAP_M = 100.0
MIN_AVG_VOL_K = 500.0
LOSER_FLOOR = -2.0
COVERAGE_OK = 0.45
REGIME_EDGE = 15.0
STABLE_EDGE = 8.0
GREY = "⬜"
LANES = (
    "standard",
    "group_leader",
    "catalyst",
    "catalyst_exception",
    "probable",
    "blocked",
)
LANE_LABELS = {
    "standard": "standard",
    "group_leader": "group leader",
    "catalyst": "catalyst",
    "catalyst_exception": "catalyst exception",
    "probable": "probable",
    "blocked": "blocked",
}

GAINER_BOX_COLS = tl.BOX_COLS + (("yday", "yΔ"),)
DOMAIN_COLS = (
    ("market", "mkt"),
    ("parent", "par"),
    ("child", "chd"),
    ("company", "co"),
    ("setup", "set"),
    ("flow", "flw"),
)
BOX_ERA = {
    "join": "join",
    "sector": "sector_predict",
    "gen": "general_predict",
    "news": "news_actions",
    "digest": "finviz_digest",
    "judge": "news_judge",
    "ab": "ab_enriched",
    "peer": "peer_rs",
    "heat": "map_heat",
    "vol": None,
    "catal": "catalyst",
    "buy": "stock_book",
    "yday": None,
}
DOMAIN_ERA = {
    "market": "general_predict",
    "parent": "sector_predict",
    "child": "map_heat",
    "company": "news_actions",
    "setup": "ab_enriched",
    "flow": None,
}

_WALK = None


def _num(x, default=None):
    return tl._num(x, default)


def _pct(x):
    return _num(x)


def _legend() -> str:
    return " · ".join(lab for _, lab in GAINER_BOX_COLS)


def _domain_legend() -> str:
    return " · ".join(lab for _, lab in DOMAIN_COLS)


def _icon(tone, era: bool = False) -> str:
    if era and (tone or "missing") == "missing":
        return GREY
    return tl.BOX_ICON.get(tone, "⬛")


def _labeled(boxes: dict | None, era_skip: list[str] | None = None) -> str:
    skip = set(era_skip or [])
    return " ".join(
        f"{lab}{_icon((boxes or {}).get(key), era=key in skip)}"
        for key, lab in GAINER_BOX_COLS
    )


def _labeled_domains(domains: dict | None, era_skip: list[str] | None = None) -> str:
    skip = set(era_skip or [])
    return " ".join(
        f"{lab}{_icon((domains or {}).get(key), era=key in skip)}"
        for key, lab in DOMAIN_COLS
    )


def lane_label(lane: str | None) -> str:
    if not lane:
        return "—"
    return LANE_LABELS.get(lane, str(lane).replace("_", " "))


def infer_lane(domains: dict | None, *, market_state: str | None = None,
               saved: str | None = None) -> str:
    """Hall-pass lane from as-of domains, else the book's saved lane."""
    d = domains or {}
    setup = d.get("setup") or "missing"
    flow = d.get("flow") or "missing"
    company = d.get("company") or "missing"
    child = d.get("child") or "missing"
    parent = d.get("parent") or "missing"
    state = str(market_state or "").lower()
    saved_ok = saved if saved in LANES else None

    if setup == "bad":
        return "blocked"

    catalyst = company == "good" and setup == "good" and flow != "bad"
    group = (
        child == "good"
        and setup == "good"
        and flow != "bad"
        and company != "bad"
    )
    standard = (
        parent != "bad"
        and child != "bad"
        and company != "bad"
        and setup == "good"
        and flow != "bad"
    )

    if state == "hard_red":
        if catalyst:
            return "catalyst_exception"
        if standard or group:
            return "probable"
        return saved_ok or "blocked"
    if catalyst:
        return "catalyst"
    if group:
        return "group_leader"
    if standard:
        return "standard"
    return saved_ok or "blocked"


def _chg_tone(chg) -> str:
    v = _pct(chg)
    if v is None:
        return "missing"
    if v > 0:
        return "good"
    if v < 0:
        return "bad"
    return "neutral"


def same_day_side_rows(date: str, side: str) -> list[dict]:
    path = BOOK_DIR / f"{date}_stock_book.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    rows = ((data.get("books") or {}).get("1d") or {}).get(side) or []
    out = []
    for i, r in enumerate(rows, 1):
        if not isinstance(r, dict):
            continue
        t = tl._tick(r.get("ticker"))
        if not t:
            continue
        out.append({
            "ticker": t,
            "company": r.get("company") or "",
            "sector": r.get("sector") or "",
            "industry": r.get("industry") or r.get("group_label") or "",
            "rank": i,
            "score": _num(r.get("score")),
            "size": r.get("size") or "",
            "side": side,
            "decision_lane": r.get("decision_lane") or r.get("lane") or "",
            "lb_blue": bool(r.get("lb_blue") or r.get("blue")),
            "lb_alarm": bool(r.get("lb_alarm") or r.get("alarm")),
            "lb_zero_red": bool(r.get("lb_zero_red") or r.get("white")),
        })
    return out


def same_day_buy_rows(date: str) -> list[dict]:
    return same_day_side_rows(date, "buy")


def same_day_sell_rows(date: str) -> list[dict]:
    return same_day_side_rows(date, "sell")


def same_day_buy_set(date: str) -> set[str]:
    return {r["ticker"] for r in same_day_buy_rows(date)}


def same_day_sell_set(date: str) -> set[str]:
    return {r["ticker"] for r in same_day_sell_rows(date)}


def load_finviz(date: str) -> pd.DataFrame:
    path = EXPORT_DIR / f"finviz_{date}.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (OSError, ValueError, pd.errors.ParserError):
        return pd.DataFrame()


def tape_coverage(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {"status": "missing", "n": 0, "n_change": 0, "frac": 0.0}
    chg = df["Change"].map(_pct) if "Change" in df.columns else pd.Series(dtype=float)
    n = int(len(df))
    n_change = int((chg.fillna(0.0) != 0.0).sum()) if n else 0
    frac = (n_change / n) if n else 0.0
    status = "full" if frac >= COVERAGE_OK else "partial"
    if n_change == 0:
        status = "empty"
    return {"status": status, "n": n, "n_change": n_change, "frac": round(frac, 3)}


def _liquid_tape(df: pd.DataFrame, *, top_n: int, min_change: float,
                 liquid: bool, min_mcap_m: float | None,
                 side: str = "up") -> list[dict]:
    if df is None or df.empty:
        return []
    work = df.copy()
    if "Ticker" not in work.columns or "Change" not in work.columns:
        return []
    floor = MIN_MCAP_M if min_mcap_m is None else float(min_mcap_m)
    work["ticker"] = work["Ticker"].map(tl._tick)
    work["chg"] = work["Change"].map(_pct)
    work["mcap"] = pd.to_numeric(work["Market Cap"], errors="coerce") if "Market Cap" in work.columns else float("nan")
    work["adv"] = pd.to_numeric(work["Average Volume"], errors="coerce") if "Average Volume" in work.columns else float("nan")
    work["volume"] = pd.to_numeric(work["Volume"], errors="coerce") if "Volume" in work.columns else float("nan")
    if "Industry" in work.columns:
        not_etf = ~work["Industry"].astype(str).eq("Exchange Traded Fund")
    else:
        not_etf = True
    chg_ok = (
        work["chg"] >= float(min_change)
        if side == "up"
        else work["chg"] <= float(min_change)
    )
    keep = (
        work["ticker"].astype(bool)
        & work["chg"].notna()
        & chg_ok
        & (work["volume"].fillna(0) > 0)
        & not_etf
    )
    if liquid:
        keep = keep & (work["mcap"] >= floor) & (work["adv"] >= MIN_AVG_VOL_K)
    ranked = work.loc[keep].sort_values("chg", ascending=(side != "up"))
    if top_n and int(top_n) > 0:
        ranked = ranked.head(int(top_n))
    out = []
    for rec in ranked.to_dict(orient="records"):
        out.append({
            "ticker": rec["ticker"],
            "company": rec.get("Company") or "",
            "sector": rec.get("Sector") or "",
            "industry": rec.get("Industry") or "",
            "change_pct": round(float(rec["chg"]), 2),
            "mcap_m": _num(rec.get("mcap")),
            "avg_vol_k": _num(rec.get("adv")),
        })
    return out


def liquid_gainers(df: pd.DataFrame, top_n: int = TOP_N,
                   min_change: float = 0.0, liquid: bool = True,
                   min_mcap_m: float | None = None) -> list[dict]:
    return _liquid_tape(
        df, top_n=top_n, min_change=min_change, liquid=liquid,
        min_mcap_m=min_mcap_m, side="up",
    )


def liquid_losers(df: pd.DataFrame, top_n: int = TOP_N,
                  min_change: float | None = None, liquid: bool = True,
                  min_mcap_m: float | None = None) -> list[dict]:
    floor = LOSER_FLOOR if min_change is None else float(min_change)
    if floor > 0:
        floor = -abs(floor)
    return _liquid_tape(
        df, top_n=top_n, min_change=floor, liquid=liquid,
        min_mcap_m=min_mcap_m, side="down",
    )


def _era_skip(date: str) -> list[str]:
    skip = []
    for key, feature in {**BOX_ERA, **DOMAIN_ERA}.items():
        if feature and not book_era.live(date, feature):
            skip.append(key)
    return skip


def _blend_tones(tones) -> str:
    printed = [t for t in tones if t in ("good", "bad", "neutral")]
    if not printed:
        return "missing"
    goods = printed.count("good")
    bads = printed.count("bad")
    if goods and not bads:
        return "good"
    if bads and not goods:
        return "bad"
    if goods and bads:
        return "neutral"
    return "neutral"


def _derive_domains(boxes: dict, market_tone: str | None = None) -> dict[str, str]:
    """Lattice checklist from as-of source boxes when the book has no d_*."""
    flow = boxes.get("vol") if boxes.get("vol") not in (None, "missing") else boxes.get("peer")
    return {
        "market": market_tone or boxes.get("gen") or "missing",
        "parent": boxes.get("sector") or "missing",
        "child": boxes.get("heat") or "missing",
        "company": _blend_tones([
            boxes.get("news"), boxes.get("digest"),
            boxes.get("judge"), boxes.get("catal"),
        ]),
        "setup": boxes.get("ab") or "missing",
        "flow": flow or "missing",
    }


def load_day_context(date: str) -> dict:
    """Session lattice + per-ticker domains / lanes / marks from that day's book."""
    market_tone = None
    market_state = None
    path = BOOK_DIR / f"{date}_stock_book.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        data = {}
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    lat = meta.get("decision_lattice") if isinstance(meta.get("decision_lattice"), dict) else {}
    market = lat.get("market") or meta.get("market_decision") or {}
    if isinstance(market, dict):
        if market.get("tone") in tl.BOX_ICON:
            market_tone = market.get("tone")
        if market.get("state"):
            market_state = str(market.get("state")).lower()
    if market_tone is None:
        bias = _num(meta.get("general_bias"))
        if bias is not None:
            market_tone = "good" if bias > 0.10 else "bad" if bias < -0.10 else "neutral"
    cmap: dict[str, dict[str, str]] = {}
    lanes: dict[str, str] = {}
    marks: dict[str, dict] = {}
    csv_path = BOOK_DIR / f"{date}_stock_book.csv"
    if csv_path.exists():
        try:
            frame = pd.read_csv(csv_path)
        except (OSError, ValueError, pd.errors.ParserError):
            frame = pd.DataFrame()
        if not frame.empty and "Ticker" in frame.columns:
            has_d = all(f"d_{k}" in frame.columns for k, _ in DOMAIN_COLS)
            for rec in frame.drop_duplicates("Ticker").to_dict(orient="records"):
                t = tl._tick(rec.get("Ticker"))
                if not t:
                    continue
                if has_d:
                    cmap[t] = {
                        key: str(rec.get(f"d_{key}") or "missing")
                        if str(rec.get(f"d_{key}") or "missing") in tl.BOX_ICON
                        else "missing"
                        for key, _ in DOMAIN_COLS
                    }
                lane = rec.get("decision_lane") or rec.get("lane")
                if lane in LANES:
                    lanes[t] = lane
                marks[t] = {
                    "blue": bool(rec.get("lb_blue") or rec.get("domain_blue")),
                    "alarm": bool(rec.get("lb_alarm") or rec.get("domain_alarm")),
                    "white": bool(rec.get("lb_zero_red") or rec.get("domain_white")),
                }
    if not cmap or not lanes:
        for side in ("buy", "sell"):
            for r in ((data.get("books") or {}).get("1d") or {}).get(side) or []:
                if not isinstance(r, dict):
                    continue
                t = tl._tick(r.get("ticker"))
                if not t:
                    continue
                stored = r.get("domain_boxes") if isinstance(r.get("domain_boxes"), dict) else {}
                if stored and t not in cmap:
                    cmap[t] = {
                        key: str(stored.get(key) or "missing")
                        if str(stored.get(key) or "missing") in tl.BOX_ICON
                        else "missing"
                        for key, _ in DOMAIN_COLS
                    }
                lane = r.get("decision_lane") or r.get("lane")
                if lane in LANES and t not in lanes:
                    lanes[t] = lane
                if t not in marks:
                    marks[t] = {
                        "blue": bool(r.get("lb_blue") or r.get("blue")),
                        "alarm": bool(r.get("lb_alarm") or r.get("alarm")),
                        "white": bool(r.get("lb_zero_red") or r.get("white")),
                    }
    return {
        "market_tone": market_tone,
        "market_state": market_state,
        "domains": cmap,
        "lanes": lanes,
        "marks": marks,
    }


def load_day_domains(date: str) -> tuple[str | None, dict[str, dict[str, str]]]:
    """Session market tone + per-ticker lattice domains from that day's book."""
    ctx = load_day_context(date)
    return ctx["market_tone"], ctx["domains"]


def _yday_from_sess(sess: dict | None, ticker: str) -> tuple[str, str | None, float | None]:
    prior = (sess or {}).get("prior")
    fv = ((prior or {}).get("finviz") or {}).get(ticker)
    vintage = (prior or {}).get("date")
    if not fv:
        return "missing", vintage, None
    chg = _pct(fv.get("Change") if fv.get("Change") is not None else fv.get("Change %"))
    return _chg_tone(chg), vintage, None if chg is None else round(float(chg), 2)


def _marks_pack(row: dict, book_marks: dict | None, t: str) -> dict:
    stored = (book_marks or {}).get(t) or {}
    blue = bool(stored.get("blue") or row.get("lb_blue") or row.get("blue"))
    alarm = bool(stored.get("alarm") or row.get("lb_alarm") or row.get("alarm"))
    white = bool(stored.get("white") or row.get("lb_zero_red") or row.get("white"))
    icons = "".join(
        bit for bit, on in (("🔵", blue), ("🚨", alarm), ("⚪", white)) if on
    )
    return {"blue": blue, "alarm": alarm, "white": white, "icons": icons}


def color_name(sess: dict | None, row: dict, buy_today: set[str],
               realized: float | None = None,
               market_tone: str | None = None,
               book_domains: dict[str, dict[str, str]] | None = None,
               sell_today: set[str] | None = None,
               market_state: str | None = None,
               book_lanes: dict[str, str] | None = None,
               book_marks: dict[str, dict] | None = None,
               era_skip: list[str] | None = None) -> dict:
    t = row["ticker"]
    card = scan._scan_session(sess, t) if sess else {}
    card = card or {}
    boxes = dict(card.get("boxes") or {k: "missing" for k, _ in tl.BOX_COLS})
    vintage = dict(card.get("factor_vintage") or {})
    yday, yv, ychg = _yday_from_sess(sess, t)
    boxes["yday"] = yday
    if yv:
        vintage["yday"] = yv
    stored = (book_domains or {}).get(t)
    if stored and all(stored.get(k) not in (None, "", "missing") for k, _ in DOMAIN_COLS):
        domains = {k: stored.get(k) or "missing" for k, _ in DOMAIN_COLS}
        vintage["domains"] = "book"
    elif stored:
        derived = _derive_domains(boxes, market_tone=market_tone)
        domains = {
            k: (stored.get(k) if stored.get(k) in tl.BOX_ICON else derived.get(k) or "missing")
            for k, _ in DOMAIN_COLS
        }
        vintage["domains"] = "book+derived"
    else:
        domains = _derive_domains(boxes, market_tone=market_tone)
        vintage["domains"] = "derived"
    skip = era_skip
    if skip is None:
        date = (sess or {}).get("date") or row.get("date")
        skip = _era_skip(date) if date else []
    cond = tl.general_condition({k: boxes.get(k) for k, _ in tl.BOX_COLS})
    region = tl.color_region({k: boxes.get(k) for k, _ in tl.BOX_COLS})
    chg = row.get("change_pct") if row.get("change_pct") is not None else realized
    saved_lane = (
        (book_lanes or {}).get(t)
        or row.get("decision_lane")
        or row.get("lane")
        or None
    )
    lane = infer_lane(domains, market_state=market_state, saved=saved_lane)
    marks = _marks_pack(row, book_marks, t)
    sell_today = sell_today or set()
    return {
        **row,
        "change_pct": chg,
        "boxes": boxes,
        "domains": domains,
        "labeled": _labeled(boxes, era_skip=skip),
        "labeled_domains": _labeled_domains(domains, era_skip=skip),
        "factor_vintage": vintage,
        "sources": card.get("sources") or [],
        "class": card.get("class") or ("no_session" if not sess else "no_data"),
        "condition": cond,
        "region": region,
        "yday_change": ychg,
        "overnight_buy": boxes.get("buy") == "good",
        "overnight_sell": card.get("class") == "overnight_sell",
        "on_1d_buy": t in buy_today,
        "on_1d_sell": t in sell_today,
        "lane": lane,
        "lane_label": lane_label(lane),
        "marks": marks,
        "bucket": row.get("size") or card.get("size") or "",
        "asof": "09:30_et",
        "prior_date": (sess or {}).get("prior_date"),
    }


def color_gainer(sess: dict, row: dict, buy_today: set[str]) -> dict:
    return color_name(sess, row, buy_today)


def _spy_change(fv: pd.DataFrame):
    if fv is None or fv.empty or "Ticker" not in fv.columns:
        return None
    hit = fv[fv["Ticker"].astype(str).str.upper() == "SPY"]
    if hit.empty:
        return None
    return _pct(hit.iloc[0].get("Change"))


def _finviz_change_map(fv: pd.DataFrame) -> dict[str, float]:
    if fv is None or fv.empty or "Ticker" not in fv.columns:
        return {}
    out = {}
    for rec in fv.to_dict(orient="records"):
        t = tl._tick(rec.get("Ticker"))
        chg = _pct(rec.get("Change"))
        if t and chg is not None:
            out[t] = chg
    return out


def day_walk(date: str, *, idx=None, top_n: int = TOP_N,
             min_change: float = MIN_CHANGE, liquid: bool = True,
             include_buys: bool = True,
             include_sells: bool = False,
             include_losers: bool = False,
             min_mcap_m: float | None = None) -> dict:
    idx = idx or tl.build_index()
    sess = next((s for s in idx["sessions"] if s["date"] == date), None)
    fv = load_finviz(date)
    cov = tape_coverage(fv)
    names = (
        liquid_gainers(
            fv, top_n=top_n, min_change=min_change, liquid=liquid,
            min_mcap_m=min_mcap_m,
        )
        if cov["status"] != "missing" else []
    )
    loser_floor = -abs(float(min_change)) if min_change else LOSER_FLOOR
    loser_names = (
        liquid_losers(
            fv, top_n=top_n, min_change=loser_floor, liquid=liquid,
            min_mcap_m=min_mcap_m,
        )
        if include_losers and cov["status"] != "missing" else []
    )
    buy_meta = same_day_buy_rows(date)
    sell_meta = same_day_sell_rows(date)
    buy_today = {r["ticker"] for r in buy_meta}
    sell_today = {r["ticker"] for r in sell_meta}
    realized = _finviz_change_map(fv)
    ctx = load_day_context(date)
    era_skip = _era_skip(date)
    cache: dict[str, dict] = {}

    def paint(row, chg=None):
        t = row["ticker"]
        if t not in cache:
            cache[t] = color_name(
                sess, row, buy_today, realized=chg,
                market_tone=ctx.get("market_tone"),
                book_domains=ctx.get("domains"),
                sell_today=sell_today,
                market_state=ctx.get("market_state"),
                book_lanes=ctx.get("lanes"),
                book_marks=ctx.get("marks"),
                era_skip=era_skip,
            )
        return cache[t]

    rows = [paint(row) for row in names]
    buys = []
    if include_buys:
        for raw in buy_meta:
            chg = realized.get(raw["ticker"])
            painted = paint({**raw, "change_pct": chg}, chg=chg)
            buys.append(painted)
    sells = []
    if include_sells:
        for raw in sell_meta:
            chg = realized.get(raw["ticker"])
            painted = paint({**raw, "change_pct": chg}, chg=chg)
            sells.append(painted)
    losers = [paint(row) for row in loser_names]
    return {
        "date": date,
        "coverage": cov,
        "spy_change": _spy_change(fv),
        "era_skip": era_skip,
        "market_state": ctx.get("market_state"),
        "n_gainers": len(rows),
        "n_losers": len(losers),
        "n_overnight_buy": sum(1 for r in rows if r.get("overnight_buy")),
        "n_on_1d_buy": sum(1 for r in rows if r.get("on_1d_buy")),
        "n_overnight_sell": sum(1 for r in losers if r.get("overnight_sell")),
        "n_on_1d_sell": sum(1 for r in losers if r.get("on_1d_sell")),
        "min_change": float(min_change),
        "loser_floor": float(loser_floor),
        "liquid": bool(liquid),
        "top_n": top_n,
        "rows": rows,
        "buys": buys,
        "sells": sells,
        "losers": losers,
    }


def _tally(rows: list[dict], era_skip: list[str] | None = None,
           skip_by_date: dict[str, list[str]] | None = None) -> dict:
    counts = {key: Counter() for key, _ in GAINER_BOX_COLS + DOMAIN_COLS}
    n = n_over = n_today = 0
    n_over_sell = n_today_sell = 0
    hit2 = hit5 = hit_neg2 = hit_neg5 = 0
    lanes = Counter()
    chgs = []
    for row in rows:
        n += 1
        if row.get("overnight_buy"):
            n_over += 1
        if row.get("on_1d_buy"):
            n_today += 1
        if row.get("overnight_sell"):
            n_over_sell += 1
        if row.get("on_1d_sell"):
            n_today_sell += 1
        if row.get("lane") in LANES:
            lanes[row["lane"]] += 1
        chg = _pct(row.get("change_pct"))
        if chg is not None:
            chgs.append(chg)
            if chg >= 2:
                hit2 += 1
            if chg >= 5:
                hit5 += 1
            if chg <= -2:
                hit_neg2 += 1
            if chg <= -5:
                hit_neg5 += 1
        skip = set(era_skip or [])
        if skip_by_date is not None:
            skip = set(skip_by_date.get(row.get("date") or "", []) or skip)
        boxes = row.get("boxes") or {}
        domains = row.get("domains") or {}
        for key, _ in GAINER_BOX_COLS:
            tone = boxes.get(key) or "missing"
            if key in skip and tone == "missing":
                counts[key]["era"] += 1
            else:
                counts[key][tone] += 1
        for key, _ in DOMAIN_COLS:
            tone = domains.get(key) or "missing"
            if key in skip and tone == "missing":
                counts[key]["era"] += 1
            else:
                counts[key][tone] += 1

    def pack(cols):
        packed = {}
        for key, _ in cols:
            c = counts[key]
            total = sum(c.values()) or 1
            packed[key] = {
                "good": c.get("good", 0),
                "neutral": c.get("neutral", 0),
                "bad": c.get("bad", 0),
                "missing": c.get("missing", 0),
                "era": c.get("era", 0),
                "good_pct": round(100.0 * c.get("good", 0) / total, 1),
                "bad_pct": round(100.0 * c.get("bad", 0) / total, 1),
                "printed_pct": round(
                    100.0 * (total - c.get("missing", 0) - c.get("era", 0)) / total, 1
                ),
            }
        return packed

    out = pack(GAINER_BOX_COLS)
    domain_out = pack(DOMAIN_COLS)
    chgs_sorted = sorted(chgs)
    mid = chgs_sorted[len(chgs_sorted) // 2] if chgs_sorted else None
    return {
        "n_names": n,
        "n_overnight_buy": n_over,
        "n_on_1d_buy": n_today,
        "overnight_buy_pct": round(100.0 * n_over / n, 1) if n else 0.0,
        "on_1d_buy_pct": round(100.0 * n_today / n, 1) if n else 0.0,
        "n_overnight_sell": n_over_sell,
        "n_on_1d_sell": n_today_sell,
        "overnight_sell_pct": round(100.0 * n_over_sell / n, 1) if n else 0.0,
        "on_1d_sell_pct": round(100.0 * n_today_sell / n, 1) if n else 0.0,
        "n_with_change": len(chgs),
        "median_change": None if mid is None else round(float(mid), 2),
        "hit_2_pct": round(100.0 * hit2 / len(chgs), 1) if chgs else 0.0,
        "hit_5_pct": round(100.0 * hit5 / len(chgs), 1) if chgs else 0.0,
        "hit_neg2_pct": round(100.0 * hit_neg2 / len(chgs), 1) if chgs else 0.0,
        "hit_neg5_pct": round(100.0 * hit_neg5 / len(chgs), 1) if chgs else 0.0,
        "lanes": dict(lanes),
        "boxes": out,
        "domains": domain_out,
    }


def _attach_dates(days: list[dict], key: str) -> list[dict]:
    out = []
    skip = {}
    for day in days:
        skip[day["date"]] = day.get("era_skip") or []
        for row in day.get(key) or []:
            rec = dict(row)
            rec["date"] = day["date"]
            out.append(rec)
    return out, skip


def _split_regime(days: list[dict], key: str) -> tuple[list[dict], list[dict]]:
    up, down = [], []
    for day in days:
        spy = day.get("spy_change")
        if spy is None:
            continue
        bucket = up if spy > 0 else down
        for row in day.get(key) or []:
            rec = dict(row)
            rec["date"] = day["date"]
            bucket.append(rec)
    return up, down


def _insights(floors: dict, buys: dict, regime: dict,
              sells: dict | None = None, losers: dict | None = None) -> list[str]:
    """Plain-language read of the job — robust vs regime-sensitive boxes."""
    lines = ["## What the boxes actually said", ""]
    g5 = (floors.get("5") or {}).get("summary") or {}
    g2 = (floors.get("2") or {}).get("summary") or {}
    bsum = buys.get("summary") or {}
    ssum = (sells or {}).get("summary") or {}
    l2 = ((losers or {}).get("2") or {}).get("summary") or {}
    l5 = ((losers or {}).get("5") or {}).get("summary") or {}
    lines.append(
        f"At ≥5% the book almost never held the rip: overnight BUY "
        f"{g5.get('overnight_buy_pct') or 0:.1f}% · today's 1d BUY "
        f"{g5.get('on_1d_buy_pct') or 0:.1f}% of "
        f"{g5.get('n_names') or 0} liquid winners. "
        f"The 1d BUY sleeve itself "
        f"({bsum.get('n_names') or 0} names) realized a median "
        f"{bsum.get('median_change') if bsum.get('median_change') is not None else '—'}% "
        f"and hit ≥2% / ≥5% on "
        f"{bsum.get('hit_2_pct') or 0:.1f}% / {bsum.get('hit_5_pct') or 0:.1f}% "
        f"of names with a printed Change%."
    )
    if l2 or l5 or ssum:
        med_s = ssum.get("median_change")
        med_s_s = f"{med_s}%" if med_s is not None else "—"
        lines.append("")
        lines.append(
            f"On the down side, overnight SELL caught "
            f"{l2.get('overnight_sell_pct') or 0:.1f}% of ≤-2% losers "
            f"({l2.get('n_names') or 0} names) · today's 1d SELL "
            f"{l2.get('on_1d_sell_pct') or 0:.1f}%. "
            f"At ≤-5% that is overnight "
            f"{l5.get('overnight_sell_pct') or 0:.1f}% · 1d SELL "
            f"{l5.get('on_1d_sell_pct') or 0:.1f}% of "
            f"{l5.get('n_names') or 0} names. "
            f"The 1d SELL sleeve itself "
            f"({ssum.get('n_names') or 0} names) realized a median "
            f"{med_s_s} and closed ≤-2% / ≤-5% on "
            f"{ssum.get('hit_neg2_pct') or 0:.1f}% / "
            f"{ssum.get('hit_neg5_pct') or 0:.1f}% "
            f"of names with a printed Change%."
        )
    lines.append("")

    g5b = {**(g5.get("boxes") or {}), **(g5.get("domains") or {})}
    upb = {
        **((regime.get("spy_up") or {}).get("boxes") or {}),
        **((regime.get("spy_up") or {}).get("domains") or {}),
    }
    dnb = {
        **((regime.get("spy_down") or {}).get("boxes") or {}),
        **((regime.get("spy_down") or {}).get("domains") or {}),
    }
    stable, sensitive, holes = [], [], []
    for key, lab in GAINER_BOX_COLS + DOMAIN_COLS:
        rec = g5b.get(key) or {}
        printed = rec.get("printed_pct") or 0
        if printed < 10:
            holes.append(
                f"**{lab}** printed on {printed:.1f}% of ≥5% winners "
                f"(green {rec.get('good_pct') or 0:.1f}%) — a coverage hole, "
                f"not a failed tone"
            )
            continue
        ug = (upb.get(key) or {}).get("good_pct")
        dg = (dnb.get(key) or {}).get("good_pct")
        if ug is None or dg is None:
            continue
        delta = ug - dg
        bit = (
            f"**{lab}** green {rec.get('good_pct') or 0:.1f}% overall "
            f"(SPY-up {ug:.1f}% / SPY-down {dg:.1f}%, Δ {delta:+.1f})"
        )
        if abs(delta) >= REGIME_EDGE:
            sensitive.append(bit)
        elif abs(delta) <= STABLE_EDGE:
            stable.append(bit)
    if stable:
        lines += [
            "Stable across the tape (green% barely moves when SPY closes up vs down):",
            "",
        ]
        for bit in stable:
            lines.append(f"- {bit}")
        lines.append("")
    if sensitive:
        lines += [
            "Moves with market conditions (green% on winners flips with SPY close):",
            "",
        ]
        for bit in sensitive:
            lines.append(f"- {bit}")
        lines.append("")
    if holes:
        lines += ["Almost never printed on the names that ripped:", ""]
        for bit in holes:
            lines.append(f"- {bit}")
        lines.append("")
    lines += [
        "`mkt` is the session market gate (same tone on every name that day "
        "when the lattice saved it). `par` / `chd` / `co` / `set` / `flw` "
        "are permission, not a second vote of join. "
        "`gen` is the market-condition box. It is the morning essay, not the "
        "close, so a SPY-down session can still show gen🟢 on the names that "
        "ripped if the pre-open write was constructive. `yΔ` is yesterday's "
        "Change% from the last completed tape — a continuation tell that does "
        "not use today's close.",
        "",
        f"The ≥2% cut is the wider net ({g2.get('n_names') or 0} names vs "
        f"{g5.get('n_names') or 0} at ≥5%). Use it to see whether a box still "
        "prints when the move is ordinary, not only on the spike tail.",
        "",
    ]
    return lines


def walk(from_date: str = START, to_date: str | None = None,
         top_n: int = TOP_N, min_change: float = MIN_CHANGE,
         liquid: bool = True, *, force: bool = False,
         floors: list[float] | None = None,
         include_buys: bool = True,
         include_sells: bool = True,
         include_losers: bool = True,
         min_mcap_m: float | None = None) -> dict:
    global _WALK
    floor_list = [float(x) for x in (floors or [min_change])]
    floor_list = sorted({x for x in floor_list})
    primary = min(floor_list) if floor_list else float(min_change)
    key = (
        from_date, to_date, top_n, primary, tuple(floor_list),
        liquid, include_buys, include_sells, include_losers, min_mcap_m,
    )
    if _WALK is not None and not force and _WALK.get("_key") == list(key):
        return _WALK
    idx = tl.build_index()
    dates = [
        s["date"] for s in idx["sessions"]
        if s["date"] >= from_date and (not to_date or s["date"] <= to_date)
    ]
    raw_days = [
        day_walk(
            d, idx=idx, top_n=top_n, min_change=primary,
            liquid=liquid, include_buys=include_buys,
            include_sells=include_sells, include_losers=include_losers,
            min_mcap_m=min_mcap_m,
        )
        for d in dates
    ]
    # Slice higher floors from the primary (widest) walk.
    days_by_floor = {}
    losers_by_floor = {}
    for fl in floor_list:
        sliced = []
        loser_sliced = []
        loser_fl = -abs(fl)
        for day in raw_days:
            rows = [
                r for r in (day.get("rows") or [])
                if (_pct(r.get("change_pct")) or 0) >= fl
            ]
            rec = dict(day)
            rec["rows"] = rows
            rec["min_change"] = fl
            rec["n_gainers"] = len(rows)
            rec["n_overnight_buy"] = sum(1 for r in rows if r.get("overnight_buy"))
            rec["n_on_1d_buy"] = sum(1 for r in rows if r.get("on_1d_buy"))
            sliced.append(rec)
            loser_rows = [
                r for r in (day.get("losers") or [])
                if (_pct(r.get("change_pct")) or 0) <= loser_fl
            ]
            lrec = dict(day)
            lrec["losers"] = loser_rows
            lrec["loser_floor"] = loser_fl
            lrec["n_losers"] = len(loser_rows)
            lrec["n_overnight_sell"] = sum(
                1 for r in loser_rows if r.get("overnight_sell")
            )
            lrec["n_on_1d_sell"] = sum(
                1 for r in loser_rows if r.get("on_1d_sell")
            )
            loser_sliced.append(lrec)
        rows, skip = _attach_dates(sliced, "rows")
        fl_key = str(int(fl)) if fl == int(fl) else str(fl)
        days_by_floor[fl_key] = {
            "min_change": fl,
            "days": sliced,
            "summary": _tally(rows, skip_by_date=skip),
        }
        if include_losers:
            lrows, lskip = _attach_dates(loser_sliced, "losers")
            losers_by_floor[fl_key] = {
                "min_change": loser_fl,
                "days": loser_sliced,
                "summary": _tally(lrows, skip_by_date=lskip),
            }
    buy_rows, buy_skip = _attach_dates(raw_days, "buys")
    buy_block = {
        "days": raw_days,
        "summary": _tally(buy_rows, skip_by_date=buy_skip),
    }
    sell_rows, sell_skip = _attach_dates(raw_days, "sells")
    sell_block = {
        "days": raw_days,
        "summary": _tally(sell_rows, skip_by_date=sell_skip),
    }
    primary_key = str(int(primary)) if primary == int(primary) else str(primary)
    primary_days = days_by_floor.get(primary_key, {}).get("days") or raw_days
    up, down = _split_regime(primary_days, "rows")
    regime = {
        "spy_up": _tally(up),
        "spy_down": _tally(down),
    }
    mcap = MIN_MCAP_M if min_mcap_m is None else float(min_mcap_m)
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "from_date": from_date,
        "to_date": to_date,
        "top_n": top_n,
        "min_change": primary,
        "floors": floor_list,
        "liquid": bool(liquid),
        "include_buys": bool(include_buys),
        "include_sells": bool(include_sells),
        "include_losers": bool(include_losers),
        "min_mcap_m": mcap if liquid else 0.0,
        "min_avg_vol_k": MIN_AVG_VOL_K if liquid else 0.0,
        "legend": _legend(),
        "domain_legend": _domain_legend(),
        "days": primary_days,
        "summary": days_by_floor.get(primary_key, {}).get("summary") or _tally([]),
        "floors_detail": days_by_floor,
        "buys": buy_block,
        "sells": sell_block,
        "losers_detail": losers_by_floor,
        "regime": regime,
        "_key": list(key),
    }
    payload["insights"] = _insights(
        days_by_floor, buy_block, regime,
        sells=sell_block, losers=losers_by_floor,
    )
    _WALK = payload
    return payload


def _box_table(boxes: dict, cols=GAINER_BOX_COLS) -> list[str]:
    lines = [
        "| Box | Green | Yellow | Red | Missing | Era-skip | Green% | Printed% |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key, lab in cols:
        rec = (boxes or {}).get(key) or {}
        lines.append(
            f"| {lab} | {rec.get('good', 0)} | {rec.get('neutral', 0)} | "
            f"{rec.get('bad', 0)} | {rec.get('missing', 0)} | "
            f"{rec.get('era', 0)} | {rec.get('good_pct', 0):.1f}% | "
            f"{rec.get('printed_pct', 0):.1f}% |"
        )
    return lines


def _name_table(rows: list[dict], *, realized_label: str = "Δ") -> list[str]:
    lines = [
        f"| # | Ticker | {realized_label} | Sector | Hall pass | Source boxes | Domains | Cond | Overnight BUY | On today's 1d BUY | On today's 1d SELL |",
        "|---:|---|---:|---|---|---|---|---|---|---|---|",
    ]
    for i, row in enumerate(rows, 1):
        cond = row.get("condition") or {}
        cond_s = (
            f"{tl.BOX_ICON.get(cond.get('tone'), '⬛')} "
            f"{cond.get('good', 0)}/{cond.get('neutral', 0)}/{cond.get('bad', 0)}"
            if cond.get("n") else "—"
        )
        chg = row.get("change_pct")
        chg_s = f"{chg:+.2f}%" if chg is not None else "—"
        marks = row.get("marks") or {}
        mark_s = marks.get("icons") if isinstance(marks, dict) else (marks or "")
        hall = row.get("lane_label") or lane_label(row.get("lane"))
        if mark_s:
            hall = f"{hall} {mark_s}".strip()
        lines.append(
            f"| {i} | `{row['ticker']}` | {chg_s} | "
            f"{row.get('sector') or '—'} | {hall} | "
            f"{row.get('labeled') or _labeled(row.get('boxes'))} | "
            f"{row.get('labeled_domains') or _labeled_domains(row.get('domains'))} | "
            f"{cond_s} | "
            f"{'yes' if row.get('overnight_buy') else '—'} | "
            f"{'yes' if row.get('on_1d_buy') else '—'} | "
            f"{'yes' if row.get('on_1d_sell') else '—'} |"
        )
    return lines


def render_day_markdown(date: str, day: dict | None = None,
                        min_mcap_m: float | None = None) -> list[str]:
    day = day or day_walk(date, min_mcap_m=min_mcap_m)
    cov = day.get("coverage") or {}
    spy = day.get("spy_change")
    spy_s = f"{spy:+.2f}%" if spy is not None else "—"
    mcap = MIN_MCAP_M if min_mcap_m is None else float(min_mcap_m)
    lines = [
        f"### Top gainers — as-of 09:30 on {date}",
        "",
        "Realized winners (Finviz Change%"
        + (
            f" ≥ {day.get('min_change'):.0f}%"
            if (day.get("min_change") or 0) > 0 else ""
        )
        + (
            f", mcap ≥ ${mcap:.0f}M, adv ≥ 500k, not ETF"
            if day.get("liquid", True) else ", any name, not ETF"
        )
        +         "). Source boxes are the 09:30 ET packet. Domains are the lattice "
        "checklist (`mkt · par · chd · co · set · flw`) from that day's book "
        "when saved, else derived from those sources. Hall pass is the "
        "lattice lane (standard / group leader / catalyst / catalyst "
        "exception / probable / blocked). `yΔ` is yesterday's Change% "
        "from the last completed tape. Missing-as-of prints grey.",
        "",
        f"_Source boxes {_legend()}_",
        f"_Domains {_domain_legend()}_",
        "",
        f"Coverage **{cov.get('status') or '?'}** "
        f"({cov.get('n_change') or 0}/{cov.get('n') or 0} printed a Change%) "
        f"· SPY {spy_s}"
        + (
            f" · era-skip {', '.join(day.get('era_skip') or [])}"
            if day.get("era_skip") else ""
        ),
        "",
    ]
    rows = day.get("rows") or []
    if not rows:
        lines += ["_No liquid gainer tape for this session._", ""]
    else:
        lines += _name_table(rows)
        lines += [
            "",
            f"Overnight BUY caught {day.get('n_overnight_buy') or 0}/{len(rows)}; "
            f"today's 1d BUY list caught {day.get('n_on_1d_buy') or 0}/{len(rows)}.",
            "",
        ]
    losers = day.get("losers") or []
    if losers:
        floor = day.get("loser_floor")
        floor_s = f"{floor:.0f}%" if floor is not None else "-2%"
        lines += [
            f"#### Liquid losers ≤{floor_s} — as-of 09:30 on {date}",
            "",
            f"{len(losers)} names. Overnight SELL caught "
            f"{day.get('n_overnight_sell') or 0}/{len(losers)}; "
            f"today's 1d SELL list caught {day.get('n_on_1d_sell') or 0}/{len(losers)}.",
            "",
        ]
        lines += _name_table(losers)
        lines.append("")
    buys = day.get("buys") or []
    if buys:
        hit2 = sum(1 for r in buys if (_pct(r.get("change_pct")) or 0) >= 2)
        hit5 = sum(1 for r in buys if (_pct(r.get("change_pct")) or 0) >= 5)
        lines += [
            f"#### Today's 1d BUY — realized vs as-of boxes on {date}",
            "",
            f"{len(buys)} names the book printed. "
            f"{hit2}/{len(buys)} closed ≥2% · {hit5}/{len(buys)} closed ≥5%.",
            "",
        ]
        lines += _name_table(buys, realized_label="Realized Δ")
        lines.append("")
    sells = day.get("sells") or []
    if sells:
        hit2 = sum(1 for r in sells if (_pct(r.get("change_pct")) or 0) <= -2)
        hit5 = sum(1 for r in sells if (_pct(r.get("change_pct")) or 0) <= -5)
        lines += [
            f"#### Today's 1d SELL — realized vs as-of boxes on {date}",
            "",
            f"{len(sells)} names the book printed. "
            f"{hit2}/{len(sells)} closed ≤-2% · {hit5}/{len(sells)} closed ≤-5%.",
            "",
        ]
        lines += _name_table(sells, realized_label="Realized Δ")
        lines.append("")
    return lines


def render_markdown(payload: dict) -> str:
    floors = payload.get("floors_detail") or {}
    buys = payload.get("buys") or {}
    sells = payload.get("sells") or {}
    losers = payload.get("losers_detail") or {}
    regime = payload.get("regime") or {}
    g5 = floors.get("5") or {
        "summary": payload.get("summary") or {},
        "days": payload.get("days") or [],
        "min_change": payload.get("min_change") or 5,
    }
    lines = [
        "# Top gainers — as-of 09:30",
        "",
        f"_Generated {payload.get('generated_at')} · as-of 09:30 ET · "
        f"{payload.get('from_date')} → {payload.get('to_date') or 'latest'}"
        + (
            " · floors "
            + ", ".join(f"≥{x:g}%" for x in (payload.get("floors") or []))
            if payload.get("floors") else ""
        )
        + (
            " · all names"
            if not payload.get("top_n") else f" · top {payload.get('top_n')}"
        )
        + "_",
        "",
        "Each session's realized gainers and losers plus the 1d BUY and "
        "1d SELL sleeves. Source boxes are the 09:30 ET packet. Domains are "
        "`mkt · par · chd · co · set · flw` (saved lattice when the book "
        "has `d_*`, else derived: market←gen/session, parent←sect, "
        "child←heat, company←news/dig/jdg/cat, setup←AB, flow←vol). "
        "Hall pass is the lattice lane. `yΔ` is yesterday's Change% from "
        "the prior tape. Missing-as-of prints grey."
        + (
            f" Liquidity: mcap ≥ ${payload.get('min_mcap_m') or 0:.0f}M, "
            f"adv ≥ {payload.get('min_avg_vol_k') or 0:.0f}k, not ETF."
            if payload.get("liquid", True) else " Any printed name, not ETF."
        ),
        "",
        f"_Source boxes {payload.get('legend') or _legend()}_",
        f"_Domains {payload.get('domain_legend') or _domain_legend()}_",
        "",
    ]
    lines += payload.get("insights") or _insights(
        floors, buys, regime, sells=sells, losers=losers,
    )

    hit_blocks = [
        ("≥5% winners", g5, "names that closed ≥5%", "up"),
        ("≥2% winners", floors.get("2"), "names that closed ≥2%", "up"),
        ("≤-2% losers", losers.get("2"), "names that closed ≤-2%", "down"),
        ("≤-5% losers", losers.get("5"), "names that closed ≤-5%", "down"),
        ("today's 1d BUY", buys, "what the book actually printed long", "buy"),
        ("today's 1d SELL", sells, "what the book actually printed short", "sell"),
    ]
    for label, block, blurb, kind in hit_blocks:
        if not block:
            continue
        summ = block.get("summary") or {}
        lines += [
            f"## Hit rate — {label}",
            "",
            blurb[0].upper() + blurb[1:] + f": {summ.get('n_names') or 0} names. ",
        ]
        extra = []
        if summ.get("median_change") is not None:
            extra.append(f"median realized Δ {summ['median_change']:+.2f}%")
        if kind == "buy":
            extra.append(f"hit ≥2% {summ.get('hit_2_pct') or 0:.1f}%")
            extra.append(f"hit ≥5% {summ.get('hit_5_pct') or 0:.1f}%")
        elif kind == "sell":
            extra.append(f"hit ≤-2% {summ.get('hit_neg2_pct') or 0:.1f}%")
            extra.append(f"hit ≤-5% {summ.get('hit_neg5_pct') or 0:.1f}%")
        elif kind == "down":
            extra.append(f"overnight SELL {summ.get('overnight_sell_pct') or 0:.1f}%")
            extra.append(f"today's 1d SELL {summ.get('on_1d_sell_pct') or 0:.1f}%")
        else:
            extra.append(f"overnight BUY {summ.get('overnight_buy_pct') or 0:.1f}%")
            extra.append(f"today's 1d BUY {summ.get('on_1d_buy_pct') or 0:.1f}%")
        lines[-1] += " · ".join(extra) + "."
        lines += [
            "",
            "Source boxes:",
            "",
            *_box_table(summ.get("boxes") or {}),
            "",
            "Domains (`mkt · par · chd · co · set · flw`):",
            "",
            *_box_table(summ.get("domains") or {}, cols=DOMAIN_COLS),
            "",
        ]

    up, down = regime.get("spy_up") or {}, regime.get("spy_down") or {}
    if up or down:
        lines += [
            "## Regime — ≥ lowest floor, SPY up vs down",
            "",
            f"SPY-up days: {up.get('n_names') or 0} winners. "
            f"SPY-down days: {down.get('n_names') or 0} winners. "
            "`gen` is the morning essay; a down close does not rewrite it.",
            "",
            "| Box | Up green% | Down green% | Δ | Up printed% | Down printed% |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for key, lab in GAINER_BOX_COLS + DOMAIN_COLS:
            src = "domains" if key in {k for k, _ in DOMAIN_COLS} else "boxes"
            a = (up.get(src) or {}).get(key) or {}
            b = (down.get(src) or {}).get(key) or {}
            delta = (a.get("good_pct") or 0) - (b.get("good_pct") or 0)
            lines.append(
                f"| {lab} | {a.get('good_pct') or 0:.1f}% | "
                f"{b.get('good_pct') or 0:.1f}% | {delta:+.1f} | "
                f"{a.get('printed_pct') or 0:.1f}% | "
                f"{b.get('printed_pct') or 0:.1f}% |"
            )
        lines.append("")

    lines += ["## Per session", ""]
    show_days = (g5.get("days") or payload.get("days") or [])
    buy_by_date = {d["date"]: d for d in (buys.get("days") or [])}
    sell_by_date = {d["date"]: d for d in (sells.get("days") or [])}
    loser_days = ((losers.get("2") or losers.get("5") or {}).get("days") or [])
    loser_by_date = {d["date"]: d for d in loser_days}
    for day in show_days:
        merged = dict(day)
        src = buy_by_date.get(day["date"]) or {}
        if src.get("buys") and not merged.get("buys"):
            merged["buys"] = src["buys"]
        ssrc = sell_by_date.get(day["date"]) or {}
        if ssrc.get("sells") and not merged.get("sells"):
            merged["sells"] = ssrc["sells"]
        lsrc = loser_by_date.get(day["date"]) or {}
        if lsrc.get("losers") and not merged.get("losers"):
            merged["losers"] = lsrc["losers"]
            merged["n_losers"] = lsrc.get("n_losers")
            merged["n_overnight_sell"] = lsrc.get("n_overnight_sell")
            merged["n_on_1d_sell"] = lsrc.get("n_on_1d_sell")
            merged["loser_floor"] = lsrc.get("loser_floor")
        lines += render_day_markdown(day["date"], day=merged,
                                     min_mcap_m=payload.get("min_mcap_m"))
    lines.append("")
    return "\n".join(lines)


def write_scoreboard(payload: dict | None = None) -> tuple[Path, Path]:
    payload = payload or walk(force=True)
    slim = {k: v for k, v in payload.items() if k != "_key"}
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(render_markdown(payload), encoding="utf-8")
    OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    return OUT_MD, OUT_JSON


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="from_date", default=START)
    ap.add_argument("--to", dest="to_date", default="")
    ap.add_argument("--top", type=int, default=TOP_N,
                    help="Cap per session (0 or --all = every name over the floor)")
    ap.add_argument("--all", action="store_true",
                    help="Do not cap — every name at/above the floor")
    ap.add_argument("--min-change", type=float, default=MIN_CHANGE,
                    help="Single floor when --floors is omitted (default 5)")
    ap.add_argument("--floors", default="",
                    help="Comma floors, e.g. 2,5 (walks the lowest, slices the rest)")
    ap.add_argument("--buys", action="store_true", default=True,
                    help="Also color today's 1d BUY sleeve (default on)")
    ap.add_argument("--no-buys", dest="buys", action="store_false")
    ap.add_argument("--sells", action="store_true", default=True,
                    help="Also color today's 1d SELL sleeve (default on)")
    ap.add_argument("--no-sells", dest="sells", action="store_false")
    ap.add_argument("--losers", action="store_true", default=True,
                    help="Also color liquid session losers (default on)")
    ap.add_argument("--no-losers", dest="losers", action="store_false")
    ap.add_argument("--no-liquid", dest="liquid", action="store_false",
                    help="Include names below the mcap / adv floor")
    ap.add_argument("--min-mcap", type=float, default=MIN_MCAP_M,
                    help="Market-cap floor in $ millions (default 100)")
    ap.add_argument("--write", action="store_true")
    return ap


def main() -> None:
    args = build_parser().parse_args()
    top_n = 0 if args.all else args.top
    floors = None
    if args.floors.strip():
        floors = [float(x) for x in args.floors.split(",") if x.strip()]
    payload = walk(
        from_date=args.from_date,
        to_date=args.to_date or None,
        top_n=top_n,
        min_change=args.min_change,
        floors=floors,
        liquid=args.liquid,
        include_buys=args.buys,
        include_sells=args.sells,
        include_losers=args.losers,
        min_mcap_m=args.min_mcap,
        force=True,
    )
    text = render_markdown(payload)
    print(text)
    if args.write:
        md, js = write_scoreboard(payload)
        print(f"[gainer-asof] wrote {md}")
        print(f"[gainer-asof] wrote {js}")


if __name__ == "__main__":
    main()
