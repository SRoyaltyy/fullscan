#!/usr/bin/env python3
"""Research audit: Finviz after-close extremes vs T+1 morning picks.

Screen on session T (after-close Finviz export):
  RelVol >= 3, |Change| >= 8%, Perf Week >= +40%

Compare those names to morning T+1 artifacts already in this repo:
  stock-book BUY / CSV universe, green pile, flatten_robust would-buy,
  factor-mine panel + a few scoreboard recipes.

Thin after-fee counterfactual is equal-weight T+1 Change-from-Open
minus 15 bp (same Futubull approximation as the open-gates boards).
Does not change live flatten_robust or rubric weights.

CLI: python3 excel_bot/research/extreme_vol_price_audit.py
"""
from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXPORT = ROOT / "data" / "exports"
BOOK_DIR = ROOT / "data" / "stock_book"
DAILY = ROOT / "01_daily"
PANEL_PATH = ROOT / "data" / "factor_mine" / "panel.json"
FLATTEN_JSON = ROOT / "03_scoreboard" / "flatten_lookback_action.json"
OUT_DIR = Path(__file__).resolve().parent
OUT_NAMES = OUT_DIR / "extreme_vol_price_names.csv"
OUT_SESS = OUT_DIR / "extreme_vol_price_sessions.csv"
OUT_MD = OUT_DIR / "EXTREME_VOL_PRICE.md"

FROM_DATE = "2026-08-13"
TO_DATE = "2026-09-11"
RELVOL_MIN = 3.0
ABS_DAY_MIN = 8.0
WEEK_MIN = 40.0
RELVOL_DEAD = 0.7
MIN_BUY_MCAP_M = 400.0
MIN_LIQ_MCAP_M = 100.0
MIN_ADV_K = 500.0
FEE_BP = 0.15  # percentage points, Futubull round-trip stand-in
MAX_PER_SECTOR = 4
MAX_PER_INDUSTRY = 3
MAX_LARGE_MEGA = 4
TOO_EXT_RET5 = 18.0
TOO_EXT_RVOL = 2.8
PROBABLE_RET5 = 10.0
COIL_RVOL = 2.2

# Closed sessions in the dashboard / factor-mine window.
SESSIONS = [
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11",
]


def _tick(v) -> str:
    return str(v or "").strip().upper()


def _pct(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s or s in ("-", "nan", "None", ""):
        return None
    s = s.replace("%", "")
    try:
        return float(s)
    except ValueError:
        return None


def _num(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s or s in ("-", "nan", "None", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _boolish(v) -> bool | None:
    if v is None or str(v).strip() == "":
        return None
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def next_session(date: str) -> str | None:
    if date not in SESSIONS:
        return None
    i = SESSIONS.index(date)
    return SESSIONS[i + 1] if i + 1 < len(SESSIONS) else None


def load_finviz(date: str) -> list[dict]:
    path = EXPORT / f"finviz_{date}.csv"
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f))


def finviz_index(date: str) -> dict[str, dict]:
    out = {}
    for rec in load_finviz(date):
        t = _tick(rec.get("Ticker"))
        if t:
            out[t] = rec
    return out


def extremes_from(date: str) -> list[dict]:
    rows = []
    for rec in load_finviz(date):
        t = _tick(rec.get("Ticker"))
        if not t:
            continue
        rel = _num(rec.get("Relative Volume"))
        chg = _pct(rec.get("Change"))
        week = _pct(rec.get("Performance (Week)"))
        if rel is None or chg is None or week is None:
            continue
        if rel < RELVOL_MIN or abs(chg) < ABS_DAY_MIN or week < WEEK_MIN:
            continue
        industry = str(rec.get("Industry") or "")
        rows.append({
            "t_date": date,
            "ticker": t,
            "company": rec.get("Company") or "",
            "sector": rec.get("Sector") or "",
            "industry": industry,
            "etf": industry == "Exchange Traded Fund",
            "t_relvol": rel,
            "t_change": chg,
            "t_week": week,
            "t_mcap_m": _num(rec.get("Market Cap")),
            "t_adv_k": _num(rec.get("Average Volume")),
            "t_price": _num(rec.get("Price")),
        })
    rows.sort(key=lambda r: (-(r["t_mcap_m"] or 0), r["ticker"]))
    return rows


def load_book_json(date: str) -> dict | None:
    path = BOOK_DIR / f"{date}_stock_book.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_book_csv(date: str) -> dict[str, dict]:
    path = BOOK_DIR / f"{date}_stock_book.csv"
    if not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        return {_tick(r.get("Ticker")): r for r in csv.DictReader(f) if _tick(r.get("Ticker"))}


def load_green(date: str) -> dict:
    path = BOOK_DIR / f"{date}_green.json"
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def book_buys(js: dict | None, horizon: str) -> list[str]:
    if not js:
        return []
    rows = ((js.get("books") or {}).get(horizon) or {}).get("buy") or []
    return [_tick(r.get("ticker")) for r in rows if _tick(r.get("ticker"))]


def parse_flatten_card(date: str) -> dict:
    path = DAILY / f"{date}_flatten_card.md"
    out = {
        "date": date,
        "score": None,
        "hard_red": False,
        "flatten_ok": False,
        "tickers": [],
        "why": "",
        "source": "missing",
    }
    if not path.is_file():
        return out
    text = path.read_text(encoding="utf-8", errors="replace")
    out["source"] = "card"
    m = re.search(r"S=([+-]?\d+(?:\.\d+)?)", text)
    if m:
        out["score"] = float(m.group(1))
    out["hard_red"] = "HARD-RED" in text or "hard-red" in text
    out["flatten_ok"] = "flatten .io" in text.lower() and not out["hard_red"]
    # Would-have-bought table tickers.
    names = []
    grab = False
    for line in text.splitlines():
        if line.startswith("## Would have bought"):
            grab = True
            continue
        if grab and line.startswith("## "):
            break
        if grab and line.startswith("|") and "Ticker" not in line and not line.startswith("|---") and "—" not in line:
            cols = [c.strip() for c in line.strip("|").split("|")]
            # clock | ticker | sleeve | shares | px | $ | why
            if len(cols) >= 2 and cols[0].endswith("ET"):
                t = _tick(cols[1])
                if t and t not in ("TICKER", "CLOCK", "*"):
                    names.append(t)
    out["tickers"] = list(dict.fromkeys(names))
    head = next((ln for ln in text.splitlines() if ln.startswith("**S=")), "")
    out["why"] = head.strip("* ")
    return out


def load_flatten_days() -> dict[str, dict]:
    by = {}
    if FLATTEN_JSON.is_file():
        raw = json.loads(FLATTEN_JSON.read_text(encoding="utf-8"))
        for day in raw.get("daily") or []:
            d = day.get("date")
            if d:
                by[d] = {
                    "date": d,
                    "score": day.get("score"),
                    "hard_red": bool(day.get("hard_red")),
                    "flatten_ok": bool(day.get("flatten_ok")),
                    "tickers": [_tick(t) for t in (day.get("tickers") or [])],
                    "why": day.get("why") or "",
                    "source": "flatten_lookback_action",
                }
    for d in SESSIONS:
        if d not in by:
            card = parse_flatten_card(d)
            if card["source"] != "missing":
                by[d] = card
    return by


def load_panel_by_date() -> dict[str, dict[str, dict]]:
    if not PANEL_PATH.is_file():
        return {}
    raw = json.loads(PANEL_PATH.read_text(encoding="utf-8"))
    out: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in raw.get("rows") or []:
        d = row.get("date")
        t = _tick(row.get("ticker"))
        if d and t:
            out[d][t] = row
    return out


def t1_h(t1_fv: dict | None) -> float | None:
    if not t1_fv:
        return None
    h = _pct(t1_fv.get("Change from Open"))
    if h is not None:
        return h
    o = _num(t1_fv.get("Open"))
    c = _num(t1_fv.get("Price"))
    if o and c:
        return 100.0 * (c / o - 1.0)
    return None


def mean(xs: list[float | None]) -> float | None:
    vals = [x for x in xs if x is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def after_fee(h: float | None) -> float | None:
    if h is None:
        return None
    return h - FEE_BP


def size_bucket(mcap: float | None, size: str) -> str:
    s = (size or "").lower()
    if s:
        return s
    if mcap is None:
        return ""
    if mcap >= 200_000:
        return "mega"
    if mcap >= 10_000:
        return "large"
    if mcap >= 2_000:
        return "mid"
    if mcap >= 300:
        return "small"
    return "micro"


def first_true(pairs: list[tuple[str, bool]]) -> str:
    for name, ok in pairs:
        if ok:
            return name
    return "none"


def recipe_keep(row: dict, universe: str, require: dict | None = None,
                forbid: dict | None = None) -> bool:
    srcs = set(row.get("sources") or [])
    if universe != "union" and universe not in srcs:
        return False
    req = require or {}
    forb = forbid or {}
    if req.get("live_entry") and not row.get("flatten_ok"):
        return False
    boxes = row.get("boxes") or {}
    if req.get("vol") == "good" and boxes.get("vol") != "good":
        return False
    if req.get("last_green") and not row.get("last_green"):
        return False
    if forb.get("alarm") and row.get("alarm"):
        return False
    ret5 = row.get("ohlc_ret_5")
    rvol = row.get("ohlc_rvol")
    if "ret_5_max" in req:
        if ret5 is None or float(ret5) > float(req["ret_5_max"]):
            return False
    if "rvol_max" in req:
        if rvol is None or float(rvol) > float(req["rvol_max"]):
            return False
    if "rvol_min" in req:
        if rvol is None or float(rvol) < float(req["rvol_min"]):
            return False
    return True


def pick_recipe(day_rows: list[dict], universe: str, top_n: int = 8,
                require: dict | None = None, forbid: dict | None = None,
                rank: str | None = None) -> list[str]:
    kept = [r for r in day_rows if recipe_keep(r, universe, require, forbid)]
    if rank == "hot_score":
        kept.sort(key=lambda r: (-(r.get("ohlc_hot_score") or 0), r["ticker"]))
    else:
        kept.sort(key=lambda r: (r.get("src_rank") if r.get("src_rank") is not None else 99, r["ticker"]))
    return [_tick(r["ticker"]) for r in kept[:top_n]]


def simulate_cap_reason(book_rows: dict[str, dict], ticker: str,
                        score_col: str = "score_3d") -> str | None:
    """Would the BUY walk seat this name before caps fill?"""
    if ticker not in book_rows:
        return "not_in_morning_book"
    ranked = []
    for t, rec in book_rows.items():
        mcap = _num(rec.get("market_cap_m"))
        size = str(rec.get("size") or "").lower()
        if size == "micro" or (mcap is not None and mcap < MIN_BUY_MCAP_M):
            continue
        rel = _num(rec.get("relvol"))
        if rel is not None and 0 < rel < RELVOL_DEAD:
            continue
        elig = _boolish(rec.get("bull_eligible"))
        if elig is False:
            continue
        sc = _num(rec.get(score_col))
        if sc is None:
            sc = _num(rec.get("green_rank"))
        ranked.append((sc if sc is not None else -999, t, rec))
    ranked.sort(key=lambda x: (-x[0], x[1]))
    sec_n: dict[str, int] = {}
    ind_n: dict[str, int] = {}
    large_n = 0
    seated = []
    for _sc, t, rec in ranked:
        sec = str(rec.get("sector") or "")
        ind = str(rec.get("industry") or "")
        size = size_bucket(_num(rec.get("market_cap_m")), rec.get("size") or "")
        mcap = _num(rec.get("market_cap_m"))
        is_large = size in ("large", "mega") or (mcap is not None and mcap > 10_000)
        if t == ticker:
            if is_large and large_n >= MAX_LARGE_MEGA:
                return "large_mega_cap"
            if sec and sec_n.get(sec, 0) >= MAX_PER_SECTOR:
                return "sector_cap"
            if ind and ind not in ("", "nan", "None") and ind_n.get(ind, 0) >= MAX_PER_INDUSTRY:
                return "industry_cap"
            if len(seated) >= 25:
                return "rank_not_top25"
            return None
        if is_large and large_n >= MAX_LARGE_MEGA:
            continue
        if sec and sec_n.get(sec, 0) >= MAX_PER_SECTOR:
            continue
        if ind and ind not in ("", "nan", "None") and ind_n.get(ind, 0) >= MAX_PER_INDUSTRY:
            continue
        seated.append(t)
        if is_large:
            large_n += 1
        if sec:
            sec_n[sec] = sec_n.get(sec, 0) + 1
        if ind and ind not in ("", "nan", "None"):
            ind_n[ind] = ind_n.get(ind, 0) + 1
        if len(seated) >= 25:
            # still continue until we see ticker or exhaust
            if t == ticker:
                return None
    if ticker not in {t for _s, t, _r in ranked}:
        return "buy_walk_veto"
    return "rank_not_top25"


def audit() -> tuple[list[dict], list[dict], dict]:
    flatten_days = load_flatten_days()
    panel = load_panel_by_date()
    name_rows: list[dict] = []
    sess_rows: list[dict] = []

    t_dates = [d for d in SESSIONS if d < TO_DATE]
    for t_date in t_dates:
        t1 = next_session(t_date)
        extremes = extremes_from(t_date)
        book_js = load_book_json(t1) if t1 else None
        book_csv = load_book_csv(t1) if t1 else {}
        green = load_green(t1) if t1 else {}
        t1_fv = finviz_index(t1) if t1 else {}
        flat = flatten_days.get(t1 or "", {})
        flatten_set = set(flat.get("tickers") or [])
        buy_1d = set(book_buys(book_js, "1d"))
        buy_3d = set(book_buys(book_js, "3d"))
        pile_tickers = {_tick(x) for x in (green.get("tickers") or [])}
        pile_used = bool(green.get("used") or ((book_js or {}).get("meta") or {}).get("pile_used"))
        hard_red = bool(flat.get("hard_red"))
        flatten_ok = bool(flat.get("flatten_ok"))
        panel_day = panel.get(t1 or "", {})
        day_panel_rows = list(panel_day.values())
        for r in day_panel_rows:
            r["flatten_ok"] = flatten_ok
        rec_union = pick_recipe(day_panel_rows, "union")
        rec_yday = pick_recipe(day_panel_rows, "yday_gainer")
        rec_flat = pick_recipe(day_panel_rows, "flatten")
        rec_live = pick_recipe(day_panel_rows, "flatten",
                               require={"live_entry": True})
        rec_hot = pick_recipe(day_panel_rows, "ohlc_hot")
        rec_prob = pick_recipe(day_panel_rows, "probable")
        rec_coil = pick_recipe(
            day_panel_rows, "union",
            require={"ret_5_min": 0.0, "ret_5_max": 10.0,
                     "rvol_min": 0.7, "rvol_max": COIL_RVOL},
            forbid={"alarm": True},
        )
        rec_volg = pick_recipe(day_panel_rows, "union",
                               require={"vol": "good", "last_green": True},
                               forbid={"alarm": True})

        pick_h = []
        for t in buy_3d or buy_1d:
            pick_h.append(t1_h(t1_fv.get(t)))
        ext_h = []
        liq_ext_h = []
        chosen_n = 0
        liq_n = 0
        book_elig_n = 0

        for ext in extremes:
            t = ext["ticker"]
            rec = book_csv.get(t) or {}
            fv1 = t1_fv.get(t)
            prow = panel_day.get(t) or {}
            sources = list(prow.get("sources") or [])
            h = t1_h(fv1)
            mcap = _num(rec.get("market_cap_m"))
            if mcap is None:
                mcap = ext["t_mcap_m"]
            adv = _num(rec.get("avg_vol_k"))
            if adv is None:
                adv = ext["t_adv_k"]
            rel_morn = _num(rec.get("relvol"))
            if rel_morn is None and fv1:
                rel_morn = _num(fv1.get("Relative Volume"))
            size = size_bucket(mcap, rec.get("size") or "")
            green_flag = _boolish(rec.get("green"))
            bull_elig = _boolish(rec.get("bull_eligible"))
            lane = rec.get("decision_lane") or rec.get("lane") or ""
            blockers_txt = rec.get("decision_blockers") or ""
            s_join = _num(rec.get("s_join"))
            s_ab = _num(rec.get("s_ab"))
            s_peer = _num(rec.get("s_peer"))
            s_gen = _num(rec.get("s_general"))
            s_sec = _num(rec.get("s_sector"))
            s_news = _num(rec.get("s_news"))
            ret5 = prow.get("ohlc_ret_5")
            ohlc_rvol = prow.get("ohlc_rvol")
            etf = bool(ext["etf"])
            in_book = bool(rec)
            liquid_gainer = (
                (mcap is not None and mcap >= MIN_LIQ_MCAP_M)
                and (adv is not None and adv >= MIN_ADV_K)
                and not etf
            )
            liquid_buy = (
                (mcap is not None and mcap >= MIN_BUY_MCAP_M)
                and size != "micro"
                and not etf
            )
            dead_rel = rel_morn is not None and 0 < rel_morn < RELVOL_DEAD
            cores_ok = all(
                v is not None and v >= 0.05
                for v in (s_join, s_ab, s_peer, s_gen)
            ) if rec and all(c in rec for c in ("s_join", "s_ab", "s_peer", "s_general")) else False
            sector_ok = s_sec is None or s_sec > -0.05
            news_ok = s_news is None or s_news > -0.05
            green_eligible = bool(
                in_book and cores_ok and sector_ok and news_ok
                and not dead_rel and liquid_buy
            )
            lattice_blocked = bull_elig is False or str(lane).lower() == "blocked"
            too_ext = (
                (ret5 is not None and float(ret5) > TOO_EXT_RET5)
                or (ohlc_rvol is not None and float(ohlc_rvol) > TOO_EXT_RVOL)
            )
            in_1d = t in buy_1d
            in_3d = t in buy_3d
            in_flat = t in flatten_set
            in_pile = t in pile_tickers
            in_panel = t in panel_day
            in_union = "union" in sources or bool(sources)
            chosen = in_1d or in_3d or in_flat
            cap_reason = None
            if in_book and liquid_buy and not dead_rel and not lattice_blocked and not chosen:
                cap_reason = simulate_cap_reason(book_csv, t)

            reasons = []
            if t1 is None:
                reasons.append("no_t1_in_window")
            if etf:
                reasons.append("etf")
            if not in_book:
                reasons.append("not_in_morning_book")
            if in_book and not liquid_buy:
                reasons.append("illiquid_mcap_lt_400_or_micro")
            if dead_rel:
                reasons.append("dead_relvol_0_0.7")
            if lattice_blocked:
                reasons.append("lattice_blocked")
            if in_book and green_flag is False:
                reasons.append("not_green")
            if in_book and not pile_used:
                reasons.append("green_pile_unused")
            if hard_red:
                reasons.append("HARD_RED_sit")
            if not flatten_ok:
                reasons.append("flatten_gate_off")
            if too_ext:
                reasons.append("too_extended_ret5_or_rvol")
            if ret5 is not None and float(ret5) > PROBABLE_RET5:
                reasons.append("probable_exploded_ret5")
            if not in_panel:
                reasons.append("not_in_factor_mine_panel")
            if cap_reason:
                reasons.append(cap_reason)
            if chosen:
                reasons.append("chosen")

            first = first_true([
                ("chosen", chosen),
                ("no_t1_in_window", t1 is None),
                ("etf", etf),
                ("not_in_morning_book", not in_book),
                ("illiquid_mcap_lt_400_or_micro", in_book and not liquid_buy),
                ("dead_relvol_0_0.7", dead_rel),
                ("lattice_blocked", lattice_blocked),
                ("not_green", in_book and green_flag is False and pile_used),
                ("too_extended_ret5_or_rvol", too_ext),
                (cap_reason or "rank_not_top25", bool(cap_reason)),
                ("HARD_RED_sit", hard_red and in_flat),
                ("flatten_gate_off", (not flatten_ok) and in_flat),
                ("not_selected", not chosen),
            ])

            row = {
                "t_date": t_date,
                "t1_date": t1 or "",
                "ticker": t,
                "company": ext["company"],
                "sector": ext["sector"] or rec.get("sector") or "",
                "industry": ext["industry"] or rec.get("industry") or "",
                "etf": etf,
                "t_relvol": ext["t_relvol"],
                "t_change": ext["t_change"],
                "t_week": ext["t_week"],
                "t_mcap_m": ext["t_mcap_m"],
                "t_adv_k": ext["t_adv_k"],
                "t1_mcap_m": mcap,
                "t1_adv_k": adv,
                "t1_relvol": rel_morn,
                "size": size,
                "liquid_gainer": liquid_gainer,
                "liquid_buy": liquid_buy,
                "in_morning_book": in_book,
                "green": green_flag,
                "green_pile_used": pile_used,
                "green_pile_eligible": green_eligible,
                "in_green_pile": in_pile,
                "bull_eligible": bull_elig,
                "decision_lane": lane,
                "decision_blockers": blockers_txt,
                "dead_relvol": dead_rel,
                "s_join": s_join,
                "s_ab": s_ab,
                "s_peer": s_peer,
                "s_general": s_gen,
                "s_sector": s_sec,
                "s_news": s_news,
                "ohlc_ret_5": ret5,
                "ohlc_rvol": ohlc_rvol,
                "too_extended": too_ext,
                "hard_red": hard_red,
                "flatten_ok": flatten_ok,
                "flatten_s": flat.get("score"),
                "in_1d_buy": in_1d,
                "in_3d_buy": in_3d,
                "in_flatten_would_buy": in_flat,
                "in_factor_panel": in_panel,
                "fm_sources": "|".join(sources),
                "in_yday_gainer": "yday_gainer" in sources,
                "in_ohlc_hot": "ohlc_hot" in sources,
                "in_probable": "probable" in sources,
                "in_fm_flatten": "flatten" in sources,
                "recipe_union_h1": t in rec_union,
                "recipe_yday_gainer_h1": t in rec_yday,
                "recipe_flatten_h1": t in rec_flat,
                "recipe_flatten_live_h1": t in rec_live,
                "recipe_ohlc_hot_h1": t in rec_hot,
                "recipe_probable_h1": t in rec_prob,
                "recipe_coil_off_h1": t in rec_coil,
                "recipe_vol_green_h1": t in rec_volg,
                "chosen_any_live": chosen,
                "cap_reason": cap_reason or "",
                "first_blocker": first,
                "all_blockers": "|".join(reasons),
                "t1_h": None if h is None else round(h, 3),
                "t1_h_after_fee": None if h is None else round(after_fee(h), 3),
            }
            name_rows.append(row)
            ext_h.append(h)
            if liquid_gainer:
                liq_n += 1
                liq_ext_h.append(h)
            if liquid_buy and in_book and not dead_rel and not lattice_blocked:
                book_elig_n += 1
            if chosen:
                chosen_n += 1

        cur_h = [x for x in pick_h if x is not None]
        force_tickers = list(dict.fromkeys(list(buy_3d or buy_1d) + [e["ticker"] for e in extremes if not e["etf"]]))
        force_h = [t1_h(t1_fv.get(t)) for t in force_tickers]
        sess_rows.append({
            "t_date": t_date,
            "t1_date": t1 or "",
            "n_extremes": len(extremes),
            "n_etf": sum(1 for e in extremes if e["etf"]),
            "n_liquid_gainer": liq_n,
            "n_book_present": sum(1 for r in name_rows if r["t_date"] == t_date and r["in_morning_book"]),
            "n_book_buy_eligible": book_elig_n,
            "n_green_eligible": sum(1 for r in name_rows if r["t_date"] == t_date and r["green_pile_eligible"]),
            "n_in_panel": sum(1 for r in name_rows if r["t_date"] == t_date and r["in_factor_panel"]),
            "n_yday_gainer": sum(1 for r in name_rows if r["t_date"] == t_date and r["in_yday_gainer"]),
            "n_chosen_1d": sum(1 for r in name_rows if r["t_date"] == t_date and r["in_1d_buy"]),
            "n_chosen_3d": sum(1 for r in name_rows if r["t_date"] == t_date and r["in_3d_buy"]),
            "n_chosen_flatten": sum(1 for r in name_rows if r["t_date"] == t_date and r["in_flatten_would_buy"]),
            "n_chosen_any": chosen_n,
            "green_pile_used": pile_used,
            "hard_red": hard_red,
            "flatten_ok": flatten_ok,
            "flatten_s": flat.get("score"),
            "n_1d_buy": len(buy_1d),
            "n_3d_buy": len(buy_3d),
            "n_flatten_would": len(flatten_set),
            "extreme_h": None if mean(ext_h) is None else round(mean(ext_h), 3),
            "extreme_h_after_fee": None if mean(ext_h) is None else round(mean(ext_h) - FEE_BP, 3),
            "liq_extreme_h_after_fee": None if mean(liq_ext_h) is None else round(mean(liq_ext_h) - FEE_BP, 3),
            "current_3d_h_after_fee": None if not cur_h else round(mean(cur_h) - FEE_BP, 3),
            "force_include_h_after_fee": None if mean(force_h) is None else round(mean(force_h) - FEE_BP, 3),
            "current_n_graded": len(cur_h),
            "force_n_graded": sum(1 for x in force_h if x is not None),
        })

    summary = summarize(name_rows, sess_rows, flatten_days)
    return name_rows, sess_rows, summary


def summarize(names: list[dict], sessions: list[dict], flatten_days: dict) -> dict:
    def hit(pred) -> int:
        return sum(1 for r in names if pred(r))

    n = len(names)
    chosen = [r for r in names if r["chosen_any_live"]]
    liq = [r for r in names if r["liquid_gainer"]]
    buy_elig = [r for r in names if r["liquid_buy"] and r["in_morning_book"]
                and not r["dead_relvol"] and r["bull_eligible"] is not False]
    blockers = Counter(r["first_blocker"] for r in names)
    all_b = Counter()
    for r in names:
        for b in (r["all_blockers"] or "").split("|"):
            if b:
                all_b[b] += 1

    def h_stats(rows, key="t1_h_after_fee"):
        xs = [r[key] for r in rows if r.get(key) is not None]
        if not xs:
            return {"n": 0, "mean": None, "hit": None}
        return {
            "n": len(xs),
            "mean": round(sum(xs) / len(xs), 3),
            "hit": round(100.0 * sum(1 for x in xs if x > 0) / len(xs), 1),
        }

    cur_xs, force_xs, ext_xs, liq_xs = [], [], [], []
    for s in sessions:
        if s.get("current_3d_h_after_fee") is not None:
            cur_xs.append(s["current_3d_h_after_fee"])
        if s.get("force_include_h_after_fee") is not None:
            force_xs.append(s["force_include_h_after_fee"])
        if s.get("extreme_h_after_fee") is not None:
            ext_xs.append(s["extreme_h_after_fee"])
        if s.get("liq_extreme_h_after_fee") is not None:
            liq_xs.append(s["liq_extreme_h_after_fee"])

    def sess_mean(xs):
        return None if not xs else round(sum(xs) / len(xs), 3)

    notable = [r for r in names if (r["t_mcap_m"] or 0) >= 1000 or r["chosen_any_live"]]
    return {
        "n_name_days": n,
        "n_t_sessions": len(sessions),
        "n_unique_tickers": len({r["ticker"] for r in names}),
        "n_etf": hit(lambda r: r["etf"]),
        "n_common": hit(lambda r: not r["etf"]),
        "n_liquid_gainer": len(liq),
        "n_in_book": hit(lambda r: r["in_morning_book"]),
        "n_liquid_buy": hit(lambda r: r["liquid_buy"]),
        "n_book_buy_eligible": len(buy_elig),
        "n_green_eligible": hit(lambda r: r["green_pile_eligible"]),
        "n_in_pile": hit(lambda r: r["in_green_pile"]),
        "n_dead_relvol": hit(lambda r: r["dead_relvol"]),
        "n_lattice_blocked": hit(lambda r: r["bull_eligible"] is False),
        "n_too_extended": hit(lambda r: r["too_extended"]),
        "n_in_panel": hit(lambda r: r["in_factor_panel"]),
        "n_yday_gainer": hit(lambda r: r["in_yday_gainer"]),
        "n_ohlc_hot": hit(lambda r: r["in_ohlc_hot"]),
        "n_probable": hit(lambda r: r["in_probable"]),
        "n_fm_flatten": hit(lambda r: r["in_fm_flatten"]),
        "n_1d": hit(lambda r: r["in_1d_buy"]),
        "n_3d": hit(lambda r: r["in_3d_buy"]),
        "n_flatten": hit(lambda r: r["in_flatten_would_buy"]),
        "n_chosen": len(chosen),
        "n_union_h1": hit(lambda r: r["recipe_union_h1"]),
        "n_yday_h1": hit(lambda r: r["recipe_yday_gainer_h1"]),
        "n_flat_h1": hit(lambda r: r["recipe_flatten_h1"]),
        "n_live_h1": hit(lambda r: r["recipe_flatten_live_h1"]),
        "n_hot_h1": hit(lambda r: r["recipe_ohlc_hot_h1"]),
        "n_prob_h1": hit(lambda r: r["recipe_probable_h1"]),
        "n_coil_h1": hit(lambda r: r["recipe_coil_off_h1"]),
        "n_volg_h1": hit(lambda r: r["recipe_vol_green_h1"]),
        "n_hard_red_mornings": sum(1 for s in sessions if s["hard_red"]),
        "n_flatten_ok_mornings": sum(1 for s in sessions if s["flatten_ok"]),
        "n_pile_used_mornings": sum(1 for s in sessions if s["green_pile_used"]),
        "first_blockers": blockers.most_common(),
        "all_blockers": all_b.most_common(),
        "h_all": h_stats(names),
        "h_liq": h_stats(liq),
        "h_chosen": h_stats(chosen),
        "h_book_elig": h_stats(buy_elig),
        "daymean_current": sess_mean(cur_xs),
        "daymean_extremes": sess_mean(ext_xs),
        "daymean_liq_extremes": sess_mean(liq_xs),
        "daymean_force": sess_mean(force_xs),
        "n_days_current": len(cur_xs),
        "n_days_force": len(force_xs),
        "notable": notable,
        "chosen_rows": chosen,
        "liq_rows": liq,
        "flatten_days_covered": sorted(flatten_days),
    }


def _fmt(x, digits=2):
    if x is None:
        return "—"
    if isinstance(x, float):
        return f"{x:+.{digits}f}"
    return str(x)


def _fmt_m(x):
    if x is None:
        return "—"
    return f"{float(x):,.0f}"


def _pct_share(n, d):
    if not d:
        return "—"
    return f"{n}/{d} ({100.0 * n / d:.1f}%)"


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    cols = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            out = {}
            for k, v in r.items():
                if isinstance(v, bool):
                    out[k] = "true" if v else "false"
                elif v is None:
                    out[k] = ""
                else:
                    out[k] = v
            w.writerow(out)


def write_md(summary: dict, sessions: list[dict], names: list[dict]) -> None:
    s = summary
    lines = [
        "# Extreme RelVol / price names vs morning picks",
        "",
        "_Research only · live `flatten_robust` untouched · no rubric-weight change._",
        "",
        f"Window: after-close **{FROM_DATE} → {TO_DATE}**. "
        "An extreme is a Finviz print on session **T** with "
        f"RelVol ≥ {RELVOL_MIN:g}, |day move| ≥ {ABS_DAY_MIN:g}%, "
        f"and Perf Week ≥ +{WEEK_MIN:g}%. Picks are the **T+1 09:30** book / "
        "flatten wish-list / factor-mine panel already sitting in this repo.",
        "",
        "Finviz exports live in `data/exports/`. Theme-radar has no extra "
        "after-close tape for this cut. Weekend / Labor Day copies "
        "(08-29, 08-30, 09-05..07) are ignored; only the 21 closed sessions "
        "in `factor_mine` panel are T dates, and T=09-11 has no T+1 in-window.",
        "",
        "## Plain English",
        "",
        "The live dashboards **almost never select** these names, and most "
        "days they are **not allowed onto the BUY walk** even when they print "
        "in the morning book.",
        "",
        f"- **{s['n_name_days']} extreme name-days** across "
        f"{s['n_t_sessions']} T sessions ({s['n_unique_tickers']} tickers). "
        f"**{s['n_etf']} are leveraged/theme ETFs** — the raw Finviz screen "
        f"is 38% products, not single names. Common-stock extremes: "
        f"**{s['n_common']}**.",
        f"- Only **{_pct_share(s['n_liquid_gainer'], s['n_name_days'])}** "
        f"clear the gainer-tape liquidity floor (mcap ≥ ${MIN_LIQ_MCAP_M:.0f}M "
        f"and ADV ≥ {MIN_ADV_K:.0f}k, not an ETF).",
        f"- Only **{_pct_share(s['n_liquid_buy'], s['n_name_days'])}** "
        f"clear the live BUY mcap floor (${MIN_BUY_MCAP_M:.0f}M, not micro).",
        f"- Morning book had the ticker on "
        f"**{_pct_share(s['n_in_book'], s['n_name_days'])}** name-days "
        "(CSV universe). Missing T+1 book files: 08-24, 08-25, 08-26, 08-28 "
        "(those mornings cannot prove BUY-walk eligibility).",
        f"- **Green-pile eligible: {_pct_share(s['n_green_eligible'], s['n_name_days'])}** "
        f"(MRVI 08-21: cores passed, pile not yet the live ranker). "
        f"On a used pile: **{s['n_in_pile']}** "
        f"(RNXT 09-04: green, but $107M micro so BUY still skips).",
        f"- Factor-mine 09:30 panel (union of flatten / probable / yday_gainer / "
        f"ohlc_hot / earn / mover): **{_pct_share(s['n_in_panel'], s['n_name_days'])}**.",
        f"- Yesterday-gainer universe (liquid top-25, mcap ≥ $100M): "
        f"**{_pct_share(s['n_yday_gainer'], s['n_name_days'])}**.",
        f"- `ohlc_hot` / `probable` (these **drop exploded tape**): "
        f"{s['n_ohlc_hot']} / {s['n_probable']}.",
        f"- **Chosen next morning: {_pct_share(s['n_chosen'], s['n_name_days'])}** "
        f"(1d BUY {s['n_1d']}, 3d BUY {s['n_3d']}, flatten would-buy {s['n_flatten']}).",
        "",
        "So: the machines *can* see a few liquid rips on the yday-gainer "
        "list, but the live 3d size book, green pile, coil/probable recipes, "
        "and flatten gate are built to **keep exploded names off the ticket**.",
        "",
        "## Do the gates allow them?",
        "",
        "| Gate | What it does to extremes | This window |",
        "|---|---|---|",
        f"| Liquidity BUY walk (`mcap < $400M` or micro) | Hard skip on BUY | "
        f"{s['n_name_days'] - s['n_liquid_buy']} of {s['n_name_days']} fail |",
        f"| Dead RelVol printed in (0, {RELVOL_DEAD}) | BUY veto + green-pile veto | "
        f"{s['n_dead_relvol']} name-days |",
        f"| Green pile (all-green join/AB/peer/gen, sector/news not red) | "
        f"Need 8 liquid greens or fallback weighted | eligible {s['n_green_eligible']}; "
        f"pile used on {s['n_pile_used_mornings']}/{s['n_t_sessions']} T+1 mornings |",
        f"| Decision lattice `bull_eligible=false` | BUY veto | "
        f"{s['n_lattice_blocked']} |",
        f"| HARD_RED (`S ≤ −3`) | Live flatten sits; wish-list still prints | "
        f"{s['n_hard_red_mornings']} T+1 mornings |",
        f"| Flatten gate (`flatten_ok`) | Live 09:30 tickets only on GO | "
        f"{s['n_flatten_ok_mornings']} GO mornings |",
        f"| `too_extended` (ret_5 > {TOO_EXT_RET5:g} or rvol > {TOO_EXT_RVOL:g}) | "
        f"Dropped from ohlc_hot | {s['n_too_extended']} of those we could score |",
        f"| Probable / coil (`ret_5 ≤ {PROBABLE_RET5:g}`, rvol ≤ {COIL_RVOL:g}) | "
        f"Exploded week names fail | probable list hits {s['n_probable']}; "
        f"coil recipe picks {s['n_coil_h1']} |",
        f"| Rank / sector / industry / large-mega caps | 25 seats, 4/sector, "
        f"3/industry, 4 large-mega | see first-blocker table |",
        "",
        "### First blocker (one per name-day)",
        "",
        "| First blocker | n |",
        "|---|---:|",
    ]
    for name, n in s["first_blockers"]:
        lines.append(f"| `{name}` | {n} |")
    common_block = Counter(r["first_blocker"] for r in names if not r["etf"])
    lines += [
        "",
        "Common stock only (ETFs stripped):",
        "",
        "| First blocker | n |",
        "|---|---:|",
    ]
    for name, n in common_block.most_common():
        lines.append(f"| `{name}` | {n} |")
    lines += [
        "",
        "A name can fail several gates at once. The first-blocker column is "
        "the earliest hard stop in the live BUY / select path. "
        "`flatten_robust` would-buy is **not** the $400M BUY walk — "
        "ARCT ($319M) still printed on the 08-21 wish-list as a mover / "
        "yday-gainer.",
        "",
        "## Do the selectors choose them?",
        "",
        "| Selector (T+1 morning) | Extreme name-days chosen |",
        "|---|---|",
        f"| Live 1d BUY | {_pct_share(s['n_1d'], s['n_name_days'])} |",
        f"| Live 3d BUY (flatten_robust size book) | {_pct_share(s['n_3d'], s['n_name_days'])} |",
        f"| flatten_robust would-buy / wish-list | {_pct_share(s['n_flatten'], s['n_name_days'])} |",
        f"| Any of the three above | {_pct_share(s['n_chosen'], s['n_name_days'])} |",
        f"| Factor-mine panel (any 09:30 list) | {_pct_share(s['n_in_panel'], s['n_name_days'])} |",
        f"| `yday_gainer` universe | {_pct_share(s['n_yday_gainer'], s['n_name_days'])} |",
        f"| `ohlc_hot` universe | {_pct_share(s['n_ohlc_hot'], s['n_name_days'])} |",
        f"| `probable` universe | {_pct_share(s['n_probable'], s['n_name_days'])} |",
        f"| `flatten` universe | {_pct_share(s['n_fm_flatten'], s['n_name_days'])} |",
        f"| Recipe `union_h1` top 8 | {_pct_share(s['n_union_h1'], s['n_name_days'])} |",
        f"| Recipe `yday_gainer_h1` top 8 | {_pct_share(s['n_yday_h1'], s['n_name_days'])} |",
        f"| Recipe `flatten_h1` | {_pct_share(s['n_flat_h1'], s['n_name_days'])} |",
        f"| Recipe `flatten_live_h1` (gate must fire) | {_pct_share(s['n_live_h1'], s['n_name_days'])} |",
        f"| Recipe `ohlc_hot_h1` | {_pct_share(s['n_hot_h1'], s['n_name_days'])} |",
        f"| Recipe `probable_h1` | {_pct_share(s['n_prob_h1'], s['n_name_days'])} |",
        f"| Recipe `coil_off` | {_pct_share(s['n_coil_h1'], s['n_name_days'])} |",
        f"| Recipe `union_vol_green_h1` | {_pct_share(s['n_volg_h1'], s['n_name_days'])} |",
        "",
        "### Names that *were* chosen",
        "",
    ]
    if s["chosen_rows"]:
        lines.append("| T | T+1 | Ticker | Week | Day T | RelVol | mcap $M | Where | T+1 H after fee |")
        lines.append("|---|---|---|---:|---:|---:|---:|---|---:|")
        for r in s["chosen_rows"]:
            where = []
            if r["in_1d_buy"]:
                where.append("1d")
            if r["in_3d_buy"]:
                where.append("3d")
            if r["in_flatten_would_buy"]:
                where.append("flatten")
            lines.append(
                f"| {r['t_date']} | {r['t1_date']} | **{r['ticker']}** | "
                f"{r['t_week']:.1f}% | {r['t_change']:+.1f}% | {r['t_relvol']:.2f} | "
                f"{_fmt_m(r['t_mcap_m'])} | {','.join(where)} | "
                f"{_fmt(r['t1_h_after_fee'])} |"
            )
    else:
        lines.append("None.")
    lines += [
        "",
        "ARCT is the only live hit: T=08-20 +9% / RelVol 4.5 / week +43%, "
        "T+1 flatten wish-list (also `yday_gainer` + `mover_buy`). "
        "It was **under** the $400M BUY floor, so the 3d size book never "
        "listed it; flatten still wanted it. Next-open H after fees **+19.24%**. "
        "One name. Not a pattern.",
    ]
    lines += [
        "",
        "### Liquid names that were *not* chosen (mcap ≥ $100M, ADV ≥ 500k)",
        "",
        "| T | T+1 | Ticker | Week | Day T | RelVol | mcap $M | First blocker | T+1 H after fee |",
        "|---|---|---|---:|---:|---:|---:|---|---:|",
    ]
    skipped_liq = [r for r in s["liq_rows"] if not r["chosen_any_live"] and not r["etf"]]
    for r in skipped_liq:
        lines.append(
            f"| {r['t_date']} | {r['t1_date']} | {r['ticker']} | "
            f"{r['t_week']:.1f}% | {r['t_change']:+.1f}% | {r['t_relvol']:.2f} | "
            f"{_fmt_m(r['t_mcap_m'])} | `{r['first_blocker']}` | "
            f"{_fmt(r['t1_h_after_fee'])} |"
        )
    if not skipped_liq:
        lines.append("| — | | | | | | | | |")

    lines += [
        "",
        "## Thin after-fee counterfactual",
        "",
        "Equal-weight T+1 **Change from Open** minus **15 bp**. "
        "Not a cash book, not dollar-weighted, not a ship signal. "
        "Current picks = that morning's **3d BUY** (fallback 1d if 3d empty).",
        "Force-include = those current picks **plus** that T's non-ETF extremes.",
        "",
        "| Book | Day-mean after-fee H | Graded mornings | Name-day after-fee H | Name-day H+ |",
        "|---|---:|---:|---:|---:|",
        f"| Current 3d/1d picks | {_fmt(s['daymean_current'])} | {s['n_days_current']} | "
        f"— | — |",
        f"| Extremes only | {_fmt(s['daymean_extremes'])} | {len([x for x in sessions if x.get('extreme_h_after_fee') is not None])} | "
        f"{_fmt(s['h_all']['mean'])} n={s['h_all']['n']} | {_fmt(s['h_all']['hit'], 1)}% |",
        f"| Liquid extremes only | {_fmt(s['daymean_liq_extremes'])} | "
        f"{len([x for x in sessions if x.get('liq_extreme_h_after_fee') is not None])} | "
        f"{_fmt(s['h_liq']['mean'])} n={s['h_liq']['n']} | {_fmt(s['h_liq']['hit'], 1)}% |",
        f"| Force-include extremes into current | {_fmt(s['daymean_force'])} | {s['n_days_force']} | — | — |",
        f"| Extremes that *were* chosen | — | — | {_fmt(s['h_chosen']['mean'])} n={s['h_chosen']['n']} | {_fmt(s['h_chosen']['hit'], 1)}% |",
        "",
        "Read this as a **miss / leak check**, not an edge. A +40% week name "
        "often fades the next open (MRNA 08-19 → 08-20 H −6.94% before fees). "
        "Force-include will usually **drag** the current book if the extremes "
        "are worse than the 3d size names — or inflate one-off if a rip continues.",
        "",
        "## Session board",
        "",
        "| T | T+1 | Ext | Liq | Book | Elig | Panel | yday | Chosen | Pile | HARD_RED | Flat GO | Ext Hff | Cur Hff | Force Hff |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|---:|---:|---:|",
    ]
    for r in sessions:
        lines.append(
            f"| {r['t_date']} | {r['t1_date']} | {r['n_extremes']} | "
            f"{r['n_liquid_gainer']} | {r['n_book_present']} | "
            f"{r['n_book_buy_eligible']} | {r['n_in_panel']} | "
            f"{r['n_yday_gainer']} | {r['n_chosen_any']} | "
            f"{'Y' if r['green_pile_used'] else 'n'} | "
            f"{'Y' if r['hard_red'] else 'n'} | "
            f"{'Y' if r['flatten_ok'] else 'n'} | "
            f"{_fmt(r['extreme_h_after_fee'])} | "
            f"{_fmt(r['current_3d_h_after_fee'])} | "
            f"{_fmt(r['force_include_h_after_fee'])} |"
        )
    lines += [
        "",
        "## Big names (mcap ≥ $1B or chosen)",
        "",
        "| T | T+1 | Ticker | Week | Day T | RelVol | mcap $M | First blocker | Chosen | T+1 Hff |",
        "|---|---|---|---:|---:|---:|---:|---|---|---:|",
    ]
    for r in s["notable"]:
        lines.append(
            f"| {r['t_date']} | {r['t1_date']} | {r['ticker']} | "
            f"{r['t_week']:.1f}% | {r['t_change']:+.1f}% | {r['t_relvol']:.2f} | "
            f"{_fmt_m(r['t_mcap_m'])} | `{r['first_blocker']}` | "
            f"{'Y' if r['chosen_any_live'] else 'n'} | {_fmt(r['t1_h_after_fee'])} |"
        )
    lines += [
        "",
        "## Method notes",
        "",
        "- T tape is the dated Finviz export (`data/exports/finviz_YYYY-MM-DD.csv`). "
        "That file is the after-close / overnight packet used everywhere else.",
        "- T+1 picks: `data/stock_book/YYYY-MM-DD_stock_book.json` BUY lists, "
        "matching CSV universe, `*_green.json`, "
        "`03_scoreboard/flatten_lookback_action.json` (through 09-08) plus "
        "`01_daily/*_flatten_card.md` would-buy tables for 09-09..11, "
        "`data/factor_mine/panel.json` 09:30 candidate rows.",
        "- T+1 H is Finviz **Change from Open** on the T+1 export (09:30 → 16:00 stand-in). "
        "`data/prices/ohlc.parquet` is not in this checkout; official bars were empty.",
        "- Green-pile RelVol=0 (no print) is **not** a veto; only printed "
        f"(0, {RELVOL_DEAD}) is. Morning books often show 0.01–0.2 pre-open — that **is** a veto.",
        "- `flatten_robust` live tickets still sit on HARD_RED / no-flatten mornings; "
        "would-buy is the 3d size wish-list with holdings disregarded.",
        "- Factor-mine recipes here are the leak-free 09:30 lists already mined; "
        "this cut does not rebuild the $10k cash blotter.",
        "",
        "Reusable rows: `extreme_vol_price_names.csv`, `extreme_vol_price_sessions.csv`. "
        "Regenerate with `python3 excel_bot/research/extreme_vol_price_audit.py`.",
        "",
        "Live frozen. Do not treat a force-include mean as a wire.",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    names, sessions, summary = audit()
    write_csv(OUT_NAMES, names)
    write_csv(OUT_SESS, sessions)
    write_md(summary, sessions, names)
    print(f"name-days {summary['n_name_days']} chosen {summary['n_chosen']} "
          f"liquid {summary['n_liquid_gainer']} book {summary['n_in_book']}")
    print(f"wrote {OUT_NAMES}")
    print(f"wrote {OUT_SESS}")
    print(f"wrote {OUT_MD}")


if __name__ == "__main__":
    main()
