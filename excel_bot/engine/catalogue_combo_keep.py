"""Catalogue combo KEEP — fee-aware prove on Clock-B tells.

Research only. Live flatten_robust / cash book are not imported or written.

Spike is the 10 Cyrus catalogue combinations mapped onto Fullscan
Clock-B fields (T−1 Finviz + panel prior tape / open-print fills).
Not a remine of EXCEL_FACTOR_MINE letter grids.

Candidate aisle = restored multi-src morning panel ∪ Theme Radar
Clock-B gap+RelVol flagged oppset (not flatten-only hot4).
KEEP bar (Cyrus): prove (time-split holdout) n ≥ 30 AND after-fee H
win rate > 55%. Futubull FEE_RT=0.0015. Lift-only is never KEEP.
Thin n with high WR is FAIL.

  python3 excel_bot/engine/catalogue_combo_keep.py
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import date
from glob import glob

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    SAME_ROW_LEAK_ABORT,
    assert_excel_clock_gate,
    assert_feature_legal,
    gate_payload,
)
from excel_factor_mine import (  # noqa: E402
    cutoff_from_dates,
    keep_verdict,
    score_hits,
    split_rows,
    walk_folds,
)
from join_post_813 import FEE_RT, is_session  # noqa: E402
from j_winrate import FEE_CAVEAT, MIN_FIRES, WIN_BAR  # noqa: E402

SCOREBOARD = os.path.join(REPO, "03_scoreboard")
PANEL_PATH = os.path.join(REPO, "data", "factor_mine", "panel.json")
EXPORT_DIR = os.path.join(REPO, "data", "exports")
OPPSET_PATH = os.path.join(
    REPO, "data", "theme_radar", "oppset_clock_b", "oppset_flagged.csv",
)
OPPSET_SOURCE = "theme-radar a782cc2b research/oppset_clock_b"
BOARD_MD = os.path.join(SCOREBOARD, "CATALOGUE_COMBO_KEEP.md")
BOARD_JSON = os.path.join(SCOREBOARD, "catalogue_combo_keep.json")

HOLD_FRAC = 0.30
CORE_AUX = frozenset({
    "probable", "yday_gainer", "yday_mover", "ohlc_hot", "earn_react",
})
# Same-day Gap / minute-hour Performance* are never Clock B, even if
# present on a T dump. T−1 RelVol/Change on finviz_asof stay aisle
# context, not these column names.
FORBIDDEN_FEATURE_FIELDS = frozenset({
    "H", "I", "net", "i_net", "close", "high", "low",
    "Gap", "Change", "change_pct", "RelVol",
    "Change from Open", "After-Hours Close", "After-Hours Change",
    "After-Hours Volume", "Performance (1 Minute)", "Performance (2 Minutes)",
    "Performance (3 Minutes)", "Performance (5 Minutes)",
    "Performance (10 Minutes)", "Performance (15 Minutes)",
    "Performance (30 Minutes)", "Performance (1 Hour)",
    "Performance (2 Hours)", "Performance (4 Hours)",
})
# Clock-B flags matchers may read. H/I/net stay labels only.
FEATURE_KEYS = frozenset({
    "mod_mom", "completed_breakout", "peer_sector_strong",
    "fresh_pos_catalyst", "limited_extension",
    "earn_improve", "raised_guidance", "fav_reaction",
    "neg_catalyst", "rel_weak", "failed_recovery", "shortable",
    "extreme_ext", "diminishing", "failed_breakout",
    "sector_weak", "stock_firm", "stock_beats_sector",
    "insider_buy", "cash_ok", "stabilizing",
    "high_short", "pos_surprise",
    "cash_worse", "issuance_headline", "failed_rally",
    "on_frozen_picks",
})

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
GUIDANCE_HINT = ("guidance", "outlook", "forecast")
GUIDANCE_LIFT = ("rais", "lift", "boost", "hike")
ISSUANCE_HINT = (
    "offering", "dilut", "atm offering", "share issuance",
    "registered direct", "public offering", "convertible",
)
KRONOS_GLOBS = (
    "data/kronos/**",
    "data/**/*kronos*",
    "03_scoreboard/*KRONOS*",
    "03_scoreboard/*kronos*",
)
BORROW_GLOBS = (
    "data/borrow/**",
    "data/**/*borrow*",
    "data/**/*locate*",
    "03_scoreboard/*BORROW*",
)


def _num(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s or s in ("-", "nan", "None", "null"):
        return None
    if s.endswith("%"):
        s = s[:-1]
    try:
        x = float(s)
    except (TypeError, ValueError):
        return None
    if x != x or x in (float("inf"), float("-inf")):
        return None
    return x


def _tick(v):
    return str(v or "").strip().upper()


def _finite(x):
    v = _num(x)
    return v


def _box(row, key):
    boxes = row.get("boxes") or {}
    return str(boxes.get(key) or "").strip().lower()


def _title(text):
    return str(text or "").strip().lower()


def _sane_surprise(v):
    """Drop junk Finviz prints (e.g. −15622%)."""
    x = _finite(v)
    if x is None or abs(x) > 200:
        return None
    return x


def assert_atoms_legal():
    """Refuse Excel same-row close leaks and H/I-as-feature."""
    for col in SAME_ROW_LEAK_ABORT:
        try:
            assert_feature_legal("value", col, 0)
        except ValueError:
            continue
        raise ValueError(f"LEAK abort: same-row {col} did not abort")
    for col in SAME_ROW_LEAK_ABORT:
        assert_feature_legal("value", col, 1)
    for name in ("H", "I"):
        try:
            assert_feature_legal("value", name, 0)
        except ValueError:
            continue
        raise ValueError(f"LEAK abort: same-row {name} must stay a label")
    leak = FEATURE_KEYS & FORBIDDEN_FEATURE_FIELDS
    if leak:
        raise ValueError(f"LEAK abort: feature keys include {sorted(leak)}")
    return True


def assert_flags_legal(flags):
    """Runtime abort if a matcher flag smuggles a close-knowable field."""
    if not flags:
        return True
    bad = set(flags) & FORBIDDEN_FEATURE_FIELDS
    if bad:
        raise ValueError(f"LEAK abort: flags contain {sorted(bad)}")
    extra = set(flags) - FEATURE_KEYS
    if extra:
        raise ValueError(f"unknown flag keys {sorted(extra)}")
    return True


def leak_check():
    clocks = assert_excel_clock_gate()
    assert_atoms_legal()
    leak = "PASS"
    try:
        for col in SAME_ROW_LEAK_ABORT:
            assert_feature_legal("value", col, 0)
        leak = "FAIL — same-row DF/BB/BQ did not abort"
    except ValueError:
        leak = "PASS"
    return clocks, leak


def find_artifacts(patterns):
    hits = []
    for pat in patterns:
        hits.extend(glob(os.path.join(REPO, pat), recursive=True))
    return sorted({p for p in hits if os.path.isfile(p)})


def kronos_available():
    return bool(find_artifacts(KRONOS_GLOBS))


def borrow_available():
    return bool(find_artifacts(BORROW_GLOBS))


def load_panel(path=None):
    path = path or PANEL_PATH
    if not os.path.isfile(path):
        return {"rows": [], "session_dates": [], "lookback": None, "n_rows": 0}
    raw = json.load(open(path, encoding="utf-8"))
    return raw


def day_has_aux(rows):
    srcs = set()
    for r in rows:
        srcs.update(r.get("sources") or [])
    return bool(srcs & CORE_AUX)


def load_oppset_flagged(path=None):
    """Theme Radar Clock-B flagged membership. Keyed by join_morning T.

    CSV features are T−1 (finviz_asof). Same-day T Gap/RelVol are absent.
    """
    path = path or OPPSET_PATH
    by = defaultdict(list)
    if not path or not os.path.isfile(path):
        return by
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for rec in csv.DictReader(f):
            iso = str(rec.get("join_morning") or "")[:10]
            asof = str(rec.get("finviz_asof") or "")[:10]
            t = _tick(rec.get("ticker"))
            if not iso or not t or not is_session(iso):
                continue
            if asof and asof >= iso:
                raise ValueError(
                    f"LEAK abort: oppset finviz_asof {asof} is not T−1 for {iso}"
                )
            flag = str(rec.get("any_opp") or "").strip()
            if flag not in ("1", "true", "True"):
                continue
            by[iso].append(rec)
    return by


def load_finviz_labels(iso, export_dir=None):
    """Same-day Open/Close as *labels only*. Never passed to build_flags."""
    export_dir = export_dir or EXPORT_DIR
    path = os.path.join(export_dir, f"finviz_{iso}.csv")
    out = {}
    if not os.path.isfile(path):
        return out
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for rec in csv.DictReader(f):
            t = _tick(rec.get("Ticker"))
            o = _finite(rec.get("Open"))
            # Fullscan Elite dumps stamp session close as Price, not Close.
            c = _finite(rec.get("Close")) or _finite(rec.get("Price"))
            if not t or not o or not c or o <= 0:
                continue
            out[t] = {"open": o, "close": c}
    return out


def synth_oppset_row(rec):
    """Panel-shaped row from flagged CSV. T−1 tape only; no T Gap/RelVol."""
    iso = str(rec.get("join_morning") or "")[:10]
    asof = str(rec.get("finviz_asof") or "")[:10]
    if asof and asof >= iso:
        raise ValueError(f"LEAK abort: oppset finviz_asof {asof} same-row as {iso}")
    chg = _finite(rec.get("change_pct"))
    pw = _finite(rec.get("pweek"))
    rvol = _finite(rec.get("rvol"))
    return {
        "date": iso,
        "ticker": _tick(rec.get("ticker")),
        "sources": ["oppset_clock_b"],
        "finviz_asof": asof or None,
        "news_export_date": asof or None,
        "prior_date": asof or None,
        "ohlc_ret_1": chg,
        "ohlc_ret_5": pw,
        "ohlc_rvol": rvol,
        "fv_rvol": rvol,
        "last_green": bool(chg is not None and chg > 0),
        "last_red": bool(chg is not None and chg < 0),
        "ohlc_break_10": False,
        "ohlc_nr7": False,
        "rsi_os": False,
        "rsi_ob": False,
        "erd_earn_react": False,
        "boxes": {"sector": "missing"},
        "open": None,
        "close": None,
    }


def aisle_rows(panel, oppset_by_date=None):
    """Multi-src morning panel ∪ Clock-B oppset. Flatten-only days stay out
    unless the Theme Radar flagged set covers that morning.
    """
    if oppset_by_date is None:
        oppset_by_date = load_oppset_flagged()
    by = defaultdict(list)
    for r in panel.get("rows") or []:
        iso = str(r.get("date") or "")[:10]
        if not iso or not is_session(iso):
            continue
        by[iso].append(r)
    out = []
    aisle_dates = []
    skipped = {}
    stats = {
        "n_panel": 0, "n_oppset_only": 0, "n_overlap": 0,
        "oppset_source": OPPSET_SOURCE if oppset_by_date else None,
    }
    days = sorted(set(by) | set(oppset_by_date or {}))
    for iso in days:
        day = list(by.get(iso) or [])
        oset = list((oppset_by_date or {}).get(iso) or [])
        if not day_has_aux(day) and not oset:
            skipped[iso] = {
                "n": len(day),
                "sources": sorted({s for r in day for s in (r.get("sources") or [])}),
                "why": "flatten-only / no Finviz-OHLC aux / no Clock-B oppset",
            }
            continue
        aisle_dates.append(iso)
        seen = set()
        oset_by_tick = {_tick(r.get("ticker")): r for r in oset}
        for r in day:
            t = _tick(r.get("ticker"))
            if not t:
                continue
            srcs = list(r.get("sources") or [])
            extra = {}
            hit = oset_by_tick.get(t)
            if hit is not None:
                asof = clock_b_asof(iso, hit.get("finviz_asof"))
                if asof:
                    extra["finviz_asof"] = asof
                    extra["news_export_date"] = asof
                    extra["prior_date"] = asof
                if "oppset_clock_b" not in srcs:
                    srcs = srcs + ["oppset_clock_b"]
                stats["n_overlap"] += 1
            r = dict(r, sources=srcs, **extra)
            seen.add(t)
            out.append(r)
            stats["n_panel"] += 1
        for rec in oset:
            t = _tick(rec.get("ticker"))
            if not t or t in seen:
                continue
            out.append(synth_oppset_row(rec))
            seen.add(t)
            stats["n_oppset_only"] += 1
    return out, aisle_dates, skipped, stats


def list_finviz_dates(export_dir=None):
    export_dir = export_dir or EXPORT_DIR
    dates = []
    if not os.path.isdir(export_dir):
        return dates
    for name in os.listdir(export_dir):
        if not name.startswith("finviz_") or not name.endswith(".csv"):
            continue
        iso = name[len("finviz_"):-4]
        if len(iso) == 10 and iso[4] == "-":
            dates.append(iso)
    return sorted(set(dates))


def _is_etf(rec):
    ind = str(rec.get("Industry") or "")
    et = str(rec.get("ETF Type") or "").strip()
    return ind == "Exchange Traded Fund" or bool(et)


def load_finviz_index(iso, export_dir=None):
    """Ticker → Clock-B fields from dated export. Empty if missing."""
    export_dir = export_dir or EXPORT_DIR
    path = os.path.join(export_dir, f"finviz_{iso}.csv")
    if not os.path.isfile(path):
        return {}, {}
    by = {}
    sector_xs = defaultdict(list)
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for rec in csv.DictReader(f):
            t = _tick(rec.get("Ticker"))
            if not t:
                continue
            row = {
                "sector": str(rec.get("Sector") or "").strip(),
                "perf_week": _finite(rec.get("Performance (Week)")),
                "sma20": _finite(rec.get("20-Day Simple Moving Average")),
                "sma50": _finite(rec.get("50-Day Simple Moving Average")),
                "rsi": _finite(rec.get("Relative Strength Index (14)")),
                "atr": _finite(rec.get("Average True Range")),
                "rvol": _finite(rec.get("Relative Volume")),
                "avgvol": _finite(rec.get("Average Volume")),
                "eps_surp": _sane_surprise(rec.get("EPS Surprise")),
                "rev_surp": _sane_surprise(rec.get("Revenue Surprise")),
                "insider_txn": _finite(rec.get("Insider Transactions")),
                "inst_txn": _finite(rec.get("Institutional Transactions")),
                "cash_sh": _finite(rec.get("Cash/sh")),
                "current_ratio": _finite(rec.get("Current Ratio")),
                "de": _finite(rec.get("Total Debt/Equity")),
                "short_float": _finite(rec.get("Short Float")),
                "short_ratio": _finite(rec.get("Short Ratio")),
                "shortable": str(rec.get("Shortable") or "").strip().lower() == "yes",
                "news_title": str(rec.get("News Title") or ""),
                "earn_date": str(rec.get("Earnings Date") or ""),
            }
            leak = set(row) & FORBIDDEN_FEATURE_FIELDS
            if leak:
                raise ValueError(f"LEAK abort: Finviz snap stored {sorted(leak)}")
            by[t] = row
            if not _is_etf(rec) and row["sector"] and row["perf_week"] is not None:
                sector_xs[row["sector"]].append(row["perf_week"])
    sector_med = {}
    for sec, xs in sector_xs.items():
        if len(xs) < 5:
            continue
        ys = sorted(xs)
        sector_med[sec] = ys[len(ys) // 2]
    return by, sector_med


def clock_b_asof(join_morning, finviz_asof):
    """Theme Radar pair: T = join_morning, T−1 = finviz_asof.

    Same-day asof is a leak (would expose T Gap / RelVol / minute Performance).
    """
    t = str(join_morning or "")[:10]
    a = str(finviz_asof or "")[:10]
    if not t or not a:
        return None
    if a >= t:
        raise ValueError(
            f"LEAK abort: finviz_asof {a} is not T−1 for join_morning {t}"
        )
    return a


def prior_finviz_date(iso, fv_dates, row=None):
    """Open-knowable Finviz date. Oppset rows join exactly on finviz_asof.

    Panel-only rows (no Theme Radar pair) may walk to the last prior
    export. Never returns join_morning itself.
    """
    row = row or {}
    iso = str(iso or row.get("date") or "")[:10]
    stamped = str(
        row.get("finviz_asof")
        or row.get("news_export_date")
        or row.get("prior_date")
        or ""
    )[:10]
    if stamped:
        return clock_b_asof(iso, stamped)
    prev = [d for d in fv_dates if d < iso]
    return prev[-1] if prev else None


def t2_finviz_date(t1, fv_dates):
    if not t1:
        return None
    prev = [d for d in fv_dates if d < t1]
    return prev[-1] if prev else None


def _has_any(text, words):
    return any(w in text for w in words)


def _raised_guidance(title):
    t = _title(title)
    if not t or not _has_any(t, GUIDANCE_HINT):
        return False
    return _has_any(t, GUIDANCE_LIFT)


def _issuance(title):
    t = _title(title)
    return bool(t) and _has_any(t, ISSUANCE_HINT)


def _pos_headline(title):
    t = _title(title)
    if not t:
        return False
    hit_pos = _has_any(t, NEWS_POS)
    hit_neg = _has_any(t, NEWS_NEG)
    return hit_pos and not hit_neg


def _neg_headline(title):
    t = _title(title)
    if not t:
        return False
    hit_pos = _has_any(t, NEWS_POS)
    hit_neg = _has_any(t, NEWS_NEG)
    return hit_neg and not hit_pos


def build_flags(row, fv=None, fv_prev=None, sector_med=None):
    """Clock-B flags only. Never reads same-row H/I/Gap/Change/RelVol."""
    fv = fv or {}
    fv_prev = fv_prev or {}
    sector_med = sector_med or {}
    rsi = _finite(row.get("rsi"))
    if rsi is None:
        rsi = _finite(fv.get("rsi"))
    sma20 = _finite(row.get("fv_sma20"))
    if sma20 is None:
        sma20 = _finite(fv.get("sma20"))
    ret1 = _finite(row.get("ohlc_ret_1"))
    ret5 = _finite(row.get("ohlc_ret_5"))
    rvol = _finite(row.get("ohlc_rvol"))
    if rvol is None:
        rvol = _finite(row.get("fv_rvol"))
    if rvol is None:
        rvol = _finite(fv.get("rvol"))
    last_green = bool(row.get("last_green"))
    last_red = bool(row.get("last_red"))
    break10 = bool(row.get("ohlc_break_10"))
    news_box = str(row.get("news_box") or "").lower()
    news_prior = str(row.get("news_prior") or "").lower()
    title = fv.get("news_title") or ""
    sector = fv.get("sector") or ""
    sec_pw = sector_med.get(sector)
    pw = _finite(fv.get("perf_week"))
    stock_pw = ret5 if ret5 is not None else pw

    fresh_pos = (
        news_box == "good" or news_prior == "good"
        or _box(row, "catal") == "good" or _box(row, "news") == "good"
        or _pos_headline(title)
    )
    neg_cat = (
        news_box == "bad" or news_prior == "bad"
        or _box(row, "news") == "bad" or _neg_headline(title)
    )
    earn_imp = (
        (fv.get("eps_surp") is not None and fv["eps_surp"] > 0)
        or (fv.get("eps_surp") is None and fv.get("rev_surp") is not None
            and fv["rev_surp"] > 0)
    )
    days_e = _finite(row.get("erd_days_since_E"))
    fav_react = bool(row.get("erd_earn_react")) or (
        days_e is not None and days_e <= 10 and last_green
    )
    cash = _finite(fv.get("cash_sh"))
    cash_prev = _finite(fv_prev.get("cash_sh"))
    cr = _finite(fv.get("current_ratio"))
    de = _finite(fv.get("de"))
    cash_ok = False
    cash_worse = False
    if cash is not None and cash_prev is not None:
        cash_ok = cash > cash_prev
        cash_worse = cash < cash_prev
    elif cash is not None:
        cash_ok = cash >= 1.0 and (cr is None or cr >= 1.2)
        cash_worse = cash < 0.5 and (de is not None and de > 1.0)
    insider = _finite(fv.get("insider_txn"))
    short_float = _finite(fv.get("short_float"))
    srcs = set(row.get("sources") or [])

    flags = {
        "mod_mom": bool(
            (ret5 is not None and 2.0 <= ret5 <= 12.0)
            or (pw is not None and 2.0 <= pw <= 15.0)
        ) and (rsi is None or 50.0 <= rsi <= 70.0),
        "completed_breakout": bool(break10 and last_green
                                   and (sma20 is None or sma20 > 0)),
        "peer_sector_strong": bool(
            (sec_pw is not None and sec_pw > 0)
            or _box(row, "sector") == "good"
            or _box(row, "peer") == "good"
        ),
        "fresh_pos_catalyst": fresh_pos,
        "limited_extension": bool(
            (rsi is None or rsi < 68.0)
            and (sma20 is None or sma20 < 12.0)
            and (ret5 is None or ret5 < 10.0)
        ),
        "earn_improve": earn_imp,
        "raised_guidance": _raised_guidance(title),
        "fav_reaction": fav_react,
        "neg_catalyst": neg_cat,
        "rel_weak": bool(
            (stock_pw is not None and sec_pw is not None
             and stock_pw < sec_pw)
            or (sma20 is not None and sma20 < 0)
            or _box(row, "sector") == "bad"
        ),
        "failed_recovery": bool(last_red and not break10),
        "shortable": bool(fv.get("shortable")),
        "extreme_ext": bool(
            (rsi is not None and rsi >= 70.0)
            or (sma20 is not None and sma20 >= 12.0)
            or (ret5 is not None and ret5 >= 12.0)
        ),
        "diminishing": bool(
            (rvol is not None and rvol >= 1.5
             and ret1 is not None and abs(ret1) < 1.5)
            or (ret1 is not None and ret5 is not None and ret5 > 0
                and ret1 < 0.25 * ret5)
        ),
        "failed_breakout": bool(last_red and (break10 or (sma20 or 0) > 0)),
        "sector_weak": bool(
            (sec_pw is not None and sec_pw < 0)
            or _box(row, "sector") == "bad"
        ),
        "stock_firm": bool(
            last_green and (ret5 is None or ret5 > 0)
            and (sma20 is None or sma20 >= 0)
        ),
        "stock_beats_sector": bool(
            stock_pw is not None and sec_pw is not None
            and stock_pw > sec_pw + 2.0
        ),
        "insider_buy": bool(insider is not None and insider >= 0.5),
        "cash_ok": cash_ok,
        "stabilizing": bool(
            (bool(row.get("rsi_os")) or bool(row.get("ohlc_nr7"))
             or (rsi is not None and 30.0 <= rsi <= 55.0))
            and not bool(row.get("rsi_ob"))
            and last_green
        ),
        "high_short": bool(short_float is not None and short_float >= 15.0),
        "pos_surprise": bool(
            (fv.get("eps_surp") is not None and fv["eps_surp"] > 0)
            or (fv.get("rev_surp") is not None and fv["rev_surp"] > 0)
        ),
        "cash_worse": cash_worse,
        "issuance_headline": _issuance(title),
        "failed_rally": bool(last_red and (ret5 is not None and ret5 > 0)),
        "on_frozen_picks": "flatten" in srcs,
    }
    assert_flags_legal(flags)
    return flags


def name_days_from_panel(panel, export_dir=None, oppset_by_date=None):
    """Open-knowable aisle name-days. H/I are labels from same-day OHLC."""
    raw, aisle_dates, skipped, stats = aisle_rows(panel, oppset_by_date)
    fv_dates = list_finviz_dates(export_dir)
    cache = {}
    sec_cache = {}
    label_cache = {}

    def _fv(iso):
        if not iso:
            return {}, {}
        if iso not in cache:
            cache[iso], sec_cache[iso] = load_finviz_index(iso, export_dir)
        return cache[iso], sec_cache[iso]

    def _labels(iso):
        if iso not in label_cache:
            label_cache[iso] = load_finviz_labels(iso, export_dir)
        return label_cache[iso]

    rows = []
    n_label_fv = 0
    for r in raw:
        iso = str(r.get("date") or "")[:10]
        tk = _tick(r.get("ticker"))
        o, c = _finite(r.get("open")), _finite(r.get("close"))
        if (not o or not c or o <= 0) and iso and tk:
            lab = _labels(iso).get(tk) or {}
            o = o or lab.get("open")
            c = c or lab.get("close")
            if lab.get("open") and lab.get("close"):
                n_label_fv += 1
        if not o or not c or o <= 0:
            continue
        stamped = str(
            r.get("finviz_asof") or r.get("news_export_date")
            or r.get("prior_date") or ""
        )[:10]
        if stamped == iso:
            raise ValueError(f"LEAK abort: Finviz date {stamped} is same-row as {iso}")
        t1 = prior_finviz_date(iso, fv_dates, r)
        if t1 == iso:
            raise ValueError(f"LEAK abort: Finviz date {t1} is same-row as {iso}")
        by, sec_med = _fv(t1)
        t2 = t2_finviz_date(t1, fv_dates)
        by2, _ = _fv(t2)
        flags = build_flags(r, by.get(tk), by2.get(tk), sec_med)
        H = (c - o) / o
        rows.append({
            "date": iso,
            "ticker": tk,
            "H": H,
            "net": H - FEE_RT,
            "net_short": -H - FEE_RT,
            "flags": flags,
            "sources": list(r.get("sources") or []),
        })
    stats["n_label_finviz_t"] = n_label_fv
    stats["n_scored"] = len(rows)
    return rows, aisle_dates, skipped, stats


def combo_hits(rows, atoms, *, side="long"):
    key = "net_short" if side == "short" else "net"
    hits = []
    for r in rows:
        if r.get(key) is None:
            continue
        fl = r.get("flags") or {}
        if all(fl.get(a) for a in atoms):
            hits.append({
                "net": r[key],
                "date": r.get("date"),
                "ticker": r.get("ticker"),
            })
    return hits


def score_combo(rows, atoms, *, side="long"):
    return score_hits(combo_hits(rows, atoms, side=side))


# 10 priority combos. role=need skips fee prove.
COMBOS = (
    {
        "id": 1, "key": "c1_continuation",
        "title": "Moderate momentum + completed breakout + peer/sector strength",
        "thesis": "continuation", "side": "long", "role": "direction",
        "atoms": ("mod_mom", "completed_breakout", "peer_sector_strong"),
        "status": "calculable",
        "have": "panel ohlc_ret_5 / last_green / ohlc_break_10 / fv_sma20 / "
                "rsi; T−1 Performance (Week); boxes.sector / peer",
        "need": "",
        "note": "T−1 RelVol is context only (VOL/CROWD). Same-day Gap out.",
    },
    {
        "id": 2, "key": "c2_room",
        "title": "Fresh material positive catalyst + limited prior extension",
        "thesis": "room after entry", "side": "long", "role": "direction",
        "atoms": ("fresh_pos_catalyst", "limited_extension"),
        "status": "have",
        "have": "news_box / news_prior / catal; T−1 News Title; rsi / "
                "fv_sma20 / ohlc_ret_5",
        "need": "",
        "note": "Headline tone is T−1 or morning packet. Mid-window news out.",
    },
    {
        "id": 3, "key": "c3_earn_drift",
        "title": "Earnings improvement + raised guidance + favorable reaction",
        "thesis": "drift", "side": "long", "role": "direction",
        "atoms": ("earn_improve", "raised_guidance", "fav_reaction"),
        "status": "calculable",
        "have": "T−1 EPS/Revenue Surprise; News Title guidance proxy; "
                "erd_earn_react / days_since_E + last_green",
        "need": "filing-grade guidance text (headline proxy used)",
        "note": "Guidance is a T−1 headline proxy, not a filing parse.",
    },
    {
        "id": 4, "key": "c4_downside",
        "title": "Negative catalyst + relative weakness + failed recovery",
        "thesis": "downside", "side": "short", "role": "direction",
        "atoms": ("neg_catalyst", "rel_weak", "failed_recovery", "shortable"),
        "status": "have",
        "have": "news bad; sector RS vs T−1 Performance (Week); last_red; "
                "Shortable",
        "need": "",
        "note": "Shorts only when Finviz Shortable=Yes. Shorts pay FEE_RT. "
                "Discovery / walk-forward are not KEEP. Oppset-only names "
                "have no 10-bar breakout, so failed_recovery is last_red "
                "and not break10 (break unknown). Borrow fee is not in "
                "the 15 bp model.",
    },
    {
        "id": 5, "key": "c5_exhaustion_veto",
        "title": "Extreme extension + diminishing progress + failed breakout",
        "thesis": "exhaustion / long veto", "side": "long", "role": "veto",
        "atoms": ("extreme_ext", "diminishing", "failed_breakout"),
        "status": "calculable",
        "have": "rsi / fv_sma20 / ohlc_ret_5; rvol+|ret1| (T−1); last_red",
        "need": "",
        "note": "Diminishing progress is VOL/CROWD — not a direction KEEP. "
                "Veto KEEP only if complement clears the fee bar and beats baseline.",
    },
    {
        "id": 6, "key": "c6_resilience",
        "title": "Stock holds firm while sector weakens",
        "thesis": "stock-specific resilience", "side": "long", "role": "direction",
        "atoms": ("sector_weak", "stock_firm", "stock_beats_sector"),
        "status": "calculable",
        "have": "T−1 sector median Performance (Week); last_green; ohlc_ret_5",
        "need": "true peer basket (sector tag is the crude CALC)",
        "note": "Peer map beyond Sector tag is NEED; sector median is used.",
    },
    {
        "id": 7, "key": "c7_insider_recovery",
        "title": "Insider buying + improving cash economics + stabilization",
        "thesis": "medium-term recovery", "side": "long", "role": "direction",
        "atoms": ("insider_buy", "cash_ok", "stabilizing"),
        "status": "have",
        "have": "T−1 Insider Transactions; Cash/sh Δ vs T−2; Current Ratio; "
                "rsi_os / ohlc_nr7 / last_green",
        "need": "",
        "note": "KEEP bar is still same-day after-fee H (open-knowable entry).",
    },
    {
        "id": 8, "key": "c8_squeeze",
        "title": "High short interest + positive surprise + constrained borrow",
        "thesis": "squeeze", "side": "long", "role": "need",
        "atoms": ("high_short", "pos_surprise"),
        "status": "need-source",
        "have": "T−1 Short Float / Short Ratio / Short Interest; EPS Surprise",
        "need": "borrow fee / locate inventory (Family 17 feasibility)",
        "note": "Constrained borrow is NEED. Fee prove skipped.",
    },
    {
        "id": 9, "key": "c9_financing",
        "title": "Deteriorating cash + credible issuance + failed rally",
        "thesis": "financing pressure", "side": "short", "role": "direction",
        "atoms": ("cash_worse", "issuance_headline", "failed_rally", "shortable"),
        "status": "calculable",
        "have": "T−1 vs T−2 Cash/sh; T−1 News Title offering/dilut; last_red "
                "after a 5-session bounce; Shortable",
        "need": "filing-grade issuance/convert/lockup events",
        "note": "Headline issuance is Clock B. Filing-grade source still NEED.",
    },
    {
        "id": 10, "key": "c10_kronos",
        "title": "Frozen Fullscan picks + Kronos agreement/disagreement",
        "thesis": "confirmation / veto", "side": "long", "role": "need",
        "atoms": ("on_frozen_picks",),
        "status": "need-source",
        "have": "flatten source flag on the morning panel",
        "need": "Kronos overlay artifacts (agree/disagree vs frozen picks)",
        "note": "No Kronos files in fullscan. Fee prove skipped.",
    },
)


def score_veto(hold_rows, atoms, baseline):
    """KEEP veto only if complement clears the fee bar and beats baseline."""
    flagged = []
    kept = []
    for r in hold_rows:
        fl = r.get("flags") or {}
        if all(fl.get(a) for a in atoms):
            flagged.append(r)
        else:
            kept.append(r)
    drop = score_hits([{"net": r["net"]} for r in flagged])
    complement = score_hits([{"net": r["net"]} for r in kept])
    base_wr = (baseline or {}).get("wr")
    comp_wr = complement.get("wr")
    lifts = (
        base_wr is not None and comp_wr is not None and comp_wr > base_wr
    )
    n, wr = complement.get("n") or 0, comp_wr
    verdict, why = keep_verdict(n, wr)
    if verdict == "KEEP" and not lifts:
        verdict, why = "FAIL", (
            f"{why}; veto lift-only vs baseline "
            f"{None if base_wr is None else f'{100 * base_wr:.1f}%'} — not KEEP"
        )
    if verdict == "KEEP":
        why = (
            f"veto complement {why}; baseline "
            f"{100 * base_wr:.1f}% → {100 * wr:.1f}%"
        )
    elif lifts and (n or 0) >= MIN_FIRES:
        verdict, why = "FAIL", (
            f"veto lifts baseline {100 * base_wr:.1f}% → {100 * wr:.1f}% "
            f"but complement {why} (lift-only is never KEEP)"
        )
    return {
        "flagged": drop,
        "complement": complement,
        "lifts_baseline": lifts,
        "verdict": verdict,
        "why": why,
        "n": n,
        "wr": wr,
    }


def score_space(disc, hold, baseline_hold):
    rows = []
    folds = walk_folds([r["date"] for r in disc])
    fold_sets = []
    for lo, hi, chunk in folds:
        s = set(chunk)
        fold_sets.append((lo, hi, [r for r in disc if r["date"] in s]))
    have_kronos = kronos_available()
    have_borrow = borrow_available()
    for spec in COMBOS:
        rec = {
            "id": spec["id"], "key": spec["key"], "title": spec["title"],
            "thesis": spec["thesis"], "side": spec["side"],
            "role": spec["role"], "atoms": list(spec["atoms"]),
            "status": spec["status"], "have": spec["have"],
            "need": spec["need"], "note": spec["note"],
        }
        if spec["role"] == "need" or spec["status"] == "need-source":
            if spec["id"] == 8 and have_borrow:
                rec["status"] = "have"
            elif spec["id"] == 10 and have_kronos:
                rec["status"] = "have"
            else:
                rec["verdict"] = "NEED"
                rec["why"] = f"NEED-source skipped: {spec['need']}"
                rec["disc"] = {"n": 0, "wr": None, "verdict": "NEED"}
                rec["hold"] = {"n": 0, "wr": None, "verdict": "NEED"}
                rows.append(rec)
                continue
        if spec["role"] == "veto":
            d = score_combo(disc, spec["atoms"], side="long")
            h = score_combo(hold, spec["atoms"], side="long")
            veto = score_veto(hold, spec["atoms"], baseline_hold)
            rec.update({
                "disc": d, "hold": h, "veto": veto,
                "verdict": veto["verdict"], "why": veto["why"],
            })
        else:
            d = score_combo(disc, spec["atoms"], side=spec["side"])
            h = score_combo(hold, spec["atoms"], side=spec["side"])
            rec.update({
                "disc": d, "hold": h,
                "verdict": h["verdict"], "why": h["why"],
            })
        walk = []
        n_ok = 0
        for lo, hi, frows in fold_sets:
            fs = score_combo(frows, spec["atoms"], side=spec["side"])
            walk.append({"lo": lo, "hi": hi, "n": fs["n"], "wr": fs["wr"],
                         "verdict": fs["verdict"]})
            if fs["n"] and fs["wr"] is not None and fs["wr"] > 0.50:
                n_ok += 1
        rec["walk"] = walk
        rec["walk_folds_wr_gt_50"] = n_ok
        rec["n_walk_folds"] = len(fold_sets)
        rows.append(rec)
    return rows


def _pct(wr):
    if wr is None:
        return "—"
    return f"{100 * wr:.1f}%"


def _mean_net(rec):
    v = (rec or {}).get("mean_net")
    if v is None:
        return "—"
    return f"{v:+.4f}"


def headline_from(scored, cutoff, baseline=None):
    keeps = [r for r in scored if r.get("verdict") == "KEEP"]
    needs = [r for r in scored if r.get("verdict") == "NEED"]
    fails = [r for r in scored if r.get("verdict") == "FAIL"]
    if keeps:
        bits = [f"`{r['key']}` {r['why']}" for r in keeps]
        return {
            "verdict": "KEEP",
            "n_keep": len(keeps),
            "text": (
                f"KEEP. {len(keeps)} of 10 cleared ≥{MIN_FIRES} prove fires "
                f"and >{100 * WIN_BAR:.0f}% after-fee H (cutoff {cutoff}): "
                + "; ".join(bits) + "."
            ),
        }
    material = []
    for r in scored:
        if r.get("verdict") == "NEED":
            continue
        h = r.get("hold") or {}
        if (h.get("n") or 0) >= MIN_FIRES:
            material.append(r)
    ranked = material or [
        r for r in scored
        if r.get("verdict") != "NEED" and (r.get("hold") or {}).get("n")
    ]
    best = None
    if ranked:
        best = max(ranked, key=lambda r: (
            ((r.get("hold") or {}).get("wr") or 0),
            (r.get("hold") or {}).get("n") or 0,
        ))
    extra = ""
    if best:
        h = best["hold"]
        extra = (
            f" Best near-miss: `{best['key']}` prove n={h.get('n')} "
            f"after-fee WR {_pct(h.get('wr'))}."
        )
    if baseline and baseline.get("n"):
        extra += (
            f" Aisle baseline prove n={baseline['n']} after-fee WR "
            f"{_pct(baseline.get('wr'))}."
        )
    text = (
        f"FAIL. 0 of 10 catalogue combos cleared the Cyrus KEEP bar on "
        f"holdout (cutoff {cutoff}). {len(needs)} NEED-source skipped, "
        f"{len(fails)} FAIL.{extra} Lift-only is not KEEP."
    )
    return {
        "verdict": "FAIL", "n_keep": 0, "text": text,
        "best": None if not best else {
            "key": best["key"], "n": (best.get("hold") or {}).get("n"),
            "wr": (best.get("hold") or {}).get("wr"),
        },
    }


def write_board(payload, path=None):
    path = path or BOARD_MD
    gp = payload["gate"]
    hl = payload["headline"]
    scored = payload["scored"]
    base = payload.get("baseline_hold") or {}
    lines = [
        "# Catalogue combo KEEP — fee-aware Clock-B prove",
        "",
        f"status={payload.get('status', 'DONE')} verdict=**{hl['verdict']}** "
        f"TIME-SPLIT cutoff={payload.get('cutoff')} "
        f"aisle_days={payload.get('n_aisle_dates')} "
        f"name-days={payload.get('n_rows')} "
        f"KEEP={hl.get('n_keep', 0)} NEED={payload.get('n_need', 0)} "
        f"FAIL={payload.get('n_fail', 0)}",
        "",
        "Research only. Live `flatten_robust` is not imported and is not written.",
        "Not a remine of `EXCEL_FACTOR_MINE` letter grids.",
        "",
        "## Headline",
        "",
        hl["text"],
        "",
        "## KEEP bar",
        "",
        f"Cyrus KEEP: **≥{MIN_FIRES} prove fires** and **after-fee H win rate "
        f"> {100 * WIN_BAR:.0f}%**. After-fee H = open-to-close minus "
        f"{FEE_RT * 10000:.0f} bp Futubull (`FEE_RT={FEE_RT}`). Shorts pay "
        "the same 15 bp (they do not collect it). A fire is an aisle "
        "name-day (multi-src panel ∪ Clock-B oppset) where every Clock-B "
        "atom is true at the 09:30 open. "
        "**Lift-only is never KEEP.** Thin n that prints >55% is FAIL. "
        "Discovery cannot KEEP. "
        f"{FEE_CAVEAT}",
        "",
        "## Clock lock",
        "",
        f"- Gate: `{gp['gate']}`",
        f"- Same-row leak abort: `{', '.join(gp['same_row_leak_abort'])}`",
        f"- Leak check: **{payload['leak']}**",
        "- Features: Theme Radar join on `join_morning` (T) + `finviz_asof` "
        "(T−1), else panel prior tape. Open is the fill, not a feature. "
        "Same-day Gap / Change / RelVol / minute Performance* are never "
        "Clock B and never flags. H/I are labels only.",
        f"- Split: TIME-SPLIT last {HOLD_FRAC:.0%} of aisle "
        f"session dates (cutoff `{payload.get('cutoff')}`). Discovery "
        "feature date is strictly before cutoff.",
        f"- Aisle: restored multi-src morning panel "
        f"(`lookback={payload.get('lookback')}`) ∪ Theme Radar Clock-B "
        f"flagged oppset (`{payload.get('oppset_source') or OPPSET_SOURCE}`). "
        "Oppset gap/RelVol flags are T−1 membership only (VOL/CROWD aisle, "
        "not direction atoms). Flatten-only / starved days stay out unless "
        "the oppset covers that morning.",
        "- Live: `flatten_robust` not imported, not written.",
        "",
        "## Aisle",
        "",
        f"Panel `{payload.get('panel_to')}` n_rows={payload.get('panel_n')} "
        f"lookback=`{payload.get('lookback')}`. "
        f"Oppset `{payload.get('oppset_source') or OPPSET_SOURCE}` "
        f"flagged={payload.get('n_oppset_flagged', '—')}. "
        f"Aisle mix: panel={payload.get('n_aisle_panel', '—')} "
        f"overlap={payload.get('n_aisle_overlap', '—')} "
        f"oppset_only={payload.get('n_aisle_oppset_only', '—')} "
        f"scored={payload.get('n_rows')}. "
        f"Oppset-only labels use same-day Finviz Open→Price "
        f"(n={payload.get('n_label_finviz_t', 0)}); not features. "
        f"Aisle days: {', '.join(payload.get('aisle_dates') or []) or '—'}.",
        "",
    ]
    skipped = payload.get("skipped_days") or {}
    if skipped:
        lines.append("Excluded days (not the restored aisle):")
        lines.append("")
        for iso, rec in skipped.items():
            lines.append(
                f"- `{iso}` n={rec.get('n')} sources={rec.get('sources')} "
                f"— {rec.get('why')}"
            )
        lines.append("")
    lines += [
        f"Holdout baseline (every aisle name-day, long): n={base.get('n', 0)} "
        f"after-fee WR {_pct(base.get('wr'))} mean_net="
        f"{_mean_net(base)}.",
        "",
        "Panel-only prove (pre-oppset fold) was 1941 name-days, 0 KEEP; "
        "best near-miss was `c6_resilience` n=236 WR 50.4%. The union is "
        "the KEEP aisle. Oppset gap+RelVol flags are membership only.",
        "",
        "## Per-combo prove",
        "",
        "| # | combo | side / role | source | prove n | after-fee WR | verdict | notes |",
        "|--:|---|---|---|---:|---:|---|---|",
    ]
    for r in scored:
        h = r.get("hold") or {}
        if r.get("role") == "veto" and r.get("veto"):
            h = (r["veto"].get("complement") or h)
        src = r.get("status") or ""
        lines.append(
            f"| {r['id']} | {r['title']} | {r['side']} / {r['role']} | {src} | "
            f"{h.get('n') if r.get('verdict') != 'NEED' else '—'} | "
            f"{_pct(h.get('wr')) if r.get('verdict') != 'NEED' else '—'} | "
            f"**{r.get('verdict')}** | {r.get('why')} |"
        )
    lines += [
        "",
        "## Cards",
        "",
    ]
    for r in scored:
        h, d = r.get("hold") or {}, r.get("disc") or {}
        lines += [
            f"### {r['id']}. {r['title']}",
            "",
            f"**{r.get('verdict')}** — {r.get('why')}",
            "",
            f"- Thesis: {r['thesis']}. Side `{r['side']}`. Role `{r['role']}`.",
            f"- Atoms: `{ ' + '.join(r.get('atoms') or []) }`.",
            f"- HAVE / calculable: {r.get('have') or '—'}",
            f"- NEED: {r.get('need') or '—'}",
            f"- Discovery n={d.get('n', 0)} after-fee WR {_pct(d.get('wr'))} "
            f"(not KEEP).",
        ]
        if r.get("veto"):
            v = r["veto"]
            c, f = v.get("complement") or {}, v.get("flagged") or {}
            lines.append(
                f"- Veto complement prove n={c.get('n')} WR {_pct(c.get('wr'))}; "
                f"flagged n={f.get('n')} WR {_pct(f.get('wr'))}; "
                f"lifts_baseline={v.get('lifts_baseline')}."
            )
        if r.get("note"):
            lines.append(f"- {r['note']}")
        lines.append("")
    lines += [
        "## Discovery (not KEEP)",
        "",
        "Discovery ranks honesty only. A discovery >55% print is not a call.",
        "",
        "| combo | disc n | disc after-fee WR |",
        "|---|---:|---:|",
    ]
    for r in scored:
        if r.get("verdict") == "NEED":
            continue
        d = r.get("disc") or {}
        lines.append(f"| `{r['key']}` | {d.get('n', 0)} | {_pct(d.get('wr'))} |")
    lines += [
        "",
        "## Walk-forward (discovery folds, not KEEP)",
        "",
        "| combo | fold1 n / WR | fold2 n / WR | fold3 n / WR |",
        "|---|---|---|---|",
    ]
    for r in scored:
        if r.get("verdict") == "NEED":
            continue
        bits = []
        for w in r.get("walk") or []:
            bits.append(f"{w['n']} / {_pct(w['wr'])}")
        while len(bits) < 3:
            bits.append("—")
        lines.append(f"| `{r['key']}` | {bits[0]} | {bits[1]} | {bits[2]} |")
    lines += [
        "",
        "## Explicit verdict",
        "",
    ]
    keep_l = [r for r in scored if r.get("verdict") == "KEEP"]
    fail_l = [r for r in scored if r.get("verdict") == "FAIL"]
    need_l = [r for r in scored if r.get("verdict") == "NEED"]
    lines.append(
        "**KEEP:** " + (", ".join(f"{r['id']} `{r['key']}`" for r in keep_l) or "none")
    )
    lines.append(
        "**FAIL:** " + (", ".join(f"{r['id']} `{r['key']}`" for r in fail_l) or "none")
    )
    lines.append(
        "**NEED-source skipped:** "
        + (", ".join(f"{r['id']} `{r['key']}` ({r.get('need')})" for r in need_l) or "none")
    )
    lines += [
        "",
        "## Explicitly not live",
        "",
        "No combo is wired into `flatten_robust` or cash/paper. A KEEP here is "
        "a research card, not a ship. Do not train ML on flatten-only n=4 days.",
        "",
        "## Source",
        "",
        "`excel_clock_gate.py` / `CLOCK_MAP.md` · `j_winrate.py` "
        f"(`WIN_BAR`, `MIN_FIRES`, `FEE_RT={FEE_RT}`) · "
        "Theme Radar `research/catalogue/FINVIZ_CATALOGUE_MAP.md` · "
        f"Clock-B oppset `{OPPSET_SOURCE}` · "
        "restored `data/factor_mine/panel.json` (PR #277 remine). "
        "Research only.",
        "",
    ]
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path, "w", encoding="utf-8").write("\n".join(lines))
    return path


def slim(r):
    def sl(s):
        if not s:
            return s
        return {
            "n": s.get("n"), "n_pos": s.get("n_pos"), "wr": s.get("wr"),
            "mean_net": s.get("mean_net"), "verdict": s.get("verdict"),
            "why": s.get("why"),
        }
    out = {
        "id": r.get("id"), "key": r.get("key"), "title": r.get("title"),
        "thesis": r.get("thesis"), "side": r.get("side"),
        "role": r.get("role"), "atoms": r.get("atoms"),
        "status": r.get("status"), "have": r.get("have"),
        "need": r.get("need"), "note": r.get("note"),
        "disc": sl(r.get("disc")), "hold": sl(r.get("hold")),
        "verdict": r.get("verdict"), "why": r.get("why"),
        "walk": r.get("walk"),
        "walk_folds_wr_gt_50": r.get("walk_folds_wr_gt_50"),
        "n_walk_folds": r.get("n_walk_folds"),
    }
    if r.get("veto"):
        v = r["veto"]
        out["veto"] = {
            "flagged": sl(v.get("flagged")),
            "complement": sl(v.get("complement")),
            "lifts_baseline": v.get("lifts_baseline"),
            "verdict": v.get("verdict"), "why": v.get("why"),
        }
    return out


def run(panel_path=None, export_dir=None, oppset_path=None,
        out_md=None, out_json=None):
    clocks, leak = leak_check()
    panel = load_panel(panel_path)
    oppset = load_oppset_flagged(oppset_path or OPPSET_PATH)
    rows, aisle_dates, skipped, aisle_stats = name_days_from_panel(
        panel, export_dir, oppset,
    )
    dates = sorted({r["date"] for r in rows})
    cutoff = cutoff_from_dates(dates, hold_frac=HOLD_FRAC, locked=None)
    disc, hold = split_rows(rows, cutoff)
    baseline_hold = score_hits(hold)
    baseline_disc = score_hits(disc)
    scored = score_space(disc, hold, baseline_hold)
    hl = headline_from(scored, cutoff, baseline_hold)
    n_need = sum(1 for r in scored if r.get("verdict") == "NEED")
    n_fail = sum(1 for r in scored if r.get("verdict") == "FAIL")
    payload = {
        "status": "DONE",
        "generated": str(date.today()),
        "leak": leak,
        "live_untouched": "flatten_robust",
        "gate": gate_payload(),
        "fire_bar": {
            "win": f"after-fee H > 0 on prove name-days, strictly > {WIN_BAR}",
            "min_fires": MIN_FIRES,
            "fee_rt": FEE_RT,
            "fee_caveat": FEE_CAVEAT,
            "lift_never_keep": True,
        },
        "cutoff": cutoff,
        "split_kind": "time",
        "hold_frac": HOLD_FRAC,
        "lookback": panel.get("lookback"),
        "panel_to": panel.get("to_date"),
        "panel_n": panel.get("n_rows"),
        "aisle_dates": aisle_dates,
        "n_aisle_dates": len(aisle_dates),
        "skipped_days": skipped,
        "oppset_source": (aisle_stats or {}).get("oppset_source") or OPPSET_SOURCE,
        "n_oppset_flagged": sum(len(v) for v in oppset.values()),
        "n_aisle_panel": (aisle_stats or {}).get("n_panel"),
        "n_aisle_overlap": (aisle_stats or {}).get("n_overlap"),
        "n_aisle_oppset_only": (aisle_stats or {}).get("n_oppset_only"),
        "n_label_finviz_t": (aisle_stats or {}).get("n_label_finviz_t"),
        "n_tickers": len({r["ticker"] for r in rows}),
        "n_rows": len(rows),
        "n_disc": len(disc),
        "n_hold": len(hold),
        "n_keep": hl["n_keep"],
        "n_need": n_need,
        "n_fail": n_fail,
        "headline": hl,
        "baseline_disc": {
            "n": baseline_disc.get("n"), "wr": baseline_disc.get("wr"),
            "mean_net": baseline_disc.get("mean_net"),
        },
        "baseline_hold": {
            "n": baseline_hold.get("n"), "wr": baseline_hold.get("wr"),
            "mean_net": baseline_hold.get("mean_net"),
            "verdict": baseline_hold.get("verdict"),
            "why": baseline_hold.get("why"),
        },
        "kronos_artifacts": find_artifacts(KRONOS_GLOBS),
        "borrow_artifacts": find_artifacts(BORROW_GLOBS),
        "scored": [slim(r) for r in scored],
        "clocks_generated": clocks.get("generated"),
        "feature_keys": sorted(FEATURE_KEYS),
        "forbidden": sorted(FORBIDDEN_FEATURE_FIELDS),
    }
    md = write_board(payload, out_md or BOARD_MD)
    js = out_json or BOARD_JSON
    os.makedirs(os.path.dirname(js) or ".", exist_ok=True)
    json.dump(payload, open(js, "w"), indent=2, default=str)
    print("wrote", md, js, flush=True)
    print("headline", hl["verdict"], hl["text"], flush=True)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", default="")
    ap.add_argument("--exports", default="")
    ap.add_argument("--oppset", default="")
    ap.add_argument("--out-md", default=BOARD_MD)
    ap.add_argument("--out-json", default=BOARD_JSON)
    args = ap.parse_args()
    run(panel_path=args.panel or None,
        export_dir=args.exports or None,
        oppset_path=args.oppset or None,
        out_md=args.out_md, out_json=args.out_json)


if __name__ == "__main__":
    main()
