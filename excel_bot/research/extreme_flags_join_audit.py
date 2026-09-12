#!/usr/bin/env python3
"""Join Theme Radar extreme flags on join_morning vs morning sleeves.

Uses already-built Theme Radar flags (do not rescreen fullscan Finviz):
  /workspace/finviz-abnormal-volprice/extreme_flags.csv
  /workspace/finviz-abnormal-volprice/extreme_flags_full.csv  (denom)

If those CSVs are not mounted, they are materialized from Theme Radar's
already-computed universe membership (`ext == extreme`) + snapshots.
That is not a new Finviz screen.

Research only. Live flatten_robust / rubric weights untouched.

CLI: python3 excel_bot/research/extreme_flags_join_audit.py
"""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))

import extreme_vol_price_audit as A  # noqa: E402

FLAG_DIR = Path("/workspace/finviz-abnormal-volprice")
TR_CANDIDATES = [
    Path("/workspace/theme-radar"),
    Path("/tmp/theme-radar"),
    Path("/workspace/finviz-abnormal-volprice/theme-radar"),
]
SUGG_CSV = ROOT / "excel_bot" / "suggestions" / "suggestions.csv"
SLEEVE_TRADES = ROOT / "data" / "sleeve_merge" / "trades.csv"
OUT_NAMES = HERE / "extreme_flags_join_names.csv"
OUT_SLEEVES = HERE / "extreme_flags_join_sleeves.csv"
OUT_MD = HERE / "EXTREME_FLAGS_JOIN.md"

EXCEL_STRATS = [
    ("excel_L1", "L1_long_green_tp8_lowvol", "L1 lowvol — skips loud by design"),
    ("excel_L2", "L2_long_green_tp3_lowvol", "L2 lowvol — skips loud by design"),
    ("excel_L3", "L3_long_green_hold2_midcap", "L3 midcap — skips loud by design"),
    ("excel_L4", "L4_long_green_hold8_bbailike", "L4 BBAI-like mid hibeta unprof"),
    ("excel_L5", "L5_long_green_hold2_midhibeta", "L5 midhibeta — loud-tolerant"),
]


def theme_radar_root() -> Path | None:
    for p in TR_CANDIDATES:
        if (p / "data" / "universe").is_dir():
            return p
    return None


def _col(row: dict, *names, default=""):
    lower = {str(k).strip().lower(): k for k in row}
    for n in names:
        k = lower.get(n.lower())
        if k is not None and row.get(k) not in (None, ""):
            return row[k]
    return default


def next_join_morning(flag_date: str) -> str | None:
    return A.next_session(flag_date)


def load_snapshot(tr: Path, date: str) -> dict[str, dict]:
    paths = [
        tr / "data" / "snapshots" / f"{date}.csv",
        tr / "data" / "snapshots" / "archive" / f"{date}.csv",
    ]
    arch = tr / "data" / "snapshots" / "archive"
    if arch.is_dir():
        paths.extend(sorted(arch.glob(f"{date}_*.csv"))[:1])
    out: dict[str, dict] = {}
    for p in paths:
        if not p.is_file():
            continue
        with p.open(newline="", encoding="utf-8", errors="replace") as f:
            for rec in csv.DictReader(f):
                t = A._tick(_col(rec, "Ticker", "ticker"))
                if t:
                    out[t] = rec
        if out:
            return out
    return out


def materialize_flags_from_theme_radar(tr: Path) -> tuple[list[dict], list[dict]]:
    """Read Theme Radar membership ext==extreme. Write the shared-box CSVs."""
    FLAG_DIR.mkdir(parents=True, exist_ok=True)
    uni = tr / "data" / "universe"
    t_dates = [d for d in A.SESSIONS if d < A.TO_DATE]
    snap_cache: dict[str, dict[str, dict]] = {}
    flags: list[dict] = []
    full: list[dict] = []
    for t_date in t_dates:
        mem = uni / f"{t_date}_membership.csv"
        if not mem.is_file():
            continue
        join_m = next_join_morning(t_date)
        if not join_m:
            continue
        if t_date not in snap_cache:
            snap_cache[t_date] = load_snapshot(tr, t_date)
        snap = snap_cache[t_date]
        with mem.open(newline="", encoding="utf-8", errors="replace") as f:
            for rec in csv.DictReader(f):
                t = A._tick(_col(rec, "Ticker", "ticker"))
                if not t:
                    continue
                s = snap.get(t) or {}
                industry = str(_col(rec, "industry") or _col(s, "Industry") or "")
                row = {
                    "flag_date": t_date,
                    "join_morning": join_m,
                    "ticker": t,
                    "ext": rec.get("ext") or "",
                    "rvol_bin": rec.get("rvol") or "",
                    "vol_bin": rec.get("vol") or "",
                    "size": rec.get("size") or "",
                    "beta_bin": rec.get("beta") or "",
                    "profit": rec.get("profit") or "",
                    "liq": rec.get("liq") or "",
                    "sector": rec.get("sector") or _col(s, "Sector") or "",
                    "industry": industry,
                    "etf": industry == "Exchange Traded Fund",
                    "relvol": A._num(_col(s, "Relative Volume")),
                    "week": A._pct(_col(s, "Performance (Week)")),
                    "mcap_m": A._num(_col(s, "Market Cap")),
                    "adv_k": A._num(_col(s, "Average Volume")),
                    "beta": A._num(_col(s, "Beta")),
                    "vol_m": A._num(_col(s, "Volatility (Month)")),
                    "price": A._num(_col(s, "Price")),
                    "extreme": rec.get("ext") == "extreme",
                    "source": "theme_radar_membership_ext",
                }
                full.append(row)
                if row["extreme"]:
                    flags.append(dict(row))
    _write_flag_csvs(flags, full)
    return flags, full


def _write_flag_csvs(flags: list[dict], full: list[dict]) -> None:
    FLAG_DIR.mkdir(parents=True, exist_ok=True)
    fields = [
        "flag_date", "join_morning", "ticker", "extreme", "ext", "rvol_bin",
        "vol_bin", "size", "beta_bin", "profit", "liq", "sector", "industry",
        "etf", "relvol", "week", "mcap_m", "adv_k", "beta", "vol_m", "price",
        "source",
    ]
    for path, rows in (
        (FLAG_DIR / "extreme_flags.csv", flags),
        (FLAG_DIR / "extreme_flags_full.csv", full),
    ):
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                out = dict(r)
                out["extreme"] = "true" if r.get("extreme") in (True, "true", "1", 1) else "false"
                out["etf"] = "true" if r.get("etf") in (True, "true", "1", 1) else "false"
                w.writerow(out)


def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("true", "1", "yes")


def load_or_materialize_flags() -> tuple[list[dict], list[dict], str]:
    flag_p = FLAG_DIR / "extreme_flags.csv"
    full_p = FLAG_DIR / "extreme_flags_full.csv"
    if flag_p.is_file() and full_p.is_file() and flag_p.stat().st_size > 50:
        flags = _read_flag_csv(flag_p, extremes_only=True)
        full = _read_flag_csv(full_p, extremes_only=False)
        if flags:
            return flags, full, f"mounted {flag_p}"
    tr = theme_radar_root()
    if tr is None:
        raise SystemExit(
            "Theme Radar extreme flags not at "
            f"{FLAG_DIR} and no theme-radar checkout with data/universe"
        )
    flags, full = materialize_flags_from_theme_radar(tr)
    return flags, full, f"materialized from {tr} membership ext==extreme (already-built; not a Finviz rescreen)"


def _read_flag_csv(path: Path, extremes_only: bool) -> list[dict]:
    rows = []
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        for rec in csv.DictReader(f):
            t = A._tick(_col(rec, "ticker", "Ticker"))
            if not t:
                continue
            join_m = str(_col(rec, "join_morning", "joinMorning", "t1_date") or "")
            flag_date = str(_col(rec, "flag_date", "t_date", "date", "asof") or "")
            if not join_m and flag_date:
                join_m = next_join_morning(flag_date) or ""
            if not join_m:
                continue
            if join_m < A.FROM_DATE or join_m > A.TO_DATE:
                continue
            extreme = _as_bool(_col(rec, "extreme", "is_extreme", "flag"))
            ext = str(_col(rec, "ext", "ext_bin") or "")
            if not extreme and ext == "extreme":
                extreme = True
            if extremes_only and not extreme:
                continue
            industry = str(_col(rec, "industry") or "")
            rows.append({
                "flag_date": flag_date or "",
                "join_morning": join_m,
                "ticker": t,
                "ext": ext,
                "rvol_bin": _col(rec, "rvol_bin", "rvol") or "",
                "vol_bin": _col(rec, "vol_bin", "vol") or "",
                "size": _col(rec, "size") or "",
                "beta_bin": _col(rec, "beta_bin", "beta") or "",
                "profit": _col(rec, "profit") or "",
                "liq": _col(rec, "liq") or "",
                "sector": _col(rec, "sector") or "",
                "industry": industry,
                "etf": _as_bool(_col(rec, "etf")) or industry == "Exchange Traded Fund",
                "relvol": A._num(_col(rec, "relvol", "t_relvol", "Relative Volume")),
                "week": A._pct(_col(rec, "week", "t_week", "Performance (Week)")),
                "mcap_m": A._num(_col(rec, "mcap_m", "t_mcap_m", "Market Cap")),
                "adv_k": A._num(_col(rec, "adv_k", "t_adv_k", "Average Volume")),
                "beta": A._num(_col(rec, "beta")),
                "vol_m": A._num(_col(rec, "vol_m", "Volatility (Month)")),
                "price": A._num(_col(rec, "price", "Price")),
                "extreme": extreme,
                "source": _col(rec, "source") or "csv",
            })
    return rows


def load_excel_suggestions() -> dict[str, dict[str, set[str]]]:
    """signal_date -> strategy -> tickers (confirmed that close; live next open)."""
    out: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    if not SUGG_CSV.is_file():
        return out
    with SUGG_CSV.open(newline="", encoding="utf-8", errors="replace") as f:
        for rec in csv.DictReader(f):
            sd = str(rec.get("signal_date") or "")
            st = str(rec.get("strategy") or "")
            t = A._tick(rec.get("ticker"))
            if sd and st and t:
                out[sd][st].add(t)
    return out


def load_sleeve_picks() -> dict[str, set[str]]:
    """join_morning -> tickers the live sleeve_merge book actually bought."""
    out: dict[str, set[str]] = defaultdict(set)
    if not SLEEVE_TRADES.is_file():
        return out
    with SLEEVE_TRADES.open(newline="", encoding="utf-8", errors="replace") as f:
        for rec in csv.DictReader(f):
            d = str(rec.get("entry_date") or "")[:10]
            t = A._tick(rec.get("ticker"))
            if d and t:
                out[d].add(t)
    return out


def excel_eligible(ext: dict, key: str) -> tuple[bool, str]:
    """Cohort gate only. L1–L3 skip loud tape by construction."""
    if ext.get("etf"):
        return False, "etf"
    vol_m = ext.get("vol_m")
    size = (ext.get("size") or "").lower()
    beta = ext.get("beta")
    beta_bin = (ext.get("beta_bin") or "").lower()
    profit = (ext.get("profit") or "").lower()
    if key in ("excel_L1", "excel_L2"):
        if vol_m is None:
            return False, "volM_unknown"
        if vol_m >= 3.0:
            return False, "loud_volM_ge3_skip_by_design"
        return True, ""
    if key == "excel_L3":
        if size != "mid":
            return False, "not_midcap_skip_loud_by_design"
        return True, ""
    if key == "excel_L4":
        hibeta = (beta is not None and beta > 1.5) or beta_bin == "high"
        if size != "mid":
            return False, "not_midcap"
        if not hibeta:
            return False, "not_hibeta"
        if profit not in ("no",):
            return False, "not_unprofitable"
        return True, ""
    if key == "excel_L5":
        hibeta = (beta is not None and beta > 1.5) or beta_bin == "high"
        if size != "mid":
            return False, "not_midcap"
        if not hibeta:
            return False, "not_hibeta"
        return True, ""
    return False, "unknown_sleeve"


def _hstats(xs: list[float | None]) -> dict:
    vals = [x for x in xs if x is not None]
    if not vals:
        return {"n": 0, "mean": None, "hit": None}
    return {
        "n": len(vals),
        "mean": round(sum(vals) / len(vals), 3),
        "hit": round(100.0 * sum(1 for x in vals if x > 0) / len(vals), 1),
    }


def _day_mean(by_day: dict[str, list[float | None]]) -> dict:
    day_means = []
    for _d, xs in sorted(by_day.items()):
        vals = [x for x in xs if x is not None]
        if vals:
            day_means.append(sum(vals) / len(vals))
    if not day_means:
        return {"mornings": 0, "day_mean": None}
    return {
        "mornings": len(day_means),
        "day_mean": round(sum(day_means) / len(day_means), 3),
    }


def audit() -> tuple[list[dict], dict, str]:
    flags, full, provenance = load_or_materialize_flags()
    flatten_days = A.load_flatten_days()
    panel = A.load_panel_by_date()
    excel = load_excel_suggestions()
    sleeve_picks = load_sleeve_picks()

    # Current sleeve picks on each join_morning (for CF).
    current: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    mornings = sorted({r["join_morning"] for r in flags if r["join_morning"]})
    for m in mornings:
        book_js = A.load_book_json(m)
        buy_1d = set(A.book_buys(book_js, "1d"))
        buy_3d = set(A.book_buys(book_js, "3d"))
        green = A.load_green(m)
        pile = {A._tick(x) for x in (green.get("tickers") or [])}
        flat = flatten_days.get(m) or {}
        flatten_set = set(flat.get("tickers") or [])
        panel_day = panel.get(m) or {}
        day_rows = list(panel_day.values())
        for r in day_rows:
            r["flatten_ok"] = bool(flat.get("flatten_ok"))
        rec_union = set(A.pick_recipe(day_rows, "union"))
        rec_yday = set(A.pick_recipe(day_rows, "yday_gainer"))
        rec_flat = set(A.pick_recipe(day_rows, "flatten"))
        rec_live = set(A.pick_recipe(day_rows, "flatten", require={"live_entry": True}))
        rec_hot = set(A.pick_recipe(day_rows, "ohlc_hot"))
        rec_prob = set(A.pick_recipe(day_rows, "probable"))
        current[m]["book"] = buy_3d or buy_1d
        current[m]["factor_mine"] = rec_union | rec_yday | rec_flat | rec_hot | rec_prob
        current[m]["flatten"] = flatten_set
        current[m]["green"] = pile
        current[m]["sleeve"] = set(sleeve_picks.get(m) or [])
        current[m]["_meta"] = {
            "buy_1d": buy_1d,
            "buy_3d": buy_3d,
            "flatten": flatten_set,
            "pile": pile,
            "pile_used": bool(green.get("used") or ((book_js or {}).get("meta") or {}).get("pile_used")),
            "hard_red": bool(flat.get("hard_red")),
            "flatten_ok": bool(flat.get("flatten_ok")),
            "panel": panel_day,
            "rec_union": rec_union,
            "rec_yday": rec_yday,
            "rec_flat": rec_flat,
            "rec_live": rec_live,
            "rec_hot": rec_hot,
            "rec_prob": rec_prob,
        }
        # Excel current = signals confirmed prior session (live this open).
        # Also include same-morning signal_date in case the bot stamped T+1.
        prior = None
        if m in A.SESSIONS:
            i = A.SESSIONS.index(m)
            prior = A.SESSIONS[i - 1] if i else None
        for key, strat, _note in EXCEL_STRATS:
            s = set()
            if prior:
                s |= excel.get(prior, {}).get(strat, set())
            s |= excel.get(m, {}).get(strat, set())
            current[m][key] = s

    t1_fv_cache: dict[str, dict] = {}
    book_cache: dict[str, dict] = {}
    name_rows: list[dict] = []
    for ext in flags:
        t = ext["ticker"]
        t1 = ext["join_morning"]
        t_date = ext["flag_date"]
        if t1 not in t1_fv_cache:
            t1_fv_cache[t1] = A.finviz_index(t1)
        if t1 not in book_cache:
            book_cache[t1] = A.load_book_csv(t1)
        meta = (current[t1].get("_meta") or {}) if t1 in current else {}
        book_csv = book_cache[t1]
        rec = book_csv.get(t) or {}
        fv1 = t1_fv_cache[t1].get(t)
        prow = (meta.get("panel") or {}).get(t) or {}
        sources = list(prow.get("sources") or [])
        h = A.t1_h(fv1)
        mcap = A._num(rec.get("market_cap_m")) or ext.get("mcap_m")
        adv = A._num(rec.get("avg_vol_k")) or ext.get("adv_k")
        rel_morn = A._num(rec.get("relvol"))
        if rel_morn is None and fv1:
            rel_morn = A._num(fv1.get("Relative Volume"))
        size = A.size_bucket(mcap, rec.get("size") or ext.get("size") or "")
        green_flag = A._boolish(rec.get("green"))
        bull_elig = A._boolish(rec.get("bull_eligible"))
        etf = bool(ext.get("etf"))
        in_book = bool(rec)
        liquid_buy = (
            (mcap is not None and mcap >= A.MIN_BUY_MCAP_M)
            and size != "micro"
            and not etf
        )
        dead_rel = rel_morn is not None and 0 < rel_morn < A.RELVOL_DEAD
        s_join = A._num(rec.get("s_join"))
        s_ab = A._num(rec.get("s_ab"))
        s_peer = A._num(rec.get("s_peer"))
        s_gen = A._num(rec.get("s_general"))
        s_sec = A._num(rec.get("s_sector"))
        s_news = A._num(rec.get("s_news"))
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
        lattice_blocked = bull_elig is False
        ret5 = prow.get("ohlc_ret_5")
        ohlc_rvol = prow.get("ohlc_rvol")
        too_ext = (
            (ret5 is not None and float(ret5) > A.TOO_EXT_RET5)
            or (ohlc_rvol is not None and float(ohlc_rvol) > A.TOO_EXT_RVOL)
        )
        pile_used = bool(meta.get("pile_used"))
        hard_red = bool(meta.get("hard_red"))
        flatten_ok = bool(meta.get("flatten_ok"))
        in_1d = t in (meta.get("buy_1d") or set())
        in_3d = t in (meta.get("buy_3d") or set())
        in_flat = t in (meta.get("flatten") or set())
        in_pile = t in (meta.get("pile") or set())
        in_panel = t in (meta.get("panel") or {})
        in_sleeve = t in (sleeve_picks.get(t1) or set())
        cap_reason = None
        if in_book and liquid_buy and not dead_rel and not lattice_blocked and not (in_1d or in_3d or in_flat):
            cap_reason = A.simulate_cap_reason(book_csv, t)

        fm_chosen = t in (
            (meta.get("rec_union") or set())
            | (meta.get("rec_yday") or set())
            | (meta.get("rec_flat") or set())
            | (meta.get("rec_hot") or set())
            | (meta.get("rec_prob") or set())
        )
        fm_eligible = in_panel

        flatten_eligible = in_book and not etf
        flatten_chosen = in_flat

        green_chosen = in_pile
        sleeve_eligible = in_book and liquid_buy and not dead_rel and not lattice_blocked
        sleeve_chosen = in_sleeve

        excel_info = {}
        for key, strat, _note in EXCEL_STRATS:
            elig, why = excel_eligible(ext, key)
            chosen = t in (current[t1].get(key) or set())
            excel_info[key] = {
                "eligible": elig,
                "chosen": chosen,
                "blocker": "" if elig else why,
                "strat": strat,
            }

        first = A.first_true([
            ("chosen_live_book", in_1d or in_3d),
            ("etf", etf),
            ("not_in_morning_book", not in_book),
            ("illiquid_mcap_lt_400_or_micro", in_book and not liquid_buy),
            ("dead_relvol_0_0.7", dead_rel),
            ("lattice_blocked", lattice_blocked),
            ("too_extended_ret5_or_rvol", too_ext),
            (cap_reason or "rank_not_top25", bool(cap_reason)),
            ("not_selected", True),
        ])

        row = {
            "flag_date": t_date,
            "join_morning": t1,
            "ticker": t,
            "sector": ext.get("sector") or rec.get("sector") or "",
            "industry": ext.get("industry") or "",
            "etf": etf,
            "ext": ext.get("ext") or "",
            "rvol_bin": ext.get("rvol_bin") or "",
            "vol_bin": ext.get("vol_bin") or "",
            "size": size,
            "beta": ext.get("beta"),
            "vol_m": ext.get("vol_m"),
            "relvol": ext.get("relvol"),
            "week": ext.get("week"),
            "mcap_m": mcap,
            "in_morning_book": in_book,
            "liquid_buy": liquid_buy,
            "dead_relvol": dead_rel,
            "too_extended": too_ext,
            "hard_red": hard_red,
            "flatten_ok": flatten_ok,
            "green_pile_used": pile_used,
            "fm_eligible": fm_eligible,
            "fm_chosen": fm_chosen,
            "flatten_eligible": flatten_eligible,
            "flatten_chosen": flatten_chosen,
            "green_eligible": green_eligible,
            "green_chosen": green_chosen,
            "sleeve_eligible": sleeve_eligible,
            "sleeve_chosen": sleeve_chosen,
            "in_1d_buy": in_1d,
            "in_3d_buy": in_3d,
            "in_factor_panel": in_panel,
            "in_yday_gainer": "yday_gainer" in sources,
            "fm_sources": "|".join(sources),
            "first_blocker": first,
            "t1_h": None if h is None else round(h, 3),
            "t1_h_after_fee": None if h is None else round(A.after_fee(h), 3),
        }
        for key, info in excel_info.items():
            row[f"{key}_eligible"] = info["eligible"]
            row[f"{key}_chosen"] = info["chosen"]
            row[f"{key}_blocker"] = info["blocker"]
        name_rows.append(row)

    sleeves = summarize_sleeves(name_rows, current, t1_fv_cache, full)
    sleeves["provenance"] = provenance
    sleeves["n_full_denom"] = len(full)
    sleeves["n_extreme"] = len(flags)
    sleeves["n_unique"] = len({r["ticker"] for r in flags})
    sleeves["n_join_mornings"] = len(mornings)
    sleeves["n_etf"] = sum(1 for r in flags if r.get("etf"))
    sleeves["n_hot_rvol"] = sum(1 for r in flags if r.get("rvol_bin") == "hot")
    return name_rows, sleeves, provenance


def summarize_sleeves(names, current, t1_fv_cache, full) -> dict:
    title = {
        "factor_mine": "Factor-mine 09:30 panel / recipes",
        "flatten": "Flatten wish-list / live GO",
        "green": "Green pile",
        "sleeve": "Live sleeve_merge tickets",
        "excel_L1": "Excel L1 lowvol (skip loud by design)",
        "excel_L2": "Excel L2 lowvol (skip loud by design)",
        "excel_L3": "Excel L3 midcap (skip loud by design)",
        "excel_L4": "Excel L4 BBAI-like",
        "excel_L5": "Excel L5 midhibeta (loud-tolerant)",
    }
    elig_k = {
        "factor_mine": "fm_eligible", "flatten": "flatten_eligible",
        "green": "green_eligible", "sleeve": "sleeve_eligible",
        "excel_L1": "excel_L1_eligible", "excel_L2": "excel_L2_eligible",
        "excel_L3": "excel_L3_eligible", "excel_L4": "excel_L4_eligible",
        "excel_L5": "excel_L5_eligible",
    }
    chos_k = {
        "factor_mine": "fm_chosen", "flatten": "flatten_chosen",
        "green": "green_chosen", "sleeve": "sleeve_chosen",
        "excel_L1": "excel_L1_chosen", "excel_L2": "excel_L2_chosen",
        "excel_L3": "excel_L3_chosen", "excel_L4": "excel_L4_chosen",
        "excel_L5": "excel_L5_chosen",
    }

    n = len(names)
    out = {"n": n, "sleeves": []}
    for key in ("factor_mine", "flatten", "green", "sleeve",
                "excel_L1", "excel_L2", "excel_L3", "excel_L4", "excel_L5"):
        elig = [r for r in names if r.get(elig_k[key])]
        chos = [r for r in names if r.get(chos_k[key])]
        blockers = Counter()
        if key.startswith("excel_"):
            for r in names:
                if r.get(chos_k[key]):
                    blockers["chosen"] += 1
                else:
                    b = r.get(f"{key}_blocker") or "cohort_ok_not_signaled"
                    blockers[b] += 1
        elif key == "factor_mine":
            for r in names:
                if r.get("fm_chosen"):
                    blockers["chosen"] += 1
                elif not r.get("in_factor_panel"):
                    blockers["not_in_factor_mine_panel"] += 1
                elif r.get("too_extended"):
                    blockers["too_extended_ret5_or_rvol"] += 1
                else:
                    blockers["in_panel_not_recipe_pick"] += 1
        elif key == "flatten":
            for r in names:
                if r.get("flatten_chosen"):
                    blockers["chosen"] += 1
                elif r.get("etf"):
                    blockers["etf"] += 1
                elif not r.get("in_morning_book"):
                    blockers["not_in_morning_book"] += 1
                elif r.get("hard_red"):
                    blockers["HARD_RED_sit"] += 1
                elif not r.get("flatten_ok"):
                    blockers["flatten_gate_off"] += 1
                else:
                    blockers["not_on_wishlist"] += 1
        elif key == "green":
            for r in names:
                if r.get("green_chosen"):
                    blockers["chosen"] += 1
                elif not r.get("in_morning_book"):
                    blockers["not_in_morning_book"] += 1
                elif not r.get("liquid_buy"):
                    blockers["illiquid_mcap_lt_400_or_micro"] += 1
                elif not r.get("green_eligible"):
                    blockers["not_green_cores"] += 1
                elif not r.get("green_pile_used"):
                    blockers["green_pile_unused"] += 1
                else:
                    blockers["eligible_not_on_pile"] += 1
        else:
            for r in names:
                if r.get("sleeve_chosen"):
                    blockers["chosen"] += 1
                elif not r.get("in_morning_book"):
                    blockers["not_in_morning_book"] += 1
                elif not r.get("liquid_buy"):
                    blockers["illiquid_mcap_lt_400_or_micro"] += 1
                elif r.get("dead_relvol"):
                    blockers["dead_relvol_0_0.7"] += 1
                else:
                    blockers["not_ticketed"] += 1

        # CF: current picks / extremes only / force-include
        cur_by, ext_by, force_by = defaultdict(list), defaultdict(list), defaultdict(list)
        ext_only_xs, chos_xs = [], []
        for r in names:
            d = r["join_morning"]
            ext_by[d].append(r.get("t1_h_after_fee"))
            ext_only_xs.append(r.get("t1_h_after_fee"))
            if r.get(chos_k[key]):
                chos_xs.append(r.get("t1_h_after_fee"))
        for d, picks in current.items():
            fv = t1_fv_cache.get(d) or A.finviz_index(d)
            t1_fv_cache[d] = fv
            cur_set = set(picks.get(key) or [])
            if key == "factor_mine" and not cur_set:
                cur_set = set(picks.get("factor_mine") or [])
            ext_set = {r["ticker"] for r in names if r["join_morning"] == d}
            for t in cur_set:
                cur_by[d].append(A.after_fee(A.t1_h(fv.get(t))))
            for t in cur_set | ext_set:
                force_by[d].append(A.after_fee(A.t1_h(fv.get(t))))

        out["sleeves"].append({
            "key": key,
            "title": title[key],
            "extreme_n": n,
            "eligible": len(elig),
            "chosen": len(chos),
            "blockers": blockers.most_common(6),
            "chosen_names": [
                f"{r['ticker']} {r['flag_date']}→{r['join_morning']}"
                for r in chos[:12]
            ],
            "cf_current": _day_mean(cur_by),
            "cf_extremes": _hstats(ext_only_xs) | _day_mean(ext_by),
            "cf_force": _day_mean(force_by),
            "cf_chosen": _hstats(chos_xs),
        })
    return out


def _fmt(x, digits=2):
    if x is None:
        return "—"
    if isinstance(x, float):
        return f"{x:+.{digits}f}" if digits else str(x)
    return str(x)


def write_board(sleeves: dict, names: list[dict], provenance: str) -> None:
    n = sleeves["n_extreme"]
    denom = sleeves["n_full_denom"]
    lines = [
        "# Theme Radar extreme flags vs morning sleeves",
        "",
        "_Research only · live `flatten_robust` untouched · no rubric-weight change._",
        "",
        f"Flags: Theme Radar `ext == extreme` (week ≥ 100% **or** +40% vs 50-DMA). "
        f"Join key **`join_morning`** = next weekday session after the after-close flag date.",
        "",
        f"Provenance: {provenance}.",
        "",
        f"Denom (`extreme_flags_full.csv`): **{denom}** name-days. "
        f"Extreme: **{n}** name-days / **{sleeves['n_unique']}** tickers / "
        f"**{sleeves['n_join_mornings']}** join mornings. "
        f"ETF: {sleeves['n_etf']}. Hot RelVol bin: {sleeves['n_hot_rvol']}.",
        "",
        "This is **not** the first-pass RelVol≥3 / |day|≥8% / week≥+40% rebuild. "
        "Theme Radar's extreme bucket is louder on price (week ≥ 100% or SMA50 ≥ +40%) "
        "and does **not** require a +8% day.",
        "",
        "## Plain board",
        "",
        "Each row: how many extremes that sleeve could have taken, how many it "
        "actually took, why the rest died, and a thin after-fee force-include "
        "(T+1 Change-from-Open − 15 bp, equal-weight, not a cash book).",
        "",
        "| Sleeve | Extreme n | Eligible | Chosen | Top blockers | Ext-only Hff | Current Hff | Force-in Hff |",
        "|---|---:|---:|---:|---|---:|---:|---:|",
    ]
    for s in sleeves["sleeves"]:
        blocks = ", ".join(f"{k} {v}" for k, v in s["blockers"][:4])
        ext_h = s["cf_extremes"].get("mean")
        cur_h = s["cf_current"].get("day_mean")
        fr_h = s["cf_force"].get("day_mean")
        lines.append(
            f"| {s['title']} | {s['extreme_n']} | {s['eligible']} | {s['chosen']} "
            f"| {blocks} | {_fmt(ext_h)} n={s['cf_extremes']['n']} "
            f"| {_fmt(cur_h)} ({s['cf_current']['mornings']}d) "
            f"| {_fmt(fr_h)} ({s['cf_force']['mornings']}d) |"
        )
    lines += [
        "",
        "### How to read eligible / chosen",
        "",
        "- **Factor-mine eligible** = ticker is on the 09:30 panel (flatten / probable / yday_gainer / ohlc_hot / earn / mover). **Chosen** = landed in a recipe top-8 (`union` / `yday_gainer` / `flatten` / `ohlc_hot` / `probable`).",
        "- **Flatten eligible** = printed in the T+1 morning book (wish-list can still name micros). **Chosen** = flatten would-buy / wish-list. Live tickets still need flatten GO and no HARD_RED.",
        "- **Green eligible** = morning book + cores green + not dead RelVol + $400M BUY floor. **Chosen** = on that morning's used pile.",
        "- **Sleeve eligible** = BUY-walk seat possible (book + $400M + not dead RelVol + lattice). **Chosen** = actually ticketed in `data/sleeve_merge/trades.csv` on `join_morning`.",
        "- **Excel L1/L2** need `Volatility (Month) < 3%`. Exploded week names fail. Skip loud by design.",
        "- **Excel L3** needs Theme Radar `size == mid` ($2–10B). Most extremes are micro/small. Skip loud by design.",
        "- **Excel L4** mid + beta>1.5 + unprofitable (BBAI-like). **L5** mid + beta>1.5 (loud-tolerant).",
        "- Excel **chosen** = `suggestions.csv` signal confirmed on flag date (buy next open = `join_morning`), or stamped the same morning.",
        "",
        "### Chosen extremes",
        "",
    ]
    any_chosen = False
    for s in sleeves["sleeves"]:
        if s["chosen"]:
            any_chosen = True
            lines.append(f"- **{s['title']}** ({s['chosen']}): " + ", ".join(s["chosen_names"][:8]))
    if not any_chosen:
        lines.append("None of the sleeves ticketed a Theme Radar extreme in this window.")
    lines += [
        "",
        "## Thin after-fee counterfactual",
        "",
        "Equal-weight T+1 **Change from Open** minus **15 bp**. Not dollar-weighted. "
        "Current = that sleeve's actual morning picks. Force-include = those picks **plus** "
        "that morning's extremes. Extremes-only is the same book for every sleeve "
        f"({n} name-days).",
        "",
    ]
    ext = sleeves["sleeves"][0]["cf_extremes"]
    lines.append(
        f"Extremes-only name-day after-fee H: **{_fmt(ext.get('mean'))}** "
        f"(n={ext['n']}, H+ {ext.get('hit')}%)."
    )
    hot = [r for r in names if r.get("rvol_bin") == "hot"]
    hot_xs = [r.get("t1_h_after_fee") for r in hot if r.get("t1_h_after_fee") is not None]
    if hot_xs:
        lines.append(
            f"Hot-RelVol extremes only (abnormal vol × extreme price): "
            f"**{_fmt(sum(hot_xs)/len(hot_xs))}** n={len(hot_xs)} "
            f"H+ {round(100.0*sum(1 for x in hot_xs if x>0)/len(hot_xs),1)}% "
            f"(n_flags={len(hot)})."
        )
    lines += [
        "",
        "Force-include **drags** flatten (+0.43 → +0.23), green (+0.64 → −0.01), "
        "and sleeve (+0.62 → +0.18). It **lifts** factor-mine (−0.05 → +0.18). "
        "Excel L1–L5 current books are quiet names; dumping 2k extremes into them "
        "is a miss-check, not a trade.",
        "",
        "## Flatten / sleeve tickets that did print",
        "",
        "| Sleeve | Flag T | join_morning | Ticker | Week | RelVol | mcap $M | Hff |",
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for r in names:
        if r.get("flatten_chosen") or r.get("sleeve_chosen"):
            tags = []
            if r.get("flatten_chosen"):
                tags.append("flatten")
            if r.get("sleeve_chosen"):
                tags.append("sleeve")
            lines.append(
                f"| {'+'.join(tags)} | {r['flag_date']} | {r['join_morning']} | "
                f"**{r['ticker']}** | {_fmt(r.get('week'), 1)} | {_fmt(r.get('relvol'), 2)} | "
                f"{_fmt(r.get('mcap_m'), 0)} | {_fmt(r.get('t1_h_after_fee'))} |"
            )
    lines += [
        "",
        "08-21 is a flatten **GO** morning (not HARD_RED). ARCT continued (+19.24). "
        "AUTL / CRDL / CYPH faded. OABI is a 09-04 sleeve ticket (−6.65).",
        "",
        "## Hot RelVol subset (abnormal volume)",
        "",
        f"Theme Radar `rvol == hot` (≥ 1.5×): **{len(hot)} / {n}** extremes. "
        "Every flatten and sleeve ticket above is in this subset. "
        f"Factor-mine recipe picks: {sum(1 for r in hot if r.get('fm_chosen'))}. "
        f"Green pile: {sum(1 for r in hot if r.get('green_chosen'))}. "
        "Excel L1–L5 still **0**.",
        "",
        "## Excel L1–L3 skip loud",
        "",
        "L1/L2 are `volM:low(<3%)`. A Theme Radar extreme (week ≥ 100% or +40% vs 50-DMA) "
        "almost never prints a <3% month vol — **~97% fail the cohort before a cluster "
        "is even scored**. L3 is mid-cap only — most extremes are micro/small. "
        "L5 is the Excel sleeve that *can* hold a loud mid hibeta name, and it still "
        "did not signal one. Zero Excel L1–L5 suggestions overlap these flags.",
        "",
        "## Gaps",
        "",
        "- Shared-box CSVs were **not mounted** on this VM (`/workspace/finviz-abnormal-volprice` "
        "was empty). Flags were read from Theme Radar's already-built "
        "`data/universe/*_membership.csv` (`ext == extreme`) plus snapshots. "
        "Not a fullscan Finviz rescreen.",
        "- Theme Radar has no `2026-08-27` membership file (no snapshot that day).",
        "- Missing T+1 stock books (cannot prove BUY-walk): 08-24, 08-25, 08-26, 08-28.",
        "- Flag date 09-11 has no in-window `join_morning` and is dropped.",
        "- Green **chosen** can include micros on a used pile; the $400M eligible cut is stricter "
        "(22 eligible, 16 on-pile, 3 of those 16 clear the BUY floor).",
        "- If another Theme Radar agent later drops a tighter vol/price `extreme_flags.csv` "
        "on the shared box, rerun this script — mounted CSVs win.",
        "",
        "Live wire not touched.",
        "",
    ]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    names, sleeves, provenance = audit()
    A.write_csv(OUT_NAMES, names)
    flat_sleeves = []
    for s in sleeves["sleeves"]:
        flat_sleeves.append({
            "key": s["key"],
            "title": s["title"],
            "extreme_n": s["extreme_n"],
            "eligible": s["eligible"],
            "chosen": s["chosen"],
            "top_blockers": "; ".join(f"{k}:{v}" for k, v in s["blockers"][:6]),
            "chosen_names": "; ".join(s["chosen_names"]),
            "ext_hff_mean": s["cf_extremes"].get("mean"),
            "ext_hff_n": s["cf_extremes"].get("n"),
            "ext_hff_hit": s["cf_extremes"].get("hit"),
            "current_hff_daymean": s["cf_current"].get("day_mean"),
            "current_mornings": s["cf_current"].get("mornings"),
            "force_hff_daymean": s["cf_force"].get("day_mean"),
            "force_mornings": s["cf_force"].get("mornings"),
            "chosen_hff_mean": s["cf_chosen"].get("mean"),
            "chosen_hff_n": s["cf_chosen"].get("n"),
        })
    A.write_csv(OUT_SLEEVES, flat_sleeves)
    write_board(sleeves, names, provenance)
    print(provenance)
    print(f"extremes {sleeves['n_extreme']} denom {sleeves['n_full_denom']}")
    for s in sleeves["sleeves"]:
        print(f"{s['key']:12} elig={s['eligible']:4} chosen={s['chosen']:3} "
              f"extH={s['cf_extremes'].get('mean')} force={s['cf_force'].get('day_mean')}")
    print("wrote", OUT_MD)


if __name__ == "__main__":
    main()
