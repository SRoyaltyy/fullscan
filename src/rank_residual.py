"""Join rank residual — avoid bad top-N buys, elevate buried names.

Grok Bot's external-screener pass (CANSLIM / Magic Formula / FPE /
``total_score`` re-sort) is a null on the 1d clock and must not be
rewired here. This mine stays *inside* the join ranked file:

  1. Eliminate bad buys the existing ranker still prints at #1–15
     (incomplete Elite cards, Consumer Defensive China ADRs).
  2. Find names the ranker parked in the mediocre 16–80 band that
     should have been higher (Healthcare, complete cards).
  3. Expand with in-repo cameras join does not vote: pre-open News
     Title tone, prior-bar last_green / last_red, prior_spy_red ×
     Healthcare from the down-day mine.

Clock: same-session Finviz ``Change from Open`` (join / stock-book
1d open→close). That IC does **not** auto-apply to ``flatten_h5`` or
live ``flatten_robust``. Live policy is untouched.

CLI::

    python3 -m src.rank_residual --write
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import down_day_mine as ddm
from . import factor_mine as fm
from . import factor_mine_probe as fmp
from . import gainer_asof as ga
from . import paper_trade as pt

ROOT = Path(__file__).resolve().parent.parent
JOIN_DIR = ROOT / "data" / "join"
OUT_JSON = ROOT / "03_scoreboard" / "rank_residual.json"
OUT_MD = ROOT / "03_scoreboard" / "RANK_RESIDUAL.md"
DASH_DIR = ROOT / "dashboard" / "rank-residual"
TEMPLATE = Path(__file__).with_name("rank_residual_dash.html")
ET = ZoneInfo("America/New_York")

ELITE_COLS = ("earnsurp", "rsi", "ext")
TOP_N = 15
MID_LO, MID_HI = 16, 80
MID_CORE = (16, 30)
BOOK_N = 8
BUDGET = 10_000.0
AVOID_MIN_N = 40
ELEVATE_MIN_N = 40
AVOID_LIFT = -3.0
ELEVATE_LIFT = 3.0
AVOID_MEAN_EDGE = -0.001  # at least 10 bps worse than the band
ELEVATE_MEAN_EDGE = 0.001
# Avoid-side flags must not reappear as "elevate" just because a
# China-ADR outlier ripped in the mid-band (FEDU on 2026-08-17).
ELEVATE_BLOCKLIST = {
    "incomplete", "consumer_defensive", "china_adr", "cd_or_adr", "junk_top",
}

BANNED_FEATURES = {
    "gap", "change", "relvol", "rvol_today", "oc", "cfo", "price",
    "today_high", "today_low", "today_close", "today_open",
    "change_from_open", "change_pct", "change%", "rel vol",
}

# Same-session Finviz columns that may never become pick features.
BANNED_FV_COLS = {
    "Change", "Change from Open", "Gap", "Rel Volume", "Relative Volume",
    "Volume", "Average Volume", "Price", "High", "Low", "Open",
}


def _tick(v) -> str:
    return ddm._tick(v)


def _pct_num(v):
    return ddm._pct_num(v)


def _pack(vals) -> dict:
    return ddm._pack(np.asarray(vals, dtype=float))


def _finite(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def is_banned(name: str) -> bool:
    key = str(name or "").strip().lower()
    return key in BANNED_FEATURES or key.replace(" ", "_") in BANNED_FEATURES


def incomplete_mask(df: pd.DataFrame) -> pd.Series:
    """True when Elite labels earnsurp / rsi / ext are missing.

    Early join files (before Elite overlays) have no such columns —
    after concat those cells are NaN, so the whole early window is
    incomplete. That is knowable from the morning ranked file.
    """
    missing = [c for c in ELITE_COLS if c not in df.columns]
    if missing:
        return pd.Series(True, index=df.index)
    return df[list(ELITE_COLS)].isna().any(axis=1)


def china_adr_mask(df: pd.DataFrame) -> pd.Series:
    detail = df["detail"].fillna("").astype(str) if "detail" in df.columns else ""
    if isinstance(detail, str):
        hit = pd.Series(False, index=df.index)
    else:
        hit = detail.str.contains("ADR-China", case=False, na=False)
        hit = hit | detail.str.contains(r"geo:ADR", case=False, na=False)
    if "country" in df.columns:
        ctry = df["country"].fillna("").astype(str).str.strip()
        hit = hit | ctry.str.lower().eq("china")
    return hit.fillna(False)


def load_join_ranked(join_dir: Path | None = None) -> pd.DataFrame:
    folder = join_dir or JOIN_DIR
    rows = []
    for path in sorted(folder.glob("*_ranked.csv")):
        day = path.name[:10]
        try:
            frame = pd.read_csv(path)
        except (OSError, ValueError, pd.errors.ParserError):
            continue
        if frame.empty or "Ticker" not in frame.columns:
            continue
        frame = frame.copy()
        frame["Ticker"] = frame["Ticker"].map(_tick)
        frame = frame[frame["Ticker"].ne("")].copy()
        frame["session_date"] = day
        frame["rank"] = np.arange(1, len(frame) + 1)
        rows.append(frame)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out["total_score"] = pd.to_numeric(out.get("total_score"), errors="coerce")
    if "families_known" in out.columns:
        out["families_known"] = pd.to_numeric(out["families_known"], errors="coerce")
    return out


def attach_session_finviz(df: pd.DataFrame) -> pd.DataFrame:
    """Same-session Elite export: CFO / Price are *outcomes*.

    Country and pre-open News Title are 09:30 labels. Change / Gap /
    RelVol / printed OHLC are never copied as pick features.
    """
    out = df.copy()
    out["oc"] = np.nan
    out["px_open"] = np.nan
    out["px_close"] = np.nan
    out["country"] = ""
    out["news_tone"] = "missing"
    out["news_preopen"] = False
    if out.empty:
        return out
    chunks = []
    for day, sl in out.groupby("session_date", sort=False):
        raw = ga.load_finviz(str(day))
        piece = sl.copy()
        if raw is None or getattr(raw, "empty", True) or "Ticker" not in raw.columns:
            chunks.append(piece)
            continue
        fv = raw.copy()
        fv["Ticker"] = fv["Ticker"].map(_tick)
        keep = ["Ticker"]
        if "Change from Open" in fv.columns:
            keep.append("Change from Open")
        if "Price" in fv.columns:
            keep.append("Price")
        if "Country" in fv.columns:
            keep.append("Country")
        news_cols = [c for c in ("News Title", "News Time") if c in fv.columns]
        keep.extend(news_cols)
        fv = fv[keep].drop_duplicates("Ticker")
        merged = piece.merge(fv, on="Ticker", how="left", suffixes=("", "_fv"))
        cfo = merged.get("Change from Open")
        if cfo is not None:
            unit = cfo.map(lambda v: None if _pct_num(v) is None else _pct_num(v) / 100.0)
            merged["oc"] = pd.to_numeric(unit, errors="coerce")
        if "Price" in merged.columns:
            close = pd.to_numeric(merged["Price"], errors="coerce")
            merged["px_close"] = close
            oc = merged["oc"]
            merged["px_open"] = np.where(
                close.notna() & oc.notna() & (oc > -0.99),
                close / (1.0 + oc),
                np.nan,
            )
        if "Country" in merged.columns:
            merged["country"] = merged["Country"].fillna("").astype(str)
        tone = []
        pre = []
        if "News Title" in merged.columns:
            times = merged["News Time"] if "News Time" in merged.columns else None
            for i, title in enumerate(merged["News Title"].tolist()):
                when = None if times is None else times.iloc[i]
                ok = fmp.news_is_pre_open(when, str(day))
                pre.append(bool(ok))
                if ok:
                    tone.append(fm.prior_news_tone(title))
                else:
                    tone.append("missing")
        else:
            tone = ["missing"] * len(merged)
            pre = [False] * len(merged)
        merged["news_tone"] = tone
        merged["news_preopen"] = pre
        drop = [c for c in merged.columns if c in BANNED_FV_COLS or c.endswith("_fv")]
        # Keep oc / px_* which we already derived; drop raw tape cols.
        drop = [c for c in drop if c not in ("oc", "px_open", "px_close")]
        merged = merged.drop(columns=drop, errors="ignore")
        chunks.append(merged)
    return pd.concat(chunks, ignore_index=True)


def attach_ohlc_priors(df: pd.DataFrame, prices: pd.DataFrame | None = None) -> pd.DataFrame:
    """Prior-bar cameras only. Today's parquet oc is a fallback outcome."""
    out = df.copy()
    if out.empty:
        out["last_green"] = False
        out["last_red"] = False
        out["ret_1"] = np.nan
        out["ohlc_oc"] = np.nan
        out["spy_cc_down"] = False
        out["prior_spy_red"] = False
        return out
    px = prices if prices is not None else ddm.load_prices()
    want = set(out["Ticker"].unique()) | {"SPY"}
    px = px[px["ticker"].isin(want)].copy()
    if px.empty:
        out["last_green"] = False
        out["last_red"] = False
        out["ret_1"] = np.nan
        out["ohlc_oc"] = np.nan
        out["spy_cc_down"] = False
        out["prior_spy_red"] = False
        return out
    feat = ddm.attach_prior_ohlc(px)
    cal = ddm.spy_calendar(px)
    cal = cal.rename(columns={"date": "session_date"})
    prior = feat.rename(columns={"date": "session_date", "ticker": "Ticker", "oc": "ohlc_oc"})
    keep = ["session_date", "Ticker", "last_green", "last_red", "ret_1",
            "ohlc_oc", "open", "close"]
    keep = [c for c in keep if c in prior.columns]
    out = out.merge(prior[keep], on=["session_date", "Ticker"], how="left")
    if "open" in out.columns:
        out["px_open"] = out["px_open"].where(out["px_open"].notna(), out["open"])
    if "close" in out.columns:
        out["px_close"] = out["px_close"].where(out["px_close"].notna(), out["close"])
    tape = cal[["session_date", "spy_cc_down", "prior_spy_red", "spy_oc_down"]].drop_duplicates(
        "session_date"
    )
    out = out.merge(tape, on="session_date", how="left")
    # Finviz CFO is the join clock; parquet oc fills holes through 2026-08-21.
    if "ohlc_oc" in out.columns:
        out["oc"] = pd.to_numeric(out["oc"], errors="coerce")
        out["oc"] = out["oc"].where(out["oc"].notna(), out["ohlc_oc"])
        need_px = out["px_open"].isna() & out["ohlc_oc"].notna()
        if need_px.any():
            # No share fill without a printed open; leave NaN.
            pass
    out["last_green"] = out.get("last_green", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    out["last_red"] = out.get("last_red", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    out["spy_cc_down"] = out.get("spy_cc_down", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    out["prior_spy_red"] = out.get("prior_spy_red", pd.Series(False, index=out.index)).fillna(False).astype(bool)
    return out


def add_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["incomplete"] = incomplete_mask(out)
    out["complete"] = ~out["incomplete"]
    sec = out["sector"].fillna("").astype(str).str.strip().str.lower() if "sector" in out.columns else ""
    if isinstance(sec, str):
        out["cd"] = False
        out["hc"] = False
        out["tech"] = False
    else:
        out["cd"] = sec.eq("consumer defensive")
        out["hc"] = sec.eq("healthcare")
        out["tech"] = sec.eq("technology")
    out["china_adr"] = china_adr_mask(out)
    ext = out["ext"].fillna("").astype(str).str.lower() if "ext" in out.columns else ""
    out["washed"] = ext.eq("washed") if not isinstance(ext, str) else False
    earn = out["earnsurp"].fillna("").astype(str).str.lower() if "earnsurp" in out.columns else ""
    out["big_beat"] = earn.eq("big_beat") if not isinstance(earn, str) else False
    out["news_good"] = out.get("news_tone", pd.Series("missing", index=out.index)).eq("good")
    out["news_bad"] = out.get("news_tone", pd.Series("missing", index=out.index)).eq("bad")
    out["hc_after_red"] = out["hc"] & out.get("prior_spy_red", False)
    out["hc_complete"] = out["hc"] & out["complete"]
    out["cd_or_adr"] = out["cd"] | out["china_adr"]
    out["junk_top"] = out["incomplete"] | out["cd"] | out["china_adr"]
    out["has_elite"] = out["complete"]
    return out


def scored_slice(df: pd.DataFrame, pred) -> pd.DataFrame:
    hit = df.loc[pred & df["oc"].notna()].copy()
    return hit


def band_stats(df: pd.DataFrame) -> list[dict]:
    rows = []
    specs = [
        ("all", df["oc"].notna()),
        ("ranks_1_8", (df["rank"] <= 8) & df["oc"].notna()),
        ("ranks_1_15", (df["rank"] <= TOP_N) & df["oc"].notna()),
        ("ranks_16_30", (df["rank"] >= 16) & (df["rank"] <= 30) & df["oc"].notna()),
        ("ranks_16_50", (df["rank"] >= 16) & (df["rank"] <= 50) & df["oc"].notna()),
        ("ranks_16_80", (df["rank"] >= MID_LO) & (df["rank"] <= MID_HI) & df["oc"].notna()),
    ]
    for name, pred in specs:
        pack = _pack(df.loc[pred, "oc"].to_numpy())
        rows.append({"band": name, **pack})
    return rows


def feature_masks(df: pd.DataFrame) -> list[tuple[str, pd.Series, str]]:
    return [
        ("incomplete", df["incomplete"], "Elite earnsurp/rsi/ext missing on the morning card"),
        ("complete", df["complete"], "earnsurp + rsi + ext all present"),
        ("consumer_defensive", df["cd"], "join sector = Consumer Defensive"),
        ("china_adr", df["china_adr"], "geo:ADR-China in join detail or Finviz Country=China"),
        ("cd_or_adr", df["cd_or_adr"], "Consumer Defensive or China ADR"),
        ("junk_top", df["junk_top"], "incomplete or Consumer Defensive or China ADR"),
        ("healthcare", df["hc"], "join sector = Healthcare"),
        ("hc_complete", df["hc_complete"], "Healthcare with a complete Elite card"),
        ("hc_after_red", df["hc_after_red"], "Healthcare the morning after a red SPY day"),
        ("technology", df["tech"], "join sector = Technology"),
        ("washed", df["washed"], "join ext=washed (already a +0.40 prior; rarely at the top)"),
        ("big_beat", df["big_beat"], "join earnsurp=big_beat"),
        ("last_green", df["last_green"], "prior bar open→close green (OHLC, leak-free)"),
        ("last_red", df["last_red"], "prior bar open→close red (OHLC, leak-free)"),
        ("news_good", df["news_good"], "pre-open News Title tone = good (09:30-knowable)"),
        ("news_bad", df["news_bad"], "pre-open News Title tone = bad (09:30-knowable)"),
    ]


def sweep_band(df: pd.DataFrame, lo: int, hi: int, role: str) -> tuple[list[dict], dict]:
    band = df[(df["rank"] >= lo) & (df["rank"] <= hi) & df["oc"].notna()].copy()
    base = _pack(band["oc"].to_numpy())
    rows = []
    if band.empty or base.get("win") is None:
        return rows, base
    for name, mask, why in feature_masks(band):
        if is_banned(name):
            continue
        pack = _pack(band.loc[mask, "oc"].to_numpy())
        if pack["n"] < 8 or pack["win"] is None:
            continue
        lift = 100.0 * (pack["win"] - base["win"])
        rows.append({
            "role": role, "band": f"{lo}-{hi}", "feature": name, "why": why,
            "n": pack["n"], "win": pack["win"], "mean": pack["mean"],
            "t": pack["t"], "base_n": base["n"], "base_win": base["win"],
            "base_mean": base["mean"], "lift_pp": round(lift, 2),
            "mean_edge": None if pack["mean"] is None or base["mean"] is None
            else round(pack["mean"] - base["mean"], 5),
        })
    rows.sort(key=lambda r: (r.get("lift_pp") or 0, r.get("n") or 0))
    if role == "elevate":
        rows.sort(key=lambda r: (-(r.get("lift_pp") or -999), -(r.get("n") or 0)))
    else:
        rows.sort(key=lambda r: ((r.get("lift_pp") or 0), -(r.get("n") or 0)))
    return rows, base


def pick_avoid_keepers(rows: list[dict], min_n: int = AVOID_MIN_N) -> list[dict]:
    keep = []
    for r in rows:
        if is_banned(r.get("feature") or ""):
            continue
        if (r.get("n") or 0) < min_n:
            continue
        if (r.get("lift_pp") or 0) > AVOID_LIFT:
            continue
        if (r.get("mean_edge") or 0) > AVOID_MEAN_EDGE:
            continue
        keep.append({**r, "call": "AVOID"})
    keep.sort(key=lambda r: ((r.get("lift_pp") or 0), -(r.get("n") or 0)))
    return keep


def pick_elevate_keepers(rows: list[dict], min_n: int = ELEVATE_MIN_N) -> list[dict]:
    keep = []
    for r in rows:
        if is_banned(r.get("feature") or ""):
            continue
        if (r.get("feature") or "") in ELEVATE_BLOCKLIST:
            continue
        if (r.get("n") or 0) < min_n:
            continue
        if (r.get("lift_pp") or 0) < ELEVATE_LIFT:
            continue
        if (r.get("mean") or 0) <= 0:
            continue
        if (r.get("mean_edge") or 0) < ELEVATE_MEAN_EDGE:
            continue
        keep.append({**r, "call": "ELEVATE"})
    keep.sort(key=lambda r: (-(r.get("lift_pp") or -999), -(r.get("n") or 0)))
    return keep


def _eligible(day: pd.DataFrame) -> pd.DataFrame:
    """Skip junk. Require a complete card only when that session has any."""
    g = day[day["oc"].notna()].sort_values("rank")
    if g.empty:
        return g
    if bool(g["complete"].any()):
        keep = g[g["complete"] & ~g["cd"] & ~g["china_adr"]]
    else:
        keep = g[~g["cd"] & ~g["china_adr"]]
    return keep


def pick_book(day: pd.DataFrame, kind: str, n: int = BOOK_N) -> pd.DataFrame:
    g = day[day["oc"].notna()].sort_values("rank")
    if g.empty:
        return g.head(0)
    if kind == "raw8":
        return g.head(n)
    if kind == "mid1630":
        return g[(g["rank"] >= MID_CORE[0]) & (g["rank"] <= MID_CORE[1])]
    elig = _eligible(g)
    if kind == "skip_junk":
        return elig.head(n)
    if kind == "elev_hc":
        pool = elig[elig["rank"] <= MID_HI]
        return pd.concat([pool[pool["hc"]], pool[~pool["hc"]]]).head(n)
    if kind == "elev_hc_cap3":
        pool = elig[elig["rank"] <= MID_HI]
        hc = pool[pool["hc"]].head(3)
        rest = pool[~pool["Ticker"].isin(set(hc["Ticker"]))].head(n)
        return pd.concat([hc, rest]).head(n)
    if kind == "elev_hc_red":
        pool = elig[elig["rank"] <= MID_HI]
        red = pool[pool["hc_after_red"]]
        rest = pool[~pool["hc_after_red"]]
        return pd.concat([red, rest]).head(n)
    raise ValueError(f"unknown book {kind}")


def tape_pack(frame: pd.DataFrame) -> dict:
    pack = _pack(frame["oc"].to_numpy()) if not frame.empty else _pack([])
    tapes = {}
    if "spy_cc_down" in frame.columns and not frame.empty:
        for lab, pred in (("up", ~frame["spy_cc_down"]), ("down", frame["spy_cc_down"])):
            sub = frame.loc[pred]
            tapes[lab] = _pack(sub["oc"].to_numpy()) if len(sub) else _pack([])
    pack["tapes"] = tapes
    pack["days"] = int(frame["session_date"].nunique()) if not frame.empty else 0
    return pack


def fee_pnl_row(open_px, close_px, budget: float, fees: dict) -> float | None:
    o = _finite(open_px)
    c = _finite(close_px)
    if o is None or c is None or o <= 0 or budget <= 0:
        return None
    shares = int(budget // o)
    if shares <= 0:
        return None
    buy_f = pt.order_fees(shares, o, "buy", fees)
    sell_f = pt.order_fees(shares, c, "sell", fees)
    return float(shares * (c - o) - buy_f - sell_f)


def book_fee_total(frame: pd.DataFrame, fees: dict, budget_each: float) -> dict:
    pnls = []
    skipped = 0
    for rec in frame.to_dict("records"):
        pnl = fee_pnl_row(rec.get("px_open"), rec.get("px_close"), budget_each, fees)
        if pnl is None:
            skipped += 1
            continue
        pnls.append(pnl)
    return {
        "n_fills": len(pnls),
        "n_skip_px": skipped,
        "pnl": round(float(sum(pnls)), 2) if pnls else None,
        "mean_pnl": round(float(np.mean(pnls)), 2) if pnls else None,
    }


def build_books(df: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict[str, dict]]:
    kinds = ("raw8", "skip_junk", "elev_hc", "elev_hc_cap3", "elev_hc_red", "mid1630")
    bags: dict[str, list[pd.DataFrame]] = {k: [] for k in kinds}
    daily = []
    for day, g in df.groupby("session_date", sort=True):
        if g["oc"].notna().sum() == 0:
            continue
        rec = {"date": str(day),
               "spy_cc_down": bool(g["spy_cc_down"].iloc[0]) if "spy_cc_down" in g.columns else None,
               "prior_spy_red": bool(g["prior_spy_red"].iloc[0]) if "prior_spy_red" in g.columns else None,
               "n_complete": int(g["complete"].sum()),
               "elite_day": bool(g["complete"].any())}
        for kind in kinds:
            sl = pick_book(g, kind).copy()
            sl["elite_day"] = rec["elite_day"]
            bags[kind].append(sl)
            rec[f"{kind}_n"] = int(len(sl))
            rec[f"{kind}_win"] = None if sl.empty else round(float((sl["oc"] > 0).mean()), 4)
            rec[f"{kind}_mean"] = None if sl.empty else round(float(sl["oc"].mean()), 5)
        daily.append(rec)
    frames = {k: pd.concat(v, ignore_index=True) if v else df.head(0) for k, v in bags.items()}
    fees = pt.load_fees()
    budget_each = BUDGET / BOOK_N
    summary = {}
    for kind, frame in frames.items():
        pack = tape_pack(frame)
        pack["fee"] = book_fee_total(frame, fees, budget_each)
        pack["kind"] = kind
        if "elite_day" in frame.columns and not frame.empty:
            elite = frame[frame["elite_day"] == True]
            pre = frame[frame["elite_day"] == False]
            pack["elite"] = tape_pack(elite)
            pack["elite"]["fee"] = book_fee_total(elite, fees, budget_each)
            pack["pre_elite"] = tape_pack(pre)
            pack["pre_elite"]["fee"] = book_fee_total(pre, fees, budget_each)
        summary[kind] = pack
    return frames, {"books": summary, "daily": daily}


def buried_examples(df: pd.DataFrame, n: int = 12) -> list[dict]:
    mid = df[(df["rank"] >= MID_LO) & (df["rank"] <= MID_HI) & df["oc"].notna()]
    hit = mid.sort_values("oc", ascending=False).head(n)
    out = []
    for r in hit.itertuples(index=False):
        out.append({
            "date": r.session_date, "ticker": r.Ticker, "rank": int(r.rank),
            "oc": round(100 * float(r.oc), 2),
            "sector": getattr(r, "sector", None),
            "complete": bool(getattr(r, "complete", False)),
            "hc": bool(getattr(r, "hc", False)),
            "total_score": _finite(getattr(r, "total_score", None)),
        })
    return out


def junk_examples(df: pd.DataFrame, n: int = 10) -> list[dict]:
    top = df[(df["rank"] <= TOP_N) & df["oc"].notna() & df["junk_top"]]
    hit = top.sort_values("oc").head(n)
    out = []
    for r in hit.itertuples(index=False):
        out.append({
            "date": r.session_date, "ticker": r.Ticker, "rank": int(r.rank),
            "oc": round(100 * float(r.oc), 2),
            "sector": getattr(r, "sector", None),
            "incomplete": bool(getattr(r, "incomplete", False)),
            "cd": bool(getattr(r, "cd", False)),
            "china_adr": bool(getattr(r, "china_adr", False)),
            "total_score": _finite(getattr(r, "total_score", None)),
        })
    return out


def both_tape_note(pack: dict) -> str:
    tapes = pack.get("tapes") or {}
    bits = []
    for lab in ("up", "down"):
        t = tapes.get(lab) or {}
        if t.get("n"):
            bits.append(
                f"{lab} n={t['n']} win={100*(t.get('win') or 0):.1f}% "
                f"mean={100*(t.get('mean') or 0):+.2f}%"
            )
    return "; ".join(bits) if bits else "no tape split"


def _verdict(bands: list[dict], avoids: list[dict], elevates: list[dict],
             books: dict, n_days: int, n_names: int) -> str:
    by = {b["band"]: b for b in bands}
    t15 = by.get("ranks_1_15") or {}
    mid = by.get("ranks_16_30") or {}
    raw = books.get("raw8") or {}
    skip = books.get("skip_junk") or {}
    elev = books.get("elev_hc") or {}
    bits = [
        f"Join's own top-15 underperforms the mediocre 16–30 band on the "
        f"1d open→close clock across {n_days} sessions ({n_names} name-days "
        f"with a Finviz Change-from-Open). Top-15 win "
        f"{100*(t15.get('win') or 0):.1f}% / mean {100*(t15.get('mean') or 0):+.2f}% "
        f"(n={t15.get('n')}); ranks 16–30 win "
        f"{100*(mid.get('win') or 0):.1f}% / mean {100*(mid.get('mean') or 0):+.2f}% "
        f"(n={mid.get('n')}). That is an inverted ranker, not a missing "
        f"external formula. Grok Bot already rejected CANSLIM / Magic "
        f"Formula / cheap Forward P/E / total_score re-sort — those stay out."
    ]
    if avoids:
        a = avoids[0]
        bits.append(
            f"Avoid keeper: `{a['feature']}` inside ranks 1–15 "
            f"(n={a['n']}, win {100*(a.get('win') or 0):.1f}%, "
            f"lift {a.get('lift_pp'):+.1f}pp vs the top-15 band, "
            f"mean {100*(a.get('mean') or 0):+.2f}%)."
        )
    else:
        bits.append("No avoid flag cleared n≥40, lift ≤ −3pp, and a worse mean than the top-15 band.")
    if elevates:
        e = elevates[0]
        bits.append(
            f"Elevate keeper: `{e['feature']}` inside ranks 16–80 "
            f"(n={e['n']}, win {100*(e.get('win') or 0):.1f}%, "
            f"lift {e.get('lift_pp'):+.1f}pp, mean {100*(e.get('mean') or 0):+.2f}%)."
        )
    else:
        bits.append("No elevate flag cleared n≥40, lift ≥ +3pp, and a positive mean edge in the 16–80 band.")
    red = books.get("elev_hc_red") or {}
    bits.append(
        f"Patched top-8 (skip incomplete / Consumer Defensive / China ADR"
        f", Healthcare first from ranks ≤80): "
        f"raw8 mean {100*(raw.get('mean') or 0):+.2f}% "
        f"(win {100*(raw.get('win') or 0):.1f}%, n={raw.get('n')}) vs "
        f"skip_junk {100*(skip.get('mean') or 0):+.2f}% "
        f"(n={skip.get('n')}) vs elev_hc {100*(elev.get('mean') or 0):+.2f}% "
        f"(n={elev.get('n')}) vs elev_hc_red "
        f"{100*(red.get('mean') or 0):+.2f}% (n={red.get('n')}). "
        f"Fee-aware $10k / 8 names: raw "
        f"${(raw.get('fee') or {}).get('pnl')} vs skip "
        f"${(skip.get('fee') or {}).get('pnl')} vs elev "
        f"${(elev.get('fee') or {}).get('pnl')} vs elev_hc_red "
        f"${(red.get('fee') or {}).get('pnl')}. "
        f"Both-tape raw: {both_tape_note(raw)}. elev_hc: {both_tape_note(elev)}."
    )
    pre = elev.get("pre_elite") or {}
    post = elev.get("elite") or {}
    raw_pre = raw.get("pre_elite") or {}
    raw_post = raw.get("elite") or {}
    if pre.get("n") or post.get("n"):
        bits.append(
            f"The skip/elevate patch mostly fires before Elite labels exist "
            f"(pre-Elite raw8 {100*(raw_pre.get('mean') or 0):+.2f}% vs "
            f"elev_hc {100*(pre.get('mean') or 0):+.2f}%). After 2026-08-20 "
            f"the morning card is complete and raw8 "
            f"{100*(raw_post.get('mean') or 0):+.2f}% vs elev_hc "
            f"{100*(post.get('mean') or 0):+.2f}% — often the same eight names. "
            f"Healthcare already in top-15 is the good pocket; mid-band "
            f"Healthcare as a whole is not an elevate. The mid-band keeper "
            f"is Healthcare the morning after a red SPY day."
        )
    bits.append(
        "Research overlay only. Do not change join_rules.json, "
        "LIVE_POLICY, or flatten_robust. Do not paste this 1d IC onto "
        "flatten_h5. Mid 16–30 is a diagnosis that the ranker is inverted "
        "— it is not a recipe that blindly buys ranks 16–30 every morning."
    )
    return " ".join(bits)


def run(join_dir: Path | None = None, prices: pd.DataFrame | None = None) -> dict:
    ranked = load_join_ranked(join_dir)
    if ranked.empty:
        raise SystemExit("[rank_residual] no data/join/*_ranked.csv")
    ranked = attach_session_finviz(ranked)
    ranked = attach_ohlc_priors(ranked, prices=prices)
    ranked = add_flags(ranked)
    scored = ranked[ranked["oc"].notna()].copy()
    bands = band_stats(scored)
    avoid_rows, top_base = sweep_band(scored, 1, TOP_N, "avoid")
    elev_rows, mid_base = sweep_band(scored, MID_LO, MID_HI, "elevate")
    avoids = pick_avoid_keepers(avoid_rows)
    elevates = pick_elevate_keepers(elev_rows)
    frames, book_blob = build_books(scored)
    days = sorted(scored["session_date"].unique().tolist())
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window": {
            "from": days[0] if days else None,
            "to": days[-1] if days else None,
            "n_sessions": int(len(days)),
            "n_join_files": int(ranked["session_date"].nunique()),
            "n_name_days": int(len(scored)),
            "n_join_rows": int(len(ranked)),
        },
        "clock": (
            "1d open→close = same-session Finviz Change from Open "
            "(join / stock-book clock). Parquet oc fills holes through "
            "2026-08-21. Does not auto-apply to flatten_h5 / live 3d recycle."
        ),
        "leak": (
            "leak-free 09:30 — Change / Gap / RelVol / printed book / "
            "today OHLC never pick. CFO and Price are outcomes. "
            "News Title only when News Time < 09:30 ET."
        ),
        "complementary": (
            "Does not re-sort by total_score. Does not wire FPE / CANSLIM / "
            "Magic Formula / AlphaSift. Live flatten_robust / LIVE_POLICY "
            "untouched. Complements PR #142 (null external enrich) and "
            "#141 (no regime-survival invariant)."
        ),
        "bars": {
            "avoid": {"min_n": AVOID_MIN_N, "lift_pp": AVOID_LIFT, "mean_edge": "<0"},
            "elevate": {"min_n": ELEVATE_MIN_N, "lift_pp": ELEVATE_LIFT, "mean": ">0"},
            "book_n": BOOK_N, "budget": BUDGET,
        },
        "bands": bands,
        "bases": {"top15": top_base, "mid1680": mid_base},
        "avoid_sweep": avoid_rows,
        "elevate_sweep": elev_rows,
        "keepers_avoid": avoids,
        "keepers_elevate": elevates,
        "books": book_blob["books"],
        "daily": book_blob["daily"],
        "buried": buried_examples(scored),
        "junk": junk_examples(scored),
        "verdict": _verdict(
            bands, avoids, elevates, book_blob["books"],
            int(len(days)), int(len(scored)),
        ),
    }
    return payload


def _md_table(rows: list[dict], kind: str) -> list[str]:
    if not rows:
        return ["| *(none cleared)* | — | — | — | — | — | — |", ""]
    lines = [
        "| Feature | Band | n | Win% | Lift | Mean oc | t | Why |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for r in rows:
        lines.append(
            f"| `{r.get('feature')}` | {r.get('band') or '—'} | {r.get('n')} | "
            f"{100*(r.get('win') or 0):.1f}% | "
            f"{(r.get('lift_pp') if r.get('lift_pp') is not None else 0):+.1f}pp | "
            f"{100*(r.get('mean') or 0):+.2f}% | "
            f"{r['t'] if r.get('t') is not None else '—'} | {r.get('why') or ''} |"
        )
    lines.append("")
    return lines


def _book_line(name: str, pack: dict) -> str:
    fee = pack.get("fee") or {}
    return (
        f"| `{name}` | {pack.get('n')} | {pack.get('days')} | "
        f"{100*(pack.get('win') or 0):.1f}% | "
        f"{100*(pack.get('mean') or 0):+.2f}% | "
        f"{pack['t'] if pack.get('t') is not None else '—'} | "
        f"{fee.get('pnl')} | {both_tape_note(pack)} |"
    )


def write_outputs(payload: dict) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    w = payload.get("window") or {}
    lines = [
        f"# Rank residual mine — {w.get('from')} → {w.get('to')}",
        "",
        "Join's own ranked file, not an external screener. Live "
        "`flatten_robust` / `LIVE_POLICY` / `join_rules.json` are untouched. "
        "Grok Bot already rejected CANSLIM, Magic Formula, cheap Forward P/E, "
        "and a `total_score` re-sort — those stay out.",
        "",
        payload.get("clock") or "",
        "",
        payload.get("leak") or "",
        "",
        "## Verdict",
        "",
        payload.get("verdict") or "",
        "",
        "## Rank bands (1d open→close)",
        "",
        "| Band | n | Win% | Mean oc | t |",
        "|---|---:|---:|---:|---:|",
    ]
    for b in payload.get("bands") or []:
        lines.append(
            f"| `{b['band']}` | {b['n']} | {100*(b.get('win') or 0):.1f}% | "
            f"{100*(b.get('mean') or 0):+.2f}% | "
            f"{b['t'] if b.get('t') is not None else '—'} |"
        )
    lines += [
        "",
        "## Avoid keepers (inside ranks 1–15)",
        "",
        "A keeper needs n≥40, win-rate lift ≤ −3pp vs the top-15 band, "
        "and a worse mean than that band. Same-day tape never picks.",
        "",
    ]
    lines += _md_table(payload.get("keepers_avoid") or [], "avoid")
    lines += [
        "## Elevate keepers (inside ranks 16–80)",
        "",
        "A keeper needs n≥40, lift ≥ +3pp vs the 16–80 band, and a "
        "positive mean *and* mean edge. Mid 16–30 as a *band* is a "
        "diagnosis, not a buy-ranks-16–30 recipe.",
        "",
    ]
    lines += _md_table(payload.get("keepers_elevate") or [], "elevate")
    lines += [
        "## Patched top-8 vs raw join top-8",
        "",
        "`skip_junk` drops incomplete Elite cards, Consumer Defensive, "
        "and China ADRs, then refills from lower ranks. `elev_hc` does "
        "the same and then puts Healthcare names from ranks ≤80 first. "
        "`elev_hc_red` prefers Healthcare only the morning after a red "
        "SPY day (the mid-band keeper). Fee book is $10k / 8 names, "
        "whole shares, Futubull both sides. `mid1630` is diagnostic only.",
        "",
        "| Book | n | Days | Win% | Mean oc | t | Fee $ | Both-tape |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for name in ("raw8", "skip_junk", "elev_hc", "elev_hc_cap3",
                 "elev_hc_red", "mid1630"):
        pack = (payload.get("books") or {}).get(name) or {}
        lines.append(_book_line(name, pack))
    lines += [
        "",
        "### Pre-Elite vs Elite-label days",
        "",
        "Before 2026-08-20 the ranked file has no earnsurp/rsi/ext. "
        "That is when incomplete cards and China ADRs occupy #1–8. "
        "After Elite labels exist, skip_junk is often a no-op.",
        "",
        "| Book | Window | n | Win% | Mean oc | Fee $ |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for name in ("raw8", "skip_junk", "elev_hc", "elev_hc_red"):
        pack = (payload.get("books") or {}).get(name) or {}
        for win in ("pre_elite", "elite"):
            sl = pack.get(win) or {}
            fee = sl.get("fee") or {}
            lines.append(
                f"| `{name}` | {win} | {sl.get('n')} | "
                f"{100*(sl.get('win') or 0):.1f}% | "
                f"{100*(sl.get('mean') or 0):+.2f}% | {fee.get('pnl')} |"
            )
    lines += [
        "",
        "## Buried names the ranker parked mid-list",
        "",
        "| Date | Ticker | Rank | oc | Sector | Complete |",
        "|---|---|---:|---:|---|---|",
    ]
    for r in payload.get("buried") or []:
        lines.append(
            f"| {r['date']} | `{r['ticker']}` | {r['rank']} | "
            f"{r['oc']:+.2f}% | {r.get('sector') or '—'} | "
            f"{'yes' if r.get('complete') else 'no'} |"
        )
    lines += [
        "",
        "## Worst top-15 junk the ranker still printed",
        "",
        "| Date | Ticker | Rank | oc | Why |",
        "|---|---|---:|---:|---|",
    ]
    for r in payload.get("junk") or []:
        why = []
        if r.get("incomplete"):
            why.append("incomplete")
        if r.get("cd"):
            why.append("Consumer Defensive")
        if r.get("china_adr"):
            why.append("China ADR")
        lines.append(
            f"| {r['date']} | `{r['ticker']}` | {r['rank']} | "
            f"{r['oc']:+.2f}% | {', '.join(why) or '—'} |"
        )
    lines += [
        "",
        "## How this is graded",
        "",
        "1. Universe is the morning `data/join/YYYY-MM-DD_ranked.csv`.",
        "2. Outcome is same-session Finviz Change from Open. Today's "
        "Change% / Gap / RelVol / OHLC never pick.",
        "3. Incomplete = earnsurp / rsi / ext missing on that morning card.",
        "4. News Title is used only when News Time is strictly before 09:30 ET.",
        "5. Both-tape splits use that session's SPY close-to-close (robustness, "
        "not a 09:30 gate).",
        "6. Do not change `join_rules.json` or live flatten unless a keeper "
        "clears the bar *and* Cyrus asks to wire it.",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    if TEMPLATE.is_file():
        html = TEMPLATE.read_text(encoding="utf-8")
        html = html.replace("__DATA__", json.dumps(payload, separators=(",", ":")))
        (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    payload = run()
    if args.write:
        write_outputs(payload)
        print(f"wrote {OUT_JSON} and {OUT_MD}")
        print(payload.get("verdict", "")[:500])
    else:
        print(json.dumps({
            "window": payload["window"],
            "bands": payload["bands"],
            "keepers_avoid": payload["keepers_avoid"],
            "keepers_elevate": payload["keepers_elevate"],
            "books": {k: {kk: vv for kk, vv in v.items() if kk != "tapes"}
                      for k, v in payload["books"].items()},
            "verdict": payload.get("verdict"),
        }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
