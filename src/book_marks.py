"""Lookback marks on the stock-book frame — the sheet the ranker ignored.

Ticker Lookback already had a working language:
  🔵 blue   — objectively better vs the prior session, or +≥3 box points
  🚨 alarm  — purely worse (no cell better, at least one worse)
  ⚪ white  — zero_red: printed factors exist and none of them is red
  Cond      — G/Y/R majority (green/red only when that color leads)
  Region    — sea of green vs sea of red (yellows ignored)
  featured  — mined setups (first_crack / alarm|heat=bad are fades)

The diagnostic printed these on the sleeve *after* the book was picked.
This module attaches the same marks to every liquid name *before*
``_book_side``, so BUY can refuse alarm / Cond-red / region-red / fade.

White (no red) is recorded and shown, but is not a hard BUY gate: a
market-wide red general makes zero_red empty for the whole universe.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from . import ticker_lookback as tl
from .ticker_lookback_setups import featured_book, match_day

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"

# Same boxes as the lookback sheet, minus ``buy`` (circular — that box
# is "are we already in the book?").
MARK_KEYS = [k for k, _ in tl.BOX_COLS if k != "buy"]
FADE_VERDICT = "fade"
BLUE_BOOST = 0.05


def _vol_tone(rel) -> str:
    try:
        v = float(rel)
    except (TypeError, ValueError):
        return "missing"
    if v != v or v <= 0:
        return "missing"
    if v >= tl.RELVOL_SPIKE:
        return "good"
    if v < tl.RELVOL_DEAD:
        return "bad"
    return "neutral"


def boxes_from_row(row) -> dict[str, str]:
    """One lookback row from stock-book signal columns."""
    get = row.get if hasattr(row, "get") else lambda k, d=None: (
        row[k] if k in getattr(row, "index", []) else d
    )
    def source(name: str, fallback: str) -> str:
        value = str(get(f"src_{name}_tone", "") or "").lower()
        return value if value in ("good", "bad", "neutral", "missing") else fallback

    return {
        "join": tl._polarity(get("s_join")),
        # Prefer lattice source/domain verdicts.  They preserve conflicts and
        # fill the digest/judge/catalyst cells that were previously hardcoded
        # missing before BUY selection.
        "sector": source("sector", tl._polarity(get("s_sector"))),
        "gen": source("gen", tl._polarity(get("s_general"))),
        "news": source("news", tl._polarity(get("s_news"))),
        "digest": source("digest", "missing"),
        "judge": source("judge", "missing"),
        "ab": source("ab", tl._polarity(get("s_ab"))),
        "peer": tl._polarity(get("s_peer")),
        "heat": source("heat", tl._polarity(get("s_heat"))),
        "vol": source("vol", _vol_tone(get("relvol"))),
        "catal": source("catal", "missing"),
    }


def _prev_csv(date: str) -> pd.DataFrame | None:
    files = sorted(BOOK_DIR.glob("????-??-??_stock_book.csv"))
    prev = None
    for p in files:
        if p.name[:10] < date:
            prev = p
    if prev is None:
        return None
    try:
        df = pd.read_csv(prev, low_memory=False)
    except OSError:
        return None
    if "Ticker" not in df.columns:
        return None
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    return df.drop_duplicates("Ticker", keep="first")


def _fade_ids() -> set[str]:
    return {s["id"] for s in featured_book() if s.get("verdict") == FADE_VERDICT}


def annotate_one(boxes: dict, prev_boxes: dict | None) -> dict:
    """Same marks ``ticker_lookback.annotate_signal_improved`` puts on a day."""
    cond = tl.general_condition(boxes)
    region = tl.color_region(boxes)
    zr = tl.zero_red(boxes)
    blue = alarm = False
    delta = None
    if prev_boxes:
        delta = tl.point_delta(prev_boxes, boxes)
        blue = tl.objectively_better(prev_boxes, boxes) or delta >= tl.BLUE_POINT_JUMP
        alarm = tl.purely_worse(prev_boxes, boxes)
    day = {
        "boxes": boxes,
        "region": region,
        "condition": cond,
        "zero_red": zr,
        "signal_improved": blue,
        "signal_alarm": alarm,
        "point_delta": delta,
        "stretch": {"tone": "missing"},
    }
    day["tag_context"] = tl.tag_context(day)
    setups = match_day(day)
    fade_ids = _fade_ids()
    fade = any(s.get("id") in fade_ids for s in setups)
    return {
        "lb_cond": cond.get("tone") or "missing",
        "lb_region": region.get("tone") or "missing",
        "lb_zero_red": bool(zr),
        "lb_blue": bool(blue),
        "lb_alarm": bool(alarm),
        "lb_fade": bool(fade),
        "lb_tags": ",".join(day.get("tag_context") or []),
        "lb_setups": ",".join(s.get("id") or "" for s in setups if s.get("id")),
        "lb_points": int(tl.box_points(boxes)),
    }


def attach(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """Add lookback mark columns. Never raises — marks stay false/missing."""
    if df is None or df.empty:
        return df
    out = df.copy()
    prev = _prev_csv(date)
    prev_boxes: dict[str, dict] = {}
    if prev is not None and len(prev):
        for _, r in prev.iterrows():
            prev_boxes[str(r["Ticker"]).upper()] = boxes_from_row(r)

    marks = {k: [] for k in (
        "lb_cond", "lb_region", "lb_zero_red", "lb_blue", "lb_alarm",
        "lb_fade", "lb_tags", "lb_setups", "lb_points",
    )}
    for _, r in out.iterrows():
        t = str(r.get("Ticker") or "").upper()
        rec = annotate_one(boxes_from_row(r), prev_boxes.get(t))
        for k, v in rec.items():
            marks[k].append(v)
    for k, vals in marks.items():
        out[k] = vals
    n_alarm = int(out["lb_alarm"].sum())
    n_fade = int(out["lb_fade"].sum())
    n_white = int(out["lb_zero_red"].sum())
    n_blue = int(out["lb_blue"].sum())
    print(
        f"[stock-book] lookback marks alarm={n_alarm} fade={n_fade} "
        f"blue={n_blue} white={n_white} / {len(out)}"
    )
    return out


def apply_blue_boost(df: pd.DataFrame, horizons: tuple[str, ...]) -> pd.DataFrame:
    """Small BUY-side nudge for a blue day. Does not touch core / SELL."""
    if df is None or df.empty or "lb_blue" not in df.columns:
        return df
    blue = df["lb_blue"].astype(str).str.lower().isin(["true", "1"])
    n = int(blue.sum())
    if not n:
        return df
    for h in horizons:
        col = f"score_{h}"
        if col in df.columns:
            df.loc[blue, col] = df.loc[blue, col] + BLUE_BOOST
    print(f"[stock-book] 🔵 blue boost +{BLUE_BOOST:.2f} on {n} names")
    return df


def veto_mask(df: pd.DataFrame) -> pd.Series:
    """True = refuse the BUY. Missing columns → no extra veto."""
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    veto = pd.Series(False, index=df.index)
    if "lb_alarm" in df.columns:
        veto |= df["lb_alarm"].astype(str).str.lower().isin(["true", "1"])
    if "lb_fade" in df.columns:
        veto |= df["lb_fade"].astype(str).str.lower().isin(["true", "1"])
    if "lb_cond" in df.columns:
        veto |= df["lb_cond"].astype(str).str.lower().eq("bad")
    if "lb_region" in df.columns:
        veto |= df["lb_region"].astype(str).str.lower().eq("bad")
    return veto
