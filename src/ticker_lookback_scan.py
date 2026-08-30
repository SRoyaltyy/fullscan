"""Scan + markdown render helpers for ticker_lookback."""
from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

import pandas as pd

from .ticker_lookback import (
    AB_DIR, BOOK_DIR, BOX_COLS, BOX_ICON, CORE, DAILY, EPS, JOIN_FAMILIES,
    RELVOL_DEAD, RELVOL_SPIKE, SCORE, ET, _fv_relvol, _join_family_tone,
    _num, _polarity, _s_from_ab, _s_from_join, _s_from_peer, _tick, build_index,
)


def _icon(kind):
    return BOX_ICON.get(kind, BOX_ICON["missing"])


def _boxes(sig, fv_rel, in_buy, present):
    out = {
        "join": _polarity(sig.get("s_join")) if present.get("join") or present.get("book") else "missing",
        "sector": _polarity(sig.get("s_sector")) if present.get("book") else "missing",
        "gen": _polarity(sig.get("s_general")) if present.get("book") else "missing",
        "news": _polarity(sig.get("s_news")) if present.get("book") else "missing",
        "digest": "neutral" if present.get("book") else "missing",
        "judge": "neutral" if present.get("book") else "missing",
        "ab": _polarity(sig.get("s_ab")) if present.get("ab") or present.get("book") else "missing",
        "peer": _polarity(sig.get("s_peer")) if present.get("peer") or present.get("book") else "missing",
        "heat": "missing", "catal": "missing",
        "buy": "good" if in_buy else "neutral",
    }
    if not present.get("finviz") and fv_rel is None:
        out["vol"] = "missing"
    elif fv_rel is None:
        out["vol"] = "neutral"
    elif fv_rel >= RELVOL_SPIKE:
        out["vol"] = "good"
    elif fv_rel < RELVOL_DEAD:
        out["vol"] = "bad"
    else:
        out["vol"] = "neutral"
    if not present.get("book"):
        for k, sk in (("sector", "s_sector"), ("gen", "s_general"), ("news", "s_news")):
            if out[k] == "neutral" and sig.get(sk) is None:
                out[k] = "missing"
        out["digest"] = "missing"
        out["judge"] = "missing"
    return out


def _independent_green(sig, fv_rel):
    cores = {c: _num(sig.get(c)) for c in CORE}
    if any(v is None for v in cores.values()):
        return {"green": False, "why": "cores incomplete (need join/gen/AB/peer prints)"}
    if any(v < EPS for v in cores.values()):
        weak = [c[2:] for c, v in cores.items() if v < EPS]
        return {"green": False, "why": "core below +0.05: " + ",".join(weak)}
    if _num(sig.get("s_sector"), 0.0) <= -EPS:
        return {"green": False, "why": "sector veto"}
    if _num(sig.get("s_news"), 0.0) <= -EPS:
        return {"green": False, "why": "news veto"}
    if fv_rel is not None and fv_rel > 0 and fv_rel < RELVOL_DEAD:
        return {"green": False, "why": f"relvol dead ({fv_rel:.2f})"}
    mean = sum(cores.values()) / 4.0
    return {"green": True, "why": f"all four cores >= +0.05, green_rank={mean:+.3f}", "green_rank": round(mean, 3)}
