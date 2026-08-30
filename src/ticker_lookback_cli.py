"""CLI wrapper: python -m src.ticker_lookback_cli --tickers TEM,ELF

Uses ticker_lookback.py helpers already on main.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from . import ticker_lookback as tl


def _boxes(sig, fv_rel, in_buy, present):
    out = {
        "join": tl._polarity(sig.get("s_join")) if present.get("join") or present.get("book") else "missing",
        "sector": tl._polarity(sig.get("s_sector")) if present.get("book") else "missing",
        "gen": tl._polarity(sig.get("s_general")) if present.get("book") else "missing",
        "news": tl._polarity(sig.get("s_news")) if present.get("book") else "missing",
        "digest": "neutral" if present.get("book") else "missing",
        "judge": "neutral" if present.get("book") else "missing",
        "ab": tl._polarity(sig.get("s_ab")) if present.get("ab") or present.get("book") else "missing",
        "peer": tl._polarity(sig.get("s_peer")) if present.get("peer") or present.get("book") else "missing",
        "heat": "missing", "catal": "missing",
        "buy": "good" if in_buy else "neutral",
    }
    if not present.get("finviz") and fv_rel is None:
        out["vol"] = "missing"
    elif fv_rel is None:
        out["vol"] = "neutral"
    elif fv_rel >= tl.RELVOL_SPIKE:
        out["vol"] = "good"
    elif fv_rel < tl.RELVOL_DEAD:
        out["vol"] = "bad"
    else:
        out["vol"] = "neutral"
    if not present.get("book"):
        for k, sk in (("sector", "s_sector"), ("gen", "s_general"), ("news", "s_news")):
            if out[k] == "neutral" and sig.get(sk) is None:
                out[k] = "missing"
        out["digest"] = out["judge"] = "missing"
    return out


def _independent_green(sig, fv_rel):
    cores = {c: tl._num(sig.get(c)) for c in tl.CORE}
    if any(v is None for v in cores.values()):
        return {"green": False, "why": "cores incomplete (need join/gen/AB/peer prints)"}
    if any(v < tl.EPS for v in cores.values()):
        weak = [c[2:] for c, v in cores.items() if v < tl.EPS]
        return {"green": False, "why": "core below +0.05: " + ",".join(weak)}
    if tl._num(sig.get("s_sector"), 0.0) <= -tl.EPS:
        return {"green": False, "why": "sector veto"}
    if tl._num(sig.get("s_news"), 0.0) <= -tl.EPS:
        return {"green": False, "why": "news veto"}
    if fv_rel is not None and fv_rel > 0 and fv_rel < tl.RELVOL_DEAD:
        return {"green": False, "why": f"relvol dead ({fv_rel:.2f})"}
    mean = sum(cores.values()) / 4.0
    return {"green": True, "why": f"all four cores >= +0.05, green_rank={mean:+.3f}", "green_rank": round(mean, 3)}


def _scan_session(sess, ticker):
    t = tl._tick(ticker)
    book, join, fv = sess["book"].get(t), sess["join"].get(t), sess["finviz"].get(t)
    ab, peer, univ = sess["ab"].get(t), sess["peer"].get(t), sess["universe"].get(t)
    if not any((book, join, fv, ab, peer, univ)):
        return None
    sig = {k: None for k in ("s_join", "s_sector", "s_general", "s_news", "s_ab", "s_peer", "s_heat", "s_opp", "score_1d", "score_3d", "score_1w")}
    reasons = sector = industry = size = mcap = None
    source = []
    if book:
        source.append("book")
        for k in sig:
            if k in book:
                sig[k] = tl._num(book.get(k), 0.0)
        reasons, sector, industry, size = book.get("reasons"), book.get("sector"), book.get("industry"), book.get("size")
        mcap = tl._num(book.get("market_cap_m"))
    if join:
        source.append("join")
        if sig["s_join"] is None:
            sig["s_join"] = tl._s_from_join(join)
        sector = sector or join.get("sector")
        industry = industry or join.get("industry")
        size = size or join.get("size")
    if ab:
        source.append("ab")
        if sig["s_ab"] is None:
            sig["s_ab"] = tl._s_from_ab(ab)
    if peer:
        source.append("peer")
        if sig["s_peer"] is None:
            sig["s_peer"] = tl._s_from_peer(peer)
    if fv:
        source.append("finviz")
        if mcap is None:
            mcap = tl._num(fv.get("Market Cap"))
        sector = sector or fv.get("Sector")
        industry = industry or fv.get("Industry")
    if univ and "universe" not in source:
        source.append("universe")
        sector = sector or univ.get("sector")
        industry = industry or univ.get("industry")
    fv_rel = tl._fv_relvol(fv)
    buys = sess["buys"].get(t) or {}
    sells = sess["sells"].get(t) or {}
    present = {
        "book": book is not None,
        "join": join is not None or (book is not None and sig.get("s_join") is not None),
        "ab": ab is not None or (book is not None and sig.get("s_ab") is not None),
        "peer": peer is not None or (book is not None and sig.get("s_peer") is not None),
        "finviz": fv is not None,
    }
    boxes = _boxes(sig, fv_rel, bool(buys), present)
    gate = _independent_green(sig, fv_rel)
    families = {}
    if join:
        for fam in tl.JOIN_FAMILIES:
            if fam in join and pd.notna(join.get(fam)):
                families[fam] = {"value": join.get(fam), "tone": tl._join_family_tone(join.get(fam))}
    cls = "no_print"
    if book:
        if size == "micro" or (mcap is not None and mcap < 400):
            cls = "gated_out"
        elif buys:
            cls = "in_buy_book"
        elif sells:
            cls = "in_sell_book"
        elif gate["green"]:
            cls = "green_not_printed"
        elif any(abs(tl._num(sig.get(c), 0) or 0) >= tl.EPS for c in ("s_ab", "s_peer", "s_news")):
            cls = "outweighed"
        else:
            cls = "in_universe"
    elif join or ab or peer:
        cls = "full_market_only"
    elif fv:
        cls = "finviz_only"
    if reasons is not None and isinstance(reasons, float) and math.isnan(reasons):
        reasons = None
    return {
        "date": sess["date"], "ticker": t, "class": cls, "sources": source,
        "sector": sector, "industry": industry, "size": size, "mcap_m": mcap,
        "signals": {k: (None if v is None else round(v, 3)) for k, v in sig.items()},
        "boxes": boxes, "independent_green": gate,
        "in_green_buy": t in sess["green_buy"], "in_live_buy": t in sess["live_buy"],
        "buy_ranks": buys, "sell_ranks": sells,
        "reasons": None if reasons is None else str(reasons),
        "finviz": None if not fv else {"relvol": None if fv_rel is None else round(fv_rel, 3),
                                        "change_pct": tl._num(fv.get("Change") or fv.get("Change %")),
                                        "price": tl._num(fv.get("Price"))},
        "ab_raw": None if not ab else tl._num(ab.get("ab_raw")),
        "peer": None if not peer else {"rs_week": tl._num(peer.get("rs_week")), "rs_month": tl._num(peer.get("rs_month"))},
        "join_families": families, "artifacts_that_day": sess["has"],
    }
