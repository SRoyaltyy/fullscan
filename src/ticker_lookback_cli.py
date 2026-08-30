"""Per-session scan helpers for ticker lookback.

Used by src.ticker_lookback_run.
"""
from __future__ import annotations

import math

import pandas as pd

from . import ticker_lookback as tl


def _tone_num(value, positive=True, neutral_band=0.0):
    v = tl._num(value)
    if v is None:
        return "missing"
    if abs(v) <= neutral_band:
        return "neutral"
    good = v > 0 if positive else v < 0
    return "good" if good else "bad"


def _finviz_factors(row):
    """Transparent colors for the full Elite export (all-market coverage)."""
    if not row:
        return {}
    specs = {
        "Change": (True, 0.0), "Performance (Week)": (True, 0.0),
        "Performance (Month)": (True, 0.0),
        "EPS Growth This Year": (True, 0.0),
        "EPS Growth Next Year": (True, 0.0),
        "EPS Year Over Year TTM": (True, 0.0),
        "Sales Year Over Year TTM": (True, 0.0),
        "EPS Growth Quarter Over Quarter": (True, 0.0),
        "Sales Growth Quarter Over Quarter": (True, 0.0),
        "EPS Surprise": (True, 0.0), "Revenue Surprise": (True, 0.0),
        "Return on Assets": (True, 0.0), "Return on Equity": (True, 0.0),
        "Return on Invested Capital": (True, 0.0),
        "Gross Margin": (True, 0.0), "Operating Margin": (True, 0.0),
        "Profit Margin": (True, 0.0),
        "Insider Transactions": (True, 0.0),
        "Institutional Transactions": (True, 0.0),
        "20-Day Simple Moving Average": (True, 0.0),
        "50-Day Simple Moving Average": (True, 0.0),
        "200-Day Simple Moving Average": (True, 0.0),
    }
    out = {}
    for name, (positive, band) in specs.items():
        if name in row and pd.notna(row.get(name)):
            out[name] = {"value": row.get(name),
                         "tone": _tone_num(row.get(name), positive, band),
                         "source": "finviz_export"}
    recom = tl._num(row.get("Analyst Recom"))
    if recom is not None:
        tone = "good" if recom <= 2.3 else "bad" if recom >= 3.3 else "neutral"
        out["Analyst Recom"] = {"value": recom, "tone": tone,
                                "source": "finviz_export"}
    target, price = tl._num(row.get("Target Price")), tl._num(row.get("Price"))
    if target is not None and price:
        upside = 100 * (target / price - 1)
        out["Target upside"] = {"value": round(upside, 2),
                                "tone": _tone_num(upside, True, 2.0),
                                "source": "finviz_export"}
    rsi = tl._num(row.get("Relative Strength Index (14)"))
    if rsi is not None:
        tone = "bad" if rsi >= 70 else "good" if 30 <= rsi <= 60 else "neutral"
        out["RSI (14)"] = {"value": rsi, "tone": tone,
                           "source": "finviz_export"}
    return out


def _ab_factors(row):
    if not row:
        return {}
    out = {}
    for key, value in row.items():
        if not str(key).startswith("status_") or pd.isna(value):
            continue
        s = str(value).strip().upper()
        tone = "good" if s == "GOOD" else "bad" if s == "BAD" else "neutral"
        out[str(key)[7:]] = {"value": s, "tone": tone, "source": "ab"}
    return out


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
    quote = sess.get("quote_colors", {}).get(t)
    catalyst = sess.get("catalyst", {}).get(t)
    if not any((book, join, fv, ab, peer, univ, quote, catalyst)):
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
    if quote:
        source.append("quote_colors")
    if catalyst:
        source.append("catalyst")
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
    if sig.get("s_heat") is not None:
        boxes["heat"] = tl._polarity(sig.get("s_heat"))
    if catalyst:
        cat_weight = tl._num(catalyst.get("conviction"), 50.0) / 100.0
        signal = str(catalyst.get("net_signal") or "").lower()
        signed = cat_weight if "bullish" in signal else -cat_weight if "bearish" in signal else 0.0
        boxes["catal"] = tl._polarity(signed)
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
        "join_families": families,
        "finviz_factors": _finviz_factors(fv),
        "quote_color_fields": (quote or {}).get("fields") or {},
        "ab_factors": _ab_factors(ab),
        "catalyst": catalyst,
        "forward_returns": tl.forward_returns(t, sess["date"]),
        "artifacts_that_day": sess["has"],
    }
