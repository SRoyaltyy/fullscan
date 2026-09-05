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


def _boxes(sig, fv_rel, in_buy, present, digest_tone=None, judge_tone=None,
           heat_tone=None, catal_tone=None, news_tone=None):
    """Color boxes from the 09:30-ET information set only.

    Tape boxes (vol/AB/peer) use the last completed tape dated before D
    (walk back when the exact prior session file is missing).
    Join uses D's ranked file when that day's weather came from the
    morning predict; otherwise the last prior join.
    Morning boxes use D's pre-open packet, or the last prior predict for
    sector/gen. Digest/judge may fall back to that day's sector print.
    The news box is per-ticker company news (or RSS/actions). A sector
    digest sample must not paint it. Missing means this name had no
    headline — never a silent yellow just because a sample file exists.
    """
    if news_tone in ("good", "bad", "neutral"):
        news_box = news_tone
    elif present.get("news"):
        news_box = tl._polarity(sig.get("s_news"))
    else:
        news_box = "missing"
    out = {
        "join": tl._polarity(sig.get("s_join")) if present.get("join") else "missing",
        "sector": tl._polarity(sig.get("s_sector")) if present.get("sector") else "missing",
        "gen": tl._polarity(sig.get("s_general")) if present.get("gen") else "missing",
        "news": news_box,
        "digest": digest_tone or "missing",
        "judge": judge_tone or "missing",
        "ab": tl._polarity(sig.get("s_ab")) if present.get("ab") else "missing",
        "peer": tl._polarity(sig.get("s_peer")) if present.get("peer") else "missing",
        "heat": heat_tone or "missing",
        "catal": catal_tone or "missing",
        "buy": "good" if in_buy else ("neutral" if present.get("overnight_book") else "missing"),
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
    """Factor colors as of 09:30 ET on sess['date'].

    Packet recipe: D's join when weather is the morning predict; last
    Finviz / AB / peer / overnight book dated before D; D's pre-open
    packet (last prior predict when D's is missing). Same-day
    stock_book and same-day post-close Finviz are ignored.
    """
    t = tl._tick(ticker)
    prior = sess.get("prior")
    prior_date = sess.get("prior_date")
    packet = tl.preopen_packet(sess["date"], prior_date=prior_date)

    use_packet_join = tl.join_packet_ok(sess["date"])
    if use_packet_join:
        join = (sess.get("join") or {}).get(t)
        join_source, join_vintage = "packet_join", sess["date"]
    else:
        join_sess = tl.walk_prior(sess, lambda s: t in (s.get("join") or {}))
        join = (join_sess.get("join") or {}).get(t) if join_sess else None
        join_source, join_vintage = "prior_join", (
            join_sess["date"] if join_sess else prior_date)
    fv_sess = tl.walk_prior(sess, lambda s: t in (s.get("finviz") or {}))
    ab_sess = tl.walk_prior(sess, lambda s: t in (s.get("ab") or {}))
    peer_sess = tl.walk_prior(sess, lambda s: t in (s.get("peer") or {}))
    book_sess = tl.walk_prior(sess, lambda s: s.get("has", {}).get("book"))
    fv = (fv_sess.get("finviz") or {}).get(t) if fv_sess else None
    ab = (ab_sess.get("ab") or {}).get(t) if ab_sess else None
    peer = (peer_sess.get("peer") or {}).get(t) if peer_sess else None
    univ = None
    if use_packet_join:
        univ = (sess.get("universe") or {}).get(t)
    if not univ:
        univ_sess = tl.walk_prior(sess, lambda s: t in (s.get("universe") or {}))
        univ = (univ_sess.get("universe") or {}).get(t) if univ_sess else None
    prior_book = (book_sess.get("book") or {}).get(t) if book_sess else None
    quote_sess = tl.walk_prior(
        sess, lambda s: t in (s.get("quote_colors") or {}))
    quote = ((quote_sess.get("quote_colors") or {}).get(t)
             if quote_sess else None)
    catalyst = packet.get("catalyst", {}).get(t)

    news_rec = (packet.get("news") or {}).get(t)
    has_preopen = any((
        packet.get("has_actions"), packet.get("has_digest"),
        packet.get("has_judge"), packet.get("has_predict"),
        bool(catalyst), bool(packet.get("heat")),
    ))
    if not any((join, fv, ab, peer, univ, prior_book, quote, news_rec, catalyst, has_preopen)):
        return None

    sig = {k: None for k in (
        "s_join", "s_sector", "s_general", "s_news", "s_ab", "s_peer",
        "s_heat", "s_opp", "score_1d", "score_3d", "score_1w")}
    reasons = sector = industry = size = mcap = None
    source = []
    vintage = {"asof": "09:30_et", "prior_date": prior_date}

    if join:
        source.append(join_source)
        sig["s_join"] = tl._s_from_join(join)
        sector = join.get("sector")
        industry = join.get("industry")
        size = join.get("size")
        vintage["join"] = join_vintage
    if ab:
        source.append("prior_ab")
        sig["s_ab"] = tl._s_from_ab(ab)
        vintage["ab"] = ab_sess["date"] if ab_sess else prior_date
    if peer:
        source.append("prior_peer")
        sig["s_peer"] = tl._s_from_peer(peer)
        vintage["peer"] = peer_sess["date"] if peer_sess else prior_date
    if fv:
        source.append("prior_finviz")
        if mcap is None:
            mcap = tl._num(fv.get("Market Cap"))
        sector = sector or fv.get("Sector")
        industry = industry or fv.get("Industry")
        vintage["vol"] = fv_sess["date"] if fv_sess else prior_date
        vintage["finviz"] = fv_sess["date"] if fv_sess else prior_date
    if prior_book:
        source.append("overnight_book")
        reasons = prior_book.get("reasons")
        sector = sector or prior_book.get("sector")
        industry = industry or prior_book.get("industry")
        size = size or prior_book.get("size")
        if mcap is None:
            mcap = tl._num(prior_book.get("market_cap_m"))
        vintage["overnight_book"] = book_sess["date"] if book_sess else prior_date
    if univ:
        sector = sector or univ.get("sector")
        industry = industry or univ.get("industry")
    if news_rec or packet.get("has_actions") or packet.get("has_judge"):
        if news_rec:
            source.append("preopen_news")
        net = (news_rec or {}).get("net")
        if net is not None:
            sig["s_news"] = max(-1.0, min(1.0, math.tanh(float(net) / 5.0)))
        vintage["news"] = sess["date"]
    if packet.get("has_digest"):
        source.append("preopen_digest")
        vintage["digest"] = sess["date"]
    if packet.get("has_judge"):
        source.append("preopen_judge")
        vintage["judge"] = sess["date"]
    if packet.get("has_predict"):
        source.append("preopen_predict")
        pred_v = packet.get("predict_vintage") or sess["date"]
        vintage["sector"] = pred_v
        vintage["gen"] = pred_v
        sec_bias = (packet.get("sector_bias") or {}).get(sector)
        if sec_bias is not None:
            sig["s_sector"] = float(sec_bias)
        else:
            # Predict printed; this sector was not called. Flat, not missing.
            sig["s_sector"] = 0.0
        beta_src = None
        if join is not None:
            beta_src = join.get("beta")
        if beta_src is None and fv is not None:
            beta_src = fv.get("Beta") or fv.get("beta")
        sig["s_general"] = float(packet.get("gen_bias") or 0.0) * tl._beta_load(beta_src)
    heat_val = (packet.get("heat") or {}).get(t)
    if heat_val is None and industry:
        heat_val = (packet.get("heat_ind") or {}).get(industry)
    if heat_val is None and sector:
        heat_val = (packet.get("heat_sec") or {}).get(sector)
    if heat_val is not None:
        source.append("preopen_heat")
        sig["s_heat"] = float(heat_val)
        vintage["heat"] = packet.get("heat_vintage") or sess["date"]
    if catalyst:
        source.append("preopen_catalyst")
        vintage["catal"] = sess["date"]

    fv_rel = tl._fv_relvol(fv)
    buys = ((book_sess or {}).get("buys") or {}).get(t) or {}
    sells = ((book_sess or {}).get("sells") or {}).get(t) or {}
    company_news_tone = (packet.get("company_news_tones") or {}).get(t)
    present = {
        "join": join is not None,
        "ab": ab is not None,
        "peer": peer is not None,
        "finviz": fv is not None,
        "sector": sig.get("s_sector") is not None or packet.get("has_predict"),
        "gen": packet.get("has_predict"),
        "news": (
            news_rec is not None
            or company_news_tone in ("good", "bad", "neutral")
        ),
        "overnight_book": book_sess is not None,
    }
    judge_raw = (packet.get("judge") or {}).get(t)
    judge_tone = tl._polarity(judge_raw) if judge_raw is not None else None
    if judge_tone is None:
        judge_tone = tl.judge_sector_tone(packet.get("judge_tilts"), sector)
    if judge_tone is None:
        judge_tone = "missing"
    elif judge_raw is None and judge_tone != "missing":
        vintage["judge"] = sess["date"]
    digest_tone = (packet.get("digest_tones") or {}).get(t)
    if digest_tone is None and sector:
        digest_tone = (packet.get("digest_sector") or {}).get(sector)
    if digest_tone is None:
        digest_tone = "missing"
    news_tone = None
    if sig.get("s_news") is not None:
        news_tone = tl._polarity(sig.get("s_news"))
    if news_tone in (None, "missing") and company_news_tone in (
        "good", "bad", "neutral"
    ):
        news_tone = company_news_tone
    heat_tone = tl._polarity(sig.get("s_heat")) if sig.get("s_heat") is not None else "missing"
    catal_tone = "missing"
    if catalyst:
        cat_weight = tl._num(catalyst.get("conviction"), 50.0) / 100.0
        signal = str(catalyst.get("net_signal") or "").lower()
        signed = cat_weight if "bullish" in signal else -cat_weight if "bearish" in signal else 0.0
        catal_tone = tl._polarity(signed)
    boxes = _boxes(
        sig, fv_rel, bool(buys), present,
        digest_tone=digest_tone, judge_tone=judge_tone,
        heat_tone=heat_tone, catal_tone=catal_tone,
        news_tone=news_tone)
    gate = _independent_green(sig, fv_rel)
    families = {}
    if join:
        for fam in tl.JOIN_FAMILIES:
            if fam in join and pd.notna(join.get(fam)):
                families[fam] = {"value": join.get(fam),
                                 "tone": tl._join_family_tone(join.get(fam))}
    cls = "asof_0930"
    if size == "micro" or (mcap is not None and mcap < 400):
        cls = "gated_out"
    elif buys:
        cls = "overnight_buy"
    elif sells:
        cls = "overnight_sell"
    elif gate["green"]:
        cls = "green_0930"
    elif not any((join, fv, ab, peer, news_rec, catalyst, packet.get("has_predict"))):
        cls = "no_print"
    if reasons is not None and isinstance(reasons, float) and math.isnan(reasons):
        reasons = None
    return {
        "date": sess["date"], "ticker": t, "class": cls, "asof": "09:30_et",
        "sources": source, "factor_vintage": vintage,
        "sector": sector, "industry": industry, "size": size, "mcap_m": mcap,
        "signals": {k: (None if v is None else round(v, 3)) for k, v in sig.items()},
        "boxes": boxes, "independent_green": gate,
        "in_green_buy": False,
        "in_live_buy": False,
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
        "prior_date": prior_date,
    }
