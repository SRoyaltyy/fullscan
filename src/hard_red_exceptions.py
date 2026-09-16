"""Daily investigator list — names weather sit sat on.

Cyrus: even on hard-red mornings (S≤−3) some names still have enough
idiosyncratic cameras / E / news / sector to be a net gain. Live
flatten_robust stay full sit. This board ranks every 09:30 looker
(the investigator card, plus extra tape) so those names are not only
visible after a click.

Universe = factor-mine panel rows that session (shopping lists), not
the 11k Finviz dump. LLM coaches are not invented for names flatten
never saw.

Does not change live sit / Webull / flatten_robust.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import factor_mine_probe as fmp
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "hard_red_exceptions"
DASH_DIR = ROOT / "dashboard" / "hard-red-exceptions"
OUT_MD = ROOT / "03_scoreboard" / "HARD_RED_EXCEPTIONS.md"
TOP_N = 8
HOLDS = (1, 3, 5)

# Weather / market cameras — not idiosyncratic. Still shown, not scored
# as "this name is strong."
WEATHER_CAMS = ("gen", "sect", "sector")


def _tone(card: dict, key: str) -> str:
    boxes = card.get("boxes") or {}
    return str(boxes.get(key) or "missing").lower()


def idio_parts(card: dict) -> list[dict]:
    """Clock-clean score pieces. Gen/sect are labeled weather, not idio."""
    parts: list[dict] = []
    good = int(card.get("cond_good") or 0)
    bad = int(card.get("cond_bad") or 0)
    parts.append({
        "k": "cameras", "pts": 2 * (good - bad),
        "why": f"+{good} −{bad} cameras",
    })
    e = str(card.get("e_pol") or "missing")
    if e == "good":
        parts.append({"k": "E", "pts": 4, "why": card.get("e_label") or "E beat"})
    elif e == "bad":
        parts.append({"k": "E", "pts": -4, "why": card.get("e_label") or "E miss"})
    r = str(card.get("r_pol") or "missing")
    if r == "good":
        parts.append({"k": "R", "pts": 2, "why": card.get("r_label") or "R up"})
    elif r == "bad":
        parts.append({"k": "R", "pts": -2, "why": card.get("r_label") or "R down"})
    news = str((card.get("news") or {}).get("tone") or "missing")
    if news == "good":
        parts.append({"k": "news", "pts": 2, "why": "news camera green"})
    elif news == "bad":
        parts.append({"k": "news", "pts": -2, "why": "news camera red"})
    if _tone(card, "join") == "good":
        parts.append({"k": "join", "pts": 1, "why": "join green"})
    elif _tone(card, "join") == "bad":
        parts.append({"k": "join", "pts": -1, "why": "join red"})
    if _tone(card, "peer") == "good":
        parts.append({"k": "peer", "pts": 1, "why": "peer green"})
    elif _tone(card, "peer") == "bad":
        parts.append({"k": "peer", "pts": -1, "why": "peer red"})
    if _tone(card, "catal") == "good" or card.get("earn_react"):
        parts.append({"k": "catalyst", "pts": 2, "why": "catalyst / earn-react"})
    y = fm._finite(card.get("yday_ret"))
    if y is not None:
        if y > 0:
            parts.append({"k": "yday", "pts": 1, "why": f"yday {y:+.1f}%"})
        elif y < 0:
            parts.append({"k": "yday", "pts": -1, "why": f"yday {y:+.1f}%"})
    elif card.get("last_green"):
        parts.append({"k": "yday", "pts": 1, "why": "last bar green"})
    elif card.get("last_red"):
        parts.append({"k": "yday", "pts": -1, "why": "last bar red"})
    if card.get("white"):
        parts.append({"k": "white", "pts": 2, "why": "zero red cameras"})
    if card.get("alarm"):
        parts.append({"k": "alarm", "pts": -3, "why": "alarm overnight"})
    if card.get("flow_in"):
        parts.append({"k": "flow", "pts": 1, "why": "FLOW IN"})
    if card.get("macd_up") or card.get("macd_cross_up"):
        parts.append({"k": "macd", "pts": 1, "why": "MACD hist + / X+"})
    if card.get("burst"):
        parts.append({"k": "burst", "pts": 1, "why": "parabolic prior tape"})
    # RSI crash-long / melt-up-short is a pause, not a weather sit.
    rsi = fm._finite(card.get("rsi"))
    if card.get("rsi_ob") or (rsi is not None and rsi >= 75):
        parts.append({"k": "rsi", "pts": -2, "why": f"RSI {rsi:.0f} OB — pause longs"})
    if card.get("rsi_os") or (rsi is not None and rsi <= 25):
        parts.append({"k": "rsi", "pts": 1, "why": f"RSI {rsi:.0f} OS — long-friendly"})
    return parts


def idio_score(card: dict) -> dict:
    parts = idio_parts(card)
    total = int(sum(int(p["pts"]) for p in parts))
    weather = []
    for k in ("gen", "sect", "sector"):
        t = _tone(card, k)
        if t in ("good", "bad"):
            weather.append(f"{k} {t}")
    return {
        "score": total,
        "parts": parts,
        "weather": weather,
        "n_pos": int(card.get("cond_good") or 0),
        "n_neg": int(card.get("cond_bad") or card.get("n_neg") or 0),
    }


def long_ok(card: dict, pack: dict) -> bool:
    """Idiosyncratic long — not merely 'weather was red'."""
    if pack["score"] < 3:
        return False
    if card.get("alarm"):
        return False
    rsi = fm._finite(card.get("rsi"))
    if rsi is not None and rsi >= 80:
        return False
    if int(card.get("cond_bad") or 0) >= 5:
        return False
    return True


def short_ok(card: dict, pack: dict) -> bool:
    if pack["score"] > -3:
        return False
    if int(card.get("cond_good") or 0) >= 5:
        return False
    return True


def why_still(card: dict, pack: dict, *, side: str, hard: bool, s) -> list[str]:
    lines = []
    if hard:
        lines.append(
            f"Weather S={s} ≤ {fmb.HARD_RED:g} — live sleeve SITS. "
            "This name is ranked anyway so idiosyncratic green is not invisible."
        )
    else:
        lines.append(f"Weather S={s}. Not a sit morning — still ranked.")
    top = sorted(pack["parts"], key=lambda p: -abs(int(p["pts"])))[:6]
    for p in top:
        sign = "+" if p["pts"] > 0 else ""
        lines.append(f"{sign}{p['pts']} {p['why']}")
    if pack.get("weather"):
        lines.append("Weather cameras (not in idio score): " + ", ".join(pack["weather"]))
    news = card.get("news") or {}
    if news.get("title"):
        lines.append(f"News: {news.get('tone')} — {news['title'][:120]}")
    if card.get("e_label"):
        lines.append(card["e_label"])
    rsi = fm._finite(card.get("rsi"))
    if rsi is not None:
        lines.append(
            f"Tape RSI {rsi:.0f}"
            f"{' OS' if card.get('rsi_os') else ''}"
            f"{' OB' if card.get('rsi_ob') else ''}"
            f" · MACD hist {card.get('macd_hist')}"
        )
    lines.append(f"Side lean: {side} · idio {pack['score']:+d}")
    return lines


CAM_ORDER = (
    "join", "sector", "gen", "news", "digest", "judge", "ab",
    "peer", "heat", "vol", "catal", "buy", "yday",
)
CAM_SHORT = {
    "join": "join", "sector": "sect", "gen": "gen", "news": "news",
    "digest": "dig", "judge": "jdg", "ab": "AB", "peer": "peer",
    "heat": "heat", "vol": "vol", "catal": "cat", "buy": "buy", "yday": "yd",
}


def px_book_from_panel(panel: dict) -> dict:
    """ticker → date → {o, c} from leak-free panel open/close."""
    book: dict[str, dict] = {}
    for r in panel.get("rows") or []:
        t = fm._tick(r.get("ticker"))
        d = str(r.get("date") or "")[:10]
        if not t or not d:
            continue
        slot = book.setdefault(t, {}).setdefault(d, {})
        o = fm._finite(r.get("open"))
        c = fm._finite(r.get("close"))
        if o is not None:
            slot["o"] = float(o)
        if c is not None:
            slot["c"] = float(c)
    return book


def fill_yahoo(book: dict, tickers: list[str], start: str, end: str) -> dict:
    """Fill missing session OHLC. Soft-fail if yfinance is missing."""
    names = sorted({fm._tick(t) for t in tickers if fm._tick(t)})
    if not names or not start:
        return book
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:
        return book
    from datetime import date as _date, timedelta
    try:
        y, m, d = (int(x) for x in end.split("-"))
        end_excl = (_date(y, m, d) + timedelta(days=3)).isoformat()
    except Exception:
        end_excl = end
    for i in range(0, len(names), 60):
        chunk = names[i:i + 60]
        try:
            df = yf.download(
                chunk, start=start, end=end_excl, auto_adjust=True,
                progress=False, threads=True, group_by="ticker",
            )
        except Exception:
            continue
        if df is None or getattr(df, "empty", True):
            continue
        cols = getattr(df, "columns", None)
        if cols is None:
            continue
        multi = getattr(cols, "nlevels", 1) == 2
        for t in chunk:
            try:
                sub = df[t] if multi else df
            except Exception:
                continue
            if sub is None or getattr(sub, "empty", True):
                continue
            try:
                recs = sub.reset_index().to_dict("records")
            except Exception:
                continue
            for rec in recs:
                dt = rec.get("Date") or rec.get("index")
                ds = str(pd.Timestamp(dt).date()) if dt is not None else ""
                if not ds:
                    continue
                o = fm._finite(rec.get("Open"))
                c = fm._finite(rec.get("Close"))
                slot = book.setdefault(t, {}).setdefault(ds, {})
                if o is not None and slot.get("o") is None:
                    slot["o"] = float(o)
                if c is not None and slot.get("c") is None:
                    slot["c"] = float(c)
    return book


def horizon_pack(px_book: dict, cal: list[str], ticker: str, date: str,
                 hold: int, side: str = "long") -> dict:
    """09:30 open → close hold sessions later. Prices + %."""
    out = {"pct": None, "px": None, "date": None, "open": None}
    if not date or date not in cal:
        return out
    i = cal.index(date)
    j = i + int(hold)
    entry = (px_book.get(ticker) or {}).get(date) or {}
    o = fm._finite(entry.get("o"))
    out["open"] = None if o is None else round(float(o), 4)
    if o is None or o == 0 or j >= len(cal):
        return out
    exd = cal[j]
    px = fm._finite(((px_book.get(ticker) or {}).get(exd) or {}).get("c"))
    out["date"] = exd
    if px is None:
        return out
    pct = 100.0 * (float(px) / float(o) - 1.0)
    if side == "short":
        pct = -pct
    out["px"] = round(float(px), 4)
    out["pct"] = round(pct, 2)
    return out


def day_move(px_book: dict, ticker: str, date: str) -> dict:
    slot = (px_book.get(ticker) or {}).get(date) or {}
    o = fm._finite(slot.get("o"))
    c = fm._finite(slot.get("c"))
    pct = None
    if o and c is not None and o != 0:
        pct = round(100.0 * (float(c) / float(o) - 1.0), 2)
    return {
        "open": None if o is None else round(float(o), 4),
        "close": None if c is None else round(float(c), 4),
        "day_pct": pct,
    }


def _grade(ticker: str, date: str, hold: int, side: str, cal: list[str],
           bars, px_book: dict | None = None):
    if px_book:
        return horizon_pack(px_book, cal, ticker, date, hold, side).get("pct")
    try:
        return fm.hold_return(ticker, date, hold, cal, side, None, {}, bars)
    except Exception:
        return None


def rank_day(date: str, cards: dict, *, s, hard: bool,
             cal: list[str], bars, px_book: dict | None = None) -> dict:
    longs, shorts, all_rows = [], [], []
    for ticker, card in (cards or {}).items():
        pack = idio_score(card)
        rec = {
            "ticker": ticker,
            "date": date,
            "idio": pack["score"],
            "n_pos": pack["n_pos"],
            "n_neg": pack["n_neg"],
            "e_pol": card.get("e_pol"),
            "e_label": card.get("e_label"),
            "r_pol": card.get("r_pol"),
            "news_tone": (card.get("news") or {}).get("tone"),
            "headline": card.get("headline") or (card.get("news") or {}).get("title"),
            "yday_ret": card.get("yday_ret"),
            "rsi": card.get("rsi"),
            "macd_hist": card.get("macd_hist"),
            "flow_in": bool(card.get("flow_in")),
            "white": bool(card.get("white")),
            "alarm": bool(card.get("alarm")),
            "on_list": bool(card.get("on_list")),
            "sources": card.get("sources") or [],
            "open": card.get("open"),
            "close": ((px_book or {}).get(ticker) or {}).get(date, {}).get("c"),
            "day_pct": day_move(px_book or {}, ticker, date).get("day_pct"),
            "boxes": card.get("boxes") or {},
            "domains": card.get("domains"),
            "parts": pack["parts"],
            "weather": pack["weather"],
            "h1": None, "h3": None, "h5": None,
        }
        all_rows.append(rec)
        if long_ok(card, pack):
            rec_l = dict(rec, side="long",
                         why=why_still(card, pack, side="long", hard=hard, s=s))
            for h in HOLDS:
                rec_l[f"h{h}"] = _grade(
                    ticker, date, h, "long", cal, bars, px_book)
                pack_h = horizon_pack(px_book or {}, cal, ticker, date, h, "long")
                rec_l[f"h{h}_px"] = pack_h.get("px")
                rec_l[f"h{h}_date"] = pack_h.get("date")
            longs.append(rec_l)
        if short_ok(card, pack):
            rec_s = dict(rec, side="short",
                         why=why_still(card, pack, side="short", hard=hard, s=s))
            for h in HOLDS:
                rec_s[f"h{h}"] = _grade(
                    ticker, date, h, "short", cal, bars, px_book)
                pack_h = horizon_pack(px_book or {}, cal, ticker, date, h, "short")
                rec_s[f"h{h}_px"] = pack_h.get("px")
                rec_s[f"h{h}_date"] = pack_h.get("date")
            shorts.append(rec_s)
    longs.sort(key=lambda r: (-r["idio"], -r["n_pos"], r["ticker"]))
    shorts.sort(key=lambda r: (r["idio"], r["n_neg"], r["ticker"]))
    all_rows.sort(key=lambda r: (-r["idio"], r["ticker"]))
    return {
        "date": date,
        "s": s,
        "hard_red": hard,
        "n_cards": len(cards or {}),
        "longs": longs[:TOP_N],
        "shorts": shorts[:TOP_N],
        "n_long_ok": len(longs),
        "n_short_ok": len(shorts),
        "all_top": all_rows[:40],
    }


def _pack_rets(vals: list) -> dict:
    xs = [float(v) for v in vals if v is not None]
    if not xs:
        return {"n": 0, "win": None, "mean": None}
    wins = sum(1 for x in xs if x > 0)
    return {
        "n": len(xs),
        "win": round(wins / len(xs), 4),
        "mean": round(sum(xs) / len(xs), 3),
    }


def backtest(days: list[dict]) -> dict:
    """If we had taken top longs/shorts on hard-red sits, hold-1/3."""
    red = [d for d in days if d.get("hard_red")]
    out = {"n_hard_red": len(red), "long_h1": {}, "long_h3": {},
           "short_h1": {}, "short_h3": {}}
    for side, key in (("longs", "long"), ("shorts", "short")):
        for h in (1, 3):
            vals = []
            for d in red:
                for rec in d.get(side) or []:
                    vals.append(rec.get(f"h{h}"))
            out[f"{key}_h{h}"] = _pack_rets(vals)
    return out


def _tally_boxes(boxes: dict) -> tuple[int, int]:
    good = sum(1 for v in (boxes or {}).values() if str(v).lower() == "good")
    bad = sum(1 for v in (boxes or {}).values() if str(v).lower() == "bad")
    return good, bad


def row_from_lookback(day: dict) -> dict:
    """Paint cameras from ticker-lookback artifacts (join/weather/Finviz/news)."""
    boxes = dict(day.get("boxes") or {})
    cond = day.get("condition") or {}
    n_pos = int(cond.get("good") or 0)
    n_neg = int(cond.get("bad") or 0)
    if not n_pos and not n_neg:
        n_pos, n_neg = _tally_boxes(boxes)
    srcs = [str(x) for x in (day.get("sources") or []) if x]
    arts = day.get("artifacts_that_day")
    if isinstance(arts, dict):
        srcs.extend(str(k) for k, v in arts.items() if v)
    elif isinstance(arts, list):
        srcs.extend(str(x) for x in arts if x)
    news = day.get("news") if isinstance(day.get("news"), dict) else {}
    title = (
        (news or {}).get("title")
        or day.get("headline")
        or day.get("news_title")
        or ""
    )
    return {
        "n_pos": n_pos,
        "n_neg": n_neg,
        "cams": f"+{n_pos} −{n_neg}",
        "boxes": boxes,
        "e_pol": day.get("e_pol") or "missing",
        "e_label": day.get("e_label") or "",
        "r_pol": day.get("r_pol") or "missing",
        "r_label": day.get("r_label") or "",
        "news": title,
        "news_tone": (news or {}).get("tone") or day.get("headline_tone")
        or fm._tone(boxes, "news"),
        "sources": srcs,
        "files": srcs,
        "rsi": day.get("rsi") or (
            (day.get("finviz") or {}).get("RSI")
            if isinstance(day.get("finviz"), dict) else None
        ),
        "macd_hist": day.get("macd_hist"),
        "reconstructed": True,
    }


def _csv_wanted(path: Path, wanted: set[str], tick_keys=("Ticker", "ticker")) -> dict:
    out: dict[str, dict] = {}
    if not path.is_file() or not wanted:
        return out
    import csv
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        for rec in csv.DictReader(fh):
            t = ""
            for k in tick_keys:
                t = (rec.get(k) or "").strip().upper()
                if t:
                    break
            if t in wanted:
                out[t] = rec
    return out


def _tone_num(v, band: float = 0.0) -> str:
    x = fm._finite(v)
    if x is None:
        return "missing"
    if abs(float(x)) <= band:
        return "neutral"
    return "good" if float(x) > 0 else "bad"


def _stance_tone(st: str) -> str:
    s = str(st or "").strip().lower()
    if s in ("favorable", "good", "up", "bull", "green"):
        return "good"
    if s in ("hostile", "bad", "down", "bear", "red"):
        return "bad"
    if s in ("neutral", "flat", "yellow"):
        return "neutral"
    return "missing"


def _weather_pack(date: str) -> dict:
    path = ROOT / "01_daily" / "weather" / f"{date}_weather.json"
    if not path.is_file():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    signals = doc.get("signals") or {}
    sectors = {}
    for name, blob in ((doc.get("stances") or {}).get("sector") or {}).items():
        if isinstance(blob, dict):
            sectors[str(name)] = _stance_tone(blob.get("stance"))
        else:
            sectors[str(name)] = _stance_tone(blob)
    gen_dir = str(signals.get("general_direction") or "").lower()
    gen = _stance_tone(gen_dir) if gen_dir else _tone_num(signals.get("general_score"), 0.5)
    return {
        "s": fm._finite(signals.get("general_score")),
        "gen": gen,
        "sectors": sectors,
        "file": str(path.relative_to(ROOT)),
    }


def load_lookback(tickers: list[str], from_date: str | None,
                  to_date: str | None, cal: list[str] | None = None) -> dict:
    """Reconstruct 09:30 cameras for every session from join/weather/Finviz.

    Does not load ticker-lookback's full index (that OOMs on 11k Finviz
    dumps). Streams only the names we already track.
    """
    wanted = {fm._tick(t) for t in tickers if fm._tick(t)}
    dates = list(cal or [])
    if not dates:
        weather_dir = ROOT / "01_daily" / "weather"
        if weather_dir.is_dir():
            dates = sorted(
                p.name[:10] for p in weather_dir.glob("*_weather.json")
                if (not from_date or p.name[:10] >= from_date)
                and (not to_date or p.name[:10] <= to_date)
            )
    out: dict[tuple[str, str], dict] = {}
    prior_fv: dict[str, dict] = {}
    for date in dates:
        wx = _weather_pack(date)
        join_path = ROOT / "data" / "join" / f"{date}_ranked.csv"
        joins = _csv_wanted(join_path, wanted)
        fv_path = ROOT / "data" / "exports" / f"finviz_{date}.csv"
        fvs = _csv_wanted(fv_path, wanted)
        for t in wanted:
            boxes = {k: "missing" for k in CAM_ORDER}
            srcs: list[str] = []
            if wx:
                boxes["gen"] = wx.get("gen") or "missing"
                srcs.append(wx.get("file") or "01_daily/weather")
            fv = fvs.get(t) or {}
            sector = (fv.get("Sector") or fv.get("sector") or
                      (joins.get(t) or {}).get("sector") or "")
            if sector and wx.get("sectors"):
                boxes["sector"] = wx["sectors"].get(sector) or "missing"
            j = joins.get(t)
            if j:
                srcs.append(f"data/join/{date}_ranked.csv")
                boxes["join"] = _tone_num(j.get("total_score") or j.get("score_norm"))
            if fv:
                srcs.append(f"data/exports/finviz_{date}.csv")
                rel = fm._finite(fv.get("Relative Volume") or fv.get("Rel Volume"))
                if rel is not None:
                    boxes["vol"] = _tone_num(float(rel) - 1.0, 0.3)
                title = (fv.get("News Title") or "").strip()
            else:
                title = ""
            prev = prior_fv.get(t) or {}
            ch = prev.get("Change") or prev.get("Change from Open")
            if ch not in (None, ""):
                try:
                    boxes["yday"] = _tone_num(float(str(ch).replace("%", "").replace(",", "")))
                except (TypeError, ValueError):
                    pass
            news_tone = "missing"
            if title:
                news_tone = "neutral"
                boxes["news"] = "neutral"
            rsi = None
            try:
                rsi = float(str(fv.get("RSI (14)") or fv.get("RSI") or "").replace(",", ""))
            except (TypeError, ValueError):
                rsi = None
            n_pos, n_neg = _tally_boxes(boxes)
            out[(date, t)] = {
                "date": date,
                "boxes": boxes,
                "condition": {"good": n_pos, "bad": n_neg},
                "sources": srcs,
                "e_pol": "missing",
                "headline": title,
                "news": {"title": title, "tone": news_tone} if title else {},
                "rsi": rsi,
            }
        prior_fv = fvs or prior_fv
    return out


def build_history(probe: dict, mornings: dict, cal: list[str],
                  px_book: dict, lookback: dict | None = None) -> dict[str, list]:
    """Every session since 8/13. Off-list days still get reconstructed cameras."""
    lookback = lookback or {}
    tickers = sorted({
        t for m in (probe or {}).values() for t in (m or {})
    })
    out: dict[str, list] = {t: [] for t in tickers}
    for date in cal:
        morn = mornings.get(date) or {}
        s = morn.get("s")
        try:
            hard = bool(morn.get("hard_red") or (
                s is not None and float(s) <= float(fmb.HARD_RED)))
        except (TypeError, ValueError):
            hard = False
        cards = probe.get(date) or {}
        for t in tickers:
            card = cards.get(t)
            recon = None if card else lookback.get((date, t))
            move = day_move(px_book, t, date)
            row = {
                "date": date,
                "s": s,
                "hard_red": hard,
                "on_list": bool(card),
                "reconstructed": bool(recon) and not card,
                "n_pos": int((card or {}).get("cond_good") or 0) if card else None,
                "n_neg": int((card or {}).get("cond_bad") or (card or {}).get("n_neg") or 0) if card else None,
                "cams": None,
                "boxes": (card or {}).get("boxes") or {},
                "e_pol": (card or {}).get("e_pol"),
                "e_label": (card or {}).get("e_label"),
                "r_pol": (card or {}).get("r_pol"),
                "r_label": (card or {}).get("r_label"),
                "news": ((card or {}).get("news") or {}).get("title") or (card or {}).get("headline"),
                "news_tone": ((card or {}).get("news") or {}).get("tone"),
                "sources": (card or {}).get("sources") or [],
                "files": (card or {}).get("files") or [],
                "rsi": (card or {}).get("rsi"),
                "macd_hist": (card or {}).get("macd_hist"),
                "open": move["open"],
                "close": move["close"],
                "day_pct": move["day_pct"],
            }
            if card:
                row["cams"] = f"+{row['n_pos']} −{row['n_neg']}"
                pack = idio_score(card)
                row["idio"] = pack["score"]
                row["why"] = why_still(card, pack, side="long", hard=hard, s=s)
            elif recon:
                fill = row_from_lookback(recon)
                for k in ("n_pos", "n_neg", "cams", "boxes", "e_pol", "e_label",
                          "r_pol", "r_label", "news", "news_tone", "sources",
                          "files", "rsi", "macd_hist", "reconstructed"):
                    row[k] = fill.get(k)
                mini = {
                    "cond_good": row["n_pos"] or 0,
                    "cond_bad": row["n_neg"] or 0,
                    "boxes": row["boxes"],
                    "e_pol": row.get("e_pol") or "missing",
                    "news": {"tone": row.get("news_tone") or "missing"},
                    "on_list": False,
                }
                pack = idio_score(mini)
                row["idio"] = pack["score"]
                row["why"] = [
                    "Reconstructed 09:30 state from join / weather / Finviz / news "
                    "(name was not on that morning's shopping lists).",
                ] + why_still(mini, pack, side="long", hard=hard, s=s)
            for h in HOLDS:
                hp = horizon_pack(px_book, cal, t, date, h, "long")
                row[f"h{h}"] = hp.get("pct")
                row[f"h{h}_px"] = hp.get("px")
                row[f"h{h}_date"] = hp.get("date")
            out[t].append(row)
    return out


def run(*, panel: dict | None = None, probe: dict | None = None,
        mornings: dict | None = None, yahoo: bool = True,
        lookback: bool = True) -> dict:
    if panel is None:
        if not fm.PANEL_PATH.is_file():
            return {"ok": False, "error": "no panel.json"}
        panel = json.loads(fm.PANEL_PATH.read_text(encoding="utf-8"))
    probe = probe if probe is not None else fmp.build_probe(panel)
    mornings = mornings if mornings is not None else fmp.build_mornings()
    cal = list(panel.get("session_dates") or [])
    px_book = px_book_from_panel(panel)
    tickers = sorted({
        t for m in (probe or {}).values() for t in (m or {})
    })
    if yahoo and cal:
        px_book = fill_yahoo(px_book, tickers, cal[0], cal[-1])
    lb_map = {}
    if lookback and cal:
        lb_map = load_lookback(tickers, cal[0], cal[-1], cal=cal)
    days = []
    for date in cal or sorted(probe):
        morn = mornings.get(date) or {}
        s = morn.get("s")
        try:
            hard = bool(morn.get("hard_red") or (
                s is not None and float(s) <= float(fmb.HARD_RED)))
        except (TypeError, ValueError):
            hard = False
        cards = probe.get(date) or {}
        days.append(rank_day(
            date, cards, s=s, hard=hard, cal=cal, bars=None, px_book=px_book,
        ))
    bt = backtest(days)
    latest = next((d for d in reversed(days) if d.get("n_cards")), None)
    history = build_history(probe, mornings, cal, px_book, lookback=lb_map)
    return {
        "ok": True,
        "generated_at": datetime.now(tl.ET).isoformat(),
        "from_date": cal[0] if cal else None,
        "to_date": cal[-1] if cal else None,
        "n_days": len(days),
        "top_n": TOP_N,
        "cam_order": list(CAM_ORDER),
        "cam_short": dict(CAM_SHORT),
        "tickers": tickers,
        "live_sit_untouched": True,
        "backtest": bt,
        "latest": latest,
        "days": days,
        "history": history,
        "note": (
            "Live sleeves still SIT on S≤−3. Search a ticker for every "
            "session since 8/13: cameras (+N −N), 09:30 open, close, "
            "same-day % and hold-1/3/5 with actual prices."
        ),
    }



def write_md(doc: dict) -> Path:
    bt = doc.get("backtest") or {}
    latest = doc.get("latest") or {}
    lines = [
        "# Hard-red exceptions — daily investigator list",
        "",
        doc.get("note") or "",
        "",
        f"Generated {doc.get('generated_at')} · "
        f"{doc.get('from_date')} → {doc.get('to_date')} · "
        f"live sit **untouched**.",
        "",
        "## Latest morning",
        "",
        f"- Date **{latest.get('date')}** S={latest.get('s')} "
        f"hard-red={latest.get('hard_red')} · "
        f"{latest.get('n_cards')} investigator cards · "
        f"{latest.get('n_long_ok')} long-ok · {latest.get('n_short_ok')} short-ok",
        "",
        "### Longs weather sat on (top)",
        "",
    ]
    for rec in latest.get("longs") or []:
        lines.append(
            f"- **{rec['ticker']}** idio {rec['idio']:+d} · "
            f"+{rec['n_pos']} −{rec['n_neg']} · E {rec.get('e_pol')} · "
            f"H1 {rec.get('h1')}% H3 {rec.get('h3')}%"
        )
    lines += [
        "",
        "### Shorts weather sat on (top)",
        "",
    ]
    for rec in latest.get("shorts") or []:
        lines.append(
            f"- **{rec['ticker']}** idio {rec['idio']:+d} · "
            f"+{rec['n_pos']} −{rec['n_neg']} · H1 {rec.get('h1')}%"
        )
    def _bt(lab, pack):
        if not pack or not pack.get("n"):
            return f"- {lab}: no graded names yet"
        win = pack.get("win")
        return (
            f"- {lab}: n={pack['n']} win="
            f"{None if win is None else round(100*win, 1)}% "
            f"mean {pack.get('mean')}%"
        )
    lines += [
        "",
        "## If we had taken them on hard-red sits (research)",
        "",
        f"- Hard-red mornings in window: {bt.get('n_hard_red')}",
        _bt("long hold-1", bt.get("long_h1")),
        _bt("long hold-3", bt.get("long_h3")),
        _bt("short hold-1", bt.get("short_h1")),
        _bt("short hold-3", bt.get("short_h3")),
        "",
        "KEEP still wants >55% after fees and n≥30. Thin n is not a wire.",
        "",
        f"Dashboard: [hard-red-exceptions](../dashboard/hard-red-exceptions/).",
        "",
    ]
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    return OUT_MD


def write_json(doc: dict) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    hist = doc.get("history") or {}
    tick_dir = DASH_DIR / "t"
    tick_dir.mkdir(parents=True, exist_ok=True)
    for t, rows in hist.items():
        (tick_dir / f"{t}.json").write_text(
            json.dumps({
                "ticker": t,
                "from_date": doc.get("from_date"),
                "to_date": doc.get("to_date"),
                "cam_order": doc.get("cam_order") or list(CAM_ORDER),
                "cam_short": doc.get("cam_short") or dict(CAM_SHORT),
                "rows": rows,
            }, default=str),
            encoding="utf-8",
        )
    slim = {k: v for k, v in doc.items() if k != "history"}
    days = []
    latest_date = (doc.get("latest") or {}).get("date")
    for d in doc.get("days") or []:
        row = dict(d)
        if d.get("date") != latest_date:
            row.pop("all_top", None)
        days.append(row)
    slim["days"] = days
    path = OUT_DIR / "latest.json"
    text = json.dumps(slim, indent=2, default=str)
    path.write_text(text, encoding="utf-8")
    (DASH_DIR / "latest.json").write_text(text, encoding="utf-8")
    (DASH_DIR / "tickers.json").write_text(json.dumps({
        "tickers": doc.get("tickers") or [],
        "from_date": doc.get("from_date"),
        "to_date": doc.get("to_date"),
        "n_days": doc.get("n_days"),
        "cam_order": doc.get("cam_order") or list(CAM_ORDER),
        "cam_short": doc.get("cam_short") or dict(CAM_SHORT),
    }), encoding="utf-8")
    return path



def write_html() -> Path:
    """HTML is the checked-in dashboard file. Do not clobber it."""
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    dest = DASH_DIR / "index.html"
    return dest


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--write", action="store_true")
    p.add_argument("--no-yahoo", action="store_true")
    p.add_argument("--no-lookback", action="store_true")
    args = p.parse_args(argv)
    doc = run(yahoo=not args.no_yahoo, lookback=not args.no_lookback)
    if args.write and doc.get("ok"):
        write_json(doc)
        write_md(doc)
        write_html()
        print("wrote", OUT_DIR / "latest.json", "latest",
              (doc.get("latest") or {}).get("date"),
              "longs", (doc.get("latest") or {}).get("n_long_ok"))
    else:
        print(json.dumps({
            "ok": doc.get("ok"),
            "latest": (doc.get("latest") or {}).get("date"),
            "hard_red": (doc.get("latest") or {}).get("hard_red"),
            "n_long_ok": (doc.get("latest") or {}).get("n_long_ok"),
            "backtest": doc.get("backtest"),
            "error": doc.get("error"),
        }, indent=2, default=str))
    return 0 if doc.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
