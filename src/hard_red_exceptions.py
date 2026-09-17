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


def _finite_open(row) -> float | None:
    if not isinstance(row, dict):
        return None
    return fm._finite(row.get("open"))


def _cam_pair(row) -> tuple[int, int] | None:
    """(n_pos, n_neg) when both are numeric. 0 is a real count; None is missing."""
    if not isinstance(row, dict):
        return None
    pos = fm._finite(row.get("n_pos"))
    neg = fm._finite(row.get("n_neg"))
    if pos is None or neg is None:
        return None
    return int(pos), int(neg)


def _open_window(rows: list, i: int, n: int = 5) -> list[float] | None:
    """Today-first list of the last n numeric opens (skip rows without an open)."""
    opens: list[float] = []
    for j in range(i, -1, -1):
        o = _finite_open(rows[j] if j < len(rows) else None)
        if o is None:
            continue
        opens.append(float(o))
        if len(opens) == n:
            return opens
    return None


def open_camera_setup(rows: list, i: int) -> dict | None:
    """LONG / SHORT open+camera badge for row i, or None.

    LONG: today's open is lowest or 2nd-lowest among the last 5 numeric
    opens (today included) AND net = n_pos − n_neg rose vs the prior
    session. SHORT is the inverse (highest / 2nd-highest + net down).

    CLEAN: net moved the right way and the other color did not
    deteriorate (long: n_neg did not rise, n_pos did not fall).
    EXPLORE: net moved the right way but the other color did
    (e.g. +1 −1 → +4 −2).

    Needs a 5-open window and a prior session with camera counts.
    Matches dashboard setupOf(rows, i).
    """
    if not rows or i < 1 or i >= len(rows):
        return None
    opens = _open_window(rows, i, 5)
    if not opens:
        return None
    cur = _cam_pair(rows[i])
    prev = _cam_pair(rows[i - 1])
    if cur is None or prev is None:
        return None
    uniq = sorted(set(opens))
    lowest, highest = uniq[0], uniq[-1]
    second_low = uniq[1] if len(uniq) > 1 else lowest
    second_high = uniq[-2] if len(uniq) > 1 else highest
    today_o = opens[0]
    cheap = today_o == lowest or today_o == second_low
    rich = today_o == highest or today_o == second_high
    net = cur[0] - cur[1]
    net_prev = prev[0] - prev[1]
    d_net = net - net_prev
    side = quality = rank = None
    if cheap and d_net > 0:
        side = "long"
        rank = "lowest" if today_o == lowest else "second_lowest"
        clean = cur[1] <= prev[1] and cur[0] >= prev[0]
        quality = "clean" if clean else "explore"
    elif rich and d_net < 0:
        side = "short"
        rank = "highest" if today_o == highest else "second_highest"
        clean = cur[0] <= prev[0] and cur[1] >= prev[1]
        quality = "clean" if clean else "explore"
    else:
        return None
    return {
        "side": side,
        "quality": quality,
        "open_rank": rank,
        "n_pos": cur[0],
        "n_neg": cur[1],
        "n_pos_prev": prev[0],
        "n_neg_prev": prev[1],
        "net": net,
        "net_prev": net_prev,
        "net_delta": d_net,
        "tight_long": (
            side == "long" and quality == "clean"
            and rank == "lowest" and d_net >= 2
        ),
    }


def _horizon_side_pct(row: dict, hold: int, side: str) -> float | None:
    raw = fm._finite((row or {}).get(f"h{hold}"))
    if raw is None:
        return None
    pct = float(raw)
    if side == "short":
        pct = -pct
    return round(pct, 2)


def _setup_bucket_key(setup: dict) -> str:
    return f"{setup['side']}_{setup['quality']}"


def _empty_setup_stats() -> dict:
    keys = (
        "long_clean", "long_explore", "long_all",
        "short_clean", "short_explore", "short_all",
        "long_tight",
    )
    out = {}
    for k in keys:
        out[k] = {
            "n": 0,
            "h1": {"n": 0, "win": None, "mean": None},
            "h3": {"n": 0, "win": None, "mean": None},
            "h5": {"n": 0, "win": None, "mean": None},
        }
    return out


def _fill_setup_stats(stats: dict) -> dict:
    for pack in stats.values():
        for h in (1, 3, 5):
            slot = pack[f"h{h}"]
            xs = slot.pop("_xs", [])
            slot.update(_pack_rets(xs))
    return stats


def _note_setup(stats: dict, setup: dict, row: dict, *, tight: bool = False) -> None:
    keys = [_setup_bucket_key(setup), f"{setup['side']}_all"]
    if tight:
        keys.append("long_tight")
    for key in keys:
        pack = stats[key]
        pack["n"] += 1
        for h in (1, 3, 5):
            pct = _horizon_side_pct(row, h, setup["side"])
            if pct is None:
                continue
            slot = pack[f"h{h}"]
            slot.setdefault("_xs", []).append(pct)


def scan_open_camera_setups(tick_dir: Path | None = None) -> dict:
    """Honest H1/H3/H5 backtest of open+camera setups on checked-in histories."""
    tick_dir = Path(tick_dir or (DASH_DIR / "t"))
    all_s = _empty_setup_stats()
    red_s = _empty_setup_stats()
    n_files = 0
    n_rows = 0
    n_setups = 0
    from_date = to_date = None
    for path in sorted(tick_dir.glob("*.json")):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        rows = doc.get("rows") or []
        if not rows:
            continue
        n_files += 1
        n_rows += len(rows)
        fd, td = doc.get("from_date"), doc.get("to_date")
        if fd and (from_date is None or fd < from_date):
            from_date = fd
        if td and (to_date is None or td > to_date):
            to_date = td
        for i, row in enumerate(rows):
            setup = open_camera_setup(rows, i)
            if not setup:
                continue
            n_setups += 1
            _note_setup(all_s, setup, row, tight=bool(setup.get("tight_long")))
            if row.get("hard_red"):
                _note_setup(red_s, setup, row, tight=bool(setup.get("tight_long")))
    _fill_setup_stats(all_s)
    _fill_setup_stats(red_s)
    return {
        "n_histories": n_files,
        "n_rows": n_rows,
        "n_setups": n_setups,
        "from_date": from_date,
        "to_date": to_date,
        "all": all_s,
        "hard_red": red_s,
        "note": (
            "Long near 50% H1 is not an edge. Shorts are graded as short "
            "P&L (− stored long hold). Thin n is said out loud."
        ),
    }


def _fmt_win(pack: dict) -> str:
    if not pack or not pack.get("n"):
        return "—"
    win = pack.get("win")
    mean = pack.get("mean")
    win_s = "—" if win is None else f"{100 * win:.1f}%"
    mean_s = "—" if mean is None else f"{mean:+.2f}%"
    return f"{win_s} / {mean_s} (n={pack['n']})"


def format_setup_backtest_md(doc: dict) -> str:
    labels = (
        ("long_clean", "long clean"),
        ("long_explore", "long explore"),
        ("long_all", "long all"),
        ("short_clean", "short clean"),
        ("short_explore", "short explore"),
        ("short_all", "short all"),
        ("long_tight", "tighter long (lowest + clean + net ≥ +2)"),
    )

    def table(title: str, stats: dict) -> list[str]:
        lines = [
            f"## {title}",
            "",
            "| sleeve | n setups | H1 win / mean | H3 win / mean | H5 win / mean |",
            "|---|---:|---|---|---|",
        ]
        for key, lab in labels:
            pack = stats.get(key) or {}
            lines.append(
                f"| {lab} | {pack.get('n', 0)} | "
                f"{_fmt_win(pack.get('h1'))} | "
                f"{_fmt_win(pack.get('h3'))} | "
                f"{_fmt_win(pack.get('h5'))} |"
            )
        lines.append("")
        return lines

    n_hist = doc.get("n_histories") or 0
    thin = n_hist < 30 or (doc.get("n_setups") or 0) < 30
    lines = [
        "# Open + camera setup backtest",
        "",
        doc.get("note") or "",
        "",
        f"Scanned **{n_hist}** ticker histories "
        f"(`dashboard/hard-red-exceptions/t/*.json`) · "
        f"{doc.get('from_date')} → {doc.get('to_date')} · "
        f"{doc.get('n_rows')} session rows · "
        f"{doc.get('n_setups')} setups.",
        "",
        "Rule (same as `setupOf` / `open_camera_setup`): 5 numeric opens "
        "including today; LONG if cheapest or 2nd-cheapest **and** camera "
        "net rose vs prior session; SHORT if richest or 2nd-richest **and** "
        "net fell. Clean = the other color did not deteriorate. Explore = "
        "net moved the right way but positives/negatives moved against the "
        "side. No badge without a 5-open window and a prior camera session.",
        "",
        "Short P&L flips the stored long hold-1/3/5. KEEP still wants "
        ">55% after fees and n≥30. "
        + ("**Sample is thin — do not wire.**" if thin else
           "Sample is large enough to read the rates; still not a live wire."),
        "",
    ]
    lines += table("ALL days", doc.get("all") or {})
    lines += table("HARD-RED-ONLY days", doc.get("hard_red") or {})
    lines += [
        "## Honesty",
        "",
        "- Do not dress up a long H1 near 50% as edge.",
        "- Prior recon (~1247 histories) was long ~49% H1, short ~56% H1 "
        "(explore short ~60% H1; hard-red shorts H5 ~63.9% +2.12). "
        "This scan is the same 1247 files through 2026-09-16: long all H1 "
        "47.9% (still a coin-flip, a point worse), short all H1 56.3% "
        "(unchanged), explore short H1 61.1% (unchanged), hard-red short "
        "H5 66.3% +2.66 (a couple points stronger — more hard-red "
        "sessions in the 9/8–9/15 tail). The table above is the source "
        "of truth.",
        "- Tighter long is a cheap extra cut (lowest open, not 2nd; clean; "
        "net improved by ≥2), reported separately so it is not p-hacked "
        "into the main rule.",
        "",
        "Dashboard: [hard-red-exceptions](./index.html).",
        "",
    ]
    return "\n".join(lines)


def write_setup_backtest_md(doc: dict | None = None,
                            dest: Path | None = None) -> Path:
    dest = dest or (DASH_DIR / "SETUP_BACKTEST.md")
    payload = doc or scan_open_camera_setups()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(format_setup_backtest_md(payload), encoding="utf-8")
    return dest


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


def _ab_path(date: str) -> Path:
    for name in (f"{date}_ab_slim.csv", f"{date}_ab_checklist_enriched.csv",
                 f"{date}_ab_checklist.csv"):
        p = ROOT / "data" / "ab_checklist" / name
        if p.is_file():
            return p
    return ROOT / "data" / "ab_checklist" / f"{date}_ab_slim.csv"


def build_skinny_index(wanted: set[str], cal: list[str]) -> dict:
    """Same session objects ticker-lookback paints, without 11k-row Finviz maps."""
    sessions = []
    for d in cal:
        book_json = {}
        pjson = ROOT / "data" / "stock_book" / f"{d}_stock_book.json"
        if pjson.is_file():
            try:
                book_json = json.loads(pjson.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                book_json = {}
        buys, sells = {}, {}
        for _h, entry in (book_json.get("books") or {}).items():
            for i, r in enumerate(entry.get("buy") or [], 1):
                t = fm._tick(r.get("ticker"))
                if t in wanted:
                    buys.setdefault(t, {})[_h] = {
                        "rank": i, "score": r.get("score"), "reasons": r.get("reasons"),
                    }
            for i, r in enumerate(entry.get("sell") or [], 1):
                t = fm._tick(r.get("ticker"))
                if t in wanted:
                    sells.setdefault(t, {})[_h] = {"rank": i, "score": r.get("score")}
        join_map = _csv_wanted(ROOT / "data" / "join" / f"{d}_ranked.csv", wanted)
        fv_map = _csv_wanted(ROOT / "data" / "exports" / f"finviz_{d}.csv", wanted)
        ab_map = _csv_wanted(_ab_path(d), wanted)
        peer_map = _csv_wanted(ROOT / "data" / "peers" / f"{d}_peer_rs.csv", wanted)
        univ_map = _csv_wanted(ROOT / "data" / "universe" / f"{d}_membership.csv", wanted)
        book_map = _csv_wanted(ROOT / "data" / "stock_book" / f"{d}_stock_book.csv", wanted)
        cat_path = ROOT / "01_daily" / "catalyst" / f"{d}_dossiers.json"
        catalyst_map = {}
        if cat_path.is_file():
            try:
                payload = json.loads(cat_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload = {}
            for r in payload.get("dossiers") or []:
                t = fm._tick((r or {}).get("ticker"))
                if t in wanted:
                    catalyst_map[t] = r
        sessions.append({
            "date": d,
            "has": {
                "book": bool(book_map), "join": bool(join_map),
                "finviz": bool(fv_map), "ab": bool(ab_map),
                "peer": bool(peer_map), "universe": bool(univ_map),
                "quote_colors": False, "catalyst": bool(catalyst_map),
                "green": False,
            },
            "n_book": len(book_map), "n_join": len(join_map),
            "n_finviz": len(fv_map), "n_ab": len(ab_map), "n_peer": len(peer_map),
            "book": book_map, "join": join_map, "finviz": fv_map, "ab": ab_map,
            "peer": peer_map, "universe": univ_map, "quote_colors": {},
            "catalyst": catalyst_map, "buys": buys, "sells": sells,
            "green_buy": set(), "live_buy": set(),
            "green_meta": {},
        })
    for i, sess in enumerate(sessions):
        sess["prior"] = sessions[i - 1] if i else None
        sess["prior_date"] = sessions[i - 1]["date"] if i else None
    return {"sessions": sessions, "paper": {}, "dates": list(cal), "preopen": {}}


def px_book_official(tickers: list[str], cal: list[str]) -> dict:
    """09:30 open / 16:00 close from ohlc.parquet, then same-dated Finviz
    only when Prev Close matches the prior official close (completed tape).
    Never loads the 11k-row Finviz maps into ticker_lookback's cache.
    """
    names = [fm._tick(t) for t in tickers if fm._tick(t)]
    wanted = set(names)
    book: dict[str, dict] = {}
    try:
        bars = tl._ohlc_bars()
    except Exception:
        bars = None
    prior_c: dict[str, float] = {}
    for d in cal:
        fvs = _csv_wanted(ROOT / "data" / "exports" / f"finviz_{d}.csv", wanted)
        for t in names:
            o = c = None
            if bars is not None and not getattr(bars, "empty", True):
                try:
                    off = tl._official_ohlc(t, d, bars)
                    o, c = off.get("open"), off.get("close")
                except Exception:
                    pass
            fv = fvs.get(t) or {}
            if o is None or c is None:
                prev = tl._num(fv.get("Prev Close") or fv.get("Previous Close"))
                fo = tl._num(fv.get("Open"))
                fc = tl._num(fv.get("Price"))
                if prev is not None and t in prior_c and tl._px_matches(prev, prior_c[t]):
                    o = o if o is not None else fo
                    c = c if c is not None else fc
            if o is None and c is None:
                continue
            slot = book.setdefault(t, {}).setdefault(d, {})
            if o is not None:
                slot["o"] = float(o)
            if c is not None:
                slot["c"] = float(c)
                prior_c[t] = float(c)
    return book


def extend_cal(cal: list[str]) -> list[str]:
    """Panel sessions plus later trading days we can price (H1/H5 tails)."""
    extra: list[str] = []
    try:
        extra = list(tl.session_dates() or [])
    except Exception:
        extra = []
    try:
        bars = tl._ohlc_bars()
        if bars is not None and not getattr(bars, "empty", True):
            idx = bars.index
            if getattr(idx, "nlevels", 1) >= 1:
                extra.extend(str(x) for x in idx.get_level_values(0).unique())
    except Exception:
        pass
    start = cal[0] if cal else "2026-08-13"
    out = sorted({d for d in list(cal) + extra if d and str(d) >= start and tl.is_trading_date(d)})
    return out or list(cal)


def merge_px(base: dict, extra: dict) -> dict:
    for t, days in (extra or {}).items():
        for d, slot in (days or {}).items():
            dst = base.setdefault(t, {}).setdefault(d, {})
            if slot.get("o") is not None:
                dst["o"] = float(slot["o"])
            if slot.get("c") is not None:
                dst["c"] = float(slot["c"])
    return base


def load_lookback(tickers: list[str], from_date: str | None,
                  to_date: str | None, cal: list[str] | None = None) -> dict:
    """Real 09:30 cameras via ticker_lookback_cli._scan_session.

    Skinny index keeps only tracked names so we do not load 11k-row
    Finviz dumps the way build_index() does.
    """
    from . import ticker_lookback_cli as scan
    wanted = {fm._tick(t) for t in tickers if fm._tick(t)}
    dates = list(cal or [])
    if not dates:
        return {}
    skinny = build_skinny_index(wanted, dates)
    tl._INDEX = skinny
    out: dict[tuple[str, str], dict] = {}
    by_date = {s["date"]: s for s in skinny["sessions"]}
    for date in dates:
        sess = by_date.get(date)
        if not sess:
            continue
        for t in wanted:
            try:
                card = scan._scan_session(sess, t)
            except Exception:
                card = None
            if not card:
                continue
            boxes = dict(card.get("boxes") or {})
            ch = ((card.get("finviz") or {}).get("change_pct"))
            if ch is not None and boxes.get("yday") in (None, "missing"):
                boxes["yday"] = tl._polarity(ch)
            n_pos, n_neg = _tally_boxes(boxes)
            cond = tl.general_condition(boxes) if hasattr(tl, "general_condition") else {}
            if cond:
                n_pos = int(cond.get("good") or n_pos)
                n_neg = int(cond.get("bad") or n_neg)
            srcs = list(card.get("sources") or [])
            arts = card.get("artifacts_that_day") or {}
            if isinstance(arts, dict):
                srcs.extend(k for k, v in arts.items() if v)
            news = (tl.preopen_packet(date, prior_date=sess.get("prior_date")).get("news") or {}).get(t) or {}
            title = ""
            if isinstance(news, dict):
                ev = news.get("events") or []
                if ev:
                    title = str((ev[0] if isinstance(ev[0], dict) else {"t": ev[0]}).get("title") or ev[0])[:160]
            headline = card.get("reasons") or title
            out[(date, t)] = {
                "date": date,
                "boxes": boxes,
                "condition": {"good": n_pos, "bad": n_neg},
                "sources": srcs,
                "e_pol": "missing",
                "headline": headline,
                "news": {"title": headline, "tone": boxes.get("news") or "missing"},
                "rsi": None,
                "signals": card.get("signals") or {},
                "class": card.get("class"),
            }
    return out


def build_history(probe: dict, mornings: dict, cal: list[str],
                  px_book: dict, lookback: dict | None = None,
                  px_cal: list[str] | None = None) -> dict[str, list]:
    """Every session since 8/13. Off-list days still get reconstructed cameras."""
    lookback = lookback or {}
    px_cal = list(px_cal or cal)
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
                    "Reconstructed 09:30 state from ticker-lookback cameras "
                    "(join / predict / AB / peer / news / digest — not on shopping lists).",
                ] + why_still(mini, pack, side="long", hard=hard, s=s)
            for h in HOLDS:
                hp = horizon_pack(px_book, px_cal, t, date, h, "long")
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
    px_cal = extend_cal(cal)
    px_book = merge_px(px_book, px_book_official(tickers, px_cal))
    if yahoo and px_cal:
        px_book = fill_yahoo(px_book, tickers, px_cal[0], px_cal[-1])
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
            date, cards, s=s, hard=hard, cal=px_cal, bars=None, px_book=px_book,
        ))
    bt = backtest(days)
    latest = next((d for d in reversed(days) if d.get("n_cards")), None)
    history = build_history(
        probe, mornings, cal, px_book, lookback=lb_map, px_cal=px_cal,
    )
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
    try:
        from . import day_movers as dm
        dm.write_from_hre(doc, tick_dir=tick_dir)
    except Exception as exc:
        print(f"[hard-red] WARN: day-movers emit: {exc}", flush=True)
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
    p.add_argument("--setup-backtest", action="store_true",
                   help="Scan t/*.json open+camera setups; write SETUP_BACKTEST.md")
    args = p.parse_args(argv)
    if args.setup_backtest:
        bt = scan_open_camera_setups()
        dest = write_setup_backtest_md(bt)
        print(json.dumps({
            "ok": True,
            "wrote": str(dest),
            "n_histories": bt.get("n_histories"),
            "n_setups": bt.get("n_setups"),
            "all": bt.get("all"),
            "hard_red": bt.get("hard_red"),
        }, indent=2, default=str))
        return 0
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
