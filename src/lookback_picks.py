"""Book picks slice for lookback: every buy/sell name and the layer scores that day.

Also backfills 2d/3d/1w forward returns via yfinance for every name already
on the report (cards, 1d winners, buy/sell picks) when the local parquet
has not caught up.

Run after src.book_lookback:
  python -m src.lookback_picks --date 2026-08-20
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from .book_learn import _load_panel, load_frame
from .book_lookback import (
    BOX_COLS,
    BOOK_DIR,
    DAILY,
    LOOK_H,
    SCORE,
    _boxes,
    _catalyst,
    _digest_hits,
    _finviz_row,
    _frame_row,
    _fwd_map,
    _heat_hits,
    _icon,
    _inventory,
    _jload,
    _judge_tilt,
    _missing_layers,
    _news_actions,
    _parse_date,
    _signals,
    _tick,
)

YF_H = {"1d": 1, "2d": 2, "3d": 3, "1w": 5}


def _yf_forwards(date: str, tickers: list[str]) -> dict[str, dict[str, float | None]]:
    tickers = sorted({_tick(t) for t in tickers if _tick(t)})
    blank = {h: None for h in YF_H}
    out = {t: dict(blank) for t in tickers}
    if not tickers:
        return out
    try:
        import yfinance as yf
    except ImportError:
        print("[lookback-picks] yfinance missing — longer horizons stay n/a")
        return out
    start = datetime.fromisoformat(date).date()
    end = start + timedelta(days=18)
    try:
        data = yf.download(
            tickers=tickers,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            group_by="ticker",
            auto_adjust=True,
            threads=True,
            progress=False,
        )
    except Exception as e:
        print(f"[lookback-picks] yfinance failed: {e}")
        return out
    if data is None or data.empty:
        return out

    def closes_of(t: str) -> pd.Series:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if t not in data.columns.get_level_values(0):
                    return pd.Series(dtype=float)
                return data[t]["Close"].dropna()
            return data["Close"].dropna() if len(tickers) == 1 else pd.Series(dtype=float)
        except Exception:
            return pd.Series(dtype=float)

    for t in tickers:
        close = closes_of(t)
        if close.empty or len(close) < 2:
            continue
        entry = float(close.iloc[0])
        if not entry:
            continue
        for h, n in YF_H.items():
            if len(close) > n:
                out[t][h] = float(close.iloc[n]) / entry - 1.0
            elif h == "1w" and len(close) >= 2:
                out[t][h] = float(close.iloc[-1]) / entry - 1.0
    print(f"[lookback-picks] yfinance forwards for {sum(1 for t in out if out[t].get('1d') is not None)}/{len(tickers)} names")
    return out


def _merge_fwd(dst: dict, yf: dict[str, dict], ticker: str) -> dict:
    cur = dict(dst or {})
    got = yf.get(_tick(ticker)) or {}
    for h in YF_H:
        if cur.get(h) is None and got.get(h) is not None:
            cur[h] = round(float(got[h]) * 100, 2)
    return cur


def _num(x, nd=2) -> str:
    try:
        return f"{float(x):+.{nd}f}"
    except (TypeError, ValueError):
        return "—"


def _book_of(r: dict, h: str = "1d") -> float | None:
    if r.get("book_score") is not None:
        try:
            return float(r["book_score"])
        except (TypeError, ValueError):
            pass
    s = r.get("signals") or {}
    for k in (f"score_{h}", "score_1d", "score_3d", "score_1w"):
        if s.get(k) not in (None, 0, 0.0):
            try:
                return float(s[k])
            except (TypeError, ValueError):
                continue
    for k in ("score_1d", "score_3d", "score_1w"):
        if s.get(k) is not None:
            try:
                return float(s[k])
            except (TypeError, ValueError):
                continue
    return None


def book_picks(date, frame, book, panel, missing):
    out = {}
    if not book:
        return out
    for h, entry in (book.get("books") or {}).items():
        sides = {}
        for side in ("buy", "sell"):
            rows = []
            for i, rec in enumerate(entry.get(side) or [], 1):
                t = _tick(rec.get("ticker"))
                fr = _frame_row(frame, t)
                ev = {
                    "news_actions": _news_actions(date, t),
                    "digest": _digest_hits(date, t),
                    "judge_tilt": _judge_tilt(date, t),
                    "finviz": _finviz_row(date, t),
                    "heat": _heat_hits(date, t),
                    "catalyst": _catalyst(date, t),
                }
                sig = _signals(fr)
                try:
                    score = round(float(rec.get("score") or 0), 3)
                except (TypeError, ValueError):
                    score = 0.0
                fwd = _fwd_map(panel, date, t)
                rows.append({
                    "rank": i,
                    "ticker": t,
                    "side": side,
                    "book_score": score,
                    "sector": rec.get("sector") or (None if fr is None else fr.get("sector")),
                    "size": rec.get("size") or (None if fr is None else fr.get("size")),
                    "reasons": rec.get("reasons"),
                    "signals": sig,
                    "boxes": _boxes(sig, ev, side == "buy", missing),
                    "fwd_pct": {
                        k: (None if v is None else round(v * 100, 2))
                        for k, v in fwd.items()
                    },
                })
            sides[side] = rows
        out[h] = sides
    return out


def _fmt(v) -> str:
    return "n/a" if v is None else f"{v:+.1f}%"


def render_picks(picks: dict) -> list[str]:
    if not picks:
        return ["## Book picks — scores that day", "", "_No stock_book.json for this date._", ""]
    L = [
        "## Book picks — scores that day",
        "",
        "What the ranker actually put in buy/sell, with every layer score and the same color boxes.",
        "`book` is the combined ranker score. 1d/2d/3d/1w are realized close-to-close after the signal.",
        "1w uses the last available close if a full 5 sessions are not in yet.",
        "",
    ]
    order = [h for h in ("1d", "3d", "1w", "2w", "1m") if h in picks]
    for extra in picks:
        if extra not in order:
            order.append(extra)
    for h in order:
        sides = picks.get(h) or {}
        L.append(f"### {h} book")
        L.append("")
        for side in ("buy", "sell"):
            rows = sides.get(side) or []
            L.append(f"**{side.upper()}** ({len(rows)})")
            L.append("")
            if not rows:
                L.append("_empty_")
                L.append("")
                continue
            L.append("| # | Ticker | book | join | sect | gen | news | AB | peer | heat | 1d | 2d | 3d | 1w |")
            L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
            for r in rows:
                fp = r.get("fwd_pct") or {}
                s = r.get("signals") or {}
                L.append(
                    f"| {r['rank']} | {r['ticker']} | {r['book_score']:+.3f} | "
                    f"{_num(s.get('s_join'))} | {_num(s.get('s_sector'))} | "
                    f"{_num(s.get('s_general'))} | {_num(s.get('s_news'))} | "
                    f"{_num(s.get('s_ab'))} | {_num(s.get('s_peer'))} | "
                    f"{_num(s.get('s_heat'))} | "
                    f"{_fmt(fp.get('1d'))} | {_fmt(fp.get('2d'))} | "
                    f"{_fmt(fp.get('3d'))} | {_fmt(fp.get('1w'))} |"
                )
            L.append("")
            heads = " | ".join(["Ticker"] + [lab for _, lab in BOX_COLS])
            L.append("| " + heads + " |")
            L.append("|" + "|".join(["---"] * (1 + len(BOX_COLS))) + "|")
            for r in rows:
                cells = [_icon((r.get("boxes") or {}).get(k, "missing")) for k, _ in BOX_COLS]
                L.append(f"| {r['ticker']} | " + " | ".join(cells) + " |")
            L.append("")
            L.append("| Ticker | size | sector | reasons |")
            L.append("|--------|------|--------|---------|")
            for r in rows:
                why = str(r.get("reasons") or "").replace("|", "/")
                L.append(
                    f"| {r['ticker']} | {r.get('size') or '?'} | {r.get('sector') or '?'} | `{why}` |"
                )
            L.append("")
        break
    return L


def render_later(winners: dict) -> list[str]:
    rows = list(winners.get("1d") or [])
    if not rows:
        return []
    L = [
        "## Same 1d movers — book score + later sessions",
        "",
        "`book` is that day's ranker score (`score_1d` in the CSV). Buys started around +0.94.",
        "A ripper with book +0.40 was seen and ranked too low. A ripper with book — was not scored.",
        "",
        "| Ticker | class | book | join | sect | gen | news | AB | peer | heat | 1d | 2d | 3d | 1w |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        fp = r.get("fwd_pct") if isinstance(r.get("fwd_pct"), dict) else {"1d": r.get("fwd_pct")}
        s = r.get("signals") or {}
        book = _book_of(r, "1d")
        L.append(
            f"| {r['ticker']} | {r.get('class')} | {_num(book, 3)} | "
            f"{_num(s.get('s_join'))} | {_num(s.get('s_sector'))} | "
            f"{_num(s.get('s_general'))} | {_num(s.get('s_news'))} | "
            f"{_num(s.get('s_ab'))} | {_num(s.get('s_peer'))} | "
            f"{_num(s.get('s_heat'))} | "
            f"{_fmt(fp.get('1d', r.get('fwd_pct') if not isinstance(r.get('fwd_pct'), dict) else None))} | "
            f"{_fmt(fp.get('2d'))} | {_fmt(fp.get('3d'))} | {_fmt(fp.get('1w'))} |"
        )
    L.append("")
    return L


def _attach_scores(rows: list, frame) -> None:
    if frame is None:
        return
    for r in rows:
        if r.get("signals"):
            continue
        fr = _frame_row(frame, _tick(r.get("ticker")))
        r["signals"] = _signals(fr)
        if r.get("book_score") is None:
            r["book_score"] = (r["signals"] or {}).get("score_1d")


def run(date: str | None = None) -> dict:
    date = _parse_date(date)
    frame = load_frame(date)
    book = _jload("data", "stock_book", f"{date}_stock_book.json")
    panel = _load_panel()
    missing = _missing_layers(_inventory(date))
    picks = book_picks(date, frame, book, panel, missing)

    js = BOOK_DIR / f"{date}_lookback.json"
    payload = {}
    if js.exists():
        try:
            payload = json.loads(js.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {"date": date}
    winners = payload.get("winners") or {}
    cards = payload.get("cards") or []

    for rows in winners.values():
        _attach_scores(rows, frame)
    _attach_scores(cards, frame)

    need = set()
    for sides in picks.values():
        for rows in (sides or {}).values():
            for r in rows:
                need.add(r["ticker"])
    for rows in winners.values():
        for r in rows:
            need.add(_tick(r.get("ticker")))
    for c in cards:
        need.add(_tick(c.get("ticker")))
    yf = _yf_forwards(date, list(need))

    for sides in picks.values():
        for rows in (sides or {}).values():
            for r in rows:
                r["fwd_pct"] = _merge_fwd(r.get("fwd_pct"), yf, r["ticker"])
    for rows in winners.values():
        for r in rows:
            t = _tick(r.get("ticker"))
            fp = r.get("fwd_pct") if isinstance(r.get("fwd_pct"), dict) else {"1d": r.get("fwd_pct")}
            r["fwd_pct"] = _merge_fwd(fp, yf, t)
    for c in cards:
        c["fwd_pct"] = _merge_fwd(c.get("fwd_pct"), yf, c.get("ticker"))

    payload["picks"] = picks
    payload["winners"] = winners
    payload["cards"] = cards
    BOOK_DIR.mkdir(parents=True, exist_ok=True)
    js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    block = "\n".join(render_picks(picks) + render_later(winners)) + "\n"
    for path in (SCORE / "BOOK_LOOKBACK.md", DAILY / f"{date}_lookback.md"):
        if path.exists():
            text = path.read_text(encoding="utf-8")
            if "## Book picks" in text:
                pre, _mid, rest = text.partition("## Book picks")
                cut = rest.find("\n## Files")
                tail = rest[cut:] if cut >= 0 else ""
                text = pre.rstrip() + "\n\n" + block + tail.lstrip("\n")
            elif "## Same 1d" in text:
                pre, _mid, rest = text.partition("## Same 1d")
                cut = rest.find("\n## Files")
                tail = rest[cut:] if cut >= 0 else ""
                text = pre.rstrip() + "\n\n" + block + tail.lstrip("\n")
            elif "## Files" in text:
                text = text.replace("## Files", block + "## Files", 1)
            else:
                text = text.rstrip() + "\n\n" + block
            path.write_text(text, encoding="utf-8")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# Book lookback — " + date + "\n\n" + block, encoding="utf-8")
    print(block[:5000])
    print(f"[lookback-picks] wrote picks + ripper book scores for {date}")
    return picks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    run(date=args.date)


if __name__ == "__main__":
    main()
