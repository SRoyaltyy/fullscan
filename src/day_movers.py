"""Day movers — per-session top gainers / losers for Pages.

One page, two clocks, never mixed:

  INTERDAY (legal at 09:30): prior close → today's open (gap %).
  Fallback prior close → today's close if open is missing.
  Never ranked by same-day Change% / o→c.

  INTRADAY (leak): open → close (o→c). Not knowable at 09:30.

Rows come from ``dashboard/hard-red-exceptions/t/*.json``. Official
OHLC / Finviz fill missing prior-close and high/low only — they are
not an interday rank key. No new market-fetch tree.

Does not change live sit / Webull / flatten_robust.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from . import factor_mine as fm
from . import hard_red_exceptions as hre
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
HRE_DASH = ROOT / "dashboard" / "hard-red-exceptions"
DASH_DIR = ROOT / "dashboard" / "day-movers"
TOP_N = 25
HOT4 = "union_hot_n4_h1"
FLATTEN = "flatten_robust"
INTRADAY_NOTE = "same-session close — not knowable at 09:30."


def _finite(v):
    return fm._finite(v)


def _tick(v) -> str:
    return fm._tick(v) or ""


def _round(v, n: int = 4):
    x = _finite(v)
    return None if x is None else round(float(x), n)


def oc_pct(open_, close) -> float | None:
    """INTRADAY leak: open → close."""
    o, c = _finite(open_), _finite(close)
    if o is None or c is None or o == 0:
        return None
    return round(100.0 * (float(c) / float(o) - 1.0), 2)


def gap_pct(prior_close, open_) -> float | None:
    """INTERDAY legal at 09:30: prior close → today's open."""
    pc, o = _finite(prior_close), _finite(open_)
    if pc is None or o is None or pc == 0:
        return None
    return round(100.0 * (float(o) / float(pc) - 1.0), 2)


def px_comparable(a, b, max_ratio: float = 4.0) -> bool:
    """Drop split / unadjusted-print pairs (BYND 0.41 → 12.45)."""
    x, y = _finite(a), _finite(b)
    if x is None or y is None or x <= 0 or y <= 0:
        return False
    return max(x, y) / min(x, y) <= float(max_ratio)


def interday_move(prior_close, open_, close=None) -> tuple[float | None, str | None]:
    """Prefer gap. Fallback prior close → close when open is missing.

    Never returns same-day Change% / o→c as the interday rank.
    Split-incompatible prior closes are not a tradeable gap.
    """
    if px_comparable(prior_close, open_):
        g = gap_pct(prior_close, open_)
        if g is not None:
            return g, "gap"
    if px_comparable(prior_close, close):
        pc, c = _finite(prior_close), _finite(close)
        if pc is not None and c is not None and pc != 0:
            return round(100.0 * (float(c) / float(pc) - 1.0), 2), "cc"
    return None, None


def rank_side(rows: list[dict], *, side: str, key: str = "pct",
              n: int = TOP_N) -> list[dict]:
    xs = [r for r in rows if _finite(r.get(key)) is not None]
    if side == "losers":
        xs.sort(key=lambda r: (_finite(r[key]), r.get("t") or ""))
    else:
        xs.sort(key=lambda r: (-(_finite(r[key]) or 0.0), r.get("t") or ""))
    return xs[: int(n)]


def prior_session(cal: list[str], date: str) -> str | None:
    earlier = [d for d in cal if d and d < date]
    return earlier[-1] if earlier else None


def lag_banner(hre_to: str | None) -> str | None:
    if not hre_to:
        return None
    today = datetime.now(tl.ET).date().isoformat()
    live = hre_to
    try:
        sess = [d for d in (tl.session_dates() or []) if d and d <= today]
        if sess:
            live = sess[-1]
    except Exception:
        live = today
    if live and live > hre_to:
        return f"HRE last session is {hre_to}; live tape is {live}."
    return None


def load_hot4_by_date() -> dict[str, set[str]]:
    """union_hot_n4_h1 would-buy that morning. Soft-miss OK."""
    out: dict[str, set[str]] = {}
    try:
        if fm.PANEL_PATH.is_file():
            panel = fm.rehydrate_panel(
                json.loads(fm.PANEL_PATH.read_text(encoding="utf-8")))
            rec = fm.make_recipe(
                name=HOT4, universe="union", hold=1, top_n=4,
                rank="hot_score", forbid={"alarm": True},
            )
            for date, rows in (panel.get("by_date") or {}).items():
                ticks = {
                    _tick(r.get("ticker"))
                    for r in fm.pick_day(list(rows or []), rec)
                    if _tick(r.get("ticker"))
                }
                if ticks:
                    out[str(date)[:10]] = ticks
    except Exception:
        pass
    _overlay_today_strategy(out, HOT4)
    return out


def load_flatten_by_date() -> dict[str, set[str]]:
    """flatten_robust would-buy that morning. Soft-miss OK."""
    out: dict[str, set[str]] = {}
    path = ROOT / "03_scoreboard" / "flatten_lookback_action.json"
    try:
        if path.is_file():
            doc = json.loads(path.read_text(encoding="utf-8"))
            for day in doc.get("daily") or []:
                d = str(day.get("date") or "")[:10]
                if not d:
                    continue
                ticks = {
                    _tick(t) for t in (day.get("tickers") or []) if _tick(t)
                }
                if ticks:
                    out[d] = ticks
    except Exception:
        pass
    _overlay_today_strategy(out, FLATTEN)
    return out


def _overlay_today_strategy(out: dict[str, set[str]], name: str) -> None:
    path = ROOT / "dashboard" / "factor-mine" / "today_strategies.json"
    try:
        if not path.is_file():
            return
        doc = json.loads(path.read_text(encoding="utf-8"))
        date = str(doc.get("date") or "")[:10]
        rec = (doc.get("strategies") or {}).get(name) or {}
        ticks = {
            _tick(r.get("ticker"))
            for r in (rec.get("buy") or [])
            if _tick(r.get("ticker"))
        }
        if date and ticks:
            out.setdefault(date, set()).update(ticks)
    except Exception:
        pass


def _load_official_bars():
    try:
        bars = tl._ohlc_bars()
    except Exception:
        return None
    if bars is None or getattr(bars, "empty", True):
        return None
    return bars


def _ohlc_slot(bars, ticker: str, date: str) -> dict:
    empty = {"open": None, "high": None, "low": None, "close": None}
    if bars is None:
        return empty
    try:
        return tl._official_ohlc(ticker, date, bars) or empty
    except Exception:
        return empty


def _finviz_ohlc(date: str, wanted: set[str]) -> dict[str, dict]:
    """OHLC fallback from the same-day Finviz dump. Not an interday rank."""
    if not date or not wanted:
        return {}
    recs = hre._csv_wanted(
        ROOT / "data" / "exports" / f"finviz_{date}.csv", wanted)
    out: dict[str, dict] = {}
    for t, rec in recs.items():
        out[t] = {
            "open": _finite(rec.get("Open")),
            "high": _finite(rec.get("High")),
            "low": _finite(rec.get("Low")),
            "close": _finite(rec.get("Price") or rec.get("Close")),
            "prev": _finite(rec.get("Prev Close") or rec.get("Previous Close")),
        }
    return out


def _setup_bits(setup: dict | None) -> tuple[str | None, str | None]:
    if not setup:
        return None, None
    side = str(setup.get("side") or "")
    qual = str(setup.get("quality") or "")
    su = {"long": "l", "short": "s"}.get(side)
    sq = {"clean": "c", "explore": "e"}.get(qual)
    return su, sq


def slim_row(ticker: str, row: dict, *, pct: float, px_ref,
             kind: str, setup: dict | None, hot4: bool, flatten: bool,
             high, low, prior_c, oc, gap) -> dict:
    su, sq = _setup_bits(setup)
    srcs = [str(x) for x in (row.get("sources") or []) if x][:8]
    return {
        "t": ticker,
        "pct": pct,
        "o": _round(row.get("open")),
        "x": _round(px_ref),
        "c": _round(row.get("close")),
        "pc": _round(prior_c),
        "h": _round(high),
        "l": _round(low),
        "s": row.get("s"),
        "hr": bool(row.get("hard_red")),
        "np": row.get("n_pos"),
        "nn": row.get("n_neg"),
        "idio": row.get("idio"),
        "src": srcs,
        "h1": row.get("h1"),
        "h3": row.get("h3"),
        "h5": row.get("h5"),
        "hot4": bool(hot4),
        "fl": bool(flatten),
        "su": su,
        "sq": sq,
        "k": kind,
        "oc": oc,
        "gap": gap,
    }


def _iter_histories(tick_dir: Path):
    for path in sorted(Path(tick_dir).glob("*.json")):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        ticker = _tick(doc.get("ticker") or path.stem)
        rows = doc.get("rows") or []
        if not ticker or not rows:
            continue
        yield ticker, rows


def pack_from_histories(
    histories: list[tuple[str, list]],
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    hot4: dict[str, set[str]] | None = None,
    flatten: dict[str, set[str]] | None = None,
    px_fallback: dict[tuple[str, str], dict] | None = None,
    top_n: int = TOP_N,
) -> dict:
    """Rank each session. ``histories`` is [(ticker, rows), ...]."""
    hot4 = hot4 or {}
    flatten = flatten or {}
    px_fallback = px_fallback or {}
    by_date: dict[str, list[dict]] = {}
    cal: set[str] = set()
    for ticker, rows in histories:
        for i, row in enumerate(rows):
            date = str((row or {}).get("date") or "")[:10]
            if not date:
                continue
            if from_date and date < from_date:
                continue
            if to_date and date > to_date:
                continue
            cal.add(date)
            prior = rows[i - 1] if i else {}
            prior_c = _finite((prior or {}).get("close"))
            fb = px_fallback.get((ticker, date)) or {}
            if prior_c is None:
                prior_c = _finite(fb.get("prev"))
            high = _finite(row.get("high"))
            low = _finite(row.get("low"))
            if high is None:
                high = _finite(fb.get("high"))
            if low is None:
                low = _finite(fb.get("low"))
            open_ = _finite(row.get("open"))
            close = _finite(row.get("close"))
            if open_ is None:
                open_ = _finite(fb.get("open"))
            if close is None:
                close = _finite(fb.get("close"))
            oc = oc_pct(open_, close)
            gap = gap_pct(prior_c, open_)
            inter, inter_k = interday_move(prior_c, open_, close)
            setup = hre.open_camera_setup(rows, i)
            by_date.setdefault(date, []).append({
                "ticker": ticker,
                "row": row,
                "open": open_,
                "close": close,
                "prior_c": prior_c,
                "high": high,
                "low": low,
                "oc": oc,
                "gap": gap,
                "inter": inter,
                "inter_k": inter_k,
                "setup": setup,
            })
    dates = sorted(cal)
    days = []
    for date in dates:
        cands = by_date.get(date) or []
        s = None
        hard = False
        for rec in cands:
            if rec["row"].get("s") is not None:
                s = rec["row"].get("s")
            if rec["row"].get("hard_red"):
                hard = True
                break
        if s is None and cands:
            s = cands[0]["row"].get("s")
        open_univ = [rec for rec in cands if _finite(rec["open"]) is not None]
        intra_rows = []
        for rec in open_univ:
            if rec["oc"] is None:
                continue
            intra_rows.append(slim_row(
                rec["ticker"], rec["row"], pct=rec["oc"],
                px_ref=rec["close"], kind="oc", setup=rec["setup"],
                hot4=rec["ticker"] in (hot4.get(date) or ()),
                flatten=rec["ticker"] in (flatten.get(date) or ()),
                high=rec["high"], low=rec["low"], prior_c=rec["prior_c"],
                oc=rec["oc"], gap=rec["gap"],
            ))
        inter_rows = []
        for rec in cands:
            # Universe is a numeric open that morning. Close-to-close
            # fallback is only for names whose open is missing.
            if rec["open"] is None and rec["inter_k"] != "cc":
                continue
            if rec["inter"] is None:
                continue
            # Never rank INTERDAY by same-day o→c / Change%.
            if rec["inter_k"] not in ("gap", "cc"):
                continue
            px_ref = rec["prior_c"] if rec["inter_k"] == "gap" else rec["close"]
            inter_rows.append(slim_row(
                rec["ticker"], rec["row"], pct=rec["inter"],
                px_ref=px_ref, kind=rec["inter_k"], setup=rec["setup"],
                hot4=rec["ticker"] in (hot4.get(date) or ()),
                flatten=rec["ticker"] in (flatten.get(date) or ()),
                high=rec["high"], low=rec["low"], prior_c=rec["prior_c"],
                oc=rec["oc"], gap=rec["gap"],
            ))
        days.append({
            "date": date,
            "s": s,
            "hard_red": hard,
            "n_open": len(open_univ),
            "intraday": {
                "n": len(intra_rows),
                "gainers": rank_side(intra_rows, side="gainers", n=top_n),
                "losers": rank_side(intra_rows, side="losers", n=top_n),
            },
            "interday": {
                "n": len(inter_rows),
                "gainers": rank_side(inter_rows, side="gainers", n=top_n),
                "losers": rank_side(inter_rows, side="losers", n=top_n),
            },
        })
    return {
        "ok": True,
        "from_date": dates[0] if dates else from_date,
        "to_date": dates[-1] if dates else to_date,
        "n_days": len(days),
        "top_n": int(top_n),
        "dates": dates,
        "days": days,
        "intraday_note": INTRADAY_NOTE,
        "clocks": {
            "interday": "prior close → today's open (gap %). "
                        "Fallback prior close → close if open missing. "
                        "Legal at 09:30. Never ranked by same-day Change%.",
            "intraday": INTRADAY_NOTE,
        },
    }


def _session_cal(dates: list[str]) -> list[str]:
    """HRE dates plus the trading day before the window (8/13 → 8/12)."""
    cal = list(dates)
    try:
        extra = [d for d in (tl.session_dates() or []) if d]
        prefix = [d for d in extra if dates and d < dates[0]]
        if prefix:
            cal = [prefix[-1]] + [d for d in dates]
    except Exception:
        pass
    return cal


def _build_px_fallback(histories, dates: list[str]) -> dict[tuple[str, str], dict]:
    """Official bars, then Finviz, for missing prior-close / high / low."""
    out: dict[tuple[str, str], dict] = {}
    if not histories or not dates:
        return out
    bars = _load_official_bars()
    cal = _session_cal(dates)
    need_fv: dict[str, set[str]] = {}
    for ticker, rows in histories:
        by = {str(r.get("date") or "")[:10]: r for r in rows}
        for i, date in enumerate(dates):
            prev_date = dates[i - 1] if i else prior_session(cal, date)
            slot = out.setdefault((ticker, date), {})
            off = _ohlc_slot(bars, ticker, date)
            for k in ("high", "low", "open", "close"):
                if off.get(k) is not None:
                    slot.setdefault(k, off[k])
            prev_row = by.get(prev_date or "")
            if prev_row and _finite(prev_row.get("close")) is not None:
                slot["prev"] = _finite(prev_row.get("close"))
            elif prev_date:
                prev_off = _ohlc_slot(bars, ticker, prev_date)
                if prev_off.get("close") is not None:
                    slot["prev"] = prev_off["close"]
            if (slot.get("prev") is None or slot.get("high") is None
                    or slot.get("low") is None):
                need_fv.setdefault(date, set()).add(ticker)
    for date, ticks in need_fv.items():
        fv = _finviz_ohlc(date, ticks)
        for t, rec in fv.items():
            slot = out.setdefault((t, date), {})
            if slot.get("prev") is None and rec.get("prev") is not None:
                slot["prev"] = rec["prev"]
            for k in ("high", "low", "open", "close"):
                if slot.get(k) is None and rec.get(k) is not None:
                    slot[k] = rec[k]
    return out


def bake(*, tick_dir: Path | None = None, meta: dict | None = None,
         top_n: int = TOP_N, membership: bool = True,
         px_fill: bool = True) -> dict:
    tick_dir = Path(tick_dir or (HRE_DASH / "t"))
    meta = dict(meta or {})
    if not meta and (HRE_DASH / "tickers.json").is_file():
        try:
            meta.update(json.loads(
                (HRE_DASH / "tickers.json").read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            pass
    histories = list(_iter_histories(tick_dir))
    from_date = meta.get("from_date")
    to_date = meta.get("to_date")
    dates = sorted({
        str(r.get("date") or "")[:10]
        for _, rows in histories for r in rows
        if r.get("date")
        and (not from_date or str(r.get("date"))[:10] >= from_date)
        and (not to_date or str(r.get("date"))[:10] <= to_date)
    })
    fb = _build_px_fallback(histories, dates) if px_fill else {}
    hot4 = load_hot4_by_date() if membership else {}
    flatten = load_flatten_by_date() if membership else {}
    doc = pack_from_histories(
        histories, from_date=from_date, to_date=to_date,
        hot4=hot4, flatten=flatten, px_fallback=fb, top_n=top_n,
    )
    doc["generated_at"] = datetime.now(tl.ET).isoformat()
    doc["hre_from"] = meta.get("from_date") or doc.get("from_date")
    doc["hre_to"] = meta.get("to_date") or doc.get("to_date")
    doc["n_tickers"] = len(histories)
    doc["lag"] = lag_banner(doc.get("hre_to"))
    doc["live_sit_untouched"] = True
    return doc


def write_days(doc: dict | None = None, *, tick_dir: Path | None = None,
               meta: dict | None = None) -> Path:
    if doc is None:
        doc = bake(tick_dir=tick_dir, meta=meta)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    dest = DASH_DIR / "days.json"
    dest.write_text(json.dumps(doc, default=str), encoding="utf-8")
    return dest


def write_from_hre(hre_doc: dict, *, tick_dir: Path | None = None) -> Path:
    """Small emitter hooked from the HRE bake. Reads t/*.json already written."""
    meta = {
        "from_date": hre_doc.get("from_date"),
        "to_date": hre_doc.get("to_date"),
        "generated_at": hre_doc.get("generated_at"),
    }
    return write_days(tick_dir=tick_dir or (HRE_DASH / "t"), meta=meta)


def write_html() -> Path:
    """HTML is the checked-in dashboard file. Do not clobber it."""
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    return DASH_DIR / "index.html"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--write", action="store_true")
    p.add_argument("--no-membership", action="store_true")
    p.add_argument("--no-px-fill", action="store_true")
    args = p.parse_args(argv)
    doc = bake(membership=not args.no_membership, px_fill=not args.no_px_fill)
    if args.write and doc.get("ok"):
        dest = write_days(doc)
        write_html()
        first = (doc.get("days") or [{}])[0]
        ig = ((first.get("interday") or {}).get("gainers") or [{}])[0]
        ag = ((first.get("intraday") or {}).get("gainers") or [{}])[0]
        print(
            "wrote", dest,
            "dates", doc.get("from_date"), "→", doc.get("to_date"),
            "n_days", doc.get("n_days"),
            "interday#1", ig.get("t"), ig.get("pct"),
            "intraday#1", ag.get("t"), ag.get("pct"),
        )
        return 0
    print(json.dumps({
        "ok": doc.get("ok"),
        "from_date": doc.get("from_date"),
        "to_date": doc.get("to_date"),
        "n_days": doc.get("n_days"),
        "lag": doc.get("lag"),
    }, indent=2, default=str))
    return 0 if doc.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
