"""Flatten-method lookback — cameras / setups / 09:30 action on live picks.

For every session since the dashboard start (2026-08-13):

  * take the names ``flatten_robust`` would buy that day (3d robust
    size-book wish-list, or ranked mover BUYs when flatten_ok)
  * also collect that day's liquid top gainers, priced mover BUYs,
    and liquid top losers so the Action / phone page can toggle
    Flatten | Gainers | Movers | Losers | Captured | Probable | Custom
    and tally how many of each tape flatten chose — plus a leak-free
    capture watchlist (prior tape + earnings reaction + morning BUYs)
    and a compact probable-ripper list (yesterday's non-exploded
    liquid gainers)
  * stamp the specialized red:green / volume / candlestick factor
    (prior sessions only) so we can capture would-be top gainers
    without buying them
  * stamp Finviz chart **E / R / D** (earnings, analyst ratings,
    dividends) knowable by 09:30 — same markers as the quote chart
  * paint ticker lookback (12 cameras + yΔ, 6 coaches, 🔵/🚨/⚪,
    featured setups, 09:30 BUY / SELL / NO BUY / HOLD)

Same-day Change% only picks the gainer / loser / tape-mover
universes and grades flatten win/lose. Flatten picks come from
the prior book + morning S + priced mover BUYs (leak-free).
Today's unprinted book never colors cameras or the flatten gate.

CLI: python -m src.flatten_lookback_action --write
"""
from __future__ import annotations

import argparse
import html
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from . import candle_factor as cf
from . import finviz_events as fe
from . import gainer_asof as ga
from . import gainer_capture as gc
from . import gainer_lookback_action as gla
from . import lookback_action as act
from . import sleeve_merge as sm
from . import ticker_lookback as tl
from . import ticker_lookback_run as run
from . import ticker_lookback_setups as setups

ROOT = Path(__file__).resolve().parent.parent
OUT_MD = ROOT / "03_scoreboard" / "FLATTEN_LOOKBACK_ACTION.md"
OUT_JSON = ROOT / "03_scoreboard" / "flatten_lookback_action.json"
OUT_HTML = ROOT / "dashboard" / "flatten-lookback" / "index.html"
DAILY_MD = ROOT / "01_daily" / "flatten_lookback_action.md"

START = ga.START
HORIZONS = act.HORIZONS
CAMERA_COLS = run.CAMERA_COLS
DOMAIN_COLS = run.DOMAIN_COLS
SOURCES = ("flatten", "gainers", "movers", "losers", "captured",
           "probable", "custom")
UNIVERSES = ("flatten", "gainers", "movers", "losers", "captured",
             "probable")


def _tick(v) -> str:
    return str(v or "").strip().upper()


def _calls_by_day(payload: dict) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for raw in payload.get("called_rows") or []:
        rec = dict(raw)
        rec["_conv"] = sm._conviction(rec)
        out[rec.get("date")].append(rec)
    return out


def _priced_buys(rows: list[dict], date: str) -> list[dict]:
    priced = []
    for r in rows:
        if r.get("action_call") != "BUY":
            continue
        t = _tick(r.get("ticker"))
        px = sm._num(((r.get("session_bar") or sm._bar(t, date)) or {}).get("open"))
        if t and px:
            priced.append(r)
    return priced


def flatten_day_targets(date: str, payload: dict | None = None,
                        books: list | None = None,
                        pol: dict | None = None,
                        book_map: dict | None = None,
                        cal: list[str] | None = None) -> dict:
    """Would-buy / wish-list for one session. No cash, no fills.

    Same leak-free gate as ``run_flatten_switch``: green S, ≥5 priced
    mover BUYs, and a book printed before 09:30. Hard-red sits but the
    3d robust wish-list is still returned (not sent live).
    """
    payload = payload if payload is not None else sm.load_payload()
    books = books if books is not None else sm.list_books()
    pol = dict(pol or sm.live_policy())
    book_map = book_map if book_map is not None else sm.load_book_map(books)
    cal = cal if cal is not None else sm.session_calendar(payload, books)

    g = (payload.get("regime") or {}).get(date) or {}
    score = g.get("predict_score")
    calls = _calls_by_day(payload)
    priced_buys = _priced_buys(calls.get(date) or [], date)
    today_book = book_map.get(date)
    book_mode = pol.get("book_for_flatten", "yesterday")
    prior = sm._prior_book(book_map, cal, date, book_mode)
    last_print = sm._prior_book(book_map, cal, date, "last")
    min_buys = int(pol.get("min_buys", 5))
    have_buys = len(priced_buys) >= min_buys
    green = score is not None and float(score) >= float(pol.get("long_gate", 1.0))
    hard_red_cut = float(pol.get("hard_red", -3.0))
    hard_red = score is not None and float(score) <= hard_red_cut
    flatten_ok = green and have_buys and (
        book_mode in ("none", None, False) or prior is not None)
    if pol.get("hard_red_no_new") and hard_red:
        flatten_ok = False

    ranked = sm.rank_calls(priced_buys, pol.get("long_rank", "cond"))
    mover_names = [_tick(r.get("ticker")) for r in ranked if r.get("ticker")]
    mover_picks = [t for t in mover_names if t][: int(pol.get("long_top_n") or 10)]

    io_book = today_book
    if io_book is None and pol.get("carry_last_book"):
        io_book = last_print
    io_picks = sm.io_select_picks(
        io_book or {}, pol, date=date, score=score,
        mover_buys=mover_names,
        top_n=int(pol.get("long_top_n") or 10),
    ) if io_book is not None else []
    from . import ohlc_ripper as ohlc
    prior_d = gc.prior_session(cal, date)
    ripper_n = int(pol.get("ripper_top_n") or 0)
    ripper_picks = ohlc.continuation(prior_d, date, top_n=ripper_n) if ripper_n else []

    if flatten_ok:
        route = "mover"
        tickers = list(mover_picks)
        sleeve = "mover_long"
        clock = sm.OPEN_CLOCK
        why = (f"flatten .io → mover at 09:30 "
               f"(S={float(score):+.2f}, {len(priced_buys)} priced BUYs)")
    else:
        route = ("hold" if (pol.get("hard_red_no_new") and hard_red
                            and not pol.get("hard_red_io_ok"))
                 else "io")
        tickers = list(io_picks)
        sleeve = "io_core"
        clock = sm.CLOSE_CLOCK
        bits = []
        if hard_red:
            bits.append(f"hard-red S={float(score):+.2f} sit")
        elif score is None:
            bits.append("blank S")
        else:
            bits.append(f"S={float(score):+.2f}")
        bits.append(
            f"no flatten ({len(priced_buys)} priced BUYs, "
            f"prior book={'yes' if prior is not None else 'no'})")
        bits.append(f"wish-list {pol.get('io_select', 'size')}:"
                    f"{pol.get('io_sleeve', '2w_size')}")
        why = "; ".join(bits)
    if ripper_picks:
        extra = [t for t in ripper_picks if t not in tickers]
        tickers = list(tickers) + extra
        if extra:
            why = (why + f"; +{len(extra)} OHLC continuation at 09:30")

    return {
        "date": date,
        "score": score,
        "route": route,
        "flatten_ok": bool(flatten_ok),
        "hard_red": bool(hard_red),
        "n_priced_buys": len(priced_buys),
        "have_prior_book": prior is not None,
        "have_today_book": today_book is not None,
        "tickers": tickers,
        "io_picks": list(io_picks),
        "mover_picks": list(mover_picks),
        "ripper_picks": list(ripper_picks),
        "sleeve": sleeve,
        "clock": clock,
        "why": why,
        "policy": pol.get("name") or sm.LIVE_POLICY,
    }


def collect_flatten(from_date: str = START, to_date: str | None = None,
                    payload: dict | None = None,
                    books: list | None = None,
                    pol: dict | None = None) -> dict:
    payload = payload if payload is not None else sm.load_payload()
    books = books if books is not None else sm.list_books()
    pol = dict(pol or sm.live_policy())
    book_map = sm.load_book_map(books)
    cal = [d for d in sm.session_calendar(payload, books)
           if d >= from_date and (not to_date or d <= to_date)]
    by_date: dict[str, dict] = {}
    names: set[str] = set()
    keys: set[tuple[str, str]] = set()
    for date in cal:
        plan = flatten_day_targets(
            date, payload=payload, books=books, pol=pol,
            book_map=book_map, cal=sm.session_calendar(payload, books),
        )
        by_date[date] = plan
        for t in plan["tickers"]:
            names.add(t)
            keys.add((date, t))
    return {
        "from_date": from_date,
        "to_date": to_date or (cal[-1] if cal else from_date),
        "policy": pol.get("name") or sm.LIVE_POLICY,
        "n_sessions": len(cal),
        "n_pick_days": len(keys),
        "n_tickers": len(names),
        "by_date": by_date,
        "keys": keys,
        "tickers": sorted(names),
        "session_dates": cal,
    }


def collect_mover_buys(payload: dict, from_date: str, to_date: str | None,
                       top_n: int = 25) -> dict:
    """Priced BUY calls flatten actually sees — not the 2k-name universe."""
    calls = _calls_by_day(payload)
    dates = sorted(
        d for d in calls
        if d >= from_date and (not to_date or d <= to_date)
    )
    by_date: dict[str, list[str]] = {}
    names: set[str] = set()
    keys: set[tuple[str, str]] = set()
    for date in dates:
        priced = _priced_buys(calls.get(date) or [], date)
        ranked = sm.rank_calls(priced, "cond")
        ticks = []
        for r in ranked:
            t = _tick(r.get("ticker"))
            if not t or t in ticks:
                continue
            ticks.append(t)
            if len(ticks) >= top_n:
                break
        by_date[date] = ticks
        for t in ticks:
            names.add(t)
            keys.add((date, t))
    return {
        "from_date": from_date,
        "to_date": to_date or (dates[-1] if dates else from_date),
        "n_sessions": len(dates),
        "n_mover_days": len(keys),
        "n_tickers": len(names),
        "by_date": by_date,
        "keys": keys,
        "tickers": sorted(names),
        "session_dates": dates,
        "top_n": top_n,
    }


def collect_losers(from_date: str = START, to_date: str | None = None,
                   top_n: int = gla.TOP_N) -> dict:
    return gla.collect_losers(from_date=from_date, to_date=to_date, top_n=top_n)


def _pattern_label(feat: dict) -> str:
    bits = []
    if feat.get("engulf_bull"):
        bits.append("engulf")
    if feat.get("hammer"):
        bits.append("hammer")
    if feat.get("morning_star"):
        bits.append("morning")
    if feat.get("three_green"):
        bits.append("3g")
    if feat.get("a15"):
        bits.append("A15")
    if feat.get("shooting_star"):
        bits.append("shoot")
    if feat.get("three_red"):
        bits.append("3r")
    if feat.get("engulf_bear"):
        bits.append("bear-engulf")
    if bits:
        return ",".join(bits)
    if feat.get("last_green"):
        return "green"
    if feat.get("last_red"):
        return "red"
    return "—"


def _attach_candle(rec: dict, date: str, ticker: str) -> dict:
    feat = cf.features(ticker, date)
    rec["candle_ok"] = bool(feat.get("ok"))
    rec["candle_body_rg"] = feat.get("body_rg")
    rec["candle_vol_rg"] = feat.get("vol_rg")
    rec["candle_score"] = feat.get("score")
    rec["candle_capture"] = bool(cf.capture(feat))
    rec["candle_last_green"] = bool(feat.get("last_green"))
    rec["candle_pattern"] = _pattern_label(feat)
    return rec


def _attach_events(rec: dict, date: str, ticker: str) -> dict:
    """Finviz chart E / R / D knowable by 09:30. Does not change the picker."""
    return fe.attach_row(rec, date, ticker)


def _chg_of(row: dict) -> float | None:
    for key in ("day_change", "gainer_change"):
        raw = row.get(key)
        if raw is None:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    oc = (row.get("session_bar") or {}).get("close_open_pct")
    if oc is None:
        return None
    try:
        return float(oc)
    except (TypeError, ValueError):
        return None


def _outcome(chg: float | None) -> str | None:
    if chg is None:
        return None
    if chg > 0:
        return "win"
    if chg < 0:
        return "lose"
    return "flat"


def paint_names(tickers: list[str], from_date: str, to_date: str | None,
                preset: str | None = None) -> dict:
    names = [_tick(t) for t in tickers if _tick(t)]
    if not names:
        return {"generated_at": datetime.now(tl.ET).isoformat(), "names": []}
    payload = run.scan_tickers(names, from_date=from_date, to_date=to_date)
    setups.attach_setups(payload)
    act.attach_actions(payload, params=act.preset_params(preset or None))
    return payload


def _day_index(payload: dict) -> dict[tuple[str, str], dict]:
    out = {}
    for rec in payload.get("names") or []:
        t = rec.get("ticker")
        for day in rec.get("days") or []:
            out[(day.get("date"), t)] = day
    return out


def _reused_cards() -> dict[tuple[str, str], dict]:
    """Sister lookback rows already stamped (cameras when the file kept them)."""
    out: dict[tuple[str, str], dict] = {}
    for path in (gla.OUT_JSON, sm.PAYLOAD):
        if not path.is_file():
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for key in ("gainer_rows", "called_rows", "rows"):
            for raw in doc.get(key) or []:
                t = _tick(raw.get("ticker"))
                d = raw.get("date")
                if t and d and (d, t) not in out:
                    out[(d, t)] = dict(raw)
    return out


def _stamp_row(row: dict, dates: list[str]) -> dict:
    packed = act.action_call(row) if not row.get("action_call") else None
    if packed:
        row["action_call"] = packed["action"]
        row["action_reason"] = packed["reason"]
    row["action_stamp"] = act.session_stamp(row.get("date"), act.OPEN_CLOCK)
    row["action_label"] = act.format_action(row.get("action_call"), row.get("date"))
    row["hits"] = act.grade_call(row.get("action_call") or "", gla._fwd(row))
    if not row.get("session_bar"):
        row["session_bar"] = tl.session_bar(row.get("ticker"), row.get("date"))
    if not row.get("horizon_dates"):
        row["horizon_dates"] = tl.horizon_dates(row.get("date"), dates)
    if not row.get("condition"):
        row["condition"] = tl.general_condition(row.get("boxes") or {})
    row["cond_tally"] = act.cond_tally(row)
    if not row.get("labeled"):
        row["labeled"] = ga._labeled(row.get("boxes"))
    if not row.get("labeled_domains"):
        row["labeled_domains"] = ga._labeled_domains(row.get("domains"))
    if not row.get("marks_cell"):
        row["marks_cell"] = ga._marks_cell(row.get("marks"))
    return row


def _attach_meta(card: dict, date: str, ticker: str, sources: set[str],
                 flatten_meta: dict, tape_raw: dict | None,
                 dates: list[str], day_change: float | None = None) -> dict:
    rec = dict(card)
    rec["ticker"] = ticker
    rec["date"] = date
    rec["sources"] = sorted(sources)
    plan = (flatten_meta.get("by_date") or {}).get(date) or {}
    rec["flatten_route"] = plan.get("route") or ""
    rec["flatten_score"] = plan.get("score")
    rec["flatten_ok"] = bool(plan.get("flatten_ok"))
    rec["flatten_why"] = plan.get("why") or ""
    rec["flatten_sleeve"] = plan.get("sleeve") or ""
    rec["flatten_rank"] = None
    picks = plan.get("tickers") or []
    if ticker in picks:
        rec["flatten_rank"] = picks.index(ticker) + 1
    if tape_raw:
        rec["gainer_change"] = tape_raw.get("change_pct")
        rec["gainer_rank"] = tape_raw.get("_rank")
        rec["sector"] = tape_raw.get("sector") or rec.get("sector")
        rec["tape_side"] = tape_raw.get("_side") or rec.get("tape_side")
    if day_change is not None:
        rec["day_change"] = day_change
    elif rec.get("gainer_change") is not None:
        rec["day_change"] = rec.get("gainer_change")
    rec["outcome"] = _outcome(_chg_of(rec))
    _attach_candle(rec, date, ticker)
    _attach_events(rec, date, ticker)
    return _stamp_row(rec, dates)


def walk(from_date: str = START, to_date: str | None = None,
         top_n: int = gla.TOP_N, min_change: float = gla.MIN_CHANGE,
         preset: str | None = None,
         extra_tickers: list[str] | None = None,
         source: str = "flatten") -> dict:
    payload_src = sm.load_payload()
    flatten_meta = collect_flatten(
        from_date=from_date, to_date=to_date, payload=payload_src)
    gainer_meta = gla.collect_gainers(
        from_date=flatten_meta["from_date"], to_date=flatten_meta["to_date"],
        top_n=top_n, min_change=min_change,
    )
    loser_meta = collect_losers(
        from_date=flatten_meta["from_date"], to_date=flatten_meta["to_date"],
        top_n=top_n,
    )
    for date, day_rows in (gainer_meta.get("by_date") or {}).items():
        for i, raw in enumerate(day_rows, 1):
            raw["_rank"] = i
            raw["_side"] = "gainer"
    for date, day_rows in (loser_meta.get("by_date") or {}).items():
        for i, raw in enumerate(day_rows, 1):
            raw["_rank"] = i
            raw["_side"] = "loser"
    mover_meta = collect_mover_buys(
        payload_src, flatten_meta["from_date"], flatten_meta["to_date"],
        top_n=top_n,
    )
    capture_meta = gc.collect_capture(
        flatten_meta["from_date"], flatten_meta["to_date"],
        flatten_meta["session_dates"],
        flatten_meta.get("by_date") or {},
        mover_meta.get("by_date") or {},
    )
    extra = [_tick(t) for t in (extra_tickers or []) if _tick(t)]
    # Paint flatten + custom. When the Action dropdown is a tape
    # universe, also paint that set so cameras exist for the tally.
    names = set(flatten_meta["tickers"]) | set(extra)
    src = (source or "flatten").lower()
    if src == "gainers":
        names |= set(gainer_meta.get("tickers") or [])
    elif src == "losers":
        names |= set(loser_meta.get("tickers") or [])
    elif src == "movers":
        names |= set(mover_meta.get("tickers") or [])
    elif src == "captured":
        names |= set(capture_meta.get("tickers") or [])
    elif src == "probable":
        for wl in (capture_meta.get("by_date") or {}).values():
            for rec in wl.get("rows") or []:
                if "probable" in (rec.get("reasons") or []):
                    names.add(_tick(rec.get("ticker")))
    names = sorted(names)
    painted = paint_names(
        names, flatten_meta["from_date"], flatten_meta["to_date"],
        preset=preset,
    )
    by_card = _day_index(painted)
    reused = _reused_cards()
    dates = flatten_meta["session_dates"]
    chg_maps = {d: ga._finviz_change_map(ga.load_finviz(d)) for d in dates}
    membership: dict[tuple[str, str], set[str]] = defaultdict(set)
    for date, plan in flatten_meta["by_date"].items():
        for t in plan.get("tickers") or []:
            membership[(date, t)].add("flatten")
    tape_raw: dict[tuple[str, str], dict] = {}
    for date, day_rows in (gainer_meta.get("by_date") or {}).items():
        for raw in day_rows:
            t = _tick(raw.get("ticker"))
            membership[(date, t)].add("gainers")
            tape_raw[(date, t)] = raw
    for date, day_rows in (loser_meta.get("by_date") or {}).items():
        for raw in day_rows:
            t = _tick(raw.get("ticker"))
            membership[(date, t)].add("losers")
            tape_raw.setdefault((date, t), raw)
    for date, ticks in (mover_meta.get("by_date") or {}).items():
        for t in ticks:
            membership[(date, t)].add("movers")
    capture_raw: dict[tuple[str, str], dict] = {}
    for date, wl in (capture_meta.get("by_date") or {}).items():
        for rec in wl.get("rows") or []:
            t = _tick(rec.get("ticker"))
            membership[(date, t)].add("captured")
            if "probable" in (rec.get("reasons") or []):
                membership[(date, t)].add("probable")
            capture_raw[(date, t)] = rec
    for t in extra:
        for date in dates:
            membership[(date, t)].add("custom")

    rows = []
    for (date, ticker), sources in sorted(membership.items()):
        card = (by_card.get((date, ticker))
                or reused.get((date, ticker))
                or {
                    "date": date, "ticker": ticker, "class": "no_data",
                    "boxes": {k: "missing" for k, _ in tl.BOX_COLS},
                })
        rec = _attach_meta(
            card, date, ticker, sources, flatten_meta,
            tape_raw.get((date, ticker)), dates,
            day_change=chg_maps.get(date, {}).get(ticker),
        )
        cap = capture_raw.get((date, ticker))
        if cap:
            rec["capture_reasons"] = cap.get("reasons") or []
            if rec.get("candle_capture") is None:
                rec["candle_capture"] = cap.get("candle_capture")
            for key in (
                "ohlc_ret_5", "ohlc_ret_10", "ohlc_rvol",
                "ohlc_nr7", "ohlc_break_10", "ohlc_hot_score",
            ):
                if cap.get(key) is not None:
                    rec[key] = cap.get(key)
        rows.append(rec)

    flatten_rows = [r for r in rows if "flatten" in r.get("sources", [])]
    recall_buy = sum(1 for r in flatten_rows if r.get("action_call") == "BUY")
    daily = []
    for date in dates:
        plan = flatten_meta["by_date"].get(date) or {}
        day_rows = [r for r in flatten_rows if r.get("date") == date]
        gset = {str(r["ticker"]).upper()
                for r in (gainer_meta.get("by_date") or {}).get(date) or []}
        lset = {str(r["ticker"]).upper()
                for r in (loser_meta.get("by_date") or {}).get(date) or []}
        mset = {str(t).upper()
                for t in (mover_meta.get("by_date") or {}).get(date) or []}
        cset = {str(t).upper()
                for t in ((capture_meta.get("by_date") or {}).get(date) or {}).get("tickers") or []}
        chosen = {str(t).upper() for t in (plan.get("tickers") or [])}
        g_rows = [r for r in rows
                  if r.get("date") == date and "gainers" in (r.get("sources") or [])]
        n_win = sum(1 for r in day_rows if r.get("outcome") == "win")
        n_lose = sum(1 for r in day_rows if r.get("outcome") == "lose")
        n_out = n_win + n_lose
        daily.append({
            "date": date,
            "score": plan.get("score"),
            "route": plan.get("route"),
            "flatten_ok": bool(plan.get("flatten_ok")),
            "hard_red": bool(plan.get("hard_red")),
            "n_priced_buys": plan.get("n_priced_buys"),
            "have_prior_book": plan.get("have_prior_book"),
            "why": plan.get("why"),
            "tickers": list(plan.get("tickers") or []),
            "n": len(day_rows),
            "n_buy": sum(1 for r in day_rows if r.get("action_call") == "BUY"),
            "n_gainers": len(gset),
            "n_gainers_chosen": len(chosen & gset),
            "n_gainers_captured": sum(1 for r in g_rows if r.get("candle_capture")),
            "n_movers": len(mset),
            "n_movers_chosen": len(chosen & mset),
            "n_losers": len(lset),
            "n_losers_chosen": len(chosen & lset),
            "n_captured": len(cset),
            "n_probable": sum(
                1 for r in rows
                if r.get("date") == date and "probable" in (r.get("sources") or [])
            ),
            "n_gainers_watch": len(cset & gset),
            "n_losers_watch": len(cset & lset),
            "n_gainers_probable": sum(
                1 for r in rows
                if r.get("date") == date
                and "probable" in (r.get("sources") or [])
                and "gainers" in (r.get("sources") or [])
            ),
            "n_win": n_win,
            "n_lose": n_lose,
            "lose_rate": None if not n_out else round(n_lose / n_out, 3),
        })
    tally = _build_tally(rows, daily, flatten_rows)
    return {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "method": "flatten_lookback_action",
        "policy": flatten_meta["policy"],
        "from_date": flatten_meta["from_date"],
        "to_date": flatten_meta["to_date"],
        "top_n": top_n,
        "min_change": min_change,
        "preset": preset or act.default_preset_name(),
        "n_sessions": flatten_meta["n_sessions"],
        "n_flatten_days": len(flatten_rows),
        "n_rows": len(rows),
        "n_tickers": len(names),
        "n_gainers": gainer_meta.get("n_gainer_days") or 0,
        "n_movers": mover_meta.get("n_mover_days") or 0,
        "n_losers": loser_meta.get("n_loser_days") or 0,
        "n_captured": capture_meta.get("n_capture_days") or 0,
        "n_probable": sum(
            1 for r in rows if "probable" in (r.get("sources") or [])
        ),
        "universe": src if src in UNIVERSES else "flatten",
        "tally": tally,
        "custom_tickers": extra,
        "recall_buy": recall_buy,
        "recall_buy_rate": (
            None if not flatten_rows
            else round(recall_buy / len(flatten_rows), 3)
        ),
        "daily": daily,
        "rows": rows,
        "session_dates": dates,
        "by_date": {
            d: list((flatten_meta["by_date"].get(d) or {}).get("tickers") or [])
            for d in dates
        },
        "lookback": {
            "generated_at": painted.get("generated_at"),
            "n_names": len(painted.get("names") or []),
        },
    }


def _build_tally(rows: list[dict], daily: list[dict],
                 flatten_rows: list[dict]) -> dict:
    def _side(name: str) -> list[dict]:
        return [r for r in rows if name in (r.get("sources") or [])]

    gainers = _side("gainers")
    movers = _side("movers")
    losers = _side("losers")
    captured = _side("captured")
    probable = _side("probable")
    n_win = sum(1 for r in flatten_rows if r.get("outcome") == "win")
    n_lose = sum(1 for r in flatten_rows if r.get("outcome") == "lose")
    n_flat = sum(1 for r in flatten_rows if r.get("outcome") == "flat")
    n_out = n_win + n_lose
    low = [r for r in flatten_rows
           if r.get("flatten_score") is not None
           and float(r.get("flatten_score") or 0) < 1.0]
    low_lose = sum(1 for r in low if r.get("outcome") == "lose")
    low_out = sum(1 for r in low if r.get("outcome") in ("win", "lose"))
    hard = [r for r in flatten_rows if r.get("flatten_ok") is False
            and r.get("flatten_score") is not None
            and float(r.get("flatten_score") or 0) <= -3.0]
    hard_lose = sum(1 for r in hard if r.get("outcome") == "lose")
    hard_out = sum(1 for r in hard if r.get("outcome") in ("win", "lose"))

    def _cap(side_rows: list[dict]) -> dict:
        chosen = sum(1 for r in side_rows if "flatten" in (r.get("sources") or []))
        captured = sum(1 for r in side_rows if r.get("candle_capture"))
        return {
            "universe": len(side_rows),
            "chosen": chosen,
            "chosen_rate": None if not side_rows else round(chosen / len(side_rows), 3),
            "captured": captured,
            "captured_rate": (
                None if not side_rows else round(captured / len(side_rows), 3)
            ),
        }

    return {
        "flatten_picks": len(flatten_rows),
        "winners": n_win,
        "losers": n_lose,
        "flats": n_flat,
        "lose_rate": None if not n_out else round(n_lose / n_out, 3),
        "win_rate": None if not n_out else round(n_win / n_out, 3),
        "lose_target": 0.25,
        "low_s": {
            "picks": len(low),
            "losers": low_lose,
            "lose_rate": None if not low_out else round(low_lose / low_out, 3),
        },
        "hard_red": {
            "picks": len(hard),
            "losers": hard_lose,
            "lose_rate": None if not hard_out else round(hard_lose / hard_out, 3),
        },
        "gainers": _cap(gainers),
        "movers": _cap(movers),
        "losers_tape": _cap(losers),
        "captured": {
            **_cap(captured),
            "gainer_hits": sum(
                1 for r in captured if "gainers" in (r.get("sources") or [])
            ),
            "loser_hits": sum(
                1 for r in captured if "losers" in (r.get("sources") or [])
            ),
        },
        "probable": {
            **_cap(probable),
            "gainer_hits": sum(
                1 for r in probable if "gainers" in (r.get("sources") or [])
            ),
            "loser_hits": sum(
                1 for r in probable if "losers" in (r.get("sources") or [])
            ),
        },
        "n_sessions": len(daily),
    }


def filter_rows(rows: list[dict], source: str = "flatten",
                date: str | None = None,
                tickers: list[str] | None = None) -> list[dict]:
    wanted = {_tick(t) for t in (tickers or []) if _tick(t)}
    out = []
    for r in rows:
        srcs = set(r.get("sources") or [])
        if source == "custom":
            if wanted and _tick(r.get("ticker")) not in wanted:
                continue
        elif source not in srcs:
            continue
        if date and r.get("date") != date:
            continue
        out.append(r)
    return out


def _source_cell(row: dict) -> str:
    return ",".join(row.get("sources") or [])


def _pct_label(rate) -> str:
    if rate is None:
        return "—"
    return f"{100 * float(rate):.0f}%"


def _rg_text(val) -> str:
    if val is None:
        return "—"
    try:
        return f"{float(val):.2f}"
    except (TypeError, ValueError):
        return "—"


def _chg_text(val) -> str:
    if val is None:
        return "—"
    try:
        return f"{float(val):+.2f}%"
    except (TypeError, ValueError):
        return "—"


def _tally_markdown(tally: dict) -> str:
    g = tally.get("gainers") or {}
    m = tally.get("movers") or {}
    l = tally.get("losers_tape") or {}
    cap = tally.get("captured") or {}
    prob = tally.get("probable") or {}
    low = tally.get("low_s") or {}
    hard = tally.get("hard_red") or {}
    g_uni = g.get("universe") or 0
    return "\n".join([
        "## Chosen tally (flatten ∩ tape)",
        "",
        f"- **Capture watchlist** (prior-session top gainers/movers + "
        f"AMC/BMO earnings + morning priced BUYs + flatten + "
        f"20-day OHLC hot rank): "
        f"**{cap.get('universe') or 0}** name-days · hit "
        f"**{cap.get('gainer_hits') or 0}** / {g_uni} top gainers "
        f"({_pct_label((cap.get('gainer_hits') or 0) / g_uni if g_uni else None)}) "
        f"· {cap.get('loser_hits') or 0} top losers. "
        f"Not a live buy list.",
        f"- **Probable rippers** (yesterday's liquid gainers, drop "
        f"exploded ret_5 > 10 / rvol > 2.8): "
        f"**{prob.get('universe') or 0}** name-days · hit "
        f"**{prob.get('gainer_hits') or 0}** / {g_uni} top gainers "
        f"({_pct_label((prob.get('gainer_hits') or 0) / g_uni if g_uni else None)}) "
        f"· {prob.get('loser_hits') or 0} top losers.",
        f"- **Top gainers:** flatten chose **{g.get('chosen') or 0}** / "
        f"{g_uni} "
        f"({_pct_label(g.get('chosen_rate'))}). "
        f"Prior R:G / candle label on the gainer tape: "
        f"**{g.get('captured') or 0}** "
        f"({_pct_label(g.get('captured_rate'))}).",
        f"- **Top movers (priced BUYs):** flatten chose "
        f"**{m.get('chosen') or 0}** / {m.get('universe') or 0} "
        f"({_pct_label(m.get('chosen_rate'))}).",
        f"- **Top losers:** flatten chose **{l.get('chosen') or 0}** / "
        f"{l.get('universe') or 0} "
        f"({_pct_label(l.get('chosen_rate'))}).",
        f"- **Flatten picks win/lose** (same-day Change%): "
        f"{tally.get('winners') or 0}W / {tally.get('losers') or 0}L / "
        f"{tally.get('flats') or 0} flat · lose rate "
        f"**{_pct_label(tally.get('lose_rate'))}** "
        f"(target ≤ 25%).",
        f"- **Low-S days (S < +1):** {low.get('picks') or 0} picks · "
        f"lose {_pct_label(low.get('lose_rate'))}.",
        f"- **Hard-red wishlist:** {hard.get('picks') or 0} names · "
        f"lose {_pct_label(hard.get('lose_rate'))}.",
    ])


def _tally_html(tally: dict) -> str:
    g = tally.get("gainers") or {}
    m = tally.get("movers") or {}
    l = tally.get("losers_tape") or {}
    low = tally.get("low_s") or {}
    cap = tally.get("captured") or {}
    prob = tally.get("probable") or {}
    g_uni = g.get("universe") or 0
    hit = cap.get("gainer_hits") or 0
    cards = [
        ("Gainers captured",
         f"{hit}/{g_uni}",
         f"watchlist {cap.get('universe') or 0} name-days · "
         f"{cap.get('loser_hits') or 0} losers also on list"),
        ("Probable rippers",
         f"{prob.get('gainer_hits') or 0}/{g_uni}",
         f"{prob.get('universe') or 0} name-days · "
         f"{prob.get('loser_hits') or 0} losers · yday continuation"),
        ("Gainers chosen",
         f"{g.get('chosen') or 0}/{g_uni}",
         f"flatten would-buy ∩ top gainers"),
        ("Movers chosen",
         f"{m.get('chosen') or 0}/{m.get('universe') or 0}",
         "priced BUY calls ∩ flatten"),
        ("Losers chosen",
         f"{l.get('chosen') or 0}/{l.get('universe') or 0}",
         "want this near zero"),
        ("Flatten lose rate",
         _pct_label(tally.get("lose_rate")),
         f"{tally.get('winners') or 0}W / {tally.get('losers') or 0}L · target ≤25%"),
        ("Low-S lose rate",
         _pct_label(low.get("lose_rate")),
         f"{low.get('picks') or 0} picks when S < +1"),
    ]
    bits = []
    for title, big, sub in cards:
        bits.append(
            f"<div><span class='muted'>{html.escape(title)}</span>"
            f"<b>{html.escape(str(big))}</b>"
            f"<span class='muted'>{html.escape(sub)}</span></div>"
        )
    return "<div class='tally'>" + "".join(bits) + "</div>"


def render_markdown(payload: dict, source: str = "flatten",
                    date: str | None = None,
                    tickers: list[str] | None = None) -> str:
    rows = filter_rows(payload.get("rows") or [], source=source,
                       date=date, tickers=tickers)
    L = [
        "# Flatten lookback action",
        "",
        f"_Generated {payload.get('generated_at')}_",
        "",
        f"Live policy **`{payload.get('policy')}`**. "
        f"Window **{payload.get('from_date')}** → **{payload.get('to_date')}**. "
        f"{payload.get('n_sessions')} sessions · "
        f"{payload.get('n_flatten_days')} flatten pick-days · "
        f"{payload.get('n_gainers')} gainer-days · "
        f"{payload.get('n_movers')} priced mover BUYs · "
        f"{payload.get('n_losers')} loser-days.",
        "",
        _tally_markdown(payload.get("tally") or {}),
        "",
        "Default view is the names **flatten_robust would buy** that "
        "session (3d robust size book, or ranked mover BUYs when the "
        "09:30 flatten gate fires). Hard-red days still show the .io "
        "wish-list — those names are not live tickets.",
        "",
        "**Clock:** cameras / setups / hall pass / Action are that date "
        "**09:30 ET**. Same-day Finviz and today's unprinted book never "
        "color cameras. Same-day Change% picks the Gainers / Losers "
        "tabs and grades flatten win/lose — never the 09:30 action. "
        "Movers tab = priced BUY calls the flatten gate actually sees "
        "(not the 2k-name liquid universe), reused from mover lookback. "
        "Gainers tab reuses the gainer-lookback sheet. Losers tab is "
        "the liquid ≤−2% tape. Flatten names are freshly painted. "
        "R:G / volume / candle pattern use **prior sessions only**. "
        "E/R/D are Finviz quote-chart markers (earnings / ratings / "
        "ex-div) knowable by 09:30. "
        "Custom = ticker filter; re-run with `--tickers` to add names "
        "that are not already here. Action dropdown `universe` = "
        "flatten | gainers | movers | losers.",
        "",
        f"Phone: `dashboard/flatten-lookback/index.html`. "
        f"Sister boards: [ticker lookback](../dashboard/ticker-lookback/) · "
        f"[gainer lookback](../dashboard/gainer-lookback/) · "
        f"[mover lookback](../dashboard/mover-lookback/) · "
        f"[sleeve merge](../dashboard/sleeve-merge/).",
        "",
        "## Each session (flatten method)",
        "",
        "| Date | S | Route | Flatten? | "
        "Gainers chosen | Movers chosen | Losers chosen | "
        "Win/lose | Would-buy | Why |",
        "|---|---:|---|---|---:|---:|---:|---|---|---|",
    ]
    for d in payload.get("daily") or []:
        score = d.get("score")
        sc = "—" if score is None else f"{float(score):+.2f}"
        lose = d.get("lose_rate")
        L.append(
            f"| {d.get('date')} | {sc} | {d.get('route') or '—'} | "
            f"{'yes' if d.get('flatten_ok') else 'no'} | "
            f"{d.get('n_gainers_chosen') or 0}/{d.get('n_gainers') or 0} | "
            f"{d.get('n_movers_chosen') or 0}/{d.get('n_movers') or 0} | "
            f"{d.get('n_losers_chosen') or 0}/{d.get('n_losers') or 0} | "
            f"{d.get('n_win') or 0}W/{d.get('n_lose') or 0}L"
            f"{'' if lose is None else f' ({100*lose:.0f}% L)'} | "
            f"{', '.join(d.get('tickers') or []) or '—'} | "
            f"{str(d.get('why') or '—').replace('|', '/')} |"
        )
    L += [
        "",
        f"## Rows — source `{source}`"
        + (f" · date `{date}`" if date else "")
        + (f" · tickers {','.join(tickers)}" if tickers else ""),
        "",
        "| Date 09:30 ET | Marks | Src | Route | # | Ticker | "
        "E/R/D | R:G | Vol R:G | Candle | Cap | 5d% | RVOL | OHLC | Δ | "
        "Close 16:00 ET | Open 09:30 ET | o→c | Cond | "
        "Action 09:30 ET | Why | Setups | Cameras | Coaches | "
        "+1d | +3d | +1w |",
        "|---|---|---|---|---:|---|---|---:|---:|---|---|---:|---:|---|---:|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        hits = r.get("hits") or {}
        bar = r.get("session_bar") or {}
        hz = r.get("horizon_dates") or {}
        fwd = gla._fwd(r) or {}
        L.append(
            f"| {act.session_stamp(r.get('date'), act.OPEN_CLOCK)} | "
            f"{r.get('marks_cell') or ga._marks_cell(r.get('marks'))} | "
            f"{_source_cell(r)} | {r.get('flatten_route') or '—'} | "
            f"{r.get('flatten_rank') or r.get('gainer_rank') or ''} | "
            f"`{r.get('ticker')}` | "
            f"{r.get('erd_cell') or '—'} | "
            f"{_rg_text(r.get('candle_body_rg'))} | "
            f"{_rg_text(r.get('candle_vol_rg'))} | "
            f"{r.get('candle_pattern') or '—'} | "
            f"{'yes' if r.get('candle_capture') else '—'} | "
            f"{_rg_text(r.get('ohlc_ret_5'))} | "
            f"{_rg_text(r.get('ohlc_rvol'))} | "
            f"{'nr7' if r.get('ohlc_nr7') else ('brk' if r.get('ohlc_break_10') else '—')} | "
            f"{_chg_text(r.get('day_change'))} | "
            f"{act.format_price(bar.get('close'), r.get('date'), act.CLOSE_CLOCK)} | "
            f"{act.format_price(bar.get('open'), r.get('date'), act.OPEN_CLOCK)} | "
            f"{gla._oc_text(bar.get('close_open_pct'), r.get('date'))} | "
            f"{r.get('cond_tally') or act.cond_tally(r)} | "
            f"**{r.get('action_label') or act.format_action(r.get('action_call'), r.get('date'))}** | "
            f"{str(r.get('action_reason') or r.get('flatten_why') or '—').replace('|', '/')} | "
            f"{setups.setup_labels(r) or '—'} | "
            f"{r.get('labeled') or ga._labeled(r.get('boxes'))} | "
            f"{r.get('labeled_domains') or ga._labeled_domains(r.get('domains'))} | "
            f"{gla._pct_text(fwd.get('1d'), hz.get('1d'), act.CLOSE_CLOCK)} | "
            f"{gla._pct_text(fwd.get('3d'), hz.get('3d'), act.CLOSE_CLOCK)} | "
            f"{gla._pct_text(fwd.get('1w'), hz.get('1w'), act.CLOSE_CLOCK)} |"
        )
    L += [
        "",
        "_🟢 up · 🟡 flat · 🔴 down · ⬛ missing · 🔵 improved · "
        "🚨 purely worse · ⚪ no red. Action = 09:30 ET. "
        "Hits in the phone table = 1d/3d/1w catch._",
        "",
    ]
    return "\n".join(L) + "\n"


def _camera_tds(row: dict) -> str:
    lit = setups.box_highlights(row)
    boxes = row.get("boxes") or {}
    cells = []
    for k, _ in CAMERA_COLS:
        hit = f" setup-hit setup-{html.escape(lit[k])}" if k in lit else ""
        cells.append(
            f'<td class="{html.escape(boxes.get(k, "missing"))}{hit}">'
            f'{run._icon(boxes.get(k, "missing"))}</td>'
        )
    domains = row.get("domains") or {}
    for k, _ in DOMAIN_COLS:
        cells.append(
            f'<td class="{html.escape(domains.get(k, "missing"))}">'
            f'{run._icon(domains.get(k, "missing"))}</td>'
        )
    return "".join(cells)


def _row_html(r: dict) -> str:
    tone = act.action_tone(r.get("action_call") or "")
    hits = r.get("hits") or {}
    bar = r.get("session_bar") or {}
    hz = r.get("horizon_dates") or {}
    fwd = gla._fwd(r) or {}
    date_cls = html.escape(run._date_classes(r))
    hall = run._hall_text(r)
    hall_cls = html.escape(str(r.get("lane") or "missing"))
    oc_tone = tl.price_tone(bar.get("close_open_pct"))
    srcs = " ".join(r.get("sources") or [])
    why = r.get("action_reason") or r.get("flatten_why") or "—"
    chips = setups.setup_chips_html(r)
    row_cls = setups.row_setup_class(r)

    def pct_td(pct, text) -> str:
        cls = html.escape(act.ret_tone(pct))
        return f"<td class='{cls}'>{html.escape(text)}</td>"

    return (
        f'<tr class="{html.escape(row_cls)}" data-date="{html.escape(str(r.get("date") or ""))}" '
        f'data-ticker="{html.escape(str(r.get("ticker") or ""))}" '
        f'data-src="{html.escape(srcs)}">'
        f'<th class="{date_cls}">{html.escape(run._date_label(r))}</th>'
        f"<td class='hits'>{html.escape(gla.hits_cell(hits))}</td>"
        f"<td class='src'>{html.escape(srcs)}</td>"
        f"<td>{html.escape(str(r.get('flatten_route') or '—'))}</td>"
        f"<td>{r.get('flatten_rank') or r.get('gainer_rank') or ''}</td>"
        f"<td>{html.escape(str(r.get('ticker') or ''))}</td>"
        f"<td class='erd'>{html.escape(str(r.get('erd_cell') or '—'))}</td>"
        f"<td>{html.escape(_rg_text(r.get('candle_body_rg')))}</td>"
        f"<td>{html.escape(_rg_text(r.get('candle_vol_rg')))}</td>"
        f"<td>{html.escape(str(r.get('candle_pattern') or '—'))}</td>"
        f"<td>{'yes' if r.get('candle_capture') else '—'}</td>"
        f"<td>{html.escape(_rg_text(r.get('ohlc_ret_5')))}</td>"
        f"<td>{html.escape(_rg_text(r.get('ohlc_rvol')))}</td>"
        f"<td>{'nr7' if r.get('ohlc_nr7') else ('brk' if r.get('ohlc_break_10') else '—')}</td>"
        f"{pct_td(r.get('day_change'), _chg_text(r.get('day_change')))}"
        f"<td>{html.escape(act.format_price(bar.get('close'), r.get('date'), act.CLOSE_CLOCK))}</td>"
        f"<td>{html.escape(act.format_price(bar.get('open'), r.get('date'), act.OPEN_CLOCK))}</td>"
        f"{pct_td(bar.get('close_open_pct'), gla._oc_text(bar.get('close_open_pct'), r.get('date')))}"
        f'<td class="{html.escape((run._condition(r) or {}).get("tone", "missing"))}">'
        f"{html.escape(str(r.get('cond_tally') or act.cond_tally(r)))}</td>"
        f'<td class="action {tone}">{html.escape(str(r.get("action_label") or act.format_action(r.get("action_call"), r.get("date"))))}</td>'
        f'<td class="hall {hall_cls}">{html.escape(hall)}</td>'
        f'<td class="setups">{chips or "—"}</td>'
        f"{_camera_tds(r)}"
        f'<td class="why">{html.escape(str(why))}</td>'
        f'<td class="why">{html.escape(str(r.get("flatten_why") or "—"))}</td>'
        f"{pct_td(fwd.get('1d'), gla._pct_text(fwd.get('1d'), hz.get('1d'), act.CLOSE_CLOCK))}"
        f"{pct_td(fwd.get('3d'), gla._pct_text(fwd.get('3d'), hz.get('3d'), act.CLOSE_CLOCK))}"
        f"{pct_td(fwd.get('1w'), gla._pct_text(fwd.get('1w'), hz.get('1w'), act.CLOSE_CLOCK))}"
        "</tr>"
    )


def render_html(payload: dict) -> str:
    dates = payload.get("session_dates") or []
    date_opts = ['<option value="">All days</option>'] + [
        f'<option value="{html.escape(d)}">{html.escape(d)}</option>' for d in dates
    ]
    day_rows = []
    for d in payload.get("daily") or []:
        score = d.get("score")
        sc = "—" if score is None else f"{float(score):+.2f}"
        names = ", ".join(d.get("tickers") or []) or "—"
        lose = d.get("lose_rate")
        wl = (f"{d.get('n_win') or 0}W/{d.get('n_lose') or 0}L"
              + ("" if lose is None else f" ({100*lose:.0f}% L)"))
        day_rows.append(
            f"<tr><th>{html.escape(str(d.get('date') or ''))}</th>"
            f"<td>{html.escape(sc)}</td>"
            f"<td>{html.escape(str(d.get('route') or '—'))}</td>"
            f"<td>{'yes' if d.get('flatten_ok') else 'no'}</td>"
            f"<td>{d.get('n_gainers_chosen') or 0}/{d.get('n_gainers') or 0}</td>"
            f"<td>{d.get('n_movers_chosen') or 0}/{d.get('n_movers') or 0}</td>"
            f"<td>{d.get('n_losers_chosen') or 0}/{d.get('n_losers') or 0}</td>"
            f"<td>{html.escape(wl)}</td>"
            f"<td class='why'>{html.escape(names)}</td>"
            f"<td class='why'>{html.escape(str(d.get('why') or '—'))}</td></tr>"
        )
    body = [_row_html(r) for r in sorted(
        payload.get("rows") or [],
        key=lambda x: (str(x.get("date") or ""), str(x.get("ticker") or "")),
    )]
    cam_h = "".join(f"<th>{html.escape(lab)}</th>" for _, lab in CAMERA_COLS)
    dom_h = "".join(f"<th>{html.escape(lab)}</th>" for _, lab in DOMAIN_COLS)
    custom = ",".join(payload.get("custom_tickers") or [])
    universe = payload.get("universe") or "flatten"
    tally_html = _tally_html(payload.get("tally") or {})
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Flatten lookback</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui}}
main{{max-width:1280px;margin:auto;padding:16px}}h1,h2{{margin:.4em 0}}
.muted{{color:var(--muted)}}a{{color:#93c5fd}}
.bar{{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:12px 0;position:sticky;top:0;z-index:4;background:#0b1020ee;padding:8px 0}}
.chip{{padding:7px 12px;border:1px solid var(--line);border-radius:999px;background:var(--card);color:var(--text);cursor:pointer}}
.chip.on{{background:#edf2ff;color:#0b1020}}
.tally{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;margin:12px 0}}
.tally div{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 12px}}
.tally b{{display:block;font-size:22px}}
.bar select,.bar input{{background:var(--card);color:var(--text);border:1px solid var(--line);border-radius:8px;padding:8px 10px}}
.sheet{{overflow-x:auto;border:1px solid var(--line);border-radius:12px;margin:14px 0}}
table{{border-collapse:separate;border-spacing:0;min-width:2200px;width:100%;background:var(--card)}}
th,td{{padding:8px 7px;text-align:center;border-bottom:1px solid var(--line);white-space:nowrap}}
thead th{{position:sticky;top:0;background:#17213a;z-index:2}}
tbody th{{position:sticky;left:0;background:#17213a;text-align:left;z-index:1}}
td.hits{{position:sticky;left:13.5rem;background:#17213a;font-weight:700;z-index:1}}
td.good{{background:#123d2c}}td.bad{{background:#4b2028}}td.neutral{{background:#473e1d}}td.missing{{background:#23283a}}
td.setup-hit{{outline:2px solid #eab308;outline-offset:-2px}}
td.why,td.setups{{text-align:left;white-space:normal;max-width:240px;font-size:12px}}
td.action{{font-weight:700}}
tbody th.better{{background:#1d4ed8}}
tbody th.alarm{{box-shadow:inset 3px 0 0 #f97316}}
tbody th.clean{{box-shadow:inset 3px 0 0 #f8fafc;color:#0b1020;background:#e8eef7}}
.setup-chip{{display:inline-block;margin:1px 2px;padding:1px 7px;border-radius:999px;font-size:11px}}
.setup-chip.good{{background:#123d2c}}.setup-chip.bad{{background:#4b2028}}
@media(max-width:600px){{main{{padding:8px}}th,td{{padding:8px 6px;font-size:13px}}}}
</style></head><body><main>
<h1>Flatten lookback</h1>
<p>Same cameras as <a href="../ticker-lookback/">ticker lookback</a> / stock-book readiness,
on the names <b>flatten_robust</b> would buy that day.
<a href="../sleeve-merge/">sleeve merge</a> ·
<a href="../gainer-lookback/">gainer lookback</a> ·
<a href="../mover-lookback/">mover lookback</a>.</p>
<p>🟢 up / 🟡 flat / 🔴 down / ⬛ missing / 🔵 improved / 🚨 worse / ⚪ no red.
Cameras = knowable by 09:30 ET. Action is that date 09:30 ET — not an end-of-day call.
Gainers / Losers tabs use same-day Change% only to pick the universe.
Movers tab = priced BUY calls the flatten gate sees (reuses mover lookback).
R:G size, volume R:G, and candle patterns are prior sessions only (pre-09:30).
E/R/D = Finviz chart earnings / ratings / dividend markers knowable by 09:30.
Flatten names are freshly painted with cameras.</p>
{tally_html}
<p class="muted">{html.escape(str(payload.get('from_date')))} → {html.escape(str(payload.get('to_date')))}
· {payload.get('n_sessions')} sessions
· flatten pick-days {payload.get('n_flatten_days')}
· gainers {payload.get('n_gainers')}
· mover BUYs {payload.get('n_movers')}
· losers {payload.get('n_losers')}
· universe <code>{html.escape(str(universe))}</code>
· preset <code>{html.escape(str(payload.get('preset') or ''))}</code>
· policy <code>{html.escape(str(payload.get('policy') or ''))}</code></p>
<div class="bar" id="filters">
<button type="button" class="chip{' on' if universe=='flatten' else ''}" data-source="flatten">Flatten</button>
<button type="button" class="chip{' on' if universe=='gainers' else ''}" data-source="gainers">Gainers</button>
<button type="button" class="chip{' on' if universe=='movers' else ''}" data-source="movers">Movers</button>
<button type="button" class="chip{' on' if universe=='losers' else ''}" data-source="losers">Losers</button>
<button type="button" class="chip{' on' if universe=='captured' else ''}" data-source="captured">Captured</button>
<button type="button" class="chip{' on' if universe=='probable' else ''}" data-source="probable">Probable</button>
<button type="button" class="chip" data-source="custom">Custom</button>
<select id="dateSel" aria-label="Session date">{''.join(date_opts)}</select>
<input id="tickerIn" type="text" placeholder="Custom tickers: TLN,VST" value="{html.escape(custom)}" size="22">
<span class="muted" id="count"></span>
</div>
<p class="muted" id="customNote">Custom filters the painted set. Re-run with <code>--tickers A,B</code> to add names that are not already here.</p>
<h2>Each session — flatten method</h2>
<div class="sheet"><table style="min-width:1100px">
<thead><tr><th>Date</th><th>S</th><th>Route</th><th>Flatten?</th>
<th>Gainers chosen</th><th>Movers chosen</th><th>Losers chosen</th>
<th>Win/lose</th><th>Would-buy</th><th>Why</th></tr></thead>
<tbody>{''.join(day_rows)}</tbody></table></div>
<h2>Picks with cameras</h2>
<div class="sheet"><table>
<thead><tr><th>Date 09:30 ET</th><th>Hits 1d/3d/1w</th><th>Src</th><th>Route</th><th>#</th><th>Ticker</th>
<th>E/R/D</th><th>R:G</th><th>Vol R:G</th><th>Candle</th><th>Cap</th><th>5d%</th><th>RVOL</th><th>OHLC</th><th>Δ</th>
<th>Close 16:00 ET</th><th>Open 09:30 ET</th><th>o→c 09:30→16:00</th><th>Cond</th>
<th>Action 09:30 ET</th><th>Hall pass</th><th>Setups</th>
{cam_h}{dom_h}<th>Trigger</th><th>Flatten why</th>
<th>+1d 16:00 ET</th><th>+3d 16:00 ET</th><th>+1w 16:00 ET</th></tr></thead>
<tbody id="rows">{''.join(body)}</tbody></table></div>
<script>
(function(){{
  const chips = [...document.querySelectorAll('.chip[data-source]')];
  const dateSel = document.getElementById('dateSel');
  const tickerIn = document.getElementById('tickerIn');
  const rows = [...document.querySelectorAll('#rows tr')];
  const count = document.getElementById('count');
  let source = (document.querySelector('.chip.on') || {{}}).dataset.source || 'flatten';
  function ticks() {{
    return tickerIn.value.toUpperCase().split(/[\\s,]+/).filter(Boolean);
  }}
  function apply() {{
    const day = dateSel.value;
    const custom = ticks();
    let n = 0;
    for (const tr of rows) {{
      const srcs = (tr.dataset.src || '').split(/\\s+/);
      const okSrc = source === 'custom'
        ? (custom.length ? custom.includes(tr.dataset.ticker) : true)
        : srcs.includes(source);
      const okDay = !day || tr.dataset.date === day;
      const show = okSrc && okDay;
      tr.hidden = !show;
      if (show) n++;
    }}
    count.textContent = n + ' rows';
  }}
  chips.forEach(btn => btn.addEventListener('click', () => {{
    chips.forEach(b => b.classList.remove('on'));
    btn.classList.add('on');
    source = btn.dataset.source;
    apply();
  }}));
  dateSel.addEventListener('change', apply);
  tickerIn.addEventListener('input', apply);
  apply();
}})();
</script>
</main></body></html>"""


def _slim_row(r: dict) -> dict:
    return {
        "date": r.get("date"),
        "ticker": r.get("ticker"),
        "sources": r.get("sources"),
        "flatten_route": r.get("flatten_route"),
        "flatten_score": r.get("flatten_score"),
        "flatten_ok": r.get("flatten_ok"),
        "flatten_why": r.get("flatten_why"),
        "flatten_rank": r.get("flatten_rank"),
        "gainer_rank": r.get("gainer_rank"),
        "gainer_change": r.get("gainer_change"),
        "day_change": r.get("day_change"),
        "outcome": r.get("outcome"),
        "candle_ok": r.get("candle_ok"),
        "candle_body_rg": r.get("candle_body_rg"),
        "candle_vol_rg": r.get("candle_vol_rg"),
        "candle_score": r.get("candle_score"),
        "candle_capture": r.get("candle_capture"),
        "candle_last_green": r.get("candle_last_green"),
        "candle_pattern": r.get("candle_pattern"),
        "erd_cell": r.get("erd_cell"),
        "erd_E_date": r.get("erd_E_date"),
        "erd_E_color": r.get("erd_E_color"),
        "erd_R_date": r.get("erd_R_date"),
        "erd_R_color": r.get("erd_R_color"),
        "erd_D_date": r.get("erd_D_date"),
        "erd_D_color": r.get("erd_D_color"),
        "erd_earn_react": r.get("erd_earn_react"),
        "capture_reasons": r.get("capture_reasons"),
        "ohlc_ret_5": r.get("ohlc_ret_5"),
        "ohlc_ret_10": r.get("ohlc_ret_10"),
        "ohlc_rvol": r.get("ohlc_rvol"),
        "ohlc_nr7": r.get("ohlc_nr7"),
        "ohlc_break_10": r.get("ohlc_break_10"),
        "ohlc_hot_score": r.get("ohlc_hot_score"),
        "session_bar": r.get("session_bar"),
        "horizon_dates": r.get("horizon_dates"),
        "condition": r.get("condition"),
        "cond_tally": r.get("cond_tally"),
        "boxes": r.get("boxes"),
        "domains": r.get("domains"),
        "labeled": r.get("labeled"),
        "labeled_domains": r.get("labeled_domains"),
        "marks": r.get("marks"),
        "marks_cell": r.get("marks_cell"),
        "lane": r.get("lane"),
        "lane_label": r.get("lane_label"),
        "action_call": r.get("action_call"),
        "action_label": r.get("action_label"),
        "action_stamp": r.get("action_stamp"),
        "action_reason": r.get("action_reason"),
        "setups": [
            {"id": s.get("id"), "short": s.get("short"),
             "verdict": s.get("verdict"), "edge_1d": s.get("edge_1d")}
            for s in (r.get("setups") or [])
        ],
        "price_changes": r.get("price_changes") or r.get("forward_returns"),
        "hits": r.get("hits"),
    }


def write(payload: dict) -> dict:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    DAILY_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    slim = {k: v for k, v in payload.items() if k != "rows"}
    slim["rows"] = [_slim_row(r) for r in payload.get("rows") or []]
    md = render_markdown(payload)
    OUT_MD.write_text(md, encoding="utf-8")
    DAILY_MD.write_text(md, encoding="utf-8")
    OUT_JSON.write_text(json.dumps(slim, indent=2, default=str), encoding="utf-8")
    OUT_HTML.write_text(render_html(payload), encoding="utf-8")
    print(f"[flatten-lookback-action] wrote {OUT_MD}")
    print(f"[flatten-lookback-action] wrote {OUT_JSON}")
    print(f"[flatten-lookback-action] phone {OUT_HTML}")
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default=START)
    ap.add_argument("--to-date", default="")
    ap.add_argument("--top-n", type=int, default=gla.TOP_N)
    ap.add_argument("--min-change", type=float, default=gla.MIN_CHANGE)
    ap.add_argument("--preset", default="", help="featured|strict|setups|lane|loose")
    ap.add_argument("--source", default="flatten",
                    choices=list(SOURCES),
                    help="Action dropdown / board tab: flatten|gainers|movers|losers|captured|probable|custom")
    ap.add_argument("--date", default="", help="Filter one session YYYY-MM-DD")
    ap.add_argument("--tickers", default="",
                    help="Custom tickers (comma) to paint and filter")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    extra = [t for t in args.tickers.replace(" ", ",").split(",") if t]
    payload = walk(
        from_date=args.from_date,
        to_date=args.to_date or None,
        top_n=args.top_n,
        min_change=args.min_change,
        preset=args.preset or None,
        extra_tickers=extra,
        source=args.source,
    )
    if args.write:
        write(payload)
    print(render_markdown(
        payload, source=args.source,
        date=args.date or None,
        tickers=extra or None,
    )[:8000])


if __name__ == "__main__":
    main()
