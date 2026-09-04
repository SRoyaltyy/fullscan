"""Today's flatten_hard_red tickets — holdings + leftover cash.

Replay the live book through yesterday (lots stay open), then print
the 09:30 / 16:00 card for `date`. Does not invent a third universe:
same lists the morning already has (S, mover BUY tape, yesterday's
book at 09:30, today's book only at 16:00).

A name already held is not bought again. Buys size from leftover
cash after due sells and Futubull fees. Whole shares only. Hard-red
S ≤ −3 blocks new risk; scheduled 1d exits still settle. A mover
day keeps leftover cash overnight (no same-day .io refill).

CLI via sleeve_merge: python -m src.sleeve_merge --card [--date D] --write-card
"""
from __future__ import annotations

import html as _html
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

from src.sleeve_merge import (
    CLOSE_CLOCK,
    DASH_DIR,
    DEFAULT,
    HOLD_SESSIONS,
    LIVE_POLICY,
    OPEN_CLOCK,
    OUT_DIR,
    ROOT,
    _bar,
    _cond_score,
    _conviction,
    _num,
    _prior_book,
    book_ticker_set,
    io_picks,
    io_select_picks,
    list_books,
    live_policy,
    load_book_map,
    load_payload,
    next_session,
    rank_calls,
    run_flatten_switch,
    session_calendar,
)

DAILY_DIR = ROOT / "01_daily"
GENERAL_DIR = DAILY_DIR / "general"
TODAY_JSON = OUT_DIR / "today.json"
POSITIONS_JSON = OUT_DIR / "positions.json"

TODAY_MARK_RE = re.compile(
    r"<!-- TODAY_BEGIN -->.*?<!-- TODAY_END -->", re.S)


def et_today() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _predict_snapshot(date: str):
    """Premarket general predict — same regex as mover_lookback_action."""
    p = GENERAL_DIR / f"{date}_predict.md"
    if not p.is_file():
        return None, None
    try:
        txt = p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None, None
    m = re.search(r"Prediction:\s*(UP|DOWN|FLAT).*?total score\s*(-?[\d.]+)",
                  txt)
    if not m:
        m2 = re.search(r"Prediction:\s*(UP|DOWN|FLAT)", txt)
        return (m2.group(1), None) if m2 else (None, None)
    return m.group(1), float(m.group(2))


def morning_regime(date: str, payload: dict) -> tuple[float | None, str | None]:
    g = (payload.get("regime") or {}).get(date) or {}
    score, pdir = g.get("predict_score"), g.get("predict_dir")
    if score is None or pdir is None:
        snap_dir, snap_score = _predict_snapshot(date)
        if pdir is None:
            pdir = snap_dir
        if score is None:
            score = snap_score
    if score is not None:
        score = float(score)
    return score, pdir


def _fees():
    from src.paper_trade import load_fees, order_fees
    return load_fees(), order_fees


def _px(ticker: str, date: str, clock: str, cache=None):
    """Prefer the clock print; fall back to last close. Never invent."""
    bar = _bar(ticker, date) or {}
    if clock == "open":
        px = _num(bar.get("open"))
        if px:
            return px, "open"
    else:
        px = _num(bar.get("close"))
        if px:
            return px, "close"
    if cache is not None:
        try:
            from src.sleeve_merge import _close_from_cache
            px = _close_from_cache(ticker, date, cache)
            if px:
                return px, "last_close"
        except Exception:
            pass
    px = _num(bar.get("close")) or _num(bar.get("open"))
    if px:
        return px, "bar"
    return None, "missing"


def _next_exit(cal: list[str], date: str, n: int) -> str | None:
    xd = next_session(cal, date, n)
    if xd:
        return xd
    d = datetime.strptime(date, "%Y-%m-%d")
    added = 0
    while added < n:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d.strftime("%Y-%m-%d")


def _copy_lot(lot: dict) -> dict:
    return dict(lot)


def _lot_view(lot: dict, px: float | None) -> dict:
    last = px or lot.get("last_px") or lot.get("entry_px")
    return {
        "ticker": lot["ticker"],
        "sleeve": lot.get("sleeve"),
        "shares": lot["shares"],
        "entry_px": lot.get("entry_px"),
        "entry_date": lot.get("entry_date"),
        "last_px": last,
        "mv": round(lot["shares"] * float(last), 2) if last else 0.0,
        "exit_date": lot.get("exit_date"),
        "reason": lot.get("reason") or "",
    }


def replay_open(date: str, capital: float = 100_000,
                payload: dict | None = None,
                books: list | None = None,
                pol: dict | None = None) -> dict:
    payload = payload if payload is not None else load_payload()
    books = books if books is not None else list_books()
    pol = pol if pol is not None else live_policy()
    return run_flatten_switch(
        payload, books, pol, capital,
        stop_before=date, close_open=False,
    )


def _marked_equity(cash: float, holds: list[dict], date: str, cache) -> float:
    eq = float(cash)
    for h in holds:
        px, _ = _px(h["ticker"], date, "close", cache)
        mark = px or h.get("last_px") or h.get("entry_px") or 0
        eq += h["shares"] * float(mark)
    return eq


def plan_would_buy(
    *,
    date: str,
    pol: dict,
    cache,
    priced_buys: list[dict],
    confirm: set[str],
    io_book: dict | None,
    cash_open: float,
    holds_open: list[dict],
    live_buys: set[str],
    flatten_without_hard_red: bool,
    hard_red: bool,
    route_mover: bool,
) -> dict:
    """Same sleeve the live rule would fill, sized as if the book were flat.

    Holdings and leftover-cash skips are ignored. Hard-red still labels
    the row; the name stays on the list so you can see the tape.
    """
    fees, order_fees = _fees()
    held = {h["ticker"] for h in holds_open}
    equity = _marked_equity(cash_open, holds_open, date, cache)
    pocket = equity
    rows: list[dict] = []

    def take(ticker, px, kind, clock, want, reason, sleeve):
        nonlocal pocket
        if not px or px <= 0:
            rows.append({
                "date": date, "clock": clock, "side": "BUY",
                "ticker": ticker, "shares": 0, "px": None, "px_kind": kind,
                "sleeve": sleeve, "notional": 0, "fee": 0, "cost": 0,
                "reason": reason, "blocked": "no price", "status": "would",
            })
            return
        shares = int(want // px) if want > 0 else 0
        fee = order_fees(shares, px, "buy", fees) if shares >= 1 else 0.0
        cost = shares * px + fee if shares >= 1 else 0.0
        if shares >= 1 and cost > pocket + 1e-6:
            shares = int((pocket - fee) // px) if px > 0 else 0
            if shares >= 1:
                fee = order_fees(shares, px, "buy", fees)
                cost = shares * px + fee
            else:
                fee, cost = 0.0, 0.0
        if shares >= 1:
            pocket -= cost
        if ticker in live_buys:
            blocked = "live"
        elif ticker in held:
            blocked = "already held"
        elif hard_red:
            blocked = "hard-red"
        elif route_mover and sleeve == "io_core":
            blocked = "mover day"
        elif shares < 1:
            blocked = "shares < 1"
        else:
            blocked = "cash tied"
        rows.append({
            "date": date, "clock": clock, "side": "BUY",
            "ticker": ticker, "shares": shares,
            "px": round(float(px), 4), "px_kind": kind,
            "sleeve": sleeve,
            "notional": round(shares * px, 2) if shares else 0,
            "fee": round(fee, 2), "cost": round(cost, 2),
            "reason": reason, "blocked": blocked, "status": "would",
        })

    if flatten_without_hard_red:
        clock = OPEN_CLOCK
        sleeve = "mover_long"
        source = "mover (holdings disregarded)"
        day_cap = float(pol.get("day_cap", 1.0))
        room = min(pocket, max(0.0, equity * day_cap))
        for r in rank_calls(priced_buys, pol.get("long_rank", "cond"))[: pol.get("long_top_n", 10)]:
            t = str(r.get("ticker") or "").upper()
            if not t:
                continue
            px, kind = _px(t, date, "open", cache)
            bump = pol.get("sizeup", 1.0) if t in confirm else 1.0
            want = min(equity * float(pol.get("long_pct", 0.10)) * bump, room, pocket)
            take(t, px, kind, clock, want,
                 f"mover BUY cond={_cond_score(r):+.0f}", sleeve)
            if rows and rows[-1]["shares"] >= 1:
                room -= rows[-1]["notional"] + rows[-1]["fee"]
            if room < 1 or pocket < 1:
                break
    elif io_book is not None:
        clock = CLOSE_CLOCK
        sleeve = "io_core"
        source = f"io {pol.get('io_sleeve', '2w_size')} (holdings disregarded)"
        targets = io_select_picks(
            io_book, pol, date=date, score=None, mover_buys=[],
            top_n=int(pol.get("long_top_n") or 10))
        if targets and pocket > 0:
            per = pocket / len(targets)
            for t in targets:
                px, kind = _px(t, date, "close", cache)
                take(t, px, kind, clock, per, source, sleeve)
        elif not targets:
            source = "empty book"
    else:
        clock = CLOSE_CLOCK
        sleeve = "io_core"
        source = "no .io book to size"

    return {
        "equity": round(equity, 2),
        "spent": round(equity - pocket, 2),
        "source": source,
        "clock": clock,
        "sleeve": sleeve,
        "rows": rows,
    }


def plan_today(date: str, capital: float = 100_000,
               payload: dict | None = None,
               books: list | None = None,
               pol: dict | None = None,
               sim: dict | None = None) -> dict:
    """Propose today's tickets from leftover cash + open lots."""
    payload = payload if payload is not None else load_payload()
    books = books if books is not None else list_books()
    pol = dict(pol) if pol is not None else live_policy()
    sim = sim if sim is not None else replay_open(
        date, capital, payload, books, pol)

    fees, order_fees = _fees()
    cal = list(sim.get("calendar") or [])
    if date not in cal:
        cal = sorted(set(cal + [date]))
    book_map = load_book_map(books)
    score, pdir = morning_regime(date, payload)

    calls = []
    for r in payload.get("called_rows") or []:
        if r.get("date") != date:
            continue
        row = dict(r)
        row["_conv"] = _conviction(row)
        calls.append(row)
    buys = [r for r in calls if r.get("action_call") == "BUY"]

    cache = None
    try:
        from src.paper_trade import get_prices
        tickers = {str(r.get("ticker") or "").upper() for r in buys}
        tickers |= {t for t in (sim.get("open_io") or {})}
        tickers |= {p["ticker"] for p in (sim.get("open_mover") or [])}
        for t in io_select_picks(book_map.get(date) or {}, pol, date=date,
                                top_n=int(pol.get("long_top_n") or 10)):
            tickers.add(t)
        if tickers:
            start = cal[0] if cal else date
            cache = get_prices(sorted(t for t in tickers if t), start, date)
    except Exception:
        cache = None

    priced_buys = []
    for r in buys:
        t = str(r.get("ticker") or "").upper()
        px, _kind = _px(t, date, "open", cache)
        if t and px:
            priced_buys.append(r)

    min_buys = int(pol.get("min_buys", 5))
    book_mode = pol.get("book_for_flatten", "yesterday")
    prior = _prior_book(book_map, cal, date, book_mode)
    last_print = _prior_book(book_map, cal, date, "last")
    have_buys = len(priced_buys) >= min_buys
    green = score is not None and score >= pol.get("long_gate", 1.0)
    blank = score is None
    hard_red_cut = float(pol.get("hard_red", DEFAULT.get("hard_red", -3.0)))
    hard_red_no_new = bool(pol.get("hard_red_no_new", False))
    hard_red_io_ok = bool(pol.get("hard_red_io_ok", False))
    hard_red = score is not None and float(score) <= hard_red_cut

    cash = float(sim.get("cash", capital))
    io_pos = {t: _copy_lot(lot) for t, lot in (sim.get("open_io") or {}).items()}
    mv_pos = [_copy_lot(lot) for lot in (sim.get("open_mover") or [])]
    flat = not io_pos
    flatten_ok = green and have_buys and (
        book_mode in ("none", None, False) or prior is not None)
    flatten_without_hard_red = flatten_ok
    cash_mover = flat and have_buys and (
        (bool(pol.get("mover_when_flat", False)) and green)
        or (bool(pol.get("blank_mover_when_flat", False)) and blank))
    if hard_red_no_new and hard_red:
        flatten_ok = False
        cash_mover = False
    route_mover = flatten_ok or cash_mover
    can_buy = not (hard_red_no_new and hard_red)
    can_buy_io = not (hard_red_no_new and hard_red and not hard_red_io_ok)
    route = ("hold" if (hard_red_no_new and hard_red and not hard_red_io_ok)
             else "mover" if route_mover else "io")
    confirm = book_ticker_set(prior or last_print or {})

    tickets: list[dict] = []
    skipped: list[dict] = []
    cash_open = cash

    def skip(clock, ticker, side, reason, sleeve=""):
        skipped.append({
            "date": date, "clock": clock, "ticker": ticker, "side": side,
            "reason": reason, "sleeve": sleeve, "status": "skip",
        })

    def sell_lot(lot, clock, why, want):
        nonlocal cash
        px, kind = _px(lot["ticker"], date, want, cache)
        if not px:
            skip(clock, lot["ticker"], "SELL",
                 f"no {'09:30 open' if want == 'open' else 'close'} — still held",
                 lot.get("sleeve") or "")
            return False
        fee = order_fees(lot["shares"], px, "sell", fees)
        proceeds = lot["shares"] * px - fee
        cash += proceeds
        tickets.append({
            "date": date, "clock": clock, "side": "SELL",
            "ticker": lot["ticker"], "shares": lot["shares"],
            "px": round(float(px), 4), "px_kind": kind,
            "sleeve": lot.get("sleeve") or "",
            "notional": round(lot["shares"] * px, 2),
            "fee": round(fee, 2),
            "proceeds": round(proceeds, 2),
            "reason": why,
            "status": "plan",
            "cash_after": round(cash, 2),
        })
        return True

    def try_buy(ticker, px, kind, clock, shares, reason, sleeve,
                allow_hard_red=False):
        nonlocal cash
        if not can_buy and not allow_hard_red:
            skip(clock, ticker, "BUY", "hard-red: no new buys", sleeve)
            return None
        if shares < 1 or not px:
            skip(clock, ticker, "BUY", "shares < 1 after fees", sleeve)
            return None
        fee = order_fees(shares, px, "buy", fees)
        cost = shares * px + fee
        if cost > cash + 1e-6:
            shares = int((cash - fee) // px) if px > 0 else 0
            if shares < 1:
                skip(clock, ticker, "BUY",
                     "cash tied in open lots / fees", sleeve)
                return None
            fee = order_fees(shares, px, "buy", fees)
            cost = shares * px + fee
        cash -= cost
        rec = {
            "date": date, "clock": clock, "side": "BUY",
            "ticker": ticker, "shares": shares,
            "px": round(float(px), 4), "px_kind": kind,
            "sleeve": sleeve,
            "notional": round(shares * px, 2),
            "fee": round(fee, 2),
            "cost": round(cost, 2),
            "reason": reason,
            "status": "plan",
            "cash_after": round(cash, 2),
        }
        tickets.append(rec)
        return rec

    # ---- 09:30 -------------------------------------------------------
    if route_mover:
        if flatten_ok:
            for t in list(io_pos):
                if sell_lot(io_pos[t], OPEN_CLOCK,
                            "flatten .io → mover (open)", "open"):
                    io_pos.pop(t, None)
        if pol.get("rotate_mover"):
            still_mv = []
            for p in mv_pos:
                if sell_lot(p, OPEN_CLOCK, "rotate mover (open)", "open"):
                    continue
                still_mv.append(p)
            mv_pos = still_mv
        eq = cash
        for p in list(io_pos.values()) + mv_pos:
            px, _ = _px(p["ticker"], date, "open", cache)
            eq += p["shares"] * float(px or p.get("last_px") or p["entry_px"])
        already = sum(
            p["shares"] * float(p.get("last_px") or p["entry_px"])
            for p in mv_pos if p.get("side") == "BUY")
        room = min(cash, max(0.0, eq * float(pol.get("day_cap", 1.0)) - already))
        held = {p["ticker"] for p in mv_pos} | set(io_pos)
        long_hold_n = HOLD_SESSIONS[pol.get("long_hold", "1d")]
        for r in rank_calls(priced_buys, pol.get("long_rank", "cond"))[: pol.get("long_top_n", 10)]:
            t = str(r.get("ticker") or "").upper()
            if not t:
                continue
            if t in held:
                skip(OPEN_CLOCK, t, "BUY", "already held", "mover_long")
                continue
            px, kind = _px(t, date, "open", cache)
            if not px:
                skip(OPEN_CLOCK, t, "BUY", "no 09:30 open", "mover_long")
                continue
            bump = pol.get("sizeup", 1.0) if t in confirm else 1.0
            want = min(eq * float(pol.get("long_pct", 0.10)) * bump, room, cash)
            lot = try_buy(t, px, kind, OPEN_CLOCK, int(want // px),
                          f"mover BUY cond={_cond_score(r):+.0f}", "mover_long")
            if lot is None:
                continue
            room -= lot["notional"] + lot["fee"]
            lot["exit_date"] = _next_exit(cal, date, long_hold_n)
            mv_pos.append({
                "ticker": t, "shares": lot["shares"], "side": "BUY",
                "entry_px": px, "sleeve": "mover_long",
                "exit_date": lot["exit_date"],
                "last_px": px, "fee_in": lot["fee"],
                "notional": lot["notional"],
            })
            held.add(t)
            if room < 1 or cash < 1:
                break
        if not priced_buys:
            skip(OPEN_CLOCK, "", "BUY",
                 "not flatten (need S≥+1 and ≥5 priced BUYs and a prior book)",
                 "mover_long")
    elif hard_red_no_new and hard_red:
        for r in rank_calls(priced_buys, pol.get("long_rank", "cond"))[: pol.get("long_top_n", 10)]:
            t = str(r.get("ticker") or "").upper()
            if t:
                skip(OPEN_CLOCK, t, "BUY", "hard-red: no new buys", "mover_long")

    cash_after_0930 = cash

    # ---- 16:00 due 1d ------------------------------------------------
    still = []
    for p in mv_pos:
        if p.get("exit_date") == date:
            sell_lot(p, CLOSE_CLOCK, "mover 1d done", "close")
        else:
            still.append(p)
    mv_pos = still

    # ---- 16:00 scheduled .io recycle --------------------------------
    io_hold_key = pol.get("io_hold")
    if io_hold_key and not route_mover:
        for t in list(io_pos):
            lot = io_pos[t]
            if lot.get("exit_date") != date:
                continue
            if sell_lot(lot, CLOSE_CLOCK, f"io {io_hold_key} done", "close"):
                io_pos.pop(t, None)

    # ---- 16:00 .io refill --------------------------------------------
    today_book = book_map.get(date)
    io_book = today_book
    if io_book is None and pol.get("carry_last_book"):
        io_book = last_print
    skip_io = bool(pol.get("skip_blank_io")) and blank
    mover_names = [str(r.get("ticker") or "").upper()
                   for r in priced_buys if r.get("ticker")]
    if route_mover:
        if io_book is not None:
            for t in io_select_picks(io_book, pol, date=date, score=score,
                                    mover_buys=mover_names,
                                    top_n=int(pol.get("long_top_n") or 10)):
                skip(CLOSE_CLOCK, t, "BUY",
                     "mover day: leftover cash overnight", "io_core")
    elif skip_io:
        skip(CLOSE_CLOCK, "", "BUY",
             "blank morning S — skip .io refill", "io_core")
    elif hard_red_no_new and hard_red and not hard_red_io_ok:
        targets = io_select_picks(io_book or {}, pol, date=date, score=score,
                                 mover_buys=mover_names,
                                 top_n=int(pol.get("long_top_n") or 10)) if io_book else []
        if not targets and io_book is None:
            skip(CLOSE_CLOCK, "", "BUY", "hard-red: no new buys", "io_core")
        for t in targets:
            if t in io_pos:
                skip(CLOSE_CLOCK, t, "BUY", "already held", "io_core")
            else:
                skip(CLOSE_CLOCK, t, "BUY", "hard-red: no new buys", "io_core")
    elif io_book is not None:
        targets = io_select_picks(io_book, pol, date=date, score=score,
                                 mover_buys=mover_names,
                                 top_n=int(pol.get("long_top_n") or 10))
        new = [t for t in targets if t not in io_pos]
        for t in targets:
            if t in io_pos:
                skip(CLOSE_CLOCK, t, "BUY", "already held", "io_core")
        io_hold_n = HOLD_SESSIONS.get(io_hold_key) if io_hold_key else None
        io_exit = next_session(session_calendar(payload, books), date, io_hold_n) \
            if io_hold_n else None
        if io_hold_n and not io_exit:
            skip(CLOSE_CLOCK, "*", "BUY",
                 f"io {io_hold_key} cannot settle", "io_core")
            new = []
        if new and cash > 100:
            per = cash / len(new)
            for t in new:
                px, kind = _px(t, date, "close", cache)
                if not px:
                    skip(CLOSE_CLOCK, t, "BUY", "no close", "io_core")
                    continue
                lot = try_buy(t, px, kind, CLOSE_CLOCK, int(per // px),
                              f"io {pol.get('io_select', 'size')}:{pol.get('io_sleeve', '2w_size')}",
                              "io_core", allow_hard_red=can_buy_io)
                if lot:
                    io_pos[t] = {
                        "ticker": t, "shares": lot["shares"], "side": "BUY",
                        "entry_px": px, "sleeve": "io_core",
                        "last_px": px, "fee_in": lot["fee"],
                        "notional": lot["notional"], "entry_date": date,
                    }
        elif new:
            for t in new:
                skip(CLOSE_CLOCK, t, "BUY",
                     "cash tied in open lots / fees", "io_core")

    cash_after_1600 = cash
    holds = [_lot_view(lot, _px(lot["ticker"], date, "close", cache)[0])
             for lot in list(io_pos.values()) + mv_pos]

    why = []
    if score is None:
        why.append("morning S missing")
    else:
        why.append(f"S={score:+.2f}")
    if hard_red_no_new and hard_red:
        why.append("hard-red: no new buys; holds and due 1d exits stay")
    elif flatten_ok:
        why.append("flatten .io → mover at 09:30")
    else:
        why.append(
            f"no flatten ({len(priced_buys)} priced BUYs, "
            f"prior book={'yes' if prior else 'no'})")
    if route == "io":
        why.append("16:00 refill 2w_size from leftover cash; skip names already held")
    elif route == "mover":
        why.append("mover day: leftover cash stays overnight")

    buy_cost = round(sum(t.get("cost") or 0 for t in tickets if t["side"] == "BUY"), 2)
    sell_proceeds = round(sum(t.get("proceeds") or 0 for t in tickets if t["side"] == "SELL"), 2)
    holds_open = [
        _lot_view(lot, lot.get("last_px"))
        for lot in list((sim.get("open_io") or {}).values())
        + list(sim.get("open_mover") or [])
    ]
    would = plan_would_buy(
        date=date, pol=pol, cache=cache, priced_buys=priced_buys,
        confirm=confirm, io_book=io_book, cash_open=cash_open,
        holds_open=holds_open,
        live_buys={t["ticker"] for t in tickets if t["side"] == "BUY"},
        flatten_without_hard_red=flatten_without_hard_red,
        hard_red=bool(hard_red_no_new and hard_red),
        route_mover=route_mover,
    )

    return {
        "date": date,
        "policy": pol.get("name") or LIVE_POLICY,
        "generated": datetime.now().isoformat(timespec="seconds"),
        "score": score,
        "predict": pdir,
        "hard_red": bool(hard_red_no_new and hard_red),
        "flatten_ok": bool(flatten_ok),
        "n_priced_buys": len(priced_buys),
        "prior_book": bool(prior is not None),
        "route": route,
        "cash_open": round(cash_open, 2),
        "cash_after_0930": round(cash_after_0930, 2),
        "cash_after_1600": round(cash_after_1600, 2),
        "buy_cost": buy_cost,
        "sell_proceeds": sell_proceeds,
        "n_holds_open": len(holds_open),
        "holds_open": holds_open,
        "holds_after": holds,
        "would_buy": would,
        "tickets": tickets,
        "skipped": skipped,
        "why": "; ".join(why),
        "replay_sessions": sim.get("calendar") or [],
    }


def positions_doc(card: dict) -> dict:
    return {
        "date": card["date"],
        "policy": card["policy"],
        "generated": card.get("generated"),
        "cash": card["cash_open"],
        "io": [h for h in card.get("holds_open") or []
               if (h.get("sleeve") or "").startswith("io")],
        "mover": [h for h in card.get("holds_open") or []
                  if "mover" in (h.get("sleeve") or "")],
    }


def today_panel_html(card: dict) -> str:
    sc = "—" if card.get("score") is None else f"{card['score']:+.2f}"
    rcls = ("good" if card["route"] == "mover"
            else "hold" if card["route"] == "hold" else "")
    cards = (
        f"<div class='card'>Today<b>{_html.escape(card['date'])}</b></div>"
        f"<div class='card'>Score<b>{sc}</b></div>"
        f"<div class='card'>Route<b class='{rcls}'>{_html.escape(card['route'])}</b></div>"
        f"<div class='card'>Cash leftover<b>${card['cash_open']:,.0f}</b></div>"
        f"<div class='card'>Open lots<b>{card['n_holds_open']}</b></div>"
        f"<div class='card'>09:30 / 16:00 plans<b>"
        f"{sum(1 for t in card['tickets'] if t['clock']==OPEN_CLOCK)} / "
        f"{sum(1 for t in card['tickets'] if t['clock']==CLOSE_CLOCK)}</b></div>"
        f"<div class='card'>Skipped<b>{len(card['skipped'])}</b></div>"
        f"<div class='card'>Would-buy (flat)<b>"
        f"{len((card.get('would_buy') or {}).get('rows') or [])}</b></div>"
        f"<div class='card'>After 16:00 cash<b>${card['cash_after_1600']:,.0f}</b></div>"
    )

    def rows_holds(holds):
        if not holds:
            return "<tr><td colspan='7' class='why'>none — flat cash</td></tr>"
        out = []
        for h in holds:
            out.append(
                f"<tr><th>{_html.escape(str(h.get('ticker') or ''))}</th>"
                f"<td>{_html.escape(str(h.get('sleeve') or ''))}</td>"
                f"<td>{h.get('shares')}</td>"
                f"<td>${h.get('entry_px') or 0:.2f}</td>"
                f"<td>{_html.escape(str(h.get('entry_date') or ''))}</td>"
                f"<td>${h.get('last_px') or 0:.2f}</td>"
                f"<td>${h.get('mv') or 0:,.0f}</td></tr>")
        return "".join(out)

    def rows_tix(clock):
        chunk = [t for t in card["tickets"] if t["clock"] == clock]
        skips = [s for s in card["skipped"] if s.get("clock") == clock]
        if not chunk and not skips:
            return "<tr><td colspan='8' class='why'>none</td></tr>"
        out = []
        for t in chunk:
            cls = "good" if t["side"] == "BUY" else "bad"
            out.append(
                f"<tr><th>{_html.escape(t['side'])}</th>"
                f"<td>{_html.escape(t['ticker'])}</td>"
                f"<td>{_html.escape(t.get('sleeve') or '')}</td>"
                f"<td>{t.get('shares')}</td>"
                f"<td>${t.get('px') or 0:.2f} <span class='muted'>{_html.escape(t.get('px_kind') or '')}</span></td>"
                f"<td>${t.get('notional') or 0:,.0f}</td>"
                f"<td>${t.get('fee') or 0:.2f}</td>"
                f"<td class='why {cls}'>{_html.escape(t.get('reason') or '')}</td></tr>")
        for s in skips:
            out.append(
                f"<tr><th class='hold'>SKIP</th>"
                f"<td>{_html.escape(s.get('ticker') or '—')}</td>"
                f"<td>{_html.escape(s.get('sleeve') or '')}</td>"
                f"<td></td><td></td><td></td><td></td>"
                f"<td class='why'>{_html.escape(s.get('reason') or '')}</td></tr>")
        return "".join(out)

    def futubull_strip() -> str:
        last_p = OUT_DIR / "futubull_last.json"
        if not last_p.is_file():
            return ("<p class='muted'>Futubull: not wired from this host. "
                    "Run <code>python -m src.futubull_exec</code> on a box "
                    "with OpenD (paper first).</p>")
        try:
            last = json.loads(last_p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return ""
        if last.get("connected"):
            st = (f"Futubull {last.get('env')} cash ${last.get('cash') or 0:,.0f} · "
                  f"{last.get('n_tickets') or 0} live tickets · "
                  f"{'submitted' if last.get('submit') else 'dry-run'}")
        else:
            st = f"Futubull offline — {_html.escape(str(last.get('error') or 'OpenD not running'))}"
        return f"<p class='muted'>{st}</p>"

    def rows_would(would):
        chunk = would.get("rows") or []
        if not chunk:
            return "<tr><td colspan='7' class='why'>none</td></tr>"
        out = []
        for t in chunk:
            cls = "good" if t.get("blocked") == "live" else "hold"
            out.append(
                f"<tr><th>{_html.escape(t.get('clock') or '')}</th>"
                f"<td>{_html.escape(t.get('ticker') or '')}</td>"
                f"<td>{_html.escape(t.get('sleeve') or '')}</td>"
                f"<td>{t.get('shares')}</td>"
                f"<td>${t.get('px') or 0:.2f}</td>"
                f"<td>${t.get('notional') or 0:,.0f}</td>"
                f"<td class='why {cls}'>{_html.escape(t.get('blocked') or '')}</td></tr>")
        return "".join(out)

    return f"""<style>
.today{{border:1px solid #fbbf24;border-radius:12px;padding:12px 14px;margin:16px 0;background:#16120a}}
.today h2{{margin-top:0}}
</style>
<section class="today" id="today-card">
<h2>Today — {_html.escape(card['policy'])} · {_html.escape(card['date'])}</h2>
<p class="muted">{_html.escape(card.get('why') or '')}.
Sells settle first. Buys only from leftover cash after Futubull fees.
Already-held names are not re-bought. Hard-red S≤−3 = no new risk.
Would-buy is not sent to Futubull.</p>
{futubull_strip()}
<div class="cards">{cards}</div>
<h3>Open holdings (cash is tied here)</h3>
<div class="sheet"><table>
<thead><tr><th>Ticker</th><th>Sleeve</th><th>Shares</th>
<th>Entry</th><th>Since</th><th>Mark</th><th>MV</th></tr></thead>
<tbody>{rows_holds(card.get('holds_open') or [])}</tbody></table></div>
<h3>09:30 tickets</h3>
<div class="sheet"><table>
<thead><tr><th>Side</th><th>Ticker</th><th>Sleeve</th><th>Shares</th>
<th>Px</th><th>Notional</th><th>Fee</th><th>Why</th></tr></thead>
<tbody>{rows_tix(OPEN_CLOCK)}</tbody></table></div>
<h3>16:00 tickets</h3>
<div class="sheet"><table>
<thead><tr><th>Side</th><th>Ticker</th><th>Sleeve</th><th>Shares</th>
<th>Px</th><th>Notional</th><th>Fee</th><th>Why</th></tr></thead>
<tbody>{rows_tix(CLOSE_CLOCK)}</tbody></table></div>
<h3>Would have bought — holdings disregarded</h3>
<p class="muted">Same sleeve the live rule points at, sized from marked equity
(${(card.get('would_buy') or {}).get('equity') or 0:,.0f}) as if the book
were flat. Not live tickets.
{( _html.escape((card.get('would_buy') or {}).get('source') or ''))}</p>
<div class="sheet"><table>
<thead><tr><th>Clock</th><th>Ticker</th><th>Sleeve</th><th>Shares</th>
<th>Px</th><th>Notional</th><th>Blocked live by</th></tr></thead>
<tbody>{rows_would(card.get('would_buy') or {})}</tbody></table></div>
</section>"""


def inject_today_panel(html: str, panel: str) -> str:
    block = f"<!-- TODAY_BEGIN -->\n{panel}\n<!-- TODAY_END -->"
    if "<!-- TODAY_BEGIN -->" in html:
        return TODAY_MARK_RE.sub(block, html)
    needle = "</p>\n<div class=\"cards\">"
    if needle in html:
        return html.replace(needle, f"</p>\n{block}\n<div class=\"cards\">", 1)
    if "<main>" in html:
        return html.replace("<main>", f"<main>\n{block}", 1)
    return block + html


def inject_today_from_disk(path: Path | None = None) -> bool:
    path = path or (DASH_DIR / "index.html")
    if not TODAY_JSON.is_file() or not path.is_file():
        return False
    try:
        card = json.loads(TODAY_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    html = path.read_text(encoding="utf-8")
    path.write_text(inject_today_panel(html, today_panel_html(card)),
                    encoding="utf-8")
    return True


def card_markdown(card: dict) -> str:
    sc = "—" if card.get("score") is None else f"{card['score']:+.2f}"
    lines = [
        f"# flatten_hard_red card — {card['date']}",
        "",
        f"_Generated {card.get('generated')} — live `{card['policy']}`._",
        "",
        f"**{card.get('why')}**",
        "",
        f"- Score **{sc}** ({card.get('predict') or '—'}) · route **{card['route']}**"
        f"{' · HARD-RED' if card.get('hard_red') else ''}",
        f"- Cash leftover **${card['cash_open']:,.2f}** "
        f"(after 09:30 ${card['cash_after_0930']:,.2f} · "
        f"after 16:00 ${card['cash_after_1600']:,.2f})",
        f"- Open lots **{card['n_holds_open']}** · "
        f"priced mover BUYs **{card['n_priced_buys']}** · "
        f"prior book {'yes' if card.get('prior_book') else 'no'}",
        f"- Planned buy cost **${card['buy_cost']:,.2f}** ≤ leftover "
        f"after sells **${card['cash_open'] + card['sell_proceeds']:,.2f}**",
        "",
        "## Open holdings",
        "",
        "| Ticker | Sleeve | Shares | Entry | Since | Mark | MV |",
        "|---|---|---:|---:|---|---:|---:|",
    ]
    if not card.get("holds_open"):
        lines.append("| — | — | | | | | |")
    for h in card.get("holds_open") or []:
        lines.append(
            f"| {h.get('ticker')} | {h.get('sleeve')} | {h.get('shares')} | "
            f"${h.get('entry_px') or 0:.2f} | {h.get('entry_date') or ''} | "
            f"${h.get('last_px') or 0:.2f} | ${h.get('mv') or 0:,.0f} |")
    lines += [
        "",
        "## Tickets",
        "",
        "| Clock | Side | Ticker | Sleeve | Shares | Px | $ | Why |",
        "|---|---|---|---|---:|---:|---:|---|",
    ]
    if not card.get("tickets"):
        lines.append("| — | — | | | | | | no new tickets |")
    for t in card.get("tickets") or []:
        pxn = t.get("cost") or t.get("proceeds") or t.get("notional") or 0
        lines.append(
            f"| {t['clock']} | {t['side']} | {t['ticker']} | {t.get('sleeve')} | "
            f"{t.get('shares')} | ${t.get('px') or 0:.2f} | ${pxn:,.2f} | "
            f"{t.get('reason')} |")
    lines += [
        "",
        "## Skipped (cannot buy)",
        "",
        "| Clock | Ticker | Why |",
        "|---|---|---|",
    ]
    if not card.get("skipped"):
        lines.append("| — | — | |")
    for s in card.get("skipped") or []:
        lines.append(
            f"| {s.get('clock')} | {s.get('ticker') or '—'} | {s.get('reason')} |")
    would = card.get("would_buy") or {}
    lines += [
        "",
        "## Would have bought — holdings disregarded",
        "",
        f"Sized from marked equity **${would.get('equity') or 0:,.2f}** as if "
        f"the book were flat. `{would.get('source') or ''}`. Not live tickets.",
        "",
        "| Clock | Ticker | Sleeve | Shares | Px | $ | Blocked live by |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    if not would.get("rows"):
        lines.append("| — | — | | | | | |")
    for t in would.get("rows") or []:
        lines.append(
            f"| {t.get('clock')} | {t.get('ticker')} | {t.get('sleeve')} | "
            f"{t.get('shares')} | ${t.get('px') or 0:.2f} | "
            f"${t.get('notional') or 0:,.2f} | {t.get('blocked')} |")
    lines += [
        "",
        "Dashboard: https://sroyaltyy.github.io/fullscan/dashboard/sleeve-merge/",
        "",
    ]
    return "\n".join(lines)


def write_card(card: dict) -> dict[str, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    TODAY_JSON.write_text(json.dumps(card, indent=2), encoding="utf-8")
    POSITIONS_JSON.write_text(
        json.dumps(positions_doc(card), indent=2), encoding="utf-8")
    daily = DAILY_DIR / f"{card['date']}_flatten_card.md"
    daily.write_text(card_markdown(card), encoding="utf-8")
    html_path = DASH_DIR / "index.html"
    if html_path.is_file():
        html = html_path.read_text(encoding="utf-8")
        html_path.write_text(
            inject_today_panel(html, today_panel_html(card)), encoding="utf-8")
    else:
        html_path.write_text(
            "<!doctype html><html><head><meta charset='utf-8'>"
            "<title>flatten_hard_red today</title></head><body><main>"
            f"{today_panel_html(card)}</main></body></html>",
            encoding="utf-8")
    return {
        "today": TODAY_JSON,
        "positions": POSITIONS_JSON,
        "daily": daily,
        "dashboard": html_path,
    }


def run_card(date: str | None = None, capital: float = 100_000,
             write: bool = False) -> int:
    date = date or et_today()
    card = plan_today(date, capital)
    print(f"[sleeve-merge-card] {card['date']} route={card['route']} "
          f"S={card.get('score')} cash=${card['cash_open']:,.2f} "
          f"holds={card['n_holds_open']} tickets={len(card['tickets'])} "
          f"skipped={len(card['skipped'])}")
    print(f"[sleeve-merge-card] {card['why']}")
    for t in card["tickets"]:
        print(f"  {t['clock']} {t['side']:4s} {t['ticker']:6s} "
              f"n={t.get('shares')} @ {t.get('px')}  {t.get('reason')}")
    n_skip = 0
    for s in card["skipped"]:
        n_skip += 1
        if n_skip <= 12:
            print(f"  SKIP {s.get('clock')} {s.get('ticker') or '—':6s} "
                  f"{s.get('reason')}")
    if n_skip > 12:
        print(f"  … {n_skip - 12} more skips")
    would = card.get("would_buy") or {}
    print(f"[sleeve-merge-card] would-buy {would.get('source')} "
          f"n={len(would.get('rows') or [])} "
          f"equity=${would.get('equity') or 0:,.2f}")
    for t in (would.get("rows") or [])[:12]:
        print(f"  WOULD {t.get('clock')} {t.get('ticker'):6s} "
              f"n={t.get('shares')} @ {t.get('px')}  "
              f"blocked={t.get('blocked')}")
    if write:
        paths = write_card(card)
        for k, p in paths.items():
            print(f"[sleeve-merge-card] wrote {k} {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_card(write=True))
