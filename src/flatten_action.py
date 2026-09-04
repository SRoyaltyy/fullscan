"""Holdings-aware buy/sell ACTION for flatten_switch_recycle.

Older mechanisms stamp a book (``stock_book_diag_signals``) or replay
independent paper sleeves (``paper_trade``, ``mover_paper``,
``lookback_action``). This module watches ONE cash account and emits
today's tickets from upstream prints only:

  pre-open   --clock open    morning S + mover BUY list + prior book
  book/close --clock close   today's 2w_size print + leftover cash
  post-close --clock close   same close tickets (idempotent)

Holdings always come from a leak-free replay of ``sleeve_merge`` through
the last completed clock. Today's 13:00–15:45 book is never used at
09:30. Tomorrow's score is never used at 16:00.

CLI: python -m src.flatten_action [--date D] [--clock open|close|both] [--write]
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src import sleeve_merge as sm

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
POLICY_PATH = ROOT / "00_grounding" / "flatten_action_policy.json"
OUT_DIR = ROOT / "data" / "flatten_action"
SCOREBOARD = ROOT / "03_scoreboard"
DAILY = ROOT / "01_daily"
DASH_DIR = ROOT / "dashboard" / "flatten-action"
PREDICT_MD = ROOT / "01_daily" / "general"
PAGES = "/fullscan/dashboard/flatten-action/"

_SCORE_LINE = re.compile(r"total_score:\s*\*\*(-?\d+(?:\.\d+)?)")
_SCORE_SNAP = re.compile(r"total score\s+(-?\d+(?:\.\d+)?)", re.I)

LOT_KEYS = (
    "ticker", "sleeve", "side", "shares", "entry_px", "entry_date",
    "entry_dt", "fee_in", "notional", "reason", "last_px", "exit_date",
)


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def winning_policy() -> dict:
    """Live knobs: grounding file, else the last sleeve_merge winner."""
    if POLICY_PATH.is_file():
        try:
            pol = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
            if isinstance(pol, dict) and pol.get("engine") == "flatten_switch":
                return {**sm.DEFAULT, **pol}
        except (OSError, json.JSONDecodeError):
            pass
    state = sm.OUT_DIR / "state.json"
    if state.is_file():
        try:
            doc = json.loads(state.read_text(encoding="utf-8"))
            pol = doc.get("policy") or {}
            if pol:
                return {**sm.DEFAULT, **pol}
        except (OSError, json.JSONDecodeError):
            pass
    return {**sm.DEFAULT, "name": "flatten_switch_recycle",
            "engine": "flatten_switch", "io_sleeve": "2w_size",
            "long_top_n": 10, "long_pct": 0.10, "day_cap": 1.00,
            "sizeup": 1.0, "allow_short": False, "min_buys": 5,
            "rotate_mover": True, "carry_last_book": True}


def score_from_predict_md(date: str) -> float | None:
    path = PREDICT_MD / f"{date}_predict.md"
    if not path.is_file():
        return None
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    m = _SCORE_LINE.search(text) or _SCORE_SNAP.search(text)
    return float(m.group(1)) if m else None


def morning_score(date: str, payload: dict) -> float | None:
    g = (payload.get("regime") or {}).get(date) or {}
    if g.get("predict_score") is not None:
        try:
            return float(g["predict_score"])
        except (TypeError, ValueError):
            pass
    return score_from_predict_md(date)


def public_lot(lot: dict) -> dict:
    return {k: lot[k] for k in LOT_KEYS if k in lot}


def prev_session(cal: list[str], date: str) -> str | None:
    prevs = [d for d in cal if d < date]
    return prevs[-1] if prevs else None


def holdings_asof(payload: dict, books: list, pol: dict, capital: float,
                  through: str | None, stop_after: str | None = None) -> dict:
    sim = sm.run_flatten_switch(
        payload, books, pol, capital,
        through=through, keep_open=True, stop_after=stop_after,
    )
    return {
        "cash": float(sim.get("cash") or 0),
        "io_pos": {t: public_lot(p) for t, p in (sim.get("io_pos") or {}).items()},
        "mv_pos": [public_lot(p) for p in (sim.get("mv_pos") or [])],
        "equity": (sim.get("curve") or [{}])[-1].get("equity") if sim.get("curve") else capital,
        "through": through,
        "stop_after": stop_after,
        "calendar": sim.get("calendar") or [],
    }


def gather_upstream(date: str, payload: dict, books: list, pol: dict,
                    clock: str) -> dict:
    book_map = sm.load_book_map(books)
    cal = sm.session_calendar(payload, books)
    if date not in cal:
        cal = sorted(set(cal + [date]))
    calls = [dict(r) for r in (payload.get("called_rows") or [])
             if r.get("date") == date]
    for r in calls:
        r["_conv"] = sm._conviction(r)
    buys = [r for r in calls if r.get("action_call") == "BUY"]
    priced = []
    for r in buys:
        t = str(r.get("ticker") or "").upper()
        px = sm._num(((r.get("session_bar") or sm._bar(t, date)) or {}).get("open"))
        if t and px:
            priced.append(r)
    missing: list[str] = []
    if not (PREDICT_MD / f"{date}_predict.md").is_file() \
            and (payload.get("regime") or {}).get(date) is None:
        missing.append("general_predict")
    if not any(r.get("date") == date for r in (payload.get("called_rows") or [])):
        missing.append("mover_lookback_action")
    today_book = book_map.get(date)
    if today_book is None and clock == "close":
        missing.append("stock_book")
    prior = sm._prior_book(book_map, cal, date, pol.get("book_for_flatten", "yesterday"))
    last_print = sm._prior_book(book_map, cal, date, "last")
    score = morning_score(date, payload)
    return {
        "date": date,
        "clock": clock,
        "calendar": cal,
        "score": score,
        "predict_dir": ((payload.get("regime") or {}).get(date) or {}).get("predict_dir"),
        "calls": calls,
        "buys": buys,
        "priced_buys": priced,
        "n_priced": len(priced),
        "today_book": today_book,
        "prior_book": prior,
        "last_print": last_print,
        "missing": missing,
        "book_tickers_today": sorted(sm.book_ticker_set(today_book or {})),
        "io_targets": sm.io_picks(today_book or last_print or {},
                                  pol.get("io_sleeve", "2w_size"))
                      if (today_book or last_print) else [],
    }


def route_flags(up: dict, holdings: dict, pol: dict) -> dict:
    min_buys = int(pol.get("min_buys", 5))
    score = up.get("score")
    have_buys = int(up.get("n_priced") or 0) >= min_buys
    green = score is not None and score >= float(pol.get("long_gate", 1.0))
    blank = score is None
    flat = not holdings.get("io_pos")
    book_mode = pol.get("book_for_flatten", "yesterday")
    prior = up.get("prior_book")
    flatten_ok = green and have_buys and (
        book_mode in ("none", None, False) or prior is not None)
    cash_mover = flat and have_buys and (
        (pol.get("mover_when_flat") and green)
        or (pol.get("blank_mover_when_flat") and blank))
    route_mover = bool(flatten_ok or cash_mover)
    why = []
    if route_mover and flatten_ok:
        why.append(f"flatten: S={score:+.2f} ≥ {pol.get('long_gate', 1):+.1f}, "
                   f"{up.get('n_priced')} priced BUYs, prior book in")
    elif route_mover:
        why.append("cash + green mover list (already flat)")
    else:
        if not green:
            why.append(f"S={score if score is not None else '—'} "
                       f"< {pol.get('long_gate', 1):+.1f} — stay in .io")
        if not have_buys:
            why.append(
                f"S={score if score is not None else '—'}; "
                f"{up.get('n_priced') or 0} priced mover BUYs "
                f"(need {min_buys}) — cannot flatten")
        if book_mode not in ("none", None, False) and prior is None:
            why.append("no prior book at 09:30 — cannot flatten")
    return {
        "flatten_ok": bool(flatten_ok),
        "cash_mover": bool(cash_mover),
        "route_mover": route_mover,
        "green": green,
        "have_buys": have_buys,
        "why": "; ".join(why) or "hold .io",
    }


def _ticket(side: str, ticker: str, shares: int | None, sleeve: str,
            clock: str, why: str, px=None, status: str = "proposed") -> dict:
    return {
        "side": side, "ticker": ticker, "shares": shares, "sleeve": sleeve,
        "clock": clock, "why": why, "px": px, "status": status,
    }


def propose_open(date: str, holdings: dict, up: dict, pol: dict) -> dict:
    flags = route_flags(up, holdings, pol)
    tickets: list[dict] = []
    skipped: list[dict] = []
    io_pos = holdings.get("io_pos") or {}
    mv_pos = holdings.get("mv_pos") or []
    if flags["route_mover"] and flags["flatten_ok"]:
        for t, lot in io_pos.items():
            tickets.append(_ticket(
                "SELL", t, int(lot.get("shares") or 0), "io_core",
                sm.OPEN_CLOCK, "flatten .io → mover (open)",
                lot.get("last_px") or lot.get("entry_px")))
    if flags["route_mover"] and pol.get("rotate_mover"):
        for lot in mv_pos:
            tickets.append(_ticket(
                "SELL", lot["ticker"], int(lot.get("shares") or 0),
                "mover_long", sm.OPEN_CLOCK, "rotate mover (open)",
                lot.get("last_px") or lot.get("entry_px")))
    if flags["route_mover"]:
        ranked = sm.rank_calls(list(up.get("priced_buys") or []),
                               pol.get("long_rank", "cond"))
        held = {lot["ticker"] for lot in mv_pos}
        if flags["flatten_ok"]:
            held = set()
        for r in ranked[: int(pol.get("long_top_n", 10))]:
            t = str(r.get("ticker") or "").upper()
            if not t or t in held:
                continue
            px = sm._num(((r.get("session_bar") or sm._bar(t, date)) or {}).get("open"))
            tickets.append(_ticket(
                "BUY", t, None, "mover_long", sm.OPEN_CLOCK,
                f"mover BUY cond={sm._cond_score(r):+.0f}",
                px, "proposed" if px else "awaiting_open"))
            held.add(t)
    else:
        for t, lot in io_pos.items():
            tickets.append(_ticket(
                "HOLD", t, int(lot.get("shares") or 0), "io_core",
                sm.OPEN_CLOCK, flags["why"],
                lot.get("last_px") or lot.get("entry_px")))
        for lot in mv_pos:
            tickets.append(_ticket(
                "HOLD", lot["ticker"], int(lot.get("shares") or 0),
                "mover_long", sm.OPEN_CLOCK, flags["why"],
                lot.get("last_px") or lot.get("entry_px")))
        if not io_pos and not mv_pos:
            tickets.append(_ticket(
                "HOLD", "CASH", None, "cash", sm.OPEN_CLOCK, flags["why"]))
    return {
        "date": date, "clock": "open", "policy": pol.get("name"),
        "route": "mover" if flags["route_mover"] else "io",
        "flags": flags, "tickets": tickets, "skipped": skipped,
        "holdings": holdings, "upstream": _slim_up(up),
    }


def propose_close(date: str, holdings: dict, up: dict, pol: dict) -> dict:
    flags = route_flags(up, holdings, pol)
    tickets: list[dict] = []
    skipped: list[dict] = []
    mv_pos = holdings.get("mv_pos") or []
    io_pos = holdings.get("io_pos") or {}
    for lot in mv_pos:
        if lot.get("exit_date") == date:
            tickets.append(_ticket(
                "SELL", lot["ticker"], int(lot.get("shares") or 0),
                "mover_long", sm.CLOSE_CLOCK, "mover 1d done",
                lot.get("last_px") or lot.get("entry_px")))
    io_book = up.get("today_book")
    if io_book is None and pol.get("carry_last_book"):
        io_book = up.get("last_print")
    skip_io = bool(pol.get("skip_blank_io") and up.get("score") is None)
    if io_book is not None and not flags["route_mover"] and not skip_io:
        targets = sm.io_picks(io_book, pol.get("io_sleeve", "2w_size"))
        held = set(io_pos)
        new = [t for t in targets if t not in held]
        cash = float(holdings.get("cash") or 0)
        if not new:
            tickets.append(_ticket(
                "HOLD", "IO_BOOK", None, "io_core", sm.CLOSE_CLOCK,
                "2w_size already held — no new names"))
        elif cash <= 100:
            for t in new:
                skipped.append({"date": date, "ticker": t, "side": "BUY",
                                "reason": "cash tied in open lots / fees"})
            tickets.append(_ticket(
                "HOLD", "CASH", None, "io_core", sm.CLOSE_CLOCK,
                f"leftover cash ${cash:,.2f} — new 2w names skip"))
        else:
            per = cash / len(new)
            for t in new:
                tickets.append(_ticket(
                    "BUY", t, None, "io_core", sm.CLOSE_CLOCK,
                    f"io {pol.get('io_sleeve', '2w_size')} leftover ${per:,.0f}",
                    None, "awaiting_close"))
    elif flags["route_mover"]:
        tickets.append(_ticket(
            "HOLD", "CASH", None, "cash", sm.CLOSE_CLOCK,
            "mover day — leftover cash overnight for the next green open"))
    return {
        "date": date, "clock": "close", "policy": pol.get("name"),
        "route": "mover" if flags["route_mover"] else "io",
        "flags": flags, "tickets": tickets, "skipped": skipped,
        "holdings": holdings, "upstream": _slim_up(up),
    }


def _slim_up(up: dict) -> dict:
    return {
        "score": up.get("score"),
        "n_priced": up.get("n_priced"),
        "n_buys": len(up.get("buys") or []),
        "missing": up.get("missing") or [],
        "has_today_book": up.get("today_book") is not None,
        "has_prior_book": up.get("prior_book") is not None,
        "priced_tickers": [str(r.get("ticker") or "").upper()
                           for r in (up.get("priced_buys") or [])],
        "io_targets": up.get("io_targets") or [],
    }


def run_clock(date: str, clock: str, *, write: bool = False,
              capital: float = 100_000) -> dict:
    payload = sm.load_payload()
    books = sm.list_books()
    pol = winning_policy()
    cal = sm.session_calendar(payload, books)
    if date not in cal:
        cal = sorted(set(cal + [date]))
    up = gather_upstream(date, payload, books, pol, clock)
    if clock == "open":
        prev = prev_session(cal, date)
        held = holdings_asof(payload, books, pol, capital, prev, None)
        card = propose_open(date, held, up, pol)
    else:
        held = holdings_asof(payload, books, pol, capital, date, "open")
        card = propose_close(date, held, up, pol)
    card["generated"] = datetime.now(ET).isoformat(timespec="seconds")
    card["capital"] = capital
    if write:
        write_outputs(card)
    return card


def write_outputs(card: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    SCOREBOARD.mkdir(parents=True, exist_ok=True)
    date, clock = card["date"], card["clock"]
    tickets_path = OUT_DIR / f"tickets_{date}_{clock}.json"
    tickets_path.write_text(json.dumps(card, indent=2, default=str), encoding="utf-8")
    (OUT_DIR / "state.json").write_text(json.dumps({
        "asof_date": date, "asof_clock": clock,
        "policy": card.get("policy"),
        "route": card.get("route"),
        "holdings": card.get("holdings"),
        "n_tickets": len(card.get("tickets") or []),
        "generated": card.get("generated"),
    }, indent=2, default=str), encoding="utf-8")

    md = render_markdown(card)
    daily = DAILY / f"{date}_flatten_action.md"
    if clock == "open":
        daily.write_text(md, encoding="utf-8")
    else:
        prev = daily.read_text(encoding="utf-8") if daily.is_file() else ""
        if prev and f"## Close {date}" not in prev:
            daily.write_text(prev.rstrip() + "\n\n" + md, encoding="utf-8")
        else:
            daily.write_text(md, encoding="utf-8")
    (SCOREBOARD / "FLATTEN_ACTION.md").write_text(md, encoding="utf-8")
    (DASH_DIR / "index.html").write_text(render_html(card), encoding="utf-8")
    print(f"[flatten-action] {date} {clock} route={card.get('route')} "
          f"tickets={len(card.get('tickets') or [])} → {tickets_path}")


def render_markdown(card: dict) -> dict | str:
    date, clock = card["date"], card["clock"]
    flags = card.get("flags") or {}
    held = card.get("holdings") or {}
    up = card.get("upstream") or {}
    io = held.get("io_pos") or {}
    mv = held.get("mv_pos") or []
    lines = [
        f"# Flatten ACTION — {date} {clock}",
        "",
        f"**Policy:** `{card.get('policy')}` · route **{card.get('route')}** · "
        f"{flags.get('why')}",
        "",
        f"S = {up.get('score') if up.get('score') is not None else '—'} · "
        f"priced mover BUYs = {up.get('n_priced')} · "
        f"prior book = {'yes' if up.get('has_prior_book') else 'no'} · "
        f"today book = {'yes' if up.get('has_today_book') else 'no'}",
        "",
    ]
    if up.get("missing"):
        lines.append("**Waiting on upstream:** " + ", ".join(up["missing"]))
        lines.append("")
    lines += [
        f"**Holdings** cash ${float(held.get('cash') or 0):,.2f} · "
        f".io {len(io)} names · mover {len(mv)} lots",
        "",
        "| Side | Ticker | Shares | Sleeve | Clock | Why |",
        "|---|---|---:|---|---|---|",
    ]
    for t in card.get("tickets") or []:
        sh = t.get("shares")
        lines.append(
            f"| {t.get('side')} | {t.get('ticker')} | "
            f"{sh if sh is not None else '—'} | {t.get('sleeve')} | "
            f"{t.get('clock')} | {t.get('why')} |"
        )
    skips = card.get("skipped") or []
    if skips:
        lines += ["", "## Skipped (cash lockup)", ""]
        for s in skips:
            lines.append(f"- {s.get('date')} {s.get('ticker')} {s.get('reason')}")
    lines += [
        "",
        "Older ACTION (`stock_book_diag`, lookback stamp, per-sleeve paper) "
        "does not watch this account. These tickets are the flatten-switch "
        "book: one cash pile, Futubull fees, whole shares.",
        "",
    ]
    return "\n".join(lines) + "\n"


def render_html(card: dict) -> str:
    import html as _html
    date, clock = card["date"], card["clock"]
    flags = card.get("flags") or {}
    held = card.get("holdings") or {}
    up = card.get("upstream") or {}
    io = held.get("io_pos") or {}
    rows = []
    for t in card.get("tickets") or []:
        sh = t.get("shares")
        cls = "good" if t.get("side") == "BUY" else (
            "bad" if t.get("side") == "SELL" else "")
        rows.append(
            f"<tr><td class='{cls}'>{_html.escape(str(t.get('side')))}</td>"
            f"<th>{_html.escape(str(t.get('ticker')))}</th>"
            f"<td>{sh if sh is not None else '—'}</td>"
            f"<td>{_html.escape(str(t.get('sleeve') or ''))}</td>"
            f"<td>{_html.escape(str(t.get('clock') or ''))}</td>"
            f"<td class='why'>{_html.escape(str(t.get('why') or ''))}</td></tr>"
        )
    hold_rows = []
    for t, lot in io.items():
        hold_rows.append(
            f"<tr><td>io</td><th>{_html.escape(t)}</th>"
            f"<td>{lot.get('shares')}</td>"
            f"<td>{lot.get('entry_px')}</td></tr>")
    for lot in held.get("mv_pos") or []:
        hold_rows.append(
            f"<tr><td>mover</td><th>{_html.escape(str(lot.get('ticker')))}</th>"
            f"<td>{lot.get('shares')}</td>"
            f"<td>{lot.get('entry_px')}</td></tr>")
    miss = up.get("missing") or []
    miss_html = ("<p class='muted'>Waiting on: "
                 + _html.escape(", ".join(miss)) + "</p>") if miss else ""
    return f"""<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Flatten ACTION — {date} {clock}</title>
<style>
:root{{--bg:#0b1020;--card:#131b31;--line:#2b3552;--text:#edf2ff;--muted:#9cabc9}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui}}
main{{max-width:1100px;margin:auto;padding:16px}}h1,h2{{margin:.4em 0}}
.muted{{color:var(--muted)}}a{{color:#93c5fd}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px;margin:12px 0}}
.sheet{{overflow-x:auto;border:1px solid var(--line);border-radius:12px;margin:14px 0}}
table{{border-collapse:separate;border-spacing:0;width:100%;background:var(--card)}}
th,td{{padding:7px 8px;text-align:center;border-bottom:1px solid var(--line);white-space:nowrap}}
thead th{{position:sticky;top:0;background:#17213a}}
tbody th{{background:#17213a;text-align:left}}
td.good{{color:#4ade80}}td.bad{{color:#f87171}}
td.why{{text-align:left;white-space:normal;max-width:360px;font-size:12px}}
</style></head><body><main>
<h1>Flatten ACTION — {date} {clock}</h1>
<p class="muted"><a href="/fullscan/dashboard/">.io paper</a>
 · <a href="/fullscan/dashboard/sleeve-merge/">combine book</a>
 · <a href="/fullscan/dashboard/mover-paper/">mover paper</a></p>
<div class="card">
<b>route { _html.escape(str(card.get('route'))) }</b>
<div class="muted">{_html.escape(str(flags.get('why') or ''))}</div>
<div class="muted">S = {up.get('score') if up.get('score') is not None else '—'}
 · priced BUYs {up.get('n_priced')}
 · cash ${float(held.get('cash') or 0):,.2f}
 · .io {len(io)} · mover {len(held.get('mv_pos') or [])}</div>
</div>
{miss_html}
<h2>Tickets</h2>
<div class="sheet"><table>
<thead><tr><th>Side</th><th>Ticker</th><th>Shares</th><th>Sleeve</th><th>Clock</th><th>Why</th></tr></thead>
<tbody>{''.join(rows) or '<tr><td colspan="6">none</td></tr>'}</tbody>
</table></div>
<h2>Holdings watched</h2>
<div class="sheet"><table>
<thead><tr><th>Book</th><th>Ticker</th><th>Shares</th><th>Entry</th></tr></thead>
<tbody>{''.join(hold_rows) or '<tr><td colspan="4">flat cash</td></tr>'}</tbody>
</table></div>
<p class="muted">Policy { _html.escape(str(card.get('policy'))) } ·
generated { _html.escape(str(card.get('generated') or '')) }.
This is the live ACTION for flatten_switch_recycle — not the old per-sleeve paper stamp.</p>
</main></body></html>
"""


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--clock", choices=("open", "close", "both"), default="open")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--capital", type=float, default=100_000)
    args = ap.parse_args(argv)
    date = args.date or _today()
    clocks = ("open", "close") if args.clock == "both" else (args.clock,)
    for clock in clocks:
        try:
            card = run_clock(date, clock, write=args.write, capital=args.capital)
        except SystemExit as e:
            print(f"[flatten-action] WARN: {e}")
            return 0
        flags = card.get("flags") or {}
        print(f"[flatten-action] {date} {clock} route={card.get('route')} "
              f"tickets={len(card.get('tickets') or [])} · {flags.get('why')}")
        for t in card.get("tickets") or []:
            sh = t.get("shares")
            print(f"  {t.get('side'):4} {t.get('ticker'):6} "
                  f"{'' if sh is None else sh}  {t.get('why')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
