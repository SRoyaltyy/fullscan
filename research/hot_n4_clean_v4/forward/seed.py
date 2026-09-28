"""Seed the 31 locked sessions from the committed day cards.

Does not rebuild a candidate list. Fills and P&L come from the same
functions the v4 score uses. The run writes the log only when it is empty,
and only after the book matches ``returns/RESULTS.json``.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.book import H1, current_book  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import (  # noqa: E402
    append_records,
    load,
    session_dates,
)
from research.hot_n4_clean_v4.forward.render import write_page  # noqa: E402
from research.hot_n4_clean_v4.forward.step import fresh_state, step  # noqa: E402
from research.hot_n4_clean_v4.protocol import (  # noqa: E402
    DAYS,
    FORWARD,
    SESSIONS,
    SLIP_PRIMARY,
    TUNE,
)
from research.hot_n4_clean_v4.run_study import (  # noqa: E402
    load_bars,
    load_fees,
    nyse_sessions,
    simulate,
    summarize_window,
)
from research.hot_n4_clean_v4.protocol import VARIANTS  # noqa: E402

RETURNS = ROOT / "research" / "hot_n4_clean_v4" / "returns" / "RESULTS.json"


def _index() -> dict[str, int]:
    calendar = nyse_sessions("2026-01-01", "2026-12-31")
    if nyse_sessions("2026-08-13", "2026-09-25") != list(SESSIONS):
        raise SystemExit("calendar")
    return {day: i for i, day in enumerate(calendar)}


def _payloads() -> list[dict]:
    ledger = {}
    for line in (DAYS / "LEDGER.jsonl").read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        ledger[row["date"]] = row["sha256"]
    out = []
    for session in SESSIONS:
        raw = (DAYS / f"{session}.json").read_bytes()
        payload = json.loads(raw)
        if ledger[session] != __import__("hashlib").sha256(raw).hexdigest():
            raise SystemExit(f"day card hash {session}")
        payload["day_card_sha256"] = ledger[session]
        out.append(payload)
    return out


def _same_trade(got: dict, exp: dict, side: str) -> None:
    if got["ticker"] != exp["ticker"] or int(got["shares"]) != int(exp["shares"]):
        raise SystemExit(f"{side} identity {got['ticker']} {exp['ticker']}")
    if float(got["fill"]) != float(exp["open"]):
        raise SystemExit(f"{side} fill {got['ticker']} {got['fill']} {exp['open']}")


def assert_matches_returns(payloads: list[dict], bars, fees, bodies: list[dict]) -> None:
    """The seeded book equals simulate(), and simulate() equals returns/."""
    recipe = current_book().recipe
    variant = next(row for row in VARIANTS if row["id"] == recipe)
    book = simulate(payloads, variant, bars, fees)
    by_day: dict[str, dict] = {}
    for body in bodies:
        by_day.setdefault(body["date"], {"buys": [], "sells": [], "closes": [], "session": None})
        if body["kind"] == "session":
            by_day[body["date"]]["session"] = body
            by_day[body["date"]]["buys"] = body["buys"]
            by_day[body["date"]]["sells"] = body["sells"]
        else:
            by_day[body["date"]]["closes"].append(body)
    buys = [row for row in book["trades"] if row["side"] == "buy"]
    sells = [row for row in book["trades"] if row["side"] == "sell"]
    got_buys = [row for body in bodies if body["kind"] == "session" for row in body["buys"]]
    got_sells = [row for body in bodies if body["kind"] == "session" for row in body["sells"]]
    if len(got_buys) != len(buys) or len(got_sells) != len(sells):
        raise SystemExit(f"trade count buys {len(got_buys)}/{len(buys)} sells {len(got_sells)}/{len(sells)}")
    for got, exp in zip(got_buys, buys):
        _same_trade(got, exp, "buy")
    for got, exp in zip(got_sells, sells):
        _same_trade(got, exp, "sell")
    closes = [body for body in bodies if body["kind"] == "close"]
    if len(closes) != len(sells):
        raise SystemExit("close count")
    for got, exp in zip(closes, sells):
        if got["ticker"] != exp["ticker"] or got["date"] != exp["date"]:
            raise SystemExit("close order")
        if float(got["pnl_primary"]) != float(exp["pnl"][SLIP_PRIMARY]):
            raise SystemExit(f"pnl {got['ticker']} {got['pnl_primary']} {exp['pnl'][SLIP_PRIMARY]}")
        if int(got["shares"]) != int(exp["shares"]) or float(got["fill"]) != float(exp["open"]):
            raise SystemExit("close fill")
    published = json.loads(RETURNS.read_text(encoding="utf-8"))["recipes"][recipe]["windows"]
    windows = {"before": list(TUNE), "after": list(FORWARD), "overall": list(SESSIONS)}
    for name, sessions in windows.items():
        stat = summarize_window(book, sessions)
        want = published[name]
        for key, label in (
            (0.0, "0.0"),
            (0.005, "0.005"),
            (0.01, "0.01"),
            ("15", "15"),
        ):
            if stat["returns"][str(key)] != want["returns"][label]:
                raise SystemExit(f"return {name} {label} {stat['returns'][str(key)]} {want['returns'][label]}")
        if stat["buys"] != want["buys"] or stat["closed"] != want["closed"]:
            raise SystemExit(f"counts {name}")
        if stat["wins"] != want["wins"] or stat["unfilled"] != want["unfilled"]:
            raise SystemExit(f"wins {name} {stat['wins']} {want['wins']} unfilled {stat['unfilled']} {want['unfilled']}")
        if stat["book_pnl"] != want["book_pnl"]:
            raise SystemExit(f"book pnl {name}")
    daily = {row["session"]: row for row in book["daily"]}
    for body in bodies:
        if body["kind"] != "session":
            continue
        if float(body["equity_primary"]) != float(daily[body["date"]][SLIP_PRIMARY]["equity"]):
            raise SystemExit(f"equity {body['date']}")
    if abs(sum(row["pnl_primary"] for row in closes) - published["overall"]["book_pnl"]) > 0:
        # book_pnl is the sum of primary closed P&L. Exact match was asserted per trade.
        raise SystemExit("pnl sum")
    skipped = [row for body in bodies if body["kind"] == "session" for row in body["unfilled"]]
    if len(skipped) != published["overall"]["unfilled"]:
        raise SystemExit("unfilled rows")


def build_bodies(payloads, bars, fees) -> list[dict]:
    index = _index()
    state = fresh_state()
    bodies = []
    for payload in payloads:
        session_body, closes, state = step(payload, state, bars, fees, index)
        bodies.append(session_body)
        bodies.extend(closes)
    assert_matches_returns(payloads, bars, fees, bodies)
    return bodies


def _h1_note(records: list[dict]) -> None:
    """Do not pre-declare 2026-09-28 missing. The 13:05 UTC PLAN seals it before 09:30 ET."""
    write_page(records, {
        "date": "2026-09-28",
        "latest_skip": None,
        "note": (
            "Seeded from the committed v4 day cards for union_hot_n4_h1__w0 "
            "and checked against returns/RESULTS.json. "
            "Session 2026-09-28 reads theme-radar data/snapshots/2026-09-25.csv. "
            "The 13:05 UTC PLAN run seals it when this is on main before 09:30 ET. "
            "It is marked missing only if no h1 plan is committed before that open. "
            "forward_shadow_v1 records union_hot_n4_h1__w0 for that day as a different study. "
            "A holdup plan already sealed is not written again."
        ),
        "pending": None,
        "phase": "awaiting-plan",
        "recipe": H1.recipe,
    })


def main() -> None:
    book = current_book()
    existing = load()
    if session_dates(existing) == list(SESSIONS):
        print("seed already written", book.recipe, len(existing), "records")
        if book is H1:
            _h1_note(existing)
        return
    if existing:
        raise SystemExit("log is not empty and is not the 31 seeded sessions")
    print("loading bars", book.recipe, flush=True)
    bars = load_bars()
    fees = load_fees()
    payloads = _payloads()
    for payload in payloads:
        if payload["excluded_unexplained_legs"] != json.loads(
            (DAYS / f"{payload['session']}.json").read_text(encoding="utf-8")
        )["excluded_unexplained_legs"]:
            raise SystemExit("excluded legs drifted")
    bodies = build_bodies(payloads, bars, fees)
    append_records(bodies)
    written = load()
    if book is H1:
        _h1_note(written)
    else:
        write_page(written, {
            "latest_skip": None,
            "note": "Seeded from the committed v4 day cards and checked against returns/RESULTS.json.",
            "recipe": book.recipe,
        })
    print("seeded", book.recipe, len(bodies), "records", "sessions", len(SESSIONS))


if __name__ == "__main__":
    main()
