"""Sent share counts survive a later open. A sealed open fill is not rewritten.

2026-10-08 stays the open-price size already on the log (NAUT 1,234, PENG 34).
The lock applies from 2026-10-09, and only when the morning submit journal
has a count.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.book import H1, HOLDUP, reset_book, use_book  # noqa: E402
from research.hot_n4_clean_v4.forward.openfill import decide_open_fill  # noqa: E402
from research.hot_n4_clean_v4.forward.planfill import fill_book  # noqa: E402
from research.hot_n4_clean_v4.forward.share_lock import (  # noqa: E402
    LOCK_FROM,
    SentShareLockMissing,
    resolve_share_lock,
)

FEES = {
    "commission_per_share": 0.0049,
    "commission_min_per_order": 0.99,
    "commission_max_pct_of_amount": 0.005,
    "platform_per_share": 0.005,
    "platform_min_per_order": 1.0,
    "platform_max_pct_of_amount": 0.005,
    "settlement_per_share": 0.003,
    "regulatory_pct_of_amount_sell_only": 0.0000278,
    "regulatory_min_per_order": 0.01,
    "taf_per_share_sell_only": 0.000166,
    "taf_min_per_order": 0.01,
    "taf_max_per_order": 8.3,
}
CASH = 5000.0
LOG = ROOT / "research" / "hot_n4_clean_v4" / "forward_h1" / "h1_log.jsonl"


def _plan(session: str, *tickers: str, cash: float = CASH) -> dict:
    picks = []
    for rank, ticker in enumerate(tickers, start=1):
        picks.append({
            "fv_avg_volume": 1_000_000,
            "fv_price": 2.0,
            "rank": rank,
            "sources": ["t"],
            "ticker": ticker,
        })
    return {
        "cash_before": cash,
        "date": session,
        "holdup_on": False,
        "kind": "plan",
        "morning_s": None,
        "picks": picks,
        "planned_sells": [],
        "sha256": "test",
    }


def _state(cash: float = CASH, held: dict | None = None) -> dict:
    pos = {}
    for ticker, shares in (held or {}).items():
        pos[ticker] = {
            "cost_primary": 10.0,
            "entry_date": "2026-10-01",
            "entry_px": 2.0,
            "last_px": 2.0,
            "min_hold": 1,
            "peak_px": 2.0,
            "shares": shares,
            "ticker": ticker,
        }
    return {"cash": cash, "pos": pos}


def _bars(session: str, prices: dict[str, float]) -> dict:
    stored = {}
    for ticker, op in prices.items():
        stored[ticker] = {
            "adjusted": True,
            "close": [op],
            "date": [session],
            "open": [op],
        }
    return {"feat": {}, "stored": stored}


def _buy_map(fill: dict) -> dict[str, int]:
    return {row["ticker"]: int(row["shares"]) for row in fill["buys"]}


def _one(session: str, op: float, **kwargs) -> dict:
    plan = kwargs.pop("plan", None) or _plan(session, "AAA")
    state = kwargs.pop("state", None) or _state()
    fill, _closes, _after = fill_book(
        plan, state, _bars(session, {"AAA": op}), FEES, {}, **kwargs,
    )
    return fill


def _journal(root: Path, date: str, shares: int, ticker: str = "AAA") -> None:
    folder = root / "data" / "paper_open"
    folder.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": date,
        "card": {"tickets": [{
            "side": "BUY",
            "ticker": ticker,
            "shares": shares,
            "sealed_shares": True,
        }]},
        "sent": [{
            "side": "BUY",
            "ticker": ticker,
            "shares": shares,
            "status": "acknowledged",
        }],
    }
    (folder / f"{date}_submit.json").write_text(json.dumps(payload), encoding="utf-8")


def _locked_count_ignores_open() -> None:
    cheap = _one("2026-10-09", 0.4, share_lock={"AAA": 100})
    rich = _one("2026-10-09", 10.0, share_lock={"AAA": 100})
    if _buy_map(cheap) != {"AAA": 100} or _buy_map(rich) != {"AAA": 100}:
        raise SystemExit(f"lock resized {_buy_map(cheap)} {_buy_map(rich)}")
    if float(cheap["buys"][0]["fill"]) != 0.4 or float(rich["buys"][0]["fill"]) != 10.0:
        raise SystemExit("fill price did not follow the open")
    if float(cheap["buys"][0]["fee"]) == float(rich["buys"][0]["fee"]):
        raise SystemExit("fee ignored the open")
    if float(cheap["holdings"][0]["cost_primary"]) == float(rich["holdings"][0]["cost_primary"]):
        raise SystemExit("cost ignored the open")
    open_sized_a = _buy_map(_one("2026-10-09", 2.0, share_lock=None))
    open_sized_b = _buy_map(_one("2026-10-09", 10.0, share_lock=None))
    if open_sized_a == open_sized_b:
        raise SystemExit(f"open price did not change the unlocked size {open_sized_a}")
    if open_sized_a == {"AAA": 100} or open_sized_b == {"AAA": 100}:
        raise SystemExit("unlocked size collided with the lock")


def _journal_lock_on_next_send() -> None:
    token = use_book(H1)
    try:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            os.environ["SENT_SHARE_LOCK_ROOT"] = str(root)
            try:
                try:
                    _one("2026-10-09", 2.0)
                except SentShareLockMissing:
                    pass
                else:
                    raise SystemExit("missing lock sized from the open")
                _journal(root, "2026-10-09", 100)
                cheap = _one("2026-10-09", 2.0)
                rich = _one("2026-10-09", 4.0)
                if _buy_map(cheap) != {"AAA": 100} or _buy_map(rich) != {"AAA": 100}:
                    raise SystemExit(f"journal lock resized {_buy_map(cheap)} {_buy_map(rich)}")
                if float(cheap["buys"][0]["fill"]) == float(rich["buys"][0]["fill"]):
                    raise SystemExit("journal lock froze the fill price")
            finally:
                os.environ.pop("SENT_SHARE_LOCK_ROOT", None)
    finally:
        reset_book(token)


def _october_8_journal_does_not_apply() -> None:
    if LOCK_FROM != "2026-10-09":
        raise SystemExit(f"lock start moved {LOCK_FROM}")
    token = use_book(H1)
    try:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            _journal(root, "2026-10-08", 7)
            os.environ["SENT_SHARE_LOCK_ROOT"] = str(root)
            try:
                if resolve_share_lock("2026-10-08") is not None:
                    raise SystemExit("2026-10-08 read the submit journal")
                cheap = _buy_map(_one("2026-10-08", 2.0))
                rich = _buy_map(_one("2026-10-08", 4.0))
            finally:
                os.environ.pop("SENT_SHARE_LOCK_ROOT", None)
    finally:
        reset_book(token)
    if cheap == rich or cheap == {"AAA": 7} or rich == {"AAA": 7}:
        raise SystemExit(f"2026-10-08 took the journal size {cheap} {rich}")


def _sealed_open_fill_not_rewritten() -> None:
    before = LOG.read_bytes()
    records = [json.loads(line) for line in before.decode().splitlines() if line.strip()]
    sealed = [
        row for row in records
        if row.get("kind") == "open_fill" and row.get("date") == "2026-10-08"
    ]
    if len(sealed) != 1:
        raise SystemExit(f"2026-10-08 open fills {len(sealed)}")
    buys = {row["ticker"]: int(row["shares"]) for row in sealed[0]["buys"]}
    prices = {row["ticker"]: float(row["fill"]) for row in sealed[0]["buys"]}
    if buys != {"NAUT": 1234, "PENG": 34} or prices != {"NAUT": 2.0, "PENG": 72.5}:
        raise SystemExit(f"sealed 2026-10-08 moved {buys} {prices}")
    bodies, _why = decide_open_fill(
        records, "2026-10-08", {"NAUT": 9.0, "PENG": 1.0}, {}, {}, {},
    )
    if bodies:
        raise SystemExit("decide_open_fill rewrote 2026-10-08")
    if LOG.read_bytes() != before:
        raise SystemExit("h1 log bytes changed")


def _carry_and_cash_refuse() -> None:
    plan = _plan("2026-10-09", "AAA", "BBB")
    state = _state(held={"BBB": 5})
    try:
        fill_book(
            plan, state, _bars("2026-10-09", {"AAA": 2.0, "BBB": 2.0}), FEES, {},
            share_lock={"AAA": 10, "BBB": 5},
        )
    except RuntimeError as exc:
        if "carry" not in str(exc):
            raise SystemExit(f"carry was resized {exc}") from exc
    else:
        raise SystemExit("carry lock was accepted")
    try:
        _one("2026-10-09", 50.0, share_lock={"AAA": 100000}, state=_state(cash=100.0), plan=_plan("2026-10-09", "AAA", cash=100.0))
    except RuntimeError as exc:
        if "refusing to resize" not in str(exc):
            raise SystemExit(f"cash shortfall resized {exc}") from exc
    else:
        raise SystemExit("cash shortfall resized the lock")


def _holdup_still_sizes_from_the_open() -> None:
    token = use_book(HOLDUP)
    try:
        with tempfile.TemporaryDirectory() as folder:
            os.environ["SENT_SHARE_LOCK_ROOT"] = str(folder)
            try:
                cheap = _buy_map(_one("2026-10-09", 2.0))
                rich = _buy_map(_one("2026-10-09", 4.0))
            finally:
                os.environ.pop("SENT_SHARE_LOCK_ROOT", None)
    finally:
        reset_book(token)
    if cheap == rich:
        raise SystemExit("holdup took a lock it does not have")


def _decide_open_fill_keeps_the_lock() -> None:
    session = "2026-10-09"
    records = [{
        "buys": [],
        "cash_primary": CASH,
        "date": "2026-10-08",
        "equity_primary": CASH,
        "holdings": [],
        "holdup_on": False,
        "kind": "session",
        "recipe": H1.recipe,
        "sells": [],
    }, _plan(session, "AAA")]
    prior = {
        "feat": {},
        "stored": {"AAA": {
            "adjusted": True,
            "close": [2.0],
            "date": ["2026-10-08"],
            "open": [2.0],
        }},
    }
    token = use_book(H1)
    try:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            _journal(root, session, 80)
            os.environ["SENT_SHARE_LOCK_ROOT"] = str(root)
            try:
                first, why = decide_open_fill(
                    records, session, {"AAA": 2.0, "IWM": 200.0}, prior, FEES, {},
                )
                second, _why = decide_open_fill(
                    records, session, {"AAA": 2.4, "IWM": 200.0}, prior, FEES, {},
                )
            finally:
                os.environ.pop("SENT_SHARE_LOCK_ROOT", None)
    finally:
        reset_book(token)
    if why or not first or not second:
        raise SystemExit(f"open fill did not seal {why} {bool(first)} {bool(second)}")
    if int(first[0]["buys"][0]["shares"]) != 80 or int(second[0]["buys"][0]["shares"]) != 80:
        raise SystemExit("decide_open_fill resized the sent count")
    if float(first[0]["buys"][0]["fill"]) == float(second[0]["buys"][0]["fill"]):
        raise SystemExit("decide_open_fill froze the open")


def main() -> None:
    _locked_count_ignores_open()
    _journal_lock_on_next_send()
    _october_8_journal_does_not_apply()
    _sealed_open_fill_not_rewritten()
    _carry_and_cash_refuse()
    _holdup_still_sizes_from_the_open()
    _decide_open_fill_keeps_the_lock()
    print("ok sent share lock")


if __name__ == "__main__":
    main()
