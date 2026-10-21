"""One-shot: bring the Webull SANDBOX account in line with the sealed h1 book.

Target = holdings on the last sealed h1 state record (read-only; the book
is never rewritten). Live sandbox positions are re-read at run time; a
failed read places nothing. Orders = held - book per ticker: excess and
names the book does not hold are sold, shortfalls are bought. Each order
gets an add-only note in data/paper_open/reconcile_log.jsonl.

Dry run is the default. --submit only on the sandbox host, only 09:40-15:50
ET on a weekday, and client_order_id uses strategy "rc" so it never
collides with the day's h1 send ids.
Run: python3 -m src.webull_reconcile [--submit] [--no-buys]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "data" / "paper_open" / "reconcile_log.jsonl"
ET = ZoneInfo("America/New_York")


class ReconcileError(RuntimeError):
    pass


def book_holdings(records=None) -> tuple[dict[str, int], str]:
    from src import h1_sealed_exec as h
    recs = records if records is not None else h.read_h1_records()
    state = h._state_before(recs, "9999-12-31")
    if not state:
        raise ReconcileError("no sealed h1 state record; refusing")
    out = {}
    for lot in state.get("holdings") or []:
        t = str((lot or {}).get("ticker") or "").upper().strip()
        n = int(float(lot.get("shares") or 0))
        if t and n > 0:
            out[t] = out.get(t, 0) + n
    return out, f"{state.get('kind')}:{state.get('date')}"


def _held(positions) -> dict[str, int]:
    out = {}
    for t, v in (positions or {}).items():
        n = v.get("shares", v.get("qty", 0)) if isinstance(v, dict) else v
        try:
            n = int(float(n))
        except (TypeError, ValueError):
            raise ReconcileError(f"unreadable sandbox qty for {t}: {v!r}")
        if n > 0:
            out[str(t).upper()] = n
    return out


def plan_orders(book: dict[str, int], held: dict[str, int], *, buys=True) -> list[dict]:
    orders = []
    for t in sorted(set(book) | set(held)):
        b, h = book.get(t, 0), held.get(t, 0)
        if h > b:
            orders.append({"ticker": t, "side": "SELL", "shares": h - b, "book": b, "held": h})
        elif b > h and buys:
            orders.append({"ticker": t, "side": "BUY", "shares": b - h, "book": b, "held": h})
    # sells first so buys use the freed cash
    return sorted(orders, key=lambda o: (o["side"] != "SELL", o["ticker"]))


def submit_window_ok(now: datetime) -> bool:
    et = now.astimezone(ET)
    hm = et.hour * 100 + et.minute
    return et.weekday() < 5 and 940 <= hm < 1550


def append_note(rec: dict, path: Path = LOG) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, sort_keys=True) + "\n")


def _place(api, order: dict, date: str) -> dict:
    from src import webull_exec as we
    body = we.order_body({**order, "date": date})
    body["client_order_id"] = we.client_order_id(date, order["side"], order["ticker"], strategy="rc")
    try:
        payload = api._json(api.trade.order_v3.place_order(api.account_id, [body]), "place_order")
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)[:240], "client_order_id": body["client_order_id"]}
    oid = we.parse_order_id(payload)
    return {"ok": bool(oid), "order_id": oid, "client_order_id": body["client_order_id"]}


def run(*, submit=False, buys=True, api=None, now=None, records=None, log=LOG) -> list[dict]:
    from src import webull_exec as we
    now = now or datetime.now(ET)
    date = now.astimezone(ET).date().isoformat()
    if submit and not submit_window_ok(now):
        raise ReconcileError("submit only 09:40-15:50 ET on a weekday; nothing placed")
    api = api or we.PaperAPI("paper")
    if getattr(api, "host", None) != we.PAPER_HOST:
        raise ReconcileError("refusing any non-sandbox host")
    if hasattr(api, "connect") and getattr(api, "trade", None) is None:
        api.connect()
    snap = api.snapshot()
    if snap is None or not getattr(snap, "connected", False):
        raise ReconcileError("sandbox position read failed; nothing placed (fail closed)")
    held = _held(getattr(snap, "positions", None) or {})
    book, src = book_holdings(records)
    orders = plan_orders(book, held, buys=buys)
    for o in orders:
        res = _place(api, o, date) if submit else {"ok": None, "status": "dry_run"}
        append_note({
            "kind": "reconcile", "date": date, "at": now.isoformat(), "host": we.PAPER_HOST,
            "book_source": src, **o, "submitted": bool(submit), **res,
            "note": (f"sandbox {o['ticker']} held {o['held']} vs h1 book {o['book']}; "
                     f"{o['side']} {o['shares']} to match the book; sealed h1 book unchanged"),
        }, log)
    print(json.dumps({"date": date, "submit": submit, "book_source": src, "orders": orders}, indent=1))
    return orders


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--submit", action="store_true")
    ap.add_argument("--no-buys", action="store_true")
    a = ap.parse_args(argv)
    run(submit=a.submit, buys=not a.no_buys)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
