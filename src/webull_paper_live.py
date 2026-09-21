"""Observe Webull *paper* cash, positions, and orders. Never places.

The in-app Paper Trading viewport is easy to lose. This writes a
sandbox snapshot the .io board can poll:

    python -m src.webull_paper_live           # connect + write
    python -m src.webull_paper_live --journal-only

Paper host only. Does not change flatten_robust or point at api.webull.com.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src import webull_exec as we

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
LIVE_JSON = ROOT / "data" / "webull_paper" / "live.json"
DASH_JSON = ROOT / "dashboard" / "webull-paper" / "live.json"
WORKING = {
    "SUBMITTED", "PENDING", "WORKING", "NEW", "ACK", "ACKNOWLEDGED",
    "QUEUED", "PARTIAL", "PARTIAL_FILLED", "PARTIALLY_FILLED",
}
FILLED = {"FILLED", "FILLED_ALL"}
DEAD = {"CANCELLED", "CANCELED", "REJECTED", "FAILED", "EXPIRED", "ERROR"}


def now_et() -> datetime:
    return datetime.now(ET)


def load_journal(date: str, root: Path | None = None) -> dict | None:
    root = Path(root or ROOT)
    path = root / "data" / "paper_open" / f"{date}_submit.json"
    if not path.is_file():
        return None
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


def _bucket(status: str) -> str:
    s = str(status or "").upper().replace(" ", "_")
    if s in FILLED:
        return "filled"
    if s in DEAD:
        return "dead"
    if s in WORKING or s in ("UNKNOWN", "INTENT", "RELEASING"):
        return "working"
    return "other"


def _slim_journal(doc: dict | None) -> dict:
    if not isinstance(doc, dict):
        return {}
    sent = []
    for row in doc.get("sent") or []:
        if not isinstance(row, dict):
            continue
        sent.append({
            "ticker": row.get("ticker"),
            "side": row.get("side"),
            "shares": row.get("shares"),
            "client_order_id": row.get("client_order_id"),
            "order_id": row.get("order_id"),
            "status": row.get("status"),
            "ok": row.get("ok"),
            "acknowledged_at": row.get("acknowledged_at"),
            "error": row.get("error"),
        })
    return {
        "date": doc.get("date"),
        "status": doc.get("status"),
        "standing": doc.get("standing"),
        "fill_status": doc.get("fill_status"),
        "prepared_at": doc.get("prepared_at"),
        "host": doc.get("host"),
        "cash": doc.get("cash"),
        "n_positions": doc.get("n_positions"),
        "sent": sent,
    }


def _index_orders(rows: list[dict]) -> dict[str, dict]:
    by: dict[str, dict] = {}
    for row in rows:
        for key in (row.get("client_order_id"), row.get("order_id")):
            if key:
                by[str(key)] = row
    return by


def join_journal(journal: dict | None, broker: list[dict]) -> list[dict]:
    """Each sent ticket + the broker row if the sandbox still knows it."""
    by = _index_orders(broker)
    out = []
    seen = set()
    for row in (journal or {}).get("sent") or []:
        if not isinstance(row, dict):
            continue
        coid = str(row.get("client_order_id") or "")
        oid = str(row.get("order_id") or "")
        found = by.get(coid) or by.get(oid)
        if found:
            seen.add(found.get("client_order_id") or found.get("order_id") or "")
            status = found.get("status") or row.get("status")
            out.append({
                **found,
                "journal_status": row.get("status"),
                "journal_ok": row.get("ok"),
                "on_broker": True,
                "bucket": _bucket(status),
            })
        else:
            status = str(row.get("status") or "UNKNOWN")
            out.append({
                "ticker": row.get("ticker"),
                "side": row.get("side"),
                "shares": row.get("shares"),
                "filled_qty": 0,
                "status": status,
                "client_order_id": coid,
                "order_id": oid,
                "journal_status": status,
                "journal_ok": row.get("ok"),
                "on_broker": False,
                "bucket": "missing" if row.get("ok") else _bucket(status),
                "acknowledged_at": row.get("acknowledged_at"),
                "error": row.get("error"),
            })
    for row in broker:
        key = row.get("client_order_id") or row.get("order_id") or ""
        if key and key in seen:
            continue
        if any(r.get("client_order_id") == row.get("client_order_id")
               and row.get("client_order_id")
               for r in out):
            continue
        out.append({**row, "on_broker": True, "journal_status": None,
                    "bucket": _bucket(row.get("status"))})
    return out


def _pos_rows(positions: dict) -> list[dict]:
    rows = []
    for ticker, rec in sorted((positions or {}).items()):
        if not isinstance(rec, dict):
            continue
        rows.append({
            "ticker": ticker,
            "shares": rec.get("shares"),
            "cost_px": rec.get("cost_px"),
            "last_px": rec.get("last_px"),
            "mv": rec.get("mv"),
        })
    return rows


def observe(*, date: str = "", api=None, journal: dict | None = None,
            journal_only: bool = False, write: bool = True,
            root: Path | None = None) -> dict:
    """Read-only sandbox snapshot. ``journal_only`` skips the broker."""
    root = Path(root or ROOT)
    clock = now_et()
    date = date or clock.date().isoformat()
    journal = journal if journal is not None else load_journal(date, root)
    slim = _slim_journal(journal)
    doc = {
        "date": date,
        "observed_at": clock.isoformat(timespec="seconds"),
        "host": we.PAPER_HOST,
        "env": "paper",
        "connected": False,
        "error": "",
        "account_tail": "",
        "cash": slim.get("cash"),
        "buying_power": None,
        "n_positions": slim.get("n_positions") or 0,
        "positions": [],
        "open_n": 0,
        "filled_n": 0,
        "missing_n": 0,
        "orders": [],
        "open_orders": [],
        "journal": slim,
        "source": "journal",
        "place": False,
    }
    if journal_only:
        doc["orders"] = join_journal(journal, [])
        doc["error"] = "journal-only — broker not queried"
        _count(doc)
        if write:
            write_live(doc, root=root)
        return doc

    api = api or we.PaperAPI("paper")
    if api.host != we.PAPER_HOST:
        doc["error"] = "refuse non-sandbox host"
        if write:
            write_live(doc, root=root)
        return doc
    if not api.connect():
        doc["error"] = api.err or "not connected"
        doc["orders"] = join_journal(journal, [])
        _count(doc)
        if write:
            write_live(doc, root=root)
        return doc
    snap = api.snapshot()
    if not snap.connected:
        doc["error"] = snap.error or "snapshot failed"
        doc["orders"] = join_journal(journal, [])
        _count(doc)
        if write:
            write_live(doc, root=root)
        return doc
    doc["connected"] = True
    doc["source"] = "sandbox"
    doc["cash"] = snap.cash
    doc["buying_power"] = snap.buying_power
    doc["positions"] = _pos_rows(snap.positions)
    doc["n_positions"] = len(doc["positions"])
    tail = str(snap.acc_id or api.account_id or "")
    doc["account_tail"] = tail[-4:] if tail else ""

    broker: list[dict] = []
    try:
        broker.extend(api.list_open_orders())
    except Exception as exc:
        doc["error"] = f"open orders: {exc}"[:200]
    try:
        broker.extend(api.list_history_orders(date, date))
    except Exception as exc:
        extra = f"history: {exc}"[:200]
        doc["error"] = (doc["error"] + " · " + extra).strip(" ·")
    for row in slim.get("sent") or []:
        coid = row.get("client_order_id")
        if not coid:
            continue
        if any(r.get("client_order_id") == coid for r in broker):
            continue
        detail = api.order_detail(str(coid))
        if detail:
            broker.append(detail)
    doc["orders"] = join_journal(journal, broker)
    doc["open_orders"] = [r for r in doc["orders"] if r.get("bucket") == "working"
                          and r.get("on_broker")]
    _count(doc)
    if write:
        write_live(doc, root=root)
    return doc


def _count(doc: dict) -> None:
    orders = doc.get("orders") or []
    doc["open_n"] = sum(1 for r in orders if r.get("bucket") == "working")
    doc["filled_n"] = sum(1 for r in orders if r.get("bucket") == "filled")
    doc["missing_n"] = sum(1 for r in orders if r.get("bucket") == "missing")


def write_live(doc: dict, *, root: Path | None = None) -> Path:
    root = Path(root or ROOT)
    payload = json.dumps(doc, indent=2, allow_nan=False)
    for dest in (root / "data" / "webull_paper" / "live.json",
                 root / "dashboard" / "webull-paper" / "live.json"):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(payload + "\n", encoding="utf-8")
    return dest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--journal-only", action="store_true",
                    help="paint the last ack journal; do not call the broker")
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)
    doc = observe(date=args.date, journal_only=args.journal_only,
                  write=not args.no_write)
    print(f"[webull-live] {doc['date']} host={doc['host']} "
          f"connected={doc['connected']} cash={doc.get('cash')} "
          f"open={doc['open_n']} filled={doc['filled_n']} "
          f"missing={doc['missing_n']} src={doc['source']}",
          flush=True)
    if doc.get("error"):
        print(f"[webull-live] {doc['error']}", flush=True)
    for row in doc.get("orders") or []:
        print(f"  {row.get('bucket')} {row.get('status')} "
              f"{row.get('side')} {row.get('ticker')} "
              f"n={row.get('shares')} filled={row.get('filled_qty')} "
              f"{row.get('client_order_id')} "
              f"{'broker' if row.get('on_broker') else 'journal-only'}",
              flush=True)
    return 0 if doc.get("connected") or args.journal_only else 2


if __name__ == "__main__":
    raise SystemExit(main())
