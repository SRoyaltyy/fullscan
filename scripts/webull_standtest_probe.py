"""One-off paper probe: cash snapshot + optional STANDTEST MARKET/CORE/DAY.
Never uses --env real. Does not touch paper_open owner lock.
Modes: cash | place | status | cancel
"""
from __future__ import annotations
import json, os, sys, time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
OUT = Path("data/paper_open")

def now_et():
    return datetime.now(ET)

def write(name: str, doc: dict) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")

def dump(obj):
    print(json.dumps(obj, indent=2, default=str))

def connect():
    sys.path.insert(0, ".")
    from src.webull_exec import PaperAPI, order_body, parse_order_id
    api = PaperAPI("paper")
    if not api.connect():
        out = {"ok": False, "stage": "connect", "error": api.err, "et": now_et().isoformat()}
        dump(out); write("standtest_cash.json", out)
        return None, None, out
    snap = api.snapshot()
    return api, snap, None

def order_status(api, order_id: str = "", coid: str = ""):
    detail = None
    errors = []
    ov3 = api.trade.order_v3
    for meth_name, args in (
        ("get_order_detail", (api.account_id, order_id) if order_id else None),
        ("get_order", (api.account_id, order_id) if order_id else None),
        ("get_order_history", (api.account_id,) ),
        ("list_orders", (api.account_id,)),
        ("get_open_orders", (api.account_id,)),
        ("get_account_orders", (api.account_id,)),
    ):
        if args is None:
            continue
        meth = getattr(ov3, meth_name, None)
        if meth is None:
            continue
        try:
            detail = api._json(meth(*args), meth_name)
            return detail, meth_name, errors
        except Exception as e:
            errors.append(f"{meth_name}: {e}"[:200])
    # account_v2 fallbacks
    av2 = getattr(api.trade, "account_v2", None)
    if av2 is not None:
        for meth_name in ("get_account_open_orders", "get_open_orders", "get_order_list"):
            meth = getattr(av2, meth_name, None)
            if meth is None:
                continue
            try:
                detail = api._json(meth(api.account_id), meth_name)
                return detail, meth_name, errors
            except Exception as e:
                errors.append(f"account_v2.{meth_name}: {e}"[:200])
    return detail, None, errors

def extract_status(detail, order_id="", coid=""):
    if detail is None:
        return None, None
    rows = []
    def walk(v):
        if isinstance(v, list):
            for x in v: walk(x)
        elif isinstance(v, dict):
            if any(k in v for k in ("order_id", "orderId", "client_order_id", "clientOrderId", "status")):
                rows.append(v)
            for k in ("data", "orders", "result", "list", "items"):
                if k in v: walk(v[k])
    walk(detail)
    want_oid = str(order_id or "")
    want_coid = str(coid or "")
    pick = None
    for r in rows:
        oid = str(r.get("order_id") or r.get("orderId") or "")
        cid = str(r.get("client_order_id") or r.get("clientOrderId") or "")
        if want_oid and oid == want_oid:
            pick = r; break
        if want_coid and cid == want_coid:
            pick = r; break
    if pick is None and len(rows) == 1:
        pick = rows[0]
    if pick is None:
        # Prefer STANDTEST rows
        for r in rows:
            cid = str(r.get("client_order_id") or r.get("clientOrderId") or "")
            if cid.startswith("STANDTEST"):
                pick = r; break
    status = None
    if pick:
        status = pick.get("status") or pick.get("order_status") or pick.get("orderStatus")
    return status, pick

def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else "cash").strip().lower()
    api, snap, err = connect()
    if err:
        return 2
    out = {
        "ok": bool(snap.connected),
        "stage": "snapshot",
        "et": now_et().isoformat(),
        "cash": snap.cash,
        "buying_power": getattr(snap, "buying_power", None),
        "account_id_suffix": (snap.acc_id or "")[-6:],
        "n_positions": len(snap.positions or {}),
        "positions": sorted((snap.positions or {}).keys())[:20],
        "error": snap.error,
        "host": api.host,
    }
    dump(out)
    write("standtest_cash.json", out)
    if not snap.connected:
        return 2

    if mode == "cash":
        return 0

    if mode in ("status", "cancel"):
        oid = os.environ.get("STANDTEST_ORDER_ID", "").strip()
        coid = os.environ.get("STANDTEST_COID", "").strip()
        detail, meth, errors = order_status(api, oid, coid)
        status, pick = extract_status(detail, oid, coid)
        result = {
            **out,
            "stage": mode,
            "lookup_method": meth,
            "lookup_errors": errors[:8],
            "status": status,
            "order_row": pick,
            "order_id": oid or (pick or {}).get("order_id") or (pick or {}).get("orderId"),
            "client_order_id": coid or (pick or {}).get("client_order_id") or (pick or {}).get("clientOrderId"),
        }
        if mode == "cancel" and pick is not None:
            cancel_err = None
            try:
                oid2 = str(result["order_id"] or "")
                cancelled = False
                for name in ("cancel_order", "cancel", "cancel_order_v3"):
                    methc = getattr(api.trade.order_v3, name, None)
                    if methc is None or not oid2:
                        continue
                    try:
                        cres = api._json(methc(api.account_id, oid2), name)
                        result["cancel_payload"] = cres
                        cancelled = True
                        break
                    except Exception as e:
                        cancel_err = str(e)[:240]
                result["cancelled"] = cancelled
                if cancel_err:
                    result["cancel_error"] = cancel_err
            except Exception as e:
                result["cancel_error"] = str(e)[:240]
        dump(result)
        write("standtest_status.json", result)
        write("standtest_result.json", result)
        return 0

    # place
    if float(snap.cash or 0) <= 0:
        out2 = {**out, "stage": "submit_blocked", "reason": "cash<=0"}
        dump(out2); write("standtest_result.json", out2)
        return 3

    from src.webull_exec import order_body, parse_order_id
    ticker = os.environ.get("STANDTEST_TICKER", "AAPL").upper()
    qty = int(os.environ.get("STANDTEST_QTY", "1"))
    coid = os.environ.get("STANDTEST_COID") or f"STANDTEST-20260918-{int(time.time())}"
    coid = coid[:32]
    ticket = {
        "ticker": ticker,
        "side": "BUY",
        "shares": qty,
        "date": now_et().strftime("%Y-%m-%d"),
        "order_type": "MARKET",
    }
    body = order_body(ticket)
    body["client_order_id"] = coid
    try:
        res = api.trade.order_v3.place_order(api.account_id, [body])
        payload = api._json(res, "place_order")
    except Exception as e:
        result = {
            "ok": False, "stage": "place", "error": str(e)[:400],
            "client_order_id": coid,
            "body": {k: body[k] for k in body},
            "et": now_et().isoformat(), "cash": snap.cash,
        }
        dump(result); write("standtest_result.json", result)
        return 4

    oid = parse_order_id(payload) if isinstance(payload, (dict, list)) else ""
    result = {
        "ok": True,
        "stage": "placed",
        "et": now_et().isoformat(),
        "cash": snap.cash,
        "buying_power": getattr(snap, "buying_power", None),
        "client_order_id": coid,
        "order_id": oid,
        "ticker": ticker,
        "qty": qty,
        "order_type": body.get("order_type"),
        "session": body.get("support_trading_session"),
        "tif": body.get("time_in_force"),
        "payload_snip": json.dumps(payload, default=str)[:800],
    }
    try:
        detail, meth, errors = order_status(api, oid, coid)
        status, pick = extract_status(detail, oid, coid)
        result["lookup_method"] = meth
        result["lookup_errors"] = errors[:6]
        result["status"] = status
        result["order_row"] = pick
    except Exception as e:
        result["detail_error"] = str(e)[:240]
    dump(result)
    write("standtest_result.json", result)
    write("standtest_preopen.json", result)
    return 0 if result.get("ok") else 4

if __name__ == "__main__":
    raise SystemExit(main())
