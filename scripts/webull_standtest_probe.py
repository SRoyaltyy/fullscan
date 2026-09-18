"""One-off paper probe: cash snapshot + optional STANDTEST MARKET/CORE/DAY.
Never uses --env real. Does not touch paper_open owner lock.
"""
from __future__ import annotations
import json, os, sys, time
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

def now_et():
    return datetime.now(ET)

def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else "cash").strip().lower()
    # import from repo
    sys.path.insert(0, ".")
    from src.webull_exec import PaperAPI, order_body, parse_order_id

    api = PaperAPI("paper")
    if not api.connect():
        out = {"ok": False, "stage": "connect", "error": api.err, "et": now_et().isoformat()}
        print(json.dumps(out, indent=2)); return 2
    snap = api.snapshot()
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
    print(json.dumps(out, indent=2))
    Path = __import__("pathlib").Path
    Path("data/paper_open").mkdir(parents=True, exist_ok=True)
    Path("data/paper_open/standtest_cash.json").write_text(json.dumps(out, indent=2))

    if mode == "cash":
        return 0 if snap.connected else 2
    if not snap.connected:
        return 2
    if float(snap.cash or 0) <= 0:
        out2 = {**out, "stage": "submit_blocked", "reason": "cash<=0"}
        print(json.dumps(out2, indent=2))
        Path("data/paper_open/standtest_result.json").write_text(json.dumps(out2, indent=2))
        return 3

    # place tiny STANDTEST
    ticker = os.environ.get("STANDTEST_TICKER", "AAPL").upper()
    qty = int(os.environ.get("STANDTEST_QTY", "1"))
    coid = os.environ.get("STANDTEST_COID", f"STANDTEST-20260918-{int(time.time())}")[:32]
    ticket = {
        "ticker": ticker,
        "side": "BUY",
        "shares": qty,
        "date": now_et().strftime("%Y-%m-%d"),
        "order_type": "MARKET",
    }
    # monkeypatch client_order_id via order_body then override
    body = order_body(ticket)
    body["client_order_id"] = coid
    # place using raw body
    try:
        res = api.trade.order_v3.place_order(api.account_id, [body])
        payload = api._json(res, "place_order")
    except Exception as e:
        result = {"ok": False, "stage": "place", "error": str(e)[:400], "client_order_id": coid,
                  "body": {k: body[k] for k in body if k != "client_order_id"},
                  "client_order_id_full": coid, "et": now_et().isoformat(), "cash": snap.cash}
        print(json.dumps(result, indent=2))
        Path("data/paper_open/standtest_result.json").write_text(json.dumps(result, indent=2))
        return 4

    oid = parse_order_id(payload) if isinstance(payload, (dict, list)) else ""
    result = {
        "ok": True,
        "stage": "placed",
        "et": now_et().isoformat(),
        "cash": snap.cash,
        "client_order_id": coid,
        "order_id": oid,
        "ticker": ticker,
        "qty": qty,
        "order_type": body.get("order_type"),
        "session": body.get("support_trading_session"),
        "tif": body.get("time_in_force"),
        "payload_snip": json.dumps(payload)[:800],
    }
    # try list open orders if API has it
    try:
        for meth in ("get_order_detail", "list_orders", "get_order_list"):
            pass
        # common SDK: order_v3.get_order_detail / get_account_orders
        detail = None
        if oid and hasattr(api.trade.order_v3, "get_order_detail"):
            detail = api._json(api.trade.order_v3.get_order_detail(api.account_id, oid), "order_detail")
        elif hasattr(api.trade.order_v3, "get_order"):
            detail = api._json(api.trade.order_v3.get_order(api.account_id, oid), "order")
        if detail is not None:
            result["detail"] = detail
            result["status"] = (
                (detail.get("status") if isinstance(detail, dict) else None)
                or (detail.get("data", {}) or {}).get("status") if isinstance(detail, dict) else None
            )
    except Exception as e:
        result["detail_error"] = str(e)[:240]
    print(json.dumps(result, indent=2))
    Path("data/paper_open/standtest_result.json").write_text(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 4

if __name__ == "__main__":
    raise SystemExit(main())
