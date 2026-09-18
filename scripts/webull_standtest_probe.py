"""Standalone Webull paper cash/place probe. Mirrors PaperAPI.connect; no pandas."""
from __future__ import annotations
import json, os, sys, time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
PAPER_HOST = "api.sandbox.webull.com"

def now_et():
    return datetime.now(ET)

def _env(name: str, default: str = "") -> str:
    raw = (os.environ.get(name) or default).strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("\"", "'"):
        raw = raw[1:-1].strip()
    return raw

def _json(res, label: str):
    code = getattr(res, "status_code", None)
    if code not in (None, 200):
        text = getattr(res, "text", "") or str(res)
        raise RuntimeError(f"{label} HTTP {code}: {text[:240]}")
    if hasattr(res, "json"):
        try:
            return res.json()
        except Exception:
            return {}
    return res if isinstance(res, (dict, list)) else {}

def parse_account_id(payload, preferred=""):
    preferred = (preferred or "").strip()
    rows = []
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        for k in ("data", "accounts", "list", "items", "result"):
            v = payload.get(k)
            if isinstance(v, list):
                rows = v
                break
        if not rows:
            aid = str(payload.get("account_id") or payload.get("accountId") or "")
            if aid:
                return aid
    if preferred:
        for row in rows:
            if isinstance(row, dict):
                aid = str(row.get("account_id") or row.get("accountId") or "")
                if aid == preferred:
                    return aid
    for row in rows:
        if isinstance(row, dict):
            aid = str(row.get("account_id") or row.get("accountId") or "")
            if aid:
                return aid
    return preferred

def parse_balance(payload):
    import math
    keys = ("available_cash", "availableCash", "cash_balance", "cashBalance",
            "cash", "total_cash", "totalCash", "total_cash_value", "totalCashValue",
            "settled_cash", "settledCash")
    found = []
    def visit(value):
        if isinstance(value, dict):
            for k in keys:
                if k in value and value[k] is not None:
                    try:
                        found.append(float(value[k]))
                    except Exception:
                        pass
            for k in ("data", "account_currency_assets", "currency_assets", "assets", "balances"):
                if k in value:
                    visit(value[k])
        elif isinstance(value, list):
            for x in value:
                visit(x)
    visit(payload)
    if not found:
        raise ValueError("unrecognized USD cash balance response: " + json.dumps(payload)[:300])
    cash = found[0]
    if not math.isfinite(cash):
        raise ValueError("invalid cash")
    return cash, cash

def connect_trade():
    key = _env("WEBULL_APP_KEY")
    secret = _env("WEBULL_APP_SECRET")
    if not key or not secret:
        raise RuntimeError("WEBULL_APP_KEY / WEBULL_APP_SECRET missing in Actions secrets")
    from webull.core.client import ApiClient
    from webull.trade.trade_client import TradeClient
    region = _env("WEBULL_REGION", "us") or "us"
    client = ApiClient(key, secret, region)
    client.add_endpoint(region, PAPER_HOST)
    return TradeClient(client)

def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else "cash").strip().lower()
    out_dir = Path("data/paper_open")
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        trade = connect_trade()
    except Exception as e:
        doc = {"ok": False, "stage": "connect", "error": str(e)[:400], "et": now_et().isoformat()}
        print(json.dumps(doc, indent=2))
        (out_dir / "standtest_cash.json").write_text(json.dumps(doc, indent=2))
        return 2

    try:
        accounts = _json(trade.account_v2.get_account_list(), "account_list")
        aid = parse_account_id(accounts, _env("WEBULL_ACCOUNT_ID"))
        if not aid:
            raise RuntimeError("no account_id in list: " + json.dumps(accounts)[:300])
        bal = _json(trade.account_v2.get_account_balance(aid), "balance")
        cash, bp = parse_balance(bal)
        try:
            pos = _json(trade.account_v2.get_account_position(aid), "positions")
            npos = len(pos) if isinstance(pos, list) else len((pos or {}).get("data") or []) if isinstance(pos, dict) else 0
        except Exception:
            npos = -1
        doc = {
            "ok": True,
            "stage": "snapshot",
            "et": now_et().isoformat(),
            "cash": cash,
            "buying_power": bp,
            "account_id_suffix": aid[-6:],
            "n_positions": npos,
            "host": PAPER_HOST,
            "balance_snip": json.dumps(bal)[:500],
        }
        print(json.dumps(doc, indent=2))
        (out_dir / "standtest_cash.json").write_text(json.dumps(doc, indent=2))
    except Exception as e:
        doc = {"ok": False, "stage": "snapshot", "error": str(e)[:400], "et": now_et().isoformat()}
        print(json.dumps(doc, indent=2))
        (out_dir / "standtest_cash.json").write_text(json.dumps(doc, indent=2))
        return 2

    if mode == "cash":
        return 0

    if mode == "status":
        want = _env("STANDTEST_ORDER_ID") or _env("STANDTEST_COID")
        status_doc = {
            "ok": True,
            "stage": "status",
            "et": now_et().isoformat(),
            "cash": cash,
            "buying_power": bp,
            "want": want,
            "open_orders": None,
            "matched": None,
        }
        open_payload = None
        # try common SDK list methods
        for call in (
            lambda: trade.order_v3.get_order_history_and_open_orders(aid),
            lambda: trade.order_v3.get_open_orders(aid),
            lambda: trade.order_v2.get_open_orders(aid) if hasattr(trade, "order_v2") else (_ for _ in ()).throw(AttributeError("no v2")),
            lambda: trade.order_v3.list_orders(aid),
        ):
            try:
                open_payload = _json(call(), "open_orders")
                break
            except Exception as e:
                status_doc.setdefault("list_errors", []).append(str(e)[:160])
        status_doc["open_snip"] = json.dumps(open_payload)[:1200] if open_payload is not None else None
        # flatten rows
        rows = []
        def walk(v):
            if isinstance(v, list):
                for x in v: walk(x)
            elif isinstance(v, dict):
                if v.get("order_id") or v.get("orderId") or v.get("client_order_id"):
                    rows.append(v)
                for k in ("data", "orders", "list", "items", "result"):
                    if k in v: walk(v[k])
        walk(open_payload)
        status_doc["n_open_rows"] = len(rows)
        if want:
            for r in rows:
                oid = str(r.get("order_id") or r.get("orderId") or "")
                coid = str(r.get("client_order_id") or r.get("clientOrderId") or "")
                if want in (oid, coid) or want in coid or want in oid:
                    status_doc["matched"] = {
                        "order_id": oid,
                        "client_order_id": coid,
                        "status": r.get("status") or r.get("order_status"),
                        "symbol": r.get("symbol"),
                        "side": r.get("side"),
                        "order_type": r.get("order_type") or r.get("orderType"),
                        "tif": r.get("time_in_force") or r.get("timeInForce"),
                        "session": r.get("support_trading_session") or r.get("tradingSession"),
                        "row_snip": json.dumps(r)[:500],
                    }
                    break
        print(json.dumps(status_doc, indent=2))
        (out_dir / "standtest_status.json").write_text(json.dumps(status_doc, indent=2))
        return 0

    if cash <= 0:
        doc2 = {**doc, "stage": "submit_blocked", "reason": "cash<=0"}
        print(json.dumps(doc2, indent=2))
        (out_dir / "standtest_result.json").write_text(json.dumps(doc2, indent=2))
        return 3

    ticker = (_env("STANDTEST_TICKER", "AAPL") or "AAPL").upper()
    qty = int(_env("STANDTEST_QTY", "1") or "1")
    coid = (_env("STANDTEST_COID") or f"STANDTEST-20260918-{int(time.time())}")[:32]
    body = {
        "combo_type": "NORMAL",
        "client_order_id": coid,
        "symbol": ticker,
        "instrument_type": "EQUITY",
        "market": "US",
        "order_type": "MARKET",
        "quantity": str(qty),
        "support_trading_session": "CORE",
        "side": "BUY",
        "time_in_force": "DAY",
        "entrust_type": "QTY",
    }
    try:
        res = trade.order_v3.place_order(aid, [body])
        payload = _json(res, "place_order")
    except Exception as e:
        result = {"ok": False, "stage": "place", "error": str(e)[:400], "client_order_id": coid,
                  "et": now_et().isoformat(), "cash": cash}
        print(json.dumps(result, indent=2))
        (out_dir / "standtest_result.json").write_text(json.dumps(result, indent=2))
        return 4

    def find_oid(v):
        if isinstance(v, dict):
            for k in ("order_id", "orderId"):
                if v.get(k):
                    return str(v[k])
            for k in ("data", "orders", "result"):
                if k in v:
                    got = find_oid(v[k])
                    if got:
                        return got
        if isinstance(v, list):
            for x in v:
                got = find_oid(x)
                if got:
                    return got
        return ""

    oid = find_oid(payload)
    result = {
        "ok": bool(oid),
        "stage": "placed",
        "et": now_et().isoformat(),
        "cash": cash,
        "client_order_id": coid,
        "order_id": oid,
        "ticker": ticker,
        "qty": qty,
        "order_type": "MARKET",
        "session": "CORE",
        "tif": "DAY",
        "payload_snip": json.dumps(payload)[:800],
    }
    try:
        if oid and hasattr(trade.order_v3, "get_order_detail"):
            detail = _json(trade.order_v3.get_order_detail(aid, oid), "order_detail")
            result["detail_snip"] = json.dumps(detail)[:600]
            if isinstance(detail, dict):
                result["status"] = detail.get("status") or (detail.get("data") or {}).get("status")
    except Exception as e:
        result["detail_error"] = str(e)[:200]

    print(json.dumps(result, indent=2))
    (out_dir / "standtest_result.json").write_text(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 4

if __name__ == "__main__":
    raise SystemExit(main())
