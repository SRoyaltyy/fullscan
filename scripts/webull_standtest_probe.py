"""Standalone Webull *paper* cash/place probe. No src.* imports (avoids pandas)."""
from __future__ import annotations
import json, os, sys, time, uuid
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

def connect():
    from webull.webullsdkcore.client import ApiClient
    from webull.core.http.initializer.client_builder import ClientBuilder
    key = _env("WEBULL_APP_KEY")
    secret = _env("WEBULL_APP_SECRET")
    if not key or not secret:
        raise RuntimeError("missing WEBULL_APP_KEY / WEBULL_APP_SECRET")
    # Match PaperAPI.connect patterns used in repo historically
    try:
        from webull.webullsdktrade.trade.trade_client import TradeClient
    except Exception:
        TradeClient = None
    # Prefer official builder used by fullscan PaperAPI
    try:
        # mirror src.webull_exec PaperAPI.connect
        from webull.webullsdkcore.client import ApiClient as AC
        client = AC()
        client.set_endpoint(PAPER_HOST.replace("https://", "").replace("http://", ""))
        # fallbacks below
    except Exception:
        client = None

    # Use the same path as PaperAPI in webull_exec if available via dynamic copy
    # Minimal: TradeClient with app key/secret + paper endpoint
    from webull.webullsdktrade.trade.trade_client import TradeClient as TC
    trade = TC(app_key=key, app_secret=secret, region_id=_env("WEBULL_REGION", "us") or "us", host=PAPER_HOST)
    return trade

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
        if not rows and payload.get("account_id"):
            return str(payload.get("account_id"))
    if preferred:
        for row in rows:
            if not isinstance(row, dict):
                continue
            aid = str(row.get("account_id") or row.get("accountId") or "")
            if aid == preferred:
                return aid
    for row in rows:
        if not isinstance(row, dict):
            continue
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
        raise ValueError("unrecognized or ambiguous USD cash balance response")
    cash = found[0]
    if not math.isfinite(cash):
        raise ValueError("invalid cash")
    return cash, cash

def _json(res, label):
    if hasattr(res, "status_code"):
        code = res.status_code
        text = getattr(res, "text", "") or ""
        if code >= 400:
            raise RuntimeError(f"{label} HTTP {code}: {text[:240]}")
    if hasattr(res, "json"):
        try:
            return res.json()
        except Exception:
            return {}
    return res if isinstance(res, (dict, list)) else {}

def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else "cash").strip().lower()
    out_dir = Path("data/paper_open")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Prefer importing PaperAPI only if deps allow; else standalone TradeClient
    trade = None
    err = None
    try:
        sys.path.insert(0, ".")
        # Try lightweight: exec only the PaperAPI class by loading file? Too heavy.
        from webull.webullsdktrade.trade.trade_client import TradeClient
        key = _env("WEBULL_APP_KEY"); secret = _env("WEBULL_APP_SECRET")
        if not key or not secret:
            raise RuntimeError("missing WEBULL_APP_KEY / WEBULL_APP_SECRET (secret empty in Actions?)")
        # Host kw may vary by SDK version — try several
        last_e = None
        for kwargs in (
            dict(app_key=key, app_secret=secret, host=PAPER_HOST),
            dict(app_key=key, app_secret=secret, endpoint=PAPER_HOST),
            dict(app_key=key, app_secret=secret),
        ):
            try:
                trade = TradeClient(**kwargs)
                break
            except TypeError as e:
                last_e = e
                continue
        if trade is None:
            raise RuntimeError(f"TradeClient init failed: {last_e}")
        # Force paper host if attribute exists
        for attr in ("host", "_host", "endpoint"):
            if hasattr(trade, attr):
                try:
                    setattr(trade, attr, PAPER_HOST)
                except Exception:
                    pass
    except Exception as e:
        err = str(e)[:400]

    if trade is None:
        doc = {"ok": False, "stage": "connect", "error": err, "et": now_et().isoformat()}
        print(json.dumps(doc, indent=2))
        (out_dir / "standtest_cash.json").write_text(json.dumps(doc, indent=2))
        return 2

    try:
        accounts = _json(trade.account_v2.get_account_list(), "account_list")
        aid = parse_account_id(accounts, _env("WEBULL_ACCOUNT_ID"))
        if not aid:
            raise RuntimeError(f"no account_id; list snip={json.dumps(accounts)[:300]}")
        bal = _json(trade.account_v2.get_account_balance(aid), "balance")
        cash, bp = parse_balance(bal)
        try:
            pos = _json(trade.account_v2.get_account_position(aid), "positions")
        except Exception:
            pos = []
        npos = 0
        held = []
        if isinstance(pos, list):
            npos = len(pos)
        elif isinstance(pos, dict):
            for k in ("data", "positions", "list"):
                if isinstance(pos.get(k), list):
                    npos = len(pos[k]); break
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

    if cash <= 0:
        doc2 = {**doc, "stage": "submit_blocked", "reason": "cash<=0"}
        print(json.dumps(doc2, indent=2))
        (out_dir / "standtest_result.json").write_text(json.dumps(doc2, indent=2))
        return 3

    ticker = _env("STANDTEST_TICKER", "AAPL").upper() or "AAPL"
    qty = int(_env("STANDTEST_QTY", "1") or "1")
    coid = (_env("STANDTEST_COID") or f"STANDTEST-20260918-{int(time.time())}")[:32]
    body = {
        "client_order_id": coid,
        "symbol": ticker,
        "side": "BUY",
        "order_type": "MARKET",
        "time_in_force": "DAY",
        "support_trading_session": "CORE",
        "quantity": str(qty),
        "entrust_type": "QTY",
    }
    # Also try qty as number variants if SDK expects
    try:
        res = trade.order_v3.place_order(aid, [body])
        payload = _json(res, "place_order")
    except Exception as e:
        result = {"ok": False, "stage": "place", "error": str(e)[:400], "client_order_id": coid,
                  "et": now_et().isoformat(), "cash": cash, "body_keys": list(body)}
        print(json.dumps(result, indent=2))
        (out_dir / "standtest_result.json").write_text(json.dumps(result, indent=2))
        return 4

    # extract order id
    def find_oid(v):
        if isinstance(v, dict):
            for k in ("order_id", "orderId"):
                if v.get(k):
                    return str(v[k])
            for k in ("data", "orders", "result"):
                if k in v:
                    got = find_oid(v[k])
                    if got: return got
        if isinstance(v, list):
            for x in v:
                got = find_oid(x)
                if got: return got
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
    # status query if possible
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
