"""Push today's combo_sh 50/50 + MACD-short tickets into Webull *paper*.

Default source is ``combo_sh_macd_5050_shared`` (short news🔴 ∩ MACD-up
+ union_hot_n4, shared leftover). Flatten live-card tickets stay
available via ``--source flatten``.

Official OpenAPI sandbox is the in-app Paper Trading book
(webull.com → Open API → “Using OpenAPI service in Paper Trading”).
App key + secret are auto-approved for sandbox in a few minutes.

    python -m src.webull_exec --date 2026-09-14          # dry-run combo
    python -m src.webull_exec --date 2026-09-14 --submit  # paper only
    python -m src.webull_exec --source flatten --submit   # old flatten path

REAL is refused unless --env real AND --live AND WEBULL_LIVE=1.
Paper never talks to api.webull.com.

Rules:
  * combo: size against the account's real cash + positions
  * skip a name already held; skip if leftover cash cannot size a share
  * hard-red S≤−3 sits; stale Friday panel is dry-run unless --allow-stale
  * flatten source: only live card tickets (never the would-buy wish list)

Env: WEBULL_APP_KEY, WEBULL_APP_SECRET, WEBULL_ACCOUNT_ID (optional),
     WEBULL_REGION (default us).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path

from src.combo_broker import PAPER_COMBO, plan_combo_for_broker
from src.futubull_exec import (
    BrokerSnap,
    plan_for_broker,
    send_card,
    tickets_to_send,
)
from src.sleeve_merge import OUT_DIR
from src.sleeve_merge_live import (
    TODAY_JSON,
    inject_today_from_disk,
)

LAST_JSON = OUT_DIR / "webull_last.json"
PAPER_HOST = "api.sandbox.webull.com"
LIVE_HOST = "api.webull.com"


def _env(name: str, default: str = "") -> str:
    raw = (os.environ.get(name) or default).strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("\"", "'"):
        raw = raw[1:-1].strip()
    return raw


def refuse_real(env: str, submit: bool, live_flag: bool) -> str | None:
    if env != "real":
        return None
    if not submit:
        return None
    if not live_flag:
        return "REAL submit refused — pass --live"
    if _env("WEBULL_LIVE") != "1":
        return "REAL submit refused — set WEBULL_LIVE=1"
    return None


def paper_host(env: str) -> str:
    """Paper always uses the sandbox host. Live host is gated."""
    if env == "real":
        return LIVE_HOST
    return PAPER_HOST


def client_order_id(date: str, side: str, ticker: str) -> str:
    """Stable ≤32-char id so a re-fire of the same morning does not double."""
    day = re.sub(r"[^0-9]", "", str(date or ""))[:8]
    sig = "B" if str(side).upper() == "BUY" else "S"
    name = re.sub(r"[^A-Z0-9]", "", str(ticker or "").upper())[:16]
    oid = f"fs{day}{sig}{name}"
    return (oid or uuid.uuid4().hex)[:32]


def _as_list(payload) -> list:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("data", "accounts", "positions", "list", "items", "result"):
        val = payload.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            nested = _as_list(val)
            if nested:
                return nested
    return []


def _num(row: dict, *keys: str, default: float = 0.0) -> float:
    for key in keys:
        raw = row.get(key)
        if raw is None or raw == "":
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return default


def parse_account_id(payload, preferred: str = "") -> str:
    want = (preferred or "").strip()
    rows = _as_list(payload)
    if want:
        for row in rows:
            if not isinstance(row, dict):
                continue
            aid = str(row.get("account_id") or row.get("accountId") or "")
            if aid == want:
                return aid
        return want
    for row in rows:
        if not isinstance(row, dict):
            continue
        aid = str(row.get("account_id") or row.get("accountId") or "")
        if aid:
            return aid
    if isinstance(payload, dict):
        return str(payload.get("account_id") or payload.get("accountId") or "")
    return ""


def parse_balance(payload) -> tuple[float, float]:
    row = payload if isinstance(payload, dict) else {}
    if "data" in row and isinstance(row["data"], dict):
        row = row["data"]
    cash = _num(row, "available_cash", "cash_balance", "cash",
                "total_cash", "settled_cash")
    power = _num(row, "buying_power", "available_buying_power",
                 "day_buying_power", default=cash)
    return cash, power


def parse_positions(payload) -> dict:
    out = {}
    for row in _as_list(payload):
        if not isinstance(row, dict):
            continue
        inner = row.get("position") if isinstance(row.get("position"), dict) else row
        t = str(inner.get("symbol") or inner.get("ticker_id")
                or inner.get("ticker") or "").upper().strip()
        if "." in t and t.split(".", 1)[0] in ("US", "NYSE", "NASDAQ"):
            t = t.split(".", 1)[-1]
        sh = int(_num(inner, "available_quantity", "quantity", "qty",
                      "position", "shares"))
        if not t or sh < 1:
            continue
        px = _num(inner, "cost_price", "average_price", "avg_price")
        last = _num(inner, "last_price", "market_price", "current_price",
                    default=px)
        mv = _num(inner, "market_value", "market_val", default=sh * last)
        out[t] = {"shares": sh, "cost_px": px, "last_px": last, "mv": mv}
    return out


def parse_order_id(payload) -> str:
    if isinstance(payload, dict):
        for key in ("order_id", "orderId", "client_order_id"):
            if payload.get(key):
                return str(payload[key])
        data = payload.get("data")
        if isinstance(data, dict):
            return parse_order_id(data)
        if isinstance(data, list) and data:
            return parse_order_id(data[0])
    if isinstance(payload, list) and payload:
        return parse_order_id(payload[0])
    return ""


class PaperAPI:
    """Thin official-SDK wrapper. Missing package / keys → connected=False."""

    def __init__(self, env: str = "paper"):
        self.env = "real" if env == "real" else "paper"
        self.host = paper_host(self.env)
        self.trade = None
        self.account_id = _env("WEBULL_ACCOUNT_ID")
        self.err: str | None = None

    def connect(self) -> bool:
        key = _env("WEBULL_APP_KEY")
        secret = _env("WEBULL_APP_SECRET")
        if not key or not secret:
            self.err = ("WEBULL_APP_KEY / WEBULL_APP_SECRET missing — "
                        "create a Paper Trading API app at "
                        "webull.com → Open API")
            return False
        try:
            from webull.core.client import ApiClient
            from webull.trade.trade_client import TradeClient
        except ImportError:
            self.err = ("webull-openapi-python-sdk not installed "
                        "(pip install webull-openapi-python-sdk)")
            return False
        try:
            region = _env("WEBULL_REGION", "us") or "us"
            client = ApiClient(key, secret, region)
            client.add_endpoint(region, self.host)
            self.trade = TradeClient(client)
        except Exception as e:  # noqa: BLE001 — keys / host miss is a soft fail
            msg = str(e)
            if "UNAUTHORIZED" in msg or "Invalid credentials" in msg:
                self.err = (
                    "sandbox 401 — keys were sent to api.sandbox.webull.com "
                    "and rejected. Regenerate under Open API → Using OpenAPI "
                    "service in Paper Trading (not the live Trading API). "
                    "Paste App Key / App Secret into GitHub secrets with no "
                    "quotes."
                )
            else:
                self.err = f"Webull client init failed: {e}"
            return False
        return True

    def _json(self, res, label: str):
        code = getattr(res, "status_code", None)
        if code not in (None, 200):
            text = getattr(res, "text", "") or str(res)
            raise RuntimeError(f"{label} HTTP {code}: {text[:240]}")
        if hasattr(res, "json"):
            try:
                return res.json()
            except Exception:
                return {}
        return res

    def snapshot(self) -> BrokerSnap:
        if self.trade is None:
            return BrokerSnap(env=self.env, cash=0, positions={},
                              connected=False,
                              error=self.err or "not connected")
        try:
            accounts = self._json(self.trade.account_v2.get_account_list(),
                                  "account_list")
            self.account_id = parse_account_id(accounts, self.account_id)
            if not self.account_id:
                return BrokerSnap(env=self.env, cash=0, positions={},
                                  connected=False,
                                  error="no Webull account_id in list")
            bal = self._json(
                self.trade.account_v2.get_account_balance(self.account_id),
                "balance")
            pos = self._json(
                self.trade.account_v2.get_account_position(self.account_id),
                "positions")
        except Exception as e:  # noqa: BLE001
            return BrokerSnap(env=self.env, cash=0, positions={},
                              connected=False, error=str(e)[:240])
        cash, power = parse_balance(bal)
        return BrokerSnap(env=self.env, cash=cash, buying_power=power,
                          positions=parse_positions(pos), connected=True,
                          acc_id=self.account_id)

    def place(self, ticket: dict, env: str) -> dict:
        if self.trade is None or not self.account_id:
            return {"ok": False, "error": self.err or "not connected"}
        side = "BUY" if ticket["side"] == "BUY" else "SELL"
        body = [{
            "combo_type": "NORMAL",
            "client_order_id": client_order_id(
                str(ticket.get("date") or ticket.get("asof") or ""),
                side, ticket["ticker"]),
            "symbol": str(ticket["ticker"]).upper(),
            "instrument_type": "EQUITY",
            "market": "US",
            "order_type": "LIMIT",
            "limit_price": str(ticket.get("px") or 0),
            "quantity": str(int(ticket["shares"])),
            "support_trading_session": "CORE",
            "side": side,
            "time_in_force": "DAY",
            "entrust_type": "QTY",
        }]
        try:
            res = self.trade.order_v3.place_order(self.account_id, body)
            payload = self._json(res, "place_order")
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e)[:240]}
        oid = parse_order_id(payload) or body[0]["client_order_id"]
        return {"ok": True, "order_id": oid}


def write_last(doc: dict) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    LAST_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return LAST_JSON


def _plan(date: str, snap: BrokerSnap, *, source: str, combo: str) -> dict:
    if source == "flatten":
        card = plan_for_broker(date, snap)
        card.setdefault("policy", "flatten_hard_red")
        card.setdefault("source", "flatten")
        card.setdefault("stale", False)
        card.setdefault("combo", "")
        return card
    return plan_combo_for_broker(date, snap, combo=combo)


def run(date: str | None, *, env: str = "paper", submit: bool = False,
        live: bool = False, write: bool = True, source: str = "combo",
        combo: str = PAPER_COMBO, allow_stale: bool = False) -> int:
    env = "real" if env == "real" else "paper"
    source = "flatten" if source == "flatten" else "combo"
    combo = combo or PAPER_COMBO
    blocked = refuse_real(env, submit, live)
    if blocked:
        print(f"[webull] {blocked}")
        write_last({"error": blocked, "env": env, "submit": submit,
                    "source": source, "combo": combo,
                    "generated": datetime.now().isoformat(timespec="seconds")})
        return 2

    from src.sleeve_merge_live import et_today
    date = date or et_today()
    api = PaperAPI(env)
    if api.connect():
        snap = api.snapshot()
    else:
        snap = BrokerSnap(env=env, cash=0, positions={},
                          connected=False, error=api.err)

    if not snap.connected:
        print(f"[webull] not connected — {snap.error}")
        print("[webull] Paper Trading API key lives in GitHub secrets "
              "WEBULL_APP_KEY / WEBULL_APP_SECRET. This job will not "
              "log into the Webull app for you.")
        preview = BrokerSnap(env=env, cash=10_000, positions={},
                             connected=False, error=snap.error)
        try:
            card = _plan(date, preview, source=source, combo=combo)
            n_tickets = len(tickets_to_send(card))
        except Exception:
            n_tickets = 0
            card = {}
        last = {
            "date": date, "env": env, "submit": False,
            "connected": False, "error": snap.error, "host": api.host,
            "n_tickets": n_tickets,
            "source": source, "combo": combo if source == "combo" else "",
            "stale": bool(card.get("stale")),
            "policy": card.get("policy") or source,
            "sent": [],
            "generated": datetime.now().isoformat(timespec="seconds"),
        }
        if write:
            write_last(last)
            if TODAY_JSON.is_file():
                inject_today_from_disk()
        return 0

    card = _plan(date, snap, source=source, combo=combo)
    for t in card.get("tickets") or []:
        t.setdefault("date", date)
    if submit and card.get("stale") and not allow_stale:
        print("[webull] stale look — dry-run only (pass --allow-stale "
              "to send Friday's list as today's tickets)")
        submit = False
    last = send_card(card, snap, submit=submit, opend=api, env=env)
    last["host"] = api.host
    last["account_id"] = snap.acc_id
    last["source"] = source
    last["combo"] = combo if source == "combo" else card.get("policy") or ""
    last["stale"] = bool(card.get("stale"))
    last["policy"] = card.get("policy") or source
    last["why"] = card.get("why") or ""
    print(f"[webull] {env} {api.host} {source} {last.get('combo') or ''} "
          f"cash=${snap.cash:,.2f} pos={len(snap.positions)} "
          f"tickets={last['n_tickets']} submit={submit} "
          f"stale={last['stale']}")
    for s in last["sent"]:
        print(f"  {s.get('status')} {s['side']} {s['ticker']} "
              f"n={s['shares']} @ {s.get('px')} {s.get('error') or ''}")
    if write:
        from src.sleeve_merge_live import write_card
        if source == "flatten":
            write_card(card)
        write_last(last)
        inject_today_from_disk()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--env", choices=("paper", "real"), default="paper")
    ap.add_argument("--submit", action="store_true",
                    help="place paper orders (default is dry-run)")
    ap.add_argument("--live", action="store_true",
                    help="required together with --env real and WEBULL_LIVE=1")
    ap.add_argument("--write", action="store_true", default=True)
    ap.add_argument("--source", choices=("combo", "flatten"), default="combo",
                    help="combo_sh_macd_5050 (default) or flatten live card")
    ap.add_argument("--combo", default=PAPER_COMBO,
                    help="combo name when --source combo")
    ap.add_argument("--allow-stale", action="store_true",
                    help="submit even if the look is last-closed, not today")
    args = ap.parse_args(argv)
    return run(args.date or None, env=args.env, submit=args.submit,
               live=args.live, write=args.write, source=args.source,
               combo=args.combo, allow_stale=args.allow_stale)


if __name__ == "__main__":
    raise SystemExit(main())
