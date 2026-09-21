"""Push today's union_hot_n4_h1 (hot4) tickets into Webull *paper*.

Default source is ``hot4``: long-only today's ``union_hot_n4_h1`` buy
list from ``dashboard/factor-mine/today_strategies.json`` (same panel
the factor-mine cash book uses). Flatten live-card tickets stay
available via ``--source flatten``. Combo is a manual escape only.

Official OpenAPI sandbox is the in-app Paper Trading book
(webull.com → Open API → “Using OpenAPI service in Paper Trading”).
App key + secret are auto-approved for sandbox in a few minutes.

    python -m src.webull_exec --date 2026-09-17          # dry-run hot4
    python -m src.webull_exec --date 2026-09-17 --submit  # paper MARKET
    python -m src.webull_exec --source flatten --submit   # flatten escape

REAL is refused unless --env real AND --live AND WEBULL_LIVE=1.
Paper never talks to api.webull.com. Do not enable --env real here.

Rules:
  * hot4: long-only leftover cash, MARKET (live print, not ticket px)
  * skip a name already held; skip if leftover cash cannot buy 1 share
  * hard-red S≤−3 sits; stale Friday panel is dry-run unless --allow-stale
  * flatten source: only live card tickets (never the would-buy wish list)
  * $0 sandbox cash still buys nothing — snapshot reports the skip

Env: WEBULL_APP_KEY, WEBULL_APP_SECRET, WEBULL_ACCOUNT_ID (optional),
     WEBULL_REGION (default us).
"""
from __future__ import annotations

import argparse
import json
import math
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

ROOT = Path(__file__).resolve().parent.parent
HOT4 = "union_hot_n4_h1"
HOT4_STRATEGY_PATHS = (
    ROOT / "dashboard" / "factor-mine" / "today_strategies.json",
    ROOT / "data" / "day_board" / "today_strategies.json",
    ROOT / "data" / "factor_mine" / "strategy_tickets.json",
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
    # Never turn an unknown response schema into a funded/connected $0 account.
    keys = ("available_cash", "availableCash", "cash_balance", "cashBalance",
            "cash", "total_cash", "totalCash", "total_cash_value", "totalCashValue",
            "settled_cash", "settledCash")
    def visit(value):
        if isinstance(value, list):
            for item in value:
                yield from visit(item)
        elif isinstance(value, dict):
            currency = value.get("currency") or value.get("currency_code")
            if currency and str(currency).upper() != "USD":
                return
            if any(value.get(k) not in (None, "") for k in keys):
                yield value
            for key in ("data", "account_currency_assets", "currency_assets", "assets", "balances"):
                if key in value:
                    yield from visit(value[key])
    rows = list(visit(payload))
    if len(rows) != 1:
        raise ValueError("unrecognized or ambiguous USD cash balance response")
    row = rows[0]
    cash = _num(row, *keys, default=float("nan"))
    power = _num(row, "buying_power", "buyingPower", "available_buying_power",
                 "availableBuyingPower", "day_buying_power", "dayBuyingPower", default=cash)
    if not math.isfinite(cash) or not math.isfinite(power):
        raise ValueError("invalid cash/buying power")
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


def _hot4_from_payload(payload) -> dict:
    if not isinstance(payload, dict):
        return {}
    strats = payload.get("strategies")
    if not isinstance(strats, dict):
        strats = payload
    rec = strats.get(HOT4)
    if not isinstance(rec, dict):
        return {}
    out = dict(rec)
    if payload.get("date") and not out.get("date"):
        out["date"] = payload.get("date")
    return out


def load_hot4_published(date: str, payload: dict | None = None) -> dict:
    """Today's union_hot_n4_h1 buy list from the factor-mine cash-book panel."""
    if payload is not None:
        rec = _hot4_from_payload(payload)
        return rec or {}
    for path in HOT4_STRATEGY_PATHS:
        if not path.is_file():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        rec = _hot4_from_payload(raw)
        if rec and str(rec.get("date") or rec.get("clock_legal_for") or "") == date:
            rec["_path"] = str(path)
            return rec
    return {}


def _hot4_num(row: dict, *keys: str):
    for key in keys:
        raw = row.get(key) if isinstance(row, dict) else None
        if raw is None or raw == "":
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def size_hot4_tickets(buys: list, *, cash: float, held: set[str] | None,
                      date: str, s=None, sit: bool = False) -> tuple[list[dict], list[dict]]:
    """Long-only leftover split. No shorts. MARKET sizing uses list px."""
    from src import factor_mine_book as fmb
    from src.combo_broker import quote_px

    held = {str(t).upper() for t in (held or set())}
    leftover = max(float(cash or 0), 0.0)
    hard_red = bool(sit) or (
        s is not None and float(s) <= float(fmb.HARD_RED))
    tickets: list[dict] = []
    skips: list[dict] = []
    names: list[dict] = []
    for raw in buys or []:
        if not isinstance(raw, dict):
            continue
        t = str(raw.get("ticker") or raw.get("symbol") or "").upper().strip()
        if not t:
            continue
        side = str(raw.get("side") or raw.get("kid_side") or "long").lower()
        if side == "short":
            skips.append({
                "date": date, "ticker": t, "kind": "short",
                "reason": "hot4 is long-only — skip short",
            })
            continue
        if hard_red:
            skips.append({
                "date": date, "ticker": t, "kind": "hard_red",
                "reason": (
                    f"hard-red S={s} sit; no new long {HOT4}"
                ),
            })
            continue
        if t in held:
            skips.append({
                "date": date, "ticker": t, "kind": "held",
                "reason": f"already held — skip {HOT4}",
            })
            continue
        names.append(raw)
    if leftover <= 0:
        for raw in names:
            t = str(raw.get("ticker") or "").upper()
            skips.append({
                "date": date, "ticker": t, "kind": "cash",
                "reason": f"leftover cash {leftover:.2f} cannot buy 1 share",
            })
        return tickets, skips
    budgets = fmb.split_budgets(names, leftover, "leftover")
    for raw, per in zip(names, budgets):
        t = str(raw.get("ticker") or "").upper()
        px = _hot4_num(raw, "px", "open", "open_px")
        if px is None or px <= 0:
            px = quote_px(t, date, row=raw.get("row") if isinstance(
                raw.get("row"), dict) else raw)
        if px is None or px <= 0:
            skips.append({
                "date": date, "ticker": t, "kind": "no_price",
                "reason": "no session_export / 09:30 open or prior close",
            })
            continue
        shares = int(per // px)
        if shares < 1:
            skips.append({
                "date": date, "ticker": t, "kind": "cash",
                "reason": f"leftover split {per:.2f} < 1 share @ {px:.2f}",
            })
            continue
        notional = shares * px
        if notional > leftover + 1e-6:
            shares = int(leftover // px)
            if shares < 1:
                skips.append({
                    "date": date, "ticker": t, "kind": "cash",
                    "reason": f"cash {leftover:.2f} < 1 share @ {px:.2f}",
                })
                continue
            notional = shares * px
        leftover -= notional
        tickets.append({
            "side": "BUY",
            "ticker": t,
            "shares": shares,
            "px": round(float(px), 4),
            "notional": round(notional, 2),
            "order_type": "MARKET",
            "status": "plan",
            "sleeve": HOT4,
            "kid_side": "long",
            "date": date,
            "clock": "09:30 ET",
            "reason": f"{HOT4} {raw.get('src') or 'long'} leftover ${per:.2f}",
        })
    return tickets, skips


def plan_hot4_for_broker(date: str, snap: BrokerSnap,
                         payload: dict | None = None,
                         panel: dict | None = None) -> dict:
    """Today's hot4 would-buy list, sized against leftover cash only."""
    from src import factor_mine as fm
    from src import factor_mine_book as fmb
    from src.combo_broker import resolve_rows

    published = load_hot4_published(date, payload)
    buys = list(published.get("buy") or [])
    use_date = str(published.get("date") or date)
    stale = bool(published and published.get("status") not in ("ok", "sit"))
    source = "today_strategies"
    look_err = ""
    if not published:
        looked = resolve_rows(date, panel)
        rec_by = {r["name"]: r for r in fm.build_recipes()}
        rec = rec_by.get(HOT4) or {}
        rows = looked.get("rows") or []
        use_date = str(looked.get("date") or date)
        stale = bool(looked.get("stale"))
        source = f"panel_{looked.get('source') or 'look'}"
        look_err = looked.get("error") or ""
        for r in fm.pick_day(rows, rec) if rec else []:
            t = str(r.get("ticker") or "").upper()
            if t:
                buys.append({
                    "ticker": t,
                    "src": ",".join(r.get("sources") or []),
                    "side": "long",
                    "row": r,
                })
    else:
        pub_date = str(published.get("date")
                       or published.get("clock_legal_for")
                       or published.get("session_open") or "")
        if pub_date and pub_date != date:
            stale = True
            use_date = pub_date
    try:
        s = published.get("s")
        if s is None:
            s = fmb.morning_s(fmb.load_regime(), date)
    except Exception:
        s = published.get("s")
    sit = bool(published.get("sit"))
    # Hot4 spends leftover cash only. Buying power is not a fill.
    cash = max(float(getattr(snap, "cash", 0) or 0), 0.0)
    held = set((snap.positions or {}) if snap else {})
    tickets, skips = size_hot4_tickets(
        buys, cash=cash, held=held, date=use_date, s=s, sit=sit,
    )
    would = []
    for raw in buys:
        t = str((raw or {}).get("ticker") or "").upper()
        if not t:
            continue
        side = str(raw.get("side") or raw.get("kid_side") or "long").lower()
        if side == "short":
            continue
        would.append({
            "ticker": t,
            "sleeve": HOT4,
            "kid_side": "long",
            "clock": "09:30 ET",
            "px": raw.get("px"),
            "src": raw.get("src"),
        })
    hard_red = sit or (
        s is not None and float(s) <= float(fmb.HARD_RED))
    why = (f"{HOT4} long-only leftover cash · MARKET · "
           f"rows via {source}")
    if stale:
        why += (f" · STALE panel {use_date} (wanted {date})"
                " — do not submit unless --allow-stale")
    if hard_red:
        why += f" · hard-red S={s} sit"
    if cash <= 0:
        why += f" · leftover cash ${cash:.2f} cannot buy 1 share"
    return {
        "date": use_date,
        "want_date": date,
        "policy": HOT4,
        "combo": "",
        "source": source,
        "stale": stale,
        "score": s,
        "hard_red": hard_red,
        "why": why,
        "tickets": tickets,
        "skipped": skips,
        "would_buy": {"rows": would},
        "flatten_ok": True,
        "look_error": look_err,
        "order_type": "MARKET",
    }


def order_body(ticket: dict) -> dict:
    """Webull paper order: MARKET at the live print, not LIMIT at ticket px."""
    side = "BUY" if str(ticket.get("side") or "").upper() == "BUY" else "SELL"
    return {
        "combo_type": "NORMAL",
        "client_order_id": client_order_id(
            str(ticket.get("date") or ticket.get("asof") or ""),
            side, ticket.get("ticker") or ""),
        "symbol": str(ticket.get("ticker") or "").upper(),
        "instrument_type": "EQUITY",
        "market": "US",
        "order_type": "MARKET",
        "quantity": str(int(ticket.get("shares") or 0)),
        "support_trading_session": "CORE",
        "side": side,
        "time_in_force": "DAY",
        "entrust_type": "QTY",
    }


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
        try:
            cash, power = parse_balance(bal)
        except ValueError as e:
            return BrokerSnap(env=self.env, cash=0, positions={}, connected=False, error=str(e))
        return BrokerSnap(env=self.env, cash=cash, buying_power=power,
                          positions=parse_positions(pos), connected=True,
                          acc_id=self.account_id)

    def place_batch(self, tickets):
        """Place each ticket as a one-element list — same path as place().

        Sandbox rejects a multi-order place_order with
        invalid combo_type=["NORMAL", ...] (OPENAPI_PARAM_ERR / HTTP 417).
        """
        if self.host != PAPER_HOST or self.trade is None or not self.account_id:
            raise RuntimeError("sandbox account not connected")
        out = {}
        ack = datetime.now().astimezone().isoformat()
        for ticket in tickets:
            body = order_body(ticket)
            coid = str(body["client_order_id"])
            reply = self.place(ticket, self.env)
            row = {
                "ok": bool(reply.get("ok")),
                "order_id": str(reply.get("order_id") or ""),
                "acknowledged_at": reply.get("acknowledged_at") or ack,
            }
            if reply.get("error"):
                row["error"] = reply["error"]
            out[coid] = row
        return out

    def place(self, ticket: dict, env: str) -> dict:
        if self.trade is None or not self.account_id:
            return {"ok": False, "error": self.err or "not connected"}
        body = [order_body(ticket)]
        try:
            res = self.trade.order_v3.place_order(self.account_id, body)
            payload = self._json(res, "place_order")
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e)[:240]}
        # HTTP 200 is not sufficient evidence of broker acceptance.
        def rejected(value):
            if isinstance(value, list):
                return any(rejected(x) for x in value)
            if not isinstance(value, dict):
                return False
            if value.get("success") is False or value.get("error") or value.get("error_code"):
                return True
            if str(value.get("status", "")).upper() in ("REJECTED", "FAILED", "ERROR"):
                return True
            if "code" in value and str(value["code"]).upper() not in ("0", "200", "SUCCESS", "OK"):
                return True
            return any(rejected(value[k]) for k in ("data", "orders") if k in value)
        oid = parse_order_id(payload)
        if rejected(payload) or not oid:
            return {"ok": False, "error": "broker rejection or missing acknowledgment; reconcile before retry",
                    "client_order_id": body[0]["client_order_id"]}
        return {"ok": True, "order_id": oid, "order_type": "MARKET",
                "acknowledged_at": datetime.now().astimezone().isoformat()}


def write_last(doc: dict) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    LAST_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return LAST_JSON


def _norm_source(source: str) -> str:
    src = (source or "hot4").strip().lower()
    if src in ("hot4", "flatten", "combo"):
        return src
    return "hot4"


def _plan(date: str, snap: BrokerSnap, *, source: str, combo: str) -> dict:
    if source == "flatten":
        card = plan_for_broker(date, snap)
        card.setdefault("policy", "flatten_hard_red")
        card.setdefault("source", "flatten")
        card.setdefault("stale", False)
        card.setdefault("combo", "")
        return card
    if source == "combo":
        return plan_combo_for_broker(date, snap, combo=combo)
    return plan_hot4_for_broker(date, snap)


def run(date: str | None, *, env: str = "paper", submit: bool = False,
        live: bool = False, write: bool = True, source: str = "hot4",
        combo: str = PAPER_COMBO, allow_stale: bool = False) -> int:
    requested_submit = submit
    env = "real" if env == "real" else "paper"
    source = _norm_source(source)
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
            "source": source,
            "combo": combo if source == "combo" else "",
            "stale": bool(card.get("stale")),
            "policy": card.get("policy") or (HOT4 if source == "hot4" else source),
            "skipped": card.get("skipped") or [],
            "why": card.get("why") or "",
            "sent": [],
            "generated": datetime.now().isoformat(timespec="seconds"),
        }
        if write:
            write_last(last)
            if TODAY_JSON.is_file():
                inject_today_from_disk()
        return 2 if requested_submit else 0

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
    last["combo"] = combo if source == "combo" else ""
    last["stale"] = bool(card.get("stale"))
    last["policy"] = card.get("policy") or (HOT4 if source == "hot4" else source)
    last["why"] = card.get("why") or ""
    last["skipped"] = card.get("skipped") or []
    last["score"] = card.get("score")
    last["hard_red"] = bool(card.get("hard_red"))
    last["order_type"] = "MARKET"
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
    failed = (not submit or card.get("stale") or card.get("look_error") or
              any(x.get("status") == "error" for x in last["sent"]) or
              (source == "hot4" and not card.get("hard_red") and
               any(x.get("kind") in ("cash", "no_price") for x in card.get("skipped", []))))
    return 2 if requested_submit and failed else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--env", choices=("paper", "real"), default="paper")
    ap.add_argument("--submit", action="store_true",
                    help="place paper orders (default is dry-run)")
    ap.add_argument("--live", action="store_true",
                    help="required together with --env real and WEBULL_LIVE=1")
    ap.add_argument("--write", action="store_true", default=True)
    ap.add_argument("--source", choices=("hot4", "flatten", "combo"),
                    default="hot4",
                    help="union_hot_n4_h1 long-only (default), flatten escape, or combo")
    ap.add_argument("--combo", default=PAPER_COMBO,
                    help="combo name when --source combo (manual escape)")
    ap.add_argument("--allow-stale", action="store_true",
                    help="submit even if the look is last-closed, not today")
    args = ap.parse_args(argv)
    return run(args.date or None, env=args.env, submit=args.submit,
               live=args.live, write=args.write, source=args.source,
               combo=args.combo, allow_stale=args.allow_stale)


if __name__ == "__main__":
    raise SystemExit(main())
