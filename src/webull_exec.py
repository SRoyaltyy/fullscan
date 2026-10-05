"""Push today's sealed h1 plan into Webull *paper*.

Default source is the sealed IRONCLAD h1 forward plan for that session
(``research/hot_n4_clean_v4/forward_h1/h1_log.jsonl``, ``kind=plan``).
The strategy name stays ``union_hot_n4_h1``. Buys are the sealed
new-buy set (picks the open fill would buy — names not already held),
at that set's share counts. Sells are ``planned_sells`` at the plan's
share counts. A carry name sized as a buy fails closed. Factor Mine
``pick_day`` / ``today_strategies.json`` is not a fallback: a missing
plan, a carry-name buy, or paper cash that cannot fund the sealed buy
notionals fails closed. Flatten live-card tickets stay available via
``--source flatten``. Combo is a manual escape only.

Official OpenAPI sandbox is the in-app Paper Trading book
(webull.com → Open API → “Using OpenAPI service in Paper Trading”).
App key + secret are auto-approved for sandbox in a few minutes.

    python -m src.webull_exec --date 2026-10-02          # dry-run sealed h1
    python -m src.webull_exec --date 2026-10-02 --submit  # paper MARKET
    python -m src.webull_exec --source flatten --submit   # flatten escape

REAL is refused unless --env real AND --live AND WEBULL_LIVE=1.
Paper never talks to api.webull.com. Do not enable --env real here.

Rules:
  * sealed h1: MARKET at the live print, not a limit at the plan px
  * buy tickers are the sealed new-buy set, not every name on the plan card
  * sell tickers and share counts are the plan's planned sells
  * a buy for a name the sealed book already holds fails closed
  * paper cash below the sealed buy notional fails closed (no HOT4 rebuild,
    no silent resize of the sealed share count)
  * sells first, at the plan's share count, even when the paper book
    does not already hold the name
  * a missing sealed plan fails closed — no pick_day rebuild
  * flatten source: only live card tickets (never the would-buy wish list)
  * REAL stays refused unless --env real AND --live AND WEBULL_LIVE=1

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
    """Today's union_hot_n4_h1 buy and sell lists from the cash-book panel."""
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


# MARKET buys fill at the live print. A rigid equal split of one cash
# snapshot can leave the last leg SUBMITTED when earlier names fill above
# the plan px (2026-09-21 DELL/GME/UMC filled rich; VSTS
# QVH6EIJB1RF3DUJJAP6LFQDTIA stayed SUBMITTED on ~$247k). Standing orders
# ack before the open, so sandbox cash has not moved yet between legs.
# Size the batch against this fraction of that snapshot. 3% covers the
# <1% per-name open overshoot from that session with room for a hotter print.
HOT4_CASH_HAIRCUT = 0.97


def clamp_buy_shares(planned: int, px: float, spendable: float) -> int:
    """Whole shares the next BUY can send. Never above the plan. 0 = skip."""
    try:
        planned_n = int(planned or 0)
        px_n = float(px)
        cash_n = float(spendable)
    except (TypeError, ValueError):
        return 0
    if planned_n < 1 or px_n <= 0 or cash_n <= 0 or not math.isfinite(px_n):
        return 0
    if not math.isfinite(cash_n):
        return 0
    fitted = int(math.floor((cash_n + 1e-6) / px_n))
    if fitted < 1:
        return 0
    return min(planned_n, fitted)


def cash_still_free(start_cash: float | None, fresh_cash: float | None,
                    reserved_notional: float) -> float | None:
    """Cash still free for the next BUY.

    None means there is no balance reading; the caller keeps the planned
    shares. A pre-open ack does not reduce sandbox cash, so notionals
    already acked in this batch stay reserved until the snapshot drops by
    at least that much. Once the drop covers the reserve, trust the
    snapshot and do not subtract the reserve a second time.
    """
    if fresh_cash is None and start_cash is None:
        return None
    try:
        fresh = float(fresh_cash if fresh_cash is not None else start_cash)
        reserve = float(reserved_notional or 0)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(fresh) or not math.isfinite(reserve):
        return None
    fresh = max(fresh, 0.0)
    reserve = max(reserve, 0.0)
    if start_cash is None:
        return max(0.0, fresh - reserve)
    try:
        start = float(start_cash)
    except (TypeError, ValueError):
        return max(0.0, fresh - reserve)
    if not math.isfinite(start):
        return max(0.0, fresh - reserve)
    dropped = max(start - fresh, 0.0)
    unseen = max(reserve - dropped, 0.0)
    return max(0.0, fresh - unseen)


def size_hot4_tickets(buys: list, *, cash: float, held: set[str] | None,
                      date: str, s=None, sit: bool = False) -> tuple[list[dict], list[dict]]:
    """Long-only leftover split. No shorts. MARKET sizing uses list px.

    Budgets are an equal split of cash × HOT4_CASH_HAIRCUT, so the sum of
    planned notionals stays strictly under the snapshot.
    """
    from src import factor_mine_book as fmb
    from src.combo_broker import quote_px

    held = {str(t).upper() for t in (held or set())}
    gross = max(float(cash or 0), 0.0)
    leftover = gross * HOT4_CASH_HAIRCUT
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


def _position_shares(positions: dict | None, ticker: str) -> int:
    raw = (positions or {}).get(ticker)
    if raw is None:
        raw = (positions or {}).get(str(ticker).upper())
    if isinstance(raw, dict):
        try:
            n = int(float(raw.get("shares") or 0))
        except (TypeError, ValueError):
            return 0
        return n if n > 0 else 0
    try:
        n = int(float(raw or 0))
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def _position_px(positions: dict | None, ticker: str):
    raw = (positions or {}).get(ticker)
    if raw is None:
        raw = (positions or {}).get(str(ticker).upper())
    if not isinstance(raw, dict):
        return None
    return _hot4_num(raw, "last_px", "cost_px", "px")


def size_hot4_sells(sells: list, *, positions: dict | None,
                    date: str) -> tuple[list[dict], list[dict]]:
    """Exit the recipe sell list using the paper lot, not $10k research size.

    A name the account does not hold is skipped. Clock-B leftovers that
    are not on the recipe sell list are not tickets.
    """
    from src.combo_broker import quote_px

    tickets: list[dict] = []
    skips: list[dict] = []
    seen: set[str] = set()
    for raw in sells or []:
        if isinstance(raw, str):
            t = raw.strip().upper()
            row: dict = {}
        elif isinstance(raw, dict):
            t = str(raw.get("ticker") or raw.get("symbol") or "").upper().strip()
            row = raw
        else:
            continue
        if not t or t in seen:
            continue
        seen.add(t)
        shares = _position_shares(positions, t)
        if shares < 1:
            skips.append({
                "date": date, "ticker": t, "kind": "unheld",
                "reason": f"never sell unheld — no paper lot for {HOT4}",
            })
            continue
        px = _position_px(positions, t)
        if px is None or px <= 0:
            px = _hot4_num(row, "px", "open", "open_px")
        if px is None or px <= 0:
            px = quote_px(t, date, row=row.get("row") if isinstance(
                row.get("row"), dict) else row)
        ticket = {
            "side": "SELL",
            "ticker": t,
            "shares": shares,
            "order_type": "MARKET",
            "status": "plan",
            "sleeve": HOT4,
            "kid_side": "long",
            "date": date,
            "clock": "09:30 ET",
            "reason": f"{HOT4} list-drop after min-hold; paper lot {shares}",
        }
        if px is not None and px > 0:
            ticket["px"] = round(float(px), 4)
            ticket["notional"] = round(shares * float(px), 2)
        tickets.append(ticket)
    return tickets, skips


def _sealed_ticket(date: str, side: str, ticker: str, shares: int, *,
                   px, reason: str) -> dict:
    ticket = {
        "side": side,
        "ticker": ticker,
        "shares": int(shares),
        "order_type": "MARKET",
        "status": "plan",
        "sleeve": HOT4,
        "kid_side": "long",
        "date": date,
        "clock": "09:30 ET",
        "reason": reason,
        "sealed_shares": True,
        "source": "sealed_h1",
    }
    if px is not None and float(px) > 0:
        ticket["px"] = round(float(px), 4)
        ticket["notional"] = round(int(shares) * float(ticket["px"]), 2)
    return ticket


def plan_hot4_for_broker(date: str, snap: BrokerSnap,
                         payload: dict | None = None,
                         panel: dict | None = None) -> dict:
    """Sealed h1 new-buy set for ``date``. ``payload`` and ``panel`` are ignored.

    The strategy name stays ``union_hot_n4_h1``. Buy tickers are the
    names the open fill would buy, not every plan-card pick. Paper cash
    that cannot fund those buys, or a buy for a carry name, returns an
    empty ticket list and ``look_error`` — it does not resize and it
    does not call ``pick_day``.
    """
    del payload, panel  # Factor Mine publish is not the send list.
    from src.h1_sealed_exec import (
        SealedH1Error, assert_buys_match_new_set, sealed_h1_orders,
    )

    cash = max(float(getattr(snap, "cash", 0) or 0), 0.0)
    try:
        orders = sealed_h1_orders(date)
        assert_buys_match_new_set(
            orders["buys"],
            carry=orders.get("carry") or [],
            expected=[row["ticker"] for row in orders["buys"]],
        )
    except SealedH1Error as exc:
        msg = str(exc)
        if "not rebuilding HOT4" not in msg:
            msg = f"{msg}; refusing; not rebuilding HOT4"
        return {
            "date": date,
            "want_date": date,
            "policy": HOT4,
            "combo": "",
            "source": "sealed_h1",
            "stale": False,
            "score": None,
            "hard_red": False,
            "why": msg,
            "tickets": [],
            "skipped": [{"date": date, "ticker": "", "kind": "sealed",
                         "reason": msg}],
            "would_buy": {"rows": []},
            "would_sell": {"rows": []},
            "flatten_ok": True,
            "look_error": msg,
            "order_type": "MARKET",
            "plan_sha256": "",
        }
    would = []
    buy_tickets = []
    for row in orders["buys"]:
        would.append({
            "ticker": row["ticker"],
            "shares": row["shares"],
            "sleeve": HOT4,
            "kid_side": "long",
            "clock": "09:30 ET",
            "px": row["px"],
            "src": ",".join(row.get("sources") or []),
        })
        buy_tickets.append(_sealed_ticket(
            date, "BUY", row["ticker"], row["shares"], px=row["px"],
            reason=(
                f"sealed h1 new buy rank {row['rank']} {row['shares']} sh"
            ),
        ))
    would_sell = []
    sell_tickets = []
    for row in orders["sells"]:
        would_sell.append({
            "ticker": row["ticker"],
            "shares": row["shares"],
            "sleeve": HOT4,
            "kid_side": "long",
            "clock": "09:30 ET",
            "px": row.get("px"),
            "src": row.get("reason") or "planned_sell",
        })
        sell_tickets.append(_sealed_ticket(
            date, "SELL", row["ticker"], row["shares"], px=row.get("px"),
            reason=f"sealed h1 {row.get('reason') or 'planned sell'} {row['shares']} sh",
        ))
    need = float(orders["notional"])
    look_err = ""
    tickets = sell_tickets + buy_tickets
    skips: list[dict] = []
    if need > cash + 1e-6:
        names = ", ".join(
            f"{row['ticker']} {row['shares']}" for row in orders["buys"]
        ) or "none"
        look_err = (
            f"paper cash {cash:.2f} cannot fund sealed h1 buys "
            f"[{names}] costing {need:.2f}; refusing; not rebuilding HOT4"
        )
        tickets = []
        skips.append({
            "date": date, "ticker": "", "kind": "cash", "reason": look_err,
        })
    for ticker in orders.get("carry") or []:
        skips.append({
            "date": date,
            "ticker": ticker,
            "kind": "held",
            "reason": "already held on the sealed book — not a new buy",
        })
    why = (
        f"{HOT4} sealed h1 new buys {orders.get('plan_sha256', '')[:12]} "
        f"· buys {len(orders['buys'])} sells {len(orders['sells'])} "
        f"· carry {len(orders.get('carry') or [])} "
        f"· notional ${need:.2f} · MARKET · no pick_day"
    )
    if look_err:
        why = look_err
    return {
        "date": date,
        "want_date": date,
        "policy": HOT4,
        "combo": "",
        "source": "sealed_h1",
        "stale": False,
        "score": orders.get("morning_s"),
        "hard_red": False,
        "why": why,
        "tickets": tickets,
        "skipped": skips,
        "would_buy": {"rows": would},
        "would_sell": {"rows": would_sell},
        "flatten_ok": True,
        "look_error": look_err,
        "order_type": "MARKET",
        "plan_sha256": orders.get("plan_sha256") or "",
        "book_recipe": orders.get("recipe") or "",
        "sealed_notional": need,
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


def _payload_order_id(payload) -> str:
    """Broker order id only — never a client_order_id echo."""
    if isinstance(payload, dict):
        for key in ("order_id", "orderId"):
            if payload.get(key):
                return str(payload[key])
        for key in ("data", "orders", "result"):
            if key in payload:
                found = _payload_order_id(payload[key])
                if found:
                    return found
    elif isinstance(payload, list):
        for item in payload:
            found = _payload_order_id(item)
            if found:
                return found
    return ""


def _broker_rejected(value) -> bool:
    if isinstance(value, list):
        return any(_broker_rejected(item) for item in value)
    if not isinstance(value, dict):
        return False
    if value.get("success") is False or value.get("error") or value.get("error_code"):
        return True
    if str(value.get("status", "")).upper() in ("REJECTED", "FAILED", "ERROR"):
        return True
    if "code" in value and str(value["code"]).upper() not in ("0", "200", "SUCCESS", "OK"):
        return True
    return any(_broker_rejected(value[key]) for key in ("data", "orders") if key in value)


def _walk_open_orders(payload) -> list:
    """Same row walk as scripts/webull_standtest_probe.py status mode."""
    rows: list = []

    def walk(value):
        if isinstance(value, list):
            for item in value:
                walk(item)
        elif isinstance(value, dict):
            if value.get("order_id") or value.get("orderId") or value.get("client_order_id"):
                rows.append(value)
            for key in ("data", "orders", "list", "items", "result"):
                if key in value:
                    walk(value[key])

    walk(payload)
    identified = []
    orphans = []
    seen = set()
    for row in rows:
        oid = str(row.get("order_id") or row.get("orderId") or "").strip()
        coid = str(row.get("client_order_id") or row.get("clientOrderId") or "").strip()
        if oid:
            if oid in seen:
                continue
            seen.add(oid)
            identified.append(row)
        elif coid:
            orphans.append(row)
    known = {
        str(row.get("client_order_id") or row.get("clientOrderId") or "").strip()
        for row in identified
    }
    seen_coid = set()
    for row in orphans:
        coid = str(row.get("client_order_id") or row.get("clientOrderId") or "").strip()
        if not coid or coid in known or coid in seen_coid:
            continue
        seen_coid.add(coid)
        identified.append(row)
    return identified


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

    def _ensure_account_id(self) -> str:
        if self.account_id:
            return str(self.account_id)
        if self.trade is None:
            raise RuntimeError(self.err or "not connected")
        accounts = self._json(self.trade.account_v2.get_account_list(), "account_list")
        self.account_id = parse_account_id(accounts, "")
        if not self.account_id:
            raise RuntimeError("no Webull account_id in list")
        return str(self.account_id)

    def list_open_orders(self) -> list:
        """Open orders via the standtest probe's SDK attempts, in that order.

        scripts/webull_standtest_probe.py status mode tries
        order_v3.list_order_open, order_v3.get_order_open,
        order_v3.get_order_detail (only when an id is already known),
        then order_v2.get_order_open. A total miss is an error, not an
        empty book — callers must not sell while orders may still be open.
        """
        if self.host != PAPER_HOST:
            raise RuntimeError("paper-open refuses any non-sandbox host")
        if self.trade is None:
            raise RuntimeError(self.err or "not connected")
        aid = self._ensure_account_id()
        trade = self.trade
        want = ""
        open_payload = None
        errors = []
        for call in (
            lambda: trade.order_v3.list_order_open(aid),
            lambda: trade.order_v3.get_order_open(aid),
            lambda: trade.order_v3.get_order_detail(aid, want) if want else (_ for _ in ()).throw(RuntimeError("no want")),
            lambda: trade.order_v2.get_order_open(aid) if hasattr(trade, "order_v2") else (_ for _ in ()).throw(AttributeError("no v2")),
        ):
            try:
                open_payload = self._json(call(), "open_orders")
                break
            except Exception as exc:  # noqa: BLE001 — try the next probe call
                errors.append(str(exc)[:160])
        if open_payload is None:
            raise RuntimeError("open-order list failed: " + " | ".join(errors))
        return _walk_open_orders(open_payload)

    def cancel_order(self, order_id: str) -> dict:
        """Cancel one sandbox order. Same call as the standtest probe."""
        if self.host != PAPER_HOST:
            raise RuntimeError("paper-open refuses any non-sandbox host")
        if self.trade is None:
            raise RuntimeError(self.err or "not connected")
        aid = self._ensure_account_id()
        oid = str(order_id or "").strip()
        if not oid:
            raise RuntimeError("cancel requires order_id")
        try:
            res = self.trade.order_v3.cancel_order(aid, oid)
            payload = self._json(res, "cancel_order")
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "order_id": oid, "error": str(exc)[:400]}
        if _broker_rejected(payload):
            snip = ""
            try:
                snip = json.dumps(payload)[:500]
            except (TypeError, ValueError):
                snip = str(payload)[:500]
            return {"ok": False, "order_id": oid, "error": "broker rejected cancel",
                    "payload_snip": snip}
        return {"ok": True, "order_id": _payload_order_id(payload) or oid}

    def _sandbox_cash(self) -> float | None:
        """Available cash from a live snapshot, or None if we cannot tell.

        A failed or disconnected read must not look like a $0 account —
        the caller keeps the planned shares in that case.
        """
        try:
            snap = self.snapshot()
        except Exception:
            return None
        if snap is None or not getattr(snap, "connected", False):
            return None
        try:
            cash = float(snap.cash)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(cash):
            return None
        return max(cash, 0.0)

    def place_batch(self, tickets):
        """Place each ticket as a one-element list — same path as place().

        Sandbox rejects a multi-order place_order with
        invalid combo_type=["NORMAL", ...] (OPENAPI_PARAM_ERR / HTTP 417).

        Before each BUY, re-read sandbox cash. Shares already acked in
        this batch stay reserved until that cash drop shows up, then the
        leg is clamped to floor(cash_still_free / px). Planned shares are
        never increased. A leg that cannot buy 1 share is skipped.
        A sealed h1 buy (``sealed_shares``) is not resized: if the
        planned share count does not fit, that leg is refused.
        """
        if self.host != PAPER_HOST or self.trade is None or not self.account_id:
            raise RuntimeError("sandbox account not connected")
        out = {}
        ack = datetime.now().astimezone().isoformat()
        start_cash = None
        reserved = 0.0
        for ticket in tickets:
            body_ticket = dict(ticket)
            side = str(body_ticket.get("side") or "").upper()
            px = _hot4_num(body_ticket, "px", "live_px") or 0.0
            planned = int(body_ticket.get("shares") or 0)
            fresh = self._sandbox_cash()
            if fresh is not None and start_cash is None:
                start_cash = fresh
            free = cash_still_free(start_cash, fresh, reserved)
            if side == "BUY" and free is not None and px > 0:
                shares = clamp_buy_shares(planned, px, free)
                if body_ticket.get("sealed_shares") and shares != planned:
                    coid = str(order_body(body_ticket)["client_order_id"])
                    out[coid] = {
                        "ok": False,
                        "shares": planned,
                        "error": (
                            f"sealed h1 {body_ticket.get('ticker')} {planned} shares "
                            f"@ {px:.4f} does not fit cash still free {free:.2f}; "
                            "refusing; not rebuilding HOT4"
                        ),
                        "acknowledged_at": ack,
                    }
                    continue
                if shares < 1:
                    coid = str(order_body(body_ticket)["client_order_id"])
                    out[coid] = {
                        "ok": True,
                        "skipped": True,
                        "shares": 0,
                        "resized_from": planned,
                        "error": (
                            f"remaining cash {free:.2f} < 1 share @ {px:.2f}"
                        ),
                        "acknowledged_at": ack,
                    }
                    continue
                if shares != planned:
                    body_ticket["shares"] = shares
                    body_ticket["notional"] = round(shares * float(px), 2)
            body = order_body(body_ticket)
            coid = str(body["client_order_id"])
            reply = self.place(body_ticket, self.env)
            sent_shares = int(body_ticket.get("shares") or 0)
            row = {
                "ok": bool(reply.get("ok")),
                "order_id": str(reply.get("order_id") or ""),
                "acknowledged_at": reply.get("acknowledged_at") or ack,
                "shares": sent_shares,
            }
            if sent_shares != planned:
                row["resized_from"] = planned
            if reply.get("error"):
                row["error"] = reply["error"]
            if row["ok"] and side == "BUY" and px > 0 and sent_shares > 0:
                reserved += sent_shares * float(px)
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
    if card.get("look_error"):
        print(f"[webull] {card['look_error']}")
        submit = False
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
               any(x.get("kind") in ("cash", "no_price", "sealed")
                   for x in card.get("skipped", []))))
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
                    help="sealed h1 plan as union_hot_n4_h1 (default), flatten escape, or combo")
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
