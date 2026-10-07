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
    python -m src.webull_exec --date 2026-10-02 --submit  # refused; seal and ECS backstop only
    python -m src.webull_exec --source flatten --submit   # refused; seal and ECS backstop only

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
import inspect
import json
import math
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

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
ET = ZoneInfo("America/New_York")
# list_order_history rejects a span over 24h. A fall-back civil day is ~25h.
_HISTORY_MAX_SECONDS = 24 * 60 * 60
_HISTORY_PAGE_CAP = 20
# get_order_history accepts page_size 10..100. 200 is OPENAPI_PARAM_ERR.
_HISTORY_PAGE_SIZE = 100
# Deprecated Account.get_account_position pages (page_size default 10,
# max 100, last_instrument_id). AccountV2.get_account_position, the call
# snapshot() makes, takes only account_id and does not page.
_POSITION_PAGE_CAP = 20
_POSITION_PAGE_SIZE = 100
_HELD_QTY_KEYS = ("quantity", "position", "qty", "shares")
_AVAILABLE_QTY_KEYS = ("available_quantity", "availableQuantity")
_RAW_POSITION_KEYS = (
    "symbol", "ticker", "ticker_id",
    "quantity", "qty", "position", "shares",
    "available_quantity", "availableQuantity",
)
_FILL_PRICE_KEYS = (
    "avg_filled_price", "average_filled_price", "avgFilledPrice",
    "filled_avg_price", "avg_fill_px", "avg_price",
    "fill_price", "filled_price",
)
_FILLED_QTY_KEYS = (
    "filled_quantity", "filledQuantity", "filled_qty", "fill_qty",
)


def _env(name: str, default: str = "") -> str:
    raw = (os.environ.get(name) or default).strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("\"", "'"):
        raw = raw[1:-1].strip()
    return raw


def at_or_after_open_deadline(clock: datetime | None = None) -> bool:
    """True at 09:30:00 ET and any later clock on that same civil day.

    Standing sandbox orders have to be resting before the open. A MARKET
    sent at or after 09:30 fills at the live print. Callers that are not
    the pre-armed bell release (the 0–2s window inside ``paper_open``)
    must not place once this is true.
    """
    zone = ZoneInfo("America/New_York")
    current = clock or datetime.now(zone)
    if current.tzinfo is None:
        current = current.replace(tzinfo=zone)
    else:
        current = current.astimezone(zone)
    target = current.replace(hour=9, minute=30, second=0, microsecond=0)
    return current >= target


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


# Webull client_order_id is at most 32 characters. The sandbox accepted
# hyphens (STANDTEST-20260918-1789726292). Letters, digits, and hyphens only.
_CLIENT_ORDER_ID_MAX = 32


def client_order_id(date: str, side: str, ticker: str, strategy: str = "h1") -> str:
    """Deterministic id from the strategy, session date, ticker, and side.

    Readable form, cut to Webull's 32-character limit:
    ``h1-2026-10-07-SDEV-buy``. The same four fields always produce the
    same id. It does not use a broker order id, a clock, or a random value,
    so a crash before any local save still matches the order on the book.
    """
    strat = re.sub(r"[^A-Za-z0-9]", "", str(strategy or "")).lower() or "h1"
    digits = re.sub(r"[^0-9]", "", str(date or ""))[:8]
    if len(digits) == 8:
        day = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    else:
        day = digits
    side_s = "buy" if str(side or "").upper() == "BUY" else "sell"
    name = re.sub(r"[^A-Za-z0-9]", "", str(ticker or "").upper())
    raw = f"{strat}-{day}-{name}-{side_s}"
    if len(raw) <= _CLIENT_ORDER_ID_MAX:
        return raw
    # Date hyphens cost two characters. Drop them before shortening the ticker.
    compact = f"{strat}-{digits}-{name}-{side_s}"
    if len(compact) <= _CLIENT_ORDER_ID_MAX:
        return compact
    overhead = len(strat) + 1 + len(digits) + 1 + 1 + len(side_s)
    room = _CLIENT_ORDER_ID_MAX - overhead
    if room < 1:
        squashed = re.sub(r"[^A-Za-z0-9]", "", f"{strat}{digits}{name}{side_s}")
        return squashed[:_CLIENT_ORDER_ID_MAX]
    return f"{strat}-{digits}-{name[:room]}-{side_s}"


_DEAD_ORDER_STATUSES = frozenset({
    "CANCELLED", "CANCELED", "REJECTED", "EXPIRED", "FAILED", "INACTIVE",
})
_ROW_DATE_KEYS = (
    "trade_date", "order_date", "date",
    "place_time", "create_time", "order_time", "filled_time",
    "createTime", "orderTime", "filledTime", "placeTime",
    "order_create_time",
)


def _norm_side(side) -> str:
    raw = str(side or "").upper()
    if raw in ("BUY", "B", "LONG"):
        return "BUY"
    if raw in ("SELL", "S", "SHORT"):
        return "SELL"
    return raw


def _row_client_order_id(row: dict) -> str:
    return str(row.get("client_order_id") or row.get("clientOrderId") or "").strip()


def _row_symbol(row: dict) -> str:
    return re.sub(
        r"[^A-Z0-9]", "",
        str(row.get("symbol") or row.get("ticker") or "").upper(),
    )


def order_blocks_resend(row: dict) -> bool:
    """Open and filled orders block a second send. A cancel does not."""
    if not isinstance(row, dict):
        return False
    status = str(
        row.get("status") or row.get("order_status") or row.get("orderStatus") or ""
    ).upper().replace(" ", "_")
    if not status:
        return True
    return status not in _DEAD_ORDER_STATUSES


def _row_session_dates(row: dict) -> set[str]:
    found: set[str] = set()
    for key in _ROW_DATE_KEYS:
        val = row.get(key)
        if val is None or val == "":
            continue
        text = str(val).strip()
        match = re.search(r"(20\d{2}-\d{2}-\d{2})", text)
        if match:
            found.add(match.group(1))
            continue
        if text.isdigit() and len(text) >= 12:
            try:
                stamp = int(text)
                if stamp > 10_000_000_000_000:
                    stamp = stamp / 1000.0
                if stamp > 10_000_000_000:
                    stamp = stamp / 1000.0
                found.add(datetime.fromtimestamp(
                    stamp, ZoneInfo("America/New_York")).date().isoformat())
            except (OverflowError, OSError, ValueError):
                continue
    return found


def row_on_session(row: dict, date: str) -> bool:
    """Keep undated rows. Drop a row whose timestamps are all another day."""
    dates = _row_session_dates(row)
    if not dates:
        return True
    return str(date or "") in dates


def _row_fill_price(row: dict):
    """Average fill. A limit ``price`` is not a fill."""
    for key in _FILL_PRICE_KEYS:
        value = row.get(key)
        if value is None or value == "":
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


def _row_filled_qty(row: dict):
    for key in _FILLED_QTY_KEYS:
        value = row.get(key)
        if value is None or value == "":
            continue
        try:
            return int(float(value))
        except (TypeError, ValueError):
            continue
    return None


def _row_filled(row: dict) -> bool:
    status = str(
        row.get("status") or row.get("order_status") or row.get("orderStatus") or ""
    ).upper().replace(" ", "_")
    if status.startswith("FILL") or "PARTIAL" in status:
        return True
    qty = _row_filled_qty(row)
    return qty is not None and qty > 0


def match_sealed_orders(tickets, rows, date: str, strategy: str = "h1"):
    """Split a sealed-h1 batch into orders already on the book and the rest.

    Match the derived ``client_order_id`` first. A same-day open or filled
    row with the same symbol and side still counts, so an order placed under
    an older id is not sent again. Returns ``(found, missing_tickets)``.
    """
    planned = []
    for ticket in tickets or []:
        if not isinstance(ticket, dict):
            continue
        side = _norm_side(ticket.get("side"))
        coid = client_order_id(
            str(ticket.get("date") or date or ""),
            side,
            ticket.get("ticker") or "",
            strategy=strategy,
        )
        planned.append((ticket, side, coid))
    live = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if not order_blocks_resend(row):
            continue
        if not row_on_session(row, date):
            continue
        live.append(row)
    used: set[int] = set()
    found = []
    matched: set[int] = set()
    derived = {coid for _, _, coid in planned}

    def _hit(ticket, side, coid, row, how):
        hit = {
            "ticker": str(ticket.get("ticker") or ""),
            "side": side,
            "shares": ticket.get("shares"),
            "client_order_id": coid,
            "match": how,
            "order_id": str(row.get("order_id") or row.get("orderId") or ""),
            "broker_client_order_id": _row_client_order_id(row),
            "symbol": str(row.get("symbol") or row.get("ticker") or ""),
            "broker_side": str(row.get("side") or row.get("order_side") or ""),
            "broker_status": str(
                row.get("status") or row.get("order_status") or row.get("orderStatus") or ""
            ),
        }
        if _row_filled(row):
            price = _row_fill_price(row)
            qty = _row_filled_qty(row)
            if price is not None:
                hit["avg_fill_px"] = price
            if qty is not None:
                hit["filled_qty"] = qty
        return hit

    for index, (ticket, side, coid) in enumerate(planned):
        hit = None
        for j, row in enumerate(live):
            if j in used:
                continue
            if _row_client_order_id(row) == coid:
                hit = j
                break
        if hit is None:
            continue
        used.add(hit)
        matched.add(index)
        found.append(_hit(ticket, side, coid, live[hit], "client_order_id"))
    missing = []
    for index, (ticket, side, coid) in enumerate(planned):
        if index in matched:
            continue
        symbol = re.sub(r"[^A-Z0-9]", "", str(ticket.get("ticker") or "").upper())
        hit = None
        for j, row in enumerate(live):
            if j in used:
                continue
            row_coid = _row_client_order_id(row)
            if row_coid and row_coid in derived and row_coid != coid:
                continue
            row_side = _norm_side(
                row.get("side") or row.get("order_side") or row.get("action"))
            if _row_symbol(row) == symbol and row_side == side and symbol:
                hit = j
                break
        if hit is None:
            missing.append(ticket)
            continue
        used.add(hit)
        found.append(_hit(ticket, side, coid, live[hit], "symbol_side"))
    return found, missing


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


def _present_num(row: dict, keys) -> float | None:
    """First present numeric field. Missing is None, so 0 stays 0."""
    if not isinstance(row, dict):
        return None
    for key in keys:
        if key not in row:
            continue
        raw = row.get(key)
        if raw is None or raw == "" or isinstance(raw, (dict, list)):
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def _compact_position_fields(*rows: dict) -> dict:
    """Symbol and quantity fields only. No account ids, no other columns."""
    out = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in _RAW_POSITION_KEYS:
            if key not in row:
                continue
            value = row.get(key)
            if value is None or value == "" or isinstance(value, (dict, list)):
                continue
            out[key] = value
    return out


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
    """Held lots keyed by symbol.

    ``quantity`` / ``position`` / ``qty`` / ``shares`` is the lot size.
    ``available_quantity`` is sellable shares and stays a separate field.
    A lot with quantity > 0 and available 0 is still held: unsettled or
    reserved shares used to disappear because available was read first.
    """
    out = {}
    for row in _as_list(payload):
        if not isinstance(row, dict):
            continue
        inner = row.get("position") if isinstance(row.get("position"), dict) else row
        t = str(inner.get("symbol") or inner.get("ticker_id")
                or inner.get("ticker") or row.get("symbol")
                or row.get("ticker_id") or row.get("ticker") or "").upper().strip()
        if "." in t and t.split(".", 1)[0] in ("US", "NYSE", "NASDAQ"):
            t = t.split(".", 1)[-1]
        held = _present_num(inner, _HELD_QTY_KEYS)
        available = _present_num(inner, _AVAILABLE_QTY_KEYS)
        if held is None:
            held = available
        if not t or held is None or int(held) < 1:
            continue
        sh = int(held)
        px = _num(inner, "cost_price", "average_price", "avg_price")
        last = _num(inner, "last_price", "market_price", "current_price",
                    default=px)
        mv = _num(inner, "market_value", "market_val", default=sh * last)
        avail_out = None if available is None else int(available)
        out[t] = {
            "shares": sh,
            "quantity": sh,
            "available_quantity": avail_out,
            "cost_px": px,
            "last_px": last,
            "mv": mv,
            "raw": _compact_position_fields(row, inner),
        }
    return out


def _flag_or_none(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in ("1", "true", "yes"):
        return True
    if text in ("0", "false", "no"):
        return False
    return None


def _position_has_next(payload):
    """True, False, or None when this page does not say."""
    if not isinstance(payload, dict):
        return None
    boxes = [payload]
    data = payload.get("data")
    if isinstance(data, dict):
        boxes.append(data)
    for box in boxes:
        for key in ("has_next", "hasNext"):
            if key not in box or box.get(key) in (None, ""):
                continue
            flag = _flag_or_none(box.get(key))
            if flag is not None:
                return flag
    return None


def _position_cursor(payload) -> str:
    if isinstance(payload, dict):
        boxes = [payload]
        data = payload.get("data")
        if isinstance(data, dict):
            boxes.append(data)
        for box in boxes:
            for key in ("last_instrument_id", "lastInstrumentId"):
                value = box.get(key)
                if value not in (None, ""):
                    return str(value)
    rows = [row for row in _as_list(payload) if isinstance(row, dict)]
    if not rows:
        return ""
    last = rows[-1]
    for key in ("instrument_id", "instrumentId"):
        value = last.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _merge_position_pages(pages: list):
    if len(pages) == 1:
        return pages[0]
    rows = []
    for page in pages:
        rows.extend(row for row in _as_list(page) if isinstance(row, dict))
    return {"positions": rows}


def _read_position_pages(api, fn, account_id: str):
    """Read get_account_position, following pages when that method pages.

    AccountV2's method takes only account_id, so this is one call.
    The deprecated Account method takes page_size and last_instrument_id;
    those are followed while has_next is true, up to the page cap.
    """
    names = _param_names(fn)
    can_page = "page_size" in names or "last_instrument_id" in names
    pages = []
    cursor = ""
    for _page in range(_POSITION_PAGE_CAP):
        if can_page:
            kwargs = {}
            if "page_size" in names:
                kwargs["page_size"] = _POSITION_PAGE_SIZE
            if cursor and "last_instrument_id" in names:
                kwargs["last_instrument_id"] = cursor
            body = api._json(fn(account_id, **kwargs), "positions")
        else:
            body = api._json(fn(account_id), "positions")
        pages.append(body)
        if _position_has_next(body) is not True:
            break
        nxt = _position_cursor(body)
        if not nxt or nxt == cursor or "last_instrument_id" not in names:
            raise RuntimeError(
                "position list has another page but "
                "get_account_position cannot request it")
        cursor = nxt
    else:
        raise RuntimeError(
            "position pagination exceeded " + str(_POSITION_PAGE_CAP))
    return _merge_position_pages(pages)


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
    # Sandbox positions are not an input. A drifted account must not
    # empty this list, add a catch-up order, or change share counts.
    # Cash below the sealed buy notional still fails closed.
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
            side, ticket.get("ticker") or "",
            strategy="h1"),
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


def _history_bounds(day: str) -> list[tuple[datetime, datetime]]:
    """One ET civil day as instants, each spanning at most 24h.

    Adding ``timedelta(hours=24)`` follows the wall clock, so on the
    fall-back Sunday it lands about 25h later. Split on absolute time.
    """
    text = str(day or "")[:10]
    year_s, month_s, day_s = text.split("-")
    start = datetime(int(year_s), int(month_s), int(day_s), tzinfo=ET)
    next_day = start.date() + timedelta(days=1)
    end = datetime(next_day.year, next_day.month, next_day.day, tzinfo=ET)
    end = end - timedelta(milliseconds=1)
    bounds: list[tuple[datetime, datetime]] = []
    cursor = start
    for _ in range(4):
        if end.timestamp() - cursor.timestamp() <= _HISTORY_MAX_SECONDS:
            bounds.append((cursor, end))
            return bounds
        chunk_end = datetime.fromtimestamp(
            cursor.timestamp() + _HISTORY_MAX_SECONDS - 0.001, ET)
        bounds.append((cursor, chunk_end))
        cursor = datetime.fromtimestamp(chunk_end.timestamp() + 0.001, ET)
    raise ValueError("history window split failed for " + text)


def _fmt_iso_offset(moment: datetime) -> str:
    """ISO-8601 with a colon offset: ``2026-10-06T00:00:00.000-04:00``."""
    local = moment.astimezone(ET)
    millis = local.microsecond // 1000
    off = local.strftime("%z")
    if len(off) == 5:
        off = off[:3] + ":" + off[3:]
    return local.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}" + off


def _fmt_utc_millis(moment: datetime) -> str:
    """UTC with milliseconds and a literal Z."""
    utc = moment.astimezone(ZoneInfo("UTC"))
    millis = utc.microsecond // 1000
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}Z"


def _fmt_utc(moment: datetime) -> str:
    """Same shape as the SDK ``x-timestamp`` header: ``yyyy-MM-dd'T'HH:mm:ssZ``."""
    utc = moment.astimezone(ZoneInfo("UTC"))
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fmt_epoch_ms(moment: datetime) -> str:
    return str(int(round(moment.timestamp() * 1000)))


# Order is the try order. ``-0400`` (no colon) and a bare ``yyyy-MM-dd``
# were both rejected by the sandbox (run 37538537300). The SDK docstring
# says ``yyyy-MM-dd'T'HH:mm:ss.SSSZ`` and does not give an example.
_HISTORY_TIME_FORMATS = (
    ("iso_offset", _fmt_iso_offset),
    ("utc_millis_z", _fmt_utc_millis),
    ("utc_z", _fmt_utc),
    ("epoch_ms", _fmt_epoch_ms),
)


def history_time_candidates(day: str) -> list[tuple[str, list[tuple[str, str]]]]:
    """``(name, [(start_time, end_time), ...])`` for one session.

    Each name is one format. A fall-back day may be two windows, both in
    that same format, and each window is at most 24h.
    """
    bounds = _history_bounds(day)
    candidates = []
    for name, fmt in _HISTORY_TIME_FORMATS:
        windows = [(fmt(start), fmt(end)) for start, end in bounds]
        candidates.append((name, windows))
    return candidates


def history_error_text(exc) -> str:
    """The server's message, from ``ServerException:`` through the request id.

    The SDK prefixes that sentence with the request dump. A short slice of
    the whole string stops at ``HTTP Stat`` and drops the parameter error.
    """
    text = str(exc).strip()
    at = text.find("ServerException:")
    if at >= 0:
        return text[at:].strip()
    return text


def _param_names(fn) -> set[str]:
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return set()
    return {
        name for name, param in signature.parameters.items()
        if param.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    }


def _history_pagination_key(payload) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("pagination_key", "paginationKey"):
        value = payload.get(key)
        if value:
            return str(value)
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("pagination_key", "paginationKey"):
            value = data.get(key)
            if value:
                return str(value)
    return ""


def _note_history_format(api, day: str, label: str) -> None:
    formats = getattr(api, "history_formats", None)
    if not isinstance(formats, dict):
        formats = {}
        try:
            api.history_formats = formats
        except Exception:
            return
    formats[str(day or "")[:10]] = label
    try:
        api.last_history_format = label
    except Exception:
        pass


def _fetch_history_windows(api, fn, account_id: str, windows, names: set[str]):
    pages = []
    for start_time, end_time in windows:
        page_key = ""
        for _page in range(_HISTORY_PAGE_CAP):
            kwargs = {"start_time": start_time, "end_time": end_time}
            if "page_size" in names:
                kwargs["page_size"] = _HISTORY_PAGE_SIZE
            if page_key and "pagination_key" in names:
                kwargs["pagination_key"] = page_key
            body = api._json(fn(account_id, **kwargs), "filled_orders")
            pages.append(body)
            if "pagination_key" not in names:
                break
            page_key = _history_pagination_key(body)
            if not page_key:
                break
        else:
            raise RuntimeError(
                "history pagination exceeded " + str(_HISTORY_PAGE_CAP))
    if len(pages) == 1:
        return pages[0]
    return pages


def _history_on_day(payload, day: str):
    session = str(day or "")[:10]
    kept = [
        row for row in _walk_open_orders(payload)
        if row_on_session(row, session)
    ]
    return {"orders": kept}


def _history_by_timestamp(api, fn, account_id: str, date: str, names: set[str]):
    """Try each start_time format. The first accepted call wins.

    An unscoped call (the server default, last 7 days) is last. An empty
    default is not success: that day may sit outside the default window,
    and an empty book must not be reported as a confirmed miss.
    """
    errors = []
    for label, windows in history_time_candidates(date):
        try:
            body = _fetch_history_windows(api, fn, account_id, windows, names)
        except Exception as exc:  # noqa: BLE001 — next format
            errors.append(label + ": " + history_error_text(exc))
            continue
        _note_history_format(api, date, label)
        return body
    try:
        body = api._json(fn(account_id), "filled_orders")
    except Exception as exc:  # noqa: BLE001 — next history method
        errors.append("default: " + history_error_text(exc))
        raise RuntimeError(" | ".join(errors))
    kept = _history_on_day(body, date)
    if not kept["orders"]:
        errors.append("default: empty")
        raise RuntimeError(" | ".join(errors))
    _note_history_format(api, date, "default")
    return kept


def _next_civil_day(day: str) -> str:
    text = str(day or "")[:10]
    year_s, month_s, day_s = text.split("-")
    start = datetime(int(year_s), int(month_s), int(day_s), tzinfo=ET)
    return (start.date() + timedelta(days=1)).isoformat()


def _history_by_date(api, fn, account_id: str, date: str, names: set[str]):
    """``get_order_history`` dates. Same-day start and end was rejected.

    The sandbox answered ``invalid start_date,end_date`` for
    ``2026-10-06,2026-10-06``. Try an exclusive next-day end, then the
    server default. ``page_size`` stays 100 when the signature has it
    (200 is out of range).
    """
    session = str(date or "")[:10]
    attempts = (
        ("start_date_exclusive", {
            "start_date": session,
            "end_date": _next_civil_day(session),
        }),
        ("start_date_default", {}),
    )
    errors = []
    for label, extra in attempts:
        kwargs = dict(extra)
        if "page_size" in names:
            kwargs["page_size"] = _HISTORY_PAGE_SIZE
        try:
            body = api._json(fn(account_id, **kwargs), "filled_orders")
        except Exception as exc:  # noqa: BLE001 — next date shape
            errors.append(label + ": " + history_error_text(exc))
            continue
        if label == "start_date_default":
            kept = _history_on_day(body, session)
            if not kept["orders"]:
                errors.append("start_date_default: empty")
                continue
            body = kept
        _note_history_format(api, session, label)
        return body
    raise RuntimeError(" | ".join(errors))


def _try_history_call(api, fn, account_id: str, date: str):
    """Call one history method with the arguments its signature accepts.

    ``list_order_history`` takes ``start_time`` / ``end_time``. Several
    formats are tried; a bare date is never passed positionally.
    ``get_order_history`` takes ``yyyy-MM-dd`` plus ``page_size`` of at
    most 100. The error text keeps the server's full ``ServerException``
    sentence. Any error fails this method. The caller tries the next
    method and, if all of them fail, raises rather than treating the
    miss as no fills.
    """
    names = _param_names(fn)
    try:
        if "start_time" in names:
            return _history_by_timestamp(api, fn, account_id, date, names), ""
        if "start_date" in names:
            return _history_by_date(api, fn, account_id, date, names), ""
        body = api._json(fn(account_id), "filled_orders")
        _note_history_format(api, date, "default")
        return body, ""
    except Exception as exc:  # noqa: BLE001 — next history method
        return None, history_error_text(exc)


class PaperAPI:
    """Thin official-SDK wrapper. Missing package / keys → connected=False."""

    def __init__(self, env: str = "paper"):
        self.env = "real" if env == "real" else "paper"
        self.host = paper_host(self.env)
        self.trade = None
        self.account_id = _env("WEBULL_ACCOUNT_ID")
        self.err: str | None = None
        self.history_formats: dict = {}
        self.last_history_format = ""

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
            pos = _read_position_pages(
                self, self.trade.account_v2.get_account_position, self.account_id)
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

    def list_filled_orders(self, date: str) -> list:
        """Filled and history rows for one session. A miss is an error.

        An empty payload is a real empty book. No history method on the
        client, or every call failing, is a failed query — callers must
        not place while a fill may already be on the sandbox book.
        """
        if self.host != PAPER_HOST:
            raise RuntimeError("paper-open refuses any non-sandbox host")
        if self.trade is None:
            raise RuntimeError(self.err or "not connected")
        aid = self._ensure_account_id()
        day = str(date or "")
        names = (
            "list_order_history",
            "get_order_history",
            "list_history_orders",
            "get_history_orders",
            "list_filled_orders",
            "get_order_filled",
            "list_today_orders",
            "get_today_orders",
        )
        candidates = []
        for label, obj in (
            ("v3", getattr(self.trade, "order_v3", None)),
            ("v2", getattr(self.trade, "order_v2", None)),
        ):
            if obj is None:
                continue
            for name in names:
                fn = getattr(obj, name, None)
                if callable(fn):
                    candidates.append((f"{label}.{name}", fn))
        if not candidates:
            raise RuntimeError(
                "filled-order list failed: no history method on the sandbox client")
        errors = []
        payload = None
        for label, fn in candidates:
            got, err = _try_history_call(self, fn, aid, day)
            if got is not None:
                payload = got
                break
            if err:
                errors.append(label + ": " + err)
        if payload is None:
            raise RuntimeError("filled-order list failed: " + " | ".join(errors))
        return _walk_open_orders(payload)

    def list_session_orders(self, date: str) -> list:
        """Today's open and filled sandbox orders. Either query failing raises.

        Cancelled, rejected, and expired rows are dropped. A row dated on
        another session is dropped. Undated open rows stay, because a
        working order with no timestamp must still block a second send.
        """
        if self.host != PAPER_HOST:
            raise RuntimeError("paper-open refuses any non-sandbox host")
        open_rows = self.list_open_orders()
        filled_rows = self.list_filled_orders(date)
        seen = set()
        out = []
        for row in list(open_rows or []) + list(filled_rows or []):
            if not isinstance(row, dict):
                continue
            if not order_blocks_resend(row):
                continue
            if not row_on_session(row, date):
                continue
            key = (
                str(row.get("order_id") or row.get("orderId") or ""),
                _row_client_order_id(row),
                _row_symbol(row),
                _norm_side(row.get("side") or row.get("order_side")),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
        return out

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


_PAPER_ENV_KEYS = ("WEBULL_APP_KEY", "WEBULL_APP_SECRET", "WEBULL_ACCOUNT_ID")


def read_paper_env(path) -> dict:
    """Parse the ECS paper env file. Values are JSON strings. No printing."""
    out = {}
    text = Path(path).read_text(encoding="utf-8")
    for line in text.splitlines():
        if not line or "=" not in line or line.startswith("#"):
            continue
        key, raw = line.split("=", 1)
        out[key] = json.loads(raw)
    return out


def write_paper_env(path, values: dict) -> None:
    """Rewrite the env file. Does not log values."""
    ordered = [key for key in _PAPER_ENV_KEYS if values.get(key)]
    extra = [key for key in values if key not in _PAPER_ENV_KEYS and values.get(key)]
    lines = [
        key + "=" + json.dumps(values[key])
        for key in ordered + extra
    ]
    dest = Path(path)
    tmp = dest.with_name(dest.name + ".tmp")
    tmp.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    os.chmod(tmp, 0o600)
    os.replace(tmp, dest)


def discover_and_persist_account_id(env_path, *, api=None) -> str:
    """Resolve sandbox account_id when the env file has keys but no id.

    ``WEBULL_ACCOUNT_ID`` is optional. After a paper connect, the account
    list supplies the id and this writes it back. A non-sandbox host is
    refused. Nothing secret, including the account id, is printed.
    Returns ``present`` when the file already had an id, ``discovered``
    when this call wrote one.
    """
    vals = read_paper_env(env_path)
    key = str(vals.get("WEBULL_APP_KEY") or "").strip()
    secret = str(vals.get("WEBULL_APP_SECRET") or "").strip()
    if not key or not secret:
        raise RuntimeError("WEBULL_APP_KEY / WEBULL_APP_SECRET missing")
    os.environ["WEBULL_APP_KEY"] = key
    os.environ["WEBULL_APP_SECRET"] = secret
    had = str(vals.get("WEBULL_ACCOUNT_ID") or "").strip()
    if had:
        os.environ["WEBULL_ACCOUNT_ID"] = had
    else:
        os.environ.pop("WEBULL_ACCOUNT_ID", None)
        vals.pop("WEBULL_ACCOUNT_ID", None)
    client = api if api is not None else PaperAPI("paper")
    if getattr(client, "env", "paper") == "real" or getattr(client, "host", "") != PAPER_HOST:
        raise RuntimeError("paper-open refuses any non-sandbox host")
    if not client.connect():
        raise RuntimeError(client.err or "not connected")
    snap = client.snapshot()
    if not getattr(snap, "connected", False):
        raise RuntimeError(getattr(snap, "error", None) or "not connected")
    found = str(getattr(snap, "acc_id", "") or getattr(client, "account_id", "") or "").strip()
    if not found:
        raise RuntimeError("no Webull account_id in list")
    if had:
        return "present"
    vals["WEBULL_ACCOUNT_ID"] = found
    write_paper_env(env_path, vals)
    return "discovered"


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
        combo: str = PAPER_COMBO, allow_stale: bool = False,
        clock: datetime | None = None) -> int:
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
    # This CLI and sleeve_merge --submit-webull are not senders.
    # The h1 seal and the ECS backstop go through paper_open. At or
    # after 09:30 the refusal is a missed_deadline; before that it
    # still places nothing.
    late_refused = False
    blocked_submit = False
    if submit:
        blocked_submit = True
        if at_or_after_open_deadline(clock):
            print("[webull] at or after 09:30 ET; paper submit refused")
            late_refused = True
        else:
            print("[webull] paper submit refused; only the h1 seal and "
                  "the ECS backstop place orders")
        submit = False
    last = send_card(card, snap, submit=submit, opend=api, env=env)
    if late_refused:
        last["status"] = "missed_deadline"
        last["submit"] = False
    elif blocked_submit:
        last["status"] = "refused"
        last["submit"] = False
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
                    help="refused: paper orders are placed only by the h1 seal and the ECS backstop")
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
