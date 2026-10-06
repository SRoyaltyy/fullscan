"""Read the Webull sandbox account and append drift-log lines.

Manual dispatch only. This module places, modifies, and cancels nothing.
It refuses a live host. From 03:00 ET until 09:40 ET it does not run:
nothing writes to the books in that window.

Each run appends two lines and leaves every earlier line alone, including
the 2026-10-06 gap. The first new line is ``account_snapshot`` (positions,
today's open orders, and recent history). The second is ``correction``:
FEAM held or not, SDEV held or not, and whether the 10:17 PACB, DNA and
QSI buys and the GLND and NAUT sells filled, with prices.

The gap line's ``feam.held_shares`` 939 is the sealed book's count.
This module does not treat that figure as the account's position.
"""
from __future__ import annotations

import json
import math
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from src import paper_drift

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
DRIFT_LOG = ROOT / "data" / "paper_open" / "drift_log.jsonl"
INCIDENT_DAY = "2026-10-06"
SDEV_ENTRY = "2026-09-30"
FEAM_BOOK_SHARES = 939
SDEV_BOOK_SHARES = 1222
MAX_ORDER_DAYS = 15
BOOK_COUNT_SENTENCE = (
    "The earlier held_shares 939 was the sealed book's share count, "
    "not the account's."
)

# Calls a snapshot is allowed to make on PaperAPI. Anything else is refused
# before the underlying client sees it.
ALLOWED_CALLS = frozenset({
    "connect",
    "snapshot",
    "list_open_orders",
    "list_filled_orders",
})
FORBIDDEN_CALLS = frozenset({
    "place",
    "place_batch",
    "place_order",
    "cancel_order",
    "modify_order",
    "replace_order",
    "amend_order",
})

# The 10:17 ET orders named on the gap line. Used when that line is not
# in the file being appended to. FEAM is not here: the sell was rejected
# and the correction reports whether the account holds it.
FALLBACK_ORDERS = (
    {
        "ticker": "PACB", "side": "BUY", "shares": 1113,
        "order_id": "3IG4QHJ4IC309B8FJNLRCJD0GA",
        "client_order_id": "fs20261006BPACB",
    },
    {
        "ticker": "DNA", "side": "BUY", "shares": 214,
        "order_id": "3UHAD2KIHSSID8SO8KDQLQKR09",
        "client_order_id": "fs20261006BDNA",
    },
    {
        "ticker": "QSI", "side": "BUY", "shares": 2482,
        "order_id": "HUIIVGPG79II8OCHIG4IDT7818",
        "client_order_id": "fs20261006BQSI",
    },
    {
        "ticker": "GLND", "side": "SELL", "shares": 633,
        "order_id": "NJJO7E6QDTMI40CLS4EGQ4KRL8",
        "client_order_id": "fs20261006SGLND",
    },
    {
        "ticker": "NAUT", "side": "SELL", "shares": 1668,
        "order_id": "1GIIVILVRVQI877IGQUS6D5ASA",
        "client_order_id": "fs20261006SNAUT",
    },
)

_FILLED_STATUSES = frozenset({
    "FILLED", "FILL", "FULL_FILL", "ALL_FILLED",
    "PARTIALLY_FILLED", "PARTIAL_FILLED", "PARTIAL",
})
_DEAD_STATUSES = frozenset({
    "CANCELLED", "CANCELED", "REJECTED", "EXPIRED", "FAILED", "INACTIVE",
})
_PRICE_KEYS = (
    "avg_filled_price", "average_filled_price", "avgFilledPrice",
    "filled_avg_price", "avg_price", "average_price",
    "fill_price", "filled_price", "price",
)
_FILLED_QTY_KEYS = (
    "filled_quantity", "filledQuantity", "filled_qty", "fill_qty",
)
_QTY_KEYS = (
    "total_quantity", "totalQuantity", "quantity", "qty",
    "shares", "order_qty",
)
_DATE_KEYS = (
    "trade_date", "order_date", "date", "filled_time", "filledTime",
    "place_time", "create_time", "order_time", "createTime", "orderTime",
)


class SnapshotRefused(RuntimeError):
    """The read-only snapshot will not perform this call."""


class SandboxRefused(SnapshotRefused):
    """The client is not the Webull sandbox."""


def now() -> datetime:
    return datetime.now(ET)


def _as_et(clock: datetime) -> datetime:
    if clock.tzinfo is None:
        return clock.replace(tzinfo=ET)
    return clock.astimezone(ET)


def in_write_freeze(clock: datetime) -> bool:
    """True from 03:00 ET inclusive until 09:40 ET exclusive.

    Same closed window the Webull sim uses. Nothing writes to the books
    then, and this snapshot does not run.
    """
    local = _as_et(clock)
    start = local.replace(hour=3, minute=0, second=0, microsecond=0)
    end = local.replace(hour=9, minute=40, second=0, microsecond=0)
    return start <= local < end


def require_sandbox(api) -> str:
    """Return the sandbox host, or refuse a live / empty / other host."""
    from src.webull_exec import PAPER_HOST

    host = str(getattr(api, "host", "") or "")
    env = str(getattr(api, "env", "") or "")
    labels = host.split(".")
    if env == "real" or host != PAPER_HOST or "sandbox" not in labels:
        raise SandboxRefused("paper snapshot refuses any non-sandbox host")
    return host


def default_api():
    """The existing paper client. Paper env only; connect happens later."""
    from src.webull_exec import PaperAPI

    api = PaperAPI("paper")
    require_sandbox(api)
    return api


class ReadOnlyPaper:
    """PaperAPI surface limited to connect, snapshot, and order lists.

    ``place``, ``place_batch``, ``cancel_order``, and any modify/replace
    call raise before the wrapped client is touched.
    """

    def __init__(self, api):
        self._api = api
        self.calls: list[str] = []

    def _call(self, name: str, *args, **kwargs):
        if name in FORBIDDEN_CALLS or name not in ALLOWED_CALLS:
            raise SnapshotRefused("read-only snapshot refuses " + name)
        require_sandbox(self._api)
        self.calls.append(name)
        fn = getattr(self._api, name)
        return fn(*args, **kwargs)

    def connect(self):
        return self._call("connect")

    def snapshot(self):
        return self._call("snapshot")

    def list_open_orders(self):
        return self._call("list_open_orders")

    def list_filled_orders(self, day: str):
        return self._call("list_filled_orders", day)

    def history_format_used(self) -> dict:
        """Which start_time/start_date shape the last history read accepted."""
        raw = getattr(self._api, "history_formats", None)
        if not isinstance(raw, dict):
            return {}
        return {str(key): str(value) for key, value in raw.items()}

    def __getattr__(self, name: str):
        if name in FORBIDDEN_CALLS or name not in ALLOWED_CALLS:
            raise SnapshotRefused("read-only snapshot refuses " + name)
        raise AttributeError(name)


def _finite(value):
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _opt_float(row: dict, *keys: str):
    for key in keys:
        if key not in row or row.get(key) in (None, ""):
            continue
        number = _finite(row.get(key))
        if number is not None:
            return number
    return None


def _opt_int(row: dict, *keys: str):
    number = _opt_float(row, *keys)
    if number is None:
        return None
    return int(number)


def _money(value):
    number = _finite(value)
    if number is None:
        return None
    return round(number, 4)


def _symbol(row: dict) -> str:
    raw = str(
        row.get("symbol") or row.get("ticker") or row.get("ticker_id") or ""
    ).upper().strip()
    if "." in raw:
        head, tail = raw.split(".", 1)
        if head in ("US", "NYSE", "NASDAQ"):
            raw = tail
        elif tail in ("US", "NYSE", "NASDAQ"):
            raw = head
    return re.sub(r"[^A-Z0-9]", "", raw)


def _side(row: dict) -> str:
    raw = str(row.get("side") or row.get("order_side") or row.get("orderSide") or "")
    text = raw.upper()
    if text in ("BUY", "B", "LONG"):
        return "BUY"
    if text in ("SELL", "S", "SHORT"):
        return "SELL"
    return text


def _status(row: dict) -> str:
    raw = row.get("status")
    if raw in (None, ""):
        raw = row.get("order_status")
    if raw in (None, ""):
        raw = row.get("orderStatus")
    return str(raw or "").upper().replace(" ", "_")


def _dig(row: dict, reader, keys):
    found = reader(row, *keys)
    if found is not None:
        return found
    for nest_name in ("order", "filled", "fill"):
        nested = row.get(nest_name)
        if isinstance(nested, dict):
            found = reader(nested, *keys)
            if found is not None:
                return found
    return None


def session_dates(row: dict) -> list[str]:
    found: list[str] = []
    for key in _DATE_KEYS:
        val = row.get(key)
        if val in (None, ""):
            continue
        match = re.search(r"(20\d{2}-\d{2}-\d{2})", str(val))
        if match and match.group(1) not in found:
            found.append(match.group(1))
    return found


def position_rows(snap) -> list[dict]:
    """Current long lots as ticker, qty, avg cost. Shorts and zeros drop."""
    raw = getattr(snap, "positions", None) or {}
    rows = []
    for ticker, lot in raw.items():
        name = str(ticker).upper().strip()
        if isinstance(lot, dict):
            qty = _opt_int(lot, "shares", "qty", "quantity")
            cost = _opt_float(lot, "cost_px", "avg_cost", "average_price", "avg_price")
        else:
            qty = _opt_int({"qty": lot}, "qty")
            cost = None
        if not name or qty is None or qty < 1:
            continue
        rows.append({
            "ticker": name,
            "qty": qty,
            "avg_cost": _money(cost),
        })
    rows.sort(key=lambda row: row["ticker"])
    return rows


def qty_of(rows, ticker: str) -> int:
    name = str(ticker).upper()
    for row in rows or []:
        if row.get("ticker") == name:
            try:
                return int(row.get("qty") or 0)
            except (TypeError, ValueError):
                return 0
    return 0


def cost_of(rows, ticker: str):
    name = str(ticker).upper()
    for row in rows or []:
        if row.get("ticker") == name:
            return row.get("avg_cost")
    return None


def normalize_order(row: dict, *, source: str, queried_date: str | None) -> dict:
    dates = session_dates(row)
    if INCIDENT_DAY in dates:
        session = INCIDENT_DAY
    elif dates:
        session = dates[0]
    elif source == "history":
        session = queried_date
    else:
        session = None
    price = _dig(row, _opt_float, _PRICE_KEYS)
    return {
        "ticker": _symbol(row),
        "side": _side(row),
        "status": _status(row),
        "qty": _dig(row, _opt_int, _QTY_KEYS),
        "filled_qty": _dig(row, _opt_int, _FILLED_QTY_KEYS),
        "avg_price": _money(price),
        "order_id": str(row.get("order_id") or row.get("orderId") or ""),
        "client_order_id": str(
            row.get("client_order_id") or row.get("clientOrderId") or ""
        ),
        "session_date": session,
        "source": source,
        "open_now": source == "open",
    }


def _rank_order(row: dict) -> tuple:
    filled_qty = row.get("filled_qty") or 0
    return (
        1 if row.get("avg_price") is not None else 0,
        1 if filled_qty > 0 else 0,
        1 if row.get("source") == "history" else 0,
    )


def dedupe_orders(rows: list[dict]) -> list[dict]:
    """One row per order id. A history row with a price beats an open echo."""
    by_key: dict[tuple, dict] = {}
    order: list[tuple] = []
    for row in rows:
        oid = row.get("order_id") or ""
        coid = row.get("client_order_id") or ""
        if oid:
            key = ("id", oid)
        elif coid:
            key = ("coid", coid)
        else:
            key = ("row", id(row))
        prev = by_key.get(key)
        if prev is None:
            by_key[key] = row
            order.append(key)
            continue
        if _rank_order(row) >= _rank_order(prev):
            by_key[key] = row
    out = [by_key[key] for key in order]
    out.sort(key=lambda row: (
        row.get("session_date") or "",
        row.get("ticker") or "",
        row.get("side") or "",
        row.get("order_id") or "",
    ))
    return out


def order_dates(clock: datetime) -> list[str]:
    """Weekdays from the SDEV entry through today, always including 10-06.

    A later dispatch keeps the incident day and the most recent sessions.
    """
    end = _as_et(clock).date()
    incident = date.fromisoformat(INCIDENT_DAY)
    start = date.fromisoformat(SDEV_ENTRY)
    if end < incident:
        end = incident
    days: list[str] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    if len(days) <= MAX_ORDER_DAYS:
        return days
    out: list[str] = []
    for day in [INCIDENT_DAY, *days[-(MAX_ORDER_DAYS - 1):]]:
        if day not in out:
            out.append(day)
    return out


def collect_orders(guard: ReadOnlyPaper, clock: datetime) -> dict:
    """Open orders plus history for the incident day and recent sessions.

    A failed query is recorded. It is not treated as an empty book.
    """
    errors = {}
    rows: list[dict] = []
    open_ok = False
    try:
        payload = guard.list_open_orders() or []
        open_ok = True
        for raw in payload:
            if isinstance(raw, dict):
                rows.append(normalize_order(raw, source="open", queried_date=None))
    except SnapshotRefused:
        raise
    except Exception as exc:  # noqa: BLE001 — recorded, not turned into an order
        errors["open"] = str(exc)[:240]
    history_ok: dict[str, bool] = {}
    for day in order_dates(clock):
        try:
            payload = guard.list_filled_orders(day) or []
            history_ok[day] = True
            for raw in payload:
                if isinstance(raw, dict):
                    rows.append(normalize_order(
                        raw, source="history", queried_date=day,
                    ))
        except SnapshotRefused:
            raise
        except Exception as exc:  # noqa: BLE001
            history_ok[day] = False
            errors[day] = str(exc)
    incident_history_ok = bool(history_ok.get(INCIDENT_DAY))
    formats = {}
    reader = getattr(guard, "history_format_used", None)
    if callable(reader):
        formats = reader() or {}
    return {
        "orders": dedupe_orders(rows),
        "open_ok": open_ok,
        "history_ok": history_ok,
        "incident_history_ok": incident_history_ok,
        "history_formats": formats,
        "errors": errors,
        "queried_dates": order_dates(clock),
    }


def read_gap(path: Path) -> dict | None:
    """The 2026-10-06 gap line, if this log already has it. Read only."""
    if not path.is_file():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("kind") == "gap" and str(row.get("date") or "") == INCIDENT_DAY:
            return row
    return None


def incident_orders(gap: dict | None) -> list[dict]:
    """10:17 buys and the GLND / NAUT sells. FEAM stays off this list."""
    if not gap:
        return [dict(row) for row in FALLBACK_ORDERS]
    out = []
    for row in gap.get("buys_sent_late") or []:
        if isinstance(row, dict):
            item = dict(row)
            item["side"] = "BUY"
            out.append(item)
    for row in gap.get("sells_sent_late") or []:
        if isinstance(row, dict):
            item = dict(row)
            item["side"] = "SELL"
            out.append(item)
    return out or [dict(row) for row in FALLBACK_ORDERS]


def _match_order(rows: list[dict], spec: dict) -> dict | None:
    oid = str(spec.get("order_id") or "")
    coid = str(spec.get("client_order_id") or "")
    ticker = str(spec.get("ticker") or "").upper()
    side = str(spec.get("side") or "").upper()
    if oid:
        hits = [row for row in rows if row.get("order_id") == oid]
        if hits:
            return hits[0]
    if coid:
        hits = [row for row in rows if row.get("client_order_id") == coid]
        if hits:
            return hits[0]
    named = [
        row for row in rows
        if row.get("ticker") == ticker and row.get("side") == side
        and row.get("session_date") in (None, "", INCIDENT_DAY)
    ]
    exact = [row for row in named if row.get("session_date") == INCIDENT_DAY]
    if len(exact) == 1:
        return exact[0]
    if len(named) == 1:
        return named[0]
    return None


def interpret_fill(row: dict | None, *, history_ok: bool, open_ok: bool) -> dict:
    """Filled, not filled, or not observed. Absence is not a fill price."""
    if row is None:
        if history_ok and open_ok:
            return {
                "filled": False,
                "status": "not_on_book",
                "filled_qty": None,
                "price": None,
                "match": "none",
            }
        return {
            "filled": None,
            "status": "not_observed",
            "filled_qty": None,
            "price": None,
            "match": "none",
        }
    status = row.get("status") or ""
    filled_qty = row.get("filled_qty")
    filled = False
    if status in _DEAD_STATUSES:
        filled = False
    elif status in _FILLED_STATUSES:
        filled = True
    elif filled_qty is not None and filled_qty > 0:
        filled = True
    elif not status and filled_qty is None:
        return {
            "filled": None,
            "status": "unknown",
            "filled_qty": None,
            "price": None,
            "order_id": row.get("order_id") or "",
            "client_order_id": row.get("client_order_id") or "",
            "match": "row",
        }
    else:
        filled = False
    how = "ticker_side"
    return {
        "filled": filled,
        "status": status or "unknown",
        "filled_qty": filled_qty,
        "price": row.get("avg_price") if filled else None,
        "order_id": row.get("order_id") or "",
        "client_order_id": row.get("client_order_id") or "",
        "match": how,
    }


def _held_sentence(ticker: str, qty: int) -> str:
    if qty >= 1:
        return "The sandbox account holds " + ticker + " (" + str(qty) + " shares)."
    return "The sandbox account does not hold " + ticker + "."


def build_correction(
    positions: list[dict],
    orders: list[dict],
    *,
    timestamp: str,
    host: str,
    history_ok: bool,
    open_ok: bool,
    gap: dict | None,
) -> dict:
    """Compare the account with the sealed 2026-10-06 h1 book.

    Drift stays a warning. This does not build a catch-up order.
    """
    feam_qty = qty_of(positions, "FEAM")
    sdev_qty = qty_of(positions, "SDEV")
    book_feam = FEAM_BOOK_SHARES
    book_sdev = SDEV_BOOK_SHARES
    sdev_entry = SDEV_ENTRY
    if gap:
        try:
            book_feam = int((gap.get("feam") or {}).get("held_shares") or book_feam)
        except (TypeError, ValueError):
            book_feam = FEAM_BOOK_SHARES
        sdev_gap = gap.get("sdev") or {}
        try:
            book_sdev = int(sdev_gap.get("sealed_book_shares") or book_sdev)
        except (TypeError, ValueError):
            book_sdev = SDEV_BOOK_SHARES
        if sdev_gap.get("sealed_entry"):
            sdev_entry = str(sdev_gap.get("sealed_entry"))
    # The sentence stays the plain one the gap requires: 939 was the book.
    statement = BOOK_COUNT_SENTENCE
    if book_feam != FEAM_BOOK_SHARES:
        statement = (
            "The earlier held_shares " + str(book_feam)
            + " was the sealed book's share count, not the account's."
        )
    fills = {}
    for spec in incident_orders(gap):
        ticker = str(spec.get("ticker") or "").upper()
        side = str(spec.get("side") or "").upper()
        matched = _match_order(orders, spec)
        judged = interpret_fill(matched, history_ok=history_ok, open_ok=open_ok)
        if matched is not None:
            if matched.get("order_id") and matched.get("order_id") == str(spec.get("order_id") or ""):
                judged["match"] = "order_id"
            elif (
                matched.get("client_order_id")
                and matched.get("client_order_id") == str(spec.get("client_order_id") or "")
            ):
                judged["match"] = "client_order_id"
        fills[ticker] = {
            "ticker": ticker,
            "side": side,
            "sealed_shares": spec.get("shares"),
            "order_id": str(spec.get("order_id") or ""),
            "client_order_id": str(spec.get("client_order_id") or ""),
            "filled": judged["filled"],
            "status": judged["status"],
            "filled_qty": judged["filled_qty"],
            "price": judged["price"],
            "match": judged["match"],
        }
    note = " ".join([
        statement,
        _held_sentence("FEAM", feam_qty),
        _held_sentence("SDEV", sdev_qty),
        (
            "The sealed book had SDEV " + str(book_sdev)
            + " shares, entered " + sdev_entry + "."
        ),
        (
            "Fill status and prices for the 10:17 PACB, DNA and QSI buys "
            "and the GLND and NAUT sells are on this line."
        ),
        "Drift is a warning only; no catch-up order.",
    ])
    return {
        "kind": "correction",
        "date": INCIDENT_DAY,
        "timestamp": timestamp,
        "host": host,
        "note": note,
        "statement": statement,
        "feam": {
            "held": feam_qty >= 1,
            "qty": feam_qty,
            "avg_cost": cost_of(positions, "FEAM"),
            "sealed_book_shares": book_feam,
            "earlier_gap_held_shares": book_feam,
            "earlier_gap_held_shares_source": "sealed book, not the sandbox account",
        },
        "sdev": {
            "held": sdev_qty >= 1,
            "qty": sdev_qty,
            "avg_cost": cost_of(positions, "SDEV"),
            "sealed_book_shares": book_sdev,
            "sealed_entry": sdev_entry,
        },
        "fills": fills,
        "catch_up_order": False,
        "plan_sha256": str((gap or {}).get("plan_sha256") or ""),
        "plan_commit": str((gap or {}).get("plan_commit") or ""),
    }


def build_snapshot(
    positions: list[dict],
    collected: dict,
    *,
    timestamp: str,
    host: str,
    today: str,
) -> dict:
    return {
        "kind": "account_snapshot",
        "timestamp": timestamp,
        "host": host,
        "today": today,
        "incident_date": INCIDENT_DAY,
        "read_only": True,
        "orders_sent": False,
        "n_positions": len(positions),
        "positions": positions,
        "orders": collected["orders"],
        "queried_dates": collected["queried_dates"],
        "order_queries": {
            "open_ok": collected["open_ok"],
            "incident_history_ok": collected["incident_history_ok"],
            "history_ok": collected["history_ok"],
            "history_formats": collected.get("history_formats") or {},
            "errors": collected["errors"],
        },
        "note": (
            "Read-only sandbox snapshot. No order was placed, modified, "
            "or cancelled."
        ),
    }


def _ensure_trailing_newline(path: Path) -> None:
    """Append a newline if the file is non-empty and lacks one.

    Existing bytes stay. A missing terminator would glue the next JSON
    object onto the last line.
    """
    if not path.is_file():
        return
    data = path.read_bytes()
    if not data or data.endswith(b"\n"):
        return
    with path.open("ab") as handle:
        handle.write(b"\n")
        handle.flush()
        os.fsync(handle.fileno())


def append_new_lines(path: Path, records: list[dict]) -> None:
    """Append records. The bytes already in the file stay a prefix."""
    path = Path(path)
    before = path.read_bytes() if path.is_file() else b""
    _ensure_trailing_newline(path)
    for record in records:
        paper_drift.append_drift(record, path)
    after = path.read_bytes()
    if not after.startswith(before):
        raise RuntimeError("drift log prefix changed")


def _assert_readonly(guard: ReadOnlyPaper) -> None:
    for name in guard.calls:
        if name in FORBIDDEN_CALLS or name not in ALLOWED_CALLS:
            raise SnapshotRefused("read-only snapshot refuses " + name)


def run_snapshot(*, api=None, clock: datetime | None = None,
                 path: Path | None = None) -> dict:
    """Read the sandbox and append snapshot + correction. Or refuse.

    A freeze or a non-sandbox host writes nothing. A failed position
    read writes nothing. Order-list failures are stored on the snapshot;
    missing fills stay ``not_observed`` rather than a guessed price.
    """
    moment = clock if clock is not None else now()
    if in_write_freeze(moment):
        print(
            "paper account snapshot: 03:00-09:40 ET is closed; "
            "nothing writes to books in this window",
            flush=True,
        )
        return {"wrote": False, "frozen": True}
    client = api if api is not None else default_api()
    require_sandbox(client)
    guard = ReadOnlyPaper(client)
    if not guard.connect():
        raise RuntimeError(
            getattr(client, "err", None) or "sandbox account not connected"
        )
    try:
        snap = guard.snapshot()
    except SnapshotRefused:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("position snapshot failed: " + str(exc)[:240]) from exc
    if not getattr(snap, "connected", False):
        detail = getattr(snap, "error", None) or "not connected"
        raise RuntimeError("position snapshot failed: " + str(detail)[:240])
    positions = position_rows(snap)
    collected = collect_orders(guard, moment)
    _assert_readonly(guard)
    host = require_sandbox(client)
    timestamp = _as_et(moment).isoformat()
    today = _as_et(moment).date().isoformat()
    log_path = Path(path) if path is not None else DRIFT_LOG
    gap = read_gap(log_path)
    snapshot = build_snapshot(
        positions, collected, timestamp=timestamp, host=host, today=today,
    )
    correction = build_correction(
        positions,
        collected["orders"],
        timestamp=timestamp,
        host=host,
        history_ok=bool(collected["incident_history_ok"]),
        open_ok=bool(collected["open_ok"]),
        gap=gap,
    )
    _assert_readonly(guard)
    append_new_lines(log_path, [snapshot, correction])
    print(_summary(correction), flush=True)
    return {
        "wrote": True,
        "frozen": False,
        "snapshot": snapshot,
        "correction": correction,
        "calls": list(guard.calls),
    }


def _summary(correction: dict) -> str:
    feam = correction["feam"]
    sdev = correction["sdev"]
    parts = [
        "host=" + str(correction.get("host")),
        "FEAM held=" + ("yes" if feam["held"] else "no") + " qty=" + str(feam["qty"]),
        "SDEV held=" + ("yes" if sdev["held"] else "no") + " qty=" + str(sdev["qty"]),
    ]
    for ticker in ("PACB", "DNA", "QSI", "GLND", "NAUT"):
        row = (correction.get("fills") or {}).get(ticker) or {}
        price = row.get("price")
        shown = "none" if price is None else str(price)
        parts.append(
            ticker + " " + str(row.get("side") or "")
            + " filled=" + str(row.get("filled"))
            + " status=" + str(row.get("status"))
            + " price=" + shown
        )
    return "paper account snapshot: " + "; ".join(parts)


def main(argv=None) -> int:
    del argv
    try:
        result = run_snapshot()
    except SnapshotRefused as exc:
        print("paper account snapshot: " + str(exc), file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print("paper account snapshot: " + str(exc), file=sys.stderr)
        return 2
    if not result.get("wrote"):
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
